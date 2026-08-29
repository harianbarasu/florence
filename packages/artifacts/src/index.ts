import { createCipheriv, createDecipheriv, createHash, randomBytes, randomUUID } from "node:crypto";
import { chmod, link, mkdir, readdir, readFile, rename, stat, unlink, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import type { ImageReference } from "@florence/contracts";

export const MAX_IMAGE_BYTES = 20 * 1024 * 1024;
export const MAX_PDF_BYTES = 20 * 1024 * 1024;
export const MAX_FILE_ARTIFACT_BYTES = 100 * 1024 * 1024;
const imageRetentionMs = 24 * 60 * 60 * 1_000;
const envelopeOverheadLimit = 16 * 1024;
const imageEnvelopeMagic = Buffer.from("FIV1");
const pdfEnvelopeMagic = Buffer.from("FPD1");
const fileArtifactEnvelopeMagic = Buffer.from("FAF1");
const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const storedImageFilenamePattern =
  /^([0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\.fiv$/;
const storedFileArtifactFilenamePattern =
  /^([0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\.faf$/;
const storedArtifactTemporaryFilenamePattern =
  /^\.([0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\.([0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\.tmp$/;

type StoredMimeType = Exclude<ImageReference["mimeType"], "image/heic">;
type AcceptedMimeType = ImageReference["mimeType"];

export type StoredImage = {
  image: { assetId: string; mimeType: StoredMimeType };
  byteLength: number;
  expiresAt: string;
};

export type SealedPdf = {
  documentId: string;
  filename: string;
  mimeType: "application/pdf";
  contentDigest: string;
  contentEnvelope: Uint8Array;
  discardAfter: string;
  byteLength: number;
};

/**
 * Browser-file persistence adapts OpenInstinct's scoped, hash-verified browser-image
 * store (480045dbc63008e7f99313d1683858cd8657b35a), Hermes's explicit artifact
 * receipt/attachment handoff (6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882), and Pi's
 * compact tool-result/reference boundary (4e494929998d6bc4fccf75e0a233f727db4b70ee).
 * Florence generalizes those patterns to original files and keeps its encrypted volume
 * plus durable family-work state instead of importing an image-only or process-local store.
 */
export type FileArtifactReference = Readonly<{
  artifactId: string;
  workId: string;
  filename: string;
  mimeType: string;
  byteLength: number;
  sha256: string;
}>;

export type HeicNormalizer = (bytes: Uint8Array) => Promise<Uint8Array>;

type HeicConverter = (input: { buffer: Uint8Array; format: "JPEG"; quality: number }) => Promise<Uint8Array>;

const requireFromHere = createRequire(import.meta.url);
let heicConverter: HeicConverter | undefined;

export async function normalizeHeicToJpeg(bytes: Uint8Array): Promise<Uint8Array> {
  heicConverter ??= requireFromHere("heic-convert") as HeicConverter;
  return heicConverter({ buffer: bytes, format: "JPEG", quality: 0.92 });
}

export type EncryptedImageVaultOptions = {
  rootDirectory: string;
  encryptionKey: Uint8Array;
  normalizeHeic?: HeicNormalizer;
};

export type ImageVaultProductionResetSnapshot = Readonly<{
  guard: string;
  encryptedImageArtifacts: number;
  encryptedFileArtifacts: number;
  encryptedImageTemporaryArtifacts: number;
}>;

export type ImageVaultProductionResetResult = Readonly<{
  encryptedImageArtifactsDeleted: number;
  encryptedFileArtifactsDeleted: number;
  encryptedImageTemporaryArtifactsDeleted: number;
}>;

export type ImageVaultErrorCode =
  | "invalid_configuration"
  | "invalid_image"
  | "image_too_large"
  | "unsupported_image"
  | "invalid_document"
  | "document_too_large"
  | "invalid_artifact"
  | "artifact_too_large"
  | "asset_conflict"
  | "unauthorized_or_missing"
  | "expired"
  | "corrupt";

export class ImageVaultError extends Error {
  readonly retryable = false;

  constructor(
    readonly code: ImageVaultErrorCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "ImageVaultError";
  }
}

type SourceIdentity = {
  assetId: string;
  householdId: string;
  sourceSignalId: string;
  sourceMimeType: AcceptedMimeType;
  sourceDigest: string;
};

type ImageMetadata = SourceIdentity & {
  version: 1;
  mimeType: StoredMimeType;
  byteLength: number;
  expiresAt: string;
  retained?: boolean;
  retentionClaims?: string[];
};

type PdfMetadata = {
  version: 1;
  kind: "pdf";
  documentId: string;
  householdId: string;
  sourceSignalId: string;
  filename: string;
  mimeType: "application/pdf";
  sourceDigest: string;
  byteLength: number;
  discardAfter: string | null;
  retained?: boolean;
};

type FileArtifactMetadata = {
  version: 1;
  kind: "file_artifact";
  artifactId: string;
  householdId: string;
  workId: string;
  sourceCallId: string;
  filename: string;
  mimeType: string;
  byteLength: number;
  sha256: string;
};

export function decodeImageVaultKey(value: string): Uint8Array {
  const canonical = value.trim();
  const decoded = Buffer.from(canonical, "base64");
  if (decoded.length !== 32 || decoded.toString("base64") !== canonical) {
    throw new ImageVaultError(
      "invalid_configuration",
      "FLORENCE_IMAGE_VAULT_KEY must be exactly 32 random bytes in canonical base64",
    );
  }
  return decoded;
}

export class EncryptedImageVault {
  readonly #rootDirectory: string;
  readonly #key: Buffer;
  readonly #normalizeHeic: HeicNormalizer | undefined;
  readonly #retentionQueues = new Map<string, Promise<void>>();

  constructor(options: EncryptedImageVaultOptions) {
    if (!path.isAbsolute(options.rootDirectory)) {
      throw new ImageVaultError("invalid_configuration", "Image vault directory must be absolute");
    }
    if (options.encryptionKey.byteLength !== 32) {
      throw new ImageVaultError("invalid_configuration", "Image vault key must contain 32 bytes");
    }
    this.#rootDirectory = options.rootDirectory;
    this.#key = Buffer.from(options.encryptionKey);
    this.#normalizeHeic = options.normalizeHeic;
  }

  async storeFileArtifact(input: {
    artifactId: string;
    householdId: string;
    workId: string;
    callId: string;
    filename: string;
    declaredMimeType: string;
    bytes: Uint8Array;
  }): Promise<FileArtifactReference> {
    requireFileArtifactUuid(input.artifactId, "File artifact");
    requireFileArtifactUuid(input.householdId, "File artifact household");
    requireFileArtifactUuid(input.workId, "File artifact work");
    const sourceCallId = requireFileArtifactCallId(input.callId);
    const filename = requireFileArtifactFilename(input.filename);
    const mimeType = requireFileArtifactMimeType(input.declaredMimeType);
    if (!(input.bytes instanceof Uint8Array) || input.bytes.byteLength < 1) {
      throw new ImageVaultError("invalid_artifact", "File artifact is empty");
    }
    if (input.bytes.byteLength > MAX_FILE_ARTIFACT_BYTES) {
      throw new ImageVaultError("artifact_too_large", "File artifact exceeds Florence's 100 MB limit");
    }
    const bytes = Buffer.from(input.bytes);
    const metadata: FileArtifactMetadata = {
      version: 1,
      kind: "file_artifact",
      artifactId: input.artifactId,
      householdId: input.householdId,
      workId: input.workId,
      sourceCallId,
      filename,
      mimeType,
      byteLength: bytes.length,
      sha256: createHash("sha256").update(bytes).digest("hex"),
    };
    const existing = await this.#loadFileArtifact(input.artifactId);
    if (existing) return existingFileArtifactResult(existing, metadata, bytes);

    const encrypted = encryptAuthenticatedEnvelope(this.#key, fileArtifactEnvelopeMagic, metadata, bytes);
    if (!(await this.#writeFileArtifactAtomically(input.artifactId, encrypted))) {
      const raced = await this.#loadFileArtifact(input.artifactId);
      if (!raced) throw new ImageVaultError("corrupt", "Concurrent file artifact write disappeared");
      return existingFileArtifactResult(raced, metadata, bytes);
    }
    return fileArtifactReference(metadata);
  }

  async readFileArtifact(input: {
    householdId: string;
    workId: string;
    artifact: FileArtifactReference;
  }): Promise<Uint8Array> {
    requireFileArtifactUuid(input.householdId, "File artifact household");
    requireFileArtifactUuid(input.workId, "File artifact work");
    requireFileArtifactReference(input.artifact);
    const loaded = await this.#loadFileArtifact(input.artifact.artifactId);
    if (!loaded) throw new ImageVaultError("unauthorized_or_missing", "File artifact is unavailable");
    const authorized =
      loaded.metadata.householdId === input.householdId &&
      loaded.metadata.workId === input.workId &&
      loaded.metadata.artifactId === input.artifact.artifactId &&
      loaded.metadata.workId === input.artifact.workId &&
      loaded.metadata.filename === input.artifact.filename &&
      loaded.metadata.mimeType === input.artifact.mimeType &&
      loaded.metadata.byteLength === input.artifact.byteLength &&
      loaded.metadata.sha256 === input.artifact.sha256;
    if (!authorized) {
      throw new ImageVaultError("unauthorized_or_missing", "File artifact is unavailable");
    }
    return loaded.bytes;
  }

  async readFileArtifactById(input: {
    householdId: string;
    workId: string;
    artifactId: string;
  }): Promise<{ artifact: FileArtifactReference; bytes: Uint8Array }> {
    requireFileArtifactUuid(input.householdId, "File artifact household");
    requireFileArtifactUuid(input.workId, "File artifact work");
    requireFileArtifactUuid(input.artifactId, "File artifact");
    const loaded = await this.#loadFileArtifact(input.artifactId);
    if (
      !loaded ||
      loaded.metadata.householdId !== input.householdId ||
      loaded.metadata.workId !== input.workId ||
      loaded.metadata.artifactId !== input.artifactId
    ) {
      throw new ImageVaultError("unauthorized_or_missing", "File artifact is unavailable");
    }
    return { artifact: fileArtifactReference(loaded.metadata), bytes: loaded.bytes };
  }

  async store(input: {
    assetId?: string;
    householdId: string;
    signalId: string;
    declaredMimeType: AcceptedMimeType;
    bytes: Uint8Array;
  }): Promise<StoredImage> {
    requireUuid(input.householdId);
    requireUuid(input.signalId);
    const assetId = input.assetId ?? randomUUID();
    requireUuid(assetId);
    const sourceBytes = Buffer.from(input.bytes);
    if (sourceBytes.length === 0) throw new ImageVaultError("invalid_image", "Image is empty");
    if (sourceBytes.length > MAX_IMAGE_BYTES) {
      throw new ImageVaultError("image_too_large", "Image exceeds Florence's 20 MB limit");
    }
    const detected = detectMimeType(sourceBytes);
    if (detected !== input.declaredMimeType) {
      throw new ImageVaultError("invalid_image", "Declared image type does not match its bytes");
    }
    const sourceDigest = createHash("sha256").update(sourceBytes).digest("hex");
    const identity = {
      assetId,
      householdId: input.householdId,
      sourceSignalId: input.signalId,
      sourceMimeType: input.declaredMimeType,
      sourceDigest,
    };
    const existing = await this.#load(assetId);
    if (existing) return existingStoreResult(existing, identity);

    let bytes = sourceBytes;
    let mimeType: StoredMimeType;
    if (detected === "image/heic") {
      if (!this.#normalizeHeic) {
        throw new ImageVaultError("unsupported_image", "HEIC normalization is not configured");
      }
      try {
        bytes = Buffer.from(await this.#normalizeHeic(bytes));
      } catch (error) {
        throw new ImageVaultError("invalid_image", "HEIC image could not be decoded", { cause: error });
      }
      if (detectMimeType(bytes) !== "image/jpeg") {
        throw new ImageVaultError("invalid_image", "HEIC normalizer did not return a JPEG");
      }
      mimeType = "image/jpeg";
    } else {
      mimeType = detected;
    }
    if (bytes.length === 0 || bytes.length > MAX_IMAGE_BYTES) {
      throw new ImageVaultError("image_too_large", "Normalized image exceeds Florence's 20 MB limit");
    }

    const expiresAt = new Date(Date.now() + imageRetentionMs);
    const metadata: ImageMetadata = {
      version: 1,
      assetId,
      householdId: input.householdId,
      sourceSignalId: input.signalId,
      sourceMimeType: input.declaredMimeType,
      sourceDigest,
      mimeType,
      byteLength: bytes.length,
      expiresAt: expiresAt.toISOString(),
    };
    const encrypted = encryptEnvelope(this.#key, metadata, bytes);
    if (!(await this.#writeAtomically(assetId, encrypted))) {
      const raced = await this.#load(assetId);
      if (!raced) throw new ImageVaultError("corrupt", "Concurrent image write disappeared");
      return existingStoreResult(raced, identity);
    }
    return { image: { assetId, mimeType }, byteLength: bytes.length, expiresAt: metadata.expiresAt };
  }

  async read(input: {
    householdId: string;
    signalId: string;
    image: ImageReference;
  }): Promise<{ mimeType: StoredMimeType; bytes: Uint8Array }> {
    const loaded = await this.#loadAuthorized(input);
    if (loaded.metadata.retained !== true && Date.parse(loaded.metadata.expiresAt) <= Date.now()) {
      await unlink(this.#assetPath(input.image.assetId)).catch(() => undefined);
      throw new ImageVaultError("expired", "Image retention has expired");
    }
    return { mimeType: loaded.metadata.mimeType, bytes: loaded.bytes };
  }

  /** Keeps one unexpired, already-authorized exact Message image available for durable family work. */
  async retain(input: {
    householdId: string;
    signalId: string;
    image: ImageReference;
    claimId: string;
    now?: Date;
  }): Promise<void> {
    requireUuid(input.claimId);
    await this.#withRetentionLock(input.image.assetId, async () => {
      const loaded = await this.#loadAuthorized(input);
      if (loaded.metadata.retained === true && loaded.metadata.retentionClaims === undefined) return;
      if (Date.parse(loaded.metadata.expiresAt) <= (input.now ?? new Date()).getTime()) {
        await unlink(this.#assetPath(input.image.assetId)).catch(() => undefined);
        throw new ImageVaultError("expired", "Image retention has expired");
      }
      const retentionClaims = [...new Set([...(loaded.metadata.retentionClaims ?? []), input.claimId])];
      const retainedEnvelope = encryptEnvelope(
        this.#key,
        { ...loaded.metadata, retained: true, retentionClaims },
        loaded.bytes,
      );
      await this.#replaceAtomically(input.image.assetId, retainedEnvelope);
    });
  }

  /** Releases only this turn's claim; another committed or concurrent claimant keeps the image. */
  async releaseRetention(input: {
    householdId: string;
    signalId: string;
    image: ImageReference;
    claimId: string;
  }): Promise<boolean> {
    requireUuid(input.claimId);
    return this.#withRetentionLock(input.image.assetId, async () => {
      const loaded = await this.#loadAuthorized(input);
      const claims = loaded.metadata.retentionClaims;
      if (claims === undefined || !claims.includes(input.claimId)) return false;
      const retentionClaims = claims.filter((claimId) => claimId !== input.claimId);
      const retainedEnvelope = encryptEnvelope(
        this.#key,
        { ...loaded.metadata, retained: retentionClaims.length > 0, retentionClaims },
        loaded.bytes,
      );
      await this.#replaceAtomically(input.image.assetId, retainedEnvelope);
      return true;
    });
  }

  sealPdf(input: {
    documentId: string;
    householdId: string;
    signalId: string;
    filename: string;
    declaredMimeType: string;
    bytes: Uint8Array;
    discardAfter: string;
  }): SealedPdf {
    requirePdfUuid(input.documentId, "PDF document");
    requirePdfUuid(input.householdId, "PDF household");
    requirePdfUuid(input.signalId, "PDF source message");
    const filename = requirePdfFilename(input.filename);
    if (input.declaredMimeType !== "application/pdf") {
      throw new ImageVaultError("invalid_document", "Declared PDF type must be application/pdf");
    }
    const bytes = Buffer.from(input.bytes);
    if (bytes.length < 1 || bytes.length > MAX_PDF_BYTES) {
      throw new ImageVaultError("document_too_large", "PDF must contain at most 20 MB");
    }
    if (!isPdf(bytes)) {
      throw new ImageVaultError("invalid_document", "PDF bytes do not contain a valid PDF signature");
    }
    const discardAfter = requireTimestamp(input.discardAfter, "PDF discard time");
    const contentDigest = createHash("sha256").update(bytes).digest("hex");
    const metadata: PdfMetadata = {
      version: 1,
      kind: "pdf",
      documentId: input.documentId,
      householdId: input.householdId,
      sourceSignalId: input.signalId,
      filename,
      mimeType: "application/pdf",
      sourceDigest: contentDigest,
      byteLength: bytes.length,
      discardAfter,
      retained: false,
    };
    return {
      documentId: input.documentId,
      filename,
      mimeType: "application/pdf",
      contentDigest,
      contentEnvelope: encryptAuthenticatedEnvelope(this.#key, pdfEnvelopeMagic, metadata, bytes),
      discardAfter,
      byteLength: bytes.length,
    };
  }

  openPdf(input: {
    documentId: string;
    householdId: string;
    signalId: string;
    filename: string;
    mimeType: string;
    contentDigest: string;
    contentEnvelope: Uint8Array;
    discardAfter: string | null;
    now?: Date;
  }): { mimeType: "application/pdf"; bytes: Uint8Array } {
    requirePdfUuid(input.documentId, "PDF document");
    requirePdfUuid(input.householdId, "PDF household");
    requirePdfUuid(input.signalId, "PDF source message");
    const filename = requirePdfFilename(input.filename);
    const discardAfter =
      input.discardAfter === null ? null : requireTimestamp(input.discardAfter, "PDF discard time");
    if (input.mimeType !== "application/pdf" || !/^[0-9a-f]{64}$/.test(input.contentDigest)) {
      throw new ImageVaultError("invalid_document", "PDF provenance is invalid");
    }
    const loaded = decryptPdfEnvelope(this.#key, input.contentEnvelope);
    const authorized =
      loaded.metadata.documentId === input.documentId &&
      loaded.metadata.householdId === input.householdId &&
      loaded.metadata.sourceSignalId === input.signalId &&
      loaded.metadata.filename === filename &&
      loaded.metadata.mimeType === input.mimeType &&
      loaded.metadata.sourceDigest === input.contentDigest &&
      loaded.metadata.discardAfter === discardAfter &&
      (loaded.metadata.retained === true) === (discardAfter === null);
    if (!authorized) {
      throw new ImageVaultError("unauthorized_or_missing", "PDF is unavailable");
    }
    if (discardAfter !== null && Date.parse(discardAfter) <= (input.now ?? new Date()).getTime()) {
      throw new ImageVaultError("expired", "PDF retention has expired");
    }
    return { mimeType: "application/pdf", bytes: loaded.bytes };
  }

  /** Reauthorizes one exact sealed Message PDF for the durable record that cited it. */
  retainPdf(input: {
    documentId: string;
    householdId: string;
    signalId: string;
    filename: string;
    mimeType: string;
    contentDigest: string;
    contentEnvelope: Uint8Array;
    discardAfter: string;
    now?: Date;
  }): { contentEnvelope: Uint8Array } {
    const opened = this.openPdf(input);
    const metadata: PdfMetadata = {
      version: 1,
      kind: "pdf",
      documentId: input.documentId,
      householdId: input.householdId,
      sourceSignalId: input.signalId,
      filename: input.filename,
      mimeType: "application/pdf",
      sourceDigest: input.contentDigest,
      byteLength: opened.bytes.byteLength,
      discardAfter: null,
      retained: true,
    };
    return {
      contentEnvelope: encryptAuthenticatedEnvelope(this.#key, pdfEnvelopeMagic, metadata, opened.bytes),
    };
  }

  async delete(input: { householdId: string; signalId: string; image: ImageReference }): Promise<void> {
    await this.#loadAuthorized(input);
    await unlink(this.#assetPath(input.image.assetId));
  }

  async purgeExpired(now = new Date()): Promise<number> {
    const shards = await readdir(this.#rootDirectory, { withFileTypes: true }).catch((error: unknown) => {
      if (isMissing(error)) return [];
      throw error;
    });
    let deleted = 0;
    for (const shard of shards) {
      if (!shard.isDirectory() || !/^[0-9a-f]{2}$/.test(shard.name)) continue;
      const directory = path.join(this.#rootDirectory, shard.name);
      const entries = await readdir(directory, { withFileTypes: true });
      for (const entry of entries) {
        if (!entry.isFile() || !entry.name.endsWith(".fiv")) continue;
        const assetPath = path.join(directory, entry.name);
        let expired = false;
        try {
          const envelope = await readBounded(assetPath);
          const { metadata } = decryptEnvelope(this.#key, envelope);
          expired = metadata.retained !== true && Date.parse(metadata.expiresAt) <= now.getTime();
        } catch {
          const file = await stat(assetPath);
          expired = file.mtimeMs + imageRetentionMs <= now.getTime();
        }
        if (expired) {
          await unlink(assetPath);
          deleted += 1;
        }
      }
    }
    return deleted;
  }

  /**
   * Inventories only canonical Florence image/file envelopes and exact atomic-write temporary
   * files. It never follows arbitrary directories, accepts non-canonical filenames, or exposes
   * artifact identities to the reset operator.
   */
  async inspectForProductionReset(): Promise<ImageVaultProductionResetSnapshot> {
    const inventory = await this.#productionResetInventory();
    return Object.freeze({
      guard: inventory.guard,
      encryptedImageArtifacts: inventory.artifacts.filter(({ kind }) => kind === "image_envelope").length,
      encryptedFileArtifacts: inventory.artifacts.filter(({ kind }) => kind === "file_envelope").length,
      encryptedImageTemporaryArtifacts: inventory.artifacts.filter(({ kind }) => kind === "temporary").length,
    });
  }

  /**
   * Deletes exactly the canonical `.fiv`/`.faf` files and Florence atomic-write temporary files
   * represented by a previously inspected inventory. Shard directories, arbitrary `.tmp` files,
   * and unrelated volume contents are deliberately retained.
   */
  async purgeForProductionReset(
    expected: ImageVaultProductionResetSnapshot,
  ): Promise<ImageVaultProductionResetResult> {
    const current = await this.#productionResetInventory();
    const encryptedImageArtifacts = current.artifacts.filter(({ kind }) => kind === "image_envelope").length;
    const encryptedFileArtifacts = current.artifacts.filter(({ kind }) => kind === "file_envelope").length;
    const encryptedImageTemporaryArtifacts = current.artifacts.filter(
      ({ kind }) => kind === "temporary",
    ).length;
    if (
      current.guard !== expected.guard ||
      encryptedImageArtifacts !== expected.encryptedImageArtifacts ||
      encryptedFileArtifacts !== expected.encryptedFileArtifacts ||
      encryptedImageTemporaryArtifacts !== expected.encryptedImageTemporaryArtifacts
    ) {
      throw new ImageVaultError(
        "asset_conflict",
        "Image vault contents changed after the production reset inspection",
      );
    }
    let encryptedImageArtifactsDeleted = 0;
    let encryptedFileArtifactsDeleted = 0;
    let encryptedImageTemporaryArtifactsDeleted = 0;
    for (const artifact of current.artifacts) {
      try {
        await unlink(path.join(this.#rootDirectory, artifact.relativePath));
        if (artifact.kind === "image_envelope") encryptedImageArtifactsDeleted += 1;
        else if (artifact.kind === "file_envelope") encryptedFileArtifactsDeleted += 1;
        else encryptedImageTemporaryArtifactsDeleted += 1;
      } catch (error) {
        if (isMissing(error)) {
          throw new ImageVaultError(
            "asset_conflict",
            "Image vault contents changed during the production reset",
          );
        }
        throw error;
      }
    }
    const remaining = await this.#productionResetInventory();
    if (remaining.artifacts.length !== 0) {
      throw new ImageVaultError("asset_conflict", "Image vault contents changed during the production reset");
    }
    return Object.freeze({
      encryptedImageArtifactsDeleted,
      encryptedFileArtifactsDeleted,
      encryptedImageTemporaryArtifactsDeleted,
    });
  }

  async #loadAuthorized(input: {
    householdId: string;
    signalId: string;
    image: ImageReference;
  }): Promise<{ metadata: ImageMetadata; bytes: Uint8Array }> {
    requireUuid(input.householdId);
    requireUuid(input.signalId);
    requireUuid(input.image.assetId);
    const loaded = await this.#load(input.image.assetId);
    if (!loaded) throw new ImageVaultError("unauthorized_or_missing", "Image is unavailable");
    const authorized =
      loaded.metadata.householdId === input.householdId &&
      loaded.metadata.sourceSignalId === input.signalId &&
      loaded.metadata.assetId === input.image.assetId &&
      loaded.metadata.mimeType === input.image.mimeType;
    if (!authorized) {
      throw new ImageVaultError("unauthorized_or_missing", "Image is unavailable");
    }
    return loaded;
  }

  async #productionResetInventory(): Promise<{
    guard: string;
    artifacts: readonly Readonly<{
      kind: "image_envelope" | "file_envelope" | "temporary";
      relativePath: string;
    }>[];
  }> {
    const shards = await readdir(this.#rootDirectory, { withFileTypes: true }).catch((error: unknown) => {
      if (isMissing(error)) return [];
      throw error;
    });
    const artifacts: {
      kind: "image_envelope" | "file_envelope" | "temporary";
      relativePath: string;
    }[] = [];
    for (const shard of shards) {
      if (!shard.isDirectory() || !/^[0-9a-f]{2}$/.test(shard.name)) continue;
      const entries = await readdir(path.join(this.#rootDirectory, shard.name), {
        withFileTypes: true,
      }).catch((error: unknown) => {
        if (isMissing(error)) return [];
        throw error;
      });
      for (const entry of entries) {
        if (!entry.isFile()) continue;
        const storedImageMatch = storedImageFilenamePattern.exec(entry.name);
        if (storedImageMatch?.[1]?.slice(0, 2) === shard.name) {
          artifacts.push({ kind: "image_envelope", relativePath: path.join(shard.name, entry.name) });
          continue;
        }
        const storedFileArtifactMatch = storedFileArtifactFilenamePattern.exec(entry.name);
        if (storedFileArtifactMatch?.[1]?.slice(0, 2) === shard.name) {
          artifacts.push({ kind: "file_envelope", relativePath: path.join(shard.name, entry.name) });
          continue;
        }
        const temporaryArtifactMatch = storedArtifactTemporaryFilenamePattern.exec(entry.name);
        if (temporaryArtifactMatch?.[1]?.slice(0, 2) === shard.name) {
          artifacts.push({ kind: "temporary", relativePath: path.join(shard.name, entry.name) });
        }
      }
    }
    artifacts.sort((left, right) =>
      left.relativePath < right.relativePath ? -1 : left.relativePath > right.relativePath ? 1 : 0,
    );
    const guard = createHash("sha256")
      .update("florence-image-vault-production-reset-v3\0")
      .update(artifacts.map(({ kind, relativePath }) => `${kind}\0${relativePath}`).join("\0"))
      .digest("hex");
    return { guard, artifacts: Object.freeze(artifacts) };
  }

  #assetPath(assetId: string): string {
    return path.join(this.#rootDirectory, assetId.slice(0, 2), `${assetId}.fiv`);
  }

  #fileArtifactPath(artifactId: string): string {
    return path.join(this.#rootDirectory, artifactId.slice(0, 2), `${artifactId}.faf`);
  }

  async #load(assetId: string): Promise<{ metadata: ImageMetadata; bytes: Uint8Array } | null> {
    try {
      return decryptEnvelope(this.#key, await readBounded(this.#assetPath(assetId)));
    } catch (error) {
      if (isMissing(error)) return null;
      if (error instanceof ImageVaultError) throw error;
      throw new ImageVaultError("corrupt", "Image envelope cannot be authenticated", { cause: error });
    }
  }

  async #loadFileArtifact(
    artifactId: string,
  ): Promise<{ metadata: FileArtifactMetadata; bytes: Uint8Array } | null> {
    try {
      const envelope = await readBounded(
        this.#fileArtifactPath(artifactId),
        MAX_FILE_ARTIFACT_BYTES,
        "File artifact",
      );
      return decryptFileArtifactEnvelope(this.#key, envelope);
    } catch (error) {
      if (isMissing(error)) return null;
      if (error instanceof ImageVaultError) throw error;
      throw new ImageVaultError("corrupt", "File artifact envelope cannot be authenticated", {
        cause: error,
      });
    }
  }

  async #writeAtomically(assetId: string, envelope: Uint8Array): Promise<boolean> {
    return this.#writeEnvelopeAtomically(assetId, this.#assetPath(assetId), envelope);
  }

  async #writeFileArtifactAtomically(artifactId: string, envelope: Uint8Array): Promise<boolean> {
    return this.#writeEnvelopeAtomically(artifactId, this.#fileArtifactPath(artifactId), envelope);
  }

  async #writeEnvelopeAtomically(
    artifactId: string,
    artifactPath: string,
    envelope: Uint8Array,
  ): Promise<boolean> {
    const directory = path.dirname(artifactPath);
    await mkdir(this.#rootDirectory, { recursive: true, mode: 0o700 });
    await chmod(this.#rootDirectory, 0o700);
    await mkdir(directory, { recursive: true, mode: 0o700 });
    await chmod(directory, 0o700);
    const temporary = path.join(directory, `.${artifactId}.${randomUUID()}.tmp`);
    try {
      await writeFile(temporary, envelope, { flag: "wx", mode: 0o600 });
      await link(temporary, artifactPath);
      await unlink(temporary);
      return true;
    } catch (error) {
      await unlink(temporary).catch(() => undefined);
      if (isAlreadyExists(error)) return false;
      throw error;
    }
  }

  async #replaceAtomically(assetId: string, envelope: Uint8Array): Promise<void> {
    const directory = path.dirname(this.#assetPath(assetId));
    await mkdir(this.#rootDirectory, { recursive: true, mode: 0o700 });
    await chmod(this.#rootDirectory, 0o700);
    await mkdir(directory, { recursive: true, mode: 0o700 });
    await chmod(directory, 0o700);
    const temporary = path.join(directory, `.${assetId}.${randomUUID()}.tmp`);
    try {
      await writeFile(temporary, envelope, { flag: "wx", mode: 0o600 });
      await rename(temporary, this.#assetPath(assetId));
    } finally {
      await unlink(temporary).catch(() => undefined);
    }
  }

  async #withRetentionLock<T>(assetId: string, operation: () => Promise<T>): Promise<T> {
    const prior = this.#retentionQueues.get(assetId) ?? Promise.resolve();
    let release = () => {};
    const current = new Promise<void>((resolve) => {
      release = resolve;
    });
    const queued = prior.catch(() => undefined).then(() => current);
    this.#retentionQueues.set(assetId, queued);
    await prior.catch(() => undefined);
    try {
      return await operation();
    } finally {
      release();
      if (this.#retentionQueues.get(assetId) === queued) this.#retentionQueues.delete(assetId);
    }
  }
}

function existingFileArtifactResult(
  loaded: { metadata: FileArtifactMetadata; bytes: Uint8Array },
  expected: FileArtifactMetadata,
  expectedBytes: Uint8Array,
): FileArtifactReference {
  const identical =
    loaded.metadata.artifactId === expected.artifactId &&
    loaded.metadata.householdId === expected.householdId &&
    loaded.metadata.workId === expected.workId &&
    loaded.metadata.sourceCallId === expected.sourceCallId &&
    loaded.metadata.filename === expected.filename &&
    loaded.metadata.mimeType === expected.mimeType &&
    loaded.metadata.byteLength === expected.byteLength &&
    loaded.metadata.sha256 === expected.sha256 &&
    Buffer.compare(loaded.bytes, expectedBytes) === 0;
  if (!identical) {
    throw new ImageVaultError("asset_conflict", "Artifact ID is already bound to a different file artifact");
  }
  return fileArtifactReference(loaded.metadata);
}

function fileArtifactReference(metadata: FileArtifactMetadata): FileArtifactReference {
  return Object.freeze({
    artifactId: metadata.artifactId,
    workId: metadata.workId,
    filename: metadata.filename,
    mimeType: metadata.mimeType,
    byteLength: metadata.byteLength,
    sha256: metadata.sha256,
  });
}

function existingStoreResult(
  loaded: { metadata: ImageMetadata; bytes: Uint8Array },
  expected: SourceIdentity,
): StoredImage {
  const identical =
    loaded.metadata.assetId === expected.assetId &&
    loaded.metadata.householdId === expected.householdId &&
    loaded.metadata.sourceSignalId === expected.sourceSignalId &&
    loaded.metadata.sourceMimeType === expected.sourceMimeType &&
    loaded.metadata.sourceDigest === expected.sourceDigest;
  if (!identical) {
    throw new ImageVaultError("asset_conflict", "Asset ID is already bound to different image evidence");
  }
  return {
    image: { assetId: loaded.metadata.assetId, mimeType: loaded.metadata.mimeType },
    byteLength: loaded.metadata.byteLength,
    expiresAt: loaded.metadata.expiresAt,
  };
}

function encryptEnvelope(key: Buffer, metadata: ImageMetadata, bytes: Uint8Array): Buffer {
  return encryptAuthenticatedEnvelope(key, imageEnvelopeMagic, metadata, bytes);
}

function encryptAuthenticatedEnvelope(
  key: Buffer,
  magic: Buffer,
  metadata: object,
  bytes: Uint8Array,
): Buffer {
  const metadataBytes = Buffer.from(JSON.stringify(metadata));
  if (metadataBytes.length < 2 || metadataBytes.length > envelopeOverheadLimit) {
    throw new ImageVaultError("corrupt", "Artifact metadata exceeds its storage limit");
  }
  const plaintext = Buffer.allocUnsafe(4 + metadataBytes.length + bytes.length);
  plaintext.writeUInt32BE(metadataBytes.length, 0);
  metadataBytes.copy(plaintext, 4);
  Buffer.from(bytes).copy(plaintext, 4 + metadataBytes.length);
  const nonce = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key, nonce);
  cipher.setAAD(magic);
  const encrypted = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  return Buffer.concat([magic, nonce, cipher.getAuthTag(), encrypted]);
}

function decryptEnvelope(key: Buffer, envelope: Uint8Array): { metadata: ImageMetadata; bytes: Uint8Array } {
  const loaded = decryptAuthenticatedEnvelope(key, imageEnvelopeMagic, envelope, MAX_IMAGE_BYTES, "Image");
  requireImageMetadata(loaded.metadata);
  if (
    loaded.bytes.length !== loaded.metadata.byteLength ||
    detectMimeType(loaded.bytes) !== loaded.metadata.mimeType
  ) {
    throw new ImageVaultError("corrupt", "Image payload is invalid");
  }
  return { metadata: loaded.metadata, bytes: loaded.bytes };
}

function decryptPdfEnvelope(key: Buffer, envelope: Uint8Array): { metadata: PdfMetadata; bytes: Uint8Array } {
  const loaded = decryptAuthenticatedEnvelope(key, pdfEnvelopeMagic, envelope, MAX_PDF_BYTES, "PDF");
  requirePdfMetadata(loaded.metadata);
  if (
    loaded.bytes.length !== loaded.metadata.byteLength ||
    !isPdf(loaded.bytes) ||
    createHash("sha256").update(loaded.bytes).digest("hex") !== loaded.metadata.sourceDigest
  ) {
    throw new ImageVaultError("corrupt", "PDF payload is invalid");
  }
  return { metadata: loaded.metadata, bytes: loaded.bytes };
}

function decryptFileArtifactEnvelope(
  key: Buffer,
  envelope: Uint8Array,
): { metadata: FileArtifactMetadata; bytes: Uint8Array } {
  const loaded = decryptAuthenticatedEnvelope(
    key,
    fileArtifactEnvelopeMagic,
    envelope,
    MAX_FILE_ARTIFACT_BYTES,
    "File artifact",
  );
  requireFileArtifactMetadata(loaded.metadata);
  if (
    loaded.bytes.length !== loaded.metadata.byteLength ||
    createHash("sha256").update(loaded.bytes).digest("hex") !== loaded.metadata.sha256
  ) {
    throw new ImageVaultError("corrupt", "File artifact payload is invalid");
  }
  return { metadata: loaded.metadata, bytes: loaded.bytes };
}

function decryptAuthenticatedEnvelope(
  key: Buffer,
  magic: Buffer,
  envelope: Uint8Array,
  maximumPayloadBytes: number,
  label: string,
): { metadata: unknown; bytes: Buffer } {
  const value = Buffer.from(envelope);
  if (value.length > maximumPayloadBytes + envelopeOverheadLimit) {
    throw new ImageVaultError("corrupt", `${label} envelope exceeds its storage limit`);
  }
  const headerBytes = magic.length + 12 + 16;
  if (value.length < headerBytes + 5 || !value.subarray(0, magic.length).equals(magic)) {
    throw new ImageVaultError("corrupt", `${label} envelope is invalid`);
  }
  const nonce = value.subarray(magic.length, magic.length + 12);
  const tag = value.subarray(magic.length + 12, headerBytes);
  const decipher = createDecipheriv("aes-256-gcm", key, nonce);
  decipher.setAAD(magic);
  decipher.setAuthTag(tag);
  let plaintext: Buffer;
  try {
    plaintext = Buffer.concat([decipher.update(value.subarray(headerBytes)), decipher.final()]);
  } catch (error) {
    throw new ImageVaultError("corrupt", `${label} envelope cannot be authenticated`, { cause: error });
  }
  const metadataLength = plaintext.readUInt32BE(0);
  if (
    metadataLength < 2 ||
    metadataLength > envelopeOverheadLimit ||
    4 + metadataLength >= plaintext.length
  ) {
    throw new ImageVaultError("corrupt", `${label} metadata is invalid`);
  }
  let metadata: unknown;
  try {
    metadata = JSON.parse(plaintext.subarray(4, 4 + metadataLength).toString("utf8"));
  } catch (error) {
    throw new ImageVaultError("corrupt", `${label} metadata is invalid`, { cause: error });
  }
  const bytes = plaintext.subarray(4 + metadataLength);
  return { metadata, bytes };
}

async function readBounded(
  assetPath: string,
  maximumPayloadBytes = MAX_IMAGE_BYTES,
  label = "Image",
): Promise<Buffer> {
  const file = await stat(assetPath);
  if (file.size > maximumPayloadBytes + envelopeOverheadLimit) {
    throw new ImageVaultError("corrupt", `${label} envelope exceeds its storage limit`);
  }
  return readFile(assetPath);
}

function detectMimeType(bytes: Uint8Array): AcceptedMimeType | null {
  const value = Buffer.from(bytes);
  if (value.length >= 3 && value[0] === 0xff && value[1] === 0xd8 && value[2] === 0xff) {
    return "image/jpeg";
  }
  if (
    value.length >= 8 &&
    value.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))
  ) {
    return "image/png";
  }
  if (
    value.length >= 12 &&
    value.toString("ascii", 0, 4) === "RIFF" &&
    value.toString("ascii", 8, 12) === "WEBP"
  ) {
    return "image/webp";
  }
  if (value.length >= 12 && value.toString("ascii", 4, 8) === "ftyp") {
    for (let offset = 8; offset + 4 <= Math.min(value.length, 64); offset += 4) {
      if (["heic", "heix", "hevc", "hevx"].includes(value.toString("ascii", offset, offset + 4))) {
        return "image/heic";
      }
    }
  }
  return null;
}

function requireUuid(value: string): void {
  if (!uuidPattern.test(value)) throw new ImageVaultError("invalid_image", "Image identity is invalid");
}

function requireFileArtifactUuid(value: string, label: string): void {
  if (!isCanonicalUuid(value)) {
    throw new ImageVaultError("invalid_artifact", `${label} identity is invalid`);
  }
}

function isCanonicalUuid(value: unknown): value is string {
  return typeof value === "string" && value === value.toLowerCase() && uuidPattern.test(value);
}

function requireFileArtifactCallId(value: string): string {
  const callId = canonicalFileArtifactCallId(value);
  if (callId === null) {
    throw new ImageVaultError(
      "invalid_artifact",
      "File artifact source call ID must be nonempty and at most 500 characters",
    );
  }
  return callId;
}

function canonicalFileArtifactCallId(value: unknown): string | null {
  if (typeof value !== "string" || !value.isWellFormed()) return null;
  const callId = value.trim();
  return callId.length >= 1 && callId.length <= 500 ? callId : null;
}

function requireFileArtifactFilename(value: string): string {
  const filename = canonicalFileArtifactFilename(value);
  if (filename === null) {
    throw new ImageVaultError("invalid_artifact", "File artifact filename is invalid");
  }
  return filename;
}

function canonicalFileArtifactFilename(value: unknown): string | null {
  if (typeof value !== "string" || !value.isWellFormed()) return null;
  const filename = value.normalize("NFC").trim();
  const hasControlCharacter = [...filename].some((character) => {
    const code = character.charCodeAt(0);
    return code < 32 || code === 127;
  });
  if (
    filename.length < 1 ||
    filename.length > 255 ||
    filename === "." ||
    filename === ".." ||
    filename.includes("/") ||
    filename.includes("\\") ||
    hasControlCharacter
  ) {
    return null;
  }
  return filename;
}

function requireFileArtifactMimeType(value: string): string {
  const mimeType = canonicalFileArtifactMimeType(value);
  if (mimeType === null) {
    throw new ImageVaultError("invalid_artifact", "File artifact MIME type is invalid");
  }
  return mimeType;
}

function canonicalFileArtifactMimeType(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const mimeType = value.trim().toLowerCase();
  if (mimeType.length < 3 || mimeType.length > 255) return null;
  const parts = mimeType.split("/");
  if (parts.length !== 2) return null;
  const mimeToken = /^[a-z0-9][a-z0-9!#$%&'*+\-.^_`|~]*$/u;
  return parts.every((part) => mimeToken.test(part)) ? mimeType : null;
}

function requireFileArtifactReference(value: FileArtifactReference): void {
  if (!value || typeof value !== "object") {
    throw new ImageVaultError("invalid_artifact", "File artifact reference is invalid");
  }
  const filename = canonicalFileArtifactFilename(value.filename);
  const mimeType = canonicalFileArtifactMimeType(value.mimeType);
  if (
    !isCanonicalUuid(value.artifactId) ||
    !isCanonicalUuid(value.workId) ||
    filename === null ||
    filename !== value.filename ||
    mimeType === null ||
    mimeType !== value.mimeType ||
    !Number.isSafeInteger(value.byteLength) ||
    value.byteLength < 1 ||
    value.byteLength > MAX_FILE_ARTIFACT_BYTES ||
    typeof value.sha256 !== "string" ||
    !/^[0-9a-f]{64}$/u.test(value.sha256)
  ) {
    throw new ImageVaultError("invalid_artifact", "File artifact reference is invalid");
  }
}

function requireImageMetadata(value: unknown): asserts value is ImageMetadata {
  if (!value || typeof value !== "object") throw new ImageVaultError("corrupt", "Image metadata is invalid");
  const metadata = value as Partial<ImageMetadata>;
  const validMime = ["image/jpeg", "image/png", "image/webp"].includes(metadata.mimeType ?? "");
  if (
    metadata.version !== 1 ||
    typeof metadata.assetId !== "string" ||
    !uuidPattern.test(metadata.assetId) ||
    typeof metadata.householdId !== "string" ||
    !uuidPattern.test(metadata.householdId) ||
    typeof metadata.sourceSignalId !== "string" ||
    !uuidPattern.test(metadata.sourceSignalId) ||
    !["image/jpeg", "image/png", "image/webp", "image/heic"].includes(metadata.sourceMimeType ?? "") ||
    typeof metadata.sourceDigest !== "string" ||
    !/^[a-f0-9]{64}$/.test(metadata.sourceDigest) ||
    !validMime ||
    !Number.isSafeInteger(metadata.byteLength) ||
    (metadata.byteLength ?? 0) < 1 ||
    (metadata.byteLength ?? 0) > MAX_IMAGE_BYTES ||
    typeof metadata.expiresAt !== "string" ||
    !Number.isFinite(Date.parse(metadata.expiresAt)) ||
    (metadata.retained !== undefined && typeof metadata.retained !== "boolean") ||
    (metadata.retentionClaims !== undefined &&
      (!Array.isArray(metadata.retentionClaims) ||
        metadata.retentionClaims.some(
          (claimId) => typeof claimId !== "string" || !uuidPattern.test(claimId),
        ) ||
        new Set(metadata.retentionClaims).size !== metadata.retentionClaims.length ||
        (metadata.retained === true) !== metadata.retentionClaims.length > 0))
  ) {
    throw new ImageVaultError("corrupt", "Image metadata is invalid");
  }
}

function requirePdfMetadata(value: unknown): asserts value is PdfMetadata {
  if (!value || typeof value !== "object") {
    throw new ImageVaultError("corrupt", "PDF metadata is invalid");
  }
  const metadata = value as Partial<PdfMetadata>;
  if (
    metadata.version !== 1 ||
    metadata.kind !== "pdf" ||
    typeof metadata.documentId !== "string" ||
    !uuidPattern.test(metadata.documentId) ||
    typeof metadata.householdId !== "string" ||
    !uuidPattern.test(metadata.householdId) ||
    typeof metadata.sourceSignalId !== "string" ||
    !uuidPattern.test(metadata.sourceSignalId) ||
    typeof metadata.filename !== "string" ||
    metadata.filename.length < 1 ||
    metadata.filename.length > 500 ||
    metadata.mimeType !== "application/pdf" ||
    typeof metadata.sourceDigest !== "string" ||
    !/^[a-f0-9]{64}$/.test(metadata.sourceDigest) ||
    !Number.isSafeInteger(metadata.byteLength) ||
    (metadata.byteLength ?? 0) < 1 ||
    (metadata.byteLength ?? 0) > MAX_PDF_BYTES ||
    (metadata.retained !== undefined && typeof metadata.retained !== "boolean") ||
    (metadata.retained === true
      ? metadata.discardAfter !== null
      : typeof metadata.discardAfter !== "string" || !Number.isFinite(Date.parse(metadata.discardAfter)))
  ) {
    throw new ImageVaultError("corrupt", "PDF metadata is invalid");
  }
}

function requireFileArtifactMetadata(value: unknown): asserts value is FileArtifactMetadata {
  if (!value || typeof value !== "object") {
    throw new ImageVaultError("corrupt", "File artifact metadata is invalid");
  }
  const metadata = value as Partial<FileArtifactMetadata>;
  const sourceCallId = canonicalFileArtifactCallId(metadata.sourceCallId);
  const filename = canonicalFileArtifactFilename(metadata.filename);
  const mimeType = canonicalFileArtifactMimeType(metadata.mimeType);
  if (
    metadata.version !== 1 ||
    metadata.kind !== "file_artifact" ||
    !isCanonicalUuid(metadata.artifactId) ||
    !isCanonicalUuid(metadata.householdId) ||
    !isCanonicalUuid(metadata.workId) ||
    sourceCallId === null ||
    sourceCallId !== metadata.sourceCallId ||
    filename === null ||
    filename !== metadata.filename ||
    mimeType === null ||
    mimeType !== metadata.mimeType ||
    !Number.isSafeInteger(metadata.byteLength) ||
    (metadata.byteLength ?? 0) < 1 ||
    (metadata.byteLength ?? 0) > MAX_FILE_ARTIFACT_BYTES ||
    typeof metadata.sha256 !== "string" ||
    !/^[0-9a-f]{64}$/u.test(metadata.sha256)
  ) {
    throw new ImageVaultError("corrupt", "File artifact metadata is invalid");
  }
}

function isPdf(bytes: Uint8Array): boolean {
  const value = Buffer.from(bytes);
  if (value.length < 12 || !value.subarray(0, 5).equals(Buffer.from("%PDF-"))) return false;
  const version = value.toString("ascii", 5, 8);
  if (!/^(?:1\.[0-9]|2\.0)$/.test(version)) return false;
  return value.subarray(Math.max(0, value.length - 2_048)).includes(Buffer.from("%%EOF"));
}

function requirePdfUuid(value: string, label: string): void {
  if (!uuidPattern.test(value)) {
    throw new ImageVaultError("invalid_document", `${label} identity is invalid`);
  }
}

function requirePdfFilename(value: string): string {
  const filename = value.trim();
  const hasControlCharacter = [...filename].some((character) => {
    const code = character.charCodeAt(0);
    return code < 32 || code === 127;
  });
  if (filename.length < 1 || filename.length > 500 || hasControlCharacter) {
    throw new ImageVaultError("invalid_document", "PDF filename is invalid");
  }
  return filename;
}

function requireTimestamp(value: string, label: string): string {
  if (!value || !Number.isFinite(Date.parse(value))) {
    throw new ImageVaultError("invalid_document", `${label} is invalid`);
  }
  return new Date(value).toISOString();
}

function isMissing(error: unknown): boolean {
  return Boolean(error && typeof error === "object" && "code" in error && error.code === "ENOENT");
}

function isAlreadyExists(error: unknown): boolean {
  return Boolean(error && typeof error === "object" && "code" in error && error.code === "EEXIST");
}
