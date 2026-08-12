import { createCipheriv, createDecipheriv, createHash, randomBytes, randomUUID } from "node:crypto";
import type { ImageReference } from "@florence/contracts";

export const MAX_IMAGE_BYTES = 20 * 1024 * 1024;
export const DEFAULT_IMAGE_RETENTION_MS = 30 * 24 * 60 * 60 * 1_000;
const maximumRetentionMs = 365 * 24 * 60 * 60 * 1_000;
const envelopeOverheadLimit = 16 * 1024;
const envelopeMagic = Buffer.from("FIV1");
const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

type StoredMimeType = Exclude<ImageReference["mimeType"], "image/heic">;
type AcceptedMimeType = ImageReference["mimeType"];

export type StoredImage = {
  image: { assetId: string; mimeType: StoredMimeType };
  byteLength: number;
  expiresAt: string;
};

export type EncryptedImageAssetRecord = {
  assetId: string;
  householdId: string;
  signalId: string;
  expiresAt: string;
  envelope: Uint8Array;
};

export interface EncryptedImageStore {
  readImageAsset(assetId: string): Promise<EncryptedImageAssetRecord | null>;
  insertImageAsset(record: EncryptedImageAssetRecord): Promise<boolean>;
  deleteImageAsset(assetId: string): Promise<void>;
  deleteExpiredImageAssets(now: Date): Promise<number>;
}

export type EncryptedImageVaultOptions = {
  store: EncryptedImageStore;
  encryptionKey: Uint8Array;
  retentionMs?: number;
};

export type ImageVaultErrorCode =
  | "invalid_configuration"
  | "invalid_image"
  | "image_too_large"
  | "unsupported_image"
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
  readonly #store: EncryptedImageStore;
  readonly #key: Buffer;
  readonly #retentionMs: number;

  constructor(options: EncryptedImageVaultOptions) {
    if (options.encryptionKey.byteLength !== 32) {
      throw new ImageVaultError("invalid_configuration", "Image vault key must contain 32 bytes");
    }
    const retentionMs = options.retentionMs ?? DEFAULT_IMAGE_RETENTION_MS;
    if (!Number.isSafeInteger(retentionMs) || retentionMs < 1 || retentionMs > maximumRetentionMs) {
      throw new ImageVaultError("invalid_configuration", "Image retention must be between 1 ms and 365 days");
    }
    this.#store = options.store;
    this.#key = Buffer.from(options.encryptionKey);
    this.#retentionMs = retentionMs;
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

    const bytes = sourceBytes;
    let mimeType: StoredMimeType;
    if (detected === "image/heic") {
      throw new ImageVaultError("unsupported_image", "HEIC is not supported in the pilot");
    } else {
      mimeType = detected;
    }
    if (bytes.length === 0 || bytes.length > MAX_IMAGE_BYTES) {
      throw new ImageVaultError("image_too_large", "Normalized image exceeds Florence's 20 MB limit");
    }

    const expiresAt = new Date(Date.now() + this.#retentionMs);
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
    const inserted = await this.#store.insertImageAsset({
      assetId,
      householdId: metadata.householdId,
      signalId: metadata.sourceSignalId,
      expiresAt: metadata.expiresAt,
      envelope: encrypted,
    });
    if (!inserted) {
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
    if (Date.parse(loaded.metadata.expiresAt) <= Date.now()) {
      await this.#store.deleteImageAsset(input.image.assetId);
      throw new ImageVaultError("expired", "Image retention has expired");
    }
    return { mimeType: loaded.metadata.mimeType, bytes: loaded.bytes };
  }

  async delete(input: { householdId: string; signalId: string; image: ImageReference }): Promise<void> {
    await this.#loadAuthorized(input);
    await this.#store.deleteImageAsset(input.image.assetId);
  }

  async purgeExpired(now = new Date()): Promise<number> {
    if (!Number.isFinite(now.getTime())) {
      throw new ImageVaultError("invalid_configuration", "Image purge time must be valid");
    }
    return this.#store.deleteExpiredImageAssets(now);
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

  async #load(assetId: string): Promise<{ metadata: ImageMetadata; bytes: Uint8Array } | null> {
    try {
      const record = await this.#store.readImageAsset(assetId);
      if (!record) return null;
      requireEncryptedRecord(record, assetId);
      if (record.envelope.byteLength > MAX_IMAGE_BYTES + envelopeOverheadLimit) {
        throw new ImageVaultError("corrupt", "Image envelope exceeds its storage limit");
      }
      const loaded = decryptEnvelope(this.#key, record.envelope);
      if (
        loaded.metadata.assetId !== record.assetId ||
        loaded.metadata.householdId !== record.householdId ||
        loaded.metadata.sourceSignalId !== record.signalId ||
        loaded.metadata.expiresAt !== record.expiresAt
      ) {
        throw new ImageVaultError("corrupt", "Image storage record does not match its envelope");
      }
      return loaded;
    } catch (error) {
      if (error instanceof ImageVaultError) throw error;
      throw new ImageVaultError("corrupt", "Image envelope cannot be authenticated", { cause: error });
    }
  }
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

function encryptEnvelope(key: Buffer, metadata: ImageMetadata, bytes: Buffer): Buffer {
  const metadataBytes = Buffer.from(JSON.stringify(metadata));
  const plaintext = Buffer.allocUnsafe(4 + metadataBytes.length + bytes.length);
  plaintext.writeUInt32BE(metadataBytes.length, 0);
  metadataBytes.copy(plaintext, 4);
  bytes.copy(plaintext, 4 + metadataBytes.length);
  const nonce = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key, nonce);
  cipher.setAAD(envelopeMagic);
  const encrypted = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  return Buffer.concat([envelopeMagic, nonce, cipher.getAuthTag(), encrypted]);
}

function decryptEnvelope(key: Buffer, envelope: Uint8Array): { metadata: ImageMetadata; bytes: Uint8Array } {
  const value = Buffer.from(envelope);
  const headerBytes = envelopeMagic.length + 12 + 16;
  if (value.length < headerBytes + 5 || !value.subarray(0, envelopeMagic.length).equals(envelopeMagic)) {
    throw new ImageVaultError("corrupt", "Image envelope is invalid");
  }
  const nonce = value.subarray(envelopeMagic.length, envelopeMagic.length + 12);
  const tag = value.subarray(envelopeMagic.length + 12, headerBytes);
  const decipher = createDecipheriv("aes-256-gcm", key, nonce);
  decipher.setAAD(envelopeMagic);
  decipher.setAuthTag(tag);
  let plaintext: Buffer;
  try {
    plaintext = Buffer.concat([decipher.update(value.subarray(headerBytes)), decipher.final()]);
  } catch (error) {
    throw new ImageVaultError("corrupt", "Image envelope cannot be authenticated", { cause: error });
  }
  const metadataLength = plaintext.readUInt32BE(0);
  if (
    metadataLength < 2 ||
    metadataLength > envelopeOverheadLimit ||
    4 + metadataLength >= plaintext.length
  ) {
    throw new ImageVaultError("corrupt", "Image metadata is invalid");
  }
  let metadata: unknown;
  try {
    metadata = JSON.parse(plaintext.subarray(4, 4 + metadataLength).toString("utf8"));
  } catch (error) {
    throw new ImageVaultError("corrupt", "Image metadata is invalid", { cause: error });
  }
  requireMetadata(metadata);
  const bytes = plaintext.subarray(4 + metadataLength);
  if (bytes.length !== metadata.byteLength || detectMimeType(bytes) !== metadata.mimeType) {
    throw new ImageVaultError("corrupt", "Image payload is invalid");
  }
  return { metadata, bytes };
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

function requireMetadata(value: unknown): asserts value is ImageMetadata {
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
    !Number.isFinite(Date.parse(metadata.expiresAt))
  ) {
    throw new ImageVaultError("corrupt", "Image metadata is invalid");
  }
}

function requireEncryptedRecord(
  value: EncryptedImageAssetRecord,
  requestedAssetId: string,
): asserts value is EncryptedImageAssetRecord {
  if (
    !value ||
    typeof value !== "object" ||
    typeof value.assetId !== "string" ||
    !uuidPattern.test(value.assetId) ||
    value.assetId !== requestedAssetId ||
    typeof value.householdId !== "string" ||
    !uuidPattern.test(value.householdId) ||
    typeof value.signalId !== "string" ||
    !uuidPattern.test(value.signalId) ||
    typeof value.expiresAt !== "string" ||
    !Number.isFinite(Date.parse(value.expiresAt)) ||
    !(value.envelope instanceof Uint8Array)
  ) {
    throw new ImageVaultError("corrupt", "Image storage record is invalid");
  }
}
