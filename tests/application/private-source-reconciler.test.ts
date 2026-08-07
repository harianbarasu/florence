import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { PrivateSourceReconciler } from "../../src/application/private-source-reconciler.js";
import type { Database } from "../../src/db/client.js";
import { gmailThreadCaseDigest, type JsonObject } from "../../src/modules/sources/index.js";
import { SecretBox } from "../../src/shared/crypto.js";

const OWNER_ID = "10000000-0000-4000-8000-000000000001";
const INTEGRATION_ID = "10000000-0000-4000-8000-000000000002";
const MAIL_REVISION_ID = "10000000-0000-4000-8000-000000000003";
const ATTACHMENT_REVISION_ID = "10000000-0000-4000-8000-000000000004";
const SOURCE_BLOB_ID = "10000000-0000-4000-8000-000000000005";
const OCCURRED_AT = new Date("2026-08-07T12:00:00.000Z");

describe("private source attachment liveness", () => {
  it("does not let an empty decorative inline image strand its Gmail thread", async () => {
    const secretBox = testSecretBox();
    const database = compileDatabase(secretBox, {
      mail: {
        threadId: "thread-one",
        bodyRetrieval: "now",
        text: "Field trip permission slips are due Friday.",
        attachments: [
          {
            partId: "inline-logo",
            filename: "school-logo.png",
            mimeType: "image/png",
            size: 512,
            inline: true,
          },
        ],
      },
      attachments: [],
    });

    const compiled = await new PrivateSourceReconciler(database, secretBox, {
      rawRetentionDays: 30,
    }).compile({
      anchorSourceRevisionId: MAIL_REVISION_ID,
      requestedAt: "2026-08-07T13:00:00.000Z",
    });

    expect(compiled.kind).toBe("ready");
    if (compiled.kind !== "ready") return;
    expect(compiled.evidence.map((item) => item.sourceRevisionId)).toEqual([MAIL_REVISION_ID]);
    expect(compiled.images).toEqual([]);
  });

  it("passes a bounded material image through the compiled governed-worker input", async () => {
    const secretBox = testSecretBox();
    const imageBytes = Buffer.from("bounded-image-evidence", "utf8");
    const database = compileDatabase(secretBox, {
      mail: mailContent({
        partId: "permission-slip",
        filename: "permission-slip.png",
        mimeType: "image/png",
        size: imageBytes.length,
        inline: false,
      }),
      attachments: [
        {
          id: ATTACHMENT_REVISION_ID,
          data: imageManifest({
            partId: "permission-slip",
            filename: "permission-slip.png",
            size: imageBytes.length,
            inline: false,
          }),
        },
      ],
      blobs: [imageBlob(secretBox, ATTACHMENT_REVISION_ID, "permission-slip", imageBytes)],
    });

    const compiled = await compile(database, secretBox);

    expect(compiled.kind).toBe("ready");
    if (compiled.kind !== "ready") return;
    expect(compiled.evidence.map((item) => item.sourceRevisionId)).toEqual([
      MAIL_REVISION_ID,
      ATTACHMENT_REVISION_ID,
    ]);
    expect(compiled.images).toEqual([
      {
        sourceRevisionId: ATTACHMENT_REVISION_ID,
        mimeType: "image/png",
        dataBase64: imageBytes.toString("base64"),
        sha256: createHash("sha256").update(imageBytes).digest("hex"),
      },
    ]);
  });

  it.each([
    {
      label: "unsupported attachment",
      inventory: {
        partId: "archive",
        filename: "records.zip",
        mimeType: "application/zip",
        size: 1_024,
        inline: false,
      },
      data: {
        parentSourceRevisionId: MAIL_REVISION_ID,
        partId: "archive",
        filename: "records.zip",
        declaredMime: "application/zip",
        detectedMime: "application/zip",
        size: 1_024,
        inline: false,
        kind: "unsupported",
        text: "",
        metadata: { bytes: 1_024, unsupportedExtraction: true },
        untrustedEvidence: true,
      },
    },
    {
      label: "oversize omitted attachment",
      inventory: {
        partId: "large-scan",
        filename: "large-scan.pdf",
        mimeType: "application/pdf",
        size: 16 * 1024 * 1024,
        inline: false,
      },
      data: {
        parentSourceRevisionId: MAIL_REVISION_ID,
        partId: "large-scan",
        filename: "large-scan.pdf",
        declaredMime: "application/pdf",
        size: 16 * 1024 * 1024,
        inline: false,
        kind: "omitted",
        text: "",
        metadata: {
          bytes: 16 * 1024 * 1024,
          omissionReason: "attachment_exceeds_15_mib_processing_limit",
        },
        untrustedEvidence: true,
      },
    },
    {
      label: "scanned PDF without extracted words",
      inventory: {
        partId: "scan",
        filename: "scan.pdf",
        mimeType: "application/pdf",
        size: 2_048,
        inline: false,
      },
      data: {
        parentSourceRevisionId: MAIL_REVISION_ID,
        partId: "scan",
        filename: "scan.pdf",
        declaredMime: "application/pdf",
        detectedMime: "application/pdf",
        size: 2_048,
        inline: false,
        kind: "pdf",
        text: "[Page 1]\n\n[Page 2]\n",
        metadata: { bytes: 2_048, pages: 2 },
        untrustedEvidence: true,
      },
    },
  ])("does not strand or claim knowledge of a $label", async ({ inventory, data }) => {
    const secretBox = testSecretBox();
    const database = compileDatabase(secretBox, {
      mail: mailContent(inventory),
      attachments: [{ id: ATTACHMENT_REVISION_ID, data }],
    });

    const compiled = await compile(database, secretBox);

    expect(compiled.kind).toBe("ready");
    if (compiled.kind !== "ready") return;
    expect(compiled.evidence.map((item) => item.sourceRevisionId)).toEqual([MAIL_REVISION_ID]);
    expect(compiled.images).toEqual([]);
  });

  it("does not claim knowledge of a material image when no bounded blob is available", async () => {
    const secretBox = testSecretBox();
    const database = compileDatabase(secretBox, {
      mail: mailContent({
        partId: "permission-slip",
        filename: "permission-slip.png",
        mimeType: "image/png",
        size: 6 * 1024 * 1024,
        inline: false,
      }),
      attachments: [
        {
          id: ATTACHMENT_REVISION_ID,
          data: imageManifest({
            partId: "permission-slip",
            filename: "permission-slip.png",
            size: 6 * 1024 * 1024,
            inline: false,
          }),
        },
      ],
    });

    const compiled = await compile(database, secretBox);

    expect(compiled.kind).toBe("ready");
    if (compiled.kind !== "ready") return;
    expect(compiled.evidence.map((item) => item.sourceRevisionId)).toEqual([MAIL_REVISION_ID]);
    expect(compiled.images).toEqual([]);
  });

  it("keeps withheld attachment revisions in the digest fence", async () => {
    const secretBox = testSecretBox();
    const input = {
      mail: mailContent({
        partId: "archive",
        filename: "records.zip",
        mimeType: "application/zip",
        size: 1_024,
        inline: false,
      }),
      attachments: [
        {
          id: ATTACHMENT_REVISION_ID,
          data: {
            parentSourceRevisionId: MAIL_REVISION_ID,
            partId: "archive",
            filename: "records.zip",
            declaredMime: "application/zip",
            detectedMime: "application/zip",
            size: 1_024,
            inline: false,
            kind: "unsupported",
            text: "",
            metadata: { bytes: 1_024, unsupportedExtraction: true },
            untrustedEvidence: true,
          },
        },
      ],
    } as const;
    const first = await compile(
      compileDatabase(secretBox, { ...input, attachmentContentDigest: "1".repeat(64) }),
      secretBox,
    );
    const second = await compile(
      compileDatabase(secretBox, { ...input, attachmentContentDigest: "2".repeat(64) }),
      secretBox,
    );

    expect(first.kind).toBe("ready");
    expect(second.kind).toBe("ready");
    if (first.kind !== "ready" || second.kind !== "ready") return;
    expect(first.frontierDigest).not.toBe(second.frontierDigest);
  });
});

function testSecretBox(): SecretBox {
  const key = Buffer.alloc(32, 7).toString("base64");
  return new SecretBox("test", JSON.stringify({ test: key }));
}

function compileDatabase(
  secretBox: SecretBox,
  input: {
    readonly mail: JsonObject;
    readonly attachments: readonly { readonly id: string; readonly data: JsonObject }[];
    readonly blobs?: readonly ReturnType<typeof imageBlob>[];
    readonly attachmentContentDigest?: string;
  },
): Database {
  const correlationDigest = gmailThreadCaseDigest({
    integrationId: INTEGRATION_ID,
    threadId: "thread-one",
  });
  const mailRow = sourceRow(secretBox, {
    id: MAIL_REVISION_ID,
    provider: "gmail",
    objectKind: "mail_message",
    correlationDigest,
    data: input.mail,
  });
  const attachmentRows = input.attachments.map((attachment) =>
    sourceRow(secretBox, {
      id: attachment.id,
      provider: "gmail.attachment",
      objectKind: "attachment_manifest",
      correlationDigest,
      data: attachment.data,
      ...(input.attachmentContentDigest ? { contentDigest: input.attachmentContentDigest } : {}),
    }),
  );

  return (async (strings: TemplateStringsArray, ...values: unknown[]) => {
    const sql = strings.join("?");
    if (sql.includes("join people person")) return [mailRow];
    if (sql.includes("object.provider = 'gmail' and object.object_kind = 'mail_message'")) {
      return [mailRow];
    }
    if (sql.includes("from source_blobs blob")) {
      return (input.blobs ?? []).filter((blob) => values.includes(blob.source_revision_id));
    }
    if (sql.includes("object.provider = 'gmail.attachment'")) return attachmentRows;
    if (sql.includes("select exists") && sql.includes("capability = 'calendar'")) {
      return [{ active: false }];
    }
    throw new Error(`Unexpected private-source compile query: ${sql}`);
  }) as unknown as Database;
}

function sourceRow(
  secretBox: SecretBox,
  input: {
    readonly id: string;
    readonly provider: string;
    readonly objectKind: "mail_message" | "attachment_manifest";
    readonly correlationDigest: string;
    readonly data: JsonObject;
    readonly contentDigest?: string;
  },
) {
  const envelope = {
    artifactKind: input.objectKind,
    origin: { system: input.provider, remoteObjectId: input.id },
    data: input.data,
  };
  return {
    id: input.id,
    owner_person_id: OWNER_ID,
    integration_id: INTEGRATION_ID,
    integration_control_epoch: 1,
    provider: input.provider,
    object_kind: input.objectKind,
    correlation_digest: input.correlationDigest,
    content_digest: input.contentDigest ?? input.id.replaceAll("-", "").padEnd(64, "0").slice(0, 64),
    content_ciphertext: Buffer.from(
      JSON.stringify(
        secretBox.encrypt(JSON.stringify(envelope), `florence:source-revision:${input.id}:content`),
      ),
      "utf8",
    ),
    occurred_at: OCCURRED_AT,
  };
}

async function compile(database: Database, secretBox: SecretBox) {
  return new PrivateSourceReconciler(database, secretBox, { rawRetentionDays: 30 }).compile({
    anchorSourceRevisionId: MAIL_REVISION_ID,
    requestedAt: "2026-08-07T13:00:00.000Z",
  });
}

function mailContent(attachment: JsonObject): JsonObject {
  return {
    threadId: "thread-one",
    bodyRetrieval: "now",
    text: "Field trip permission slips are due Friday.",
    attachments: [attachment],
  };
}

function imageManifest(input: {
  readonly partId: string;
  readonly filename: string;
  readonly size: number;
  readonly inline: boolean;
}): JsonObject {
  return {
    parentSourceRevisionId: MAIL_REVISION_ID,
    partId: input.partId,
    filename: input.filename,
    declaredMime: "image/png",
    detectedMime: "image/png",
    size: input.size,
    inline: input.inline,
    kind: "image",
    text: "",
    metadata: { bytes: input.size, requiresVisionInterpretation: true },
    untrustedEvidence: true,
  };
}

function imageBlob(secretBox: SecretBox, sourceRevisionId: string, partId: string, bytes: Buffer) {
  return {
    id: SOURCE_BLOB_ID,
    source_revision_id: sourceRevisionId,
    blob_kind: `gmail_attachment:${partId}`,
    mime_type: "image/png",
    content_digest: createHash("sha256").update(bytes).digest("hex"),
    byte_length: bytes.length,
    ciphertext: Buffer.from(
      JSON.stringify(secretBox.encrypt(bytes, `florence:source-blob:${SOURCE_BLOB_ID}:bytes`)),
      "utf8",
    ),
    retention_until: new Date("2026-09-07T00:00:00.000Z"),
  };
}
