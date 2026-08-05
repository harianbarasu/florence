import type { gmail_v1 } from "googleapis";
import { describe, expect, it, vi } from "vitest";
import {
  GmailAdapter,
  type GmailApiFactory,
  type GmailAttachment,
  GmailAttachmentContentError,
  GoogleSyncTokenExpiredError,
  parseGmailHistoryPage,
  parseGmailMessage,
  parseGmailPubSubPush,
  parseGoogleAdapterConfig,
} from "../../src/adapters/google/index.js";
import { jsonFixture } from "./fixture.js";

const CONFIG = parseGoogleAdapterConfig({
  clientId: "synthetic-client-id",
  clientSecret: "synthetic-client-secret",
  redirectUri: "https://florence.example.test/oauth/google/callback",
});

const GMAIL_MESSAGE = jsonFixture<gmail_v1.Schema$Message>("google/gmail-message.json");

function gmailFactory(overrides: Record<string, unknown> = {}): GmailApiFactory {
  return () =>
    ({
      getMessage: vi.fn(async () => ({ data: GMAIL_MESSAGE })),
      getAttachment: vi.fn(async () => ({
        data: { attachmentId: "gmail-attachment-001", size: 0, data: "" },
      })),
      listHistory: vi.fn(async () => ({ data: { historyId: "1000" } })),
      listMessages: vi.fn(async () => ({ data: {} })),
      watch: vi.fn(async () => ({
        data: { historyId: "1000", expiration: "1785945600000" },
      })),
      stop: vi.fn(async () => undefined),
      ...overrides,
    }) as ReturnType<GmailApiFactory>;
}

function attachmentDescriptor(bytes: Uint8Array, overrides: Partial<GmailAttachment> = {}): GmailAttachment {
  return {
    partId: "1",
    providerAttachmentId: "gmail-attachment-001",
    filename: "permission-slip.pdf",
    mimeType: "application/pdf",
    sizeBytes: bytes.byteLength,
    contentId: null,
    inline: false,
    embeddedDataBase64Url: null,
    ...overrides,
  };
}

function base64Url(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("base64url");
}

const PDF_BYTES = Buffer.from("%PDF-1.7\n%%EOF", "utf8");

describe("Gmail normalization", () => {
  it("parses headers, MIME alternatives, and attachment references as personal source data", () => {
    const message = parseGmailMessage(GMAIL_MESSAGE, "google-subject-001");

    expect(message).toMatchObject({
      source: "gmail",
      sourceScope: "personal",
      googleSubject: "google-subject-001",
      sourceKey: "google-subject-001:gmail-message-001",
      messageId: "gmail-message-001",
      threadId: "gmail-thread-001",
      historyId: "9001",
      internalDate: "2026-08-05T15:00:00.000Z",
      labelIds: ["INBOX", "UNREAD"],
      headers: {
        from: "School Office <office@school.example>",
        to: "Parent <parent@example.test>",
        subject: "Field trip form",
        messageId: "<fixture-001@school.example>",
      },
      body: {
        text: "Field trip form due Friday.",
        html: "<p>Field trip form due Friday.</p>",
      },
    });
    expect(message.rawHeaders["x-repeat"]).toEqual(["one", "two"]);
    expect(message.attachments).toEqual([
      {
        partId: "1",
        providerAttachmentId: "gmail-attachment-001",
        filename: "permission-slip.pdf",
        mimeType: "application/pdf",
        sizeBytes: 8192,
        contentId: null,
        inline: false,
        embeddedDataBase64Url: null,
      },
    ]);
  });

  it("normalizes specific history changes with stable IDs and deduplicates repeats", () => {
    const response: gmail_v1.Schema$ListHistoryResponse = {
      historyId: "1003",
      nextPageToken: "page-2",
      history: [
        {
          id: "1001",
          messagesAdded: [
            { message: { id: "message-1", threadId: "thread-1" } },
            { message: { id: "message-1", threadId: "thread-1" } },
          ],
          labelsAdded: [
            {
              message: { id: "message-1", threadId: "thread-1" },
              labelIds: ["UNREAD", "INBOX"],
            },
          ],
        },
        {
          id: "1002",
          messagesDeleted: [{ message: { id: "message-2", threadId: "thread-2" } }],
        },
      ],
    };

    const first = parseGmailHistoryPage(response, "google-subject-001");
    const second = parseGmailHistoryPage(response, "google-subject-001");
    expect(first).toMatchObject({ mailboxHistoryId: "1003", nextPageToken: "page-2" });
    expect(first.changes).toHaveLength(3);
    expect(first.changes.map((change) => change.changeType)).toEqual([
      "message.added",
      "labels.added",
      "message.deleted",
    ]);
    expect(first.changes.every((change) => change.sourceScope === "personal")).toBe(true);
    expect(first.changes.map((change) => change.providerEventId)).toEqual(
      second.changes.map((change) => change.providerEventId),
    );
    expect(first.changes[1]?.labelIds).toEqual(["INBOX", "UNREAD"]);
  });

  it("decodes Gmail's Pub/Sub invalidation hint without treating it as message content", () => {
    const data = Buffer.from(
      JSON.stringify({ emailAddress: "parent@example.test", historyId: "1004" }),
    ).toString("base64url");
    const event = parseGmailPubSubPush({
      message: {
        data,
        messageId: "pubsub-message-001",
        publishTime: "2026-08-05T16:00:00.000Z",
        deliveryAttempt: 2,
      },
      subscription: "projects/florence/subscriptions/gmail-push",
    });

    expect(event).toEqual({
      schemaVersion: 1,
      source: "gmail",
      sourceScope: "personal",
      providerEventId: "gmail-pubsub:projects/florence/subscriptions/gmail-push:pubsub-message-001",
      subscription: "projects/florence/subscriptions/gmail-push",
      mailboxEmail: "parent@example.test",
      historyId: "1004",
      publishedAt: "2026-08-05T16:00:00.000Z",
      deliveryAttempt: 2,
    });
  });
});

describe("Gmail API adapter", () => {
  it("requests either metadata or full messages without widening the mailbox identity", async () => {
    const getMessage = vi.fn(async () => ({ data: GMAIL_MESSAGE }));
    const adapter = new GmailAdapter(CONFIG, gmailFactory({ getMessage }));

    await adapter.getMessage({
      accessToken: "opaque-access-token",
      googleSubject: "google-subject-001",
      messageId: "gmail-message-001",
      format: "metadata",
    });
    await adapter.getMessage({
      accessToken: "opaque-access-token",
      googleSubject: "google-subject-001",
      messageId: "gmail-message-001",
    });

    expect(getMessage).toHaveBeenNthCalledWith(1, {
      userId: "me",
      id: "gmail-message-001",
      format: "metadata",
    });
    expect(getMessage).toHaveBeenNthCalledWith(2, {
      userId: "me",
      id: "gmail-message-001",
      format: "full",
    });
  });

  it("retrieves an external attachment from the exact message with a bounded provider response", async () => {
    const getAttachment = vi.fn(async () => ({
      data: {
        attachmentId: "gmail-attachment-001",
        size: PDF_BYTES.byteLength,
        data: base64Url(PDF_BYTES),
      },
    }));
    const adapter = new GmailAdapter(CONFIG, gmailFactory({ getAttachment }));

    await expect(
      adapter.retrieveAttachment({
        accessToken: "opaque-access-token",
        messageId: "gmail-message-001",
        attachment: attachmentDescriptor(PDF_BYTES, {
          mimeType: " Application/X-PDF ; name=permission-slip.pdf ",
        }),
      }),
    ).resolves.toEqual({
      kind: "file",
      mediaType: "application/pdf",
      filename: "permission-slip.pdf",
      sizeBytes: PDF_BYTES.byteLength,
      bytes: Uint8Array.from(PDF_BYTES),
    });
    expect(getAttachment).toHaveBeenCalledWith(
      {
        userId: "me",
        messageId: "gmail-message-001",
        id: "gmail-attachment-001",
      },
      { maxContentLength: 14_000_000 },
    );
  });

  it("reads canonical embedded data without making an attachment API request", async () => {
    const png = Uint8Array.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
    const getAttachment = vi.fn();
    const adapter = new GmailAdapter(CONFIG, gmailFactory({ getAttachment }));

    await expect(
      adapter.retrieveAttachment({
        accessToken: "opaque-access-token",
        messageId: "gmail-message-001",
        attachment: attachmentDescriptor(png, {
          providerAttachmentId: null,
          filename: "map.png",
          mimeType: "IMAGE/PNG",
          inline: true,
          embeddedDataBase64Url: base64Url(png),
        }),
      }),
    ).resolves.toEqual({
      kind: "image",
      mediaType: "image/png",
      filename: "map.png",
      sizeBytes: png.byteLength,
      bytes: png,
    });
    expect(getAttachment).not.toHaveBeenCalled();
  });

  it.each([
    ["application/pdf", PDF_BYTES, "file"],
    ["image/jpeg", Uint8Array.from([0xff, 0xd8, 0xff, 0xe0]), "image"],
    ["image/png", Uint8Array.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]), "image"],
    ["image/gif", Buffer.from("GIF89a", "ascii"), "image"],
    ["image/webp", Buffer.from("RIFF0000WEBP", "ascii"), "image"],
    ["text/plain; charset=UTF-8", Buffer.from("Family note", "utf8"), "file"],
    ['text/csv; charset="utf8"', Buffer.from("name,date\nAda,Friday", "utf8"), "file"],
    ["text/calendar; method=REQUEST", Buffer.from("BEGIN:VCALENDAR\nEND:VCALENDAR", "utf8"), "file"],
    ["text/x-markdown", Buffer.from("# Field trip", "utf8"), "file"],
  ] as const)("accepts policy-safe %s content after validation", async (mimeType, bytes, kind) => {
    const adapter = new GmailAdapter(CONFIG, gmailFactory());

    await expect(
      adapter.retrieveAttachment({
        accessToken: "opaque-access-token",
        messageId: "gmail-message-001",
        attachment: attachmentDescriptor(bytes, {
          providerAttachmentId: null,
          filename: "attachment",
          mimeType,
          embeddedDataBase64Url: base64Url(bytes),
        }),
      }),
    ).resolves.toMatchObject({ kind, sizeBytes: bytes.byteLength });
  });

  it.each([
    ["image/svg+xml", Buffer.from("<svg/>", "utf8")],
    ["text/html", Buffer.from("<p>field trip</p>", "utf8")],
    ["application/octet-stream", PDF_BYTES],
    ["application/zip", Uint8Array.from([0x50, 0x4b, 0x03, 0x04])],
    ["application/x-msdownload", Uint8Array.from([0x4d, 0x5a, 0x00, 0x00])],
    ["text/plain; charset=iso-8859-1", Buffer.from("note", "utf8")],
  ] as const)("rejects disallowed declared type %s before retrieval", async (mimeType, bytes) => {
    const getAttachment = vi.fn();
    const adapter = new GmailAdapter(CONFIG, gmailFactory({ getAttachment }));

    await expect(
      adapter.retrieveAttachment({
        accessToken: "opaque-access-token",
        messageId: "gmail-message-001",
        attachment: attachmentDescriptor(bytes, { mimeType }),
      }),
    ).rejects.toMatchObject({
      name: "GmailAttachmentContentError",
      reason: "unsupported_type",
      code: "permanent",
      retryable: false,
    });
    expect(getAttachment).not.toHaveBeenCalled();
  });

  it.each([
    ["invalid UTF-8", Uint8Array.from([0xc3, 0x28])],
    ["NUL bytes", Buffer.from("field\0trip", "utf8")],
    ["SVG disguised as text", Buffer.from('<?xml version="1.0"?><svg></svg>', "utf8")],
    ["HTML disguised as text", Buffer.from("<!doctype html><html></html>", "utf8")],
    ["an archive disguised as text", Uint8Array.from([0x50, 0x4b, 0x03, 0x04])],
    ["an executable disguised as text", Uint8Array.from([0x4d, 0x5a, 0x00, 0x00])],
    ["an executable script disguised as text", Buffer.from("#!/bin/sh\necho unsafe", "utf8")],
  ] as const)("rejects %s with a terminal content error", async (_label, bytes) => {
    const adapter = new GmailAdapter(CONFIG, gmailFactory());

    await expect(
      adapter.retrieveAttachment({
        accessToken: "opaque-access-token",
        messageId: "gmail-message-001",
        attachment: attachmentDescriptor(bytes, {
          providerAttachmentId: null,
          filename: "note.txt",
          mimeType: "text/plain; charset=utf-8",
          embeddedDataBase64Url: base64Url(bytes),
        }),
      }),
    ).rejects.toMatchObject({
      name: "GmailAttachmentContentError",
      reason: "invalid_content",
      code: "permanent",
      retryable: false,
    });
  });

  it("rejects mismatched magic bytes for an otherwise allowed binary type", async () => {
    const bytes = Buffer.from("not a PDF", "utf8");
    const adapter = new GmailAdapter(CONFIG, gmailFactory());

    await expect(
      adapter.retrieveAttachment({
        accessToken: "opaque-access-token",
        messageId: "gmail-message-001",
        attachment: attachmentDescriptor(bytes, {
          providerAttachmentId: null,
          embeddedDataBase64Url: base64Url(bytes),
        }),
      }),
    ).rejects.toBeInstanceOf(GmailAttachmentContentError);
  });

  it.each([
    ["padded", `${base64Url(Buffer.from("note", "utf8"))}=`],
    ["classic alphabet", "+w"],
    ["noncanonical pad bits", "AB"],
    ["invalid alphabet", "!!"],
  ])("rejects %s base64url", async (_label, encodedData) => {
    const expectedSize = encodedData.length === 2 ? 1 : 4;
    const adapter = new GmailAdapter(CONFIG, gmailFactory());

    await expect(
      adapter.retrieveAttachment({
        accessToken: "opaque-access-token",
        messageId: "gmail-message-001",
        attachment: attachmentDescriptor(new Uint8Array(expectedSize), {
          providerAttachmentId: null,
          filename: "note.txt",
          mimeType: "text/plain",
          embeddedDataBase64Url: encodedData,
        }),
      }),
    ).rejects.toMatchObject({ reason: "invalid_content", retryable: false });
  });

  it("rejects descriptors above 10 MiB before any provider call", async () => {
    const getAttachment = vi.fn();
    const adapter = new GmailAdapter(CONFIG, gmailFactory({ getAttachment }));

    await expect(
      adapter.retrieveAttachment({
        accessToken: "opaque-access-token",
        messageId: "gmail-message-001",
        attachment: attachmentDescriptor(PDF_BYTES, { sizeBytes: 10 * 1024 * 1024 + 1 }),
      }),
    ).rejects.toMatchObject({ reason: "too_large", status: 413, retryable: false });
    expect(getAttachment).not.toHaveBeenCalled();
  });

  it("rejects an encoded payload above the decoded 10 MiB ceiling", async () => {
    const adapter = new GmailAdapter(CONFIG, gmailFactory());

    await expect(
      adapter.retrieveAttachment({
        accessToken: "opaque-access-token",
        messageId: "gmail-message-001",
        attachment: attachmentDescriptor(new Uint8Array(10 * 1024 * 1024), {
          providerAttachmentId: null,
          embeddedDataBase64Url: "A".repeat(Math.ceil((10 * 1024 * 1024 * 4) / 3) + 1),
        }),
      }),
    ).rejects.toMatchObject({ reason: "too_large", retryable: false });
  });

  it.each([
    ["both embedded and external sources", { embeddedDataBase64Url: base64Url(PDF_BYTES) }],
    ["neither embedded nor external source", { providerAttachmentId: null }],
    ["a missing immutable part ID", { partId: null }],
  ] as const)("rejects %s", async (_label, overrides) => {
    const getAttachment = vi.fn();
    const adapter = new GmailAdapter(CONFIG, gmailFactory({ getAttachment }));

    await expect(
      adapter.retrieveAttachment({
        accessToken: "opaque-access-token",
        messageId: "gmail-message-001",
        attachment: attachmentDescriptor(PDF_BYTES, overrides),
      }),
    ).rejects.toMatchObject({ reason: "missing_reference", retryable: false });
    expect(getAttachment).not.toHaveBeenCalled();
  });

  it.each([
    [
      "a changed response ID",
      { attachmentId: "other-attachment", size: PDF_BYTES.byteLength, data: base64Url(PDF_BYTES) },
    ],
    [
      "a changed response size",
      { attachmentId: "gmail-attachment-001", size: PDF_BYTES.byteLength + 1, data: base64Url(PDF_BYTES) },
    ],
    [
      "changed decoded bytes",
      {
        attachmentId: "gmail-attachment-001",
        size: PDF_BYTES.byteLength,
        data: base64Url(Buffer.concat([PDF_BYTES, Buffer.from("x")])),
      },
    ],
    ["missing response data", { attachmentId: "gmail-attachment-001", size: PDF_BYTES.byteLength }],
    ["missing response size", { attachmentId: "gmail-attachment-001", data: base64Url(PDF_BYTES) }],
    ["a malformed response body", null],
  ] as const)("rejects %s against the pinned descriptor", async (_label, data) => {
    const adapter = new GmailAdapter(CONFIG, gmailFactory({ getAttachment: vi.fn(async () => ({ data })) }));

    await expect(
      adapter.retrieveAttachment({
        accessToken: "opaque-access-token",
        messageId: "gmail-message-001",
        attachment: attachmentDescriptor(PDF_BYTES),
      }),
    ).rejects.toMatchObject({ reason: "invalid_content", retryable: false });
  });

  it.each([
    [404, "not_found", false],
    [401, "unauthorized", false],
    [429, "rate_limited", true],
    [503, "transient", true],
  ] as const)(
    "maps attachment HTTP %i through Google provider semantics",
    async (status, code, retryable) => {
      const providerError = Object.assign(new Error("provider detail must stay private"), {
        response: { status },
      });
      const adapter = new GmailAdapter(
        CONFIG,
        gmailFactory({ getAttachment: vi.fn(async () => Promise.reject(providerError)) }),
      );

      const error = await adapter
        .retrieveAttachment({
          accessToken: "opaque-access-token",
          messageId: "gmail-message-001",
          attachment: attachmentDescriptor(PDF_BYTES),
        })
        .then(
          () => null,
          (failure: unknown) => failure,
        );
      expect(error).toMatchObject({ code, status, retryable });
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).not.toContain("provider detail must stay private");
    },
  );

  it("maps attachment network failures to retryable transient errors", async () => {
    const providerError = Object.assign(new Error("socket failed"), { code: "ECONNRESET" });
    const adapter = new GmailAdapter(
      CONFIG,
      gmailFactory({ getAttachment: vi.fn(async () => Promise.reject(providerError)) }),
    );

    await expect(
      adapter.retrieveAttachment({
        accessToken: "opaque-access-token",
        messageId: "gmail-message-001",
        attachment: attachmentDescriptor(PDF_BYTES),
      }),
    ).rejects.toMatchObject({ code: "transient", status: null, retryable: true });
  });

  it("maps an expired history cursor to a full-sync signal", async () => {
    const error = Object.assign(new Error("gone"), { response: { status: 404 } });
    const adapter = new GmailAdapter(
      CONFIG,
      gmailFactory({ listHistory: vi.fn(async () => Promise.reject(error)) }),
    );

    await expect(
      adapter.listHistoryPage({
        accessToken: "opaque-access-token",
        googleSubject: "google-subject-001",
        startHistoryId: "999",
      }),
    ).rejects.toBeInstanceOf(GoogleSyncTokenExpiredError);
  });

  it("creates and stops a filtered mailbox watch", async () => {
    const watch = vi.fn(async () => ({
      data: { historyId: "1000", expiration: "1785945600000" },
    }));
    const stop = vi.fn(async () => undefined);
    const adapter = new GmailAdapter(CONFIG, gmailFactory({ watch, stop }));

    await expect(
      adapter.startWatch({
        accessToken: "opaque-access-token",
        topicName: "projects/florence/topics/gmail-push",
        labelIds: ["INBOX"],
        labelFilterBehavior: "include",
      }),
    ).resolves.toEqual({ historyId: "1000", expiresAt: "2026-08-05T16:00:00.000Z" });
    expect(watch).toHaveBeenCalledWith({
      userId: "me",
      requestBody: {
        topicName: "projects/florence/topics/gmail-push",
        labelIds: ["INBOX"],
        labelFilterBehavior: "include",
      },
    });

    await adapter.stopWatch("opaque-access-token");
    expect(stop).toHaveBeenCalledWith({ userId: "me" });
  });
});
