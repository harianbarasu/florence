import type { gmail_v1 } from "googleapis";
import { describe, expect, it, vi } from "vitest";
import {
  GmailAdapter,
  type GmailApiFactory,
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
      listHistory: vi.fn(async () => ({ data: { historyId: "1000" } })),
      listMessages: vi.fn(async () => ({ data: {} })),
      watch: vi.fn(async () => ({
        data: { historyId: "1000", expiration: "1785945600000" },
      })),
      stop: vi.fn(async () => undefined),
      ...overrides,
    }) as ReturnType<GmailApiFactory>;
}

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
