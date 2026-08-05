import { createHmac } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import {
  LinqApiError,
  LinqClient,
  LinqWebhookVerificationError,
  normalizeLinqConsentCommand,
  parseLinqConfig,
  parseVerifiedLinqWebhook,
} from "../../src/adapters/linq/index.js";
import { readFixture } from "./fixture.js";

const NOW = new Date("2026-08-05T16:12:00.000Z");
const SIGNING_KEY = Buffer.alloc(32, 0x42);
const CONFIG = parseLinqConfig({
  apiKey: "synthetic-api-key",
  webhookSecret: `whsec_${SIGNING_KEY.toString("base64")}`,
});

function signedWebhook(rawBody: string, webhookId?: string, now = NOW) {
  const payload = JSON.parse(rawBody) as { event_id: string };
  const id = webhookId ?? payload.event_id;
  const timestamp = String(Math.floor(now.getTime() / 1_000));
  const signature = createHmac("sha256", SIGNING_KEY)
    .update(Buffer.concat([Buffer.from(`${id}.${timestamp}.`), Buffer.from(rawBody)]))
    .digest("base64");
  return {
    rawBody,
    headers: {
      "webhook-id": id,
      "webhook-timestamp": timestamp,
      "webhook-signature": `v1,invalid v1,${signature}`,
    },
    config: CONFIG,
    now,
  };
}

describe("Linq v3 webhook adapter", () => {
  it("normalizes a direct message and START command", () => {
    const raw = readFixture("linq/direct-message.json");
    const event = parseVerifiedLinqWebhook(signedWebhook(raw));

    expect(event).toMatchObject({
      eventType: "message.received",
      providerEventId: "evt-direct-001",
      dedupeKey: "linq:partner-fixture:evt-direct-001",
      scope: "direct",
      conversation: { id: "chat-direct-001", kind: "direct" },
      sender: { id: "handle-adult-1", handle: "+12025550101" },
      message: {
        id: "message-direct-001",
        text: "START",
        consentCommand: "start",
        attachments: [],
      },
    });
    expect(event?.occurredAt).toBe("2026-08-05T15:00:00.999Z");
  });

  it("normalizes group scope, attachments, replies, and STOP", () => {
    const raw = readFixture("linq/group-message.json");
    const event = parseVerifiedLinqWebhook(signedWebhook(raw));

    expect(event).toMatchObject({
      scope: "group",
      conversation: {
        id: "chat-group-001",
        kind: "group",
        knownParticipantHandles: ["+12025550100", "+12025550102"],
      },
      message: {
        text: "STOP",
        consentCommand: "stop",
        replyTo: { messageId: "message-parent-001", partIndex: 0 },
      },
    });
    if (event?.eventType !== "message.received") {
      throw new Error("expected message event");
    }
    expect(event.message.attachments).toEqual([
      {
        kind: "media",
        partIndex: 1,
        providerAttachmentId: "attachment-001",
        url: "https://cdn.example.test/attachment-001",
        mimeType: "application/pdf",
        filename: "field-trip.pdf",
        sizeBytes: 4096,
      },
      {
        kind: "link",
        partIndex: 2,
        providerAttachmentId: null,
        url: "https://school.example.test/form",
        mimeType: null,
        filename: null,
        sizeBytes: null,
      },
    ]);
  });

  it("normalizes reactions without inventing group/direct scope", () => {
    const raw = readFixture("linq/reaction-added.json");
    const event = parseVerifiedLinqWebhook(signedWebhook(raw));

    expect(event).toMatchObject({
      eventType: "reaction.added",
      scope: "unknown",
      conversation: { id: "chat-group-001", kind: "unknown" },
      reaction: {
        operation: "add",
        targetMessageId: "message-group-001",
        targetPartIndex: 0,
        type: "love",
      },
    });
  });

  it("rejects stale, invalid, and mismatched signed deliveries", () => {
    const raw = readFixture("linq/direct-message.json");
    const stale = signedWebhook(raw, undefined, new Date("2026-08-05T15:00:00.000Z"));
    expect(() => parseVerifiedLinqWebhook({ ...stale, now: NOW })).toThrow(LinqWebhookVerificationError);

    const invalid = signedWebhook(raw);
    invalid.headers["webhook-signature"] = "v1,AAAA";
    expect(() => parseVerifiedLinqWebhook(invalid)).toThrow(LinqWebhookVerificationError);

    const mismatched = signedWebhook(raw, "different-event-id");
    expect(() => parseVerifiedLinqWebhook(mismatched)).toThrow("webhook header and payload event IDs differ");
  });

  it("acknowledges unsupported event types by returning null", () => {
    const payload = JSON.parse(readFixture("linq/direct-message.json")) as Record<string, unknown>;
    payload.event_type = "message.delivered";
    const raw = JSON.stringify(payload);
    expect(parseVerifiedLinqWebhook(signedWebhook(raw))).toBeNull();
  });

  it("normalizes only complete consent keywords", () => {
    expect(normalizeLinqConsentCommand(" Unsubscribe ")).toBe("stop");
    expect(normalizeLinqConsentCommand("resume")).toBe("start");
    expect(normalizeLinqConsentCommand("please stop")).toBeNull();
  });
});

describe("Linq outbound adapter", () => {
  it("retrieves and normalizes the active participant set before binding a group", async () => {
    const request = vi.fn(async (_url: string | URL, _init?: RequestInit) => ({
      ok: true,
      status: 200,
      json: async () => ({
        id: "chat-group-001",
        is_group: true,
        display_name: "Family",
        service: "iMessage",
        health_status: { status: "HEALTHY" },
        handles: [
          { handle: "+16462350806", is_me: true, status: "active" },
          { handle: "+12025550101", is_me: false, status: "active" },
          { handle: "+12025550102", is_me: false, status: "active" },
          { handle: "+12025550999", is_me: false, status: "removed" },
        ],
      }),
    }));
    const client = new LinqClient(CONFIG, request);

    await expect(client.getChat("chat-group-001")).resolves.toEqual({
      id: "chat-group-001",
      isGroup: true,
      displayName: "Family",
      service: "iMessage",
      healthStatus: "HEALTHY",
      activeHandles: ["+16462350806", "+12025550101", "+12025550102"],
      selfHandles: ["+16462350806"],
      participantHandles: ["+12025550101", "+12025550102"],
    });
    expect(request.mock.calls[0]?.[0]).toBe("https://api.linqapp.com/api/partner/v3/chats/chat-group-001");
  });

  it("fails closed when a chat lookup does not identify Florence's own handle", async () => {
    const client = new LinqClient(
      CONFIG,
      vi.fn(async () => ({
        ok: true,
        status: 200,
        json: async () => ({
          id: "chat-group-001",
          is_group: true,
          health_status: { status: "HEALTHY" },
          handles: [
            { handle: "+12025550101", status: "active" },
            { handle: "+12025550102", status: "active" },
          ],
        }),
      })),
    );

    await expect(client.getChat("chat-group-001")).rejects.toMatchObject({
      name: "LinqApiError",
      retryable: false,
    });
  });

  it("requires an idempotency key in the provider message body", async () => {
    const request = vi.fn(async (_url: string | URL, _init?: RequestInit) => ({
      ok: true,
      status: 200,
      json: async () => ({ id: "outbound-message-001" }),
    }));
    const client = new LinqClient(CONFIG, request);

    await expect(
      client.sendText({
        chatId: "chat-group-001",
        text: "A neutral reminder",
        idempotencyKey: "effect-001",
        replyTo: { messageId: "message-group-001", partIndex: 0 },
      }),
    ).resolves.toEqual({
      provider: "linq",
      providerMessageId: "outbound-message-001",
      chatId: "chat-group-001",
      idempotencyKey: "effect-001",
    });

    const [url, init] = request.mock.calls[0] ?? [];
    expect(url).toBe("https://api.linqapp.com/api/partner/v3/chats/chat-group-001/messages");
    expect(JSON.parse(String(init?.body))).toEqual({
      message: {
        parts: [{ type: "text", value: "A neutral reminder" }],
        idempotency_key: "effect-001",
        reply_to: { message_id: "message-group-001", part_index: 0 },
      },
    });
  });

  it("maps retryable response status without returning response content", async () => {
    const client = new LinqClient(
      CONFIG,
      vi.fn(async () => ({ ok: false, status: 503, json: async () => ({}) })),
    );

    const error = await client
      .sendText({ chatId: "chat-1", text: "hello", idempotencyKey: "effect-002" })
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(LinqApiError);
    expect(error).toMatchObject({ status: 503, retryable: true });
  });
});
