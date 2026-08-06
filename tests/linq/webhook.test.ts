import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";
import { unwrapLinqWebhook } from "../../src/adapters/linq/index.js";

const now = new Date("2026-02-05T19:52:20.000Z");
const secretBytes = Buffer.alloc(32, 7);
const webhookSecret = `whsec_${secretBytes.toString("base64")}`;

function signedWebhook(payload: unknown, rawOverride?: string) {
  const rawBody = rawOverride ?? JSON.stringify(payload);
  const eventId =
    typeof payload === "object" && payload !== null && "event_id" in payload
      ? String(payload.event_id)
      : "event-invalid-json";
  const timestamp = String(Math.floor(now.getTime() / 1000));
  const signature = createHmac("sha256", secretBytes)
    .update(`${eventId}.${timestamp}.`)
    .update(rawBody)
    .digest("base64");
  return {
    rawBody,
    headers: {
      "webhook-id": eventId,
      "webhook-timestamp": timestamp,
      "webhook-signature": `v1,${signature}`,
    },
  };
}

const self = {
  id: "self-handle",
  handle: "+16462350806",
  is_me: true,
  joined_at: "2026-01-01T10:00:00Z",
  left_at: null,
  service: "iMessage",
  status: "active",
};

const parent = {
  id: "parent-handle",
  handle: "+14155550100",
  is_me: false,
  joined_at: "2026-01-02T10:00:00Z",
  left_at: null,
  service: "iMessage",
  status: "active",
};

const chat = {
  id: "chat-one",
  is_group: true,
  owner_handle: self,
  health_status: {
    status: "HEALTHY",
    updated_at: "2026-02-05T19:50:00Z",
  },
};

function envelope(eventType: string, data: unknown, eventId = `event-${eventType}`) {
  return {
    api_version: "v3",
    webhook_version: "2026-02-03",
    event_type: eventType,
    event_id: eventId,
    created_at: "2026-02-05T19:52:18.101373886Z",
    trace_id: `trace-${eventType}`,
    partner_id: "partner-one",
    data,
  };
}

describe("unwrapLinqWebhook", () => {
  it("verifies raw bytes before parsing and normalizes message content without retaining signed URLs", () => {
    const payload = envelope("message.received", {
      chat,
      id: "message-one",
      direction: "inbound",
      sender_handle: parent,
      parts: [
        { type: "text", value: "Pickup moved to 3:15." },
        {
          type: "media",
          id: "attachment-one",
          filename: "schedule.pdf",
          mime_type: "application/pdf",
          size_bytes: 1234,
          url: "https://cdn.linqapp.com/short-lived-secret",
        },
      ],
      reply_to: { message_id: "message-parent", part_index: 1 },
      sent_at: "2026-02-05T19:52:17.219Z",
      service: "iMessage",
    });
    const signed = signedWebhook(payload);

    const result = unwrapLinqWebhook({ ...signed, webhookSecret, receivedAt: now });

    expect(result.eventType).toBe("linq.message.received");
    if (result.eventType !== "linq.message.received") {
      throw new Error("Expected a message event");
    }
    expect(result.channel).toEqual({ providerChatId: "chat-one", kind: "group" });
    expect(result.message.parts).toEqual([
      { kind: "text", text: "Pickup moved to 3:15." },
      {
        kind: "attachment",
        providerAttachmentId: "attachment-one",
        filename: "schedule.pdf",
        mediaType: "application/pdf",
        sizeBytes: 1234,
      },
    ]);
    expect(JSON.stringify(result)).not.toContain("short-lived-secret");
  });

  it("supports edits, reactions, participant changes, and sent/failed receipts", () => {
    const cases = [
      envelope("message.edited", {
        chat,
        id: "message-one",
        direction: "inbound",
        sender_handle: parent,
        part: { index: 0, text: "Pickup moved to 3:30." },
        edited_at: "2026-02-05T19:52:18Z",
      }),
      envelope("reaction.added", {
        chat_id: "chat-one",
        message_id: "message-one",
        part_index: 0,
        reaction_type: "like",
        custom_emoji: null,
        from_handle: parent,
        reacted_at: "2026-02-05T19:52:18Z",
      }),
      envelope("reaction.removed", {
        chat_id: "chat-one",
        message_id: "message-one",
        part_index: 0,
        reaction_type: "like",
        custom_emoji: null,
        from_handle: parent,
        reacted_at: "2026-02-05T19:52:18Z",
      }),
      envelope("participant.added", {
        chat_id: "chat-one",
        participant: parent,
        added_at: "2026-02-05T19:52:18Z",
      }),
      envelope("participant.removed", {
        chat_id: "chat-one",
        participant: { ...parent, status: "removed", left_at: "2026-02-05T19:52:18Z" },
        removed_at: "2026-02-05T19:52:18Z",
      }),
      envelope("message.sent", {
        chat,
        id: "message-outbound",
        idempotency_key: "effect-one",
        direction: "outbound",
        sender_handle: self,
        parts: [{ type: "text", value: "Can anyone cover pickup?" }],
        sent_at: "2026-02-05T19:52:18Z",
        service: "iMessage",
      }),
      envelope("message.failed", {
        chat_id: "chat-one",
        message_id: "message-outbound",
        code: 4001,
        reason: "Delivery failed",
        failed_at: "2026-02-05T19:52:18Z",
      }),
    ];

    const normalized = cases.map((payload) => {
      const signed = signedWebhook(payload);
      return unwrapLinqWebhook({ ...signed, webhookSecret, receivedAt: now }).eventType;
    });

    expect(normalized).toEqual([
      "linq.message.edited",
      "linq.reaction.added",
      "linq.reaction.removed",
      "linq.participant.added",
      "linq.participant.removed",
      "linq.outbound.sent",
      "linq.outbound.failed",
    ]);
  });

  it("does not parse unverified JSON", () => {
    const invalidRawBody = "{not-json";
    const signed = signedWebhook({}, invalidRawBody);

    expect(() =>
      unwrapLinqWebhook({
        ...signed,
        headers: { ...signed.headers, "webhook-signature": "v1,AAAAAAAA" },
        webhookSecret,
        receivedAt: now,
      }),
    ).toThrowError(expect.objectContaining({ code: "invalid_signature" }));

    expect(() => unwrapLinqWebhook({ ...signed, webhookSecret, receivedAt: now })).toThrowError(
      expect.objectContaining({ code: "invalid_json" }),
    );
  });
});
