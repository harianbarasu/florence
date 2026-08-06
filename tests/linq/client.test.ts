import { createHash } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import {
  computeLinqParticipantDigest,
  LinqAudienceChangedError,
  LinqClient,
  type LinqConfig,
  type LinqParticipant,
} from "../../src/adapters/linq/index.js";

const fixedNow = new Date("2026-08-05T12:00:00Z");
const chatId = "550e8400-e29b-41d4-a716-446655440000";
const selfId = "550e8400-e29b-41d4-a716-446655440010";
const parentId = "550e8400-e29b-41d4-a716-446655440011";
const messageId = "550e8400-e29b-41d4-a716-446655440020";
const attachmentId = "550e8400-e29b-41d4-a716-446655440030";

const config: LinqConfig = {
  apiKey: "not-a-real-key",
  baseUrl: "https://linq.test/api/partner/v3",
  phoneNumber: "+16462350806",
  webhookSecret: `whsec_${Buffer.alloc(32, 1).toString("base64")}`,
  requestTimeoutMs: 5_000,
  maxAttachmentBytes: 1024,
  maxWebhookBytes: 1024 * 1024,
};

const providerChat = {
  id: chatId,
  created_at: "2026-01-01T10:00:00Z",
  display_name: "Family",
  handles: [
    {
      id: selfId,
      handle: config.phoneNumber,
      joined_at: "2026-01-01T10:00:00Z",
      service: "iMessage",
      is_me: true,
      left_at: null,
      status: "active",
    },
    {
      id: parentId,
      handle: "+14155550100",
      joined_at: "2026-01-01T10:00:00Z",
      service: "iMessage",
      is_me: false,
      left_at: null,
      status: "active",
    },
  ],
  health_status: {
    status: "HEALTHY",
    updated_at: "2026-08-05T11:00:00Z",
  },
  is_group: true,
  updated_at: "2026-08-05T11:00:00Z",
  service: "iMessage",
};

const normalizedParticipants: LinqParticipant[] = [
  {
    providerParticipantId: selfId,
    address: config.phoneNumber,
    service: "imessage",
    isSelf: true,
    status: "active",
    joinedAt: "2026-01-01T10:00:00.000Z",
  },
  {
    providerParticipantId: parentId,
    address: "+14155550100",
    service: "imessage",
    isSelf: false,
    status: "active",
    joinedAt: "2026-01-01T10:00:00.000Z",
  },
];

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

describe("LinqClient", () => {
  it("opens one exact direct chat with a link-free idempotent enrollment message", async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: String(input), ...(init ? { init } : {}) });
      return jsonResponse({
        chat: {
          ...providerChat,
          display_name: null,
          is_group: false,
          message: {
            id: messageId,
            created_at: "2026-08-05T12:00:00Z",
            delivery_status: "pending",
          },
        },
      });
    }) as typeof fetch;
    const client = new LinqClient(config, { fetch: fetchMock, now: () => fixedNow });

    const receipt = await client.createDirectChat({
      recipient: "+14155550100",
      idempotencyKey: "household-enrollment-one",
      text: "Jackson invited you to Florence. Reply yes to agree, or STOP.",
    });

    expect(calls.map((call) => [call.init?.method, call.url])).toEqual([
      ["POST", "https://linq.test/api/partner/v3/chats"],
    ]);
    expect(JSON.parse(String(calls[0]?.init?.body))).toEqual({
      from: config.phoneNumber,
      to: ["+14155550100"],
      message: {
        idempotency_key: "household-enrollment-one",
        parts: [
          {
            type: "text",
            value: "Jackson invited you to Florence. Reply yes to agree, or STOP.",
          },
        ],
      },
    });
    expect(receipt).toMatchObject({
      providerChatId: chatId,
      providerMessageId: messageId,
      idempotencyKey: "household-enrollment-one",
    });
  });

  it("reauthorizes the live audience and sends one idempotent message", async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: String(input), ...(init ? { init } : {}) });
      if (calls.length === 1) {
        return jsonResponse(providerChat);
      }
      return jsonResponse({
        chat_id: chatId,
        message: {
          id: messageId,
          created_at: "2026-08-05T12:00:00Z",
          delivery_status: "pending",
        },
      });
    }) as typeof fetch;
    const client = new LinqClient(config, { fetch: fetchMock, now: () => fixedNow });
    const digest = computeLinqParticipantDigest(normalizedParticipants);

    const receipt = await client.sendMessage({
      providerChatId: chatId,
      expectedParticipantDigest: digest,
      idempotencyKey: "effect-coverage-one",
      text: "Can anyone cover pickup?",
      providerAttachmentIds: [attachmentId],
      replyTo: { providerMessageId: messageId, partIndex: 0 },
    });

    expect(calls.map((call) => [call.init?.method, call.url])).toEqual([
      ["GET", `https://linq.test/api/partner/v3/chats/${chatId}`],
      ["POST", `https://linq.test/api/partner/v3/chats/${chatId}/messages`],
    ]);
    const headers = calls[1]?.init?.headers;
    expect(new Headers(headers).get("authorization")).toBe("Bearer not-a-real-key");
    expect(JSON.parse(String(calls[1]?.init?.body))).toEqual({
      message: {
        parts: [
          { type: "text", value: "Can anyone cover pickup?" },
          { type: "media", attachment_id: attachmentId },
        ],
        idempotency_key: "effect-coverage-one",
        reply_to: { message_id: messageId, part_index: 0 },
      },
    });
    expect(receipt).toMatchObject({
      providerMessageId: messageId,
      idempotencyKey: "effect-coverage-one",
      status: "accepted",
      participantDigest: digest,
    });
  });

  it("closes the send gate when the authoritative participant digest changed", async () => {
    const fetchMock = vi.fn(async () => jsonResponse(providerChat)) as typeof fetch;
    const client = new LinqClient(config, { fetch: fetchMock, now: () => fixedNow });

    await expect(
      client.sendMessage({
        providerChatId: chatId,
        expectedParticipantDigest: `linq-v1:${"0".repeat(64)}`,
        idempotencyKey: "effect-stale-audience",
        text: "This must not send",
      }),
    ).rejects.toBeInstanceOf(LinqAudienceChangedError);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("downloads only bounded attachments from Linq's allowlisted CDN without forwarding credentials", async () => {
    const bytes = Uint8Array.from([1, 2, 3, 4]);
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: String(input), ...(init ? { init } : {}) });
      if (calls.length === 1) {
        return jsonResponse({
          id: attachmentId,
          filename: "schedule.pdf",
          content_type: "application/pdf",
          size_bytes: bytes.byteLength,
          download_url: `https://cdn.linqapp.com/attachments/${attachmentId}/schedule.pdf?signature=hidden`,
          created_at: "2026-08-05T11:00:00Z",
        });
      }
      return new Response(bytes, {
        headers: {
          "content-length": String(bytes.byteLength),
          "content-type": "application/pdf",
        },
      });
    }) as typeof fetch;
    const client = new LinqClient(config, { fetch: fetchMock, now: () => fixedNow });

    const result = await client.fetchAttachment(attachmentId);

    expect(new Headers(calls[0]?.init?.headers).get("authorization")).toBe("Bearer not-a-real-key");
    expect(new Headers(calls[1]?.init?.headers).has("authorization")).toBe(false);
    expect(result.sha256).toBe(createHash("sha256").update(bytes).digest("hex"));
    expect(result.bytes).toEqual(bytes);

    const unsafeFetch = vi.fn(async () =>
      jsonResponse({
        id: attachmentId,
        filename: "schedule.pdf",
        content_type: "application/pdf",
        size_bytes: 4,
        download_url: "https://attacker.example/schedule.pdf",
        created_at: "2026-08-05T11:00:00Z",
      }),
    ) as typeof fetch;
    const unsafeClient = new LinqClient(config, { fetch: unsafeFetch, now: () => fixedNow });
    await expect(unsafeClient.fetchAttachment(attachmentId)).rejects.toMatchObject({
      code: "download_url_not_allowed",
    });
    expect(unsafeFetch).toHaveBeenCalledTimes(1);
  });
});
