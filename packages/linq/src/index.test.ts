import { createHmac } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import {
  LinqClient,
  type LinqMediaReference,
  linqIdentitySubjectDigest,
  unwrapLinqWebhook,
} from "./index.js";

const now = new Date("2026-08-12T16:00:00.000Z");
const webhookId = "2915e81c-5068-4796-ace2-21d2c94ad298";
const rawKey = Buffer.from("test-webhook-secret-with-enough-entropy");
const signingSecret = `whsec_${rawKey.toString("base64")}`;
const chatId = "8f392755-6865-4b18-880a-227f9d8b458f";
const ownerHandleId = "6d6c617f-187a-4dcd-a0d5-988347a8c092";
const adultOneHandleId = "e604375a-5913-483a-8278-c631e8f0ffda";
const adultTwoHandleId = "9fbf3926-a97f-43bc-bd0c-e93643a928d1";
const formerAdultHandleId = "47a65c02-7f83-47c1-84e7-7063d18da707";

function chat(
  overrides: {
    isGroup?: boolean;
    activeParticipantIds?: readonly string[];
    removedParticipantIds?: readonly string[];
    service?: "iMessage" | "RCS" | "SMS";
    handleService?: "iMessage" | "RCS" | "SMS";
  } = {},
): Record<string, unknown> {
  const activeParticipantIds = overrides.activeParticipantIds ?? [adultOneHandleId];
  return {
    id: chatId,
    is_group: overrides.isGroup ?? false,
    service: overrides.service ?? "iMessage",
    handles: [
      {
        id: ownerHandleId,
        handle: "+12025551234",
        service: overrides.handleService ?? overrides.service ?? "iMessage",
        is_me: true,
        status: "active",
        left_at: null,
      },
      ...activeParticipantIds.map((id) => ({
        id,
        handle: "+12025559876",
        service: overrides.handleService ?? overrides.service ?? "iMessage",
        is_me: false,
        status: "active",
        left_at: null,
      })),
      ...(overrides.removedParticipantIds ?? []).map((id) => ({
        id,
        handle: "+12025550000",
        service: overrides.handleService ?? overrides.service ?? "iMessage",
        is_me: false,
        status: "removed",
        left_at: "2026-08-11T16:00:00.000Z",
      })),
    ],
  };
}

function deliverable(
  audience: "private" | "group" = "private",
  participantIds: readonly string[] = [adultOneHandleId],
) {
  return {
    idempotencyKey: "effect:one",
    providerConversationId: chatId,
    payload: { text: "I’ll remind everyone tomorrow morning." },
    expectedAudience: audience,
    expectedParticipantIdentityDigests: participantIds.map(linqIdentitySubjectDigest),
  };
}

function envelope(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    api_version: "v3",
    webhook_version: "2026-02-03",
    event_type: "message.received",
    event_id: webhookId,
    created_at: now.toISOString(),
    trace_id: "8af9171a45022df2eb74ba4e4c83be0f",
    partner_id: "partner-one",
    data: {
      chat: {
        id: "8f392755-6865-4b18-880a-227f9d8b458f",
        is_group: true,
        owner_handle: {
          id: "6d6c617f-187a-4dcd-a0d5-988347a8c092",
          handle: "+12025551234",
          is_me: true,
        },
      },
      id: "89e3566e-1d13-49e5-a8ee-48490d5bfeb7",
      direction: "inbound",
      sender_handle: {
        id: "e604375a-5913-483a-8278-c631e8f0ffda",
        handle: "+12025559876",
        is_me: false,
      },
      parts: [
        { type: "text", value: "The details are in this photo." },
        {
          type: "media",
          id: "f13dda7d-ecac-49eb-b3fe-16fe286abf19",
          filename: "permission-slip.jpg",
          mime_type: "image/jpeg",
          size_bytes: 4,
          url: "https://cdn.linqapp.com/attachments/signed-but-not-exposed.jpg",
        },
      ],
      reply_to: { message_id: "6ba7b810-9dad-41d1-80b4-00c04fd430c8", part_index: 0 },
      sent_at: "2026-08-12T15:59:59.000Z",
      reconciled_at: null,
      service: "iMessage",
    },
    ...overrides,
  };
}

function signed(payload: Record<string, unknown>, timestamp = Math.floor(now.getTime() / 1000)) {
  const rawBody = JSON.stringify(payload);
  const signature = createHmac("sha256", rawKey)
    .update(`${webhookId}.${timestamp}.${rawBody}`)
    .digest("base64");
  return {
    rawBody,
    headers: {
      "webhook-id": webhookId,
      "webhook-timestamp": String(timestamp),
      "webhook-signature": `v1,not-a-signature v1,${signature}`,
    },
  };
}

describe("unwrapLinqWebhook", () => {
  it("verifies the raw body and returns authority-neutral message evidence", () => {
    const input = signed(envelope());
    const proposal = unwrapLinqWebhook({ signingSecret, now, expectedPartnerId: "partner-one", ...input });

    expect(proposal).toEqual({
      kind: "inbound_message",
      providerEventId: webhookId,
      providerConversationId: "8f392755-6865-4b18-880a-227f9d8b458f",
      providerMessageId: "89e3566e-1d13-49e5-a8ee-48490d5bfeb7",
      occurredAt: "2026-08-12T15:59:59.000Z",
      isGroup: true,
      service: "iMessage",
      sender: {
        providerHandleId: "e604375a-5913-483a-8278-c631e8f0ffda",
      },
      text: "The details are in this photo.",
      media: [
        {
          providerAttachmentId: "f13dda7d-ecac-49eb-b3fe-16fe286abf19",
          filename: "permission-slip.jpg",
          mimeType: "image/jpeg",
          sizeBytes: 4,
        },
      ],
      replyTo: { providerMessageId: "6ba7b810-9dad-41d1-80b4-00c04fd430c8" },
    });
    expect("householdId" in proposal).toBe(false);
    expect("adultId" in proposal).toBe(false);
    expect(JSON.stringify(proposal)).not.toContain("signed-but-not-exposed");
  });

  it("rejects tampering, replay, event-id mismatch, and unpinned payloads", () => {
    const valid = signed(envelope());
    expect(() =>
      unwrapLinqWebhook({ signingSecret, now, ...valid, rawBody: `${valid.rawBody} ` }),
    ).toThrowError(expect.objectContaining({ code: "invalid_signature" }));
    expect(() =>
      unwrapLinqWebhook({
        signingSecret,
        now,
        ...signed(envelope(), Math.floor(now.getTime() / 1000) - 301),
      }),
    ).toThrowError(expect.objectContaining({ code: "stale_webhook" }));
    expect(() =>
      unwrapLinqWebhook({ signingSecret, now, ...signed(envelope({ event_id: crypto.randomUUID() })) }),
    ).toThrowError(expect.objectContaining({ code: "invalid_payload" }));
    expect(() =>
      unwrapLinqWebhook({
        signingSecret,
        now,
        ...signed(envelope({ webhook_version: "2025-01-01" })),
      }),
    ).toThrowError(expect.objectContaining({ code: "invalid_payload" }));
  });
});

describe("LinqClient", () => {
  it("derives the canonical opaque Linq identity digest", () => {
    expect(linqIdentitySubjectDigest(adultOneHandleId)).toBe(
      "f538ffa25ab884f00d39bfd0d1205e4459ab1028b2d2404f5108202db9a7e65a",
    );
  });

  it("sends only after the current chat exactly matches the expected authority", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(chat()))
      .mockResolvedValueOnce(
        Response.json({
          chat_id: chatId,
          message: {
            id: "347d62c2-2170-4754-8d30-c76d0c727d96",
            created_at: "2026-08-12T16:00:01.000Z",
            delivery_status: "queued",
          },
        }),
      );
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher, now: () => now });

    await expect(client.execute(deliverable())).resolves.toEqual({
      status: "committed",
      providerReceiptId: "347d62c2-2170-4754-8d30-c76d0c727d96",
      detail: null,
      occurredAt: "2026-08-12T16:00:01.000Z",
    });
    expect(String(fetcher.mock.calls[0]?.[0])).toBe(`https://api.linqapp.com/api/partner/v3/chats/${chatId}`);
    expect(fetcher.mock.calls[0]?.[1]).toMatchObject({ method: "GET", redirect: "error" });
    const [url, init] = fetcher.mock.calls[1] ?? [];
    expect(String(url)).toBe(`https://api.linqapp.com/api/partner/v3/chats/${chatId}/messages`);
    expect(init).toMatchObject({
      method: "POST",
      headers: expect.objectContaining({ Authorization: "Bearer secret" }),
      redirect: "error",
    });
    expect(JSON.parse(String(init?.body))).toEqual({
      message: {
        parts: [{ type: "text", value: "I’ll remind everyone tomorrow morning." }],
        idempotency_key: "effect:one",
      },
    });
  });

  it("returns a definitive failure when Linq rejects an exact-authority send", async () => {
    const rejected = new LinqClient({
      apiKey: "secret",
      fetch: vi
        .fn<typeof fetch>()
        .mockResolvedValueOnce(Response.json(chat()))
        .mockResolvedValueOnce(new Response(null, { status: 422 })),
      now: () => now,
    });

    await expect(rejected.execute(deliverable())).resolves.toMatchObject({
      status: "failed",
      detail: "Linq rejected message delivery (HTTP 422).",
    });
  });

  it("fails closed without reading or sending when expected authority is missing", async () => {
    const fetcher = vi.fn<typeof fetch>();
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher, now: () => now });

    await expect(
      client.execute({
        idempotencyKey: "effect:one",
        providerConversationId: chatId,
        payload: { text: "Hello" },
      }),
    ).resolves.toMatchObject({
      status: "failed",
      detail: "Florence did not supply valid current chat authority.",
    });
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("does not send when a third active group participant appears", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValueOnce(
      Response.json(
        chat({
          isGroup: true,
          activeParticipantIds: [adultOneHandleId, adultTwoHandleId, formerAdultHandleId],
        }),
      ),
    );
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher, now: () => now });

    await expect(
      client.execute(deliverable("group", [adultOneHandleId, adultTwoHandleId])),
    ).resolves.toMatchObject({ status: "failed" });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("does not send when the live chat is not iMessage", async () => {
    for (const observed of [chat({ service: "SMS" }), chat({ handleService: "RCS" })]) {
      const fetcher = vi.fn<typeof fetch>().mockResolvedValueOnce(Response.json(observed));
      const client = new LinqClient({ apiKey: "secret", fetch: fetcher, now: () => now });

      await expect(client.execute(deliverable())).resolves.toMatchObject({
        status: "failed",
        detail: "Linq could not verify the current chat authority.",
      });
      expect(fetcher).toHaveBeenCalledTimes(1);
    }
  });

  it("retries a transient chat observation without attempting a send", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValueOnce(new Response(null, { status: 503 }));
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher });

    await expect(client.execute(deliverable())).rejects.toMatchObject({
      code: "provider_retryable",
      retryable: true,
    });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("never restores authority after a participant has left the chat", async () => {
    const currentGroup = chat({
      isGroup: true,
      activeParticipantIds: [adultOneHandleId, adultTwoHandleId],
      removedParticipantIds: [formerAdultHandleId],
    });
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json(currentGroup));

    await expect(
      new LinqClient({ apiKey: "secret", fetch: fetcher }).execute(
        deliverable("group", [adultTwoHandleId, adultOneHandleId]),
      ),
    ).resolves.toMatchObject({
      status: "failed",
      detail: "Linq could not verify the current chat authority.",
    });
    await expect(
      new LinqClient({ apiKey: "secret", fetch: fetcher, now: () => now }).execute(
        deliverable("group", [adultOneHandleId, formerAdultHandleId]),
      ),
    ).resolves.toMatchObject({ status: "failed" });
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("fetches signed attachment evidence through authenticated metadata and an allowlisted CDN", async () => {
    const reference: LinqMediaReference = {
      providerAttachmentId: "f13dda7d-ecac-49eb-b3fe-16fe286abf19",
      filename: "permission-slip.jpg",
      mimeType: "image/jpeg",
      sizeBytes: 4,
    };
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        Response.json({
          id: reference.providerAttachmentId,
          filename: reference.filename,
          content_type: reference.mimeType,
          size_bytes: reference.sizeBytes,
          status: "complete",
          download_url: "https://cdn.linqapp.com/attachments/fresh-signed-url",
        }),
      )
      .mockResolvedValueOnce(
        new Response(Uint8Array.from([1, 2, 3, 4]), {
          headers: { "content-type": "image/jpeg", "content-length": "4" },
        }),
      );
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher });

    await expect(client.fetchMedia(reference)).resolves.toEqual({
      ...reference,
      bytes: Uint8Array.from([1, 2, 3, 4]),
    });
    expect(fetcher.mock.calls[0]?.[1]?.headers).toMatchObject({ Authorization: "Bearer secret" });
    expect(fetcher.mock.calls[1]?.[1]?.headers).toEqual({ Accept: "image/jpeg" });
  });

  it("rejects provider media redirects outside Linq's CDN", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
      Response.json({
        id: "attachment-one",
        filename: "photo.jpg",
        content_type: "image/jpeg",
        size_bytes: 4,
        status: "complete",
        download_url: "https://attacker.example/steal",
      }),
    );
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher });

    await expect(
      client.fetchMedia({
        providerAttachmentId: "attachment-one",
        filename: "photo.jpg",
        mimeType: "image/jpeg",
        sizeBytes: 4,
      }),
    ).rejects.toMatchObject({ code: "unsafe_media" });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
});
