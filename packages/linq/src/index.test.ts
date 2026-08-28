import { describe, expect, it, vi } from "vitest";
import {
  LinqClient,
  type LinqConversationAuthority,
  type LinqReactionValue,
  linqIdentitySubjectDigest,
} from "./index.js";

const chatId = "8f392755-6865-4b18-880a-227f9d8b458f";
const ownerHandleId = "6d6c617f-187a-4dcd-a0d5-988347a8c092";
const adultOneHandleId = "e604375a-5913-483a-8278-c631e8f0ffda";
const adultTwoHandleId = "9fbf3926-a97f-43bc-bd0c-e93643a928d1";

function authority(
  audience: "private" | "group",
  participantHandleIds: readonly string[],
): LinqConversationAuthority {
  return {
    audience,
    participantIdentityDigests: participantHandleIds.map(linqIdentitySubjectDigest),
  };
}

function observedChat(
  audience: "private" | "group",
  participantHandleIds: readonly string[],
): Record<string, unknown> {
  return {
    id: chatId,
    is_group: audience === "group",
    service: "iMessage",
    handles: [
      {
        id: ownerHandleId,
        handle: "+12025551234",
        service: "iMessage",
        is_me: true,
        status: "active",
        left_at: null,
      },
      ...participantHandleIds.map((id, index) => ({
        id,
        handle: index === 0 ? "+12025559876" : "+12025550000",
        service: "iMessage",
        is_me: false,
        status: "active",
        left_at: null,
      })),
    ],
  };
}

function expectBodylessAuthorizedRequest(
  call: readonly unknown[] | undefined,
  method: "POST" | "DELETE",
  suffix: "typing" | "read",
): void {
  const url = call?.[0];
  const init = call?.[1] as RequestInit | undefined;
  expect(String(url)).toBe(`https://api.linqapp.com/api/partner/v3/chats/${chatId}/${suffix}`);
  expect(init).toMatchObject({
    method,
    headers: expect.objectContaining({ Authorization: "Bearer secret" }),
    redirect: "error",
  });
  expect(init?.body).toBeUndefined();
}

function requestBody(call: readonly unknown[] | undefined): Record<string, unknown> {
  const init = call?.[1] as RequestInit | undefined;
  return JSON.parse(String(init?.body)) as Record<string, unknown>;
}

function messageReceipt(id = "message-1", deliveryStatus = "sent"): Response {
  return Response.json({
    message: {
      id,
      created_at: "2026-08-28T16:00:00.000Z",
      delivery_status: deliveryStatus,
    },
  });
}

function reactionTarget(
  targetMessageId: string,
  reactions: readonly Record<string, unknown>[],
  partIndex = 0,
): Response {
  return Response.json({
    id: targetMessageId,
    chat_id: chatId,
    parts: Array.from({ length: partIndex + 1 }, (_, index) => ({
      type: "text",
      value: `Part ${index}`,
      reactions: index === partIndex ? reactions : [],
    })),
  });
}

describe("LinqClient native presence", () => {
  it.each([
    {
      audience: "private" as const,
      participantHandleIds: [adultOneHandleId],
      active: true,
      method: "POST" as const,
    },
    {
      audience: "group" as const,
      participantHandleIds: [adultOneHandleId, adultTwoHandleId],
      active: true,
      method: "POST" as const,
    },
    {
      audience: "group" as const,
      participantHandleIds: [adultOneHandleId, adultTwoHandleId],
      active: false,
      method: "DELETE" as const,
    },
  ])("sends bodyless $audience typing requests after exact authority checks", async (input) => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(observedChat(input.audience, input.participantHandleIds)))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher });

    await expect(
      client.setTyping({
        providerConversationId: chatId,
        expectedAuthority: authority(input.audience, input.participantHandleIds),
        active: input.active,
      }),
    ).resolves.toBe(true);

    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(String(fetcher.mock.calls[0]?.[0])).toBe(`https://api.linqapp.com/api/partner/v3/chats/${chatId}`);
    expect(fetcher.mock.calls[0]?.[1]).toMatchObject({ method: "GET", redirect: "error" });
    expectBodylessAuthorizedRequest(fetcher.mock.calls[1], input.method, "typing");
  });

  it("marks an accepted private chat read only after exact authority verification", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(observedChat("private", [adultOneHandleId])))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher });

    await expect(
      client.markRead({
        providerConversationId: chatId,
        expectedAuthority: authority("private", [adultOneHandleId]),
      }),
    ).resolves.toBe(true);

    expect(fetcher).toHaveBeenCalledTimes(2);
    expectBodylessAuthorizedRequest(fetcher.mock.calls[1], "POST", "read");
  });

  it("does not mark read when authority drifts or the expected chat is a group", async () => {
    const driftedFetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(observedChat("private", [adultTwoHandleId])));
    const driftedClient = new LinqClient({ apiKey: "secret", fetch: driftedFetcher });

    await expect(
      driftedClient.markRead({
        providerConversationId: chatId,
        expectedAuthority: authority("private", [adultOneHandleId]),
      }),
    ).resolves.toBe(false);
    expect(driftedFetcher).toHaveBeenCalledTimes(1);

    const groupFetcher = vi.fn<typeof fetch>();
    const groupClient = new LinqClient({ apiKey: "secret", fetch: groupFetcher });
    await expect(
      groupClient.markRead({
        providerConversationId: chatId,
        expectedAuthority: authority("group", [adultOneHandleId, adultTwoHandleId]),
      }),
    ).resolves.toBe(false);
    expect(groupFetcher).not.toHaveBeenCalled();
  });

  it("reuses already-observed authority without another chat read", async () => {
    const observed = observedChat("private", [adultOneHandleId]);
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }));
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher });

    await expect(
      client.setTyping({
        providerConversationId: chatId,
        expectedAuthority: authority("private", [adultOneHandleId]),
        observedAuthority: {
          providerConversationId: chatId,
          authority: authority("private", [adultOneHandleId]),
        },
        active: true,
      }),
    ).resolves.toBe(true);
    await expect(
      client.markRead({
        providerConversationId: chatId,
        expectedAuthority: authority("private", [adultOneHandleId]),
        observedAuthority: {
          providerConversationId: chatId,
          authority: authority("private", [adultOneHandleId]),
        },
      }),
    ).resolves.toBe(true);

    expect(observed).toMatchObject({ is_group: false });
    expect(fetcher).toHaveBeenCalledTimes(2);
    expectBodylessAuthorizedRequest(fetcher.mock.calls[0], "POST", "typing");
    expectBodylessAuthorizedRequest(fetcher.mock.calls[1], "POST", "read");
  });

  it("bounds a stalled native presence request", async () => {
    vi.useFakeTimers();
    try {
      const fetcher = vi.fn<typeof fetch>().mockImplementation(
        (_url, init) =>
          new Promise<Response>((_resolve, reject) => {
            init?.signal?.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
          }),
      );
      const client = new LinqClient({ apiKey: "secret", fetch: fetcher });
      const result = client.setTyping({
        providerConversationId: chatId,
        expectedAuthority: authority("private", [adultOneHandleId]),
        observedAuthority: {
          providerConversationId: chatId,
          authority: authority("private", [adultOneHandleId]),
        },
        active: true,
      });

      await vi.advanceTimersByTimeAsync(1_500);
      await expect(result).resolves.toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("LinqClient native conversation moves", () => {
  it("sends a threaded group mention with UTF-16 offsets", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(observedChat("group", [adultOneHandleId, adultTwoHandleId])))
      .mockResolvedValueOnce(messageReceipt());
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher });

    await expect(
      client.sendMove({
        idempotencyKey: "mention-1",
        providerConversationId: chatId,
        expectedAuthority: authority("group", [adultOneHandleId, adultTwoHandleId]),
        move: {
          type: "message",
          parts: [
            {
              type: "text",
              text: "Hey Kevin, can you confirm?",
              mention: { handle: "+12025559876", range: [4, 9] },
            },
          ],
          replyTo: { providerMessageId: "parent-message", partIndex: 2 },
        },
      }),
    ).resolves.toMatchObject({ status: "committed", providerState: "sent" });

    expect(requestBody(fetcher.mock.calls[1])).toEqual({
      message: {
        parts: [
          {
            type: "text",
            value: "Hey Kevin, can you confirm?",
            mention: "+12025559876",
            mention_range: [4, 9],
          },
        ],
        idempotency_key: "mention-1",
        preferred_service: "iMessage",
        reply_to: { message_id: "parent-message", part_index: 2 },
      },
    });
  });

  it.each([
    {
      name: "rich link",
      parts: [{ type: "link" as const, url: "https://example.com/plan" }],
      expected: [{ type: "link", value: "https://example.com/plan" }],
    },
    {
      name: "exact media URL",
      parts: [
        {
          type: "media" as const,
          source: { type: "url" as const, url: "https://cdn.example.com/menu.pdf" },
        },
      ],
      expected: [{ type: "media", url: "https://cdn.example.com/menu.pdf" }],
    },
    {
      name: "pre-uploaded media",
      parts: [
        {
          type: "media" as const,
          source: { type: "attachment" as const, providerAttachmentId: "attachment-123" },
        },
      ],
      expected: [{ type: "media", attachment_id: "attachment-123" }],
    },
  ])("sends a native $name payload", async ({ parts, expected }) => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(observedChat("private", [adultOneHandleId])))
      .mockResolvedValueOnce(messageReceipt());
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher });

    await client.sendMove({
      idempotencyKey: "native-message-1",
      providerConversationId: chatId,
      expectedAuthority: authority("private", [adultOneHandleId]),
      move: { type: "message", parts },
    });

    expect(requestBody(fetcher.mock.calls[1])).toEqual({
      message: {
        parts: expected,
        idempotency_key: "native-message-1",
        preferred_service: "iMessage",
      },
    });
  });

  it.each([
    {
      name: "a rich link mixed with another part",
      audience: "private" as const,
      parts: [
        { type: "text" as const, text: "Here it is" },
        { type: "link" as const, url: "https://example.com" },
      ],
    },
    {
      name: "a private-chat mention",
      audience: "private" as const,
      parts: [
        {
          type: "text" as const,
          text: "Kevin",
          mention: { handle: "+12025559876", range: [0, 5] as const },
        },
      ],
    },
    {
      name: "an insecure media URL",
      audience: "private" as const,
      parts: [
        {
          type: "media" as const,
          source: { type: "url" as const, url: "http://example.com/photo.jpg" },
        },
      ],
    },
    {
      name: "consecutive text parts",
      audience: "private" as const,
      parts: [
        { type: "text" as const, text: "First" },
        { type: "text" as const, text: "Second" },
      ],
    },
    {
      name: "more than 40 public URL media parts",
      audience: "private" as const,
      parts: Array.from({ length: 41 }, (_, index) => ({
        type: "media" as const,
        source: { type: "url" as const, url: `https://cdn.example.com/photo-${index}.jpg` },
      })),
    },
  ])("rejects $name before provider mutation", async ({ audience, parts }) => {
    const participantIds = audience === "private" ? [adultOneHandleId] : [adultOneHandleId, adultTwoHandleId];
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(observedChat(audience, participantIds)));
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher });

    await expect(
      client.sendMove({
        idempotencyKey: "invalid-native-message",
        providerConversationId: chatId,
        expectedAuthority: authority(audience, participantIds),
        move: { type: "message", parts },
      }),
    ).rejects.toMatchObject({ code: "configuration" });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("returns a documented failed message receipt as a terminal failure", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(observedChat("private", [adultOneHandleId])))
      .mockResolvedValueOnce(messageReceipt("message-failed", "failed"));
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher });

    await expect(
      client.sendMove({
        idempotencyKey: "failed-message",
        providerConversationId: chatId,
        expectedAuthority: authority("private", [adultOneHandleId]),
        move: { type: "message", parts: [{ type: "text", text: "Hello" }] },
      }),
    ).resolves.toEqual({
      status: "failed",
      idempotencyKey: "failed-message",
      providerReceiptId: null,
      detail: "Linq reported message delivery failed.",
      occurredAt: "2026-08-28T16:00:00.000Z",
    });
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it.each([
    {
      operation: "add" as const,
      reaction: { type: "custom" as const, emoji: "😍" },
      before: [],
      after: [{ is_me: true, type: "custom", custom_emoji: "😍" }],
      expectedBody: { operation: "add", type: "custom", custom_emoji: "😍", part_index: 1 },
      expectedState: "reaction_added",
    },
    {
      operation: "remove" as const,
      reaction: { type: "tapback" as const, reaction: "like" as const },
      before: [{ is_me: true, type: "like", custom_emoji: null }],
      after: [],
      expectedBody: { operation: "remove", type: "like", part_index: 1 },
      expectedState: "reaction_removed",
    },
  ])(
    "$operation reconciles a full-fidelity reaction on the selected part",
    async ({ operation, reaction, before, after, expectedBody, expectedState }) => {
      const targetMessageId = "target-message";
      const fetcher = vi
        .fn<typeof fetch>()
        .mockResolvedValueOnce(Response.json(observedChat("group", [adultOneHandleId, adultTwoHandleId])))
        .mockResolvedValueOnce(reactionTarget(targetMessageId, before, 1))
        .mockResolvedValueOnce(Response.json({ status: "accepted", trace_id: "trace-1" }))
        .mockResolvedValueOnce(reactionTarget(targetMessageId, after, 1));
      const client = new LinqClient({ apiKey: "secret", fetch: fetcher });

      await expect(
        client.sendMove({
          idempotencyKey: `reaction-${operation}`,
          providerConversationId: chatId,
          expectedAuthority: authority("group", [adultOneHandleId, adultTwoHandleId]),
          move: {
            type: "reaction",
            operation,
            targetProviderMessageId: targetMessageId,
            partIndex: 1,
            reaction: reaction as LinqReactionValue,
          },
        }),
      ).resolves.toMatchObject({ status: "committed", providerState: expectedState });

      expect(String(fetcher.mock.calls[2]?.[0])).toBe(
        `https://api.linqapp.com/api/partner/v3/messages/${targetMessageId}/reactions`,
      );
      expect(requestBody(fetcher.mock.calls[2])).toEqual(expectedBody);
    },
  );

  it("creates an idempotent native poll and retains its poll-definition message ID", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(observedChat("group", [adultOneHandleId, adultTwoHandleId])))
      .mockResolvedValueOnce(
        Response.json(
          {
            chat_id: chatId,
            created_at: "2026-08-28T16:00:00.000Z",
            message_id: "poll-message-1",
            poll: { options: [], total_voters: 0 },
          },
          { status: 202 },
        ),
      );
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher });

    await expect(
      client.sendMove({
        idempotencyKey: "poll-1",
        providerConversationId: chatId,
        expectedAuthority: authority("group", [adultOneHandleId, adultTwoHandleId]),
        move: { type: "poll", options: ["Tacos", "Sushi"] },
      }),
    ).resolves.toMatchObject({
      status: "committed",
      providerState: "accepted",
      providerReceiptId: "poll-message-1",
    });
    expect(String(fetcher.mock.calls[1]?.[0])).toBe(
      `https://api.linqapp.com/api/partner/v3/chats/${chatId}/polls`,
    );
    expect(requestBody(fetcher.mock.calls[1])).toEqual({
      poll: {
        options: [{ text: "Tacos" }, { text: "Sushi" }],
        idempotency_key: "poll-1",
      },
    });
  });

  it("rejects a poll with fewer than two options before provider mutation", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(observedChat("group", [adultOneHandleId, adultTwoHandleId])));
    const client = new LinqClient({ apiKey: "secret", fetch: fetcher });

    await expect(
      client.sendMove({
        idempotencyKey: "poll-invalid",
        providerConversationId: chatId,
        expectedAuthority: authority("group", [adultOneHandleId, adultTwoHandleId]),
        move: { type: "poll", options: ["Only one"] },
      }),
    ).rejects.toMatchObject({ code: "configuration" });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
});
