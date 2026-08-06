import { describe, expect, it, vi } from "vitest";
import { LinqClient, type LinqConfig } from "../../src/adapters/linq/index.js";
import {
  type ClaimedSubmittedEffect,
  type EffectOutbox,
  LinqMessageEffectExecutor,
} from "../../src/modules/effects/index.js";

const chatId = "550e8400-e29b-41d4-a716-446655440000";
const messageId = "550e8400-e29b-41d4-a716-446655440020";
const fixedNow = new Date("2026-08-06T12:10:00Z");

describe("Linq submitted-message reconciliation", () => {
  it("confirms by provider message ID without invoking the send endpoint again", async () => {
    const fetchSpy = vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      expect(init?.method).toBe("GET");
      return new Response(
        JSON.stringify({
          id: messageId,
          chat_id: chatId,
          created_at: "2026-08-06T12:00:00Z",
          updated_at: "2026-08-06T12:05:00Z",
          delivery_status: "delivered",
          is_from_me: true,
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    });
    const fetchMock = fetchSpy as typeof fetch;
    const config: LinqConfig = {
      apiKey: "not-a-real-key",
      baseUrl: "https://linq.test/api/partner/v3",
      phoneNumber: "+16462350806",
      webhookSecret: `whsec_${Buffer.alloc(32, 1).toString("base64")}`,
      requestTimeoutMs: 5_000,
      maxAttachmentBytes: 1_024,
      maxWebhookBytes: 1_024,
    };
    const linq = new LinqClient(config, { fetch: fetchMock, now: () => fixedNow });
    const recordReconciliation = vi.fn(async () => true);
    const outbox = {
      reauthorizeForSubmission: vi.fn(async () => true),
      recordReceipt: vi.fn(async () => true),
      recordReconciliation,
      retry: vi.fn(async () => "retry" as const),
    } as unknown as Pick<
      EffectOutbox,
      "reauthorizeForSubmission" | "recordReceipt" | "recordReconciliation" | "retry"
    >;
    const executor = new LinqMessageEffectExecutor(linq, outbox);
    const effect: ClaimedSubmittedEffect = {
      outboxId: "550e8400-e29b-41d4-a716-446655440030",
      effectKind: "linq.message",
      idempotencyKey: "coverage-loop-1",
      payload: {
        providerChatId: chatId,
        expectedProviderParticipantDigest: `linq-v1:${"a".repeat(64)}`,
        text: "Can anyone cover pickup?",
      },
      attemptCount: 1,
      leaseToken: "550e8400-e29b-41d4-a716-446655440040",
      providerReceiptId: messageId,
      submittedAt: new Date("2026-08-06T12:00:00Z"),
      reconciliationAttemptCount: 1,
    };

    await executor.reconcile(effect, fixedNow);

    expect(fetchSpy).toHaveBeenCalledOnce();
    expect(String(fetchSpy.mock.calls[0]?.[0])).toBe(
      `https://linq.test/api/partner/v3/messages/${messageId}`,
    );
    expect(recordReconciliation).toHaveBeenCalledWith(
      expect.objectContaining({
        effect,
        status: "confirmed",
        providerReceiptId: messageId,
        now: fixedNow,
      }),
    );
  });
});
