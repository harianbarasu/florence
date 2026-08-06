import { z } from "zod";
import type { LinqClient, LinqMessageDeliveryReceipt, LinqSendReceipt } from "../../adapters/linq/index.js";
import { LinqApiError, LinqAudienceChangedError } from "../../adapters/linq/index.js";
import type { ClaimedEffect, ClaimedSubmittedEffect, EffectOutbox } from "./outbox.js";

const MAX_RECONCILIATION_ATTEMPTS = 8;
const MAX_RECONCILIATION_AGE_MS = 30 * 60_000;

const LinqEffectPayloadSchema = z.strictObject({
  providerChatId: z.string().uuid(),
  expectedProviderParticipantDigest: z.string().regex(/^linq-v1:[a-f0-9]{64}$/u),
  text: z.string().trim().min(1).max(10_000),
});
const LinqCreateDirectEffectPayloadSchema = z.strictObject({
  recipient: z.union([z.string().regex(/^\+[1-9]\d{6,14}$/u), z.string().trim().email().max(320)]),
  text: z.string().trim().min(1).max(10_000),
});
const LinqMessageEffectPayloadSchema = z.union([
  LinqEffectPayloadSchema,
  LinqCreateDirectEffectPayloadSchema,
]);

export class LinqMessageEffectExecutor {
  public constructor(
    private readonly linq: Pick<
      LinqClient,
      "createDirectChat" | "getChat" | "getMessageDelivery" | "sendMessage"
    >,
    private readonly outbox: Pick<
      EffectOutbox,
      "reauthorizeForSubmission" | "recordReceipt" | "recordReconciliation" | "retry"
    >,
  ) {}

  public async execute(effect: ClaimedEffect): Promise<void> {
    const payload = LinqMessageEffectPayloadSchema.parse(effect.payload);
    try {
      const beforeSubmit = async () => {
        try {
          if (!(await this.outbox.reauthorizeForSubmission(effect))) {
            throw new EffectAuthorizationStaleError();
          }
        } catch (error) {
          if (error instanceof EffectAuthorizationStaleError) throw error;
          throw new EffectAuthorizationCheckError();
        }
      };
      const receipt =
        "providerChatId" in payload
          ? await this.linq.sendMessage(
              {
                providerChatId: payload.providerChatId,
                expectedParticipantDigest: payload.expectedProviderParticipantDigest,
                idempotencyKey: effect.idempotencyKey,
                text: payload.text,
              },
              undefined,
              beforeSubmit,
            )
          : await this.linq.createDirectChat(
              {
                recipient: payload.recipient,
                idempotencyKey: effect.idempotencyKey,
                text: payload.text,
              },
              undefined,
              beforeSubmit,
            );
      await this.record(effect, receipt);
    } catch (error) {
      if (error instanceof EffectAuthorizationStaleError) return;
      const audienceChanged = error instanceof LinqAudienceChangedError;
      const retryable =
        error instanceof EffectAuthorizationCheckError ||
        (error instanceof LinqApiError ? error.retryable : false);
      await this.outbox.retry(
        effect,
        error instanceof EffectAuthorizationCheckError
          ? "effect_authority_check_failed"
          : audienceChanged
            ? "linq_audience_changed"
            : error instanceof LinqApiError
              ? String(error.providerCode ?? "linq_api_error")
              : "linq_send_failed",
        retryable && !audienceChanged,
      );
    }
  }

  /**
   * Resolves an already-accepted send by its provider message ID. This path never
   * calls sendMessage; the original external mutation remains a one-way boundary.
   */
  public async reconcile(effect: ClaimedSubmittedEffect, now = new Date()): Promise<void> {
    const payload = LinqMessageEffectPayloadSchema.safeParse(effect.payload);
    if (!payload.success || effect.providerReceiptId === null) {
      await this.outbox.recordReconciliation({
        effect,
        status: "ambiguous",
        ...(effect.providerReceiptId ? { providerReceiptId: effect.providerReceiptId } : {}),
        receipt: {
          kind: "linq_message_delivery_lookup",
          outcome: payload.success ? "missing_provider_message_id" : "invalid_effect_payload",
        },
        errorCode: payload.success ? "linq_missing_provider_message_id" : "linq_invalid_effect_payload",
        now,
      });
      return;
    }

    let receipt: LinqMessageDeliveryReceipt;
    try {
      receipt = await this.linq.getMessageDelivery(effect.providerReceiptId);
    } catch (error) {
      await this.recordLookupFailure(effect, effect.providerReceiptId, error, now);
      return;
    }
    if ("providerChatId" in payload.data && receipt.providerChatId !== payload.data.providerChatId) {
      await this.outbox.recordReconciliation({
        effect,
        status: "ambiguous",
        providerReceiptId: effect.providerReceiptId,
        receipt: auditReceipt(receipt, "chat_mismatch"),
        errorCode: "linq_delivery_chat_mismatch",
        now,
      });
      return;
    }
    if (!("providerChatId" in payload.data)) {
      try {
        const chat = await this.linq.getChat(receipt.providerChatId);
        const recipient = payload.data.recipient.toLocaleLowerCase("en-US");
        const humans = chat.participants.filter(
          (participant) => participant.status === "active" && !participant.isSelf,
        );
        if (
          chat.kind !== "direct" ||
          humans.length !== 1 ||
          humans[0]?.address.toLocaleLowerCase("en-US") !== recipient
        ) {
          await this.outbox.recordReconciliation({
            effect,
            status: "ambiguous",
            providerReceiptId: effect.providerReceiptId,
            receipt: auditReceipt(receipt, "chat_mismatch"),
            errorCode: "linq_created_chat_audience_mismatch",
            now,
          });
          return;
        }
      } catch (error) {
        await this.recordLookupFailure(effect, effect.providerReceiptId, error, now);
        return;
      }
    }
    const status = reconciliationStatus(receipt);
    const exhausted = reconciliationExhausted(effect, now);
    if ((status === "submitted" || status === "failed") && !exhausted) {
      await this.outbox.recordReconciliation({
        effect,
        status,
        providerReceiptId: effect.providerReceiptId,
        receipt: auditReceipt(receipt, status === "failed" ? "failure_grace" : "unresolved"),
        ...(status === "failed" ? { errorCode: "linq_delivery_failure_observed" } : {}),
        nextAttemptAt: nextReconciliationAt(effect.reconciliationAttemptCount, now),
        now,
      });
      return;
    }
    await this.outbox.recordReconciliation({
      effect,
      status: status === "submitted" ? "ambiguous" : status,
      providerReceiptId: effect.providerReceiptId,
      receipt: auditReceipt(receipt, status === "submitted" ? "deadline_exhausted" : "terminal"),
      ...(status === "failed"
        ? { errorCode: "linq_delivery_failed" }
        : status === "submitted"
          ? { errorCode: "linq_delivery_unresolved" }
          : {}),
      now,
    });
  }

  private async recordLookupFailure(
    effect: ClaimedSubmittedEffect,
    providerReceiptId: string,
    error: unknown,
    now: Date,
  ): Promise<void> {
    const notFound = error instanceof LinqApiError && error.status === 404;
    const exhausted = reconciliationExhausted(effect, now);
    const terminal = notFound || exhausted;
    await this.outbox.recordReconciliation({
      effect,
      status: terminal ? "ambiguous" : "submitted",
      providerReceiptId,
      receipt: {
        kind: "linq_message_delivery_lookup",
        outcome: notFound ? "not_found" : terminal ? "lookup_exhausted" : "lookup_deferred",
        ...(error instanceof LinqApiError
          ? {
              httpStatus: error.status,
              providerCode: normalizeProviderCode(error.providerCode),
              retryable: error.retryable,
            }
          : { providerCode: "unexpected_lookup_error", retryable: false }),
      },
      errorCode: notFound
        ? "linq_delivery_not_found"
        : terminal
          ? "linq_delivery_lookup_exhausted"
          : "linq_delivery_lookup_deferred",
      ...(terminal ? {} : { nextAttemptAt: nextReconciliationAt(effect.reconciliationAttemptCount, now) }),
      now,
    });
  }

  private async record(effect: ClaimedEffect, receipt: LinqSendReceipt): Promise<void> {
    const status = reconciliationStatus(receipt);
    await this.outbox.recordReceipt({
      effect,
      status,
      providerReceiptId: receipt.providerMessageId,
      receipt,
      ...(status === "failed" ? { errorCode: "linq_delivery_failed" } : {}),
    });
  }
}

class EffectAuthorizationStaleError extends Error {
  public constructor() {
    super("effect_authority_stale");
    this.name = "EffectAuthorizationStaleError";
  }
}

class EffectAuthorizationCheckError extends Error {
  public constructor() {
    super("effect_authority_check_failed");
    this.name = "EffectAuthorizationCheckError";
  }
}

function reconciliationStatus(
  receipt: Pick<LinqSendReceipt | LinqMessageDeliveryReceipt, "providerDeliveryStatus">,
): "submitted" | "confirmed" | "failed" {
  switch (receipt.providerDeliveryStatus) {
    case "failed":
      return "failed";
    case "sent":
    case "delivered":
    case "received":
    case "read":
      return "confirmed";
    case "pending":
    case "queued":
      return "submitted";
  }
}

function reconciliationExhausted(effect: ClaimedSubmittedEffect, now: Date): boolean {
  return (
    effect.reconciliationAttemptCount >= MAX_RECONCILIATION_ATTEMPTS ||
    now.getTime() - effect.submittedAt.getTime() >= MAX_RECONCILIATION_AGE_MS
  );
}

function nextReconciliationAt(attempt: number, now: Date): Date {
  const delay = Math.min(5 * 60_000, 60_000 * 2 ** Math.max(0, attempt - 1));
  return new Date(now.getTime() + delay);
}

function auditReceipt(
  receipt: LinqMessageDeliveryReceipt,
  outcome: "chat_mismatch" | "deadline_exhausted" | "failure_grace" | "terminal" | "unresolved",
): Record<string, unknown> {
  return {
    kind: "linq_message_delivery_lookup",
    outcome,
    providerChatId: receipt.providerChatId,
    providerMessageId: receipt.providerMessageId,
    providerDeliveryStatus: receipt.providerDeliveryStatus,
    createdAt: receipt.createdAt,
    updatedAt: receipt.updatedAt,
  };
}

function normalizeProviderCode(value: string | number | null): string | number | null {
  return typeof value === "string" ? value.slice(0, 100) : value;
}
