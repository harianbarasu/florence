import type { GoogleCredentials } from "../../adapters/google/contracts.js";
import { type GoogleOAuthAdapter, GoogleTokenRevocationError } from "../../adapters/google/oauth.js";
import type { FlorenceApplication } from "../../application/florence-application.js";
import type { ClaimedEffect, ClaimedSubmittedEffect, EffectOutbox } from "./outbox.js";

export class GoogleTokenRevocationEffectExecutor {
  public constructor(
    private readonly google: Pick<GoogleOAuthAdapter, "revokeToken">,
    private readonly outbox: Pick<
      EffectOutbox,
      "reauthorizeGoogleTokenRevocation" | "recordReconciliation" | "retry"
    >,
    private readonly application: Pick<FlorenceApplication, "process">,
  ) {}

  public async execute(effect: ClaimedEffect): Promise<void> {
    await this.attempt(effect);
  }

  public async reconcile(effect: ClaimedSubmittedEffect): Promise<void> {
    await this.attempt(effect);
  }

  private async attempt(effect: ClaimedEffect | ClaimedSubmittedEffect): Promise<void> {
    if (effect.effectKind !== "google.oauth_token.revoke") return;

    let authorized:
      | { readonly authorized: false }
      | {
          readonly authorized: true;
          readonly integrationId: string;
          readonly expectedIntegrationControlEpoch: number;
          readonly result: Awaited<ReturnType<GoogleOAuthAdapter["revokeToken"]>>;
        };
    try {
      authorized = await this.outbox.reauthorizeGoogleTokenRevocation(effect, (stored) =>
        this.google.revokeToken(googleCredentials(stored)),
      );
    } catch (error) {
      await this.fail(
        effect,
        error instanceof GoogleTokenRevocationError ? error.code : "google_token_revoke_unexpected_failure",
        error instanceof GoogleTokenRevocationError ? error.httpStatus : null,
        error instanceof GoogleTokenRevocationError ? error.retryable : null,
      );
      return;
    }
    if (!authorized.authorized) return;

    const occurredAt = new Date();
    try {
      await this.application.process({
        kind: "google.oauth.revoke_receipt",
        outboxId: effect.outboxId,
        leaseToken: effect.leaseToken,
        idempotencyKey: effect.idempotencyKey,
        integrationId: authorized.integrationId,
        expectedIntegrationControlEpoch: authorized.expectedIntegrationControlEpoch,
        receipt: authorized.result,
        occurredAt: occurredAt.toISOString(),
      });
    } catch {
      // The provider mutation has already crossed the one-way boundary and the
      // outbox is `submitted`. Its reconciliation lease will safely revoke the
      // same token again and re-enter the application receipt seam.
    }
  }

  private async fail(
    effect: ClaimedEffect | ClaimedSubmittedEffect,
    errorCode: string,
    httpStatus: number | null = null,
    providerRetryable: boolean | null = null,
  ): Promise<void> {
    if ("reconciliationAttemptCount" in effect) {
      await this.outbox.recordReconciliation({
        effect,
        status: "submitted",
        receipt: {
          kind: "google_oauth_token_revocation_retry",
          errorCode,
          retryable: true,
          ...(providerRetryable === null ? {} : { providerRetryable }),
          ...(httpStatus === null ? {} : { httpStatus }),
        },
        errorCode,
        nextAttemptAt: revocationRetryAt(effect.reconciliationAttemptCount),
      });
      return;
    }
    await this.outbox.retry(effect, errorCode, true);
  }
}

function googleCredentials(stored: Readonly<Record<string, unknown>>): GoogleCredentials {
  return {
    ...(typeof stored.accessToken === "string" ? { accessToken: stored.accessToken } : {}),
    ...(typeof stored.refreshToken === "string" ? { refreshToken: stored.refreshToken } : {}),
    ...(typeof stored.expiryDate === "number" ? { expiryDate: stored.expiryDate } : {}),
    ...(typeof stored.scope === "string" ? { scope: stored.scope } : {}),
    ...(typeof stored.tokenType === "string" ? { tokenType: stored.tokenType } : {}),
  };
}

function revocationRetryAt(attempt: number, now = new Date()): Date {
  const delay = Math.min(15 * 60_000, 1_000 * 2 ** Math.min(10, Math.max(0, attempt - 1)));
  return new Date(now.getTime() + delay);
}
