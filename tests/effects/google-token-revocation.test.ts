import { OAuth2Client } from "google-auth-library";
import { describe, expect, it, vi } from "vitest";
import { GoogleOAuthAdapter, GoogleTokenRevocationError } from "../../src/adapters/google/oauth.js";
import type { FlorenceApplication } from "../../src/application/florence-application.js";
import {
  type ClaimedEffect,
  type ClaimedSubmittedEffect,
  type EffectOutbox,
  GoogleTokenRevocationEffectExecutor,
} from "../../src/modules/effects/index.js";

const integrationId = "550e8400-e29b-41d4-a716-446655440000";
const outboxId = "550e8400-e29b-41d4-a716-446655440001";
const leaseToken = "550e8400-e29b-41d4-a716-446655440002";

describe("Google token revocation effect", () => {
  it("keeps credentials behind the guarded callback and reconciles through the application", async () => {
    const revokeToken = vi.fn(async () => ({ outcome: "revoked" as const, httpStatus: 200 }));
    const process = vi.fn(async () => ({
      accepted: true,
      duplicate: false,
      disposition: "google_token_revocation_reconciled",
      ids: { integrationId },
    }));
    const retry = vi.fn(async () => "retry" as const);
    const outbox = {
      reauthorizeGoogleTokenRevocation: vi.fn(async (_effect, revoke) => ({
        authorized: true as const,
        integrationId,
        expectedIntegrationControlEpoch: 8,
        result: await revoke({
          refreshToken: "refresh-token-never-in-effect",
          accessToken: "access-token-never-in-effect",
          accountEmail: "person@example.test",
        }),
      })),
      recordReconciliation: vi.fn(async () => true),
      retry,
    } as Pick<EffectOutbox, "reauthorizeGoogleTokenRevocation" | "recordReconciliation" | "retry">;
    const executor = new GoogleTokenRevocationEffectExecutor({ revokeToken }, outbox, { process } as Pick<
      FlorenceApplication,
      "process"
    >);
    const effect = claimedEffect();
    const malformedPayloadEffect = { ...effect, payload: { malformed: true } };

    await executor.execute(malformedPayloadEffect);

    expect(revokeToken).toHaveBeenCalledWith({
      refreshToken: "refresh-token-never-in-effect",
      accessToken: "access-token-never-in-effect",
    });
    expect(process).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: "google.oauth.revoke_receipt",
        outboxId,
        leaseToken,
        integrationId,
        expectedIntegrationControlEpoch: 8,
        receipt: { outcome: "revoked", httpStatus: 200 },
      }),
    );
    expect(retry).not.toHaveBeenCalled();
  });

  it("keeps submitted revocations reconcilable after a terminal-looking provider failure", async () => {
    const providerError = new GoogleTokenRevocationError("google_token_revoke_failed:400", false, 400);
    const recordReconciliation = vi.fn(async () => true);
    const outbox = {
      reauthorizeGoogleTokenRevocation: vi.fn(async (_effect, revoke) => {
        await revoke({ refreshToken: "refresh-token-never-in-effect" });
        throw new Error("unreachable");
      }),
      recordReconciliation,
      retry: vi.fn(async () => "retry" as const),
    } as Pick<EffectOutbox, "reauthorizeGoogleTokenRevocation" | "recordReconciliation" | "retry">;
    const executor = new GoogleTokenRevocationEffectExecutor(
      { revokeToken: vi.fn(async () => Promise.reject(providerError)) },
      outbox,
      { process: vi.fn() } as unknown as Pick<FlorenceApplication, "process">,
    );
    const effect: ClaimedSubmittedEffect = {
      ...claimedEffect(),
      providerReceiptId: null,
      submittedAt: new Date("2026-08-09T20:00:00Z"),
      reconciliationAttemptCount: 2,
      lastErrorCode: null,
    };

    await executor.reconcile(effect);

    expect(recordReconciliation).toHaveBeenCalledWith(
      expect.objectContaining({
        effect,
        status: "submitted",
        errorCode: "google_token_revoke_failed:400",
        nextAttemptAt: expect.any(Date),
        receipt: expect.objectContaining({ retryable: true, providerRetryable: false, httpStatus: 400 }),
      }),
    );
  });

  it("retries an initial terminal-looking provider failure instead of stranding the credential", async () => {
    const providerError = new GoogleTokenRevocationError("google_token_revoke_failed:400", false, 400);
    const retry = vi.fn(async () => "retry" as const);
    const outbox = {
      reauthorizeGoogleTokenRevocation: vi.fn(async (_effect, revoke) => {
        await revoke({ refreshToken: "refresh-token-never-in-effect" });
        throw new Error("unreachable");
      }),
      recordReconciliation: vi.fn(async () => true),
      retry,
    } as Pick<EffectOutbox, "reauthorizeGoogleTokenRevocation" | "recordReconciliation" | "retry">;
    const executor = new GoogleTokenRevocationEffectExecutor(
      { revokeToken: vi.fn(async () => Promise.reject(providerError)) },
      outbox,
      { process: vi.fn() } as unknown as Pick<FlorenceApplication, "process">,
    );

    await executor.execute(claimedEffect());

    expect(retry).toHaveBeenCalledWith(
      expect.objectContaining({ outboxId }),
      "google_token_revoke_failed:400",
      true,
    );
  });

  it("revokes the refresh token before falling back to an access token", async () => {
    const revoke = vi
      .spyOn(OAuth2Client.prototype, "revokeToken")
      .mockResolvedValue({ status: 200 } as never);
    const adapter = new GoogleOAuthAdapter({
      clientId: "client-id.apps.googleusercontent.com",
      clientSecret: "client-secret",
      redirectUri: "https://florence.example/oauth/google/callback",
      identityRedirectUri: "https://florence.example/auth/google/callback",
    });

    await expect(
      adapter.revokeToken({ refreshToken: "refresh-token", accessToken: "access-token" }),
    ).resolves.toEqual({ outcome: "revoked", httpStatus: 200 });
    expect(revoke).toHaveBeenCalledWith("refresh-token");
    revoke.mockRestore();
  });

  it("returns an explicit local-finalization receipt when no provider token is retained", async () => {
    const revoke = vi.spyOn(OAuth2Client.prototype, "revokeToken");
    const adapter = new GoogleOAuthAdapter({
      clientId: "client-id.apps.googleusercontent.com",
      clientSecret: "client-secret",
      redirectUri: "https://florence.example/oauth/google/callback",
      identityRedirectUri: "https://florence.example/auth/google/callback",
    });

    await expect(adapter.revokeToken({ scope: "openid email" })).resolves.toEqual({
      outcome: "no_token",
      httpStatus: 0,
    });
    expect(revoke).not.toHaveBeenCalled();
    revoke.mockRestore();
  });
});

function claimedEffect(): ClaimedEffect {
  return {
    outboxId,
    effectKind: "google.oauth_token.revoke",
    idempotencyKey: `google:token-revoke:${integrationId}:e8`,
    payload: { integrationId, expectedIntegrationControlEpoch: 8 },
    attemptCount: 1,
    leaseToken,
  };
}
