import { randomBytes, randomUUID } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { GoogleAdapterError } from "../../src/adapters/google/errors.js";
import type {
  CustomerCleanupConnection,
  CustomerDeletionCleanupLease,
} from "../../src/infrastructure/customer-data-control-store.js";
import { GoogleCustomerDeletionCleanup } from "../../src/infrastructure/customer-deletion-host.js";
import { googleConnectionCredentialsAad } from "../../src/infrastructure/google-sync.js";
import { SecretBox } from "../../src/security/secret-box.js";

describe("Google customer deletion cleanup", () => {
  it("treats invalid grants as terminal while retrying transient failures", async () => {
    const secretBox = new SecretBox(randomBytes(32).toString("base64url"));
    const connectionBase = {
      id: randomUUID(),
      householdId: randomUUID(),
      adultId: randomUUID(),
      externalAccountId: `subject-${randomUUID()}`,
      grantedScopes: ["gmail.readonly"],
      cursor: {},
      metadata: { credentialAadVersion: 1 },
    };
    const tokens = {
      accessToken: "expired-access-token",
      refreshToken: "revoked-refresh-token",
      idToken: null,
      expiresAt: null,
      scope: ["gmail.readonly"],
      tokenType: "Bearer",
    };
    const connection: CustomerCleanupConnection = {
      ...connectionBase,
      encryptedCredentials: secretBox.seal(
        JSON.stringify(tokens),
        googleConnectionCredentialsAad(connectionBase),
      ),
    };
    const store = {
      loadCleanupConnection: vi.fn(async () => connection),
      loadCleanupCalendarChannel: vi.fn(async () => null),
    };
    const lease = (kind: CustomerDeletionCleanupLease["kind"]): CustomerDeletionCleanupLease => ({
      rowId: randomUUID(),
      requestId: randomUUID(),
      householdId: connection.householdId,
      controlEpoch: 1,
      kind,
      connectionId: connection.id,
      calendarChannelId: null,
      attempt: 1,
      leaseToken: randomUUID(),
    });
    const permanent = new GoogleCustomerDeletionCleanup({
      store,
      gmail: {
        stopWatch: vi.fn(async () => {
          throw new GoogleAdapterError("expired", "unauthorized", 401, false);
        }),
      },
      calendar: { stopChannel: vi.fn(async () => undefined) },
      oauth: {
        refresh: vi.fn(async () => {
          throw new GoogleAdapterError("revoked", "invalid_request", 400, false);
        }),
        revoke: vi.fn(async () => {
          throw new GoogleAdapterError("already revoked", "invalid_request", 400, false);
        }),
      },
      secretBox,
    });

    await expect(permanent.execute(lease("google.gmail_watch.stop"))).resolves.toEqual({
      status: "succeeded",
    });
    await expect(permanent.execute(lease("google.oauth.revoke"))).resolves.toEqual({
      status: "succeeded",
    });

    const transient = new GoogleCustomerDeletionCleanup({
      store,
      gmail: {
        stopWatch: vi.fn(async () => {
          throw new GoogleAdapterError("expired", "unauthorized", 401, false);
        }),
      },
      calendar: { stopChannel: vi.fn(async () => undefined) },
      oauth: {
        refresh: vi.fn(async () => {
          throw new GoogleAdapterError("try later", "transient", 503, true);
        }),
        revoke: vi.fn(async () => undefined),
      },
      secretBox,
    });
    await expect(transient.execute(lease("google.gmail_watch.stop"))).resolves.toEqual({
      status: "retry",
      safeErrorCode: "google_cleanup_transient",
    });
  });
});
