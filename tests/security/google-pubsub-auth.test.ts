import { describe, expect, it, vi } from "vitest";
import {
  type GoogleIdTokenPayload,
  type GoogleIdTokenVerifier,
  GooglePubSubOidcAuthenticator,
} from "../../src/security/google-pubsub-auth.js";

const AUDIENCE = "https://florence.example.test/webhooks/google/gmail";
const SERVICE_ACCOUNT = "florence-push@example-project.iam.gserviceaccount.com";
const JWT = "eyJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJmbG9yZW5jZSJ9.c2lnbmF0dXJl";

function verifier(payload: GoogleIdTokenPayload | undefined): GoogleIdTokenVerifier {
  return {
    verifyIdToken: vi.fn(async () => ({ getPayload: () => payload })),
  };
}

function authorization(
  overrides: Partial<Parameters<GooglePubSubOidcAuthenticator["authenticate"]>[0]> = {},
) {
  return {
    authorizationHeader: `Bearer ${JWT}`,
    expectedAudience: AUDIENCE,
    expectedServiceAccountEmail: SERVICE_ACCOUNT,
    ...overrides,
  };
}

function validPayload(overrides: GoogleIdTokenPayload = {}): GoogleIdTokenPayload {
  return {
    aud: AUDIENCE,
    email: SERVICE_ACCOUNT,
    email_verified: true,
    iss: "https://accounts.google.com",
    ...overrides,
  };
}

describe("GooglePubSubOidcAuthenticator", () => {
  it("accepts only a Google-verified token with the configured audience and service identity", async () => {
    const tokenVerifier = verifier(validPayload());
    const authenticator = new GooglePubSubOidcAuthenticator(tokenVerifier);

    await expect(authenticator.authenticate(authorization())).resolves.toBe(true);
    expect(tokenVerifier.verifyIdToken).toHaveBeenCalledWith({ idToken: JWT, audience: AUDIENCE });
  });

  it.each([undefined, "", "Basic credential", "Bearer malformed", `Bearer ${"a".repeat(16_385)}`])(
    "rejects a missing or malformed Authorization header",
    async (authorizationHeader) => {
      const tokenVerifier = verifier(validPayload());
      const authenticator = new GooglePubSubOidcAuthenticator(tokenVerifier);

      await expect(authenticator.authenticate(authorization({ authorizationHeader }))).resolves.toBe(false);
      expect(tokenVerifier.verifyIdToken).not.toHaveBeenCalled();
    },
  );

  it.each([
    validPayload({ aud: "https://attacker.example.test" }),
    validPayload({ email: "other@example-project.iam.gserviceaccount.com" }),
    validPayload({ email_verified: false }),
    validPayload({ iss: "https://attacker.example.test" }),
    undefined,
  ])("rejects an unexpected signed claim set", async (payload) => {
    const authenticator = new GooglePubSubOidcAuthenticator(verifier(payload));
    await expect(authenticator.authenticate(authorization())).resolves.toBe(false);
  });

  it("fails closed when signature or token-time verification fails", async () => {
    const tokenVerifier: GoogleIdTokenVerifier = {
      verifyIdToken: vi.fn(async () => {
        throw new Error("synthetic verification detail");
      }),
    };
    const authenticator = new GooglePubSubOidcAuthenticator(tokenVerifier);

    await expect(authenticator.authenticate(authorization())).resolves.toBe(false);
  });
});
