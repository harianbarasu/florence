import { describe, expect, it } from "vitest";
import { GOOGLE_SCOPE_BY_CAPABILITY, GoogleOAuthAdapter } from "../../src/adapters/google/oauth.js";
import {
  BeginGoogleAuthAttemptInputSchema,
  CompleteGoogleLoginInputSchema,
  GOOGLE_IDENTITY_LINK_ACTION,
  GOOGLE_IDENTITY_LINK_RETURN_PATH,
  GOOGLE_IDENTITY_REVOKE_ACTION,
  GoogleIdentityRevokeAssuranceContextSchema,
  hasFreshGoogleIdentityLinkAssurance,
} from "../../src/modules/auth/contracts.js";
import {
  assertGoogleSourceStartPolicy,
  googleAuthResultPath,
  safeGoogleAuthReturnPath,
} from "../../src/server.js";

const personId = "10000000-0000-4000-8000-000000000001";
const sessionId = "10000000-0000-4000-8000-000000000002";

describe("Google login contracts", () => {
  it("keeps anonymous login separate from an exact-session account link", () => {
    expect(BeginGoogleAuthAttemptInputSchema.parse({ mode: "login", returnPath: "/home" })).toEqual({
      mode: "login",
      returnPath: "/home",
    });
    expect(
      BeginGoogleAuthAttemptInputSchema.parse({
        mode: "link",
        personId,
        initiatingSessionId: sessionId,
        personControlEpoch: 3,
        returnPath: "/onboarding",
      }),
    ).toEqual({
      mode: "link",
      personId,
      initiatingSessionId: sessionId,
      personControlEpoch: 3,
      returnPath: "/onboarding",
    });
    expect(
      BeginGoogleAuthAttemptInputSchema.safeParse({ mode: "link", returnPath: "/onboarding" }).success,
    ).toBe(false);
    expect(
      BeginGoogleAuthAttemptInputSchema.safeParse({ mode: "login", returnPath: "https://evil.test" }).success,
    ).toBe(false);
  });

  it("accepts only exact callback proof shapes", () => {
    expect(
      CompleteGoogleLoginInputSchema.safeParse({
        state: "s".repeat(43),
        browserBinding: "b".repeat(43),
        externalSubjectDigest: "a".repeat(64),
      }).success,
    ).toBe(true);
    expect(
      CompleteGoogleLoginInputSchema.safeParse({
        state: "s".repeat(43),
        browserBinding: "b".repeat(43),
        externalSubjectDigest: "google-email@example.test",
      }).success,
    ).toBe(false);
  });

  it("requires onboarding for the first Google identity and exact account controls thereafter", () => {
    const asOf = new Date("2026-08-09T12:00:00.000Z");
    const expiresAt = new Date("2026-08-09T12:10:00.000Z");
    expect(
      hasFreshGoogleIdentityLinkAssurance({
        hasVerifiedGoogleIdentity: false,
        assuranceKind: "onboarding",
        assuranceContext: {},
        assuranceExpiresAt: expiresAt,
        asOf,
      }),
    ).toBe(true);
    expect(
      hasFreshGoogleIdentityLinkAssurance({
        hasVerifiedGoogleIdentity: false,
        assuranceKind: "account_controls",
        assuranceContext: {
          action: GOOGLE_IDENTITY_LINK_ACTION,
          returnPath: GOOGLE_IDENTITY_LINK_RETURN_PATH,
        },
        assuranceExpiresAt: expiresAt,
        asOf,
      }),
    ).toBe(false);

    const subsequent = {
      hasVerifiedGoogleIdentity: true,
      assuranceKind: "account_controls" as const,
      assuranceContext: {
        action: GOOGLE_IDENTITY_LINK_ACTION,
        returnPath: GOOGLE_IDENTITY_LINK_RETURN_PATH,
      },
      assuranceExpiresAt: expiresAt,
      asOf,
    };
    expect(hasFreshGoogleIdentityLinkAssurance(subsequent)).toBe(true);
    expect(
      hasFreshGoogleIdentityLinkAssurance({
        ...subsequent,
        assuranceContext: { ...subsequent.assuranceContext, unrelated: "value" },
      }),
    ).toBe(false);
    expect(
      hasFreshGoogleIdentityLinkAssurance({
        ...subsequent,
        assuranceExpiresAt: asOf,
      }),
    ).toBe(false);
  });

  it("binds sign-in removal assurance to one exact provider identity", () => {
    const identityId = "10000000-0000-4000-8000-000000000003";
    expect(
      GoogleIdentityRevokeAssuranceContextSchema.parse({
        action: GOOGLE_IDENTITY_REVOKE_ACTION,
        identityId,
        returnPath: "/safety",
      }),
    ).toEqual({ action: GOOGLE_IDENTITY_REVOKE_ACTION, identityId, returnPath: "/safety" });
    expect(
      GoogleIdentityRevokeAssuranceContextSchema.safeParse({
        action: GOOGLE_IDENTITY_REVOKE_ACTION,
        identityId: personId,
        returnPath: "/safety",
        unrelated: identityId,
      }).success,
    ).toBe(false);
  });

  it("requests identity only for login and retains nonce on connector authorization", () => {
    const adapter = new GoogleOAuthAdapter({
      clientId: "client-id.apps.googleusercontent.com",
      clientSecret: "client-secret",
      redirectUri: "https://florence.example/oauth/google/callback",
      identityRedirectUri: "https://florence.example/auth/google/callback",
    });
    const login = new URL(
      adapter.identityAuthorizationUrl({
        state: "state",
        challenge: "challenge",
        nonce: "nonce",
      }),
    );
    const loginScopes = new Set(login.searchParams.get("scope")?.split(" "));
    expect(login.searchParams.get("access_type")).toBe("online");
    expect(login.searchParams.get("prompt")).toBe("select_account");
    expect(login.searchParams.get("redirect_uri")).toBe("https://florence.example/auth/google/callback");
    expect(login.searchParams.get("nonce")).toBe("nonce");
    expect(loginScopes).toEqual(new Set(["openid", "email"]));
    expect(loginScopes.has(GOOGLE_SCOPE_BY_CAPABILITY.mail)).toBe(false);
    expect(loginScopes.has(GOOGLE_SCOPE_BY_CAPABILITY.calendar)).toBe(false);

    const connector = new URL(
      adapter.authorizationUrl({
        state: "connector-state",
        challenge: "connector-challenge",
        nonce: "connector-nonce",
        requestedCapabilities: ["calendar"],
      }),
    );
    expect(connector.searchParams.get("nonce")).toBe("connector-nonce");
    expect(connector.searchParams.get("scope")?.split(" ")).toContain(GOOGLE_SCOPE_BY_CAPABILITY.calendar);
  });

  it("redirects only to Florence-owned app routes after Google", () => {
    expect(safeGoogleAuthReturnPath("/sources")).toBe("/sources");
    expect(safeGoogleAuthReturnPath("/onboarding?connected=1")).toBe("/onboarding");
    expect(safeGoogleAuthReturnPath("//evil.test/sources")).toBe("/home");
    expect(safeGoogleAuthReturnPath("https://evil.test/sources")).toBe("/home");
  });

  it("returns controlled login and linking errors without losing a safe destination", () => {
    expect(googleAuthResultPath("link", "/onboarding", "account_conflict")).toBe(
      "/link-account?error=account_conflict",
    );
    expect(googleAuthResultPath("login", "/sources", "not_linked")).toBe(
      "/sign-in?error=not_linked&returnTo=%2Fsources",
    );
  });

  it("limits incomplete onboarding to one primary personal Google source", () => {
    expect(() =>
      assertGoogleSourceStartPolicy({
        onboardingCompleted: false,
        onboardingStep: "google",
        from: "onboarding",
        profile: "personal_family",
        includeWorkMail: false,
        reconnectAccountKind: null,
        existingGoogleIntegrations: false,
      }),
    ).not.toThrow();
    expect(() =>
      assertGoogleSourceStartPolicy({
        onboardingCompleted: false,
        onboardingStep: "google",
        from: "onboarding",
        profile: "personal_family",
        includeWorkMail: false,
        reconnectAccountKind: "personal_family",
        existingGoogleIntegrations: true,
      }),
    ).not.toThrow();
    for (const blocked of [
      {
        from: "sources" as const,
        profile: "personal_family" as const,
        includeWorkMail: false,
        reconnectAccountKind: null,
        existingGoogleIntegrations: false,
        onboardingStep: "google" as const,
      },
      {
        from: "onboarding" as const,
        profile: "work" as const,
        includeWorkMail: true,
        reconnectAccountKind: null,
        existingGoogleIntegrations: false,
        onboardingStep: "google" as const,
      },
      {
        from: "onboarding" as const,
        profile: "personal_family" as const,
        includeWorkMail: false,
        reconnectAccountKind: null,
        existingGoogleIntegrations: true,
        onboardingStep: "google" as const,
      },
      {
        from: "onboarding" as const,
        profile: "personal_family" as const,
        includeWorkMail: false,
        reconnectAccountKind: null,
        existingGoogleIntegrations: false,
        onboardingStep: "children" as const,
      },
    ]) {
      expect(() => assertGoogleSourceStartPolicy({ onboardingCompleted: false, ...blocked })).toThrow(
        "Finish family onboarding",
      );
    }
  });
});
