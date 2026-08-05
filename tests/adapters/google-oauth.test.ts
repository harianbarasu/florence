import { describe, expect, it, vi } from "vitest";
import {
  DEFAULT_GOOGLE_SCOPES,
  GoogleOAuthAdapter,
  type GoogleOAuthClientPort,
  parseGoogleAdapterConfig,
} from "../../src/adapters/google/index.js";

const CONFIG = parseGoogleAdapterConfig({
  clientId: "synthetic-client-id",
  clientSecret: "synthetic-client-secret",
  redirectUri: "https://florence.example.test/oauth/google/callback",
});

function oauthPort(overrides: Partial<GoogleOAuthClientPort> = {}): GoogleOAuthClientPort {
  return {
    generateAuthUrl: vi.fn((options) => {
      const url = new URL("https://accounts.google.example.test/o/oauth2/v2/auth");
      url.searchParams.set("state", options.state);
      url.searchParams.set("code_challenge", options.code_challenge);
      url.searchParams.set("scope", options.scope.join(" "));
      return url.toString();
    }),
    getToken: vi.fn(async () => ({
      tokens: {
        access_token: "opaque-access-token",
        refresh_token: "opaque-refresh-token",
        id_token: "opaque-id-token",
        expiry_date: Date.parse("2026-08-05T18:00:00.000Z"),
        scope: DEFAULT_GOOGLE_SCOPES.join(" "),
        token_type: "Bearer",
      },
    })),
    setCredentials: vi.fn(),
    refreshAccessToken: vi.fn(async () => ({
      credentials: {
        access_token: "opaque-refreshed-access-token",
        expiry_date: Date.parse("2026-08-05T19:00:00.000Z"),
      },
    })),
    revokeToken: vi.fn(async () => undefined),
    verifyIdToken: vi.fn(async () => ({
      getPayload: () => ({
        sub: "google-subject-001",
        email: "parent@example.test",
        email_verified: true,
      }),
    })),
    ...overrides,
  };
}

describe("Google OAuth adapter", () => {
  it("generates an offline PKCE authorization request with narrow scopes", () => {
    const port = oauthPort();
    const adapter = new GoogleOAuthAdapter(CONFIG, () => port);
    const request = adapter.createAuthorizationRequest({ state: "csrf-state-001" });
    const url = new URL(request.url);

    expect(request.codeVerifier.length).toBeGreaterThanOrEqual(43);
    expect(url.searchParams.get("state")).toBe("csrf-state-001");
    expect(url.searchParams.get("code_challenge")).toHaveLength(43);
    expect(url.searchParams.get("scope")?.split(" ")).toEqual(DEFAULT_GOOGLE_SCOPES);
    expect(port.generateAuthUrl).toHaveBeenCalledWith(
      expect.objectContaining({
        access_type: "offline",
        include_granted_scopes: true,
        prompt: "consent select_account",
        code_challenge_method: "S256",
      }),
    );
  });

  it("validates callback state, verifies identity, and returns opaque tokens", async () => {
    const port = oauthPort();
    const adapter = new GoogleOAuthAdapter(CONFIG, () => port);

    await expect(
      adapter.completeCallback({
        expectedState: "state-001",
        returnedState: "state-001",
        code: "authorization-code",
        codeVerifier: "pkce-verifier",
      }),
    ).resolves.toEqual({
      tokens: {
        accessToken: "opaque-access-token",
        refreshToken: "opaque-refresh-token",
        idToken: "opaque-id-token",
        expiresAt: "2026-08-05T18:00:00.000Z",
        scope: [...DEFAULT_GOOGLE_SCOPES],
        tokenType: "Bearer",
      },
      identity: {
        subject: "google-subject-001",
        email: "parent@example.test",
        emailVerified: true,
      },
    });
    expect(port.verifyIdToken).toHaveBeenCalledWith({
      idToken: "opaque-id-token",
      audience: "synthetic-client-id",
    });
  });

  it("fails before token exchange when callback state does not match", async () => {
    const port = oauthPort();
    const adapter = new GoogleOAuthAdapter(CONFIG, () => port);

    await expect(
      adapter.completeCallback({
        expectedState: "state-001",
        returnedState: "state-002",
        code: "authorization-code",
        codeVerifier: "pkce-verifier",
      }),
    ).rejects.toMatchObject({ code: "unauthorized" });
    expect(port.getToken).not.toHaveBeenCalled();
  });

  it("refreshes without losing a refresh token omitted by Google", async () => {
    const port = oauthPort();
    const adapter = new GoogleOAuthAdapter(CONFIG, () => port);
    const refreshed = await adapter.refresh({
      accessToken: "opaque-old-access-token",
      refreshToken: "opaque-refresh-token",
      idToken: "opaque-id-token",
      expiresAt: "2026-08-05T17:00:00.000Z",
      scope: [...DEFAULT_GOOGLE_SCOPES],
      tokenType: "Bearer",
    });

    expect(port.setCredentials).toHaveBeenCalledWith({
      refresh_token: "opaque-refresh-token",
    });
    expect(refreshed).toEqual({
      accessToken: "opaque-refreshed-access-token",
      refreshToken: "opaque-refresh-token",
      idToken: "opaque-id-token",
      expiresAt: "2026-08-05T19:00:00.000Z",
      scope: [...DEFAULT_GOOGLE_SCOPES],
      tokenType: "Bearer",
    });
  });

  it("revokes the refresh token without owning token storage", async () => {
    const port = oauthPort();
    const adapter = new GoogleOAuthAdapter(CONFIG, () => port);
    await adapter.revoke({
      accessToken: "opaque-access-token",
      refreshToken: "opaque-refresh-token",
      idToken: null,
      expiresAt: null,
      scope: [],
      tokenType: "Bearer",
    });

    expect(port.revokeToken).toHaveBeenCalledWith("opaque-refresh-token");
  });
});
