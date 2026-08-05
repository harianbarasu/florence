import { createHash, randomBytes, timingSafeEqual } from "node:crypto";
import { google } from "googleapis";
import { z } from "zod";
import type { GoogleAdapterConfig } from "./config.js";
import { GoogleAdapterError, mapGoogleProviderError } from "./errors.js";

export const googleTokenSetSchema = z
  .object({
    accessToken: z.string().min(1),
    refreshToken: z.string().min(1).nullable(),
    idToken: z.string().min(1).nullable(),
    expiresAt: z.string().min(1).nullable(),
    scope: z.array(z.string().min(1)),
    tokenType: z.string().min(1).nullable(),
  })
  .strict();

export type GoogleTokenSet = z.infer<typeof googleTokenSetSchema>;

export const googleIdentitySchema = z
  .object({
    subject: z.string().min(1),
    email: z.string().email().nullable(),
    emailVerified: z.boolean(),
  })
  .strict();

export type GoogleIdentity = z.infer<typeof googleIdentitySchema>;

export interface GoogleOAuthGrant {
  tokens: GoogleTokenSet;
  identity: GoogleIdentity;
}

interface OAuthWireTokens {
  access_token?: string | null;
  refresh_token?: string | null;
  id_token?: string | null;
  expiry_date?: number | null;
  scope?: string | null;
  token_type?: string | null;
}

export interface GoogleIdTokenPayload {
  sub?: string;
  email?: string;
  email_verified?: boolean;
}

export interface GoogleOAuthClientPort {
  generateAuthUrl(options: {
    access_type: "offline";
    include_granted_scopes: true;
    prompt: string;
    scope: readonly string[];
    state: string;
    code_challenge: string;
    code_challenge_method: "S256";
    login_hint?: string;
  }): string;
  getToken(options: {
    code: string;
    codeVerifier: string;
    redirect_uri: string;
  }): Promise<{ tokens: OAuthWireTokens }>;
  setCredentials(tokens: OAuthWireTokens): void;
  refreshAccessToken(): Promise<{ credentials: OAuthWireTokens }>;
  revokeToken(token: string): Promise<unknown>;
  verifyIdToken(options: {
    idToken: string;
    audience: string;
  }): Promise<{ getPayload(): GoogleIdTokenPayload | undefined }>;
}

export type GoogleOAuthClientFactory = () => GoogleOAuthClientPort;

export interface GoogleAuthorizationRequest {
  url: string;
  codeVerifier: string;
}

export interface CompleteGoogleCallbackInput {
  expectedState: string;
  returnedState: string;
  code: string;
  codeVerifier: string;
  error?: string;
}

export class GoogleOAuthAdapter {
  readonly #clientFactory: GoogleOAuthClientFactory;

  constructor(
    private readonly config: GoogleAdapterConfig,
    clientFactory?: GoogleOAuthClientFactory,
  ) {
    this.#clientFactory = clientFactory ?? defaultOAuthClientFactory(config);
  }

  createAuthorizationRequest(input: { state: string; loginHint?: string }): GoogleAuthorizationRequest {
    if (!input.state) {
      throw new GoogleAdapterError("OAuth state is required", "invalid_request", null, false);
    }
    const codeVerifier = randomBytes(32).toString("base64url");
    const codeChallenge = createHash("sha256").update(codeVerifier).digest("base64url");
    const url = this.#clientFactory().generateAuthUrl({
      access_type: "offline",
      include_granted_scopes: true,
      prompt: "consent select_account",
      scope: this.config.scopes,
      state: input.state,
      code_challenge: codeChallenge,
      code_challenge_method: "S256",
      ...(input.loginHint ? { login_hint: input.loginHint } : {}),
    });
    return { url, codeVerifier };
  }

  async completeCallback(input: CompleteGoogleCallbackInput): Promise<GoogleOAuthGrant> {
    if (input.error) {
      throw new GoogleAdapterError("Google authorization was not granted", "unauthorized", null, false);
    }
    if (!statesMatch(input.expectedState, input.returnedState)) {
      throw new GoogleAdapterError("OAuth state validation failed", "unauthorized", null, false);
    }
    if (!input.code || !input.codeVerifier) {
      throw new GoogleAdapterError("OAuth callback is incomplete", "invalid_request", null, false);
    }

    const client = this.#clientFactory();
    let wireTokens: OAuthWireTokens;
    try {
      const response = await client.getToken({
        code: input.code,
        codeVerifier: input.codeVerifier,
        redirect_uri: this.config.redirectUri,
      });
      wireTokens = response.tokens;
    } catch (error) {
      throw mapGoogleProviderError("Google authorization-code exchange", error);
    }

    const tokens = normalizeTokens({
      ...wireTokens,
      scope: wireTokens.scope ?? this.config.scopes.join(" "),
    });
    if (tokens.idToken === null) {
      throw new GoogleAdapterError(
        "Google callback did not include an identity token",
        "unauthorized",
        null,
        false,
      );
    }

    let payload: GoogleIdTokenPayload | undefined;
    try {
      const ticket = await client.verifyIdToken({
        idToken: tokens.idToken,
        audience: this.config.clientId,
      });
      payload = ticket.getPayload();
    } catch {
      throw new GoogleAdapterError("Google identity token verification failed", "unauthorized", null, false);
    }
    if (!payload?.sub) {
      throw new GoogleAdapterError(
        "Google identity token is missing its subject",
        "unauthorized",
        null,
        false,
      );
    }

    return {
      tokens,
      identity: googleIdentitySchema.parse({
        subject: payload.sub,
        email: payload.email ?? null,
        emailVerified: payload.email_verified === true,
      }),
    };
  }

  async refresh(tokens: GoogleTokenSet): Promise<GoogleTokenSet> {
    const current = googleTokenSetSchema.parse(tokens);
    if (current.refreshToken === null) {
      throw new GoogleAdapterError("Google grant has no refresh token", "unauthorized", null, false);
    }
    const client = this.#clientFactory();
    client.setCredentials({ refresh_token: current.refreshToken });
    let refreshed: OAuthWireTokens;
    try {
      refreshed = (await client.refreshAccessToken()).credentials;
    } catch (error) {
      throw mapGoogleProviderError("Google token refresh", error);
    }

    return normalizeTokens({
      ...refreshed,
      refresh_token: refreshed.refresh_token ?? current.refreshToken,
      id_token: refreshed.id_token ?? current.idToken,
      scope: refreshed.scope ?? current.scope.join(" "),
      token_type: refreshed.token_type ?? current.tokenType,
    });
  }

  async revoke(tokens: GoogleTokenSet): Promise<void> {
    const current = googleTokenSetSchema.parse(tokens);
    const token = current.refreshToken ?? current.accessToken;
    try {
      await this.#clientFactory().revokeToken(token);
    } catch (error) {
      throw mapGoogleProviderError("Google token revocation", error);
    }
  }
}

function defaultOAuthClientFactory(config: GoogleAdapterConfig): GoogleOAuthClientFactory {
  return () => {
    const client = new google.auth.OAuth2(config.clientId, config.clientSecret, config.redirectUri);
    return {
      generateAuthUrl: (options) => {
        const { login_hint: loginHint, scope, ...requiredOptions } = options;
        const nativeOptions = {
          ...requiredOptions,
          scope: [...scope],
          ...(loginHint ? { login_hint: loginHint } : {}),
        } as Parameters<typeof client.generateAuthUrl>[0];
        return client.generateAuthUrl(nativeOptions);
      },
      getToken: async (options) => {
        const response = await client.getToken(options);
        return { tokens: response.tokens };
      },
      setCredentials: (tokens) => client.setCredentials(compactWireTokens(tokens)),
      refreshAccessToken: async () => {
        const response = await client.refreshAccessToken();
        return { credentials: response.credentials };
      },
      revokeToken: (token) => client.revokeToken(token),
      verifyIdToken: (options) => client.verifyIdToken(options),
    };
  };
}

function normalizeTokens(tokens: OAuthWireTokens): GoogleTokenSet {
  if (typeof tokens.access_token !== "string" || !tokens.access_token) {
    throw new GoogleAdapterError(
      "Google token response is missing an access token",
      "unauthorized",
      null,
      false,
    );
  }
  return googleTokenSetSchema.parse({
    accessToken: tokens.access_token,
    refreshToken: tokens.refresh_token || null,
    idToken: tokens.id_token || null,
    expiresAt:
      typeof tokens.expiry_date === "number" && Number.isFinite(tokens.expiry_date)
        ? new Date(tokens.expiry_date).toISOString()
        : null,
    scope: typeof tokens.scope === "string" ? tokens.scope.split(/\s+/).filter(Boolean) : [],
    tokenType: tokens.token_type || null,
  });
}

function compactWireTokens(tokens: OAuthWireTokens): {
  access_token?: string;
  refresh_token?: string;
  id_token?: string;
  expiry_date?: number;
  scope?: string;
  token_type?: string;
} {
  return {
    ...(tokens.access_token ? { access_token: tokens.access_token } : {}),
    ...(tokens.refresh_token ? { refresh_token: tokens.refresh_token } : {}),
    ...(tokens.id_token ? { id_token: tokens.id_token } : {}),
    ...(typeof tokens.expiry_date === "number" ? { expiry_date: tokens.expiry_date } : {}),
    ...(tokens.scope ? { scope: tokens.scope } : {}),
    ...(tokens.token_type ? { token_type: tokens.token_type } : {}),
  };
}

function statesMatch(expected: string, returned: string): boolean {
  if (!expected || !returned) {
    return false;
  }
  const expectedBytes = Buffer.from(expected, "utf8");
  const returnedBytes = Buffer.from(returned, "utf8");
  return expectedBytes.length === returnedBytes.length && timingSafeEqual(expectedBytes, returnedBytes);
}
