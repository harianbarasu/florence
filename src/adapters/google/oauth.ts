import { createHash, randomBytes } from "node:crypto";
import type { Credentials } from "google-auth-library";
import { CodeChallengeMethod, OAuth2Client } from "google-auth-library";
import type { FlorenceConfig } from "../../config.js";
import type {
  GoogleCapability,
  GoogleCredentials,
  GoogleTokenExchange,
  GoogleTokenRevocationReceipt,
} from "./contracts.js";

const GOOGLE_IDENTITY_SCOPES = ["openid", "email"] as const;
export const GOOGLE_SCOPE_BY_CAPABILITY = {
  mail: "https://www.googleapis.com/auth/gmail.readonly",
  calendar: "https://www.googleapis.com/auth/calendar.readonly",
} as const satisfies Record<GoogleCapability, string>;

export interface GooglePkce {
  verifier: string;
  challenge: string;
}

export interface GoogleIdentityExchange {
  readonly subject: string;
  readonly email: string;
}

export class GoogleTokenRevocationError extends Error {
  public constructor(
    readonly code: string,
    readonly retryable: boolean,
    readonly httpStatus: number | null,
  ) {
    super(code);
    this.name = "GoogleTokenRevocationError";
  }
}

export class GoogleOAuthAdapter {
  public constructor(private readonly config: FlorenceConfig["google"]) {}

  public createPkce(): GooglePkce {
    const verifier = randomBytes(48).toString("base64url");
    return {
      verifier,
      challenge: createHash("sha256").update(verifier).digest("base64url"),
    };
  }

  public authorizationUrl(input: {
    state: string;
    challenge: string;
    nonce: string;
    requestedCapabilities: readonly GoogleCapability[];
    loginHint?: string;
  }): string {
    const client = this.#client(this.config.redirectUri);
    return client.generateAuthUrl({
      access_type: "offline",
      prompt: "consent select_account",
      scope: googleScopes(input.requestedCapabilities),
      state: input.state,
      nonce: input.nonce,
      code_challenge: input.challenge,
      code_challenge_method: CodeChallengeMethod.S256,
      include_granted_scopes: false,
      ...(input.loginHint ? { login_hint: input.loginHint } : {}),
    });
  }

  public async exchange(
    code: string,
    verifier: string,
    expectedNonce: string,
    requestedCapabilities: readonly GoogleCapability[],
  ): Promise<GoogleTokenExchange> {
    assertCapabilities(requestedCapabilities);
    const client = this.#client(this.config.redirectUri);
    const response = await client.getToken({ code, codeVerifier: verifier });
    client.setCredentials(response.tokens);
    const identity = await this.#verifiedIdentity(client, response.tokens.id_token, expectedNonce);
    const grantedScopes = await resolveGrantedScopes(client, response.tokens);
    const grantedCapabilities = capabilitiesForScopes(grantedScopes).filter((capability) =>
      requestedCapabilities.includes(capability),
    );
    if (grantedCapabilities.length === 0) {
      throw new Error("Google did not grant Mail or Calendar access");
    }
    return {
      credentials: normalizeCredentials({ ...response.tokens, scope: grantedScopes.join(" ") }),
      subject: identity.subject,
      email: identity.email,
      grantedScopes,
      grantedCapabilities,
    };
  }

  /** Starts identity-only OIDC. It never requests or retains a Google data grant. */
  public identityAuthorizationUrl(input: {
    state: string;
    challenge: string;
    nonce: string;
    loginHint?: string;
  }): string {
    const client = this.#client(this.config.identityRedirectUri);
    return client.generateAuthUrl({
      access_type: "online",
      prompt: "select_account",
      scope: [...GOOGLE_IDENTITY_SCOPES],
      state: input.state,
      nonce: input.nonce,
      code_challenge: input.challenge,
      code_challenge_method: CodeChallengeMethod.S256,
      include_granted_scopes: false,
      ...(input.loginHint ? { login_hint: input.loginHint } : {}),
    });
  }

  /** Exchanges an identity-only code and returns no access or refresh token. */
  public async exchangeIdentity(
    code: string,
    verifier: string,
    expectedNonce: string,
  ): Promise<GoogleIdentityExchange> {
    const client = this.#client(this.config.identityRedirectUri);
    const response = await client.getToken({ code, codeVerifier: verifier });
    return this.#verifiedIdentity(client, response.tokens.id_token, expectedNonce);
  }

  public client(credentials: GoogleCredentials): OAuth2Client {
    const client = this.#client(this.config.redirectUri);
    client.setCredentials(toGoogleCredentials(credentials));
    return client;
  }

  /** Revokes the complete offline grant when a refresh token is available. */
  public async revokeToken(credentials: GoogleCredentials): Promise<GoogleTokenRevocationReceipt> {
    const token = credentials.refreshToken || credentials.accessToken;
    if (!token) {
      // There is no provider credential Florence could present or retain. This
      // is an explicit local-finalization receipt, not a claim that Google was
      // contacted.
      return { outcome: "no_token", httpStatus: 0 };
    }
    try {
      const response = await this.#client(this.config.redirectUri).revokeToken(token);
      return { outcome: "revoked", httpStatus: response.status };
    } catch (error) {
      const httpStatus = googleErrorStatus(error);
      const providerCode = googleErrorCode(error);
      if (httpStatus === 400 && providerCode === "invalid_token") {
        return { outcome: "already_invalid", httpStatus };
      }
      throw new GoogleTokenRevocationError(
        `google_token_revoke_failed:${providerCode ?? (httpStatus === null ? "network" : String(httpStatus))}`,
        httpStatus === null || httpStatus === 408 || httpStatus === 429 || httpStatus >= 500,
        httpStatus,
      );
    }
  }

  async #verifiedIdentity(
    client: OAuth2Client,
    idToken: string | null | undefined,
    expectedNonce: string,
  ): Promise<GoogleIdentityExchange> {
    const ticket = await client.verifyIdToken({
      idToken: requireString(idToken, "Google did not return an ID token"),
      audience: this.config.clientId,
    });
    const payload = ticket.getPayload();
    if (payload?.nonce !== expectedNonce) throw new Error("Google ID token nonce does not match");
    const subject = requireString(payload.sub, "Google ID token has no subject");
    const email = requireString(payload.email, "Google ID token has no email");
    if (payload.email_verified !== true) throw new Error("Google email is not verified");
    return {
      subject,
      email: email.toLowerCase(),
    };
  }

  #client(redirectUri: string): OAuth2Client {
    return new OAuth2Client({
      clientId: this.config.clientId,
      clientSecret: this.config.clientSecret,
      redirectUri,
    });
  }
}

export function googleScopes(capabilities: readonly GoogleCapability[]): string[] {
  assertCapabilities(capabilities);
  return [
    ...GOOGLE_IDENTITY_SCOPES,
    ...capabilities.map((capability) => GOOGLE_SCOPE_BY_CAPABILITY[capability]),
  ];
}

function capabilitiesForScopes(scopes: readonly string[]): GoogleCapability[] {
  const granted = new Set(scopes);
  return (Object.entries(GOOGLE_SCOPE_BY_CAPABILITY) as [GoogleCapability, string][])
    .filter(([, scope]) => granted.has(scope))
    .map(([capability]) => capability);
}

async function resolveGrantedScopes(client: OAuth2Client, credentials: Credentials): Promise<string[]> {
  const responseScopes = splitScopes(credentials.scope);
  if (responseScopes.length > 0) return responseScopes;
  const accessToken = requireString(
    credentials.access_token,
    "Google did not return an access token for scope verification",
  );
  const tokenInfo = await client.getTokenInfo(accessToken);
  return [...new Set(tokenInfo.scopes.filter(Boolean))];
}

function splitScopes(value: string | null | undefined): string[] {
  return [...new Set((value ?? "").split(/\s+/u).filter(Boolean))];
}

function assertCapabilities(capabilities: readonly GoogleCapability[]): void {
  if (capabilities.length === 0) throw new Error("At least one Google capability is required");
  if (new Set(capabilities).size !== capabilities.length) {
    throw new Error("Google capabilities must be unique");
  }
}

export function normalizeCredentials(credentials: Credentials): GoogleCredentials {
  return {
    ...(credentials.access_token ? { accessToken: credentials.access_token } : {}),
    ...(credentials.refresh_token ? { refreshToken: credentials.refresh_token } : {}),
    ...(credentials.expiry_date ? { expiryDate: credentials.expiry_date } : {}),
    ...(credentials.scope ? { scope: credentials.scope } : {}),
    ...(credentials.token_type ? { tokenType: credentials.token_type } : {}),
  };
}

function toGoogleCredentials(credentials: GoogleCredentials): Credentials {
  return {
    ...(credentials.accessToken ? { access_token: credentials.accessToken } : {}),
    ...(credentials.refreshToken ? { refresh_token: credentials.refreshToken } : {}),
    ...(credentials.expiryDate ? { expiry_date: credentials.expiryDate } : {}),
    ...(credentials.scope ? { scope: credentials.scope } : {}),
    ...(credentials.tokenType ? { token_type: credentials.tokenType } : {}),
  };
}

function requireString(value: string | null | undefined, message: string): string {
  if (!value) throw new Error(message);
  return value;
}

function googleErrorStatus(error: unknown): number | null {
  if (!isRecord(error)) return null;
  if (typeof error.code === "number") return error.code;
  if (typeof error.code === "string" && /^\d{3}$/u.test(error.code)) return Number(error.code);
  return isRecord(error.response) && typeof error.response.status === "number" ? error.response.status : null;
}

function googleErrorCode(error: unknown): string | null {
  if (!isRecord(error)) return null;
  const candidates = [
    error.error,
    isRecord(error.response) && isRecord(error.response.data) ? error.response.data.error : null,
  ];
  return candidates.find((candidate): candidate is string => typeof candidate === "string") ?? null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
