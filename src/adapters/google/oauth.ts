import { createHash, randomBytes } from "node:crypto";
import type { Credentials } from "google-auth-library";
import { CodeChallengeMethod, OAuth2Client } from "google-auth-library";
import type { FlorenceConfig } from "../../config.js";
import type { GoogleCapability, GoogleCredentials, GoogleTokenExchange } from "./contracts.js";

const GOOGLE_IDENTITY_SCOPES = ["openid", "email"] as const;
export const GOOGLE_SCOPE_BY_CAPABILITY = {
  mail: "https://www.googleapis.com/auth/gmail.readonly",
  calendar: "https://www.googleapis.com/auth/calendar.readonly",
} as const satisfies Record<GoogleCapability, string>;

export interface GooglePkce {
  verifier: string;
  challenge: string;
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
    requestedCapabilities: readonly GoogleCapability[];
    loginHint?: string;
  }): string {
    const client = this.#client();
    return client.generateAuthUrl({
      access_type: "offline",
      prompt: "consent select_account",
      scope: googleScopes(input.requestedCapabilities),
      state: input.state,
      code_challenge: input.challenge,
      code_challenge_method: CodeChallengeMethod.S256,
      include_granted_scopes: false,
      ...(input.loginHint ? { login_hint: input.loginHint } : {}),
    });
  }

  public async exchange(
    code: string,
    verifier: string,
    requestedCapabilities: readonly GoogleCapability[],
  ): Promise<GoogleTokenExchange> {
    assertCapabilities(requestedCapabilities);
    const client = this.#client();
    const response = await client.getToken({ code, codeVerifier: verifier });
    client.setCredentials(response.tokens);
    const ticket = await client.verifyIdToken({
      idToken: requireString(response.tokens.id_token, "Google did not return an ID token"),
      audience: this.config.clientId,
    });
    const payload = ticket.getPayload();
    const subject = requireString(payload?.sub, "Google ID token has no subject");
    const email = requireString(payload?.email, "Google ID token has no email");
    if (payload?.email_verified !== true) throw new Error("Google email is not verified");
    const grantedScopes = await resolveGrantedScopes(client, response.tokens);
    const grantedCapabilities = capabilitiesForScopes(grantedScopes).filter((capability) =>
      requestedCapabilities.includes(capability),
    );
    if (grantedCapabilities.length === 0) {
      throw new Error("Google did not grant Mail or Calendar access");
    }
    return {
      credentials: normalizeCredentials({ ...response.tokens, scope: grantedScopes.join(" ") }),
      subject,
      email: email.toLowerCase(),
      grantedScopes,
      grantedCapabilities,
    };
  }

  public client(credentials: GoogleCredentials): OAuth2Client {
    const client = this.#client();
    client.setCredentials(toGoogleCredentials(credentials));
    return client;
  }

  #client(): OAuth2Client {
    return new OAuth2Client({
      clientId: this.config.clientId,
      clientSecret: this.config.clientSecret,
      redirectUri: this.config.redirectUri,
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
    ...(credentials.id_token ? { idToken: credentials.id_token } : {}),
  };
}

function toGoogleCredentials(credentials: GoogleCredentials): Credentials {
  return {
    ...(credentials.accessToken ? { access_token: credentials.accessToken } : {}),
    ...(credentials.refreshToken ? { refresh_token: credentials.refreshToken } : {}),
    ...(credentials.expiryDate ? { expiry_date: credentials.expiryDate } : {}),
    ...(credentials.scope ? { scope: credentials.scope } : {}),
    ...(credentials.tokenType ? { token_type: credentials.tokenType } : {}),
    ...(credentials.idToken ? { id_token: credentials.idToken } : {}),
  };
}

function requireString(value: string | null | undefined, message: string): string {
  if (!value) throw new Error(message);
  return value;
}
