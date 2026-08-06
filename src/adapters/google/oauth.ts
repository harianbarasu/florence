import { createHash, randomBytes } from "node:crypto";
import type { Credentials } from "google-auth-library";
import { CodeChallengeMethod, OAuth2Client } from "google-auth-library";
import type { FlorenceConfig } from "../../config.js";
import type { GoogleCredentials, GoogleTokenExchange } from "./contracts.js";

export const GOOGLE_SCOPES = [
  "openid",
  "email",
  "https://www.googleapis.com/auth/gmail.readonly",
  "https://www.googleapis.com/auth/calendar.readonly",
] as const;

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

  public authorizationUrl(input: { state: string; challenge: string; loginHint?: string }): string {
    const client = this.#client();
    return client.generateAuthUrl({
      access_type: "offline",
      prompt: "consent",
      scope: [...GOOGLE_SCOPES],
      state: input.state,
      code_challenge: input.challenge,
      code_challenge_method: CodeChallengeMethod.S256,
      include_granted_scopes: true,
      ...(input.loginHint ? { login_hint: input.loginHint } : {}),
    });
  }

  public async exchange(code: string, verifier: string): Promise<GoogleTokenExchange> {
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
    return {
      credentials: normalizeCredentials(response.tokens),
      subject,
      email: email.toLowerCase(),
      grantedScopes: (response.tokens.scope ?? "").split(/\s+/u).filter(Boolean),
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
