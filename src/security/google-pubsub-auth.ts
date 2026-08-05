import { OAuth2Client } from "google-auth-library";

const GOOGLE_TOKEN_ISSUERS = new Set(["accounts.google.com", "https://accounts.google.com"]);
const MAX_AUTHORIZATION_HEADER_LENGTH = 16_384;
const bearerTokenPattern = /^Bearer ([A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)$/iu;

export interface GoogleIdTokenPayload {
  readonly aud?: string;
  readonly email?: string;
  readonly email_verified?: boolean;
  readonly iss?: string;
}

export interface GoogleIdTokenVerifier {
  verifyIdToken(input: {
    readonly idToken: string;
    readonly audience: string;
  }): Promise<{ getPayload(): GoogleIdTokenPayload | undefined }>;
}

export interface GooglePubSubAuthorization {
  readonly authorizationHeader: string | undefined;
  readonly expectedAudience: string;
  readonly expectedServiceAccountEmail: string;
}

/** Verifies Google signature/time/issuer/audience and the configured Pub/Sub service identity. */
export class GooglePubSubOidcAuthenticator {
  readonly #verifier: GoogleIdTokenVerifier;

  public constructor(
    verifier: GoogleIdTokenVerifier = new OAuth2Client({
      issuers: [...GOOGLE_TOKEN_ISSUERS],
    }),
  ) {
    this.#verifier = verifier;
  }

  public async authenticate(input: GooglePubSubAuthorization): Promise<boolean> {
    const idToken = bearerToken(input.authorizationHeader);
    if (idToken === null) return false;

    try {
      const ticket = await this.#verifier.verifyIdToken({
        idToken,
        audience: input.expectedAudience,
      });
      const payload = ticket.getPayload();
      return Boolean(
        payload &&
          payload.aud === input.expectedAudience &&
          payload.email === input.expectedServiceAccountEmail &&
          payload.email_verified === true &&
          payload.iss !== undefined &&
          GOOGLE_TOKEN_ISSUERS.has(payload.iss),
      );
    } catch {
      return false;
    }
  }
}

function bearerToken(header: string | undefined): string | null {
  if (!header || header.length > MAX_AUTHORIZATION_HEADER_LENGTH) return null;
  return bearerTokenPattern.exec(header)?.[1] ?? null;
}
