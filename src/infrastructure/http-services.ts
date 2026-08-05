import { createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { z } from "zod";
import type { GmailPubSubEvent, GoogleOAuthGrant } from "../adapters/google/index.js";
import type { LinqInboundEvent } from "../adapters/linq/index.js";
import type {
  DurableIngress,
  GoogleOAuthCompletionResult,
  GoogleOAuthHandoff,
  GoogleOAuthStartResult,
  ReadinessProbe,
} from "../http/index.js";
import type { SecretBox } from "../security/secret-box.js";

const handoffPayloadSchema = z.strictObject({
  version: z.literal(1),
  householdId: z.uuid(),
  adultId: z.uuid(),
  returnConversationId: z.string().min(1).max(500),
  accountLabel: z.string().trim().min(1).max(200),
  loginHint: z.email().optional(),
  issuedAt: z.number().int().nonnegative(),
  expiresAt: z.number().int().positive(),
  nonce: z.string().min(16).max(200),
});

type GoogleHandoffPayload = z.infer<typeof handoffPayloadSchema>;

const oauthCiphertextSchema = z.strictObject({
  householdId: z.uuid(),
  adultId: z.uuid(),
  returnConversationId: z.string().min(1).max(500),
  accountLabel: z.string().trim().min(1).max(200),
  stateHash: z.string().regex(/^[a-f0-9]{64}$/u),
  codeVerifier: z.string().min(1),
});

export interface ProviderIngressStore {
  ingestProviderEvent(input: {
    provider: string;
    idempotencyKey: string;
    authentication: Record<string, unknown>;
    eventKind: string;
    occurredAt: string;
    payload: Record<string, unknown>;
  }): Promise<{ disposition: "accepted" | "duplicate" | "quarantined" }>;
}

export class DurableProviderIngress implements DurableIngress {
  public constructor(private readonly store: ProviderIngressStore) {}

  public async acceptLinq(event: LinqInboundEvent): Promise<void> {
    const receipt = await this.store.ingestProviderEvent({
      provider: "linq",
      idempotencyKey: event.dedupeKey,
      authentication: {
        verified: true,
        webhookVersion: event.webhookVersion,
        partnerId: event.partnerId,
      },
      eventKind: event.eventType,
      occurredAt: event.occurredAt,
      payload: jsonObject(event),
    });
    if (receipt.disposition === "quarantined") {
      throw new Error("Linq delivery conflicted with a previously authenticated event");
    }
  }

  public async acceptGmailPush(event: GmailPubSubEvent): Promise<void> {
    const receipt = await this.store.ingestProviderEvent({
      provider: "gmail",
      idempotencyKey: `gmail:pubsub:${event.providerEventId}`,
      authentication: { verified: true, subscription: event.subscription },
      eventKind: "gmail.history_available",
      occurredAt: event.publishedAt,
      payload: jsonObject(event),
    });
    if (receipt.disposition === "quarantined") {
      throw new Error("Gmail delivery conflicted with a previously authenticated event");
    }
  }
}

export interface GoogleOAuthPort {
  createAuthorizationRequest(input: { state: string; loginHint?: string }): {
    url: string;
    codeVerifier: string;
  };
  completeCallback(input: {
    expectedState: string;
    returnedState: string;
    code: string;
    codeVerifier: string;
    error?: string;
  }): Promise<GoogleOAuthGrant>;
}

export interface GoogleOAuthStore {
  createOAuthState(input: {
    householdId: string;
    adultId: string;
    provider: string;
    stateHash: string;
    returnConversationId: string;
    expiresAt: string;
    encryptedPayload: string;
  }): Promise<{ stateId: string }>;
  consumeOAuthState(input: { provider: string; stateHash: string; consumedAt: string }): Promise<{
    stateId: string;
    householdId: string;
    adultId: string;
    returnConversationId: string;
    encryptedPayload: string;
  } | null>;
  upsertExternalConnection(input: {
    householdId: string;
    adultId: string;
    provider: "google";
    label: string;
    externalAccountId: string;
    email?: string;
    encryptedCredentials: string;
    grantedScopes: string[];
    cursor: Record<string, unknown>;
    metadata: Record<string, unknown>;
  }): Promise<{ connectionId: string }>;
}

export interface GoogleOAuthConnectedEvent {
  householdId: string;
  adultId: string;
  returnConversationId: string;
  connectionId: string;
  accountLabel: string;
  email: string | null;
}

export interface GoogleOAuthHandoffServiceOptions {
  oauth: GoogleOAuthPort;
  store: GoogleOAuthStore;
  secretBox: SecretBox;
  handoffSecret: string;
  oauthStateTtlMs?: number;
  now?: () => Date;
  onConnected?: (event: GoogleOAuthConnectedEvent) => Promise<void>;
}

export class GoogleOAuthHandoffService implements GoogleOAuthHandoff {
  readonly #oauth: GoogleOAuthPort;
  readonly #store: GoogleOAuthStore;
  readonly #secretBox: SecretBox;
  readonly #handoffSecret: string;
  readonly #oauthStateTtlMs: number;
  readonly #now: () => Date;
  readonly #onConnected: ((event: GoogleOAuthConnectedEvent) => Promise<void>) | undefined;

  public constructor(options: GoogleOAuthHandoffServiceOptions) {
    if (Buffer.byteLength(options.handoffSecret, "utf8") < 32) {
      throw new Error("Google OAuth handoff secret must contain at least 32 bytes");
    }
    this.#oauth = options.oauth;
    this.#store = options.store;
    this.#secretBox = options.secretBox;
    this.#handoffSecret = options.handoffSecret;
    this.#oauthStateTtlMs = options.oauthStateTtlMs ?? 15 * 60_000;
    this.#now = options.now ?? (() => new Date());
    this.#onConnected = options.onConnected;
  }

  public async start(input: { handoffToken: string }): Promise<GoogleOAuthStartResult> {
    const verified = verifyGoogleHandoffToken(input.handoffToken, this.#handoffSecret, this.#now());
    if (verified.kind !== "valid") return verified;

    const state = randomBytes(32).toString("base64url");
    const stateHash = sha256(state);
    const request = this.#oauth.createAuthorizationRequest({
      state,
      ...(verified.payload.loginHint ? { loginHint: verified.payload.loginHint } : {}),
    });
    const ciphertext = this.#secretBox.seal(
      JSON.stringify({
        householdId: verified.payload.householdId,
        adultId: verified.payload.adultId,
        returnConversationId: verified.payload.returnConversationId,
        accountLabel: verified.payload.accountLabel,
        stateHash,
        codeVerifier: request.codeVerifier,
      } satisfies z.input<typeof oauthCiphertextSchema>),
      `google-oauth-state:${stateHash}`,
    );
    await this.#store.createOAuthState({
      householdId: verified.payload.householdId,
      adultId: verified.payload.adultId,
      provider: "google",
      stateHash,
      returnConversationId: verified.payload.returnConversationId,
      expiresAt: new Date(this.#now().getTime() + this.#oauthStateTtlMs).toISOString(),
      encryptedPayload: ciphertext,
    });
    return { kind: "redirect", authorizationUrl: request.url };
  }

  public async complete(input: {
    state: string;
    code: string | null;
    providerError: string | null;
  }): Promise<GoogleOAuthCompletionResult> {
    const stateHash = sha256(input.state);
    const consumed = await this.#store.consumeOAuthState({
      provider: "google",
      stateHash,
      consumedAt: this.#now().toISOString(),
    });
    if (consumed === null) return { kind: "expired" };
    if (input.providerError !== null) return { kind: "declined" };
    if (input.code === null) return { kind: "invalid" };

    let context: z.infer<typeof oauthCiphertextSchema>;
    try {
      context = oauthCiphertextSchema.parse(
        JSON.parse(this.#secretBox.open(consumed.encryptedPayload, `google-oauth-state:${stateHash}`)),
      );
    } catch {
      return { kind: "invalid" };
    }
    if (
      context.stateHash !== stateHash ||
      context.householdId !== consumed.householdId ||
      context.adultId !== consumed.adultId ||
      context.returnConversationId !== consumed.returnConversationId
    ) {
      return { kind: "invalid" };
    }

    const grant = await this.#oauth.completeCallback({
      expectedState: input.state,
      returnedState: input.state,
      code: input.code,
      codeVerifier: context.codeVerifier,
    });
    if (!grant.identity.emailVerified) return { kind: "invalid" };
    const credentialsAad = `google-connection:${context.householdId}:${context.adultId}:${grant.identity.subject}`;
    const connection = await this.#store.upsertExternalConnection({
      householdId: context.householdId,
      adultId: context.adultId,
      provider: "google",
      label: context.accountLabel,
      externalAccountId: grant.identity.subject,
      ...(grant.identity.email ? { email: grant.identity.email } : {}),
      encryptedCredentials: this.#secretBox.seal(JSON.stringify(grant.tokens), credentialsAad),
      grantedScopes: grant.tokens.scope,
      cursor: {},
      metadata: { credentialAadVersion: 1 },
    });
    await this.#onConnected?.({
      householdId: context.householdId,
      adultId: context.adultId,
      returnConversationId: context.returnConversationId,
      connectionId: connection.connectionId,
      accountLabel: context.accountLabel,
      email: grant.identity.email,
    });
    return { kind: "connected" };
  }
}

export function issueGoogleHandoffToken(
  input: {
    householdId: string;
    adultId: string;
    returnConversationId: string;
    accountLabel: string;
    loginHint?: string;
    expiresAt: Date;
  },
  secret: string,
  now = new Date(),
): string {
  if (Buffer.byteLength(secret, "utf8") < 32) {
    throw new Error("Google OAuth handoff secret must contain at least 32 bytes");
  }
  const payload = handoffPayloadSchema.parse({
    version: 1,
    householdId: input.householdId,
    adultId: input.adultId,
    returnConversationId: input.returnConversationId,
    accountLabel: input.accountLabel,
    ...(input.loginHint ? { loginHint: input.loginHint } : {}),
    issuedAt: now.getTime(),
    expiresAt: input.expiresAt.getTime(),
    nonce: randomBytes(18).toString("base64url"),
  });
  const encoded = Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
  return `${encoded}.${signature(encoded, secret)}`;
}

export class ProductionReadiness implements ReadinessProbe {
  public constructor(
    private readonly checkDatabase: () => Promise<void>,
    private readonly enabled: Readonly<Record<string, boolean>>,
  ) {}

  public async isReady(): Promise<boolean> {
    if (Object.values(this.enabled).some((value) => !value)) return false;
    try {
      await this.checkDatabase();
      return true;
    } catch {
      return false;
    }
  }
}

function verifyGoogleHandoffToken(
  token: string,
  secret: string,
  now: Date,
): { kind: "valid"; payload: GoogleHandoffPayload } | { kind: "expired" } | { kind: "invalid" } {
  const [encoded, supplied, extra] = token.split(".");
  if (!encoded || !supplied || extra !== undefined) return { kind: "invalid" };
  const expected = signature(encoded, secret);
  const suppliedBytes = Buffer.from(supplied, "utf8");
  const expectedBytes = Buffer.from(expected, "utf8");
  if (suppliedBytes.length !== expectedBytes.length || !timingSafeEqual(suppliedBytes, expectedBytes)) {
    return { kind: "invalid" };
  }
  try {
    const payload = handoffPayloadSchema.parse(
      JSON.parse(Buffer.from(encoded, "base64url").toString("utf8")),
    );
    if (payload.expiresAt <= now.getTime()) return { kind: "expired" };
    if (payload.issuedAt > now.getTime() + 60_000 || payload.expiresAt <= payload.issuedAt) {
      return { kind: "invalid" };
    }
    return { kind: "valid", payload };
  } catch {
    return { kind: "invalid" };
  }
}

function signature(encoded: string, secret: string): string {
  return createHmac("sha256", secret).update(encoded).digest("base64url");
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function jsonObject(value: unknown): Record<string, unknown> {
  const serialized = JSON.stringify(value);
  if (serialized === undefined) throw new Error("Provider event is not JSON serializable");
  const parsed: unknown = JSON.parse(serialized);
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("Provider event must serialize to an object");
  }
  return parsed as Record<string, unknown>;
}
