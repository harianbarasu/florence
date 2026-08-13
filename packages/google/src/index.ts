import { createCipheriv, createDecipheriv, createHash, randomBytes, randomUUID } from "node:crypto";

export const GOOGLE_SCOPES = [
  "openid",
  "email",
  "https://www.googleapis.com/auth/gmail.readonly",
  "https://www.googleapis.com/auth/calendar.events.owned",
] as const;

export type GoogleScope = (typeof GOOGLE_SCOPES)[number];
export type GoogleConnectionStatus = "pending" | "active" | "disconnected";

export type GoogleConnectionView = {
  connectionId: string;
  householdId: string;
  ownerAdultId: string;
  status: GoogleConnectionStatus;
  emailLabel: string | null;
  grantedScopes: readonly GoogleScope[];
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
};

export type PendingGoogleConnection = {
  connectionId: string;
  householdId: string;
  ownerAdultId: string;
  stateDigest: string;
  sessionBindingDigest: string;
};

export type ActiveGoogleCredential = {
  connectionId: string;
  householdId: string;
  ownerAdultId: string;
  refreshTokenEnvelope: string;
  gmailCursor: string | null;
};

export type ClaimedGmailSync = ActiveGoogleCredential & { leaseOwner: string };

export type GmailMessageChange = {
  messageId: string;
  threadId: string;
  historyId: string;
};

export type GmailSyncBatch = {
  connectionId: string;
  householdId: string;
  ownerAdultId: string;
  leaseOwner: string;
  cursor: string | null;
  changes: readonly GmailMessageChange[];
  nextCursor: string;
};

export type GoogleCalendarDraft = {
  title: string;
  startsAt: string;
  endsAt: string;
  timeZone: string;
  location: string | null;
};

/** Structurally matches the worker's narrow, approved Calendar effect. */
export type GoogleCalendarEffect = {
  id: string;
  householdId: string;
  idempotencyKey: string;
  kind: "google.calendar.create";
  connectionId: string;
  ownerAdultId: string;
  actionId: string;
  approvalDigest: string;
  candidateId: string;
  candidateVersion: 1;
  candidateDigest: string;
  payload: GoogleCalendarDraft;
};

export type GoogleCalendarExecutionResult =
  | { status: "committed"; providerReceiptId: string; detail: string; occurredAt: string }
  | { status: "failed"; providerReceiptId: null; detail: string; occurredAt: string };

/** Matches HouseholdChiefOfStaff's private, just-in-time source-reader seam. */
export type GmailEvidence = {
  messageId: string;
  threadId: string;
  historyId: string;
  from: string;
  subject: string | null;
  sentAt: string;
  text: string;
};

/** Infrastructure seam. Credential plaintext must never cross this interface. */
export interface GoogleConnectionStore {
  createPending(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
    stateDigest: string;
    sessionBindingDigest: string;
    stateExpiresAt: string;
    now: string;
  }): Promise<GoogleConnectionView>;
  consumePendingState(input: {
    stateDigest: string;
    sessionBindingDigest: string;
    now: string;
  }): Promise<PendingGoogleConnection | null>;
  activate(input: {
    connectionId: string;
    stateDigest: string;
    googleSubjectDigest: string;
    emailLabel: string;
    grantedScopes: readonly GoogleScope[];
    refreshTokenEnvelope: string;
    now: string;
  }): Promise<GoogleConnectionView>;
  markPendingFailure(input: {
    connectionId: string;
    stateDigest: string;
    error: string;
    now: string;
  }): Promise<void>;
  listActive(input: { householdId: string; ownerAdultId: string }): Promise<readonly GoogleConnectionView[]>;
  disconnect(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
    now: string;
  }): Promise<{ view: GoogleConnectionView; refreshTokenEnvelope: string | null } | null>;
  readActiveGoogleCredential(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
  }): Promise<ActiveGoogleCredential | null>;
  claimNextGmailSync(input: {
    owner: string;
    now: string;
    leaseUntil: string;
  }): Promise<ClaimedGmailSync | null>;
  releaseGmailSync(input: {
    connectionId: string;
    owner: string;
    nextAt: string;
    cursor?: string;
    error?: string | null;
  }): Promise<void>;
}

export type BeginGoogleConnectionResult = {
  connection: GoogleConnectionView;
  authorizationUrl: string;
  expiresAt: string;
};

export type DisconnectGoogleConnectionResult = {
  connection: GoogleConnectionView;
  providerRevocation: "confirmed" | "unconfirmed" | "not-needed";
};

export class GoogleConnectionError extends Error {
  constructor(
    message: string,
    readonly code:
      | "invalid_state"
      | "provider_rejected"
      | "invalid_grant"
      | "identity_conflict"
      | "not_found",
  ) {
    super(message);
    this.name = "GoogleConnectionError";
  }
}

export type GoogleConnectionOptions = {
  store: GoogleConnectionStore;
  clientId: string;
  clientSecret: string;
  redirectUri: string;
  encryptionKey: Uint8Array;
  fetch?: typeof fetch;
  stateTtlMs?: number;
};

export class GoogleConnection {
  readonly #store: GoogleConnectionStore;
  readonly #clientId: string;
  readonly #clientSecret: string;
  readonly #redirectUri: string;
  readonly #key: Buffer;
  readonly #fetch: typeof fetch;
  readonly #stateTtlMs: number;

  constructor(options: GoogleConnectionOptions) {
    if (options.encryptionKey.byteLength !== 32) {
      throw new Error("Google refresh-token encryption key must contain exactly 32 bytes");
    }
    if (!URL.canParse(options.redirectUri)) throw new Error("Google OAuth redirect URI is invalid");
    this.#store = options.store;
    this.#clientId = required(options.clientId, "Google OAuth client ID");
    this.#clientSecret = required(options.clientSecret, "Google OAuth client secret");
    this.#redirectUri = options.redirectUri;
    this.#key = Buffer.from(options.encryptionKey);
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#stateTtlMs = options.stateTtlMs ?? 10 * 60_000;
    if (this.#stateTtlMs <= 0 || this.#stateTtlMs > 60 * 60_000) {
      throw new Error("Google OAuth state lifetime must be between 1 ms and 1 hour");
    }
  }

  async begin(input: {
    householdId: string;
    ownerAdultId: string;
    sessionBindingDigest: string;
    now: string;
  }): Promise<BeginGoogleConnectionResult> {
    const now = instant(input.now);
    const connectionId = randomUUID();
    const state = randomBytes(32).toString("base64url");
    assertDigest(input.sessionBindingDigest, "OAuth session binding");
    const expiresAt = new Date(now.getTime() + this.#stateTtlMs).toISOString();
    const connection = await this.#store.createPending({
      connectionId,
      householdId: input.householdId,
      ownerAdultId: input.ownerAdultId,
      stateDigest: digest(`oauth-state\0${state}`),
      sessionBindingDigest: input.sessionBindingDigest,
      stateExpiresAt: expiresAt,
      now: now.toISOString(),
    });
    const authorization = new URL("https://accounts.google.com/o/oauth2/v2/auth");
    authorization.search = new URLSearchParams({
      access_type: "offline",
      client_id: this.#clientId,
      include_granted_scopes: "false",
      prompt: "consent select_account",
      redirect_uri: this.#redirectUri,
      response_type: "code",
      scope: GOOGLE_SCOPES.join(" "),
      state,
    }).toString();
    return { connection, authorizationUrl: authorization.toString(), expiresAt };
  }

  async finish(input: {
    state: string;
    code: string;
    sessionBindingDigest: string;
    now: string;
  }): Promise<GoogleConnectionView> {
    const now = instant(input.now).toISOString();
    if (!input.state || !input.code)
      throw new GoogleConnectionError("OAuth state or code is missing", "invalid_state");
    assertDigest(input.sessionBindingDigest, "OAuth session binding");
    const stateDigest = digest(`oauth-state\0${input.state}`);
    const pending = await this.#store.consumePendingState({
      stateDigest,
      sessionBindingDigest: input.sessionBindingDigest,
      now,
    });
    if (!pending)
      throw new GoogleConnectionError("OAuth state is invalid, expired, or already used", "invalid_state");

    try {
      const tokenResponse = await this.#fetch("https://oauth2.googleapis.com/token", {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          client_id: this.#clientId,
          client_secret: this.#clientSecret,
          code: input.code,
          grant_type: "authorization_code",
          redirect_uri: this.#redirectUri,
        }),
      });
      if (!tokenResponse.ok) throw providerError("Google rejected the authorization code");
      const token = await safeJson(tokenResponse);
      const accessToken = stringField(token, "access_token");
      const refreshToken = stringField(token, "refresh_token");
      const scopes = exactScopes(stringField(token, "scope"));
      if (token.token_type !== "Bearer") throw providerError("Google returned an unsupported token type");

      const identityResponse = await this.#fetch("https://openidconnect.googleapis.com/v1/userinfo", {
        headers: { authorization: `Bearer ${accessToken}` },
      });
      if (!identityResponse.ok) throw providerError("Google account identity could not be verified");
      const identity = await safeJson(identityResponse);
      const subject = stringField(identity, "sub");
      const email = stringField(identity, "email");
      if (identity.email_verified !== true) throw providerError("Google account email is not verified");
      const refreshTokenEnvelope = encrypt(
        refreshToken,
        this.#key,
        aad(pending.connectionId, pending.householdId, pending.ownerAdultId),
      );
      try {
        return await this.#store.activate({
          connectionId: pending.connectionId,
          stateDigest,
          googleSubjectDigest: digest(`google-sub\0${subject}`),
          emailLabel: email,
          grantedScopes: scopes,
          refreshTokenEnvelope,
          now,
        });
      } catch (error) {
        if (isUniqueViolation(error)) {
          throw new GoogleConnectionError("This Google account is already connected", "identity_conflict");
        }
        throw error;
      }
    } catch (error) {
      await this.#store.markPendingFailure({
        connectionId: pending.connectionId,
        stateDigest,
        error: error instanceof GoogleConnectionError ? error.code : "provider_rejected",
        now,
      });
      throw error;
    }
  }

  status(input: { householdId: string; ownerAdultId: string }): Promise<readonly GoogleConnectionView[]> {
    return this.#store.listActive(input);
  }

  async disconnect(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
    now: string;
  }): Promise<DisconnectGoogleConnectionResult> {
    const disconnected = await this.#store.disconnect({ ...input, now: instant(input.now).toISOString() });
    if (!disconnected) throw new GoogleConnectionError("Google connection was not found", "not_found");
    if (!disconnected.refreshTokenEnvelope) {
      return { connection: disconnected.view, providerRevocation: "not-needed" };
    }
    let providerRevocation: DisconnectGoogleConnectionResult["providerRevocation"] = "unconfirmed";
    try {
      const token = decrypt(
        disconnected.refreshTokenEnvelope,
        this.#key,
        aad(input.connectionId, input.householdId, input.ownerAdultId),
      );
      const response = await this.#fetch("https://oauth2.googleapis.com/revoke", {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ token }),
      });
      if (response.ok) providerRevocation = "confirmed";
    } catch {
      // Local credential deletion already stopped Florence's access even if Google is unavailable.
    }
    return { connection: disconnected.view, providerRevocation };
  }

  async readGmailMessage(input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    messageId: string;
    threadId: string;
    historyId: string;
  }): Promise<GmailEvidence> {
    const credential = await this.#store.readActiveGoogleCredential(input);
    if (!credential) throw new GoogleConnectionError("Active Google connection was not found", "not_found");
    const accessToken = await this.#accessToken(credential);
    const response = await this.#fetch(
      `https://gmail.googleapis.com/gmail/v1/users/me/messages/${encodeURIComponent(input.messageId)}?format=full`,
      { headers: { authorization: `Bearer ${accessToken}` } },
    );
    if (!response.ok) throw providerError("Gmail message could not be read");
    const message = await safeJson(response);
    const messageId = stringField(message, "id");
    const threadId = stringField(message, "threadId");
    const historyId = stringField(message, "historyId");
    if (messageId !== input.messageId || threadId !== input.threadId || historyId !== input.historyId) {
      throw providerError("Gmail returned evidence for a different message");
    }
    const payload = recordField(message, "payload");
    const headers = headerMap(payload.headers);
    const from = bounded(required(headers.get("from") ?? "", "Gmail From header"), 500);
    const subject = nullableBounded(headers.get("subject"), 1_000);
    const timestamp = Number(stringField(message, "internalDate"));
    if (!Number.isFinite(timestamp)) throw providerError("Gmail returned an invalid message date");
    const body = collectPlainText(payload).trim() || stringField(message, "snippet").trim();
    if (!body) throw providerError("Gmail message has no readable text");
    return {
      messageId,
      threadId,
      historyId,
      from,
      subject,
      sentAt: new Date(timestamp).toISOString(),
      text: bounded(body, 50_000),
    };
  }

  async claimNextGmailSync(input: {
    owner: string;
    now: string;
    leaseUntil: string;
    limit?: number;
  }): Promise<GmailSyncBatch | null> {
    const now = instant(input.now);
    const leaseUntil = instant(input.leaseUntil);
    if (leaseUntil <= now) throw new Error("Gmail sync lease must end after now");
    const limit = input.limit ?? 50;
    if (!Number.isInteger(limit) || limit < 1 || limit > 50) {
      throw new Error("Gmail sync limit must be between 1 and 50");
    }
    const claim = await this.#store.claimNextGmailSync({
      owner: required(input.owner, "Gmail sync owner"),
      now: now.toISOString(),
      leaseUntil: leaseUntil.toISOString(),
    });
    if (!claim) return null;
    try {
      const accessToken = await this.#accessToken(claim);
      const result = claim.gmailCursor
        ? await this.#incrementalChanges(accessToken, claim.gmailCursor, limit)
        : await this.#initialChanges(accessToken, limit);
      return {
        connectionId: claim.connectionId,
        householdId: claim.householdId,
        ownerAdultId: claim.ownerAdultId,
        leaseOwner: claim.leaseOwner,
        cursor: claim.gmailCursor,
        changes: result.changes,
        nextCursor: result.nextCursor,
      };
    } catch (error) {
      await this.#store.releaseGmailSync({
        connectionId: claim.connectionId,
        owner: claim.leaseOwner,
        nextAt: new Date(now.getTime() + 5 * 60_000).toISOString(),
        error: error instanceof GoogleConnectionError ? error.code : "gmail_poll_failed",
      });
      throw error;
    }
  }

  releaseGmailSync(input: {
    connectionId: string;
    owner: string;
    nextAt: string;
    cursor?: string;
    error?: string | null;
  }): Promise<void> {
    instant(input.nextAt);
    return this.#store.releaseGmailSync(input);
  }

  async executeCalendar(effect: GoogleCalendarEffect): Promise<GoogleCalendarExecutionResult> {
    const failed = (detail: string): GoogleCalendarExecutionResult => ({
      status: "failed",
      providerReceiptId: null,
      detail: bounded(detail, 500),
      occurredAt: new Date().toISOString(),
    });
    try {
      validateCalendarEffect(effect);
    } catch {
      return failed("The approved Calendar effect is invalid");
    }

    const credential = await this.#store.readActiveGoogleCredential({
      connectionId: effect.connectionId,
      householdId: effect.householdId,
      ownerAdultId: effect.ownerAdultId,
    });
    if (!credential) return failed("The approved Google connection is no longer active");

    let accessToken: string;
    try {
      accessToken = await this.#calendarAccessToken(credential);
    } catch (error) {
      if (error instanceof DefinitiveCalendarError) return failed(error.message);
      throw error;
    }

    const eventId = calendarEventId(effect.actionId);
    const eventBody = {
      id: eventId,
      summary: effect.payload.title,
      start: { dateTime: effect.payload.startsAt, timeZone: effect.payload.timeZone },
      end: { dateTime: effect.payload.endsAt, timeZone: effect.payload.timeZone },
      ...(effect.payload.location === null ? {} : { location: effect.payload.location }),
      extendedProperties: {
        private: {
          florenceActionId: effect.actionId,
          florenceApprovalDigest: effect.approvalDigest,
          florenceCandidateDigest: effect.candidateDigest,
        },
      },
    };
    const collectionUrl = "https://www.googleapis.com/calendar/v3/calendars/primary/events?sendUpdates=none";
    let insertDisposition: "accepted" | "ambiguous" | "rejected" = "ambiguous";
    let rejectedStatus: number | null = null;
    try {
      const insert = await this.#fetch(collectionUrl, {
        method: "POST",
        headers: {
          authorization: `Bearer ${accessToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(eventBody),
        signal: AbortSignal.timeout(15_000),
      });
      if (insert.ok) insertDisposition = "accepted";
      else if (insert.status === 409 || transientHttpStatus(insert.status)) {
        insertDisposition = "ambiguous";
      } else {
        insertDisposition = "rejected";
        rejectedStatus = insert.status;
      }
    } catch {
      // A timed-out insert may have committed. The deterministic ID makes the mandatory read safe.
      insertDisposition = "ambiguous";
    }

    let reread: Response;
    try {
      reread = await this.#fetch(
        `https://www.googleapis.com/calendar/v3/calendars/primary/events/${encodeURIComponent(eventId)}`,
        {
          headers: { authorization: `Bearer ${accessToken}` },
          signal: AbortSignal.timeout(15_000),
        },
      );
    } catch (error) {
      throw transientCalendarError("Google Calendar proof read failed", error);
    }
    if (transientHttpStatus(reread.status)) {
      throw transientCalendarError(`Google Calendar proof read returned HTTP ${reread.status}`);
    }
    if (!reread.ok) {
      if (reread.status === 404 && insertDisposition !== "rejected") {
        throw transientCalendarError("The Google Calendar insert is not visible yet");
      }
      return failed(
        rejectedStatus === null
          ? `Google Calendar proof read was rejected with HTTP ${reread.status}`
          : `Google Calendar insert was rejected with HTTP ${rejectedStatus}`,
      );
    }

    let event: Record<string, unknown>;
    try {
      event = await safeJson(reread);
    } catch {
      return failed("Google Calendar returned an invalid proof event");
    }
    const proof = calendarEventProof(event, eventId, effect);
    if (!proof) return failed("Google Calendar did not preserve the exact approved event");
    const detail = JSON.stringify({
      provider: "google-calendar",
      eventId,
      etag: proof.etag,
      digest: proof.digest,
    });
    if (detail.length > 2_000) return failed("Google Calendar proof exceeded the safe receipt limit");
    return {
      status: "committed",
      providerReceiptId: eventId,
      detail,
      occurredAt: new Date().toISOString(),
    };
  }

  async #accessToken(credential: ActiveGoogleCredential): Promise<string> {
    const refreshToken = decrypt(
      credential.refreshTokenEnvelope,
      this.#key,
      aad(credential.connectionId, credential.householdId, credential.ownerAdultId),
    );
    const response = await this.#fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: this.#clientId,
        client_secret: this.#clientSecret,
        grant_type: "refresh_token",
        refresh_token: refreshToken,
      }),
    });
    if (!response.ok) throw providerError("Google access-token refresh failed");
    const token = await safeJson(response);
    if (token.token_type !== "Bearer") throw providerError("Google returned an unsupported token type");
    return stringField(token, "access_token");
  }

  async #calendarAccessToken(credential: ActiveGoogleCredential): Promise<string> {
    let refreshToken: string;
    try {
      refreshToken = decrypt(
        credential.refreshTokenEnvelope,
        this.#key,
        aad(credential.connectionId, credential.householdId, credential.ownerAdultId),
      );
    } catch {
      throw new DefinitiveCalendarError("The active Google credential could not be opened");
    }
    let response: Response;
    try {
      response = await this.#fetch("https://oauth2.googleapis.com/token", {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          client_id: this.#clientId,
          client_secret: this.#clientSecret,
          grant_type: "refresh_token",
          refresh_token: refreshToken,
        }),
        signal: AbortSignal.timeout(15_000),
      });
    } catch (error) {
      throw transientCalendarError("Google access-token refresh failed", error);
    }
    if (transientHttpStatus(response.status)) {
      throw transientCalendarError(`Google access-token refresh returned HTTP ${response.status}`);
    }
    if (!response.ok) {
      throw new DefinitiveCalendarError(
        `Google rejected the active Calendar credential with HTTP ${response.status}`,
      );
    }
    let token: Record<string, unknown>;
    try {
      token = await safeJson(response);
    } catch {
      throw new DefinitiveCalendarError("Google returned an invalid Calendar access token");
    }
    if (token.token_type !== "Bearer") {
      throw new DefinitiveCalendarError("Google returned an unsupported Calendar token type");
    }
    try {
      return stringField(token, "access_token");
    } catch {
      throw new DefinitiveCalendarError("Google returned an incomplete Calendar access token");
    }
  }

  async #initialChanges(accessToken: string, limit: number) {
    const profile = await this.#gmailJson("profile", accessToken);
    const nextCursor = stringField(profile, "historyId");
    const query = new URLSearchParams({ maxResults: String(limit), q: "newer_than:90d" });
    const list = await this.#gmailJson(`messages?${query}`, accessToken);
    const messages = recordArray(list.messages).slice(0, limit);
    return {
      changes: await Promise.all(messages.map((message) => this.#messageIdentity(accessToken, message))),
      nextCursor,
    };
  }

  async #incrementalChanges(accessToken: string, cursor: string, limit: number) {
    const query = new URLSearchParams({
      historyTypes: "messageAdded",
      maxResults: String(limit),
      startHistoryId: cursor,
    });
    const result = await this.#gmailJson(`history?${query}`, accessToken);
    const messages: Record<string, unknown>[] = [];
    let fullyConsumedCursor = cursor;
    let truncated = false;
    for (const history of recordArray(result.history)) {
      const historyId = stringField(history, "id");
      const added = recordArray(history.messagesAdded).map((entry) => recordField(entry, "message"));
      if (added.length > limit) throw providerError("One Gmail history record exceeds the safe batch limit");
      if (messages.length + added.length > limit) {
        truncated = true;
        break;
      }
      messages.push(...added);
      fullyConsumedCursor = historyId;
    }
    return {
      changes: await Promise.all(messages.map((message) => this.#messageIdentity(accessToken, message))),
      nextCursor:
        truncated || typeof result.nextPageToken === "string"
          ? fullyConsumedCursor
          : stringField(result, "historyId"),
    };
  }

  async #gmailJson(path: string, accessToken: string): Promise<Record<string, unknown>> {
    const response = await this.#fetch(`https://gmail.googleapis.com/gmail/v1/users/me/${path}`, {
      headers: { authorization: `Bearer ${accessToken}` },
    });
    if (!response.ok) throw providerError("Gmail synchronization failed");
    return safeJson(response);
  }

  async #messageIdentity(accessToken: string, listed: Record<string, unknown>): Promise<GmailMessageChange> {
    const messageId = stringField(listed, "id");
    const threadId = stringField(listed, "threadId");
    const message = await this.#gmailJson(
      `messages/${encodeURIComponent(messageId)}?format=metadata`,
      accessToken,
    );
    if (stringField(message, "id") !== messageId || stringField(message, "threadId") !== threadId) {
      throw providerError("Gmail message identity changed during synchronization");
    }
    return { messageId, threadId, historyId: stringField(message, "historyId") };
  }
}

class DefinitiveCalendarError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DefinitiveCalendarError";
  }
}

function transientCalendarError(message: string, cause?: unknown): Error {
  return cause === undefined ? new Error(message) : new Error(message, { cause });
}

function transientHttpStatus(status: number): boolean {
  return status === 408 || status === 425 || status === 429 || status >= 500;
}

function calendarEventId(actionId: string): string {
  return digest(`florence-google-calendar-event-v1\0${actionId}`);
}

function validateCalendarEffect(effect: GoogleCalendarEffect): void {
  if (effect.kind !== "google.calendar.create" || effect.candidateVersion !== 1) {
    throw new Error("Invalid Google Calendar effect kind or version");
  }
  for (const [value, label] of [
    [effect.id, "effect ID"],
    [effect.householdId, "household ID"],
    [effect.connectionId, "connection ID"],
    [effect.ownerAdultId, "owner adult ID"],
    [effect.actionId, "action ID"],
    [effect.candidateId, "candidate ID"],
    [effect.payload.title, "Calendar title"],
    [effect.payload.timeZone, "Calendar time zone"],
  ] as const) {
    required(value, label);
  }
  assertDigest(effect.approvalDigest, "Calendar approval");
  assertDigest(effect.candidateDigest, "Calendar candidate");
  const startsAt = instant(effect.payload.startsAt);
  const endsAt = instant(effect.payload.endsAt);
  if (endsAt <= startsAt) throw new Error("Calendar end must follow start");
  if (effect.payload.location !== null) required(effect.payload.location, "Calendar location");
}

function calendarEventProof(
  event: Record<string, unknown>,
  expectedId: string,
  effect: GoogleCalendarEffect,
): { etag: string; digest: string } | null {
  try {
    const start = recordField(event, "start");
    const end = recordField(event, "end");
    const extended = recordField(event, "extendedProperties");
    const privateProperties = recordField(extended, "private");
    const location = event.location === undefined ? null : event.location;
    const startsAt = stringField(start, "dateTime");
    const endsAt = stringField(end, "dateTime");
    const etag = stringField(event, "etag");
    if (
      stringField(event, "id") !== expectedId ||
      event.status !== "confirmed" ||
      event.summary !== effect.payload.title ||
      !sameInstant(startsAt, effect.payload.startsAt) ||
      !sameInstant(endsAt, effect.payload.endsAt) ||
      start.timeZone !== effect.payload.timeZone ||
      end.timeZone !== effect.payload.timeZone ||
      location !== effect.payload.location ||
      privateProperties.florenceActionId !== effect.actionId ||
      privateProperties.florenceApprovalDigest !== effect.approvalDigest ||
      privateProperties.florenceCandidateDigest !== effect.candidateDigest
    ) {
      return null;
    }
    const canonicalProof = JSON.stringify({
      provider: "google-calendar",
      eventId: expectedId,
      etag,
      status: "confirmed",
      title: effect.payload.title,
      startsAt: new Date(startsAt).toISOString(),
      endsAt: new Date(endsAt).toISOString(),
      timeZone: effect.payload.timeZone,
      location: effect.payload.location,
      actionId: effect.actionId,
      approvalDigest: effect.approvalDigest,
      candidateId: effect.candidateId,
      candidateVersion: effect.candidateVersion,
      candidateDigest: effect.candidateDigest,
    });
    return { etag, digest: digest(canonicalProof) };
  } catch {
    return null;
  }
}

function sameInstant(left: string, right: string): boolean {
  const leftTime = new Date(left).getTime();
  const rightTime = new Date(right).getTime();
  return Number.isFinite(leftTime) && leftTime === rightTime;
}

function exactScopes(value: string): readonly GoogleScope[] {
  const actual = [...new Set(value.split(/\s+/).filter(Boolean))].sort();
  const expected = [...GOOGLE_SCOPES].sort();
  if (actual.length !== expected.length || actual.some((scope, index) => scope !== expected[index])) {
    throw new GoogleConnectionError(
      "Google did not grant exactly the requested permissions",
      "invalid_grant",
    );
  }
  return [...GOOGLE_SCOPES];
}

function encrypt(plaintext: string, key: Buffer, associatedData: string): string {
  const nonce = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key, nonce);
  cipher.setAAD(Buffer.from(associatedData));
  const ciphertext = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
  return [
    "g1",
    nonce.toString("base64url"),
    ciphertext.toString("base64url"),
    cipher.getAuthTag().toString("base64url"),
  ].join(".");
}

function decrypt(envelope: string, key: Buffer, associatedData: string): string {
  const [version, nonce, ciphertext, tag, extra] = envelope.split(".");
  if (version !== "g1" || !nonce || !ciphertext || !tag || extra)
    throw new Error("Invalid Google credential envelope");
  const decipher = createDecipheriv("aes-256-gcm", key, Buffer.from(nonce, "base64url"));
  decipher.setAAD(Buffer.from(associatedData));
  decipher.setAuthTag(Buffer.from(tag, "base64url"));
  return Buffer.concat([decipher.update(Buffer.from(ciphertext, "base64url")), decipher.final()]).toString(
    "utf8",
  );
}

function aad(connectionId: string, householdId: string, ownerAdultId: string): string {
  return `florence-google-refresh-v1\0${connectionId}\0${householdId}\0${ownerAdultId}`;
}

function digest(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function instant(value: string): Date {
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) throw new Error("Invalid timestamp");
  return parsed;
}

function required(value: string, label: string): string {
  if (!value.trim()) throw new Error(`${label} is required`);
  return value;
}

function assertDigest(value: string, label: string): void {
  if (!/^[0-9a-f]{64}$/.test(value)) throw new Error(`${label} must be a SHA-256 digest`);
}

function providerError(message: string): GoogleConnectionError {
  return new GoogleConnectionError(message, "provider_rejected");
}

async function safeJson(response: Response): Promise<Record<string, unknown>> {
  const value: unknown = await response.json().catch(() => null);
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw providerError("Google returned an invalid response");
  return value as Record<string, unknown>;
}

function stringField(value: Record<string, unknown>, field: string): string {
  const result = value[field];
  if (typeof result !== "string" || !result) throw providerError("Google returned an incomplete response");
  return result;
}

function recordField(value: Record<string, unknown>, field: string): Record<string, unknown> {
  const result = value[field];
  if (!result || typeof result !== "object" || Array.isArray(result)) {
    throw providerError("Google returned an incomplete response");
  }
  return result as Record<string, unknown>;
}

function recordArray(value: unknown): readonly Record<string, unknown>[] {
  if (value === undefined) return [];
  if (
    !Array.isArray(value) ||
    value.some((item) => !item || typeof item !== "object" || Array.isArray(item))
  ) {
    throw providerError("Google returned an invalid list response");
  }
  return value as readonly Record<string, unknown>[];
}

function headerMap(value: unknown): Map<string, string> {
  const headers = new Map<string, string>();
  for (const header of recordArray(value)) {
    headers.set(stringField(header, "name").toLowerCase(), stringField(header, "value"));
  }
  return headers;
}

function collectPlainText(part: Record<string, unknown>): string {
  const own = part.mimeType === "text/plain" ? decodeBody(part.body) : "";
  const nested = recordArray(part.parts).map(collectPlainText).filter(Boolean);
  return [own, ...nested].filter(Boolean).join("\n");
}

function decodeBody(value: unknown): string {
  if (!value || typeof value !== "object" || Array.isArray(value)) return "";
  const data = (value as Record<string, unknown>).data;
  return typeof data === "string" ? Buffer.from(data, "base64url").toString("utf8") : "";
}

function bounded(value: string, maximum: number): string {
  return value.length <= maximum ? value : value.slice(0, maximum);
}

function nullableBounded(value: string | undefined, maximum: number): string | null {
  const trimmed = value?.trim();
  return trimmed ? bounded(trimmed, maximum) : null;
}

function isUniqueViolation(error: unknown): boolean {
  return Boolean(error && typeof error === "object" && "code" in error && error.code === "23505");
}
