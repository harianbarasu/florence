import { createCipheriv, createDecipheriv, createHash, randomBytes, randomUUID } from "node:crypto";

// Keep these aligned with @florence/artifacts. This package has no workspace dependencies.
const MAX_GMAIL_IMAGE_BYTES = 20 * 1024 * 1024;
const MAX_GMAIL_PDF_BYTES = 20 * 1024 * 1024;
const MAX_GMAIL_ATTACHMENTS_PER_MESSAGE = 20;
const CALENDAR_CURSOR_OVERLAP_MS = 5 * 60_000;
const GOOGLE_CHANGE_PAGE_SIZE = 100;
const MAX_GOOGLE_CHANGE_PAGES = 20;
const MAX_GMAIL_CHANGED_MESSAGES = 500;
const CALENDAR_CHANGE_HORIZON_MS = 21 * 24 * 60 * 60_000;

export const GOOGLE_SCOPES = [
  "openid",
  "email",
  "https://www.googleapis.com/auth/gmail.readonly",
  "https://www.googleapis.com/auth/calendar.events.owned",
  "https://www.googleapis.com/auth/calendar.app.created",
  "https://www.googleapis.com/auth/calendar.acls",
  "https://www.googleapis.com/auth/calendar.calendarlist",
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

export type StoredGoogleConnection = GoogleConnectionView & {
  stateDigest: string;
  stateExpiresAt: string;
  stateConsumedAt: string | null;
  googleSubjectDigest: string | null;
  refreshTokenEnvelope: string | null;
};

export type ActiveGoogleCredential = {
  connectionId: string;
  householdId: string;
  ownerAdultId: string;
  refreshTokenEnvelope: string;
};

export type CalendarEventDraft =
  | {
      intervalKind: "timed";
      title: string;
      startsAt: string;
      endsAt: string;
      timeZone: string;
      location: string | null;
    }
  | {
      intervalKind: "all_day";
      title: string;
      startDate: string;
      endDate: string;
      location: string | null;
    };

export type CalendarEventTarget = {
  providerEventId: string;
  providerRevision: string;
  observedEvent: CalendarEventDraft;
};

export type FamilyCalendarMutation =
  | { operation: "create"; event: CalendarEventDraft; target: null }
  | { operation: "update"; event: CalendarEventDraft; target: CalendarEventTarget }
  | { operation: "delete"; event: null; target: CalendarEventTarget };

export type ApprovedCalendarAction = {
  actionId: string;
  householdId: string;
  connectionId: string;
  ownerAdultId: string;
  calendarId: string;
  mutation: FamilyCalendarMutation;
};

export type GoogleCalendarExecutionResult =
  | {
      status: "committed";
      providerEventId: string;
      providerRevision: string | null;
      occurredAt: string;
    }
  | { status: "failed"; detail: string; occurredAt: string }
  | { status: "credential_rejected"; detail: string; occurredAt: string };

type GoogleCalendarWindowEventFields = {
  providerEventId: string;
  providerRevision: string;
  providerUpdatedAt: string;
  status: "confirmed" | "tentative";
  busy: boolean;
  title: string | null;
  location: string | null;
};

export type GoogleCalendarWindowEvent = GoogleCalendarWindowEventFields &
  (
    | {
        intervalKind: "timed";
        startsAt: string;
        endsAt: string;
        timeZone: string;
      }
    | {
        intervalKind: "all_day";
        startDate: string;
        endDate: string;
      }
  );

/**
 * Rolling Calendar polling cursor. The five-minute overlap intentionally favors duplicate reads
 * over gaps caused by provider/local clock skew. De-duplicate with providerEventId + providerRevision.
 * Google sync tokens inherit an initial time filter and cannot later change timeMin/timeMax, so a
 * token created from the fixed briefing window cannot follow a rolling 21-day window. The outer
 * read status still reports when the initial window itself was truncated.
 */
export type GoogleCalendarBoundedCursor = {
  kind: "calendar_updated_min_v1";
  calendarId: string;
  updatedMin: string;
  windowTimeMin: string;
  windowTimeMax: string;
  overlapMs: typeof CALENDAR_CURSOR_OVERLAP_MS;
};

export type GoogleCalendarWindowRead = {
  status: "complete" | "truncated" | "unavailable";
  events: readonly GoogleCalendarWindowEvent[];
  cursor: GoogleCalendarBoundedCursor | null;
};

export type GoogleCalendarChange = {
  providerEventId: string;
  providerRevision: string;
  providerUpdatedAt: string;
  status: "confirmed" | "tentative" | "cancelled";
  busy: boolean;
  title: string | null;
  startsAt: string | null;
  endsAt: string | null;
  allDay: boolean | null;
  timeZone: string | null;
  startDate: string | null;
  endDate: string | null;
};

export type GoogleCalendarChangesRead =
  | {
      status: "complete";
      resyncRequired: false;
      events: readonly GoogleCalendarChange[];
      cursor: GoogleCalendarBoundedCursor;
    }
  | {
      status: "resync_required";
      resyncRequired: true;
      reason: "cursor_expired" | "change_volume_exceeded";
      events: readonly GoogleCalendarChange[];
      cursor: null;
    }
  | {
      status: "unavailable";
      resyncRequired: false;
      reason: "calendar_unavailable";
      events: readonly GoogleCalendarChange[];
      cursor: GoogleCalendarBoundedCursor;
    };

export type GoogleFamilyCalendarProvisioningResult = {
  calendarId: string;
  summary: string;
  founderConnectionId: string;
  partnerConnectionId: string;
  occurredAt: string;
};

export type GoogleFamilyCalendarRenameResult = {
  summary: string;
  occurredAt: string;
};

export type GoogleSupportedGmailAttachmentMimeType =
  | "application/pdf"
  | "image/jpeg"
  | "image/png"
  | "image/webp";

export type GmailAttachmentReference = {
  messageId: string;
  threadId: string;
  historyId: string;
  partId: string;
  attachmentId: string;
  storage: "external" | "inline";
  filename: string;
  mimeType: GoogleSupportedGmailAttachmentMimeType;
  sizeBytes: number;
};

export type GmailAttachmentRead = GmailAttachmentReference & { bytes: Uint8Array };

export type GmailEvidence = {
  messageId: string;
  threadId: string;
  historyId: string;
  from: string;
  subject: string | null;
  sentAt: string;
  text: string;
  textStatus: "complete" | "truncated" | "unavailable";
  attachments: readonly GmailAttachmentReference[];
  attachmentsStatus: "complete" | "truncated";
};

export type GmailSearchResult = {
  status: "complete" | "truncated";
  messages: readonly GmailEvidence[];
};

/** Capture before the baseline Gmail reads, then persist only after that review commits. */
export type GoogleGmailCursor = {
  kind: "gmail_history_v1";
  historyId: string;
  capturedAt: string;
};

export type GoogleGmailChangesRead =
  | {
      status: "complete";
      resyncRequired: false;
      messages: readonly GmailEvidence[];
      cursor: GoogleGmailCursor;
    }
  | {
      status: "bounded_resync_required";
      resyncRequired: true;
      reason: "cursor_expired" | "change_volume_exceeded";
      messages: readonly GmailEvidence[];
      cursor: null;
    };

type FamilyCalendarListEntry = {
  id: string;
  summary: string;
  timeZone: string;
  accessRole: string;
  selected: boolean;
  primary: boolean;
};

type CalendarMutationEventRead =
  | { status: "found"; event: Record<string, unknown> }
  | { status: "missing" }
  | { status: "credential_rejected"; detail: string }
  | { status: "rejected"; detail: string };

/** Credential plaintext must never cross this interface. */
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
    notifyReconnect?: boolean;
    now: string;
  }): Promise<{ view: GoogleConnectionView; refreshTokenEnvelope: string | null } | null>;
  readActiveGoogleCredential(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
  }): Promise<ActiveGoogleCredential | null>;
  /** Records the one allowed Family Calendar create before Google receives the POST. */
  beginFamilyCalendarCreation(input: {
    householdId: string;
    now: string;
  }): Promise<{ createAllowed: boolean; calendarId: string | null }>;
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
      | "credential_invalid_grant"
      | "identity_conflict"
      | "not_found",
  ) {
    super(message);
    this.name = "GoogleConnectionError";
  }
}

export class GoogleCalendarTransientError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "GoogleCalendarTransientError";
  }
}

/** The calendar exists; retry provisioning with this exact ID rather than creating another one. */
export class GoogleFamilyCalendarTransientError extends GoogleCalendarTransientError {
  constructor(
    message: string,
    readonly calendarId: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "GoogleFamilyCalendarTransientError";
  }
}

export class GoogleFamilyCalendarProvisioningError extends Error {
  constructor(
    message: string,
    readonly code: "provider_rejected" | "manual_repair_required",
    readonly calendarId: string | null = null,
  ) {
    super(message);
    this.name = "GoogleFamilyCalendarProvisioningError";
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
    notifyReconnect?: boolean;
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
    return this.#readGmailMessage(accessToken, input);
  }

  async captureGmailCursor(input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
  }): Promise<GoogleGmailCursor> {
    const credential = await this.#store.readActiveGoogleCredential(input);
    if (!credential) throw new GoogleConnectionError("Active Google connection was not found", "not_found");
    const accessToken = await this.#accessToken(credential);
    const profile = await this.#gmailJson("profile", accessToken);
    const historyId = gmailHistoryId(profile.historyId);
    stringField(profile, "emailAddress");
    nonNegativeIntegerField(profile, "messagesTotal");
    nonNegativeIntegerField(profile, "threadsTotal");
    return { kind: "gmail_history_v1", historyId, capturedAt: new Date().toISOString() };
  }

  async readGmailChanges(input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    cursor: GoogleGmailCursor;
  }): Promise<GoogleGmailChangesRead> {
    const cursor = gmailCursor(input.cursor);
    const credential = await this.#store.readActiveGoogleCredential({
      householdId: input.householdId,
      ownerAdultId: input.ownerAdultId,
      connectionId: input.connectionId,
    });
    if (!credential) throw new GoogleConnectionError("Active Google connection was not found", "not_found");
    const accessToken = await this.#accessToken(credential);
    const identities = new Map<string, { messageId: string; threadId: string }>();
    const seenPageTokens = new Set<string>();
    let pageToken: string | null = null;
    let nextHistoryId = cursor.historyId;

    for (let page = 0; page < MAX_GOOGLE_CHANGE_PAGES; page += 1) {
      const query = new URLSearchParams({
        historyTypes: "messageAdded",
        labelId: "INBOX",
        maxResults: String(GOOGLE_CHANGE_PAGE_SIZE),
        startHistoryId: cursor.historyId,
      });
      if (pageToken) query.set("pageToken", pageToken);
      const response = await this.#fetch(`https://gmail.googleapis.com/gmail/v1/users/me/history?${query}`, {
        headers: { authorization: `Bearer ${accessToken}` },
        signal: AbortSignal.timeout(15_000),
      });
      if (response.status === 404) {
        return {
          status: "bounded_resync_required",
          resyncRequired: true,
          reason: "cursor_expired",
          messages: [],
          cursor: null,
        };
      }
      if (!response.ok) throw providerError("Gmail history read failed");
      const body = await safeJson(response);
      const pageHistoryId = gmailHistoryId(body.historyId);
      if (BigInt(pageHistoryId) < BigInt(cursor.historyId)) {
        throw providerError("Gmail returned a history cursor older than the requested cursor");
      }
      nextHistoryId = pageHistoryId;
      for (const history of recordArray(body.history)) {
        gmailHistoryId(history.id);
        for (const addition of recordArray(history.messagesAdded)) {
          const identity = gmailHistoryMessageIdentity(addition);
          const existing = identities.get(identity.messageId);
          if (existing && existing.threadId !== identity.threadId) {
            throw providerError("Gmail reused a changed message ID in another thread");
          }
          identities.set(identity.messageId, identity);
          if (identities.size > MAX_GMAIL_CHANGED_MESSAGES) {
            return {
              status: "bounded_resync_required",
              resyncRequired: true,
              reason: "change_volume_exceeded",
              messages: [],
              cursor: null,
            };
          }
        }
      }
      const nextPageToken = optionalStringField(body, "nextPageToken");
      if (!nextPageToken) {
        const messages: GmailEvidence[] = [];
        for (const identity of identities.values()) {
          const message = await this.#readCurrentGmailMessage(accessToken, identity);
          if (message) messages.push(message);
        }
        messages.sort(
          (left, right) =>
            left.sentAt.localeCompare(right.sentAt) || left.messageId.localeCompare(right.messageId),
        );
        return {
          status: "complete",
          resyncRequired: false,
          messages,
          cursor: {
            kind: "gmail_history_v1",
            historyId: nextHistoryId,
            capturedAt: new Date().toISOString(),
          },
        };
      }
      if (seenPageTokens.has(nextPageToken)) {
        throw providerError("Gmail repeated a history page token");
      }
      seenPageTokens.add(nextPageToken);
      pageToken = nextPageToken;
    }

    return {
      status: "bounded_resync_required",
      resyncRequired: true,
      reason: "change_volume_exceeded",
      messages: [],
      cursor: null,
    };
  }

  async readGmailAttachment(input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    attachment: GmailAttachmentReference;
  }): Promise<GmailAttachmentRead> {
    const expected = validateGmailAttachmentReference(input.attachment);
    const credential = await this.#store.readActiveGoogleCredential({
      householdId: input.householdId,
      ownerAdultId: input.ownerAdultId,
      connectionId: input.connectionId,
    });
    if (!credential) throw new GoogleConnectionError("Active Google connection was not found", "not_found");
    const accessToken = await this.#accessToken(credential);
    const message = await this.#readGmailMessage(accessToken, expected);
    if (!message.attachments.some((attachment) => sameGmailAttachment(attachment, expected))) {
      throw providerError("Gmail attachment identity changed before it could be read");
    }
    const body =
      expected.storage === "external"
        ? await this.#gmailJson(
            `messages/${encodeURIComponent(expected.messageId)}/attachments/${encodeURIComponent(expected.attachmentId)}`,
            accessToken,
          )
        : await this.#readInlineGmailAttachmentBody(accessToken, expected);
    const sizeBytes = nonNegativeIntegerField(body, "size");
    if (sizeBytes !== expected.sizeBytes) {
      throw providerError("Gmail attachment size changed before it could be read");
    }
    const encoded = stringField(body, "data");
    const bytes = strictBase64UrlDecode(encoded, gmailAttachmentLimit(expected.mimeType));
    if (bytes.byteLength !== expected.sizeBytes || !gmailBytesMatchMimeType(bytes, expected.mimeType)) {
      throw providerError("Gmail attachment content did not match its declared type and size");
    }
    return { ...expected, bytes };
  }

  async searchGmail(input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    query: string;
    after?: string;
    before?: string;
    limit?: number;
  }): Promise<GmailSearchResult> {
    const queryText = required(input.query, "Gmail search query").trim();
    if (queryText.length > 500) throw new Error("Gmail search query exceeds 500 characters");
    const bounds = gmailSearchBounds(input.after, input.before);
    const limit = input.limit ?? 10;
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 20) {
      throw new Error("Gmail search limit must be between 1 and 20");
    }
    const credential = await this.#store.readActiveGoogleCredential({
      householdId: input.householdId,
      ownerAdultId: input.ownerAdultId,
      connectionId: input.connectionId,
    });
    if (!credential) throw new GoogleConnectionError("Active Google connection was not found", "not_found");
    const accessToken = await this.#accessToken(credential);
    const boundedQuery = bounds
      ? `(${queryText}) after:${Math.floor(bounds.after.getTime() / 1_000)} before:${Math.ceil(bounds.before.getTime() / 1_000)}`
      : queryText;
    const query = new URLSearchParams({ maxResults: String(limit), q: boundedQuery });
    const list = await this.#gmailJson(`messages?${query}`, accessToken);
    const nextPageToken = optionalStringField(list, "nextPageToken");
    const identities = await Promise.all(
      recordArray(list.messages)
        .slice(0, limit)
        .map((message) => this.#messageIdentity(accessToken, message)),
    );
    return {
      status: nextPageToken === null ? "complete" : "truncated",
      messages: await Promise.all(
        identities.map((identity) => this.#readGmailMessage(accessToken, identity)),
      ),
    };
  }

  async provisionFamilyCalendar(input: {
    householdId: string;
    founderAdultId: string;
    founderConnectionId: string;
    partnerAdultId: string;
    partnerConnectionId: string;
    summary: string;
    timeZone: string;
    /** Supply the returned ID when resuming after a transient post-creation failure. */
    calendarId?: string;
  }): Promise<GoogleFamilyCalendarProvisioningResult> {
    const householdId = required(input.householdId, "household ID");
    const founderAdultId = required(input.founderAdultId, "founder adult ID");
    const founderConnectionId = required(input.founderConnectionId, "founder Google connection ID");
    const partnerAdultId = required(input.partnerAdultId, "partner adult ID");
    const partnerConnectionId = required(input.partnerConnectionId, "partner Google connection ID");
    if (founderAdultId === partnerAdultId) {
      throw new Error("Family Calendar adults must be different");
    }
    if (founderConnectionId === partnerConnectionId) {
      throw new Error("Family Calendar Google connections must be different");
    }
    const summary = required(input.summary, "Family Calendar title").trim();
    if (summary.length > 500) throw new Error("Family Calendar title exceeds 500 characters");
    const timeZone = calendarTimeZone(input.timeZone);
    const resumedCalendarId =
      input.calendarId === undefined ? null : secondaryCalendarTarget(input.calendarId);
    const provisioningMarker = familyCalendarProvisioningMarker(householdId);
    const description = familyCalendarDescription(provisioningMarker);

    const [founderCredential, partnerCredential, founderConnections, partnerConnections] = await Promise.all([
      this.#store.readActiveGoogleCredential({
        connectionId: founderConnectionId,
        householdId,
        ownerAdultId: founderAdultId,
      }),
      this.#store.readActiveGoogleCredential({
        connectionId: partnerConnectionId,
        householdId,
        ownerAdultId: partnerAdultId,
      }),
      this.#store.listActive({ householdId, ownerAdultId: founderAdultId }),
      this.#store.listActive({ householdId, ownerAdultId: partnerAdultId }),
    ]);
    const founderConnection = exactActiveConnection(
      founderConnections,
      householdId,
      founderAdultId,
      founderConnectionId,
    );
    const partnerConnection = exactActiveConnection(
      partnerConnections,
      householdId,
      partnerAdultId,
      partnerConnectionId,
    );
    if (!founderCredential || !founderConnection) {
      throw new GoogleConnectionError("The founder Google connection is no longer active", "not_found");
    }
    if (!partnerCredential || !partnerConnection) {
      throw new GoogleConnectionError("The partner Google connection is no longer active", "not_found");
    }
    const founderEmail = required(founderConnection.emailLabel ?? "", "founder Google email");
    const partnerEmail = required(partnerConnection.emailLabel ?? "", "partner Google email");

    let founderAccessToken: string;
    let partnerAccessToken: string;
    try {
      [founderAccessToken, partnerAccessToken] = await Promise.all([
        this.#calendarAccessToken(founderCredential),
        this.#calendarAccessToken(partnerCredential),
      ]);
    } catch (error) {
      if (error instanceof DefinitiveCalendarError) {
        throw new GoogleFamilyCalendarProvisioningError(
          error.message,
          "provider_rejected",
          resumedCalendarId,
        );
      }
      if (resumedCalendarId && error instanceof GoogleCalendarTransientError) {
        throw new GoogleFamilyCalendarTransientError(error.message, resumedCalendarId, {
          cause: error,
        });
      }
      throw error;
    }

    let calendarId =
      resumedCalendarId ?? (await this.#findFamilyCalendarByMarker(founderAccessToken, description));
    if (calendarId === null) {
      const createState = await this.#store.beginFamilyCalendarCreation({
        householdId,
        now: new Date().toISOString(),
      });
      calendarId = createState.calendarId === null ? null : secondaryCalendarTarget(createState.calendarId);
      if (calendarId === null && createState.createAllowed) {
        calendarId = await this.#createFamilyCalendar(founderAccessToken, {
          summary,
          description,
          timeZone,
        });
        if (calendarId === null) {
          calendarId = await this.#findFamilyCalendarByMarker(founderAccessToken, description);
        }
      }
    }
    if (calendarId === null) {
      throw new GoogleFamilyCalendarProvisioningError(
        "Family Calendar creation could not be confirmed; inspect the marked Google calendars before trying again",
        "manual_repair_required",
      );
    }
    try {
      await this.#readFamilyCalendar(founderAccessToken, calendarId, summary, description, timeZone);
      if (!(await this.#readOwnerAcl(founderAccessToken, calendarId, founderEmail))) {
        throw new GoogleFamilyCalendarProvisioningError(
          "Google did not verify the founder as a Family Calendar owner",
          "provider_rejected",
        );
      }
      await this.#ensureOwnerAcl(founderAccessToken, calendarId, partnerEmail);
      await this.#ensurePartnerCalendarList(partnerAccessToken, calendarId, summary, timeZone);
      return {
        calendarId,
        summary,
        founderConnectionId,
        partnerConnectionId,
        occurredAt: new Date().toISOString(),
      };
    } catch (error) {
      if (error instanceof GoogleCalendarTransientError) {
        throw new GoogleFamilyCalendarTransientError(error.message, calendarId, { cause: error });
      }
      if (error instanceof GoogleFamilyCalendarProvisioningError && error.calendarId === null) {
        throw new GoogleFamilyCalendarProvisioningError(error.message, error.code, calendarId);
      }
      throw error;
    }
  }

  async renameFamilyCalendar(input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    calendarId: string;
    summary: string;
  }): Promise<GoogleFamilyCalendarRenameResult> {
    const householdId = required(input.householdId, "household ID");
    const ownerAdultId = required(input.ownerAdultId, "Family Calendar owner adult ID");
    const connectionId = required(input.connectionId, "Family Calendar Google connection ID");
    const calendarId = secondaryCalendarTarget(input.calendarId);
    const summary = required(input.summary, "Family Calendar title").trim();
    if (summary.length > 160) throw new Error("Family Calendar title exceeds 160 characters");
    const credential = await this.#store.readActiveGoogleCredential({
      connectionId,
      householdId,
      ownerAdultId,
    });
    if (!credential) {
      throw new GoogleConnectionError(
        "The Family Calendar Google connection is no longer active",
        "not_found",
      );
    }
    let accessToken: string;
    try {
      accessToken = await this.#calendarAccessToken(credential);
    } catch (error) {
      if (error instanceof DefinitiveCalendarError) {
        throw new GoogleFamilyCalendarProvisioningError(error.message, "provider_rejected", calendarId);
      }
      throw error;
    }
    const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}`;
    let response: Response;
    try {
      response = await this.#fetch(url, {
        method: "PATCH",
        headers: {
          authorization: `Bearer ${accessToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ summary }),
        signal: AbortSignal.timeout(15_000),
      });
    } catch (error) {
      throw transientCalendarError("Google Family Calendar rename failed", error);
    }
    if (transientHttpStatus(response.status)) {
      throw transientCalendarError(`Google Family Calendar rename returned HTTP ${response.status}`);
    }
    if (!response.ok) {
      throw new GoogleFamilyCalendarProvisioningError(
        `Google rejected the Family Calendar rename with HTTP ${response.status}`,
        "provider_rejected",
        calendarId,
      );
    }

    let read: Response;
    try {
      read = await this.#fetch(url, {
        headers: { authorization: `Bearer ${accessToken}` },
        signal: AbortSignal.timeout(15_000),
      });
    } catch (error) {
      throw transientCalendarError("Google Family Calendar rename verification failed", error);
    }
    if (transientHttpStatus(read.status)) {
      throw transientCalendarError(`Google Family Calendar rename verification returned HTTP ${read.status}`);
    }
    if (!read.ok) {
      throw new GoogleFamilyCalendarProvisioningError(
        `Google rejected the Family Calendar rename verification with HTTP ${read.status}`,
        "provider_rejected",
        calendarId,
      );
    }
    try {
      const calendar = await safeJson(read);
      if (stringField(calendar, "id") !== calendarId || stringField(calendar, "summary") !== summary) {
        throw new Error("different calendar");
      }
    } catch {
      throw new GoogleFamilyCalendarProvisioningError(
        "Google did not preserve the new Family Calendar name",
        "provider_rejected",
        calendarId,
      );
    }
    return { summary, occurredAt: new Date().toISOString() };
  }

  async readInitialCalendarReview(input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    calendarId?: string;
    currentTime: string;
    limit?: number;
  }): Promise<GoogleCalendarWindowRead> {
    const timeMin = explicitInstant(input.currentTime);
    return this.readCalendarWindow({
      householdId: input.householdId,
      ownerAdultId: input.ownerAdultId,
      connectionId: input.connectionId,
      ...(input.calendarId === undefined ? {} : { calendarId: input.calendarId }),
      timeMin: timeMin.toISOString(),
      timeMax: new Date(timeMin.getTime() + 21 * 24 * 60 * 60_000).toISOString(),
      ...(input.limit === undefined ? {} : { limit: input.limit }),
    });
  }

  async readCalendarWindow(input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    calendarId?: string;
    timeMin: string;
    timeMax: string;
    limit?: number;
    eventSelection?: "busy" | "all";
  }): Promise<GoogleCalendarWindowRead> {
    const calendarId = calendarTarget(input.calendarId);
    const timeMin = explicitInstant(input.timeMin);
    const timeMax = explicitInstant(input.timeMax);
    if (timeMax <= timeMin) throw new Error("Calendar read end must follow start");
    if (timeMax.getTime() - timeMin.getTime() > 31 * 24 * 60 * 60_000) {
      throw new Error("Calendar read window cannot exceed 31 days");
    }
    const limit = input.limit ?? 50;
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 50) {
      throw new Error("Calendar read limit must be between 1 and 50");
    }
    const eventSelection = input.eventSelection ?? "busy";
    if (eventSelection !== "busy" && eventSelection !== "all") {
      throw new Error("Calendar event selection is invalid");
    }
    const credential = await this.#store.readActiveGoogleCredential({
      householdId: input.householdId,
      ownerAdultId: input.ownerAdultId,
      connectionId: input.connectionId,
    });
    if (!credential) return { status: "unavailable", events: [], cursor: null };

    try {
      const accessToken = await this.#calendarAccessToken(credential);
      const cursorCapturedAt = new Date();
      const query = new URLSearchParams({
        fields:
          "nextPageToken,timeZone,items(id,etag,updated,status,summary,location,start,end,transparency,attendees(self,responseStatus))",
        maxResults: String(limit),
        orderBy: "startTime",
        showDeleted: "false",
        singleEvents: "true",
        timeMax: timeMax.toISOString(),
        timeMin: timeMin.toISOString(),
      });
      let response: Response;
      try {
        response = await this.#fetch(
          `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events?${query}`,
          {
            headers: { authorization: `Bearer ${accessToken}` },
            signal: AbortSignal.timeout(15_000),
          },
        );
      } catch (error) {
        throw transientCalendarError("Google Calendar read failed", error);
      }
      if (transientHttpStatus(response.status)) {
        throw transientCalendarError(`Google Calendar read returned HTTP ${response.status}`);
      }
      if (!response.ok) return { status: "unavailable", events: [], cursor: null };
      const body = await safeJson(response);
      const calendarTimeZone = stringField(body, "timeZone");
      const nextPageToken = body.nextPageToken;
      if (nextPageToken !== undefined && (typeof nextPageToken !== "string" || !nextPageToken)) {
        return { status: "unavailable", events: [], cursor: null };
      }
      const events = recordArray(body.items).flatMap((event) => {
        const calendarEvent = calendarWindowEvent(event, calendarTimeZone, timeMin, timeMax, eventSelection);
        return calendarEvent ? [calendarEvent] : [];
      });
      return {
        status: nextPageToken ? "truncated" : "complete",
        events,
        cursor: {
          kind: "calendar_updated_min_v1",
          calendarId,
          updatedMin: new Date(cursorCapturedAt.getTime() - CALENDAR_CURSOR_OVERLAP_MS).toISOString(),
          windowTimeMin: timeMin.toISOString(),
          windowTimeMax: timeMax.toISOString(),
          overlapMs: CALENDAR_CURSOR_OVERLAP_MS,
        },
      };
    } catch (error) {
      if (
        error instanceof GoogleCalendarTransientError ||
        (error instanceof GoogleConnectionError && error.code === "credential_invalid_grant")
      ) {
        throw error;
      }
      return { status: "unavailable", events: [], cursor: null };
    }
  }

  async readCalendarChanges(input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    calendarId: string;
    cursor: GoogleCalendarBoundedCursor;
    currentTime: string;
  }): Promise<GoogleCalendarChangesRead> {
    const calendarId = calendarTarget(input.calendarId);
    const cursor = calendarBoundedCursor(input.cursor, calendarId);
    const timeMin = explicitInstant(input.currentTime);
    const timeMax = new Date(timeMin.getTime() + CALENDAR_CHANGE_HORIZON_MS);
    const unavailable = (): GoogleCalendarChangesRead => ({
      status: "unavailable",
      resyncRequired: false,
      reason: "calendar_unavailable",
      events: [],
      cursor,
    });
    const credential = await this.#store.readActiveGoogleCredential({
      householdId: input.householdId,
      ownerAdultId: input.ownerAdultId,
      connectionId: input.connectionId,
    });
    if (!credential) return unavailable();

    try {
      const accessToken = await this.#calendarAccessToken(credential);
      const events = new Map<string, GoogleCalendarChange>();
      let observedCalendarTimeZone: string | null = null;
      let pagesRead = 0;
      const enteringTimeMin = new Date(
        Math.max(timeMin.getTime(), explicitInstant(cursor.windowTimeMax).getTime()),
      );
      const scans = [
        {
          timeMin,
          timeMax,
          updatedMin: cursor.updatedMin,
          showDeleted: true,
        },
        ...(enteringTimeMin < timeMax
          ? [
              {
                timeMin: enteringTimeMin,
                timeMax,
                updatedMin: null,
                showDeleted: false,
              },
            ]
          : []),
      ] as const;

      for (const scan of scans) {
        const seenPageTokens = new Set<string>();
        let pageToken: string | null = null;
        while (true) {
          if (pagesRead >= MAX_GOOGLE_CHANGE_PAGES) {
            return {
              status: "resync_required",
              resyncRequired: true,
              reason: "change_volume_exceeded",
              events: [],
              cursor: null,
            };
          }
          pagesRead += 1;
          const query = new URLSearchParams({
            fields:
              "nextPageToken,timeZone,items(id,etag,updated,status,summary,location,start,end,transparency,attendees(self,responseStatus))",
            maxResults: String(GOOGLE_CHANGE_PAGE_SIZE),
            showDeleted: String(scan.showDeleted),
            singleEvents: "true",
            timeMax: scan.timeMax.toISOString(),
            timeMin: scan.timeMin.toISOString(),
          });
          if (scan.updatedMin) query.set("updatedMin", scan.updatedMin);
          if (pageToken) query.set("pageToken", pageToken);
          let response: Response;
          try {
            response = await this.#fetch(
              `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events?${query}`,
              {
                headers: { authorization: `Bearer ${accessToken}` },
                signal: AbortSignal.timeout(15_000),
              },
            );
          } catch (error) {
            throw transientCalendarError("Google Calendar changes read failed", error);
          }
          if (response.status === 410) {
            return {
              status: "resync_required",
              resyncRequired: true,
              reason: "cursor_expired",
              events: [],
              cursor: null,
            };
          }
          if (transientHttpStatus(response.status)) {
            throw transientCalendarError(`Google Calendar changes read returned HTTP ${response.status}`);
          }
          if (!response.ok) return unavailable();
          const body = await safeJson(response);
          const pageTimeZone = calendarTimeZone(stringField(body, "timeZone"));
          if (observedCalendarTimeZone !== null && observedCalendarTimeZone !== pageTimeZone) {
            return unavailable();
          }
          observedCalendarTimeZone = pageTimeZone;
          for (const event of recordArray(body.items)) {
            const change = calendarChangedEvent(event, pageTimeZone);
            const key = `${change.providerEventId}\0${change.providerRevision}`;
            events.set(key, change);
          }
          const nextPageToken = optionalStringField(body, "nextPageToken");
          if (!nextPageToken) break;
          if (seenPageTokens.has(nextPageToken)) return unavailable();
          seenPageTokens.add(nextPageToken);
          pageToken = nextPageToken;
        }
      }

      const changes = [...events.values()].sort(
        (left, right) =>
          left.providerUpdatedAt.localeCompare(right.providerUpdatedAt) ||
          left.providerEventId.localeCompare(right.providerEventId) ||
          left.providerRevision.localeCompare(right.providerRevision),
      );
      return {
        status: "complete",
        resyncRequired: false,
        events: changes,
        cursor: {
          kind: "calendar_updated_min_v1",
          calendarId,
          updatedMin: new Date(timeMin.getTime() - CALENDAR_CURSOR_OVERLAP_MS).toISOString(),
          windowTimeMin: timeMin.toISOString(),
          windowTimeMax: timeMax.toISOString(),
          overlapMs: CALENDAR_CURSOR_OVERLAP_MS,
        },
      };
    } catch (error) {
      if (
        error instanceof GoogleCalendarTransientError ||
        (error instanceof GoogleConnectionError && error.code === "credential_invalid_grant")
      ) {
        throw error;
      }
      return unavailable();
    }
  }

  async executeCalendar(action: ApprovedCalendarAction): Promise<GoogleCalendarExecutionResult> {
    const rejected = (
      status: "failed" | "credential_rejected",
      detail: string,
    ): GoogleCalendarExecutionResult => ({
      status,
      detail: bounded(detail, 500),
      occurredAt: new Date().toISOString(),
    });
    const failed = (detail: string) => rejected("failed", detail);
    const credentialRejected = (detail: string) => rejected("credential_rejected", detail);
    try {
      validateCalendarAction(action);
    } catch {
      return failed("The approved Calendar action is invalid");
    }

    const credential = await this.#store.readActiveGoogleCredential({
      connectionId: action.connectionId,
      householdId: action.householdId,
      ownerAdultId: action.ownerAdultId,
    });
    if (!credential) {
      return credentialRejected("The approved Google connection is no longer active");
    }

    let accessToken: string;
    try {
      accessToken = await this.#calendarAccessToken(credential);
    } catch (error) {
      if (error instanceof DefinitiveCalendarError) return credentialRejected(error.message);
      throw error;
    }

    const calendarId = secondaryCalendarTarget(action.calendarId);
    const eventId =
      action.mutation.operation === "create"
        ? googleCalendarEventId(action.actionId)
        : action.mutation.target.providerEventId;
    const calendarUrl = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}`;
    const eventUrl = `${calendarUrl}/events/${encodeURIComponent(eventId)}`;

    let preRevision: string | null = null;
    if (action.mutation.operation !== "create") {
      const preRead = await this.#readCalendarMutationEvent(accessToken, calendarId, eventId);
      if (preRead.status === "credential_rejected") return credentialRejected(preRead.detail);
      if (preRead.status === "rejected") return failed(preRead.detail);
      if (preRead.status === "missing") {
        if (action.mutation.operation === "update") {
          return failed("The Family Calendar event no longer exists");
        }
        // A retry may arrive after Google committed the prior conditional DELETE. Reissuing the
        // same If-Match delete is safe, and the mandatory read below confirms it remains absent.
        preRevision = action.mutation.target.providerRevision;
      } else {
        const desiredEvent =
          action.mutation.operation === "update"
            ? confirmedCalendarEvent(preRead.event, eventId, action.mutation.event)
            : null;
        if (desiredEvent) {
          return committedCalendarResult(eventId, desiredEvent.revision);
        }
        const observed = calendarEventSnapshot(
          preRead.event,
          eventId,
          calendarEventTimeZone(action.mutation.target.observedEvent),
        );
        if (
          observed?.status !== "confirmed" ||
          observed.revision !== action.mutation.target.providerRevision ||
          !sameCalendarEventDraft(observed.event, action.mutation.target.observedEvent)
        ) {
          return failed("The Family Calendar event changed after Florence proposed this action");
        }
        preRevision = observed.revision;
      }
    }

    const eventBody =
      action.mutation.operation === "delete"
        ? null
        : calendarMutationEventBody(
            action.mutation.event,
            action.mutation.operation === "create" ? eventId : null,
          );
    const requestUrl =
      action.mutation.operation === "create"
        ? `${calendarUrl}/events?sendUpdates=none`
        : `${eventUrl}?sendUpdates=none`;
    const method =
      action.mutation.operation === "create"
        ? "POST"
        : action.mutation.operation === "update"
          ? "PATCH"
          : "DELETE";
    let disposition: "accepted" | "ambiguous" | "rejected" | "credential_rejected" = "ambiguous";
    let rejectedStatus: number | null = null;
    try {
      const response = await this.#fetch(requestUrl, {
        method,
        headers: {
          authorization: `Bearer ${accessToken}`,
          ...(eventBody === null ? {} : { "content-type": "application/json" }),
          ...(preRevision === null ? {} : { "if-match": preRevision }),
        },
        ...(eventBody === null ? {} : { body: JSON.stringify(eventBody) }),
        signal: AbortSignal.timeout(15_000),
      });
      if (response.ok) disposition = "accepted";
      else if (response.status === 401 || response.status === 403) {
        disposition = "credential_rejected";
        rejectedStatus = response.status;
      } else if (response.status === 409 || transientHttpStatus(response.status)) {
        disposition = "ambiguous";
      } else {
        disposition = "rejected";
        rejectedStatus = response.status;
      }
    } catch {
      // The mandatory read below resolves successful responses lost after any mutation.
      disposition = "ambiguous";
    }

    const confirmationRead = await this.#readCalendarMutationEvent(accessToken, calendarId, eventId);
    if (confirmationRead.status === "credential_rejected") {
      return credentialRejected(confirmationRead.detail);
    }
    if (confirmationRead.status === "rejected") return failed(confirmationRead.detail);

    if (action.mutation.operation === "delete") {
      if (confirmationRead.status === "missing") {
        return committedCalendarResult(eventId, null);
      }
      const observed = calendarEventSnapshot(
        confirmationRead.event,
        eventId,
        calendarEventTimeZone(action.mutation.target.observedEvent),
      );
      if (observed?.status === "cancelled") {
        return committedCalendarResult(eventId, observed.revision);
      }
      if (disposition === "credential_rejected" || (disposition === "rejected" && rejectedStatus === 401)) {
        return credentialRejected(
          `Google rejected the Calendar delete credential with HTTP ${rejectedStatus}`,
        );
      }
      if (disposition === "rejected") {
        return failed(`Google rejected the Calendar delete with HTTP ${rejectedStatus}`);
      }
      if (
        observed?.status === "confirmed" &&
        observed.revision === preRevision &&
        sameCalendarEventDraft(observed.event, action.mutation.target.observedEvent)
      ) {
        throw transientCalendarError("The Google Calendar delete is not visible yet");
      }
      return failed("The Family Calendar event changed while Florence was deleting it");
    }

    if (confirmationRead.status === "found") {
      const confirmed = confirmedCalendarEvent(confirmationRead.event, eventId, action.mutation.event);
      if (confirmed) return committedCalendarResult(eventId, confirmed.revision);
    }

    if (disposition === "credential_rejected") {
      return credentialRejected(
        `Google rejected the Calendar ${action.mutation.operation} credential with HTTP ${rejectedStatus}`,
      );
    }
    if (disposition === "rejected") {
      return failed(`Google rejected the Calendar ${action.mutation.operation} with HTTP ${rejectedStatus}`);
    }
    if (confirmationRead.status === "missing") {
      throw transientCalendarError(`The Google Calendar ${action.mutation.operation} is not visible yet`);
    }
    if (action.mutation.operation === "update") {
      const observed = calendarEventSnapshot(
        confirmationRead.event,
        eventId,
        calendarEventTimeZone(action.mutation.target.observedEvent),
      );
      if (
        observed?.status === "confirmed" &&
        observed.revision === preRevision &&
        sameCalendarEventDraft(observed.event, action.mutation.target.observedEvent)
      ) {
        throw transientCalendarError("The Google Calendar update is not visible yet");
      }
    }
    return failed("Google Calendar did not preserve the exact Family Calendar event");
  }

  async #readCalendarMutationEvent(
    accessToken: string,
    calendarId: string,
    eventId: string,
  ): Promise<CalendarMutationEventRead> {
    let response: Response;
    try {
      response = await this.#fetch(
        `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}`,
        {
          headers: { authorization: `Bearer ${accessToken}` },
          signal: AbortSignal.timeout(15_000),
        },
      );
    } catch (error) {
      throw transientCalendarError("Google Calendar confirmation read failed", error);
    }
    if (response.status === 404 || response.status === 410) return { status: "missing" };
    if (response.status === 401 || response.status === 403) {
      return {
        status: "credential_rejected",
        detail: `Google rejected the Calendar confirmation credential with HTTP ${response.status}`,
      };
    }
    if (transientHttpStatus(response.status)) {
      throw transientCalendarError(`Google Calendar confirmation read returned HTTP ${response.status}`);
    }
    if (!response.ok) {
      return {
        status: "rejected",
        detail: `Google rejected the Calendar confirmation read with HTTP ${response.status}`,
      };
    }
    try {
      return { status: "found", event: await safeJson(response) };
    } catch {
      return { status: "rejected", detail: "Google returned an invalid Calendar confirmation" };
    }
  }

  async #createFamilyCalendar(
    accessToken: string,
    input: { summary: string; description: string; timeZone: string },
  ): Promise<string | null> {
    let response: Response;
    try {
      response = await this.#fetch("https://www.googleapis.com/calendar/v3/calendars", {
        method: "POST",
        headers: {
          authorization: `Bearer ${accessToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(input),
        signal: AbortSignal.timeout(15_000),
      });
    } catch {
      return null;
    }
    if (response.status === 409 || transientHttpStatus(response.status)) return null;
    if (!response.ok) {
      throw new GoogleFamilyCalendarProvisioningError(
        `Google rejected Family Calendar creation with HTTP ${response.status}`,
        "provider_rejected",
      );
    }
    try {
      return secondaryCalendarTarget(stringField(await safeJson(response), "id"));
    } catch {
      // A successful create with an unreadable response may have committed. Its marker is the only
      // safe recovery key because the durable create latch forbids another POST.
      return null;
    }
  }

  async #findFamilyCalendarByMarker(
    accessToken: string,
    expectedDescription: string,
  ): Promise<string | null> {
    const matches = new Set<string>();
    const query = new URLSearchParams({
      fields: "nextPageToken,items(id,description,deleted,primary)",
      maxResults: "250",
      minAccessRole: "owner",
      showDeleted: "false",
      showHidden: "true",
    });
    let response: Response;
    try {
      response = await this.#fetch(`https://www.googleapis.com/calendar/v3/users/me/calendarList?${query}`, {
        headers: { authorization: `Bearer ${accessToken}` },
        signal: AbortSignal.timeout(15_000),
      });
    } catch (error) {
      throw transientCalendarError("Google Family Calendar reconciliation failed", error);
    }
    if (transientHttpStatus(response.status)) {
      throw transientCalendarError(`Google Family Calendar reconciliation returned HTTP ${response.status}`);
    }
    if (!response.ok) {
      throw new GoogleFamilyCalendarProvisioningError(
        `Google rejected Family Calendar reconciliation with HTTP ${response.status}`,
        "provider_rejected",
      );
    }
    let body: Record<string, unknown>;
    let nextPageToken: string | null;
    try {
      body = await safeJson(response);
      nextPageToken = optionalStringField(body, "nextPageToken");
      for (const entry of recordArray(body.items)) {
        if (entry.description !== expectedDescription || entry.deleted === true) continue;
        if (entry.deleted !== undefined && typeof entry.deleted !== "boolean") {
          throw new Error("invalid deleted flag");
        }
        if (entry.primary === true) throw new Error("marker appeared on primary Calendar");
        matches.add(secondaryCalendarTarget(stringField(entry, "id")));
      }
    } catch {
      throw new GoogleFamilyCalendarProvisioningError(
        "Google returned an invalid marked Family Calendar list",
        "provider_rejected",
      );
    }
    if (matches.size > 1) {
      throw new GoogleFamilyCalendarProvisioningError(
        "Google contains multiple calendars with this household marker; inspect them before retrying",
        "manual_repair_required",
      );
    }
    const match = matches.values().next().value ?? null;
    if (match) return match;
    if (nextPageToken) {
      throw new GoogleFamilyCalendarProvisioningError(
        "The marked Family Calendar was not on the first Google calendar page; inspect it before retrying",
        "manual_repair_required",
      );
    }
    return null;
  }

  async #readFamilyCalendar(
    accessToken: string,
    calendarId: string,
    summary: string,
    description: string,
    timeZone: string,
  ): Promise<void> {
    let response: Response;
    try {
      response = await this.#fetch(
        `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}`,
        {
          headers: { authorization: `Bearer ${accessToken}` },
          signal: AbortSignal.timeout(15_000),
        },
      );
    } catch (error) {
      throw transientCalendarError("Google Family Calendar verification read failed", error);
    }
    if (transientHttpStatus(response.status)) {
      throw transientCalendarError(
        `Google Family Calendar verification read returned HTTP ${response.status}`,
      );
    }
    if (!response.ok) {
      throw new GoogleFamilyCalendarProvisioningError(
        `Google rejected the Family Calendar verification read with HTTP ${response.status}`,
        "provider_rejected",
      );
    }
    let calendar: Record<string, unknown>;
    try {
      calendar = await safeJson(response);
    } catch {
      throw new GoogleFamilyCalendarProvisioningError(
        "Google returned an invalid Family Calendar",
        "provider_rejected",
      );
    }
    try {
      if (
        stringField(calendar, "id") !== calendarId ||
        stringField(calendar, "summary") !== summary ||
        stringField(calendar, "description") !== description ||
        stringField(calendar, "timeZone") !== timeZone
      ) {
        throw invalidFamilyCalendarProvider("Google did not preserve the exact Family Calendar");
      }
    } catch (error) {
      if (error instanceof GoogleFamilyCalendarProvisioningError) throw error;
      throw invalidFamilyCalendarProvider("Google returned an incomplete Family Calendar");
    }
  }

  async #readOwnerAcl(
    accessToken: string,
    calendarId: string,
    email: string,
  ): Promise<{ id: string; role: string } | null> {
    for (const rule of await this.#readCalendarAcls(accessToken, calendarId)) {
      const match = calendarAclForEmail(rule, email);
      if (match?.role === "owner") return match;
    }
    return null;
  }

  async #readCalendarAcls(
    accessToken: string,
    calendarId: string,
  ): Promise<readonly Record<string, unknown>[]> {
    const rules: Record<string, unknown>[] = [];
    let pageToken: string | null = null;
    for (let page = 0; page < 25; page += 1) {
      const query = new URLSearchParams({
        fields: "nextPageToken,items(deleted,id,role,scope(type,value))",
        maxResults: "250",
        showDeleted: "false",
      });
      if (pageToken) query.set("pageToken", pageToken);
      let response: Response;
      try {
        response = await this.#fetch(
          `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/acl?${query}`,
          {
            headers: { authorization: `Bearer ${accessToken}` },
            signal: AbortSignal.timeout(15_000),
          },
        );
      } catch (error) {
        throw transientCalendarError("Google Family Calendar owner verification failed", error);
      }
      if (transientHttpStatus(response.status)) {
        throw transientCalendarError(
          `Google Family Calendar owner verification returned HTTP ${response.status}`,
        );
      }
      if (!response.ok) {
        throw new GoogleFamilyCalendarProvisioningError(
          `Google rejected the Family Calendar owner verification with HTTP ${response.status}`,
          "provider_rejected",
        );
      }
      let body: Record<string, unknown>;
      try {
        body = await safeJson(response);
        rules.push(...recordArray(body.items));
      } catch {
        throw new GoogleFamilyCalendarProvisioningError(
          "Google returned an invalid Family Calendar owner list",
          "provider_rejected",
        );
      }
      const nextPageToken = body.nextPageToken;
      if (nextPageToken === undefined) return rules;
      if (typeof nextPageToken !== "string" || !nextPageToken) {
        throw new GoogleFamilyCalendarProvisioningError(
          "Google returned an invalid Family Calendar owner page",
          "provider_rejected",
        );
      }
      pageToken = nextPageToken;
    }
    throw new GoogleFamilyCalendarProvisioningError(
      "Google Family Calendar has too many owner rules to verify safely",
      "provider_rejected",
    );
  }

  async #ensureOwnerAcl(accessToken: string, calendarId: string, email: string): Promise<void> {
    const existingRules = await this.#readCalendarAcls(accessToken, calendarId);
    const existing = existingRules
      .map((rule) => calendarAclForEmail(rule, email))
      .find((rule) => rule !== null);
    if (existing?.role === "owner") return;

    const collectionUrl = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/acl`;
    const requestUrl = existing
      ? `${collectionUrl}/${encodeURIComponent(existing.id)}?sendNotifications=true`
      : `${collectionUrl}?sendNotifications=true`;
    const requestBody = existing
      ? { role: "owner" }
      : { role: "owner", scope: { type: "user", value: email } };
    let disposition: "accepted" | "retryable" | "rejected" = "retryable";
    let rejectedStatus: number | null = null;
    let acceptedResponseValid = true;
    try {
      const response = await this.#fetch(requestUrl, {
        method: existing ? "PATCH" : "POST",
        headers: {
          authorization: `Bearer ${accessToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(requestBody),
        signal: AbortSignal.timeout(15_000),
      });
      if (response.ok) {
        disposition = "accepted";
        try {
          const verified = calendarAclForEmail(await safeJson(response), email);
          acceptedResponseValid = verified?.role === "owner";
        } catch {
          acceptedResponseValid = false;
        }
      } else if (response.status === 409 || transientHttpStatus(response.status)) {
        disposition = "retryable";
      } else {
        disposition = "rejected";
        rejectedStatus = response.status;
      }
    } catch {
      // A timed-out ACL write may have committed; the owner-list read below resolves it.
    }

    if (await this.#readOwnerAcl(accessToken, calendarId, email)) return;
    if (disposition === "rejected") {
      throw new GoogleFamilyCalendarProvisioningError(
        `Google rejected partner Family Calendar ownership with HTTP ${rejectedStatus}`,
        "provider_rejected",
      );
    }
    if (!acceptedResponseValid) {
      throw new GoogleFamilyCalendarProvisioningError(
        "Google returned an invalid partner Family Calendar owner rule",
        "provider_rejected",
      );
    }
    throw transientCalendarError(
      disposition === "accepted"
        ? "The partner Family Calendar owner rule is not visible yet"
        : "Google has not confirmed the partner Family Calendar owner rule",
    );
  }

  async #ensurePartnerCalendarList(
    accessToken: string,
    calendarId: string,
    summary: string,
    timeZone: string,
  ): Promise<void> {
    const existing = await this.#readCalendarListEntry(accessToken, calendarId);
    if (familyCalendarListReady(existing, calendarId, summary, timeZone)) return;
    if (existing && existing.accessRole !== "owner") {
      throw transientCalendarError("The partner Family Calendar owner access is not visible yet");
    }

    const collectionUrl = "https://www.googleapis.com/calendar/v3/users/me/calendarList";
    const requestUrl = existing ? `${collectionUrl}/${encodeURIComponent(calendarId)}` : collectionUrl;
    let disposition: "accepted" | "retryable" | "rejected" = "retryable";
    let rejectedStatus: number | null = null;
    let acceptedResponseValid = true;
    try {
      const response = await this.#fetch(requestUrl, {
        method: existing ? "PATCH" : "POST",
        headers: {
          authorization: `Bearer ${accessToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(existing ? { selected: true } : { id: calendarId, selected: true }),
        signal: AbortSignal.timeout(15_000),
      });
      if (response.ok) {
        disposition = "accepted";
        try {
          acceptedResponseValid = Boolean(
            familyCalendarListReady(
              calendarListEntry(await safeJson(response), calendarId),
              calendarId,
              summary,
              timeZone,
            ),
          );
        } catch {
          acceptedResponseValid = false;
        }
      } else if (response.status === 409 || transientHttpStatus(response.status)) {
        disposition = "retryable";
      } else {
        disposition = "rejected";
        rejectedStatus = response.status;
      }
    } catch {
      // A timed-out CalendarList write may have committed; the read below resolves it.
    }

    const verified = familyCalendarListReady(
      await this.#readCalendarListEntry(accessToken, calendarId),
      calendarId,
      summary,
      timeZone,
    );
    if (verified) return;
    if (disposition === "rejected") {
      throw new GoogleFamilyCalendarProvisioningError(
        `Google rejected the partner Family Calendar list entry with HTTP ${rejectedStatus}`,
        "provider_rejected",
      );
    }
    if (!acceptedResponseValid) {
      throw new GoogleFamilyCalendarProvisioningError(
        "Google returned an invalid partner Family Calendar list entry",
        "provider_rejected",
      );
    }
    throw transientCalendarError(
      disposition === "accepted"
        ? "The partner Family Calendar list entry is not visible yet"
        : "Google has not confirmed the partner Family Calendar list entry",
    );
  }

  async #readCalendarListEntry(
    accessToken: string,
    calendarId: string,
  ): Promise<FamilyCalendarListEntry | null> {
    const query = new URLSearchParams({
      fields: "accessRole,id,primary,selected,summary,timeZone",
    });
    let response: Response;
    try {
      response = await this.#fetch(
        `https://www.googleapis.com/calendar/v3/users/me/calendarList/${encodeURIComponent(calendarId)}?${query}`,
        {
          headers: { authorization: `Bearer ${accessToken}` },
          signal: AbortSignal.timeout(15_000),
        },
      );
    } catch (error) {
      throw transientCalendarError("Google partner Calendar list verification failed", error);
    }
    if (response.status === 404) return null;
    if (transientHttpStatus(response.status)) {
      throw transientCalendarError(
        `Google partner Calendar list verification returned HTTP ${response.status}`,
      );
    }
    if (!response.ok) {
      throw new GoogleFamilyCalendarProvisioningError(
        `Google rejected the partner Calendar list verification with HTTP ${response.status}`,
        "provider_rejected",
      );
    }
    try {
      return calendarListEntry(await safeJson(response), calendarId);
    } catch {
      throw new GoogleFamilyCalendarProvisioningError(
        "Google returned an invalid partner Family Calendar list entry",
        "provider_rejected",
      );
    }
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
      signal: AbortSignal.timeout(15_000),
    });
    if (!response.ok) {
      await this.#throwIfRefreshTokenInvalidGrant(response, credential);
      throw providerError("Google access-token refresh failed");
    }
    const token = await safeJson(response);
    if (token.token_type !== "Bearer") throw providerError("Google returned an unsupported token type");
    return stringField(token, "access_token");
  }

  async #readGmailMessage(
    accessToken: string,
    expected: { messageId: string; threadId: string; historyId: string },
  ): Promise<GmailEvidence> {
    const evidence = await this.#readCurrentGmailMessage(accessToken, expected);
    if (!evidence) throw providerError("Gmail message could not be read");
    if (evidence.historyId !== expected.historyId) {
      throw providerError("Gmail returned evidence for a different message");
    }
    return evidence;
  }

  async #readCurrentGmailMessage(
    accessToken: string,
    expected: { messageId: string; threadId: string },
  ): Promise<GmailEvidence | null> {
    const response = await this.#fetch(
      `https://gmail.googleapis.com/gmail/v1/users/me/messages/${encodeURIComponent(expected.messageId)}?format=full`,
      {
        headers: { authorization: `Bearer ${accessToken}` },
        signal: AbortSignal.timeout(15_000),
      },
    );
    if (response.status === 404) return null;
    if (!response.ok) throw providerError("Gmail message could not be read");
    const message = await safeJson(response);
    const messageId = stringField(message, "id");
    const threadId = stringField(message, "threadId");
    if (messageId !== expected.messageId || threadId !== expected.threadId) {
      throw providerError("Gmail returned evidence for a different message");
    }
    return gmailEvidence(message);
  }

  async #readInlineGmailAttachmentBody(
    accessToken: string,
    expected: GmailAttachmentReference,
  ): Promise<Record<string, unknown>> {
    const message = await this.#gmailJson(
      `messages/${encodeURIComponent(expected.messageId)}?format=full`,
      accessToken,
    );
    if (
      stringField(message, "id") !== expected.messageId ||
      stringField(message, "threadId") !== expected.threadId ||
      stringField(message, "historyId") !== expected.historyId
    ) {
      throw providerError("Gmail inline attachment message identity changed");
    }
    const part = findGmailPart(recordField(message, "payload"), expected.partId);
    if (
      !part ||
      typeof part.filename !== "string" ||
      part.filename.trim() !== expected.filename ||
      supportedGmailAttachmentMimeType(String(part.mimeType ?? "")) !== expected.mimeType
    ) {
      throw providerError("Gmail inline attachment part identity changed");
    }
    const body = recordField(part, "body");
    if (body.attachmentId !== undefined) {
      throw providerError("Gmail inline attachment storage identity changed");
    }
    return body;
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
      await this.#throwIfRefreshTokenInvalidGrant(response, credential);
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

  async #throwIfRefreshTokenInvalidGrant(
    response: Response,
    credential: ActiveGoogleCredential,
  ): Promise<void> {
    if (transientHttpStatus(response.status) || (await oauthErrorCode(response)) !== "invalid_grant") {
      return;
    }
    await this.#store.disconnect({
      connectionId: credential.connectionId,
      householdId: credential.householdId,
      ownerAdultId: credential.ownerAdultId,
      now: new Date().toISOString(),
    });
    throw new GoogleConnectionError(
      "The active Google credential is no longer valid",
      "credential_invalid_grant",
    );
  }

  async #gmailJson(path: string, accessToken: string): Promise<Record<string, unknown>> {
    const response = await this.#fetch(`https://gmail.googleapis.com/gmail/v1/users/me/${path}`, {
      headers: { authorization: `Bearer ${accessToken}` },
      signal: AbortSignal.timeout(15_000),
    });
    if (!response.ok) throw providerError("Gmail read failed");
    return safeJson(response);
  }

  async #messageIdentity(
    accessToken: string,
    listed: Record<string, unknown>,
  ): Promise<{ messageId: string; threadId: string; historyId: string }> {
    const messageId = stringField(listed, "id");
    const threadId = stringField(listed, "threadId");
    const message = await this.#gmailJson(
      `messages/${encodeURIComponent(messageId)}?format=metadata`,
      accessToken,
    );
    if (stringField(message, "id") !== messageId || stringField(message, "threadId") !== threadId) {
      throw providerError("Gmail message identity changed during search");
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

function transientCalendarError(message: string, cause?: unknown): GoogleCalendarTransientError {
  return new GoogleCalendarTransientError(message, cause === undefined ? undefined : { cause });
}

function transientHttpStatus(status: number): boolean {
  return status === 403 || status === 408 || status === 425 || status === 429 || status >= 500;
}

function exactActiveConnection(
  connections: readonly GoogleConnectionView[],
  householdId: string,
  ownerAdultId: string,
  connectionId: string,
): GoogleConnectionView | null {
  return (
    connections.find(
      (connection) =>
        connection.connectionId === connectionId &&
        connection.householdId === householdId &&
        connection.ownerAdultId === ownerAdultId &&
        connection.status === "active",
    ) ?? null
  );
}

function calendarAclForEmail(
  rule: Record<string, unknown>,
  email: string,
): { id: string; role: string } | null {
  if (rule.deleted === true) return null;
  if (rule.deleted !== undefined && typeof rule.deleted !== "boolean") {
    throw invalidFamilyCalendarProvider("Google returned an invalid Family Calendar owner rule");
  }
  const scope = rule.scope;
  if (!scope || typeof scope !== "object" || Array.isArray(scope)) {
    throw invalidFamilyCalendarProvider("Google returned an invalid Family Calendar owner scope");
  }
  const scopeRecord = scope as Record<string, unknown>;
  if (typeof scopeRecord.type !== "string" || !scopeRecord.type) {
    throw invalidFamilyCalendarProvider("Google returned an invalid Family Calendar owner scope");
  }
  if (scopeRecord.type !== "user") return null;
  if (typeof scopeRecord.value !== "string" || !scopeRecord.value) {
    throw invalidFamilyCalendarProvider("Google returned an incomplete Family Calendar user scope");
  }
  if (scopeRecord.value.trim().toLowerCase() !== email.trim().toLowerCase()) return null;
  if (typeof rule.id !== "string" || !rule.id || typeof rule.role !== "string" || !rule.role) {
    throw invalidFamilyCalendarProvider("Google returned an incomplete Family Calendar owner rule");
  }
  return { id: rule.id, role: rule.role };
}

function calendarListEntry(
  value: Record<string, unknown>,
  expectedCalendarId: string,
): FamilyCalendarListEntry {
  if (value.id !== expectedCalendarId) {
    throw invalidFamilyCalendarProvider("Google returned a different partner Calendar list entry");
  }
  if (
    typeof value.summary !== "string" ||
    !value.summary ||
    typeof value.timeZone !== "string" ||
    !value.timeZone ||
    typeof value.accessRole !== "string" ||
    !value.accessRole ||
    (value.selected !== undefined && typeof value.selected !== "boolean") ||
    (value.primary !== undefined && typeof value.primary !== "boolean")
  ) {
    throw invalidFamilyCalendarProvider("Google returned an incomplete partner Calendar list entry");
  }
  return {
    id: expectedCalendarId,
    summary: value.summary,
    timeZone: value.timeZone,
    accessRole: value.accessRole,
    selected: value.selected === true,
    primary: value.primary === true,
  };
}

function familyCalendarListReady(
  entry: FamilyCalendarListEntry | null,
  calendarId: string,
  summary: string,
  timeZone: string,
): boolean {
  return Boolean(
    entry &&
      entry.id === calendarId &&
      entry.summary === summary &&
      entry.timeZone === timeZone &&
      entry.accessRole === "owner" &&
      entry.selected &&
      !entry.primary,
  );
}

function invalidFamilyCalendarProvider(message: string): GoogleFamilyCalendarProvisioningError {
  return new GoogleFamilyCalendarProvisioningError(message, "provider_rejected");
}

export function familyCalendarProvisioningMarker(householdId: string): string {
  return digest(`florence-family-calendar-v1\0${required(householdId, "household ID")}`);
}

function familyCalendarDescription(provisioningMarker: string): string {
  assertDigest(provisioningMarker, "Family Calendar provisioning marker");
  return `Florence Family Calendar\nflorence-household-marker:${provisioningMarker}`;
}

export function googleCalendarEventId(actionId: string): string {
  return digest(`florence-google-calendar-event-v1\0${required(actionId, "Calendar action ID")}`);
}

function validateCalendarAction(action: ApprovedCalendarAction): void {
  for (const [value, label] of [
    [action.householdId, "household ID"],
    [action.connectionId, "connection ID"],
    [action.ownerAdultId, "owner adult ID"],
    [action.actionId, "action ID"],
  ] as const) {
    required(value, label);
  }
  if (secondaryCalendarTarget(action.calendarId) !== action.calendarId) {
    throw new Error("Family Calendar ID must be canonical");
  }
  if (action.mutation.operation === "create") {
    if (action.mutation.target !== null) throw new Error("Calendar create cannot have a target");
    validateCalendarEventDraft(action.mutation.event);
    return;
  }
  if (action.mutation.operation === "update") {
    validateCalendarEventDraft(action.mutation.event);
  } else if (action.mutation.operation === "delete") {
    if (action.mutation.event !== null) throw new Error("Calendar delete cannot have an event draft");
  } else {
    throw new Error("Calendar operation is invalid");
  }
  boundedRequired(action.mutation.target.providerEventId, "Calendar provider event ID", 1_024);
  boundedRequired(action.mutation.target.providerRevision, "Calendar provider revision", 500);
  validateCalendarEventDraft(action.mutation.target.observedEvent);
}

function validateCalendarEventDraft(event: CalendarEventDraft): void {
  boundedRequired(event.title, "Calendar title", 500);
  if (event.location !== null) boundedRequired(event.location, "Calendar location", 500);
  if (event.intervalKind === "all_day") {
    const startDate = calendarDate(event.startDate);
    const endDate = calendarDate(event.endDate);
    if (endDate <= startDate) throw new Error("All-day Calendar end date must follow start date");
    return;
  }
  calendarTimeZone(event.timeZone);
  const startsAt = explicitInstant(event.startsAt);
  const endsAt = explicitInstant(event.endsAt);
  if (endsAt <= startsAt) throw new Error("Calendar end must follow start");
}

function calendarWindowEvent(
  event: Record<string, unknown>,
  fallbackTimeZone: string,
  timeMin: Date,
  timeMax: Date,
  eventSelection: "busy" | "all",
): GoogleCalendarWindowEvent | null {
  const status = calendarEventStatus(event.status);
  if (status === "cancelled") return null;
  const busy = calendarEventBusy(event);
  if (eventSelection === "busy" && !busy) return null;
  const start = recordField(event, "start");
  const end = recordField(event, "end");
  const startDateTime = optionalStringField(start, "dateTime");
  const endDateTime = optionalStringField(end, "dateTime");
  const startDate = optionalStringField(start, "date");
  const endDate = optionalStringField(end, "date");
  const allDay = startDateTime === null && endDateTime === null;
  if (allDay !== (startDate !== null && endDate !== null)) {
    throw providerError("Google returned an invalid Calendar event interval");
  }
  if (!allDay && (!startDateTime || !endDateTime || startDate || endDate)) {
    throw providerError("Google returned an invalid Calendar event interval");
  }
  const eventTimeZone = calendarTimeZone(
    allDay ? fallbackTimeZone : (optionalStringField(start, "timeZone") ?? fallbackTimeZone),
  );
  calendarTimeZone(allDay ? fallbackTimeZone : (optionalStringField(end, "timeZone") ?? eventTimeZone));
  const exactStartDate = allDay ? calendarDate(required(startDate ?? "", "Calendar all-day start")) : null;
  const exactEndDate = allDay ? calendarDate(required(endDate ?? "", "Calendar all-day end")) : null;
  const startInstant = allDay
    ? zonedDateStart(exactStartDate ?? "", eventTimeZone)
    : explicitInstant(startDateTime ?? "");
  const endInstant = allDay
    ? zonedDateStart(exactEndDate ?? "", eventTimeZone)
    : explicitInstant(endDateTime ?? "");
  if (endInstant <= startInstant) {
    throw providerError("Google returned an invalid Calendar event interval");
  }
  if (endInstant <= timeMin || startInstant >= timeMax) return null;
  const summary = optionalStringField(event, "summary");
  const location = optionalStringField(event, "location");
  const common = {
    providerEventId: bounded(stringField(event, "id"), 1_024),
    providerRevision: bounded(stringField(event, "etag"), 500),
    providerUpdatedAt: explicitInstant(stringField(event, "updated")).toISOString(),
    status,
    busy,
    title: summary === null ? null : bounded(summary, 500),
    location: location === null ? null : bounded(location, 500),
  };
  return allDay
    ? {
        ...common,
        intervalKind: "all_day",
        startDate: exactStartDate ?? "",
        endDate: exactEndDate ?? "",
      }
    : {
        ...common,
        intervalKind: "timed",
        startsAt: startInstant.toISOString(),
        endsAt: endInstant.toISOString(),
        timeZone: eventTimeZone,
      };
}

function calendarChangedEvent(
  event: Record<string, unknown>,
  fallbackTimeZone: string,
): GoogleCalendarChange {
  const providerEventId = bounded(stringField(event, "id"), 1_024);
  const providerRevision = bounded(stringField(event, "etag"), 500);
  const providerUpdatedAt = explicitInstant(stringField(event, "updated")).toISOString();
  const status = calendarEventStatus(event.status);
  const summary = optionalStringField(event, "summary");
  const title = summary === null ? null : bounded(summary, 500);
  if (status === "cancelled") {
    return {
      providerEventId,
      providerRevision,
      providerUpdatedAt,
      status,
      busy: false,
      title,
      startsAt: null,
      endsAt: null,
      allDay: null,
      timeZone: null,
      startDate: null,
      endDate: null,
    };
  }

  const start = recordField(event, "start");
  const end = recordField(event, "end");
  const startDateTime = optionalStringField(start, "dateTime");
  const endDateTime = optionalStringField(end, "dateTime");
  const startDate = optionalStringField(start, "date");
  const endDate = optionalStringField(end, "date");
  const allDay = startDateTime === null && endDateTime === null;
  if (allDay !== (startDate !== null && endDate !== null)) {
    throw providerError("Google returned an invalid Calendar event interval");
  }
  if (!allDay && (!startDateTime || !endDateTime || startDate || endDate)) {
    throw providerError("Google returned an invalid Calendar event interval");
  }
  const eventTimeZone = allDay
    ? null
    : calendarTimeZone(optionalStringField(start, "timeZone") ?? fallbackTimeZone);
  if (!allDay) {
    calendarTimeZone(optionalStringField(end, "timeZone") ?? eventTimeZone ?? fallbackTimeZone);
  }
  const startInstant = allDay
    ? zonedDateStart(required(startDate ?? "", "Calendar all-day start"), fallbackTimeZone)
    : explicitInstant(startDateTime ?? "");
  const endInstant = allDay
    ? zonedDateStart(required(endDate ?? "", "Calendar all-day end"), fallbackTimeZone)
    : explicitInstant(endDateTime ?? "");
  if (endInstant <= startInstant) {
    throw providerError("Google returned an invalid Calendar event interval");
  }
  const busy = calendarEventBusy(event);
  return {
    providerEventId,
    providerRevision,
    providerUpdatedAt,
    status,
    busy,
    title,
    startsAt: startInstant.toISOString(),
    endsAt: endInstant.toISOString(),
    allDay,
    timeZone: eventTimeZone,
    startDate: allDay ? calendarDate(required(startDate ?? "", "Calendar all-day start")) : null,
    endDate: allDay ? calendarDate(required(endDate ?? "", "Calendar all-day end")) : null,
  };
}

function calendarEventStatus(value: unknown): GoogleCalendarChange["status"] {
  if (value !== "confirmed" && value !== "tentative" && value !== "cancelled") {
    throw providerError("Google returned an invalid Calendar event status");
  }
  return value;
}

function calendarEventBusy(event: Record<string, unknown>): boolean {
  return (
    event.transparency !== "transparent" &&
    !recordArray(event.attendees).some(
      (attendee) => attendee.self === true && attendee.responseStatus === "declined",
    )
  );
}

function confirmedCalendarEvent(
  event: Record<string, unknown>,
  expectedId: string,
  expectedEvent: CalendarEventDraft,
): { revision: string; event: CalendarEventDraft } | null {
  try {
    const snapshot = calendarEventSnapshot(event, expectedId, calendarEventTimeZone(expectedEvent));
    if (snapshot?.status !== "confirmed" || snapshot.event === null) return null;
    if (!sameCalendarEventDraft(snapshot.event, expectedEvent)) return null;
    if (snapshot.revision === null) return null;
    return { revision: snapshot.revision, event: snapshot.event };
  } catch {
    return null;
  }
}

function calendarEventSnapshot(
  event: Record<string, unknown>,
  expectedId: string,
  fallbackTimeZone?: string,
): {
  revision: string | null;
  status: GoogleCalendarChange["status"];
  event: CalendarEventDraft | null;
} | null {
  try {
    if (stringField(event, "id") !== expectedId) return null;
    const status = calendarEventStatus(event.status);
    const revision = event.etag === undefined ? null : bounded(stringField(event, "etag"), 500);
    if (status === "cancelled") return { revision, status, event: null };
    if (revision === null) return null;
    const start = recordField(event, "start");
    const end = recordField(event, "end");
    const startDateTime = optionalStringField(start, "dateTime");
    const endDateTime = optionalStringField(end, "dateTime");
    const startDate = optionalStringField(start, "date");
    const endDate = optionalStringField(end, "date");
    const allDay = startDateTime === null && endDateTime === null;
    if (allDay !== (startDate !== null && endDate !== null)) return null;
    if (!allDay && (!startDateTime || !endDateTime || startDate || endDate)) return null;
    const location = event.location === undefined ? null : event.location;
    if (location !== null && typeof location !== "string") return null;
    const common = {
      title: bounded(stringField(event, "summary"), 500),
      location: location === null ? null : bounded(location, 500),
    };
    let draft: CalendarEventDraft;
    if (allDay) {
      draft = {
        ...common,
        intervalKind: "all_day",
        startDate: calendarDate(startDate ?? ""),
        endDate: calendarDate(endDate ?? ""),
      };
    } else {
      const startTimeZone = calendarTimeZone(
        optionalStringField(start, "timeZone") ?? required(fallbackTimeZone ?? "", "Calendar time zone"),
      );
      const endTimeZone = calendarTimeZone(optionalStringField(end, "timeZone") ?? startTimeZone);
      if (startTimeZone !== endTimeZone) return null;
      draft = {
        ...common,
        intervalKind: "timed",
        startsAt: explicitInstant(startDateTime ?? "").toISOString(),
        endsAt: explicitInstant(endDateTime ?? "").toISOString(),
        timeZone: startTimeZone,
      };
    }
    validateCalendarEventDraft(draft);
    return { revision, status, event: draft };
  } catch {
    return null;
  }
}

function calendarMutationEventBody(
  event: CalendarEventDraft,
  eventId: string | null,
): Record<string, unknown> {
  return {
    ...(eventId === null ? {} : { id: eventId }),
    summary: event.title,
    start:
      event.intervalKind === "timed"
        ? { dateTime: event.startsAt, timeZone: event.timeZone }
        : { date: event.startDate },
    end:
      event.intervalKind === "timed"
        ? { dateTime: event.endsAt, timeZone: event.timeZone }
        : { date: event.endDate },
    ...(event.location === null && eventId !== null ? {} : { location: event.location }),
  };
}

function sameCalendarEventDraft(left: CalendarEventDraft | null, right: CalendarEventDraft): boolean {
  if (
    !left ||
    left.intervalKind !== right.intervalKind ||
    left.title !== right.title ||
    left.location !== right.location
  ) {
    return false;
  }
  if (left.intervalKind === "all_day" && right.intervalKind === "all_day") {
    return left.startDate === right.startDate && left.endDate === right.endDate;
  }
  return (
    left.intervalKind === "timed" &&
    right.intervalKind === "timed" &&
    sameInstant(left.startsAt, right.startsAt) &&
    sameInstant(left.endsAt, right.endsAt) &&
    left.timeZone === right.timeZone
  );
}

function calendarEventTimeZone(event: CalendarEventDraft): string | undefined {
  return event.intervalKind === "timed" ? event.timeZone : undefined;
}

function committedCalendarResult(
  eventId: string,
  providerRevision: string | null,
): GoogleCalendarExecutionResult {
  return {
    status: "committed",
    providerEventId: eventId,
    providerRevision,
    occurredAt: new Date().toISOString(),
  };
}

function sameInstant(left: string, right: string): boolean {
  try {
    return explicitInstant(left).getTime() === explicitInstant(right).getTime();
  } catch {
    return false;
  }
}

function exactScopes(value: string): readonly GoogleScope[] {
  const actual = new Set(
    value
      .split(/\s+/)
      .filter(Boolean)
      .map((scope) => (scope === "https://www.googleapis.com/auth/userinfo.email" ? "email" : scope)),
  );
  if (GOOGLE_SCOPES.some((scope) => !actual.has(scope))) {
    throw new GoogleConnectionError("Google did not grant all required permissions", "invalid_grant");
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

function explicitInstant(value: string): Date {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/.test(value)) {
    throw new Error("Calendar timestamp must include Z or a UTC offset");
  }
  return instant(value);
}

function calendarTarget(value: string | undefined): string {
  const calendarId = required(value ?? "primary", "Google Calendar ID").trim();
  if (calendarId.length > 1_024) throw new Error("Google Calendar ID exceeds 1024 characters");
  return calendarId;
}

function calendarBoundedCursor(
  value: GoogleCalendarBoundedCursor,
  expectedCalendarId: string,
): GoogleCalendarBoundedCursor {
  if (value.kind !== "calendar_updated_min_v1") {
    throw new Error("Unsupported Google Calendar cursor version");
  }
  const calendarId = calendarTarget(value.calendarId);
  if (calendarId !== expectedCalendarId) {
    throw new Error("Google Calendar cursor belongs to another calendar");
  }
  if (value.overlapMs !== CALENDAR_CURSOR_OVERLAP_MS) {
    throw new Error("Google Calendar cursor has an unsupported overlap");
  }
  const updatedMin = explicitInstant(value.updatedMin);
  const windowTimeMin = explicitInstant(value.windowTimeMin);
  const windowTimeMax = explicitInstant(value.windowTimeMax);
  if (
    windowTimeMax <= windowTimeMin ||
    windowTimeMax.getTime() - windowTimeMin.getTime() > 31 * 24 * 60 * 60_000
  ) {
    throw new Error("Google Calendar cursor has an invalid window");
  }
  return {
    kind: "calendar_updated_min_v1",
    calendarId,
    updatedMin: updatedMin.toISOString(),
    windowTimeMin: windowTimeMin.toISOString(),
    windowTimeMax: windowTimeMax.toISOString(),
    overlapMs: CALENDAR_CURSOR_OVERLAP_MS,
  };
}

function secondaryCalendarTarget(value: string): string {
  const calendarId = calendarTarget(value);
  if (calendarId === "primary") throw new Error("Family Calendar must be a secondary calendar");
  return calendarId;
}

function calendarTimeZone(value: string): string {
  const timeZone = required(value, "Family Calendar time zone").trim();
  try {
    new Intl.DateTimeFormat("en-US", { timeZone }).format(0);
  } catch {
    throw new Error("Family Calendar time zone is invalid");
  }
  return timeZone;
}

function calendarDate(value: string): string {
  const parsed = new Date(`${value}T00:00:00.000Z`);
  if (
    !/^\d{4}-\d{2}-\d{2}$/.test(value) ||
    !Number.isFinite(parsed.getTime()) ||
    parsed.toISOString().slice(0, 10) !== value
  ) {
    throw new Error("Google returned an invalid Calendar all-day date");
  }
  return value;
}

function zonedDateStart(value: string, timeZone: string): Date {
  calendarDate(value);
  const [year, month, day] = value.split("-").map(Number) as [number, number, number];
  const wanted = Date.UTC(year, month - 1, day);
  const formatter = new Intl.DateTimeFormat("en-US", {
    calendar: "iso8601",
    numberingSystem: "latn",
    timeZone: required(timeZone, "Calendar time zone"),
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  });
  let candidate = wanted;
  for (let pass = 0; pass < 4; pass += 1) {
    const parts = Object.fromEntries(
      formatter
        .formatToParts(new Date(candidate))
        .filter((part) => part.type !== "literal")
        .map((part) => [part.type, Number(part.value)]),
    );
    const observed = Date.UTC(
      parts.year ?? 0,
      (parts.month ?? 0) - 1,
      parts.day ?? 0,
      parts.hour ?? 0,
      parts.minute ?? 0,
      parts.second ?? 0,
    );
    const correction = wanted - observed;
    if (correction === 0) return new Date(candidate);
    candidate += correction;
  }
  throw new Error("Google Calendar all-day boundary is not representable in its time zone");
}

function required(value: string, label: string): string {
  if (!value.trim()) throw new Error(`${label} is required`);
  return value;
}

function boundedRequired(value: string, label: string, maximum: number): string {
  const result = required(value, label).trim();
  if (result.length > maximum) throw new Error(`${label} exceeds ${maximum} characters`);
  return result;
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

async function oauthErrorCode(response: Response): Promise<string | null> {
  const value: unknown = await response.json().catch(() => null);
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const error = (value as Record<string, unknown>).error;
  return typeof error === "string" ? error : null;
}

function stringField(value: Record<string, unknown>, field: string): string {
  const result = value[field];
  if (typeof result !== "string" || !result) throw providerError("Google returned an incomplete response");
  return result;
}

function optionalStringField(value: Record<string, unknown>, field: string): string | null {
  const result = value[field];
  if (result === undefined) return null;
  if (typeof result !== "string" || !result) throw providerError("Google returned an invalid response");
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

function gmailSearchBounds(
  afterValue: string | undefined,
  beforeValue: string | undefined,
): { after: Date; before: Date } | null {
  if (afterValue === undefined && beforeValue === undefined) return null;
  if (afterValue === undefined || beforeValue === undefined) {
    throw new Error("Gmail search bounds must include both after and before");
  }
  const after = explicitInstant(afterValue);
  const before = explicitInstant(beforeValue);
  if (before <= after) throw new Error("Gmail search before must follow after");
  if (before.getTime() - after.getTime() > 90 * 24 * 60 * 60_000) {
    throw new Error("Gmail search window cannot exceed 90 days");
  }
  return { after, before };
}

function gmailEvidence(message: Record<string, unknown>): GmailEvidence {
  const messageId = stringField(message, "id");
  const threadId = stringField(message, "threadId");
  const historyId = gmailHistoryId(message.historyId);
  const payload = recordField(message, "payload");
  const headers = headerMap(payload.headers);
  const from = bounded(required(headers.get("from") ?? "", "Gmail From header"), 500);
  const subject = nullableBounded(headers.get("subject"), 1_000);
  const timestamp = Number(stringField(message, "internalDate"));
  if (!Number.isFinite(timestamp)) throw providerError("Gmail returned an invalid message date");
  const supportedAttachments = collectGmailAttachmentReferences(payload, {
    messageId,
    threadId,
    historyId,
  });
  if (typeof message.snippet !== "string") throw providerError("Gmail returned an invalid snippet");
  const plainText = collectPlainText(payload).trim();
  const snippet = message.snippet.trim();
  const readableBody = plainText || snippet;
  if (!readableBody && supportedAttachments.attachments.length === 0) {
    throw providerError("Gmail message has no readable text or supported attachment");
  }
  const text =
    readableBody ||
    `Attachment-only message: ${supportedAttachments.attachments.map((attachment) => attachment.filename).join(", ")}`;
  return {
    messageId,
    threadId,
    historyId,
    from,
    subject,
    sentAt: new Date(timestamp).toISOString(),
    text: bounded(text, 50_000),
    textStatus: plainText
      ? plainText.length > 50_000
        ? "truncated"
        : "complete"
      : snippet
        ? "truncated"
        : "unavailable",
    attachments: supportedAttachments.attachments,
    attachmentsStatus: supportedAttachments.status,
  };
}

function gmailHistoryId(value: unknown): string {
  if (typeof value !== "string" || !/^[1-9][0-9]{0,29}$/.test(value)) {
    throw providerError("Gmail returned an invalid history cursor");
  }
  return value;
}

function gmailCursor(value: GoogleGmailCursor): GoogleGmailCursor {
  if (value.kind !== "gmail_history_v1") throw new Error("Unsupported Gmail cursor version");
  const historyId = gmailHistoryId(value.historyId);
  return {
    kind: "gmail_history_v1",
    historyId,
    capturedAt: explicitInstant(value.capturedAt).toISOString(),
  };
}

function gmailHistoryMessageIdentity(addition: Record<string, unknown>): {
  messageId: string;
  threadId: string;
} {
  const message = recordField(addition, "message");
  return {
    messageId: boundedRequired(stringField(message, "id"), "Gmail changed message ID", 500),
    threadId: boundedRequired(stringField(message, "threadId"), "Gmail changed thread ID", 500),
  };
}

function nonNegativeIntegerField(value: Record<string, unknown>, field: string): number {
  const result = value[field];
  if (!Number.isSafeInteger(result) || (result as number) < 0) {
    throw providerError("Google returned an invalid numeric field");
  }
  return result as number;
}

function collectGmailAttachmentReferences(
  payload: Record<string, unknown>,
  message: { messageId: string; threadId: string; historyId: string },
): { attachments: readonly GmailAttachmentReference[]; status: "complete" | "truncated" } {
  const found: GmailAttachmentReference[] = [];
  const visit = (part: Record<string, unknown>): void => {
    const rawMimeType = part.mimeType;
    const mimeType = typeof rawMimeType === "string" ? supportedGmailAttachmentMimeType(rawMimeType) : null;
    const rawFilename = part.filename;
    const filename = typeof rawFilename === "string" ? rawFilename.trim() : "";
    if (mimeType && filename) {
      const body = recordField(part, "body");
      const sizeBytes = nonNegativeIntegerField(body, "size");
      const externalAttachmentId = optionalStringField(body, "attachmentId");
      const inlineData = body.data;
      const partId = stringField(part, "partId");
      if (
        sizeBytes > 0 &&
        sizeBytes <= gmailAttachmentLimit(mimeType) &&
        (externalAttachmentId || (typeof inlineData === "string" && inlineData))
      ) {
        found.push(
          validateGmailAttachmentReference({
            ...message,
            partId,
            attachmentId: externalAttachmentId ?? `inline:${partId}`,
            storage: externalAttachmentId ? "external" : "inline",
            filename,
            mimeType,
            sizeBytes,
          }),
        );
      }
    }
    for (const child of recordArray(part.parts)) visit(child);
  };
  visit(payload);
  return {
    attachments: found.slice(0, MAX_GMAIL_ATTACHMENTS_PER_MESSAGE),
    status: found.length > MAX_GMAIL_ATTACHMENTS_PER_MESSAGE ? "truncated" : "complete",
  };
}

function findGmailPart(
  part: Record<string, unknown>,
  expectedPartId: string,
): Record<string, unknown> | null {
  if (part.partId === expectedPartId) return part;
  for (const child of recordArray(part.parts)) {
    const found = findGmailPart(child, expectedPartId);
    if (found) return found;
  }
  return null;
}

function supportedGmailAttachmentMimeType(value: string): GoogleSupportedGmailAttachmentMimeType | null {
  const normalized = value.trim().toLowerCase();
  return normalized === "application/pdf" ||
    normalized === "image/jpeg" ||
    normalized === "image/png" ||
    normalized === "image/webp"
    ? normalized
    : null;
}

function validateGmailAttachmentReference(value: GmailAttachmentReference): GmailAttachmentReference {
  const mimeType = supportedGmailAttachmentMimeType(value.mimeType);
  if (!mimeType) throw new Error("Unsupported Gmail attachment type");
  const filename = value.filename.trim();
  if (
    filename.length < 1 ||
    filename.length > 500 ||
    [...filename].some((character) => {
      const code = character.charCodeAt(0);
      return code < 32 || code === 127;
    })
  ) {
    throw providerError("Gmail returned an invalid attachment filename");
  }
  const result = {
    messageId: boundedRequired(value.messageId, "Gmail message ID", 500),
    threadId: boundedRequired(value.threadId, "Gmail thread ID", 500),
    historyId: gmailHistoryId(value.historyId),
    partId: boundedRequired(value.partId, "Gmail attachment part ID", 500),
    attachmentId: boundedRequired(value.attachmentId, "Gmail attachment ID", 500),
    storage: value.storage,
    filename,
    mimeType,
    sizeBytes: value.sizeBytes,
  };
  if (result.storage !== "external" && result.storage !== "inline") {
    throw providerError("Gmail returned an invalid attachment storage type");
  }
  if (result.storage === "inline" && result.attachmentId !== `inline:${result.partId}`) {
    throw providerError("Gmail returned an invalid inline attachment identity");
  }
  if (
    !Number.isSafeInteger(result.sizeBytes) ||
    result.sizeBytes < 1 ||
    result.sizeBytes > gmailAttachmentLimit(result.mimeType)
  ) {
    throw providerError("Gmail attachment exceeds Florence's artifact limit");
  }
  return result;
}

function sameGmailAttachment(left: GmailAttachmentReference, right: GmailAttachmentReference): boolean {
  return (
    left.messageId === right.messageId &&
    left.threadId === right.threadId &&
    left.historyId === right.historyId &&
    left.partId === right.partId &&
    left.attachmentId === right.attachmentId &&
    left.storage === right.storage &&
    left.filename === right.filename &&
    left.mimeType === right.mimeType &&
    left.sizeBytes === right.sizeBytes
  );
}

function gmailAttachmentLimit(mimeType: GoogleSupportedGmailAttachmentMimeType): number {
  return mimeType === "application/pdf" ? MAX_GMAIL_PDF_BYTES : MAX_GMAIL_IMAGE_BYTES;
}

function strictBase64UrlDecode(value: string, maximumBytes: number): Uint8Array {
  const unpadded = value.replace(/=+$/, "");
  const paddingLength = value.length - unpadded.length;
  if (
    !unpadded ||
    !/^[A-Za-z0-9_-]+$/.test(unpadded) ||
    paddingLength > 2 ||
    unpadded.length % 4 === 1 ||
    unpadded.length > Math.ceil((maximumBytes * 4) / 3) + 4
  ) {
    throw providerError("Gmail returned invalid attachment encoding");
  }
  const requiredPadding = (4 - (unpadded.length % 4)) % 4;
  if (paddingLength !== 0 && paddingLength !== requiredPadding) {
    throw providerError("Gmail returned non-canonical attachment encoding");
  }
  const bytes = Buffer.from(unpadded, "base64url");
  if (bytes.toString("base64url") !== unpadded || bytes.byteLength > maximumBytes) {
    throw providerError("Gmail returned invalid attachment encoding");
  }
  return bytes;
}

function gmailBytesMatchMimeType(
  bytes: Uint8Array,
  mimeType: GoogleSupportedGmailAttachmentMimeType,
): boolean {
  const value = Buffer.from(bytes);
  if (mimeType === "application/pdf") {
    if (value.length < 12 || value.toString("ascii", 0, 5) !== "%PDF-") return false;
    if (!/^(?:1\.[0-9]|2\.0)$/.test(value.toString("ascii", 5, 8))) return false;
    return value.subarray(Math.max(0, value.length - 2_048)).includes(Buffer.from("%%EOF"));
  }
  if (mimeType === "image/jpeg") {
    return value.length >= 3 && value[0] === 0xff && value[1] === 0xd8 && value[2] === 0xff;
  }
  if (mimeType === "image/png") {
    return (
      value.length >= 8 &&
      value.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))
    );
  }
  return (
    value.length >= 12 &&
    value.toString("ascii", 0, 4) === "RIFF" &&
    value.toString("ascii", 8, 12) === "WEBP"
  );
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
