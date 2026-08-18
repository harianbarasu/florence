import { createCipheriv, createDecipheriv, createHash, randomBytes, randomUUID } from "node:crypto";

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

export type CalendarEventDraft = {
  title: string;
  startsAt: string;
  endsAt: string;
  timeZone: string;
  location: string | null;
};

export type ApprovedCalendarAction = {
  actionId: string;
  householdId: string;
  connectionId: string;
  ownerAdultId: string;
  calendarId?: string;
  approvalMessageId: string;
  approvalDigest: string;
  proposalDigest: string;
  event: CalendarEventDraft;
};

export type GoogleCalendarExecutionResult =
  | { status: "committed"; providerReceiptId: string; detail: string; occurredAt: string }
  | { status: "failed"; providerReceiptId: null; detail: string; occurredAt: string };

export type GoogleCalendarWindowEvent = {
  title: string | null;
  startsAt: string;
  endsAt: string;
  allDay: boolean;
};

export type GoogleCalendarWindowRead = {
  status: "complete" | "truncated" | "unavailable";
  events: readonly GoogleCalendarWindowEvent[];
};

export type GoogleFamilyCalendarProvisioningResult = {
  status: "committed";
  calendarId: string;
  summary: string;
  timeZone: string;
  founderConnectionId: string;
  founderAccessRole: "owner";
  partnerConnectionId: string;
  partnerEmailLabel: string;
  partnerAccessRole: "owner";
  partnerCalendarListSelected: true;
  providerReceiptId: string;
  detail: string;
  occurredAt: string;
};

export type GmailEvidence = {
  messageId: string;
  threadId: string;
  historyId: string;
  from: string;
  subject: string | null;
  sentAt: string;
  text: string;
};

type FamilyCalendarListEntry = {
  id: string;
  etag: string;
  summary: string;
  timeZone: string;
  accessRole: string;
  selected: boolean;
  primary: boolean;
};

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
    now: string;
  }): Promise<{ view: GoogleConnectionView; refreshTokenEnvelope: string | null } | null>;
  readActiveGoogleCredential(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
  }): Promise<ActiveGoogleCredential | null>;
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
    readonly code: "provider_rejected" | "indeterminate_create",
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

  async searchGmail(input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    query: string;
    limit?: number;
  }): Promise<readonly GmailEvidence[]> {
    const queryText = required(input.query, "Gmail search query").trim();
    if (queryText.length > 500) throw new Error("Gmail search query exceeds 500 characters");
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
    const query = new URLSearchParams({ maxResults: String(limit), q: queryText });
    const list = await this.#gmailJson(`messages?${query}`, accessToken);
    const identities = await Promise.all(
      recordArray(list.messages)
        .slice(0, limit)
        .map((message) => this.#messageIdentity(accessToken, message)),
    );
    return Promise.all(identities.map((identity) => this.#readGmailMessage(accessToken, identity)));
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

    const calendarId =
      resumedCalendarId ??
      (await this.#createFamilyCalendar(founderAccessToken, {
        summary,
        timeZone,
      }));
    try {
      const calendar = await this.#readFamilyCalendar(founderAccessToken, calendarId, summary, timeZone);
      const founderAcl = await this.#readOwnerAcl(founderAccessToken, calendarId, founderEmail);
      if (!founderAcl) {
        throw new GoogleFamilyCalendarProvisioningError(
          "Google did not verify the founder as a Family Calendar owner",
          "provider_rejected",
        );
      }
      const partnerAcl = await this.#ensureOwnerAcl(founderAccessToken, calendarId, partnerEmail);
      const partnerList = await this.#ensurePartnerCalendarList(
        partnerAccessToken,
        calendarId,
        summary,
        timeZone,
      );
      const canonical = {
        provider: "google-calendar",
        calendarId,
        calendarEtag: calendar.etag,
        summary,
        timeZone,
        founderConnectionId,
        founderAclId: founderAcl.id,
        founderAclEtag: founderAcl.etag,
        partnerConnectionId,
        partnerAclId: partnerAcl.id,
        partnerAclEtag: partnerAcl.etag,
        partnerCalendarListEtag: partnerList.etag,
        partnerAccessRole: "owner",
        partnerCalendarListSelected: true,
      } as const;
      const canonicalProof = JSON.stringify(canonical);
      const detail = JSON.stringify({
        ...canonical,
        digest: digest(canonicalProof),
      });
      if (detail.length > 2_000) {
        throw new GoogleFamilyCalendarProvisioningError(
          "Google Family Calendar proof exceeded the safe receipt limit",
          "provider_rejected",
        );
      }
      return {
        status: "committed",
        calendarId,
        summary,
        timeZone,
        founderConnectionId,
        founderAccessRole: "owner",
        partnerConnectionId,
        partnerEmailLabel: partnerEmail,
        partnerAccessRole: "owner",
        partnerCalendarListSelected: true,
        providerReceiptId: calendarId,
        detail,
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

  async readCalendarWindow(input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    calendarId?: string;
    timeMin: string;
    timeMax: string;
    limit?: number;
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
    const credential = await this.#store.readActiveGoogleCredential({
      householdId: input.householdId,
      ownerAdultId: input.ownerAdultId,
      connectionId: input.connectionId,
    });
    if (!credential) return { status: "unavailable", events: [] };

    try {
      const accessToken = await this.#calendarAccessToken(credential);
      const query = new URLSearchParams({
        fields:
          "nextPageToken,timeZone,items(status,summary,start,end,transparency,attendees(self,responseStatus))",
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
      if (!response.ok) return { status: "unavailable", events: [] };
      const body = await safeJson(response);
      const calendarTimeZone = stringField(body, "timeZone");
      const nextPageToken = body.nextPageToken;
      if (nextPageToken !== undefined && (typeof nextPageToken !== "string" || !nextPageToken)) {
        return { status: "unavailable", events: [] };
      }
      const events = recordArray(body.items).flatMap((event) => {
        const busy = calendarWindowEvent(event, calendarTimeZone, timeMin, timeMax);
        return busy ? [busy] : [];
      });
      return {
        status: nextPageToken ? "truncated" : "complete",
        events,
      };
    } catch (error) {
      if (error instanceof GoogleCalendarTransientError) throw error;
      return { status: "unavailable", events: [] };
    }
  }

  async executeCalendar(action: ApprovedCalendarAction): Promise<GoogleCalendarExecutionResult> {
    const failed = (detail: string): GoogleCalendarExecutionResult => ({
      status: "failed",
      providerReceiptId: null,
      detail: bounded(detail, 500),
      occurredAt: new Date().toISOString(),
    });
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
    if (!credential) return failed("The approved Google connection is no longer active");

    let accessToken: string;
    try {
      accessToken = await this.#calendarAccessToken(credential);
    } catch (error) {
      if (error instanceof DefinitiveCalendarError) return failed(error.message);
      throw error;
    }

    const eventId = googleCalendarEventId(action.actionId);
    const eventBody = {
      id: eventId,
      summary: action.event.title,
      start: { dateTime: action.event.startsAt, timeZone: action.event.timeZone },
      end: { dateTime: action.event.endsAt, timeZone: action.event.timeZone },
      ...(action.event.location === null ? {} : { location: action.event.location }),
      extendedProperties: {
        private: {
          florenceActionId: action.actionId,
          florenceApprovalMessageId: action.approvalMessageId,
          florenceApprovalDigest: action.approvalDigest,
          florenceProposalDigest: action.proposalDigest,
        },
      },
    };
    const calendarId = calendarTarget(action.calendarId);
    const calendarUrl = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}`;
    const collectionUrl = `${calendarUrl}/events?sendUpdates=none`;
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
      reread = await this.#fetch(`${calendarUrl}/events/${encodeURIComponent(eventId)}`, {
        headers: { authorization: `Bearer ${accessToken}` },
        signal: AbortSignal.timeout(15_000),
      });
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
    const proof = calendarEventProof(event, eventId, calendarId, action);
    if (!proof) return failed("Google Calendar did not preserve the exact approved event");
    const detail = JSON.stringify({
      provider: "google-calendar",
      calendarId,
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

  async #createFamilyCalendar(
    accessToken: string,
    input: { summary: string; timeZone: string },
  ): Promise<string> {
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
      throw new GoogleFamilyCalendarProvisioningError(
        "Google did not confirm whether the Family Calendar was created",
        "indeterminate_create",
      );
    }
    if (response.status === 408 || response.status >= 500) {
      throw new GoogleFamilyCalendarProvisioningError(
        `Google did not confirm whether the Family Calendar was created (HTTP ${response.status})`,
        "indeterminate_create",
      );
    }
    if (transientHttpStatus(response.status)) {
      throw transientCalendarError(
        `Google Family Calendar creation was temporarily rejected with HTTP ${response.status}`,
      );
    }
    if (!response.ok) {
      throw new GoogleFamilyCalendarProvisioningError(
        `Google rejected Family Calendar creation with HTTP ${response.status}`,
        "provider_rejected",
      );
    }
    try {
      return secondaryCalendarTarget(stringField(await safeJson(response), "id"));
    } catch {
      // A successful create with an unreadable response may have committed, but its ID is unrecoverable here.
      throw new GoogleFamilyCalendarProvisioningError(
        "Google created an unidentifiable Family Calendar",
        "indeterminate_create",
      );
    }
  }

  async #readFamilyCalendar(
    accessToken: string,
    calendarId: string,
    summary: string,
    timeZone: string,
  ): Promise<{ etag: string }> {
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
      throw transientCalendarError("Google Family Calendar proof read failed", error);
    }
    if (transientHttpStatus(response.status)) {
      throw transientCalendarError(`Google Family Calendar proof read returned HTTP ${response.status}`);
    }
    if (!response.ok) {
      throw new GoogleFamilyCalendarProvisioningError(
        `Google rejected the Family Calendar proof read with HTTP ${response.status}`,
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
        stringField(calendar, "timeZone") !== timeZone
      ) {
        throw invalidFamilyCalendarProvider("Google did not preserve the exact Family Calendar");
      }
      return { etag: stringField(calendar, "etag") };
    } catch (error) {
      if (error instanceof GoogleFamilyCalendarProvisioningError) throw error;
      throw invalidFamilyCalendarProvider("Google returned an incomplete Family Calendar");
    }
  }

  async #readOwnerAcl(
    accessToken: string,
    calendarId: string,
    email: string,
  ): Promise<{ id: string; etag: string } | null> {
    for (const rule of await this.#readCalendarAcls(accessToken, calendarId)) {
      const match = calendarAclForEmail(rule, email);
      if (match?.role === "owner") return { id: match.id, etag: match.etag };
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
        fields: "nextPageToken,items(deleted,etag,id,role,scope(type,value))",
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
        throw transientCalendarError("Google Family Calendar owner proof read failed", error);
      }
      if (transientHttpStatus(response.status)) {
        throw transientCalendarError(
          `Google Family Calendar owner proof read returned HTTP ${response.status}`,
        );
      }
      if (!response.ok) {
        throw new GoogleFamilyCalendarProvisioningError(
          `Google rejected the Family Calendar owner proof read with HTTP ${response.status}`,
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

  async #ensureOwnerAcl(
    accessToken: string,
    calendarId: string,
    email: string,
  ): Promise<{ id: string; etag: string }> {
    const existingRules = await this.#readCalendarAcls(accessToken, calendarId);
    const existing = existingRules
      .map((rule) => calendarAclForEmail(rule, email))
      .find((rule) => rule !== null);
    if (existing?.role === "owner") return { id: existing.id, etag: existing.etag };

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
          const proof = calendarAclForEmail(await safeJson(response), email);
          acceptedResponseValid = proof?.role === "owner";
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
      // A timed-out ACL write may have committed; the owner-list proof below resolves it.
    }

    const proof = await this.#readOwnerAcl(accessToken, calendarId, email);
    if (proof) return proof;
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
  ): Promise<{ etag: string }> {
    const existing = await this.#readCalendarListEntry(accessToken, calendarId);
    const existingProof = familyCalendarListProof(existing, calendarId, summary, timeZone);
    if (existingProof) return existingProof;
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
            familyCalendarListProof(
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
      // A timed-out CalendarList write may have committed; the proof read below resolves it.
    }

    const proof = familyCalendarListProof(
      await this.#readCalendarListEntry(accessToken, calendarId),
      calendarId,
      summary,
      timeZone,
    );
    if (proof) return proof;
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
      fields: "accessRole,etag,id,primary,selected,summary,timeZone",
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
      throw transientCalendarError("Google partner Calendar list proof read failed", error);
    }
    if (response.status === 404) return null;
    if (transientHttpStatus(response.status)) {
      throw transientCalendarError(
        `Google partner Calendar list proof read returned HTTP ${response.status}`,
      );
    }
    if (!response.ok) {
      throw new GoogleFamilyCalendarProvisioningError(
        `Google rejected the partner Calendar list proof read with HTTP ${response.status}`,
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
    if (!response.ok) throw providerError("Google access-token refresh failed");
    const token = await safeJson(response);
    if (token.token_type !== "Bearer") throw providerError("Google returned an unsupported token type");
    return stringField(token, "access_token");
  }

  async #readGmailMessage(
    accessToken: string,
    expected: { messageId: string; threadId: string; historyId: string },
  ): Promise<GmailEvidence> {
    const response = await this.#fetch(
      `https://gmail.googleapis.com/gmail/v1/users/me/messages/${encodeURIComponent(expected.messageId)}?format=full`,
      {
        headers: { authorization: `Bearer ${accessToken}` },
        signal: AbortSignal.timeout(15_000),
      },
    );
    if (!response.ok) throw providerError("Gmail message could not be read");
    const message = await safeJson(response);
    const messageId = stringField(message, "id");
    const threadId = stringField(message, "threadId");
    const historyId = stringField(message, "historyId");
    if (
      messageId !== expected.messageId ||
      threadId !== expected.threadId ||
      historyId !== expected.historyId
    ) {
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
): { id: string; etag: string; role: string } | null {
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
  if (
    typeof rule.id !== "string" ||
    !rule.id ||
    typeof rule.etag !== "string" ||
    !rule.etag ||
    typeof rule.role !== "string" ||
    !rule.role
  ) {
    throw invalidFamilyCalendarProvider("Google returned an incomplete Family Calendar owner rule");
  }
  return { id: rule.id, etag: rule.etag, role: rule.role };
}

function calendarListEntry(
  value: Record<string, unknown>,
  expectedCalendarId: string,
): FamilyCalendarListEntry {
  if (value.id !== expectedCalendarId) {
    throw invalidFamilyCalendarProvider("Google returned a different partner Calendar list entry");
  }
  if (
    typeof value.etag !== "string" ||
    !value.etag ||
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
    etag: value.etag,
    summary: value.summary,
    timeZone: value.timeZone,
    accessRole: value.accessRole,
    selected: value.selected === true,
    primary: value.primary === true,
  };
}

function familyCalendarListProof(
  entry: FamilyCalendarListEntry | null,
  calendarId: string,
  summary: string,
  timeZone: string,
): { etag: string } | null {
  if (
    !entry ||
    entry.id !== calendarId ||
    entry.summary !== summary ||
    entry.timeZone !== timeZone ||
    entry.accessRole !== "owner" ||
    !entry.selected ||
    entry.primary
  ) {
    return null;
  }
  return { etag: entry.etag };
}

function invalidFamilyCalendarProvider(message: string): GoogleFamilyCalendarProvisioningError {
  return new GoogleFamilyCalendarProvisioningError(message, "provider_rejected");
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
    [action.approvalMessageId, "approval message ID"],
    [action.event.title, "Calendar title"],
    [action.event.timeZone, "Calendar time zone"],
  ] as const) {
    required(value, label);
  }
  calendarTarget(action.calendarId);
  assertDigest(action.approvalDigest, "Calendar approval");
  assertDigest(action.proposalDigest, "Calendar proposal");
  const startsAt = explicitInstant(action.event.startsAt);
  const endsAt = explicitInstant(action.event.endsAt);
  if (endsAt <= startsAt) throw new Error("Calendar end must follow start");
  if (action.event.location !== null) required(action.event.location, "Calendar location");
}

function calendarWindowEvent(
  event: Record<string, unknown>,
  calendarTimeZone: string,
  timeMin: Date,
  timeMax: Date,
): GoogleCalendarWindowEvent | null {
  if (event.status === "cancelled" || event.transparency === "transparent") return null;
  if (
    recordArray(event.attendees).some(
      (attendee) => attendee.self === true && attendee.responseStatus === "declined",
    )
  ) {
    return null;
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
  const startInstant = allDay
    ? zonedDateStart(required(startDate ?? "", "Calendar all-day start"), calendarTimeZone)
    : explicitInstant(startDateTime ?? "");
  const endInstant = allDay
    ? zonedDateStart(required(endDate ?? "", "Calendar all-day end"), calendarTimeZone)
    : explicitInstant(endDateTime ?? "");
  if (endInstant <= startInstant) {
    throw providerError("Google returned an invalid Calendar event interval");
  }
  if (endInstant <= timeMin || startInstant >= timeMax) return null;
  const summary = optionalStringField(event, "summary");
  return {
    title: summary === null ? null : bounded(summary, 500),
    startsAt: startInstant.toISOString(),
    endsAt: endInstant.toISOString(),
    allDay,
  };
}

function calendarEventProof(
  event: Record<string, unknown>,
  expectedId: string,
  calendarId: string,
  action: ApprovedCalendarAction,
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
      event.summary !== action.event.title ||
      !sameInstant(startsAt, action.event.startsAt) ||
      !sameInstant(endsAt, action.event.endsAt) ||
      start.timeZone !== action.event.timeZone ||
      end.timeZone !== action.event.timeZone ||
      location !== action.event.location ||
      privateProperties.florenceActionId !== action.actionId ||
      privateProperties.florenceApprovalMessageId !== action.approvalMessageId ||
      privateProperties.florenceApprovalDigest !== action.approvalDigest ||
      privateProperties.florenceProposalDigest !== action.proposalDigest
    ) {
      return null;
    }
    const canonicalProof = JSON.stringify({
      provider: "google-calendar",
      calendarId,
      eventId: expectedId,
      etag,
      status: "confirmed",
      title: action.event.title,
      startsAt: new Date(startsAt).toISOString(),
      endsAt: new Date(endsAt).toISOString(),
      timeZone: action.event.timeZone,
      location: action.event.location,
      actionId: action.actionId,
      approvalMessageId: action.approvalMessageId,
      approvalDigest: action.approvalDigest,
      proposalDigest: action.proposalDigest,
    });
    return { etag, digest: digest(canonicalProof) };
  } catch {
    return null;
  }
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

function zonedDateStart(value: string, timeZone: string): Date {
  if (
    !/^\d{4}-\d{2}-\d{2}$/.test(value) ||
    new Date(`${value}T00:00:00.000Z`).toISOString().slice(0, 10) !== value
  ) {
    throw new Error("Google returned an invalid Calendar all-day date");
  }
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
