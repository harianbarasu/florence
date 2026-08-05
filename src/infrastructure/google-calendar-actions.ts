import { Temporal } from "@js-temporal/polyfill";
import type { GoogleCalendarAdapter, GoogleOAuthAdapter, GoogleTokenSet } from "../adapters/google/index.js";
import {
  GOOGLE_CALENDAR_EVENTS_SCOPE,
  GoogleAdapterError,
  googleTokenSetSchema,
} from "../adapters/google/index.js";
import type { CalendarCreatePreparation, HouseholdCalendarActionsPort } from "../application/index.js";
import { type CalendarEventCreateAction, CalendarEventCreateActionSchema } from "../domain/index.js";
import { canonicalJson, sha256 } from "../security/canonical-json.js";
import type { SecretBox } from "../security/secret-box.js";
import {
  type CalendarApplicationContent,
  calendarApplicationContent,
  calendarApplicationContentDigest,
  calendarSourceContentAad,
  normalizeCalendarBusyWindow,
  type PersistPersonalCalendarSourceInput,
  type PersistPersonalCalendarSourceResult,
} from "./google-calendar-sync.js";
import {
  type GoogleSyncConnection,
  googleConnectionCredentialsAad,
  type ScopedMutationResult,
} from "./google-sync.js";

export type GoogleCalendarActionErrorCode =
  | "approval_invalidated"
  | "invalid_state"
  | "not_authorized"
  | "projection_incomplete"
  | "provider_failure";

/** Stable execution errors; provider and private projection details never cross the effect seam. */
export class GoogleCalendarActionError extends Error {
  override readonly name = "GoogleCalendarActionError";

  public constructor(
    readonly code: GoogleCalendarActionErrorCode,
    readonly retryable: boolean,
  ) {
    super(code);
  }
}

export interface GoogleCalendarActionStore {
  prepareCreate(
    input: Parameters<HouseholdCalendarActionsPort["prepareCreate"]>[0],
  ): Promise<CalendarCreatePreparation>;
  getOwnedGoogleConnection(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
  }): Promise<GoogleSyncConnection | null>;
  replaceEncryptedCredentials(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    expectedCiphertext: string;
    encryptedCredentials: string;
    grantedScopes: readonly string[];
  }): Promise<ScopedMutationResult>;
  markConnectionStatus(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    status: "reauth_required" | "error";
  }): Promise<"updated" | "not_found">;
  persistPersonalCalendarSource(
    input: PersistPersonalCalendarSourceInput,
  ): Promise<PersistPersonalCalendarSourceResult>;
}

export interface GoogleCalendarCreateProvider {
  insertEvent(
    input: Parameters<GoogleCalendarAdapter["insertEvent"]>[0],
  ): ReturnType<GoogleCalendarAdapter["insertEvent"]>;
}

export interface GoogleCalendarCredentialProvider {
  refresh(tokens: GoogleTokenSet): ReturnType<GoogleOAuthAdapter["refresh"]>;
}

export interface GoogleCalendarActionsOptions {
  store: GoogleCalendarActionStore;
  calendar: GoogleCalendarCreateProvider;
  oauth: GoogleCalendarCredentialProvider;
  secretBox: Pick<SecretBox, "open" | "seal">;
  now?: () => Date;
  refreshSkewMs?: number;
}

/**
 * The sole production Calendar-write capability. It revalidates the approval's
 * private-data digest, refreshes only the selected adult's credentials, inserts
 * idempotently, and persists the provider result back into the personal source.
 */
export class GoogleCalendarActions implements HouseholdCalendarActionsPort {
  readonly #store: GoogleCalendarActionStore;
  readonly #calendar: GoogleCalendarCreateProvider;
  readonly #oauth: GoogleCalendarCredentialProvider;
  readonly #secretBox: Pick<SecretBox, "open" | "seal">;
  readonly #now: () => Date;
  readonly #refreshSkewMs: number;

  public constructor(options: GoogleCalendarActionsOptions) {
    this.#store = options.store;
    this.#calendar = options.calendar;
    this.#oauth = options.oauth;
    this.#secretBox = options.secretBox;
    this.#now = options.now ?? (() => new Date());
    this.#refreshSkewMs = options.refreshSkewMs ?? 5 * 60_000;
  }

  public prepareCreate(
    input: Parameters<HouseholdCalendarActionsPort["prepareCreate"]>[0],
  ): Promise<CalendarCreatePreparation> {
    return this.#store.prepareCreate(input);
  }

  public async createApprovedEvent(input: {
    action: CalendarEventCreateAction;
    idempotencyKey: string;
    asOf: string;
  }): Promise<{ provider: "google-calendar"; providerReference: string }> {
    const action = CalendarEventCreateActionSchema.parse(input.action);
    let preflight: CalendarCreatePreparation;
    try {
      preflight = await this.#store.prepareCreate({
        householdId: action.householdId,
        verifiedAdultIds: action.availabilityAdultIds,
        requestedByAdultId: action.requestedByAdultId,
        asOf: input.asOf,
        startsAt: action.startsAt,
        endsAt: action.endsAt,
        targetConnectionId: action.targetConnectionId,
      });
    } catch {
      throw new GoogleCalendarActionError("invalid_state", true);
    }
    return this.#executeAfterPreflight(action, input.idempotencyKey, input.asOf, preflight);
  }

  async #executeAfterPreflight(
    action: CalendarEventCreateAction,
    idempotencyKey: string,
    asOf: string,
    preflight: CalendarCreatePreparation,
  ): Promise<{ provider: "google-calendar"; providerReference: string }> {
    if (preflight.status === "unavailable") {
      throw new GoogleCalendarActionError(
        preflight.reason === "projection_incomplete" ? "projection_incomplete" : "not_authorized",
        preflight.reason === "projection_incomplete",
      );
    }
    if (
      preflight.targetConnectionId !== action.targetConnectionId ||
      preflight.calendarId !== action.calendarId ||
      preflight.relevantDataDigest !== action.relevantDataDigest ||
      preflight.hasConflict !== action.hasConflict
    ) {
      throw new GoogleCalendarActionError("approval_invalidated", false);
    }
    let connection: GoogleSyncConnection | null;
    try {
      connection = await this.#store.getOwnedGoogleConnection({
        householdId: action.householdId,
        adultId: action.requestedByAdultId,
        connectionId: action.targetConnectionId,
      });
    } catch {
      throw new GoogleCalendarActionError("invalid_state", true);
    }
    if (
      connection === null ||
      connection.status !== "active" ||
      !connection.grantedScopes.includes(GOOGLE_CALENDAR_EVENTS_SCOPE)
    ) {
      throw new GoogleCalendarActionError("not_authorized", false);
    }
    try {
      const event = await this.#withAccessToken(connection, async (accessToken) =>
        this.#calendar.insertEvent({
          accessToken,
          googleSubject: connection.externalAccountId,
          calendarId: action.calendarId,
          idempotencyKey,
          sendUpdates: "none",
          summary: action.title,
          start: { dateTime: action.startsAt, timeZone: action.timeZone },
          end: { dateTime: action.endsAt, timeZone: action.timeZone },
          visibility: "default",
        }),
      );
      const applicationContent = approvedCalendarApplicationContent(event, action);
      if (
        event.googleSubject !== connection.externalAccountId ||
        event.calendarId !== action.calendarId ||
        applicationContent === null
      ) {
        throw new GoogleCalendarActionError("invalid_state", false);
      }
      const serialized = canonicalJson(event);
      const applicationContentDigest = calendarApplicationContentDigest(applicationContent);
      await this.#store.persistPersonalCalendarSource({
        householdId: connection.householdId,
        adultId: connection.adultId,
        connectionId: connection.id,
        calendarId: event.calendarId,
        externalId: event.eventId,
        kind: "calendar_event",
        occurredAt: event.updatedAt ?? event.createdAt ?? asOf,
        contentHash: `sha256:${sha256(serialized)}`,
        encryptedContent: this.#secretBox.seal(
          serialized,
          calendarSourceContentAad(connection, event.calendarId, event.eventId),
        ),
        metadata: {
          schemaVersion: 1,
          provider: "google-calendar",
          sourceScope: "personal",
          calendarId: event.calendarId,
          googleSubject: connection.externalAccountId,
          changeKey: event.changeKey,
          etag: event.etag,
          deleted: false,
          contentAadVersion: 1,
          createdByApprovedActionId: action.actionId,
          applicationContentDigest,
        },
        busyWindow: normalizeCalendarBusyWindow(event, action.timeZone),
      });
      return {
        provider: "google-calendar",
        providerReference: `google-calendar:${event.calendarId}:${event.eventId}`,
      };
    } catch (error) {
      if (error instanceof GoogleCalendarActionError) throw error;
      if (error instanceof GoogleAdapterError) {
        throw new GoogleCalendarActionError("provider_failure", error.retryable);
      }
      // A provider insertion may already have succeeded before a downstream
      // persistence error. Retry with the same provider idempotency key.
      throw new GoogleCalendarActionError("invalid_state", true);
    }
  }

  async #withAccessToken<T>(
    connection: GoogleSyncConnection,
    operation: (accessToken: string) => Promise<T>,
  ): Promise<T> {
    let tokens = this.#decryptCredentials(connection);
    this.#assertWriteScope(connection, tokens);
    if (tokenNeedsRefresh(tokens, this.#now(), this.#refreshSkewMs)) {
      tokens = await this.#refreshCredentials(connection, tokens);
    }
    try {
      return await operation(tokens.accessToken);
    } catch (error) {
      if (!(error instanceof GoogleAdapterError) || error.code !== "unauthorized") throw error;
      tokens = await this.#refreshCredentials(connection, tokens);
      return operation(tokens.accessToken);
    }
  }

  #decryptCredentials(connection: GoogleSyncConnection): GoogleTokenSet {
    if (connection.encryptedCredentials === null) {
      throw new GoogleCalendarActionError("not_authorized", false);
    }
    try {
      return googleTokenSetSchema.parse(
        JSON.parse(
          this.#secretBox.open(connection.encryptedCredentials, googleConnectionCredentialsAad(connection)),
        ),
      );
    } catch {
      throw new GoogleCalendarActionError("invalid_state", false);
    }
  }

  #assertWriteScope(connection: GoogleSyncConnection, tokens: GoogleTokenSet): void {
    if (
      !connection.grantedScopes.includes(GOOGLE_CALENDAR_EVENTS_SCOPE) ||
      !tokens.scope.includes(GOOGLE_CALENDAR_EVENTS_SCOPE)
    ) {
      throw new GoogleCalendarActionError("not_authorized", false);
    }
  }

  async #refreshCredentials(
    connection: GoogleSyncConnection,
    current: GoogleTokenSet,
  ): Promise<GoogleTokenSet> {
    let refreshed: GoogleTokenSet;
    try {
      refreshed = await this.#oauth.refresh(current);
    } catch (error) {
      if (error instanceof GoogleAdapterError && error.code === "unauthorized") {
        await this.#store.markConnectionStatus({
          householdId: connection.householdId,
          adultId: connection.adultId,
          connectionId: connection.id,
          status: "reauth_required",
        });
        throw new GoogleCalendarActionError("not_authorized", false);
      }
      throw error;
    }
    this.#assertWriteScope(connection, refreshed);
    const encryptedCredentials = this.#secretBox.seal(
      JSON.stringify(refreshed),
      googleConnectionCredentialsAad(connection),
    );
    const replaced = await this.#store.replaceEncryptedCredentials({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      expectedCiphertext: connection.encryptedCredentials ?? "",
      encryptedCredentials,
      grantedScopes: refreshed.scope,
    });
    if (replaced !== "updated") {
      throw new GoogleCalendarActionError(
        replaced === "conflict" ? "invalid_state" : "not_authorized",
        replaced === "conflict",
      );
    }
    connection.encryptedCredentials = encryptedCredentials;
    connection.grantedScopes = [...refreshed.scope];
    return refreshed;
  }
}

function tokenNeedsRefresh(tokens: GoogleTokenSet, now: Date, skewMs: number): boolean {
  if (tokens.expiresAt === null) return false;
  const expiry = Date.parse(tokens.expiresAt);
  return !Number.isFinite(expiry) || expiry <= now.getTime() + skewMs;
}

function approvedCalendarApplicationContent(
  event: Awaited<ReturnType<GoogleCalendarCreateProvider["insertEvent"]>>,
  action: CalendarEventCreateAction,
): CalendarApplicationContent | null {
  if (
    event.deleted ||
    event.summary !== action.title ||
    event.start?.kind !== "dateTime" ||
    event.end?.kind !== "dateTime" ||
    event.start.timeZone !== action.timeZone ||
    event.end.timeZone !== action.timeZone
  ) {
    return null;
  }
  const content = calendarApplicationContent(event, action.timeZone);
  if (
    content === null ||
    content.description !== null ||
    content.location !== null ||
    content.allDay ||
    content.status !== "confirmed" ||
    content.recurrence.length !== 0
  ) {
    return null;
  }
  try {
    return Temporal.Instant.compare(event.start.dateTime, action.startsAt) === 0 &&
      Temporal.Instant.compare(event.end.dateTime, action.endsAt) === 0
      ? content
      : null;
  } catch {
    return null;
  }
}
