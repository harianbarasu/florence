import { randomBytes, randomUUID } from "node:crypto";
import { Temporal } from "@js-temporal/polyfill";
import { z } from "zod";
import {
  type CalendarPushHeaders,
  GOOGLE_CALENDAR_EVENTS_SCOPE,
  GOOGLE_CALENDAR_READONLY_SCOPE,
  GoogleAdapterError,
  type GoogleCalendarAdapter,
  type GoogleCalendarEvent,
  type GoogleCalendarPushEvent,
  GoogleSyncTokenExpiredError,
  type GoogleTokenSet,
  googleTokenSetSchema,
  parseGoogleCalendarPush,
} from "../adapters/google/index.js";
import { TimeZoneSchema } from "../domain/index.js";
import { canonicalJson, sha256 } from "../security/canonical-json.js";
import type { SecretBox } from "../security/secret-box.js";
import {
  type GoogleCredentialLifecyclePort,
  type GoogleSyncConnection,
  GoogleSyncError,
  googleConnectionCredentialsAad,
  type ScopedMutationResult,
} from "./google-sync.js";

const instantSchema = z.iso.datetime({ offset: true });
const DAY_MS = 24 * 60 * 60_000;

export const calendarSyncPhaseSchema = z.enum(["initial", "live"]);
export type CalendarSyncPhase = z.infer<typeof calendarSyncPhaseSchema>;

export const calendarWatchStateSchema = z.strictObject({
  channelId: z.string().min(1).max(500),
  resourceId: z.string().min(1).max(1_000),
  expiresAt: instantSchema,
});

export const calendarSyncStateSchema = z
  .strictObject({
    schemaVersion: z.literal(1),
    revision: z.number().int().nonnegative(),
    phase: calendarSyncPhaseSchema,
    calendarId: z.string().min(1).max(1_000),
    initialTimeMin: instantSchema,
    initialTimeMax: instantSchema,
    pageToken: z.string().min(1).max(4_096).nullable(),
    syncToken: z.string().min(1).max(4_096).nullable(),
    timeZone: TimeZoneSchema.nullable(),
    projectionReady: z.boolean(),
    watch: calendarWatchStateSchema.nullable(),
    lastSuccessfulSyncAt: instantSchema.nullable(),
  })
  .superRefine((state, context) => {
    if (Temporal.Instant.compare(state.initialTimeMin, state.initialTimeMax) >= 0) {
      context.addIssue({ code: "custom", path: ["initialTimeMax"], message: "invalid initial range" });
    }
    if (state.phase === "initial" && state.syncToken !== null) {
      context.addIssue({ code: "custom", path: ["syncToken"], message: "initial sync has no token" });
    }
    if (state.phase === "initial" && state.projectionReady) {
      context.addIssue({ code: "custom", path: ["projectionReady"], message: "initial sync is incomplete" });
    }
    if (state.phase === "live" && state.syncToken === null) {
      context.addIssue({ code: "custom", path: ["syncToken"], message: "live sync needs a token" });
    }
  });

export type CalendarSyncState = z.infer<typeof calendarSyncStateSchema>;

const calendarWorkIdentityShape = {
  householdId: z.uuid(),
  adultId: z.uuid(),
  connectionId: z.uuid(),
  calendarId: z.string().min(1).max(1_000),
} as const;

export const calendarSyncWorkSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("start"), ...calendarWorkIdentityShape }),
  z.strictObject({ kind: z.literal("continue"), ...calendarWorkIdentityShape }),
  z.strictObject({ kind: z.literal("push"), ...calendarWorkIdentityShape }),
  z.strictObject({ kind: z.literal("scheduled"), ...calendarWorkIdentityShape }),
  z.strictObject({ kind: z.literal("renew_watch"), ...calendarWorkIdentityShape }),
  z.strictObject({ kind: z.literal("refresh_horizon"), ...calendarWorkIdentityShape }),
  z.strictObject({ kind: z.literal("revoke"), ...calendarWorkIdentityShape }),
]);

export type CalendarSyncWork = z.infer<typeof calendarSyncWorkSchema>;

export type CalendarSyncResult = {
  status: "processed" | "continuation_required" | "noop" | "reauth_required" | "revoked";
  connectionId: string;
  householdId: string;
  adultId: string;
  calendarId: string;
  phase: CalendarSyncPhase;
  processedEvents: number;
};

export const calendarBusyWindowSchema = z.strictObject({
  startsAt: instantSchema,
  endsAt: instantSchema,
  allDay: z.boolean(),
});

export type CalendarBusyWindow = z.infer<typeof calendarBusyWindowSchema>;

export interface CalendarProviderPort {
  listEventsPage(input: {
    accessToken: string;
    googleSubject: string;
    calendarId: string;
    pageToken?: string;
    syncToken?: string;
    timeMin?: string;
    timeMax?: string;
    maxResults?: number;
    singleEvents?: boolean;
  }): ReturnType<GoogleCalendarAdapter["listEventsPage"]>;
  watchEvents(input: {
    accessToken: string;
    calendarId: string;
    channelId: string;
    address: string;
    channelToken: string;
    expiresAt?: string;
  }): ReturnType<GoogleCalendarAdapter["watchEvents"]>;
  stopChannel(input: {
    accessToken: string;
    channelId: string;
    resourceId: string;
  }): ReturnType<GoogleCalendarAdapter["stopChannel"]>;
}

export interface PersistPersonalCalendarSourceInput {
  householdId: string;
  adultId: string;
  connectionId: string;
  calendarId: string;
  externalId: string;
  kind: "calendar_event" | "calendar_event_deleted";
  occurredAt: string;
  contentHash: string;
  encryptedContent: string;
  metadata: Record<string, unknown>;
  busyWindow: CalendarBusyWindow | null;
}

export interface CalendarPushTarget {
  householdId: string;
  adultId: string;
  connectionId: string;
  calendarId: string;
}

export interface CalendarSyncRepositoryPort {
  saveCalendarSyncState(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    expectedRevision: number;
    state: CalendarSyncState;
  }): Promise<ScopedMutationResult>;
  restartCalendarSync(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    expectedRevision: number;
    state: CalendarSyncState;
  }): Promise<ScopedMutationResult>;
  persistPersonalCalendarSource(input: PersistPersonalCalendarSourceInput): Promise<{
    sourceItemId: string;
    disposition: "inserted" | "unchanged" | "revised";
    revision: number;
  }>;
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
  replaceCalendarWatch(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    calendarId: string;
    expectedRevision: number;
    state: CalendarSyncState;
    channel: {
      channelId: string;
      resourceId: string;
      resourceUri: string;
      channelToken: string;
      expiresAt: string;
    };
  }): Promise<ScopedMutationResult>;
  markCalendarWatchStopped(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    channelId: string;
  }): Promise<"updated" | "not_found">;
  revokeConnection(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    revokedAt: string;
  }): Promise<"revoked" | "not_found">;
}

export interface CalendarConnectionDirectoryPort {
  getOwnedGoogleConnection(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
  }): Promise<GoogleSyncConnection | null>;
}

export interface CalendarPushStorePort {
  authenticateCalendarPush(input: {
    channelId: string;
    resourceId: string;
    resourceUri: string;
    channelToken: string;
    messageNumber: string;
    receivedAt: string;
  }): Promise<CalendarPushTarget | null>;
  enqueueCalendarSyncWork(input: {
    idempotencyKey: string;
    householdId: string;
    work: CalendarSyncWork;
  }): Promise<{ jobId: string; created: boolean }>;
}

export interface GoogleCalendarPushIngressOptions {
  store: CalendarPushStorePort;
  now?: () => Date;
}

/** Authenticates a per-watch token and durably records only an invalidation trigger. */
export class GoogleCalendarPushIngress {
  readonly #store: CalendarPushStorePort;
  readonly #now: () => Date;

  constructor(options: GoogleCalendarPushIngressOptions) {
    this.#store = options.store;
    this.#now = options.now ?? (() => new Date());
  }

  async accept(headers: CalendarPushHeaders): Promise<"accepted" | "unauthorized"> {
    const channelToken = headerValue(headers, "x-goog-channel-token");
    if (channelToken === null) return "unauthorized";
    let event: GoogleCalendarPushEvent;
    try {
      event = parseGoogleCalendarPush(headers, channelToken);
    } catch {
      return "unauthorized";
    }
    const target = await this.#store.authenticateCalendarPush({
      channelId: event.channelId,
      resourceId: event.resourceId,
      resourceUri: event.resourceUri,
      channelToken,
      messageNumber: event.messageNumber,
      receivedAt: this.#now().toISOString(),
    });
    if (target === null) return "unauthorized";
    await this.#store.enqueueCalendarSyncWork({
      householdId: target.householdId,
      idempotencyKey: event.providerEventId,
      work: { kind: "push", ...target },
    });
    return "accepted";
  }
}

export interface GoogleCalendarSyncServiceOptions {
  directory: CalendarConnectionDirectoryPort;
  repository: CalendarSyncRepositoryPort;
  calendar: CalendarProviderPort;
  oauth: GoogleCredentialLifecyclePort;
  secretBox: Pick<SecretBox, "open" | "seal">;
  publicBaseUrl: string;
  now?: () => Date;
  pageSize?: number;
  refreshSkewMs?: number;
  watchTtlDays?: number;
}

/** Owns Calendar paging, cursor CAS, encryption, busy projection, and watch replacement. */
export class GoogleCalendarSyncService {
  readonly #directory: CalendarConnectionDirectoryPort;
  readonly #repository: CalendarSyncRepositoryPort;
  readonly #calendar: CalendarProviderPort;
  readonly #oauth: GoogleCredentialLifecyclePort;
  readonly #secretBox: Pick<SecretBox, "open" | "seal">;
  readonly #pushAddress: string;
  readonly #now: () => Date;
  readonly #pageSize: number;
  readonly #refreshSkewMs: number;
  readonly #watchTtlDays: number;

  constructor(options: GoogleCalendarSyncServiceOptions) {
    this.#directory = options.directory;
    this.#repository = options.repository;
    this.#calendar = options.calendar;
    this.#oauth = options.oauth;
    this.#secretBox = options.secretBox;
    this.#pushAddress = new URL("/webhooks/google/calendar", z.url().parse(options.publicBaseUrl)).toString();
    this.#now = options.now ?? (() => new Date());
    this.#pageSize = z
      .number()
      .int()
      .min(1)
      .max(2_500)
      .parse(options.pageSize ?? 250);
    this.#refreshSkewMs = z
      .number()
      .int()
      .nonnegative()
      .parse(options.refreshSkewMs ?? 5 * 60_000);
    this.#watchTtlDays = z
      .number()
      .int()
      .min(1)
      .max(30)
      .parse(options.watchTtlDays ?? 6);
  }

  async execute(rawWork: unknown, signal?: AbortSignal): Promise<CalendarSyncResult> {
    const work = calendarSyncWorkSchema.parse(rawWork);
    assertNotAborted(signal);
    try {
      const connection = await this.#ownedConnection(work);
      if (connection.status === "revoked")
        return calendarResult(connection, initialState(this.#now(), work.calendarId), "revoked", 0);
      if (connection.status === "reauth_required") {
        return calendarResult(
          connection,
          stateFromConnection(connection, work.calendarId, this.#now()),
          "reauth_required",
          0,
        );
      }
      if (connection.status !== "active") {
        throw new GoogleSyncError("Google Calendar connection is inactive", "not_authorized", false);
      }
      switch (work.kind) {
        case "start":
          return await this.#start(connection, work.calendarId);
        case "renew_watch":
          return await this.#renewWatch(connection, work.calendarId, signal);
        case "refresh_horizon":
          return await this.#refreshHorizon(connection, work.calendarId);
        case "revoke":
          return await this.#revoke(connection, work.calendarId, signal);
        case "continue":
        case "push":
        case "scheduled":
          return await this.#processPage(connection, work.calendarId, signal);
      }
    } catch (error) {
      if (error instanceof GoogleSyncError) throw error;
      if (signal?.aborted || (error instanceof Error && error.name === "AbortError")) {
        throw new GoogleSyncError("Google Calendar synchronization was cancelled", "cancelled", true);
      }
      if (error instanceof GoogleAdapterError) {
        throw new GoogleSyncError(
          "Google Calendar failed at the provider boundary",
          "provider_failure",
          error.retryable,
        );
      }
      throw new GoogleSyncError("Google Calendar synchronization failed closed", "invalid_state", false);
    }
  }

  async #start(connection: GoogleSyncConnection, calendarId: string): Promise<CalendarSyncResult> {
    const existing = connection.cursor.calendar;
    if (existing !== undefined) {
      const state = stateFromConnection(connection, calendarId, this.#now());
      return calendarResult(
        connection,
        state,
        state.phase === "live" && state.projectionReady && state.watch !== null
          ? "noop"
          : "continuation_required",
        0,
      );
    }
    const state = initialState(this.#now(), calendarId);
    const saved = await this.#saveState(connection, state);
    return calendarResult(connection, saved, "continuation_required", 0);
  }

  async #processPage(
    connection: GoogleSyncConnection,
    calendarId: string,
    signal?: AbortSignal,
  ): Promise<CalendarSyncResult> {
    const state = stateFromConnection(connection, calendarId, this.#now());
    // This CAS is both a per-connection page lease and a fail-closed coverage marker.
    // Schedule readers expose no windows until the complete page chain commits.
    const workingState = await this.#saveState(connection, { ...state, projectionReady: false });
    try {
      const page = await this.#withCredentials(connection, signal, (accessToken) =>
        this.#calendar.listEventsPage({
          accessToken,
          googleSubject: connection.externalAccountId,
          calendarId,
          maxResults: this.#pageSize,
          singleEvents: true,
          ...(workingState.pageToken ? { pageToken: workingState.pageToken } : {}),
          ...(workingState.phase === "initial"
            ? { timeMin: workingState.initialTimeMin, timeMax: workingState.initialTimeMax }
            : { syncToken: workingState.syncToken as string }),
        }),
      );
      const timeZone = normalizedTimeZone(page.timeZone ?? workingState.timeZone);
      let processedEvents = 0;
      for (const event of page.events) {
        assertNotAborted(signal);
        if (event.googleSubject !== connection.externalAccountId || event.calendarId !== calendarId) {
          throw new GoogleSyncError(
            "Calendar returned an event outside the owned connection",
            "not_authorized",
            false,
          );
        }
        await this.#persistEvent(connection, event, timeZone);
        processedEvents += 1;
      }

      const nextState = nextCalendarState(
        workingState,
        page.nextPageToken,
        page.nextSyncToken,
        timeZone,
        this.#now(),
      );
      const saved = await this.#saveState(connection, nextState);
      const continuation = saved.pageToken !== null || (saved.phase === "live" && saved.watch === null);
      return calendarResult(
        connection,
        saved,
        continuation ? "continuation_required" : "processed",
        processedEvents,
      );
    } catch (error) {
      if (!(error instanceof GoogleSyncTokenExpiredError)) throw error;
      const restarted = initialState(this.#now(), calendarId, workingState.revision + 1);
      const saved = await this.#repository.restartCalendarSync({
        householdId: connection.householdId,
        adultId: connection.adultId,
        connectionId: connection.id,
        expectedRevision: workingState.revision,
        state: restarted,
      });
      assertScopedUpdate(saved, "Calendar sync-token reset");
      return calendarResult(connection, restarted, "continuation_required", 0);
    }
  }

  async #renewWatch(
    connection: GoogleSyncConnection,
    calendarId: string,
    signal?: AbortSignal,
  ): Promise<CalendarSyncResult> {
    const state = stateFromConnection(connection, calendarId, this.#now());
    if (state.phase !== "live" || !state.projectionReady) {
      return calendarResult(connection, state, "continuation_required", 0);
    }
    if (
      state.watch !== null &&
      Temporal.Instant.compare(
        state.watch.expiresAt,
        Temporal.Instant.from(this.#now().toISOString()).add({ hours: 24 }),
      ) > 0
    ) {
      return calendarResult(connection, state, "noop", 0);
    }

    const channelId = randomUUID();
    const channelToken = randomBytes(32).toString("base64url");
    const requestedExpiry = new Date(this.#now().getTime() + this.#watchTtlDays * DAY_MS).toISOString();
    const receipt = await this.#withCredentials(connection, signal, (accessToken) =>
      this.#calendar.watchEvents({
        accessToken,
        calendarId,
        channelId,
        channelToken,
        address: this.#pushAddress,
        expiresAt: requestedExpiry,
      }),
    );
    const expiresAt = receipt.expiresAt ?? requestedExpiry;
    const next = calendarSyncStateSchema.parse({
      ...state,
      revision: state.revision + 1,
      watch: { channelId: receipt.channelId, resourceId: receipt.resourceId, expiresAt },
    });
    let replaced: ScopedMutationResult;
    try {
      replaced = await this.#repository.replaceCalendarWatch({
        householdId: connection.householdId,
        adultId: connection.adultId,
        connectionId: connection.id,
        calendarId,
        expectedRevision: state.revision,
        state: next,
        channel: {
          channelId: receipt.channelId,
          resourceId: receipt.resourceId,
          resourceUri: receipt.resourceUri,
          channelToken,
          expiresAt,
        },
      });
    } catch (error) {
      await this.#stopChannelBestEffort(connection, receipt.channelId, receipt.resourceId, signal);
      throw error;
    }
    if (replaced !== "updated") {
      await this.#stopChannelBestEffort(connection, receipt.channelId, receipt.resourceId, signal);
      assertScopedUpdate(replaced, "Calendar watch replacement");
    }
    if (state.watch !== null) {
      try {
        await this.#withCredentials(connection, signal, (accessToken) =>
          this.#calendar.stopChannel({
            accessToken,
            channelId: state.watch?.channelId as string,
            resourceId: state.watch?.resourceId as string,
          }),
        );
        await this.#repository.markCalendarWatchStopped({
          householdId: connection.householdId,
          adultId: connection.adultId,
          connectionId: connection.id,
          channelId: state.watch.channelId,
        });
      } catch {
        // The retiring channel remains authenticated until its recorded expiry.
      }
    }
    return calendarResult(connection, next, "processed", 0);
  }

  async #refreshHorizon(connection: GoogleSyncConnection, calendarId: string): Promise<CalendarSyncResult> {
    const state = stateFromConnection(connection, calendarId, this.#now());
    const restarted = initialState(this.#now(), calendarId, state.revision + 1);
    const saved = await this.#repository.restartCalendarSync({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      expectedRevision: state.revision,
      state: restarted,
    });
    assertScopedUpdate(saved, "Calendar horizon refresh");
    return calendarResult(connection, restarted, "continuation_required", 0);
  }

  async #revoke(
    connection: GoogleSyncConnection,
    calendarId: string,
    signal?: AbortSignal,
  ): Promise<CalendarSyncResult> {
    const state = stateFromConnection(connection, calendarId, this.#now());
    if (connection.encryptedCredentials !== null) {
      try {
        const tokens = decryptCredentials(connection, this.#secretBox);
        if (state.watch !== null) {
          await this.#calendar.stopChannel({
            accessToken: tokens.accessToken,
            channelId: state.watch.channelId,
            resourceId: state.watch.resourceId,
          });
        }
        assertNotAborted(signal);
        await this.#oauth.revoke(tokens);
      } catch {
        // Local revocation is authoritative even when remote cleanup is unavailable.
      }
    }
    const revoked = await this.#repository.revokeConnection({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      revokedAt: this.#now().toISOString(),
    });
    if (revoked === "not_found") {
      throw new GoogleSyncError("Calendar connection revocation was not scoped", "not_authorized", false);
    }
    return calendarResult(connection, state, "revoked", 0);
  }

  async #persistEvent(
    connection: GoogleSyncConnection,
    event: GoogleCalendarEvent,
    timeZone: string | null,
  ): Promise<void> {
    const serialized = canonicalJson(event);
    const busyWindow = normalizeCalendarBusyWindow(event, timeZone);
    await this.#repository.persistPersonalCalendarSource({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      calendarId: event.calendarId,
      externalId: event.eventId,
      kind: event.deleted ? "calendar_event_deleted" : "calendar_event",
      occurredAt: event.updatedAt ?? event.createdAt ?? busyWindow?.startsAt ?? this.#now().toISOString(),
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
        deleted: event.deleted,
        contentAadVersion: 1,
      },
      busyWindow,
    });
  }

  async #stopChannelBestEffort(
    connection: GoogleSyncConnection,
    channelId: string,
    resourceId: string,
    signal?: AbortSignal,
  ): Promise<void> {
    await this.#withCredentials(connection, signal, (accessToken) =>
      this.#calendar.stopChannel({ accessToken, channelId, resourceId }),
    ).catch(() => undefined);
  }

  async #withCredentials<T>(
    connection: GoogleSyncConnection,
    signal: AbortSignal | undefined,
    operation: (accessToken: string) => Promise<T>,
  ): Promise<T> {
    let tokens = decryptCredentials(connection, this.#secretBox);
    assertCalendarScope(connection, tokens);
    if (tokenNeedsRefresh(tokens, this.#now(), this.#refreshSkewMs)) {
      tokens = await this.#refreshCredentials(connection, tokens, signal);
    }
    assertNotAborted(signal);
    try {
      return await operation(tokens.accessToken);
    } catch (error) {
      if (!(error instanceof GoogleAdapterError) || error.code !== "unauthorized") throw error;
      tokens = await this.#refreshCredentials(connection, tokens, signal);
      assertNotAborted(signal);
      return operation(tokens.accessToken);
    }
  }

  async #refreshCredentials(
    connection: GoogleSyncConnection,
    current: GoogleTokenSet,
    signal?: AbortSignal,
  ): Promise<GoogleTokenSet> {
    assertNotAborted(signal);
    let refreshed: GoogleTokenSet;
    try {
      refreshed = await this.#oauth.refresh(current);
    } catch (error) {
      if (error instanceof GoogleAdapterError && error.code === "unauthorized") {
        await this.#repository.markConnectionStatus({
          householdId: connection.householdId,
          adultId: connection.adultId,
          connectionId: connection.id,
          status: "reauth_required",
        });
      }
      throw error;
    }
    assertCalendarScope(connection, refreshed);
    const encryptedCredentials = this.#secretBox.seal(
      JSON.stringify(refreshed),
      googleConnectionCredentialsAad(connection),
    );
    const replaced = await this.#repository.replaceEncryptedCredentials({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      expectedCiphertext: connection.encryptedCredentials ?? "",
      encryptedCredentials,
      grantedScopes: refreshed.scope,
    });
    assertScopedUpdate(replaced, "Calendar credential refresh");
    connection.encryptedCredentials = encryptedCredentials;
    connection.grantedScopes = [...refreshed.scope];
    return refreshed;
  }

  async #saveState(connection: GoogleSyncConnection, current: CalendarSyncState): Promise<CalendarSyncState> {
    const state = calendarSyncStateSchema.parse({ ...current, revision: current.revision + 1 });
    const saved = await this.#repository.saveCalendarSyncState({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      expectedRevision: current.revision,
      state,
    });
    assertScopedUpdate(saved, "Calendar cursor update");
    return state;
  }

  async #ownedConnection(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
  }): Promise<GoogleSyncConnection> {
    const connection = await this.#directory.getOwnedGoogleConnection(input);
    if (
      connection === null ||
      connection.householdId !== input.householdId ||
      connection.adultId !== input.adultId ||
      connection.id !== input.connectionId
    ) {
      throw new GoogleSyncError("Calendar connection ownership did not match", "not_authorized", false);
    }
    return connection;
  }
}

export function normalizeCalendarBusyWindow(
  event: GoogleCalendarEvent,
  calendarTimeZone: string | null,
): CalendarBusyWindow | null {
  if (event.deleted || event.transparency === "transparent" || event.start === null || event.end === null) {
    return null;
  }
  try {
    const fallback = normalizedTimeZone(calendarTimeZone);
    const startsAt = eventTimeToInstant(event.start, fallback);
    const endsAt = eventTimeToInstant(event.end, fallback);
    if (Temporal.Instant.compare(startsAt, endsAt) >= 0) return null;
    return calendarBusyWindowSchema.parse({
      startsAt,
      endsAt,
      allDay: event.start.kind === "date",
    });
  } catch {
    return null;
  }
}

export function calendarSourceContentAad(
  connection: Pick<GoogleSyncConnection, "householdId" | "adultId" | "id">,
  calendarId: string,
  eventId: string,
): string {
  return `google-source:${connection.householdId}:${connection.adultId}:${connection.id}:calendar:${calendarId}:${eventId}`;
}

function initialState(now: Date, calendarId: string, revision = 0): CalendarSyncState {
  const boundary = now.getTime();
  return calendarSyncStateSchema.parse({
    schemaVersion: 1,
    revision,
    phase: "initial",
    calendarId,
    initialTimeMin: new Date(boundary - 90 * DAY_MS).toISOString(),
    initialTimeMax: new Date(boundary + 540 * DAY_MS).toISOString(),
    pageToken: null,
    syncToken: null,
    timeZone: null,
    projectionReady: false,
    watch: null,
    lastSuccessfulSyncAt: null,
  });
}

function stateFromConnection(
  connection: GoogleSyncConnection,
  calendarId: string,
  now: Date,
): CalendarSyncState {
  const raw = connection.cursor.calendar;
  if (raw === undefined) return initialState(now, calendarId);
  const parsed = calendarSyncStateSchema.safeParse(raw);
  if (!parsed.success || parsed.data.calendarId !== calendarId) {
    throw new GoogleSyncError("Stored Calendar cursor is invalid", "invalid_state", false);
  }
  return parsed.data;
}

function nextCalendarState(
  state: CalendarSyncState,
  nextPageToken: string | null,
  nextSyncToken: string | null,
  timeZone: string | null,
  now: Date,
): CalendarSyncState {
  if (nextPageToken !== null) {
    return calendarSyncStateSchema.parse({
      ...state,
      pageToken: nextPageToken,
      timeZone,
      projectionReady: false,
    });
  }
  if (nextSyncToken === null) {
    throw new GoogleSyncError("Calendar page ended without a sync token", "invalid_state", false);
  }
  return calendarSyncStateSchema.parse({
    ...state,
    phase: "live",
    pageToken: null,
    syncToken: nextSyncToken,
    timeZone,
    projectionReady: true,
    lastSuccessfulSyncAt: now.toISOString(),
  });
}

function eventTimeToInstant(
  value: NonNullable<GoogleCalendarEvent["start"]>,
  fallbackTimeZone: string | null,
): string {
  if (value.kind === "date") {
    if (fallbackTimeZone === null) throw new Error("all-day event has no time zone");
    return Temporal.PlainDateTime.from(`${value.date}T00:00:00`)
      .toZonedDateTime(fallbackTimeZone, { disambiguation: "compatible" })
      .toInstant()
      .toString();
  }
  try {
    return Temporal.Instant.from(value.dateTime).toString();
  } catch {
    const timeZone = normalizedTimeZone(value.timeZone ?? fallbackTimeZone);
    if (timeZone === null) throw new Error("local event has no time zone");
    return Temporal.PlainDateTime.from(value.dateTime)
      .toZonedDateTime(timeZone, { disambiguation: "compatible" })
      .toInstant()
      .toString();
  }
}

function normalizedTimeZone(value: string | null): string | null {
  return value === null ? null : TimeZoneSchema.parse(value);
}

function decryptCredentials(
  connection: GoogleSyncConnection,
  secretBox: Pick<SecretBox, "open">,
): GoogleTokenSet {
  if (connection.encryptedCredentials === null) {
    throw new GoogleSyncError("Google Calendar credentials are unavailable", "not_authorized", false);
  }
  try {
    return googleTokenSetSchema.parse(
      JSON.parse(secretBox.open(connection.encryptedCredentials, googleConnectionCredentialsAad(connection))),
    );
  } catch {
    throw new GoogleSyncError(
      "Google Calendar credentials could not be authenticated",
      "invalid_state",
      false,
    );
  }
}

function assertCalendarScope(connection: GoogleSyncConnection, tokens: GoogleTokenSet): void {
  const hasScope = (scopes: readonly string[]) =>
    scopes.includes(GOOGLE_CALENDAR_READONLY_SCOPE) || scopes.includes(GOOGLE_CALENDAR_EVENTS_SCOPE);
  if (!hasScope(connection.grantedScopes) || !hasScope(tokens.scope)) {
    throw new GoogleSyncError(
      "Google connection does not grant Calendar read access",
      "not_authorized",
      false,
    );
  }
}

function tokenNeedsRefresh(tokens: GoogleTokenSet, now: Date, skewMs: number): boolean {
  if (tokens.expiresAt === null) return false;
  const expiresAt = Date.parse(tokens.expiresAt);
  return !Number.isFinite(expiresAt) || expiresAt <= now.getTime() + skewMs;
}

function assertScopedUpdate(result: ScopedMutationResult, operation: string): asserts result is "updated" {
  if (result === "updated") return;
  throw new GoogleSyncError(
    `${operation} lost its owned connection`,
    result === "conflict" ? "conflict" : "not_authorized",
    result === "conflict",
  );
}

function calendarResult(
  connection: GoogleSyncConnection,
  state: CalendarSyncState,
  status: CalendarSyncResult["status"],
  processedEvents: number,
): CalendarSyncResult {
  return {
    status,
    connectionId: connection.id,
    householdId: connection.householdId,
    adultId: connection.adultId,
    calendarId: state.calendarId,
    phase: state.phase,
    processedEvents,
  };
}

function headerValue(headers: CalendarPushHeaders, name: string): string | null {
  for (const [key, raw] of Object.entries(headers)) {
    if (key.toLowerCase() !== name || raw === undefined) continue;
    const value = Array.isArray(raw) ? raw[0] : raw;
    return value?.trim() || null;
  }
  return null;
}

function assertNotAborted(signal?: AbortSignal): void {
  if (signal?.aborted) throw signal.reason ?? new Error("aborted");
}
