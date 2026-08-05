import { randomBytes, randomUUID } from "node:crypto";
import { Temporal } from "@js-temporal/polyfill";
import { z } from "zod";
import {
  type CalendarPushHeaders,
  GOOGLE_CALENDAR_EVENTS_SCOPE,
  GOOGLE_CALENDAR_READONLY_SCOPE,
  GoogleAdapterError,
  type GoogleCalendarAccessRole,
  type GoogleCalendarAdapter,
  type GoogleCalendarEvent,
  type GoogleCalendarListEntry,
  type GoogleCalendarPushEvent,
  GoogleSyncTokenExpiredError,
  type GoogleTokenSet,
  googleTokenSetSchema,
  parseGoogleCalendarPush,
} from "../adapters/google/index.js";
import {
  CalendarEventDeletedInboxItemSchema,
  CalendarEventInboxItemSchema,
  type FlorenceApplication,
} from "../application/index.js";
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

export const calendarCatalogStateSchema = z
  .strictObject({
    schemaVersion: z.literal(1),
    revision: z.number().int().nonnegative(),
    phase: calendarSyncPhaseSchema,
    scanId: z.uuid(),
    pageToken: z.string().min(1).max(4_096).nullable(),
    syncToken: z.string().min(1).max(4_096).nullable(),
    projectionReady: z.boolean(),
    lastSuccessfulSyncAt: instantSchema.nullable(),
    lastFullScanAt: instantSchema.nullable(),
  })
  .superRefine((state, context) => {
    if (state.phase === "initial" && state.syncToken !== null) {
      context.addIssue({ code: "custom", path: ["syncToken"], message: "initial catalog has no token" });
    }
    if (state.phase === "initial" && state.projectionReady) {
      context.addIssue({
        code: "custom",
        path: ["projectionReady"],
        message: "initial catalog is incomplete",
      });
    }
    if (state.phase === "live" && state.syncToken === null) {
      context.addIssue({ code: "custom", path: ["syncToken"], message: "live catalog needs a token" });
    }
  });

export type CalendarCatalogState = z.infer<typeof calendarCatalogStateSchema>;

export type CalendarSyncRecord = {
  calendarId: string;
  providerCalendarId: string;
  status: "active" | "excluded" | "deleted";
  selectionSource: "provider" | "adult";
  availabilityOnly: boolean;
  accessRole: GoogleCalendarAccessRole | null;
  state: CalendarSyncState | null;
};

const calendarConnectionWorkIdentityShape = {
  householdId: z.uuid(),
  adultId: z.uuid(),
  connectionId: z.uuid(),
} as const;

const calendarWorkIdentityShape = {
  ...calendarConnectionWorkIdentityShape,
  calendarId: z.string().min(1).max(1_000),
} as const;

export const calendarSyncWorkSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("catalog"), ...calendarConnectionWorkIdentityShape }),
  z.strictObject({ kind: z.literal("catalog_refresh"), ...calendarConnectionWorkIdentityShape }),
  z.strictObject({ kind: z.literal("start"), ...calendarWorkIdentityShape }),
  z.strictObject({ kind: z.literal("continue"), ...calendarWorkIdentityShape }),
  z.strictObject({ kind: z.literal("push"), ...calendarWorkIdentityShape }),
  z.strictObject({ kind: z.literal("scheduled"), ...calendarWorkIdentityShape }),
  z.strictObject({ kind: z.literal("renew_watch"), ...calendarWorkIdentityShape }),
  z.strictObject({ kind: z.literal("refresh_horizon"), ...calendarWorkIdentityShape }),
  z.strictObject({ kind: z.literal("revoke"), ...calendarConnectionWorkIdentityShape }),
]);

export type CalendarSyncWork = z.infer<typeof calendarSyncWorkSchema>;

export type CalendarSyncResult = {
  status: "processed" | "continuation_required" | "noop" | "reauth_required" | "revoked";
  connectionId: string;
  householdId: string;
  adultId: string;
  calendarId: string | null;
  phase: CalendarSyncPhase | "catalog";
  processedEvents: number;
};

export const calendarBusyWindowSchema = z.strictObject({
  startsAt: instantSchema,
  endsAt: instantSchema,
  allDay: z.boolean(),
});

export type CalendarBusyWindow = z.infer<typeof calendarBusyWindowSchema>;

export interface CalendarProviderPort {
  listCalendarsPage(input: {
    accessToken: string;
    pageToken?: string;
    syncToken?: string;
    maxResults?: number;
  }): ReturnType<GoogleCalendarAdapter["listCalendarsPage"]>;
  queryFreeBusy(input: {
    accessToken: string;
    calendarId: string;
    timeMin: string;
    timeMax: string;
  }): ReturnType<GoogleCalendarAdapter["queryFreeBusy"]>;
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

export type PersistPersonalCalendarSourceResult = {
  sourceItemId: string;
  disposition: "inserted" | "unchanged" | "revised";
  revision: number;
  createdByApprovedActionId: string | null;
  retainedExisting?: "stale";
};

export type CalendarApplicationContent = {
  title: string;
  description: string | null;
  location: string | null;
  startsAt: string;
  endsAt: string;
  timeZone: string;
  allDay: boolean;
  status: "confirmed" | "tentative";
  recurrence: string[];
};

export interface CalendarPushTarget {
  householdId: string;
  adultId: string;
  connectionId: string;
  calendarId: string;
}

export interface CalendarSyncRepositoryPort {
  getCalendarSyncRecord(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    calendarId: string;
  }): Promise<CalendarSyncRecord | null>;
  saveCalendarCatalogState(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    expectedRevision: number;
    state: CalendarCatalogState;
  }): Promise<ScopedMutationResult>;
  applyCalendarCatalogPage(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    expectedRevision: number;
    state: CalendarCatalogState;
    fullScan: boolean;
    entries: readonly {
      calendarId: string;
      providerCalendarId: string;
      encryptedDisplayName: string;
      accessRole: GoogleCalendarAccessRole | null;
      primary: boolean;
      selected: boolean;
      hidden: boolean;
      deleted: boolean;
      defaultEnabled: boolean;
      availabilityOnly: boolean;
      initialState: CalendarSyncState;
    }[];
  }): Promise<ScopedMutationResult>;
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
  persistPersonalCalendarSource(
    input: PersistPersonalCalendarSourceInput,
  ): Promise<PersistPersonalCalendarSourceResult>;
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
  listCalendarWatches(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
  }): Promise<readonly { channelId: string; resourceId: string }[]>;
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
  application: Pick<FlorenceApplication, "process">;
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
  readonly #application: Pick<FlorenceApplication, "process">;
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
    this.#application = options.application;
    this.#secretBox = options.secretBox;
    this.#pushAddress = new URL("/webhooks/google/calendar", z.url().parse(options.publicBaseUrl)).toString();
    this.#now = options.now ?? (() => new Date());
    this.#pageSize = z
      .number()
      .int()
      .min(1)
      .max(2_500)
      .parse(options.pageSize ?? 25);
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
      // Revocation is the escape hatch from every non-revoked state. Provider
      // cleanup is best-effort; the scoped local revoke remains authoritative.
      if (work.kind === "revoke") {
        return connection.status === "revoked"
          ? connectionCalendarResult(connection, "revoked")
          : await this.#revoke(connection, signal);
      }
      if (connection.status === "revoked") return connectionCalendarResult(connection, "revoked");
      if (connection.status === "reauth_required") {
        return connectionCalendarResult(connection, "reauth_required");
      }
      if (connection.status !== "active") {
        throw new GoogleSyncError("Google Calendar connection is inactive", "not_authorized", false);
      }
      switch (work.kind) {
        case "catalog":
          return await this.#processCatalog(connection, signal);
        case "catalog_refresh":
          return await this.#refreshCatalog(connection);
        case "start":
          return await this.#withActiveCalendar(connection, work.calendarId, (record) =>
            this.#start(connection, record),
          );
        case "renew_watch":
          return await this.#withActiveCalendar(connection, work.calendarId, (record) =>
            this.#renewWatch(connection, record, signal),
          );
        case "refresh_horizon":
          return await this.#withActiveCalendar(connection, work.calendarId, (record) =>
            this.#refreshHorizon(connection, record),
          );
        case "continue":
        case "push":
        case "scheduled":
          return await this.#withActiveCalendar(connection, work.calendarId, (record) =>
            this.#processPage(connection, record, signal),
          );
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

  async #refreshCatalog(connection: GoogleSyncConnection): Promise<CalendarSyncResult> {
    const current = catalogStateFromConnection(connection, this.#now());
    const restarted = initialCatalogState(this.#now(), current.revision + 1);
    const saved = await this.#repository.saveCalendarCatalogState({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      expectedRevision: current.revision,
      state: restarted,
    });
    assertScopedUpdate(saved, "Calendar catalog full refresh");
    return connectionCalendarResult(connection, "continuation_required");
  }

  async #processCatalog(connection: GoogleSyncConnection, signal?: AbortSignal): Promise<CalendarSyncResult> {
    const state = catalogStateFromConnection(connection, this.#now());
    const working = calendarCatalogStateSchema.parse({
      ...state,
      revision: state.revision + 1,
      projectionReady: false,
    });
    const acquired = await this.#repository.saveCalendarCatalogState({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      expectedRevision: state.revision,
      state: working,
    });
    assertScopedUpdate(acquired, "Calendar catalog lease");
    try {
      const page = await this.#withCredentials(connection, signal, (accessToken) =>
        this.#calendar.listCalendarsPage({
          accessToken,
          maxResults: 250,
          ...(working.pageToken ? { pageToken: working.pageToken } : {}),
          ...(working.phase === "live" ? { syncToken: working.syncToken as string } : {}),
        }),
      );
      const next = nextCatalogState(working, page.nextPageToken, page.nextSyncToken, this.#now());
      const entries = page.calendars.map((entry) => this.#catalogEntry(connection, entry));
      const applied = await this.#repository.applyCalendarCatalogPage({
        householdId: connection.householdId,
        adultId: connection.adultId,
        connectionId: connection.id,
        expectedRevision: working.revision,
        state: next,
        fullScan: working.phase === "initial",
        entries,
      });
      assertScopedUpdate(applied, "Calendar catalog page");
      return {
        status: next.pageToken === null ? "processed" : "continuation_required",
        connectionId: connection.id,
        householdId: connection.householdId,
        adultId: connection.adultId,
        calendarId: null,
        phase: "catalog",
        processedEvents: page.calendars.length,
      };
    } catch (error) {
      if (!(error instanceof GoogleSyncTokenExpiredError)) throw error;
      const restarted = initialCatalogState(this.#now(), working.revision + 1);
      const saved = await this.#repository.saveCalendarCatalogState({
        householdId: connection.householdId,
        adultId: connection.adultId,
        connectionId: connection.id,
        expectedRevision: working.revision,
        state: restarted,
      });
      assertScopedUpdate(saved, "Calendar catalog reset");
      return connectionCalendarResult(connection, "continuation_required");
    }
  }

  #catalogEntry(
    connection: GoogleSyncConnection,
    entry: GoogleCalendarListEntry,
  ): Parameters<CalendarSyncRepositoryPort["applyCalendarCatalogPage"]>[0]["entries"][number] {
    const calendarId = entry.primary ? "primary" : entry.calendarId;
    const availabilityOnly = entry.accessRole === "freeBusyReader";
    const readable = entry.accessRole !== null;
    return {
      calendarId,
      providerCalendarId: entry.calendarId,
      encryptedDisplayName: this.#secretBox.seal(
        entry.displayName,
        calendarCatalogNameAad(connection, entry.calendarId),
      ),
      accessRole: entry.accessRole,
      primary: entry.primary,
      selected: entry.selected,
      hidden: entry.hidden,
      deleted: entry.deleted,
      defaultEnabled: !entry.deleted && readable && (entry.primary || (entry.selected && !entry.hidden)),
      availabilityOnly,
      initialState: initialState(this.#now(), calendarId),
    };
  }

  async #withActiveCalendar(
    connection: GoogleSyncConnection,
    calendarId: string,
    operation: (record: CalendarSyncRecord) => Promise<CalendarSyncResult>,
  ): Promise<CalendarSyncResult> {
    const record = await this.#repository.getCalendarSyncRecord({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      calendarId,
    });
    if (record === null || record.status !== "active") {
      return calendarResult(connection, record?.state ?? initialState(this.#now(), calendarId), "noop", 0);
    }
    return operation(record);
  }

  async #start(connection: GoogleSyncConnection, record: CalendarSyncRecord): Promise<CalendarSyncResult> {
    if (record.state !== null) {
      const state = record.state;
      return calendarResult(
        connection,
        state,
        state.phase === "live" && state.projectionReady && state.watch !== null
          ? "noop"
          : "continuation_required",
        0,
      );
    }
    const state = initialState(this.#now(), record.calendarId);
    const saved = await this.#saveState(connection, state);
    return calendarResult(connection, saved, "continuation_required", 0);
  }

  async #processPage(
    connection: GoogleSyncConnection,
    record: CalendarSyncRecord,
    signal?: AbortSignal,
  ): Promise<CalendarSyncResult> {
    if (record.availabilityOnly) return this.#processFreeBusy(connection, record, signal);
    const calendarId = record.calendarId;
    const state = record.state ?? initialState(this.#now(), calendarId);
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
        await this.#persistEvent(connection, event, timeZone, workingState, record.availabilityOnly);
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

  async #processFreeBusy(
    connection: GoogleSyncConnection,
    record: CalendarSyncRecord,
    signal?: AbortSignal,
  ): Promise<CalendarSyncResult> {
    const current = record.state ?? initialState(this.#now(), record.calendarId);
    const working = calendarSyncStateSchema.parse({
      ...current,
      revision: current.revision + 1,
      projectionReady: false,
      watch: null,
    });
    const restarted = await this.#repository.restartCalendarSync({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      expectedRevision: current.revision,
      state: working,
    });
    assertScopedUpdate(restarted, "Calendar availability snapshot lease");
    const windows = await this.#withCredentials(connection, signal, (accessToken) =>
      this.#calendar.queryFreeBusy({
        accessToken,
        calendarId: record.calendarId,
        timeMin: working.initialTimeMin,
        timeMax: working.initialTimeMax,
      }),
    );
    for (const window of windows) {
      assertNotAborted(signal);
      const externalId = `freebusy-${sha256(canonicalJson([window.startsAt, window.endsAt]))}`;
      const content = canonicalJson({
        schemaVersion: 1,
        availabilityOnly: true,
        calendarId: record.calendarId,
        startsAt: window.startsAt,
        endsAt: window.endsAt,
      });
      await this.#repository.persistPersonalCalendarSource({
        householdId: connection.householdId,
        adultId: connection.adultId,
        connectionId: connection.id,
        calendarId: record.calendarId,
        externalId,
        kind: "calendar_event",
        occurredAt: this.#now().toISOString(),
        contentHash: `sha256:${sha256(content)}`,
        encryptedContent: this.#secretBox.seal(
          content,
          calendarSourceContentAad(connection, record.calendarId, externalId),
        ),
        metadata: {
          schemaVersion: 1,
          provider: "google-calendar",
          sourceScope: "personal",
          availabilityOnly: true,
          calendarId: record.calendarId,
          googleSubject: connection.externalAccountId,
          contentAadVersion: 1,
        },
        busyWindow: { startsAt: window.startsAt, endsAt: window.endsAt, allDay: false },
      });
    }
    const completed = calendarSyncStateSchema.parse({
      ...working,
      phase: "live",
      pageToken: null,
      syncToken: "freebusy-snapshot",
      projectionReady: true,
      lastSuccessfulSyncAt: this.#now().toISOString(),
    });
    const saved = await this.#saveState(connection, completed);
    return calendarResult(connection, saved, "processed", windows.length);
  }

  async #renewWatch(
    connection: GoogleSyncConnection,
    record: CalendarSyncRecord,
    signal?: AbortSignal,
  ): Promise<CalendarSyncResult> {
    const calendarId = record.calendarId;
    const state = record.state ?? initialState(this.#now(), calendarId);
    if (record.availabilityOnly) return calendarResult(connection, state, "noop", 0);
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

  async #refreshHorizon(
    connection: GoogleSyncConnection,
    record: CalendarSyncRecord,
  ): Promise<CalendarSyncResult> {
    const calendarId = record.calendarId;
    const state = record.state ?? initialState(this.#now(), calendarId);
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

  async #revoke(connection: GoogleSyncConnection, signal?: AbortSignal): Promise<CalendarSyncResult> {
    if (connection.encryptedCredentials !== null) {
      try {
        const tokens = decryptCredentials(connection, this.#secretBox);
        const watches = await this.#repository.listCalendarWatches({
          householdId: connection.householdId,
          adultId: connection.adultId,
          connectionId: connection.id,
        });
        for (const watch of watches) {
          assertNotAborted(signal);
          await this.#calendar
            .stopChannel({
              accessToken: tokens.accessToken,
              channelId: watch.channelId,
              resourceId: watch.resourceId,
            })
            .catch(() => undefined);
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
    return connectionCalendarResult(connection, "revoked");
  }

  async #persistEvent(
    connection: GoogleSyncConnection,
    event: GoogleCalendarEvent,
    timeZone: string | null,
    state: CalendarSyncState,
    availabilityOnly: boolean,
  ): Promise<void> {
    const serialized = canonicalJson(event);
    const busyWindow = normalizeCalendarBusyWindow(event, timeZone);
    const applicationContent = event.deleted ? null : calendarApplicationContent(event, timeZone);
    const applicationContentDigest =
      applicationContent === null ? null : calendarApplicationContentDigest(applicationContent);
    const persisted = await this.#repository.persistPersonalCalendarSource({
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
        ...(applicationContentDigest === null ? {} : { applicationContentDigest }),
      },
      busyWindow,
    });
    if (persisted.retainedExisting === "stale") return;
    if (availabilityOnly) return;
    if (!event.deleted && persisted.createdByApprovedActionId !== null) return;

    const observedAt = this.#now().toISOString();
    if (event.deleted) {
      await this.#processApplicationItem(
        CalendarEventDeletedInboxItemSchema.parse({
          kind: "calendar_event_deleted",
          ...calendarApplicationIdentity(connection, event, persisted.revision, observedAt),
        }),
      );
      return;
    }
    if (
      applicationContent === null ||
      !isCalendarEventRelevantForApplication(event, applicationContent, state)
    ) {
      return;
    }
    await this.#processApplicationItem(
      CalendarEventInboxItemSchema.parse({
        kind: "calendar_event",
        ...calendarApplicationIdentity(connection, event, persisted.revision, observedAt),
        contentDigest: applicationContentDigest,
        ...applicationContent,
      }),
    );
  }

  async #processApplicationItem(
    item:
      | ReturnType<typeof CalendarEventInboxItemSchema.parse>
      | ReturnType<typeof CalendarEventDeletedInboxItemSchema.parse>,
  ): Promise<void> {
    let applicationResult: Awaited<ReturnType<FlorenceApplication["process"]>>;
    try {
      applicationResult = await this.#application.process(item);
    } catch {
      throw new GoogleSyncError("Calendar application processing did not complete", "invalid_state", true);
    }
    if (applicationResult.outcome.status !== "processed") {
      throw new GoogleSyncError(
        "Calendar item was rejected by the application boundary",
        "invalid_state",
        false,
      );
    }
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

/**
 * Produces the complete provider-neutral Calendar payload that may cross into
 * the application. Provider identities, people, links, visibility, and change
 * markers deliberately have no representation here.
 */
export function calendarApplicationContent(
  event: GoogleCalendarEvent,
  calendarTimeZone: string | null,
): CalendarApplicationContent | null {
  if (
    event.deleted ||
    (event.status !== "confirmed" && event.status !== "tentative") ||
    event.start === null ||
    event.end === null ||
    event.start.kind !== event.end.kind
  ) {
    return null;
  }
  const title = boundedCalendarText(event.summary.trim(), 2_000);
  if (title.length === 0) return null;
  const timeZone = eventApplicationTimeZone(event, calendarTimeZone);
  if (timeZone === null) return null;
  try {
    const startsAt = eventTimeToInstant(event.start, timeZone);
    const endsAt = eventTimeToInstant(event.end, timeZone);
    if (Temporal.Instant.compare(startsAt, endsAt) >= 0) return null;
    const recurrence = event.recurrence.slice(0, 100).map((rule) => boundedCalendarText(rule.trim(), 2_000));
    if (recurrence.some((rule) => rule.length === 0)) return null;
    return {
      title,
      description: event.description === null ? null : boundedCalendarText(event.description, 20_000),
      location: event.location === null ? null : boundedCalendarText(event.location, 2_000),
      startsAt,
      endsAt,
      timeZone,
      allDay: event.start.kind === "date",
      status: event.status,
      recurrence,
    };
  } catch {
    return null;
  }
}

/** Hashes exactly the semantic fields visible to Calendar triage. */
export function calendarApplicationContentDigest(content: CalendarApplicationContent): string {
  return `sha256:${sha256(canonicalJson(content))}`;
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

function initialCatalogState(_now: Date, revision = 0): CalendarCatalogState {
  return calendarCatalogStateSchema.parse({
    schemaVersion: 1,
    revision,
    phase: "initial",
    scanId: randomUUID(),
    pageToken: null,
    syncToken: null,
    projectionReady: false,
    lastSuccessfulSyncAt: null,
    lastFullScanAt: null,
  });
}

function catalogStateFromConnection(connection: GoogleSyncConnection, now: Date): CalendarCatalogState {
  const raw = connection.cursor.calendarCatalog;
  if (raw === undefined) return initialCatalogState(now);
  const parsed = calendarCatalogStateSchema.safeParse(raw);
  if (!parsed.success) {
    throw new GoogleSyncError("Stored Calendar catalog cursor is invalid", "invalid_state", false);
  }
  return parsed.data;
}

function nextCatalogState(
  state: CalendarCatalogState,
  nextPageToken: string | null,
  nextSyncToken: string | null,
  now: Date,
): CalendarCatalogState {
  if (nextPageToken !== null) {
    return calendarCatalogStateSchema.parse({
      ...state,
      revision: state.revision + 1,
      pageToken: nextPageToken,
      projectionReady: false,
    });
  }
  if (nextSyncToken === null) {
    throw new GoogleSyncError("Calendar catalog page ended without a sync token", "invalid_state", false);
  }
  return calendarCatalogStateSchema.parse({
    ...state,
    revision: state.revision + 1,
    phase: "live",
    pageToken: null,
    syncToken: nextSyncToken,
    projectionReady: true,
    lastSuccessfulSyncAt: now.toISOString(),
    lastFullScanAt: state.phase === "initial" ? now.toISOString() : state.lastFullScanAt,
  });
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

function calendarApplicationIdentity(
  connection: GoogleSyncConnection,
  event: GoogleCalendarEvent,
  revision: number,
  observedAt: string,
) {
  const eventIdentity = `${connection.id}:${event.calendarId}:${event.eventId}`;
  return {
    householdId: connection.householdId,
    idempotencyKey: `calendar:${eventIdentity}:revision:${revision}`,
    occurredAt: observedAt,
    ownerAdultId: connection.adultId,
    accountRef: `google:${connection.id}`,
    eventRef: `calendar:${eventIdentity}`,
    providerRef: `google-calendar:${eventIdentity}`,
    revision,
  };
}

function isCalendarEventRelevantForApplication(
  event: GoogleCalendarEvent,
  content: CalendarApplicationContent,
  state: CalendarSyncState,
): boolean {
  if (state.phase === "live") return true;
  // initialTimeMin is anchored once per initial scan. Deriving the observation
  // boundary from it keeps the decision stable across page/application retries.
  const observationBoundary = Date.parse(state.initialTimeMin) + 90 * DAY_MS;
  if (Date.parse(content.endsAt) >= observationBoundary) return true;
  const providerChangedAt = Date.parse(event.updatedAt ?? event.createdAt ?? "");
  return Number.isFinite(providerChangedAt) && providerChangedAt >= observationBoundary;
}

function eventApplicationTimeZone(
  event: GoogleCalendarEvent,
  calendarTimeZone: string | null,
): string | null {
  const requested =
    event.start?.kind === "dateTime" && event.start.timeZone !== null
      ? event.start.timeZone
      : event.end?.kind === "dateTime" && event.end.timeZone !== null
        ? event.end.timeZone
        : calendarTimeZone;
  if (requested === null || /^[+-]\d/u.test(requested)) return null;
  try {
    const canonical = new Intl.DateTimeFormat("en-US", { timeZone: requested }).resolvedOptions().timeZone;
    if (/^[+-]\d/u.test(canonical)) return null;
    return TimeZoneSchema.parse(canonical);
  } catch {
    return null;
  }
}

function boundedCalendarText(value: string, maxLength: number): string {
  if (value.length <= maxLength) return value;
  const bounded = value.slice(0, maxLength);
  const last = bounded.charCodeAt(bounded.length - 1);
  return last >= 0xd800 && last <= 0xdbff ? bounded.slice(0, -1) : bounded;
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

function connectionCalendarResult(
  connection: GoogleSyncConnection,
  status: CalendarSyncResult["status"],
): CalendarSyncResult {
  return {
    status,
    connectionId: connection.id,
    householdId: connection.householdId,
    adultId: connection.adultId,
    calendarId: null,
    phase: "catalog",
    processedEvents: 0,
  };
}

export function calendarCatalogNameAad(
  connection: Pick<GoogleSyncConnection, "householdId" | "adultId" | "id">,
  providerCalendarId: string,
): string {
  return `google-calendar-name:${connection.householdId}:${connection.adultId}:${connection.id}:${providerCalendarId}`;
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
