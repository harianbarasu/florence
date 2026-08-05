import { describe, expect, it, vi } from "vitest";
import {
  GOOGLE_CALENDAR_READONLY_SCOPE,
  type GoogleCalendarEvent,
  GoogleSyncTokenExpiredError,
  type GoogleTokenSet,
  googleCalendarEventSchema,
} from "../../src/adapters/google/index.js";
import {
  type ApplicationResult,
  ApplicationResultSchema,
  type FlorenceApplication,
} from "../../src/application/index.js";
import {
  type CalendarProviderPort,
  type CalendarSyncRepositoryPort,
  calendarApplicationContent,
  calendarApplicationContentDigest,
  calendarSourceContentAad,
  GoogleCalendarPushIngress,
  GoogleCalendarSyncService,
  normalizeCalendarBusyWindow,
  type PersistPersonalCalendarSourceInput,
} from "../../src/infrastructure/google-calendar-sync.js";
import {
  type GoogleSyncConnection,
  googleConnectionCredentialsAad,
} from "../../src/infrastructure/google-sync.js";
import { SecretBox } from "../../src/security/secret-box.js";

const NOW = new Date("2027-02-01T08:00:00.000Z");
const HOUSEHOLD_ID = "11111111-1111-4111-8111-111111111111";
const ADULT_ID = "22222222-2222-4222-8222-222222222222";
const CONNECTION_ID = "33333333-3333-4333-8333-333333333333";
const SECRET_KEY = Buffer.alloc(32, 0x35).toString("base64url");

function event(overrides: Partial<GoogleCalendarEvent> = {}): GoogleCalendarEvent {
  return googleCalendarEventSchema.parse({
    schemaVersion: 1,
    source: "google-calendar",
    sourceScope: "personal",
    googleSubject: "google-subject-parent",
    calendarId: "primary",
    sourceKey: "google-subject-parent:primary:event-1",
    changeKey: "google-calendar-change:one",
    eventId: "event-1",
    etag: '"etag-1"',
    iCalUid: "event-1@example.test",
    status: "confirmed",
    updatedAt: "2027-02-01T07:30:00Z",
    createdAt: "2027-01-01T00:00:00Z",
    sequence: 1,
    summary: "Private medical appointment",
    description: "Private details",
    location: "Private clinic",
    start: { kind: "dateTime", dateTime: "2027-02-02T09:00:00-08:00", timeZone: null },
    end: { kind: "dateTime", dateTime: "2027-02-02T10:00:00-08:00", timeZone: null },
    recurringEventId: null,
    originalStart: null,
    recurrence: [],
    organizer: null,
    creator: null,
    attendees: [],
    transparency: "opaque",
    visibility: "private",
    htmlLink: "https://calendar.google.com/event?eid=private",
    deleted: false,
    ...overrides,
  });
}

function tokenSet(): GoogleTokenSet {
  return {
    accessToken: "access-token",
    refreshToken: "refresh-token",
    idToken: "id-token",
    expiresAt: "2027-02-01T10:00:00Z",
    scope: [GOOGLE_CALENDAR_READONLY_SCOPE],
    tokenType: "Bearer",
  };
}

function connection(secretBox: SecretBox): GoogleSyncConnection {
  const record: GoogleSyncConnection = {
    id: CONNECTION_ID,
    householdId: HOUSEHOLD_ID,
    adultId: ADULT_ID,
    provider: "google",
    externalAccountId: "google-subject-parent",
    email: "parent@example.test",
    encryptedCredentials: null,
    grantedScopes: [GOOGLE_CALENDAR_READONLY_SCOPE],
    status: "active",
    cursor: {},
    metadata: { credentialAadVersion: 1 },
  };
  record.encryptedCredentials = secretBox.seal(
    JSON.stringify(tokenSet()),
    googleConnectionCredentialsAad(record),
  );
  return record;
}

function harness() {
  const secretBox = new SecretBox(SECRET_KEY);
  const owned = connection(secretBox);
  const sources: PersistPersonalCalendarSourceInput[] = [];
  const sourceRecords = new Map<string, { contentHash: string; kind: string; revision: number }>();
  const restart = vi.fn(async (input: Parameters<CalendarSyncRepositoryPort["restartCalendarSync"]>[0]) => {
    owned.cursor.calendar = structuredClone(input.state);
    return "updated" as const;
  });
  const persistPersonalCalendarSource = vi.fn<CalendarSyncRepositoryPort["persistPersonalCalendarSource"]>(
    async (input) => {
      sources.push(structuredClone(input));
      const key = `${input.calendarId}:${input.externalId}`;
      const prior = sourceRecords.get(key);
      const unchanged = prior?.contentHash === input.contentHash && prior.kind === input.kind;
      const revision = prior === undefined ? 1 : unchanged ? prior.revision : prior.revision + 1;
      sourceRecords.set(key, { contentHash: input.contentHash, kind: input.kind, revision });
      return {
        sourceItemId: `source-${key}`,
        disposition: prior === undefined ? "inserted" : unchanged ? "unchanged" : "revised",
        revision,
        createdByApprovedActionId: null,
      };
    },
  );
  const repository: CalendarSyncRepositoryPort = {
    async saveCalendarSyncState(input) {
      const revision = Number((owned.cursor.calendar as { revision?: number } | undefined)?.revision ?? 0);
      if (revision !== input.expectedRevision) return "conflict";
      owned.cursor.calendar = structuredClone(input.state);
      return "updated";
    },
    restartCalendarSync: restart,
    persistPersonalCalendarSource,
    async replaceEncryptedCredentials(input) {
      owned.encryptedCredentials = input.encryptedCredentials;
      owned.grantedScopes = [...input.grantedScopes];
      return "updated";
    },
    async markConnectionStatus(input) {
      owned.status = input.status;
      return "updated";
    },
    async replaceCalendarWatch(input) {
      const revision = Number((owned.cursor.calendar as { revision?: number }).revision ?? 0);
      if (revision !== input.expectedRevision) return "conflict";
      owned.cursor.calendar = structuredClone(input.state);
      return "updated";
    },
    async markCalendarWatchStopped() {
      return "updated";
    },
    async revokeConnection() {
      owned.status = "revoked";
      owned.encryptedCredentials = null;
      return "revoked";
    },
  };
  const listEventsPage = vi.fn<CalendarProviderPort["listEventsPage"]>(async () => ({
    events: [event()],
    nextPageToken: null,
    nextSyncToken: "sync-token-1",
    timeZone: "America/Los_Angeles",
  }));
  const watchEvents = vi.fn<CalendarProviderPort["watchEvents"]>(async () => ({
    channelId: "calendar-channel-1",
    resourceId: "calendar-resource-1",
    resourceUri: "https://www.googleapis.com/calendar/v3/calendars/primary/events",
    expiresAt: "2027-02-07T08:00:00Z",
  }));
  const stopChannel = vi.fn<CalendarProviderPort["stopChannel"]>(async () => undefined);
  const applicationProcess = vi.fn(async (input: unknown): Promise<ApplicationResult> => {
    const identity = input as { householdId: string; idempotencyKey: string };
    return ApplicationResultSchema.parse({
      householdId: identity.householdId,
      idempotencyKey: identity.idempotencyKey,
      disposition: "committed",
      revision: 1,
      outcome: {
        status: "processed",
        classification: "calendar:test",
        domainReceipts: [],
        outboxIntentIds: [],
      },
    });
  });
  const service = new GoogleCalendarSyncService({
    directory: { getOwnedGoogleConnection: async () => owned },
    repository,
    calendar: { listEventsPage, watchEvents, stopChannel },
    oauth: { refresh: vi.fn(async () => tokenSet()), revoke: vi.fn(async () => undefined) },
    application: { process: applicationProcess as FlorenceApplication["process"] },
    secretBox,
    publicBaseUrl: "https://florence.example.test",
    now: () => NOW,
  });
  const identity = {
    householdId: HOUSEHOLD_ID,
    adultId: ADULT_ID,
    connectionId: CONNECTION_ID,
    calendarId: "primary",
  };
  return {
    secretBox,
    owned,
    sources,
    repository,
    persistPersonalCalendarSource,
    restart,
    listEventsPage,
    watchEvents,
    stopChannel,
    applicationProcess,
    service,
    identity,
  };
}

describe("GoogleCalendarSyncService", () => {
  it("normalizes all-day windows through DST using the calendar time zone", () => {
    const allDay = event({
      start: { kind: "date", date: "2027-03-14", timeZone: null },
      end: { kind: "date", date: "2027-03-15", timeZone: null },
    });
    expect(normalizeCalendarBusyWindow(allDay, "America/Los_Angeles")).toEqual({
      startsAt: "2027-03-14T08:00:00Z",
      endsAt: "2027-03-15T07:00:00Z",
      allDay: true,
    });
    expect(normalizeCalendarBusyWindow({ ...allDay, transparency: "transparent" }, "UTC")).toBeNull();
  });

  it("performs a bounded initial page, encrypts raw events, and exposes only a busy projection", async () => {
    const fixture = harness();
    await expect(fixture.service.execute({ kind: "start", ...fixture.identity })).resolves.toMatchObject({
      status: "continuation_required",
      phase: "initial",
    });
    await expect(fixture.service.execute({ kind: "continue", ...fixture.identity })).resolves.toMatchObject({
      status: "continuation_required",
      phase: "live",
      processedEvents: 1,
    });

    expect(fixture.listEventsPage).toHaveBeenCalledWith(
      expect.objectContaining({
        calendarId: "primary",
        maxResults: 25,
        singleEvents: true,
        timeMin: "2026-11-03T08:00:00.000Z",
        timeMax: "2028-07-25T08:00:00.000Z",
      }),
    );
    const persisted = fixture.sources[0];
    if (!persisted) throw new Error("Expected a persisted Calendar source");
    expect(persisted.busyWindow).toEqual({
      startsAt: "2027-02-02T17:00:00Z",
      endsAt: "2027-02-02T18:00:00Z",
      allDay: false,
    });
    expect(persisted.encryptedContent).not.toContain("Private medical appointment");
    expect(
      fixture.secretBox.open(
        persisted.encryptedContent,
        calendarSourceContentAad(fixture.owned, "primary", "event-1"),
      ),
    ).toContain("Private medical appointment");
    expect(JSON.stringify(persisted.busyWindow)).not.toMatch(/summary|description|location|attendee/iu);
    const content = calendarApplicationContent(event(), "America/Los_Angeles");
    if (content === null) throw new Error("Expected normalized Calendar application content");
    expect(fixture.applicationProcess).toHaveBeenCalledWith({
      kind: "calendar_event",
      householdId: HOUSEHOLD_ID,
      idempotencyKey: `calendar:${CONNECTION_ID}:primary:event-1:revision:1`,
      occurredAt: "2027-02-01T08:00:00Z",
      ownerAdultId: ADULT_ID,
      accountRef: `google:${CONNECTION_ID}`,
      eventRef: `calendar:${CONNECTION_ID}:primary:event-1`,
      providerRef: `google-calendar:${CONNECTION_ID}:primary:event-1`,
      revision: 1,
      contentDigest: calendarApplicationContentDigest(content),
      ...content,
    });
    expect(persisted.metadata).toMatchObject({
      applicationContentDigest: calendarApplicationContentDigest(content),
    });
    expect(JSON.stringify(fixture.applicationProcess.mock.calls)).not.toMatch(
      /etag-1|google-calendar-change|event\?eid|google-subject-parent|organizer|creator|attendees|visibility/iu,
    );
  });

  it("persists malformed and historical initial events privately without flooding Calendar triage", async () => {
    const fixture = harness();
    fixture.listEventsPage.mockResolvedValueOnce({
      events: [
        event({
          eventId: "malformed-event",
          summary: "   ",
          updatedAt: NOW.toISOString(),
        }),
        event({
          eventId: "old-event",
          summary: "Old private event",
          start: { kind: "dateTime", dateTime: "2027-01-01T09:00:00-08:00", timeZone: null },
          end: { kind: "dateTime", dateTime: "2027-01-01T10:00:00-08:00", timeZone: null },
          updatedAt: "2027-01-01T18:00:00Z",
        }),
        event({
          eventId: "recently-edited-old-event",
          summary: "Recently edited old event",
          start: { kind: "dateTime", dateTime: "2027-01-02T09:00:00-08:00", timeZone: null },
          end: { kind: "dateTime", dateTime: "2027-01-02T10:00:00-08:00", timeZone: null },
          updatedAt: NOW.toISOString(),
        }),
      ],
      nextPageToken: null,
      nextSyncToken: "sync-token-initial",
      timeZone: "America/Los_Angeles",
    });

    await fixture.service.execute({ kind: "start", ...fixture.identity });
    await expect(fixture.service.execute({ kind: "continue", ...fixture.identity })).resolves.toMatchObject({
      processedEvents: 3,
    });

    expect(fixture.sources).toHaveLength(3);
    expect(fixture.sources[0]?.metadata).not.toHaveProperty("applicationContentDigest");
    expect(fixture.applicationProcess).toHaveBeenCalledTimes(1);
    expect(fixture.applicationProcess).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: "calendar_event",
        eventRef: `calendar:${CONNECTION_ID}:primary:recently-edited-old-event`,
      }),
    );
  });

  it("retries application reconciliation after source persistence committed unchanged", async () => {
    const fixture = harness();
    fixture.applicationProcess.mockRejectedValueOnce(new Error("application unavailable"));
    await fixture.service.execute({ kind: "start", ...fixture.identity });

    await expect(fixture.service.execute({ kind: "continue", ...fixture.identity })).rejects.toMatchObject({
      code: "invalid_state",
      retryable: true,
    });
    await expect(fixture.service.execute({ kind: "continue", ...fixture.identity })).resolves.toMatchObject({
      status: "continuation_required",
      processedEvents: 1,
    });

    expect(fixture.persistPersonalCalendarSource).toHaveBeenCalledTimes(2);
    expect(fixture.applicationProcess).toHaveBeenCalledTimes(2);
    expect(fixture.applicationProcess.mock.calls[0]?.[0]).toEqual(
      fixture.applicationProcess.mock.calls[1]?.[0],
    );
    expect(fixture.applicationProcess.mock.calls[1]?.[0]).toMatchObject({
      idempotencyKey: `calendar:${CONNECTION_ID}:primary:event-1:revision:1`,
      revision: 1,
    });
  });

  it("suppresses only an exact approved-action echo, then reconciles an edit and deletion", async () => {
    const fixture = harness();
    fixture.persistPersonalCalendarSource
      .mockResolvedValueOnce({
        sourceItemId: "source-action-event",
        disposition: "revised",
        revision: 2,
        createdByApprovedActionId: "approved-action-1",
      })
      .mockResolvedValueOnce({
        sourceItemId: "source-action-event",
        disposition: "revised",
        revision: 3,
        createdByApprovedActionId: "approved-action-1",
      })
      .mockResolvedValueOnce({
        sourceItemId: "source-action-event",
        disposition: "revised",
        revision: 4,
        createdByApprovedActionId: null,
      })
      .mockResolvedValueOnce({
        sourceItemId: "source-action-event",
        disposition: "revised",
        revision: 5,
        createdByApprovedActionId: null,
      });
    await fixture.service.execute({ kind: "start", ...fixture.identity });
    await fixture.service.execute({ kind: "continue", ...fixture.identity });
    expect(fixture.applicationProcess).not.toHaveBeenCalled();

    fixture.listEventsPage.mockResolvedValueOnce({
      events: [event({ etag: '"provider-only-etag"', changeKey: "google-calendar-change:two" })],
      nextPageToken: null,
      nextSyncToken: "sync-token-2",
      timeZone: "America/Los_Angeles",
    });
    await fixture.service.execute({ kind: "push", ...fixture.identity });
    expect(fixture.applicationProcess).not.toHaveBeenCalled();

    fixture.listEventsPage.mockResolvedValueOnce({
      events: [event({ summary: "Meaningfully edited appointment" })],
      nextPageToken: null,
      nextSyncToken: "sync-token-3",
      timeZone: "America/Los_Angeles",
    });
    await fixture.service.execute({ kind: "push", ...fixture.identity });
    expect(fixture.applicationProcess).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: "calendar_event",
        revision: 4,
        title: "Meaningfully edited appointment",
      }),
    );

    fixture.listEventsPage.mockResolvedValueOnce({
      events: [
        event({
          status: "cancelled",
          deleted: true,
          summary: "",
          description: null,
          location: null,
          start: null,
          end: null,
        }),
      ],
      nextPageToken: null,
      nextSyncToken: "sync-token-4",
      timeZone: "America/Los_Angeles",
    });
    await fixture.service.execute({ kind: "push", ...fixture.identity });
    expect(fixture.applicationProcess).toHaveBeenLastCalledWith({
      kind: "calendar_event_deleted",
      householdId: HOUSEHOLD_ID,
      idempotencyKey: `calendar:${CONNECTION_ID}:primary:event-1:revision:5`,
      occurredAt: "2027-02-01T08:00:00Z",
      ownerAdultId: ADULT_ID,
      accountRef: `google:${CONNECTION_ID}`,
      eventRef: `calendar:${CONNECTION_ID}:primary:event-1`,
      providerRef: `google-calendar:${CONNECTION_ID}:primary:event-1`,
      revision: 5,
    });
    expect(fixture.applicationProcess).toHaveBeenCalledTimes(2);
  });

  it("uses the live sync token, replaces watches, and never persists the channel token", async () => {
    const fixture = harness();
    await fixture.service.execute({ kind: "start", ...fixture.identity });
    await fixture.service.execute({ kind: "continue", ...fixture.identity });
    await expect(
      fixture.service.execute({ kind: "renew_watch", ...fixture.identity }),
    ).resolves.toMatchObject({ status: "processed" });
    const watchCall = fixture.watchEvents.mock.calls[0]?.[0];
    expect(watchCall).toMatchObject({
      address: "https://florence.example.test/webhooks/google/calendar",
      channelId: expect.any(String),
      channelToken: expect.any(String),
    });
    expect(JSON.stringify(fixture.owned.cursor)).not.toContain(watchCall?.channelToken);

    fixture.listEventsPage.mockResolvedValueOnce({
      events: [],
      nextPageToken: null,
      nextSyncToken: "sync-token-2",
      timeZone: "America/Los_Angeles",
    });
    await fixture.service.execute({ kind: "push", ...fixture.identity });
    expect(fixture.listEventsPage).toHaveBeenLastCalledWith(
      expect.objectContaining({ syncToken: "sync-token-1" }),
    );
  });

  it("stops a newly created remote channel when the durable watch transaction fails", async () => {
    const fixture = harness();
    await fixture.service.execute({ kind: "start", ...fixture.identity });
    await fixture.service.execute({ kind: "continue", ...fixture.identity });
    fixture.repository.replaceCalendarWatch = vi.fn(async () => {
      throw new Error("database unavailable");
    });

    await expect(fixture.service.execute({ kind: "renew_watch", ...fixture.identity })).rejects.toMatchObject(
      { code: "invalid_state" },
    );
    expect(fixture.stopChannel).toHaveBeenCalledWith({
      accessToken: "access-token",
      channelId: "calendar-channel-1",
      resourceId: "calendar-resource-1",
    });
  });

  it("resets only the Calendar cursor and projection after a 410", async () => {
    const fixture = harness();
    await fixture.service.execute({ kind: "start", ...fixture.identity });
    await fixture.service.execute({ kind: "continue", ...fixture.identity });
    fixture.listEventsPage.mockRejectedValueOnce(new GoogleSyncTokenExpiredError("calendar"));

    await expect(fixture.service.execute({ kind: "scheduled", ...fixture.identity })).resolves.toMatchObject({
      status: "continuation_required",
      phase: "initial",
    });
    expect(fixture.restart).toHaveBeenCalledWith(
      expect.objectContaining({
        expectedRevision: 4,
        state: expect.objectContaining({
          revision: 5,
          syncToken: null,
          phase: "initial",
          projectionReady: false,
        }),
      }),
    );
  });

  it("does not call Google after the owned connection is revoked", async () => {
    const fixture = harness();
    fixture.owned.status = "revoked";
    fixture.owned.encryptedCredentials = null;
    await expect(fixture.service.execute({ kind: "continue", ...fixture.identity })).resolves.toMatchObject({
      status: "revoked",
    });
    expect(fixture.listEventsPage).not.toHaveBeenCalled();
  });

  it("stops the owned Calendar watch before making local revocation authoritative", async () => {
    const fixture = harness();
    await fixture.service.execute({ kind: "start", ...fixture.identity });
    await fixture.service.execute({ kind: "continue", ...fixture.identity });
    await fixture.service.execute({ kind: "renew_watch", ...fixture.identity });

    await expect(fixture.service.execute({ kind: "revoke", ...fixture.identity })).resolves.toMatchObject({
      status: "revoked",
    });
    expect(fixture.stopChannel).toHaveBeenLastCalledWith({
      accessToken: "access-token",
      channelId: "calendar-channel-1",
      resourceId: "calendar-resource-1",
    });
    expect(fixture.owned).toMatchObject({ status: "revoked", encryptedCredentials: null });
  });
});

describe("GoogleCalendarPushIngress", () => {
  const headers = {
    "x-goog-channel-id": "calendar-channel-1",
    "x-goog-channel-token": "private-channel-token",
    "x-goog-resource-id": "calendar-resource-1",
    "x-goog-resource-uri": "https://www.googleapis.com/calendar/v3/calendars/primary/events",
    "x-goog-resource-state": "exists",
    "x-goog-message-number": "42",
  };

  it("authenticates dynamically and persists only a scoped invalidation job", async () => {
    const enqueueCalendarSyncWork = vi.fn(async () => ({ jobId: "job-1", created: true }));
    const authenticateCalendarPush = vi.fn(async () => ({
      householdId: HOUSEHOLD_ID,
      adultId: ADULT_ID,
      connectionId: CONNECTION_ID,
      calendarId: "primary",
    }));
    const ingress = new GoogleCalendarPushIngress({
      store: { authenticateCalendarPush, enqueueCalendarSyncWork },
      now: () => NOW,
    });

    await expect(ingress.accept(headers)).resolves.toBe("accepted");
    expect(enqueueCalendarSyncWork).toHaveBeenCalledWith({
      householdId: HOUSEHOLD_ID,
      idempotencyKey: "google-calendar-push:calendar-channel-1:calendar-resource-1:42",
      work: {
        kind: "push",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
        calendarId: "primary",
      },
    });
    expect(JSON.stringify(enqueueCalendarSyncWork.mock.calls)).not.toContain("private-channel-token");
  });

  it("does not enqueue an unknown or mistokened channel", async () => {
    const enqueueCalendarSyncWork = vi.fn(async () => ({ jobId: "job-1", created: true }));
    const ingress = new GoogleCalendarPushIngress({
      store: {
        authenticateCalendarPush: async () => null,
        enqueueCalendarSyncWork,
      },
    });
    await expect(ingress.accept(headers)).resolves.toBe("unauthorized");
    await expect(ingress.accept({ ...headers, "x-goog-channel-token": "" })).resolves.toBe("unauthorized");
    expect(enqueueCalendarSyncWork).not.toHaveBeenCalled();
  });
});
