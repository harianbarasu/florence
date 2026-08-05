import { describe, expect, it, vi } from "vitest";
import {
  GOOGLE_CALENDAR_READONLY_SCOPE,
  type GoogleCalendarEvent,
  GoogleSyncTokenExpiredError,
  type GoogleTokenSet,
  googleCalendarEventSchema,
} from "../../src/adapters/google/index.js";
import {
  type CalendarProviderPort,
  type CalendarSyncRepositoryPort,
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
  const restart = vi.fn(async (input: Parameters<CalendarSyncRepositoryPort["restartCalendarSync"]>[0]) => {
    owned.cursor.calendar = structuredClone(input.state);
    return "updated" as const;
  });
  const repository: CalendarSyncRepositoryPort = {
    async saveCalendarSyncState(input) {
      const revision = Number((owned.cursor.calendar as { revision?: number } | undefined)?.revision ?? 0);
      if (revision !== input.expectedRevision) return "conflict";
      owned.cursor.calendar = structuredClone(input.state);
      return "updated";
    },
    restartCalendarSync: restart,
    async persistPersonalCalendarSource(input) {
      sources.push(structuredClone(input));
      return { sourceItemId: `source-${sources.length}`, disposition: "inserted", revision: 1 };
    },
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
  const service = new GoogleCalendarSyncService({
    directory: { getOwnedGoogleConnection: async () => owned },
    repository,
    calendar: { listEventsPage, watchEvents, stopChannel },
    oauth: { refresh: vi.fn(async () => tokenSet()), revoke: vi.fn(async () => undefined) },
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
    restart,
    listEventsPage,
    watchEvents,
    stopChannel,
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
