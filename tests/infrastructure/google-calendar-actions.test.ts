import { describe, expect, it, vi } from "vitest";
import {
  GOOGLE_CALENDAR_EVENTS_SCOPE,
  type GoogleCalendarEvent,
  type GoogleTokenSet,
} from "../../src/adapters/google/index.js";
import { CalendarEventCreateActionSchema, calendarEventCreateActionDigest } from "../../src/domain/index.js";
import {
  type GoogleSyncConnection,
  googleConnectionCredentialsAad,
} from "../../src/infrastructure/google-sync.js";
import {
  calendarSourceContentAad,
  GoogleCalendarActionError,
  type GoogleCalendarActionStore,
  GoogleCalendarActions,
} from "../../src/infrastructure/index.js";
import { SecretBox } from "../../src/security/secret-box.js";

const HOUSEHOLD_ID = "10000000-0000-4000-8000-000000000001";
const ADULT_A = "20000000-0000-4000-8000-000000000001";
const ADULT_B = "20000000-0000-4000-8000-000000000002";
const CONNECTION_ID = "30000000-0000-4000-8000-000000000001";
const AS_OF = "2027-09-01T17:02:05Z";
const AVAILABILITY_DIGEST = `sha256:${"d".repeat(64)}`;

function tokens(overrides: Partial<GoogleTokenSet> = {}): GoogleTokenSet {
  return {
    accessToken: "access-token",
    refreshToken: "refresh-token",
    idToken: "identity-token",
    expiresAt: "2027-09-01T19:00:00Z",
    scope: [GOOGLE_CALENDAR_EVENTS_SCOPE],
    tokenType: "Bearer",
    ...overrides,
  };
}

function event(): GoogleCalendarEvent {
  return {
    schemaVersion: 1,
    source: "google-calendar",
    sourceScope: "personal",
    googleSubject: "google-subject-parent",
    calendarId: "primary",
    sourceKey: "google-subject-parent:primary:provider-event-1",
    changeKey: "google-calendar-change:one",
    eventId: "provider-event-1",
    etag: '"etag-1"',
    iCalUid: "ical-1",
    status: "confirmed",
    updatedAt: "2027-09-01T17:02:04Z",
    createdAt: "2027-09-01T17:02:03Z",
    sequence: 0,
    summary: "School welcome night",
    description: null,
    location: null,
    start: { kind: "dateTime", dateTime: "2027-09-08T01:00:00Z", timeZone: "America/Los_Angeles" },
    end: { kind: "dateTime", dateTime: "2027-09-08T02:30:00Z", timeZone: "America/Los_Angeles" },
    recurringEventId: null,
    originalStart: null,
    recurrence: [],
    organizer: null,
    creator: null,
    attendees: [],
    transparency: null,
    visibility: "default",
    htmlLink: "https://calendar.google.com/event?eid=provider-event-1",
    deleted: false,
  };
}

function action() {
  const withoutDigest = {
    actionId: "action_calendar_school_night",
    kind: "calendar_update" as const,
    calendarActionVersion: 1 as const,
    operation: "create" as const,
    householdId: HOUSEHOLD_ID,
    summary: "create the approved household calendar event",
    relevantDataDigest: AVAILABILITY_DIGEST,
    requestedFor: { kind: "household" as const },
    evidence: [
      {
        evidenceId: "evidence_calendar_school_night",
        source: "linq" as const,
        sourceRef: "message_calendar_school_night",
        scope: { kind: "household" as const },
        observedAt: "2027-09-01T17:00:00Z",
        revision: 1,
      },
    ],
    title: "School welcome night",
    startsAt: "2027-09-08T01:00:00Z",
    endsAt: "2027-09-08T02:30:00Z",
    timeZone: "America/Los_Angeles",
    requestedByAdultId: ADULT_A,
    availabilityAdultIds: [ADULT_A, ADULT_B],
    targetConnectionId: CONNECTION_ID,
    calendarId: "primary" as const,
    hasConflict: true,
  };
  return CalendarEventCreateActionSchema.parse({
    ...withoutDigest,
    actionDigest: calendarEventCreateActionDigest(withoutDigest),
  });
}

function harness() {
  const secretBox = new SecretBox("0".repeat(64));
  const connection: GoogleSyncConnection = {
    id: CONNECTION_ID,
    householdId: HOUSEHOLD_ID,
    adultId: ADULT_A,
    provider: "google",
    externalAccountId: "google-subject-parent",
    email: "parent@example.test",
    encryptedCredentials: null,
    grantedScopes: [GOOGLE_CALENDAR_EVENTS_SCOPE],
    status: "active",
    cursor: {},
    metadata: { credentialAadVersion: 1 },
  };
  connection.encryptedCredentials = secretBox.seal(
    JSON.stringify(tokens()),
    googleConnectionCredentialsAad(connection),
  );
  const persisted: Parameters<GoogleCalendarActionStore["persistPersonalCalendarSource"]>[0][] = [];
  const prepareCreate = vi.fn<GoogleCalendarActionStore["prepareCreate"]>(async () => ({
    status: "ready",
    targetConnectionId: CONNECTION_ID,
    calendarId: "primary",
    relevantDataDigest: AVAILABILITY_DIGEST,
    hasConflict: true,
  }));
  const store: GoogleCalendarActionStore = {
    prepareCreate,
    getOwnedGoogleConnection: vi.fn(async () => structuredClone(connection)),
    replaceEncryptedCredentials: vi.fn(async () => "updated" as const),
    markConnectionStatus: vi.fn(async () => "updated" as const),
    async persistPersonalCalendarSource(input) {
      persisted.push(structuredClone(input));
      return { sourceItemId: "source-calendar-created", disposition: "inserted", revision: 1 };
    },
  };
  const insertEvent = vi.fn(async () => event());
  const refresh = vi.fn(async () => tokens());
  const service = new GoogleCalendarActions({
    store,
    calendar: { insertEvent },
    oauth: { refresh },
    secretBox,
    now: () => new Date(AS_OF),
  });
  return { service, store, secretBox, connection, prepareCreate, persisted, insertEvent, refresh };
}

describe("GoogleCalendarActions", () => {
  it("revalidates the opaque household projection, writes idempotently, and stores raw output privately", async () => {
    const fixture = harness();
    const approved = action();

    await expect(
      fixture.service.createApprovedEvent({
        action: approved,
        idempotencyKey: "florence:approved-calendar-action",
        asOf: AS_OF,
      }),
    ).resolves.toEqual({
      provider: "google-calendar",
      providerReference: "google-calendar:primary:provider-event-1",
    });

    expect(fixture.prepareCreate).toHaveBeenCalledWith({
      householdId: HOUSEHOLD_ID,
      verifiedAdultIds: [ADULT_A, ADULT_B],
      requestedByAdultId: ADULT_A,
      asOf: AS_OF,
      startsAt: approved.startsAt,
      endsAt: approved.endsAt,
      targetConnectionId: CONNECTION_ID,
    });
    expect(fixture.insertEvent).toHaveBeenCalledWith({
      accessToken: "access-token",
      googleSubject: "google-subject-parent",
      calendarId: "primary",
      idempotencyKey: "florence:approved-calendar-action",
      sendUpdates: "none",
      summary: "School welcome night",
      start: { dateTime: "2027-09-08T01:00:00Z", timeZone: "America/Los_Angeles" },
      end: { dateTime: "2027-09-08T02:30:00Z", timeZone: "America/Los_Angeles" },
      visibility: "default",
    });
    expect(fixture.persisted).toHaveLength(1);
    expect(fixture.persisted[0]).toMatchObject({
      householdId: HOUSEHOLD_ID,
      adultId: ADULT_A,
      connectionId: CONNECTION_ID,
      kind: "calendar_event",
      busyWindow: {
        startsAt: "2027-09-08T01:00:00Z",
        endsAt: "2027-09-08T02:30:00Z",
        allDay: false,
      },
      metadata: { sourceScope: "personal", createdByApprovedActionId: approved.actionId },
    });
    const encrypted = fixture.persisted[0]?.encryptedContent;
    expect(encrypted).not.toContain("School welcome night");
    expect(
      fixture.secretBox.open(
        encrypted as string,
        calendarSourceContentAad(fixture.connection, "primary", "provider-event-1"),
      ),
    ).toContain("School welcome night");
    expect(fixture.refresh).not.toHaveBeenCalled();
  });

  it("fails before provider access when the approval's availability version changed", async () => {
    const fixture = harness();
    fixture.prepareCreate.mockResolvedValueOnce({
      status: "ready",
      targetConnectionId: CONNECTION_ID,
      calendarId: "primary",
      relevantDataDigest: `sha256:${"e".repeat(64)}`,
      hasConflict: false,
    });

    await expect(
      fixture.service.createApprovedEvent({
        action: action(),
        idempotencyKey: "florence:stale-calendar-action",
        asOf: AS_OF,
      }),
    ).rejects.toEqual(new GoogleCalendarActionError("approval_invalidated", false));
    expect(fixture.insertEvent).not.toHaveBeenCalled();
    expect(fixture.persisted).toEqual([]);
  });

  it("refreshes an expiring write grant and persists the replacement before insertion", async () => {
    const fixture = harness();
    const expiring = tokens({ expiresAt: "2027-09-01T17:03:00Z" });
    fixture.connection.encryptedCredentials = fixture.secretBox.seal(
      JSON.stringify(expiring),
      googleConnectionCredentialsAad(fixture.connection),
    );
    vi.mocked(fixture.store.getOwnedGoogleConnection).mockResolvedValueOnce(
      structuredClone(fixture.connection),
    );
    fixture.refresh.mockResolvedValueOnce(tokens({ accessToken: "refreshed-access-token" }));

    await fixture.service.createApprovedEvent({
      action: action(),
      idempotencyKey: "florence:refresh-calendar-action",
      asOf: AS_OF,
    });

    expect(fixture.refresh).toHaveBeenCalledOnce();
    expect(fixture.store.replaceEncryptedCredentials).toHaveBeenCalledOnce();
    expect(fixture.insertEvent).toHaveBeenCalledWith(
      expect.objectContaining({ accessToken: "refreshed-access-token" }),
    );
  });
});
