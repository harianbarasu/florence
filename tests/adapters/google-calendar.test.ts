import type { calendar_v3 } from "googleapis";
import { describe, expect, it, vi } from "vitest";
import {
  type CalendarApiFactory,
  calendarEventIdFromIdempotencyKey,
  GoogleCalendarAdapter,
  GoogleSyncTokenExpiredError,
  parseGoogleAdapterConfig,
} from "../../src/adapters/google/index.js";
import { jsonFixture } from "./fixture.js";

const CONFIG = parseGoogleAdapterConfig({
  clientId: "synthetic-client-id",
  clientSecret: "synthetic-client-secret",
  redirectUri: "https://florence.example.test/oauth/google/callback",
});

const CALENDAR_EVENT = jsonFixture<calendar_v3.Schema$Event>("google/calendar-event.json");

function calendarFactory(overrides: Record<string, unknown> = {}): CalendarApiFactory {
  return () =>
    ({
      listCalendars: vi.fn(async () => ({ data: { items: [], nextSyncToken: "calendar-list-sync-001" } })),
      queryFreeBusy: vi.fn(async () => ({ data: { calendars: { primary: { busy: [] } } } })),
      listEvents: vi.fn(async () => ({ data: { items: [], nextSyncToken: "sync-001" } })),
      getEvent: vi.fn(async () => ({ data: CALENDAR_EVENT })),
      insertEvent: vi.fn(async () => ({ data: CALENDAR_EVENT })),
      patchEvent: vi.fn(async () => ({ data: CALENDAR_EVENT })),
      deleteEvent: vi.fn(async () => undefined),
      watchEvents: vi.fn(async () => ({
        data: {
          id: "channel-001",
          resourceId: "resource-001",
          resourceUri: "https://www.googleapis.com/calendar/v3/calendars/primary/events",
        },
      })),
      stopChannel: vi.fn(async () => undefined),
      ...overrides,
    }) as ReturnType<CalendarApiFactory>;
}

describe("Google Calendar normalization", () => {
  it("maps HTTP 410 to an explicit full-sync requirement", async () => {
    const error = Object.assign(new Error("gone"), { response: { status: 410 } });
    const adapter = new GoogleCalendarAdapter(
      CONFIG,
      calendarFactory({ listEvents: vi.fn(async () => Promise.reject(error)) }),
    );

    await expect(
      adapter.listEventsPage({
        accessToken: "opaque-access-token",
        googleSubject: "google-subject-001",
        calendarId: "primary",
        syncToken: "expired-sync-token",
      }),
    ).rejects.toBeInstanceOf(GoogleSyncTokenExpiredError);
  });

  it("uses a deterministic provider event ID for idempotent insert retries", async () => {
    const insertEvent = vi.fn(
      async (params: { calendarId: string; sendUpdates: string; requestBody: calendar_v3.Schema$Event }) => ({
        data: { ...CALENDAR_EVENT, ...params.requestBody },
      }),
    );
    const adapter = new GoogleCalendarAdapter(CONFIG, calendarFactory({ insertEvent }));
    const input = {
      accessToken: "opaque-access-token",
      googleSubject: "google-subject-001",
      calendarId: "primary",
      idempotencyKey: "approved-effect-001",
      summary: "School orientation",
      start: { dateTime: "2026-08-10T09:00:00-07:00", timeZone: "America/Los_Angeles" },
      end: { dateTime: "2026-08-10T10:00:00-07:00", timeZone: "America/Los_Angeles" },
    } as const;

    const first = await adapter.insertEvent(input);
    const second = await adapter.insertEvent(input);
    const expectedId = calendarEventIdFromIdempotencyKey("approved-effect-001");

    expect(first.eventId).toBe(expectedId);
    expect(second.eventId).toBe(expectedId);
    expect(insertEvent).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        calendarId: "primary",
        sendUpdates: "none",
        requestBody: expect.objectContaining({
          id: expectedId,
          extendedProperties: {
            private: {
              florence_idempotency_hash: "d595c42eaec17bb0ebb0e3dbfa589668d6bc96dfc13079a09baadb54cc565a2b",
            },
          },
        }),
      }),
    );
  });

  it("reconciles an ambiguous insert retry to the original provider event", async () => {
    const providerConflict = Object.assign(new Error("conflict"), { response: { status: 409 } });
    const eventId = calendarEventIdFromIdempotencyKey("approved-effect-001");
    const getEvent = vi.fn(async () => ({
      data: {
        ...CALENDAR_EVENT,
        id: eventId,
        extendedProperties: {
          private: {
            florence_idempotency_hash: "d595c42eaec17bb0ebb0e3dbfa589668d6bc96dfc13079a09baadb54cc565a2b",
          },
        },
      },
    }));
    const adapter = new GoogleCalendarAdapter(
      CONFIG,
      calendarFactory({
        insertEvent: vi.fn(async () => Promise.reject(providerConflict)),
        getEvent,
      }),
    );

    await expect(
      adapter.insertEvent({
        accessToken: "opaque-access-token",
        googleSubject: "google-subject-001",
        calendarId: "primary",
        idempotencyKey: "approved-effect-001",
        summary: "School orientation",
        start: { dateTime: "2026-08-10T09:00:00-07:00" },
        end: { dateTime: "2026-08-10T10:00:00-07:00" },
      }),
    ).resolves.toMatchObject({ eventId });
    expect(getEvent).toHaveBeenCalledWith({ calendarId: "primary", eventId });
  });

  it("treats already-absent delete targets as an idempotent success", async () => {
    const error = Object.assign(new Error("not found"), { response: { status: 404 } });
    const adapter = new GoogleCalendarAdapter(
      CONFIG,
      calendarFactory({ deleteEvent: vi.fn(async () => Promise.reject(error)) }),
    );

    await expect(
      adapter.deleteEvent({
        accessToken: "opaque-access-token",
        calendarId: "primary",
        eventId: "event-001",
        etag: "etag-event-001",
      }),
    ).resolves.toEqual({
      provider: "google-calendar",
      calendarId: "primary",
      eventId: "event-001",
      deleted: true,
    });
  });
});
