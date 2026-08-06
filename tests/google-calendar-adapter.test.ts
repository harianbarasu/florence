import type { OAuth2Client } from "google-auth-library";
import { beforeEach, describe, expect, it, vi } from "vitest";

const { listEvents } = vi.hoisted(() => ({ listEvents: vi.fn() }));

vi.mock("googleapis", () => ({
  google: {
    calendar: () => ({
      calendarList: { list: vi.fn() },
      events: { list: listEvents },
    }),
  },
}));

import { CalendarAdapter } from "../src/adapters/google/calendar.js";

describe("CalendarAdapter", () => {
  beforeEach(() => {
    listEvents.mockReset();
  });

  it("preserves cancelled sync tombstones that omit start and end", async () => {
    listEvents.mockResolvedValue({
      data: {
        items: [
          {
            id: "cancelled-event",
            status: "cancelled",
            updated: "2026-08-06T12:00:00Z",
          },
          {
            id: "incomplete-confirmed-event",
            status: "confirmed",
          },
        ],
        nextSyncToken: "next-sync-token",
      },
    });
    const adapter = new CalendarAdapter({} as OAuth2Client);

    const page = await adapter.listEvents({
      calendarId: "family@example.test",
      syncToken: "prior-sync-token",
    });

    expect(page.events).toEqual([
      {
        id: "cancelled-event",
        calendarId: "family@example.test",
        etag: null,
        status: "cancelled",
        summary: null,
        description: null,
        location: null,
        start: "",
        end: "",
        timezone: null,
        recurringEventId: null,
        updatedAt: new Date("2026-08-06T12:00:00Z"),
        attendees: [],
      },
    ]);
    expect(page.nextSyncToken).toBe("next-sync-token");
  });
});
