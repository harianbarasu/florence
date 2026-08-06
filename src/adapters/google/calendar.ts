import type { OAuth2Client } from "google-auth-library";
import { type calendar_v3, google } from "googleapis";
import type { NormalizedCalendar, NormalizedCalendarEvent } from "./contracts.js";

export interface CalendarListPage {
  calendars: readonly NormalizedCalendar[];
  nextPageToken?: string;
}

export interface CalendarEventPage {
  events: readonly NormalizedCalendarEvent[];
  nextPageToken?: string;
  nextSyncToken?: string;
}

export class CalendarAdapter {
  readonly #calendar: calendar_v3.Calendar;

  public constructor(auth: OAuth2Client) {
    this.#calendar = google.calendar({ version: "v3", auth });
  }

  public async listCalendars(pageToken?: string): Promise<CalendarListPage> {
    const response = await this.#calendar.calendarList.list({
      maxResults: 250,
      showDeleted: true,
      showHidden: true,
      ...(pageToken ? { pageToken } : {}),
    });
    return {
      calendars: (response.data.items ?? []).flatMap((calendar) => {
        if (!calendar.id) return [];
        return [
          {
            id: calendar.id,
            summary: calendar.summaryOverride ?? calendar.summary ?? calendar.id,
            primary: calendar.primary === true,
            accessRole: calendar.accessRole ?? "reader",
            timezone: calendar.timeZone ?? null,
            deleted: calendar.deleted === true,
          },
        ];
      }),
      ...(response.data.nextPageToken ? { nextPageToken: response.data.nextPageToken } : {}),
    };
  }

  public async listEvents(input: {
    calendarId: string;
    syncToken?: string;
    pageToken?: string;
    timeMin?: Date;
    timeMax?: Date;
  }): Promise<CalendarEventPage> {
    const request: calendar_v3.Params$Resource$Events$List = {
      calendarId: input.calendarId,
      maxResults: 2500,
      showDeleted: true,
      singleEvents: true,
      ...(input.pageToken ? { pageToken: input.pageToken } : {}),
    };
    if (input.syncToken) {
      request.syncToken = input.syncToken;
    } else {
      if (input.timeMin) request.timeMin = input.timeMin.toISOString();
      if (input.timeMax) request.timeMax = input.timeMax.toISOString();
    }
    const response = await this.#calendar.events.list(request);
    return {
      events: (response.data.items ?? []).flatMap((event) => {
        if (!event.id) return [];
        if (event.status !== "cancelled" && (!event.start || !event.end)) return [];
        return [normalizeEvent(input.calendarId, event)];
      }),
      ...(response.data.nextPageToken ? { nextPageToken: response.data.nextPageToken } : {}),
      ...(response.data.nextSyncToken ? { nextSyncToken: response.data.nextSyncToken } : {}),
    };
  }
}

function normalizeEvent(calendarId: string, event: calendar_v3.Schema$Event): NormalizedCalendarEvent {
  return {
    id: event.id as string,
    calendarId,
    etag: event.etag ?? null,
    status: event.status ?? "confirmed",
    summary: event.summary ?? null,
    description: event.description ?? null,
    location: event.location ?? null,
    start: event.start?.dateTime ?? event.start?.date ?? "",
    end: event.end?.dateTime ?? event.end?.date ?? "",
    timezone: event.start?.timeZone ?? null,
    recurringEventId: event.recurringEventId ?? null,
    updatedAt: event.updated ? new Date(event.updated) : null,
    attendees: (event.attendees ?? []).flatMap((attendee) =>
      attendee.email
        ? [
            {
              email: attendee.email.toLowerCase(),
              responseStatus: attendee.responseStatus ?? null,
              self: attendee.self === true,
            },
          ]
        : [],
    ),
  };
}
