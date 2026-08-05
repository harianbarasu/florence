import { createHash, timingSafeEqual } from "node:crypto";
import { type calendar_v3, google } from "googleapis";
import { z } from "zod";
import { googleAuthWithAccessToken } from "./auth.js";
import type { GoogleAdapterConfig } from "./config.js";
import {
  GoogleAdapterError,
  GoogleSyncTokenExpiredError,
  mapGoogleProviderError,
  providerStatus,
} from "./errors.js";

const calendarDateSchema = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}$/)
  .refine(isValidCalendarDate, "invalid calendar date");
const calendarDateTimeSchema = z
  .string()
  .min(1)
  .refine((value) => Number.isFinite(Date.parse(value)), "invalid calendar date-time");

export const calendarEventTimeSchema = z.discriminatedUnion("kind", [
  z
    .object({
      kind: z.literal("date"),
      date: calendarDateSchema,
      timeZone: z.null(),
    })
    .strict(),
  z
    .object({
      kind: z.literal("dateTime"),
      dateTime: calendarDateTimeSchema,
      timeZone: z.string().min(1).nullable(),
    })
    .strict(),
]);

export type CalendarEventTime = z.infer<typeof calendarEventTimeSchema>;

const calendarPersonSchema = z
  .object({
    id: z.string().min(1).nullable(),
    email: z.string().nullable(),
    displayName: z.string().nullable(),
    self: z.boolean(),
  })
  .strict();

const calendarAttendeeSchema = calendarPersonSchema
  .extend({
    organizer: z.boolean(),
    optional: z.boolean(),
    resource: z.boolean(),
    responseStatus: z.string().nullable(),
    comment: z.string().nullable(),
    additionalGuests: z.number().int().nonnegative(),
  })
  .strict();

export const googleCalendarEventSchema = z
  .object({
    schemaVersion: z.literal(1),
    source: z.literal("google-calendar"),
    sourceScope: z.literal("personal"),
    googleSubject: z.string().min(1),
    calendarId: z.string().min(1),
    sourceKey: z.string().min(1),
    changeKey: z.string().min(1),
    eventId: z.string().min(1),
    etag: z.string().nullable(),
    iCalUid: z.string().nullable(),
    status: z.string(),
    updatedAt: z.string().nullable(),
    createdAt: z.string().nullable(),
    sequence: z.number().int(),
    summary: z.string(),
    description: z.string().nullable(),
    location: z.string().nullable(),
    start: calendarEventTimeSchema.nullable(),
    end: calendarEventTimeSchema.nullable(),
    recurringEventId: z.string().nullable(),
    originalStart: calendarEventTimeSchema.nullable(),
    recurrence: z.array(z.string()),
    organizer: calendarPersonSchema.nullable(),
    creator: calendarPersonSchema.nullable(),
    attendees: z.array(calendarAttendeeSchema),
    transparency: z.string().nullable(),
    visibility: z.string().nullable(),
    htmlLink: z.string().nullable(),
    deleted: z.boolean(),
  })
  .strict();

export type GoogleCalendarEvent = z.infer<typeof googleCalendarEventSchema>;

export const googleCalendarPushEventSchema = z
  .object({
    schemaVersion: z.literal(1),
    source: z.literal("google-calendar"),
    providerEventId: z.string().min(1),
    channelId: z.string().min(1),
    resourceId: z.string().min(1),
    resourceState: z.enum(["sync", "exists", "not_exists"]),
    resourceUri: z.string().min(1),
    messageNumber: z.string().regex(/^\d+$/),
    channelExpiresAt: z.string().nullable(),
  })
  .strict();

export type GoogleCalendarPushEvent = z.infer<typeof googleCalendarPushEventSchema>;

const providerWriteTimeSchema = z.union([
  z.object({ date: calendarDateSchema }).strict(),
  z
    .object({
      dateTime: calendarDateTimeSchema,
      timeZone: z.string().min(1).optional(),
    })
    .strict(),
]);

type ProviderWriteTime = z.infer<typeof providerWriteTimeSchema>;

const calendarWriteFieldsSchema = z.object({
  summary: z.string().min(1).max(1_024),
  description: z.string().max(8_192).optional(),
  location: z.string().max(1_024).optional(),
  start: providerWriteTimeSchema,
  end: providerWriteTimeSchema,
  attendees: z
    .array(z.object({ email: z.string().email() }).strict())
    .max(200)
    .optional(),
  recurrence: z.array(z.string().min(1)).max(100).optional(),
  transparency: z.enum(["opaque", "transparent"]).optional(),
  visibility: z.enum(["default", "public", "private", "confidential"]).optional(),
});

type CalendarWriteFields = z.infer<typeof calendarWriteFieldsSchema>;
type CalendarWriteBodyInput = {
  [Key in keyof CalendarWriteFields]?: CalendarWriteFields[Key] | undefined;
};

export const insertCalendarEventInputSchema = calendarWriteFieldsSchema
  .extend({
    accessToken: z.string().min(1),
    googleSubject: z.string().min(1),
    calendarId: z.string().min(1),
    idempotencyKey: z.string().min(1).max(255),
    sendUpdates: z.enum(["all", "externalOnly", "none"]).default("none"),
  })
  .strict()
  .superRefine(validateWriteTimes);

export type InsertCalendarEventInput = z.input<typeof insertCalendarEventInputSchema>;

const calendarPatchFieldsSchema = calendarWriteFieldsSchema.partial();

export const patchCalendarEventInputSchema = calendarPatchFieldsSchema
  .extend({
    accessToken: z.string().min(1),
    googleSubject: z.string().min(1),
    calendarId: z.string().min(1),
    eventId: z.string().min(1),
    etag: z.string().min(1),
    sendUpdates: z.enum(["all", "externalOnly", "none"]).default("none"),
  })
  .strict()
  .superRefine((input, context) => {
    const writeKeys = [
      "summary",
      "description",
      "location",
      "start",
      "end",
      "attendees",
      "recurrence",
      "transparency",
      "visibility",
    ] as const;
    if (!writeKeys.some((key) => input[key] !== undefined)) {
      context.addIssue({ code: "custom", message: "at least one calendar field must be patched" });
    }
    if ((input.start === undefined) !== (input.end === undefined)) {
      context.addIssue({ code: "custom", message: "calendar start and end must be patched together" });
    }
    if (input.start && input.end) {
      validateWriteTimes({ start: input.start, end: input.end }, context);
    }
  });

export type PatchCalendarEventInput = z.input<typeof patchCalendarEventInputSchema>;

export const deleteCalendarEventInputSchema = z
  .object({
    accessToken: z.string().min(1),
    calendarId: z.string().min(1),
    eventId: z.string().min(1),
    etag: z.string().min(1),
    sendUpdates: z.enum(["all", "externalOnly", "none"]).default("none"),
  })
  .strict();

export type DeleteCalendarEventInput = z.input<typeof deleteCalendarEventInputSchema>;

export interface GoogleCalendarPage {
  events: GoogleCalendarEvent[];
  nextPageToken: string | null;
  nextSyncToken: string | null;
  timeZone: string | null;
}

export interface GoogleCalendarWatchReceipt {
  channelId: string;
  resourceId: string;
  resourceUri: string;
  expiresAt: string | null;
}

export interface CalendarDeleteReceipt {
  provider: "google-calendar";
  calendarId: string;
  eventId: string;
  deleted: true;
}

interface CalendarApiPort {
  listEvents(params: {
    calendarId: string;
    maxResults: number;
    showDeleted: true;
    singleEvents: false;
    pageToken?: string;
    syncToken?: string;
    timeMin?: string;
    timeMax?: string;
  }): Promise<{ data: calendar_v3.Schema$Events }>;
  getEvent(params: { calendarId: string; eventId: string }): Promise<{ data: calendar_v3.Schema$Event }>;
  insertEvent(params: {
    calendarId: string;
    sendUpdates: "all" | "externalOnly" | "none";
    requestBody: calendar_v3.Schema$Event;
  }): Promise<{ data: calendar_v3.Schema$Event }>;
  patchEvent(
    params: {
      calendarId: string;
      eventId: string;
      sendUpdates: "all" | "externalOnly" | "none";
      requestBody: calendar_v3.Schema$Event;
    },
    options: { headers: { "If-Match": string } },
  ): Promise<{ data: calendar_v3.Schema$Event }>;
  deleteEvent(
    params: {
      calendarId: string;
      eventId: string;
      sendUpdates: "all" | "externalOnly" | "none";
    },
    options: { headers: { "If-Match": string } },
  ): Promise<unknown>;
  watchEvents(params: {
    calendarId: string;
    requestBody: calendar_v3.Schema$Channel;
  }): Promise<{ data: calendar_v3.Schema$Channel }>;
  stopChannel(params: { requestBody: { id: string; resourceId: string } }): Promise<unknown>;
}

export type CalendarApiFactory = (accessToken: string) => CalendarApiPort;

export class GoogleCalendarAdapter {
  readonly #apiFactory: CalendarApiFactory;

  constructor(config: GoogleAdapterConfig, apiFactory?: CalendarApiFactory) {
    this.#apiFactory = apiFactory ?? defaultCalendarApiFactory(config);
  }

  async listEventsPage(input: {
    accessToken: string;
    googleSubject: string;
    calendarId: string;
    pageToken?: string;
    syncToken?: string;
    timeMin?: string;
    timeMax?: string;
    maxResults?: number;
  }): Promise<GoogleCalendarPage> {
    requireAccessToken(input.accessToken);
    if (input.syncToken && (input.timeMin || input.timeMax)) {
      throw new GoogleAdapterError(
        "Calendar incremental sync cannot change its time bounds",
        "invalid_request",
        null,
        false,
      );
    }
    const maxResults = boundedInteger(input.maxResults ?? 2_500, 1, 2_500, "Calendar page size");
    try {
      const response = await this.#apiFactory(input.accessToken).listEvents({
        calendarId: input.calendarId,
        maxResults,
        showDeleted: true,
        singleEvents: false,
        ...(input.pageToken ? { pageToken: input.pageToken } : {}),
        ...(input.syncToken ? { syncToken: input.syncToken } : {}),
        ...(input.timeMin ? { timeMin: normalizeIsoTimestamp(input.timeMin, "Calendar timeMin") } : {}),
        ...(input.timeMax ? { timeMax: normalizeIsoTimestamp(input.timeMax, "Calendar timeMax") } : {}),
      });
      return {
        events: (response.data.items ?? []).map((event) =>
          parseGoogleCalendarEvent(event, input.googleSubject, input.calendarId),
        ),
        nextPageToken: response.data.nextPageToken ?? null,
        nextSyncToken: response.data.nextSyncToken ?? null,
        timeZone: response.data.timeZone ?? null,
      };
    } catch (error) {
      if (providerStatus(error) === 410) {
        throw new GoogleSyncTokenExpiredError("calendar");
      }
      throw mapGoogleProviderError("Google Calendar event listing", error);
    }
  }

  async insertEvent(rawInput: InsertCalendarEventInput): Promise<GoogleCalendarEvent> {
    const input = insertCalendarEventInputSchema.parse(rawInput);
    const api = this.#apiFactory(input.accessToken);
    const eventId = calendarEventIdFromIdempotencyKey(input.idempotencyKey);
    const idempotencyHash = hashIdempotencyKey(input.idempotencyKey);
    const requestBody = writeEventBody(input, {
      id: eventId,
      extendedProperties: {
        private: { florence_idempotency_hash: idempotencyHash },
      },
    });
    try {
      const response = await api.insertEvent({
        calendarId: input.calendarId,
        sendUpdates: input.sendUpdates,
        requestBody,
      });
      return parseGoogleCalendarEvent(response.data, input.googleSubject, input.calendarId);
    } catch (error) {
      if (providerStatus(error) !== 409) {
        throw mapGoogleProviderError("Google Calendar event insertion", error);
      }
      try {
        const existing = await api.getEvent({ calendarId: input.calendarId, eventId });
        if (existing.data.extendedProperties?.private?.florence_idempotency_hash !== idempotencyHash) {
          throw new GoogleAdapterError(
            "Calendar event ID conflicts with a different action",
            "permanent",
            409,
            false,
          );
        }
        return parseGoogleCalendarEvent(existing.data, input.googleSubject, input.calendarId);
      } catch (reconciliationError) {
        throw mapGoogleProviderError("Google Calendar insertion reconciliation", reconciliationError);
      }
    }
  }

  async patchEvent(rawInput: PatchCalendarEventInput): Promise<GoogleCalendarEvent> {
    const input = patchCalendarEventInputSchema.parse(rawInput);
    const requestBody = writeEventBody(input);
    try {
      const response = await this.#apiFactory(input.accessToken).patchEvent(
        {
          calendarId: input.calendarId,
          eventId: input.eventId,
          sendUpdates: input.sendUpdates,
          requestBody,
        },
        { headers: { "If-Match": input.etag } },
      );
      return parseGoogleCalendarEvent(response.data, input.googleSubject, input.calendarId);
    } catch (error) {
      throw mapGoogleProviderError("Google Calendar event patch", error);
    }
  }

  async deleteEvent(rawInput: DeleteCalendarEventInput): Promise<CalendarDeleteReceipt> {
    const input = deleteCalendarEventInputSchema.parse(rawInput);
    try {
      await this.#apiFactory(input.accessToken).deleteEvent(
        {
          calendarId: input.calendarId,
          eventId: input.eventId,
          sendUpdates: input.sendUpdates,
        },
        { headers: { "If-Match": input.etag } },
      );
    } catch (error) {
      const status = providerStatus(error);
      if (status !== 404 && status !== 410) {
        throw mapGoogleProviderError("Google Calendar event deletion", error);
      }
    }
    return {
      provider: "google-calendar",
      calendarId: input.calendarId,
      eventId: input.eventId,
      deleted: true,
    };
  }

  async watchEvents(input: {
    accessToken: string;
    calendarId: string;
    channelId: string;
    address: string;
    channelToken: string;
    expiresAt?: string;
  }): Promise<GoogleCalendarWatchReceipt> {
    requireAccessToken(input.accessToken);
    if (!input.channelId || !input.channelToken) {
      throw new GoogleAdapterError("Calendar channel identity is incomplete", "invalid_request", null, false);
    }
    let address: URL;
    try {
      address = new URL(input.address);
    } catch {
      throw new GoogleAdapterError("Calendar watch address is invalid", "invalid_request", null, false);
    }
    if (address.protocol !== "https:") {
      throw new GoogleAdapterError("Calendar watch address must use HTTPS", "invalid_request", null, false);
    }
    const expiration = input.expiresAt
      ? String(new Date(normalizeIsoTimestamp(input.expiresAt, "Calendar channel expiration")).getTime())
      : undefined;
    try {
      const response = await this.#apiFactory(input.accessToken).watchEvents({
        calendarId: input.calendarId,
        requestBody: {
          id: input.channelId,
          type: "web_hook",
          address: address.toString(),
          token: input.channelToken,
          ...(expiration ? { expiration } : {}),
        },
      });
      if (!response.data.id || !response.data.resourceId || !response.data.resourceUri) {
        throw new GoogleAdapterError("Calendar watch response is incomplete", "permanent", null, false);
      }
      return {
        channelId: response.data.id,
        resourceId: response.data.resourceId,
        resourceUri: response.data.resourceUri,
        expiresAt: response.data.expiration
          ? epochMillisecondsToIso(response.data.expiration, "Calendar channel expiration")
          : null,
      };
    } catch (error) {
      throw mapGoogleProviderError("Google Calendar watch creation", error);
    }
  }

  async stopChannel(input: { accessToken: string; channelId: string; resourceId: string }): Promise<void> {
    requireAccessToken(input.accessToken);
    try {
      await this.#apiFactory(input.accessToken).stopChannel({
        requestBody: { id: input.channelId, resourceId: input.resourceId },
      });
    } catch (error) {
      throw mapGoogleProviderError("Google Calendar watch removal", error);
    }
  }
}

export function parseGoogleCalendarEvent(
  event: calendar_v3.Schema$Event,
  googleSubject: string,
  calendarId: string,
): GoogleCalendarEvent {
  if (!event.id || !googleSubject || !calendarId) {
    throw new GoogleAdapterError("Calendar event is missing a stable identity", "permanent", null, false);
  }
  const status = event.status ?? "confirmed";
  const sourceKey = `${googleSubject}:${calendarId}:${event.id}`;
  const changeDigest = createHash("sha256")
    .update(
      JSON.stringify([sourceKey, event.etag ?? null, event.updated ?? null, event.sequence ?? 0, status]),
    )
    .digest("hex");

  return googleCalendarEventSchema.parse({
    schemaVersion: 1,
    source: "google-calendar",
    sourceScope: "personal",
    googleSubject,
    calendarId,
    sourceKey,
    changeKey: `google-calendar-change:${changeDigest}`,
    eventId: event.id,
    etag: event.etag ?? null,
    iCalUid: event.iCalUID ?? null,
    status,
    updatedAt: nullableTimestamp(event.updated, "Calendar updated time"),
    createdAt: nullableTimestamp(event.created, "Calendar created time"),
    sequence: event.sequence ?? 0,
    summary: event.summary ?? "",
    description: event.description ?? null,
    location: event.location ?? null,
    start: parseEventTime(event.start),
    end: parseEventTime(event.end),
    recurringEventId: event.recurringEventId ?? null,
    originalStart: parseEventTime(event.originalStartTime),
    recurrence: event.recurrence ?? [],
    organizer: parsePerson(event.organizer),
    creator: parsePerson(event.creator),
    attendees: (event.attendees ?? []).map((attendee) => ({
      ...parsePerson(attendee),
      organizer: attendee.organizer === true,
      optional: attendee.optional === true,
      resource: attendee.resource === true,
      responseStatus: attendee.responseStatus ?? null,
      comment: attendee.comment ?? null,
      additionalGuests: attendee.additionalGuests ?? 0,
    })),
    transparency: event.transparency ?? null,
    visibility: event.visibility ?? null,
    htmlLink: event.htmlLink ?? null,
    deleted: status === "cancelled",
  });
}

export type CalendarPushHeaders = Readonly<Record<string, string | readonly string[] | undefined>>;

export function parseGoogleCalendarPush(
  headers: CalendarPushHeaders,
  expectedChannelToken: string,
): GoogleCalendarPushEvent {
  const channelToken = requiredHeader(headers, "x-goog-channel-token");
  if (!secretValuesMatch(expectedChannelToken, channelToken)) {
    throw new GoogleAdapterError("Calendar channel token is invalid", "unauthorized", null, false);
  }
  const channelId = requiredHeader(headers, "x-goog-channel-id");
  const resourceId = requiredHeader(headers, "x-goog-resource-id");
  const resourceState = requiredHeader(headers, "x-goog-resource-state");
  const resourceUri = requiredHeader(headers, "x-goog-resource-uri");
  const messageNumber = requiredHeader(headers, "x-goog-message-number");
  const expiration = optionalHeader(headers, "x-goog-channel-expiration");

  return googleCalendarPushEventSchema.parse({
    schemaVersion: 1,
    source: "google-calendar",
    providerEventId: `google-calendar-push:${channelId}:${resourceId}:${messageNumber}`,
    channelId,
    resourceId,
    resourceState,
    resourceUri,
    messageNumber,
    channelExpiresAt: expiration ? normalizeHeaderTimestamp(expiration) : null,
  });
}

export function calendarEventIdFromIdempotencyKey(idempotencyKey: string): string {
  if (!idempotencyKey || idempotencyKey.length > 255) {
    throw new GoogleAdapterError("Calendar idempotency key is invalid", "invalid_request", null, false);
  }
  // Hex is a valid subset of Google's base32hex event-ID alphabet.
  return `f${hashIdempotencyKey(idempotencyKey).slice(0, 39)}`;
}

function defaultCalendarApiFactory(config: GoogleAdapterConfig): CalendarApiFactory {
  return (accessToken) => {
    const api = google.calendar({
      version: "v3",
      auth: googleAuthWithAccessToken(config, accessToken),
    });
    return {
      listEvents: (params) => api.events.list(params),
      getEvent: (params) => api.events.get(params),
      insertEvent: (params) => api.events.insert(params),
      patchEvent: (params, options) => api.events.patch(params, options),
      deleteEvent: (params, options) => api.events.delete(params, options),
      watchEvents: (params) => api.events.watch(params),
      stopChannel: (params) => api.channels.stop(params),
    };
  };
}

function parseEventTime(
  value: calendar_v3.Schema$EventDateTime | null | undefined,
): CalendarEventTime | null {
  if (value?.date) {
    return calendarEventTimeSchema.parse({ kind: "date", date: value.date, timeZone: null });
  }
  if (value?.dateTime) {
    return calendarEventTimeSchema.parse({
      kind: "dateTime",
      dateTime: normalizeIsoTimestamp(value.dateTime, "Calendar event time"),
      timeZone: value.timeZone ?? null,
    });
  }
  return null;
}

function parsePerson(
  value: CalendarPersonLike | null | undefined,
): z.infer<typeof calendarPersonSchema> | null {
  if (!value) {
    return null;
  }
  return calendarPersonSchema.parse({
    id: value.id ?? null,
    email: value.email ?? null,
    displayName: value.displayName ?? null,
    self: value.self === true,
  });
}

function validateWriteTimes(
  input: { start: ProviderWriteTime; end: ProviderWriteTime },
  context: z.RefinementCtx,
): void {
  if ("date" in input.start !== "date" in input.end) {
    context.addIssue({ code: "custom", message: "calendar start and end must have the same type" });
    return;
  }
  const start = "date" in input.start ? input.start.date : input.start.dateTime;
  const end = "date" in input.end ? input.end.date : input.end.dateTime;
  if (new Date(start).getTime() >= new Date(end).getTime()) {
    context.addIssue({ code: "custom", message: "calendar end must be after start" });
  }
}

function writeEventBody(
  input: CalendarWriteBodyInput,
  initial: calendar_v3.Schema$Event = {},
): calendar_v3.Schema$Event {
  const body: calendar_v3.Schema$Event = { ...initial };
  if (input.summary !== undefined) body.summary = input.summary;
  if (input.description !== undefined) body.description = input.description;
  if (input.location !== undefined) body.location = input.location;
  if (input.start !== undefined) body.start = toProviderEventTime(input.start);
  if (input.end !== undefined) body.end = toProviderEventTime(input.end);
  if (input.attendees !== undefined) body.attendees = input.attendees.map(({ email }) => ({ email }));
  if (input.recurrence !== undefined) body.recurrence = input.recurrence;
  if (input.transparency !== undefined) body.transparency = input.transparency;
  if (input.visibility !== undefined) body.visibility = input.visibility;
  return body;
}

interface CalendarPersonLike {
  id?: string | null;
  email?: string | null;
  displayName?: string | null;
  self?: boolean | null;
}

function toProviderEventTime(value: ProviderWriteTime): calendar_v3.Schema$EventDateTime {
  if ("date" in value) {
    return { date: value.date };
  }
  return {
    dateTime: value.dateTime,
    ...(value.timeZone ? { timeZone: value.timeZone } : {}),
  };
}

function hashIdempotencyKey(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function requireAccessToken(token: string): void {
  if (!token) {
    throw new GoogleAdapterError("Google access token is required", "unauthorized", null, false);
  }
}

function boundedInteger(value: number, min: number, max: number, field: string): number {
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new GoogleAdapterError(`${field} is invalid`, "invalid_request", null, false);
  }
  return value;
}

function normalizeIsoTimestamp(value: string, field: string): string {
  const timestamp = new Date(value);
  if (!Number.isFinite(timestamp.getTime())) {
    throw new GoogleAdapterError(`${field} is invalid`, "invalid_request", null, false);
  }
  return timestamp.toISOString();
}

function isValidCalendarDate(value: string): boolean {
  const timestamp = new Date(`${value}T00:00:00.000Z`);
  return Number.isFinite(timestamp.getTime()) && timestamp.toISOString().slice(0, 10) === value;
}

function epochMillisecondsToIso(value: string, field: string): string {
  const timestamp = new Date(Number(value));
  if (!Number.isFinite(timestamp.getTime())) {
    throw new GoogleAdapterError(`${field} is invalid`, "permanent", null, false);
  }
  return timestamp.toISOString();
}

function normalizeHeaderTimestamp(value: string): string {
  const timestamp = new Date(value);
  if (!Number.isFinite(timestamp.getTime())) {
    throw new GoogleAdapterError("Calendar channel expiration is invalid", "invalid_request", null, false);
  }
  return timestamp.toISOString();
}

function nullableTimestamp(value: string | null | undefined, field: string): string | null {
  return value ? normalizeIsoTimestamp(value, field) : null;
}

function requiredHeader(headers: CalendarPushHeaders, name: string): string {
  const value = optionalHeader(headers, name);
  if (value === null) {
    throw new GoogleAdapterError(`Calendar push is missing ${name}`, "invalid_request", null, false);
  }
  return value;
}

function optionalHeader(headers: CalendarPushHeaders, name: string): string | null {
  const target = name.toLowerCase();
  for (const [key, raw] of Object.entries(headers)) {
    if (key.toLowerCase() !== target || raw === undefined) {
      continue;
    }
    const value = Array.isArray(raw) ? raw[0] : raw;
    return value?.trim() || null;
  }
  return null;
}

function secretValuesMatch(expected: string, supplied: string): boolean {
  if (!expected || !supplied) {
    return false;
  }
  const expectedBytes = Buffer.from(expected, "utf8");
  const suppliedBytes = Buffer.from(supplied, "utf8");
  return expectedBytes.length === suppliedBytes.length && timingSafeEqual(expectedBytes, suppliedBytes);
}
