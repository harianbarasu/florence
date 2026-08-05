import { z } from "zod";
import type { BlindIndex } from "./blind-index.js";
import type { EncryptionContext } from "./tenant-json-cipher.js";

const instantSchema = z.iso.datetime({ offset: true });
const WEEK_MS = 7 * 24 * 60 * 60_000;

export const calendarBusyWindowSchema = z.strictObject({
  startsAt: instantSchema,
  endsAt: instantSchema,
  allDay: z.boolean(),
});

export type EncryptedCalendarBusyWindow = z.infer<typeof calendarBusyWindowSchema>;

/**
 * Maps an exact interval to opaque UTC-week candidates. The database can only
 * narrow a query to a coarse superset; overlap is always decided after decrypt.
 */
export function calendarBusyWindowCandidateBuckets(
  blindIndex: BlindIndex,
  householdId: string,
  startsAt: string,
  endsAt: string,
): string[] {
  const start = Date.parse(instantSchema.parse(startsAt));
  const end = Date.parse(instantSchema.parse(endsAt));
  if (start >= end) throw new Error("Calendar busy window must have a positive duration");

  const firstWeek = Math.floor(start / WEEK_MS);
  const lastWeek = Math.floor((end - 1) / WEEK_MS);
  const buckets: string[] = [];
  for (let week = firstWeek; week <= lastWeek; week += 1) {
    buckets.push(blindIndex.digest("calendar_busy_windows.week.v1", `${householdId}\u0000${week}`));
  }
  return buckets;
}

export function calendarBusyWindowEncryptionContext(input: {
  householdId: string;
  connectionId: string;
  calendarId: string;
  externalEventId: string;
}): EncryptionContext {
  return {
    tenant: { kind: "household", id: input.householdId },
    table: "calendar_busy_windows",
    rowId: JSON.stringify([input.connectionId, input.calendarId, input.externalEventId]),
    field: "window",
  };
}
