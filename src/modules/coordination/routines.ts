import { Temporal } from "@js-temporal/polyfill";
import { z } from "zod";
import {
  CoordinationError,
  EntityIdSchema,
  EvidenceRefSchema,
  InstantSchema,
  type Routine,
  type RoutineOccurrence,
  RoutineOccurrenceSchema,
  type RoutineRecurrence,
  type RoutineRevision,
  RoutineRevisionSchema,
  RoutineSchema,
  SemanticTimePlanSchema,
} from "./contracts.js";
import { resolveSemanticTime } from "./household-time.js";

export const MaterializeRoutineOccurrenceInputSchema = z.strictObject({
  occurrenceId: EntityIdSchema,
  routine: RoutineSchema,
  revision: RoutineRevisionSchema,
  localDate: z.iso.date(),
  materializedAt: InstantSchema,
});

export const ReviseOccurrenceCommandSchema = z.discriminatedUnion("kind", [
  z.strictObject({
    kind: z.literal("reschedule"),
    expectedVersion: z.number().int().positive(),
    occurredAt: InstantSchema,
    timePlan: SemanticTimePlanSchema,
    evidenceRef: EvidenceRefSchema,
  }),
  z.strictObject({
    kind: z.literal("swap_holder"),
    expectedVersion: z.number().int().positive(),
    occurredAt: InstantSchema,
    proposedHolderPersonId: EntityIdSchema.nullable(),
    evidenceRef: EvidenceRefSchema,
  }),
  z.strictObject({
    kind: z.enum(["skip", "cancel"]),
    expectedVersion: z.number().int().positive(),
    occurredAt: InstantSchema,
    evidenceRef: EvidenceRefSchema,
  }),
]);
export type ReviseOccurrenceCommand = z.input<typeof ReviseOccurrenceCommandSchema>;

/** Materializes one immutable, revision-bound occurrence. */
export function materializeRoutineOccurrence(inputCandidate: {
  readonly occurrenceId: string;
  readonly routine: Routine;
  readonly revision: RoutineRevision;
  readonly localDate: string;
  readonly materializedAt: string;
}): RoutineOccurrence {
  const input = MaterializeRoutineOccurrenceInputSchema.parse(inputCandidate);
  if (input.routine.status !== "active") throw new CoordinationError("routine_not_active");
  if (
    input.routine.routineId !== input.revision.routineId ||
    input.revision.revision > input.routine.currentRevision
  ) {
    throw new CoordinationError("routine_revision_mismatch");
  }
  if (!routineRevisionIncludesDate(input.revision, input.localDate)) {
    throw new CoordinationError("date_not_in_recurrence");
  }

  return RoutineOccurrenceSchema.parse({
    occurrenceId: input.occurrenceId,
    materializationKey: `${input.routine.routineId}:${input.revision.revision}:${input.localDate}`,
    routineId: input.routine.routineId,
    routineRevision: input.revision.revision,
    localDate: input.localDate,
    version: 1,
    supersedesVersion: null,
    planVersion: 1,
    status: "materialized",
    title: input.revision.title,
    minimumSharedMeaning: input.revision.minimumSharedMeaning,
    timing: resolveSemanticTime(input.revision.timePlan, input.localDate),
    notificationMode: input.revision.notificationMode,
    destination: input.revision.destination,
    proposedHolderPersonId: input.revision.proposedHolderPersonId,
    standingCoverage: input.revision.standingCoverage,
    sourceRevisionRefs: input.revision.sourceRevisionRefs,
    materializedAt: input.materializedAt,
  });
}

/**
 * Revises only one dated occurrence. The standing routine and every prior
 * occurrence version remain unchanged.
 */
export function reviseRoutineOccurrence(
  occurrenceCandidate: RoutineOccurrence,
  commandCandidate: ReviseOccurrenceCommand,
): RoutineOccurrence {
  const occurrence = RoutineOccurrenceSchema.parse(occurrenceCandidate);
  const command = ReviseOccurrenceCommandSchema.parse(commandCandidate);
  if (occurrence.version !== command.expectedVersion) {
    throw new CoordinationError("version_conflict");
  }
  if (occurrence.status !== "materialized") {
    throw new CoordinationError("invalid_transition");
  }

  const next = {
    ...occurrence,
    version: occurrence.version + 1,
    supersedesVersion: occurrence.version,
    materializedAt: command.occurredAt,
    sourceRevisionRefs: unique([...occurrence.sourceRevisionRefs, command.evidenceRef]),
  };

  switch (command.kind) {
    case "reschedule":
      return RoutineOccurrenceSchema.parse({
        ...next,
        planVersion: occurrence.planVersion + 1,
        timing: resolveSemanticTime(command.timePlan, occurrence.localDate),
      });
    case "swap_holder":
      return RoutineOccurrenceSchema.parse({
        ...next,
        proposedHolderPersonId: command.proposedHolderPersonId,
        standingCoverage: null,
      });
    case "skip":
      return RoutineOccurrenceSchema.parse({ ...next, status: "skipped", standingCoverage: null });
    case "cancel":
      return RoutineOccurrenceSchema.parse({ ...next, status: "cancelled", standingCoverage: null });
  }
}

export function recurrenceIncludesDate(recurrence: RoutineRecurrence, localDateCandidate: string): boolean {
  const localDate = Temporal.PlainDate.from(localDateCandidate);
  if (recurrence.kind === "once") return localDate.equals(Temporal.PlainDate.from(recurrence.on));

  const startsOn = Temporal.PlainDate.from(recurrence.startsOn);
  if (Temporal.PlainDate.compare(localDate, startsOn) < 0) return false;
  if (
    recurrence.endsOn !== null &&
    Temporal.PlainDate.compare(localDate, Temporal.PlainDate.from(recurrence.endsOn)) > 0
  ) {
    return false;
  }
  if (recurrence.excludedDates.includes(localDate.toString())) return false;
  const daysFromStart = startsOn.until(localDate, { largestUnit: "days" }).days;
  const weekIndex = Math.floor(daysFromStart / 7);
  return weekIndex % recurrence.intervalWeeks === 0 && recurrence.weekdays.includes(localDate.dayOfWeek);
}

export function routineRevisionIncludesDate(revision: RoutineRevision, localDate: string): boolean {
  if (Temporal.PlainDate.compare(localDate, revision.effectiveFrom) < 0) return false;
  if (
    revision.effectiveThrough !== null &&
    Temporal.PlainDate.compare(localDate, revision.effectiveThrough) > 0
  ) {
    return false;
  }
  return recurrenceIncludesDate(revision.recurrence, localDate);
}

function unique(values: readonly string[]): string[] {
  return [...new Set(values)];
}
