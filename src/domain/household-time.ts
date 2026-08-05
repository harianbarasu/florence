import { Temporal } from "@js-temporal/polyfill";
import { z } from "zod";
import {
  type InstantString,
  InstantStringSchema,
  type ResolvedTimePlan,
  ResolvedTimePlanSchema,
  type RoutineAnchor,
  RoutineAnchorSchema,
  type SemanticMoment,
  type SemanticTimePlan,
  SemanticTimePlanSchema,
  TemporalTriggerIdSchema,
} from "./contracts.js";

export type HouseholdTimeErrorCode =
  | "missing_anchor"
  | "anchor_not_active"
  | "invalid_local_time"
  | "inconsistent_plan"
  | "trigger_outside_window";

export class HouseholdTimeError extends Error {
  public readonly code: HouseholdTimeErrorCode;

  public constructor(code: HouseholdTimeErrorCode, message: string) {
    super(message);
    this.name = "HouseholdTimeError";
    this.code = code;
  }
}

const ResolveTimePlanInputSchema = z.strictObject({
  plan: SemanticTimePlanSchema,
  routineAnchors: z.array(RoutineAnchorSchema).max(100),
});

const TimerEvaluationInputSchema = z.strictObject({
  plan: ResolvedTimePlanSchema,
  triggerId: TemporalTriggerIdSchema,
  now: InstantStringSchema,
});

export const TimerEvaluationSchema = z.discriminatedUnion("decision", [
  z.strictObject({ decision: z.literal("not_due"), scheduledAt: InstantStringSchema }),
  z.strictObject({ decision: z.literal("remind"), scheduledAt: InstantStringSchema }),
  z.strictObject({ decision: z.literal("missed_window"), scheduledAt: InstantStringSchema }),
  z.strictObject({ decision: z.literal("obsolete"), scheduledAt: InstantStringSchema }),
]);

export type TimerEvaluation = z.infer<typeof TimerEvaluationSchema>;

function toInstant(value: string): Temporal.Instant {
  return Temporal.Instant.from(value);
}

function canonicalInstant(value: Temporal.Instant): InstantString {
  return InstantStringSchema.parse(value.toString());
}

function resolveLocalMoment(
  date: string,
  time: string,
  timeZone: string,
  disambiguation: "compatible" | "earlier" | "later" | "reject",
): Temporal.Instant {
  try {
    return Temporal.PlainDateTime.from(`${date}T${time}`)
      .toZonedDateTime(timeZone, {
        disambiguation,
      })
      .toInstant();
  } catch (error) {
    throw new HouseholdTimeError(
      "invalid_local_time",
      `Could not resolve ${date}T${time} in ${timeZone}: ${error instanceof Error ? error.message : "invalid time"}`,
    );
  }
}

function findAnchor(moment: Extract<SemanticMoment, { kind: "routine_anchor" }>, anchors: RoutineAnchor[]) {
  const anchor = anchors.find((candidate) => candidate.anchorId === moment.anchorId);
  if (anchor === undefined) {
    throw new HouseholdTimeError("missing_anchor", `Routine anchor ${moment.anchorId} does not exist`);
  }

  const dayOfWeek = Temporal.PlainDate.from(moment.date).dayOfWeek;
  if (!anchor.daysOfWeek.includes(dayOfWeek)) {
    throw new HouseholdTimeError(
      "anchor_not_active",
      `Routine anchor ${moment.anchorId} is not active on ${moment.date}`,
    );
  }

  return anchor;
}

function resolveMoment(moment: SemanticMoment, anchors: RoutineAnchor[]): Temporal.Instant {
  switch (moment.kind) {
    case "instant":
      return toInstant(moment.at);
    case "local":
      return resolveLocalMoment(moment.date, moment.time, moment.timeZone, moment.disambiguation);
    case "routine_anchor": {
      const anchor = findAnchor(moment, anchors);
      return resolveLocalMoment(moment.date, anchor.localTime, anchor.timeZone, moment.disambiguation).add({
        minutes: moment.offsetMinutes,
      });
    }
  }
}

function compare(left: Temporal.Instant, right: Temporal.Instant): number {
  return Temporal.Instant.compare(left, right);
}

function resolvePlan(input: { plan: SemanticTimePlan; routineAnchors: RoutineAnchor[] }): ResolvedTimePlan {
  const parsed = ResolveTimePlanInputSchema.parse(input);
  const { plan, routineAnchors } = parsed;

  const event = plan.event === undefined ? undefined : resolveMoment(plan.event, routineAnchors);
  const deadline = plan.deadline === undefined ? undefined : resolveMoment(plan.deadline, routineAnchors);
  const reference = deadline ?? event;

  if (reference === undefined) {
    throw new HouseholdTimeError("inconsistent_plan", "A plan needs an event or deadline");
  }

  if (event !== undefined && deadline !== undefined && compare(deadline, event) > 0) {
    throw new HouseholdTimeError("inconsistent_plan", "The formal deadline cannot be after the event");
  }

  const requiredLeadMinutes = plan.preparationMinutes + plan.finalBufferMinutes;
  const derivedUsefulLeadMinutes = Math.max(plan.usefulLeadMinutes, requiredLeadMinutes);
  const earliestUseful =
    plan.earliestUseful === undefined
      ? reference.subtract({ minutes: derivedUsefulLeadMinutes })
      : resolveMoment(plan.earliestUseful, routineAnchors);
  const lastResponsible =
    plan.lastResponsible === undefined
      ? reference.subtract({ minutes: requiredLeadMinutes })
      : resolveMoment(plan.lastResponsible, routineAnchors);

  if (compare(earliestUseful, lastResponsible) > 0 || compare(lastResponsible, reference) > 0) {
    throw new HouseholdTimeError(
      "inconsistent_plan",
      "The useful window must satisfy earliest useful <= last responsible <= deadline or event",
    );
  }

  const triggers = plan.triggers
    .map((trigger) => ({
      triggerId: trigger.triggerId,
      timerId: trigger.timerId,
      kind: trigger.kind,
      at: resolveMoment(trigger.at, routineAnchors),
    }))
    .sort((left, right) => {
      const byTime = compare(left.at, right.at);
      return byTime === 0 ? left.triggerId.localeCompare(right.triggerId) : byTime;
    });

  for (const trigger of triggers) {
    if (compare(trigger.at, earliestUseful) < 0 || compare(trigger.at, reference) > 0) {
      throw new HouseholdTimeError(
        "trigger_outside_window",
        `Trigger ${trigger.triggerId} is outside the useful action window`,
      );
    }
    if (trigger.kind === "reminder" && compare(trigger.at, lastResponsible) > 0) {
      throw new HouseholdTimeError(
        "trigger_outside_window",
        `Reminder ${trigger.triggerId} occurs after the last responsible moment`,
      );
    }
    if (trigger.kind === "window_check" && compare(trigger.at, lastResponsible) < 0) {
      throw new HouseholdTimeError(
        "trigger_outside_window",
        `Window check ${trigger.triggerId} occurs before the last responsible moment`,
      );
    }
  }

  return ResolvedTimePlanSchema.parse({
    definition: plan,
    ...(event === undefined ? {} : { eventAt: canonicalInstant(event) }),
    ...(deadline === undefined ? {} : { deadlineAt: canonicalInstant(deadline) }),
    referenceAt: canonicalInstant(reference),
    earliestUsefulAt: canonicalInstant(earliestUseful),
    lastResponsibleAt: canonicalInstant(lastResponsible),
    triggers: triggers.map((trigger) => ({
      ...trigger,
      at: canonicalInstant(trigger.at),
      status: "pending" as const,
    })),
  });
}

function evaluateTimer(input: {
  plan: ResolvedTimePlan;
  triggerId: z.infer<typeof TemporalTriggerIdSchema>;
  now: InstantString;
}): TimerEvaluation {
  const parsed = TimerEvaluationInputSchema.parse(input);
  const trigger = parsed.plan.triggers.find((candidate) => candidate.triggerId === parsed.triggerId);

  if (trigger === undefined) {
    throw new HouseholdTimeError("inconsistent_plan", `Trigger ${parsed.triggerId} does not exist`);
  }

  if (trigger.status !== "pending") {
    return TimerEvaluationSchema.parse({ decision: "obsolete", scheduledAt: trigger.at });
  }

  const now = toInstant(parsed.now);
  const scheduledAt = toInstant(trigger.at);
  if (compare(now, scheduledAt) < 0) {
    return TimerEvaluationSchema.parse({ decision: "not_due", scheduledAt: trigger.at });
  }

  const lastResponsible = toInstant(parsed.plan.lastResponsibleAt);
  if (trigger.kind === "window_check" || compare(now, lastResponsible) > 0) {
    return TimerEvaluationSchema.parse({ decision: "missed_window", scheduledAt: trigger.at });
  }

  return TimerEvaluationSchema.parse({ decision: "remind", scheduledAt: trigger.at });
}

export interface HouseholdTimeModule {
  resolve(input: { plan: SemanticTimePlan; routineAnchors: RoutineAnchor[] }): ResolvedTimePlan;
  evaluateTimer(input: {
    plan: ResolvedTimePlan;
    triggerId: z.infer<typeof TemporalTriggerIdSchema>;
    now: InstantString;
  }): TimerEvaluation;
}

export const HouseholdTime: HouseholdTimeModule = Object.freeze({
  resolve: resolvePlan,
  evaluateTimer,
});
