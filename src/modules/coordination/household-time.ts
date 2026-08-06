import { Temporal } from "@js-temporal/polyfill";
import type { z } from "zod";
import {
  CoordinationError,
  EntityIdSchema,
  type NotificationCategory,
  type QuietHoursPolicy,
  QuietHoursPolicySchema,
  type ResolvedTimePlan,
  ResolvedTimePlanSchema,
  type SemanticAnchor,
  type SemanticMoment,
  SemanticTimePlanSchema,
  TimeZoneSchema,
} from "./contracts.js";

const QUIET_HOURS_SEARCH_MINUTES = 8 * 24 * 60;

export type QuietHoursDecision =
  | { readonly kind: "allowed"; readonly sendAt: string; readonly usedOverride: boolean }
  | { readonly kind: "delayed"; readonly sendAt: string; readonly usedOverride: false }
  | {
      readonly kind: "suppressed";
      readonly reason: "after_last_responsible" | "quiet_hours_cross_last_responsible";
    };

export function defaultQuietHours(personIdCandidate: string, timeZoneCandidate: string): QuietHoursPolicy {
  return QuietHoursPolicySchema.parse({
    personId: EntityIdSchema.parse(personIdCandidate),
    timeZone: TimeZoneSchema.parse(timeZoneCandidate),
    startLocalTime: "21:00",
    endLocalTime: "07:00",
    allowLastResponsibleOverrideFor: [],
  });
}

export function resolveSemanticTime(
  planCandidate: z.input<typeof SemanticTimePlanSchema>,
  localDateCandidate: string,
): ResolvedTimePlan {
  const plan = SemanticTimePlanSchema.parse(planCandidate);
  const localDate = Temporal.PlainDate.from(localDateCandidate).toString();

  try {
    const eventAt = plan.event === null ? null : resolveAnchor(plan.event, localDate, plan.timeZone);
    const deadlineAt = plan.deadline === null ? null : resolveAnchor(plan.deadline, localDate, plan.timeZone);
    const earliestUsefulAt = resolveMoment(plan.earliestUseful, localDate, plan.timeZone, {
      event: eventAt,
      deadline: deadlineAt,
    });
    const lastResponsibleAt = resolveMoment(plan.lastResponsible, localDate, plan.timeZone, {
      event: eventAt,
      deadline: deadlineAt,
    });

    if (compareInstants(earliestUsefulAt, lastResponsibleAt) > 0) {
      throw new CoordinationError("invalid_time_plan");
    }
    const outcomeAnchors = [eventAt, deadlineAt].filter((value): value is string => value !== null);
    const lastOutcomeAt = outcomeAnchors.reduce((latest, candidate) =>
      compareInstants(candidate, latest) > 0 ? candidate : latest,
    );
    if (compareInstants(lastResponsibleAt, lastOutcomeAt) > 0) {
      throw new CoordinationError("invalid_time_plan");
    }

    return ResolvedTimePlanSchema.parse({
      timeZone: plan.timeZone,
      localDate,
      eventAt,
      deadlineAt,
      preparationMinutes: plan.preparationMinutes,
      travelMinutes: plan.travelMinutes,
      earliestUsefulAt,
      lastResponsibleAt,
      resolutionPolicy: "wall_clock_compatible",
    });
  } catch (error) {
    if (error instanceof CoordinationError) throw error;
    throw new CoordinationError("invalid_time_plan");
  }
}

export function isQuietAt(instantCandidate: string, policyCandidate: QuietHoursPolicy): boolean {
  const policy = QuietHoursPolicySchema.parse(policyCandidate);
  const instant = Temporal.Instant.from(instantCandidate);
  const local = instant.toZonedDateTimeISO(policy.timeZone);
  const minuteOfDay = local.hour * 60 + local.minute;
  const start = localTimeMinutes(policy.startLocalTime);
  const end = localTimeMinutes(policy.endLocalTime);
  return start < end ? minuteOfDay >= start && minuteOfDay < end : minuteOfDay >= start || minuteOfDay < end;
}

/**
 * Applies the conservative group union: delivery is quiet when any affected
 * participant is quiet. An override is possible only when waiting would cross
 * the useful window and every participant explicitly allows this category.
 */
export function resolveQuietHours(input: {
  readonly candidateAt: string;
  readonly lastResponsibleAt: string;
  readonly category: NotificationCategory;
  readonly policies: readonly QuietHoursPolicy[];
}): QuietHoursDecision {
  const candidateAt = Temporal.Instant.from(input.candidateAt).toString();
  const lastResponsibleAt = Temporal.Instant.from(input.lastResponsibleAt).toString();
  const policies = input.policies.map((policy) => QuietHoursPolicySchema.parse(policy));

  if (compareInstants(candidateAt, lastResponsibleAt) > 0) {
    return { kind: "suppressed", reason: "after_last_responsible" };
  }
  if (!policies.some((policy) => isQuietAt(candidateAt, policy))) {
    return { kind: "allowed", sendAt: candidateAt, usedOverride: false };
  }

  const nextAllowed = findNextCommonAllowedInstant(candidateAt, policies);
  if (nextAllowed !== null && compareInstants(nextAllowed, lastResponsibleAt) <= 0) {
    return { kind: "delayed", sendAt: nextAllowed, usedOverride: false };
  }

  const everyParticipantAllowsOverride =
    policies.length > 0 &&
    policies.every((policy) => policy.allowLastResponsibleOverrideFor.includes(input.category));
  if (everyParticipantAllowsOverride) {
    return { kind: "allowed", sendAt: candidateAt, usedOverride: true };
  }
  return { kind: "suppressed", reason: "quiet_hours_cross_last_responsible" };
}

export function compareInstants(left: string, right: string): number {
  return Temporal.Instant.compare(Temporal.Instant.from(left), Temporal.Instant.from(right));
}

function resolveAnchor(anchor: SemanticAnchor, localDate: string, timeZone: string): string {
  if (anchor.kind === "instant") return Temporal.Instant.from(anchor.at).toString();
  const date = Temporal.PlainDate.from(localDate).add({ days: anchor.dayOffset });
  const [hourText, minuteText] = anchor.time.split(":");
  const hour = Number(hourText);
  const minute = Number(minuteText);
  return Temporal.ZonedDateTime.from(
    {
      timeZone,
      year: date.year,
      month: date.month,
      day: date.day,
      hour,
      minute,
    },
    { disambiguation: "compatible", offset: "prefer" },
  )
    .toInstant()
    .toString();
}

function resolveMoment(
  moment: SemanticMoment,
  localDate: string,
  timeZone: string,
  anchors: { readonly event: string | null; readonly deadline: string | null },
): string {
  if (moment.kind !== "relative") return resolveAnchor(moment, localDate, timeZone);
  const anchor = anchors[moment.anchor];
  if (anchor === null) throw new CoordinationError("invalid_time_plan");
  return Temporal.Instant.from(anchor).add({ minutes: moment.offsetMinutes }).toString();
}

function findNextCommonAllowedInstant(
  candidateAt: string,
  policies: readonly QuietHoursPolicy[],
): string | null {
  let cursor = Temporal.Instant.from(candidateAt);
  for (let minute = 0; minute < QUIET_HOURS_SEARCH_MINUTES; minute += 1) {
    cursor = cursor.add({ minutes: 1 });
    const candidate = cursor.toString();
    if (!policies.some((policy) => isQuietAt(candidate, policy))) return candidate;
  }
  return null;
}

function localTimeMinutes(localTime: string): number {
  const [hourText, minuteText] = localTime.split(":");
  return Number(hourText) * 60 + Number(minuteText);
}
