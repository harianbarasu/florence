import { Temporal } from "@js-temporal/polyfill";
import { z } from "zod";

export const EntityIdSchema = z.string().uuid();
export const InstantSchema = z.iso.datetime({ offset: true });
export const LocalDateSchema = z.iso.date();
export const LocalTimeSchema = z.string().regex(/^(?:[01]\d|2[0-3]):[0-5]\d$/u);

export const TimeZoneSchema = z
  .string()
  .min(1)
  .max(100)
  .superRefine((candidate, context) => {
    try {
      Temporal.ZonedDateTime.from({
        timeZone: candidate,
        year: 2026,
        month: 1,
        day: 1,
        hour: 12,
      });
    } catch {
      context.addIssue({ code: "custom", message: "Expected an IANA time zone" });
    }
  });

export const EvidenceRefSchema = z.string().min(1).max(500);

export const DestinationEpochSchema = z.strictObject({
  conversationId: EntityIdSchema,
  participantEpochId: EntityIdSchema,
  participantSetDigest: z.string().regex(/^[a-f0-9]{64}$/u),
  audience: z.enum(["private", "group"]),
});
export type DestinationEpoch = z.infer<typeof DestinationEpochSchema>;

export const LocalClockAnchorSchema = z.strictObject({
  kind: z.literal("local_clock"),
  time: LocalTimeSchema,
  dayOffset: z.number().int().min(-14).max(14).default(0),
});

export const InstantAnchorSchema = z.strictObject({
  kind: z.literal("instant"),
  at: InstantSchema,
});

export const SemanticAnchorSchema = z.discriminatedUnion("kind", [
  LocalClockAnchorSchema,
  InstantAnchorSchema,
]);
export type SemanticAnchor = z.infer<typeof SemanticAnchorSchema>;

export const RelativeMomentSchema = z.strictObject({
  kind: z.literal("relative"),
  anchor: z.enum(["event", "deadline"]),
  offsetMinutes: z
    .number()
    .int()
    .min(-60 * 24 * 30)
    .max(60 * 24 * 30),
});

export const SemanticMomentSchema = z.discriminatedUnion("kind", [
  LocalClockAnchorSchema,
  InstantAnchorSchema,
  RelativeMomentSchema,
]);
export type SemanticMoment = z.infer<typeof SemanticMomentSchema>;

export const SemanticTimePlanSchema = z
  .strictObject({
    timeZone: TimeZoneSchema,
    event: SemanticAnchorSchema.nullable(),
    deadline: SemanticAnchorSchema.nullable(),
    preparationMinutes: z
      .number()
      .int()
      .min(0)
      .max(60 * 24 * 30)
      .default(0),
    travelMinutes: z
      .number()
      .int()
      .min(0)
      .max(60 * 24 * 7)
      .default(0),
    earliestUseful: SemanticMomentSchema,
    lastResponsible: SemanticMomentSchema,
  })
  .superRefine((plan, context) => {
    if (plan.event === null && plan.deadline === null) {
      context.addIssue({
        code: "custom",
        message: "A semantic plan requires an event or deadline",
      });
    }
    for (const [name, moment] of [
      ["earliestUseful", plan.earliestUseful],
      ["lastResponsible", plan.lastResponsible],
    ] as const) {
      if (moment.kind === "relative" && plan[moment.anchor] === null) {
        context.addIssue({
          code: "custom",
          path: [name],
          message: `Relative moment requires a ${moment.anchor}`,
        });
      }
    }
  });
export type SemanticTimePlan = z.infer<typeof SemanticTimePlanSchema>;

export const ResolvedTimePlanSchema = z.strictObject({
  timeZone: TimeZoneSchema,
  localDate: LocalDateSchema,
  eventAt: InstantSchema.nullable(),
  deadlineAt: InstantSchema.nullable(),
  preparationMinutes: z.number().int().nonnegative(),
  travelMinutes: z.number().int().nonnegative(),
  earliestUsefulAt: InstantSchema,
  lastResponsibleAt: InstantSchema,
  resolutionPolicy: z.literal("wall_clock_compatible"),
});
export type ResolvedTimePlan = z.infer<typeof ResolvedTimePlanSchema>;

export const NotificationCategorySchema = z.enum([
  "coverage_opening",
  "coverage_reminder",
  "coverage_steward_escalation",
  "coverage_state_change",
  "coverage_closure",
]);
export type NotificationCategory = z.infer<typeof NotificationCategorySchema>;

export const QuietHoursPolicySchema = z
  .strictObject({
    personId: EntityIdSchema,
    timeZone: TimeZoneSchema,
    startLocalTime: LocalTimeSchema,
    endLocalTime: LocalTimeSchema,
    allowLastResponsibleOverrideFor: z.array(NotificationCategorySchema).max(4).default([]),
  })
  .refine((policy) => policy.startLocalTime !== policy.endLocalTime, {
    message: "Quiet hours may not cover an ambiguous full day",
  });
export type QuietHoursPolicy = z.infer<typeof QuietHoursPolicySchema>;

export const WeeklyRecurrenceSchema = z.strictObject({
  kind: z.literal("weekly"),
  weekdays: z
    .array(z.number().int().min(1).max(7))
    .min(1)
    .max(7)
    .refine((values) => new Set(values).size === values.length, "Weekdays must be unique"),
  intervalWeeks: z.number().int().min(1).max(52).default(1),
  startsOn: LocalDateSchema,
  endsOn: LocalDateSchema.nullable().default(null),
  excludedDates: z.array(LocalDateSchema).max(500).default([]),
});

export const OnceRecurrenceSchema = z.strictObject({
  kind: z.literal("once"),
  on: LocalDateSchema,
});

export const RoutineRecurrenceSchema = z.discriminatedUnion("kind", [
  WeeklyRecurrenceSchema,
  OnceRecurrenceSchema,
]);
export type RoutineRecurrence = z.infer<typeof RoutineRecurrenceSchema>;

export const StandingCoverageAuthorizationSchema = z
  .strictObject({
    holderPersonId: EntityIdSchema,
    authorizedByPersonId: EntityIdSchema,
    authorizationKind: z.enum(["created", "approved"]),
    authorizedAt: InstantSchema,
  })
  .refine((authorization) => authorization.holderPersonId === authorization.authorizedByPersonId, {
    message: "Standing coverage must be created or approved by its holder",
  });
export type StandingCoverageAuthorization = z.infer<typeof StandingCoverageAuthorizationSchema>;

export const RoutineSchema = z.strictObject({
  routineId: EntityIdSchema,
  householdId: EntityIdSchema,
  version: z.number().int().positive(),
  currentRevision: z.number().int().positive(),
  status: z.enum(["active", "paused", "retired"]),
});
export type Routine = z.infer<typeof RoutineSchema>;

export const RoutineRevisionSchema = z
  .strictObject({
    routineId: EntityIdSchema,
    revision: z.number().int().positive(),
    title: z.string().trim().min(1).max(200),
    minimumSharedMeaning: z.string().trim().min(1).max(500),
    recurrence: RoutineRecurrenceSchema,
    timePlan: SemanticTimePlanSchema,
    notificationMode: z.enum(["exceptions_only", "always", "silent"]).default("exceptions_only"),
    destination: DestinationEpochSchema,
    proposedHolderPersonId: EntityIdSchema.nullable(),
    standingCoverage: StandingCoverageAuthorizationSchema.nullable(),
    sourceRevisionRefs: z.array(EvidenceRefSchema).max(100),
    effectiveFrom: LocalDateSchema,
    effectiveThrough: LocalDateSchema.nullable(),
    createdAt: InstantSchema,
    createdByPersonId: EntityIdSchema,
  })
  .superRefine((revision, context) => {
    if (
      revision.standingCoverage !== null &&
      revision.proposedHolderPersonId !== revision.standingCoverage.holderPersonId
    ) {
      context.addIssue({
        code: "custom",
        path: ["standingCoverage"],
        message: "Standing coverage must match the routine holder",
      });
    }
    if (
      revision.effectiveThrough !== null &&
      Temporal.PlainDate.compare(revision.effectiveThrough, revision.effectiveFrom) < 0
    ) {
      context.addIssue({
        code: "custom",
        path: ["effectiveThrough"],
        message: "Routine revision cannot end before it begins",
      });
    }
  });
export type RoutineRevision = z.infer<typeof RoutineRevisionSchema>;

export const RoutineOccurrenceStatusSchema = z.enum(["materialized", "skipped", "cancelled"]);

export const RoutineOccurrenceSchema = z.strictObject({
  occurrenceId: EntityIdSchema,
  materializationKey: z.string().min(1).max(600),
  routineId: EntityIdSchema,
  routineRevision: z.number().int().positive(),
  localDate: LocalDateSchema,
  version: z.number().int().positive(),
  supersedesVersion: z.number().int().positive().nullable(),
  planVersion: z.number().int().positive(),
  status: RoutineOccurrenceStatusSchema,
  title: z.string().trim().min(1).max(200),
  minimumSharedMeaning: z.string().trim().min(1).max(500),
  timing: ResolvedTimePlanSchema,
  notificationMode: z.enum(["exceptions_only", "always", "silent"]),
  destination: DestinationEpochSchema,
  proposedHolderPersonId: EntityIdSchema.nullable(),
  standingCoverage: StandingCoverageAuthorizationSchema.nullable(),
  sourceRevisionRefs: z.array(EvidenceRefSchema).max(100),
  materializedAt: InstantSchema,
});
export type RoutineOccurrence = z.infer<typeof RoutineOccurrenceSchema>;

export const CoverageStateSchema = z.enum([
  "provisional",
  "open",
  "awaiting_response",
  "covered",
  "at_risk",
  "cancelled",
  "superseded",
  "dismissed",
  "expired_uncovered",
]);
export type CoverageState = z.infer<typeof CoverageStateSchema>;

export const CoverageNotificationRecordSchema = z.strictObject({
  notificationId: EntityIdSchema,
  category: NotificationCategorySchema,
  cycle: z.number().int().positive(),
  sentAt: InstantSchema,
});
export type CoverageNotificationRecord = z.infer<typeof CoverageNotificationRecordSchema>;

export const CoverageAcknowledgmentSchema = z.strictObject({
  personId: EntityIdSchema,
  acknowledgedAt: InstantSchema,
  kind: z.enum(["explicit_self", "standing_routine_self_authorized"]),
  holderDisclosure: z.enum(["shared", "minimum_only"]),
});
export type CoverageAcknowledgment = z.infer<typeof CoverageAcknowledgmentSchema>;

export const CoverageLoopSchema = z
  .strictObject({
    loopId: EntityIdSchema,
    householdId: EntityIdSchema,
    version: z.number().int().positive(),
    state: CoverageStateSchema,
    minimumSharedMeaning: z.string().trim().min(1).max(500),
    unresolvedFacts: z.array(z.string().trim().min(1).max(300)).max(20),
    proposedHolderPersonId: EntityIdSchema.nullable(),
    acknowledgment: CoverageAcknowledgmentSchema.nullable(),
    timing: ResolvedTimePlanSchema,
    planVersion: z.number().int().positive(),
    notificationMode: z.enum(["exceptions_only", "always", "silent"]),
    destination: DestinationEpochSchema,
    sourceEvidenceRefs: z.array(EvidenceRefSchema).max(100),
    routineOccurrence: z
      .strictObject({
        occurrenceId: EntityIdSchema,
        occurrenceVersion: z.number().int().positive(),
        routineId: EntityIdSchema,
        routineRevision: z.number().int().positive(),
      })
      .nullable(),
    attentionCycle: z.number().int().positive(),
    notificationHistory: z.array(CoverageNotificationRecordSchema).max(100),
    lastTransitionAt: InstantSchema,
  })
  .superRefine((loop, context) => {
    if (loop.state === "provisional" && loop.unresolvedFacts.length === 0) {
      context.addIssue({ code: "custom", message: "A provisional loop requires unresolved facts" });
    }
    if (loop.state === "covered" && loop.acknowledgment === null) {
      context.addIssue({ code: "custom", message: "A covered loop requires self-acknowledgment" });
    }
    if (loop.state !== "covered" && loop.acknowledgment !== null) {
      context.addIssue({ code: "custom", message: "Only a covered loop may have current acknowledgment" });
    }
  });
export type CoverageLoop = z.infer<typeof CoverageLoopSchema>;

export const CoverageTransitionSchema = z.strictObject({
  transitionId: EntityIdSchema,
  loopId: EntityIdSchema,
  fromState: CoverageStateSchema,
  toState: CoverageStateSchema,
  fromVersion: z.number().int().positive(),
  toVersion: z.number().int().positive(),
  kind: z.enum([
    "facts_resolved",
    "coverage_revised",
    "coverage_requested",
    "coverage_acknowledged",
    "coverage_declined_privately",
    "coverage_at_risk",
    "coverage_participant_revoked",
    "cancelled",
    "superseded",
    "dismissed",
    "expired_uncovered",
  ]),
  actorPersonId: EntityIdSchema.nullable(),
  evidenceRefs: z.array(EvidenceRefSchema).max(100),
  occurredAt: InstantSchema,
});
export type CoverageTransition = z.infer<typeof CoverageTransitionSchema>;

export const MinimumSharedCoverageStatusSchema = z.strictObject({
  kind: z.enum([
    "coverage_still_open",
    "coverage_recorded",
    "coverage_at_risk",
    "coverage_cancelled",
    "coverage_dismissed",
    "coverage_expired_uncovered",
  ]),
  minimumSharedMeaning: z.string().trim().min(1).max(500),
  holderPersonId: EntityIdSchema.nullable(),
  destination: DestinationEpochSchema,
  loopVersion: z.number().int().positive(),
});
export type MinimumSharedCoverageStatus = z.infer<typeof MinimumSharedCoverageStatusSchema>;

export const CoverageTimerSchema = z.strictObject({
  timerId: EntityIdSchema,
  loopId: EntityIdSchema,
  loopVersion: z.number().int().positive(),
  planVersion: z.number().int().positive(),
  attentionCycle: z.number().int().positive(),
  participantEpochId: EntityIdSchema,
  participantSetDigest: z.string().regex(/^[a-f0-9]{64}$/u),
  category: z.enum(["coverage_opening", "coverage_reminder", "coverage_steward_escalation"]),
  dueAt: InstantSchema,
});
export type CoverageTimer = z.infer<typeof CoverageTimerSchema>;

export const NeutralNotificationPlanSchema = z.strictObject({
  notificationId: EntityIdSchema,
  loopId: EntityIdSchema,
  loopVersion: z.number().int().positive(),
  planVersion: z.number().int().positive(),
  attentionCycle: z.number().int().positive(),
  category: NotificationCategorySchema,
  destination: DestinationEpochSchema,
  sendAt: InstantSchema,
  template: z.enum([
    "coverage_open_question",
    "coverage_still_open_question",
    "coverage_recorded",
    "coverage_at_risk",
    "coverage_cancelled",
    "coverage_dismissed",
    "coverage_expired_uncovered",
  ]),
  minimumSharedMeaning: z.string().trim().min(1).max(500),
  dueAt: InstantSchema,
  holderPersonId: EntityIdSchema.nullable(),
});
export type NeutralNotificationPlan = z.infer<typeof NeutralNotificationPlanSchema>;

export type CoordinationErrorCode =
  | "invalid_input"
  | "version_conflict"
  | "invalid_transition"
  | "not_proposed_holder"
  | "private_decline_required"
  | "too_early_to_expire"
  | "routine_revision_mismatch"
  | "routine_not_active"
  | "date_not_in_recurrence"
  | "invalid_time_plan"
  | "write_not_authorized"
  | "stale_timer"
  | "notification_not_useful";

export class CoordinationError extends Error {
  public readonly code: CoordinationErrorCode;

  public constructor(code: CoordinationErrorCode) {
    super(code);
    this.name = "CoordinationError";
    this.code = code;
  }
}
