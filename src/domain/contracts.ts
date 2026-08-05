import { createHash } from "node:crypto";
import { Temporal } from "@js-temporal/polyfill";
import { z } from "zod";

const APP_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]*$/;
const SOURCE_CLASS_PATTERN = /^[a-z][a-z0-9_.-]*$/;
const SHA256_PATTERN = /^sha256:[a-f0-9]{64}$/;

function appIdSchema<T extends string>(name: T) {
  return z
    .string()
    .min(1)
    .max(256)
    .regex(APP_ID_PATTERN, `${name} must be an app-owned stable identifier`)
    .brand<T>();
}

export const HouseholdIdSchema = appIdSchema("HouseholdId");
export const AdultIdSchema = appIdSchema("AdultId");
export const SignalIdSchema = appIdSchema("SignalId");
export const EpisodeIdSchema = appIdSchema("EpisodeId");
export const EvidenceIdSchema = appIdSchema("EvidenceId");
export const PolicyIdSchema = appIdSchema("PolicyId");
export const PolicyCandidateIdSchema = appIdSchema("PolicyCandidateId");
export const ApprovalIdSchema = appIdSchema("ApprovalId");
export const MemoryCandidateIdSchema = appIdSchema("MemoryCandidateId");
export const MemoryIdSchema = appIdSchema("MemoryId");
export const WorkerJobIdSchema = appIdSchema("WorkerJobId");
export const WorkerResultIdSchema = appIdSchema("WorkerResultId");
export const ContextGrantIdSchema = appIdSchema("ContextGrantId");
export const CapabilityGrantIdSchema = appIdSchema("CapabilityGrantId");
export const OutputContractRefSchema = appIdSchema("OutputContractRef");
export const TemporalPlanIdSchema = appIdSchema("TemporalPlanId");
export const TemporalTriggerIdSchema = appIdSchema("TemporalTriggerId");
export const RoutineAnchorIdSchema = appIdSchema("RoutineAnchorId");
export const TimerIdSchema = appIdSchema("TimerId");
export const ExternalActionIdSchema = appIdSchema("ExternalActionId");
export const OutboxIntentIdSchema = appIdSchema("OutboxIntentId");
export const ConversationIdSchema = appIdSchema("ConversationId");
export const EffectReceiptIdSchema = appIdSchema("EffectReceiptId");

export type HouseholdId = z.infer<typeof HouseholdIdSchema>;
export type AdultId = z.infer<typeof AdultIdSchema>;
export type SignalId = z.infer<typeof SignalIdSchema>;
export type EpisodeId = z.infer<typeof EpisodeIdSchema>;
export type EvidenceId = z.infer<typeof EvidenceIdSchema>;
export type PolicyId = z.infer<typeof PolicyIdSchema>;
export type ApprovalId = z.infer<typeof ApprovalIdSchema>;
export type WorkerJobId = z.infer<typeof WorkerJobIdSchema>;

export const InstantStringSchema = z
  .string()
  .superRefine((value, context) => {
    try {
      Temporal.Instant.from(value);
    } catch {
      context.addIssue({ code: "custom", message: "must be an ISO 8601 instant" });
    }
  })
  .transform((value) => Temporal.Instant.from(value).toString());

export const LocalDateSchema = z.string().superRefine((value, context) => {
  try {
    if (Temporal.PlainDate.from(value).toString() !== value) {
      context.addIssue({ code: "custom", message: "must be a canonical ISO date" });
    }
  } catch {
    context.addIssue({ code: "custom", message: "must be an ISO date" });
  }
});

export const LocalTimeSchema = z
  .string()
  .regex(/^\d{2}:\d{2}$/, "must be HH:mm")
  .superRefine((value, context) => {
    try {
      Temporal.PlainTime.from(value);
    } catch {
      context.addIssue({ code: "custom", message: "must be a valid local time" });
    }
  });

export const TimeZoneSchema = z
  .string()
  .min(1)
  .max(100)
  .superRefine((value, context) => {
    try {
      Temporal.Now.zonedDateTimeISO(value);
    } catch {
      context.addIssue({ code: "custom", message: "must be an IANA time zone" });
    }
  });

export type InstantString = z.infer<typeof InstantStringSchema>;

export const SourceClassSchema = z
  .string()
  .min(1)
  .max(100)
  .regex(SOURCE_CLASS_PATTERN, "must be a stable source class");

export const ContentDigestSchema = z.string().regex(SHA256_PATTERN);
export const ActionDigestSchema = z.string().regex(SHA256_PATTERN);
export const ConfidenceSchema = z.number().min(0).max(1);
export const SensitivitySchema = z.enum(["ordinary", "sensitive", "highly_sensitive"]);

export const FactualTextSchema = z.string().trim().min(1).max(500);
export const NeutralFactualTextSchema = FactualTextSchema.superRefine((value, context) => {
  const blamePatterns = [
    /\b(?:forgot|neglected|lazy|fault|blame|careless|irresponsible)\b/i,
    /\bstill has not\b/i,
    /\b(?:hasn't|didn't)\b/i,
    /\byou (?:failed|ignored)\b/i,
  ];

  if (blamePatterns.some((pattern) => pattern.test(value))) {
    context.addIssue({ code: "custom", message: "must be neutral and factual" });
  }
});

export const NeutralDisplayTextSchema = NeutralFactualTextSchema.pipe(
  z
    .string()
    .max(240)
    .superRefine((value, context) => {
      if (/\bfailed\b/i.test(value)) {
        context.addIssue({ code: "custom", message: "display text must not assign failure" });
      }
    }),
);

export const JobScopeSchema = z.strictObject({
  kind: z.literal("job"),
  jobId: WorkerJobIdSchema,
});

export const PersonalScopeSchema = z.strictObject({
  kind: z.literal("personal"),
  adultId: AdultIdSchema,
});

export const HouseholdScopeSchema = z.strictObject({
  kind: z.literal("household"),
});

export const DataScopeSchema = z.discriminatedUnion("kind", [
  JobScopeSchema,
  PersonalScopeSchema,
  HouseholdScopeSchema,
]);

export const DurableScopeSchema = z.discriminatedUnion("kind", [PersonalScopeSchema, HouseholdScopeSchema]);

export type DataScope = z.infer<typeof DataScopeSchema>;
export type DurableScope = z.infer<typeof DurableScopeSchema>;

export const EvidenceRefSchema = z.strictObject({
  evidenceId: EvidenceIdSchema,
  source: z.enum(["adult", "linq", "gmail", "calendar", "attachment", "worker", "effect"]),
  sourceRef: z.string().min(1).max(500),
  scope: DataScopeSchema,
  observedAt: InstantStringSchema,
  revision: z.number().int().positive(),
  contentDigest: ContentDigestSchema.optional(),
});

export type EvidenceRef = z.infer<typeof EvidenceRefSchema>;

export const AbsoluteMomentSchema = z.strictObject({
  kind: z.literal("instant"),
  at: InstantStringSchema,
});

export const LocalMomentSchema = z.strictObject({
  kind: z.literal("local"),
  date: LocalDateSchema,
  time: LocalTimeSchema,
  timeZone: TimeZoneSchema,
  disambiguation: z.enum(["compatible", "earlier", "later", "reject"]),
});

export const RoutineAnchorMomentSchema = z.strictObject({
  kind: z.literal("routine_anchor"),
  anchorId: RoutineAnchorIdSchema,
  date: LocalDateSchema,
  offsetMinutes: z.number().int().min(-10_080).max(10_080),
  disambiguation: z.enum(["compatible", "earlier", "later", "reject"]),
});

export const SemanticMomentSchema = z.discriminatedUnion("kind", [
  AbsoluteMomentSchema,
  LocalMomentSchema,
  RoutineAnchorMomentSchema,
]);

export type SemanticMoment = z.infer<typeof SemanticMomentSchema>;

export const RoutineAnchorSchema = z
  .strictObject({
    anchorId: RoutineAnchorIdSchema,
    label: FactualTextSchema,
    timeZone: TimeZoneSchema,
    localTime: LocalTimeSchema,
    daysOfWeek: z.array(z.number().int().min(1).max(7)).min(1).max(7),
  })
  .superRefine((anchor, context) => {
    const canonical = [...new Set(anchor.daysOfWeek)].sort((left, right) => left - right);
    if (
      canonical.length !== anchor.daysOfWeek.length ||
      canonical.some((day, index) => day !== anchor.daysOfWeek[index])
    ) {
      context.addIssue({
        code: "custom",
        path: ["daysOfWeek"],
        message: "daysOfWeek must contain unique ISO weekdays in ascending order",
      });
    }
  });

export type RoutineAnchor = z.infer<typeof RoutineAnchorSchema>;

export const TemporalTriggerDefinitionSchema = z.strictObject({
  triggerId: TemporalTriggerIdSchema,
  timerId: TimerIdSchema,
  kind: z.enum(["reminder", "window_check"]),
  at: SemanticMomentSchema,
});

export const SemanticTimePlanSchema = z
  .strictObject({
    planId: TemporalPlanIdSchema,
    version: z.number().int().positive(),
    timeZone: TimeZoneSchema,
    event: SemanticMomentSchema.optional(),
    deadline: SemanticMomentSchema.optional(),
    earliestUseful: SemanticMomentSchema.optional(),
    lastResponsible: SemanticMomentSchema.optional(),
    usefulLeadMinutes: z.number().int().min(0).max(525_600),
    preparationMinutes: z.number().int().min(0).max(525_600),
    finalBufferMinutes: z.number().int().min(0).max(525_600),
    triggers: z.array(TemporalTriggerDefinitionSchema).max(50),
  })
  .superRefine((plan, context) => {
    if (plan.event === undefined && plan.deadline === undefined) {
      context.addIssue({
        code: "custom",
        message: "a temporal plan needs an event or deadline",
        path: ["event"],
      });
    }

    const ids = new Set<string>();
    const timerIds = new Set<string>();
    for (const [index, trigger] of plan.triggers.entries()) {
      if (ids.has(trigger.triggerId)) {
        context.addIssue({ code: "custom", message: "duplicate triggerId", path: ["triggers", index] });
      }
      if (timerIds.has(trigger.timerId)) {
        context.addIssue({ code: "custom", message: "duplicate timerId", path: ["triggers", index] });
      }
      ids.add(trigger.triggerId);
      timerIds.add(trigger.timerId);
    }
  });

export type SemanticTimePlan = z.infer<typeof SemanticTimePlanSchema>;

export const ResolvedTemporalTriggerSchema = z.strictObject({
  triggerId: TemporalTriggerIdSchema,
  timerId: TimerIdSchema,
  kind: z.enum(["reminder", "window_check"]),
  at: InstantStringSchema,
  status: z.enum(["pending", "emitted", "skipped"]),
});

export const ResolvedTimePlanSchema = z
  .strictObject({
    definition: SemanticTimePlanSchema,
    eventAt: InstantStringSchema.optional(),
    deadlineAt: InstantStringSchema.optional(),
    referenceAt: InstantStringSchema,
    earliestUsefulAt: InstantStringSchema,
    lastResponsibleAt: InstantStringSchema,
    triggers: z.array(ResolvedTemporalTriggerSchema).max(50),
  })
  .superRefine((plan, context) => {
    if (
      Temporal.Instant.compare(plan.earliestUsefulAt, plan.lastResponsibleAt) > 0 ||
      Temporal.Instant.compare(plan.lastResponsibleAt, plan.referenceAt) > 0
    ) {
      context.addIssue({ code: "custom", message: "resolved useful window is inconsistent" });
    }
    const definitions = new Map(
      plan.definition.triggers.map((trigger) => [trigger.triggerId, trigger] as const),
    );
    if (
      plan.triggers.length !== plan.definition.triggers.length ||
      plan.triggers.some((trigger) => {
        const definition = definitions.get(trigger.triggerId);
        return (
          definition === undefined ||
          definition.timerId !== trigger.timerId ||
          definition.kind !== trigger.kind
        );
      })
    ) {
      context.addIssue({ code: "custom", message: "resolved triggers must match their definitions" });
    }
  });

export type ResolvedTimePlan = z.infer<typeof ResolvedTimePlanSchema>;

export const PrivateSourceMatcherSchema = z.discriminatedUnion("source", [
  z.strictObject({
    source: z.literal("gmail"),
    accountRefDigest: ContentDigestSchema,
    senderIdentityDigest: ContentDigestSchema,
  }),
  z.strictObject({
    source: z.literal("calendar"),
    accountRefDigest: ContentDigestSchema,
  }),
]);

export type PrivateSourceMatcher = z.infer<typeof PrivateSourceMatcherSchema>;

export const PromotionAuthoritySchema = z.discriminatedUnion("kind", [
  z.strictObject({
    kind: z.literal("approval"),
    approvalId: ApprovalIdSchema,
  }),
  z.strictObject({
    kind: z.literal("policy"),
    policyId: PolicyIdSchema,
    policyVersion: z.number().int().positive(),
  }),
]);

export type PromotionAuthority = z.infer<typeof PromotionAuthoritySchema>;

export const EpisodeTypeSchema = z.enum(["commitment", "research", "meal_plan"]);
export const CommitmentStateSchema = z.enum([
  "proposed",
  "awaiting_acknowledgement",
  "active",
  "blocked",
  "completed",
  "dismissed",
  "superseded",
  "failed",
]);

export const CommitmentOwnerSchema = z.discriminatedUnion("status", [
  z.strictObject({ status: z.literal("unassigned") }),
  z.strictObject({
    status: z.literal("proposed"),
    adultId: AdultIdSchema,
    proposedAt: InstantStringSchema,
  }),
  z.strictObject({
    status: z.literal("acknowledged"),
    adultId: AdultIdSchema,
    proposedAt: InstantStringSchema,
    acknowledgedAt: InstantStringSchema,
  }),
]);

export const EpisodeOutcomeSchema = z.strictObject({
  kind: z.enum(["completed", "dismissed", "superseded", "failed"]),
  summary: NeutralFactualTextSchema,
  evidence: z.array(EvidenceRefSchema).max(20),
  recordedAt: InstantStringSchema,
});

export const FamilyEpisodeSchema = z
  .strictObject({
    episodeId: EpisodeIdSchema,
    householdId: HouseholdIdSchema,
    type: EpisodeTypeSchema,
    version: z.number().int().positive(),
    scope: DurableScopeSchema,
    state: CommitmentStateSchema,
    title: NeutralDisplayTextSchema,
    requiredOutcome: NeutralFactualTextSchema,
    owner: CommitmentOwnerSchema,
    evidence: z.array(EvidenceRefSchema).min(1).max(50),
    sourceClass: SourceClassSchema,
    sensitivity: SensitivitySchema,
    promotionAuthority: PromotionAuthoritySchema.optional(),
    sourceMatcher: PrivateSourceMatcherSchema.optional(),
    temporalPlan: ResolvedTimePlanSchema.optional(),
    blockedReason: NeutralFactualTextSchema.optional(),
    createdAt: InstantStringSchema,
    updatedAt: InstantStringSchema,
    outcome: EpisodeOutcomeSchema.optional(),
  })
  .superRefine((episode, context) => {
    if (episode.state === "awaiting_acknowledgement" && episode.owner.status !== "proposed") {
      context.addIssue({ code: "custom", message: "awaiting acknowledgement requires a proposed owner" });
    }
    if (["active", "blocked"].includes(episode.state) && episode.owner.status !== "acknowledged") {
      context.addIssue({ code: "custom", message: `${episode.state} requires an acknowledged owner` });
    }
    const terminal = ["completed", "dismissed", "superseded", "failed"].includes(episode.state);
    if (terminal !== (episode.outcome !== undefined)) {
      context.addIssue({ code: "custom", message: "terminal state and outcome must agree" });
    }
    if (episode.outcome !== undefined && episode.outcome.kind !== episode.state) {
      context.addIssue({ code: "custom", message: "outcome kind must match terminal episode state" });
    }
    if ((episode.state === "blocked") !== (episode.blockedReason !== undefined)) {
      context.addIssue({ code: "custom", message: "blocked state and reason must agree" });
    }
    if (episode.evidence.some((item) => item.scope.kind === "job")) {
      context.addIssue({ code: "custom", message: "job-scoped evidence cannot enter a durable episode" });
    }
    const personalAdults = new Set(
      episode.evidence.flatMap((item) => (item.scope.kind === "personal" ? [item.scope.adultId] : [])),
    );
    if (
      episode.scope.kind === "household" &&
      personalAdults.size > 0 &&
      episode.promotionAuthority === undefined
    ) {
      context.addIssue({ code: "custom", message: "household promotion must retain its authority" });
    }
    if (episode.promotionAuthority?.kind === "policy" && episode.sourceMatcher === undefined) {
      context.addIssue({
        code: "custom",
        message: "standing-policy promotion must retain its exact private source matcher",
      });
    }
    if (episode.scope.kind === "personal") {
      const scopeAdultId = episode.scope.adultId;
      if ([...personalAdults].some((adultId) => adultId !== scopeAdultId)) {
        context.addIssue({
          code: "custom",
          message: "personal episodes cannot expose another adult's data",
        });
      }
    }
  });

export type FamilyEpisode = z.infer<typeof FamilyEpisodeSchema>;

export const EpisodeProposalSchema = z.strictObject({
  episodeId: EpisodeIdSchema,
  type: EpisodeTypeSchema,
  targetScope: DurableScopeSchema,
  title: NeutralDisplayTextSchema,
  requiredOutcome: NeutralFactualTextSchema,
  proposedOwnerAdultId: AdultIdSchema.optional(),
  evidence: z.array(EvidenceRefSchema).min(1).max(50),
  sourceClass: SourceClassSchema,
  sensitivity: SensitivitySchema,
  temporalPlan: SemanticTimePlanSchema.optional(),
  promotionAuthority: PromotionAuthoritySchema.optional(),
  sourceMatcher: PrivateSourceMatcherSchema.optional(),
});

export type EpisodeProposal = z.infer<typeof EpisodeProposalSchema>;

export const ExternalActionKindSchema = z.enum([
  "send_email",
  "message_third_party",
  "submit_form",
  "book",
  "purchase",
  "payment",
  "cancel",
  "account_mutation",
  "calendar_update",
]);

const ExternalActionBaseShape = {
  actionId: ExternalActionIdSchema,
  summary: NeutralDisplayTextSchema,
  actionDigest: ActionDigestSchema,
  relevantDataDigest: ContentDigestSchema,
  requestedFor: DurableScopeSchema,
  evidence: z.array(EvidenceRefSchema).min(1).max(50),
} as const;

const NonCalendarExternalActionKindSchema = z.enum([
  "send_email",
  "message_third_party",
  "submit_form",
  "book",
  "purchase",
  "payment",
  "cancel",
  "account_mutation",
]);

export const CalendarEventCreateActionSchema = z
  .strictObject({
    ...ExternalActionBaseShape,
    kind: z.literal("calendar_update"),
    calendarActionVersion: z.literal(1),
    operation: z.literal("create"),
    householdId: HouseholdIdSchema,
    title: NeutralDisplayTextSchema,
    startsAt: InstantStringSchema,
    endsAt: InstantStringSchema,
    timeZone: TimeZoneSchema,
    requestedByAdultId: AdultIdSchema,
    availabilityAdultIds: z.array(AdultIdSchema).min(1).max(20),
    targetConnectionId: appIdSchema("ExternalConnectionId"),
    calendarId: z.literal("primary"),
    hasConflict: z.boolean(),
  })
  .superRefine((action, context) => {
    if (Temporal.Instant.compare(action.startsAt, action.endsAt) >= 0) {
      context.addIssue({ code: "custom", path: ["endsAt"], message: "calendar end must follow start" });
    }
    if (action.requestedFor.kind !== "household") {
      context.addIssue({
        code: "custom",
        path: ["requestedFor"],
        message: "calendar creation is currently household-scoped",
      });
    }
    const sortedAdults = [...new Set(action.availabilityAdultIds)].sort();
    if (
      sortedAdults.length !== action.availabilityAdultIds.length ||
      sortedAdults.some((adultId, index) => adultId !== action.availabilityAdultIds[index]) ||
      !sortedAdults.includes(action.requestedByAdultId)
    ) {
      context.addIssue({
        code: "custom",
        path: ["availabilityAdultIds"],
        message: "calendar availability adults must be unique, sorted, and include the requester",
      });
    }
    if (action.actionDigest !== calendarEventCreateActionDigest(action)) {
      context.addIssue({
        code: "custom",
        path: ["actionDigest"],
        message: "calendar action digest does not match its exact write payload",
      });
    }
  });

export type CalendarEventCreateAction = z.infer<typeof CalendarEventCreateActionSchema>;

export function calendarEventCreateActionDigest(action: {
  actionId: string;
  kind: "calendar_update";
  calendarActionVersion: 1;
  operation: "create";
  householdId: string;
  summary: string;
  relevantDataDigest: string;
  requestedFor: unknown;
  evidence: readonly unknown[];
  title: string;
  startsAt: string;
  endsAt: string;
  timeZone: string;
  requestedByAdultId: string;
  availabilityAdultIds: readonly string[];
  targetConnectionId: string;
  calendarId: "primary";
  hasConflict: boolean;
}): z.infer<typeof ActionDigestSchema> {
  return ActionDigestSchema.parse(
    `sha256:${createHash("sha256")
      .update(
        JSON.stringify([
          action.actionId,
          action.kind,
          action.calendarActionVersion,
          action.operation,
          action.householdId,
          action.summary,
          action.relevantDataDigest,
          action.requestedFor,
          action.evidence,
          action.title,
          action.startsAt,
          action.endsAt,
          action.timeZone,
          action.requestedByAdultId,
          action.availabilityAdultIds,
          action.targetConnectionId,
          action.calendarId,
          action.hasConflict,
        ]),
      )
      .digest("hex")}`,
  );
}

const NonCalendarExternalActionSchema = z.strictObject({
  ...ExternalActionBaseShape,
  kind: NonCalendarExternalActionKindSchema,
});

export const ExternalActionSchema = z.union([
  CalendarEventCreateActionSchema,
  NonCalendarExternalActionSchema,
]);

export type ExternalAction = z.infer<typeof ExternalActionSchema>;

export const ScopePromotionApprovalTargetSchema = z
  .strictObject({
    kind: z.literal("scope_promotion"),
    from: PersonalScopeSchema,
    to: HouseholdScopeSchema,
    evidenceIds: z.array(EvidenceIdSchema).min(1).max(50),
  })
  .superRefine((target, context) => {
    if (new Set(target.evidenceIds).size !== target.evidenceIds.length) {
      context.addIssue({ code: "custom", message: "promotion evidenceIds must be unique" });
    }
  });

export const ExternalActionApprovalTargetSchema = z.strictObject({
  kind: z.literal("external_action"),
  actionId: ExternalActionIdSchema,
  actionDigest: ActionDigestSchema,
  relevantDataDigest: ContentDigestSchema,
});

export const PolicyChangeApprovalTargetSchema = z.strictObject({
  kind: z.literal("policy_change"),
  policyCandidateId: PolicyCandidateIdSchema,
  policyDigest: ContentDigestSchema,
});

export const ApprovalTargetSchema = z.discriminatedUnion("kind", [
  ScopePromotionApprovalTargetSchema,
  ExternalActionApprovalTargetSchema,
  PolicyChangeApprovalTargetSchema,
]);

export const ApprovalRecordSchema = z
  .strictObject({
    approvalId: ApprovalIdSchema,
    householdId: HouseholdIdSchema,
    grantedByAdultId: AdultIdSchema,
    target: ApprovalTargetSchema,
    policyVersion: z.number().int().nonnegative(),
    grantedAt: InstantStringSchema,
    expiresAt: InstantStringSchema,
    status: z.enum(["active", "consumed", "revoked", "expired"]),
  })
  .superRefine((approval, context) => {
    if (Temporal.Instant.compare(approval.expiresAt, approval.grantedAt) <= 0) {
      context.addIssue({ code: "custom", message: "approval expiry must follow grant time" });
    }
  });

export type ApprovalRecord = z.infer<typeof ApprovalRecordSchema>;

export const SharingPolicyRuleSchema = z.strictObject({
  kind: z.literal("sharing"),
  from: PersonalScopeSchema,
  to: HouseholdScopeSchema,
  sourceClass: SourceClassSchema,
  maximumSensitivity: z.enum(["ordinary", "sensitive"]),
  sourceMatcher: PrivateSourceMatcherSchema,
});

export const RoutingPolicyRuleSchema = z.strictObject({
  kind: z.literal("routing"),
  scope: DurableScopeSchema,
  sourceClass: SourceClassSchema,
  decision: z.enum(["suppress", "private_review", "interrupt", "propose_episode"]),
});

export const TimingPolicyRuleSchema = z.strictObject({
  kind: z.literal("timing"),
  scope: DurableScopeSchema,
  localTime: LocalTimeSchema,
  timeZone: TimeZoneSchema,
});

export const InternalActionPolicyRuleSchema = z.strictObject({
  kind: z.literal("internal_action"),
  sourceClass: SourceClassSchema,
  action: z.enum(["calendar_projection", "reschedule_own_reminder"]),
});

export const PolicyRuleSchema = z.discriminatedUnion("kind", [
  SharingPolicyRuleSchema,
  RoutingPolicyRuleSchema,
  TimingPolicyRuleSchema,
  InternalActionPolicyRuleSchema,
]);

export type PolicyRule = z.infer<typeof PolicyRuleSchema>;

export const PolicyRecordSchema = z
  .strictObject({
    policyId: PolicyIdSchema,
    householdId: HouseholdIdSchema,
    version: z.number().int().positive(),
    status: z.enum(["active", "revoked"]),
    rule: PolicyRuleSchema,
    approvedByAdultId: AdultIdSchema,
    approvedAt: InstantStringSchema,
    revokedAt: InstantStringSchema.optional(),
  })
  .superRefine((policy, context) => {
    if ((policy.status === "revoked") !== (policy.revokedAt !== undefined)) {
      context.addIssue({ code: "custom", message: "revoked policy state and time must agree" });
    }
  });

export type PolicyRecord = z.infer<typeof PolicyRecordSchema>;

export const PolicyCandidateSchema = z.strictObject({
  candidateId: PolicyCandidateIdSchema,
  householdId: HouseholdIdSchema,
  proposedByJobId: WorkerJobIdSchema,
  basePolicyVersion: z.number().int().nonnegative(),
  rule: PolicyRuleSchema,
  direction: z.enum(["narrowing", "expanding"]),
  rationale: NeutralFactualTextSchema,
  createdAt: InstantStringSchema,
});

export type PolicyCandidate = z.infer<typeof PolicyCandidateSchema>;

export const MemoryCandidateSchema = z
  .strictObject({
    candidateId: MemoryCandidateIdSchema,
    householdId: HouseholdIdSchema,
    proposedBy: z.discriminatedUnion("kind", [
      z.strictObject({ kind: z.literal("worker"), jobId: WorkerJobIdSchema }),
      z.strictObject({ kind: z.literal("adult"), adultId: AdultIdSchema }),
    ]),
    kind: z.enum(["preference", "routine", "fact", "routing_observation"]),
    statement: FactualTextSchema,
    scope: DurableScopeSchema,
    sourceClass: SourceClassSchema,
    evidence: z.array(EvidenceRefSchema).min(1).max(50),
    confidence: ConfidenceSchema,
    sensitivity: SensitivitySchema,
    validFrom: InstantStringSchema,
    expiresAt: InstantStringSchema.optional(),
  })
  .superRefine((candidate, context) => {
    if (
      candidate.expiresAt !== undefined &&
      Temporal.Instant.compare(candidate.expiresAt, candidate.validFrom) <= 0
    ) {
      context.addIssue({ code: "custom", message: "memory candidate expiry must follow valid-from" });
    }
    if (candidate.evidence.some((item) => item.scope.kind === "job")) {
      context.addIssue({ code: "custom", message: "job-scoped evidence cannot become durable memory" });
    }
    const personalAdults = candidate.evidence.flatMap((item) =>
      item.scope.kind === "personal" ? [item.scope.adultId] : [],
    );
    let widensPersonalScope = false;
    if (candidate.scope.kind === "personal") {
      const scopeAdultId = candidate.scope.adultId;
      widensPersonalScope = personalAdults.some((adultId) => adultId !== scopeAdultId);
    }
    if ((candidate.scope.kind === "household" && personalAdults.length > 0) || widensPersonalScope) {
      context.addIssue({ code: "custom", message: "memory candidates cannot widen source visibility" });
    }
  });

export type MemoryCandidate = z.infer<typeof MemoryCandidateSchema>;

export const DurableMemorySchema = z
  .strictObject({
    memoryId: MemoryIdSchema,
    householdId: HouseholdIdSchema,
    kind: z.enum(["preference", "routine", "fact", "routing_observation"]),
    statement: FactualTextSchema,
    scope: DurableScopeSchema,
    sourceClass: SourceClassSchema,
    evidence: z.array(EvidenceRefSchema).min(1).max(50),
    confidence: ConfidenceSchema,
    sensitivity: SensitivitySchema,
    validFrom: InstantStringSchema,
    expiresAt: InstantStringSchema.optional(),
    confirmedByAdultId: AdultIdSchema,
    confirmedAt: InstantStringSchema,
    promotionAuthority: PromotionAuthoritySchema.optional(),
    status: z.enum(["active", "revoked", "superseded"]),
    revokedAt: InstantStringSchema.optional(),
    revokedByAdultId: AdultIdSchema.optional(),
  })
  .superRefine((memory, context) => {
    if (memory.expiresAt !== undefined && Temporal.Instant.compare(memory.expiresAt, memory.validFrom) <= 0) {
      context.addIssue({ code: "custom", message: "memory expiry must follow valid-from" });
    }
    if (memory.evidence.some((item) => item.scope.kind === "job")) {
      context.addIssue({ code: "custom", message: "job-scoped evidence cannot become durable memory" });
    }
    const personalAdults = memory.evidence.flatMap((item) =>
      item.scope.kind === "personal" ? [item.scope.adultId] : [],
    );
    if (
      memory.scope.kind === "household" &&
      personalAdults.length > 0 &&
      memory.promotionAuthority === undefined
    ) {
      context.addIssue({ code: "custom", message: "promoted memory must retain its authority" });
    }
    if (memory.scope.kind === "personal") {
      const scopeAdultId = memory.scope.adultId;
      if (personalAdults.some((adultId) => adultId !== scopeAdultId)) {
        context.addIssue({
          code: "custom",
          message: "personal memory cannot expose another adult's data",
        });
      }
    }
    const hasRevocation = memory.revokedAt !== undefined || memory.revokedByAdultId !== undefined;
    if ((memory.status === "revoked") !== hasRevocation) {
      context.addIssue({ code: "custom", message: "revoked memory state and authority must agree" });
    }
    if ((memory.revokedAt === undefined) !== (memory.revokedByAdultId === undefined)) {
      context.addIssue({ code: "custom", message: "memory revocation time and adult must both be recorded" });
    }
  });

export type DurableMemory = z.infer<typeof DurableMemorySchema>;

export const ProposedMessageSchema = z.strictObject({
  proposalId: appIdSchema("MessageProposalId"),
  targetScope: DurableScopeSchema,
  purpose: z.enum(["clarifying_question", "status_update"]),
  body: NeutralFactualTextSchema,
  evidence: z.array(EvidenceRefSchema).min(1).max(50),
  sourceClass: SourceClassSchema,
  sensitivity: SensitivitySchema,
  promotionAuthority: PromotionAuthoritySchema.optional(),
});

export const ProposedExternalActionSchema = z.strictObject({
  action: ExternalActionSchema,
  approvalId: ApprovalIdSchema.optional(),
  promotionAuthority: PromotionAuthoritySchema.optional(),
});

export const WorkerJobSchema = z.strictObject({
  jobId: WorkerJobIdSchema,
  householdId: HouseholdIdSchema,
  objective: NeutralFactualTextSchema,
  baseHouseholdVersion: z.number().int().nonnegative(),
  policyVersion: z.number().int().nonnegative(),
  sourceEvidenceIds: z.array(EvidenceIdSchema).max(50),
  contextGrantId: ContextGrantIdSchema,
  capabilityGrantIds: z.array(CapabilityGrantIdSchema).max(20),
  budget: z.strictObject({
    maxSteps: z.number().int().positive().max(1_000),
    maxDurationMs: z.number().int().positive().max(86_400_000),
  }),
  deadline: InstantStringSchema,
  outputContractRef: OutputContractRefSchema,
});

export type WorkerJob = z.infer<typeof WorkerJobSchema>;

export const WorkerProposalSchema = z.strictObject({
  resultId: WorkerResultIdSchema,
  jobId: WorkerJobIdSchema,
  householdId: HouseholdIdSchema,
  baseHouseholdVersion: z.number().int().nonnegative(),
  basePolicyVersion: z.number().int().nonnegative(),
  completedAt: InstantStringSchema,
  confidence: ConfidenceSchema,
  evidence: z.array(EvidenceRefSchema).max(100),
  episodeProposals: z.array(EpisodeProposalSchema).max(20),
  messageProposals: z.array(ProposedMessageSchema).max(20),
  actionProposals: z.array(ProposedExternalActionSchema).max(20),
  memoryCandidates: z.array(MemoryCandidateSchema).max(50),
  policyCandidates: z.array(PolicyCandidateSchema).max(20),
  unresolvedQuestions: z.array(NeutralFactualTextSchema).max(20),
  diagnostics: z.strictObject({
    warnings: z.array(z.string().min(1).max(500)).max(50),
  }),
});

export type WorkerProposal = z.infer<typeof WorkerProposalSchema>;

export const PendingExternalActionSchema = z.strictObject({
  action: ExternalActionSchema,
  state: z.enum(["awaiting_approval", "authorized", "executing", "succeeded", "failed", "unknown"]),
  approvalId: ApprovalIdSchema.optional(),
  promotionAuthority: PromotionAuthoritySchema.optional(),
  proposedAt: InstantStringSchema,
  updatedAt: InstantStringSchema,
  effectReceipt: z
    .strictObject({
      receiptId: EffectReceiptIdSchema,
      outcome: z.enum(["succeeded", "failed", "unknown"]),
      recordedAt: InstantStringSchema,
      providerReference: z.string().min(1).max(500).optional(),
    })
    .optional(),
});

export type PendingExternalAction = z.infer<typeof PendingExternalActionSchema>;

const OutboxBaseShape = {
  intentId: OutboxIntentIdSchema,
  householdId: HouseholdIdSchema,
  idempotencyKey: z.string().min(1).max(500),
  createdFromSignalId: SignalIdSchema,
} as const;

export const ScheduleTimerIntentSchema = z.strictObject({
  ...OutboxBaseShape,
  kind: z.literal("schedule_timer"),
  timerId: TimerIdSchema,
  episodeId: EpisodeIdSchema,
  temporalPlanVersion: z.number().int().positive(),
  triggerId: TemporalTriggerIdSchema,
  at: InstantStringSchema,
});

export const CancelTimerIntentSchema = z.strictObject({
  ...OutboxBaseShape,
  kind: z.literal("cancel_timer"),
  timerId: TimerIdSchema,
  episodeId: EpisodeIdSchema,
  temporalPlanVersion: z.number().int().positive(),
});

export const SendMessageIntentSchema = z.strictObject({
  ...OutboxBaseShape,
  kind: z.literal("send_message"),
  targetScope: DurableScopeSchema,
  messageClass: z.enum(["reminder", "missed_window", "approval_request", "clarifying_question", "status"]),
  body: NeutralFactualTextSchema,
  episodeId: EpisodeIdSchema.optional(),
  evidenceIds: z.array(EvidenceIdSchema).max(50).optional(),
  promotionAuthority: PromotionAuthoritySchema.optional(),
});

export const ExecuteExternalActionIntentSchema = z.strictObject({
  ...OutboxBaseShape,
  kind: z.literal("execute_external_action"),
  action: ExternalActionSchema,
  approvalId: ApprovalIdSchema,
});

export const EnqueueWorkerIntentSchema = z.strictObject({
  ...OutboxBaseShape,
  kind: z.literal("enqueue_worker"),
  job: WorkerJobSchema,
});

export const OutboxIntentSchema = z.discriminatedUnion("kind", [
  ScheduleTimerIntentSchema,
  CancelTimerIntentSchema,
  SendMessageIntentSchema,
  ExecuteExternalActionIntentSchema,
  EnqueueWorkerIntentSchema,
]);

export type OutboxIntent = z.infer<typeof OutboxIntentSchema>;

export const HouseholdAggregateSchema = z
  .strictObject({
    schemaVersion: z.literal(1),
    householdId: HouseholdIdSchema,
    version: z.number().int().nonnegative(),
    policyVersion: z.number().int().nonnegative(),
    lastProcessedSequence: z.number().int().nonnegative(),
    timeZone: TimeZoneSchema,
    verifiedAdultIds: z.array(AdultIdSchema).min(1),
    routineAnchors: z.array(RoutineAnchorSchema).max(100),
    episodes: z.array(FamilyEpisodeSchema).max(10_000),
    policies: z.array(PolicyRecordSchema).max(10_000),
    policyCandidates: z.array(PolicyCandidateSchema).max(10_000),
    approvals: z.array(ApprovalRecordSchema).max(10_000),
    memoryCandidates: z.array(MemoryCandidateSchema).max(10_000),
    memories: z.array(DurableMemorySchema).max(10_000),
    pendingActions: z.array(PendingExternalActionSchema).max(10_000),
  })
  .superRefine((aggregate, context) => {
    const unique = (values: string[]) => new Set(values).size === values.length;
    const identityGroups = [
      aggregate.verifiedAdultIds,
      aggregate.routineAnchors.map((item) => item.anchorId),
      aggregate.episodes.map((item) => item.episodeId),
      aggregate.policies.map((item) => item.policyId),
      aggregate.policyCandidates.map((item) => item.candidateId),
      aggregate.approvals.map((item) => item.approvalId),
      aggregate.memoryCandidates.map((item) => item.candidateId),
      aggregate.memories.map((item) => item.memoryId),
      aggregate.pendingActions.map((item) => item.action.actionId),
    ];
    if (identityGroups.some((values) => !unique(values))) {
      context.addIssue({ code: "custom", message: "aggregate entity identifiers must be unique" });
    }
    const householdRecords = [
      ...aggregate.episodes,
      ...aggregate.policies,
      ...aggregate.policyCandidates,
      ...aggregate.approvals,
      ...aggregate.memoryCandidates,
      ...aggregate.memories,
    ];
    if (householdRecords.some((record) => record.householdId !== aggregate.householdId)) {
      context.addIssue({ code: "custom", message: "aggregate records must belong to its household" });
    }
    if (
      aggregate.policies.some((policy) => policy.version > aggregate.policyVersion) ||
      aggregate.policyCandidates.some((candidate) => candidate.basePolicyVersion > aggregate.policyVersion)
    ) {
      context.addIssue({ code: "custom", message: "aggregate policy versions are inconsistent" });
    }
    const isVerifiedAdult = (adultId: string) =>
      aggregate.verifiedAdultIds.some((candidate) => candidate === adultId);
    const referencesUnknownAdult =
      aggregate.episodes.some(
        (episode) => episode.owner.status !== "unassigned" && !isVerifiedAdult(episode.owner.adultId),
      ) ||
      aggregate.policies.some((policy) => !isVerifiedAdult(policy.approvedByAdultId)) ||
      aggregate.approvals.some((approval) => !isVerifiedAdult(approval.grantedByAdultId)) ||
      aggregate.memoryCandidates.some(
        (candidate) =>
          candidate.proposedBy.kind === "adult" && !isVerifiedAdult(candidate.proposedBy.adultId),
      ) ||
      aggregate.memories.some((memory) => !isVerifiedAdult(memory.confirmedByAdultId));
    if (referencesUnknownAdult) {
      context.addIssue({ code: "custom", message: "aggregate authority must reference verified adults" });
    }

    const evidenceIdsMatch = (left: EvidenceId[], right: EvidenceId[]) => {
      const leftValues = [...new Set(left)].sort();
      const rightValues = [...new Set(right)].sort();
      return (
        leftValues.length === rightValues.length &&
        leftValues.every((value, index) => value === rightValues[index])
      );
    };
    const sensitivityRank = { ordinary: 0, sensitive: 1, highly_sensitive: 2 } as const;
    const sourceMatchersEqual = (
      left: PrivateSourceMatcher | undefined,
      right: PrivateSourceMatcher | undefined,
    ) => {
      if (left === undefined || right === undefined || left.source !== right.source) return false;
      if (left.accountRefDigest !== right.accountRefDigest) return false;
      return left.source === "calendar"
        ? true
        : right.source === "gmail" && left.senderIdentityDigest === right.senderIdentityDigest;
    };
    const hasPromotionProof = (record: {
      evidence: EvidenceRef[];
      sourceClass: string;
      sensitivity: keyof typeof sensitivityRank;
      promotionAuthority?: PromotionAuthority | undefined;
      sourceMatcher?: PrivateSourceMatcher | undefined;
    }) => {
      const personalAdults = [
        ...new Set(
          record.evidence.flatMap((item) => (item.scope.kind === "personal" ? [item.scope.adultId] : [])),
        ),
      ];
      if (personalAdults.length === 0) {
        return true;
      }
      const authority = record.promotionAuthority;
      if (personalAdults.length !== 1 || authority === undefined) {
        return false;
      }
      if (authority.kind === "approval") {
        const approvalId = authority.approvalId;
        const approval = aggregate.approvals.find((candidate) => candidate.approvalId === approvalId);
        return (
          approval?.target.kind === "scope_promotion" &&
          approval.target.from.adultId === personalAdults[0] &&
          evidenceIdsMatch(
            approval.target.evidenceIds,
            record.evidence.map((item) => item.evidenceId),
          )
        );
      }
      const policyId = authority.policyId;
      const policy = aggregate.policies.find((candidate) => candidate.policyId === policyId);
      return (
        policy?.version === authority.policyVersion &&
        policy.rule.kind === "sharing" &&
        policy.rule.from.adultId === personalAdults[0] &&
        policy.rule.sourceClass === record.sourceClass &&
        sourceMatchersEqual(policy.rule.sourceMatcher, record.sourceMatcher) &&
        record.evidence.every(
          (item) => item.scope.kind !== "personal" || item.source === record.sourceMatcher?.source,
        ) &&
        sensitivityRank[record.sensitivity] <= sensitivityRank[policy.rule.maximumSensitivity]
      );
    };
    if (
      aggregate.episodes.some(
        (episode) => episode.scope.kind === "household" && !hasPromotionProof(episode),
      ) ||
      aggregate.memories.some((memory) => memory.scope.kind === "household" && !hasPromotionProof(memory))
    ) {
      context.addIssue({ code: "custom", message: "promoted records must retain applicable proof" });
    }
  });

export type HouseholdAggregate = z.infer<typeof HouseholdAggregateSchema>;

export const AdultActorSchema = z.strictObject({
  kind: z.literal("adult"),
  adultId: AdultIdSchema,
});

export const WorkerActorSchema = z.strictObject({
  kind: z.literal("worker"),
  jobId: WorkerJobIdSchema,
});

export const SourceActorSchema = z.strictObject({
  kind: z.literal("source_adapter"),
  source: z.enum(["linq", "gmail", "calendar", "effect_executor", "system_clock"]),
});

export const SignalActorSchema = z.discriminatedUnion("kind", [
  AdultActorSchema,
  WorkerActorSchema,
  SourceActorSchema,
]);

const SignalBaseShape = {
  householdId: HouseholdIdSchema,
  signalId: SignalIdSchema,
  sequence: z.number().int().positive(),
  occurredAt: InstantStringSchema,
  actor: SignalActorSchema,
} as const;

export const EpisodeProposedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("episode.proposed"),
  proposal: EpisodeProposalSchema,
});

export const CommitmentOwnerAcknowledgedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("commitment.owner_acknowledged"),
  episodeId: EpisodeIdSchema,
  baseEpisodeVersion: z.number().int().positive(),
});

export const CommitmentOwnerReassignedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("commitment.owner_reassigned"),
  episodeId: EpisodeIdSchema,
  baseEpisodeVersion: z.number().int().positive(),
  proposedOwnerAdultId: AdultIdSchema,
});

export const ConversationDeliveryObservedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("conversation.delivery_observed"),
  conversationId: ConversationIdSchema,
  episodeId: EpisodeIdSchema.optional(),
  deliveredAt: InstantStringSchema,
});

export const EpisodeClosedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("episode.closed"),
  episodeId: EpisodeIdSchema,
  baseEpisodeVersion: z.number().int().positive(),
  outcome: EpisodeOutcomeSchema,
});

export const EpisodeTemporalPlanReplacedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("episode.temporal_plan_replaced"),
  episodeId: EpisodeIdSchema,
  baseEpisodeVersion: z.number().int().positive(),
  plan: SemanticTimePlanSchema,
});

export const EpisodeSourceSupersededSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("episode.source_superseded"),
  episodeId: EpisodeIdSchema,
  baseEpisodeVersion: z.number().int().positive(),
  supersedingEvidence: EvidenceRefSchema,
});

export const RoutineAnchorsReplacedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("routine_anchors.replaced"),
  anchors: z.array(RoutineAnchorSchema).max(100),
});

export const EpisodeBlockedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("episode.blocked"),
  episodeId: EpisodeIdSchema,
  baseEpisodeVersion: z.number().int().positive(),
  reason: NeutralFactualTextSchema,
});

export const EpisodeResumedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("episode.resumed"),
  episodeId: EpisodeIdSchema,
  baseEpisodeVersion: z.number().int().positive(),
});

export const ExternalActionProposedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("external_action.proposed"),
  action: ExternalActionSchema,
});

export const ApprovalGrantedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("approval.granted"),
  approval: ApprovalRecordSchema,
});

export const ApprovalRevokedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("approval.revoked"),
  approvalId: ApprovalIdSchema,
});

export const PolicyApprovedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("policy.approved"),
  policy: PolicyRecordSchema,
  candidateId: PolicyCandidateIdSchema.optional(),
});

export const PolicyRevokedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("policy.revoked"),
  policyId: PolicyIdSchema,
  expectedPolicyVersion: z.number().int().nonnegative(),
});

export const MemoryConfirmedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("memory.confirmed"),
  memoryId: MemoryIdSchema,
  candidateId: MemoryCandidateIdSchema,
  targetScope: DurableScopeSchema,
  promotionAuthority: PromotionAuthoritySchema.optional(),
});

export const MemoryRevokedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("memory.revoked"),
  memoryId: MemoryIdSchema,
});

export const WorkerProposalReceivedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("worker.proposal_received"),
  proposal: WorkerProposalSchema,
});

export const TimerFiredSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("timer.fired"),
  timerId: TimerIdSchema,
  episodeId: EpisodeIdSchema,
  temporalPlanVersion: z.number().int().positive(),
  triggerId: TemporalTriggerIdSchema,
  firedAt: InstantStringSchema,
});

export const EffectReceiptReceivedSignalSchema = z.strictObject({
  ...SignalBaseShape,
  kind: z.literal("effect.receipt_received"),
  receiptId: EffectReceiptIdSchema,
  actionId: ExternalActionIdSchema,
  actionDigest: ActionDigestSchema,
  outcome: z.enum(["succeeded", "failed", "unknown"]),
  recordedAt: InstantStringSchema,
  providerReference: z.string().min(1).max(500).optional(),
});

export const HouseholdSignalSchema = z.discriminatedUnion("kind", [
  EpisodeProposedSignalSchema,
  CommitmentOwnerAcknowledgedSignalSchema,
  CommitmentOwnerReassignedSignalSchema,
  ConversationDeliveryObservedSignalSchema,
  EpisodeClosedSignalSchema,
  EpisodeTemporalPlanReplacedSignalSchema,
  EpisodeSourceSupersededSignalSchema,
  RoutineAnchorsReplacedSignalSchema,
  EpisodeBlockedSignalSchema,
  EpisodeResumedSignalSchema,
  ExternalActionProposedSignalSchema,
  ApprovalGrantedSignalSchema,
  ApprovalRevokedSignalSchema,
  PolicyApprovedSignalSchema,
  PolicyRevokedSignalSchema,
  MemoryConfirmedSignalSchema,
  MemoryRevokedSignalSchema,
  WorkerProposalReceivedSignalSchema,
  TimerFiredSignalSchema,
  EffectReceiptReceivedSignalSchema,
]);

export type HouseholdSignal = z.infer<typeof HouseholdSignalSchema>;

export const RejectionReasonSchema = z.enum([
  "household_mismatch",
  "duplicate_signal",
  "out_of_order_signal",
  "unauthorized_actor",
  "stale_household_version",
  "stale_policy_version",
  "stale_episode_version",
  "stale_temporal_plan",
  "episode_already_exists",
  "episode_not_found",
  "invalid_transition",
  "owner_mismatch",
  "privacy_promotion_requires_authority",
  "invalid_promotion_authority",
  "worker_cannot_promote",
  "approval_invalid",
  "approval_expired",
  "action_digest_mismatch",
  "policy_invalid",
  "memory_not_found",
  "candidate_not_found",
  "timer_not_due",
  "timer_no_longer_relevant",
  "effect_receipt_invalid",
  "duplicate_entity",
]);

export type RejectionReason = z.infer<typeof RejectionReasonSchema>;

export const DomainChangeSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("signal_rejected"), reason: RejectionReasonSchema }),
  z.strictObject({
    kind: z.literal("signal_ignored"),
    reason: z.enum(["delivery_is_not_approval", "timer_no_longer_relevant"]),
  }),
  z.strictObject({
    kind: z.literal("episode_created"),
    episodeId: EpisodeIdSchema,
    state: CommitmentStateSchema,
  }),
  z.strictObject({
    kind: z.literal("episode_state_changed"),
    episodeId: EpisodeIdSchema,
    from: CommitmentStateSchema,
    to: CommitmentStateSchema,
    episodeVersion: z.number().int().positive(),
  }),
  z.strictObject({
    kind: z.literal("temporal_plan_replaced"),
    episodeId: EpisodeIdSchema,
    fromVersion: z.number().int().positive().optional(),
    toVersion: z.number().int().positive(),
  }),
  z.strictObject({
    kind: z.literal("routine_anchors_replaced"),
    anchorIds: z.array(RoutineAnchorIdSchema).max(100),
  }),
  z.strictObject({
    kind: z.literal("approval_recorded"),
    approvalId: ApprovalIdSchema,
    status: z.enum(["active", "consumed", "revoked"]),
  }),
  z.strictObject({
    kind: z.literal("policy_changed"),
    policyId: PolicyIdSchema,
    status: z.enum(["active", "revoked"]),
    policyVersion: z.number().int().nonnegative(),
  }),
  z.strictObject({
    kind: z.literal("memory_candidate_recorded"),
    candidateId: MemoryCandidateIdSchema,
  }),
  z.strictObject({
    kind: z.literal("memory_confirmed"),
    memoryId: MemoryIdSchema,
  }),
  z.strictObject({
    kind: z.literal("memory_revoked"),
    memoryId: MemoryIdSchema,
  }),
  z.strictObject({
    kind: z.literal("policy_candidate_recorded"),
    candidateId: PolicyCandidateIdSchema,
  }),
  z.strictObject({
    kind: z.literal("action_state_changed"),
    actionId: ExternalActionIdSchema,
    state: PendingExternalActionSchema.shape.state,
  }),
  z.strictObject({
    kind: z.literal("reminder_decided"),
    episodeId: EpisodeIdSchema,
    triggerId: TemporalTriggerIdSchema,
    decision: z.enum(["remind", "missed_window", "skipped"]),
  }),
]);

export type DomainChange = z.infer<typeof DomainChangeSchema>;

export const AcceptanceReceiptSchema = z.strictObject({
  householdId: HouseholdIdSchema,
  signalId: SignalIdSchema,
  sequence: z.number().int().positive(),
  aggregateVersion: z.number().int().nonnegative(),
  disposition: z.enum(["accepted", "rejected", "ignored"]),
  reason: RejectionReasonSchema.optional(),
});

export const AcceptanceResultSchema = z.strictObject({
  receipt: AcceptanceReceiptSchema,
  aggregate: HouseholdAggregateSchema,
  changes: z.array(DomainChangeSchema),
  effects: z.array(OutboxIntentSchema),
});

export type AcceptanceReceipt = z.infer<typeof AcceptanceReceiptSchema>;
export type AcceptanceResult = z.infer<typeof AcceptanceResultSchema>;

export const HouseholdChiefOfStaffInputSchema = z.strictObject({
  current: HouseholdAggregateSchema,
  signal: HouseholdSignalSchema,
});

export type HouseholdChiefOfStaffInput = z.infer<typeof HouseholdChiefOfStaffInputSchema>;
