import { z } from "zod";
import {
  AcceptanceReceiptSchema,
  AdultIdSchema,
  ConfidenceSchema,
  OutboxIntentSchema as DomainOutboxIntentSchema,
  DurableScopeSchema,
  EpisodeIdSchema,
  EpisodeProposalSchema,
  EvidenceRefSchema,
  ExternalActionIdSchema,
  FactualTextSchema,
  HouseholdAggregateSchema,
  HouseholdIdSchema,
  InstantStringSchema,
  LocalTimeSchema,
  MemoryCandidateSchema,
  NeutralDisplayTextSchema,
  NeutralFactualTextSchema,
  PolicyCandidateSchema,
  PolicyIdSchema,
  ProposedExternalActionSchema,
  ProposedMessageSchema,
  RoutineAnchorIdSchema,
  SemanticTimePlanSchema,
  SensitivitySchema,
  SourceClassSchema,
  TimeZoneSchema,
} from "../domain/index.js";
import {
  WorkerJobSchema as RuntimeWorkerJobSchema,
  WorkerResultSchema as RuntimeWorkerResultSchema,
  WorkerContextItemSchema,
} from "../runtime/index.js";

const StableReferenceSchema = z.string().trim().min(1).max(500);
const IdempotencyKeySchema = z.string().trim().min(1).max(500);
const Sha256DigestSchema = z.string().regex(/^sha256:[a-f0-9]{64}$/);

const ConversationAttachmentBaseShape = {
  reference: StableReferenceSchema,
  mediaType: z.string().trim().min(1).max(255).nullable(),
  filename: z.string().trim().min(1).max(500).nullable(),
  sizeBytes: z
    .number()
    .int()
    .nonnegative()
    .max(100 * 1024 * 1024)
    .nullable(),
  contentDigest: Sha256DigestSchema,
} as const;

const ImageAttachmentContentSchema = z.strictObject({
  ...ConversationAttachmentBaseShape,
  kind: z.literal("image"),
  mediaType: z
    .string()
    .trim()
    .regex(/^image\//u)
    .max(255),
  sizeBytes: z
    .number()
    .int()
    .nonnegative()
    .max(10 * 1024 * 1024),
  dataBase64: z.string().min(1).max(14_000_000),
});

const FileAttachmentContentSchema = z.strictObject({
  ...ConversationAttachmentBaseShape,
  kind: z.literal("file"),
  mediaType: z.string().trim().min(1).max(255),
  sizeBytes: z
    .number()
    .int()
    .nonnegative()
    .max(10 * 1024 * 1024),
  dataBase64: z.string().min(1).max(14_000_000),
});

const UnavailableAttachmentContentSchema = z.strictObject({
  ...ConversationAttachmentBaseShape,
  kind: z.literal("unavailable"),
  reason: z.enum(["missing_reference", "too_large", "unsupported_type", "not_found", "invalid_content"]),
});

export const ConversationAttachmentContentSchema = z.discriminatedUnion("kind", [
  ImageAttachmentContentSchema,
  FileAttachmentContentSchema,
  z.strictObject({
    ...ConversationAttachmentBaseShape,
    kind: z.literal("link"),
    url: z.url().refine((value) => new URL(value).protocol === "https:", "Conversation links must use HTTPS"),
  }),
  UnavailableAttachmentContentSchema,
]);

export type ConversationAttachmentContent = z.infer<typeof ConversationAttachmentContentSchema>;

export const ConversationChannelSchema = z
  .strictObject({
    channelId: StableReferenceSchema,
    scope: z.enum(["personal", "household"]),
    adultId: AdultIdSchema.optional(),
  })
  .superRefine((channel, context) => {
    if (channel.scope === "personal" && channel.adultId === undefined) {
      context.addIssue({
        code: "custom",
        path: ["adultId"],
        message: "A personal conversation must identify its adult",
      });
    }
    if (channel.scope === "household" && channel.adultId !== undefined) {
      context.addIssue({
        code: "custom",
        path: ["adultId"],
        message: "A household conversation cannot identify one adult",
      });
    }
  });

export const ConversationInboxItemSchema = z
  .strictObject({
    kind: z.literal("conversation_message"),
    householdId: HouseholdIdSchema,
    idempotencyKey: IdempotencyKeySchema,
    occurredAt: InstantStringSchema,
    channel: ConversationChannelSchema,
    senderAdultId: AdultIdSchema,
    messageRef: StableReferenceSchema,
    text: z.string().max(20_000),
    attachmentRefs: z.array(StableReferenceSchema).max(20),
    attachmentContents: z.array(ConversationAttachmentContentSchema).max(20),
  })
  .superRefine((item, context) => {
    if (item.text.trim().length === 0 && item.attachmentRefs.length === 0) {
      context.addIssue({ code: "custom", message: "A conversation message needs content" });
    }
    if (item.channel.scope === "personal" && item.channel.adultId !== item.senderAdultId) {
      context.addIssue({
        code: "custom",
        path: ["senderAdultId"],
        message: "A personal message must be sent by that conversation's adult",
      });
    }
    const contentRefs = item.attachmentContents.map((attachment) => attachment.reference);
    if (
      contentRefs.length !== item.attachmentRefs.length ||
      contentRefs.some((reference, index) => reference !== item.attachmentRefs[index]) ||
      new Set(contentRefs).size !== contentRefs.length
    ) {
      context.addIssue({
        code: "custom",
        path: ["attachmentContents"],
        message: "Attachment content must account for each attachment reference exactly once and in order",
      });
    }
  });

export type ConversationInboxItem = z.infer<typeof ConversationInboxItemSchema>;

export const GmailAttachmentContentSchema = z.discriminatedUnion("kind", [
  ImageAttachmentContentSchema,
  FileAttachmentContentSchema,
  UnavailableAttachmentContentSchema,
]);

export type GmailAttachmentContent = z.infer<typeof GmailAttachmentContentSchema>;

export const GmailInboxItemSchema = z
  .strictObject({
    kind: z.literal("gmail_message"),
    householdId: HouseholdIdSchema,
    idempotencyKey: IdempotencyKeySchema,
    occurredAt: InstantStringSchema,
    ownerAdultId: AdultIdSchema,
    accountRef: StableReferenceSchema,
    messageRef: StableReferenceSchema,
    revision: z.number().int().positive(),
    labels: z.array(z.string().trim().min(1).max(100)).max(100),
    sender: z.string().trim().max(1_000).optional(),
    subject: z.string().max(2_000).optional(),
    snippet: z.string().max(10_000).optional(),
    bodyText: z.string().max(1_000_000).optional(),
    attachmentRefs: z.array(StableReferenceSchema).max(20),
    attachmentContents: z.array(GmailAttachmentContentSchema).max(20),
  })
  .superRefine((item, context) => {
    const contentRefs = item.attachmentContents.map((attachment) => attachment.reference);
    if (
      contentRefs.length !== item.attachmentRefs.length ||
      contentRefs.some((reference, index) => reference !== item.attachmentRefs[index]) ||
      new Set(contentRefs).size !== contentRefs.length
    ) {
      context.addIssue({
        code: "custom",
        path: ["attachmentContents"],
        message: "Attachment content must account for each attachment reference exactly once and in order",
      });
    }
  });

export type GmailInboxItem = z.infer<typeof GmailInboxItemSchema>;

const CalendarSourceIdentityShape = {
  householdId: HouseholdIdSchema,
  idempotencyKey: IdempotencyKeySchema,
  occurredAt: InstantStringSchema,
  ownerAdultId: AdultIdSchema,
  accountRef: StableReferenceSchema,
  eventRef: StableReferenceSchema,
  providerRef: StableReferenceSchema,
  revision: z.number().int().positive(),
} as const;

export const CalendarEventInboxItemSchema = z
  .strictObject({
    kind: z.literal("calendar_event"),
    ...CalendarSourceIdentityShape,
    contentDigest: Sha256DigestSchema,
    title: z.string().trim().min(1).max(2_000),
    description: z.string().max(20_000).nullable(),
    location: z.string().max(2_000).nullable(),
    startsAt: InstantStringSchema,
    endsAt: InstantStringSchema,
    timeZone: TimeZoneSchema,
    allDay: z.boolean(),
    status: z.enum(["confirmed", "tentative"]),
    recurrence: z.array(z.string().trim().min(1).max(2_000)).max(100),
  })
  .superRefine((item, context) => {
    if (Date.parse(item.startsAt) >= Date.parse(item.endsAt)) {
      context.addIssue({ code: "custom", path: ["endsAt"], message: "calendar end must follow start" });
    }
  });

export type CalendarEventInboxItem = z.infer<typeof CalendarEventInboxItemSchema>;

export const CalendarEventDeletedInboxItemSchema = z.strictObject({
  kind: z.literal("calendar_event_deleted"),
  ...CalendarSourceIdentityShape,
});

export type CalendarEventDeletedInboxItem = z.infer<typeof CalendarEventDeletedInboxItemSchema>;

export const ProviderInboxItemSchema = z.discriminatedUnion("kind", [
  ConversationInboxItemSchema,
  GmailInboxItemSchema,
  CalendarEventInboxItemSchema,
  CalendarEventDeletedInboxItemSchema,
]);

export type ProviderInboxItem = z.infer<typeof ProviderInboxItemSchema>;

export const HouseholdScopeAssessmentSchema = z.discriminatedUnion("decision", [
  z.strictObject({
    decision: z.literal("in_scope"),
    reason: NeutralFactualTextSchema,
  }),
  z.strictObject({
    decision: z.literal("narrow"),
    reason: NeutralFactualTextSchema,
    householdConsequence: NeutralFactualTextSchema,
  }),
  z.strictObject({
    decision: z.literal("out_of_scope"),
    reason: NeutralFactualTextSchema,
  }),
]);

export type HouseholdScopeAssessment = z.infer<typeof HouseholdScopeAssessmentSchema>;

const ClassificationBaseShape = {
  confidence: ConfidenceSchema,
  rationale: NeutralFactualTextSchema,
} as const;

export const SharedProfileCategorySchema = z.enum([
  "dependent",
  "school_childcare",
  "recurring_activity",
  "routine_anchor",
  "dietary_constraint",
]);

const NonRoutineSharedProfileCategorySchema = z.enum([
  "dependent",
  "school_childcare",
  "recurring_activity",
  "dietary_constraint",
]);

const CanonicalDaysOfWeekSchema = z
  .array(z.number().int().min(1).max(7))
  .min(1)
  .max(7)
  .superRefine((days, context) => {
    const canonical = [...new Set(days)].sort((left, right) => left - right);
    if (canonical.length !== days.length || canonical.some((day, index) => day !== days[index])) {
      context.addIssue({
        code: "custom",
        message: "daysOfWeek must contain unique ISO weekdays in ascending order",
      });
    }
  });

export const SharedProfileFactCandidateSchema = z.discriminatedUnion("category", [
  z.strictObject({
    category: NonRoutineSharedProfileCategorySchema,
    subject: NeutralDisplayTextSchema,
    detail: FactualTextSchema,
  }),
  z.strictObject({
    category: z.literal("routine_anchor"),
    anchorId: RoutineAnchorIdSchema.optional(),
    subject: NeutralDisplayTextSchema,
    detail: FactualTextSchema,
    timeZone: TimeZoneSchema,
    localTime: LocalTimeSchema,
    daysOfWeek: CanonicalDaysOfWeekSchema,
  }),
]);

export type SharedProfileFactCandidate = z.infer<typeof SharedProfileFactCandidateSchema>;

const OnboardingClassificationSchema = z
  .strictObject({
    ...ClassificationBaseShape,
    intent: z.literal("onboarding"),
    action: z.enum([
      "consent",
      "invite_adult",
      "accept_invite",
      "register_group",
      "update_profile",
      "confirm_profile",
    ]),
    invitedAdultId: AdultIdSchema.optional(),
    profileFacts: z.array(SharedProfileFactCandidateSchema).min(1).max(30).optional(),
  })
  .superRefine((classification, context) => {
    if ((classification.action === "invite_adult") !== (classification.invitedAdultId !== undefined)) {
      context.addIssue({
        code: "custom",
        path: ["invitedAdultId"],
        message: "Only an invite action identifies the invited adult",
      });
    }
    if ((classification.action === "update_profile") !== (classification.profileFacts !== undefined)) {
      context.addIssue({
        code: "custom",
        path: ["profileFacts"],
        message: "Only a profile update supplies shared profile facts",
      });
    }
  });

const ProposedCommitmentClassificationSchema = z.strictObject({
  ...ClassificationBaseShape,
  intent: z.literal("propose_commitment"),
  title: NeutralDisplayTextSchema,
  requiredOutcome: NeutralFactualTextSchema,
  proposedOwnerAdultId: AdultIdSchema.optional(),
  sourceClass: SourceClassSchema,
  sensitivity: SensitivitySchema,
  temporalPlan: SemanticTimePlanSchema.optional(),
});

const ResearchRequestClassificationSchema = z.strictObject({
  ...ClassificationBaseShape,
  intent: z.literal("research_request"),
  title: NeutralDisplayTextSchema,
  requiredOutcome: NeutralFactualTextSchema,
  constraints: z.array(FactualTextSchema).max(20),
  scopeAssessment: HouseholdScopeAssessmentSchema,
});

const MealPlanRequestClassificationSchema = z.strictObject({
  ...ClassificationBaseShape,
  intent: z.literal("meal_plan_request"),
  title: NeutralDisplayTextSchema,
  requiredOutcome: NeutralFactualTextSchema,
  horizon: FactualTextSchema,
  constraints: z.array(FactualTextSchema).max(20),
  scopeAssessment: HouseholdScopeAssessmentSchema,
});

const CalendarEventCreateRequestClassificationSchema = z
  .strictObject({
    ...ClassificationBaseShape,
    intent: z.literal("calendar_event_create_request"),
    title: NeutralDisplayTextSchema,
    startsAt: InstantStringSchema,
    endsAt: InstantStringSchema,
    timeZone: TimeZoneSchema,
    calendarAccountLabel: z.string().trim().min(1).max(200).optional(),
  })
  .superRefine((classification, context) => {
    if (Date.parse(classification.startsAt) >= Date.parse(classification.endsAt)) {
      context.addIssue({ code: "custom", path: ["endsAt"], message: "calendar end must follow start" });
    }
  });

const CalendarEventClarificationClassificationSchema = z
  .strictObject({
    ...ClassificationBaseShape,
    intent: z.literal("calendar_event_clarification"),
    missingFields: z
      .array(z.enum(["title", "start", "end", "timeZone"]))
      .min(1)
      .max(4),
  })
  .superRefine((classification, context) => {
    if (new Set(classification.missingFields).size !== classification.missingFields.length) {
      context.addIssue({ code: "custom", path: ["missingFields"], message: "missing fields must be unique" });
    }
  });

export const ConversationClassificationSchema = z.discriminatedUnion("intent", [
  z.strictObject({
    ...ClassificationBaseShape,
    intent: z.literal("ignore"),
  }),
  OnboardingClassificationSchema,
  ProposedCommitmentClassificationSchema,
  z.strictObject({
    ...ClassificationBaseShape,
    intent: z.literal("acknowledge_owner"),
    episodeId: EpisodeIdSchema,
    baseEpisodeVersion: z.number().int().positive(),
  }),
  z.strictObject({
    ...ClassificationBaseShape,
    intent: z.literal("reassign_owner"),
    episodeId: EpisodeIdSchema,
    baseEpisodeVersion: z.number().int().positive(),
    proposedOwnerAdultId: AdultIdSchema,
  }),
  z.strictObject({
    ...ClassificationBaseShape,
    intent: z.literal("close_episode"),
    episodeId: EpisodeIdSchema,
    baseEpisodeVersion: z.number().int().positive(),
    outcome: z.enum(["completed", "dismissed"]),
    summary: NeutralFactualTextSchema,
  }),
  ResearchRequestClassificationSchema,
  MealPlanRequestClassificationSchema,
  CalendarEventCreateRequestClassificationSchema,
  CalendarEventClarificationClassificationSchema,
  z.strictObject({
    ...ClassificationBaseShape,
    intent: z.literal("approve_calendar_event"),
    actionId: ExternalActionIdSchema,
  }),
  z.strictObject({
    ...ClassificationBaseShape,
    intent: z.literal("daily_brief_request"),
  }),
  z.strictObject({
    ...ClassificationBaseShape,
    intent: z.literal("approve_promotion"),
    promotionId: StableReferenceSchema,
    rememberForMatchingSource: z.boolean().optional(),
  }),
  z.strictObject({
    ...ClassificationBaseShape,
    intent: z.literal("decline_promotion"),
    promotionId: StableReferenceSchema,
  }),
  z.strictObject({
    ...ClassificationBaseShape,
    intent: z.literal("revoke_policy"),
    policyId: PolicyIdSchema,
    expectedPolicyVersion: z.number().int().nonnegative(),
  }),
]);

export type ConversationClassification = z.infer<typeof ConversationClassificationSchema>;

const GmailTriageBaseShape = {
  confidence: ConfidenceSchema,
  sourceClass: SourceClassSchema,
  sensitivity: SensitivitySchema,
  familyImpact: z.boolean(),
  rationale: NeutralFactualTextSchema,
} as const;

export const GmailTriageResultSchema = z.discriminatedUnion("decision", [
  z.strictObject({
    ...GmailTriageBaseShape,
    decision: z.literal("ignore"),
  }),
  z.strictObject({
    ...GmailTriageBaseShape,
    decision: z.literal("retain_private"),
    privateSummary: NeutralFactualTextSchema,
  }),
  z.strictObject({
    ...GmailTriageBaseShape,
    decision: z.literal("private_review"),
    privateSummary: NeutralFactualTextSchema,
  }),
  z.strictObject({
    ...GmailTriageBaseShape,
    decision: z.literal("private_interrupt"),
    privateSummary: NeutralFactualTextSchema,
    urgencyReason: NeutralFactualTextSchema,
  }),
  z.strictObject({
    ...GmailTriageBaseShape,
    decision: z.literal("propose_family_episode"),
    materialException: z.boolean(),
    privateSummary: NeutralFactualTextSchema,
    minimumHouseholdMeaning: NeutralDisplayTextSchema,
    title: NeutralDisplayTextSchema,
    requiredOutcome: NeutralFactualTextSchema,
    proposedOwnerAdultId: AdultIdSchema.optional(),
    temporalPlan: SemanticTimePlanSchema.optional(),
  }),
]);

export type GmailTriageResult = z.infer<typeof GmailTriageResultSchema>;

const CalendarTriageBaseShape = {
  confidence: ConfidenceSchema,
  sourceClass: SourceClassSchema,
  sensitivity: SensitivitySchema,
  familyImpact: z.boolean(),
  rationale: NeutralFactualTextSchema,
} as const;

export const CalendarTriageResultSchema = z.discriminatedUnion("decision", [
  z.strictObject({
    ...CalendarTriageBaseShape,
    decision: z.literal("ignore"),
  }),
  z.strictObject({
    ...CalendarTriageBaseShape,
    decision: z.literal("retain_private"),
    privateSummary: NeutralFactualTextSchema,
  }),
  z.strictObject({
    ...CalendarTriageBaseShape,
    decision: z.literal("private_review"),
    privateSummary: NeutralFactualTextSchema,
  }),
  z.strictObject({
    ...CalendarTriageBaseShape,
    decision: z.literal("private_interrupt"),
    privateSummary: NeutralFactualTextSchema,
    urgencyReason: NeutralFactualTextSchema,
  }),
  z.strictObject({
    ...CalendarTriageBaseShape,
    decision: z.literal("propose_family_episode"),
    materialException: z.boolean(),
    privateSummary: NeutralFactualTextSchema,
    minimumHouseholdMeaning: NeutralDisplayTextSchema,
    minimumRequiredOutcome: NeutralFactualTextSchema,
  }),
]);

export type CalendarTriageResult = z.infer<typeof CalendarTriageResultSchema>;

export const OnboardingProjectionSchema = z
  .strictObject({
    phase: z.enum([
      "awaiting_initiator_consent",
      "awaiting_invitation",
      "awaiting_invitee_consent",
      "awaiting_group",
      "building_profile",
      "active",
    ]),
    initiatorAdultId: AdultIdSchema,
    invitedAdultId: AdultIdSchema.optional(),
    consentedAdultIds: z.array(AdultIdSchema).max(2),
    privateDmAdultIds: z.array(AdultIdSchema).max(2),
    groupChannelId: StableReferenceSchema.optional(),
    profileConfirmedAdultIds: z.array(AdultIdSchema).max(2),
  })
  .superRefine((projection, context) => {
    const unique = (values: readonly string[]) => new Set(values).size === values.length;
    if (
      !unique(projection.consentedAdultIds) ||
      !unique(projection.privateDmAdultIds) ||
      !unique(projection.profileConfirmedAdultIds)
    ) {
      context.addIssue({ code: "custom", message: "Onboarding adult lists must be unique" });
    }
    if (
      ["awaiting_invitee_consent", "awaiting_group", "building_profile", "active"].includes(
        projection.phase,
      ) &&
      projection.invitedAdultId === undefined
    ) {
      context.addIssue({ code: "custom", message: "This onboarding phase requires an invitee" });
    }
    if (
      ["building_profile", "active"].includes(projection.phase) &&
      projection.groupChannelId === undefined
    ) {
      context.addIssue({ code: "custom", message: "This onboarding phase requires a group" });
    }
  });

export type OnboardingProjection = z.infer<typeof OnboardingProjectionSchema>;

const SharedProfileFactBaseShape = {
  factKey: z.string().regex(/^profile:[a-f0-9]{32}$/u),
  subject: NeutralDisplayTextSchema,
  detail: FactualTextSchema,
  sourceRef: StableReferenceSchema,
  recordedByAdultId: AdultIdSchema,
  recordedAt: InstantStringSchema,
} as const;

export const SharedProfileFactSchema = z.discriminatedUnion("category", [
  z.strictObject({
    ...SharedProfileFactBaseShape,
    category: NonRoutineSharedProfileCategorySchema,
  }),
  z.strictObject({
    ...SharedProfileFactBaseShape,
    category: z.literal("routine_anchor"),
    anchorId: RoutineAnchorIdSchema,
    timeZone: TimeZoneSchema,
    localTime: LocalTimeSchema,
    daysOfWeek: CanonicalDaysOfWeekSchema,
  }),
]);

export type SharedProfileFact = z.infer<typeof SharedProfileFactSchema>;

export const SharedHouseholdProfileSchema = z.strictObject({
  facts: z.array(SharedProfileFactSchema).max(200),
});

export type SharedHouseholdProfile = z.infer<typeof SharedHouseholdProfileSchema>;

export const GmailTriageRecordSchema = z.strictObject({
  messageRef: StableReferenceSchema,
  ownerAdultId: AdultIdSchema,
  decision: z.enum([
    "ignore",
    "retain_private",
    "private_review",
    "private_interrupt",
    "propose_family_episode",
  ]),
  sourceClass: SourceClassSchema,
  sensitivity: SensitivitySchema,
  familyImpact: z.boolean(),
  confidence: ConfidenceSchema,
  recordedAt: InstantStringSchema,
});

export const CalendarTriageRecordSchema = z.strictObject({
  sourceKey: StableReferenceSchema,
  ownerAdultId: AdultIdSchema,
  revision: z.number().int().positive(),
  decision: z.enum([
    "ignore",
    "retain_private",
    "private_review",
    "private_interrupt",
    "propose_family_episode",
  ]),
  sourceClass: SourceClassSchema,
  sensitivity: SensitivitySchema,
  familyImpact: z.boolean(),
  confidence: ConfidenceSchema,
  recordedAt: InstantStringSchema,
});

export const CalendarSourceRecordSchema = z
  .strictObject({
    sourceKey: StableReferenceSchema,
    ownerAdultId: AdultIdSchema,
    latestRevision: z.number().int().positive(),
    status: z.enum(["active", "deleted"]),
    contentDigest: Sha256DigestSchema.optional(),
    pendingPromotionId: StableReferenceSchema.optional(),
    episodeId: EpisodeIdSchema.optional(),
    recordedAt: InstantStringSchema,
  })
  .superRefine((record, context) => {
    if ((record.status === "active") !== (record.contentDigest !== undefined)) {
      context.addIssue({
        code: "custom",
        path: ["contentDigest"],
        message: "Only an active Calendar source retains its content digest",
      });
    }
    if (
      record.status === "deleted" &&
      (record.pendingPromotionId !== undefined || record.episodeId !== undefined)
    ) {
      context.addIssue({
        code: "custom",
        message: "A deleted Calendar source cannot retain pending or active application work",
      });
    }
    if (record.pendingPromotionId !== undefined && record.episodeId !== undefined) {
      context.addIssue({
        code: "custom",
        message: "A Calendar source cannot be both pending promotion and promoted",
      });
    }
  });

export type CalendarSourceRecord = z.infer<typeof CalendarSourceRecordSchema>;

export const PendingPromotionSchema = z.strictObject({
  promotionId: StableReferenceSchema,
  ownerAdultId: AdultIdSchema,
  evidence: EvidenceRefSchema,
  proposal: EpisodeProposalSchema,
  minimumHouseholdMeaning: NeutralDisplayTextSchema,
  standingRuleEligible: z.boolean(),
  createdAt: InstantStringSchema,
});

export type PendingPromotion = z.infer<typeof PendingPromotionSchema>;

export const WorkerPurposeSchema = z.enum(["family_research", "meal_plan"]);
export type WorkerPurpose = z.infer<typeof WorkerPurposeSchema>;

export const WorkerRecordSchema = z.strictObject({
  purpose: WorkerPurposeSchema,
  episodeId: EpisodeIdSchema,
  job: RuntimeWorkerJobSchema,
  status: z.enum(["queued", "reconciled", "rejected", "failed"]),
  createdAt: InstantStringSchema,
  resultRef: StableReferenceSchema.optional(),
});

export type WorkerRecord = z.infer<typeof WorkerRecordSchema>;

export const ApplicationProjectionSchema = z
  .strictObject({
    onboarding: OnboardingProjectionSchema,
    sharedProfile: SharedHouseholdProfileSchema,
    gmailTriage: z.array(GmailTriageRecordSchema).max(100_000),
    calendarTriage: z.array(CalendarTriageRecordSchema).max(100_000),
    calendarSources: z.array(CalendarSourceRecordSchema).max(100_000),
    pendingPromotions: z.array(PendingPromotionSchema).max(10_000),
    workers: z.array(WorkerRecordSchema).max(10_000),
  })
  .superRefine((projection, context) => {
    const sourceKeys = projection.calendarSources.map((record) => record.sourceKey);
    if (new Set(sourceKeys).size !== sourceKeys.length) {
      context.addIssue({
        code: "custom",
        path: ["calendarSources"],
        message: "Calendar source records must have unique source keys",
      });
    }
  });

export type ApplicationProjection = z.infer<typeof ApplicationProjectionSchema>;

export const HouseholdApplicationSnapshotSchema = z.strictObject({
  revision: z.number().int().nonnegative(),
  aggregate: HouseholdAggregateSchema,
  projection: ApplicationProjectionSchema,
});

export type HouseholdApplicationSnapshot = z.infer<typeof HouseholdApplicationSnapshotSchema>;

const ApplicationOutboxBaseShape = {
  intentId: StableReferenceSchema,
  householdId: HouseholdIdSchema,
  idempotencyKey: IdempotencyKeySchema,
} as const;

export const ConversationSendIntentSchema = z.strictObject({
  ...ApplicationOutboxBaseShape,
  kind: z.literal("conversation.send"),
  targetScope: DurableScopeSchema,
  messageClass: z.enum([
    "onboarding",
    "private_review",
    "private_interrupt",
    "promotion_request",
    "clarifying_question",
    "status",
    "daily_brief",
  ]),
  body: z.string().trim().min(1).max(4_000),
});

export const WorkerRunIntentSchema = z.strictObject({
  ...ApplicationOutboxBaseShape,
  kind: z.literal("worker.run"),
  job: RuntimeWorkerJobSchema,
});

export const DomainEffectIntentSchema = z.strictObject({
  ...ApplicationOutboxBaseShape,
  kind: z.literal("domain.effect"),
  effect: DomainOutboxIntentSchema,
});

export const ApplicationOutboxIntentSchema = z.discriminatedUnion("kind", [
  ConversationSendIntentSchema,
  WorkerRunIntentSchema,
  DomainEffectIntentSchema,
]);

export type ApplicationOutboxIntent = z.infer<typeof ApplicationOutboxIntentSchema>;

export const WorkerCommandSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("episode.propose"), payload: EpisodeProposalSchema }),
  z.strictObject({ kind: z.literal("message.propose"), payload: ProposedMessageSchema }),
  z.strictObject({ kind: z.literal("action.propose"), payload: ProposedExternalActionSchema }),
  z.strictObject({
    kind: z.literal("memory.candidate"),
    payload: MemoryCandidateSchema,
  }),
  z.strictObject({
    kind: z.literal("policy.candidate"),
    payload: PolicyCandidateSchema,
  }),
]);

export type WorkerCommand = z.infer<typeof WorkerCommandSchema>;

export const ApplicationAuditEntrySchema = z.strictObject({
  kind: z.enum([
    "conversation_classified",
    "gmail_triaged",
    "calendar_triaged",
    "calendar_reconciled",
    "onboarding_transition",
    "domain_accepted",
    "worker_reconciled",
    "daily_brief_built",
    "external_action_reconciled",
  ]),
  occurredAt: InstantStringSchema,
  decision: z.string().trim().min(1).max(100),
  sourceRef: StableReferenceSchema.optional(),
  adultId: AdultIdSchema.optional(),
  containsPrivateData: z.boolean(),
});

export type ApplicationAuditEntry = z.infer<typeof ApplicationAuditEntrySchema>;

export const TimerFiredInputSchema = z.strictObject({
  kind: z.literal("timer_fired"),
  householdId: HouseholdIdSchema,
  idempotencyKey: IdempotencyKeySchema,
  timerId: StableReferenceSchema,
  episodeId: EpisodeIdSchema,
  temporalPlanVersion: z.number().int().positive(),
  triggerId: StableReferenceSchema,
  firedAt: InstantStringSchema,
});

export const WorkerResultInputSchema = z.strictObject({
  kind: z.literal("worker_result"),
  householdId: HouseholdIdSchema,
  idempotencyKey: IdempotencyKeySchema,
  receivedAt: InstantStringSchema,
  result: RuntimeWorkerResultSchema,
});

export const WorkerRunInputSchema = z.strictObject({
  kind: z.literal("run_worker"),
  householdId: HouseholdIdSchema,
  idempotencyKey: IdempotencyKeySchema,
  jobId: StableReferenceSchema,
  requestedAt: InstantStringSchema,
});

export const DailyBriefInputSchema = z
  .strictObject({
    kind: z.literal("daily_brief"),
    householdId: HouseholdIdSchema,
    idempotencyKey: IdempotencyKeySchema,
    occurredAt: InstantStringSchema,
    reason: z.enum(["scheduled", "adult_request"]),
    requestedByAdultId: AdultIdSchema.optional(),
  })
  .superRefine((input, context) => {
    if ((input.reason === "adult_request") !== (input.requestedByAdultId !== undefined)) {
      context.addIssue({
        code: "custom",
        path: ["requestedByAdultId"],
        message: "Only an adult-requested brief identifies an adult",
      });
    }
  });

export const EffectReceiptInputSchema = z.strictObject({
  kind: z.literal("effect_receipt"),
  householdId: HouseholdIdSchema,
  idempotencyKey: IdempotencyKeySchema,
  receiptId: StableReferenceSchema,
  actionId: StableReferenceSchema,
  actionDigest: z.string().regex(/^sha256:[a-f0-9]{64}$/),
  outcome: z.enum(["succeeded", "failed", "unknown"]),
  recordedAt: InstantStringSchema,
  providerReference: StableReferenceSchema.optional(),
});

export const ApplicationInputSchema = z.discriminatedUnion("kind", [
  ConversationInboxItemSchema,
  GmailInboxItemSchema,
  CalendarEventInboxItemSchema,
  CalendarEventDeletedInboxItemSchema,
  TimerFiredInputSchema,
  WorkerResultInputSchema,
  WorkerRunInputSchema,
  DailyBriefInputSchema,
  EffectReceiptInputSchema,
]);

export type ApplicationInput = z.infer<typeof ApplicationInputSchema>;

export const ApplicationOutcomeSchema = z.strictObject({
  status: z.enum(["processed", "rejected"]),
  classification: z.string().trim().min(1).max(100),
  domainReceipts: z.array(AcceptanceReceiptSchema).max(20),
  outboxIntentIds: z.array(StableReferenceSchema).max(100),
});

export type ApplicationOutcome = z.infer<typeof ApplicationOutcomeSchema>;

export const ApplicationResultSchema = z.strictObject({
  householdId: HouseholdIdSchema,
  idempotencyKey: IdempotencyKeySchema,
  disposition: z.enum(["committed", "duplicate"]),
  revision: z.number().int().nonnegative(),
  outcome: ApplicationOutcomeSchema,
});

export type ApplicationResult = z.infer<typeof ApplicationResultSchema>;

export const WorkerRouteSchema = z.strictObject({
  modelRouteId: StableReferenceSchema,
  outputContractRef: StableReferenceSchema,
  capabilityIds: z.array(StableReferenceSchema).max(100),
  allowedToolNames: z.array(z.string().regex(/^[A-Za-z0-9_-]{1,64}$/)).max(100),
  maxDurationMs: z.number().int().positive().max(86_400_000),
  maxModelCalls: z.number().int().positive().max(1_000),
  maxToolCalls: z.number().int().nonnegative().max(10_000),
});

export type WorkerRoute = z.infer<typeof WorkerRouteSchema>;

export const WorkerRoutesSchema = z.strictObject({
  family_research: WorkerRouteSchema.extend({
    modelCapabilityProfile: z.literal("long_context_research"),
  }).strict(),
  meal_plan: WorkerRouteSchema.extend({
    modelCapabilityProfile: z.literal("tool_planning"),
  }).strict(),
});

export type WorkerRoutes = z.infer<typeof WorkerRoutesSchema>;

export const EffectExecutionReceiptSchema = z.strictObject({
  status: z.enum(["succeeded", "retryable_failure", "permanent_failure"]),
  receiptRef: StableReferenceSchema.optional(),
  recordedAt: InstantStringSchema,
  externalAction: z
    .strictObject({
      receiptId: StableReferenceSchema,
      actionId: StableReferenceSchema,
      actionDigest: z.string().regex(/^sha256:[a-f0-9]{64}$/),
      outcome: z.enum(["succeeded", "failed", "unknown"]),
      providerReference: StableReferenceSchema.optional(),
    })
    .optional(),
});

export type EffectExecutionReceipt = z.infer<typeof EffectExecutionReceiptSchema>;

export const OutboxExecutionResultSchema = z.strictObject({
  intentId: StableReferenceSchema,
  status: z.enum(["succeeded", "retryable_failure", "permanent_failure"]),
  applicationResult: ApplicationResultSchema.optional(),
});

export type OutboxExecutionResult = z.infer<typeof OutboxExecutionResultSchema>;

export { WorkerContextItemSchema };
