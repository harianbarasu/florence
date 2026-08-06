import { z } from "zod";

export const EntityIdSchema = z.string().uuid();
export const DigestSchema = z.string().regex(/^[a-f0-9]{64}$/u);
export const InstantSchema = z.iso.datetime({ offset: true });

export const ConversationModeSchema = z.enum([
  "content_disabled",
  "read_enabled_write_disabled",
  "trusted_write_enabled",
  "paused",
]);
export type ConversationMode = z.infer<typeof ConversationModeSchema>;

export const ParticipantPolicyValueSchema = z.strictObject({
  allowContentProcessing: z.boolean(),
  allowDirectResponses: z.boolean(),
  allowProactiveWrites: z.boolean(),
  retentionSeconds: z.number().int().min(0).max(2_592_000),
});
export type ParticipantPolicyValue = z.infer<typeof ParticipantPolicyValueSchema>;

export const ParticipantAuthoritySchema = z.strictObject({
  personIdentityId: EntityIdSchema,
  personId: EntityIdSchema,
  registrationStatus: z.enum(["provisional", "registered"]),
  consentedAt: InstantSchema.nullable(),
  proactivePaused: z.boolean(),
  policy: ParticipantPolicyValueSchema.nullable(),
});
export type ParticipantAuthority = z.infer<typeof ParticipantAuthoritySchema>;

export const ConversationRuleSchema = z.strictObject({
  ruleId: EntityIdSchema,
  ruleKey: z.string().trim().min(1).max(100),
  participantSetDigest: DigestSchema,
  allowedOperations: z.array(z.string().trim().min(1).max(100)),
  active: z.boolean(),
});
export type ConversationRule = z.infer<typeof ConversationRuleSchema>;

export const ConversationAuthoritySnapshotSchema = z.strictObject({
  conversationId: EntityIdSchema,
  conversationStatus: z.enum(["active", "paused", "deletion_fenced", "deleted"]),
  authorityVersion: z.number().int().positive(),
  participantEpochId: EntityIdSchema.nullable(),
  participantSetDigest: DigestSchema.nullable(),
  participants: z.array(ParticipantAuthoritySchema),
  activeSuppressions: z.array(
    z.strictObject({
      id: EntityIdSchema,
      kind: z.enum(["stop", "pause", "read_only", "retention_cap", "deletion_fence", "safety_hold"]),
      retentionSeconds: z.number().int().min(0).max(2_592_000).nullable(),
    }),
  ),
  rules: z.array(ConversationRuleSchema),
});
export type ConversationAuthoritySnapshot = z.infer<typeof ConversationAuthoritySnapshotSchema>;

export const ParticipantEpochSchema = z.strictObject({
  participantEpochId: EntityIdSchema,
  conversationId: EntityIdSchema,
  sequence: z.number().int().positive(),
  participantSetDigest: DigestSchema,
  authorityDigest: DigestSchema,
  startedAt: InstantSchema,
  endedAt: InstantSchema.nullable(),
  participants: z.array(
    z.strictObject({
      personIdentityId: EntityIdSchema,
      personId: EntityIdSchema,
      registrationStatus: z.enum(["provisional", "registered"]),
      consentedAt: InstantSchema.nullable(),
    }),
  ),
});
export type ParticipantEpoch = z.infer<typeof ParticipantEpochSchema>;

export const CreateConversationInputSchema = z.strictObject({
  householdId: EntityIdSchema.nullable(),
  kind: z.enum(["direct", "group"]),
  purpose: z.string().trim().min(1).max(200),
  createdAt: InstantSchema,
});
export type CreateConversationInput = z.infer<typeof CreateConversationInputSchema>;

export const BindConversationChannelInputSchema = z.strictObject({
  conversationId: EntityIdSchema,
  provider: z.string().trim().min(1).max(100),
  externalChannelId: z.string().trim().min(1).max(500),
  boundAt: InstantSchema,
});
export type BindConversationChannelInput = z.infer<typeof BindConversationChannelInputSchema>;

export const ConversationChannelLookupSchema = z.strictObject({
  provider: z.string().trim().min(1).max(100),
  externalChannelId: z.string().trim().min(1).max(500),
});
export type ConversationChannelLookup = z.infer<typeof ConversationChannelLookupSchema>;

export const ConversationChannelBindingSchema = z.strictObject({
  channelId: EntityIdSchema,
  conversationId: EntityIdSchema,
  provider: z.string(),
  externalChannelId: z.string(),
  status: z.enum(["active", "paused", "revoked"]),
});
export type ConversationChannelBinding = z.infer<typeof ConversationChannelBindingSchema>;

export const RecordParticipantEpochInputSchema = z
  .strictObject({
    conversationId: EntityIdSchema,
    participantIdentityIds: z.array(EntityIdSchema).min(1).max(100),
    changeReason: z.string().trim().min(1).max(200),
    observedAt: InstantSchema,
  })
  .refine((input) => new Set(input.participantIdentityIds).size === input.participantIdentityIds.length, {
    message: "Participant identities must be unique",
    path: ["participantIdentityIds"],
  });
export type RecordParticipantEpochInput = z.infer<typeof RecordParticipantEpochInputSchema>;

export const ConsentToEpochInputSchema = z.strictObject({
  participantEpochId: EntityIdSchema,
  personId: EntityIdSchema,
  policy: ParticipantPolicyValueSchema.extend({ allowProactiveWrites: z.literal(false) }),
  consentedAt: InstantSchema,
});
export type ConsentToEpochInput = z.infer<typeof ConsentToEpochInputSchema>;

export const SetParticipantPolicyInputSchema = z.strictObject({
  conversationId: EntityIdSchema,
  actorPersonId: EntityIdSchema,
  targetPersonId: EntityIdSchema,
  policy: ParticipantPolicyValueSchema,
  expectedAuthorityVersion: z.number().int().positive(),
  approvedByPersonIds: z.array(EntityIdSchema).max(100).default([]),
  changedAt: InstantSchema,
});
export type SetParticipantPolicyInput = z.infer<typeof SetParticipantPolicyInputSchema>;

export const ApplyNarrowingInputSchema = z.strictObject({
  conversationId: EntityIdSchema,
  actorPersonId: EntityIdSchema,
  kind: z.enum(["stop", "pause", "read_only", "retention_cap"]),
  retentionSeconds: z.number().int().min(0).max(2_592_000).nullable().default(null),
  reason: z.string().trim().min(1).max(200),
  appliedAt: InstantSchema,
});
export type ApplyNarrowingInput = z.infer<typeof ApplyNarrowingInputSchema>;

export const LiftNarrowingInputSchema = z.strictObject({
  suppressionId: EntityIdSchema,
  actorPersonId: EntityIdSchema,
  approvedByPersonIds: z.array(EntityIdSchema).min(1).max(100),
  expectedAuthorityVersion: z.number().int().positive(),
  liftedAt: InstantSchema,
});
export type LiftNarrowingInput = z.infer<typeof LiftNarrowingInputSchema>;

export const ActivateConversationRuleInputSchema = z.strictObject({
  conversationId: EntityIdSchema,
  actorPersonId: EntityIdSchema,
  ruleKey: z.string().trim().min(1).max(100),
  purpose: z.string().trim().min(1).max(300),
  allowedOperations: z.array(z.string().trim().min(1).max(100)).min(1).max(50),
  approvedByPersonIds: z.array(EntityIdSchema).min(1).max(100),
  expectedAuthorityVersion: z.number().int().positive(),
  activatedAt: InstantSchema,
});
export type ActivateConversationRuleInput = z.infer<typeof ActivateConversationRuleInputSchema>;

export const AuthorizeSendInputSchema = z
  .strictObject({
    conversationId: EntityIdSchema,
    expectedParticipantEpochId: EntityIdSchema,
    expectedParticipantSetDigest: DigestSchema,
    liveParticipantIdentityIds: z.array(EntityIdSchema).min(1).max(100),
    sendKind: z.enum(["direct_response", "proactive"]),
    operation: z.string().trim().min(1).max(100),
    ruleId: EntityIdSchema.nullable().default(null),
  })
  .refine(
    (input) => new Set(input.liveParticipantIdentityIds).size === input.liveParticipantIdentityIds.length,
    { message: "Live participant identities must be unique", path: ["liveParticipantIdentityIds"] },
  );
export type AuthorizeSendInput = z.infer<typeof AuthorizeSendInputSchema>;

export const SendAuthorizationSchema = z.strictObject({
  allowed: z.boolean(),
  reason: z.enum([
    "allowed",
    "conversation_paused",
    "content_disabled",
    "epoch_mismatch",
    "live_participant_mismatch",
    "participant_policy_denied",
    "participant_proactive_paused",
    "proactive_rule_missing",
  ]),
  mode: ConversationModeSchema,
  conversationId: EntityIdSchema,
  participantEpochId: EntityIdSchema.nullable(),
  participantSetDigest: DigestSchema.nullable(),
  authorityVersion: z.number().int().positive(),
  effectiveRetentionSeconds: z.number().int().min(0).max(2_592_000).nullable(),
});
export type SendAuthorization = z.infer<typeof SendAuthorizationSchema>;

export interface ConversationAuthority {
  createConversation(input: CreateConversationInput): Promise<{ conversationId: string }>;
  bindChannel(input: BindConversationChannelInput): Promise<{ channelId: string }>;
  findByChannel(input: ConversationChannelLookup): Promise<ConversationChannelBinding | null>;
  snapshot(conversationId: string): Promise<ConversationAuthoritySnapshot>;
  recordParticipantEpoch(input: RecordParticipantEpochInput): Promise<ParticipantEpoch>;
  consentToEpoch(input: ConsentToEpochInput): Promise<ConversationAuthoritySnapshot>;
  setParticipantPolicy(input: SetParticipantPolicyInput): Promise<ConversationAuthoritySnapshot>;
  applyNarrowing(input: ApplyNarrowingInput): Promise<ConversationAuthoritySnapshot>;
  liftNarrowing(input: LiftNarrowingInput): Promise<ConversationAuthoritySnapshot>;
  activateRule(input: ActivateConversationRuleInput): Promise<ConversationAuthoritySnapshot>;
  authorizeSend(input: AuthorizeSendInput): Promise<SendAuthorization>;
}
