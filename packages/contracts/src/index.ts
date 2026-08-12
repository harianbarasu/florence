import { z } from "zod";

export const idSchema = z.uuid();
export const timestampSchema = z.iso.datetime();

const signalEnvelopeSchema = z.object({
  signalId: idSchema,
  householdId: idSchema,
  occurredAt: timestampSchema,
  idempotencyKey: z.string().trim().min(8).max(200),
});
const sourceSignalIdsSchema = z.array(idSchema).min(1).max(50);

export const conversationAudienceSchema = z.enum(["private", "group"]);
export type ConversationAudience = z.infer<typeof conversationAudienceSchema>;

const conversationAuthorityFields = {
  authorityVersion: z.number().int().positive(),
  participantSetDigest: z.string().regex(/^[a-f0-9]{64}$/),
};

export const verifiedAdultSchema = z
  .object({
    id: idSchema,
    displayName: z.string().trim().min(1).max(160),
  })
  .strict();
export type VerifiedAdult = z.infer<typeof verifiedAdultSchema>;

export const plannedAdultSchema = z
  .object({
    id: idSchema,
    displayName: z.string().trim().min(1).max(160),
    role: z.enum(["steward", "caregiver"]),
    relationship: z.string().trim().min(1).max(160),
  })
  .strict();
export type PlannedAdult = z.infer<typeof plannedAdultSchema>;

const familyMemberFields = {
  id: idSchema,
  kind: z.enum(["adult", "child"]),
  role: z.enum(["steward", "caregiver", "dependent"]),
  displayName: z.string().trim().min(1).max(160),
  relationship: z.string().trim().min(1).max(160),
  aliases: z.array(z.string().trim().min(1).max(160)).max(20).optional(),
  birthYear: z.number().int().min(1800).max(3000).optional(),
  school: z.string().trim().min(1).max(300).optional(),
  currentGrade: z.string().trim().min(1).max(100).optional(),
  academicYear: z.string().trim().min(1).max(100).optional(),
  gradeEffectiveFrom: z.iso.date().optional(),
  activities: z.array(z.string().trim().min(1).max(300)).max(50).optional(),
};

function requireCompleteGrade(
  member: {
    currentGrade?: string | undefined;
    academicYear?: string | undefined;
    gradeEffectiveFrom?: string | undefined;
  },
  context: z.core.$RefinementCtx,
): void {
  const gradeFields = [member.currentGrade, member.academicYear, member.gradeEffectiveFrom];
  if (gradeFields.some(Boolean) && !gradeFields.every(Boolean)) {
    context.addIssue({
      code: "custom",
      path: ["currentGrade"],
      message: "Grade, academic year, and effective date must be provided together.",
    });
  }
}

const familyMemberInputSchema = z.object(familyMemberFields).strict().superRefine(requireCompleteGrade);

export const familyMemberStatusSchema = z.enum(["verified", "planned", "represented"]);
export type FamilyMemberStatus = z.infer<typeof familyMemberStatusSchema>;

export const familyMemberProfileSchema = z
  .object({
    ...familyMemberFields,
    status: familyMemberStatusSchema,
  })
  .strict()
  .superRefine(requireCompleteGrade);
export type FamilyMemberProfile = z.infer<typeof familyMemberProfileSchema>;

export const familyMemberSnapshotSchema = z
  .object({
    ...familyMemberFields,
    status: familyMemberStatusSchema,
    sourceSignalIds: sourceSignalIdsSchema,
  })
  .strict()
  .superRefine(requireCompleteGrade);
export type FamilyMemberSnapshot = z.infer<typeof familyMemberSnapshotSchema>;

export const imageReferenceSchema = z
  .object({
    assetId: idSchema,
    mimeType: z.enum(["image/jpeg", "image/png", "image/webp", "image/heic"]),
  })
  .strict();
export type ImageReference = z.infer<typeof imageReferenceSchema>;

export const conversationMessageSourceSchema = z
  .object({
    system: z.literal("linq-v3"),
    providerEventId: z.string().trim().min(1).max(500),
    providerMessageId: z.string().trim().min(1).max(500),
  })
  .strict();
export type ConversationMessageSource = z.infer<typeof conversationMessageSourceSchema>;

export const conversationMessageSignalSchema = signalEnvelopeSchema
  .extend({
    type: z.literal("conversation.message"),
    conversationId: idSchema,
    audience: conversationAudienceSchema,
    ...conversationAuthorityFields,
    senderAdultId: idSchema,
    text: z.string().trim().min(1).max(20_000).nullable(),
    images: z.array(imageReferenceSchema).max(10),
    replyToSignalId: idSchema.nullable(),
    source: conversationMessageSourceSchema,
  })
  .strict()
  .superRefine((signal, context) => {
    if (signal.text === null && signal.images.length === 0) {
      context.addIssue({
        code: "custom",
        message: "A conversation message must contain text or an image.",
      });
    }
  });

export const gmailMessageChangedSignalSchema = signalEnvelopeSchema
  .extend({
    type: z.literal("gmail.message.changed"),
    ownerAdultId: idSchema,
    connectionId: idSchema,
    messageId: z.string().trim().min(1).max(500),
    threadId: z.string().trim().min(1).max(500),
    historyId: z.string().trim().min(1).max(500),
  })
  .strict();

export const timerFiredSignalSchema = signalEnvelopeSchema
  .extend({
    type: z.literal("timer.fired"),
    timerId: idSchema,
    episodeId: idSchema,
    episodeVersion: z.number().int().positive(),
    scheduledFor: timestampSchema,
  })
  .strict();

export const effectReceiptSignalSchema = signalEnvelopeSchema
  .extend({
    type: z.literal("effect.receipt"),
    effectId: idSchema,
    episodeId: idSchema.nullable(),
    status: z.enum(["committed", "failed"]),
    providerReceiptId: z.string().trim().min(1).max(500).nullable(),
    detail: z.string().trim().min(1).max(2_000).nullable(),
  })
  .strict()
  .superRefine((receipt, context) => {
    if (receipt.status === "committed" && receipt.providerReceiptId === null) {
      context.addIssue({
        code: "custom",
        path: ["providerReceiptId"],
        message: "A committed effect requires authenticated provider proof.",
      });
    }
    if (receipt.status === "failed" && receipt.detail === null) {
      context.addIssue({
        code: "custom",
        path: ["detail"],
        message: "A failed effect receipt requires a failure detail.",
      });
    }
  });

export const householdCreatedSignalSchema = signalEnvelopeSchema
  .extend({
    type: z.literal("household.created"),
    name: z.string().trim().min(1).max(160),
    timeZone: z.string().trim().min(1).max(100),
    foundingAdult: verifiedAdultSchema,
    plannedAdult: plannedAdultSchema.optional(),
  })
  .strict();

export const familyMemberUpsertedSignalSchema = signalEnvelopeSchema
  .extend({
    type: z.literal("family.member.upserted"),
    actorAdultId: idSchema,
    member: familyMemberInputSchema,
    status: familyMemberStatusSchema,
  })
  .strict();

export const adultEnrollmentIssuedSignalSchema = signalEnvelopeSchema
  .extend({
    type: z.literal("adult.enrollment.issued"),
    actorAdultId: idSchema,
    adultId: idSchema,
    challengeDigest: z.string().regex(/^[a-f0-9]{64}$/),
    expiresAt: timestampSchema,
  })
  .strict();

export const adultEnrollmentRedeemedSignalSchema = signalEnvelopeSchema
  .extend({
    type: z.literal("adult.enrollment.redeemed"),
    adultId: idSchema,
    challengeDigest: z.string().regex(/^[a-f0-9]{64}$/),
    identitySubjectDigest: z.string().regex(/^[a-f0-9]{64}$/),
    consentVersion: z.string().trim().min(1).max(100),
    consentedAt: timestampSchema,
    conversationId: idSchema,
    providerConversationId: z.string().trim().min(1).max(500),
  })
  .strict();

const conversationBindingFields = {
  conversationId: idSchema,
  audience: conversationAudienceSchema,
  ...conversationAuthorityFields,
  authorizedAdultIds: z.array(idSchema).min(1).max(20),
  providerConversationId: z.string().trim().min(1).max(500),
};

function requireDistinctAuthorizedAdults(
  value: { authorizedAdultIds: string[] },
  context: z.core.$RefinementCtx,
): void {
  if (new Set(value.authorizedAdultIds).size !== value.authorizedAdultIds.length) {
    context.addIssue({
      code: "custom",
      path: ["authorizedAdultIds"],
      message: "Authorized adults must be distinct.",
    });
  }
}

export const conversationBoundSignalSchema = signalEnvelopeSchema
  .extend({
    type: z.literal("conversation.bound"),
    actorAdultId: idSchema,
    ...conversationBindingFields,
  })
  .strict()
  .superRefine(requireDistinctAuthorizedAdults);

export const householdProfileSchema = z
  .object({
    householdId: idSchema,
    name: z.string().trim().min(1).max(160),
    timeZone: z.string().trim().min(1).max(100),
    version: z.number().int().nonnegative(),
    members: z.array(familyMemberProfileSchema).max(100),
    identityBoundAdultIds: z.array(idSchema).max(2),
    onboardingComplete: z.boolean(),
  })
  .strict();
export type HouseholdProfile = z.infer<typeof householdProfileSchema>;

export const householdSignalSchema = z.discriminatedUnion("type", [
  householdCreatedSignalSchema,
  familyMemberUpsertedSignalSchema,
  adultEnrollmentIssuedSignalSchema,
  adultEnrollmentRedeemedSignalSchema,
  conversationBoundSignalSchema,
  conversationMessageSignalSchema,
  gmailMessageChangedSignalSchema,
  timerFiredSignalSchema,
  effectReceiptSignalSchema,
]);
export type HouseholdSignal = z.infer<typeof householdSignalSchema>;

export const workerSignalSchema = z.discriminatedUnion("type", [
  conversationMessageSignalSchema,
  gmailMessageChangedSignalSchema,
  timerFiredSignalSchema,
  effectReceiptSignalSchema,
]);
export type WorkerSignal = z.infer<typeof workerSignalSchema>;

export const acceptanceReceiptSchema = z
  .object({
    signalId: idSchema,
    householdId: idSchema,
    disposition: z.enum(["accepted", "duplicate"]),
    acceptedAt: timestampSchema,
  })
  .strict();
export type AcceptanceReceipt = z.infer<typeof acceptanceReceiptSchema>;

export const conversationTurnSchema = z
  .object({
    signalId: idSchema,
    speaker: z.union([z.literal("florence"), idSchema]),
    text: z.string().trim().min(1).max(20_000),
    occurredAt: timestampSchema,
  })
  .strict();
export type ConversationTurn = z.infer<typeof conversationTurnSchema>;

export const conversationSnapshotSchema = z
  .object({
    id: idSchema,
    audience: conversationAudienceSchema,
    ...conversationAuthorityFields,
    authorizedAdultIds: z.array(idSchema).min(1).max(2),
    recentTurns: z.array(conversationTurnSchema).max(100),
  })
  .strict();
export type ConversationSnapshot = z.infer<typeof conversationSnapshotSchema>;

export const householdMemorySchema = z
  .object({
    id: idSchema,
    statement: z.string().trim().min(1).max(4_000),
    sourceSignalIds: z.array(idSchema).min(1).max(50),
    supersedesMemoryId: idSchema.nullable(),
    recordedAt: timestampSchema,
  })
  .strict();
export type HouseholdMemory = z.infer<typeof householdMemorySchema>;

export const openEpisodeSchema = z
  .object({
    id: idSchema,
    title: z.string().trim().min(1).max(160),
    outcome: z.string().trim().min(1).max(2_000),
    dueAt: timestampSchema.nullable(),
    status: z.enum(["proposed", "owned"]),
    ownerAdultId: idSchema.nullable(),
    sourceSignalIds: z.array(idSchema).min(1).max(50),
    version: z.number().int().positive(),
    updatedAt: timestampSchema,
  })
  .strict();
export type OpenEpisode = z.infer<typeof openEpisodeSchema>;

export const gmailCalendarDraftSchema = z
  .object({
    title: z.string().trim().min(1).max(160),
    startsAt: timestampSchema,
    endsAt: timestampSchema,
    timeZone: z.string().trim().min(1).max(100),
    location: z.string().trim().min(1).max(500).nullable(),
  })
  .strict()
  .superRefine((draft, context) => {
    if (new Date(draft.endsAt).getTime() <= new Date(draft.startsAt).getTime()) {
      context.addIssue({ code: "custom", path: ["endsAt"], message: "Calendar end must follow start." });
    }
  });
export type GmailCalendarDraft = z.infer<typeof gmailCalendarDraftSchema>;

export const gmailEvidenceSchema = z
  .object({
    messageId: z.string().trim().min(1).max(500),
    threadId: z.string().trim().min(1).max(500),
    historyId: z.string().trim().min(1).max(500),
    from: z.string().trim().min(1).max(500),
    subject: z.string().trim().min(1).max(1_000).nullable(),
    sentAt: timestampSchema,
    text: z.string().trim().min(1).max(50_000),
  })
  .strict();
export type GmailEvidence = z.infer<typeof gmailEvidenceSchema>;

export const privateGmailCandidateSchema = z
  .object({
    candidateId: idSchema,
    version: z.literal(1),
    candidateDigest: z.string().regex(/^[a-f0-9]{64}$/),
    ownerAdultId: idSchema,
    privateSummary: z.string().trim().min(1).max(2_000),
    householdMeaning: z.string().trim().min(1).max(160),
    calendarDraft: gmailCalendarDraftSchema.nullable(),
    sourceSignalIds: sourceSignalIdsSchema,
  })
  .strict();
export type PrivateGmailCandidate = z.infer<typeof privateGmailCandidateSchema>;

export const householdSnapshotSchema = z
  .object({
    householdId: idSchema,
    timeZone: z.string().trim().min(1).max(100),
    asOf: timestampSchema,
    members: z.array(familyMemberSnapshotSchema).min(1).max(100),
    conversation: conversationSnapshotSchema.nullable(),
    memories: z.array(householdMemorySchema).max(500),
    openEpisodes: z.array(openEpisodeSchema).max(100),
    privateGmailCandidates: z.array(privateGmailCandidateSchema).max(50).optional(),
    privateCalendarApprovalCandidate: privateGmailCandidateSchema.nullable().optional(),
  })
  .strict()
  .superRefine((snapshot, context) => {
    const memberIds = new Set(snapshot.members.map((member) => member.id));
    if (memberIds.size !== snapshot.members.length) {
      context.addIssue({
        code: "custom",
        path: ["members"],
        message: "A household snapshot requires distinct family members.",
      });
    }

    const verifiedAdultIds = new Set(
      snapshot.members
        .filter((member) => member.kind === "adult" && member.status === "verified")
        .map((member) => member.id),
    );
    if (verifiedAdultIds.size === 0 || verifiedAdultIds.size > 2) {
      context.addIssue({
        code: "custom",
        path: ["members"],
        message: "A household snapshot requires one or two verified adults.",
      });
    }

    if (snapshot.conversation?.audience === "group" && verifiedAdultIds.size !== 2) {
      context.addIssue({
        code: "custom",
        path: ["members"],
        message: "A household group snapshot requires two verified adults.",
      });
    }

    if (
      snapshot.conversation?.audience === "group" &&
      ((snapshot.privateGmailCandidates?.length ?? 0) > 0 || snapshot.privateCalendarApprovalCandidate)
    ) {
      context.addIssue({
        code: "custom",
        path: ["privateGmailCandidates"],
        message: "Private Gmail candidates cannot enter a household group snapshot.",
      });
    }
    for (const [index, candidate] of (snapshot.privateGmailCandidates ?? []).entries()) {
      if (!snapshot.conversation?.authorizedAdultIds.includes(candidate.ownerAdultId)) {
        context.addIssue({
          code: "custom",
          path: ["privateGmailCandidates", index, "ownerAdultId"],
          message: "A private Gmail candidate must belong to this conversation's adult.",
        });
      }
    }
    if (snapshot.privateCalendarApprovalCandidate) {
      const candidate = snapshot.privateCalendarApprovalCandidate;
      if (
        candidate.calendarDraft === null ||
        !snapshot.conversation?.authorizedAdultIds.includes(candidate.ownerAdultId) ||
        !(snapshot.privateGmailCandidates ?? []).some(
          (visible) =>
            visible.candidateId === candidate.candidateId &&
            visible.version === candidate.version &&
            visible.candidateDigest === candidate.candidateDigest,
        )
      ) {
        context.addIssue({
          code: "custom",
          path: ["privateCalendarApprovalCandidate"],
          message: "Calendar consent requires one current draft in its owner's private conversation.",
        });
      }
    }

    for (const [index, adultId] of (snapshot.conversation?.authorizedAdultIds ?? []).entries()) {
      if (!verifiedAdultIds.has(adultId)) {
        context.addIssue({
          code: "custom",
          path: ["conversation", "authorizedAdultIds", index],
          message: "Conversation access must belong to a verified household adult.",
        });
      }
    }
  });
export type HouseholdSnapshot = z.infer<typeof householdSnapshotSchema>;

export const workerProposalSchema = z.discriminatedUnion("type", [
  z.object({ type: z.literal("ignore"), reason: z.string().trim().min(1).max(500) }).strict(),
  z
    .object({
      type: z.literal("ask"),
      text: z.string().trim().min(1).max(2_000),
      episodeId: idSchema.nullable(),
      sourceSignalIds: sourceSignalIdsSchema,
    })
    .strict(),
  z
    .object({
      type: z.literal("respond"),
      text: z.string().trim().min(1).max(2_000),
      sourceSignalIds: sourceSignalIdsSchema,
    })
    .strict(),
  z
    .object({
      type: z.literal("remember"),
      statement: z.string().trim().min(1).max(4_000),
      sourceSignalIds: sourceSignalIdsSchema,
      supersedesMemoryId: idSchema.nullable(),
    })
    .strict(),
  z
    .object({
      type: z.literal("stage_gmail_candidate"),
      privateSummary: z.string().trim().min(1).max(2_000),
      householdMeaning: z.string().trim().min(1).max(160),
      calendarDraft: gmailCalendarDraftSchema.nullable(),
      sourceSignalIds: sourceSignalIdsSchema,
    })
    .strict(),
  z
    .object({
      type: z.literal("promote_gmail_candidate"),
      candidateId: idSchema,
      version: z.literal(1),
      candidateDigest: z.string().regex(/^[a-f0-9]{64}$/),
      responseText: z.string().trim().min(1).max(2_000),
      sourceSignalIds: sourceSignalIdsSchema,
    })
    .strict(),
  z
    .object({
      type: z.literal("approve_gmail_calendar"),
      candidateId: idSchema,
      version: z.literal(1),
      candidateDigest: z.string().regex(/^[a-f0-9]{64}$/),
      sourceSignalIds: sourceSignalIdsSchema,
    })
    .strict(),
  z
    .object({
      type: z.literal("propose_episode"),
      title: z.string().trim().min(1).max(160),
      outcome: z.string().trim().min(1).max(2_000),
      dueAt: timestampSchema.nullable(),
      suggestedOwnerAdultId: idSchema.nullable(),
      responseText: z.string().trim().min(1).max(2_000).nullable(),
      sourceSignalIds: sourceSignalIdsSchema,
    })
    .strict(),
  z
    .object({
      type: z.literal("set_episode_owner"),
      episodeId: idSchema,
      ownerAdultId: idSchema.nullable(),
      responseText: z.string().trim().min(1).max(2_000).nullable(),
      sourceSignalIds: sourceSignalIdsSchema,
    })
    .strict(),
  z
    .object({
      type: z.literal("update_episode"),
      episodeId: idSchema,
      outcome: z.string().trim().min(1).max(2_000),
      dueAt: timestampSchema.nullable(),
      responseText: z.string().trim().min(1).max(2_000).nullable(),
      sourceSignalIds: sourceSignalIdsSchema,
    })
    .strict(),
  z
    .object({
      type: z.literal("cancel_episode"),
      episodeId: idSchema,
      reason: z.string().trim().min(1).max(4_000),
      responseText: z.string().trim().min(1).max(2_000).nullable(),
      sourceSignalIds: sourceSignalIdsSchema,
    })
    .strict(),
  z
    .object({
      type: z.literal("complete_episode"),
      episodeId: idSchema,
      result: z.string().trim().min(1).max(4_000),
      responseText: z.string().trim().min(1).max(2_000).nullable(),
      sourceSignalIds: sourceSignalIdsSchema,
    })
    .strict(),
]);
export type WorkerProposal = z.infer<typeof workerProposalSchema>;
export const workerProposalsSchema = z.array(workerProposalSchema).max(6);
export const workerResultSchema = z.object({ proposals: workerProposalsSchema }).strict();
export type WorkerResult = z.infer<typeof workerResultSchema>;

export const workerInputSchema = z
  .object({
    signal: workerSignalSchema,
    snapshot: householdSnapshotSchema,
    gmailEvidence: gmailEvidenceSchema.optional(),
  })
  .strict()
  .superRefine(({ signal, snapshot, gmailEvidence }, context) => {
    if (signal.householdId !== snapshot.householdId) {
      context.addIssue({
        code: "custom",
        path: ["signal", "householdId"],
        message: "A signal must belong to its household snapshot.",
      });
    }

    if (signal.type === "conversation.message") {
      const conversation = snapshot.conversation;
      const isAuthorized =
        conversation?.id === signal.conversationId &&
        conversation.audience === signal.audience &&
        conversation.authorityVersion === signal.authorityVersion &&
        conversation.participantSetDigest === signal.participantSetDigest &&
        conversation.authorizedAdultIds.includes(signal.senderAdultId);
      if (!isAuthorized) {
        context.addIssue({
          code: "custom",
          path: ["signal", "conversationId"],
          message: "A conversation signal must match an authorized snapshot conversation.",
        });
      }
    }

    if (signal.type === "gmail.message.changed") {
      const conversation = snapshot.conversation;
      const isPrivateOwner =
        conversation?.audience === "private" &&
        conversation.authorizedAdultIds.length === 1 &&
        conversation.authorizedAdultIds[0] === signal.ownerAdultId;
      const evidenceMatches =
        signal.messageId === gmailEvidence?.messageId &&
        signal.threadId === gmailEvidence.threadId &&
        signal.historyId === gmailEvidence.historyId;
      if (!isPrivateOwner || !evidenceMatches) {
        context.addIssue({
          code: "custom",
          path: ["gmailEvidence"],
          message: "Gmail evidence must match its current private owner and immutable source IDs.",
        });
      }
    } else if (gmailEvidence !== undefined) {
      context.addIssue({
        code: "custom",
        path: ["gmailEvidence"],
        message: "Gmail evidence is only valid for a Gmail source signal.",
      });
    }
  });
export type WorkerInput = z.infer<typeof workerInputSchema>;
