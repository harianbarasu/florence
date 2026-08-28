import { z } from "zod";

export const idSchema = z.uuid();
export const timestampSchema = z.iso.datetime();

const nonempty = (maximum: number) => z.string().trim().min(1).max(maximum);
const postalCodeSchema = z.string().regex(/^\d{5}(?:-\d{4})?$/);
const childAgeSchema = z.number().int().min(0).max(120);
const childGradeSchema = nonempty(80);
const familyOnboardingUsPhoneNumberSchema = z
  .string()
  .trim()
  .refine(
    (value) =>
      /^\+1\d{10}$/.test(value) || (/^[\d\s().-]+$/.test(value) && value.replace(/\D/g, "").length === 10),
    "Enter a 10-digit US mobile number.",
  )
  .transform((value) => (value.startsWith("+1") ? value : `+1${value.replace(/\D/g, "")}`));

const familyMemberFields = {
  kind: z.enum(["adult", "child"]),
  firstName: nonempty(160),
  lastName: nonempty(160).nullable(),
  displayName: nonempty(160),
  relationship: nonempty(160),
  age: childAgeSchema.optional(),
  grade: childGradeSchema.optional(),
  school: nonempty(300).optional(),
  activities: z.array(nonempty(300)).max(50).optional(),
};

export const familyMemberInputSchema = z
  .object({
    kind: z.literal("child"),
    firstName: nonempty(160),
    lastName: nonempty(160).nullable().optional(),
    age: childAgeSchema.optional(),
    grade: childGradeSchema.optional(),
    school: nonempty(300).optional(),
    activities: z.array(nonempty(300)).max(50).optional(),
  })
  .strict();
export type FamilyMemberInput = z.infer<typeof familyMemberInputSchema>;

const familyMemberPatchFields = {
  firstName: nonempty(160).optional(),
  lastName: nonempty(160).nullable().optional(),
  age: childAgeSchema.nullable().optional(),
  grade: childGradeSchema.nullable().optional(),
  school: nonempty(300).nullable().optional(),
  activities: z.array(nonempty(300)).max(50).optional(),
  postalCode: postalCodeSchema.optional(),
};

export const patchFamilyMemberInputSchema = z
  .object(familyMemberPatchFields)
  .strict()
  .superRefine((member, context) => {
    if (Object.values(member).every((value) => value === undefined)) {
      context.addIssue({ code: "custom", message: "Change at least one family member detail." });
    }
  });
export type PatchFamilyMemberInput = z.infer<typeof patchFamilyMemberInputSchema>;

export const familyMemberMutationInputSchema = z.union([
  familyMemberInputSchema,
  patchFamilyMemberInputSchema,
]);
export type FamilyMemberMutationInput = z.infer<typeof familyMemberMutationInputSchema>;

export const familyMemberStatusSchema = z.enum(["verified", "planned", "represented"]);

export const messagesIdentityStatusSchema = z.enum(["not_invited", "invited", "connected"]);

export const familyMemberProfileSchema = z
  .object({
    id: idSchema,
    ...familyMemberFields,
    postalCode: postalCodeSchema.nullable().optional(),
    status: familyMemberStatusSchema,
    messagesIdentity: messagesIdentityStatusSchema.nullable(),
  })
  .strict()
  .superRefine((member, context) => {
    if (member.kind === "adult" && member.lastName === null) {
      context.addIssue({
        code: "custom",
        path: ["lastName"],
        message: "An adult needs a last name.",
      });
    }
    if (member.kind === "adult" && member.age !== undefined) {
      context.addIssue({
        code: "custom",
        path: ["age"],
        message: "Age is only recorded for children.",
      });
    }
    if (member.kind === "adult" && member.grade !== undefined) {
      context.addIssue({
        code: "custom",
        path: ["grade"],
        message: "Grade is only recorded for children.",
      });
    }
    if (member.kind === "child" && member.messagesIdentity !== null) {
      context.addIssue({
        code: "custom",
        path: ["kind"],
        message: "A child is represented by the household and never has a Messages identity.",
      });
    }
  });
export type FamilyMemberProfile = z.infer<typeof familyMemberProfileSchema>;

export const imageReferenceSchema = z
  .object({
    assetId: idSchema,
    mimeType: z.enum(["image/jpeg", "image/png", "image/webp", "image/heic"]),
  })
  .strict();
export type ImageReference = z.infer<typeof imageReferenceSchema>;

export const vaultVisibilitySchema = z.enum(["private", "household"]);
export type VaultVisibility = z.infer<typeof vaultVisibilitySchema>;

export const vaultSourceSchema = z
  .object({
    id: idSchema,
    kind: z.enum(["message", "gmail", "document", "web"]),
    label: nonempty(300),
    occurredAt: timestampSchema,
  })
  .strict();
export type VaultSource = z.infer<typeof vaultSourceSchema>;

const memoryKindSchema = z.enum(["fact", "preference", "routine", "artifact"]);
const artifactKindSchema = z.enum(["recipe", "list", "plan", "note", "reference", "other"]);

export const memoryPresentationSchema = z
  .object({
    memoryKind: memoryKindSchema.default("fact"),
    artifactKind: artifactKindSchema.nullable().default(null),
    title: nonempty(300).nullable().default(null),
    details: nonempty(12_000).nullable().default(null),
    tags: z.array(nonempty(80)).max(20).default([]),
  })
  .strict()
  .superRefine((memory, context) => {
    const normalizedTags = memory.tags.map((tag) => tag.toLocaleLowerCase("en-US"));
    if (new Set(normalizedTags).size !== normalizedTags.length) {
      context.addIssue({
        code: "custom",
        path: ["tags"],
        message: "Memory tags must be unique.",
      });
    }
    if (memory.memoryKind === "artifact") {
      if (memory.artifactKind === null || memory.title === null || memory.details === null) {
        context.addIssue({
          code: "custom",
          message: "A reusable artifact needs a kind, title, and usable details.",
        });
      }
      return;
    }
    if (memory.artifactKind !== null) {
      context.addIssue({
        code: "custom",
        path: ["artifactKind"],
        message: "Only a reusable artifact may have an artifact kind.",
      });
    }
  });
export type MemoryPresentation = z.infer<typeof memoryPresentationSchema>;
const storedMemoryPresentationSchema = memoryPresentationSchema.required();

/** Decode only the explicit presentation persisted by current writers and migration 006. */
export function decodeMemoryPresentation(value: unknown): MemoryPresentation {
  const keys = ["memoryKind", "artifactKind", "title", "details", "tags"] as const;
  if (!isUnknownRecord(value) || keys.some((key) => !Object.hasOwn(value, key))) {
    throw new Error("Stored memory presentation is missing explicit fields");
  }
  return storedMemoryPresentationSchema.parse({
    memoryKind: value.memoryKind,
    artifactKind: value.artifactKind,
    title: value.title,
    details: value.details,
    tags: value.tags,
  });
}

export const vaultFactSchema = memoryPresentationSchema
  .safeExtend({
    id: idSchema,
    statement: nonempty(4_000),
    visibility: vaultVisibilitySchema,
    source: vaultSourceSchema.nullable(),
    recordedAt: timestampSchema.nullable(),
    editable: z.boolean(),
    deletable: z.boolean(),
  })
  .strict();
export type VaultFact = z.infer<typeof vaultFactSchema>;

function isUnknownRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const vaultWatchFields = {
  workId: idSchema,
  objective: nonempty(2_000),
  currentConclusion: nonempty(4_000).nullable(),
  visibility: vaultVisibilitySchema,
  status: z.enum(["active", "paused"]),
  source: vaultSourceSchema.nullable(),
};

const watchInterestTermSchema = nonempty(100);

export const vaultWatchSchema = z.discriminatedUnion("kind", [
  z
    .object({
      ...vaultWatchFields,
      kind: z.literal("monitor"),
    })
    .strict(),
  z
    .object({
      ...vaultWatchFields,
      kind: z.literal("interest"),
    })
    .strict(),
]);
export type VaultWatch = z.infer<typeof vaultWatchSchema>;

export const googleConnectionSummarySchema = z
  .object({
    connectionId: idSchema,
    status: z.literal("active"),
    emailLabel: z.email(),
    historyReviewReady: z.boolean(),
    assistantWorkReady: z.boolean(),
    lastError: nonempty(2_000).nullable(),
  })
  .strict();
export type GoogleConnectionSummary = z.infer<typeof googleConnectionSummarySchema>;

export const setupChecklistSchema = z
  .object({
    ownOnboardingComplete: z.boolean(),
    secondAdultAdded: z.boolean(),
    partnerInvitation: z.enum(["not_ready", "ready", "approved", "invited", "connected"]),
    bothAdultsMessagesConnected: z.boolean(),
    bothAdultsGoogleConnected: z.boolean(),
    familyGroupConnected: z.boolean(),
    familyCalendarConnected: z.boolean(),
    initialBriefing: z.enum(["not_ready", "preparing", "sent"]),
  })
  .strict();

export const preferencesInputSchema = z
  .object({
    proactiveGoogleEnabled: z.boolean(),
    automaticFamilyCalendarEnabled: z.boolean(),
    privateConflictBusySharingEnabled: z.boolean(),
  })
  .strict();
export type PreferencesInput = z.infer<typeof preferencesInputSchema>;
export const preferencesViewSchema = preferencesInputSchema;

export const householdVaultSchema = z
  .object({
    timeZone: nonempty(100),
    postalCode: postalCodeSchema.nullable(),
    members: z.array(familyMemberProfileSchema).max(100),
    facts: z.array(vaultFactSchema).max(500),
    watches: z.array(vaultWatchSchema).max(100),
  })
  .strict();
export type HouseholdVault = z.infer<typeof householdVaultSchema>;

export const workspaceViewSchema = z
  .object({
    viewer: z
      .object({
        adultId: idSchema,
        displayName: nonempty(160).nullable(),
        lastName: nonempty(160).nullable(),
      })
      .strict(),
    workspace: z
      .object({
        messagesUrl: nonempty(2_000).nullable(),
        googleConnections: z.array(googleConnectionSummarySchema).max(10),
        setup: setupChecklistSchema,
      })
      .strict(),
    vault: householdVaultSchema.nullable(),
    preferences: preferencesViewSchema,
  })
  .strict();
export type WorkspaceView = z.infer<typeof workspaceViewSchema>;

export const calendarMonthSchema = z.string().regex(/^[1-9]\d{3}-(?:0[1-9]|1[0-2])$/);

export const familyCalendarMonthQuerySchema = z
  .object({
    month: calendarMonthSchema,
  })
  .strict();

const familyCalendarEventFields = {
  status: z.enum(["confirmed", "tentative"]),
  title: nonempty(500).nullable(),
  location: nonempty(500).nullable(),
};

export const familyCalendarEventSchema = z.discriminatedUnion("intervalKind", [
  z
    .object({
      ...familyCalendarEventFields,
      intervalKind: z.literal("timed"),
      startsAt: timestampSchema,
      endsAt: timestampSchema,
      timeZone: nonempty(100),
    })
    .strict(),
  z
    .object({
      ...familyCalendarEventFields,
      intervalKind: z.literal("all_day"),
      startDate: z.iso.date(),
      endDate: z.iso.date(),
    })
    .strict(),
]);
export type FamilyCalendarEvent = z.infer<typeof familyCalendarEventSchema>;

const familyCalendarMonthFields = {
  month: calendarMonthSchema,
  timeZone: nonempty(100),
};

export const familyCalendarMonthViewSchema = z.discriminatedUnion("status", [
  z
    .object({
      ...familyCalendarMonthFields,
      status: z.literal("ready"),
      calendarName: nonempty(160),
      truncated: z.boolean(),
      events: z.array(familyCalendarEventSchema).max(100),
    })
    .strict(),
  z
    .object({
      ...familyCalendarMonthFields,
      status: z.literal("not_ready"),
      calendarName: nonempty(160).nullable(),
    })
    .strict(),
  z
    .object({
      ...familyCalendarMonthFields,
      status: z.literal("temporarily_unavailable"),
      calendarName: nonempty(160),
    })
    .strict(),
]);
export type FamilyCalendarMonthView = z.infer<typeof familyCalendarMonthViewSchema>;

const personNameSchema = z
  .object({
    firstName: nonempty(160),
    lastName: nonempty(160),
  })
  .strict();

const familyOnboardingPartnerSchema = personNameSchema
  .extend({
    phoneNumber: familyOnboardingUsPhoneNumberSchema,
  })
  .strict();

const familyOnboardingChildSchema = z
  .object({
    firstName: nonempty(160),
    lastName: nonempty(160).optional(),
  })
  .extend({
    age: childAgeSchema.optional(),
    grade: childGradeSchema.optional(),
    school: nonempty(300).optional(),
    activities: z.array(nonempty(300)).max(50).optional(),
  })
  .strict();

const familyOnboardingBase = {
  postalCode: postalCodeSchema,
  children: z.array(familyOnboardingChildSchema).min(1).max(20),
};

export const completeFamilyOnboardingInputSchema = z.discriminatedUnion("mode", [
  z
    .object({
      ...familyOnboardingBase,
      mode: z.literal("two_adult"),
      partner: familyOnboardingPartnerSchema,
    })
    .strict(),
  z
    .object({
      ...familyOnboardingBase,
      mode: z.literal("solo"),
      partner: z.null(),
    })
    .strict(),
]);
export type CompleteFamilyOnboardingInput = z.infer<typeof completeFamilyOnboardingInputSchema>;

export const setupSessionInputSchema = z
  .object({
    setupToken: nonempty(2_000),
    profile: z
      .object({
        firstName: nonempty(160),
        lastName: nonempty(160),
        timeZone: nonempty(100),
        guardianAttested: z.literal(true),
        proactiveUseAccepted: z.literal(true),
        privateConflictBusySharingEnabled: z.boolean().default(false),
      })
      .strict(),
  })
  .strict();
export type SetupSessionInput = z.infer<typeof setupSessionInputSchema>;

export const webAccessPathSchema = z.enum(["/", "/calendar", "/vault", "/preferences"]);
export type WebAccessPath = z.infer<typeof webAccessPathSchema>;

export const accessSessionInputSchema = z.object({ accessToken: nonempty(2_000) }).strict();
export type AccessSessionInput = z.infer<typeof accessSessionInputSchema>;

export const sessionInputSchema = z.union([setupSessionInputSchema, accessSessionInputSchema]);
export type SessionInput = z.infer<typeof sessionInputSchema>;

export const sessionResponseSchema = z
  .object({ adultId: idSchema, accessPath: webAccessPathSchema.optional() })
  .strict();
export type SessionResponse = z.infer<typeof sessionResponseSchema>;

export const patchFactInputSchema = z
  .object({
    statement: nonempty(4_000),
  })
  .strict();
export type PatchFactInput = z.infer<typeof patchFactInputSchema>;

export const patchWatchInputSchema = z
  .discriminatedUnion("kind", [
    z
      .object({
        kind: z.literal("monitor"),
        objective: nonempty(2_000).optional(),
        endCondition: nonempty(2_000).optional(),
        status: z.enum(["active", "paused"]).optional(),
      })
      .strict(),
    z
      .object({
        kind: z.literal("interest"),
        objective: nonempty(2_000).optional(),
        genericTerms: z.array(watchInterestTermSchema).min(1).max(8).optional(),
        status: z.enum(["active", "paused"]).optional(),
      })
      .strict(),
  ])
  .superRefine((input, context) => {
    const hasCorrection =
      input.objective !== undefined ||
      input.status !== undefined ||
      (input.kind === "monitor" ? input.endCondition !== undefined : input.genericTerms !== undefined);
    if (!hasCorrection) {
      context.addIssue({
        code: "custom",
        message: "Change what Florence watches or whether it is paused.",
      });
    }
    if (input.kind === "interest" && input.genericTerms) {
      const terms = input.genericTerms.map((term) => term.toLocaleLowerCase("en-US"));
      if (new Set(terms).size !== terms.length) {
        context.addIssue({
          code: "custom",
          path: ["genericTerms"],
          message: "Use each interest once.",
        });
      }
    }
  });
export type PatchWatchInput = z.infer<typeof patchWatchInputSchema>;

export const disconnectGoogleConnectionInputSchema = z.object({ connectionId: idSchema }).strict();
export type DisconnectGoogleConnectionInput = z.infer<typeof disconnectGoogleConnectionInputSchema>;

export const workspaceResponseSchema = z.object({ workspace: workspaceViewSchema }).strict();

export const googleProviderRevocationSchema = z.enum(["confirmed", "unconfirmed", "not-needed"]);
export type GoogleProviderRevocation = z.infer<typeof googleProviderRevocationSchema>;

export const disconnectGoogleConnectionResponseSchema = z
  .object({
    workspace: workspaceViewSchema,
    localAccess: z.literal("disconnected"),
    providerRevocation: googleProviderRevocationSchema,
  })
  .strict();
export type DisconnectGoogleConnectionResponse = z.infer<typeof disconnectGoogleConnectionResponseSchema>;

export const googleDataDeletionSummarySchema = z
  .object({
    disconnectedConnections: z.number().int().nonnegative(),
    googleSources: z.number().int().nonnegative(),
    facts: z.number().int().nonnegative(),
    watches: z.number().int().nonnegative(),
    calendarActions: z.number().int().nonnegative(),
    unsentMessages: z.number().int().nonnegative(),
  })
  .strict();
export type GoogleDataDeletionSummary = z.infer<typeof googleDataDeletionSummarySchema>;

export const deleteGoogleDerivedDataResponseSchema = z
  .object({
    workspace: workspaceViewSchema,
    providerRevocation: googleProviderRevocationSchema,
    deletion: googleDataDeletionSummarySchema,
  })
  .strict();
export type DeleteGoogleDerivedDataResponse = z.infer<typeof deleteGoogleDerivedDataResponseSchema>;

export const googleStartResponseSchema = z.object({ authorizationUrl: z.url() }).strict();
