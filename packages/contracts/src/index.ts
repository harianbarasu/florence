import { z } from "zod";

export const idSchema = z.uuid();
export const timestampSchema = z.iso.datetime();

const nonempty = (maximum: number) => z.string().trim().min(1).max(maximum);

const familyMemberFields = {
  kind: z.enum(["adult", "child"]),
  role: z.enum(["steward", "caregiver", "dependent"]),
  displayName: nonempty(160),
  relationship: nonempty(160),
  aliases: z.array(nonempty(160)).max(20).optional(),
  birthYear: z.number().int().min(1800).max(3000).optional(),
  school: nonempty(300).optional(),
  currentGrade: nonempty(100).optional(),
  academicYear: nonempty(100).optional(),
  gradeEffectiveFrom: z.iso.date().optional(),
  activities: z.array(nonempty(300)).max(50).optional(),
};

function requireCompleteGrade(
  member: {
    currentGrade?: string | undefined;
    academicYear?: string | undefined;
    gradeEffectiveFrom?: string | undefined;
  },
  context: z.core.$RefinementCtx,
): void {
  const fields = [member.currentGrade, member.academicYear, member.gradeEffectiveFrom];
  if (fields.some(Boolean) && !fields.every(Boolean)) {
    context.addIssue({
      code: "custom",
      path: ["currentGrade"],
      message: "Grade, academic year, and effective date must be provided together.",
    });
  }
}

export const familyMemberInputSchema = z
  .object(familyMemberFields)
  .strict()
  .superRefine((member, context) => {
    requireCompleteGrade(member, context);
    if (member.kind === "child" && member.role !== "dependent") {
      context.addIssue({ code: "custom", path: ["role"], message: "A child must be a dependent." });
    }
    if (member.kind === "adult" && member.role === "dependent") {
      context.addIssue({
        code: "custom",
        path: ["role"],
        message: "An adult must be a steward or caregiver.",
      });
    }
  });
export type FamilyMemberInput = z.infer<typeof familyMemberInputSchema>;

export const familyMemberStatusSchema = z.enum(["verified", "planned", "represented"]);

export const messagesIdentityStatusSchema = z.enum(["not_invited", "invited", "connected"]);

export const familyMemberProfileSchema = z
  .object({
    id: idSchema,
    ...familyMemberFields,
    status: familyMemberStatusSchema,
    messagesIdentity: messagesIdentityStatusSchema.nullable(),
  })
  .strict()
  .superRefine((member, context) => {
    requireCompleteGrade(member, context);
    if (member.kind === "child" && (member.role !== "dependent" || member.messagesIdentity !== null)) {
      context.addIssue({
        code: "custom",
        path: ["kind"],
        message: "A child is represented by the household and never has a Messages identity.",
      });
    }
    if (member.kind === "adult" && member.role === "dependent") {
      context.addIssue({
        code: "custom",
        path: ["role"],
        message: "An adult must be a steward or caregiver.",
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

export const vaultFactSchema = z
  .object({
    id: idSchema,
    statement: nonempty(4_000),
    visibility: vaultVisibilitySchema,
    source: vaultSourceSchema,
    recordedAt: timestampSchema,
    editable: z.boolean(),
    deletable: z.boolean(),
  })
  .strict();
export type VaultFact = z.infer<typeof vaultFactSchema>;

export const vaultContactInputSchema = z
  .object({
    kind: z.enum(["address", "phone"]),
    label: nonempty(160),
    value: nonempty(1_000),
    visibility: vaultVisibilitySchema,
  })
  .strict();

export const vaultContactSchema = vaultContactInputSchema
  .extend({
    id: idSchema,
    source: vaultSourceSchema,
    editable: z.boolean(),
    deletable: z.boolean(),
  })
  .strict();
export type VaultContact = z.infer<typeof vaultContactSchema>;

export const googleConnectionSummarySchema = z
  .object({
    connectionId: idSchema,
    status: z.literal("active"),
    emailLabel: z.email(),
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
  })
  .strict();

export const preferencesInputSchema = z
  .object({
    appearance: z.enum(["light", "dark", "system"]),
  })
  .strict();
export type PreferencesInput = z.infer<typeof preferencesInputSchema>;
export const preferencesViewSchema = preferencesInputSchema;

export const householdVaultSchema = z
  .object({
    timeZone: nonempty(100),
    members: z.array(familyMemberProfileSchema).max(100),
    contacts: z.array(vaultContactSchema).max(100),
    facts: z.array(vaultFactSchema).max(500),
  })
  .strict();
export type HouseholdVault = z.infer<typeof householdVaultSchema>;

export const workspaceViewSchema = z
  .object({
    viewer: z
      .object({
        adultId: idSchema,
        displayName: nonempty(160).nullable(),
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

const personNameSchema = z
  .object({
    firstName: nonempty(160),
    lastName: nonempty(160),
  })
  .strict();

const familyOnboardingPartnerSchema = personNameSchema
  .extend({
    phoneNumber: z.string().regex(/^\+[1-9]\d{7,14}$/),
  })
  .strict();

const familyOnboardingChildSchema = z
  .object({
    firstName: nonempty(160),
    lastName: nonempty(160).optional(),
  })
  .extend({
    school: nonempty(300).optional(),
    activities: z.array(nonempty(300)).max(50).optional(),
  })
  .strict();

const familyOnboardingBase = {
  familyLabel: nonempty(160),
  postalCode: z.string().regex(/^\d{5}(?:-\d{4})?$/),
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
      })
      .strict(),
  })
  .strict();
export type SetupSessionInput = z.infer<typeof setupSessionInputSchema>;

export const sessionResponseSchema = z.object({ adultId: idSchema }).strict();
export type SessionResponse = z.infer<typeof sessionResponseSchema>;

export const patchFactInputSchema = z.object({ statement: nonempty(4_000) }).strict();
export type PatchFactInput = z.infer<typeof patchFactInputSchema>;

export const disconnectGoogleConnectionInputSchema = z.object({ connectionId: idSchema }).strict();
export type DisconnectGoogleConnectionInput = z.infer<typeof disconnectGoogleConnectionInputSchema>;

export const workspaceResponseSchema = z.object({ workspace: workspaceViewSchema }).strict();

export const googleStartResponseSchema = z.object({ authorizationUrl: z.url() }).strict();
