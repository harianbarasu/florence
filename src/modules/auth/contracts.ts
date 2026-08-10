import { z } from "zod";

export const DEFAULT_HANDOFF_TTL_SECONDS = 10 * 60;
export const MAX_STANDARD_HANDOFF_TTL_SECONDS = 15 * 60;
export const ONBOARDING_HANDOFF_TTL_SECONDS = 24 * 60 * 60;
export const GOOGLE_AUTH_ATTEMPT_TTL_SECONDS = 10 * 60;

export const HandoffPurposeSchema = z.enum([
  "web_sign_in",
  "onboarding",
  "account_controls",
  "household_invitation",
  "private_bridge_standing",
  "invitation",
  "private_review",
]);
export type HandoffPurpose = z.infer<typeof HandoffPurposeSchema>;

export const CreateHandoffInputSchema = z
  .strictObject({
    personId: z.string().uuid(),
    privateIdentityId: z.string().uuid(),
    privateConversationId: z.string().uuid().nullable(),
    purpose: HandoffPurposeSchema,
    context: z.record(z.string(), z.unknown()).default({}),
    expiresInSeconds: z
      .number()
      .int()
      .min(60)
      .max(ONBOARDING_HANDOFF_TTL_SECONDS)
      .default(DEFAULT_HANDOFF_TTL_SECONDS),
  })
  .superRefine((input, context) => {
    if (input.purpose !== "onboarding" && input.expiresInSeconds > MAX_STANDARD_HANDOFF_TTL_SECONDS) {
      context.addIssue({
        code: "too_big",
        maximum: MAX_STANDARD_HANDOFF_TTL_SECONDS,
        origin: "number",
        inclusive: true,
        path: ["expiresInSeconds"],
        message: "Only onboarding handoffs may remain valid for longer than 15 minutes",
      });
    }
  });
export type CreateHandoffInput = z.infer<typeof CreateHandoffInputSchema>;

export const HouseholdInvitationStepUpContextSchema = z
  .discriminatedUnion("action", [
    z.strictObject({
      action: z.literal("invite"),
      householdId: z.string().uuid(),
      conversationId: z.string().uuid(),
      expectedParticipantEpochId: z.string().uuid(),
      expectedParticipantDigest: z.string().regex(/^[a-f0-9]{64}$/u),
      inviteeIdentityId: z.string().uuid(),
      inviteePersonId: z.string().uuid(),
      proposedDisplayName: z.string().trim().min(1).max(80),
      role: z.literal("steward"),
      onboardingAdultIntentId: z
        .union([z.string().uuid(), z.literal("")])
        .nullable()
        .default("")
        .transform((value) => value ?? ""),
      onboardingAdultIntentVersion: z
        .union([z.string().regex(/^[1-9][0-9]*$/u), z.literal("")])
        .nullable()
        .default("")
        .transform((value) => value ?? ""),
    }),
    z.strictObject({
      action: z.enum(["approve", "accept"]),
      householdId: z.string().uuid(),
      invitationId: z.string().uuid(),
    }),
  ])
  .superRefine((value, context) => {
    if (
      value.action === "invite" &&
      Boolean(value.onboardingAdultIntentId) !== Boolean(value.onboardingAdultIntentVersion)
    ) {
      context.addIssue({
        code: "custom",
        message: "An onboarding adult invitation requires one exact intent version",
        path: ["onboardingAdultIntentVersion"],
      });
    }
  });
export type HouseholdInvitationStepUpContext = z.infer<typeof HouseholdInvitationStepUpContextSchema>;

export interface CreatedHandoff {
  readonly handoffId: string;
  readonly token: string;
  readonly expiresAt: Date;
}

export interface HandoffPreview {
  readonly handoffId: string;
  readonly purpose: HandoffPurpose;
  readonly expiresAt: Date;
}

export interface AuthenticatedSession {
  readonly sessionId: string;
  readonly personId: string;
  readonly authenticationIdentityId: string;
  readonly sessionToken: string;
  readonly csrfToken: string;
  readonly idleExpiresAt: Date;
  readonly absoluteExpiresAt: Date;
  readonly assuranceKind: AssuranceKind;
  readonly assuranceContext: Record<string, string>;
  readonly assuranceExpiresAt: Date | null;
}

export interface SessionPrincipal {
  readonly sessionId: string;
  readonly personId: string;
  readonly authenticationIdentityId: string;
  readonly controlEpoch: number;
  readonly csrfToken: string;
  readonly createdAt: Date;
  readonly lastSeenAt: Date;
  readonly idleExpiresAt: Date;
  readonly absoluteExpiresAt: Date;
  readonly assuranceKind: AssuranceKind;
  readonly assuranceContext: Record<string, string>;
  readonly assuranceExpiresAt: Date | null;
}

export type AssuranceKind =
  | "base"
  | "onboarding"
  | "account_controls"
  | "household_invitation"
  | "private_bridge_standing";

export const GOOGLE_IDENTITY_LINK_ACTION = "link_google_identity";
export const GOOGLE_IDENTITY_LINK_RETURN_PATH = "/link-account";
export const GOOGLE_IDENTITY_REVOKE_ACTION = "revoke_login_identity";
export const GOOGLE_IDENTITY_REVOKE_RETURN_PATH = "/safety";

export const GoogleIdentityLinkAssuranceContextSchema = z.strictObject({
  action: z.literal(GOOGLE_IDENTITY_LINK_ACTION),
  returnPath: z.literal(GOOGLE_IDENTITY_LINK_RETURN_PATH),
});

export const GoogleIdentityRevokeAssuranceContextSchema = z.strictObject({
  action: z.literal(GOOGLE_IDENTITY_REVOKE_ACTION),
  identityId: z.string().uuid(),
  returnPath: z.literal(GOOGLE_IDENTITY_REVOKE_RETURN_PATH),
});

export function hasFreshGoogleIdentityLinkAssurance(input: {
  readonly hasVerifiedGoogleIdentity: boolean;
  readonly assuranceKind: AssuranceKind;
  readonly assuranceContext: unknown;
  readonly assuranceExpiresAt: Date | null;
  readonly asOf: Date;
}): boolean {
  if (
    input.assuranceExpiresAt === null ||
    Number.isNaN(input.assuranceExpiresAt.getTime()) ||
    input.assuranceExpiresAt <= input.asOf
  ) {
    return false;
  }
  if (!input.hasVerifiedGoogleIdentity) return input.assuranceKind === "onboarding";
  return (
    input.assuranceKind === "account_controls" &&
    GoogleIdentityLinkAssuranceContextSchema.safeParse(input.assuranceContext).success
  );
}

export const GoogleAuthModeSchema = z.enum(["login", "link"]);
export type GoogleAuthMode = z.infer<typeof GoogleAuthModeSchema>;

const GoogleAuthReturnPathSchema = z
  .string()
  .min(1)
  .max(1_000)
  .refine(
    (value) =>
      value.startsWith("/") &&
      !value.startsWith("//") &&
      ![...value].some((character) => character.charCodeAt(0) < 32),
    "Return path must be a local path without control characters",
  );

export const BeginGoogleAuthAttemptInputSchema = z.discriminatedUnion("mode", [
  z.strictObject({
    mode: z.literal("login"),
    returnPath: GoogleAuthReturnPathSchema,
  }),
  z.strictObject({
    mode: z.literal("link"),
    personId: z.string().uuid(),
    initiatingSessionId: z.string().uuid(),
    personControlEpoch: z.number().int().positive(),
    returnPath: GoogleAuthReturnPathSchema,
  }),
]);
export type BeginGoogleAuthAttemptInput = z.infer<typeof BeginGoogleAuthAttemptInputSchema>;

export interface CreatedGoogleAuthAttempt {
  readonly attemptId: string;
  readonly mode: GoogleAuthMode;
  readonly state: string;
  readonly browserBinding: string;
  readonly pkceChallenge: string;
  readonly nonce: string;
  readonly expiresAt: Date;
}

interface GoogleAuthAttemptAccessBase {
  readonly attemptId: string;
  readonly provider: "google";
  readonly pkceVerifier: string;
  readonly nonce: string;
  readonly returnPath: string;
  readonly expiresAt: Date;
}

export type GoogleAuthAttemptAccess =
  | (GoogleAuthAttemptAccessBase & {
      readonly mode: "login";
      readonly personId: null;
      readonly initiatingSessionId: null;
      readonly personControlEpoch: null;
    })
  | (GoogleAuthAttemptAccessBase & {
      readonly mode: "link";
      readonly personId: string;
      readonly initiatingSessionId: string;
      readonly personControlEpoch: number;
    });

export const CompleteGoogleLoginInputSchema = z.strictObject({
  state: z.string().regex(/^[A-Za-z0-9_-]{32,128}$/u),
  browserBinding: z.string().regex(/^[A-Za-z0-9_-]{32,128}$/u),
  externalSubjectDigest: z.string().regex(/^[a-f0-9]{64}$/u),
});
export type CompleteGoogleLoginInput = z.infer<typeof CompleteGoogleLoginInputSchema>;

export type GoogleLoginCompletion =
  | {
      readonly kind: "signed_in";
      readonly identityId: string;
      readonly returnPath: string;
      readonly session: AuthenticatedSession;
    }
  | {
      readonly kind: "not_linked";
      readonly returnPath: string;
    };
