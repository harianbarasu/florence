import { z } from "zod";

export const DEFAULT_HANDOFF_TTL_SECONDS = 10 * 60;
export const MAX_STANDARD_HANDOFF_TTL_SECONDS = 15 * 60;
export const GOOGLE_CONNECT_HANDOFF_TTL_SECONDS = 30 * 60;
export const ONBOARDING_HANDOFF_TTL_SECONDS = 24 * 60 * 60;

export const HandoffPurposeSchema = z.enum([
  "web_sign_in",
  "onboarding",
  "google_connect",
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
    if (input.purpose === "google_connect" && input.expiresInSeconds > GOOGLE_CONNECT_HANDOFF_TTL_SECONDS) {
      context.addIssue({
        code: "too_big",
        maximum: GOOGLE_CONNECT_HANDOFF_TTL_SECONDS,
        origin: "number",
        inclusive: true,
        path: ["expiresInSeconds"],
        message: "Google connection handoffs expire within 30 minutes",
      });
    }
    if (
      input.purpose !== "google_connect" &&
      input.purpose !== "onboarding" &&
      input.expiresInSeconds > MAX_STANDARD_HANDOFF_TTL_SECONDS
    ) {
      context.addIssue({
        code: "too_big",
        maximum: MAX_STANDARD_HANDOFF_TTL_SECONDS,
        origin: "number",
        inclusive: true,
        path: ["expiresInSeconds"],
        message: "Only onboarding and Google connection handoffs may remain valid for longer than 15 minutes",
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
  | "google_connect"
  | "account_controls"
  | "household_invitation"
  | "private_bridge_standing";
