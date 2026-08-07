import { z } from "zod";

export const DEFAULT_HANDOFF_TTL_SECONDS = 10 * 60;
export const MAX_STANDARD_HANDOFF_TTL_SECONDS = 15 * 60;
export const GOOGLE_CONNECT_HANDOFF_TTL_SECONDS = 30 * 60;

export const HandoffPurposeSchema = z.enum([
  "web_sign_in",
  "google_connect",
  "account_controls",
  "household_invitation",
  "group_coverage",
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
      .max(GOOGLE_CONNECT_HANDOFF_TTL_SECONDS)
      .default(DEFAULT_HANDOFF_TTL_SECONDS),
  })
  .superRefine((input, context) => {
    if (input.purpose !== "google_connect" && input.expiresInSeconds > MAX_STANDARD_HANDOFF_TTL_SECONDS) {
      context.addIssue({
        code: "too_big",
        maximum: MAX_STANDARD_HANDOFF_TTL_SECONDS,
        origin: "number",
        inclusive: true,
        path: ["expiresInSeconds"],
        message: "Only Google connection handoffs may remain valid for longer than 15 minutes",
      });
    }
  });
export type CreateHandoffInput = z.infer<typeof CreateHandoffInputSchema>;

export interface CreatedHandoff {
  readonly handoffId: string;
  readonly token: string;
  readonly expiresAt: Date;
}

export interface HandoffPreview {
  readonly handoffId: string;
  readonly purpose: HandoffPurpose;
  readonly expiresAt: Date;
  readonly groupLabel?: string;
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
  | "google_connect"
  | "account_controls"
  | "household_invitation"
  | "group_coverage"
  | "private_bridge_standing";
