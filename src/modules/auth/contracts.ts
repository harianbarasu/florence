import { z } from "zod";

export const HandoffPurposeSchema = z.enum([
  "web_sign_in",
  "google_connect",
  "account_controls",
  "invitation",
  "private_review",
]);
export type HandoffPurpose = z.infer<typeof HandoffPurposeSchema>;

export const CreateHandoffInputSchema = z.strictObject({
  personId: z.string().uuid(),
  privateIdentityId: z.string().uuid(),
  privateConversationId: z.string().uuid().nullable(),
  purpose: HandoffPurposeSchema,
  context: z.record(z.string(), z.unknown()).default({}),
  expiresInSeconds: z
    .number()
    .int()
    .min(60)
    .max(15 * 60)
    .default(10 * 60),
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
}

export interface AuthenticatedSession {
  readonly sessionId: string;
  readonly personId: string;
  readonly sessionToken: string;
  readonly csrfToken: string;
  readonly idleExpiresAt: Date;
  readonly absoluteExpiresAt: Date;
  readonly assuranceKind: "base" | "google_connect" | "account_controls";
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
  readonly assuranceKind: "base" | "google_connect" | "account_controls";
  readonly assuranceExpiresAt: Date | null;
}
