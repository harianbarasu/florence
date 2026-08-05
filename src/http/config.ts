import { z } from "zod";
import { LINQ_WEBHOOK_VERSION, type LinqConfig } from "../adapters/linq/index.js";

export const DEFAULT_HTTP_BODY_LIMIT_BYTES = 256 * 1_024;
export const DEFAULT_LINQ_BODY_LIMIT_BYTES = 512 * 1_024;
export const DEFAULT_GMAIL_PUSH_BODY_LIMIT_BYTES = 64 * 1_024;
export const DEFAULT_OPERATOR_BODY_LIMIT_BYTES = 16 * 1_024;

const linqWebhookConfigSchema = z
  .object({
    webhookSecret: z.string().min(1),
    webhookToleranceMs: z
      .number()
      .int()
      .positive()
      .max(60 * 60 * 1_000),
    webhookVersion: z.literal(LINQ_WEBHOOK_VERSION),
  })
  .strict();

export const florenceHttpConfigSchema = z
  .object({
    publicUrl: z.string().url(),
    operatorToken: z.string().min(24),
    gmailPubSubVerificationToken: z.string().min(1).nullable(),
    linqWebhook: linqWebhookConfigSchema.nullable(),
    trustProxy: z.boolean().default(true),
    bodyLimitBytes: z
      .number()
      .int()
      .min(1_024)
      .max(2 * 1_024 * 1_024)
      .default(DEFAULT_HTTP_BODY_LIMIT_BYTES),
    rateLimit: z
      .object({
        max: z.number().int().positive().max(10_000).default(120),
        timeWindowMs: z
          .number()
          .int()
          .min(1_000)
          .max(60 * 60 * 1_000)
          .default(60_000),
      })
      .strict()
      .default({ max: 120, timeWindowMs: 60_000 }),
  })
  .strict();

export type FlorenceHttpConfig = z.output<typeof florenceHttpConfigSchema>;
export type FlorenceHttpConfigInput = z.input<typeof florenceHttpConfigSchema>;

export interface FlorenceHttpEnvironmentConfig {
  FLORENCE_WEB_BASE_URL: string;
  FLORENCE_ADMIN_API_KEY: string;
  LINQ_WEBHOOK_SECRET?: string;
  GOOGLE_PUBSUB_VERIFICATION_TOKEN?: string;
}

export function parseFlorenceHttpConfig(input: FlorenceHttpConfigInput): FlorenceHttpConfig {
  return florenceHttpConfigSchema.parse(input);
}

export function httpConfigFromFlorenceConfig(config: FlorenceHttpEnvironmentConfig): FlorenceHttpConfig {
  const linqWebhook: Pick<LinqConfig, "webhookSecret" | "webhookToleranceMs" | "webhookVersion"> | null =
    config.LINQ_WEBHOOK_SECRET
      ? {
          webhookSecret: config.LINQ_WEBHOOK_SECRET,
          webhookToleranceMs: 5 * 60 * 1_000,
          webhookVersion: LINQ_WEBHOOK_VERSION,
        }
      : null;

  return parseFlorenceHttpConfig({
    publicUrl: config.FLORENCE_WEB_BASE_URL,
    operatorToken: config.FLORENCE_ADMIN_API_KEY,
    gmailPubSubVerificationToken: config.GOOGLE_PUBSUB_VERIFICATION_TOKEN ?? null,
    linqWebhook,
  });
}
