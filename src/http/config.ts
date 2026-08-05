import { z } from "zod";
import { LINQ_WEBHOOK_VERSION, type LinqConfig } from "../adapters/linq/index.js";

export const DEFAULT_HTTP_BODY_LIMIT_BYTES = 256 * 1_024;
export const DEFAULT_LINQ_BODY_LIMIT_BYTES = 512 * 1_024;
export const DEFAULT_GMAIL_PUSH_BODY_LIMIT_BYTES = 64 * 1_024;
export const DEFAULT_CALENDAR_PUSH_BODY_LIMIT_BYTES = 4 * 1_024;

const linqWebhookConfigSchema = z
  .object({
    fromPhone: z.string().regex(/^\+[1-9]\d{1,14}$/u),
    webhookSecret: z.string().min(1),
    webhookToleranceMs: z
      .number()
      .int()
      .positive()
      .max(60 * 60 * 1_000),
    webhookVersion: z.literal(LINQ_WEBHOOK_VERSION),
  })
  .strict();

const gmailPubSubAuthenticationSchema = z
  .object({
    verificationToken: z.string().min(1),
    oidcAudience: z.string().url(),
    serviceAccountEmail: z.email().transform((value) => value.toLowerCase()),
  })
  .strict();

export const florenceHttpConfigSchema = z
  .object({
    publicUrl: z.string().url(),
    operatorToken: z.string().min(24),
    gmailPubSubAuthentication: gmailPubSubAuthenticationSchema.nullable(),
    googleCalendarPushEnabled: z.boolean().default(false),
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
  LINQ_FROM_PHONE?: string;
  LINQ_WEBHOOK_SECRET?: string;
  GOOGLE_PUBSUB_VERIFICATION_TOKEN?: string;
  GOOGLE_PUBSUB_OIDC_AUDIENCE?: string;
  GOOGLE_PUBSUB_SERVICE_ACCOUNT_EMAIL?: string;
  GOOGLE_CALENDAR_PUSH_ENABLED?: boolean;
}

export function parseFlorenceHttpConfig(input: FlorenceHttpConfigInput): FlorenceHttpConfig {
  return florenceHttpConfigSchema.parse(input);
}

export function httpConfigFromFlorenceConfig(config: FlorenceHttpEnvironmentConfig): FlorenceHttpConfig {
  const linqWebhook:
    | (Pick<LinqConfig, "webhookSecret" | "webhookToleranceMs" | "webhookVersion"> & {
        fromPhone: string;
      })
    | null =
    config.LINQ_WEBHOOK_SECRET && config.LINQ_FROM_PHONE
      ? {
          fromPhone: config.LINQ_FROM_PHONE,
          webhookSecret: config.LINQ_WEBHOOK_SECRET,
          webhookToleranceMs: 5 * 60 * 1_000,
          webhookVersion: LINQ_WEBHOOK_VERSION,
        }
      : null;
  const gmailPubSubAuthentication =
    config.GOOGLE_PUBSUB_VERIFICATION_TOKEN &&
    config.GOOGLE_PUBSUB_OIDC_AUDIENCE &&
    config.GOOGLE_PUBSUB_SERVICE_ACCOUNT_EMAIL
      ? {
          verificationToken: config.GOOGLE_PUBSUB_VERIFICATION_TOKEN,
          oidcAudience: config.GOOGLE_PUBSUB_OIDC_AUDIENCE,
          serviceAccountEmail: config.GOOGLE_PUBSUB_SERVICE_ACCOUNT_EMAIL,
        }
      : null;

  return parseFlorenceHttpConfig({
    publicUrl: config.FLORENCE_WEB_BASE_URL,
    operatorToken: config.FLORENCE_ADMIN_API_KEY,
    gmailPubSubAuthentication,
    googleCalendarPushEnabled: config.GOOGLE_CALENDAR_PUSH_ENABLED ?? false,
    linqWebhook,
  });
}
