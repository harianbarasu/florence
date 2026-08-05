import { z } from "zod";

export const LINQ_V3_BASE_URL = "https://api.linqapp.com/api/partner/v3";
export const LINQ_WEBHOOK_VERSION = "2026-02-03";

export const linqConfigSchema = z
  .object({
    apiKey: z.string().min(1),
    webhookSecret: z.string().regex(/^whsec_[A-Za-z0-9+/]+={0,2}$/),
    apiBaseUrl: z
      .string()
      .url()
      .refine((value) => new URL(value).protocol === "https:", "Linq API URL must use HTTPS")
      .default(LINQ_V3_BASE_URL),
    webhookVersion: z.literal(LINQ_WEBHOOK_VERSION).default(LINQ_WEBHOOK_VERSION),
    webhookToleranceMs: z
      .number()
      .int()
      .positive()
      .max(15 * 60_000)
      .default(5 * 60_000),
    requestTimeoutMs: z.number().int().positive().max(60_000).default(20_000),
  })
  .strict()
  .transform((config) => ({
    ...config,
    apiBaseUrl: config.apiBaseUrl.replace(/\/+$/, ""),
  }));

export type LinqConfig = z.output<typeof linqConfigSchema>;

export function parseLinqConfig(input: z.input<typeof linqConfigSchema>): LinqConfig {
  return linqConfigSchema.parse(input);
}

export function linqConfigFromEnv(env: Readonly<Record<string, string | undefined>>): LinqConfig {
  return parseLinqConfig({
    apiKey: requiredEnv(env, "LINQ_API_KEY"),
    webhookSecret: requiredEnv(env, "LINQ_WEBHOOK_SECRET"),
    ...(env.LINQ_BASE_URL ? { apiBaseUrl: env.LINQ_BASE_URL } : {}),
    ...(env.LINQ_WEBHOOK_TOLERANCE_MS ? { webhookToleranceMs: Number(env.LINQ_WEBHOOK_TOLERANCE_MS) } : {}),
    ...(env.LINQ_REQUEST_TIMEOUT_MS ? { requestTimeoutMs: Number(env.LINQ_REQUEST_TIMEOUT_MS) } : {}),
  });
}

function requiredEnv(env: Readonly<Record<string, string | undefined>>, name: string): string {
  const value = env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}
