import { z } from "zod";
import { LinqConfigurationError } from "./errors.js";

const DEFAULT_BASE_URL = "https://api.linqapp.com/api/partner/v3";

const envSchema = z.object({
  LINQ_API_KEY: z.string().trim().min(1),
  LINQ_BASE_URL: z.string().trim().url().default(DEFAULT_BASE_URL),
  LINQ_PHONE_NUMBER: z
    .string()
    .trim()
    .regex(/^\+[1-9]\d{6,14}$/),
  LINQ_WEBHOOK_SECRET: z
    .string()
    .trim()
    .regex(/^whsec_[A-Za-z0-9+/]+={0,2}$/),
  LINQ_REQUEST_TIMEOUT_MS: z.coerce.number().int().min(1_000).max(120_000).default(15_000),
  LINQ_MAX_ATTACHMENT_BYTES: z.coerce
    .number()
    .int()
    .min(1)
    .max(100 * 1024 * 1024)
    .default(20 * 1024 * 1024),
  LINQ_MAX_WEBHOOK_BYTES: z.coerce
    .number()
    .int()
    .min(1_024)
    .max(10 * 1024 * 1024)
    .default(1024 * 1024),
});

export interface LinqConfig {
  apiKey: string;
  baseUrl: string;
  phoneNumber: string;
  webhookSecret: string;
  requestTimeoutMs: number;
  maxAttachmentBytes: number;
  maxWebhookBytes: number;
}

export function loadLinqConfig(environment: NodeJS.ProcessEnv = process.env): LinqConfig {
  const result = envSchema.safeParse(environment);
  if (!result.success) {
    const names = [...new Set(result.error.issues.map((issue) => issue.path.join(".")))].join(", ");
    throw new LinqConfigurationError(`Invalid Linq configuration: ${names}`);
  }

  const baseUrl = new URL(result.data.LINQ_BASE_URL);
  if (baseUrl.protocol !== "https:" || baseUrl.search || baseUrl.hash) {
    throw new LinqConfigurationError("LINQ_BASE_URL must be an HTTPS URL without a query or fragment");
  }

  return {
    apiKey: result.data.LINQ_API_KEY,
    baseUrl: baseUrl.toString().replace(/\/$/, ""),
    phoneNumber: result.data.LINQ_PHONE_NUMBER,
    webhookSecret: result.data.LINQ_WEBHOOK_SECRET,
    requestTimeoutMs: result.data.LINQ_REQUEST_TIMEOUT_MS,
    maxAttachmentBytes: result.data.LINQ_MAX_ATTACHMENT_BYTES,
    maxWebhookBytes: result.data.LINQ_MAX_WEBHOOK_BYTES,
  };
}
