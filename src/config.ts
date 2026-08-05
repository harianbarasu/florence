import { z } from "zod";

const emptyStringAsUndefined = (value: unknown): unknown =>
  typeof value === "string" && value.trim() === "" ? undefined : value;
const optionalSecret = z.preprocess(emptyStringAsUndefined, z.string().min(1).optional());
const optionalUrl = z.preprocess(emptyStringAsUndefined, z.string().url().optional());

const environmentSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  FLORENCE_PROCESS_ROLE: z.enum(["all", "web", "worker"]).default("all"),
  PORT: z.coerce.number().int().min(1).max(65_535).default(3000),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"]).default("info"),
  FLORENCE_DATABASE_URL: z.string().url(),
  FLORENCE_DB_SCHEMA: z
    .string()
    .regex(/^[a-z][a-z0-9_]{0,62}$/u)
    .default("florence"),
  FLORENCE_WEB_BASE_URL: z.string().url(),
  FLORENCE_TOKEN_ENCRYPTION_KEY: z.string().min(43),
  FLORENCE_ADMIN_API_KEY: z.string().min(24),
  FLORENCE_DEFAULT_TIMEZONE: z.string().min(1).default("America/Los_Angeles"),
  LINQ_API_KEY: optionalSecret,
  LINQ_BASE_URL: z.string().url().default("https://api.linqapp.com/api/partner/v3"),
  LINQ_FROM_PHONE: optionalSecret,
  LINQ_WEBHOOK_SECRET: optionalSecret,
  GOOGLE_CLIENT_ID: optionalSecret,
  GOOGLE_CLIENT_SECRET: optionalSecret,
  GOOGLE_OAUTH_STATE_SECRET: optionalSecret,
  GOOGLE_REDIRECT_URI: optionalUrl,
  GOOGLE_PUBSUB_VERIFICATION_TOKEN: optionalSecret,
  GOOGLE_GMAIL_TOPIC_NAME: z.preprocess(
    emptyStringAsUndefined,
    z
      .string()
      .regex(/^projects\/[^/]+\/topics\/[^/]+$/u)
      .optional(),
  ),
  GOOGLE_GMAIL_PUBSUB_SUBSCRIPTION: z.preprocess(
    emptyStringAsUndefined,
    z
      .string()
      .max(1_000)
      .regex(/^projects\/[^/]+\/subscriptions\/[^/]+$/u)
      .optional(),
  ),
  MODEL_PROVIDER: z.enum(["openai", "anthropic", "open-weight"]).default("openai"),
  OPENAI_API_KEY: optionalSecret,
  OPENAI_BASE_URL: z.string().url().default("https://api.openai.com/v1"),
  OPENAI_MODEL: z.string().min(1).default("gpt-5.6-terra"),
  ANTHROPIC_API_KEY: optionalSecret,
  ANTHROPIC_MODEL: z.string().min(1).default("claude-sonnet-4-6"),
  OPEN_WEIGHT_BASE_URL: optionalUrl,
  OPEN_WEIGHT_API_KEY: optionalSecret,
  OPEN_WEIGHT_MODEL: optionalSecret,
  WORKER_POLL_INTERVAL_MS: z.coerce.number().int().min(100).max(60_000).default(1_000),
  WORKER_LEASE_SECONDS: z.coerce.number().int().min(10).max(3_600).default(60),
  DAILY_BRIEF_LOCAL_TIME: z
    .string()
    .regex(/^(?:[01]\d|2[0-3]):[0-5]\d$/u)
    .default("06:30"),
});

export type FlorenceConfig = z.infer<typeof environmentSchema>;

let cachedConfig: FlorenceConfig | undefined;

export function loadConfig(environment: NodeJS.ProcessEnv = process.env): FlorenceConfig {
  if (environment === process.env && cachedConfig) {
    return cachedConfig;
  }

  const parsed = environmentSchema.safeParse(environment);
  if (!parsed.success) {
    const fields = parsed.error.issues.map((issue) => issue.path.join(".") || "environment");
    throw new Error(`Invalid Florence configuration: ${[...new Set(fields)].join(", ")}`);
  }

  if (environment === process.env) {
    cachedConfig = parsed.data;
  }
  return parsed.data;
}

export function resetConfigForTests(): void {
  cachedConfig = undefined;
}

export function availableIntegrations(config: FlorenceConfig): {
  linq: boolean;
  google: boolean;
  openai: boolean;
  anthropic: boolean;
  openWeight: boolean;
} {
  return {
    linq: Boolean(config.LINQ_API_KEY && config.LINQ_FROM_PHONE && config.LINQ_WEBHOOK_SECRET),
    google: Boolean(
      config.GOOGLE_CLIENT_ID &&
        config.GOOGLE_CLIENT_SECRET &&
        config.GOOGLE_OAUTH_STATE_SECRET &&
        config.GOOGLE_REDIRECT_URI &&
        config.GOOGLE_PUBSUB_VERIFICATION_TOKEN &&
        config.GOOGLE_GMAIL_TOPIC_NAME &&
        config.GOOGLE_GMAIL_PUBSUB_SUBSCRIPTION,
    ),
    openai: Boolean(config.OPENAI_API_KEY),
    anthropic: Boolean(config.ANTHROPIC_API_KEY),
    openWeight: Boolean(config.OPEN_WEIGHT_BASE_URL && config.OPEN_WEIGHT_MODEL),
  };
}
