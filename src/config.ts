import { z } from "zod";

const emptyStringAsUndefined = (value: unknown): unknown =>
  typeof value === "string" && value.trim() === "" ? undefined : value;
const optionalSecret = z.preprocess(emptyStringAsUndefined, z.string().min(1).optional());
const optionalUrl = z.preprocess(emptyStringAsUndefined, z.string().url().optional());
const optionalE164Phone = z.preprocess(
  emptyStringAsUndefined,
  z
    .string()
    .regex(/^\+[1-9]\d{1,14}$/u)
    .optional(),
);
const encryptionKeyIdSchema = z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$/u);
const encryptionKeyringSchema = z.preprocess(
  (value) => {
    if (typeof value !== "string") return value;
    try {
      return JSON.parse(value) as unknown;
    } catch {
      return value;
    }
  },
  z.record(encryptionKeyIdSchema, z.string().min(43)).refine((keys) => Object.keys(keys).length > 0),
);

const environmentSchema = z
  .object({
    NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
    FLORENCE_PROCESS_ROLE: z.enum(["all", "web", "worker"]).default("all"),
    PORT: z.coerce.number().int().min(1).max(65_535).default(3000),
    LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"]).default("info"),
    FLORENCE_DATABASE_URL: z.string().url(),
    FLORENCE_POSTGRES_SCHEMA: z
      .string()
      .regex(/^[a-z][a-z0-9_]{0,62}$/u)
      .default("florence"),
    FLORENCE_WEB_BASE_URL: z.string().url(),
    FLORENCE_TOKEN_ENCRYPTION_KEY: z.string().min(43),
    FLORENCE_DATA_ACTIVE_KEY_ID: encryptionKeyIdSchema,
    FLORENCE_DATA_KEYRING_JSON: encryptionKeyringSchema,
    FLORENCE_BLIND_INDEX_KEY: z.string().min(43),
    FLORENCE_ADMIN_API_KEY: z.string().min(24),
    FLORENCE_DEFAULT_TIMEZONE: z.string().min(1).default("America/Los_Angeles"),
    LINQ_API_KEY: optionalSecret,
    LINQ_BASE_URL: z.string().url().default("https://api.linqapp.com/api/partner/v3"),
    LINQ_FROM_PHONE: optionalE164Phone,
    LINQ_WEBHOOK_SECRET: optionalSecret,
    GOOGLE_CLIENT_ID: optionalSecret,
    GOOGLE_CLIENT_SECRET: optionalSecret,
    GOOGLE_OAUTH_STATE_SECRET: optionalSecret,
    GOOGLE_REDIRECT_URI: optionalUrl,
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
  })
  .superRefine((value, context) => {
    if (!Object.hasOwn(value.FLORENCE_DATA_KEYRING_JSON, value.FLORENCE_DATA_ACTIVE_KEY_ID)) {
      context.addIssue({
        code: "custom",
        path: ["FLORENCE_DATA_ACTIVE_KEY_ID"],
        message: "Active Florence data-encryption key is absent from the keyring",
      });
    }
    if (value.NODE_ENV !== "production") return;

    const requireFields = <Key extends keyof typeof value>(fields: readonly Key[]): void => {
      for (const field of fields) {
        if (value[field] !== undefined) continue;
        context.addIssue({
          code: "custom",
          path: [field],
          message: `${field} is required for the configured production integration`,
        });
      }
    };
    const linqFields = ["LINQ_API_KEY", "LINQ_FROM_PHONE", "LINQ_WEBHOOK_SECRET"] as const;
    requireFields(linqFields);

    const googleOAuthFields = [
      "GOOGLE_CLIENT_ID",
      "GOOGLE_CLIENT_SECRET",
      "GOOGLE_OAUTH_STATE_SECRET",
      "GOOGLE_REDIRECT_URI",
    ] as const;
    requireFields(googleOAuthFields);

    switch (value.MODEL_PROVIDER) {
      case "openai":
        requireFields(["OPENAI_API_KEY"]);
        break;
      case "anthropic":
        requireFields(["ANTHROPIC_API_KEY"]);
        break;
      case "open-weight":
        requireFields(["OPEN_WEIGHT_BASE_URL", "OPEN_WEIGHT_MODEL"]);
        break;
    }
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
  googleOAuth: boolean;
  gmail: boolean;
  googleCalendar: boolean;
  openai: boolean;
  anthropic: boolean;
  openWeight: boolean;
} {
  const googleOAuth = Boolean(
    config.GOOGLE_CLIENT_ID &&
      config.GOOGLE_CLIENT_SECRET &&
      config.GOOGLE_OAUTH_STATE_SECRET &&
      config.GOOGLE_REDIRECT_URI,
  );
  return {
    linq: Boolean(config.LINQ_API_KEY && config.LINQ_FROM_PHONE && config.LINQ_WEBHOOK_SECRET),
    googleOAuth,
    gmail: googleOAuth,
    googleCalendar: googleOAuth,
    openai: Boolean(config.OPENAI_API_KEY),
    anthropic: Boolean(config.ANTHROPIC_API_KEY),
    openWeight: Boolean(config.OPEN_WEIGHT_BASE_URL && config.OPEN_WEIGHT_MODEL),
  };
}
