import { z } from "zod";

const optionalString = z.preprocess(
  (value) => (typeof value === "string" && value.trim() === "" ? undefined : value),
  z.string().trim().min(1).optional(),
);

const optionalUrl = z.preprocess(
  (value) => (typeof value === "string" && value.trim() === "" ? undefined : value),
  z.string().trim().url().optional(),
);

const environmentSchema = z
  .object({
    NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
    PORT: z.coerce.number().int().min(1).max(65_535).default(3000),
    LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"]).default("info"),
    FLORENCE_DATABASE_URL: z.string().trim().min(1),
    FLORENCE_POSTGRES_SCHEMA: z
      .string()
      .regex(/^[a-z][a-z0-9_]{0,62}$/u)
      .default("florence_v4"),
    FLORENCE_WEB_BASE_URL: z.string().trim().url(),
    FLORENCE_TOKEN_ENCRYPTION_KEY: z.string().min(32).max(4_096),
    FLORENCE_DATA_ACTIVE_KEY_ID: z.string().regex(/^[A-Za-z0-9._-]{1,64}$/u),
    FLORENCE_DATA_KEYRING_JSON: z.string().min(2).max(32_768),
    FLORENCE_DEFAULT_TIMEZONE: z.string().trim().min(1).default("America/Los_Angeles"),
    LINQ_API_KEY: z.string().trim().min(1),
    LINQ_BASE_URL: z.string().trim().url().default("https://api.linqapp.com/api/partner/v3"),
    LINQ_FROM_PHONE: z
      .string()
      .trim()
      .regex(/^\+[1-9]\d{6,14}$/u),
    LINQ_WEBHOOK_SECRET: z
      .string()
      .trim()
      .regex(/^whsec_[A-Za-z0-9+/]+={0,2}$/u)
      .refine((value) => Buffer.from(value.slice("whsec_".length), "base64").length >= 16, {
        message: "LINQ_WEBHOOK_SECRET signing key is too short",
      }),
    GOOGLE_CLIENT_ID: z.string().trim().min(1),
    GOOGLE_CLIENT_SECRET: z.string().trim().min(1),
    MODEL_PROVIDER: z.enum(["openai", "anthropic", "open_weight"]).default("openai"),
    OPENAI_API_KEY: optionalString,
    OPENAI_BASE_URL: z.string().trim().url().default("https://api.openai.com/v1"),
    OPENAI_MODEL: z.string().trim().min(1).default("gpt-5-mini"),
    ANTHROPIC_API_KEY: optionalString,
    ANTHROPIC_MODEL: z.string().trim().min(1).default("claude-sonnet-4-5"),
    OPEN_WEIGHT_BASE_URL: optionalUrl,
    OPEN_WEIGHT_API_KEY: optionalString,
    OPEN_WEIGHT_MODEL: optionalString,
    WORKER_POLL_INTERVAL_MS: z.coerce.number().int().min(100).max(60_000).default(1_000),
    GMAIL_POLL_INTERVAL_MS: z.coerce.number().int().min(5_000).max(3_600_000).default(60_000),
    CALENDAR_POLL_INTERVAL_MS: z.coerce.number().int().min(5_000).max(3_600_000).default(120_000),
    RAW_SOURCE_RETENTION_DAYS: z.coerce.number().int().min(1).max(30).default(30),
    WORKER_SCRATCH_RETENTION_DAYS: z.coerce.number().int().min(1).max(7).default(7),
  })
  .superRefine((environment, context) => {
    const requiredByProvider = {
      openai: [["OPENAI_API_KEY", environment.OPENAI_API_KEY]],
      anthropic: [["ANTHROPIC_API_KEY", environment.ANTHROPIC_API_KEY]],
      open_weight: [
        ["OPEN_WEIGHT_BASE_URL", environment.OPEN_WEIGHT_BASE_URL],
        ["OPEN_WEIGHT_MODEL", environment.OPEN_WEIGHT_MODEL],
      ],
    } as const;

    for (const [name, value] of requiredByProvider[environment.MODEL_PROVIDER]) {
      if (!value) {
        context.addIssue({
          code: "custom",
          message: `${name} is required for model provider ${environment.MODEL_PROVIDER}`,
          path: [name],
        });
      }
    }
  });

export type FlorenceConfig = ReturnType<typeof loadConfig>;

export function loadConfig(environment: NodeJS.ProcessEnv = process.env) {
  const parsed = environmentSchema.parse(environment);
  const publicBaseUrl = parsePublicBaseUrl(parsed.FLORENCE_WEB_BASE_URL, parsed.NODE_ENV);
  const databaseUrl = parseDatabaseUrl(parsed.FLORENCE_DATABASE_URL);
  const linqBaseUrl = parseServiceBaseUrl(parsed.LINQ_BASE_URL, "LINQ_BASE_URL", true);
  const openAiBaseUrl = parseServiceBaseUrl(parsed.OPENAI_BASE_URL, "OPENAI_BASE_URL", false);
  const openWeightBaseUrl = parsed.OPEN_WEIGHT_BASE_URL
    ? parseServiceBaseUrl(parsed.OPEN_WEIGHT_BASE_URL, "OPEN_WEIGHT_BASE_URL", false)
    : undefined;
  validateDataKeyring(parsed.FLORENCE_DATA_KEYRING_JSON, parsed.FLORENCE_DATA_ACTIVE_KEY_ID);
  validateTimeZone(parsed.FLORENCE_DEFAULT_TIMEZONE);

  return {
    environment: parsed.NODE_ENV,
    port: parsed.PORT,
    logLevel: parsed.LOG_LEVEL,
    publicBaseUrl: publicBaseUrl.origin,
    database: {
      url: databaseUrl.toString(),
      schema: parsed.FLORENCE_POSTGRES_SCHEMA,
      ssl: parsed.NODE_ENV === "production",
    },
    security: {
      tokenKey: parsed.FLORENCE_TOKEN_ENCRYPTION_KEY,
      activeDataKeyId: parsed.FLORENCE_DATA_ACTIVE_KEY_ID,
      dataKeyringJson: parsed.FLORENCE_DATA_KEYRING_JSON,
    },
    linq: {
      apiKey: parsed.LINQ_API_KEY,
      baseUrl: trimTrailingSlash(linqBaseUrl.toString()),
      fromPhone: parsed.LINQ_FROM_PHONE,
      webhookSecret: parsed.LINQ_WEBHOOK_SECRET,
    },
    google: {
      clientId: parsed.GOOGLE_CLIENT_ID,
      clientSecret: parsed.GOOGLE_CLIENT_SECRET,
      redirectUri: new URL("/oauth/google/callback", publicBaseUrl).toString(),
    },
    model: {
      provider: parsed.MODEL_PROVIDER,
      openai: {
        apiKey: parsed.OPENAI_API_KEY,
        baseUrl: trimTrailingSlash(openAiBaseUrl.toString()),
        model: parsed.OPENAI_MODEL,
      },
      anthropic: {
        apiKey: parsed.ANTHROPIC_API_KEY,
        model: parsed.ANTHROPIC_MODEL,
      },
      openWeight: {
        apiKey: parsed.OPEN_WEIGHT_API_KEY,
        baseUrl: openWeightBaseUrl ? trimTrailingSlash(openWeightBaseUrl.toString()) : undefined,
        model: parsed.OPEN_WEIGHT_MODEL,
      },
    },
    defaults: {
      timezone: parsed.FLORENCE_DEFAULT_TIMEZONE,
      rawSourceRetentionDays: parsed.RAW_SOURCE_RETENTION_DAYS,
      workerScratchRetentionDays: parsed.WORKER_SCRATCH_RETENTION_DAYS,
    },
    intervals: {
      workerPollMs: parsed.WORKER_POLL_INTERVAL_MS,
      gmailPollMs: parsed.GMAIL_POLL_INTERVAL_MS,
      calendarPollMs: parsed.CALENDAR_POLL_INTERVAL_MS,
    },
  } as const;
}

function parsePublicBaseUrl(raw: string, environment: "development" | "test" | "production"): URL {
  const url = new URL(raw);
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    url.pathname !== "/"
  ) {
    throw new Error("FLORENCE_WEB_BASE_URL must be a clean HTTP(S) origin URL");
  }
  if (environment === "production" && url.protocol !== "https:") {
    throw new Error("FLORENCE_WEB_BASE_URL must use HTTPS in production");
  }
  return url;
}

function parseDatabaseUrl(raw: string): URL {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new Error("FLORENCE_DATABASE_URL must be a valid PostgreSQL URL");
  }
  if (!["postgres:", "postgresql:"].includes(url.protocol) || !url.hostname || !url.pathname.slice(1)) {
    throw new Error("FLORENCE_DATABASE_URL must be a valid PostgreSQL URL");
  }
  return url;
}

function parseServiceBaseUrl(raw: string, name: string, requireHttps: boolean): URL {
  const url = new URL(raw);
  const allowedProtocols = requireHttps ? ["https:"] : ["http:", "https:"];
  if (
    !allowedProtocols.includes(url.protocol) ||
    !url.hostname ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  ) {
    throw new Error(`${name} must be a clean ${requireHttps ? "HTTPS" : "HTTP(S)"} service URL`);
  }
  return url;
}

function validateDataKeyring(raw: string, activeKeyId: string): void {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("FLORENCE_DATA_KEYRING_JSON must be valid JSON");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("FLORENCE_DATA_KEYRING_JSON must be an object of data keys");
  }

  const entries = Object.entries(parsed);
  if (entries.length === 0) throw new Error("FLORENCE_DATA_KEYRING_JSON must contain at least one data key");
  for (const [keyId, encoded] of entries) {
    if (!/^[A-Za-z0-9._-]{1,64}$/u.test(keyId) || typeof encoded !== "string") {
      throw new Error("FLORENCE_DATA_KEYRING_JSON contains an invalid key entry");
    }
    const decoded = Buffer.from(encoded, "base64");
    const canonicalEncoding =
      decoded.toString("base64") === encoded || decoded.toString("base64url") === encoded;
    if (decoded.length !== 32 || !canonicalEncoding) {
      throw new Error(`Data key ${keyId} must be canonical base64 or base64url encoding of exactly 32 bytes`);
    }
  }
  if (!Object.hasOwn(parsed, activeKeyId)) {
    throw new Error(`Active data key ${activeKeyId} is missing from FLORENCE_DATA_KEYRING_JSON`);
  }
}

function validateTimeZone(timeZone: string): void {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone }).format(0);
  } catch {
    throw new Error("FLORENCE_DEFAULT_TIMEZONE must be a valid IANA time zone");
  }
}

function trimTrailingSlash(value: string): string {
  return value.replace(/\/$/u, "");
}
