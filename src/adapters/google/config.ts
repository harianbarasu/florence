import { z } from "zod";

export const GOOGLE_GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly";
export const GOOGLE_CALENDAR_READONLY_SCOPE = "https://www.googleapis.com/auth/calendar.readonly";
export const GOOGLE_CALENDAR_EVENTS_SCOPE = "https://www.googleapis.com/auth/calendar.events";

export const DEFAULT_GOOGLE_SCOPES = [
  "openid",
  "email",
  GOOGLE_GMAIL_READONLY_SCOPE,
  GOOGLE_CALENDAR_READONLY_SCOPE,
  GOOGLE_CALENDAR_EVENTS_SCOPE,
] as const;

export const googleScopeSchema = z.enum(DEFAULT_GOOGLE_SCOPES);

export const googleAdapterConfigSchema = z
  .object({
    clientId: z.string().min(1),
    clientSecret: z.string().min(1),
    redirectUri: z.string().url(),
    scopes: z
      .array(googleScopeSchema)
      .min(1)
      .refine(
        (scopes) => scopes.includes("openid") && scopes.includes("email"),
        "Google OAuth scopes must include openid and email",
      )
      .default([...DEFAULT_GOOGLE_SCOPES]),
  })
  .strict()
  .transform((config) => ({
    ...config,
    scopes: [...new Set(config.scopes)],
  }));

export type GoogleAdapterConfig = z.output<typeof googleAdapterConfigSchema>;

export function parseGoogleAdapterConfig(
  input: z.input<typeof googleAdapterConfigSchema>,
): GoogleAdapterConfig {
  return googleAdapterConfigSchema.parse(input);
}

export function googleAdapterConfigFromEnv(
  env: Readonly<Record<string, string | undefined>>,
): GoogleAdapterConfig {
  const scopes = env.GOOGLE_OAUTH_SCOPES?.split(/[\s,]+/)
    .map((scope) => scope.trim())
    .filter(Boolean);
  return googleAdapterConfigSchema.parse({
    clientId: requiredEnv(env, "GOOGLE_CLIENT_ID"),
    clientSecret: requiredEnv(env, "GOOGLE_CLIENT_SECRET"),
    redirectUri: requiredEnv(env, "GOOGLE_REDIRECT_URI"),
    ...(scopes?.length ? { scopes } : {}),
  });
}

function requiredEnv(env: Readonly<Record<string, string | undefined>>, name: string): string {
  const value = env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}
