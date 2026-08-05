import { z } from "zod";
import type { EncryptionContext } from "./tenant-json-cipher.js";

const emailSchema = z.email().transform((value) => value.toLowerCase());

export const googleAccountAliasSchema = z
  .string()
  .transform((value) => value.normalize("NFKC").trim().replace(/\s+/gu, " "))
  .pipe(z.string().min(1).max(80))
  .refine((value) => !/[\p{Cc}\p{Cf}/]/u.test(value), {
    message: "Google account aliases cannot contain control characters or slashes",
  });

export const adultIdentityDetailsSchema = z.strictObject({
  displayName: z.string().trim().min(1).max(200),
});

export const googleConnectionDetailsSchema = z.strictObject({
  label: googleAccountAliasSchema,
  accountLabel: googleAccountAliasSchema,
  email: emailSchema.nullable(),
});

export type AdultIdentityDetails = z.infer<typeof adultIdentityDetailsSchema>;
export type GoogleConnectionDetails = z.infer<typeof googleConnectionDetailsSchema>;

export function normalizedEmail(value: string): string {
  return emailSchema.parse(value);
}

export function canonicalGoogleAccountAlias(value: string): string {
  return googleAccountAliasSchema.parse(value);
}

export function googleAccountAliasKey(value: string): string {
  const normalized = canonicalGoogleAccountAlias(value).toLocaleLowerCase("en-US");
  if (["google", "google account"].includes(normalized)) return "google";
  if (["work", "work google", "work google account", "google work"].includes(normalized)) {
    return "work";
  }
  if (["personal", "personal google", "personal google account", "google personal"].includes(normalized)) {
    return "personal";
  }
  return normalized;
}

export function adultIdentityDetailsContext(input: {
  householdId: string;
  adultId: string;
}): EncryptionContext {
  return {
    tenant: { kind: "household", id: input.householdId },
    table: "adult_identity_details",
    rowId: input.adultId,
    field: "details",
  };
}

export function googleConnectionDetailsContext(input: {
  householdId: string;
  adultId: string;
  connectionId: string;
}): EncryptionContext {
  return {
    tenant: { kind: "household", id: input.householdId },
    table: "external_connections",
    rowId: input.connectionId,
    field: "details",
  };
}

export const REVOKED_GOOGLE_ACCOUNT_LABEL = "Revoked Google account";
