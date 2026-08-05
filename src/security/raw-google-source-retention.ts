const RAW_GOOGLE_SOURCE_RETENTION_MS = 30 * 24 * 60 * 60_000;

/** Google remains source of truth, so locally retained raw source ciphertext is short-lived. */
export function rawGoogleSourceRetentionUntil(requestedUntil?: string): string {
  const maximum = Date.now() + RAW_GOOGLE_SOURCE_RETENTION_MS;
  const requested = requestedUntil === undefined ? maximum : Date.parse(requestedUntil);
  if (!Number.isFinite(requested)) throw new Error("Raw Google source retention must be a finite instant");
  return new Date(Math.min(requested, maximum)).toISOString();
}

export function isRawGoogleSource(provider: string, encryptedContent: string | undefined): boolean {
  return encryptedContent !== undefined && (provider === "gmail" || provider === "google-calendar");
}
