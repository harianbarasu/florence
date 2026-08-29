import { createHash } from "node:crypto";

/** Stable, provider-neutral ID exposed to the model for one authorized Gmail file. */
export function gmailFileAssetId(sourceId: string, attachmentRef: string): string {
  return deterministicUuid(`family-work-gmail-file\0${sourceId}\0${attachmentRef}`);
}

export function vaultFileArtifactId(factId: string, sourceAssetId: string): string {
  return deterministicUuid(`family-work-vault-file\0${factId}\0${sourceAssetId}`);
}

function deterministicUuid(value: string): string {
  const digest = createHash("sha256").update(value, "utf8").digest("hex");
  const variant = ((Number.parseInt(digest[16] ?? "0", 16) & 3) | 8).toString(16);
  return `${digest.slice(0, 8)}-${digest.slice(8, 12)}-5${digest.slice(13, 16)}-${variant}${digest.slice(17, 20)}-${digest.slice(20, 32)}`;
}
