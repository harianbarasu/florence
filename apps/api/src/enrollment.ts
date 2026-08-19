import { createHmac, timingSafeEqual } from "node:crypto";

const FOUNDER_SETUP_LIFETIME_MS = 15 * 60_000;
const PARTNER_SETUP_LIFETIME_MS = 24 * 60 * 60_000;
const MAX_PROVIDER_ID_BYTES = 500;
const COMPACT_SIGNATURE_BYTES = 24;
const COMPACT_HEADER_BYTES = 6;
const IDENTITY_DIGEST_BYTES = 32;

export type FounderSetupClaims = {
  providerConversationId: string;
  identitySubjectDigest: string;
  expiresAt: string;
  householdId: string;
  adultId: string;
};

export type FounderSetupIssue = FounderSetupClaims & {
  token: string;
};

export type PartnerSetupClaims = {
  providerConversationId: string;
  identitySubjectDigest: string;
  expiresAt: string;
  householdId: string;
  adultId: string;
};

export type PartnerSetupIssue = PartnerSetupClaims & { token: string };

export class EnrollmentCodes {
  readonly #secret: string;

  constructor(secret: string) {
    if (Buffer.byteLength(secret, "utf8") < 32) {
      throw new Error("FLORENCE_ENROLLMENT_SECRET must contain at least 32 UTF-8 bytes");
    }
    this.#secret = secret;
  }

  issueFounderSetup(input: {
    providerConversationId: string;
    identitySubjectDigest: string;
    occurredAt: string;
  }): FounderSetupIssue {
    const providerConversationId = boundedProviderId(
      input.providerConversationId,
      "Provider conversation ID",
    );
    const identitySubjectDigest = digest(input.identitySubjectDigest, "Messages identity");
    const occurredAt = instant(input.occurredAt, "Founder setup issue time");
    const expiresAtSeconds = Math.floor((occurredAt.getTime() + FOUNDER_SETUP_LIFETIME_MS) / 1_000);
    const expiresAt = new Date(expiresAtSeconds * 1_000);
    const householdId = this.#deterministicUuid(`founder-household\0${identitySubjectDigest}`);
    const adultId = this.#deterministicUuid(`founder-adult\0${identitySubjectDigest}`);
    if (expiresAtSeconds < 0 || expiresAtSeconds > 0xffff_ffff) {
      throw new Error("Founder setup expiry is outside the compact token range");
    }
    const conversation = Buffer.from(providerConversationId, "utf8");
    const identity = Buffer.from(identitySubjectDigest, "hex");
    const payloadBytes = Buffer.alloc(COMPACT_HEADER_BYTES + conversation.length + identity.length);
    payloadBytes.writeUInt32BE(expiresAtSeconds, 0);
    payloadBytes.writeUInt16BE(conversation.length, 4);
    conversation.copy(payloadBytes, COMPACT_HEADER_BYTES);
    identity.copy(payloadBytes, COMPACT_HEADER_BYTES + conversation.length);
    const payload = payloadBytes.toString("base64url");
    const signature = this.#compactSignature(payloadBytes).toString("base64url");
    const token = `fs2.${payload}.${signature}`;
    return {
      token,
      providerConversationId,
      identitySubjectDigest,
      expiresAt: expiresAt.toISOString(),
      householdId,
      adultId,
    };
  }

  verifyFounderSetup(token: string, now = new Date()): FounderSetupClaims | null {
    if (!Number.isFinite(now.getTime())) return null;
    if (Buffer.byteLength(token, "utf8") > 4_000) return null;
    const [version, payload, signature, extra] = token.split(".");
    if (!payload || !signature || extra) return null;
    if (version !== "fs2") return null;
    return this.#verifyCompactFounderSetup(payload, signature, now);
  }

  #verifyCompactFounderSetup(payload: string, signature: string, now: Date): FounderSetupClaims | null {
    let bytes: Buffer;
    let signatureBytes: Buffer;
    try {
      bytes = Buffer.from(payload, "base64url");
      signatureBytes = Buffer.from(signature, "base64url");
    } catch {
      return null;
    }
    if (
      bytes.toString("base64url") !== payload ||
      signatureBytes.toString("base64url") !== signature ||
      signatureBytes.length !== COMPACT_SIGNATURE_BYTES ||
      bytes.length < COMPACT_HEADER_BYTES + 1 + 32 ||
      !timingSafeEqual(signatureBytes, this.#compactSignature(bytes))
    ) {
      return null;
    }
    const expiresAtSeconds = bytes.readUInt32BE(0);
    const conversationLength = bytes.readUInt16BE(4);
    if (bytes.length !== COMPACT_HEADER_BYTES + conversationLength + IDENTITY_DIGEST_BYTES) return null;
    const conversationBytes = bytes.subarray(COMPACT_HEADER_BYTES, COMPACT_HEADER_BYTES + conversationLength);
    const conversation = conversationBytes.toString("utf8");
    if (!Buffer.from(conversation, "utf8").equals(conversationBytes) || !isBoundedProviderId(conversation)) {
      return null;
    }
    const identity = bytes.subarray(COMPACT_HEADER_BYTES + conversationLength).toString("hex");
    const expiresAt = new Date(expiresAtSeconds * 1_000);
    if (expiresAt.getTime() <= now.getTime()) return null;
    return {
      providerConversationId: conversation,
      identitySubjectDigest: identity,
      expiresAt: expiresAt.toISOString(),
      householdId: this.#deterministicUuid(`founder-household\0${identity}`),
      adultId: this.#deterministicUuid(`founder-adult\0${identity}`),
    };
  }

  digestFounderSetup(token: string): string {
    return this.#macText(`florence-founder-setup-digest-v1\0${token}`, "hex");
  }

  issuePartnerSetup(input: {
    providerConversationId: string;
    identitySubjectDigest: string;
    householdId: string;
    adultId: string;
    occurredAt: string;
  }): PartnerSetupIssue {
    const providerConversationId = boundedProviderId(
      input.providerConversationId,
      "Provider conversation ID",
    );
    const identitySubjectDigest = digest(input.identitySubjectDigest, "Messages identity");
    const householdId = uuid(input.householdId, "Household ID");
    const adultId = uuid(input.adultId, "Adult ID");
    const occurredAt = instant(input.occurredAt, "Partner setup issue time");
    const expiresAtSeconds = Math.floor((occurredAt.getTime() + PARTNER_SETUP_LIFETIME_MS) / 1_000);
    if (expiresAtSeconds < 0 || expiresAtSeconds > 0xffff_ffff) {
      throw new Error("Partner setup expiry is outside the compact token range");
    }
    const conversation = Buffer.from(providerConversationId, "utf8");
    const payload = Buffer.alloc(COMPACT_HEADER_BYTES + conversation.length + 16 + 16 + 32);
    payload.writeUInt32BE(expiresAtSeconds, 0);
    payload.writeUInt16BE(conversation.length, 4);
    conversation.copy(payload, COMPACT_HEADER_BYTES);
    uuidBytes(householdId).copy(payload, COMPACT_HEADER_BYTES + conversation.length);
    uuidBytes(adultId).copy(payload, COMPACT_HEADER_BYTES + conversation.length + 16);
    Buffer.from(identitySubjectDigest, "hex").copy(payload, COMPACT_HEADER_BYTES + conversation.length + 32);
    const tag = createHmac("sha256", this.#secret)
      .update("florence-partner-setup-signature-v1\0")
      .update(payload)
      .digest()
      .subarray(0, COMPACT_SIGNATURE_BYTES);
    return {
      token: `ps1.${Buffer.concat([payload, tag]).toString("base64url")}`,
      providerConversationId,
      identitySubjectDigest,
      expiresAt: new Date(expiresAtSeconds * 1_000).toISOString(),
      householdId,
      adultId,
    };
  }

  verifyPartnerSetup(token: string, now = new Date()): PartnerSetupClaims | null {
    if (!Number.isFinite(now.getTime()) || Buffer.byteLength(token, "utf8") > 4_000) return null;
    const [version, encoded, extra] = token.split(".");
    if (version !== "ps1" || !encoded || extra) return null;
    let bytes: Buffer;
    try {
      bytes = Buffer.from(encoded, "base64url");
    } catch {
      return null;
    }
    if (bytes.toString("base64url") !== encoded || bytes.length < COMPACT_HEADER_BYTES + 65 + 24) {
      return null;
    }
    const payload = bytes.subarray(0, -COMPACT_SIGNATURE_BYTES);
    const tag = bytes.subarray(-COMPACT_SIGNATURE_BYTES);
    const expected = createHmac("sha256", this.#secret)
      .update("florence-partner-setup-signature-v1\0")
      .update(payload)
      .digest()
      .subarray(0, COMPACT_SIGNATURE_BYTES);
    if (!timingSafeEqual(tag, expected)) return null;
    const expiresAtSeconds = payload.readUInt32BE(0);
    const conversationLength = payload.readUInt16BE(4);
    if (payload.length !== COMPACT_HEADER_BYTES + conversationLength + 16 + 16 + 32) return null;
    const conversationBytes = payload.subarray(
      COMPACT_HEADER_BYTES,
      COMPACT_HEADER_BYTES + conversationLength,
    );
    const providerConversationId = conversationBytes.toString("utf8");
    if (
      !Buffer.from(providerConversationId, "utf8").equals(conversationBytes) ||
      !isBoundedProviderId(providerConversationId)
    ) {
      return null;
    }
    const offset = COMPACT_HEADER_BYTES + conversationLength;
    const householdId = uuidFromBytes(payload.subarray(offset, offset + 16));
    const adultId = uuidFromBytes(payload.subarray(offset + 16, offset + 32));
    const identitySubjectDigest = payload.subarray(offset + 32).toString("hex");
    const expiresAt = new Date(expiresAtSeconds * 1_000);
    if (expiresAt.getTime() <= now.getTime() || !isDigest(identitySubjectDigest)) return null;
    return {
      providerConversationId,
      identitySubjectDigest,
      expiresAt: expiresAt.toISOString(),
      householdId,
      adultId,
    };
  }

  digestPartnerSetup(token: string): string {
    return this.#macText(`florence-partner-setup-digest-v1\0${token}`, "hex");
  }

  #deterministicUuid(material: string): string {
    const bytes = this.#macBytes(`florence-founder-setup-id-v1\0${material}`).subarray(0, 16);
    bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x50;
    bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
    const hex = bytes.toString("hex");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }

  #macText(material: string, encoding: "base64url" | "hex"): string {
    return createHmac("sha256", this.#secret).update(material).digest(encoding);
  }

  #macBytes(material: string): Buffer {
    return createHmac("sha256", this.#secret).update(material).digest();
  }

  #compactSignature(payload: Buffer): Buffer {
    return createHmac("sha256", this.#secret)
      .update("florence-founder-setup-signature-v2\0")
      .update(payload)
      .digest()
      .subarray(0, COMPACT_SIGNATURE_BYTES);
  }
}

function boundedProviderId(value: string, label: string): string {
  if (!isBoundedProviderId(value)) throw new Error(`${label} must be a nonempty bounded string`);
  return value;
}

function isBoundedProviderId(value: string): boolean {
  const bytes = Buffer.byteLength(value, "utf8");
  return value.trim() === value && bytes > 0 && bytes <= MAX_PROVIDER_ID_BYTES;
}

function digest(value: string, label: string): string {
  if (!isDigest(value)) throw new Error(`${label} must be a lowercase SHA-256 digest`);
  return value;
}

function isDigest(value: string): boolean {
  return /^[0-9a-f]{64}$/.test(value);
}

function instant(value: string, label: string): Date {
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) throw new Error(`${label} must be a timestamp`);
  return parsed;
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value);
}

function uuid(value: string, label: string): string {
  if (!isUuid(value)) throw new Error(`${label} must be a UUID`);
  return value;
}

function uuidBytes(value: string): Buffer {
  return Buffer.from(value.replaceAll("-", ""), "hex");
}

function uuidFromBytes(value: Buffer): string {
  const hex = value.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
