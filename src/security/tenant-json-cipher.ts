import { createCipheriv, createDecipheriv, hkdfSync, randomBytes } from "node:crypto";

const FORMAT_VERSION = "v1";
const KEY_BYTES = 32;
const IV_BYTES = 12;
const TAG_BYTES = 16;
const MINIMUM_PAYLOAD_BYTES = IV_BYTES + TAG_BYTES + 1;
const HKDF_SALT = Buffer.from("florence.tenant-json-cipher.hkdf.v1", "utf8");

export type EncryptionTenantKind = "household" | "provider_ingress";

export type SensitiveTable =
  | "provider_inbox"
  | "provider_inbox_conflicts"
  | "household_projections"
  | "application_snapshots"
  | "application_commits"
  | "household_signals"
  | "jobs"
  | "scheduled_triggers"
  | "outbox"
  | "calendar_busy_windows"
  | "personal_attention_rule_revisions"
  | "adult_identity_details"
  | "external_connections";

export interface EncryptionTenant {
  readonly kind: EncryptionTenantKind;
  readonly id: string;
}

export interface EncryptionContext {
  readonly tenant: EncryptionTenant;
  readonly table: SensitiveTable;
  readonly rowId: string;
  readonly field: string;
}

export interface EncryptionKeyring {
  readonly activeKeyId: string;
  readonly keys: Readonly<Record<string, string>>;
}

export interface EncryptedJson {
  readonly keyId: string;
  readonly ciphertext: string;
}

export type EncryptionErrorCode = "unknown_key" | "invalid_envelope" | "authentication_failed";

const ERROR_MESSAGES: Readonly<Record<EncryptionErrorCode, string>> = Object.freeze({
  unknown_key: "The encrypted value references an unavailable key.",
  invalid_envelope: "The encrypted value has an invalid envelope.",
  authentication_failed: "The encrypted value could not be authenticated.",
});

export class EncryptionError extends Error {
  readonly code: EncryptionErrorCode;

  constructor(code: EncryptionErrorCode) {
    super(ERROR_MESSAGES[code]);
    this.name = "EncryptionError";
    this.code = code;
  }
}

/** Encrypts JSON with a tenant-derived key and binds it to one durable storage location. */
export class TenantJsonCipher {
  readonly #activeKeyId: string;
  readonly #keys: ReadonlyMap<string, Buffer>;

  constructor(keyring: EncryptionKeyring) {
    if (!isNonEmptyString(keyring?.activeKeyId) || !isRecord(keyring?.keys)) {
      throw new Error("The encryption keyring is invalid.");
    }

    const keys = new Map<string, Buffer>();
    for (const [keyId, encodedKey] of Object.entries(keyring.keys)) {
      if (!isNonEmptyString(keyId) || typeof encodedKey !== "string") {
        throw new Error("The encryption keyring is invalid.");
      }
      keys.set(keyId, decodeKey(encodedKey));
    }
    if (keys.size === 0 || !keys.has(keyring.activeKeyId)) {
      throw new Error("The encryption keyring must contain its active key.");
    }

    this.#activeKeyId = keyring.activeKeyId;
    this.#keys = keys;
  }

  get activeKeyId(): string {
    return this.#activeKeyId;
  }

  hasKey(keyId: string): boolean {
    return this.#keys.has(keyId);
  }

  seal(value: unknown, context: EncryptionContext): EncryptedJson {
    assertContext(context);
    const plaintext = serializeJson(value);
    const rootKey = this.#keys.get(this.#activeKeyId);
    if (rootKey === undefined) {
      throw new EncryptionError("unknown_key");
    }

    const tenantKey = deriveTenantKey(rootKey, this.#activeKeyId, context.tenant);
    try {
      const iv = randomBytes(IV_BYTES);
      const cipher = createCipheriv("aes-256-gcm", tenantKey, iv);
      cipher.setAAD(associatedData(this.#activeKeyId, context));
      const ciphertext = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
      const tag = cipher.getAuthTag();
      const payload = Buffer.concat([iv, tag, ciphertext]).toString("base64url");
      return {
        keyId: this.#activeKeyId,
        ciphertext: `${FORMAT_VERSION}:${payload}`,
      };
    } finally {
      tenantKey.fill(0);
    }
  }

  open(sealed: EncryptedJson, context: EncryptionContext): unknown {
    assertContext(context);
    const envelope = parseEnvelope(sealed);
    const rootKey = this.#keys.get(envelope.keyId);
    if (rootKey === undefined) {
      throw new EncryptionError("unknown_key");
    }

    const tenantKey = deriveTenantKey(rootKey, envelope.keyId, context.tenant);
    let plaintext: string;
    try {
      const decipher = createDecipheriv("aes-256-gcm", tenantKey, envelope.iv);
      decipher.setAAD(associatedData(envelope.keyId, context));
      decipher.setAuthTag(envelope.tag);
      plaintext = Buffer.concat([decipher.update(envelope.ciphertext), decipher.final()]).toString("utf8");
    } catch {
      throw new EncryptionError("authentication_failed");
    } finally {
      tenantKey.fill(0);
    }

    try {
      return JSON.parse(plaintext) as unknown;
    } catch {
      throw new EncryptionError("invalid_envelope");
    }
  }

  rewrap(sealed: EncryptedJson, context: EncryptionContext): EncryptedJson {
    return this.seal(this.open(sealed, context), context);
  }
}

interface ParsedEnvelope {
  readonly keyId: string;
  readonly iv: Buffer;
  readonly tag: Buffer;
  readonly ciphertext: Buffer;
}

function parseEnvelope(sealed: EncryptedJson): ParsedEnvelope {
  if (!isRecord(sealed) || !isNonEmptyString(sealed.keyId) || typeof sealed.ciphertext !== "string") {
    throw new EncryptionError("invalid_envelope");
  }

  const parts = sealed.ciphertext.split(":");
  if (parts.length !== 2 || parts[0] !== FORMAT_VERSION) {
    throw new EncryptionError("invalid_envelope");
  }
  const payload = decodePayload(parts[1]);
  if (payload === null || payload.length < MINIMUM_PAYLOAD_BYTES) {
    throw new EncryptionError("invalid_envelope");
  }

  return {
    keyId: sealed.keyId,
    iv: payload.subarray(0, IV_BYTES),
    tag: payload.subarray(IV_BYTES, IV_BYTES + TAG_BYTES),
    ciphertext: payload.subarray(IV_BYTES + TAG_BYTES),
  };
}

function decodePayload(encoded: string | undefined): Buffer | null {
  if (encoded === undefined || !/^[A-Za-z0-9_-]+$/u.test(encoded) || encoded.length % 4 === 1) {
    return null;
  }
  const decoded = Buffer.from(encoded, "base64url");
  return decoded.toString("base64url") === encoded ? decoded : null;
}

function decodeKey(encoded: string): Buffer {
  if (/^[\da-f]{64}$/iu.test(encoded)) {
    return Buffer.from(encoded, "hex");
  }

  if (!/^[A-Za-z0-9_-]{43}=?$/u.test(encoded)) {
    throw new Error("Encryption keys must encode exactly 32 bytes.");
  }
  const unpadded = encoded.endsWith("=") ? encoded.slice(0, -1) : encoded;
  const decoded = Buffer.from(unpadded, "base64url");
  if (decoded.length !== KEY_BYTES || decoded.toString("base64url") !== unpadded) {
    throw new Error("Encryption keys must encode exactly 32 bytes.");
  }
  return decoded;
}

function deriveTenantKey(rootKey: Buffer, keyId: string, tenant: EncryptionTenant): Buffer {
  const info = encodeTuple([
    "florence.tenant-json-cipher.tenant-key",
    FORMAT_VERSION,
    keyId,
    tenant.kind,
    tenant.id,
  ]);
  return Buffer.from(hkdfSync("sha256", rootKey, HKDF_SALT, info, KEY_BYTES));
}

function associatedData(keyId: string, context: EncryptionContext): Buffer {
  return encodeTuple([
    "florence.tenant-json-cipher",
    FORMAT_VERSION,
    keyId,
    context.tenant.kind,
    context.tenant.id,
    context.table,
    context.rowId,
    context.field,
  ]);
}

function encodeTuple(values: readonly string[]): Buffer {
  return Buffer.from(JSON.stringify(values), "utf8");
}

function serializeJson(value: unknown): string {
  try {
    const serialized = JSON.stringify(value);
    if (serialized !== undefined) return serialized;
  } catch {
    // Use one non-sensitive error for every serialization failure.
  }
  throw new TypeError("The value must be JSON-serializable.");
}

function assertContext(context: EncryptionContext): void {
  if (
    !isRecord(context) ||
    !isRecord(context.tenant) ||
    (context.tenant.kind !== "household" && context.tenant.kind !== "provider_ingress") ||
    !isNonEmptyString(context.tenant.id) ||
    !isSensitiveTable(context.table) ||
    !isNonEmptyString(context.rowId) ||
    !isNonEmptyString(context.field)
  ) {
    throw new TypeError("The encryption context is invalid.");
  }
}

function isSensitiveTable(value: unknown): value is SensitiveTable {
  return (
    value === "provider_inbox" ||
    value === "provider_inbox_conflicts" ||
    value === "household_projections" ||
    value === "application_snapshots" ||
    value === "application_commits" ||
    value === "household_signals" ||
    value === "jobs" ||
    value === "scheduled_triggers" ||
    value === "outbox" ||
    value === "calendar_busy_windows" ||
    value === "personal_attention_rule_revisions" ||
    value === "adult_identity_details" ||
    value === "external_connections"
  );
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
