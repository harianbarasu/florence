import { createHash } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { z } from "zod";
import type {
  HouseholdOperations,
  JsonObject,
  JsonValue,
  OperatorDeleteResult,
  OperatorStatus,
} from "../http/contracts.js";

const HouseholdIdSchema = z
  .string()
  .trim()
  .min(1)
  .max(128)
  .regex(/^[A-Za-z0-9_-]+$/u);
const IdempotencyKeySchema = z.string().min(16).max(256);
const NonNegativeCountSchema = z.number().int().nonnegative();

const STATUS_CHECK_NAMES = ["database", "model", "linq", "google", "worker"] as const;
type StatusCheckName = (typeof STATUS_CHECK_NAMES)[number];

export type OperatorHealthProbe = () => Promise<boolean>;

export interface OperatorHealthChecks {
  readonly database: OperatorHealthProbe;
  readonly model: OperatorHealthProbe;
  readonly linq: OperatorHealthProbe;
  readonly google: OperatorHealthProbe;
  readonly worker: OperatorHealthProbe;
}

/** Deterministically selects the oldest active owner for an operator-authorized export or deletion. */
export interface OperatorOwnerDirectory {
  firstActiveOwner(householdId: string): Promise<string | null>;
}

export interface OperatorHouseholdStore {
  exportHouseholdData(input: {
    readonly householdId: string;
    readonly requestedByAdultId: string;
    readonly exportedAt: string;
  }): Promise<Record<string, unknown>>;

  requestHouseholdDeletion(input: {
    readonly requestId: string;
    readonly householdId: string;
    readonly requestedByAdultId: string;
    readonly confirmationDigest: string;
  }): Promise<{ readonly requestId: string }>;

  confirmHouseholdDeletion(input: {
    readonly requestId: string;
    readonly confirmationDigest: string;
    readonly confirmedAt: string;
  }): Promise<boolean>;

  executeHouseholdDeletion(input: {
    readonly requestId: string;
    readonly completedAt: string;
  }): Promise<{ readonly householdId: string; readonly adultsDeleted: number }>;

  getDeletionTombstone(requestId: string): Promise<{
    readonly requestId: string;
    readonly householdId: string;
  } | null>;
}

export type OperatorServiceErrorCode =
  | "invalid_export"
  | "invalid_input"
  | "maintenance_unavailable"
  | "operation_unavailable";

/** Fixed error codes prevent provider, credential, and private-data details from escaping. */
export class OperatorServiceError extends Error {
  override readonly name = "OperatorServiceError";

  constructor(readonly code: OperatorServiceErrorCode) {
    super(code);
  }
}

export interface ProductionHouseholdOperationsOptions {
  readonly healthChecks: OperatorHealthChecks;
  readonly ownerDirectory: OperatorOwnerDirectory;
  readonly store: OperatorHouseholdStore;
  readonly now?: () => Date;
}

export class ProductionHouseholdOperations implements HouseholdOperations {
  readonly #healthChecks: OperatorHealthChecks;
  readonly #ownerDirectory: OperatorOwnerDirectory;
  readonly #store: OperatorHouseholdStore;
  readonly #now: () => Date;

  constructor(options: ProductionHouseholdOperationsOptions) {
    this.#healthChecks = options.healthChecks;
    this.#ownerDirectory = options.ownerDirectory;
    this.#store = options.store;
    this.#now = options.now ?? (() => new Date());
  }

  async status(): Promise<OperatorStatus> {
    const results = await Promise.all(
      STATUS_CHECK_NAMES.map(
        async (name) => [name, await safeHealthCheck(this.#healthChecks[name])] as const,
      ),
    );
    const checks: Record<StatusCheckName, "ok" | "degraded" | "unavailable"> = {
      database: "unavailable",
      model: "unavailable",
      linq: "unavailable",
      google: "unavailable",
      worker: "unavailable",
    };
    for (const [name, status] of results) checks[name] = status;
    return {
      status: Object.values(checks).every((value) => value === "ok") ? "ok" : "degraded",
      checks,
    };
  }

  async exportHousehold(rawInput: { householdId: string }): Promise<JsonObject | null> {
    const householdId = parseHouseholdId(rawInput.householdId);
    let ownerAdultId: string | null;
    try {
      ownerAdultId = await this.#ownerDirectory.firstActiveOwner(householdId);
    } catch {
      throw new OperatorServiceError("operation_unavailable");
    }
    if (ownerAdultId === null) return null;

    const exportedAt = instantFrom(this.#now);
    let rawExport: Record<string, unknown>;
    try {
      rawExport = await this.#store.exportHouseholdData({
        householdId,
        requestedByAdultId: ownerAdultId,
        exportedAt,
      });
    } catch (error) {
      if (hasStoreCode(error, "not_found") || hasStoreCode(error, "not_authorized")) return null;
      throw new OperatorServiceError("operation_unavailable");
    }

    try {
      return sanitizeHouseholdExport(rawExport, exportedAt);
    } catch (error) {
      if (error instanceof OperatorServiceError) throw error;
      throw new OperatorServiceError("invalid_export");
    }
  }

  async deleteHousehold(rawInput: {
    householdId: string;
    idempotencyKey: string;
  }): Promise<OperatorDeleteResult> {
    let input: { householdId: string; idempotencyKey: string };
    try {
      input = z
        .strictObject({
          householdId: HouseholdIdSchema,
          idempotencyKey: IdempotencyKeySchema,
        })
        .parse(rawInput);
    } catch {
      throw new OperatorServiceError("invalid_input");
    }

    const requestId = deterministicDeletionRequestId(input.householdId, input.idempotencyKey);
    const confirmationDigest = deterministicDeletionConfirmationDigest(
      input.householdId,
      input.idempotencyKey,
      requestId,
    );
    const occurredAt = instantFrom(this.#now);

    const existing = await this.#safeTombstone(requestId);
    if (existing !== null) return tombstoneResult(existing, input.householdId);

    let ownerAdultId: string | null;
    try {
      ownerAdultId = await this.#ownerDirectory.firstActiveOwner(input.householdId);
    } catch {
      throw new OperatorServiceError("operation_unavailable");
    }
    if (ownerAdultId === null) {
      const raced = await this.#safeTombstone(requestId);
      return raced === null ? "not_found" : tombstoneResult(raced, input.householdId);
    }

    let requested: { requestId: string } | null = null;
    try {
      requested = await this.#store.requestHouseholdDeletion({
        requestId,
        householdId: input.householdId,
        requestedByAdultId: ownerAdultId,
        confirmationDigest,
      });
    } catch {
      const raced = await this.#safeTombstone(requestId);
      if (raced !== null) return tombstoneResult(raced, input.householdId);
      // A deterministic request may already be pending or confirmed after a
      // prior ambiguous response. Confirmation below safely distinguishes it.
    }
    if (requested !== null && requested.requestId !== requestId) {
      throw new OperatorServiceError("operation_unavailable");
    }

    try {
      await this.#store.confirmHouseholdDeletion({
        requestId,
        confirmationDigest,
        confirmedAt: occurredAt,
      });
    } catch {
      const raced = await this.#safeTombstone(requestId);
      if (raced !== null) return tombstoneResult(raced, input.householdId);
      throw new OperatorServiceError("operation_unavailable");
    }

    try {
      const deleted = await this.#store.executeHouseholdDeletion({
        requestId,
        completedAt: occurredAt,
      });
      if (deleted.householdId !== input.householdId) {
        throw new OperatorServiceError("operation_unavailable");
      }
      return "accepted";
    } catch (error) {
      if (error instanceof OperatorServiceError) throw error;
      const raced = await this.#safeTombstone(requestId);
      if (raced !== null) return tombstoneResult(raced, input.householdId);
      if (hasStoreCode(error, "not_found")) return "not_found";
      throw new OperatorServiceError("operation_unavailable");
    }
  }

  async #safeTombstone(requestId: string): Promise<{ requestId: string; householdId: string } | null> {
    try {
      const tombstone = await this.#store.getDeletionTombstone(requestId);
      if (tombstone !== null && tombstone.requestId !== requestId) {
        throw new OperatorServiceError("operation_unavailable");
      }
      return tombstone;
    } catch {
      throw new OperatorServiceError("operation_unavailable");
    }
  }
}

async function safeHealthCheck(probe: OperatorHealthProbe): Promise<"ok" | "degraded" | "unavailable"> {
  try {
    return (await probe()) ? "ok" : "degraded";
  } catch {
    return "unavailable";
  }
}

function parseHouseholdId(rawHouseholdId: string): string {
  const parsed = HouseholdIdSchema.safeParse(rawHouseholdId);
  if (!parsed.success) throw new OperatorServiceError("invalid_input");
  return parsed.data;
}

function instantFrom(clock: () => Date): string {
  const value = clock();
  if (!(value instanceof Date) || !Number.isFinite(value.getTime())) {
    throw new OperatorServiceError("operation_unavailable");
  }
  return value.toISOString();
}

function hasStoreCode(error: unknown, code: string): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as { code?: unknown }).code === code
  );
}

function tombstoneResult(
  tombstone: { requestId: string; householdId: string },
  householdId: string,
): "already_deleted" {
  if (tombstone.householdId !== householdId) {
    throw new OperatorServiceError("operation_unavailable");
  }
  return "already_deleted";
}

export function deterministicDeletionRequestId(householdId: string, idempotencyKey: string): string {
  const hex = createHash("sha256")
    .update("florence:operator-deletion-request:v1\0")
    .update(householdId)
    .update("\0")
    .update(idempotencyKey)
    .digest("hex");
  const variant = ((Number.parseInt(hex[16] ?? "0", 16) & 0x3) | 0x8).toString(16);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-${variant}${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}

export function deterministicDeletionConfirmationDigest(
  householdId: string,
  idempotencyKey: string,
  requestId = deterministicDeletionRequestId(householdId, idempotencyKey),
): string {
  return createHash("sha256")
    .update("florence:operator-deletion-confirmation:v1\0")
    .update(requestId)
    .update("\0")
    .update(householdId)
    .update("\0")
    .update(idempotencyKey)
    .digest("hex");
}

const EXPORT_FIELDS = {
  household: ["id", "name", "timezone", "status", "version", "created_at", "updated_at"],
  adult: ["id", "display_name", "timezone", "role", "status", "consented_at", "created_at", "updated_at"],
  channel: [
    "id",
    "adult_id",
    "provider",
    "channel_type",
    "external_chat_id",
    "external_handle",
    "status",
    "metadata",
    "created_at",
    "updated_at",
  ],
  connection: [
    "id",
    "adult_id",
    "provider",
    "label",
    "external_account_id",
    "email",
    "granted_scopes",
    "status",
    "cursor",
    "metadata",
    "last_synced_at",
    "created_at",
    "updated_at",
  ],
  source: [
    "id",
    "connection_id",
    "owner_adult_id",
    "visibility",
    "provider",
    "external_id",
    "kind",
    "occurred_at",
    "subject",
    "content_hash",
    "metadata",
    "retention_until",
    "revision",
    "created_at",
    "updated_at",
  ],
  projection: [
    "schema_version",
    "version",
    "revision",
    "state",
    "state_redacted",
    "redaction_reason",
    "created_at",
    "updated_at",
  ],
  commit: ["idempotency_key", "base_revision", "revision", "outcome", "committed_at"],
  audit: [
    "sequence",
    "actor_kind",
    "actor_id",
    "action",
    "target_type",
    "target_id",
    "visibility",
    "owner_adult_id",
    "source_refs",
    "policy_refs",
    "details",
    "created_at",
  ],
} as const;

function sanitizeHouseholdExport(raw: Record<string, unknown>, exportedAt: string): JsonObject {
  return {
    schemaVersion: 1,
    exportedAt,
    household: sanitizeSelectedObject(raw.household, EXPORT_FIELDS.household),
    adults: sanitizeSelectedArray(raw.adults, EXPORT_FIELDS.adult),
    channels: sanitizeSelectedArray(raw.channels, EXPORT_FIELDS.channel),
    connections: sanitizeSelectedArray(raw.connections, EXPORT_FIELDS.connection),
    sources: sanitizeSelectedArray(raw.sources, EXPORT_FIELDS.source),
    projection: sanitizeNullableObject(raw.projection, EXPORT_FIELDS.projection),
    applicationSnapshot: sanitizeNullableObject(raw.applicationSnapshot, EXPORT_FIELDS.projection),
    applicationCommits: sanitizeSelectedArray(raw.applicationCommits, EXPORT_FIELDS.commit),
    audits: sanitizeSelectedArray(raw.audits, EXPORT_FIELDS.audit),
  };
}

function sanitizeSelectedArray(raw: unknown, fields: readonly string[]): JsonValue[] {
  if (!Array.isArray(raw)) throw new OperatorServiceError("invalid_export");
  return raw.map((item) => sanitizeSelectedObject(item, fields));
}

function sanitizeNullableObject(raw: unknown, fields: readonly string[]): JsonObject | null {
  return raw === null ? null : sanitizeSelectedObject(raw, fields);
}

function sanitizeSelectedObject(raw: unknown, fields: readonly string[]): JsonObject {
  if (!isPlainRecord(raw)) throw new OperatorServiceError("invalid_export");
  const selected: Record<string, unknown> = {};
  for (const field of fields) {
    if (field in raw) selected[field] = raw[field];
  }
  const sanitized = sanitizeJsonValue(selected, new WeakSet(), 0);
  if (!isJsonObject(sanitized)) throw new OperatorServiceError("invalid_export");
  return sanitized;
}

function sanitizeJsonValue(value: unknown, seen: WeakSet<object>, depth: number): JsonValue {
  if (depth > 64) throw new OperatorServiceError("invalid_export");
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new OperatorServiceError("invalid_export");
    return value;
  }
  if (value instanceof Date) {
    if (!Number.isFinite(value.getTime())) throw new OperatorServiceError("invalid_export");
    return value.toISOString();
  }
  if (Array.isArray(value)) {
    if (seen.has(value)) throw new OperatorServiceError("invalid_export");
    seen.add(value);
    const result = value.map((item) => sanitizeJsonValue(item, seen, depth + 1));
    seen.delete(value);
    return result;
  }
  if (!isPlainRecord(value)) throw new OperatorServiceError("invalid_export");
  if (seen.has(value)) throw new OperatorServiceError("invalid_export");
  seen.add(value);
  const result: JsonObject = {};
  for (const [key, item] of Object.entries(value)) {
    if (item === undefined || isSensitiveExportKey(key)) continue;
    result[key] = sanitizeJsonValue(item, seen, depth + 1);
  }
  seen.delete(value);
  return result;
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function isJsonObject(value: JsonValue): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isSensitiveExportKey(key: string): boolean {
  const normalized = key.toLowerCase().replace(/[^a-z0-9]/gu, "");
  return (
    normalized.includes("accesstoken") ||
    normalized.includes("refreshtoken") ||
    normalized.includes("idtoken") ||
    normalized.includes("apikey") ||
    normalized.includes("authorization") ||
    normalized.includes("clientsecret") ||
    normalized.includes("cookie") ||
    normalized.includes("credential") ||
    normalized.includes("databaseurl") ||
    normalized.includes("encryptedcontent") ||
    normalized.startsWith("encrypted") ||
    normalized.includes("encryptionkey") ||
    normalized.includes("modeltrace") ||
    normalized.includes("operatortoken") ||
    normalized.includes("password") ||
    normalized.includes("privatekey") ||
    normalized.includes("redisurl") ||
    normalized.includes("signingkey") ||
    normalized.includes("webhooksecret") ||
    normalized.includes("secret") ||
    normalized === "tokens" ||
    normalized.endsWith("dsn") ||
    normalized.endsWith("secret") ||
    normalized.endsWith("token")
  );
}

export interface OperatorMaintenancePort {
  purgeExpiredSourceContent(asOf: string): Promise<number>;
  purgeExpiredProviderInbox(asOf: string): Promise<number>;
  executeConfirmedHouseholdDeletions(input: {
    readonly completedAt: string;
    readonly limit: number;
  }): Promise<number>;
}

export interface MaintenanceRunReceipt {
  readonly ranAt: string;
  readonly sourceItemsPurged: number;
  readonly providerInboxItemsPurged: number;
  readonly householdDeletionsCompleted: number;
}

export type MaintenanceWait = (milliseconds: number, signal: AbortSignal) => Promise<void>;

export interface PeriodicMaintenanceCoordinatorOptions {
  readonly maintenance: OperatorMaintenancePort;
  readonly intervalMs?: number;
  readonly deletionBatchSize?: number;
  readonly now?: () => Date;
  readonly wait?: MaintenanceWait;
}

/** A bounded loop over retention and already-confirmed deletion work only. */
export class PeriodicMaintenanceCoordinator {
  readonly #maintenance: OperatorMaintenancePort;
  readonly #intervalMs: number;
  readonly #deletionBatchSize: number;
  readonly #now: () => Date;
  readonly #wait: MaintenanceWait;

  constructor(options: PeriodicMaintenanceCoordinatorOptions) {
    this.#maintenance = options.maintenance;
    this.#intervalMs = z
      .number()
      .int()
      .min(1_000)
      .max(86_400_000)
      .parse(options.intervalMs ?? 60_000);
    this.#deletionBatchSize = z
      .number()
      .int()
      .positive()
      .max(100)
      .parse(options.deletionBatchSize ?? 25);
    this.#now = options.now ?? (() => new Date());
    this.#wait = options.wait ?? defaultMaintenanceWait;
  }

  async runOnce(): Promise<MaintenanceRunReceipt> {
    const ranAt = instantFrom(this.#now);
    try {
      const [sourceItemsPurged, providerInboxItemsPurged] = await Promise.all([
        this.#maintenance.purgeExpiredSourceContent(ranAt),
        this.#maintenance.purgeExpiredProviderInbox(ranAt),
      ]);
      const householdDeletionsCompleted = await this.#maintenance.executeConfirmedHouseholdDeletions({
        completedAt: ranAt,
        limit: this.#deletionBatchSize,
      });
      return {
        ranAt,
        sourceItemsPurged: NonNegativeCountSchema.parse(sourceItemsPurged),
        providerInboxItemsPurged: NonNegativeCountSchema.parse(providerInboxItemsPurged),
        householdDeletionsCompleted: NonNegativeCountSchema.parse(householdDeletionsCompleted),
      };
    } catch {
      throw new OperatorServiceError("maintenance_unavailable");
    }
  }

  async run(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      await this.runOnce();
      if (signal.aborted) return;
      try {
        await this.#wait(this.#intervalMs, signal);
      } catch {
        if (signal.aborted) return;
        throw new OperatorServiceError("maintenance_unavailable");
      }
    }
  }
}

async function defaultMaintenanceWait(milliseconds: number, signal: AbortSignal): Promise<void> {
  await sleep(milliseconds, undefined, { signal });
}
