import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { z } from "zod";
import { type ApplicationOutboxIntent, ApplicationOutboxIntentSchema } from "../application/contracts.js";
import type { JsonObject, JsonValue } from "../http/contracts.js";
import {
  CustomerDataControlStoreError,
  type CustomerDeletionRequestRecord,
  type PostgresCustomerDataControlStore,
} from "./customer-data-control-store.js";
import type { PrivateCommandHandler } from "./provider-processor.js";

const exportPayloadSchema = z
  .object({
    version: z.literal(1),
    handoffId: z.uuid(),
    issuedAt: z.number().int().nonnegative(),
    expiresAt: z.number().int().positive(),
  })
  .strict();

export interface CustomerDataControlOutbox {
  enqueueApplicationIntent(intent: ApplicationOutboxIntent): Promise<{ rowId: string }>;
}

export interface CustomerExportReader {
  exportHouseholdData(input: {
    householdId: string;
    requestedByAdultId: string;
    exportedAt: string;
  }): Promise<Record<string, unknown>>;
}

export type CustomerExportConsumption =
  | { readonly status: "download"; readonly filename: string; readonly artifact: JsonObject }
  | { readonly status: "expired" | "invalid" | "consumed" | "unavailable" };

export interface CustomerDataControlCommandServiceOptions {
  readonly store: PostgresCustomerDataControlStore;
  readonly outbox: CustomerDataControlOutbox;
  readonly exportReader: CustomerExportReader;
  readonly publicBaseUrl: string;
  readonly signingSecret: string;
  readonly now?: () => Date;
  readonly exportTtlMs?: number;
  readonly challengeTtlMs?: number;
}

/** App-owned customer control: models and workers never see or authorize these commands. */
export class CustomerDataControlCommandService implements PrivateCommandHandler {
  readonly #store: PostgresCustomerDataControlStore;
  readonly #outbox: CustomerDataControlOutbox;
  readonly #exportReader: CustomerExportReader;
  readonly #publicBaseUrl: string;
  readonly #signingSecret: string;
  readonly #now: () => Date;
  readonly #exportTtlMs: number;
  readonly #challengeTtlMs: number;

  public constructor(options: CustomerDataControlCommandServiceOptions) {
    if (Buffer.byteLength(options.signingSecret, "utf8") < 32) {
      throw new Error("Customer data-control signing secret must contain at least 32 bytes");
    }
    this.#store = options.store;
    this.#outbox = options.outbox;
    this.#exportReader = options.exportReader;
    this.#publicBaseUrl = z.url().parse(options.publicBaseUrl);
    this.#signingSecret = options.signingSecret;
    this.#now = options.now ?? (() => new Date());
    this.#exportTtlMs = z
      .number()
      .int()
      .min(60_000)
      .max(60 * 60_000)
      .parse(options.exportTtlMs ?? 15 * 60_000);
    this.#challengeTtlMs = z
      .number()
      .int()
      .min(5 * 60_000)
      .max(7 * 24 * 60 * 60_000)
      .parse(options.challengeTtlMs ?? 24 * 60 * 60_000);
  }

  public async handle(input: {
    householdId: string;
    adultId: string;
    channelId: string;
    messageId: string;
    text: string;
    occurredAt: string;
    idempotencyKey: string;
  }): Promise<{ handled: boolean; classification?: string }> {
    const normalized = normalizePhrase(input.text);
    const deletion = await this.#safeCurrentDeletion(input);

    if (deletion && ["fenced", "cleaning", "blocked"].includes(deletion.status)) {
      await this.#queuePrivate(
        input,
        "deletion-fenced-status",
        deletion.status === "blocked"
          ? "Household deletion is fenced and a provider cleanup is retrying. Normal sync, reminders, and messages remain stopped. Florence still retains encrypted credentials only so it can finish revocation safely."
          : "Household deletion is fenced and cleanup is in progress. Normal sync, reminders, and messages are stopped.",
      );
      return { handled: true, classification: `customer_control:deletion_${deletion.status}` };
    }

    if (normalized === "EXPORT MY DATA") {
      return this.#issueExport(input);
    }
    if (normalized === "DELETE MY DATA") {
      return this.#beginDeletion(input);
    }
    if (normalized === "DELETION STATUS" || normalized === "DELETE STATUS") {
      await this.#queuePrivate(
        input,
        "deletion-status",
        deletion ? deletionStatusText(deletion) : "There is no active household-deletion request.",
      );
      return { handled: true, classification: "customer_control:deletion_status" };
    }
    if (/^CONFIRM DELETE [A-Z0-9]{8} [A-Z0-9]{16}$/u.test(normalized)) {
      return this.#confirmDeletion(input, normalized);
    }
    const cancellation = normalized.match(/^CANCEL DELETE ([A-Z0-9]{8})$/u);
    if (cancellation) {
      return this.#cancelDeletion(input, cancellation[1] as string, deletion);
    }
    return { handled: false };
  }

  public async consumeExportToken(token: string): Promise<CustomerExportConsumption> {
    const verified = verifyExportToken(token, this.#signingSecret, this.#now());
    if (verified.status !== "valid") return { status: verified.status };
    const consumedAt = validNow(this.#now).toISOString();
    const consumed = await this.#store.consumeExportHandoff({
      handoffId: verified.handoffId,
      tokenDigest: sha256(token),
      consumedAt,
    });
    if (consumed.status === "expired") return { status: "expired" };
    if (consumed.status === "already_consumed") return { status: "consumed" };
    if (consumed.status !== "consumed") return { status: "invalid" };
    try {
      const raw = await this.#exportReader.exportHouseholdData({
        householdId: consumed.householdId,
        requestedByAdultId: consumed.adultId,
        exportedAt: consumedAt,
      });
      return {
        status: "download",
        filename: `florence-export-${consumedAt.slice(0, 10)}.json`,
        artifact: sanitizeCustomerExport(raw, consumed.adultId, consumedAt),
      };
    } catch {
      return { status: "unavailable" };
    }
  }

  async #issueExport(
    input: Parameters<PrivateCommandHandler["handle"]>[0],
  ): Promise<{ handled: true; classification: string }> {
    const issuedAt = validNow(this.#now);
    const expiresAt = new Date(issuedAt.getTime() + this.#exportTtlMs);
    const handoffId = deterministicUuid(
      this.#signingSecret,
      "export-handoff",
      input.householdId,
      input.adultId,
      input.idempotencyKey,
    );
    const proposedToken = issueExportToken(
      {
        handoffId,
        issuedAt: issuedAt.getTime(),
        expiresAt: expiresAt.getTime(),
      },
      this.#signingSecret,
    );
    try {
      const handoff = await this.#store.issueExportHandoff({
        handoffId,
        householdId: input.householdId,
        adultId: input.adultId,
        channelId: input.channelId,
        idempotencyKey: input.idempotencyKey,
        tokenDigest: sha256(proposedToken),
        issuedAt: issuedAt.toISOString(),
        expiresAt: expiresAt.toISOString(),
      });
      const token = issueExportToken(
        {
          handoffId: handoff.handoffId,
          issuedAt: Date.parse(handoff.issuedAt),
          expiresAt: Date.parse(handoff.expiresAt),
        },
        this.#signingSecret,
      );
      if (sha256(token) !== handoff.tokenDigest) throw new Error("Export handoff identity conflict");
      const url = new URL(`/control/export/${token}`, this.#publicBaseUrl).toString();
      await this.#queuePrivate(
        input,
        "customer-export",
        `Your private Florence export is ready. This browser link expires in 15 minutes and works once: ${url}`,
      );
    } catch {
      await this.#queuePrivate(
        input,
        "customer-export-unavailable",
        "I could not create a private export link safely. Nothing was exported; please try again.",
      );
    }
    return { handled: true, classification: "customer_control:export" };
  }

  async #beginDeletion(
    input: Parameters<PrivateCommandHandler["handle"]>[0],
  ): Promise<{ handled: true; classification: string }> {
    const requestedAt = validNow(this.#now);
    const expiresAt = new Date(requestedAt.getTime() + this.#challengeTtlMs);
    const requestId = deterministicUuid(
      this.#signingSecret,
      "deletion-request",
      input.householdId,
      input.idempotencyKey,
    );
    const requestCode = deletionRequestCode(requestId, this.#signingSecret);
    let request: CustomerDeletionRequestRecord;
    try {
      request = await this.#store.beginDeletion({
        requestId,
        householdId: input.householdId,
        adultId: input.adultId,
        channelId: input.channelId,
        requestCodeDigest: sha256(requestCode),
        requestedAt: requestedAt.toISOString(),
        expiresAt: expiresAt.toISOString(),
        challengeDigest: (challenge) => sha256(this.#challengeCommand(challenge)),
      });
    } catch {
      await this.#queuePrivate(
        input,
        "deletion-unavailable",
        "I could not open a household-deletion request safely. Deletion requires exactly two active adults, each with an active private Florence conversation. No data was deleted.",
      );
      return { handled: true, classification: "customer_control:deletion_rejected" };
    }
    for (const recipient of request.recipients) {
      const command = this.#challengeCommand({
        requestId: request.requestId,
        adultId: recipient.adultId,
        privateChannelBindingId: recipient.privateChannelBindingId,
        expiresAt: request.expiresAt,
      });
      await this.#queueToAdult(
        input.householdId,
        recipient.adultId,
        `deletion-challenge-${request.requestId}-${recipient.adultId}`,
        `Household deletion removes Florence's shared household records and both adults' Florence-held personal records. It disconnects Google accounts and stops reminders; it cannot remove messages already delivered by iMessage or data still held by Google. Nothing happens unless both adults independently confirm from this exact private conversation before ${request.expiresAt}.\n\nTo confirm exactly, reply:\n${command}\n\nBefore both confirmations, either adult can cancel with:\nCANCEL DELETE ${deletionRequestCode(request.requestId, this.#signingSecret)}`,
      );
    }
    return { handled: true, classification: "customer_control:deletion_challenge" };
  }

  async #confirmDeletion(
    input: Parameters<PrivateCommandHandler["handle"]>[0],
    normalizedCommand: string,
  ): Promise<{ handled: true; classification: string }> {
    let result: Awaited<ReturnType<PostgresCustomerDataControlStore["confirmDeletion"]>>;
    try {
      result = await this.#store.confirmDeletion({
        householdId: input.householdId,
        adultId: input.adultId,
        channelId: input.channelId,
        commandDigest: sha256(normalizedCommand),
        idempotencyKey: input.idempotencyKey,
        confirmedAt: validNow(this.#now).toISOString(),
      });
    } catch {
      result = { status: "invalid" };
    }
    if (result.status === "waiting") {
      await this.#queuePrivate(
        input,
        "deletion-confirmed-waiting",
        "Your confirmation is recorded. Florence is waiting for the other adult's independent private confirmation. Either adult can still cancel before that happens.",
      );
    } else if (result.status === "expired") {
      await this.#queuePrivate(
        input,
        "deletion-expired",
        "That deletion challenge expired. No data was deleted.",
      );
    } else if (result.status === "invalid") {
      await this.#queuePrivate(
        input,
        "deletion-invalid",
        "That confirmation does not match an active challenge for you in this private conversation. No data was deleted.",
      );
    } else if (result.status === "already_confirmed") {
      await this.#queuePrivate(
        input,
        "deletion-already-confirmed",
        "That confirmation was already recorded. No additional action was taken.",
      );
    }
    return { handled: true, classification: `customer_control:deletion_${result.status}` };
  }

  async #cancelDeletion(
    input: Parameters<PrivateCommandHandler["handle"]>[0],
    requestCode: string,
    deletion: CustomerDeletionRequestRecord | null,
  ): Promise<{ handled: true; classification: string }> {
    let result: Awaited<ReturnType<PostgresCustomerDataControlStore["cancelDeletion"]>>;
    try {
      result = await this.#store.cancelDeletion({
        householdId: input.householdId,
        adultId: input.adultId,
        channelId: input.channelId,
        requestCodeDigest: sha256(requestCode),
        cancelledAt: validNow(this.#now).toISOString(),
      });
    } catch {
      result = "invalid";
    }
    const body =
      result === "cancelled"
        ? "The household-deletion request is cancelled. No data was deleted."
        : result === "too_late"
          ? "Both adults already confirmed, so the deletion fence is active and can no longer be cancelled."
          : result === "expired"
            ? "That deletion challenge expired. No data was deleted."
            : "That cancellation does not match an active request for this private conversation.";
    const recipients = deletion?.recipients ?? [{ adultId: input.adultId }];
    for (const recipient of recipients) {
      await this.#queueToAdult(
        input.householdId,
        recipient.adultId,
        `deletion-cancel-${input.idempotencyKey}-${recipient.adultId}`,
        body,
      );
    }
    return { handled: true, classification: `customer_control:deletion_cancel_${result}` };
  }

  async #safeCurrentDeletion(
    input: Parameters<PrivateCommandHandler["handle"]>[0],
  ): Promise<CustomerDeletionRequestRecord | null> {
    try {
      return await this.#store.currentDeletion(input.householdId, input.adultId, input.channelId);
    } catch (error) {
      if (error instanceof CustomerDataControlStoreError) return null;
      throw error;
    }
  }

  #challengeCommand(input: {
    requestId: string;
    adultId: string;
    privateChannelBindingId: string;
    expiresAt: string;
  }): string {
    const requestCode = deletionRequestCode(input.requestId, this.#signingSecret);
    const challengeCode = hmacCode(
      this.#signingSecret,
      "deletion-challenge",
      16,
      input.requestId,
      input.adultId,
      input.privateChannelBindingId,
      input.expiresAt,
    );
    return `CONFIRM DELETE ${requestCode} ${challengeCode}`;
  }

  async #queuePrivate(
    input: Pick<Parameters<PrivateCommandHandler["handle"]>[0], "householdId" | "adultId" | "idempotencyKey">,
    suffix: string,
    body: string,
  ): Promise<void> {
    return this.#queueToAdult(input.householdId, input.adultId, `${input.idempotencyKey}:${suffix}`, body);
  }

  async #queueToAdult(householdId: string, adultId: string, key: string, body: string): Promise<void> {
    const intentId = `customer_control.${sha256(key)}`;
    await this.#outbox.enqueueApplicationIntent(
      ApplicationOutboxIntentSchema.parse({
        intentId,
        householdId,
        idempotencyKey: `florence:${intentId}`,
        kind: "conversation.send",
        targetScope: { kind: "personal", adultId },
        messageClass: "status",
        body,
      }),
    );
  }
}

function issueExportToken(
  input: { handoffId: string; issuedAt: number; expiresAt: number },
  secret: string,
): string {
  const payload = exportPayloadSchema.parse({ version: 1, ...input });
  const encoded = Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
  return `${encoded}.${sign(encoded, secret, "export")}`;
}

function verifyExportToken(
  token: string,
  secret: string,
  now: Date,
): { status: "valid"; handoffId: string } | { status: "expired" | "invalid" | "consumed" } {
  const [encoded, supplied, extra] = token.split(".");
  if (!encoded || !supplied || extra !== undefined) return { status: "invalid" };
  const expected = sign(encoded, secret, "export");
  const suppliedBytes = Buffer.from(supplied, "utf8");
  const expectedBytes = Buffer.from(expected, "utf8");
  if (suppliedBytes.length !== expectedBytes.length || !timingSafeEqual(suppliedBytes, expectedBytes)) {
    return { status: "invalid" };
  }
  try {
    const payload = exportPayloadSchema.parse(JSON.parse(Buffer.from(encoded, "base64url").toString("utf8")));
    if (payload.expiresAt <= now.getTime()) return { status: "expired" };
    if (payload.issuedAt > now.getTime() + 60_000 || payload.expiresAt <= payload.issuedAt) {
      return { status: "invalid" };
    }
    return { status: "valid", handoffId: payload.handoffId };
  } catch {
    return { status: "invalid" };
  }
}

function deletionRequestCode(requestId: string, secret: string): string {
  return hmacCode(secret, "deletion-request-code", 8, requestId);
}

function deterministicUuid(secret: string, purpose: string, ...parts: readonly string[]): string {
  const hex = createHmac("sha256", secret)
    .update(`florence:customer-control:${purpose}:v1\0`)
    .update(parts.join("\0"))
    .digest("hex");
  const variant = ((Number.parseInt(hex[16] ?? "0", 16) & 0x3) | 0x8).toString(16);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-${variant}${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}

function hmacCode(secret: string, purpose: string, length: number, ...parts: readonly string[]): string {
  return createHmac("sha256", secret)
    .update(`florence:customer-control:${purpose}:v1\0`)
    .update(parts.join("\0"))
    .digest("hex")
    .toUpperCase()
    .slice(0, length);
}

function sign(encoded: string, secret: string, purpose: string): string {
  return createHmac("sha256", secret)
    .update(`florence:customer-control:${purpose}:v1\0`)
    .update(encoded)
    .digest("base64url");
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function normalizePhrase(value: string): string {
  return value.normalize("NFKC").trim().replace(/\s+/gu, " ").toUpperCase();
}

function validNow(clock: () => Date): Date {
  const value = clock();
  if (!Number.isFinite(value.getTime())) throw new Error("Customer-control clock is invalid");
  return value;
}

function deletionStatusText(request: CustomerDeletionRequestRecord): string {
  if (request.status === "awaiting_confirmations") {
    return `${request.confirmedAdultIds.length} of 2 adults have confirmed household deletion. The challenge expires at ${request.expiresAt}; either adult can still cancel before both confirmations.`;
  }
  if (request.status === "blocked") {
    return "Household deletion is fenced. A provider cleanup is retrying, and normal Florence work remains stopped.";
  }
  return `Household deletion status: ${request.status}.`;
}

function sanitizeCustomerExport(
  raw: Record<string, unknown>,
  requestedByAdultId: string,
  exportedAt: string,
): JsonObject {
  assertScopedRows(raw.connections, (row) => row.adult_id === requestedByAdultId);
  assertScopedRows(
    raw.sources,
    (row) => row.visibility === "household" || row.owner_adult_id === requestedByAdultId,
  );
  assertScopedRows(
    raw.audits,
    (row) => row.visibility === "household" || row.owner_adult_id === requestedByAdultId,
  );
  return {
    schemaVersion: 1,
    exportedAt,
    requestedByAdultId,
    household: selectObject(raw.household, [
      "id",
      "name",
      "timezone",
      "status",
      "version",
      "created_at",
      "updated_at",
    ]),
    adults: selectArray(raw.adults, [
      "id",
      "display_name",
      "timezone",
      "role",
      "status",
      "consented_at",
      "created_at",
      "updated_at",
    ]),
    channels: selectArray(raw.channels, [
      "id",
      "adult_id",
      "provider",
      "channel_type",
      "status",
      "created_at",
      "updated_at",
    ]),
    connections: selectArray(raw.connections, [
      "id",
      "adult_id",
      "provider",
      "label",
      "email",
      "granted_scopes",
      "status",
      "last_synced_at",
      "created_at",
      "updated_at",
    ]),
    sources: selectArray(raw.sources, [
      "id",
      "owner_adult_id",
      "visibility",
      "provider",
      "kind",
      "occurred_at",
      "subject",
      "retention_until",
      "revision",
      "created_at",
      "updated_at",
    ]),
    projection:
      raw.projection === null ? null : selectObject(raw.projection, ["schema_version", "version", "state"]),
    applicationSnapshot:
      raw.applicationSnapshot === null
        ? null
        : selectObject(raw.applicationSnapshot, ["schema_version", "revision", "state", "state_redacted"]),
    audits: selectArray(raw.audits, [
      "sequence",
      "actor_kind",
      "actor_id",
      "action",
      "target_type",
      "target_id",
      "visibility",
      "owner_adult_id",
      "details",
      "created_at",
    ]),
  };
}

function assertScopedRows(value: unknown, predicate: (row: Record<string, unknown>) => boolean): void {
  if (!Array.isArray(value)) throw new Error("Customer export is malformed");
  for (const item of value) {
    if (!isRecord(item) || !predicate(item)) throw new Error("Customer export crossed scope");
  }
}

function selectArray(value: unknown, fields: readonly string[]): JsonValue[] {
  if (!Array.isArray(value)) throw new Error("Customer export is malformed");
  return value.map((item) => selectObject(item, fields));
}

function selectObject(value: unknown, fields: readonly string[]): JsonObject {
  if (!isRecord(value)) throw new Error("Customer export is malformed");
  const selected: Record<string, unknown> = {};
  for (const field of fields) {
    if (field in value) selected[field] = value[field];
  }
  return sanitizeJson(selected, new WeakSet(), 0) as JsonObject;
}

function sanitizeJson(value: unknown, seen: WeakSet<object>, depth: number): JsonValue {
  if (depth > 48) throw new Error("Customer export is too deeply nested");
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("Customer export number is invalid");
    return value;
  }
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return value.map((item) => sanitizeJson(item, seen, depth + 1));
  if (!isRecord(value) || seen.has(value)) throw new Error("Customer export object is invalid");
  seen.add(value);
  const result: JsonObject = {};
  for (const [key, item] of Object.entries(value)) {
    if (item === undefined || forbiddenExportKey(key)) continue;
    result[key] = sanitizeJson(item, seen, depth + 1);
  }
  seen.delete(value);
  return result;
}

function forbiddenExportKey(key: string): boolean {
  const normalized = key.toLowerCase().replace(/[^a-z0-9]/gu, "");
  return [
    "accesstoken",
    "refreshtoken",
    "idtoken",
    "apikey",
    "authorization",
    "credential",
    "encrypted",
    "modeltrace",
    "password",
    "privatekey",
    "rawpayload",
    "secret",
    "token",
  ].some((part) => normalized.includes(part));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
