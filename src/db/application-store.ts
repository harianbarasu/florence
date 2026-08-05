import { randomUUID } from "node:crypto";
import type { JSONValue, TransactionSql } from "postgres";
import { z } from "zod";
import {
  ApplicationAuditEntrySchema,
  ApplicationOutboxIntentSchema,
  ApplicationOutcomeSchema,
  ApplicationProjectionSchema,
  type ApplicationResult,
  ApplicationResultSchema,
  type HouseholdApplicationSnapshot,
  HouseholdApplicationSnapshotSchema,
  PrivateReviewItemSchema,
} from "../application/contracts.js";
import type {
  ApplicationCommit,
  ApplicationCommitResult,
  ApplicationRepositoryPort,
} from "../application/ports.js";
import {
  DomainChangeSchema,
  type HouseholdAggregate,
  HouseholdAggregateSchema,
  HouseholdSignalSchema,
} from "../domain/index.js";
import type { BlindIndex } from "../security/blind-index.js";
import { calendarBusyWindowCandidateBuckets } from "../security/calendar-busy-window-privacy.js";
import { canonicalJson, payloadDigest } from "../security/canonical-json.js";
import {
  adultIdentityDetailsContext,
  adultIdentityDetailsSchema,
  canonicalGoogleAccountAlias,
  googleAccountAliasKey,
  googleAccountAliasSchema,
  googleConnectionDetailsContext,
  googleConnectionDetailsSchema,
  normalizedEmail,
  REVOKED_GOOGLE_ACCOUNT_LABEL,
} from "../security/durable-identity-privacy.js";
import { privateReviewSummaryAad } from "../security/private-review.js";
import { isRawGoogleSource, rawGoogleSourceRetentionUntil } from "../security/raw-google-source-retention.js";
import type { SecretBox } from "../security/secret-box.js";
import type {
  EncryptionContext,
  EncryptionTenantKind,
  TenantJsonCipher,
} from "../security/tenant-json-cipher.js";
import type { Database } from "./client.js";

const jsonObjectSchema = z.record(z.string(), z.unknown());
const instantSchema = z.iso.datetime({ offset: true });
const queueErrorCodeSchema = z.string().regex(/^[a-z][a-z0-9_.-]{0,99}$/);
const leaseInputSchema = z.strictObject({
  owner: z.string().min(1).max(200),
  limit: z.number().int().positive().max(500),
  leaseSeconds: z.number().int().positive().max(86_400),
});

function toJsonValue(value: unknown): JSONValue {
  const serialized = JSON.stringify(value);
  if (serialized === undefined) {
    throw new Error("Database JSON value cannot be undefined");
  }
  return JSON.parse(serialized) as JSONValue;
}

function json(database: Database, value: unknown) {
  return database.json(toJsonValue(value));
}

function dateToString(value: Date): string {
  return value.toISOString();
}

function isGoogleSubjectOwnershipViolation(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "23505" &&
    "constraint_name" in error &&
    error.constraint_name === "external_connections_live_google_subject_idx"
  );
}

export class ApplicationStoreError extends Error {
  public constructor(
    public readonly code:
      | "not_found"
      | "not_authorized"
      | "external_account_in_use"
      | "external_account_alias_in_use"
      | "stale_projection_version"
      | "outbox_idempotency_conflict"
      | "invalid_state",
    message: string,
  ) {
    super(message);
    this.name = "ApplicationStoreError";
  }
}

const projectionTimerSchema = z.strictObject({
  timerKey: z.string().min(1).max(500),
  episodeKey: z.string().min(1).max(500).optional(),
  triggerKind: z.string().min(1).max(200),
  planVersion: z.number().int().nonnegative(),
  dueAt: instantSchema,
  payload: jsonObjectSchema,
  maxAttempts: z.number().int().positive().max(100).default(8),
});

const projectionOutboxSchema = z.strictObject({
  intentKey: z.string().min(1).max(500),
  effectKind: z.string().min(1).max(200),
  idempotencyKey: z.string().min(1).max(512),
  payload: jsonObjectSchema,
  maxAttempts: z.number().int().positive().max(100).default(8),
});

const projectionAuditSchema = z
  .strictObject({
    id: z.uuid().optional(),
    actorKind: z.string().min(1).max(200),
    actorId: z.string().min(1).max(500).optional(),
    action: z.string().min(1).max(200),
    targetType: z.string().min(1).max(200),
    targetId: z.string().min(1).max(500).optional(),
    visibility: z.enum(["personal", "household", "restricted"]).default("household"),
    ownerAdultId: z.uuid().optional(),
    sourceRefs: z.array(z.unknown()).max(500).default([]),
    policyRefs: z.array(z.unknown()).max(500).default([]),
    details: jsonObjectSchema,
  })
  .superRefine((value, context) => {
    if (value.visibility === "personal" && !value.ownerAdultId) {
      context.addIssue({
        code: "custom",
        path: ["ownerAdultId"],
        message: "Personal audit entries require an owner",
      });
    }
  });

const projectionCommitSchema = z
  .strictObject({
    householdId: z.uuid(),
    expectedVersion: z.number().int().nonnegative(),
    schemaVersion: z.number().int().positive(),
    nextState: jsonObjectSchema,
    processedSignalId: z.uuid().optional(),
    cancelTimerKeys: z.array(z.string().min(1).max(500)).max(1000).default([]),
    timers: z.array(projectionTimerSchema).max(1000).default([]),
    outbox: z.array(projectionOutboxSchema).max(1000).default([]),
    audits: z.array(projectionAuditSchema).max(1000).default([]),
  })
  .superRefine((value, context) => {
    const duplicateTimerKey = findDuplicate(value.timers.map((timer) => timer.timerKey));
    if (duplicateTimerKey) {
      context.addIssue({
        code: "custom",
        path: ["timers"],
        message: `Duplicate timer key: ${duplicateTimerKey}`,
      });
    }
    const duplicateIntentKey = findDuplicate(value.outbox.map((effect) => effect.intentKey));
    if (duplicateIntentKey) {
      context.addIssue({
        code: "custom",
        path: ["outbox"],
        message: `Duplicate outbox intent key: ${duplicateIntentKey}`,
      });
    }
    const duplicateIdempotencyKey = findDuplicate(value.outbox.map((effect) => effect.idempotencyKey));
    if (duplicateIdempotencyKey) {
      context.addIssue({
        code: "custom",
        path: ["outbox"],
        message: `Duplicate outbox idempotency key: ${duplicateIdempotencyKey}`,
      });
    }
  });

const applicationCommitSchema = z
  .strictObject({
    householdId: z.uuid(),
    idempotencyKey: z.string().min(1).max(512),
    expectedRevision: z.number().int().nonnegative(),
    aggregate: HouseholdAggregateSchema,
    projection: ApplicationProjectionSchema,
    signals: z.array(HouseholdSignalSchema).max(1000),
    changes: z.array(DomainChangeSchema).max(5000),
    outbox: z.array(ApplicationOutboxIntentSchema).max(1000),
    audit: z.array(ApplicationAuditEntrySchema).max(1000),
    privateReviewItems: z.array(PrivateReviewItemSchema).max(100),
    outcome: ApplicationOutcomeSchema,
  })
  .superRefine((value, context) => {
    if (value.aggregate.householdId !== value.householdId) {
      context.addIssue({
        code: "custom",
        path: ["aggregate", "householdId"],
        message: "Aggregate belongs to a different household",
      });
    }
    value.signals.forEach((signal, index) => {
      if (signal.householdId !== value.householdId) {
        context.addIssue({
          code: "custom",
          path: ["signals", index, "householdId"],
          message: "Signal belongs to a different household",
        });
      }
    });
    value.outbox.forEach((intent, index) => {
      if (intent.householdId !== value.householdId) {
        context.addIssue({
          code: "custom",
          path: ["outbox", index, "householdId"],
          message: "Outbox intent belongs to a different household",
        });
      }
    });
    const duplicateSignalId = findDuplicate(value.signals.map((signal) => signal.signalId));
    if (duplicateSignalId) {
      context.addIssue({
        code: "custom",
        path: ["signals"],
        message: `Duplicate signal ID: ${duplicateSignalId}`,
      });
    }
    const duplicateIntentId = findDuplicate(value.outbox.map((intent) => intent.intentId));
    if (duplicateIntentId) {
      context.addIssue({
        code: "custom",
        path: ["outbox"],
        message: `Duplicate application outbox intent ID: ${duplicateIntentId}`,
      });
    }
    const duplicateOutboxKey = findDuplicate(value.outbox.map((intent) => intent.idempotencyKey));
    if (duplicateOutboxKey) {
      context.addIssue({
        code: "custom",
        path: ["outbox"],
        message: `Duplicate application outbox idempotency key: ${duplicateOutboxKey}`,
      });
    }
    const duplicatePrivateReviewKey = findDuplicate(value.privateReviewItems.map((item) => item.itemKey));
    if (duplicatePrivateReviewKey) {
      context.addIssue({
        code: "custom",
        path: ["privateReviewItems"],
        message: `Duplicate private-review item key: ${duplicatePrivateReviewKey}`,
      });
    }
  });

function findDuplicate(values: readonly string[]): string | null {
  const seen = new Set<string>();
  for (const value of values) {
    if (seen.has(value)) return value;
    seen.add(value);
  }
  return null;
}

export class StaleProjectionVersionError extends ApplicationStoreError {
  public constructor(
    public readonly expected: number,
    public readonly actual: number,
  ) {
    super(
      "stale_projection_version",
      `Household projection changed while processing (expected ${expected}, found ${actual})`,
    );
    this.name = "StaleProjectionVersionError";
  }
}

export class OutboxIdempotencyConflictError extends ApplicationStoreError {
  public constructor(public readonly idempotencyKey: string) {
    super(
      "outbox_idempotency_conflict",
      `Outbox idempotency key was reused with different content: ${idempotencyKey}`,
    );
    this.name = "OutboxIdempotencyConflictError";
  }
}

export type ProviderInboxReceipt = {
  inboxId: string;
  disposition: "accepted" | "duplicate" | "quarantined";
  status: "pending" | "leased" | "resolved" | "quarantined" | "dead";
};

export type ClaimedProviderInboxItem = {
  id: string;
  provider: string;
  idempotencyKey: string;
  payloadHash: string;
  authentication: Record<string, unknown>;
  eventKind: string;
  occurredAt: string;
  payload: Record<string, unknown>;
  attempt: number;
  maxAttempts: number;
  leaseToken: string;
  leaseExpiresAt: string;
};

export type HouseholdProjection = {
  householdId: string;
  schemaVersion: number;
  version: number;
  state: Record<string, unknown>;
  updatedAt: string;
};

export type ChannelResolution = {
  bindingId: string;
  provider: "linq";
  channelType: "private" | "group";
  householdId: string;
  adultId: string | null;
  bindingStatus: "pending" | "active" | "paused" | "revoked";
  membershipStatus: "invited" | "active" | "revoked" | null;
  metadata: Record<string, unknown>;
};

export type ExternalConnectionRecord = {
  id: string;
  householdId: string;
  adultId: string;
  provider: "google";
  label: string;
  externalAccountId: string;
  email: string | null;
  encryptedCredentials: string | null;
  grantedScopes: string[];
  status: "active" | "reauth_required" | "revoked" | "error";
  cursor: Record<string, unknown>;
  metadata: Record<string, unknown>;
  lastSyncedAt: string | null;
};

export type SourceItemRecord = {
  id: string;
  householdId: string;
  connectionId: string | null;
  ownerAdultId: string | null;
  visibility: "personal" | "household";
  provider: string;
  externalId: string;
  kind: string;
  occurredAt: string;
  contentHash: string;
  encryptedContent: string | null;
  metadata: Record<string, unknown>;
  retentionUntil: string | null;
  revision: number;
};

export type ClaimedTimer = {
  rowId: string;
  timerKey: string;
  householdId: string;
  episodeKey: string | null;
  triggerKind: string;
  planVersion: number;
  dueAt: string;
  payload: Record<string, unknown>;
  attempt: number;
  maxAttempts: number;
  leaseToken: string;
  leaseExpiresAt: string;
};

export type ClaimedOutboxItem = {
  rowId: string;
  intentKey: string;
  householdId: string;
  effectKind: string;
  idempotencyKey: string;
  payload: Record<string, unknown>;
  attempt: number;
  maxAttempts: number;
  leaseToken: string;
  leaseExpiresAt: string;
};

export type ProjectionTimerIntent = {
  timerKey: string;
  episodeKey?: string;
  triggerKind: string;
  planVersion: number;
  dueAt: string;
  payload: Record<string, unknown>;
  maxAttempts?: number;
};

export type ProjectionOutboxIntent = {
  intentKey: string;
  effectKind: string;
  idempotencyKey: string;
  payload: Record<string, unknown>;
  maxAttempts?: number;
};

export type ProjectionAuditIntent = {
  id?: string;
  actorKind: string;
  actorId?: string;
  action: string;
  targetType: string;
  targetId?: string;
  visibility?: "personal" | "household" | "restricted";
  ownerAdultId?: string;
  sourceRefs?: unknown[];
  policyRefs?: unknown[];
  details: Record<string, unknown>;
};

type ProviderInboxRow = {
  id: string;
  provider: string;
  content_digest: string;
  encryption_tenant_kind: "household" | "provider_ingress";
  encryption_tenant_id: string;
  body_key_id: string;
  body_ciphertext: string;
  status: ProviderInboxReceipt["status"];
  attempt: number;
  max_attempts: number;
  lease_token: string;
  lease_expires_at: Date;
};

type ProjectionRow = {
  household_id: string;
  schema_version: number;
  version: string;
  state_key_id: string;
  state_ciphertext: string;
  updated_at: Date;
};

type ApplicationSnapshotRow = {
  household_id: string;
  schema_version: number;
  revision: string;
  application_phase: string;
  snapshot_key_id: string;
  snapshot_ciphertext: string;
  updated_at: Date;
};

type ApplicationCommitRow = {
  id: string;
  revision: string;
  content_digest: string;
  body_key_id: string;
  body_ciphertext: string;
};

const ingestProviderEventSchema = z.strictObject({
  id: z.uuid().optional(),
  provider: z.string().min(1).max(100),
  idempotencyKey: z.string().min(1).max(512),
  authentication: jsonObjectSchema,
  eventKind: z.string().min(1).max(200),
  occurredAt: instantSchema,
  payload: jsonObjectSchema,
  maxAttempts: z.number().int().positive().max(100).default(8),
});

const providerInboxBodySchema = z.strictObject({
  idempotencyKey: z.string().min(1).max(512),
  authentication: jsonObjectSchema,
  eventKind: z.string().min(1).max(200),
  occurredAt: instantSchema,
  payload: jsonObjectSchema,
  resolution: jsonObjectSchema.optional(),
});

const applicationCommitBodySchema = z.strictObject({
  idempotencyKey: z.string().min(1).max(512),
  outcome: ApplicationOutcomeSchema,
});

const onboardingSchema = z.strictObject({
  householdId: z.uuid().optional(),
  adultId: z.uuid().optional(),
  channelBindingId: z.uuid().optional(),
  householdName: z.string().trim().min(1).max(200),
  adultDisplayName: z.string().trim().min(1).max(200),
  timeZone: z.string().min(1).max(100),
  consent: z.discriminatedUnion("status", [
    z.strictObject({ status: z.literal("pending") }),
    z.strictObject({ status: z.literal("consented"), consentedAt: instantSchema }),
  ]),
  projectionSchemaVersion: z.number().int().positive(),
  initialProjection: jsonObjectSchema,
  privateChannel: z
    .strictObject({
      externalChatId: z.string().min(1).max(500),
      externalHandle: z.string().min(1).max(500),
      metadata: jsonObjectSchema.default({}),
    })
    .optional(),
});

const channelBindingSchema = z
  .strictObject({
    id: z.uuid().optional(),
    householdId: z.uuid(),
    adultId: z.uuid().optional(),
    provider: z.literal("linq"),
    channelType: z.enum(["private", "group"]),
    externalChatId: z.string().min(1).max(500),
    externalHandle: z.string().min(1).max(500).optional(),
    status: z.enum(["pending", "active", "paused", "revoked"]),
    metadata: jsonObjectSchema.default({}),
  })
  .superRefine((input, context) => {
    if (input.channelType === "private" && (!input.adultId || !input.externalHandle)) {
      context.addIssue({ code: "custom", message: "Private channels require adultId and externalHandle" });
    }
    if (input.channelType === "group" && (input.adultId || input.externalHandle)) {
      context.addIssue({
        code: "custom",
        message: "Group channels belong to the household, not one adult handle",
      });
    }
  });

const connectionSchema = z.strictObject({
  id: z.uuid().optional(),
  householdId: z.uuid(),
  adultId: z.uuid(),
  provider: z.literal("google"),
  label: googleAccountAliasSchema,
  externalAccountId: z.string().min(1).max(500),
  email: z.email().optional(),
  encryptedCredentials: z.string().min(1),
  grantedScopes: z.array(z.string().min(1).max(500)).max(100),
  cursor: jsonObjectSchema.default({}),
  metadata: jsonObjectSchema.default({}),
  lastSyncedAt: instantSchema.optional(),
});

const sourceItemSchema = z
  .strictObject({
    id: z.uuid().optional(),
    householdId: z.uuid(),
    connectionId: z.uuid().optional(),
    ownerAdultId: z.uuid().optional(),
    visibility: z.enum(["personal", "household"]),
    provider: z.string().min(1).max(100),
    externalId: z.string().min(1).max(1000),
    kind: z.string().min(1).max(200),
    occurredAt: instantSchema,
    contentHash: z.string().min(1).max(200),
    encryptedContent: z.string().min(1).optional(),
    metadata: jsonObjectSchema.default({}),
    retentionUntil: instantSchema.optional(),
  })
  .superRefine((input, context) => {
    if (input.visibility === "personal" && !input.ownerAdultId) {
      context.addIssue({ code: "custom", path: ["ownerAdultId"], message: "Personal sources need an owner" });
    }
    if (input.connectionId && !input.ownerAdultId) {
      context.addIssue({
        code: "custom",
        path: ["ownerAdultId"],
        message: "Connected sources must preserve their account owner",
      });
    }
  });

function gmailContentCompleteness(metadata: Record<string, unknown>): "metadata" | "full" | null {
  const parsed = z
    .object({
      schemaVersion: z.literal(2),
      contentCompleteness: z.enum(["metadata", "full"]),
    })
    .passthrough()
    .safeParse(metadata);
  return parsed.success ? parsed.data.contentCompleteness : null;
}

function gmailMessageHistoryId(metadata: Record<string, unknown>): bigint | null {
  const value = metadata.messageHistoryId;
  return typeof value === "string" && /^\d+$/u.test(value) ? BigInt(value) : null;
}

function mergedGmailDiscoveryMetadata(
  existing: Record<string, unknown>,
  incoming: Record<string, unknown>,
): Record<string, unknown> {
  const merged: Record<string, unknown> = { ...existing, contentCompleteness: "full" };
  for (const key of ["discoveryMode", "discoveryHistoryId", "providerEventIds"] as const) {
    if (incoming[key] !== undefined) merged[key] = incoming[key];
  }
  return merged;
}

const calendarApplicationDigestSchema = z.string().regex(/^sha256:[a-f0-9]{64}$/u);
const calendarApprovedActionIdSchema = z
  .string()
  .min(1)
  .max(500)
  .refine((value) => value.trim() === value);

type CalendarEchoGuard = {
  applicationContentDigest: string;
  createdByApprovedActionId: string;
};

function calendarEchoGuard(metadata: Record<string, unknown>): CalendarEchoGuard | null {
  const parsed = z
    .object({
      applicationContentDigest: calendarApplicationDigestSchema,
      createdByApprovedActionId: calendarApprovedActionIdSchema,
    })
    .passthrough()
    .safeParse(metadata);
  return parsed.success
    ? {
        applicationContentDigest: parsed.data.applicationContentDigest,
        createdByApprovedActionId: parsed.data.createdByApprovedActionId,
      }
    : null;
}

function calendarSourceMetadata(
  kind: string,
  incoming: Record<string, unknown>,
  existing?: { kind: string; metadata: Record<string, unknown> },
): Record<string, unknown> {
  const metadata = { ...incoming };
  delete metadata.createdByApprovedActionId;
  const digest = calendarApplicationDigestSchema.safeParse(metadata.applicationContentDigest);
  if (kind !== "calendar_event" || !digest.success) {
    delete metadata.applicationContentDigest;
    return metadata;
  }
  const requestedGuard = calendarEchoGuard(incoming);
  const retainedGuard =
    requestedGuard?.applicationContentDigest === digest.data
      ? requestedGuard
      : existing?.kind === "calendar_event"
        ? calendarEchoGuard(existing.metadata)
        : null;
  if (retainedGuard?.applicationContentDigest === digest.data) {
    metadata.createdByApprovedActionId = retainedGuard.createdByApprovedActionId;
  }
  return metadata;
}

function calendarEchoReceipt(provider: string, metadata: Record<string, unknown>) {
  if (provider !== "google-calendar") return {};
  return {
    createdByApprovedActionId: calendarEchoGuard(metadata)?.createdByApprovedActionId ?? null,
  };
}

export class ApplicationStore implements ApplicationRepositoryPort {
  public constructor(
    private readonly database: Database,
    private readonly privateReviewSecrets: SecretBox,
    private readonly sensitiveJson: TenantJsonCipher,
    private readonly blindIndex: BlindIndex,
  ) {}

  /** Returns keyed coarse candidates; exact calendar time never enters a query predicate. */
  public calendarBusyWindowCandidateBuckets(input: {
    householdId: string;
    startsAt: string;
    endsAt: string;
  }): string[] {
    const parsed = z
      .strictObject({ householdId: z.uuid(), startsAt: instantSchema, endsAt: instantSchema })
      .parse(input);
    return calendarBusyWindowCandidateBuckets(
      this.blindIndex,
      parsed.householdId,
      parsed.startsAt,
      parsed.endsAt,
    );
  }

  public googleConnectionEmailDigest(email: string): string {
    return this.blindIndex.digest("google-connection-email", normalizedEmail(email));
  }

  public async initializeApplicationSnapshot(input: {
    schemaVersion?: number;
    snapshot: HouseholdApplicationSnapshot;
  }): Promise<{ created: boolean; snapshot: HouseholdApplicationSnapshot }> {
    const parsed = z
      .strictObject({
        schemaVersion: z.number().int().positive().default(1),
        snapshot: HouseholdApplicationSnapshotSchema,
      })
      .parse(input);
    const householdId = z.uuid().parse(parsed.snapshot.aggregate.householdId);
    const sealed = this.sensitiveJson.seal(
      parsed.snapshot,
      encryptedJsonContext("household", householdId, "application_snapshots", householdId, "snapshot"),
    );
    return this.database.begin(async (transaction) => {
      const households = await transaction<{ id: string; status: string }[]>`
        select id, status from households where id = ${householdId} for update
      `;
      if (!households[0]) {
        throw new ApplicationStoreError("not_found", "Unknown household");
      }
      if (households[0].status === "deleting") {
        throw new ApplicationStoreError("invalid_state", "Household deletion is fenced");
      }
      const inserted = await transaction<{ household_id: string }[]>`
        insert into application_snapshots (
          household_id, schema_version, revision, application_phase,
          snapshot_key_id, snapshot_ciphertext
        ) values (
          ${householdId}, ${parsed.schemaVersion}, ${parsed.snapshot.revision},
          ${parsed.snapshot.projection.onboarding.phase}, ${sealed.keyId}, ${sealed.ciphertext}
        )
        on conflict (household_id) do nothing
        returning household_id
      `;
      const rows = await transaction<ApplicationSnapshotRow[]>`
        select household_id, schema_version, revision, application_phase,
          snapshot_key_id, snapshot_ciphertext, updated_at
        from application_snapshots where household_id = ${householdId}
      `;
      const snapshot = rows[0];
      if (!snapshot) {
        throw new ApplicationStoreError("invalid_state", "Application snapshot disappeared");
      }
      const mapped = mapApplicationSnapshot(snapshot, this.sensitiveJson);
      if (inserted.length === 0 && payloadDigest(mapped) !== payloadDigest(parsed.snapshot)) {
        throw new ApplicationStoreError(
          "invalid_state",
          "Application snapshot is already initialized with different content",
        );
      }
      return { created: inserted.length === 1, snapshot: mapped };
    });
  }

  public async load(householdId: string): Promise<HouseholdApplicationSnapshot | null> {
    const parsedId = z.uuid().parse(householdId);
    const rows = await this.database<ApplicationSnapshotRow[]>`
      select household_id, schema_version, revision, application_phase,
        snapshot_key_id, snapshot_ciphertext, updated_at
      from application_snapshots where household_id = ${parsedId}
    `;
    return rows[0] ? mapApplicationSnapshot(rows[0], this.sensitiveJson) : null;
  }

  public async loadInTransaction(
    transaction: TransactionSql<Record<string, never>>,
    householdId: string,
    lock: "none" | "update" = "none",
  ): Promise<HouseholdApplicationSnapshot | null> {
    const parsedId = z.uuid().parse(householdId);
    const rows =
      lock === "update"
        ? await transaction<ApplicationSnapshotRow[]>`
            select household_id, schema_version, revision, application_phase,
              snapshot_key_id, snapshot_ciphertext, updated_at
            from application_snapshots where household_id = ${parsedId} for update
          `
        : await transaction<ApplicationSnapshotRow[]>`
            select household_id, schema_version, revision, application_phase,
              snapshot_key_id, snapshot_ciphertext, updated_at
            from application_snapshots where household_id = ${parsedId}
          `;
    return rows[0] ? mapApplicationSnapshot(rows[0], this.sensitiveJson) : null;
  }

  public async findProcessed(householdId: string, idempotencyKey: string): Promise<ApplicationResult | null> {
    const parsedHouseholdId = z.uuid().parse(householdId);
    const parsedKey = z.string().min(1).max(512).parse(idempotencyKey);
    const digest = this.blindIndex.digest("application-idempotency", `${parsedHouseholdId}\0${parsedKey}`);
    const rows = await this.database<ApplicationCommitRow[]>`
      select id, revision, content_digest, body_key_id, body_ciphertext
      from application_commits
      where household_id = ${parsedHouseholdId} and idempotency_digest = ${digest}
    `;
    const row = rows[0];
    const body = row ? openApplicationCommitBody(row, parsedHouseholdId, this.sensitiveJson) : null;
    return row
      ? ApplicationResultSchema.parse({
          householdId: parsedHouseholdId,
          idempotencyKey: parsedKey,
          disposition: "committed",
          revision: Number(row.revision),
          outcome: body?.outcome,
        })
      : null;
  }

  public async commit(input: ApplicationCommit): Promise<ApplicationCommitResult> {
    const parsed = applicationCommitSchema.parse(input);
    const idempotencyDigest = this.blindIndex.digest(
      "application-idempotency",
      `${parsed.householdId}\0${parsed.idempotencyKey}`,
    );
    const contentDigest = this.blindIndex.digest(
      "application-content",
      canonicalJson({
        expectedRevision: parsed.expectedRevision,
        aggregate: parsed.aggregate,
        projection: parsed.projection,
        signals: parsed.signals,
        changes: parsed.changes,
        outbox: parsed.outbox,
        audit: parsed.audit,
        privateReviewItems: parsed.privateReviewItems,
        outcome: parsed.outcome,
      }),
    );

    return this.database.begin(async (transaction) => {
      const currentRows = await transaction<
        { revision: string; next_audit_sequence: string; household_status: string }[]
      >`
        select application_snapshots.revision, households.next_audit_sequence,
          households.status as household_status
        from application_snapshots
        join households on households.id = application_snapshots.household_id
        where application_snapshots.household_id = ${parsed.householdId}
        for update of application_snapshots, households
      `;
      const current = currentRows[0];
      if (!current) {
        throw new ApplicationStoreError("not_found", "Household application is not initialized");
      }

      const priorRows = await transaction<ApplicationCommitRow[]>`
        select id, revision, content_digest, body_key_id, body_ciphertext
        from application_commits
        where household_id = ${parsed.householdId}
          and idempotency_digest = ${idempotencyDigest}
      `;
      const prior = priorRows[0];
      if (prior) {
        if (prior.content_digest !== contentDigest) {
          throw new ApplicationStoreError(
            "invalid_state",
            "Application idempotency key was reused with different commit content",
          );
        }
        return {
          disposition: "duplicate" as const,
          revision: Number(prior.revision),
          outcome: openApplicationCommitBody(prior, parsed.householdId, this.sensitiveJson).outcome,
        };
      }

      if (current.household_status === "deleting") {
        throw new ApplicationStoreError("invalid_state", "Household deletion is fenced");
      }

      const currentRevision = Number(current.revision);
      if (currentRevision !== parsed.expectedRevision) {
        return {
          disposition: "conflict" as const,
          actualRevision: currentRevision,
        };
      }

      const revision = parsed.expectedRevision + 1;
      const sealedSnapshot = this.sensitiveJson.seal(
        { revision, aggregate: parsed.aggregate, projection: parsed.projection },
        encryptedJsonContext(
          "household",
          parsed.householdId,
          "application_snapshots",
          parsed.householdId,
          "snapshot",
        ),
      );
      await transaction`
        update application_snapshots
        set revision = ${revision}, application_phase = ${parsed.projection.onboarding.phase},
          snapshot_key_id = ${sealedSnapshot.keyId},
          snapshot_ciphertext = ${sealedSnapshot.ciphertext}, updated_at = now()
        where household_id = ${parsed.householdId}
      `;

      // The application projection is authoritative for the names each adult asked Florence to use.
      // Keep the identity directory in the same transaction so invitations, exports, and operator
      // views never continue showing onboarding placeholders after a successful naming turn.
      for (const adult of parsed.projection.onboarding.adultNames) {
        const details = this.sensitiveJson.seal(
          adultIdentityDetailsSchema.parse({ displayName: adult.displayName }),
          adultIdentityDetailsContext({ householdId: parsed.householdId, adultId: adult.adultId }),
        );
        await transaction`
          update adult_identity_details details
          set details_key_id = ${details.keyId}, details_ciphertext = ${details.ciphertext}, updated_at = now()
          where details.household_id = ${parsed.householdId} and details.adult_id = ${adult.adultId}
            and exists (
              select 1 from household_memberships membership
              where membership.household_id = details.household_id and membership.adult_id = details.adult_id
                and membership.status in ('invited', 'active')
            )
        `;
      }

      for (const intent of parsed.outbox) {
        await insertOutboxIntent(transaction, this.sensitiveJson, parsed.householdId, {
          intentKey: intent.intentId,
          effectKind: intent.kind,
          idempotencyKey: intent.idempotencyKey,
          payload: intent,
          maxAttempts: 8,
        });
      }

      for (const item of parsed.privateReviewItems) {
        const activeOwner = await transaction<{ adult_id: string }[]>`
          select adult_id from household_memberships
          where household_id = ${parsed.householdId} and adult_id = ${item.adultId}
            and status = 'active'
        `;
        if (activeOwner.length !== 1) {
          throw new ApplicationStoreError(
            "not_authorized",
            "Private-review item owner is not an active household adult",
          );
        }
        const itemDigest = payloadDigest(item);
        const inserted = await transaction<{ id: string }[]>`
          insert into private_review_items (
            id, household_id, adult_id, item_key, source, summary_ciphertext, observed_at, payload_digest
          ) values (
            ${randomUUID()}, ${parsed.householdId}, ${item.adultId}, ${item.itemKey},
            ${item.source}, ${this.privateReviewSecrets.seal(
              item.summary,
              privateReviewSummaryAad({
                householdId: parsed.householdId,
                adultId: item.adultId,
                itemKey: item.itemKey,
              }),
            )}, ${item.observedAt}, ${itemDigest}
          )
          on conflict (household_id, item_key) do nothing
          returning id
        `;
        if (inserted.length === 0) {
          const existing = await transaction<{ payload_digest: string }[]>`
            select payload_digest from private_review_items
            where household_id = ${parsed.householdId} and item_key = ${item.itemKey}
          `;
          if (existing[0]?.payload_digest !== itemDigest) {
            throw new ApplicationStoreError(
              "invalid_state",
              "Private-review item key was reused with different content",
            );
          }
        }
      }

      let auditSequence = Number(current.next_audit_sequence);
      for (const audit of parsed.audit) {
        await insertAudit(transaction, this.database, parsed.householdId, auditSequence, {
          actorKind: audit.adultId ? "adult" : "application",
          ...(audit.adultId ? { actorId: audit.adultId } : {}),
          action: audit.kind,
          targetType: "application_commit",
          targetId: parsed.idempotencyKey,
          visibility: audit.containsPrivateData ? (audit.adultId ? "personal" : "restricted") : "household",
          ...(audit.containsPrivateData && audit.adultId ? { ownerAdultId: audit.adultId } : {}),
          sourceRefs: audit.sourceRef ? [{ sourceRef: audit.sourceRef }] : [],
          policyRefs: [],
          details: {
            occurredAt: audit.occurredAt,
            decision: audit.decision,
            containsPrivateData: audit.containsPrivateData,
          },
        });
        auditSequence += 1;
      }

      const commitId = randomUUID();
      const sealedCommit = this.sensitiveJson.seal(
        {
          idempotencyKey: parsed.idempotencyKey,
          outcome: parsed.outcome,
        },
        encryptedJsonContext("household", parsed.householdId, "application_commits", commitId, "body"),
      );
      await transaction`
        insert into application_commits (
          id, household_id, idempotency_digest, content_digest, base_revision, revision,
          body_key_id, body_ciphertext
        ) values (
          ${commitId}, ${parsed.householdId}, ${idempotencyDigest}, ${contentDigest},
          ${parsed.expectedRevision}, ${revision}, ${sealedCommit.keyId}, ${sealedCommit.ciphertext}
        )
      `;
      await transaction`
        update households
        set next_audit_sequence = ${auditSequence},
          status = case
            when ${parsed.projection.onboarding.phase} = 'active' and status = 'onboarding'
              then 'active'
            else status
          end,
          updated_at = now()
        where id = ${parsed.householdId}
      `;
      return { disposition: "committed" as const, revision, outcome: parsed.outcome };
    });
  }

  public async ingestProviderEvent(
    rawInput: z.input<typeof ingestProviderEventSchema>,
  ): Promise<ProviderInboxReceipt> {
    const input = ingestProviderEventSchema.parse(rawInput);
    const inboxId = input.id ?? randomUUID();
    const idempotencyDigest = this.blindIndex.digest(
      "provider-idempotency",
      `${input.provider}\0${input.idempotencyKey}`,
    );
    const contentDigest = this.blindIndex.digest(
      "provider-content",
      canonicalJson({
        provider: input.provider,
        eventKind: input.eventKind,
        occurredAt: input.occurredAt,
        payload: input.payload,
      }),
    );
    const tenantId = providerIngressTenantId(input, this.blindIndex);
    const routingDigests = providerRoutingDigests(input, this.blindIndex);
    const sealed = this.sensitiveJson.seal(
      {
        idempotencyKey: input.idempotencyKey,
        authentication: input.authentication,
        eventKind: input.eventKind,
        occurredAt: input.occurredAt,
        payload: input.payload,
      },
      encryptedJsonContext("provider_ingress", tenantId, "provider_inbox", inboxId, "body"),
    );

    return this.database.begin(async (transaction) => {
      const inserted = await transaction<{ id: string; status: ProviderInboxReceipt["status"] }[]>`
        insert into provider_inbox (
          id, provider, idempotency_digest, content_digest, routing_digests,
          encryption_tenant_kind, encryption_tenant_id, body_key_id, body_ciphertext,
          status, max_attempts
        ) values (
          ${inboxId}, ${input.provider}, ${idempotencyDigest}, ${contentDigest},
          ${routingDigests}, 'provider_ingress', ${tenantId}, ${sealed.keyId},
          ${sealed.ciphertext}, 'pending', ${input.maxAttempts}
        )
        on conflict (provider, idempotency_digest) do nothing
        returning id, status
      `;
      if (inserted[0]) {
        return { inboxId: inserted[0].id, disposition: "accepted", status: inserted[0].status };
      }

      const existingRows = await transaction<
        { id: string; content_digest: string; status: ProviderInboxReceipt["status"] }[]
      >`
        select id, content_digest, status
        from provider_inbox
        where provider = ${input.provider} and idempotency_digest = ${idempotencyDigest}
        for update
      `;
      const existing = existingRows[0];
      if (!existing) {
        throw new ApplicationStoreError("invalid_state", "Provider inbox conflict row disappeared");
      }
      if (existing.content_digest === contentDigest) {
        return { inboxId: existing.id, disposition: "duplicate", status: existing.status };
      }

      await transaction`
        update provider_inbox
        set status = 'quarantined', quarantine_reason = 'idempotency_hash_conflict',
            lease_owner = null, lease_token = null, lease_expires_at = null,
            body_key_id = null, body_ciphertext = null, updated_at = now()
        where id = ${existing.id}
      `;
      const conflictId = randomUUID();
      await transaction`
        insert into provider_inbox_conflicts (
          id, inbox_id, content_digest, encryption_tenant_kind, encryption_tenant_id,
          body_key_id, body_ciphertext
        ) values (
          ${conflictId}, ${existing.id}, ${contentDigest}, 'provider_ingress', ${tenantId},
          null, null
        )
        on conflict (inbox_id, content_digest) do nothing
      `;
      return { inboxId: existing.id, disposition: "quarantined", status: "quarantined" };
    });
  }

  public async claimProviderInbox(input: {
    owner: string;
    limit: number;
    leaseSeconds: number;
  }): Promise<ClaimedProviderInboxItem[]> {
    const parsed = leaseInputSchema.parse(input);
    const leaseToken = randomUUID();
    return this.database.begin(async (transaction) => {
      await transaction`
        with exhausted as (
          select id from provider_inbox
          where status = 'leased' and lease_expires_at < now() and attempt >= max_attempts
          for update skip locked
        )
        update provider_inbox
        set status = 'dead', lease_owner = null, lease_token = null,
          lease_expires_at = null, body_key_id = null, body_ciphertext = null,
          last_error_code = 'lease_expired_after_max_attempts',
          updated_at = now()
        from exhausted where provider_inbox.id = exhausted.id
      `;
      const rows = await transaction<ProviderInboxRow[]>`
        with candidates as (
          select inbox.id
          from provider_inbox inbox
          left join households household on household.id = inbox.household_id
          where (
            (inbox.status = 'pending' and inbox.available_at <= now())
            or (inbox.status = 'leased' and inbox.lease_expires_at < now())
          )
            and (inbox.household_id is null or household.status <> 'deleting')
          order by inbox.available_at, inbox.received_at
          for update of inbox skip locked
          limit ${parsed.limit}
        )
        update provider_inbox
        set status = 'leased', lease_owner = ${parsed.owner}, lease_token = ${leaseToken},
            lease_expires_at = now() + (${parsed.leaseSeconds} * interval '1 second'),
            attempt = attempt + 1, updated_at = now()
        from candidates
        where provider_inbox.id = candidates.id
        returning provider_inbox.id, provider_inbox.provider, provider_inbox.content_digest,
          provider_inbox.encryption_tenant_kind, provider_inbox.encryption_tenant_id,
          provider_inbox.body_key_id, provider_inbox.body_ciphertext, provider_inbox.status,
          provider_inbox.attempt, provider_inbox.max_attempts, provider_inbox.lease_token,
          provider_inbox.lease_expires_at
      `;
      return rows.map((row) => {
        const body = openProviderInboxBody(row, this.sensitiveJson);
        return {
          id: row.id,
          provider: row.provider,
          idempotencyKey: body.idempotencyKey,
          payloadHash: row.content_digest,
          authentication: body.authentication,
          eventKind: body.eventKind,
          occurredAt: body.occurredAt,
          payload: body.payload,
          attempt: row.attempt,
          maxAttempts: row.max_attempts,
          leaseToken: row.lease_token,
          leaseExpiresAt: dateToString(row.lease_expires_at),
        };
      });
    });
  }

  public async renewProviderInboxLease(input: {
    inboxId: string;
    leaseToken: string;
    leaseSeconds: number;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        inboxId: z.uuid(),
        leaseToken: z.uuid(),
        leaseSeconds: z.number().int().positive().max(86_400),
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update provider_inbox
      set lease_expires_at = now() + (${parsed.leaseSeconds} * interval '1 second'),
          updated_at = now()
      where id = ${parsed.inboxId} and status = 'leased'
        and lease_token = ${parsed.leaseToken} and lease_expires_at > now()
      returning id
    `;
    return rows.length === 1;
  }

  public async resolveProviderInbox(input: {
    inboxId: string;
    leaseToken: string;
    householdId?: string;
    resolution: Record<string, unknown>;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        inboxId: z.uuid(),
        leaseToken: z.uuid(),
        householdId: z.uuid().optional(),
        resolution: jsonObjectSchema,
      })
      .parse(input);
    return this.database.begin(async (transaction) => {
      const rows = await transaction<ProviderInboxRow[]>`
        select id, provider, content_digest, encryption_tenant_kind, encryption_tenant_id,
          body_key_id, body_ciphertext, status, attempt, max_attempts, lease_token,
          lease_expires_at
        from provider_inbox
        where id = ${parsed.inboxId} and status = 'leased' and lease_token = ${parsed.leaseToken}
          and lease_expires_at > now()
        for update
      `;
      const row = rows[0];
      if (!row) return false;
      const updated = await transaction<{ id: string }[]>`
        update provider_inbox
        set status = 'resolved', household_id = ${parsed.householdId ?? null},
          body_key_id = null, body_ciphertext = null,
          resolved_at = now(), lease_owner = null, lease_token = null,
          lease_expires_at = null, updated_at = now()
        where id = ${parsed.inboxId} and status = 'leased' and lease_token = ${parsed.leaseToken}
          and lease_expires_at > now()
        returning id
      `;
      return updated.length === 1;
    });
  }

  public async failProviderInbox(input: {
    inboxId: string;
    leaseToken: string;
    errorCode: string;
    safeDetail?: string;
    retryAfterSeconds: number;
  }): Promise<"pending" | "dead" | "lost_lease"> {
    const parsed = z
      .strictObject({
        inboxId: z.uuid(),
        leaseToken: z.uuid(),
        errorCode: z.string().min(1).max(200),
        safeDetail: z.string().max(2000).optional(),
        retryAfterSeconds: z.number().int().nonnegative().max(604_800),
      })
      .parse(input);
    const rows = await this.database<{ status: "pending" | "dead" }[]>`
      update provider_inbox
      set status = case when attempt >= max_attempts then 'dead' else 'pending' end,
          available_at = now() + (${parsed.retryAfterSeconds} * interval '1 second'),
          lease_owner = null, lease_token = null, lease_expires_at = null,
          last_error_code = ${parsed.errorCode}, last_error_detail = ${parsed.safeDetail ?? null},
          body_key_id = case when attempt >= max_attempts then null else body_key_id end,
          body_ciphertext = case when attempt >= max_attempts then null else body_ciphertext end,
          updated_at = now()
      where id = ${parsed.inboxId} and status = 'leased' and lease_token = ${parsed.leaseToken}
        and lease_expires_at > now()
      returning status
    `;
    return rows[0]?.status ?? "lost_lease";
  }

  public async onboardFoundingAdult(
    rawInput: z.input<typeof onboardingSchema>,
  ): Promise<{ householdId: string; adultId: string; channelBindingId: string | null }> {
    const input = onboardingSchema.parse(rawInput);
    const householdId = input.householdId ?? randomUUID();
    const adultId = input.adultId ?? randomUUID();
    const channelBindingId = input.privateChannel ? (input.channelBindingId ?? randomUUID()) : null;
    const membershipStatus = input.consent.status === "consented" ? "active" : "invited";
    const consentedAt = input.consent.status === "consented" ? input.consent.consentedAt : null;
    const channelStatus = input.consent.status === "consented" ? "active" : "pending";
    const initialProjectionSealed = this.sensitiveJson.seal(
      input.initialProjection,
      encryptedJsonContext("household", householdId, "household_projections", householdId, "state"),
    );

    await this.database.begin(async (transaction) => {
      await transaction`
        insert into households (id, name, timezone, status, version)
        values (${householdId}, ${input.householdName}, ${input.timeZone}, 'onboarding', 0)
      `;
      await transaction`insert into adults (id, timezone) values (${adultId}, ${input.timeZone})`;
      await transaction`
        insert into household_memberships (household_id, adult_id, role, status, consented_at)
        values (
          ${householdId}, ${adultId}, 'owner', ${membershipStatus}, ${consentedAt}
        )
      `;
      const adultDetails = this.sensitiveJson.seal(
        adultIdentityDetailsSchema.parse({ displayName: input.adultDisplayName }),
        adultIdentityDetailsContext({ householdId, adultId }),
      );
      await transaction`
        insert into adult_identity_details (household_id, adult_id, details_key_id, details_ciphertext)
        values (${householdId}, ${adultId}, ${adultDetails.keyId}, ${adultDetails.ciphertext})
      `;
      await transaction`
        insert into household_projections (
          household_id, schema_version, version, state_key_id, state_ciphertext
        )
        values (
          ${householdId}, ${input.projectionSchemaVersion}, 0,
          ${initialProjectionSealed.keyId}, ${initialProjectionSealed.ciphertext}
        )
      `;
      if (input.privateChannel && channelBindingId) {
        await transaction`
          insert into channel_bindings (
            id, household_id, adult_id, provider, channel_type, external_chat_id,
            external_handle, status, metadata
          ) values (
            ${channelBindingId}, ${householdId}, ${adultId}, 'linq', 'private',
            ${input.privateChannel.externalChatId}, ${input.privateChannel.externalHandle},
            ${channelStatus},
            ${json(this.database, input.privateChannel.metadata)}
          )
        `;
      }
    });
    return { householdId, adultId, channelBindingId };
  }

  public async addAdultMembership(input: {
    householdId: string;
    adultId?: string;
    displayName: string;
    timeZone?: string;
    role?: "owner" | "adult";
    status: "invited" | "active";
    consentedAt?: string;
  }): Promise<{ adultId: string }> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid().optional(),
        displayName: z.string().trim().min(1).max(200),
        timeZone: z.string().min(1).max(100).optional(),
        role: z.enum(["owner", "adult"]).default("adult"),
        status: z.enum(["invited", "active"]),
        consentedAt: instantSchema.optional(),
      })
      .superRefine((value, context) => {
        if (value.status === "active" && !value.consentedAt) {
          context.addIssue({ code: "custom", message: "Active adults require consent" });
        }
        if (value.status === "invited" && value.consentedAt) {
          context.addIssue({ code: "custom", message: "Invited adults cannot be pre-consented" });
        }
      })
      .parse(input);
    const adultId = parsed.adultId ?? randomUUID();
    await this.database.begin(async (transaction) => {
      const households = await transaction<{ id: string }[]>`
        select id from households where id = ${parsed.householdId} for update
      `;
      if (!households[0]) {
        throw new ApplicationStoreError("not_found", "Unknown household");
      }
      await transaction`insert into adults (id, timezone) values (${adultId}, ${parsed.timeZone ?? null})`;
      await transaction`
        insert into household_memberships (
          household_id, adult_id, role, status, consented_at
        ) values (
          ${parsed.householdId}, ${adultId}, ${parsed.role}, ${parsed.status},
          ${parsed.consentedAt ?? null}
        )
      `;
      const adultDetails = this.sensitiveJson.seal(
        adultIdentityDetailsSchema.parse({ displayName: parsed.displayName }),
        adultIdentityDetailsContext({ householdId: parsed.householdId, adultId }),
      );
      await transaction`
        insert into adult_identity_details (household_id, adult_id, details_key_id, details_ciphertext)
        values (${parsed.householdId}, ${adultId}, ${adultDetails.keyId}, ${adultDetails.ciphertext})
      `;
    });
    return { adultId };
  }

  public async activateAdultMembership(input: {
    householdId: string;
    adultId: string;
    consentedAt: string;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({ householdId: z.uuid(), adultId: z.uuid(), consentedAt: instantSchema })
      .parse(input);
    const rows = await this.database<{ adult_id: string }[]>`
      update household_memberships
      set status = 'active', consented_at = ${parsed.consentedAt}, updated_at = now()
      where household_id = ${parsed.householdId} and adult_id = ${parsed.adultId}
        and status = 'invited'
      returning adult_id
    `;
    return rows.length === 1;
  }

  public async upsertChannelBinding(
    rawInput: z.input<typeof channelBindingSchema>,
  ): Promise<{ bindingId: string }> {
    const input = channelBindingSchema.parse(rawInput);
    const bindingId = input.id ?? randomUUID();
    return this.database.begin(async (transaction) => {
      const memberships = await transaction<{ status: string }[]>`
        select status
        from household_memberships
        where household_id = ${input.householdId}
          and (${input.adultId ?? null}::uuid is null or adult_id = ${input.adultId ?? null})
      `;
      if (memberships.length === 0) {
        throw new ApplicationStoreError("not_authorized", "Channel identity is not in the household");
      }
      if (input.status === "active" && !memberships.some((membership) => membership.status === "active")) {
        throw new ApplicationStoreError(
          "not_authorized",
          "An active channel requires an active household membership",
        );
      }
      if (input.status !== "revoked" && memberships.every((membership) => membership.status === "revoked")) {
        throw new ApplicationStoreError(
          "not_authorized",
          "A revoked household identity cannot own a live channel",
        );
      }
      await lockApplicationIdentity(transaction, "channel_binding", {
        provider: input.provider,
        channelType: input.channelType,
        externalChatId: input.externalChatId,
        externalHandle: input.externalHandle ?? null,
      });

      const existing = await transaction<
        { id: string; household_id: string | null; adult_id: string | null }[]
      >`
        select id, household_id, adult_id
        from channel_bindings
        where provider = ${input.provider}
          and channel_type = ${input.channelType}
          and external_chat_id = ${input.externalChatId}
          and external_handle is not distinct from ${input.externalHandle ?? null}
          and status <> 'revoked'
        for update
      `;
      if (existing[0]) {
        if (
          existing[0].household_id !== input.householdId ||
          existing[0].adult_id !== (input.adultId ?? null)
        ) {
          throw new ApplicationStoreError(
            "not_authorized",
            "External channel identity is already bound to a different household identity",
          );
        }
        await transaction`
          update channel_bindings
          set status = ${input.status}, metadata = ${json(this.database, input.metadata)},
              updated_at = now()
          where id = ${existing[0].id}
        `;
        return { bindingId: existing[0].id };
      }

      await transaction`
        insert into channel_bindings (
          id, household_id, adult_id, provider, channel_type, external_chat_id,
          external_handle, status, metadata
        ) values (
          ${bindingId}, ${input.householdId}, ${input.adultId ?? null}, ${input.provider},
          ${input.channelType}, ${input.externalChatId}, ${input.externalHandle ?? null},
          ${input.status}, ${json(this.database, input.metadata)}
        )
      `;
      return { bindingId };
    });
  }

  public async resolveChannel(input: {
    provider: "linq";
    externalChatId: string;
    externalHandle?: string;
  }): Promise<ChannelResolution | null> {
    const parsed = z
      .strictObject({
        provider: z.literal("linq"),
        externalChatId: z.string().min(1).max(500),
        externalHandle: z.string().min(1).max(500).optional(),
      })
      .parse(input);
    const rows = await this.database<
      {
        id: string;
        provider: "linq";
        channel_type: "private" | "group";
        household_id: string;
        adult_id: string | null;
        status: ChannelResolution["bindingStatus"];
        membership_status: ChannelResolution["membershipStatus"];
        metadata: Record<string, unknown>;
      }[]
    >`
      select cb.id, cb.provider, cb.channel_type, cb.household_id, cb.adult_id,
        cb.status, hm.status as membership_status, cb.metadata
      from channel_bindings cb
      left join household_memberships hm
        on hm.household_id = cb.household_id and hm.adult_id = cb.adult_id
      where cb.provider = ${parsed.provider}
        and cb.external_chat_id = ${parsed.externalChatId}
        and cb.external_handle is not distinct from ${parsed.externalHandle ?? null}
        and cb.status <> 'revoked'
      limit 1
    `;
    const row = rows[0];
    if (!row) return null;
    return {
      bindingId: row.id,
      provider: row.provider,
      channelType: row.channel_type,
      householdId: row.household_id,
      adultId: row.adult_id,
      bindingStatus: row.status,
      membershipStatus: row.membership_status,
      metadata: row.metadata,
    };
  }

  public async initializeHouseholdProjection(input: {
    householdId: string;
    schemaVersion: number;
    version?: number;
    state: Record<string, unknown>;
  }): Promise<{ created: boolean; projection: HouseholdProjection }> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        schemaVersion: z.number().int().positive(),
        version: z.number().int().nonnegative().default(0),
        state: jsonObjectSchema,
      })
      .parse(input);
    return this.database.begin(async (transaction) => {
      const households = await transaction<{ version: string; status: string }[]>`
        select version, status from households where id = ${parsed.householdId} for update
      `;
      const household = households[0];
      if (!household) {
        throw new ApplicationStoreError("not_found", "Unknown household");
      }
      if (household.status === "deleting") {
        throw new ApplicationStoreError("invalid_state", "Household deletion is fenced");
      }
      const sealed = this.sensitiveJson.seal(
        parsed.state,
        encryptedJsonContext(
          "household",
          parsed.householdId,
          "household_projections",
          parsed.householdId,
          "state",
        ),
      );
      const inserted = await transaction<{ household_id: string }[]>`
        insert into household_projections (
          household_id, schema_version, version, state_key_id, state_ciphertext
        )
        values (
          ${parsed.householdId}, ${parsed.schemaVersion}, ${parsed.version},
          ${sealed.keyId}, ${sealed.ciphertext}
        )
        on conflict (household_id) do nothing
        returning household_id
      `;
      if (inserted[0]) {
        const householdVersion = Number(household.version);
        if (householdVersion !== parsed.version) {
          throw new StaleProjectionVersionError(parsed.version, householdVersion);
        }
        await transaction`
          update households set updated_at = now() where id = ${parsed.householdId}
        `;
      }
      const rows = await transaction<ProjectionRow[]>`
        select household_id, schema_version, version, state_key_id, state_ciphertext, updated_at
        from household_projections where household_id = ${parsed.householdId}
      `;
      const projection = rows[0];
      if (!projection) {
        throw new ApplicationStoreError("invalid_state", "Household projection disappeared");
      }
      return {
        created: inserted.length === 1,
        projection: mapProjection(projection, this.sensitiveJson),
      };
    });
  }

  public async getHouseholdProjection(householdId: string): Promise<HouseholdProjection | null> {
    const parsedId = z.uuid().parse(householdId);
    const rows = await this.database<ProjectionRow[]>`
      select household_id, schema_version, version, state_key_id, state_ciphertext, updated_at
      from household_projections where household_id = ${parsedId}
    `;
    return rows[0] ? mapProjection(rows[0], this.sensitiveJson) : null;
  }

  public async commitHouseholdProjection(input: {
    householdId: string;
    expectedVersion: number;
    schemaVersion: number;
    nextState: Record<string, unknown>;
    processedSignalId?: string;
    cancelTimerKeys?: string[];
    timers?: ProjectionTimerIntent[];
    outbox?: ProjectionOutboxIntent[];
    audits?: ProjectionAuditIntent[];
  }): Promise<number> {
    const parsed = projectionCommitSchema.parse(input);
    return this.database.begin(async (transaction) => {
      const rows = await transaction<
        {
          version: string;
          household_version: string;
          next_audit_sequence: string;
          household_status: string;
        }[]
      >`
        select hp.version, h.version as household_version, h.next_audit_sequence,
          h.status as household_status
        from household_projections hp
        join households h on h.id = hp.household_id
        where hp.household_id = ${parsed.householdId}
        for update of hp, h
      `;
      const current = rows[0];
      if (!current) {
        throw new ApplicationStoreError("not_found", "Household projection is not initialized");
      }
      if (current.household_status === "deleting") {
        throw new ApplicationStoreError("invalid_state", "Household deletion is fenced");
      }
      const projectionVersion = Number(current.version);
      const householdVersion = Number(current.household_version);
      if (projectionVersion !== parsed.expectedVersion || householdVersion !== parsed.expectedVersion) {
        throw new StaleProjectionVersionError(
          parsed.expectedVersion,
          Math.max(projectionVersion, householdVersion),
        );
      }

      const nextVersion = parsed.expectedVersion + 1;
      const sealed = this.sensitiveJson.seal(
        parsed.nextState,
        encryptedJsonContext(
          "household",
          parsed.householdId,
          "household_projections",
          parsed.householdId,
          "state",
        ),
      );
      await transaction`
        update household_projections
        set schema_version = ${parsed.schemaVersion}, version = ${nextVersion},
            state_key_id = ${sealed.keyId}, state_ciphertext = ${sealed.ciphertext},
            updated_at = now()
        where household_id = ${parsed.householdId}
      `;

      if (parsed.cancelTimerKeys.length > 0) {
        await transaction`
          update scheduled_triggers
          set status = 'superseded', lease_owner = null, lease_token = null,
              lease_expires_at = null, due_at = null, payload_key_id = null, payload_ciphertext = null,
              updated_at = now()
          where household_id = ${parsed.householdId}
            and timer_key = any(${parsed.cancelTimerKeys})
            and status in ('scheduled', 'claimed')
        `;
      }
      for (const timer of parsed.timers) {
        await upsertTimerIntent(transaction, this.sensitiveJson, parsed.householdId, timer);
      }

      for (const effect of parsed.outbox) {
        await insertOutboxIntent(transaction, this.sensitiveJson, parsed.householdId, effect);
      }

      let auditSequence = Number(current.next_audit_sequence);
      for (const audit of parsed.audits) {
        await insertAudit(transaction, this.database, parsed.householdId, auditSequence, audit);
        auditSequence += 1;
      }

      await transaction`
        update households
        set version = ${nextVersion}, next_audit_sequence = ${auditSequence}, updated_at = now()
        where id = ${parsed.householdId}
      `;
      if (parsed.processedSignalId) {
        const processed = await transaction<{ id: string }[]>`
          update household_signals set processing_status = 'processed'
          where id = ${parsed.processedSignalId} and household_id = ${parsed.householdId}
          returning id
        `;
        if (processed.length !== 1) {
          throw new ApplicationStoreError("not_found", "Processed household signal does not exist");
        }
      }
      return nextVersion;
    });
  }

  public async scheduleTimer(input: ProjectionTimerIntent & { householdId: string }): Promise<{
    rowId: string;
  }> {
    const parsed = z.strictObject({ householdId: z.uuid(), ...projectionTimerSchema.shape }).parse(input);
    return this.database.begin(async (transaction) => {
      await requireWritableHousehold(transaction, parsed.householdId);
      return {
        rowId: await upsertTimerIntent(transaction, this.sensitiveJson, parsed.householdId, parsed),
      };
    });
  }

  public async cancelTimer(input: { householdId: string; timerKey: string }): Promise<boolean> {
    const parsed = z
      .strictObject({ householdId: z.uuid(), timerKey: z.string().min(1).max(500) })
      .parse(input);
    return this.database.begin(async (transaction) => {
      const timers = await transaction<{ id: string; status: string }[]>`
        select id, status from scheduled_triggers
        where household_id = ${parsed.householdId} and timer_key = ${parsed.timerKey}
        for update
      `;
      const timer = timers[0];
      if (!timer) return false;
      if (timer.status === "scheduled" || timer.status === "claimed") {
        await transaction`
          update scheduled_triggers
          set status = 'cancelled', lease_owner = null, lease_token = null,
            lease_expires_at = null, due_at = null, payload_key_id = null, payload_ciphertext = null,
            updated_at = now()
          where id = ${timer.id}
        `;
      }
      return true;
    });
  }

  public async createOAuthState(input: {
    id?: string;
    householdId: string;
    adultId: string;
    provider: string;
    stateHash: string;
    encryptedPayload: string;
    returnConversationId: string;
    expiresAt: string;
  }): Promise<{ stateId: string }> {
    const parsed = z
      .strictObject({
        id: z.uuid().optional(),
        householdId: z.uuid(),
        adultId: z.uuid(),
        provider: z.string().min(1).max(100),
        stateHash: z.string().min(32).max(256),
        encryptedPayload: z.string().min(1),
        returnConversationId: z.string().min(1).max(500),
        expiresAt: instantSchema,
      })
      .parse(input);
    await this.assertActiveMember(parsed.householdId, parsed.adultId);
    const stateId = parsed.id ?? randomUUID();
    await this.database`
      insert into oauth_states (
        id, household_id, adult_id, provider, state_hash, encrypted_payload,
        return_conversation_id, expires_at
      ) values (
        ${stateId}, ${parsed.householdId}, ${parsed.adultId}, ${parsed.provider},
        ${parsed.stateHash}, ${parsed.encryptedPayload}, ${parsed.returnConversationId},
        ${parsed.expiresAt}
      )
    `;
    return { stateId };
  }

  public async consumeOAuthState(input: { provider: string; stateHash: string }): Promise<{
    stateId: string;
    householdId: string;
    adultId: string;
    encryptedPayload: string;
    returnConversationId: string;
  } | null> {
    const parsed = z
      .strictObject({
        provider: z.string().min(1).max(100),
        stateHash: z.string().min(32).max(256),
      })
      .parse(input);
    const rows = await this.database<
      {
        id: string;
        household_id: string;
        adult_id: string;
        encrypted_payload: string;
        return_conversation_id: string;
      }[]
    >`
      with candidate as (
        select id, household_id, adult_id, encrypted_payload, return_conversation_id
        from oauth_states
        where provider = ${parsed.provider} and state_hash = ${parsed.stateHash}
          and consumed_at is null and expires_at > now()
          and encrypted_payload is not null
        for update
      )
      update oauth_states oauth
      set consumed_at = now(), encrypted_payload = null
      from candidate
      where oauth.id = candidate.id
      returning oauth.id, oauth.household_id, oauth.adult_id,
        candidate.encrypted_payload, oauth.return_conversation_id
    `;
    const row = rows[0];
    return row
      ? {
          stateId: row.id,
          householdId: row.household_id,
          adultId: row.adult_id,
          encryptedPayload: row.encrypted_payload,
          returnConversationId: row.return_conversation_id,
        }
      : null;
  }

  public async purgeExpiredOAuthStates(asOf: string): Promise<number> {
    const parsed = instantSchema.parse(asOf);
    const rows = await this.database<{ id: string }[]>`
      delete from oauth_states where expires_at <= ${parsed} returning id
    `;
    return rows.length;
  }

  public async upsertExternalConnection(
    rawInput: z.input<typeof connectionSchema>,
  ): Promise<{ connectionId: string; hadPriorGmailState: boolean }> {
    const input = connectionSchema.parse(rawInput);
    const requestedId = input.id ?? randomUUID();
    const accountLabel = canonicalGoogleAccountAlias(input.label);
    const accountAliasKey = googleAccountAliasKey(accountLabel);
    const metadata = { ...input.metadata };
    delete metadata.accountLabel;
    try {
      return await this.database.begin(async (transaction) => {
        await transaction`
          select pg_advisory_xact_lock(
            hashtextextended(${`external-connection:${input.provider}:${input.externalAccountId}`}, 0)
          )
        `;
        await transaction`
          select pg_advisory_xact_lock(
            hashtextextended(
              ${`external-connection-alias:${input.householdId}:${input.adultId}:${accountAliasKey}`},
              0
            )
          )
        `;
        const memberships = await transaction<{ adult_id: string }[]>`
          select adult_id from household_memberships
          where household_id = ${input.householdId} and adult_id = ${input.adultId}
            and status = 'active'
          for update
        `;
        if (memberships.length !== 1) {
          throw new ApplicationStoreError("not_authorized", "Adult is not active in this household");
        }
        const existingRows = await transaction<
          {
            id: string;
            status: ExternalConnectionRecord["status"];
            cursor: Record<string, unknown>;
          }[]
        >`
          select id, status, cursor
          from external_connections
          where household_id = ${input.householdId} and adult_id = ${input.adultId}
            and provider = ${input.provider} and external_account_id = ${input.externalAccountId}
          for update
        `;
        const existing = existingRows[0];
        const aliasRows = await transaction<
          { id: string; details_key_id: string; details_ciphertext: string }[]
        >`
          select id, details_key_id, details_ciphertext
          from external_connections
          where household_id = ${input.householdId} and adult_id = ${input.adultId}
            and provider = ${input.provider} and status <> 'revoked'
          order by id
        `;
        const aliasInUse = aliasRows.some((row) => {
          if (row.id === existing?.id) return false;
          const details = googleConnectionDetailsSchema.parse(
            this.sensitiveJson.open(
              { keyId: row.details_key_id, ciphertext: row.details_ciphertext },
              googleConnectionDetailsContext({
                householdId: input.householdId,
                adultId: input.adultId,
                connectionId: row.id,
              }),
            ),
          );
          return googleAccountAliasKey(details.accountLabel) === accountAliasKey;
        });
        if (aliasInUse) {
          throw new ApplicationStoreError(
            "external_account_alias_in_use",
            "Google account alias is already in use by this adult",
          );
        }
        const liveOwners = await transaction<{ household_id: string; adult_id: string }[]>`
          select household_id, adult_id from external_connections
          where provider = ${input.provider} and external_account_id = ${input.externalAccountId}
            and status <> 'revoked'
          for update
        `;
        if (
          liveOwners.some(
            (owner) => owner.household_id !== input.householdId || owner.adult_id !== input.adultId,
          )
        ) {
          throw new ApplicationStoreError(
            "external_account_in_use",
            "Google account is already connected to another adult",
          );
        }
        const details = this.sensitiveJson.seal(
          googleConnectionDetailsSchema.parse({
            label: input.label,
            accountLabel,
            email: input.email === undefined ? null : normalizedEmail(input.email),
          }),
          googleConnectionDetailsContext({
            householdId: input.householdId,
            adultId: input.adultId,
            connectionId: existing?.id ?? requestedId,
          }),
        );
        const emailDigest =
          input.email === undefined
            ? null
            : this.blindIndex.digest("google-connection-email", normalizedEmail(input.email));
        const rows = await transaction<{ id: string }[]>`
          insert into external_connections (
            id, household_id, adult_id, provider, external_account_id, email_digest,
            details_key_id, details_ciphertext,
            encrypted_credentials, granted_scopes, status, cursor, metadata, last_synced_at
          ) values (
            ${requestedId}, ${input.householdId}, ${input.adultId}, ${input.provider},
            ${input.externalAccountId}, ${emailDigest}, ${details.keyId}, ${details.ciphertext},
            ${input.encryptedCredentials}, ${input.grantedScopes}, 'active',
            ${json(this.database, input.cursor)}, ${json(this.database, metadata)},
            ${input.lastSyncedAt ?? null}
          )
          on conflict (household_id, adult_id, provider, external_account_id)
          do update set email_digest = excluded.email_digest,
            details_key_id = excluded.details_key_id, details_ciphertext = excluded.details_ciphertext,
            encrypted_credentials = excluded.encrypted_credentials,
            granted_scopes = excluded.granted_scopes, status = 'active',
            cursor = case when external_connections.status = 'revoked'
              then excluded.cursor else external_connections.cursor end,
            metadata = case when external_connections.status = 'revoked'
              then excluded.metadata else external_connections.metadata end,
            last_synced_at = case when external_connections.status = 'revoked'
              then excluded.last_synced_at else external_connections.last_synced_at end,
            updated_at = now()
          returning id
        `;
        const row = rows[0];
        if (!row) throw new ApplicationStoreError("invalid_state", "Connection upsert returned no row");
        return {
          connectionId: row.id,
          hadPriorGmailState:
            existing !== undefined &&
            existing.status !== "revoked" &&
            typeof existing.cursor.gmail === "object" &&
            existing.cursor.gmail !== null &&
            !Array.isArray(existing.cursor.gmail),
        };
      });
    } catch (error) {
      if (isGoogleSubjectOwnershipViolation(error)) {
        throw new ApplicationStoreError(
          "external_account_in_use",
          "Google account is already connected to another adult",
        );
      }
      throw error;
    }
  }

  public async getExternalConnection(input: {
    connectionId: string;
    householdId: string;
    adultId: string;
  }): Promise<ExternalConnectionRecord | null> {
    const parsed = z
      .strictObject({ connectionId: z.uuid(), householdId: z.uuid(), adultId: z.uuid() })
      .parse(input);
    await this.assertActiveMember(parsed.householdId, parsed.adultId);
    const rows = await this.database<ExternalConnectionRow[]>`
        select id, household_id, adult_id, provider, external_account_id, email_digest,
          details_key_id, details_ciphertext,
          encrypted_credentials, granted_scopes, status, cursor, metadata, last_synced_at
      from external_connections
      where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
        and adult_id = ${parsed.adultId}
    `;
    return rows[0] ? mapExternalConnection(rows[0], this.sensitiveJson) : null;
  }

  public async revokeExternalConnection(input: {
    connectionId: string;
    householdId: string;
    adultId: string;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({ connectionId: z.uuid(), householdId: z.uuid(), adultId: z.uuid() })
      .parse(input);
    await this.assertActiveMember(parsed.householdId, parsed.adultId);
    return this.database.begin(async (transaction) => {
      const rows = await transaction<{ id: string }[]>`
        update external_connections
        set status = 'revoked', encrypted_credentials = null, granted_scopes = '{}',
            email_digest = null, details_key_id = null, details_ciphertext = null,
            cursor = '{}'::jsonb, metadata = '{}'::jsonb,
            updated_at = now()
        where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and adult_id = ${parsed.adultId} and status <> 'revoked'
        returning id
      `;
      if (rows.length !== 1) return false;
      await transaction`
        delete from gmail_recovery_runs where connection_id = ${parsed.connectionId}
      `;
      await transaction`
        update google_calendar_channels set status = 'stopped', updated_at = now()
        where connection_id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and adult_id = ${parsed.adultId} and status in ('active', 'retiring')
      `;
      await transaction`
        delete from calendar_busy_windows
        where connection_id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and owner_adult_id = ${parsed.adultId}
      `;
      await transaction`
        delete from google_calendar_sync_states
        where connection_id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and adult_id = ${parsed.adultId}
      `;
      return true;
    });
  }

  public async persistSourceItem(rawInput: z.input<typeof sourceItemSchema>): Promise<{
    sourceItemId: string;
    disposition: "inserted" | "unchanged" | "revised";
    revision: number;
    retainedExisting?: "full" | "deleted" | "stale";
    createdByApprovedActionId?: string | null;
  }> {
    const input = sourceItemSchema.parse(rawInput);
    const retentionUntil = isRawGoogleSource(input.provider, input.encryptedContent)
      ? rawGoogleSourceRetentionUntil(input.retentionUntil)
      : (input.retentionUntil ?? null);
    if (input.ownerAdultId) {
      await this.assertActiveMember(input.householdId, input.ownerAdultId);
    }
    const sourceItemId = input.id ?? randomUUID();

    return this.database.begin(async (transaction) => {
      await lockApplicationIdentity(transaction, "source_item", {
        householdId: input.householdId,
        provider: input.provider,
        connectionId: input.connectionId ?? null,
        externalId: input.externalId,
      });
      if (input.connectionId) {
        const connections = await transaction<{ id: string }[]>`
          select id from external_connections
          where id = ${input.connectionId} and household_id = ${input.householdId}
            and status = 'active'
            and (${input.ownerAdultId ?? null}::uuid is null or adult_id = ${input.ownerAdultId ?? null})
        `;
        if (!connections[0]) {
          throw new ApplicationStoreError("not_authorized", "Source connection is outside its scope");
        }
      }

      const existingRows = await transaction<
        {
          id: string;
          owner_adult_id: string | null;
          visibility: SourceItemRecord["visibility"];
          kind: string;
          occurred_at: Date;
          content_hash: string;
          metadata: Record<string, unknown>;
          revision: string;
        }[]
      >`
        select id, owner_adult_id, visibility, kind, occurred_at, content_hash, metadata, revision
        from source_items
        where household_id = ${input.householdId}
          and provider = ${input.provider} and external_id = ${input.externalId}
          and connection_id is not distinct from ${input.connectionId ?? null}
        for update
      `;
      const existing = existingRows[0];
      const persistedMetadata =
        input.provider === "google-calendar"
          ? calendarSourceMetadata(input.kind, input.metadata, existing)
          : input.metadata;
      if (!existing) {
        await transaction`
          insert into source_items (
            id, household_id, connection_id, owner_adult_id, visibility, provider,
            external_id, kind, occurred_at, content_hash, encrypted_content,
            metadata, retention_until, revision
          ) values (
            ${sourceItemId}, ${input.householdId}, ${input.connectionId ?? null},
            ${input.ownerAdultId ?? null}, ${input.visibility}, ${input.provider},
            ${input.externalId}, ${input.kind}, ${input.occurredAt}, ${input.contentHash},
            ${input.encryptedContent ?? null},
            ${json(this.database, persistedMetadata)}, ${retentionUntil}, 1
          )
        `;
        return {
          sourceItemId,
          disposition: "inserted" as const,
          revision: 1,
          ...calendarEchoReceipt(input.provider, persistedMetadata),
        };
      }

      if (
        existing.owner_adult_id !== (input.ownerAdultId ?? null) ||
        existing.visibility !== input.visibility
      ) {
        throw new ApplicationStoreError(
          "invalid_state",
          "A source item's ownership and visibility are immutable",
        );
      }

      if (
        input.provider === "google-calendar" &&
        existing.occurred_at.getTime() > Date.parse(input.occurredAt)
      ) {
        return {
          sourceItemId: existing.id,
          disposition: "unchanged" as const,
          revision: Number(existing.revision),
          retainedExisting: "stale" as const,
          ...calendarEchoReceipt(input.provider, existing.metadata),
        };
      }

      const existingGmailCompleteness = gmailContentCompleteness(existing.metadata);
      const incomingGmailCompleteness = gmailContentCompleteness(input.metadata);
      if (
        input.provider === "gmail" &&
        input.kind === "gmail_message" &&
        existing.kind === "gmail_message" &&
        existingGmailCompleteness === "full" &&
        incomingGmailCompleteness === "full"
      ) {
        const existingHistoryId = gmailMessageHistoryId(existing.metadata);
        const incomingHistoryId = gmailMessageHistoryId(input.metadata);
        if (
          existingHistoryId !== null &&
          incomingHistoryId !== null &&
          incomingHistoryId < existingHistoryId
        ) {
          return {
            sourceItemId: existing.id,
            disposition: "unchanged" as const,
            revision: Number(existing.revision),
            retainedExisting: "stale" as const,
          };
        }
        if (
          existing.content_hash !== input.contentHash &&
          (existingHistoryId === null ||
            incomingHistoryId === null ||
            incomingHistoryId === existingHistoryId)
        ) {
          throw new ApplicationStoreError(
            "invalid_state",
            "Gmail full content conflicts at an unordered provider revision",
          );
        }
      }
      const authoritativeGmailRecovery =
        input.provider === "gmail" &&
        input.kind === "gmail_message" &&
        input.metadata.discoveryMode === "recovery";
      const retainedExistingGmailSource =
        input.provider === "gmail" && input.kind === "gmail_message"
          ? existing.kind === "gmail_message_deleted" && !authoritativeGmailRecovery
            ? ("deleted" as const)
            : existing.kind === "gmail_message" &&
                existingGmailCompleteness === "full" &&
                incomingGmailCompleteness === "metadata"
              ? ("full" as const)
              : null
          : null;
      if (retainedExistingGmailSource !== null) {
        if (retainedExistingGmailSource === "full") {
          await transaction`
            update source_items
            set retention_until = ${retentionUntil},
                metadata = ${json(this.database, {
                  ...mergedGmailDiscoveryMetadata(existing.metadata, input.metadata),
                })},
                updated_at = now()
            where id = ${existing.id}
          `;
        }
        return {
          sourceItemId: existing.id,
          disposition: "unchanged" as const,
          revision: Number(existing.revision),
          retainedExisting: retainedExistingGmailSource,
        };
      }

      const forcesGmailSourceReplacement =
        input.provider === "gmail" &&
        ((input.kind === "gmail_message_deleted" && existing.kind !== "gmail_message_deleted") ||
          (authoritativeGmailRecovery && existing.kind === "gmail_message_deleted") ||
          (input.kind === "gmail_message" &&
            existing.kind === "gmail_message" &&
            existingGmailCompleteness === "metadata" &&
            incomingGmailCompleteness === "full"));
      const forcesCalendarSourceReplacement =
        input.provider === "google-calendar" &&
        (existing.kind !== input.kind ||
          existing.metadata.applicationContentDigest !== persistedMetadata.applicationContentDigest);
      if (
        existing.content_hash === input.contentHash &&
        !forcesGmailSourceReplacement &&
        !forcesCalendarSourceReplacement
      ) {
        await transaction`
          update source_items
          set retention_until = ${retentionUntil}, metadata = ${json(this.database, persistedMetadata)},
              updated_at = now()
          where id = ${existing.id}
        `;
        return {
          sourceItemId: existing.id,
          disposition: "unchanged" as const,
          revision: Number(existing.revision),
          ...calendarEchoReceipt(input.provider, persistedMetadata),
        };
      }

      const revision = Number(existing.revision) + 1;
      await transaction`
        update source_items
        set kind = ${input.kind}, occurred_at = ${input.occurredAt},
            content_hash = ${input.contentHash}, encrypted_content = ${input.encryptedContent ?? null},
            metadata = ${json(this.database, persistedMetadata)},
            retention_until = ${retentionUntil}, revision = ${revision}, updated_at = now()
        where id = ${existing.id}
      `;
      return {
        sourceItemId: existing.id,
        disposition: "revised" as const,
        revision,
        ...calendarEchoReceipt(input.provider, persistedMetadata),
      };
    });
  }

  public async getSourceItem(input: {
    sourceItemId: string;
    householdId: string;
    viewerAdultId: string;
  }): Promise<SourceItemRecord | null> {
    const parsed = z
      .strictObject({ sourceItemId: z.uuid(), householdId: z.uuid(), viewerAdultId: z.uuid() })
      .parse(input);
    await this.assertActiveMember(parsed.householdId, parsed.viewerAdultId);
    const rows = await this.database<SourceItemRow[]>`
      select id, household_id, connection_id, owner_adult_id, visibility, provider,
        external_id, kind, occurred_at, content_hash, encrypted_content,
        metadata, retention_until, revision
      from source_items
      where id = ${parsed.sourceItemId} and household_id = ${parsed.householdId}
        and (visibility = 'household' or owner_adult_id = ${parsed.viewerAdultId})
    `;
    return rows[0] ? mapSourceItem(rows[0]) : null;
  }

  public async purgeExpiredSourceContent(asOf: string): Promise<number> {
    const parsed = instantSchema.parse(asOf);
    return this.database.begin(async (transaction) => {
      const sources = await transaction<{ id: string }[]>`
        update source_items
        set encrypted_content = null, updated_at = now()
        where retention_until is not null and retention_until <= ${parsed}
          and encrypted_content is not null
        returning id
      `;
      const privateReviews = await transaction<{ id: string }[]>`
        delete from private_review_items where retention_until <= ${parsed} returning id
      `;
      return sources.length + privateReviews.length;
    });
  }

  public async claimDueTimers(input: {
    owner: string;
    limit: number;
    leaseSeconds: number;
  }): Promise<ClaimedTimer[]> {
    const parsed = leaseInputSchema.parse(input);
    const leaseToken = randomUUID();
    return this.database.begin(async (transaction) => {
      await transaction`
        with exhausted as (
          select id from scheduled_triggers
          where attempt >= max_attempts
            and (
              status = 'scheduled'
              or (status = 'claimed' and lease_expires_at < now())
            )
          for update skip locked
        )
        update scheduled_triggers
        set status = 'dead', dead_at = now(), lease_owner = null, lease_token = null,
          lease_expires_at = null, due_at = null, payload_key_id = null, payload_ciphertext = null,
          last_error_code = coalesce(last_error_code, 'max_attempts_exhausted'),
          updated_at = now()
        from exhausted where scheduled_triggers.id = exhausted.id
      `;
      const rows = await transaction<TimerRow[]>`
        with candidates as (
          select trigger.id
          from scheduled_triggers trigger
          join households household on household.id = trigger.household_id
          where (
            (trigger.status = 'scheduled' and trigger.due_at <= now() and trigger.available_at <= now())
            or (trigger.status = 'claimed' and trigger.lease_expires_at < now())
          )
            and trigger.attempt < trigger.max_attempts
            and household.status <> 'deleting'
            and trigger.control_epoch = household.control_epoch
          order by trigger.due_at, trigger.created_at
          for update skip locked
          limit ${parsed.limit}
        )
        update scheduled_triggers
        set status = 'claimed', lease_owner = ${parsed.owner}, lease_token = ${leaseToken},
            lease_expires_at = now() + (${parsed.leaseSeconds} * interval '1 second'),
            attempt = attempt + 1, updated_at = now()
        from candidates
        where scheduled_triggers.id = candidates.id
        returning scheduled_triggers.id,
          coalesce(scheduled_triggers.timer_key, 'legacy:' || scheduled_triggers.id::text) as timer_key,
          scheduled_triggers.household_id, scheduled_triggers.episode_key,
          scheduled_triggers.trigger_kind, scheduled_triggers.plan_version,
          scheduled_triggers.due_at, scheduled_triggers.payload_key_id,
          scheduled_triggers.payload_ciphertext, scheduled_triggers.attempt,
          scheduled_triggers.max_attempts, scheduled_triggers.lease_token,
          scheduled_triggers.lease_expires_at
      `;
      return rows.map((row) => mapTimer(row, this.sensitiveJson));
    });
  }

  public async renewTimerLease(input: {
    rowId: string;
    leaseToken: string;
    leaseSeconds: number;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        leaseSeconds: z.number().int().positive().max(86_400),
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update scheduled_triggers
      set lease_expires_at = now() + (${parsed.leaseSeconds} * interval '1 second'),
          updated_at = now()
      where id = ${parsed.rowId} and status = 'claimed'
        and lease_token = ${parsed.leaseToken} and lease_expires_at > now()
      returning id
    `;
    return rows.length === 1;
  }

  public async finishTimer(input: {
    rowId: string;
    leaseToken: string;
    outcome: "fired" | "cancelled" | "superseded";
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        outcome: z.enum(["fired", "cancelled", "superseded"]),
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update scheduled_triggers
      set status = ${parsed.outcome}, lease_owner = null, lease_token = null,
          lease_expires_at = null, due_at = null, payload_key_id = null, payload_ciphertext = null,
          updated_at = now()
      where id = ${parsed.rowId} and status = 'claimed' and lease_token = ${parsed.leaseToken}
        and lease_expires_at > now()
      returning id
    `;
    return rows.length === 1;
  }

  public async releaseTimer(input: {
    rowId: string;
    leaseToken: string;
    retryAt: string;
    errorCode: string;
  }): Promise<"scheduled" | "dead" | "lost_lease"> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        retryAt: instantSchema,
        errorCode: z.string().min(1).max(200),
      })
      .parse(input);
    const rows = await this.database<{ status: "scheduled" | "dead" }[]>`
      update scheduled_triggers
      set status = case when attempt >= max_attempts then 'dead' else 'scheduled' end,
          available_at = case when attempt >= max_attempts then available_at else ${parsed.retryAt} end,
          dead_at = case when attempt >= max_attempts then now() else null end,
          last_error_code = ${parsed.errorCode}, lease_owner = null, lease_token = null,
          lease_expires_at = null,
          due_at = case when attempt >= max_attempts then null else due_at end,
          payload_key_id = case when attempt >= max_attempts then null else payload_key_id end,
          payload_ciphertext = case when attempt >= max_attempts then null else payload_ciphertext end,
          updated_at = now()
      where id = ${parsed.rowId} and status = 'claimed' and lease_token = ${parsed.leaseToken}
        and lease_expires_at > now()
      returning status
    `;
    return rows[0]?.status ?? "lost_lease";
  }

  public async countDeadSemanticTimers(): Promise<number> {
    const rows = await this.database<{ count: string }[]>`
      select count(*)::text as count from scheduled_triggers where status = 'dead'
    `;
    return Number(rows[0]?.count ?? 0);
  }

  public async claimOutbox(input: {
    owner: string;
    limit: number;
    leaseSeconds: number;
  }): Promise<ClaimedOutboxItem[]> {
    const parsed = leaseInputSchema.parse(input);
    const leaseToken = randomUUID();
    return this.database.begin(async (transaction) => {
      await transaction`
        with exhausted as (
          select id from outbox
          where status = 'leased' and lease_expires_at < now() and attempt >= max_attempts
          for update skip locked
        )
        update outbox
        set status = 'dead', dead_at = now(), lease_owner = null, lease_token = null,
          lease_expires_at = null, payload_key_id = null, payload_ciphertext = null,
          last_error_code = 'lease_expired_after_max_attempts',
          updated_at = now()
        from exhausted where outbox.id = exhausted.id
      `;
      const rows = await transaction<OutboxRow[]>`
        with candidates as (
          select effect.id
          from outbox effect
          join households household on household.id = effect.household_id
          where (
            (effect.status in ('pending', 'retry') and effect.available_at <= now())
            or (effect.status = 'leased' and effect.lease_expires_at < now())
          )
            and effect.control_epoch = household.control_epoch
            and (
              household.status <> 'deleting'
              or effect.intent_key like 'customer_control.deletion.fenced.%'
            )
          order by effect.available_at, effect.created_at
          for update skip locked
          limit ${parsed.limit}
        )
        update outbox
        set status = 'leased', lease_owner = ${parsed.owner}, lease_token = ${leaseToken},
            lease_expires_at = now() + (${parsed.leaseSeconds} * interval '1 second'),
            attempt = attempt + 1, updated_at = now()
        from candidates
        where outbox.id = candidates.id
        returning outbox.id,
          coalesce(outbox.intent_key, 'legacy:' || outbox.idempotency_key) as intent_key,
          outbox.household_id, outbox.effect_kind, outbox.idempotency_key,
          outbox.payload_key_id, outbox.payload_ciphertext,
          outbox.attempt, outbox.max_attempts,
          outbox.lease_token, outbox.lease_expires_at
      `;
      return rows.map((row) => mapOutbox(row, this.sensitiveJson));
    });
  }

  public async renewOutboxLease(input: {
    rowId: string;
    leaseToken: string;
    leaseSeconds: number;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        leaseSeconds: z.number().int().positive().max(86_400),
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update outbox
      set lease_expires_at = now() + (${parsed.leaseSeconds} * interval '1 second'),
          updated_at = now()
      where id = ${parsed.rowId} and status = 'leased'
        and lease_token = ${parsed.leaseToken} and lease_expires_at > now()
      returning id
    `;
    return rows.length === 1;
  }

  public async enqueueApplicationIntent(rawIntent: unknown): Promise<{ rowId: string }> {
    const rowId = await this.database.begin(async (transaction) => {
      const intent = ApplicationOutboxIntentSchema.parse(rawIntent);
      const households = await transaction<{ status: string }[]>`
        select status from households where id = ${intent.householdId} for update
      `;
      if (!households[0]) throw new ApplicationStoreError("not_found", "Unknown household");
      if (households[0].status === "deleting") {
        throw new ApplicationStoreError("invalid_state", "Household deletion is fenced");
      }
      const inserted = await this.insertApplicationIntent(transaction, rawIntent);
      return inserted.rowId;
    });
    return { rowId };
  }

  /** Allows an infrastructure state transition and its outbox effect to share one transaction. */
  public async insertApplicationIntent(
    transaction: TransactionSql<Record<string, never>>,
    rawIntent: unknown,
  ): Promise<{ rowId: string }> {
    const intent = ApplicationOutboxIntentSchema.parse(rawIntent);
    await requireWritableHousehold(transaction, intent.householdId);
    const rowId = await insertOutboxIntent(transaction, this.sensitiveJson, intent.householdId, {
      intentKey: intent.intentId,
      effectKind: intent.kind,
      idempotencyKey: intent.idempotencyKey,
      payload: intent,
      maxAttempts: 8,
    });
    return { rowId };
  }

  public async recordOutboxSuccess(input: {
    rowId: string;
    leaseToken: string;
    providerReceipt: Record<string, unknown>;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({ rowId: z.uuid(), leaseToken: z.uuid(), providerReceipt: jsonObjectSchema })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update outbox
      set status = 'sent', provider_receipt = ${json(this.database, parsed.providerReceipt)},
          sent_at = now(), lease_owner = null, lease_token = null, lease_expires_at = null,
          payload_key_id = null, payload_ciphertext = null, updated_at = now()
      where id = ${parsed.rowId} and status = 'leased' and lease_token = ${parsed.leaseToken}
        and lease_expires_at > now()
      returning id
    `;
    return rows.length === 1;
  }

  public async recordOutboxFailure(input: {
    rowId: string;
    leaseToken: string;
    errorCode: string;
    safeDetail?: string;
    retryAfterSeconds: number;
    outcomeCertain: boolean;
  }): Promise<"retry" | "dead" | "ambiguous" | "lost_lease"> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        errorCode: z.string().min(1).max(200),
        safeDetail: z.string().max(2000).optional(),
        retryAfterSeconds: z.number().int().nonnegative().max(604_800),
        outcomeCertain: z.boolean(),
      })
      .parse(input);
    const rows = await this.database<{ status: "retry" | "dead" | "ambiguous" }[]>`
      update outbox
      set status = case
            when not ${parsed.outcomeCertain} then 'ambiguous'
            when attempt >= max_attempts then 'dead'
            else 'retry'
          end,
          available_at = now() + (${parsed.retryAfterSeconds} * interval '1 second'),
          dead_at = case when ${parsed.outcomeCertain} and attempt >= max_attempts then now() else null end,
          last_error_code = ${parsed.errorCode}, last_error_detail = ${parsed.safeDetail ?? null},
          lease_owner = null, lease_token = null, lease_expires_at = null,
          payload_key_id = case
            when ${parsed.outcomeCertain} and attempt >= max_attempts then null
            else payload_key_id
          end,
          payload_ciphertext = case
            when ${parsed.outcomeCertain} and attempt >= max_attempts then null
            else payload_ciphertext
          end,
          updated_at = now()
      where id = ${parsed.rowId} and status = 'leased' and lease_token = ${parsed.leaseToken}
        and lease_expires_at > now()
      returning status
    `;
    return rows[0]?.status ?? "lost_lease";
  }

  public async recordOutboxPermanent(input: {
    rowId: string;
    leaseToken: string;
    errorCode: string;
    safeDetail?: string;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        errorCode: queueErrorCodeSchema,
        safeDetail: z.string().trim().min(1).max(2_000).optional(),
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update outbox
      set status = 'dead', dead_at = now(), last_error_code = ${parsed.errorCode},
          last_error_detail = ${parsed.safeDetail ?? null}, lease_owner = null,
          lease_token = null, lease_expires_at = null,
          payload_key_id = null, payload_ciphertext = null, updated_at = now()
      where id = ${parsed.rowId} and status = 'leased' and lease_token = ${parsed.leaseToken}
        and lease_expires_at > now()
      returning id
    `;
    return rows.length === 1;
  }

  public async resolveAmbiguousOutbox(input: {
    rowId: string;
    resolution: "sent" | "dead" | "retry";
    providerReceipt?: Record<string, unknown>;
    retryAt?: string;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        resolution: z.enum(["sent", "dead", "retry"]),
        providerReceipt: jsonObjectSchema.optional(),
        retryAt: instantSchema.optional(),
      })
      .superRefine((value, context) => {
        if (value.resolution === "sent" && !value.providerReceipt) {
          context.addIssue({ code: "custom", message: "Sent resolution requires a provider receipt" });
        }
        if (value.resolution === "retry" && !value.retryAt) {
          context.addIssue({ code: "custom", message: "Retry resolution requires retryAt" });
        }
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update outbox
      set status = ${parsed.resolution},
          provider_receipt = ${parsed.providerReceipt ? json(this.database, parsed.providerReceipt) : null},
          available_at = coalesce(${parsed.retryAt ?? null}, available_at),
          sent_at = case when ${parsed.resolution} = 'sent' then now() else null end,
          dead_at = case when ${parsed.resolution} = 'dead' then now() else null end,
          payload_key_id = case
            when ${parsed.resolution} in ('sent', 'dead') then null else payload_key_id
          end,
          payload_ciphertext = case
            when ${parsed.resolution} in ('sent', 'dead') then null else payload_ciphertext
          end,
          updated_at = now()
      where id = ${parsed.rowId} and status = 'ambiguous'
      returning id
    `;
    return rows.length === 1;
  }

  public async appendAudit(input: { householdId: string; audit: ProjectionAuditIntent }): Promise<number> {
    const parsed = z.strictObject({ householdId: z.uuid(), audit: projectionAuditSchema }).parse(input);
    return this.database.begin(async (transaction) => {
      await requireWritableHousehold(transaction, parsed.householdId);
      const rows = await transaction<{ next_audit_sequence: string }[]>`
        select next_audit_sequence from households
        where id = ${parsed.householdId} for update
      `;
      const row = rows[0];
      if (!row) throw new ApplicationStoreError("not_found", "Unknown household");
      const sequence = Number(row.next_audit_sequence);
      await insertAudit(transaction, this.database, parsed.householdId, sequence, parsed.audit);
      await transaction`
        update households set next_audit_sequence = ${sequence + 1}, updated_at = now()
        where id = ${parsed.householdId}
      `;
      return sequence;
    });
  }

  public async exportHouseholdData(input: {
    householdId: string;
    requestedByAdultId: string;
    exportedAt: string;
  }): Promise<Record<string, unknown>> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        requestedByAdultId: z.uuid(),
        exportedAt: instantSchema,
      })
      .parse(input);

    return this.database.begin("isolation level repeatable read read only", async (transaction) => {
      const memberships = await transaction<{ adult_id: string }[]>`
        select adult_id from household_memberships
        where household_id = ${parsed.householdId}
          and adult_id = ${parsed.requestedByAdultId} and status = 'active'
      `;
      if (!memberships[0]) {
        throw new ApplicationStoreError("not_authorized", "Adult is not an active household member");
      }
      const household = await transaction<Record<string, unknown>[]>`
        select id, name, timezone, status, version, created_at, updated_at
        from households where id = ${parsed.householdId}
      `;
      if (!household[0]) throw new ApplicationStoreError("not_found", "Unknown household");
      const adultRows = await transaction<
        Array<Record<string, unknown> & { id: string; details_key_id: string; details_ciphertext: string }>
      >`
        select a.id, a.timezone, hm.role, hm.status, hm.consented_at,
          details.details_key_id, details.details_ciphertext,
          hm.created_at, hm.updated_at
        from household_memberships hm join adults a on a.id = hm.adult_id
        join adult_identity_details details
          on details.household_id = hm.household_id and details.adult_id = hm.adult_id
        where hm.household_id = ${parsed.householdId}
        order by hm.created_at, a.id
      `;
      const adults = adultRows.map(({ details_key_id, details_ciphertext, id, ...adult }) => ({
        id,
        display_name: adultIdentityDetailsSchema.parse(
          this.sensitiveJson.open(
            { keyId: details_key_id, ciphertext: details_ciphertext },
            adultIdentityDetailsContext({ householdId: parsed.householdId, adultId: id }),
          ),
        ).displayName,
        ...adult,
      }));
      const channels = await transaction<Record<string, unknown>[]>`
        select id, adult_id, provider, channel_type, external_chat_id, external_handle,
          status, metadata, created_at, updated_at
        from channel_bindings
        where household_id = ${parsed.householdId}
          and (channel_type = 'group' or adult_id = ${parsed.requestedByAdultId})
        order by created_at, id
      `;
      const connectionRows = await transaction<
        Array<
          Record<string, unknown> & {
            id: string;
            adult_id: string;
            details_key_id: string | null;
            details_ciphertext: string | null;
          }
        >
      >`
        select id, adult_id, provider, external_account_id, granted_scopes,
          status, cursor, metadata, last_synced_at, created_at, updated_at,
          details_key_id, details_ciphertext
        from external_connections
        where household_id = ${parsed.householdId} and adult_id = ${parsed.requestedByAdultId}
        order by created_at, id
      `;
      const connections = connectionRows.map(
        ({ details_key_id, details_ciphertext, id, adult_id, ...connection }) => {
          const details =
            details_key_id && details_ciphertext
              ? googleConnectionDetailsSchema.parse(
                  this.sensitiveJson.open(
                    { keyId: details_key_id, ciphertext: details_ciphertext },
                    googleConnectionDetailsContext({
                      householdId: parsed.householdId,
                      adultId: adult_id,
                      connectionId: id,
                    }),
                  ),
                )
              : {
                  label: REVOKED_GOOGLE_ACCOUNT_LABEL,
                  accountLabel: REVOKED_GOOGLE_ACCOUNT_LABEL,
                  email: null,
                };
          return {
            id,
            adult_id,
            ...connection,
            label: details.label,
            email: details.email,
            account_label: details.accountLabel,
          };
        },
      );
      const sources = await transaction<Record<string, unknown>[]>`
        select id, connection_id, owner_adult_id, visibility, provider, external_id, kind,
          occurred_at, content_hash, encrypted_content, metadata, retention_until,
          revision, created_at, updated_at
        from source_items
        where household_id = ${parsed.householdId}
          and (visibility = 'household' or owner_adult_id = ${parsed.requestedByAdultId})
        order by occurred_at, id
      `;
      const privateReviewRows = await transaction<
        Array<
          Record<string, unknown> & {
            adult_id: string;
            item_key: string;
            summary_ciphertext: string;
          }
        >
      >`
        select id, adult_id, item_key, source, summary_ciphertext, observed_at, digest_run_id,
          reviewed_at, retention_until, created_at, updated_at
        from private_review_items
        where household_id = ${parsed.householdId} and adult_id = ${parsed.requestedByAdultId}
        order by observed_at, id
      `;
      const privateReviews = privateReviewRows.map(({ summary_ciphertext, ...row }) => ({
        ...row,
        summary: this.privateReviewSecrets.open(
          summary_ciphertext,
          privateReviewSummaryAad({
            householdId: parsed.householdId,
            adultId: row.adult_id,
            itemKey: row.item_key,
          }),
        ),
      }));
      const projectionRows = await transaction<(ProjectionRow & { created_at: Date })[]>`
        select household_id, schema_version, version, state_key_id, state_ciphertext,
          created_at, updated_at
        from household_projections where household_id = ${parsed.householdId}
      `;
      const projection = projectionRows[0]
        ? {
            schema_version: projectionRows[0].schema_version,
            version: projectionRows[0].version,
            state: mapProjection(projectionRows[0], this.sensitiveJson).state,
            created_at: projectionRows[0].created_at,
            updated_at: projectionRows[0].updated_at,
          }
        : null;
      const snapshotRows = await transaction<(ApplicationSnapshotRow & { created_at: Date })[]>`
        select household_id, schema_version, revision, application_phase,
          snapshot_key_id, snapshot_ciphertext, created_at, updated_at
        from application_snapshots where household_id = ${parsed.householdId}
      `;
      const applicationSnapshot = snapshotRows[0]
        ? {
            schema_version: snapshotRows[0].schema_version,
            ...mapApplicationSnapshot(snapshotRows[0], this.sensitiveJson),
            created_at: snapshotRows[0].created_at,
            updated_at: snapshotRows[0].updated_at,
          }
        : null;
      const commitRows = await transaction<
        (ApplicationCommitRow & { base_revision: string; committed_at: Date })[]
      >`
        select id, base_revision, revision, content_digest, body_key_id, body_ciphertext,
          committed_at
        from application_commits where household_id = ${parsed.householdId}
        order by revision
      `;
      const applicationCommits = commitRows.map((row) => {
        const body = openApplicationCommitBody(row, parsed.householdId, this.sensitiveJson);
        return {
          idempotency_key: body.idempotencyKey,
          base_revision: row.base_revision,
          revision: row.revision,
          outcome: body.outcome,
          committed_at: row.committed_at,
        };
      });
      const audits = await transaction<Record<string, unknown>[]>`
        select sequence, actor_kind, actor_id, action, target_type, target_id,
          visibility, owner_adult_id, source_refs, policy_refs, details, created_at
        from audit_log
        where household_id = ${parsed.householdId}
          and (visibility = 'household' or owner_adult_id = ${parsed.requestedByAdultId})
        order by sequence
      `;
      return {
        schemaVersion: 1,
        exportedAt: parsed.exportedAt,
        household: household[0],
        adults,
        channels,
        connections,
        sources,
        privateReviews,
        projection: projection
          ? projectionForExport(projection, parsed.requestedByAdultId, adults.length)
          : null,
        applicationSnapshot: applicationSnapshot
          ? applicationSnapshotForExport(applicationSnapshot, parsed.requestedByAdultId)
          : null,
        applicationCommits,
        audits,
      };
    });
  }

  private async assertActiveMember(householdId: string, adultId: string): Promise<void> {
    const rows = await this.database<{ adult_id: string }[]>`
      select adult_id from household_memberships
      where household_id = ${householdId} and adult_id = ${adultId} and status = 'active'
    `;
    if (!rows[0]) {
      throw new ApplicationStoreError("not_authorized", "Adult is not an active household member");
    }
  }
}

type ExternalConnectionRow = {
  id: string;
  household_id: string;
  adult_id: string;
  provider: "google";
  external_account_id: string;
  email_digest: string | null;
  details_key_id: string | null;
  details_ciphertext: string | null;
  encrypted_credentials: string | null;
  granted_scopes: string[];
  status: ExternalConnectionRecord["status"];
  cursor: Record<string, unknown>;
  metadata: Record<string, unknown>;
  last_synced_at: Date | null;
};

type SourceItemRow = {
  id: string;
  household_id: string;
  connection_id: string | null;
  owner_adult_id: string | null;
  visibility: SourceItemRecord["visibility"];
  provider: string;
  external_id: string;
  kind: string;
  occurred_at: Date;
  content_hash: string;
  encrypted_content: string | null;
  metadata: Record<string, unknown>;
  retention_until: Date | null;
  revision: string;
};

type TimerRow = {
  id: string;
  timer_key: string | null;
  household_id: string;
  episode_key: string | null;
  trigger_kind: string;
  plan_version: string;
  due_at: Date;
  payload_key_id: string | null;
  payload_ciphertext: string | null;
  attempt: number;
  max_attempts: number;
  lease_token: string | null;
  lease_expires_at: Date | null;
};

type OutboxRow = {
  id: string;
  intent_key: string | null;
  household_id: string;
  effect_kind: string;
  idempotency_key: string;
  payload_key_id: string | null;
  payload_ciphertext: string | null;
  attempt: number;
  max_attempts: number;
  lease_token: string | null;
  lease_expires_at: Date | null;
};

type OutboxIdentityRow = {
  id: string;
  household_id: string;
  intent_key: string | null;
  effect_kind: string;
  idempotency_key: string;
  payload_digest: string;
};

function mapProjection(row: ProjectionRow, cipher: TenantJsonCipher): HouseholdProjection {
  const state = jsonObjectSchema.parse(
    cipher.open(
      { keyId: row.state_key_id, ciphertext: row.state_ciphertext },
      encryptedJsonContext("household", row.household_id, "household_projections", row.household_id, "state"),
    ),
  );
  return {
    householdId: row.household_id,
    schemaVersion: row.schema_version,
    version: Number(row.version),
    state,
    updatedAt: dateToString(row.updated_at),
  };
}

function mapApplicationSnapshot(
  row: ApplicationSnapshotRow,
  cipher: TenantJsonCipher,
): HouseholdApplicationSnapshot {
  try {
    const snapshot = HouseholdApplicationSnapshotSchema.parse(
      cipher.open(
        { keyId: row.snapshot_key_id, ciphertext: row.snapshot_ciphertext },
        encryptedJsonContext(
          "household",
          row.household_id,
          "application_snapshots",
          row.household_id,
          "snapshot",
        ),
      ),
    );
    if (snapshot.revision !== Number(row.revision)) throw new Error("Snapshot revision mismatch");
    if (snapshot.projection.onboarding.phase !== row.application_phase) {
      throw new Error("Snapshot phase mismatch");
    }
    return snapshot;
  } catch {
    throw new ApplicationStoreError(
      "invalid_state",
      "Household projection does not contain a valid application snapshot",
    );
  }
}

function openApplicationCommitBody(row: ApplicationCommitRow, householdId: string, cipher: TenantJsonCipher) {
  return applicationCommitBodySchema.parse(
    cipher.open(
      { keyId: row.body_key_id, ciphertext: row.body_ciphertext },
      encryptedJsonContext("household", householdId, "application_commits", row.id, "body"),
    ),
  );
}

function openProviderInboxBody(row: ProviderInboxRow, cipher: TenantJsonCipher) {
  return providerInboxBodySchema.parse(
    cipher.open(
      { keyId: row.body_key_id, ciphertext: row.body_ciphertext },
      encryptedJsonContext(
        row.encryption_tenant_kind,
        row.encryption_tenant_id,
        "provider_inbox",
        row.id,
        "body",
      ),
    ),
  );
}

function encryptedJsonContext(
  tenantKind: EncryptionTenantKind,
  tenantId: string,
  table: EncryptionContext["table"],
  rowId: string,
  field: string,
): EncryptionContext {
  return { tenant: { kind: tenantKind, id: tenantId }, table, rowId, field };
}

function providerIngressTenantId(
  input: z.output<typeof ingestProviderEventSchema>,
  blindIndex: BlindIndex,
): string {
  const partnerId = stringField(input.authentication, "partnerId");
  const mailboxEmail = stringField(input.payload, "mailboxEmail");
  return blindIndex.digest(
    "provider-tenant",
    `${input.provider}\0${partnerId ?? mailboxEmail?.toLowerCase() ?? input.provider}`,
  );
}

function providerRoutingDigests(
  input: z.output<typeof ingestProviderEventSchema>,
  blindIndex: BlindIndex,
): string[] {
  const digests = new Set<string>([
    blindIndex.digest("provider-idempotency", `${input.provider}\0${input.idempotencyKey}`),
  ]);
  const conversation =
    typeof input.payload.conversation === "object" && input.payload.conversation !== null
      ? (input.payload.conversation as Record<string, unknown>)
      : null;
  const chatId = conversation ? stringField(conversation, "id") : null;
  if (chatId) digests.add(blindIndex.digest("linq-chat", chatId));
  const mailboxEmail = stringField(input.payload, "mailboxEmail");
  if (mailboxEmail) digests.add(blindIndex.digest("gmail-mailbox", mailboxEmail.toLowerCase()));
  return [...digests].sort();
}

function stringField(record: Record<string, unknown>, key: string): string | null {
  const value = record[key];
  return typeof value === "string" && value.length > 0 ? value : null;
}

type AggregatePolicyRule = z.output<typeof HouseholdAggregateSchema>["policies"][number]["rule"];
type DurableRecordScope = { kind: "household" } | { kind: "personal"; adultId: string };

function canViewScope(scope: DurableRecordScope, viewerAdultId: string): boolean {
  return scope.kind === "household" || scope.adultId === viewerAdultId;
}

function canViewPolicyRule(rule: AggregatePolicyRule, viewerAdultId: string): boolean {
  switch (rule.kind) {
    case "routing":
    case "timing":
      return canViewScope(rule.scope, viewerAdultId);
    case "sharing":
      return rule.from.adultId === viewerAdultId;
    case "internal_action":
      return true;
  }
}

function pendingActionForExport(
  pending: HouseholdAggregate["pendingActions"][number],
  viewerAdultId: string,
): Record<string, unknown> {
  if (pending.action.kind !== "calendar_update" || pending.action.requestedByAdultId === viewerAdultId) {
    return pending;
  }
  const {
    targetConnectionId: _targetConnectionId,
    calendarId: _calendarId,
    relevantDataDigest: _relevantDataDigest,
    ...sharedAction
  } = pending.action;
  const sharedReceipt =
    pending.effectReceipt === undefined
      ? undefined
      : (({ providerReference: _providerReference, ...receipt }) => receipt)(pending.effectReceipt);
  return {
    ...pending,
    action: sharedAction,
    ...(sharedReceipt === undefined ? {} : { effectReceipt: sharedReceipt }),
  };
}

function projectionForExport(
  row: Record<string, unknown>,
  viewerAdultId: string,
  householdAdultCount: number,
): Record<string, unknown> {
  const state = row.state;
  const parsed = HouseholdApplicationSnapshotSchema.safeParse({
    revision: Number(row.version),
    aggregate:
      typeof state === "object" && state !== null && "aggregate" in state ? state.aggregate : undefined,
    projection:
      typeof state === "object" && state !== null && "projection" in state ? state.projection : undefined,
  });
  if (!parsed.success) {
    return householdAdultCount === 1
      ? row
      : { ...row, state: null, state_redacted: true, redaction_reason: "unscoped_legacy_projection" };
  }

  const aggregate = parsed.data.aggregate;
  const projection = parsed.data.projection;
  return {
    ...row,
    state: {
      aggregate: {
        ...aggregate,
        episodes: aggregate.episodes.filter((episode) => canViewScope(episode.scope, viewerAdultId)),
        policies: aggregate.policies.filter((policy) => canViewPolicyRule(policy.rule, viewerAdultId)),
        policyCandidates: aggregate.policyCandidates.filter((candidate) =>
          canViewPolicyRule(candidate.rule, viewerAdultId),
        ),
        approvals: aggregate.approvals.filter((approval) => approval.grantedByAdultId === viewerAdultId),
        memoryCandidates: aggregate.memoryCandidates.filter((candidate) =>
          canViewScope(candidate.scope, viewerAdultId),
        ),
        memories: aggregate.memories.filter((memory) => canViewScope(memory.scope, viewerAdultId)),
        pendingActions: aggregate.pendingActions
          .filter((action) => canViewScope(action.action.requestedFor, viewerAdultId))
          .map((action) => pendingActionForExport(action, viewerAdultId)),
      },
      projection: {
        ...projection,
        gmailTriage: projection.gmailTriage.filter((record) => record.ownerAdultId === viewerAdultId),
        gmailSources: projection.gmailSources.filter((record) => record.ownerAdultId === viewerAdultId),
        calendarTriage: projection.calendarTriage.filter((record) => record.ownerAdultId === viewerAdultId),
        calendarSources: projection.calendarSources.filter((record) => record.ownerAdultId === viewerAdultId),
        pendingPromotions: projection.pendingPromotions.filter(
          (promotion) => promotion.ownerAdultId === viewerAdultId,
        ),
        workers: projection.workers.filter(
          (worker) =>
            worker.job.scopeGrant.visibility === "household" ||
            worker.job.scopeGrant.adultId === viewerAdultId,
        ),
      },
    },
    state_redacted: true,
    redaction_reason: "viewer_scope",
  };
}

function applicationSnapshotForExport(
  row: Record<string, unknown>,
  viewerAdultId: string,
): Record<string, unknown> {
  const { aggregate, projection, revision, ...metadata } = row;
  const exported = projectionForExport(
    {
      ...metadata,
      version: revision,
      state: { aggregate, projection },
    },
    viewerAdultId,
    2,
  );
  const { version, ...rest } = exported;
  return { ...rest, revision: version };
}

function mapExternalConnection(
  row: ExternalConnectionRow,
  cipher: TenantJsonCipher,
): ExternalConnectionRecord {
  const details =
    row.details_key_id && row.details_ciphertext
      ? googleConnectionDetailsSchema.parse(
          cipher.open(
            { keyId: row.details_key_id, ciphertext: row.details_ciphertext },
            googleConnectionDetailsContext({
              householdId: row.household_id,
              adultId: row.adult_id,
              connectionId: row.id,
            }),
          ),
        )
      : { label: REVOKED_GOOGLE_ACCOUNT_LABEL, accountLabel: REVOKED_GOOGLE_ACCOUNT_LABEL, email: null };
  return {
    id: row.id,
    householdId: row.household_id,
    adultId: row.adult_id,
    provider: row.provider,
    label: details.label,
    externalAccountId: row.external_account_id,
    email: details.email,
    encryptedCredentials: row.encrypted_credentials,
    grantedScopes: row.granted_scopes,
    status: row.status,
    cursor: row.cursor,
    metadata: { ...row.metadata, accountLabel: details.accountLabel },
    lastSyncedAt: row.last_synced_at ? dateToString(row.last_synced_at) : null,
  };
}

function mapSourceItem(row: SourceItemRow): SourceItemRecord {
  return {
    id: row.id,
    householdId: row.household_id,
    connectionId: row.connection_id,
    ownerAdultId: row.owner_adult_id,
    visibility: row.visibility,
    provider: row.provider,
    externalId: row.external_id,
    kind: row.kind,
    occurredAt: dateToString(row.occurred_at),
    contentHash: row.content_hash,
    encryptedContent: row.encrypted_content,
    metadata: row.metadata,
    retentionUntil: row.retention_until ? dateToString(row.retention_until) : null,
    revision: Number(row.revision),
  };
}

function mapTimer(row: TimerRow, cipher: TenantJsonCipher): ClaimedTimer {
  if (
    !row.timer_key ||
    !row.lease_token ||
    !row.lease_expires_at ||
    !row.payload_key_id ||
    !row.payload_ciphertext
  ) {
    throw new ApplicationStoreError(
      "invalid_state",
      "Claimed timer is missing its durable identity or lease",
    );
  }
  return {
    rowId: row.id,
    timerKey: row.timer_key,
    householdId: row.household_id,
    episodeKey: row.episode_key,
    triggerKind: row.trigger_kind,
    planVersion: Number(row.plan_version),
    dueAt: dateToString(row.due_at),
    payload: jsonObjectSchema.parse(
      cipher.open(
        { keyId: row.payload_key_id, ciphertext: row.payload_ciphertext },
        encryptedJsonContext("household", row.household_id, "scheduled_triggers", row.id, "payload"),
      ),
    ),
    attempt: row.attempt,
    maxAttempts: row.max_attempts,
    leaseToken: row.lease_token,
    leaseExpiresAt: dateToString(row.lease_expires_at),
  };
}

function mapOutbox(row: OutboxRow, cipher: TenantJsonCipher): ClaimedOutboxItem {
  if (
    !row.intent_key ||
    !row.lease_token ||
    !row.lease_expires_at ||
    !row.payload_key_id ||
    !row.payload_ciphertext
  ) {
    throw new ApplicationStoreError("invalid_state", "Claimed outbox item is missing its identity or lease");
  }
  return {
    rowId: row.id,
    intentKey: row.intent_key,
    householdId: row.household_id,
    effectKind: row.effect_kind,
    idempotencyKey: row.idempotency_key,
    payload: jsonObjectSchema.parse(
      cipher.open(
        { keyId: row.payload_key_id, ciphertext: row.payload_ciphertext },
        encryptedJsonContext("household", row.household_id, "outbox", row.id, "payload"),
      ),
    ),
    attempt: row.attempt,
    maxAttempts: row.max_attempts,
    leaseToken: row.lease_token,
    leaseExpiresAt: dateToString(row.lease_expires_at),
  };
}

async function lockApplicationIdentity(
  transaction: TransactionSql<Record<string, never>>,
  namespace: string,
  identity: Record<string, unknown>,
): Promise<void> {
  const lockKey = `${namespace}:${payloadDigest(identity)}`;
  await transaction`
    select pg_advisory_xact_lock(hashtextextended(${lockKey}, 0::bigint))
  `;
}

async function requireWritableHousehold(
  transaction: TransactionSql<Record<string, never>>,
  householdId: string,
): Promise<void> {
  const rows = await transaction<{ status: string }[]>`
    select status from households where id = ${householdId} for update
  `;
  if (!rows[0]) throw new ApplicationStoreError("not_found", "Unknown household");
  if (rows[0].status === "deleting") {
    throw new ApplicationStoreError("invalid_state", "Household deletion is fenced");
  }
}

async function upsertTimerIntent(
  transaction: TransactionSql<Record<string, never>>,
  cipher: TenantJsonCipher,
  householdId: string,
  timer: z.output<typeof projectionTimerSchema>,
): Promise<string> {
  const rowId = randomUUID();
  const digest = payloadDigest(timer.payload);
  const definitionDigest = payloadDigest({
    episodeKey: timer.episodeKey ?? null,
    triggerKind: timer.triggerKind,
    planVersion: timer.planVersion,
    dueAt: timer.dueAt,
    maxAttempts: timer.maxAttempts,
    payloadDigest: digest,
  });
  const sealed = cipher.seal(
    timer.payload,
    encryptedJsonContext("household", householdId, "scheduled_triggers", rowId, "payload"),
  );
  const inserted = await transaction<{ id: string }[]>`
    insert into scheduled_triggers (
      id, household_id, timer_key, episode_key, trigger_kind,
      plan_version, due_at, available_at, status, definition_digest, payload_digest, payload_key_id,
      payload_ciphertext, max_attempts, control_epoch
    ) values (
      ${rowId}, ${householdId}, ${timer.timerKey}, ${timer.episodeKey ?? null},
      ${timer.triggerKind}, ${timer.planVersion}, ${timer.dueAt}, ${timer.dueAt},
      'scheduled', ${definitionDigest}, ${digest}, ${sealed.keyId}, ${sealed.ciphertext}, ${timer.maxAttempts},
      (select control_epoch from households where id = ${householdId})
    )
    on conflict (household_id, timer_key) where timer_key is not null
    do nothing
    returning id
  `;
  if (inserted[0]) return inserted[0].id;

  const existingRows = await transaction<{ id: string; definition_matches: boolean }[]>`
    select id,
      definition_digest = ${definitionDigest} as definition_matches
    from scheduled_triggers
    where household_id = ${householdId} and timer_key = ${timer.timerKey}
    for update
  `;
  const existing = existingRows[0];
  if (!existing) {
    throw new ApplicationStoreError("invalid_state", "Timer identity row disappeared");
  }
  if (!existing.definition_matches) {
    throw new ApplicationStoreError(
      "invalid_state",
      `Timer key was reused with a different definition: ${timer.timerKey}`,
    );
  }
  return existing.id;
}

async function insertOutboxIntent(
  transaction: TransactionSql<Record<string, never>>,
  cipher: TenantJsonCipher,
  householdId: string,
  effect: z.output<typeof projectionOutboxSchema>,
): Promise<string> {
  const rowId = randomUUID();
  const hash = payloadDigest({ effectKind: effect.effectKind, payload: effect.payload });
  const sealed = cipher.seal(
    effect.payload,
    encryptedJsonContext("household", householdId, "outbox", rowId, "payload"),
  );
  const inserted = await transaction<{ id: string }[]>`
    insert into outbox (
      id, household_id, intent_key, effect_kind, idempotency_key, payload_digest,
      payload_key_id, payload_ciphertext, status, max_attempts, control_epoch
    ) values (
      ${rowId}, ${householdId}, ${effect.intentKey}, ${effect.effectKind},
      ${effect.idempotencyKey}, ${hash}, ${sealed.keyId}, ${sealed.ciphertext},
      'pending', ${effect.maxAttempts},
      (select control_epoch from households where id = ${householdId})
    )
    on conflict do nothing
    returning id
  `;
  if (inserted[0]) return inserted[0].id;

  const existingRows = await transaction<OutboxIdentityRow[]>`
    select id, household_id, intent_key, effect_kind, idempotency_key, payload_digest
    from outbox
    where idempotency_key = ${effect.idempotencyKey}
      or (household_id = ${householdId} and intent_key = ${effect.intentKey})
    for update
  `;
  const matches = existingRows.filter(
    (row) =>
      row.household_id === householdId &&
      row.intent_key === effect.intentKey &&
      row.effect_kind === effect.effectKind &&
      row.idempotency_key === effect.idempotencyKey &&
      row.payload_digest === hash,
  );
  if (existingRows.length !== 1 || matches.length !== 1) {
    throw new OutboxIdempotencyConflictError(effect.idempotencyKey);
  }
  const existing = matches[0];
  if (!existing) {
    throw new ApplicationStoreError("invalid_state", "Outbox identity row disappeared");
  }
  return existing.id;
}

async function insertAudit(
  transaction: TransactionSql<Record<string, never>>,
  database: Database,
  householdId: string,
  sequence: number,
  audit: z.output<typeof projectionAuditSchema>,
): Promise<void> {
  await transaction`
    insert into audit_log (
      id, household_id, sequence, actor_kind, actor_id, action, target_type,
      target_id, visibility, owner_adult_id, source_refs, policy_refs, details
    ) values (
      ${audit.id ?? randomUUID()}, ${householdId}, ${sequence}, ${audit.actorKind},
      ${audit.actorId ?? null}, ${audit.action}, ${audit.targetType},
      ${audit.targetId ?? null}, ${audit.visibility}, ${audit.ownerAdultId ?? null},
      ${json(database, audit.sourceRefs)}, ${json(database, audit.policyRefs)},
      ${json(database, audit.details)}
    )
  `;
}
