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
} from "../application/contracts.js";
import type {
  ApplicationCommit,
  ApplicationCommitResult,
  ApplicationRepositoryPort,
} from "../application/ports.js";
import { DomainChangeSchema, HouseholdAggregateSchema, HouseholdSignalSchema } from "../domain/index.js";
import { payloadDigest } from "../security/canonical-json.js";
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

export class ApplicationStoreError extends Error {
  public constructor(
    public readonly code:
      | "not_found"
      | "not_authorized"
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
  subject: string | null;
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
  idempotency_key: string;
  payload_hash: string;
  authentication: Record<string, unknown>;
  event_kind: string;
  occurred_at: Date;
  payload: Record<string, unknown>;
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
  state: Record<string, unknown>;
  updated_at: Date;
};

type ApplicationSnapshotRow = {
  household_id: string;
  schema_version: number;
  revision: string;
  aggregate: unknown;
  projection: unknown;
  updated_at: Date;
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
  label: z.string().trim().min(1).max(200),
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
    subject: z.string().max(2000).optional(),
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
  public constructor(private readonly database: Database) {}

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
          household_id, schema_version, revision, aggregate, projection
        ) values (
          ${householdId}, ${parsed.schemaVersion}, ${parsed.snapshot.revision},
          ${json(this.database, parsed.snapshot.aggregate)},
          ${json(this.database, parsed.snapshot.projection)}
        )
        on conflict (household_id) do nothing
        returning household_id
      `;
      const rows = await transaction<ApplicationSnapshotRow[]>`
        select household_id, schema_version, revision, aggregate, projection, updated_at
        from application_snapshots where household_id = ${householdId}
      `;
      const snapshot = rows[0];
      if (!snapshot) {
        throw new ApplicationStoreError("invalid_state", "Application snapshot disappeared");
      }
      const mapped = mapApplicationSnapshot(snapshot);
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
      select household_id, schema_version, revision, aggregate, projection, updated_at
      from application_snapshots where household_id = ${parsedId}
    `;
    return rows[0] ? mapApplicationSnapshot(rows[0]) : null;
  }

  public async findProcessed(householdId: string, idempotencyKey: string): Promise<ApplicationResult | null> {
    const parsedHouseholdId = z.uuid().parse(householdId);
    const parsedKey = z.string().min(1).max(512).parse(idempotencyKey);
    const rows = await this.database<{ revision: string; outcome: unknown }[]>`
      select revision, outcome
      from application_commits
      where household_id = ${parsedHouseholdId} and idempotency_key = ${parsedKey}
    `;
    const row = rows[0];
    return row
      ? ApplicationResultSchema.parse({
          householdId: parsedHouseholdId,
          idempotencyKey: parsedKey,
          disposition: "committed",
          revision: Number(row.revision),
          outcome: row.outcome,
        })
      : null;
  }

  public async commit(input: ApplicationCommit): Promise<ApplicationCommitResult> {
    const parsed = applicationCommitSchema.parse(input);
    const commitHash = payloadDigest({
      aggregate: parsed.aggregate,
      projection: parsed.projection,
      signals: parsed.signals,
      changes: parsed.changes,
      outbox: parsed.outbox,
      audit: parsed.audit,
      outcome: parsed.outcome,
    });

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

      const priorRows = await transaction<{ revision: string; outcome: unknown; commit_hash: string }[]>`
        select revision, outcome, commit_hash
        from application_commits
        where household_id = ${parsed.householdId}
          and idempotency_key = ${parsed.idempotencyKey}
      `;
      const prior = priorRows[0];
      if (prior) {
        if (prior.commit_hash !== commitHash) {
          throw new ApplicationStoreError(
            "invalid_state",
            "Application idempotency key was reused with different commit content",
          );
        }
        return {
          disposition: "duplicate" as const,
          revision: Number(prior.revision),
          outcome: ApplicationOutcomeSchema.parse(prior.outcome),
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
      await transaction`
        update application_snapshots
        set revision = ${revision}, aggregate = ${json(this.database, parsed.aggregate)},
          projection = ${json(this.database, parsed.projection)}, updated_at = now()
        where household_id = ${parsed.householdId}
      `;

      // The application projection is authoritative for the names each adult asked Florence to use.
      // Keep the identity directory in the same transaction so invitations, exports, and operator
      // views never continue showing onboarding placeholders after a successful naming turn.
      for (const adult of parsed.projection.onboarding.adultNames) {
        await transaction`
          update adults
          set display_name = ${adult.displayName}, updated_at = now()
          where id = ${adult.adultId}
            and exists (
              select 1 from household_memberships membership
              where membership.household_id = ${parsed.householdId}
                and membership.adult_id = adults.id
                and membership.status in ('invited', 'active')
            )
        `;
      }

      for (const intent of parsed.outbox) {
        await insertOutboxIntent(transaction, this.database, parsed.householdId, {
          intentKey: intent.intentId,
          effectKind: intent.kind,
          idempotencyKey: intent.idempotencyKey,
          payload: intent,
          maxAttempts: 8,
        });
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

      await transaction`
        insert into application_commits (
          id, household_id, idempotency_key, commit_hash, base_revision, revision,
          signals, changes, outcome
        ) values (
          ${randomUUID()}, ${parsed.householdId}, ${parsed.idempotencyKey}, ${commitHash},
          ${parsed.expectedRevision}, ${revision}, ${json(this.database, parsed.signals)},
          ${json(this.database, parsed.changes)}, ${json(this.database, parsed.outcome)}
        )
      `;
      await transaction`
        update households
        set next_audit_sequence = ${auditSequence}, updated_at = now()
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
    const hash = payloadDigest({
      provider: input.provider,
      eventKind: input.eventKind,
      occurredAt: input.occurredAt,
      payload: input.payload,
    });

    return this.database.begin(async (transaction) => {
      const inserted = await transaction<{ id: string; status: ProviderInboxReceipt["status"] }[]>`
        insert into provider_inbox (
          id, provider, idempotency_key, payload_hash, authentication, event_kind,
          occurred_at, payload, status, max_attempts
        ) values (
          ${inboxId}, ${input.provider}, ${input.idempotencyKey}, ${hash},
          ${json(this.database, input.authentication)}, ${input.eventKind}, ${input.occurredAt},
          ${json(this.database, input.payload)}, 'pending', ${input.maxAttempts}
        )
        on conflict (provider, idempotency_key) do nothing
        returning id, status
      `;
      if (inserted[0]) {
        return { inboxId: inserted[0].id, disposition: "accepted", status: inserted[0].status };
      }

      const existingRows = await transaction<
        { id: string; payload_hash: string; status: ProviderInboxReceipt["status"] }[]
      >`
        select id, payload_hash, status
        from provider_inbox
        where provider = ${input.provider} and idempotency_key = ${input.idempotencyKey}
        for update
      `;
      const existing = existingRows[0];
      if (!existing) {
        throw new ApplicationStoreError("invalid_state", "Provider inbox conflict row disappeared");
      }
      if (existing.payload_hash === hash) {
        return { inboxId: existing.id, disposition: "duplicate", status: existing.status };
      }

      await transaction`
        update provider_inbox
        set status = 'quarantined', quarantine_reason = 'idempotency_hash_conflict',
            lease_owner = null, lease_token = null, lease_expires_at = null, updated_at = now()
        where id = ${existing.id}
      `;
      await transaction`
        insert into provider_inbox_conflicts (id, inbox_id, payload_hash, payload)
        values (${randomUUID()}, ${existing.id}, ${hash}, ${json(this.database, input.payload)})
        on conflict (inbox_id, payload_hash) do nothing
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
          lease_expires_at = null, last_error_code = 'lease_expired_after_max_attempts',
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
        returning provider_inbox.id, provider_inbox.provider, provider_inbox.idempotency_key,
          provider_inbox.payload_hash, provider_inbox.authentication, provider_inbox.event_kind,
          provider_inbox.occurred_at, provider_inbox.payload, provider_inbox.status,
          provider_inbox.attempt, provider_inbox.max_attempts, provider_inbox.lease_token,
          provider_inbox.lease_expires_at
      `;
      return rows.map((row) => ({
        id: row.id,
        provider: row.provider,
        idempotencyKey: row.idempotency_key,
        payloadHash: row.payload_hash,
        authentication: row.authentication,
        eventKind: row.event_kind,
        occurredAt: dateToString(row.occurred_at),
        payload: row.payload,
        attempt: row.attempt,
        maxAttempts: row.max_attempts,
        leaseToken: row.lease_token,
        leaseExpiresAt: dateToString(row.lease_expires_at),
      }));
    });
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
    const rows = await this.database<{ id: string }[]>`
      update provider_inbox
      set status = 'resolved', household_id = ${parsed.householdId ?? null},
          resolution = ${json(this.database, parsed.resolution)}, resolved_at = now(),
          lease_owner = null, lease_token = null, lease_expires_at = null, updated_at = now()
      where id = ${parsed.inboxId} and status = 'leased' and lease_token = ${parsed.leaseToken}
      returning id
    `;
    return rows.length === 1;
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
          updated_at = now()
      where id = ${parsed.inboxId} and status = 'leased' and lease_token = ${parsed.leaseToken}
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

    await this.database.begin(async (transaction) => {
      await transaction`
        insert into households (id, name, timezone, status, version)
        values (${householdId}, ${input.householdName}, ${input.timeZone}, 'onboarding', 0)
      `;
      await transaction`
        insert into adults (id, display_name, timezone)
        values (${adultId}, ${input.adultDisplayName}, ${input.timeZone})
      `;
      await transaction`
        insert into household_memberships (household_id, adult_id, role, status, consented_at)
        values (
          ${householdId}, ${adultId}, 'owner', ${membershipStatus}, ${consentedAt}
        )
      `;
      await transaction`
        insert into household_projections (household_id, schema_version, version, state)
        values (
          ${householdId}, ${input.projectionSchemaVersion}, 0,
          ${json(this.database, input.initialProjection)}
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
      await transaction`
        insert into adults (id, display_name, timezone)
        values (${adultId}, ${parsed.displayName}, ${parsed.timeZone ?? null})
      `;
      await transaction`
        insert into household_memberships (
          household_id, adult_id, role, status, consented_at
        ) values (
          ${parsed.householdId}, ${adultId}, ${parsed.role}, ${parsed.status},
          ${parsed.consentedAt ?? null}
        )
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
      const inserted = await transaction<{ household_id: string }[]>`
        insert into household_projections (household_id, schema_version, version, state)
        values (
          ${parsed.householdId}, ${parsed.schemaVersion}, ${parsed.version},
          ${json(this.database, parsed.state)}
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
        select household_id, schema_version, version, state, updated_at
        from household_projections where household_id = ${parsed.householdId}
      `;
      const projection = rows[0];
      if (!projection) {
        throw new ApplicationStoreError("invalid_state", "Household projection disappeared");
      }
      return { created: inserted.length === 1, projection: mapProjection(projection) };
    });
  }

  public async getHouseholdProjection(householdId: string): Promise<HouseholdProjection | null> {
    const parsedId = z.uuid().parse(householdId);
    const rows = await this.database<ProjectionRow[]>`
      select household_id, schema_version, version, state, updated_at
      from household_projections where household_id = ${parsedId}
    `;
    return rows[0] ? mapProjection(rows[0]) : null;
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
      await transaction`
        update household_projections
        set schema_version = ${parsed.schemaVersion}, version = ${nextVersion},
            state = ${json(this.database, parsed.nextState)}, updated_at = now()
        where household_id = ${parsed.householdId}
      `;

      if (parsed.cancelTimerKeys.length > 0) {
        await transaction`
          update scheduled_triggers
          set status = 'superseded', lease_owner = null, lease_token = null,
              lease_expires_at = null, updated_at = now()
          where household_id = ${parsed.householdId}
            and timer_key = any(${parsed.cancelTimerKeys})
            and status in ('scheduled', 'claimed')
        `;
      }
      for (const timer of parsed.timers) {
        await upsertTimerIntent(transaction, this.database, parsed.householdId, timer);
      }

      for (const effect of parsed.outbox) {
        await insertOutboxIntent(transaction, this.database, parsed.householdId, effect);
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
        rowId: await upsertTimerIntent(transaction, this.database, parsed.householdId, parsed),
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
            lease_expires_at = null, updated_at = now()
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

  public async upsertExternalConnection(
    rawInput: z.input<typeof connectionSchema>,
  ): Promise<{ connectionId: string }> {
    const input = connectionSchema.parse(rawInput);
    const requestedId = input.id ?? randomUUID();
    return this.database.begin(async (transaction) => {
      const memberships = await transaction<{ adult_id: string }[]>`
        select adult_id from household_memberships
        where household_id = ${input.householdId} and adult_id = ${input.adultId}
          and status = 'active'
        for update
      `;
      if (memberships.length !== 1) {
        throw new ApplicationStoreError("not_authorized", "Adult is not active in this household");
      }
      const rows = await transaction<{ id: string }[]>`
        insert into external_connections (
          id, household_id, adult_id, provider, label, external_account_id, email,
          encrypted_credentials, granted_scopes, status, cursor, metadata, last_synced_at
        ) values (
          ${requestedId}, ${input.householdId}, ${input.adultId}, ${input.provider},
          ${input.label}, ${input.externalAccountId}, ${input.email ?? null},
          ${input.encryptedCredentials}, ${input.grantedScopes}, 'active',
          ${json(this.database, input.cursor)}, ${json(this.database, input.metadata)},
          ${input.lastSyncedAt ?? null}
        )
        on conflict (household_id, adult_id, provider, external_account_id)
        do update set label = excluded.label, email = excluded.email,
          encrypted_credentials = excluded.encrypted_credentials,
          granted_scopes = excluded.granted_scopes, status = 'active', cursor = excluded.cursor,
          metadata = excluded.metadata, last_synced_at = excluded.last_synced_at, updated_at = now()
        returning id
      `;
      const row = rows[0];
      if (!row) throw new ApplicationStoreError("invalid_state", "Connection upsert returned no row");
      return { connectionId: row.id };
    });
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
      select id, household_id, adult_id, provider, label, external_account_id, email,
        encrypted_credentials, granted_scopes, status, cursor, metadata, last_synced_at
      from external_connections
      where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
        and adult_id = ${parsed.adultId}
    `;
    return rows[0] ? mapExternalConnection(rows[0]) : null;
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
            cursor = '{}'::jsonb, updated_at = now()
        where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and adult_id = ${parsed.adultId} and status <> 'revoked'
        returning id
      `;
      if (rows.length !== 1) return false;
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
            external_id, kind, occurred_at, subject, content_hash, encrypted_content,
            metadata, retention_until, revision
          ) values (
            ${sourceItemId}, ${input.householdId}, ${input.connectionId ?? null},
            ${input.ownerAdultId ?? null}, ${input.visibility}, ${input.provider},
            ${input.externalId}, ${input.kind}, ${input.occurredAt}, ${input.subject ?? null},
            ${input.contentHash}, ${input.encryptedContent ?? null},
            ${json(this.database, persistedMetadata)}, ${input.retentionUntil ?? null}, 1
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
      const retainedExistingGmailSource =
        input.provider === "gmail" && input.kind === "gmail_message"
          ? existing.kind === "gmail_message_deleted"
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
            set retention_until = ${input.retentionUntil ?? null},
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
          set retention_until = ${input.retentionUntil ?? null}, metadata = ${json(this.database, persistedMetadata)},
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
            subject = ${input.subject ?? null}, content_hash = ${input.contentHash},
            encrypted_content = ${input.encryptedContent ?? null}, metadata = ${json(this.database, persistedMetadata)},
            retention_until = ${input.retentionUntil ?? null}, revision = ${revision}, updated_at = now()
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
        external_id, kind, occurred_at, subject, content_hash, encrypted_content,
        metadata, retention_until, revision
      from source_items
      where id = ${parsed.sourceItemId} and household_id = ${parsed.householdId}
        and (visibility = 'household' or owner_adult_id = ${parsed.viewerAdultId})
    `;
    return rows[0] ? mapSourceItem(rows[0]) : null;
  }

  public async purgeExpiredSourceContent(asOf: string): Promise<number> {
    const parsed = instantSchema.parse(asOf);
    const rows = await this.database<{ id: string }[]>`
      update source_items
      set subject = null, encrypted_content = null, updated_at = now()
      where retention_until is not null and retention_until <= ${parsed}
        and (subject is not null or encrypted_content is not null)
      returning id
    `;
    return rows.length;
  }

  public async claimDueTimers(input: {
    owner: string;
    limit: number;
    leaseSeconds: number;
  }): Promise<ClaimedTimer[]> {
    const parsed = leaseInputSchema.parse(input);
    const leaseToken = randomUUID();
    const rows = await this.database<TimerRow[]>`
      with candidates as (
        select trigger.id
        from scheduled_triggers trigger
        join households household on household.id = trigger.household_id
        where (
          (trigger.status = 'scheduled' and trigger.due_at <= now() and trigger.available_at <= now())
          or (trigger.status = 'claimed' and trigger.lease_expires_at < now())
        )
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
        scheduled_triggers.due_at, scheduled_triggers.payload, scheduled_triggers.attempt,
        scheduled_triggers.lease_token, scheduled_triggers.lease_expires_at
    `;
    return rows.map(mapTimer);
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
          lease_expires_at = null, updated_at = now()
      where id = ${parsed.rowId} and status = 'claimed' and lease_token = ${parsed.leaseToken}
      returning id
    `;
    return rows.length === 1;
  }

  public async releaseTimer(input: {
    rowId: string;
    leaseToken: string;
    retryAt: string;
    errorCode: string;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        retryAt: instantSchema,
        errorCode: z.string().min(1).max(200),
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update scheduled_triggers
      set status = 'scheduled', available_at = ${parsed.retryAt}, last_error_code = ${parsed.errorCode},
          lease_owner = null, lease_token = null, lease_expires_at = null, updated_at = now()
      where id = ${parsed.rowId} and status = 'claimed' and lease_token = ${parsed.leaseToken}
      returning id
    `;
    return rows.length === 1;
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
          lease_expires_at = null, last_error_code = 'lease_expired_after_max_attempts',
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
          outbox.payload, outbox.attempt, outbox.max_attempts,
          outbox.lease_token, outbox.lease_expires_at
      `;
      return rows.map(mapOutbox);
    });
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
    const rowId = await insertOutboxIntent(transaction, this.database, intent.householdId, {
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
          updated_at = now()
      where id = ${parsed.rowId} and status = 'leased' and lease_token = ${parsed.leaseToken}
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
          lease_owner = null, lease_token = null, lease_expires_at = null, updated_at = now()
      where id = ${parsed.rowId} and status = 'leased' and lease_token = ${parsed.leaseToken}
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
          lease_token = null, lease_expires_at = null, updated_at = now()
      where id = ${parsed.rowId} and status = 'leased' and lease_token = ${parsed.leaseToken}
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
      const adults = await transaction<Record<string, unknown>[]>`
        select a.id, a.display_name, a.timezone, hm.role, hm.status, hm.consented_at,
          hm.created_at, hm.updated_at
        from household_memberships hm join adults a on a.id = hm.adult_id
        where hm.household_id = ${parsed.householdId}
        order by hm.created_at, a.id
      `;
      const channels = await transaction<Record<string, unknown>[]>`
        select id, adult_id, provider, channel_type, external_chat_id, external_handle,
          status, metadata, created_at, updated_at
        from channel_bindings
        where household_id = ${parsed.householdId}
          and (channel_type = 'group' or adult_id = ${parsed.requestedByAdultId})
        order by created_at, id
      `;
      const connections = await transaction<Record<string, unknown>[]>`
        select id, adult_id, provider, label, external_account_id, email, granted_scopes,
          status, cursor, metadata, last_synced_at, created_at, updated_at
        from external_connections
        where household_id = ${parsed.householdId} and adult_id = ${parsed.requestedByAdultId}
        order by created_at, id
      `;
      const sources = await transaction<Record<string, unknown>[]>`
        select id, connection_id, owner_adult_id, visibility, provider, external_id, kind,
          occurred_at, subject, content_hash, encrypted_content, metadata, retention_until,
          revision, created_at, updated_at
        from source_items
        where household_id = ${parsed.householdId}
          and (visibility = 'household' or owner_adult_id = ${parsed.requestedByAdultId})
        order by occurred_at, id
      `;
      const projection = await transaction<Record<string, unknown>[]>`
        select schema_version, version, state, created_at, updated_at
        from household_projections where household_id = ${parsed.householdId}
      `;
      const applicationSnapshot = await transaction<Record<string, unknown>[]>`
        select schema_version, revision, aggregate, projection, created_at, updated_at
        from application_snapshots where household_id = ${parsed.householdId}
      `;
      const applicationCommits = await transaction<Record<string, unknown>[]>`
        select idempotency_key, base_revision, revision, outcome, committed_at
        from application_commits where household_id = ${parsed.householdId}
        order by revision
      `;
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
        projection: projection[0]
          ? projectionForExport(projection[0], parsed.requestedByAdultId, adults.length)
          : null,
        applicationSnapshot: applicationSnapshot[0]
          ? applicationSnapshotForExport(applicationSnapshot[0], parsed.requestedByAdultId)
          : null,
        applicationCommits,
        audits,
      };
    });
  }

  public async requestHouseholdDeletion(input: {
    requestId?: string;
    householdId: string;
    requestedByAdultId: string;
    confirmationDigest: string;
  }): Promise<{ requestId: string }> {
    const parsed = z
      .strictObject({
        requestId: z.uuid().optional(),
        householdId: z.uuid(),
        requestedByAdultId: z.uuid(),
        confirmationDigest: z.string().min(32).max(256),
      })
      .parse(input);
    await this.assertActiveOwner(parsed.householdId, parsed.requestedByAdultId);
    const requestId = parsed.requestId ?? randomUUID();
    await this.database`
      insert into deletion_requests (
        id, household_id, requested_by_adult_id, status, confirmation_digest
      ) values (
        ${requestId}, ${parsed.householdId}, ${parsed.requestedByAdultId},
        'pending', ${parsed.confirmationDigest}
      )
    `;
    return { requestId };
  }

  public async confirmHouseholdDeletion(input: {
    requestId: string;
    confirmationDigest: string;
    confirmedAt: string;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        requestId: z.uuid(),
        confirmationDigest: z.string().min(32).max(256),
        confirmedAt: instantSchema,
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update deletion_requests
      set status = 'confirmed', confirmed_at = ${parsed.confirmedAt}
      where id = ${parsed.requestId} and status = 'pending'
        and confirmation_digest = ${parsed.confirmationDigest}
      returning id
    `;
    return rows.length === 1;
  }

  public async executeHouseholdDeletion(input: {
    requestId: string;
    completedAt: string;
  }): Promise<{ householdId: string; adultsDeleted: number }> {
    const parsed = z.strictObject({ requestId: z.uuid(), completedAt: instantSchema }).parse(input);
    return this.database.begin(async (transaction) => {
      const requests = await transaction<{ household_id: string; requested_by_adult_id: string }[]>`
        select household_id, requested_by_adult_id
        from deletion_requests
        where id = ${parsed.requestId} and status = 'confirmed'
        for update
      `;
      const request = requests[0];
      if (!request) {
        throw new ApplicationStoreError("invalid_state", "Deletion request is not confirmed");
      }
      const memberRows = await transaction<{ adult_id: string }[]>`
        select adult_id from household_memberships where household_id = ${request.household_id}
      `;
      const inboxDeleted = await transaction<{ id: string }[]>`
        delete from provider_inbox where household_id = ${request.household_id} returning id
      `;
      const report = {
        householdDeleted: true,
        providerInboxDeleted: inboxDeleted.length,
      };
      await transaction`
        insert into deletion_tombstones (
          request_id, household_id, requested_by_adult_id, completed_at, report
        ) values (
          ${parsed.requestId}, ${request.household_id}, ${request.requested_by_adult_id},
          ${parsed.completedAt}, ${json(this.database, report)}
        )
      `;
      const deleted = await transaction<{ id: string }[]>`
        delete from households where id = ${request.household_id} returning id
      `;
      if (deleted.length !== 1) {
        throw new ApplicationStoreError("not_found", "Household disappeared during deletion");
      }
      let adultsDeleted = 0;
      for (const member of memberRows) {
        const removed = await transaction<{ id: string }[]>`
          delete from adults
          where id = ${member.adult_id}
            and not exists (
              select 1 from household_memberships where adult_id = ${member.adult_id}
            )
          returning id
        `;
        adultsDeleted += removed.length;
      }
      return { householdId: request.household_id, adultsDeleted };
    });
  }

  public async getDeletionTombstone(requestId: string): Promise<{
    requestId: string;
    householdId: string;
    requestedByAdultId: string;
    completedAt: string;
    report: Record<string, unknown>;
  } | null> {
    const parsedId = z.uuid().parse(requestId);
    const rows = await this.database<
      {
        request_id: string;
        household_id: string;
        requested_by_adult_id: string;
        completed_at: Date;
        report: Record<string, unknown>;
      }[]
    >`
      select request_id, household_id, requested_by_adult_id, completed_at, report
      from deletion_tombstones where request_id = ${parsedId}
    `;
    const row = rows[0];
    return row
      ? {
          requestId: row.request_id,
          householdId: row.household_id,
          requestedByAdultId: row.requested_by_adult_id,
          completedAt: dateToString(row.completed_at),
          report: row.report,
        }
      : null;
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

  private async assertActiveOwner(householdId: string, adultId: string): Promise<void> {
    const rows = await this.database<{ adult_id: string }[]>`
      select adult_id from household_memberships
      where household_id = ${householdId} and adult_id = ${adultId}
        and role = 'owner' and status = 'active'
    `;
    if (!rows[0]) {
      throw new ApplicationStoreError("not_authorized", "Adult is not an active household owner");
    }
  }
}

type ExternalConnectionRow = {
  id: string;
  household_id: string;
  adult_id: string;
  provider: "google";
  label: string;
  external_account_id: string;
  email: string | null;
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
  subject: string | null;
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
  payload: Record<string, unknown>;
  attempt: number;
  lease_token: string | null;
  lease_expires_at: Date | null;
};

type OutboxRow = {
  id: string;
  intent_key: string | null;
  household_id: string;
  effect_kind: string;
  idempotency_key: string;
  payload: Record<string, unknown>;
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
  payload_hash: string | null;
};

function mapProjection(row: ProjectionRow): HouseholdProjection {
  return {
    householdId: row.household_id,
    schemaVersion: row.schema_version,
    version: Number(row.version),
    state: row.state,
    updatedAt: dateToString(row.updated_at),
  };
}

function mapApplicationSnapshot(row: ApplicationSnapshotRow): HouseholdApplicationSnapshot {
  try {
    return HouseholdApplicationSnapshotSchema.parse({
      revision: Number(row.revision),
      aggregate: row.aggregate,
      projection: row.projection,
    });
  } catch {
    throw new ApplicationStoreError(
      "invalid_state",
      "Household projection does not contain a valid application snapshot",
    );
  }
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
        pendingActions: aggregate.pendingActions.filter((action) =>
          canViewScope(action.action.requestedFor, viewerAdultId),
        ),
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

function mapExternalConnection(row: ExternalConnectionRow): ExternalConnectionRecord {
  return {
    id: row.id,
    householdId: row.household_id,
    adultId: row.adult_id,
    provider: row.provider,
    label: row.label,
    externalAccountId: row.external_account_id,
    email: row.email,
    encryptedCredentials: row.encrypted_credentials,
    grantedScopes: row.granted_scopes,
    status: row.status,
    cursor: row.cursor,
    metadata: row.metadata,
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
    subject: row.subject,
    contentHash: row.content_hash,
    encryptedContent: row.encrypted_content,
    metadata: row.metadata,
    retentionUntil: row.retention_until ? dateToString(row.retention_until) : null,
    revision: Number(row.revision),
  };
}

function mapTimer(row: TimerRow): ClaimedTimer {
  if (!row.timer_key || !row.lease_token || !row.lease_expires_at) {
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
    payload: row.payload,
    attempt: row.attempt,
    leaseToken: row.lease_token,
    leaseExpiresAt: dateToString(row.lease_expires_at),
  };
}

function mapOutbox(row: OutboxRow): ClaimedOutboxItem {
  if (!row.intent_key || !row.lease_token || !row.lease_expires_at) {
    throw new ApplicationStoreError("invalid_state", "Claimed outbox item is missing its identity or lease");
  }
  return {
    rowId: row.id,
    intentKey: row.intent_key,
    householdId: row.household_id,
    effectKind: row.effect_kind,
    idempotencyKey: row.idempotency_key,
    payload: row.payload,
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
  database: Database,
  householdId: string,
  timer: z.output<typeof projectionTimerSchema>,
): Promise<string> {
  const rowId = randomUUID();
  const inserted = await transaction<{ id: string }[]>`
    insert into scheduled_triggers (
      id, household_id, episode_id, timer_key, episode_key, trigger_kind,
      plan_version, due_at, available_at, status, payload, control_epoch
    ) values (
      ${rowId}, ${householdId}, null, ${timer.timerKey}, ${timer.episodeKey ?? null},
      ${timer.triggerKind}, ${timer.planVersion}, ${timer.dueAt}, ${timer.dueAt},
      'scheduled', ${json(database, timer.payload)},
      (select control_epoch from households where id = ${householdId})
    )
    on conflict (household_id, timer_key) where timer_key is not null
    do nothing
    returning id
  `;
  if (inserted[0]) return inserted[0].id;

  const existingRows = await transaction<{ id: string; definition_matches: boolean }[]>`
    select id,
      episode_key is not distinct from ${timer.episodeKey ?? null}
        and trigger_kind = ${timer.triggerKind}
        and plan_version = ${timer.planVersion}
        and due_at = ${timer.dueAt}
        and payload = ${json(database, timer.payload)} as definition_matches
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
  database: Database,
  householdId: string,
  effect: z.output<typeof projectionOutboxSchema>,
): Promise<string> {
  const rowId = randomUUID();
  const hash = payloadDigest({ effectKind: effect.effectKind, payload: effect.payload });
  const inserted = await transaction<{ id: string }[]>`
    insert into outbox (
      id, household_id, intent_key, effect_kind, idempotency_key, payload,
      payload_hash, status, max_attempts, control_epoch
    ) values (
      ${rowId}, ${householdId}, ${effect.intentKey}, ${effect.effectKind},
      ${effect.idempotencyKey}, ${json(database, effect.payload)}, ${hash},
      'pending', ${effect.maxAttempts},
      (select control_epoch from households where id = ${householdId})
    )
    on conflict do nothing
    returning id
  `;
  if (inserted[0]) return inserted[0].id;

  const existingRows = await transaction<OutboxIdentityRow[]>`
    select id, household_id, intent_key, effect_kind, idempotency_key, payload_hash
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
      row.payload_hash === hash,
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
