import { randomUUID } from "node:crypto";
import type { JSONValue } from "postgres";
import { z } from "zod";
import { payloadDigest } from "../security/canonical-json.js";
import type { Database } from "./client.js";

const jsonObjectSchema = z.record(z.string(), z.unknown());

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

export const acceptSignalInputSchema = z
  .object({
    id: z.uuid().optional(),
    householdId: z.uuid(),
    idempotencyKey: z.string().min(1).max(512),
    kind: z.string().min(1).max(128),
    actorKind: z.enum(["adult", "integration", "clock", "worker", "operator"]),
    actorId: z.string().min(1).max(256).optional(),
    visibility: z.enum(["personal", "household"]),
    ownerAdultId: z.uuid().optional(),
    occurredAt: z.iso.datetime({ offset: true }),
    payload: jsonObjectSchema,
    jobKind: z.string().min(1).max(128).default("process_signal"),
  })
  .superRefine((input, context) => {
    if (input.visibility === "personal" && !input.ownerAdultId) {
      context.addIssue({
        code: "custom",
        path: ["ownerAdultId"],
        message: "Personal signals require an ownerAdultId",
      });
    }
    if (input.visibility === "household" && input.ownerAdultId) {
      context.addIssue({
        code: "custom",
        path: ["ownerAdultId"],
        message: "Household signals cannot have a personal owner",
      });
    }
  });

export type AcceptSignalInput = z.input<typeof acceptSignalInputSchema>;

export type AcceptanceReceipt = {
  signalId: string;
  householdId: string;
  sequence: number;
  disposition: "accepted" | "duplicate";
  jobId: string | null;
};

export class IdempotencyConflictError extends Error {
  readonly code = "idempotency_conflict";

  constructor(readonly idempotencyKey: string) {
    super(`Idempotency key was reused with different content: ${idempotencyKey}`);
    this.name = "IdempotencyConflictError";
  }
}

export class StaleHouseholdVersionError extends Error {
  readonly code = "stale_household_version";

  constructor(
    readonly expected: number,
    readonly actual: number,
  ) {
    super(`Household changed while work was running (expected ${expected}, found ${actual})`);
    this.name = "StaleHouseholdVersionError";
  }
}

export type ClaimedJob = {
  id: string;
  householdId: string;
  signalId: string | null;
  kind: string;
  payload: Record<string, unknown>;
  attempt: number;
  maxAttempts: number;
  leaseToken: string;
  leaseExpiresAt: string;
};

export type EpisodeMutation = {
  id: string;
  episodeType: "commitment" | "research" | "meal_plan" | "household_project";
  visibility: "personal" | "household";
  ownerAdultId?: string;
  status: "proposed" | "awaiting_owner" | "open" | "blocked" | "handled" | "dismissed" | "superseded";
  title: string;
  acceptedMeaning: Record<string, unknown>;
  sourceRefs: unknown[];
  authority: Record<string, unknown>;
  temporalPlan?: Record<string, unknown>;
  outcome?: Record<string, unknown>;
  expectedVersion?: number;
};

export type TriggerIntent = {
  id: string;
  episodeId?: string;
  triggerKind: string;
  planVersion: number;
  dueAt: string;
  payload: Record<string, unknown>;
};

export type OutboxIntent = {
  id: string;
  effectKind: string;
  idempotencyKey: string;
  payload: Record<string, unknown>;
};

export type AuditIntent = {
  id: string;
  actorKind: string;
  actorId?: string;
  action: string;
  targetType: string;
  targetId?: string;
  sourceRefs?: unknown[];
  policyRefs?: unknown[];
  details: Record<string, unknown>;
};

export type HouseholdTransition = {
  householdId: string;
  signalId: string;
  expectedHouseholdVersion: number;
  episodes?: EpisodeMutation[];
  cancelTriggerIds?: string[];
  triggers?: TriggerIntent[];
  outbox?: OutboxIntent[];
  audits: AuditIntent[];
};

type ExistingSignal = {
  id: string;
  household_id: string;
  sequence: string;
  payload_hash: string;
};

type JobRow = {
  id: string;
  household_id: string;
  signal_id: string | null;
  kind: string;
  payload: Record<string, unknown>;
  attempt: number;
  max_attempts: number;
  lease_token: string;
  lease_expires_at: Date;
};

function receiptFromExisting(
  existing: ExistingSignal,
  payloadHash: string,
  idempotencyKey: string,
): AcceptanceReceipt {
  if (existing.payload_hash !== payloadHash) {
    throw new IdempotencyConflictError(idempotencyKey);
  }
  return {
    signalId: existing.id,
    householdId: existing.household_id,
    sequence: Number(existing.sequence),
    disposition: "duplicate",
    jobId: null,
  };
}

function isUniqueViolation(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === "23505";
}

export class FlorenceStore {
  constructor(private readonly database: Database) {}

  async createFoundingHousehold(input: {
    householdId?: string;
    adultId?: string;
    householdName: string;
    adultName: string;
    timezone: string;
  }): Promise<{ householdId: string; adultId: string }> {
    const householdId = input.householdId ?? randomUUID();
    const adultId = input.adultId ?? randomUUID();
    await this.database.begin(async (transaction) => {
      await transaction`
        insert into households (id, name, timezone, status)
        values (${householdId}, ${input.householdName}, ${input.timezone}, 'onboarding')
      `;
      await transaction`
        insert into adults (id, display_name, timezone)
        values (${adultId}, ${input.adultName}, ${input.timezone})
      `;
      await transaction`
        insert into household_memberships (household_id, adult_id, role, status, consented_at)
        values (${householdId}, ${adultId}, 'owner', 'active', now())
      `;
    });
    return { householdId, adultId };
  }

  async acceptSignal(rawInput: AcceptSignalInput): Promise<AcceptanceReceipt> {
    const input = acceptSignalInputSchema.parse(rawInput);
    const signalId = input.id ?? randomUUID();
    const jobId = randomUUID();
    const hash = payloadDigest({
      householdId: input.householdId,
      kind: input.kind,
      actorKind: input.actorKind,
      actorId: input.actorId,
      visibility: input.visibility,
      ownerAdultId: input.ownerAdultId,
      occurredAt: input.occurredAt,
      payload: input.payload,
    });

    const existing = await this.database<ExistingSignal[]>`
      select id, household_id, sequence, payload_hash
      from household_signals
      where idempotency_key = ${input.idempotencyKey}
    `;
    if (existing[0]) {
      return receiptFromExisting(existing[0], hash, input.idempotencyKey);
    }

    try {
      return await this.database.begin(async (transaction) => {
        const households = await transaction<{ next_signal_sequence: string }[]>`
          select next_signal_sequence
          from households
          where id = ${input.householdId}
          for update
        `;
        const household = households[0];
        if (!household) {
          throw new Error("Unknown household");
        }
        const sequence = Number(household.next_signal_sequence);
        await transaction`
          update households
          set next_signal_sequence = next_signal_sequence + 1, updated_at = now()
          where id = ${input.householdId}
        `;
        await transaction`
          insert into household_signals (
            id, household_id, sequence, idempotency_key, payload_hash, kind,
            actor_kind, actor_id, visibility, owner_adult_id, occurred_at, payload
          ) values (
            ${signalId}, ${input.householdId}, ${sequence}, ${input.idempotencyKey}, ${hash}, ${input.kind},
            ${input.actorKind}, ${input.actorId ?? null}, ${input.visibility},
            ${input.ownerAdultId ?? null}, ${input.occurredAt}, ${json(this.database, input.payload)}
          )
        `;
        await transaction`
          insert into jobs (id, household_id, signal_id, kind, status, payload)
          values (
            ${jobId}, ${input.householdId}, ${signalId}, ${input.jobKind}, 'pending',
            ${json(this.database, { signalId, sequence })}
          )
        `;
        return {
          signalId,
          householdId: input.householdId,
          sequence,
          disposition: "accepted" as const,
          jobId,
        };
      });
    } catch (error) {
      if (!isUniqueViolation(error)) {
        throw error;
      }
      const raced = await this.database<ExistingSignal[]>`
        select id, household_id, sequence, payload_hash
        from household_signals
        where idempotency_key = ${input.idempotencyKey}
      `;
      if (!raced[0]) {
        throw error;
      }
      return receiptFromExisting(raced[0], hash, input.idempotencyKey);
    }
  }

  async claimJobs(owner: string, limit: number, leaseSeconds: number): Promise<ClaimedJob[]> {
    const token = randomUUID();
    const rows = await this.database<JobRow[]>`
      with candidates as (
        select id
        from jobs
        where (
          (status in ('pending', 'retry') and available_at <= now())
          or (status = 'leased' and lease_expires_at < now())
        )
        order by available_at, created_at
        for update skip locked
        limit ${limit}
      )
      update jobs
      set status = 'leased',
          lease_owner = ${owner},
          lease_token = ${token},
          lease_expires_at = now() + (${leaseSeconds} * interval '1 second'),
          attempt = attempt + 1,
          updated_at = now()
      from candidates
      where jobs.id = candidates.id
      returning jobs.id, jobs.household_id, jobs.signal_id, jobs.kind, jobs.payload,
                jobs.attempt, jobs.max_attempts, jobs.lease_token, jobs.lease_expires_at
    `;
    return rows.map((row) => ({
      id: row.id,
      householdId: row.household_id,
      signalId: row.signal_id,
      kind: row.kind,
      payload: row.payload,
      attempt: row.attempt,
      maxAttempts: row.max_attempts,
      leaseToken: row.lease_token,
      leaseExpiresAt: row.lease_expires_at.toISOString(),
    }));
  }

  async completeJob(jobId: string, leaseToken: string): Promise<boolean> {
    const rows = await this.database<{ id: string }[]>`
      update jobs
      set status = 'succeeded', lease_owner = null, lease_token = null, lease_expires_at = null, updated_at = now()
      where id = ${jobId} and status = 'leased' and lease_token = ${leaseToken}
      returning id
    `;
    return rows.length === 1;
  }

  async failJob(input: {
    jobId: string;
    leaseToken: string;
    errorCode: string;
    safeDetail?: string;
    retryAfterSeconds: number;
  }): Promise<"retry" | "dead" | "lost_lease"> {
    const rows = await this.database<{ status: "retry" | "dead" }[]>`
      update jobs
      set status = case when attempt >= max_attempts then 'dead' else 'retry' end,
          available_at = now() + (${input.retryAfterSeconds} * interval '1 second'),
          lease_owner = null,
          lease_token = null,
          lease_expires_at = null,
          last_error_code = ${input.errorCode},
          last_error_detail = ${input.safeDetail ?? null},
          updated_at = now()
      where id = ${input.jobId} and status = 'leased' and lease_token = ${input.leaseToken}
      returning status
    `;
    return rows[0]?.status ?? "lost_lease";
  }

  async commitTransition(input: HouseholdTransition): Promise<number> {
    return this.database.begin(async (transaction) => {
      const households = await transaction<
        {
          version: string;
          next_audit_sequence: string;
        }[]
      >`
        select version, next_audit_sequence
        from households
        where id = ${input.householdId}
        for update
      `;
      const household = households[0];
      if (!household) {
        throw new Error("Unknown household");
      }
      const currentVersion = Number(household.version);
      if (currentVersion !== input.expectedHouseholdVersion) {
        throw new StaleHouseholdVersionError(input.expectedHouseholdVersion, currentVersion);
      }

      for (const episode of input.episodes ?? []) {
        if (episode.expectedVersion === undefined) {
          await transaction`
            insert into family_episodes (
              id, household_id, episode_type, visibility, owner_adult_id, status, title,
              accepted_meaning, source_refs, authority, temporal_plan, outcome, closed_at
            ) values (
              ${episode.id}, ${input.householdId}, ${episode.episodeType}, ${episode.visibility},
              ${episode.ownerAdultId ?? null}, ${episode.status}, ${episode.title},
              ${json(this.database, episode.acceptedMeaning)}, ${json(this.database, episode.sourceRefs)},
              ${json(this.database, episode.authority)},
              ${episode.temporalPlan ? json(this.database, episode.temporalPlan) : null},
              ${episode.outcome ? json(this.database, episode.outcome) : null},
              ${episode.status === "handled" || episode.status === "dismissed" ? new Date() : null}
            )
          `;
        } else {
          const updated = await transaction<{ id: string }[]>`
            update family_episodes
            set owner_adult_id = ${episode.ownerAdultId ?? null},
                status = ${episode.status},
                title = ${episode.title},
                accepted_meaning = ${json(this.database, episode.acceptedMeaning)},
                source_refs = ${json(this.database, episode.sourceRefs)},
                authority = ${json(this.database, episode.authority)},
                temporal_plan = ${episode.temporalPlan ? json(this.database, episode.temporalPlan) : null},
                outcome = ${episode.outcome ? json(this.database, episode.outcome) : null},
                version = version + 1,
                closed_at = case when ${episode.status} in ('handled', 'dismissed') then now() else null end,
                updated_at = now()
            where id = ${episode.id} and household_id = ${input.householdId}
              and version = ${episode.expectedVersion}
            returning id
          `;
          if (updated.length !== 1) {
            throw new Error(`Stale or missing episode: ${episode.id}`);
          }
        }
      }

      if ((input.cancelTriggerIds?.length ?? 0) > 0) {
        await transaction`
          update scheduled_triggers
          set status = 'superseded', updated_at = now()
          where household_id = ${input.householdId}
            and id = any(${input.cancelTriggerIds ?? []}::uuid[])
            and status in ('scheduled', 'claimed')
        `;
      }
      for (const trigger of input.triggers ?? []) {
        await transaction`
          insert into scheduled_triggers (
            id, household_id, episode_id, trigger_kind, plan_version, due_at, status, payload
          ) values (
            ${trigger.id}, ${input.householdId}, ${trigger.episodeId ?? null}, ${trigger.triggerKind},
            ${trigger.planVersion}, ${trigger.dueAt}, 'scheduled', ${json(this.database, trigger.payload)}
          )
        `;
      }
      for (const effect of input.outbox ?? []) {
        await transaction`
          insert into outbox (id, household_id, effect_kind, idempotency_key, payload, status)
          values (
            ${effect.id}, ${input.householdId}, ${effect.effectKind}, ${effect.idempotencyKey},
            ${json(this.database, effect.payload)}, 'pending'
          )
          on conflict (idempotency_key) do nothing
        `;
      }

      let auditSequence = Number(household.next_audit_sequence);
      for (const audit of input.audits) {
        await transaction`
          insert into audit_log (
            id, household_id, sequence, actor_kind, actor_id, action, target_type, target_id,
            source_refs, policy_refs, details
          ) values (
            ${audit.id}, ${input.householdId}, ${auditSequence}, ${audit.actorKind},
            ${audit.actorId ?? null}, ${audit.action}, ${audit.targetType}, ${audit.targetId ?? null},
            ${json(this.database, audit.sourceRefs ?? [])}, ${json(this.database, audit.policyRefs ?? [])},
            ${json(this.database, audit.details)}
          )
        `;
        auditSequence += 1;
      }

      const nextVersion = currentVersion + 1;
      await transaction`
        update households
        set version = ${nextVersion}, next_audit_sequence = ${auditSequence}, updated_at = now()
        where id = ${input.householdId}
      `;
      await transaction`
        update household_signals
        set processing_status = 'processed'
        where id = ${input.signalId} and household_id = ${input.householdId}
      `;
      return nextVersion;
    });
  }

  async householdVersion(householdId: string): Promise<number> {
    const rows = await this.database<{ version: string }[]>`
      select version from households where id = ${householdId}
    `;
    if (!rows[0]) {
      throw new Error("Unknown household");
    }
    return Number(rows[0].version);
  }
}
