import { createHash, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { Database } from "../../db/client.js";
import { canonicalJson } from "../../shared/canonical-json.js";
import type { SecretBox } from "../../shared/crypto.js";

export interface AuthorityFence {
  readonly person?: { id: string; controlEpoch: number };
  readonly household?: { id: string; controlEpoch: number };
  readonly conversation?: { id: string; authorityVersion: number };
  readonly integration?: { id: string; controlEpoch: number };
}

export interface EnqueueJobInput extends AuthorityFence {
  readonly kind: string;
  readonly idempotencyKey: string;
  readonly payload: unknown;
  readonly taskId?: string;
  readonly availableAt?: Date;
  readonly deadlineAt?: Date;
  readonly maxAttempts?: number;
  readonly priority?: number;
  /** Opaque application case identity used only to supersede recovered bounded work. */
  readonly caseKeyDigest?: string;
}

export interface ClaimedJob<Payload = unknown> {
  readonly id: string;
  readonly kind: string;
  readonly payload: Payload;
  readonly attemptCount: number;
  readonly maxAttempts: number;
  readonly leaseToken: string;
  readonly deadlineAt: Date | null;
  readonly fence: AuthorityFence;
  readonly caseKeyDigest?: string | null;
}

export interface DeadJobRedriveInput {
  readonly kind: string;
  readonly idempotencyNamespace: string;
  readonly now?: Date;
  readonly limit?: number;
  readonly lookbackMs: number;
  readonly bucketMs: number;
  readonly maxGenerations: number;
  readonly requireIntegrationFence?: boolean;
}

interface JobRow {
  id: string;
  job_kind: string;
  payload_ciphertext: Buffer;
  payload_key_version: string;
  attempt_count: number;
  max_attempts: number;
  deadline_at: Date | null;
  lease_token: string;
  person_id: string | null;
  person_control_epoch: number | string | null;
  household_id: string | null;
  household_control_epoch: number | string | null;
  conversation_id: string | null;
  conversation_authority_version: number | string | null;
  integration_id: string | null;
  integration_control_epoch: number | string | null;
  case_key_digest: string | null;
}

interface DeadJobRow {
  id: string;
  job_kind: string;
  household_id: string | null;
  person_id: string | null;
  conversation_id: string | null;
  integration_id: string | null;
  task_id: string | null;
  idempotency_key: string;
  payload_digest: string;
  payload_ciphertext: Buffer;
  payload_key_version: string;
  max_attempts: number;
  priority: number;
  deadline_at: Date | null;
  person_control_epoch: number | string | null;
  household_control_epoch: number | string | null;
  conversation_authority_version: number | string | null;
  integration_control_epoch: number | string | null;
  case_key_digest: string | null;
}

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

const DEFAULT_PRIORITY = 100;
const MAX_PRIORITY = 2_147_483_647;

/** Postgres-backed at-least-once work queue with explicit authority fences. */
export class DurableWork {
  public constructor(
    private readonly database: Executor,
    private readonly secretBox: SecretBox,
  ) {}

  public async enqueue(input: EnqueueJobInput): Promise<{ jobId: string; created: boolean }> {
    validateFence(input);
    validatePriority(input.priority);
    validateCaseKeyDigest(input.caseKeyDigest);
    const payloadJson = canonicalJson(input.payload);
    const digest = sha256Hex(payloadJson);
    const encrypted = this.secretBox.encrypt(payloadJson, "durable-job-payload");
    const rows = await this.database<{ id: string; inserted: boolean }[]>`
      insert into jobs (
        id, job_kind, household_id, person_id, conversation_id, integration_id, task_id,
        idempotency_key, payload_digest, payload_ciphertext, payload_key_version,
        status, max_attempts, priority, available_at, deadline_at,
        person_control_epoch, household_control_epoch, conversation_authority_version,
        integration_control_epoch, case_key_digest
      ) values (
        ${randomUUID()}, ${input.kind}, ${input.household?.id ?? null}, ${input.person?.id ?? null},
        ${input.conversation?.id ?? null}, ${input.integration?.id ?? null},
        ${input.taskId ?? null}, ${input.idempotencyKey}, ${digest},
        ${Buffer.from(JSON.stringify(encrypted), "utf8")}, ${encrypted.kid},
        'pending', ${input.maxAttempts ?? 8}, ${input.priority ?? DEFAULT_PRIORITY},
        ${input.availableAt ?? new Date()},
        ${input.deadlineAt ?? null}, ${input.person?.controlEpoch ?? null},
        ${input.household?.controlEpoch ?? null}, ${input.conversation?.authorityVersion ?? null},
        ${input.integration?.controlEpoch ?? null}, ${input.caseKeyDigest ?? null}
      )
      on conflict (idempotency_key) do update set idempotency_key = jobs.idempotency_key
      returning id, (xmax = 0) as inserted
    `;
    const row = rows[0];
    if (!row) throw new Error("Job enqueue returned no row");
    return { jobId: row.id, created: row.inserted };
  }

  public async claim(workerId: string, limit = 10, now = new Date()): Promise<ClaimedJob[]> {
    const leaseUntil = new Date(now.getTime() + 5 * 60_000);
    return inTransaction(this.database, async (transaction) => {
      const candidateRows = await transaction<{ id: string }[]>`
        select job.id
        from jobs job
        left join people person on person.id = job.person_id
        left join households household on household.id = job.household_id
        left join conversations conversation on conversation.id = job.conversation_id
        left join integrations integration on integration.id = job.integration_id
        where job.status in ('pending', 'retry', 'leased')
          and job.available_at <= ${now}
          and (job.deadline_at is null or job.deadline_at > ${now})
          and (job.status <> 'leased' or job.lease_expires_at <= ${now})
          and (job.job_kind not like 'google.%' or job.integration_id is not null)
          and (job.person_id is null or (
            person.status = 'registered' and person.control_epoch = job.person_control_epoch
          ))
          and (job.household_id is null or (
            household.status in ('onboarding', 'active', 'paused')
            and household.control_epoch = job.household_control_epoch
          ))
          and (job.conversation_id is null or (
            conversation.status not in ('deletion_fenced', 'deleted')
            and conversation.authority_version = job.conversation_authority_version
          ))
          and (job.integration_id is null or (
            integration.status = 'active'
            and integration.control_epoch = job.integration_control_epoch
          ))
        order by job.priority, job.available_at, job.created_at
        for update of job skip locked
        limit ${Math.max(1, Math.min(limit, 100))}
      `;
      const claimed: ClaimedJob[] = [];
      for (const candidate of candidateRows) {
        const leaseToken = randomUUID();
        const rows = await transaction<JobRow[]>`
          update jobs set status = 'leased', lease_owner = ${workerId}, lease_token = ${leaseToken},
            lease_expires_at = ${leaseUntil}, attempt_count = attempt_count + 1, updated_at = ${now}
          where id = ${candidate.id}
          returning id, job_kind, payload_ciphertext, payload_key_version, attempt_count,
            max_attempts, deadline_at, lease_token, person_id, person_control_epoch,
            household_id, household_control_epoch, conversation_id, conversation_authority_version,
            integration_id, integration_control_epoch, case_key_digest
        `;
        const row = rows[0];
        if (!row) continue;
        const payload = JSON.parse(
          this.secretBox
            .decrypt(JSON.parse(row.payload_ciphertext.toString("utf8")), "durable-job-payload")
            .toString("utf8"),
        ) as unknown;
        claimed.push({
          id: row.id,
          kind: row.job_kind,
          payload,
          attemptCount: row.attempt_count,
          maxAttempts: row.max_attempts,
          leaseToken: row.lease_token,
          deadlineAt: row.deadline_at,
          fence: fenceFromRow(row),
          caseKeyDigest: row.case_key_digest,
        });
      }
      return claimed;
    });
  }

  public async succeed(
    job: Pick<ClaimedJob, "id" | "leaseToken" | "fence" | "caseKeyDigest">,
    now = new Date(),
  ): Promise<boolean> {
    return inTransaction(this.database, async (transaction) => {
      const rows = await transaction<{ id: string; created_at: Date }[]>`
        update jobs set status = 'succeeded', lease_owner = null, lease_token = null,
          lease_expires_at = null, updated_at = ${now}
        where id = ${job.id} and status = 'leased' and lease_token = ${job.leaseToken}
        returning id, created_at
      `;
      const succeeded = rows[0];
      if (!succeeded) return false;
      if (job.caseKeyDigest && job.fence.integration) {
        await transaction`
          update jobs prior set status = 'cancelled', last_error_code = 'case_frontier_recovered',
            updated_at = ${now}
          where prior.id <> ${job.id} and prior.job_kind = 'orchestrate.private_source'
            and prior.status in ('attention', 'dead')
            and prior.integration_id = ${job.fence.integration.id}
            and prior.integration_control_epoch = ${job.fence.integration.controlEpoch}
            and prior.case_key_digest = ${job.caseKeyDigest}
            and prior.created_at < ${succeeded.created_at}
        `;
      }
      return true;
    });
  }

  /** Settles bounded work that cannot be completed without a new source or user action. */
  public async needsAttention(
    job: Pick<ClaimedJob, "id" | "leaseToken">,
    reasonCode: string,
    now = new Date(),
  ): Promise<boolean> {
    const rows = await this.database<{ id: string }[]>`
      update jobs set status = 'attention', available_at = ${now},
        lease_owner = null, lease_token = null, lease_expires_at = null,
        last_error_code = ${reasonCode.slice(0, 200)}, updated_at = ${now}
      where id = ${job.id} and status = 'leased' and lease_token = ${job.leaseToken}
      returning id
    `;
    return rows.length === 1;
  }

  public async fail(
    job: Pick<ClaimedJob, "id" | "leaseToken" | "attemptCount" | "maxAttempts">,
    errorCode: string,
    options: { retryable: boolean; now?: Date } = { retryable: true },
  ): Promise<"retry" | "dead" | "stale"> {
    const now = options.now ?? new Date();
    const retry = options.retryable && job.attemptCount < job.maxAttempts;
    const delay = Math.min(15 * 60_000, 1_000 * 2 ** Math.max(0, job.attemptCount - 1));
    const rows = await this.database<{ status: "retry" | "dead" }[]>`
      update jobs set status = ${retry ? "retry" : "dead"},
        available_at = ${retry ? new Date(now.getTime() + delay) : now},
        lease_owner = null, lease_token = null, lease_expires_at = null,
        last_error_code = ${errorCode.slice(0, 200)}, updated_at = ${now}
      where id = ${job.id} and status = 'leased' and lease_token = ${job.leaseToken}
      returning status
    `;
    return rows[0]?.status ?? "stale";
  }

  /** Reissues a bounded, linear generation of dead work while its authority fences are current. */
  public async redriveDeadCurrentAuthority(input: DeadJobRedriveInput): Promise<number> {
    validateRedriveInput(input);
    const now = input.now ?? new Date();
    const limit = input.limit ?? 20;
    const bucket = Math.floor(now.getTime() / input.bucketMs);
    const namespace = input.idempotencyNamespace;
    const currentBucketPattern = `${namespace}:%:b${bucket}`;
    const exhaustedGenerationPattern = `${namespace}:g${input.maxGenerations}:%`;
    const lookbackStart = new Date(now.getTime() - input.lookbackMs);
    return inTransaction(this.database, async (transaction) => {
      const candidates = await transaction<DeadJobRow[]>`
        select job.id, job.job_kind, job.household_id, job.person_id, job.conversation_id,
          job.integration_id, job.task_id, job.idempotency_key, job.payload_digest,
          job.payload_ciphertext, job.payload_key_version, job.max_attempts, job.priority,
          job.deadline_at, job.person_control_epoch, job.household_control_epoch,
          job.conversation_authority_version, job.integration_control_epoch, job.case_key_digest
        from jobs job
        left join people person on person.id = job.person_id
        left join households household on household.id = job.household_id
        left join conversations conversation on conversation.id = job.conversation_id
        left join integrations integration on integration.id = job.integration_id
        where job.job_kind = ${input.kind}
          and job.status = 'dead'
          and (${input.requireIntegrationFence !== true} or job.integration_id is not null)
          and job.updated_at >= ${lookbackStart}
          and (job.deadline_at is null or job.deadline_at > ${now})
          and job.idempotency_key not like ${currentBucketPattern}
          and job.idempotency_key not like ${exhaustedGenerationPattern}
          and (job.person_id is null or (
            person.status = 'registered' and person.control_epoch = job.person_control_epoch
          ))
          and (job.household_id is null or (
            household.status in ('onboarding', 'active', 'paused')
            and household.control_epoch = job.household_control_epoch
          ))
          and (job.conversation_id is null or (
            conversation.status not in ('deletion_fenced', 'deleted')
            and conversation.authority_version = job.conversation_authority_version
          ))
          and (job.integration_id is null or (
            integration.status = 'active'
            and integration.control_epoch = job.integration_control_epoch
          ))
        order by job.updated_at, job.id
        for update of job skip locked
        limit ${limit}
      `;
      let redriven = 0;
      for (const candidate of candidates) {
        const identity = nextRedriveIdentity(candidate, namespace, bucket, input.maxGenerations);
        if (!identity) continue;
        const inserted = await transaction<{ readonly id: string }[]>`
          insert into jobs (
            id, job_kind, household_id, person_id, conversation_id, integration_id, task_id,
            idempotency_key, payload_digest, payload_ciphertext, payload_key_version,
            status, max_attempts, priority, available_at, deadline_at,
            person_control_epoch, household_control_epoch, conversation_authority_version,
            integration_control_epoch, case_key_digest
          ) values (
            ${randomUUID()}, ${candidate.job_kind}, ${candidate.household_id}, ${candidate.person_id},
            ${candidate.conversation_id}, ${candidate.integration_id}, ${candidate.task_id},
            ${identity.idempotencyKey}, ${candidate.payload_digest}, ${candidate.payload_ciphertext},
            ${candidate.payload_key_version}, 'pending', ${candidate.max_attempts},
            ${candidate.priority}, ${now}, ${candidate.deadline_at},
            ${candidate.person_control_epoch}, ${candidate.household_control_epoch},
            ${candidate.conversation_authority_version}, ${candidate.integration_control_epoch},
            ${candidate.case_key_digest}
          )
          on conflict (idempotency_key) do nothing
          returning id
        `;
        if (!inserted[0]) continue;
        await transaction`
          update jobs
          set status = 'cancelled', last_error_code = 'recovery_scheduled', updated_at = ${now}
          where id = ${candidate.id} and status = 'dead'
        `;
        redriven += 1;
      }
      return redriven;
    });
  }

  public async cancelStale(now = new Date()): Promise<number> {
    return inTransaction(this.database, async (transaction) => {
      const attentionRows = await transaction<{ id: string }[]>`
        update jobs job set status = 'attention', available_at = ${now},
          lease_owner = null, lease_token = null, lease_expires_at = null,
          last_error_code = 'conversation_response_window_expired', updated_at = ${now}
        where job.status in ('pending', 'retry', 'leased')
          and job.job_kind in ('orchestrate.linq_message', 'orchestrate.linq_observation')
          and job.deadline_at is not null and job.deadline_at <= ${now}
          and job.person_id is not null and job.conversation_id is not null
          and exists (
            select 1 from people person where person.id = job.person_id
              and person.status = 'registered' and person.control_epoch = job.person_control_epoch
          )
          and exists (
            select 1 from conversations conversation where conversation.id = job.conversation_id
              and conversation.status not in ('deletion_fenced', 'deleted')
              and conversation.authority_version = job.conversation_authority_version
          )
          and (job.household_id is null or exists (
            select 1 from households household where household.id = job.household_id
              and household.status in ('onboarding', 'active', 'paused')
              and household.control_epoch = job.household_control_epoch
          ))
          and (job.integration_id is null or exists (
            select 1 from integrations integration where integration.id = job.integration_id
              and integration.status = 'active'
              and integration.control_epoch = job.integration_control_epoch
          ))
        returning id
      `;
      const cancelledRows = await transaction<{ id: string }[]>`
        update jobs job set status = 'cancelled', lease_owner = null, lease_token = null,
          lease_expires_at = null,
          last_error_code = case
            when job.deadline_at is not null and job.deadline_at <= ${now}
              then 'deadline_expired'
            else 'authority_fence_changed'
          end,
          updated_at = ${now}
        where job.status in ('pending', 'retry', 'leased') and (
          (job.deadline_at is not null and job.deadline_at <= ${now})
          or (job.person_id is not null and not exists (
            select 1 from people person where person.id = job.person_id
              and person.status = 'registered' and person.control_epoch = job.person_control_epoch
          ))
          or (job.household_id is not null and not exists (
            select 1 from households household where household.id = job.household_id
              and household.status in ('onboarding', 'active', 'paused')
              and household.control_epoch = job.household_control_epoch
          ))
          or (job.conversation_id is not null and not exists (
            select 1 from conversations conversation where conversation.id = job.conversation_id
              and conversation.status not in ('deletion_fenced', 'deleted')
              and conversation.authority_version = job.conversation_authority_version
          ))
          or (job.integration_id is not null and not exists (
            select 1 from integrations integration where integration.id = job.integration_id
              and integration.status = 'active'
              and integration.control_epoch = job.integration_control_epoch
          ))
          or (job.job_kind like 'google.%' and job.integration_id is null)
        ) returning id
      `;
      return attentionRows.length + cancelledRows.length;
    });
  }
}

function inTransaction<Result>(
  executor: Executor,
  operation: (transaction: Transaction) => Promise<Result>,
): Promise<Result> {
  return "begin" in executor
    ? (executor.begin(operation) as unknown as Promise<Result>)
    : operation(executor);
}

function validateFence(input: AuthorityFence): void {
  for (const entry of [input.person, input.household, input.conversation, input.integration]) {
    if (
      entry &&
      (!Number.isSafeInteger("controlEpoch" in entry ? entry.controlEpoch : entry.authorityVersion) ||
        ("controlEpoch" in entry ? entry.controlEpoch : entry.authorityVersion) < 1)
    ) {
      throw new Error("Authority fence versions must be positive integers");
    }
  }
}

function validatePriority(priority: number | undefined): void {
  if (
    priority !== undefined &&
    (!Number.isSafeInteger(priority) || priority < 0 || priority > MAX_PRIORITY)
  ) {
    throw new Error(`Job priority must be an integer between 0 and ${MAX_PRIORITY}`);
  }
}

function validateCaseKeyDigest(value: string | undefined): void {
  if (value !== undefined && !/^[a-f0-9]{64}$/u.test(value)) {
    throw new Error("Job case key digest is invalid");
  }
}

function validateRedriveInput(input: DeadJobRedriveInput): void {
  if (!/^[a-z0-9][a-z0-9._:-]{0,119}$/u.test(input.kind)) {
    throw new Error("Redrive job kind is invalid");
  }
  if (!/^[a-z0-9][a-z0-9.:-]{0,119}$/u.test(input.idempotencyNamespace)) {
    throw new Error("Redrive idempotency namespace is invalid");
  }
  for (const [name, value] of [
    ["lookbackMs", input.lookbackMs],
    ["bucketMs", input.bucketMs],
    ["maxGenerations", input.maxGenerations],
    ["limit", input.limit ?? 20],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 1) throw new Error(`Redrive ${name} must be positive`);
  }
  if ((input.limit ?? 20) > 100) throw new Error("Redrive limit cannot exceed 100");
}

function nextRedriveIdentity(
  candidate: Pick<DeadJobRow, "id" | "idempotency_key">,
  namespace: string,
  bucket: number,
  maxGenerations: number,
): { readonly idempotencyKey: string } | null {
  const escapedNamespace = namespace.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  const match = new RegExp(`^${escapedNamespace}:g([1-9][0-9]*):([0-9a-f-]{36}):b[0-9]+$`, "u").exec(
    candidate.idempotency_key,
  );
  const generation = match ? Number(match[1]) : 0;
  const rootId = match?.[2] ?? candidate.id;
  if (!Number.isSafeInteger(generation) || generation >= maxGenerations) return null;
  return {
    idempotencyKey: `${namespace}:g${generation + 1}:${rootId}:b${bucket}`,
  };
}

function fenceFromRow(row: JobRow): AuthorityFence {
  return {
    ...(row.person_id && row.person_control_epoch
      ? { person: { id: row.person_id, controlEpoch: Number(row.person_control_epoch) } }
      : {}),
    ...(row.household_id && row.household_control_epoch
      ? { household: { id: row.household_id, controlEpoch: Number(row.household_control_epoch) } }
      : {}),
    ...(row.conversation_id && row.conversation_authority_version
      ? {
          conversation: {
            id: row.conversation_id,
            authorityVersion: Number(row.conversation_authority_version),
          },
        }
      : {}),
    ...(row.integration_id && row.integration_control_epoch
      ? {
          integration: {
            id: row.integration_id,
            controlEpoch: Number(row.integration_control_epoch),
          },
        }
      : {}),
  };
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
