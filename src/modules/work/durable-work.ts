import { createHash, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { Database } from "../../db/client.js";
import { canonicalJson } from "../../shared/canonical-json.js";
import type { SecretBox } from "../../shared/crypto.js";

export interface AuthorityFence {
  readonly person?: { id: string; controlEpoch: number };
  readonly household?: { id: string; controlEpoch: number };
  readonly conversation?: { id: string; authorityVersion: number };
}

export interface EnqueueJobInput extends AuthorityFence {
  readonly kind: string;
  readonly idempotencyKey: string;
  readonly payload: unknown;
  readonly taskId?: string;
  readonly availableAt?: Date;
  readonly deadlineAt?: Date;
  readonly maxAttempts?: number;
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
}

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

/** Postgres-backed at-least-once work queue with explicit authority fences. */
export class DurableWork {
  public constructor(
    private readonly database: Executor,
    private readonly secretBox: SecretBox,
  ) {}

  public async enqueue(input: EnqueueJobInput): Promise<{ jobId: string; created: boolean }> {
    validateFence(input);
    const payloadJson = canonicalJson(input.payload);
    const digest = sha256Hex(payloadJson);
    const encrypted = this.secretBox.encrypt(payloadJson, "durable-job-payload");
    const rows = await this.database<{ id: string; inserted: boolean }[]>`
      insert into jobs (
        id, job_kind, household_id, person_id, conversation_id, task_id,
        idempotency_key, payload_digest, payload_ciphertext, payload_key_version,
        status, max_attempts, available_at, deadline_at,
        person_control_epoch, household_control_epoch, conversation_authority_version
      ) values (
        ${randomUUID()}, ${input.kind}, ${input.household?.id ?? null}, ${input.person?.id ?? null},
        ${input.conversation?.id ?? null}, ${input.taskId ?? null}, ${input.idempotencyKey},
        ${digest}, ${Buffer.from(JSON.stringify(encrypted), "utf8")}, ${encrypted.kid},
        'pending', ${input.maxAttempts ?? 8}, ${input.availableAt ?? new Date()},
        ${input.deadlineAt ?? null}, ${input.person?.controlEpoch ?? null},
        ${input.household?.controlEpoch ?? null}, ${input.conversation?.authorityVersion ?? null}
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
        where job.status in ('pending', 'retry', 'leased')
          and job.available_at <= ${now}
          and (job.deadline_at is null or job.deadline_at > ${now})
          and (job.status <> 'leased' or job.lease_expires_at <= ${now})
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
        order by job.available_at, job.created_at
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
            household_id, household_control_epoch, conversation_id, conversation_authority_version
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
        });
      }
      return claimed;
    });
  }

  public async succeed(job: Pick<ClaimedJob, "id" | "leaseToken">, now = new Date()): Promise<boolean> {
    const rows = await this.database<{ id: string }[]>`
      update jobs set status = 'succeeded', lease_owner = null, lease_token = null,
        lease_expires_at = null, updated_at = ${now}
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

  public async cancelStale(now = new Date()): Promise<number> {
    const rows = await this.database<{ id: string }[]>`
      update jobs job set status = 'cancelled', lease_owner = null, lease_token = null,
        lease_expires_at = null, last_error_code = 'authority_fence_changed', updated_at = ${now}
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
      ) returning id
    `;
    return rows.length;
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
  for (const entry of [input.person, input.household, input.conversation]) {
    if (
      entry &&
      (!Number.isSafeInteger("controlEpoch" in entry ? entry.controlEpoch : entry.authorityVersion) ||
        ("controlEpoch" in entry ? entry.controlEpoch : entry.authorityVersion) < 1)
    ) {
      throw new Error("Authority fence versions must be positive integers");
    }
  }
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
  };
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
