import { createHash, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { Database } from "../../db/client.js";
import { canonicalJson } from "../../shared/canonical-json.js";
import { StaleAuthorityError } from "../../shared/errors.js";
import { type CoverageTimer, CoverageTimerSchema } from "../coordination/contracts.js";
import type { AuthorityFence } from "./durable-work.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

export interface ScheduleTimerInput extends AuthorityFence {
  readonly kind: string;
  readonly coverageLoopId?: string;
  readonly definition: unknown;
  readonly dueAt: Date;
  readonly expectedDomainVersion: number;
}

export interface DueTimer {
  readonly id: string;
  readonly kind: string;
  readonly coverageLoopId: string | null;
  readonly expectedDomainVersion: number;
  readonly dueAt: string;
  readonly fence: AuthorityFence;
}

export const TimerProcessPayloadSchema = z.strictObject({
  id: z.string().uuid(),
  kind: z.string().trim().min(1).max(100),
  coverageLoopId: z.string().uuid().nullable(),
  expectedDomainVersion: z.number().int().positive(),
  dueAt: z.iso.datetime({ offset: true }),
});
export type TimerProcessPayload = z.infer<typeof TimerProcessPayloadSchema>;

export type DurableTimerStatus = "scheduled" | "claimed" | "fired" | "cancelled" | "superseded" | "dead";

export interface ClaimedDurableTimer extends DueTimer {
  readonly status: "claimed";
  readonly coverageTimer: CoverageTimer | null;
}

export type ScheduleCoverageTimerInput = {
  readonly timer: CoverageTimer;
  readonly household: { readonly id: string; readonly controlEpoch: number };
  readonly conversation: { readonly id: string; readonly authorityVersion: number };
};

interface TimerRow {
  readonly id: string;
  readonly timer_kind: string;
  readonly household_id: string | null;
  readonly person_id: string | null;
  readonly conversation_id: string | null;
  readonly coverage_loop_id: string | null;
  readonly loop_version: number | string | null;
  readonly plan_version: number | string | null;
  readonly attention_cycle: number | null;
  readonly participant_epoch_id: string | null;
  readonly participant_set_digest: string | null;
  readonly notification_category: string | null;
  readonly due_at: Date;
  readonly status: DurableTimerStatus;
  readonly person_control_epoch: number | string | null;
  readonly household_control_epoch: number | string | null;
  readonly conversation_authority_version: number | string | null;
  readonly expected_domain_version: number | string;
}

export class DurableTimers {
  public constructor(private readonly database: Executor) {}

  public async schedule(input: ScheduleTimerInput): Promise<string> {
    const id = randomUUID();
    await this.database`
      insert into timers (
        id, timer_kind, household_id, person_id, conversation_id, coverage_loop_id,
        definition_digest, due_at, status, person_control_epoch, household_control_epoch,
        conversation_authority_version, expected_domain_version
      ) values (
        ${id}, ${input.kind}, ${input.household?.id ?? null}, ${input.person?.id ?? null},
        ${input.conversation?.id ?? null}, ${input.coverageLoopId ?? null},
        ${createHash("sha256").update(canonicalJson(input.definition)).digest("hex")},
        ${input.dueAt}, 'scheduled', ${input.person?.controlEpoch ?? null},
        ${input.household?.controlEpoch ?? null}, ${input.conversation?.authorityVersion ?? null},
        ${input.expectedDomainVersion}
      )
    `;
    return id;
  }

  /** Schedules an exact, reconstructable coverage timer against current loop and authority versions. */
  public async scheduleCoverage(inputCandidate: ScheduleCoverageTimerInput): Promise<string> {
    const timer = CoverageTimerSchema.parse(inputCandidate.timer);
    const household = positiveFence(inputCandidate.household, "controlEpoch");
    const conversation = positiveFence(inputCandidate.conversation, "authorityVersion");
    const definitionDigest = createHash("sha256").update(canonicalJson(timer)).digest("hex");
    const rows = await this.database<{ readonly id: string }[]>`
      insert into timers (
        id, timer_kind, household_id, conversation_id, coverage_loop_id,
        loop_version, plan_version, attention_cycle, participant_epoch_id,
        participant_set_digest, notification_category, definition_digest,
        due_at, status, household_control_epoch, conversation_authority_version,
        expected_domain_version
      )
      select
        ${timer.timerId}, 'coverage.notification', loop.household_id,
        loop.destination_conversation_id, loop.id, ${timer.loopVersion},
        ${timer.planVersion}, ${timer.attentionCycle}, ${timer.participantEpochId},
        ${timer.participantSetDigest}, ${timer.category}, ${definitionDigest},
        ${new Date(timer.dueAt)}, 'scheduled', ${household.controlEpoch},
        ${conversation.authorityVersion}, ${timer.loopVersion}
      from coverage_loops loop
      join households household on household.id = loop.household_id
      join conversations conversation on conversation.id = loop.destination_conversation_id
      where loop.id = ${timer.loopId}
        and loop.household_id = ${household.id}
        and household.control_epoch = ${household.controlEpoch}
        and loop.destination_conversation_id = ${conversation.id}
        and conversation.authority_version = ${conversation.authorityVersion}
        and loop.version = ${timer.loopVersion}
        and loop.plan_version = ${timer.planVersion}
        and loop.attention_cycle = ${timer.attentionCycle}
        and loop.participant_epoch_id = ${timer.participantEpochId}
        and loop.participant_set_digest = ${timer.participantSetDigest}
      on conflict (id) do update set
        status = case when timers.status = 'cancelled' then 'scheduled' else timers.status end,
        due_at = case when timers.status = 'cancelled' then excluded.due_at else timers.due_at end,
        household_control_epoch = case
          when timers.status = 'cancelled' then excluded.household_control_epoch
          else timers.household_control_epoch
        end,
        conversation_authority_version = case
          when timers.status = 'cancelled' then excluded.conversation_authority_version
          else timers.conversation_authority_version
        end,
        updated_at = case when timers.status = 'cancelled' then now() else timers.updated_at end
      where timers.definition_digest = excluded.definition_digest
        and timers.household_id = excluded.household_id
        and timers.conversation_id = excluded.conversation_id
        and timers.status in ('scheduled', 'claimed', 'cancelled')
        and (
          timers.status = 'cancelled'
          or (
            timers.household_control_epoch = excluded.household_control_epoch
            and timers.conversation_authority_version = excluded.conversation_authority_version
          )
        )
      returning id
    `;
    if (!rows[0]) throw new StaleAuthorityError("Coverage timer authority changed before scheduling");
    return rows[0].id;
  }

  /** Cancels durable checks whose deletion/authority/domain fence can no longer succeed. */
  public async cancelStale(now = new Date()): Promise<number> {
    const rows = await this.database<{ readonly id: string }[]>`
      update timers timer
      set status = 'cancelled', due_at = null, updated_at = ${now}
      where timer.status in ('scheduled', 'claimed')
        and (
          (timer.person_id is not null and not exists (
            select 1 from people person
            where person.id = timer.person_id
              and person.control_epoch = timer.person_control_epoch
              and person.status = 'registered'
          ))
          or (timer.household_id is not null and not exists (
            select 1 from households household
            where household.id = timer.household_id
              and household.control_epoch = timer.household_control_epoch
              and household.status not in ('deletion_fenced', 'deleted')
          ))
          or (timer.conversation_id is not null and not exists (
            select 1 from conversations conversation
            where conversation.id = timer.conversation_id
              and conversation.authority_version = timer.conversation_authority_version
              and conversation.status not in ('deletion_fenced', 'deleted')
          ))
          or (timer.coverage_loop_id is not null and not exists (
            select 1 from coverage_loops loop
            where loop.id = timer.coverage_loop_id
              and loop.version = timer.expected_domain_version
              and loop.version = timer.loop_version
              and loop.plan_version = timer.plan_version
              and loop.attention_cycle = timer.attention_cycle
              and loop.participant_epoch_id = timer.participant_epoch_id
              and loop.participant_set_digest = timer.participant_set_digest
          ))
        )
      returning timer.id
    `;
    return rows.length;
  }

  /** Re-arms exact, still-current coverage timers whose processing job exhausted or disappeared. */
  public async recoverOrphanedClaims(now = new Date()): Promise<number> {
    const rows = await this.database<{ readonly id: string }[]>`
      update timers timer
      set status = 'scheduled',
        due_at = greatest(
          ${now},
          least(${new Date(now.getTime() + 5 * 60_000)}, loop.last_responsible_at)
        ),
        updated_at = ${now}
      from coverage_loops loop, households household, conversations conversation
      where timer.status = 'claimed'
        and timer.coverage_loop_id = loop.id
        and timer.household_id = household.id
        and timer.conversation_id = conversation.id
        and household.control_epoch = timer.household_control_epoch
        and household.status not in ('deletion_fenced', 'deleted')
        and conversation.authority_version = timer.conversation_authority_version
        and conversation.status not in ('deletion_fenced', 'deleted')
        and loop.version = timer.expected_domain_version
        and loop.version = timer.loop_version
        and loop.plan_version = timer.plan_version
        and loop.attention_cycle = timer.attention_cycle
        and loop.participant_epoch_id = timer.participant_epoch_id
        and loop.participant_set_digest = timer.participant_set_digest
        and loop.state in ('provisional', 'open', 'awaiting_response', 'at_risk')
        and not exists (
          select 1 from jobs job
          where job.idempotency_key like ('timer:' || timer.id::text || ':%')
            and job.status in ('pending', 'retry', 'leased')
        )
      returning timer.id
    `;
    return rows.length;
  }

  public async claimDue(limit = 50, now = new Date()): Promise<DueTimer[]> {
    return inTransaction(this.database, async (transaction) => {
      const rows = await transaction<TimerRow[]>`
        select timer.id, timer.timer_kind, timer.household_id, timer.person_id,
          timer.conversation_id, timer.coverage_loop_id, timer.loop_version,
          timer.plan_version, timer.attention_cycle, timer.participant_epoch_id,
          timer.participant_set_digest, timer.notification_category, timer.due_at,
          timer.status, timer.person_control_epoch, timer.household_control_epoch,
          timer.conversation_authority_version, timer.expected_domain_version
        from timers timer
        left join people person on person.id = timer.person_id
        left join households household on household.id = timer.household_id
        left join conversations conversation on conversation.id = timer.conversation_id
        left join coverage_loops loop on loop.id = timer.coverage_loop_id
        where timer.status = 'scheduled' and timer.due_at <= ${now}
          and (timer.person_id is null or person.control_epoch = timer.person_control_epoch)
          and (timer.household_id is null or household.control_epoch = timer.household_control_epoch)
          and (timer.conversation_id is null or conversation.authority_version = timer.conversation_authority_version)
          and (timer.coverage_loop_id is null or (
            loop.version = timer.expected_domain_version
            and timer.loop_version = loop.version
            and timer.plan_version = loop.plan_version
            and timer.attention_cycle = loop.attention_cycle
            and timer.participant_epoch_id = loop.participant_epoch_id
            and timer.participant_set_digest = loop.participant_set_digest
          ))
        order by timer.due_at for update of timer skip locked
        limit ${Math.max(1, Math.min(limit, 100))}
      `;
      if (rows.length > 0) {
        await transaction`
          update timers set status = 'claimed', updated_at = ${now}
          where id in ${transaction(rows.map((row) => row.id))}
        `;
      }
      return rows.map((row) => ({
        id: row.id,
        kind: row.timer_kind,
        coverageLoopId: row.coverage_loop_id,
        expectedDomainVersion: Number(row.expected_domain_version),
        dueAt: row.due_at.toISOString(),
        fence: fenceFromTimerRow(row),
      }));
    });
  }

  /** Locks a claimed timer for idempotent processing inside the caller's transaction. */
  public async loadClaimed(timerIdCandidate: string): Promise<ClaimedDurableTimer | null> {
    const timerId = z.string().uuid().parse(timerIdCandidate);
    const rows = await this.database<TimerRow[]>`
      select id, timer_kind, household_id, person_id, conversation_id,
        coverage_loop_id, loop_version, plan_version, attention_cycle,
        participant_epoch_id, participant_set_digest, notification_category,
        due_at, status, person_control_epoch, household_control_epoch,
        conversation_authority_version, expected_domain_version
      from timers where id = ${timerId} and status = 'claimed'
      for update
    `;
    const row = rows[0];
    if (!row) return null;
    return {
      id: row.id,
      kind: row.timer_kind,
      coverageLoopId: row.coverage_loop_id,
      expectedDomainVersion: Number(row.expected_domain_version),
      dueAt: row.due_at.toISOString(),
      status: "claimed",
      fence: fenceFromTimerRow(row),
      coverageTimer: coverageTimerFromRow(row),
    };
  }

  public async reschedule(timerIdCandidate: string, dueAt: Date): Promise<boolean> {
    const timerId = z.string().uuid().parse(timerIdCandidate);
    if (!Number.isFinite(dueAt.getTime())) throw new Error("Timer reschedule requires a valid due instant");
    const rows = await this.database<{ readonly id: string }[]>`
      update timers set status = 'scheduled', due_at = ${dueAt}, updated_at = now()
      where id = ${timerId} and status = 'claimed'
      returning id
    `;
    return rows.length === 1;
  }

  public async supersedeCoverageTimers(
    coverageLoopIdCandidate: string,
    beforeVersion: number,
  ): Promise<number> {
    const coverageLoopId = z.string().uuid().parse(coverageLoopIdCandidate);
    if (!Number.isSafeInteger(beforeVersion) || beforeVersion < 1) {
      throw new Error("Coverage timer version must be a positive integer");
    }
    const rows = await this.database<{ readonly id: string }[]>`
      update timers set status = 'superseded', due_at = null, updated_at = now()
      where coverage_loop_id = ${coverageLoopId}
        and loop_version < ${beforeVersion}
        and status in ('scheduled', 'claimed')
      returning id
    `;
    return rows.length;
  }

  public async finish(timerId: string, status: "fired" | "cancelled" | "superseded" | "dead"): Promise<void> {
    await this.database`
      update timers set status = ${status}, due_at = null, updated_at = now()
      where id = ${timerId} and status in ('scheduled', 'claimed')
    `;
  }
}

function coverageTimerFromRow(row: TimerRow): CoverageTimer | null {
  if (row.coverage_loop_id === null) return null;
  return CoverageTimerSchema.parse({
    timerId: row.id,
    loopId: row.coverage_loop_id,
    loopVersion: Number(row.loop_version),
    planVersion: Number(row.plan_version),
    attentionCycle: row.attention_cycle,
    participantEpochId: row.participant_epoch_id,
    participantSetDigest: row.participant_set_digest,
    category: row.notification_category,
    dueAt: row.due_at.toISOString(),
  });
}

function fenceFromTimerRow(row: TimerRow): AuthorityFence {
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

function positiveFence<Key extends "controlEpoch" | "authorityVersion">(
  entry: { readonly id: string } & Record<Key, number>,
  versionKey: Key,
): { readonly id: string } & Record<Key, number> {
  const parsedId = z.string().uuid().parse(entry.id);
  const version = entry[versionKey];
  if (!Number.isSafeInteger(version) || version < 1)
    throw new Error("Authority fence versions must be positive integers");
  return { ...entry, id: parsedId, [versionKey]: version } as { readonly id: string } & Record<Key, number>;
}

function inTransaction<Result>(
  executor: Executor,
  operation: (transaction: Transaction) => Promise<Result>,
): Promise<Result> {
  return "begin" in executor
    ? (executor.begin(operation) as unknown as Promise<Result>)
    : operation(executor);
}
