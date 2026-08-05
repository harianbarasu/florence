import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { Temporal } from "@js-temporal/polyfill";
import { z } from "zod";
import type { FlorenceApplication } from "../application/ports.js";
import type { ApplicationWorkerHost } from "../application/worker-entrypoint.js";
import type { Database } from "../db/client.js";
import { HouseholdIdSchema, InstantStringSchema, LocalTimeSchema, TimeZoneSchema } from "../domain/index.js";

const QueueErrorCodeSchema = z.string().regex(/^[a-z][a-z0-9_.-]{0,99}$/);
const LocalDateSchema = z.iso.date();

export const DailyBriefLeaseSchema = z.strictObject({
  rowId: z.uuid(),
  householdId: HouseholdIdSchema,
  localDate: LocalDateSchema,
  timeZone: TimeZoneSchema,
  scheduledFor: InstantStringSchema,
  expiresAt: InstantStringSchema,
  idempotencyKey: z.string().trim().min(1).max(512),
  attempt: z.number().int().positive(),
  maxAttempts: z.number().int().positive(),
  leaseToken: z.uuid(),
  leaseExpiresAt: InstantStringSchema,
});

export type DailyBriefLease = z.infer<typeof DailyBriefLeaseSchema>;

export interface EligibleDailyBriefHousehold {
  readonly householdId: string;
  readonly timeZone: string;
}

export interface DailyBriefSchedule {
  readonly localDate: string;
  readonly timeZone: string;
  readonly localTime: string;
  readonly scheduledFor: string;
  readonly expiresAt: string;
  readonly due: boolean;
}

export function resolveDailyBriefSchedule(input: {
  readonly asOf: string;
  readonly timeZone: string;
  readonly localTime: string;
}): DailyBriefSchedule {
  const asOf = Temporal.Instant.from(InstantStringSchema.parse(input.asOf));
  const timeZone = TimeZoneSchema.parse(input.timeZone);
  const localTime = LocalTimeSchema.parse(input.localTime);
  const localDate = asOf.toZonedDateTimeISO(timeZone).toPlainDate();
  const scheduledFor = Temporal.PlainDateTime.from(`${localDate.toString()}T${localTime}`)
    .toZonedDateTime(timeZone, { disambiguation: "compatible" })
    .toInstant();
  const nextDate = localDate.add({ days: 1 });
  const expiresAt = Temporal.PlainDateTime.from(`${nextDate.toString()}T${localTime}`)
    .toZonedDateTime(timeZone, { disambiguation: "compatible" })
    .toInstant();
  return {
    localDate: localDate.toString(),
    timeZone,
    localTime,
    scheduledFor: InstantStringSchema.parse(scheduledFor.toString()),
    expiresAt: InstantStringSchema.parse(expiresAt.toString()),
    due: Temporal.Instant.compare(asOf, scheduledFor) >= 0,
  };
}

export type DailyBriefFailureDisposition = "retry" | "dead" | "lost_lease";

export interface DailyBriefQueuePort {
  listEligibleHouseholds(): Promise<readonly EligibleDailyBriefHousehold[]>;
  enqueue(input: {
    readonly householdId: string;
    readonly localDate: string;
    readonly timeZone: string;
    readonly scheduledFor: string;
    readonly expiresAt: string;
    readonly idempotencyKey: string;
    readonly maxAttempts: number;
  }): Promise<boolean>;
  expire(input: { readonly asOf: string }): Promise<number>;
  claim(input: {
    readonly owner: string;
    readonly asOf: string;
    readonly limit: number;
    readonly leaseSeconds: number;
  }): Promise<readonly DailyBriefLease[]>;
  complete(input: {
    readonly rowId: string;
    readonly leaseToken: string;
    readonly completedAt: string;
  }): Promise<boolean>;
  fail(input: {
    readonly rowId: string;
    readonly leaseToken: string;
    readonly errorCode: string;
    readonly failedAt: string;
    readonly retryAt: string;
    readonly permanent: boolean;
  }): Promise<DailyBriefFailureDisposition>;
}

type DailyBriefRow = {
  id: string;
  household_id: string;
  local_date: string;
  time_zone: string;
  scheduled_for: Date;
  expires_at: Date;
  idempotency_key: string;
  attempt: number;
  max_attempts: number;
  lease_token: string;
  lease_expires_at: Date;
};

export class PostgresDailyBriefQueue implements DailyBriefQueuePort {
  constructor(private readonly database: Database) {}

  async listEligibleHouseholds(): Promise<readonly EligibleDailyBriefHousehold[]> {
    const rows = await this.database<{ household_id: string; time_zone: string }[]>`
      select h.id as household_id, h.timezone as time_zone
      from households h
      join application_snapshots snapshot on snapshot.household_id = h.id
      where h.status = 'active'
        and snapshot.projection #>> '{onboarding,phase}' = 'active'
        and exists (
          select 1 from channel_bindings channel
          where channel.household_id = h.id and channel.provider = 'linq'
            and channel.channel_type = 'group' and channel.status = 'active'
        )
      order by h.id
    `;
    return rows.map((row) => ({ householdId: row.household_id, timeZone: row.time_zone }));
  }

  async enqueue(input: {
    householdId: string;
    localDate: string;
    timeZone: string;
    scheduledFor: string;
    expiresAt: string;
    idempotencyKey: string;
    maxAttempts: number;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        localDate: LocalDateSchema,
        timeZone: TimeZoneSchema,
        scheduledFor: InstantStringSchema,
        expiresAt: InstantStringSchema,
        idempotencyKey: z.string().trim().min(1).max(512),
        maxAttempts: z.number().int().positive().max(100),
      })
      .parse(input);
    if (Temporal.Instant.compare(parsed.expiresAt, parsed.scheduledFor) <= 0) {
      throw new Error("Daily brief expiry must follow its scheduled time");
    }
    const rows = await this.database<{ id: string }[]>`
      insert into daily_brief_runs (
        id, household_id, local_date, time_zone, scheduled_for, expires_at,
        idempotency_key, max_attempts, available_at
      )
      select ${randomUUID()}, h.id, ${parsed.localDate}::date, ${parsed.timeZone},
        ${parsed.scheduledFor}, ${parsed.expiresAt}, ${parsed.idempotencyKey},
        ${parsed.maxAttempts}, ${parsed.scheduledFor}
      from households h
      join application_snapshots snapshot on snapshot.household_id = h.id
      where h.id = ${parsed.householdId} and h.timezone = ${parsed.timeZone}
        and h.status = 'active'
        and snapshot.projection #>> '{onboarding,phase}' = 'active'
        and exists (
          select 1 from channel_bindings channel
          where channel.household_id = h.id and channel.provider = 'linq'
            and channel.channel_type = 'group' and channel.status = 'active'
        )
      on conflict (household_id, local_date) do nothing
      returning id
    `;
    return rows.length === 1;
  }

  async expire(input: { asOf: string }): Promise<number> {
    const parsed = z.strictObject({ asOf: InstantStringSchema }).parse(input);
    const rows = await this.database<{ id: string }[]>`
      update daily_brief_runs
      set status = 'dead', dead_at = ${parsed.asOf},
          last_error_code = 'daily_brief_window_expired', lease_owner = null,
          lease_token = null, lease_expires_at = null, updated_at = now()
      where status in ('pending', 'retry', 'leased') and expires_at <= ${parsed.asOf}
      returning id
    `;
    return rows.length;
  }

  async claim(input: {
    owner: string;
    asOf: string;
    limit: number;
    leaseSeconds: number;
  }): Promise<readonly DailyBriefLease[]> {
    const parsed = z
      .strictObject({
        owner: z.string().trim().min(1).max(200),
        asOf: InstantStringSchema,
        limit: z.number().int().positive().max(500),
        leaseSeconds: z.number().int().positive().max(86_400),
      })
      .parse(input);
    const leaseToken = randomUUID();
    return this.database.begin(async (transaction) => {
      await transaction`
        update daily_brief_runs
        set status = 'dead', dead_at = ${parsed.asOf},
            last_error_code = case
              when expires_at <= ${parsed.asOf} then 'daily_brief_window_expired'
              else 'daily_brief_max_attempts'
            end,
            lease_owner = null, lease_token = null, lease_expires_at = null,
            updated_at = now()
        where status in ('pending', 'retry', 'leased')
          and (
            expires_at <= ${parsed.asOf}
            or (
              attempt >= max_attempts
              and (
                status in ('pending', 'retry')
                or (status = 'leased' and lease_expires_at <= ${parsed.asOf})
              )
            )
          )
      `;
      const rows = await transaction<DailyBriefRow[]>`
        with candidates as (
          select run.id
          from daily_brief_runs run
          join households household on household.id = run.household_id
          join application_snapshots snapshot on snapshot.household_id = run.household_id
          where run.expires_at > ${parsed.asOf} and run.attempt < run.max_attempts
            and run.scheduled_for <= ${parsed.asOf}
            and household.status = 'active' and household.timezone = run.time_zone
            and snapshot.projection #>> '{onboarding,phase}' = 'active'
            and exists (
              select 1 from channel_bindings channel
              where channel.household_id = run.household_id and channel.provider = 'linq'
                and channel.channel_type = 'group' and channel.status = 'active'
            )
            and (
              (run.status in ('pending', 'retry') and run.available_at <= ${parsed.asOf})
              or
              (run.status = 'leased' and run.lease_expires_at <= ${parsed.asOf})
            )
          order by run.available_at, run.scheduled_for, run.created_at, run.id
          for update of run skip locked
          limit ${parsed.limit}
        )
        update daily_brief_runs run
        set status = 'leased', lease_owner = ${parsed.owner}, lease_token = ${leaseToken},
            lease_expires_at = least(
              ${parsed.asOf}::timestamptz + (${parsed.leaseSeconds} * interval '1 second'),
              run.expires_at
            ),
            attempt = run.attempt + 1, updated_at = now()
        from candidates where run.id = candidates.id
        returning run.id, run.household_id, run.local_date::text, run.time_zone,
          run.scheduled_for, run.expires_at, run.idempotency_key, run.attempt,
          run.max_attempts, run.lease_token, run.lease_expires_at
      `;
      return rows.map(mapDailyBriefLease);
    });
  }

  async complete(input: { rowId: string; leaseToken: string; completedAt: string }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        completedAt: InstantStringSchema,
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update daily_brief_runs
      set status = 'succeeded', completed_at = ${parsed.completedAt},
          lease_owner = null, lease_token = null, lease_expires_at = null,
          updated_at = now()
      where id = ${parsed.rowId} and status = 'leased' and lease_token = ${parsed.leaseToken}
      returning id
    `;
    return rows.length === 1;
  }

  async fail(input: {
    rowId: string;
    leaseToken: string;
    errorCode: string;
    failedAt: string;
    retryAt: string;
    permanent: boolean;
  }): Promise<DailyBriefFailureDisposition> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        errorCode: QueueErrorCodeSchema,
        failedAt: InstantStringSchema,
        retryAt: InstantStringSchema,
        permanent: z.boolean(),
      })
      .parse(input);
    const rows = await this.database<{ status: "retry" | "dead" }[]>`
      update daily_brief_runs
      set status = case
            when ${parsed.permanent}
              or attempt >= max_attempts
              or ${parsed.retryAt}::timestamptz >= expires_at
              then 'dead'
            else 'retry'
          end,
          available_at = ${parsed.retryAt},
          dead_at = case
            when ${parsed.permanent}
              or attempt >= max_attempts
              or ${parsed.retryAt}::timestamptz >= expires_at
              then ${parsed.failedAt}::timestamptz
            else null::timestamptz
          end,
          last_error_code = case
            when ${parsed.permanent} then ${parsed.errorCode}
            when attempt >= max_attempts then 'daily_brief_max_attempts'
            when ${parsed.retryAt}::timestamptz >= expires_at then 'daily_brief_window_expired'
            else ${parsed.errorCode}
          end,
          lease_owner = null,
          lease_token = null, lease_expires_at = null, updated_at = now()
      where id = ${parsed.rowId} and status = 'leased' and lease_token = ${parsed.leaseToken}
      returning status
    `;
    return rows[0]?.status ?? "lost_lease";
  }
}

function mapDailyBriefLease(row: DailyBriefRow): DailyBriefLease {
  return DailyBriefLeaseSchema.parse({
    rowId: row.id,
    householdId: row.household_id,
    localDate: row.local_date,
    timeZone: row.time_zone,
    scheduledFor: row.scheduled_for.toISOString(),
    expiresAt: row.expires_at.toISOString(),
    idempotencyKey: row.idempotency_key,
    attempt: Number(row.attempt),
    maxAttempts: Number(row.max_attempts),
    leaseToken: row.lease_token,
    leaseExpiresAt: row.lease_expires_at.toISOString(),
  });
}

export interface DailyBriefApplicationPort {
  process(input: unknown): Promise<{
    readonly outcome: { readonly status: "processed" | "rejected" };
  }>;
}

export interface DailyBriefCycleReport {
  readonly eligible: number;
  readonly created: number;
  readonly expired: number;
  readonly invalidSchedules: number;
  readonly claimed: number;
  readonly succeeded: number;
  readonly retried: number;
  readonly dead: number;
  readonly lostLease: number;
  readonly failed: number;
  readonly reconciliationFailures: number;
  readonly claimFailures: number;
}

type MutableCycleReport = {
  eligible: number;
  created: number;
  expired: number;
  invalidSchedules: number;
  claimed: number;
  succeeded: number;
  retried: number;
  dead: number;
  lostLease: number;
  failed: number;
  reconciliationFailures: number;
  claimFailures: number;
};

export type DailyBriefWait = (milliseconds: number, signal: AbortSignal) => Promise<void>;

export interface DailyBriefHostOptions {
  readonly queue: DailyBriefQueuePort;
  readonly localTime: string;
  readonly ownerId: string;
  readonly pollIntervalMs?: number;
  readonly leaseSeconds?: number;
  readonly batchSize?: number;
  readonly maxAttempts?: number;
  readonly baseRetrySeconds?: number;
  readonly maxRetrySeconds?: number;
  readonly now?: () => Date;
  readonly wait?: DailyBriefWait;
}

export class DailyBriefHost implements ApplicationWorkerHost {
  readonly #queue: DailyBriefQueuePort;
  readonly #localTime: string;
  readonly #ownerId: string;
  readonly #pollIntervalMs: number;
  readonly #leaseSeconds: number;
  readonly #batchSize: number;
  readonly #maxAttempts: number;
  readonly #baseRetrySeconds: number;
  readonly #maxRetrySeconds: number;
  readonly #now: () => Date;
  readonly #wait: DailyBriefWait;

  constructor(options: DailyBriefHostOptions) {
    this.#queue = options.queue;
    this.#localTime = LocalTimeSchema.parse(options.localTime);
    this.#ownerId = z.string().trim().min(1).max(200).parse(options.ownerId);
    this.#pollIntervalMs = z
      .number()
      .int()
      .min(250)
      .max(86_400_000)
      .parse(options.pollIntervalMs ?? 30_000);
    this.#leaseSeconds = z
      .number()
      .int()
      .positive()
      .max(86_400)
      .parse(options.leaseSeconds ?? 120);
    this.#batchSize = z
      .number()
      .int()
      .positive()
      .max(500)
      .parse(options.batchSize ?? 50);
    this.#maxAttempts = z
      .number()
      .int()
      .positive()
      .max(100)
      .parse(options.maxAttempts ?? 5);
    this.#baseRetrySeconds = z
      .number()
      .int()
      .positive()
      .max(86_400)
      .parse(options.baseRetrySeconds ?? 30);
    this.#maxRetrySeconds = z
      .number()
      .int()
      .positive()
      .max(604_800)
      .parse(options.maxRetrySeconds ?? 900);
    if (this.#baseRetrySeconds > this.#maxRetrySeconds) {
      throw new Error("Daily brief base retry must not exceed its maximum retry");
    }
    this.#now = options.now ?? (() => new Date());
    this.#wait = options.wait ?? defaultWait;
  }

  async runOnce(
    application: DailyBriefApplicationPort,
    signal: AbortSignal = new AbortController().signal,
  ): Promise<DailyBriefCycleReport> {
    const report = emptyReport();
    const asOf = this.#instantNow();
    try {
      report.expired = await this.#queue.expire({ asOf });
    } catch {
      report.reconciliationFailures += 1;
    }

    let households: readonly EligibleDailyBriefHousehold[] = [];
    try {
      households = await this.#queue.listEligibleHouseholds();
      report.eligible = households.length;
    } catch {
      report.reconciliationFailures += 1;
    }
    for (const household of households) {
      let schedule: DailyBriefSchedule;
      try {
        schedule = resolveDailyBriefSchedule({
          asOf,
          timeZone: household.timeZone,
          localTime: this.#localTime,
        });
      } catch {
        report.invalidSchedules += 1;
        continue;
      }
      try {
        const created = await this.#queue.enqueue({
          householdId: household.householdId,
          localDate: schedule.localDate,
          timeZone: schedule.timeZone,
          scheduledFor: schedule.scheduledFor,
          expiresAt: schedule.expiresAt,
          idempotencyKey: dailyBriefIdempotencyKey(household.householdId, schedule.localDate),
          maxAttempts: this.#maxAttempts,
        });
        if (created) report.created += 1;
      } catch {
        report.reconciliationFailures += 1;
      }
    }

    if (signal.aborted) return report;

    let leases: readonly DailyBriefLease[];
    try {
      leases = await this.#queue.claim({
        owner: this.#ownerId,
        asOf,
        limit: this.#batchSize,
        leaseSeconds: this.#leaseSeconds,
      });
      report.claimed = leases.length;
    } catch {
      report.claimFailures += 1;
      return report;
    }

    for (const lease of leases) {
      await this.#processLease(application, lease, signal, report);
    }
    return report;
  }

  async run(application: FlorenceApplication, signal?: AbortSignal): Promise<void> {
    const effectiveSignal = signal ?? new AbortController().signal;
    while (!effectiveSignal.aborted) {
      await this.runOnce(application, effectiveSignal);
      if (effectiveSignal.aborted) return;
      try {
        await this.#wait(this.#pollIntervalMs, effectiveSignal);
      } catch {
        if (effectiveSignal.aborted) return;
        throw new Error("Daily brief wait failed");
      }
    }
  }

  async #processLease(
    application: DailyBriefApplicationPort,
    lease: DailyBriefLease,
    signal: AbortSignal,
    report: MutableCycleReport,
  ): Promise<void> {
    if (signal.aborted) {
      await this.#recordFailure(lease, "daily_brief_aborted", false, report);
      return;
    }
    const startedAt = this.#instantNow();
    if (Temporal.Instant.compare(startedAt, lease.expiresAt) >= 0) {
      await this.#recordFailure(lease, "daily_brief_window_expired", true, report);
      return;
    }
    try {
      const result = await application.process({
        kind: "daily_brief",
        householdId: lease.householdId,
        idempotencyKey: lease.idempotencyKey,
        occurredAt: lease.scheduledFor,
        reason: "scheduled",
      });
      if (result.outcome.status === "rejected") {
        await this.#recordFailure(lease, "daily_brief_rejected", true, report);
        return;
      }
      const settled = await this.#queue.complete({
        rowId: lease.rowId,
        leaseToken: lease.leaseToken,
        completedAt: this.#instantNow(),
      });
      if (settled) report.succeeded += 1;
      else report.lostLease += 1;
    } catch {
      await this.#recordFailure(lease, "daily_brief_processing_failure", false, report);
    }
  }

  async #recordFailure(
    lease: DailyBriefLease,
    errorCode: string,
    permanent: boolean,
    report: MutableCycleReport,
  ): Promise<void> {
    const failedAt = this.#instantNow();
    const delaySeconds = Math.min(
      this.#maxRetrySeconds,
      this.#baseRetrySeconds * 2 ** Math.max(0, lease.attempt - 1),
    );
    const retryAt = InstantStringSchema.parse(
      Temporal.Instant.from(failedAt).add({ seconds: delaySeconds }).toString(),
    );
    try {
      const disposition = await this.#queue.fail({
        rowId: lease.rowId,
        leaseToken: lease.leaseToken,
        errorCode: QueueErrorCodeSchema.parse(errorCode),
        failedAt,
        retryAt,
        permanent,
      });
      if (disposition === "retry") report.retried += 1;
      else if (disposition === "dead") report.dead += 1;
      else report.lostLease += 1;
    } catch {
      report.failed += 1;
    }
  }

  #instantNow(): string {
    const value = this.#now();
    if (!(value instanceof Date) || !Number.isFinite(value.getTime())) {
      throw new Error("Daily brief clock returned an invalid date");
    }
    return InstantStringSchema.parse(value.toISOString());
  }
}

export interface CreatePostgresDailyBriefHostOptions extends Omit<DailyBriefHostOptions, "queue"> {
  readonly database: Database;
}

export function createPostgresDailyBriefHost(options: CreatePostgresDailyBriefHostOptions): DailyBriefHost {
  return new DailyBriefHost({
    ...options,
    queue: new PostgresDailyBriefQueue(options.database),
  });
}

function dailyBriefIdempotencyKey(householdId: string, localDate: string): string {
  return `daily-brief:scheduled:${householdId}:${localDate}`;
}

function emptyReport(): MutableCycleReport {
  return {
    eligible: 0,
    created: 0,
    expired: 0,
    invalidSchedules: 0,
    claimed: 0,
    succeeded: 0,
    retried: 0,
    dead: 0,
    lostLease: 0,
    failed: 0,
    reconciliationFailures: 0,
    claimFailures: 0,
  };
}

async function defaultWait(milliseconds: number, signal: AbortSignal): Promise<void> {
  await sleep(milliseconds, undefined, { signal });
}
