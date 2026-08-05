import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { Temporal } from "@js-temporal/polyfill";
import { z } from "zod";
import type { FlorenceApplication } from "../application/ports.js";
import type { ApplicationWorkerHost } from "../application/worker-entrypoint.js";
import type { Database } from "../db/client.js";
import {
  AdultIdSchema,
  HouseholdIdSchema,
  InstantStringSchema,
  LocalTimeSchema,
  TimeZoneSchema,
} from "../domain/index.js";
import { privateReviewSummaryAad } from "../security/private-review.js";
import type { SecretBox } from "../security/secret-box.js";
import { startLeaseHeartbeat } from "./lease-heartbeat.js";

const QueueErrorCodeSchema = z.string().regex(/^[a-z][a-z0-9_.-]{0,99}$/);
const LocalDateSchema = z.iso.date();

const DailyBriefLeaseBaseShape = {
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
} as const;

const PrivateReviewLeaseItemSchema = z.strictObject({
  itemKey: z.string().trim().min(1).max(500),
  adultId: AdultIdSchema,
  source: z.enum(["gmail", "calendar"]),
  summary: z.string().trim().min(1).max(500),
  observedAt: InstantStringSchema,
});

export const DailyBriefLeaseSchema = z.discriminatedUnion("kind", [
  z.strictObject({
    ...DailyBriefLeaseBaseShape,
    kind: z.literal("household"),
  }),
  z.strictObject({
    ...DailyBriefLeaseBaseShape,
    kind: z.literal("private_review"),
    adultId: AdultIdSchema,
    items: z.array(PrivateReviewLeaseItemSchema).min(1).max(5),
  }),
]);

export type DailyBriefLease = z.infer<typeof DailyBriefLeaseSchema>;

export interface EligibleDailyBriefHousehold {
  readonly kind: "household";
  readonly householdId: string;
  readonly timeZone: string;
}

export interface EligiblePrivateReviewAdult {
  readonly kind: "private_review";
  readonly householdId: string;
  readonly adultId: string;
  readonly timeZone: string;
}

export type EligibleDailyBriefRecipient = EligibleDailyBriefHousehold | EligiblePrivateReviewAdult;

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
  listEligibleRecipients(): Promise<readonly EligibleDailyBriefRecipient[]>;
  enqueue(input: {
    readonly kind: "household" | "private_review";
    readonly householdId: string;
    readonly adultId?: string;
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
  renewLease(input: {
    readonly rowId: string;
    readonly leaseToken: string;
    readonly leaseSeconds: number;
  }): Promise<boolean>;
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
  kind: "household" | "private_review";
  adult_id: string | null;
};

type PrivateReviewRow = {
  digest_run_id: string;
  household_id: string;
  adult_id: string;
  item_key: string;
  source: "gmail" | "calendar";
  summary_ciphertext: string;
  observed_at: Date;
};

export class PostgresDailyBriefQueue implements DailyBriefQueuePort {
  constructor(
    private readonly database: Database,
    private readonly privateReviewSecrets: SecretBox,
  ) {}

  async listEligibleRecipients(): Promise<readonly EligibleDailyBriefRecipient[]> {
    const rows = await this.database<
      {
        kind: "household" | "private_review";
        household_id: string;
        adult_id: string | null;
        time_zone: string;
      }[]
    >`
      select h.id as household_id, h.timezone as time_zone
        , 'household'::text as kind, null::uuid as adult_id
      from households h
      join application_snapshots snapshot on snapshot.household_id = h.id
      where h.status = 'active'
        and snapshot.application_phase = 'active'
        and exists (
          select 1 from channel_bindings channel
          where channel.household_id = h.id and channel.provider = 'linq'
            and channel.channel_type = 'group' and channel.status = 'active'
        )
      union all
      select distinct h.id as household_id, coalesce(adult.timezone, h.timezone) as time_zone,
        'private_review'::text as kind, review.adult_id
      from private_review_items review
      join households h on h.id = review.household_id
      join adults adult on adult.id = review.adult_id
      join household_memberships membership
        on membership.household_id = review.household_id and membership.adult_id = review.adult_id
      join application_snapshots snapshot on snapshot.household_id = h.id
      where h.status = 'active' and membership.status = 'active'
        and snapshot.application_phase = 'active'
        and review.digest_run_id is null and review.retention_until > now()
        and exists (
          select 1 from channel_bindings channel
          where channel.household_id = review.household_id and channel.adult_id = review.adult_id
            and channel.provider = 'linq' and channel.channel_type = 'private'
            and channel.status = 'active'
        )
      order by household_id, adult_id nulls first
    `;
    return rows.map((row) =>
      row.kind === "household"
        ? { kind: "household", householdId: row.household_id, timeZone: row.time_zone }
        : {
            kind: "private_review",
            householdId: row.household_id,
            adultId: AdultIdSchema.parse(row.adult_id),
            timeZone: row.time_zone,
          },
    );
  }

  async enqueue(input: {
    kind: "household" | "private_review";
    householdId: string;
    adultId?: string;
    localDate: string;
    timeZone: string;
    scheduledFor: string;
    expiresAt: string;
    idempotencyKey: string;
    maxAttempts: number;
  }): Promise<boolean> {
    const scheduleShape = {
      householdId: z.uuid(),
      localDate: LocalDateSchema,
      timeZone: TimeZoneSchema,
      scheduledFor: InstantStringSchema,
      expiresAt: InstantStringSchema,
      idempotencyKey: z.string().trim().min(1).max(512),
      maxAttempts: z.number().int().positive().max(100),
    } as const;
    const parsed = z
      .discriminatedUnion("kind", [
        z.strictObject({ ...scheduleShape, kind: z.literal("household") }),
        z.strictObject({
          ...scheduleShape,
          kind: z.literal("private_review"),
          adultId: z.uuid(),
        }),
      ])
      .parse(input);
    if (Temporal.Instant.compare(parsed.expiresAt, parsed.scheduledFor) <= 0) {
      throw new Error("Daily brief expiry must follow its scheduled time");
    }
    if (parsed.kind === "private_review") {
      return this.database.begin(async (transaction) => {
        const runId = randomUUID();
        const runs = await transaction<{ id: string }[]>`
          insert into daily_brief_runs (
            id, household_id, adult_id, kind, local_date, time_zone, scheduled_for,
            expires_at, idempotency_key, max_attempts, available_at
          )
          select ${runId}, household.id, ${parsed.adultId}, 'private_review',
            ${parsed.localDate}::date, ${parsed.timeZone}, ${parsed.scheduledFor},
            ${parsed.expiresAt}, ${parsed.idempotencyKey}, ${parsed.maxAttempts},
            ${parsed.scheduledFor}
          from households household
          join adults adult on adult.id = ${parsed.adultId}
          join household_memberships membership
            on membership.household_id = household.id and membership.adult_id = adult.id
          join application_snapshots snapshot on snapshot.household_id = household.id
          where household.id = ${parsed.householdId} and household.status = 'active'
            and membership.status = 'active'
            and coalesce(adult.timezone, household.timezone) = ${parsed.timeZone}
            and snapshot.application_phase = 'active'
            and exists (
              select 1 from channel_bindings channel
              where channel.household_id = household.id and channel.adult_id = adult.id
                and channel.provider = 'linq' and channel.channel_type = 'private'
                and channel.status = 'active'
            )
            and exists (
              select 1 from private_review_items review
              where review.household_id = household.id and review.adult_id = adult.id
                and review.digest_run_id is null and review.created_at <= ${parsed.scheduledFor}
                and review.retention_until > now()
            )
          on conflict do nothing
          returning id
        `;
        if (runs.length !== 1) return false;
        const assigned = await transaction<{ id: string }[]>`
          with candidates as (
            select review.id from private_review_items review
            where review.household_id = ${parsed.householdId}
              and review.adult_id = ${parsed.adultId} and review.digest_run_id is null
              and review.created_at <= ${parsed.scheduledFor} and review.retention_until > now()
            order by review.observed_at, review.id
            for update skip locked
            limit 5
          )
          update private_review_items review
          set digest_run_id = ${runId}, updated_at = now()
          from candidates where review.id = candidates.id
          returning review.id
        `;
        if (assigned.length > 0) return true;
        await transaction`delete from daily_brief_runs where id = ${runId}`;
        return false;
      });
    }
    const rows = await this.database<{ id: string }[]>`
      insert into daily_brief_runs (
        id, household_id, kind, local_date, time_zone, scheduled_for, expires_at,
        idempotency_key, max_attempts, available_at
      )
      select ${randomUUID()}, h.id, 'household', ${parsed.localDate}::date, ${parsed.timeZone},
        ${parsed.scheduledFor}, ${parsed.expiresAt}, ${parsed.idempotencyKey},
        ${parsed.maxAttempts}, ${parsed.scheduledFor}
      from households h
      join application_snapshots snapshot on snapshot.household_id = h.id
      where h.id = ${parsed.householdId} and h.timezone = ${parsed.timeZone}
        and h.status = 'active'
        and snapshot.application_phase = 'active'
        and exists (
          select 1 from channel_bindings channel
          where channel.household_id = h.id and channel.provider = 'linq'
            and channel.channel_type = 'group' and channel.status = 'active'
        )
      on conflict do nothing
      returning id
    `;
    return rows.length === 1;
  }

  async expire(input: { asOf: string }): Promise<number> {
    const parsed = z.strictObject({ asOf: InstantStringSchema }).parse(input);
    return this.database.begin(async (transaction) => {
      const rows = await transaction<{ id: string }[]>`
        update daily_brief_runs
        set status = 'dead', dead_at = ${parsed.asOf},
            last_error_code = 'daily_brief_window_expired', lease_owner = null,
            lease_token = null, lease_expires_at = null, updated_at = now()
        where status in ('pending', 'retry', 'leased') and expires_at <= ${parsed.asOf}
        returning id
      `;
      await transaction`
        update private_review_items review
        set digest_run_id = null, updated_at = now()
        from daily_brief_runs run
        where review.digest_run_id = run.id and run.status = 'dead'
          and run.kind = 'private_review' and review.reviewed_at is null
      `;
      return rows.length;
    });
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
    const claimed = await this.database.begin(async (transaction) => {
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
      await transaction`
        update private_review_items review
        set digest_run_id = null, updated_at = now()
        from daily_brief_runs run
        where review.digest_run_id = run.id and run.status = 'dead'
          and run.kind = 'private_review' and review.reviewed_at is null
      `;
      const rows = await transaction<DailyBriefRow[]>`
        with candidates as (
          select run.id
          from daily_brief_runs run
          join households household on household.id = run.household_id
          join application_snapshots snapshot on snapshot.household_id = run.household_id
          where run.expires_at > ${parsed.asOf} and run.attempt < run.max_attempts
            and run.scheduled_for <= ${parsed.asOf}
            and household.status = 'active'
            and snapshot.application_phase = 'active'
            and (
              (
                run.kind = 'household' and run.adult_id is null
                and household.timezone = run.time_zone
                and exists (
                  select 1 from channel_bindings channel
                  where channel.household_id = run.household_id and channel.provider = 'linq'
                    and channel.channel_type = 'group' and channel.status = 'active'
                )
              )
              or (
                run.kind = 'private_review' and run.adult_id is not null
                and exists (
                  select 1 from adults adult
                  join household_memberships membership
                    on membership.household_id = run.household_id
                    and membership.adult_id = adult.id and membership.status = 'active'
                  where adult.id = run.adult_id
                    and coalesce(adult.timezone, household.timezone) = run.time_zone
                )
                and exists (
                  select 1 from channel_bindings channel
                  where channel.household_id = run.household_id
                    and channel.adult_id = run.adult_id and channel.provider = 'linq'
                    and channel.channel_type = 'private' and channel.status = 'active'
                )
                and exists (
                  select 1 from private_review_items review
                  where review.digest_run_id = run.id
                    and review.household_id = run.household_id
                    and review.adult_id = run.adult_id
                    and review.retention_until > ${parsed.asOf}
                )
              )
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
          run.max_attempts, run.lease_token, run.lease_expires_at, run.kind, run.adult_id
      `;
      const privateRunIds = rows.filter((row) => row.kind === "private_review").map((row) => row.id);
      const reviewRows =
        privateRunIds.length === 0
          ? []
          : await transaction<PrivateReviewRow[]>`
              select review.digest_run_id, review.household_id, review.adult_id,
                review.item_key, review.source, review.summary_ciphertext, review.observed_at
              from private_review_items review
              join daily_brief_runs run on run.id = review.digest_run_id
                and run.household_id = review.household_id and run.adult_id = review.adult_id
              where review.digest_run_id = any(${privateRunIds})
                and run.status = 'leased' and run.lease_token = ${leaseToken}
                and review.retention_until > ${parsed.asOf}
              order by review.digest_run_id, review.observed_at, review.id
              for share of review
            `;
      return { rows, reviewRows };
    });
    const itemsByRun = new Map<string, PrivateReviewRow[]>();
    for (const item of claimed.reviewRows) {
      const existing = itemsByRun.get(item.digest_run_id) ?? [];
      existing.push(item);
      itemsByRun.set(item.digest_run_id, existing);
    }
    return claimed.rows.map((row) =>
      mapDailyBriefLease(row, itemsByRun.get(row.id) ?? [], this.privateReviewSecrets),
    );
  }

  async renewLease(input: { rowId: string; leaseToken: string; leaseSeconds: number }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        leaseSeconds: z.number().int().positive().max(86_400),
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update daily_brief_runs
      set lease_expires_at = least(
            now() + (${parsed.leaseSeconds} * interval '1 second'),
            expires_at
          ),
          updated_at = now()
      where id = ${parsed.rowId} and status = 'leased'
        and lease_token = ${parsed.leaseToken} and lease_expires_at > now()
        and expires_at > now()
      returning id
    `;
    return rows.length === 1;
  }

  async complete(input: { rowId: string; leaseToken: string; completedAt: string }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        completedAt: InstantStringSchema,
      })
      .parse(input);
    return this.database.begin(async (transaction) => {
      const rows = await transaction<{ id: string; kind: "household" | "private_review" }[]>`
        update daily_brief_runs
        set status = 'succeeded', completed_at = ${parsed.completedAt},
            lease_owner = null, lease_token = null, lease_expires_at = null,
            updated_at = now()
        where id = ${parsed.rowId} and status = 'leased' and lease_token = ${parsed.leaseToken}
          and lease_expires_at > now() and expires_at > ${parsed.completedAt}
        returning id, kind
      `;
      if (rows[0]?.kind === "private_review") {
        await transaction`
          delete from private_review_items
          where digest_run_id = ${parsed.rowId}
        `;
      }
      return rows.length === 1;
    });
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
    return this.database.begin(async (transaction) => {
      const rows = await transaction<{ status: "retry" | "dead" }[]>`
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
          and lease_expires_at > now()
        returning status
      `;
      if (rows[0]?.status === "dead") {
        await transaction`
          update private_review_items review
          set digest_run_id = null, updated_at = now()
          from daily_brief_runs run
          where review.digest_run_id = run.id and run.id = ${parsed.rowId}
            and run.status = 'dead' and run.kind = 'private_review'
            and review.reviewed_at is null
        `;
      }
      return rows[0]?.status ?? "lost_lease";
    });
  }
}

function mapDailyBriefLease(
  row: DailyBriefRow,
  privateReviewRows: readonly PrivateReviewRow[],
  privateReviewSecrets: SecretBox,
): DailyBriefLease {
  const common = {
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
  };
  if (row.kind === "household") {
    return DailyBriefLeaseSchema.parse({ ...common, kind: "household" });
  }
  const adultId = AdultIdSchema.parse(row.adult_id);
  return DailyBriefLeaseSchema.parse({
    ...common,
    kind: "private_review",
    adultId,
    items: privateReviewRows.map((item) => {
      if (item.household_id !== row.household_id || item.adult_id !== adultId) {
        throw new Error("Private-review digest crossed its owning adult scope");
      }
      return {
        itemKey: item.item_key,
        adultId,
        source: item.source,
        summary: privateReviewSecrets.open(
          item.summary_ciphertext,
          privateReviewSummaryAad({
            householdId: row.household_id,
            adultId,
            itemKey: item.item_key,
          }),
        ),
        observedAt: item.observed_at.toISOString(),
      };
    }),
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

    let recipients: readonly EligibleDailyBriefRecipient[] = [];
    try {
      recipients = await this.#queue.listEligibleRecipients();
      report.eligible = recipients.length;
    } catch {
      report.reconciliationFailures += 1;
    }
    for (const recipient of recipients) {
      let schedule: DailyBriefSchedule;
      try {
        schedule = resolveDailyBriefSchedule({
          asOf,
          timeZone: recipient.timeZone,
          localTime: this.#localTime,
        });
      } catch {
        report.invalidSchedules += 1;
        continue;
      }
      if (recipient.kind === "private_review" && !schedule.due) continue;
      try {
        const created = await this.#queue.enqueue({
          kind: recipient.kind,
          householdId: recipient.householdId,
          ...(recipient.kind === "private_review" ? { adultId: recipient.adultId } : {}),
          localDate: schedule.localDate,
          timeZone: schedule.timeZone,
          scheduledFor: schedule.scheduledFor,
          expiresAt: schedule.expiresAt,
          idempotencyKey:
            recipient.kind === "household"
              ? dailyBriefIdempotencyKey(recipient.householdId, schedule.localDate)
              : privateReviewDigestIdempotencyKey(
                  recipient.householdId,
                  recipient.adultId,
                  schedule.localDate,
                ),
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
    const heartbeat = await startLeaseHeartbeat({
      leaseSeconds: this.#leaseSeconds,
      upstreamSignal: signal,
      renew: () =>
        this.#queue.renewLease({
          rowId: lease.rowId,
          leaseToken: lease.leaseToken,
          leaseSeconds: this.#leaseSeconds,
        }),
    });
    if (!heartbeat.owned) {
      await heartbeat.stop();
      report.lostLease += 1;
      return;
    }
    let processing:
      | {
          readonly ok: true;
          readonly result: { readonly outcome: { readonly status: "processed" | "rejected" } };
        }
      | { readonly ok: false };
    try {
      processing = {
        ok: true,
        result: await application.process(
          lease.kind === "household"
            ? {
                kind: "daily_brief",
                householdId: lease.householdId,
                idempotencyKey: lease.idempotencyKey,
                occurredAt: lease.scheduledFor,
                reason: "scheduled",
              }
            : {
                kind: "private_review_digest",
                householdId: lease.householdId,
                idempotencyKey: lease.idempotencyKey,
                occurredAt: lease.scheduledFor,
                localDate: lease.localDate,
                adultId: lease.adultId,
                items: lease.items,
              },
        ),
      };
    } catch {
      processing = { ok: false };
    }
    if (!(await heartbeat.stop())) {
      report.lostLease += 1;
      return;
    }
    if (!processing.ok) {
      await this.#recordFailure(lease, "daily_brief_processing_failure", false, report);
      return;
    }
    if (processing.result.outcome.status === "rejected") {
      await this.#recordFailure(lease, "daily_brief_rejected", true, report);
      return;
    }
    try {
      const settled = await this.#queue.complete({
        rowId: lease.rowId,
        leaseToken: lease.leaseToken,
        completedAt: this.#instantNow(),
      });
      if (settled) report.succeeded += 1;
      else report.lostLease += 1;
    } catch {
      await this.#recordFailure(lease, "daily_brief_settlement_failure", false, report);
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
  readonly privateReviewSecrets: SecretBox;
}

export function createPostgresDailyBriefHost(options: CreatePostgresDailyBriefHostOptions): DailyBriefHost {
  return new DailyBriefHost({
    ...options,
    queue: new PostgresDailyBriefQueue(options.database, options.privateReviewSecrets),
  });
}

function dailyBriefIdempotencyKey(householdId: string, localDate: string): string {
  return `daily-brief:scheduled:${householdId}:${localDate}`;
}

function privateReviewDigestIdempotencyKey(householdId: string, adultId: string, localDate: string): string {
  return `private-review-digest:scheduled:${householdId}:${adultId}:${localDate}`;
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
