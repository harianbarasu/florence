import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { closeDatabase, createDatabase, type Database } from "../../src/db/client.js";
import { migrateDatabase } from "../../src/db/migrate.js";
import { DailyBriefHost, PostgresDailyBriefQueue } from "../../src/infrastructure/daily-brief-host.js";

const databaseUrl = process.env.TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)("daily brief PostgreSQL integration", () => {
  const schema = `daily_brief_${randomUUID().replaceAll("-", "").slice(0, 20)}`;
  let database: Database;

  beforeAll(async () => {
    database = createDatabase(databaseUrl as string, { max: 12, schema });
    await migrateDatabase(database, schema);
  });

  beforeEach(async () => {
    await database`truncate table households cascade`;
  });

  afterAll(async () => {
    if (!database) return;
    await database.unsafe(`drop schema if exists "${schema}" cascade`);
    await closeDatabase(database);
  });

  async function seedHousehold(
    input: {
      status?: "onboarding" | "learning" | "active" | "paused" | "deleting";
      timeZone?: string;
      onboardingPhase?: string;
      withActiveGroup?: boolean;
    } = {},
  ): Promise<string> {
    const householdId = randomUUID();
    await database`
      insert into households (id, name, timezone, status)
      values (
        ${householdId}, ${`Daily brief ${householdId}`}, ${input.timeZone ?? "UTC"},
        ${input.status ?? "active"}
      )
    `;
    await database`
      insert into application_snapshots (
        household_id, schema_version, revision, aggregate, projection
      ) values (
        ${householdId}, 1, 0, ${database.json({})},
        ${database.json({ onboarding: { phase: input.onboardingPhase ?? "active" } })}
      )
    `;
    if (input.withActiveGroup ?? true) {
      await database`
        insert into channel_bindings (
          id, household_id, adult_id, provider, channel_type,
          external_chat_id, external_handle, status, metadata
        ) values (
          ${randomUUID()}, ${householdId}, null, 'linq', 'group',
          ${`daily-brief-group-${randomUUID()}`}, null, 'active', ${database.json({})}
        )
      `;
    }
    return householdId;
  }

  it("reconciles and claims exactly once across concurrent hosts on a DST transition", async () => {
    const householdId = await seedHousehold({ timeZone: "America/Los_Angeles" });
    await seedHousehold({ status: "paused", timeZone: "America/Los_Angeles" });
    await seedHousehold({ timeZone: "America/Los_Angeles", withActiveGroup: false });
    await seedHousehold({ timeZone: "America/Los_Angeles", onboardingPhase: "building_profile" });
    const process = vi.fn(async () => ({ outcome: { status: "processed" as const } }));
    const now = () => new Date("2027-03-14T10:31:00.000Z");
    const hosts = ["left", "right"].map(
      (ownerId) =>
        new DailyBriefHost({
          queue: new PostgresDailyBriefQueue(database),
          localTime: "02:30",
          ownerId,
          now,
        }),
    );

    const reports = await Promise.all(hosts.map((candidate) => candidate.runOnce({ process })));

    expect(reports.reduce((total, report) => total + report.created, 0)).toBe(1);
    expect(reports.reduce((total, report) => total + report.claimed, 0)).toBe(1);
    expect(reports.reduce((total, report) => total + report.succeeded, 0)).toBe(1);
    expect(process).toHaveBeenCalledOnce();
    expect(process).toHaveBeenCalledWith({
      kind: "daily_brief",
      householdId,
      idempotencyKey: `daily-brief:scheduled:${householdId}:2027-03-14`,
      occurredAt: "2027-03-14T10:30:00Z",
      reason: "scheduled",
    });

    const rows = await database<
      Array<{
        household_id: string;
        local_date: string;
        scheduled_for: Date;
        expires_at: Date;
        status: string;
        attempt: number;
      }>
    >`
      select household_id, local_date::text, scheduled_for, expires_at, status, attempt
      from daily_brief_runs
    `;
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      household_id: householdId,
      local_date: "2027-03-14",
      status: "succeeded",
      attempt: 1,
    });
    expect(rows[0]?.scheduled_for.toISOString()).toBe("2027-03-14T10:30:00.000Z");
    expect(rows[0]?.expires_at.toISOString()).toBe("2027-03-15T09:30:00.000Z");

    await expect(hosts[0]?.runOnce({ process })).resolves.toMatchObject({ created: 0, claimed: 0 });
    expect(process).toHaveBeenCalledOnce();
  });

  it("fences stale owners, honors retry availability, and dead-letters exhausted work", async () => {
    const householdId = await seedHousehold();
    const queue = new PostgresDailyBriefQueue(database);
    await expect(
      queue.enqueue({
        householdId,
        localDate: "2027-01-05",
        timeZone: "UTC",
        scheduledFor: "2027-01-05T08:00:00Z",
        expiresAt: "2027-01-06T08:00:00Z",
        idempotencyKey: `daily-brief:scheduled:${householdId}:2027-01-05`,
        maxAttempts: 2,
      }),
    ).resolves.toBe(true);

    await database`
      update channel_bindings set status = 'paused' where household_id = ${householdId}
    `;
    await expect(
      queue.claim({ owner: "paused", asOf: "2027-01-05T08:00:30Z", limit: 1, leaseSeconds: 60 }),
    ).resolves.toHaveLength(0);
    await database`
      update channel_bindings set status = 'active' where household_id = ${householdId}
    `;

    const first = (
      await queue.claim({ owner: "first", asOf: "2027-01-05T08:01:00Z", limit: 1, leaseSeconds: 60 })
    )[0];
    if (!first) throw new Error("Expected the first daily brief lease");
    await expect(
      queue.fail({
        rowId: first.rowId,
        leaseToken: first.leaseToken,
        errorCode: "temporary_failure",
        failedAt: "2027-01-05T08:01:00Z",
        retryAt: "2027-01-05T08:01:30Z",
        permanent: false,
      }),
    ).resolves.toBe("retry");
    await expect(
      queue.claim({ owner: "early", asOf: "2027-01-05T08:01:29Z", limit: 1, leaseSeconds: 60 }),
    ).resolves.toHaveLength(0);

    const second = (
      await queue.claim({ owner: "second", asOf: "2027-01-05T08:01:30Z", limit: 1, leaseSeconds: 60 })
    )[0];
    if (!second) throw new Error("Expected the retry daily brief lease");
    expect(second.attempt).toBe(2);
    expect(second.leaseToken).not.toBe(first.leaseToken);
    await expect(
      queue.complete({
        rowId: first.rowId,
        leaseToken: first.leaseToken,
        completedAt: "2027-01-05T08:01:31Z",
      }),
    ).resolves.toBe(false);
    await expect(
      queue.claim({ owner: "intruder", asOf: "2027-01-05T08:02:00Z", limit: 1, leaseSeconds: 60 }),
    ).resolves.toHaveLength(0);
    await expect(
      queue.fail({
        rowId: second.rowId,
        leaseToken: randomUUID(),
        errorCode: "temporary_failure",
        failedAt: "2027-01-05T08:02:00Z",
        retryAt: "2027-01-05T08:03:00Z",
        permanent: false,
      }),
    ).resolves.toBe("lost_lease");
    await expect(
      queue.fail({
        rowId: second.rowId,
        leaseToken: second.leaseToken,
        errorCode: "temporary_failure",
        failedAt: "2027-01-05T08:02:00Z",
        retryAt: "2027-01-05T08:03:00Z",
        permanent: false,
      }),
    ).resolves.toBe("dead");

    const rows = await database<Array<{ status: string; attempt: number; last_error_code: string }>>`
      select status, attempt, last_error_code from daily_brief_runs where household_id = ${householdId}
    `;
    expect(rows).toEqual([{ status: "dead", attempt: 2, last_error_code: "daily_brief_max_attempts" }]);
  });

  it("expires a missed local day before creating the current run", async () => {
    const householdId = await seedHousehold();
    const queue = new PostgresDailyBriefQueue(database);
    await queue.enqueue({
      householdId,
      localDate: "2027-01-04",
      timeZone: "UTC",
      scheduledFor: "2027-01-04T08:00:00Z",
      expiresAt: "2027-01-05T08:00:00Z",
      idempotencyKey: `daily-brief:scheduled:${householdId}:2027-01-04`,
      maxAttempts: 5,
    });
    const process = vi.fn(async () => ({ outcome: { status: "processed" as const } }));
    const scheduler = new DailyBriefHost({
      queue,
      localTime: "08:00",
      ownerId: "current-day",
      now: () => new Date("2027-01-05T09:00:00.000Z"),
    });

    await expect(scheduler.runOnce({ process })).resolves.toMatchObject({
      expired: 1,
      created: 1,
      claimed: 1,
      succeeded: 1,
    });
    expect(process).toHaveBeenCalledOnce();
    expect(process).toHaveBeenCalledWith(
      expect.objectContaining({
        idempotencyKey: `daily-brief:scheduled:${householdId}:2027-01-05`,
        occurredAt: "2027-01-05T08:00:00Z",
      }),
    );
    const rows = await database<
      Array<{ local_date: string; status: string; last_error_code: string | null }>
    >`
      select local_date::text, status, last_error_code
      from daily_brief_runs where household_id = ${householdId} order by local_date
    `;
    expect(rows).toEqual([
      {
        local_date: "2027-01-04",
        status: "dead",
        last_error_code: "daily_brief_window_expired",
      },
      { local_date: "2027-01-05", status: "succeeded", last_error_code: null },
    ]);
  });
});
