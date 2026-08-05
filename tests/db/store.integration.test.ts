import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { closeDatabase, createDatabase, type Database } from "../../src/db/client.js";
import { migrateDatabase } from "../../src/db/migrate.js";
import { FlorenceStore, IdempotencyConflictError, StaleHouseholdVersionError } from "../../src/db/store.js";

const databaseUrl = process.env.TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)("FlorenceStore PostgreSQL integration", () => {
  let database: Database;
  let store: FlorenceStore;
  const householdIds: string[] = [];

  beforeAll(async () => {
    database = createDatabase(databaseUrl as string, { max: 12 });
    await migrateDatabase(database);
    store = new FlorenceStore(database);
  });

  afterAll(async () => {
    if (!database) return;
    for (const householdId of householdIds) {
      await database`delete from households where id = ${householdId}`;
    }
    await closeDatabase(database);
  });

  async function household(): Promise<{ householdId: string; adultId: string }> {
    const created = await store.createFoundingHousehold({
      householdName: "Store test family",
      adultName: "Parent",
      timezone: "America/Los_Angeles",
    });
    householdIds.push(created.householdId);
    return created;
  }

  it("accepts once and deduplicates concurrent retries", async () => {
    const { householdId } = await household();
    const idempotencyKey = `linq:${randomUUID()}`;
    const input = {
      householdId,
      idempotencyKey,
      kind: "linq.message.received",
      actorKind: "adult" as const,
      visibility: "household" as const,
      occurredAt: new Date().toISOString(),
      payload: { text: "School closes early Friday" },
    };

    const receipts = await Promise.all(Array.from({ length: 12 }, () => store.acceptSignal(input)));

    expect(receipts.filter((receipt) => receipt.disposition === "accepted")).toHaveLength(1);
    expect(new Set(receipts.map((receipt) => receipt.signalId))).toHaveLength(1);
    expect(new Set(receipts.map((receipt) => receipt.sequence))).toEqual(new Set([1]));
    const jobs = await database<{ count: string }[]>`
      select count(*)::text as count from jobs where household_id = ${householdId}
    `;
    expect(jobs[0]?.count).toBe("1");
  });

  it("rejects idempotency-key reuse with different content", async () => {
    const { householdId } = await household();
    const idempotencyKey = `linq:${randomUUID()}`;
    const base = {
      householdId,
      idempotencyKey,
      kind: "linq.message.received",
      actorKind: "adult" as const,
      visibility: "household" as const,
      occurredAt: new Date().toISOString(),
    };
    await store.acceptSignal({ ...base, payload: { text: "One" } });

    await expect(store.acceptSignal({ ...base, payload: { text: "Two" } })).rejects.toBeInstanceOf(
      IdempotencyConflictError,
    );
  });

  it("leases and completes jobs using an unforgeable lease token", async () => {
    const { householdId } = await household();
    await store.acceptSignal({
      householdId,
      idempotencyKey: `timer:${randomUUID()}`,
      kind: "timer.fired",
      actorKind: "clock",
      visibility: "household",
      occurredAt: new Date().toISOString(),
      payload: { triggerId: randomUUID() },
    });

    const jobs = await store.claimJobs("worker-a", 10, 60);
    const job = jobs.find((candidate) => candidate.householdId === householdId);
    expect(job).toBeDefined();
    if (!job) throw new Error("Expected a claimed job");

    await expect(store.completeJob(job.id, randomUUID())).resolves.toBe(false);
    await expect(store.completeJob(job.id, job.leaseToken)).resolves.toBe(true);
  });

  it("atomically commits episodes, timers, effects, audit, and household version", async () => {
    const { householdId, adultId } = await household();
    const signal = await store.acceptSignal({
      householdId,
      idempotencyKey: `linq:${randomUUID()}`,
      kind: "linq.message.received",
      actorKind: "adult",
      actorId: adultId,
      visibility: "household",
      occurredAt: new Date().toISOString(),
      payload: { text: "Permission slip due Friday" },
    });
    const episodeId = randomUUID();
    const triggerId = randomUUID();
    const effectId = randomUUID();

    await expect(
      store.commitTransition({
        householdId,
        signalId: signal.signalId,
        expectedHouseholdVersion: 0,
        episodes: [
          {
            id: episodeId,
            episodeType: "commitment",
            visibility: "household",
            ownerAdultId: adultId,
            status: "open",
            title: "Return permission slip",
            acceptedMeaning: { outcome: "Signed form returned" },
            sourceRefs: [{ signalId: signal.signalId }],
            authority: { acknowledgedBy: adultId },
            temporalPlan: { planVersion: 1 },
          },
        ],
        triggers: [
          {
            id: triggerId,
            episodeId,
            triggerKind: "reminder_recheck",
            planVersion: 1,
            dueAt: new Date(Date.now() + 60_000).toISOString(),
            payload: { episodeId },
          },
        ],
        outbox: [
          {
            id: effectId,
            effectKind: "linq.message",
            idempotencyKey: `episode:${episodeId}:opened`,
            payload: { text: "The permission slip is owned and open." },
          },
        ],
        audits: [
          {
            id: randomUUID(),
            actorKind: "adult",
            actorId: adultId,
            action: "episode.opened",
            targetType: "family_episode",
            targetId: episodeId,
            details: { status: "open" },
          },
        ],
      }),
    ).resolves.toBe(1);

    await expect(
      store.commitTransition({
        householdId,
        signalId: signal.signalId,
        expectedHouseholdVersion: 0,
        audits: [],
      }),
    ).rejects.toBeInstanceOf(StaleHouseholdVersionError);

    const state = await database<
      {
        episodes: string;
        triggers: string;
        effects: string;
        audits: string;
      }[]
    >`
      select
        (select count(*)::text from family_episodes where household_id = ${householdId}) as episodes,
        (select count(*)::text from scheduled_triggers where household_id = ${householdId}) as triggers,
        (select count(*)::text from outbox where household_id = ${householdId}) as effects,
        (select count(*)::text from audit_log where household_id = ${householdId}) as audits
    `;
    expect(state[0]).toEqual({ episodes: "1", triggers: "1", effects: "1", audits: "1" });
  });
});
