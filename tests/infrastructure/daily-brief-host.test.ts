import { describe, expect, it, vi } from "vitest";
import type { FlorenceApplication } from "../../src/application/ports.js";
import {
  DailyBriefHost,
  type DailyBriefLease,
  DailyBriefLeaseSchema,
  type DailyBriefQueuePort,
  resolveDailyBriefSchedule,
} from "../../src/infrastructure/daily-brief-host.js";

const NOW = new Date("2027-01-05T16:00:00.000Z");
const HOUSEHOLD_ID = "00000000-0000-4000-8000-000000000001";

function lease(index: number, overrides: Partial<DailyBriefLease> = {}): DailyBriefLease {
  const suffix = String(index).padStart(12, "0");
  return DailyBriefLeaseSchema.parse({
    rowId: `00000000-0000-4001-8000-${suffix}`,
    householdId: HOUSEHOLD_ID,
    localDate: "2027-01-05",
    timeZone: "America/Los_Angeles",
    scheduledFor: "2027-01-05T15:00:00Z",
    expiresAt: "2027-01-06T15:00:00Z",
    idempotencyKey: `daily-brief:scheduled:${HOUSEHOLD_ID}:2027-01-05:${index}`,
    attempt: 1,
    maxAttempts: 5,
    leaseToken: `00000000-0000-4002-8000-${suffix}`,
    leaseExpiresAt: "2027-01-05T16:02:00Z",
    ...overrides,
  });
}

function fakeQueue(overrides: Partial<DailyBriefQueuePort> = {}): DailyBriefQueuePort {
  return {
    listEligibleHouseholds: vi.fn(async () => []),
    enqueue: vi.fn(async () => false),
    expire: vi.fn(async () => 0),
    claim: vi.fn(async () => []),
    complete: vi.fn(async () => true),
    fail: vi.fn(async () => "retry" as const),
    ...overrides,
  };
}

function host(
  queue: DailyBriefQueuePort,
  overrides: Partial<ConstructorParameters<typeof DailyBriefHost>[0]> = {},
) {
  return new DailyBriefHost({
    queue,
    localTime: "07:00",
    ownerId: "daily-brief-test",
    now: () => NOW,
    ...overrides,
  });
}

describe("resolveDailyBriefSchedule", () => {
  it("resolves nonexistent and repeated wall times with Temporal-compatible DST semantics", () => {
    const spring = resolveDailyBriefSchedule({
      asOf: "2027-03-14T10:31:00Z",
      timeZone: "America/Los_Angeles",
      localTime: "02:30",
    });
    expect(spring).toEqual({
      localDate: "2027-03-14",
      timeZone: "America/Los_Angeles",
      localTime: "02:30",
      scheduledFor: "2027-03-14T10:30:00Z",
      expiresAt: "2027-03-15T09:30:00Z",
      due: true,
    });

    const fall = resolveDailyBriefSchedule({
      asOf: "2027-11-07T08:31:00Z",
      timeZone: "America/Los_Angeles",
      localTime: "01:30",
    });
    expect(fall).toMatchObject({
      localDate: "2027-11-07",
      scheduledFor: "2027-11-07T08:30:00Z",
      expiresAt: "2027-11-08T09:30:00Z",
      due: true,
    });
  });

  it("marks the current local-day run not due before its wall-clock time", () => {
    expect(
      resolveDailyBriefSchedule({
        asOf: "2027-03-14T09:59:00Z",
        timeZone: "America/Los_Angeles",
        localTime: "02:30",
      }).due,
    ).toBe(false);
  });
});

describe("DailyBriefHost", () => {
  it("reconciles one stable local-day key and isolates invalid household time zones", async () => {
    const keys = new Set<string>();
    const enqueue = vi.fn(async (input: Parameters<DailyBriefQueuePort["enqueue"]>[0]) => {
      if (keys.has(input.idempotencyKey)) return false;
      keys.add(input.idempotencyKey);
      return true;
    });
    const queue = fakeQueue({
      listEligibleHouseholds: vi.fn(async () => [
        { householdId: HOUSEHOLD_ID, timeZone: "America/Los_Angeles" },
        { householdId: "00000000-0000-4000-8000-000000000002", timeZone: "Not/A_Zone" },
      ]),
      enqueue,
    });
    const scheduler = host(queue);
    const application = { process: vi.fn(async () => ({ outcome: { status: "processed" as const } })) };

    const first = await scheduler.runOnce(application);
    const second = await scheduler.runOnce(application);

    expect(first).toMatchObject({ eligible: 2, created: 1, invalidSchedules: 1, claimed: 0 });
    expect(second).toMatchObject({ eligible: 2, created: 0, invalidSchedules: 1, claimed: 0 });
    expect(enqueue).toHaveBeenCalledTimes(2);
    expect(enqueue).toHaveBeenNthCalledWith(1, {
      householdId: HOUSEHOLD_ID,
      localDate: "2027-01-05",
      timeZone: "America/Los_Angeles",
      scheduledFor: "2027-01-05T15:00:00Z",
      expiresAt: "2027-01-06T15:00:00Z",
      idempotencyKey: `daily-brief:scheduled:${HOUSEHOLD_ID}:2027-01-05`,
      maxAttempts: 5,
    });
  });

  it("invokes the application with the durable scheduled identity before fencing completion", async () => {
    const claimed = lease(1);
    const complete = vi.fn(async () => true);
    const queue = fakeQueue({ claim: vi.fn(async () => [claimed]), complete });
    const process = vi.fn(async () => ({ outcome: { status: "processed" as const } }));

    const report = await host(queue).runOnce({ process });

    expect(process).toHaveBeenCalledWith({
      kind: "daily_brief",
      householdId: claimed.householdId,
      idempotencyKey: claimed.idempotencyKey,
      occurredAt: claimed.scheduledFor,
      reason: "scheduled",
    });
    expect(complete).toHaveBeenCalledWith({
      rowId: claimed.rowId,
      leaseToken: claimed.leaseToken,
      completedAt: "2027-01-05T16:00:00Z",
    });
    expect(report).toMatchObject({ claimed: 1, succeeded: 1, lostLease: 0 });
  });

  it("uses bounded deterministic retry and permanently rejects application refusals", async () => {
    const retry = lease(1, { attempt: 3 });
    const rejected = lease(2);
    const fail = vi.fn(async (input: Parameters<DailyBriefQueuePort["fail"]>[0]) =>
      input.permanent ? ("dead" as const) : ("retry" as const),
    );
    const queue = fakeQueue({ claim: vi.fn(async () => [retry, rejected]), fail });
    const process = vi.fn(async (input: unknown) => {
      if ((input as { idempotencyKey: string }).idempotencyKey === retry.idempotencyKey) {
        throw new Error("private provider detail must not reach durable state");
      }
      return { outcome: { status: "rejected" as const } };
    });

    const report = await host(queue, { baseRetrySeconds: 2, maxRetrySeconds: 60 }).runOnce({ process });

    expect(fail).toHaveBeenNthCalledWith(1, {
      rowId: retry.rowId,
      leaseToken: retry.leaseToken,
      errorCode: "daily_brief_processing_failure",
      failedAt: "2027-01-05T16:00:00Z",
      retryAt: "2027-01-05T16:00:08Z",
      permanent: false,
    });
    expect(fail).toHaveBeenNthCalledWith(2, {
      rowId: rejected.rowId,
      leaseToken: rejected.leaseToken,
      errorCode: "daily_brief_rejected",
      failedAt: "2027-01-05T16:00:00Z",
      retryAt: "2027-01-05T16:00:02Z",
      permanent: true,
    });
    expect(JSON.stringify(fail.mock.calls)).not.toContain("private provider detail");
    expect(report).toMatchObject({ claimed: 2, retried: 1, dead: 1, failed: 0 });
  });

  it("never invokes a lease at or beyond its next local-day boundary", async () => {
    const expired = lease(1, { expiresAt: "2027-01-05T16:00:00Z" });
    const fail = vi.fn(async () => "dead" as const);
    const process = vi.fn(async () => ({ outcome: { status: "processed" as const } }));

    const report = await host(fakeQueue({ claim: vi.fn(async () => [expired]), fail })).runOnce({ process });

    expect(process).not.toHaveBeenCalled();
    expect(fail).toHaveBeenCalledWith(
      expect.objectContaining({ errorCode: "daily_brief_window_expired", permanent: true }),
    );
    expect(report).toMatchObject({ claimed: 1, dead: 1, succeeded: 0 });
  });

  it("does not claim new work after shutdown and exits its polling loop after abort", async () => {
    const claim = vi.fn(async () => []);
    const queue = fakeQueue({ claim });
    const alreadyStopped = new AbortController();
    alreadyStopped.abort();
    const scheduler = host(queue);
    const narrowApplication = {
      process: vi.fn(async () => ({ outcome: { status: "processed" as const } })),
    };

    await scheduler.runOnce(narrowApplication, alreadyStopped.signal);
    expect(claim).not.toHaveBeenCalled();

    const controller = new AbortController();
    const wait = vi.fn(async () => {
      controller.abort();
    });
    const application: FlorenceApplication = {
      process: vi.fn(async () => {
        throw new Error("unexpected process call");
      }),
      executeOutbox: vi.fn(async () => {
        throw new Error("unexpected outbox call");
      }),
    };
    await host(fakeQueue(), { pollIntervalMs: 250, wait }).run(application, controller.signal);
    expect(wait).toHaveBeenCalledOnce();
  });
});
