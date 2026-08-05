import { describe, expect, it, vi } from "vitest";
import type { FlorenceApplication } from "../../src/application/ports.js";
import { HouseholdIdSchema } from "../../src/domain/index.js";
import {
  DurableWorkerHost,
  type OutboxLease,
  type ProviderInboxLease,
  type ProviderItemProcessor,
  type QueueStore,
  type TimerLease,
} from "../../src/infrastructure/worker-host.js";

const HOUSEHOLD_ID = HouseholdIdSchema.parse("00000000-0000-4000-8000-000000000001");
const NOW = "2027-01-05T12:00:00.000Z";
const LEASE_EXPIRES = "2027-01-05T12:05:00.000Z";

function providerLease(index: number, attempt = 1): ProviderInboxLease {
  return {
    id: `00000000-0000-4000-8000-${String(100 + index).padStart(12, "0")}`,
    provider: "linq",
    idempotencyKey: `provider:${index}`,
    payloadHash: `sha256:${String(index).padStart(64, "0")}`,
    authentication: { verified: true },
    eventKind: "message.received",
    occurredAt: NOW,
    payload: { message: { id: `message-${index}` } },
    attempt,
    maxAttempts: 8,
    leaseToken: `00000000-0000-4000-8001-${String(100 + index).padStart(12, "0")}`,
    leaseExpiresAt: LEASE_EXPIRES,
  };
}

function timerLease(index: number, attempt = 1): TimerLease {
  const timerId = `timer_${index}`;
  const episodeId = `episode_${index}`;
  return {
    rowId: `00000000-0000-4000-8002-${String(100 + index).padStart(12, "0")}`,
    timerKey: timerId,
    householdId: HOUSEHOLD_ID,
    episodeKey: episodeId,
    triggerKind: "domain.timer",
    planVersion: 1,
    dueAt: "2027-01-05T11:59:00Z",
    payload: {
      timerId,
      episodeId,
      temporalPlanVersion: 1,
      triggerId: `trigger_${index}`,
    },
    attempt,
    leaseToken: `00000000-0000-4000-8003-${String(100 + index).padStart(12, "0")}`,
    leaseExpiresAt: LEASE_EXPIRES,
  };
}

function outboxLease(index: number, attempt = 1, body = `Status ${index}`): OutboxLease {
  const intentId = `app_outbox_${index}`;
  const idempotencyKey = `outbox:${index}`;
  return {
    rowId: `00000000-0000-4000-8004-${String(100 + index).padStart(12, "0")}`,
    intentKey: intentId,
    householdId: HOUSEHOLD_ID,
    effectKind: "conversation.send",
    idempotencyKey,
    payload: {
      intentId,
      householdId: HOUSEHOLD_ID,
      idempotencyKey,
      kind: "conversation.send",
      targetScope: { kind: "household" },
      messageClass: "status",
      body,
    },
    attempt,
    maxAttempts: 8,
    leaseToken: `00000000-0000-4000-8005-${String(100 + index).padStart(12, "0")}`,
    leaseExpiresAt: LEASE_EXPIRES,
  };
}

function applicationResult(input: { householdId: string; idempotencyKey: string }) {
  return {
    householdId: HouseholdIdSchema.parse(input.householdId),
    idempotencyKey: input.idempotencyKey,
    disposition: "committed" as const,
    revision: 1,
    outcome: {
      status: "processed" as const,
      classification: "fixture",
      domainReceipts: [],
      outboxIntentIds: [],
    },
  };
}

function fakeApplication(overrides: Partial<FlorenceApplication> = {}): FlorenceApplication {
  return {
    process: vi.fn(async (rawInput: unknown) => {
      const input = rawInput as { householdId: string; idempotencyKey: string };
      return applicationResult(input);
    }),
    executeOutbox: vi.fn(async (rawIntent: unknown) => {
      const intent = rawIntent as { intentId: string };
      return { intentId: intent.intentId, status: "succeeded" as const };
    }),
    ...overrides,
  };
}

function fakeQueue(overrides: Partial<QueueStore> = {}): QueueStore {
  return {
    claimProviderInbox: vi.fn(async () => []),
    resolveProviderInbox: vi.fn(async () => true),
    failProviderInbox: vi.fn(async () => "pending" as const),
    claimDueTimers: vi.fn(async () => []),
    finishTimer: vi.fn(async () => true),
    releaseTimer: vi.fn(async () => true),
    claimOutbox: vi.fn(async () => []),
    recordOutboxSuccess: vi.fn(async () => true),
    recordOutboxFailure: vi.fn(async () => "retry" as const),
    recordOutboxPermanent: vi.fn(async () => true),
    ...overrides,
  };
}

function host(
  queueStore: QueueStore,
  providerProcessor: ProviderItemProcessor,
  overrides: Partial<ConstructorParameters<typeof DurableWorkerHost>[0]> = {},
) {
  return new DurableWorkerHost({
    queueStore,
    providerProcessor,
    ownerId: "worker-test",
    now: () => new Date(NOW),
    random: () => 0.5,
    jitterRatio: 0.2,
    ...overrides,
  });
}

describe("DurableWorkerHost", () => {
  it("fairly interleaves all durable queues and completes successful leases", async () => {
    const order: string[] = [];
    const providers = [providerLease(1), providerLease(2)];
    const timers = [timerLease(1), timerLease(2)];
    const outbox = [outboxLease(1), outboxLease(2)];
    const queue = fakeQueue({
      claimProviderInbox: vi.fn(async () => providers),
      claimDueTimers: vi.fn(async () => timers),
      claimOutbox: vi.fn(async () => outbox),
    });
    const processor: ProviderItemProcessor = {
      process: vi.fn(async (item) => {
        order.push(`provider:${item.id}`);
        return { status: "resolved", householdId: HOUSEHOLD_ID, resolution: { status: "processed" } };
      }),
    };
    const application = fakeApplication({
      process: vi.fn(async (rawInput: unknown) => {
        const input = rawInput as { timerId: string; householdId: string; idempotencyKey: string };
        order.push(`timer:${input.timerId}`);
        return applicationResult(input);
      }),
      executeOutbox: vi.fn(async (rawIntent: unknown) => {
        const intent = rawIntent as { intentId: string };
        order.push(`outbox:${intent.intentId}`);
        return { intentId: intent.intentId, status: "succeeded" as const };
      }),
    });

    const report = await host(queue, processor, { concurrency: 1, batchSize: 2 }).runOnce(application);

    expect(order).toEqual([
      `provider:${providers[0]?.id}`,
      "timer:timer_1",
      "outbox:app_outbox_1",
      `provider:${providers[1]?.id}`,
      "timer:timer_2",
      "outbox:app_outbox_2",
    ]);
    expect(report).toEqual({
      providerInbox: { claimed: 2, resolved: 2, retried: 0, discarded: 0, lostLease: 0, failed: 0 },
      timers: { claimed: 2, fired: 2, released: 0, superseded: 0, lostLease: 0, failed: 0 },
      outbox: {
        claimed: 2,
        succeeded: 2,
        retried: 0,
        permanent: 0,
        ambiguous: 0,
        lostLease: 0,
        failed: 0,
      },
      claimFailures: 0,
    });
    expect(queue.claimProviderInbox).toHaveBeenCalledWith({
      owner: "worker-test",
      limit: 2,
      leaseSeconds: 120,
    });
    expect(queue.resolveProviderInbox).toHaveBeenCalledTimes(2);
    expect(queue.finishTimer).toHaveBeenCalledTimes(2);
    expect(queue.recordOutboxSuccess).toHaveBeenCalledTimes(2);
  });

  it("uses deterministic exponential backoff and distinguishes retry, permanent, and ambiguity", async () => {
    const retry = outboxLease(1, 3);
    const permanent = outboxLease(2, 3);
    const ambiguous = outboxLease(3, 3);
    const malformed = { ...outboxLease(4, 3), payload: { privateContent: "must-not-execute" } };
    const queue = fakeQueue({
      claimProviderInbox: vi.fn(async () => [providerLease(1, 3)]),
      claimDueTimers: vi.fn(async () => [timerLease(1, 3)]),
      claimOutbox: vi.fn(async () => [retry, permanent, ambiguous, malformed]),
      recordOutboxFailure: vi.fn(async (input) =>
        input.outcomeCertain ? ("retry" as const) : ("ambiguous" as const),
      ),
    });
    const processor: ProviderItemProcessor = {
      process: vi.fn(async () => ({ status: "retryable_failure", errorCode: "provider_busy" })),
    };
    const application = fakeApplication({
      process: vi.fn(async () => {
        throw new Error("private timer detail");
      }),
      executeOutbox: vi.fn(async (rawIntent: unknown) => {
        const intent = rawIntent as { intentId: string };
        if (intent.intentId === retry.intentKey) {
          return { intentId: intent.intentId, status: "retryable_failure" as const };
        }
        if (intent.intentId === permanent.intentKey) {
          return { intentId: intent.intentId, status: "permanent_failure" as const };
        }
        throw new Error("provider may have accepted private content");
      }),
    });

    const report = await host(queue, processor, {
      concurrency: 1,
      baseRetrySeconds: 2,
      maxRetrySeconds: 60,
    }).runOnce(application);

    expect(queue.failProviderInbox).toHaveBeenCalledWith({
      inboxId: providerLease(1, 3).id,
      leaseToken: providerLease(1, 3).leaseToken,
      errorCode: "provider_busy",
      retryAfterSeconds: 8,
    });
    expect(queue.releaseTimer).toHaveBeenCalledWith({
      rowId: timerLease(1, 3).rowId,
      leaseToken: timerLease(1, 3).leaseToken,
      retryAt: "2027-01-05T12:00:08.000Z",
      errorCode: "timer_processing_failure",
    });
    expect(queue.recordOutboxFailure).toHaveBeenNthCalledWith(1, {
      rowId: retry.rowId,
      leaseToken: retry.leaseToken,
      errorCode: "outbox_retryable_failure",
      retryAfterSeconds: 8,
      outcomeCertain: true,
    });
    expect(queue.recordOutboxFailure).toHaveBeenNthCalledWith(2, {
      rowId: ambiguous.rowId,
      leaseToken: ambiguous.leaseToken,
      errorCode: "outbox_execution_failure",
      retryAfterSeconds: 8,
      outcomeCertain: false,
    });
    expect(queue.recordOutboxPermanent).toHaveBeenCalledWith({
      rowId: permanent.rowId,
      leaseToken: permanent.leaseToken,
      errorCode: "outbox_permanent_failure",
    });
    expect(queue.recordOutboxPermanent).toHaveBeenCalledWith({
      rowId: malformed.rowId,
      leaseToken: malformed.leaseToken,
      errorCode: "invalid_outbox_payload",
    });
    expect(report.providerInbox.retried).toBe(1);
    expect(report.timers.released).toBe(1);
    expect(report.outbox).toMatchObject({ retried: 1, permanent: 2, ambiguous: 1, failed: 0 });
  });

  it("discards known permanent provider failures and supersedes invalid timer payloads", async () => {
    const invalidTimer = {
      ...timerLease(1),
      payload: { ...timerLease(1).payload, temporalPlanVersion: 2 },
    };
    const queue = fakeQueue({
      claimProviderInbox: vi.fn(async () => [providerLease(1)]),
      claimDueTimers: vi.fn(async () => [invalidTimer]),
    });
    const processor: ProviderItemProcessor = {
      process: vi.fn(async () => ({
        status: "permanent_failure",
        householdId: HOUSEHOLD_ID,
        errorCode: "unsupported_event",
      })),
    };
    const application = fakeApplication();

    const report = await host(queue, processor).runOnce(application);

    expect(queue.resolveProviderInbox).toHaveBeenCalledWith({
      inboxId: providerLease(1).id,
      leaseToken: providerLease(1).leaseToken,
      householdId: HOUSEHOLD_ID,
      resolution: { status: "discarded", errorCode: "unsupported_event" },
    });
    expect(queue.finishTimer).toHaveBeenCalledWith({
      rowId: invalidTimer.rowId,
      leaseToken: invalidTimer.leaseToken,
      outcome: "superseded",
    });
    expect(application.process).not.toHaveBeenCalled();
    expect(report.providerInbox.discarded).toBe(1);
    expect(report.timers.superseded).toBe(1);
  });

  it("bounds concurrency and prevents overlapping cycles", async () => {
    const items = Array.from({ length: 5 }, (_, index) => providerLease(index + 1));
    const queue = fakeQueue({ claimProviderInbox: vi.fn(async () => items) });
    let active = 0;
    let maximumActive = 0;
    const releases: (() => void)[] = [];
    const processor: ProviderItemProcessor = {
      process: vi.fn(async () => {
        active += 1;
        maximumActive = Math.max(maximumActive, active);
        await new Promise<void>((resolve) => releases.push(resolve));
        active -= 1;
        return { status: "resolved", resolution: { status: "processed" } };
      }),
    };
    const worker = host(queue, processor, { concurrency: 2 });
    const application = fakeApplication();

    const first = worker.runOnce(application);
    const overlapping = worker.runOnce(application);
    expect(overlapping).toBe(first);
    await vi.waitFor(() => expect(releases).toHaveLength(2));
    expect(processor.process).toHaveBeenCalledTimes(2);
    releases.splice(0).forEach((release) => {
      release();
    });
    await vi.waitFor(() => expect(releases).toHaveLength(2));
    releases.splice(0).forEach((release) => {
      release();
    });
    await vi.waitFor(() => expect(releases).toHaveLength(1));
    releases.splice(0).forEach((release) => {
      release();
    });
    const report = await first;

    expect(maximumActive).toBe(2);
    expect(queue.claimProviderInbox).toHaveBeenCalledTimes(1);
    expect(report.providerInbox.resolved).toBe(5);
  });

  it("isolates claim and item settlement failures and stops an idle loop on abort", async () => {
    const providers = [providerLease(1), providerLease(2)];
    const queue = fakeQueue({
      claimProviderInbox: vi.fn(async () => providers),
      claimDueTimers: vi.fn(async () => {
        throw new Error("database unavailable");
      }),
      resolveProviderInbox: vi.fn(async ({ inboxId }) => {
        if (inboxId === providers[0]?.id) throw new Error("lease settlement unavailable");
        return true;
      }),
    });
    const processor: ProviderItemProcessor = {
      process: vi.fn(async () => ({ status: "resolved", resolution: { status: "processed" } })),
    };
    const worker = host(queue, processor, { concurrency: 1, pollIntervalMs: 60_000 });
    const application = fakeApplication();

    const report = await worker.runOnce(application);
    expect(report.claimFailures).toBe(1);
    expect(report.providerInbox).toMatchObject({ claimed: 2, resolved: 1, failed: 1 });
    expect(processor.process).toHaveBeenCalledTimes(2);

    const emptyQueue = fakeQueue();
    const idleWorker = host(emptyQueue, processor, { pollIntervalMs: 60_000 });
    const controller = new AbortController();
    const running = idleWorker.run(application, controller.signal);
    await vi.waitFor(() => expect(emptyQueue.claimProviderInbox).toHaveBeenCalledTimes(1));
    const started = Date.now();
    controller.abort();
    await running;
    expect(Date.now() - started).toBeLessThan(250);
  });

  it("releases already-claimed work without invoking processors when aborted", async () => {
    const provider = providerLease(1);
    const timer = timerLease(1);
    const outbox = outboxLease(1);
    const controller = new AbortController();
    const queue = fakeQueue({
      claimProviderInbox: vi.fn(async () => {
        controller.abort();
        return [provider];
      }),
      claimDueTimers: vi.fn(async () => [timer]),
      claimOutbox: vi.fn(async () => [outbox]),
    });
    const processor: ProviderItemProcessor = { process: vi.fn(async () => undefined) };
    const application = fakeApplication();

    const report = await host(queue, processor, { concurrency: 1 }).runOnce(application, controller.signal);

    expect(processor.process).not.toHaveBeenCalled();
    expect(application.process).not.toHaveBeenCalled();
    expect(application.executeOutbox).not.toHaveBeenCalled();
    expect(queue.failProviderInbox).toHaveBeenCalledWith(
      expect.objectContaining({ errorCode: "worker_aborted" }),
    );
    expect(queue.releaseTimer).toHaveBeenCalledWith(expect.objectContaining({ errorCode: "worker_aborted" }));
    expect(queue.recordOutboxFailure).toHaveBeenCalledWith(
      expect.objectContaining({ errorCode: "worker_aborted", outcomeCertain: true }),
    );
    expect(report.providerInbox.retried).toBe(1);
    expect(report.timers.released).toBe(1);
    expect(report.outbox.retried).toBe(1);
  });
});
