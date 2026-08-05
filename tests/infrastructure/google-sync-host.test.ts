import { describe, expect, it, vi } from "vitest";
import {
  type GmailSyncResult,
  type GmailSyncWork,
  GoogleSyncError,
} from "../../src/infrastructure/google-sync.js";
import {
  type ClaimedGoogleSyncWork,
  GoogleSyncBackgroundHost,
  type GoogleSyncQueuePort,
} from "../../src/infrastructure/google-sync-host.js";

const NOW = new Date("2027-01-01T08:00:00.000Z");
const HOUSEHOLD_ID = "11111111-1111-4111-8111-111111111111";
const ADULT_ID = "22222222-2222-4222-8222-222222222222";
const CONNECTION_ID = "33333333-3333-4333-8333-333333333333";

function work(suffix = ""): GmailSyncWork {
  return {
    kind: "continue",
    householdId: `${HOUSEHOLD_ID}${suffix}`,
    adultId: ADULT_ID,
    connectionId: CONNECTION_ID,
  };
}

function lease(
  rowId: string,
  input: { attempt?: number; maxAttempts?: number; work?: GmailSyncWork } = {},
): ClaimedGoogleSyncWork {
  return {
    rowId,
    leaseToken: rowId.replace(/^./u, "9"),
    work: input.work ?? work(),
    attempt: input.attempt ?? 1,
    maxAttempts: input.maxAttempts ?? 8,
  };
}

function result(status: GmailSyncResult["status"] = "processed"): GmailSyncResult {
  return {
    status,
    connectionId: CONNECTION_ID,
    householdId: HOUSEHOLD_ID,
    adultId: ADULT_ID,
    phase: status === "continuation_required" ? "one_year_backfill" : "live",
    processedMessages: 2,
    processedDeletions: 0,
  };
}

function queueHarness(
  input: {
    leases?: ClaimedGoogleSyncWork[];
    reconciliations?: number[];
    complete?: boolean;
    retry?: boolean;
    dead?: boolean;
  } = {},
) {
  const leases = [...(input.leases ?? [])];
  const reconciliations = [...(input.reconciliations ?? [0])];
  const calls = {
    reconcile: [] as string[],
    claim: [] as Array<{ owner: string; limit: number; leaseSeconds: number }>,
    complete: [] as Array<{ rowId: string; leaseToken: string }>,
    retry: [] as Array<{ rowId: string; leaseToken: string; retryAt: string; errorCode: string }>,
    dead: [] as Array<{ rowId: string; leaseToken: string; errorCode: string }>,
  };
  const queue: GoogleSyncQueuePort = {
    async reconcileGoogleSyncWork(asOf) {
      calls.reconcile.push(asOf);
      return reconciliations.shift() ?? 0;
    },
    async claimGoogleSyncWork(claimInput) {
      calls.claim.push(claimInput);
      return leases.splice(0);
    },
    async completeGoogleSyncWork(completeInput) {
      calls.complete.push(completeInput);
      return input.complete ?? true;
    },
    async retryGoogleSyncWork(retryInput) {
      calls.retry.push(retryInput);
      return input.retry ?? true;
    },
    async deadLetterGoogleSyncWork(deadInput) {
      calls.dead.push(deadInput);
      return input.dead ?? true;
    },
  };
  return { queue, calls };
}

function host(
  queue: GoogleSyncQueuePort,
  execute: (work: GmailSyncWork, signal?: AbortSignal) => Promise<GmailSyncResult>,
) {
  return new GoogleSyncBackgroundHost({
    queue,
    sync: { execute },
    workerId: "google-sync-test-worker",
    batchSize: 5,
    leaseSeconds: 120,
    pollIntervalMs: 1,
    retryBaseMs: 1_000,
    retryMaximumMs: 8_000,
    now: () => NOW,
  });
}

describe("GoogleSyncBackgroundHost", () => {
  it("reconciles, leases, completes, and reconciles the next durable continuation", async () => {
    const claimed = lease("11111111-1111-4111-8111-111111111111");
    const queue = queueHarness({ leases: [claimed], reconciliations: [1, 1] });
    const execute = vi.fn(async () => result("continuation_required"));

    await expect(host(queue.queue, execute).runOnce()).resolves.toEqual({
      reconciled: 1,
      continuationReconciled: 1,
      claimed: 1,
      completed: 1,
      retried: 0,
      deadLettered: 0,
      leaseLost: 0,
    });
    expect(queue.calls.claim).toEqual([{ owner: "google-sync-test-worker", limit: 5, leaseSeconds: 120 }]);
    expect(execute).toHaveBeenCalledWith(claimed.work, undefined);
    expect(queue.calls.complete).toEqual([{ rowId: claimed.rowId, leaseToken: claimed.leaseToken }]);
    expect(queue.calls.reconcile).toEqual([NOW.toISOString(), NOW.toISOString()]);
  });

  it("retries only retryable GoogleSyncError failures with deterministic bounded backoff", async () => {
    const claimed = lease("22222222-2222-4222-8222-222222222222", { attempt: 3 });
    const queue = queueHarness({ leases: [claimed] });
    const execute = vi.fn(async () => {
      throw new GoogleSyncError("provider response contained private data", "provider_failure", true);
    });

    await expect(host(queue.queue, execute).runOnce()).resolves.toMatchObject({ retried: 1 });
    expect(queue.calls.retry).toEqual([
      {
        rowId: claimed.rowId,
        leaseToken: claimed.leaseToken,
        retryAt: "2027-01-01T08:00:04.000Z",
        errorCode: "google_sync.provider_failure",
      },
    ]);
    expect(JSON.stringify(queue.calls)).not.toContain("private data");
    expect(queue.calls.dead).toHaveLength(0);
  });

  it("dead-letters permanent failures and retryable failures at their attempt limit", async () => {
    const permanent = lease("33333333-3333-4333-8333-333333333333");
    const exhausted = lease("44444444-4444-4444-8444-444444444444", {
      attempt: 4,
      maxAttempts: 4,
    });
    const queue = queueHarness({ leases: [permanent, exhausted] });
    let invocation = 0;
    const execute = vi.fn(async () => {
      invocation += 1;
      if (invocation === 1) {
        throw new GoogleSyncError("bad account state", "invalid_state", false);
      }
      throw new GoogleSyncError("temporary provider issue", "provider_failure", true);
    });

    await expect(host(queue.queue, execute).runOnce()).resolves.toMatchObject({
      deadLettered: 2,
      retried: 0,
    });
    expect(queue.calls.dead).toEqual([
      {
        rowId: permanent.rowId,
        leaseToken: permanent.leaseToken,
        errorCode: "google_sync.invalid_state",
      },
      {
        rowId: exhausted.rowId,
        leaseToken: exhausted.leaseToken,
        errorCode: "google_sync.max_attempts",
      },
    ]);
  });

  it("records lease fencing loss instead of claiming completion", async () => {
    const queue = queueHarness({
      leases: [lease("55555555-5555-4555-8555-555555555555")],
      complete: false,
    });

    await expect(host(queue.queue, async () => result()).runOnce()).resolves.toMatchObject({
      completed: 0,
      leaseLost: 1,
    });
  });

  it("stops the run loop on abort and releases every claimed but unfinished lease for retry", async () => {
    const controller = new AbortController();
    const first = lease("66666666-6666-4666-8666-666666666666");
    const second = lease("77777777-7777-4777-8777-777777777777");
    const queue = queueHarness({ leases: [first, second] });
    const execute = vi.fn(async () => {
      controller.abort();
      throw new GoogleSyncError("cancelled by shutdown", "cancelled", true);
    });

    await expect(host(queue.queue, execute).run(controller.signal)).resolves.toBeUndefined();
    expect(execute).toHaveBeenCalledOnce();
    expect(queue.calls.retry).toEqual([
      {
        rowId: first.rowId,
        leaseToken: first.leaseToken,
        retryAt: "2027-01-01T08:00:01.000Z",
        errorCode: "google_sync.host_shutdown",
      },
      {
        rowId: second.rowId,
        leaseToken: second.leaseToken,
        retryAt: "2027-01-01T08:00:01.000Z",
        errorCode: "google_sync.host_shutdown",
      },
    ]);
  });
});
