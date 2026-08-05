import { z } from "zod";
import { type GmailSyncWork, GoogleSyncError } from "./google-sync.js";

export type ClaimedGoogleSyncWork<TWork = GmailSyncWork> = {
  rowId: string;
  leaseToken: string;
  work: TWork;
  attempt: number;
  maxAttempts: number;
};

/**
 * Queue implementations own durable idempotency and lease fencing. Reconciliation
 * must derive stable work keys from connection ID, work kind, and cursor revision.
 */
export interface GoogleSyncQueuePort<TWork = GmailSyncWork> {
  reconcileGoogleSyncWork(asOf: string): Promise<number>;
  claimGoogleSyncWork(input: {
    owner: string;
    limit: number;
    leaseSeconds: number;
  }): Promise<readonly ClaimedGoogleSyncWork<TWork>[]>;
  completeGoogleSyncWork(input: { rowId: string; leaseToken: string }): Promise<boolean>;
  retryGoogleSyncWork(input: {
    rowId: string;
    leaseToken: string;
    retryAt: string;
    errorCode: string;
  }): Promise<boolean>;
  deadLetterGoogleSyncWork(input: { rowId: string; leaseToken: string; errorCode: string }): Promise<boolean>;
}

export type GoogleSyncRunSummary = {
  reconciled: number;
  continuationReconciled: number;
  claimed: number;
  completed: number;
  retried: number;
  deadLettered: number;
  leaseLost: number;
};

export interface GoogleSyncExecutor<TWork> {
  execute(work: TWork, signal?: AbortSignal): Promise<{ status: string }>;
}

export interface GoogleSyncBackgroundHostOptions<TWork = GmailSyncWork> {
  queue: GoogleSyncQueuePort<TWork>;
  sync: GoogleSyncExecutor<TWork>;
  workerId: string;
  batchSize?: number;
  leaseSeconds?: number;
  pollIntervalMs?: number;
  retryBaseMs?: number;
  retryMaximumMs?: number;
  now?: () => Date;
}

const ZERO_SUMMARY: GoogleSyncRunSummary = {
  reconciled: 0,
  continuationReconciled: 0,
  claimed: 0,
  completed: 0,
  retried: 0,
  deadLettered: 0,
  leaseLost: 0,
};

export class GoogleSyncBackgroundHost<TWork = GmailSyncWork> {
  readonly #queue: GoogleSyncQueuePort<TWork>;
  readonly #sync: GoogleSyncExecutor<TWork>;
  readonly #workerId: string;
  readonly #batchSize: number;
  readonly #leaseSeconds: number;
  readonly #pollIntervalMs: number;
  readonly #retryBaseMs: number;
  readonly #retryMaximumMs: number;
  readonly #now: () => Date;

  public constructor(options: GoogleSyncBackgroundHostOptions<TWork>) {
    this.#queue = options.queue;
    this.#sync = options.sync;
    this.#workerId = z.string().trim().min(1).max(200).parse(options.workerId);
    this.#batchSize = z
      .number()
      .int()
      .min(1)
      .max(100)
      .parse(options.batchSize ?? 10);
    this.#leaseSeconds = z
      .number()
      .int()
      .min(1)
      .max(3_600)
      .parse(options.leaseSeconds ?? 300);
    this.#pollIntervalMs = z
      .number()
      .int()
      .min(1)
      .max(60_000)
      .parse(options.pollIntervalMs ?? 1_000);
    this.#retryBaseMs = z
      .number()
      .int()
      .min(1)
      .max(60 * 60_000)
      .parse(options.retryBaseMs ?? 5_000);
    this.#retryMaximumMs = z
      .number()
      .int()
      .min(this.#retryBaseMs)
      .max(24 * 60 * 60_000)
      .parse(options.retryMaximumMs ?? 15 * 60_000);
    this.#now = options.now ?? (() => new Date());
  }

  public async runOnce(signal?: AbortSignal): Promise<GoogleSyncRunSummary> {
    assertNotAborted(signal);
    const summary = { ...ZERO_SUMMARY };
    summary.reconciled = await this.#queue.reconcileGoogleSyncWork(this.#now().toISOString());
    assertNotAborted(signal);
    const leases = await this.#queue.claimGoogleSyncWork({
      owner: this.#workerId,
      limit: this.#batchSize,
      leaseSeconds: this.#leaseSeconds,
    });
    summary.claimed = leases.length;

    let continuationRequired = false;
    for (const lease of leases) {
      const outcome = await this.#processLease(lease, signal);
      summary[outcome.settlement] += 1;
      if (outcome.settlement === "completed" && outcome.continuationRequired) {
        continuationRequired = true;
      }
    }

    if (continuationRequired && !signal?.aborted) {
      summary.continuationReconciled = await this.#queue.reconcileGoogleSyncWork(this.#now().toISOString());
    }
    return summary;
  }

  public async run(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      try {
        await this.runOnce(signal);
      } catch (error) {
        if (signal.aborted || isAbort(error)) return;
        throw error;
      }
      if (signal.aborted) return;
      try {
        await abortableDelay(this.#pollIntervalMs, signal);
      } catch (error) {
        if (signal.aborted || isAbort(error)) return;
        throw error;
      }
    }
  }

  async #processLease(
    lease: ClaimedGoogleSyncWork<TWork>,
    signal?: AbortSignal,
  ): Promise<{
    settlement: "completed" | "retried" | "deadLettered" | "leaseLost";
    continuationRequired: boolean;
  }> {
    if (signal?.aborted) {
      return this.#settleFailure(lease, {
        errorCode: "google_sync.host_shutdown",
        retryable: true,
      });
    }
    try {
      const result = await this.#sync.execute(lease.work, signal);
      const continuationRequired = result.status === "continuation_required";
      const completed = await this.#queue.completeGoogleSyncWork({
        rowId: lease.rowId,
        leaseToken: lease.leaseToken,
      });
      return {
        settlement: completed ? "completed" : "leaseLost",
        continuationRequired,
      };
    } catch (error) {
      return this.#settleFailure(lease, classifyFailure(error, signal));
    }
  }

  async #settleFailure(
    lease: ClaimedGoogleSyncWork<TWork>,
    failure: { errorCode: string; retryable: boolean },
  ): Promise<{
    settlement: "retried" | "deadLettered" | "leaseLost";
    continuationRequired: false;
  }> {
    if (failure.retryable && lease.attempt < lease.maxAttempts) {
      const retryAt = new Date(
        this.#now().getTime() + retryDelayMs(lease.attempt, this.#retryBaseMs, this.#retryMaximumMs),
      ).toISOString();
      const retried = await this.#queue.retryGoogleSyncWork({
        rowId: lease.rowId,
        leaseToken: lease.leaseToken,
        retryAt,
        errorCode: failure.errorCode,
      });
      return {
        settlement: retried ? "retried" : "leaseLost",
        continuationRequired: false,
      };
    }
    const deadLettered = await this.#queue.deadLetterGoogleSyncWork({
      rowId: lease.rowId,
      leaseToken: lease.leaseToken,
      errorCode: failure.retryable ? "google_sync.max_attempts" : failure.errorCode,
    });
    return {
      settlement: deadLettered ? "deadLettered" : "leaseLost",
      continuationRequired: false,
    };
  }
}

function classifyFailure(error: unknown, signal?: AbortSignal): { errorCode: string; retryable: boolean } {
  if (signal?.aborted || isAbort(error)) {
    return { errorCode: "google_sync.host_shutdown", retryable: true };
  }
  if (error instanceof GoogleSyncError) {
    return { errorCode: `google_sync.${error.code}`, retryable: error.retryable };
  }
  return { errorCode: "google_sync.unexpected", retryable: false };
}

export function retryDelayMs(attempt: number, baseMs: number, maximumMs: number): number {
  const safeAttempt = Math.max(1, Math.min(31, Math.trunc(attempt)));
  return Math.min(maximumMs, baseMs * 2 ** (safeAttempt - 1));
}

function assertNotAborted(signal?: AbortSignal): void {
  if (signal?.aborted) throw signal.reason ?? abortError();
}

function abortableDelay(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(signal.reason ?? abortError());
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    const onAbort = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      reject(signal.reason ?? abortError());
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

function abortError(): Error {
  const error = new Error("aborted");
  error.name = "AbortError";
  return error;
}

function isAbort(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}
