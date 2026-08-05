import type { WorkerJob } from "../runtime/index.js";
import type { ApplicationProjection } from "./contracts.js";

type ActiveAttempt = {
  readonly householdId: string;
  readonly attemptId: string;
  readonly controller: AbortController;
};

export type ActiveWorkerAttempt = {
  readonly signal: AbortSignal;
  finish(): void;
};

export type ActiveWorkerAttemptOptions = {
  readonly upstream?: AbortSignal;
  readonly isStillQueued?: () => Promise<boolean>;
  readonly pollIntervalMs?: number;
};

/**
 * Bridges a durable worker status change to an in-process abort signal. Durable
 * projection state remains authoritative; this registry only shortens the time
 * an obsolete model or tool call can continue consuming resources.
 */
export class ActiveWorkerAttempts {
  readonly #attempts = new Map<string, ActiveAttempt>();

  begin(job: WorkerJob, options: ActiveWorkerAttemptOptions = {}): ActiveWorkerAttempt {
    const key = attemptKey(job.householdId, job.attemptId);
    if (this.#attempts.has(key)) {
      throw new Error(`Worker attempt is already active: ${job.attemptId}`);
    }
    const controller = new AbortController();
    this.#attempts.set(key, {
      householdId: job.householdId,
      attemptId: job.attemptId,
      controller,
    });
    const signal =
      options.upstream === undefined
        ? controller.signal
        : AbortSignal.any([options.upstream, controller.signal]);
    let finished = false;
    let pollTimer: ReturnType<typeof setTimeout> | undefined;
    const pollIntervalMs = Math.max(250, options.pollIntervalMs ?? 1_000);
    const pollDurableStatus = async () => {
      if (finished || controller.signal.aborted || options.isStillQueued === undefined) return;
      try {
        if (!(await options.isStillQueued())) {
          controller.abort(new Error(`Worker attempt is no longer queued: ${job.attemptId}`));
          return;
        }
      } catch {
        // A transient read failure cannot safely prove cancellation. The attempt
        // deadline remains the outer bound and the next poll rechecks durable state.
      }
      if (finished || controller.signal.aborted) return;
      pollTimer = setTimeout(pollDurableStatus, pollIntervalMs);
      pollTimer.unref();
    };
    if (options.isStillQueued !== undefined) {
      pollTimer = setTimeout(pollDurableStatus, pollIntervalMs);
      pollTimer.unref();
    }
    return Object.freeze({
      signal,
      finish: () => {
        if (finished) return;
        finished = true;
        if (pollTimer !== undefined) clearTimeout(pollTimer);
        const current = this.#attempts.get(key);
        if (current?.controller === controller) this.#attempts.delete(key);
      },
    });
  }

  reconcile(householdId: string, workers: ApplicationProjection["workers"]): void {
    const queued = new Set(
      workers
        .filter((worker) => worker.status === "queued")
        .map((worker) => attemptKey(householdId, worker.job.attemptId)),
    );
    for (const [key, attempt] of this.#attempts) {
      if (attempt.householdId === householdId && !queued.has(key)) {
        attempt.controller.abort(new Error(`Worker attempt is no longer queued: ${attempt.attemptId}`));
      }
    }
  }
}

function attemptKey(householdId: string, attemptId: string): string {
  return `${householdId}\0${attemptId}`;
}
