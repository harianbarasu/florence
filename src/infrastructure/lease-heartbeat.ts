import { setTimeout as sleep } from "node:timers/promises";

export interface LeaseHeartbeat {
  readonly signal: AbortSignal;
  readonly owned: boolean;
  stop(): Promise<boolean>;
}

/**
 * Renews a durable lease immediately and then every third of its lifetime.
 * Any rejected or unsuccessful renewal is ownership loss and aborts supported work.
 */
export async function startLeaseHeartbeat(options: {
  readonly leaseSeconds: number;
  readonly renew: () => Promise<boolean>;
  readonly upstreamSignal?: AbortSignal;
}): Promise<LeaseHeartbeat> {
  const leaseLost = new AbortController();
  const stopped = new AbortController();
  const workSignal = options.upstreamSignal
    ? AbortSignal.any([options.upstreamSignal, leaseLost.signal])
    : leaseLost.signal;
  const heartbeatSignal = options.upstreamSignal
    ? AbortSignal.any([options.upstreamSignal, stopped.signal])
    : stopped.signal;
  const intervalMs = Math.max(250, Math.floor((options.leaseSeconds * 1_000) / 3));
  let owned = await renewOrLose(options.renew, leaseLost);

  const running = (async () => {
    while (owned && !heartbeatSignal.aborted) {
      try {
        await sleep(intervalMs, undefined, { signal: heartbeatSignal });
      } catch (error) {
        if (heartbeatSignal.aborted || isAbort(error)) return;
        owned = false;
        leaseLost.abort(leaseLostError());
        return;
      }
      if (heartbeatSignal.aborted) return;
      owned = await renewOrLose(options.renew, leaseLost);
    }
  })();

  return {
    signal: workSignal,
    get owned() {
      return owned;
    },
    async stop() {
      stopped.abort();
      await running;
      return owned;
    },
  };
}

async function renewOrLose(renew: () => Promise<boolean>, controller: AbortController): Promise<boolean> {
  try {
    if (await renew()) return true;
  } catch {
    // A worker cannot distinguish a failed renewal call from ownership loss safely.
  }
  controller.abort(leaseLostError());
  return false;
}

function leaseLostError(): Error {
  const error = new Error("durable lease lost");
  error.name = "AbortError";
  return error;
}

function isAbort(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}
