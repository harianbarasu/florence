import { setTimeout as sleep } from "node:timers/promises";

export interface WorkerHeartbeatStore {
  renewWorkerHeartbeat(name: string): Promise<void>;
}

/** Publishes durable worker liveness for web processes in a split deployment. */
export class WorkerHeartbeatHost {
  readonly #store: WorkerHeartbeatStore;
  readonly #name: string;
  readonly #intervalMs: number;

  public constructor(options: { store: WorkerHeartbeatStore; name?: string; intervalMs?: number }) {
    this.#store = options.store;
    this.#name = options.name ?? "durable-worker";
    this.#intervalMs = options.intervalMs ?? 10_000;
  }

  public async run(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      await this.#store.renewWorkerHeartbeat(this.#name);
      if (signal.aborted) return;
      try {
        await sleep(this.#intervalMs, undefined, { signal });
      } catch {
        if (signal.aborted) return;
        throw new Error("worker_heartbeat_unavailable");
      }
    }
  }
}
