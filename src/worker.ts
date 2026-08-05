import { pathToFileURL } from "node:url";
import { type ApplicationWorkerComposition, createApplicationWorkerEntrypoint } from "./application/index.js";
import { createProductionComposition, type FlorenceProductionComposition } from "./composition.js";
import type { FlorenceConfig } from "./config.js";
import { loadConfig } from "./config.js";

/**
 * Deployment code supplies durable queue, repository, connector, runtime, and config adapters.
 * This entry point deliberately reads and prints no credentials.
 */
export async function startWorker(
  composition: ApplicationWorkerComposition,
  signal?: AbortSignal,
): Promise<void> {
  await createApplicationWorkerEntrypoint(composition).run(signal);
}

export interface RunProductionWorkerOptions {
  readonly config?: FlorenceConfig;
  readonly signal?: AbortSignal;
  readonly createComposition?: (options: {
    readonly config: FlorenceConfig;
    readonly migrate: false;
  }) => Promise<FlorenceProductionComposition>;
}

/** Runs application, Google sync, and maintenance loops as a standalone worker service. */
export async function runProductionWorker(options: RunProductionWorkerOptions = {}): Promise<void> {
  const config = options.config ?? loadConfig();
  if (config.FLORENCE_PROCESS_ROLE !== "worker") {
    throw new Error("The Florence worker entrypoint requires the worker process role");
  }
  const composition = await (options.createComposition ?? createProductionComposition)({
    config,
    migrate: false,
  });
  const controller = new AbortController();
  const abort = () => controller.abort(options.signal?.reason);
  if (options.signal?.aborted) controller.abort(options.signal.reason);
  else options.signal?.addEventListener("abort", abort, { once: true });
  try {
    await composition.background.run(controller.signal);
  } finally {
    controller.abort();
    options.signal?.removeEventListener("abort", abort);
    await composition.close();
  }
}

function installShutdownHandlers(controller: AbortController): () => void {
  const shutdown = () => controller.abort();
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
  return () => {
    process.removeListener("SIGINT", shutdown);
    process.removeListener("SIGTERM", shutdown);
  };
}

async function main(): Promise<void> {
  const controller = new AbortController();
  const dispose = installShutdownHandlers(controller);
  try {
    await runProductionWorker({ signal: controller.signal });
  } finally {
    dispose();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  void main().catch(() => {
    process.stderr.write("Florence worker stopped after a fatal startup or runtime error.\n");
    process.exitCode = 1;
  });
}
