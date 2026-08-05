import { type ApplicationWorkerComposition, createApplicationWorkerEntrypoint } from "./application/index.js";

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
