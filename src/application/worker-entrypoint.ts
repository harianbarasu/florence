import { createFlorenceApplication } from "./coordinator.js";
import type { FlorenceApplication, FlorenceApplicationDependencies } from "./ports.js";

/** Durable queue/timer implementations inject their loop here; the app owns all decisions. */
export interface ApplicationWorkerHost {
  run(application: FlorenceApplication, signal?: AbortSignal): Promise<void>;
}

export interface ApplicationWorkerComposition {
  readonly dependencies: FlorenceApplicationDependencies;
  readonly host: ApplicationWorkerHost;
}

export interface ApplicationWorkerEntrypoint {
  run(signal?: AbortSignal): Promise<void>;
}

export function createApplicationWorkerEntrypoint(
  composition: ApplicationWorkerComposition,
): ApplicationWorkerEntrypoint {
  const application = createFlorenceApplication(composition.dependencies);
  return Object.freeze({
    async run(signal?: AbortSignal) {
      await composition.host.run(application, signal);
    },
  });
}
