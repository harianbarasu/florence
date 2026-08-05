import { pathToFileURL } from "node:url";
import type { FastifyInstance } from "fastify";
import { createProductionComposition, type FlorenceProductionComposition } from "./composition.js";
import type { FlorenceConfig } from "./config.js";
import { loadConfig } from "./config.js";
import { type CreateFlorenceHttpServerOptions, createFlorenceHttpServer } from "./http/index.js";

export interface StartFlorenceServerOptions extends CreateFlorenceHttpServerOptions {
  host?: string;
  port: number;
}

/**
 * Production composition roots inject application services here. This module
 * owns HTTP lifecycle only; it does not construct domain, database, or provider
 * implementations.
 */
export async function startFlorenceServer(options: StartFlorenceServerOptions): Promise<FastifyInstance> {
  const server = await createFlorenceHttpServer(options);
  try {
    await server.listen({
      host: options.host ?? "0.0.0.0",
      port: options.port,
    });
    return server;
  } catch (error) {
    await server.close();
    throw error;
  }
}

export interface RunProductionServerOptions {
  readonly config?: FlorenceConfig;
  readonly signal?: AbortSignal;
  readonly createComposition?: (options: {
    readonly config: FlorenceConfig;
    readonly migrate: false;
  }) => Promise<FlorenceProductionComposition>;
  readonly startServer?: (options: StartFlorenceServerOptions) => Promise<{ close(): Promise<void> }>;
}

/** Runs HTTP alone (`web`) or HTTP plus durable background loops (`all`). */
export async function runProductionServer(options: RunProductionServerOptions = {}): Promise<void> {
  const config = options.config ?? loadConfig();
  if (config.FLORENCE_PROCESS_ROLE === "worker") {
    throw new Error("The Florence server entrypoint cannot run with the worker process role");
  }
  const composition = await (options.createComposition ?? createProductionComposition)({
    config,
    migrate: false,
  });
  const controller = new AbortController();
  const abort = () => controller.abort(options.signal?.reason);
  if (options.signal?.aborted) controller.abort(options.signal.reason);
  else options.signal?.addEventListener("abort", abort, { once: true });

  let server: { close(): Promise<void> } | null = null;
  const background =
    config.FLORENCE_PROCESS_ROLE === "all" ? composition.background.run(controller.signal) : null;
  const backgroundOutcome = background?.then(
    () => ({ status: "stopped" as const }),
    (error: unknown) => ({ status: "failed" as const, error }),
  );
  try {
    server = await (options.startServer ?? startFlorenceServer)({
      ...composition.http,
      port: config.PORT,
    });
    if (backgroundOutcome === undefined) {
      await waitForAbort(controller.signal);
    } else {
      const outcome = await Promise.race([backgroundOutcome, waitForAbort(controller.signal)]);
      if (outcome.status === "failed") throw outcome.error;
      if (outcome.status === "stopped" && !controller.signal.aborted) {
        throw new Error("Florence background runtime stopped unexpectedly");
      }
    }
  } finally {
    controller.abort();
    options.signal?.removeEventListener("abort", abort);
    if (background !== null) await Promise.allSettled([background]);
    try {
      if (server !== null) await server.close();
    } finally {
      await composition.close();
    }
  }
}

async function waitForAbort(signal: AbortSignal): Promise<{ status: "aborted" }> {
  if (signal.aborted) return { status: "aborted" };
  await new Promise<void>((resolve) => signal.addEventListener("abort", () => resolve(), { once: true }));
  return { status: "aborted" };
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
    await runProductionServer({ signal: controller.signal });
  } finally {
    dispose();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  void main().catch(() => {
    process.stderr.write("Florence server stopped after a fatal startup or runtime error.\n");
    process.exitCode = 1;
  });
}
