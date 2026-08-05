import { describe, expect, it, vi } from "vitest";
import {
  type BackgroundLoop,
  type FlorenceProductionComposition,
  ProductionBackgroundRuntime,
} from "../src/composition.js";
import { type FlorenceConfig, loadConfig } from "../src/config.js";
import { runProductionServer } from "../src/server.js";
import { runProductionWorker } from "../src/worker.js";

function config(role: "all" | "web" | "worker"): FlorenceConfig {
  return loadConfig({
    NODE_ENV: "test",
    FLORENCE_PROCESS_ROLE: role,
    FLORENCE_DATABASE_URL: "postgres://florence:florence@127.0.0.1:55432/florence_test",
    FLORENCE_WEB_BASE_URL: "https://florence.example.test",
    FLORENCE_TOKEN_ENCRYPTION_KEY: Buffer.alloc(32, 7).toString("base64url"),
    FLORENCE_ADMIN_API_KEY: "operator-test-key-with-enough-bytes",
    OPENAI_API_KEY: "test-model-key",
  });
}

function composition(
  roleConfig: FlorenceConfig,
  background: ProductionBackgroundRuntime,
  close: () => Promise<void>,
): FlorenceProductionComposition {
  return {
    config: roleConfig,
    background,
    close,
    http: {},
  } as unknown as FlorenceProductionComposition;
}

function blockingLoop(started: () => void): BackgroundLoop {
  return {
    async run(signal) {
      started();
      if (signal.aborted) return;
      await new Promise<void>((resolve) => signal.addEventListener("abort", () => resolve(), { once: true }));
    },
  };
}

describe("production process topology", () => {
  it("runs HTTP and local backgrounds in the all role", async () => {
    const roleConfig = config("all");
    const external = new AbortController();
    const loopStarted = vi.fn();
    const closeComposition = vi.fn(async () => undefined);
    const closeServer = vi.fn(async () => undefined);
    const runtime = new ProductionBackgroundRuntime([blockingLoop(loopStarted)]);
    const createComposition = vi.fn(async () => composition(roleConfig, runtime, closeComposition));

    await runProductionServer({
      config: roleConfig,
      signal: external.signal,
      createComposition,
      startServer: async () => {
        external.abort();
        return { close: closeServer };
      },
    });

    expect(loopStarted).toHaveBeenCalledOnce();
    expect(createComposition).toHaveBeenCalledWith({ config: roleConfig, migrate: false });
    expect(closeServer).toHaveBeenCalledOnce();
    expect(closeComposition).toHaveBeenCalledOnce();
  });

  it("runs only HTTP in the web role", async () => {
    const roleConfig = config("web");
    const external = new AbortController();
    const loopStarted = vi.fn();
    const closeComposition = vi.fn(async () => undefined);
    const runtime = new ProductionBackgroundRuntime([blockingLoop(loopStarted)]);

    await runProductionServer({
      config: roleConfig,
      signal: external.signal,
      createComposition: async () => composition(roleConfig, runtime, closeComposition),
      startServer: async () => {
        external.abort();
        return { close: vi.fn(async () => undefined) };
      },
    });

    expect(loopStarted).not.toHaveBeenCalled();
    expect(closeComposition).toHaveBeenCalledOnce();
  });

  it("rejects the worker role at the HTTP entrypoint before composing resources", async () => {
    const createComposition = vi.fn();
    await expect(runProductionServer({ config: config("worker"), createComposition })).rejects.toThrow(
      "server entrypoint cannot run with the worker process role",
    );
    expect(createComposition).not.toHaveBeenCalled();
  });

  it("runs backgrounds in the worker role and closes composed resources", async () => {
    const roleConfig = config("worker");
    const external = new AbortController();
    const loopStarted = vi.fn();
    const closeComposition = vi.fn(async () => undefined);
    const runtime = new ProductionBackgroundRuntime([blockingLoop(loopStarted)]);
    const createComposition = vi.fn(async () => composition(roleConfig, runtime, closeComposition));
    const running = runProductionWorker({
      config: roleConfig,
      signal: external.signal,
      createComposition,
    });

    await vi.waitFor(() => expect(loopStarted).toHaveBeenCalledOnce());
    external.abort();
    await running;

    expect(closeComposition).toHaveBeenCalledOnce();
    expect(createComposition).toHaveBeenCalledWith({ config: roleConfig, migrate: false });
  });

  it.each(["web", "all"] as const)(
    "rejects the %s role at the worker entrypoint before composing resources",
    async (role) => {
      const createComposition = vi.fn();
      await expect(runProductionWorker({ config: config(role), createComposition })).rejects.toThrow(
        "worker entrypoint requires the worker process role",
      );
      expect(createComposition).not.toHaveBeenCalled();
    },
  );
});
