import { describe, expect, it, vi } from "vitest";
import { startWorker } from "../../src/worker.js";
import { setup } from "./fixtures.js";

describe("application worker entry point", () => {
  it("constructs the application behind an injected durable host", async () => {
    const harness = setup();
    const run = vi.fn(async (application, signal?: AbortSignal) => {
      expect(application.process).toBeTypeOf("function");
      expect(application.executeOutbox).toBeTypeOf("function");
      expect(signal?.aborted).toBe(false);
    });
    const controller = new AbortController();

    await startWorker({ dependencies: harness.dependencies, host: { run } }, controller.signal);

    expect(run).toHaveBeenCalledOnce();
  });
});
