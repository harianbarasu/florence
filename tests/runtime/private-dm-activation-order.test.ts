import { describe, expect, it, vi } from "vitest";
import { isNaturalPrivateGreeting } from "../../src/application/florence-application.js";
import type { WorkerRuntime } from "../../src/modules/orchestration/contracts.js";
import { FlorenceOrchestrator } from "../../src/runtime/orchestrator.js";

describe("private DM activation ordering", () => {
  it("recognizes only a standalone private greeting", () => {
    expect(isNaturalPrivateGreeting("direct", "Hi Florence 👋")).toBe(true);
    expect(isNaturalPrivateGreeting("direct", "Hello there!")).toBe(true);
    expect(isNaturalPrivateGreeting("direct", "Hi Florence, Jackson needs pickup")).toBe(false);
    expect(isNaturalPrivateGreeting("group", "Hi Florence")).toBe(false);
  });

  it("routes a private greeting through the post-orchestration application continuation", async () => {
    const internalProviderEventId = "10000000-0000-4000-8000-000000000001";
    const process = vi.fn().mockResolvedValue({
      accepted: true,
      duplicate: false,
      disposition: "private_dm_response_then_google_activation_queued",
      ids: {},
    });
    const workers: WorkerRuntime = {
      run: vi.fn(() => {
        throw new Error("A deterministic greeting must not call a model worker");
      }),
      reconcile: vi.fn().mockResolvedValue(undefined),
    };
    const orchestrator = new FlorenceOrchestrator(
      null as never,
      { defaults: { rawSourceRetentionDays: 30 } } as never,
      null as never,
      workers,
      null,
      { process },
    );
    Object.defineProperty(orchestrator, "compileLinqContext", {
      value: vi.fn().mockResolvedValue({
        row: { id: internalProviderEventId },
        record: { routing: { chatKind: "direct" } },
        text: "Hi Florence",
      }),
    });

    await expect(orchestrator.processLinqMessage(internalProviderEventId)).resolves.toBe(
      "private_greeting_acknowledgment_queued",
    );
    expect(process).toHaveBeenCalledOnce();
    expect(process).toHaveBeenCalledWith({
      kind: "linq.private_dm_orchestration_complete",
      internalProviderEventId,
      response: { kind: "greeting_acknowledgment" },
    });
    expect(workers.run).not.toHaveBeenCalled();
  });
});
