import { describe, expect, test, vi } from "vitest";
import { z } from "zod";
import {
  type CapabilityDefinition,
  CapabilityRegistry,
  defineCapability,
  type JsonValue,
} from "./capability-lifecycle.js";

type TestContext = { readonly householdId: string };
type TestArguments = { readonly query: string };
type TestOutput = { readonly value: string };

const context = { householdId: "household-1" } satisfies TestContext;
const modelSchema = {
  type: "object",
  properties: { query: { type: "string" } },
  required: ["query"],
  additionalProperties: false,
} as const satisfies JsonValue;

function testCapability(
  overrides: Partial<CapabilityDefinition<TestContext, TestArguments, TestOutput>> = {},
): CapabilityDefinition<TestContext> {
  return defineCapability<TestContext, TestArguments, TestOutput>({
    name: "lookup",
    description: "Look something up.",
    modelSchema,
    inputSchema: z.object({ query: z.string().min(1) }).strict(),
    outputSchema: z.object({ value: z.string() }).strict(),
    executionMode: "parallel",
    timeoutMs: 100,
    maxOutputBytes: 4_096,
    async execute({ arguments: args }) {
      return { output: { value: args.query } };
    },
    ...overrides,
  });
}

function rawCall(callId: string, query: string) {
  return { callId, name: "lookup", argumentsJson: JSON.stringify({ query }) };
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

describe("capability execution", () => {
  test("malformed and truncated calls never execute", async () => {
    const execute = vi.fn(async () => ({ output: { value: "unexpected" } }));
    const registry = new CapabilityRegistry([testCapability({ execute })]);
    const snapshot = await registry.catalog(context);

    const malformed = await registry.executeCalls({
      snapshot,
      context,
      calls: [
        { callId: "bad-json", name: "lookup", argumentsJson: "{" },
        { callId: "bad-schema", name: "lookup", argumentsJson: '{"query":42}' },
      ],
      completion: "complete",
    });
    const truncated = await registry.executeCalls({
      snapshot,
      context,
      calls: [rawCall("cut-off", "weather")],
      completion: "truncated",
    });

    expect(execute).not.toHaveBeenCalled();
    expect(malformed.results.map((result) => result.errorCode)).toEqual([
      "invalid_arguments",
      "invalid_arguments",
    ]);
    expect(truncated.results[0]?.errorCode).toBe("truncated_model_output");
  });

  test("timeout and cancellation each produce exactly one terminal result", async () => {
    const timeoutRegistry = new CapabilityRegistry([
      testCapability({
        timeoutMs: 5,
        async execute() {
          return new Promise(() => undefined);
        },
      }),
    ]);
    const timeoutSnapshot = await timeoutRegistry.catalog(context);
    const timedOut = await timeoutRegistry.executeCalls({
      snapshot: timeoutSnapshot,
      context,
      calls: [rawCall("timeout", "weather")],
      completion: "complete",
    });

    const controller = new AbortController();
    const cancelRegistry = new CapabilityRegistry([
      testCapability({
        async execute() {
          return new Promise(() => undefined);
        },
      }),
    ]);
    const cancelSnapshot = await cancelRegistry.catalog(context);
    const cancelled = await cancelRegistry.executeCalls({
      snapshot: cancelSnapshot,
      context,
      calls: [rawCall("cancel", "weather")],
      completion: "complete",
      signal: controller.signal,
      onStart() {
        queueMicrotask(() => controller.abort());
      },
    });

    expect(timedOut.results[0]).toMatchObject({ outcome: "failed", errorCode: "timeout" });
    expect(cancelled.results[0]).toMatchObject({ outcome: "cancelled", errorCode: "cancelled" });
    expect(timedOut.results).toHaveLength(1);
    expect(cancelled.results).toHaveLength(1);
  });

  test("parallel execution retains source order and starts presentation once", async () => {
    const releaseFirst = deferred<void>();
    const onStart = vi.fn();
    const registry = new CapabilityRegistry([
      testCapability({
        async execute({ arguments: args }) {
          if (args.query === "first") await releaseFirst.promise;
          return { output: { value: args.query } };
        },
      }),
    ]);
    const snapshot = await registry.catalog(context);
    setTimeout(() => releaseFirst.resolve(), 10);

    const batch = await registry.executeCalls({
      snapshot,
      context,
      calls: [rawCall("first-call", "first"), rawCall("second-call", "second")],
      completion: "complete",
      onStart,
    });

    expect(onStart).toHaveBeenCalledOnce();
    expect(batch.results.map((result) => result.callId)).toEqual(["first-call", "second-call"]);
    expect(
      batch.results.map(
        (result) => (JSON.parse(result.modelOutput) as { readonly output: TestOutput }).output.value,
      ),
    ).toEqual(["first", "second"]);
  });
});
