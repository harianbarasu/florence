import { describe, expect, test, vi } from "vitest";
import { z } from "zod";
import {
  CapabilityAdapterError,
  type CapabilityDefinition,
  CapabilityRegistry,
  type CapabilitySource,
  defineCapability,
  type JsonValue,
} from "./capability-lifecycle.js";

type TestContext = {
  readonly householdId: string;
};
type TestArguments = {
  readonly query: string;
  readonly filters?:
    | {
        readonly limit: number;
      }
    | undefined;
};
type TestOutput = {
  readonly value: string;
};
type TestProgress = {
  readonly note: string;
};

const argumentSchema = z
  .object({
    query: z.string().min(1),
    filters: z.object({ limit: z.number().int().positive() }).strict().optional(),
  })
  .strict();
const outputSchema = z.object({ value: z.string() }).strict();
const progressSchema = z.object({ note: z.string() }).strict();
const modelSchema = {
  type: "object",
  properties: {
    query: { type: "string" },
  },
  required: ["query"],
  additionalProperties: false,
} as const satisfies JsonValue;
const context = { householdId: "household-1" } satisfies TestContext;
const turnSource = {
  sourceId: "message-1",
  kind: "turn",
  ownerId: "adult-1",
  visibility: "runtime",
  provider: "linq",
  label: "Current private message",
  observedAt: "2026-08-27T20:00:00.000Z",
} as const satisfies CapabilitySource;
const publicSource = {
  sourceId: "public-1",
  kind: "public",
  ownerId: null,
  visibility: "public",
  provider: "weather.gov",
  label: "Weather observation",
  observedAt: "2026-08-27T20:00:01.000Z",
} as const satisfies CapabilitySource;

function testCapability(
  overrides: Partial<CapabilityDefinition<TestContext, TestArguments, TestOutput, TestProgress>> = {},
): CapabilityDefinition<TestContext> {
  return defineCapability<TestContext, TestArguments, TestOutput, TestProgress>({
    name: "lookup",
    description: "Look up a harmless public fact.",
    modelSchema,
    inputSchema: argumentSchema,
    outputSchema,
    progressSchema,
    consequence: "read_only",
    executionMode: "parallel",
    timeoutMs: 100,
    maxOutputBytes: 4_096,
    maxProgressBytes: 1_024,
    maxProgressEvents: 4,
    provenance: {
      provider: "test-provider",
      adapter: "test-adapter",
      operation: "lookup",
    },
    admit: () => true,
    async execute({ arguments: args }) {
      return {
        output: { value: args.query },
        sources: [publicSource],
      };
    },
    ...overrides,
  });
}

function rawCall(callId: string, query: string, name = "lookup") {
  return {
    callId,
    name,
    argumentsJson: JSON.stringify({ query }),
  };
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

function fixedNow(): Date {
  return new Date("2026-08-27T20:00:02.000Z");
}

describe("CapabilityRegistry", () => {
  test("rejects duplicate names and effect definitions, and replacement advances generation", () => {
    const definition = testCapability();
    expect(() => new CapabilityRegistry([definition, definition])).toThrow(/Duplicate Florence capability/);

    expect(
      () =>
        new CapabilityRegistry([
          testCapability({
            consequence: "effect",
          }),
        ]),
    ).toThrow(/provider settlement/);

    const first = new CapabilityRegistry([definition], { generation: 7 });
    const second = first.withDefinitions([definition]);
    expect(first.generation).toBe(7);
    expect(second.generation).toBe(8);
  });

  test("creates a sorted frozen coherent catalog and shares availability probes", async () => {
    let probes = 0;
    const availability = () => {
      probes += 1;
      return true;
    };
    const registry = new CapabilityRegistry([
      testCapability({ name: "zeta", availability }),
      testCapability({ name: "alpha", availability }),
    ]);

    const snapshot = await registry.catalog(context);
    const result = await registry.executeCalls({
      snapshot,
      context,
      calls: [rawCall("call-1", "one", "zeta"), rawCall("call-2", "two", "alpha")],
      completion: "complete",
      turnSource,
    });

    expect(snapshot.tools.map((tool) => tool.name)).toEqual(["alpha", "zeta"]);
    expect(result.results.map((terminal) => terminal.outcome)).toEqual(["succeeded", "succeeded"]);
    expect(probes).toBe(2);
    expect(Object.isFrozen(snapshot)).toBe(true);
    expect(Object.isFrozen(snapshot.tools)).toBe(true);
    expect(Object.isFrozen(snapshot.tools[0]?.parameters)).toBe(true);
    expect(() => (snapshot.tools as unknown as { name: string }[]).push({ name: "rogue" })).toThrow();
  });

  test("uses the same live admission boundary for catalog and dispatch and can only narrow", async () => {
    let allowDispatch = true;
    let executions = 0;
    const phases: string[] = [];
    const registry = new CapabilityRegistry([
      testCapability({
        admit(input) {
          phases.push(input.phase);
          return input.phase === "catalog" || allowDispatch;
        },
        async execute() {
          executions += 1;
          return { output: { value: "never" }, sources: [publicSource] };
        },
      }),
    ]);
    const snapshot = await registry.catalog(context);
    allowDispatch = false;

    const result = await registry.executeCalls({
      snapshot,
      context,
      calls: [rawCall("call-1", "weather")],
      completion: "complete",
      turnSource,
      now: fixedNow,
    });

    expect(phases).toEqual(["catalog", "dispatch"]);
    expect(executions).toBe(0);
    expect(result.results[0]?.outcome).toBe("protocol_rejected");
    expect(result.results[0]?.errorCode).toBe("admission_revoked");

    const nextRegistry = registry.withDefinitions([testCapability()]);
    await expect(
      nextRegistry.executeCalls({
        snapshot,
        context,
        calls: [rawCall("call-2", "weather")],
        completion: "complete",
        turnSource,
      }),
    ).rejects.toThrow(/not minted/);
  });

  test("bounds availability probes that ignore cancellation", async () => {
    const registry = new CapabilityRegistry(
      [
        testCapability({
          availability: () => new Promise<boolean>(() => undefined),
        }),
      ],
      { gateTimeoutMs: 5 },
    );

    const snapshot = await registry.catalog(context);
    expect(snapshot.tools).toEqual([]);
  });
});

describe("capability protocol", () => {
  test("records every request in source order and rejects truncated output without execution", async () => {
    const execute = vi.fn(async () => ({ output: { value: "bad" }, sources: [publicSource] }));
    const registry = new CapabilityRegistry([testCapability({ execute })]);
    const snapshot = await registry.catalog(context);
    const calls = [rawCall("call-1", "first"), rawCall("call-2", "second")];

    const result = await registry.executeCalls({
      snapshot,
      context,
      calls,
      completion: "truncated",
      turnSource,
      now: fixedNow,
    });

    expect(execute).not.toHaveBeenCalled();
    expect(result.events.slice(0, 2)).toMatchObject([
      { phase: "requested", callId: "call-1", sourceIndex: 0 },
      { phase: "requested", callId: "call-2", sourceIndex: 1 },
    ]);
    expect(result.results.map((item) => item.outcome)).toEqual(["protocol_rejected", "protocol_rejected"]);
    expect(result.results.map((item) => item.errorCode)).toEqual([
      "truncated_model_output",
      "truncated_model_output",
    ]);
  });

  test("protocol-rejects malformed, unknown, schema-invalid, and every duplicate call", async () => {
    const execute = vi.fn(async () => ({ output: { value: "bad" }, sources: [publicSource] }));
    const registry = new CapabilityRegistry([testCapability({ execute })]);
    const snapshot = await registry.catalog(context);
    const result = await registry.executeCalls({
      snapshot,
      context,
      calls: [
        { callId: "json", name: "lookup", argumentsJson: "{" },
        rawCall("unknown", "x", "not_registered"),
        { callId: "schema", name: "lookup", argumentsJson: JSON.stringify({ query: 42 }) },
        rawCall("duplicate", "one"),
        rawCall("duplicate", "two"),
        { callId: 42, name: "lookup", argumentsJson: "{}" },
      ],
      completion: "complete",
      turnSource,
      now: fixedNow,
    });

    expect(execute).not.toHaveBeenCalled();
    expect(result.results.map((item) => item.errorCode)).toEqual([
      "invalid_arguments",
      "unknown_or_unavailable_capability",
      "invalid_arguments",
      "duplicate_call_id",
      "duplicate_call_id",
      "malformed_call",
    ]);
    expect(result.events.filter((event) => event.phase === "terminal")).toHaveLength(6);
    expect(result.results.map((item) => item.sourceIndex)).toEqual([0, 1, 2, 3, 4, 5]);
  });
});

describe("capability lifecycle", () => {
  test("canonicalizes and freezes arguments, emits bounded progress, and returns a sourced model envelope", async () => {
    let capturedArguments: TestArguments | undefined;
    let lateProgress: ((progress: TestProgress) => void) | undefined;
    const observedPhases: string[] = [];
    const registry = new CapabilityRegistry([
      testCapability({
        async execute({ arguments: args, reportProgress }) {
          capturedArguments = args;
          lateProgress = reportProgress;
          expect(Object.isFrozen(args)).toBe(true);
          expect(Object.isFrozen(args.filters)).toBe(true);
          reportProgress({ note: "Checking the public source" });
          return { output: { value: args.query }, sources: [publicSource] };
        },
      }),
    ]);
    const snapshot = await registry.catalog(context);
    const result = await registry.executeCalls({
      snapshot,
      context,
      calls: [
        {
          callId: "call-1",
          name: "lookup",
          argumentsJson: '{"query":"rain","filters":{"limit":3}}',
        },
      ],
      completion: "complete",
      turnSource,
      now: fixedNow,
      observer(event) {
        observedPhases.push(event.phase);
        if (event.phase === "progress") throw new Error("presentation failed");
      },
    });
    const eventCount = result.events.length;
    lateProgress?.({ note: "late" });
    await Promise.resolve();

    expect(capturedArguments).toEqual({ query: "rain", filters: { limit: 3 } });
    expect(observedPhases).toEqual(["requested", "admitted", "running", "progress", "terminal"]);
    expect(result.events).toHaveLength(eventCount);
    expect(result.results[0]?.outcome).toBe("succeeded");
    expect(result.results[0]?.sources.map((source) => source.sourceId)).toEqual(["message-1", "public-1"]);
    expect(result.results[0]?.provenance).toMatchObject({
      provider: "test-provider",
      adapter: "test-adapter",
      operation: "lookup",
      registryGeneration: 1,
    });
    expect(result.results[0]?.provenance.inputDigest).toMatch(/^[0-9a-f]{64}$/);
    expect(result.results[0]?.serializedBytes).toBeLessThan(70_000);
    expect(JSON.parse(result.results[0]?.modelOutput ?? "{}")).toEqual({
      error: null,
      outcome: "succeeded",
      output: { value: "rain" },
      provenance: result.results[0]?.provenance,
      sources: result.results[0]?.sources,
    });
    expect(Object.isFrozen(result.results[0])).toBe(true);
    expect(Object.isFrozen(result.results[0]?.sources)).toBe(true);
  });

  test("digests semantically identical validated arguments identically", async () => {
    const registry = new CapabilityRegistry([testCapability()]);
    const snapshot = await registry.catalog(context);
    const result = await registry.executeCalls({
      snapshot,
      context,
      calls: [
        {
          callId: "call-1",
          name: "lookup",
          argumentsJson: '{"query":"rain","filters":{"limit":3}}',
        },
        {
          callId: "call-2",
          name: "lookup",
          argumentsJson: '{"filters":{"limit":3},"query":"rain"}',
        },
      ],
      completion: "complete",
      turnSource,
    });

    expect(result.results[0]?.provenance.inputDigest).toBe(result.results[1]?.provenance.inputDigest);
  });

  test("ignores observer rejection and does not await a hanging observer", async () => {
    const registry = new CapabilityRegistry([testCapability()]);
    const snapshot = await registry.catalog(context);

    const result = await registry.executeCalls({
      snapshot,
      context,
      calls: [rawCall("call-1", "rain")],
      completion: "complete",
      turnSource,
      observer(event) {
        if (event.phase === "requested") return new Promise<void>(() => undefined);
        return Promise.reject(new Error("observer failed"));
      },
    });

    expect(result.results[0]?.outcome).toBe("succeeded");
  });

  test("emits parallel terminals in completion order but returns model evidence in source order", async () => {
    const firstRelease = deferred<void>();
    let sawSecondBeforeFirstFinished = false;
    let firstFinished = false;
    const registry = new CapabilityRegistry([
      testCapability({
        async execute({ arguments: args }) {
          if (args.query === "first") {
            await firstRelease.promise;
            firstFinished = true;
          } else if (!firstFinished) {
            sawSecondBeforeFirstFinished = true;
          }
          return { output: { value: args.query }, sources: [publicSource] };
        },
      }),
    ]);
    const snapshot = await registry.catalog(context);
    setTimeout(() => firstRelease.resolve(), 15);

    const result = await registry.executeCalls({
      snapshot,
      context,
      calls: [rawCall("call-1", "first"), rawCall("call-2", "second")],
      completion: "complete",
      turnSource,
    });

    const terminalIds = result.events.flatMap((event) =>
      event.phase === "terminal" ? [event.terminal.callId] : [],
    );
    expect(sawSecondBeforeFirstFinished).toBe(true);
    expect(terminalIds).toEqual(["call-2", "call-1"]);
    expect(result.results.map((item) => item.callId)).toEqual(["call-1", "call-2"]);
    expect(
      result.results.map((item) => (JSON.parse(item.modelOutput) as { output: TestOutput }).output.value),
    ).toEqual(["first", "second"]);
  });

  test("a sequential override makes the entire admitted batch execute exactly once in source order", async () => {
    const starts: string[] = [];
    const executions = new Map<string, number>();
    const registry = new CapabilityRegistry([
      testCapability({
        name: "slow",
        executionMode: "sequential",
        async execute({ arguments: args }) {
          starts.push(`slow:${args.query}`);
          executions.set(`slow:${args.query}`, (executions.get(`slow:${args.query}`) ?? 0) + 1);
          await new Promise((resolve) => setTimeout(resolve, 5));
          return { output: { value: args.query }, sources: [publicSource] };
        },
      }),
      testCapability({
        name: "fast",
        async execute({ arguments: args }) {
          starts.push(`fast:${args.query}`);
          executions.set(`fast:${args.query}`, (executions.get(`fast:${args.query}`) ?? 0) + 1);
          return { output: { value: args.query }, sources: [publicSource] };
        },
      }),
    ]);
    const snapshot = await registry.catalog(context);

    const result = await registry.executeCalls({
      snapshot,
      context,
      calls: [rawCall("call-1", "one", "slow"), rawCall("call-2", "two", "fast")],
      completion: "complete",
      turnSource,
    });

    expect(starts).toEqual(["slow:one", "fast:two"]);
    expect([...executions.values()]).toEqual([1, 1]);
    expect(result.results.map((item) => item.outcome)).toEqual(["succeeded", "succeeded"]);
  });

  test("outer cancellation terminalizes a running ignored signal and every unstarted call", async () => {
    const controller = new AbortController();
    let executions = 0;
    const registry = new CapabilityRegistry([
      testCapability({
        executionMode: "sequential",
        async execute() {
          executions += 1;
          return new Promise(() => undefined);
        },
      }),
    ]);
    const snapshot = await registry.catalog(context);

    const result = await registry.executeCalls({
      snapshot,
      context,
      calls: [rawCall("call-1", "one"), rawCall("call-2", "two"), rawCall("call-3", "three")],
      completion: "complete",
      turnSource,
      signal: controller.signal,
      observer(event) {
        if (event.phase === "running") controller.abort();
      },
    });

    expect(executions).toBe(1);
    expect(result.results.map((item) => item.outcome)).toEqual(["cancelled", "cancelled", "cancelled"]);
    expect(result.events.filter((event) => event.phase === "running")).toHaveLength(1);
    expect(result.events.filter((event) => event.phase === "terminal")).toHaveLength(3);
  });

  test("times out an adapter that ignores its signal and suppresses late updates and completion", async () => {
    const release = deferred<{ output: TestOutput; sources: readonly CapabilitySource[] }>();
    let reportLate: ((progress: TestProgress) => void) | undefined;
    const registry = new CapabilityRegistry([
      testCapability({
        timeoutMs: 5,
        async execute({ reportProgress }) {
          reportLate = reportProgress;
          return release.promise;
        },
      }),
    ]);
    const snapshot = await registry.catalog(context);
    const result = await registry.executeCalls({
      snapshot,
      context,
      calls: [rawCall("call-1", "rain")],
      completion: "complete",
      turnSource,
    });
    const eventCount = result.events.length;
    reportLate?.({ note: "late" });
    release.resolve({ output: { value: "late" }, sources: [publicSource] });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(result.results[0]?.outcome).toBe("failed");
    expect(result.results[0]?.errorCode).toBe("timeout");
    expect(result.results[0]?.retryable).toBe(true);
    expect(result.events).toHaveLength(eventCount);
  });

  test("turns invalid or excessive progress into one closed terminal failure", async () => {
    const registry = new CapabilityRegistry([
      testCapability({
        async execute({ reportProgress }) {
          reportProgress({ note: 42 } as unknown as TestProgress);
          return new Promise(() => undefined);
        },
      }),
    ]);
    const snapshot = await registry.catalog(context);
    const result = await registry.executeCalls({
      snapshot,
      context,
      calls: [rawCall("call-1", "rain")],
      completion: "complete",
      turnSource,
    });

    expect(result.results[0]?.outcome).toBe("failed");
    expect(result.results[0]?.errorCode).toBe("adapter_contract");
    expect(result.events.filter((event) => event.phase === "terminal")).toHaveLength(1);
    expect(result.events.filter((event) => event.phase === "progress")).toHaveLength(0);
  });

  test("validates and bounds outputs and evidence before they reach the model", async () => {
    const registry = new CapabilityRegistry([
      testCapability({
        name: "oversized",
        maxOutputBytes: 10,
        async execute() {
          return { output: { value: "far too much output" }, sources: [publicSource] };
        },
      }),
      testCapability({
        name: "bad_schema",
        async execute() {
          return {
            output: { nope: true } as unknown as TestOutput,
            sources: [publicSource],
          };
        },
      }),
    ]);
    const snapshot = await registry.catalog(context);
    const result = await registry.executeCalls({
      snapshot,
      context,
      calls: [rawCall("call-1", "x", "oversized"), rawCall("call-2", "x", "bad_schema")],
      completion: "complete",
      turnSource,
    });

    expect(result.results.map((item) => item.errorCode)).toEqual(["output_too_large", "adapter_contract"]);
    for (const terminal of result.results) {
      const modelEnvelope = JSON.parse(terminal.modelOutput) as {
        outcome: string;
        sources: CapabilitySource[];
        provenance: { adapter: string };
      };
      expect(modelEnvelope.outcome).toBe("failed");
      expect(modelEnvelope.sources[0]?.sourceId).toBe("message-1");
      expect(modelEnvelope.provenance.adapter).toBe("test-adapter");
    }
  });

  test("normalizes adapter errors to a closed safe vocabulary and forbids unknown reads", async () => {
    const registry = new CapabilityRegistry([
      testCapability({
        name: "secret_error",
        async execute() {
          throw new Error("secret provider token abc123");
        },
      }),
      testCapability({
        name: "transient_error",
        async execute() {
          throw new CapabilityAdapterError("transient", "The public source is briefly unavailable.");
        },
      }),
      testCapability({
        name: "unknown_read",
        async execute() {
          throw new CapabilityAdapterError("unknown", "Maybe it happened.");
        },
      }),
    ]);
    const snapshot = await registry.catalog(context);
    const result = await registry.executeCalls({
      snapshot,
      context,
      calls: [
        rawCall("call-1", "x", "secret_error"),
        rawCall("call-2", "x", "transient_error"),
        rawCall("call-3", "x", "unknown_read"),
      ],
      completion: "complete",
      turnSource,
    });

    expect(result.results.map((item) => item.errorCode)).toEqual([
      "internal_adapter_error",
      "transient",
      "adapter_contract",
    ]);
    expect(result.results.map((item) => item.retryable)).toEqual([false, true, false]);
    expect(result.results[0]?.modelOutput).not.toContain("abc123");
    expect(result.results[2]?.outcome).toBe("failed");
  });
});
