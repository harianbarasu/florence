import { describe, expect, test } from "vitest";
import { z } from "zod";
import { runAgentLoop } from "./agent-loop.js";
import { CapabilityRegistry, defineCapability, type JsonValue } from "./capability-lifecycle.js";

interface TestContext {
  readonly allowWrites: boolean;
}

const operationSchema = z.object({
  operation: z.enum(["read", "write", "denied"]),
});

function operationOf(value: JsonValue | undefined): string | undefined {
  if (!value || Array.isArray(value) || typeof value !== "object") return undefined;
  const operation = (value as { readonly [key: string]: JsonValue }).operation;
  return typeof operation === "string" ? operation : undefined;
}

function testRegistry(executions: string[], resolvedArguments: JsonValue[]) {
  return new CapabilityRegistry<TestContext>([
    defineCapability({
      name: "household_tool",
      description: "Use the household tool.",
      modelSchema: modelSchema(["read", "write", "denied"]),
      inputSchema: operationSchema,
      outputSchema: z.object({ operation: z.string() }),
      executionMode: "sequential",
      presentation: ({ context }) => ({
        description: context.allowWrites
          ? "Read or change household information."
          : "Read household information.",
        modelSchema: modelSchema(context.allowWrites ? ["read", "write", "denied"] : ["read"]),
      }),
      executionBoundary: ({ canonicalArguments }) => {
        resolvedArguments.push(canonicalArguments);
        return operationOf(canonicalArguments) === "read" ? "inline" : "external";
      },
      admit: ({ canonicalArguments }) => operationOf(canonicalArguments) !== "denied",
      timeoutMs: 1_000,
      maxOutputBytes: 1_000,
      async execute({ arguments: args }) {
        executions.push(args.operation);
        return { output: { operation: args.operation } };
      },
    }),
  ]);
}

describe("general agent capability lifecycle", () => {
  test("continues through as many useful tool turns as the task needs by default", async () => {
    const executions: string[] = [];
    const registry = testRegistry(executions, []);
    let modelTurns = 0;

    const result = await runAgentLoop({
      client: {} as never,
      request: { model: "test-model" },
      modelCall: () => {
        modelTurns += 1;
        if (modelTurns <= 13) {
          return response([
            functionCall(`read-${modelTurns}`, "household_tool", { operation: "read" }),
          ]) as never;
        }
        return response([], { text: "I finished the complete multi-source review." }) as never;
      },
      transcript: [],
      registry,
      getCapabilityContext: () => ({ allowWrites: false }),
      parallelToolCalls: false,
    });

    expect(result).toMatchObject({ kind: "completed", turns: 14 });
    expect(executions).toEqual(Array.from({ length: 13 }, () => "read"));
  });

  test("derives a truthful model contract and resolves boundaries from canonical arguments", async () => {
    const executions: string[] = [];
    const resolvedArguments: JsonValue[] = [];
    const registry = testRegistry(executions, resolvedArguments);
    const readOnlyContext = { allowWrites: false };
    const readOnlyCatalog = await registry.catalog(readOnlyContext);

    expect(readOnlyCatalog.tools).toMatchObject([
      {
        name: "household_tool",
        description: "Read household information.",
        parameters: { properties: { operation: { enum: ["read"] } } },
      },
    ]);

    let suspensionChecks = 0;
    const read = await registry.executeCalls({
      snapshot: readOnlyCatalog,
      context: readOnlyContext,
      calls: [
        {
          callId: "read-call",
          name: "household_tool",
          argumentsJson: JSON.stringify({ ignored: "not canonical", operation: "read" }),
        },
      ],
      completion: "complete",
      suspendBeforeExternal: () => {
        suspensionChecks += 1;
        return true;
      },
    });

    expect(read).toMatchObject({ suspendedBeforeExternal: false });
    expect(executions).toEqual(["read"]);
    expect(resolvedArguments).toEqual([{ operation: "read" }]);
    expect(suspensionChecks).toBe(0);

    const writeContext = { allowWrites: true };
    const writeCatalog = await registry.catalog(writeContext);
    expect(writeCatalog.tools[0]).toMatchObject({
      description: "Read or change household information.",
      parameters: { properties: { operation: { enum: ["read", "write", "denied"] } } },
    });
    const write = await registry.executeCalls({
      snapshot: writeCatalog,
      context: writeContext,
      calls: [
        {
          callId: "write-call",
          name: "household_tool",
          argumentsJson: JSON.stringify({ operation: "write" }),
        },
      ],
      completion: "complete",
      suspendBeforeExternal: () => {
        suspensionChecks += 1;
        return true;
      },
    });

    expect(write).toEqual({ results: [], suspendedBeforeExternal: true });
    expect(executions).toEqual(["read"]);
    expect(suspensionChecks).toBe(1);
  });

  test("keeps rejected and truncated calls inline, but suspends a batch before any valid external call", async () => {
    const executions: string[] = [];
    const registry = testRegistry(executions, []);
    const context = { allowWrites: true };
    const rejectedRequests: Array<{ readonly input?: unknown }> = [];
    const rejectedResponses = [
      response([
        functionCall("invalid-call", "household_tool", { operation: "unknown" }),
        functionCall("denied-call", "household_tool", { operation: "denied" }),
      ]),
      response([], { text: "I used the tool errors to replan." }),
    ];
    let suspensionChecks = 0;

    const rejected = await runAgentLoop({
      client: {} as never,
      request: { model: "test-model" },
      modelCall: (request) => {
        rejectedRequests.push(request);
        const next = rejectedResponses.shift();
        if (!next) throw new Error("Unexpected rejected-call model turn");
        return next as never;
      },
      transcript: [],
      registry,
      getCapabilityContext: () => context,
      parallelToolCalls: false,
      suspendBeforeToolExecution: () => {
        suspensionChecks += 1;
        return { value: "checkpoint" };
      },
    });

    expect(rejected.kind).toBe("completed");
    expect(suspensionChecks).toBe(0);
    expect(executions).toEqual([]);
    expect(toolErrors(rejectedRequests[1])).toEqual(["invalid_arguments", "not_admitted"]);

    const truncatedRequests: Array<{ readonly input?: unknown }> = [];
    const truncatedResponses = [
      response(
        [functionCall("truncated-call", "household_tool", { operation: "write" }, "incomplete")],
        null,
        "incomplete",
      ),
      response([], { text: "I retried after the truncated call." }),
    ];
    const truncated = await runAgentLoop({
      client: {} as never,
      request: { model: "test-model" },
      modelCall: (request) => {
        truncatedRequests.push(request);
        const next = truncatedResponses.shift();
        if (!next) throw new Error("Unexpected truncated-call model turn");
        return next as never;
      },
      transcript: [],
      registry,
      getCapabilityContext: () => context,
      parallelToolCalls: false,
      suspendBeforeToolExecution: () => {
        suspensionChecks += 1;
        return { value: "checkpoint" };
      },
    });

    expect(truncated.kind).toBe("completed");
    expect(toolErrors(truncatedRequests[1])).toEqual(["truncated_model_output"]);
    expect(suspensionChecks).toBe(0);

    const mixed = await runAgentLoop({
      client: {} as never,
      request: { model: "test-model" },
      modelCall: () =>
        response([
          functionCall("read-call", "household_tool", { operation: "read" }),
          functionCall("write-call", "household_tool", { operation: "write" }),
        ]) as never,
      transcript: [],
      registry,
      getCapabilityContext: () => context,
      parallelToolCalls: false,
      suspendBeforeToolExecution: () => {
        suspensionChecks += 1;
        return { value: "checkpoint" };
      },
    });

    expect(mixed).toMatchObject({
      kind: "suspended",
      calls: [{ callId: "read-call" }, { callId: "write-call" }],
      suspension: "checkpoint",
    });
    expect(executions).toEqual([]);
    expect(suspensionChecks).toBe(1);
  });
});

function modelSchema(operations: readonly string[]): JsonValue {
  return {
    type: "object",
    additionalProperties: false,
    properties: {
      operation: { type: "string", enum: operations },
    },
    required: ["operation"],
  };
}

function functionCall(
  callId: string,
  name: string,
  args: object,
  status: "completed" | "incomplete" = "completed",
) {
  return {
    id: `item-${callId}`,
    type: "function_call" as const,
    call_id: callId,
    name,
    arguments: JSON.stringify(args),
    status,
  };
}

function response(
  output: readonly ReturnType<typeof functionCall>[],
  outputParsed: object | null = null,
  status: "completed" | "incomplete" = "completed",
) {
  return {
    status,
    output,
    output_parsed: outputParsed,
    output_text: "",
  };
}

function toolErrors(request: { readonly input?: unknown } | undefined): string[] {
  const input = (request?.input as Array<{ type?: string; output?: unknown }> | undefined) ?? [];
  return input
    .filter((item) => item.type === "function_call_output")
    .map((item) => {
      const envelope = JSON.parse(typeof item.output === "string" ? item.output : "{}") as {
        error?: { code?: string } | null;
      };
      return envelope.error?.code ?? "";
    });
}
