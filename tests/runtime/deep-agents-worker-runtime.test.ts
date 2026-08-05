import { describe, expect, it, vi } from "vitest";
import { z } from "zod";
import type { ModelCompletionRequest, ModelCompletionResult, ModelGateway } from "../../src/models/index.js";
import { DeepAgentsWorkerRuntime, type WorkerResultPayload } from "../../src/runtime/index.js";
import { fakeRoute, workerJob, workerPayload } from "./fixtures.js";

class StructuredResultGateway implements ModelGateway {
  readonly requests: ModelCompletionRequest[] = [];

  async complete(
    _profile:
      | "classification_extraction"
      | "tool_planning"
      | "vision_document"
      | "long_context_research"
      | "private_processing",
    request: ModelCompletionRequest,
  ): Promise<ModelCompletionResult> {
    this.requests.push(request);
    const resultTool = request.tools?.find((candidate) => {
      const properties = candidate.inputSchema.properties;
      return (
        typeof properties === "object" &&
        properties !== null &&
        "summary" in properties &&
        "proposedCommands" in properties
      );
    });
    if (resultTool === undefined) {
      throw new Error("The app-owned result tool was not bound.");
    }

    return {
      content: [
        {
          type: "tool_request",
          requestId: `result-${this.requests.length}`,
          name: resultTool.name,
          arguments: workerPayload as unknown as Record<string, never>,
        },
      ],
      finishReason: "tool_request",
      usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
      latencyMs: 1,
      route: fakeRoute,
    };
  }
}

class ToolThenResultGateway implements ModelGateway {
  readonly requests: ModelCompletionRequest[] = [];

  async complete(
    _profile:
      | "classification_extraction"
      | "tool_planning"
      | "vision_document"
      | "long_context_research"
      | "private_processing",
    request: ModelCompletionRequest,
  ): Promise<ModelCompletionResult> {
    this.requests.push(request);
    const hasLookupResult = request.messages.some(
      (message) =>
        message.role === "tool" &&
        message.parts.some((part) => part.type === "tool_result" && part.name === "lookup_evidence"),
    );
    const selected = hasLookupResult
      ? findResultTool(request)
      : request.tools?.find((tool) => tool.name === "lookup_evidence");
    if (selected === undefined) {
      throw new Error("The expected tool was not bound.");
    }

    return {
      content: [
        {
          type: "tool_request",
          requestId: `call-${this.requests.length}`,
          name: selected.name,
          arguments: hasLookupResult
            ? (workerPayload as unknown as Record<string, never>)
            : { reference: "evidence-1" },
        },
      ],
      finishReason: "tool_request",
      usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
      latencyMs: 1,
      route: fakeRoute,
    };
  }
}

class ProseThenResultGateway implements ModelGateway {
  readonly requests: ModelCompletionRequest[] = [];

  async complete(
    _profile:
      | "classification_extraction"
      | "tool_planning"
      | "vision_document"
      | "long_context_research"
      | "private_processing",
    request: ModelCompletionRequest,
  ): Promise<ModelCompletionResult> {
    this.requests.push(request);
    if (this.requests.length === 1) {
      return {
        content: [{ type: "text", text: "The form is due Friday." }],
        finishReason: "stop",
        usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
        latencyMs: 1,
        route: fakeRoute,
      };
    }

    const resultTool = findResultTool(request);
    if (resultTool === undefined) {
      throw new Error("The app-owned result tool was not bound.");
    }
    return {
      content: [
        {
          type: "tool_request",
          requestId: "repaired-result",
          name: resultTool.name,
          arguments: workerPayload as unknown as Record<string, never>,
        },
      ],
      finishReason: "tool_request",
      usage: { inputTokens: 12, outputTokens: 6, totalTokens: 18 },
      latencyMs: 1,
      route: fakeRoute,
    };
  }
}

function runtime(gateway: ModelGateway): DeepAgentsWorkerRuntime {
  return new DeepAgentsWorkerRuntime({
    modelGateway: gateway,
    systemPrompt: "Perform the bounded worker job and return only proposals.",
    generalPurpose: {
      description: "Handle bounded general analysis.",
      systemPrompt: "Analyze only the delegated objective.",
      allowedToolNames: [],
    },
    specialists: [
      {
        name: "verifier",
        description: "Verify claims against granted evidence.",
        systemPrompt: "Return a concise verification report.",
        allowedToolNames: [],
      },
    ],
  });
}

describe("DeepAgentsWorkerRuntime", () => {
  it("runs the pinned Deep Agents graph through the app-owned model seam", async () => {
    const gateway = new StructuredResultGateway();
    const cleanup = vi.fn(async () => undefined);

    const result = await runtime(gateway).run(workerJob(), { cleanup });

    expect(result.summary).toBe(workerPayload.summary);
    expect(result.proposedCommands).toEqual(workerPayload.proposedCommands);
    expect(result.diagnostics).toMatchObject({
      modelCalls: 1,
      toolCalls: 0,
      usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
      modelRoute: fakeRoute,
      traceReferences: [],
    });
    expect(cleanup).toHaveBeenCalledOnce();
    expect(gateway.requests).toHaveLength(1);
  });

  it("repairs a premature prose answer by forcing the app-owned result contract", async () => {
    const gateway = new ProseThenResultGateway();

    const result = await runtime(gateway).run(workerJob());

    expect(result.summary).toBe(workerPayload.summary);
    expect(result.diagnostics).toMatchObject({
      modelCalls: 2,
      toolCalls: 0,
      usage: { inputTokens: 22, outputTokens: 11, totalTokens: 33 },
    });
    expect(gateway.requests).toHaveLength(2);
    const retryRequest = gateway.requests[1];
    expect(retryRequest).toBeDefined();
    if (retryRequest === undefined) throw new Error("The repair request was not recorded.");
    expect(retryRequest.toolChoice).toEqual({ name: findResultTool(retryRequest)?.name });
    expect(retryRequest.messages.at(-2)).toMatchObject({
      role: "assistant",
      parts: [{ type: "text", text: "The form is due Friday." }],
    });
  });

  it("does not expose a shell or async-task tool with StateBackend", async () => {
    const gateway = new StructuredResultGateway();
    await runtime(gateway).run(workerJob());

    const toolNames = gateway.requests[0]?.tools?.map((tool) => tool.name) ?? [];
    expect(toolNames).not.toContain("execute");
    expect(toolNames).not.toContain("launch_task");
    expect(toolNames).not.toContain("get_task");
    expect(toolNames).toContain("task");
  });

  it("rejects an ungranted tool before invoking the model and still cleans it", async () => {
    const gateway = new StructuredResultGateway();
    const cleanup = vi.fn(async () => undefined);
    const toolCleanup = vi.fn(async () => undefined);

    await expect(
      runtime(gateway).run(workerJob(), {
        tools: [
          {
            name: "not_allowlisted",
            description: "Must never be exposed.",
            inputSchema: z.object({}),
            execute: async () => null,
            cleanup: toolCleanup,
          },
        ],
        cleanup,
      }),
    ).rejects.toMatchObject({ code: "invalid_job" });

    expect(gateway.requests).toHaveLength(0);
    expect(toolCleanup).toHaveBeenCalledOnce();
    expect(cleanup).toHaveBeenCalledOnce();
  });

  it("counts every requested allowlisted tool against the attempt budget", async () => {
    const gateway = new ToolThenResultGateway();
    const execute = vi.fn(async () => ({ claim: "The form is due Friday." }));
    const toolCleanup = vi.fn(async () => undefined);
    const job = workerJob({
      capabilityIds: ["read-evidence"],
      allowedToolNames: ["lookup_evidence"],
      budget: {
        maxDurationMs: 30_000,
        maxModelCalls: 3,
        maxToolCalls: 1,
        maxOutputTokens: 1_000,
      },
    });

    const result = await runtime(gateway).run(job, {
      tools: [
        {
          name: "lookup_evidence",
          description: "Read exactly one granted evidence reference.",
          inputSchema: z.object({ reference: z.string() }),
          requiredCapabilityIds: ["read-evidence"],
          execute,
          cleanup: toolCleanup,
        },
      ],
    });

    expect(result.diagnostics).toMatchObject({ modelCalls: 2, toolCalls: 1 });
    expect(execute).toHaveBeenCalledOnce();
    expect(toolCleanup).toHaveBeenCalledOnce();
  });

  it("stops before executing a tool that would exceed its budget", async () => {
    const gateway = new ToolThenResultGateway();
    const execute = vi.fn(async () => ({ claim: "must not run" }));
    const cleanup = vi.fn(async () => undefined);
    const job = workerJob({
      capabilityIds: ["read-evidence"],
      allowedToolNames: ["lookup_evidence"],
      budget: {
        maxDurationMs: 30_000,
        maxModelCalls: 3,
        maxToolCalls: 0,
        maxOutputTokens: 1_000,
      },
    });

    await expect(
      runtime(gateway).run(job, {
        tools: [
          {
            name: "lookup_evidence",
            description: "Read exactly one granted evidence reference.",
            inputSchema: z.object({ reference: z.string() }),
            requiredCapabilityIds: ["read-evidence"],
            execute,
          },
        ],
        cleanup,
      }),
    ).rejects.toMatchObject({ code: "budget_exceeded" });

    expect(execute).not.toHaveBeenCalled();
    expect(cleanup).toHaveBeenCalledOnce();
    expect(gateway.requests).toHaveLength(1);
  });

  it("stops before a model call that would exceed its budget", async () => {
    const gateway = new ToolThenResultGateway();
    const execute = vi.fn(async () => ({ claim: "The form is due Friday." }));
    const job = workerJob({
      capabilityIds: ["read-evidence"],
      allowedToolNames: ["lookup_evidence"],
      budget: {
        maxDurationMs: 30_000,
        maxModelCalls: 1,
        maxToolCalls: 1,
        maxOutputTokens: 1_000,
      },
    });

    await expect(
      runtime(gateway).run(job, {
        tools: [
          {
            name: "lookup_evidence",
            description: "Read exactly one granted evidence reference.",
            inputSchema: z.object({ reference: z.string() }),
            requiredCapabilityIds: ["read-evidence"],
            execute,
          },
        ],
      }),
    ).rejects.toMatchObject({ code: "budget_exceeded" });

    expect(execute).toHaveBeenCalledOnce();
    expect(gateway.requests).toHaveLength(1);
  });

  it("rejects invalid app-owned output even when the graph completed", async () => {
    const invalidPayload: WorkerResultPayload = {
      ...workerPayload,
      confidence: Number.NaN,
    };
    const gateway: ModelGateway = {
      async complete(_profile, request) {
        const resultTool = request.tools?.find((tool) => {
          const properties = tool.inputSchema.properties;
          return (
            typeof properties === "object" &&
            properties !== null &&
            !Array.isArray(properties) &&
            Object.hasOwn(properties, "summary")
          );
        });
        if (resultTool === undefined) {
          throw new Error("missing result tool");
        }
        return {
          content: [
            {
              type: "tool_request",
              requestId: "bad-result",
              name: resultTool.name,
              arguments: invalidPayload as unknown as Record<string, never>,
            },
          ],
          finishReason: "tool_request",
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
          latencyMs: 0,
          route: fakeRoute,
        };
      },
    };

    await expect(runtime(gateway).run(workerJob())).rejects.toMatchObject({
      code: "invalid_output",
    });
  });
});

function findResultTool(request: ModelCompletionRequest) {
  return request.tools?.find((candidate) => {
    const properties = candidate.inputSchema.properties;
    return (
      typeof properties === "object" &&
      properties !== null &&
      !Array.isArray(properties) &&
      Object.hasOwn(properties, "summary") &&
      Object.hasOwn(properties, "proposedCommands")
    );
  });
}
