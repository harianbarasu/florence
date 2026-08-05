import type {
  ModelCompletionResult,
  ModelProviderAdapter,
  ModelRouteCapabilities,
  ModelRouteReference,
} from "../../src/models/index.js";
import type { WorkerJob, WorkerResultPayload } from "../../src/runtime/index.js";

export const allCapabilities: ModelRouteCapabilities = {
  structuredOutput: true,
  toolCalling: true,
  vision: true,
  documentUnderstanding: true,
  longContext: true,
  privateProcessing: true,
};

export const fakeRoute: ModelRouteReference = {
  routeId: "fake-tools",
  provider: "openai-compatible",
  model: "fake-model",
  version: "test",
};

export function modelResult(overrides: Partial<ModelCompletionResult> = {}): ModelCompletionResult {
  return {
    content: [{ type: "text", text: "ok" }],
    finishReason: "stop",
    usage: { inputTokens: 2, outputTokens: 1, totalTokens: 3 },
    latencyMs: 1,
    route: fakeRoute,
    ...overrides,
  };
}

export function providerAdapter(
  complete: ModelProviderAdapter["complete"],
  capabilities: ModelRouteCapabilities = allCapabilities,
): ModelProviderAdapter {
  return { route: fakeRoute, capabilities, complete };
}

export function workerJob(overrides: Partial<WorkerJob> = {}): WorkerJob {
  const now = Date.now();
  return {
    jobId: "job-1",
    attemptId: "attempt-1",
    householdId: "household-1",
    baseHouseholdVersion: 7,
    policyVersion: 3,
    objective: "Interpret the family obligation.",
    scopeGrant: {
      grantId: "grant-1",
      visibility: "household",
      purpose: "obligation interpretation",
      expiresAt: new Date(now + 60_000).toISOString(),
    },
    evidenceRefs: ["evidence-1"],
    capabilityIds: [],
    modelRouteId: fakeRoute.routeId,
    modelCapabilityProfile: "tool_planning",
    budget: {
      maxDurationMs: 30_000,
      maxModelCalls: 3,
      maxToolCalls: 0,
      maxOutputTokens: 1_000,
    },
    deadline: new Date(now + 60_000).toISOString(),
    outputContractRef: "worker-result/v1",
    allowedToolNames: [],
    ...overrides,
  };
}

export const workerPayload: WorkerResultPayload = {
  summary: "A signed form is due Friday.",
  evidenceRefs: ["evidence-1"],
  questions: [],
  warnings: [],
  proposedCommands: [
    {
      kind: "propose_commitment",
      payload: { outcome: "Return the signed form" },
    },
  ],
  confidence: 0.92,
};
