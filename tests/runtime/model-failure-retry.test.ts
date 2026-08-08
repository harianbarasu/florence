import { describe, expect, it } from "vitest";
import {
  BoundedWorkerRuntime,
  requireWorkerProposal,
  WorkerAttemptError,
} from "../../src/modules/orchestration/bounded-worker-runtime.js";
import type { ModelGateway, WorkerJob } from "../../src/modules/orchestration/contracts.js";
import { PRODUCT_SKILLS } from "../../src/modules/orchestration/skills.js";
import { isDeterministicStandingProposalFailure } from "../../src/runtime/orchestrator.js";
import { StaleAuthorityError } from "../../src/shared/errors.js";
import { modelFailureSettlement, requireDurableConversationCompletion } from "../../src/worker.js";

const attemptId = "10000000-0000-4000-8000-000000000001";

function job(): WorkerJob<typeof PRODUCT_SKILLS.needInterpret.outputSchema> {
  return {
    attemptId,
    taskVersionId: "10000000-0000-4000-8000-000000000002",
    authority: {
      person: { id: "10000000-0000-4000-8000-000000000003", controlEpoch: 1 },
    },
    skill: PRODUCT_SKILLS.needInterpret,
    authorizedContext: "An admitted private Florence message.",
    goal: "Interpret the message.",
    deadline: new Date(Date.now() + 30_000),
    budget: { maxModelCalls: 1, maxOutputTokens: 1_000 },
  };
}

describe("durable model failure handling", () => {
  it("surfaces a transient gateway failure as a typed retryable worker failure", async () => {
    let observedMaxOutputTokens: number | null = null;
    const gateway: ModelGateway = {
      completeStructured: async (request) => {
        observedMaxOutputTokens = request.maxOutputTokens;
        throw new Error("temporary provider failure");
      },
    };
    const runtime = new BoundedWorkerRuntime(gateway, "test:model");

    const result = await runtime.run(job());

    expect(observedMaxOutputTokens).toBe(1_000);
    expect(() => requireWorkerProposal(result)).toThrowError(WorkerAttemptError);
    try {
      requireWorkerProposal(result);
    } catch (error) {
      expect(error).toMatchObject({
        attemptId,
        code: "worker_model_failed",
        retryable: true,
      });
    }
  });

  it("retries only while the durable attempt and deadline bounds leave room", () => {
    const failure = new WorkerAttemptError({
      attemptId,
      status: "failed",
      errorCode: "model_failed",
    });
    const now = new Date("2026-08-07T20:00:00.000Z");
    const base = {
      attemptCount: 2,
      maxAttempts: 5,
      deadlineAt: new Date("2026-08-07T20:01:00.000Z"),
    };

    expect(modelFailureSettlement(base, failure, now)).toBe("retry");
    expect(modelFailureSettlement({ ...base, attemptCount: 5 }, failure, now)).toBe("attention");
    expect(
      modelFailureSettlement({ ...base, deadlineAt: new Date("2026-08-07T20:00:02.000Z") }, failure, now),
    ).toBe("attention");
  });

  it("does not turn a transient standing-rule model failure into private-review fallback", () => {
    const failure = new WorkerAttemptError({
      attemptId,
      status: "failed",
      errorCode: "model_failed",
    });

    expect(isDeterministicStandingProposalFailure(failure)).toBe(false);
    expect(isDeterministicStandingProposalFailure(new StaleAuthorityError("authority changed"))).toBe(true);
  });

  it("keeps a reply job open until the provider accepts its outbox effect", async () => {
    const completion = {
      kind: "responded" as const,
      route: "source_chat" as const,
      outboxEffectId: "10000000-0000-4000-8000-000000000004",
      duplicate: false,
    };
    const database = (status: string) => (async () => [{ status }]) as never;

    await expect(
      requireDurableConversationCompletion(database("pending"), completion, "Conversation turn"),
    ).rejects.toThrow(/waiting for provider acceptance/iu);
    await expect(
      requireDurableConversationCompletion(database("submitted"), completion, "Conversation turn"),
    ).resolves.toBeUndefined();
  });
});
