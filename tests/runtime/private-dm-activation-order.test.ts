import { describe, expect, it, vi } from "vitest";
import { FlorenceApplication, isNaturalPrivateGreeting } from "../../src/application/florence-application.js";
import type { Database } from "../../src/db/client.js";
import type { WorkerResult, WorkerRuntime } from "../../src/modules/orchestration/contracts.js";
import { GENERAL_ANSWER_SKILL, PRODUCT_SKILLS } from "../../src/modules/orchestration/skills.js";
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

  it("answers an admitted private conversational turn without requiring question punctuation", async () => {
    const internalProviderEventId = "10000000-0000-4000-8000-000000000011";
    const personId = "10000000-0000-4000-8000-000000000012";
    const conversationId = "10000000-0000-4000-8000-000000000013";
    const sourceRevisionId = "10000000-0000-4000-8000-000000000014";
    const run = vi
      .fn()
      .mockResolvedValueOnce(
        workerResult(PRODUCT_SKILLS.needInterpret, {
          disposition: "ignore",
          requiredOutcome: null,
          changedFact: null,
          evidence: [{ sourceRevisionId, support: "The exact admitted private turn." }],
          sensitivity: "ordinary",
          timeFacts: [],
          uncertainties: [],
          priorLoopId: null,
          rationale: "This is conversation, not a coverage need.",
        }),
      )
      .mockResolvedValueOnce(
        workerResult(GENERAL_ANSWER_SKILL, {
          answer: "Next, tell me about the family details and routines you want me to help with.",
          uncertainty: null,
        }),
      );
    const database = vi.fn(async () => [{ created_at: new Date() }]) as unknown as Database;
    Object.assign(database, {
      begin: async (callback: (transaction: Database) => unknown) => callback(database),
    });
    const application = new FlorenceApplication(
      database,
      { defaults: { rawSourceRetentionDays: 30 } } as never,
      null as never,
    );
    const queue = vi.fn().mockResolvedValue({
      outboxId: "10000000-0000-4000-8000-000000000015",
      created: true,
    });
    Object.defineProperties(application, {
      requireProcessedPrivateDmSource: {
        value: vi.fn().mockResolvedValue({
          record: {
            routing: {
              conversationId,
              participantEpochId: "10000000-0000-4000-8000-000000000016",
              appParticipantDigest: "a".repeat(64),
            },
          },
          snapshot: {},
          personId,
          event: { message: { parts: [{ kind: "text", text: "ok what should we keep doing" }] } },
        }),
      },
      queueAuthorizedConversationMessage: { value: queue },
      queueParentGoogleActivationOffer: { value: vi.fn().mockResolvedValue(null) },
    });
    const workers: WorkerRuntime = {
      run: run as WorkerRuntime["run"],
      reconcile: vi.fn().mockResolvedValue(undefined),
    };
    const orchestrator = new FlorenceOrchestrator(
      null as never,
      { defaults: { rawSourceRetentionDays: 30 } } as never,
      null as never,
      workers,
      null,
      application,
    );
    const context = {
      row: { id: internalProviderEventId },
      record: {
        routing: {
          chatKind: "direct",
          senderPersonId: personId,
          conversationId,
        },
      },
      text: "ok what should we keep doing",
      sourceRevisionId,
      evidenceSourceRevisionIds: [sourceRevisionId],
      images: [],
      snapshot: { authorityVersion: 1 },
      requestingPerson: { id: personId, controlEpoch: 1 },
      household: null,
    };
    Object.defineProperties(orchestrator, {
      compileLinqContext: { value: vi.fn().mockResolvedValue(context) },
      loadReplyTargetCoverageLoopId: { value: vi.fn().mockResolvedValue(null) },
      tryExplicitCoverageResponse: { value: vi.fn().mockResolvedValue(null) },
      loadCurrentCoverageContext: { value: vi.fn().mockResolvedValue([]) },
    });

    await expect(orchestrator.processLinqMessage(internalProviderEventId)).resolves.toBe(
      "general_answer_queued",
    );
    expect(queue).toHaveBeenCalledOnce();
    expect(queue.mock.calls[0]?.[5]).toBe("general_answer");
  });
});

function workerResult<Output>(
  skill: { readonly id: string; readonly version: number; readonly evaluationRelease: string },
  proposal: Output,
): WorkerResult<Output> {
  return {
    attemptId: crypto.randomUUID(),
    taskVersionId: crypto.randomUUID(),
    skillId: skill.id,
    skillVersion: skill.version,
    evaluationRelease: skill.evaluationRelease,
    runtimeRoute: "test",
    status: "proposed",
    proposal,
    startedAt: new Date("2026-08-08T19:00:00.000Z"),
    completedAt: new Date("2026-08-08T19:00:01.000Z"),
  };
}
