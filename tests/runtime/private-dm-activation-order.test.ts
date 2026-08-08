import { afterEach, describe, expect, it, vi } from "vitest";
import { FlorenceApplication, isNaturalPrivateGreeting } from "../../src/application/florence-application.js";
import type { Database } from "../../src/db/client.js";
import { WorkerAttemptError } from "../../src/modules/orchestration/bounded-worker-runtime.js";
import type { WorkerResult, WorkerRuntime } from "../../src/modules/orchestration/contracts.js";
import { GENERAL_ANSWER_SKILL, PRODUCT_SKILLS } from "../../src/modules/orchestration/skills.js";
import { PostgresSourceIntelligence } from "../../src/modules/sources/index.js";
import { FlorenceOrchestrator } from "../../src/runtime/orchestrator.js";

afterEach(() => vi.restoreAllMocks());

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
      ids: { responseOutboxId: "10000000-0000-4000-8000-000000000002" },
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
        requestingPerson: { id: "10000000-0000-4000-8000-000000000003", controlEpoch: 1 },
      }),
    });
    Object.defineProperty(orchestrator, "findActiveSourceChatResponse", {
      value: vi.fn().mockResolvedValue(null),
    });

    await expect(orchestrator.processLinqMessage(internalProviderEventId)).resolves.toEqual({
      kind: "responded",
      route: "source_chat",
      outboxEffectId: "10000000-0000-4000-8000-000000000002",
      duplicate: false,
    });
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
          disposition: "no_coverage_need",
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
    vi.spyOn(PostgresSourceIntelligence.prototype, "read").mockResolvedValue({
      kind: "source_revision",
      sourceRevisionId,
      sourceObjectId: "10000000-0000-4000-8000-000000000020",
      revisionNumber: 1,
      scopeDigest: "b".repeat(64),
      contentDigest: "c".repeat(64),
      content: {},
      occurredAt: new Date().toISOString(),
      capturedAt: new Date().toISOString(),
      retentionUntil: new Date(Date.now() + 86_400_000).toISOString(),
      accessExpiresAt: new Date(Date.now() + 3_600_000).toISOString(),
    });
    Object.defineProperties(application, {
      requireProcessedPrivateDmSource: {
        value: vi.fn().mockResolvedValue({
          record: {
            routing: {
              conversationId,
              senderPersonId: personId,
              participantEpochId: "10000000-0000-4000-8000-000000000016",
              appParticipantDigest: "a".repeat(64),
            },
          },
          snapshot: {
            conversationId,
            authorityVersion: 1,
            participantEpochId: "10000000-0000-4000-8000-000000000016",
            participantSetDigest: "a".repeat(64),
          },
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
      snapshot: {
        conversationId,
        authorityVersion: 1,
        participantEpochId: "10000000-0000-4000-8000-000000000016",
        participantSetDigest: "a".repeat(64),
      },
      requestingPerson: { id: personId, controlEpoch: 1 },
      household: null,
    };
    Object.defineProperties(orchestrator, {
      compileLinqContext: { value: vi.fn().mockResolvedValue(context) },
      findActiveSourceChatResponse: { value: vi.fn().mockResolvedValue(null) },
      loadReplyTargetCoverageLoopId: { value: vi.fn().mockResolvedValue(null) },
      tryExplicitCoverageResponse: { value: vi.fn().mockResolvedValue(null) },
      loadCurrentCoverageContext: { value: vi.fn().mockResolvedValue([]) },
    });

    await expect(orchestrator.processLinqMessage(internalProviderEventId)).resolves.toEqual({
      kind: "responded",
      route: "source_chat",
      outboxEffectId: "10000000-0000-4000-8000-000000000015",
      duplicate: false,
    });
    expect(queue).toHaveBeenCalledOnce();
    expect(queue.mock.calls[0]?.[5]).toBe("general_answer");
  });

  it("finishes a coverage no-op with a conversational reply", async () => {
    const internalProviderEventId = "10000000-0000-4000-8000-000000000017";
    const outboxEffectId = "10000000-0000-4000-8000-000000000018";
    const orchestrator = new FlorenceOrchestrator(
      null as never,
      { defaults: { rawSourceRetentionDays: 30 } } as never,
      null as never,
      { run: vi.fn(), reconcile: vi.fn() } as never,
    );
    const context = {
      row: { id: internalProviderEventId },
      record: { routing: { chatKind: "direct" } },
      requestingPerson: { id: "10000000-0000-4000-8000-000000000019", controlEpoch: 1 },
    };
    Object.defineProperties(orchestrator, {
      compileLinqContext: { value: vi.fn().mockResolvedValue(context) },
      orchestrateCurrentLinqMessage: {
        value: vi.fn().mockResolvedValue("coverage_change_assessed_no_reopen"),
      },
      findActiveSourceChatResponse: { value: vi.fn().mockResolvedValue(null) },
      answerGeneralQuestion: {
        value: vi.fn().mockResolvedValue({
          outboxEffectId,
          duplicate: false,
          route: "source_chat",
        }),
      },
    });

    await expect(orchestrator.processLinqMessage(internalProviderEventId)).resolves.toEqual({
      kind: "responded",
      route: "source_chat",
      outboxEffectId,
      duplicate: false,
    });
  });

  it("turns a specialist failure into a bounded private reply", async () => {
    const internalProviderEventId = "10000000-0000-4000-8000-000000000027";
    const outboxEffectId = "10000000-0000-4000-8000-000000000028";
    const orchestrator = new FlorenceOrchestrator(
      null as never,
      { defaults: { rawSourceRetentionDays: 30 } } as never,
      null as never,
      { run: vi.fn(), reconcile: vi.fn() } as never,
    );
    Object.defineProperties(orchestrator, {
      compileLinqContext: {
        value: vi.fn().mockResolvedValue({
          row: { id: internalProviderEventId },
          record: { routing: { chatKind: "direct" } },
          requestingPerson: { id: "10000000-0000-4000-8000-000000000029", controlEpoch: 1 },
        }),
      },
      orchestrateCurrentLinqMessage: {
        value: vi.fn().mockRejectedValue(
          new WorkerAttemptError({
            attemptId: "10000000-0000-4000-8000-000000000030",
            status: "failed",
            errorCode: "model_unavailable",
          }),
        ),
      },
      findActiveSourceChatResponse: { value: vi.fn().mockResolvedValue(null) },
      commitBoundedFailureResponse: {
        value: vi.fn().mockResolvedValue({
          kind: "responded",
          route: "source_chat",
          outboxEffectId,
          duplicate: false,
        }),
      },
    });

    await expect(orchestrator.processLinqMessage(internalProviderEventId)).resolves.toMatchObject({
      kind: "responded",
      outboxEffectId,
    });
  });

  it("fences household context supplied to a conversational answer", async () => {
    const personId = "10000000-0000-4000-8000-000000000021";
    const householdId = "10000000-0000-4000-8000-000000000022";
    const conversationId = "10000000-0000-4000-8000-000000000023";
    const run = vi.fn().mockResolvedValue(
      workerResult(GENERAL_ANSWER_SKILL, {
        answer: "Keep the current school routine.",
        uncertainty: null,
      }),
    );
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
      {
        process: vi.fn().mockResolvedValue({
          accepted: true,
          duplicate: false,
          disposition: "private_dm_response_queued",
          ids: { responseOutboxId: "10000000-0000-4000-8000-000000000025" },
        }),
      },
    );
    const context = {
      row: { id: "10000000-0000-4000-8000-000000000024" },
      record: {
        routing: {
          chatKind: "direct",
          senderPersonId: personId,
          conversationId,
        },
      },
      text: "ok what should we keep doing",
      evidenceSourceRevisionIds: [],
      images: [],
      snapshot: {
        conversationId,
        authorityVersion: 3,
        participantEpochId: "10000000-0000-4000-8000-000000000026",
        participantSetDigest: "d".repeat(64),
      },
      requestingPerson: { id: personId, controlEpoch: 5 },
      household: { id: householdId, controlEpoch: 7, timezone: "America/Los_Angeles" },
    };
    const householdContext = {
      householdId,
      representedChildren: [],
      activeRoutines: [],
      truncated: { representedChildren: false, activeRoutines: false },
    };

    await expect(
      (
        orchestrator as unknown as {
          answerGeneralQuestion(
            inputContext: unknown,
            authorizedHouseholdContext: unknown,
          ): Promise<{
            outboxEffectId: string;
          }>;
        }
      ).answerGeneralQuestion(context, householdContext),
    ).resolves.toMatchObject({ outboxEffectId: "10000000-0000-4000-8000-000000000025" });
    expect(run.mock.calls[0]?.[0]).toMatchObject({
      authority: {
        person: { id: personId, controlEpoch: 5 },
        household: { id: householdId, controlEpoch: 7 },
        conversation: { id: conversationId, authorityVersion: 3 },
      },
    });
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
