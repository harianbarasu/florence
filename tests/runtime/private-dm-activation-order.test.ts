import { afterEach, describe, expect, it, vi } from "vitest";
import { FlorenceApplication, isNaturalPrivateGreeting } from "../../src/application/florence-application.js";
import { PostgresPrivateOnboardingGuidance } from "../../src/application/private-onboarding-guidance.js";
import type { Database } from "../../src/db/client.js";
import { PostgresWebAuth } from "../../src/modules/auth/postgres-web-auth.js";
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
    const householdId = "10000000-0000-4000-8000-000000000021";
    const guidance = {
      stateDigest: "e".repeat(64),
      currentWork: "google_syncing" as const,
      householdCount: 1,
      household: {
        id: householdId,
        controlEpoch: 4,
        dependentCount: 0,
        activeRoutineCount: 0,
      },
      recommendedNextStep: {
        kind: "add_first_child" as const,
        action: "people_handoff" as const,
        returnPath: "/people" as const,
      },
    };
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
          useRecommendedNextStep: true,
        }),
      );
    const database = vi.fn(async () => [{ created_at: new Date() }]) as unknown as Database;
    Object.assign(database, {
      begin: async (
        modeOrCallback: string | ((transaction: Database) => unknown),
        callback?: (transaction: Database) => unknown,
      ) => (typeof modeOrCallback === "function" ? modeOrCallback : callback)?.(database),
    });
    const application = new FlorenceApplication(
      database,
      {
        defaults: { rawSourceRetentionDays: 30 },
        security: { tokenKey: "test-token-key" },
        publicBaseUrl: "https://florence.test",
      } as never,
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
              senderIdentityId: "10000000-0000-4000-8000-000000000022",
              participantEpochId: "10000000-0000-4000-8000-000000000016",
              appParticipantDigest: "a".repeat(64),
              chatKind: "direct",
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
    vi.spyOn(PostgresPrivateOnboardingGuidance.prototype, "projectPrivateGuidance").mockResolvedValue(
      guidance,
    );
    vi.spyOn(PostgresWebAuth.prototype, "createHandoff").mockResolvedValue({
      handoffId: "10000000-0000-4000-8000-000000000023",
      token: "private-guidance-token",
      expiresAt: new Date(Date.now() + 600_000),
    });
    const workers: WorkerRuntime = {
      run: run as WorkerRuntime["run"],
      reconcile: vi.fn().mockResolvedValue(undefined),
    };
    const orchestrator = new FlorenceOrchestrator(
      database,
      { defaults: { rawSourceRetentionDays: 30 } } as never,
      null as never,
      workers,
      null,
      application,
      null,
      { projectPrivateGuidance: vi.fn().mockResolvedValue(guidance) },
    );
    const context = {
      row: { id: internalProviderEventId },
      record: {
        routing: {
          chatKind: "direct",
          senderPersonId: personId,
          conversationId,
          participantEpochId: "10000000-0000-4000-8000-000000000016",
          appParticipantDigest: "a".repeat(64),
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
      household: { id: householdId, controlEpoch: 4, timezone: "America/Los_Angeles" },
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
    const queuedResponse = queue.mock.calls[0];
    expect(queuedResponse?.[5]).toBe("general_answer");
    expect(queuedResponse?.[3]).toContain("https://florence.test/handoff/private-guidance-token");
    expect(queuedResponse?.[7]).toEqual(expect.any(Date));
    const authorizationExpiresAt = queuedResponse?.[7] as Date;
    expect(authorizationExpiresAt.getTime()).toBeLessThanOrEqual(Date.now() + 10 * 60_000);
    expect(queuedResponse?.[8]).toContain(
      `general-answer:${internalProviderEventId}:guidance:${guidance.stateDigest}:`,
    );
    expect(queuedResponse?.[10]).toEqual({ id: householdId, controlEpoch: 4 });
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

  it("turns Kendall's open-ended follow-up into one app-ranked next step", async () => {
    const personId = "10000000-0000-4000-8000-000000000021";
    const householdId = "10000000-0000-4000-8000-000000000022";
    const conversationId = "10000000-0000-4000-8000-000000000023";
    const run = vi.fn().mockResolvedValue(
      workerResult(GENERAL_ANSWER_SKILL, {
        answer: "Google is already reviewing your recent information. Next, let’s add your first child.",
        uncertainty: null,
        useRecommendedNextStep: true,
      }),
    );
    const workers: WorkerRuntime = {
      run: run as WorkerRuntime["run"],
      reconcile: vi.fn().mockResolvedValue(undefined),
    };
    const process = vi.fn().mockResolvedValue({
      accepted: true,
      duplicate: false,
      disposition: "private_dm_response_queued",
      ids: { responseOutboxId: "10000000-0000-4000-8000-000000000025" },
    });
    const compilePrivateQuestionContext = vi.fn();
    const projectPrivateGuidance = vi.fn().mockResolvedValue({
      stateDigest: "e".repeat(64),
      currentWork: "google_syncing",
      householdCount: 1,
      household: {
        id: householdId,
        controlEpoch: 7,
        dependentCount: 0,
        activeRoutineCount: 0,
      },
      recommendedNextStep: {
        kind: "add_first_child",
        action: "people_handoff",
        returnPath: "/people",
      },
    });
    const orchestrator = new FlorenceOrchestrator(
      null as never,
      { defaults: { rawSourceRetentionDays: 30 } } as never,
      null as never,
      workers,
      null,
      { process },
      { compilePrivateQuestionContext },
      { projectPrivateGuidance },
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
      ).answerGeneralQuestion(context, null),
    ).resolves.toMatchObject({ outboxEffectId: "10000000-0000-4000-8000-000000000025" });
    expect(run.mock.calls[0]?.[0]).toMatchObject({
      authority: {
        person: { id: personId, controlEpoch: 5 },
        household: { id: householdId, controlEpoch: 7 },
        conversation: { id: conversationId, authorityVersion: 3 },
      },
    });
    expect(compilePrivateQuestionContext).not.toHaveBeenCalled();
    expect(projectPrivateGuidance).toHaveBeenCalledWith({
      personId,
      expectedPersonControlEpoch: 5,
    });
    const answerJob = run.mock.calls[0]?.[0];
    expect(answerJob?.authorizedContext).toContain('"currentWork":"google_syncing"');
    expect(answerJob?.authorizedContext).toContain('"kind":"add_first_child"');
    expect(answerJob?.goal).toContain("lead like a Chief of Staff");
    expect(process).toHaveBeenCalledWith(
      expect.objectContaining({
        response: expect.objectContaining({
          sourceAuthorities: [],
          guidance: {
            stateDigest: "e".repeat(64),
            step: "add_first_child",
            useRecommendedNextStep: true,
          },
        }),
      }),
    );
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
