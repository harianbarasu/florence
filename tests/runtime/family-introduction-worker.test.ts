import { describe, expect, it, vi } from "vitest";
import type { AppEnvelope, ProcessReceipt } from "../../src/application/contracts.js";
import type { WorkerJob, WorkerResult, WorkerRuntime } from "../../src/modules/orchestration/contracts.js";
import { familyIntroductionProposalSchema, PRODUCT_SKILLS } from "../../src/modules/orchestration/skills.js";
import { FlorenceOrchestrator } from "../../src/runtime/orchestrator.js";

const INTERNAL_EVENT_ID = "10000000-0000-4000-8000-000000000001";
const PERSON_ID = "10000000-0000-4000-8000-000000000002";
const CONVERSATION_ID = "10000000-0000-4000-8000-000000000003";
const SOURCE_REVISION_ID = "10000000-0000-4000-8000-000000000004";

describe("family introduction worker proposal", () => {
  it("keeps introduction output bounded to a trimmed human name and relationship role", () => {
    expect(
      familyIntroductionProposalSchema.parse({
        kind: "introduction",
        displayName: "  Kendall  ",
        role: "steward",
      }),
    ).toEqual({ kind: "introduction", displayName: "Kendall", role: "steward" });
    expect(
      familyIntroductionProposalSchema.safeParse({
        kind: "other",
        displayName: null,
        role: null,
      }).success,
    ).toBe(true);
    expect(
      familyIntroductionProposalSchema.safeParse({
        kind: "other",
        displayName: "Kendall",
        role: "steward",
      }).success,
    ).toBe(false);
    expect(
      familyIntroductionProposalSchema.safeParse({
        kind: "introduction",
        displayName: null,
        role: null,
      }).success,
    ).toBe(false);
    expect(
      familyIntroductionProposalSchema.safeParse({
        kind: "introduction",
        displayName: "K".repeat(81),
        role: "participant",
      }).success,
    ).toBe(false);
  });

  it("submits only bounded introduction meaning through the application mutation seam", async () => {
    const run = vi
      .fn()
      .mockResolvedValue(workerResult({ kind: "introduction", displayName: "Kendall", role: "steward" }));
    const reconcile = vi.fn().mockResolvedValue(undefined);
    const process = vi.fn().mockResolvedValue({
      accepted: true,
      duplicate: false,
      disposition: "family_introduction_invitation_queued",
      ids: {},
    });
    const orchestrator = introductionOrchestrator({ run, reconcile }, process);

    await expect(
      introductionMethod(orchestrator).tryFamilyIntroductionProposal(
        introductionContext(),
        "this is my wife Kendall",
      ),
    ).resolves.toBe("family_introduction_invitation_queued");

    const job = run.mock.calls[0]?.[0] as WorkerJob;
    expect(job.skill).toBe(PRODUCT_SKILLS.familyIntroduction);
    expect(job.skill.requestedCapabilities).toEqual([]);
    expect(job.authority).toEqual({
      person: { id: PERSON_ID, controlEpoch: 7 },
      conversation: { id: CONVERSATION_ID, authorityVersion: 11 },
    });
    expect(job.authorizedContext).toBe(
      "Exact registered sender's leading-Florence request: this is my wife Kendall\n" +
        "This is an observe-only group. Classify only the supplied request and propose no action.",
    );
    expect(process).toHaveBeenCalledWith({
      kind: "linq.family_introduction_proposal",
      internalProviderEventId: INTERNAL_EVENT_ID,
      sourceRevisionId: SOURCE_REVISION_ID,
      proposal: { displayName: "Kendall", role: "steward" },
    });
    expect(reconcile).toHaveBeenCalledWith("attempt-family-introduction", "accepted");
  });

  it.each([
    {
      label: "other classification",
      result: workerResult({ kind: "other", displayName: null, role: null }),
      reconciliation: "accepted",
    },
    {
      label: "model failure",
      result: workerResult(undefined, "failed", "model_failed"),
      reconciliation: "rejected",
    },
  ])("falls through to the private general answer after $label", async ({ result, reconciliation }) => {
    const run = vi.fn().mockResolvedValue(result);
    const reconcile = vi.fn().mockResolvedValue(undefined);
    const process = vi.fn();
    const orchestrator = introductionOrchestrator({ run, reconcile }, process);

    await expect(
      introductionMethod(orchestrator).tryFamilyIntroductionProposal(
        introductionContext(),
        "what did Kendall say about pickup?",
      ),
    ).resolves.toBeNull();

    expect(process).not.toHaveBeenCalled();
    expect(reconcile).toHaveBeenCalledWith("attempt-family-introduction", reconciliation);
  });
});

function introductionOrchestrator(
  workers: Pick<WorkerRuntime, "reconcile"> & { readonly run: ReturnType<typeof vi.fn> },
  process: ReturnType<typeof vi.fn>,
): FlorenceOrchestrator {
  return new FlorenceOrchestrator(
    null as never,
    { defaults: { rawSourceRetentionDays: 30 } } as never,
    null as never,
    workers as unknown as WorkerRuntime,
    null,
    {
      process: process as unknown as (input: AppEnvelope) => Promise<ProcessReceipt>,
    },
  );
}

function introductionMethod(orchestrator: FlorenceOrchestrator): {
  tryFamilyIntroductionProposal(context: never, requestText: string): Promise<string | null>;
} {
  return orchestrator as unknown as {
    tryFamilyIntroductionProposal(context: never, requestText: string): Promise<string | null>;
  };
}

function introductionContext(): never {
  return {
    row: { id: INTERNAL_EVENT_ID },
    requestingPerson: { id: PERSON_ID, controlEpoch: 7 },
    snapshot: { authorityVersion: 11 },
    record: { routing: { conversationId: CONVERSATION_ID } },
    sourceRevisionId: SOURCE_REVISION_ID,
  } as never;
}

function workerResult(
  proposal:
    | { kind: "introduction"; displayName: string; role: "steward" | "caregiver" | "participant" }
    | { kind: "other"; displayName: null; role: null }
    | undefined,
  status: WorkerResult["status"] = "proposed",
  errorCode?: string,
): WorkerResult {
  return {
    attemptId: "attempt-family-introduction",
    taskVersionId: "task-family-introduction",
    skillId: PRODUCT_SKILLS.familyIntroduction.id,
    skillVersion: PRODUCT_SKILLS.familyIntroduction.version,
    evaluationRelease: PRODUCT_SKILLS.familyIntroduction.evaluationRelease,
    runtimeRoute: "test",
    status,
    ...(proposal ? { proposal } : {}),
    ...(errorCode ? { errorCode } : {}),
    startedAt: new Date("2026-08-07T20:00:00.000Z"),
    completedAt: new Date("2026-08-07T20:00:01.000Z"),
  };
}
