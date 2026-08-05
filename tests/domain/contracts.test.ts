import { describe, expect, it } from "vitest";
import {
  ActionDigestSchema,
  FamilyEpisodeSchema,
  HouseholdSignalSchema,
  NeutralDisplayTextSchema,
  NeutralFactualTextSchema,
  OutboxIntentSchema,
  WorkerProposalSchema,
} from "../../src/domain/index.js";
import {
  ADULT_A,
  DIGEST_A,
  episodeProposal,
  evidence,
  HOUSEHOLD_ID,
  T0,
  workerProposal,
} from "./fixtures.js";

describe("app-owned domain contracts", () => {
  it("rejects unknown fields at the signal envelope and nested proposal", () => {
    const valid = {
      householdId: HOUSEHOLD_ID,
      signalId: "signal_1",
      sequence: 1,
      occurredAt: T0,
      actor: { kind: "adult", adultId: ADULT_A },
      kind: "episode.proposed",
      proposal: episodeProposal(),
    } as const;

    expect(HouseholdSignalSchema.safeParse({ ...valid, providerThreadId: "provider_123" }).success).toBe(
      false,
    );
    expect(
      HouseholdSignalSchema.safeParse({
        ...valid,
        proposal: { ...valid.proposal, frameworkState: { messages: [] } },
      }).success,
    ).toBe(false);
  });

  it("keeps provider and framework identifiers out of worker proposals", () => {
    expect(
      WorkerProposalSchema.safeParse({
        ...workerProposal(),
        providerModel: "provider-specific-model",
        langGraphThreadId: "thread_1",
      }).success,
    ).toBe(false);
  });

  it("requires exact SHA-256 action digests", () => {
    expect(ActionDigestSchema.safeParse(DIGEST_A).success).toBe(true);
    expect(ActionDigestSchema.safeParse("provider-call-123").success).toBe(false);
  });

  it("rejects blame-bearing reminder text", () => {
    expect(NeutralFactualTextSchema.safeParse("The field-trip form is still open.").success).toBe(true);
    expect(NeutralFactualTextSchema.safeParse("You failed to submit the form.").success).toBe(false);
    expect(NeutralFactualTextSchema.safeParse("Alex still has not handled it.").success).toBe(false);
    expect(NeutralDisplayTextSchema.safeParse("Alex failed to submit the form.").success).toBe(false);
  });

  it("enforces commitment state and owner invariants", () => {
    const base = {
      episodeId: "episode_invalid",
      householdId: HOUSEHOLD_ID,
      type: "commitment",
      version: 1,
      scope: { kind: "household" },
      state: "active",
      title: "Submit the form",
      requiredOutcome: "The form is submitted",
      owner: { status: "proposed", adultId: ADULT_A, proposedAt: T0 },
      evidence: [evidence()],
      sourceClass: "school.notice",
      sensitivity: "ordinary",
      createdAt: T0,
      updatedAt: T0,
    } as const;

    expect(FamilyEpisodeSchema.safeParse(base).success).toBe(false);
    expect(
      FamilyEpisodeSchema.safeParse({
        ...base,
        state: "completed",
        owner: { ...base.owner, status: "acknowledged", acknowledgedAt: T0 },
      }).success,
    ).toBe(false);
  });

  it("does not allow an outbox action without an app-owned approval reference", () => {
    expect(
      OutboxIntentSchema.safeParse({
        intentId: "intent_1",
        householdId: HOUSEHOLD_ID,
        idempotencyKey: "idempotency_1",
        createdFromSignalId: "signal_1",
        kind: "execute_external_action",
        action: {
          actionId: "action_1",
          kind: "send_email",
          summary: "send the school email",
          actionDigest: DIGEST_A,
          relevantDataDigest: DIGEST_A,
          requestedFor: { kind: "household" },
          evidence: [evidence()],
        },
      }).success,
    ).toBe(false);
  });
});
