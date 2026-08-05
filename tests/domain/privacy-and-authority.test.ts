import { describe, expect, it } from "vitest";
import {
  CalendarEventCreateActionSchema,
  calendarEventCreateActionDigest,
  type HouseholdAggregate,
  HouseholdChiefOfStaff,
  type HouseholdSignal,
} from "../../src/domain/index.js";
import {
  ADULT_A,
  ADULT_B,
  aggregate,
  DIGEST_A,
  DIGEST_B,
  episodeProposal,
  evidence,
  HOUSEHOLD_ID,
  signal,
  T0,
  WORKER_JOB_ID,
  workerProposal,
} from "./fixtures.js";

function accept(current: HouseholdAggregate, next: HouseholdSignal) {
  return HouseholdChiefOfStaff.accept({ current, signal: next });
}

const GMAIL_MATCHER = {
  source: "gmail" as const,
  accountRefDigest: DIGEST_A,
  senderIdentityDigest: DIGEST_B,
};

describe("privacy and authority through the HouseholdChiefOfStaff interface", () => {
  it("does not let one adult mutate another adult's personal scope", () => {
    const personalEpisode = accept(
      aggregate(),
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_cross_adult_episode",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "adult", adultId: ADULT_B },
        kind: "episode.proposed",
        proposal: episodeProposal({
          episodeId: "episode_personal_alex",
          targetScope: { kind: "personal", adultId: ADULT_A },
          proposedOwnerAdultId: ADULT_A,
          temporalPlan: undefined,
        }),
      }),
    );
    expect(personalEpisode.receipt.reason).toBe("unauthorized_actor");

    const personalPolicy = accept(
      personalEpisode.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_cross_adult_policy",
        sequence: 2,
        occurredAt: "2026-01-01T08:01:00Z",
        actor: { kind: "adult", adultId: ADULT_B },
        kind: "policy.approved",
        policy: {
          policyId: "policy_personal_alex",
          householdId: HOUSEHOLD_ID,
          version: 1,
          status: "active",
          rule: {
            kind: "routing",
            scope: { kind: "personal", adultId: ADULT_A },
            sourceClass: "gmail.private",
            decision: "suppress",
          },
          approvedByAdultId: ADULT_B,
          approvedAt: "2026-01-01T08:01:00Z",
        },
      }),
    );
    expect(personalPolicy.receipt.reason).toBe("policy_invalid");
    expect(personalPolicy.aggregate.policies).toEqual([]);
  });

  it("requires the personal-scope adult to approve their own external action", () => {
    const current = aggregate({
      pendingActions: [
        {
          action: {
            actionId: "action_personal_alex",
            kind: "send_email",
            summary: "send the personal email",
            actionDigest: DIGEST_A,
            relevantDataDigest: DIGEST_B,
            requestedFor: { kind: "personal", adultId: ADULT_A },
            evidence: [evidence()],
          },
          state: "awaiting_approval",
          proposedAt: T0,
          updatedAt: T0,
        },
      ],
    });
    const result = accept(
      current,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_cross_adult_action_approval",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "adult", adultId: ADULT_B },
        kind: "approval.granted",
        approval: {
          approvalId: "approval_cross_adult_action",
          householdId: HOUSEHOLD_ID,
          grantedByAdultId: ADULT_B,
          target: {
            kind: "external_action",
            actionId: "action_personal_alex",
            actionDigest: DIGEST_A,
            relevantDataDigest: DIGEST_B,
          },
          policyVersion: 0,
          grantedAt: T0,
          expiresAt: "2026-01-01T09:00:00Z",
          status: "active",
        },
      }),
    );

    expect(result.receipt.reason).toBe("approval_invalid");
    expect(result.aggregate.approvals).toEqual([]);
    expect(result.aggregate.pendingActions[0]?.state).toBe("awaiting_approval");
    expect(result.effects).toEqual([]);
  });

  it("keeps personal evidence private by default", () => {
    const privateEvidence = evidence(
      { kind: "personal", adultId: ADULT_A },
      { evidenceId: "evidence_private_mail", source: "gmail" },
    );
    const result = accept(
      aggregate(),
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_private_without_authority",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "episode.proposed",
        proposal: episodeProposal({
          episodeId: "episode_private_mail",
          evidence: [privateEvidence],
          temporalPlan: undefined,
        }),
      }),
    );

    expect(result.receipt.reason).toBe("privacy_promotion_requires_authority");
    expect(result.aggregate.episodes).toEqual([]);
    expect(result.effects).toEqual([]);
  });

  it("honors an exact share-once approval and consumes it", () => {
    const privateEvidence = evidence(
      { kind: "personal", adultId: ADULT_A },
      { evidenceId: "evidence_share_once", source: "gmail" },
    );
    const approved = accept(
      aggregate(),
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_share_approval",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "approval.granted",
        approval: {
          approvalId: "approval_share_once",
          householdId: HOUSEHOLD_ID,
          grantedByAdultId: ADULT_A,
          target: {
            kind: "scope_promotion",
            from: { kind: "personal", adultId: ADULT_A },
            to: { kind: "household" },
            evidenceIds: [privateEvidence.evidenceId],
          },
          policyVersion: 0,
          grantedAt: T0,
          expiresAt: "2026-01-02T08:00:00Z",
          status: "active",
        },
      }),
    );
    const promoted = accept(
      approved.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_share_promote",
        sequence: 2,
        occurredAt: "2026-01-01T08:01:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "episode.proposed",
        proposal: episodeProposal({
          episodeId: "episode_promoted",
          evidence: [privateEvidence],
          temporalPlan: undefined,
          promotionAuthority: { kind: "approval", approvalId: "approval_share_once" },
        }),
      }),
    );

    expect(promoted.receipt.disposition).toBe("accepted");
    expect(promoted.aggregate.episodes[0]?.scope).toEqual({ kind: "household" });
    expect(promoted.aggregate.approvals[0]?.status).toBe("consumed");

    const reuse = accept(
      promoted.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_share_reuse",
        sequence: 3,
        occurredAt: "2026-01-01T08:02:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "episode.proposed",
        proposal: episodeProposal({
          episodeId: "episode_illegal_reuse",
          evidence: [privateEvidence],
          temporalPlan: undefined,
          promotionAuthority: { kind: "approval", approvalId: "approval_share_once" },
        }),
      }),
    );
    expect(reuse.receipt.reason).toBe("invalid_promotion_authority");
    expect(reuse.aggregate.episodes).toHaveLength(1);
  });

  it("lets only the granting adult revoke an unused approval", () => {
    const privateEvidence = evidence(
      { kind: "personal", adultId: ADULT_A },
      { evidenceId: "evidence_revoke_approval", source: "gmail" },
    );
    const granted = accept(
      aggregate(),
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_grant_revocable",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "approval.granted",
        approval: {
          approvalId: "approval_revocable",
          householdId: HOUSEHOLD_ID,
          grantedByAdultId: ADULT_A,
          target: {
            kind: "scope_promotion",
            from: { kind: "personal", adultId: ADULT_A },
            to: { kind: "household" },
            evidenceIds: [privateEvidence.evidenceId],
          },
          policyVersion: 0,
          grantedAt: T0,
          expiresAt: "2026-01-02T08:00:00Z",
          status: "active",
        },
      }),
    );
    const wrongAdult = accept(
      granted.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_revoke_wrong_adult",
        sequence: 2,
        occurredAt: "2026-01-01T08:01:00Z",
        actor: { kind: "adult", adultId: ADULT_B },
        kind: "approval.revoked",
        approvalId: "approval_revocable",
      }),
    );
    expect(wrongAdult.receipt.reason).toBe("approval_invalid");
    expect(wrongAdult.aggregate.approvals[0]?.status).toBe("active");

    const revoked = accept(
      wrongAdult.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_revoke_owner",
        sequence: 3,
        occurredAt: "2026-01-01T08:02:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "approval.revoked",
        approvalId: "approval_revocable",
      }),
    );
    expect(revoked.aggregate.approvals[0]?.status).toBe("revoked");

    const promotion = accept(
      revoked.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_use_revoked",
        sequence: 4,
        occurredAt: "2026-01-01T08:03:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "episode.proposed",
        proposal: episodeProposal({
          episodeId: "episode_revoked_approval",
          evidence: [privateEvidence],
          temporalPlan: undefined,
          promotionAuthority: { kind: "approval", approvalId: "approval_revocable" },
        }),
      }),
    );
    expect(promotion.receipt.reason).toBe("invalid_promotion_authority");
    expect(promotion.aggregate.episodes).toEqual([]);
  });

  it("applies an owner-approved sharing policy only to its exact class and sensitivity", () => {
    const policyResult = accept(
      aggregate(),
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_policy_approve",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "policy.approved",
        policy: {
          policyId: "policy_school_share",
          householdId: HOUSEHOLD_ID,
          version: 1,
          status: "active",
          rule: {
            kind: "sharing",
            from: { kind: "personal", adultId: ADULT_A },
            to: { kind: "household" },
            sourceClass: "school.notice",
            maximumSensitivity: "ordinary",
            sourceMatcher: GMAIL_MATCHER,
          },
          approvedByAdultId: ADULT_A,
          approvedAt: T0,
        },
      }),
    );
    const privateEvidence = evidence(
      { kind: "personal", adultId: ADULT_A },
      { evidenceId: "evidence_policy_match", source: "gmail" },
    );
    const ordinary = accept(
      policyResult.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_policy_match",
        sequence: 2,
        occurredAt: "2026-01-01T08:01:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "episode.proposed",
        proposal: episodeProposal({
          episodeId: "episode_policy_match",
          evidence: [privateEvidence],
          temporalPlan: undefined,
          sourceMatcher: GMAIL_MATCHER,
          promotionAuthority: {
            kind: "policy",
            policyId: "policy_school_share",
            policyVersion: 1,
          },
        }),
      }),
    );
    expect(ordinary.receipt.disposition).toBe("accepted");

    const wrongPrivateSource = accept(
      ordinary.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_policy_wrong_private_source",
        sequence: 3,
        occurredAt: "2026-01-01T08:02:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "episode.proposed",
        proposal: episodeProposal({
          episodeId: "episode_policy_wrong_private_source",
          evidence: [privateEvidence],
          temporalPlan: undefined,
          sourceMatcher: { ...GMAIL_MATCHER, senderIdentityDigest: DIGEST_A },
          promotionAuthority: {
            kind: "policy",
            policyId: "policy_school_share",
            policyVersion: 1,
          },
        }),
      }),
    );
    expect(wrongPrivateSource.receipt.reason).toBe("invalid_promotion_authority");

    const sensitive = accept(
      ordinary.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_policy_sensitive",
        sequence: 3,
        occurredAt: "2026-01-01T08:02:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "episode.proposed",
        proposal: episodeProposal({
          episodeId: "episode_policy_sensitive",
          evidence: [privateEvidence],
          sensitivity: "sensitive",
          temporalPlan: undefined,
          sourceMatcher: GMAIL_MATCHER,
          promotionAuthority: {
            kind: "policy",
            policyId: "policy_school_share",
            policyVersion: 1,
          },
        }),
      }),
    );
    expect(sensitive.receipt.reason).toBe("invalid_promotion_authority");
  });

  it("revokes sharing authority immediately for future processing", () => {
    const active = accept(
      aggregate(),
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_policy_active",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "policy.approved",
        policy: {
          policyId: "policy_revocable",
          householdId: HOUSEHOLD_ID,
          version: 1,
          status: "active",
          rule: {
            kind: "sharing",
            from: { kind: "personal", adultId: ADULT_A },
            to: { kind: "household" },
            sourceClass: "school.notice",
            maximumSensitivity: "sensitive",
            sourceMatcher: GMAIL_MATCHER,
          },
          approvedByAdultId: ADULT_A,
          approvedAt: T0,
        },
      }),
    );
    const revoked = accept(
      active.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_policy_revoke",
        sequence: 2,
        occurredAt: "2026-01-01T08:01:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "policy.revoked",
        policyId: "policy_revocable",
        expectedPolicyVersion: 1,
      }),
    );
    const privateEvidence = evidence(
      { kind: "personal", adultId: ADULT_A },
      { evidenceId: "evidence_after_revoke", source: "gmail" },
    );
    const after = accept(
      revoked.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_after_revoke",
        sequence: 3,
        occurredAt: "2026-01-01T08:02:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "episode.proposed",
        proposal: episodeProposal({
          episodeId: "episode_after_revoke",
          evidence: [privateEvidence],
          temporalPlan: undefined,
          sourceMatcher: GMAIL_MATCHER,
          promotionAuthority: {
            kind: "policy",
            policyId: "policy_revocable",
            policyVersion: 1,
          },
        }),
      }),
    );

    expect(revoked.aggregate.policyVersion).toBe(2);
    expect(after.receipt.reason).toBe("invalid_promotion_authority");
  });

  it("keeps worker memory as a private candidate until its owner explicitly promotes it", () => {
    const privateEvidence = evidence(
      { kind: "personal", adultId: ADULT_A },
      { evidenceId: "evidence_memory_private", source: "gmail" },
    );
    const candidate = accept(
      aggregate(),
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_memory_candidate",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "worker", jobId: WORKER_JOB_ID },
        kind: "worker.proposal_received",
        proposal: workerProposal({
          memoryCandidates: [
            {
              candidateId: "memory_candidate_owner",
              householdId: HOUSEHOLD_ID,
              proposedBy: { kind: "worker", jobId: WORKER_JOB_ID },
              kind: "routine",
              statement: "School notices are reviewed during the evening",
              scope: { kind: "personal", adultId: ADULT_A },
              sourceClass: "gmail.private",
              evidence: [privateEvidence],
              confidence: 0.92,
              sensitivity: "ordinary",
              validFrom: T0,
            },
          ],
        }),
      }),
    );
    expect(candidate.aggregate.memoryCandidates[0]?.scope).toEqual({
      kind: "personal",
      adultId: ADULT_A,
    });
    expect(candidate.aggregate.memories).toEqual([]);

    const withoutAuthority = accept(
      candidate.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_memory_without_approval",
        sequence: 2,
        occurredAt: "2026-01-01T08:01:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "memory.confirmed",
        memoryId: "memory_routine",
        candidateId: "memory_candidate_owner",
        targetScope: { kind: "household" },
      }),
    );
    expect(withoutAuthority.receipt.reason).toBe("privacy_promotion_requires_authority");
    expect(withoutAuthority.aggregate.memories).toEqual([]);

    const approved = accept(
      withoutAuthority.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_memory_approval",
        sequence: 3,
        occurredAt: "2026-01-01T08:02:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "approval.granted",
        approval: {
          approvalId: "approval_memory_promote",
          householdId: HOUSEHOLD_ID,
          grantedByAdultId: ADULT_A,
          target: {
            kind: "scope_promotion",
            from: { kind: "personal", adultId: ADULT_A },
            to: { kind: "household" },
            evidenceIds: [privateEvidence.evidenceId],
          },
          policyVersion: 0,
          grantedAt: "2026-01-01T08:02:00Z",
          expiresAt: "2026-01-01T09:02:00Z",
          status: "active",
        },
      }),
    );
    const promoted = accept(
      approved.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_memory_confirm",
        sequence: 4,
        occurredAt: "2026-01-01T08:03:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "memory.confirmed",
        memoryId: "memory_routine",
        candidateId: "memory_candidate_owner",
        targetScope: { kind: "household" },
        promotionAuthority: {
          kind: "approval",
          approvalId: "approval_memory_promote",
        },
      }),
    );

    expect(promoted.receipt.disposition).toBe("accepted");
    expect(promoted.aggregate.memoryCandidates).toEqual([]);
    expect(promoted.aggregate.memories[0]).toMatchObject({
      memoryId: "memory_routine",
      scope: { kind: "household" },
      promotionAuthority: { kind: "approval", approvalId: "approval_memory_promote" },
    });
    expect(promoted.aggregate.approvals[0]?.status).toBe("consumed");
  });

  it("keeps external actions pending until an exact approval arrives", () => {
    const householdEvidence = evidence();
    const proposed = accept(
      aggregate(),
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_action_proposal",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "worker", jobId: WORKER_JOB_ID },
        kind: "worker.proposal_received",
        proposal: workerProposal({
          actionProposals: [
            {
              action: {
                actionId: "action_send_email",
                kind: "send_email",
                summary: "send the field-trip email",
                actionDigest: DIGEST_A,
                relevantDataDigest: DIGEST_B,
                requestedFor: { kind: "household" },
                evidence: [householdEvidence],
              },
            },
          ],
        }),
      }),
    );

    expect(proposed.aggregate.pendingActions[0]?.state).toBe("awaiting_approval");
    expect(proposed.effects.some((effect) => effect.kind === "execute_external_action")).toBe(false);
    expect(proposed.effects).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ kind: "send_message", messageClass: "approval_request" }),
      ]),
    );

    const approved = accept(
      proposed.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_action_approval",
        sequence: 2,
        occurredAt: "2026-01-01T08:01:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "approval.granted",
        approval: {
          approvalId: "approval_send_email",
          householdId: HOUSEHOLD_ID,
          grantedByAdultId: ADULT_A,
          target: {
            kind: "external_action",
            actionId: "action_send_email",
            actionDigest: DIGEST_A,
            relevantDataDigest: DIGEST_B,
          },
          policyVersion: 0,
          grantedAt: "2026-01-01T08:01:00Z",
          expiresAt: "2026-01-01T09:01:00Z",
          status: "active",
        },
      }),
    );

    expect(approved.aggregate.pendingActions[0]?.state).toBe("executing");
    expect(approved.aggregate.approvals[0]?.status).toBe("consumed");
    expect(approved.effects).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: "execute_external_action",
          approvalId: "approval_send_email",
        }),
      ]),
    );
  });

  it("accepts Calendar creation only from a verified adult, never from an ephemeral worker", () => {
    const withoutDigest = {
      actionId: "action_calendar_school_night",
      kind: "calendar_update" as const,
      calendarActionVersion: 1 as const,
      operation: "create" as const,
      householdId: HOUSEHOLD_ID,
      summary: "create the approved household calendar event",
      relevantDataDigest: DIGEST_B,
      requestedFor: { kind: "household" as const },
      evidence: [evidence()],
      title: "School welcome night",
      startsAt: "2026-01-03T02:00:00Z",
      endsAt: "2026-01-03T03:00:00Z",
      timeZone: "America/Los_Angeles",
      requestedByAdultId: ADULT_A,
      availabilityAdultIds: [ADULT_A, ADULT_B],
      targetConnectionId: "connection_parent_calendar",
      calendarId: "primary" as const,
      hasConflict: false,
    };
    const action = CalendarEventCreateActionSchema.parse({
      ...withoutDigest,
      actionDigest: calendarEventCreateActionDigest(withoutDigest),
    });
    const worker = accept(
      aggregate(),
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_worker_calendar_action",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "worker", jobId: WORKER_JOB_ID },
        kind: "worker.proposal_received",
        proposal: workerProposal({ actionProposals: [{ action }] }),
      }),
    );
    expect(worker.receipt).toMatchObject({ disposition: "rejected", reason: "unauthorized_actor" });
    expect(worker.aggregate.pendingActions).toEqual([]);
    expect(worker.effects).toEqual([]);

    const adult = accept(
      aggregate(),
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_adult_calendar_action",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "external_action.proposed",
        action,
      }),
    );
    expect(adult.receipt.disposition).toBe("accepted");
    expect(adult.aggregate.pendingActions[0]).toMatchObject({ state: "awaiting_approval", action });
    expect(adult.effects).toEqual([
      expect.objectContaining({ kind: "send_message", messageClass: "approval_request" }),
    ]);
  });

  it("invalidates approval when action data changes", () => {
    const householdEvidence = evidence();
    const proposed = accept(
      aggregate(),
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_action_edit_proposal",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "worker", jobId: WORKER_JOB_ID },
        kind: "worker.proposal_received",
        proposal: workerProposal({
          actionProposals: [
            {
              action: {
                actionId: "action_edited",
                kind: "submit_form",
                summary: "submit the registration form",
                actionDigest: DIGEST_A,
                relevantDataDigest: DIGEST_B,
                requestedFor: { kind: "household" },
                evidence: [householdEvidence],
              },
            },
          ],
        }),
      }),
    );
    const mismatched = accept(
      proposed.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_action_edit_approval",
        sequence: 2,
        occurredAt: "2026-01-01T08:01:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "approval.granted",
        approval: {
          approvalId: "approval_edited",
          householdId: HOUSEHOLD_ID,
          grantedByAdultId: ADULT_A,
          target: {
            kind: "external_action",
            actionId: "action_edited",
            actionDigest: DIGEST_B,
            relevantDataDigest: DIGEST_B,
          },
          policyVersion: 0,
          grantedAt: "2026-01-01T08:01:00Z",
          expiresAt: "2026-01-01T09:01:00Z",
          status: "active",
        },
      }),
    );

    expect(mismatched.receipt.reason).toBe("action_digest_mismatch");
    expect(mismatched.aggregate.pendingActions[0]?.state).toBe("awaiting_approval");
    expect(mismatched.aggregate.approvals).toEqual([]);
    expect(mismatched.effects).toEqual([]);
  });

  it("records an uncertain external outcome without retry authority and accepts later reconciliation", () => {
    const householdEvidence = evidence();
    const approvedAggregate = aggregate({
      version: 2,
      lastProcessedSequence: 2,
      approvals: [
        {
          approvalId: "approval_reconcile",
          householdId: HOUSEHOLD_ID,
          grantedByAdultId: ADULT_A,
          target: {
            kind: "external_action",
            actionId: "action_reconcile",
            actionDigest: DIGEST_A,
            relevantDataDigest: DIGEST_B,
          },
          policyVersion: 0,
          grantedAt: "2026-01-01T08:00:00Z",
          expiresAt: "2026-01-01T09:00:00Z",
          status: "consumed",
        },
      ],
      pendingActions: [
        {
          action: {
            actionId: "action_reconcile",
            kind: "submit_form",
            summary: "submit the field-trip form",
            actionDigest: DIGEST_A,
            relevantDataDigest: DIGEST_B,
            requestedFor: { kind: "household" },
            evidence: [householdEvidence],
          },
          state: "executing",
          approvalId: "approval_reconcile",
          proposedAt: T0,
          updatedAt: "2026-01-01T08:01:00Z",
        },
      ],
    });
    const unknown = accept(
      approvedAggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_effect_unknown",
        sequence: 3,
        occurredAt: "2026-01-01T08:02:00Z",
        actor: { kind: "source_adapter", source: "effect_executor" },
        kind: "effect.receipt_received",
        receiptId: "receipt_unknown",
        actionId: "action_reconcile",
        actionDigest: DIGEST_A,
        outcome: "unknown",
        recordedAt: "2026-01-01T08:02:00Z",
      }),
    );

    expect(unknown.aggregate.pendingActions[0]?.state).toBe("unknown");
    expect(unknown.effects).toEqual([]);

    const reconciled = accept(
      unknown.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_effect_reconciled",
        sequence: 4,
        occurredAt: "2026-01-01T08:03:00Z",
        actor: { kind: "source_adapter", source: "effect_executor" },
        kind: "effect.receipt_received",
        receiptId: "receipt_success",
        actionId: "action_reconcile",
        actionDigest: DIGEST_A,
        outcome: "succeeded",
        recordedAt: "2026-01-01T08:03:00Z",
      }),
    );
    expect(reconciled.aggregate.pendingActions[0]?.state).toBe("succeeded");
    expect(reconciled.effects).toEqual([]);
  });
});
