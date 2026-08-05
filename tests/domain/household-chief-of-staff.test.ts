import { describe, expect, it } from "vitest";
import {
  type HouseholdAggregate,
  HouseholdChiefOfStaff,
  type HouseholdSignal,
} from "../../src/domain/index.js";
import {
  ADULT_A,
  ADULT_B,
  aggregate,
  episodeProposal,
  evidence,
  HOUSEHOLD_ID,
  signal,
  T0,
  timePlan,
  WORKER_JOB_ID,
  workerProposal,
} from "./fixtures.js";

function accept(current: HouseholdAggregate, next: HouseholdSignal) {
  return HouseholdChiefOfStaff.accept({ current, signal: next });
}

function proposeSignal(sequence = 1) {
  return signal({
    householdId: HOUSEHOLD_ID,
    signalId: `signal_propose_${sequence}`,
    sequence,
    occurredAt: T0,
    actor: { kind: "adult", adultId: ADULT_A },
    kind: "episode.proposed",
    proposal: episodeProposal(),
  });
}

describe("HouseholdChiefOfStaff interface", () => {
  it("is deterministic and does not mutate its input aggregate", () => {
    const current = aggregate();
    const next = proposeSignal();

    const first = accept(current, next);
    const second = accept(current, next);

    expect(first).toEqual(second);
    expect(current.episodes).toEqual([]);
    expect(first.receipt.disposition).toBe("accepted");
    expect(first.aggregate.episodes[0]?.state).toBe("awaiting_acknowledgement");
    expect(first.effects).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: "schedule_timer",
          temporalPlanVersion: 1,
          timerId: "timer_1",
        }),
      ]),
    );
  });

  it("rejects duplicate and out-of-order signals without advancing state", () => {
    const current = aggregate();
    const outOfOrder = accept(current, proposeSignal(2));

    expect(outOfOrder.receipt).toMatchObject({
      disposition: "rejected",
      reason: "out_of_order_signal",
      aggregateVersion: 0,
    });
    expect(outOfOrder.aggregate).toEqual(current);

    const accepted = accept(current, proposeSignal(1));
    const duplicate = accept(accepted.aggregate, proposeSignal(1));
    expect(duplicate.receipt.reason).toBe("duplicate_signal");
    expect(duplicate.aggregate).toEqual(accepted.aggregate);
    expect(duplicate.effects).toEqual([]);
  });

  it("treats delivery and silence as neither ownership nor approval", () => {
    const proposed = accept(aggregate(), proposeSignal());
    const delivery = accept(
      proposed.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_delivery",
        sequence: 2,
        occurredAt: "2026-01-01T08:01:00Z",
        actor: { kind: "source_adapter", source: "linq" },
        kind: "conversation.delivery_observed",
        conversationId: "conversation_group",
        episodeId: "episode_field_trip",
        deliveredAt: "2026-01-01T08:01:00Z",
      }),
    );

    expect(delivery.receipt.disposition).toBe("ignored");
    expect(delivery.aggregate.episodes[0]?.state).toBe("awaiting_acknowledgement");
    expect(delivery.aggregate.episodes[0]?.owner.status).toBe("proposed");

    const wrongAdult = accept(
      delivery.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_wrong_owner",
        sequence: 3,
        occurredAt: "2026-01-01T08:02:00Z",
        actor: { kind: "adult", adultId: ADULT_B },
        kind: "commitment.owner_acknowledged",
        episodeId: "episode_field_trip",
        baseEpisodeVersion: 1,
      }),
    );
    expect(wrongAdult.receipt.reason).toBe("owner_mismatch");
    expect(wrongAdult.aggregate.episodes[0]?.state).toBe("awaiting_acknowledgement");

    const acknowledged = accept(
      wrongAdult.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_owner_ack",
        sequence: 4,
        occurredAt: "2026-01-01T08:03:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "commitment.owner_acknowledged",
        episodeId: "episode_field_trip",
        baseEpisodeVersion: 1,
      }),
    );
    expect(acknowledged.aggregate.episodes[0]).toMatchObject({
      state: "active",
      version: 2,
      owner: { status: "acknowledged", adultId: ADULT_A },
    });

    const reassigned = accept(
      acknowledged.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_reassign",
        sequence: 5,
        occurredAt: "2026-01-01T08:04:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "commitment.owner_reassigned",
        episodeId: "episode_field_trip",
        baseEpisodeVersion: 2,
        proposedOwnerAdultId: ADULT_B,
      }),
    );
    expect(reassigned.aggregate.episodes[0]).toMatchObject({
      state: "awaiting_acknowledgement",
      version: 3,
      owner: { status: "proposed", adultId: ADULT_B },
    });

    const newOwnerAcknowledged = accept(
      reassigned.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_reassigned_ack",
        sequence: 6,
        occurredAt: "2026-01-01T08:05:00Z",
        actor: { kind: "adult", adultId: ADULT_B },
        kind: "commitment.owner_acknowledged",
        episodeId: "episode_field_trip",
        baseEpisodeVersion: 3,
      }),
    );
    expect(newOwnerAcknowledged.aggregate.episodes[0]).toMatchObject({
      state: "active",
      version: 4,
      owner: { status: "acknowledged", adultId: ADULT_B },
    });

    const blocked = accept(
      newOwnerAcknowledged.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_blocked",
        sequence: 7,
        occurredAt: "2026-01-01T08:06:00Z",
        actor: { kind: "adult", adultId: ADULT_B },
        kind: "episode.blocked",
        episodeId: "episode_field_trip",
        baseEpisodeVersion: 4,
        reason: "The school confirmation is still needed",
      }),
    );
    expect(blocked.aggregate.episodes[0]).toMatchObject({
      state: "blocked",
      version: 5,
      blockedReason: "The school confirmation is still needed",
    });

    const resumed = accept(
      blocked.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_resumed",
        sequence: 8,
        occurredAt: "2026-01-01T08:07:00Z",
        actor: { kind: "adult", adultId: ADULT_B },
        kind: "episode.resumed",
        episodeId: "episode_field_trip",
        baseEpisodeVersion: 5,
      }),
    );
    expect(resumed.aggregate.episodes[0]).toMatchObject({ state: "active", version: 6 });
    expect(resumed.aggregate.episodes[0]).not.toHaveProperty("blockedReason");
  });

  it("consumes a rejected ordered signal so one bad proposal cannot block the stream", () => {
    const result = accept(
      aggregate(),
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_unauthorized",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "worker", jobId: WORKER_JOB_ID },
        kind: "episode.proposed",
        proposal: episodeProposal(),
      }),
    );

    expect(result.receipt).toMatchObject({
      disposition: "rejected",
      reason: "unauthorized_actor",
      aggregateVersion: 1,
    });
    expect(result.aggregate.lastProcessedSequence).toBe(1);
    expect(result.aggregate.episodes).toEqual([]);
  });

  it("rejects stale worker results atomically", () => {
    const current = aggregate({ version: 3, lastProcessedSequence: 3, policyVersion: 2 });
    const result = accept(
      current,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_stale_worker",
        sequence: 4,
        occurredAt: T0,
        actor: { kind: "worker", jobId: WORKER_JOB_ID },
        kind: "worker.proposal_received",
        proposal: workerProposal({
          baseHouseholdVersion: 2,
          basePolicyVersion: 2,
          episodeProposals: [episodeProposal()],
        }),
      }),
    );

    expect(result.receipt.reason).toBe("stale_household_version");
    expect(result.aggregate.version).toBe(4);
    expect(result.aggregate.episodes).toEqual([]);
    expect(result.effects).toEqual([]);
  });

  it("reconciles valid worker proposals while retaining memory and policy output as candidates", () => {
    const householdEvidence = evidence();
    const result = accept(
      aggregate(),
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_worker_valid",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "worker", jobId: WORKER_JOB_ID },
        kind: "worker.proposal_received",
        proposal: workerProposal({
          episodeProposals: [episodeProposal()],
          memoryCandidates: [
            {
              candidateId: "memory_candidate_1",
              householdId: HOUSEHOLD_ID,
              proposedBy: { kind: "worker", jobId: WORKER_JOB_ID },
              kind: "preference",
              statement: "The household prefers early reminders",
              scope: { kind: "household" },
              sourceClass: "household.feedback",
              evidence: [householdEvidence],
              confidence: 0.9,
              sensitivity: "ordinary",
              validFrom: T0,
            },
          ],
          policyCandidates: [
            {
              candidateId: "policy_candidate_1",
              householdId: HOUSEHOLD_ID,
              proposedByJobId: WORKER_JOB_ID,
              basePolicyVersion: 0,
              rule: {
                kind: "timing",
                scope: { kind: "household" },
                localTime: "18:00",
                timeZone: "America/Los_Angeles",
              },
              direction: "narrowing",
              rationale: "Use the household preparation time",
              createdAt: T0,
            },
          ],
        }),
      }),
    );

    expect(result.receipt.disposition).toBe("accepted");
    expect(result.aggregate.episodes).toHaveLength(1);
    expect(result.aggregate.memoryCandidates).toHaveLength(1);
    expect(result.aggregate.memories).toEqual([]);
    expect(result.aggregate.policyCandidates).toHaveLength(1);
    expect(result.aggregate.policies).toEqual([]);
  });

  it("prevents workers from widening a private memory candidate", () => {
    const privateEvidence = evidence(
      { kind: "personal", adultId: ADULT_A },
      { evidenceId: "evidence_private" },
    );
    expect(() =>
      workerProposal({
        memoryCandidates: [
          {
            candidateId: "memory_candidate_private",
            householdId: HOUSEHOLD_ID,
            proposedBy: { kind: "worker", jobId: WORKER_JOB_ID },
            kind: "fact",
            statement: "A private fact",
            scope: { kind: "household" },
            sourceClass: "gmail.private",
            evidence: [privateEvidence],
            confidence: 0.9,
            sensitivity: "sensitive",
            validFrom: T0,
          },
        ],
      }),
    ).toThrow();
  });

  it("requires an explicit adult or verified effect receipt to close an episode", () => {
    const proposed = accept(aggregate(), proposeSignal());
    const workerClose = accept(
      proposed.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_worker_close",
        sequence: 2,
        occurredAt: "2026-01-01T09:00:00Z",
        actor: { kind: "worker", jobId: WORKER_JOB_ID },
        kind: "episode.closed",
        episodeId: "episode_field_trip",
        baseEpisodeVersion: 1,
        outcome: {
          kind: "completed",
          summary: "The form is submitted",
          evidence: [],
          recordedAt: "2026-01-01T09:00:00Z",
        },
      }),
    );

    expect(workerClose.receipt.reason).toBe("unauthorized_actor");
    expect(workerClose.aggregate.episodes[0]?.state).toBe("awaiting_acknowledgement");

    const adultClose = accept(
      workerClose.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_adult_close",
        sequence: 3,
        occurredAt: "2026-01-01T09:01:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "episode.closed",
        episodeId: "episode_field_trip",
        baseEpisodeVersion: 1,
        outcome: {
          kind: "completed",
          summary: "The form is submitted",
          evidence: [],
          recordedAt: "2026-01-01T09:01:00Z",
        },
      }),
    );
    expect(adultClose.aggregate.episodes[0]?.state).toBe("completed");
    expect(adultClose.effects).toEqual(
      expect.arrayContaining([expect.objectContaining({ kind: "cancel_timer", timerId: "timer_1" })]),
    );
  });

  it("requires effect provenance before an executor receipt can close an episode", () => {
    const proposed = accept(aggregate(), proposeSignal());
    const unverified = accept(
      proposed.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_effect_close_without_evidence",
        sequence: 2,
        occurredAt: "2026-01-01T09:00:00Z",
        actor: { kind: "source_adapter", source: "effect_executor" },
        kind: "episode.closed",
        episodeId: "episode_field_trip",
        baseEpisodeVersion: 1,
        outcome: {
          kind: "completed",
          summary: "The form is submitted",
          evidence: [],
          recordedAt: "2026-01-01T09:00:00Z",
        },
      }),
    );
    expect(unverified.receipt.reason).toBe("invalid_transition");

    const verified = accept(
      unverified.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_effect_close_verified",
        sequence: 3,
        occurredAt: "2026-01-01T09:01:00Z",
        actor: { kind: "source_adapter", source: "effect_executor" },
        kind: "episode.closed",
        episodeId: "episode_field_trip",
        baseEpisodeVersion: 1,
        outcome: {
          kind: "completed",
          summary: "The form is submitted",
          evidence: [
            evidence(
              { kind: "household" },
              {
                evidenceId: "evidence_effect_receipt",
                source: "effect",
                sourceRef: "receipt_effect_1",
                observedAt: "2026-01-01T09:01:00Z",
              },
            ),
          ],
          recordedAt: "2026-01-01T09:01:00Z",
        },
      }),
    );
    expect(verified.aggregate.episodes[0]?.state).toBe("completed");
  });

  it("revalidates timers and emits neutral, owner-free reminders", () => {
    const proposed = accept(aggregate(), proposeSignal());
    const fired = accept(
      proposed.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_timer",
        sequence: 2,
        occurredAt: "2026-01-02T09:00:00Z",
        actor: { kind: "source_adapter", source: "system_clock" },
        kind: "timer.fired",
        timerId: "timer_1",
        episodeId: "episode_field_trip",
        temporalPlanVersion: 1,
        triggerId: "trigger_1",
        firedAt: "2026-01-02T09:00:00Z",
      }),
    );

    expect(fired.receipt.disposition).toBe("accepted");
    const message = fired.effects.find((effect) => effect.kind === "send_message");
    expect(message).toMatchObject({ kind: "send_message", messageClass: "reminder" });
    if (message?.kind !== "send_message") {
      throw new Error("expected a message intent");
    }
    expect(message.body).not.toContain("Alex");
    expect(message.body).not.toMatch(/forgot|failed|blame/i);
    expect(message.body).toContain("Is it handled, or should we reassign it?");
  });

  it("rejects early and stale timers without sending", () => {
    const proposed = accept(aggregate(), proposeSignal());
    const early = accept(
      proposed.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_timer_early",
        sequence: 2,
        occurredAt: "2026-01-02T08:59:59Z",
        actor: { kind: "source_adapter", source: "system_clock" },
        kind: "timer.fired",
        timerId: "timer_1",
        episodeId: "episode_field_trip",
        temporalPlanVersion: 1,
        triggerId: "trigger_1",
        firedAt: "2026-01-02T08:59:59Z",
      }),
    );
    expect(early.receipt.reason).toBe("timer_not_due");
    expect(early.effects).toEqual([]);

    const stale = accept(
      early.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_timer_stale",
        sequence: 3,
        occurredAt: "2026-01-02T09:00:00Z",
        actor: { kind: "source_adapter", source: "system_clock" },
        kind: "timer.fired",
        timerId: "timer_1",
        episodeId: "episode_field_trip",
        temporalPlanVersion: 99,
        triggerId: "trigger_1",
        firedAt: "2026-01-02T09:00:00Z",
      }),
    );
    expect(stale.receipt.reason).toBe("stale_temporal_plan");
    expect(stale.effects).toEqual([]);
  });

  it("surfaces a missed useful window honestly", () => {
    const proposed = accept(
      aggregate(),
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_window_episode",
        sequence: 1,
        occurredAt: T0,
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "episode.proposed",
        proposal: episodeProposal({
          episodeId: "episode_window",
          temporalPlan: timePlan({
            planId: "plan_window",
            triggers: [
              {
                triggerId: "trigger_window",
                timerId: "timer_window",
                kind: "window_check",
                at: { kind: "instant", at: "2026-01-02T16:00:00Z" },
              },
            ],
          }),
        }),
      }),
    );
    const fired = accept(
      proposed.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_window_fired",
        sequence: 2,
        occurredAt: "2026-01-02T16:05:00Z",
        actor: { kind: "source_adapter", source: "system_clock" },
        kind: "timer.fired",
        timerId: "timer_window",
        episodeId: "episode_window",
        temporalPlanVersion: 1,
        triggerId: "trigger_window",
        firedAt: "2026-01-02T16:05:00Z",
      }),
    );

    const message = fired.effects.find((effect) => effect.kind === "send_message");
    expect(message).toMatchObject({ messageClass: "missed_window" });
    if (message?.kind !== "send_message") {
      throw new Error("expected a missed-window message");
    }
    expect(message.body).toContain("passed its last responsible moment");
    expect(message.body).not.toContain("Alex");
  });

  it("treats a timer for a closed episode as reevaluation-only", () => {
    const proposed = accept(aggregate(), proposeSignal());
    const closed = accept(
      proposed.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_close_before_timer",
        sequence: 2,
        occurredAt: "2026-01-01T09:00:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "episode.closed",
        episodeId: "episode_field_trip",
        baseEpisodeVersion: 1,
        outcome: {
          kind: "completed",
          summary: "The form is submitted",
          evidence: [],
          recordedAt: "2026-01-01T09:00:00Z",
        },
      }),
    );
    const timer = accept(
      closed.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_timer_after_close",
        sequence: 3,
        occurredAt: "2026-01-02T09:00:00Z",
        actor: { kind: "source_adapter", source: "system_clock" },
        kind: "timer.fired",
        timerId: "timer_1",
        episodeId: "episode_field_trip",
        temporalPlanVersion: 1,
        triggerId: "trigger_1",
        firedAt: "2026-01-02T09:00:00Z",
      }),
    );

    expect(timer.receipt).toMatchObject({
      disposition: "ignored",
      reason: "timer_no_longer_relevant",
    });
    expect(timer.effects).toEqual([]);
  });

  it("invalidates old timers when a temporal plan is replaced", () => {
    const proposed = accept(aggregate(), proposeSignal());
    const replaced = accept(
      proposed.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_replan",
        sequence: 2,
        occurredAt: "2026-01-01T10:00:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "episode.temporal_plan_replaced",
        episodeId: "episode_field_trip",
        baseEpisodeVersion: 1,
        plan: timePlan({
          planId: "plan_2",
          version: 2,
          deadline: { kind: "instant", at: "2026-01-03T17:00:00Z" },
          triggers: [
            {
              triggerId: "trigger_2",
              timerId: "timer_2",
              kind: "reminder",
              at: { kind: "instant", at: "2026-01-03T09:00:00Z" },
            },
          ],
        }),
      }),
    );

    expect(replaced.receipt.disposition).toBe("accepted");
    expect(replaced.effects).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ kind: "cancel_timer", timerId: "timer_1" }),
        expect.objectContaining({ kind: "schedule_timer", timerId: "timer_2" }),
      ]),
    );

    const stale = accept(
      replaced.aggregate,
      signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_old_timer",
        sequence: 3,
        occurredAt: "2026-01-02T09:00:00Z",
        actor: { kind: "source_adapter", source: "system_clock" },
        kind: "timer.fired",
        timerId: "timer_1",
        episodeId: "episode_field_trip",
        temporalPlanVersion: 1,
        triggerId: "trigger_1",
        firedAt: "2026-01-02T09:00:00Z",
      }),
    );
    expect(stale.receipt.reason).toBe("stale_temporal_plan");
    expect(stale.effects).toEqual([]);
  });
});
