import { describe, expect, it } from "vitest";
import { HouseholdChiefOfStaff } from "../../src/domain/index.js";
import { ADULT_A, aggregate, evidence, HOUSEHOLD_ID, signal, T0, timePlan } from "./fixtures.js";

function proposedCalendarEpisode() {
  const sourceEvidence = evidence(
    { kind: "personal", adultId: ADULT_A },
    {
      evidenceId: "evidence_calendar_revision_1",
      source: "calendar",
      sourceRef: "calendar_source_private_event",
      revision: 1,
    },
  );
  return HouseholdChiefOfStaff.accept({
    current: aggregate(),
    signal: signal({
      householdId: HOUSEHOLD_ID,
      signalId: "signal_calendar_episode",
      sequence: 1,
      occurredAt: T0,
      actor: { kind: "adult", adultId: ADULT_A },
      kind: "episode.proposed",
      proposal: {
        episodeId: "episode_calendar_private",
        type: "commitment",
        targetScope: { kind: "personal", adultId: ADULT_A },
        title: "Review the private Calendar item",
        requiredOutcome: "The private Calendar item is handled",
        proposedOwnerAdultId: ADULT_A,
        evidence: [sourceEvidence],
        sourceClass: "calendar.school",
        sensitivity: "sensitive",
        temporalPlan: timePlan(),
      },
    }),
  });
}

function supersedingEvidence(revision = 2) {
  return evidence(
    { kind: "personal", adultId: ADULT_A },
    {
      evidenceId: `evidence_calendar_revision_${revision}`,
      source: "calendar",
      sourceRef: "calendar_source_private_event",
      observedAt: "2026-01-01T09:00:00Z",
      revision,
    },
  );
}

describe("source-owned episode supersession", () => {
  it("supersedes a nonterminal source episode and cancels every pending timer", () => {
    const proposed = proposedCalendarEpisode();
    const result = HouseholdChiefOfStaff.accept({
      current: proposed.aggregate,
      signal: signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_calendar_revision_2",
        sequence: 2,
        occurredAt: "2026-01-01T09:00:00Z",
        actor: { kind: "source_adapter", source: "calendar" },
        kind: "episode.source_superseded",
        episodeId: "episode_calendar_private",
        baseEpisodeVersion: 1,
        supersedingEvidence: supersedingEvidence(),
      }),
    });

    expect(result.receipt.disposition).toBe("accepted");
    expect(result.aggregate.episodes[0]).toMatchObject({
      state: "superseded",
      version: 2,
      outcome: {
        kind: "superseded",
        evidence: [expect.objectContaining({ revision: 2 })],
      },
      temporalPlan: { triggers: [expect.objectContaining({ status: "skipped" })] },
    });
    expect(result.effects).toEqual([expect.objectContaining({ kind: "cancel_timer", timerId: "timer_1" })]);
  });

  it("rejects unauthorized actors, stale episode versions, and terminal reuse", () => {
    const proposed = proposedCalendarEpisode();
    const unauthorized = HouseholdChiefOfStaff.accept({
      current: proposed.aggregate,
      signal: signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_calendar_unauthorized",
        sequence: 2,
        occurredAt: "2026-01-01T09:00:00Z",
        actor: { kind: "source_adapter", source: "linq" },
        kind: "episode.source_superseded",
        episodeId: "episode_calendar_private",
        baseEpisodeVersion: 1,
        supersedingEvidence: supersedingEvidence(),
      }),
    });
    expect(unauthorized.receipt.reason).toBe("unauthorized_actor");

    const stale = HouseholdChiefOfStaff.accept({
      current: proposed.aggregate,
      signal: signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_calendar_stale",
        sequence: 2,
        occurredAt: "2026-01-01T09:00:00Z",
        actor: { kind: "source_adapter", source: "calendar" },
        kind: "episode.source_superseded",
        episodeId: "episode_calendar_private",
        baseEpisodeVersion: 99,
        supersedingEvidence: supersedingEvidence(),
      }),
    });
    expect(stale.receipt.reason).toBe("stale_episode_version");

    const superseded = HouseholdChiefOfStaff.accept({
      current: proposed.aggregate,
      signal: signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_calendar_superseded",
        sequence: 2,
        occurredAt: "2026-01-01T09:00:00Z",
        actor: { kind: "source_adapter", source: "calendar" },
        kind: "episode.source_superseded",
        episodeId: "episode_calendar_private",
        baseEpisodeVersion: 1,
        supersedingEvidence: supersedingEvidence(),
      }),
    });
    const terminal = HouseholdChiefOfStaff.accept({
      current: superseded.aggregate,
      signal: signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_calendar_terminal",
        sequence: 3,
        occurredAt: "2026-01-01T10:00:00Z",
        actor: { kind: "source_adapter", source: "calendar" },
        kind: "episode.source_superseded",
        episodeId: "episode_calendar_private",
        baseEpisodeVersion: 2,
        supersedingEvidence: {
          ...supersedingEvidence(3),
          evidenceId: "evidence_calendar_revision_3",
          observedAt: "2026-01-01T10:00:00Z",
        },
      }),
    });
    expect(terminal.receipt.reason).toBe("invalid_transition");
  });
});
