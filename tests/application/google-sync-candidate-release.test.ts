import { describe, expect, it } from "vitest";
import { rankPrivateSourceReleaseCandidates } from "../../src/application/google-sync-coordinator.js";

const IDS = {
  highConfidence: "10000000-0000-4000-8000-000000000001",
  urgent: "10000000-0000-4000-8000-000000000002",
  recent: "10000000-0000-4000-8000-000000000003",
  stable: "10000000-0000-4000-8000-000000000004",
} as const;

describe("private source first-value release", () => {
  it("selects one stable winner independent of retry order and advances after resolution", () => {
    const candidates = [
      {
        candidateId: IDS.stable,
        confidence: 0.84,
        urgencyAt: null,
        proposedAt: 1_000,
      },
      {
        candidateId: IDS.recent,
        confidence: 0.84,
        urgencyAt: 5_000,
        proposedAt: 3_000,
      },
      {
        candidateId: IDS.urgent,
        confidence: 0.84,
        urgencyAt: 4_000,
        proposedAt: 2_000,
      },
      {
        candidateId: IDS.highConfidence,
        confidence: 0.9,
        urgencyAt: null,
        proposedAt: 500,
      },
    ] as const;

    expect(rankPrivateSourceReleaseCandidates(candidates)).toBe(IDS.highConfidence);
    expect(rankPrivateSourceReleaseCandidates([...candidates].reverse())).toBe(IDS.highConfidence);

    const afterWinnerResolved = candidates.filter(
      (candidate) => candidate.candidateId !== IDS.highConfidence,
    );
    expect(rankPrivateSourceReleaseCandidates(afterWinnerResolved)).toBe(IDS.urgent);
  });
});
