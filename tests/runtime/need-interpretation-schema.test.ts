import { describe, expect, it } from "vitest";
import { needInterpretationSchema } from "../../src/modules/orchestration/skills.js";

const BASE_INTERPRETATION = {
  changedFact: null,
  evidence: [
    {
      sourceRevisionId: "10000000-0000-4000-8000-000000000001",
      support: "Exact admitted evidence",
    },
  ],
  sensitivity: "ordinary",
  timeFacts: [],
  uncertainties: [],
  priorLoopId: null,
  rationale: "No current coverage need was found.",
} as const;

describe("coverage need interpretation schema", () => {
  it("requires an outcome only when proposing coverage", () => {
    expect(
      needInterpretationSchema.safeParse({
        ...BASE_INTERPRETATION,
        disposition: "propose_coverage",
        requiredOutcome: null,
      }).success,
    ).toBe(false);
    expect(
      needInterpretationSchema.safeParse({
        ...BASE_INTERPRETATION,
        disposition: "propose_coverage",
        requiredOutcome: "Violet is picked up after school.",
      }).success,
    ).toBe(true);
    expect(
      needInterpretationSchema.safeParse({
        ...BASE_INTERPRETATION,
        disposition: "no_coverage_need",
        requiredOutcome: null,
      }).success,
    ).toBe(true);
  });
});
