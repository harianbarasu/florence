import { describe, expect, it } from "vitest";
import { projectGuidance } from "../../src/application/private-onboarding-guidance.js";
import type {
  FamilyOnboardingHousehold,
  FamilyOnboardingProjection,
} from "../../src/modules/relationships/index.js";

const personId = "10000000-0000-4000-8000-000000000001";
const householdId = "10000000-0000-4000-8000-000000000002";
const membershipId = "10000000-0000-4000-8000-000000000003";

function household(overrides: Partial<FamilyOnboardingHousehold> = {}): FamilyOnboardingHousehold {
  return {
    householdId,
    membershipId,
    membershipVersion: 5,
    role: "steward",
    timezone: "America/Los_Angeles",
    intakeVersion: 0,
    adultRosterReviewed: false,
    adultRosterReviewedByPersonId: null,
    adults: [],
    childRosterReviewed: false,
    childRosterReviewedByPersonId: null,
    children: [],
    sharedContextReviewed: false,
    membershipOnboardingVersion: 0,
    googleDecision: null,
    completed: false,
    ...overrides,
  };
}

function projection(
  nextStep: FamilyOnboardingProjection["nextStep"],
  overrides: Partial<FamilyOnboardingProjection> = {},
): FamilyOnboardingProjection {
  const selected = household();
  return {
    personId,
    profile: {
      displayName: "Hari",
      timezone: "America/Los_Angeles",
      authorityVersion: 2,
      controlEpoch: 3,
      reviewVersion: 1,
      onboardingVersion: 4,
      selectedHouseholdId: householdId,
      remindersSent: 0,
      lastRemindedAt: null,
      remindersSuppressedAt: null,
      lastProgressedAt: "2026-08-08T12:00:00.000Z",
    },
    householdChoices: [
      {
        householdId,
        membershipId,
        role: "steward",
        timezone: "America/Los_Angeles",
      },
    ],
    household: selected,
    nextStep,
    ...overrides,
  };
}

describe("private onboarding guidance", () => {
  it("uses canonical family intake before optional Google for open-ended guidance", () => {
    const result = projectGuidance({
      personControlEpoch: 3,
      projection: projection({ kind: "adults", householdId, expectedVersion: 0 }),
      householdControlEpoch: 4,
    });

    expect(result.recommendedNextStep).toEqual({
      kind: "adults",
      action: "onboarding_handoff",
      returnPath: "/onboarding",
    });
    expect(result.currentWork).toBe("onboarding_incomplete");
  });

  it("hands every incomplete canonical step to the same resumable wizard", () => {
    const result = projectGuidance({
      personControlEpoch: 3,
      projection: projection({ kind: "children", householdId, expectedVersion: 2 }),
      householdControlEpoch: 4,
    });

    expect(result.recommendedNextStep).toEqual({
      kind: "children",
      action: "onboarding_handoff",
      returnPath: "/onboarding",
    });
  });

  it("does not append an onboarding action after the selected membership is complete", () => {
    const completeHousehold = household({ completed: true, membershipOnboardingVersion: 2 });
    const result = projectGuidance({
      personControlEpoch: 3,
      projection: projection({ kind: "complete", householdId }, { household: completeHousehold }),
      householdControlEpoch: 4,
    });

    expect(result.recommendedNextStep).toEqual({
      kind: "complete",
      action: "none",
      returnPath: null,
    });
    expect(result.currentWork).toBeNull();
  });
});
