import { describe, expect, it } from "vitest";
import {
  type FamilyOnboardingHousehold,
  type FamilyOnboardingPolicyState,
  projectFamilyOnboardingStep,
} from "../../src/modules/relationships/family-onboarding.js";

const personId = "10000000-0000-4000-8000-000000000001";
const householdId = "10000000-0000-4000-8000-000000000002";
const membershipId = "10000000-0000-4000-8000-000000000003";

function household(overrides: Partial<FamilyOnboardingHousehold> = {}): FamilyOnboardingHousehold {
  return {
    householdId,
    membershipId,
    membershipVersion: 1,
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

function state(overrides: Partial<FamilyOnboardingPolicyState> = {}): FamilyOnboardingPolicyState {
  const selectedHousehold = household();
  return {
    personId,
    profile: {
      displayName: "Hari",
      timezone: "America/Los_Angeles",
      authorityVersion: 1,
      controlEpoch: 1,
      reviewVersion: 1,
      onboardingVersion: 1,
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
    selectedHousehold,
    selectionIsStale: false,
    ...overrides,
  };
}

describe("family onboarding policy", () => {
  it("requires a durable explicit choice instead of selecting among multiple families", () => {
    const secondChoice = {
      householdId: "10000000-0000-4000-8000-000000000004",
      membershipId: "10000000-0000-4000-8000-000000000005",
      role: "steward" as const,
      timezone: "America/New_York",
    };
    expect(
      projectFamilyOnboardingStep(
        state({
          profile: { ...state().profile, selectedHouseholdId: null },
          householdChoices: [...state().householdChoices, secondChoice],
          selectedHousehold: null,
        }),
      ),
    ).toEqual({ kind: "choose_household" });
    expect(projectFamilyOnboardingStep(state({ selectionIsStale: true }))).toEqual({
      kind: "choose_household",
    });
  });

  it("requires an explicit selection even when only one family is available", () => {
    expect(
      projectFamilyOnboardingStep(
        state({
          profile: { ...state().profile, selectedHouseholdId: null },
          selectedHousehold: null,
        }),
      ),
    ).toEqual({ kind: "choose_household" });
  });

  it("orders the starter journey without making invitations gate setup", () => {
    expect(projectFamilyOnboardingStep(state()).kind).toBe("adults");
    expect(
      projectFamilyOnboardingStep(
        state({
          selectedHousehold: household({
            intakeVersion: 1,
            adultRosterReviewed: true,
            adultRosterReviewedByPersonId: personId,
            adults: [
              {
                id: "10000000-0000-4000-8000-000000000007",
                version: 1,
                displayName: "Kendall",
                role: "steward",
                matchedPersonId: null,
                invitationId: null,
                status: "not_invited",
              },
            ],
          }),
        }),
      ).kind,
    ).toBe("children");
    expect(
      projectFamilyOnboardingStep(
        state({
          selectedHousehold: household({
            intakeVersion: 2,
            adultRosterReviewed: true,
            adultRosterReviewedByPersonId: personId,
            adults: [
              {
                id: "10000000-0000-4000-8000-000000000007",
                version: 1,
                displayName: "Kendall",
                role: "steward",
                matchedPersonId: null,
                invitationId: null,
                status: "not_invited",
              },
            ],
            childRosterReviewed: true,
            childRosterReviewedByPersonId: personId,
          }),
        }),
      ).kind,
    ).toBe("google");
  });

  it("reuses one parent's roster and gives another adult only review plus private steps", () => {
    const shared = household({
      role: "steward",
      intakeVersion: 4,
      adultRosterReviewed: true,
      adultRosterReviewedByPersonId: "10000000-0000-4000-8000-000000000006",
      adults: [],
      childRosterReviewed: true,
      childRosterReviewedByPersonId: "10000000-0000-4000-8000-000000000006",
    });
    expect(projectFamilyOnboardingStep(state({ selectedHousehold: shared })).kind).toBe(
      "review_shared_context",
    );
    expect(
      projectFamilyOnboardingStep(
        state({
          selectedHousehold: {
            ...shared,
            sharedContextReviewed: true,
            membershipOnboardingVersion: 1,
          },
        }),
      ).kind,
    ).toBe("google");
    expect(
      projectFamilyOnboardingStep(
        state({
          selectedHousehold: {
            ...shared,
            sharedContextReviewed: true,
            membershipOnboardingVersion: 1,
            googleDecision: "limited",
          },
        }),
      ).kind,
    ).toBe("review");
  });

  it("keeps an early caregiver on a private branch until shared context is ready", () => {
    const earlyCaregiver = household({
      role: "caregiver",
      intakeVersion: 1,
      adultRosterReviewed: true,
      adultRosterReviewedByPersonId: "10000000-0000-4000-8000-000000000006",
      childRosterReviewed: false,
      childRosterReviewedByPersonId: null,
    });

    expect(projectFamilyOnboardingStep(state({ selectedHousehold: earlyCaregiver })).kind).toBe("google");
    expect(
      projectFamilyOnboardingStep(
        state({
          selectedHousehold: { ...earlyCaregiver, googleDecision: "limited" },
        }),
      ).kind,
    ).toBe("review");

    const sharedContextReady = {
      ...earlyCaregiver,
      childRosterReviewed: true,
      childRosterReviewedByPersonId: "10000000-0000-4000-8000-000000000006",
    };
    expect(projectFamilyOnboardingStep(state({ selectedHousehold: sharedContextReady })).kind).toBe(
      "review_shared_context",
    );
  });
});
