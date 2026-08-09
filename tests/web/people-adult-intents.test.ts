import { describe, expect, it } from "vitest";
import type { FamilyOnboardingProjection } from "../../src/modules/relationships/family-onboarding.js";
import { projectPeopleAdultIntents } from "../../src/server.js";
import type { PeopleView } from "../../src/web/api.js";

const personId = "10000000-0000-4000-8000-000000000001";
const householdId = "10000000-0000-4000-8000-000000000002";
const membershipId = "10000000-0000-4000-8000-000000000003";
const adultIntentId = "10000000-0000-4000-8000-000000000004";

describe("post-launch planned adults", () => {
  it("keeps an unconnected onboarding adult reachable on the selected family", () => {
    const people: PeopleView = {
      households: [
        {
          id: householdId,
          name: "Your family",
          status: "active",
          rosterVersion: 1,
          intakeVersion: 2,
          viewerRole: "steward",
          canInvite: true,
          canAddDependent: true,
          plannedAdults: [],
          members: [],
          eligibleParticipants: [],
        },
      ],
      invitations: [],
    };
    const projection: FamilyOnboardingProjection = {
      personId,
      profile: {
        displayName: "Jackson",
        timezone: "America/Los_Angeles",
        authorityVersion: 1,
        controlEpoch: 1,
        reviewVersion: 1,
        onboardingVersion: 1,
        selectedHouseholdId: householdId,
        remindersSent: 0,
        lastRemindedAt: null,
        remindersSuppressedAt: null,
        lastProgressedAt: "2026-08-09T12:00:00.000Z",
      },
      householdChoices: [
        {
          householdId,
          membershipId,
          role: "steward",
          timezone: "America/Los_Angeles",
        },
      ],
      household: {
        householdId,
        membershipId,
        membershipVersion: 1,
        role: "steward",
        timezone: "America/Los_Angeles",
        intakeVersion: 2,
        adultRosterReviewed: true,
        adultRosterReviewedByPersonId: personId,
        adults: [
          {
            id: adultIntentId,
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
        children: [],
        sharedContextReviewed: false,
        membershipOnboardingVersion: 1,
        googleDecision: "limited",
        completed: true,
      },
      nextStep: { kind: "complete", householdId },
    };

    expect(projectPeopleAdultIntents(people, projection).households[0]?.plannedAdults).toEqual([
      {
        id: adultIntentId,
        version: 1,
        displayName: "Kendall",
        role: "steward",
        matchedPersonId: null,
        progress: "not_connected",
      },
    ]);
  });
});
