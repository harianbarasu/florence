import { describe, expect, it } from "vitest";
import { projectGuidance } from "../../src/application/private-onboarding-guidance.js";

describe("private onboarding guidance", () => {
  const oneHousehold = [
    {
      householdId: "10000000-0000-4000-8000-000000000002",
      householdControlEpoch: 4,
      membershipVersion: 5,
      canGovern: true,
      canCoordinate: true,
    },
  ];
  const completeSetup = {
    dependentCount: 1,
    activeRoutineCount: 1,
  };

  it("chooses one current next step without selecting an arbitrary household", () => {
    const syncingGoogle = [
      {
        id: "10000000-0000-4000-8000-000000000001",
        status: "active" as const,
        controlEpoch: 2,
        informationCurrentControlEpoch: null,
        capabilities: ["calendar", "mail"],
      },
    ];
    expect(
      projectGuidance({
        personControlEpoch: 3,
        googleActivationSuppressed: false,
        memberships: oneHousehold,
        householdSetup: {
          dependentCount: 0,
          activeRoutineCount: 0,
        },
        integrations: syncingGoogle,
      }).recommendedNextStep,
    ).toEqual({ kind: "add_first_child", action: "people_handoff", returnPath: "/people" });

    expect(
      projectGuidance({
        personControlEpoch: 3,
        googleActivationSuppressed: false,
        memberships: [
          ...oneHousehold,
          {
            householdId: "10000000-0000-4000-8000-000000000003",
            householdControlEpoch: 1,
            membershipVersion: 1,
            canGovern: true,
            canCoordinate: true,
          },
        ],
        householdSetup: null,
        integrations: syncingGoogle,
      }).recommendedNextStep,
    ).toEqual({ kind: "choose_household", action: "none", returnPath: null });
  });

  it("aggregates every Google account conservatively", () => {
    const result = projectGuidance({
      personControlEpoch: 3,
      googleActivationSuppressed: false,
      memberships: oneHousehold,
      householdSetup: completeSetup,
      integrations: [
        {
          id: "10000000-0000-4000-8000-000000000001",
          status: "active",
          controlEpoch: 2,
          informationCurrentControlEpoch: 2,
          capabilities: ["calendar", "mail"],
        },
        {
          id: "10000000-0000-4000-8000-000000000004",
          status: "reauth_required",
          controlEpoch: 1,
          informationCurrentControlEpoch: null,
          capabilities: ["mail"],
        },
      ],
    });

    expect(result.currentWork).toBe("google_attention");
    expect(result.recommendedNextStep).toEqual({
      kind: "reconnect_google",
      action: "google_handoff",
      returnPath: "/sources",
    });
  });

  it("does not call all active accounts current while one is syncing", () => {
    const result = projectGuidance({
      personControlEpoch: 3,
      googleActivationSuppressed: false,
      memberships: oneHousehold,
      householdSetup: completeSetup,
      integrations: [
        {
          id: "10000000-0000-4000-8000-000000000001",
          status: "active",
          controlEpoch: 2,
          informationCurrentControlEpoch: 2,
          capabilities: ["mail"],
        },
        {
          id: "10000000-0000-4000-8000-000000000004",
          status: "active",
          controlEpoch: 3,
          informationCurrentControlEpoch: null,
          capabilities: ["calendar"],
        },
      ],
    });

    expect(result.currentWork).toBe("google_syncing");
    expect(result.recommendedNextStep).toEqual({
      kind: "wait_for_google",
      action: "none",
      returnPath: null,
    });
  });

  it("counts a partial active Google grant as connected", () => {
    const result = projectGuidance({
      personControlEpoch: 3,
      googleActivationSuppressed: false,
      memberships: oneHousehold,
      householdSetup: completeSetup,
      integrations: [
        {
          id: "10000000-0000-4000-8000-000000000001",
          status: "active",
          controlEpoch: 2,
          informationCurrentControlEpoch: 2,
          capabilities: ["calendar"],
        },
      ],
    });

    expect(result.currentWork).toBe("google_current");
    expect(result.recommendedNextStep).toEqual({ kind: "ready", action: "none", returnPath: null });
  });
});
