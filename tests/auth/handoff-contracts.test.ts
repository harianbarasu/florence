import { describe, expect, it } from "vitest";
import {
  CreateHandoffInputSchema,
  HouseholdInvitationStepUpContextSchema,
  ONBOARDING_HANDOFF_TTL_SECONDS,
} from "../../src/modules/auth/contracts.js";
import { completedHandoffRedirect } from "../../src/server.js";

const baseInput = {
  personId: "10000000-0000-4000-8000-000000000001",
  privateIdentityId: "10000000-0000-4000-8000-000000000002",
  privateConversationId: "10000000-0000-4000-8000-000000000003",
  context: {},
};

describe("handoff expiry contracts", () => {
  it("permits a day-long onboarding handoff without extending other private handoffs", () => {
    expect(
      CreateHandoffInputSchema.safeParse({
        ...baseInput,
        purpose: "onboarding",
        expiresInSeconds: ONBOARDING_HANDOFF_TTL_SECONDS,
      }).success,
    ).toBe(true);
    expect(
      CreateHandoffInputSchema.safeParse({
        ...baseInput,
        purpose: "web_sign_in",
        expiresInSeconds: ONBOARDING_HANDOFF_TTL_SECONDS,
      }).success,
    ).toBe(false);
  });

  it("routes web sign-in only to an exact app-owned private control path", () => {
    const session = {
      sessionId: "10000000-0000-4000-8000-000000000004",
      personId: baseInput.personId,
      authenticationIdentityId: baseInput.privateIdentityId,
      sessionToken: "session-token",
      csrfToken: "csrf-token",
      idleExpiresAt: new Date("2026-08-08T02:00:00.000Z"),
      absoluteExpiresAt: new Date("2026-08-09T02:00:00.000Z"),
      assuranceKind: "base" as const,
      assuranceContext: { returnPath: "/sources" },
      assuranceExpiresAt: null,
    };

    expect(completedHandoffRedirect("web_sign_in", session)).toBe("/sources");
    expect(
      completedHandoffRedirect("web_sign_in", {
        ...session,
        assuranceContext: { returnPath: "/people" },
      }),
    ).toBe("/people");
    expect(
      completedHandoffRedirect("web_sign_in", {
        ...session,
        assuranceContext: { returnPath: "/sources?connected=1" },
      }),
    ).toBe("/people");
    expect(completedHandoffRedirect("web_sign_in", { ...session, assuranceContext: {} })).toBe("/people");
  });

  it("continues an exact family action on a dedicated confirmation page", () => {
    const session = {
      sessionId: "10000000-0000-4000-8000-000000000004",
      personId: baseInput.personId,
      authenticationIdentityId: baseInput.privateIdentityId,
      sessionToken: "session-token",
      csrfToken: "csrf-token",
      idleExpiresAt: new Date("2026-08-08T02:00:00.000Z"),
      absoluteExpiresAt: new Date("2026-08-09T02:00:00.000Z"),
      assuranceKind: "household_invitation" as const,
      assuranceContext: {
        action: "accept",
        householdId: "10000000-0000-4000-8000-000000000005",
        invitationId: "10000000-0000-4000-8000-000000000006",
        returnPath: "/people",
      },
      assuranceExpiresAt: new Date("2026-08-08T01:15:00.000Z"),
    };

    expect(completedHandoffRedirect("household_invitation", session)).toBe("/confirm-action");
  });

  it("returns an exact Google-link account-control handoff to the linking page", () => {
    const session = {
      sessionId: "10000000-0000-4000-8000-000000000004",
      personId: baseInput.personId,
      authenticationIdentityId: baseInput.privateIdentityId,
      sessionToken: "session-token",
      csrfToken: "csrf-token",
      idleExpiresAt: new Date("2026-08-08T02:00:00.000Z"),
      absoluteExpiresAt: new Date("2026-08-09T02:00:00.000Z"),
      assuranceKind: "account_controls" as const,
      assuranceContext: {
        action: "link_google_identity",
        returnPath: "/link-account",
      },
      assuranceExpiresAt: new Date("2026-08-08T01:15:00.000Z"),
    };

    expect(completedHandoffRedirect("account_controls", session)).toBe("/link-account");
    expect(
      completedHandoffRedirect("account_controls", {
        ...session,
        assuranceContext: { ...session.assuranceContext, unrelated: "value" },
      }),
    ).toBe("/safety?step_up=account_controls");
  });

  it("returns an exact Google-removal account-control handoff to safety", () => {
    const session = {
      sessionId: "10000000-0000-4000-8000-000000000004",
      personId: baseInput.personId,
      authenticationIdentityId: baseInput.privateIdentityId,
      sessionToken: "session-token",
      csrfToken: "csrf-token",
      idleExpiresAt: new Date("2026-08-08T02:00:00.000Z"),
      absoluteExpiresAt: new Date("2026-08-09T02:00:00.000Z"),
      assuranceKind: "account_controls" as const,
      assuranceContext: {
        action: "revoke_login_identity",
        identityId: "10000000-0000-4000-8000-000000000005",
        returnPath: "/safety",
      },
      assuranceExpiresAt: new Date("2026-08-08T01:15:00.000Z"),
    };

    expect(completedHandoffRedirect("account_controls", session)).toBe("/safety?step_up=account_controls");
  });

  it("round-trips an optional onboarding adult through exact invitation assurance", () => {
    const context = {
      action: "invite",
      householdId: "10000000-0000-4000-8000-000000000005",
      conversationId: "10000000-0000-4000-8000-000000000006",
      expectedParticipantEpochId: "10000000-0000-4000-8000-000000000007",
      expectedParticipantDigest: "a".repeat(64),
      inviteeIdentityId: "10000000-0000-4000-8000-000000000008",
      inviteePersonId: "10000000-0000-4000-8000-000000000009",
      proposedDisplayName: "Kendall",
      role: "steward",
    } as const;

    const generic = HouseholdInvitationStepUpContextSchema.parse(context);
    expect(generic.action).toBe("invite");
    if (generic.action !== "invite") throw new Error("Expected invitation assurance");
    expect(generic.onboardingAdultIntentId).toBe("");
    expect(generic.onboardingAdultIntentVersion).toBe("");
    expect(HouseholdInvitationStepUpContextSchema.parse(generic)).toEqual(generic);

    const onboarding = HouseholdInvitationStepUpContextSchema.parse({
      ...context,
      onboardingAdultIntentId: "10000000-0000-4000-8000-000000000010",
      onboardingAdultIntentVersion: "3",
    });
    expect(onboarding.action).toBe("invite");
    if (onboarding.action !== "invite") throw new Error("Expected onboarding invitation assurance");
    expect(HouseholdInvitationStepUpContextSchema.parse(onboarding)).toEqual(onboarding);
  });
});
