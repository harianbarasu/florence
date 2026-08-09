import { describe, expect, it } from "vitest";
import {
  familyInvitationApprovalReadiness,
  householdInvitationPromptApprovalGeneration,
  householdInvitationPromptIdempotencyKey,
  isHouseholdInvitationPromptIdempotencyKey,
} from "../../src/application/florence-application.js";

describe("family invitation approval policy", () => {
  it("waits for outstanding stewards and advances the exact prompt with approval authority", () => {
    const invitationId = "10000000-0000-4000-8000-000000000001";

    expect(familyInvitationApprovalReadiness(1)).toBe("waiting");
    expect(familyInvitationApprovalReadiness(0)).toBe("ready");

    const beforeNewSteward = householdInvitationPromptIdempotencyKey("enrollment", invitationId, 3);
    const afterNewStewardApproval = householdInvitationPromptIdempotencyKey("enrollment", invitationId, 4);

    expect(afterNewStewardApproval).not.toBe(beforeNewSteward);
    expect(isHouseholdInvitationPromptIdempotencyKey(afterNewStewardApproval, invitationId)).toBe(true);
    expect(householdInvitationPromptApprovalGeneration(afterNewStewardApproval, invitationId)).toBe(4);
    expect(
      isHouseholdInvitationPromptIdempotencyKey(`household-invitation:${invitationId}`, invitationId),
    ).toBe(false);
  });
});
