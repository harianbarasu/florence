import { describe, expect, it, vi } from "vitest";
import type { ApplicationInterpreterPort } from "../../src/application/index.js";
import {
  extractInviteeHandle,
  OnboardingAwareInterpreter,
} from "../../src/infrastructure/onboarding-interpreter.js";
import { ADULT_A, ADULT_B, directMessage, HOUSEHOLD_ID } from "../application/fixtures.js";

const MODEL_RESULT = { intent: "ignore", confidence: 1, rationale: "No explicit action." };
const CALENDAR_CONTEXT = {
  currentTime: "2026-08-05T16:00:00Z",
  householdTimeZone: "America/Los_Angeles",
  pendingCalendarActions: [],
} as const;

function model(): ApplicationInterpreterPort {
  return {
    interpretConversation: vi.fn(async () => MODEL_RESULT),
    triageGmail: vi.fn(async () => ({ decision: "ignore" })),
  };
}

describe("OnboardingAwareInterpreter", () => {
  it("creates an identity-backed invitation without exposing a guessed adult ID to the model", async () => {
    const fallback = model();
    const prepareInvitation = vi.fn(async () => ({
      invitationId: "invitation-1",
      householdId: HOUSEHOLD_ID,
      adultId: ADULT_B,
      expiresAt: "2026-08-12T16:00:00.000Z",
    }));
    const interpreter = new OnboardingAwareInterpreter(
      fallback,
      { prepareInvitation },
      () => new Date("2026-08-05T16:00:00Z"),
    );

    await expect(
      interpreter.interpretConversation(
        directMessage(
          "invite",
          "Please invite my partner at +1 (202) 555-0102",
          ADULT_A,
          "2026-08-05T16:00:00Z",
        ),
        {
          ...CALENDAR_CONTEXT,
          onboarding: {
            phase: "awaiting_invitation",
            initiatorAdultId: ADULT_A,
            consentedAdultIds: [ADULT_A],
            privateDmAdultIds: [ADULT_A],
            profileConfirmedAdultIds: [],
          },
          sharedProfile: { facts: [] },
          openEpisodes: [],
          pendingPromotionIds: [],
          activePolicies: [],
        },
      ),
    ).resolves.toMatchObject({
      intent: "onboarding",
      action: "invite_adult",
      invitedAdultId: ADULT_B,
    });
    expect(prepareInvitation).toHaveBeenCalledWith({
      householdId: HOUSEHOLD_ID,
      invitedByAdultId: ADULT_A,
      inviteeHandle: "+12025550102",
      expiresAt: "2026-08-12T16:00:00.000Z",
    });
    expect(fallback.interpretConversation).not.toHaveBeenCalled();
  });

  it("requires explicit consent without exposing pre-consent messages to the model", async () => {
    const fallback = model();
    const interpreter = new OnboardingAwareInterpreter(fallback, {
      prepareInvitation: vi.fn(),
    });
    const context = {
      ...CALENDAR_CONTEXT,
      onboarding: {
        phase: "awaiting_initiator_consent" as const,
        initiatorAdultId: ADULT_A,
        consentedAdultIds: [],
        privateDmAdultIds: [],
        profileConfirmedAdultIds: [],
      },
      sharedProfile: { facts: [] },
      openEpisodes: [],
      pendingPromotionIds: [],
      activePolicies: [],
    };

    await expect(
      interpreter.interpretConversation(
        directMessage("hello", "Hi Florence", ADULT_A, "2026-08-05T16:00:00Z"),
        context,
      ),
    ).resolves.toMatchObject({
      intent: "ignore",
      rationale: expect.stringContaining("not explicitly consented"),
    });
    expect(fallback.interpretConversation).not.toHaveBeenCalled();
    await expect(
      interpreter.interpretConversation(
        directMessage("consent", "I consent", ADULT_A, "2026-08-05T16:01:00Z"),
        context,
      ),
    ).resolves.toMatchObject({ intent: "onboarding", action: "consent" });
    expect(fallback.interpretConversation).not.toHaveBeenCalled();
  });

  it("keeps an invitee's pre-consent messages out of the model", async () => {
    const fallback = model();
    const interpreter = new OnboardingAwareInterpreter(fallback, { prepareInvitation: vi.fn() });
    await expect(
      interpreter.interpretConversation(
        directMessage("invitee-question", "What is this?", ADULT_B, "2026-08-05T16:00:00Z"),
        {
          ...CALENDAR_CONTEXT,
          onboarding: {
            phase: "awaiting_invitee_consent",
            initiatorAdultId: ADULT_A,
            invitedAdultId: ADULT_B,
            consentedAdultIds: [ADULT_A],
            privateDmAdultIds: [ADULT_A],
            profileConfirmedAdultIds: [],
          },
          sharedProfile: { facts: [] },
          openEpisodes: [],
          pendingPromotionIds: [],
          activePolicies: [],
        },
      ),
    ).resolves.toMatchObject({ intent: "ignore" });
    expect(fallback.interpretConversation).not.toHaveBeenCalled();
  });

  it("recognizes invitee consent, group registration, and explicit profile confirmation", async () => {
    const interpreter = new OnboardingAwareInterpreter(model(), { prepareInvitation: vi.fn() });
    const base = {
      ...CALENDAR_CONTEXT,
      sharedProfile: { facts: [] },
      openEpisodes: [],
      pendingPromotionIds: [],
      activePolicies: [],
    };
    await expect(
      interpreter.interpretConversation(
        directMessage("accept", "I accept and consent", ADULT_B, "2026-08-05T16:00:00Z"),
        {
          ...base,
          onboarding: {
            phase: "awaiting_invitee_consent",
            initiatorAdultId: ADULT_A,
            invitedAdultId: ADULT_B,
            consentedAdultIds: [ADULT_A],
            privateDmAdultIds: [ADULT_A],
            profileConfirmedAdultIds: [],
          },
        },
      ),
    ).resolves.toMatchObject({ action: "accept_invite" });

    const groupMessage = {
      kind: "conversation_message" as const,
      householdId: HOUSEHOLD_ID,
      idempotencyKey: "group-register",
      occurredAt: "2026-08-05T16:01:00Z",
      channel: { channelId: "family-group", scope: "household" as const },
      senderAdultId: ADULT_A,
      messageRef: "message-group-register",
      text: "Florence, connect this family group",
      attachmentRefs: [],
      attachmentContents: [],
    };
    await expect(
      interpreter.interpretConversation(groupMessage, {
        ...base,
        onboarding: {
          phase: "awaiting_group",
          initiatorAdultId: ADULT_A,
          invitedAdultId: ADULT_B,
          consentedAdultIds: [ADULT_A, ADULT_B],
          privateDmAdultIds: [ADULT_A, ADULT_B],
          profileConfirmedAdultIds: [],
        },
      }),
    ).resolves.toMatchObject({ action: "register_group" });
    await expect(
      interpreter.interpretConversation(
        { ...groupMessage, idempotencyKey: "profile", text: "I confirm the profile details" },
        {
          ...base,
          onboarding: {
            phase: "building_profile",
            initiatorAdultId: ADULT_A,
            invitedAdultId: ADULT_B,
            consentedAdultIds: [ADULT_A, ADULT_B],
            privateDmAdultIds: [ADULT_A, ADULT_B],
            groupChannelId: "family-group",
            profileConfirmedAdultIds: [],
          },
        },
      ),
    ).resolves.toMatchObject({ action: "confirm_profile" });
  });

  it("normalizes E.164 and iMessage email invite handles", () => {
    expect(extractInviteeHandle("invite +1 (415) 555-0123")).toBe("+14155550123");
    expect(extractInviteeHandle("invite Parent.Name@icloud.com")).toBe("parent.name@icloud.com");
    expect(extractInviteeHandle("invite my partner")).toBeNull();
  });
});
