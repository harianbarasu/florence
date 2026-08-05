import { describe, expect, it } from "vitest";
import {
  ConversationClassificationSchema,
  ConversationInboxItemSchema,
  GmailTriageResultSchema,
  WorkerCommandSchema,
} from "../../src/application/index.js";
import { ADULT_A, classificationBase, HOUSEHOLD_ID } from "./fixtures.js";

describe("application-owned structured contracts", () => {
  it("rejects classifier metadata that is outside the conversation contract", () => {
    expect(
      ConversationClassificationSchema.safeParse({
        ...classificationBase,
        intent: "propose_commitment",
        title: "Return the field-trip form",
        requiredOutcome: "The signed form is returned",
        sourceClass: "school.form",
        sensitivity: "ordinary",
        providerToolCallId: "provider-call-123",
      }).success,
    ).toBe(false);
  });

  it("rejects raw provider content from a Gmail triage result", () => {
    expect(
      GmailTriageResultSchema.safeParse({
        decision: "propose_family_episode",
        confidence: 0.95,
        sourceClass: "school.notice",
        sensitivity: "sensitive",
        familyImpact: true,
        rationale: "The message has a current household consequence.",
        privateSummary: "A private school notice needs review.",
        minimumHouseholdMeaning: "A school form is due Friday.",
        title: "Return the school form",
        requiredOutcome: "The form is returned",
        rawProviderBody: "private mailbox content",
      }).success,
    ).toBe(false);
  });

  it("rejects framework-shaped worker commands and mismatched personal conversations", () => {
    expect(
      WorkerCommandSchema.safeParse({
        kind: "provider_tool_call",
        payload: { tool_call_id: "call-1", arguments: {} },
      }).success,
    ).toBe(false);
    expect(
      ConversationInboxItemSchema.safeParse({
        kind: "conversation_message",
        householdId: HOUSEHOLD_ID,
        idempotencyKey: "dm-cross-adult",
        occurredAt: "2027-01-01T08:00:00Z",
        channel: { channelId: "dm-alex", scope: "personal", adultId: ADULT_A },
        senderAdultId: "adult_bailey",
        messageRef: "message-1",
        text: "hello",
        attachmentRefs: [],
      }).success,
    ).toBe(false);
  });

  it("requires bounded facts only on an explicit shared-profile update", () => {
    expect(
      ConversationClassificationSchema.safeParse({
        ...classificationBase,
        intent: "onboarding",
        action: "update_profile",
      }).success,
    ).toBe(false);
    expect(
      ConversationClassificationSchema.safeParse({
        ...classificationBase,
        intent: "onboarding",
        action: "confirm_profile",
        profileFacts: [
          { category: "dependent", subject: "Maya", detail: "Maya is a child in the household." },
        ],
      }).success,
    ).toBe(false);
    expect(
      ConversationClassificationSchema.safeParse({
        ...classificationBase,
        intent: "onboarding",
        action: "update_profile",
        profileFacts: [
          { category: "dependent", subject: "Maya", detail: "Maya is a child in the household." },
        ],
      }).success,
    ).toBe(true);
  });
});
