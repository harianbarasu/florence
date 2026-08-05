import { describe, expect, it } from "vitest";
import {
  CalendarEventDeletedInboxItemSchema,
  CalendarEventInboxItemSchema,
  CalendarTriageResultSchema,
  ConversationClassificationSchema,
  ConversationInboxItemSchema,
  GmailInboxItemSchema,
  GmailTriageResultSchema,
  WorkerCommandSchema,
} from "../../src/application/index.js";
import { PrivateSourceMatcherSchema } from "../../src/domain/index.js";
import { ADULT_A, classificationBase, HOUSEHOLD_ID } from "./fixtures.js";

describe("application-owned structured contracts", () => {
  it("accepts only digest-based app-owned private source matchers", () => {
    const matcher = {
      source: "gmail" as const,
      accountRefDigest: `sha256:${"a".repeat(64)}`,
      senderIdentityDigest: `sha256:${"b".repeat(64)}`,
    };
    expect(PrivateSourceMatcherSchema.safeParse(matcher).success).toBe(true);
    expect(
      PrivateSourceMatcherSchema.safeParse({
        ...matcher,
        accountRef: "raw-private-account",
      }).success,
    ).toBe(false);
    expect(
      PrivateSourceMatcherSchema.safeParse({
        ...matcher,
        sender: "raw-private-sender@example.com",
      }).success,
    ).toBe(false);
  });

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
        materialException: false,
        rationale: "The message has a current household consequence.",
        privateSummary: "A private school notice needs review.",
        minimumHouseholdMeaning: "A school form is due Friday.",
        title: "Return the school form",
        requiredOutcome: "The form is returned",
        rawProviderBody: "private mailbox content",
      }).success,
    ).toBe(false);
  });

  it("keeps inbound Calendar contracts provider-neutral and deletion identity-only", () => {
    const active = {
      kind: "calendar_event" as const,
      householdId: HOUSEHOLD_ID,
      idempotencyKey: "calendar:event:1:revision:4",
      occurredAt: "2027-01-01T08:00:00Z",
      ownerAdultId: ADULT_A,
      accountRef: "account-1",
      eventRef: "event-1",
      providerRef: "provider-event-1",
      revision: 4,
      contentDigest: `sha256:${"a".repeat(64)}`,
      title: "Private appointment",
      description: null,
      location: null,
      startsAt: "2027-01-02T17:00:00Z",
      endsAt: "2027-01-02T18:00:00Z",
      timeZone: "America/Los_Angeles",
      allDay: false,
      status: "confirmed" as const,
      recurrence: [],
    };
    expect(CalendarEventInboxItemSchema.safeParse(active).success).toBe(true);
    expect(
      CalendarEventInboxItemSchema.safeParse({
        ...active,
        attendees: [{ email: "private@example.com" }],
      }).success,
    ).toBe(false);
    expect(CalendarEventInboxItemSchema.safeParse({ ...active, htmlLink: "https://calendar" }).success).toBe(
      false,
    );
    expect(CalendarEventInboxItemSchema.safeParse({ ...active, providerEvent: {} }).success).toBe(false);

    const deleted = {
      kind: "calendar_event_deleted" as const,
      householdId: HOUSEHOLD_ID,
      idempotencyKey: "calendar:event:1:revision:5",
      occurredAt: "2027-01-01T09:00:00Z",
      ownerAdultId: ADULT_A,
      accountRef: "account-1",
      eventRef: "event-1",
      providerRef: "provider-event-1",
      revision: 5,
    };
    expect(CalendarEventDeletedInboxItemSchema.safeParse(deleted).success).toBe(true);
    expect(
      CalendarEventDeletedInboxItemSchema.safeParse({ ...deleted, title: "must not cross" }).success,
    ).toBe(false);
  });

  it("allows only minimum household content in Calendar triage proposals", () => {
    const proposal = {
      decision: "propose_family_episode" as const,
      confidence: 0.95,
      sourceClass: "calendar.school",
      sensitivity: "ordinary" as const,
      familyImpact: true,
      materialException: false,
      rationale: "The event needs household coordination.",
      privateSummary: "A private event needs coordination.",
      minimumHouseholdMeaning: "School has an evening event.",
      minimumRequiredOutcome: "The family plan accounts for the school event.",
    };
    expect(CalendarTriageResultSchema.safeParse(proposal).success).toBe(true);
    const { materialException: _materialException, ...withoutMaterialException } = proposal;
    expect(CalendarTriageResultSchema.safeParse(withoutMaterialException).success).toBe(false);
    expect(CalendarTriageResultSchema.safeParse({ ...proposal, rawTitle: "Private title" }).success).toBe(
      false,
    );
    expect(CalendarTriageResultSchema.safeParse({ ...proposal, proposedOwnerAdultId: ADULT_A }).success).toBe(
      false,
    );
  });

  it("requires exact ordered Gmail attachment evidence and retains safe integrity failures", () => {
    const attachment = {
      reference: "gmail-attachment-1",
      kind: "unavailable" as const,
      mediaType: null,
      filename: "notice.pdf",
      sizeBytes: null,
      contentDigest: `sha256:${"b".repeat(64)}`,
      reason: "invalid_content" as const,
    };
    const message = {
      kind: "gmail_message" as const,
      householdId: HOUSEHOLD_ID,
      idempotencyKey: "gmail:event:attachment",
      occurredAt: "2027-01-01T08:00:00Z",
      ownerAdultId: ADULT_A,
      accountRef: "account-1",
      messageRef: "message-1",
      revision: 1,
      labels: ["INBOX"],
      attachmentRefs: [attachment.reference],
      attachmentContents: [attachment],
    };
    expect(GmailInboxItemSchema.safeParse(message).success).toBe(true);
    expect(GmailInboxItemSchema.safeParse({ ...message, attachmentRefs: ["different"] }).success).toBe(false);
    expect(GmailInboxItemSchema.safeParse({ ...message, attachmentContents: [] }).success).toBe(false);
    expect(
      GmailInboxItemSchema.safeParse({
        ...message,
        attachmentRefs: Array.from({ length: 21 }, (_, index) => `attachment-${index}`),
        attachmentContents: Array.from({ length: 21 }, (_, index) => ({
          ...attachment,
          reference: `attachment-${index}`,
        })),
      }).success,
    ).toBe(false);
    expect(
      GmailInboxItemSchema.safeParse({
        ...message,
        attachmentContents: [
          {
            ...attachment,
            kind: "link",
            url: "https://example.com",
          },
        ],
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
        attachmentContents: [],
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
    expect(
      ConversationClassificationSchema.safeParse({
        ...classificationBase,
        intent: "onboarding",
        action: "update_profile",
        profileFacts: [
          {
            category: "routine_anchor",
            subject: "School pickup",
            detail: "Pickup is at 3:15 PM on weekdays.",
          },
        ],
      }).success,
    ).toBe(false);
    expect(
      ConversationClassificationSchema.safeParse({
        ...classificationBase,
        intent: "onboarding",
        action: "update_profile",
        profileFacts: [
          {
            category: "routine_anchor",
            subject: "School pickup",
            detail: "Pickup is at 3:15 PM on weekdays.",
            timeZone: "America/Los_Angeles",
            localTime: "15:15",
            daysOfWeek: [1, 2, 3, 4, 5],
          },
        ],
      }).success,
    ).toBe(true);
  });
});
