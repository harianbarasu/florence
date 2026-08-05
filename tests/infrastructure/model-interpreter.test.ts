import { describe, expect, it } from "vitest";
import { AdultIdSchema, HouseholdIdSchema } from "../../src/domain/index.js";
import { ModelApplicationInterpreter } from "../../src/infrastructure/index.js";
import type {
  ModelCapabilityProfile,
  ModelCompletionRequest,
  ModelCompletionResult,
  ModelGateway,
} from "../../src/models/index.js";

class RecordingGateway implements ModelGateway {
  public readonly calls: Array<{ profile: ModelCapabilityProfile; request: ModelCompletionRequest }> = [];

  public constructor(private readonly value: unknown) {}

  public async complete(
    profile: ModelCapabilityProfile,
    request: ModelCompletionRequest,
  ): Promise<ModelCompletionResult> {
    this.calls.push({ profile, request });
    return {
      content: [{ type: "structured_result", value: this.value as never }],
      finishReason: "stop",
      usage: {},
      latencyMs: 1,
      route: { routeId: "test", provider: "openai", model: "test" },
    };
  }
}

const householdId = HouseholdIdSchema.parse("11111111-1111-4111-8111-111111111111");
const adultId = AdultIdSchema.parse("22222222-2222-4222-8222-222222222222");
const secondAdultId = AdultIdSchema.parse("33333333-3333-4333-8333-333333333333");
const calendarContext = {
  currentTime: "2027-01-01T08:00:00Z",
  householdTimeZone: "America/Los_Angeles",
  confirmedRoutineAnchors: [],
  activeMemories: [],
  pendingMemoryCandidates: [],
  pendingCalendarActions: [],
} as const;

describe("ModelApplicationInterpreter", () => {
  it("keeps Gmail binary evidence on the private multimodal route without duplicating bytes in JSON", async () => {
    const gateway = new RecordingGateway({
      decision: "private_review",
      confidence: 0.9,
      sourceClass: "school.notice",
      sensitivity: "sensitive",
      familyImpact: true,
      rationale: "The attachment may require a family response.",
      privateSummary: "A school attachment needs review.",
    });
    const interpreter = new ModelApplicationInterpreter(gateway);
    const dataBase64 = Buffer.from("private image bytes").toString("base64");
    await interpreter.triageGmail(
      {
        kind: "gmail_message",
        householdId,
        idempotencyKey: "gmail:event:binary",
        occurredAt: "2027-01-01T08:00:00Z",
        ownerAdultId: adultId,
        accountRef: "account-1",
        messageRef: "message-binary",
        revision: 1,
        labels: ["INBOX"],
        attachmentRefs: ["attachment-1"],
        attachmentContents: [
          {
            reference: "attachment-1",
            kind: "image",
            mediaType: "image/png",
            filename: "notice.png",
            sizeBytes: 19,
            dataBase64,
            contentDigest: `sha256:${"c".repeat(64)}`,
          },
        ],
      },
      { confirmedRoutineAnchors: [], activeMemories: [], activeSharingRules: [] },
    );

    expect(gateway.calls[0]?.profile).toBe("private_processing");
    const userParts = gateway.calls[0]?.request.messages[1]?.parts ?? [];
    expect(userParts[1]).toMatchObject({
      type: "image",
      mediaType: "image/png",
      alt: "notice.png",
      data: dataBase64,
    });
    expect(userParts[0]?.type === "text" ? userParts[0].text : "").not.toContain(dataBase64);
  });

  it("uses a private, provider-neutral route and context for inbound Calendar triage", async () => {
    const gateway = new RecordingGateway({
      decision: "retain_private",
      confidence: 0.91,
      sourceClass: "calendar.personal",
      sensitivity: "sensitive",
      familyImpact: false,
      rationale: "The event is private and has no family consequence.",
      privateSummary: "A private appointment remains private.",
    });
    const interpreter = new ModelApplicationInterpreter(gateway);
    await interpreter.triageCalendar(
      {
        kind: "calendar_event",
        householdId,
        idempotencyKey: "calendar:event:1",
        occurredAt: "2027-01-01T08:00:00Z",
        ownerAdultId: adultId,
        accountRef: "account-1",
        eventRef: "event-1",
        providerRef: "provider-event-1",
        revision: 1,
        contentDigest: `sha256:${"d".repeat(64)}`,
        title: "Private appointment",
        description: "Do not share this raw text",
        location: null,
        startsAt: "2027-01-02T17:00:00Z",
        endsAt: "2027-01-02T18:00:00Z",
        timeZone: "America/Los_Angeles",
        allDay: false,
        status: "confirmed",
        recurrence: [],
      },
      {
        currentTime: "2027-01-01T08:00:00Z",
        householdTimeZone: "America/Los_Angeles",
        activeMemories: [],
        activeSharingRules: [],
      },
    );

    expect(gateway.calls[0]?.profile).toBe("private_processing");
    expect(gateway.calls[0]?.request.responseSchemaName).toBe("florence_private_calendar_triage");
    const serialized = JSON.stringify(gateway.calls[0]?.request.messages);
    expect(serialized).toContain("triage_private_calendar_event");
    expect(serialized).toContain("untrusted evidence");
    expect(serialized).toContain("materialException");
    expect(serialized).toContain("America/Los_Angeles");
  });

  it("routes attachment bytes through the vision/document contract without duplicating them in JSON", async () => {
    const gateway = new RecordingGateway({
      intent: "ignore",
      confidence: 0.9,
      rationale: "The attachment does not establish a household action.",
    });
    const interpreter = new ModelApplicationInterpreter(gateway);
    const dataBase64 = Buffer.from("synthetic pdf").toString("base64");

    await interpreter.interpretConversation(
      {
        kind: "conversation_message",
        householdId,
        idempotencyKey: "linq:event:attachment",
        occurredAt: "2027-01-01T08:00:00Z",
        channel: { channelId: "chat-1", scope: "household" },
        senderAdultId: adultId,
        messageRef: "message-attachment",
        text: "What is this?",
        attachmentRefs: ["linq:attachment:1"],
        attachmentContents: [
          {
            reference: "linq:attachment:1",
            kind: "file",
            mediaType: "application/pdf",
            filename: "notice.pdf",
            sizeBytes: 13,
            dataBase64,
            contentDigest: `sha256:${"a".repeat(64)}`,
          },
        ],
      },
      {
        ...calendarContext,
        onboarding: {
          phase: "active",
          initiatorAdultId: adultId,
          invitedAdultId: secondAdultId,
          consentedAdultIds: [adultId, secondAdultId],
          privateDmAdultIds: [adultId, secondAdultId],
          groupChannelId: "chat-1",
          adultNames: [
            { adultId, displayName: "Hari" },
            { adultId: secondAdultId, displayName: "Partner" },
          ],
          profileConfirmedAdultIds: [adultId, secondAdultId],
          googleConnectedAdultIds: [adultId, secondAdultId],
        },
        sharedProfile: { facts: [] },
        openEpisodes: [],
        pendingPromotionIds: [],
        activePolicies: [],
      },
    );

    expect(gateway.calls[0]?.profile).toBe("vision_document");
    const userParts = gateway.calls[0]?.request.messages[1]?.parts ?? [];
    expect(userParts[1]).toMatchObject({
      type: "file",
      mediaType: "application/pdf",
      filename: "notice.pdf",
      data: dataBase64,
    });
    expect(userParts[0]?.type === "text" ? userParts[0].text : "").not.toContain(dataBase64);
  });
});
