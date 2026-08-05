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

describe("ModelApplicationInterpreter", () => {
  it("uses the classification profile and strict conversation schema", async () => {
    const gateway = new RecordingGateway({
      intent: "ignore",
      confidence: 0.98,
      rationale: "This is ordinary household conversation.",
    });
    const interpreter = new ModelApplicationInterpreter(gateway);

    await interpreter.interpretConversation(
      {
        kind: "conversation_message",
        householdId,
        idempotencyKey: "linq:event:1",
        occurredAt: "2027-01-01T08:00:00Z",
        channel: { channelId: "chat-1", scope: "household" },
        senderAdultId: adultId,
        messageRef: "message-1",
        text: "That game was fun",
        attachmentRefs: [],
        attachmentContents: [],
      },
      {
        onboarding: {
          phase: "active",
          initiatorAdultId: adultId,
          invitedAdultId: secondAdultId,
          consentedAdultIds: [adultId, secondAdultId],
          privateDmAdultIds: [adultId, secondAdultId],
          groupChannelId: "chat-1",
          profileConfirmedAdultIds: [adultId, secondAdultId],
        },
        sharedProfile: { facts: [] },
        openEpisodes: [],
        pendingPromotionIds: [],
        activePolicies: [],
      },
    );

    expect(gateway.calls[0]?.profile).toBe("classification_extraction");
    expect(gateway.calls[0]?.request.responseSchemaName).toBe("florence_conversation_classification");
    expect(JSON.stringify(gateway.calls[0]?.request.messages)).toContain("Ordinary conversation");
  });

  it("uses a separate private-processing route for Gmail", async () => {
    const gateway = new RecordingGateway({
      decision: "private_review",
      confidence: 0.9,
      sourceClass: "school.notice",
      sensitivity: "sensitive",
      familyImpact: true,
      rationale: "The notice has a current household consequence.",
      privateSummary: "A school response is needed.",
    });
    const interpreter = new ModelApplicationInterpreter(gateway);
    await interpreter.triageGmail(
      {
        kind: "gmail_message",
        householdId,
        idempotencyKey: "gmail:event:1",
        occurredAt: "2027-01-01T08:00:00Z",
        ownerAdultId: adultId,
        accountRef: "account-1",
        messageRef: "message-1",
        revision: 1,
        labels: ["INBOX"],
        subject: "School update",
        bodyText: "Private access code 9917",
        attachmentRefs: [],
      },
      { activeSharingRules: [] },
    );

    expect(gateway.calls[0]?.profile).toBe("private_processing");
    expect(gateway.calls[0]?.request.responseSchemaName).toBe("florence_private_gmail_triage");
    expect(JSON.stringify(gateway.calls[0]?.request.messages)).toContain("Private access code 9917");
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
        onboarding: {
          phase: "active",
          initiatorAdultId: adultId,
          invitedAdultId: secondAdultId,
          consentedAdultIds: [adultId, secondAdultId],
          privateDmAdultIds: [adultId, secondAdultId],
          groupChannelId: "chat-1",
          profileConfirmedAdultIds: [adultId, secondAdultId],
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
