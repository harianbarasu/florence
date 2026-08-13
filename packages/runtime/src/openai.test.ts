import type { WorkerInput } from "@florence/contracts";
import type OpenAI from "openai";
import { describe, expect, it, vi } from "vitest";
import { GatewayWorkerRuntime, type ImageAssetReader } from "./index.js";
import { OpenAIResponsesGateway } from "./openai.js";
import { WORKER_INSTRUCTIONS } from "./prompt.js";

const householdId = "11111111-1111-4111-8111-111111111111";
const jacksonId = "22222222-2222-4222-8222-222222222222";
const partnerId = "33333333-3333-4333-8333-333333333333";
const conversationId = "44444444-4444-4444-8444-444444444444";
const signalId = "55555555-5555-4555-8555-555555555555";
const assetId = "66666666-6666-4666-8666-666666666666";

const input: WorkerInput = {
  signal: {
    type: "conversation.message",
    signalId,
    householdId,
    occurredAt: "2026-08-12T18:00:00.000Z",
    idempotencyKey: `linq:${signalId}`,
    conversationId,
    audience: "group",
    authorityVersion: 1,
    participantSetDigest: "a".repeat(64),
    senderAdultId: jacksonId,
    text: "The field-trip form in this photo is due Friday.",
    images: [{ assetId, mimeType: "image/heic" }],
    replyToSignalId: null,
    source: {
      system: "linq-v3",
      providerEventId: "event-1",
      providerMessageId: "message-1",
    },
  },
  snapshot: {
    householdId,
    timeZone: "America/Los_Angeles",
    asOf: "2026-08-12T18:00:00.000Z",
    members: [
      {
        id: jacksonId,
        kind: "adult",
        role: "steward",
        displayName: "Jackson",
        relationship: "Parent",
        status: "verified",
        sourceSignalIds: [signalId],
      },
      {
        id: partnerId,
        kind: "adult",
        role: "steward",
        displayName: "Partner",
        relationship: "Co-parent",
        status: "verified",
        sourceSignalIds: [signalId],
      },
    ],
    conversation: {
      id: conversationId,
      audience: "group",
      authorityVersion: 1,
      participantSetDigest: "a".repeat(64),
      authorizedAdultIds: [jacksonId, partnerId],
      recentTurns: [],
    },
    memories: [],
    openEpisodes: [],
  },
};

describe("OpenAIResponsesGateway", () => {
  it("limits the model to semantic consent classification for one app-selected Calendar draft", () => {
    expect(WORKER_INSTRUCTIONS).toContain("only to classify an unambiguous natural-language request");
    expect(WORKER_INSTRUCTIONS).toContain("one privateCalendarApprovalCandidate exactly as displayed");
    expect(WORKER_INSTRUCTIONS).toContain("Never select among drafts, rewrite a draft");
    expect(WORKER_INSTRUCTIONS).toContain("application revalidates the stored draft");
  });

  it("sends bounded image context without provider state and rejects invalid output", async () => {
    const validOutput = {
      proposals: [
        {
          type: "propose_episode",
          title: "Field-trip form",
          outcome: "The field-trip form is submitted by Friday.",
          dueAt: "2026-08-14T17:00:00.000Z",
          suggestedOwnerAdultId: null,
          responseText: "The form is due Friday. Who can own it?",
          sourceSignalIds: [signalId],
        },
      ],
    };
    const parse = vi
      .fn()
      .mockResolvedValueOnce({
        id: "resp_1",
        model: "test-vision-model",
        output_parsed: validOutput,
        usage: { input_tokens: 20, output_tokens: 10 },
      })
      .mockResolvedValueOnce({
        id: "resp_2",
        model: "test-vision-model",
        output_parsed: { proposals: [{ type: "complete_episode" }] },
        usage: null,
      });
    const client = { responses: { parse } } as unknown as OpenAI;
    const reader: ImageAssetReader = {
      async read(request) {
        expect(request).toMatchObject({ householdId, signalId, image: { assetId } });
        return { mimeType: "image/jpeg", bytes: Uint8Array.from([1, 2, 3]) };
      },
    };
    const runtime = new GatewayWorkerRuntime(
      new OpenAIResponsesGateway({ apiKey: "test-key", model: "test-vision-model" }, client),
      reader,
    );

    await expect(runtime.deliberate(input)).resolves.toEqual(validOutput.proposals);
    const request = parse.mock.calls[0]?.[0] as Record<string, unknown>;
    expect(request).toMatchObject({ model: "test-vision-model", store: false, max_output_tokens: 4_000 });
    expect(request).not.toHaveProperty("tools");
    expect(request).not.toHaveProperty("previous_response_id");
    expect(JSON.stringify(request.input)).toContain("data:image/jpeg;base64,AQID");
    expect(JSON.stringify(request.input)).toContain(signalId);
    expect(request.text).toMatchObject({ format: { type: "json_schema", strict: true } });

    await expect(runtime.deliberate(input)).rejects.toMatchObject({
      category: "invalid_output",
      retryable: false,
    });

    const unavailableImage = new GatewayWorkerRuntime(
      new OpenAIResponsesGateway({ apiKey: "test-key", model: "test-vision-model" }, client),
      {
        async read() {
          throw Object.assign(new Error("Image expired"), { retryable: false });
        },
      },
    );
    await expect(unavailableImage.deliberate(input)).rejects.toMatchObject({
      category: "unsupported_capability",
      retryable: false,
    });
  });
});
