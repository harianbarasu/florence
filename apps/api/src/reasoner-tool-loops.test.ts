import { access, readFile, stat } from "node:fs/promises";
import {
  type FamilyWorkOriginContext,
  type FamilyWorkStateV1,
  steerFamilyWorkState,
} from "@florence/database";
import {
  GoogleCalendarTransientError,
  GoogleWorkspaceError,
  type GoogleWorkspaceOperation,
  type GoogleWorkspaceResult,
} from "@florence/google";
import { describe, expect, test } from "vitest";
import { isContextOverflowError } from "./agent-loop.js";
import {
  BrowserbaseBrowserClient,
  type FlorenceBrowserObservation,
  type FlorenceBrowserOperation,
  KernelBrowserClient,
  kernelProfileName,
} from "./browser.js";
import {
  type FlorenceDecision,
  type FlorenceGoogleChangesAssessmentInput,
  type FlorenceHouseholdNextActionInput,
  type FlorencePrivateGoogleBatchInput,
  FlorenceReasoner,
  FlorenceReasonerError,
  type FlorenceReasonerInput,
  florenceGoogleChangesAssessmentDecisionSchema,
  florenceHouseholdBriefingInputSchema,
  florenceHouseholdSafeCandidateSchema,
  florencePrivateDocketCoordinationSchema,
  florencePrivateGoogleBatchDecisionSchema,
} from "./reasoner.js";
import type { FlorenceTelephonyOperation, FlorenceTelephonyResult } from "./telephony.js";

const NOW = "2026-08-27T20:00:00.000Z";
const PUBLIC_URL = "https://example.com/current-result";
const admittedReadAccounting = {
  settleSources() {},
};

function familyWorkOrigin(text: string, speaker = "adult-1"): FamilyWorkOriginContext {
  return {
    message: {
      sourceId: `source-${speaker}`,
      speaker,
      moveKind: "message",
      text,
      authoredText: text,
      voiceTranscriptPresent: false,
      reaction: null,
      images: [],
      replyToSourceId: null,
      occurredAt: NOW,
    },
    supersededMessages: [],
    replyTarget: null,
    currentDocuments: [],
  };
}

describe("Florence reasoner capability cutover", () => {
  test("the model can choose a native group mention and poll within one human-sized turn", async () => {
    const requests: Record<string, unknown>[] = [];
    const decision = ordinaryDecision();
    decision.conversation.bubbles = [];
    decision.conversation.nativeMoves = [
      { type: "mention", text: "Jackson, which night works best?", adultDisplayName: "Jackson" },
      {
        type: "poll",
        question: "Dinner this week?",
        options: ["Tuesday", "Thursday"],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          return fakeStream({ status: "completed", output_parsed: decision, output: [] });
        },
      },
    } as never);
    const input = foregroundInput();
    input.audience = "group";
    input.googleConnections = [
      { emailLabel: "Family", calendarAvailable: true, kind: "family", writesEnabled: false },
    ];

    const result = await reasoner.decide(input, inertReads());

    expect(result.conversation.nativeMoves).toEqual(decision.conversation.nativeMoves);
    expect(String(requests[0]?.instructions)).toContain("conversation.nativeMoves");
    expect(String(requests[0]?.instructions)).toContain("three physical sends");
  });

  test("native rich links and custom reactions stay grounded in exact research and Message sources", async () => {
    const decision = ordinaryDecision({ researchUrls: [PUBLIC_URL] });
    decision.conversation.bubbles = [];
    decision.conversation.nativeMoves = [
      { type: "rich_link", url: PUBLIC_URL },
      {
        type: "reaction",
        operation: "add",
        targetSourceId: "turn-1",
        partIndex: 0,
        reaction: { type: "custom", emoji: "🙌" },
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () =>
          fakeStream({
            status: "completed",
            output_parsed: decision,
            output: [completedWebSearch(PUBLIC_URL)],
          }),
      },
    } as never);

    const result = await reasoner.decide(foregroundInput(), inertReads());

    expect(result.conversation.nativeMoves).toEqual(decision.conversation.nativeMoves);
    expect(result.researchUrls).toEqual([PUBLIC_URL]);
  });

  test("a natural reaction can be the whole visible move for a low-content acknowledgement", async () => {
    const decision = ordinaryDecision();
    decision.conversation.reaction = "like";
    decision.conversation.bubbles = [];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => fakeStream({ status: "completed", output_parsed: decision, output: [] }),
      },
    } as never);

    await expect(reasoner.decide(foregroundInput(), inertReads())).resolves.toMatchObject({
      conversation: { reaction: "like", bubbles: [] },
    });
  });

  test("ordinary parent turns still cannot be totally silent", async () => {
    const decision = ordinaryDecision();
    decision.conversation.bubbles = [];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => fakeStream({ status: "completed", output_parsed: decision, output: [] }),
      },
    } as never);

    await expect(reasoner.decide(foregroundInput(), inertReads())).rejects.toMatchObject({
      code: "invalid_output",
      message: expect.stringContaining("no visible conversational move"),
    });
  });

  test("an unbacked future commitment is reviewed and repaired once in the same tool transcript", async () => {
    const input = foregroundInput();
    input.currentMessage.text = "Can you compare the options and tell me which one is best?";
    input.currentMessage.authoredText = input.currentMessage.text;
    const promised = ordinaryDecision({
      bubbleText: "I’ll compare them and get back to you with the best one.",
    });
    const repaired = ordinaryDecision({
      bubbleText: "I’m on it—I’ll compare them and bring the best option back here.",
    });
    repaired.familyWork = {
      operation: "create",
      workId: null,
      objective: "Compare the supplied options and report the best one with the reasons.",
      schedule: null,
      instruction: null,
      candidateIds: [],
    };
    const modelRequests: Record<string, unknown>[] = [];
    const reviewRequests: Record<string, unknown>[] = [];
    let modelTurn = 0;
    let reviewTurn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          modelRequests.push(request);
          modelTurn += 1;
          if (modelTurn === 1) {
            return fakeStream({
              status: "completed",
              output_parsed: null,
              output: [
                functionCall("commitment-context-read", "search_gmail", {
                  query: "supplied options",
                  limit: 3,
                }),
              ],
            });
          }
          const decision = modelTurn === 2 ? promised : repaired;
          return fakeStream({
            status: "completed",
            output_parsed: decision,
            output: [decisionMessage(`commitment-${modelTurn}`, decision)],
          });
        },
        parse: async (request: Record<string, unknown>) => {
          reviewRequests.push(request);
          reviewTurn += 1;
          return {
            status: "completed",
            output_parsed:
              reviewTurn === 1
                ? {
                    verdict: "repair",
                    reason: "Florence promises a later comparison without starting matching work.",
                  }
                : { verdict: "accept", reason: null },
            output: [],
          };
        },
      },
    } as never);

    const result = await reasoner.decide(input, inertReads());

    expect(result.familyWork).toEqual(repaired.familyWork);
    expect(modelRequests).toHaveLength(3);
    expect(reviewRequests).toHaveLength(2);
    expect(modelRequests.every((request) => !("max_tool_calls" in request))).toBe(true);
    const repairedTranscript = JSON.stringify(modelRequests[2]?.input);
    expect(repairedTranscript).toContain("commitment-context-read");
    expect(repairedTranscript).toContain("function_call_output");
    expect(repairedTranscript).toContain(promised.conversation.bubbles[0]?.text);
    expect(repairedTranscript).toContain("foreground_commitment_repair");
    const firstReview = JSON.stringify(reviewRequests[0]?.input);
    const secondReview = JSON.stringify(reviewRequests[1]?.input);
    expect(firstReview).toContain(input.currentMessage.text);
    expect(firstReview).toContain(promised.conversation.bubbles[0]?.text);
    expect(firstReview).toContain('\\"familyWork\\":null');
    expect(secondReview).toContain(repaired.familyWork.objective);
    expect(String(reviewRequests[0]?.instructions)).toContain("An unrelated mutation never backs");
    expect(String(reviewRequests[0]?.instructions)).toContain("conditional offer or capability statement");
  });

  test("a still-unbacked commitment is rejected after the one semantic repair", async () => {
    const promised = ordinaryDecision({
      bubbleText: "I’ll keep checking and let you know what I find.",
    });
    let modelTurns = 0;
    let reviewTurns = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => {
          modelTurns += 1;
          return fakeStream({
            status: "completed",
            output_parsed: promised,
            output: [decisionMessage(`still-unbacked-${modelTurns}`, promised)],
          });
        },
        parse: async () => {
          reviewTurns += 1;
          return {
            status: "completed",
            output_parsed: {
              verdict: "repair",
              reason: "Florence still promises later monitoring without matching durable work.",
            },
            output: [],
          };
        },
      },
    } as never);

    await expect(reasoner.decide(foregroundInput(), inertReads())).rejects.toMatchObject({
      code: "invalid_output",
      message: expect.stringContaining("without matching durable work"),
    });
    expect(modelTurns).toBe(2);
    expect(reviewTurns).toBe(2);
  });

  test("a reaction cannot replace the spoken acknowledgement for work Florence starts", async () => {
    const decision = ordinaryDecision();
    decision.conversation.bubbles = [];
    decision.conversation.nativeMoves = [
      {
        type: "reaction",
        operation: "add",
        targetSourceId: "turn-1",
        partIndex: 0,
        reaction: { type: "tapback", reaction: "like" },
      },
    ];
    decision.familyWork = {
      operation: "create",
      workId: null,
      objective: "Find three good dinner options for Saturday and compare them.",
      schedule: null,
      instruction: null,
      candidateIds: [],
    };
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => fakeStream({ status: "completed", output_parsed: decision, output: [] }),
      },
    } as never);

    await expect(reasoner.decide(foregroundInput(), inertReads())).rejects.toMatchObject({
      code: "invalid_output",
      message: expect.stringContaining("spoken acknowledgement"),
    });
  });

  test("a docket change needs a spoken acknowledgement and cannot duplicate tracked work", async () => {
    const reactionOnly = ordinaryDecision();
    reactionOnly.conversation.bubbles = [];
    reactionOnly.conversation.reaction = "like";
    reactionOnly.docketUpsert = {
      operation: "create",
      candidateId: null,
      candidate: {
        category: "deadline",
        summary: "The field-trip form still needs a signature.",
        urgency: "soon",
        dueAt: "2026-08-29T20:00:00.000Z",
        owner: "Hari",
        nextAction: "Sign the field-trip form.",
        waitingOn: "Hari's signature",
        needsAnswer: true,
      },
      sourceIds: ["turn-1"],
    };
    const reactionReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => fakeStream({ status: "completed", output_parsed: reactionOnly, output: [] }),
      },
    } as never);

    await expect(reactionReasoner.decide(foregroundInput(), inertReads())).rejects.toMatchObject({
      code: "invalid_output",
      message: expect.stringContaining("spoken acknowledgement bubble"),
    });

    const duplicateTracking = ordinaryDecision();
    duplicateTracking.docketUpsert = reactionOnly.docketUpsert;
    duplicateTracking.followUp = {
      operation: "schedule",
      followUpId: null,
      objective: "Watch for confirmation that the field-trip form was signed.",
      currentConclusion: "The signature is still outstanding.",
      endCondition: "A parent confirms the form was signed.",
      nextCheck: "2026-08-28T21:00:00.000Z",
      why: "The form has a deadline.",
      sourceIds: ["turn-1"],
    };
    const duplicateReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => fakeStream({ status: "completed", output_parsed: duplicateTracking, output: [] }),
      },
    } as never);

    await expect(duplicateReasoner.decide(foregroundInput(), inertReads())).rejects.toMatchObject({
      code: "invalid_output",
      message: expect.stringContaining("already doing or tracking"),
    });

    const missingDependency = ordinaryDecision();
    missingDependency.docketUpsert = {
      operation: "create",
      candidateId: null,
      candidate: {
        category: "deadline",
        summary: "The field-trip form still needs a signature.",
        urgency: "soon",
        dueAt: "2026-08-29T20:00:00.000Z",
        owner: "Hari",
        nextAction: "Sign the field-trip form.",
        waitingOn: null,
        needsAnswer: true,
      },
      sourceIds: ["turn-1"],
    };
    const missingDependencyReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => fakeStream({ status: "completed", output_parsed: missingDependency, output: [] }),
      },
    } as never);

    await expect(missingDependencyReasoner.decide(foregroundInput(), inertReads())).rejects.toMatchObject({
      code: "invalid_output",
      message: expect.stringContaining("must name what it is waiting on"),
    });
  });

  test("docket coordination schemas require the exact dependency behind a needed answer", () => {
    const unresolved = {
      owner: "Jackson",
      nextAction: "Choose which after-school option works.",
      waitingOn: null,
      needsAnswer: true,
    };

    expect(florencePrivateDocketCoordinationSchema.safeParse(unresolved).success).toBe(false);
    expect(
      florenceHouseholdSafeCandidateSchema.safeParse({
        ...unresolved,
        category: "loose_end",
        summary: "The after-school plan still needs a choice.",
        urgency: "soon",
        dueAt: null,
      }).success,
    ).toBe(false);
    expect(
      florencePrivateDocketCoordinationSchema.parse({
        ...unresolved,
        waitingOn: "Jackson's choice of after-school option",
      }),
    ).toMatchObject({ owner: "Jackson", nextAction: expect.stringContaining("Choose") });
  });

  test("household briefing accepts the complete unresolved docket without an item ceiling", () => {
    const candidates = Array.from({ length: 101 }, (_, index) => ({
      candidateId: `candidate-${index + 1}`,
      category: "loose_end" as const,
      summary: `Family item ${index + 1} still needs follow-through.`,
      urgency: "watch" as const,
      dueAt: null,
      needsAnswer: false,
      owner: null,
      nextAction: `Move family item ${index + 1} forward.`,
      waitingOn: null,
    }));

    expect(florenceHouseholdBriefingInputSchema.shape.candidates.safeParse(candidates).success).toBe(true);
  });

  test("a household wake can page through Vault and start one objective from the complete docket", async () => {
    const candidates = Array.from({ length: 101 }, (_, index) => ({
      candidateId: `candidate-${index + 1}`,
      category: "loose_end" as const,
      summary: `Family item ${index + 1} still needs follow-through.`,
      urgency: "watch" as const,
      dueAt: null,
      needsAnswer: false,
      owner: null,
      nextAction: `Move family item ${index + 1} forward.`,
      waitingOn: null,
    }));
    const input: FlorenceHouseholdNextActionInput = {
      currentTime: NOW,
      familyProfile: {
        familyLabel: "Test family",
        timeZone: "America/Los_Angeles",
        adultFirstNames: ["Hari", "Jackson"],
        children: [],
        postalCode: "90045",
      },
      householdDocket: { totalItems: candidates.length, items: candidates },
      activeWork: [],
      lastInterruption: null,
      familyCalendar: {
        timeMin: NOW,
        timeMax: "2026-09-17T20:00:00.000Z",
        events: [],
      },
    };
    const memoryUri = "vault://fact/11111111-1111-4111-8111-111111111111";
    const requests: Record<string, unknown>[] = [];
    const responses = [
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("search-page-1", "search_vault", { query: "family item 101", cursor: null })],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("search-page-2", "search_vault", {
            query: "family item 101",
            cursor: "vault-page-2",
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("read-memory", "read_vault", { uri: memoryUri, level: "overview" })],
      },
      {
        status: "completed",
        output_parsed: {
          message: "I found the family plan that fits this—I’m moving item 101 forward now.",
          nextJob: {
            objective: "Move family item 101 forward using the retained family plan.",
            candidateIds: ["candidate-101"],
          },
        },
        output: [],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          requests.push(request);
          const response = responses.shift();
          if (!response) throw new Error("Unexpected household-next-action model turn");
          return response;
        },
      },
    } as never);
    const searches: Array<{ query: string; cursor: string | null }> = [];
    const reads: Array<{ uri: string; level: string }> = [];

    const decision = await reasoner.decideHouseholdNextAction(input, {
      async searchVault(arguments_) {
        searches.push(arguments_);
        return arguments_.cursor === null
          ? {
              query: arguments_.query,
              results: [],
              total: 1,
              complete: false,
              nextCursor: "vault-page-2",
            }
          : {
              query: arguments_.query,
              results: [
                {
                  uri: memoryUri,
                  score: 1,
                  abstract: "A reusable family plan for moving this item forward.",
                  memoryKind: "artifact",
                  artifactKind: "plan",
                  title: "Family plan",
                  tags: ["family", "plan"],
                  updatedAt: NOW,
                },
              ],
              total: 1,
              complete: true,
              nextCursor: null,
            };
      },
      async readVault(arguments_) {
        reads.push(arguments_);
        return {
          uri: memoryUri,
          level: "overview",
          memory: {
            factId: "11111111-1111-4111-8111-111111111111",
            statement: "The family has a reusable plan for moving this item forward.",
            memoryKind: "artifact",
            artifactKind: "plan",
            title: "Family plan",
            details: "Use the retained family plan to move the item forward.",
            tags: ["family", "plan"],
            files: [],
            visibility: "household",
            updatedAt: NOW,
          },
          supports: [],
        };
      },
    });

    expect(decision.nextJob?.candidateIds).toEqual(["candidate-101"]);
    expect(searches).toEqual([
      { query: "family item 101", cursor: null },
      { query: "family item 101", cursor: "vault-page-2" },
    ]);
    expect(reads).toEqual([{ uri: memoryUri, level: "overview" }]);
    const initialInput = JSON.parse(
      String(
        ((requests[0]?.input as Array<{ content?: Array<{ text?: string }> }>)?.[0]?.content ?? [])[0]?.text,
      ),
    ) as Record<string, unknown>;
    expect((initialInput.householdDocket as { items: unknown[] }).items).toHaveLength(101);
    expect(initialInput).not.toHaveProperty("memory");
    expect(requests.every((request) => !("max_tool_calls" in request))).toBe(true);
  });

  test("a natural text mention can acknowledge work Florence starts in the family group", async () => {
    const decision = ordinaryDecision();
    decision.conversation.bubbles = [];
    decision.conversation.nativeMoves = [
      {
        type: "mention",
        text: "Jackson, I’m comparing the Saturday dinner options now.",
        adultDisplayName: "Jackson",
      },
    ];
    decision.familyWork = {
      operation: "create",
      workId: null,
      objective: "Find three good dinner options for Saturday and compare them.",
      schedule: null,
      instruction: null,
      candidateIds: [],
    };
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => fakeStream({ status: "completed", output_parsed: decision, output: [] }),
      },
    } as never);
    const input = foregroundInput();
    input.audience = "group";
    input.googleConnections = [
      { emailLabel: "Family", calendarAvailable: true, kind: "family", writesEnabled: false },
    ];

    await expect(reasoner.decide(input, inertReads())).resolves.toMatchObject({
      conversation: { bubbles: [], nativeMoves: decision.conversation.nativeMoves },
      familyWork: { operation: "create" },
    });
  });

  test("family work cannot start twice from the same open docket item", async () => {
    const decision = ordinaryDecision();
    decision.familyWork = {
      operation: "create",
      workId: null,
      objective: "Make sure the school form is submitted.",
      schedule: null,
      instruction: null,
      candidateIds: ["candidate-1"],
    };
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => fakeStream({ status: "completed", output_parsed: decision, output: [] }),
      },
    } as never);
    const input = foregroundInput();
    input.householdDocket = {
      totalItems: 1,
      items: [
        {
          candidateId: "candidate-1",
          visibility: "household",
          category: "deadline",
          summary: "The school form still needs to be submitted.",
          urgency: "soon",
          dueAt: null,
          owner: "Florence",
          nextAction: "Submit the school form.",
          waitingOn: null,
          needsAnswer: false,
        },
      ],
    };
    input.visibleFamilyWork = [
      {
        workId: "work-1",
        objective: "Handle the school paperwork.",
        candidateIds: ["candidate-1"],
        currentProgress: null,
        schedule: null,
        paused: false,
        status: "active",
        nextAt: null,
        lastRunAt: null,
        lastResult: null,
        createdAt: NOW,
      },
    ];

    await expect(reasoner.decide(input, inertReads())).rejects.toMatchObject({
      code: "invalid_output",
      message: expect.stringContaining("already in progress"),
    });
  });

  test("a verified voice note can relay a concise derived household conclusion with a large Vault", async () => {
    const requests: Record<string, unknown>[] = [];
    const decision = ordinaryDecision();
    decision.conversation.bubbles = [];
    decision.householdUpdate = {
      text: "School pickup is at 2:45 PM today, so Jackson should plan to leave by 2:25.",
      sourceIds: ["turn-1"],
    };
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          return fakeStream({ status: "completed", output_parsed: decision, output: [] });
        },
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text =
      "[Automatic voice-note transcript]\nTell Jackson what you worked out about pickup today.";
    input.currentMessage.authoredText = null;
    input.currentMessage.voiceTranscriptPresent = true;
    input.visibleSources = Array.from({ length: 75 }, (_, index) => ({
      sourceId: `memory-source-${index}`,
      recordId: `memory-fact-${index}`,
      kind: "memory" as const,
      visibility: "adult_private" as const,
      label: `Retained family fact ${index}`,
      occurredAt: NOW,
      text:
        index === 0
          ? "The school dismissal notice says pickup is at 2:45 PM today; normal drive time is 20 minutes."
          : `Useful retained family detail ${index}.`,
    }));

    const result = await reasoner.decide(input, inertReads());

    expect(result.householdUpdate?.text).toContain("leave by 2:25");
    const instructions = String(requests[0]?.instructions);
    expect(instructions).toContain("concise household-relevant conclusion");
    expect(instructions).toContain("typed text or verified voice note");
    expect(instructions).toContain("Never copy or dump raw Gmail");
    expect(instructions).toContain(
      "Return direct only when the current parent's typed text or verified voice note",
    );
    expect(instructions).toContain("states a stable interest in typed text or a verified voice note");
  });

  test("keeps reading a long parent-supplied public page until the later answer", async () => {
    const pageUrl = "https://school.example/fall-fair";
    const selectedUrl = "https://school.example/fall-fair/faq";
    const pageFingerprint = "a".repeat(64);
    const requests: Record<string, unknown>[] = [];
    const pageReads: unknown[] = [];
    let modelTurn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          modelTurn += 1;
          return fakeStream(
            modelTurn === 1
              ? {
                  status: "completed",
                  output_parsed: null,
                  output: [
                    functionCall("parent-page", "read_public_page", { url: pageUrl }),
                    functionCall("selected-page", "read_public_page", {
                      url: selectedUrl,
                    }),
                  ],
                }
              : modelTurn === 2
                ? {
                    status: "completed",
                    output_parsed: null,
                    output: [
                      functionCall("continue-parent-page", "read_public_page", {
                        url: pageUrl,
                        offset: 15_000,
                        contentFingerprint: pageFingerprint,
                      }),
                    ],
                  }
                : {
                    status: "completed",
                    output_parsed: ordinaryDecision({
                      bubbleText: "The fall-fair RSVP closes September 6 at 5 PM.",
                      researchUrls: [pageUrl],
                    }),
                    output: [],
                  },
          );
        },
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text = `Read ${pageUrl} — when is the RSVP due?`;
    input.currentMessage.authoredText = input.currentMessage.text;

    const result = await reasoner.decide(input, {
      ...inertReads(),
      async runPublicPage(request) {
        pageReads.push(request);
        if (request.url === pageUrl && request.offset === 15_000) {
          return {
            ...publicPageResult(request.url, "Fall Fair", "RSVP closes September 6 at 5 PM."),
            offset: 15_000,
            truncated: true,
            totalCleanCharacters: 15_041,
            totalCleanBytes: 15_041,
            contentFingerprint: pageFingerprint,
          };
        }
        if (request.url === pageUrl) {
          return {
            ...publicPageResult(
              request.url,
              "Fall Fair",
              "Fall Fair information continues beyond this first section.",
            ),
            truncated: true,
            totalCleanCharacters: 15_041,
            totalCleanBytes: 15_041,
            nextOffset: 15_000,
            contentFingerprint: pageFingerprint,
          };
        }
        return publicPageResult(
          request.url,
          "Fall Fair FAQ",
          "General accessibility and parking information.",
        );
      },
    });

    expect(pageReads).toEqual([
      { url: pageUrl, offset: 0, contentFingerprint: null, charLimit: 15_000 },
      { url: selectedUrl, offset: 0, contentFingerprint: null, charLimit: 15_000 },
      { url: pageUrl, offset: 15_000, contentFingerprint: pageFingerprint, charLimit: 15_000 },
    ]);
    expect(result.conversation.bubbles[0]?.text).toContain("September 6 at 5 PM");
    expect(result.researchUrls).toEqual([pageUrl]);
    const envelopes = functionOutputEnvelopes(requests[1]);
    expect(envelopes.find((envelope) => envelope.callId === "parent-page")).toMatchObject({
      outcome: "succeeded",
      output: { title: "Fall Fair", text: expect.not.stringContaining("September 6") },
    });
    expect(envelopes.find((envelope) => envelope.callId === "selected-page")).toMatchObject({
      outcome: "succeeded",
      output: { title: "Fall Fair FAQ", text: expect.not.stringContaining("September 6") },
    });
    expect(
      functionOutputEnvelopes(requests[2]).find((envelope) => envelope.callId === "continue-parent-page"),
    ).toMatchObject({
      outcome: "succeeded",
      output: {
        offset: 15_000,
        nextOffset: null,
        contentFingerprint: pageFingerprint,
        text: expect.stringContaining("September 6"),
      },
    });
    expect(JSON.stringify(requests[0]?.tools)).toContain("nextOffset");
  });

  test("follows a verified search result and reads the page before answering", async () => {
    const searchUrl = "https://school.example/field-trip";
    const requests: Record<string, unknown>[] = [];
    let modelTurn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          modelTurn += 1;
          return fakeStream(
            modelTurn === 1
              ? {
                  status: "completed",
                  output_parsed: null,
                  output: [
                    completedWebSearch(
                      searchUrl,
                      "school field trip permission form deadline",
                      "field-trip-search",
                    ),
                    functionCall("open-trip", "read_public_page", { url: searchUrl }),
                  ],
                }
              : {
                  status: "completed",
                  output_parsed: ordinaryDecision({
                    bubbleText: "The permission form is due Tuesday at 3 PM.",
                    researchUrls: [searchUrl],
                  }),
                  output: [],
                },
          );
        },
      },
    } as never);

    const result = await reasoner.decide(foregroundInput(), {
      ...inertReads(),
      async runPublicPage(request) {
        expect(request.url).toBe(searchUrl);
        return publicPageResult(searchUrl, "Field trip", "Permission form due Tuesday at 3 PM.");
      },
    });

    expect(result.conversation.bubbles[0]?.text).toContain("Tuesday at 3 PM");
    expect(JSON.stringify(requests[1]?.input)).toContain("Permission form due Tuesday at 3 PM");
    expect(requests[0]?.tools).toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "web_search" })]),
    );
    expect(result.researchUrls).toEqual([searchUrl]);
  });

  test("durable work composes a linked PDF read inline before answering", async () => {
    const pdfUrl = "https://school.example/forms/field-trip.pdf";
    const modelRequests: Record<string, unknown>[] = [];
    const modelResponses = [
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("read-field-trip-pdf", "read_public_page", { url: pdfUrl })],
      },
      {
        status: "completed",
        output_parsed: {
          outcome: "succeeded",
          text: "The field-trip form is due Tuesday at 3 PM and needs a parent signature.",
        },
        output: [],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          modelRequests.push(request);
          const response = modelResponses.shift();
          if (!response) throw new Error("Unexpected durable PDF model turn");
          return response;
        },
      },
    } as never);
    const state: FamilyWorkStateV1 = {
      kind: "family_work_v1",
      version: 1,
      generation: 0,
      phase: "ready",
      claim: null,
      activePhoneCall: null,
      activeTextMessage: null,
      pendingParticipantRequest: null,
      browserSession: null,
      continuationItems: [],
      pendingCall: null,
      steering: [],
      progressRevision: 0,
      terminal: null,
    };
    const originBase = familyWorkOrigin(
      "Use the revised field-trip form. Voice transcript: the blue form is the right one.",
    );
    const origin: FamilyWorkOriginContext = {
      message: {
        ...originBase.message,
        authoredText: "Use the revised field-trip form.",
        voiceTranscriptPresent: true,
        images: [{ assetId: "field-trip-image", mimeType: "image/jpeg" }],
      },
      supersededMessages: [
        {
          ...originBase.message,
          sourceId: "original-field-trip-request",
          text: "Use the original field-trip form.",
          authoredText: "Use the original field-trip form.",
          voiceTranscriptPresent: false,
          images: [],
        },
      ],
      replyTarget: {
        ...originBase.message,
        sourceId: "florence-field-trip-question",
        speaker: "florence",
        text: "Which form should I use?",
        authoredText: "Which form should I use?",
        voiceTranscriptPresent: false,
        images: [{ assetId: "reply-field-trip-image", mimeType: "image/png" }],
      },
      currentDocuments: [
        {
          id: "field-trip-pdf",
          parentSourceId: originBase.message.sourceId,
          filename: "revised-field-trip.pdf",
          mimeType: "application/pdf",
          contentDigest: "a".repeat(64),
          contentEnvelope: Uint8Array.from([1]),
          discardAfter: "2026-08-28T20:00:00.000Z",
        },
        {
          id: "reply-field-trip-pdf",
          parentSourceId: "florence-field-trip-question",
          filename: "original-field-trip.pdf",
          mimeType: "application/pdf",
          contentDigest: "b".repeat(64),
          contentEnvelope: Uint8Array.from([2]),
          discardAfter: "2026-08-28T20:00:00.000Z",
        },
      ],
    };
    const input = {
      workId: "family-work-pdf",
      scheduledOccurrence: null,
      objective: `Read ${pdfUrl} and tell me the deadline and what I need to do.`,
      visibility: "private" as const,
      ownerAdultId: "adult-1",
      origin,
      household: {
        householdId: "household-1",
        familyLabel: "Test family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [{ adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" }],
        children: [],
      },
      state,
      currentTime: NOW,
    };
    const imageReads: string[] = [];
    const pdfReads: string[] = [];
    const reads = {
      readCurrentImage: async ({
        assetId,
        mimeType,
      }: {
        assetId: string;
        mimeType: "image/jpeg" | "image/png" | "image/webp";
      }) => {
        imageReads.push(assetId);
        return { mimeType, bytes: Uint8Array.from([0xff, 0xd8, 0xff]) };
      },
      readCurrentPdf: async ({ documentId }: { documentId: string }) => {
        pdfReads.push(documentId);
        return {
          mimeType: "application/pdf" as const,
          bytes: Uint8Array.from([0x25, 0x50, 0x44, 0x46, 0x2d]),
        };
      },
      runPublicPage: async () => ({
        ...publicPageResult(
          pdfUrl,
          "Field trip form",
          "Return by Tuesday at 3 PM. Parent signature required.",
        ),
        kind: "pdf" as const,
        filename: "field-trip.pdf",
      }),
    };

    const terminal = await reasoner.continueFamilyWork(input, reads);
    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("parent signature"),
    });
    expect(imageReads).toEqual(["reply-field-trip-image", "field-trip-image"]);
    expect(pdfReads).toEqual(["field-trip-pdf", "reply-field-trip-pdf"]);
    expect(JSON.stringify(modelRequests[0]?.input)).toContain("Use the revised field-trip form.");
    expect(JSON.stringify(modelRequests[0]?.input)).toContain("Use the original field-trip form.");
    expect(JSON.stringify(modelRequests[0]?.input)).toContain("Which form should I use?");
    expect(JSON.stringify(modelRequests[0]?.input)).toContain('"type":"input_image"');
    expect(JSON.stringify(modelRequests[0]?.input)).toContain('"type":"input_file"');
    expect(JSON.stringify(modelRequests[1]?.input)).toContain("Parent signature required");
    expect(JSON.stringify(modelRequests[1]?.input)).toContain('"type":"input_image"');
    expect(JSON.stringify(modelRequests[1]?.input)).toContain('"type":"input_file"');
  });

  test("partial and failed family work leave honest terminal docket coordination", async () => {
    const requests: Record<string, unknown>[] = [];
    const terminalDecision = {
      outcome: "partial",
      text: "I found the form, but the school portal is unavailable, so I couldn't submit it.",
      docket: {
        owner: "Florence",
        nextAction: "Submit the form when the school portal is available.",
        waitingOn: "The school portal to become available",
        needsAnswer: false,
      },
    } as const;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          requests.push(request);
          return { status: "completed", output_parsed: terminalDecision, output: [] };
        },
      },
    } as never);
    const state: FamilyWorkStateV1 = {
      kind: "family_work_v1",
      version: 1,
      generation: 0,
      docketCandidateIds: ["candidate-1"],
      phase: "ready",
      claim: null,
      activePhoneCall: null,
      activeTextMessage: null,
      pendingParticipantRequest: null,
      browserSession: null,
      continuationItems: [],
      pendingCall: null,
      steering: [],
      progressRevision: 0,
      terminal: null,
    };
    const input = {
      workId: "family-work-partial",
      scheduledOccurrence: null,
      objective: "Submit the school form.",
      visibility: "household" as const,
      ownerAdultId: null,
      initiatingAdultId: "adult-1",
      origin: familyWorkOrigin("Submit the school form."),
      household: {
        householdId: "household-1",
        familyLabel: "Test family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [{ adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" }],
        children: [],
      },
      state,
      currentTime: NOW,
    };

    const result = await reasoner.continueFamilyWork(input, {} as never);

    expect(result).toMatchObject({
      kind: "terminal",
      outcome: "partial",
      docket: terminalDecision.docket,
      state: { terminal: { docket: terminalDecision.docket } },
    });
    expect(String(requests[0]?.instructions)).toContain(
      "For waiting, partial, or failed, docket must describe",
    );

    for (const invalidDecision of [
      { ...terminalDecision, docket: null },
      { ...terminalDecision, outcome: "succeeded" as const },
    ]) {
      const invalidReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
        responses: {
          parse: () => ({ status: "completed", output_parsed: invalidDecision, output: [] }),
        },
      } as never);
      await expect(invalidReasoner.continueFamilyWork(input, {} as never)).rejects.toMatchObject({
        code: "invalid_output",
        message: expect.stringContaining("docket coordination"),
      });
    }
  });

  test("waiting family work persists exact coordination and clears it after the answer", async () => {
    const waitingDecision = {
      outcome: "waiting",
      text: "Which pickup time should I confirm, 2:45 or 3:15?",
      docket: {
        owner: "Hari",
        nextAction: "Choose the pickup time.",
        waitingOn: "Hari's choice between 2:45 and 3:15",
        needsAnswer: true,
      },
    } as const;
    const baseState: FamilyWorkStateV1 = {
      kind: "family_work_v1",
      version: 1,
      generation: 0,
      docketCandidateIds: ["candidate-1"],
      phase: "ready",
      claim: null,
      activePhoneCall: null,
      activeTextMessage: null,
      pendingParticipantRequest: null,
      browserSession: null,
      continuationItems: [],
      pendingCall: null,
      steering: [],
      progressRevision: 0,
      terminal: null,
    };
    const input = {
      workId: "family-work-waiting",
      scheduledOccurrence: null,
      objective: "Confirm the children's pickup time.",
      visibility: "household" as const,
      ownerAdultId: null,
      initiatingAdultId: "adult-1",
      origin: familyWorkOrigin("Please confirm pickup."),
      household: {
        householdId: "household-1",
        familyLabel: "Test family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [{ adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" }],
        children: [],
      },
      state: baseState,
      currentTime: NOW,
    };
    const waitingReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse: () => ({ status: "completed", output_parsed: waitingDecision, output: [] }),
      },
    } as never);

    const waiting = await waitingReasoner.continueFamilyWork(input, {} as never);

    expect(waiting).toMatchObject({
      kind: "waiting",
      state: { phase: "waiting", waitingDocket: waitingDecision.docket },
    });

    const succeededReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse: () => ({
          status: "completed",
          output_parsed: {
            outcome: "succeeded",
            text: "Pickup is confirmed for 2:45 PM.",
            docket: null,
          },
          output: [],
        }),
      },
    } as never);
    if (waiting.kind !== "waiting") throw new Error("Expected family work to wait");

    const succeeded = await succeededReasoner.continueFamilyWork(
      { ...input, state: { ...waiting.state, phase: "ready" } },
      {} as never,
    );

    expect(succeeded).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      state: { waitingDocket: null, terminal: { docket: null } },
    });

    const invalidWaitingReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse: () => ({
          status: "completed",
          output_parsed: {
            ...waitingDecision,
            docket: { ...waitingDecision.docket, needsAnswer: false },
          },
          output: [],
        }),
      },
    } as never);
    await expect(invalidWaitingReasoner.continueFamilyWork(input, {} as never)).rejects.toMatchObject({
      code: "invalid_output",
      message: expect.stringContaining("needs an answer"),
    });
  });

  test("durable household work calls a business and reports the transcript-backed outcome", async () => {
    const modelRequests: Record<string, unknown>[] = [];
    const usefulProgress = {
      outcome: "progress",
      text: null,
      resumeAt: null,
      progressText:
        "The dentist has a Wednesday 3:30 PM opening and is checking whether they can hold it for Violet.",
      selectedImageAssetIds: [],
    } as const;
    const unchangedProgress = {
      ...usefulProgress,
      progressText: "The office still has the request, and I’m still waiting for a time.",
    } as const;
    const modelResponses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("call-dentist", "phone_agent_call", {
            operation: "start",
            to: "+13105550144",
            task: "Call the dentist, ask for a cleaning appointment for Violet on Tuesday or Wednesday after 3 PM, and do not book outside those times.",
            providerCallId: null,
            firstSentence: "Hi, I’m calling for the Williams family about a cleaning appointment.",
            voice: null,
            maxDurationMinutes: 5,
            record: true,
            summaryPrompt: "State whether an appointment was booked and give the exact date and time.",
            dispositions: ["booked", "availability_found", "no_availability"],
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("check-dentist-call-progress", "phone_agent_call", {
            operation: "status",
            to: null,
            task: null,
            providerCallId: "bland-call-1",
            firstSentence: null,
            voice: null,
            maxDurationMinutes: null,
            record: false,
            summaryPrompt: null,
            dispositions: [],
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: usefulProgress,
        output: [familyWorkResultMessage("dentist-progress", usefulProgress)],
      },
      {
        status: "completed",
        output_parsed: { deliver: true },
        output: [],
      },
      {
        status: "completed",
        output_parsed: usefulProgress,
        output: [familyWorkResultMessage("dentist-progress-repeat", usefulProgress)],
      },
      {
        status: "completed",
        output_parsed: unchangedProgress,
        output: [familyWorkResultMessage("dentist-progress-paraphrase", unchangedProgress)],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("check-dentist-call", "phone_agent_call", {
            operation: "status",
            to: null,
            task: null,
            providerCallId: "bland-call-1",
            firstSentence: null,
            voice: null,
            maxDurationMinutes: null,
            record: false,
            summaryPrompt: null,
            dispositions: [],
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: {
          outcome: "succeeded",
          text: "Violet’s cleaning is booked for Wednesday at 3:30 PM. The office confirmed it on the call.",
        },
        output: [],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          modelRequests.push(request);
          const response = modelResponses.shift();
          if (!response) throw new Error("Unexpected dentist-call model turn");
          return response;
        },
      },
    } as never);
    const input = {
      workId: "family-work-dentist-call",
      scheduledOccurrence: null,
      objective: "Call the dentist and arrange Violet’s cleaning Tuesday or Wednesday after 3 PM.",
      visibility: "household" as const,
      ownerAdultId: null,
      initiatingAdultId: "adult-jackson",
      origin: familyWorkOrigin(
        "Call the dentist and arrange Violet’s cleaning Tuesday or Wednesday after 3 PM.",
        "adult-jackson",
      ),
      household: {
        householdId: "household-1",
        familyLabel: "Williams family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [
          { adultId: "adult-jackson", firstName: "Jackson", displayName: "Jackson Williams" },
          { adultId: "adult-hari", firstName: "Hari", displayName: "Hari Anbarasu" },
        ],
        children: [
          {
            childId: "child-violet",
            firstName: "Violet",
            displayName: "Violet Williams",
            age: 4,
            grade: "TK",
            school: "Wish Charter",
            activities: [],
          },
        ],
      },
      state: {
        kind: "family_work_v1" as const,
        version: 1 as const,
        generation: 0,
        acknowledgementText: "I’ll call the dentist and work within those times.",
        phase: "ready" as const,
        claim: null,
        activePhoneCall: null,
        activeTextMessage: null,
        pendingParticipantRequest: null,
        browserSession: null,
        continuationItems: [],
        pendingCall: null,
        steering: [],
        progressRevision: 0,
        terminal: null,
      },
      currentTime: NOW,
    };
    const operations: FlorenceTelephonyOperation[] = [];
    const reads = {
      telephonyProviders: ["bland"] as const,
      async runTelephony(operation: FlorenceTelephonyOperation): Promise<FlorenceTelephonyResult> {
        operations.push(operation);
        if (operation.kind === "ai_call_start") {
          return telephonyResult({
            kind: "accepted",
            provider: "bland",
            operation: operation.kind,
            providerId: "bland-call-1",
            providerStatus: "queued",
          });
        }
        if (operation.kind === "ai_call_status" && operations.length === 2) {
          return telephonyResult({
            kind: "progress",
            provider: "bland",
            operation: operation.kind,
            providerId: "bland-call-1",
            providerStatus: "in-progress",
            summary: "The office has a Wednesday 3:30 PM opening and is checking whether it can be held.",
          });
        }
        if (operation.kind === "ai_call_status") {
          return telephonyResult({
            kind: "completed",
            provider: "bland",
            operation: operation.kind,
            providerId: "bland-call-1",
            providerStatus: "completed",
            summary: "Cleaning booked for Wednesday at 3:30 PM.",
            disposition: "booked",
            transcript: "Office: We can do Wednesday at 3:30. Florence: Please book it.",
          });
        }
        throw new Error(`Unexpected telephony operation ${operation.kind}`);
      },
    };

    const plannedCall = await reasoner.continueFamilyWork(input, reads);
    if (plannedCall.kind !== "continue") throw new Error("Dentist call was not planned");
    expect(String(modelRequests[0]?.instructions)).toContain(
      "An exact parent instruction that already requests the proposed outside commitment is authorization",
    );
    expect(plannedCall.state).toMatchObject({
      phase: "tool_pending",
      pendingCall: { name: "phone_agent_call" },
    });
    const started = await reasoner.continueFamilyWork({ ...input, state: plannedCall.state }, reads);
    if (started.kind !== "continue") throw new Error("Dentist call did not start");
    expect(started.progressText).toBeNull();
    expect(started.nextCheckDelayMs).toBe(0);
    expect(started.state.activePhoneCall).toEqual({
      provider: "bland",
      kind: "agent",
      providerCallId: "bland-call-1",
    });

    const plannedProgressCheck = await reasoner.continueFamilyWork({ ...input, state: started.state }, reads);
    if (plannedProgressCheck.kind !== "continue") {
      throw new Error("Dentist interim call status was not planned");
    }
    const checkedProgress = await reasoner.continueFamilyWork(
      { ...input, state: plannedProgressCheck.state },
      reads,
    );
    if (checkedProgress.kind !== "continue") {
      throw new Error("Dentist interim call status did not settle");
    }

    const progress = await reasoner.continueFamilyWork({ ...input, state: checkedProgress.state }, reads);
    if (progress.kind !== "continue") throw new Error("Dentist progress was not checkpointed");
    expect(progress.progressText).toBe(usefulProgress.progressText);
    expect(progress.state.progressRevision).toBe(1);
    expect(progress.state.activePhoneCall).toEqual({
      provider: "bland",
      kind: "agent",
      providerCallId: "bland-call-1",
    });

    const repeatedProgress = await reasoner.continueFamilyWork({ ...input, state: progress.state }, reads);
    if (repeatedProgress.kind !== "continue") throw new Error("Repeated progress did not continue");
    expect(repeatedProgress.progressText).toBeNull();
    expect(repeatedProgress.state.progressRevision).toBe(1);

    const paraphrasedProgress = await reasoner.continueFamilyWork(
      { ...input, state: repeatedProgress.state },
      reads,
    );
    if (paraphrasedProgress.kind !== "continue") {
      throw new Error("Paraphrased progress did not continue");
    }
    expect(paraphrasedProgress.progressText).toBeNull();
    expect(paraphrasedProgress.state.progressRevision).toBe(1);

    const plannedStatus = await reasoner.continueFamilyWork(
      { ...input, state: paraphrasedProgress.state },
      reads,
    );
    if (plannedStatus.kind !== "continue") throw new Error("Dentist call status was not planned");
    const completed = await reasoner.continueFamilyWork({ ...input, state: plannedStatus.state }, reads);
    if (completed.kind !== "continue") throw new Error("Dentist call status did not settle");
    expect(completed.nextCheckDelayMs).toBe(0);
    expect(completed.state.activePhoneCall).toBeNull();
    expect(JSON.stringify(completed.state.continuationItems)).toContain("Wednesday at 3:30 PM");

    const terminal = await reasoner.continueFamilyWork({ ...input, state: completed.state }, reads);
    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("Wednesday at 3:30 PM"),
    });
    expect(operations).toEqual([
      expect.objectContaining({ kind: "ai_call_start", to: "+13105550144" }),
      { kind: "ai_call_status", provider: "bland", providerCallId: "bland-call-1" },
      { kind: "ai_call_status", provider: "bland", providerCallId: "bland-call-1" },
    ]);
    expect(JSON.stringify(modelRequests[3]?.input)).toContain(
      "I’ll call the dentist and work within those times.",
    );
    const reviewInput = modelRequests[3]?.input as Array<{
      content: Array<{ text: string }>;
    }>;
    const reviewContext = JSON.parse(reviewInput[0]?.content[0]?.text ?? "{}") as {
      taskTranscript?: unknown;
    };
    expect(JSON.stringify(reviewContext.taskTranscript)).not.toContain(usefulProgress.progressText);
  });

  test("waits for a Bland start confirmation after cancellation so the provider call stays tracked", async () => {
    let resolveProvider!: (result: FlorenceTelephonyResult) => void;
    let markProviderStarted!: () => void;
    const providerStarted = new Promise<void>((resolve) => {
      markProviderStarted = resolve;
    });
    const providerResult = new Promise<FlorenceTelephonyResult>((resolve) => {
      resolveProvider = resolve;
    });
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {} as never);
    const controller = new AbortController();
    const callArguments = {
      operation: "start",
      to: "+13105550144",
      task: "Ask the dentist for an appointment.",
      providerCallId: null,
      firstSentence: null,
      voice: null,
      maxDurationMinutes: null,
      record: true,
      summaryPrompt: null,
      dispositions: [],
    };
    const input = {
      workId: "family-work-cancelled-call-start",
      scheduledOccurrence: null,
      objective: "Call the dentist and ask for an appointment.",
      visibility: "private" as const,
      ownerAdultId: "adult-jackson",
      initiatingAdultId: "adult-jackson",
      origin: familyWorkOrigin("Call the dentist and ask for an appointment.", "adult-jackson"),
      household: {
        householdId: "household-1",
        familyLabel: "Williams family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [{ adultId: "adult-jackson", firstName: "Jackson", displayName: "Jackson Williams" }],
        children: [],
      },
      state: {
        kind: "family_work_v1" as const,
        version: 1 as const,
        generation: 0,
        phase: "tool_pending" as const,
        claim: null,
        activePhoneCall: null,
        activeTextMessage: null,
        pendingParticipantRequest: null,
        browserSession: null,
        continuationItems: [functionCall("start-cancelled-call", "phone_agent_call", callArguments)],
        pendingCall: {
          callId: "start-cancelled-call",
          name: "phone_agent_call",
          argumentsJson: JSON.stringify(callArguments),
          attempt: 1,
        },
        steering: [],
        progressRevision: 0,
        terminal: null,
      },
      currentTime: NOW,
    };

    const continued = reasoner.continueFamilyWork(
      input,
      {
        telephonyProviders: ["bland"],
        runTelephony() {
          markProviderStarted();
          return providerResult;
        },
      },
      controller.signal,
    );
    await providerStarted;
    controller.abort(new Error("The parent cancelled the task"));
    const settledBeforeProvider = await Promise.race([
      continued.then(() => true),
      new Promise<false>((resolve) => setTimeout(() => resolve(false), 0)),
    ]);
    resolveProvider(
      telephonyResult({
        kind: "accepted",
        provider: "bland",
        operation: "ai_call_start",
        providerId: "bland-late-call-1",
        providerStatus: "queued",
      }),
    );
    const step = await continued;

    expect(settledBeforeProvider).toBe(false);
    expect(step).toMatchObject({
      kind: "continue",
      state: {
        activePhoneCall: {
          provider: "bland",
          kind: "agent",
          providerCallId: "bland-late-call-1",
        },
      },
    });
  });

  test("replaces a pending Twilio call handle with the real call SID returned by status", async () => {
    const pendingCallSid = `pending_twilio_call_${Date.parse(NOW).toString(36)}_${"a".repeat(64)}_KzEzMTA1NTUwMTQ0`;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {} as never);
    const statusArguments = {
      operation: "status",
      to: null,
      message: null,
      callSid: pendingCallSid,
      voice: null,
      sendDigits: null,
      record: false,
    };
    const input = {
      workId: "family-work-pending-twilio-call",
      scheduledOccurrence: null,
      objective: "Announce that pickup moved to 3 PM.",
      visibility: "private" as const,
      ownerAdultId: "adult-jackson",
      initiatingAdultId: "adult-jackson",
      origin: familyWorkOrigin("Announce that pickup moved to 3 PM.", "adult-jackson"),
      household: {
        householdId: "household-1",
        familyLabel: "Williams family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [{ adultId: "adult-jackson", firstName: "Jackson", displayName: "Jackson Williams" }],
        children: [],
      },
      state: {
        kind: "family_work_v1" as const,
        version: 1 as const,
        generation: 0,
        phase: "tool_pending" as const,
        claim: null,
        activePhoneCall: {
          provider: "twilio" as const,
          kind: "announcement" as const,
          providerCallId: pendingCallSid,
        },
        activeTextMessage: null,
        pendingParticipantRequest: null,
        browserSession: null,
        continuationItems: [functionCall("check-pending-twilio-call", "phone_announcement", statusArguments)],
        pendingCall: {
          callId: "check-pending-twilio-call",
          name: "phone_announcement",
          argumentsJson: JSON.stringify(statusArguments),
          attempt: 1,
        },
        steering: [],
        progressRevision: 0,
        terminal: null,
      },
      currentTime: NOW,
    };

    const step = await reasoner.continueFamilyWork(input, {
      telephonyProviders: ["twilio"],
      async runTelephony() {
        return telephonyResult({
          kind: "progress",
          provider: "twilio",
          operation: "call_status",
          providerId: "CA-real-call-1",
          providerStatus: "ringing",
        });
      },
    });

    expect(step).toMatchObject({
      kind: "continue",
      state: {
        activePhoneCall: {
          provider: "twilio",
          kind: "announcement",
          providerCallId: "CA-real-call-1",
        },
      },
    });
  });

  test("durable work sends once, waits for a reply, and resumes the same text task", async () => {
    const firstDeferResult = {
      outcome: "deferred",
      text: null,
      resumeAt: "2026-08-27T21:00:00.000Z",
      progressText: "The text was delivered. I’ll check for their reply this afternoon.",
    } as const;
    const secondDeferResult = {
      outcome: "deferred",
      text: null,
      resumeAt: "2026-08-27T22:00:00.000Z",
      progressText: "There’s no reply yet, so I’m still keeping an eye on it.",
    } as const;
    const modelResponses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("send-dentist-text", "sms_work", {
            operation: "send",
            to: "+13105550144",
            from: null,
            body: "Could you confirm Violet’s Wednesday 3:30 PM cleaning?",
            mediaUrls: [],
            messageSid: null,
            limit: null,
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("check-dentist-text", "sms_work", {
            operation: "status",
            to: null,
            from: null,
            body: null,
            mediaUrls: [],
            messageSid: "SM-dentist-1",
            limit: null,
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: firstDeferResult,
        output: [familyWorkResultMessage("dentist-defer-1", firstDeferResult)],
      },
      {
        status: "completed",
        output_parsed: { deliver: true },
        output: [],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("read-dentist-replies-1", "sms_work", {
            operation: "inbox",
            to: null,
            from: "+13105550144",
            body: null,
            mediaUrls: [],
            messageSid: null,
            limit: 20,
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: secondDeferResult,
        output: [familyWorkResultMessage("dentist-defer-2", secondDeferResult)],
      },
      {
        status: "completed",
        output_parsed: { deliver: false },
        output: [],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("read-dentist-replies-2", "sms_work", {
            operation: "inbox",
            to: null,
            from: "+13105550144",
            body: null,
            mediaUrls: [],
            messageSid: null,
            limit: 20,
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: {
          outcome: "succeeded",
          text: "The dentist confirmed Violet’s Wednesday 3:30 PM cleaning.",
          resumeAt: null,
          progressText: null,
        },
        output: [],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse() {
          const response = modelResponses.shift();
          if (!response) throw new Error("Unexpected dentist-text model turn");
          return response;
        },
      },
    } as never);
    const input = {
      workId: "family-work-dentist-text",
      scheduledOccurrence: null,
      objective: "Text the dentist and confirm Violet’s Wednesday cleaning.",
      visibility: "private" as const,
      ownerAdultId: "adult-jackson",
      initiatingAdultId: "adult-jackson",
      origin: familyWorkOrigin("Text the dentist and confirm Violet’s Wednesday cleaning.", "adult-jackson"),
      household: {
        householdId: "household-1",
        familyLabel: "Williams family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [{ adultId: "adult-jackson", firstName: "Jackson", displayName: "Jackson Williams" }],
        children: [],
      },
      state: {
        kind: "family_work_v1" as const,
        version: 1 as const,
        generation: 0,
        phase: "ready" as const,
        claim: null,
        activePhoneCall: null,
        activeTextMessage: null,
        pendingParticipantRequest: null,
        browserSession: null,
        continuationItems: [],
        pendingCall: null,
        steering: [],
        progressRevision: 0,
        terminal: null,
      },
      currentTime: NOW,
    };
    const operations: FlorenceTelephonyOperation[] = [];
    let inboxReads = 0;
    const reads = {
      telephonyProviders: ["twilio"] as const,
      async runTelephony(operation: FlorenceTelephonyOperation): Promise<FlorenceTelephonyResult> {
        operations.push(operation);
        if (operation.kind === "sms_send") {
          return telephonyResult({
            kind: "accepted",
            provider: "twilio",
            operation: operation.kind,
            providerId: "SM-dentist-1",
            providerStatus: "queued",
          });
        }
        if (operation.kind === "sms_status") {
          return telephonyResult({
            kind: "completed",
            provider: "twilio",
            operation: operation.kind,
            providerId: "SM-dentist-1",
            providerStatus: "delivered",
          });
        }
        if (operation.kind === "sms_inbox") {
          inboxReads += 1;
          return telephonyResult({
            kind: "completed",
            provider: "twilio",
            operation: operation.kind,
            providerId: null,
            providerStatus: "read",
            messages:
              inboxReads === 1
                ? []
                : [
                    {
                      messageSid: "SM-dentist-reply-1",
                      direction: "inbound",
                      status: "received",
                      fromPhoneNumber: "+13105550144",
                      toPhoneNumber: "+13105550999",
                      sentAt: "2026-08-27T21:45:00.000Z",
                      body: "Yes, Violet is confirmed for Wednesday at 3:30 PM.",
                      mediaCount: 0,
                    },
                  ],
          });
        }
        throw new Error(`Unexpected telephony operation ${operation.kind}`);
      },
    };

    const plannedSend = await reasoner.continueFamilyWork(input, reads);
    if (plannedSend.kind !== "continue") throw new Error("Dentist text was not planned");
    const sent = await reasoner.continueFamilyWork({ ...input, state: plannedSend.state }, reads);
    if (sent.kind !== "continue") throw new Error("Dentist text did not send");
    expect(sent.state.activeTextMessage).toEqual({
      provider: "twilio",
      messageSid: "SM-dentist-1",
    });

    const plannedStatus = await reasoner.continueFamilyWork({ ...input, state: sent.state }, reads);
    if (plannedStatus.kind !== "continue") throw new Error("Dentist text status was not planned");
    const delivered = await reasoner.continueFamilyWork({ ...input, state: plannedStatus.state }, reads);
    if (delivered.kind !== "continue") throw new Error("Dentist text status did not settle");
    expect(delivered.state.activeTextMessage).toBeNull();

    const deferred = await reasoner.continueFamilyWork({ ...input, state: delivered.state }, reads);
    expect(deferred).toMatchObject({
      kind: "deferred",
      resumeAt: "2026-08-27T21:00:00.000Z",
      progressText: "The text was delivered. I’ll check for their reply this afternoon.",
      state: { phase: "ready" },
    });
    if (deferred.kind !== "deferred") throw new Error("Dentist reply check was not deferred");

    const firstInboxPlanned = await reasoner.continueFamilyWork(
      { ...input, currentTime: deferred.resumeAt, state: deferred.state },
      reads,
    );
    if (firstInboxPlanned.kind !== "continue") throw new Error("First dentist inbox read was not planned");
    const firstInboxRead = await reasoner.continueFamilyWork(
      { ...input, currentTime: deferred.resumeAt, state: firstInboxPlanned.state },
      reads,
    );
    if (firstInboxRead.kind !== "continue") throw new Error("First dentist inbox read did not settle");

    const deferredAgain = await reasoner.continueFamilyWork(
      { ...input, currentTime: deferred.resumeAt, state: firstInboxRead.state },
      reads,
    );
    expect(deferredAgain).toMatchObject({
      kind: "deferred",
      resumeAt: "2026-08-27T22:00:00.000Z",
      progressText: null,
    });
    if (deferredAgain.kind !== "deferred") throw new Error("Second dentist reply check was not deferred");

    const secondInboxPlanned = await reasoner.continueFamilyWork(
      { ...input, currentTime: deferredAgain.resumeAt, state: deferredAgain.state },
      reads,
    );
    if (secondInboxPlanned.kind !== "continue") throw new Error("Second dentist inbox read was not planned");
    const secondInboxRead = await reasoner.continueFamilyWork(
      { ...input, currentTime: deferredAgain.resumeAt, state: secondInboxPlanned.state },
      reads,
    );
    if (secondInboxRead.kind !== "continue") throw new Error("Second dentist inbox read did not settle");

    const terminal = await reasoner.continueFamilyWork(
      { ...input, currentTime: deferredAgain.resumeAt, state: secondInboxRead.state },
      reads,
    );
    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("confirmed"),
    });
    expect(operations).toEqual([
      expect.objectContaining({ kind: "sms_send", to: "+13105550144" }),
      { kind: "sms_status", provider: "twilio", messageSid: "SM-dentist-1" },
      { kind: "sms_inbox", provider: "twilio", from: "+13105550144", limit: 20 },
      { kind: "sms_inbox", provider: "twilio", from: "+13105550144", limit: 20 },
    ]);
  });

  test("durable work prepares a camp form, asks once before the unrequested submission, then submits once", async () => {
    const portalUrl = "https://camp.example/register";
    const liveViewUrl = "https://www.browserbase.com/sessions/session-1";
    const gmail = conversationalGmailSource();
    const gmailAttachmentRef = gmail.attachments[0]?.attachmentRef ?? "missing-attachment";
    const modelRequests: Record<string, unknown>[] = [];
    const modelResponses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("read-camp-form-message", "gmail_work", {
            operation: "gmail_get",
            query: null,
            limit: null,
            messageId: "gmail-school-message",
            to: [],
            cc: [],
            bcc: [],
            subject: null,
            body: null,
            bodyFormat: null,
            threadId: null,
            addLabelIds: [],
            removeLabelIds: [],
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("open-camp-form", "read_gmail_attachment", {
            sourceId: gmail.sourceId,
            attachmentRef: gmailAttachmentRef,
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("open-camp-portal", "browser_work", browserArguments("navigate", { url: portalUrl })),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("sign-in-handoff", "browser_work", browserArguments("owner_handoff"))],
      },
      {
        status: "completed",
        output_parsed: {
          outcome: "waiting",
          text: `Please sign in here, then tell me when you’re done: ${liveViewUrl}`,
          docket: {
            owner: "Hari",
            nextAction: "Sign in to the camp portal.",
            waitingOn: "Hari to finish signing in",
            needsAnswer: true,
          },
        },
        output: [],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall(
            "fill-child-name",
            "browser_work",
            browserArguments("playwright", {
              code: `await page.getByLabel("Child name").fill("Violet Williams"); return { childName: await page.getByLabel("Child name").inputValue() };`,
              timeoutSeconds: 60,
            }),
          ),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall(
            "upload-medical-form",
            "browser_work",
            browserArguments("upload", {
              ref: "e7",
              sourceId: gmail.sourceId,
              attachmentRef: gmailAttachmentRef,
            }),
          ),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall(
            "inspect-registration",
            "browser_work",
            browserArguments("computer", {
              actions: [
                {
                  type: "move_mouse",
                  x: 400,
                  y: 300,
                  button: null,
                  clickType: null,
                  numClicks: null,
                  text: null,
                  keys: [],
                  deltaX: null,
                  deltaY: null,
                  path: [],
                  milliseconds: null,
                },
              ],
              screenshot: true,
            }),
          ),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("open-registration-review", "browser_work", browserArguments("click", { ref: "e9" })),
        ],
      },
      {
        status: "completed",
        output_parsed: {
          outcome: "waiting",
          text: "Violet’s Adventure Camp registration for June 15–19, 2027 is ready, including her medical form. The fee is $425. Should I submit it?",
          docket: {
            owner: "Hari",
            nextAction: "Decide whether to submit the camp registration.",
            waitingOn: "Hari's approval of the $425 registration",
            needsAnswer: true,
          },
        },
        output: [],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("submit-registration", "browser_work", browserArguments("click", { ref: "e12" })),
        ],
      },
      {
        status: "completed",
        output_parsed: {
          outcome: "succeeded",
          text: "Violet is registered for Adventure Camp, June 15–19, 2027. The camp confirmed it as CAMP-20481.",
        },
        output: [],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          modelRequests.push(request);
          const response = modelResponses.shift();
          if (!response) throw new Error("Unexpected family portal model turn");
          return response;
        },
      },
    } as never);
    const initialState: FamilyWorkStateV1 = {
      kind: "family_work_v1",
      version: 1,
      generation: 0,
      phase: "ready",
      claim: null,
      activePhoneCall: null,
      activeTextMessage: null,
      pendingParticipantRequest: null,
      browserSession: null,
      continuationItems: [],
      pendingCall: null,
      steering: [],
      progressRevision: 0,
      terminal: null,
    };
    const input = {
      workId: "family-work-camp-registration",
      scheduledOccurrence: null,
      objective:
        "Find Violet’s medical form in Gmail, fill out her camp registration, and get it ready for my final review.",
      visibility: "private" as const,
      ownerAdultId: "adult-1",
      origin: familyWorkOrigin(
        "Find Violet’s medical form in Gmail, fill out her camp registration, and get it ready for my final review.",
      ),
      household: {
        householdId: "household-1",
        familyLabel: "Test family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [{ adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" }],
        children: [],
      },
      googleConnections: [
        {
          emailLabel: "Personal Google",
          calendarAvailable: true,
          kind: "personal" as const,
          writesEnabled: true,
        },
      ],
      state: initialState,
      currentTime: NOW,
    };
    const browserOperations: FlorenceBrowserOperation[] = [];
    const openedAttachmentRefs: string[] = [];
    const reads = {
      async runGoogleWorkspace(operation: GoogleWorkspaceOperation): Promise<GoogleWorkspaceResult> {
        if (operation.operation !== "gmail_get") {
          throw new Error(`Unexpected Google operation ${operation.operation}`);
        }
        return {
          operation: operation.operation,
          result: {
            message: {
              messageId: "gmail-school-message",
              threadId: "gmail-thread-1",
              historyId: "gmail-history-1",
              subject: "Camp medical form",
              body: "Please upload the attached medical form during camp registration.",
              attachments: [
                {
                  attachmentId: "gmail-attachment-1",
                  partId: "1",
                  filename: gmail.attachments[0]?.filename ?? "form.pdf",
                  mimeType: gmail.attachments[0]?.mimeType ?? "application/pdf",
                  sizeBytes: gmail.attachments[0]?.sizeBytes ?? 5,
                },
              ],
            },
          },
        };
      },
      async readWorkspaceGmailSource(identity: { messageId: string; threadId: string; historyId: string }) {
        expect(identity).toEqual({
          messageId: "gmail-school-message",
          threadId: "gmail-thread-1",
          historyId: "gmail-history-1",
        });
        return gmail;
      },
      async readGmailAttachment(input: { sourceId: string; attachment: (typeof gmail.attachments)[number] }) {
        expect(input).toEqual({ sourceId: gmail.sourceId, attachment: gmail.attachments[0] });
        openedAttachmentRefs.push(input.attachment.attachmentRef);
        return {
          sourceId: input.sourceId,
          attachmentRef: input.attachment.attachmentRef,
          filename: "violet-medical-form.pdf",
          mimeType: input.attachment.mimeType,
          bytes: Uint8Array.from([0x25, 0x50, 0x44, 0x46]),
        };
      },
      async runBrowser(operation: FlorenceBrowserOperation) {
        browserOperations.push(operation);
        switch (operation.kind) {
          case "navigate":
            return browserObservation({
              title: "Family Camp Portal",
              url: portalUrl,
              snapshot: "- button Sign in [ref=e1]",
            });
          case "owner_handoff":
            return browserObservation({
              kind: "owner_handoff",
              title: "Family Camp Portal",
              url: portalUrl,
              snapshot: "- button Sign in [ref=e1]",
              liveViewUrl,
              reason: "The parent can sign in through the live browser.",
            });
          case "playwright":
            expect(operation.code).toContain('getByLabel("Child name")');
            return browserObservation({
              title: "Camp registration",
              url: portalUrl,
              snapshot:
                '- textbox "Child name" [ref=e5] value="Violet Williams"\n- button "Upload medical form" [ref=e7]\n- button Preview [ref=e9]',
            });
          case "upload":
            expect(operation).toMatchObject({
              sourceId: gmail.sourceId,
              attachmentRef: gmailAttachmentRef,
            });
            return browserObservation({
              title: "Camp registration",
              url: portalUrl,
              snapshot:
                '- textbox "Child name" [ref=e5] value="Violet Williams"\n- text "violet-medical-form.pdf attached"\n- button Preview [ref=e9]',
            });
          case "computer":
            expect(operation.actions).toEqual([{ type: "move_mouse", x: 400, y: 300 }]);
            expect(operation.screenshot).toBe(true);
            return browserObservation({
              title: "Camp registration",
              url: portalUrl,
              snapshot: '- textbox "Child name" [ref=e5] value="Violet Williams"\n- button Preview [ref=e9]',
              screenshot: {
                mimeType: "image/png" as const,
                bytes: Uint8Array.from([0x89, 0x50, 0x4e, 0x47]),
              },
            });
          case "click": {
            if (operation.ref === "e9") {
              return browserObservation({
                title: "Review registration",
                url: `${portalUrl}/review`,
                snapshot:
                  '- heading "Review registration"\n- text "Violet Williams"\n- text "Adventure Camp, June 15–19, 2027"\n- text "Medical form attached"\n- text "Total $425"\n- button Submit registration [ref=e12]',
              });
            }
            if (operation.ref === "e12") {
              return browserObservation({
                kind: "uncertain_effect",
                reason:
                  "The browser connection ended after the submit click, but the current provider page shows the result.",
                title: "Registration confirmed",
                url: `${portalUrl}/confirmation/CAMP-20481`,
                snapshot:
                  '- heading "Registration confirmed"\n- text "Confirmation CAMP-20481"\n- text "Violet Williams"\n- text "Adventure Camp, June 15–19, 2027"',
              });
            }
            throw new Error(`Unexpected browser click ref ${operation.ref}`);
          }
          default:
            throw new Error(`Unexpected browser operation ${String(operation.kind)}`);
        }
      },
    };

    const navigationPlanned = await reasoner.continueFamilyWork(input, reads);
    if (navigationPlanned.kind !== "continue") throw new Error("Portal navigation was not planned");
    const navigated = await reasoner.continueFamilyWork({ ...input, state: navigationPlanned.state }, reads);
    if (navigated.kind !== "continue") throw new Error("Portal navigation did not settle");
    expect(navigated).toMatchObject({ progressText: null, nextCheckDelayMs: 0 });

    const handoffPlanned = await reasoner.continueFamilyWork({ ...input, state: navigated.state }, reads);
    if (handoffPlanned.kind !== "continue") throw new Error("Portal handoff was not planned");
    const handedOff = await reasoner.continueFamilyWork({ ...input, state: handoffPlanned.state }, reads);
    if (handedOff.kind !== "continue") throw new Error("Portal handoff did not settle");
    const waiting = await reasoner.continueFamilyWork({ ...input, state: handedOff.state }, reads);
    expect(waiting).toMatchObject({
      kind: "waiting",
      question: expect.stringContaining("sign in"),
    });
    if (waiting.kind !== "waiting") throw new Error("Portal work did not wait for sign-in");
    expect(waiting.question).toContain(liveViewUrl);
    expect(JSON.stringify(modelRequests[4]?.input)).toContain(liveViewUrl);

    const signedInState = steerFamilyWorkState(waiting.state, {
      sourceId: "00000000-0000-4000-8000-000000000002",
      text: "I’m signed in—keep going.",
      occurredAt: "2026-08-27T20:02:00.000Z",
    });
    const fillPlanned = await reasoner.continueFamilyWork({ ...input, state: signedInState }, reads);
    if (fillPlanned.kind !== "continue") throw new Error("Camp form fill was not planned");
    const filled = await reasoner.continueFamilyWork({ ...input, state: fillPlanned.state }, reads);
    if (filled.kind !== "continue") throw new Error("Camp form fill did not settle");

    const uploadPlanned = await reasoner.continueFamilyWork({ ...input, state: filled.state }, reads);
    if (uploadPlanned.kind !== "continue") throw new Error("Camp attachment upload was not planned");
    const uploaded = await reasoner.continueFamilyWork({ ...input, state: uploadPlanned.state }, reads);
    if (uploaded.kind !== "continue") throw new Error("Camp attachment upload did not settle");

    const screenshotPlanned = await reasoner.continueFamilyWork({ ...input, state: uploaded.state }, reads);
    if (screenshotPlanned.kind !== "continue") throw new Error("Camp form inspection was not planned");
    const inspected = await reasoner.continueFamilyWork({ ...input, state: screenshotPlanned.state }, reads);
    if (inspected.kind !== "continue") throw new Error("Camp form inspection did not settle");
    expect(JSON.stringify(inspected.state.continuationItems)).toContain("input_image");

    const reviewPlanned = await reasoner.continueFamilyWork({ ...input, state: inspected.state }, reads);
    if (reviewPlanned.kind !== "continue") throw new Error("Camp review was not planned");
    expect(JSON.stringify(modelRequests[8]?.input)).toContain("input_image");
    expect(JSON.stringify(reviewPlanned.state.continuationItems)).not.toContain("input_image");
    const reviewed = await reasoner.continueFamilyWork({ ...input, state: reviewPlanned.state }, reads);
    if (reviewed.kind !== "continue") throw new Error("Camp review did not settle");
    const awaitingApproval = await reasoner.continueFamilyWork({ ...input, state: reviewed.state }, reads);
    expect(awaitingApproval).toMatchObject({
      kind: "waiting",
      question: expect.stringContaining("$425"),
    });
    if (awaitingApproval.kind !== "waiting") {
      throw new Error("Camp registration did not wait for final authorization");
    }
    expect(String(modelRequests[0]?.instructions)).toContain(
      "whose requested endpoint leaves the family free to choose does not authorize",
    );
    expect(awaitingApproval.state).toMatchObject({ phase: "waiting", pendingCall: null });
    expect(browserOperations.some((operation) => operation.kind === "click" && operation.ref === "e12")).toBe(
      false,
    );

    const submitSteeringState = steerFamilyWorkState(awaitingApproval.state, {
      sourceId: "00000000-0000-4000-8000-000000000003",
      text: "Yes—submit this exact registration now.",
      occurredAt: "2026-08-27T20:04:00.000Z",
    });
    const submitPlanned = await reasoner.continueFamilyWork({ ...input, state: submitSteeringState }, reads);
    if (submitPlanned.kind !== "continue") throw new Error("Camp submission was not planned");
    const submitted = await reasoner.continueFamilyWork({ ...input, state: submitPlanned.state }, reads);
    if (submitted.kind !== "continue") throw new Error("Camp submission did not settle");
    const terminal = await reasoner.continueFamilyWork({ ...input, state: submitted.state }, reads);

    expect(browserOperations.map((operation) => operation.kind)).toEqual([
      "navigate",
      "owner_handoff",
      "playwright",
      "upload",
      "computer",
      "click",
      "click",
    ]);
    expect(
      browserOperations.filter((operation) => operation.kind === "click" && operation.ref === "e12"),
    ).toHaveLength(1);
    expect(openedAttachmentRefs).toEqual([gmailAttachmentRef]);
    expect(JSON.stringify(modelRequests)).toContain(gmail.sourceId);
    expect(JSON.stringify(modelRequests[5]?.input)).toContain("I’m signed in—keep going.");
    expect(awaitingApproval.question).toContain("June 15–19, 2027");
    expect(JSON.stringify(modelRequests[10]?.input)).toContain("Yes—submit this exact registration now.");
    expect(JSON.stringify(modelRequests[11]?.input)).toContain("uncertain_effect");
    expect(JSON.stringify(modelRequests[11]?.input)).toContain("CAMP-20481");
    expect(JSON.stringify(modelRequests[11]?.input)).toContain("Adventure Camp, June 15–19, 2027");
    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("CAMP-20481"),
    });
    if (terminal.kind !== "terminal") throw new Error("Camp registration did not finish");
    expect(terminal.text).toContain("Adventure Camp, June 15–19, 2027");
  });

  test("browser upload stages exact bytes once and observes an uncertain retry", async () => {
    const sessionPayload = {
      id: "browser-session-upload-1",
      expiresAt: "2026-08-29T20:00:00.000Z",
      connectUrl: "wss://connect.browserbase.example/session-1",
      projectId: "project-1",
      status: "RUNNING",
    };
    let uploadedPath: string | null = null;
    let uploadedBytes: Uint8Array | null = null;
    let uploadedMode: number | null = null;
    let uploadCommands = 0;
    const commandRunner = async (input: { readonly args: readonly string[] }) => {
      const command = input.args[7];
      let data: Record<string, unknown> = {};
      if (command === "upload") {
        uploadCommands += 1;
        uploadedPath = input.args[9] ?? null;
        if (!uploadedPath) throw new Error("Upload command omitted its temporary file");
        uploadedBytes = new Uint8Array(await readFile(uploadedPath));
        uploadedMode = (await stat(uploadedPath)).mode & 0o777;
      } else if (command === "snapshot") {
        data = {
          snapshot: '- text "medical-form.pdf attached"\n- button Submit [ref=e12]',
          refs: { e12: { role: "button", name: "Submit" } },
        };
      } else if (command === "get") {
        data =
          input.args[8] === "url" ? { url: "https://camp.example/register" } : { title: "Camp registration" };
      }
      return {
        exitCode: 0,
        stdout: JSON.stringify({ success: true, data }),
        stderr: "",
        timedOut: false,
        cancelled: false,
        stdoutTruncated: false,
      };
    };
    const client = new BrowserbaseBrowserClient({
      apiKey: "browserbase-test-key",
      projectId: "project-1",
      now: () => Date.parse(NOW),
      fetch: (async () =>
        new Response(JSON.stringify(sessionPayload), {
          status: 200,
          headers: { "content-type": "application/json" },
        })) as typeof globalThis.fetch,
      commandRunner: commandRunner as never,
    });
    const navigated = await client.run({
      householdId: "household-1",
      workId: "work-upload-1",
      ownerAdultId: "adult-1",
      callId: "navigate-1",
      attempt: 1,
      session: null,
      operation: { kind: "navigate", url: "https://camp.example/register" },
    });
    const fileBytes = Uint8Array.from([0x25, 0x50, 0x44, 0x46, 0x2d, 0x31, 0x2e, 0x37]);
    const uploaded = await client.run({
      householdId: "household-1",
      workId: "work-upload-1",
      ownerAdultId: "adult-1",
      callId: "upload-1",
      attempt: 1,
      session: navigated.session,
      operation: { kind: "upload", ref: "e7", attachmentRef: "document-1" },
      uploadFile: { filename: "medical-form.pdf", bytes: fileBytes },
    });

    expect(uploadCommands).toBe(1);
    expect(uploadedBytes).toEqual(fileBytes);
    expect(uploadedMode).toBe(0o600);
    if (!uploadedPath) throw new Error("Upload path was not observed");
    await expect(access(uploadedPath)).rejects.toMatchObject({ code: "ENOENT" });
    expect(uploaded.observation.snapshot).toContain("medical-form.pdf attached");

    const retried = await client.run({
      householdId: "household-1",
      workId: "work-upload-1",
      ownerAdultId: "adult-1",
      callId: "upload-1",
      attempt: 2,
      session: uploaded.session,
      operation: { kind: "upload", ref: "e7", attachmentRef: "document-1" },
    });
    expect(uploadCommands).toBe(1);
    expect(retried.observation.kind).toBe("uncertain_effect");
  });

  test("Kernel keeps one household profile, composes browser tactics, and saves owner sign-in", async () => {
    const profileName = kernelProfileName("household-kernel-1");
    expect(profileName).toMatch(/^florence-[a-f0-9]{40}$/u);
    expect(profileName).not.toContain("household-kernel-1");

    let profileExists = false;
    const createdBrowsers: Record<string, unknown>[] = [];
    const browserSessions = new Map<string, Record<string, unknown>>();
    const deletedBrowsers: string[] = [];
    const playwrightCalls: Record<string, unknown>[] = [];
    const computerBatches: Record<string, unknown>[] = [];
    let uploads = 0;
    let uploadedBytes: Uint8Array | null = null;

    const kernel = {
      profiles: {
        async retrieve(name: string) {
          expect(name).toBe(profileName);
          if (!profileExists) throw Object.assign(new Error("missing profile"), { status: 404 });
          return { id: "profile-kernel-1", name };
        },
        async create(input: { readonly name: string }) {
          expect(input).toEqual({ name: profileName });
          profileExists = true;
          throw Object.assign(new Error("profile created concurrently"), { status: 409 });
        },
      },
      browsers: {
        async *list() {
          for (const session of browserSessions.values()) yield session;
        },
        async create(input: Record<string, unknown>) {
          createdBrowsers.push(input);
          const id = `kernel-session-${createdBrowsers.length}`;
          const profile = input.profile as { readonly save_changes: boolean };
          const session = {
            session_id: id,
            cdp_ws_url: `wss://kernel.example/${id}`,
            browser_live_view_url: `https://kernel.example/live/${id}`,
            profile: { id: "profile-kernel-1", name: profileName },
            profile_save_changes: profile.save_changes,
            timeout_seconds: 259_200,
          };
          browserSessions.set(id, session);
          return session;
        },
        async retrieve(id: string) {
          const session = browserSessions.get(id);
          if (!session) throw Object.assign(new Error("missing browser"), { status: 404 });
          return session;
        },
        async deleteByID(id: string) {
          deletedBrowsers.push(id);
          browserSessions.delete(id);
        },
        playwright: {
          async execute(_id: string, input: Record<string, unknown>) {
            playwrightCalls.push(input);
            return { success: true, result: { route: "JFK to LAX", alternatives: 3 } };
          },
        },
        computer: {
          async batch(_id: string, input: Record<string, unknown>) {
            computerBatches.push(input);
          },
          async captureScreenshot() {
            return new Response(Uint8Array.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]));
          },
        },
      },
    };
    const commandRunner = async (input: { readonly args: readonly string[] }) => {
      const commandIndex = input.args.findIndex((value) =>
        ["open", "snapshot", "get", "upload", "close"].includes(value),
      );
      const command = input.args[commandIndex];
      let data: Record<string, unknown> = {};
      if (command === "snapshot") {
        data = {
          snapshot: '- textbox "Search" [ref=e1]\n- button "Continue" [ref=e2]',
          refs: { e1: {}, e2: {} },
        };
      } else if (command === "get") {
        data =
          input.args[commandIndex + 1] === "url"
            ? { url: "https://family.example/current" }
            : { title: "Family account" };
      } else if (command === "upload") {
        uploads += 1;
        const path = input.args[commandIndex + 2];
        if (!path) throw new Error("Kernel upload omitted its temporary file");
        uploadedBytes = new Uint8Array(await readFile(path));
      }
      return {
        exitCode: 0,
        stdout: JSON.stringify({ success: true, data }),
        stderr: "",
        timedOut: false,
        cancelled: false,
        stdoutTruncated: false,
      };
    };
    const client = new KernelBrowserClient({
      apiKey: "kernel-test-key",
      client: kernel as never,
      commandRunner: commandRunner as never,
      now: () => Date.parse(NOW),
    });

    const navigated = await client.run({
      householdId: "household-kernel-1",
      workId: "kernel-work-1",
      ownerAdultId: "adult-1",
      callId: "navigate-kernel",
      attempt: 1,
      session: null,
      operation: { kind: "navigate", url: "https://family.example/start" },
    });
    expect(createdBrowsers[0]).toMatchObject({
      profile: { id: "profile-kernel-1", save_changes: false },
      start_url: "https://family.example/start",
      stealth: true,
      timeout_seconds: 259_200,
    });

    const playwright = await client.run({
      householdId: "household-kernel-1",
      workId: "kernel-work-1",
      ownerAdultId: "adult-1",
      callId: "playwright-kernel",
      attempt: 1,
      session: navigated.session,
      operation: {
        kind: "playwright",
        code: "return { route: await page.title(), alternatives: 3 };",
        timeoutSeconds: 120,
      },
    });
    expect(playwrightCalls).toEqual([
      {
        code: "return { route: await page.title(), alternatives: 3 };",
        timeout_sec: 120,
      },
    ]);
    expect(playwright.observation.snapshot).toContain('"alternatives":3');

    const computerOperation = {
      kind: "computer" as const,
      actions: [
        { type: "click_mouse" as const, x: 120, y: 240 },
        { type: "type_text" as const, text: "Violet" },
      ],
      screenshot: true,
    };
    const computer = await client.run({
      householdId: "household-kernel-1",
      workId: "kernel-work-1",
      ownerAdultId: "adult-1",
      callId: "computer-kernel",
      attempt: 1,
      session: playwright.session,
      operation: computerOperation,
    });
    expect(computerBatches).toEqual([
      {
        actions: [
          { type: "click_mouse", click_mouse: { x: 120, y: 240 } },
          { type: "type_text", type_text: { text: "Violet" } },
        ],
      },
    ]);
    expect(computer.observation.screenshot).toMatchObject({ mimeType: "image/png" });

    const uncertainComputer = await client.run({
      householdId: "household-kernel-1",
      workId: "kernel-work-1",
      ownerAdultId: "adult-1",
      callId: "computer-kernel",
      attempt: 2,
      session: computer.session,
      operation: computerOperation,
    });
    expect(computerBatches).toHaveLength(1);
    expect(uncertainComputer.observation.kind).toBe("uncertain_effect");

    const fileBytes = Uint8Array.from([0x25, 0x50, 0x44, 0x46]);
    const uploaded = await client.run({
      householdId: "household-kernel-1",
      workId: "kernel-work-1",
      ownerAdultId: "adult-1",
      callId: "upload-kernel",
      attempt: 1,
      session: uncertainComputer.session,
      operation: { kind: "upload", ref: "e1", attachmentRef: "document-1" },
      uploadFile: { filename: "camp-form.pdf", bytes: fileBytes },
    });
    expect(uploads).toBe(1);
    expect(uploadedBytes).toEqual(fileBytes);

    const handoff = await client.run({
      householdId: "household-kernel-1",
      workId: "kernel-work-1",
      ownerAdultId: "adult-1",
      callId: "owner-handoff-kernel",
      attempt: 1,
      session: uploaded.session,
      operation: { kind: "owner_handoff" },
    });
    expect(handoff.session.sessionId).toBe("kernel-session-2");
    expect(handoff.observation.liveViewUrl).toBe("https://kernel.example/live/kernel-session-2");
    expect(createdBrowsers[1]).toMatchObject({
      profile: { id: "profile-kernel-1", save_changes: true },
      start_url: "https://family.example/current",
      stealth: true,
    });
    expect(deletedBrowsers).toEqual(["kernel-session-1"]);

    await client.close(handoff.session);
    expect(deletedBrowsers).toEqual(["kernel-session-1", "kernel-session-2"]);
  });

  test("ordinary route questions use the dedicated maps tools and start visible work once", async () => {
    const requests: Record<string, unknown>[] = [];
    const mapRequests: unknown[] = [];
    let workStarts = 0;
    let modelTurn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          modelTurn += 1;
          return fakeStream(
            modelTurn === 1
              ? {
                  status: "completed",
                  output_parsed: null,
                  output: [
                    functionCall("route-call", "maps_distance", {
                      origin: "LAX",
                      destination: "Wish Charter School, Los Angeles",
                      mode: "driving",
                    }),
                  ],
                }
              : { status: "completed", output_parsed: ordinaryDecision(), output: [] },
          );
        },
      },
    } as never);

    const input = foregroundInput();
    input.currentMessage.text = "How is traffic on that drive right now?";
    input.currentMessage.authoredText = input.currentMessage.text;
    input.recentMessages = [
      {
        sourceId: "earlier-route",
        senderName: "Hari",
        text: "We are driving from LAX to Wish Charter School in Los Angeles.",
        occurredAt: "2026-08-27T19:55:00.000Z",
      },
    ];
    await reasoner.decide(
      input,
      {
        ...inertReads(),
        async runMaps(request) {
          mapRequests.push(request);
          return {
            operation: "distance" as const,
            origin: {
              query: "LAX",
              displayName: "Los Angeles International Airport",
              lat: 33.9416,
              lon: -118.4085,
            },
            destination: {
              query: "Wish Charter School, Los Angeles",
              displayName: "Wish Charter School, Los Angeles",
              lat: 33.958,
              lon: -118.416,
            },
            mode: "driving" as const,
            distanceM: 4_200,
            durationSeconds: 720,
            straightLineM: 2_000,
            attribution: [
              {
                provider: "OpenStreetMap",
                label: "© OpenStreetMap contributors",
                url: "https://www.openstreetmap.org/copyright",
              },
              {
                provider: "Valhalla",
                label: "Routing by Valhalla's FOSSGIS public service",
                url: "https://valhalla.openstreetmap.de/",
              },
            ],
          };
        },
      },
      undefined,
      {
        onWorkStarted() {
          workStarts += 1;
        },
      },
    );

    expect(mapRequests).toEqual([
      {
        operation: "distance",
        origin: "LAX",
        destination: "Wish Charter School, Los Angeles",
        mode: "driving",
      },
    ]);
    expect(workStarts).toBe(1);
    const toolNames = ((requests[0]?.tools as { name: string }[]) ?? []).map((tool) => tool.name);
    expect(toolNames).toEqual(
      expect.arrayContaining([
        "maps_area",
        "maps_bounds",
        "maps_directions",
        "maps_distance",
        "maps_nearby",
        "maps_reverse",
        "maps_search",
        "maps_time_zone",
      ]),
    );
    const result = functionOutputEnvelopes(requests[1]).find((envelope) => envelope.callId === "route-call");
    expect(result).toMatchObject({
      outcome: "succeeded",
      output: {
        operation: "distance",
        mode: "driving",
        distanceM: 4_200,
        durationSeconds: 720,
      },
    });
    expect(String(requests[0]?.instructions)).not.toContain(
      "route endpoints are already in the parent's current typed request",
    );
  });

  test("ordinary weather questions resolve a place and use live NWS weather with one work cue", async () => {
    const requests: Record<string, unknown>[] = [];
    const weatherRequests: unknown[] = [];
    let workStarts = 0;
    let modelTurn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          modelTurn += 1;
          return fakeStream(
            modelTurn === 1
              ? {
                  status: "completed",
                  output_parsed: null,
                  output: [
                    functionCall("weather-place", "maps_search", {
                      query: "Los Angeles, CA",
                      limit: 1,
                    }),
                  ],
                }
              : modelTurn === 2
                ? {
                    status: "completed",
                    output_parsed: null,
                    output: [
                      functionCall("weather-live", "weather_forecast", {
                        coordinates: { lat: 34.0522, lon: -118.2437 },
                        kind: "hourly",
                        periodCount: 12,
                      }),
                    ],
                  }
                : {
                    status: "completed",
                    output_parsed: ordinaryDecision({
                      bubbleText:
                        "No rain is expected this evening in Los Angeles; it should stay mostly sunny.",
                    }),
                    output: [],
                  },
          );
        },
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text = "Will it rain in Los Angeles this evening?";
    input.currentMessage.authoredText = input.currentMessage.text;

    const result = await reasoner.decide(
      input,
      {
        ...inertReads(),
        async runMaps() {
          return {
            operation: "search" as const,
            query: "Los Angeles, CA",
            count: 1,
            results: [
              {
                name: "Los Angeles",
                displayName: "Los Angeles, Los Angeles County, California, United States",
                lat: 34.0522,
                lon: -118.2437,
                type: "city",
                category: "place",
                osmType: "relation",
                osmId: "207359",
                importance: 0.9,
                boundingBox: null,
                mapsUrl: "https://www.google.com/maps/search/?api=1&query=34.0522%2C-118.2437",
              },
            ],
            attribution: [
              {
                provider: "OpenStreetMap",
                label: "© OpenStreetMap contributors",
                url: "https://www.openstreetmap.org/copyright",
              },
            ],
          };
        },
        async runWeather(request) {
          weatherRequests.push(request);
          return weatherResult();
        },
      },
      undefined,
      {
        onWorkStarted() {
          workStarts += 1;
        },
      },
    );

    expect(weatherRequests).toEqual([
      {
        coordinates: { lat: 34.0522, lon: -118.2437 },
        kind: "hourly",
        periodCount: 12,
      },
    ]);
    expect(workStarts).toBe(1);
    expect(result.conversation.bubbles[0]?.text).toContain("No rain is expected");
    expect(((requests[0]?.tools as { name: string }[]) ?? []).map((tool) => tool.name)).toEqual(
      expect.arrayContaining(["maps_search", "weather_forecast"]),
    );
    const weatherParameters = (
      (requests[0]?.tools as Array<{ name: string; parameters?: unknown }>) ?? []
    ).find((tool) => tool.name === "weather_forecast")?.parameters as
      | { properties?: { periodCount?: { maximum?: number } } }
      | undefined;
    expect(weatherParameters?.properties?.periodCount?.maximum).toBe(48);
    expect(
      functionOutputEnvelopes(requests[2]).find((envelope) => envelope.callId === "weather-live"),
    ).toMatchObject({
      outcome: "succeeded",
      output: {
        location: { city: "Los Angeles", state: "CA" },
        periods: [expect.objectContaining({ condition: "Mostly Sunny" })],
      },
    });
  });

  test("a flight identifier resolves live route and status before searching real alternatives", async () => {
    const requests: Record<string, unknown>[] = [];
    const flightRequests: unknown[] = [];
    const statusUrl = "https://www.delta.com/flight-status/search?flightId=DL747";
    const bookingUrl = "https://www.kiwi.com/deep?from=JFK&to=LAX&date=2026-08-27";
    let workStarts = 0;
    let modelTurn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          modelTurn += 1;
          return fakeStream(
            modelTurn === 1
              ? {
                  status: "completed",
                  output_parsed: null,
                  output: [
                    completedWebSearch(
                      statusUrl,
                      "DL 747 status route tonight JFK LAX",
                      "flight-status-search",
                    ),
                    functionCall("flight-options", "flights_search", {
                      origin: "JFK",
                      destination: "LAX",
                      departureDate: "2026-08-27",
                      returnDate: null,
                      adults: 1,
                      children: 0,
                      infants: 0,
                      cabinClass: "economy",
                      preferredAirlines: [],
                      maxStops: 0,
                      outboundDepartureHours: { from: 17, to: 23 },
                      maxPrice: null,
                      allowSelfTransfer: false,
                      allowOvernightStopovers: false,
                      allowAirportChanges: false,
                      sort: "quality",
                    }),
                  ],
                }
              : {
                  status: "completed",
                  output_parsed: ordinaryDecision({
                    bubbleText:
                      "DL 747 is delayed tonight from JFK to LAX. I found a direct alternative leaving at 7:00 PM for $412.",
                    researchUrls: [statusUrl, bookingUrl],
                  }),
                  output: [],
                },
          );
        },
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text =
      "My wife's flight is delayed tonight. Can you find other options? DL 747 is the original.";
    input.currentMessage.authoredText = input.currentMessage.text;

    const result = await reasoner.decide(
      input,
      {
        ...inertReads(),
        async runFlights(request) {
          flightRequests.push(request);
          return flightResult(bookingUrl);
        },
      },
      undefined,
      {
        onWorkStarted() {
          workStarts += 1;
        },
      },
    );

    expect(flightRequests).toEqual([
      expect.objectContaining({
        operation: "search",
        origin: "JFK",
        destination: "LAX",
        departureDate: "2026-08-27",
        maxStops: 0,
        allowSelfTransfer: false,
        allowOvernightStopovers: false,
        allowAirportChanges: false,
      }),
    ]);
    expect(result.researchUrls).toEqual([statusUrl, bookingUrl]);
    expect(result.conversation.bubbles[0]?.text).toContain("direct alternative");
    expect(result.conversation.bubbles[0]?.text).not.toMatch(
      /what(?:'s| is) (?:the )?(?:origin|destination)/iu,
    );
    expect(workStarts).toBe(1);
    expect(
      functionOutputEnvelopes(requests[1]).find((envelope) => envelope.callId === "flight-options"),
    ).toMatchObject({
      outcome: "succeeded",
      output: {
        operation: "search",
        returnedCount: 1,
        timeBasis: "provider_local_time_at_each_airport",
      },
    });
  });

  test("durable flight work resumes from its checkpoint, accepts steering, and reaches one result", async () => {
    const statusUrl = "https://www.delta.com/flight-status/search?flightId=DL747";
    const bookingUrl = "https://www.kiwi.com/deep?from=JFK&to=LAX&date=2026-08-27";
    const secondBookingUrl = `${bookingUrl}&option=2`;
    const firstRequests: Record<string, unknown>[] = [];
    const firstResponses = [
      {
        status: "completed",
        output_parsed: {
          outcome: "deferred",
          text: null,
          resumeAt: "2026-08-27T20:05:00.000Z",
          progressText: null,
        },
        output: [
          completedWebSearch(statusUrl, "DL 747 live status route tonight", "durable-flight-status-search"),
        ],
      },
    ];
    const firstReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          firstRequests.push(request);
          const response = firstResponses.shift();
          if (!response) throw new Error("Unexpected first durable-work request");
          return response;
        },
      },
    } as never);
    const initialState: FamilyWorkStateV1 = {
      kind: "family_work_v1",
      version: 1,
      generation: 0,
      phase: "ready",
      claim: null,
      activePhoneCall: null,
      activeTextMessage: null,
      pendingParticipantRequest: null,
      browserSession: null,
      continuationItems: [],
      pendingCall: null,
      steering: [],
      progressRevision: 0,
      terminal: null,
    };
    const input = {
      workId: "family-work-1",
      scheduledOccurrence: null,
      objective: "DL 747 is delayed tonight. Find the two best nonstop alternatives; Delta if possible.",
      visibility: "household" as const,
      ownerAdultId: null,
      origin: familyWorkOrigin(
        "DL 747 is delayed tonight. Find the two best nonstop alternatives; Delta if possible.",
      ),
      household: {
        householdId: "household-1",
        familyLabel: "Test family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [
          { adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" },
          { adultId: "adult-2", firstName: "Jackson", displayName: "Jackson Williams" },
        ],
        children: [],
      },
      state: initialState,
      currentTime: NOW,
    };

    const checkedStatus = await firstReasoner.continueFamilyWork(input, {});
    expect(checkedStatus).toMatchObject({
      kind: "deferred",
      progressText: null,
      state: { phase: "ready", progressRevision: 0 },
    });
    if (checkedStatus.kind !== "deferred") throw new Error("Status lookup did not settle inline");
    expect(JSON.stringify(checkedStatus.state.continuationItems)).toContain(statusUrl);
    expect(firstRequests[0]?.tools).toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "web_search" })]),
    );

    const resumedRequests: Record<string, unknown>[] = [];
    const resumedResponses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("durable-flight-options-steered", "flights_search", {
            origin: "JFK",
            destination: "LAX",
            departureDate: "2026-08-27",
            returnDate: null,
            adults: 1,
            children: 0,
            infants: 0,
            cabinClass: "economy",
            preferredAirlines: ["DL", "B6"],
            maxStops: 0,
            outboundDepartureHours: { from: 19, to: 23 },
            maxPrice: null,
            allowSelfTransfer: false,
            allowOvernightStopovers: false,
            allowAirportChanges: false,
            sort: "quality",
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: {
          outcome: "succeeded",
          text: `1. Delta nonstop at 7:00 PM for $412: ${bookingUrl}\n2. JetBlue nonstop at 8:15 PM for $438: ${secondBookingUrl}`,
        },
        output: [],
      },
    ];
    const resumedReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          resumedRequests.push(request);
          const response = resumedResponses.shift();
          if (!response) throw new Error("Unexpected resumed durable-work request");
          return response;
        },
      },
    } as never);
    let flightSearches = 0;
    const steeredState = steerFamilyWorkState(checkedStatus.state, {
      sourceId: "00000000-0000-4000-8000-000000000001",
      text: "JetBlue is fine too, but nothing before 7 PM.",
      occurredAt: "2026-08-27T20:01:00.000Z",
    });
    expect(steeredState).toMatchObject({
      phase: "ready",
      generation: 1,
      pendingCall: null,
    });
    expect(JSON.stringify(steeredState.continuationItems)).toContain("durable-flight-status");
    const terminal = await resumedReasoner.continueFamilyWork(
      { ...input, state: steeredState },
      {
        async runFlights() {
          flightSearches += 1;
          return flightResult(bookingUrl, 2);
        },
      },
    );

    expect(flightSearches).toBe(1);
    expect(JSON.stringify(resumedRequests[0]?.input)).toContain(
      "JetBlue is fine too, but nothing before 7 PM.",
    );
    expect(JSON.stringify(resumedRequests[1]?.input)).toContain(
      '\\"preferredAirlines\\":[\\"DL\\",\\"B6\\"]',
    );
    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("1. Delta nonstop"),
      state: { phase: "terminal", progressRevision: 1 },
    });
    if (terminal.kind !== "terminal") throw new Error("Durable flight work did not finish");
    expect(terminal.text).toContain("2. JetBlue nonstop");
    expect(terminal.text).toContain(bookingUrl);
    expect(terminal.text).toContain(secondBookingUrl);
  });

  test("one durable household objective composes Vault, reminder, and Family Calendar receipts", async () => {
    const calendarRef = "calendar-family";
    const recipeFileAssetId = "10000000-0000-4000-8000-000000000010";
    const modelRequests: Record<string, unknown>[] = [];
    const responses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("remember-dinner", "vault_work", {
            operation: "remember",
            factId: null,
            statement: "Tuesday dinner is sheet-pan chicken with lemon potatoes.",
            visibility: "household",
            memory: {
              memoryKind: "artifact",
              artifactKind: "recipe",
              title: "Sheet-pan chicken with lemon potatoes",
              details: "Roast chicken thighs and lemon potatoes together at 425°F until browned.",
              tags: ["dinner", "chicken"],
            },
            sourceIds: ["source-adult-1"],
            fileAssetIds: [recipeFileAssetId],
            expectedUpdatedAt: null,
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("list-dinner-reminders", "reminder_work", {
            operation: "list",
            reminderId: null,
            action: null,
            schedule: null,
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("remind-dinner", "reminder_work", {
            operation: "create",
            reminderId: null,
            action: "Start Tuesday dinner prep",
            schedule: { kind: "once", at: "2026-09-01T23:00:00.000Z" },
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("list-family-calendar", "list_calendars", {})],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("read-family-calendar", "read_calendar_window", {
            timeMin: "2026-09-01T00:00:00.000Z",
            timeMax: "2026-09-02T00:00:00.000Z",
            pageSize: 50,
            cursor: null,
            scope: "selected",
            calendarRefs: [calendarRef],
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("add-dinner", "family_calendar_work", {
            operation: "create",
            event: {
              intervalKind: "timed",
              title: "Family dinner",
              startsAt: "2026-09-02T01:00:00.000Z",
              endsAt: "2026-09-02T02:00:00.000Z",
              timeZone: "America/Los_Angeles",
              location: null,
            },
            target: null,
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: {
          outcome: "succeeded",
          text: "Dinner is saved, prep is scheduled, and the Family Calendar is updated.",
          resumeAt: null,
          progressText: null,
        },
        output: [],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          modelRequests.push(request);
          const response = responses.shift();
          if (!response) throw new Error("Unexpected household-work request");
          return response;
        },
      },
    } as never);
    const input = {
      workId: "family-work-dinner",
      scheduledOccurrence: null,
      objective: "Save Tuesday's recipe, remind us to start prep, and put dinner on our calendar.",
      visibility: "household" as const,
      ownerAdultId: null,
      origin: familyWorkOrigin(
        "Save Tuesday's recipe, remind us to start prep, and put dinner on our calendar.",
      ),
      household: {
        householdId: "household-1",
        familyLabel: "Test family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [{ adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" }],
        children: [],
      },
      googleConnections: [
        {
          emailLabel: "Test Family Calendar",
          calendarAvailable: true,
          kind: "family" as const,
          writesEnabled: true,
        },
      ],
      state: {
        kind: "family_work_v1" as const,
        version: 1 as const,
        generation: 0,
        phase: "ready" as const,
        claim: null,
        activePhoneCall: null,
        activeTextMessage: null,
        pendingParticipantRequest: null,
        browserSession: null,
        browserFiles: [
          {
            assetId: recipeFileAssetId,
            signalId: "family-work-dinner",
            workId: "family-work-dinner",
            filename: "sheet-pan-chicken.pdf",
            mimeType: "application/pdf",
            byteLength: 2_048,
            sha256: "a".repeat(64),
          },
        ],
        continuationItems: [],
        pendingCall: null,
        steering: [],
        progressRevision: 0,
        terminal: null,
      },
      currentTime: NOW,
    };
    let vaultReceipt: Awaited<
      ReturnType<NonNullable<Parameters<typeof reasoner.continueFamilyWork>[1]["runVaultWork"]>>
    > | null = null;
    let vaultCommits = 0;
    const capabilityCalls: string[] = [];
    const reads = {
      async runVaultWork(request: { fileAssetIds: readonly string[] | null }) {
        capabilityCalls.push("vault_work");
        expect(request.fileAssetIds).toEqual([recipeFileAssetId]);
        if (!vaultReceipt) {
          vaultCommits += 1;
          vaultReceipt = {
            operation: "remember" as const,
            status: "committed" as const,
            factId: "fact-dinner",
            statement: "Tuesday dinner is sheet-pan chicken with lemon potatoes.",
          };
        }
        return vaultReceipt;
      },
      async runReminderWork(request: { operation: string }) {
        capabilityCalls.push(`reminder_work:${request.operation}`);
        if (request.operation === "list") return { status: "listed" as const, reminders: [] };
        return {
          status: "committed" as const,
          operation: "create" as const,
          reminderId: "reminder-dinner",
          action: "Start Tuesday dinner prep",
          schedule: { kind: "once" as const, at: "2026-09-01T23:00:00.000Z" },
          state: "active" as const,
          nextAt: "2026-09-01T23:00:00.000Z",
          lastRunAt: null,
          createdAt: NOW,
          deliveryStatus: null,
        };
      },
      async runFamilyCalendarWork() {
        capabilityCalls.push("family_calendar_work");
        return {
          status: "committed" as const,
          operation: "create" as const,
          providerEventId: "provider-dinner",
          providerRevision: "revision-1",
        };
      },
      async listCalendars() {
        return {
          status: "complete" as const,
          calendars: [
            {
              calendarRef,
              label: "Test Family Calendar",
              timeZone: "America/Los_Angeles",
              primary: null,
              accessRole: null,
              eventCoverage: "readable" as const,
            },
          ],
          totalCalendarCount: 1,
        };
      },
      async readCalendarWindow() {
        return {
          status: "complete" as const,
          calendars: [
            {
              calendarRef,
              label: "Test Family Calendar",
              timeZone: "America/Los_Angeles",
              primary: null,
              accessRole: null,
              status: "complete" as const,
              eventCount: 0,
            },
          ],
          totalCalendarCount: 1,
          events: [],
          totalEventCount: 0,
          nextCursor: null,
        };
      },
    };

    const vaultPlanned = await reasoner.continueFamilyWork(input, reads);
    if (vaultPlanned.kind !== "continue") throw new Error("Vault work was not planned");
    const vaultSettled = await reasoner.continueFamilyWork({ ...input, state: vaultPlanned.state }, reads);
    const vaultCrashReplay = await reasoner.continueFamilyWork(
      { ...input, state: vaultPlanned.state },
      reads,
    );
    expect(vaultSettled).toMatchObject({ kind: "continue", state: { phase: "ready" } });
    expect(vaultCrashReplay).toMatchObject({ kind: "continue", state: { phase: "ready" } });
    expect(vaultCommits).toBe(1);
    if (vaultSettled.kind !== "continue") throw new Error("Vault work did not settle");

    const reminderPlanned = await reasoner.continueFamilyWork({ ...input, state: vaultSettled.state }, reads);
    if (reminderPlanned.kind !== "continue") throw new Error("Reminder work was not planned");
    const reminderSettled = await reasoner.continueFamilyWork(
      { ...input, state: reminderPlanned.state },
      reads,
    );
    if (reminderSettled.kind !== "continue") throw new Error("Reminder work did not settle");

    const calendarPlanned = await reasoner.continueFamilyWork(
      { ...input, state: reminderSettled.state },
      reads,
    );
    if (calendarPlanned.kind !== "continue") throw new Error("Calendar work was not planned");
    const calendarSettled = await reasoner.continueFamilyWork(
      { ...input, state: calendarPlanned.state },
      reads,
    );
    if (calendarSettled.kind !== "continue") throw new Error("Calendar work did not settle");
    const terminal = await reasoner.continueFamilyWork({ ...input, state: calendarSettled.state }, reads);

    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("Family Calendar is updated"),
    });
    expect(capabilityCalls).toEqual([
      "vault_work",
      "vault_work",
      "reminder_work:list",
      "reminder_work:create",
      "family_calendar_work",
    ]);
    expect(modelRequests[0]?.tools).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "vault_work" }),
        expect.objectContaining({ name: "reminder_work" }),
        expect.objectContaining({ name: "family_calendar_work" }),
      ]),
    );
  });

  test("a pending Vault correction keeps the exact revision it read before the checkpoint", async () => {
    const factId = "22222222-2222-4222-8222-222222222222";
    const expectedUpdatedAt = "2026-08-27T19:55:00.000Z";
    const callId = "correct-saved-recipe";
    const arguments_ = {
      operation: "correct" as const,
      factId,
      statement: "The weeknight noodle recipe uses tamari instead of soy sauce.",
      visibility: "household" as const,
      memory: {
        memoryKind: "artifact" as const,
        artifactKind: "recipe" as const,
        title: "Weeknight noodles",
        details: "Toss noodles with sesame oil, tamari, and rice vinegar.",
        tags: ["dinner", "noodles", "tamari"],
      },
      sourceIds: ["source-adult-1"],
      fileAssetIds: null,
      expectedUpdatedAt,
    };
    const state: FamilyWorkStateV1 = {
      kind: "family_work_v1",
      version: 1,
      generation: 0,
      phase: "tool_pending",
      claim: null,
      activePhoneCall: null,
      activeTextMessage: null,
      pendingParticipantRequest: null,
      browserSession: null,
      continuationItems: [functionCall(callId, "vault_work", arguments_)],
      pendingCall: {
        callId,
        name: "vault_work",
        argumentsJson: JSON.stringify(arguments_),
        attempt: 0,
        receipt: null,
      },
      steering: [],
      progressRevision: 0,
      terminal: null,
    };
    const input = {
      workId: "family-work-vault-correction",
      scheduledOccurrence: null,
      objective: "Update the saved noodle recipe to use tamari.",
      visibility: "household" as const,
      ownerAdultId: null,
      origin: familyWorkOrigin("Update the saved noodle recipe to use tamari."),
      household: {
        householdId: "household-1",
        familyLabel: "Test family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [
          { adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" },
          { adultId: "adult-2", firstName: "Jackson", displayName: "Jackson Williams" },
        ],
        children: [],
      },
      state,
      currentTime: NOW,
    };
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse() {
          throw new Error("A resumed pending effect should settle before another model turn");
        },
      },
    } as never);
    const requests: unknown[] = [];

    const settled = await reasoner.continueFamilyWork(input, {
      async runVaultWork(request) {
        requests.push(request);
        return {
          operation: "correct" as const,
          status: "committed" as const,
          factId,
          statement: arguments_.statement,
        };
      },
    });

    expect(requests).toEqual([expect.objectContaining({ factId, expectedUpdatedAt })]);
    expect(settled).toMatchObject({ kind: "continue", state: { phase: "ready", pendingCall: null } });
  });

  test("household work privately asks one exact other adult and waits without polling the group", async () => {
    const modelRequests: Record<string, unknown>[] = [];
    const question = "Can Violet stay for the full field-trip day on Friday?";
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          modelRequests.push(request);
          return {
            status: "completed",
            output_parsed: null,
            output: [
              functionCall("ask-hari", "participant_request", {
                targetAdultName: "Hari Anbarasu",
                question,
              }),
            ],
          };
        },
      },
    } as never);
    const input = {
      workId: "family-work-field-trip",
      scheduledOccurrence: null,
      objective: "Confirm whether Violet can stay for the full field-trip day, then finish the plan.",
      visibility: "household" as const,
      ownerAdultId: null,
      initiatingAdultId: "adult-2",
      origin: familyWorkOrigin("Can you finish the field-trip plan?"),
      household: {
        householdId: "household-1",
        familyLabel: "Test family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [
          { adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" },
          { adultId: "adult-2", firstName: "Jackson", displayName: "Jackson Williams" },
        ],
        children: [],
      },
      googleConnections: [],
      state: {
        kind: "family_work_v1" as const,
        version: 1 as const,
        generation: 0,
        phase: "ready" as const,
        claim: null,
        activePhoneCall: null,
        activeTextMessage: null,
        pendingParticipantRequest: null,
        browserSession: null,
        continuationItems: [],
        pendingCall: null,
        steering: [],
        progressRevision: 0,
        terminal: null,
      },
      currentTime: NOW,
    };
    const queuedRequests: unknown[] = [];
    const reads = {
      async runParticipantRequest(request: unknown) {
        queuedRequests.push(request);
        return {
          status: "queued" as const,
          requestId: "participant-request-1",
          targetAdultId: "adult-1",
          targetAdultName: "Hari Anbarasu",
          channelId: "private-channel-1",
          questionSourceId: "private-question-1",
          question,
          askedAt: NOW,
        };
      },
    };

    const planned = await reasoner.continueFamilyWork(input, reads);
    expect(planned).toMatchObject({
      kind: "continue",
      state: {
        phase: "tool_pending",
        pendingParticipantRequest: null,
        pendingCall: { name: "participant_request" },
      },
    });
    if (planned.kind !== "continue") throw new Error("Participant request was not planned");

    const waiting = await reasoner.continueFamilyWork({ ...input, state: planned.state }, reads);

    expect(waiting).toEqual(
      expect.objectContaining({
        kind: "participant_waiting",
        state: expect.objectContaining({
          phase: "waiting",
          pendingCall: null,
          pendingParticipantRequest: expect.objectContaining({
            requestId: "participant-request-1",
            targetAdultId: "adult-1",
            targetAdultName: "Hari Anbarasu",
            channelId: "private-channel-1",
            questionSourceId: "private-question-1",
            question,
          }),
          waitingDocket: {
            owner: "Hari Anbarasu",
            nextAction: "Answer Florence's private question.",
            waitingOn: question,
            needsAnswer: true,
          },
        }),
      }),
    );
    expect(waiting).not.toHaveProperty("question");
    expect(queuedRequests).toEqual([{ targetAdultName: "Hari Anbarasu", question }]);
    expect(modelRequests).toHaveLength(1);
    const [modelRequest] = modelRequests;
    if (!modelRequest) throw new Error("Participant request was not presented to the model");
    const participantTool = (modelRequest.tools as { name?: string; parameters?: unknown }[]).find(
      (tool) => tool.name === "participant_request",
    );
    expect(participantTool).toMatchObject({
      parameters: {
        properties: {
          targetAdultName: { enum: ["Hari Anbarasu"] },
        },
      },
    });
  });

  test("participant replies are matched semantically and acknowledged for the actual moment", async () => {
    const requests: Record<string, unknown>[] = [];
    const outputs = [
      {
        belongsToRequest: false,
        acknowledgement: null,
      },
      {
        belongsToRequest: true,
        acknowledgement: {
          kind: "text" as const,
          text: "Got it—thanks for letting me know. I’ll update the plan.",
        },
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          requests.push(request);
          return {
            status: "completed",
            output_parsed: outputs.shift() ?? null,
            output: [],
          };
        },
      },
    } as never);
    const pendingRequest = {
      targetAdultName: "Alex Anbarasu",
      question: "Can Maya stay for the whole field-trip day on Friday?",
      askedAt: NOW,
      taskObjective: "Finish the family field-trip plan.",
    };

    const unrelated = await reasoner.interpretParticipantReply({
      pendingRequest,
      currentMessage: {
        text: "Can you add milk to the grocery list?",
        occurredAt: "2026-08-27T20:05:00.000Z",
        explicitlyRepliesToQuestion: false,
      },
    });
    expect(unrelated).toEqual({ belongsToRequest: false, acknowledgement: null });

    const refusal = await reasoner.interpretParticipantReply({
      pendingRequest,
      currentMessage: {
        text: "No, she needs to come home right after lunch.",
        occurredAt: "2026-08-27T20:06:00.000Z",
        explicitlyRepliesToQuestion: true,
      },
    });
    expect(refusal).toEqual({
      belongsToRequest: true,
      acknowledgement: {
        kind: "text",
        text: "Got it—thanks for letting me know. I’ll update the plan.",
      },
    });
    expect(requests).toHaveLength(2);
    expect(JSON.stringify(requests[0])).toContain("Can Maya stay for the whole field-trip day");
  });

  test("durable work compacts complete history and preserves its recent tail before continuing", async () => {
    const oldResultUrl = "https://example.com/older-comparison";
    const recentResultUrl = "https://example.com/recent-comparison";
    const compactionRequests: Record<string, unknown>[] = [];
    const continuationRequests: Record<string, unknown>[] = [];
    const compactionSummary = `## Goal
Compare the useful family options.

## Constraints & Preferences
- Keep the recent option in consideration.

## Progress
### Done
- [x] Reviewed the older comparison at ${oldResultUrl}.

### In Progress
- [ ] Compare it with the recent option.

### Blocked
- (none)

## Key Decisions
- **Retain exact sources**: The older comparison URL is still needed.

## Next Steps
1. Finish the comparison using the retained recent result.

## Critical Context
- Older source: ${oldResultUrl}`;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        create(request: Record<string, unknown>) {
          compactionRequests.push(request);
          return {
            status: "completed",
            output_text: compactionSummary,
            output: [],
          };
        },
        parse(request: Record<string, unknown>) {
          continuationRequests.push(request);
          return {
            status: "completed",
            output_parsed: {
              outcome: "succeeded",
              text: `The recent option is the better fit. Details: ${recentResultUrl}`,
              resumeAt: null,
              progressText: null,
            },
            output: [],
          };
        },
      },
    } as never);
    const state: FamilyWorkStateV1 = {
      kind: "family_work_v1",
      version: 1,
      generation: 3,
      phase: "ready",
      claim: null,
      activePhoneCall: null,
      activeTextMessage: null,
      pendingParticipantRequest: null,
      browserSession: null,
      continuationItems: [
        familyWorkResultMessage("older-comparison", {
          observation: "Older comparison result",
          url: oldResultUrl,
          details: `older:${"x".repeat(180 * 1024)}`,
        }),
        familyWorkResultMessage("recent-comparison", {
          observation: "Recent comparison result",
          url: recentResultUrl,
          details: `recent:${"y".repeat(80 * 1024)}`,
        }),
      ],
      pendingCall: null,
      steering: [],
      progressRevision: 4,
      terminal: null,
    };

    const result = await reasoner.continueFamilyWork(
      {
        workId: "family-work-large",
        scheduledOccurrence: null,
        objective: "Compare the useful options.",
        visibility: "household",
        ownerAdultId: null,
        origin: familyWorkOrigin("Compare the useful options."),
        household: {
          householdId: "household-1",
          familyLabel: "Test family",
          timeZone: "America/Los_Angeles",
          postalCode: "90045",
          adults: [{ adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" }],
          children: [],
        },
        state,
        currentTime: NOW,
      },
      {},
    );

    expect(compactionRequests).toHaveLength(1);
    expect(compactionRequests[0]).toMatchObject({
      tools: [],
      instructions: expect.stringContaining("ONLY output the structured summary"),
    });
    const compactionInput = JSON.stringify(compactionRequests[0]?.input);
    expect(compactionInput).toContain(oldResultUrl);
    expect(compactionInput).not.toContain(recentResultUrl);

    expect(continuationRequests).toHaveLength(1);
    const compactedInput = JSON.stringify(continuationRequests[0]?.input);
    expect(compactedInput).toContain("The task history before this point was compacted");
    expect(compactedInput).toContain(oldResultUrl);
    expect(compactedInput).toContain(recentResultUrl);
    expect(compactedInput).not.toContain("x".repeat(10_000));
    expect(Buffer.byteLength(compactedInput, "utf8")).toBeLessThan(120 * 1024);
    expect(result).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining(recentResultUrl),
      state: {
        phase: "terminal",
        continuationItems: [],
        progressRevision: 5,
      },
    });
  });

  test("provider context overflow compacts below the storage threshold once and retries with atomic tool history", async () => {
    expect(
      isContextOverflowError(
        new Error("Throttling error: Too many tokens, please wait before trying again."),
      ),
    ).toBe(false);
    const compactionRequests: Record<string, unknown>[] = [];
    const continuationRequests: Record<string, unknown>[] = [];
    let modelAttempt = 0;
    const compactionSummary = `## Goal
Compare the family options.

## Constraints & Preferences
- Preserve the useful evidence.

## Progress
### Done
- [x] Reviewed the older option.

### In Progress
- [ ] Finish the comparison.

### Blocked
- (none)

## Key Decisions
- **Older evidence retained**: The older option was reviewed.

## Next Steps
1. Use the recent result to finish.

## Critical Context
- The older tool result was complete.`;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        create(request: Record<string, unknown>) {
          compactionRequests.push(request);
          return { status: "completed", output_text: compactionSummary, output: [] };
        },
        parse(request: Record<string, unknown>) {
          continuationRequests.push(request);
          modelAttempt += 1;
          throw new Error(
            modelAttempt === 1
              ? "Your input exceeds the context window of this model"
              : "The model provider failed again after compaction",
          );
        },
      },
    } as never);
    const recentCall = functionCall("recent-read", "read_public_page", {
      url: "https://example.com/recent",
      offset: 0,
      contentFingerprint: null,
    });
    const recentOutput = {
      type: "function_call_output" as const,
      call_id: "recent-read",
      output: JSON.stringify({ observation: "recent evidence", details: "y".repeat(4_000) }),
    };
    const state: FamilyWorkStateV1 = {
      kind: "family_work_v1",
      version: 1,
      generation: 3,
      phase: "ready",
      claim: { claimId: "claim-overflow", leaseUntil: "2026-08-27T20:05:00.000Z" },
      activePhoneCall: null,
      activeTextMessage: null,
      pendingParticipantRequest: null,
      browserSession: null,
      continuationItems: [
        functionCall("older-read", "read_public_page", {
          url: "https://example.com/older",
          offset: 0,
          contentFingerprint: null,
        }),
        {
          type: "function_call_output",
          call_id: "older-read",
          output: JSON.stringify({ observation: "older evidence", details: "x".repeat(60_000) }),
        },
        recentCall,
        recentOutput,
      ],
      pendingCall: null,
      steering: [],
      progressRevision: 0,
      terminal: null,
    };

    const failure = await reasoner
      .continueFamilyWork(
        {
          workId: "family-work-overflow",
          scheduledOccurrence: null,
          objective: "Compare the family options.",
          visibility: "household",
          ownerAdultId: null,
          origin: familyWorkOrigin("Compare the family options."),
          household: {
            householdId: "household-1",
            familyLabel: "Test family",
            timeZone: "America/Los_Angeles",
            postalCode: "90045",
            adults: [{ adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" }],
            children: [],
          },
          state,
          currentTime: NOW,
        },
        {},
      )
      .then(
        () => null,
        (error: unknown) => error,
      );

    expect(compactionRequests).toHaveLength(1);
    expect(continuationRequests).toHaveLength(2);
    const firstInput = JSON.stringify(continuationRequests[0]?.input);
    const retryInput = JSON.stringify(continuationRequests[1]?.input);
    expect(Buffer.byteLength(firstInput, "utf8")).toBeLessThan(240 * 1024);
    expect(retryInput).toContain("The task history before this point was compacted");
    expect(retryInput).toContain(recentCall.call_id);
    const retryItems = continuationRequests[1]?.input as
      | { type?: string; call_id?: string; output?: unknown }[]
      | undefined;
    expect(
      retryItems?.find((item) => item.type === "function_call_output" && item.call_id === recentCall.call_id),
    ).toEqual(recentOutput);
    expect(retryInput).not.toContain("x".repeat(10_000));
    expect(failure).toBeInstanceOf(FlorenceReasonerError);
    if (!(failure instanceof FlorenceReasonerError)) throw new Error("Expected a reasoner failure");
    expect(failure.familyWorkCheckpoint).toMatchObject({
      generation: 3,
      claim: { claimId: "claim-overflow" },
      continuationItems: [
        expect.objectContaining({ type: "message", role: "user" }),
        expect.objectContaining({ type: "function_call", call_id: recentCall.call_id }),
        expect.objectContaining({ type: "function_call_output", call_id: recentCall.call_id }),
      ],
    });
  });

  test("foreground Workspace reads hand real send requests to durable work", async () => {
    const requests: Record<string, unknown>[] = [];
    const operations: unknown[] = [];
    let modelTurn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream(request: Record<string, unknown>) {
          requests.push(request);
          modelTurn += 1;
          return fakeStream(
            modelTurn === 1
              ? {
                  status: "completed",
                  output_parsed: null,
                  output: [
                    functionCall("find-school-file", "drive_work", {
                      operation: "drive_search",
                      query: "school enrollment form",
                      limit: 10,
                      fileId: null,
                      name: null,
                      parentId: null,
                      role: null,
                      type: null,
                      email: null,
                      domain: null,
                      notify: false,
                    }),
                    functionCall("send-before-durable", "gmail_work", {
                      operation: "gmail_send",
                      query: null,
                      limit: null,
                      messageId: null,
                      to: ["school@example.com"],
                      cc: [],
                      bcc: [],
                      subject: "Enrollment status update",
                      body: "Violet's enrollment paperwork is complete. Please confirm her status is current.",
                      bodyFormat: "plain",
                      threadId: null,
                      addLabelIds: [],
                      removeLabelIds: [],
                    }),
                  ],
                }
              : {
                  status: "completed",
                  output_parsed: {
                    ...ordinaryDecision({
                      bubbleText: "I found the enrollment form. I’m sending the update now.",
                    }),
                    policy: { retain: true, schedule: false, stopMessaging: false },
                    familyWork: {
                      operation: "create",
                      workId: null,
                      objective:
                        "Email the school that Violet's enrollment paperwork is complete and ask them to confirm her status is current.",
                      schedule: null,
                      instruction: null,
                      candidateIds: [],
                    },
                  },
                  output: [],
                },
          );
        },
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text =
      "Find Violet's enrollment form, then email the school that her paperwork is complete and ask them to confirm.";
    input.currentMessage.authoredText = input.currentMessage.text;
    const result = await reasoner.decide(input, {
      ...inertReads(),
      async runGoogleWorkspace(operation) {
        operations.push(operation);
        return {
          operation: operation.operation,
          result: {
            files: [
              {
                fileId: "drive-file-1",
                name: "Enrollment form",
                mimeType: "application/pdf",
              },
            ],
          },
        };
      },
    });

    expect(operations).toEqual([{ operation: "drive_search", query: "school enrollment form", limit: 10 }]);
    expect(result.conversation.bubbles[0]?.text).toContain("sending the update");
    expect(result.familyWork).toMatchObject({
      operation: "create",
      objective: expect.stringContaining("Email the school"),
    });
    expect(result.policy.schedule).toBe(false);
    expect(String(requests[0]?.instructions)).toContain(
      "do not substitute advice, a draft, or a promise for requested execution",
    );
    expect(functionOutputEnvelopes(requests[1])).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          callId: "find-school-file",
          outcome: "succeeded",
          output: expect.objectContaining({ operation: "drive_search" }),
        }),
        expect.objectContaining({ callId: "send-before-durable", outcome: "protocol_rejected" }),
      ]),
    );
  });

  test("durable Workspace retries keep one semantic identity across new model call IDs", async () => {
    const emailArguments = {
      operation: "gmail_send",
      query: null,
      limit: null,
      messageId: null,
      to: ["school@example.com", "registrar@example.com"],
      cc: ["office@example.com", "records@example.com"],
      bcc: ["archive@example.com"],
      subject: "Enrollment status update",
      body: "Violet's enrollment paperwork is complete. Please confirm her status is current.",
      bodyFormat: "plain",
      threadId: null,
      addLabelIds: [],
      removeLabelIds: [],
    };
    const retryEmailArguments = {
      ...emailArguments,
      to: ["REGISTRAR@EXAMPLE.COM", "School@Example.com", "school@example.com"],
      cc: ["records@example.com", "OFFICE@EXAMPLE.COM"],
      bcc: ["ARCHIVE@example.com"],
    };
    const modelResponses = [
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("send-school-email-first", "gmail_work", emailArguments)],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("send-school-email-retry", "gmail_work", retryEmailArguments)],
      },
      {
        status: "completed",
        output_parsed: {
          outcome: "succeeded",
          text: "I sent the enrollment email to the school.",
        },
        output: [],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse() {
          const response = modelResponses.shift();
          if (!response) throw new Error("Unexpected durable Workspace model turn");
          return response;
        },
      },
    } as never);
    const state: FamilyWorkStateV1 = {
      kind: "family_work_v1",
      version: 1,
      generation: 0,
      phase: "ready",
      claim: null,
      activePhoneCall: null,
      activeTextMessage: null,
      pendingParticipantRequest: null,
      browserSession: null,
      continuationItems: [],
      pendingCall: null,
      steering: [],
      progressRevision: 0,
      terminal: null,
    };
    const input = {
      workId: "family-work-workspace-email",
      scheduledOccurrence: null,
      objective: "Email the school an enrollment status update and ask them to confirm it is current.",
      visibility: "private" as const,
      ownerAdultId: "adult-1",
      origin: familyWorkOrigin(
        "Email the school an enrollment status update and ask them to confirm it is current.",
      ),
      household: {
        householdId: "household-1",
        familyLabel: "Test family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [{ adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" }],
        children: [],
      },
      state,
      currentTime: NOW,
    };
    const operations: Array<
      Parameters<NonNullable<Parameters<typeof reasoner.continueFamilyWork>[1]["runGoogleWorkspace"]>>[0]
    > = [];
    const runGoogleWorkspace = async (
      operation: Parameters<
        NonNullable<Parameters<typeof reasoner.continueFamilyWork>[1]["runGoogleWorkspace"]>
      >[0],
    ) => {
      operations.push(operation);
      if (operations.length === 1) {
        throw new GoogleWorkspaceError(
          "Gmail timed out after accepting the request",
          "provider_unavailable",
          {
            service: "Gmail",
          },
        );
      }
      return {
        operation: operation.operation,
        result: { status: "sent", messageId: "gmail-message-1", threadId: "gmail-thread-1" },
      };
    };

    const planned = await reasoner.continueFamilyWork(input, { runGoogleWorkspace });
    if (planned.kind !== "continue") throw new Error("Workspace email was not planned");
    expect(planned.state).toMatchObject({
      phase: "tool_pending",
      pendingCall: { callId: "send-school-email-first", name: "gmail_work" },
    });
    const firstAttempt = await reasoner.continueFamilyWork(
      { ...input, state: planned.state },
      { runGoogleWorkspace },
    );
    if (firstAttempt.kind !== "continue") throw new Error("Workspace email failure was not settled");
    expect(firstAttempt.state.continuationItems).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: "function_call_output",
          output: expect.stringContaining('"retryable":true'),
        }),
      ]),
    );

    const retryPlanned = await reasoner.continueFamilyWork(
      { ...input, state: firstAttempt.state },
      { runGoogleWorkspace },
    );
    if (retryPlanned.kind !== "continue") throw new Error("Workspace email retry was not planned");
    expect(retryPlanned.state).toMatchObject({
      phase: "tool_pending",
      pendingCall: { callId: "send-school-email-retry", name: "gmail_work" },
    });
    const retryExecuted = await reasoner.continueFamilyWork(
      { ...input, state: retryPlanned.state },
      { runGoogleWorkspace },
    );
    if (retryExecuted.kind !== "continue") throw new Error("Workspace email retry was not executed");
    const terminal = await reasoner.continueFamilyWork(
      { ...input, state: retryExecuted.state },
      { runGoogleWorkspace },
    );

    expect(operations).toHaveLength(2);
    const firstOperation = operations[0];
    if (!firstOperation || !("idempotencyKey" in firstOperation)) {
      throw new Error("Workspace email did not receive an idempotency key");
    }
    expect(operations[0]).toMatchObject({
      operation: "gmail_send",
      to: ["school@example.com", "registrar@example.com"],
      subject: "Enrollment status update",
      idempotencyKey: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
    expect(operations[1]).toMatchObject({
      operation: "gmail_send",
      to: ["REGISTRAR@EXAMPLE.COM", "School@Example.com", "school@example.com"],
      idempotencyKey: firstOperation.idempotencyKey,
    });
    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: "I sent the enrollment email to the school.",
    });
  });

  test("durable household work opens and preserves a Gmail attachment across a draft checkpoint", async () => {
    const gmail = conversationalGmailSource();
    const modelResponses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("read-school-message", "gmail_work", {
            operation: "gmail_get",
            query: null,
            limit: null,
            messageId: "gmail-school-message",
            to: [],
            cc: [],
            bcc: [],
            subject: null,
            body: null,
            bodyFormat: null,
            threadId: null,
            addLabelIds: [],
            removeLabelIds: [],
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("open-school-form", "read_gmail_attachment", {
            sourceId: gmail.sourceId,
            attachmentRef: gmail.attachments[0]?.attachmentRef,
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("draft-school-forward", "gmail_draft_work", {
            operation: "create_forward",
            messageId: "gmail-school-message",
            draftId: null,
            messageHeaderId: null,
            to: ["jackson@example.com"],
            cc: [],
            bcc: [],
            subject: null,
            body: "Here is the school form Florence found.",
            bodyFormat: "plain",
            includeSourceAttachments: false,
            attachments: [
              {
                source: "gmail",
                sourceId: gmail.sourceId,
                attachmentRef: gmail.attachments[0]?.attachmentRef ?? "missing-attachment",
                messageId: null,
                attachmentId: null,
                fileId: null,
              },
              {
                source: "gmail",
                sourceId: null,
                attachmentRef: null,
                messageId: "gmail-school-message",
                attachmentId: "gmail-docx-1",
                fileId: null,
              },
            ],
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("send-school-forward", "gmail_draft_work", {
            operation: "send",
            messageId: null,
            draftId: "gmail-draft-1",
            messageHeaderId: "<florence-draft-1@messages.florence.local>",
            to: [],
            cc: [],
            bcc: [],
            subject: null,
            body: null,
            bodyFormat: null,
            includeSourceAttachments: false,
            attachments: [],
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: {
          outcome: "succeeded",
          text: "I forwarded Jackson the school form with the original PDF attached.",
        },
        output: [],
      },
    ];
    const modelRequests: Record<string, unknown>[] = [];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          modelRequests.push(request);
          const response = modelResponses.shift();
          if (!response) throw new Error("Unexpected Gmail draft model turn");
          return response;
        },
      },
    } as never);
    const input = {
      workId: "family-work-forward-school-form",
      scheduledOccurrence: null,
      objective: "Forward the school email and its form attachment to Jackson.",
      visibility: "household" as const,
      ownerAdultId: null,
      initiatingAdultId: "adult-hari",
      origin: familyWorkOrigin("Forward the school email and its form attachment to Jackson.", "adult-hari"),
      household: {
        householdId: "household-1",
        familyLabel: "Test family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [
          { adultId: "adult-hari", firstName: "Hari", displayName: "Hari Anbarasu" },
          { adultId: "adult-jackson", firstName: "Jackson", displayName: "Jackson Williams" },
        ],
        children: [],
      },
      googleConnections: [
        {
          emailLabel: "Family Calendar",
          calendarAvailable: true,
          kind: "family" as const,
          writesEnabled: true,
        },
      ],
      state: {
        kind: "family_work_v1" as const,
        version: 1 as const,
        generation: 0,
        phase: "ready" as const,
        claim: null,
        activePhoneCall: null,
        activeTextMessage: null,
        pendingParticipantRequest: null,
        browserSession: null,
        continuationItems: [],
        pendingCall: null,
        steering: [],
        progressRevision: 0,
        terminal: null,
      },
      currentTime: NOW,
    };
    const operations: GoogleWorkspaceOperation[] = [];
    const runGoogleWorkspace = async (
      operation: GoogleWorkspaceOperation,
    ): Promise<GoogleWorkspaceResult> => {
      operations.push(operation);
      if (operation.operation === "gmail_get") {
        return {
          operation: operation.operation,
          result: {
            message: {
              messageId: "gmail-school-message",
              threadId: "gmail-thread-1",
              historyId: "gmail-history-1",
              subject: "School form",
              body: "Please return the attached form.",
              attachments: [
                {
                  attachmentId: "gmail-attachment-1",
                  partId: "1",
                  filename: "form.pdf",
                  mimeType: "application/pdf",
                  sizeBytes: 5,
                },
                {
                  attachmentId: "gmail-docx-1",
                  partId: "2",
                  filename: "instructions.docx",
                  mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                  sizeBytes: 12_000,
                },
              ],
            },
          },
        };
      }
      if (operation.operation === "gmail_draft_create") {
        return {
          operation: operation.operation,
          result: {
            status: "created",
            draftId: "gmail-draft-1",
            messageHeaderId: "<florence-draft-1@messages.florence.local>",
            messageId: "gmail-draft-message-1",
            threadId: null,
          },
        };
      }
      if (operation.operation === "gmail_draft_send") {
        return {
          operation: operation.operation,
          result: {
            status: "sent",
            draftId: operation.draftId,
            messageHeaderId: operation.messageHeaderId,
            messageId: "gmail-sent-message-1",
            threadId: null,
          },
        };
      }
      throw new Error(`Unexpected Google operation ${operation.operation}`);
    };
    const workspaceReads: Array<{ messageId: string; threadId: string; historyId: string }> = [];
    const openedAttachmentRefs: string[] = [];
    const reads = {
      runGoogleWorkspace,
      async readWorkspaceGmailSource(identity: { messageId: string; threadId: string; historyId: string }) {
        workspaceReads.push(identity);
        return gmail;
      },
      async readGmailAttachment(input: { sourceId: string; attachment: (typeof gmail.attachments)[number] }) {
        expect(input).toEqual({ sourceId: gmail.sourceId, attachment: gmail.attachments[0] });
        openedAttachmentRefs.push(input.attachment.attachmentRef);
        return {
          sourceId: input.sourceId,
          attachmentRef: input.attachment.attachmentRef,
          filename: input.attachment.filename,
          mimeType: input.attachment.mimeType,
          bytes: new Uint8Array(Buffer.from("%PDF-")),
        };
      },
      async resolveWorkspaceGmailAttachment(input: { sourceId: string; attachmentRef: string }) {
        expect(input).toEqual({
          sourceId: gmail.sourceId,
          attachmentRef: gmail.attachments[0]?.attachmentRef,
        });
        return { messageId: "gmail-school-message", attachmentId: "gmail-attachment-1" };
      },
    };

    const draftPlanned = await reasoner.continueFamilyWork(input, reads);
    expect(draftPlanned).toMatchObject({
      kind: "continue",
      state: {
        phase: "tool_pending",
        pendingCall: { callId: "draft-school-forward", name: "gmail_draft_work" },
      },
    });
    if (draftPlanned.kind !== "continue") throw new Error("Gmail forward draft was not planned");
    expect(operations.map((operation) => operation.operation)).toEqual(["gmail_get"]);
    expect(openedAttachmentRefs).toEqual([gmail.attachments[0]?.attachmentRef]);
    expect(JSON.stringify(modelRequests)).toContain('"type":"input_file"');
    expect(JSON.stringify(modelRequests)).toContain('"filename":"form.pdf"');
    expect(JSON.stringify(draftPlanned.state.continuationItems)).toContain("draftAttachmentAccess");
    expect(JSON.stringify(draftPlanned.state.continuationItems)).toContain("gmail-docx-1");

    const draftCreated = await reasoner.continueFamilyWork({ ...input, state: draftPlanned.state }, reads);
    if (draftCreated.kind !== "continue") throw new Error("Gmail forward draft was not created");
    expect(operations.map((operation) => operation.operation)).toEqual(["gmail_get", "gmail_draft_create"]);

    const sendPlanned = await reasoner.continueFamilyWork({ ...input, state: draftCreated.state }, reads);
    expect(sendPlanned).toMatchObject({
      kind: "continue",
      state: {
        phase: "tool_pending",
        pendingCall: { callId: "send-school-forward", name: "gmail_draft_work" },
      },
    });
    if (sendPlanned.kind !== "continue") throw new Error("Exact Gmail draft send was not planned");
    expect(operations).toHaveLength(2);

    const sent = await reasoner.continueFamilyWork({ ...input, state: sendPlanned.state }, reads);
    if (sent.kind !== "continue") throw new Error("Exact Gmail draft was not sent");
    const terminal = await reasoner.continueFamilyWork({ ...input, state: sent.state }, reads);

    expect(operations[1]).toMatchObject({
      operation: "gmail_draft_create",
      mode: "forward",
      messageId: "gmail-school-message",
      to: ["jackson@example.com"],
      includeSourceAttachments: false,
      attachments: [
        {
          source: "gmail",
          messageId: "gmail-school-message",
          attachmentId: "gmail-attachment-1",
        },
        {
          source: "gmail",
          messageId: "gmail-school-message",
          attachmentId: "gmail-docx-1",
        },
      ],
      idempotencyKey: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
    expect(workspaceReads).toEqual(
      expect.arrayContaining([
        {
          messageId: "gmail-school-message",
          threadId: "gmail-thread-1",
          historyId: "gmail-history-1",
        },
      ]),
    );
    expect(workspaceReads.length).toBeGreaterThan(1);
    expect(operations[2]).toEqual({
      operation: "gmail_draft_send",
      draftId: "gmail-draft-1",
      messageHeaderId: "<florence-draft-1@messages.florence.local>",
    });
    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("original PDF attached"),
    });
  });

  test("durable work composes Calendar reads and replans a transient read inline", async () => {
    const calendarRef = "calendar-school";
    const calendarWindowArguments = {
      timeMin: "2026-08-28T00:00:00.000Z",
      timeMax: "2026-08-29T00:00:00.000Z",
      pageSize: 20,
      cursor: null,
      scope: "selected",
      calendarRefs: [calendarRef],
    };
    const modelRequests: Record<string, unknown>[] = [];
    const modelResponses = [
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("list-work-calendars", "list_calendars", {})],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("read-work-calendar", "read_calendar_window", calendarWindowArguments)],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("read-work-calendar-retry", "read_calendar_window", calendarWindowArguments)],
      },
      {
        status: "completed",
        output_parsed: {
          outcome: "succeeded",
          text: "Back-to-school night is tomorrow from 4 to 6 PM.",
        },
        output: [],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          modelRequests.push(request);
          const response = modelResponses.shift();
          if (!response) throw new Error("Unexpected durable Calendar model turn");
          return response;
        },
      },
    } as never);
    const state: FamilyWorkStateV1 = {
      kind: "family_work_v1",
      version: 1,
      generation: 0,
      phase: "ready",
      claim: null,
      activePhoneCall: null,
      activeTextMessage: null,
      pendingParticipantRequest: null,
      browserSession: null,
      continuationItems: [],
      pendingCall: null,
      steering: [],
      progressRevision: 0,
      terminal: null,
    };
    const input = {
      workId: "family-work-calendar",
      scheduledOccurrence: null,
      objective: "Check the School Calendar for tomorrow's schedule.",
      visibility: "private" as const,
      ownerAdultId: "adult-1",
      origin: familyWorkOrigin("Check the School Calendar for tomorrow's schedule."),
      household: {
        householdId: "household-1",
        familyLabel: "Test family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [{ adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" }],
        children: [],
      },
      googleConnections: [
        {
          emailLabel: "Personal Google",
          calendarAvailable: true,
          kind: "personal" as const,
          writesEnabled: false,
        },
      ],
      state,
      currentTime: NOW,
    };
    const calendarInputs: unknown[] = [];
    const reads = {
      async listCalendars() {
        return {
          status: "complete" as const,
          calendars: [
            {
              calendarRef,
              label: "School",
              timeZone: "America/Los_Angeles",
              primary: false,
              accessRole: "reader" as const,
              eventCoverage: "readable" as const,
            },
          ],
          totalCalendarCount: 1,
        };
      },
      async readCalendarWindow(calendarInput: {
        timeMin: string;
        timeMax: string;
        pageSize: number;
        cursor: string | null;
        scope: "all" | "primary" | "selected";
        calendarRefs: readonly string[];
      }) {
        calendarInputs.push(calendarInput);
        if (calendarInputs.length === 1) {
          throw new GoogleCalendarTransientError("Calendar timed out");
        }
        return {
          status: "complete" as const,
          calendars: [
            {
              calendarRef,
              label: "School",
              timeZone: "America/Los_Angeles",
              primary: false,
              accessRole: "reader" as const,
              status: "complete" as const,
              eventCount: 1,
            },
          ],
          totalCalendarCount: 1,
          events: [
            {
              eventRef: "event-school-night",
              providerUpdatedAt: "2026-08-27T19:00:00.000Z",
              calendarRef,
              calendarLabel: "School",
              title: "Back-to-school night",
              location: "Wish Charter",
              status: "confirmed" as const,
              busy: true,
              intervalKind: "timed" as const,
              startsAt: "2026-08-28T23:00:00.000Z",
              endsAt: "2026-08-29T01:00:00.000Z",
              timeZone: "America/Los_Angeles",
            },
          ],
          totalEventCount: 1,
          nextCursor: null,
        };
      },
    };

    const terminal = await reasoner.continueFamilyWork(input, reads);

    expect(calendarInputs).toEqual([
      expect.objectContaining({ scope: "selected", calendarRefs: [calendarRef] }),
      expect.objectContaining({ scope: "selected", calendarRefs: [calendarRef] }),
    ]);
    expect(JSON.stringify(modelRequests[1]?.input)).toContain(calendarRef);
    expect(JSON.stringify(modelRequests[2]?.input)).toContain('\\"retryable\\":true');
    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("Back-to-school night"),
    });
  });

  test("public place verification composes map candidates with direct web search", async () => {
    let modelTurn = 0;
    const modelRequests: Record<string, unknown>[] = [];
    let workStarts = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          modelRequests.push(request);
          modelTurn += 1;
          return fakeStream(
            modelTurn === 1
              ? {
                  status: "completed",
                  output_parsed: null,
                  output: [
                    functionCall("nearby-call", "maps_nearby", {
                      center: { lat: 40.758, lon: -73.9855 },
                      categories: ["restaurant"],
                      radiusM: 300,
                      limit: 3,
                    }),
                  ],
                }
              : {
                  status: "completed",
                  output_parsed: ordinaryDecision({ researchUrls: [PUBLIC_URL] }),
                  output: [
                    completedWebSearch(
                      PUBLIC_URL,
                      "Junior's Times Square current opening hours",
                      "place-hours-search",
                    ),
                  ],
                },
          );
        },
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text = "Which of these restaurants is open now?";
    input.currentMessage.authoredText = input.currentMessage.text;

    await reasoner.decide(
      input,
      {
        ...inertReads(),
        async runMaps() {
          return {
            operation: "nearby" as const,
            center: {
              query: null,
              displayName: "40.758, -73.9855",
              lat: 40.758,
              lon: -73.9855,
            },
            categories: ["restaurant" as const],
            radiusM: 300,
            count: 1,
            results: [
              {
                name: "Junior's",
                address: "1515 Broadway, New York",
                lat: 40.7582151,
                lon: -73.9866267,
                osmType: "node",
                osmId: "763650163",
                category: "restaurant" as const,
                distanceM: 97.9,
                mapsUrl: "https://www.google.com/maps/search/?api=1&query=40.7582151%2C-73.9866267",
                directionsUrl:
                  "https://www.google.com/maps/dir/?api=1&origin=40.758%2C-73.9855&destination=40.7582151%2C-73.9866267",
                website: "https://www.juniorscheesecake.com/blog/restaurants/times-square/",
                tags: {},
              },
            ],
            attribution: [
              {
                provider: "OpenStreetMap",
                label: "© OpenStreetMap contributors",
                url: "https://www.openstreetmap.org/copyright",
              },
            ],
          };
        },
      },
      undefined,
      {
        onWorkStarted() {
          workStarts += 1;
        },
      },
    );

    expect(modelRequests[0]?.tools).toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "web_search" })]),
    );
    expect(JSON.stringify(modelRequests[1]?.input)).toContain("Junior's");
    expect(JSON.stringify(modelRequests[1]?.input)).toContain("1515 Broadway, New York");
    expect(workStarts).toBe(1);
  });

  test("a foreground Workspace Gmail get can open its app-scoped verified attachment", async () => {
    const gmail = conversationalGmailSource();
    const requests: Record<string, unknown>[] = [];
    const responses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("gmail-get", "gmail_work", {
            operation: "gmail_get",
            query: null,
            limit: null,
            messageId: "gmail-message-1",
            to: [],
            cc: [],
            bcc: [],
            subject: null,
            body: null,
            bodyFormat: null,
            threadId: null,
            addLabelIds: [],
            removeLabelIds: [],
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("gmail-attachment", "read_gmail_attachment", {
            sourceId: gmail.sourceId,
            attachmentRef: gmail.attachments[0]?.attachmentRef ?? "missing-attachment",
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: ordinaryDecision(),
        output: [],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          const response = responses.shift();
          if (!response) throw new Error("Unexpected model request");
          return fakeStream(response);
        },
      },
    } as never);
    let attachmentInput: { sourceId: string; attachmentRef: string } | null = null;
    let workspaceIdentity: { messageId: string; threadId: string; historyId: string } | null = null;

    await reasoner.decide(foregroundInput(), {
      ...inertReads(),
      async runGoogleWorkspace(operation) {
        expect(operation).toEqual({ operation: "gmail_get", messageId: "gmail-message-1" });
        return {
          operation: "gmail_get",
          result: {
            message: {
              messageId: "gmail-message-1",
              threadId: "gmail-thread-1",
              historyId: "gmail-history-1",
              from: "School",
              subject: "School form",
              body: "Please review the attached form.",
              attachments: [
                {
                  attachmentId: "provider-attachment-1",
                  partId: "1",
                  filename: "form.pdf",
                  mimeType: "application/pdf",
                  sizeBytes: 5,
                },
              ],
            },
          },
        };
      },
      async readWorkspaceGmailSource(identity) {
        workspaceIdentity = identity;
        return gmail;
      },
      async readGmailAttachment(input) {
        attachmentInput = {
          sourceId: input.sourceId,
          attachmentRef: input.attachment.attachmentRef,
        };
        return {
          sourceId: input.sourceId,
          attachmentRef: input.attachment.attachmentRef,
          filename: input.attachment.filename,
          mimeType: input.attachment.mimeType,
          bytes: new Uint8Array(Buffer.from("%PDF-")),
        };
      },
    });

    expect(attachmentInput).toEqual({
      sourceId: gmail.sourceId,
      attachmentRef: gmail.attachments[0]?.attachmentRef,
    });
    expect((requests[0]?.tools as Array<{ name?: string }> | undefined)?.map((tool) => tool.name)).toContain(
      "gmail_work",
    );
    expect(
      (requests[0]?.tools as Array<{ name?: string }> | undefined)?.map((tool) => tool.name),
    ).not.toContain("search_gmail");
    expect(workspaceIdentity).toEqual({
      messageId: "gmail-message-1",
      threadId: "gmail-thread-1",
      historyId: "gmail-history-1",
    });
    const getEnvelope = functionOutputEnvelopes(requests[1]).find(
      (envelope) => envelope.callId === "gmail-get",
    );
    expect(getEnvelope?.output).toMatchObject({
      operation: "gmail_get",
      result: {
        message: {
          sourceId: gmail.sourceId,
          body: gmail.text,
          bodyFormat: "plain",
          textStatus: "complete",
          attachments: gmail.attachments,
        },
        attachmentAccess: {
          sourceId: gmail.sourceId,
          attachments: gmail.attachments,
          attachmentsStatus: "complete",
        },
      },
    });
    expect(JSON.stringify(getEnvelope?.output)).not.toContain("provider-attachment-1");
    expect(JSON.stringify(getEnvelope?.output)).not.toContain("attachmentId");
    expect(JSON.stringify(getEnvelope?.output)).not.toContain("partId");
    const attachmentOutput = functionOutputs(requests[2]).find(
      (item) => item.call_id === "gmail-attachment",
    )?.output;
    expect(Array.isArray(attachmentOutput)).toBe(true);
    expect(JSON.stringify(attachmentOutput)).toContain("input_file");
    expect(JSON.stringify(requests)).not.toContain("connectionId");
  });

  test("durable private work composes visible memory and Gmail attachments inline", async () => {
    const gmail = conversationalGmailSource();
    const memoryFactId = "11111111-1111-4111-8111-111111111111";
    const memoryUri = `vault://fact/${memoryFactId}`;
    const memory = {
      sourceId: "memory-pickup-source",
      recordId: memoryFactId,
      kind: "memory" as const,
      visibility: "adult_private" as const,
      label: "School pickup",
      occurredAt: NOW,
      text: "School pickup is normally at 2:45 PM.",
    };
    const requests: Record<string, unknown>[] = [];
    const responses = [
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("memory-search", "search_vault", { query: "pickup", cursor: null })],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("memory-read", "read_vault", { uri: memoryUri, level: "overview" })],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("durable-gmail-search", "search_gmail", { query: "school form", limit: 3 })],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("durable-gmail-attachment", "read_gmail_attachment", {
            sourceId: gmail.sourceId,
            attachmentRef: gmail.attachments[0]?.attachmentRef ?? "missing-attachment",
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: {
          outcome: "succeeded",
          text: "Pickup is at 2:45 PM, and the attached school form confirms the updated instructions.",
        },
        output: [],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          requests.push(request);
          const response = responses.shift();
          if (!response) throw new Error("Unexpected durable private-read model turn");
          return response;
        },
      },
    } as never);
    let attachmentReads = 0;

    const result = await reasoner.continueFamilyWork(
      {
        workId: "family-work-private-reads",
        scheduledOccurrence: null,
        objective: "Check what we know about pickup and verify the latest school form in Gmail.",
        visibility: "private",
        ownerAdultId: "adult-1",
        origin: familyWorkOrigin(
          "Check what we know about pickup and verify the latest school form in Gmail.",
        ),
        household: {
          householdId: "household-1",
          familyLabel: "Test family",
          timeZone: "America/Los_Angeles",
          postalCode: "90045",
          adults: [{ adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" }],
          children: [],
        },
        visibleSources: [memory],
        googleConnections: [
          {
            emailLabel: "Personal Google",
            calendarAvailable: true,
            kind: "personal",
            writesEnabled: false,
          },
        ],
        state: {
          kind: "family_work_v1",
          version: 1,
          generation: 0,
          phase: "ready",
          claim: null,
          activePhoneCall: null,
          activeTextMessage: null,
          pendingParticipantRequest: null,
          browserSession: null,
          continuationItems: [],
          pendingCall: null,
          steering: [],
          progressRevision: 0,
          terminal: null,
        },
        currentTime: NOW,
      },
      {
        async searchVault() {
          return {
            query: "pickup",
            results: [
              {
                uri: memoryUri,
                score: 1,
                abstract: "School pickup is normally at 2:45 PM.",
                memoryKind: "routine" as const,
                artifactKind: null,
                title: "School pickup",
                tags: ["school", "pickup"],
                updatedAt: NOW,
              },
            ],
            total: 1,
            complete: true,
            nextCursor: null,
          };
        },
        async readVault() {
          return {
            uri: memoryUri,
            level: "overview" as const,
            memory: {
              factId: memoryFactId,
              statement: "School pickup is normally at 2:45 PM.",
              memoryKind: "routine" as const,
              artifactKind: null,
              title: "School pickup",
              details: "School pickup is normally at 2:45 PM.",
              tags: ["school", "pickup"],
              files: [],
              visibility: "private" as const,
              updatedAt: NOW,
            },
            supports: [],
          };
        },
        async searchGmail() {
          return { status: "complete", sources: [gmail] };
        },
        async readGmailAttachment(input) {
          attachmentReads += 1;
          return {
            sourceId: input.sourceId,
            attachmentRef: input.attachment.attachmentRef,
            filename: input.attachment.filename,
            mimeType: input.attachment.mimeType,
            bytes: new Uint8Array(Buffer.from("%PDF-")),
          };
        },
      },
    );

    expect(result).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("2:45 PM"),
    });
    expect(attachmentReads).toBe(1);
    expect(((requests[0]?.tools as { name: string }[]) ?? []).map((tool) => tool.name)).toEqual(
      expect.arrayContaining(["search_vault", "read_vault", "search_gmail", "read_gmail_attachment"]),
    );
    expect(JSON.stringify(requests.at(-1)?.input)).toContain("input_file");
    expect(JSON.stringify(requests)).not.toContain("connectionId");
  });

  test("a Vault item discovered and read this turn can be corrected in place", async () => {
    const factId = "22222222-2222-4222-8222-222222222222";
    const uri = `vault://fact/${factId}`;
    const corrected = ordinaryDecision({ bubbleText: "Got it—I updated the noodle recipe to use tamari." });
    corrected.facts = [
      {
        operation: "correct",
        factId,
        statement: "The family noodle recipe uses tamari instead of soy sauce.",
        visibility: "household",
        memory: {
          memoryKind: "artifact",
          artifactKind: "recipe",
          title: "Weeknight noodles",
          details: "Toss noodles with sesame oil, tamari, and rice vinegar. Use tamari instead of soy sauce.",
          tags: ["dinner", "noodles", "tamari"],
        },
        sourceIds: ["turn-1"],
      },
    ];
    const responses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("vault-search-correction", "search_vault", { query: "noodle recipe", cursor: null }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("vault-read-correction", "read_vault", { uri, level: "overview" })],
      },
      {
        status: "completed",
        output_parsed: corrected,
        output: [decisionMessage("vault-correction", corrected)],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => {
          const response = responses.shift();
          if (!response) throw new Error("Unexpected Vault-correction model turn");
          return fakeStream(response);
        },
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text = "Actually, use tamari instead of soy sauce in that noodle recipe.";
    input.currentMessage.authoredText = input.currentMessage.text;

    const result = await reasoner.decide(input, {
      ...inertReads(),
      async searchVault() {
        return {
          query: "noodle recipe",
          results: [
            {
              uri,
              score: 1,
              abstract: "The family weeknight noodle recipe.",
              memoryKind: "artifact" as const,
              artifactKind: "recipe" as const,
              title: "Weeknight noodles",
              tags: ["dinner", "noodles"],
              updatedAt: NOW,
            },
          ],
          total: 1,
          complete: true,
          nextCursor: null,
        };
      },
      async readVault() {
        return {
          uri,
          level: "overview" as const,
          memory: {
            factId,
            statement: "The family noodle recipe uses soy sauce.",
            memoryKind: "artifact" as const,
            artifactKind: "recipe" as const,
            title: "Weeknight noodles",
            details: "Toss noodles with sesame oil, soy sauce, and rice vinegar.",
            tags: ["dinner", "noodles"],
            files: [],
            visibility: "household" as const,
            updatedAt: NOW,
          },
          supports: [],
        };
      },
    });

    expect(result.facts).toEqual(corrected.facts);
    expect(responses).toHaveLength(0);
  });

  test("a complete Family Calendar cursor chain retains an earlier-page target for update", async () => {
    const requests: Record<string, unknown>[] = [];
    const calendarRef = "calendar-family";
    const calendarEvents = Array.from({ length: 73 }, (_, index) => ({
      eventRef: `event-${index.toString().padStart(2, "0")}`,
      providerUpdatedAt: "2026-08-27T19:00:00.000Z",
      calendarRef,
      calendarLabel: "Family Calendar",
      title: `Family event ${index + 1}`,
      location: null,
      status: "confirmed" as const,
      busy: true,
      intervalKind: "timed" as const,
      startsAt: "2026-08-28T16:00:00.000Z",
      endsAt: "2026-08-28T18:00:00.000Z",
      timeZone: "America/Los_Angeles",
    }));
    const calendarCoverage = [
      {
        calendarRef,
        label: "Family Calendar",
        timeZone: "America/Los_Angeles",
        primary: null,
        accessRole: null,
        status: "complete" as const,
        eventCount: 73,
      },
    ];
    const firstRead = {
      status: "truncated" as const,
      calendars: calendarCoverage,
      events: calendarEvents.slice(0, 50),
      totalCalendarCount: 1,
      totalEventCount: 73,
      nextCursor: "calendar-page-2",
    };
    const secondRead = {
      status: "complete" as const,
      calendars: calendarCoverage,
      events: calendarEvents.slice(50),
      totalCalendarCount: 1,
      totalEventCount: 73,
      nextCursor: null,
    };
    const calendarArguments = {
      timeMin: "2026-08-28T00:00:00.000Z",
      timeMax: "2026-08-29T00:00:00.000Z",
      pageSize: 50,
      cursor: null,
      scope: "all",
      calendarRefs: [],
    };
    const completedDecision = ordinaryDecision();
    completedDecision.conversation.bubbles = [];
    completedDecision.calendar = {
      mode: "direct",
      proposalId: null,
      mutation: {
        operation: "update",
        event: {
          intervalKind: "timed",
          title: "Updated family event",
          startsAt: "2026-08-28T16:00:00.000Z",
          endsAt: "2026-08-28T18:00:00.000Z",
          timeZone: "America/Los_Angeles",
          location: null,
        },
        target: {
          eventRef: calendarEvents[0]?.eventRef ?? "missing-event",
          observedEvent: {
            intervalKind: "timed",
            title: calendarEvents[0]?.title ?? "Missing family event",
            startsAt: "2026-08-28T16:00:00.000Z",
            endsAt: "2026-08-28T18:00:00.000Z",
            timeZone: "America/Los_Angeles",
            location: null,
          },
        },
      },
      sourceIds: ["turn-1"],
    };
    const responses = [
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("calendar-catalog", "list_calendars", {})],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("calendar-window-1", "read_calendar_window", calendarArguments)],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("calendar-window-2", "read_calendar_window", {
            ...calendarArguments,
            cursor: firstRead.nextCursor,
          }),
        ],
      },
      { status: "completed", output_parsed: completedDecision, output: [] },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          const response = responses.shift();
          if (!response) throw new Error("Unexpected model request");
          return fakeStream(response);
        },
      },
    } as never);
    const calendarInputs: Record<string, unknown>[] = [];

    const input = foregroundInput();
    input.audience = "group";
    input.googleConnections = [
      { emailLabel: "Family Google", calendarAvailable: true, kind: "family", writesEnabled: true },
    ];
    const result = await reasoner.decide(input, {
      ...inertReads(),
      async listCalendars() {
        return {
          status: "complete",
          calendars: [
            {
              calendarRef,
              label: "Family Calendar",
              timeZone: "America/Los_Angeles",
              primary: null,
              accessRole: null,
              eventCoverage: "readable",
            },
          ],
          totalCalendarCount: 1,
        };
      },
      async readCalendarWindow(input) {
        calendarInputs.push(input);
        return input.cursor === null ? firstRead : secondRead;
      },
    });

    expect(calendarInputs).toEqual([
      calendarArguments,
      { ...calendarArguments, cursor: firstRead.nextCursor },
    ]);
    const firstEnvelope = functionOutputEnvelopes(requests[2]).find(
      (envelope) => envelope.callId === "calendar-window-1",
    );
    expect(firstEnvelope?.output).toMatchObject({
      status: "truncated",
      totalEventCount: 73,
      nextCursor: firstRead.nextCursor,
      events: { length: 50 },
    });
    const secondEnvelope = functionOutputEnvelopes(requests[3]).find(
      (envelope) => envelope.callId === "calendar-window-2",
    );
    expect(secondEnvelope?.output).toMatchObject({
      status: "complete",
      totalEventCount: 73,
      nextCursor: null,
      events: { length: 23 },
    });
    const observedEvents = [firstEnvelope, secondEnvelope].flatMap((envelope) => {
      const output = envelope?.output as { events?: readonly { eventRef?: unknown }[] } | undefined;
      return output?.events?.map((event) => event.eventRef) ?? [];
    });
    expect(observedEvents).toEqual(calendarEvents.map((event) => event.eventRef));
    expect(new Set(observedEvents).size).toBe(73);
    expect(result.calendar).toEqual(completedDecision.calendar);
  });

  test("a truncated Calendar page cannot authorize a Family Calendar write", async () => {
    const calendarEvent = {
      eventRef: "event-existing",
      providerUpdatedAt: "2026-08-27T19:00:00.000Z",
      calendarRef: "calendar-family",
      calendarLabel: "Family Calendar",
      title: "Existing family event",
      location: null,
      status: "confirmed" as const,
      busy: true,
      intervalKind: "timed" as const,
      startsAt: "2026-08-28T16:00:00.000Z",
      endsAt: "2026-08-28T17:00:00.000Z",
      timeZone: "America/Los_Angeles",
    };
    const decision = ordinaryDecision();
    decision.conversation.bubbles = [];
    decision.calendar = {
      mode: "direct",
      proposalId: null,
      mutation: {
        operation: "create",
        event: {
          intervalKind: "timed",
          title: "New family event",
          startsAt: "2026-08-28T18:00:00.000Z",
          endsAt: "2026-08-28T19:00:00.000Z",
          timeZone: "America/Los_Angeles",
          location: null,
        },
        target: null,
      },
      sourceIds: ["turn-1"],
    };
    const responses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("calendar-window", "read_calendar_window", {
            timeMin: "2026-08-28T00:00:00.000Z",
            timeMax: "2026-08-29T00:00:00.000Z",
            pageSize: 50,
            cursor: null,
            scope: "all",
            calendarRefs: [],
          }),
        ],
      },
      { status: "completed", output_parsed: decision, output: [] },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => {
          const response = responses.shift();
          if (!response) throw new Error("Unexpected model request");
          return fakeStream(response);
        },
      },
    } as never);
    const input = foregroundInput();
    input.audience = "group";
    input.googleConnections = [
      { emailLabel: "Family Google", calendarAvailable: true, kind: "family", writesEnabled: true },
    ];

    await expect(
      reasoner.decide(input, {
        ...inertReads(),
        async readCalendarWindow() {
          return {
            status: "truncated",
            calendars: [
              {
                calendarRef: "calendar-family",
                label: "Family Calendar",
                timeZone: "America/Los_Angeles",
                primary: false,
                accessRole: "owner",
                status: "complete",
                eventCount: 73,
              },
            ],
            totalCalendarCount: 1,
            events: [calendarEvent],
            totalEventCount: 73,
            nextCursor: "calendar-page-2",
          };
        },
      }),
    ).rejects.toMatchObject({
      code: "invalid_output",
      message: expect.stringContaining("complete covering family read"),
    });
  });

  test("truncated Calendar metadata remains usable for a focused read", async () => {
    const requests: Record<string, unknown>[] = [];
    const responses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("calendar-window", "read_calendar_window", {
            timeMin: "2026-08-28T00:00:00.000Z",
            timeMax: "2026-08-29T00:00:00.000Z",
            pageSize: 50,
            cursor: null,
            scope: "all",
            calendarRefs: [],
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: ordinaryDecision({ bubbleText: "I found the event you asked about." }),
        output: [],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          const response = responses.shift();
          if (!response) throw new Error("Unexpected model request");
          return fakeStream(response);
        },
      },
    } as never);
    const calendars = Array.from({ length: 100 }, (_, index) => ({
      calendarRef: `calendar-${index}`,
      label: `Calendar ${index + 1}`,
      timeZone: "America/Los_Angeles",
      primary: index === 0,
      accessRole: "owner" as const,
      status: "complete" as const,
      eventCount: index === 0 ? 1 : 0,
    }));

    const result = await reasoner.decide(foregroundInput(), {
      ...inertReads(),
      async readCalendarWindow() {
        return {
          status: "truncated",
          calendars,
          totalCalendarCount: 101,
          events: [
            {
              eventRef: "event-wanted",
              providerUpdatedAt: "2026-08-27T19:00:00.000Z",
              calendarRef: calendars[0]?.calendarRef ?? "missing-calendar",
              calendarLabel: calendars[0]?.label ?? "Calendar",
              title: "The event I wanted",
              location: null,
              status: "confirmed",
              busy: true,
              intervalKind: "timed",
              startsAt: "2026-08-28T16:00:00.000Z",
              endsAt: "2026-08-28T17:00:00.000Z",
              timeZone: "America/Los_Angeles",
            },
          ],
          totalEventCount: 1,
          nextCursor: null,
        };
      },
    });

    expect(result.conversation.bubbles[0]?.text).toContain("found the event");
    expect(functionOutputEnvelopes(requests[1])[0]?.output).toMatchObject({
      status: "truncated",
      totalCalendarCount: 101,
      nextCursor: null,
    });
  });

  test("both private Gmail attachment loops continue past the former four-read ceiling", async () => {
    const gmail = {
      ...privateGmailSource(),
      attachments: Array.from({ length: 5 }, (_, index) => ({
        attachmentRef: `attachment-${index + 1}`,
        filename: `form-${index + 1}.pdf`,
        mimeType: "application/pdf" as const,
        sizeBytes: 5,
      })),
    };
    const requests: Record<string, unknown>[] = [];
    let workStarts = 0;
    const attachmentSequence = (prefix: string) => [
      ...Array.from({ length: 5 }, (_, index) => ({
        status: "completed",
        output_parsed: null,
        output: [
          functionCall(`${prefix}-attachment-${index + 1}`, "read_private_gmail_attachment", {
            sourceId: gmail.sourceId,
            attachmentRef: gmail.attachments[index]?.attachmentRef,
          }),
        ],
      })),
      {
        status: "completed",
        output_parsed: { findings: [], facts: [], dismissedSourceIds: [gmail.sourceId] },
        output: [],
      },
    ];
    const responses = [...attachmentSequence("batch"), ...attachmentSequence("change")];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse: (request: Record<string, unknown>) => {
          requests.push(request);
          const response = responses.shift();
          if (!response) throw new Error("Unexpected model request");
          return response;
        },
      },
    } as never);
    let attachmentReads = 0;
    const reads = {
      async readGmailAttachment(input: { attachment: { attachmentRef: string; filename: string } }) {
        attachmentReads += 1;
        return {
          sourceId: gmail.sourceId,
          attachmentRef: input.attachment.attachmentRef,
          filename: input.attachment.filename,
          mimeType: "application/pdf" as const,
          bytes: new Uint8Array(Buffer.from("%PDF-")),
        };
      },
    };
    const presentation = {
      onWorkStarted() {
        workStarts += 1;
      },
    };

    await reasoner.classifyPrivateGoogleBatch(privateBatchInput(gmail), reads, undefined, presentation);
    await reasoner.assessGoogleChanges(privateAssessmentInput(gmail), reads, undefined, presentation);

    expect(attachmentReads).toBe(10);
    expect(workStarts).toBe(2);
    for (const firstRequest of [requests[0], requests[6]]) {
      expect(JSON.stringify(firstRequest)).not.toContain("private-google-connection");
      expect(JSON.stringify(firstRequest)).not.toContain("connectionId");
      expect(String(firstRequest?.instructions)).toContain("privateDocket");
      expect(String(firstRequest?.instructions)).toContain("nextAction is the smallest concrete move");
      expect(String(firstRequest?.instructions)).toContain("account ownership");
      expect(((firstRequest?.tools as { name: string }[]) ?? []).map((tool) => tool.name)).toEqual([
        "read_private_gmail_attachment",
      ]);
    }
    expect(JSON.stringify(requests[6])).toContain("artifact:recipe:weeknight-noodles");
    expect(JSON.stringify(requests[6])).toContain(
      "A reusable family recipe with noodles, sesame oil, soy sauce, and rice vinegar.",
    );
    for (const request of requests) {
      expect(request).not.toHaveProperty("max_tool_calls");
    }
    for (const continuation of [...requests.slice(1, 6), ...requests.slice(7, 12)]) {
      const output = functionOutputs(continuation)[0]?.output;
      expect(Array.isArray(output)).toBe(true);
      const serialized = JSON.stringify(output);
      const envelope = JSON.parse((output as { type: string; text?: string }[])[0]?.text ?? "{}") as {
        outcome?: string;
      };
      expect(envelope.outcome).toBe("succeeded");
      expect(serialized).toContain(gmail.sourceId);
      expect(serialized).toContain("input_file");
    }
  });

  test("both private Google decisions carry owner-private docket coordination", () => {
    const privateDocket = {
      owner: "Hari",
      nextAction: "Choose whether to send the permission form.",
      waitingOn: "Hari's decision about the permission form",
      needsAnswer: true,
    };
    const shared = {
      privateDocket,
      actionAnchor: "Permission form",
      familyRelevance: "owner_private" as const,
      sourceIds: ["gmail-source-1"],
      urgency: "soon" as const,
      dueAt: null,
    };
    const initial = {
      privateSummary: "The permission form still needs a decision.",
      ...shared,
      surfaceNow: false,
      candidate: null,
      monitor: null,
      familyCalendar: null,
    };
    const incremental = {
      privateDetail: "The permission form still needs a decision.",
      ...shared,
      householdConclusion: null,
      materialChange: true,
      monitor: null,
      familyCalendar: null,
    };

    expect(
      florencePrivateGoogleBatchDecisionSchema.parse({
        findings: [initial],
        facts: [],
        dismissedSourceIds: [],
      }).findings[0]?.privateDocket,
    ).toEqual(privateDocket);
    expect(
      florenceGoogleChangesAssessmentDecisionSchema.parse({
        findings: [incremental],
        facts: [],
        dismissedSourceIds: [],
        nextJob: null,
      }).findings[0]?.privateDocket,
    ).toEqual(privateDocket);
    const { privateDocket: _privateDocket, ...withoutPrivateDocket } = initial;
    expect(
      florencePrivateGoogleBatchDecisionSchema.safeParse({
        findings: [withoutPrivateDocket],
        facts: [],
        dismissedSourceIds: [],
      }).success,
    ).toBe(false);
  });

  test("keeps every durable fact supported by one Google transport batch", async () => {
    const statements = Array.from(
      { length: 21 },
      (_, index) => `Durable family preference ${index + 1} is choice ${index + 1}.`,
    );
    const gmail = {
      ...privateGmailSource(),
      text: statements.join(" "),
      attachments: [],
    };
    const facts = statements.map((statement, index) => ({
      slot: `family:preference:${index + 1}`,
      statement,
      memory: {
        memoryKind: "preference" as const,
        artifactKind: null,
        title: null,
        details: null,
        tags: [],
      },
      familyRelevance: "household" as const,
      sourceIds: [gmail.sourceId],
    }));
    const privateBatchDecision = florencePrivateGoogleBatchDecisionSchema.parse({
      findings: [],
      facts,
      dismissedSourceIds: [],
    });
    const changesDecision = florenceGoogleChangesAssessmentDecisionSchema.parse({
      findings: [],
      facts,
      dismissedSourceIds: [],
      nextJob: null,
    });
    const requests: Record<string, unknown>[] = [];
    const responses = [privateBatchDecision, changesDecision];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse: (request: Record<string, unknown>) => {
          requests.push(request);
          const output_parsed = responses.shift();
          if (!output_parsed) throw new Error("Unexpected model request");
          return { status: "completed", output_parsed, output: [] };
        },
      },
    } as never);
    const reads = {
      async readGmailAttachment(): Promise<never> {
        throw new Error("The source has no attachment");
      },
    };

    const initial = await reasoner.classifyPrivateGoogleBatch(privateBatchInput(gmail), reads);
    const incremental = await reasoner.assessGoogleChanges(privateAssessmentInput(gmail), reads);

    expect(initial.facts).toEqual(facts);
    expect(incremental.facts).toEqual(facts);
    expect(String(requests[0]?.instructions)).toContain("Return every eligible fact supported by this batch");
    expect(String(requests[1]?.instructions)).toContain("never omit one merely to satisfy an output count");
    expect(requests.map((request) => String(request.instructions))).not.toEqual(
      expect.arrayContaining([expect.stringContaining("up to twenty")]),
    );
  });
});

function foregroundInput(): FlorenceReasonerInput {
  return {
    household: {
      householdId: "household-1",
      name: "Test family",
      timeZone: "America/Los_Angeles",
      adultNames: ["Hari", "Jackson"],
      familyProfile: "A test family.",
    },
    audience: "private",
    currentAdultId: "adult-1",
    currentTime: NOW,
    currentMessage: {
      sourceId: "turn-1",
      senderName: "Hari",
      moveKind: "message",
      text: "Please check the current details.",
      authoredText: "Please check the current details.",
      voiceTranscriptPresent: false,
      occurredAt: NOW,
      images: [],
      pdfs: [],
      replyTo: null,
    },
    recentMessages: [],
    visibleSources: [],
    pendingFollowUps: [],
    householdDocket: { totalItems: 0, items: [] },
    visibleReminders: [],
    visibleFamilyWork: [],
    visibleInterests: [],
    pendingCalendarOffers: [],
    googleConnections: [
      { emailLabel: "Personal Google", calendarAvailable: true, kind: "personal", writesEnabled: false },
    ],
  };
}

function ordinaryDecision(input: { bubbleText?: string; researchUrls?: string[] } = {}): FlorenceDecision {
  return {
    policy: { retain: true, schedule: true, stopMessaging: false },
    conversation: {
      replyToCurrentMessage: false,
      reaction: null,
      bubbles: [{ text: input.bubbleText ?? "Done.", delayMs: 0 }],
      nativeMoves: null,
    },
    facts: [],
    followUp: null,
    reminder: null,
    familyWork: null,
    docketUpsert: null,
    docketCompletions: null,
    interest: null,
    calendar: null,
    householdUpdate: null,
    webAccessPath: null,
    researchUrls: input.researchUrls ?? null,
  };
}

function browserArguments(operation: string, overrides: Record<string, unknown> = {}) {
  return {
    operation,
    url: null,
    ref: null,
    text: null,
    sourceId: null,
    attachmentRef: null,
    values: [],
    checked: null,
    key: null,
    direction: null,
    milliseconds: null,
    compact: true,
    code: null,
    timeoutSeconds: null,
    actions: [],
    screenshot: false,
    ...overrides,
  };
}

function browserObservation(
  input: Partial<FlorenceBrowserObservation> & Pick<FlorenceBrowserObservation, "url" | "title" | "snapshot">,
): FlorenceBrowserObservation {
  return {
    kind: "page",
    reason: null,
    refCount: (input.snapshot.match(/\[ref=/gu) ?? []).length,
    truncated: false,
    ...input,
  };
}

function telephonyResult(
  input: Partial<FlorenceTelephonyResult> &
    Pick<FlorenceTelephonyResult, "kind" | "provider" | "operation" | "providerId" | "providerStatus">,
): FlorenceTelephonyResult {
  return {
    reason: null,
    toPhoneNumberMasked: null,
    answeredBy: null,
    durationSeconds: null,
    summary: null,
    disposition: null,
    transcript: null,
    recordingUrl: null,
    messages: [],
    ...input,
  };
}

function functionCall(callId: string, name: string, args: object) {
  return {
    id: `item-${callId}`,
    type: "function_call" as const,
    call_id: callId,
    name,
    arguments: JSON.stringify(args),
    status: "completed" as const,
  };
}

function decisionMessage(id: string, decision: FlorenceDecision) {
  return {
    id: `message-${id}`,
    type: "message" as const,
    role: "assistant" as const,
    status: "completed" as const,
    content: [
      {
        type: "output_text" as const,
        text: JSON.stringify(decision),
        annotations: [],
      },
    ],
  };
}

function familyWorkResultMessage(id: string, result: object) {
  return {
    id: `message-${id}`,
    type: "message" as const,
    role: "assistant" as const,
    status: "completed" as const,
    content: [
      {
        type: "output_text" as const,
        text: JSON.stringify(result),
        annotations: [],
      },
    ],
  };
}

function completedWebSearch(url: string, query = "current result", id = "web-search-1") {
  return {
    id,
    type: "web_search_call" as const,
    status: "completed" as const,
    action: { type: "search" as const, query, sources: [{ type: "url", url }] },
  };
}

function fakeStream(response: unknown) {
  return {
    async *[Symbol.asyncIterator]() {},
    async finalResponse() {
      return response;
    },
  };
}

function functionOutputs(request: Record<string, unknown> | undefined) {
  return ((request?.input as { type?: string; call_id?: string; output?: unknown }[]) ?? []).filter(
    (item) => item.type === "function_call_output",
  );
}

function functionOutputEnvelopes(request: Record<string, unknown> | undefined) {
  return functionOutputs(request).map((item) => ({
    callId: item.call_id,
    ...(JSON.parse(typeof item.output === "string" ? item.output : "{}") as {
      outcome: string;
      output: unknown;
      error: { code: string } | null;
    }),
  }));
}

function privateGmailSource() {
  return {
    sourceId: "gmail-private-1",
    kind: "gmail" as const,
    visibility: "adult_private" as const,
    sentAt: "2026-08-27T19:00:00.000Z",
    sender: "School",
    subject: "School form",
    text: "Please review School form",
    textStatus: "complete" as const,
    attachments: [
      {
        attachmentRef: "attachment-1",
        filename: "form.pdf",
        mimeType: "application/pdf" as const,
        sizeBytes: 5,
      },
    ],
    attachmentsStatus: "complete" as const,
  };
}

function conversationalGmailSource() {
  return privateGmailSource();
}

function completeCalendarRead() {
  return {
    status: "complete" as const,
    calendars: [],
    totalCalendarCount: 0,
    events: [],
    totalEventCount: 0,
    nextCursor: null,
  };
}

function publicPageResult(url: string, title: string, text: string) {
  return {
    requestedUrl: url,
    finalUrl: url,
    kind: "html" as const,
    title,
    filename: null,
    text,
    offset: 0,
    nextOffset: null,
    contentFingerprint: "0".repeat(64),
    truncated: false,
    totalCleanCharacters: text.length,
    totalCleanBytes: Buffer.byteLength(text),
    responseBytes: Buffer.byteLength(text),
    fetchedAt: NOW,
  };
}

function weatherResult() {
  return {
    kind: "hourly" as const,
    coordinates: { lat: 34.0522, lon: -118.2437 },
    location: {
      city: "Los Angeles",
      state: "CA",
      timeZone: "America/Los_Angeles",
      forecastOfficeUrl: "https://api.weather.gov/offices/LOX",
      gridId: "LOX",
      gridX: 154,
      gridY: 44,
    },
    requestedPeriodCount: 12,
    forecastGeneratedAt: "2026-08-28T20:00:00Z",
    forecastUpdatedAt: "2026-08-28T19:00:00Z",
    periods: [
      {
        number: 1,
        name: "This Afternoon",
        startTime: "2026-08-28T13:00:00-07:00",
        endTime: "2026-08-28T14:00:00-07:00",
        isDaytime: true,
        temperature: 82,
        temperatureUnit: "F",
        precipitationChancePercent: 5,
        windSpeed: "5 mph",
        windDirection: "SW",
        condition: "Mostly Sunny",
        detailedForecast: "Mostly sunny.",
        iconUrl: null,
      },
    ],
    observation: null,
    alertsAvailable: true,
    activeAlertCount: 0,
    alertsTruncated: false,
    alerts: [],
    fetchedAt: "2026-08-28T20:01:00Z",
    attribution: {
      provider: "National Weather Service" as const,
      label: "Weather data from the U.S. National Weather Service" as const,
      url: "https://www.weather.gov/",
    },
  };
}

function flightSegment(carrier: string, carrierName: string, flightNumber: string) {
  return {
    from: "JFK",
    to: "LAX",
    fromCity: "New York",
    toCity: "Los Angeles",
    fromName: "John F. Kennedy International Airport",
    toName: "Los Angeles International Airport",
    fromCountry: "US",
    toCountry: "US",
    departureTime: "2026-08-27T19:00:00",
    arrivalTime: "2026-08-27T22:00:00",
    durationSeconds: 21_600,
    carrier,
    carrierName,
    flightNumber,
    cabinClass: "M",
  };
}

function flightResult(bookingUrl: string, returnedCount = 1) {
  const first = {
    id: "alternative-1",
    price: 412,
    priceFormatted: "$412",
    totalDurationSeconds: 21_600,
    bookingUrl,
    imageId: null,
    baggage: null,
    outbound: {
      from: "JFK",
      to: "LAX",
      departureTime: "2026-08-27T19:00:00",
      arrivalTime: "2026-08-27T22:00:00",
      durationSeconds: 21_600,
      stops: 0,
      route: ["JFK", "LAX"],
      cabinClass: "M",
      segments: [flightSegment("DL", "Delta", "DL 321")],
    },
    inbound: null,
    highlights: ["cheapest" as const, "shortest" as const, "earliest" as const],
  };
  const second = {
    id: "alternative-2",
    price: 438,
    priceFormatted: "$438",
    totalDurationSeconds: 21_900,
    bookingUrl: `${bookingUrl}&option=2`,
    imageId: null,
    baggage: null,
    outbound: {
      from: "JFK",
      to: "LAX",
      departureTime: "2026-08-27T20:15:00",
      arrivalTime: "2026-08-27T23:20:00",
      durationSeconds: 21_900,
      stops: 0,
      route: ["JFK", "LAX"],
      cabinClass: "M",
      segments: [flightSegment("B6", "JetBlue", "B6 523")],
    },
    inbound: null,
    highlights: [] as ("cheapest" | "shortest" | "earliest")[],
  };
  const itineraries = returnedCount > 1 ? [first, second] : [first];
  return {
    operation: "search" as const,
    query: "JFK to LAX",
    currency: "USD",
    passengers: { adults: 1, children: 0, infants: 0 },
    resultsCount: returnedCount,
    returnedCount,
    itineraries,
    searchTimeMs: 250,
    error: null,
    highlights: {
      cheapestItineraryId: "alternative-1",
      shortestItineraryId: "alternative-1",
      earliestItineraryId: "alternative-1",
    },
    timeBasis: "provider_local_time_at_each_airport" as const,
    provider: {
      name: "Kiwi.com" as const,
      searchOnly: true as const,
      bookingOccursOnProvider: true as const,
      url: "https://www.kiwi.com/" as const,
    },
  };
}

function familyProfile() {
  return {
    familyLabel: "Test family",
    timeZone: "America/Los_Angeles",
    adultFirstNames: ["Hari", "Jackson"],
    children: [],
    postalCode: null,
  };
}

function privateBatchInput(gmail: ReturnType<typeof privateGmailSource>): FlorencePrivateGoogleBatchInput {
  return {
    familyProfile: familyProfile(),
    adult: { adultId: "adult-1", firstName: "Hari" },
    googleConnection: { connectionId: "private-google-connection", status: "active", kind: "personal" },
    currentTime: NOW,
    currentFacts: [],
    sources: [gmail],
    reviewKind: "initial",
  };
}

function privateAssessmentInput(
  gmail: ReturnType<typeof privateGmailSource>,
): FlorenceGoogleChangesAssessmentInput {
  return {
    familyProfile: familyProfile(),
    adult: { adultId: "adult-1", firstName: "Hari" },
    googleConnection: { connectionId: "private-google-connection", status: "active", kind: "personal" },
    currentTime: NOW,
    evidence: {
      gmail: {
        status: "complete",
        after: "2026-08-26T20:00:00.000Z",
        before: NOW,
        sources: [gmail],
      },
      calendar: {
        status: "complete",
        timeMin: NOW,
        timeMax: "2026-09-03T20:00:00.000Z",
        events: [],
      },
    },
    activeMonitors: [],
    memory: [
      {
        slot: "artifact:recipe:weeknight-noodles",
        label: "Weeknight noodles",
        text: "A reusable family recipe with noodles, sesame oil, soy sauce, and rice vinegar.",
      },
    ],
    currentFacts: [],
  };
}

function inertReads() {
  return {
    ...admittedReadAccounting,
    async searchFamilyMemory() {
      return [];
    },
    async readSource() {
      return null;
    },
    async searchGmail() {
      return { status: "complete" as const, sources: [] };
    },
    async readCalendarWindow() {
      return completeCalendarRead();
    },
    async readCurrentImage() {
      throw new Error("No image was authorized");
    },
  };
}
