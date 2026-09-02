import type { FamilyWorkOriginContext, FamilyWorkStateV1 } from "@florence/database";
import { describe, expect, test } from "vitest";
import {
  type FlorenceHouseholdNextActionInput,
  FlorenceReasoner,
  florenceDecisionSchema,
  florenceHouseholdBriefingInputSchema,
  florenceHouseholdSafeCandidateSchema,
  florencePrivateDocketCoordinationSchema,
} from "./reasoner.js";
import {
  completedWebSearch,
  decisionMessage,
  defaultFamilyWorkCompletionReview,
  fakeStream,
  familyWorkOrigin,
  foregroundInput,
  functionCall,
  functionOutputEnvelopes,
  inertReads,
  NOW,
  ordinaryDecision,
  PUBLIC_URL,
  publicPageResult,
} from "./reasoner-tool-loops.test-kit.js";

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

  test("only intentional family-group observation can be totally silent", async () => {
    const silentResponse = ordinaryDecision();
    silentResponse.conversation.bubbles = [];
    const privateReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => fakeStream({ status: "completed", output_parsed: silentResponse, output: [] }),
      },
    } as never);

    await expect(privateReasoner.decide(foregroundInput(), inertReads())).rejects.toMatchObject({
      code: "invalid_output",
      message: expect.stringContaining("no visible conversational move"),
    });

    const observation = ordinaryDecision({ participation: "observe" });
    let groupRequest: Record<string, unknown> | null = null;
    const groupReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          groupRequest = request;
          return fakeStream({
            status: "completed",
            output_parsed: observation,
            output: [completedWebSearch(PUBLIC_URL)],
          });
        },
      },
    } as never);
    const groupInput = foregroundInput();
    groupInput.audience = "group";
    groupInput.googleConnections = [
      { emailLabel: "Family", calendarAvailable: true, kind: "family", writesEnabled: false },
    ];

    await expect(groupReasoner.decide(groupInput, inertReads())).resolves.toMatchObject({
      policy: { retain: false, schedule: false },
      conversation: { participation: "observe", bubbles: [], nativeMoves: null },
    });
    expect(JSON.stringify(groupRequest)).toContain("clearly addressed only to the other enrolled adult");
    expect(JSON.stringify(groupRequest)).toContain("ordinary family request or task with no named addressee");

    const noisyObservation = ordinaryDecision({ participation: "observe" });
    noisyObservation.conversation.bubbles = [{ text: "I’m listening.", delayMs: 0 }];
    const noisyReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => fakeStream({ status: "completed", output_parsed: noisyObservation, output: [] }),
      },
    } as never);
    await expect(noisyReasoner.decide(groupInput, inertReads())).rejects.toMatchObject({
      code: "invalid_output",
      message: expect.stringContaining("observed"),
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
      completionCondition:
        "The supplied options are compared and the best one is identified with supporting reasons.",
      responsibleAdultName: null,
      briefing: null,
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
    if (!repaired.familyWork) throw new Error("Commitment repair did not create durable work");
    expect(secondReview).toContain(repaired.familyWork.objective);
    expect(String(reviewRequests[0]?.instructions)).toContain("An unrelated mutation never backs");
    expect(String(reviewRequests[0]?.instructions)).toContain("conditional offer or capability statement");
  });

  test("an exact changing-state request starts one immediate durable task instead of a finite follow-up", async () => {
    const completionCondition = "The public status page explicitly says that\nregistration is open.";
    const decision = ordinaryDecision({
      bubbleText: "I’m on it—I’ll keep checking and let you know when registration opens.",
    });
    decision.familyWork = {
      operation: "create",
      workId: null,
      objective:
        "Keep checking the supplied public status page until it explicitly says that registration is open.",
      completionCondition,
      responsibleAdultName: null,
      briefing: null,
      schedule: null,
      instruction: null,
      candidateIds: [],
    };
    decision.followUp = null;
    const requests: Record<string, unknown>[] = [];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          return fakeStream({
            status: "completed",
            output_parsed: florenceDecisionSchema.parse(decision),
            output: [],
          });
        },
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text =
      "Keep checking https://example.com/registration until it actually says registration is open.";
    input.currentMessage.authoredText = input.currentMessage.text;

    const result = await reasoner.decide(input, inertReads());

    expect(result).toMatchObject({
      followUp: null,
      familyWork: {
        operation: "create",
        schedule: null,
        completionCondition,
      },
    });
    expect(String(requests[0]?.instructions)).toContain("changing external state");
  });

  test("family-work corrections can replace or preserve the completion condition", () => {
    const replacement = "The corrected requested result is confirmed.";
    const controls = [
      {
        operation: "update",
        workId: "work-1",
        objective: "Complete the corrected request.",
        completionCondition: replacement,
        responsibleAdultName: null,
        briefing: null,
        schedule: null,
        instruction: null,
      },
      {
        operation: "steer",
        workId: "work-1",
        objective: null,
        completionCondition: replacement,
        responsibleAdultName: null,
        schedule: null,
        instruction: "Use the corrected requested result.",
      },
    ] as const;

    for (const familyWork of controls) {
      expect(florenceDecisionSchema.parse({ ...ordinaryDecision(), familyWork }).familyWork).toMatchObject({
        operation: familyWork.operation,
        completionCondition: replacement,
      });
      expect(
        florenceDecisionSchema.parse({
          ...ordinaryDecision(),
          familyWork: { ...familyWork, completionCondition: null },
        }).familyWork,
      ).toMatchObject({ operation: familyWork.operation, completionCondition: null });
    }
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
      completionCondition: "Three suitable Saturday dinner options and their tradeoffs are reported.",
      responsibleAdultName: null,
      briefing: null,
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
        completionCondition: "The signed form is confirmed received by the school.",
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
    duplicateTracking.familyWork = {
      operation: "create",
      workId: null,
      objective: "Keep checking until the signed field-trip form is confirmed received by the school.",
      completionCondition: "The signed form is confirmed received by the school.",
      responsibleAdultName: null,
      briefing: null,
      schedule: null,
      instruction: null,
      candidateIds: [],
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
        completionCondition: "The signed form is confirmed received by the school.",
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
      completionCondition: "The after-school option is chosen and confirmed.",
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
      completionCondition: `Family item ${index + 1} is completed and confirmed.`,
    }));

    expect(florenceHouseholdBriefingInputSchema.shape.candidates.safeParse(candidates).success).toBe(true);
  });

  test("a household wake can start one provably distinct objective alongside current work", async () => {
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
      completionCondition: `Family item ${index + 1} is completed and confirmed.`,
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
      activeWork: [
        {
          workId: "work-1",
          candidateIds: ["candidate-1"],
          objective: "Finish the first family item.",
          currentProgress: "The first item is already moving.",
          status: "working",
          owner: "Florence",
          nextAction: "Complete the remaining step for family item 1.",
          waitingOn: null,
          needsAnswer: false,
          completionCondition: "Family item 1 is completed and confirmed.",
          nextCheckAt: null,
        },
      ],
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
        searches.push({ query: arguments_.query, cursor: arguments_.cursor });
        return arguments_.cursor === null
          ? {
              query: arguments_.query,
              results: [],
              total: 1,
              complete: false,
              nextCursor: "vault-page-2",
              retrievalMode: "lexical_fallback" as const,
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
              retrievalMode: "lexical_fallback" as const,
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

    for (const candidateIds of [["candidate-1"], []]) {
      const ambiguousReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
        responses: {
          parse: () => ({
            status: "completed",
            output_parsed: {
              message: "I noticed another useful move and I’m starting it now.",
              nextJob: {
                objective: "Start another family objective.",
                candidateIds,
              },
            },
            output: [],
          }),
        },
      } as never);

      await expect(
        ambiguousReasoner.decideHouseholdNextAction(input, {
          async searchVault() {
            throw new Error("Unexpected Vault search");
          },
          async readVault() {
            throw new Error("Unexpected Vault read");
          },
        }),
      ).rejects.toMatchObject({
        code: "invalid_output",
        message: expect.stringContaining("provably distinct"),
      });
    }
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
      completionCondition: "Three suitable Saturday dinner options and their tradeoffs are reported.",
      responsibleAdultName: null,
      briefing: null,
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
      completionCondition: "The school confirms the form was received.",
      responsibleAdultName: null,
      briefing: null,
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
          completionCondition: "The school confirms the form was received.",
          needsAnswer: false,
        },
      ],
    };
    input.visibleFamilyWork = [
      {
        workId: "work-1",
        objective: "Handle the school paperwork.",
        candidateIds: ["candidate-1"],
        responsibleAdultName: null,
        briefing: null,
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

  test("an inline task reply cannot steer a different visible task", async () => {
    const decision = ordinaryDecision();
    decision.familyWork = {
      operation: "steer",
      workId: "work-1",
      objective: null,
      completionCondition: null,
      responsibleAdultName: null,
      schedule: null,
      instruction: "Use Friday instead.",
    };
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => fakeStream({ status: "completed", output_parsed: decision, output: [] }),
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.moveKind = "reply";
    input.currentMessage.replyTo = {
      sourceId: "progress-2",
      familyWorkId: "work-2",
      senderName: "Florence",
      text: "I’m comparing the afternoon camp options now.",
      occurredAt: NOW,
    };
    input.visibleFamilyWork = [
      {
        workId: "work-1",
        objective: "Compare the morning camp options.",
        candidateIds: [],
        responsibleAdultName: null,
        briefing: null,
        currentProgress: "Comparing Monday and Tuesday.",
        schedule: null,
        paused: false,
        status: "active",
        nextAt: null,
        lastRunAt: null,
        lastResult: null,
        createdAt: NOW,
      },
      {
        workId: "work-2",
        objective: "Compare the afternoon camp options.",
        candidateIds: [],
        responsibleAdultName: null,
        briefing: null,
        currentProgress: "Comparing Thursday and Friday.",
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
      message: expect.stringContaining("direct task reply"),
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
          const review = defaultFamilyWorkCompletionReview(request);
          if (review) return review;
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
    const completionCondition = "The school form is submitted and the school confirms receipt.";
    const terminalDecision = {
      outcome: "partial",
      text: "I found the form, but the school portal is unavailable, so I couldn't submit it.",
      docket: {
        owner: "Florence",
        nextAction: "Submit the form when the school portal is available.",
        waitingOn: "The school portal to become available",
        needsAnswer: false,
        completionCondition: "The school form is submitted.",
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
      completionCondition,
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
    const expectedDocket = { ...terminalDecision.docket, completionCondition };

    expect(result).toMatchObject({
      kind: "terminal",
      outcome: "partial",
      docket: expectedDocket,
      state: { terminal: { docket: expectedDocket } },
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
          parse: (request: Record<string, unknown>) =>
            defaultFamilyWorkCompletionReview(request) ?? {
              status: "completed",
              output_parsed: invalidDecision,
              output: [],
            },
        },
      } as never);
      await expect(invalidReasoner.continueFamilyWork(input, {} as never)).rejects.toMatchObject({
        code: "invalid_output",
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
        completionCondition: "The children's pickup time is chosen and confirmed.",
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
        parse: (request: Record<string, unknown>) =>
          defaultFamilyWorkCompletionReview(request) ?? {
            status: "completed",
            output_parsed: {
              outcome: "succeeded",
              text: "Pickup is confirmed for 2:45 PM.",
              docket: null,
            },
            output: [],
          },
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
});
