import type { FamilyWorkOriginContext, FamilyWorkStateV1 } from "@florence/database";
import { describe, expect, test } from "vitest";
import {
  type FlorenceHouseholdNextActionInput,
  FlorenceReasoner,
  FlorenceReasonerError,
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

  test("an invalid-only native draft is repaired into a visible natural reply", async () => {
    const invalid = ordinaryDecision();
    invalid.conversation.bubbles = [];
    invalid.conversation.nativeMoves = [{ type: "rich_link", url: PUBLIC_URL }];
    const repaired = ordinaryDecision({
      bubbleText: "I couldn’t verify that link, but I can keep looking if you want.",
    });
    const requests: Record<string, unknown>[] = [];
    let turn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          turn += 1;
          const output = turn === 1 ? invalid : repaired;
          return fakeStream({
            status: "completed",
            output_parsed: output,
            output: [decisionMessage(`native-repair-${turn}`, output)],
          });
        },
      },
    } as never);

    await expect(reasoner.decide(foregroundInput(), inertReads())).resolves.toEqual(repaired);
    expect(requests).toHaveLength(2);
    expect(JSON.stringify(requests[1]?.input)).toContain("no visible conversational move");
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
    let privateTurns = 0;
    const privateReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => {
          privateTurns += 1;
          return fakeStream({ status: "completed", output_parsed: silentResponse, output: [] });
        },
      },
    } as never);

    await expect(privateReasoner.decide(foregroundInput(), inertReads())).rejects.toMatchObject({
      code: "invalid_output",
      message: expect.stringContaining("no visible conversational move"),
    });
    expect(privateTurns).toBe(3);

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
    await expect(noisyReasoner.decide(groupInput, inertReads())).resolves.toMatchObject({
      conversation: {
        participation: "respond",
        bubbles: [{ text: "I’m listening.", delayMs: 0 }],
      },
    });

    const workingObservation = ordinaryDecision({ participation: "observe" });
    workingObservation.familyWork = {
      operation: "create",
      workId: null,
      objective: "Find a family-friendly dinner option for tonight.",
      completionCondition: "A suitable dinner option is reported to the family.",
      responsibleAdultName: null,
      schedule: null,
      briefing: null,
      instruction: null,
      candidateIds: [],
    };
    const repairedObservation = ordinaryDecision({ participation: "observe" });
    const observationRequests: Record<string, unknown>[] = [];
    let observationTurn = 0;
    const workingReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          observationRequests.push(request);
          observationTurn += 1;
          const output = observationTurn === 1 ? workingObservation : repairedObservation;
          return fakeStream({
            status: "completed",
            output_parsed: output,
            output: [decisionMessage(`observed-parent-talk-${observationTurn}`, output)],
          });
        },
      },
    } as never);
    groupInput.currentMessage.text = "Jackson, can you pick up milk on your way home?";
    groupInput.currentMessage.authoredText = groupInput.currentMessage.text;
    await expect(workingReasoner.decide(groupInput, inertReads())).resolves.toMatchObject({
      conversation: { participation: "observe" },
      familyWork: null,
    });
    expect(observationRequests).toHaveLength(2);
    expect(JSON.stringify(observationRequests[1]?.input)).toContain(
      "observed parent-to-parent Message cannot perform work",
    );
  });

  test("a natural answer is repaired in-transcript when its retention and scheduling fields contradict it", async () => {
    const contradictory = ordinaryDecision({
      bubbleText: "The temporary code is blue. I won’t save it or schedule anything.",
    });
    contradictory.policy = { retain: false, schedule: false };
    contradictory.facts = [
      {
        operation: "remember",
        factId: null,
        statement: "The temporary code is blue.",
        visibility: "private",
        memory: {
          memoryKind: "fact",
          artifactKind: null,
          title: null,
          details: "The temporary code is blue.",
          tags: ["temporary code"],
        },
        sourceIds: ["turn-1"],
      },
    ];
    contradictory.reminder = {
      operation: "create",
      reminderId: null,
      action: "Check the temporary code",
      schedule: { kind: "once", at: "2026-08-28T20:00:00.000Z" },
    };
    const repaired = ordinaryDecision({
      bubbleText: "The temporary code is blue. I won’t save it or schedule anything.",
    });
    repaired.policy = { retain: false, schedule: false };
    const requests: Record<string, unknown>[] = [];
    let turn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          turn += 1;
          const output = turn === 1 ? contradictory : repaired;
          return fakeStream({
            status: "completed",
            output_parsed: output,
            output: [decisionMessage(`policy-repair-${turn}`, output)],
          });
        },
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text =
      "Answer this, but don’t remember it or schedule anything: the temporary code is blue.";
    input.currentMessage.authoredText = input.currentMessage.text;

    await expect(reasoner.decide(input, inertReads())).resolves.toMatchObject({
      policy: { retain: false, schedule: false },
      conversation: {
        bubbles: [{ text: "The temporary code is blue. I won’t save it or schedule anything.", delayMs: 0 }],
      },
      facts: [],
      reminder: null,
    });
    expect(requests).toHaveLength(2);
    expect(JSON.stringify(requests[1]?.input)).toContain("words and consequences agree");
  });

  test("one unreadable attachment does not discard the parent's text or other readable media", async () => {
    const requests: Record<string, unknown>[] = [];
    const decision = ordinaryDecision({
      bubbleText: "I can use the note and the readable flyer; the other image didn’t come through.",
    });
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          return fakeStream({ status: "completed", output_parsed: decision, output: [] });
        },
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text = "Please use my note and whichever flyer attachments you can read.";
    input.currentMessage.authoredText = input.currentMessage.text;
    input.currentMessage.images = [
      { assetId: "unreadable-image", mimeType: "image/jpeg" },
      { assetId: "readable-image", mimeType: "image/png" },
    ];
    input.currentMessage.pdfs = [
      {
        documentId: "readable-pdf",
        filename: "school-flyer.pdf",
        mimeType: "application/pdf",
        contentDigest: "a".repeat(64),
      },
      {
        documentId: "unreadable-pdf",
        filename: "missing-flyer.pdf",
        mimeType: "application/pdf",
        contentDigest: "b".repeat(64),
      },
    ];
    const reads = {
      ...inertReads(),
      async readCurrentImage(image: (typeof input.currentMessage.images)[number]) {
        if (image.assetId === "unreadable-image") {
          throw new Error("private adapter detail that must not enter the model transcript");
        }
        return { mimeType: image.mimeType, bytes: Uint8Array.from([1, 2, 3]) };
      },
      async readCurrentPdf(document: NonNullable<typeof input.currentMessage.pdfs>[number]) {
        if (document.documentId === "unreadable-pdf") {
          throw new Error("private PDF adapter detail that must not enter the model transcript");
        }
        return {
          mimeType: document.mimeType,
          bytes: Uint8Array.from([0x25, 0x50, 0x44, 0x46, 0x2d]),
        };
      },
    };

    await expect(reasoner.decide(input, reads)).resolves.toEqual(decision);

    const modelContent = (
      requests[0]?.input as Array<{
        content?: Array<{ type?: string; text?: string }>;
      }>
    )?.[0]?.content;
    expect(modelContent?.filter((part) => part.type === "input_image")).toHaveLength(1);
    expect(modelContent?.filter((part) => part.type === "input_file")).toHaveLength(1);
    const availability = modelContent?.find((part) => part.text?.includes("current_attachment_availability"));
    expect(availability?.text).toContain('"reference":"unreadable-image"');
    expect(availability?.text).toContain('"reference":"unreadable-pdf"');
    expect(availability?.text).toContain("Do not infer their contents");
    expect(availability?.text).not.toContain("private adapter detail");
    expect(JSON.stringify(modelContent)).toContain(input.currentMessage.text);
  });

  test("an unreadable PDF cannot substantiate a retained claim", async () => {
    const unsupported = ordinaryDecision({ bubbleText: "I saved the deadline from the flyer." });
    unsupported.facts = [
      {
        operation: "remember",
        factId: null,
        statement: "The school deadline is Friday.",
        visibility: "private",
        memory: {
          memoryKind: "fact",
          artifactKind: null,
          title: null,
          details: "The school deadline is Friday.",
          tags: ["school deadline"],
        },
        sourceIds: ["unreadable-pdf"],
      },
    ];
    const repaired = ordinaryDecision({
      bubbleText: "That flyer didn’t come through clearly. Please resend it and I’ll pull out the deadline.",
    });
    const requests: Record<string, unknown>[] = [];
    let turn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          turn += 1;
          const output = turn === 1 ? unsupported : repaired;
          return fakeStream({
            status: "completed",
            output_parsed: output,
            output: [decisionMessage(`unreadable-pdf-${turn}`, output)],
          });
        },
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text = "Please save the deadline from this flyer.";
    input.currentMessage.authoredText = input.currentMessage.text;
    input.currentMessage.pdfs = [
      {
        documentId: "unreadable-pdf",
        filename: "school-flyer.pdf",
        mimeType: "application/pdf",
        contentDigest: "c".repeat(64),
      },
    ];
    const reads = {
      ...inertReads(),
      async readCurrentPdf() {
        throw new Error("provider read failed");
      },
    };

    await expect(reasoner.decide(input, reads)).resolves.toEqual(repaired);
    expect(requests).toHaveLength(2);
    expect(JSON.stringify(requests[1]?.input)).toContain("cited a source it did not receive");
  });

  test("duplicate projections and a stale task reply are normalized before interpretation", async () => {
    const requests: Record<string, unknown>[] = [];
    const decision = ordinaryDecision({ bubbleText: "I understand—I'll use the quoted update." });
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          return fakeStream({ status: "completed", output_parsed: decision, output: [] });
        },
      },
    } as never);
    const input = foregroundInput();
    const monitor = {
      followUpId: "monitor-1",
      objective: "Watch for the school update.",
      currentConclusion: "No update yet.",
      endCondition: "The school posts the update.",
      nextCheck: "2026-08-28T20:00:00.000Z",
      why: "The family asked Florence to watch.",
      sourceIds: ["turn-1"],
    };
    const reminder = {
      reminderId: "reminder-1",
      action: "Pack the school bag",
      schedule: {
        kind: "weekly" as const,
        everyWeeks: 1,
        weekdays: [1, 1, 3],
        localTime: "19:00",
        startsOn: "2026-08-31",
      },
      status: "active" as const,
      nextAt: "2026-08-31T19:00:00.000Z",
      lastRunAt: null,
      createdAt: NOW,
    };
    const work = {
      workId: "work-1",
      objective: "Handle the school paperwork.",
      completionCondition: "The school confirms the paperwork was received.",
      candidateIds: ["candidate-1", "candidate-1"],
      responsibleAdultName: null,
      currentProgress: "Reviewing the form.",
      schedule: null,
      briefing: null,
      paused: false,
      status: "active" as const,
      nextAt: null,
      lastRunAt: null,
      lastResult: null,
      createdAt: NOW,
    };
    input.pendingFollowUps = [monitor, { ...monitor }];
    input.visibleReminders = [reminder, { ...reminder }];
    input.visibleFamilyWork = [work, { ...work }];
    input.visibleInterests = [
      {
        interestWorkId: "interest-bad",
        status: "active",
        genericTerms: ["Hari soccer"],
        objective: "Find soccer opportunities for Hari.",
        why: "This stale projection contains a private name.",
      },
      {
        interestWorkId: "interest-good",
        status: "active",
        genericTerms: ["Soccer", "soccer", "children's theater"],
        objective: "Find family-friendly activities.",
        why: "The family asked Florence to keep looking.",
      },
      {
        interestWorkId: "interest-good",
        status: "paused",
        genericTerms: ["soccer"],
        objective: "Duplicate stale projection.",
        why: "This duplicate should not reach interpretation.",
      },
    ];
    input.currentMessage.moveKind = "reply";
    input.currentMessage.replyTo = {
      sourceId: "quoted-update",
      familyWorkId: "stale-work",
      senderName: "Florence",
      text: "I found the revised school form.",
      occurredAt: NOW,
    };

    await expect(reasoner.decide(input, inertReads())).resolves.toEqual(decision);

    const requestContent = (
      requests[0]?.input as Array<{ content?: Array<{ type?: string; text?: string }> }>
    )?.[0]?.content;
    const modelInputText = requestContent?.find((part) => part.type === "input_text")?.text;
    if (!modelInputText) throw new Error("The normalized foreground input was not sent to the model");
    const modelInput = JSON.parse(modelInputText) as typeof input;
    expect(modelInput.pendingFollowUps).toHaveLength(1);
    expect(modelInput.visibleReminders).toHaveLength(1);
    expect(modelInput.visibleReminders[0]?.schedule).toMatchObject({ weekdays: [1, 3] });
    expect(modelInput.visibleFamilyWork).toHaveLength(1);
    expect(modelInput.visibleFamilyWork[0]?.candidateIds).toEqual(["candidate-1"]);
    expect(modelInput.visibleInterests).toEqual([
      expect.objectContaining({
        interestWorkId: "interest-good",
        genericTerms: ["Soccer", "children's theater"],
      }),
    ]);
    expect(modelInput.currentMessage.replyTo).toMatchObject({
      sourceId: "quoted-update",
      familyWorkId: null,
      text: "I found the revised school form.",
    });
  });

  test("consequence-free output bookkeeping and presentation are compacted without a repair turn", async () => {
    const decision = ordinaryDecision();
    decision.conversation.bubbles = [
      { text: "I found it.", delayMs: -100 },
      { text: "I found it.", delayMs: 100 },
      { text: "I’m taking care of the school item now.", delayMs: 9_000 },
      { text: "I’ll bring the result back here.", delayMs: 5.8 },
      { text: "This extra draft bubble is unnecessary.", delayMs: 0 },
    ];
    decision.followUp = {
      operation: "list",
      followUpId: null,
      objective: null,
      currentConclusion: null,
      endCondition: null,
      nextCheck: null,
      why: null,
      sourceIds: ["turn-1", "turn-1"],
    };
    decision.interest = {
      operation: "create",
      interestWorkId: null,
      genericTerms: ["Soccer", "soccer", "children's theater"],
      objective: "Find family-friendly soccer and theater opportunities.",
      why: "The parent wants useful local opportunities.",
      sourceIds: ["turn-1", "turn-1"],
    };
    decision.familyWork = {
      operation: "create",
      workId: null,
      objective: "Take care of the school form.",
      completionCondition: "The school confirms the form was received.",
      responsibleAdultName: null,
      schedule: null,
      briefing: null,
      instruction: null,
      candidateIds: ["candidate-1", "candidate-1"],
    };
    const requests: Record<string, unknown>[] = [];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          return fakeStream({ status: "completed", output_parsed: decision, output: [] });
        },
      },
    } as never);
    const input = foregroundInput();
    input.householdDocket = {
      totalItems: 1,
      items: [
        {
          candidateId: "candidate-1",
          visibility: "private",
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

    const result = await reasoner.decide(input, inertReads());

    expect(requests).toHaveLength(1);
    expect(result.followUp).toMatchObject({ operation: "list", sourceIds: [] });
    expect(result.interest).toMatchObject({
      genericTerms: ["Soccer", "children's theater"],
      sourceIds: ["turn-1"],
    });
    expect(result.familyWork).toMatchObject({ candidateIds: ["candidate-1"] });
    expect(result.conversation.bubbles).toEqual([
      { text: "I found it.", delayMs: 0 },
      { text: "I’m taking care of the school item now.", delayMs: 5_000 },
      {
        text: "I’ll bring the result back here.\n\nThis extra draft bubble is unnecessary.",
        delayMs: 0,
      },
    ]);
  });

  test("a non-delivering family-work control is repaired until it has a visible acknowledgement", async () => {
    const silentList = ordinaryDecision();
    silentList.conversation.bubbles = [];
    silentList.familyWork = {
      operation: "list",
      workId: null,
      objective: null,
      schedule: null,
      instruction: null,
    };
    const repaired = ordinaryDecision({ bubbleText: "Here’s what I’m currently handling for you." });
    repaired.familyWork = { ...silentList.familyWork };
    const requests: Record<string, unknown>[] = [];
    let turn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          turn += 1;
          const output = turn === 1 ? silentList : repaired;
          return fakeStream({
            status: "completed",
            output_parsed: output,
            output: [decisionMessage(`family-work-list-${turn}`, output)],
          });
        },
      },
    } as never);

    await expect(reasoner.decide(foregroundInput(), inertReads())).resolves.toEqual(repaired);
    expect(requests).toHaveLength(2);
    expect(JSON.stringify(requests[1]?.input)).toContain("no visible conversational move");
  });

  test("a no-op family-work update is repaired instead of claiming false success", async () => {
    const noOp = ordinaryDecision({ bubbleText: "Done—I updated it." });
    noOp.familyWork = {
      operation: "update",
      workId: "work-1",
      objective: null,
      completionCondition: null,
      responsibleAdultName: null,
      schedule: null,
      briefing: null,
      instruction: null,
    };
    const repaired = ordinaryDecision({
      bubbleText: "I don’t have a change to apply yet—what should I update?",
    });
    const requests: Record<string, unknown>[] = [];
    let turn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          turn += 1;
          const output = turn === 1 ? noOp : repaired;
          return fakeStream({
            status: "completed",
            output_parsed: output,
            output: [decisionMessage(`family-work-no-op-${turn}`, output)],
          });
        },
      },
    } as never);

    await expect(reasoner.decide(foregroundInput(), inertReads())).resolves.toEqual(repaired);
    expect(requests).toHaveLength(2);
    expect(JSON.stringify(requests[1]?.input)).toContain("must change at least one supplied field");
  });

  test("duplicate approvals collapse only when their authority claim agrees", async () => {
    const exact = ordinaryDecision({ bubbleText: "Yes—I’ll use that sharing review." });
    exact.approvals = [
      { kind: "partner_invitation", adultId: "adult-2", standaloneExplicit: false },
      { kind: "partner_invitation", adultId: "adult-2", standaloneExplicit: false },
      { kind: "partner_invitation", adultId: "adult-2", standaloneExplicit: false },
    ];
    const input = foregroundInput();
    input.pendingPartnerInvitation = {
      adultId: "adult-2",
      firstName: "Jackson",
      maskedPhoneNumber: "••••1234",
      approvalPromptCurrent: true,
      requiresFreshReview: false,
    };
    const exactReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => fakeStream({ status: "completed", output_parsed: exact, output: [] }),
      },
    } as never);

    await expect(exactReasoner.decide(input, inertReads())).resolves.toMatchObject({
      approvals: [{ kind: "partner_invitation", adultId: "adult-2", standaloneExplicit: false }],
    });

    const contradictory = ordinaryDecision({ bubbleText: "Yes—I’ll send it." });
    contradictory.approvals = [
      { kind: "partner_invitation", adultId: "adult-2", standaloneExplicit: true },
      { kind: "partner_invitation", adultId: "adult-2", standaloneExplicit: false },
    ];
    const repaired = ordinaryDecision({ bubbleText: "Yes—I’ll use the review you just approved." });
    repaired.approvals = [{ kind: "partner_invitation", adultId: "adult-2", standaloneExplicit: false }];
    const requests: Record<string, unknown>[] = [];
    let turn = 0;
    const conflictReasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          turn += 1;
          const output = turn === 1 ? contradictory : repaired;
          return fakeStream({
            status: "completed",
            output_parsed: output,
            output: [decisionMessage(`approval-authority-${turn}`, output)],
          });
        },
      },
    } as never);

    await expect(conflictReasoner.decide(input, inertReads())).resolves.toEqual(repaired);
    expect(requests).toHaveLength(2);
    expect(JSON.stringify(requests[1]?.input)).toContain(
      "disagree about whether the parent explicitly authorized",
    );
  });

  test("repairs an invalid ordinary partner-planning reply before it reaches the parent fallback", async () => {
    const invalid = ordinaryDecision();
    invalid.conversation.bubbles = [];
    const repaired = ordinaryDecision({
      bubbleText: "Absolutely—what’s your partner’s first name?",
    });
    const requests: Record<string, unknown>[] = [];
    let turn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          turn += 1;
          const decision = turn === 1 ? invalid : repaired;
          return fakeStream({
            status: "completed",
            output_parsed: decision,
            output: [decisionMessage(`partner-plan-${turn}`, decision)],
          });
        },
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text = "Let’s add my partner";
    input.currentMessage.authoredText = input.currentMessage.text;
    input.household.adultNames = ["Jackson"];
    input.household.familyProfile = JSON.stringify({
      florenceCalendarAudience: "owner_private",
      florenceCalendarReady: true,
      setupAttention: null,
      members: [
        {
          id: input.currentAdultId,
          kind: "adult",
          status: "verified",
          adultSlot: 1,
        },
      ],
    });

    await expect(reasoner.decide(input, inertReads())).resolves.toMatchObject({
      conversation: { bubbles: [{ text: expect.stringContaining("first name") }] },
      secondAdultPlan: null,
    });
    expect(requests).toHaveLength(2);
    expect(JSON.stringify(requests[1]?.input)).toContain("foreground_decision_repair");
    expect(JSON.stringify(requests[1]?.input)).toContain("no visible conversational move");
  });

  test("repairs a host-rejected draft inside the same natural-language turn", async () => {
    const rejected = ordinaryDecision({
      bubbleText: "I’ll put that on someone outside this family.",
    });
    const repaired = ordinaryDecision({
      bubbleText: "Absolutely—what part of the school week should we tackle first?",
    });
    const requests: Record<string, unknown>[] = [];
    const admitted: string[] = [];
    let turn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          turn += 1;
          const decision = turn === 1 ? rejected : repaired;
          return fakeStream({
            status: "completed",
            output_parsed: decision,
            output: [decisionMessage(`host-admission-${turn}`, decision)],
          });
        },
        parse: async () => ({
          status: "completed",
          output_parsed: { verdict: "accept", reason: null },
          output: [],
        }),
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text = "Can you help me sort out the school week?";
    input.currentMessage.authoredText = input.currentMessage.text;

    const result = await reasoner.decide(input, inertReads(), undefined, {
      admitDecision: (decision) => {
        admitted.push(decision.conversation.bubbles[0]?.text ?? "");
        if (admitted.length === 1) {
          throw new FlorenceReasonerError(
            "invalid_output",
            "Only household family work can assign responsibility to a family member",
          );
        }
      },
    });

    expect(result).toEqual(repaired);
    expect(admitted).toEqual([
      "I’ll put that on someone outside this family.",
      "Absolutely—what part of the school week should we tackle first?",
    ]);
    expect(requests).toHaveLength(2);
    expect(JSON.stringify(requests[1]?.input)).toContain("foreground_decision_repair");
    expect(JSON.stringify(requests[1]?.input)).toContain(
      "Only household family work can assign responsibility to a family member",
    );
    expect(JSON.stringify(requests[1]?.input)).toContain("I’ll put that on someone outside this family.");
    expect(JSON.stringify(requests[1]?.input)).toContain("Never expose validator language");
  });

  test("recovers a mixed draft failure without blaming the parent", async () => {
    const malformedDraft = ordinaryDecision();
    malformedDraft.conversation.bubbles = [];
    const firstHostRejectedDraft = ordinaryDecision({
      bubbleText: "I’ll put that on someone outside this family.",
    });
    const recoveredConversation = ordinaryDecision({
      bubbleText: "Absolutely—what part of the school week should we tackle first?",
    });
    recoveredConversation.policy = { retain: false, schedule: false };
    const requests: Record<string, unknown>[] = [];
    let turn = 0;
    let admissions = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          turn += 1;
          const decision =
            turn === 1 ? malformedDraft : turn === 2 ? firstHostRejectedDraft : recoveredConversation;
          return fakeStream({
            status: "completed",
            output_parsed: decision,
            output: [decisionMessage(`mixed-admission-${turn}`, decision)],
          });
        },
        parse: async () => ({
          status: "completed",
          output_parsed: { verdict: "accept", reason: null },
          output: [],
        }),
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text = "Can you help me sort out the school week?";
    input.currentMessage.authoredText = input.currentMessage.text;

    await expect(
      reasoner.decide(input, inertReads(), undefined, {
        admitDecision: () => {
          admissions += 1;
          if (admissions === 1) {
            throw new FlorenceReasonerError(
              "invalid_output",
              "Only household family work can assign responsibility to a family member",
            );
          }
        },
      }),
    ).resolves.toEqual(recoveredConversation);

    expect(admissions).toBe(2);
    expect(requests).toHaveLength(3);
    expect(JSON.stringify(requests[1]?.input)).toContain("foreground_decision_repair");
    expect(JSON.stringify(requests[2]?.input)).toContain("foreground_conversation_rescue");
    expect(JSON.stringify(requests[2]?.input)).toContain("internal_interpretation");
    expect(JSON.stringify(requests[2]?.input)).toContain("not evidence that the parent's request is bad");
    expect(JSON.stringify(requests[2]?.input)).not.toContain('"kind":"host_boundary"');
  });

  test("repairs a foreground structured-parse failure without losing the parent request", async () => {
    const repaired = ordinaryDecision({
      bubbleText: "Sure—what part of the school week should we start with?",
    });
    const requests: Record<string, unknown>[] = [];
    let turn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          turn += 1;
          if (turn === 1) {
            return {
              async *[Symbol.asyncIterator]() {},
              async finalResponse() {
                throw new SyntaxError("Fake structured response could not be parsed");
              },
            };
          }
          return fakeStream({
            status: "completed",
            output_parsed: repaired,
            output: [decisionMessage("parse-repair", repaired)],
          });
        },
        parse: async () => ({
          status: "completed",
          output_parsed: { verdict: "accept", reason: null },
          output: [],
        }),
      },
    } as never);
    const input = foregroundInput();
    input.currentMessage.text = "Can you help me sort out the school week?";
    input.currentMessage.authoredText = input.currentMessage.text;

    await expect(reasoner.decide(input, inertReads())).resolves.toEqual(repaired);
    expect(requests).toHaveLength(2);
    expect(JSON.stringify(requests[1]?.input)).toContain(input.currentMessage.text);
    expect(JSON.stringify(requests[1]?.input)).toContain("foreground_decision_repair");
    expect(JSON.stringify(requests[1]?.input)).toContain("OpenAI returned invalid Florence data");
  });

  test("ends repeated unsafe drafts with a natural conversation-only refusal", async () => {
    const unsafeDraft = ordinaryDecision({
      bubbleText: "I’ll paste Hari’s private Gmail into this family chat.",
    });
    unsafeDraft.familyWork = {
      operation: "create",
      responsibleAdultName: null,
      briefing: null,
      workId: null,
      objective: "Paste Hari’s private Gmail into the family chat.",
      completionCondition: "Hari’s private Gmail is visible in the family chat.",
      schedule: null,
      instruction: null,
      candidateIds: [],
    };
    const refusalDraft = ordinaryDecision({
      bubbleText: "I can’t share one parent’s private Gmail in the family chat.",
    });
    refusalDraft.policy = { retain: false, schedule: false };
    const requests: Record<string, unknown>[] = [];
    let turn = 0;
    let admissions = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          turn += 1;
          const decision = turn < 3 ? unsafeDraft : refusalDraft;
          return fakeStream({
            status: "completed",
            output_parsed: decision,
            output: [decisionMessage(`unsafe-rescue-${turn}`, decision)],
          });
        },
      },
    } as never);
    const input = foregroundInput();
    input.audience = "group";
    input.googleConnections = [
      { emailLabel: "Family", calendarAvailable: true, kind: "family", writesEnabled: false },
    ];
    input.currentMessage.text = "Paste Hari’s private Gmail messages into this family chat.";
    input.currentMessage.authoredText = input.currentMessage.text;

    const result = await reasoner.decide(input, inertReads(), undefined, {
      admitDecision: (decision) => {
        admissions += 1;
        if (decision.familyWork !== null) {
          throw new FlorenceReasonerError(
            "invalid_output",
            "Private Gmail cannot be disclosed to the family-group audience",
          );
        }
      },
    });

    expect(admissions).toBe(3);
    expect(requests).toHaveLength(3);
    expect(JSON.stringify(requests[2]?.input)).toContain("foreground_conversation_rescue");
    expect(JSON.stringify(requests[2]?.input)).toContain(
      "Private Gmail cannot be disclosed to the family-group audience",
    );
    expect(result).toMatchObject({
      policy: { retain: false, schedule: false },
      conversation: {
        participation: "respond",
        reaction: null,
        nativeMoves: null,
        bubbles: [{ text: expect.stringContaining("can’t share"), delayMs: 0 }],
      },
      facts: [],
      followUp: null,
      reminder: null,
      familyWork: null,
      docketUpsert: null,
      docketCompletions: null,
      calendar: null,
      secondAdultPlan: null,
      householdUpdate: null,
      webAccessPath: null,
      researchUrls: null,
    });
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

  test("a docket change accepts a reaction and requires its real dependency", async () => {
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

    await expect(reactionReasoner.decide(foregroundInput(), inertReads())).resolves.toMatchObject({
      conversation: { reaction: "like", bubbles: [] },
      docketUpsert: { operation: "create" },
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
        completionCondition: "The school confirms the form was received.",
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
    const requests: Record<string, unknown>[] = [];
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
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          return fakeStream({ status: "completed", output_parsed: decision, output: [] });
        },
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
        completionCondition: "The best morning camp option is identified.",
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
        completionCondition: "The best afternoon camp option is identified.",
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
    expect(JSON.stringify(requests[0]?.input)).toContain("The best afternoon camp option is identified.");
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
