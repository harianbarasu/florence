import type { FamilyWorkStateV1 } from "@florence/database";
import {
  GoogleCalendarTransientError,
  GoogleWorkspaceError,
  type GoogleWorkspaceOperation,
  type GoogleWorkspaceResult,
} from "@florence/google";
import { describe, expect, test } from "vitest";
import { isContextOverflowError } from "./agent-loop.js";
import { FlorenceReasoner, FlorenceReasonerError } from "./reasoner.js";
import {
  completionOutputDigest,
  conversationalGmailSource,
  defaultFamilyWorkCompletionReview,
  fakeStream,
  familyWorkOrigin,
  familyWorkResultMessage,
  foregroundInput,
  functionCall,
  functionOutputEnvelopes,
  inertReads,
  NOW,
  ordinaryDecision,
} from "./reasoner-tool-loops.test-kit.js";

describe("Florence reasoner capability cutover", () => {
  test("verified useful work searches before retaining reusable completion memory", async () => {
    const modelRequests: Record<string, unknown>[] = [];
    const completionMemory = {
      operation: "retain" as const,
      changes: [
        {
          operation: "remember" as const,
          factId: null,
          statement: "The family's weeknight noodle recipe uses sesame oil, soy sauce, and rice vinegar.",
          visibility: "household" as const,
          memory: {
            memoryKind: "artifact" as const,
            artifactKind: "recipe" as const,
            title: "Weeknight noodles",
            details:
              "Mix sesame oil, soy sauce, and rice vinegar for the sauce, then toss with cooked noodles.",
            tags: ["weeknight", "noodles"],
          },
          sourceIds: ["source-adult-1"],
          expectedUpdatedAt: null,
        },
      ],
    };
    let taskTurn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          const review = defaultFamilyWorkCompletionReview(request);
          if (review) return review;
          modelRequests.push(request);
          if (taskTurn++ === 0) {
            return {
              status: "completed",
              output_parsed: null,
              output: [
                functionCall("completion-memory-search", "search_vault", {
                  query: "family weeknight noodle recipe",
                  cursor: null,
                }),
              ],
            };
          }
          return {
            status: "completed",
            output_parsed: {
              outcome: "succeeded",
              text: "Weeknight noodles: toss cooked noodles with sesame oil, soy sauce, and rice vinegar.",
              resumeAt: null,
              progressText: null,
              completionMemory,
            },
            output: [],
          };
        },
      },
    } as never);
    const state: FamilyWorkStateV1 = {
      kind: "family_work_v1",
      version: 1,
      generation: 1,
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

    const result = await reasoner.continueFamilyWork(
      {
        workId: "family-work-recipe",
        scheduledOccurrence: null,
        objective: "Turn our noodle notes into one reusable recipe.",
        visibility: "household",
        ownerAdultId: null,
        origin: familyWorkOrigin("Please turn the noodle notes into a reusable recipe."),
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
      {
        async searchVault({ query }) {
          return {
            query,
            results: [],
            total: 0,
            complete: true,
            nextCursor: null,
          };
        },
      },
    );

    expect(result).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      completionMemory,
    });
    expect(String(modelRequests[0]?.instructions)).toContain("selective completionMemory decision");
    expect(String(modelRequests[0]?.instructions)).toContain("search the Vault");
    expect(JSON.stringify(modelRequests[0]?.text)).toContain('"completionMemory"');
    expect(functionOutputEnvelopes(modelRequests[1])).toContainEqual(
      expect.objectContaining({ callId: "completion-memory-search", outcome: "succeeded" }),
    );
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
          const review = defaultFamilyWorkCompletionReview(request);
          if (review) return review;
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
    const completionReviewRequests: Record<string, unknown>[] = [];
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
          if (JSON.stringify(request.text).includes("florence_family_work_completion_review")) {
            completionReviewRequests.push(request);
            throw new Error("The model provider failed again after compaction");
          }
          continuationRequests.push(request);
          modelAttempt += 1;
          if (modelAttempt === 1) {
            throw new Error("Your input exceeds the context window of this model");
          }
          return {
            status: "completed",
            output_parsed: {
              outcome: "succeeded",
              text: "The comparison is complete.",
              resumeAt: null,
              progressText: null,
              selectedImageAssetIds: [],
              selectedFileAssetIds: [],
              docket: null,
            },
            output: [],
          };
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
      output: JSON.stringify({
        outcome: "succeeded",
        output: { observation: "recent evidence", details: "y".repeat(4_000) },
        error: null,
      }),
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
      completionEvidence: [
        {
          callId: "older-read",
          capabilityName: "read_public_page",
          arguments: {
            url: "https://example.com/older",
            offset: 0,
            contentFingerprint: null,
          },
          recordedAt: NOW,
          outputDigest: completionOutputDigest({
            observation: "older evidence",
            details: "x".repeat(60_000),
          }),
          facts: [],
        },
        {
          callId: "recent-read",
          capabilityName: "read_public_page",
          arguments: {
            url: "https://example.com/recent",
            offset: 0,
            contentFingerprint: null,
          },
          recordedAt: NOW,
          outputDigest: completionOutputDigest({
            observation: "recent evidence",
            details: "y".repeat(4_000),
          }),
          facts: [],
        },
      ],
      continuationItems: [
        functionCall("older-read", "read_public_page", {
          url: "https://example.com/older",
          offset: 0,
          contentFingerprint: null,
        }),
        {
          type: "function_call_output",
          call_id: "older-read",
          output: JSON.stringify({
            outcome: "succeeded",
            output: { observation: "older evidence", details: "x".repeat(60_000) },
            error: null,
          }),
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
    expect(completionReviewRequests).toHaveLength(1);
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
    const completionReviewInput = completionReviewRequests[0]?.input as
      | { content?: { text?: string }[] }[]
      | undefined;
    const completionReviewPayload = JSON.parse(completionReviewInput?.[0]?.content?.[0]?.text ?? "{}") as {
      successfulCapabilityResults?: { callId?: string; output?: unknown }[];
    };
    expect(completionReviewPayload.successfulCapabilityResults).toEqual([
      expect.objectContaining({
        callId: "recent-read",
        output: { observation: "recent evidence", details: "y".repeat(4_000) },
      }),
    ]);
    expect(failure).toBeInstanceOf(FlorenceReasonerError);
    if (!(failure instanceof FlorenceReasonerError)) throw new Error("Expected a reasoner failure");
    expect(failure.familyWorkCheckpoint).toMatchObject({
      generation: 3,
      claim: { claimId: "claim-overflow" },
      completionEvidence: [expect.objectContaining({ callId: "recent-read" })],
      continuationItems: [
        expect.objectContaining({ type: "message", role: "user" }),
        expect.objectContaining({ type: "function_call", call_id: recentCall.call_id }),
        expect.objectContaining({ type: "function_call_output", call_id: recentCall.call_id }),
      ],
    });
    expect(failure.familyWorkCheckpoint?.completionEvidence).toHaveLength(1);
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
        parse(request: Record<string, unknown>) {
          const review = defaultFamilyWorkCompletionReview(request);
          if (review) return review;
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
          const review = defaultFamilyWorkCompletionReview(request);
          if (review) return review;
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

  test("durable work rejects premature success, replans inline, and retains exact completion evidence", async () => {
    const completionCondition =
      "Tomorrow's School Calendar schedule is established from a successful Calendar-window read.";
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
    const reviewRequests: Record<string, unknown>[] = [];
    const prematureResult = {
      outcome: "succeeded",
      text: "Back-to-school night is tomorrow from 4 to 6 PM.",
    };
    const verifiedResult = {
      outcome: "succeeded",
      text: "Back-to-school night is tomorrow from 4 to 6 PM.",
    };
    const modelResponses = [
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("list-work-calendars", "list_calendars", {})],
      },
      {
        status: "completed",
        output_parsed: prematureResult,
        output: [familyWorkResultMessage("premature-calendar-success", prematureResult)],
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
        output_parsed: verifiedResult,
        output: [familyWorkResultMessage("verified-calendar-success", verifiedResult)],
      },
    ];
    const reviewResponses = [
      {
        status: "completed",
        output_parsed: {
          verdict: "continue",
          reason: "The Calendar was listed, but tomorrow's event window was not read.",
          condition: "Establish tomorrow's School Calendar schedule from a successful Calendar-window read.",
          basisKind: null,
          summary: null,
          evidenceCallIds: [],
        },
        output: [],
      },
      {
        status: "completed",
        output_parsed: {
          verdict: "verified",
          reason: null,
          condition: completionCondition,
          basisKind: "capability_evidence",
          summary: "The successful Calendar-window read confirms Back-to-school night from 4 to 6 PM.",
          evidenceCallIds: ["read-work-calendar-retry"],
          evidenceSelections: [
            {
              callId: "read-work-calendar-retry",
              pointers: ["/events/0/eventRef", "/events/0/startsAt", "/events/0/endsAt"],
            },
          ],
        },
        output: [],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          if (JSON.stringify(request.text).includes("florence_family_work_completion_review")) {
            reviewRequests.push(request);
            const response = reviewResponses.shift();
            if (!response) throw new Error("Unexpected durable Calendar completion review");
            return response;
          }
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
      completionCondition,
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
    expect(modelRequests).toHaveLength(5);
    expect(reviewRequests).toHaveLength(2);
    expect(JSON.stringify(modelRequests[1]?.input)).toContain(calendarRef);
    expect(JSON.stringify(modelRequests[2]?.input)).toContain("premature-calendar-success");
    expect(JSON.stringify(modelRequests[2]?.input)).toContain("family_work_completion_repair");
    expect(JSON.stringify(modelRequests[2]?.input)).toContain("event window was not read");
    expect(JSON.stringify(modelRequests[3]?.input)).toContain('\\"retryable\\":true');
    expect(JSON.stringify(reviewRequests[1]?.input)).toContain("read-work-calendar-retry");
    expect(JSON.stringify(reviewRequests[1]?.input)).toContain("event-school-night");
    expect(JSON.stringify(reviewRequests[0]?.input)).toContain(completionCondition);
    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("Back-to-school night"),
      state: {
        terminal: {
          completionBasis: {
            condition: completionCondition,
            summary: "The successful Calendar-window read confirms Back-to-school night from 4 to 6 PM.",
            evidenceCallIds: ["read-work-calendar-retry"],
          },
        },
        completionEvidence: [
          expect.objectContaining({
            callId: "read-work-calendar-retry",
            capabilityName: "read_calendar_window",
            arguments: calendarWindowArguments,
            recordedAt: NOW,
          }),
        ],
      },
    });
    if (terminal.kind !== "terminal") throw new Error("Durable Calendar work did not finish");
    const confirmed = functionOutputEnvelopes(modelRequests[4]).find(
      (entry) => entry.callId === "read-work-calendar-retry",
    );
    expect(terminal.state.completionEvidence?.[0]?.outputDigest).toBe(
      completionOutputDigest(confirmed?.output),
    );
    expect(terminal.state.completionEvidence?.[0]).not.toHaveProperty("output");
  });

  test("persisted completion rejection closes unchanged success with one truthful disposition", async () => {
    const modelRequests: Record<string, unknown>[] = [];
    const reviewRequests: Record<string, unknown>[] = [];
    const dispositionRequests: Record<string, unknown>[] = [];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          const format = JSON.stringify(request.text);
          if (format.includes("florence_family_work_completion_review")) {
            reviewRequests.push(request);
            return {
              status: "completed",
              output_parsed: {
                verdict: "continue",
                reason: "No successful capability result establishes that the requested outcome happened.",
                condition: "The requested family outcome is established.",
                basisKind: null,
                summary: null,
                evidenceCallIds: [],
              },
              output: [],
            };
          }
          if (format.includes("florence_family_work_unverified_disposition")) {
            dispositionRequests.push(request);
            return {
              status: "completed",
              output_parsed: {
                outcome: "partial",
                text: "I found the relevant details, but I couldn't verify that the outside action was completed.",
                resumeAt: null,
                progressText: null,
                selectedImageAssetIds: [],
                selectedFileAssetIds: [],
                docket: {
                  owner: "Florence",
                  nextAction: "Verify the outside action through an available source.",
                  waitingOn: "A source that confirms the resulting state.",
                  needsAnswer: false,
                  completionCondition: "The requested family outcome is established.",
                },
              },
              output: [],
            };
          }
          modelRequests.push(request);
          return {
            status: "completed",
            output_parsed: {
              outcome: "succeeded",
              text: "Everything is done.",
              resumeAt: null,
              progressText: null,
              selectedImageAssetIds: [],
              selectedFileAssetIds: [],
              docket: null,
            },
            output: [],
          };
        },
      },
    } as never);
    const state: FamilyWorkStateV1 = {
      kind: "family_work_v1",
      version: 1,
      generation: 2,
      completionCondition: "The requested family outcome is established.",
      phase: "ready",
      claim: null,
      activePhoneCall: null,
      activeTextMessage: null,
      pendingParticipantRequest: null,
      browserSession: null,
      completionEvidence: [],
      completionRejection: {
        condition: "The requested family outcome is established.",
        reason: "The earlier success claim had no confirming result.",
      },
      continuationItems: [
        {
          type: "message",
          role: "user",
          content: [
            {
              type: "input_text",
              text: "The task history before this point was compacted into the following summary:\n\n<summary>\nThe previous completion claim was unverified.\n</summary>",
            },
          ],
        },
      ],
      pendingCall: null,
      steering: [],
      progressRevision: 0,
      terminal: null,
    };

    const result = await reasoner.continueFamilyWork(
      {
        workId: "family-work-fixed-point",
        scheduledOccurrence: null,
        objective: "Take care of the requested family outcome.",
        visibility: "household",
        ownerAdultId: null,
        origin: familyWorkOrigin("Take care of the requested family outcome."),
        household: {
          householdId: "household-1",
          familyLabel: "Test family",
          timeZone: "America/Los_Angeles",
          postalCode: "90045",
          adults: [{ adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" }],
          children: [],
        },
        state: JSON.parse(JSON.stringify(state)) as FamilyWorkStateV1,
        currentTime: NOW,
      },
      {},
    );

    expect(modelRequests).toHaveLength(1);
    expect(reviewRequests).toHaveLength(1);
    expect(dispositionRequests).toHaveLength(1);
    expect(JSON.stringify(modelRequests[0]?.input)).toContain("completionRejection");
    expect(JSON.stringify(dispositionRequests[0]?.text)).toContain(
      "florence_family_work_unverified_disposition",
    );
    const dispositionOutcomes = (
      dispositionRequests[0]?.text as
        | { format?: { schema?: { properties?: { outcome?: { enum?: unknown } } } } }
        | undefined
    )?.format?.schema?.properties?.outcome?.enum;
    expect(dispositionOutcomes).toEqual(["partial", "waiting", "failed"]);
    expect(dispositionRequests[0]?.tools).toEqual([]);
    expect(
      JSON.stringify(
        [...modelRequests, ...reviewRequests, ...dispositionRequests].map((request) => request.input),
      ),
    ).not.toContain("family_work_completion_repair");
    expect(result).toMatchObject({
      kind: "terminal",
      outcome: "partial",
      state: {
        phase: "terminal",
        completionEvidence: [],
        completionRejection: null,
      },
    });
    if (result.kind !== "terminal") throw new Error("Unverified fixed point did not close");
    expect(result.state.terminal?.completionBasis).toBeUndefined();
    expect(result).not.toHaveProperty("resumeAt");
    expect(result).not.toHaveProperty("nextCheckDelayMs");
  });

  test("large Workspace evidence finishes with one exact digest receipt instead of a copied payload", async () => {
    const uniqueClause = "Parents must submit the signed field-trip form by Friday at 3 PM.";
    const workspaceResult: GoogleWorkspaceResult = {
      operation: "gmail_search",
      result: {
        messages: [
          {
            messageId: "school-policy-message",
            subject: "Field trip paperwork",
            relevantClause: uniqueClause,
            providerPayload: "x".repeat(300 * 1024),
          },
        ],
      },
    };
    const modelResponses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("large-school-search", "gmail_work", {
            operation: "gmail_search",
            query: "field trip paperwork",
            limit: 20,
            messageId: null,
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
        output_parsed: {
          outcome: "succeeded",
          text: uniqueClause,
          resumeAt: null,
          progressText: null,
          selectedImageAssetIds: [],
          selectedFileAssetIds: [],
          docket: null,
        },
        output: [],
      },
    ];
    const reviewRequests: Record<string, unknown>[] = [];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          if (JSON.stringify(request.text).includes("florence_family_work_completion_review")) {
            reviewRequests.push(request);
            return {
              status: "completed",
              output_parsed: {
                verdict: "verified",
                reason: null,
                condition:
                  "The matching school messages were read and their actionable deadline was reported.",
                basisKind: "capability_evidence",
                summary: "The Gmail search result contains the field-trip form deadline.",
                evidenceCallIds: ["large-school-search"],
                evidenceSelections: [
                  {
                    callId: "large-school-search",
                    pointers: ["/result/messages/0/relevantClause"],
                  },
                ],
              },
              output: [],
            };
          }
          const response = modelResponses.shift();
          if (!response) throw new Error("Unexpected large-evidence model turn");
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
      completionEvidence: [],
      completionRejection: null,
      continuationItems: [],
      pendingCall: null,
      steering: [],
      progressRevision: 0,
      terminal: null,
    };

    const result = await reasoner.continueFamilyWork(
      {
        workId: "family-work-large-workspace-evidence",
        scheduledOccurrence: null,
        objective: "Compile the actionable field-trip requirements from the school messages.",
        visibility: "private",
        ownerAdultId: "adult-1",
        origin: familyWorkOrigin("Compile the actionable field-trip requirements from the school messages."),
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
            kind: "personal",
            writesEnabled: false,
          },
        ],
        state,
        currentTime: NOW,
      },
      {
        async runGoogleWorkspace() {
          return workspaceResult;
        },
      },
    );

    expect(reviewRequests).toHaveLength(1);
    const reviewInput = reviewRequests[0]?.input as
      | { content?: { type?: string; text?: string }[] }[]
      | undefined;
    const reviewPayload = JSON.parse(reviewInput?.[0]?.content?.[0]?.text ?? "{}") as {
      successfulCapabilityResults?: { output?: unknown }[];
    };
    expect(reviewPayload.successfulCapabilityResults?.[0]?.output).toEqual(workspaceResult);
    expect(result).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      state: {
        terminal: { completionBasis: { evidenceCallIds: ["large-school-search"] } },
        completionEvidence: [
          expect.objectContaining({
            callId: "large-school-search",
            outputDigest: completionOutputDigest(workspaceResult),
            facts: [
              {
                pointer: "/result/messages/0/relevantClause",
                value: uniqueClause,
              },
            ],
          }),
        ],
      },
    });
    if (result.kind !== "terminal") throw new Error("Large Workspace evidence did not finish");
    expect(result.completionEvidenceOutputs).toEqual([
      { callId: "large-school-search", output: workspaceResult },
    ]);
    expect(result.state.completionEvidence?.[0]).not.toHaveProperty("output");
    expect(JSON.stringify(result.state)).not.toContain("x".repeat(10_000));
    expect(Buffer.byteLength(JSON.stringify(result.state), "utf8")).toBeLessThan(240 * 1024);
  });
});
