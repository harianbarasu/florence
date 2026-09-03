import { access, readFile, stat } from "node:fs/promises";
import { type FamilyWorkStateV1, steerFamilyWorkState } from "@florence/database";
import type { GoogleWorkspaceOperation, GoogleWorkspaceResult } from "@florence/google";
import { describe, expect, test } from "vitest";
import {
  BrowserbaseBrowserClient,
  type FlorenceBrowserOperation,
  KernelBrowserClient,
  kernelProfileName,
} from "./browser.js";
import type { FlorenceMapsRequest, FlorenceMapsResult } from "./maps.js";
import { FlorenceReasoner } from "./reasoner.js";
import {
  browserArguments,
  browserObservation,
  conversationalGmailSource,
  defaultFamilyWorkCompletionReview,
  familyWorkOrigin,
  familyWorkResultMessage,
  functionCall,
  NOW,
  telephonyResult,
} from "./reasoner-tool-loops.test-kit.js";
import type { FlorenceTelephonyOperation, FlorenceTelephonyResult } from "./telephony.js";

describe("Florence reasoner capability cutover", () => {
  test("durable household work calls a business, saves the appointment, and reports the transcript-backed outcome", async () => {
    const calendarRef = "calendar-family";
    const completionCondition =
      "Westside Pediatric Dental has confirmed Violet’s cleaning on Tuesday or Wednesday after 3 PM within the family’s stated bounds, and the matching event is committed to the Florence Calendar.";
    const modelRequests: Record<string, unknown>[] = [];
    const completionReviewRequests: Record<string, unknown>[] = [];
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
          functionCall("find-dentist", "maps_search", {
            query: "Westside Pediatric Dental near 90045",
            limit: 5,
          }),
        ],
      },
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
            record: false,
            summaryPrompt:
              "State whether an appointment was booked and give its exact local date, start and end time or duration, location, and confirmation reference.",
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
        output_parsed: null,
        output: [functionCall("list-family-calendar", "list_calendars", {})],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("read-family-calendar", "read_calendar_window", {
            timeMin: "2026-09-02T07:00:00.000Z",
            timeMax: "2026-09-03T07:00:00.000Z",
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
          functionCall("save-dentist-appointment", "family_calendar_work", {
            operation: "create",
            event: {
              intervalKind: "timed",
              title: "Violet dental cleaning",
              startsAt: "2026-09-02T22:30:00.000Z",
              endsAt: "2026-09-02T23:15:00.000Z",
              timeZone: "America/Los_Angeles",
              location: "1234 Sepulveda Boulevard, Los Angeles, CA 90045",
            },
            target: null,
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: {
          outcome: "succeeded",
          text: "Violet’s cleaning is confirmed for Wednesday, September 2, from 3:30 to 4:15 PM at Westside Pediatric Dental. Confirmation DENT-42 is on the Florence Calendar.",
        },
        output: [],
      },
    ];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(request: Record<string, unknown>) {
          if (JSON.stringify(request.text).includes("florence_family_work_completion_review")) {
            completionReviewRequests.push(request);
            return {
              status: "completed",
              output_parsed: {
                verdict: "verified",
                reason: null,
                condition: completionCondition,
                basisKind: "capability_evidence",
                summary:
                  "The completed phone call confirms the booking, and the Calendar receipt confirms the matching event was saved.",
                evidenceCallIds: ["check-dentist-call", "save-dentist-appointment"],
                evidenceSelections: [
                  {
                    callId: "check-dentist-call",
                    pointers: ["/summary", "/disposition", "/providerStatus"],
                  },
                  {
                    callId: "save-dentist-appointment",
                    pointers: ["/status", "/providerEventId"],
                  },
                ],
              },
              output: [],
            };
          }
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
      objective:
        "Call Westside Pediatric Dental near 90045 and arrange Violet’s cleaning Tuesday or Wednesday after 3 PM.",
      visibility: "household" as const,
      ownerAdultId: null,
      initiatingAdultId: "adult-jackson",
      origin: familyWorkOrigin(
        "Call Westside Pediatric Dental near 90045 and arrange Violet’s cleaning Tuesday or Wednesday after 3 PM.",
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
      googleConnections: [
        {
          emailLabel: "Florence Calendar",
          calendarAvailable: true,
          kind: "family" as const,
          writesEnabled: true,
        },
      ],
      state: {
        kind: "family_work_v1" as const,
        version: 1 as const,
        generation: 0,
        acknowledgementText: "I’ll call Westside Pediatric Dental and work within those times.",
        completionCondition,
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
    const mapOperations: FlorenceMapsRequest[] = [];
    const calendarOperations: unknown[] = [];
    const calendarReads: unknown[] = [];
    const reads = {
      telephonyProviders: ["bland"] as const,
      async runMaps(operation: FlorenceMapsRequest): Promise<FlorenceMapsResult> {
        mapOperations.push(operation);
        return {
          operation: "search",
          query: "Westside Pediatric Dental near 90045",
          count: 1,
          results: [
            {
              name: "Westside Pediatric Dental",
              displayName: "Westside Pediatric Dental, Los Angeles, California",
              address: "1234 Sepulveda Boulevard, Los Angeles, CA 90045",
              lat: 33.959,
              lon: -118.396,
              type: "dentist",
              category: "amenity",
              osmType: "node",
              osmId: "123456789",
              boundingBox: null,
              importance: 0.7,
              mapsUrl: "https://www.openstreetmap.org/node/123456789",
              phone: "+13105550144",
              website: "https://westside-pediatric-dental.example/",
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
            summary:
              "Westside Pediatric Dental booked Violet’s cleaning for Wednesday, September 2, from 3:30 to 4:15 PM at 1234 Sepulveda Boulevard. Confirmation DENT-42.",
            disposition: "booked",
            transcript:
              "Office: Violet is confirmed Wednesday, September 2, from 3:30 to 4:15 PM at 1234 Sepulveda Boulevard. The confirmation is DENT-42. Florence: Thank you.",
          });
        }
        throw new Error(`Unexpected telephony operation ${operation.kind}`);
      },
      async listCalendars() {
        return {
          status: "complete" as const,
          calendars: [
            {
              calendarRef,
              label: "Florence Calendar",
              timeZone: "America/Los_Angeles",
              primary: null,
              accessRole: null,
              eventCoverage: "readable" as const,
            },
          ],
          totalCalendarCount: 1,
          nextCursor: null,
        };
      },
      async readCalendarWindow(operation: unknown) {
        calendarReads.push(operation);
        return {
          status: "complete" as const,
          calendars: [
            {
              calendarRef,
              label: "Florence Calendar",
              timeZone: "America/Los_Angeles",
              primary: null,
              accessRole: null,
              status: "complete" as const,
              eventCount: 0,
            },
          ],
          totalCalendarCount: 1,
          calendarCoverage: {
            complete: true,
            observedCalendarCount: 1,
            completeCalendarCount: 1,
            missingCalendarCount: 0,
            unavailableCalendarCount: 0,
            digest: "2".repeat(64),
          },
          events: [],
          totalEventCount: 0,
          nextCursor: null,
        };
      },
      async runFamilyCalendarWork(operation: { operation: string }) {
        calendarOperations.push(operation);
        return {
          status: "committed" as const,
          operation: "create" as const,
          providerEventId: "provider-dentist",
          providerRevision: "revision-1",
        };
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
    expect(started.nextCheckDelayMs).toBe(15_000);
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
    expect(JSON.stringify(completed.state.continuationItems)).toContain("3:30 to 4:15 PM");

    const calendarPlanned = await reasoner.continueFamilyWork({ ...input, state: completed.state }, reads);
    if (calendarPlanned.kind !== "continue") throw new Error("Dentist calendar write was not planned");
    expect(calendarPlanned.state).toMatchObject({
      phase: "tool_pending",
      pendingCall: { name: "family_calendar_work" },
    });
    const calendarSettled = await reasoner.continueFamilyWork(
      { ...input, state: calendarPlanned.state },
      reads,
    );
    if (calendarSettled.kind !== "continue") throw new Error("Dentist calendar write did not settle");

    const terminal = await reasoner.continueFamilyWork({ ...input, state: calendarSettled.state }, reads);
    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("Florence Calendar"),
      state: {
        terminal: {
          completionBasis: {
            condition: completionCondition,
            evidenceCallIds: ["check-dentist-call", "save-dentist-appointment"],
          },
        },
        completionEvidence: [
          expect.objectContaining({ callId: "check-dentist-call", capabilityName: "phone_agent_call" }),
          expect.objectContaining({
            callId: "save-dentist-appointment",
            capabilityName: "family_calendar_work",
          }),
        ],
      },
    });
    expect(completionReviewRequests).toHaveLength(1);
    const completionReviewInput = completionReviewRequests[0]?.input as
      | Array<{ content?: Array<{ type?: string; text?: string }> }>
      | undefined;
    const completionReviewText =
      completionReviewInput?.[0]?.content?.find((part) => part.type === "input_text")?.text ?? "{}";
    const completionReviewPayload = JSON.parse(completionReviewText) as {
      successfulCapabilityResults?: Array<{ callId?: string; output?: unknown }>;
    };
    expect(completionReviewPayload.successfulCapabilityResults).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          callId: "check-dentist-call",
          output: expect.objectContaining({
            summary: expect.stringContaining("3:30 to 4:15 PM"),
            disposition: "booked",
            transcript: expect.stringContaining("confirmation is DENT-42"),
          }),
        }),
        expect.objectContaining({
          callId: "save-dentist-appointment",
          output: expect.objectContaining({
            status: "committed",
            providerEventId: "provider-dentist",
          }),
        }),
      ]),
    );
    expect(JSON.stringify(terminal.state.completionEvidence)).not.toContain("confirmation is DENT-42");
    expect(mapOperations).toEqual([
      { operation: "search", query: "Westside Pediatric Dental near 90045", limit: 5 },
    ]);
    expect(operations).toEqual([
      expect.objectContaining({
        kind: "ai_call_start",
        to: "+13105550144",
        timeZone: "America/Los_Angeles",
        record: false,
      }),
      { kind: "ai_call_status", provider: "bland", providerCallId: "bland-call-1" },
      { kind: "ai_call_status", provider: "bland", providerCallId: "bland-call-1" },
    ]);
    expect(calendarOperations).toEqual([
      expect.objectContaining({
        operation: "create",
        event: expect.objectContaining({
          title: "Violet dental cleaning",
          startsAt: "2026-09-02T22:30:00.000Z",
          endsAt: "2026-09-02T23:15:00.000Z",
          location: "1234 Sepulveda Boulevard, Los Angeles, CA 90045",
        }),
      }),
    ]);
    expect(calendarReads).toEqual([
      expect.objectContaining({
        scope: "selected",
        calendarRefs: [calendarRef],
        timeMin: "2026-09-02T07:00:00.000Z",
        timeMax: "2026-09-03T07:00:00.000Z",
      }),
    ]);
    expect(JSON.stringify(modelRequests[4]?.input)).toContain(
      "I’ll call Westside Pediatric Dental and work within those times.",
    );
    const reviewInput = modelRequests[4]?.input as Array<{
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
      firstSentence: "I’m calling for the Williams family about an appointment.",
      voice: null,
      maxDurationMinutes: null,
      record: false,
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
    const completionCondition = "The dentist confirms whether Violet’s Wednesday 3:30 PM cleaning is booked.";
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
        parse(request: Record<string, unknown>) {
          const review = defaultFamilyWorkCompletionReview(request);
          if (review) {
            return {
              ...review,
              output_parsed: { ...review.output_parsed, condition: completionCondition },
            };
          }
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
        completionCondition,
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
      state: { completionCondition },
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
            completionCondition:
              "Violet's Adventure Camp registration is submitted and the provider returns a confirmation.",
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
            completionCondition:
              "Violet's Adventure Camp registration is submitted and the provider returns a confirmation.",
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
          const review = defaultFamilyWorkCompletionReview(request);
          if (review) return review;
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

  test("Kernel keeps each adult's profile private, composes browser tactics, and saves owner sign-in", async () => {
    const profileName = kernelProfileName("household-kernel-1", "adult-1");
    const partnerProfileName = kernelProfileName("household-kernel-1", "adult-2");
    expect(profileName).toMatch(/^florence-[a-f0-9]{40}$/u);
    expect(profileName).not.toContain("household-kernel-1");
    expect(profileName).not.toContain("adult-1");
    expect(partnerProfileName).not.toBe(profileName);

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

    await expect(
      client.run({
        householdId: "household-kernel-1",
        workId: "kernel-work-2",
        ownerAdultId: "adult-2",
        callId: "snapshot-partner-kernel",
        attempt: 1,
        session: navigated.session,
        operation: { kind: "snapshot" },
      }),
    ).rejects.toMatchObject({ code: "session_expired", retryable: false });

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
});
