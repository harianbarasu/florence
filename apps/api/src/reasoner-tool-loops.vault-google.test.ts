import { describe, expect, test } from "vitest";
import {
  FlorenceReasoner,
  florenceGoogleChangesAssessmentDecisionSchema,
  florencePrivateGoogleBatchDecisionSchema,
} from "./reasoner.js";
import {
  completedWebSearch,
  conversationalGmailSource,
  decisionMessage,
  defaultFamilyWorkCompletionReview,
  fakeStream,
  familyWorkOrigin,
  foregroundInput,
  functionCall,
  functionOutputEnvelopes,
  functionOutputs,
  inertReads,
  NOW,
  ordinaryDecision,
  PUBLIC_URL,
  privateAssessmentInput,
  privateBatchInput,
  privateGmailSource,
} from "./reasoner-tool-loops.test-kit.js";

describe("Florence reasoner capability cutover", () => {
  test("rewrites a family need into one contextual Vault query", async () => {
    const requests: Record<string, unknown>[] = [];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse(input: Record<string, unknown>) {
          requests.push(input);
          return {
            status: "completed",
            output_parsed: {
              query:
                "Which family dinner recipe and substitutions did we previously prefer for busy evenings?",
            },
            output: [],
          };
        },
      },
    } as never);

    const query = await reasoner.rewriteMemoryQuery({
      primary: "Make that one again for the busy night.",
      context: ["Use the version Jackson liked, with our substitution."],
    });

    const request = requests[0];
    expect(request).toBeDefined();
    expect(query).toContain("recipe and substitutions");
    expect(String(request?.instructions)).toContain("Resolve pronouns and elliptical references");
    expect(JSON.stringify(request?.input)).toContain("Use the version Jackson liked");
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

  test("a foreground Workspace Gmail thread read preserves every source and opens its verified attachment", async () => {
    const gmail = conversationalGmailSource();
    const earlierGmail = {
      ...gmail,
      sourceId: "gmail-private-0",
      sentAt: "2026-08-27T18:00:00.000Z",
      subject: "Earlier school question",
      text: "Can your child attend?",
      attachments: [],
    };
    const requests: Record<string, unknown>[] = [];
    const responses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("gmail-thread-get", "gmail_work", {
            operation: "gmail_thread_get",
            query: null,
            limit: null,
            messageId: null,
            to: [],
            cc: [],
            bcc: [],
            subject: null,
            body: null,
            bodyFormat: null,
            threadId: "gmail-thread-1",
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
    const workspaceIdentities: Array<{ messageId: string; threadId: string; historyId: string }> = [];

    await reasoner.decide(foregroundInput(), {
      ...inertReads(),
      async runGoogleWorkspace(operation) {
        expect(operation).toEqual({ operation: "gmail_thread_get", threadId: "gmail-thread-1" });
        return {
          operation: "gmail_thread_get",
          result: {
            thread: {
              threadId: "gmail-thread-1",
              historyId: "gmail-history-thread",
              messages: [
                {
                  messageId: "gmail-message-0",
                  threadId: "gmail-thread-1",
                  historyId: "gmail-history-0",
                  from: "School",
                  subject: "Earlier school question",
                  body: "Can your child attend?",
                  attachments: [],
                },
                {
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
              ],
            },
          },
        };
      },
      async readWorkspaceGmailSource(identity) {
        workspaceIdentities.push(identity);
        return identity.messageId === "gmail-message-0" ? earlierGmail : gmail;
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
    expect(workspaceIdentities).toEqual([
      {
        messageId: "gmail-message-0",
        threadId: "gmail-thread-1",
        historyId: "gmail-history-0",
      },
      {
        messageId: "gmail-message-1",
        threadId: "gmail-thread-1",
        historyId: "gmail-history-1",
      },
    ]);
    const getEnvelope = functionOutputEnvelopes(requests[1]).find(
      (envelope) => envelope.callId === "gmail-thread-get",
    );
    expect(getEnvelope?.output).toMatchObject({
      operation: "gmail_thread_get",
      result: {
        thread: {
          messages: [
            {
              sourceId: earlierGmail.sourceId,
              body: earlierGmail.text,
              attachmentAccess: {
                sourceId: earlierGmail.sourceId,
              },
            },
            {
              sourceId: gmail.sourceId,
              body: gmail.text,
              bodyFormat: "plain",
              textStatus: "complete",
              attachments: gmail.attachments,
              attachmentAccess: {
                sourceId: gmail.sourceId,
                attachments: gmail.attachments,
                attachmentsStatus: "complete",
              },
            },
          ],
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

  test("durable private work composes prefetched Vault memory and Gmail attachments inline", async () => {
    const gmail = conversationalGmailSource();
    const memoryFactId = "11111111-1111-4111-8111-111111111111";
    const memoryUri = `vault://fact/${memoryFactId}`;
    const requests: Record<string, unknown>[] = [];
    const responses = [
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("memory-read", "read_vault", { uri: memoryUri, level: "overview" })],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("durable-gmail-search-1", "search_gmail", {
            query: "school form",
            limit: 3,
            cursor: null,
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("durable-gmail-search-2", "search_gmail", {
            query: "school form",
            limit: 3,
            cursor: "gmail-page-2",
          }),
        ],
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
          const review = defaultFamilyWorkCompletionReview(request);
          if (review) return review;
          requests.push(request);
          const response = responses.shift();
          if (!response) throw new Error("Unexpected durable private-read model turn");
          return response;
        },
      },
    } as never);
    let attachmentReads = 0;
    const gmailSearchCursors: Array<string | null> = [];

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
        prefetchedVault: {
          query: "school pickup",
          results: [
            {
              uri: memoryUri,
              score: 1,
              abstract: "School pickup is normally at 2:45 PM.",
              memoryKind: "routine",
              artifactKind: null,
              title: "School pickup",
              tags: ["school", "pickup"],
              updatedAt: NOW,
            },
          ],
          total: 1,
          complete: true,
          nextCursor: null,
        },
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
          throw new Error("The prefetched Vault result should be directly readable");
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
        async searchGmail(input) {
          gmailSearchCursors.push(input.cursor);
          if (input.cursor === null) {
            return {
              status: "truncated" as const,
              complete: false,
              sources: [],
              nextCursor: "gmail-page-2",
            };
          }
          return {
            status: "complete" as const,
            complete: true,
            sources: [gmail],
            nextCursor: null,
          };
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

    expect(gmailSearchCursors).toEqual([null, "gmail-page-2"]);
    expect(result).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("2:45 PM"),
    });
    expect(attachmentReads).toBe(1);
    expect(((requests[0]?.tools as { name: string }[]) ?? []).map((tool) => tool.name)).toEqual(
      expect.arrayContaining(["search_vault", "read_vault", "search_gmail", "read_gmail_attachment"]),
    );
    expect(JSON.stringify(requests[0]?.input)).toContain("recalledMemory");
    expect(JSON.stringify(requests[0]?.input)).toContain("School pickup is normally at 2:45 PM.");
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
      calendarCoverage: {
        complete: true,
        observedCalendarCount: 1,
        completeCalendarCount: 1,
        missingCalendarCount: 0,
        unavailableCalendarCount: 0,
        digest: "3".repeat(64),
      },
      events: calendarEvents.slice(0, 50),
      totalCalendarCount: 1,
      totalEventCount: 73,
      nextCursor: "calendar-page-2",
    };
    const secondRead = {
      status: "complete" as const,
      calendars: calendarCoverage,
      calendarCoverage: firstRead.calendarCoverage,
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
          nextCursor: null,
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
            calendarCoverage: {
              complete: false,
              observedCalendarCount: 1,
              completeCalendarCount: 1,
              missingCalendarCount: 0,
              unavailableCalendarCount: 0,
              digest: "4".repeat(64),
            },
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

  test("complete Calendar coverage remains bounded for an all-calendar read", async () => {
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
    const result = await reasoner.decide(foregroundInput(), {
      ...inertReads(),
      async readCalendarWindow() {
        return {
          status: "complete",
          calendars: [],
          totalCalendarCount: 101,
          calendarCoverage: {
            complete: true,
            observedCalendarCount: 101,
            completeCalendarCount: 101,
            missingCalendarCount: 0,
            unavailableCalendarCount: 0,
            digest: "5".repeat(64),
          },
          events: [
            {
              eventRef: "event-wanted",
              providerUpdatedAt: "2026-08-27T19:00:00.000Z",
              calendarRef: "calendar-0",
              calendarLabel: "Calendar 1",
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
      status: "complete",
      totalCalendarCount: 101,
      calendars: [],
      calendarCoverage: { complete: true, observedCalendarCount: 101 },
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
      completionCondition:
        "The permission-form decision is made and any requested school response is confirmed.",
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
    const currentFacts = Array.from({ length: 125 }, (_, index) => ({
      slot: `family:retained-context:${index + 1}`,
      statement: `Retained family context ${index + 1} remains current.`,
      memory: {
        memoryKind: "fact" as const,
        artifactKind: null,
        title: null,
        details: null,
        tags: [],
      },
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

    const initial = await reasoner.classifyPrivateGoogleBatch(
      { ...privateBatchInput(gmail), currentFacts },
      reads,
    );
    const incremental = await reasoner.assessGoogleChanges(
      {
        ...privateAssessmentInput(gmail),
        currentFacts,
        memory: currentFacts.map((fact) => ({
          slot: fact.slot,
          label: fact.statement,
          text: fact.statement,
        })),
      },
      reads,
    );

    expect(initial.facts).toEqual(facts);
    expect(incremental.facts).toEqual(facts);
    expect(String(requests[0]?.instructions)).toContain("Return every eligible fact supported by this batch");
    expect(String(requests[1]?.instructions)).toContain("never omit one merely to satisfy an output count");
    expect(JSON.stringify(requests[0]?.input)).toContain("Retained family context 125 remains current.");
    expect(JSON.stringify(requests[1]?.input)).toContain("Retained family context 125 remains current.");
    expect(requests.map((request) => String(request.instructions))).not.toEqual(
      expect.arrayContaining([expect.stringContaining("up to twenty")]),
    );
  });
});
