import { type FamilyWorkStateV1, steerFamilyWorkState } from "@florence/database";
import { describe, expect, test } from "vitest";
import { FlorenceReasoner } from "./reasoner.js";
import {
  completedWebSearch,
  defaultFamilyWorkCompletionReview,
  fakeStream,
  familyWorkOrigin,
  flightResult,
  foregroundInput,
  functionCall,
  functionOutputEnvelopes,
  inertReads,
  NOW,
  ordinaryDecision,
  weatherResult,
} from "./reasoner-tool-loops.test-kit.js";

describe("Florence reasoner capability cutover", () => {
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
                address: "Los Angeles, California, United States",
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
          const review = defaultFamilyWorkCompletionReview(request);
          if (review) return review;
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
          const review = defaultFamilyWorkCompletionReview(request);
          if (review) return review;
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
          nextCursor: null,
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

  test("household work privately asks one exact enrolled adult and waits without polling the group", async () => {
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
              functionCall("ask-jackson", "participant_request", {
                targetAdultName: "Jackson Williams",
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
        responsibleAdultId: "adult-2",
        completionCondition:
          "Violet's full field-trip-day attendance is decided and the family plan is complete.",
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
          targetAdultId: "adult-2",
          targetAdultName: "Jackson Williams",
          channelId: "private-channel-2",
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
            targetAdultId: "adult-2",
            targetAdultName: "Jackson Williams",
            channelId: "private-channel-2",
            questionSourceId: "private-question-1",
            question,
          }),
          waitingDocket: {
            owner: "Jackson Williams",
            nextAction: "Answer Florence's private question.",
            waitingOn: question,
            needsAnswer: true,
            completionCondition:
              "Violet's full field-trip-day attendance is decided and the family plan is complete.",
          },
        }),
      }),
    );
    expect(waiting).not.toHaveProperty("question");
    expect(queuedRequests).toEqual([{ targetAdultName: "Jackson Williams", question }]);
    expect(modelRequests).toHaveLength(1);
    const [modelRequest] = modelRequests;
    if (!modelRequest) throw new Error("Participant request was not presented to the model");
    const participantTool = (modelRequest.tools as { name?: string; parameters?: unknown }[]).find(
      (tool) => tool.name === "participant_request",
    );
    expect(participantTool).toMatchObject({
      parameters: {
        properties: {
          targetAdultName: { enum: ["Hari Anbarasu", "Jackson Williams"] },
        },
      },
    });
    const familyWorkInput = JSON.parse(
      String(
        ((modelRequest.input as Array<{ content?: Array<{ text?: string }> }>)?.[0]?.content ?? [])[0]?.text,
      ),
    ) as { responsibleAdult?: { displayName?: string } | null };
    expect(familyWorkInput.responsibleAdult).toEqual({ displayName: "Jackson Williams" });
  });
});
