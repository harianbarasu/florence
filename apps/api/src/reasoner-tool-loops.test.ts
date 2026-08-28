import { describe, expect, test } from "vitest";
import {
  type FlorenceDecision,
  type FlorenceGoogleChangesAssessmentInput,
  type FlorencePrivateGoogleBatchInput,
  FlorenceReasoner,
  type FlorenceReasonerInput,
  type FlorenceSource,
} from "./reasoner.js";

const NOW = "2026-08-27T20:00:00.000Z";
const PUBLIC_URL = "https://example.com/current-result";
const admittedReadAccounting = {
  settleSources() {},
};

describe("Florence reasoner capability cutover", () => {
  test("all foreground function calls use one source-bearing lifecycle and cue only after admission", async () => {
    const requests: Record<string, unknown>[] = [];
    let workStarts = 0;
    const calls = [
      functionCall("memory-call", "search_family_memory", { query: "school", limit: 3 }),
      functionCall("source-call", "read_source", { sourceId: "turn-1" }),
      functionCall("public-call", "research_public_web", {}),
      functionCall("gmail-call", "search_gmail", { query: "pickup", limit: 3 }),
      functionCall("calendar-call", "read_calendar_window", {
        timeMin: "2026-08-28T16:00:00.000Z",
        timeMax: "2026-08-28T18:00:00.000Z",
        limit: 10,
        scope: "all",
        calendarRefs: [],
      }),
    ];
    let foregroundTurn = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          foregroundTurn += 1;
          return fakeStream(
            foregroundTurn === 1
              ? { status: "completed", output_parsed: null, output: calls }
              : {
                  status: "completed",
                  output_parsed: ordinaryDecision({ researchUrls: [PUBLIC_URL] }),
                  output: [],
                },
          );
        },
        parse: () => ({
          status: "completed",
          output_parsed: { outcome: "result", summary: "Current result", urls: [PUBLIC_URL] },
          output: [completedWebSearch(PUBLIC_URL)],
        }),
      },
    } as never);
    const readCounts = { memory: 0, source: 0, gmail: 0, calendar: 0 };
    const result = await reasoner.decide(
      foregroundInput(),
      {
        ...admittedReadAccounting,
        async searchFamilyMemory() {
          readCounts.memory += 1;
          return [source("memory-1", "memory", "shared")];
        },
        async readSource() {
          readCounts.source += 1;
          return source("turn-1", "message", "adult_private");
        },
        async searchGmail() {
          readCounts.gmail += 1;
          return { status: "complete" as const, sources: [conversationalGmailSource()] };
        },
        async readCalendarWindow() {
          readCounts.calendar += 1;
          return completeCalendarRead();
        },
        async readCurrentImage() {
          throw new Error("No image was authorized");
        },
      },
      undefined,
      {
        onWorkStarted() {
          workStarts += 1;
        },
      },
    );

    expect(result.conversation.bubbles[0]?.text).toBe("Done.");
    expect(readCounts).toEqual({ memory: 1, source: 1, gmail: 1, calendar: 1 });
    const toolNames = ((requests[0]?.tools as { name: string }[]) ?? []).map((tool) => tool.name);
    expect(toolNames).toEqual([
      "list_calendars",
      "read_calendar_window",
      "read_gmail_attachment",
      "read_source",
      "research_public_web",
      "search_family_memory",
      "search_gmail",
    ]);
    expect(JSON.stringify(requests[0])).not.toContain("connectionId");
    expect(workStarts).toBe(1);
    const secondInput = JSON.stringify(requests[1]?.input);
    for (const call of calls) {
      expect(secondInput).toContain(call.call_id);
    }
    const envelopes = functionOutputEnvelopes(requests[1]);
    expect(envelopes).toHaveLength(5);
    for (const envelope of envelopes) {
      expect(envelope.outcome).toBe("succeeded");
      expect(JSON.stringify(envelope).length).toBeLessThan(120_000);
    }
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

    await reasoner.decide(
      foregroundInput(),
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
    const bookingUrl = "https://www.kiwi.com/deep?from=JFK&to=LAX&date=2026-08-28";
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
                  output: [functionCall("flight-status", "research_public_web", {})],
                }
              : modelTurn === 2
                ? {
                    status: "completed",
                    output_parsed: null,
                    output: [
                      functionCall("flight-options", "flights_search", {
                        origin: "JFK",
                        destination: "LAX",
                        departureDate: "2026-08-28",
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
        parse: () => ({
          status: "completed",
          output_parsed: {
            outcome: "result",
            summary:
              "DL 747 on August 28, 2026 is delayed from JFK to LAX; scheduled 5:30 PM local departure and 8:46 PM local arrival.",
            urls: [statusUrl],
          },
          output: [completedWebSearch(statusUrl)],
        }),
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
        departureDate: "2026-08-28",
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
      functionOutputEnvelopes(requests[2]).find((envelope) => envelope.callId === "flight-options"),
    ).toMatchObject({
      outcome: "succeeded",
      output: {
        operation: "search",
        returnedCount: 1,
        timeBasis: "provider_local_time_at_each_airport",
      },
    });
  });

  test("public place verification receives the map candidates selected earlier in the turn", async () => {
    let modelTurn = 0;
    const publicResearchRequests: Record<string, unknown>[] = [];
    let workStarts = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: () => {
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
              : modelTurn === 2
                ? {
                    status: "completed",
                    output_parsed: null,
                    output: [functionCall("research-call", "research_public_web", {})],
                  }
                : {
                    status: "completed",
                    output_parsed: ordinaryDecision({ researchUrls: [PUBLIC_URL] }),
                    output: [],
                  },
          );
        },
        parse: (request: Record<string, unknown>) => {
          publicResearchRequests.push(request);
          return {
            status: "completed",
            output_parsed: { outcome: "result", summary: "Open now", urls: [PUBLIC_URL] },
            output: [completedWebSearch(PUBLIC_URL)],
          };
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

    const isolatedInput = JSON.stringify(publicResearchRequests[0]?.input);
    expect(isolatedInput).toContain("Junior's");
    expect(isolatedInput).toContain("1515 Broadway, New York");
    expect(isolatedInput).toContain("juniorscheesecake.com");
    expect(isolatedInput).not.toContain("40.7582151");
    expect(workStarts).toBe(1);
  });

  test("a foreground Gmail search can open only its verified attachment as an ephemeral artifact", async () => {
    const gmail = conversationalGmailSource();
    const requests: Record<string, unknown>[] = [];
    const responses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("gmail-search", "search_gmail", {
            query: "school form",
            limit: 3,
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

    await reasoner.decide(foregroundInput(), {
      ...inertReads(),
      async searchGmail() {
        return { status: "complete", sources: [gmail] };
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
    const searchEnvelope = functionOutputEnvelopes(requests[1]).find(
      (envelope) => envelope.callId === "gmail-search",
    );
    expect(searchEnvelope?.output).toMatchObject({
      status: "complete",
      sources: [
        expect.objectContaining({
          sourceId: gmail.sourceId,
          textStatus: "complete",
          attachmentsStatus: "complete",
        }),
      ],
    });
    const attachmentOutput = functionOutputs(requests[2]).find(
      (item) => item.call_id === "gmail-attachment",
    )?.output;
    expect(Array.isArray(attachmentOutput)).toBe(true);
    expect(JSON.stringify(attachmentOutput)).toContain("input_file");
    expect(JSON.stringify(requests)).not.toContain("connectionId");
  });

  test("calendar catalog references admit a selected window without hiding partial coverage", async () => {
    const requests: Record<string, unknown>[] = [];
    const calendarRefs = ["calendar-school", "calendar-work"];
    const selectedRead = {
      status: "partial" as const,
      calendars: [
        {
          calendarRef: calendarRefs[0] ?? "missing-school",
          label: "School",
          timeZone: "America/Los_Angeles",
          primary: false,
          accessRole: "reader" as const,
          status: "complete" as const,
          eventCount: 1,
        },
        {
          calendarRef: calendarRefs[1] ?? "missing-work",
          label: "Work",
          timeZone: "America/Los_Angeles",
          primary: false,
          accessRole: "owner" as const,
          status: "unavailable" as const,
          eventCount: 0,
        },
      ],
      events: [
        {
          eventRef: "event-1",
          providerUpdatedAt: "2026-08-27T19:00:00.000Z",
          calendarRef: calendarRefs[0] ?? "missing-school",
          calendarLabel: "School",
          title: "Back-to-school night",
          location: "Wish Charter",
          status: "tentative" as const,
          busy: true,
          intervalKind: "timed" as const,
          startsAt: "2026-08-28T16:00:00.000Z",
          endsAt: "2026-08-28T18:00:00.000Z",
          timeZone: "America/Los_Angeles",
        },
      ],
      totalCalendarCount: 2,
      totalEventCount: 1,
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
        output: [
          functionCall("calendar-window", "read_calendar_window", {
            timeMin: "2026-08-28T00:00:00.000Z",
            timeMax: "2026-08-29T00:00:00.000Z",
            limit: 20,
            scope: "selected",
            calendarRefs,
          }),
        ],
      },
      { status: "completed", output_parsed: ordinaryDecision(), output: [] },
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
    let calendarInput: Record<string, unknown> | null = null;

    await reasoner.decide(foregroundInput(), {
      ...inertReads(),
      async listCalendars() {
        return {
          status: "complete",
          calendars: [
            {
              calendarRef: calendarRefs[0] ?? "missing-school",
              label: "School",
              timeZone: "America/Los_Angeles",
              primary: false,
              accessRole: "reader",
              eventCoverage: "readable",
            },
            {
              calendarRef: calendarRefs[1] ?? "missing-work",
              label: "Work",
              timeZone: "America/Los_Angeles",
              primary: false,
              accessRole: "owner",
              eventCoverage: "readable",
            },
          ],
          totalCalendarCount: 2,
        };
      },
      async readCalendarWindow(input) {
        calendarInput = input;
        return selectedRead;
      },
    });

    expect(calendarInput).toMatchObject({ scope: "selected", calendarRefs });
    const windowEnvelope = functionOutputEnvelopes(requests[2]).find(
      (envelope) => envelope.callId === "calendar-window",
    );
    expect(windowEnvelope?.output).toMatchObject({
      status: "partial",
      totalEventCount: 1,
      calendars: [
        expect.objectContaining({ label: "School", status: "complete", eventCount: 1 }),
        expect.objectContaining({ label: "Work", status: "unavailable", eventCount: 0 }),
      ],
      events: [expect.objectContaining({ calendarLabel: "School", status: "tentative", busy: true })],
    });
  });

  test("truncated, malformed, and unknown calls never execute and remain model-visible for recovery", async () => {
    const requests: Record<string, unknown>[] = [];
    let workStarts = 0;
    let modelTurn = 0;
    let memoryReads = 0;
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          modelTurn += 1;
          if (modelTurn === 1) {
            return fakeStream({
              status: "incomplete",
              output_parsed: null,
              output: [functionCall("truncated-call", "search_family_memory", { query: "x", limit: 1 })],
            });
          }
          if (modelTurn === 2) {
            return fakeStream({
              status: "completed",
              output_parsed: null,
              output: [
                functionCall("unknown-call", "invented_tool", {}),
                {
                  ...functionCall("malformed-call", "search_family_memory", {}),
                  arguments: "{",
                },
              ],
            });
          }
          return fakeStream({
            status: "completed",
            output_parsed: ordinaryDecision(),
            output: [],
          });
        },
      },
    } as never);

    await reasoner.decide(
      foregroundInput(),
      {
        ...admittedReadAccounting,
        async searchFamilyMemory() {
          memoryReads += 1;
          return [];
        },
        async readSource() {
          return null;
        },
        async searchGmail() {
          return { status: "complete", sources: [] };
        },
        async readCalendarWindow() {
          return completeCalendarRead();
        },
        async readCurrentImage() {
          throw new Error("No image was authorized");
        },
      },
      undefined,
      {
        onWorkStarted() {
          workStarts += 1;
        },
      },
    );

    expect(memoryReads).toBe(0);
    expect(workStarts).toBe(0);
    expect(functionOutputEnvelopes(requests[1])[0]?.error?.code).toBe("truncated_model_output");
    const rejected = functionOutputEnvelopes(requests[2]).slice(-2);
    expect(rejected.map((envelope) => envelope.error?.code)).toEqual([
      "unknown_or_unavailable_capability",
      "invalid_arguments",
    ]);
  });

  test("both private Gmail attachment loops use the registry without exposing connection IDs", async () => {
    const gmail = privateGmailSource();
    const requests: Record<string, unknown>[] = [];
    let workStarts = 0;
    const responses = [
      { status: "completed", output_parsed: null, output: [attachmentCall("batch-attachment")] },
      {
        status: "completed",
        output_parsed: { findings: [], facts: [], dismissedSourceIds: [gmail.sourceId] },
        output: [],
      },
      { status: "completed", output_parsed: null, output: [attachmentCall("change-attachment")] },
      {
        status: "completed",
        output_parsed: { findings: [], facts: [], dismissedSourceIds: [gmail.sourceId] },
        output: [],
      },
    ];
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
      async readGmailAttachment() {
        attachmentReads += 1;
        return {
          sourceId: gmail.sourceId,
          attachmentRef: gmail.attachments[0]?.attachmentRef ?? "missing",
          filename: gmail.attachments[0]?.filename ?? "missing.pdf",
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

    expect(attachmentReads).toBe(2);
    expect(workStarts).toBe(2);
    for (const firstRequest of [requests[0], requests[2]]) {
      expect(JSON.stringify(firstRequest)).not.toContain("private-google-connection");
      expect(JSON.stringify(firstRequest)).not.toContain("connectionId");
      expect(((firstRequest?.tools as { name: string }[]) ?? []).map((tool) => tool.name)).toEqual([
        "read_private_gmail_attachment",
      ]);
    }
    for (const continuation of [requests[1], requests[3]]) {
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

  test("ports Pi transient classification with quota and billing precedence but performs no blind retry", async () => {
    const reasonerFor = (message: string) =>
      new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
        responses: {
          stream() {
            throw new Error(message);
          },
        },
      } as never);
    const reads = inertReads();

    await expect(
      reasonerFor("upstream connect reset before headers 503").decide(foregroundInput(), reads),
    ).rejects.toMatchObject({ code: "transient", retryable: true });
    await expect(
      reasonerFor("429 insufficient_quota billing exhausted").decide(foregroundInput(), reads),
    ).rejects.toMatchObject({ code: "rejected", retryable: false });
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
    visibleReminders: [],
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
    },
    facts: [],
    followUp: null,
    reminder: null,
    interest: null,
    calendar: null,
    householdUpdate: null,
    webAccessPath: null,
    researchUrls: input.researchUrls ?? null,
  };
}

function source(
  sourceId: string,
  kind: FlorenceSource["kind"],
  visibility: FlorenceSource["visibility"],
): FlorenceSource {
  return {
    sourceId,
    recordId: kind === "memory" ? "fact-1" : null,
    kind,
    visibility,
    label: `${kind} source`,
    occurredAt: NOW,
    text: "Bounded source text",
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

function attachmentCall(callId: string) {
  return functionCall(callId, "read_private_gmail_attachment", {
    sourceId: "gmail-private-1",
    attachmentRef: "attachment-1",
  });
}

function completedWebSearch(url: string) {
  return {
    id: "web-search-1",
    type: "web_search_call" as const,
    status: "completed" as const,
    action: { type: "search" as const, query: "current result", sources: [{ type: "url", url }] },
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

function flightResult(bookingUrl: string) {
  return {
    operation: "search" as const,
    query: "JFK to LAX",
    currency: "USD",
    passengers: { adults: 1, children: 0, infants: 0 },
    resultsCount: 1,
    returnedCount: 1,
    itineraries: [
      {
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
          departureTime: "2026-08-28T19:00:00",
          arrivalTime: "2026-08-28T22:00:00",
          durationSeconds: 21_600,
          stops: 0,
          route: ["JFK", "LAX"],
          cabinClass: "M",
          segments: [],
        },
        inbound: null,
        highlights: ["cheapest" as const, "shortest" as const, "earliest" as const],
      },
    ],
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
