import { type FamilyWorkStateV1, steerFamilyWorkState } from "@florence/database";
import { GoogleCalendarTransientError, GoogleWorkspaceError } from "@florence/google";
import { describe, expect, test } from "vitest";
import type { FlorenceBrowserObservation, FlorenceBrowserOperation } from "./browser.js";
import {
  type FlorenceDecision,
  type FlorenceGoogleChangesAssessmentInput,
  type FlorencePrivateGoogleBatchInput,
  FlorenceReasoner,
  type FlorenceReasonerInput,
} from "./reasoner.js";

const NOW = "2026-08-27T20:00:00.000Z";
const PUBLIC_URL = "https://example.com/current-result";
const admittedReadAccounting = {
  settleSources() {},
};

describe("Florence reasoner capability cutover", () => {
  test("reads parent-supplied and task-selected public pages", async () => {
    const pageUrl = "https://school.example/fall-fair";
    const selectedUrl = "https://school.example/fall-fair/faq";
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
        return publicPageResult(request.url, "Fall Fair", "RSVP closes September 6 at 5 PM.");
      },
    });

    expect(pageReads).toEqual([
      { url: pageUrl, charLimit: 15_000 },
      { url: selectedUrl, charLimit: 15_000 },
    ]);
    expect(result.conversation.bubbles[0]?.text).toContain("September 6 at 5 PM");
    expect(result.researchUrls).toEqual([pageUrl]);
    const envelopes = functionOutputEnvelopes(requests[1]);
    expect(envelopes.find((envelope) => envelope.callId === "parent-page")).toMatchObject({
      outcome: "succeeded",
      output: { title: "Fall Fair", text: expect.stringContaining("September 6") },
    });
    expect(envelopes.find((envelope) => envelope.callId === "selected-page")).toMatchObject({
      outcome: "succeeded",
      output: { title: "Fall Fair", text: expect.stringContaining("September 6") },
    });
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
                    functionCall("find-trip", "research_public_web", {
                      query: "school field trip permission form deadline",
                    }),
                  ],
                }
              : modelTurn === 2
                ? {
                    status: "completed",
                    output_parsed: null,
                    output: [functionCall("open-trip", "read_public_page", { url: searchUrl })],
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
        parse: () => ({
          status: "completed",
          output_parsed: {
            outcome: "result",
            summary: "The school posted a field-trip page.",
            urls: [searchUrl],
          },
          output: [completedWebSearch(searchUrl)],
        }),
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
    expect(JSON.stringify(requests[2]?.input)).toContain("Permission form due Tuesday at 3 PM");
    expect(result.researchUrls).toEqual([searchUrl]);
  });

  test("durable work reads a linked PDF at a persisted checkpoint", async () => {
    const pdfUrl = "https://school.example/forms/field-trip.pdf";
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
        parse() {
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
      browserSession: null,
      continuationItems: [],
      pendingCall: null,
      steering: [],
      publicMapResearchContext: [],
      progressRevision: 0,
      terminal: null,
    };
    const input = {
      workId: "family-work-pdf",
      objective: `Read ${pdfUrl} and tell me the deadline and what I need to do.`,
      visibility: "private" as const,
      ownerAdultId: "adult-1",
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
    const runPublicPage = async () => ({
      ...publicPageResult(pdfUrl, "Field trip form", "Return by Tuesday at 3 PM. Parent signature required."),
      kind: "pdf" as const,
      filename: "field-trip.pdf",
    });

    const planned = await reasoner.continueFamilyWork(input, { runPublicPage });
    if (planned.kind !== "continue") throw new Error("Durable PDF read was not planned");
    expect(planned.state).toMatchObject({ phase: "tool_pending" });
    const read = await reasoner.continueFamilyWork({ ...input, state: planned.state }, { runPublicPage });
    if (read.kind !== "continue") throw new Error("Durable PDF read did not settle");
    expect(JSON.stringify(read.state.continuationItems)).toContain("Parent signature required");
    expect(Buffer.byteLength(JSON.stringify(read.state))).toBeLessThan(240 * 1024);
    const terminal = await reasoner.continueFamilyWork({ ...input, state: read.state }, { runPublicPage });
    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("parent signature"),
    });
  });

  test("durable work operates a family portal, pauses for sign-in, and resumes at review", async () => {
    const portalUrl = "https://camp.example/register";
    const liveViewUrl = "https://www.browserbase.com/sessions/session-1";
    const modelRequests: Record<string, unknown>[] = [];
    const modelResponses = [
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
            browserArguments("type", { ref: "e5", text: "Violet Williams" }),
          ),
        ],
      },
      {
        status: "completed",
        output_parsed: null,
        output: [functionCall("inspect-registration", "browser_work", browserArguments("screenshot"))],
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
          outcome: "succeeded",
          text: "I filled Violet’s camp registration and reached the final review page. The Submit registration button is ready for your final check.",
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
      browserSession: null,
      continuationItems: [],
      pendingCall: null,
      steering: [],
      publicMapResearchContext: [],
      progressRevision: 0,
      terminal: null,
    };
    const input = {
      workId: "family-work-camp-registration",
      objective: "Fill out Violet’s camp registration and get it ready for my final review.",
      visibility: "private" as const,
      ownerAdultId: "adult-1",
      household: {
        householdId: "household-1",
        familyLabel: "Test family",
        timeZone: "America/Los_Angeles",
        postalCode: "90045",
        adults: [{ adultId: "adult-1", firstName: "Hari", displayName: "Hari Anbarasu" }],
        children: [],
      },
      state: initialState,
      currentTime: NOW,
    };
    const browserOperations: FlorenceBrowserOperation[] = [];
    const reads = {
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
          case "type":
            return browserObservation({
              title: "Camp registration",
              url: portalUrl,
              snapshot: '- textbox "Child name" [ref=e5] value="Violet Williams"\n- button Preview [ref=e9]',
            });
          case "screenshot":
            return browserObservation({
              title: "Camp registration",
              url: portalUrl,
              snapshot: '- textbox "Child name" [ref=e5] value="Violet Williams"\n- button Preview [ref=e9]',
              screenshot: {
                mimeType: "image/jpeg" as const,
                bytes: Uint8Array.from([0xff, 0xd8, 0xff, 0xd9]),
              },
            });
          case "click":
            return browserObservation({
              title: "Review registration",
              url: `${portalUrl}/review`,
              snapshot: '- heading "Review registration"\n- button Submit registration [ref=e12]',
            });
          default:
            throw new Error(`Unexpected browser operation ${String(operation.kind)}`);
        }
      },
    };

    const navigationPlanned = await reasoner.continueFamilyWork(input, reads);
    if (navigationPlanned.kind !== "continue") throw new Error("Portal navigation was not planned");
    const navigated = await reasoner.continueFamilyWork({ ...input, state: navigationPlanned.state }, reads);
    if (navigated.kind !== "continue") throw new Error("Portal navigation did not settle");
    expect(navigated.progressText).toContain("Family Camp Portal");

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
    expect(JSON.stringify(modelRequests[2]?.input)).toContain(liveViewUrl);

    const signedInState = steerFamilyWorkState(waiting.state, {
      sourceId: "00000000-0000-4000-8000-000000000002",
      text: "I’m signed in—keep going.",
      occurredAt: "2026-08-27T20:02:00.000Z",
    });
    const fillPlanned = await reasoner.continueFamilyWork({ ...input, state: signedInState }, reads);
    if (fillPlanned.kind !== "continue") throw new Error("Camp form fill was not planned");
    const filled = await reasoner.continueFamilyWork({ ...input, state: fillPlanned.state }, reads);
    if (filled.kind !== "continue") throw new Error("Camp form fill did not settle");

    const screenshotPlanned = await reasoner.continueFamilyWork({ ...input, state: filled.state }, reads);
    if (screenshotPlanned.kind !== "continue") throw new Error("Camp form inspection was not planned");
    const inspected = await reasoner.continueFamilyWork({ ...input, state: screenshotPlanned.state }, reads);
    if (inspected.kind !== "continue") throw new Error("Camp form inspection did not settle");
    expect(JSON.stringify(inspected.state.continuationItems)).toContain("input_image");

    const reviewPlanned = await reasoner.continueFamilyWork({ ...input, state: inspected.state }, reads);
    if (reviewPlanned.kind !== "continue") throw new Error("Camp review was not planned");
    expect(JSON.stringify(modelRequests[5]?.input)).toContain("input_image");
    expect(JSON.stringify(reviewPlanned.state.continuationItems)).not.toContain("input_image");
    const reviewed = await reasoner.continueFamilyWork({ ...input, state: reviewPlanned.state }, reads);
    if (reviewed.kind !== "continue") throw new Error("Camp review did not settle");
    const terminal = await reasoner.continueFamilyWork({ ...input, state: reviewed.state }, reads);

    expect(browserOperations.map((operation) => operation.kind)).toEqual([
      "navigate",
      "owner_handoff",
      "type",
      "screenshot",
      "click",
    ]);
    expect(JSON.stringify(modelRequests[3]?.input)).toContain("I’m signed in—keep going.");
    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("final review page"),
    });
    if (terminal.kind !== "terminal") throw new Error("Camp registration did not finish");
    expect(terminal.text).toContain("Submit registration");
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
                    functionCall("flight-status", "research_public_web", {
                      query: "DL 747 status route tonight JFK LAX",
                    }),
                  ],
                }
              : modelTurn === 2
                ? {
                    status: "completed",
                    output_parsed: null,
                    output: [
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
        parse: () => ({
          status: "completed",
          output_parsed: {
            outcome: "result",
            summary:
              "DL 747 on August 27, 2026 is delayed from JFK to LAX; scheduled 5:30 PM local departure and 8:46 PM local arrival.",
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

  test("durable flight work resumes from its checkpoint, accepts steering, and reaches one result", async () => {
    const statusUrl = "https://www.delta.com/flight-status/search?flightId=DL747";
    const bookingUrl = "https://www.kiwi.com/deep?from=JFK&to=LAX&date=2026-08-27";
    const secondBookingUrl = `${bookingUrl}&option=2`;
    const firstRequests: Record<string, unknown>[] = [];
    const firstResponses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("durable-flight-status", "research_public_web", {
            query: "DL 747 live status route tonight",
          }),
        ],
      },
      {
        status: "completed",
        output_parsed: {
          outcome: "result",
          summary: "DL 747 is delayed tonight from JFK to LAX.",
          urls: [statusUrl],
        },
        output: [completedWebSearch(statusUrl)],
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
      browserSession: null,
      continuationItems: [],
      pendingCall: null,
      steering: [],
      publicMapResearchContext: [],
      progressRevision: 0,
      terminal: null,
    };
    const input = {
      workId: "family-work-1",
      objective: "DL 747 is delayed tonight. Find the two best nonstop alternatives; Delta if possible.",
      visibility: "household" as const,
      ownerAdultId: null,
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

    const plannedStatus = await firstReasoner.continueFamilyWork(input, {});
    expect(plannedStatus).toMatchObject({ kind: "continue", state: { phase: "tool_pending" } });
    if (plannedStatus.kind !== "continue") throw new Error("Status lookup was not planned");
    const checkedStatus = await firstReasoner.continueFamilyWork(
      { ...input, state: plannedStatus.state },
      {},
    );
    expect(checkedStatus).toMatchObject({
      kind: "continue",
      progressText: expect.stringContaining("DL 747 is delayed"),
      state: { phase: "ready", progressRevision: 1 },
    });
    if (checkedStatus.kind !== "continue") throw new Error("Status lookup did not settle");

    const resumedRequests: Record<string, unknown>[] = [];
    const resumedResponses = [
      {
        status: "completed",
        output_parsed: null,
        output: [
          functionCall("durable-flight-options", "flights_search", {
            origin: "JFK",
            destination: "LAX",
            departureDate: "2026-08-27",
            returnDate: null,
            adults: 1,
            children: 0,
            infants: 0,
            cabinClass: "economy",
            preferredAirlines: ["DL"],
            maxStops: 0,
            outboundDepartureHours: { from: 17, to: 23 },
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
    const plannedAlternatives = await resumedReasoner.continueFamilyWork(
      { ...input, state: checkedStatus.state },
      {
        async runFlights() {
          flightSearches += 1;
          return flightResult(bookingUrl, 2);
        },
      },
    );
    expect(plannedAlternatives).toMatchObject({
      kind: "continue",
      state: { phase: "tool_pending" },
    });
    if (plannedAlternatives.kind !== "continue") throw new Error("Alternative search was not planned");
    const steeredPendingState = steerFamilyWorkState(plannedAlternatives.state, {
      sourceId: "00000000-0000-4000-8000-000000000001",
      text: "JetBlue is fine too, but nothing before 7 PM.",
      occurredAt: "2026-08-27T20:01:00.000Z",
    });
    expect(steeredPendingState).toMatchObject({
      phase: "ready",
      generation: 1,
      pendingCall: null,
    });
    expect(JSON.stringify(steeredPendingState.continuationItems)).not.toContain("durable-flight-options");
    expect(JSON.stringify(steeredPendingState.continuationItems)).toContain("durable-flight-status");
    const replannedAlternatives = await resumedReasoner.continueFamilyWork(
      { ...input, state: steeredPendingState },
      {
        async runFlights() {
          flightSearches += 1;
          return flightResult(bookingUrl, 2);
        },
      },
    );
    if (replannedAlternatives.kind !== "continue") {
      throw new Error("Steered alternative search was not replanned");
    }
    const searchedAlternatives = await resumedReasoner.continueFamilyWork(
      { ...input, state: replannedAlternatives.state },
      {
        async runFlights() {
          flightSearches += 1;
          return flightResult(bookingUrl, 2);
        },
      },
    );
    if (searchedAlternatives.kind !== "continue") throw new Error("Alternative search did not settle");
    const terminal = await resumedReasoner.continueFamilyWork(
      { ...input, state: searchedAlternatives.state },
      {},
    );

    expect(flightSearches).toBe(1);
    expect(JSON.stringify(resumedRequests[1]?.input)).toContain(
      "JetBlue is fine too, but nothing before 7 PM.",
    );
    expect(JSON.parse(replannedAlternatives.state.pendingCall?.argumentsJson ?? "{}")).toMatchObject({
      departureDate: "2026-08-27",
      preferredAirlines: ["DL", "B6"],
      outboundDepartureHours: { from: 19, to: 23 },
    });
    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("1. Delta nonstop"),
      state: { phase: "terminal", progressRevision: 2 },
    });
    if (terminal.kind !== "terminal") throw new Error("Durable flight work did not finish");
    expect(terminal.text).toContain("2. JetBlue nonstop");
    expect(terminal.text).toContain(bookingUrl);
    expect(terminal.text).toContain(secondBookingUrl);
  });

  test("durable work ends cleanly before an oversized checkpoint can retry forever", async () => {
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        parse() {
          throw new Error("An oversized durable checkpoint must not reach the model");
        },
      },
    } as never);
    const state: FamilyWorkStateV1 = {
      kind: "family_work_v1",
      version: 1,
      generation: 3,
      phase: "ready",
      claim: null,
      browserSession: null,
      continuationItems: [
        {
          type: "message",
          role: "user",
          content: [{ type: "input_text", text: "x".repeat(241 * 1024) }],
        },
      ],
      pendingCall: null,
      steering: [],
      publicMapResearchContext: [],
      progressRevision: 4,
      terminal: null,
    };

    const result = await reasoner.continueFamilyWork(
      {
        workId: "family-work-large",
        objective: "Compare the useful options.",
        visibility: "household",
        ownerAdultId: null,
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

    expect(result).toMatchObject({
      kind: "terminal",
      outcome: "failed",
      state: {
        phase: "terminal",
        continuationItems: [],
        progressRevision: 5,
      },
    });
  });

  test("Workspace reads run in private conversation while writes stay in durable work", async () => {
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
                  output_parsed: ordinaryDecision({
                    bubbleText: "I found the enrollment form. I haven’t sent anything.",
                  }),
                  output: [],
                },
          );
        },
      },
    } as never);
    const result = await reasoner.decide(foregroundInput(), {
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
    expect(result.conversation.bubbles[0]?.text).toContain("haven’t sent anything");
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
      browserSession: null,
      continuationItems: [],
      pendingCall: null,
      steering: [],
      publicMapResearchContext: [],
      progressRevision: 0,
      terminal: null,
    };
    const input = {
      workId: "family-work-workspace-email",
      objective: "Email the school an enrollment status update and ask them to confirm it is current.",
      visibility: "private" as const,
      ownerAdultId: "adult-1",
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

  test("durable work lists Calendars and reads a selected reference across checkpoints", async () => {
    const calendarRef = "calendar-school";
    const calendarWindowArguments = {
      timeMin: "2026-08-28T00:00:00.000Z",
      timeMax: "2026-08-29T00:00:00.000Z",
      limit: 20,
      scope: "selected",
      calendarRefs: [calendarRef],
    };
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
        parse() {
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
      browserSession: null,
      continuationItems: [],
      pendingCall: null,
      steering: [],
      publicMapResearchContext: [],
      progressRevision: 0,
      terminal: null,
    };
    const input = {
      workId: "family-work-calendar",
      objective: "Check the School Calendar for tomorrow's schedule.",
      visibility: "private" as const,
      ownerAdultId: "adult-1",
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
        limit: number;
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
        };
      },
    };

    const listPlanned = await reasoner.continueFamilyWork(input, reads);
    if (listPlanned.kind !== "continue") throw new Error("Calendar listing was not planned");
    const listed = await reasoner.continueFamilyWork({ ...input, state: listPlanned.state }, reads);
    if (listed.kind !== "continue") throw new Error("Calendar listing was not executed");
    expect(JSON.stringify(listed.state.continuationItems)).toContain(calendarRef);

    const windowPlanned = await reasoner.continueFamilyWork({ ...input, state: listed.state }, reads);
    if (windowPlanned.kind !== "continue") throw new Error("Calendar window was not planned");
    expect(windowPlanned.state).toMatchObject({
      phase: "tool_pending",
      pendingCall: { callId: "read-work-calendar", name: "read_calendar_window" },
    });
    const transientRead = await reasoner.continueFamilyWork({ ...input, state: windowPlanned.state }, reads);
    if (transientRead.kind !== "continue") throw new Error("Calendar transient failure was not settled");
    expect(transientRead.state.continuationItems).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: "function_call_output",
          output: expect.stringContaining('"retryable":true'),
        }),
      ]),
    );
    const retryPlanned = await reasoner.continueFamilyWork({ ...input, state: transientRead.state }, reads);
    if (retryPlanned.kind !== "continue") throw new Error("Calendar retry was not planned");
    expect(retryPlanned.state).toMatchObject({
      phase: "tool_pending",
      pendingCall: { callId: "read-work-calendar-retry", name: "read_calendar_window" },
    });
    const windowRead = await reasoner.continueFamilyWork({ ...input, state: retryPlanned.state }, reads);
    if (windowRead.kind !== "continue") throw new Error("Calendar window was not read");
    const terminal = await reasoner.continueFamilyWork({ ...input, state: windowRead.state }, reads);

    expect(calendarInputs).toEqual([
      expect.objectContaining({ scope: "selected", calendarRefs: [calendarRef] }),
      expect.objectContaining({ scope: "selected", calendarRefs: [calendarRef] }),
    ]);
    expect(terminal).toMatchObject({
      kind: "terminal",
      outcome: "succeeded",
      text: expect.stringContaining("Back-to-school night"),
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
                    output: [
                      functionCall("research-call", "research_public_web", {
                        query: "Junior's Times Square current opening hours",
                      }),
                    ],
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
    },
    facts: [],
    followUp: null,
    reminder: null,
    familyWork: null,
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
    values: [],
    checked: null,
    key: null,
    direction: null,
    milliseconds: null,
    compact: true,
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

function publicPageResult(url: string, title: string, text: string) {
  return {
    requestedUrl: url,
    finalUrl: url,
    kind: "html" as const,
    title,
    filename: null,
    text,
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
