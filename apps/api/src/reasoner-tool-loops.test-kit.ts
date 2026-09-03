import { createHash } from "node:crypto";
import type { FamilyWorkOriginContext } from "@florence/database";
import type { FlorenceBrowserObservation } from "./browser.js";
import type {
  FlorenceDecision,
  FlorenceGoogleChangesAssessmentInput,
  FlorencePrivateGoogleBatchInput,
  FlorenceReasonerInput,
} from "./reasoner.js";
import type { FlorenceTelephonyResult } from "./telephony.js";

export const NOW = "2026-08-27T20:00:00.000Z";
export const PUBLIC_URL = "https://example.com/current-result";

export function completionOutputDigest(value: unknown): string {
  const canonical = (item: unknown): string => {
    if (item === null || typeof item !== "object") return JSON.stringify(item);
    if (Array.isArray(item)) return `[${item.map((entry) => canonical(entry)).join(",")}]`;
    return `{${Object.entries(item)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, field]) => `${JSON.stringify(key)}:${canonical(field)}`)
      .join(",")}}`;
  };
  return createHash("sha256").update(canonical(value)).digest("hex");
}
export const admittedReadAccounting = {
  settleSources() {},
};

export function familyWorkOrigin(text: string, speaker = "adult-1"): FamilyWorkOriginContext {
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

export function foregroundInput(): FlorenceReasonerInput {
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

export function ordinaryDecision(
  input: {
    participation?: FlorenceDecision["conversation"]["participation"];
    bubbleText?: string;
    researchUrls?: string[];
  } = {},
): FlorenceDecision {
  const participation = input.participation ?? "respond";
  return {
    policy:
      participation === "observe" ? { retain: false, schedule: false } : { retain: true, schedule: true },
    conversation: {
      participation,
      replyToCurrentMessage: false,
      reaction: null,
      bubbles: participation === "observe" ? [] : [{ text: input.bubbleText ?? "Done.", delayMs: 0 }],
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
    secondAdultPlan: null,
    householdUpdate: null,
    webAccessPath: null,
    researchUrls: input.researchUrls ?? null,
  };
}

export function browserArguments(operation: string, overrides: Record<string, unknown> = {}) {
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

export function browserObservation(
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

export function telephonyResult(
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

export function functionCall(callId: string, name: string, args: object) {
  return {
    id: `item-${callId}`,
    type: "function_call" as const,
    call_id: callId,
    name,
    arguments: JSON.stringify(args),
    status: "completed" as const,
  };
}

export function decisionMessage(id: string, decision: FlorenceDecision) {
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

export function familyWorkResultMessage(id: string, result: object) {
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

export function completedWebSearch(url: string, query = "current result", id = "web-search-1") {
  return {
    id,
    type: "web_search_call" as const,
    status: "completed" as const,
    action: { type: "search" as const, query, sources: [{ type: "url", url }] },
  };
}

export function fakeStream(response: unknown) {
  return {
    async *[Symbol.asyncIterator]() {},
    async finalResponse() {
      return response;
    },
  };
}

export function functionOutputs(request: Record<string, unknown> | undefined) {
  return ((request?.input as { type?: string; call_id?: string; output?: unknown }[]) ?? []).filter(
    (item) => item.type === "function_call_output",
  );
}

export function functionOutputEnvelopes(request: Record<string, unknown> | undefined) {
  return functionOutputs(request).map((item) => ({
    callId: item.call_id,
    ...(JSON.parse(typeof item.output === "string" ? item.output : "{}") as {
      outcome: string;
      output: unknown;
      error: { code: string } | null;
    }),
  }));
}

export function defaultFamilyWorkCompletionReview(request: Record<string, unknown>) {
  if (!JSON.stringify(request.text).includes("florence_family_work_completion_review")) return null;
  const input = request.input as Array<{ content?: Array<{ type?: string; text?: string }> }> | undefined;
  const text = input?.[0]?.content?.find((part) => part.type === "input_text")?.text ?? "{}";
  const context = JSON.parse(text) as {
    taskContext?: { objective?: string };
    proposedResult?: { text?: string | null };
    successfulCapabilityResults?: Array<{ callId?: string; output?: unknown }>;
  };
  const selectedEvidence = context.successfulCapabilityResults?.at(-1);
  const evidenceCallId = selectedEvidence?.callId ?? null;
  const evidencePointer = selectedEvidence ? firstCompletionEvidencePointer(selectedEvidence.output) : null;
  return {
    status: "completed",
    output_parsed: {
      verdict: "verified",
      reason: null,
      condition: context.taskContext?.objective ?? "The requested test objective is complete.",
      basisKind: evidenceCallId ? "capability_evidence" : "reasoned_result",
      summary: context.proposedResult?.text ?? "The requested test objective is complete.",
      evidenceCallIds: evidenceCallId ? [evidenceCallId] : [],
      evidenceSelections:
        evidenceCallId && evidencePointer ? [{ callId: evidenceCallId, pointers: [evidencePointer] }] : [],
    },
    output: [],
  };
}

export function firstCompletionEvidencePointer(value: unknown, prefix = ""): string | null {
  if (value === null || typeof value !== "object") return prefix || null;
  if (Array.isArray(value)) {
    for (const [index, item] of value.entries()) {
      const pointer = firstCompletionEvidencePointer(item, `${prefix}/${index}`);
      if (pointer) return pointer;
    }
    return null;
  }
  for (const [key, item] of Object.entries(value)) {
    const escaped = key.replace(/~/gu, "~0").replace(/\//gu, "~1");
    const pointer = firstCompletionEvidencePointer(item, `${prefix}/${escaped}`);
    if (pointer) return pointer;
  }
  return null;
}

export function privateGmailSource() {
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

export function conversationalGmailSource() {
  return privateGmailSource();
}

export function completeCalendarRead() {
  return {
    status: "complete" as const,
    calendars: [],
    totalCalendarCount: 0,
    calendarCoverage: {
      complete: true,
      observedCalendarCount: 0,
      completeCalendarCount: 0,
      missingCalendarCount: 0,
      unavailableCalendarCount: 0,
      digest: "0".repeat(64),
    },
    events: [],
    totalEventCount: 0,
    nextCursor: null,
  };
}

export function publicPageResult(url: string, title: string, text: string) {
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

export function weatherResult() {
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

export function flightSegment(carrier: string, carrierName: string, flightNumber: string) {
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

export function flightResult(bookingUrl: string, returnedCount = 1) {
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

export function familyProfile() {
  return {
    familyLabel: "Test family",
    timeZone: "America/Los_Angeles",
    adultFirstNames: ["Hari", "Jackson"],
    children: [],
    postalCode: null,
  };
}

export function privateBatchInput(
  gmail: ReturnType<typeof privateGmailSource>,
): FlorencePrivateGoogleBatchInput {
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

export function privateAssessmentInput(
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

export function inertReads() {
  return {
    ...admittedReadAccounting,
    async searchFamilyMemory() {
      return [];
    },
    async readSource() {
      return null;
    },
    async searchGmail() {
      return {
        status: "complete" as const,
        complete: true,
        sources: [],
        nextCursor: null,
      };
    },
    async readCalendarWindow() {
      return completeCalendarRead();
    },
    async readCurrentImage() {
      throw new Error("No image was authorized");
    },
  };
}
