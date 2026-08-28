import { describe, expect, test } from "vitest";
import type { CapabilityLifecycleEvent } from "./capability-lifecycle.js";
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
  async admitCapability() {
    return true;
  },
  settleSources() {},
  settleCalendarRead() {},
};

describe("Florence reasoner capability cutover", () => {
  test("all foreground function calls use one source-bearing lifecycle and cue only after admission", async () => {
    const requests: Record<string, unknown>[] = [];
    const events: CapabilityLifecycleEvent[] = [];
    const calls = [
      functionCall("memory-call", "search_family_memory", { query: "school", limit: 3 }),
      functionCall("source-call", "read_source", { sourceId: "turn-1" }),
      functionCall("public-call", "research_public_web", {}),
      functionCall("gmail-call", "search_gmail", { query: "pickup", limit: 3 }),
      functionCall("calendar-call", "read_calendar_window", {
        timeMin: "2026-08-28T16:00:00.000Z",
        timeMax: "2026-08-28T18:00:00.000Z",
        limit: 10,
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
          return [source("gmail-1", "gmail", "adult_private")];
        },
        async readCalendarWindow() {
          readCounts.calendar += 1;
          return { status: "complete", events: [] };
        },
        async readCurrentImage() {
          throw new Error("No image was authorized");
        },
      },
      undefined,
      {
        onLifecycleEvent(event) {
          events.push(event);
        },
      },
    );

    expect(result.conversation.bubbles[0]?.text).toBe("Done.");
    expect(readCounts).toEqual({ memory: 1, source: 1, gmail: 1, calendar: 1 });
    const toolNames = ((requests[0]?.tools as { name: string }[]) ?? []).map((tool) => tool.name);
    expect(toolNames).toEqual([
      "read_calendar_window",
      "read_source",
      "research_public_web",
      "search_family_memory",
      "search_gmail",
    ]);
    expect(JSON.stringify(requests[0])).not.toContain("connectionId");
    expect(events.filter((event) => event.phase === "requested")).toHaveLength(5);
    expect(events.findIndex((event) => event.phase === "running")).toBeGreaterThan(
      events.map((event) => event.phase).lastIndexOf("admitted"),
    );
    const secondInput = JSON.stringify(requests[1]?.input);
    for (const call of calls) {
      expect(secondInput).toContain(call.call_id);
    }
    const envelopes = functionOutputEnvelopes(requests[1]);
    expect(envelopes).toHaveLength(5);
    for (const envelope of envelopes) {
      expect(envelope.outcome).toBe("succeeded");
      expect(envelope.sources.length).toBeGreaterThan(0);
      expect(JSON.stringify(envelope).length).toBeLessThan(120_000);
    }
  });

  test("truncated, malformed, and unknown calls never execute and remain model-visible for recovery", async () => {
    const requests: Record<string, unknown>[] = [];
    const events: CapabilityLifecycleEvent[] = [];
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
          return [];
        },
        async readCalendarWindow() {
          return { status: "complete", events: [] };
        },
        async readCurrentImage() {
          throw new Error("No image was authorized");
        },
      },
      undefined,
      {
        onLifecycleEvent(event) {
          events.push(event);
        },
      },
    );

    expect(memoryReads).toBe(0);
    expect(events.some((event) => event.phase === "admitted" || event.phase === "running")).toBe(false);
    expect(functionOutputEnvelopes(requests[1])[0]?.error?.code).toBe("truncated_model_output");
    const rejected = functionOutputEnvelopes(requests[2]).slice(-2);
    expect(rejected.map((envelope) => envelope.error?.code)).toEqual([
      "unknown_or_unavailable_capability",
      "invalid_arguments",
    ]);
    expect(rejected.every((envelope) => envelope.sources.length > 0)).toBe(true);
  });

  test("both private Gmail attachment loops use the registry without exposing connection IDs", async () => {
    const gmail = privateGmailSource();
    const requests: Record<string, unknown>[] = [];
    const events: CapabilityLifecycleEvent[] = [];
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
          attachmentId: gmail.attachments[0]?.attachmentId ?? "missing",
          filename: gmail.attachments[0]?.filename ?? "missing.pdf",
          mimeType: "application/pdf" as const,
          bytes: new Uint8Array(Buffer.from("%PDF-")),
        };
      },
    };
    const presentation = {
      onLifecycleEvent(event: CapabilityLifecycleEvent) {
        events.push(event);
      },
    };

    await reasoner.classifyPrivateGoogleBatch(privateBatchInput(gmail), reads, undefined, presentation);
    await reasoner.assessGoogleChanges(privateAssessmentInput(gmail), reads, undefined, presentation);

    expect(attachmentReads).toBe(2);
    expect(events.filter((event) => event.phase === "running")).toHaveLength(2);
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
    visibleInterests: [],
    pendingCalendarOffers: [],
    googleConnections: [
      { emailLabel: "Personal Google", calendarAvailable: true, kind: "personal", writesEnabled: false },
    ],
  };
}

function ordinaryDecision(input: { researchUrls?: string[] } = {}): FlorenceDecision {
  return {
    policy: { retain: true, schedule: true, stopMessaging: false },
    conversation: {
      replyToCurrentMessage: false,
      reaction: null,
      bubbles: [{ text: "Done.", delayMs: 0 }],
    },
    facts: [],
    followUp: null,
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
    attachmentId: "attachment-1",
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
  return ((request?.input as { type?: string; output?: unknown }[]) ?? []).filter(
    (item) => item.type === "function_call_output",
  );
}

function functionOutputEnvelopes(request: Record<string, unknown> | undefined) {
  return functionOutputs(request).map(
    (item) =>
      JSON.parse(typeof item.output === "string" ? item.output : "{}") as {
        outcome: string;
        error: { code: string } | null;
        sources: unknown[];
      },
  );
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
    attachments: [
      {
        attachmentId: "attachment-1",
        filename: "form.pdf",
        mimeType: "application/pdf" as const,
        sizeBytes: 5,
      },
    ],
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
      return [];
    },
    async readCalendarWindow() {
      return { status: "complete" as const, events: [] };
    },
    async readCurrentImage() {
      throw new Error("No image was authorized");
    },
  };
}
