import { MAX_IMAGE_BYTES, MAX_PDF_BYTES } from "@florence/artifacts";
import { APIConnectionError, APIError, InternalServerError, OpenAI, RateLimitError } from "openai";
import { zodTextFormat } from "openai/helpers/zod";
import type {
  FunctionTool,
  ResponseInput,
  ResponseInputItem,
  ResponseOutputItem,
} from "openai/resources/responses/responses";
import { z } from "zod";

const opaqueId = z.string().trim().min(1).max(500);
const shortText = z.string().trim().min(1).max(2_000);
const timestamp = z
  .string()
  .max(100)
  .refine((value) => Number.isFinite(Date.parse(value)), "Invalid timestamp");
const calendarInstant = z
  .string()
  .max(100)
  .regex(
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/,
    "Calendar time must include Z or a UTC offset",
  )
  .refine((value) => Number.isFinite(Date.parse(value)), "Invalid Calendar time");
const sourceIds = z.array(opaqueId).min(1);
const currentImageSchema = z
  .object({
    assetId: opaqueId,
    mimeType: z.enum(["image/jpeg", "image/png", "image/webp"]),
  })
  .strict();
const currentPdfSchema = z
  .object({
    documentId: opaqueId,
    filename: z.string().trim().min(1).max(500),
    mimeType: z.literal("application/pdf"),
    contentDigest: z.string().regex(/^[0-9a-f]{64}$/),
  })
  .strict();
const repliedMessageSchema = z
  .object({
    sourceId: opaqueId,
    senderName: z.string().trim().min(1).max(500),
    text: z.string().trim().min(1).max(20_000),
    occurredAt: timestamp,
  })
  .strict();

export const florenceSourceSchema = z
  .object({
    sourceId: opaqueId,
    recordId: opaqueId.nullable(),
    kind: z.enum(["message", "gmail", "calendar", "memory", "document"]),
    visibility: z.enum(["shared", "adult_private"]),
    label: z.string().trim().min(1).max(500),
    occurredAt: timestamp.nullable(),
    text: z.string().trim().min(1).max(50_000),
  })
  .strict();

const calendarEventSchema = z
  .object({
    title: z.string().trim().min(1).max(1_000),
    startsAt: calendarInstant,
    endsAt: calendarInstant,
    timeZone: z.string().trim().min(1).max(100),
    location: z.string().trim().min(1).max(1_000).nullable(),
  })
  .strict();

const calendarWindowEventSchema = z
  .object({
    title: z.string().trim().min(1).max(500).nullable(),
    startsAt: calendarInstant,
    endsAt: calendarInstant,
    allDay: z.boolean(),
  })
  .strict();

const calendarWindowReadSchema = z
  .object({
    status: z.enum(["complete", "truncated", "unavailable"]),
    events: z.array(calendarWindowEventSchema).max(50),
  })
  .strict();

export const florenceReasonerInputSchema = z
  .object({
    household: z
      .object({
        householdId: opaqueId,
        name: z.string().trim().min(1).max(500),
        timeZone: z.string().trim().min(1).max(100),
        adultNames: z.array(z.string().trim().min(1).max(500)).min(1).max(2),
        familyProfile: z.string().trim().max(20_000),
      })
      .strict(),
    audience: z.enum(["private", "group"]),
    currentAdultId: opaqueId,
    currentMessage: z
      .object({
        sourceId: opaqueId,
        senderName: z.string().trim().min(1).max(500),
        moveKind: z.enum(["message", "reply", "reaction"]),
        text: z.string().trim().min(1).max(20_000),
        occurredAt: timestamp,
        images: z.array(currentImageSchema).max(10),
        pdfs: z.array(currentPdfSchema).max(3).optional(),
        replyTo: repliedMessageSchema.nullable(),
      })
      .strict(),
    recentMessages: z
      .array(
        z
          .object({
            sourceId: opaqueId,
            senderName: z.string().trim().min(1).max(500),
            text: z.string().trim().min(1).max(20_000),
            occurredAt: timestamp,
          })
          .strict(),
      )
      .max(24),
    visibleSources: z.array(florenceSourceSchema).max(50),
    pendingFollowUps: z.array(
      z
        .object({
          followUpId: opaqueId,
          at: timestamp,
          text: shortText,
          sourceIds,
        })
        .strict(),
    ),
    pendingCalendarOffers: z.array(
      z
        .object({
          proposalId: opaqueId,
          connectionId: opaqueId,
          event: calendarEventSchema,
          sourceIds,
        })
        .strict(),
    ),
    googleConnections: z.array(
      z
        .object({
          connectionId: opaqueId,
          emailLabel: z.string().trim().min(1).max(500),
        })
        .strict(),
    ),
  })
  .strict();

const factDecisionSchema = z.discriminatedUnion("operation", [
  z
    .object({
      operation: z.literal("remember"),
      factId: z.null(),
      statement: shortText,
      sourceIds,
    })
    .strict(),
  z
    .object({
      operation: z.literal("correct"),
      factId: opaqueId,
      statement: shortText,
      sourceIds,
    })
    .strict(),
  z
    .object({
      operation: z.literal("forget"),
      factId: opaqueId,
      statement: z.null(),
      sourceIds,
    })
    .strict(),
]);

const followUpDecisionSchema = z.discriminatedUnion("operation", [
  z
    .object({
      operation: z.literal("schedule"),
      followUpId: z.null(),
      at: timestamp,
      text: shortText,
      sourceIds,
    })
    .strict(),
  z
    .object({
      operation: z.literal("cancel"),
      followUpId: opaqueId,
      at: z.null(),
      text: z.null(),
      sourceIds,
    })
    .strict(),
]);

const calendarDecisionSchema = z.discriminatedUnion("mode", [
  z
    .object({
      mode: z.literal("offer"),
      proposalId: z.null(),
      connectionId: opaqueId,
      event: calendarEventSchema,
      sourceIds,
    })
    .strict(),
  z
    .object({
      mode: z.literal("direct"),
      proposalId: z.null(),
      connectionId: opaqueId,
      event: calendarEventSchema,
      sourceIds,
    })
    .strict(),
  z
    .object({
      mode: z.literal("approve"),
      proposalId: opaqueId,
      connectionId: z.null(),
      event: z.null(),
      sourceIds,
    })
    .strict(),
]);

export const florenceDecisionSchema = z
  .object({
    conversation: z
      .object({
        replyToCurrentMessage: z.boolean(),
        reaction: z.enum(["love", "like", "dislike", "laugh", "emphasize", "question"]).nullable(),
        bubbles: z
          .array(
            z
              .object({
                text: shortText,
                delayMs: z.number().int().min(0).max(5_000),
              })
              .strict(),
          )
          .max(3),
      })
      .strict(),
    facts: z.array(factDecisionSchema),
    followUp: followUpDecisionSchema.nullable(),
    calendar: calendarDecisionSchema.nullable(),
  })
  .strict();

export type FlorenceSource = z.infer<typeof florenceSourceSchema>;
export type FlorenceReasonerInput = z.infer<typeof florenceReasonerInputSchema>;
export type FlorenceDecision = z.infer<typeof florenceDecisionSchema>;
export type FlorenceCalendarWindowRead = {
  status: "complete" | "truncated" | "unavailable";
  events: readonly z.infer<typeof calendarWindowEventSchema>[];
};

type CalendarReadCoverage = {
  connectionId: string;
  timeMin: number;
  timeMax: number;
};

export interface FlorenceReadTools {
  searchGmail(input: {
    connectionId: string;
    query: string;
    limit: number;
  }): Promise<readonly FlorenceSource[]>;
  searchFamilyMemory(input: { query: string; limit: number }): Promise<readonly FlorenceSource[]>;
  readCalendarWindow(input: {
    connectionId: string;
    timeMin: string;
    timeMax: string;
    limit: number;
  }): Promise<FlorenceCalendarWindowRead>;
  readSource(input: { sourceId: string }): Promise<FlorenceSource | null>;
  readCurrentImage(input: z.infer<typeof currentImageSchema>): Promise<{
    mimeType: "image/jpeg" | "image/png" | "image/webp";
    bytes: Uint8Array;
  }>;
  readCurrentPdf?(input: z.infer<typeof currentPdfSchema>): Promise<{
    mimeType: "application/pdf";
    bytes: Uint8Array;
  }>;
}

export type FlorenceReasonerOptions = {
  apiKey: string;
  model: string;
  timeoutMs?: number;
  maxOutputTokens?: number;
};

export type FlorenceReasonerErrorCode =
  | "configuration"
  | "rate_limited"
  | "transient"
  | "invalid_output"
  | "unsafe_read"
  | "rejected";

export class FlorenceReasonerError extends Error {
  readonly retryable: boolean;

  constructor(
    readonly code: FlorenceReasonerErrorCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "FlorenceReasonerError";
    this.retryable = code === "rate_limited" || code === "transient";
  }
}

const INSTRUCTIONS = `You are Florence, a warm, capable family assistant inside iMessage.

Act like an excellent participant in the family thread, not a workflow engine. Use short, natural language. A useful turn may be silence, a reaction, one bubble, or at most three paced bubbles. Do not narrate internal work. Reply inline only when it materially disambiguates what you are answering.

Use currentMessage.replyTo as the exact message the parent replied to when it is present. Use current-message images and PDFs directly when attached. An attached PDF's documentId is its source ID. Use read tools naturally when the answer depends on family memory or the current adult's Google context. Gmail and Calendar are private to their owning adult and are never available in a group turn. Never expose an adult_private source in the group. Calendar window results are ephemeral scheduling context: never cite them as sources or turn their contents into memory. Every fact change, follow-up, and Calendar decision must cite source IDs you actually received.

A currentMessage with moveKind reaction is affect or acknowledgement only. Never interpret a reaction as an approval, confirmation, completion, cancellation, instruction, factual correction, memory request, scheduling request, or Calendar authority. For a reaction turn, facts must be empty and followUp and calendar must be null; use natural silence or a conversational response.

Facts from a group turn are household-visible. Facts from a private turn are always private, including a private correction of an existing household fact. A private turn cannot forget a household fact. Never claim that a private correction or deletion was shared; the parent must make shared changes in the family group.

Calendar decisions are proposals, never claims that a write happened. Before returning offer or direct, read a window on that same connection which completely covers the proposed event; if the read is truncated or unavailable, do not offer or write and explain briefly. Use direct only for an explicit, complete request from the current adult that needs no judgment. Use offer when Florence should show the exact event and ask. Use approve only when the current message unambiguously approves one listed pending proposal. Never reconstruct or alter an approved proposal.

Prefer useful silence over filler, acknowledgements, status chatter, or repeating the user's words.`;

const gmailArguments = z
  .object({
    connectionId: opaqueId,
    query: z.string().trim().min(1).max(500),
    limit: z.number().int().min(1).max(10),
  })
  .strict();
const memoryArguments = z
  .object({ query: z.string().trim().min(1).max(500), limit: z.number().int().min(1).max(10) })
  .strict();
const sourceArguments = z.object({ sourceId: opaqueId }).strict();
const calendarArguments = z
  .object({
    connectionId: opaqueId,
    timeMin: calendarInstant,
    timeMax: calendarInstant,
    limit: z.number().int().min(1).max(50),
  })
  .strict();

const MEMORY_TOOL: FunctionTool = {
  type: "function",
  name: "search_family_memory",
  description: "Search source-linked family memory visible in this conversation.",
  strict: true,
  parameters: {
    type: "object",
    additionalProperties: false,
    properties: {
      query: { type: "string", minLength: 1, maxLength: 500 },
      limit: { type: "integer", minimum: 1, maximum: 10 },
    },
    required: ["query", "limit"],
  },
};

const SOURCE_TOOL: FunctionTool = {
  type: "function",
  name: "read_source",
  description: "Read a source already referenced in the supplied turn or a search result.",
  strict: true,
  parameters: {
    type: "object",
    additionalProperties: false,
    properties: { sourceId: { type: "string", minLength: 1, maxLength: 500 } },
    required: ["sourceId"],
  },
};

const GMAIL_TOOL: FunctionTool = {
  type: "function",
  name: "search_gmail",
  description: "Search the current adult's connected Gmail when private email context is needed.",
  strict: true,
  parameters: {
    type: "object",
    additionalProperties: false,
    properties: {
      connectionId: { type: "string", minLength: 1, maxLength: 500 },
      query: { type: "string", minLength: 1, maxLength: 500 },
      limit: { type: "integer", minimum: 1, maximum: 10 },
    },
    required: ["connectionId", "query", "limit"],
  },
};

const CALENDAR_TOOL: FunctionTool = {
  type: "function",
  name: "read_calendar_window",
  description:
    "Privately read a bounded window from the current adult's primary Google Calendar before proposing or directly creating an event.",
  strict: true,
  parameters: {
    type: "object",
    additionalProperties: false,
    properties: {
      connectionId: { type: "string", minLength: 1, maxLength: 500 },
      timeMin: { type: "string", minLength: 1, maxLength: 100 },
      timeMax: { type: "string", minLength: 1, maxLength: 100 },
      limit: { type: "integer", minimum: 1, maximum: 50 },
    },
    required: ["connectionId", "timeMin", "timeMax", "limit"],
  },
};

export class FlorenceReasoner {
  readonly #client: OpenAI;
  readonly #model: string;
  readonly #maxOutputTokens: number;

  constructor(options: FlorenceReasonerOptions, client?: OpenAI) {
    if (!options.apiKey.trim()) throw configuration("OPENAI_API_KEY is required");
    if (!options.model.trim()) throw configuration("FLORENCE_OPENAI_MODEL is required");
    const timeout = positiveInteger(options.timeoutMs ?? 30_000, "OpenAI timeout");
    this.#maxOutputTokens = positiveInteger(options.maxOutputTokens ?? 4_000, "OpenAI output limit");
    this.#model = options.model;
    this.#client = client ?? new OpenAI({ apiKey: options.apiKey, timeout, maxRetries: 0 });
  }

  async decide(untrustedInput: FlorenceReasonerInput, reads: FlorenceReadTools): Promise<FlorenceDecision> {
    const input = florenceReasonerInputSchema.parse(untrustedInput);
    if (
      input.audience === "group" &&
      (input.visibleSources.some((source) => source.visibility !== "shared") ||
        input.googleConnections.length > 0 ||
        input.pendingCalendarOffers.length > 0)
    ) {
      throw unsafeRead("Private adult context cannot enter a group turn");
    }
    const knownSources = new Set([
      input.currentMessage.sourceId,
      ...(input.currentMessage.replyTo ? [input.currentMessage.replyTo.sourceId] : []),
      ...(input.currentMessage.pdfs ?? []).map((document) => document.documentId),
      ...input.recentMessages.map((message) => message.sourceId),
      ...input.visibleSources.map((source) => source.sourceId),
      ...input.pendingFollowUps.flatMap((followUp) => followUp.sourceIds),
      ...input.pendingCalendarOffers.flatMap((offer) => offer.sourceIds),
    ]);
    const knownFacts = new Set(
      input.visibleSources.flatMap((source) =>
        source.kind === "memory" && source.recordId ? [source.recordId] : [],
      ),
    );
    const calendarReads: CalendarReadCoverage[] = [];
    const tools: FunctionTool[] =
      input.currentMessage.moveKind === "reaction" ? [] : [MEMORY_TOOL, SOURCE_TOOL];
    if (
      input.currentMessage.moveKind !== "reaction" &&
      input.audience === "private" &&
      input.googleConnections.length > 0
    ) {
      tools.push(GMAIL_TOOL, CALENDAR_TOOL);
    }
    const currentImages = await Promise.all(
      input.currentMessage.images.map(async (image) => {
        const read = await reads.readCurrentImage(image);
        if (
          read.mimeType !== image.mimeType ||
          read.bytes.byteLength < 1 ||
          read.bytes.byteLength > MAX_IMAGE_BYTES
        ) {
          throw unsafeRead("The current-message image did not match its authorized reference");
        }
        return {
          type: "input_image" as const,
          detail: "auto" as const,
          image_url: `data:${read.mimeType};base64,${Buffer.from(read.bytes).toString("base64")}`,
        };
      }),
    );
    const currentPdfs = await Promise.all(
      (input.currentMessage.pdfs ?? []).map(async (document) => {
        if (!reads.readCurrentPdf) throw unsafeRead("Current-message PDF reading is unavailable");
        const read = await reads.readCurrentPdf(document);
        if (
          read.mimeType !== document.mimeType ||
          read.bytes.byteLength < 1 ||
          read.bytes.byteLength > MAX_PDF_BYTES
        ) {
          throw unsafeRead("The current-message PDF did not match its authorized reference");
        }
        return {
          type: "input_file" as const,
          filename: document.filename,
          file_data: Buffer.from(read.bytes).toString("base64"),
        };
      }),
    );
    const modelInput: ResponseInput = [
      {
        role: "user",
        content: [{ type: "input_text", text: JSON.stringify(input) }, ...currentImages, ...currentPdfs],
      },
    ];

    try {
      for (let turn = 0; turn < 5; turn += 1) {
        const response = await this.#client.responses.parse({
          model: this.#model,
          store: false,
          include: ["reasoning.encrypted_content"],
          instructions: INSTRUCTIONS,
          input: modelInput,
          tools,
          parallel_tool_calls: false,
          max_tool_calls: 4,
          max_output_tokens: this.#maxOutputTokens,
          text: { format: zodTextFormat(florenceDecisionSchema, "florence_decision") },
        });
        const calls = response.output.filter((item) => item.type === "function_call");
        if (input.currentMessage.moveKind === "reaction" && calls.length > 0) {
          throw unsafeRead("Reaction turns cannot call read tools");
        }
        if (calls.length === 0) {
          if (response.output_parsed === null) throw invalidOutput("OpenAI returned no Florence decision");
          return validateDecision(response.output_parsed, input, knownSources, knownFacts, calendarReads);
        }
        modelInput.push(...continuationItems(response.output));
        for (const call of calls) {
          modelInput.push({
            type: "function_call_output",
            call_id: call.call_id,
            output: await runReadTool(
              call.name,
              call.arguments,
              input,
              reads,
              knownSources,
              knownFacts,
              calendarReads,
            ),
          });
        }
      }
      throw invalidOutput("OpenAI exceeded Florence's read-tool turn limit");
    } catch (error) {
      throw normalizeError(error);
    }
  }
}

export function createFlorenceReasonerFromEnv(env: NodeJS.ProcessEnv = process.env): FlorenceReasoner {
  const timeoutMs = optionalPositiveInteger(env.FLORENCE_MODEL_TIMEOUT_MS);
  const maxOutputTokens = optionalPositiveInteger(env.FLORENCE_MODEL_MAX_OUTPUT_TOKENS);
  return new FlorenceReasoner({
    apiKey: env.OPENAI_API_KEY ?? "",
    model: env.FLORENCE_OPENAI_MODEL ?? "",
    ...(timeoutMs === undefined ? {} : { timeoutMs }),
    ...(maxOutputTokens === undefined ? {} : { maxOutputTokens }),
  });
}

async function runReadTool(
  name: string,
  rawArguments: string,
  input: FlorenceReasonerInput,
  reads: FlorenceReadTools,
  knownSources: Set<string>,
  knownFacts: Set<string>,
  calendarReads: CalendarReadCoverage[],
): Promise<string> {
  if (name === "read_calendar_window") {
    if (input.audience !== "private") throw unsafeRead("Calendar cannot be read from a group turn");
    const args = calendarArguments.parse(JSON.parse(rawArguments));
    if (!input.googleConnections.some((connection) => connection.connectionId === args.connectionId)) {
      throw unsafeRead("Calendar connection is not owned by the current adult");
    }
    const timeMin = Date.parse(args.timeMin);
    const timeMax = Date.parse(args.timeMax);
    if (timeMax <= timeMin || timeMax - timeMin > 31 * 24 * 60 * 60_000) {
      throw unsafeRead("Calendar read window is invalid");
    }
    const read = calendarWindowReadSchema.parse(await reads.readCalendarWindow(args));
    if (read.status === "complete") {
      calendarReads.push({ connectionId: args.connectionId, timeMin, timeMax });
    }
    const output = JSON.stringify({
      connectionId: args.connectionId,
      timeMin: args.timeMin,
      timeMax: args.timeMax,
      ...read,
    });
    if (output.length > 100_000) throw unsafeRead("Calendar output exceeded the safe context limit");
    return output;
  }
  let sources: readonly FlorenceSource[];
  if (name === "search_gmail") {
    if (input.audience !== "private") throw unsafeRead("Gmail cannot be read from a group turn");
    const args = gmailArguments.parse(JSON.parse(rawArguments));
    if (!input.googleConnections.some((connection) => connection.connectionId === args.connectionId)) {
      throw unsafeRead("Gmail connection is not owned by the current adult");
    }
    sources = await reads.searchGmail(args);
    if (sources.some((source) => source.visibility !== "adult_private" || source.kind !== "gmail")) {
      throw unsafeRead("Gmail returned incorrectly scoped evidence");
    }
  } else if (name === "search_family_memory") {
    const args = memoryArguments.parse(JSON.parse(rawArguments));
    sources = await reads.searchFamilyMemory(args);
  } else if (name === "read_source") {
    const args = sourceArguments.parse(JSON.parse(rawArguments));
    if (!knownSources.has(args.sourceId)) throw unsafeRead("OpenAI requested an unreferenced source");
    const source = await reads.readSource(args);
    sources = source ? [source] : [];
  } else {
    throw unsafeRead("OpenAI requested an unknown read tool");
  }

  const parsed = z.array(florenceSourceSchema).max(10).parse(sources);
  if (input.audience === "group" && parsed.some((source) => source.visibility !== "shared")) {
    throw unsafeRead("A private source cannot enter a group turn");
  }
  for (const source of parsed) knownSources.add(source.sourceId);
  for (const source of parsed) {
    if (source.kind === "memory" && source.recordId) knownFacts.add(source.recordId);
  }
  const output = JSON.stringify({ sources: parsed });
  if (output.length > 100_000) throw unsafeRead("Read-tool output exceeded the safe context limit");
  return output;
}

function validateDecision(
  decision: FlorenceDecision,
  input: FlorenceReasonerInput,
  knownSources: ReadonlySet<string>,
  knownFacts: ReadonlySet<string>,
  calendarReads: readonly CalendarReadCoverage[],
): FlorenceDecision {
  if (decision.conversation.replyToCurrentMessage && decision.conversation.bubbles.length === 0) {
    throw invalidOutput("OpenAI requested an inline reply without a message");
  }
  for (const ids of [
    ...decision.facts.map((fact) => fact.sourceIds),
    ...(decision.followUp ? [decision.followUp.sourceIds] : []),
    ...(decision.calendar ? [decision.calendar.sourceIds] : []),
  ]) {
    if (ids.some((sourceId) => !knownSources.has(sourceId))) {
      throw invalidOutput("OpenAI cited a source it did not receive");
    }
  }
  for (const fact of decision.facts) {
    if (fact.operation !== "remember" && !knownFacts.has(fact.factId)) {
      throw invalidOutput("OpenAI changed a fact it did not receive");
    }
  }
  if (
    decision.followUp?.operation === "cancel" &&
    !input.pendingFollowUps.some((followUp) => followUp.followUpId === decision.followUp?.followUpId)
  ) {
    throw invalidOutput("OpenAI cancelled an unknown follow-up");
  }
  if (decision.calendar?.mode === "approve") {
    const pending = input.pendingCalendarOffers.find(
      (offer) => offer.proposalId === decision.calendar?.proposalId,
    );
    if (!pending) throw invalidOutput("OpenAI approved an unknown Calendar proposal");
    if (!decision.calendar.sourceIds.includes(input.currentMessage.sourceId)) {
      throw invalidOutput("Calendar approval did not cite the current approval message");
    }
    decision.calendar.sourceIds = [...new Set([...pending.sourceIds, ...decision.calendar.sourceIds])];
  }
  if (
    decision.calendar &&
    decision.calendar.mode !== "approve" &&
    !input.googleConnections.some((connection) => connection.connectionId === decision.calendar?.connectionId)
  ) {
    throw invalidOutput("OpenAI selected an unavailable Google connection");
  }
  if (
    decision.calendar?.mode === "direct" &&
    !decision.calendar.sourceIds.includes(input.currentMessage.sourceId)
  ) {
    throw invalidOutput("A direct Calendar action must cite the current explicit request");
  }
  if (
    decision.calendar &&
    decision.calendar.mode !== "approve" &&
    Date.parse(decision.calendar.event.endsAt) <= Date.parse(decision.calendar.event.startsAt)
  ) {
    throw invalidOutput("OpenAI returned an invalid Calendar interval");
  }
  if (decision.calendar?.mode === "offer" || decision.calendar?.mode === "direct") {
    const startsAt = Date.parse(decision.calendar.event.startsAt);
    const endsAt = Date.parse(decision.calendar.event.endsAt);
    if (
      !calendarReads.some(
        (read) =>
          read.connectionId === decision.calendar?.connectionId &&
          read.timeMin <= startsAt &&
          read.timeMax >= endsAt,
      )
    ) {
      throw invalidOutput("A Calendar offer or direct action requires a complete covering Calendar read");
    }
  }
  if (input.audience === "group" && decision.calendar !== null) {
    throw invalidOutput("Calendar writes must originate in a private adult turn");
  }
  return decision;
}

function continuationItems(output: readonly ResponseOutputItem[]): ResponseInputItem[] {
  return output.filter(
    (item): item is Extract<ResponseOutputItem, { type: "message" | "function_call" | "reasoning" }> =>
      item.type === "message" || item.type === "function_call" || item.type === "reasoning",
  );
}

function normalizeError(error: unknown): FlorenceReasonerError {
  if (error instanceof FlorenceReasonerError) return error;
  if (error instanceof RateLimitError) {
    return new FlorenceReasonerError("rate_limited", "OpenAI rate limit reached", { cause: error });
  }
  if (error instanceof APIConnectionError || error instanceof InternalServerError) {
    return new FlorenceReasonerError("transient", "Temporary OpenAI request failure", { cause: error });
  }
  if (error instanceof APIError) {
    return new FlorenceReasonerError("rejected", "OpenAI rejected the Florence request", {
      cause: error,
    });
  }
  if (error instanceof z.ZodError || error instanceof SyntaxError) {
    return invalidOutput("OpenAI returned invalid Florence data", error);
  }
  return new FlorenceReasonerError("rejected", "Unexpected Florence reasoning failure", {
    cause: error,
  });
}

function configuration(message: string): FlorenceReasonerError {
  return new FlorenceReasonerError("configuration", message);
}

function invalidOutput(message: string, cause?: unknown): FlorenceReasonerError {
  return new FlorenceReasonerError("invalid_output", message, cause === undefined ? undefined : { cause });
}

function unsafeRead(message: string): FlorenceReasonerError {
  return new FlorenceReasonerError("unsafe_read", message);
}

function positiveInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw configuration(`${label} must be positive`);
  return value;
}

function optionalPositiveInteger(value: string | undefined): number | undefined {
  return value === undefined ? undefined : positiveInteger(Number(value), "Model limit");
}
