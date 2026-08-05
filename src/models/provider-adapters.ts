import { ChatAnthropic } from "@langchain/anthropic";
import type { BaseChatModel } from "@langchain/core/language_models/chat_models";
import {
  AIMessage,
  BaseMessage,
  type ContentBlock,
  HumanMessage,
  SystemMessage,
  ToolMessage,
} from "@langchain/core/messages";
import { tool } from "@langchain/core/tools";
import type { JSONSchema } from "@langchain/core/utils/json_schema";
import { ChatOpenAI } from "@langchain/openai";
import { z } from "zod";
import type { ModelRouteConfig } from "./config.js";
import {
  type JsonValue,
  JsonValueSchema,
  type ModelCallOptions,
  type ModelCompletionRequest,
  type ModelCompletionResult,
  ModelCompletionResultSchema,
  type ModelMessage,
  type ModelMessagePart,
  type ModelOutputPart,
  type ModelProviderAdapter,
  type ModelRouteReference,
  type ModelToolChoice,
  type ModelToolDefinition,
  ModelToolRequestSchema,
} from "./contracts.js";
import { asModelGatewayError, ModelGatewayError } from "./errors.js";
import { runWithoutModelTracing } from "./no-tracing.js";

export function createProviderAdapter(route: ModelRouteConfig): ModelProviderAdapter {
  return new LangChainProviderAdapter(route);
}

class LangChainProviderAdapter implements ModelProviderAdapter {
  readonly route: ModelRouteReference;
  readonly capabilities;
  readonly #config: ModelRouteConfig;

  constructor(config: ModelRouteConfig) {
    this.#config = config;
    this.route = {
      routeId: config.routeId,
      provider: config.provider,
      model: config.model,
      ...(config.version === undefined ? {} : { version: config.version }),
    };
    this.capabilities = config.capabilities;
  }

  async complete(
    request: ModelCompletionRequest,
    options?: ModelCallOptions,
  ): Promise<ModelCompletionResult> {
    if (options?.signal?.aborted) {
      throw new ModelGatewayError("cancelled");
    }

    const startedAt = performance.now();
    try {
      const model = this.#createModel(request);
      const messages = request.messages.flatMap(toBaseMessages);
      let output: unknown;

      if (request.responseSchema !== undefined) {
        if (request.tools !== undefined && request.tools.length > 0) {
          throw new ModelGatewayError("unsupported_capability");
        }
        const wrappedSchema = z.strictObject({ result: request.responseSchema });
        const structured = model.withStructuredOutput(providerJsonSchema(wrappedSchema), {
          ...(request.responseSchemaName === undefined ? {} : { name: request.responseSchemaName }),
          strict: false,
        });
        const attempts = (this.#config.maxRetries ?? 2) + 1;
        for (let attempt = 0; attempt < attempts; attempt += 1) {
          const attemptMessages =
            attempt === 0
              ? messages
              : [
                  ...messages,
                  new HumanMessage(
                    "Return a fresh structured result that exactly satisfies the supplied schema. Omit unavailable optional fields instead of returning null, and do not invent identifiers or facts.",
                  ),
                ];
          const wrappedOutput = await runWithoutModelTracing(() =>
            structured.invoke(attemptMessages, invocationOptions(options?.signal)),
          );
          const wrapped = wrappedSchema.safeParse(wrappedOutput);
          if (!wrapped.success) continue;
          output = wrapped.data.result;
          const parsed = request.responseSchema.safeParse(output);
          if (!parsed.success || !JsonValueSchema.safeParse(parsed.data).success) continue;

          return ModelCompletionResultSchema.parse({
            content: [{ type: "structured_result", value: parsed.data }],
            finishReason: "stop",
            usage: {},
            latencyMs: performance.now() - startedAt,
            route: this.route,
          });
        }
        throw new ModelGatewayError("invalid_output");
      }

      if (request.tools !== undefined && request.tools.length > 0) {
        const choice = toProviderNeutralToolChoice(request.toolChoice);
        const bound = model.bindTools?.(request.tools.map(toLangChainTool), {
          ...(choice === undefined ? {} : { tool_choice: choice }),
        });
        if (bound === undefined) {
          throw new ModelGatewayError("unsupported_capability");
        }
        output = await runWithoutModelTracing(() =>
          bound.invoke(messages, invocationOptions(options?.signal)),
        );
      } else {
        output = await runWithoutModelTracing(() =>
          model.invoke(messages, invocationOptions(options?.signal)),
        );
      }

      if (!BaseMessage.isInstance(output)) {
        throw new ModelGatewayError("invalid_output");
      }

      return normalizeMessage(output, this.route, performance.now() - startedAt);
    } catch (error) {
      throw asModelGatewayError(error, options?.signal);
    }
  }

  #createModel(request: ModelCompletionRequest): BaseChatModel {
    const common = {
      model: this.#config.model,
      callbacks: [],
      maxRetries: this.#config.maxRetries ?? 2,
      ...(this.#config.timeoutMs === undefined ? {} : { timeout: this.#config.timeoutMs }),
      ...(request.temperature === undefined ? {} : { temperature: request.temperature }),
      ...(request.maxOutputTokens === undefined ? {} : { maxTokens: request.maxOutputTokens }),
    };

    if (this.#config.provider === "anthropic") {
      return new ChatAnthropic({ ...common, apiKey: this.#config.apiKey });
    }

    if (this.#config.provider === "openai") {
      return new ChatOpenAI({
        ...common,
        apiKey: this.#config.apiKey,
        // Florence's workers combine reasoning, strict output contracts, and
        // tools. The Responses API is the supported OpenAI surface for that
        // combination and keeps the choice inside this provider adapter.
        useResponsesApi: true,
      });
    }

    // Supplying an explicit placeholder prevents the SDK from reading an
    // unrelated OPENAI_API_KEY for an allowlisted local/self-hosted route.
    return new ChatOpenAI({
      ...common,
      apiKey: this.#config.apiKey ?? "not-provided",
      useResponsesApi: false,
      configuration: { baseURL: this.#config.baseUrl },
    });
  }
}

/**
 * Provider schemas describe input-shaped values because app schemas may use
 * transforms (for example canonical instants). The app still performs the
 * authoritative output parse after the provider returns. OpenAI accepts
 * nested `anyOf` but not Zod's emitted `oneOf`, and a non-strict response
 * schema preserves optional fields used by discriminated domain contracts.
 */
export function providerJsonSchema(schema: z.ZodType): JSONSchema {
  return replaceOneOf(z.toJSONSchema(schema, { io: "input" })) as JSONSchema;
}

function replaceOneOf(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(replaceOneOf);
  if (typeof value !== "object" || value === null) return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, child]) => [key === "oneOf" ? "anyOf" : key, replaceOneOf(child)]),
  );
}

function toBaseMessages(message: ModelMessage): BaseMessage[] {
  if (message.role === "system") {
    return [new SystemMessage(partsToText(message.parts))];
  }

  if (message.role === "user") {
    return [new HumanMessage({ content: toUserContent(message.parts) })];
  }

  if (message.role === "assistant") {
    return [
      new AIMessage({
        content: partsToText(message.parts),
        tool_calls: message.parts
          .filter((part) => part.type === "tool_request")
          .map((part) => ({ id: part.requestId, name: part.name, args: part.arguments })),
      }),
    ];
  }

  const toolResults = message.parts.filter((part) => part.type === "tool_result");
  if (toolResults.length === 0) {
    throw new ModelGatewayError("permanent");
  }
  return toolResults.map(
    (part) =>
      new ToolMessage({
        content: serializeJson(part.output),
        tool_call_id: part.requestId,
        name: part.name,
        status: part.isError === true ? "error" : "success",
      }),
  );
}

function toUserContent(parts: readonly ModelMessagePart[]): string | ContentBlock.Standard[] {
  if (parts.every((part) => part.type === "text" || part.type === "document_reference")) {
    return partsToText(parts);
  }

  return parts.flatMap((part): ContentBlock.Standard[] => {
    if (part.type === "text") {
      return [{ type: "text", text: part.text }];
    }
    if (part.type === "image") {
      return part.uri === undefined
        ? [{ type: "image", data: part.data ?? "", mimeType: part.mediaType }]
        : [{ type: "image", url: part.uri, mimeType: part.mediaType }];
    }
    if (part.type === "document_reference") {
      return [{ type: "text", text: documentReferenceText(part) }];
    }
    return [];
  });
}

function partsToText(parts: readonly ModelMessagePart[]): string {
  return parts
    .flatMap((part): string[] => {
      if (part.type === "text") {
        return [part.text];
      }
      if (part.type === "document_reference") {
        return [documentReferenceText(part)];
      }
      if (part.type === "tool_result") {
        return [serializeJson(part.output)];
      }
      return [];
    })
    .join("\n");
}

function documentReferenceText(part: Extract<ModelMessagePart, { type: "document_reference" }>): string {
  const label = part.title === undefined ? "Document" : `Document (${part.title})`;
  const media = part.mediaType === undefined ? "" : ` [${part.mediaType}]`;
  return `${label}${media}: ${part.reference}`;
}

function toLangChainTool(definition: ModelToolDefinition) {
  return tool(async () => "", {
    name: definition.name,
    description: definition.description,
    schema: definition.inputSchema as JSONSchema,
  });
}

function toProviderNeutralToolChoice(choice: ModelToolChoice | undefined): string | undefined {
  if (choice === "required") {
    return "any";
  }
  if (choice === undefined || choice === "auto" || choice === "none") {
    return choice;
  }
  return choice.name;
}

function invocationOptions(signal: AbortSignal | undefined) {
  return { callbacks: [], ...(signal === undefined ? {} : { signal }) };
}

function normalizeMessage(
  message: BaseMessage,
  route: ModelRouteReference,
  latencyMs: number,
): ModelCompletionResult {
  const content: ModelOutputPart[] = extractContent(message);
  if (AIMessage.isInstance(message)) {
    for (const call of message.tool_calls ?? []) {
      const parsedArguments = ModelToolRequestSchema.shape.arguments.safeParse(call.args);
      if (!parsedArguments.success || call.id === undefined) {
        throw new ModelGatewayError("invalid_output");
      }
      content.push({
        type: "tool_request",
        requestId: call.id,
        name: call.name,
        arguments: parsedArguments.data,
      });
    }
  }

  const usage = readUsage(message);
  return ModelCompletionResultSchema.parse({
    content,
    finishReason: content.some((part) => part.type === "tool_request")
      ? "tool_request"
      : readFinishReason(message),
    usage,
    latencyMs,
    route,
  });
}

function extractContent(message: BaseMessage): ModelOutputPart[] {
  if (typeof message.content === "string") {
    return message.content === "" ? [] : [{ type: "text", text: message.content }];
  }

  const parts: ModelOutputPart[] = [];
  for (const candidate of message.content) {
    if (typeof candidate === "string") {
      parts.push({ type: "text", text: candidate });
      continue;
    }
    if (typeof candidate !== "object" || candidate === null) {
      continue;
    }
    const record = candidate as Record<string, unknown>;
    if (record.type === "text" && typeof record.text === "string") {
      parts.push({ type: "text", text: record.text });
      extractCitations(record, parts);
    } else if (record.type === "citation") {
      const uri = citationUri(record);
      if (uri !== undefined) {
        parts.push({ type: "citation", uri });
      }
    }
  }
  return parts;
}

function extractCitations(record: Record<string, unknown>, output: ModelOutputPart[]): void {
  if (!Array.isArray(record.citations)) {
    return;
  }
  for (const candidate of record.citations) {
    if (typeof candidate !== "object" || candidate === null) {
      continue;
    }
    const citation = candidate as Record<string, unknown>;
    const uri = citationUri(citation);
    if (uri !== undefined) {
      output.push({
        type: "citation",
        uri,
        ...(typeof citation.title === "string" ? { title: citation.title } : {}),
        ...(typeof citation.cited_text === "string" ? { excerpt: citation.cited_text } : {}),
      });
    }
  }
}

function citationUri(record: Record<string, unknown>): string | undefined {
  for (const key of ["uri", "url", "source"]) {
    if (typeof record[key] === "string" && record[key] !== "") {
      return record[key];
    }
  }
  return undefined;
}

function readUsage(message: BaseMessage): ModelCompletionResult["usage"] {
  const metadata = "usage_metadata" in message ? message.usage_metadata : undefined;
  if (typeof metadata !== "object" || metadata === null) {
    return {};
  }
  const record = metadata as Record<string, unknown>;
  return {
    ...(typeof record.input_tokens === "number" ? { inputTokens: record.input_tokens } : {}),
    ...(typeof record.output_tokens === "number" ? { outputTokens: record.output_tokens } : {}),
    ...(typeof record.total_tokens === "number" ? { totalTokens: record.total_tokens } : {}),
  };
}

function readFinishReason(message: BaseMessage): ModelCompletionResult["finishReason"] {
  const metadata = message.response_metadata as Record<string, unknown>;
  const reason = metadata.finish_reason ?? metadata.stop_reason;
  if (reason === "length" || reason === "max_tokens") {
    return "length";
  }
  if (reason === "content_filter" || reason === "refusal") {
    return "content_filter";
  }
  if (reason === "stop" || reason === "end_turn") {
    return "stop";
  }
  return "unknown";
}

function serializeJson(value: JsonValue): string {
  return typeof value === "string" ? value : JSON.stringify(value);
}
