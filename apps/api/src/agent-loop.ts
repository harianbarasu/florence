import { createHash } from "node:crypto";
import type OpenAI from "openai";
import type { ExtractParsedContentFromParams } from "openai/lib/ResponsesParser";
import type {
  FunctionTool,
  ParsedResponse,
  ResponseCreateParamsBase,
  ResponseFunctionToolCall,
  ResponseInput,
  ResponseInputItem,
  ResponseOutputItem,
  ResponseStreamEvent,
  Tool,
} from "openai/resources/responses/responses";
import type {
  CapabilityBatchResult,
  CapabilityCatalogSnapshot,
  CapabilityCompletion,
  CapabilityRegistry,
  CapabilityTerminalEnvelope,
  RawCapabilityCall,
} from "./capability-lifecycle.js";

/**
 * OpenAI Responses adaptation of Pi's agent loop and event contracts
 * (pi 4e494929998d6bc4fccf75e0a233f727db4b70ee,
 * packages/agent/src/{agent-loop,types}.ts), including its unlimited useful-tool
 * loop. Exact-call/result progress streaks and tools-free final synthesis adapt
 * Hermes (hermes-agent 6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882,
 * agent/tool_guardrails.py:285-295,333-617 and
 * agent/chat_completion_helpers.py:2890-3232).
 */

const DEFAULT_EMPTY_FINAL_RETRIES = 1;
const DUPLICATE_RESULT_STUB_MIN_CHARS = 512;
const NO_PROGRESS_WARNING_AFTER = 3;
const NO_PROGRESS_SYNTHESIS_AFTER = 4;
const EMPTY_FINAL_NUDGE =
  "Your previous turn produced no final response. Continue from the available context and tool results, then provide a useful final response.";
const NO_CAPABILITIES: CapabilityCatalogSnapshot = Object.freeze({ tools: Object.freeze([]) });

interface AgentLoopProgressStreak {
  readonly signature: string;
  readonly resultDigest: string;
  readonly count: number;
  readonly firstCallId: string;
  readonly terminal: CapabilityTerminalEnvelope;
}

export type AgentLoopRequest = Omit<
  ResponseCreateParamsBase,
  "input" | "parallel_tool_calls" | "stream" | "tools"
> & {
  readonly model: string;
  /** Accepted by the Responses streaming endpoint but missing from its helper type. */
  readonly max_tool_calls?: number | null;
};

export type AgentLoopParsed<TRequest extends AgentLoopRequest> = ExtractParsedContentFromParams<TRequest>;

export interface AgentLoopPlannedCall extends RawCapabilityCall {
  readonly callId: string;
  readonly name: string;
  readonly argumentsJson: string;
}

export interface AgentLoopTurn<TContext, TParsed> {
  readonly turn: number;
  readonly transcript: readonly ResponseInputItem[];
  readonly response: ParsedResponse<TParsed>;
  readonly catalog: CapabilityCatalogSnapshot;
  readonly capabilityContext: Readonly<TContext>;
  readonly calls: readonly AgentLoopPlannedCall[];
}

export interface AgentLoopSuspension<TValue> {
  readonly value: TValue;
}

export type AgentLoopEvent<TParsed> =
  | { readonly type: "agent_start" }
  | {
      readonly type: "input";
      readonly boundary: "steering" | "follow_up" | "empty_final_recovery";
      readonly items: readonly ResponseInputItem[];
    }
  | { readonly type: "turn_start"; readonly turn: number }
  | { readonly type: "model_start"; readonly turn: number; readonly catalog: CapabilityCatalogSnapshot }
  | { readonly type: "model_event"; readonly turn: number; readonly event: ResponseStreamEvent }
  | { readonly type: "response_end"; readonly turn: number; readonly response: ParsedResponse<TParsed> }
  | {
      readonly type: "tool_execution_start";
      readonly turn: number;
      readonly call: AgentLoopPlannedCall;
    }
  | {
      readonly type: "tool_execution_end";
      readonly turn: number;
      readonly result: CapabilityTerminalEnvelope;
    }
  | {
      readonly type: "turn_end";
      readonly turn: number;
      readonly response: ParsedResponse<TParsed>;
      readonly results: readonly CapabilityTerminalEnvelope[];
    }
  | { readonly type: "empty_final_retry"; readonly turn: number; readonly attempt: number }
  | { readonly type: "agent_end"; readonly outcome: AgentLoopResultKind };

export type AgentLoopEventSink<TParsed> = (event: AgentLoopEvent<TParsed>) => Promise<void> | void;

export type AgentLoopToolResultFormatter<TContext> = (
  result: CapabilityTerminalEnvelope,
  context: Readonly<TContext>,
) => ResponseInputItem.FunctionCallOutput | Promise<ResponseInputItem.FunctionCallOutput>;

export type AgentLoopModelCall<TParsed> = (
  request: ResponseCreateParamsBase,
  signal?: AbortSignal,
) => ParsedResponse<TParsed> | Promise<ParsedResponse<TParsed>>;

export interface AgentLoopInput<TContext, TParsed = unknown, TSuspension = never> {
  readonly client: OpenAI;
  readonly request: AgentLoopRequest;
  /** Pi-style provider seam; defaults to OpenAI Responses streaming. */
  readonly modelCall?: AgentLoopModelCall<TParsed>;
  readonly transcript: ResponseInput;
  readonly registry: CapabilityRegistry<TContext>;
  /** Provider-hosted tools that execute inside the model response, alongside Florence capabilities. */
  readonly builtInTools?: readonly Tool[];
  /** Calls already checkpointed with the trailing assistant output in `transcript`. */
  readonly resumePendingCalls?: readonly AgentLoopPlannedCall[];
  /** Settle resumed calls and yield before making another model request. */
  readonly yieldAfterPendingCalls?: boolean;
  /** Re-resolved before every model turn so availability and execution share one current context. */
  readonly getCapabilityContext: () => Readonly<TContext> | Promise<Readonly<TContext>>;
  readonly signal?: AbortSignal;
  readonly maxEmptyFinalRetries?: number;
  readonly parallelToolCalls?: boolean;
  readonly getSteeringInput?: () => ResponseInput | undefined | Promise<ResponseInput | undefined>;
  readonly getFollowUpInput?: () => ResponseInput | undefined | Promise<ResponseInput | undefined>;
  readonly onEvent?: AgentLoopEventSink<TParsed>;
  /**
   * For a valid external call, returning a value suspends after the assistant
   * output is appended to `transcript`, but before any call in the batch can
   * execute. Invalid, unavailable, rejected, and truncated calls bypass this
   * hook and return model-visible tool errors inline.
   */
  readonly suspendBeforeToolExecution?: (
    turn: AgentLoopTurn<TContext, TParsed>,
  ) => AgentLoopSuspension<TSuspension> | undefined | Promise<AgentLoopSuspension<TSuspension> | undefined>;
  /** Formats one terminal result; calls are made in assistant source order. */
  readonly formatToolResult?: AgentLoopToolResultFormatter<TContext>;
  readonly isUsableFinal?: (response: ParsedResponse<TParsed>) => boolean | Promise<boolean>;
}

export type AgentLoopResultKind = "completed" | "suspended" | "yielded" | "empty_final";

interface AgentLoopResultBase {
  readonly transcript: readonly ResponseInputItem[];
  readonly turns: number;
}

export interface AgentLoopCompleted<TParsed> extends AgentLoopResultBase {
  readonly kind: "completed";
  readonly response: ParsedResponse<TParsed>;
}

export interface AgentLoopSuspended<TParsed, TSuspension> extends AgentLoopResultBase {
  readonly kind: "suspended";
  readonly response: ParsedResponse<TParsed>;
  readonly catalog: CapabilityCatalogSnapshot;
  readonly calls: readonly AgentLoopPlannedCall[];
  readonly suspension: TSuspension;
}

export interface AgentLoopYielded extends AgentLoopResultBase {
  readonly kind: "yielded";
  readonly results: readonly CapabilityTerminalEnvelope[];
}

export interface AgentLoopEmptyFinal<TParsed> extends AgentLoopResultBase {
  readonly kind: "empty_final";
  readonly response: ParsedResponse<TParsed>;
}

export type AgentLoopResult<TParsed, TSuspension> =
  | AgentLoopCompleted<TParsed>
  | AgentLoopSuspended<TParsed, TSuspension>
  | AgentLoopYielded
  | AgentLoopEmptyFinal<TParsed>;

/** Run or continue a model-directed capability loop over an existing Responses transcript. */
export async function runAgentLoop<TContext, const TRequest extends AgentLoopRequest, TSuspension = never>(
  input: Omit<AgentLoopInput<TContext, AgentLoopParsed<TRequest>, TSuspension>, "request"> & {
    readonly request: TRequest;
  },
): Promise<AgentLoopResult<AgentLoopParsed<TRequest>, TSuspension>> {
  type TParsed = AgentLoopParsed<TRequest>;
  const maxEmptyFinalRetries = boundedCount(
    input.maxEmptyFinalRetries,
    DEFAULT_EMPTY_FINAL_RETRIES,
    "maxEmptyFinalRetries",
    0,
    5,
  );
  const transcript: ResponseInputItem[] = [...input.transcript];
  const emit = async (event: AgentLoopEvent<TParsed>): Promise<void> => {
    await input.onEvent?.(event);
  };
  let turns = 0;
  let emptyFinalRetries = 0;
  let progressStreak: AgentLoopProgressStreak | null = null;
  let synthesisOnly = false;

  throwIfAborted(input.signal);
  await emit({ type: "agent_start" });
  let resumedResults: readonly CapabilityTerminalEnvelope[] | null = null;
  if (input.resumePendingCalls && input.resumePendingCalls.length > 0) {
    const capabilityContext = await input.getCapabilityContext();
    throwIfAborted(input.signal);
    const catalog = await input.registry.catalog(capabilityContext, input.signal);
    throwIfAborted(input.signal);
    const resumedBatch = await executeCapabilityCalls({
      registry: input.registry,
      catalog,
      capabilityContext,
      calls: input.resumePendingCalls,
      completion: "complete",
      transcript,
      turn: 0,
      emit,
      ...(input.signal ? { signal: input.signal } : {}),
      ...(input.formatToolResult ? { formatToolResult: input.formatToolResult } : {}),
    });
    if (resumedBatch.suspendedBeforeExternal) {
      throw new Error("A persisted capability call was suspended while resuming");
    }
    resumedResults = resumedBatch.results;
  }
  await inject(await input.getSteeringInput?.(), "steering", transcript, emit);
  if (resumedResults && input.yieldAfterPendingCalls === true) {
    const result: AgentLoopYielded = {
      kind: "yielded",
      transcript,
      turns,
      results: resumedResults,
    };
    await emit({ type: "agent_end", outcome: result.kind });
    return result;
  }

  // Direct Pi behavior: useful tool turns continue until the model reaches a
  // final response, the caller cancels, or a durable effect checkpoints. An
  // arbitrary iteration count must never decide that a household task failed.
  while (true) {
    throwIfAborted(input.signal);
    turns += 1;
    await emit({ type: "turn_start", turn: turns });

    const capabilityContext = await input.getCapabilityContext();
    throwIfAborted(input.signal);
    const catalog = synthesisOnly
      ? NO_CAPABILITIES
      : await input.registry.catalog(capabilityContext, input.signal);
    throwIfAborted(input.signal);
    await emit({ type: "model_start", turn: turns, catalog });

    const request = { ...input.request };
    if (synthesisOnly) {
      Reflect.deleteProperty(request, "max_tool_calls");
      Reflect.deleteProperty(request, "tool_choice");
    }
    const modelRequest = {
      ...request,
      input: transcript,
      tools: synthesisOnly ? [] : [...(input.builtInTools ?? []), ...functionTools(catalog)],
      parallel_tool_calls: synthesisOnly ? false : (input.parallelToolCalls ?? true),
    };
    let response: ParsedResponse<TParsed>;
    if (input.modelCall) {
      response = await input.modelCall(modelRequest, input.signal);
    } else {
      const stream = input.client.responses.stream(
        modelRequest,
        input.signal ? { signal: input.signal } : undefined,
      );
      for await (const event of stream) {
        await emit({ type: "model_event", turn: turns, event });
      }
      response = (await stream.finalResponse()) as ParsedResponse<TParsed>;
    }
    throwIfAborted(input.signal);
    await emit({ type: "response_end", turn: turns, response });

    const calls = plannedCalls(response.output);
    transcript.push(...continuationItems(response.output));

    if (calls.length > 0) {
      emptyFinalRetries = 0;
      const turn: AgentLoopTurn<TContext, TParsed> = {
        turn: turns,
        transcript,
        response,
        catalog,
        capabilityContext,
        calls,
      };
      let requestedSuspension: AgentLoopSuspension<TSuspension> | undefined;
      const batch = await executeCapabilityCalls({
        registry: input.registry,
        catalog,
        capabilityContext,
        calls,
        completion: responseCompletion(response),
        transcript,
        turn: turns,
        emit,
        ...(input.suspendBeforeToolExecution
          ? {
              suspendBeforeExternal: async () => {
                const suspension = await input.suspendBeforeToolExecution?.(turn);
                throwIfAborted(input.signal);
                if (suspension === undefined) return false;
                requestedSuspension = suspension;
                return true;
              },
            }
          : {}),
        ...(input.signal ? { signal: input.signal } : {}),
        ...(input.formatToolResult ? { formatToolResult: input.formatToolResult } : {}),
      });
      throwIfAborted(input.signal);
      if (batch.suspendedBeforeExternal) {
        if (requestedSuspension === undefined) {
          throw new Error("Capability execution suspended without a suspension value");
        }
        const result: AgentLoopSuspended<TParsed, TSuspension> = {
          kind: "suspended",
          transcript,
          turns,
          response,
          catalog,
          calls,
          suspension: requestedSuspension.value,
        };
        await emit({ type: "agent_end", outcome: result.kind });
        return result;
      }
      const { results } = batch;
      progressStreak = observeProgress(results, progressStreak);
      annotateRepeatedResult(transcript, progressStreak);
      await emit({ type: "turn_end", turn: turns, response, results });
      const steering = await input.getSteeringInput?.();
      if (await inject(steering, "steering", transcript, emit)) {
        emptyFinalRetries = 0;
        progressStreak = null;
        synthesisOnly = false;
      } else if ((progressStreak?.count ?? 0) >= NO_PROGRESS_SYNTHESIS_AFTER) {
        synthesisOnly = true;
      }
      continue;
    }

    await emit({ type: "turn_end", turn: turns, response, results: [] });
    const steering = await input.getSteeringInput?.();
    if (await inject(steering, "steering", transcript, emit)) {
      emptyFinalRetries = 0;
      progressStreak = null;
      synthesisOnly = false;
      continue;
    }

    const usableFinal = input.isUsableFinal
      ? await input.isUsableFinal(response)
      : defaultUsableFinal(response);
    if (!usableFinal) {
      if (emptyFinalRetries >= maxEmptyFinalRetries) {
        const result: AgentLoopEmptyFinal<TParsed> = {
          kind: "empty_final",
          transcript,
          turns,
          response,
        };
        await emit({ type: "agent_end", outcome: result.kind });
        return result;
      }
      emptyFinalRetries += 1;
      const recovery: ResponseInput = [{ role: "user", content: EMPTY_FINAL_NUDGE }];
      await emit({ type: "empty_final_retry", turn: turns, attempt: emptyFinalRetries });
      await inject(recovery, "empty_final_recovery", transcript, emit);
      continue;
    }

    const followUp = await input.getFollowUpInput?.();
    if (await inject(followUp, "follow_up", transcript, emit)) {
      emptyFinalRetries = 0;
      progressStreak = null;
      synthesisOnly = false;
      continue;
    }

    const result: AgentLoopCompleted<TParsed> = {
      kind: "completed",
      transcript,
      turns,
      response,
    };
    await emit({ type: "agent_end", outcome: result.kind });
    return result;
  }
}

function observeProgress(
  results: readonly CapabilityTerminalEnvelope[],
  previous: AgentLoopProgressStreak | null,
): AgentLoopProgressStreak | null {
  const terminal = results.length === 1 ? results[0] : undefined;
  if (!terminal || terminal.canonicalArguments === null) return null;
  const signature = terminalSignature(terminal);
  const resultDigest = terminalDigest(terminal);
  if (previous?.signature === signature && previous.resultDigest === resultDigest) {
    return { ...previous, count: previous.count + 1, terminal };
  }
  return {
    signature,
    resultDigest,
    count: 1,
    firstCallId: terminal.callId,
    terminal,
  };
}

function terminalSignature(terminal: CapabilityTerminalEnvelope): string {
  return `${terminal.capabilityName}\0${JSON.stringify(canonicalJsonValue(terminal.canonicalArguments))}`;
}

function canonicalJsonValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalJsonValue);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
        .map(([key, item]) => [key, canonicalJsonValue(item)]),
    );
  }
  return value;
}

function terminalDigest(terminal: CapabilityTerminalEnvelope): string {
  return createHash("sha256")
    .update(terminal.outcome)
    .update("\0")
    .update(terminal.errorCode ?? "")
    .update("\0")
    .update(terminal.modelOutput)
    .digest("hex");
}

function annotateRepeatedResult(
  transcript: ResponseInputItem[],
  streak: AgentLoopProgressStreak | null,
): void {
  if (!streak || streak.count < 2) return;
  const index = transcript.findLastIndex(
    (item) => item.type === "function_call_output" && item.call_id === streak.terminal.callId,
  );
  const output = transcript[index];
  if (output?.type !== "function_call_output") return;
  const note = repeatedResultNote(streak);
  if (typeof output.output === "string") {
    transcript[index] = {
      ...output,
      output:
        output.output.length >= DUPLICATE_RESULT_STUB_MIN_CHARS
          ? resultReference(streak, note)
          : `${output.output}\n\n${note}`,
    };
    return;
  }
  transcript[index] = {
    ...output,
    output: [...output.output, { type: "input_text" as const, text: note }],
  };
}

function repeatedResultNote(streak: AgentLoopProgressStreak): string {
  const warning =
    streak.count >= NO_PROGRESS_WARNING_AFTER
      ? " Do not repeat it unchanged again; use the existing result, change the arguments or approach, or synthesize the best answer now."
      : " Reuse the existing result instead of expanding this duplicate payload.";
  return `[Florence loop note: this exact capability call returned the same terminal result ${streak.count} consecutive times.${warning}]`;
}

function resultReference(streak: AgentLoopProgressStreak, note: string): string {
  return `[Duplicate result omitted. The full structured result is in tool call ${streak.firstCallId}.]\n\n${note}`;
}

async function executeCapabilityCalls<TContext, TParsed>(input: {
  readonly registry: CapabilityRegistry<TContext>;
  readonly catalog: CapabilityCatalogSnapshot;
  readonly capabilityContext: Readonly<TContext>;
  readonly calls: readonly AgentLoopPlannedCall[];
  readonly completion: CapabilityCompletion;
  readonly transcript: ResponseInputItem[];
  /** Zero denotes a persisted call resumed before this invocation's first model turn. */
  readonly turn: number;
  readonly signal?: AbortSignal;
  readonly emit: AgentLoopEventSink<TParsed>;
  readonly formatToolResult?: AgentLoopToolResultFormatter<TContext>;
  readonly suspendBeforeExternal?: () => boolean | Promise<boolean>;
}): Promise<CapabilityBatchResult> {
  const batch = await input.registry.executeCalls({
    snapshot: input.catalog,
    context: input.capabilityContext,
    calls: input.calls,
    completion: input.completion,
    onStart: async () => {
      for (const call of input.calls) {
        await input.emit({ type: "tool_execution_start", turn: input.turn, call });
      }
    },
    ...(input.suspendBeforeExternal ? { suspendBeforeExternal: input.suspendBeforeExternal } : {}),
    ...(input.signal ? { signal: input.signal } : {}),
  });
  if (batch.suspendedBeforeExternal) return batch;
  for (const result of batch.results) {
    await input.emit({ type: "tool_execution_end", turn: input.turn, result });
    const formatted = input.formatToolResult
      ? await input.formatToolResult(result, input.capabilityContext)
      : defaultToolResult(result);
    if (formatted.call_id !== result.callId) {
      throw new Error("A formatted capability result changed its tool call identifier");
    }
    input.transcript.push(formatted);
  }
  return batch;
}

function functionTools(snapshot: CapabilityCatalogSnapshot): FunctionTool[] {
  return snapshot.tools.map(
    (tool) =>
      ({
        type: "function",
        name: tool.name,
        description: tool.description,
        strict: true,
        parameters: tool.parameters,
      }) as FunctionTool,
  );
}

function plannedCalls(output: readonly ResponseOutputItem[]): AgentLoopPlannedCall[] {
  return output
    .filter((item): item is ResponseFunctionToolCall => item.type === "function_call")
    .map((call) => ({
      callId: call.call_id,
      name: call.name,
      argumentsJson: call.arguments,
    }));
}

function continuationItems(output: readonly ResponseOutputItem[]): ResponseInputItem[] {
  const items: ResponseInputItem[] = [];
  for (const item of output) {
    if (item.type === "function_call") {
      const { parsed_arguments: _parsedArguments, ...call } = item as typeof item & {
        readonly parsed_arguments?: unknown;
      };
      items.push(call);
    } else if (item.type === "message" || item.type === "reasoning" || item.type === "web_search_call") {
      items.push(item);
    }
  }
  return items;
}

function responseCompletion(response: ParsedResponse<unknown>): "complete" | "truncated" {
  return response.status === "incomplete" ||
    response.output.some(
      (item) => item.type === "function_call" && item.status !== undefined && item.status !== "completed",
    )
    ? "truncated"
    : "complete";
}

function defaultToolResult(result: CapabilityTerminalEnvelope): ResponseInputItem.FunctionCallOutput {
  return {
    type: "function_call_output",
    call_id: result.callId,
    output: result.modelOutput,
  };
}

function defaultUsableFinal(response: ParsedResponse<unknown>): boolean {
  if (response.output_parsed !== null && response.output_parsed !== undefined) return true;
  if (response.output_text.trim().length > 0) return true;
  return response.output.some(
    (item) =>
      item.type === "message" &&
      item.content.some((part) => part.type === "refusal" && part.refusal.trim().length > 0),
  );
}

async function inject<TParsed>(
  items: ResponseInput | undefined,
  boundary: "steering" | "follow_up" | "empty_final_recovery",
  transcript: ResponseInputItem[],
  emit: AgentLoopEventSink<TParsed>,
): Promise<boolean> {
  if (!items || items.length === 0) return false;
  const injected = [...items];
  transcript.push(...injected);
  await emit({ type: "input", boundary, items: injected });
  return true;
}

function boundedCount(
  value: number | undefined,
  fallback: number,
  label: string,
  minimum: number,
  maximum: number,
): number {
  const candidate = value ?? fallback;
  if (!Number.isSafeInteger(candidate) || candidate < minimum || candidate > maximum) {
    throw new Error(`${label} must be an integer from ${minimum} through ${maximum}`);
  }
  return candidate;
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) throw signal.reason ?? new Error("Agent loop aborted");
}
