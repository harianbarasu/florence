import { z } from "zod";

/**
 * Small tool-execution kernel adapted from Pi's ordered tool loop
 * (4e494929, packages/agent/src/agent-loop.ts) and Hermes's typed registry
 * (6dcebea7, tools/registry.py). Product policy stays with the capability
 * adapter and its caller; this module only validates and runs model-requested
 * work.
 */

const MAX_RAW_ARGUMENT_BYTES = 1_048_576;
const MAX_TOOL_OUTPUT_BYTES = 1_048_576;
const MAX_PROGRESS_BYTES = 32_768;
const MAX_PROGRESS_EVENTS = 100;
const MAX_ENVELOPE_OVERHEAD_BYTES = 4_096;
const MAX_SAFE_MESSAGE_CHARS = 500;

type JsonPrimitive = boolean | number | string | null;
export type JsonValue = JsonPrimitive | readonly JsonValue[] | { readonly [key: string]: JsonValue };

export type CapabilityExecutionMode = "parallel" | "sequential";
export type CapabilityCompletion = "complete" | "truncated";
export type CapabilityTerminalOutcome =
  | "succeeded"
  | "failed"
  | "cancelled"
  | "unknown"
  | "protocol_rejected";

export type CapabilityAdapterErrorCode =
  | "unavailable"
  | "invalid_response"
  | "transient"
  | "permanent"
  | "unknown";

export class CapabilityAdapterError extends Error {
  readonly code: CapabilityAdapterErrorCode;
  readonly safeMessage: string;

  constructor(code: CapabilityAdapterErrorCode, safeMessage: string) {
    super("Capability adapter failed");
    this.name = "CapabilityAdapterError";
    this.code = code;
    this.safeMessage = boundSafeMessage(safeMessage, "The capability could not finish.");
  }
}

export interface CapabilityAdapterResult<TOutput> {
  readonly output: TOutput;
}

export type CapabilityProgressReporter<TProgress> = (progress: TProgress) => void;

export interface CapabilityExecutionInput<TContext, TArguments, TProgress> {
  readonly callId: string;
  readonly arguments: TArguments;
  readonly context: Readonly<TContext>;
  readonly signal: AbortSignal;
  readonly reportProgress: CapabilityProgressReporter<TProgress>;
}

export type CapabilityAvailabilityProbe<TContext> = (
  context: Readonly<TContext>,
  signal: AbortSignal,
) => boolean | Promise<boolean>;

export interface CapabilityAdmissionInput<TContext> {
  readonly context: Readonly<TContext>;
  readonly capabilityName: string;
  readonly canonicalArguments?: JsonValue;
}

export type CapabilityAdmissionPredicate<TContext> = (
  input: CapabilityAdmissionInput<TContext>,
  signal: AbortSignal,
) => boolean | Promise<boolean>;

export interface CapabilityDefinition<
  TContext,
  TArguments = unknown,
  TOutput = unknown,
  TProgress = unknown,
> {
  readonly name: string;
  readonly description: string;
  readonly modelSchema: JsonValue;
  readonly inputSchema: z.ZodType<TArguments>;
  readonly outputSchema: z.ZodType<TOutput>;
  readonly progressSchema?: z.ZodType<TProgress>;
  readonly executionMode: CapabilityExecutionMode;
  readonly timeoutMs: number;
  readonly maxOutputBytes: number;
  readonly maxProgressBytes?: number;
  readonly maxProgressEvents?: number;
  readonly availability?: CapabilityAvailabilityProbe<TContext>;
  readonly admit?: CapabilityAdmissionPredicate<TContext>;
  readonly execute: (
    input: CapabilityExecutionInput<TContext, TArguments, TProgress>,
  ) => Promise<CapabilityAdapterResult<TOutput>>;
}

type ErasedCapabilityDefinition<TContext> = CapabilityDefinition<TContext>;

/** Preserve adapter-local schema inference while erasing it at the registry seam. */
export function defineCapability<TContext, TArguments, TOutput, TProgress = unknown>(
  definition: CapabilityDefinition<TContext, TArguments, TOutput, TProgress>,
): CapabilityDefinition<TContext> {
  return definition as unknown as CapabilityDefinition<TContext>;
}

export interface CapabilityCatalogEntry {
  readonly name: string;
  readonly description: string;
  readonly parameters: JsonValue;
}

export interface CapabilityCatalogSnapshot {
  readonly tools: readonly CapabilityCatalogEntry[];
}

export interface RawCapabilityCall {
  readonly callId: unknown;
  readonly name: unknown;
  readonly argumentsJson: unknown;
}

export interface CapabilityTerminalEnvelope {
  readonly callId: string;
  readonly capabilityName: string;
  readonly sourceIndex: number;
  readonly outcome: CapabilityTerminalOutcome;
  readonly errorCode: string | null;
  readonly retryable: boolean;
  readonly safeMessage: string | null;
  /** Canonical JSON intended for the next model request. */
  readonly modelOutput: string;
  readonly serializedBytes: number;
}

export type CapabilityLifecycleEvent =
  | {
      readonly phase: "requested";
      readonly callId: string;
      readonly capabilityName: string;
      readonly sourceIndex: number;
    }
  | {
      readonly phase: "admitted";
      readonly callId: string;
      readonly capabilityName: string;
      readonly sourceIndex: number;
    }
  | {
      readonly phase: "running";
      readonly callId: string;
      readonly capabilityName: string;
      readonly sourceIndex: number;
    }
  | {
      readonly phase: "progress";
      readonly callId: string;
      readonly capabilityName: string;
      readonly sourceIndex: number;
      readonly sequence: number;
      readonly progress: JsonValue;
      readonly serializedBytes: number;
    }
  | {
      readonly phase: "terminal";
      readonly terminal: CapabilityTerminalEnvelope;
    };

export type CapabilityLifecycleObserver = (event: CapabilityLifecycleEvent) => void | Promise<void>;

export interface ExecuteCapabilityCallsInput<TContext> {
  readonly snapshot: CapabilityCatalogSnapshot;
  readonly context: Readonly<TContext>;
  readonly calls: readonly RawCapabilityCall[];
  readonly completion: CapabilityCompletion;
  readonly signal?: AbortSignal;
  readonly observer?: CapabilityLifecycleObserver;
  readonly now?: () => Date;
}

export interface CapabilityBatchResult {
  /** Terminal envelopes are always returned in assistant source order. */
  readonly results: readonly CapabilityTerminalEnvelope[];
  /** Lifecycle events preserve emission order; parallel terminals use completion order. */
  readonly events: readonly CapabilityLifecycleEvent[];
}

interface NormalizedDefinition<TContext> {
  readonly definition: ErasedCapabilityDefinition<TContext>;
  readonly catalog: CapabilityCatalogEntry;
}

/** Immutable typed registry and execution kernel. */
export class CapabilityRegistry<TContext> {
  readonly #entries: ReadonlyMap<string, NormalizedDefinition<TContext>>;

  constructor(definitions: readonly CapabilityDefinition<TContext>[]) {
    const entries = new Map<string, NormalizedDefinition<TContext>>();
    for (const rawDefinition of definitions) {
      const definition = normalizeDefinition(rawDefinition);
      if (entries.has(definition.name)) {
        throw new Error(`Duplicate Florence capability: ${definition.name}`);
      }
      entries.set(
        definition.name,
        Object.freeze({
          definition,
          catalog: deepFreeze({
            name: definition.name,
            description: definition.description,
            parameters: cloneCanonicalJson(definition.modelSchema),
          }),
        }),
      );
    }
    this.#entries = entries;
    Object.freeze(this);
  }

  async catalog(context: Readonly<TContext>, signal?: AbortSignal): Promise<CapabilityCatalogSnapshot> {
    const localSignal = signal ?? new AbortController().signal;
    const tools: CapabilityCatalogEntry[] = [];

    for (const name of [...this.#entries.keys()].sort()) {
      if (localSignal.aborted) break;
      const entry = this.#entries.get(name);
      if (!entry) continue;
      if (entry.definition.availability) {
        const available = await settleAvailability(entry.definition.availability, context, localSignal);
        if (!available) continue;
      }
      tools.push(entry.catalog);
    }

    return deepFreeze({ tools });
  }

  async executeCalls(input: ExecuteCapabilityCallsInput<TContext>): Promise<CapabilityBatchResult> {
    const availableNames = new Set(input.snapshot.tools.map((tool) => tool.name));
    const outerSignal = input.signal ?? new AbortController().signal;
    const events: CapabilityLifecycleEvent[] = [];
    const results: Array<CapabilityTerminalEnvelope | undefined> = new Array(input.calls.length);
    const normalizedCalls = input.calls.map((call, sourceIndex) => normalizeRawCall(call, sourceIndex));

    const emit = (event: CapabilityLifecycleEvent): void => {
      const frozen = deepFreeze(structuredClone(event)) as CapabilityLifecycleEvent;
      events.push(frozen);
      try {
        const observed = input.observer?.(frozen);
        if (observed && typeof observed.then === "function") void observed.catch(() => undefined);
      } catch {
        // Observers present lifecycle state; they do not participate in execution.
      }
    };

    for (const call of normalizedCalls) {
      emit({
        phase: "requested",
        callId: call.callId,
        capabilityName: call.name,
        sourceIndex: call.sourceIndex,
      });
    }

    const terminalize = (call: NormalizedRawCall, terminal: TerminalSeed): CapabilityTerminalEnvelope => {
      const existing = results[call.sourceIndex];
      if (existing) return existing;
      const envelope = buildTerminalEnvelope(call, terminal);
      results[call.sourceIndex] = envelope;
      emit({ phase: "terminal", terminal: envelope });
      return envelope;
    };

    if (outerSignal.aborted) {
      for (const call of normalizedCalls) terminalize(call, cancelledBeforeStart());
      return freezeBatch(results, events);
    }

    if (input.completion === "truncated") {
      for (const call of normalizedCalls) {
        terminalize(
          call,
          protocolFailure("truncated_model_output", "The model tool call was truncated and was not run."),
        );
      }
      return freezeBatch(results, events);
    }

    const duplicateIds = duplicateCallIds(normalizedCalls);
    const prepared: PreparedCall<TContext>[] = [];
    for (const call of normalizedCalls) {
      if (outerSignal.aborted) break;
      if (call.problem) {
        terminalize(call, protocolFailure("malformed_call", call.problem));
        continue;
      }
      if (duplicateIds.has(call.callId)) {
        terminalize(
          call,
          protocolFailure(
            "duplicate_call_id",
            "The model reused a tool call identifier; neither duplicate was run.",
          ),
        );
        continue;
      }
      const definition = availableNames.has(call.name) ? this.#entries.get(call.name)?.definition : undefined;
      if (!definition) {
        terminalize(
          call,
          protocolFailure(
            "unknown_or_unavailable_capability",
            "That capability was not available in this turn.",
          ),
        );
        continue;
      }
      const parsed = parseArguments(call.argumentsJson, definition.inputSchema);
      if (!parsed.ok) {
        terminalize(call, protocolFailure("invalid_arguments", parsed.safeMessage));
        continue;
      }
      const admitted = await settleAdmission(definition, input.context, parsed.arguments, outerSignal);
      if (!admitted) {
        terminalize(
          call,
          outerSignal.aborted
            ? cancelledBeforeStart()
            : protocolFailure("not_admitted", "That capability call was not admitted."),
        );
        continue;
      }
      prepared.push({ call, definition, arguments: parsed.arguments });
      emit({
        phase: "admitted",
        callId: call.callId,
        capabilityName: call.name,
        sourceIndex: call.sourceIndex,
      });
    }

    const runOne = async (item: PreparedCall<TContext>): Promise<void> => {
      if (results[item.call.sourceIndex]) return;
      if (outerSignal.aborted) {
        terminalize(item.call, cancelledBeforeStart());
        return;
      }
      emit({
        phase: "running",
        callId: item.call.callId,
        capabilityName: item.call.name,
        sourceIndex: item.call.sourceIndex,
      });
      terminalize(
        item.call,
        await runPreparedCall({
          item,
          context: input.context,
          outerSignal,
          emit,
        }),
      );
    };

    if (prepared.length > 0 && prepared.every(({ definition }) => definition.executionMode === "parallel")) {
      await Promise.all(prepared.map((item) => runOne(item)));
    } else {
      for (const item of prepared) await runOne(item);
    }

    for (const call of normalizedCalls) {
      if (!results[call.sourceIndex]) terminalize(call, cancelledBeforeStart());
    }
    return freezeBatch(results, events);
  }
}

interface NormalizedRawCall {
  readonly callId: string;
  readonly name: string;
  readonly argumentsJson: string;
  readonly sourceIndex: number;
  readonly problem?: string;
}

interface PreparedCall<TContext> {
  readonly call: NormalizedRawCall;
  readonly definition: ErasedCapabilityDefinition<TContext>;
  readonly arguments: JsonValue;
}

interface TerminalSeed {
  readonly outcome: CapabilityTerminalOutcome;
  readonly errorCode: string | null;
  readonly retryable: boolean;
  readonly safeMessage: string | null;
  readonly output?: JsonValue;
  readonly maxOutputBytes?: number;
}

async function settleAvailability<TContext>(
  availability: CapabilityAvailabilityProbe<TContext>,
  context: Readonly<TContext>,
  signal: AbortSignal,
): Promise<boolean> {
  if (signal.aborted) return false;
  try {
    return (
      (await raceAbort(
        Promise.resolve().then(() => availability(context, signal)),
        signal,
      )) === true
    );
  } catch {
    return false;
  }
}

async function settleAdmission<TContext>(
  definition: ErasedCapabilityDefinition<TContext>,
  context: Readonly<TContext>,
  canonicalArguments: JsonValue,
  signal: AbortSignal,
): Promise<boolean> {
  if (signal.aborted) return false;
  if (!definition.admit) return true;
  try {
    return (
      (await raceAbort(
        Promise.resolve().then(() =>
          definition.admit?.(
            {
              context,
              capabilityName: definition.name,
              canonicalArguments,
            },
            signal,
          ),
        ),
        signal,
      )) === true
    );
  } catch {
    return false;
  }
}

async function raceAbort<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) throw signal.reason;
  let onAbort: (() => void) | undefined;
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => reject(signal.reason ?? new Error("Capability call cancelled"));
    signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    return await Promise.race([promise, aborted]);
  } finally {
    if (onAbort) signal.removeEventListener("abort", onAbort);
  }
}

async function runPreparedCall<TContext>(input: {
  readonly item: PreparedCall<TContext>;
  readonly context: Readonly<TContext>;
  readonly outerSignal: AbortSignal;
  readonly emit: (event: CapabilityLifecycleEvent) => void;
}): Promise<TerminalSeed> {
  const { definition } = input.item;
  const controller = new AbortController();
  const onAbort = () => controller.abort(input.outerSignal.reason);
  if (input.outerSignal.aborted) controller.abort(input.outerSignal.reason);
  else input.outerSignal.addEventListener("abort", onAbort, { once: true });
  let timer: ReturnType<typeof setTimeout> | undefined;
  let acceptingUpdates = true;
  let progressCount = 0;
  let resolveProgressViolation: ((seed: TerminalSeed) => void) | undefined;
  const progressViolation = new Promise<TerminalSeed>((resolve) => {
    resolveProgressViolation = resolve;
  });

  const failProgressContract = (): void => {
    if (!acceptingUpdates) return;
    acceptingUpdates = false;
    controller.abort(new Error("Capability progress contract violation"));
    resolveProgressViolation?.(
      failureSeed("adapter_contract", "The capability returned an invalid progress update.", false),
    );
  };

  const reportProgress = (rawProgress: unknown): void => {
    if (!acceptingUpdates) return;
    if (!definition.progressSchema) {
      failProgressContract();
      return;
    }
    progressCount += 1;
    if (progressCount > (definition.maxProgressEvents ?? 8)) {
      failProgressContract();
      return;
    }
    const parsed = definition.progressSchema.safeParse(rawProgress);
    if (!parsed.success) {
      failProgressContract();
      return;
    }
    try {
      const progress = cloneCanonicalJson(parsed.data);
      const serializedBytes = jsonBytes(progress);
      if (serializedBytes > (definition.maxProgressBytes ?? MAX_PROGRESS_BYTES)) {
        failProgressContract();
        return;
      }
      input.emit({
        phase: "progress",
        callId: input.item.call.callId,
        capabilityName: input.item.call.name,
        sourceIndex: input.item.call.sourceIndex,
        sequence: progressCount,
        progress,
        serializedBytes,
      });
    } catch {
      failProgressContract();
    }
  };

  const execution = Promise.resolve()
    .then(() =>
      definition.execute({
        callId: input.item.call.callId,
        arguments: input.item.arguments,
        context: input.context,
        signal: controller.signal,
        reportProgress,
      }),
    )
    .then((result) => normalizeAdapterSuccess(result, definition))
    .catch((error: unknown) => normalizeAdapterFailure(error));

  let settleCancellation: (() => void) | undefined;
  const cutoff = new Promise<TerminalSeed>((resolve) => {
    timer = setTimeout(() => {
      controller.abort(new Error("Capability timed out"));
      resolve(failureSeed("timeout", "The capability timed out before returning a result.", true));
    }, definition.timeoutMs);
    settleCancellation = () => resolve(cancelledSeed());
    if (input.outerSignal.aborted) settleCancellation();
    else input.outerSignal.addEventListener("abort", settleCancellation, { once: true });
  });

  try {
    const terminal = await Promise.race([execution, cutoff, progressViolation]);
    acceptingUpdates = false;
    void execution.catch(() => undefined);
    return terminal;
  } finally {
    acceptingUpdates = false;
    if (timer) clearTimeout(timer);
    input.outerSignal.removeEventListener("abort", onAbort);
    if (settleCancellation) input.outerSignal.removeEventListener("abort", settleCancellation);
    controller.abort(new Error("Capability execution settled"));
  }
}

function normalizeAdapterSuccess<TContext>(
  rawResult: unknown,
  definition: ErasedCapabilityDefinition<TContext>,
): TerminalSeed {
  if (!rawResult || typeof rawResult !== "object" || !("output" in rawResult)) {
    return failureSeed("adapter_contract", "The capability returned an invalid result.", false);
  }
  const parsedOutput = definition.outputSchema.safeParse((rawResult as { readonly output: unknown }).output);
  if (!parsedOutput.success) {
    return failureSeed("adapter_contract", "The capability returned an invalid result.", false);
  }
  try {
    const output = cloneCanonicalJson(parsedOutput.data);
    if (jsonBytes(output) > definition.maxOutputBytes) {
      return failureSeed("output_too_large", "The capability result was too large to use.", false);
    }
    return {
      outcome: "succeeded",
      errorCode: null,
      retryable: false,
      safeMessage: null,
      output,
      maxOutputBytes: definition.maxOutputBytes,
    };
  } catch {
    return failureSeed("adapter_contract", "The capability returned an invalid result.", false);
  }
}

function normalizeAdapterFailure(error: unknown): TerminalSeed {
  if (!(error instanceof CapabilityAdapterError)) {
    return failureSeed("internal_adapter_error", "The capability failed unexpectedly.", false);
  }
  return failureSeed(
    error.code,
    error.safeMessage,
    error.code === "transient" || error.code === "unavailable",
  );
}

function normalizeDefinition<TContext>(
  definition: CapabilityDefinition<TContext>,
): CapabilityDefinition<TContext> {
  const name = z
    .string()
    .regex(/^[a-z][a-z0-9_]{0,127}$/)
    .parse(definition.name);
  const description = z.string().trim().min(1).max(2_000).parse(definition.description);
  const timeoutMs = boundedInteger(definition.timeoutMs, 1, 300_000, `${name}.timeoutMs`);
  const maxOutputBytes = boundedInteger(
    definition.maxOutputBytes,
    1,
    MAX_TOOL_OUTPUT_BYTES,
    `${name}.maxOutputBytes`,
  );
  const maxProgressBytes = boundedInteger(
    definition.maxProgressBytes ?? MAX_PROGRESS_BYTES,
    1,
    MAX_PROGRESS_BYTES,
    `${name}.maxProgressBytes`,
  );
  const maxProgressEvents = boundedInteger(
    definition.maxProgressEvents ?? 8,
    1,
    MAX_PROGRESS_EVENTS,
    `${name}.maxProgressEvents`,
  );
  return Object.freeze({
    name,
    description,
    modelSchema: cloneCanonicalJson(definition.modelSchema),
    inputSchema: definition.inputSchema,
    outputSchema: definition.outputSchema,
    ...(definition.progressSchema ? { progressSchema: definition.progressSchema } : {}),
    executionMode: definition.executionMode,
    timeoutMs,
    maxOutputBytes,
    maxProgressBytes,
    maxProgressEvents,
    ...(definition.availability ? { availability: definition.availability } : {}),
    ...(definition.admit ? { admit: definition.admit } : {}),
    execute: definition.execute,
  });
}

function normalizeRawCall(call: RawCapabilityCall, sourceIndex: number): NormalizedRawCall {
  const callId = typeof call.callId === "string" ? call.callId.trim() : "";
  const name = typeof call.name === "string" ? call.name.trim() : "";
  const argumentsJson = typeof call.argumentsJson === "string" ? call.argumentsJson : "";
  const displayCallId = callId && callId.length <= 500 ? callId : `invalid-call-${sourceIndex + 1}`;
  const displayName = name && name.length <= 128 ? name : "invalid_capability";
  let problem: string | undefined;
  if (!callId || callId.length > 500) problem = "The model supplied an invalid tool call identifier.";
  else if (!/^[a-z][a-z0-9_]{0,127}$/.test(name)) problem = "The model supplied an invalid capability name.";
  else if (typeof call.argumentsJson !== "string") problem = "The model supplied non-text tool arguments.";
  else if (Buffer.byteLength(argumentsJson, "utf8") > MAX_RAW_ARGUMENT_BYTES) {
    problem = "The model supplied oversized tool arguments.";
  }
  return Object.freeze({
    callId: displayCallId,
    name: displayName,
    argumentsJson,
    sourceIndex,
    ...(problem ? { problem } : {}),
  });
}

function duplicateCallIds(calls: readonly NormalizedRawCall[]): ReadonlySet<string> {
  const counts = new Map<string, number>();
  for (const call of calls) {
    if (call.problem?.includes("identifier")) continue;
    counts.set(call.callId, (counts.get(call.callId) ?? 0) + 1);
  }
  return new Set([...counts].filter(([, count]) => count > 1).map(([callId]) => callId));
}

function parseArguments(
  argumentsJson: string,
  schema: z.ZodType<unknown>,
):
  | { readonly ok: true; readonly arguments: JsonValue }
  | { readonly ok: false; readonly safeMessage: string } {
  let raw: unknown;
  try {
    raw = JSON.parse(argumentsJson) as unknown;
  } catch {
    return { ok: false, safeMessage: "The model supplied malformed JSON arguments." };
  }
  const parsed = schema.safeParse(structuredClone(raw));
  if (!parsed.success) {
    return {
      ok: false,
      safeMessage: "The model supplied arguments that do not match the capability schema.",
    };
  }
  try {
    return { ok: true, arguments: cloneCanonicalJson(parsed.data) };
  } catch {
    return { ok: false, safeMessage: "The model supplied arguments that are not JSON." };
  }
}

function buildTerminalEnvelope(call: NormalizedRawCall, terminal: TerminalSeed): CapabilityTerminalEnvelope {
  const modelPayload = deepFreeze({
    outcome: terminal.outcome,
    output: terminal.output ?? null,
    error:
      terminal.errorCode === null
        ? null
        : {
            code: terminal.errorCode,
            message: terminal.safeMessage,
            retryable: terminal.retryable,
          },
  });
  const modelOutput = canonicalJson(modelPayload);
  const serializedBytes = Buffer.byteLength(modelOutput, "utf8");
  if (serializedBytes > (terminal.maxOutputBytes ?? 4_096) + MAX_ENVELOPE_OVERHEAD_BYTES) {
    throw new Error("Runtime model output exceeded its configured bound");
  }
  return deepFreeze({
    callId: call.callId,
    capabilityName: call.name,
    sourceIndex: call.sourceIndex,
    outcome: terminal.outcome,
    errorCode: terminal.errorCode,
    retryable: terminal.retryable,
    safeMessage: terminal.safeMessage,
    modelOutput,
    serializedBytes,
  });
}

function protocolFailure(errorCode: string, safeMessage: string): TerminalSeed {
  return errorSeed("protocol_rejected", errorCode, safeMessage, false);
}

function cancelledBeforeStart(): TerminalSeed {
  return errorSeed(
    "cancelled",
    "cancelled_before_start",
    "The capability call was cancelled before it started.",
    false,
  );
}

function cancelledSeed(): TerminalSeed {
  return errorSeed("cancelled", "cancelled", "The capability call was cancelled.", false);
}

function failureSeed(errorCode: string, safeMessage: string, retryable: boolean): TerminalSeed {
  return errorSeed("failed", errorCode, safeMessage, retryable);
}

function errorSeed(
  outcome: Exclude<CapabilityTerminalOutcome, "succeeded">,
  errorCode: string,
  safeMessage: string,
  retryable: boolean,
): TerminalSeed {
  return {
    outcome,
    errorCode,
    retryable,
    safeMessage: boundSafeMessage(safeMessage, "The capability could not finish."),
  };
}

function freezeBatch(
  results: readonly (CapabilityTerminalEnvelope | undefined)[],
  events: readonly CapabilityLifecycleEvent[],
): CapabilityBatchResult {
  if (results.some((result) => !result)) {
    throw new Error("Capability lifecycle ended without terminalizing every call");
  }
  return deepFreeze({
    results: results as readonly CapabilityTerminalEnvelope[],
    events: [...events],
  });
}

function cloneCanonicalJson(value: unknown): JsonValue {
  const cloned = structuredClone(value);
  canonicalJson(cloned);
  return deepFreeze(cloned as JsonValue);
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(canonicalize(value));
}

function canonicalize(value: unknown): JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("Non-finite JSON number");
    return value;
  }
  if (Array.isArray(value)) return value.map((entry) => canonicalize(entry));
  if (typeof value !== "object" || Object.getPrototypeOf(value) !== Object.prototype) {
    throw new Error("Value is not plain JSON");
  }
  const record = value as Record<string, unknown>;
  const output: Record<string, JsonValue> = {};
  for (const key of Object.keys(record).sort()) {
    const entry = record[key];
    if (entry === undefined) throw new Error("Undefined is not JSON");
    output[key] = canonicalize(entry);
  }
  return output;
}

function deepFreeze<T>(value: T): T {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const nested of Object.values(value as Record<string, unknown>)) deepFreeze(nested);
  }
  return value;
}

function jsonBytes(value: unknown): number {
  return Buffer.byteLength(canonicalJson(value), "utf8");
}

function boundSafeMessage(message: string, fallback: string): string {
  const normalized = [...message.trim()]
    .map((character) => {
      const code = character.charCodeAt(0);
      return code <= 31 || code === 127 ? " " : character;
    })
    .join("");
  return (normalized || fallback).slice(0, MAX_SAFE_MESSAGE_CHARS);
}

function boundedInteger(value: number, minimum: number, maximum: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${label} must be an integer from ${minimum} through ${maximum}`);
  }
  return value;
}
