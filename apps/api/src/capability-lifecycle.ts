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
const MAX_ENVELOPE_OVERHEAD_BYTES = 4_096;
const MAX_SAFE_MESSAGE_CHARS = 500;

type JsonPrimitive = boolean | number | string | null;
export type JsonValue = JsonPrimitive | readonly JsonValue[] | { readonly [key: string]: JsonValue };

export type CapabilityExecutionMode = "parallel" | "sequential";
export type CapabilityExecutionBoundary = "inline" | "external";
export type CapabilityCompletion = "complete" | "truncated";
export type CapabilityTerminalOutcome = "succeeded" | "failed" | "cancelled" | "protocol_rejected";

export type CapabilityAdapterErrorCode = "unavailable" | "invalid_response" | "transient" | "permanent";

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

export interface CapabilityExecutionInput<TContext, TArguments> {
  readonly callId: string;
  readonly arguments: TArguments;
  readonly context: Readonly<TContext>;
  readonly signal: AbortSignal;
}

export type CapabilityAvailabilityProbe<TContext> = (
  context: Readonly<TContext>,
  signal: AbortSignal,
) => boolean | Promise<boolean>;

export interface CapabilityPresentationInput<TContext> {
  readonly context: Readonly<TContext>;
  readonly capabilityName: string;
  readonly baseDescription: string;
  readonly baseModelSchema: JsonValue;
}

export interface CapabilityPresentation {
  readonly description?: string;
  readonly modelSchema?: JsonValue;
}

/**
 * Adapted port of Hermes's per-definition dynamic_schema_overrides
 * (6dcebea7, tools/registry.py:204-233, 1044-1090). The resolver runs for
 * every catalog snapshot so the model sees the contract for this context,
 * rather than a wider static contract that execution would later reject.
 */
export type CapabilityPresentationResolver<TContext> = (
  input: CapabilityPresentationInput<TContext>,
  signal: AbortSignal,
) => CapabilityPresentation | Promise<CapabilityPresentation>;

export interface CapabilityAdmissionInput<TContext> {
  readonly context: Readonly<TContext>;
  readonly capabilityName: string;
  readonly canonicalArguments?: JsonValue;
}

export type CapabilityAdmissionPredicate<TContext> = (
  input: CapabilityAdmissionInput<TContext>,
  signal: AbortSignal,
) => boolean | Promise<boolean>;

export interface CapabilityExecutionBoundaryInput<TContext> {
  readonly context: Readonly<TContext>;
  readonly capabilityName: string;
  readonly canonicalArguments: JsonValue;
}

export type CapabilityExecutionBoundaryResolver<TContext> = (
  input: CapabilityExecutionBoundaryInput<TContext>,
) => CapabilityExecutionBoundary;

export interface CapabilityDefinition<TContext, TArguments = unknown, TOutput = unknown> {
  readonly name: string;
  readonly description: string;
  readonly modelSchema: JsonValue;
  readonly inputSchema: z.ZodType<TArguments>;
  readonly outputSchema: z.ZodType<TOutput>;
  readonly executionMode: CapabilityExecutionMode;
  /** Resolve only from canonical arguments and current context; callers may replay this after a checkpoint. */
  readonly executionBoundary: CapabilityExecutionBoundary | CapabilityExecutionBoundaryResolver<TContext>;
  readonly timeoutMs: number;
  readonly maxOutputBytes: number;
  readonly availability?: CapabilityAvailabilityProbe<TContext>;
  readonly presentation?: CapabilityPresentationResolver<TContext>;
  readonly admit?: CapabilityAdmissionPredicate<TContext>;
  readonly execute: (
    input: CapabilityExecutionInput<TContext, TArguments>,
  ) => Promise<CapabilityAdapterResult<TOutput>>;
}

type ErasedCapabilityDefinition<TContext> = CapabilityDefinition<TContext>;

/** Preserve adapter-local schema inference while erasing it at the registry seam. */
export function defineCapability<TContext, TArguments, TOutput>(
  definition: CapabilityDefinition<TContext, TArguments, TOutput>,
): CapabilityDefinition<TContext> {
  return definition as unknown as CapabilityDefinition<TContext>;
}

export interface CapabilityCatalogEntry {
  readonly name: string;
  readonly description: string;
  readonly parameters: JsonValue;
  readonly executionBoundary: CapabilityExecutionBoundary;
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
  /** Schema-parsed arguments used for execution; null when the lifecycle could not prepare the call. */
  readonly canonicalArguments: JsonValue | null;
  readonly outcome: CapabilityTerminalOutcome;
  readonly errorCode: string | null;
  readonly retryable: boolean;
  readonly safeMessage: string | null;
  /** Canonical JSON intended for the next model request. */
  readonly modelOutput: string;
  readonly serializedBytes: number;
}

export interface ExecuteCapabilityCallsInput<TContext> {
  readonly snapshot: CapabilityCatalogSnapshot;
  readonly context: Readonly<TContext>;
  readonly calls: readonly RawCapabilityCall[];
  readonly completion: CapabilityCompletion;
  readonly signal?: AbortSignal;
  readonly onStart?: () => void | Promise<void>;
  /**
   * Called after validation, admission, and argument-sensitive boundary
   * resolution, but before any call in a batch executes. Returning true
   * leaves every call unexecuted for a durable checkpoint.
   */
  readonly suspendBeforeExternal?: () => boolean | Promise<boolean>;
}

export interface CapabilityBatchResult {
  /** Terminal envelopes are always returned in assistant source order. */
  readonly results: readonly CapabilityTerminalEnvelope[];
  readonly suspendedBeforeExternal: boolean;
}

interface NormalizedDefinition<TContext> {
  readonly definition: ErasedCapabilityDefinition<TContext>;
  readonly baseCatalog: CapabilityCatalogEntry;
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
          baseCatalog: deepFreeze({
            name: definition.name,
            description: definition.description,
            parameters: cloneCanonicalJson(definition.modelSchema),
            // A call-sensitive definition is conservatively external for
            // legacy catalog consumers. Execution always uses its resolved
            // canonical-argument boundary below.
            executionBoundary:
              typeof definition.executionBoundary === "function" ? "external" : definition.executionBoundary,
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
      const presentation = await settlePresentation(entry, context, localSignal);
      if (presentation) tools.push(presentation);
    }

    return deepFreeze({ tools });
  }

  async executeCalls(input: ExecuteCapabilityCallsInput<TContext>): Promise<CapabilityBatchResult> {
    const availableNames = new Set(input.snapshot.tools.map((tool) => tool.name));
    const outerSignal = input.signal ?? new AbortController().signal;
    const results: Array<CapabilityTerminalEnvelope | undefined> = new Array(input.calls.length);
    const normalizedCalls = input.calls.map((call, sourceIndex) => normalizeRawCall(call, sourceIndex));
    let started = false;
    const notifyStart = async (): Promise<void> => {
      if (started) return;
      started = true;
      try {
        await input.onStart?.();
      } catch {
        // Presentation callbacks do not participate in execution.
      }
    };

    const terminalize = (
      call: NormalizedRawCall,
      terminal: TerminalSeed,
      canonicalArguments: JsonValue | null = null,
    ): CapabilityTerminalEnvelope => {
      const existing = results[call.sourceIndex];
      if (existing) return existing;
      const envelope = buildTerminalEnvelope(call, terminal, canonicalArguments);
      results[call.sourceIndex] = envelope;
      return envelope;
    };

    if (outerSignal.aborted) {
      for (const call of normalizedCalls) terminalize(call, cancelledBeforeStart());
      return freezeBatch(results);
    }

    if (input.completion === "truncated") {
      if (normalizedCalls.length > 0) await notifyStart();
      for (const call of normalizedCalls) {
        terminalize(
          call,
          protocolFailure("truncated_model_output", "The model tool call was truncated and was not run."),
        );
      }
      return freezeBatch(results);
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
      const executionBoundary = resolveExecutionBoundary(definition, input.context, parsed.arguments);
      if (!executionBoundary) {
        terminalize(
          call,
          protocolFailure(
            "invalid_execution_boundary",
            "That capability call could not determine how it should execute.",
          ),
        );
        continue;
      }
      prepared.push({ call, definition, arguments: parsed.arguments, executionBoundary });
    }

    // Adapted from Pi's batch preparation/execution split and truncated-call
    // rejection (4e494929, packages/agent/src/agent-loop.ts:374-526): prepare
    // the whole ordered batch before any adapter runs. Florence adds one
    // durable seam here—if any prepared call is external, the caller may
    // checkpoint the untouched batch before execution.
    if (
      !outerSignal.aborted &&
      prepared.some((item) => item.executionBoundary === "external") &&
      (await input.suspendBeforeExternal?.()) === true
    ) {
      if (!outerSignal.aborted) return suspendedBatch();
    }

    if (normalizedCalls.length > 0) await notifyStart();

    const runOne = async (item: PreparedCall<TContext>): Promise<void> => {
      if (results[item.call.sourceIndex]) return;
      if (outerSignal.aborted) {
        terminalize(item.call, cancelledBeforeStart(), item.arguments);
        return;
      }
      terminalize(
        item.call,
        await runPreparedCall({
          item,
          context: input.context,
          outerSignal,
        }),
        item.arguments,
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
    return freezeBatch(results);
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
  readonly executionBoundary: CapabilityExecutionBoundary;
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

async function settlePresentation<TContext>(
  entry: NormalizedDefinition<TContext>,
  context: Readonly<TContext>,
  signal: AbortSignal,
): Promise<CapabilityCatalogEntry | null> {
  const { definition, baseCatalog } = entry;
  if (!definition.presentation) return baseCatalog;
  if (signal.aborted) return null;
  try {
    const override = await raceAbort(
      Promise.resolve().then(() =>
        definition.presentation?.(
          {
            context,
            capabilityName: definition.name,
            baseDescription: definition.description,
            baseModelSchema: cloneCanonicalJson(definition.modelSchema),
          },
          signal,
        ),
      ),
      signal,
    );
    if (!override) return null;
    const description = z
      .string()
      .trim()
      .min(1)
      .max(2_000)
      .parse(override.description ?? definition.description);
    const parameters = cloneCanonicalJson(override.modelSchema ?? definition.modelSchema);
    return deepFreeze({
      ...baseCatalog,
      description,
      parameters,
    });
  } catch {
    // A dynamic contract that cannot be resolved is omitted rather than
    // exposing a wider static contract that would mislead the model.
    return null;
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

function resolveExecutionBoundary<TContext>(
  definition: ErasedCapabilityDefinition<TContext>,
  context: Readonly<TContext>,
  canonicalArguments: JsonValue,
): CapabilityExecutionBoundary | null {
  if (typeof definition.executionBoundary !== "function") return definition.executionBoundary;
  try {
    const resolved = definition.executionBoundary({
      context,
      capabilityName: definition.name,
      canonicalArguments,
    });
    return resolved === "inline" || resolved === "external" ? resolved : null;
  } catch {
    return null;
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
}): Promise<TerminalSeed> {
  const { definition } = input.item;
  const controller = new AbortController();
  const onAbort = () => controller.abort(input.outerSignal.reason);
  if (input.outerSignal.aborted) controller.abort(input.outerSignal.reason);
  else input.outerSignal.addEventListener("abort", onAbort, { once: true });
  let timer: ReturnType<typeof setTimeout> | undefined;

  const execution = Promise.resolve()
    .then(() =>
      definition.execute({
        callId: input.item.call.callId,
        arguments: input.item.arguments,
        context: input.context,
        signal: controller.signal,
      }),
    )
    .then((result) => normalizeAdapterSuccess(result, definition))
    .catch((error: unknown) => normalizeAdapterFailure(error));

  // Match Pi's started-tool semantics: forward cancellation to the adapter,
  // then await its settlement (or the bounded timeout) so a remote effect's
  // returned handle is not discarded merely because the parent cancelled.
  const cutoff = new Promise<TerminalSeed>((resolve) => {
    timer = setTimeout(() => {
      controller.abort(new Error("Capability timed out"));
      resolve(failureSeed("timeout", "The capability timed out before returning a result.", true));
    }, definition.timeoutMs);
  });

  try {
    const terminal = await Promise.race([execution, cutoff]);
    void execution.catch(() => undefined);
    return terminal;
  } finally {
    if (timer) clearTimeout(timer);
    input.outerSignal.removeEventListener("abort", onAbort);
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
  return Object.freeze({
    name,
    description,
    modelSchema: cloneCanonicalJson(definition.modelSchema),
    inputSchema: definition.inputSchema,
    outputSchema: definition.outputSchema,
    executionMode: definition.executionMode,
    executionBoundary: definition.executionBoundary,
    timeoutMs,
    maxOutputBytes,
    ...(definition.availability ? { availability: definition.availability } : {}),
    ...(definition.presentation ? { presentation: definition.presentation } : {}),
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

function buildTerminalEnvelope(
  call: NormalizedRawCall,
  terminal: TerminalSeed,
  canonicalArguments: JsonValue | null,
): CapabilityTerminalEnvelope {
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
    canonicalArguments,
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

function suspendedBatch(): CapabilityBatchResult {
  return deepFreeze({ results: [], suspendedBeforeExternal: true });
}

function freezeBatch(results: readonly (CapabilityTerminalEnvelope | undefined)[]): CapabilityBatchResult {
  if (results.some((result) => !result)) {
    throw new Error("Capability execution ended without terminalizing every call");
  }
  return deepFreeze({
    results: results as readonly CapabilityTerminalEnvelope[],
    suspendedBeforeExternal: false,
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
