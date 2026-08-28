import { createHash } from "node:crypto";
import { z } from "zod";

/**
 * The family can now receive truthful start, progress, cancellation, and terminal
 * behavior when Florence uses an admitted capability.
 *
 * Adapted port provenance:
 * - Pi 4e494929998d6bc4fccf75e0a233f727db4b70ee
 *   - packages/agent/src/types.ts:259-292, 360-408, 421-443
 *   - packages/agent/src/agent-loop.ts:374-405, 408-580, 600-795
 *   - packages/agent/test/agent-loop.test.ts:371-427, 586-675, 787-1028
 *   - packages/agent/test/agent.test.ts:306-443
 * - Hermes Agent 6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882
 *   - tools/registry.py:452-534, 1044-1091, 1097-1168
 *
 * Florence deliberately diverges from Pi's generic agent loop and Hermes's
 * mutable/plugin registry. This module owns a curated immutable registry,
 * rechecks live turn admission, freezes canonical arguments, enforces runtime
 * source/provenance envelopes, and never executes a consequential provider
 * effect. Those differences preserve Florence's household authority, privacy,
 * commitTurn(), provider-settlement, and Linq-delivery boundaries.
 * Pi's agent-session automatic retry helper is intentionally not ported:
 * this layer reports a closed retryable classification but never blindly
 * repeats work; the foreground or durable-work owner decides whether a fresh
 * read is still useful, while effects never enter this registry.
 */

const MAX_RAW_ARGUMENT_BYTES = 1_048_576;
const MAX_TOOL_OUTPUT_BYTES = 1_048_576;
const MAX_PROGRESS_BYTES = 32_768;
const MAX_PROGRESS_EVENTS = 100;
const MAX_EVIDENCE_BYTES = 65_536;
const MAX_ENVELOPE_OVERHEAD_BYTES = 16_384;
const MAX_SOURCES = 32;
const MAX_SAFE_MESSAGE_CHARS = 500;
const DEFAULT_GATE_TIMEOUT_MS = 1_000;

type JsonPrimitive = boolean | number | string | null;
export type JsonValue = JsonPrimitive | readonly JsonValue[] | { readonly [key: string]: JsonValue };

export const capabilitySourceSchema = z
  .object({
    sourceId: z.string().trim().min(1).max(500),
    kind: z.enum(["turn", "public", "gmail", "calendar", "document", "connector", "computed"]),
    ownerId: z.string().trim().min(1).max(500).nullable(),
    visibility: z.enum(["runtime", "public", "adult_private", "household"]),
    provider: z.string().trim().min(1).max(100),
    label: z.string().trim().min(1).max(500),
    observedAt: z.iso.datetime({ offset: true }),
  })
  .strict();

export type CapabilitySource = z.infer<typeof capabilitySourceSchema>;
export type CapabilityConsequence = "read_only" | "effect";
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
  readonly sources: readonly CapabilitySource[];
}

export type CapabilityProgressReporter<TProgress> = (progress: TProgress) => void;

export interface CapabilityExecutionInput<TContext, TArguments, TProgress> {
  readonly callId: string;
  /** Arguments were cloned, canonicalized, and recursively frozen by the runtime. */
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
  readonly phase: "catalog" | "dispatch";
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
  readonly consequence: CapabilityConsequence;
  readonly executionMode: CapabilityExecutionMode;
  readonly timeoutMs: number;
  readonly maxOutputBytes: number;
  readonly maxProgressBytes?: number;
  readonly maxProgressEvents?: number;
  readonly provenance: {
    readonly provider: string;
    readonly adapter: string;
    readonly operation: string;
  };
  readonly availability?: CapabilityAvailabilityProbe<TContext>;
  readonly admit: CapabilityAdmissionPredicate<TContext>;
  readonly execute: (
    input: CapabilityExecutionInput<TContext, TArguments, TProgress>,
  ) => Promise<CapabilityAdapterResult<TOutput>>;
}

type ErasedCapabilityDefinition<TContext> = CapabilityDefinition<TContext>;

/**
 * Preserve adapter-local schema inference while erasing it at the curated
 * registry boundary. The runtime validates before invoking the erased closure.
 */
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
  readonly generation: number;
  readonly tools: readonly CapabilityCatalogEntry[];
}

interface CatalogSnapshotState<TContext> {
  readonly registry: CapabilityRegistry<TContext>;
  readonly admitted: ReadonlyMap<string, ErasedCapabilityDefinition<TContext>>;
}

const catalogSnapshotStates = new WeakMap<CapabilityCatalogSnapshot, CatalogSnapshotState<unknown>>();

export interface RawCapabilityCall {
  readonly callId: unknown;
  readonly name: unknown;
  readonly argumentsJson: unknown;
}

export interface CapabilityProvenance {
  readonly provider: string;
  readonly adapter: string;
  readonly operation: string;
  readonly registryGeneration: number;
  readonly inputDigest: string | null;
  readonly startedAt: string | null;
  readonly finishedAt: string;
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
  readonly sources: readonly CapabilitySource[];
  readonly provenance: CapabilityProvenance;
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
      readonly inputDigest: string;
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
  readonly turnSource: CapabilitySource;
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

interface RegistryOptions {
  readonly generation?: number;
  readonly gateTimeoutMs?: number;
}

interface NormalizedDefinition<TContext> {
  readonly definition: ErasedCapabilityDefinition<TContext>;
  readonly catalog: CapabilityCatalogEntry;
}

/**
 * Immutable, curated registry adapted from Hermes's coherent generation
 * snapshots. Replacement creates a new registry generation; there is no
 * ambient register/override/plugin mutation path for model-callable tools.
 */
export class CapabilityRegistry<TContext> {
  readonly generation: number;
  readonly #gateTimeoutMs: number;
  readonly #entries: ReadonlyMap<string, NormalizedDefinition<TContext>>;

  constructor(definitions: readonly CapabilityDefinition<TContext>[], options: RegistryOptions = {}) {
    this.generation = boundedInteger(options.generation ?? 1, 1, Number.MAX_SAFE_INTEGER, "generation");
    this.#gateTimeoutMs = boundedInteger(
      options.gateTimeoutMs ?? DEFAULT_GATE_TIMEOUT_MS,
      1,
      60_000,
      "gateTimeoutMs",
    );

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

  withDefinitions(definitions: readonly CapabilityDefinition<TContext>[]): CapabilityRegistry<TContext> {
    return new CapabilityRegistry(definitions, {
      generation: this.generation + 1,
      gateTimeoutMs: this.#gateTimeoutMs,
    });
  }

  async catalog(context: Readonly<TContext>, signal?: AbortSignal): Promise<CapabilityCatalogSnapshot> {
    const localSignal = signal ?? new AbortController().signal;
    const probeResults = new Map<CapabilityAvailabilityProbe<TContext>, Promise<boolean>>();
    const admitted = new Map<string, ErasedCapabilityDefinition<TContext>>();
    const tools: CapabilityCatalogEntry[] = [];

    for (const name of [...this.#entries.keys()].sort()) {
      if (localSignal.aborted) break;
      const entry = this.#entries.get(name);
      if (!entry) continue;
      const available = await this.#availability(entry.definition, context, localSignal, probeResults);
      if (!available) continue;
      const allowed = await this.#gate(
        entry.definition.admit,
        {
          phase: "catalog",
          context,
          capabilityName: name,
        },
        localSignal,
      );
      if (!allowed) continue;
      admitted.set(name, entry.definition);
      tools.push(entry.catalog);
    }

    const snapshot = deepFreeze({
      generation: this.generation,
      tools,
    }) satisfies CapabilityCatalogSnapshot;
    catalogSnapshotStates.set(snapshot, {
      registry: this,
      admitted,
    } as CatalogSnapshotState<unknown>);
    return snapshot;
  }

  async executeCalls(input: ExecuteCapabilityCallsInput<TContext>): Promise<CapabilityBatchResult> {
    const snapshotState = catalogSnapshotStates.get(input.snapshot) as
      | CatalogSnapshotState<TContext>
      | undefined;
    if (!snapshotState || snapshotState.registry !== this || input.snapshot.generation !== this.generation) {
      throw new Error("Capability catalog snapshot was not minted by this registry generation");
    }

    const runtimeSource = deepFreeze(capabilitySourceSchema.parse(structuredClone(input.turnSource)));
    const now = input.now ?? (() => new Date());
    const outerSignal = input.signal ?? new AbortController().signal;
    const events: CapabilityLifecycleEvent[] = [];
    const results: Array<CapabilityTerminalEnvelope | undefined> = new Array(input.calls.length);
    const terminalized = new Set<number>();
    const normalizedCalls = input.calls.map((call, sourceIndex) => normalizeRawCall(call, sourceIndex));

    const emit = (event: CapabilityLifecycleEvent): void => {
      const frozen = deepFreeze(structuredClone(event)) as CapabilityLifecycleEvent;
      events.push(frozen);
      try {
        const observed = input.observer?.(frozen);
        if (observed && typeof observed.then === "function") {
          void observed.catch(() => undefined);
        }
      } catch {
        // Pi subscribers are presentation-only. A broken or hanging observer
        // cannot change execution truth or delay terminal settlement.
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

    const terminalize = (
      call: NormalizedRawCall,
      terminal: TerminalSeed,
      definition?: ErasedCapabilityDefinition<TContext>,
    ): CapabilityTerminalEnvelope => {
      const existing = results[call.sourceIndex];
      if (existing) return existing;
      if (terminalized.has(call.sourceIndex)) {
        throw new Error("Capability call reached more than one terminal state");
      }
      terminalized.add(call.sourceIndex);
      const envelope = buildTerminalEnvelope({
        call,
        terminal,
        ...(definition ? { definition } : {}),
        generation: this.generation,
        runtimeSource,
        finishedAt: now().toISOString(),
      });
      results[call.sourceIndex] = envelope;
      emit({ phase: "terminal", terminal: envelope });
      return envelope;
    };

    if (outerSignal.aborted) {
      for (const call of normalizedCalls) {
        terminalize(call, cancelledBeforeStart());
      }
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
    const dispatchProbeResults = new Map<CapabilityAvailabilityProbe<TContext>, Promise<boolean>>();

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
      const originalDefinition = snapshotState.admitted.get(call.name);
      if (!originalDefinition) {
        terminalize(
          call,
          protocolFailure(
            "unknown_or_unavailable_capability",
            "That capability was not available in this turn.",
          ),
        );
        continue;
      }
      const parsed = parseArguments(call.argumentsJson, originalDefinition.inputSchema);
      if (!parsed.ok) {
        terminalize(call, protocolFailure("invalid_arguments", parsed.safeMessage), originalDefinition);
        continue;
      }
      const stillAdmitted = await this.#freshDispatchAdmission(
        originalDefinition,
        input.context,
        parsed.arguments,
        outerSignal,
        dispatchProbeResults,
      );
      if (!stillAdmitted) {
        terminalize(
          call,
          outerSignal.aborted
            ? cancelledBeforeStart()
            : protocolFailure("admission_revoked", "This capability is no longer authorized for the turn."),
          originalDefinition,
        );
        continue;
      }
      const preparedCall = {
        call,
        definition: originalDefinition,
        arguments: parsed.arguments,
        inputDigest: parsed.digest,
      } satisfies PreparedCall<TContext>;
      prepared.push(preparedCall);
      emit({
        phase: "admitted",
        callId: call.callId,
        capabilityName: call.name,
        sourceIndex: call.sourceIndex,
        inputDigest: parsed.digest,
      });
    }

    if (outerSignal.aborted) {
      for (const call of normalizedCalls) {
        if (!results[call.sourceIndex]) terminalize(call, cancelledBeforeStart());
      }
      return freezeBatch(results, events);
    }

    const mayRunInParallel =
      prepared.length > 0 &&
      prepared.every(
        ({ definition }) => definition.executionMode === "parallel" && definition.consequence === "read_only",
      );

    const runOne = async (item: PreparedCall<TContext>): Promise<void> => {
      if (results[item.call.sourceIndex]) return;
      if (outerSignal.aborted) {
        terminalize(item.call, cancelledBeforeStart(), item.definition);
        return;
      }
      const startedAt = now().toISOString();
      emit({
        phase: "running",
        callId: item.call.callId,
        capabilityName: item.call.name,
        sourceIndex: item.call.sourceIndex,
      });
      const terminal = await runPreparedCall({
        item,
        context: input.context,
        outerSignal,
        startedAt,
        emit,
      });
      terminalize(item.call, terminal, item.definition);
    };

    if (mayRunInParallel) {
      await Promise.all(prepared.map((item) => runOne(item)));
    } else {
      for (const item of prepared) {
        await runOne(item);
      }
    }

    // Pi stops its sequential loop on abort. Florence must additionally produce
    // an explicit terminal artifact for every unstarted source-order call.
    for (const call of normalizedCalls) {
      if (!results[call.sourceIndex]) terminalize(call, cancelledBeforeStart());
    }

    return freezeBatch(results, events);
  }

  async #freshDispatchAdmission(
    definition: ErasedCapabilityDefinition<TContext>,
    context: Readonly<TContext>,
    canonicalArguments: JsonValue,
    signal: AbortSignal,
    probeResults: Map<CapabilityAvailabilityProbe<TContext>, Promise<boolean>>,
  ): Promise<boolean> {
    if (!this.#entries.has(definition.name) || signal.aborted) return false;
    if (!(await this.#availability(definition, context, signal, probeResults))) return false;
    return this.#gate(
      definition.admit,
      {
        phase: "dispatch",
        context,
        capabilityName: definition.name,
        canonicalArguments,
      },
      signal,
    );
  }

  async #availability(
    definition: ErasedCapabilityDefinition<TContext>,
    context: Readonly<TContext>,
    signal: AbortSignal,
    cache: Map<CapabilityAvailabilityProbe<TContext>, Promise<boolean>>,
  ): Promise<boolean> {
    if (!definition.availability) return !signal.aborted;
    let result = cache.get(definition.availability);
    if (!result) {
      result = this.#gate(definition.availability, context, signal);
      cache.set(definition.availability, result);
    }
    return result;
  }

  async #gate<TInput>(
    gate: (input: TInput, signal: AbortSignal) => boolean | Promise<boolean>,
    input: TInput,
    signal: AbortSignal,
  ): Promise<boolean> {
    if (signal.aborted) return false;
    const controller = new AbortController();
    const onAbort = () => controller.abort(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    let timer: ReturnType<typeof setTimeout> | undefined;
    let onCutoffAbort: (() => void) | undefined;
    try {
      const cutoff = new Promise<false>((resolve) => {
        timer = setTimeout(() => {
          controller.abort(new Error("Capability gate timed out"));
          resolve(false);
        }, this.#gateTimeoutMs);
        onCutoffAbort = () => resolve(false);
        signal.addEventListener("abort", onCutoffAbort, { once: true });
        if (signal.aborted) onCutoffAbort();
      });
      const decision = Promise.resolve()
        .then(() => gate(input, controller.signal))
        .then((value) => value === true)
        .catch(() => false);
      return await Promise.race([decision, cutoff]);
    } finally {
      if (timer) clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      if (onCutoffAbort) signal.removeEventListener("abort", onCutoffAbort);
      controller.abort(new Error("Capability gate settled"));
    }
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
  readonly inputDigest: string;
}

interface TerminalSeed {
  readonly outcome: CapabilityTerminalOutcome;
  readonly errorCode: string | null;
  readonly retryable: boolean;
  readonly safeMessage: string | null;
  readonly output?: JsonValue;
  readonly adapterSources?: readonly CapabilitySource[];
  readonly inputDigest?: string;
  readonly startedAt?: string;
}

interface TerminalBuildInput<TContext> {
  readonly call: NormalizedRawCall;
  readonly terminal: TerminalSeed;
  readonly definition?: ErasedCapabilityDefinition<TContext>;
  readonly generation: number;
  readonly runtimeSource: CapabilitySource;
  readonly finishedAt: string;
}

async function runPreparedCall<TContext>(input: {
  readonly item: PreparedCall<TContext>;
  readonly context: Readonly<TContext>;
  readonly outerSignal: AbortSignal;
  readonly startedAt: string;
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
      failureSeed(
        "adapter_contract",
        "The capability returned an invalid progress update.",
        false,
        input.item.inputDigest,
        input.startedAt,
      ),
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
    .then((result) => normalizeAdapterSuccess(result, definition, input.item.inputDigest, input.startedAt))
    .catch((error: unknown) => normalizeAdapterFailure(error, input.item.inputDigest, input.startedAt));

  let onCutoffAbort: (() => void) | undefined;
  const cutoff = new Promise<TerminalSeed>((resolve) => {
    timer = setTimeout(() => {
      controller.abort(new Error("Capability timed out"));
      resolve(
        failureSeed(
          "timeout",
          "The capability timed out before returning a result.",
          true,
          input.item.inputDigest,
          input.startedAt,
        ),
      );
    }, definition.timeoutMs);
    onCutoffAbort = () => {
      resolve(cancelledSeed(input.item.inputDigest, input.startedAt));
    };
    if (input.outerSignal.aborted) onCutoffAbort();
    else input.outerSignal.addEventListener("abort", onCutoffAbort, { once: true });
  });

  try {
    const terminal = await Promise.race([execution, cutoff, progressViolation]);
    acceptingUpdates = false;
    // An adapter may ignore cancellation. Its late promise is still observed,
    // but it cannot emit progress or replace this terminal result.
    void execution.catch(() => undefined);
    return terminal;
  } finally {
    acceptingUpdates = false;
    if (timer) clearTimeout(timer);
    input.outerSignal.removeEventListener("abort", onAbort);
    if (onCutoffAbort) input.outerSignal.removeEventListener("abort", onCutoffAbort);
    controller.abort(new Error("Capability execution settled"));
  }
}

function normalizeAdapterSuccess<TContext>(
  rawResult: unknown,
  definition: ErasedCapabilityDefinition<TContext>,
  inputDigest: string,
  startedAt: string,
): TerminalSeed {
  if (!rawResult || typeof rawResult !== "object" || !("output" in rawResult) || !("sources" in rawResult)) {
    return failureSeed(
      "adapter_contract",
      "The capability returned an invalid result.",
      false,
      inputDigest,
      startedAt,
    );
  }
  const result = rawResult as { readonly output: unknown; readonly sources: unknown };
  const parsedOutput = definition.outputSchema.safeParse(result.output);
  const parsedSources = z.array(capabilitySourceSchema).min(1).max(MAX_SOURCES).safeParse(result.sources);
  if (!parsedOutput.success || !parsedSources.success) {
    return failureSeed(
      "adapter_contract",
      "The capability returned an invalid result.",
      false,
      inputDigest,
      startedAt,
    );
  }
  try {
    const output = cloneCanonicalJson(parsedOutput.data);
    if (jsonBytes(output) > definition.maxOutputBytes) {
      return failureSeed(
        "output_too_large",
        "The capability result was too large to use safely.",
        false,
        inputDigest,
        startedAt,
      );
    }
    const adapterSources = deepFreeze(structuredClone(parsedSources.data));
    if (jsonBytes(adapterSources) > MAX_EVIDENCE_BYTES) {
      return failureSeed(
        "evidence_too_large",
        "The capability evidence was too large to use safely.",
        false,
        inputDigest,
        startedAt,
      );
    }
    return {
      outcome: "succeeded",
      errorCode: null,
      retryable: false,
      safeMessage: null,
      output,
      adapterSources,
      inputDigest,
      startedAt,
    };
  } catch {
    return failureSeed(
      "adapter_contract",
      "The capability returned an invalid result.",
      false,
      inputDigest,
      startedAt,
    );
  }
}

function normalizeAdapterFailure(error: unknown, inputDigest: string, startedAt: string): TerminalSeed {
  if (!(error instanceof CapabilityAdapterError)) {
    return failureSeed(
      "internal_adapter_error",
      "The capability failed unexpectedly.",
      false,
      inputDigest,
      startedAt,
    );
  }
  if (error.code === "unknown") {
    return failureSeed(
      "adapter_contract",
      "A read capability returned an invalid unknown outcome.",
      false,
      inputDigest,
      startedAt,
    );
  }
  return failureSeed(
    error.code,
    error.safeMessage,
    error.code === "transient" || error.code === "unavailable",
    inputDigest,
    startedAt,
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
  const provenance = z
    .object({
      provider: z.string().trim().min(1).max(100),
      adapter: z.string().trim().min(1).max(100),
      operation: z.string().trim().min(1).max(100),
    })
    .strict()
    .parse(definition.provenance);
  const timeoutMs = boundedInteger(definition.timeoutMs, 1, 300_000, `${name}.timeoutMs`);
  if (definition.consequence !== "read_only") {
    throw new Error(
      `${name} is an effect capability; model tools may only read or compute, while effects use commitTurn() and provider settlement`,
    );
  }
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
  const modelSchema = cloneCanonicalJson(definition.modelSchema);
  return Object.freeze({
    name,
    description,
    modelSchema,
    inputSchema: definition.inputSchema,
    outputSchema: definition.outputSchema,
    ...(definition.progressSchema ? { progressSchema: definition.progressSchema } : {}),
    consequence: "read_only",
    executionMode: definition.executionMode,
    timeoutMs,
    maxOutputBytes,
    maxProgressBytes,
    maxProgressEvents,
    provenance: deepFreeze(provenance),
    ...(definition.availability ? { availability: definition.availability } : {}),
    admit: definition.admit,
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
  else if (Buffer.byteLength(argumentsJson, "utf8") > MAX_RAW_ARGUMENT_BYTES)
    problem = "The model supplied oversized tool arguments.";
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
  | { readonly ok: true; readonly arguments: JsonValue; readonly digest: string }
  | {
      readonly ok: false;
      readonly safeMessage: string;
    } {
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
    const canonicalArguments = cloneCanonicalJson(parsed.data);
    const serialized = canonicalJson(canonicalArguments);
    return {
      ok: true,
      arguments: canonicalArguments,
      digest: createHash("sha256").update(serialized).digest("hex"),
    };
  } catch {
    return { ok: false, safeMessage: "The model supplied arguments that are not canonical JSON." };
  }
}

function buildTerminalEnvelope<TContext>(input: TerminalBuildInput<TContext>): CapabilityTerminalEnvelope {
  const definition = input.definition;
  const sources = deduplicateSources([input.runtimeSource, ...(input.terminal.adapterSources ?? [])]);
  const provenance = deepFreeze({
    provider: definition?.provenance.provider ?? "florence-runtime",
    adapter: definition?.provenance.adapter ?? "capability-lifecycle",
    operation: definition?.provenance.operation ?? "protocol",
    registryGeneration: input.generation,
    inputDigest: input.terminal.inputDigest ?? null,
    startedAt: input.terminal.startedAt ?? null,
    finishedAt: input.finishedAt,
  });
  const modelPayload = deepFreeze({
    outcome: input.terminal.outcome,
    output: input.terminal.output ?? null,
    error:
      input.terminal.errorCode === null
        ? null
        : {
            code: input.terminal.errorCode,
            message: input.terminal.safeMessage,
            retryable: input.terminal.retryable,
          },
    sources,
    provenance,
  });
  const modelOutput = canonicalJson(modelPayload);
  const serializedBytes = Buffer.byteLength(modelOutput, "utf8");
  const maximum = (definition?.maxOutputBytes ?? 4_096) + MAX_EVIDENCE_BYTES + MAX_ENVELOPE_OVERHEAD_BYTES;
  if (serializedBytes > maximum) {
    throw new Error("Runtime model output envelope exceeded its configured bound");
  }
  const base = {
    callId: input.call.callId,
    capabilityName: input.call.name,
    sourceIndex: input.call.sourceIndex,
    outcome: input.terminal.outcome,
    errorCode: input.terminal.errorCode,
    retryable: input.terminal.retryable,
    safeMessage: input.terminal.safeMessage,
    modelOutput,
    sources,
    provenance,
  };
  return deepFreeze({ ...base, serializedBytes });
}

function deduplicateSources(sources: readonly CapabilitySource[]): readonly CapabilitySource[] {
  const seen = new Set<string>();
  const unique: CapabilitySource[] = [];
  for (const source of sources) {
    if (seen.has(source.sourceId)) continue;
    seen.add(source.sourceId);
    unique.push(source);
  }
  return deepFreeze(unique);
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

function cancelledSeed(inputDigest: string, startedAt: string): TerminalSeed {
  return {
    ...errorSeed("cancelled", "cancelled", "The capability call was cancelled.", false),
    inputDigest,
    startedAt,
  };
}

function failureSeed(
  errorCode: string,
  safeMessage: string,
  retryable: boolean,
  inputDigest: string,
  startedAt: string,
): TerminalSeed {
  return {
    ...errorSeed("failed", errorCode, safeMessage, retryable),
    inputDigest,
    startedAt,
  };
}

function errorSeed(
  outcome: Exclude<CapabilityTerminalOutcome, "succeeded">,
  errorCode: string,
  safeMessage: string,
  retryable: boolean,
): TerminalSeed {
  const boundedMessage = boundSafeMessage(safeMessage, "The capability could not finish.");
  return {
    outcome,
    errorCode,
    retryable,
    safeMessage: boundedMessage,
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
    if (entry === undefined) throw new Error("Undefined is not canonical JSON");
    output[key] = canonicalize(entry);
  }
  return output;
}

function deepFreeze<T>(value: T): T {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const nested of Object.values(value as Record<string, unknown>)) {
      deepFreeze(nested);
    }
  }
  return value;
}

function jsonBytes(value: unknown): number {
  return Buffer.byteLength(canonicalJson(value), "utf8");
}

function boundSafeMessage(message: string, fallback: string): string {
  const normalized =
    typeof message === "string"
      ? [...message.trim()]
          .map((character) => {
            const code = character.charCodeAt(0);
            return code <= 31 || code === 127 ? " " : character;
          })
          .join("")
      : "";
  return (normalized || fallback).slice(0, MAX_SAFE_MESSAGE_CHARS);
}

function boundedInteger(value: number, minimum: number, maximum: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${label} must be an integer from ${minimum} through ${maximum}`);
  }
  return value;
}
