import { createHash } from "node:crypto";
import { z } from "zod";
import {
  ApplicationOutboxIntentSchema,
  ApplicationResultSchema,
  OutboxExecutionResultSchema,
  TimerFiredInputSchema,
} from "../application/contracts.js";
import type { FlorenceApplication } from "../application/ports.js";
import type { ApplicationWorkerHost } from "../application/worker-entrypoint.js";
import { HouseholdIdSchema, InstantStringSchema } from "../domain/index.js";

const ErrorCodeSchema = z.string().regex(/^[a-z][a-z0-9_.-]{0,99}$/);
const JsonObjectSchema = z.record(z.string(), z.json());
const LeaseRequestSchema = z.strictObject({
  owner: z.string().trim().min(1).max(200),
  limit: z.number().int().positive().max(500),
  leaseSeconds: z.number().int().positive().max(86_400),
});

const ProviderInboxLeaseSchema = z.strictObject({
  id: z.uuid(),
  provider: z.string().trim().min(1).max(100),
  idempotencyKey: z.string().trim().min(1).max(512),
  payloadHash: z.string().trim().min(1).max(500),
  authentication: JsonObjectSchema,
  eventKind: z.string().trim().min(1).max(200),
  occurredAt: InstantStringSchema,
  payload: JsonObjectSchema,
  attempt: z.number().int().positive(),
  maxAttempts: z.number().int().positive(),
  leaseToken: z.uuid(),
  leaseExpiresAt: InstantStringSchema,
});

const TimerLeaseSchema = z.strictObject({
  rowId: z.uuid(),
  timerKey: z.string().trim().min(1).max(500),
  householdId: HouseholdIdSchema,
  episodeKey: z.string().trim().min(1).max(500).nullable(),
  triggerKind: z.string().trim().min(1).max(200),
  planVersion: z.number().int().nonnegative(),
  dueAt: InstantStringSchema,
  payload: JsonObjectSchema,
  attempt: z.number().int().positive(),
  leaseToken: z.uuid(),
  leaseExpiresAt: InstantStringSchema,
});

const OutboxLeaseSchema = z.strictObject({
  rowId: z.uuid(),
  intentKey: z.string().trim().min(1).max(500),
  householdId: HouseholdIdSchema,
  effectKind: z.string().trim().min(1).max(200),
  idempotencyKey: z.string().trim().min(1).max(512),
  payload: JsonObjectSchema,
  attempt: z.number().int().positive(),
  maxAttempts: z.number().int().positive(),
  leaseToken: z.uuid(),
  leaseExpiresAt: InstantStringSchema,
});

const TimerPayloadSchema = TimerFiredInputSchema.pick({
  timerId: true,
  episodeId: true,
  temporalPlanVersion: true,
  triggerId: true,
});

const ProviderItemProcessingResultSchema = z.discriminatedUnion("status", [
  z.strictObject({
    status: z.literal("resolved"),
    householdId: HouseholdIdSchema.optional(),
    resolution: JsonObjectSchema,
  }),
  z.strictObject({
    status: z.literal("retryable_failure"),
    errorCode: ErrorCodeSchema,
  }),
  z.strictObject({
    status: z.literal("permanent_failure"),
    householdId: HouseholdIdSchema.optional(),
    errorCode: ErrorCodeSchema,
  }),
]);

export type ProviderInboxLease = z.infer<typeof ProviderInboxLeaseSchema>;
export type TimerLease = z.infer<typeof TimerLeaseSchema>;
export type OutboxLease = z.infer<typeof OutboxLeaseSchema>;
export type ProviderItemProcessingResult = z.infer<typeof ProviderItemProcessingResultSchema>;

export interface ProviderItemProcessor {
  process(item: ProviderInboxLease, application: FlorenceApplication, signal: AbortSignal): Promise<unknown>;
}

/** The complete durable queue seam needed by the worker; ApplicationStore satisfies it structurally. */
export interface QueueStore {
  claimProviderInbox(input: z.input<typeof LeaseRequestSchema>): Promise<unknown[]>;
  resolveProviderInbox(input: {
    readonly inboxId: string;
    readonly leaseToken: string;
    readonly householdId?: string;
    readonly resolution: Record<string, unknown>;
  }): Promise<boolean>;
  failProviderInbox(input: {
    readonly inboxId: string;
    readonly leaseToken: string;
    readonly errorCode: string;
    readonly safeDetail?: string;
    readonly retryAfterSeconds: number;
  }): Promise<"pending" | "dead" | "lost_lease">;

  claimDueTimers(input: z.input<typeof LeaseRequestSchema>): Promise<unknown[]>;
  finishTimer(input: {
    readonly rowId: string;
    readonly leaseToken: string;
    readonly outcome: "fired" | "cancelled" | "superseded";
  }): Promise<boolean>;
  releaseTimer(input: {
    readonly rowId: string;
    readonly leaseToken: string;
    readonly retryAt: string;
    readonly errorCode: string;
  }): Promise<boolean>;

  claimOutbox(input: z.input<typeof LeaseRequestSchema>): Promise<unknown[]>;
  recordOutboxSuccess(input: {
    readonly rowId: string;
    readonly leaseToken: string;
    readonly providerReceipt: Record<string, unknown>;
  }): Promise<boolean>;
  recordOutboxFailure(input: {
    readonly rowId: string;
    readonly leaseToken: string;
    readonly errorCode: string;
    readonly safeDetail?: string;
    readonly retryAfterSeconds: number;
    readonly outcomeCertain: boolean;
  }): Promise<"retry" | "dead" | "ambiguous" | "lost_lease">;
  recordOutboxPermanent(input: {
    readonly rowId: string;
    readonly leaseToken: string;
    readonly errorCode: string;
    readonly safeDetail?: string;
  }): Promise<boolean>;
}

export interface DurableWorkerHostOptions {
  readonly queueStore: QueueStore;
  readonly providerProcessor: ProviderItemProcessor;
  readonly ownerId: string;
  readonly pollIntervalMs?: number;
  readonly leaseSeconds?: number;
  readonly batchSize?: number;
  readonly concurrency?: number;
  readonly baseRetrySeconds?: number;
  readonly maxRetrySeconds?: number;
  readonly jitterRatio?: number;
  readonly now?: () => Date;
  readonly random?: () => number;
}

export interface WorkerCycleReport {
  readonly providerInbox: {
    readonly claimed: number;
    readonly resolved: number;
    readonly retried: number;
    readonly discarded: number;
    readonly lostLease: number;
    readonly failed: number;
  };
  readonly timers: {
    readonly claimed: number;
    readonly fired: number;
    readonly released: number;
    readonly superseded: number;
    readonly lostLease: number;
    readonly failed: number;
  };
  readonly outbox: {
    readonly claimed: number;
    readonly succeeded: number;
    readonly retried: number;
    readonly permanent: number;
    readonly ambiguous: number;
    readonly lostLease: number;
    readonly failed: number;
  };
  readonly claimFailures: number;
}

type MutableCycleReport = {
  providerInbox: {
    claimed: number;
    resolved: number;
    retried: number;
    discarded: number;
    lostLease: number;
    failed: number;
  };
  timers: {
    claimed: number;
    fired: number;
    released: number;
    superseded: number;
    lostLease: number;
    failed: number;
  };
  outbox: {
    claimed: number;
    succeeded: number;
    retried: number;
    permanent: number;
    ambiguous: number;
    lostLease: number;
    failed: number;
  };
  claimFailures: number;
};

type TaggedLease =
  | { readonly queue: "providerInbox"; readonly item: unknown }
  | { readonly queue: "timers"; readonly item: unknown }
  | { readonly queue: "outbox"; readonly item: unknown };

const HostConfigurationSchema = z.strictObject({
  ownerId: z.string().trim().min(1).max(200),
  pollIntervalMs: z.number().int().positive().max(60_000),
  leaseSeconds: z.number().int().positive().max(86_400),
  batchSize: z.number().int().positive().max(500),
  concurrency: z.number().int().positive().max(100),
  baseRetrySeconds: z.number().int().positive().max(86_400),
  maxRetrySeconds: z.number().int().positive().max(604_800),
  jitterRatio: z.number().min(0).max(1),
});

/**
 * A safe error for adapters that know whether an attempted effect can be retried.
 * Unknown exceptions from outbox execution are deliberately treated as ambiguous.
 */
export class QueueExecutionError extends Error {
  override readonly name = "QueueExecutionError";

  constructor(
    readonly code: string,
    readonly outcomeCertain: boolean,
    readonly permanent: boolean,
  ) {
    const parsedCode = ErrorCodeSchema.parse(code);
    super(parsedCode);
    this.code = parsedCode;
  }
}

/** Durable, fair polling host for provider ingress, semantic timers, and application effects. */
export class DurableWorkerHost implements ApplicationWorkerHost {
  readonly #queueStore: QueueStore;
  readonly #providerProcessor: ProviderItemProcessor;
  readonly #ownerId: string;
  readonly #pollIntervalMs: number;
  readonly #leaseSeconds: number;
  readonly #batchSize: number;
  readonly #concurrency: number;
  readonly #baseRetrySeconds: number;
  readonly #maxRetrySeconds: number;
  readonly #jitterRatio: number;
  readonly #now: () => Date;
  readonly #random: () => number;
  #activeCycle: Promise<WorkerCycleReport> | null = null;
  #activeRun: Promise<void> | null = null;

  constructor(options: DurableWorkerHostOptions) {
    const configuration = HostConfigurationSchema.parse({
      ownerId: options.ownerId,
      pollIntervalMs: options.pollIntervalMs ?? 1_000,
      leaseSeconds: options.leaseSeconds ?? 120,
      batchSize: options.batchSize ?? 20,
      concurrency: options.concurrency ?? 8,
      baseRetrySeconds: options.baseRetrySeconds ?? 2,
      maxRetrySeconds: options.maxRetrySeconds ?? 300,
      jitterRatio: options.jitterRatio ?? 0.2,
    });
    if (configuration.baseRetrySeconds > configuration.maxRetrySeconds) {
      throw new Error("baseRetrySeconds cannot exceed maxRetrySeconds");
    }
    this.#queueStore = options.queueStore;
    this.#providerProcessor = options.providerProcessor;
    this.#ownerId = configuration.ownerId;
    this.#pollIntervalMs = configuration.pollIntervalMs;
    this.#leaseSeconds = configuration.leaseSeconds;
    this.#batchSize = configuration.batchSize;
    this.#concurrency = configuration.concurrency;
    this.#baseRetrySeconds = configuration.baseRetrySeconds;
    this.#maxRetrySeconds = configuration.maxRetrySeconds;
    this.#jitterRatio = configuration.jitterRatio;
    this.#now = options.now ?? (() => new Date());
    this.#random = options.random ?? Math.random;
  }

  run(application: FlorenceApplication, signal: AbortSignal = new AbortController().signal): Promise<void> {
    if (this.#activeRun !== null) return this.#activeRun;
    const run = this.#runLoop(application, signal).finally(() => {
      if (this.#activeRun === run) this.#activeRun = null;
    });
    this.#activeRun = run;
    return run;
  }

  runOnce(
    application: FlorenceApplication,
    signal: AbortSignal = new AbortController().signal,
  ): Promise<WorkerCycleReport> {
    if (this.#activeCycle !== null) return this.#activeCycle;
    const cycle = this.#runCycle(application, signal).finally(() => {
      if (this.#activeCycle === cycle) this.#activeCycle = null;
    });
    this.#activeCycle = cycle;
    return cycle;
  }

  async #runLoop(application: FlorenceApplication, signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      await this.runOnce(application, signal);
      if (!signal.aborted) await abortableDelay(this.#pollIntervalMs, signal);
    }
  }

  async #runCycle(application: FlorenceApplication, signal: AbortSignal): Promise<WorkerCycleReport> {
    const report = emptyReport();
    if (signal.aborted) return freezeReport(report);

    const request = LeaseRequestSchema.parse({
      owner: this.#ownerId,
      limit: this.#batchSize,
      leaseSeconds: this.#leaseSeconds,
    });
    const claims = await Promise.allSettled([
      this.#queueStore.claimProviderInbox(request),
      this.#queueStore.claimDueTimers(request),
      this.#queueStore.claimOutbox(request),
    ] as const);
    const providerItems = settledClaims(claims[0], report);
    const timerItems = settledClaims(claims[1], report);
    const outboxItems = settledClaims(claims[2], report);
    report.providerInbox.claimed = providerItems.length;
    report.timers.claimed = timerItems.length;
    report.outbox.claimed = outboxItems.length;

    const work = interleave<TaggedLease>([
      providerItems.map((item) => ({ queue: "providerInbox" as const, item })),
      timerItems.map((item) => ({ queue: "timers" as const, item })),
      outboxItems.map((item) => ({ queue: "outbox" as const, item })),
    ]);
    await forEachBounded(work, this.#concurrency, async (lease) => {
      try {
        switch (lease.queue) {
          case "providerInbox":
            await this.#processProviderItem(lease.item, application, signal, report);
            break;
          case "timers":
            await this.#processTimer(lease.item, application, signal, report);
            break;
          case "outbox":
            await this.#processOutbox(lease.item, application, signal, report);
            break;
        }
      } catch {
        report[lease.queue].failed += 1;
      }
    });
    return freezeReport(report);
  }

  async #processProviderItem(
    rawItem: unknown,
    application: FlorenceApplication,
    signal: AbortSignal,
    report: MutableCycleReport,
  ): Promise<void> {
    const parsedItem = ProviderInboxLeaseSchema.safeParse(rawItem);
    if (!parsedItem.success) {
      report.providerInbox.failed += 1;
      return;
    }
    const item = parsedItem.data;
    if (signal.aborted) {
      await this.#failProvider(item, "worker_aborted", report);
      return;
    }

    let result: ProviderItemProcessingResult;
    try {
      result = ProviderItemProcessingResultSchema.parse(
        await this.#providerProcessor.process(item, application, signal),
      );
    } catch (error) {
      if (error instanceof QueueExecutionError && error.permanent) {
        await this.#discardProvider(item, error.code, undefined, report);
      } else {
        await this.#failProvider(
          item,
          error instanceof QueueExecutionError ? error.code : "provider_processor_failure",
          report,
        );
      }
      return;
    }

    switch (result.status) {
      case "resolved": {
        const settled = await this.#queueStore.resolveProviderInbox({
          inboxId: item.id,
          leaseToken: item.leaseToken,
          ...(result.householdId === undefined ? {} : { householdId: result.householdId }),
          resolution: result.resolution,
        });
        if (settled) report.providerInbox.resolved += 1;
        else report.providerInbox.lostLease += 1;
        break;
      }
      case "retryable_failure":
        await this.#failProvider(item, result.errorCode, report);
        break;
      case "permanent_failure":
        await this.#discardProvider(item, result.errorCode, result.householdId, report);
        break;
    }
  }

  async #failProvider(
    item: ProviderInboxLease,
    errorCode: string,
    report: MutableCycleReport,
  ): Promise<void> {
    const status = await this.#queueStore.failProviderInbox({
      inboxId: item.id,
      leaseToken: item.leaseToken,
      errorCode: ErrorCodeSchema.parse(errorCode),
      retryAfterSeconds: this.#retryDelaySeconds(item.attempt),
    });
    if (status === "pending") report.providerInbox.retried += 1;
    else if (status === "dead") report.providerInbox.discarded += 1;
    else report.providerInbox.lostLease += 1;
  }

  async #discardProvider(
    item: ProviderInboxLease,
    errorCode: string,
    householdId: string | undefined,
    report: MutableCycleReport,
  ): Promise<void> {
    const settled = await this.#queueStore.resolveProviderInbox({
      inboxId: item.id,
      leaseToken: item.leaseToken,
      ...(householdId === undefined ? {} : { householdId }),
      resolution: { status: "discarded", errorCode: ErrorCodeSchema.parse(errorCode) },
    });
    if (settled) report.providerInbox.discarded += 1;
    else report.providerInbox.lostLease += 1;
  }

  async #processTimer(
    rawItem: unknown,
    application: FlorenceApplication,
    signal: AbortSignal,
    report: MutableCycleReport,
  ): Promise<void> {
    const parsedItem = TimerLeaseSchema.safeParse(rawItem);
    if (!parsedItem.success) {
      report.timers.failed += 1;
      return;
    }
    const item = parsedItem.data;
    const payload = TimerPayloadSchema.safeParse(item.payload);
    if (
      !payload.success ||
      item.episodeKey === null ||
      payload.data.timerId !== item.timerKey ||
      payload.data.episodeId !== item.episodeKey ||
      payload.data.temporalPlanVersion !== item.planVersion
    ) {
      const settled = await this.#queueStore.finishTimer({
        rowId: item.rowId,
        leaseToken: item.leaseToken,
        outcome: "superseded",
      });
      if (settled) report.timers.superseded += 1;
      else report.timers.lostLease += 1;
      return;
    }
    if (signal.aborted) {
      await this.#releaseTimer(item, "worker_aborted", report);
      return;
    }

    const firedAt = this.#instantNow();
    const input = TimerFiredInputSchema.parse({
      kind: "timer_fired",
      householdId: item.householdId,
      idempotencyKey: timerIdempotencyKey(item),
      timerId: payload.data.timerId,
      episodeId: payload.data.episodeId,
      temporalPlanVersion: payload.data.temporalPlanVersion,
      triggerId: payload.data.triggerId,
      firedAt,
    });
    try {
      const result = ApplicationResultSchema.parse(await application.process(input));
      if (result.householdId !== input.householdId || result.idempotencyKey !== input.idempotencyKey) {
        throw new QueueExecutionError("timer_result_identity_mismatch", true, false);
      }
      const settled = await this.#queueStore.finishTimer({
        rowId: item.rowId,
        leaseToken: item.leaseToken,
        outcome: "fired",
      });
      if (settled) report.timers.fired += 1;
      else report.timers.lostLease += 1;
    } catch (error) {
      await this.#releaseTimer(
        item,
        error instanceof QueueExecutionError ? error.code : "timer_processing_failure",
        report,
      );
    }
  }

  async #releaseTimer(item: TimerLease, errorCode: string, report: MutableCycleReport): Promise<void> {
    const retryAt = new Date(
      Date.parse(this.#instantNow()) + this.#retryDelaySeconds(item.attempt) * 1_000,
    ).toISOString();
    const settled = await this.#queueStore.releaseTimer({
      rowId: item.rowId,
      leaseToken: item.leaseToken,
      retryAt,
      errorCode: ErrorCodeSchema.parse(errorCode),
    });
    if (settled) report.timers.released += 1;
    else report.timers.lostLease += 1;
  }

  async #processOutbox(
    rawItem: unknown,
    application: FlorenceApplication,
    signal: AbortSignal,
    report: MutableCycleReport,
  ): Promise<void> {
    const parsedItem = OutboxLeaseSchema.safeParse(rawItem);
    if (!parsedItem.success) {
      report.outbox.failed += 1;
      return;
    }
    const item = parsedItem.data;
    const intent = ApplicationOutboxIntentSchema.safeParse(item.payload);
    if (
      !intent.success ||
      intent.data.intentId !== item.intentKey ||
      intent.data.householdId !== item.householdId ||
      intent.data.idempotencyKey !== item.idempotencyKey ||
      intent.data.kind !== item.effectKind
    ) {
      await this.#permanentOutbox(item, "invalid_outbox_payload", report);
      return;
    }
    if (signal.aborted) {
      await this.#retryOutbox(item, "worker_aborted", true, report);
      return;
    }

    let result: z.infer<typeof OutboxExecutionResultSchema>;
    try {
      result = OutboxExecutionResultSchema.parse(
        await application.executeOutbox(intent.data, this.#instantNow()),
      );
      if (result.intentId !== intent.data.intentId) {
        throw new QueueExecutionError("outbox_result_identity_mismatch", false, false);
      }
    } catch (error) {
      if (error instanceof QueueExecutionError && error.outcomeCertain && error.permanent) {
        await this.#permanentOutbox(item, error.code, report);
      } else {
        await this.#retryOutbox(
          item,
          error instanceof QueueExecutionError ? error.code : "outbox_execution_failure",
          error instanceof QueueExecutionError ? error.outcomeCertain : false,
          report,
        );
      }
      return;
    }

    switch (result.status) {
      case "succeeded": {
        const settled = await this.#queueStore.recordOutboxSuccess({
          rowId: item.rowId,
          leaseToken: item.leaseToken,
          providerReceipt: { intentId: result.intentId, status: result.status },
        });
        if (settled) report.outbox.succeeded += 1;
        else report.outbox.lostLease += 1;
        break;
      }
      case "retryable_failure":
        await this.#retryOutbox(item, "outbox_retryable_failure", true, report);
        break;
      case "permanent_failure":
        await this.#permanentOutbox(item, "outbox_permanent_failure", report);
        break;
    }
  }

  async #retryOutbox(
    item: OutboxLease,
    errorCode: string,
    outcomeCertain: boolean,
    report: MutableCycleReport,
  ): Promise<void> {
    const status = await this.#queueStore.recordOutboxFailure({
      rowId: item.rowId,
      leaseToken: item.leaseToken,
      errorCode: ErrorCodeSchema.parse(errorCode),
      retryAfterSeconds: this.#retryDelaySeconds(item.attempt),
      outcomeCertain,
    });
    if (status === "retry") report.outbox.retried += 1;
    else if (status === "dead") report.outbox.permanent += 1;
    else if (status === "ambiguous") report.outbox.ambiguous += 1;
    else report.outbox.lostLease += 1;
  }

  async #permanentOutbox(item: OutboxLease, errorCode: string, report: MutableCycleReport): Promise<void> {
    const settled = await this.#queueStore.recordOutboxPermanent({
      rowId: item.rowId,
      leaseToken: item.leaseToken,
      errorCode: ErrorCodeSchema.parse(errorCode),
    });
    if (settled) report.outbox.permanent += 1;
    else report.outbox.lostLease += 1;
  }

  #retryDelaySeconds(attempt: number): number {
    const exponential = Math.min(
      this.#maxRetrySeconds,
      this.#baseRetrySeconds * 2 ** Math.max(0, attempt - 1),
    );
    const random = this.#random();
    const unit = Number.isFinite(random) ? Math.min(1, Math.max(0, random)) : 0.5;
    const jittered = exponential * (1 - this.#jitterRatio + 2 * this.#jitterRatio * unit);
    return Math.max(1, Math.min(this.#maxRetrySeconds, Math.round(jittered)));
  }

  #instantNow(): string {
    const value = this.#now();
    if (!Number.isFinite(value.getTime())) throw new Error("Worker clock returned an invalid date");
    return value.toISOString();
  }
}

function emptyReport(): MutableCycleReport {
  return {
    providerInbox: { claimed: 0, resolved: 0, retried: 0, discarded: 0, lostLease: 0, failed: 0 },
    timers: { claimed: 0, fired: 0, released: 0, superseded: 0, lostLease: 0, failed: 0 },
    outbox: {
      claimed: 0,
      succeeded: 0,
      retried: 0,
      permanent: 0,
      ambiguous: 0,
      lostLease: 0,
      failed: 0,
    },
    claimFailures: 0,
  };
}

function freezeReport(report: MutableCycleReport): WorkerCycleReport {
  return Object.freeze({
    providerInbox: Object.freeze({ ...report.providerInbox }),
    timers: Object.freeze({ ...report.timers }),
    outbox: Object.freeze({ ...report.outbox }),
    claimFailures: report.claimFailures,
  });
}

function settledClaims<T>(result: PromiseSettledResult<T[]>, report: MutableCycleReport): T[] {
  if (result.status === "fulfilled") return result.value;
  report.claimFailures += 1;
  return [];
}

function interleave<T>(groups: readonly (readonly T[])[]): T[] {
  const result: T[] = [];
  const length = Math.max(0, ...groups.map((group) => group.length));
  for (let index = 0; index < length; index += 1) {
    for (const group of groups) {
      const item = group[index];
      if (item !== undefined) result.push(item);
    }
  }
  return result;
}

async function forEachBounded<T>(
  items: readonly T[],
  concurrency: number,
  operation: (item: T) => Promise<void>,
): Promise<void> {
  let next = 0;
  async function consume(): Promise<void> {
    while (next < items.length) {
      const index = next;
      next += 1;
      const item = items[index];
      if (item !== undefined) await operation(item);
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => consume()));
}

function timerIdempotencyKey(item: TimerLease): string {
  const digest = createHash("sha256")
    .update(`${item.householdId}\0${item.timerKey}\0${item.planVersion}`)
    .digest("hex");
  return `timer_fired:${digest}`;
}

function abortableDelay(durationMs: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(finish, durationMs);
    function finish() {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    }
    signal.addEventListener("abort", finish, { once: true });
  });
}
