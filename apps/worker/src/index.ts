import { createHash, randomUUID } from "node:crypto";
import type { GmailCalendarDraft, HouseholdSignal } from "@florence/contracts";
import {
  type FlorenceRepository,
  HouseholdChiefOfStaff,
  type PersonalSourceReader,
} from "@florence/control-plane";
import type { ClaimedEffect, DueTimer, PostgresFlorenceRepository } from "@florence/database";
import type { WorkerRuntime } from "@florence/runtime";

export type DeliverableEffect = {
  id: string;
  householdId: string;
  idempotencyKey: string;
  kind: "conversation.message";
  conversationId: string;
  conversationAuthorityVersion: number;
  participantSetDigest: string;
  providerConversationId: string;
  expectedAudience: "private" | "group";
  expectedParticipantIdentityDigests: readonly string[];
  episodeId: string | null;
  payload: { text: string };
};

export type EffectExecutionResult =
  | { status: "committed"; providerReceiptId: string; detail: string | null; occurredAt: string }
  | { status: "failed"; providerReceiptId: null; detail: string; occurredAt: string };

export interface EffectExecutor {
  execute(effect: DeliverableEffect): Promise<EffectExecutionResult>;
}

export type DeliverableCalendarEffect = {
  id: string;
  householdId: string;
  idempotencyKey: string;
  kind: "google.calendar.create";
  connectionId: string;
  ownerAdultId: string;
  actionId: string;
  approvalDigest: string;
  candidateId: string;
  candidateVersion: 1;
  candidateDigest: string;
  payload: GmailCalendarDraft;
};

export interface CalendarEffectExecutor {
  executeCalendar(effect: DeliverableCalendarEffect): Promise<EffectExecutionResult>;
}

export type FlorenceWorkerRepository = Omit<
  FlorenceRepository,
  "redeemLinqEnrollment" | "bootstrapLinqHouseholdGroup"
> &
  Pick<
    PostgresFlorenceRepository,
    "claimNextDueTimer" | "releaseTimerClaim" | "claimNextEffect" | "releaseEffectClaim"
  >;

export type FlorenceWorkerOptions = {
  workerId?: string;
  pollIntervalMs?: number;
  leaseMs?: number;
  now?: () => Date;
  onError?: (failure: FlorenceBackgroundFailure) => void;
};

export type FlorenceBackgroundFailure = {
  code: "background_iteration_failed";
  message: "Florence background iteration failed";
};

export interface GmailSyncSource {
  claimNextGmailSync(input: { owner: string; now: string; leaseUntil: string; limit?: number }): Promise<{
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
    leaseOwner: string;
    changes: readonly { messageId: string; threadId: string; historyId: string }[];
    nextCursor: string;
  } | null>;
  releaseGmailSync(input: {
    connectionId: string;
    owner: string;
    nextAt: string;
    cursor?: string;
    error?: string | null;
  }): Promise<void>;
}

/**
 * App-owned dispatcher for accepted signals, timers, and authorized outbox effects.
 * Model work remains isolated behind WorkerRuntime and can only return proposals.
 */
export class FlorenceWorker {
  readonly #chief: HouseholdChiefOfStaff;
  readonly #workerId: string;
  readonly #pollIntervalMs: number;
  readonly #leaseMs: number;
  readonly #now: () => Date;
  readonly #onError: (failure: FlorenceBackgroundFailure) => void;
  #controller: AbortController | null = null;
  #loop: Promise<void> | null = null;
  #closed = false;

  constructor(
    private readonly repository: FlorenceWorkerRepository,
    runtime: WorkerRuntime,
    private readonly effectExecutor: EffectExecutor,
    options: FlorenceWorkerOptions = {},
    personalSourceReader: PersonalSourceReader | null = null,
    private readonly gmailSyncSource: GmailSyncSource | null = null,
    private readonly calendarEffectExecutor: CalendarEffectExecutor | null = null,
  ) {
    this.#now = options.now ?? (() => new Date());
    this.#workerId = options.workerId ?? randomUUID();
    this.#pollIntervalMs = positive(options.pollIntervalMs, 250, "pollIntervalMs");
    this.#leaseMs = positive(options.leaseMs, 30_000, "leaseMs");
    this.#onError = options.onError ?? logBackgroundFailure;
    this.#chief = new HouseholdChiefOfStaff(repository, runtime, this.#now, personalSourceReader);
  }

  async runOnce(): Promise<boolean> {
    if (this.#closed) throw new Error("Florence worker is closed");
    const processedSignal = await this.#chief.processNext(`${this.#workerId}:signal`);
    const acceptedGmail = await this.acceptGmailChanges();
    const acceptedTimer = await this.acceptDueTimer();
    const deliveredEffect = await this.deliverEffect();
    return processedSignal || acceptedGmail || acceptedTimer || deliveredEffect;
  }

  start(): void {
    if (this.#closed) throw new Error("Florence worker is closed");
    if (this.#loop) return;
    this.#controller = new AbortController();
    this.#loop = this.poll(this.#controller.signal);
  }

  async stop(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#controller?.abort();
    await this.#loop;
  }

  private async poll(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      let worked = false;
      try {
        worked = await this.runOnce();
      } catch {
        this.#onError({
          code: "background_iteration_failed",
          message: "Florence background iteration failed",
        });
      }
      if (!worked && !signal.aborted) await pause(this.#pollIntervalMs, signal);
    }
  }

  private claimWindow(suffix: string): { leaseOwner: string; now: string; leaseUntil: string } {
    const now = this.#now();
    return {
      leaseOwner: `${this.#workerId}:${suffix}`,
      now: now.toISOString(),
      leaseUntil: new Date(now.getTime() + this.#leaseMs).toISOString(),
    };
  }

  private async acceptGmailChanges(): Promise<boolean> {
    if (!this.gmailSyncSource) return false;
    const claim = this.claimWindow("gmail");
    const batch = await this.gmailSyncSource.claimNextGmailSync({ ...claim, owner: claim.leaseOwner });
    if (!batch) return false;
    try {
      for (const change of batch.changes) {
        await this.#chief.accept(gmailChangeSignal(batch, change, this.#now().toISOString()));
      }
      await this.gmailSyncSource.releaseGmailSync({
        connectionId: batch.connectionId,
        owner: batch.leaseOwner,
        nextAt: new Date(this.#now().getTime() + 60_000).toISOString(),
        cursor: batch.nextCursor,
        error: null,
      });
    } catch (error) {
      await this.gmailSyncSource.releaseGmailSync({
        connectionId: batch.connectionId,
        owner: batch.leaseOwner,
        nextAt: new Date(this.#now().getTime() + retryDelay(1)).toISOString(),
        error: error instanceof Error ? error.message : "Unknown Gmail ingestion failure",
      });
      throw error;
    }
    return true;
  }

  private async acceptDueTimer(): Promise<boolean> {
    const claim = this.claimWindow("timer");
    const timer = await this.repository.claimNextDueTimer(claim);
    if (!timer) return false;
    try {
      await this.#chief.accept(timerSignal(timer));
    } finally {
      await this.repository.releaseTimerClaim({ timerId: timer.id, leaseOwner: claim.leaseOwner });
    }
    return true;
  }

  private async deliverEffect(): Promise<boolean> {
    const claim = this.claimWindow("effect");
    const effect = await this.repository.claimNextEffect(claim);
    if (!effect) return false;
    try {
      const result = await this.executeEffect(effect);
      await this.#chief.accept(effectReceiptSignal(effect, result));
      await this.repository.releaseEffectClaim({
        effectId: effect.id,
        leaseOwner: claim.leaseOwner,
        availableAt: this.#now().toISOString(),
        lastError: null,
      });
    } catch (error) {
      await this.repository.releaseEffectClaim({
        effectId: effect.id,
        leaseOwner: claim.leaseOwner,
        availableAt: new Date(this.#now().getTime() + retryDelay(effect.attempt)).toISOString(),
        lastError: error instanceof Error ? error.message : "Unknown effect delivery failure",
      });
      throw error;
    }
    return true;
  }

  private async executeEffect(effect: ClaimedEffect): Promise<EffectExecutionResult> {
    if (effect.kind === "google.calendar.create") {
      if (!this.calendarEffectExecutor) return missingCalendarExecutorResult(this.#now().toISOString());
      return this.calendarEffectExecutor.executeCalendar({
        id: effect.id,
        householdId: effect.householdId,
        idempotencyKey: effect.idempotencyKey,
        kind: effect.kind,
        connectionId: effect.connectionId,
        ownerAdultId: effect.ownerAdultId,
        actionId: effect.actionId,
        approvalDigest: effect.approvalDigest,
        candidateId: effect.candidateId,
        candidateVersion: effect.candidateVersion,
        candidateDigest: effect.candidateDigest,
        payload: effect.payload,
      });
    }
    return effect.providerConversationId && effect.audience && effect.expectedParticipantIdentityDigests
      ? this.effectExecutor.execute({
          id: effect.id,
          householdId: effect.householdId,
          idempotencyKey: effect.idempotencyKey,
          kind: effect.kind,
          conversationId: effect.conversationId,
          conversationAuthorityVersion: effect.conversationAuthorityVersion,
          participantSetDigest: effect.participantSetDigest,
          providerConversationId: effect.providerConversationId,
          expectedAudience: effect.audience,
          expectedParticipantIdentityDigests: effect.expectedParticipantIdentityDigests,
          episodeId: effect.episodeId,
          payload: effect.payload,
        })
      : staleAuthorityResult(this.#now().toISOString());
  }
}

function gmailChangeSignal(
  batch: { connectionId: string; householdId: string; ownerAdultId: string },
  change: { messageId: string; threadId: string; historyId: string },
  occurredAt: string,
): Extract<HouseholdSignal, { type: "gmail.message.changed" }> {
  const identity = `gmail.message.changed:${batch.connectionId}:${change.messageId}:${change.historyId}`;
  return {
    type: "gmail.message.changed",
    signalId: deterministicUuid(identity),
    householdId: batch.householdId,
    occurredAt,
    idempotencyKey: identity,
    ownerAdultId: batch.ownerAdultId,
    connectionId: batch.connectionId,
    messageId: change.messageId,
    threadId: change.threadId,
    historyId: change.historyId,
  };
}

function timerSignal(timer: DueTimer): Extract<HouseholdSignal, { type: "timer.fired" }> {
  return {
    type: "timer.fired",
    signalId: deterministicUuid(`timer.fired:${timer.id}`),
    householdId: timer.householdId,
    occurredAt: timer.scheduledFor,
    idempotencyKey: `timer.fired:${timer.id}`,
    timerId: timer.id,
    episodeId: timer.episodeId,
    episodeVersion: timer.episodeVersion,
    scheduledFor: timer.scheduledFor,
  };
}

function effectReceiptSignal(
  effect: ClaimedEffect,
  result: EffectExecutionResult,
): Extract<HouseholdSignal, { type: "effect.receipt" }> {
  return {
    type: "effect.receipt",
    signalId: deterministicUuid(`effect.receipt:${effect.id}`),
    householdId: effect.householdId,
    occurredAt: result.occurredAt,
    idempotencyKey: `effect.receipt:${effect.id}`,
    effectId: effect.id,
    episodeId: effect.kind === "conversation.message" ? effect.episodeId : null,
    status: result.status,
    providerReceiptId: result.providerReceiptId,
    detail: result.detail,
  };
}

function missingCalendarExecutorResult(occurredAt: string): EffectExecutionResult {
  return {
    status: "failed",
    providerReceiptId: null,
    detail: "Google Calendar delivery is not configured.",
    occurredAt,
  };
}

function staleAuthorityResult(occurredAt: string): EffectExecutionResult {
  return {
    status: "failed",
    providerReceiptId: null,
    detail: "Conversation authority changed before delivery.",
    occurredAt,
  };
}

function deterministicUuid(value: string): string {
  const digest = createHash("sha256").update(value).digest("hex");
  const variant = ((Number.parseInt(digest[16] ?? "0", 16) & 3) | 8).toString(16);
  return `${digest.slice(0, 8)}-${digest.slice(8, 12)}-5${digest.slice(13, 16)}-${variant}${digest.slice(17, 20)}-${digest.slice(20, 32)}`;
}

function retryDelay(attempt: number): number {
  return Math.min(60_000, 1_000 * 2 ** Math.min(attempt - 1, 6));
}

function positive(value: number | undefined, fallback: number, name: string): number {
  const selected = value ?? fallback;
  if (!Number.isSafeInteger(selected) || selected <= 0) throw new Error(`${name} must be positive`);
  return selected;
}

function pause(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, milliseconds);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

function logBackgroundFailure(failure: FlorenceBackgroundFailure): void {
  console.error(`[${failure.code}] ${failure.message}`);
}
