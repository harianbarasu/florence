import { randomUUID } from "node:crypto";
import type { AcceptanceReceipt, ConversationSnapshot, HouseholdSignal } from "@florence/contracts";
import type { ClaimedSignal, HouseholdCommit, PersistedHouseholdEvent } from "@florence/control-plane";
import type { ClaimedEffect, DueTimer, PersistedDeliberation } from "@florence/database";
import { ScriptedWorkerRuntime } from "@florence/runtime";
import { describe, expect, it, vi } from "vitest";
import {
  type CalendarEffectExecutor,
  type EffectExecutor,
  FlorenceWorker,
  type FlorenceWorkerRepository,
} from "./index.js";
import { createWorkerFromEnv } from "./server.js";

class FakeRepository implements FlorenceWorkerRepository {
  pending: HouseholdSignal[] = [];
  events: PersistedHouseholdEvent[] = [];
  accepted: HouseholdSignal[] = [];
  commits: HouseholdCommit[] = [];
  timer: DueTimer | null = null;
  effect: ClaimedEffect | null = null;
  releasedTimer = false;
  releasedEffect: { availableAt: string; lastError: string | null } | null = null;

  async accept(signal: HouseholdSignal, acceptedAt: string): Promise<AcceptanceReceipt> {
    this.accepted.push(signal);
    this.pending.push(signal);
    return {
      signalId: signal.signalId,
      householdId: signal.householdId,
      disposition: "accepted",
      acceptedAt,
    };
  }

  async claimNext(input: { leaseOwner: string }): Promise<ClaimedSignal | null> {
    const signal = this.pending.shift();
    return signal ? { signal, attempt: 1, leaseOwner: input.leaseOwner } : null;
  }

  async loadEvents(): Promise<readonly PersistedHouseholdEvent[]> {
    return this.events;
  }

  async loadRecentConversationTurns(): Promise<ConversationSnapshot["recentTurns"]> {
    return [];
  }

  async loadDeliberation(): Promise<PersistedDeliberation | null> {
    return null;
  }

  async saveDeliberation(
    _input: Parameters<FlorenceWorkerRepository["saveDeliberation"]>[0],
  ): Promise<void> {}

  async commit(input: HouseholdCommit): Promise<void> {
    this.commits.push(input);
    input.events.forEach((event, index) => {
      this.events.push({
        ...event,
        id: randomUUID(),
        householdId: input.householdId,
        signalId: input.signalId,
        version: input.expectedVersion + index + 1,
      });
    });
  }

  async fail(): Promise<void> {}

  async claimNextDueTimer(): Promise<DueTimer | null> {
    const timer = this.timer;
    this.timer = null;
    return timer;
  }

  async releaseTimerClaim(): Promise<void> {
    this.releasedTimer = true;
  }

  async claimNextEffect(): Promise<ClaimedEffect | null> {
    const effect = this.effect;
    this.effect = null;
    return effect;
  }

  async releaseEffectClaim(input: { availableAt: string; lastError: string | null }): Promise<void> {
    this.releasedEffect = input;
  }
}

const now = new Date("2026-08-12T16:00:00.000Z");

function effect(providerConversationId: string | null): ClaimedEffect {
  return {
    id: randomUUID(),
    householdId: randomUUID(),
    idempotencyKey: `effect:${randomUUID()}`,
    kind: "conversation.message",
    conversationId: randomUUID(),
    conversationAuthorityVersion: 3,
    participantSetDigest: "a".repeat(64),
    providerConversationId,
    audience: providerConversationId ? "group" : null,
    expectedParticipantIdentityDigests: providerConversationId ? ["b".repeat(64), "c".repeat(64)] : null,
    episodeId: randomUUID(),
    payload: { text: "The field trip form still needs an owner." },
    occurredAt: now.toISOString(),
    attempt: 1,
    leaseOwner: "ignored-by-worker",
  };
}

function calendarEffect(): ClaimedEffect {
  return {
    id: randomUUID(),
    householdId: randomUUID(),
    idempotencyKey: `google-calendar:${randomUUID()}`,
    kind: "google.calendar.create",
    connectionId: randomUUID(),
    ownerAdultId: randomUUID(),
    actionId: randomUUID(),
    approvalDigest: "d".repeat(64),
    candidateId: randomUUID(),
    candidateVersion: 1,
    candidateDigest: "e".repeat(64),
    episodeId: null,
    payload: {
      title: "School open house",
      startsAt: "2026-08-19T01:00:00.000Z",
      endsAt: "2026-08-19T02:00:00.000Z",
      timeZone: "America/Los_Angeles",
      location: "Harper Elementary",
    },
    occurredAt: now.toISOString(),
    attempt: 1,
    leaseOwner: "ignored-by-worker",
  };
}

function worker(
  repository: FakeRepository,
  executor: EffectExecutor,
  calendarExecutor: CalendarEffectExecutor | null = null,
): FlorenceWorker {
  return new FlorenceWorker(
    repository,
    new ScriptedWorkerRuntime(() => [{ type: "ignore", reason: "quiet" }]),
    executor,
    { workerId: "test-worker", now: () => now, pollIntervalMs: 1 },
    null,
    null,
    calendarExecutor,
  );
}

describe("FlorenceWorker", () => {
  it("processes one signal, ingests one due timer, and delivers one current effect", async () => {
    const repository = new FakeRepository();
    const householdId = randomUUID();
    repository.pending.push({
      type: "household.created",
      signalId: randomUUID(),
      householdId,
      occurredAt: now.toISOString(),
      idempotencyKey: `household:${householdId}`,
      name: "The Example family",
      timeZone: "America/Los_Angeles",
      foundingAdult: { id: randomUUID(), displayName: "Jackson" },
    });
    repository.timer = {
      id: randomUUID(),
      householdId,
      episodeId: randomUUID(),
      episodeVersion: 2,
      scheduledFor: now.toISOString(),
    };
    const currentEffect = effect("provider-group-1");
    repository.effect = currentEffect;
    const execute = vi.fn<EffectExecutor["execute"]>().mockResolvedValue({
      status: "committed",
      providerReceiptId: "provider-receipt-1",
      detail: null,
      occurredAt: now.toISOString(),
    });

    await expect(worker(repository, { execute }).runOnce()).resolves.toBe(true);

    expect(repository.commits).toHaveLength(1);
    expect(repository.accepted.map((signal) => signal.type)).toEqual(["timer.fired", "effect.receipt"]);
    expect(execute).toHaveBeenCalledWith(
      expect.objectContaining({
        idempotencyKey: currentEffect.idempotencyKey,
        providerConversationId: "provider-group-1",
        expectedAudience: "group",
        expectedParticipantIdentityDigests: ["b".repeat(64), "c".repeat(64)],
      }),
    );
    expect(repository.releasedTimer).toBe(true);
    expect(repository.releasedEffect?.lastError).toBeNull();
  });

  it("turns stale authority into a failed app-owned receipt without provider access", async () => {
    const repository = new FakeRepository();
    repository.effect = effect(null);
    const execute = vi.fn<EffectExecutor["execute"]>();

    await worker(repository, { execute }).runOnce();

    expect(execute).not.toHaveBeenCalled();
    expect(repository.accepted[0]).toMatchObject({
      type: "effect.receipt",
      status: "failed",
      detail: "Conversation authority changed before delivery.",
    });
  });

  it("dispatches an approved calendar effect only through its Google executor", async () => {
    const repository = new FakeRepository();
    const approved = calendarEffect();
    repository.effect = approved;
    const execute = vi.fn<EffectExecutor["execute"]>();
    const executeCalendar = vi.fn<CalendarEffectExecutor["executeCalendar"]>().mockResolvedValue({
      status: "committed",
      providerReceiptId: "google-event-42",
      detail: "verified event digest",
      occurredAt: now.toISOString(),
    });

    await expect(worker(repository, { execute }, { executeCalendar }).runOnce()).resolves.toBe(true);

    expect(execute).not.toHaveBeenCalled();
    expect(executeCalendar).toHaveBeenCalledWith(
      expect.objectContaining({
        id: approved.id,
        kind: "google.calendar.create",
        payload: approved.payload,
      }),
    );
    expect(repository.accepted[0]).toMatchObject({
      type: "effect.receipt",
      effectId: approved.id,
      episodeId: null,
      status: "committed",
      providerReceiptId: "google-event-42",
      detail: "verified event digest",
    });
    expect(repository.releasedEffect?.lastError).toBeNull();
  });

  it("fails a calendar effect by receipt when Google delivery is not configured", async () => {
    const repository = new FakeRepository();
    repository.effect = calendarEffect();
    const execute = vi.fn<EffectExecutor["execute"]>();

    await expect(worker(repository, { execute }).runOnce()).resolves.toBe(true);

    expect(execute).not.toHaveBeenCalled();
    expect(repository.accepted[0]).toMatchObject({
      type: "effect.receipt",
      status: "failed",
      detail: "Google Calendar delivery is not configured.",
    });
    expect(repository.releasedEffect?.lastError).toBeNull();
  });

  it("injects private Gmail retrieval only when processing its identifier-only signal", async () => {
    const repository = new FakeRepository();
    const householdId = randomUUID();
    const adultId = randomUUID();
    const privateConversationId = randomUUID();
    const gmailSignal: Extract<HouseholdSignal, { type: "gmail.message.changed" }> = {
      type: "gmail.message.changed",
      signalId: randomUUID(),
      householdId,
      occurredAt: now.toISOString(),
      idempotencyKey: `gmail:${randomUUID()}`,
      ownerAdultId: adultId,
      connectionId: randomUUID(),
      messageId: "message-42",
      threadId: "thread-7",
      historyId: "991",
    };
    repository.pending.push(gmailSignal);
    repository.events.push(
      {
        id: randomUUID(),
        householdId,
        signalId: randomUUID(),
        version: 1,
        type: "household.created",
        householdName: "Family",
        timeZone: "America/Los_Angeles",
        foundingAdult: {
          id: adultId,
          kind: "adult",
          role: "steward",
          displayName: "Jackson",
          relationship: "Parent",
          status: "verified",
        },
      },
      {
        id: randomUUID(),
        householdId,
        signalId: randomUUID(),
        version: 2,
        type: "adult.verified",
        adultId,
        identitySubjectDigest: "c".repeat(64),
        consentVersion: "pilot-v1",
        consentedAt: now.toISOString(),
      },
      {
        id: randomUUID(),
        householdId,
        signalId: randomUUID(),
        version: 3,
        type: "conversation.bound",
        conversation: {
          conversationId: privateConversationId,
          audience: "private",
          authorityVersion: 1,
          participantSetDigest: "d".repeat(64),
          providerConversationId: "linq-private",
          authorizedAdultIds: [adultId],
        },
      },
    );
    const readGmailMessage = vi.fn().mockResolvedValue({
      messageId: gmailSignal.messageId,
      threadId: gmailSignal.threadId,
      historyId: gmailSignal.historyId,
      from: "school@example.com",
      subject: "Permission slip",
      sentAt: now.toISOString(),
      text: "The permission slip is due Friday.",
    });
    const instance = new FlorenceWorker(
      repository,
      new ScriptedWorkerRuntime((input) => [
        {
          type: "stage_gmail_candidate",
          privateSummary: input.gmailEvidence?.text ?? "missing",
          householdMeaning: "The permission slip is due Friday.",
          calendarDraft: null,
          sourceSignalIds: [input.signal.signalId],
        },
      ]),
      { execute: vi.fn() },
      { workerId: "gmail-worker", now: () => now },
      { readGmailMessage },
    );

    await expect(instance.runOnce()).resolves.toBe(true);
    expect(readGmailMessage).toHaveBeenCalledWith({
      householdId,
      ownerAdultId: adultId,
      connectionId: gmailSignal.connectionId,
      messageId: gmailSignal.messageId,
      threadId: gmailSignal.threadId,
      historyId: gmailSignal.historyId,
    });
    expect(repository.commits[0]?.events.map((event) => event.type)).toEqual(["gmail.candidate.staged"]);
  });

  it("turns one claimed Gmail change batch into deterministic ID-only household signals", async () => {
    const repository = new FakeRepository();
    const householdId = randomUUID();
    const ownerAdultId = randomUUID();
    const connectionId = randomUUID();
    const releaseGmailSync = vi.fn().mockResolvedValue(undefined);
    const gmailSyncSource = {
      claimNextGmailSync: vi.fn().mockResolvedValue({
        connectionId,
        householdId,
        ownerAdultId,
        leaseOwner: "gmail-worker:gmail",
        cursor: null,
        changes: [{ messageId: "message-42", threadId: "thread-7", historyId: "991" }],
        nextCursor: "991",
      }),
      releaseGmailSync,
    };
    const instance = new FlorenceWorker(
      repository,
      new ScriptedWorkerRuntime(() => [{ type: "ignore", reason: "not reached" }]),
      { execute: vi.fn() },
      { workerId: "gmail-worker", now: () => now },
      null,
      gmailSyncSource,
    );

    await expect(instance.runOnce()).resolves.toBe(true);
    expect(repository.accepted).toEqual([
      expect.objectContaining({
        type: "gmail.message.changed",
        householdId,
        ownerAdultId,
        connectionId,
        messageId: "message-42",
        threadId: "thread-7",
        historyId: "991",
        idempotencyKey: `gmail.message.changed:${connectionId}:message-42:991`,
      }),
    ]);
    expect(releaseGmailSync).toHaveBeenCalledWith({
      connectionId,
      owner: "gmail-worker:gmail",
      nextAt: "2026-08-12T16:01:00.000Z",
      cursor: "991",
      error: null,
    });
  });

  it("backs off a thrown provider attempt and stops its poll loop cleanly", async () => {
    const repository = new FakeRepository();
    repository.effect = effect("provider-private-1");
    const failure = new Error("provider unavailable");
    const instance = worker(repository, { execute: async () => Promise.reject(failure) });

    await expect(instance.runOnce()).rejects.toThrow(failure);
    expect(repository.releasedEffect).toMatchObject({
      availableAt: "2026-08-12T16:00:01.000Z",
      lastError: failure.message,
    });

    instance.start();
    await instance.stop();
    await expect(instance.runOnce()).rejects.toThrow("Florence worker is closed");
  });

  it("reports a stable background failure without exposing the caught exception", async () => {
    const repository = new FakeRepository();
    repository.effect = effect("provider-private-1");
    let instance: FlorenceWorker;
    const onError = vi.fn(() => {
      void instance.stop();
    });
    instance = new FlorenceWorker(
      repository,
      new ScriptedWorkerRuntime(() => [{ type: "ignore", reason: "quiet" }]),
      {
        execute: async () => Promise.reject(new Error("sensitive provider response")),
      },
      { workerId: "test-worker", now: () => now, pollIntervalMs: 1, onError },
    );

    instance.start();
    await vi.waitFor(() => expect(onError).toHaveBeenCalledTimes(1));
    await instance.stop();

    expect(onError).toHaveBeenCalledWith({
      code: "background_iteration_failed",
      message: "Florence background iteration failed",
    });
    expect(JSON.stringify(onError.mock.calls)).not.toContain("sensitive provider response");
  });
});

describe("createWorkerFromEnv", () => {
  const completeEnv = (): NodeJS.ProcessEnv => ({
    FLORENCE_DATABASE_URL: "postgres://florence:florence@127.0.0.1:5432/florence",
    FLORENCE_POSTGRES_SCHEMA: "florence_test",
    FLORENCE_IMAGE_RETENTION_DAYS: "30",
    FLORENCE_IMAGE_VAULT_KEY: Buffer.alloc(32, 7).toString("base64"),
    LINQ_API_KEY: "linq-test-key",
    OPENAI_API_KEY: "openai-test-key",
    OPENAI_MODEL: "gpt-test",
  });

  it.each([
    "FLORENCE_DATABASE_URL",
    "FLORENCE_POSTGRES_SCHEMA",
    "FLORENCE_IMAGE_VAULT_KEY",
    "LINQ_API_KEY",
    "OPENAI_API_KEY",
    "OPENAI_MODEL",
  ])("fails closed without %s", (name) => {
    const env = completeEnv();
    delete env[name];
    expect(() => createWorkerFromEnv(env)).toThrow(name);
  });

  it("uses valid production configuration and closes idempotently before startup", async () => {
    const service = createWorkerFromEnv(completeEnv());
    await Promise.all([service.stop(), service.stop()]);
  });

  it("rejects an invalid image-retention window", () => {
    expect(() => createWorkerFromEnv({ ...completeEnv(), FLORENCE_IMAGE_RETENTION_DAYS: "0" })).toThrow(
      "FLORENCE_IMAGE_RETENTION_DAYS",
    );
  });
});
