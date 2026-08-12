import { createHash, randomUUID } from "node:crypto";
import type {
  AcceptanceReceipt,
  ConversationSnapshot,
  HouseholdSignal,
  WorkerInput,
  WorkerProposal,
} from "@florence/contracts";
import { ScriptedWorkerRuntime, WorkerRuntimeError } from "@florence/runtime";
import { describe, expect, it } from "vitest";
import {
  type ClaimedSignal,
  type FlorenceRepository,
  HouseholdChiefOfStaff,
  type HouseholdCommit,
  type PersistedHouseholdEvent,
} from "./index.js";

class TestRepository implements FlorenceRepository {
  readonly pending: HouseholdSignal[] = [];
  readonly events: PersistedHouseholdEvent[] = [];
  readonly commits: HouseholdCommit[] = [];
  readonly accepted = new Map<string, AcceptanceReceipt>();
  readonly deliberations = new Map<string, { inputDigest: string; proposals: readonly WorkerProposal[] }>();
  readonly signals = new Map<string, HouseholdSignal>();
  readonly attempts = new Map<string, number>();
  readonly failures: { signalId: string; retryAt: string | null; error: string }[] = [];
  requeueFailures = false;
  failNextCommit = false;

  async accept(signal: HouseholdSignal, acceptedAt: string): Promise<AcceptanceReceipt> {
    const duplicate = this.accepted.get(signal.idempotencyKey);
    if (duplicate) return { ...duplicate, disposition: "duplicate" };
    const receipt: AcceptanceReceipt = {
      signalId: signal.signalId,
      householdId: signal.householdId,
      disposition: "accepted",
      acceptedAt,
    };
    this.accepted.set(signal.idempotencyKey, receipt);
    this.signals.set(signal.signalId, signal);
    this.pending.push(signal);
    return receipt;
  }

  async redeemLinqEnrollment(): Promise<null> {
    return null;
  }

  async bootstrapLinqHouseholdGroup(): Promise<null> {
    return null;
  }

  async claimNext(input: { leaseOwner: string }): Promise<ClaimedSignal | null> {
    const signal = this.pending.shift();
    if (!signal) return null;
    const attempt = (this.attempts.get(signal.signalId) ?? 0) + 1;
    this.attempts.set(signal.signalId, attempt);
    return { signal, attempt, leaseOwner: input.leaseOwner };
  }

  async loadEvents(): Promise<readonly PersistedHouseholdEvent[]> {
    return this.events;
  }

  async loadRecentConversationTurns(): Promise<ConversationSnapshot["recentTurns"]> {
    return [];
  }

  async loadDeliberation(signalId: string) {
    return this.deliberations.get(signalId) ?? null;
  }

  async saveDeliberation(input: {
    signalId: string;
    inputDigest: string;
    proposals: readonly WorkerProposal[];
  }): Promise<void> {
    const existing = this.deliberations.get(input.signalId);
    if (
      existing &&
      (existing.inputDigest !== input.inputDigest ||
        JSON.stringify(existing.proposals) !== JSON.stringify(input.proposals))
    ) {
      throw new Error("conflict");
    }
    this.deliberations.set(input.signalId, {
      inputDigest: input.inputDigest,
      proposals: input.proposals,
    });
  }

  async commit(input: HouseholdCommit): Promise<void> {
    if (this.failNextCommit) {
      this.failNextCommit = false;
      throw new Error("simulated commit interruption");
    }
    this.commits.push(input);
    let version = input.expectedVersion;
    for (const event of input.events) {
      version += 1;
      this.events.push({
        ...event,
        id: randomUUID(),
        householdId: input.householdId,
        signalId: input.signalId,
        version,
      });
    }
  }

  async fail(input: { signalId: string; retryAt: string | null; error: string }): Promise<void> {
    this.failures.push(input);
    const signal = this.signals.get(input.signalId);
    if (this.requeueFailures && input.retryAt && signal) this.pending.push(signal);
  }
}

const householdId = randomUUID();
const jacksonId = randomUUID();
const partnerId = randomUUID();
const childId = randomUUID();
const conversationId = randomUUID();
const privateConversationId = randomUUID();
let tick = 0;
const groupDigest = createHash("sha256")
  .update(JSON.stringify([jacksonId, partnerId].sort()))
  .digest("hex");
const privateDigest = createHash("sha256")
  .update(JSON.stringify([jacksonId]))
  .digest("hex");

type SignalBody<T extends HouseholdSignal> = T extends HouseholdSignal
  ? Omit<T, "signalId" | "householdId" | "occurredAt" | "idempotencyKey">
  : never;

function signal<T extends HouseholdSignal>(input: SignalBody<T>): T {
  tick += 1;
  return {
    ...input,
    signalId: randomUUID(),
    householdId,
    occurredAt: new Date(Date.UTC(2026, 7, 12, 16, 0, tick)).toISOString(),
    idempotencyKey: `test-signal-${tick}`,
  } as unknown as T;
}

async function acceptAndProcess(app: HouseholdChiefOfStaff, value: HouseholdSignal): Promise<void> {
  await app.accept(value);
  await app.processNext("test-worker");
}

function seedEstablishedGroup(repository: TestRepository): void {
  const base = { householdId, signalId: randomUUID(), id: randomUUID() };
  repository.events.push(
    {
      ...base,
      version: 1,
      type: "household.created",
      householdName: "Family",
      timeZone: "America/Los_Angeles",
      foundingAdult: {
        id: jacksonId,
        kind: "adult",
        role: "steward",
        displayName: "Jackson",
        relationship: "Parent",
        status: "verified",
      },
    },
    {
      ...base,
      id: randomUUID(),
      version: 2,
      type: "adult.verified",
      adultId: jacksonId,
      identitySubjectDigest: "c".repeat(64),
      consentVersion: "pilot-v1",
      consentedAt: "2026-08-12T15:00:00.000Z",
    },
    {
      ...base,
      id: randomUUID(),
      version: 3,
      type: "family.member.upserted",
      member: {
        id: partnerId,
        kind: "adult",
        role: "steward",
        displayName: "Partner",
        relationship: "Co-parent",
        status: "planned",
      },
    },
    {
      ...base,
      id: randomUUID(),
      version: 4,
      type: "adult.verified",
      adultId: partnerId,
      identitySubjectDigest: "d".repeat(64),
      consentVersion: "pilot-v1",
      consentedAt: "2026-08-12T15:01:00.000Z",
    },
    {
      ...base,
      id: randomUUID(),
      version: 5,
      type: "conversation.bound",
      conversation: {
        conversationId,
        audience: "group",
        authorityVersion: 1,
        participantSetDigest: groupDigest,
        providerConversationId: "linq-group",
        authorizedAdultIds: [jacksonId, partnerId],
      },
    },
  );
}

function seedJacksonPrivateConversation(repository: TestRepository): void {
  const version = Math.max(0, ...repository.events.map((event) => event.version)) + 1;
  repository.events.push({
    id: randomUUID(),
    householdId,
    signalId: randomUUID(),
    version,
    type: "conversation.bound",
    conversation: {
      conversationId: privateConversationId,
      audience: "private",
      authorityVersion: 1,
      participantSetDigest: privateDigest,
      providerConversationId: "linq-private-jackson",
      authorizedAdultIds: [jacksonId],
    },
  });
}

describe("HouseholdChiefOfStaff", () => {
  it("keeps account stewardship distinct from one-use Linq enrollment authority", async () => {
    const repository = new TestRepository();
    const app = new HouseholdChiefOfStaff(
      repository,
      new ScriptedWorkerRuntime(() => [{ type: "ignore", reason: "not used for enrollment" }]),
      () => new Date("2026-08-12T16:00:00.000Z"),
    );
    await acceptAndProcess(
      app,
      signal({
        type: "household.created",
        name: "Family",
        timeZone: "America/Los_Angeles",
        foundingAdult: { id: jacksonId, displayName: "Jackson" },
      }),
    );
    expect((await app.profile(householdId))?.identityBoundAdultIds).toEqual([]);

    const challengeDigest = "e".repeat(64);
    await acceptAndProcess(
      app,
      signal({
        type: "adult.enrollment.issued",
        actorAdultId: jacksonId,
        adultId: jacksonId,
        challengeDigest,
        expiresAt: "2026-08-13T16:00:00.000Z",
      }),
    );
    await acceptAndProcess(
      app,
      signal({
        type: "adult.enrollment.redeemed",
        adultId: jacksonId,
        challengeDigest,
        identitySubjectDigest: "f".repeat(64),
        consentVersion: "linq-private-code-v1",
        consentedAt: "2026-08-12T16:01:00.000Z",
        conversationId: privateConversationId,
        providerConversationId: "linq-private-jackson",
      }),
    );

    const connected = await app.profile(householdId);
    expect(connected?.identityBoundAdultIds).toEqual([jacksonId]);
    expect(repository.events).toContainEqual(
      expect.objectContaining({
        type: "conversation.bound",
        conversation: expect.objectContaining({
          conversationId: privateConversationId,
          audience: "private",
          authorizedAdultIds: [jacksonId],
        }),
      }),
    );
    expect(repository.commits.at(-1)?.effects[0]).toMatchObject({
      kind: "conversation.message",
      payload: { text: expect.stringContaining("connected to Florence") },
    });
  });

  it("projects family context and keeps an obligation correctable through reassignment and cancellation", async () => {
    const repository = new TestRepository();
    const proposals: WorkerProposal[][] = [];
    const workerInputs: WorkerInput[] = [];
    const runtime = new ScriptedWorkerRuntime((input) => {
      workerInputs.push(input);
      return proposals.shift() ?? [{ type: "ignore", reason: "quiet" }];
    });
    const app = new HouseholdChiefOfStaff(repository, runtime, () => new Date("2026-08-12T16:00:00.000Z"));

    await acceptAndProcess(
      app,
      signal({
        type: "household.created",
        name: "The Barasu family",
        timeZone: "America/Los_Angeles",
        foundingAdult: { id: jacksonId, displayName: "Jackson" },
      }),
    );
    await app.accept(
      signal({
        type: "conversation.bound",
        actorAdultId: jacksonId,
        conversationId: privateConversationId,
        audience: "private",
        authorityVersion: 1,
        participantSetDigest: privateDigest,
        providerConversationId: "linq-private-unbound",
        authorizedAdultIds: [jacksonId],
      }),
    );
    await expect(app.processNext("worker-unbound-conversation")).rejects.toThrow(
      "bound verified household identities",
    );
    repository.events.push({
      id: randomUUID(),
      householdId,
      signalId: randomUUID(),
      version: 2,
      type: "adult.verified",
      adultId: jacksonId,
      identitySubjectDigest: "c".repeat(64),
      consentVersion: "pilot-v1",
      consentedAt: "2026-08-12T16:00:01.000Z",
    });
    await acceptAndProcess(
      app,
      signal({
        type: "family.member.upserted",
        actorAdultId: jacksonId,
        member: {
          id: partnerId,
          kind: "adult",
          role: "steward",
          displayName: "Partner",
          relationship: "Co-parent",
          activities: [],
        },
        status: "planned",
      }),
    );
    repository.events.push({
      id: randomUUID(),
      householdId,
      signalId: randomUUID(),
      version: 4,
      type: "adult.verified",
      adultId: partnerId,
      identitySubjectDigest: "a".repeat(64),
      consentVersion: "pilot-v1",
      consentedAt: "2026-08-12T16:00:03.000Z",
    });
    const childProfile = signal<Extract<HouseholdSignal, { type: "family.member.upserted" }>>({
      type: "family.member.upserted",
      actorAdultId: jacksonId,
      member: {
        id: childId,
        kind: "child",
        role: "dependent",
        displayName: "Harper",
        relationship: "Child",
        aliases: ["Harp"],
        birthYear: 2017,
        school: "Lakeside Elementary",
        currentGrade: "3rd",
        academicYear: "2026-2027",
        gradeEffectiveFrom: "2026-08-10",
        activities: ["Soccer"],
      },
      status: "represented",
    });
    await acceptAndProcess(app, childProfile);
    await acceptAndProcess(
      app,
      signal({
        type: "conversation.bound",
        actorAdultId: jacksonId,
        conversationId,
        audience: "group",
        authorityVersion: 1,
        participantSetDigest: groupDigest,
        providerConversationId: "linq-group-1",
        authorizedAdultIds: [jacksonId, partnerId],
      }),
    );
    await app.accept(
      signal({
        type: "conversation.bound",
        actorAdultId: jacksonId,
        conversationId: randomUUID(),
        audience: "group",
        authorityVersion: 1,
        participantSetDigest: groupDigest,
        providerConversationId: "linq-group-1",
        authorizedAdultIds: [jacksonId, partnerId],
      }),
    );
    await expect(app.processNext("worker-duplicate-provider-chat")).rejects.toThrow(
      "cannot authorize two household conversations",
    );

    const factQuestion = signal<Extract<HouseholdSignal, { type: "conversation.message" }>>({
      type: "conversation.message",
      conversationId,
      audience: "group",
      authorityVersion: 1,
      participantSetDigest: groupDigest,
      senderAdultId: partnerId,
      text: "What school and grade is Harper in?",
      images: [],
      replyToSignalId: null,
      source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
    });
    proposals.push([
      {
        type: "respond",
        text: "Harper is in third grade at Lakeside Elementary.",
        sourceSignalIds: [childProfile.signalId],
      },
    ]);
    await acceptAndProcess(app, factQuestion);
    expect(repository.commits.at(-1)?.effects[0]).toMatchObject({
      kind: "conversation.message",
      payload: { text: "Harper is in third grade at Lakeside Elementary." },
    });

    const obligation = signal<Extract<HouseholdSignal, { type: "conversation.message" }>>({
      type: "conversation.message",
      conversationId,
      audience: "group",
      authorityVersion: 1,
      participantSetDigest: groupDigest,
      senderAdultId: jacksonId,
      text: "The field trip form is due Friday.",
      images: [],
      replyToSignalId: null,
      source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
    });
    proposals.push([
      {
        type: "propose_episode",
        title: "Field trip form",
        outcome: "The field trip form is submitted by Friday.",
        dueAt: "2026-08-14T17:00:00.000Z",
        suggestedOwnerAdultId: null,
        responseText: "The field trip form is due Friday. Who can own it?",
        sourceSignalIds: [obligation.signalId],
      },
    ]);
    await acceptAndProcess(app, obligation);
    const episodeId = repository.commits
      .at(-1)
      ?.events.find((event) => event.type === "episode.proposed")?.episodeId;
    expect(episodeId).toBeDefined();

    const ownership = signal<Extract<HouseholdSignal, { type: "conversation.message" }>>({
      type: "conversation.message",
      conversationId,
      audience: "group",
      authorityVersion: 1,
      participantSetDigest: groupDigest,
      senderAdultId: jacksonId,
      text: "I can take it.",
      images: [],
      replyToSignalId: obligation.signalId,
      source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
    });
    proposals.push([
      {
        type: "set_episode_owner",
        episodeId: episodeId as string,
        ownerAdultId: jacksonId,
        responseText: null,
        sourceSignalIds: [ownership.signalId],
      },
    ]);
    await acceptAndProcess(app, ownership);
    expect(repository.commits.at(-1)?.timers).toHaveLength(1);

    const reassignment = signal<Extract<HouseholdSignal, { type: "conversation.message" }>>({
      type: "conversation.message",
      conversationId,
      audience: "group",
      authorityVersion: 1,
      participantSetDigest: groupDigest,
      senderAdultId: partnerId,
      text: "I'll take the form from Jackson.",
      images: [],
      replyToSignalId: null,
      source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
    });
    proposals.push([
      {
        type: "set_episode_owner",
        episodeId: episodeId as string,
        ownerAdultId: partnerId,
        responseText: null,
        sourceSignalIds: [reassignment.signalId],
      },
    ]);
    await acceptAndProcess(app, reassignment);

    const reassignmentInput = workerInputs.at(-1);
    expect(reassignmentInput?.snapshot).toMatchObject({
      timeZone: "America/Los_Angeles",
      members: [
        { id: jacksonId, kind: "adult", status: "verified" },
        { id: partnerId, kind: "adult", status: "verified" },
        {
          id: childId,
          kind: "child",
          displayName: "Harper",
          aliases: ["Harp"],
          birthYear: 2017,
          school: "Lakeside Elementary",
          currentGrade: "3rd",
          academicYear: "2026-2027",
          gradeEffectiveFrom: "2026-08-10",
          activities: ["Soccer"],
          sourceSignalIds: [childProfile.signalId],
        },
      ],
      openEpisodes: [
        {
          id: episodeId,
          title: "Field trip form",
          dueAt: "2026-08-14T17:00:00.000Z",
          ownerAdultId: jacksonId,
          version: 2,
        },
      ],
    });
    expect(repository.commits.at(-1)?.timers[0]?.episodeVersion).toBe(3);
    expect(repository.commits.at(-1)?.cancelEpisodeIds).toEqual([episodeId]);

    const correction = signal<Extract<HouseholdSignal, { type: "conversation.message" }>>({
      type: "conversation.message",
      conversationId,
      audience: "group",
      authorityVersion: 1,
      participantSetDigest: groupDigest,
      senderAdultId: partnerId,
      text: "The school changed the deadline to Monday.",
      images: [],
      replyToSignalId: null,
      source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
    });
    proposals.push([
      {
        type: "update_episode",
        episodeId: episodeId as string,
        outcome: "The field trip form is submitted by Monday.",
        dueAt: "2026-08-17T17:00:00.000Z",
        responseText: null,
        sourceSignalIds: [correction.signalId],
      },
    ]);
    await acceptAndProcess(app, correction);
    expect(repository.commits.at(-1)?.timers[0]?.episodeVersion).toBe(4);

    const cancellation = signal<Extract<HouseholdSignal, { type: "conversation.message" }>>({
      type: "conversation.message",
      conversationId,
      audience: "group",
      authorityVersion: 1,
      participantSetDigest: groupDigest,
      senderAdultId: partnerId,
      text: "The trip was cancelled, so we don't need the form.",
      images: [],
      replyToSignalId: null,
      source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
    });
    proposals.push([
      {
        type: "cancel_episode",
        episodeId: episodeId as string,
        reason: "The field trip was cancelled.",
        responseText: null,
        sourceSignalIds: [cancellation.signalId],
      },
    ]);
    await acceptAndProcess(app, cancellation);

    expect(repository.events).toContainEqual(
      expect.objectContaining({ type: "episode.cancelled", episodeId }),
    );
    expect(repository.commits.at(-1)?.cancelEpisodeIds).toEqual([episodeId]);
    expect(repository.commits.flatMap((commit) => commit.effects)).toHaveLength(6);
  });

  it("deduplicates a retried signal before cognition", async () => {
    const repository = new TestRepository();
    let modelCalls = 0;
    const app = new HouseholdChiefOfStaff(
      repository,
      new ScriptedWorkerRuntime(() => {
        modelCalls += 1;
        return [{ type: "ignore", reason: "ordinary conversation" }];
      }),
      () => new Date("2026-08-12T16:00:00.000Z"),
    );
    const created = signal<Extract<HouseholdSignal, { type: "household.created" }>>({
      type: "household.created",
      name: "Family",
      timeZone: "America/Los_Angeles",
      foundingAdult: { id: jacksonId, displayName: "Jackson" },
    });

    expect((await app.accept(created)).disposition).toBe("accepted");
    expect((await app.accept(created)).disposition).toBe("duplicate");
    await app.processNext("worker");
    expect(modelCalls).toBe(0);
    expect(repository.commits).toHaveLength(1);
  });

  it("rejects multiple consequential capabilities from one otherwise valid conversation result", async () => {
    const proposalSets = [
      (signalId: string): WorkerProposal[] => [
        { type: "ask", text: "Who can take this?", episodeId: null, sourceSignalIds: [signalId] },
        { type: "ask", text: "When is it due?", episodeId: null, sourceSignalIds: [signalId] },
      ],
      (signalId: string): WorkerProposal[] => [
        {
          type: "propose_episode",
          title: "Permission slip",
          outcome: "The permission slip is returned.",
          dueAt: null,
          suggestedOwnerAdultId: null,
          responseText: null,
          sourceSignalIds: [signalId],
        },
        {
          type: "propose_episode",
          title: "Library book",
          outcome: "The library book is returned.",
          dueAt: null,
          suggestedOwnerAdultId: null,
          responseText: null,
          sourceSignalIds: [signalId],
        },
      ],
      (signalId: string): WorkerProposal[] => [
        { type: "ask", text: "Who can take this?", episodeId: null, sourceSignalIds: [signalId] },
        {
          type: "propose_episode",
          title: "Permission slip",
          outcome: "The permission slip is returned.",
          dueAt: null,
          suggestedOwnerAdultId: null,
          responseText: null,
          sourceSignalIds: [signalId],
        },
      ],
    ];

    for (const proposalsFor of proposalSets) {
      const repository = new TestRepository();
      seedEstablishedGroup(repository);
      const message = signal<Extract<HouseholdSignal, { type: "conversation.message" }>>({
        type: "conversation.message",
        conversationId,
        audience: "group",
        authorityVersion: 1,
        participantSetDigest: groupDigest,
        senderAdultId: jacksonId,
        text: "The permission slip and library book are due Friday.",
        images: [],
        replyToSignalId: null,
        source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
      });
      const app = new HouseholdChiefOfStaff(
        repository,
        new ScriptedWorkerRuntime(() => proposalsFor(message.signalId)),
      );
      await app.accept(message);

      await expect(app.processNext("cardinality-worker")).rejects.toThrow("one consequential capability");
      expect(repository.commits).toEqual([]);
      expect(repository.failures.at(-1)).toMatchObject({ signalId: message.signalId, retryAt: null });
    }
  });

  it("allows one response to remember supporting context in the same conversation turn", async () => {
    const repository = new TestRepository();
    seedEstablishedGroup(repository);
    const message = signal<Extract<HouseholdSignal, { type: "conversation.message" }>>({
      type: "conversation.message",
      conversationId,
      audience: "group",
      authorityVersion: 1,
      participantSetDigest: groupDigest,
      senderAdultId: jacksonId,
      text: "Library pickup is always on Thursdays.",
      images: [],
      replyToSignalId: null,
      source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
    });
    const app = new HouseholdChiefOfStaff(
      repository,
      new ScriptedWorkerRuntime(() => [
        {
          type: "remember",
          statement: "Library pickup is on Thursdays.",
          sourceSignalIds: [message.signalId],
          supersedesMemoryId: null,
        },
        {
          type: "respond",
          text: "Got it — library pickup is on Thursdays.",
          sourceSignalIds: [message.signalId],
        },
      ]),
    );

    await acceptAndProcess(app, message);

    expect(repository.commits.at(-1)?.events.map(({ type }) => type)).toEqual(["memory.remembered"]);
    expect(repository.commits.at(-1)?.effects).toHaveLength(1);
  });

  it("reuses one immutable deliberation after an interrupted commit", async () => {
    const repository = new TestRepository();
    seedEstablishedGroup(repository);
    let modelCalls = 0;
    let currentSignalId = "";
    const app = new HouseholdChiefOfStaff(
      repository,
      new ScriptedWorkerRuntime(() => {
        modelCalls += 1;
        return [
          {
            type: "ask",
            text: "Who can own the field-trip form?",
            episodeId: null,
            sourceSignalIds: [currentSignalId],
          },
        ];
      }),
      () => new Date("2026-08-12T16:00:00.000Z"),
    );
    const message = signal<Extract<HouseholdSignal, { type: "conversation.message" }>>({
      type: "conversation.message",
      conversationId,
      audience: "group",
      authorityVersion: 1,
      participantSetDigest: groupDigest,
      senderAdultId: jacksonId,
      text: "The field-trip form is due Friday.",
      images: [],
      replyToSignalId: null,
      source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
    });
    currentSignalId = message.signalId;
    repository.requeueFailures = true;
    repository.failNextCommit = true;
    await app.accept(message);

    await expect(app.processNext("worker-first")).rejects.toThrow("simulated commit interruption");
    await expect(app.processNext("worker-retry")).resolves.toBe(true);

    expect(modelCalls).toBe(1);
    expect(repository.deliberations.get(message.signalId)?.proposals).toHaveLength(1);
    expect(repository.commits).toHaveLength(1);
    expect(repository.commits[0]?.effects).toHaveLength(1);
  });

  it("does not retry a permanent model failure", async () => {
    const repository = new TestRepository();
    seedEstablishedGroup(repository);
    const app = new HouseholdChiefOfStaff(
      repository,
      new ScriptedWorkerRuntime(() => {
        throw new WorkerRuntimeError("invalid_output", "The model returned an invalid result");
      }),
      () => new Date("2026-08-12T16:00:00.000Z"),
    );
    const message = signal<Extract<HouseholdSignal, { type: "conversation.message" }>>({
      type: "conversation.message",
      conversationId,
      audience: "group",
      authorityVersion: 1,
      participantSetDigest: groupDigest,
      senderAdultId: jacksonId,
      text: "Can you help with the field-trip form?",
      images: [],
      replyToSignalId: null,
      source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
    });
    await app.accept(message);

    await expect(app.processNext("worker-permanent")).rejects.toThrow("invalid result");

    expect(repository.failures.at(-1)).toMatchObject({ signalId: message.signalId, retryAt: null });
  });

  it("binds each adult only through the enrollment flow", async () => {
    const repository = new TestRepository();
    const app = new HouseholdChiefOfStaff(
      repository,
      new ScriptedWorkerRuntime(() => [{ type: "ignore", reason: "not used" }]),
      () => new Date("2026-08-12T16:00:00.000Z"),
    );
    await acceptAndProcess(
      app,
      signal({
        type: "household.created",
        name: "Family",
        timeZone: "America/Los_Angeles",
        foundingAdult: { id: jacksonId, displayName: "Jackson" },
      }),
    );
    const enroll = async (adultId: string, digest: string, conversationId: string) => {
      const challengeDigest = createHash("sha256").update(`${adultId}:challenge`).digest("hex");
      await acceptAndProcess(
        app,
        signal({
          type: "adult.enrollment.issued",
          actorAdultId: jacksonId,
          adultId,
          challengeDigest,
          expiresAt: "2026-08-13T16:00:00.000Z",
        }),
      );
      const redemption = signal<Extract<HouseholdSignal, { type: "adult.enrollment.redeemed" }>>({
        type: "adult.enrollment.redeemed",
        adultId,
        challengeDigest,
        identitySubjectDigest: digest,
        consentVersion: "pilot-v1",
        consentedAt: "2026-08-12T16:01:00.000Z",
        conversationId,
        providerConversationId: `linq-${conversationId}`,
      });
      await acceptAndProcess(app, redemption);
      return redemption;
    };
    const founderRedemption = await enroll(jacksonId, "1".repeat(64), privateConversationId);
    await app.accept({ ...founderRedemption, signalId: randomUUID(), idempotencyKey: randomUUID() });
    await expect(app.processNext("worker-rebind")).rejects.toThrow("challenge is not current");
    await acceptAndProcess(
      app,
      signal({
        type: "family.member.upserted",
        actorAdultId: jacksonId,
        member: {
          id: partnerId,
          kind: "adult",
          role: "steward",
          displayName: "Partner",
          relationship: "Co-parent",
        },
        status: "planned",
      }),
    );
    await app.accept(
      signal({
        type: "family.member.upserted",
        actorAdultId: jacksonId,
        member: {
          id: partnerId,
          kind: "adult",
          role: "steward",
          displayName: "Partner",
          relationship: "Co-parent",
        },
        status: "verified",
      }),
    );
    await expect(app.processNext("worker-invalid")).rejects.toThrow("dedicated identity flow");

    await enroll(partnerId, "b".repeat(64), randomUUID());
    expect((await app.profile(householdId))?.members.find(({ id }) => id === partnerId)?.status).toBe(
      "verified",
    );
  });

  it("keeps personal memory scoped to its adult in private and group projections", async () => {
    const repository = new TestRepository();
    const jacksonPrivateSourceId = randomUUID();
    const partnerPrivateSourceId = randomUUID();
    const sharedSourceId = randomUUID();
    const base = { householdId, signalId: randomUUID(), id: randomUUID() };
    repository.events.push(
      {
        ...base,
        version: 1,
        type: "household.created",
        householdName: "Family",
        timeZone: "America/Los_Angeles",
        foundingAdult: {
          id: jacksonId,
          kind: "adult",
          role: "steward",
          displayName: "Jackson",
          relationship: "Parent",
          status: "verified",
        },
      },
      {
        ...base,
        id: randomUUID(),
        version: 2,
        type: "adult.verified",
        adultId: jacksonId,
        identitySubjectDigest: "e".repeat(64),
        consentVersion: "pilot-v1",
        consentedAt: "2026-08-12T14:58:00.000Z",
      },
      {
        ...base,
        id: randomUUID(),
        version: 3,
        type: "family.member.upserted",
        member: {
          id: partnerId,
          kind: "adult",
          role: "steward",
          displayName: "Partner",
          relationship: "Co-parent",
          status: "planned",
        },
      },
      {
        ...base,
        id: randomUUID(),
        version: 4,
        type: "adult.verified",
        adultId: partnerId,
        identitySubjectDigest: "f".repeat(64),
        consentVersion: "pilot-v1",
        consentedAt: "2026-08-12T14:59:00.000Z",
      },
      {
        ...base,
        id: randomUUID(),
        version: 5,
        type: "conversation.bound",
        conversation: {
          conversationId,
          audience: "group",
          authorityVersion: 1,
          participantSetDigest: groupDigest,
          providerConversationId: "linq-group",
          authorizedAdultIds: [jacksonId, partnerId],
        },
      },
      {
        ...base,
        id: randomUUID(),
        version: 6,
        type: "conversation.bound",
        conversation: {
          conversationId: privateConversationId,
          audience: "private",
          authorityVersion: 1,
          participantSetDigest: privateDigest,
          providerConversationId: "linq-private-jackson",
          authorizedAdultIds: [jacksonId],
        },
      },
      {
        ...base,
        id: randomUUID(),
        version: 7,
        type: "memory.remembered",
        memoryId: randomUUID(),
        statement: "Jackson has a private medical appointment.",
        scope: "personal",
        personId: jacksonId,
        sourceSignalIds: [jacksonPrivateSourceId],
        supersedesMemoryId: null,
        recordedAt: "2026-08-12T15:00:00.000Z",
      },
      {
        ...base,
        id: randomUUID(),
        version: 8,
        type: "memory.remembered",
        memoryId: randomUUID(),
        statement: "Partner is privately considering a job change.",
        scope: "personal",
        personId: partnerId,
        sourceSignalIds: [partnerPrivateSourceId],
        supersedesMemoryId: null,
        recordedAt: "2026-08-12T15:00:30.000Z",
      },
      {
        ...base,
        id: randomUUID(),
        version: 9,
        type: "memory.remembered",
        memoryId: randomUUID(),
        statement: "School starts at eight.",
        scope: "household",
        personId: null,
        sourceSignalIds: [sharedSourceId],
        supersedesMemoryId: null,
        recordedAt: "2026-08-12T15:01:00.000Z",
      },
    );
    const projections: string[][] = [];
    const app = new HouseholdChiefOfStaff(
      repository,
      new ScriptedWorkerRuntime((input) => {
        projections.push(input.snapshot.memories.map((memory) => memory.statement));
        return [{ type: "ignore", reason: "ordinary conversation" }];
      }),
      () => new Date("2026-08-12T16:00:00.000Z"),
    );
    await acceptAndProcess(
      app,
      signal({
        type: "conversation.message",
        conversationId,
        audience: "group",
        authorityVersion: 1,
        participantSetDigest: groupDigest,
        senderAdultId: jacksonId,
        text: "What time does school start?",
        images: [],
        replyToSignalId: null,
        source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
      }),
    );

    await acceptAndProcess(
      app,
      signal({
        type: "conversation.message",
        conversationId: privateConversationId,
        audience: "private",
        authorityVersion: 1,
        participantSetDigest: privateDigest,
        senderAdultId: jacksonId,
        text: "What do you remember about my schedule?",
        images: [],
        replyToSignalId: null,
        source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
      }),
    );

    expect(projections).toEqual([
      ["School starts at eight."],
      ["Jackson has a private medical appointment.", "School starts at eight."],
    ]);
  });

  it("stages Gmail privately and promotes only the exact stored household meaning", async () => {
    const repository = new TestRepository();
    seedEstablishedGroup(repository);
    seedJacksonPrivateConversation(repository);
    const connectionId = randomUUID();
    const gmailSignal = signal<Extract<HouseholdSignal, { type: "gmail.message.changed" }>>({
      type: "gmail.message.changed",
      ownerAdultId: jacksonId,
      connectionId,
      messageId: "gmail-message-42",
      threadId: "gmail-thread-7",
      historyId: "991",
    });
    const privateRawText = "PRIVATE RAW EMAIL BODY: pediatric diagnosis and billing details.";
    const sourceReads: unknown[] = [];
    const stageApp = new HouseholdChiefOfStaff(
      repository,
      new ScriptedWorkerRuntime((input) => [
        {
          type: "stage_gmail_candidate",
          privateSummary: "The school nurse included private medical and billing context.",
          householdMeaning: "The school health form is due Friday.",
          calendarDraft: {
            title: "School health form deadline",
            startsAt: "2026-08-14T16:00:00.000Z",
            endsAt: "2026-08-14T16:30:00.000Z",
            timeZone: "America/Los_Angeles",
            location: null,
          },
          sourceSignalIds: [input.signal.signalId],
        },
      ]),
      () => new Date("2026-08-12T16:00:00.000Z"),
      {
        async readGmailMessage(input) {
          sourceReads.push(input);
          return {
            messageId: "gmail-message-42",
            threadId: "gmail-thread-7",
            historyId: "991",
            from: "nurse@school.example",
            subject: "Health form",
            sentAt: "2026-08-12T15:00:00.000Z",
            text: privateRawText,
          };
        },
      },
    );
    await acceptAndProcess(stageApp, gmailSignal);

    const staged = repository.events.find((event) => event.type === "gmail.candidate.staged");
    if (staged?.type !== "gmail.candidate.staged") throw new Error("missing staged candidate");
    expect(sourceReads).toEqual([
      {
        householdId,
        ownerAdultId: jacksonId,
        connectionId,
        messageId: "gmail-message-42",
        threadId: "gmail-thread-7",
        historyId: "991",
      },
    ]);
    expect(JSON.stringify(gmailSignal)).not.toContain(privateRawText);
    expect(JSON.stringify(staged)).not.toContain(privateRawText);
    expect(repository.commits.at(-1)?.effects).toEqual([
      expect.objectContaining({
        kind: "conversation.message",
        conversationId: privateConversationId,
        payload: expect.objectContaining({
          text: expect.stringContaining(
            "Title: School health form deadline\nStart: 2026-08-14T16:00:00.000Z\nEnd: 2026-08-14T16:30:00.000Z\nTime zone: America/Los_Angeles\nLocation: None",
          ),
        }),
      }),
    ]);
    const stagedReply = repository.commits.at(-1)?.effects[0];
    expect(stagedReply?.kind === "conversation.message" ? stagedReply.payload.text : "").toContain(
      "Sharing and Calendar approval are separate. Reply naturally to approve or decline this exact Calendar draft",
    );

    const promotionMessage = signal<Extract<HouseholdSignal, { type: "conversation.message" }>>({
      type: "conversation.message",
      conversationId: privateConversationId,
      audience: "private",
      authorityVersion: 1,
      participantSetDigest: privateDigest,
      senderAdultId: jacksonId,
      text: "Share",
      images: [],
      replyToSignalId: null,
      source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
    });
    const promotionApp = new HouseholdChiefOfStaff(
      repository,
      new ScriptedWorkerRuntime(() => [
        {
          type: "promote_gmail_candidate",
          candidateId: staged.candidate.candidateId,
          version: staged.candidate.version,
          candidateDigest: staged.candidate.candidateDigest,
          responseText: "Shared that exact family-relevant line.",
          sourceSignalIds: [gmailSignal.signalId, promotionMessage.signalId],
        },
      ]),
      () => new Date("2026-08-12T16:00:00.000Z"),
    );

    const unauthorizedAttempt = async (audience: "private" | "group", senderAdultId: string) => {
      const message = signal<Extract<HouseholdSignal, { type: "conversation.message" }>>({
        type: "conversation.message",
        conversationId: audience === "group" ? conversationId : privateConversationId,
        audience,
        authorityVersion: 1,
        participantSetDigest: audience === "group" ? groupDigest : privateDigest,
        senderAdultId,
        text: "Share",
        images: [],
        replyToSignalId: null,
        source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
      });
      const app = new HouseholdChiefOfStaff(
        repository,
        new ScriptedWorkerRuntime(() => [
          {
            type: "promote_gmail_candidate",
            candidateId: staged.candidate.candidateId,
            version: 1,
            candidateDigest: staged.candidate.candidateDigest,
            responseText: "Shared.",
            sourceSignalIds: [gmailSignal.signalId, message.signalId],
          },
        ]),
        () => new Date("2026-08-12T16:00:00.000Z"),
      );
      await app.accept(message);
      return app.processNext("unauthorized-worker");
    };
    await expect(unauthorizedAttempt("group", jacksonId)).rejects.toThrow(
      "evidence outside its household snapshot",
    );
    await expect(unauthorizedAttempt("private", partnerId)).rejects.toThrow(
      "outside current household authority",
    );
    expect(repository.events.filter((event) => event.type === "gmail.candidate.promoted")).toHaveLength(0);

    await acceptAndProcess(promotionApp, promotionMessage);

    const promotionCommit = repository.commits.at(-1);
    expect(promotionCommit?.events.map((event) => event.type)).toEqual([
      "gmail.candidate.promoted",
      "episode.proposed",
    ]);
    const groupEffect = promotionCommit?.effects.find(
      (effect) => effect.kind === "conversation.message" && effect.conversationId === conversationId,
    );
    expect(groupEffect).toMatchObject({ payload: { text: "The school health form is due Friday." } });
    expect(JSON.stringify(groupEffect)).not.toContain("medical");
    expect(JSON.stringify(groupEffect)).not.toContain(privateRawText);

    const commitsBeforeReplay = repository.commits.length;
    await expect(promotionApp.accept(promotionMessage)).resolves.toMatchObject({ disposition: "duplicate" });
    await expect(promotionApp.processNext("replay-worker")).resolves.toBe(false);
    expect(repository.commits).toHaveLength(commitsBeforeReplay);
  });

  it("shows one exact private Calendar draft and app-verifies natural owner consent once", async () => {
    const connectionId = randomUUID();
    const partnerPrivateConversationId = randomUUID();
    const partnerPrivateDigest = createHash("sha256")
      .update(JSON.stringify([partnerId]))
      .digest("hex");
    const calendarDraft = {
      title: "Harper school open house",
      startsAt: "2026-08-20T01:00:00.000Z",
      endsAt: "2026-08-20T02:00:00.000Z",
      timeZone: "America/Los_Angeles",
      location: "Lakeside Elementary",
    };

    const stageCandidate = async () => {
      const repository = new TestRepository();
      seedEstablishedGroup(repository);
      seedJacksonPrivateConversation(repository);
      const gmailSignal = signal<Extract<HouseholdSignal, { type: "gmail.message.changed" }>>({
        type: "gmail.message.changed",
        ownerAdultId: jacksonId,
        connectionId,
        messageId: `gmail-open-house-${randomUUID()}`,
        threadId: "gmail-thread-open-house",
        historyId: "1001",
      });
      const app = new HouseholdChiefOfStaff(
        repository,
        new ScriptedWorkerRuntime((input) => [
          {
            type: "stage_gmail_candidate",
            privateSummary: "Harper's school open house is next Wednesday.",
            householdMeaning: "Harper's school open house is next Wednesday.",
            calendarDraft,
            sourceSignalIds: [input.signal.signalId],
          },
        ]),
        () => new Date("2026-08-12T16:00:00.000Z"),
        {
          async readGmailMessage() {
            return {
              messageId: gmailSignal.messageId,
              threadId: gmailSignal.threadId,
              historyId: gmailSignal.historyId,
              from: "school@example.test",
              subject: "Open house",
              sentAt: "2026-08-12T15:00:00.000Z",
              text: "Harper's school open house is next Wednesday.",
            };
          },
        },
      );
      await acceptAndProcess(app, gmailSignal);
      const staged = repository.events.find(
        (event) => event.type === "gmail.candidate.staged" && event.signalId === gmailSignal.signalId,
      );
      if (staged?.type !== "gmail.candidate.staged") throw new Error("missing staged candidate");
      const presentation = repository.commits
        .at(-1)
        ?.effects.find((effect) => effect.kind === "conversation.message");
      const presentationText = presentation?.kind === "conversation.message" ? presentation.payload.text : "";
      return { repository, staged, gmailSignal, presentationText };
    };
    const message = (
      text: string,
      audience: "private" | "group" = "private",
      senderAdultId = jacksonId,
      authorityVersion = 1,
    ) =>
      signal<Extract<HouseholdSignal, { type: "conversation.message" }>>({
        type: "conversation.message",
        conversationId:
          audience === "group"
            ? conversationId
            : senderAdultId === jacksonId
              ? privateConversationId
              : partnerPrivateConversationId,
        audience,
        authorityVersion,
        participantSetDigest:
          audience === "group"
            ? groupDigest
            : senderAdultId === jacksonId
              ? privateDigest
              : partnerPrivateDigest,
        senderAdultId,
        text,
        images: [],
        replyToSignalId: null,
        source: { system: "linq-v3", providerEventId: randomUUID(), providerMessageId: randomUUID() },
      });
    const approval = (
      fixture: Awaited<ReturnType<typeof stageCandidate>>,
      current: ReturnType<typeof message>,
      overrides: Partial<{ candidateId: string; candidateDigest: string; sourceSignalIds: string[] }> = {},
    ): WorkerProposal => ({
      type: "approve_gmail_calendar",
      candidateId: overrides.candidateId ?? fixture.staged.candidate.candidateId,
      version: 1,
      candidateDigest: overrides.candidateDigest ?? fixture.staged.candidate.candidateDigest,
      sourceSignalIds: overrides.sourceSignalIds ?? [fixture.gmailSignal.signalId, current.signalId],
    });
    const process = async (
      fixture: Awaited<ReturnType<typeof stageCandidate>>,
      current: ReturnType<typeof message>,
      proposal: WorkerProposal,
    ) => {
      const inputs: WorkerInput[] = [];
      const app = new HouseholdChiefOfStaff(
        fixture.repository,
        new ScriptedWorkerRuntime((input) => {
          inputs.push(input);
          return [proposal];
        }),
      );
      await app.accept(current);
      const processed = app.processNext("calendar-consent-worker");
      return { app, inputs, processed };
    };

    const fixture = await stageCandidate();
    expect(fixture.presentationText).toContain(
      "Title: Harper school open house\nStart: 2026-08-20T01:00:00.000Z\nEnd: 2026-08-20T02:00:00.000Z\nTime zone: America/Los_Angeles\nLocation: Lakeside Elementary",
    );
    expect(fixture.presentationText).toContain(
      "Reply naturally to approve or decline this exact Calendar draft",
    );
    expect(fixture.presentationText).not.toContain("ADD TO CALENDAR");

    const explicit = message("Yes, add that exact event to my calendar.");
    const valid = await process(fixture, explicit, approval(fixture, explicit));
    await expect(valid.processed).resolves.toBe(true);
    expect(valid.inputs[0]?.snapshot.privateCalendarApprovalCandidate).toMatchObject({
      candidateId: fixture.staged.candidate.candidateId,
      candidateDigest: fixture.staged.candidate.candidateDigest,
      calendarDraft,
    });
    const commit = fixture.repository.commits.at(-1);
    expect(commit?.events).toEqual([
      expect.objectContaining({
        type: "gmail.calendar.approved",
        approvedByAdultId: jacksonId,
        conversationId: privateConversationId,
        conversationAuthorityVersion: 1,
        candidate: {
          candidateId: fixture.staged.candidate.candidateId,
          version: 1,
          candidateDigest: fixture.staged.candidate.candidateDigest,
        },
      }),
    ]);
    expect(commit?.effects).toEqual([
      expect.objectContaining({
        kind: "google.calendar.create",
        connectionId,
        candidateId: fixture.staged.candidate.candidateId,
        candidateVersion: 1,
        candidateDigest: fixture.staged.candidate.candidateDigest,
        approvalDigest: expect.stringMatching(/^[a-f0-9]{64}$/),
        payload: calendarDraft,
      }),
    ]);

    for (const text of ["Sounds good", "Calendar stuff maybe"]) {
      const ambiguous = await stageCandidate();
      const ambiguousMessage = message(text);
      const ignored = await process(ambiguous, ambiguousMessage, {
        type: "ignore",
        reason: "No unambiguous Calendar consent.",
      });
      await expect(ignored.processed).resolves.toBe(true);
      expect(ambiguous.repository.commits.at(-1)?.effects).toEqual([]);
    }

    const forged = await stageCandidate();
    const forgedMessage = message("Yes, add it.");
    const forgedAttempt = await process(
      forged,
      forgedMessage,
      approval(forged, forgedMessage, { candidateId: randomUUID() }),
    );
    await expect(forgedAttempt.processed).rejects.toThrow("exactly one current private draft");

    const group = await stageCandidate();
    const groupMessage = message("Add that exact event", "group");
    const groupAttempt = await process(
      group,
      groupMessage,
      approval(group, groupMessage, { sourceSignalIds: [groupMessage.signalId] }),
    );
    await expect(groupAttempt.processed).rejects.toThrow();

    const other = await stageCandidate();
    other.repository.events.push({
      id: randomUUID(),
      householdId,
      signalId: randomUUID(),
      version: Math.max(...other.repository.events.map((event) => event.version)) + 1,
      type: "conversation.bound",
      conversation: {
        conversationId: partnerPrivateConversationId,
        audience: "private",
        authorityVersion: 1,
        participantSetDigest: partnerPrivateDigest,
        providerConversationId: "linq-private-partner",
        authorizedAdultIds: [partnerId],
      },
    });
    const otherMessage = message("Add that exact event", "private", partnerId);
    const otherAttempt = await process(
      other,
      otherMessage,
      approval(other, otherMessage, { sourceSignalIds: [otherMessage.signalId] }),
    );
    await expect(otherAttempt.processed).rejects.toThrow();

    const stale = await stageCandidate();
    stale.repository.events.push({
      id: randomUUID(),
      householdId,
      signalId: randomUUID(),
      version: Math.max(...stale.repository.events.map((event) => event.version)) + 1,
      type: "conversation.bound",
      conversation: {
        conversationId: privateConversationId,
        audience: "private",
        authorityVersion: 2,
        participantSetDigest: privateDigest,
        providerConversationId: "linq-private-jackson",
        authorizedAdultIds: [jacksonId],
      },
    });
    const staleMessage = message("Yes, add that exact event", "private", jacksonId, 2);
    const staleAttempt = await process(stale, staleMessage, approval(stale, staleMessage));
    await expect(staleAttempt.processed).rejects.toThrow("exactly one current private draft");

    const effectsBeforeReplay = fixture.repository.commits.flatMap(({ effects }) => effects).length;
    const replayMessage = message("Yes, add that exact event too.");
    const replay = await process(fixture, replayMessage, approval(fixture, replayMessage));
    await expect(replay.processed).rejects.toThrow("exactly one current private draft");
    expect(fixture.repository.commits.flatMap(({ effects }) => effects)).toHaveLength(effectsBeforeReplay);
  });
});
