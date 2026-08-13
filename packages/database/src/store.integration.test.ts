import { randomUUID } from "node:crypto";
import type { HouseholdSignal } from "@florence/contracts";
import {
  HouseholdChiefOfStaff,
  type HouseholdCommit,
  type HouseholdDomainEvent,
} from "@florence/control-plane";
import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { migrateDatabase } from "./migrate.js";
import {
  DeliberationConflictError,
  HouseholdConcurrencyError,
  linqIdentitySubjectDigest,
  PostgresFlorenceRepository,
  SignalConflictError,
} from "./store.js";

const databaseUrl = process.env.FLORENCE_TEST_DATABASE_URL;
const databaseSchema = process.env.FLORENCE_TEST_POSTGRES_SCHEMA ?? "florence_test";
const describeWithDatabase = databaseUrl ? describe.sequential : describe.skip;
const households = new Set<string>();

function message(
  householdId: string,
  conversationId = randomUUID(),
  text = "The field trip form is due Friday.",
): Extract<HouseholdSignal, { type: "conversation.message" }> {
  const signalId = randomUUID();
  households.add(householdId);
  return {
    type: "conversation.message",
    signalId,
    householdId,
    occurredAt: new Date().toISOString(),
    idempotencyKey: `signal:${signalId}`,
    conversationId,
    audience: "group",
    authorityVersion: 1,
    participantSetDigest: "a".repeat(64),
    senderAdultId: randomUUID(),
    text,
    images: [],
    replyToSignalId: null,
    source: {
      system: "linq-v3",
      providerEventId: `event:${signalId}`,
      providerMessageId: `message:${signalId}`,
    },
  };
}

function lease(owner: string, now = new Date()) {
  return {
    leaseOwner: owner,
    now: now.toISOString(),
    leaseUntil: new Date(now.getTime() + 60_000).toISOString(),
  };
}

async function commitEvents(
  repository: PostgresFlorenceRepository,
  signal: HouseholdSignal,
  expectedVersion: number,
  events: readonly HouseholdDomainEvent[],
  effects: HouseholdCommit["effects"] = [],
): Promise<void> {
  households.add(signal.householdId);
  await repository.accept(signal, signal.occurredAt);
  const leaseOwner = `fixture:${signal.signalId}`;
  const claimed = await repository.claimNext(
    lease(leaseOwner, new Date(new Date(signal.occurredAt).getTime() + 1)),
  );
  expect(claimed?.signal.signalId).toBe(signal.signalId);
  await repository.commit({
    signalId: signal.signalId,
    householdId: signal.householdId,
    leaseOwner,
    expectedVersion,
    events,
    effects,
    timers: [],
    cancelEpisodeIds: [],
    firedTimerId: null,
    effectReceipt: null,
  });
}

describeWithDatabase("PostgresFlorenceRepository", () => {
  const connectionString = databaseUrl as string;
  let repository: PostgresFlorenceRepository;

  beforeAll(async () => {
    await migrateDatabase({ connectionString, schema: databaseSchema });
    repository = new PostgresFlorenceRepository({ connectionString, schema: databaseSchema });
  });

  afterAll(async () => {
    if (repository) await repository.close();
    const sql = postgres(connectionString, {
      max: 1,
      connection: { search_path: databaseSchema },
    });
    try {
      const ids = [...households];
      if (ids.length > 0) {
        await sql`delete from household_events where household_id in ${sql(ids)}`;
        await sql`delete from outbox_effects where household_id in ${sql(ids)}`;
        await sql`delete from episode_timers where household_id in ${sql(ids)}`;
        await sql`delete from household_signals where household_id in ${sql(ids)}`;
        await sql`delete from household_streams where household_id in ${sql(ids)}`;
      }
    } finally {
      await sql.end({ timeout: 5 });
    }
  });

  it("deduplicates exact signals, serializes each household, and commits durable truth", async () => {
    const householdId = randomUUID();
    const otherHouseholdId = randomUUID();
    const conversationId = randomUUID();
    const primary = message(householdId, conversationId);
    const acceptedAt = new Date();
    const accepted = await repository.accept(primary, acceptedAt.toISOString());

    expect(accepted.disposition).toBe("accepted");
    await expect(repository.accept(primary, acceptedAt.toISOString())).resolves.toEqual({
      ...accepted,
      disposition: "duplicate",
    });
    await expect(
      repository.accept({ ...primary, text: "Different payload" }, acceptedAt.toISOString()),
    ).rejects.toBeInstanceOf(SignalConflictError);

    const effectId = "00000000-0000-4000-8000-000000000001";
    const currentEffectId = "00000000-0000-4000-8000-000000000002";
    const staleEffectId = "00000000-0000-4000-8000-000000000003";
    const timerId = randomUUID();
    const episodeId = randomUUID();
    const receiptId = randomUUID();
    const receipt: Extract<HouseholdSignal, { type: "effect.receipt" }> = {
      type: "effect.receipt",
      signalId: receiptId,
      householdId,
      occurredAt: new Date(acceptedAt.getTime() + 1).toISOString(),
      idempotencyKey: `signal:${receiptId}`,
      effectId,
      episodeId,
      status: "committed",
      providerReceiptId: "provider-message-1",
      detail: null,
    };
    households.add(householdId);
    await repository.accept(receipt, new Date(acceptedAt.getTime() + 1).toISOString());
    const other = message(otherHouseholdId, randomUUID(), "An unrelated household message");
    await repository.accept(other, new Date(acceptedAt.getTime() + 2).toISOString());

    const claimed = await repository.claimNext(lease("worker-a", new Date(acceptedAt.getTime() + 3)));
    expect(claimed?.signal.signalId).toBe(primary.signalId);
    const cognition = {
      signalId: primary.signalId,
      leaseOwner: "worker-a",
      inputDigest: "b".repeat(64),
      proposals: [{ type: "ignore" as const, reason: "ordinary conversation" }],
    };
    await expect(repository.loadDeliberation(primary.signalId)).resolves.toBeNull();
    await repository.saveDeliberation(cognition);
    await repository.saveDeliberation(cognition);
    await expect(repository.loadDeliberation(primary.signalId)).resolves.toEqual({
      inputDigest: cognition.inputDigest,
      proposals: cognition.proposals,
    });
    await expect(
      repository.saveDeliberation({ ...cognition, inputDigest: "c".repeat(64) }),
    ).rejects.toBeInstanceOf(DeliberationConflictError);
    const independentlyClaimed = await repository.claimNext(
      lease("worker-b", new Date(acceptedAt.getTime() + 4)),
    );
    expect(independentlyClaimed?.signal.householdId).toBe(otherHouseholdId);
    await expect(
      repository.claimNext(lease("worker-c", new Date(acceptedAt.getTime() + 5))),
    ).resolves.toBeNull();
    await repository.fail({
      signalId: other.signalId,
      leaseOwner: "worker-b",
      retryAt: null,
      error: "Deliberately terminal in this test",
    });

    const adultId = randomUUID();
    await repository.commit({
      signalId: primary.signalId,
      householdId,
      leaseOwner: "worker-a",
      expectedVersion: 0,
      events: [
        {
          type: "household.created",
          householdName: "The Example family",
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
          type: "adult.verified",
          adultId,
          identitySubjectDigest: linqIdentitySubjectDigest("provider-handle-1"),
          consentVersion: "pilot-v1",
          consentedAt: acceptedAt.toISOString(),
        },
        {
          type: "conversation.bound",
          conversation: {
            conversationId,
            audience: "group",
            authorityVersion: 1,
            participantSetDigest: "a".repeat(64),
            providerConversationId: "provider-group-1",
            authorizedAdultIds: [adultId],
          },
        },
      ],
      effects: [
        {
          id: effectId,
          idempotencyKey: `${primary.signalId}:reply`,
          kind: "conversation.message",
          conversationId,
          conversationAuthorityVersion: 1,
          participantSetDigest: "a".repeat(64),
          episodeId,
          payload: { text: "I’ll keep the field trip form alive." },
        },
        {
          id: currentEffectId,
          idempotencyKey: `${primary.signalId}:current-authority`,
          kind: "conversation.message",
          conversationId,
          conversationAuthorityVersion: 1,
          participantSetDigest: "a".repeat(64),
          episodeId,
          payload: { text: "Current destination" },
        },
        {
          id: staleEffectId,
          idempotencyKey: `${primary.signalId}:stale-authority`,
          kind: "conversation.message",
          conversationId,
          conversationAuthorityVersion: 2,
          participantSetDigest: "a".repeat(64),
          episodeId,
          payload: { text: "Never send to a stale destination" },
        },
      ],
      timers: [
        {
          id: timerId,
          idempotencyKey: `${primary.signalId}:timer`,
          episodeId,
          episodeVersion: 1,
          scheduledFor: new Date(acceptedAt.getTime() + 86_400_000).toISOString(),
        },
      ],
      cancelEpisodeIds: [episodeId],
      firedTimerId: null,
      effectReceipt: null,
    });

    const effectClaim = await repository.claimNextEffect(
      lease("effect-worker", new Date(acceptedAt.getTime() + 10_000)),
    );
    expect(effectClaim).toMatchObject({
      id: currentEffectId,
      providerConversationId: "provider-group-1",
      audience: "group",
      expectedParticipantIdentityDigests: [linqIdentitySubjectDigest("provider-handle-1")],
      attempt: 1,
    });
    const currentReceiptId = randomUUID();
    await repository.accept(
      {
        type: "effect.receipt",
        signalId: currentReceiptId,
        householdId,
        occurredAt: new Date(acceptedAt.getTime() + 5).toISOString(),
        idempotencyKey: `signal:${currentReceiptId}`,
        effectId: currentEffectId,
        episodeId,
        status: "failed",
        providerReceiptId: null,
        detail: "Test exclusion after an accepted receipt",
      },
      new Date(acceptedAt.getTime() + 5).toISOString(),
    );
    await repository.releaseEffectClaim({
      effectId: currentEffectId,
      leaseOwner: "effect-worker",
      availableAt: acceptedAt.toISOString(),
      lastError: null,
    });
    await expect(
      repository.claimNextEffect(lease("stale-worker", new Date(acceptedAt.getTime() + 10_001))),
    ).resolves.toMatchObject({ id: staleEffectId, providerConversationId: null });

    const timerClaim = await repository.claimNextDueTimer(
      lease("timer-worker", new Date(acceptedAt.getTime() + 86_400_001)),
    );
    expect(timerClaim?.id).toBe(timerId);
    const timerSignalId = randomUUID();
    await repository.accept(
      {
        type: "timer.fired",
        signalId: timerSignalId,
        householdId,
        occurredAt: timerClaim?.scheduledFor as string,
        idempotencyKey: `signal:${timerSignalId}`,
        timerId,
        episodeId,
        episodeVersion: 1,
        scheduledFor: timerClaim?.scheduledFor as string,
      },
      new Date(acceptedAt.getTime() + 86_400_001).toISOString(),
    );
    await repository.releaseTimerClaim({ timerId, leaseOwner: "timer-worker" });
    await expect(
      repository.claimNextDueTimer(lease("other-timer-worker", new Date(acceptedAt.getTime() + 86_400_002))),
    ).resolves.toBeNull();

    const receiptClaim = await repository.claimNext(
      lease("receipt-worker", new Date(acceptedAt.getTime() + 6)),
    );
    expect(receiptClaim?.signal.signalId).toBe(receipt.signalId);
    await repository.commit({
      signalId: receipt.signalId,
      householdId,
      leaseOwner: "receipt-worker",
      expectedVersion: 3,
      events: [],
      effects: [],
      timers: [],
      cancelEpisodeIds: [],
      firedTimerId: null,
      effectReceipt: {
        effectId,
        episodeId,
        status: "committed",
        providerReceiptId: "provider-message-1",
        detail: null,
        occurredAt: receipt.occurredAt,
      },
    });

    expect(await repository.loadRecentConversationTurns(householdId, conversationId)).toEqual([
      {
        signalId: primary.signalId,
        speaker: primary.senderAdultId,
        text: primary.text,
        occurredAt: primary.occurredAt,
      },
      {
        signalId: effectId,
        speaker: "florence",
        text: "I’ll keep the field trip form alive.",
        occurredAt: primary.occurredAt,
      },
    ]);

    const verifier = postgres(connectionString, {
      max: 1,
      connection: { search_path: databaseSchema },
    });
    try {
      const [effect] =
        await verifier`select status, provider_receipt_id from outbox_effects where id = ${effectId}`;
      const [timer] =
        await verifier`select status, episode_version from episode_timers where id = ${timerId}`;
      expect(effect).toMatchObject({ status: "committed", provider_receipt_id: "provider-message-1" });
      expect(timer).toMatchObject({ status: "scheduled", episode_version: 1 });
    } finally {
      await verifier.end({ timeout: 5 });
    }

    await repository.close();
    repository = new PostgresFlorenceRepository({ connectionString, schema: databaseSchema });
    const events = await repository.loadEvents(householdId);
    expect(events).toHaveLength(3);
    expect(events[0]).toMatchObject({ type: "household.created", version: 1, signalId: primary.signalId });

    for (let index = 0; index < 2; index += 1) {
      const residual = await repository.claimNext(
        lease(`cleanup-worker-${index}`, new Date(acceptedAt.getTime() + 172_800_000)),
      );
      expect(residual).not.toBeNull();
      await repository.fail({
        signalId: residual?.signal.signalId as string,
        leaseOwner: `cleanup-worker-${index}`,
        retryAt: null,
        error: "Test cleanup",
      });
    }
  });

  it("shares encrypted image envelopes durably without overwriting an existing asset", async () => {
    const householdId = randomUUID();
    const signal = message(householdId);
    await repository.accept(signal, signal.occurredAt);
    const asset = {
      assetId: randomUUID(),
      householdId,
      signalId: signal.signalId,
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
      envelope: Uint8Array.from([1, 2, 3, 4]),
    };

    await expect(repository.insertImageAsset(asset)).resolves.toBe(true);
    await expect(repository.insertImageAsset({ ...asset, envelope: Uint8Array.of(9) })).resolves.toBe(false);
    await expect(repository.readImageAsset(asset.assetId)).resolves.toEqual(asset);

    await repository.deleteImageAsset(asset.assetId);
    await expect(repository.readImageAsset(asset.assetId)).resolves.toBeNull();

    const claimed = await repository.claimNext(
      lease(`asset-cleanup:${asset.assetId}`, new Date(new Date(signal.occurredAt).getTime() + 1)),
    );
    expect(claimed?.signal.signalId).toBe(signal.signalId);
    await repository.fail({
      signalId: signal.signalId,
      leaseOwner: `asset-cleanup:${asset.assetId}`,
      retryAt: null,
      error: "Artifact store fixture complete",
    });
  });

  it("holds exactly one observable worker lease and releases it cleanly", async () => {
    const first = await repository.acquireWorkerLease({ workerId: "worker:first" });
    expect(first).not.toBeNull();
    await expect(repository.acquireWorkerLease({ workerId: "worker:second" })).resolves.toBeNull();
    await first?.heartbeat();
    await first?.release();

    const second = await repository.acquireWorkerLease({ workerId: "worker:second" });
    expect(second).not.toBeNull();
    await second?.release();
  });

  it("rolls back a partial commit and supports retry then terminal failure", async () => {
    const householdId = randomUUID();
    const signal = message(householdId);
    const now = new Date();
    await repository.accept(signal, now.toISOString());
    await repository.claimNext(lease("rollback-worker", now));

    await expect(
      repository.commit({
        signalId: signal.signalId,
        householdId,
        leaseOwner: "rollback-worker",
        expectedVersion: 0,
        events: [
          {
            type: "household.created",
            householdName: "Rollback family",
            timeZone: "UTC",
            foundingAdult: {
              id: randomUUID(),
              kind: "adult",
              role: "steward",
              displayName: "Parent",
              relationship: "Parent",
              status: "verified",
            },
          },
        ],
        effects: [],
        timers: [
          {
            id: randomUUID(),
            idempotencyKey: `${signal.signalId}:invalid-timer`,
            episodeId: randomUUID(),
            episodeVersion: 0,
            scheduledFor: new Date(now.getTime() + 10_000).toISOString(),
          },
        ],
        cancelEpisodeIds: [],
        firedTimerId: null,
        effectReceipt: null,
      }),
    ).rejects.toThrow();
    await expect(repository.loadEvents(householdId)).resolves.toEqual([]);

    const retryAt = new Date(now.getTime() + 10);
    await repository.fail({
      signalId: signal.signalId,
      leaseOwner: "rollback-worker",
      retryAt: retryAt.toISOString(),
      error: "Retry the atomic unit",
    });
    const retried = await repository.claimNext(lease("final-worker", new Date(now.getTime() + 20)));
    expect(retried?.attempt).toBe(2);
    await repository.fail({
      signalId: signal.signalId,
      leaseOwner: "final-worker",
      retryAt: null,
      error: "Terminal test failure",
    });

    const verifier = postgres(connectionString, {
      max: 1,
      connection: { search_path: databaseSchema },
    });
    try {
      const [state] = await verifier`
        select status, attempt_count from household_signals where signal_id = ${signal.signalId}
      `;
      const [timers] = await verifier`
        select count(*)::integer as count from episode_timers where household_id = ${householdId}
      `;
      expect(state).toMatchObject({ status: "dead", attempt_count: 2 });
      expect(timers?.count).toBe(0);
    } finally {
      await verifier.end({ timeout: 5 });
    }
  });

  it("redeems one current enrollment into durable private authority without persisting its code", async () => {
    const householdId = randomUUID();
    const stewardId = randomUUID();
    const adultId = randomUUID();
    const issuedAt = new Date();
    const code = `FLORENCE-${"A".repeat(43)}`;
    const challengeDigest = "e".repeat(64);
    const chief = new HouseholdChiefOfStaff(
      repository,
      {
        async deliberate() {
          return [{ type: "ignore" as const, reason: "not used" }];
        },
      },
      () => issuedAt,
    );
    households.add(householdId);
    const created: Extract<HouseholdSignal, { type: "household.created" }> = {
      type: "household.created",
      signalId: randomUUID(),
      householdId,
      idempotencyKey: `create:${householdId}`,
      occurredAt: issuedAt.toISOString(),
      name: "Enrollment fixture",
      timeZone: "America/Los_Angeles",
      foundingAdult: { id: stewardId, displayName: "Jackson" },
      plannedAdult: {
        id: adultId,
        displayName: "Kendall",
        role: "steward",
        relationship: "Co-parent",
      },
    };
    await chief.accept(created);
    await chief.processNext(`enrollment-create:${householdId}`);
    const issue: Extract<HouseholdSignal, { type: "adult.enrollment.issued" }> = {
      type: "adult.enrollment.issued",
      signalId: randomUUID(),
      householdId,
      idempotencyKey: `issue:${householdId}`,
      occurredAt: issuedAt.toISOString(),
      actorAdultId: stewardId,
      adultId,
      challengeDigest,
      expiresAt: new Date(issuedAt.getTime() + 86_400_000).toISOString(),
    };
    await chief.accept(issue);
    await chief.processNext(`enrollment-issue:${householdId}`);

    const redemption = {
      signalId: randomUUID(),
      idempotencyKey: `redeem:${householdId}`,
      occurredAt: new Date(issuedAt.getTime() + 1_000).toISOString(),
      challengeDigest,
      identitySubjectDigest: linqIdentitySubjectDigest(`handle-${randomUUID()}`),
      consentVersion: "linq-private-code-v1",
      consentedAt: new Date(issuedAt.getTime() + 1_000).toISOString(),
      providerConversationId: `private-${randomUUID()}`,
    };
    const redeem = () => chief.accept({ command: "linq.enrollment.redeem", input: redemption });
    const accepted = await redeem();
    expect(accepted).toMatchObject({ householdId, adultId, disposition: "accepted" });
    await expect(redeem()).resolves.toMatchObject({
      disposition: "duplicate",
      signalId: redemption.signalId,
    });
    await expect(chief.processNext(`enrollment-redeem:${householdId}`)).resolves.toBe(true);

    const profile = await chief.profile(householdId);
    expect(profile?.identityBoundAdultIds).toEqual([adultId]);
    await expect(repository.loadEvents(householdId)).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: "conversation.bound",
          conversation: expect.objectContaining({ audience: "private", authorizedAdultIds: [adultId] }),
        }),
      ]),
    );
    await expect(repository.listHouseholdIdsForAdult(adultId)).resolves.toContain(householdId);
    await expect(
      chief.accept({
        command: "linq.enrollment.redeem",
        input: {
          ...redemption,
          signalId: randomUUID(),
          idempotencyKey: `second-redeem:${householdId}`,
        },
      }),
    ).resolves.toBeNull();

    const verifier = postgres(connectionString, {
      max: 1,
      connection: { search_path: databaseSchema },
    });
    try {
      const [raw] = await verifier<{ count: number }[]>`
        select count(*)::integer as count
        from household_signals
        where household_id = ${householdId} and payload::text like ${`%${code}%`}
      `;
      expect(raw?.count).toBe(0);
    } finally {
      await verifier.end({ timeout: 5 });
    }
  });

  it("atomically bootstraps the exact two-adult Linq group before preserving its first message", async () => {
    const householdId = randomUUID();
    const stewardId = randomUUID();
    const caregiverId = randomUUID();
    const stewardHandle = `handle-${randomUUID()}`;
    const caregiverHandle = `handle-${randomUUID()}`;
    const occurredAt = new Date();
    const setup = {
      type: "household.created" as const,
      signalId: randomUUID(),
      householdId,
      idempotencyKey: `group-setup:${householdId}`,
      occurredAt: occurredAt.toISOString(),
      name: "Group bootstrap fixture",
      timeZone: "America/Los_Angeles",
      foundingAdult: { id: stewardId, displayName: "Steward" },
    };
    await commitEvents(repository, setup, 0, [
      {
        type: "household.created",
        householdName: setup.name,
        timeZone: setup.timeZone,
        foundingAdult: {
          id: stewardId,
          kind: "adult",
          role: "steward",
          displayName: "Steward",
          relationship: "Parent",
          status: "verified",
        },
      },
      {
        type: "adult.verified",
        adultId: stewardId,
        identitySubjectDigest: linqIdentitySubjectDigest(stewardHandle),
        consentVersion: "linq-private-code-v1",
        consentedAt: occurredAt.toISOString(),
      },
      {
        type: "family.member.upserted",
        member: {
          id: caregiverId,
          kind: "adult",
          role: "caregiver",
          displayName: "Caregiver",
          relationship: "Parent",
          status: "verified",
        },
      },
      {
        type: "adult.verified",
        adultId: caregiverId,
        identitySubjectDigest: linqIdentitySubjectDigest(caregiverHandle),
        consentVersion: "linq-private-code-v1",
        consentedAt: occurredAt.toISOString(),
      },
    ]);
    households.add(householdId);
    const runtimeInputs: unknown[] = [];
    const acceptedAt = new Date(occurredAt.getTime() + 100);
    const chief = new HouseholdChiefOfStaff(
      repository,
      {
        async deliberate(input) {
          runtimeInputs.push(input);
          return [{ type: "ignore" as const, reason: "test" }];
        },
      },
      () => acceptedAt,
    );
    const providerConversationId = `group-${randomUUID()}`;
    const bootstrap = {
      bindingSignalId: randomUUID(),
      bindingIdempotencyKey: `group-binding:${randomUUID()}`,
      messageSignalId: randomUUID(),
      messageIdempotencyKey: `group-message:${randomUUID()}`,
      occurredAt: acceptedAt.toISOString(),
      providerConversationId,
      participantIdentityDigests: [stewardHandle, caregiverHandle].map(linqIdentitySubjectDigest).sort(),
      senderIdentitySubjectDigest: linqIdentitySubjectDigest(caregiverHandle),
      text: "The permission slip is due Friday.",
      providerEventId: `event-${randomUUID()}`,
      providerMessageId: `message-${randomUUID()}`,
    };

    const bindGroup = () => chief.accept({ command: "linq.group.bootstrap", input: bootstrap });
    await expect(bindGroup()).resolves.toMatchObject({
      householdId,
      disposition: "accepted",
    });
    await expect(bindGroup()).resolves.toMatchObject({
      disposition: "duplicate",
      signalId: bootstrap.messageSignalId,
    });
    await expect(
      chief.accept({
        command: "linq.group.bootstrap",
        input: {
          ...bootstrap,
          bindingSignalId: randomUUID(),
          bindingIdempotencyKey: `wrong-binding:${randomUUID()}`,
          messageSignalId: randomUUID(),
          messageIdempotencyKey: `wrong-message:${randomUUID()}`,
          providerConversationId: `wrong-group-${randomUUID()}`,
          participantIdentityDigests: [linqIdentitySubjectDigest(stewardHandle), "f".repeat(64)].sort(),
        },
      }),
    ).resolves.toBeNull();

    await expect(chief.processNext(`group-binding:${householdId}`)).resolves.toBe(true);
    await expect(chief.processNext(`group-message:${householdId}`)).resolves.toBe(true);
    expect(runtimeInputs).toHaveLength(1);
    expect(runtimeInputs[0]).toMatchObject({
      signal: {
        type: "conversation.message",
        senderAdultId: caregiverId,
        text: bootstrap.text,
      },
    });
    await expect(chief.profile(householdId)).resolves.toMatchObject({ onboardingComplete: true });
    await expect(repository.loadEvents(householdId)).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: "conversation.bound",
          conversation: expect.objectContaining({
            audience: "group",
            providerConversationId,
            authorizedAdultIds: [caregiverId, stewardId].sort(),
          }),
        }),
      ]),
    );

    const verifier = postgres(connectionString, {
      max: 1,
      connection: { search_path: databaseSchema },
    });
    try {
      const rows = await verifier<{ type: string; status: string }[]>`
        select type, status from household_signals
        where signal_id in (${bootstrap.bindingSignalId}, ${bootstrap.messageSignalId})
        order by accepted_at, signal_id
      `;
      expect(rows).toEqual([
        { type: "conversation.bound", status: "completed" },
        { type: "conversation.message", status: "completed" },
      ]);
    } finally {
      await verifier.end({ timeout: 5 });
    }
  });

  it("resolves Linq ingress from only current app-owned identity and conversation authority", async () => {
    const householdId = randomUUID();
    const conversationId = randomUUID();
    const foundingAdultId = randomUUID();
    const currentAdultId = randomUUID();
    const providerConversationId = `linq-chat-${randomUUID()}`;
    const oldHandleId = `linq-handle-${randomUUID()}`;
    const currentHandleId = `linq-handle-${randomUUID()}`;
    const occurredAt = new Date().toISOString();
    const setupSignalId = randomUUID();
    const setupSignal: Extract<HouseholdSignal, { type: "household.created" }> = {
      type: "household.created",
      signalId: setupSignalId,
      householdId,
      occurredAt,
      idempotencyKey: `signal:${setupSignalId}`,
      name: "Authority fixture",
      timeZone: "America/Los_Angeles",
      foundingAdult: { id: foundingAdultId, displayName: "First adult" },
    };
    const currentParticipantSetDigest = "2".repeat(64);

    await commitEvents(repository, setupSignal, 0, [
      {
        type: "household.created",
        householdName: setupSignal.name,
        timeZone: setupSignal.timeZone,
        foundingAdult: {
          ...setupSignal.foundingAdult,
          kind: "adult",
          role: "steward",
          relationship: "Parent",
          status: "verified",
        },
      },
      {
        type: "adult.verified",
        adultId: foundingAdultId,
        identitySubjectDigest: linqIdentitySubjectDigest(oldHandleId),
        consentVersion: "pilot-v1",
        consentedAt: occurredAt,
      },
      {
        type: "family.member.upserted",
        member: {
          id: currentAdultId,
          kind: "adult",
          role: "caregiver",
          displayName: "Current adult",
          relationship: "Parent",
          status: "verified",
        },
      },
      {
        type: "adult.verified",
        adultId: currentAdultId,
        identitySubjectDigest: linqIdentitySubjectDigest(currentHandleId),
        consentVersion: "pilot-v1",
        consentedAt: occurredAt,
      },
      {
        type: "conversation.bound",
        conversation: {
          conversationId,
          audience: "private",
          authorityVersion: 1,
          participantSetDigest: "1".repeat(64),
          providerConversationId,
          authorizedAdultIds: [foundingAdultId],
        },
      },
    ]);

    const advanceOccurredAt = new Date(new Date(occurredAt).getTime() + 5).toISOString();
    const advanceSignalId = randomUUID();
    const advanceSignal: Extract<HouseholdSignal, { type: "conversation.bound" }> = {
      type: "conversation.bound",
      signalId: advanceSignalId,
      householdId,
      occurredAt: advanceOccurredAt,
      idempotencyKey: `signal:${advanceSignalId}`,
      actorAdultId: foundingAdultId,
      conversationId,
      audience: "private",
      authorityVersion: 2,
      participantSetDigest: currentParticipantSetDigest,
      providerConversationId,
      authorizedAdultIds: [currentAdultId],
    };
    await commitEvents(repository, advanceSignal, 5, [
      {
        type: "conversation.bound",
        conversation: {
          conversationId,
          audience: "private",
          authorityVersion: 2,
          participantSetDigest: currentParticipantSetDigest,
          providerConversationId,
          authorizedAdultIds: [currentAdultId],
        },
      },
    ]);

    const priorReply = {
      ...message(householdId, conversationId, "The message being replied to"),
      occurredAt: new Date(new Date(occurredAt).getTime() + 10).toISOString(),
      audience: "private" as const,
      authorityVersion: 2,
      participantSetDigest: currentParticipantSetDigest,
      senderAdultId: currentAdultId,
      source: {
        system: "linq-v3" as const,
        providerEventId: `linq-event-${randomUUID()}`,
        providerMessageId: `linq-message-${randomUUID()}`,
      },
    };
    const promptEffectId = randomUUID();
    await commitEvents(
      repository,
      priorReply,
      6,
      [],
      [
        {
          id: promptEffectId,
          idempotencyKey: `${priorReply.signalId}:prompt`,
          kind: "conversation.message",
          conversationId,
          conversationAuthorityVersion: 2,
          participantSetDigest: currentParticipantSetDigest,
          episodeId: null,
          payload: { text: "Who wants to own this?" },
        },
      ],
    );

    const lookup = {
      providerConversationId,
      providerHandleId: currentHandleId,
      replyToProviderMessageId: priorReply.source.providerMessageId,
      occurredAt: priorReply.occurredAt,
    };
    await expect(repository.resolveLinqIngressAuthority(lookup)).resolves.toEqual({
      householdId,
      conversationId,
      audience: "private",
      authorityVersion: 2,
      participantSetDigest: currentParticipantSetDigest,
      expectedParticipantIdentityDigests: [linqIdentitySubjectDigest(currentHandleId)],
      senderAdultId: currentAdultId,
      replyToSignalId: priorReply.signalId,
    });
    await expect(
      repository.resolveLinqIngressAuthority({
        ...lookup,
        replyToProviderMessageId: "not-a-completed-message-in-this-conversation",
      }),
    ).resolves.toMatchObject({ replyToSignalId: null });
    await expect(
      repository.resolveLinqIngressAuthority({ ...lookup, providerHandleId: oldHandleId }),
    ).resolves.toBeNull();
    await expect(
      repository.resolveLinqIngressAuthority({
        ...lookup,
        providerHandleId: oldHandleId,
        occurredAt: new Date(new Date(occurredAt).getTime() + 1).toISOString(),
      }),
    ).resolves.toBeNull();
    await expect(
      repository.resolveLinqIngressAuthority({ ...lookup, providerHandleId: `other-${randomUUID()}` }),
    ).resolves.toBeNull();

    const promptProviderReceiptId = `linq-message-${randomUUID()}`;
    const promptReceiptOccurredAt = new Date(new Date(priorReply.occurredAt).getTime() + 1).toISOString();
    const promptReceiptId = randomUUID();
    await repository.accept(
      {
        type: "effect.receipt",
        signalId: promptReceiptId,
        householdId,
        occurredAt: promptReceiptOccurredAt,
        idempotencyKey: `signal:${promptReceiptId}`,
        effectId: promptEffectId,
        episodeId: null,
        status: "committed",
        providerReceiptId: promptProviderReceiptId,
        detail: null,
      },
      promptReceiptOccurredAt,
    );
    await expect(
      repository.resolveLinqIngressAuthority({
        ...lookup,
        replyToProviderMessageId: promptProviderReceiptId,
        occurredAt: new Date(new Date(promptReceiptOccurredAt).getTime() + 1).toISOString(),
      }),
    ).resolves.toMatchObject({ replyToSignalId: promptEffectId });
    const receiptClaim = await repository.claimNext(
      lease("authority-receipt-cleanup", new Date(new Date(promptReceiptOccurredAt).getTime() + 2)),
    );
    expect(receiptClaim?.signal.signalId).toBe(promptReceiptId);
    await repository.fail({
      signalId: promptReceiptId,
      leaseOwner: "authority-receipt-cleanup",
      retryAt: null,
      error: "Receipt mapping fixture cleanup",
    });

    const ambiguousHouseholdId = randomUUID();
    const ambiguousAdultId = randomUUID();
    const ambiguousSignalId = randomUUID();
    const ambiguousSetup: Extract<HouseholdSignal, { type: "household.created" }> = {
      type: "household.created",
      signalId: ambiguousSignalId,
      householdId: ambiguousHouseholdId,
      occurredAt: new Date(new Date(occurredAt).getTime() + 20).toISOString(),
      idempotencyKey: `signal:${ambiguousSignalId}`,
      name: "Ambiguous authority fixture",
      timeZone: "America/Los_Angeles",
      foundingAdult: { id: ambiguousAdultId, displayName: "Other adult" },
    };
    await commitEvents(repository, ambiguousSetup, 0, [
      {
        type: "household.created",
        householdName: ambiguousSetup.name,
        timeZone: ambiguousSetup.timeZone,
        foundingAdult: {
          ...ambiguousSetup.foundingAdult,
          kind: "adult",
          role: "steward",
          relationship: "Parent",
          status: "verified",
        },
      },
      {
        type: "adult.verified",
        adultId: ambiguousAdultId,
        identitySubjectDigest: linqIdentitySubjectDigest(currentHandleId),
        consentVersion: "pilot-v1",
        consentedAt: ambiguousSetup.occurredAt,
      },
      {
        type: "conversation.bound",
        conversation: {
          conversationId: randomUUID(),
          audience: "private",
          authorityVersion: 1,
          participantSetDigest: "3".repeat(64),
          providerConversationId,
          authorizedAdultIds: [ambiguousAdultId],
        },
      },
    ]);
    await expect(
      repository.resolveLinqIngressAuthority({ ...lookup, occurredAt: ambiguousSetup.occurredAt }),
    ).rejects.toThrow("Ambiguous Linq ingress authority");
  });

  it("round-trips an approved Calendar effect without inventing conversation authority", async () => {
    const householdId = randomUUID();
    const ownerAdultId = randomUUID();
    const setupSignalId = randomUUID();
    const occurredAt = new Date().toISOString();
    const setup: Extract<HouseholdSignal, { type: "household.created" }> = {
      type: "household.created",
      signalId: setupSignalId,
      householdId,
      occurredAt,
      idempotencyKey: `signal:${setupSignalId}`,
      name: "Calendar effect fixture",
      timeZone: "America/Los_Angeles",
      foundingAdult: { id: ownerAdultId, displayName: "Jackson" },
    };
    await commitEvents(repository, setup, 0, [
      {
        type: "household.created",
        householdName: setup.name,
        timeZone: setup.timeZone,
        foundingAdult: {
          ...setup.foundingAdult,
          kind: "adult",
          role: "steward",
          relationship: "Parent",
          status: "verified",
        },
      },
    ]);

    const approval = {
      ...message(householdId, randomUUID(), "Yes, put that on my calendar."),
      occurredAt: new Date(new Date(occurredAt).getTime() + 10).toISOString(),
      audience: "private" as const,
      senderAdultId: ownerAdultId,
    };
    const effectId = randomUUID();
    const connectionId = randomUUID();
    const actionId = randomUUID();
    const candidateId = randomUUID();
    await commitEvents(
      repository,
      approval,
      1,
      [],
      [
        {
          id: effectId,
          idempotencyKey: `google-calendar:${actionId}`,
          kind: "google.calendar.create",
          connectionId,
          ownerAdultId,
          actionId,
          approvalDigest: "a".repeat(64),
          candidateId,
          candidateVersion: 1,
          candidateDigest: "b".repeat(64),
          payload: {
            title: "School field trip",
            startsAt: "2026-08-21T16:00:00.000Z",
            endsAt: "2026-08-21T17:00:00.000Z",
            timeZone: "America/Los_Angeles",
            location: "School office",
          },
        },
      ],
    );
    const verifier = postgres(connectionString, {
      max: 1,
      connection: { search_path: databaseSchema },
    });
    try {
      await verifier`
        update outbox_effects
        set available_at = ${new Date("2020-01-01T00:00:00.000Z")}
        where id = ${effectId}
      `;
    } finally {
      await verifier.end({ timeout: 5 });
    }

    await expect(
      repository.claimNextEffect(
        lease("calendar-effect-worker", new Date(new Date(approval.occurredAt).getTime() + 1)),
      ),
    ).resolves.toMatchObject({
      id: effectId,
      householdId,
      kind: "google.calendar.create",
      connectionId,
      ownerAdultId,
      actionId,
      candidateId,
      candidateVersion: 1,
      episodeId: null,
      payload: {
        title: "School field trip",
        location: "School office",
      },
    });
  });

  it("persists one-use session-bound Google authority outside household events", async () => {
    const householdId = randomUUID();
    const ownerAdultId = randomUUID();
    const signalId = randomUUID();
    const setup: Extract<HouseholdSignal, { type: "household.created" }> = {
      type: "household.created",
      signalId,
      householdId,
      occurredAt: new Date().toISOString(),
      idempotencyKey: `signal:${signalId}`,
      name: "Google connection fixture",
      timeZone: "America/Los_Angeles",
      foundingAdult: { id: ownerAdultId, displayName: "Jackson" },
    };
    await commitEvents(repository, setup, 0, [
      {
        type: "household.created",
        householdName: setup.name,
        timeZone: setup.timeZone,
        foundingAdult: {
          ...setup.foundingAdult,
          kind: "adult",
          role: "steward",
          relationship: "Parent",
          status: "verified",
        },
      },
    ]);
    const connectionId = randomUUID();
    const stateDigest = "4".repeat(64);
    const sessionBindingDigest = "5".repeat(64);
    const created = await repository.createPending({
      connectionId,
      householdId,
      ownerAdultId,
      stateDigest,
      sessionBindingDigest,
      stateExpiresAt: new Date(Date.now() + 60_000).toISOString(),
      now: new Date().toISOString(),
    });
    expect(created).toMatchObject({ status: "pending", emailLabel: null });
    await expect(
      repository.consumePendingState({
        stateDigest,
        sessionBindingDigest: "6".repeat(64),
        now: new Date().toISOString(),
      }),
    ).resolves.toBeNull();
    await expect(
      repository.consumePendingState({ stateDigest, sessionBindingDigest, now: new Date().toISOString() }),
    ).resolves.toMatchObject({ connectionId, householdId, ownerAdultId });
    await expect(
      repository.consumePendingState({ stateDigest, sessionBindingDigest, now: new Date().toISOString() }),
    ).resolves.toBeNull();

    const active = await repository.activate({
      connectionId,
      stateDigest,
      googleSubjectDigest: "7".repeat(64),
      emailLabel: "jackson@example.com",
      grantedScopes: [
        "openid",
        "email",
        "https://www.googleapis.com/auth/gmail.readonly",
        "https://www.googleapis.com/auth/calendar.events.owned",
      ],
      refreshTokenEnvelope: "g1.encrypted.refresh-token",
      now: new Date().toISOString(),
    });
    expect(active).toMatchObject({ status: "active", emailLabel: "jackson@example.com" });
    await expect(repository.listActive({ householdId, ownerAdultId })).resolves.toEqual([active]);
    await expect(
      repository.readActiveGoogleCredential({ connectionId, householdId, ownerAdultId }),
    ).resolves.toMatchObject({ connectionId, gmailCursor: null });

    const claimNow = new Date(Date.now() + 1_000);
    const claim = await repository.claimNextGmailSync({
      owner: "gmail-worker-a",
      now: claimNow.toISOString(),
      leaseUntil: new Date(claimNow.getTime() + 60_000).toISOString(),
    });
    expect(claim).toMatchObject({ connectionId, leaseOwner: "gmail-worker-a", gmailCursor: null });
    await expect(
      repository.claimNextGmailSync({
        owner: "gmail-worker-b",
        now: new Date(claimNow.getTime() + 1).toISOString(),
        leaseUntil: new Date(claimNow.getTime() + 60_001).toISOString(),
      }),
    ).resolves.toBeNull();
    await expect(
      repository.releaseGmailSync({
        connectionId,
        owner: "gmail-worker-b",
        nextAt: new Date(claimNow.getTime() + 120_000).toISOString(),
        cursor: "123",
      }),
    ).rejects.toBeInstanceOf(HouseholdConcurrencyError);
    await repository.releaseGmailSync({
      connectionId,
      owner: "gmail-worker-a",
      nextAt: new Date(claimNow.getTime() + 120_000).toISOString(),
      cursor: "123",
    });
    await expect(
      repository.claimNextGmailSync({
        owner: "gmail-worker-b",
        now: new Date(claimNow.getTime() + 120_000).toISOString(),
        leaseUntil: new Date(claimNow.getTime() + 180_000).toISOString(),
      }),
    ).resolves.toMatchObject({ connectionId, leaseOwner: "gmail-worker-b", gmailCursor: "123" });

    const disconnected = await repository.disconnect({
      connectionId,
      householdId,
      ownerAdultId,
      now: new Date().toISOString(),
    });
    expect(disconnected).toMatchObject({
      view: { status: "disconnected" },
      refreshTokenEnvelope: "g1.encrypted.refresh-token",
    });
    await expect(repository.listActive({ householdId, ownerAdultId })).resolves.toEqual([]);
    await expect(
      repository.readActiveGoogleCredential({ connectionId, householdId, ownerAdultId }),
    ).resolves.toBeNull();
  });
});
