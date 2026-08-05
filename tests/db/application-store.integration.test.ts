import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  type ApplicationCommit,
  createApplicationProjection,
  createOnboardingProjection,
} from "../../src/application/index.js";
import {
  ApplicationStore,
  OutboxIdempotencyConflictError,
  StaleProjectionVersionError,
} from "../../src/db/application-store.js";
import { closeDatabase, createDatabase, type Database } from "../../src/db/client.js";
import { migrateDatabase } from "../../src/db/migrate.js";
import { HouseholdAggregateSchema } from "../../src/domain/index.js";

const databaseUrl = process.env.TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)("ApplicationStore PostgreSQL integration", () => {
  const schema = `application_store_${randomUUID().replaceAll("-", "").slice(0, 20)}`;
  let database: Database;
  let store: ApplicationStore;

  beforeAll(async () => {
    database = createDatabase(databaseUrl as string, { max: 16, schema });
    await migrateDatabase(database, schema);
    store = new ApplicationStore(database);
  });

  afterAll(async () => {
    if (!database) return;
    await database.unsafe(`drop schema if exists "${schema}" cascade`);
    await closeDatabase(database);
  });

  async function household(): Promise<{ householdId: string; adultId: string }> {
    const created = await store.onboardFoundingAdult({
      householdName: "Application store family",
      adultDisplayName: "Founding adult",
      timeZone: "America/Los_Angeles",
      consentedAt: new Date().toISOString(),
      projectionSchemaVersion: 1,
      initialProjection: { episodes: {}, learnedPreferences: {} },
    });
    return { householdId: created.householdId, adultId: created.adultId };
  }

  it("deduplicates provider ingress before household resolution and quarantines hash conflicts", async () => {
    const idempotencyKey = `linq:${randomUUID()}`;
    const input = {
      provider: "linq",
      idempotencyKey,
      authentication: { verified: true },
      eventKind: "message.received",
      occurredAt: new Date().toISOString(),
      payload: { text: "Pickup moved to five" },
    };

    const receipts = await Promise.all(Array.from({ length: 12 }, () => store.ingestProviderEvent(input)));
    expect(receipts.filter((receipt) => receipt.disposition === "accepted")).toHaveLength(1);
    expect(new Set(receipts.map((receipt) => receipt.inboxId))).toHaveLength(1);

    const conflict = await store.ingestProviderEvent({
      ...input,
      payload: { text: "Pickup moved to six" },
    });
    expect(conflict).toMatchObject({
      inboxId: receipts[0]?.inboxId,
      disposition: "quarantined",
      status: "quarantined",
    });

    const queued = await store.ingestProviderEvent({
      ...input,
      idempotencyKey: `linq:${randomUUID()}`,
      payload: { text: "Ordinary event" },
      maxAttempts: 1,
    });
    const claimed = await store.claimProviderInbox({ owner: "ingress-a", limit: 10, leaseSeconds: 60 });
    expect(claimed.map((item) => item.id)).toEqual([queued.inboxId]);
    const item = claimed[0];
    if (!item) throw new Error("Expected a provider inbox lease");
    await expect(
      store.failProviderInbox({
        inboxId: item.id,
        leaseToken: randomUUID(),
        errorCode: "fixture_failure",
        retryAfterSeconds: 0,
      }),
    ).resolves.toBe("lost_lease");
    await expect(
      store.failProviderInbox({
        inboxId: item.id,
        leaseToken: item.leaseToken,
        errorCode: "fixture_failure",
        retryAfterSeconds: 0,
      }),
    ).resolves.toBe("dead");

    const state = await database<{ status: string; household_id: string | null; conflicts: string }[]>`
      select pi.status, pi.household_id,
        (select count(*)::text from provider_inbox_conflicts pic where pic.inbox_id = pi.id) as conflicts
      from provider_inbox pi where pi.id = ${conflict.inboxId}
    `;
    expect(state[0]).toEqual({ status: "quarantined", household_id: null, conflicts: "1" });

    await Promise.all(
      Array.from({ length: 8 }, (_, index) =>
        store.ingestProviderEvent({
          ...input,
          idempotencyKey: `linq:lease:${index}:${randomUUID()}`,
          payload: { index },
        }),
      ),
    );
    const [leftClaims, rightClaims] = await Promise.all([
      store.claimProviderInbox({ owner: "ingress-left", limit: 4, leaseSeconds: 60 }),
      store.claimProviderInbox({ owner: "ingress-right", limit: 4, leaseSeconds: 60 }),
    ]);
    expect(leftClaims).toHaveLength(4);
    expect(rightClaims).toHaveLength(4);
    expect(new Set([...leftClaims, ...rightClaims].map((claimedItem) => claimedItem.id))).toHaveLength(8);
  });

  it("onboards adults and resolves private and group channel identities", async () => {
    const externalChatId = `private-${randomUUID()}`;
    const externalHandle = `adult-${randomUUID()}`;
    const created = await store.onboardFoundingAdult({
      householdName: "Channel family",
      adultDisplayName: "Owner",
      timeZone: "America/New_York",
      consentedAt: new Date().toISOString(),
      projectionSchemaVersion: 1,
      initialProjection: { episodes: {} },
      privateChannel: { externalChatId, externalHandle, metadata: { route: "primary" } },
    });

    await expect(
      store.resolveChannel({ provider: "linq", externalChatId, externalHandle }),
    ).resolves.toMatchObject({
      householdId: created.householdId,
      adultId: created.adultId,
      channelType: "private",
      bindingStatus: "active",
      membershipStatus: "active",
    });

    const invited = await store.addAdultMembership({
      householdId: created.householdId,
      displayName: "Second adult",
      status: "invited",
    });
    await expect(
      store.upsertChannelBinding({
        householdId: created.householdId,
        adultId: invited.adultId,
        provider: "linq",
        channelType: "private",
        externalChatId: `invited-${randomUUID()}`,
        externalHandle: `invited-${randomUUID()}`,
        status: "active",
      }),
    ).rejects.toMatchObject({ code: "not_authorized" });
    await expect(
      store.activateAdultMembership({
        householdId: created.householdId,
        adultId: invited.adultId,
        consentedAt: new Date().toISOString(),
      }),
    ).resolves.toBe(true);

    const groupChatId = `group-${randomUUID()}`;
    await store.upsertChannelBinding({
      householdId: created.householdId,
      provider: "linq",
      channelType: "group",
      externalChatId: groupChatId,
      status: "active",
      metadata: { purpose: "family" },
    });
    await expect(
      store.resolveChannel({ provider: "linq", externalChatId: groupChatId }),
    ).resolves.toMatchObject({
      householdId: created.householdId,
      adultId: null,
      channelType: "group",
      membershipStatus: null,
    });

    const otherHousehold = await household();
    await expect(
      store.upsertChannelBinding({
        householdId: otherHousehold.householdId,
        provider: "linq",
        channelType: "group",
        externalChatId: groupChatId,
        status: "active",
      }),
    ).rejects.toMatchObject({ code: "not_authorized" });
  });

  it("implements the application repository with atomic dedupe and optimistic revision", async () => {
    const householdId = randomUUID();
    const adultId = randomUUID();
    const aggregate = HouseholdAggregateSchema.parse({
      schemaVersion: 1,
      householdId,
      version: 0,
      policyVersion: 0,
      lastProcessedSequence: 0,
      timeZone: "America/Los_Angeles",
      verifiedAdultIds: [adultId],
      routineAnchors: [],
      episodes: [],
      policies: [],
      policyCandidates: [],
      approvals: [],
      memoryCandidates: [],
      memories: [],
      pendingActions: [],
    });
    const projection = createApplicationProjection(createOnboardingProjection({ initiatorAdultId: adultId }));
    await store.onboardFoundingAdult({
      householdId,
      adultId,
      householdName: "Application repository family",
      adultDisplayName: "Owner",
      timeZone: "America/Los_Angeles",
      consentedAt: new Date().toISOString(),
      projectionSchemaVersion: 1,
      initialProjection: { legacyProjection: true },
    });
    await expect(
      store.initializeApplicationSnapshot({ snapshot: { revision: 0, aggregate, projection } }),
    ).resolves.toMatchObject({ created: true, snapshot: { revision: 0, aggregate, projection } });
    await expect(store.load(householdId)).resolves.toEqual({
      revision: 0,
      aggregate,
      projection,
    });

    const intentId = `app_outbox_${randomUUID()}`;
    const idempotencyKey = `app-input:${randomUUID()}`;
    const commit: ApplicationCommit = {
      householdId,
      idempotencyKey,
      expectedRevision: 0,
      aggregate,
      projection,
      signals: [],
      changes: [],
      outbox: [
        {
          intentId,
          householdId: aggregate.householdId,
          idempotencyKey: `app-effect:${randomUUID()}`,
          kind: "conversation.send",
          targetScope: { kind: "household" },
          messageClass: "status",
          body: "Application repository fixture",
        },
      ],
      audit: [
        {
          kind: "daily_brief_built",
          occurredAt: new Date().toISOString(),
          decision: "integration_fixture",
          containsPrivateData: false,
        },
      ],
      outcome: {
        status: "processed",
        classification: "integration_fixture",
        domainReceipts: [],
        outboxIntentIds: [intentId],
      },
    };
    await expect(store.commit(commit)).resolves.toMatchObject({
      disposition: "committed",
      revision: 1,
      outcome: commit.outcome,
    });
    await expect(store.commit(commit)).resolves.toMatchObject({
      disposition: "duplicate",
      revision: 1,
      outcome: commit.outcome,
    });
    await expect(
      store.commit({
        ...commit,
        outcome: { ...commit.outcome, classification: "conflicting_fixture" },
      }),
    ).rejects.toMatchObject({ code: "invalid_state" });
    await expect(store.findProcessed(householdId, idempotencyKey)).resolves.toMatchObject({
      householdId,
      idempotencyKey,
      disposition: "committed",
      revision: 1,
    });
    await expect(store.commit({ ...commit, idempotencyKey: `app-input:${randomUUID()}` })).resolves.toEqual({
      disposition: "conflict",
      actualRevision: 1,
    });
    await expect(store.load(householdId)).resolves.toMatchObject({ revision: 1, aggregate, projection });

    const persisted = await database<
      { commits: string; effects: string; audits: string; payload: Record<string, unknown> }[]
    >`
      select
        (select count(*)::text from application_commits where household_id = ${householdId}) as commits,
        (select count(*)::text from outbox where household_id = ${householdId}) as effects,
        (select count(*)::text from audit_log where household_id = ${householdId}) as audits,
        (select payload from outbox where household_id = ${householdId} limit 1) as payload
    `;
    expect(persisted[0]).toMatchObject({
      commits: "1",
      effects: "1",
      audits: "1",
      payload: { intentId, kind: "conversation.send" },
    });
    const applicationEffects = await store.claimOutbox({
      owner: "application-sender",
      limit: 10,
      leaseSeconds: 60,
    });
    expect(applicationEffects).toHaveLength(1);
    if (!applicationEffects[0]) throw new Error("Expected the application outbox lease");
    await expect(
      store.recordOutboxSuccess({
        rowId: applicationEffects[0].rowId,
        leaseToken: applicationEffects[0].leaseToken,
        providerReceipt: { fixture: true },
      }),
    ).resolves.toBe(true);
  });

  it("dead-letters permanent outbox failures only under the current lease fence", async () => {
    const { householdId } = await household();
    const intentKey = `permanent:${randomUUID()}`;
    await store.commitHouseholdProjection({
      householdId,
      expectedVersion: 0,
      schemaVersion: 1,
      nextState: { fixture: "permanent-outbox" },
      outbox: [
        {
          intentKey,
          effectKind: "fixture.permanent",
          idempotencyKey: `permanent:${randomUUID()}`,
          payload: { fixture: true },
        },
      ],
    });
    const firstClaims = await store.claimOutbox({
      owner: "permanent-first-owner",
      limit: 100,
      leaseSeconds: 60,
    });
    const first = firstClaims.find((item) => item.intentKey === intentKey);
    if (!first) throw new Error("Expected the permanent outbox fixture lease");

    await expect(
      store.recordOutboxPermanent({
        rowId: first.rowId,
        leaseToken: randomUUID(),
        errorCode: "invalid_outbox_payload",
      }),
    ).resolves.toBe(false);
    await expect(
      store.recordOutboxPermanent({
        rowId: first.rowId,
        leaseToken: first.leaseToken,
        errorCode: "Invalid Error Code",
      }),
    ).rejects.toThrow();
    await expect(
      store.recordOutboxPermanent({
        rowId: first.rowId,
        leaseToken: first.leaseToken,
        errorCode: "invalid_outbox_payload",
        safeDetail: "",
      }),
    ).rejects.toThrow();
    await expect(
      store.recordOutboxPermanent({
        rowId: first.rowId,
        leaseToken: first.leaseToken,
        errorCode: "invalid_outbox_payload",
        unexpected: true,
      } as never),
    ).rejects.toThrow();

    await database`
      update outbox set lease_expires_at = now() - interval '1 second'
      where id = ${first.rowId}
    `;
    const reclaimedClaims = await store.claimOutbox({
      owner: "permanent-second-owner",
      limit: 100,
      leaseSeconds: 60,
    });
    const reclaimed = reclaimedClaims.find((item) => item.intentKey === intentKey);
    if (!reclaimed) throw new Error("Expected the reclaimed permanent outbox fixture lease");
    expect(reclaimed.leaseToken).not.toBe(first.leaseToken);
    expect(reclaimed.attempt).toBe(2);

    await expect(
      store.recordOutboxPermanent({
        rowId: first.rowId,
        leaseToken: first.leaseToken,
        errorCode: "invalid_outbox_payload",
      }),
    ).resolves.toBe(false);
    await expect(
      store.recordOutboxPermanent({
        rowId: reclaimed.rowId,
        leaseToken: reclaimed.leaseToken,
        errorCode: "invalid_outbox_payload",
        safeDetail: "  Strict application intent parsing failed.  ",
      }),
    ).resolves.toBe(true);

    const dead = await database<
      {
        status: string;
        attempt: number;
        dead_at: Date | null;
        last_error_code: string | null;
        last_error_detail: string | null;
        lease_owner: string | null;
        lease_token: string | null;
        lease_expires_at: Date | null;
      }[]
    >`
      select status, attempt, dead_at, last_error_code, last_error_detail,
        lease_owner, lease_token, lease_expires_at
      from outbox where id = ${reclaimed.rowId}
    `;
    expect(dead[0]).toMatchObject({
      status: "dead",
      attempt: 2,
      last_error_code: "invalid_outbox_payload",
      last_error_detail: "Strict application intent parsing failed.",
      lease_owner: null,
      lease_token: null,
      lease_expires_at: null,
    });
    expect(dead[0]?.dead_at).toBeInstanceOf(Date);
    const deadAt = dead[0]?.dead_at?.toISOString();

    await expect(
      store.recordOutboxPermanent({
        rowId: reclaimed.rowId,
        leaseToken: reclaimed.leaseToken,
        errorCode: "different_error",
      }),
    ).resolves.toBe(false);
    const settled = await database<
      { status: string; dead_at: Date; last_error_code: string; last_error_detail: string }[]
    >`
      select status, dead_at, last_error_code, last_error_detail
      from outbox where id = ${reclaimed.rowId}
    `;
    expect(settled[0]).toMatchObject({
      status: "dead",
      last_error_code: "invalid_outbox_payload",
      last_error_detail: "Strict application intent parsing failed.",
    });
    expect(settled[0]?.dead_at.toISOString()).toBe(deadAt);
  });

  it("commits projections, timers, outbox effects, and audits atomically", async () => {
    const { householdId, adultId } = await household();
    const dueAt = new Date(Date.now() - 10_000).toISOString();
    const idempotencyKey = `message:${randomUUID()}`;

    await expect(
      store.commitHouseholdProjection({
        householdId,
        expectedVersion: 0,
        schemaVersion: 1,
        nextState: { episodes: { one: { status: "open" } } },
        timers: [
          {
            timerKey: "episode:one:recheck",
            episodeKey: "one",
            triggerKind: "recheck",
            planVersion: 1,
            dueAt,
            payload: { episodeKey: "one" },
          },
        ],
        outbox: [
          {
            intentKey: "episode:one:opened",
            effectKind: "linq.message",
            idempotencyKey,
            payload: { text: "I’ll keep track of that." },
          },
          {
            intentKey: "episode:one:owner-copy",
            effectKind: "linq.message",
            idempotencyKey: `message:${randomUUID()}`,
            payload: { text: "Owner copy" },
            maxAttempts: 1,
          },
          {
            intentKey: "episode:one:retry-copy",
            effectKind: "linq.message",
            idempotencyKey: `message:${randomUUID()}`,
            payload: { text: "Retry copy" },
            maxAttempts: 2,
          },
        ],
        audits: [
          {
            actorKind: "adult",
            actorId: adultId,
            action: "episode.opened",
            targetType: "episode",
            targetId: "one",
            details: { accepted: true },
          },
        ],
      }),
    ).resolves.toBe(1);
    await expect(store.getHouseholdProjection(householdId)).resolves.toMatchObject({
      version: 1,
      schemaVersion: 1,
      state: { episodes: { one: { status: "open" } } },
    });

    await expect(
      store.commitHouseholdProjection({
        householdId,
        expectedVersion: 0,
        schemaVersion: 1,
        nextState: {},
      }),
    ).rejects.toBeInstanceOf(StaleProjectionVersionError);

    await expect(
      store.commitHouseholdProjection({
        householdId,
        expectedVersion: 1,
        schemaVersion: 1,
        nextState: { shouldRollBack: true },
        outbox: [
          {
            intentKey: "episode:one:conflict",
            effectKind: "linq.message",
            idempotencyKey,
            payload: { text: "Different content" },
          },
        ],
      }),
    ).rejects.toBeInstanceOf(OutboxIdempotencyConflictError);
    await expect(store.getHouseholdProjection(householdId)).resolves.toMatchObject({ version: 1 });

    const timers = await store.claimDueTimers({ owner: "timer-a", limit: 10, leaseSeconds: 60 });
    expect(timers).toHaveLength(1);
    const timer = timers[0];
    if (!timer) throw new Error("Expected a timer lease");
    await expect(
      store.finishTimer({ rowId: timer.rowId, leaseToken: randomUUID(), outcome: "fired" }),
    ).resolves.toBe(false);
    await expect(
      store.releaseTimer({
        rowId: timer.rowId,
        leaseToken: timer.leaseToken,
        retryAt: new Date(Date.now() - 1_000).toISOString(),
        errorCode: "fixture_retry",
      }),
    ).resolves.toBe(true);
    const reclaimedTimers = await store.claimDueTimers({
      owner: "timer-b",
      limit: 10,
      leaseSeconds: 60,
    });
    expect(reclaimedTimers[0]?.attempt).toBe(2);
    if (!reclaimedTimers[0]) throw new Error("Expected a reclaimed timer");
    await expect(
      store.finishTimer({
        rowId: reclaimedTimers[0].rowId,
        leaseToken: reclaimedTimers[0].leaseToken,
        outcome: "fired",
      }),
    ).resolves.toBe(true);
    const cancellableTimer = {
      householdId,
      timerKey: "episode:one:cancelled",
      episodeKey: "one",
      triggerKind: "recheck",
      planVersion: 2,
      dueAt: new Date(Date.now() + 60_000).toISOString(),
      payload: { triggerId: "cancelled-fixture" },
    };
    const scheduled = await store.scheduleTimer(cancellableTimer);
    await expect(store.cancelTimer({ householdId, timerKey: "episode:one:cancelled" })).resolves.toBe(true);
    await expect(store.cancelTimer({ householdId, timerKey: "episode:one:cancelled" })).resolves.toBe(true);
    await expect(store.scheduleTimer(cancellableTimer)).resolves.toEqual(scheduled);
    await expect(
      store.scheduleTimer({ ...cancellableTimer, dueAt: new Date(Date.now() + 120_000).toISOString() }),
    ).rejects.toMatchObject({ code: "invalid_state" });
    const cancelledTimer = await database<{ status: string }[]>`
      select status from scheduled_triggers where id = ${scheduled.rowId}
    `;
    expect(cancelledTimer[0]?.status).toBe("cancelled");

    const effects = await store.claimOutbox({ owner: "sender-a", limit: 10, leaseSeconds: 60 });
    expect(effects).toHaveLength(3);
    const ambiguous = effects.find((effect) => effect.intentKey === "episode:one:opened");
    const terminal = effects.find((effect) => effect.intentKey === "episode:one:owner-copy");
    const retry = effects.find((effect) => effect.intentKey === "episode:one:retry-copy");
    if (!ambiguous || !terminal || !retry) throw new Error("Expected all outbox leases");
    await expect(
      store.recordOutboxFailure({
        rowId: ambiguous.rowId,
        leaseToken: ambiguous.leaseToken,
        errorCode: "network_timeout",
        retryAfterSeconds: 0,
        outcomeCertain: false,
      }),
    ).resolves.toBe("ambiguous");
    await expect(
      store.resolveAmbiguousOutbox({
        rowId: ambiguous.rowId,
        resolution: "sent",
        providerReceipt: { providerMessageId: "fixture-message" },
      }),
    ).resolves.toBe(true);
    await expect(
      store.recordOutboxFailure({
        rowId: terminal.rowId,
        leaseToken: terminal.leaseToken,
        errorCode: "provider_rejected",
        safeDetail: "fixture rejection",
        retryAfterSeconds: 0,
        outcomeCertain: true,
      }),
    ).resolves.toBe("dead");
    await expect(
      store.recordOutboxFailure({
        rowId: retry.rowId,
        leaseToken: retry.leaseToken,
        errorCode: "provider_busy",
        retryAfterSeconds: 0,
        outcomeCertain: true,
      }),
    ).resolves.toBe("retry");
    const retriedEffects = await store.claimOutbox({
      owner: "sender-b",
      limit: 10,
      leaseSeconds: 60,
    });
    expect(retriedEffects).toHaveLength(1);
    expect(retriedEffects[0]).toMatchObject({ intentKey: retry.intentKey, attempt: 2 });
    if (!retriedEffects[0]) throw new Error("Expected a retried outbox lease");
    await expect(
      store.recordOutboxSuccess({
        rowId: retriedEffects[0].rowId,
        leaseToken: retriedEffects[0].leaseToken,
        providerReceipt: { providerMessageId: "fixture-retried-message" },
      }),
    ).resolves.toBe(true);

    const state = await database<
      { version: string; timers: string; sent: string; dead: string; audits: string }[]
    >`
      select h.version::text as version,
        (select count(*)::text from scheduled_triggers where household_id = h.id and status = 'fired') as timers,
        (select count(*)::text from outbox where household_id = h.id and status = 'sent') as sent,
        (select count(*)::text from outbox where household_id = h.id and status = 'dead') as dead,
        (select count(*)::text from audit_log where household_id = h.id) as audits
      from households h where h.id = ${householdId}
    `;
    expect(state[0]).toEqual({ version: "1", timers: "1", sent: "2", dead: "1", audits: "1" });
  });

  it("consumes OAuth state once and keeps connection credentials opaque", async () => {
    const { householdId, adultId } = await household();
    const stateHash = "a".repeat(64);
    const encryptedPayload = "sealed:v1:pkce-and-account-label-fixture";
    const now = new Date();
    const state = await store.createOAuthState({
      householdId,
      adultId,
      provider: "google",
      stateHash,
      encryptedPayload,
      returnConversationId: "fixture-conversation",
      expiresAt: new Date(now.getTime() + 60_000).toISOString(),
    });
    await expect(store.consumeOAuthState({ provider: "google", stateHash })).resolves.toMatchObject({
      stateId: state.stateId,
      householdId,
      adultId,
      encryptedPayload,
    });
    await expect(store.consumeOAuthState({ provider: "google", stateHash })).resolves.toBeNull();
    const consumedState = await database<{ encrypted_payload: string | null; consumed_at: Date | null }[]>`
      select encrypted_payload, consumed_at from oauth_states where id = ${state.stateId}
    `;
    expect(consumedState[0]?.encrypted_payload).toBeNull();
    expect(consumedState[0]?.consumed_at).toBeInstanceOf(Date);

    await store.createOAuthState({
      householdId,
      adultId,
      provider: "google",
      stateHash: "b".repeat(64),
      encryptedPayload: "sealed:v1:expired-pkce-fixture",
      returnConversationId: "expired-fixture",
      expiresAt: new Date(now.getTime() - 60_000).toISOString(),
    });
    await expect(
      store.consumeOAuthState({
        provider: "google",
        stateHash: "b".repeat(64),
      }),
    ).resolves.toBeNull();

    const ciphertext = "sealed:v1:opaque-fixture";
    const connection = await store.upsertExternalConnection({
      householdId,
      adultId,
      provider: "google",
      label: "Family calendar",
      externalAccountId: `account-${randomUUID()}`,
      email: "fixture@example.invalid",
      encryptedCredentials: ciphertext,
      grantedScopes: ["calendar.readonly"],
      cursor: { sync: "fixture" },
      metadata: { calendar: "primary" },
    });
    await expect(
      store.getExternalConnection({ connectionId: connection.connectionId, householdId, adultId }),
    ).resolves.toMatchObject({ encryptedCredentials: ciphertext, status: "active" });
    await expect(
      store.revokeExternalConnection({ connectionId: connection.connectionId, householdId, adultId }),
    ).resolves.toBe(true);
    await expect(
      store.getExternalConnection({ connectionId: connection.connectionId, householdId, adultId }),
    ).resolves.toMatchObject({ encryptedCredentials: null, grantedScopes: [], status: "revoked" });
  });

  it("enforces source visibility, connection scope, revision, and retention", async () => {
    const { householdId, adultId } = await household();
    const secondAdult = await store.addAdultMembership({
      householdId,
      displayName: "Second adult",
      status: "active",
      consentedAt: new Date().toISOString(),
    });
    const connection = await store.upsertExternalConnection({
      householdId,
      adultId,
      provider: "google",
      label: "Personal calendar",
      externalAccountId: `account-${randomUUID()}`,
      encryptedCredentials: "sealed:v1:source-fixture",
      grantedScopes: ["calendar.readonly"],
    });
    const occurredAt = new Date().toISOString();
    const retentionUntil = new Date(Date.now() - 10_000).toISOString();
    const base = {
      householdId,
      connectionId: connection.connectionId,
      ownerAdultId: adultId,
      visibility: "personal" as const,
      provider: "google",
      externalId: `event-${randomUUID()}`,
      kind: "calendar.event",
      occurredAt,
      subject: "Private appointment",
      contentHash: "hash-v1",
      encryptedContent: "sealed:v1:event-fixture",
      metadata: { calendar: "primary" },
      retentionUntil,
    };
    const sourceReceipts = await Promise.all(Array.from({ length: 12 }, () => store.persistSourceItem(base)));
    expect(sourceReceipts.filter((receipt) => receipt.disposition === "inserted")).toHaveLength(1);
    expect(sourceReceipts.filter((receipt) => receipt.disposition === "unchanged")).toHaveLength(11);
    expect(new Set(sourceReceipts.map((receipt) => receipt.sourceItemId))).toHaveLength(1);
    const inserted = sourceReceipts[0];
    if (!inserted) throw new Error("Expected a source item receipt");
    await expect(
      store.persistSourceItem({ ...base, contentHash: "hash-v2", subject: "Updated appointment" }),
    ).resolves.toMatchObject({ disposition: "revised", revision: 2 });

    await expect(
      store.getSourceItem({
        sourceItemId: inserted.sourceItemId,
        householdId,
        viewerAdultId: secondAdult.adultId,
      }),
    ).resolves.toBeNull();
    await expect(
      store.getSourceItem({ sourceItemId: inserted.sourceItemId, householdId, viewerAdultId: adultId }),
    ).resolves.toMatchObject({ revision: 2, subject: "Updated appointment" });
    await expect(
      store.persistSourceItem({ ...base, ownerAdultId: secondAdult.adultId }),
    ).rejects.toMatchObject({ code: "not_authorized" });
    await expect(
      store.persistSourceItem({ ...base, visibility: "household", contentHash: "hash-v3" }),
    ).rejects.toMatchObject({ code: "invalid_state" });

    const householdSource = await store.persistSourceItem({
      ...base,
      externalId: `event-${randomUUID()}`,
      visibility: "household",
      retentionUntil: new Date(Date.now() + 60_000).toISOString(),
    });
    await expect(
      store.getSourceItem({
        sourceItemId: householdSource.sourceItemId,
        householdId,
        viewerAdultId: secondAdult.adultId,
      }),
    ).resolves.toMatchObject({ visibility: "household" });

    await expect(store.purgeExpiredSourceContent(new Date().toISOString())).resolves.toBe(1);
    await expect(
      store.getSourceItem({ sourceItemId: inserted.sourceItemId, householdId, viewerAdultId: adultId }),
    ).resolves.toMatchObject({ subject: null, encryptedContent: null, revision: 2 });
    await expect(
      store.revokeExternalConnection({ connectionId: connection.connectionId, householdId, adultId }),
    ).resolves.toBe(true);
    await expect(
      store.persistSourceItem({ ...base, externalId: `event-${randomUUID()}` }),
    ).rejects.toMatchObject({ code: "not_authorized" });
  });

  it("exports auditable household data and leaves a deletion tombstone", async () => {
    const { householdId, adultId } = await household();
    const secondAdult = await store.addAdultMembership({
      householdId,
      displayName: "Private export adult",
      status: "active",
      consentedAt: new Date().toISOString(),
    });
    await expect(
      store.appendAudit({
        householdId,
        audit: {
          actorKind: "adult",
          actorId: adultId,
          action: "export.requested",
          targetType: "household",
          targetId: householdId,
          details: { fixture: true },
        },
      }),
    ).resolves.toBe(1);
    await expect(
      store.appendAudit({
        householdId,
        audit: {
          actorKind: "adult",
          actorId: secondAdult.adultId,
          action: "private.fixture",
          targetType: "source",
          visibility: "personal",
          ownerAdultId: secondAdult.adultId,
          details: { privateFixture: true },
        },
      }),
    ).resolves.toBe(2);
    const exported = await store.exportHouseholdData({
      householdId,
      requestedByAdultId: adultId,
      exportedAt: new Date().toISOString(),
    });
    expect(exported).toMatchObject({
      schemaVersion: 1,
      household: { id: householdId },
      audits: [{ sequence: "1", action: "export.requested" }],
      projection: {
        state: null,
        state_redacted: true,
        redaction_reason: "unscoped_legacy_projection",
      },
    });

    const inbox = await store.ingestProviderEvent({
      provider: "linq",
      idempotencyKey: `deletion:${randomUUID()}`,
      authentication: { verified: true },
      eventKind: "message.received",
      occurredAt: new Date().toISOString(),
      payload: { text: "Delete this linked event too" },
    });
    const leased = await store.claimProviderInbox({ owner: "deletion-fixture", limit: 1, leaseSeconds: 60 });
    expect(leased[0]?.id).toBe(inbox.inboxId);
    if (!leased[0]) throw new Error("Expected provider inbox lease");
    await store.resolveProviderInbox({
      inboxId: leased[0].id,
      leaseToken: leased[0].leaseToken,
      householdId,
      resolution: { routed: true },
    });

    const confirmationDigest = "c".repeat(64);
    const deletion = await store.requestHouseholdDeletion({
      householdId,
      requestedByAdultId: adultId,
      confirmationDigest,
    });
    await expect(
      store.confirmHouseholdDeletion({
        requestId: deletion.requestId,
        confirmationDigest: "d".repeat(64),
        confirmedAt: new Date().toISOString(),
      }),
    ).resolves.toBe(false);
    await expect(
      store.confirmHouseholdDeletion({
        requestId: deletion.requestId,
        confirmationDigest,
        confirmedAt: new Date().toISOString(),
      }),
    ).resolves.toBe(true);
    await expect(
      store.executeHouseholdDeletion({
        requestId: deletion.requestId,
        completedAt: new Date().toISOString(),
      }),
    ).resolves.toEqual({ householdId, adultsDeleted: 2 });
    await expect(store.getHouseholdProjection(householdId)).resolves.toBeNull();
    await expect(store.getDeletionTombstone(deletion.requestId)).resolves.toMatchObject({
      householdId,
      requestedByAdultId: adultId,
      report: { householdDeleted: true, providerInboxDeleted: 1 },
    });
  });
});
