import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ApplicationStore } from "../../src/db/application-store.js";
import { closeDatabase, createDatabase, type Database } from "../../src/db/client.js";
import { migrateDatabase } from "../../src/db/migrate.js";
import { AdultIdSchema } from "../../src/domain/index.js";
import { FlorenceRuntimeStore } from "../../src/infrastructure/runtime-store.js";

const GOOGLE_CALENDAR_READONLY_SCOPE = "https://www.googleapis.com/auth/calendar.readonly";

const databaseUrl = process.env.TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)("FlorenceRuntimeStore PostgreSQL integration", () => {
  const schema = `runtime_store_${randomUUID().replaceAll("-", "").slice(0, 20)}`;
  let database: Database;
  let applicationStore: ApplicationStore;
  let store: FlorenceRuntimeStore;

  beforeAll(async () => {
    database = createDatabase(databaseUrl as string, { max: 12, schema });
    await migrateDatabase(database, schema);
    applicationStore = new ApplicationStore(database);
    store = new FlorenceRuntimeStore(database, applicationStore, "identity-test-key-with-more-than-32-bytes");
  });

  afterAll(async () => {
    if (!database) return;
    await database.unsafe(`drop schema if exists "${schema}" cascade`);
    await closeDatabase(database);
  });

  async function provisionConsentedFounder(input: {
    externalChatId: string;
    externalHandle: string;
    timeZone: string;
    occurredAt: string;
  }) {
    const founded = await store.provisionFoundingAdult(input);
    if (!founded.adultId) throw new Error("Expected a founding adult");
    await expect(
      store.finalizeFoundingAdult({
        householdId: founded.householdId,
        adultId: founded.adultId,
        externalChatId: input.externalChatId,
        externalHandle: input.externalHandle,
        consentedAt: input.occurredAt,
      }),
    ).resolves.toBe(true);
    return founded;
  }

  it("provisions inbound-first adults and binds only an exact two-adult group", async () => {
    const now = "2026-08-05T16:00:00Z";
    const ownerHandle = "+12025550101";
    const partnerHandle = "+12025550102";
    const founder = await store.provisionFoundingAdult({
      externalChatId: "dm-owner",
      externalHandle: ownerHandle,
      timeZone: "America/Los_Angeles",
      occurredAt: now,
    });
    expect(founder).toMatchObject({
      channelType: "private",
      bindingStatus: "pending",
      membershipStatus: "invited",
    });
    await expect(database<{ status: string; consented_at: Date | null }[]>`
      select status, consented_at from household_memberships
      where household_id = ${founder.householdId} and adult_id = ${founder.adultId}
    `).resolves.toEqual([{ status: "invited", consented_at: null }]);
    const snapshot = await applicationStore.load(founder.householdId);
    expect(snapshot?.projection.onboarding).toMatchObject({
      phase: "awaiting_initiator_consent",
      initiatorAdultId: founder.adultId,
    });
    await expect(
      store.finalizeFoundingAdult({
        householdId: founder.householdId,
        adultId: founder.adultId as string,
        externalChatId: "dm-owner",
        externalHandle: ownerHandle,
        consentedAt: now,
      }),
    ).resolves.toBe(true);
    await expect(
      store.finalizeFoundingAdult({
        householdId: founder.householdId,
        adultId: founder.adultId as string,
        externalChatId: "dm-owner",
        externalHandle: ownerHandle,
        consentedAt: now,
      }),
    ).resolves.toBe(true);
    await expect(
      applicationStore.resolveChannel({
        provider: "linq",
        externalChatId: "dm-owner",
        externalHandle: ownerHandle,
      }),
    ).resolves.toMatchObject({ bindingStatus: "active", membershipStatus: "active" });

    const invitation = await store.prepareInvitation({
      householdId: founder.householdId,
      invitedByAdultId: founder.adultId as string,
      inviteeHandle: "+1 (202) 555-0102",
      expiresAt: "2026-08-12T16:00:00Z",
    });
    const duplicate = await store.prepareInvitation({
      householdId: founder.householdId,
      invitedByAdultId: founder.adultId as string,
      inviteeHandle: partnerHandle,
      expiresAt: "2026-08-12T16:00:00Z",
    });
    expect(duplicate).toEqual(invitation);
    await expect(store.findPendingInvitation(partnerHandle)).resolves.toMatchObject({
      invitationId: invitation.invitationId,
      adultId: invitation.adultId,
    });

    const pending = await store.findPendingInvitation(partnerHandle);
    if (!pending) throw new Error("Expected a pending invitation");
    const invitee = await store.bindPendingInvitee({
      invitation: pending,
      externalChatId: "dm-partner",
      externalHandle: partnerHandle,
      occurredAt: "2026-08-05T16:05:00Z",
    });
    expect(invitee).toMatchObject({ bindingStatus: "pending", membershipStatus: "invited" });
    await expect(
      store.resolveTarget({
        householdId: founder.householdId,
        targetScope: { kind: "personal", adultId: AdultIdSchema.parse(invitation.adultId) },
      }),
    ).resolves.toMatchObject({ chatId: "dm-partner", status: "active" });
    await expect(
      store.finalizeInvitation({
        householdId: founder.householdId,
        adultId: invitation.adultId,
        externalChatId: "dm-partner",
        externalHandle: partnerHandle,
        consentedAt: "2026-08-05T16:06:00Z",
      }),
    ).resolves.toBe(true);

    const exact = await store.resolveExactGroup([partnerHandle, ownerHandle]);
    expect(exact?.householdId).toBe(founder.householdId);
    expect(exact?.adultsByHandle.get(ownerHandle)).toBe(founder.adultId);
    await expect(store.resolveExactGroup([partnerHandle, ownerHandle, "+12025550103"])).resolves.toBeNull();

    const group = await store.bindHouseholdGroup({
      householdId: founder.householdId,
      externalChatId: "group-family",
      participantHandles: [ownerHandle, partnerHandle],
      healthStatus: "HEALTHY",
    });
    expect(group).toMatchObject({ channelType: "group", bindingStatus: "active" });
    await expect(
      store.resolveTarget({ householdId: founder.householdId, targetScope: { kind: "household" } }),
    ).resolves.toMatchObject({ chatId: "group-family", status: "active" });
  });

  it("persists STOP before onboarding and requires an explicit release", async () => {
    const handle = "+12025550199";
    await store.setSuppression({
      externalChatId: "dm-opted-out",
      externalHandle: handle,
      scope: "private",
      suppressed: true,
      occurredAt: "2026-08-05T17:00:00Z",
      reason: "stop_command",
    });
    await expect(store.isSuppressed("dm-opted-out", handle)).resolves.toBe(true);
    await expect(
      store.provisionFoundingAdult({
        externalChatId: "dm-opted-out",
        externalHandle: handle,
        timeZone: "America/Los_Angeles",
        occurredAt: "2026-08-05T17:01:00Z",
      }),
    ).rejects.toMatchObject({ code: "not_authorized" });
    await store.setSuppression({
      externalChatId: "dm-opted-out",
      externalHandle: handle,
      scope: "private",
      suppressed: false,
      occurredAt: "2026-08-05T17:02:00Z",
      reason: "start_command",
    });
    await expect(store.isSuppressed("dm-opted-out", handle)).resolves.toBe(false);
  });

  it("purges expired resolved, dead, and quarantined provider payloads", async () => {
    const receipt = await applicationStore.ingestProviderEvent({
      provider: "linq",
      idempotencyKey: `fixture:${randomUUID()}`,
      authentication: { verified: true },
      eventKind: "message.received",
      occurredAt: "2026-08-05T18:00:00Z",
      payload: { private: "content" },
    });
    await database`
      update provider_inbox
      set status = 'dead', retention_until = '2026-08-05T18:01:00Z'
      where id = ${receipt.inboxId}
    `;
    await expect(store.purgeExpiredProviderInbox("2026-08-05T18:02:00Z")).resolves.toBe(1);
  });

  it("keeps Gmail credentials, cursors, and sources scoped to their owning adult", async () => {
    const founded = await provisionConsentedFounder({
      externalChatId: `dm-google-${randomUUID()}`,
      externalHandle: "+12025550211",
      timeZone: "America/Los_Angeles",
      occurredAt: "2026-08-05T19:00:00Z",
    });
    if (!founded.adultId) throw new Error("Expected a founding adult");
    const connection = await applicationStore.upsertExternalConnection({
      householdId: founded.householdId,
      adultId: founded.adultId,
      provider: "google",
      label: "Personal",
      externalAccountId: "google-subject-1",
      email: "Parent@Example.test",
      encryptedCredentials: "encrypted-v1",
      grantedScopes: ["https://www.googleapis.com/auth/gmail.readonly"],
      cursor: {},
      metadata: { credentialAadVersion: 1 },
    });
    const state = {
      schemaVersion: 1 as const,
      revision: 1,
      phase: "live" as const,
      requestedDepth: "full_history" as const,
      boundaryAt: "2026-08-05T19:00:00Z",
      scanPageToken: null,
      history: { cursorId: "100", startId: null, pageToken: null, targetId: null },
      watch: {
        historyId: "100",
        expiresAt: "2026-08-12T19:00:00Z",
        subscription: "projects/test/subscriptions/florence",
      },
      lastSuccessfulSyncAt: "2026-08-05T19:00:00Z",
      cancellation: null,
    };
    await expect(
      store.saveGmailSyncState({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: connection.connectionId,
        expectedRevision: 0,
        state,
      }),
    ).resolves.toBe("updated");
    await expect(
      store.saveGmailSyncState({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: connection.connectionId,
        expectedRevision: 0,
        state,
      }),
    ).resolves.toBe("conflict");
    await expect(
      store.findActiveGmailConnections({
        normalizedMailboxEmail: "parent@example.test",
        subscription: "projects/test/subscriptions/florence",
      }),
    ).resolves.toHaveLength(1);
    await expect(
      store.getOwnedGoogleConnection({
        householdId: founded.householdId,
        adultId: randomUUID(),
        connectionId: connection.connectionId,
      }),
    ).resolves.toBeNull();
    await expect(
      store.replaceEncryptedCredentials({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: connection.connectionId,
        expectedCiphertext: "encrypted-v1",
        encryptedCredentials: "encrypted-v2",
        grantedScopes: ["https://www.googleapis.com/auth/gmail.readonly"],
      }),
    ).resolves.toBe("updated");
    await expect(
      store.persistPersonalGmailSource({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: connection.connectionId,
        externalId: "gmail-message-1",
        kind: "gmail_message",
        occurredAt: "2026-08-05T19:01:00Z",
        contentHash: `sha256:${"a".repeat(64)}`,
        encryptedContent: "encrypted-message",
        metadata: { sourceScope: "personal" },
      }),
    ).resolves.toMatchObject({ disposition: "inserted", revision: 1 });
    await expect(
      store.revokeConnection({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: connection.connectionId,
        revokedAt: "2026-08-05T19:02:00Z",
      }),
    ).resolves.toBe("revoked");
    await expect(
      store.findActiveGmailConnections({
        normalizedMailboxEmail: "parent@example.test",
        subscription: "projects/test/subscriptions/florence",
      }),
    ).resolves.toHaveLength(0);
  });

  it("reconciles and leases idempotent Gmail continuation work", async () => {
    const founded = await provisionConsentedFounder({
      externalChatId: `dm-sync-${randomUUID()}`,
      externalHandle: "+12025550212",
      timeZone: "America/Los_Angeles",
      occurredAt: new Date().toISOString(),
    });
    if (!founded.adultId) throw new Error("Expected a founding adult");
    const connection = await applicationStore.upsertExternalConnection({
      householdId: founded.householdId,
      adultId: founded.adultId,
      provider: "google",
      label: "Family",
      externalAccountId: "google-subject-sync",
      email: "sync@example.test",
      encryptedCredentials: "encrypted",
      grantedScopes: ["https://www.googleapis.com/auth/gmail.readonly"],
      cursor: {},
      metadata: { credentialAadVersion: 1 },
    });
    const current = new Date().toISOString();
    await expect(
      store.saveGmailSyncState({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: connection.connectionId,
        expectedRevision: 0,
        state: {
          schemaVersion: 1,
          revision: 1,
          phase: "recent_90_days",
          requestedDepth: "full_history",
          boundaryAt: current,
          scanPageToken: null,
          history: { cursorId: "100", startId: null, pageToken: null, targetId: null },
          watch: {
            historyId: "100",
            expiresAt: new Date(Date.now() + 7 * 24 * 60 * 60_000).toISOString(),
            subscription: "projects/test/subscriptions/florence",
          },
          lastSuccessfulSyncAt: null,
          cancellation: null,
        },
      }),
    ).resolves.toBe("updated");
    await expect(store.reconcileGoogleSyncWork(current)).resolves.toBe(1);
    await expect(store.reconcileGoogleSyncWork(current)).resolves.toBe(0);
    const claims = await store.claimGoogleSyncWork({ owner: "google-worker", limit: 10, leaseSeconds: 60 });
    expect(claims).toHaveLength(1);
    expect(claims[0]?.work).toMatchObject({
      kind: "continue",
      householdId: founded.householdId,
      adultId: founded.adultId,
      connectionId: connection.connectionId,
    });
    const lease = claims[0];
    if (!lease) throw new Error("Expected Google sync lease");
    await expect(
      store.retryGoogleSyncWork({
        rowId: lease.rowId,
        leaseToken: randomUUID(),
        retryAt: new Date(Date.now() - 1_000).toISOString(),
        errorCode: "retryable",
      }),
    ).resolves.toBe(false);
    await expect(
      store.retryGoogleSyncWork({
        rowId: lease.rowId,
        leaseToken: lease.leaseToken,
        retryAt: new Date(Date.now() - 1_000).toISOString(),
        errorCode: "retryable",
      }),
    ).resolves.toBe(true);
    const retry = await store.claimGoogleSyncWork({ owner: "google-worker", limit: 1, leaseSeconds: 60 });
    expect(retry).toHaveLength(1);
    await expect(
      store.completeGoogleSyncWork({
        rowId: retry[0]?.rowId ?? "",
        leaseToken: retry[0]?.leaseToken ?? "",
      }),
    ).resolves.toBe(true);
  });

  it("isolates Calendar cursors, channels, encrypted sources, and privacy-safe busy windows", async () => {
    const founded = await provisionConsentedFounder({
      externalChatId: `dm-calendar-${randomUUID()}`,
      externalHandle: "+12025550213",
      timeZone: "America/Los_Angeles",
      occurredAt: "2027-02-01T08:00:00Z",
    });
    if (!founded.adultId) throw new Error("Expected a founding adult");
    const first = await applicationStore.upsertExternalConnection({
      householdId: founded.householdId,
      adultId: founded.adultId,
      provider: "google",
      label: "Parent one",
      externalAccountId: `google-calendar-${randomUUID()}`,
      email: `calendar-${randomUUID()}@example.test`,
      encryptedCredentials: "encrypted-calendar-one",
      grantedScopes: [GOOGLE_CALENDAR_READONLY_SCOPE],
      cursor: {},
      metadata: { credentialAadVersion: 1 },
    });
    const second = await applicationStore.upsertExternalConnection({
      householdId: founded.householdId,
      adultId: founded.adultId,
      provider: "google",
      label: "Parent two",
      externalAccountId: `google-calendar-${randomUUID()}`,
      email: `calendar-${randomUUID()}@example.test`,
      encryptedCredentials: "encrypted-calendar-two",
      grantedScopes: [GOOGLE_CALENDAR_READONLY_SCOPE],
      cursor: {},
      metadata: { credentialAadVersion: 1 },
    });
    const liveState = {
      schemaVersion: 1 as const,
      revision: 1,
      phase: "live" as const,
      calendarId: "primary",
      initialTimeMin: "2026-11-03T08:00:00Z",
      initialTimeMax: "2028-07-25T08:00:00Z",
      pageToken: null,
      syncToken: "sync-token-one",
      timeZone: "America/Los_Angeles",
      projectionReady: true,
      watch: null,
      lastSuccessfulSyncAt: "2027-02-01T08:00:00Z",
    };
    await expect(
      store.saveCalendarSyncState({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: first.connectionId,
        expectedRevision: 0,
        state: liveState,
      }),
    ).resolves.toBe("updated");
    await expect(
      store.saveCalendarSyncState({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: first.connectionId,
        expectedRevision: 0,
        state: liveState,
      }),
    ).resolves.toBe("conflict");
    await expect(
      store.saveCalendarSyncState({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: second.connectionId,
        expectedRevision: 0,
        state: { ...liveState, syncToken: "sync-token-two" },
      }),
    ).resolves.toBe("updated");

    const persistWindow = (connectionId: string, suffix: string, startsAt: string, endsAt: string) =>
      store.persistPersonalCalendarSource({
        householdId: founded.householdId,
        adultId: founded.adultId as string,
        connectionId,
        calendarId: "primary",
        externalId: `private-event-${suffix}`,
        kind: "calendar_event",
        occurredAt: startsAt,
        contentHash: `sha256:${suffix.repeat(64).slice(0, 64)}`,
        encryptedContent: `encrypted-private-calendar-payload-${suffix}`,
        metadata: { provider: "google-calendar", sourceScope: "personal" },
        busyWindow: { startsAt, endsAt, allDay: false },
      });
    await persistWindow(first.connectionId, "a", "2027-02-02T17:00:00Z", "2027-02-02T18:00:00Z");
    await persistWindow(second.connectionId, "b", "2027-02-03T17:00:00Z", "2027-02-03T18:00:00Z");
    const page = await store.listPersonalCalendarBusyWindows({
      householdId: founded.householdId,
      adultId: founded.adultId,
      asOf: "2027-02-01T08:05:00Z",
      from: "2027-02-01T00:00:00Z",
      to: "2027-02-05T00:00:00Z",
      limit: 10,
    });
    const windows = page.windows;
    expect(page).toMatchObject({ complete: true, synchronizedAt: "2027-02-01T08:00:00.000Z" });
    expect(windows).toHaveLength(2);
    expect(Object.keys(windows[0] ?? {}).sort()).toEqual(["allDay", "endsAt", "startsAt"]);
    expect(JSON.stringify(windows)).not.toMatch(/title|summary|description|location|attendee|credential/iu);
    await expect(
      store.listPersonalCalendarBusyWindows({
        householdId: founded.householdId,
        adultId: founded.adultId,
        asOf: "2027-02-01T08:05:00Z",
        from: "2027-02-01T00:00:00Z",
        to: "2027-02-05T00:00:00Z",
        limit: 1,
      }),
    ).resolves.toMatchObject({ complete: false, windows: [expect.any(Object)] });
    await expect(
      store.saveCalendarSyncState({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: second.connectionId,
        expectedRevision: 1,
        state: { ...liveState, revision: 2, syncToken: "sync-token-two", projectionReady: false },
      }),
    ).resolves.toBe("updated");
    await expect(
      store.listPersonalCalendarBusyWindows({
        householdId: founded.householdId,
        adultId: founded.adultId,
        asOf: "2027-02-01T08:05:00Z",
        from: "2027-02-01T00:00:00Z",
        to: "2027-02-05T00:00:00Z",
        limit: 10,
      }),
    ).resolves.toMatchObject({ complete: false, windows: [] });
    await expect(
      store.saveCalendarSyncState({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: second.connectionId,
        expectedRevision: 2,
        state: { ...liveState, revision: 3, syncToken: "sync-token-two", projectionReady: true },
      }),
    ).resolves.toBe("updated");

    const channelToken = "calendar-channel-token-that-is-secret";
    const watchedState = {
      ...liveState,
      revision: 2,
      watch: {
        channelId: "channel-one",
        resourceId: "resource-one",
        expiresAt: "2027-02-07T08:00:00Z",
      },
    };
    await expect(
      store.replaceCalendarWatch({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: first.connectionId,
        calendarId: "primary",
        expectedRevision: 1,
        state: watchedState,
        channel: {
          channelId: "channel-one",
          resourceId: "resource-one",
          resourceUri: "https://www.googleapis.com/calendar/v3/calendars/primary/events",
          channelToken,
          expiresAt: "2027-02-07T08:00:00Z",
        },
      }),
    ).resolves.toBe("updated");
    const pushIdentity = {
      channelId: "channel-one",
      resourceId: "resource-one",
      resourceUri: "https://www.googleapis.com/calendar/v3/calendars/primary/events",
      messageNumber: "1",
      receivedAt: "2027-02-02T08:00:00Z",
    };
    await expect(
      store.authenticateCalendarPush({ ...pushIdentity, channelToken: "wrong-channel-token" }),
    ).resolves.toBeNull();
    await expect(store.authenticateCalendarPush({ ...pushIdentity, channelToken })).resolves.toEqual({
      householdId: founded.householdId,
      adultId: founded.adultId,
      connectionId: first.connectionId,
      calendarId: "primary",
    });
    const replacementToken = "replacement-calendar-channel-token";
    await expect(
      store.replaceCalendarWatch({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: first.connectionId,
        calendarId: "primary",
        expectedRevision: 2,
        state: {
          ...watchedState,
          revision: 3,
          watch: {
            channelId: "channel-two",
            resourceId: "resource-two",
            expiresAt: "2027-02-08T08:00:00Z",
          },
        },
        channel: {
          channelId: "channel-two",
          resourceId: "resource-two",
          resourceUri: "https://www.googleapis.com/calendar/v3/calendars/primary/events",
          channelToken: replacementToken,
          expiresAt: "2027-02-08T08:00:00Z",
        },
      }),
    ).resolves.toBe("updated");
    await expect(
      store.authenticateCalendarPush({
        ...pushIdentity,
        channelToken,
        messageNumber: "2",
        receivedAt: "2027-02-02T08:01:00Z",
      }),
    ).resolves.toMatchObject({ connectionId: first.connectionId });
    await expect(
      store.markCalendarWatchStopped({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: first.connectionId,
        channelId: "channel-one",
      }),
    ).resolves.toBe("updated");
    await expect(
      store.authenticateCalendarPush({
        ...pushIdentity,
        channelToken,
        messageNumber: "3",
        receivedAt: "2027-02-02T08:02:00Z",
      }),
    ).resolves.toBeNull();

    const queued = await store.enqueueCalendarSyncWork({
      householdId: founded.householdId,
      idempotencyKey: `calendar-push:${randomUUID()}`,
      work: {
        kind: "push",
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: first.connectionId,
        calendarId: "primary",
      },
    });
    expect(queued.created).toBe(true);
    const leases = await store.claimCalendarSyncWork({
      owner: "calendar-worker",
      limit: 10,
      leaseSeconds: 60,
    });
    expect(leases.some((lease) => lease.rowId === queued.jobId)).toBe(true);
    const lease = leases.find((candidate) => candidate.rowId === queued.jobId);
    if (!lease) throw new Error("Expected Calendar sync lease");
    await expect(
      store.completeCalendarSyncWork({ rowId: lease.rowId, leaseToken: lease.leaseToken }),
    ).resolves.toBe(true);

    await expect(
      store.revokeConnection({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: first.connectionId,
        revokedAt: "2027-02-02T09:00:00Z",
      }),
    ).resolves.toBe("revoked");
    await expect(
      store.authenticateCalendarPush({
        ...pushIdentity,
        channelId: "channel-two",
        resourceId: "resource-two",
        channelToken: replacementToken,
        messageNumber: "2",
        receivedAt: "2027-02-02T09:01:00Z",
      }),
    ).resolves.toBeNull();
    await expect(
      store.listPersonalCalendarBusyWindows({
        householdId: founded.householdId,
        adultId: founded.adultId,
        asOf: "2027-02-01T08:05:00Z",
        from: "2027-02-01T00:00:00Z",
        to: "2027-02-05T00:00:00Z",
        limit: 10,
      }),
    ).resolves.toMatchObject({ windows: [{ startsAt: "2027-02-03T17:00:00.000Z" }] });
    await expect(
      store.getOwnedGoogleConnection({
        householdId: founded.householdId,
        adultId: founded.adultId,
        connectionId: second.connectionId,
      }),
    ).resolves.toMatchObject({ status: "active" });
  });
});
