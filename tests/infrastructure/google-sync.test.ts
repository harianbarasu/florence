import { randomBytes } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import {
  type GmailHistoryPage,
  type GmailMessage,
  type GmailMessageIdPage,
  GOOGLE_GMAIL_READONLY_SCOPE,
  GoogleSyncTokenExpiredError,
  type GoogleTokenSet,
} from "../../src/adapters/google/index.js";
import type { ApplicationResult } from "../../src/application/index.js";
import { HouseholdIdSchema } from "../../src/domain/index.js";
import {
  type GmailProviderPort,
  type GmailSyncState,
  type GoogleConnectionDirectoryPort,
  type GoogleCredentialLifecyclePort,
  type GoogleSyncConnection,
  type GoogleSyncError,
  type GoogleSyncRepositoryPort,
  GoogleSyncService,
  gmailSourceContentAad,
  gmailSyncStateSchema,
  googleConnectionCredentialsAad,
  type PersistPersonalGmailSourceInput,
  type PersistPersonalGmailSourceResult,
} from "../../src/infrastructure/google-sync.js";
import { SecretBox } from "../../src/security/secret-box.js";

const NOW = new Date("2027-01-01T08:00:00.000Z");
const HOUSEHOLD_ID = HouseholdIdSchema.parse("11111111-1111-4111-8111-111111111111");
const ADULT_ID = "22222222-2222-4222-8222-222222222222";
const OTHER_ADULT_ID = "33333333-3333-4333-8333-333333333333";
const CONNECTION_ID = "44444444-4444-4444-8444-444444444444";
const SUBSCRIPTION = "projects/florence/subscriptions/gmail";
const TOPIC = "projects/florence/topics/gmail";

function syncState(overrides: Partial<GmailSyncState> = {}): GmailSyncState {
  return gmailSyncStateSchema.parse({
    schemaVersion: 1,
    revision: 0,
    phase: "live",
    requestedDepth: "full_history",
    boundaryAt: NOW.toISOString(),
    scanPageToken: null,
    history: { cursorId: "100", startId: null, pageToken: null, targetId: null },
    watch: {
      historyId: "100",
      expiresAt: "2027-01-08T08:00:00.000Z",
      subscription: SUBSCRIPTION,
    },
    lastSuccessfulSyncAt: null,
    cancellation: null,
    ...overrides,
  });
}

function tokenSet(overrides: Partial<GoogleTokenSet> = {}): GoogleTokenSet {
  return {
    accessToken: "access-token",
    refreshToken: "refresh-token",
    idToken: "identity-token",
    expiresAt: "2027-01-01T10:00:00.000Z",
    scope: [GOOGLE_GMAIL_READONLY_SCOPE],
    tokenType: "Bearer",
    ...overrides,
  };
}

function connection(secretBox: SecretBox, state = syncState()): GoogleSyncConnection {
  const base = {
    id: CONNECTION_ID,
    householdId: HOUSEHOLD_ID,
    adultId: ADULT_ID,
    provider: "google" as const,
    externalAccountId: "google-subject-parent",
    email: "parent@example.com",
    encryptedCredentials: null,
    grantedScopes: [GOOGLE_GMAIL_READONLY_SCOPE],
    status: "active" as const,
    cursor: { gmail: state },
    metadata: { credentialAadVersion: 1 },
  };
  return {
    ...base,
    encryptedCredentials: secretBox.seal(JSON.stringify(tokenSet()), googleConnectionCredentialsAad(base)),
  };
}

function message(version: number, id = "message-1"): GmailMessage {
  return {
    schemaVersion: 1,
    source: "gmail",
    sourceScope: "personal",
    googleSubject: "google-subject-parent",
    sourceKey: `google-subject-parent:${id}`,
    messageId: id,
    threadId: "thread-1",
    historyId: String(100 + version),
    internalDate: "2027-01-01T07:00:00.000Z",
    labelIds: ["INBOX"],
    snippet: `School update ${version}`,
    headers: {
      from: "School <school@example.org>",
      to: "parent@example.com",
      cc: null,
      bcc: null,
      replyTo: null,
      subject: `Schedule version ${version}`,
      date: null,
      messageId: `<${id}@example.org>`,
      inReplyTo: null,
      references: null,
    },
    rawHeaders: { subject: [`Schedule version ${version}`] },
    body: { text: `Pickup is at ${2 + version}:00.`, html: null },
    attachments: [],
  };
}

class MemoryGoogleStore implements GoogleConnectionDirectoryPort, GoogleSyncRepositoryPort {
  public readonly sources = new Map<string, PersistPersonalGmailSourceInput & { revision: number }>();
  public readonly savedStates: GmailSyncState[] = [];
  public connection: GoogleSyncConnection;

  public constructor(connectionRecord: GoogleSyncConnection) {
    this.connection = connectionRecord;
  }

  public async findActiveGmailConnections(input: { normalizedMailboxEmail: string; subscription: string }) {
    const state = this.connection.cursor.gmail as GmailSyncState | undefined;
    return this.connection.status === "active" &&
      this.connection.email?.toLowerCase() === input.normalizedMailboxEmail &&
      state?.watch?.subscription === input.subscription
      ? [structuredClone(this.connection)]
      : [];
  }

  public async getOwnedGoogleConnection(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
  }) {
    return this.connection.householdId === input.householdId &&
      this.connection.adultId === input.adultId &&
      this.connection.id === input.connectionId
      ? structuredClone(this.connection)
      : null;
  }

  public async replaceEncryptedCredentials(
    input: Parameters<GoogleSyncRepositoryPort["replaceEncryptedCredentials"]>[0],
  ) {
    if (this.connection.status !== "active") return "inactive" as const;
    if (this.connection.encryptedCredentials !== input.expectedCiphertext) return "conflict" as const;
    this.connection.encryptedCredentials = input.encryptedCredentials;
    this.connection.grantedScopes = [...input.grantedScopes];
    return "updated" as const;
  }

  public async saveGmailSyncState(input: Parameters<GoogleSyncRepositoryPort["saveGmailSyncState"]>[0]) {
    if (this.connection.status !== "active") return "inactive" as const;
    const current = this.connection.cursor.gmail as GmailSyncState | undefined;
    if ((current?.revision ?? 0) !== input.expectedRevision) return "conflict" as const;
    this.connection.cursor = { ...this.connection.cursor, gmail: structuredClone(input.state) };
    this.savedStates.push(structuredClone(input.state));
    return "updated" as const;
  }

  public async persistPersonalGmailSource(
    input: PersistPersonalGmailSourceInput,
  ): Promise<PersistPersonalGmailSourceResult> {
    if (
      input.householdId !== this.connection.householdId ||
      input.adultId !== this.connection.adultId ||
      input.connectionId !== this.connection.id ||
      this.connection.status !== "active"
    ) {
      throw new Error("unscoped source write");
    }
    const previous = this.sources.get(input.externalId);
    if (!previous) {
      this.sources.set(input.externalId, { ...input, revision: 1 });
      return { sourceItemId: `source-${input.externalId}`, disposition: "inserted", revision: 1 };
    }
    if (previous.contentHash === input.contentHash) {
      return {
        sourceItemId: `source-${input.externalId}`,
        disposition: "unchanged",
        revision: previous.revision,
      };
    }
    const revision = previous.revision + 1;
    this.sources.set(input.externalId, { ...input, revision });
    return { sourceItemId: `source-${input.externalId}`, disposition: "revised", revision };
  }

  public async markConnectionStatus(input: Parameters<GoogleSyncRepositoryPort["markConnectionStatus"]>[0]) {
    if (input.connectionId !== this.connection.id) return "not_found" as const;
    this.connection.status = input.status;
    return "updated" as const;
  }

  public async revokeConnection(input: Parameters<GoogleSyncRepositoryPort["revokeConnection"]>[0]) {
    if (
      input.connectionId !== this.connection.id ||
      input.householdId !== this.connection.householdId ||
      input.adultId !== this.connection.adultId
    ) {
      return "not_found" as const;
    }
    this.connection.status = "revoked";
    this.connection.encryptedCredentials = null;
    this.connection.grantedScopes = [];
    this.connection.cursor = {};
    return "revoked" as const;
  }
}

type FakeGmailOptions = {
  history?: (input: Parameters<GmailProviderPort["listHistoryPage"]>[0]) => Promise<GmailHistoryPage>;
  list?: (input: Parameters<GmailProviderPort["listMessageIdsPage"]>[0]) => Promise<GmailMessageIdPage>;
  get?: (input: Parameters<GmailProviderPort["getMessage"]>[0]) => Promise<GmailMessage>;
};

function createHarness(options: FakeGmailOptions = {}) {
  const secretBox = new SecretBox(randomBytes(32).toString("base64url"));
  const store = new MemoryGoogleStore(connection(secretBox));
  const queries: string[] = [];
  const applicationItems: unknown[] = [];
  const stopWatch = vi.fn(async () => undefined);
  const revoke = vi.fn(async () => undefined);
  const gmail: GmailProviderPort = {
    getMessage: options.get ?? (async ({ messageId }) => message(1, messageId)),
    listHistoryPage:
      options.history ??
      (async ({ startHistoryId }) => ({
        changes: [],
        mailboxHistoryId: startHistoryId,
        nextPageToken: null,
      })),
    async listMessageIdsPage(input) {
      if (input.query) queries.push(input.query);
      return options.list?.(input) ?? { messages: [], nextPageToken: null, resultSizeEstimate: 0 };
    },
    async startWatch() {
      return { historyId: "100", expiresAt: "2027-01-08T08:00:00.000Z" };
    },
    stopWatch,
  };
  const oauth: GoogleCredentialLifecyclePort = {
    async refresh(tokens) {
      return { ...tokens, accessToken: "refreshed-access", expiresAt: "2027-01-02T08:00:00.000Z" };
    },
    revoke,
  };
  const service = new GoogleSyncService({
    directory: store,
    repository: store,
    gmail,
    oauth,
    application: {
      async process(input): Promise<ApplicationResult> {
        const item = input as { householdId: typeof HOUSEHOLD_ID; idempotencyKey: string };
        applicationItems.push(structuredClone(input));
        return {
          householdId: item.householdId,
          idempotencyKey: item.idempotencyKey,
          disposition: "committed",
          revision: applicationItems.length,
          outcome: {
            status: "processed",
            classification: "gmail:retain_private",
            domainReceipts: [],
            outboxIntentIds: [],
          },
        };
      },
    },
    secretBox,
    gmailTopicName: TOPIC,
    gmailPubSubSubscription: SUBSCRIPTION,
    now: () => NOW,
    pageSize: 25,
  });
  return { service, store, secretBox, queries, applicationItems, stopWatch, revoke };
}

function historyNotice(historyId: string) {
  return {
    kind: "history_notice" as const,
    event: {
      schemaVersion: 1 as const,
      source: "gmail" as const,
      sourceScope: "personal" as const,
      providerEventId: `push-${historyId}`,
      subscription: SUBSCRIPTION,
      mailboxEmail: "PARENT@example.com",
      historyId,
      publishedAt: NOW.toISOString(),
      deliveryAttempt: 1,
    },
  };
}

describe("GoogleSyncService", () => {
  it("fails closed when an explicit request tries to cross the connection's adult boundary", async () => {
    const harness = createHarness();
    const unsafeDirectory: GoogleConnectionDirectoryPort = {
      findActiveGmailConnections: (input) => harness.store.findActiveGmailConnections(input),
      async getOwnedGoogleConnection() {
        return structuredClone(harness.store.connection);
      },
    };
    const service = new GoogleSyncService({
      directory: unsafeDirectory,
      repository: harness.store,
      gmail: {
        getMessage: async () => {
          throw new Error("must not fetch");
        },
        listHistoryPage: async () => {
          throw new Error("must not list");
        },
        listMessageIdsPage: async () => {
          throw new Error("must not list");
        },
        startWatch: async () => {
          throw new Error("must not watch");
        },
        stopWatch: async () => undefined,
      },
      oauth: {
        refresh: async (tokens) => tokens,
        revoke: async () => undefined,
      },
      application: { process: async () => Promise.reject(new Error("must not process")) },
      secretBox: harness.secretBox,
      gmailTopicName: TOPIC,
      gmailPubSubSubscription: SUBSCRIPTION,
      now: () => NOW,
    });

    await expect(
      service.execute({
        kind: "continue",
        householdId: HOUSEHOLD_ID,
        adultId: OTHER_ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).rejects.toMatchObject({ code: "not_authorized" } satisfies Partial<GoogleSyncError>);
    expect(harness.store.sources.size).toBe(0);
  });

  it("persists encrypted personal revisions before application processing and checkpoints history", async () => {
    let page = 0;
    let fetchedVersion = 0;
    const harness = createHarness({
      async history() {
        page += 1;
        return page === 1
          ? {
              changes: [
                {
                  schemaVersion: 1,
                  source: "gmail",
                  sourceScope: "personal",
                  googleSubject: "google-subject-parent",
                  providerEventId: "history-event-101",
                  historyId: "101",
                  changeType: "message.added",
                  messageId: "message-1",
                  threadId: "thread-1",
                  labelIds: [],
                },
              ],
              mailboxHistoryId: "102",
              nextPageToken: "page-2",
            }
          : {
              changes: [
                {
                  schemaVersion: 1,
                  source: "gmail",
                  sourceScope: "personal",
                  googleSubject: "google-subject-parent",
                  providerEventId: "history-event-102",
                  historyId: "102",
                  changeType: "labels.added",
                  messageId: "message-1",
                  threadId: "thread-1",
                  labelIds: ["STARRED"],
                },
              ],
              mailboxHistoryId: "102",
              nextPageToken: null,
            };
      },
      async get() {
        fetchedVersion += 1;
        return message(fetchedVersion);
      },
    });

    await expect(harness.service.processPush(historyNotice("102").event)).resolves.toMatchObject({
      status: "continuation_required",
      householdId: HOUSEHOLD_ID,
      adultId: ADULT_ID,
      processedMessages: 1,
    });
    await expect(
      harness.service.execute({
        kind: "continue",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).resolves.toMatchObject({ status: "processed", processedMessages: 1 });

    expect(harness.applicationItems).toMatchObject([
      { ownerAdultId: ADULT_ID, revision: 1, idempotencyKey: expect.stringContaining("revision:1") },
      { ownerAdultId: ADULT_ID, revision: 2, idempotencyKey: expect.stringContaining("revision:2") },
    ]);
    const stored = harness.store.sources.get("message-1");
    expect(stored).toMatchObject({ adultId: ADULT_ID, kind: "gmail_message", revision: 2 });
    expect(stored?.encryptedContent).not.toContain("Pickup");
    expect(
      harness.secretBox.open(
        stored?.encryptedContent ?? "",
        gmailSourceContentAad(harness.store.connection, "message-1"),
      ),
    ).toContain("Pickup is at 4:00");
    expect((harness.store.connection.cursor.gmail as GmailSyncState).history).toEqual({
      cursorId: "102",
      startId: null,
      pageToken: null,
      targetId: null,
    });
    await expect(harness.service.execute(historyNotice("102"))).resolves.toMatchObject({ status: "noop" });
    expect(harness.applicationItems).toHaveLength(2);
  });

  it("falls back to a recent scan when the Gmail history cursor has expired", async () => {
    const harness = createHarness({
      async history() {
        throw new GoogleSyncTokenExpiredError("gmail");
      },
    });

    await expect(harness.service.execute(historyNotice("150"))).resolves.toMatchObject({
      status: "processed",
      phase: "live",
    });
    const ninetyDaysAgo = Math.floor((NOW.getTime() - 90 * 24 * 60 * 60 * 1_000) / 1_000);
    expect(harness.queries).toEqual([`after:${ninetyDaysAgo}`]);
    expect((harness.store.connection.cursor.gmail as GmailSyncState).history.cursorId).toBe("150");
  });

  it("imports the recent 90 days before the one-year and progressive full-history phases", async () => {
    const harness = createHarness();
    await expect(
      harness.service.execute({
        kind: "start",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
        depth: "full_history",
      }),
    ).resolves.toMatchObject({ status: "continuation_required", phase: "recent_90_days" });

    for (const expectedPhase of ["one_year_backfill", "full_history_backfill", "live"] as const) {
      await expect(
        harness.service.execute({
          kind: "continue",
          householdId: HOUSEHOLD_ID,
          adultId: ADULT_ID,
          connectionId: CONNECTION_ID,
        }),
      ).resolves.toMatchObject({ phase: expectedPhase });
    }

    const ninetyDaysAgo = Math.floor((NOW.getTime() - 90 * 24 * 60 * 60 * 1_000) / 1_000);
    const oneYearAgo = Math.floor((NOW.getTime() - 365 * 24 * 60 * 60 * 1_000) / 1_000);
    expect(harness.queries).toEqual([
      `after:${ninetyDaysAgo}`,
      `after:${oneYearAgo} before:${ninetyDaysAgo}`,
      `before:${oneYearAgo}`,
    ]);
  });

  it("durably cancels without touching Google and revokes both provider and local credentials", async () => {
    const cancelled = createHarness();
    await expect(
      cancelled.service.execute({
        kind: "cancel",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).resolves.toMatchObject({ status: "cancelled", phase: "cancelled" });
    await expect(
      cancelled.service.execute({
        kind: "continue",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).resolves.toMatchObject({ status: "cancelled" });
    expect(cancelled.stopWatch).not.toHaveBeenCalled();
    expect(cancelled.revoke).not.toHaveBeenCalled();

    const revoked = createHarness();
    await expect(
      revoked.service.execute({
        kind: "revoke",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).resolves.toMatchObject({ status: "revoked", phase: "revoked" });
    expect(revoked.stopWatch).toHaveBeenCalledWith("access-token");
    expect(revoked.revoke).toHaveBeenCalledOnce();
    expect(revoked.store.connection).toMatchObject({
      status: "revoked",
      encryptedCredentials: null,
      grantedScopes: [],
      cursor: {},
    });
  });
});
