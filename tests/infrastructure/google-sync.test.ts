import { randomBytes } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import {
  type GmailAttachment,
  type GmailHistoryPage,
  type GmailMessage,
  type GmailMessageIdPage,
  type GmailRetrievedAttachment,
  GOOGLE_GMAIL_READONLY_SCOPE,
  GoogleAdapterError,
  GoogleSyncTokenExpiredError,
  type GoogleTokenSet,
} from "../../src/adapters/google/index.js";
import type { ApplicationResult } from "../../src/application/index.js";
import { HouseholdIdSchema } from "../../src/domain/index.js";
import {
  GMAIL_DISCOVERY_MESSAGE_COUNT_MAX,
  type GmailDiscoveryCompletionPort,
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
function syncState(overrides: Partial<GmailSyncState> = {}): GmailSyncState {
  return gmailSyncStateSchema.parse({
    schemaVersion: 2,
    revision: 0,
    phase: "live",
    requestedDepth: "full_history",
    boundaryAt: NOW.toISOString(),
    scanPageToken: null,
    scanProcessedMessageIds: [],
    history: { cursorId: "100", startId: null, pageToken: null, targetId: null },
    recovery: null,
    lastSuccessfulSyncAt: null,
    discovery: null,
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

function message(version: number, id = "message-1", overrides: Partial<GmailMessage> = {}): GmailMessage {
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
    ...overrides,
  };
}

function attachment(index: number, overrides: Partial<GmailAttachment> = {}): GmailAttachment {
  return {
    partId: `part-${index}`,
    providerAttachmentId: `attachment-${index}`,
    filename: `attachment-${index}.txt`,
    mimeType: "text/plain",
    sizeBytes: 4,
    contentId: null,
    inline: false,
    embeddedDataBase64Url: null,
    ...overrides,
  };
}

class MemoryGoogleStore implements GoogleConnectionDirectoryPort, GoogleSyncRepositoryPort {
  public readonly sources = new Map<string, PersistPersonalGmailSourceInput & { revision: number }>();
  public readonly savedStates: GmailSyncState[] = [];
  public recoveryRun: { generationId: string; targetHistoryId: string } | null = null;
  public readonly recoverySeen = new Set<string>();
  public readonly recoveryMissing = new Set<string>();
  public readonly recoveryProcessed = new Set<string>();
  public connection: GoogleSyncConnection;

  public constructor(connectionRecord: GoogleSyncConnection) {
    this.connection = connectionRecord;
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
    if (
      previous.kind === "gmail_message" &&
      input.kind === "gmail_message" &&
      previous.metadata.contentCompleteness === "full" &&
      input.metadata.contentCompleteness === "full"
    ) {
      const previousHistoryId = previous.metadata.messageHistoryId;
      const incomingHistoryId = input.metadata.messageHistoryId;
      if (
        typeof previousHistoryId === "string" &&
        typeof incomingHistoryId === "string" &&
        BigInt(incomingHistoryId) < BigInt(previousHistoryId)
      ) {
        return {
          sourceItemId: `source-${input.externalId}`,
          disposition: "unchanged",
          revision: previous.revision,
          retainedExisting: "stale",
        };
      }
      if (
        previous.contentHash !== input.contentHash &&
        (typeof previousHistoryId !== "string" ||
          typeof incomingHistoryId !== "string" ||
          incomingHistoryId === previousHistoryId)
      ) {
        throw new Error("conflicting Gmail full content");
      }
    }
    const authoritativeRecovery =
      input.kind === "gmail_message" && input.metadata.discoveryMode === "recovery";
    if (
      input.kind === "gmail_message" &&
      ((previous.kind === "gmail_message_deleted" && !authoritativeRecovery) ||
        (previous.metadata.contentCompleteness === "full" &&
          input.metadata.contentCompleteness === "metadata"))
    ) {
      if (previous.kind !== "gmail_message_deleted") {
        this.sources.set(input.externalId, {
          ...previous,
          metadata: {
            ...previous.metadata,
            discoveryMode: input.metadata.discoveryMode,
            discoveryHistoryId: input.metadata.discoveryHistoryId,
            providerEventIds: input.metadata.providerEventIds,
            contentCompleteness: "full",
          },
        });
      }
      return {
        sourceItemId: `source-${input.externalId}`,
        disposition: "unchanged",
        revision: previous.revision,
        retainedExisting:
          previous.kind === "gmail_message_deleted" ? ("deleted" as const) : ("full" as const),
      };
    }
    if (previous.contentHash === input.contentHash && !authoritativeRecovery) {
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

  public async beginGmailRecovery(input: Parameters<GoogleSyncRepositoryPort["beginGmailRecovery"]>[0]) {
    if (this.recoveryRun?.targetHistoryId === input.targetHistoryId) return this.recoveryRun;
    this.recoverySeen.clear();
    this.recoveryMissing.clear();
    this.recoveryProcessed.clear();
    this.recoveryRun = {
      generationId: "55555555-5555-4555-8555-555555555555",
      targetHistoryId: input.targetHistoryId,
    };
    return this.recoveryRun;
  }

  public async recordGmailRecoveryPage(
    input: Parameters<GoogleSyncRepositoryPort["recordGmailRecoveryPage"]>[0],
  ) {
    if (this.recoveryRun?.generationId !== input.generationId) throw new Error("unknown recovery");
    for (const externalId of input.externalIds) {
      this.recoverySeen.add(externalId);
    }
    return {
      pendingExternalIds: input.externalIds.filter((externalId) => !this.recoveryProcessed.has(externalId)),
    };
  }

  public async markGmailRecoveryWorkProcessed(
    input: Parameters<GoogleSyncRepositoryPort["markGmailRecoveryWorkProcessed"]>[0],
  ) {
    if (this.recoveryRun?.generationId !== input.generationId) throw new Error("unknown recovery");
    this.recoveryProcessed.add(input.externalId);
  }

  public async listMissingGmailRecoverySources(
    input: Parameters<GoogleSyncRepositoryPort["listMissingGmailRecoverySources"]>[0],
  ) {
    if (this.recoveryRun?.generationId !== input.generationId) throw new Error("unknown recovery");
    const pending = [...this.recoveryMissing]
      .filter((externalId) => !this.recoveryProcessed.has(externalId))
      .sort()
      .slice(0, input.limit);
    if (pending.length > 0) return pending;
    const materialized = [...this.sources.entries()]
      .filter(([externalId, source]) => source.kind === "gmail_message" && !this.recoverySeen.has(externalId))
      .map(([externalId]) => externalId)
      .sort()
      .slice(0, input.limit);
    for (const externalId of materialized) {
      this.recoverySeen.add(externalId);
      this.recoveryMissing.add(externalId);
    }
    return materialized;
  }

  public async finishGmailRecovery(input: Parameters<GoogleSyncRepositoryPort["finishGmailRecovery"]>[0]) {
    if (this.connection.status !== "active") return "inactive" as const;
    if (this.recoveryRun?.generationId !== input.generationId) return "conflict" as const;
    const saved = await this.saveGmailSyncState(input);
    if (saved !== "updated") return saved;
    this.recoveryRun = null;
    this.recoverySeen.clear();
    this.recoveryMissing.clear();
    this.recoveryProcessed.clear();
    return "updated" as const;
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
  profile?: () => Promise<{ emailAddress: string; historyId: string }>;
  history?: (input: Parameters<GmailProviderPort["listHistoryPage"]>[0]) => Promise<GmailHistoryPage>;
  list?: (input: Parameters<GmailProviderPort["listMessageIdsPage"]>[0]) => Promise<GmailMessageIdPage>;
  get?: (input: Parameters<GmailProviderPort["getMessage"]>[0]) => Promise<GmailMessage>;
  retrieve?: (
    input: Parameters<GmailProviderPort["retrieveAttachment"]>[0],
  ) => Promise<GmailRetrievedAttachment>;
};

function createHarness(options: FakeGmailOptions = {}) {
  const secretBox = new SecretBox(randomBytes(32).toString("base64url"));
  const store = new MemoryGoogleStore(connection(secretBox));
  const queries: string[] = [];
  const listInputs: Parameters<GmailProviderPort["listMessageIdsPage"]>[0][] = [];
  const getInputs: Parameters<GmailProviderPort["getMessage"]>[0][] = [];
  const retrieveInputs: Parameters<GmailProviderPort["retrieveAttachment"]>[0][] = [];
  const applicationItems: unknown[] = [];
  const completionPublishes: Parameters<GmailDiscoveryCompletionPort["publish"]>[0][] = [];
  const completionControl = { fail: false };
  const applicationControl = { fail: false };
  const profileHistoryId = { value: "100" };
  const revoke = vi.fn(async () => undefined);
  const gmail: GmailProviderPort = {
    async getProfile() {
      return options.profile?.() ?? {
        emailAddress: "parent@example.com",
        historyId: profileHistoryId.value,
      };
    },
    async getMessage(input) {
      getInputs.push(structuredClone(input));
      return options.get?.(input) ?? message(1, input.messageId);
    },
    async retrieveAttachment(input) {
      retrieveInputs.push(structuredClone(input));
      if (!options.retrieve) throw new Error("unexpected attachment retrieval");
      return options.retrieve(input);
    },
    listHistoryPage:
      options.history ??
      (async ({ startHistoryId }) => ({
        changes: [],
        mailboxHistoryId: startHistoryId,
        nextPageToken: null,
      })),
    async listMessageIdsPage(input) {
      listInputs.push(structuredClone(input));
      if (input.query) queries.push(input.query);
      return options.list?.(input) ?? { messages: [], nextPageToken: null, resultSizeEstimate: 0 };
    },
  };
  const oauth: GoogleCredentialLifecyclePort = {
    async refresh(tokens) {
      return { ...tokens, accessToken: "refreshed-access", expiresAt: "2027-01-02T08:00:00.000Z" };
    },
    revoke,
  };
  const completionDigest: GmailDiscoveryCompletionPort = {
    async publish(input) {
      completionPublishes.push(structuredClone(input));
      if (completionControl.fail) throw new Error("completion publication failed");
      return store.saveGmailSyncState(input);
    },
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
        if (applicationControl.fail) throw new Error("application processing failed");
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
    completionDigest,
    secretBox,
    now: () => NOW,
    pageSize: 25,
  });
  return {
    service,
    store,
    secretBox,
    queries,
    listInputs,
    getInputs,
    retrieveInputs,
    applicationItems,
    completionPublishes,
    completionControl,
    applicationControl,
    profileHistoryId,
    revoke,
  };
}

function pollWork(harness: { profileHistoryId: { value: string } }, historyId: string) {
  harness.profileHistoryId.value = historyId;
  return {
    kind: "continue" as const,
    householdId: HOUSEHOLD_ID,
    adultId: ADULT_ID,
    connectionId: CONNECTION_ID,
  };
}

function historyChange(
  messageId: string,
  historyId: string,
  changeType: "message.added" | "message.deleted" | "labels.added" | "labels.removed",
) {
  return {
    schemaVersion: 1 as const,
    source: "gmail" as const,
    sourceScope: "personal" as const,
    googleSubject: "google-subject-parent",
    providerEventId: `history-${historyId}-${changeType}-${messageId}`,
    historyId,
    changeType,
    messageId,
    threadId: `thread-${messageId}`,
    labelIds: changeType.startsWith("labels.") ? ["STARRED"] : [],
  };
}

describe("GoogleSyncService", () => {
  it("fails closed when an explicit request tries to cross the connection's adult boundary", async () => {
    const harness = createHarness();
    const unsafeDirectory: GoogleConnectionDirectoryPort = {
      async getOwnedGoogleConnection() {
        return structuredClone(harness.store.connection);
      },
    };
    const service = new GoogleSyncService({
      directory: unsafeDirectory,
      repository: harness.store,
      gmail: {
        getProfile: async () => {
          throw new Error("must not fetch profile");
        },
        getMessage: async () => {
          throw new Error("must not fetch");
        },
        retrieveAttachment: async () => {
          throw new Error("must not retrieve");
        },
        listHistoryPage: async () => {
          throw new Error("must not list");
        },
        listMessageIdsPage: async () => {
          throw new Error("must not list");
        },
      },
      oauth: {
        refresh: async (tokens) => tokens,
        revoke: async () => undefined,
      },
      application: { process: async () => Promise.reject(new Error("must not process")) },
      completionDigest: { publish: async () => Promise.reject(new Error("must not publish")) },
      secretBox: harness.secretBox,
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

  it("rejects cross-account history before an unfetched deletion can become authoritative", async () => {
    const harness = createHarness({
      async history() {
        return {
          changes: [
            {
              ...historyChange("foreign-deletion", "101", "message.deleted"),
              googleSubject: "different-google-subject",
            },
          ],
          mailboxHistoryId: "101",
          nextPageToken: null,
        };
      },
    });

    await expect(harness.service.execute(pollWork(harness, "101"))).rejects.toMatchObject({
      code: "not_authorized",
    } satisfies Partial<GoogleSyncError>);
    expect(harness.store.sources.size).toBe(0);
    expect(harness.getInputs).toHaveLength(0);
  });

  it("reconciles both a recent addition and its later label-only revision", async () => {
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

    await expect(harness.service.execute(pollWork(harness, "102"))).resolves.toMatchObject({
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
      { ownerAdultId: ADULT_ID, revision: 2, idempotencyKey: expect.stringContaining("revision:2") },
      { ownerAdultId: ADULT_ID, revision: 3, idempotencyKey: expect.stringContaining("revision:3") },
    ]);
    const stored = harness.store.sources.get("message-1");
    expect(stored).toMatchObject({ adultId: ADULT_ID, kind: "gmail_message", revision: 3 });
    expect(stored?.encryptedContent).not.toContain("Pickup");
    expect(
      harness.secretBox.open(
        stored?.encryptedContent ?? "",
        gmailSourceContentAad(harness.store.connection, "message-1"),
      ),
    ).toContain("Pickup is at 6:00");
    expect((harness.store.connection.cursor.gmail as GmailSyncState).history).toEqual({
      cursorId: "102",
      startId: null,
      pageToken: null,
      targetId: null,
    });
    await expect(harness.service.execute(pollWork(harness, "102"))).resolves.toMatchObject({ status: "noop" });
    expect(harness.applicationItems).toHaveLength(2);
    expect(stored?.metadata).toMatchObject({ schemaVersion: 2, contentCompleteness: "full" });
    expect(harness.getInputs.map((input) => input.format)).toEqual(["metadata", "full", "metadata", "full"]);
  });

  it("bounds live hydration to twenty descriptors, ten MiB per file, and fifteen MiB total", async () => {
    const eightMiB = 8 * 1024 * 1024;
    const descriptors = [
      attachment(1, { sizeBytes: eightMiB }),
      attachment(2, { sizeBytes: eightMiB }),
      attachment(3, { sizeBytes: 11 * 1024 * 1024 }),
      ...Array.from({ length: 19 }, (_, index) => attachment(index + 4, { sizeBytes: 1 })),
    ];
    const harness = createHarness({
      async history() {
        return {
          changes: [historyChange("bounded-attachments", "101", "message.added")],
          mailboxHistoryId: "101",
          nextPageToken: null,
        };
      },
      async get({ messageId, format }) {
        return message(1, messageId, { attachments: format === "full" ? descriptors : [] });
      },
      async retrieve({ attachment: descriptor }) {
        const bytes = new Uint8Array(descriptor.sizeBytes);
        return {
          kind: "file",
          mediaType: "text/plain",
          filename: descriptor.filename,
          sizeBytes: bytes.byteLength,
          bytes,
        };
      },
    });

    await expect(harness.service.execute(pollWork(harness, "101"))).resolves.toMatchObject({
      processedMessages: 1,
    });

    expect(harness.retrieveInputs).toHaveLength(18);
    expect(harness.retrieveInputs[0]?.attachment.providerAttachmentId).toBe("attachment-1");
    expect(
      harness.retrieveInputs.some((input) => input.attachment.providerAttachmentId === "attachment-2"),
    ).toBe(false);
    expect(
      harness.retrieveInputs.some((input) => input.attachment.providerAttachmentId === "attachment-3"),
    ).toBe(false);
    expect(
      harness.retrieveInputs.some((input) => input.attachment.providerAttachmentId === "attachment-21"),
    ).toBe(false);
    const item = harness.applicationItems[0] as {
      attachmentRefs: string[];
      attachmentContents: Array<{ kind: string; reason?: string }>;
    };
    expect(item.attachmentRefs).toHaveLength(20);
    expect(item.attachmentContents).toHaveLength(20);
    expect(item.attachmentContents.slice(0, 3)).toMatchObject([
      { kind: "file" },
      { kind: "unavailable", reason: "too_large" },
      { kind: "unavailable", reason: "too_large" },
    ]);
  });

  it("retries a crash after full persistence without downgrading or changing the app idempotency key", async () => {
    const harness = createHarness({
      async history() {
        return {
          changes: [historyChange("crash-safe", "101", "message.added")],
          mailboxHistoryId: "101",
          nextPageToken: null,
        };
      },
    });
    harness.applicationControl.fail = true;

    await expect(harness.service.execute(pollWork(harness, "101"))).rejects.toMatchObject({
      code: "invalid_state",
    } satisfies Partial<GoogleSyncError>);
    expect(harness.store.sources.get("crash-safe")).toMatchObject({
      revision: 2,
      metadata: { contentCompleteness: "full" },
    });

    harness.applicationControl.fail = false;
    await expect(harness.service.execute(pollWork(harness, "101"))).resolves.toMatchObject({
      status: "processed",
    });
    expect(harness.store.sources.get("crash-safe")).toMatchObject({
      revision: 2,
      metadata: { contentCompleteness: "full" },
    });
    expect(
      harness.applicationItems.map((item) => (item as { idempotencyKey: string }).idempotencyKey),
    ).toEqual([
      `gmail:${CONNECTION_ID}:crash-safe:revision:2`,
      `gmail:${CONNECTION_ID}:crash-safe:revision:2`,
    ]);
    expect(harness.getInputs.map((input) => input.format)).toEqual(["metadata", "full", "metadata", "full"]);
  });

  it("keeps a deletion tombstone authoritative over stale live hydration", async () => {
    const harness = createHarness({
      async history() {
        return {
          changes: [historyChange("deleted-sticky", "101", "message.added")],
          mailboxHistoryId: "101",
          nextPageToken: null,
        };
      },
    });
    harness.store.sources.set("deleted-sticky", {
      householdId: HOUSEHOLD_ID,
      adultId: ADULT_ID,
      connectionId: CONNECTION_ID,
      externalId: "deleted-sticky",
      kind: "gmail_message_deleted",
      occurredAt: NOW.toISOString(),
      contentHash: `sha256:${"d".repeat(64)}`,
      encryptedContent: "deleted-ciphertext",
      metadata: {
        schemaVersion: 2,
        provider: "gmail",
        sourceScope: "personal",
        contentCompleteness: "metadata",
        deleted: true,
      },
      revision: 3,
    });

    await expect(harness.service.execute(pollWork(harness, "101"))).resolves.toMatchObject({
      status: "processed",
      processedMessages: 1,
    });
    expect(harness.getInputs.map((input) => input.format)).toEqual(["metadata"]);
    expect(harness.retrieveInputs).toHaveLength(0);
    expect(harness.applicationItems).toHaveLength(0);
    expect(harness.store.sources.get("deleted-sticky")).toMatchObject({
      kind: "gmail_message_deleted",
      encryptedContent: "deleted-ciphertext",
      revision: 3,
    });
  });

  it("does not send a rejected lower provider revision to the application", async () => {
    const harness = createHarness({
      async history() {
        return {
          changes: [historyChange("stale-live", "103", "message.added")],
          mailboxHistoryId: "103",
          nextPageToken: null,
        };
      },
    });
    harness.store.sources.set("stale-live", {
      householdId: HOUSEHOLD_ID,
      adultId: ADULT_ID,
      connectionId: CONNECTION_ID,
      externalId: "stale-live",
      kind: "gmail_message",
      occurredAt: NOW.toISOString(),
      contentHash: `sha256:${"f".repeat(64)}`,
      encryptedContent: "newer-full-ciphertext",
      metadata: {
        schemaVersion: 2,
        provider: "gmail",
        sourceScope: "personal",
        contentCompleteness: "full",
        messageHistoryId: "102",
      },
      revision: 2,
    });

    await expect(harness.service.execute(pollWork(harness, "103"))).resolves.toMatchObject({
      status: "processed",
      processedMessages: 1,
    });
    expect(harness.getInputs.map((input) => input.format)).toEqual(["metadata", "full"]);
    expect(harness.applicationItems).toHaveLength(0);
    expect(harness.store.sources.get("stale-live")).toMatchObject({
      encryptedContent: "newer-full-ciphertext",
      revision: 2,
      metadata: { contentCompleteness: "full", messageHistoryId: "102" },
    });
  });

  it("drains pushed history before scanning and preserves every scan checkpoint", async () => {
    const harness = createHarness({
      async history() {
        return { changes: [], mailboxHistoryId: "101", nextPageToken: null };
      },
      async list() {
        throw new Error("scan must not run before pushed history");
      },
    });
    harness.store.connection.cursor = {
      gmail: syncState({
        phase: "recent_90_days",
        scanPageToken: "scan-page-2",
        scanProcessedMessageIds: ["already-persisted"],
        discovery: { runId: "run-1", messageCount: 7, status: "collecting" },
      }),
    };

    await expect(harness.service.execute(pollWork(harness, "101"))).resolves.toMatchObject({
      status: "continuation_required",
      phase: "recent_90_days",
    });
    expect(harness.listInputs).toHaveLength(0);
    expect(harness.store.connection.cursor.gmail).toMatchObject({
      phase: "recent_90_days",
      scanPageToken: "scan-page-2",
      scanProcessedMessageIds: ["already-persisted"],
      discovery: { runId: "run-1", messageCount: 7, status: "collecting" },
      history: { cursorId: "101", startId: null, pageToken: null, targetId: null },
    });
  });

  it("paginates an expired-history gap without advancing the cursor past failed private ingestion", async () => {
    const harness = createHarness({
      async history() {
        throw new GoogleSyncTokenExpiredError("gmail");
      },
      async list({ pageToken }) {
        return pageToken
          ? {
              messages: [{ messageId: "recovery-page-2", threadId: "thread-page-2" }],
              nextPageToken: null,
              resultSizeEstimate: 1,
            }
          : {
              messages: [{ messageId: "recovery-page-1", threadId: "thread-page-1" }],
              nextPageToken: "recovery-page-token-2",
              resultSizeEstimate: 2,
            };
      },
      async get({ messageId }) {
        if (messageId === "missing-from-provider") {
          throw new GoogleAdapterError("Gmail message was not found", "not_found", 404, false);
        }
        return message(1, messageId, { internalDate: "2026-12-30T07:00:00.000Z" });
      },
    });
    harness.store.connection.cursor = {
      gmail: syncState({ lastSuccessfulSyncAt: "2026-12-29T08:00:00.000Z" }),
    };
    harness.store.sources.set("missing-from-provider", {
      householdId: HOUSEHOLD_ID,
      adultId: ADULT_ID,
      connectionId: CONNECTION_ID,
      externalId: "missing-from-provider",
      kind: "gmail_message",
      occurredAt: "2026-12-29T07:00:00.000Z",
      contentHash: "sha256:existing",
      encryptedContent: "sealed:existing",
      metadata: {
        schemaVersion: 2,
        provider: "gmail",
        sourceScope: "personal",
        contentCompleteness: "full",
      },
      revision: 1,
    });

    await expect(harness.service.execute(pollWork(harness, "150"))).resolves.toMatchObject({
      status: "continuation_required",
      phase: "live",
      processedMessages: 0,
    });
    expect((harness.store.connection.cursor.gmail as GmailSyncState).history).toEqual({
      cursorId: null,
      startId: "150",
      pageToken: null,
      targetId: "150",
    });

    await expect(
      harness.service.execute({
        kind: "continue",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).resolves.toMatchObject({ status: "continuation_required", processedMessages: 1 });
    expect((harness.store.connection.cursor.gmail as GmailSyncState).history.pageToken).toBe(
      "recovery-page-token-2",
    );

    harness.applicationControl.fail = true;
    await expect(
      harness.service.execute({
        kind: "continue",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).rejects.toMatchObject({ code: "invalid_state" } satisfies Partial<GoogleSyncError>);
    expect((harness.store.connection.cursor.gmail as GmailSyncState).history).toEqual({
      cursorId: null,
      startId: "150",
      pageToken: "recovery-page-token-2",
      targetId: "150",
    });

    harness.applicationControl.fail = false;
    await expect(
      harness.service.execute({
        kind: "continue",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).resolves.toMatchObject({ status: "continuation_required", processedMessages: 1 });

    await expect(
      harness.service.execute({
        kind: "continue",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).resolves.toMatchObject({ status: "continuation_required", processedDeletions: 1 });
    await expect(
      harness.service.execute({
        kind: "continue",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).resolves.toMatchObject({ status: "processed" });

    expect(harness.queries).toEqual([]);
    expect(harness.listInputs).toMatchObject([
      { maxResults: 100, includeSpamTrash: true },
      { maxResults: 100, includeSpamTrash: true, pageToken: "recovery-page-token-2" },
      { maxResults: 100, includeSpamTrash: true, pageToken: "recovery-page-token-2" },
    ]);
    expect((harness.store.connection.cursor.gmail as GmailSyncState).history).toEqual({
      cursorId: "150",
      startId: null,
      pageToken: null,
      targetId: null,
    });
    expect(harness.store.sources.get("missing-from-provider")?.kind).toBe("gmail_message_deleted");
    expect(harness.applicationItems).toHaveLength(4);
    expect(harness.getInputs.map((input) => input.format)).toEqual([
      "metadata",
      "full",
      "metadata",
      "full",
      "metadata",
      "full",
      "metadata",
    ]);
    expect(harness.completionPublishes).toHaveLength(0);
  });

  it("uses the newer successful-sync boundary for recovery and resumes the current scan", async () => {
    const harness = createHarness({
      async history() {
        throw new GoogleSyncTokenExpiredError("gmail");
      },
      async list() {
        return {
          messages: [{ messageId: "recovered", threadId: "thread-recovered" }],
          nextPageToken: null,
          resultSizeEstimate: 40,
        };
      },
    });
    harness.store.connection.cursor = {
      gmail: syncState({
        phase: "one_year_backfill",
        scanPageToken: "year-page-4",
        scanProcessedMessageIds: ["year-item"],
        lastSuccessfulSyncAt: "2027-01-01T07:00:00.000Z",
        discovery: { runId: "run-recovery", messageCount: 12, status: "collecting" },
      }),
    };

    await expect(harness.service.execute(pollWork(harness, "150"))).resolves.toMatchObject({
      status: "continuation_required",
      phase: "one_year_backfill",
      processedMessages: 0,
    });
    await expect(
      harness.service.execute({
        kind: "continue",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).resolves.toMatchObject({ status: "continuation_required", processedMessages: 1 });
    await expect(
      harness.service.execute({
        kind: "continue",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).resolves.toMatchObject({ status: "continuation_required", processedMessages: 0 });
    expect(harness.queries).toEqual([]);
    expect(harness.listInputs).toHaveLength(1);
    expect(harness.store.connection.cursor.gmail).toMatchObject({
      phase: "one_year_backfill",
      scanPageToken: "year-page-4",
      scanProcessedMessageIds: ["year-item"],
      discovery: { runId: "run-recovery", messageCount: 12, status: "collecting" },
      history: { cursorId: "150", startId: null, pageToken: null, targetId: null },
    });
    expect(harness.applicationItems).toHaveLength(1);
    expect(harness.getInputs.map((input) => input.format)).toEqual(["metadata", "full"]);
    expect(harness.completionPublishes).toHaveLength(0);
  });

  it("saturates the durable discovery count and recovers pending publication idempotently", async () => {
    const harness = createHarness({
      async list() {
        return {
          messages: [{ messageId: "at-cap", threadId: "thread-cap" }],
          nextPageToken: null,
          resultSizeEstimate: 1,
        };
      },
    });
    harness.store.connection.cursor = {
      gmail: syncState({
        phase: "recent_90_days",
        requestedDepth: "recent_90_days",
        discovery: {
          runId: "run-at-cap",
          messageCount: GMAIL_DISCOVERY_MESSAGE_COUNT_MAX,
          status: "collecting",
        },
      }),
    };
    harness.completionControl.fail = true;

    await expect(
      harness.service.execute({
        kind: "continue",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).rejects.toMatchObject({ code: "invalid_state" } satisfies Partial<GoogleSyncError>);
    expect(harness.store.connection.cursor.gmail).toMatchObject({
      phase: "live",
      discovery: {
        runId: "run-at-cap",
        messageCount: GMAIL_DISCOVERY_MESSAGE_COUNT_MAX,
        status: "pending",
      },
    });

    harness.completionControl.fail = false;
    await expect(
      harness.service.execute({
        kind: "continue",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).resolves.toMatchObject({ status: "processed", phase: "live" });
    expect(harness.store.connection.cursor.gmail).toMatchObject({
      discovery: {
        runId: "run-at-cap",
        messageCount: GMAIL_DISCOVERY_MESSAGE_COUNT_MAX,
        status: "published",
      },
    });
    expect(harness.applicationItems).toHaveLength(1);
    expect(harness.completionPublishes).toHaveLength(2);
  });

  it("durably cancels without touching Google and revokes both provider and local credentials", async () => {
    const cancelled = createHarness();
    cancelled.store.connection.cursor = {
      gmail: syncState({
        phase: "recent_90_days",
        discovery: { runId: "cancelled-run", messageCount: 9, status: "collecting" },
      }),
    };
    await expect(
      cancelled.service.execute({
        kind: "cancel",
        householdId: HOUSEHOLD_ID,
        adultId: ADULT_ID,
        connectionId: CONNECTION_ID,
      }),
    ).resolves.toMatchObject({ status: "cancelled", phase: "cancelled" });
    expect((cancelled.store.connection.cursor.gmail as GmailSyncState).discovery).toBeNull();
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
