import { z } from "zod";
import {
  type GmailAdapter,
  type GmailHistoryChange,
  type GmailMessage,
  GOOGLE_GMAIL_READONLY_SCOPE,
  GoogleAdapterError,
  type GoogleOAuthAdapter,
  GoogleSyncTokenExpiredError,
  type GoogleTokenSet,
  gmailPubSubEventSchema,
  googleTokenSetSchema,
} from "../adapters/google/index.js";
import { type FlorenceApplication, type GmailInboxItem, GmailInboxItemSchema } from "../application/index.js";
import { canonicalJson, sha256 } from "../security/canonical-json.js";
import type { SecretBox } from "../security/secret-box.js";

const instantSchema = z.iso.datetime({ offset: true });
const jsonObjectSchema = z.record(z.string(), z.unknown());
const googleConnectionStatusSchema = z.enum(["active", "reauth_required", "revoked", "error"]);

export const gmailSyncDepthSchema = z.enum(["recent_90_days", "one_year", "full_history"]);
export type GmailSyncDepth = z.infer<typeof gmailSyncDepthSchema>;

export const gmailSyncPhaseSchema = z.enum([
  "recent_90_days",
  "one_year_backfill",
  "full_history_backfill",
  "live",
  "cancelled",
  "reauth_required",
  "revoked",
]);
export type GmailSyncPhase = z.infer<typeof gmailSyncPhaseSchema>;

const gmailHistoryCursorSchema = z.strictObject({
  cursorId: z.string().regex(/^\d+$/).nullable(),
  startId: z.string().regex(/^\d+$/).nullable(),
  pageToken: z.string().min(1).nullable(),
  targetId: z.string().regex(/^\d+$/).nullable(),
});

const gmailWatchStateSchema = z.strictObject({
  historyId: z.string().regex(/^\d+$/),
  expiresAt: instantSchema,
  subscription: z.string().min(1).max(1_000),
});

export const gmailSyncStateSchema = z.strictObject({
  schemaVersion: z.literal(1),
  revision: z.number().int().nonnegative(),
  phase: gmailSyncPhaseSchema,
  requestedDepth: gmailSyncDepthSchema,
  boundaryAt: instantSchema,
  scanPageToken: z.string().min(1).nullable(),
  history: gmailHistoryCursorSchema,
  watch: gmailWatchStateSchema.nullable(),
  lastSuccessfulSyncAt: instantSchema.nullable(),
  cancellation: z
    .strictObject({
      requestedAt: instantSchema,
      requestedByAdultId: z.string().min(1).max(500),
    })
    .nullable(),
});

export type GmailSyncState = z.infer<typeof gmailSyncStateSchema>;

export const googleSyncConnectionSchema = z.strictObject({
  id: z.uuid(),
  householdId: z.string().min(1).max(500),
  adultId: z.string().min(1).max(500),
  provider: z.literal("google"),
  externalAccountId: z.string().min(1).max(500),
  email: z.email().nullable(),
  encryptedCredentials: z.string().min(1).nullable(),
  grantedScopes: z.array(z.string().min(1).max(500)).max(100),
  status: googleConnectionStatusSchema,
  cursor: jsonObjectSchema,
  metadata: jsonObjectSchema,
});

export type GoogleSyncConnection = z.infer<typeof googleSyncConnectionSchema>;

export const gmailSyncWorkSchema = z.discriminatedUnion("kind", [
  z.strictObject({
    kind: z.literal("history_notice"),
    event: gmailPubSubEventSchema,
  }),
  z.strictObject({
    kind: z.literal("start"),
    householdId: z.string().min(1).max(500),
    adultId: z.string().min(1).max(500),
    connectionId: z.uuid(),
    depth: gmailSyncDepthSchema.default("full_history"),
  }),
  z.strictObject({
    kind: z.literal("continue"),
    householdId: z.string().min(1).max(500),
    adultId: z.string().min(1).max(500),
    connectionId: z.uuid(),
  }),
  z.strictObject({
    kind: z.literal("renew_watch"),
    householdId: z.string().min(1).max(500),
    adultId: z.string().min(1).max(500),
    connectionId: z.uuid(),
  }),
  z.strictObject({
    kind: z.literal("cancel"),
    householdId: z.string().min(1).max(500),
    adultId: z.string().min(1).max(500),
    connectionId: z.uuid(),
  }),
  z.strictObject({
    kind: z.literal("revoke"),
    householdId: z.string().min(1).max(500),
    adultId: z.string().min(1).max(500),
    connectionId: z.uuid(),
  }),
]);

export type GmailSyncWork = z.infer<typeof gmailSyncWorkSchema>;

export type GmailSyncResult = {
  status: "processed" | "continuation_required" | "noop" | "cancelled" | "revoked" | "reauth_required";
  connectionId: string;
  householdId: string;
  adultId: string;
  phase: GmailSyncPhase;
  processedMessages: number;
  processedDeletions: number;
};

/**
 * Implementations must scope both lookups at the storage layer. Returning an
 * unscoped connection and filtering it in the caller is not sufficient.
 */
export interface GoogleConnectionDirectoryPort {
  findActiveGmailConnections(input: {
    normalizedMailboxEmail: string;
    subscription: string;
  }): Promise<readonly GoogleSyncConnection[]>;

  getOwnedGoogleConnection(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
  }): Promise<GoogleSyncConnection | null>;
}

export interface PersistPersonalGmailSourceInput {
  householdId: string;
  adultId: string;
  connectionId: string;
  externalId: string;
  kind: "gmail_message" | "gmail_message_deleted";
  occurredAt: string;
  contentHash: string;
  encryptedContent: string;
  metadata: Record<string, unknown>;
}

export type PersistPersonalGmailSourceResult = {
  sourceItemId: string;
  disposition: "inserted" | "unchanged" | "revised";
  revision: number;
};

export type ScopedMutationResult = "updated" | "conflict" | "inactive" | "not_found";

/** Every mutation must repeat the household/adult/connection predicate and fail closed. */
export interface GoogleSyncRepositoryPort {
  replaceEncryptedCredentials(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    expectedCiphertext: string;
    encryptedCredentials: string;
    grantedScopes: readonly string[];
  }): Promise<ScopedMutationResult>;

  saveGmailSyncState(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    expectedRevision: number;
    state: GmailSyncState;
  }): Promise<ScopedMutationResult>;

  persistPersonalGmailSource(
    input: PersistPersonalGmailSourceInput,
  ): Promise<PersistPersonalGmailSourceResult>;

  markConnectionStatus(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    status: "reauth_required" | "error";
  }): Promise<"updated" | "not_found">;

  revokeConnection(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    revokedAt: string;
  }): Promise<"revoked" | "not_found">;
}

export interface GmailProviderPort {
  getMessage(input: {
    accessToken: string;
    googleSubject: string;
    messageId: string;
  }): ReturnType<GmailAdapter["getMessage"]>;
  listHistoryPage(input: {
    accessToken: string;
    googleSubject: string;
    startHistoryId: string;
    pageToken?: string;
    maxResults?: number;
  }): ReturnType<GmailAdapter["listHistoryPage"]>;
  listMessageIdsPage(input: {
    accessToken: string;
    pageToken?: string;
    maxResults?: number;
    query?: string;
    includeSpamTrash?: boolean;
  }): ReturnType<GmailAdapter["listMessageIdsPage"]>;
  startWatch(input: { accessToken: string; topicName: string }): ReturnType<GmailAdapter["startWatch"]>;
  stopWatch(accessToken: string): ReturnType<GmailAdapter["stopWatch"]>;
}

export interface GoogleCredentialLifecyclePort {
  refresh(tokens: GoogleTokenSet): ReturnType<GoogleOAuthAdapter["refresh"]>;
  revoke(tokens: GoogleTokenSet): ReturnType<GoogleOAuthAdapter["revoke"]>;
}

export interface GoogleSyncServiceOptions {
  directory: GoogleConnectionDirectoryPort;
  repository: GoogleSyncRepositoryPort;
  gmail: GmailProviderPort;
  oauth: GoogleCredentialLifecyclePort;
  application: Pick<FlorenceApplication, "process">;
  secretBox: Pick<SecretBox, "open" | "seal">;
  gmailTopicName: string;
  gmailPubSubSubscription: string;
  now?: () => Date;
  refreshSkewMs?: number;
  pageSize?: number;
}

export class GoogleSyncError extends Error {
  public override readonly name = "GoogleSyncError";

  public constructor(
    message: string,
    public readonly code:
      | "invalid_state"
      | "not_authorized"
      | "ambiguous_connection"
      | "conflict"
      | "cancelled"
      | "provider_failure",
    public readonly retryable: boolean,
  ) {
    super(message);
  }
}

type ProcessCounts = { processedMessages: number; processedDeletions: number };

const EMPTY_COUNTS: ProcessCounts = { processedMessages: 0, processedDeletions: 0 };
const DAY_MS = 24 * 60 * 60 * 1_000;

export class GoogleSyncService {
  readonly #directory: GoogleConnectionDirectoryPort;
  readonly #repository: GoogleSyncRepositoryPort;
  readonly #gmail: GmailProviderPort;
  readonly #oauth: GoogleCredentialLifecyclePort;
  readonly #application: Pick<FlorenceApplication, "process">;
  readonly #secretBox: Pick<SecretBox, "open" | "seal">;
  readonly #gmailTopicName: string;
  readonly #gmailPubSubSubscription: string;
  readonly #now: () => Date;
  readonly #refreshSkewMs: number;
  readonly #pageSize: number;

  public constructor(options: GoogleSyncServiceOptions) {
    this.#directory = options.directory;
    this.#repository = options.repository;
    this.#gmail = options.gmail;
    this.#oauth = options.oauth;
    this.#application = options.application;
    this.#secretBox = options.secretBox;
    this.#gmailTopicName = z
      .string()
      .regex(/^projects\/[^/]+\/topics\/[^/]+$/)
      .parse(options.gmailTopicName);
    this.#gmailPubSubSubscription = z.string().min(1).max(1_000).parse(options.gmailPubSubSubscription);
    this.#now = options.now ?? (() => new Date());
    this.#refreshSkewMs = z
      .number()
      .int()
      .nonnegative()
      .parse(options.refreshSkewMs ?? 5 * 60_000);
    this.#pageSize = z
      .number()
      .int()
      .min(1)
      .max(500)
      .parse(options.pageSize ?? 100);
  }

  public async execute(rawWork: unknown, signal?: AbortSignal): Promise<GmailSyncResult> {
    const work = gmailSyncWorkSchema.parse(rawWork);
    assertNotAborted(signal);

    try {
      switch (work.kind) {
        case "history_notice":
          return await this.#handleHistoryNotice(work.event, signal);
        case "start":
          return await this.#start(work, signal);
        case "continue":
          return await this.#continue(work, signal);
        case "renew_watch":
          return await this.#renewWatch(work, signal);
        case "cancel":
          return await this.#cancel(work);
        case "revoke":
          return await this.#revoke(work, signal);
      }
    } catch (error) {
      if (isAbort(error, signal)) {
        throw new GoogleSyncError("Google synchronization was cancelled", "cancelled", true);
      }
      if (error instanceof GoogleSyncError) throw error;
      if (error instanceof GoogleAdapterError) {
        throw new GoogleSyncError(
          "Google synchronization failed at the provider boundary",
          "provider_failure",
          error.retryable,
        );
      }
      throw new GoogleSyncError("Google synchronization failed closed", "invalid_state", false);
    }
  }

  public processPush(
    event: z.input<typeof gmailPubSubEventSchema>,
    signal?: AbortSignal,
  ): Promise<GmailSyncResult> {
    return this.execute({ kind: "history_notice", event }, signal);
  }

  async #handleHistoryNotice(
    event: z.infer<typeof gmailPubSubEventSchema>,
    signal?: AbortSignal,
  ): Promise<GmailSyncResult> {
    if (event.subscription !== this.#gmailPubSubSubscription) {
      throw new GoogleSyncError("Gmail notification subscription is not authorized", "not_authorized", false);
    }
    const normalizedMailboxEmail = normalizeEmail(event.mailboxEmail);
    const candidates = await this.#directory.findActiveGmailConnections({
      normalizedMailboxEmail,
      subscription: event.subscription,
    });
    if (candidates.length !== 1) {
      throw new GoogleSyncError(
        "Gmail notification could not be attributed to exactly one adult account",
        candidates.length === 0 ? "not_authorized" : "ambiguous_connection",
        false,
      );
    }
    const connection = requireValidConnection(candidates[0]);
    if (connection.status !== "active" || normalizeEmail(connection.email) !== normalizedMailboxEmail) {
      throw new GoogleSyncError("Gmail notification ownership did not match", "not_authorized", false);
    }
    const state = stateFromConnection(connection, this.#now());
    if (state.watch?.subscription !== event.subscription) {
      throw new GoogleSyncError(
        "Gmail notification is not bound to this connection",
        "not_authorized",
        false,
      );
    }
    if (state.phase === "cancelled") return result(connection, state, "cancelled", EMPTY_COUNTS);
    if (state.phase === "revoked") return result(connection, state, "revoked", EMPTY_COUNTS);
    if (state.phase === "reauth_required") {
      return result(connection, state, "reauth_required", EMPTY_COUNTS);
    }

    const targetId = maxHistoryId(state.history.targetId, event.historyId);
    if (
      state.phase === "live" &&
      state.history.pageToken === null &&
      state.history.cursorId !== null &&
      compareHistoryIds(event.historyId, state.history.cursorId) <= 0
    ) {
      return result(connection, state, "noop", EMPTY_COUNTS);
    }

    const targetedState: GmailSyncState = {
      ...state,
      history: { ...state.history, targetId },
    };
    if (targetedState.phase !== "live") {
      return this.#processScanPage(connection, targetedState, signal);
    }
    if (targetedState.history.cursorId === null) {
      return this.#processScanPage(
        connection,
        restartRecentScan(targetedState, this.#now().toISOString()),
        signal,
      );
    }
    return this.#processHistoryPage(connection, targetedState, event.publishedAt, signal);
  }

  async #start(
    work: Extract<GmailSyncWork, { kind: "start" }>,
    signal?: AbortSignal,
  ): Promise<GmailSyncResult> {
    const connection = await this.#ownedActiveConnection(work);
    const current = stateFromConnection(connection, this.#now());
    const watch = await this.#withCredentials(connection, signal, (accessToken) =>
      this.#gmail.startWatch({ accessToken, topicName: this.#gmailTopicName }),
    );
    const state = gmailSyncStateSchema.parse({
      ...current,
      phase: "recent_90_days",
      requestedDepth: work.depth,
      boundaryAt: this.#now().toISOString(),
      scanPageToken: null,
      history: {
        cursorId: current.history.cursorId ?? watch.historyId,
        startId: null,
        pageToken: null,
        targetId: current.history.targetId,
      },
      watch: {
        historyId: watch.historyId,
        expiresAt: watch.expiresAt,
        subscription: this.#gmailPubSubSubscription,
      },
      cancellation: null,
    });
    const saved = await this.#saveState(connection, state);
    return result(connection, saved, "continuation_required", EMPTY_COUNTS);
  }

  async #continue(
    work: Extract<GmailSyncWork, { kind: "continue" }>,
    signal?: AbortSignal,
  ): Promise<GmailSyncResult> {
    const connection = await this.#ownedConnection(work);
    const state = stateFromConnection(connection, this.#now());
    if (connection.status === "revoked" || state.phase === "revoked") {
      return result(connection, state, "revoked", EMPTY_COUNTS);
    }
    if (connection.status === "reauth_required" || state.phase === "reauth_required") {
      return result(connection, state, "reauth_required", EMPTY_COUNTS);
    }
    if (connection.status !== "active") {
      throw new GoogleSyncError("Google connection is not active", "not_authorized", false);
    }
    if (state.phase === "cancelled") return result(connection, state, "cancelled", EMPTY_COUNTS);
    if (state.phase !== "live") return this.#processScanPage(connection, state, signal);
    if (state.history.pageToken !== null || state.history.targetId !== null) {
      if (state.history.cursorId === null) {
        return this.#processScanPage(connection, restartRecentScan(state, this.#now().toISOString()), signal);
      }
      return this.#processHistoryPage(connection, state, this.#now().toISOString(), signal);
    }
    return result(connection, state, "noop", EMPTY_COUNTS);
  }

  async #renewWatch(
    work: Extract<GmailSyncWork, { kind: "renew_watch" }>,
    signal?: AbortSignal,
  ): Promise<GmailSyncResult> {
    const connection = await this.#ownedActiveConnection(work);
    const state = stateFromConnection(connection, this.#now());
    const watch = await this.#withCredentials(connection, signal, (accessToken) =>
      this.#gmail.startWatch({ accessToken, topicName: this.#gmailTopicName }),
    );
    const saved = await this.#saveState(connection, {
      ...state,
      history: {
        ...state.history,
        cursorId: state.history.cursorId ?? watch.historyId,
      },
      watch: {
        historyId: watch.historyId,
        expiresAt: watch.expiresAt,
        subscription: this.#gmailPubSubSubscription,
      },
    });
    return result(
      connection,
      saved,
      saved.phase === "live" ? "processed" : "continuation_required",
      EMPTY_COUNTS,
    );
  }

  async #cancel(work: Extract<GmailSyncWork, { kind: "cancel" }>): Promise<GmailSyncResult> {
    const connection = await this.#ownedConnection(work);
    const state = stateFromConnection(connection, this.#now());
    if (connection.status === "revoked" || state.phase === "revoked") {
      return result(connection, state, "revoked", EMPTY_COUNTS);
    }
    if (state.phase === "cancelled") return result(connection, state, "cancelled", EMPTY_COUNTS);
    const saved = await this.#saveState(connection, {
      ...state,
      phase: "cancelled",
      scanPageToken: null,
      history: { ...state.history, startId: null, pageToken: null, targetId: null },
      cancellation: {
        requestedAt: this.#now().toISOString(),
        requestedByAdultId: work.adultId,
      },
    });
    return result(connection, saved, "cancelled", EMPTY_COUNTS);
  }

  async #revoke(
    work: Extract<GmailSyncWork, { kind: "revoke" }>,
    signal?: AbortSignal,
  ): Promise<GmailSyncResult> {
    const connection = await this.#ownedConnection(work);
    const state = stateFromConnection(connection, this.#now());
    if (connection.status === "revoked")
      return result(connection, { ...state, phase: "revoked" }, "revoked", EMPTY_COUNTS);

    if (connection.encryptedCredentials !== null) {
      try {
        const tokens = decryptCredentials(connection, this.#secretBox);
        assertNotAborted(signal);
        await this.#gmail.stopWatch(tokens.accessToken).catch(() => undefined);
        assertNotAborted(signal);
        await this.#oauth.revoke(tokens).catch(() => undefined);
      } catch {
        // Local revocation below is authoritative even when Google is unavailable.
      }
      const revoked = await this.#repository.revokeConnection({
        householdId: connection.householdId,
        adultId: connection.adultId,
        connectionId: connection.id,
        revokedAt: this.#now().toISOString(),
      });
      if (revoked === "not_found") {
        throw new GoogleSyncError("Google connection revocation was not scoped", "not_authorized", false);
      }
    } else {
      const revoked = await this.#repository.revokeConnection({
        householdId: connection.householdId,
        adultId: connection.adultId,
        connectionId: connection.id,
        revokedAt: this.#now().toISOString(),
      });
      if (revoked === "not_found") {
        throw new GoogleSyncError("Google connection revocation was not scoped", "not_authorized", false);
      }
    }
    return result(connection, { ...state, phase: "revoked" }, "revoked", EMPTY_COUNTS);
  }

  async #processScanPage(
    connection: GoogleSyncConnection,
    state: GmailSyncState,
    signal?: AbortSignal,
  ): Promise<GmailSyncResult> {
    if (!isScanPhase(state.phase)) {
      throw new GoogleSyncError("Gmail scan cursor is invalid", "invalid_state", false);
    }
    const query = queryForPhase(state.phase, state.boundaryAt);
    const page = await this.#withCredentials(connection, signal, (accessToken) =>
      this.#gmail.listMessageIdsPage({
        accessToken,
        maxResults: this.#pageSize,
        query,
        includeSpamTrash: false,
        ...(state.scanPageToken ? { pageToken: state.scanPageToken } : {}),
      }),
    );
    let processedMessages = 0;
    for (const item of page.messages) {
      assertNotAborted(signal);
      await this.#fetchPersistAndProcess(
        connection,
        item.messageId,
        {
          mode: state.phase,
          providerEventIds: [],
        },
        signal,
      );
      processedMessages += 1;
    }

    const nextState = page.nextPageToken
      ? { ...state, scanPageToken: page.nextPageToken }
      : advanceScanPhase(state, this.#now().toISOString());
    const saved = await this.#saveState(connection, nextState);
    const historyCatchUpRequired =
      saved.phase === "live" &&
      saved.history.cursorId !== null &&
      saved.history.targetId !== null &&
      compareHistoryIds(saved.history.targetId, saved.history.cursorId) > 0;
    const status = saved.phase === "live" && !historyCatchUpRequired ? "processed" : "continuation_required";
    return result(connection, saved, status, { processedMessages, processedDeletions: 0 });
  }

  async #processHistoryPage(
    connection: GoogleSyncConnection,
    state: GmailSyncState,
    deletionOccurredAt: string,
    signal?: AbortSignal,
  ): Promise<GmailSyncResult> {
    const startHistoryId = state.history.startId ?? state.history.cursorId;
    if (startHistoryId === null) {
      throw new GoogleSyncError("Gmail history cursor is missing", "invalid_state", false);
    }

    try {
      const page = await this.#withCredentials(connection, signal, (accessToken) =>
        this.#gmail.listHistoryPage({
          accessToken,
          googleSubject: connection.externalAccountId,
          startHistoryId,
          maxResults: this.#pageSize,
          ...(state.history.pageToken ? { pageToken: state.history.pageToken } : {}),
        }),
      );
      const decisions = finalHistoryDecisions(page.changes);
      let processedMessages = 0;
      let processedDeletions = 0;
      for (const decision of decisions) {
        assertNotAborted(signal);
        if (decision.deleted) {
          await this.#persistDeletion(
            connection,
            decision.messageId,
            decision.historyId,
            decision.providerEventIds,
            deletionOccurredAt,
          );
          processedDeletions += 1;
        } else {
          await this.#fetchPersistAndProcess(
            connection,
            decision.messageId,
            {
              mode: "history",
              historyId: decision.historyId,
              providerEventIds: decision.providerEventIds,
            },
            signal,
          );
          processedMessages += 1;
        }
      }

      const nextHistory = page.nextPageToken
        ? {
            ...state.history,
            startId: startHistoryId,
            pageToken: page.nextPageToken,
          }
        : {
            cursorId: maxHistoryId(page.mailboxHistoryId, state.history.targetId),
            startId: null,
            pageToken: null,
            targetId: null,
          };
      const saved = await this.#saveState(connection, {
        ...state,
        history: nextHistory,
        lastSuccessfulSyncAt: this.#now().toISOString(),
      });
      return result(connection, saved, page.nextPageToken ? "continuation_required" : "processed", {
        processedMessages,
        processedDeletions,
      });
    } catch (error) {
      if (!(error instanceof GoogleSyncTokenExpiredError)) throw error;
      return this.#processScanPage(connection, restartRecentScan(state, this.#now().toISOString()), signal);
    }
  }

  async #fetchPersistAndProcess(
    connection: GoogleSyncConnection,
    messageId: string,
    discovery: {
      mode: "recent_90_days" | "one_year_backfill" | "full_history_backfill" | "history";
      historyId?: string;
      providerEventIds: readonly string[];
    },
    signal?: AbortSignal,
  ): Promise<void> {
    const message = await this.#withCredentials(connection, signal, (accessToken) =>
      this.#gmail.getMessage({
        accessToken,
        googleSubject: connection.externalAccountId,
        messageId,
      }),
    );
    if (message.googleSubject !== connection.externalAccountId || message.messageId !== messageId) {
      throw new GoogleSyncError("Gmail returned data outside the requested account", "not_authorized", false);
    }

    const serialized = canonicalJson(message);
    const persisted = await this.#repository.persistPersonalGmailSource({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      externalId: message.messageId,
      kind: "gmail_message",
      occurredAt: messageOccurredAt(message, this.#now()),
      contentHash: `sha256:${sha256(serialized)}`,
      encryptedContent: this.#secretBox.seal(
        serialized,
        gmailSourceContentAad(connection, message.messageId),
      ),
      metadata: sourceMetadata(connection, message, discovery),
    });

    const inboxItem = toGmailInboxItem(connection, message, persisted.revision, this.#now());
    const applicationResult = await this.#application.process(inboxItem);
    if (applicationResult.outcome.status !== "processed") {
      throw new GoogleSyncError(
        "Gmail item was rejected by the application boundary",
        "invalid_state",
        false,
      );
    }
  }

  async #persistDeletion(
    connection: GoogleSyncConnection,
    messageId: string,
    historyId: string,
    providerEventIds: readonly string[],
    occurredAt: string,
  ): Promise<void> {
    const tombstone = canonicalJson({
      schemaVersion: 1,
      source: "gmail",
      sourceScope: "personal",
      googleSubject: connection.externalAccountId,
      messageId,
      deleted: true,
      historyId,
    });
    await this.#repository.persistPersonalGmailSource({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      externalId: messageId,
      kind: "gmail_message_deleted",
      occurredAt,
      contentHash: `sha256:${sha256(tombstone)}`,
      encryptedContent: this.#secretBox.seal(tombstone, gmailSourceContentAad(connection, messageId)),
      metadata: {
        schemaVersion: 1,
        provider: "gmail",
        sourceScope: "personal",
        googleSubject: connection.externalAccountId,
        discoveryMode: "history",
        historyId,
        providerEventIds: [...providerEventIds],
        contentAadVersion: 1,
        deleted: true,
      },
    });
  }

  async #withCredentials<T>(
    connection: GoogleSyncConnection,
    signal: AbortSignal | undefined,
    operation: (accessToken: string) => Promise<T>,
  ): Promise<T> {
    let tokens = decryptCredentials(connection, this.#secretBox);
    assertGmailScope(connection, tokens);
    if (tokenNeedsRefresh(tokens, this.#now(), this.#refreshSkewMs)) {
      tokens = await this.#refreshCredentials(connection, tokens, signal);
    }
    assertNotAborted(signal);
    try {
      return await operation(tokens.accessToken);
    } catch (error) {
      if (!(error instanceof GoogleAdapterError) || error.code !== "unauthorized") throw error;
      tokens = await this.#refreshCredentials(connection, tokens, signal);
      assertNotAborted(signal);
      return operation(tokens.accessToken);
    }
  }

  async #refreshCredentials(
    connection: GoogleSyncConnection,
    current: GoogleTokenSet,
    signal?: AbortSignal,
  ): Promise<GoogleTokenSet> {
    assertNotAborted(signal);
    let refreshed: GoogleTokenSet;
    try {
      refreshed = await this.#oauth.refresh(current);
    } catch (error) {
      if (error instanceof GoogleAdapterError && error.code === "unauthorized") {
        await this.#repository.markConnectionStatus({
          householdId: connection.householdId,
          adultId: connection.adultId,
          connectionId: connection.id,
          status: "reauth_required",
        });
      }
      throw error;
    }
    assertGmailScope(connection, refreshed);
    const encryptedCredentials = this.#secretBox.seal(
      JSON.stringify(refreshed),
      googleConnectionCredentialsAad(connection),
    );
    const replaced = await this.#repository.replaceEncryptedCredentials({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      expectedCiphertext: connection.encryptedCredentials ?? "",
      encryptedCredentials,
      grantedScopes: refreshed.scope,
    });
    if (replaced !== "updated") {
      throw new GoogleSyncError(
        "Google credentials changed concurrently or became inactive",
        replaced === "conflict" ? "conflict" : "not_authorized",
        replaced === "conflict",
      );
    }
    connection.encryptedCredentials = encryptedCredentials;
    connection.grantedScopes = [...refreshed.scope];
    return refreshed;
  }

  async #saveState(connection: GoogleSyncConnection, rawState: GmailSyncState): Promise<GmailSyncState> {
    const state = gmailSyncStateSchema.parse({
      ...rawState,
      revision: rawState.revision + 1,
    });
    const saved = await this.#repository.saveGmailSyncState({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      expectedRevision: rawState.revision,
      state,
    });
    if (saved !== "updated") {
      throw new GoogleSyncError(
        "Gmail sync state changed concurrently or became inactive",
        saved === "conflict" ? "conflict" : "not_authorized",
        saved === "conflict",
      );
    }
    return state;
  }

  async #ownedActiveConnection(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
  }): Promise<GoogleSyncConnection> {
    const connection = await this.#ownedConnection(input);
    if (connection.status !== "active") {
      throw new GoogleSyncError("Google connection is not active", "not_authorized", false);
    }
    return connection;
  }

  async #ownedConnection(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
  }): Promise<GoogleSyncConnection> {
    const raw = await this.#directory.getOwnedGoogleConnection(input);
    if (raw === null) {
      throw new GoogleSyncError("Google connection is outside the authorized scope", "not_authorized", false);
    }
    const connection = requireValidConnection(raw);
    if (
      connection.id !== input.connectionId ||
      connection.householdId !== input.householdId ||
      connection.adultId !== input.adultId
    ) {
      throw new GoogleSyncError("Google connection ownership did not match", "not_authorized", false);
    }
    return connection;
  }
}

function initialState(now: Date): GmailSyncState {
  return {
    schemaVersion: 1,
    revision: 0,
    phase: "live",
    requestedDepth: "full_history",
    boundaryAt: now.toISOString(),
    scanPageToken: null,
    history: { cursorId: null, startId: null, pageToken: null, targetId: null },
    watch: null,
    lastSuccessfulSyncAt: null,
    cancellation: null,
  };
}

function stateFromConnection(connection: GoogleSyncConnection, now: Date): GmailSyncState {
  const raw = connection.cursor.gmail;
  if (raw === undefined) return initialState(now);
  const parsed = gmailSyncStateSchema.safeParse(raw);
  if (!parsed.success) {
    throw new GoogleSyncError("Stored Gmail sync state is invalid", "invalid_state", false);
  }
  return parsed.data;
}

function requireValidConnection(raw: GoogleSyncConnection | undefined): GoogleSyncConnection {
  const parsed = googleSyncConnectionSchema.safeParse(raw);
  if (!parsed.success) {
    throw new GoogleSyncError("Google connection record is invalid", "invalid_state", false);
  }
  if (parsed.data.metadata.credentialAadVersion !== 1) {
    throw new GoogleSyncError("Google credential encryption metadata is invalid", "invalid_state", false);
  }
  return parsed.data;
}

function decryptCredentials(
  connection: GoogleSyncConnection,
  secretBox: Pick<SecretBox, "open">,
): GoogleTokenSet {
  if (connection.encryptedCredentials === null) {
    throw new GoogleSyncError("Google credentials are unavailable", "not_authorized", false);
  }
  try {
    const tokens = googleTokenSetSchema.parse(
      JSON.parse(secretBox.open(connection.encryptedCredentials, googleConnectionCredentialsAad(connection))),
    );
    return tokens;
  } catch {
    throw new GoogleSyncError("Google credentials could not be authenticated", "invalid_state", false);
  }
}

export function googleConnectionCredentialsAad(
  connection: Pick<GoogleSyncConnection, "householdId" | "adultId" | "externalAccountId">,
): string {
  return `google-connection:${connection.householdId}:${connection.adultId}:${connection.externalAccountId}`;
}

export function gmailSourceContentAad(
  connection: Pick<GoogleSyncConnection, "householdId" | "adultId" | "id">,
  messageId: string,
): string {
  return `google-source:${connection.householdId}:${connection.adultId}:${connection.id}:gmail:${messageId}`;
}

function assertGmailScope(connection: GoogleSyncConnection, tokens: GoogleTokenSet): void {
  if (
    !connection.grantedScopes.includes(GOOGLE_GMAIL_READONLY_SCOPE) ||
    !tokens.scope.includes(GOOGLE_GMAIL_READONLY_SCOPE)
  ) {
    throw new GoogleSyncError("Google connection does not grant Gmail read access", "not_authorized", false);
  }
}

function tokenNeedsRefresh(tokens: GoogleTokenSet, now: Date, skewMs: number): boolean {
  if (tokens.expiresAt === null) return false;
  const expiresAt = new Date(tokens.expiresAt).getTime();
  return !Number.isFinite(expiresAt) || expiresAt <= now.getTime() + skewMs;
}

function isScanPhase(
  phase: GmailSyncPhase,
): phase is Extract<GmailSyncPhase, "recent_90_days" | "one_year_backfill" | "full_history_backfill"> {
  return ["recent_90_days", "one_year_backfill", "full_history_backfill"].includes(phase);
}

function queryForPhase(
  phase: Extract<GmailSyncPhase, "recent_90_days" | "one_year_backfill" | "full_history_backfill">,
  boundaryAt: string,
): string {
  const boundary = new Date(boundaryAt).getTime();
  const ninetyDaysAgo = Math.floor((boundary - 90 * DAY_MS) / 1_000);
  const oneYearAgo = Math.floor((boundary - 365 * DAY_MS) / 1_000);
  switch (phase) {
    case "recent_90_days":
      return `after:${ninetyDaysAgo}`;
    case "one_year_backfill":
      return `after:${oneYearAgo} before:${ninetyDaysAgo}`;
    case "full_history_backfill":
      return `before:${oneYearAgo}`;
  }
}

function advanceScanPhase(state: GmailSyncState, completedAt: string): GmailSyncState {
  if (state.phase === "recent_90_days" && state.requestedDepth !== "recent_90_days") {
    return { ...state, phase: "one_year_backfill", scanPageToken: null };
  }
  if (state.phase === "one_year_backfill" && state.requestedDepth === "full_history") {
    return { ...state, phase: "full_history_backfill", scanPageToken: null };
  }
  return {
    ...state,
    phase: "live",
    scanPageToken: null,
    history: {
      cursorId: state.history.cursorId ?? state.watch?.historyId ?? null,
      startId: null,
      pageToken: null,
      targetId:
        state.history.cursorId !== null &&
        state.history.targetId !== null &&
        compareHistoryIds(state.history.targetId, state.history.cursorId) <= 0
          ? null
          : state.history.targetId,
    },
    lastSuccessfulSyncAt: completedAt,
  };
}

function restartRecentScan(state: GmailSyncState, boundaryAt: string): GmailSyncState {
  const rebasedCursor = state.history.targetId ?? state.watch?.historyId ?? state.history.cursorId;
  return {
    ...state,
    phase: "recent_90_days",
    requestedDepth: "recent_90_days",
    boundaryAt,
    scanPageToken: null,
    history: { cursorId: rebasedCursor, startId: null, pageToken: null, targetId: null },
  };
}

function finalHistoryDecisions(changes: readonly GmailHistoryChange[]): Array<{
  messageId: string;
  historyId: string;
  deleted: boolean;
  providerEventIds: string[];
}> {
  const decisions = new Map<
    string,
    { messageId: string; historyId: string; deleted: boolean; providerEventIds: string[] }
  >();
  const ordered = [...changes].sort((left, right) => compareHistoryIds(left.historyId, right.historyId));
  for (const change of ordered) {
    const existing = decisions.get(change.messageId);
    decisions.set(change.messageId, {
      messageId: change.messageId,
      historyId: change.historyId,
      deleted: change.changeType === "message.deleted",
      providerEventIds: [...(existing?.providerEventIds ?? []), change.providerEventId],
    });
  }
  return [...decisions.values()];
}

function sourceMetadata(
  connection: GoogleSyncConnection,
  message: GmailMessage,
  discovery: {
    mode: string;
    historyId?: string;
    providerEventIds: readonly string[];
  },
): Record<string, unknown> {
  return {
    schemaVersion: 1,
    provider: "gmail",
    sourceScope: "personal",
    googleSubject: connection.externalAccountId,
    threadId: message.threadId,
    messageHistoryId: message.historyId,
    discoveryMode: discovery.mode,
    ...(discovery.historyId ? { discoveryHistoryId: discovery.historyId } : {}),
    providerEventIds: [...discovery.providerEventIds],
    contentAadVersion: 1,
  };
}

function toGmailInboxItem(
  connection: GoogleSyncConnection,
  message: GmailMessage,
  revision: number,
  now: Date,
): GmailInboxItem {
  const bodyText =
    message.body.text ?? (message.body.html ? plainTextFromHtml(message.body.html) : undefined);
  return GmailInboxItemSchema.parse({
    kind: "gmail_message",
    householdId: connection.householdId,
    idempotencyKey: `gmail:${connection.id}:${message.messageId}:revision:${revision}`,
    occurredAt: messageOccurredAt(message, now),
    ownerAdultId: connection.adultId,
    accountRef: `google:${connection.id}`,
    messageRef: `gmail:${connection.id}:${message.messageId}`,
    revision,
    labels: message.labelIds.slice(0, 100),
    ...(message.headers.from ? { sender: bounded(message.headers.from, 1_000) } : {}),
    ...(message.headers.subject ? { subject: bounded(message.headers.subject, 2_000) } : {}),
    ...(message.snippet ? { snippet: bounded(message.snippet, 10_000) } : {}),
    ...(bodyText ? { bodyText: bounded(bodyText, 1_000_000) } : {}),
    attachmentRefs: message.attachments
      .slice(0, 100)
      .map((attachment) =>
        bounded(
          `gmail:${connection.id}:${message.messageId}:attachment:${attachment.providerAttachmentId ?? attachment.partId ?? sha256(attachment.filename).slice(0, 24)}`,
          500,
        ),
      ),
  });
}

function messageOccurredAt(message: GmailMessage, now: Date): string {
  if (message.internalDate !== null) return message.internalDate;
  if (message.headers.date !== null) {
    const date = new Date(message.headers.date);
    if (Number.isFinite(date.getTime())) return date.toISOString();
  }
  return now.toISOString();
}

function plainTextFromHtml(html: string): string {
  return html
    .replace(/<(script|style)\b[^>]*>[\s\S]*?<\/\1>/giu, " ")
    .replace(/<br\s*\/?>/giu, "\n")
    .replace(/<\/p\s*>/giu, "\n")
    .replace(/<[^>]+>/gu, " ")
    .replace(/&nbsp;/giu, " ")
    .replace(/&amp;/giu, "&")
    .replace(/&lt;/giu, "<")
    .replace(/&gt;/giu, ">")
    .replace(/&quot;/giu, '"')
    .replace(/&#39;/giu, "'")
    .replace(/[ \t]+/gu, " ")
    .replace(/\n{3,}/gu, "\n\n")
    .trim();
}

function result(
  connection: GoogleSyncConnection,
  state: GmailSyncState,
  status: GmailSyncResult["status"],
  counts: ProcessCounts,
): GmailSyncResult {
  return {
    status,
    connectionId: connection.id,
    householdId: connection.householdId,
    adultId: connection.adultId,
    phase: state.phase,
    ...counts,
  };
}

function normalizeEmail(email: string | null): string {
  return email?.trim().toLowerCase() ?? "";
}

function compareHistoryIds(left: string, right: string): number {
  const leftValue = BigInt(left);
  const rightValue = BigInt(right);
  return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
}

function maxHistoryId(left: string | null, right: string | null): string | null {
  if (left === null) return right;
  if (right === null) return left;
  return compareHistoryIds(left, right) >= 0 ? left : right;
}

function bounded(value: string, maxLength: number): string {
  return value.length <= maxLength ? value : value.slice(0, maxLength);
}

function assertNotAborted(signal?: AbortSignal): void {
  if (signal?.aborted) throw signal.reason ?? new Error("aborted");
}

function isAbort(error: unknown, signal?: AbortSignal): boolean {
  return signal?.aborted === true || (error instanceof Error && error.name === "AbortError");
}

/** Calendar follows the same owned-connection and cursor-CAS boundary without sharing Gmail state. */
export interface GoogleCalendarSyncPort {
  synchronize(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    calendarId: string;
    reason: "initial" | "push" | "scheduled" | "manual";
    signal?: AbortSignal;
  }): Promise<{
    status: "processed" | "continuation_required" | "noop" | "reauth_required";
    nextSyncToken: string | null;
  }>;
}
