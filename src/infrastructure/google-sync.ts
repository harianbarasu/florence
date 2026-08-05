import { z } from "zod";
import {
  type GmailAdapter,
  type GmailAttachment,
  GmailAttachmentContentError,
  type GmailHistoryChange,
  type GmailMessage,
  type GmailMessageFormat,
  type GmailRetrievedAttachment,
  GOOGLE_GMAIL_READONLY_SCOPE,
  GoogleAdapterError,
  type GoogleOAuthAdapter,
  GoogleSyncTokenExpiredError,
  type GoogleTokenSet,
  gmailAttachmentSchema,
  gmailMessageSchema,
  gmailPubSubEventSchema,
  googleTokenSetSchema,
  type RetrieveGmailAttachmentInput,
} from "../adapters/google/index.js";
import {
  type FlorenceApplication,
  type GmailAttachmentContent,
  GmailAttachmentContentSchema,
  type GmailInboxItem,
  GmailInboxItemSchema,
  type GmailMessageDeletedInboxItem,
  GmailMessageDeletedInboxItemSchema,
} from "../application/index.js";
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

export const GMAIL_DISCOVERY_MESSAGE_COUNT_MAX = 1_000_000;
const GMAIL_RECOVERY_PAGE_SIZE = 100;
const GMAIL_LIVE_WINDOW_MS = 24 * 60 * 60_000;
const GMAIL_LIVE_FUTURE_TOLERANCE_MS = 5 * 60_000;
const GMAIL_MAX_ATTACHMENT_COUNT = 20;
const GMAIL_MAX_ATTACHMENT_BYTES = 10 * 1024 * 1024;
const GMAIL_MAX_TOTAL_ATTACHMENT_BYTES = 15 * 1024 * 1024;

const storedGmailAttachmentSchema = gmailAttachmentSchema.extend({
  embeddedDataBase64Url: z.null(),
});

const storedGmailMessageSchema = gmailMessageSchema.extend({
  attachments: z.array(storedGmailAttachmentSchema),
});

export const gmailStoredSourceEnvelopeSchema = z.discriminatedUnion("contentCompleteness", [
  z.strictObject({
    schemaVersion: z.literal(2),
    contentCompleteness: z.literal("metadata"),
    message: storedGmailMessageSchema,
  }),
  z.strictObject({
    schemaVersion: z.literal(2),
    contentCompleteness: z.literal("full"),
    message: storedGmailMessageSchema,
    attachmentContents: z.array(GmailAttachmentContentSchema).max(GMAIL_MAX_ATTACHMENT_COUNT),
  }),
]);

export type GmailStoredSourceEnvelope = z.infer<typeof gmailStoredSourceEnvelopeSchema>;

export const gmailSourceMetadataSchema = z.strictObject({
  schemaVersion: z.literal(2),
  provider: z.literal("gmail"),
  sourceScope: z.literal("personal"),
  contentCompleteness: z.enum(["metadata", "full"]),
  googleSubject: z.string().min(1).max(500).optional(),
  threadId: z.string().min(1).max(1_000).optional(),
  messageHistoryId: z.string().regex(/^\d+$/).max(100).nullable().optional(),
  discoveryMode: z
    .enum(["recent_90_days", "one_year_backfill", "full_history_backfill", "history", "recovery"])
    .optional(),
  discoveryHistoryId: z.string().min(1).max(100).optional(),
  historyId: z.string().min(1).max(100).optional(),
  providerEventIds: z.array(z.string().min(1).max(1_000)).max(1_000).optional(),
  contentAadVersion: z.literal(1).optional(),
  deleted: z.literal(true).optional(),
});

export type GmailSourceMetadata = z.infer<typeof gmailSourceMetadataSchema>;

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

const gmailRecoveryStateSchema = z.strictObject({
  generationId: z.uuid(),
  snapshotComplete: z.boolean(),
});

export const gmailDiscoveryRunSchema = z.strictObject({
  runId: z.string().min(1).max(500),
  messageCount: z.number().int().nonnegative().max(GMAIL_DISCOVERY_MESSAGE_COUNT_MAX),
  status: z.enum(["collecting", "pending", "published"]),
});

export type GmailDiscoveryRun = z.infer<typeof gmailDiscoveryRunSchema>;

export const gmailSyncStateSchema = z.strictObject({
  schemaVersion: z.literal(2),
  revision: z.number().int().nonnegative(),
  phase: gmailSyncPhaseSchema,
  requestedDepth: gmailSyncDepthSchema,
  boundaryAt: instantSchema,
  scanPageToken: z.string().min(1).nullable(),
  scanProcessedMessageIds: z
    .array(z.string().min(1).max(500))
    .max(500)
    .refine((ids) => new Set(ids).size === ids.length, "Processed Gmail message IDs must be unique"),
  history: gmailHistoryCursorSchema,
  watch: gmailWatchStateSchema.nullable(),
  recovery: gmailRecoveryStateSchema.nullable(),
  lastSuccessfulSyncAt: instantSchema.nullable(),
  discovery: gmailDiscoveryRunSchema.nullable(),
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
  metadata: GmailSourceMetadata;
}

export type PersistPersonalGmailSourceResult = {
  sourceItemId: string;
  disposition: "inserted" | "unchanged" | "revised";
  revision: number;
  retainedExisting?: "full" | "deleted" | "stale";
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

  beginGmailRecovery(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    targetHistoryId: string;
  }): Promise<{ generationId: string }>;

  recordGmailRecoveryPage(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    generationId: string;
    externalIds: readonly string[];
  }): Promise<{ pendingExternalIds: readonly string[] }>;

  markGmailRecoveryWorkProcessed(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    generationId: string;
    externalId: string;
  }): Promise<void>;

  listMissingGmailRecoverySources(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    generationId: string;
    limit: number;
  }): Promise<readonly string[]>;

  finishGmailRecovery(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    generationId: string;
    expectedRevision: number;
    state: GmailSyncState;
  }): Promise<ScopedMutationResult>;

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

export interface PublishGmailDiscoveryCompletionInput {
  householdId: string;
  adultId: string;
  connectionId: string;
  expectedRevision: number;
  state: GmailSyncState;
}

/** Publishes the one private discovery status atomically with its cursor transition. */
export interface GmailDiscoveryCompletionPort {
  publish(input: PublishGmailDiscoveryCompletionInput): Promise<ScopedMutationResult>;
}

export interface GmailProviderPort {
  getMessage(input: {
    accessToken: string;
    googleSubject: string;
    messageId: string;
    format: GmailMessageFormat;
  }): ReturnType<GmailAdapter["getMessage"]>;
  retrieveAttachment(input: RetrieveGmailAttachmentInput): Promise<GmailRetrievedAttachment>;
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
  completionDigest: GmailDiscoveryCompletionPort;
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
  readonly #completionDigest: GmailDiscoveryCompletionPort;
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
    this.#completionDigest = options.completionDigest;
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
      state.history.pageToken === null &&
      state.history.cursorId !== null &&
      targetId !== null &&
      compareHistoryIds(targetId, state.history.cursorId) <= 0
    ) {
      if (isScanPhase(state.phase)) {
        return result(connection, state, "continuation_required", EMPTY_COUNTS);
      }
      if (state.discovery?.status === "pending") {
        const published = await this.#publishPendingDiscovery(connection, state);
        return result(connection, published, "processed", EMPTY_COUNTS);
      }
      return result(connection, state, "noop", EMPTY_COUNTS);
    }

    const targetedState: GmailSyncState = {
      ...state,
      history: { ...state.history, targetId },
    };
    if (targetedState.history.cursorId === null) {
      return this.#recoverExpiredHistoryCursor(connection, targetedState, signal);
    }
    return this.#processHistoryPage(connection, targetedState, event.publishedAt, signal);
  }

  async #start(
    work: Extract<GmailSyncWork, { kind: "start" }>,
    signal?: AbortSignal,
  ): Promise<GmailSyncResult> {
    const connection = await this.#ownedActiveConnection(work);
    const current = stateFromConnection(connection, this.#now());
    if (current.discovery?.status === "pending") {
      const published = await this.#publishPendingDiscovery(connection, current);
      return result(connection, published, "processed", EMPTY_COUNTS);
    }
    if (current.discovery?.status === "collecting" || isScanPhase(current.phase)) {
      return result(connection, current, "continuation_required", EMPTY_COUNTS);
    }
    if (current.discovery?.status === "published") {
      return result(connection, current, "noop", EMPTY_COUNTS);
    }
    const boundaryAt = this.#now().toISOString();
    const watch = await this.#withCredentials(connection, signal, (accessToken) =>
      this.#gmail.startWatch({ accessToken, topicName: this.#gmailTopicName }),
    );
    const state = gmailSyncStateSchema.parse({
      ...current,
      phase: "recent_90_days",
      requestedDepth: work.depth,
      boundaryAt,
      scanPageToken: null,
      scanProcessedMessageIds: [],
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
      recovery: null,
      discovery: {
        runId: gmailDiscoveryRunId(connection.id, boundaryAt),
        messageCount: 0,
        status: "collecting",
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
    if (hasPendingHistory(state)) {
      if (state.history.cursorId === null) {
        return this.#recoverExpiredHistoryCursor(connection, state, signal);
      }
      return this.#processHistoryPage(connection, state, this.#now().toISOString(), signal);
    }
    if (state.discovery?.status === "pending") {
      const published = await this.#publishPendingDiscovery(connection, state);
      return result(connection, published, "processed", EMPTY_COUNTS);
    }
    if (isScanPhase(state.phase)) return this.#processScanPage(connection, state, signal);
    if (state.phase !== "live") {
      throw new GoogleSyncError("Gmail sync phase cannot continue", "invalid_state", false);
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
    const recoveringHistory = isRecoveringExpiredHistory(state);
    const nextCursorId = recoveringHistory ? null : (state.history.cursorId ?? watch.historyId);
    const observedTargetId = maxHistoryId(state.history.targetId, watch.historyId);
    const nextTargetId = recoveringHistory
      ? observedTargetId
      : nextCursorId !== null &&
          observedTargetId !== null &&
          compareHistoryIds(observedTargetId, nextCursorId) > 0
        ? observedTargetId
        : null;
    let saved = await this.#saveState(connection, {
      ...state,
      history: {
        ...state.history,
        cursorId: nextCursorId,
        targetId: nextTargetId,
      },
      watch: {
        historyId: watch.historyId,
        expiresAt: watch.expiresAt,
        subscription: this.#gmailPubSubSubscription,
      },
    });
    if (saved.discovery?.status === "pending" && !hasPendingHistory(saved)) {
      saved = await this.#publishPendingDiscovery(connection, saved);
    }
    return result(
      connection,
      saved,
      requiresContinuation(saved) ? "continuation_required" : "processed",
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
    const cancelled = {
      ...state,
      phase: "cancelled",
      scanPageToken: null,
      scanProcessedMessageIds: [],
      history: { ...state.history, startId: null, pageToken: null, targetId: null },
      recovery: null,
      discovery: null,
      cancellation: {
        requestedAt: this.#now().toISOString(),
        requestedByAdultId: work.adultId,
      },
    } satisfies GmailSyncState;
    const saved =
      state.recovery === null
        ? await this.#saveState(connection, cancelled)
        : await this.#finishRecovery(connection, cancelled, state.recovery.generationId);
    return result(connection, saved, "cancelled", EMPTY_COUNTS);
  }

  async #revoke(
    work: Extract<GmailSyncWork, { kind: "revoke" }>,
    signal?: AbortSignal,
  ): Promise<GmailSyncResult> {
    const connection = await this.#ownedConnection(work);
    const state = stateFromConnection(connection, this.#now());
    if (connection.status === "revoked")
      return result(
        connection,
        {
          ...state,
          phase: "revoked",
          scanProcessedMessageIds: [],
          recovery: null,
          discovery: null,
        },
        "revoked",
        EMPTY_COUNTS,
      );

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
    return result(
      connection,
      {
        ...state,
        phase: "revoked",
        scanProcessedMessageIds: [],
        recovery: null,
        discovery: null,
      },
      "revoked",
      EMPTY_COUNTS,
    );
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
    let workingState = state;
    const alreadyProcessed = new Set(state.scanProcessedMessageIds);
    for (const item of page.messages) {
      assertNotAborted(signal);
      if (alreadyProcessed.has(item.messageId)) continue;
      await this.#fetchPersistAndProcessPrivate(
        connection,
        item.messageId,
        {
          mode: state.phase,
          providerEventIds: [],
        },
        signal,
      );
      processedMessages += 1;
      alreadyProcessed.add(item.messageId);
      workingState = await this.#saveState(connection, {
        ...workingState,
        scanProcessedMessageIds: [...alreadyProcessed],
        discovery:
          workingState.discovery?.status === "collecting"
            ? {
                ...workingState.discovery,
                messageCount: saturatingDiscoveryCount(workingState.discovery.messageCount, 1),
              }
            : workingState.discovery,
      });
    }

    const nextState = page.nextPageToken
      ? { ...workingState, scanPageToken: page.nextPageToken, scanProcessedMessageIds: [] }
      : advanceScanPhase(workingState, this.#now().toISOString());
    let saved = await this.#saveState(connection, nextState);
    if (saved.discovery?.status === "pending" && !hasPendingHistory(saved)) {
      saved = await this.#publishPendingDiscovery(connection, saved);
    }
    const status = requiresContinuation(saved) ? "continuation_required" : "processed";
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
      assertOwnedGmailHistoryChanges(connection, page.changes);
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
          const discovery = {
            mode: "history" as const,
            historyId: decision.historyId,
            providerEventIds: decision.providerEventIds,
          };
          await this.#fetchPersistAndMaybeProcessLive(connection, decision.messageId, discovery, signal);
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
        lastSuccessfulSyncAt: page.nextPageToken ? state.lastSuccessfulSyncAt : this.#now().toISOString(),
      });
      return result(connection, saved, requiresContinuation(saved) ? "continuation_required" : "processed", {
        processedMessages,
        processedDeletions,
      });
    } catch (error) {
      if (!(error instanceof GoogleSyncTokenExpiredError)) throw error;
      return this.#recoverExpiredHistoryCursor(connection, state, signal);
    }
  }

  async #recoverExpiredHistoryCursor(
    connection: GoogleSyncConnection,
    state: GmailSyncState,
    signal?: AbortSignal,
  ): Promise<GmailSyncResult> {
    const now = this.#now();
    const continuingRecovery = isRecoveringExpiredHistory(state) && state.recovery !== null;
    const recoveryBaseId = isRecoveringExpiredHistory(state)
      ? state.history.startId
      : maxHistoryId(
          maxHistoryId(state.history.targetId, state.watch?.historyId ?? null),
          state.history.cursorId,
        );
    if (recoveryBaseId === null) {
      throw new GoogleSyncError("Gmail recovery target is missing", "invalid_state", false);
    }

    if (!continuingRecovery) {
      const run = await this.#repository.beginGmailRecovery({
        householdId: connection.householdId,
        adultId: connection.adultId,
        connectionId: connection.id,
        targetHistoryId: recoveryBaseId,
      });
      const saved = await this.#saveState(connection, {
        ...state,
        history: {
          cursorId: null,
          startId: recoveryBaseId,
          pageToken: null,
          targetId: maxHistoryId(state.history.targetId, recoveryBaseId),
        },
        recovery: { generationId: run.generationId, snapshotComplete: false },
      });
      return result(connection, saved, "continuation_required", EMPTY_COUNTS);
    }

    const recovery = state.recovery;
    if (recovery === null) {
      throw new GoogleSyncError("Gmail recovery state is missing", "invalid_state", false);
    }
    if (recovery.snapshotComplete) {
      const pendingMissing = await this.#repository.listMissingGmailRecoverySources({
        householdId: connection.householdId,
        adultId: connection.adultId,
        connectionId: connection.id,
        generationId: recovery.generationId,
        limit: GMAIL_RECOVERY_PAGE_SIZE,
      });
      let processedMessages = 0;
      let processedDeletions = 0;
      for (const messageId of pendingMissing) {
        assertNotAborted(signal);
        const disposition = await this.#processRecoveryMessage(connection, messageId, recoveryBaseId, signal);
        if (disposition === "message") {
          processedMessages += 1;
        } else {
          processedDeletions += 1;
        }
        await this.#repository.markGmailRecoveryWorkProcessed({
          householdId: connection.householdId,
          adultId: connection.adultId,
          connectionId: connection.id,
          generationId: recovery.generationId,
          externalId: messageId,
        });
      }
      if (pendingMissing.length > 0) {
        const saved = await this.#saveState(connection, state);
        return result(connection, saved, "continuation_required", {
          processedMessages,
          processedDeletions,
        });
      }

      const latestTargetId = maxHistoryId(state.history.targetId, recoveryBaseId);
      const saved = await this.#finishRecovery(
        connection,
        {
          ...state,
          history: {
            cursorId: recoveryBaseId,
            startId: null,
            pageToken: null,
            targetId:
              latestTargetId !== null && compareHistoryIds(latestTargetId, recoveryBaseId) > 0
                ? latestTargetId
                : null,
          },
          recovery: null,
          lastSuccessfulSyncAt: now.toISOString(),
        },
        recovery.generationId,
      );
      return result(
        connection,
        saved,
        requiresContinuation(saved) ? "continuation_required" : "processed",
        EMPTY_COUNTS,
      );
    }

    const page = await this.#withCredentials(connection, signal, (accessToken) =>
      this.#gmail.listMessageIdsPage({
        accessToken,
        maxResults: GMAIL_RECOVERY_PAGE_SIZE,
        includeSpamTrash: true,
        ...(state.history.pageToken ? { pageToken: state.history.pageToken } : {}),
      }),
    );
    if (page.nextPageToken !== null && page.nextPageToken === state.history.pageToken) {
      throw new GoogleSyncError("Gmail recovery pagination did not advance", "provider_failure", true);
    }
    const recorded = await this.#repository.recordGmailRecoveryPage({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      generationId: recovery.generationId,
      externalIds: page.messages.map((item) => item.messageId),
    });
    let processedMessages = 0;
    let processedDeletions = 0;
    for (const messageId of recorded.pendingExternalIds) {
      assertNotAborted(signal);
      const disposition = await this.#processRecoveryMessage(connection, messageId, recoveryBaseId, signal);
      await this.#repository.markGmailRecoveryWorkProcessed({
        householdId: connection.householdId,
        adultId: connection.adultId,
        connectionId: connection.id,
        generationId: recovery.generationId,
        externalId: messageId,
      });
      if (disposition === "message") processedMessages += 1;
      else processedDeletions += 1;
    }
    const saved = await this.#saveState(connection, {
      ...state,
      history: {
        cursorId: null,
        startId: recoveryBaseId,
        pageToken: page.nextPageToken,
        targetId: maxHistoryId(state.history.targetId, recoveryBaseId),
      },
      recovery: {
        ...recovery,
        snapshotComplete: page.nextPageToken === null,
      },
    });
    return result(connection, saved, "continuation_required", {
      processedMessages,
      processedDeletions,
    });
  }

  async #processRecoveryMessage(
    connection: GoogleSyncConnection,
    messageId: string,
    historyId: string,
    signal?: AbortSignal,
  ): Promise<"message" | "deletion"> {
    try {
      await this.#fetchPersistAndProcessPrivate(
        connection,
        messageId,
        { mode: "recovery", providerEventIds: [] },
        signal,
      );
      return "message";
    } catch (error) {
      if (!(error instanceof GoogleAdapterError) || error.code !== "not_found") throw error;
      await this.#persistDeletion(connection, messageId, historyId, [], this.#now().toISOString());
      return "deletion";
    }
  }

  async #fetchMetadataAndPersist(
    connection: GoogleSyncConnection,
    messageId: string,
    discovery: {
      mode: "recent_90_days" | "one_year_backfill" | "full_history_backfill" | "history" | "recovery";
      historyId?: string;
      providerEventIds: readonly string[];
    },
    signal?: AbortSignal,
  ): Promise<{ message: GmailMessage; persisted: PersistPersonalGmailSourceResult }> {
    const message = await this.#fetchMessage(connection, messageId, "metadata", signal);
    const persisted = await this.#persistMessage(connection, message, "metadata", [], discovery);
    return { message, persisted };
  }

  async #fetchPersistAndMaybeProcessLive(
    connection: GoogleSyncConnection,
    messageId: string,
    discovery: {
      mode: "history";
      historyId?: string;
      providerEventIds: readonly string[];
    },
    signal?: AbortSignal,
  ): Promise<{ message: GmailMessage; persisted: PersistPersonalGmailSourceResult }> {
    const metadata = await this.#fetchMetadataAndPersist(connection, messageId, discovery, signal);
    const eligibilityAt = this.#now();
    if (
      metadata.persisted.retainedExisting === "deleted" ||
      !isWithinLiveTriageWindow(metadata.message, eligibilityAt)
    ) {
      return metadata;
    }

    const full = await this.#fetchFullMessageWithAttachments(connection, messageId, signal);
    assertStableMessageIdentity(metadata.message, full.message);
    const persisted = await this.#persistMessage(
      connection,
      full.message,
      "full",
      full.attachmentContents,
      discovery,
    );
    if (persisted.retainedExisting === "deleted" || persisted.retainedExisting === "stale") {
      return { message: full.message, persisted };
    }
    await this.#processLiveMessage(connection, full.message, full.attachmentContents, persisted.revision);
    return { message: full.message, persisted };
  }

  async #fetchPersistAndProcessPrivate(
    connection: GoogleSyncConnection,
    messageId: string,
    discovery: {
      mode: "recent_90_days" | "one_year_backfill" | "full_history_backfill" | "recovery";
      historyId?: string;
      providerEventIds: readonly string[];
    },
    signal?: AbortSignal,
  ): Promise<{ message: GmailMessage; persisted: PersistPersonalGmailSourceResult }> {
    const metadata = await this.#fetchMetadataAndPersist(connection, messageId, discovery, signal);
    if (
      metadata.persisted.retainedExisting === "deleted" ||
      metadata.persisted.retainedExisting === "stale"
    ) {
      return metadata;
    }

    const full = await this.#fetchFullMessageWithAttachments(connection, messageId, signal);
    assertStableMessageIdentity(metadata.message, full.message);
    const persisted = await this.#persistMessage(
      connection,
      full.message,
      "full",
      full.attachmentContents,
      discovery,
    );
    if (persisted.retainedExisting === "deleted" || persisted.retainedExisting === "stale") {
      return { message: full.message, persisted };
    }
    await this.#processLiveMessage(connection, full.message, full.attachmentContents, persisted.revision);
    return { message: full.message, persisted };
  }

  async #fetchMessage(
    connection: GoogleSyncConnection,
    messageId: string,
    format: GmailMessageFormat,
    signal?: AbortSignal,
  ): Promise<GmailMessage> {
    const message = await this.#withCredentials(connection, signal, (accessToken) =>
      this.#gmail.getMessage({
        accessToken,
        googleSubject: connection.externalAccountId,
        messageId,
        format,
      }),
    );
    assertOwnedGmailMessage(connection, messageId, message);
    return message;
  }

  async #fetchFullMessageWithAttachments(
    connection: GoogleSyncConnection,
    messageId: string,
    signal?: AbortSignal,
  ): Promise<{ message: GmailMessage; attachmentContents: GmailAttachmentContent[] }> {
    return this.#withCredentials(connection, signal, async (accessToken) => {
      const message = await this.#gmail.getMessage({
        accessToken,
        googleSubject: connection.externalAccountId,
        messageId,
        format: "full",
      });
      assertOwnedGmailMessage(connection, messageId, message);
      const attachmentContents = await this.#retrieveAttachmentContents(
        connection,
        accessToken,
        message,
        signal,
      );
      return { message, attachmentContents };
    });
  }

  async #retrieveAttachmentContents(
    connection: GoogleSyncConnection,
    accessToken: string,
    message: GmailMessage,
    signal?: AbortSignal,
  ): Promise<GmailAttachmentContent[]> {
    const contents: GmailAttachmentContent[] = [];
    let remainingBytes = GMAIL_MAX_TOTAL_ATTACHMENT_BYTES;
    const attachments = message.attachments.slice(0, GMAIL_MAX_ATTACHMENT_COUNT);
    for (const [index, attachment] of attachments.entries()) {
      assertNotAborted(signal);
      const reference = gmailAttachmentReference(connection, message, attachment, index);
      const unavailableBase = safeAttachmentMetadata(reference, attachment);
      if (attachment.sizeBytes === 0) {
        contents.push(unavailableGmailAttachment(unavailableBase, "invalid_content"));
        continue;
      }
      if (attachment.sizeBytes > GMAIL_MAX_ATTACHMENT_BYTES || attachment.sizeBytes > remainingBytes) {
        contents.push(unavailableGmailAttachment(unavailableBase, "too_large"));
        continue;
      }

      try {
        const retrieved = await this.#gmail.retrieveAttachment({
          accessToken,
          messageId: message.messageId,
          attachment,
        });
        assertNotAborted(signal);
        if (retrieved.sizeBytes !== retrieved.bytes.byteLength || retrieved.sizeBytes === 0) {
          contents.push(unavailableGmailAttachment(unavailableBase, "invalid_content"));
          continue;
        }
        if (retrieved.sizeBytes > GMAIL_MAX_ATTACHMENT_BYTES || retrieved.sizeBytes > remainingBytes) {
          contents.push(unavailableGmailAttachment(unavailableBase, "too_large"));
          continue;
        }
        remainingBytes -= retrieved.sizeBytes;
        const bytes = Buffer.from(retrieved.bytes);
        contents.push({
          reference,
          kind: retrieved.kind,
          mediaType: retrieved.mediaType,
          filename: safeAttachmentFilename(retrieved.filename),
          sizeBytes: retrieved.sizeBytes,
          dataBase64: bytes.toString("base64"),
          contentDigest: `sha256:${sha256(bytes)}`,
        });
      } catch (error) {
        const reason = unavailableReasonForAttachmentError(error);
        if (reason !== null) {
          contents.push(unavailableGmailAttachment(unavailableBase, reason));
          continue;
        }
        throw error;
      }
    }
    return contents;
  }

  async #persistMessage(
    connection: GoogleSyncConnection,
    message: GmailMessage,
    contentCompleteness: "metadata" | "full",
    attachmentContents: readonly GmailAttachmentContent[],
    discovery: {
      mode: "recent_90_days" | "one_year_backfill" | "full_history_backfill" | "history" | "recovery";
      historyId?: string;
      providerEventIds: readonly string[];
    },
  ): Promise<PersistPersonalGmailSourceResult> {
    const envelope = gmailSourceEnvelope(message, contentCompleteness, attachmentContents);
    const serialized = canonicalJson(envelope);
    return this.#repository.persistPersonalGmailSource({
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
      metadata: sourceMetadata(connection, message, discovery, contentCompleteness),
    });
  }

  async #processLiveMessage(
    connection: GoogleSyncConnection,
    message: GmailMessage,
    attachmentContents: readonly GmailAttachmentContent[],
    revision: number,
  ): Promise<void> {
    const inboxItem = toGmailInboxItem(connection, message, attachmentContents, revision, this.#now());
    const applicationResult = await this.#application.process(inboxItem);
    if (applicationResult.outcome.status !== "processed") {
      throw new GoogleSyncError(
        "Gmail item was rejected by the application boundary",
        "invalid_state",
        false,
      );
    }
  }

  async #publishPendingDiscovery(
    connection: GoogleSyncConnection,
    state: GmailSyncState,
  ): Promise<GmailSyncState> {
    if (state.discovery?.status !== "pending") {
      throw new GoogleSyncError("Gmail discovery completion is not pending", "invalid_state", false);
    }
    const published = gmailSyncStateSchema.parse({
      ...state,
      revision: state.revision + 1,
      discovery: { ...state.discovery, status: "published" },
    });
    const outcome = await this.#completionDigest.publish({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      expectedRevision: state.revision,
      state: published,
    });
    if (outcome !== "updated") {
      throw new GoogleSyncError(
        "Gmail discovery completion changed concurrently or became inactive",
        outcome === "conflict" ? "conflict" : "not_authorized",
        outcome === "conflict",
      );
    }
    return published;
  }

  async #persistDeletion(
    connection: GoogleSyncConnection,
    messageId: string,
    historyId: string,
    providerEventIds: readonly string[],
    occurredAt: string,
  ): Promise<void> {
    const tombstone = canonicalJson({
      schemaVersion: 2,
      source: "gmail",
      sourceScope: "personal",
      contentCompleteness: "metadata",
      googleSubject: connection.externalAccountId,
      messageId,
      deleted: true,
      historyId,
    });
    const persisted = await this.#repository.persistPersonalGmailSource({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      externalId: messageId,
      kind: "gmail_message_deleted",
      occurredAt,
      contentHash: `sha256:${sha256(tombstone)}`,
      encryptedContent: this.#secretBox.seal(tombstone, gmailSourceContentAad(connection, messageId)),
      metadata: {
        schemaVersion: 2,
        provider: "gmail",
        sourceScope: "personal",
        contentCompleteness: "metadata",
        googleSubject: connection.externalAccountId,
        discoveryMode: "history",
        historyId,
        providerEventIds: [...providerEventIds],
        contentAadVersion: 1,
        deleted: true,
      },
    });
    await this.#processDeletedMessage(connection, messageId, persisted.revision, occurredAt);
  }

  async #processDeletedMessage(
    connection: GoogleSyncConnection,
    messageId: string,
    revision: number,
    occurredAt: string,
  ): Promise<void> {
    const inboxItem = toGmailDeletedInboxItem(connection, messageId, revision, occurredAt);
    const applicationResult = await this.#application.process(inboxItem);
    if (applicationResult.outcome.status !== "processed") {
      throw new GoogleSyncError(
        "Gmail deletion was rejected by the application boundary",
        "invalid_state",
        false,
      );
    }
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

  async #finishRecovery(
    connection: GoogleSyncConnection,
    rawState: GmailSyncState,
    generationId: string,
  ): Promise<GmailSyncState> {
    const state = gmailSyncStateSchema.parse({
      ...rawState,
      revision: rawState.revision + 1,
      recovery: null,
    });
    const saved = await this.#repository.finishGmailRecovery({
      householdId: connection.householdId,
      adultId: connection.adultId,
      connectionId: connection.id,
      generationId,
      expectedRevision: rawState.revision,
      state,
    });
    if (saved !== "updated") {
      throw new GoogleSyncError(
        "Gmail recovery completion changed concurrently or became inactive",
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
    schemaVersion: 2,
    revision: 0,
    phase: "live",
    requestedDepth: "full_history",
    boundaryAt: now.toISOString(),
    scanPageToken: null,
    scanProcessedMessageIds: [],
    history: { cursorId: null, startId: null, pageToken: null, targetId: null },
    watch: null,
    recovery: null,
    lastSuccessfulSyncAt: null,
    discovery: null,
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
    return {
      ...state,
      phase: "one_year_backfill",
      scanPageToken: null,
      scanProcessedMessageIds: [],
    };
  }
  if (state.phase === "one_year_backfill" && state.requestedDepth === "full_history") {
    return {
      ...state,
      phase: "full_history_backfill",
      scanPageToken: null,
      scanProcessedMessageIds: [],
    };
  }
  return {
    ...state,
    phase: "live",
    scanPageToken: null,
    scanProcessedMessageIds: [],
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
    discovery:
      state.discovery?.status === "collecting" ? { ...state.discovery, status: "pending" } : state.discovery,
  };
}

function finalHistoryDecisions(changes: readonly GmailHistoryChange[]): Array<{
  messageId: string;
  historyId: string;
  deleted: boolean;
  added: boolean;
  providerEventIds: string[];
}> {
  const decisions = new Map<
    string,
    {
      messageId: string;
      historyId: string;
      deleted: boolean;
      added: boolean;
      providerEventIds: string[];
    }
  >();
  const ordered = [...changes].sort((left, right) => compareHistoryIds(left.historyId, right.historyId));
  for (const change of ordered) {
    const existing = decisions.get(change.messageId);
    decisions.set(change.messageId, {
      messageId: change.messageId,
      historyId: change.historyId,
      deleted:
        change.changeType === "message.deleted"
          ? true
          : change.changeType === "message.added"
            ? false
            : (existing?.deleted ?? false),
      added:
        change.changeType === "message.added"
          ? true
          : change.changeType === "message.deleted"
            ? false
            : (existing?.added ?? false),
      providerEventIds: [...(existing?.providerEventIds ?? []), change.providerEventId],
    });
  }
  return [...decisions.values()];
}

function hasPendingHistory(state: GmailSyncState): boolean {
  if (state.history.pageToken !== null) return true;
  if (state.history.targetId === null) return false;
  return (
    state.history.cursorId === null || compareHistoryIds(state.history.targetId, state.history.cursorId) > 0
  );
}

function isRecoveringExpiredHistory(state: GmailSyncState): boolean {
  return state.history.cursorId === null && state.history.startId !== null;
}

function requiresContinuation(state: GmailSyncState): boolean {
  return isScanPhase(state.phase) || hasPendingHistory(state) || state.discovery?.status === "pending";
}

function isWithinLiveTriageWindow(message: GmailMessage, now: Date): boolean {
  if (message.internalDate === null) return false;
  const occurredAt = new Date(message.internalDate).getTime();
  if (!Number.isFinite(occurredAt)) return false;
  return (
    occurredAt >= now.getTime() - GMAIL_LIVE_WINDOW_MS &&
    occurredAt <= now.getTime() + GMAIL_LIVE_FUTURE_TOLERANCE_MS
  );
}

function saturatingDiscoveryCount(current: number, increment: number): number {
  return Math.min(GMAIL_DISCOVERY_MESSAGE_COUNT_MAX, current + increment);
}

function gmailDiscoveryRunId(connectionId: string, boundaryAt: string): string {
  return `gmail-discovery:${connectionId}:${boundaryAt}`;
}

function gmailSourceEnvelope(
  message: GmailMessage,
  contentCompleteness: "metadata" | "full",
  attachmentContents: readonly GmailAttachmentContent[],
): GmailStoredSourceEnvelope {
  const storedMessage = storedGmailMessageSchema.parse({
    ...message,
    attachments: message.attachments.map((attachment) => ({
      ...attachment,
      embeddedDataBase64Url: null,
    })),
  });
  return gmailStoredSourceEnvelopeSchema.parse(
    contentCompleteness === "metadata"
      ? { schemaVersion: 2, contentCompleteness, message: storedMessage }
      : {
          schemaVersion: 2,
          contentCompleteness,
          message: storedMessage,
          attachmentContents: [...attachmentContents],
        },
  );
}

function assertOwnedGmailMessage(
  connection: GoogleSyncConnection,
  requestedMessageId: string,
  message: GmailMessage,
): void {
  if (
    message.googleSubject !== connection.externalAccountId ||
    message.messageId !== requestedMessageId ||
    message.sourceKey !== `${connection.externalAccountId}:${requestedMessageId}`
  ) {
    throw new GoogleSyncError("Gmail returned data outside the requested account", "not_authorized", false);
  }
}

function assertOwnedGmailHistoryChanges(
  connection: GoogleSyncConnection,
  changes: readonly GmailHistoryChange[],
): void {
  if (changes.some((change) => change.googleSubject !== connection.externalAccountId)) {
    throw new GoogleSyncError(
      "Gmail returned history outside the requested account",
      "not_authorized",
      false,
    );
  }
}

function assertStableMessageIdentity(metadata: GmailMessage, full: GmailMessage): void {
  if (
    metadata.googleSubject !== full.googleSubject ||
    metadata.messageId !== full.messageId ||
    metadata.sourceKey !== full.sourceKey ||
    metadata.threadId !== full.threadId ||
    metadata.internalDate !== full.internalDate
  ) {
    throw new GoogleSyncError("Gmail changed immutable message identity", "invalid_state", false);
  }
}

function gmailAttachmentReference(
  connection: GoogleSyncConnection,
  message: GmailMessage,
  attachment: GmailAttachment,
  index: number,
): string {
  const descriptorIdentity = canonicalJson({
    googleSubject: connection.externalAccountId,
    messageId: message.messageId,
    index,
    partId: attachment.partId,
    providerAttachmentId: attachment.providerAttachmentId,
    contentId: attachment.contentId,
    filename: attachment.filename,
    mimeType: attachment.mimeType,
    sizeBytes: attachment.sizeBytes,
    inline: attachment.inline,
  });
  return `gmail:${connection.id}:attachment:${sha256(descriptorIdentity)}`;
}

type GmailUnavailableAttachment = Extract<GmailAttachmentContent, { kind: "unavailable" }>;
type GmailUnavailableReason = GmailUnavailableAttachment["reason"];

function safeAttachmentMetadata(
  reference: string,
  attachment: GmailAttachment,
): Pick<GmailUnavailableAttachment, "reference" | "mediaType" | "filename" | "sizeBytes"> {
  const mediaType = attachment.mimeType.trim();
  return {
    reference,
    mediaType: mediaType.length > 0 && mediaType.length <= 255 ? mediaType : null,
    filename: safeAttachmentFilename(attachment.filename),
    sizeBytes: attachment.sizeBytes <= 100 * 1024 * 1024 ? attachment.sizeBytes : null,
  };
}

function safeAttachmentFilename(filename: string): string | null {
  const trimmed = filename.trim();
  return trimmed.length === 0 ? null : bounded(trimmed, 500);
}

function unavailableGmailAttachment(
  base: Pick<GmailUnavailableAttachment, "reference" | "mediaType" | "filename" | "sizeBytes">,
  reason: GmailUnavailableReason,
): GmailUnavailableAttachment {
  return {
    ...base,
    kind: "unavailable",
    reason,
    contentDigest: `sha256:${sha256(canonicalJson({ ...base, reason }))}`,
  };
}

function unavailableReasonForAttachmentError(error: unknown): GmailUnavailableReason | null {
  if (error instanceof GmailAttachmentContentError) {
    return error.reason;
  }
  if (error instanceof GoogleAdapterError && error.code === "not_found") {
    return "not_found";
  }
  return null;
}

function sourceMetadata(
  connection: GoogleSyncConnection,
  message: GmailMessage,
  discovery: {
    mode: string;
    historyId?: string;
    providerEventIds: readonly string[];
  },
  contentCompleteness: "metadata" | "full",
): GmailSourceMetadata {
  return gmailSourceMetadataSchema.parse({
    schemaVersion: 2,
    provider: "gmail",
    sourceScope: "personal",
    contentCompleteness,
    googleSubject: connection.externalAccountId,
    threadId: message.threadId,
    messageHistoryId: message.historyId,
    discoveryMode: discovery.mode,
    ...(discovery.historyId ? { discoveryHistoryId: discovery.historyId } : {}),
    providerEventIds: [...discovery.providerEventIds],
    contentAadVersion: 1,
  });
}

function toGmailInboxItem(
  connection: GoogleSyncConnection,
  message: GmailMessage,
  attachmentContents: readonly GmailAttachmentContent[],
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
    attachmentRefs: attachmentContents.map((attachment) => attachment.reference),
    attachmentContents: [...attachmentContents],
  });
}

function toGmailDeletedInboxItem(
  connection: GoogleSyncConnection,
  messageId: string,
  revision: number,
  occurredAt: string,
): GmailMessageDeletedInboxItem {
  return GmailMessageDeletedInboxItemSchema.parse({
    kind: "gmail_message_deleted",
    householdId: connection.householdId,
    idempotencyKey: `gmail:${connection.id}:${messageId}:revision:${revision}:deleted`,
    occurredAt,
    ownerAdultId: connection.adultId,
    accountRef: `google:${connection.id}`,
    messageRef: `gmail:${connection.id}:${messageId}`,
    revision,
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
