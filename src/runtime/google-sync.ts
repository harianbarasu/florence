import { createHash } from "node:crypto";
import type { OAuth2Client } from "google-auth-library";
import type { TransactionSql } from "postgres";
import { z } from "zod";
import { extractDocument } from "../adapters/content/extract.js";
import { CalendarAdapter } from "../adapters/google/calendar.js";
import type { GoogleCredentials, NormalizedGmailMessage } from "../adapters/google/contracts.js";
import { GmailAdapter } from "../adapters/google/gmail.js";
import { GoogleOAuthAdapter } from "../adapters/google/oauth.js";
import type { FlorenceConfig } from "../config.js";
import type { Database } from "../db/client.js";
import {
  assessMailMetadata,
  type CalendarPrivacyMode,
  gmailThreadCaseDigest,
  type IntegrationAccountKind,
  type IntegrationCapability,
  isFullMailContentAdmitted,
  JsonObjectSchema,
  type JsonValue,
  PostgresSourceIntelligence,
  planCalendarSyncWindow,
  planNewestFirstMailBackfill,
  privateSourceIntegrationLockKey,
  projectCalendarArtifact,
  type SourceReadResult,
} from "../modules/sources/index.js";
import { DurableWork } from "../modules/work/index.js";
import type { SecretBox } from "../shared/crypto.js";
import { NotFoundError, StaleAuthorityError, UnauthorizedError } from "../shared/errors.js";
import { type PrivateQuestionContext, privateMailSearchQuery } from "./private-question-context.js";

const GOOGLE_JOB_PRIORITY = {
  activation: 50,
  live: 50,
  calendar: 60,
  recentBackfill: 110,
  middleBackfill: 120,
  yearBackfill: 130,
  olderHistory: 140,
} as const;
const GOOGLE_OAUTH_CLIENT_CACHE_LIMIT = 512;

const GoogleJobBaseSchema = z.strictObject({
  integrationId: z.string().uuid(),
  personId: z.string().uuid(),
  integrationControlEpoch: z.number().int().positive(),
  personControlEpoch: z.number().int().positive(),
});

const GoogleActivationPayloadSchema = GoogleJobBaseSchema.extend({
  olderHistoryEnabled: z.boolean().default(false),
});
export const GoogleBootstrapPayloadSchema = GoogleActivationPayloadSchema;
export const GmailBootstrapPayloadSchema = GoogleActivationPayloadSchema.extend({
  runKey: z.string().min(1).max(100).optional(),
});

export const GmailPollPayloadSchema = GoogleJobBaseSchema;
export const GmailMessagePayloadSchema = GoogleJobBaseSchema.extend({
  messageId: z.string().min(1).max(500),
  sourcePriority: z.number().int().min(0),
});
export const GmailBackfillPayloadSchema = GoogleJobBaseSchema.extend({
  stage: z.enum(["newest_30_days", "days_31_to_90", "days_91_to_365", "older_history"]),
  afterExclusive: z.string().datetime({ offset: true }).nullable(),
  beforeOrEqual: z.string().datetime({ offset: true }).nullable(),
  pageToken: z.string().min(1).max(2_000).nullable().default(null),
  runKey: z.string().min(1).max(100).default("initial"),
});
export const CalendarCatalogPayloadSchema = GoogleJobBaseSchema;
export const CalendarPollPayloadSchema = GoogleJobBaseSchema.extend({
  calendarId: z.string().min(1).max(2_000),
  calendarIdDigest: z.string().regex(/^[a-f0-9]{64}$/u),
  mode: z.enum(["full_private", "availability_only"]),
  grantVersion: z.number().int().positive(),
});

type GoogleJobBase = z.infer<typeof GoogleJobBaseSchema>;
type CalendarPollPayload = z.infer<typeof CalendarPollPayloadSchema>;
type Transaction = TransactionSql<Record<string, never>>;
interface CalendarAuthorityContext {
  readonly transaction: Transaction;
  readonly sources: PostgresSourceIntelligence;
  readonly work: DurableWork;
}

interface PrivateGmailAccountRow {
  readonly id: string;
  readonly control_epoch: number | string;
  readonly account_kind: "personal_family" | "work";
  readonly status: "active" | "paused" | "reauth_required" | "error";
  readonly connected_at: Date;
}

interface PrivateGmailCursorRow {
  readonly integration_id: string;
  readonly resource_kind: string;
  readonly state: string;
  readonly updated_at: Date;
}

interface PrivateGmailJobRow {
  readonly integration_id: string;
  readonly job_kind: string;
  readonly idempotency_key: string;
  readonly status: string;
  readonly updated_at: Date;
}

interface PrivateGmailSearchResult {
  readonly integrationId: string;
  readonly search: "searched" | "not_requested" | "temporarily_unavailable";
  readonly evidence: readonly PrivateQuestionContext["evidence"][number][];
  readonly authorityOverride?: {
    readonly integrationControlEpoch: number;
    readonly status: PrivateGmailAccountRow["status"];
  };
}

export class GoogleSyncService {
  readonly #database: Database;
  readonly #sources: PostgresSourceIntelligence;
  readonly #work: DurableWork;
  readonly #oauth: GoogleOAuthAdapter;
  readonly #secretBox: SecretBox;
  readonly #oauthClients = new Map<string, OAuth2Client>();

  public constructor(
    database: Database,
    private readonly config: FlorenceConfig,
    secretBox: SecretBox,
  ) {
    this.#database = database;
    this.#secretBox = secretBox;
    this.#sources = new PostgresSourceIntelligence(database, secretBox, {
      rawRetentionDays: config.defaults.rawSourceRetentionDays,
      privateCandidateRetentionDays: 7,
    });
    this.#work = new DurableWork(database, secretBox);
    this.#oauth = new GoogleOAuthAdapter(config.google);
  }

  public async compilePrivateQuestionContext(input: {
    readonly personId: string;
    readonly expectedPersonControlEpoch: number;
    readonly question: string;
    readonly maxResults: number;
  }): Promise<PrivateQuestionContext> {
    const people = await this.#database<
      { readonly status: string; readonly control_epoch: number | string }[]
    >`
      select status, control_epoch from people where id = ${input.personId}
    `;
    const person = people[0];
    if (
      person?.status !== "registered" ||
      Number(person.control_epoch) !== input.expectedPersonControlEpoch
    ) {
      throw new StaleAuthorityError("Private source owner authority changed");
    }
    const accounts = await this.#database<PrivateGmailAccountRow[]>`
      select integration.id, integration.control_epoch, integration.account_kind,
        integration.status, integration.connected_at
      from integrations integration
      where integration.person_id = ${input.personId}
        and integration.provider = 'google'
        and integration.status in ('active', 'paused', 'reauth_required', 'error')
        and exists(
          select 1 from integration_capabilities capability
          where capability.integration_id = integration.id
            and capability.capability = 'mail' and capability.status = 'active'
        )
      order by integration.connected_at, integration.id
    `;
    if (accounts.length === 0) {
      return {
        provider: "google",
        searchQuery: null,
        sourceAuthorities: [],
        accounts: [],
        evidence: [],
      };
    }
    const integrationIds = accounts.map((account) => account.id);
    const [cursorRows, jobRows] = await Promise.all([
      this.#database<PrivateGmailCursorRow[]>`
        select integration_id, resource_kind, state, updated_at
        from sync_cursors
        where integration_id = any(${this.#database.array(integrationIds)}::uuid[])
          and resource_kind in (
            'gmail_history', 'gmail_backfill:newest_30_days',
            'gmail_backfill:days_31_to_90', 'gmail_backfill:days_91_to_365',
            'gmail_backfill:older_history'
          )
      `,
      this.#database<PrivateGmailJobRow[]>`
        select distinct on (job.integration_id, job.job_kind, job.idempotency_key)
          job.integration_id, job.job_kind, job.idempotency_key, job.status, job.updated_at
        from jobs job
        join integrations integration on integration.id = job.integration_id
        where job.integration_id = any(${this.#database.array(integrationIds)}::uuid[])
          and job.integration_control_epoch = integration.control_epoch
          and job.job_kind in ('google.gmail.bootstrap', 'google.gmail.poll', 'google.gmail.backfill')
        order by job.integration_id, job.job_kind, job.idempotency_key, job.updated_at desc
      `,
    ]);
    const query = privateMailSearchQuery(input.question);
    const activeAccounts = accounts.filter((account) => account.status === "active");
    const perAccountLimit = Math.max(
      1,
      Math.min(3, Math.ceil(Math.max(1, input.maxResults) / Math.max(1, activeAccounts.length))),
    );
    const searchResults = await Promise.all(
      activeAccounts.map(async (account): Promise<PrivateGmailSearchResult> => {
        if (!query) {
          return { integrationId: account.id, search: "not_requested", evidence: [] };
        }
        const payload = {
          integrationId: account.id,
          personId: input.personId,
          integrationControlEpoch: Number(account.control_epoch),
          personControlEpoch: input.expectedPersonControlEpoch,
          sourcePriority: GOOGLE_JOB_PRIORITY.activation,
        };
        try {
          const { gmail } = await this.clients(payload, "mail");
          const page = await gmail.listMessages(query, undefined, perAccountLimit, 8_000);
          const revisions = await Promise.all(
            page.messageIds
              .slice(0, perAccountLimit)
              .map((messageId) => this.ingestPrivateGmailQuestionHit(gmail, payload, messageId)),
          );
          const evidence = (
            await Promise.all(
              revisions.flatMap((sourceRevisionId) =>
                sourceRevisionId
                  ? [this.readPrivateGmailQuestionEvidence(sourceRevisionId, payload, account.account_kind)]
                  : [],
              ),
            )
          ).flatMap((item) => (item ? [item] : []));
          return { integrationId: account.id, search: "searched", evidence };
        } catch (error) {
          if (
            error instanceof NotFoundError ||
            error instanceof StaleAuthorityError ||
            error instanceof UnauthorizedError
          ) {
            throw error;
          }
          if (isGoogleProviderAuthFailure(error)) {
            const changed = await this.#sources.apply({
              kind: "set_integration_status",
              integrationId: account.id,
              personId: input.personId,
              expectedControlEpoch: Number(account.control_epoch),
              status: "reauth_required",
              changedAt: new Date().toISOString(),
            });
            if (changed.kind !== "integration_status_changed") {
              throw new Error("Google account reauthorization transition did not complete");
            }
            return {
              integrationId: account.id,
              search: "temporarily_unavailable",
              evidence: [],
              authorityOverride: {
                integrationControlEpoch: changed.controlEpoch,
                status: "reauth_required",
              },
            };
          }
          if (isRetryableGoogleError(error)) {
            return {
              integrationId: account.id,
              search: "temporarily_unavailable",
              evidence: [],
            };
          }
          throw error;
        }
      }),
    );
    const resultByIntegration = new Map(searchResults.map((result) => [result.integrationId, result]));
    const effectiveAccounts = accounts.map((account) => {
      const override = resultByIntegration.get(account.id)?.authorityOverride;
      return override
        ? { ...account, control_epoch: override.integrationControlEpoch, status: override.status }
        : account;
    });
    const evidence = searchResults.flatMap((result) => result.evidence);
    evidence.sort((left, right) => right.occurredAt.localeCompare(left.occurredAt));
    const boundedEvidence = evidence.slice(0, Math.max(1, Math.min(input.maxResults, 12)));
    return {
      provider: "google",
      searchQuery: query,
      sourceAuthorities: effectiveAccounts.map((account) => ({
        integrationId: account.id,
        integrationControlEpoch: Number(account.control_epoch),
        status: account.status,
      })),
      accounts: effectiveAccounts.map((account) =>
        privateGmailAccountStatus(
          account,
          cursorRows.filter(
            (cursor) => cursor.integration_id === account.id && cursor.updated_at >= account.connected_at,
          ),
          jobRows.filter((job) => job.integration_id === account.id),
          resultByIntegration.get(account.id)?.search ?? "not_requested",
        ),
      ),
      evidence: boundedEvidence,
    };
  }

  public async bootstrap(payloadCandidate: unknown): Promise<void> {
    const payload = GoogleBootstrapPayloadSchema.parse(payloadCandidate);
    const profile = await this.integrationProfile(payload);
    const capabilities = new Set(profile.integration.activeCapabilities);
    if (capabilities.has("mail")) {
      await this.#work.enqueue({
        kind: "google.gmail.bootstrap",
        idempotencyKey: `gmail:bootstrap:${payload.integrationId}:${controlEpochKey(payload)}`,
        payload,
        ...googleJobFence(payload),
        priority: GOOGLE_JOB_PRIORITY.activation,
        maxAttempts: 8,
      });
    }
    if (capabilities.has("calendar")) {
      await this.#work.enqueue({
        kind: "google.calendar.catalog",
        idempotencyKey: `calendar:catalog:${payload.integrationId}:${controlEpochKey(payload)}:activation`,
        payload: basePayload(payload),
        ...googleJobFence(payload),
        priority: GOOGLE_JOB_PRIORITY.calendar,
        maxAttempts: 8,
      });
    }
    if (!capabilities.has("mail") && !capabilities.has("calendar")) {
      throw new UnauthorizedError("Google integration has no active capability");
    }
  }

  public async bootstrapGmail(payloadCandidate: unknown): Promise<void> {
    const payload = GmailBootstrapPayloadSchema.parse(payloadCandidate);
    const { gmail } = await this.clients(payload, "mail");
    const existingCursor = await this.readOptionalCursor(payload, "gmail_history");
    let historyId =
      existingCursor &&
      existingCursor.state === "active" &&
      isRecord(existingCursor.cursor) &&
      typeof existingCursor.cursor.historyId === "string"
        ? existingCursor.cursor.historyId
        : null;
    if (!historyId) {
      const profile = await gmail.profile();
      historyId = profile.historyId;
      const checkpointedAt = new Date().toISOString();
      await this.#sources.apply({
        kind: "checkpoint_cursor",
        integrationId: payload.integrationId,
        personId: payload.personId,
        expectedIntegrationControlEpoch: payload.integrationControlEpoch,
        resourceKind: "gmail_history",
        cursor: { historyId },
        state: "active",
        expectedUpdatedAt: existingCursor?.updatedAt ?? null,
        checkpointAt: checkpointedAt,
        updatedAt: checkpointedAt,
      });
    }
    await this.#work.enqueue({
      kind: "google.gmail.poll",
      idempotencyKey: `gmail:poll:${payload.integrationId}:${controlEpochKey(payload)}:${historyId}`,
      payload: basePayload(payload),
      ...googleJobFence(payload),
      priority: GOOGLE_JOB_PRIORITY.live,
      maxAttempts: 8,
    });
    const runKey = payload.runKey ?? `activation-${payload.integrationControlEpoch}`;
    for (const stage of planNewestFirstMailBackfill({
      asOf: new Date().toISOString(),
      olderHistoryEnabled: payload.olderHistoryEnabled,
    }).filter((entry) => entry.kind !== "live")) {
      const stageCursor = await this.readOptionalCursor(payload, `gmail_backfill:${stage.kind}`);
      if (stageCursor?.state === "exhausted") continue;
      await this.#work.enqueue({
        kind: "google.gmail.backfill",
        idempotencyKey: `gmail:backfill:${payload.integrationId}:${controlEpochKey(payload)}:${stage.kind}:${runKey}:start`,
        payload: {
          ...basePayload(payload),
          stage: stage.kind,
          afterExclusive: stage.afterExclusive,
          beforeOrEqual: stage.beforeOrEqual,
          pageToken: null,
          runKey,
        },
        ...googleJobFence(payload),
        priority: backfillPriority(stage.kind),
        maxAttempts: 8,
      });
    }
  }

  public async pollGmail(payloadCandidate: unknown): Promise<void> {
    const payload = GmailPollPayloadSchema.parse(payloadCandidate);
    const { gmail } = await this.clients(payload, "mail");
    const cursor = await this.#sources.read({
      kind: "sync_cursor",
      integrationId: payload.integrationId,
      personId: payload.personId,
      expectedIntegrationControlEpoch: payload.integrationControlEpoch,
      resourceKind: "gmail_history",
    });
    if (cursor.kind !== "sync_cursor") throw new Error("Gmail history cursor is unavailable");
    let historyId =
      isRecord(cursor.cursor) && typeof cursor.cursor.historyId === "string" ? cursor.cursor.historyId : null;
    let expectedCursorUpdate = cursor.updatedAt;
    if (!historyId) {
      const profile = await gmail.profile();
      historyId = profile.historyId;
      const recoveredAt = new Date().toISOString();
      const recovered = await this.#sources.apply({
        kind: "checkpoint_cursor",
        integrationId: payload.integrationId,
        personId: payload.personId,
        expectedIntegrationControlEpoch: payload.integrationControlEpoch,
        resourceKind: "gmail_history",
        cursor: { historyId },
        state: "active",
        expectedUpdatedAt: cursor.updatedAt,
        checkpointAt: recoveredAt,
        updatedAt: recoveredAt,
      });
      if (recovered.kind !== "cursor_checkpointed") throw new Error("Gmail history recovery failed");
      expectedCursorUpdate = recovered.updatedAt;
      await this.enqueueRecoveryBackfills(payload);
    }
    let pageToken: string | undefined;
    let newestHistoryId = historyId;
    const messageIds = new Set<string>();
    try {
      do {
        const page = await gmail.history(historyId, pageToken);
        newestHistoryId = page.historyId;
        for (const id of page.messageIds) messageIds.add(id);
        pageToken = page.nextPageToken;
      } while (pageToken);
    } catch (error) {
      if (googleErrorStatus(error) === 404) {
        await this.recoverIntegrationSync(payload, "mail");
        return;
      }
      throw error;
    }
    for (const messageId of messageIds) {
      await this.enqueueGmailMessage(
        payload,
        messageId,
        `history:${newestHistoryId}`,
        GOOGLE_JOB_PRIORITY.live,
      );
    }
    await this.#sources.apply({
      kind: "checkpoint_cursor",
      integrationId: payload.integrationId,
      personId: payload.personId,
      expectedIntegrationControlEpoch: payload.integrationControlEpoch,
      resourceKind: "gmail_history",
      cursor: { historyId: newestHistoryId },
      state: "active",
      expectedUpdatedAt: expectedCursorUpdate,
      checkpointAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });
    const bucket = Math.floor(
      (Date.now() + this.config.intervals.gmailPollMs) / this.config.intervals.gmailPollMs,
    );
    await this.#work.enqueue({
      kind: "google.gmail.poll",
      idempotencyKey: `gmail:poll:${payload.integrationId}:${controlEpochKey(payload)}:t${bucket}`,
      payload,
      ...googleJobFence(payload),
      priority: GOOGLE_JOB_PRIORITY.live,
      availableAt: new Date(Date.now() + this.config.intervals.gmailPollMs),
      maxAttempts: 8,
    });
  }

  private async enqueueRecoveryBackfills(payload: GoogleJobBase): Promise<void> {
    const runKey = `recovery-${Math.floor(Date.now() / 86_400_000)}`;
    for (const stage of planNewestFirstMailBackfill({
      asOf: new Date().toISOString(),
      olderHistoryEnabled: true,
    }).filter((entry) => entry.kind !== "live")) {
      await this.#work.enqueue({
        kind: "google.gmail.backfill",
        idempotencyKey: `gmail:backfill:${payload.integrationId}:${controlEpochKey(payload)}:${stage.kind}:${runKey}:start`,
        payload: {
          ...payload,
          stage: stage.kind,
          afterExclusive: stage.afterExclusive,
          beforeOrEqual: stage.beforeOrEqual,
          pageToken: null,
          runKey,
        },
        ...googleJobFence(payload),
        priority: backfillPriority(stage.kind),
        maxAttempts: 8,
      });
    }
  }

  public async backfillGmail(payloadCandidate: unknown): Promise<void> {
    const payload = GmailBackfillPayloadSchema.parse(payloadCandidate);
    const { gmail } = await this.clients(payload, "mail");
    const page = await gmail.listMessages(
      gmailDateQuery(payload.afterExclusive, payload.beforeOrEqual),
      payload.pageToken ?? undefined,
      50,
    );
    for (const messageId of page.messageIds) {
      await this.enqueueGmailMessage(
        payload,
        messageId,
        `backfill:${payload.runKey}`,
        backfillPriority(payload.stage),
      );
    }
    if (page.nextPageToken) {
      await this.#work.enqueue({
        kind: "google.gmail.backfill",
        idempotencyKey: `gmail:backfill:${payload.integrationId}:${controlEpochKey(payload)}:${payload.stage}:${payload.runKey}:${sha256Hex(page.nextPageToken)}`,
        payload: { ...payload, pageToken: page.nextPageToken },
        ...googleJobFence(payload),
        priority: backfillPriority(payload.stage),
        maxAttempts: 8,
      });
    } else {
      const resourceKind = `gmail_backfill:${payload.stage}`;
      const expectedUpdatedAt = await this.cursorUpdatedAt(payload, resourceKind);
      await this.#sources.apply({
        kind: "checkpoint_cursor",
        integrationId: payload.integrationId,
        personId: payload.personId,
        expectedIntegrationControlEpoch: payload.integrationControlEpoch,
        resourceKind,
        cursor: null,
        state: "exhausted",
        expectedUpdatedAt,
        checkpointAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      });
    }
  }

  public async ingestGmailMessage(payloadCandidate: unknown): Promise<string | null> {
    const payload = GmailMessagePayloadSchema.parse(payloadCandidate);
    const { gmail } = await this.clients(payload, "mail");
    let triggeredMetadata: NormalizedGmailMessage;
    try {
      triggeredMetadata = await gmail.message(payload.messageId, true);
    } catch (error) {
      if (googleErrorStatus(error) === 404) {
        await this.markGmailMessageDeleted(payload, payload.messageId);
        return null;
      }
      throw error;
    }

    const correlationDigest = gmailThreadCaseDigest({
      integrationId: payload.integrationId,
      threadId: triggeredMetadata.threadId,
    });
    let threadMetadata: readonly NormalizedGmailMessage[];
    try {
      threadMetadata = await gmail.threadMetadata(triggeredMetadata.threadId);
    } catch (error) {
      if (googleErrorStatus(error) === 404) {
        await this.markGmailMessageDeleted(payload, payload.messageId, correlationDigest);
        return null;
      }
      throw error;
    }
    let triggeredSourceRevisionId: string | null = null;
    let newestCurrent: {
      readonly internalDate: Date;
      readonly messageId: string;
      readonly sourceRevisionId: string;
    } | null = null;
    let hasFullContent = false;

    for (const metadata of threadMetadata) {
      if (metadata.threadId !== triggeredMetadata.threadId) {
        throw new Error("Gmail thread metadata contained a message from a different thread");
      }
      const admission = assessMailMetadata({
        labelIds: metadata.labelIds,
        from: metadata.from,
        subject: metadata.subject,
        snippet: metadata.snippet,
        hasAttachments: metadata.hasAttachmentHint,
      });
      if (!admission.ingestMetadata) {
        await this.markGmailMessageDeleted(payload, metadata.id, correlationDigest);
        continue;
      }

      const fullContentAdmitted = isFullMailContentAdmitted(admission);
      let message = metadata;
      if (fullContentAdmitted) {
        try {
          message = await gmail.message(metadata.id, false);
        } catch (error) {
          if (googleErrorStatus(error) === 404) {
            await this.markGmailMessageDeleted(payload, metadata.id, correlationDigest);
            continue;
          }
          throw error;
        }
      }

      const ingested = await this.ingestGmailThreadMessage(
        payload,
        message,
        admission,
        correlationDigest,
        fullContentAdmitted,
      );
      if (metadata.id === payload.messageId) triggeredSourceRevisionId = ingested.sourceRevisionId;

      if (
        newestCurrent === null ||
        compareGmailMessageRecency(
          { internalDate: message.internalDate, messageId: message.id },
          newestCurrent,
        ) > 0
      ) {
        newestCurrent = {
          internalDate: message.internalDate,
          messageId: message.id,
          sourceRevisionId: ingested.sourceRevisionId,
        };
      }

      if (fullContentAdmitted) {
        hasFullContent = true;
        for (const attachment of message.attachments) {
          if (attachment.size > 15 * 1024 * 1024) {
            await this.ingestGmailAttachmentOmission(
              message,
              ingested.sourceRevisionId,
              correlationDigest,
              payload,
              attachment,
            );
          } else {
            await this.ingestGmailAttachment(
              gmail,
              message,
              ingested.sourceRevisionId,
              correlationDigest,
              payload,
              attachment,
            );
          }
        }
      }
    }

    if (!hasFullContent || !newestCurrent) return triggeredSourceRevisionId;
    await this.#work.enqueue({
      kind: "orchestrate.private_source",
      idempotencyKey: `orchestrate:source:${newestCurrent.sourceRevisionId}:${controlEpochKey(payload)}`,
      payload: {
        sourceRevisionId: newestCurrent.sourceRevisionId,
        personId: payload.personId,
        integrationId: payload.integrationId,
        integrationControlEpoch: payload.integrationControlEpoch,
      },
      ...googleJobFence(payload),
      caseKeyDigest: correlationDigest,
      priority: Math.min(1_000, payload.sourcePriority + 5),
      maxAttempts: 8,
    });
    return triggeredSourceRevisionId ?? newestCurrent.sourceRevisionId;
  }

  public async catalogCalendars(payloadCandidate: unknown): Promise<void> {
    const payload = CalendarCatalogPayloadSchema.parse(payloadCandidate);
    const { accountKind, calendar } = await this.clients(payload, "calendar");
    const calendars: Array<{
      id: string;
      summary: string;
      primary: boolean;
      timezone: string | null;
      deleted: boolean;
    }> = [];
    let pageToken: string | undefined;
    do {
      const page = await calendar.listCalendars(pageToken);
      calendars.push(
        ...page.calendars.map((entry) => ({
          id: entry.id,
          summary: entry.summary,
          primary: entry.primary,
          timezone: entry.timezone,
          deleted: entry.deleted,
        })),
      );
      pageToken = page.nextPageToken;
    } while (pageToken);
    const expectedCatalogUpdate = await this.cursorUpdatedAt(payload, "calendar_catalog");
    await this.#sources.apply({
      kind: "checkpoint_cursor",
      integrationId: payload.integrationId,
      personId: payload.personId,
      expectedIntegrationControlEpoch: payload.integrationControlEpoch,
      resourceKind: "calendar_catalog",
      cursor: calendars,
      state: "active",
      expectedUpdatedAt: expectedCatalogUpdate,
      checkpointAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });
    const reconciled = await this.#sources.apply({
      kind: "reconcile_calendar_catalog",
      integrationId: payload.integrationId,
      personId: payload.personId,
      expectedIntegrationControlEpoch: payload.integrationControlEpoch,
      activeCalendarIdDigests: [
        ...new Set(calendars.filter((entry) => !entry.deleted).map((entry) => sha256Hex(entry.id))),
      ],
      reconciledAt: new Date().toISOString(),
    });
    if (reconciled.kind !== "calendar_catalog_reconciled") {
      throw new Error("Calendar catalog reconciliation did not complete");
    }
    if (reconciled.resetRequired) {
      await this.enqueueIntegrationBootstrap(payload, reconciled.integrationControlEpoch, "calendar-catalog");
      return;
    }
    for (const entry of calendars.filter((calendarEntry) => !calendarEntry.deleted)) {
      const digest = sha256Hex(entry.id);
      try {
        const policy = await this.#sources.read({
          kind: "calendar_privacy",
          integrationId: payload.integrationId,
          personId: payload.personId,
          expectedIntegrationControlEpoch: payload.integrationControlEpoch,
          calendarIdDigest: digest,
        });
        if (policy.kind === "calendar_privacy" && policy.mode !== "off") {
          await this.enqueueCalendarPoll(payload, entry.id, digest, policy.mode, policy.grantVersion);
        }
      } catch (error) {
        if (!(error instanceof NotFoundError)) throw error;
        if (!entry.primary) continue;
        const mode = defaultPrimaryCalendarMode(accountKind);
        const configured = await this.#sources.apply({
          kind: "configure_calendar_privacy",
          integrationId: payload.integrationId,
          personId: payload.personId,
          expectedIntegrationControlEpoch: payload.integrationControlEpoch,
          calendarIdDigest: digest,
          mode,
          changedAt: new Date().toISOString(),
        });
        if (configured.kind !== "calendar_privacy_configured") {
          throw new Error("Primary calendar privacy default was not configured");
        }
        await this.enqueueCalendarPoll(payload, entry.id, digest, mode, configured.grantVersion);
      }
    }
    const bucket = Math.floor((Date.now() + 24 * 60 * 60_000) / (24 * 60 * 60_000));
    await this.#work.enqueue({
      kind: "google.calendar.catalog",
      idempotencyKey: `calendar:catalog:${payload.integrationId}:${controlEpochKey(payload)}:d${bucket}`,
      payload,
      ...googleJobFence(payload),
      priority: GOOGLE_JOB_PRIORITY.calendar,
      availableAt: new Date(Date.now() + 24 * 60 * 60_000),
      maxAttempts: 8,
    });
  }

  public async pollCalendar(payloadCandidate: unknown): Promise<void> {
    const payload = CalendarPollPayloadSchema.parse(payloadCandidate);
    await this.assertCalendarPollAuthority(payload);
    const { calendar } = await this.clients(payload, "calendar");
    const cursorKind = `calendar:${payload.calendarIdDigest}`;
    let syncToken: string | undefined;
    let expectedCursorUpdate: string | null = null;
    let cursorForCheckpoint: JsonValue | null = null;
    let reconcileInterruptedFrontier = false;
    try {
      const cursor = await this.#sources.read({
        kind: "sync_cursor",
        integrationId: payload.integrationId,
        personId: payload.personId,
        expectedIntegrationControlEpoch: payload.integrationControlEpoch,
        resourceKind: cursorKind,
      });
      if (
        cursor.kind === "sync_cursor" &&
        isRecord(cursor.cursor) &&
        typeof cursor.cursor.syncToken === "string"
      ) {
        syncToken = cursor.cursor.syncToken;
      }
      if (cursor.kind === "sync_cursor") {
        expectedCursorUpdate = cursor.updatedAt;
        cursorForCheckpoint = cursor.cursor;
        reconcileInterruptedFrontier = cursor.state === "initial" && cursor.checkpointAt === null;
      }
    } catch (error) {
      if (!(error instanceof NotFoundError)) throw error;
    }
    const processingAt = new Date().toISOString();
    await this.withCalendarPollAuthority(payload, async ({ sources }) => {
      await sources.apply({
        kind: "checkpoint_cursor",
        integrationId: payload.integrationId,
        personId: payload.personId,
        expectedIntegrationControlEpoch: payload.integrationControlEpoch,
        resourceKind: cursorKind,
        cursor: cursorForCheckpoint,
        state: "initial",
        expectedUpdatedAt: expectedCursorUpdate,
        checkpointAt: null,
        updatedAt: processingAt,
      });
    });
    expectedCursorUpdate = processingAt;
    const window = planCalendarSyncWindow(new Date().toISOString());
    let pageToken: string | undefined;
    let nextSyncToken: string | undefined;
    let frontierChanged = false;
    try {
      do {
        await this.assertCalendarPollAuthority(payload);
        const page = await calendar.listEvents({
          calendarId: payload.calendarId,
          ...(syncToken
            ? { syncToken }
            : { timeMin: new Date(window.startsAt), timeMax: new Date(window.endsAt) }),
          ...(pageToken ? { pageToken } : {}),
        });
        for (const event of page.events) {
          const origin = {
            system: "google.calendar",
            remoteObjectId: `${payload.calendarId}:${event.id}`,
            ...(event.etag ? { remoteRevisionId: event.etag } : {}),
          };
          const projected = projectCalendarArtifact(
            {
              remoteEventId: event.id,
              start: event.start,
              end: event.end,
              status: event.status,
              ...(event.summary !== null ? { title: event.summary } : {}),
              ...(event.description !== null ? { description: event.description } : {}),
              ...(event.location !== null ? { location: event.location } : {}),
              ...(event.recurringEventId !== null ? { recurrenceId: event.recurringEventId } : {}),
              attendees: event.attendees.map((attendee) => JsonObjectSchema.parse(attendee)),
            },
            payload.mode,
          );
          if (!projected) continue;
          await this.withCalendarPollAuthority(payload, async ({ sources }) => {
            const ingested = await sources.apply({
              kind: "ingest_source",
              integrationId: payload.integrationId,
              expectedIntegrationControlEpoch: payload.integrationControlEpoch,
              artifactKind: "calendar_event",
              origin,
              resourceDigest: payload.calendarIdDigest,
              scope: { kind: "person", personId: payload.personId },
              content: projected,
              occurredAt: event.updatedAt?.toISOString() ?? new Date().toISOString(),
              capturedAt: new Date().toISOString(),
              requestedRetentionUntil: new Date(
                Date.now() + this.config.defaults.rawSourceRetentionDays * 86_400_000,
              ).toISOString(),
            });
            if (ingested.kind === "source_ingested" && !ingested.duplicate) frontierChanged = true;
          });
        }
        pageToken = page.nextPageToken;
        nextSyncToken = page.nextSyncToken ?? nextSyncToken;
      } while (pageToken);
    } catch (error) {
      if (googleErrorStatus(error) === 410) {
        await this.withCalendarPollAuthority(payload, async ({ sources, work }) => {
          await this.recoverIntegrationSync(payload, "calendar", sources, work);
        });
        return;
      }
      throw error;
    }
    const settledAt = new Date().toISOString();
    await this.withCalendarPollAuthority(payload, async ({ transaction, sources, work }) => {
      await sources.apply({
        kind: "checkpoint_cursor",
        integrationId: payload.integrationId,
        personId: payload.personId,
        expectedIntegrationControlEpoch: payload.integrationControlEpoch,
        resourceKind: cursorKind,
        cursor: nextSyncToken ? { syncToken: nextSyncToken } : null,
        state: nextSyncToken ? "active" : "initial",
        expectedUpdatedAt: expectedCursorUpdate,
        checkpointAt: settledAt,
        updatedAt: settledAt,
      });
      if ((frontierChanged || reconcileInterruptedFrontier) && nextSyncToken) {
        await this.enqueueCurrentPrivateFrontiers(payload, `${cursorKind}:${settledAt}`, transaction, work);
      }
    });
    const bucket = Math.floor(
      (Date.now() + this.config.intervals.calendarPollMs) / this.config.intervals.calendarPollMs,
    );
    await this.withCalendarPollAuthority(payload, async ({ work }) => {
      await work.enqueue({
        kind: "google.calendar.poll",
        idempotencyKey: calendarPollKey(payload, `t${bucket}`),
        payload,
        ...googleJobFence(payload),
        priority: GOOGLE_JOB_PRIORITY.calendar,
        availableAt: new Date(Date.now() + this.config.intervals.calendarPollMs),
        maxAttempts: 8,
      });
    });
  }

  private async enqueueCurrentPrivateFrontiers(
    payload: GoogleJobBase,
    reconcileKey: string,
    transaction: Transaction,
    work: DurableWork,
  ): Promise<void> {
    const frontiers = await transaction<
      { readonly case_key_digest: string; readonly source_revision_id: string }[]
    >`
      select frontier.case_key_digest, anchor.source_revision_id
      from private_source_frontiers frontier
      join lateral (
        select revision.id as source_revision_id
        from source_objects object
        join source_revisions revision on revision.source_object_id = object.id
          and revision.revision_number = object.latest_revision_number
        where object.integration_id = frontier.integration_id
          and object.provider = 'gmail' and object.object_kind = 'mail_message'
          and object.correlation_digest = frontier.case_key_digest
          and object.status = 'active'
          and revision.owner_person_id = frontier.owner_person_id
          and revision.revoked_at is null and revision.retention_until > now()
          and revision.content_ciphertext is not null
        order by revision.occurred_at desc, revision.id desc
        limit 1
      ) anchor on true
      where frontier.integration_id = ${payload.integrationId}
        and frontier.owner_person_id = ${payload.personId}
      order by frontier.updated_at, frontier.id
    `;
    const generation = sha256Hex(reconcileKey);
    for (const frontier of frontiers) {
      await work.enqueue({
        kind: "orchestrate.private_source",
        idempotencyKey: `orchestrate:frontier:${payload.integrationId}:${controlEpochKey(payload)}:${frontier.case_key_digest}:calendar:${generation}`,
        payload: {
          sourceRevisionId: frontier.source_revision_id,
          personId: payload.personId,
          integrationId: payload.integrationId,
          integrationControlEpoch: payload.integrationControlEpoch,
        },
        ...googleJobFence(payload),
        caseKeyDigest: frontier.case_key_digest,
        priority: GOOGLE_JOB_PRIORITY.calendar + 5,
        maxAttempts: 8,
      });
    }
  }

  public async handleProviderAuthFailure(
    jobKind: string,
    payloadCandidate: unknown,
    error: unknown,
  ): Promise<boolean> {
    const affectedCapability = googleJobCapability(jobKind);
    if (!affectedCapability || !isGoogleProviderAuthFailure(error)) return false;
    const parsedPayload = GoogleJobBaseSchema.safeParse(payloadCandidate);
    if (!parsedPayload.success) return false;
    const payload = parsedPayload.data;
    await this.#sources.apply({
      kind: "set_integration_status",
      integrationId: payload.integrationId,
      personId: payload.personId,
      expectedControlEpoch: payload.integrationControlEpoch,
      status: "reauth_required",
      changedAt: new Date().toISOString(),
    });
    return true;
  }

  private async integrationProfile(payload: GoogleJobBase) {
    const profile = await this.#sources.read({
      kind: "integration_profile",
      integrationId: payload.integrationId,
      personId: payload.personId,
      expectedControlEpoch: payload.integrationControlEpoch,
    });
    if (profile.kind !== "integration_profile") {
      throw new Error("Google integration profile is not accessible");
    }
    if (profile.integration.provider !== "google") {
      throw new UnauthorizedError("Integration is not a Google account");
    }
    if (profile.integration.status !== "active") {
      throw new UnauthorizedError("Google integration is not active");
    }
    return profile;
  }

  private async integrationAccess(payload: GoogleJobBase, requiredCapability: IntegrationCapability) {
    const access = await this.#sources.read({
      kind: "integration_access",
      integrationId: payload.integrationId,
      personId: payload.personId,
      expectedControlEpoch: payload.integrationControlEpoch,
      requiredCapability,
    });
    if (access.kind !== "integration_access") throw new Error("Google integration is not accessible");
    if (access.integration.provider !== "google") {
      throw new UnauthorizedError("Integration is not a Google account");
    }
    return access;
  }

  private async clients(payload: GoogleJobBase, requiredCapability: IntegrationCapability) {
    const access = await this.integrationAccess(payload, requiredCapability);
    const credentials = access.credentials as GoogleCredentials;
    const oauthClient = this.oauthClient(payload, credentials);
    return {
      accountKind: access.integration.accountKind,
      gmail: new GmailAdapter(oauthClient),
      calendar: new CalendarAdapter(oauthClient),
    };
  }

  private oauthClient(payload: GoogleJobBase, credentials: GoogleCredentials): OAuth2Client {
    const cacheKey = `${payload.integrationId}:e${payload.integrationControlEpoch}`;
    const cached = this.#oauthClients.get(cacheKey);
    if (cached) {
      this.#oauthClients.delete(cacheKey);
      this.#oauthClients.set(cacheKey, cached);
      return cached;
    }
    for (const key of this.#oauthClients.keys()) {
      if (key.startsWith(`${payload.integrationId}:`) && key !== cacheKey) {
        this.#oauthClients.delete(key);
      }
    }
    const client = this.#oauth.client(credentials);
    this.#oauthClients.set(cacheKey, client);
    while (this.#oauthClients.size > GOOGLE_OAUTH_CLIENT_CACHE_LIMIT) {
      const oldest = this.#oauthClients.keys().next().value;
      if (oldest === undefined) break;
      this.#oauthClients.delete(oldest);
    }
    return client;
  }

  private async assertCalendarPollAuthority(
    payload: CalendarPollPayload,
    sources = this.#sources,
  ): Promise<void> {
    const policy = await sources.read({
      kind: "calendar_privacy",
      integrationId: payload.integrationId,
      personId: payload.personId,
      expectedIntegrationControlEpoch: payload.integrationControlEpoch,
      calendarIdDigest: payload.calendarIdDigest,
    });
    if (
      policy.kind !== "calendar_privacy" ||
      policy.mode === "off" ||
      policy.mode !== payload.mode ||
      policy.grantVersion !== payload.grantVersion
    ) {
      throw new StaleAuthorityError("Calendar privacy authority changed");
    }
  }

  private async withCalendarPollAuthority<Result>(
    payload: CalendarPollPayload,
    operation: (context: CalendarAuthorityContext) => Promise<Result>,
  ): Promise<Result> {
    return this.#database.begin(async (transaction) => {
      await transaction`
        select pg_advisory_xact_lock(
          hashtextextended(${privateSourceIntegrationLockKey(payload.integrationId)}, 0)
        )
      `;
      const integrations = await transaction<
        {
          readonly person_id: string;
          readonly provider: string;
          readonly status: string;
          readonly control_epoch: number | string;
        }[]
      >`
        select person_id, provider, status, control_epoch
        from integrations
        where id = ${payload.integrationId}
        for update
      `;
      const integration = integrations[0];
      if (!integration || integration.person_id !== payload.personId) {
        throw new UnauthorizedError("Calendar integration is not accessible");
      }
      if (integration.provider !== "google" || integration.status !== "active") {
        throw new UnauthorizedError("Calendar integration is not active");
      }
      if (Number(integration.control_epoch) !== payload.integrationControlEpoch) {
        throw new StaleAuthorityError("Calendar integration authority changed");
      }
      await transaction`select pg_advisory_xact_lock(hashtextextended(${calendarPolicyLockKey(payload)}, 0))`;
      const sources = new PostgresSourceIntelligence(transaction, this.#secretBox, {
        rawRetentionDays: this.config.defaults.rawSourceRetentionDays,
        privateCandidateRetentionDays: 7,
      });
      await this.assertCalendarPollAuthority(payload, sources);
      return operation({
        transaction,
        sources,
        work: new DurableWork(transaction, this.#secretBox),
      });
    }) as unknown as Promise<Result>;
  }

  private async enqueueGmailMessage(
    payload: GoogleJobBase,
    messageId: string,
    observationKey: string,
    priority: number,
  ): Promise<void> {
    await this.#work.enqueue({
      kind: "google.gmail.message",
      idempotencyKey: `gmail:message:${payload.integrationId}:${controlEpochKey(payload)}:${messageId}:${sha256Hex(observationKey)}`,
      payload: buildGmailMessageJobPayload(payload, messageId, priority),
      ...googleJobFence(payload),
      priority,
      maxAttempts: 8,
    });
  }

  private async enqueueCalendarPoll(
    payload: GoogleJobBase,
    calendarId: string,
    calendarIdDigest: string,
    mode: Exclude<CalendarPrivacyMode, "off">,
    grantVersion: number,
  ): Promise<void> {
    const pollPayload = CalendarPollPayloadSchema.parse({
      ...payload,
      calendarId,
      calendarIdDigest,
      mode,
      grantVersion,
    });
    await this.withCalendarPollAuthority(pollPayload, async ({ transaction, work }) => {
      const live = await transaction<{ readonly present: boolean }[]>`
        select exists(
          select 1 from jobs
          where integration_id = ${payload.integrationId}
            and integration_control_epoch = ${payload.integrationControlEpoch}
            and person_id = ${payload.personId}
            and job_kind = 'google.calendar.poll'
            and idempotency_key like ${`${calendarPollKeyPrefix(pollPayload)}%`}
            and status in ('pending', 'retry', 'leased')
        ) as present
      `;
      if (live[0]?.present) return;
      const recoveryBucket = Math.floor(Date.now() / 60_000);
      await work.enqueue({
        kind: "google.calendar.poll",
        idempotencyKey: calendarPollKey(pollPayload, `seed${recoveryBucket}`),
        payload: pollPayload,
        ...googleJobFence(payload),
        priority: GOOGLE_JOB_PRIORITY.calendar,
        maxAttempts: 8,
      });
    });
  }

  private async readOptionalCursor(
    payload: GoogleJobBase,
    resourceKind: string,
  ): Promise<Extract<SourceReadResult, { kind: "sync_cursor" }> | null> {
    try {
      const cursor = await this.#sources.read({
        kind: "sync_cursor",
        integrationId: payload.integrationId,
        personId: payload.personId,
        expectedIntegrationControlEpoch: payload.integrationControlEpoch,
        resourceKind,
      });
      if (cursor.kind !== "sync_cursor") throw new Error("Google sync cursor read was inconsistent");
      return cursor;
    } catch (error) {
      if (error instanceof NotFoundError) return null;
      throw error;
    }
  }

  private async cursorUpdatedAt(payload: GoogleJobBase, resourceKind: string): Promise<string | null> {
    try {
      const cursor = await this.#sources.read({
        kind: "sync_cursor",
        integrationId: payload.integrationId,
        personId: payload.personId,
        expectedIntegrationControlEpoch: payload.integrationControlEpoch,
        resourceKind,
      });
      return cursor.kind === "sync_cursor" ? cursor.updatedAt : null;
    } catch {
      return null;
    }
  }

  private async recoverIntegrationSync(
    payload: GoogleJobBase,
    affectedCapability: IntegrationCapability,
    sources = this.#sources,
    work = this.#work,
  ): Promise<void> {
    const reset = await sources.apply({
      kind: "reset_integration_sync",
      integrationId: payload.integrationId,
      personId: payload.personId,
      expectedIntegrationControlEpoch: payload.integrationControlEpoch,
      affectedCapability,
      resetAt: new Date().toISOString(),
    });
    if (reset.kind !== "integration_sync_reset") {
      throw new Error("Google synchronization recovery did not reset authority");
    }
    await this.enqueueIntegrationBootstrap(
      payload,
      reset.integrationControlEpoch,
      `${affectedCapability}-cursor`,
      work,
    );
  }

  private async enqueueIntegrationBootstrap(
    payload: GoogleJobBase,
    integrationControlEpoch: number,
    reason: "mail-cursor" | "calendar-cursor" | "calendar-catalog",
    work = this.#work,
  ): Promise<void> {
    const recoveredPayload = {
      ...payload,
      integrationControlEpoch,
      olderHistoryEnabled: true,
    };
    await work.enqueue({
      kind: "google.bootstrap",
      idempotencyKey: `google:bootstrap:${payload.integrationId}:e${integrationControlEpoch}:recovery:${reason}`,
      payload: recoveredPayload,
      ...googleJobFence(recoveredPayload),
      priority: GOOGLE_JOB_PRIORITY.activation,
      maxAttempts: 8,
    });
  }

  /**
   * An explicit private Gmail question needs the matching message, not an
   * eager replay of its entire thread and attachment graph. This path keeps
   * the same admission and encrypted-source boundary as background sync while
   * remaining bounded enough for an interactive reply.
   */
  private async ingestPrivateGmailQuestionHit(
    gmail: GmailAdapter,
    payload: Omit<z.infer<typeof GmailMessagePayloadSchema>, "messageId">,
    messageId: string,
  ): Promise<string | null> {
    const messagePayload = { ...payload, messageId };
    let metadata: NormalizedGmailMessage;
    try {
      metadata = await gmail.message(messageId, true, 8_000);
    } catch (error) {
      if (googleErrorStatus(error) === 404) {
        await this.markGmailMessageDeleted(messagePayload, messageId);
        return null;
      }
      throw error;
    }
    const correlationDigest = gmailThreadCaseDigest({
      integrationId: payload.integrationId,
      threadId: metadata.threadId,
    });
    const admission = assessMailMetadata({
      labelIds: metadata.labelIds,
      from: metadata.from,
      subject: metadata.subject,
      snippet: metadata.snippet,
      hasAttachments: metadata.hasAttachmentHint,
    });
    if (!admission.ingestMetadata) {
      await this.markGmailMessageDeleted(messagePayload, messageId, correlationDigest);
      return null;
    }
    const fullContentAdmitted = isFullMailContentAdmitted(admission);
    let message = metadata;
    if (fullContentAdmitted) {
      try {
        message = await gmail.message(messageId, false, 8_000);
      } catch (error) {
        if (googleErrorStatus(error) === 404) {
          await this.markGmailMessageDeleted(messagePayload, messageId, correlationDigest);
          return null;
        }
        throw error;
      }
    }
    const ingested = await this.ingestGmailThreadMessage(
      messagePayload,
      message,
      admission,
      correlationDigest,
      fullContentAdmitted,
    );
    return ingested.sourceRevisionId;
  }

  private async readPrivateGmailQuestionEvidence(
    sourceRevisionId: string,
    payload: GoogleJobBase,
    accountKind: PrivateGmailAccountRow["account_kind"],
  ): Promise<PrivateQuestionContext["evidence"][number] | null> {
    const revision = await this.#sources.read({
      kind: "source_revision",
      sourceRevisionId,
      scope: { kind: "person", personId: payload.personId },
      integrationId: payload.integrationId,
      expectedIntegrationControlEpoch: payload.integrationControlEpoch,
      asOf: new Date().toISOString(),
    });
    if (revision.kind !== "source_revision") return null;
    return {
      sourceRevisionId,
      occurredAt: revision.occurredAt,
      accountKind,
      content: revision.content,
    };
  }

  private async ingestGmailThreadMessage(
    payload: z.infer<typeof GmailMessagePayloadSchema>,
    message: NormalizedGmailMessage,
    admission: ReturnType<typeof assessMailMetadata>,
    correlationDigest: string,
    fullContentAdmitted: boolean,
  ) {
    const content = JsonObjectSchema.parse(
      JSON.parse(
        JSON.stringify({
          threadId: message.threadId,
          historyId: message.historyId,
          internalDate: message.internalDate.toISOString(),
          labelIds: [...message.labelIds],
          from: message.from,
          to: [...message.to],
          cc: [...message.cc],
          subject: message.subject,
          messageIdHeader: message.messageIdHeader,
          snippet: message.snippet,
          text: fullContentAdmitted ? message.text : null,
          admissionReasons: [...admission.reasons],
          bodyRetrieval: admission.bodyRetrieval,
          attachments: message.attachments.map((attachment) => ({
            partId: attachment.partId,
            filename: attachment.filename,
            mimeType: attachment.mimeType,
            size: attachment.size,
            inline: attachment.inline,
          })),
        }),
      ),
    );
    const capturedAt = new Date();
    const ingested = await this.#sources.apply({
      kind: "ingest_source",
      integrationId: payload.integrationId,
      expectedIntegrationControlEpoch: payload.integrationControlEpoch,
      artifactKind: "mail_message",
      origin: { system: "gmail", remoteObjectId: message.id, remoteRevisionId: message.historyId },
      correlationDigest,
      scope: { kind: "person", personId: payload.personId },
      content,
      occurredAt: message.internalDate.toISOString(),
      capturedAt: capturedAt.toISOString(),
      requestedRetentionUntil: new Date(
        capturedAt.getTime() + this.config.defaults.rawSourceRetentionDays * 86_400_000,
      ).toISOString(),
    });
    if (ingested.kind !== "source_ingested") {
      throw new Error("Gmail source ingestion did not produce a revision");
    }
    return ingested;
  }

  private async markGmailMessageDeleted(
    payload: z.infer<typeof GmailMessagePayloadSchema>,
    messageId: string,
    correlationDigest?: string,
  ): Promise<void> {
    await this.#sources.apply({
      kind: "mark_source_deleted",
      integrationId: payload.integrationId,
      expectedIntegrationControlEpoch: payload.integrationControlEpoch,
      artifactKind: "mail_message",
      origin: { system: "gmail", remoteObjectId: messageId },
      ...(correlationDigest ? { correlationDigest } : {}),
      scope: { kind: "person", personId: payload.personId },
      deletedAt: new Date().toISOString(),
    });
  }

  private async ingestGmailAttachment(
    gmail: GmailAdapter,
    message: NormalizedGmailMessage,
    parentSourceRevisionId: string,
    correlationDigest: string,
    payload: z.infer<typeof GmailMessagePayloadSchema>,
    attachment: NormalizedGmailMessage["attachments"][number],
  ): Promise<void> {
    const bytes = await gmail.attachment(message.id, attachment);
    let extracted: Awaited<ReturnType<typeof extractDocument>> | null = null;
    try {
      extracted = await extractDocument(bytes, attachment.mimeType);
    } catch {
      // Keep a bounded encrypted manifest/blob even when no extractor supports the format.
    }
    const capturedAt = new Date();
    const detectedMime = extracted?.detectedMime ?? attachment.mimeType;
    const ingested = await this.#sources.apply({
      kind: "ingest_source",
      integrationId: payload.integrationId,
      expectedIntegrationControlEpoch: payload.integrationControlEpoch,
      artifactKind: "attachment_manifest",
      origin: {
        system: "gmail.attachment",
        remoteObjectId: `${message.id}:${attachment.partId}`,
        remoteRevisionId: message.historyId,
      },
      correlationDigest,
      scope: { kind: "person", personId: payload.personId },
      content: JsonObjectSchema.parse({
        parentSourceRevisionId,
        partId: attachment.partId,
        filename: attachment.filename,
        declaredMime: attachment.mimeType,
        detectedMime,
        size: attachment.size,
        inline: attachment.inline,
        kind: extracted?.kind ?? "unsupported",
        text: extracted?.text ?? "",
        metadata: extracted?.metadata ?? { bytes: bytes.length, unsupportedExtraction: true },
        untrustedEvidence: true,
      }),
      occurredAt: message.internalDate.toISOString(),
      capturedAt: capturedAt.toISOString(),
      requestedRetentionUntil: new Date(
        capturedAt.getTime() + this.config.defaults.rawSourceRetentionDays * 86_400_000,
      ).toISOString(),
    });
    if (ingested.kind !== "source_ingested") return;
    await this.#sources.apply({
      kind: "store_blob",
      sourceRevisionId: ingested.sourceRevisionId,
      scope: { kind: "person", personId: payload.personId },
      integrationId: payload.integrationId,
      expectedIntegrationControlEpoch: payload.integrationControlEpoch,
      blobKind: `gmail_attachment:${attachment.partId}`,
      mimeType: detectedMime,
      bytes: new Uint8Array(bytes),
      storedAt: capturedAt.toISOString(),
    });
    if (extracted) {
      await this.#sources.apply({
        kind: "store_derivative",
        sourceRevisionId: ingested.sourceRevisionId,
        scope: { kind: "person", personId: payload.personId },
        integrationId: payload.integrationId,
        expectedIntegrationControlEpoch: payload.integrationControlEpoch,
        derivativeKind: `attachment_text:${attachment.partId}`,
        content: JsonObjectSchema.parse({
          filename: attachment.filename,
          detectedMime: extracted.detectedMime,
          kind: extracted.kind,
          text: extracted.text,
          metadata: extracted.metadata,
          untrustedEvidence: true,
        }),
        requestedRetentionUntil: new Date(
          Date.now() + this.config.defaults.rawSourceRetentionDays * 86_400_000,
        ).toISOString(),
        createdAt: new Date().toISOString(),
      });
    }
  }

  private async ingestGmailAttachmentOmission(
    message: NormalizedGmailMessage,
    parentSourceRevisionId: string,
    correlationDigest: string,
    payload: z.infer<typeof GmailMessagePayloadSchema>,
    attachment: NormalizedGmailMessage["attachments"][number],
  ): Promise<void> {
    const capturedAt = new Date();
    await this.#sources.apply({
      kind: "ingest_source",
      integrationId: payload.integrationId,
      expectedIntegrationControlEpoch: payload.integrationControlEpoch,
      artifactKind: "attachment_manifest",
      origin: {
        system: "gmail.attachment",
        remoteObjectId: `${message.id}:${attachment.partId}`,
        remoteRevisionId: message.historyId,
      },
      correlationDigest,
      scope: { kind: "person", personId: payload.personId },
      content: JsonObjectSchema.parse({
        parentSourceRevisionId,
        partId: attachment.partId,
        filename: attachment.filename,
        declaredMime: attachment.mimeType,
        size: attachment.size,
        inline: attachment.inline,
        kind: "omitted",
        text: "",
        metadata: {
          bytes: attachment.size,
          omissionReason: "attachment_exceeds_15_mib_processing_limit",
        },
        untrustedEvidence: true,
      }),
      occurredAt: message.internalDate.toISOString(),
      capturedAt: capturedAt.toISOString(),
      requestedRetentionUntil: new Date(
        capturedAt.getTime() + this.config.defaults.rawSourceRetentionDays * 86_400_000,
      ).toISOString(),
    });
  }
}

export function buildGmailMessageJobPayload(
  payload: GoogleJobBase,
  messageId: string,
  sourcePriority: number,
): z.input<typeof GmailMessagePayloadSchema> {
  return GmailMessagePayloadSchema.parse({
    ...basePayload(payload),
    messageId,
    sourcePriority,
  });
}

function basePayload(payload: GoogleJobBase): GoogleJobBase {
  return {
    integrationId: payload.integrationId,
    personId: payload.personId,
    integrationControlEpoch: payload.integrationControlEpoch,
    personControlEpoch: payload.personControlEpoch,
  };
}

function googleJobFence(payload: GoogleJobBase) {
  return {
    person: { id: payload.personId, controlEpoch: payload.personControlEpoch },
    integration: { id: payload.integrationId, controlEpoch: payload.integrationControlEpoch },
  } as const;
}

function privateGmailAccountStatus(
  account: PrivateGmailAccountRow,
  cursors: readonly PrivateGmailCursorRow[],
  jobs: readonly PrivateGmailJobRow[],
  search: PrivateQuestionContext["accounts"][number]["search"],
): PrivateQuestionContext["accounts"][number] {
  const cursorByResource = new Map(cursors.map((cursor) => [cursor.resource_kind, cursor] as const));
  const cursor = (resourceKind: string) => cursorByResource.get(resourceKind);
  const exhausted = (stage: string) => cursor(`gmail_backfill:${stage}`)?.state === "exhausted";
  const cursorNeedsRecovery = (resourceKind: string) =>
    ["error", "expired"].includes(cursor(resourceKind)?.state ?? "");
  const latestJob = (predicate: (job: PrivateGmailJobRow) => boolean) =>
    jobs.filter(predicate).sort((left, right) => right.updated_at.getTime() - left.updated_at.getTime())[0];
  const jobNeedsRecovery = (job: PrivateGmailJobRow | undefined, resourceKind: string) => {
    if (!job || !["attention", "dead"].includes(job.status)) return false;
    const currentCursor = cursor(resourceKind);
    return !currentCursor || job.updated_at >= currentCursor.updated_at;
  };
  const failedBackfill = (stage: string) =>
    jobNeedsRecovery(
      latestJob(
        (job) => job.job_kind === "google.gmail.backfill" && job.idempotency_key.includes(`:${stage}:`),
      ),
      `gmail_backfill:${stage}`,
    );
  const recentComplete = exhausted("newest_30_days");
  const recentRecovering =
    cursorNeedsRecovery("gmail_backfill:newest_30_days") || failedBackfill("newest_30_days");
  const olderStages = ["days_31_to_90", "days_91_to_365", "older_history"] as const;
  const olderComplete = olderStages.every(exhausted);
  const olderRecovering = olderStages.some(
    (stage) => cursorNeedsRecovery(`gmail_backfill:${stage}`) || failedBackfill(stage),
  );
  const liveCursor = cursor("gmail_history");
  const liveJobFailed = jobNeedsRecovery(
    latestJob((job) => job.job_kind === "google.gmail.poll"),
    "gmail_history",
  );
  return {
    accountKind: account.account_kind,
    connection: account.status,
    liveMonitoring:
      account.status !== "active"
        ? "unavailable"
        : liveJobFailed
          ? "unavailable"
          : liveCursor?.state === "active"
            ? "watching"
            : "starting",
    recentImport: recentComplete ? "complete" : recentRecovering ? "recovering" : "importing",
    olderHistoryImport: olderComplete ? "complete" : olderRecovering ? "recovering" : "importing",
    search,
  };
}

function controlEpochKey(payload: GoogleJobBase): string {
  return `e${payload.integrationControlEpoch}`;
}

function calendarPolicyLockKey(payload: Pick<CalendarPollPayload, "integrationId" | "calendarIdDigest">) {
  return `calendar-privacy:${payload.integrationId}:${payload.calendarIdDigest}`;
}

function calendarPollKeyPrefix(payload: CalendarPollPayload): string {
  return `calendar:poll:${payload.integrationId}:${controlEpochKey(payload)}:${payload.calendarIdDigest}:v${payload.grantVersion}:${payload.mode}:`;
}

function calendarPollKey(payload: CalendarPollPayload, occurrence: string): string {
  return `${calendarPollKeyPrefix(payload)}${occurrence}`;
}

function backfillPriority(
  stage: "live" | "newest_30_days" | "days_31_to_90" | "days_91_to_365" | "older_history",
): number {
  switch (stage) {
    case "live":
      return GOOGLE_JOB_PRIORITY.live;
    case "newest_30_days":
      return GOOGLE_JOB_PRIORITY.recentBackfill;
    case "days_31_to_90":
      return GOOGLE_JOB_PRIORITY.middleBackfill;
    case "days_91_to_365":
      return GOOGLE_JOB_PRIORITY.yearBackfill;
    case "older_history":
      return GOOGLE_JOB_PRIORITY.olderHistory;
  }
}

function defaultPrimaryCalendarMode(
  accountKind: IntegrationAccountKind,
): Exclude<CalendarPrivacyMode, "off"> {
  return accountKind === "personal_family" ? "full_private" : "availability_only";
}

function googleJobCapability(jobKind: string): IntegrationCapability | null {
  if (jobKind.startsWith("google.gmail.")) return "mail";
  if (jobKind.startsWith("google.calendar.")) return "calendar";
  return null;
}

function gmailDateQuery(afterExclusive: string | null, beforeOrEqual: string | null): string {
  const parts: string[] = [];
  if (afterExclusive) parts.push(`after:${gmailDate(afterExclusive)}`);
  if (beforeOrEqual)
    parts.push(`before:${gmailDate(new Date(new Date(beforeOrEqual).getTime() + 86_400_000).toISOString())}`);
  return parts.join(" ");
}

function gmailDate(instant: string): string {
  return new Date(instant).toISOString().slice(0, 10).replaceAll("-", "/");
}

function compareGmailMessageRecency(
  left: Pick<NormalizedGmailMessage, "internalDate"> & { readonly messageId: string },
  right: Pick<NormalizedGmailMessage, "internalDate"> & { readonly messageId: string },
): number {
  const timeDifference = left.internalDate.getTime() - right.internalDate.getTime();
  return timeDifference === 0 ? left.messageId.localeCompare(right.messageId) : timeDifference;
}

function googleErrorStatus(error: unknown): number | undefined {
  if (typeof error !== "object" || error === null) return undefined;
  if ("code" in error && typeof error.code === "number") return error.code;
  if ("code" in error && typeof error.code === "string" && /^\d{3}$/u.test(error.code)) {
    return Number(error.code);
  }
  if (
    "response" in error &&
    typeof error.response === "object" &&
    error.response !== null &&
    "status" in error.response
  ) {
    return typeof error.response.status === "number" ? error.response.status : undefined;
  }
  return undefined;
}

function isGoogleProviderAuthFailure(error: unknown): boolean {
  if (googleErrorStatus(error) === 401) return true;
  const markers = googleErrorMarkers(error);
  return markers.some((marker) =>
    /(?:^|[^a-z])(?:invalid_grant|invalid_client|unauthorized_client|autherror)(?:[^a-z]|$)/iu.test(marker),
  );
}

function isRetryableGoogleError(error: unknown): boolean {
  const status = googleErrorStatus(error);
  return status === 408 || status === 429 || (status !== undefined && status >= 500);
}

function googleErrorMarkers(error: unknown): string[] {
  if (!isRecord(error)) return [];
  const markers: string[] = [];
  for (const value of [error.code, error.message, error.error, error.error_description]) {
    if (typeof value === "string") markers.push(value);
  }
  if (!isRecord(error.response) || !isRecord(error.response.data)) return markers;
  for (const value of [error.response.data.error, error.response.data.error_description]) {
    if (typeof value === "string") markers.push(value);
  }
  const nestedErrors = error.response.data.errors;
  if (Array.isArray(nestedErrors)) {
    for (const nested of nestedErrors) {
      if (!isRecord(nested)) continue;
      for (const value of [nested.reason, nested.message]) {
        if (typeof value === "string") markers.push(value);
      }
    }
  }
  return markers;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
