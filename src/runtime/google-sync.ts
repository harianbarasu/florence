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
  type IntegrationAccountKind,
  type IntegrationCapability,
  isFullMailContentAdmitted,
  JsonObjectSchema,
  PostgresSourceIntelligence,
  planCalendarSyncWindow,
  planNewestFirstMailBackfill,
  projectCalendarArtifact,
  type SourceReadResult,
} from "../modules/sources/index.js";
import { DurableWork } from "../modules/work/index.js";
import type { SecretBox } from "../shared/crypto.js";
import { NotFoundError, StaleAuthorityError, UnauthorizedError } from "../shared/errors.js";

const GOOGLE_JOB_PRIORITY = {
  activation: 50,
  live: 50,
  calendar: 60,
  calendarBackfill: 110,
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
    let metadata: NormalizedGmailMessage;
    try {
      metadata = await gmail.message(payload.messageId, true);
    } catch (error) {
      if (googleErrorStatus(error) === 404) {
        await this.#sources.apply({
          kind: "mark_source_deleted",
          integrationId: payload.integrationId,
          expectedIntegrationControlEpoch: payload.integrationControlEpoch,
          artifactKind: "mail_message",
          origin: { system: "gmail", remoteObjectId: payload.messageId },
          scope: { kind: "person", personId: payload.personId },
          deletedAt: new Date().toISOString(),
        });
        return null;
      }
      throw error;
    }
    const admission = assessMailMetadata({
      labelIds: metadata.labelIds,
      from: metadata.from,
      subject: metadata.subject,
      snippet: metadata.snippet,
      hasAttachments: metadata.attachments.length > 0,
    });
    if (!admission.ingestMetadata) return null;
    const fullContentAdmitted = isFullMailContentAdmitted(admission);
    const message = fullContentAdmitted ? await gmail.message(payload.messageId, false) : metadata;
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
    const ingested = await this.#sources.apply({
      kind: "ingest_source",
      integrationId: payload.integrationId,
      expectedIntegrationControlEpoch: payload.integrationControlEpoch,
      artifactKind: "mail_message",
      origin: { system: "gmail", remoteObjectId: payload.messageId, remoteRevisionId: message.historyId },
      scope: { kind: "person", personId: payload.personId },
      content,
      occurredAt: message.internalDate.toISOString(),
      capturedAt: new Date().toISOString(),
      requestedRetentionUntil: new Date(
        Date.now() + this.config.defaults.rawSourceRetentionDays * 86_400_000,
      ).toISOString(),
    });
    if (ingested.kind !== "source_ingested") return null;
    if (fullContentAdmitted) {
      for (const attachment of message.attachments.filter(
        (entry) => !entry.inline && entry.size <= 15 * 1024 * 1024,
      )) {
        await this.ingestGmailAttachment(gmail, message, ingested.sourceRevisionId, payload, attachment);
      }
    }
    if (!fullContentAdmitted) return ingested.sourceRevisionId;
    await this.#work.enqueue({
      kind: "orchestrate.private_source",
      idempotencyKey: `orchestrate:source:${ingested.sourceRevisionId}:${controlEpochKey(payload)}`,
      payload: {
        sourceRevisionId: ingested.sourceRevisionId,
        personId: payload.personId,
        integrationId: payload.integrationId,
        integrationControlEpoch: payload.integrationControlEpoch,
      },
      ...googleJobFence(payload),
      priority: payload.sourcePriority,
      maxAttempts: 8,
    });
    return ingested.sourceRevisionId;
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
      if (cursor.kind === "sync_cursor") expectedCursorUpdate = cursor.updatedAt;
    } catch (error) {
      if (!(error instanceof NotFoundError)) throw error;
    }
    const window = planCalendarSyncWindow(new Date().toISOString());
    const interpretationPriority = syncToken
      ? GOOGLE_JOB_PRIORITY.calendar
      : GOOGLE_JOB_PRIORITY.calendarBackfill;
    let pageToken: string | undefined;
    let nextSyncToken: string | undefined;
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
          if (event.status === "cancelled") {
            await this.withCalendarPollAuthority(payload, async ({ sources }) => {
              await sources.apply({
                kind: "mark_source_deleted",
                integrationId: payload.integrationId,
                expectedIntegrationControlEpoch: payload.integrationControlEpoch,
                artifactKind: "calendar_event",
                origin,
                scope: { kind: "person", personId: payload.personId },
                deletedAt: new Date().toISOString(),
              });
            });
            continue;
          }
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
          await this.withCalendarPollAuthority(payload, async ({ sources, work }) => {
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
            if (ingested.kind === "source_ingested") {
              await work.enqueue({
                kind: "orchestrate.private_source",
                idempotencyKey: `orchestrate:source:${ingested.sourceRevisionId}:${controlEpochKey(payload)}`,
                payload: {
                  sourceRevisionId: ingested.sourceRevisionId,
                  personId: payload.personId,
                  integrationId: payload.integrationId,
                  integrationControlEpoch: payload.integrationControlEpoch,
                },
                ...googleJobFence(payload),
                priority: interpretationPriority,
                maxAttempts: 8,
              });
            }
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
    await this.withCalendarPollAuthority(payload, async ({ sources }) => {
      await sources.apply({
        kind: "checkpoint_cursor",
        integrationId: payload.integrationId,
        personId: payload.personId,
        expectedIntegrationControlEpoch: payload.integrationControlEpoch,
        resourceKind: cursorKind,
        cursor: nextSyncToken ? { syncToken: nextSyncToken } : null,
        state: nextSyncToken ? "active" : "initial",
        expectedUpdatedAt: expectedCursorUpdate,
        checkpointAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      });
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
      payload: { ...payload, messageId, sourcePriority: priority },
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

  private async ingestGmailAttachment(
    gmail: GmailAdapter,
    message: NormalizedGmailMessage,
    parentSourceRevisionId: string,
    payload: z.infer<typeof GmailMessagePayloadSchema>,
    attachment: NormalizedGmailMessage["attachments"][number],
  ): Promise<void> {
    const bytes = await gmail.attachment(message.id, attachment.attachmentId);
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
      scope: { kind: "person", personId: payload.personId },
      content: JsonObjectSchema.parse({
        parentSourceRevisionId,
        filename: attachment.filename,
        declaredMime: attachment.mimeType,
        detectedMime,
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
    await this.#work.enqueue({
      kind: "orchestrate.private_source",
      idempotencyKey: `orchestrate:source:${ingested.sourceRevisionId}:${controlEpochKey(payload)}`,
      payload: {
        sourceRevisionId: ingested.sourceRevisionId,
        personId: payload.personId,
        integrationId: payload.integrationId,
        integrationControlEpoch: payload.integrationControlEpoch,
      },
      ...googleJobFence(payload),
      priority: payload.sourcePriority,
      maxAttempts: 8,
    });
  }
}

function basePayload(payload: z.infer<typeof GoogleBootstrapPayloadSchema>): GoogleJobBase {
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
