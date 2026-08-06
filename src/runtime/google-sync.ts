import { createHash } from "node:crypto";
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
  isFullMailContentAdmitted,
  JsonObjectSchema,
  PostgresSourceIntelligence,
  planCalendarSyncWindow,
  planNewestFirstMailBackfill,
  projectCalendarArtifact,
} from "../modules/sources/index.js";
import { DurableWork } from "../modules/work/index.js";
import type { SecretBox } from "../shared/crypto.js";

const GoogleJobBaseSchema = z.strictObject({
  integrationId: z.string().uuid(),
  personId: z.string().uuid(),
  integrationControlEpoch: z.number().int().positive(),
  personControlEpoch: z.number().int().positive(),
});

export const GoogleBootstrapPayloadSchema = z.strictObject({
  integrationId: z.string().uuid(),
  personId: z.string().uuid(),
  integrationControlEpoch: z.number().int().positive(),
  personControlEpoch: z.number().int().positive(),
  olderHistoryEnabled: z.boolean().default(false),
});

export const GmailPollPayloadSchema = GoogleJobBaseSchema;
export const GmailMessagePayloadSchema = GoogleJobBaseSchema.extend({
  messageId: z.string().min(1).max(500),
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
});

type GoogleJobBase = z.infer<typeof GoogleJobBaseSchema>;

export class GoogleSyncService {
  readonly #database: Database;
  readonly #sources: PostgresSourceIntelligence;
  readonly #work: DurableWork;
  readonly #oauth: GoogleOAuthAdapter;

  public constructor(
    database: Database,
    private readonly config: FlorenceConfig,
    secretBox: SecretBox,
  ) {
    this.#database = database;
    this.#sources = new PostgresSourceIntelligence(database, secretBox, {
      rawRetentionDays: config.defaults.rawSourceRetentionDays,
      privateCandidateRetentionDays: 7,
    });
    this.#work = new DurableWork(database, secretBox);
    this.#oauth = new GoogleOAuthAdapter(config.google);
  }

  public async bootstrap(payloadCandidate: unknown): Promise<void> {
    const payload = GoogleBootstrapPayloadSchema.parse(payloadCandidate);
    const { gmail } = await this.clients(payload);
    const profile = await gmail.profile();
    await this.#sources.apply({
      kind: "checkpoint_cursor",
      integrationId: payload.integrationId,
      personId: payload.personId,
      expectedIntegrationControlEpoch: payload.integrationControlEpoch,
      resourceKind: "gmail_history",
      cursor: { historyId: profile.historyId },
      state: "active",
      expectedUpdatedAt: null,
      checkpointAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });
    await this.#work.enqueue({
      kind: "google.gmail.poll",
      idempotencyKey: `gmail:poll:${payload.integrationId}:${profile.historyId}`,
      payload: basePayload(payload),
      person: { id: payload.personId, controlEpoch: payload.personControlEpoch },
      maxAttempts: 8,
    });
    for (const stage of planNewestFirstMailBackfill({
      asOf: new Date().toISOString(),
      olderHistoryEnabled: payload.olderHistoryEnabled,
    }).filter((entry) => entry.kind !== "live")) {
      await this.#work.enqueue({
        kind: "google.gmail.backfill",
        idempotencyKey: `gmail:backfill:${payload.integrationId}:${stage.kind}:start`,
        payload: {
          ...basePayload(payload),
          stage: stage.kind,
          afterExclusive: stage.afterExclusive,
          beforeOrEqual: stage.beforeOrEqual,
          pageToken: null,
          runKey: "initial",
        },
        person: { id: payload.personId, controlEpoch: payload.personControlEpoch },
        availableAt: new Date(Date.now() + stage.priority * 1_000),
        maxAttempts: 8,
      });
    }
    await this.#work.enqueue({
      kind: "google.calendar.catalog",
      idempotencyKey: `calendar:catalog:${payload.integrationId}:${payload.integrationControlEpoch}`,
      payload: basePayload(payload),
      person: { id: payload.personId, controlEpoch: payload.personControlEpoch },
      maxAttempts: 8,
    });
  }

  public async pollGmail(payloadCandidate: unknown): Promise<void> {
    const payload = GmailPollPayloadSchema.parse(payloadCandidate);
    const { gmail } = await this.clients(payload);
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
        await this.#sources.apply({
          kind: "checkpoint_cursor",
          integrationId: payload.integrationId,
          personId: payload.personId,
          expectedIntegrationControlEpoch: payload.integrationControlEpoch,
          resourceKind: "gmail_history",
          cursor: null,
          state: "expired",
          expectedUpdatedAt: expectedCursorUpdate,
          checkpointAt: null,
          updatedAt: new Date().toISOString(),
        });
      }
      throw error;
    }
    for (const messageId of messageIds) {
      await this.enqueueGmailMessage(payload, messageId, `history:${newestHistoryId}`);
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
      idempotencyKey: `gmail:poll:${payload.integrationId}:t${bucket}`,
      payload,
      person: { id: payload.personId, controlEpoch: payload.personControlEpoch },
      availableAt: new Date(Date.now() + this.config.intervals.gmailPollMs),
      maxAttempts: 8,
    });
  }

  private async enqueueRecoveryBackfills(payload: GoogleJobBase): Promise<void> {
    const runKey = `recovery-${Math.floor(Date.now() / 86_400_000)}`;
    for (const stage of planNewestFirstMailBackfill({
      asOf: new Date().toISOString(),
      olderHistoryEnabled: false,
    }).filter((entry) => entry.kind !== "live")) {
      await this.#work.enqueue({
        kind: "google.gmail.backfill",
        idempotencyKey: `gmail:${runKey}:${payload.integrationId}:${stage.kind}:start`,
        payload: {
          ...payload,
          stage: stage.kind,
          afterExclusive: stage.afterExclusive,
          beforeOrEqual: stage.beforeOrEqual,
          pageToken: null,
          runKey,
        },
        person: { id: payload.personId, controlEpoch: payload.personControlEpoch },
        maxAttempts: 8,
      });
    }
  }

  public async backfillGmail(payloadCandidate: unknown): Promise<void> {
    const payload = GmailBackfillPayloadSchema.parse(payloadCandidate);
    const { gmail } = await this.clients(payload);
    const page = await gmail.listMessages(
      gmailDateQuery(payload.afterExclusive, payload.beforeOrEqual),
      payload.pageToken ?? undefined,
    );
    for (const messageId of page.messageIds) {
      await this.enqueueGmailMessage(payload, messageId, `backfill:${payload.runKey}`);
    }
    if (page.nextPageToken) {
      await this.#work.enqueue({
        kind: "google.gmail.backfill",
        idempotencyKey: `gmail:backfill:${payload.integrationId}:${payload.stage}:${payload.runKey}:${sha256Hex(page.nextPageToken)}`,
        payload: { ...payload, pageToken: page.nextPageToken },
        person: { id: payload.personId, controlEpoch: payload.personControlEpoch },
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
    const { gmail } = await this.clients(payload);
    let metadata: NormalizedGmailMessage;
    try {
      metadata = await gmail.message(payload.messageId, true);
    } catch (error) {
      if (googleErrorStatus(error) === 404) {
        await this.#sources.apply({
          kind: "mark_source_deleted",
          integrationId: payload.integrationId,
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
    if (fullContentAdmitted && !ingested.duplicate) {
      for (const attachment of message.attachments.filter(
        (entry) => !entry.inline && entry.size <= 15 * 1024 * 1024,
      )) {
        await this.ingestGmailAttachment(gmail, message, ingested.sourceRevisionId, payload, attachment);
      }
    }
    if (!fullContentAdmitted) return ingested.sourceRevisionId;
    await this.#work.enqueue({
      kind: "orchestrate.private_source",
      idempotencyKey: `orchestrate:source:${ingested.sourceRevisionId}`,
      payload: { sourceRevisionId: ingested.sourceRevisionId, personId: payload.personId },
      person: { id: payload.personId, controlEpoch: payload.personControlEpoch },
      maxAttempts: 5,
      deadlineAt: new Date(Date.now() + 10 * 60_000),
    });
    return ingested.sourceRevisionId;
  }

  public async catalogCalendars(payloadCandidate: unknown): Promise<void> {
    const payload = CalendarCatalogPayloadSchema.parse(payloadCandidate);
    const { calendar } = await this.clients(payload);
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
          await this.enqueueCalendarPoll(payload, entry.id, digest, policy.mode);
        }
      } catch {
        // Missing policy is a deliberate default-off state surfaced in the control plane.
      }
    }
    const bucket = Math.floor((Date.now() + 24 * 60 * 60_000) / (24 * 60 * 60_000));
    await this.#work.enqueue({
      kind: "google.calendar.catalog",
      idempotencyKey: `calendar:catalog:${payload.integrationId}:d${bucket}`,
      payload,
      person: { id: payload.personId, controlEpoch: payload.personControlEpoch },
      availableAt: new Date(Date.now() + 24 * 60 * 60_000),
      maxAttempts: 8,
    });
  }

  public async pollCalendar(payloadCandidate: unknown): Promise<void> {
    const payload = CalendarPollPayloadSchema.parse(payloadCandidate);
    const { calendar } = await this.clients(payload);
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
    } catch {
      syncToken = undefined;
    }
    const window = planCalendarSyncWindow(new Date().toISOString());
    let pageToken: string | undefined;
    let nextSyncToken: string | undefined;
    try {
      do {
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
            await this.#sources.apply({
              kind: "mark_source_deleted",
              integrationId: payload.integrationId,
              artifactKind: "calendar_event",
              origin,
              scope: { kind: "person", personId: payload.personId },
              deletedAt: new Date().toISOString(),
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
          const ingested = await this.#sources.apply({
            kind: "ingest_source",
            integrationId: payload.integrationId,
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
            await this.#work.enqueue({
              kind: "orchestrate.private_source",
              idempotencyKey: `orchestrate:source:${ingested.sourceRevisionId}`,
              payload: { sourceRevisionId: ingested.sourceRevisionId, personId: payload.personId },
              person: { id: payload.personId, controlEpoch: payload.personControlEpoch },
              maxAttempts: 5,
            });
          }
        }
        pageToken = page.nextPageToken;
        nextSyncToken = page.nextSyncToken ?? nextSyncToken;
      } while (pageToken);
    } catch (error) {
      if (googleErrorStatus(error) === 410) {
        await this.#sources.apply({
          kind: "checkpoint_cursor",
          integrationId: payload.integrationId,
          personId: payload.personId,
          expectedIntegrationControlEpoch: payload.integrationControlEpoch,
          resourceKind: cursorKind,
          cursor: null,
          state: "expired",
          expectedUpdatedAt: expectedCursorUpdate,
          checkpointAt: null,
          updatedAt: new Date().toISOString(),
        });
      }
      throw error;
    }
    await this.#sources.apply({
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
    const bucket = Math.floor(
      (Date.now() + this.config.intervals.calendarPollMs) / this.config.intervals.calendarPollMs,
    );
    await this.#work.enqueue({
      kind: "google.calendar.poll",
      idempotencyKey: `calendar:poll:${payload.integrationId}:${payload.calendarIdDigest}:t${bucket}`,
      payload,
      person: { id: payload.personId, controlEpoch: payload.personControlEpoch },
      availableAt: new Date(Date.now() + this.config.intervals.calendarPollMs),
      maxAttempts: 8,
    });
  }

  private async clients(payload: GoogleJobBase) {
    const access = await this.#sources.read({
      kind: "integration_access",
      integrationId: payload.integrationId,
      personId: payload.personId,
      expectedControlEpoch: payload.integrationControlEpoch,
    });
    if (access.kind !== "integration_access") throw new Error("Google integration is not accessible");
    const credentials = access.credentials as GoogleCredentials;
    const oauthClient = this.#oauth.client(credentials);
    return { gmail: new GmailAdapter(oauthClient), calendar: new CalendarAdapter(oauthClient) };
  }

  private async enqueueGmailMessage(
    payload: GoogleJobBase,
    messageId: string,
    observationKey: string,
  ): Promise<void> {
    await this.#work.enqueue({
      kind: "google.gmail.message",
      idempotencyKey: `gmail:message:${payload.integrationId}:${messageId}:${sha256Hex(observationKey)}`,
      payload: { ...payload, messageId },
      person: { id: payload.personId, controlEpoch: payload.personControlEpoch },
      maxAttempts: 8,
    });
  }

  private async enqueueCalendarPoll(
    payload: GoogleJobBase,
    calendarId: string,
    calendarIdDigest: string,
    mode: Exclude<CalendarPrivacyMode, "off">,
  ): Promise<void> {
    const live = await this.#database<{ readonly present: boolean }[]>`
      select exists(
        select 1 from jobs
        where person_id = ${payload.personId}
          and job_kind = 'google.calendar.poll'
          and idempotency_key like ${`calendar:poll:${payload.integrationId}:${calendarIdDigest}:%`}
          and status in ('pending', 'retry', 'leased')
      ) as present
    `;
    if (live[0]?.present) return;
    const recoveryBucket = Math.floor(Date.now() / 60_000);
    await this.#work.enqueue({
      kind: "google.calendar.poll",
      idempotencyKey: `calendar:poll:${payload.integrationId}:${calendarIdDigest}:seed${recoveryBucket}`,
      payload: { ...payload, calendarId, calendarIdDigest, mode },
      person: { id: payload.personId, controlEpoch: payload.personControlEpoch },
      maxAttempts: 8,
    });
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
    if (!ingested.duplicate) {
      await this.#work.enqueue({
        kind: "orchestrate.private_source",
        idempotencyKey: `orchestrate:source:${ingested.sourceRevisionId}`,
        payload: { sourceRevisionId: ingested.sourceRevisionId, personId: payload.personId },
        person: { id: payload.personId, controlEpoch: payload.personControlEpoch },
        maxAttempts: 5,
        deadlineAt: new Date(Date.now() + 10 * 60_000),
      });
    }
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
