import { randomUUID } from "node:crypto";
import path from "node:path";
import { z } from "zod";
import {
  LinqApiError,
  LinqAttachmentError,
  LinqAudienceChangedError,
  LinqClient,
  type LinqConfig,
} from "./adapters/linq/index.js";
import { FlorenceApplication } from "./application/index.js";
import { loadConfig } from "./config.js";
import { createDatabase } from "./db/client.js";
import { PostgresWebAuth } from "./modules/auth/index.js";
import { PostgresConversationAuthority } from "./modules/conversations/index.js";
import { CoordinationError } from "./modules/coordination/index.js";
import { EffectOutbox, LinqMessageEffectExecutor } from "./modules/effects/index.js";
import { BoundedWorkerRuntime } from "./modules/orchestration/bounded-worker-runtime.js";
import { LangChainModelGateway } from "./modules/orchestration/langchain-model-gateway.js";
import { PostgresSourceIntelligence } from "./modules/sources/index.js";
import { type ClaimedJob, DurableTimers, DurableWork } from "./modules/work/index.js";
import {
  CalendarCatalogPayloadSchema,
  CalendarPollPayloadSchema,
  GmailBackfillPayloadSchema,
  GmailBootstrapPayloadSchema,
  GmailMessagePayloadSchema,
  GmailPollPayloadSchema,
  GoogleBootstrapPayloadSchema,
  GoogleSyncService,
} from "./runtime/google-sync.js";
import { bootstrapGovernedSkills, GovernedWorkerRuntime } from "./runtime/governed-worker-runtime.js";
import { FlorenceOrchestrator } from "./runtime/orchestrator.js";
import { dispatchTimerProcessJob, TimerRuntime } from "./runtime/timer-runtime.js";
import { SecretBox } from "./shared/crypto.js";
import { ConflictError, NotFoundError, StaleAuthorityError, UnauthorizedError } from "./shared/errors.js";

const WORKER_ADVISORY_LOCK = 4_607_346_623;
const WORKER_LEASE_NAME = "florence-worker-singleton";
const WORKER_LEASE_WAIT_MS = 45_000;
const WORKER_HEARTBEAT_MS = 10_000;
const GOOGLE_JOB_REDRIVE_LIMIT = 20;
const GOOGLE_JOB_REDRIVE_LOOKBACK_MS = 7 * 24 * 60 * 60_000;
const GOOGLE_JOB_REDRIVE_BUCKET_MS = 60 * 60_000;
const GOOGLE_JOB_REDRIVE_MAX_GENERATIONS = 3;

const StepUpPayloadSchema = z.strictObject({
  actorPersonId: z.string().uuid(),
  purpose: z.enum(["account_controls", "google_connect"]),
});
const OrchestrateMessagePayloadSchema = z.strictObject({ internalProviderEventId: z.string().uuid() });
const PrivateSourcePayloadSchema = z.strictObject({
  sourceRevisionId: z.string().uuid(),
  personId: z.string().uuid(),
  integrationId: z.string().uuid(),
  integrationControlEpoch: z.number().int().positive(),
});
const PrivateBridgePayloadSchema = z.strictObject({ actionIntentId: z.string().uuid() });

async function main(): Promise<void> {
  const config = loadConfig();
  const database = createDatabase(config, "florence-worker");
  const secretBox = new SecretBox(config.security.activeDataKeyId, config.security.dataKeyringJson);
  const workerId = `florence-worker:${process.pid}:${randomUUID()}`;
  let running = true;
  const stop = () => {
    running = false;
  };
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  const reserved = await database.reserve();
  let acquired = false;
  const leaseDeadline = Date.now() + WORKER_LEASE_WAIT_MS;
  while (running && Date.now() < leaseDeadline) {
    const locks = await reserved<{ acquired: boolean }[]>`
      select pg_try_advisory_lock(${WORKER_ADVISORY_LOCK}) as acquired
    `;
    if (locks[0]?.acquired) {
      acquired = true;
      break;
    }
    await wait(1_000);
  }
  if (!acquired) {
    reserved.release();
    await database.end();
    process.off("SIGTERM", stop);
    process.off("SIGINT", stop);
    if (!running) return;
    throw new Error("Another Florence worker owns the singleton lease");
  }

  let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  let heartbeatInFlight: Promise<void> | null = null;
  try {
    await recordWorkerStart(database, workerId, workerReleaseId());
    heartbeatTimer = setInterval(() => {
      if (heartbeatInFlight) return;
      heartbeatInFlight = recordWorkerHeartbeat(database, workerId)
        .catch(() => {
          process.stderr.write(
            `${JSON.stringify({ level: "error", errorCode: "worker_heartbeat_failed" })}\n`,
          );
        })
        .finally(() => {
          heartbeatInFlight = null;
        });
    }, WORKER_HEARTBEAT_MS);
    heartbeatTimer.unref();

    const linqConfig: LinqConfig = {
      apiKey: config.linq.apiKey,
      baseUrl: config.linq.baseUrl,
      phoneNumber: config.linq.fromPhone,
      webhookSecret: config.linq.webhookSecret,
      requestTimeoutMs: 15_000,
      maxAttachmentBytes: 20 * 1024 * 1024,
      maxWebhookBytes: 1024 * 1024,
    };
    const linq = new LinqClient(linqConfig);
    const work = new DurableWork(database, secretBox);
    const outbox = new EffectOutbox(database, secretBox);
    const effectExecutor = new LinqMessageEffectExecutor(linq, outbox);
    const timerRuntime = new TimerRuntime(database, secretBox, linq);
    const application = new FlorenceApplication(database, config, secretBox, timerRuntime);
    const modelRuntime = new GovernedWorkerRuntime(
      database,
      secretBox,
      new BoundedWorkerRuntime(
        new LangChainModelGateway(config),
        `${config.model.provider}:langchain-structured-v1`,
      ),
    );
    const orchestrator = new FlorenceOrchestrator(
      database,
      config,
      secretBox,
      modelRuntime,
      linq,
      application,
    );
    const google = new GoogleSyncService(database, config, secretBox);
    const sources = new PostgresSourceIntelligence(database, secretBox, {
      rawRetentionDays: config.defaults.rawSourceRetentionDays,
      privateCandidateRetentionDays: 7,
    });
    await bootstrapGovernedSkills(database);
    let lastMaintenanceCheckBucket: number | null = null;

    while (running) {
      const maintenanceBucket = Math.floor(Date.now() / 60_000);
      if (maintenanceBucket !== lastMaintenanceCheckBucket) {
        await ensureMaintenanceTick(maintenanceBucket);
        lastMaintenanceCheckBucket = maintenanceBucket;
      }
      const effects = await outbox.claim(workerId, 20);
      for (const effect of effects) await effectExecutor.execute(effect);

      const submittedEffects = await outbox.claimSubmittedForReconciliation(workerId, 20);
      for (const effect of submittedEffects) await effectExecutor.reconcile(effect);

      const jobs = await work.claim(workerId, 12);
      for (const job of jobs) {
        try {
          await dispatch(job);
          await work.succeed(job);
        } catch (error) {
          let providerAuthFailure = false;
          try {
            providerAuthFailure = await google.handleProviderAuthFailure(job.kind, job.payload, error);
          } catch {
            // Retry the provider job when the guarded health transition could not be persisted.
          }
          const retryable = !providerAuthFailure && isRetryableJobError(error);
          const jobErrorCode = providerAuthFailure ? "google_reauth_required" : errorCode(error);
          await work.fail(job, jobErrorCode, { retryable });
          process.stderr.write(
            `${JSON.stringify({ level: "error", jobKind: job.kind, errorCode: jobErrorCode })}\n`,
          );
        }
      }
      if (effects.length === 0 && submittedEffects.length === 0 && jobs.length === 0) {
        await wait(config.intervals.workerPollMs);
      }
    }
    async function dispatch(job: ClaimedJob): Promise<void> {
      if (
        await dispatchTimerProcessJob(job, {
          process: (timer) => application.process({ kind: "timer.process", timer }),
        })
      )
        return;
      switch (job.kind) {
        case "linq.process_event": {
          const payload = z.strictObject({ providerEventId: z.string().min(1).max(500) }).parse(job.payload);
          await application.process({ kind: "linq.process_event", providerEventId: payload.providerEventId });
          return;
        }
        case "orchestrate.linq_message": {
          const payload = OrchestrateMessagePayloadSchema.parse(job.payload);
          await orchestrator.processLinqMessage(payload.internalProviderEventId);
          return;
        }
        case "orchestrate.private_source": {
          const payload = PrivateSourcePayloadSchema.parse(job.payload);
          await orchestrator.processPrivateSourceRevision(
            payload.sourceRevisionId,
            payload.personId,
            payload.integrationId,
            payload.integrationControlEpoch,
          );
          return;
        }
        case "orchestrate.private_bridge_proposal": {
          const payload = PrivateBridgePayloadSchema.parse(job.payload);
          await orchestrator.proposePrivateBridge(payload.actionIntentId);
          return;
        }
        case "private_bridge.commit": {
          const payload = PrivateBridgePayloadSchema.parse(job.payload);
          await application.process({
            kind: "private_bridge.commit",
            actionIntentId: payload.actionIntentId,
          });
          return;
        }
        case "google.bootstrap":
          await google.bootstrap(GoogleBootstrapPayloadSchema.parse(job.payload));
          return;
        case "google.gmail.bootstrap":
          await google.bootstrapGmail(GmailBootstrapPayloadSchema.parse(job.payload));
          return;
        case "google.gmail.poll":
          await google.pollGmail(GmailPollPayloadSchema.parse(job.payload));
          return;
        case "google.gmail.backfill":
          await google.backfillGmail(GmailBackfillPayloadSchema.parse(job.payload));
          return;
        case "google.gmail.message":
          await google.ingestGmailMessage(GmailMessagePayloadSchema.parse(job.payload));
          return;
        case "google.calendar.catalog":
          await google.catalogCalendars(CalendarCatalogPayloadSchema.parse(job.payload));
          return;
        case "google.calendar.poll":
          await google.pollCalendar(CalendarPollPayloadSchema.parse(job.payload));
          return;
        case "auth.send_step_up":
          await sendStepUp(StepUpPayloadSchema.parse(job.payload));
          return;
        case "maintenance.tick":
          await maintenance();
          return;
        default:
          throw new Error(`unsupported_job_kind:${job.kind}`);
      }
    }

    async function sendStepUp(payload: z.infer<typeof StepUpPayloadSchema>): Promise<void> {
      const rows = await database<
        {
          identity_id: string;
          conversation_id: string;
          external_channel_id: string;
          authority_version: number | string;
          participant_epoch_id: string;
          participant_set_digest: string;
          person_control_epoch: number | string;
        }[]
      >`
      select participant.person_identity_id as identity_id, conversation.id as conversation_id,
        channel.external_channel_id, conversation.authority_version,
        epoch.id as participant_epoch_id, epoch.participant_set_digest,
        person.control_epoch as person_control_epoch
      from conversations conversation
      join conversation_channels channel on channel.conversation_id = conversation.id
        and channel.provider = 'linq' and channel.status = 'active'
      join participant_epochs epoch on epoch.id = conversation.current_epoch_id and epoch.ended_at is null
      join epoch_participants participant on participant.participant_epoch_id = epoch.id
        and participant.person_id = ${payload.actorPersonId}
      join people person on person.id = participant.person_id and person.status = 'registered'
      join person_identities identity on identity.id = participant.person_identity_id and identity.status = 'verified'
      where conversation.kind = 'direct' and conversation.status = 'active'
      order by conversation.updated_at desc limit 1
    `;
      const row = rows[0];
      if (!row) throw new NotFoundError("No active private Florence conversation exists");
      const live = await linq.getChat(row.external_channel_id);
      if (
        live.kind !== "direct" ||
        live.participants.filter((entry) => entry.status === "active" && !entry.isSelf).length !== 1
      ) {
        throw new UnauthorizedError("Private Florence audience changed");
      }
      await database.begin(async (transaction) => {
        const snapshot = await new PostgresConversationAuthority(transaction).snapshot(row.conversation_id);
        const authorization = await new PostgresConversationAuthority(transaction).authorizeSend({
          conversationId: row.conversation_id,
          expectedParticipantEpochId: row.participant_epoch_id,
          expectedParticipantSetDigest: row.participant_set_digest,
          liveParticipantIdentityIds: [row.identity_id],
          sendKind: "direct_response",
          operation: "private_step_up",
          ruleId: null,
        });
        if (!authorization.allowed || snapshot.authorityVersion !== Number(row.authority_version)) {
          throw new StaleAuthorityError("Private conversation authority changed");
        }
        const handoff = await new PostgresWebAuth(
          transaction,
          secretBox,
          config.security.tokenKey,
        ).createHandoff({
          personId: payload.actorPersonId,
          privateIdentityId: row.identity_id,
          privateConversationId: row.conversation_id,
          purpose: payload.purpose,
          context: { returnPath: payload.purpose === "google_connect" ? "/sources" : "/safety" },
          expiresInSeconds: 10 * 60,
        });
        await new EffectOutbox(transaction, secretBox).authorizeAndEnqueue({
          actorPersonId: payload.actorPersonId,
          person: { id: payload.actorPersonId, controlEpoch: Number(row.person_control_epoch) },
          conversation: { id: row.conversation_id, authorityVersion: snapshot.authorityVersion },
          participantEpochId: row.participant_epoch_id,
          expectedParticipantDigest: row.participant_set_digest,
          effectKind: "linq.message",
          idempotencyKey: `step-up:${handoff.handoffId}`,
          data: { purpose: payload.purpose },
          policy: { exactPrivateDm: true, authorityVersion: snapshot.authorityVersion },
          target: { providerChatId: row.external_channel_id, participantEpochId: row.participant_epoch_id },
          payload: {
            providerChatId: row.external_channel_id,
            expectedProviderParticipantDigest: live.activeParticipantDigest,
            text: `Open your private Florence controls: ${config.publicBaseUrl}/handoff/${handoff.token}`,
          },
          reasonCodes: ["fresh_private_step_up"],
          authorizationExpiresAt: new Date(Date.now() + 5 * 60_000),
        });
      });
    }

    async function maintenance(): Promise<void> {
      const now = new Date();
      await work.cancelStale(now);
      await outbox.cancelStale(now);
      await application.process({
        kind: "maintenance.redrive_effects",
        asOf: now.toISOString(),
        limit: 20,
      });
      await work.redriveDeadCurrentAuthority({
        kind: "google.gmail.message",
        idempotencyNamespace: "job-redrive:google-gmail-message",
        now,
        limit: GOOGLE_JOB_REDRIVE_LIMIT,
        lookbackMs: GOOGLE_JOB_REDRIVE_LOOKBACK_MS,
        bucketMs: GOOGLE_JOB_REDRIVE_BUCKET_MS,
        maxGenerations: GOOGLE_JOB_REDRIVE_MAX_GENERATIONS,
        requireIntegrationFence: true,
      });
      await work.redriveDeadCurrentAuthority({
        kind: "orchestrate.private_source",
        idempotencyNamespace: "job-redrive:google-private-source",
        now,
        limit: GOOGLE_JOB_REDRIVE_LIMIT,
        lookbackMs: GOOGLE_JOB_REDRIVE_LOOKBACK_MS,
        bucketMs: GOOGLE_JOB_REDRIVE_BUCKET_MS,
        maxGenerations: GOOGLE_JOB_REDRIVE_MAX_GENERATIONS,
        requireIntegrationFence: true,
      });
      const timers = new DurableTimers(database);
      await timers.cancelStale(now);
      await timers.recoverOrphanedClaims(now);
      await seedGoogleRecovery(now);
      await sources.apply({ kind: "sweep_retention", asOf: now.toISOString(), limit: 500 });
      await database`
      delete from provider_events where received_at < ${new Date(now.getTime() - config.defaults.rawSourceRetentionDays * 86_400_000)}
        and processing_status in ('processed', 'ignored', 'failed')
    `;
      await database`
      delete from worker_results result using worker_attempts attempt
      where result.worker_attempt_id = attempt.id
        and attempt.completed_at < ${new Date(now.getTime() - config.defaults.workerScratchRetentionDays * 86_400_000)}
    `;
      await database`
      delete from trace_manifests where retention_until <= ${now}
    `;
      await database`
      delete from person_sessions where absolute_expires_at <= ${now} or (revoked_at is not null and revoked_at < ${new Date(now.getTime() - 7 * 86_400_000)})
    `;
      await materializeRoutineCoverage(now);
      await database.begin(async (transaction) => {
        const timers = await new DurableTimers(transaction).claimDue(100, now);
        const transactionalWork = new DurableWork(transaction, secretBox);
        for (const timer of timers) {
          await transactionalWork.enqueue({
            kind: "timer.process",
            idempotencyKey: `timer:${timer.id}:${timer.dueAt}`,
            payload: {
              id: timer.id,
              kind: timer.kind,
              coverageLoopId: timer.coverageLoopId,
              expectedDomainVersion: timer.expectedDomainVersion,
              dueAt: timer.dueAt,
            },
            ...timer.fence,
            maxAttempts: 5,
          });
        }
      });
    }

    async function ensureMaintenanceTick(bucket: number): Promise<void> {
      const live = await database<{ readonly present: boolean }[]>`
        select exists(
          select 1 from jobs
          where job_kind = 'maintenance.tick' and status in ('pending', 'retry', 'leased')
        ) as present
      `;
      if (live[0]?.present) return;
      await work.enqueue({
        kind: "maintenance.tick",
        idempotencyKey: `maintenance:${bucket}`,
        payload: {},
        maxAttempts: 3,
      });
    }

    async function seedGoogleRecovery(now: Date): Promise<void> {
      const integrations = await database<
        {
          readonly integration_id: string;
          readonly integration_control_epoch: number | string;
          readonly person_id: string;
          readonly person_control_epoch: number | string;
          readonly mail_active: boolean;
          readonly calendar_active: boolean;
          readonly gmail_cursor_ready: boolean;
          readonly gmail_chain_live: boolean;
          readonly gmail_backfill_healthy: boolean;
          readonly calendar_chain_healthy: boolean;
        }[]
      >`
        select integration.id as integration_id,
          integration.control_epoch as integration_control_epoch,
          person.id as person_id, person.control_epoch as person_control_epoch,
          exists(
            select 1 from integration_capabilities capability
            where capability.integration_id = integration.id
              and capability.capability = 'mail' and capability.status = 'active'
          ) as mail_active,
          exists(
            select 1 from integration_capabilities capability
            where capability.integration_id = integration.id
              and capability.capability = 'calendar' and capability.status = 'active'
          ) as calendar_active,
          exists(
            select 1 from sync_cursors cursor
            where cursor.integration_id = integration.id
              and cursor.resource_kind = 'gmail_history'
              and cursor.state in ('initial', 'active', 'expired')
          ) as gmail_cursor_ready,
          exists(
            select 1 from jobs job
            where job.person_id = person.id
              and job.integration_id = integration.id
              and job.integration_control_epoch = integration.control_epoch
              and job.job_kind in ('google.bootstrap', 'google.gmail.bootstrap', 'google.gmail.poll')
              and job.status in ('pending', 'retry', 'leased')
          ) as gmail_chain_live,
          (
            exists(
              select 1 from jobs job
              where job.person_id = person.id
                and job.integration_id = integration.id
                and job.integration_control_epoch = integration.control_epoch
                and job.job_kind in ('google.bootstrap', 'google.gmail.bootstrap')
                and job.status in ('pending', 'retry', 'leased')
            )
            or not exists(
              select 1
              from (values
                ('newest_30_days'),
                ('days_31_to_90'),
                ('days_91_to_365'),
                ('older_history')
              ) as stage(name)
              where not exists(
                select 1 from sync_cursors cursor
                where cursor.integration_id = integration.id
                  and cursor.resource_kind = 'gmail_backfill:' || stage.name
                  and cursor.state = 'exhausted'
              )
              and not exists(
                select 1 from jobs job
                where job.person_id = person.id
                  and job.integration_id = integration.id
                  and job.integration_control_epoch = integration.control_epoch
                  and job.job_kind = 'google.gmail.backfill'
                  and job.idempotency_key like '%:' || stage.name || ':%'
                  and job.status in ('pending', 'retry', 'leased')
              )
            )
          ) as gmail_backfill_healthy,
          (
            exists(
              select 1 from jobs job
              where job.person_id = person.id
                and job.integration_id = integration.id
                and job.integration_control_epoch = integration.control_epoch
                and job.job_kind in ('google.bootstrap', 'google.calendar.catalog')
                and job.status in ('pending', 'retry', 'leased')
            )
            and not exists(
              select 1
              from integration_grants grant_row
              where grant_row.integration_id = integration.id
                and grant_row.grant_kind = 'calendar_privacy'
                and grant_row.status = 'active'
                and grant_row.scope->>'mode' <> 'off'
                and not exists(
                  select 1 from jobs job
                  where job.person_id = person.id
                    and job.integration_id = integration.id
                    and job.integration_control_epoch = integration.control_epoch
                    and job.job_kind = 'google.calendar.poll'
                    and job.idempotency_key like
                      'calendar:poll:' || integration.id || ':e' || integration.control_epoch || ':' ||
                      (grant_row.scope->>'calendarIdDigest') || ':v' || grant_row.version || ':' ||
                      (grant_row.scope->>'mode') || ':%'
                    and job.status in ('pending', 'retry', 'leased')
                )
            )
          ) as calendar_chain_healthy
        from integrations integration
        join people person on person.id = integration.person_id and person.status = 'registered'
        where integration.provider = 'google' and integration.status = 'active'
        order by integration.id
      `;
      const minuteBucket = Math.floor(now.getTime() / 60_000);
      for (const integration of integrations) {
        const payload = {
          integrationId: integration.integration_id,
          personId: integration.person_id,
          integrationControlEpoch: Number(integration.integration_control_epoch),
          personControlEpoch: Number(integration.person_control_epoch),
        };
        const person = { id: integration.person_id, controlEpoch: payload.personControlEpoch };
        const integrationFence = {
          id: integration.integration_id,
          controlEpoch: payload.integrationControlEpoch,
        };
        const epochKey = `e${payload.integrationControlEpoch}`;
        if (integration.mail_active && !integration.gmail_chain_live) {
          await work.enqueue(
            integration.gmail_cursor_ready
              ? {
                  kind: "google.gmail.poll",
                  idempotencyKey: `gmail:poll:${integration.integration_id}:${epochKey}:watchdog:t${minuteBucket}`,
                  payload,
                  person,
                  integration: integrationFence,
                  priority: 50,
                  maxAttempts: 8,
                }
              : {
                  kind: "google.gmail.bootstrap",
                  idempotencyKey: `gmail:bootstrap:${integration.integration_id}:${epochKey}:watchdog:t${minuteBucket}`,
                  payload: {
                    ...payload,
                    olderHistoryEnabled: true,
                    runKey: `watchdog-${minuteBucket}`,
                  },
                  person,
                  integration: integrationFence,
                  priority: 50,
                  maxAttempts: 8,
                },
          );
        }
        if (integration.mail_active && !integration.gmail_backfill_healthy) {
          await work.enqueue({
            kind: "google.gmail.bootstrap",
            idempotencyKey: `gmail:bootstrap:${integration.integration_id}:${epochKey}:backfill-watchdog:t${minuteBucket}`,
            payload: {
              ...payload,
              olderHistoryEnabled: true,
              runKey: `watchdog-${minuteBucket}`,
            },
            person,
            integration: integrationFence,
            priority: 50,
            maxAttempts: 8,
          });
        }
        if (integration.calendar_active && !integration.calendar_chain_healthy) {
          await work.enqueue({
            kind: "google.calendar.catalog",
            idempotencyKey: `calendar:catalog:${integration.integration_id}:${epochKey}:watchdog:t${minuteBucket}`,
            payload,
            person,
            integration: integrationFence,
            priority: 60,
            maxAttempts: 8,
          });
        }
      }
    }

    async function materializeRoutineCoverage(now: Date): Promise<void> {
      const fromLocalDate = new Date(now.getTime() - 86_400_000).toISOString().slice(0, 10);
      const throughLocalDate = new Date(now.getTime() + 14 * 86_400_000).toISOString().slice(0, 10);
      let afterRoutineId: string | null = null;
      for (let page = 0; page < 20; page += 1) {
        const receipt = await application.process({
          kind: "maintenance.materialize_routines",
          fromLocalDate,
          throughLocalDate,
          materializedAt: now.toISOString(),
          afterRoutineId,
          maxOccurrences: 500,
        });
        afterRoutineId = receipt.ids.nextRoutineCursor ?? null;
        if (afterRoutineId === null) return;
      }
      throw new Error("Routine materialization page limit exceeded");
    }
  } finally {
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    await heartbeatInFlight;
    try {
      await recordWorkerStopped(database, workerId);
    } catch {
      process.stderr.write(
        `${JSON.stringify({ level: "error", errorCode: "worker_stop_heartbeat_failed" })}\n`,
      );
    }
    await reserved`select pg_advisory_unlock(${WORKER_ADVISORY_LOCK})`;
    reserved.release();
    await database.end({ timeout: 10 });
    process.off("SIGTERM", stop);
    process.off("SIGINT", stop);
  }
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, Math.min(milliseconds, 60_000)));
}

function errorCode(error: unknown): string {
  if (error instanceof z.ZodError) return "invalid_job_payload";
  if (error instanceof LinqAudienceChangedError) return "linq_audience_changed";
  if (error instanceof LinqAttachmentError) return `linq_attachment_${error.code}`;
  if (error instanceof LinqApiError) return "linq_api_error";
  if (error instanceof CoordinationError) return `coordination_${error.code}`;
  if (error instanceof StaleAuthorityError) return "stale_authority";
  if (error instanceof UnauthorizedError) return "unauthorized";
  if (error instanceof NotFoundError) return "not_found";
  if (error instanceof ConflictError) return "conflict";
  return "internal_worker_error";
}

function isRetryableJobError(error: unknown): boolean {
  if (error instanceof LinqApiError) return error.retryable;
  if (error instanceof LinqAudienceChangedError) return false;
  if (error instanceof LinqAttachmentError) return error.code === "download_failed";
  return !(
    error instanceof z.ZodError ||
    error instanceof UnauthorizedError ||
    error instanceof StaleAuthorityError ||
    error instanceof NotFoundError ||
    error instanceof ConflictError ||
    error instanceof CoordinationError
  );
}

async function recordWorkerStart(
  database: ReturnType<typeof createDatabase>,
  workerId: string,
  releaseId: string,
): Promise<void> {
  await database`
    insert into worker_leases (
      lease_name, worker_id, release_id, started_at, last_seen_at, stopped_at
    ) values (
      ${WORKER_LEASE_NAME}, ${workerId}, ${releaseId}, now(), now(), null
    )
    on conflict (lease_name) do update set
      worker_id = excluded.worker_id,
      release_id = excluded.release_id,
      started_at = excluded.started_at,
      last_seen_at = excluded.last_seen_at,
      stopped_at = null
  `;
}

async function recordWorkerHeartbeat(
  database: ReturnType<typeof createDatabase>,
  workerId: string,
): Promise<void> {
  const rows = await database<{ readonly lease_name: string }[]>`
    update worker_leases set last_seen_at = now()
    where lease_name = ${WORKER_LEASE_NAME} and worker_id = ${workerId} and stopped_at is null
    returning lease_name
  `;
  if (!rows[0]) throw new Error("Worker heartbeat lease changed");
}

async function recordWorkerStopped(
  database: ReturnType<typeof createDatabase>,
  workerId: string,
): Promise<void> {
  await database`
    update worker_leases set last_seen_at = now(), stopped_at = now()
    where lease_name = ${WORKER_LEASE_NAME} and worker_id = ${workerId}
  `;
}

function workerReleaseId(): string {
  const deploymentId = process.env.RAILWAY_DEPLOYMENT_ID?.trim();
  if (deploymentId) return `railway:${deploymentId.slice(0, 180)}`;
  const commitSha = process.env.RAILWAY_GIT_COMMIT_SHA?.trim();
  if (commitSha) return `git:${commitSha.slice(0, 180)}`;
  return "florence@4.0.0";
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(new URL(import.meta.url).pathname)) {
  void main().catch(() => {
    process.stderr.write(`${JSON.stringify({ level: "fatal", errorCode: "worker_startup_failed" })}\n`);
    process.exitCode = 1;
  });
}
