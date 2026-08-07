import { createHash, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type {
  LinqChatSnapshot,
  LinqMessageReceivedEvent,
  LinqWebhookEnvelope,
} from "../adapters/linq/index.js";
import type { FlorenceConfig } from "../config.js";
import type { Database } from "../db/client.js";
import { PostgresWebAuth } from "../modules/auth/index.js";
import { openPrivateBridgePayload, PrivateSourceBridge } from "../modules/bridges/index.js";
import {
  type ConversationAuthoritySnapshot,
  evaluateConversationMode,
  type GroupInvocation,
  GroupRuleOnboarding,
  leadingGroupInvocation,
  PostgresConversationAuthority,
} from "../modules/conversations/index.js";
import { PostgresRoutines, type RoutineRevisionDraft } from "../modules/coordination/index.js";
import { PostgresDataControls } from "../modules/data-controls/index.js";
import { EffectOutbox } from "../modules/effects/index.js";
import { PostgresIdentityRelationships } from "../modules/identity/index.js";
import { HouseholdOnboarding } from "../modules/relationships/index.js";
import { type IntegrationCapability, PostgresSourceIntelligence } from "../modules/sources/index.js";
import { DurableWork } from "../modules/work/index.js";
import { canonicalDigest, canonicalJson } from "../shared/canonical-json.js";
import type { SecretBox } from "../shared/crypto.js";
import { ConflictError, NotFoundError, StaleAuthorityError, UnauthorizedError } from "../shared/errors.js";
import type {
  AppEnvelope,
  ApplicationTimerProcessor,
  ProcessReceipt,
  WebRoutineFields,
} from "./contracts.js";
import { CoverageCoordinator } from "./coverage-coordinator.js";
import { reconcileCoverageTimers } from "./coverage-timer-reconciliation.js";
import { GoogleSyncCoordinator } from "./google-sync-coordinator.js";
import { PrivateSourceReconciler } from "./private-source-reconciler.js";

type Transaction = TransactionSql<Record<string, never>>;
const MAX_LIVE_GROUP_INVOCATION_AGE_MS = 10 * 60_000;
const LINQ_FAILURE_RECONCILIATION_DELAY_MS = 60_000;

interface ReconciledConversation {
  readonly conversationId: string;
  readonly epochId: string;
  readonly epochSequence: number;
  readonly epochStartedAt: string;
  readonly contentEligibleAt: string | null;
  readonly appParticipantDigest: string;
  readonly providerParticipantDigest: string;
  readonly liveIdentityIds: readonly string[];
  readonly senderIdentityId: string | null;
  readonly senderPersonId: string | null;
  readonly snapshot: ConversationAuthoritySnapshot;
  readonly mode: ReturnType<typeof evaluateConversationMode>;
  readonly householdId: string | null;
  readonly householdControlEpoch: number | null;
}

export interface StoredLinqEvent {
  readonly schemaVersion: 1;
  readonly classification: "enrollment" | "stop" | "full" | "observe_only" | "receipt" | "routing_only";
  readonly enrollmentAction?: "consent" | "other";
  readonly invocation?: GroupInvocation;
  readonly event?: LinqWebhookEnvelope;
  readonly routing: {
    readonly conversationId: string;
    readonly participantEpochId: string;
    readonly appParticipantDigest: string;
    readonly providerParticipantDigest: string;
    readonly liveIdentityIds: readonly string[];
    readonly senderIdentityId: string | null;
    readonly senderPersonId: string | null;
    readonly providerChatId: string;
    readonly chatKind: "direct" | "group";
  };
}

interface ProviderEventRow {
  id: string;
  provider_event_id: string;
  envelope_ciphertext: Buffer;
  processing_status: string;
}

interface ExactPrivateRoute {
  readonly conversationId: string;
  readonly participantEpochId: string;
  readonly participantSetDigest: string;
  readonly liveIdentityIds: readonly string[];
  readonly privateIdentityId: string;
  readonly providerChatId: string;
  readonly providerParticipantDigest: string;
}

interface ProcessedPrivateDmSource {
  readonly record: StoredLinqEvent;
  readonly event: LinqMessageReceivedEvent;
  readonly snapshot: ConversationAuthoritySnapshot;
  readonly personId: string;
}

type ParentGoogleActivationReason =
  | "household_resolved"
  | "existing_steward_dm"
  | "reengagement_after_expiry";

/**
 * Florence's sole authoritative mutation seam. Provider, timer, browser, worker,
 * and receipt inputs all re-enter here so current scope and authority are checked
 * at the moment state changes.
 */
export class FlorenceApplication {
  public constructor(
    private readonly database: Database,
    private readonly config: FlorenceConfig,
    private readonly secretBox: SecretBox,
    private readonly timerProcessor: ApplicationTimerProcessor | null = null,
  ) {}

  public async process(input: AppEnvelope): Promise<ProcessReceipt> {
    switch (input.kind) {
      case "linq.webhook":
        return this.admitLinqWebhook(input.event, input.liveChat);
      case "linq.process_event":
        return this.processLinqEvent(input.providerEventId);
      case "linq.private_invocation_response":
        return this.commitPrivateGroupInvocationResponse(
          input.internalProviderEventId,
          input.responseText,
          input.evidenceSourceRevisionIds,
        );
      case "linq.private_dm_orchestration_complete":
        return this.completePrivateDmOrchestration(input);
      case "linq.reconcile_chat":
        return this.reconcileLiveLinqChat(input.liveChat);
      case "timer.process":
        if (!this.timerProcessor) throw new Error("Timer processing is not configured in this process");
        await this.timerProcessor.process(input.timer);
        return {
          accepted: true,
          duplicate: false,
          disposition: "timer_processed",
          ids: { timerId: input.timer.id },
        };
      case "maintenance.materialize_routines":
        return this.materializeRoutines(input);
      case "maintenance.redrive_effects": {
        const redriven = await new EffectOutbox(this.database, this.secretBox).redriveFailed(
          new Date(input.asOf),
          input.limit,
        );
        return {
          accepted: true,
          duplicate: redriven === 0,
          disposition: "effect_redrive_complete",
          ids: { redriven: String(redriven) },
        };
      }
      case "google.oauth.begin":
        return this.beginGoogleOAuth(input);
      case "google.oauth.complete":
        return this.completeGoogleOAuth(input);
      case "google.sync.observe":
        return this.observeGoogleSync(input);
      case "private_source.notify_candidate":
        return this.schedulePrivateSourceCandidateNotice(input);
      case "private_source.deliver_candidate_notice":
        return this.deliverPrivateSourceCandidateNotice(input);
      case "private_source.reconcile":
        return this.reconcilePrivateSource(input);
      case "private_bridge.proposal": {
        const result = await new PrivateSourceBridge(
          this.database,
          this.secretBox,
          this.config.defaults.rawSourceRetentionDays,
        ).recordProposal(input.proposal);
        return {
          accepted: true,
          duplicate: false,
          disposition:
            result.status === "approved"
              ? "private_bridge_standing_approved"
              : "private_bridge_awaiting_owner_approval",
          ids: { actionIntentId: result.actionIntentId },
        };
      }
      case "private_bridge.commit": {
        const result = await new PrivateSourceBridge(
          this.database,
          this.secretBox,
          this.config.defaults.rawSourceRetentionDays,
        ).commit(input.actionIntentId);
        return {
          accepted: true,
          duplicate: result.duplicate,
          disposition: result.cancelled
            ? "private_bridge_cancelled_for_fresh_approval"
            : "private_bridge_committed",
          ids: { actionIntentId: input.actionIntentId, loopId: result.loopId },
        };
      }
      case "coverage.apply":
        return new CoverageCoordinator(this.database, this.config, this.secretBox).apply(input.proposal);
      case "web.command":
        return this.processWebCommand(input.actorPersonId, input.command);
    }
  }

  private async beginGoogleOAuth(
    input: Extract<AppEnvelope, { kind: "google.oauth.begin" }>,
  ): Promise<ProcessReceipt> {
    const result = await new PostgresSourceIntelligence(this.database, this.secretBox, {
      rawRetentionDays: this.config.defaults.rawSourceRetentionDays,
    }).apply({
      kind: "begin_oauth_attempt",
      personId: input.personId,
      provider: "google",
      initiatingSessionId: input.initiatingSessionId,
      stateDigest: input.stateDigest,
      pkceVerifier: input.pkceVerifier,
      returnPath: input.returnPath,
      requestedCapabilities: [...input.requestedCapabilities],
      accountKind: input.accountKind,
      expectedPersonControlEpoch: input.expectedPersonControlEpoch,
      expiresAt: input.expiresAt,
      createdAt: input.createdAt,
    });
    if (result.kind !== "oauth_attempt_started") {
      throw new Error("Google OAuth attempt did not start");
    }
    return {
      accepted: true,
      duplicate: false,
      disposition: "google_oauth_started",
      ids: { oauthAttemptId: result.oauthAttemptId },
    };
  }

  private async completeGoogleOAuth(
    input: Extract<AppEnvelope, { kind: "google.oauth.complete" }>,
  ): Promise<ProcessReceipt> {
    return this.database.begin(async (transaction) => {
      const sources = new PostgresSourceIntelligence(transaction, this.secretBox, {
        rawRetentionDays: this.config.defaults.rawSourceRetentionDays,
      });
      const consumed = await sources.apply({
        kind: "consume_oauth_attempt",
        provider: "google",
        stateDigest: input.stateDigest,
        consumedAt: input.completedAt,
      });
      if (consumed.kind !== "oauth_attempt_consumed" || consumed.provider !== "google") {
        throw new Error("Google OAuth attempt did not resolve");
      }

      const actuallyGranted = new Set<IntegrationCapability>(input.grantedCapabilities);
      const activeCapabilities = consumed.requestedCapabilities.filter((capability) =>
        actuallyGranted.has(capability),
      );
      if (activeCapabilities.length === 0) {
        throw new UnauthorizedError("Google granted no requested account capability");
      }

      const connected = await sources.apply({
        kind: "connect_integration",
        personId: consumed.personId,
        provider: "google",
        externalSubjectDigest: input.externalSubjectDigest,
        accountKind: consumed.accountKind,
        activeCapabilities: [...activeCapabilities],
        credentials: input.credentials,
        expectedPersonControlEpoch: consumed.personControlEpoch,
        connectedAt: input.completedAt,
      });
      if (connected.kind !== "integration_connected") {
        throw new Error("Google integration did not connect");
      }

      const bootstrap = await new DurableWork(transaction, this.secretBox).enqueue({
        kind: "google.bootstrap",
        idempotencyKey: `google:bootstrap:${connected.integrationId}:e${connected.controlEpoch}`,
        payload: {
          integrationId: connected.integrationId,
          personId: consumed.personId,
          integrationControlEpoch: connected.controlEpoch,
          personControlEpoch: consumed.personControlEpoch,
          olderHistoryEnabled: true,
        },
        person: { id: consumed.personId, controlEpoch: consumed.personControlEpoch },
        integration: { id: connected.integrationId, controlEpoch: connected.controlEpoch },
        priority: 50,
        maxAttempts: 8,
      });

      const milestone = await new GoogleSyncCoordinator(transaction, this.config, this.secretBox).observe({
        integrationId: connected.integrationId,
        personId: consumed.personId,
        triggeringJobId: null,
      });

      return {
        accepted: true,
        duplicate: !bootstrap.created,
        disposition: "google_oauth_completed",
        ids: {
          oauthAttemptId: consumed.oauthAttemptId,
          integrationId: connected.integrationId,
          bootstrapJobId: bootstrap.jobId,
          ...(milestone.outboxIds[0] ? { milestoneOutboxId: milestone.outboxIds[0] } : {}),
          returnPath: consumed.returnPath,
        },
      };
    });
  }

  private async observeGoogleSync(
    input: Extract<AppEnvelope, { kind: "google.sync.observe" }>,
  ): Promise<ProcessReceipt> {
    const result = await new GoogleSyncCoordinator(this.database, this.config, this.secretBox).observe(input);
    return {
      accepted: true,
      duplicate: result.outboxIds.length === 0,
      disposition: result.disposition,
      ids: {
        integrationId: input.integrationId,
        ...(result.outboxIds[0] ? { outboxId: result.outboxIds[0] } : {}),
        ...(result.outboxIds[1] ? { secondOutboxId: result.outboxIds[1] } : {}),
      },
    };
  }

  private async schedulePrivateSourceCandidateNotice(
    input: Extract<AppEnvelope, { kind: "private_source.notify_candidate" }>,
  ): Promise<ProcessReceipt> {
    return this.database.begin(async (transaction) => {
      const rows = await transaction<
        {
          readonly person_control_epoch: number | string;
          readonly integration_control_epoch: number | string;
          readonly expires_at: Date;
        }[]
      >`
        select person.control_epoch as person_control_epoch,
          integration.control_epoch as integration_control_epoch,
          candidate.expires_at
        from knowledge_candidates candidate
        join private_source_frontiers frontier
          on frontier.current_candidate_id = candidate.id
          and frontier.owner_person_id = candidate.owner_person_id
          and frontier.disposition = 'candidate'
          and frontier.source_generation = frontier.reconciled_generation
        join people person on person.id = candidate.owner_person_id and person.status = 'registered'
        join integrations integration on integration.id = frontier.integration_id
          and integration.person_id = person.id and integration.status = 'active'
        where candidate.id = ${input.candidateId}
          and candidate.owner_person_id = ${input.personId}
          and candidate.scope_kind = 'person' and candidate.status = 'pending'
          and candidate.expires_at > now()
          and integration.id = ${input.integrationId}
          and integration.control_epoch = ${input.expectedIntegrationControlEpoch}
        for share of candidate, frontier, person, integration
      `;
      const current = rows[0];
      if (!current) {
        return {
          accepted: true,
          duplicate: true,
          disposition: "private_source_candidate_notice_obsolete",
          ids: { candidateId: input.candidateId },
        };
      }
      const queued = await new DurableWork(transaction, this.secretBox).enqueue({
        kind: "private_source.deliver_candidate_notice",
        idempotencyKey: `private-source-notice:${input.candidateId}`,
        payload: {
          candidateId: input.candidateId,
          personId: input.personId,
          integrationId: input.integrationId,
          expectedIntegrationControlEpoch: input.expectedIntegrationControlEpoch,
        },
        person: { id: input.personId, controlEpoch: Number(current.person_control_epoch) },
        integration: {
          id: input.integrationId,
          controlEpoch: Number(current.integration_control_epoch),
        },
        deadlineAt: current.expires_at,
        priority: 25,
        maxAttempts: 672,
      });
      return {
        accepted: true,
        duplicate: !queued.created,
        disposition: queued.created
          ? "private_source_candidate_notice_scheduled"
          : "private_source_candidate_notice_already_scheduled",
        ids: { candidateId: input.candidateId, jobId: queued.jobId },
      };
    });
  }

  private async deliverPrivateSourceCandidateNotice(
    input: Extract<AppEnvelope, { kind: "private_source.deliver_candidate_notice" }>,
  ): Promise<ProcessReceipt> {
    const result = await new GoogleSyncCoordinator(
      this.database,
      this.config,
      this.secretBox,
    ).notifyPrivateSourceCandidate(input);
    return {
      accepted: result.kind !== "route_unavailable" && result.kind !== "not_ready",
      duplicate: result.kind !== "queued" || !result.created,
      disposition:
        result.kind === "route_unavailable"
          ? "private_source_candidate_private_route_unavailable"
          : result.kind === "not_ready"
            ? "private_source_candidate_information_not_current"
            : result.kind === "obsolete"
              ? "private_source_candidate_notice_obsolete"
              : result.created
                ? "private_source_candidate_private_notice_queued"
                : "private_source_candidate_private_notice_already_recorded",
      ids: {
        candidateId: input.candidateId,
        ...(result.kind === "queued" ? { outboxId: result.outboxId } : {}),
      },
    };
  }

  private async reconcilePrivateSource(
    input: Extract<AppEnvelope, { kind: "private_source.reconcile" }>,
  ): Promise<ProcessReceipt> {
    const reconciledAt = new Date().toISOString();
    const result = await this.database.begin(async (transaction) =>
      new PrivateSourceReconciler(transaction, this.secretBox, {
        rawRetentionDays: this.config.defaults.rawSourceRetentionDays,
      }).apply(input.proposal, reconciledAt),
    );
    switch (result.kind) {
      case "duplicate":
        return {
          accepted: true,
          duplicate: true,
          disposition: "private_source_reconciliation_duplicate",
          ids: {
            anchorSourceRevisionId: input.proposal.anchorSourceRevisionId,
            ...(result.candidateId ? { candidateId: result.candidateId } : {}),
          },
        };
      case "unchanged":
        return {
          accepted: true,
          duplicate: false,
          disposition: "private_source_reconciliation_unchanged",
          ids: {
            anchorSourceRevisionId: input.proposal.anchorSourceRevisionId,
            ...(result.candidateId ? { candidateId: result.candidateId } : {}),
          },
        };
      case "candidate_created":
        return {
          accepted: true,
          duplicate: false,
          disposition: "private_source_coverage_candidate_created",
          ids: {
            anchorSourceRevisionId: input.proposal.anchorSourceRevisionId,
            candidateId: result.candidateId,
          },
        };
      case "cancelled":
        return {
          accepted: true,
          duplicate: false,
          disposition: "private_source_coverage_cancelled",
          ids: {
            anchorSourceRevisionId: input.proposal.anchorSourceRevisionId,
            ...(result.loopId ? { loopId: result.loopId } : {}),
            correction: result.correction,
          },
        };
      case "loop_update_review_created":
      case "loop_update_review_pending":
        return {
          accepted: true,
          duplicate: result.kind === "loop_update_review_pending",
          disposition: "private_source_loop_update_review_pending",
          ids: {
            anchorSourceRevisionId: input.proposal.anchorSourceRevisionId,
            candidateId: result.candidateId,
            loopId: result.loopId,
          },
        };
    }
  }

  private async materializeRoutines(
    input: Extract<AppEnvelope, { kind: "maintenance.materialize_routines" }>,
  ): Promise<ProcessReceipt> {
    return this.database.begin(async (transaction) => {
      const result = await new PostgresRoutines(transaction, this.secretBox).materializeDue({
        fromLocalDate: input.fromLocalDate,
        throughLocalDate: input.throughLocalDate,
        materializedAt: new Date(input.materializedAt),
        afterRoutineId: input.afterRoutineId,
        maxOccurrences: input.maxOccurrences,
      });
      let timersScheduled = 0;
      for (const entry of result.coverage) {
        const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
          entry.loop.destination.conversationId,
        );
        const timerId = await reconcileCoverageTimers({
          transaction,
          loop: entry.loop,
          snapshot,
          now: new Date(input.materializedAt),
          allowReminder: true,
          openingRequired: true,
        });
        if (timerId) timersScheduled += 1;
      }
      return {
        accepted: true,
        duplicate: result.coverage.every((entry) => !entry.occurrenceCreated && !entry.loopCreated),
        disposition: result.nextRoutineCursor
          ? "routine_materialization_page"
          : "routine_materialization_complete",
        ids: {
          ...(result.nextRoutineCursor ? { nextRoutineCursor: result.nextRoutineCursor } : {}),
          coverageCount: String(result.coverage.length),
          timersScheduled: String(timersScheduled),
          staleCount: String(result.stale.length),
        },
      };
    });
  }

  private async admitLinqWebhook(
    event: LinqWebhookEnvelope,
    liveChat: LinqChatSnapshot | null,
  ): Promise<ProcessReceipt> {
    if (event.eventType !== "linq.ignored" && liveChat === null) {
      throw new Error("A supported Linq event requires an authoritative live chat");
    }
    if (
      event.eventType !== "linq.ignored" &&
      liveChat !== null &&
      event.channel.providerChatId !== liveChat.providerChatId
    ) {
      throw new Error("Linq webhook and live chat do not match");
    }
    return this.database.begin(async (transaction) => {
      const existing = await transaction<{ id: string; processing_status: string }[]>`
        select id, processing_status from provider_events
        where provider = 'linq' and provider_event_id = ${event.providerEventId}
      `;
      if (existing[0]) {
        return {
          accepted: true,
          duplicate: true,
          disposition: existing[0].processing_status,
          ids: { providerEventId: existing[0].id },
        };
      }

      if (event.eventType === "linq.ignored") {
        const record = sanitizedIgnored(event);
        const eventId = await insertProviderEvent(transaction, this.secretBox, event, record, "ignored");
        return {
          accepted: true,
          duplicate: false,
          disposition: "ignored",
          ids: { providerEventId: eventId },
        };
      }

      if (liveChat === null) throw new Error("Supported Linq event lost its live chat");
      if (!hasActiveFlorenceParticipant(liveChat)) {
        return this.deactivateLinqConversation(transaction, event, liveChat);
      }
      const reconciled = await this.reconcileLinqConversation(transaction, event, liveChat);
      if (liveChat.kind === "group") {
        const hasUnregisteredParticipant = reconciled.snapshot.participants.some(
          (participant) =>
            participant.registrationStatus !== "registered" || participant.consentedAt === null,
        );
        if (hasUnregisteredParticipant) {
          await this.queuePrivateGroupEnrollmentOffers(transaction, reconciled.conversationId);
        } else {
          await this.queuePrivateGroupReadyNotices(transaction, reconciled.conversationId, "group-ready");
        }
      }
      const ordinaryGroupContentAt =
        liveChat.kind !== "group"
          ? null
          : event.eventType === "linq.message.received"
            ? event.message.sentAt
            : event.eventType === "linq.message.edited"
              ? event.edit.editedAt
              : event.eventType === "linq.reaction.added" || event.eventType === "linq.reaction.removed"
                ? event.reaction.changedAt
                : null;
      const belongsToPriorParticipantEpoch =
        ordinaryGroupContentAt !== null &&
        Date.parse(ordinaryGroupContentAt) < Date.parse(reconciled.epochStartedAt);
      const precedesParticipantConsent =
        ordinaryGroupContentAt !== null &&
        reconciled.contentEligibleAt !== null &&
        Date.parse(ordinaryGroupContentAt) < Date.parse(reconciled.contentEligibleAt);
      const isStopCommand =
        event.eventType === "linq.message.received" && messageText(event).trim().toUpperCase() === "STOP";
      const outsideCurrentContentWindow =
        !isStopCommand && (belongsToPriorParticipantEpoch || precedesParticipantConsent);
      if (
        !outsideCurrentContentWindow &&
        event.eventType === "linq.message.received" &&
        event.message.replyTo
      ) {
        await this.confirmOutboundByReply(transaction, event, reconciled);
      }
      const classification: ReturnType<typeof classifyEvent> = outsideCurrentContentWindow
        ? ({ kind: "routing_only", retainEvent: false } as const)
        : classifyEvent(event, liveChat.kind, reconciled.mode);
      const invocation =
        classification.kind === "observe_only" &&
        event.eventType === "linq.message.received" &&
        isFreshLiveMessage(event) &&
        (await this.isExactRegisteredGroupSender(transaction, reconciled))
          ? await this.resolveObserveOnlyInvocation(transaction, event, reconciled)
          : null;
      const record: StoredLinqEvent = {
        schemaVersion: 1,
        classification: classification.kind,
        ...(classification.enrollmentAction ? { enrollmentAction: classification.enrollmentAction } : {}),
        ...(invocation ? { invocation } : {}),
        ...(classification.retainEvent ? { event } : {}),
        routing: {
          conversationId: reconciled.conversationId,
          participantEpochId: reconciled.epochId,
          appParticipantDigest: reconciled.appParticipantDigest,
          providerParticipantDigest: reconciled.providerParticipantDigest,
          liveIdentityIds: reconciled.liveIdentityIds,
          senderIdentityId: reconciled.senderIdentityId,
          senderPersonId: reconciled.senderPersonId,
          providerChatId: liveChat.providerChatId,
          chatKind: liveChat.kind,
        },
      };
      // An edit is a source-revocation event, so apply it from the verified raw
      // webhook before content classification can discard the event and before
      // the effect worker can claim a response derived from the old text.
      // Retained edits repeat this idempotently during durable processing.
      if (event.eventType === "linq.message.edited") {
        await this.invalidateEditedLinqMessageSource(transaction, event, record.routing);
      }
      const ignored = classification.kind === "routing_only";
      const eventId = await insertProviderEvent(
        transaction,
        this.secretBox,
        event,
        record,
        ignored ? "ignored" : "pending",
      );
      if (!ignored) {
        await new DurableWork(transaction, this.secretBox).enqueue({
          kind: "linq.process_event",
          idempotencyKey: `linq:process:${event.providerEventId}`,
          payload: { providerEventId: event.providerEventId },
          ...(classification.kind === "observe_only"
            ? {}
            : {
                conversation: {
                  id: reconciled.conversationId,
                  authorityVersion: reconciled.snapshot.authorityVersion,
                },
              }),
          ...(classification.kind !== "observe_only" &&
          reconciled.householdId &&
          reconciled.householdControlEpoch
            ? { household: { id: reconciled.householdId, controlEpoch: reconciled.householdControlEpoch } }
            : {}),
          priority: 10,
          maxAttempts: 8,
        });
      }
      await appendAudit(transaction, {
        conversationId: reconciled.conversationId,
        householdId: reconciled.householdId,
        personId: reconciled.senderPersonId,
        eventType: ignored ? "linq_event_routing_only" : "linq_event_admitted",
        targetType: "provider_event",
        targetId: eventId,
        reasons: [
          classification.kind,
          reconciled.mode,
          ...(belongsToPriorParticipantEpoch ? ["message_precedes_current_participant_epoch"] : []),
          ...(precedesParticipantConsent ? ["message_precedes_current_participant_consent"] : []),
        ],
        manifest: { retainedOrdinaryContent: classification.retainEvent },
      });
      return {
        accepted: true,
        duplicate: false,
        disposition: ignored ? "ignored" : "queued",
        ids: { providerEventId: eventId, conversationId: reconciled.conversationId },
      };
    });
  }

  private async reconcileLiveLinqChat(liveChat: LinqChatSnapshot): Promise<ProcessReceipt> {
    return this.database.begin(async (transaction) => {
      if (!hasActiveFlorenceParticipant(liveChat)) {
        return this.deactivateLinqConversation(transaction, null, liveChat);
      }
      const reconciled = await this.reconcileLinqConversation(transaction, null, liveChat);
      if (liveChat.kind === "group") {
        const hasUnregisteredParticipant = reconciled.snapshot.participants.some(
          (participant) =>
            participant.registrationStatus !== "registered" || participant.consentedAt === null,
        );
        if (hasUnregisteredParticipant) {
          await this.queuePrivateGroupEnrollmentOffers(transaction, reconciled.conversationId);
        } else {
          await this.queuePrivateGroupReadyNotices(transaction, reconciled.conversationId, "group-ready");
        }
      }
      await appendAudit(transaction, {
        conversationId: reconciled.conversationId,
        householdId: reconciled.householdId,
        personId: null,
        eventType: "linq_chat_reconciled_before_outbound",
        targetType: "conversation",
        targetId: reconciled.conversationId,
        reasons: [reconciled.mode, "authoritative_live_audience"],
        manifest: { retainedOrdinaryContent: false },
      });
      return {
        accepted: true,
        duplicate: false,
        disposition: "linq_chat_reconciled",
        ids: {
          conversationId: reconciled.conversationId,
          participantEpochId: reconciled.epochId,
        },
      };
    });
  }

  private async deactivateLinqConversation(
    transaction: Transaction,
    event: Exclude<LinqWebhookEnvelope, { eventType: "linq.ignored" }> | null,
    liveChat: LinqChatSnapshot,
  ): Promise<ProcessReceipt> {
    const rows = await transaction<
      {
        conversation_id: string;
        conversation_status: string;
        current_epoch_id: string | null;
        epoch_started_at: Date | null;
        channel_status: string;
        latest_participant_digest: string | null;
        latest_participant_checked_at: Date | null;
      }[]
    >`
      select conversation.id as conversation_id, conversation.status as conversation_status,
        conversation.current_epoch_id, epoch.started_at as epoch_started_at,
        channel.status as channel_status, channel.latest_participant_digest,
        channel.latest_participant_checked_at
      from conversation_channels channel
      join conversations conversation on conversation.id = channel.conversation_id
      left join participant_epochs epoch on epoch.id = conversation.current_epoch_id
      where channel.provider = 'linq'
        and channel.external_channel_id = ${liveChat.providerChatId}
      for update of channel, conversation
    `;
    const binding = rows[0];
    if (!binding) {
      if (event) {
        const providerEventId = await insertProviderEvent(
          transaction,
          this.secretBox,
          event,
          sanitizedIgnored(event),
          "ignored",
        );
        return {
          accepted: true,
          duplicate: false,
          disposition: "unbound_removed_chat_ignored",
          ids: { providerEventId },
        };
      }
      return {
        accepted: true,
        duplicate: true,
        disposition: "unbound_removed_chat_ignored",
        ids: {},
      };
    }
    const checkedAt = new Date(liveChat.checkedAt);
    if (
      binding.latest_participant_checked_at !== null &&
      (checkedAt < binding.latest_participant_checked_at ||
        (checkedAt.getTime() === binding.latest_participant_checked_at.getTime() &&
          binding.latest_participant_digest !== liveChat.activeParticipantDigest))
    ) {
      throw new StaleAuthorityError("An older removed-audience snapshot cannot replace a newer one");
    }
    const boundary =
      binding.epoch_started_at && checkedAt < binding.epoch_started_at ? binding.epoch_started_at : checkedAt;
    if (binding.current_epoch_id) {
      await transaction`
        update participant_epochs set ended_at = coalesce(ended_at, ${boundary})
        where id = ${binding.current_epoch_id}
      `;
    }
    await transaction`
      update conversation_rules
      set status = case when status = 'active' then 'superseded' else 'revoked' end,
        ended_at = coalesce(ended_at, ${boundary})
      where conversation_id = ${binding.conversation_id}
        and status in ('active', 'candidate')
    `;
    if (binding.channel_status !== "revoked") {
      await transaction`
        update conversations
        set current_epoch_id = null, status = 'paused',
          authority_version = authority_version + 1,
          control_epoch = control_epoch + 1, updated_at = ${boundary}
        where id = ${binding.conversation_id}
          and status not in ('deletion_fenced', 'deleted')
      `;
    }
    await transaction`
      update conversation_channels
      set status = 'revoked', revoked_at = coalesce(revoked_at, ${boundary}),
        latest_participant_digest = ${liveChat.activeParticipantDigest},
        latest_participant_checked_at = ${checkedAt}
      where conversation_id = ${binding.conversation_id}
        and provider = 'linq' and external_channel_id = ${liveChat.providerChatId}
    `;
    const providerEventId = event
      ? await insertProviderEvent(transaction, this.secretBox, event, sanitizedIgnored(event), "ignored")
      : null;
    await appendAudit(transaction, {
      conversationId: binding.conversation_id,
      householdId: null,
      personId: null,
      eventType: "linq_channel_deactivated",
      targetType: "conversation",
      targetId: binding.conversation_id,
      reasons: ["configured_line_not_active", "participant_epoch_closed", "group_rules_revoked"],
      manifest: { providerChatId: liveChat.providerChatId },
    });
    return {
      accepted: true,
      duplicate: binding.channel_status === "revoked",
      disposition: "linq_channel_deactivated",
      ids: {
        conversationId: binding.conversation_id,
        ...(providerEventId ? { providerEventId } : {}),
      },
    };
  }

  private async reconcileLinqConversation(
    transaction: Transaction,
    event: Exclude<LinqWebhookEnvelope, { eventType: "linq.ignored" }> | null,
    liveChat: LinqChatSnapshot,
  ): Promise<ReconciledConversation> {
    const identity = new PostgresIdentityRelationships(transaction);
    const conversations = new PostgresConversationAuthority(transaction);
    const observed = new Map<string, { identityId: string; personId: string }>();
    for (const participant of liveChat.participants.filter(
      (entry) => entry.status === "active" && !entry.isSelf,
    )) {
      const normalized = normalizeHandle(participant.address);
      const principal = await identity.observeIdentity({
        kind: /^\+[1-9]\d{6,14}$/u.test(normalized) ? "phone" : "provider_handle",
        issuer: "linq",
        subjectDigest: sha256Hex(normalized),
        observedAt: liveChat.checkedAt,
      });
      const encryptedSubject = this.secretBox.encrypt(normalized, `identity-subject:${principal.identityId}`);
      await transaction`
        update person_identities
        set subject_ciphertext = ${Buffer.from(JSON.stringify(encryptedSubject), "utf8")},
          subject_key_version = ${encryptedSubject.kid}, updated_at = ${new Date(liveChat.checkedAt)}
        where id = ${principal.identityId} and status <> 'revoked'
      `;
      observed.set(normalized, { identityId: principal.identityId, personId: principal.personId });
    }
    if (observed.size === 0) throw new Error("A Florence conversation needs at least one human participant");

    let binding = await conversations.findByChannel({
      provider: "linq",
      externalChannelId: liveChat.providerChatId,
    });
    const channelWasNew = binding === null;
    if (!binding) {
      const created = await conversations.createConversation({
        householdId: null,
        kind: liveChat.kind,
        purpose:
          liveChat.kind === "direct"
            ? "private Florence conversation"
            : "participant-governed family context",
        createdAt: liveChat.checkedAt,
      });
      await conversations.bindChannel({
        conversationId: created.conversationId,
        provider: "linq",
        externalChannelId: liveChat.providerChatId,
        boundAt: liveChat.checkedAt,
      });
      binding = await conversations.findByChannel({
        provider: "linq",
        externalChannelId: liveChat.providerChatId,
      });
      if (!binding) throw new Error("New Linq channel binding could not be reloaded");
    }
    const priorAudience = await transaction<
      {
        channel_status: "active" | "paused" | "revoked";
        conversation_status: "active" | "paused" | "deletion_fenced" | "deleted";
        conversation_kind: "direct" | "group";
        latest_participant_digest: string | null;
        latest_participant_checked_at: Date | null;
      }[]
    >`
      select channel.status as channel_status, conversation.status as conversation_status,
        conversation.kind as conversation_kind,
        channel.latest_participant_digest, channel.latest_participant_checked_at
      from conversation_channels channel
      join conversations conversation on conversation.id = channel.conversation_id
      where channel.conversation_id = ${binding.conversationId}
        and channel.provider = 'linq'
      for update of channel, conversation
    `;
    const previous = priorAudience[0];
    if (!previous) throw new StaleAuthorityError("The Linq channel binding is no longer active");
    const checkedAt = new Date(liveChat.checkedAt);
    if (
      !channelWasNew &&
      previous.latest_participant_checked_at !== null &&
      (checkedAt < previous.latest_participant_checked_at ||
        (checkedAt.getTime() === previous.latest_participant_checked_at.getTime() &&
          (previous.latest_participant_digest !== liveChat.activeParticipantDigest ||
            previous.conversation_kind !== liveChat.kind)))
    ) {
      throw new StaleAuthorityError("An older Linq audience snapshot cannot replace a newer one");
    }
    if (previous.conversation_status === "deletion_fenced" || previous.conversation_status === "deleted") {
      throw new StaleAuthorityError("A deleted Florence conversation cannot be reactivated");
    }
    const channelReactivated = !channelWasNew && previous.channel_status !== "active";
    if (channelReactivated) {
      await transaction`
        update conversation_channels
        set status = 'active', revoked_at = null
        where conversation_id = ${binding.conversationId} and provider = 'linq'
          and external_channel_id = ${liveChat.providerChatId}
      `;
      await transaction`
        update conversations
        set status = 'active', control_epoch = control_epoch + 1, updated_at = ${checkedAt}
        where id = ${binding.conversationId} and status = 'paused'
      `;
    }
    const providerKindChanged = !channelWasNew && previous.conversation_kind !== liveChat.kind;
    const providerAudienceChanged =
      !channelWasNew &&
      (channelReactivated ||
        previous.latest_participant_digest === null ||
        previous.latest_participant_checked_at === null ||
        previous.latest_participant_digest !== liveChat.activeParticipantDigest);
    if (providerKindChanged) {
      await transaction`
        update conversations set kind = ${liveChat.kind}, updated_at = ${checkedAt}
        where id = ${binding.conversationId}
      `;
    }
    const participantIdentityIds = [...observed.values()].map((entry) => entry.identityId).sort();
    const florenceJoinedAt = liveChat.participants.find(
      (participant) => participant.status === "active" && participant.isSelf,
    )?.joinedAt;
    const epochObservedAt = channelWasNew ? (florenceJoinedAt ?? liveChat.checkedAt) : liveChat.checkedAt;
    const epoch = await conversations.recordParticipantEpoch({
      conversationId: binding.conversationId,
      participantIdentityIds,
      changeReason: event?.eventType.startsWith("linq.participant.")
        ? event.eventType
        : channelReactivated
          ? "authoritative_provider_channel_reactivation"
          : providerKindChanged
            ? "authoritative_provider_chat_kind_change"
            : providerAudienceChanged
              ? "authoritative_provider_audience_change"
              : event === null
                ? "authoritative_outbound_reconciliation"
                : "authoritative_chat_reconciliation",
      forceNewEpoch: providerAudienceChanged || providerKindChanged,
      observedAt: epochObservedAt,
    });
    await transaction`
      update conversation_channels
      set latest_participant_digest = ${liveChat.activeParticipantDigest},
        latest_participant_checked_at = ${checkedAt}
      where conversation_id = ${binding.conversationId}
        and provider = 'linq' and status = 'active'
    `;
    const snapshot = await conversations.snapshot(binding.conversationId);
    const people = [...observed.values()].map((entry) => entry.personId);
    const household = await inferSharedHousehold(transaction, people);
    if (household) {
      await transaction`
        update conversations set household_id = coalesce(household_id, ${household.id})
        where id = ${binding.conversationId}
          and (household_id is null or household_id = ${household.id})
      `;
    }
    const sender =
      event === null
        ? null
        : "message" in event && "sender" in event.message
          ? (observed.get(normalizeHandle(event.message.sender.address)) ?? null)
          : "edit" in event
            ? (observed.get(normalizeHandle(event.edit.editor.address)) ?? null)
            : "reaction" in event
              ? (observed.get(normalizeHandle(event.reaction.reactor.address)) ?? null)
              : "participant" in event
                ? (observed.get(normalizeHandle(event.participant.address)) ?? null)
                : null;
    return {
      conversationId: binding.conversationId,
      epochId: epoch.participantEpochId,
      epochSequence: epoch.sequence,
      epochStartedAt: epoch.startedAt,
      contentEligibleAt:
        snapshot.participants.length > 0 &&
        snapshot.participants.every((participant) => participant.consentedAt !== null)
          ? (snapshot.participants
              .map((participant) => participant.consentedAt as string)
              .sort((left, right) => Date.parse(right) - Date.parse(left))[0] ?? null)
          : null,
      appParticipantDigest: epoch.participantSetDigest,
      providerParticipantDigest: liveChat.activeParticipantDigest,
      liveIdentityIds: participantIdentityIds,
      senderIdentityId: sender?.identityId ?? null,
      senderPersonId: sender?.personId ?? null,
      snapshot,
      mode: evaluateConversationMode(snapshot),
      householdId: household?.id ?? null,
      householdControlEpoch: household?.controlEpoch ?? null,
    };
  }

  private async isExactRegisteredGroupSender(
    transaction: Transaction,
    reconciled: ReconciledConversation,
  ): Promise<boolean> {
    if (!reconciled.senderIdentityId || !reconciled.senderPersonId) return false;
    const rows = await transaction<{ eligible: boolean }[]>`
      select exists(
        select 1
        from epoch_participants participant
        join people person on person.id = participant.person_id and person.status = 'registered'
        join person_identities identity on identity.id = participant.person_identity_id
          and identity.person_id = person.id and identity.status = 'verified'
        join participant_policies policy on policy.conversation_id = ${reconciled.conversationId}
          and policy.person_id = person.id and policy.status = 'active'
          and policy.allow_content_processing
        where participant.participant_epoch_id = ${reconciled.epochId}
          and participant.person_identity_id = ${reconciled.senderIdentityId}
          and participant.person_id = ${reconciled.senderPersonId}
          and participant.registration_status = 'registered'
          and participant.consented_at is not null
      ) as eligible
    `;
    return rows[0]?.eligible === true;
  }

  private async resolveObserveOnlyInvocation(
    transaction: Transaction,
    event: LinqMessageReceivedEvent,
    reconciled: ReconciledConversation,
  ): Promise<GroupInvocation | null> {
    const text = messageText(event).trim();
    const leading = leadingGroupInvocation(text);
    if (leading) return leading;
    if (!text || !event.message.replyTo) return null;

    const rows = await transaction<{ matching_effects: number | string }[]>`
      select count(distinct effect.id) as matching_effects
      from outbox effect
      join effect_receipts receipt on receipt.outbox_id = effect.id
        and receipt.provider_receipt_id = ${event.message.replyTo.providerMessageId}
        and receipt.status in ('submitted', 'confirmed')
      where effect.effect_kind = 'linq.message'
        and effect.conversation_id = ${reconciled.conversationId}
        and effect.participant_epoch_id = ${reconciled.epochId}
        and effect.expected_participant_digest = ${reconciled.appParticipantDigest}
        and effect.status in ('submitted', 'confirmed')
    `;
    return Number(rows[0]?.matching_effects ?? 0) === 1 ? { basis: "proven_reply", requestText: text } : null;
  }

  /** A verified provider reply is stronger delivery proof than a missing or failed receipt webhook. */
  private async confirmOutboundByReply(
    transaction: Transaction,
    event: LinqMessageReceivedEvent,
    reconciled: ReconciledConversation,
  ): Promise<void> {
    const providerMessageId = event.message.replyTo?.providerMessageId;
    if (!providerMessageId) return;
    const effects = await transaction<{ readonly id: string; readonly idempotency_key: string }[]>`
      select distinct effect.id, effect.idempotency_key
      from outbox effect
      join effect_receipts receipt on receipt.outbox_id = effect.id
        and receipt.provider_receipt_id = ${providerMessageId}
      where effect.effect_kind = 'linq.message'
        and effect.conversation_id = ${reconciled.conversationId}
        and effect.participant_epoch_id = ${reconciled.epochId}
        and effect.expected_participant_digest = ${reconciled.appParticipantDigest}
        and effect.status <> 'cancelled'
      order by effect.id
      limit 2
    `;
    if (effects.length !== 1 || !effects[0]) return;

    const occurredAt = new Date(event.message.sentAt);
    const receiptJson = canonicalJson({
      kind: "linq_reply_delivery_proof",
      providerEventId: event.providerEventId,
      providerMessageId,
      replyMessageId: event.message.providerMessageId,
      occurredAt: event.message.sentAt,
    });
    const encrypted = this.secretBox.encrypt(receiptJson, "effect-receipt");
    await transaction`
      insert into effect_receipts (
        id, outbox_id, idempotency_key, provider_receipt_id, status,
        receipt_digest, receipt_ciphertext, receipt_key_version, occurred_at,
        reconciled_at, error_code
      ) values (
        ${randomUUID()}, ${effects[0].id}, ${effects[0].idempotency_key}, ${providerMessageId},
        'confirmed', ${sha256Hex(receiptJson)},
        ${Buffer.from(JSON.stringify(encrypted), "utf8")}, ${encrypted.kid},
        ${occurredAt}, ${occurredAt}, null
      ) on conflict do nothing
    `;
    await transaction`
      update outbox set status = 'confirmed', available_at = now(),
        lease_owner = null, lease_token = null, lease_expires_at = null,
        last_error_code = null, updated_at = now()
      where id = ${effects[0].id} and status <> 'cancelled'
    `;
  }

  private async processLinqEvent(providerEventId: string): Promise<ProcessReceipt> {
    return this.database.begin(async (transaction) => {
      const rows = await transaction<ProviderEventRow[]>`
        select id, provider_event_id, envelope_ciphertext, processing_status
        from provider_events where provider = 'linq' and provider_event_id = ${providerEventId}
        for update
      `;
      const row = rows[0];
      if (!row) throw new Error("Linq provider event does not exist");
      if (row.processing_status === "processed" || row.processing_status === "ignored") {
        return {
          accepted: true,
          duplicate: true,
          disposition: row.processing_status,
          ids: { providerEventId: row.id },
        };
      }
      const record = JSON.parse(
        this.secretBox
          .decrypt(JSON.parse(row.envelope_ciphertext.toString("utf8")), `provider-event:${providerEventId}`)
          .toString("utf8"),
      ) as StoredLinqEvent;
      let disposition: string = record.classification;
      switch (record.classification) {
        case "enrollment":
          disposition = await this.handleEnrollment(transaction, record);
          break;
        case "stop":
          disposition = await this.handleStop(transaction, record);
          break;
        case "full":
          disposition = await this.handleFullLinqEvent(transaction, row.id, record);
          break;
        case "observe_only":
          disposition = await this.handleObserveOnlyLinqEvent(transaction, row.id, record);
          break;
        case "receipt":
          disposition = await this.handleProviderReceipt(transaction, record);
          break;
        case "routing_only":
          disposition = "ignored";
          break;
      }
      await transaction`
        update provider_events set processing_status = ${disposition === "ignored" ? "ignored" : "processed"},
          processed_at = now(), error_code = null where id = ${row.id}
      `;
      return { accepted: true, duplicate: false, disposition, ids: { providerEventId: row.id } };
    });
  }

  private async handleObserveOnlyLinqEvent(
    transaction: Transaction,
    internalProviderEventId: string,
    record: StoredLinqEvent,
  ): Promise<string> {
    if (record.event?.eventType === "linq.message.edited") {
      return this.invalidateEditedLinqMessageSource(transaction, record.event, record.routing);
    }
    if (record.event?.eventType !== "linq.message.received") return "observed_silently";
    let invocationAuthority:
      | {
          readonly person: { readonly id: string; readonly controlEpoch: number };
          readonly conversation: { readonly id: string; readonly authorityVersion: number };
          readonly deadlineAt: Date;
        }
      | undefined;
    if (record.invocation && record.routing.senderPersonId) {
      const people = await transaction<{ readonly control_epoch: number | string }[]>`
        select control_epoch from people
        where id = ${record.routing.senderPersonId} and status = 'registered'
        for share
      `;
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        record.routing.conversationId,
      );
      if (
        !people[0] ||
        snapshot.participantEpochId !== record.routing.participantEpochId ||
        snapshot.participantSetDigest !== record.routing.appParticipantDigest
      ) {
        return "observed_silently";
      }
      invocationAuthority = {
        person: {
          id: record.routing.senderPersonId,
          controlEpoch: Number(people[0].control_epoch),
        },
        conversation: {
          id: record.routing.conversationId,
          authorityVersion: snapshot.authorityVersion,
        },
        deadlineAt: new Date(Date.parse(record.event.message.sentAt) + MAX_LIVE_GROUP_INVOCATION_AGE_MS),
      };
    }
    await new DurableWork(transaction, this.secretBox).enqueue({
      kind: "orchestrate.linq_observation",
      idempotencyKey: `orchestrate:linq-observation:${internalProviderEventId}`,
      payload: { internalProviderEventId },
      ...(invocationAuthority ?? {}),
      maxAttempts: 5,
    });
    return record.invocation ? "private_invocation_queued" : "observation_queued";
  }

  /**
   * A private conversational response and the one-time source activation are
   * committed in separate transactions. That makes the response durable before
   * the activation offer can enter the outbox, while a retry can safely finish
   * either half through their independent idempotency keys.
   */
  private async completePrivateDmOrchestration(
    input: Extract<AppEnvelope, { kind: "linq.private_dm_orchestration_complete" }>,
  ): Promise<ProcessReceipt> {
    const response = await this.database.begin(async (transaction) => {
      const source = await this.requireProcessedPrivateDmSource(transaction, input.internalProviderEventId);
      const sourceText = messageText(source.event);
      if (input.response.kind === "greeting_acknowledgment") {
        if (!isNaturalPrivateGreeting("direct", sourceText)) {
          throw new UnauthorizedError("Private DM is not a deterministic greeting");
        }
        const googleOffer = await this.queueParentGoogleActivationOffer(
          transaction,
          source.personId,
          "reengagement_after_expiry",
          source.record,
          input.internalProviderEventId,
        );
        if (googleOffer) {
          return {
            ...googleOffer,
            personId: source.personId,
            googleActivationIncluded: true,
          };
        }
      }
      let text: string;
      let operation: string;
      let idempotencyKey: string;
      if (input.response.kind === "greeting_acknowledgment") {
        text = "Hi! I’m here. What can I help you with?";
        operation = "private_dm_greeting";
        idempotencyKey = `private-dm-greeting:${input.internalProviderEventId}`;
      } else {
        if (!isExplicitPrivateQuestion(sourceText)) {
          throw new UnauthorizedError("Private DM is not an explicit question");
        }
        text = input.response.text.trim();
        if (!text || text.length > 10_000) {
          throw new UnauthorizedError("Private DM answer is outside the allowed bounds");
        }
        operation = "general_answer";
        idempotencyKey = `general-answer:${input.internalProviderEventId}`;
      }

      const queued = await this.queueAuthorizedConversationMessage(
        transaction,
        source.record,
        source.snapshot,
        text,
        "direct_response",
        operation,
        null,
        new Date(Date.now() + 5 * 60_000),
        idempotencyKey,
      );
      if (!queued) throw new StaleAuthorityError("Private DM response is no longer authorized");
      return { ...queued, personId: source.personId, googleActivationIncluded: false };
    });

    const activationOffered = response.googleActivationIncluded
      ? true
      : await this.database.begin(async (transaction) => {
          const source = await this.requireProcessedPrivateDmSource(
            transaction,
            input.internalProviderEventId,
          );
          if (source.personId !== response.personId) {
            throw new StaleAuthorityError("Private DM sender changed before activation");
          }
          const committed = await transaction<{ readonly created_at: Date }[]>`
        select created_at from outbox
        where id = ${response.outboxId}
          and person_id = ${source.personId}
          and conversation_id = ${source.record.routing.conversationId}
          and participant_epoch_id = ${source.record.routing.participantEpochId}
          and expected_participant_digest = ${source.record.routing.appParticipantDigest}
          and status in ('pending', 'leased', 'retry', 'submitted', 'confirmed')
        for share
      `;
          const committedResponse = committed[0];
          if (!committedResponse) return false;

          const offered = await this.queueParentGoogleActivationOffer(
            transaction,
            source.personId,
            "existing_steward_dm",
            source.record,
          );
          if (offered) {
            await transaction`
          update outbox
          set available_at = greatest(
            available_at,
            ${new Date(committedResponse.created_at.getTime() + 1)}
          )
          where idempotency_key = ${`google-parent-activation:${source.personId}`}
        `;
          }
          return offered !== null;
        });

    return {
      accepted: true,
      duplicate: !response.created && !activationOffered,
      disposition: activationOffered
        ? "private_dm_response_then_google_activation_queued"
        : "private_dm_response_queued",
      ids: {
        providerEventId: input.internalProviderEventId,
        responseOutboxId: response.outboxId,
      },
    };
  }

  private async requireProcessedPrivateDmSource(
    transaction: Transaction,
    internalProviderEventId: string,
  ): Promise<ProcessedPrivateDmSource> {
    const rows = await transaction<ProviderEventRow[]>`
      select id, provider_event_id, envelope_ciphertext, processing_status
      from provider_events
      where id = ${internalProviderEventId} and provider = 'linq'
      for share
    `;
    const row = rows[0];
    if (row?.processing_status !== "processed") {
      throw new StaleAuthorityError("Private DM source is no longer processed");
    }
    const record = JSON.parse(
      this.secretBox
        .decrypt(
          JSON.parse(row.envelope_ciphertext.toString("utf8")),
          `provider-event:${row.provider_event_id}`,
        )
        .toString("utf8"),
    ) as StoredLinqEvent;
    const event = record.event;
    if (
      record.classification !== "full" ||
      record.routing.chatKind !== "direct" ||
      event?.eventType !== "linq.message.received" ||
      !record.routing.senderIdentityId ||
      !record.routing.senderPersonId ||
      record.routing.liveIdentityIds.length !== 1 ||
      record.routing.liveIdentityIds[0] !== record.routing.senderIdentityId
    ) {
      throw new UnauthorizedError("Provider event is not an eligible private DM");
    }

    const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
      record.routing.conversationId,
    );
    const participant = snapshot.participants.length === 1 ? snapshot.participants[0] : null;
    if (
      snapshot.conversationKind !== "direct" ||
      snapshot.conversationStatus !== "active" ||
      snapshot.participantEpochId !== record.routing.participantEpochId ||
      snapshot.participantSetDigest !== record.routing.appParticipantDigest ||
      participant?.personIdentityId !== record.routing.senderIdentityId ||
      participant.personId !== record.routing.senderPersonId ||
      participant.registrationStatus !== "registered" ||
      participant.consentedAt === null ||
      participant.policy?.allowContentProcessing !== true ||
      participant.policy.allowDirectResponses !== true
    ) {
      throw new StaleAuthorityError("Private DM authority changed before response");
    }
    return {
      record,
      event,
      snapshot,
      personId: record.routing.senderPersonId,
    };
  }

  private async commitPrivateGroupInvocationResponse(
    internalProviderEventId: string,
    responseCandidate: string,
    evidenceSourceRevisionIdsCandidate: readonly string[],
  ): Promise<ProcessReceipt> {
    const responseText = responseCandidate.trim();
    if (!responseText || responseText.length > 10_000) {
      throw new UnauthorizedError("Private group invocation response is outside the allowed bounds");
    }
    const evidenceSourceRevisionIds = exactEvidenceSourceRevisionIds(evidenceSourceRevisionIdsCandidate);
    return this.database.begin(async (transaction) => {
      const events = await transaction<ProviderEventRow[]>`
        select id, provider_event_id, envelope_ciphertext, processing_status
        from provider_events
        where id = ${internalProviderEventId} and provider = 'linq'
        for update
      `;
      const row = events[0];
      if (row?.processing_status !== "processed") {
        throw new StaleAuthorityError("Private group invocation source is no longer processed");
      }
      const record = JSON.parse(
        this.secretBox
          .decrypt(
            JSON.parse(row.envelope_ciphertext.toString("utf8")),
            `provider-event:${row.provider_event_id}`,
          )
          .toString("utf8"),
      ) as StoredLinqEvent;
      if (
        record.classification !== "observe_only" ||
        !record.invocation ||
        record.routing.chatKind !== "group" ||
        record.event?.eventType !== "linq.message.received" ||
        !record.routing.senderIdentityId ||
        !record.routing.senderPersonId
      ) {
        throw new UnauthorizedError("Provider event is not an eligible private group invocation");
      }

      const sources = await transaction<
        {
          group_authority_version: number | string;
          group_epoch_id: string;
          group_participant_digest: string;
          person_control_epoch: number | string;
          identity_subject_ciphertext: Buffer;
          household_id: string | null;
          household_control_epoch: number | string | null;
        }[]
      >`
        select conversation.authority_version as group_authority_version,
          epoch.id as group_epoch_id, epoch.participant_set_digest as group_participant_digest,
          person.control_epoch as person_control_epoch,
          identity.subject_ciphertext as identity_subject_ciphertext,
          conversation.household_id, household.control_epoch as household_control_epoch
        from conversations conversation
        join participant_epochs epoch on epoch.id = conversation.current_epoch_id
          and epoch.ended_at is null
        join epoch_participants participant on participant.participant_epoch_id = epoch.id
          and participant.person_identity_id = ${record.routing.senderIdentityId}
          and participant.person_id = ${record.routing.senderPersonId}
          and participant.registration_status = 'registered' and participant.consented_at is not null
        join people person on person.id = participant.person_id and person.status = 'registered'
        join person_identities identity on identity.id = participant.person_identity_id
          and identity.person_id = person.id and identity.status = 'verified'
          and identity.subject_ciphertext is not null
        join participant_policies policy on policy.conversation_id = conversation.id
          and policy.person_id = person.id and policy.status = 'active'
          and policy.allow_content_processing
        left join households household on household.id = conversation.household_id
        where conversation.id = ${record.routing.conversationId}
          and conversation.kind = 'group' and conversation.status = 'active'
          and epoch.id = ${record.routing.participantEpochId}
          and epoch.participant_set_digest = ${record.routing.appParticipantDigest}
      `;
      const source = sources[0];
      if (!source) throw new StaleAuthorityError("Private group invocation audience changed");

      const evidenceReadAt = new Date();
      const sourceIntelligence = new PostgresSourceIntelligence(transaction, this.secretBox, {
        rawRetentionDays: this.config.defaults.rawSourceRetentionDays,
        privateCandidateRetentionDays: 7,
      });
      let evidenceAccessExpiresAt = new Date(Date.now() + 5 * 60_000);
      for (const sourceRevisionId of evidenceSourceRevisionIds) {
        const evidence = await sourceIntelligence
          .read({
            kind: "source_revision",
            sourceRevisionId,
            scope: {
              kind: "conversation_epoch",
              participantEpochId: record.routing.participantEpochId,
            },
            privateViewerPersonId: record.routing.senderPersonId,
            asOf: evidenceReadAt.toISOString(),
          })
          .catch((error: unknown) => {
            if (error instanceof NotFoundError || error instanceof UnauthorizedError) {
              throw new StaleAuthorityError("Private invocation evidence changed before commit");
            }
            throw error;
          });
        if (evidence.kind !== "source_revision") {
          throw new UnauthorizedError("Private invocation evidence is no longer readable");
        }
        const accessExpiresAt = new Date(evidence.accessExpiresAt);
        if (accessExpiresAt <= new Date(evidenceReadAt.getTime() + 3 * 60_000)) {
          throw new StaleAuthorityError("Private invocation evidence is too close to expiry to send");
        }
        if (accessExpiresAt < evidenceAccessExpiresAt) evidenceAccessExpiresAt = accessExpiresAt;
      }

      const routes = await transaction<
        {
          conversation_id: string;
          authority_version: number | string;
          participant_epoch_id: string;
          participant_set_digest: string;
          external_channel_id: string;
          latest_participant_digest: string;
        }[]
      >`
        select direct.id as conversation_id, direct.authority_version,
          direct_epoch.id as participant_epoch_id,
          direct_epoch.participant_set_digest, channel.external_channel_id,
          channel.latest_participant_digest
        from conversations direct
        join conversation_channels channel on channel.conversation_id = direct.id
          and channel.provider = 'linq' and channel.status = 'active'
          and channel.latest_participant_digest is not null
        join participant_epochs direct_epoch on direct_epoch.id = direct.current_epoch_id
          and direct_epoch.ended_at is null
        join epoch_participants direct_participant
          on direct_participant.participant_epoch_id = direct_epoch.id
          and direct_participant.person_identity_id = ${record.routing.senderIdentityId}
          and direct_participant.person_id = ${record.routing.senderPersonId}
          and direct_participant.registration_status = 'registered'
          and direct_participant.consented_at is not null
        where direct.kind = 'direct' and direct.status = 'active'
          and (select count(*) from epoch_participants exact
            where exact.participant_epoch_id = direct_epoch.id) = 1
        order by direct.updated_at desc, direct.id
        limit 1
      `;
      const route = routes[0];
      const sourceConversation = {
        id: record.routing.conversationId,
        authorityVersion: Number(source.group_authority_version),
        participantEpochId: source.group_epoch_id,
        participantSetDigest: source.group_participant_digest,
      };
      const commonEffect = {
        actorPersonId: record.routing.senderPersonId,
        person: {
          id: record.routing.senderPersonId,
          controlEpoch: Number(source.person_control_epoch),
        },
        ...(source.household_id && source.household_control_epoch
          ? {
              household: {
                id: source.household_id,
                controlEpoch: Number(source.household_control_epoch),
              },
            }
          : {}),
        sourceConversation,
        effectKind: "linq.message" as const,
        idempotencyKey: `private-group-invocation:${internalProviderEventId}`,
        data: {
          responseDigest: sha256Hex(responseText),
          invocationBasis: record.invocation.basis,
          evidenceSourceRevisionIds,
        },
        evidenceSourceRevisionIds,
        policy: { exactPrivateRecipient: true, sourceGroupSilent: true },
        reasonCodes: [
          "registered_exact_group_sender",
          "deterministic_group_invocation",
          "private_response_only",
        ],
        authorizationExpiresAt: evidenceAccessExpiresAt,
      };

      if (route) {
        const authorization = await new PostgresConversationAuthority(transaction).authorizeSend({
          conversationId: route.conversation_id,
          expectedParticipantEpochId: route.participant_epoch_id,
          expectedParticipantSetDigest: route.participant_set_digest,
          liveParticipantIdentityIds: [record.routing.senderIdentityId],
          sendKind: "direct_response",
          operation: "private_group_invocation",
          ruleId: null,
        });
        if (!authorization.allowed) {
          throw new UnauthorizedError("Exact private conversation does not permit the response");
        }
        await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
          ...commonEffect,
          conversation: {
            id: route.conversation_id,
            authorityVersion: Number(route.authority_version),
          },
          participantEpochId: route.participant_epoch_id,
          expectedParticipantDigest: route.participant_set_digest,
          target: {
            providerChatId: route.external_channel_id,
            personId: record.routing.senderPersonId,
          },
          payload: {
            providerChatId: route.external_channel_id,
            expectedProviderParticipantDigest: route.latest_participant_digest,
            text: responseText,
          },
        });
      } else {
        const recipient = this.secretBox
          .decrypt(
            JSON.parse(source.identity_subject_ciphertext.toString("utf8")),
            `identity-subject:${record.routing.senderIdentityId}`,
          )
          .toString("utf8");
        if (!/^\+[1-9]\d{6,14}$/u.test(recipient) && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/u.test(recipient)) {
          throw new UnauthorizedError("Exact sender does not have a safe private Linq route");
        }
        const sanitizedFirstMessage = responseText.replace(/https?:\/\/\S+/giu, "[link omitted]").trim();
        const firstMessageText =
          sanitizedFirstMessage ||
          "You addressed me in a group. Text me here and I’ll continue the answer privately.";
        await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
          ...commonEffect,
          target: { recipient, personId: record.routing.senderPersonId },
          payload: { recipient, text: firstMessageText },
        });
      }

      return {
        accepted: true,
        duplicate: false,
        disposition: "private_group_invocation_response_queued",
        ids: { providerEventId: internalProviderEventId },
      };
    });
  }

  private async handleEnrollment(transaction: Transaction, record: StoredLinqEvent): Promise<string> {
    const identityId = record.routing.senderIdentityId;
    const personId = record.routing.senderPersonId;
    if (!identityId || !personId) return "ignored";
    const promptRows = await transaction<{ prompted: boolean }[]>`
      select exists(
        select 1 from outbox
        where idempotency_key like ${`enrollment:${record.routing.providerChatId}:%`}
        union all
        select 1
        from invitations invitation
        join outbox effect
          on effect.idempotency_key = 'household-enrollment-invitation:' || invitation.id::text
          and effect.status in ('pending', 'leased', 'retry', 'submitted', 'confirmed')
        where invitation.invitee_identity_id = ${identityId}
          and invitation.status = 'pending' and invitation.expires_at > now()
      ) as prompted
    `;
    if (record.enrollmentAction !== "consent" || !promptRows[0]?.prompted) {
      await this.queueSystemEnrollmentMessage(
        transaction,
        record,
        `Hi—I’m Florence, a family Chief of Staff. I can read what you send me and help your family keep logistics covered. When I’m added to a group, new messages may be retained as private context for that exact participant lineup, but I stay silent there unless every current participant registers and approves writing in that exact group. Permitted raw context is kept for up to ${this.config.defaults.rawSourceRetentionDays} days; you can change controls or delete data later. Privacy: ${this.config.publicBaseUrl}/privacy\n\nWould you like to create your private Florence account? Reply yes to agree. You can text STOP any time.`,
      );
      return "enrollment_prompted";
    }
    const identities = await transaction<
      {
        authority_version: number | string;
        status: string;
        person_status: string;
        display_name_ciphertext: Buffer | null;
      }[]
    >`
      select identity.authority_version, identity.status, person.status as person_status,
        person.display_name_ciphertext
      from person_identities identity join people person on person.id = identity.person_id
      where identity.id = ${identityId} for update of identity, person
    `;
    const current = identities[0];
    if (!current) return "ignored";
    const consentedAt = new Date();
    if (current.status !== "verified") {
      if (current.person_status === "stopped") {
        await transaction`
          update people set status = 'provisional', updated_at = ${consentedAt}
          where id = ${personId} and status = 'stopped'
        `;
      }
      await new PostgresIdentityRelationships(transaction).claimIdentity({
        identityId,
        confirmedByIdentityId: identityId,
        expectedIdentityAuthorityVersion: Number(current.authority_version),
        consentedAt: consentedAt.toISOString(),
        timezone: this.config.defaults.timezone,
      });
    } else if (current.person_status !== "registered") {
      await transaction`
        update people
        set status = 'registered', consented_at = ${consentedAt},
          registered_at = coalesce(registered_at, ${consentedAt}),
          timezone = coalesce(timezone, ${this.config.defaults.timezone}),
          onboarding_step = ${current.display_name_ciphertext ? "complete" : "name_pending"},
          authority_version = authority_version + 1,
          control_epoch = control_epoch + 1, updated_at = ${consentedAt}
        where id = ${personId} and status in ('provisional', 'stopped')
      `;
    }
    await transaction`
      update people
      set onboarding_step = ${current.display_name_ciphertext ? "complete" : "name_pending"},
        updated_at = ${consentedAt}
      where id = ${personId} and status = 'registered'
    `;
    await this.consentPersonAcrossObservedEpochs(transaction, personId, consentedAt);
    const affectedGroups = await transaction<{ id: string }[]>`
      select distinct conversation.id
      from conversations conversation
      join participant_epochs epoch on epoch.id = conversation.current_epoch_id
        and epoch.ended_at is null
      join epoch_participants participant on participant.participant_epoch_id = epoch.id
        and participant.person_id = ${personId}
      where conversation.kind = 'group' and conversation.status = 'active'
      order by conversation.id
    `;
    for (const group of affectedGroups) {
      await this.queuePrivateGroupReadyNotices(transaction, group.id, "group-ready");
    }
    const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
      record.routing.conversationId,
    );
    const knownName = decryptPersonName(this.secretBox, personId, current.display_name_ciphertext);
    const googleOffered = knownName
      ? await this.queueParentGoogleActivationOffer(transaction, personId, "household_resolved", record)
      : false;
    if (!googleOffered) {
      const welcome = knownName
        ? await this.returningEnrollmentWelcome(transaction, record, knownName)
        : "You’re in. I’m Florence—what should I call you?";
      await this.queueAuthorizedConversationMessage(
        transaction,
        record,
        snapshot,
        welcome,
        "direct_response",
        "onboarding_welcome",
        null,
        new Date(consentedAt.getTime() + 24 * 60 * 60_000),
      );
    }
    return "registered";
  }

  /**
   * A verified affirmative reply to Florence's private consent question is the
   * person's explicit registration.
   * It applies their conservative default to every exact chat audience in which
   * that verified identity was observed. Still-retained history receives a private
   * view even if membership has since changed; this never enables group writing.
   */
  private async consentPersonAcrossObservedEpochs(
    transaction: Transaction,
    personId: string,
    consentedAt: Date,
  ): Promise<void> {
    const epochs = await transaction<{ participant_epoch_id: string }[]>`
      select distinct epoch.id as participant_epoch_id
      from participant_epochs epoch
      join conversations conversation on conversation.id = epoch.conversation_id
        and conversation.status = 'active'
      join epoch_participants participant on participant.participant_epoch_id = epoch.id
      join person_identities identity on identity.id = participant.person_identity_id
        and identity.person_id = participant.person_id and identity.status = 'verified'
      where participant.person_id = ${personId}
      order by epoch.id
    `;
    const authority = new PostgresConversationAuthority(transaction);
    const sources = new PostgresSourceIntelligence(transaction, this.secretBox, {
      rawRetentionDays: this.config.defaults.rawSourceRetentionDays,
      privateCandidateRetentionDays: 7,
    });
    for (const epoch of epochs) {
      await authority.consentToEpoch({
        participantEpochId: epoch.participant_epoch_id,
        personId,
        policy: {
          allowContentProcessing: true,
          allowDirectResponses: true,
          allowProactiveWrites: false,
          retentionSeconds: this.config.defaults.rawSourceRetentionDays * 86_400,
        },
        consentedAt: consentedAt.toISOString(),
      });
      await sources.apply({
        kind: "grant_conversation_private_views",
        participantEpochId: epoch.participant_epoch_id,
        personId,
        grantedAt: consentedAt.toISOString(),
      });
    }
  }

  private async returningEnrollmentWelcome(
    transaction: Transaction,
    record: StoredLinqEvent,
    displayName: string,
  ): Promise<string> {
    const personId = record.routing.senderPersonId;
    const identityId = record.routing.senderIdentityId;
    if (!personId || !identityId || record.routing.chatKind !== "direct") {
      throw new UnauthorizedError("Onboarding links require the exact private Florence conversation");
    }
    const memberships = await transaction<{ household_id: string; dependent_count: number | string }[]>`
      select membership.household_id,
        count(dependent.id) filter (where dependent.role = 'dependent' and dependent.status = 'active')
          as dependent_count
      from household_memberships membership
      left join household_memberships dependent on dependent.household_id = membership.household_id
      where membership.person_id = ${personId} and membership.status = 'active'
      group by membership.household_id, membership.joined_at
      order by membership.joined_at limit 1
    `;
    const invitations = memberships[0]
      ? []
      : await transaction<
          {
            invitation_id: string;
            inviter_person_id: string;
            inviter_name_ciphertext: Buffer | null;
          }[]
        >`
          select invitation.id as invitation_id, inviter.person_id as inviter_person_id,
            inviter_person.display_name_ciphertext as inviter_name_ciphertext
          from invitations invitation
          join person_identities invitee_identity on invitee_identity.id = invitation.invitee_identity_id
            and invitee_identity.person_id = ${personId} and invitee_identity.status = 'verified'
          join household_memberships inviter on inviter.id = invitation.invited_by_membership_id
          join people inviter_person on inviter_person.id = inviter.person_id
          join households household on household.id = invitation.household_id
            and household.membership_version = invitation.household_membership_version
          where invitation.status = 'pending' and invitation.expires_at > now()
          order by invitation.created_at limit 1
        `;
    const invitation = invitations[0];
    await transaction`
      update people
      set onboarding_step = ${memberships[0] || invitation ? "complete" : "family_pending"}, updated_at = now()
      where id = ${personId} and status = 'registered'
    `;
    if (!memberships[0] && !invitation) {
      return `Nice to meet you, ${displayName}. Would you like me to set up your private family space? Then I can learn the children, schools, activities, and routines you want me to help keep covered.`;
    }
    const handoff = await new PostgresWebAuth(
      transaction,
      this.secretBox,
      this.config.security.tokenKey,
    ).createHandoff({
      personId,
      privateIdentityId: identityId,
      privateConversationId: record.routing.conversationId,
      purpose: invitation ? "invitation" : "web_sign_in",
      context: invitation
        ? { invitationId: invitation.invitation_id, returnPath: "/people" }
        : { onboarding: true, returnPath: "/people" },
      expiresInSeconds: 10 * 60,
    });
    const link = `${this.config.publicBaseUrl}/handoff/${handoff.token}`;
    if (invitation) {
      const inviterName =
        decryptPersonName(this.secretBox, invitation.inviter_person_id, invitation.inviter_name_ciphertext) ??
        "Your family member";
      return `Nice to meet you, ${displayName}. ${inviterName} invited you to join their Florence family. I won’t ask you to repeat family details they already shared. Review it privately here: ${link}\n\nIf the link expires, text me “settings” for a fresh one.`;
    }
    if (memberships[0]) {
      const knownChildren = Number(memberships[0].dependent_count);
      return `Welcome back, ${displayName}. Your family space${knownChildren > 0 ? ` already includes ${knownChildren} ${knownChildren === 1 ? "child" : "children"}` : " is ready"}. Open your private controls here: ${link}`;
    }
    throw new Error("Family onboarding state changed unexpectedly");
  }

  private async handleStop(transaction: Transaction, record: StoredLinqEvent): Promise<string> {
    const personId = record.routing.senderPersonId;
    if (!personId) return "ignored";
    if (record.routing.chatKind === "direct") {
      if (record.routing.senderIdentityId) {
        await transaction`
          update invitations set status = 'declined', updated_at = now()
          where invitee_identity_id = ${record.routing.senderIdentityId} and status = 'pending'
        `;
      }
      await transaction`
        update people set status = 'stopped', control_epoch = control_epoch + 1,
          authority_version = authority_version + 1, onboarding_step = 'consent_pending',
          updated_at = now()
        where id = ${personId} and status not in ('deleted', 'merged')
      `;
      const affectedConversations = await transaction<{ conversation_id: string }[]>`
        select conversation.id as conversation_id
        from conversations conversation
        join participant_epochs epoch on epoch.id = conversation.current_epoch_id
          and epoch.ended_at is null
        join epoch_participants participant on participant.participant_epoch_id = epoch.id
        where participant.person_id = ${personId}
        for update of conversation
      `;
      await transaction`
        update epoch_participants participant
        set registration_status = 'provisional', consented_at = null
        from conversations conversation, participant_epochs epoch
        where conversation.current_epoch_id = epoch.id and epoch.ended_at is null
          and participant.participant_epoch_id = epoch.id
          and participant.person_id = ${personId}
      `;
      await transaction`
        update participant_policies policy
        set status = 'revoked', superseded_at = now()
        where policy.person_id = ${personId} and policy.status = 'active'
          and policy.conversation_id = any(${transaction.array(affectedConversations.map((entry) => entry.conversation_id))}::uuid[])
      `;
      await transaction`
        update conversations
        set authority_version = authority_version + 1, updated_at = now()
        where id = any(${transaction.array(affectedConversations.map((entry) => entry.conversation_id))}::uuid[])
      `;
      await transaction`
        update person_sessions set revoked_at = now()
        where person_id = ${personId} and revoked_at is null
      `;
    } else {
      await new PostgresConversationAuthority(transaction).applyNarrowing({
        conversationId: record.routing.conversationId,
        actorPersonId: personId,
        kind: "stop",
        retentionSeconds: null,
        reason: "participant_stop",
        appliedAt: new Date().toISOString(),
      });
    }
    return "stopped";
  }

  private async handleFullLinqEvent(
    transaction: Transaction,
    internalProviderEventId: string,
    record: StoredLinqEvent,
  ): Promise<string> {
    const event = record.event;
    if (!event) return "ignored";
    if (event.eventType === "linq.message.edited") {
      return this.invalidateEditedLinqMessageSource(transaction, event, record.routing);
    }
    if (event.eventType !== "linq.message.received") return "observed";
    const text = messageText(event);
    const normalizedCommand = text.trim().toLowerCase();
    const senderPersonId = record.routing.senderPersonId;
    const registeredSender = senderPersonId
      ? (
          await transaction<
            {
              display_name_ciphertext: Buffer | null;
              onboarding_step: string;
              control_epoch: number | string;
            }[]
          >`
              select display_name_ciphertext, onboarding_step, control_epoch from people
              where id = ${senderPersonId} and status = 'registered'
            `
        )[0]
      : null;
    const person = record.routing.chatKind === "direct" ? registeredSender : null;
    const naturalGreeting = isNaturalPrivateGreeting(record.routing.chatKind, text);
    const displayName =
      record.routing.chatKind === "direct" && !naturalGreeting
        ? parseNameResponse(text, person?.onboarding_step === "name_pending")
        : null;
    if (displayName) {
      const personId = record.routing.senderPersonId;
      if (!personId) return "ignored";
      const encryptedName = this.secretBox.encrypt(displayName, `person-display-name:${personId}`);
      await transaction`
        update people
        set display_name_ciphertext = ${Buffer.from(JSON.stringify(encryptedName), "utf8")},
          display_name_key_version = ${encryptedName.kid}, onboarding_step = 'complete',
          updated_at = now()
        where id = ${personId} and status = 'registered'
      `;
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        record.routing.conversationId,
      );
      const googleOffered = await this.queueParentGoogleActivationOffer(
        transaction,
        personId,
        "household_resolved",
        record,
      );
      if (!googleOffered) {
        const nextStep = await this.returningEnrollmentWelcome(transaction, record, displayName);
        await this.queueAuthorizedConversationMessage(
          transaction,
          record,
          snapshot,
          nextStep,
          "direct_response",
          "profile_name_updated",
          null,
          new Date(Date.now() + 24 * 60 * 60_000),
        );
      }
      return "profile_name_updated";
    }
    if (
      record.routing.chatKind === "direct" &&
      person?.onboarding_step === "name_pending" &&
      naturalGreeting
    ) {
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        record.routing.conversationId,
      );
      await this.queueAuthorizedConversationMessage(
        transaction,
        record,
        snapshot,
        "Hi! What should I call you?",
        "direct_response",
        "profile_name_requested",
      );
      return "profile_name_requested";
    }
    if (record.routing.chatKind === "direct" && person && isSelfNameQuestion(text)) {
      const personId = record.routing.senderPersonId;
      if (!personId) return "ignored";
      const knownName = decryptPersonName(this.secretBox, personId, person.display_name_ciphertext);
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        record.routing.conversationId,
      );
      await this.queueAuthorizedConversationMessage(
        transaction,
        record,
        snapshot,
        knownName
          ? `I have your name as ${knownName}.`
          : "I don’t have your name yet—what should I call you?",
        "direct_response",
        "profile_name_recalled",
      );
      return "profile_name_recalled";
    }
    if (
      record.routing.chatKind === "direct" &&
      person &&
      /^(?:not now|no thanks|skip google|don['’]t connect google|do not connect google)$/u.test(
        normalizedCommand,
      )
    ) {
      const personId = record.routing.senderPersonId;
      if (!personId) return "ignored";
      const recentOffers = await transaction<{ readonly present: boolean }[]>`
        select exists(
          select 1 from auth_handoffs handoff
          where handoff.person_id = ${personId} and handoff.purpose = 'google_connect'
            and handoff.created_at > now() - interval '24 hours'
        ) and not exists(
          select 1 from integrations integration
          where integration.person_id = ${personId} and integration.provider = 'google'
            and integration.account_kind = 'personal_family'
            and integration.status <> 'revoked'
        ) as present
      `;
      if (recentOffers[0]?.present) {
        await transaction`
          update people set google_activation_suppressed_at = now(), updated_at = now()
          where id = ${personId} and status = 'registered'
        `;
        const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
          record.routing.conversationId,
        );
        await this.queueAuthorizedConversationMessage(
          transaction,
          record,
          snapshot,
          "No problem—I won’t keep asking. If you change your mind, just say “connect Google.”",
          "direct_response",
          "google_activation_suppressed",
        );
        return "google_activation_suppressed";
      }
    }
    if (
      record.routing.chatKind === "direct" &&
      /^(sign in|settings|open florence|connect google|review|open review|send me (?:a|the) review link)$/u.test(
        normalizedCommand,
      )
    ) {
      const identityId = record.routing.senderIdentityId;
      const personId = record.routing.senderPersonId;
      if (!identityId || !personId) return "ignored";
      if (normalizedCommand === "connect google") {
        await transaction`
          update people set google_activation_suppressed_at = null, updated_at = now()
          where id = ${personId} and status = 'registered'
        `;
      }
      const handoff = await new PostgresWebAuth(
        transaction,
        this.secretBox,
        this.config.security.tokenKey,
      ).createHandoff({
        personId,
        privateIdentityId: identityId,
        privateConversationId: record.routing.conversationId,
        purpose:
          normalizedCommand === "connect google"
            ? "google_connect"
            : /review/u.test(normalizedCommand)
              ? "private_review"
              : "web_sign_in",
        context: {
          ...(normalizedCommand === "connect google"
            ? { activation: "parent_google", profile: "personal_family" }
            : {}),
          returnPath:
            normalizedCommand === "connect google" || /review/u.test(normalizedCommand)
              ? "/sources"
              : "/home",
        },
        expiresInSeconds: 10 * 60,
      });
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        record.routing.conversationId,
      );
      await this.queueAuthorizedConversationMessage(
        transaction,
        record,
        snapshot,
        normalizedCommand === "connect google"
          ? `Here’s a fresh link to connect your personal Gmail and Calendar: ${this.config.publicBaseUrl}/handoff/${handoff.token}\n\nIt expires in 10 minutes.`
          : `Here’s your private, single-use Florence link: ${this.config.publicBaseUrl}/handoff/${handoff.token}`,
        "direct_response",
        "private_handoff",
      );
      return "handoff_created";
    }
    const wantsFamilySetup =
      record.routing.chatKind === "direct" &&
      (/^(set up|setup|create)( my| our)? family$/u.test(normalizedCommand) ||
        (person?.onboarding_step === "family_pending" && isExplicitEnrollmentConsent(text)));
    if (wantsFamilySetup) {
      const personId = record.routing.senderPersonId;
      const identityId = record.routing.senderIdentityId;
      if (!personId || !identityId) return "ignored";
      const existing = await transaction<{ id: string }[]>`
        select household_id as id from household_memberships
        where person_id = ${personId} and status = 'active' limit 1
      `;
      if (!existing[0]) {
        await new PostgresIdentityRelationships(transaction).createHousehold({
          founderPersonId: personId,
          timezone: this.config.defaults.timezone,
          createdAt: new Date().toISOString(),
        });
      }
      await transaction`
        update people set onboarding_step = 'complete', updated_at = now()
        where id = ${personId} and status = 'registered'
      `;
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        record.routing.conversationId,
      );
      const googleOffered = await this.queueParentGoogleActivationOffer(
        transaction,
        personId,
        "household_resolved",
        record,
      );
      if (!googleOffered) {
        const handoff = await new PostgresWebAuth(
          transaction,
          this.secretBox,
          this.config.security.tokenKey,
        ).createHandoff({
          personId,
          privateIdentityId: identityId,
          privateConversationId: record.routing.conversationId,
          purpose: "web_sign_in",
          context: { onboarding: true, returnPath: "/people" },
          expiresInSeconds: 10 * 60,
        });
        await this.queueAuthorizedConversationMessage(
          transaction,
          record,
          snapshot,
          `Done—your private family space is ready. Add me to a group with your co-parent or caregiver; I’ll stay silent there unless every current participant registers and approves writing in that exact group. Add your children and the family context you want me to know here: ${this.config.publicBaseUrl}/handoff/${handoff.token}\n\nIf the link expires, text me “settings” for a fresh one.`,
          "direct_response",
          "family_created",
          null,
          new Date(Date.now() + 24 * 60 * 60_000),
        );
      }
      return "family_created";
    }
    if (
      record.routing.chatKind === "direct" &&
      person?.onboarding_step === "family_pending" &&
      /^(?:no(?: thanks| thank you)?|not now|maybe later|later|skip)$/u.test(normalizedCommand)
    ) {
      const personId = record.routing.senderPersonId;
      if (!personId) return "ignored";
      await transaction`
        update people set onboarding_step = 'complete', updated_at = now()
        where id = ${personId} and status = 'registered'
      `;
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        record.routing.conversationId,
      );
      await this.queueAuthorizedConversationMessage(
        transaction,
        record,
        snapshot,
        "No problem. You can keep talking with me normally, and say ‘set up our family’ whenever you’re ready.",
        "direct_response",
        "family_setup_deferred",
      );
      return "family_setup_deferred";
    }
    await new DurableWork(transaction, this.secretBox).enqueue({
      kind: "orchestrate.linq_message",
      idempotencyKey: `orchestrate:linq:${internalProviderEventId}`,
      payload: { internalProviderEventId },
      conversation: {
        id: record.routing.conversationId,
        authorityVersion: (
          await new PostgresConversationAuthority(transaction).snapshot(record.routing.conversationId)
        ).authorityVersion,
      },
      ...(registeredSender && senderPersonId
        ? { person: { id: senderPersonId, controlEpoch: Number(registeredSender.control_epoch) } }
        : {}),
      priority: 20,
      maxAttempts: 5,
      deadlineAt: new Date(Date.now() + 5 * 60_000),
    });
    return "orchestration_queued";
  }

  private async invalidateEditedLinqMessageSource(
    transaction: Transaction,
    event: Extract<LinqWebhookEnvelope, { eventType: "linq.message.edited" }>,
    routing: StoredLinqEvent["routing"],
  ): Promise<string> {
    const scope =
      routing.chatKind === "group"
        ? {
            kind: "conversation_epoch" as const,
            participantEpochId: routing.participantEpochId,
          }
        : routing.senderPersonId
          ? ({ kind: "person" as const, personId: routing.senderPersonId } as const)
          : null;
    if (!scope) return "observed_silently";
    await new PostgresSourceIntelligence(transaction, this.secretBox, {
      rawRetentionDays: this.config.defaults.rawSourceRetentionDays,
      privateCandidateRetentionDays: 7,
    }).apply({
      kind: "mark_source_deleted",
      integrationId: null,
      expectedIntegrationControlEpoch: null,
      artifactKind: "conversation_message",
      origin: { system: "linq", remoteObjectId: event.edit.providerMessageId },
      scope,
      deletedAt: event.edit.editedAt,
    });
    return "message_edit_source_invalidated";
  }

  private async handleProviderReceipt(transaction: Transaction, record: StoredLinqEvent): Promise<string> {
    const event = record.event;
    if (
      !event ||
      ![
        "linq.outbound.sent",
        "linq.outbound.delivered",
        "linq.outbound.read",
        "linq.outbound.failed",
      ].includes(event.eventType)
    ) {
      return "observed";
    }
    if (
      event.eventType !== "linq.outbound.sent" &&
      event.eventType !== "linq.outbound.delivered" &&
      event.eventType !== "linq.outbound.read" &&
      event.eventType !== "linq.outbound.failed"
    ) {
      return "observed";
    }
    const providerMessageId = event.receipt.providerMessageId;
    const providerIdempotencyKey =
      event.eventType === "linq.outbound.failed" ? null : (event.receipt.idempotencyKey ?? null);
    const rows = await transaction<
      { outbox_id: string; idempotency_key: string; latest_confirmed_at: Date | null }[]
    >`
      select effect.id as outbox_id, effect.idempotency_key,
        max(receipt.occurred_at) filter (where receipt.status = 'confirmed') as latest_confirmed_at
      from outbox effect
      left join effect_receipts receipt on receipt.outbox_id = effect.id
      where receipt.provider_receipt_id = ${providerMessageId}
        or (${providerIdempotencyKey}::text is not null and effect.idempotency_key = ${providerIdempotencyKey})
      group by effect.id, effect.idempotency_key
      order by bool_or(receipt.provider_receipt_id = ${providerMessageId}) desc nulls last
      limit 1
    `;
    if (!rows[0]) return "unmatched_receipt";
    const failed = event.eventType === "linq.outbound.failed";
    const submitted = event.eventType === "linq.outbound.sent" && event.receipt.sender.service !== "sms";
    const mayArriveLate = failed && event.receipt.errorCode === "4001";
    const occurredAt = new Date(event.occurredAt);
    const failureIsCurrent = rows[0].latest_confirmed_at === null || occurredAt > rows[0].latest_confirmed_at;
    const receiptJson = canonicalJson({
      kind: "linq_delivery_webhook",
      providerEventId: event.providerEventId,
      providerMessageId,
      eventType: event.eventType,
      occurredAt: event.occurredAt,
      ...(failed ? { errorCode: event.receipt.errorCode } : {}),
    });
    const encrypted = this.secretBox.encrypt(receiptJson, "effect-receipt");
    await transaction`
      insert into effect_receipts (
        id, outbox_id, idempotency_key, provider_receipt_id, status,
        receipt_digest, receipt_ciphertext, receipt_key_version, occurred_at,
        reconciled_at, error_code
      ) values (
        ${randomUUID()}, ${rows[0].outbox_id}, ${rows[0].idempotency_key}, ${providerMessageId},
        ${failed ? "failed" : submitted ? "submitted" : "confirmed"}, ${sha256Hex(receiptJson)},
        ${Buffer.from(JSON.stringify(encrypted), "utf8")}, ${encrypted.kid},
        ${occurredAt}, ${failed || submitted ? null : occurredAt},
        ${failed ? event.receipt.errorCode : null}
      ) on conflict do nothing
    `;
    if (mayArriveLate && failureIsCurrent) {
      // Linq explicitly documents that a 4001 failure can be followed by a
      // delivered event for the same provider message. Reconcile the original
      // provider ID; never create a second family message from this uncertainty.
      await transaction`
        update outbox set status = 'submitted',
          available_at = ${new Date(Date.now() + LINQ_FAILURE_RECONCILIATION_DELAY_MS)},
          lease_owner = null, lease_token = null, lease_expires_at = null,
          last_error_code = 'linq_delivery_may_arrive_late', updated_at = now()
        where id = ${rows[0].outbox_id} and status <> 'cancelled'
      `;
    } else if (failed && failureIsCurrent) {
      await transaction`
        update outbox set status = 'dead', available_at = now(),
          lease_owner = null, lease_token = null, lease_expires_at = null,
          last_error_code = 'linq_delivery_failed', updated_at = now()
        where id = ${rows[0].outbox_id}
      `;
    } else if (failed) {
      // A stronger confirmation with a later provider occurrence time already
      // won. Persist this receipt for audit without regressing delivery state.
    } else if (submitted) {
      // iMessage/RCS `sent` is provider acceptance, not proof of delivery.
      // Keep reconciling the same provider message; a reply can itself prove
      // receipt without ever authorizing a duplicate send.
      await transaction`
        update outbox set status = 'submitted',
          available_at = ${new Date(Date.now() + LINQ_FAILURE_RECONCILIATION_DELAY_MS)},
          lease_owner = null, lease_token = null, lease_expires_at = null,
          updated_at = now()
        where id = ${rows[0].outbox_id}
          and status in ('pending', 'retry', 'leased', 'submitted')
      `;
    } else if (event.eventType === "linq.outbound.sent") {
      // SMS has no delivered/read lifecycle, so `sent` is its strongest
      // transport receipt. It must not resurrect an explicitly failed send.
      await transaction`
        update outbox set status = 'confirmed', available_at = now(),
          lease_owner = null, lease_token = null, lease_expires_at = null,
          last_error_code = null, updated_at = now()
        where id = ${rows[0].outbox_id}
          and status in ('pending', 'retry', 'leased', 'submitted', 'confirmed')
      `;
    } else {
      // Delivered/read may legitimately follow a provisional 4001 failure, so
      // any verified positive lifecycle event confirms the original attempt.
      await transaction`
        update outbox set status = 'confirmed', available_at = now(),
          lease_owner = null, lease_token = null, lease_expires_at = null,
          last_error_code = null, updated_at = now()
        where id = ${rows[0].outbox_id} and status <> 'cancelled'
      `;
    }
    return failed && !failureIsCurrent
      ? "delivery_failure_superseded"
      : mayArriveLate
        ? "delivery_failure_reconciling"
        : failed
          ? "delivery_failure_observed"
          : submitted
            ? "delivery_submitted"
            : "delivery_confirmed";
  }

  private async queueSystemEnrollmentMessage(
    transaction: Transaction,
    record: StoredLinqEvent,
    text: string,
  ): Promise<void> {
    await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
      effectKind: "linq.message",
      idempotencyKey: `enrollment:${record.routing.providerChatId}:${sha256Hex(text)}`,
      data: { classification: "registration_prompt" },
      policy: { systemEnrollment: true, exactPrivateDm: record.routing.chatKind === "direct" },
      target: {
        providerChatId: record.routing.providerChatId,
        participantDigest: record.routing.providerParticipantDigest,
      },
      payload: {
        providerChatId: record.routing.providerChatId,
        expectedProviderParticipantDigest: record.routing.providerParticipantDigest,
        text,
      },
      reasonCodes: ["exact_private_dm_enrollment"],
      authorizationExpiresAt: new Date(Date.now() + 24 * 60 * 60_000),
    });
  }

  private async queueAuthorizedConversationMessage(
    transaction: Transaction,
    record: StoredLinqEvent,
    snapshot: ConversationAuthoritySnapshot,
    text: string,
    sendKind: "direct_response" | "proactive",
    operation: string,
    ruleId: string | null = null,
    authorizationExpiresAt = new Date(Date.now() + 5 * 60_000),
    idempotencyKey = `linq:${operation}:${record.routing.conversationId}:${sha256Hex(text)}`,
  ): Promise<{ readonly outboxId: string; readonly created: boolean } | null> {
    if (!snapshot.participantEpochId || !snapshot.participantSetDigest)
      throw new Error("Conversation has no live epoch");
    const authority = await new PostgresConversationAuthority(transaction).authorizeSend({
      conversationId: record.routing.conversationId,
      expectedParticipantEpochId: snapshot.participantEpochId,
      expectedParticipantSetDigest: snapshot.participantSetDigest,
      liveParticipantIdentityIds: [...record.routing.liveIdentityIds],
      sendKind,
      operation,
      ruleId,
    });
    if (!authority.allowed) return null;
    const household = await transaction<{ id: string; control_epoch: number | string }[]>`
      select household.id, household.control_epoch
      from conversations conversation join households household on household.id = conversation.household_id
      where conversation.id = ${record.routing.conversationId}
    `;
    return new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
      ...(record.routing.senderPersonId ? { actorPersonId: record.routing.senderPersonId } : {}),
      participantEpochId: snapshot.participantEpochId,
      expectedParticipantDigest: snapshot.participantSetDigest,
      effectKind: "linq.message",
      idempotencyKey,
      data: { textDigest: sha256Hex(text) },
      policy: { authorityVersion: snapshot.authorityVersion, operation, sendKind },
      target: {
        providerChatId: record.routing.providerChatId,
        participantEpochId: snapshot.participantEpochId,
      },
      payload: {
        providerChatId: record.routing.providerChatId,
        expectedProviderParticipantDigest: record.routing.providerParticipantDigest,
        text,
      },
      reasonCodes: ["conversation_authority_allowed", operation],
      authorizationExpiresAt,
      conversation: { id: record.routing.conversationId, authorityVersion: snapshot.authorityVersion },
      ...(record.routing.senderPersonId ? await personFence(transaction, record.routing.senderPersonId) : {}),
      ...(household[0]
        ? { household: { id: household[0].id, controlEpoch: Number(household[0].control_epoch) } }
        : {}),
    });
  }

  /**
   * Offers the first parent-owned source exactly once. The outbox key is also
   * the durable "do not nag" receipt: ignoring or declining this generic offer
   * never creates a later generic resend. Work and additional Google accounts
   * remain independent connections; only an existing personal connection
   * suppresses this activation step.
   */
  private async queueParentGoogleActivationOffer(
    transaction: Transaction,
    personId: string,
    reason: ParentGoogleActivationReason,
    currentRecord?: StoredLinqEvent,
    reengagementEventId?: string,
  ): Promise<{ readonly outboxId: string; readonly created: boolean } | null> {
    const scopes = await transaction<
      {
        household_id: string;
        household_control_epoch: number | string;
        person_control_epoch: number | string;
      }[]
    >`
      select membership.household_id, household.control_epoch as household_control_epoch,
        person.control_epoch as person_control_epoch
      from household_memberships membership
      join households household on household.id = membership.household_id
        and household.status in ('onboarding', 'active')
      join people person on person.id = membership.person_id and person.status = 'registered'
      where membership.person_id = ${personId} and membership.role = 'steward'
        and membership.status = 'active'
        and person.google_activation_suppressed_at is null
        and not exists(
          select 1 from integrations integration
          where integration.person_id = membership.person_id
            and integration.provider = 'google'
            and integration.account_kind = 'personal_family'
            and integration.status <> 'revoked'
        )
      order by membership.joined_at, membership.id
      limit 1
      for update of person
    `;
    const scope = scopes[0];
    if (!scope) return null;

    const initialIdempotencyKey = `google-parent-activation:${personId}`;
    const existing = await transaction<{ present: boolean }[]>`
      select exists(
        select 1 from outbox where idempotency_key = ${initialIdempotencyKey}
      ) as present
    `;
    const initialOfferExists = existing[0]?.present === true;
    if (initialOfferExists) {
      if (!reengagementEventId || reason !== "reengagement_after_expiry") return null;
      const latestHandoffs = await transaction<
        { readonly created_at: Date; readonly expires_at: Date; readonly consumed_at: Date | null }[]
      >`
        select created_at, expires_at, consumed_at
        from auth_handoffs
        where person_id = ${personId} and purpose = 'google_connect'
        order by created_at desc limit 1
      `;
      const latest = latestHandoffs[0];
      const now = new Date();
      if (
        latest &&
        ((latest.consumed_at === null && latest.expires_at > now) ||
          (latest.consumed_at !== null && latest.created_at > new Date(now.getTime() - 15 * 60_000)))
      ) {
        return null;
      }
    }
    const idempotencyKey = initialOfferExists
      ? `google-parent-activation:${personId}:reengagement:${reengagementEventId}`
      : initialIdempotencyKey;

    let route: ExactPrivateRoute | null = null;
    if (
      currentRecord?.routing.chatKind === "direct" &&
      currentRecord.routing.senderPersonId === personId &&
      currentRecord.routing.senderIdentityId !== null &&
      currentRecord.routing.liveIdentityIds.length === 1 &&
      currentRecord.routing.liveIdentityIds[0] === currentRecord.routing.senderIdentityId
    ) {
      route = {
        conversationId: currentRecord.routing.conversationId,
        participantEpochId: currentRecord.routing.participantEpochId,
        participantSetDigest: currentRecord.routing.appParticipantDigest,
        liveIdentityIds: [...currentRecord.routing.liveIdentityIds],
        privateIdentityId: currentRecord.routing.senderIdentityId,
        providerChatId: currentRecord.routing.providerChatId,
        providerParticipantDigest: currentRecord.routing.providerParticipantDigest,
      };
    } else if (!currentRecord) {
      const routes = await transaction<
        {
          conversation_id: string;
          participant_epoch_id: string;
          participant_set_digest: string;
          identity_id: string;
          external_channel_id: string;
          latest_participant_digest: string | null;
        }[]
      >`
        select conversation.id as conversation_id, epoch.id as participant_epoch_id,
          epoch.participant_set_digest, participant.person_identity_id as identity_id,
          channel.external_channel_id, channel.latest_participant_digest
        from conversations conversation
        join conversation_channels channel on channel.conversation_id = conversation.id
          and channel.provider = 'linq' and channel.status = 'active'
        join participant_epochs epoch on epoch.id = conversation.current_epoch_id
          and epoch.ended_at is null
        join epoch_participants participant on participant.participant_epoch_id = epoch.id
          and participant.person_id = ${personId}
          and participant.registration_status = 'registered' and participant.consented_at is not null
        join person_identities identity on identity.id = participant.person_identity_id
          and identity.status = 'verified'
        where conversation.kind = 'direct' and conversation.status = 'active'
          and (select count(*) from epoch_participants exact_participant
            where exact_participant.participant_epoch_id = epoch.id) = 1
        order by conversation.updated_at desc, conversation.id
        limit 1
      `;
      const saved = routes[0];
      if (saved?.latest_participant_digest) {
        route = {
          conversationId: saved.conversation_id,
          participantEpochId: saved.participant_epoch_id,
          participantSetDigest: saved.participant_set_digest,
          liveIdentityIds: [saved.identity_id],
          privateIdentityId: saved.identity_id,
          providerChatId: saved.external_channel_id,
          providerParticipantDigest: saved.latest_participant_digest,
        };
      }
    }
    if (!route) return null;

    const sendKind = currentRecord ? "direct_response" : "transactional";
    const authority = await new PostgresConversationAuthority(transaction).authorizeSend({
      conversationId: route.conversationId,
      expectedParticipantEpochId: route.participantEpochId,
      expectedParticipantSetDigest: route.participantSetDigest,
      liveParticipantIdentityIds: [...route.liveIdentityIds],
      sendKind,
      operation: "parent_google_activation",
      ruleId: null,
    });
    if (!authority.allowed || !authority.participantEpochId || !authority.participantSetDigest) {
      return null;
    }

    const handoff = await new PostgresWebAuth(
      transaction,
      this.secretBox,
      this.config.security.tokenKey,
    ).createHandoff({
      personId,
      privateIdentityId: route.privateIdentityId,
      privateConversationId: route.conversationId,
      purpose: "google_connect",
      context: {
        activation: "parent_google",
        profile: "personal_family",
        returnPath: "/sources",
      },
      expiresInSeconds: 10 * 60,
    });
    const link = `${this.config.publicBaseUrl}/handoff/${handoff.token}`;
    const text =
      reason === "reengagement_after_expiry"
        ? `Hi! Your family space is ready, but Google still isn’t connected. Here’s a fresh link to connect your personal Gmail and Calendar: ${link}\n\nIt expires in 10 minutes. If you’d rather skip this, just say “not now” and I won’t keep asking.`
        : reason === "household_resolved"
          ? `Your private family space is ready. The best next step is to connect your primary personal Google account so I can privately start reviewing recent Gmail and Calendar: ${link}\n\nWhile that sync runs, you can keep talking with me and add your co-parent, children, and family details. Connecting Google is optional; if you skip it, I won’t keep asking. If the link expires, text me “connect Google” for a fresh one.`
          : `I’ll keep working on what you just sent. One private setup step can make future family help more useful: connect your primary personal Google account so I can start reviewing recent Gmail and Calendar: ${link}\n\nIt’s optional; if you skip it, I won’t keep asking. If the link expires, text me “connect Google” for a fresh one.`;
    const queued = await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
      actorPersonId: personId,
      person: { id: personId, controlEpoch: Number(scope.person_control_epoch) },
      household: {
        id: scope.household_id,
        controlEpoch: Number(scope.household_control_epoch),
      },
      conversation: { id: route.conversationId, authorityVersion: authority.authorityVersion },
      participantEpochId: authority.participantEpochId,
      expectedParticipantDigest: authority.participantSetDigest,
      effectKind: "linq.message",
      idempotencyKey,
      data: { accountKind: "personal_family", reason, textDigest: sha256Hex(text) },
      policy: {
        exactPrivateDm: true,
        operation: "parent_google_activation",
        optional: true,
        sendKind,
      },
      target: {
        providerChatId: route.providerChatId,
        personId,
        participantEpochId: authority.participantEpochId,
      },
      payload: {
        providerChatId: route.providerChatId,
        expectedProviderParticipantDigest: route.providerParticipantDigest,
        text,
      },
      reasonCodes: ["active_parent_steward", "no_personal_google_connection", "exact_private_dm", reason],
      authorizationExpiresAt: handoff.expiresAt,
    });
    return queued;
  }

  private async queueHouseholdInvitationMessage(
    transaction: Transaction,
    invitationId: string,
  ): Promise<void> {
    const invitations = await transaction<
      {
        household_id: string;
        household_control_epoch: number | string;
        inviter_person_id: string;
        inviter_person_control_epoch: number | string;
        inviter_name_ciphertext: Buffer | null;
        invitee_person_id: string;
        invitee_person_status: string;
        invitee_identity_id: string;
        invitee_identity_status: string;
        invitee_identity_authority_version: number | string;
        invitee_subject_digest: string;
        invitee_subject_ciphertext: Buffer | null;
        requested_role: "steward" | "caregiver" | "participant";
        expires_at: Date;
        ready: boolean;
      }[]
    >`
      select invitation.household_id, household.control_epoch as household_control_epoch,
        inviter_membership.person_id as inviter_person_id,
        inviter.control_epoch as inviter_person_control_epoch,
        inviter.display_name_ciphertext as inviter_name_ciphertext,
        invitee_identity.person_id as invitee_person_id,
        invitee.status as invitee_person_status,
        invitee_identity.id as invitee_identity_id,
        invitee_identity.status as invitee_identity_status,
        invitee_identity.authority_version as invitee_identity_authority_version,
        invitee_identity.subject_digest as invitee_subject_digest,
        invitee_identity.subject_ciphertext as invitee_subject_ciphertext,
        invitation.requested_role, invitation.expires_at,
        not exists(
          select 1 from invitation_approvals approval
          where approval.invitation_id = invitation.id and approval.approved_at is null
        ) as ready
      from invitations invitation
      join households household on household.id = invitation.household_id
        and household.membership_version = invitation.household_membership_version
      join household_memberships inviter_membership on inviter_membership.id = invitation.invited_by_membership_id
        and inviter_membership.status = 'active'
      join people inviter on inviter.id = inviter_membership.person_id and inviter.status = 'registered'
      join person_identities invitee_identity on invitee_identity.id = invitation.invitee_identity_id
        and invitee_identity.status in ('observed', 'verified')
      join people invitee on invitee.id = invitee_identity.person_id
        and invitee.status in ('provisional', 'registered')
      where invitation.id = ${invitationId} and invitation.status = 'pending'
        and invitation.expires_at > now()
    `;
    const invitation = invitations[0];
    if (!invitation?.ready) return;
    const inviterName =
      decryptPersonName(this.secretBox, invitation.inviter_person_id, invitation.inviter_name_ciphertext) ??
      "Someone in your shared Florence group";
    const role =
      invitation.requested_role === "steward"
        ? "a parent / steward"
        : invitation.requested_role === "caregiver"
          ? "a caregiver"
          : "a family participant";

    if (
      invitation.invitee_person_status !== "registered" ||
      invitation.invitee_identity_status !== "verified"
    ) {
      const recipient = decryptIdentitySubject(
        this.secretBox,
        invitation.invitee_identity_id,
        invitation.invitee_subject_ciphertext,
      );
      if (!recipient || !isOutboundIdentitySubject(recipient)) {
        throw new NotFoundError("Florence cannot safely open a private chat with that group participant");
      }
      const text = `${inviterName} invited you to Florence as ${role}. Florence is a private family Chief of Staff. I won’t use or share your private messages unless you opt in, and groups stay observe-only unless every current participant registers and approves writing in that exact group. Would you like to create your private Florence account and review the invitation? Reply yes to agree, or STOP to decline.`;
      await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
        actorPersonId: invitation.inviter_person_id,
        person: {
          id: invitation.inviter_person_id,
          controlEpoch: Number(invitation.inviter_person_control_epoch),
        },
        household: {
          id: invitation.household_id,
          controlEpoch: Number(invitation.household_control_epoch),
        },
        invitation: {
          id: invitationId,
          inviteeIdentityAuthorityVersion: Number(invitation.invitee_identity_authority_version),
        },
        effectKind: "linq.message",
        idempotencyKey: `household-enrollment-invitation:${invitationId}`,
        data: {
          invitationId,
          inviteeIdentityId: invitation.invitee_identity_id,
          textDigest: sha256Hex(text),
        },
        policy: { explicitInviterApproval: true, enrollmentOnly: true },
        target: {
          inviteeIdentityId: invitation.invitee_identity_id,
          inviteeSubjectDigest: invitation.invitee_subject_digest,
        },
        payload: { recipient, text },
        reasonCodes: ["explicit_household_inviter", "exact_current_group_participant", "enrollment_only"],
        authorizationExpiresAt: invitation.expires_at,
      });
      return;
    }

    const routes = await transaction<
      {
        conversation_id: string;
        authority_version: number | string;
        participant_epoch_id: string;
        participant_set_digest: string;
        identity_id: string;
        external_channel_id: string;
        latest_participant_digest: string | null;
      }[]
    >`
      select conversation.id as conversation_id, conversation.authority_version,
        epoch.id as participant_epoch_id, epoch.participant_set_digest,
        participant.person_identity_id as identity_id, channel.external_channel_id,
        channel.latest_participant_digest
      from conversations conversation
      join conversation_channels channel on channel.conversation_id = conversation.id
        and channel.provider = 'linq' and channel.status = 'active'
      join participant_epochs epoch on epoch.id = conversation.current_epoch_id
        and epoch.ended_at is null
      join epoch_participants participant on participant.participant_epoch_id = epoch.id
        and participant.person_id = ${invitation.invitee_person_id}
        and participant.person_identity_id = ${invitation.invitee_identity_id}
        and participant.registration_status = 'registered' and participant.consented_at is not null
      join person_identities identity on identity.id = participant.person_identity_id
        and identity.status = 'verified'
      join people invitee on invitee.id = participant.person_id and invitee.status = 'registered'
      where conversation.kind = 'direct' and conversation.status = 'active'
        and (select count(*) from epoch_participants exact_participant
          where exact_participant.participant_epoch_id = epoch.id) = 1
      order by conversation.updated_at desc limit 1
    `;
    const route = routes[0];
    if (!route?.latest_participant_digest) {
      throw new NotFoundError("That person needs a fresh private Florence conversation before being invited");
    }
    const authority = await new PostgresConversationAuthority(transaction).authorizeSend({
      conversationId: route.conversation_id,
      expectedParticipantEpochId: route.participant_epoch_id,
      expectedParticipantSetDigest: route.participant_set_digest,
      liveParticipantIdentityIds: [route.identity_id],
      sendKind: "direct_response",
      operation: "household_invitation",
      ruleId: null,
    });
    if (!authority.allowed) {
      throw new UnauthorizedError("That person’s private Florence settings do not allow an invitation");
    }
    const handoff = await new PostgresWebAuth(
      transaction,
      this.secretBox,
      this.config.security.tokenKey,
    ).createHandoff({
      personId: invitation.invitee_person_id,
      privateIdentityId: route.identity_id,
      privateConversationId: route.conversation_id,
      purpose: "invitation",
      context: { invitationId, returnPath: "/people" },
      expiresInSeconds: 10 * 60,
    });
    const text = `${inviterName} invited you to join their Florence family as ${role}. I won’t ask you to repeat family details they already shared. Review and accept privately: ${this.config.publicBaseUrl}/handoff/${handoff.token}\n\nIf the link expires, text me “settings” for a fresh one.`;
    await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
      actorPersonId: invitation.inviter_person_id,
      ...(await personFence(transaction, invitation.invitee_person_id)),
      household: {
        id: invitation.household_id,
        controlEpoch: Number(invitation.household_control_epoch),
      },
      invitation: {
        id: invitationId,
        inviteeIdentityAuthorityVersion: Number(invitation.invitee_identity_authority_version),
      },
      conversation: { id: route.conversation_id, authorityVersion: Number(route.authority_version) },
      participantEpochId: route.participant_epoch_id,
      expectedParticipantDigest: route.participant_set_digest,
      effectKind: "linq.message",
      idempotencyKey: `household-invitation:${invitationId}`,
      data: { invitationId, textDigest: sha256Hex(text) },
      policy: { exactPrivateDm: true, operation: "household_invitation" },
      target: {
        providerChatId: route.external_channel_id,
        participantEpochId: route.participant_epoch_id,
      },
      payload: {
        providerChatId: route.external_channel_id,
        expectedProviderParticipantDigest: route.latest_participant_digest,
        text,
      },
      reasonCodes: ["registered_exact_private_invitee", "household_invitation"],
      authorizationExpiresAt: invitation.expires_at,
    });
  }

  private async queuePrivateGroupEnrollmentOffers(
    transaction: Transaction,
    conversationId: string,
  ): Promise<void> {
    const candidates = await transaction<
      { person_id: string; household_id: string; unregistered_count: number | string }[]
    >`
      select distinct participant.person_id, membership.household_id,
        (
          select count(*)
          from epoch_participants unregistered
          where unregistered.participant_epoch_id = epoch.id
            and (unregistered.registration_status <> 'registered' or unregistered.consented_at is null)
        ) as unregistered_count
      from conversations conversation
      join participant_epochs epoch on epoch.id = conversation.current_epoch_id and epoch.ended_at is null
      join epoch_participants participant on participant.participant_epoch_id = epoch.id
        and participant.registration_status = 'registered' and participant.consented_at is not null
      join people person on person.id = participant.person_id and person.status = 'registered'
      join household_memberships membership on membership.person_id = participant.person_id
        and membership.status = 'active'
      join membership_capabilities capability on capability.membership_id = membership.id
        and capability.capability = 'membership.invite' and capability.status = 'active'
      where conversation.id = ${conversationId} and conversation.kind = 'group'
        and exists(
          select 1 from epoch_participants unregistered
          where unregistered.participant_epoch_id = epoch.id
            and (unregistered.registration_status <> 'registered' or unregistered.consented_at is null)
        )
    `;
    for (const candidate of candidates) {
      const count = Number(candidate.unregistered_count);
      await this.queuePrivateGroupNotice(transaction, {
        conversationId,
        personId: candidate.person_id,
        householdId: candidate.household_id,
        noticeKind: "enrollment-offer",
        message: (link) =>
          `I’m now in one of your observe-only groups with ${count === 1 ? "one person who hasn’t" : `${count} people who haven’t`} registered. I’ll stay silent there, and any permitted new context stays scoped to that exact participant lineup. If you want me to send ${count === 1 ? "them" : "each of them"} one private enrollment invitation, review the exact group here: ${link}`,
      });
    }
  }

  private async queuePrivateGroupNotice(
    transaction: Transaction,
    input: {
      readonly conversationId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly noticeKind: "coverage-active" | "enrollment-offer" | "group-ready";
      readonly message: (link: string) => string;
    },
  ): Promise<void> {
    const routes = await transaction<
      {
        group_authority_version: number | string;
        group_epoch_id: string;
        group_participant_digest: string;
        household_control_epoch: number | string;
        person_control_epoch: number | string;
        direct_conversation_id: string;
        direct_authority_version: number | string;
        direct_epoch_id: string;
        direct_participant_digest: string;
        direct_identity_id: string;
        external_channel_id: string;
        latest_participant_digest: string;
      }[]
    >`
      select group_conversation.authority_version as group_authority_version,
        group_epoch.id as group_epoch_id,
        group_epoch.participant_set_digest as group_participant_digest,
        household.control_epoch as household_control_epoch,
        person.control_epoch as person_control_epoch,
        direct.id as direct_conversation_id,
        direct.authority_version as direct_authority_version,
        direct_epoch.id as direct_epoch_id,
        direct_epoch.participant_set_digest as direct_participant_digest,
        direct_participant.person_identity_id as direct_identity_id,
        channel.external_channel_id, channel.latest_participant_digest
      from conversations group_conversation
      join participant_epochs group_epoch on group_epoch.id = group_conversation.current_epoch_id
        and group_epoch.ended_at is null
      join epoch_participants group_participant on group_participant.participant_epoch_id = group_epoch.id
        and group_participant.person_id = ${input.personId}
        and group_participant.registration_status = 'registered'
        and group_participant.consented_at is not null
      join people person on person.id = group_participant.person_id and person.status = 'registered'
      join household_memberships membership on membership.household_id = ${input.householdId}
        and membership.person_id = person.id and membership.status = 'active'
      join households household on household.id = membership.household_id
        and household.status in ('onboarding', 'active', 'paused')
      join conversations direct on direct.kind = 'direct' and direct.status = 'active'
      join conversation_channels channel on channel.conversation_id = direct.id
        and channel.provider = 'linq' and channel.status = 'active'
        and channel.latest_participant_digest is not null
      join participant_epochs direct_epoch on direct_epoch.id = direct.current_epoch_id
        and direct_epoch.ended_at is null
      join epoch_participants direct_participant on direct_participant.participant_epoch_id = direct_epoch.id
        and direct_participant.person_id = person.id
        and direct_participant.registration_status = 'registered'
        and direct_participant.consented_at is not null
      where group_conversation.id = ${input.conversationId}
        and group_conversation.kind = 'group' and group_conversation.status = 'active'
        and (select count(*) from epoch_participants exact
          where exact.participant_epoch_id = direct_epoch.id) = 1
      order by direct.updated_at desc limit 1
    `;
    const route = routes[0];
    if (!route) return;
    const idempotencyKey = `group-${input.noticeKind}:${input.conversationId}:${route.group_epoch_id}:${input.personId}`;
    const existing = await transaction<{ present: boolean }[]>`
      select exists(
        select 1 from outbox
        where idempotency_key = ${idempotencyKey}
          or redrive_root_id = (
            select id from outbox where idempotency_key = ${idempotencyKey}
          )
      ) as present
    `;
    if (existing[0]?.present) return;
    const directAuthority = await new PostgresConversationAuthority(transaction).authorizeSend({
      conversationId: route.direct_conversation_id,
      expectedParticipantEpochId: route.direct_epoch_id,
      expectedParticipantSetDigest: route.direct_participant_digest,
      liveParticipantIdentityIds: [route.direct_identity_id],
      sendKind: "transactional",
      operation: "private_group_notice",
      ruleId: null,
    });
    if (!directAuthority.allowed) return;
    const isGroupCoverageApproval = input.noticeKind === "group-ready";
    const handoff = await new PostgresWebAuth(
      transaction,
      this.secretBox,
      this.config.security.tokenKey,
    ).createHandoff({
      personId: input.personId,
      privateIdentityId: route.direct_identity_id,
      privateConversationId: route.direct_conversation_id,
      purpose: isGroupCoverageApproval ? "group_coverage" : "web_sign_in",
      context: isGroupCoverageApproval
        ? {
            action: "approve",
            conversationId: input.conversationId,
            expectedParticipantEpochId: route.group_epoch_id,
            expectedParticipantSetDigest: route.group_participant_digest,
            expectedConversationAuthorityVersion: String(route.group_authority_version),
            expectedHouseholdControlEpoch: String(route.household_control_epoch),
            returnPath: "/people",
          }
        : { returnPath: "/people" },
      expiresInSeconds: 10 * 60,
    });
    const text = `${input.message(`${this.config.publicBaseUrl}/handoff/${handoff.token}`)}\n\nIf the link expires, text me “settings” for a fresh one.`;
    await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
      actorPersonId: input.personId,
      person: { id: input.personId, controlEpoch: Number(route.person_control_epoch) },
      household: { id: input.householdId, controlEpoch: Number(route.household_control_epoch) },
      conversation: {
        id: route.direct_conversation_id,
        authorityVersion: Number(route.direct_authority_version),
      },
      participantEpochId: route.direct_epoch_id,
      expectedParticipantDigest: route.direct_participant_digest,
      sourceConversation: {
        id: input.conversationId,
        authorityVersion: Number(route.group_authority_version),
        participantEpochId: route.group_epoch_id,
        participantSetDigest: route.group_participant_digest,
      },
      effectKind: "linq.message",
      idempotencyKey,
      data: { noticeKind: input.noticeKind, textDigest: sha256Hex(text) },
      policy: { exactPrivateDm: true, transactionalGroupNotice: true },
      target: {
        providerChatId: route.external_channel_id,
        personId: input.personId,
      },
      payload: {
        providerChatId: route.external_channel_id,
        expectedProviderParticipantDigest: route.latest_participant_digest,
        text,
      },
      reasonCodes: ["exact_current_group_epoch", "exact_private_recipient", input.noticeKind],
      authorizationExpiresAt: handoff.expiresAt,
    });
  }

  private async queuePrivateGroupReadyNotices(
    transaction: Transaction,
    conversationId: string,
    noticeKind: "coverage-active" | "group-ready",
  ): Promise<void> {
    const groups = await transaction<{ household_id: string }[]>`
      select household_id from conversations
      where id = ${conversationId} and kind = 'group' and status = 'active'
        and household_id is not null
    `;
    const group = groups[0];
    if (!group) return;
    const snapshot = await new PostgresConversationAuthority(transaction).snapshot(conversationId);
    if (!snapshot.participantEpochId || !snapshot.participantSetDigest) return;
    if (
      snapshot.participants.length === 0 ||
      snapshot.participants.some(
        (participant) =>
          participant.registrationStatus !== "registered" ||
          participant.consentedAt === null ||
          participant.policy === null,
      )
    )
      return;
    const participantIds = [
      ...new Set(snapshot.participants.map((participant) => participant.personId)),
    ].sort();
    const memberships = await transaction<{ person_id: string }[]>`
      select person_id from household_memberships
      where household_id = ${group.household_id} and status = 'active'
        and person_id = any(${transaction.array(participantIds)}::uuid[])
      order by person_id
    `;
    if (memberships.length !== participantIds.length) return;
    const coverageActive = snapshot.rules.some(
      (rule) =>
        rule.active &&
        rule.participantSetDigest === snapshot.participantSetDigest &&
        rule.allowedOperations.includes("proactive_coverage"),
    );
    if (
      (noticeKind === "group-ready" && coverageActive) ||
      (noticeKind === "coverage-active" && !coverageActive)
    )
      return;
    for (const personId of participantIds) {
      await this.queuePrivateGroupNotice(transaction, {
        conversationId,
        personId,
        householdId: group.household_id,
        noticeKind,
        message:
          noticeKind === "coverage-active"
            ? (link) =>
                `Coverage help is on for this exact family group. I can now open, follow, and close coverage loops there without assigning blame. If anyone joins or leaves, I’ll turn it off until the new group approves again. Your private controls: ${link}`
            : (link) =>
                `Everyone in your family group has registered and the family invitation is complete. I’m still read-only for proactive coverage until each current person approves it privately. Review this exact group: ${link}`,
      });
    }
  }

  private async processWebCommand(
    actorPersonId: string,
    command: Extract<AppEnvelope, { kind: "web.command" }>["command"],
  ): Promise<ProcessReceipt> {
    return this.database.begin(async (transaction) => {
      const people = await transaction<{ status: string; control_epoch: number | string }[]>`
        select status, control_epoch from people where id = ${actorPersonId} for update
      `;
      if (people[0]?.status !== "registered") throw new Error("Web command actor is not registered");
      switch (command.kind) {
        case "create_household": {
          const existing = await transaction<{ id: string }[]>`
            select household_id as id from household_memberships
            where person_id = ${actorPersonId} and status = 'active' limit 1
          `;
          if (existing[0]) {
            await this.queueParentGoogleActivationOffer(transaction, actorPersonId, "household_resolved");
            return {
              accepted: true,
              duplicate: true,
              disposition: "household_exists",
              ids: { householdId: existing[0].id },
            };
          }
          const result = await new PostgresIdentityRelationships(transaction).createHousehold({
            founderPersonId: actorPersonId,
            timezone: this.config.defaults.timezone,
            createdAt: new Date().toISOString(),
          });
          await this.queueParentGoogleActivationOffer(transaction, actorPersonId, "household_resolved");
          return {
            accepted: true,
            duplicate: false,
            disposition: "household_created",
            ids: { householdId: result.householdId },
          };
        }
        case "invite_household_participant": {
          const invitation = await new HouseholdOnboarding(
            transaction,
            this.secretBox,
          ).inviteCurrentParticipant({
            actorPersonId,
            householdId: command.householdId,
            conversationId: command.conversationId,
            inviteePersonId: command.inviteePersonId,
            role: command.role,
            createdAt: new Date(),
          });
          await this.queueHouseholdInvitationMessage(transaction, invitation.invitationId);
          return {
            accepted: true,
            duplicate: false,
            disposition:
              invitation.approvedByPersonIds.length === invitation.requiredApproverPersonIds.length
                ? "household_invitation_ready"
                : "household_invitation_awaiting_stewards",
            ids: { invitationId: invitation.invitationId, householdId: invitation.householdId },
          };
        }
        case "approve_household_invitation": {
          const invitation = await new HouseholdOnboarding(transaction, this.secretBox).approveInvitation({
            actorPersonId,
            invitationId: command.invitationId,
            approvedAt: new Date(),
          });
          await this.queueHouseholdInvitationMessage(transaction, invitation.invitationId);
          return {
            accepted: true,
            duplicate: false,
            disposition:
              invitation.approvedByPersonIds.length === invitation.requiredApproverPersonIds.length
                ? "household_invitation_ready"
                : "household_invitation_awaiting_stewards",
            ids: { invitationId: invitation.invitationId, householdId: invitation.householdId },
          };
        }
        case "accept_household_invitation": {
          const membership = await new HouseholdOnboarding(transaction, this.secretBox).acceptInvitation({
            actorPersonId,
            invitationId: command.invitationId,
            acceptedAt: new Date(),
          });
          const linkedGroups = await transaction<{ id: string }[]>`
            update conversations conversation
            set household_id = ${membership.householdId}, authority_version = authority_version + 1,
              updated_at = now()
            where conversation.kind = 'group' and conversation.household_id is null
              and exists(
                select 1 from epoch_participants actor
                where actor.participant_epoch_id = conversation.current_epoch_id
                  and actor.person_id = ${actorPersonId}
              )
              and not exists(
                select 1 from epoch_participants participant
                where participant.participant_epoch_id = conversation.current_epoch_id
                  and not exists(
                    select 1 from household_memberships member
                    where member.household_id = ${membership.householdId}
                      and member.person_id = participant.person_id and member.status = 'active'
                  )
              )
            returning conversation.id
          `;
          await transaction`
            update households set status = 'active', updated_at = now()
            where id = ${membership.householdId} and status = 'onboarding'
          `;
          await this.queueParentGoogleActivationOffer(transaction, actorPersonId, "household_resolved");
          for (const group of linkedGroups) {
            await this.queuePrivateGroupReadyNotices(transaction, group.id, "group-ready");
          }
          return {
            accepted: true,
            duplicate: false,
            disposition: "household_invitation_accepted",
            ids: { householdId: membership.householdId, membershipId: membership.membershipId },
          };
        }
        case "add_dependent": {
          const dependent = await new HouseholdOnboarding(transaction, this.secretBox).addDependent({
            actorPersonId,
            householdId: command.householdId,
            displayName: command.displayName,
            aliases: command.aliases,
            birthYear: command.birthYear,
            school: command.school,
            activities: command.activities,
            createdAt: new Date(),
          });
          return {
            accepted: true,
            duplicate: false,
            disposition: "dependent_added",
            ids: {
              dependentPersonId: dependent.dependentPersonId,
              membershipId: dependent.membershipId,
            },
          };
        }
        case "update_dependent": {
          await new HouseholdOnboarding(transaction, this.secretBox).updateDependent({
            actorPersonId,
            householdId: command.householdId,
            dependentPersonId: command.dependentPersonId,
            displayName: command.displayName,
            aliases: command.aliases,
            birthYear: command.birthYear,
            school: command.school,
            activities: command.activities,
            updatedAt: new Date(),
          });
          return {
            accepted: true,
            duplicate: false,
            disposition: "dependent_updated",
            ids: { dependentPersonId: command.dependentPersonId },
          };
        }
        case "approve_group_coverage_rule": {
          const result = await new GroupRuleOnboarding(transaction).approveFamilyCoverage({
            conversationId: command.conversationId,
            actorPersonId,
            expectedParticipantEpochId: command.expectedParticipantEpochId,
            expectedParticipantSetDigest: command.expectedParticipantSetDigest,
            expectedConversationAuthorityVersion: command.expectedConversationAuthorityVersion,
            expectedHouseholdControlEpoch: command.expectedHouseholdControlEpoch,
            approvedAt: new Date(),
          });
          if (result.status === "active") {
            await this.queuePrivateGroupReadyNotices(transaction, result.conversationId, "coverage-active");
          }
          return {
            accepted: true,
            duplicate: result.status === "active" && result.approvedCount === result.requiredCount,
            disposition:
              result.status === "active"
                ? "group_coverage_rule_active"
                : "group_coverage_rule_awaiting_participants",
            ids: { conversationId: result.conversationId },
          };
        }
        case "create_routine": {
          const occurredAt = new Date();
          const routineId = randomUUID();
          const resolved = await resolveRoutineDestination(
            transaction,
            actorPersonId,
            command.destinationConversationId,
          );
          await requireRoutineHolder(transaction, resolved.householdId, command.usualPersonId);
          const result = await new PostgresRoutines(transaction, this.secretBox).save({
            kind: "create",
            routineId,
            householdId: resolved.householdId,
            actorPersonId,
            occurredAt,
            revision: webRoutineRevision({
              fields: command,
              actorPersonId,
              routineId,
              occurredAt,
              authorizationKind: "created",
              destination: resolved.destination,
            }),
          });
          return {
            accepted: true,
            duplicate: false,
            disposition: "routine_created",
            ids: {
              routineId: result.routine.routineId,
              version: String(result.routine.version),
            },
          };
        }
        case "revise_routine": {
          const occurredAt = new Date();
          const householdId = await loadRoutineHousehold(transaction, actorPersonId, command.routineId);
          const resolved = await resolveRoutineDestination(
            transaction,
            actorPersonId,
            command.destinationConversationId,
          );
          if (resolved.householdId !== householdId) {
            throw new UnauthorizedError("A routine must stay within its family");
          }
          await requireRoutineHolder(transaction, householdId, command.usualPersonId);
          const result = await new PostgresRoutines(transaction, this.secretBox).save({
            kind: "revise",
            routineId: command.routineId,
            householdId,
            expectedVersion: command.expectedVersion,
            actorPersonId,
            occurredAt,
            revision: webRoutineRevision({
              fields: command,
              actorPersonId,
              routineId: command.routineId,
              occurredAt,
              authorizationKind: "approved",
              destination: resolved.destination,
            }),
          });
          return {
            accepted: true,
            duplicate: false,
            disposition: "routine_revised",
            ids: {
              routineId: result.routine.routineId,
              version: String(result.routine.version),
            },
          };
        }
        case "set_routine_status": {
          const occurredAt = new Date();
          const householdId = await loadRoutineHousehold(transaction, actorPersonId, command.routineId);
          const result = await new PostgresRoutines(transaction, this.secretBox).setStatus({
            routineId: command.routineId,
            householdId,
            actorPersonId,
            expectedVersion: command.expectedVersion,
            status: command.status,
            occurredAt,
          });
          let timersScheduled = 0;
          for (const entry of result.coverage) {
            const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
              entry.loop.destination.conversationId,
            );
            const timerId = await reconcileCoverageTimers({
              transaction,
              loop: entry.loop,
              snapshot,
              now: occurredAt,
              allowReminder: true,
              openingRequired: true,
            });
            if (timerId) timersScheduled += 1;
          }
          return {
            accepted: true,
            duplicate: result.duplicate,
            disposition:
              command.status === "active"
                ? "routine_resumed"
                : command.status === "paused"
                  ? "routine_paused"
                  : "routine_retired",
            ids: {
              routineId: result.routine.routineId,
              version: String(result.routine.version),
              timersScheduled: String(timersScheduled),
            },
          };
        }
        case "set_calendar_mode": {
          const integrations = await transaction<{ id: string; control_epoch: number | string }[]>`
            select id, control_epoch from integrations where id = ${command.integrationId}
              and person_id = ${actorPersonId} and status <> 'revoked' for update
          `;
          const integration = integrations[0];
          if (!integration) throw new Error("Google connection does not belong to this person");
          const sources = new PostgresSourceIntelligence(transaction, this.secretBox, {
            rawRetentionDays: this.config.defaults.rawSourceRetentionDays,
          });
          const changedAt = new Date();
          const result = await sources.apply({
            kind: "configure_calendar_privacy",
            integrationId: command.integrationId,
            personId: actorPersonId,
            expectedIntegrationControlEpoch: Number(integration.control_epoch),
            calendarIdDigest: sha256Hex(command.calendarId),
            mode: command.mode,
            changedAt: changedAt.toISOString(),
          });
          if (result.kind !== "calendar_privacy_configured")
            throw new Error("Calendar privacy did not update");

          await transaction`
            update jobs
            set status = 'cancelled', lease_owner = null, lease_token = null,
              lease_expires_at = null, last_error_code = 'calendar_policy_changed',
              updated_at = ${changedAt}
            where integration_id = ${command.integrationId}
              and integration_control_epoch = ${result.integrationControlEpoch}
              and person_id = ${actorPersonId}
              and job_kind = 'google.calendar.poll'
              and status in ('pending', 'retry', 'leased')
              and (
                idempotency_key like ${`calendar:poll:${command.integrationId}:%:${result.calendarIdDigest}:%`}
                or idempotency_key like ${`google:calendar:${command.integrationId}:%:${result.calendarIdDigest}:%`}
              )
          `;

          const cursorKind = `calendar:${result.calendarIdDigest}`;
          const cursorRows = await transaction<{ readonly updated_at: Date }[]>`
            select updated_at from sync_cursors
            where integration_id = ${command.integrationId}
              and resource_kind = ${cursorKind}
            for update
          `;
          const resetAt = new Date(Math.max(Date.now(), changedAt.getTime() + 1));
          await sources.apply({
            kind: "checkpoint_cursor",
            integrationId: command.integrationId,
            personId: actorPersonId,
            expectedIntegrationControlEpoch: result.integrationControlEpoch,
            resourceKind: cursorKind,
            cursor: null,
            state: "initial",
            expectedUpdatedAt: cursorRows[0]?.updated_at.toISOString() ?? null,
            checkpointAt: null,
            updatedAt: resetAt.toISOString(),
          });
          if (command.mode !== "off") {
            await new DurableWork(transaction, this.secretBox).enqueue({
              kind: "google.calendar.poll",
              idempotencyKey: `calendar:poll:${command.integrationId}:e${result.integrationControlEpoch}:${result.calendarIdDigest}:v${result.grantVersion}:${command.mode}:policy`,
              payload: {
                integrationId: command.integrationId,
                personId: actorPersonId,
                integrationControlEpoch: result.integrationControlEpoch,
                personControlEpoch: Number(people[0].control_epoch),
                calendarId: command.calendarId,
                calendarIdDigest: result.calendarIdDigest,
                mode: command.mode,
                grantVersion: result.grantVersion,
              },
              person: { id: actorPersonId, controlEpoch: Number(people[0].control_epoch) },
              integration: {
                id: command.integrationId,
                controlEpoch: result.integrationControlEpoch,
              },
              priority: 60,
              maxAttempts: 8,
            });
          }
          return {
            accepted: true,
            duplicate: false,
            disposition: "calendar_mode_updated",
            ids: { integrationId: command.integrationId },
          };
        }
        case "review_private_candidate": {
          const candidateKinds = await transaction<{ readonly candidate_kind: string }[]>`
            select candidate_kind from knowledge_candidates
            where id = ${command.candidateId} and owner_person_id = ${actorPersonId}
              and scope_kind = 'person'
          `;
          if (
            command.decision === "accepted" &&
            candidateKinds[0]?.candidate_kind === "coverage_loop_update_review"
          ) {
            throw new ConflictError(
              "An existing family loop can change only through its explicit update approval flow",
            );
          }
          const result = await new PostgresSourceIntelligence(transaction, this.secretBox, {
            rawRetentionDays: this.config.defaults.rawSourceRetentionDays,
            privateCandidateRetentionDays: 7,
          }).apply({
            kind: "review_private_candidate",
            candidateId: command.candidateId,
            personId: actorPersonId,
            decision: command.decision,
            reviewedAt: new Date().toISOString(),
          });
          if (result.kind !== "private_candidate_reviewed") {
            throw new Error("Private review did not update");
          }
          return {
            accepted: true,
            duplicate: result.duplicate,
            disposition: command.decision === "accepted" ? "private_review_kept" : "private_review_dismissed",
            ids: { candidateId: result.candidateId },
          };
        }
        case "prepare_private_bridge": {
          const result = await new PrivateSourceBridge(
            transaction,
            this.secretBox,
            this.config.defaults.rawSourceRetentionDays,
          ).prepare({
            actorPersonId,
            candidateId: command.candidateId,
            conversationId: command.conversationId,
          });
          return {
            accepted: true,
            duplicate: result.duplicate,
            disposition: "private_bridge_preparing_minimum_meaning",
            ids: { actionIntentId: result.actionIntentId },
          };
        }
        case "approve_private_bridge": {
          const result = await new PrivateSourceBridge(
            transaction,
            this.secretBox,
            this.config.defaults.rawSourceRetentionDays,
          ).approve({
            actorPersonId,
            actionIntentId: command.actionIntentId,
            actionDigest: command.actionDigest,
            dataDigest: command.dataDigest,
            policyDigest: command.policyDigest,
            targetDigest: command.targetDigest,
            mode: command.mode,
          });
          return {
            accepted: true,
            duplicate: result.duplicate,
            disposition: result.bridgeRuleId
              ? "private_bridge_approved_with_standing_rule"
              : "private_bridge_approved_once",
            ids: {
              actionIntentId: result.actionIntentId,
              ...(result.bridgeRuleId ? { bridgeRuleId: result.bridgeRuleId } : {}),
            },
          };
        }
        case "forget_memory": {
          const forgotten = await transaction<{ id: string; current_revision_id: string | null }[]>`
            update memory_records
            set status = 'forgotten', current_revision_id = null,
              version = version + 1, revoked_at = now(), updated_at = now()
            where id = ${command.memoryId} and scope_kind = 'person'
              and owner_person_id = ${actorPersonId} and status = 'accepted'
            returning id, current_revision_id
          `;
          if (forgotten[0]) {
            await transaction`
              delete from memory_revisions
              where memory_record_id = ${forgotten[0].id}
            `;
            await appendAudit(transaction, {
              conversationId: null,
              householdId: null,
              personId: actorPersonId,
              eventType: "private_memory_forgotten",
              targetType: "memory_record",
              targetId: command.memoryId,
              reasons: ["owner_requested_narrowing", "replay_tombstone_retained"],
              manifest: { contentErased: true, exactReplayBlocked: true },
            });
          }
          return {
            accepted: true,
            duplicate: !forgotten[0],
            disposition: "memory_forgotten",
            ids: { memoryId: command.memoryId },
          };
        }
        case "revoke_bridge_rule": {
          const revokedAt = new Date();
          const revoked = await transaction<{ id: string; current_revision_id: string | null }[]>`
            update bridge_rules
            set status = 'revoked', version = version + 1, updated_at = ${revokedAt}
            where id = ${command.ruleId} and owner_person_id = ${actorPersonId}
              and status <> 'revoked'
            returning id, current_revision_id
          `;
          if (revoked[0]?.current_revision_id) {
            await transaction`
              update bridge_rule_revisions set ended_at = coalesce(ended_at, ${revokedAt})
              where id = ${revoked[0].current_revision_id}
            `;
          }
          if (revoked[0]) {
            const intents = await transaction<{ id: string; payload_ciphertext: Buffer; status: string }[]>`
              select id, payload_ciphertext, status from action_intents
              where person_id = ${actorPersonId}
                and action_kind = 'private_source_to_coverage_loop'
                and status in ('proposed', 'awaiting_approval', 'approved', 'executing', 'succeeded')
              for update
            `;
            for (const intent of intents) {
              let belongsToRule = false;
              try {
                belongsToRule =
                  openPrivateBridgePayload(this.secretBox, intent.id, intent.payload_ciphertext).standingRule
                    ?.ruleId === command.ruleId;
              } catch {
                // Unsupported historical actions cannot inherit an active standing authorization.
              }
              if (!belongsToRule) continue;
              await transaction`
                update action_approvals set revoked_at = coalesce(revoked_at, ${revokedAt})
                where action_intent_id = ${intent.id}
              `;
              await transaction`
                update disclosure_decisions decision set revoked_at = coalesce(decision.revoked_at, ${revokedAt})
                from outbox effect
                where effect.action_intent_id = ${intent.id}
                  and effect.authorization_decision_id = decision.id
                  and effect.status in ('pending', 'retry', 'leased')
              `;
              const cancelledEffects = await transaction<{ id: string }[]>`
                update outbox set status = 'cancelled', lease_owner = null, lease_token = null,
                  lease_expires_at = null, updated_at = ${revokedAt}
                where action_intent_id = ${intent.id} and status in ('pending', 'retry', 'leased')
                returning id
              `;
              if (intent.status !== "succeeded" || cancelledEffects.length > 0) {
                await transaction`
                  update action_intents set status = 'cancelled', updated_at = ${revokedAt}
                  where id = ${intent.id}
                `;
              }
              await transaction`
                update jobs set status = 'cancelled', lease_owner = null, lease_token = null,
                  lease_expires_at = null, updated_at = ${revokedAt}
                where idempotency_key in (
                  ${`private-bridge:proposal:${intent.id}`},
                  ${`private-bridge:commit:${intent.id}`}
                ) and status in ('pending', 'retry', 'leased')
              `;
            }
            await appendAudit(transaction, {
              conversationId: null,
              householdId: null,
              personId: actorPersonId,
              eventType: "bridge_rule_revoked",
              targetType: "bridge_rule",
              targetId: command.ruleId,
              reasons: ["owner_requested_narrowing"],
              manifest: { futureDisclosureAllowed: false },
            });
          }
          return {
            accepted: true,
            duplicate: !revoked[0],
            disposition: "bridge_rule_revoked",
            ids: { ruleId: command.ruleId },
          };
        }
        case "disconnect_integration": {
          const integrations = await transaction<{ control_epoch: number | string }[]>`
            select control_epoch from integrations
            where id = ${command.integrationId} and person_id = ${actorPersonId}
              and status <> 'revoked' for update
          `;
          const integration = integrations[0];
          if (!integration) throw new Error("Google connection does not belong to this person");
          const result = await new PostgresSourceIntelligence(transaction, this.secretBox, {
            rawRetentionDays: this.config.defaults.rawSourceRetentionDays,
          }).apply({
            kind: "revoke_integration",
            integrationId: command.integrationId,
            personId: actorPersonId,
            expectedControlEpoch: Number(integration.control_epoch),
            revokedAt: new Date().toISOString(),
          });
          if (result.kind !== "integration_revoked") throw new Error("Google connection was not revoked");
          return {
            accepted: true,
            duplicate: false,
            disposition: "integration_disconnected",
            ids: { integrationId: result.integrationId },
          };
        }
        case "revoke_session": {
          const revoked = await transaction<{ id: string }[]>`
            update person_sessions set revoked_at = now()
            where id = ${command.sessionId} and person_id = ${actorPersonId} and revoked_at is null
            returning id
          `;
          return {
            accepted: true,
            duplicate: !revoked[0],
            disposition: "session_revoked",
            ids: { sessionId: command.sessionId },
          };
        }
        case "delete_person": {
          const result = await new PostgresDataControls(transaction, this.secretBox).deletePerson({
            actorPersonId,
            deletedAt: new Date(),
          });
          return {
            accepted: true,
            duplicate: result.duplicate,
            disposition: "person_deleted",
            ids: { deletionRequestId: result.deletionRequestId },
          };
        }
        case "pause_person": {
          const changed = await transaction<{ id: string }[]>`
            update people set quiet_hours =
                case when jsonb_typeof(quiet_hours) = 'object' then quiet_hours else '{}'::jsonb end
                || ${transaction.json({ proactivePaused: command.paused })},
              authority_version = authority_version + 1, updated_at = now()
            where id = ${actorPersonId}
              and not (
                jsonb_typeof(quiet_hours) = 'object'
                and quiet_hours @> ${transaction.json({ proactivePaused: command.paused })}
              )
            returning id
          `;
          if (changed[0]) {
            await transaction`
              update conversations conversation
              set authority_version = conversation.authority_version + 1, updated_at = now()
              where conversation.current_epoch_id is not null
                and exists (
                  select 1 from epoch_participants participant
                  where participant.participant_epoch_id = conversation.current_epoch_id
                    and participant.person_id = ${actorPersonId}
                )
            `;
          }
          return {
            accepted: true,
            duplicate: !changed[0],
            disposition: command.paused ? "paused" : "resumed",
            ids: {},
          };
        }
        case "request_step_up": {
          await new DurableWork(transaction, this.secretBox).enqueue({
            kind: "auth.send_step_up",
            idempotencyKey: `step-up:${actorPersonId}:${command.purpose}:${JSON.stringify(command.context ?? {})}:${Math.floor(Date.now() / 60_000)}`,
            payload: { actorPersonId, purpose: command.purpose, context: command.context ?? {} },
            person: { id: actorPersonId, controlEpoch: Number(people[0].control_epoch) },
            maxAttempts: 3,
          });
          return { accepted: true, duplicate: false, disposition: "step_up_queued", ids: {} };
        }
      }
    });
  }
}

async function resolveRoutineDestination(
  transaction: Transaction,
  actorPersonId: string,
  conversationId: string,
): Promise<{
  readonly householdId: string;
  readonly destination: RoutineRevisionDraft["destination"];
}> {
  const rows = await transaction<
    {
      readonly household_id: string;
      readonly participant_epoch_id: string;
      readonly participant_set_digest: string;
    }[]
  >`
    select conversation.household_id, epoch.id as participant_epoch_id,
      epoch.participant_set_digest
    from conversations conversation
    join households household on household.id = conversation.household_id
      and household.status = 'active'
    join participant_epochs epoch on epoch.id = conversation.current_epoch_id
      and epoch.ended_at is null
    join epoch_participants participant on participant.participant_epoch_id = epoch.id
      and participant.person_id = ${actorPersonId}
    join household_memberships membership on membership.household_id = household.id
      and membership.person_id = ${actorPersonId} and membership.status = 'active'
    where conversation.id = ${conversationId} and conversation.kind = 'group'
      and conversation.status = 'active'
  `;
  const row = rows[0];
  if (!row) {
    throw new StaleAuthorityError("That family group is no longer available for this routine");
  }
  return {
    householdId: row.household_id,
    destination: {
      conversationId,
      participantEpochId: row.participant_epoch_id,
      participantSetDigest: row.participant_set_digest,
      audience: "group",
    },
  };
}

async function loadRoutineHousehold(
  transaction: Transaction,
  actorPersonId: string,
  routineId: string,
): Promise<string> {
  const rows = await transaction<{ readonly household_id: string }[]>`
    select routine.household_id
    from routines routine
    join households household on household.id = routine.household_id
      and household.status = 'active'
    join household_memberships membership on membership.household_id = household.id
      and membership.person_id = ${actorPersonId} and membership.status = 'active'
    where routine.id = ${routineId}
  `;
  if (!rows[0]) throw new NotFoundError("Routine does not exist in this family");
  return rows[0].household_id;
}

async function requireRoutineHolder(
  transaction: Transaction,
  householdId: string,
  holderPersonId: string | null,
): Promise<void> {
  if (holderPersonId === null) return;
  const rows = await transaction<{ readonly allowed: boolean }[]>`
    select exists(
      select 1 from household_memberships membership
      join people person on person.id = membership.person_id and person.status = 'registered'
      where membership.household_id = ${householdId}
        and membership.person_id = ${holderPersonId} and membership.status = 'active'
        and membership.role <> 'dependent'
    ) as allowed
  `;
  if (!rows[0]?.allowed) {
    throw new UnauthorizedError("The usual person must be an active registered family member");
  }
}

function webRoutineRevision(input: {
  readonly fields: WebRoutineFields;
  readonly actorPersonId: string;
  readonly routineId: string;
  readonly occurredAt: Date;
  readonly authorizationKind: "created" | "approved";
  readonly destination: RoutineRevisionDraft["destination"];
}): RoutineRevisionDraft {
  if (input.fields.standingSelfCoverage && input.fields.usualPersonId !== input.actorPersonId) {
    throw new UnauthorizedError("Standing coverage can only be approved for yourself");
  }
  return {
    title: input.fields.title,
    minimumSharedMeaning: input.fields.sharedMeaning,
    recurrence: {
      kind: "weekly",
      weekdays: [...input.fields.weekdays].sort((left, right) => left - right),
      intervalWeeks: 1,
      startsOn: input.fields.startsOn,
      endsOn: input.fields.endsOn,
      excludedDates: [],
    },
    timePlan: {
      timeZone: input.fields.timeZone,
      event: { kind: "local_clock", time: input.fields.localEventTime, dayOffset: 0 },
      deadline: null,
      preparationMinutes: 0,
      travelMinutes: 0,
      earliestUseful: {
        kind: "relative",
        anchor: "event",
        offsetMinutes: -input.fields.earliestUsefulMinutesBefore,
      },
      lastResponsible: {
        kind: "relative",
        anchor: "event",
        offsetMinutes: -input.fields.lastResponsibleMinutesBefore,
      },
    },
    notificationMode: input.fields.notificationMode,
    destination: input.destination,
    proposedHolderPersonId: input.fields.usualPersonId,
    standingCoverage: input.fields.standingSelfCoverage
      ? {
          holderPersonId: input.actorPersonId,
          authorizedByPersonId: input.actorPersonId,
          authorizationKind: input.authorizationKind,
          authorizedAt: input.occurredAt.toISOString(),
        }
      : null,
    sourceRevisionRefs: [`web:routine:${input.routineId}:${input.occurredAt.toISOString()}`],
    effectiveFrom: input.fields.startsOn,
    effectiveThrough: input.fields.endsOn,
  };
}

function isExplicitEnrollmentConsent(value: string): boolean {
  const normalized = value
    .trim()
    .toLocaleLowerCase("en-US")
    .replace(/[.!]+$/gu, "")
    .replace(/\s+/gu, " ");
  return /^(?:yes(?:,? please)?|yep|yeah|yes,? (?:that sounds good|sounds good|i agree|let's do it)|i (?:agree|consent|do|would like that)|sure(?: thing)?|sounds good|absolutely|okay|ok|sign me up|let's do it)$/u.test(
    normalized,
  );
}

/** A strict social greeting, never a message that may contain family work. */
export function isNaturalPrivateGreeting(chatKind: "direct" | "group", value: string): boolean {
  if (chatKind !== "direct") return false;
  const normalized = value.trim().replace(/\s+/gu, " ");
  return /^(?:hi|hello|hey)(?:[ ,]+(?:there|florence))?[!. 👋]*$/iu.test(normalized);
}

function isExplicitPrivateQuestion(value: string): boolean {
  return /\?|^(?:who|what|when|where|why|how|can|could|would|should|is|are|do|does)\b/iu.test(value.trim());
}

function isSelfNameQuestion(value: string): boolean {
  const normalized = value
    .trim()
    .toLocaleLowerCase("en-US")
    .replace(/[’]/gu, "'")
    .replace(/[.!?]+$/gu, "")
    .replace(/\s+/gu, " ");
  return /^(?:what(?:'s| is) my name|what do you (?:call|know me as)|what should you call me)$/u.test(
    normalized,
  );
}

function parseNameResponse(value: string, allowBareName: boolean): string | null {
  const normalized = value.trim().replace(/\s+/gu, " ");
  if (!normalized || normalized.length > 100 || /[\r\n]/u.test(normalized)) return null;
  const introduced =
    /^(?:actually[, ]+)?(?:my name(?: is|['’]s)|i am|i['’]m|call me)(?:\s+actually)?[, ]+(.+)$/iu.exec(
      normalized,
    );
  const candidate = (introduced?.[1] ?? (allowBareName ? normalized : "")).replace(/[.!]+$/gu, "").trim();
  if (!candidate || candidate.length > 80) return null;
  const reserved = new Set([
    "yes",
    "no",
    "sure",
    "okay",
    "ok",
    "stop",
    "help",
    "settings",
    "sign in",
    "open florence",
    "connect google",
    "review",
    "open review",
    "hi",
    "hello",
    "hey",
  ]);
  if (reserved.has(candidate.toLocaleLowerCase("en-US"))) return null;
  if (!/^[\p{L}\p{M}][\p{L}\p{M}'’ .-]*$/u.test(candidate)) return null;
  if (candidate.split(" ").length > 6) return null;
  return candidate;
}

function decryptPersonName(secretBox: SecretBox, personId: string, ciphertext: Buffer | null): string | null {
  if (!ciphertext) return null;
  try {
    const name = secretBox
      .decrypt(JSON.parse(ciphertext.toString("utf8")) as unknown, `person-display-name:${personId}`)
      .toString("utf8")
      .replace(/\s+/gu, " ")
      .trim();
    return name.length > 0 && name.length <= 80 ? name : null;
  } catch {
    return null;
  }
}

function decryptIdentitySubject(
  secretBox: SecretBox,
  identityId: string,
  ciphertext: Buffer | null,
): string | null {
  if (!ciphertext) return null;
  try {
    const subject = secretBox
      .decrypt(JSON.parse(ciphertext.toString("utf8")) as unknown, `identity-subject:${identityId}`)
      .toString("utf8")
      .trim();
    return subject.length > 0 && subject.length <= 320 ? subject : null;
  } catch {
    return null;
  }
}

function isOutboundIdentitySubject(value: string): boolean {
  if (/^\+[1-9]\d{6,14}$/u.test(value)) return true;
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/u.test(value) && value.length <= 320;
}

function classifyEvent(
  event: Exclude<LinqWebhookEnvelope, { eventType: "linq.ignored" }>,
  chatKind: "direct" | "group",
  mode: ReturnType<typeof evaluateConversationMode>,
): { kind: StoredLinqEvent["classification"]; enrollmentAction?: "consent" | "other"; retainEvent: boolean } {
  if (
    event.eventType === "linq.outbound.sent" ||
    event.eventType === "linq.outbound.delivered" ||
    event.eventType === "linq.outbound.read" ||
    event.eventType === "linq.outbound.failed"
  ) {
    return { kind: "receipt", retainEvent: true };
  }
  if (event.eventType === "linq.message.received") {
    const command = messageText(event).trim();
    if (command.toUpperCase() === "STOP") return { kind: "stop", retainEvent: false };
    if (chatKind === "direct" && mode === "registration_required") {
      return {
        kind: "enrollment",
        enrollmentAction: isExplicitEnrollmentConsent(command) ? "consent" : "other",
        retainEvent: false,
      };
    }
  }
  if (mode === "paused" || (chatKind === "direct" && mode === "observe_only")) {
    return { kind: "routing_only", retainEvent: false };
  }
  if (chatKind === "group" && mode === "observe_only") {
    return { kind: "observe_only", retainEvent: true };
  }
  if (mode === "registration_required") return { kind: "routing_only", retainEvent: false };
  return { kind: "full", retainEvent: true };
}

async function insertProviderEvent(
  transaction: Transaction,
  secretBox: SecretBox,
  event: LinqWebhookEnvelope,
  record: StoredLinqEvent,
  processingStatus: "pending" | "ignored",
): Promise<string> {
  const id = randomUUID();
  const encrypted = secretBox.encrypt(canonicalJson(record), `provider-event:${event.providerEventId}`);
  const externalChannelId = event.eventType === "linq.ignored" ? null : event.channel.providerChatId;
  await transaction`
    insert into provider_events (
      id, provider, provider_event_id, event_type, external_channel_id, occurred_at,
      received_at, payload_digest, envelope_ciphertext, envelope_key_version,
      admission_status, processing_status, processed_at
    ) values (
      ${id}, 'linq', ${event.providerEventId}, ${event.eventType}, ${externalChannelId},
      ${new Date(event.occurredAt)}, ${new Date(event.receivedAt)}, ${canonicalDigest(event)},
      ${Buffer.from(JSON.stringify(encrypted), "utf8")}, ${encrypted.kid}, 'verified',
      ${processingStatus}, ${processingStatus === "ignored" ? new Date() : null}
    )
  `;
  return id;
}

function sanitizedIgnored(_event: LinqWebhookEnvelope): StoredLinqEvent {
  return {
    schemaVersion: 1,
    classification: "routing_only",
    routing: {
      conversationId: "00000000-0000-0000-0000-000000000000",
      participantEpochId: "00000000-0000-0000-0000-000000000000",
      appParticipantDigest: "0".repeat(64),
      providerParticipantDigest: `linq-v1:${"0".repeat(64)}`,
      liveIdentityIds: [],
      senderIdentityId: null,
      senderPersonId: null,
      providerChatId: "00000000-0000-0000-0000-000000000000",
      chatKind: "direct",
    },
  };
}

function hasActiveFlorenceParticipant(liveChat: LinqChatSnapshot): boolean {
  return liveChat.configuredLineActive;
}

function messageText(event: LinqMessageReceivedEvent): string {
  return event.message.parts
    .flatMap((part) => (part.kind === "text" ? [part.text] : part.kind === "link" ? [part.url] : []))
    .join("\n");
}

function isFreshLiveMessage(event: LinqMessageReceivedEvent): boolean {
  if (event.message.reconciledAt !== undefined) return false;
  const age = Date.parse(event.receivedAt) - Date.parse(event.message.sentAt);
  return Number.isFinite(age) && age >= -60_000 && age <= MAX_LIVE_GROUP_INVOCATION_AGE_MS;
}

function normalizeHandle(value: string): string {
  const normalized = value.trim().toLowerCase();
  const phone = normalized.replace(/[\s().-]/gu, "");
  return /^\+[1-9]\d{6,14}$/u.test(phone) ? phone : normalized;
}

async function inferSharedHousehold(
  transaction: Transaction,
  personIds: readonly string[],
): Promise<{ id: string; controlEpoch: number } | null> {
  const uniquePersonIds = [...new Set(personIds)];
  if (uniquePersonIds.length === 0) return null;
  const rows = await transaction<{ id: string; control_epoch: number | string }[]>`
    select household.id, household.control_epoch
    from households household
    join household_memberships membership on membership.household_id = household.id
    where membership.person_id = any(${transaction.array(uniquePersonIds)}::uuid[])
      and membership.status = 'active' and household.status in ('onboarding', 'active', 'paused')
    group by household.id
    having count(distinct membership.person_id) = ${uniquePersonIds.length}
    order by household.created_at limit 2
  `;
  const only = rows.length === 1 ? rows[0] : undefined;
  return only ? { id: only.id, controlEpoch: Number(only.control_epoch) } : null;
}

async function personFence(
  transaction: Transaction,
  personId: string,
): Promise<{ person: { id: string; controlEpoch: number } }> {
  const rows = await transaction<{ control_epoch: number | string }[]>`
    select control_epoch from people where id = ${personId}
  `;
  if (!rows[0]) throw new Error("Person disappeared while authorizing effect");
  return { person: { id: personId, controlEpoch: Number(rows[0].control_epoch) } };
}

async function appendAudit(
  transaction: Transaction,
  input: {
    conversationId: string | null;
    householdId: string | null;
    personId: string | null;
    eventType: string;
    targetType: string;
    targetId: string | null;
    reasons: readonly string[];
    manifest: Record<string, unknown>;
  },
): Promise<void> {
  const sequenceRows = await transaction<{ sequence: number | string }[]>`
    select coalesce(max(sequence), 0) + 1 as sequence from audit_events
    where (${input.householdId}::uuid is not null and household_id = ${input.householdId})
      or (${input.householdId}::uuid is null and ${input.conversationId}::uuid is not null
        and conversation_id = ${input.conversationId})
      or (${input.householdId}::uuid is null and ${input.conversationId}::uuid is null
        and person_id = ${input.personId})
  `;
  await transaction`
    insert into audit_events (
      id, household_id, person_id, conversation_id, sequence, actor_kind, actor_id,
      event_type, target_type, target_id, reason_codes, decision_manifest, occurred_at
    ) values (
      ${randomUUID()}, ${input.householdId}, ${input.personId}, ${input.conversationId},
      ${Number(sequenceRows[0]?.sequence ?? 1)}, 'application', null, ${input.eventType},
      ${input.targetType}, ${input.targetId}, ${transaction.array([...input.reasons])},
      ${transaction.json(JSON.parse(canonicalJson(input.manifest)))}, now()
    )
  `;
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function exactEvidenceSourceRevisionIds(candidate: readonly string[]): readonly string[] {
  if (candidate.length < 1 || candidate.length > 32) {
    throw new UnauthorizedError("Private invocation evidence is outside the allowed bounds");
  }
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;
  if (candidate.some((sourceRevisionId) => !uuid.test(sourceRevisionId))) {
    throw new UnauthorizedError("Private invocation evidence contains an invalid source revision");
  }
  if (new Set(candidate).size !== candidate.length) {
    throw new UnauthorizedError("Private invocation evidence must be distinct");
  }
  return [...candidate];
}
