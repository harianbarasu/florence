import { createHash, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type {
  LinqChatSnapshot,
  LinqMessageReceivedEvent,
  LinqWebhookEnvelope,
} from "../adapters/linq/index.js";
import type { FlorenceConfig } from "../config.js";
import type { Database } from "../db/client.js";
import {
  GOOGLE_CONNECT_HANDOFF_TTL_SECONDS,
  ONBOARDING_HANDOFF_TTL_SECONDS,
  PostgresWebAuth,
} from "../modules/auth/index.js";
import { openPrivateBridgePayload, PrivateSourceBridge } from "../modules/bridges/index.js";
import {
  type ConversationAuthoritySnapshot,
  evaluateConversationMode,
  FamilyGroupAuthority,
  type GroupInvocation,
  leadingGroupInvocation,
  PostgresConversationAuthority,
  provenReplyGroupInvocation,
} from "../modules/conversations/index.js";
import { PostgresRoutines, type RoutineRevisionDraft } from "../modules/coordination/index.js";
import { PostgresDataControls } from "../modules/data-controls/index.js";
import { EffectOutbox } from "../modules/effects/index.js";
import { type HouseholdMembership, PostgresIdentityRelationships } from "../modules/identity/index.js";
import {
  type FamilyOnboardingProjection,
  PostgresFamilyOnboarding,
} from "../modules/relationships/family-onboarding.js";
import { HouseholdOnboarding } from "../modules/relationships/index.js";
import { type IntegrationCapability, PostgresSourceIntelligence } from "../modules/sources/index.js";
import { DurableWork } from "../modules/work/index.js";
import { canonicalDigest, canonicalJson } from "../shared/canonical-json.js";
import type { SecretBox } from "../shared/crypto.js";
import { ConflictError, NotFoundError, StaleAuthorityError, UnauthorizedError } from "../shared/errors.js";
import type {
  AppEnvelope,
  ApplicationTimerProcessor,
  ExpectedConversationAuthority,
  ProcessReceipt,
  WebRoutineFields,
} from "./contracts.js";
import { CoverageCoordinator } from "./coverage-coordinator.js";
import { reconcileCoverageTimers } from "./coverage-timer-reconciliation.js";
import { GoogleSyncCoordinator } from "./google-sync-coordinator.js";
import { decideOnboardingReminder, type OnboardingReminderStep } from "./onboarding-reminder-policy.js";
import {
  PostgresPrivateOnboardingGuidance,
  type PrivateOnboardingGuidance,
} from "./private-onboarding-guidance.js";
import { PrivateSourceReconciler } from "./private-source-reconciler.js";

type Transaction = TransactionSql<Record<string, never>>;

const PRIVATE_GUIDANCE_RESPONSE_TTL_MS = 10 * 60_000;
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
  readonly enrollmentAction?: "consent" | "decline_invitation" | "other";
  /** Provider occurrence time for an inbound message, retained even when its content is not. */
  readonly messageOccurredAt?: string;
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

interface LatestRelevantPrivateEffect {
  readonly kind: "enrollment" | "family_invitation" | "other";
  readonly invitationId: string | null;
}

type ParentOnboardingReason = "registration" | "household_resolved" | "invitation_accepted" | "resume";

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
          input.expectedPerson,
          input.expectedConversation,
        );
      case "linq.group_invocation_response":
        return this.commitWritableGroupInvocationResponse(
          input.internalProviderEventId,
          input.responseText,
          input.evidenceSourceRevisionIds,
          input.expectedPerson,
          input.expectedConversation,
          input.expectedHousehold ?? null,
        );
      case "linq.family_introduction_proposal":
        return this.commitFamilyIntroductionProposal(
          input.internalProviderEventId,
          input.sourceRevisionId,
          input.proposal,
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
      case "onboarding.reminder_due":
        return this.processOnboardingReminder(input);
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
      case "private_source.select_candidate_release":
        return this.selectPrivateSourceCandidateRelease(input);
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
      if (consumed.returnPath === "/onboarding") {
        await this.noteOnboardingProgress(transaction, consumed.personId, new Date(input.completedAt));
      }

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
            ? "private_source_candidate_waiting_behind_current_winner"
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

  private async selectPrivateSourceCandidateRelease(
    input: Extract<AppEnvelope, { kind: "private_source.select_candidate_release" }>,
  ): Promise<ProcessReceipt> {
    const result = await new GoogleSyncCoordinator(
      this.database,
      this.config,
      this.secretBox,
    ).selectPrivateSourceCandidate(input);
    return {
      accepted: result.kind !== "not_ready",
      duplicate: result.kind === "obsolete",
      disposition:
        result.kind === "selected"
          ? "private_source_candidate_selected_for_release"
          : result.kind === "not_ready"
            ? "private_source_candidate_waiting_behind_current_winner"
            : "private_source_candidate_notice_obsolete",
      ids: { candidateId: input.candidateId },
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
        liveChat.kind === "group" &&
        (classification.kind === "full" || classification.kind === "observe_only") &&
        event.eventType === "linq.message.received" &&
        isFreshLiveMessage(event) &&
        (await this.isExactRegisteredGroupSender(transaction, reconciled))
          ? await this.resolveGroupInvocation(transaction, event, reconciled)
          : null;
      const record: StoredLinqEvent = {
        schemaVersion: 1,
        classification: classification.kind,
        ...(classification.enrollmentAction ? { enrollmentAction: classification.enrollmentAction } : {}),
        ...(event.eventType === "linq.message.received" ? { messageOccurredAt: event.message.sentAt } : {}),
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
    const familyAuthority =
      liveChat.kind === "group"
        ? await new FamilyGroupAuthority(transaction).reconcile({
            conversationId: binding.conversationId,
            occurredAt: checkedAt,
          })
        : null;
    const snapshot = familyAuthority?.snapshot ?? (await conversations.snapshot(binding.conversationId));
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
      householdId: familyAuthority?.householdId ?? null,
      householdControlEpoch: familyAuthority?.householdControlEpoch ?? null,
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

  private async resolveGroupInvocation(
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
    return Number(rows[0]?.matching_effects ?? 0) === 1 ? provenReplyGroupInvocation(text) : null;
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
      };
    }
    // Invocation freshness is decided before admission. Once admitted, its
    // private reply obligation survives worker outages until authority revokes it.
    await new DurableWork(transaction, this.secretBox).enqueue({
      kind: "orchestrate.linq_observation",
      idempotencyKey: `orchestrate:linq-observation:${internalProviderEventId}`,
      payload: { internalProviderEventId },
      ...(invocationAuthority ?? {}),
      maxAttempts: 5,
    });
    return record.invocation ? "private_invocation_queued" : "observation_queued";
  }

  private async resolveExactPrivateRoute(
    transaction: Transaction,
    personId: string,
    currentRecord?: StoredLinqEvent,
  ): Promise<ExactPrivateRoute | null> {
    if (
      currentRecord?.routing.chatKind === "direct" &&
      currentRecord.routing.senderPersonId === personId &&
      currentRecord.routing.senderIdentityId !== null &&
      currentRecord.routing.liveIdentityIds.length === 1 &&
      currentRecord.routing.liveIdentityIds[0] === currentRecord.routing.senderIdentityId
    ) {
      return {
        conversationId: currentRecord.routing.conversationId,
        participantEpochId: currentRecord.routing.participantEpochId,
        participantSetDigest: currentRecord.routing.appParticipantDigest,
        liveIdentityIds: [...currentRecord.routing.liveIdentityIds],
        privateIdentityId: currentRecord.routing.senderIdentityId,
        providerChatId: currentRecord.routing.providerChatId,
        providerParticipantDigest: currentRecord.routing.providerParticipantDigest,
      };
    }
    if (currentRecord) return null;
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
    if (!saved?.latest_participant_digest) return null;
    return {
      conversationId: saved.conversation_id,
      participantEpochId: saved.participant_epoch_id,
      participantSetDigest: saved.participant_set_digest,
      liveIdentityIds: [saved.identity_id],
      privateIdentityId: saved.identity_id,
      providerChatId: saved.external_channel_id,
      providerParticipantDigest: saved.latest_participant_digest,
    };
  }

  /** Makes an accepted relationship the exact family whose onboarding state governs this person. */
  private async selectAcceptedOnboardingHousehold(
    transaction: Transaction,
    personId: string,
    householdId: string,
    selectedAt: Date,
  ): Promise<void> {
    const onboarding = new PostgresFamilyOnboarding(this.secretBox);
    const projection = await onboarding.project(transaction, {
      actorPersonId: personId,
      personId,
    });
    await onboarding.selectHousehold(transaction, {
      actorPersonId: personId,
      personId,
      householdId,
      expectedPersonOnboardingVersion: projection.profile.onboardingVersion,
      selectedAt,
    });
  }

  private async requireConfirmedPersonTimeZone(transaction: Transaction, personId: string): Promise<string> {
    const rows = await transaction<{ readonly timezone: string | null }[]>`
      select person.timezone
      from people person
      join person_onboarding onboarding on onboarding.person_id = person.id
        and onboarding.profile_review_version > 0
        and onboarding.reviewed_person_authority_version = person.authority_version
      where person.id = ${personId} and person.status = 'registered'
      for share of person, onboarding
    `;
    const timeZone = rows[0]?.timezone;
    if (!timeZone || !validTimeZone(timeZone)) {
      throw new ConflictError("Confirm your time zone before creating a Florence family");
    }
    return timeZone;
  }

  /** Queues one exact-private entry into the canonical family onboarding journey. */
  private async queueParentOnboardingOffer(
    transaction: Transaction,
    personId: string,
    reason: ParentOnboardingReason,
    currentRecord?: StoredLinqEvent,
    resumeKey?: string,
  ): Promise<{ readonly outboxId: string; readonly created: boolean } | null> {
    const people = await transaction<{ readonly control_epoch: number | string }[]>`
      select person.control_epoch
      from people person
      where person.id = ${personId} and person.status = 'registered'
        and not exists (
          select 1 from person_onboarding selection
          join household_memberships membership
            on membership.person_id = selection.person_id
            and membership.household_id = selection.selected_household_id
            and membership.status = 'active' and membership.role <> 'dependent'
          join households household on household.id = membership.household_id
            and household.status in ('onboarding', 'active')
          join membership_capabilities read_capability on read_capability.membership_id = membership.id
            and read_capability.capability = 'household.read' and read_capability.status = 'active'
          join membership_onboarding onboarding on onboarding.membership_id = membership.id
          where selection.person_id = person.id and onboarding.completed_at is not null
        )
      for update of person
    `;
    const person = people[0];
    if (!person) return null;
    const route = await this.resolveExactPrivateRoute(transaction, personId, currentRecord);
    if (!route) return null;
    const sendKind = currentRecord ? "direct_response" : "transactional";
    const authority = await new PostgresConversationAuthority(transaction).authorizeSend({
      conversationId: route.conversationId,
      expectedParticipantEpochId: route.participantEpochId,
      expectedParticipantSetDigest: route.participantSetDigest,
      liveParticipantIdentityIds: [...route.liveIdentityIds],
      sendKind,
      operation: "family_onboarding",
      ruleId: null,
    });
    if (!authority.allowed || !authority.participantEpochId || !authority.participantSetDigest) {
      return null;
    }
    const idempotencyKey =
      reason === "resume" && resumeKey
        ? `parent-onboarding:${personId}:resume:${resumeKey}`
        : reason === "invitation_accepted" && resumeKey
          ? `parent-onboarding:${personId}:invitation:${resumeKey}`
          : `parent-onboarding:${personId}:initial`;
    const existing = await transaction<{ readonly present: boolean }[]>`
      select exists(
        select 1 from outbox
        where idempotency_key = ${idempotencyKey}
          and status in ('pending', 'leased', 'retry', 'submitted', 'confirmed')
      ) as present
    `;
    if (existing[0]?.present) return null;
    const handoff = await new PostgresWebAuth(
      transaction,
      this.secretBox,
      this.config.security.tokenKey,
    ).createHandoff({
      personId,
      privateIdentityId: route.privateIdentityId,
      privateConversationId: route.conversationId,
      purpose: "onboarding",
      context: { returnPath: "/onboarding" },
      expiresInSeconds: ONBOARDING_HANDOFF_TTL_SECONDS,
    });
    const link = `${this.config.publicBaseUrl}/handoff/${handoff.token}`;
    const text =
      reason === "resume"
        ? `I saved where you left off. Continue your private family setup here: ${link}\n\nIt takes you back to the exact next step. If the link expires, ask me to continue setup.`
        : reason === "invitation_accepted"
          ? `You’re all set—your Florence family membership is active. I’ll reuse the family details already there instead of asking you to repeat them.\n\nFinish your private setup here: ${link}\n\nI save each step, so you can leave and come back. If the link expires, ask me to continue setup.`
          : `Let’s set Florence up around your actual family. I’ll confirm you, learn who helps coordinate, and capture the children, schools, and activities I should recognize. Then you can connect Gmail and Calendar—or use Florence without them.\n\nStart here: ${link}\n\nI save each step, so you can leave and come back. If the link expires, ask me to continue setup.`;
    const queued = await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
      actorPersonId: personId,
      person: { id: personId, controlEpoch: Number(person.control_epoch) },
      conversation: { id: route.conversationId, authorityVersion: authority.authorityVersion },
      participantEpochId: authority.participantEpochId,
      expectedParticipantDigest: authority.participantSetDigest,
      effectKind: "linq.message",
      idempotencyKey,
      data: { reason, textDigest: sha256Hex(text) },
      policy: { exactPrivateDm: true, operation: "family_onboarding", sendKind },
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
      reasonCodes: ["registered_private_person", "family_onboarding_incomplete", reason],
      authorizationExpiresAt: handoff.expiresAt,
    });
    if (queued.created) {
      const onboarding = new PostgresFamilyOnboarding(this.secretBox);
      await onboarding.touchProgress(transaction, {
        actorPersonId: personId,
        personId,
        expectedPersonControlEpoch: Number(person.control_epoch),
        progressedAt: new Date(),
      });
      await this.reconcileOnboardingReminder(transaction, personId, new Date());
    }
    return queued;
  }

  private async processOnboardingReminder(
    input: Extract<AppEnvelope, { kind: "onboarding.reminder_due" }>,
  ): Promise<ProcessReceipt> {
    return this.database.begin(async (transaction) => {
      const onboarding = new PostgresFamilyOnboarding(this.secretBox);
      const projection = await onboarding.project(transaction, {
        actorPersonId: input.personId,
        personId: input.personId,
      });
      if (
        projection.profile.controlEpoch !== input.expectedPersonControlEpoch ||
        projection.profile.onboardingVersion !== input.expectedOnboardingVersion
      ) {
        await this.reconcileOnboardingReminder(transaction, input.personId, new Date());
        return {
          accepted: true,
          duplicate: true,
          disposition: "onboarding_reminder_superseded",
          ids: { personId: input.personId },
        };
      }
      const route = await this.resolveExactPrivateRoute(transaction, input.personId);
      const privateAuthority = route ? await this.onboardingReminderAuthority(transaction, route) : null;
      const nextStep = onboardingReminderStep(projection);
      const decision = decideOnboardingReminder({
        onboardingComplete: projection.nextStep.kind === "complete",
        reminderStage: reminderStage(projection.profile.remindersSent),
        savedNextStep: nextStep,
        lastProgressedAt: projection.profile.lastProgressedAt ?? input.dueAt,
        lastRemindedAt: projection.profile.lastRemindedAt,
        timeZone: projection.profile.timezone ?? this.config.defaults.timezone,
        suppressedAt: projection.profile.remindersSuppressedAt,
        privateRouteAvailable: route !== null,
        privateAuthority: route === null ? "absent" : privateAuthority ? "current" : "invalid",
        now: new Date().toISOString(),
      });
      if (decision.kind === "suppress") {
        return {
          accepted: true,
          duplicate: false,
          disposition: `onboarding_reminder_${decision.reason}`,
          ids: { personId: input.personId },
        };
      }
      if (decision.kind === "schedule") {
        await this.enqueueOnboardingReminder(transaction, projection, decision.stage, decision.dueAt);
        return {
          accepted: true,
          duplicate: false,
          disposition: "onboarding_reminder_rescheduled",
          ids: { personId: input.personId, stage: String(decision.stage) },
        };
      }
      if (!route || !privateAuthority) {
        throw new StaleAuthorityError("Private onboarding reminder authority changed");
      }
      if (!privateAuthority.participantEpochId || !privateAuthority.participantSetDigest) {
        throw new StaleAuthorityError("Private onboarding reminder has no current audience");
      }
      const recorded = await onboarding.recordReminderSent(transaction, {
        personId: input.personId,
        expectedPersonControlEpoch: projection.profile.controlEpoch,
        expectedPersonOnboardingVersion: projection.profile.onboardingVersion,
        sentAt: new Date(),
      });
      const handoff = await new PostgresWebAuth(
        transaction,
        this.secretBox,
        this.config.security.tokenKey,
      ).createHandoff({
        personId: input.personId,
        privateIdentityId: route.privateIdentityId,
        privateConversationId: route.conversationId,
        purpose: "onboarding",
        context: { returnPath: "/onboarding" },
        expiresInSeconds: ONBOARDING_HANDOFF_TTL_SECONDS,
      });
      const text = `${decision.copy.lead} ${decision.copy.progress} ${decision.copy.nextStep}\n\n${decision.copy.action} ${this.config.publicBaseUrl}/handoff/${handoff.token}`;
      const queued = await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
        actorPersonId: input.personId,
        person: { id: input.personId, controlEpoch: projection.profile.controlEpoch },
        conversation: {
          id: route.conversationId,
          authorityVersion: privateAuthority.authorityVersion,
        },
        participantEpochId: privateAuthority.participantEpochId,
        expectedParticipantDigest: privateAuthority.participantSetDigest,
        effectKind: "linq.message",
        idempotencyKey: `onboarding-reminder:${input.personId}:stage:${decision.stage}`,
        data: { stage: decision.stage, textDigest: sha256Hex(text) },
        policy: {
          exactPrivateDm: true,
          operation: "family_onboarding_reminder",
          sendKind: "transactional",
        },
        target: {
          providerChatId: route.providerChatId,
          personId: input.personId,
          participantEpochId: privateAuthority.participantEpochId,
        },
        payload: {
          providerChatId: route.providerChatId,
          expectedProviderParticipantDigest: route.providerParticipantDigest,
          text,
        },
        reasonCodes: ["onboarding_incomplete", "bounded_private_reminder"],
        authorizationExpiresAt: handoff.expiresAt,
      });
      const current = await onboarding.project(transaction, {
        actorPersonId: input.personId,
        personId: input.personId,
      });
      if (current.profile.onboardingVersion !== recorded.version) {
        throw new StaleAuthorityError("Onboarding reminder state changed during commit");
      }
      await this.reconcileOnboardingReminder(transaction, input.personId, new Date());
      return {
        accepted: true,
        duplicate: !queued.created,
        disposition: "onboarding_reminder_queued",
        ids: { personId: input.personId, stage: String(decision.stage), outboxId: queued.outboxId },
      };
    });
  }

  private async reconcileOnboardingReminder(
    transaction: Transaction,
    personId: string,
    now: Date,
  ): Promise<void> {
    const onboarding = new PostgresFamilyOnboarding(this.secretBox);
    const projection = await onboarding.project(transaction, {
      actorPersonId: personId,
      personId,
    });
    const nextStep = onboardingReminderStep(projection);
    if (!nextStep || projection.nextStep.kind === "complete" || !projection.profile.lastProgressedAt) return;
    const route = await this.resolveExactPrivateRoute(transaction, personId);
    const authority = route ? await this.onboardingReminderAuthority(transaction, route) : null;
    const decision = decideOnboardingReminder({
      onboardingComplete: false,
      reminderStage: reminderStage(projection.profile.remindersSent),
      savedNextStep: nextStep,
      lastProgressedAt: projection.profile.lastProgressedAt,
      lastRemindedAt: projection.profile.lastRemindedAt,
      timeZone: projection.profile.timezone ?? this.config.defaults.timezone,
      suppressedAt: projection.profile.remindersSuppressedAt,
      privateRouteAvailable: route !== null,
      privateAuthority: route === null ? "absent" : authority ? "current" : "invalid",
      now: now.toISOString(),
    });
    if (decision.kind === "schedule" || decision.kind === "send") {
      await this.enqueueOnboardingReminder(transaction, projection, decision.stage, decision.dueAt);
    }
  }

  private async noteOnboardingProgress(
    transaction: Transaction,
    personId: string,
    progressedAt: Date,
  ): Promise<void> {
    const onboarding = new PostgresFamilyOnboarding(this.secretBox);
    const projection = await onboarding.project(transaction, {
      actorPersonId: personId,
      personId,
    });
    if (projection.nextStep.kind === "complete") return;
    await onboarding.touchProgress(transaction, {
      actorPersonId: personId,
      personId,
      expectedPersonControlEpoch: projection.profile.controlEpoch,
      progressedAt,
    });
    await this.reconcileOnboardingReminder(transaction, personId, progressedAt);
  }

  private async enqueueOnboardingReminder(
    transaction: Transaction,
    projection: FamilyOnboardingProjection,
    stage: 1 | 2,
    dueAt: string,
  ): Promise<void> {
    await new DurableWork(transaction, this.secretBox).enqueue({
      kind: "onboarding.reminder",
      idempotencyKey: `onboarding-reminder:${projection.personId}:v${projection.profile.onboardingVersion}:stage${stage}:${sha256Hex(dueAt).slice(0, 12)}`,
      payload: {
        personId: projection.personId,
        expectedPersonControlEpoch: projection.profile.controlEpoch,
        expectedOnboardingVersion: projection.profile.onboardingVersion,
        targetStage: stage,
        dueAt,
      },
      person: { id: projection.personId, controlEpoch: projection.profile.controlEpoch },
      availableAt: new Date(dueAt),
      maxAttempts: 8,
      priority: 45,
    });
  }

  private async onboardingReminderAuthority(transaction: Transaction, route: ExactPrivateRoute) {
    const authority = await new PostgresConversationAuthority(transaction).authorizeSend({
      conversationId: route.conversationId,
      expectedParticipantEpochId: route.participantEpochId,
      expectedParticipantSetDigest: route.participantSetDigest,
      liveParticipantIdentityIds: [...route.liveIdentityIds],
      sendKind: "transactional",
      operation: "family_onboarding_reminder",
      ruleId: null,
    });
    return authority.allowed && authority.participantEpochId && authority.participantSetDigest
      ? authority
      : null;
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
      }
      let text: string;
      let operation: string;
      let idempotencyKey = `general-answer:${input.internalProviderEventId}`;
      let evidenceSourceRevisionIds: readonly string[] = [];
      let responseHousehold: { readonly id: string; readonly controlEpoch: number } | null = null;
      let authorizationExpiresAt = new Date(
        Date.now() + this.config.defaults.rawSourceRetentionDays * 86_400_000,
      );
      if (input.response.kind === "greeting_acknowledgment") {
        text = "Hi! I’m here. What can I help you with?";
        operation = "private_dm_greeting";
        idempotencyKey = `private-dm-greeting:${input.internalProviderEventId}`;
      } else {
        // The exact processed DM, current sender, epoch, consent, and response
        // policy are the authority boundary. Punctuation is not.
        text = input.response.text.trim();
        if (!text || text.length > 10_000) {
          throw new UnauthorizedError("Private DM answer is outside the allowed bounds");
        }
        assertExpectedConversationAuthority(
          input.response.expectedConversation,
          source.snapshot.conversationId,
          source.snapshot.authorityVersion,
          source.snapshot.participantEpochId,
          source.snapshot.participantSetDigest,
        );
        await assertExpectedResponseAuthority(
          transaction,
          source.record,
          input.response.expectedPerson,
          input.response.expectedHousehold ?? null,
        );
        responseHousehold = input.response.expectedHousehold ?? null;
        evidenceSourceRevisionIds =
          input.response.evidenceSourceRevisionIds.length > 0
            ? exactEvidenceSourceRevisionIds(input.response.evidenceSourceRevisionIds)
            : [];
        await assertPrivateQuestionSourceAuthorities(
          transaction,
          source.personId,
          input.response.sourceAuthorities,
        );
        if (input.response.guidance) {
          const guidance = await new PostgresPrivateOnboardingGuidance(
            transaction,
            this.secretBox,
          ).projectPrivateGuidance({
            personId: source.personId,
            expectedPersonControlEpoch: input.response.expectedPerson.controlEpoch,
          });
          if (
            guidance.stateDigest !== input.response.guidance.stateDigest ||
            guidance.recommendedNextStep.kind !== input.response.guidance.step
          ) {
            throw new StaleAuthorityError("Private guidance changed before response commit");
          }
          if (
            (guidance.household === null) !== (responseHousehold === null) ||
            (guidance.household !== null &&
              (guidance.household.id !== responseHousehold?.id ||
                guidance.household.controlEpoch !== responseHousehold.controlEpoch))
          ) {
            throw new StaleAuthorityError("Private guidance household changed before response commit");
          }
          const guidanceIssuedAt = new Date();
          const guidanceBucket = Math.floor(guidanceIssuedAt.getTime() / PRIVATE_GUIDANCE_RESPONSE_TTL_MS);
          idempotencyKey = `general-answer:${input.internalProviderEventId}:guidance:${input.response.guidance.stateDigest}:${guidanceBucket}`;
          const guidanceAuthorizationExpiresAt = new Date(
            guidanceIssuedAt.getTime() + PRIVATE_GUIDANCE_RESPONSE_TTL_MS,
          );
          if (guidanceAuthorizationExpiresAt < authorizationExpiresAt) {
            authorizationExpiresAt = guidanceAuthorizationExpiresAt;
          }
          if (input.response.guidance.useRecommendedNextStep) {
            const priorText = await recoverPriorConversationMessageText(
              transaction,
              this.secretBox,
              idempotencyKey,
            );
            if (priorText) {
              text = priorText;
            } else if (guidance.recommendedNextStep.action !== "none") {
              text = await this.appendPrivateGuidanceHandoff(transaction, source, text, guidance);
            }
          }
        }
        const sourceIntelligence = new PostgresSourceIntelligence(transaction, this.secretBox, {
          rawRetentionDays: this.config.defaults.rawSourceRetentionDays,
          privateCandidateRetentionDays: 7,
        });
        const evidenceReadAt = new Date();
        for (const sourceRevisionId of evidenceSourceRevisionIds) {
          const evidence = await sourceIntelligence
            .read({
              kind: "source_revision",
              sourceRevisionId,
              scope: { kind: "person", personId: source.personId },
              asOf: evidenceReadAt.toISOString(),
            })
            .catch((error: unknown) => {
              if (error instanceof NotFoundError || error instanceof UnauthorizedError) {
                throw new StaleAuthorityError("Private answer evidence changed before commit");
              }
              throw error;
            });
          if (evidence.kind !== "source_revision") {
            throw new StaleAuthorityError("Private answer evidence is no longer readable");
          }
          const accessExpiresAt = new Date(evidence.accessExpiresAt);
          if (accessExpiresAt <= new Date(evidenceReadAt.getTime() + 3 * 60_000)) {
            throw new StaleAuthorityError("Private answer evidence is too close to expiry");
          }
          if (accessExpiresAt < authorizationExpiresAt) authorizationExpiresAt = accessExpiresAt;
        }
        operation = "general_answer";
      }

      const queued = await this.queueAuthorizedConversationMessage(
        transaction,
        source.record,
        source.snapshot,
        text,
        "direct_response",
        operation,
        null,
        authorizationExpiresAt,
        idempotencyKey,
        evidenceSourceRevisionIds,
        responseHousehold,
      );
      if (!queued) throw new StaleAuthorityError("Private DM response is no longer authorized");
      return { ...queued, personId: source.personId };
    });

    let onboardingOfferFailed = false;
    const onboardingOffered = await this.database
      .begin(async (transaction) => {
        const source = await this.requireProcessedPrivateDmSource(transaction, input.internalProviderEventId);
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

        const offered = await this.queueParentOnboardingOffer(
          transaction,
          source.personId,
          "registration",
          source.record,
        );
        if (offered) {
          await transaction`
          update outbox
          set available_at = greatest(
            available_at,
            ${new Date(committedResponse.created_at.getTime() + 1)}
          )
          where idempotency_key = ${`parent-onboarding:${source.personId}:initial`}
        `;
        }
        return offered !== null;
      })
      .catch(() => {
        onboardingOfferFailed = true;
        return false;
      });

    return {
      accepted: true,
      duplicate: !response.created && !onboardingOffered,
      disposition: onboardingOffered
        ? "private_dm_response_then_onboarding_queued"
        : onboardingOfferFailed
          ? "private_dm_response_queued_after_onboarding_failure"
          : "private_dm_response_queued",
      ids: {
        providerEventId: input.internalProviderEventId,
        responseOutboxId: response.outboxId,
      },
    };
  }

  private async appendPrivateGuidanceHandoff(
    transaction: Transaction,
    source: ProcessedPrivateDmSource,
    text: string,
    guidance: PrivateOnboardingGuidance,
  ): Promise<string> {
    const identityId = source.record.routing.senderIdentityId;
    if (!identityId || source.record.routing.chatKind !== "direct") {
      throw new StaleAuthorityError("Private guidance no longer has an exact action route");
    }
    const handoff = await new PostgresWebAuth(
      transaction,
      this.secretBox,
      this.config.security.tokenKey,
    ).createHandoff({
      personId: source.personId,
      privateIdentityId: identityId,
      privateConversationId: source.record.routing.conversationId,
      purpose: "onboarding",
      context: { returnPath: "/onboarding" },
      expiresInSeconds: ONBOARDING_HANDOFF_TTL_SECONDS,
    });
    const link = `${this.config.publicBaseUrl}/handoff/${handoff.token}`;
    return `${text}\n\n${privateGuidanceActionCopy(guidance.recommendedNextStep.kind)} ${link}\n\nI save each step. If the link expires, ask me to continue setup.`;
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
    expectedPerson: { readonly id: string; readonly controlEpoch: number },
    expectedConversation: ExpectedConversationAuthority,
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
        for update of conversation
      `;
      const source = sources[0];
      if (!source) throw new StaleAuthorityError("Private group invocation audience changed");
      assertExpectedConversationAuthority(
        expectedConversation,
        record.routing.conversationId,
        Number(source.group_authority_version),
        source.group_epoch_id,
        source.group_participant_digest,
      );
      if (
        record.routing.senderPersonId !== expectedPerson.id ||
        Number(source.person_control_epoch) !== expectedPerson.controlEpoch
      ) {
        throw new StaleAuthorityError("Private group invocation person authority changed");
      }

      const evidenceReadAt = new Date();
      const sourceIntelligence = new PostgresSourceIntelligence(transaction, this.secretBox, {
        rawRetentionDays: this.config.defaults.rawSourceRetentionDays,
        privateCandidateRetentionDays: 7,
      });
      let evidenceAccessExpiresAt = new Date(
        Date.now() + this.config.defaults.rawSourceRetentionDays * 86_400_000,
      );
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

      let responseEffect: { readonly outboxId: string; readonly created: boolean };
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
        responseEffect = await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
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
        responseEffect = await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
          ...commonEffect,
          target: { recipient, personId: record.routing.senderPersonId },
          payload: { recipient, text: firstMessageText },
        });
      }

      return {
        accepted: true,
        duplicate: !responseEffect.created,
        disposition: "private_group_invocation_response_queued",
        ids: {
          providerEventId: internalProviderEventId,
          responseOutboxId: responseEffect.outboxId,
        },
      };
    });
  }

  private async commitWritableGroupInvocationResponse(
    internalProviderEventId: string,
    responseCandidate: string,
    evidenceSourceRevisionIdsCandidate: readonly string[],
    expectedPerson: { readonly id: string; readonly controlEpoch: number },
    expectedConversation: ExpectedConversationAuthority,
    expectedHousehold: { readonly id: string; readonly controlEpoch: number } | null,
  ): Promise<ProcessReceipt> {
    const responseText = responseCandidate.trim();
    if (!responseText || responseText.length > 10_000) {
      throw new UnauthorizedError("Group invocation response is outside the allowed bounds");
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
        throw new StaleAuthorityError("Group invocation source is no longer processed");
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
        record.classification !== "full" ||
        !record.invocation ||
        record.routing.chatKind !== "group" ||
        record.event?.eventType !== "linq.message.received" ||
        !record.routing.senderIdentityId ||
        !record.routing.senderPersonId
      ) {
        throw new UnauthorizedError("Provider event is not an eligible writable group invocation");
      }

      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        record.routing.conversationId,
      );
      const sender = snapshot.participants.find(
        (participant) =>
          participant.personIdentityId === record.routing.senderIdentityId &&
          participant.personId === record.routing.senderPersonId,
      );
      if (
        snapshot.conversationKind !== "group" ||
        snapshot.conversationStatus !== "active" ||
        snapshot.participantEpochId !== record.routing.participantEpochId ||
        snapshot.participantSetDigest !== record.routing.appParticipantDigest ||
        sender?.registrationStatus !== "registered" ||
        sender.consentedAt === null ||
        sender.policy?.allowContentProcessing !== true ||
        sender.policy.allowDirectResponses !== true
      ) {
        throw new StaleAuthorityError("Writable group invocation authority changed before response");
      }
      assertExpectedConversationAuthority(
        expectedConversation,
        snapshot.conversationId,
        snapshot.authorityVersion,
        snapshot.participantEpochId,
        snapshot.participantSetDigest,
      );
      await assertExpectedResponseAuthority(transaction, record, expectedPerson, expectedHousehold);

      const responseEffect = await this.queueAuthorizedConversationMessage(
        transaction,
        record,
        snapshot,
        responseText,
        "direct_response",
        "general_answer",
        null,
        new Date(Date.now() + this.config.defaults.rawSourceRetentionDays * 86_400_000),
        `general-answer:${internalProviderEventId}`,
        evidenceSourceRevisionIds,
      );
      return {
        accepted: true,
        duplicate: !responseEffect.created,
        disposition: "group_invocation_response_queued",
        ids: {
          providerEventId: internalProviderEventId,
          responseOutboxId: responseEffect.outboxId,
        },
      };
    });
  }

  /**
   * Commits only the relationship meaning proposed by the ephemeral worker.
   * The application reopens the exact group, chooses the sole non-household
   * participant, checks the inviter's relationship-local grants, and sends two
   * independent private messages. The source group remains completely silent.
   */
  private async commitFamilyIntroductionProposal(
    internalProviderEventId: string,
    sourceRevisionId: string,
    proposalCandidate: Extract<AppEnvelope, { kind: "linq.family_introduction_proposal" }>["proposal"],
  ): Promise<ProcessReceipt> {
    const displayName = proposalCandidate.displayName.trim();
    if (!displayName || displayName.length > 80) {
      throw new UnauthorizedError("Family introduction name is outside the allowed bounds");
    }
    if (!["steward", "caregiver", "participant"].includes(proposalCandidate.role)) {
      throw new UnauthorizedError("Family introduction role is not supported");
    }

    return this.database.begin(async (transaction) => {
      const events = await transaction<ProviderEventRow[]>`
        select id, provider_event_id, envelope_ciphertext, processing_status
        from provider_events
        where id = ${internalProviderEventId} and provider = 'linq'
        for update
      `;
      const row = events[0];
      if (row?.processing_status !== "processed") {
        throw new StaleAuthorityError("Family introduction source is no longer processed");
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
        (record.invocation?.basis !== "leading_address" && record.invocation?.basis !== "proven_reply") ||
        record.routing.chatKind !== "group" ||
        record.event?.eventType !== "linq.message.received" ||
        !record.routing.senderIdentityId ||
        !record.routing.senderPersonId
      ) {
        throw new UnauthorizedError("Provider event is not an eligible family introduction");
      }
      const expectedSourceObjectId = `conversation_message:${canonicalDigest({
        integrationId: null,
        scope: {
          kind: "conversation_epoch",
          participantEpochId: record.routing.participantEpochId,
        },
        artifactKind: "conversation_message",
        system: "linq",
        remoteObjectId: record.event.message.providerMessageId,
      })}`;

      const sources = await transaction<
        {
          conversation_authority_version: number | string;
          participant_epoch_id: string;
          participant_set_digest: string;
          sender_identity_authority_version: number | string;
          sender_subject_digest: string;
          sender_subject_ciphertext: Buffer;
        }[]
      >`
        select conversation.authority_version as conversation_authority_version,
          epoch.id as participant_epoch_id, epoch.participant_set_digest,
          identity.authority_version as sender_identity_authority_version,
          identity.subject_digest as sender_subject_digest,
          identity.subject_ciphertext as sender_subject_ciphertext
        from conversations conversation
        join conversation_channels channel on channel.conversation_id = conversation.id
          and channel.provider = 'linq' and channel.status = 'active'
          and channel.external_channel_id = ${record.routing.providerChatId}
          and channel.latest_participant_digest = ${record.routing.providerParticipantDigest}
        join participant_epochs epoch on epoch.id = conversation.current_epoch_id
          and epoch.ended_at is null
        join source_revisions source_revision on source_revision.id = ${sourceRevisionId}
          and source_revision.participant_epoch_id = epoch.id
          and source_revision.revoked_at is null
          and source_revision.content_ciphertext is not null
          and source_revision.retention_until > now()
        join source_objects source_object on source_object.id = source_revision.source_object_id
          and source_object.provider = 'linq'
          and source_object.object_kind = 'conversation_message'
          and source_object.external_object_id = ${expectedSourceObjectId}
          and source_object.status = 'active'
          and source_object.latest_revision_number = source_revision.revision_number
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
          and policy.allow_content_processing and policy.allow_direct_responses
        where conversation.id = ${record.routing.conversationId}
          and conversation.kind = 'group' and conversation.status = 'active'
          and epoch.id = ${record.routing.participantEpochId}
          and epoch.participant_set_digest = ${record.routing.appParticipantDigest}
        for update of conversation
      `;
      const source = sources[0];
      if (!source) throw new StaleAuthorityError("Family introduction audience changed");

      const households = await transaction<
        {
          household_id: string;
        }[]
      >`
        select household.id as household_id
        from household_memberships membership
        join households household on household.id = membership.household_id
          and household.status in ('onboarding', 'active')
        join membership_capabilities invitation_grant
          on invitation_grant.membership_id = membership.id
          and invitation_grant.capability = 'membership.invite'
          and invitation_grant.status = 'active'
        where membership.person_id = ${record.routing.senderPersonId}
          and membership.status = 'active'
          and (
            ${proposalCandidate.role} <> 'steward'
            or exists(
              select 1 from membership_capabilities govern_grant
              where govern_grant.membership_id = membership.id
                and govern_grant.capability = 'household.govern'
                and govern_grant.status = 'active'
            )
          )
          and (
            select count(distinct participant.person_id)
            from epoch_participants participant
            where participant.participant_epoch_id = ${record.routing.participantEpochId}
              and participant.person_id <> ${record.routing.senderPersonId}
              and not exists(
                select 1 from household_memberships current_member
                where current_member.household_id = household.id
                  and current_member.person_id = participant.person_id
                  and current_member.status = 'active'
              )
          ) = 1
          and not exists(
            select 1
            from epoch_participants participant
            where participant.participant_epoch_id = ${record.routing.participantEpochId}
              and exists(
                select 1 from household_memberships current_member
                where current_member.household_id = household.id
                  and current_member.person_id = participant.person_id
                  and current_member.status = 'active'
              )
              and (
                participant.registration_status <> 'registered'
                or participant.consented_at is null
              )
          )
        order by household.id
        limit 2
      `;
      const household = households.length === 1 ? households[0] : null;
      if (!household) {
        throw new UnauthorizedError(
          "The sender does not have one unambiguous family relationship for this introduction",
        );
      }
      const currentHouseholds = await transaction<
        {
          household_control_epoch: number | string;
          household_membership_version: number | string;
        }[]
      >`
        select control_epoch as household_control_epoch,
          membership_version as household_membership_version
        from households
        where id = ${household.household_id} and status in ('onboarding', 'active')
        for update
      `;
      const currentHousehold = currentHouseholds[0];
      if (!currentHousehold) {
        throw new StaleAuthorityError("The family changed before the introduction was saved");
      }

      const targets = await transaction<
        {
          person_id: string;
          identity_id: string;
          identity_authority_version: number | string;
          subject_digest: string;
        }[]
      >`
        select participant.person_id, identity.id as identity_id,
          identity.authority_version as identity_authority_version,
          identity.subject_digest
        from epoch_participants participant
        join people person on person.id = participant.person_id
          and person.status in ('provisional', 'registered')
        join person_identities identity on identity.id = participant.person_identity_id
          and identity.person_id = participant.person_id
          and identity.status in ('observed', 'verified')
        where participant.participant_epoch_id = ${record.routing.participantEpochId}
          and participant.person_id <> ${record.routing.senderPersonId}
          and not exists(
            select 1 from household_memberships current_member
            where current_member.household_id = ${household.household_id}
              and current_member.person_id = participant.person_id
              and current_member.status = 'active'
          )
        order by identity.id
        limit 2
      `;
      const target = targets.length === 1 ? targets[0] : null;
      if (!target) {
        throw new StaleAuthorityError("The exact introduced participant is no longer unambiguous");
      }

      const invitationResult = await new HouseholdOnboarding(
        transaction,
        this.secretBox,
      ).inviteCurrentParticipant({
        actorPersonId: record.routing.senderPersonId,
        householdId: household.household_id,
        conversationId: record.routing.conversationId,
        expectedParticipantEpochId: record.routing.participantEpochId,
        expectedParticipantDigest: source.participant_set_digest,
        inviteeIdentityId: target.identity_id,
        inviteePersonId: target.person_id,
        proposedDisplayName: displayName,
        role: proposalCandidate.role,
        sourceRevisionId,
        createdAt: new Date(record.event.message.sentAt),
      });
      const invitationId = invitationResult.invitation.invitationId;
      const duplicate = invitationResult.duplicate;

      const readiness = await transaction<{ ready: boolean }[]>`
        select not exists(
          select 1 from invitation_approvals approval
          where approval.invitation_id = invitation.id and approval.approved_at is null
        ) as ready
        from invitations invitation where invitation.id = ${invitationId}
      `;
      const ready = readiness[0]?.ready === true;
      if (ready) await this.queueHouseholdInvitationMessage(transaction, invitationId);

      const senderRecipient = this.secretBox
        .decrypt(
          JSON.parse(source.sender_subject_ciphertext.toString("utf8")),
          `identity-subject:${record.routing.senderIdentityId}`,
        )
        .toString("utf8");
      if (!isOutboundIdentitySubject(senderRecipient)) {
        throw new UnauthorizedError("Exact sender does not have a safe private Florence route");
      }
      const responseText = duplicate
        ? `There’s already a private family invitation pending for this person. I won’t send another one. I’ll stay silent in the group unless that exact invitation is accepted and everyone there belongs to the family.`
        : ready
          ? `I privately asked ${displayName} to confirm the invitation. I’ll stay silent in the group until they do.`
          : `I recorded ${displayName} as a proposed ${relationshipLabel(proposalCandidate.role)}. The other current family stewards need to approve before I contact them privately.`;
      const responseEffect = await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
        actorPersonId: record.routing.senderPersonId,
        person: {
          id: record.routing.senderPersonId,
          controlEpoch: (await personFence(transaction, record.routing.senderPersonId)).person.controlEpoch,
        },
        household: {
          id: household.household_id,
          controlEpoch: Number(currentHousehold.household_control_epoch),
        },
        recipientIdentity: {
          id: record.routing.senderIdentityId,
          authorityVersion: Number(source.sender_identity_authority_version),
          subjectDigest: source.sender_subject_digest,
        },
        sourceConversation: {
          id: record.routing.conversationId,
          authorityVersion: Number(source.conversation_authority_version),
          participantEpochId: source.participant_epoch_id,
          participantSetDigest: source.participant_set_digest,
        },
        evidenceSourceRevisionIds: [sourceRevisionId],
        effectKind: "linq.message",
        idempotencyKey: `family-introduction-private-ack:${internalProviderEventId}`,
        data: { invitationId, responseDigest: sha256Hex(responseText) },
        policy: { exactPrivateRecipient: true, sourceGroupSilent: true },
        target: {
          recipient: senderRecipient,
          personId: record.routing.senderPersonId,
          recipientIdentityId: record.routing.senderIdentityId,
        },
        payload: { recipient: senderRecipient, text: responseText },
        reasonCodes: [
          "registered_exact_group_sender",
          "explicit_family_introduction",
          "private_response_only",
        ],
        authorizationExpiresAt: new Date(Date.now() + 24 * 60 * 60_000),
      });

      return {
        accepted: true,
        duplicate,
        disposition: ready
          ? duplicate
            ? "family_invitation_already_pending"
            : "family_invitation_sent_privately"
          : "family_invitation_awaiting_stewards",
        ids: {
          providerEventId: internalProviderEventId,
          invitationId,
          householdId: household.household_id,
          responseOutboxId: responseEffect.outboxId,
        },
      };
    });
  }

  /**
   * Returns the one logical Florence message most recently delivered before
   * this inbound response. Provider receipt time, rather than queue time,
   * determines the order. Equal-time effects are ambiguous and fail closed.
   */
  private async latestRelevantPrivateEffect(
    transaction: Transaction,
    record: StoredLinqEvent,
    identityId: string,
  ): Promise<LatestRelevantPrivateEffect | null> {
    if (record.routing.chatKind !== "direct" || !record.messageOccurredAt) return null;
    const responseOccurredAt = new Date(record.messageOccurredAt);
    if (!Number.isFinite(responseOccurredAt.getTime())) return null;
    const enrollmentPrefix = `enrollment:${record.routing.providerChatId}:%`;
    const effects = await transaction<
      {
        logical_effect_id: string;
        logical_idempotency_key: string;
        invitation_id: string | null;
      }[]
    >`
      with logical_deliveries as (
        select coalesce(effect.redrive_root_id, effect.id) as logical_effect_id,
          coalesce(root_effect.idempotency_key, effect.idempotency_key)
            as logical_idempotency_key,
          coalesce(root_effect.invitation_id, effect.invitation_id) as invitation_id,
          min(receipt.occurred_at) as delivered_at
        from outbox effect
        left join outbox root_effect on root_effect.id = effect.redrive_root_id
        join effect_receipts receipt on receipt.outbox_id = effect.id
          and receipt.status = 'confirmed'
          and receipt.occurred_at <= ${responseOccurredAt}
        left join invitations effect_invitation
          on effect_invitation.id = coalesce(root_effect.invitation_id, effect.invitation_id)
        where effect.effect_kind = 'linq.message'
          and (
            coalesce(root_effect.conversation_id, effect.conversation_id) =
              ${record.routing.conversationId}
            or coalesce(root_effect.idempotency_key, effect.idempotency_key)
              like ${enrollmentPrefix}
            or effect_invitation.invitee_identity_id = ${identityId}
          )
        group by coalesce(effect.redrive_root_id, effect.id),
          coalesce(root_effect.idempotency_key, effect.idempotency_key),
          coalesce(root_effect.invitation_id, effect.invitation_id)
      )
      select logical_effect_id, logical_idempotency_key, invitation_id
      from logical_deliveries
      where delivered_at = (select max(delivered_at) from logical_deliveries)
      order by logical_effect_id
      limit 2
    `;
    if (effects.length !== 1 || !effects[0]) return null;
    const effect = effects[0];
    if (
      effect.invitation_id &&
      (effect.logical_idempotency_key === `household-enrollment-invitation:${effect.invitation_id}` ||
        effect.logical_idempotency_key === `household-invitation:${effect.invitation_id}`)
    ) {
      return { kind: "family_invitation", invitationId: effect.invitation_id };
    }
    const enrollmentKeyPrefix = `enrollment:${record.routing.providerChatId}:`;
    const enrollmentKeySuffix = effect.logical_idempotency_key.startsWith(enrollmentKeyPrefix)
      ? effect.logical_idempotency_key.slice(enrollmentKeyPrefix.length)
      : null;
    return enrollmentKeySuffix && /^[a-f0-9]{64}$/u.test(enrollmentKeySuffix)
      ? { kind: "enrollment", invitationId: null }
      : { kind: "other", invitationId: null };
  }

  private async latestPrivateOnboardingPromptWasDelivered(
    transaction: Transaction,
    record: StoredLinqEvent,
  ): Promise<boolean> {
    if (record.routing.chatKind !== "direct" || !record.messageOccurredAt) return false;
    const responseOccurredAt = new Date(record.messageOccurredAt);
    if (!Number.isFinite(responseOccurredAt.getTime())) return false;
    const effects = await transaction<{ readonly idempotency_key: string }[]>`
      with delivered as (
        select coalesce(root_effect.idempotency_key, effect.idempotency_key) as idempotency_key,
          coalesce(effect.redrive_root_id, effect.id) as logical_effect_id,
          min(receipt.occurred_at) as delivered_at
        from outbox effect
        left join outbox root_effect on root_effect.id = effect.redrive_root_id
        join effect_receipts receipt on receipt.outbox_id = effect.id
          and receipt.status = 'confirmed' and receipt.occurred_at <= ${responseOccurredAt}
        where effect.effect_kind = 'linq.message'
          and coalesce(root_effect.conversation_id, effect.conversation_id) =
            ${record.routing.conversationId}
        group by coalesce(root_effect.idempotency_key, effect.idempotency_key),
          coalesce(effect.redrive_root_id, effect.id)
      )
      select idempotency_key from delivered
      where delivered_at = (select max(delivered_at) from delivered)
      order by logical_effect_id
      limit 2
    `;
    if (effects.length !== 1 || !effects[0]) return false;
    return (
      effects[0].idempotency_key.startsWith(`parent-onboarding:${record.routing.senderPersonId}:`) ||
      effects[0].idempotency_key.startsWith(`onboarding-reminder:${record.routing.senderPersonId}:stage:`)
    );
  }

  private async handleEnrollment(transaction: Transaction, record: StoredLinqEvent): Promise<string> {
    const identityId = record.routing.senderIdentityId;
    const personId = record.routing.senderPersonId;
    if (!identityId || !personId) return "ignored";
    const latestEffect = await this.latestRelevantPrivateEffect(transaction, record, identityId);
    if (record.enrollmentAction === "decline_invitation") {
      const declined =
        latestEffect?.kind === "family_invitation" && latestEffect.invitationId
          ? await this.declineExactPendingFamilyInvitation(
              transaction,
              record,
              identityId,
              false,
              latestEffect.invitationId,
            )
          : null;
      if (declined) return declined;
    }
    if (
      record.enrollmentAction !== "consent" ||
      (latestEffect?.kind !== "enrollment" && latestEffect?.kind !== "family_invitation")
    ) {
      await this.queueSystemEnrollmentMessage(
        transaction,
        record,
        `Hi—I’m Florence, a family Chief of Staff. I can read what you send me and help your family keep logistics covered. When I’m added to a group, new messages may be retained as private context for that exact participant lineup, but I stay silent there unless every current person is a registered member of the same Florence family and their settings permit writing. Permitted raw context is kept for up to ${this.config.defaults.rawSourceRetentionDays} days; you can change controls or delete data later. Privacy: ${this.config.publicBaseUrl}/privacy\n\nWould you like to create your private Florence account? Reply yes to agree. You can text STOP any time.`,
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
    const invitationAcceptance =
      latestEffect.kind === "family_invitation" && latestEffect.invitationId
        ? await this.acceptExactPendingFamilyInvitation(
            transaction,
            record,
            personId,
            identityId,
            latestEffect.invitationId,
          )
        : null;
    if (invitationAcceptance) return invitationAcceptance;
    const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
      record.routing.conversationId,
    );
    const knownName = decryptPersonName(this.secretBox, personId, current.display_name_ciphertext);
    const onboardingOffered = knownName
      ? await this.queueParentOnboardingOffer(transaction, personId, "registration", record)
      : false;
    if (!onboardingOffered) {
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
   * A natural private decline applies only to the single invitation Florence
   * most recently delivered in this exact DM. It does not stop Florence,
   * unregister the person, or change any broader privacy setting.
   */
  private async declineExactPendingFamilyInvitation(
    transaction: Transaction,
    record: StoredLinqEvent,
    identityId: string,
    registeredPerson: boolean,
    invitationId: string,
  ): Promise<string | null> {
    if (record.routing.chatKind !== "direct") return null;
    const invitations = await transaction<{ invitation_id: string }[]>`
      select invitation.id as invitation_id
      from invitations invitation
      where invitation.id = ${invitationId}
        and invitation.invitee_identity_id = ${identityId}
        and invitation.status = 'pending'
      for update of invitation
    `;
    if (invitations.length !== 1 || !invitations[0]) return null;
    await transaction`
      update invitations set status = 'declined', updated_at = now()
      where id = ${invitationId} and status = 'pending'
    `;
    const responseText = registeredPerson
      ? "Got it—I declined that family invitation. Your Florence account and settings are unchanged."
      : "Got it—I declined that family invitation. I didn’t create or stop a Florence account.";
    if (registeredPerson) {
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        record.routing.conversationId,
      );
      await this.queueAuthorizedConversationMessage(
        transaction,
        record,
        snapshot,
        responseText,
        "direct_response",
        "family_invitation_declined",
        null,
        new Date(Date.now() + 24 * 60 * 60_000),
        `family-invitation-declined:${invitationId}`,
      );
    } else {
      await this.queueSystemEnrollmentMessage(
        transaction,
        record,
        responseText,
        `enrollment:${record.routing.providerChatId}:family-invitation-declined:${invitationId}`,
      );
    }
    return "family_invitation_declined";
  }

  /**
   * A delivered invitation can become invalid while its private prompt is
   * still visible. An affirmative still completes registration, but it cannot
   * revive stale household or group authority. Explain that outcome instead
   * of silently falling through to ordinary onboarding.
   */
  private async rejectStaleDeliveredFamilyInvitation(
    transaction: Transaction,
    record: StoredLinqEvent,
    personId: string,
    identityId: string,
    invitationId: string,
  ): Promise<string | null> {
    if (record.routing.chatKind !== "direct") return null;
    const locations = await transaction<
      {
        household_id: string;
        source_conversation_id: string;
        source_revision_id: string | null;
      }[]
    >`
      select invitation.household_id, invitation.source_conversation_id,
        invitation.source_revision_id
      from invitations invitation
      where invitation.id = ${invitationId}
        and invitation.invitee_identity_id = ${identityId}
    `;
    const location = locations[0];
    if (!location) {
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        record.routing.conversationId,
      );
      await this.queueAuthorizedConversationMessage(
        transaction,
        record,
        snapshot,
        "Your Florence account is active, but that family invitation is no longer available. Ask the person who invited you to introduce you again in the current family group.",
        "direct_response",
        "family_invitation_stale",
        null,
        new Date(Date.now() + 24 * 60 * 60_000),
        `family-invitation-stale:${sha256Hex(invitationId)}`,
      );
      return "family_invitation_stale";
    }
    await transaction`
      select id from conversations
      where id = ${location.source_conversation_id}
      for update
    `;
    await transaction`
      select id from households
      where id = ${location.household_id}
      for update
    `;
    const lockedInvitations = await transaction<{ readonly id: string }[]>`
      select id from invitations
      where id = ${invitationId}
        and household_id = ${location.household_id}
        and source_conversation_id = ${location.source_conversation_id}
      for update
    `;
    if (!lockedInvitations[0]) {
      throw new StaleAuthorityError("The family invitation changed before it could be accepted");
    }
    if (location.source_revision_id) {
      await transaction`
        select id from source_revisions
        where id = ${location.source_revision_id}
        for update
      `;
    }
    const invitations = await transaction<
      {
        invitation_id: string;
        status: "pending" | "revoked" | "expired";
        expires_at: Date;
        inviter_person_id: string | null;
        inviter_name_ciphertext: Buffer | null;
      }[]
    >`
      select invitation.id as invitation_id, invitation.status,
        invitation.expires_at,
        inviter_membership.person_id as inviter_person_id,
        inviter.display_name_ciphertext as inviter_name_ciphertext
      from invitations invitation
      left join household_memberships inviter_membership
        on inviter_membership.id = invitation.invited_by_membership_id
      left join people inviter on inviter.id = inviter_membership.person_id
      where invitation.id = ${invitationId}
        and invitation.invitee_identity_id = ${identityId}
        and invitation.status in ('pending', 'revoked', 'expired')
        and (
          invitation.status <> 'pending'
          or invitation.expires_at <= now()
          or invitation.proposed_display_name_ciphertext is null
          or not exists(
            select 1
            from households current_household
            join household_memberships current_inviter_membership
              on current_inviter_membership.id = invitation.invited_by_membership_id
              and current_inviter_membership.household_id = current_household.id
              and current_inviter_membership.status = 'active'
            join people current_inviter on current_inviter.id = current_inviter_membership.person_id
              and current_inviter.status = 'registered'
            join person_identities current_invitee_identity
              on current_invitee_identity.id = invitation.invitee_identity_id
              and current_invitee_identity.id = ${identityId}
              and current_invitee_identity.person_id = ${personId}
              and current_invitee_identity.status = 'verified'
            join conversations source_conversation
              on source_conversation.id = invitation.source_conversation_id
              and source_conversation.kind = 'group' and source_conversation.status = 'active'
            join participant_epochs source_epoch on source_epoch.id = source_conversation.current_epoch_id
              and source_epoch.id = invitation.source_participant_epoch_id
              and source_epoch.ended_at is null
              and source_epoch.participant_set_digest = invitation.source_participant_digest
            join epoch_participants exact_invitee
              on exact_invitee.participant_epoch_id = source_epoch.id
              and exact_invitee.person_identity_id = current_invitee_identity.id
              and exact_invitee.person_id = current_invitee_identity.person_id
            where current_household.id = invitation.household_id
              and current_household.status in ('onboarding', 'active')
              and current_household.membership_version = invitation.household_membership_version
              and (
                invitation.source_revision_id is null
                or exists(
                  select 1
                  from source_revisions source_revision
                  join source_objects source_object on source_object.id = source_revision.source_object_id
                    and source_object.status = 'active'
                    and source_object.latest_revision_number = source_revision.revision_number
                  where source_revision.id = invitation.source_revision_id
                    and source_revision.participant_epoch_id = source_epoch.id
                    and source_revision.revoked_at is null
                    and source_revision.content_ciphertext is not null
                    and source_revision.retention_until > now()
                )
              )
              and not exists(
                select 1 from invitation_approvals approval
                where approval.invitation_id = invitation.id and approval.approved_at is null
              )
          )
        )
      order by invitation.created_at desc, invitation.id desc
      limit 2
      for update of invitation
    `;
    if (invitations.length === 0) return null;
    const invitationIds = invitations.map((invitation) => invitation.invitation_id);
    await transaction`
      update invitations
      set status = case when expires_at <= now() then 'expired' else 'revoked' end,
        updated_at = now()
      where id = any(${transaction.array(invitationIds)}::uuid[]) and status = 'pending'
    `;
    const invitation = invitations.length === 1 ? invitations[0] : null;
    const inviterName =
      invitation?.inviter_person_id && invitation.inviter_name_ciphertext
        ? decryptPersonName(this.secretBox, invitation.inviter_person_id, invitation.inviter_name_ciphertext)
        : null;
    const reintroduction = inviterName
      ? `Ask ${inviterName} to introduce you again in the current family group.`
      : "Ask the person who invited you to introduce you again in the current family group.";
    const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
      record.routing.conversationId,
    );
    await this.queueAuthorizedConversationMessage(
      transaction,
      record,
      snapshot,
      `Your Florence account is active, but that family invitation is no longer current because the family or group changed. ${reintroduction}`,
      "direct_response",
      "family_invitation_stale",
      null,
      new Date(Date.now() + 24 * 60 * 60_000),
      `family-invitation-stale:${sha256Hex(invitationIds.sort().join(":"))}`,
    );
    return "family_invitation_stale";
  }

  /** The caller locks the source conversation before this second, authoritative read. */
  private async familyInvitationIsCurrentForPerson(
    transaction: Transaction,
    invitationId: string,
    personId: string,
  ): Promise<boolean> {
    const rows = await transaction<{ current: boolean }[]>`
      select exists(
        select 1
        from invitations invitation
        join households household on household.id = invitation.household_id
          and household.status in ('onboarding', 'active')
          and household.membership_version = invitation.household_membership_version
        join household_memberships inviter_membership
          on inviter_membership.id = invitation.invited_by_membership_id
          and inviter_membership.household_id = household.id
          and inviter_membership.status = 'active'
        join people inviter on inviter.id = inviter_membership.person_id
          and inviter.status = 'registered'
        join person_identities invitee_identity on invitee_identity.id = invitation.invitee_identity_id
          and invitee_identity.person_id = ${personId}
          and invitee_identity.status = 'verified'
        join conversations source_conversation on source_conversation.id = invitation.source_conversation_id
          and source_conversation.kind = 'group' and source_conversation.status = 'active'
        join participant_epochs source_epoch on source_epoch.id = source_conversation.current_epoch_id
          and source_epoch.id = invitation.source_participant_epoch_id
          and source_epoch.ended_at is null
          and source_epoch.participant_set_digest = invitation.source_participant_digest
        join epoch_participants exact_invitee on exact_invitee.participant_epoch_id = source_epoch.id
          and exact_invitee.person_identity_id = invitee_identity.id
          and exact_invitee.person_id = invitee_identity.person_id
        where invitation.id = ${invitationId}
          and invitation.status = 'pending' and invitation.expires_at > now()
          and invitation.proposed_display_name_ciphertext is not null
          and (
            invitation.source_revision_id is null
            or exists(
              select 1
              from source_revisions source_revision
              join source_objects source_object on source_object.id = source_revision.source_object_id
                and source_object.status = 'active'
                and source_object.latest_revision_number = source_revision.revision_number
              where source_revision.id = invitation.source_revision_id
                and source_revision.participant_epoch_id = source_epoch.id
                and source_revision.revoked_at is null
                and source_revision.content_ciphertext is not null
                and source_revision.retention_until > now()
            )
          )
          and not exists(
            select 1 from invitation_approvals approval
            where approval.invitation_id = invitation.id and approval.approved_at is null
          )
      ) as current
    `;
    return rows[0]?.current === true;
  }

  /**
   * One exact private yes both confirms the observed identity and accepts the
   * one current relationship invitation. It never guesses between invitations,
   * and it activates only the invitation's still-current source group.
   */
  private async acceptExactPendingFamilyInvitation(
    transaction: Transaction,
    record: StoredLinqEvent,
    personId: string,
    identityId: string,
    invitationId: string,
  ): Promise<string | null> {
    if (record.routing.chatKind !== "direct") return null;
    const invitations = await transaction<
      {
        invitation_id: string;
        household_id: string;
        source_conversation_id: string;
        proposed_display_name_ciphertext: Buffer;
        requested_role: "steward" | "caregiver" | "participant";
        inviter_person_id: string;
        inviter_name_ciphertext: Buffer | null;
      }[]
    >`
      select invitation.id as invitation_id, invitation.household_id,
        invitation.source_conversation_id,
        invitation.proposed_display_name_ciphertext,
        invitation.requested_role,
        inviter_membership.person_id as inviter_person_id,
        inviter.display_name_ciphertext as inviter_name_ciphertext
      from invitations invitation
      join households household on household.id = invitation.household_id
        and household.status in ('onboarding', 'active')
        and household.membership_version = invitation.household_membership_version
      join household_memberships inviter_membership
        on inviter_membership.id = invitation.invited_by_membership_id
        and inviter_membership.status = 'active'
      join people inviter on inviter.id = inviter_membership.person_id
        and inviter.status = 'registered'
      join person_identities invitee_identity on invitee_identity.id = invitation.invitee_identity_id
        and invitee_identity.person_id = ${personId}
        and invitee_identity.id = ${identityId}
        and invitee_identity.status = 'verified'
      join conversations source_conversation on source_conversation.id = invitation.source_conversation_id
        and source_conversation.kind = 'group' and source_conversation.status = 'active'
      join participant_epochs source_epoch on source_epoch.id = source_conversation.current_epoch_id
        and source_epoch.id = invitation.source_participant_epoch_id
        and source_epoch.ended_at is null
        and source_epoch.participant_set_digest = invitation.source_participant_digest
      join epoch_participants exact_invitee on exact_invitee.participant_epoch_id = source_epoch.id
        and exact_invitee.person_identity_id = invitee_identity.id
        and exact_invitee.person_id = invitee_identity.person_id
      where invitation.id = ${invitationId}
        and invitation.status = 'pending' and invitation.expires_at > now()
        and invitation.proposed_display_name_ciphertext is not null
        and not exists(
          select 1 from invitation_approvals approval
          where approval.invitation_id = invitation.id and approval.approved_at is null
        )
      order by invitation.created_at, invitation.id
      limit 2
    `;
    if (invitations.length === 0) {
      return this.rejectStaleDeliveredFamilyInvitation(
        transaction,
        record,
        personId,
        identityId,
        invitationId,
      );
    }
    const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
      record.routing.conversationId,
    );
    if (invitations.length !== 1 || !invitations[0]) {
      await this.queueAuthorizedConversationMessage(
        transaction,
        record,
        snapshot,
        "I found more than one current family invitation, so I didn’t guess. Open your private settings to choose the right one.",
        "direct_response",
        "family_invitation_choice_required",
      );
      return "family_invitation_choice_required";
    }
    const invitation = invitations[0];
    const acceptedAt = new Date();
    if (!(await this.familyInvitationIsCurrentForPerson(transaction, invitation.invitation_id, personId))) {
      const staleDisposition = await this.rejectStaleDeliveredFamilyInvitation(
        transaction,
        record,
        personId,
        identityId,
        invitationId,
      );
      if (staleDisposition) return staleDisposition;
      throw new StaleAuthorityError("The family invitation changed before it could be accepted");
    }
    const proposedDisplayName = this.secretBox
      .decrypt(
        JSON.parse(invitation.proposed_display_name_ciphertext.toString("utf8")),
        `invitation-proposed-display-name:${invitation.invitation_id}`,
      )
      .toString("utf8");
    const people = await transaction<{ display_name_ciphertext: Buffer | null }[]>`
      select display_name_ciphertext from people
      where id = ${personId} and status = 'registered'
      for update
    `;
    if (!people[0]) throw new StaleAuthorityError("Invitation identity is no longer registered");
    if (people[0].display_name_ciphertext === null) {
      const encryptedName = this.secretBox.encrypt(proposedDisplayName, `person-display-name:${personId}`);
      await transaction`
        update people
        set display_name_ciphertext = ${Buffer.from(JSON.stringify(encryptedName), "utf8")},
          display_name_key_version = ${encryptedName.kid}, onboarding_step = 'complete',
          updated_at = ${acceptedAt}
        where id = ${personId} and status = 'registered'
      `;
    } else {
      await transaction`
        update people set onboarding_step = 'complete', updated_at = ${acceptedAt}
        where id = ${personId} and status = 'registered'
      `;
    }

    const membership = await new HouseholdOnboarding(transaction, this.secretBox).acceptInvitation({
      actorPersonId: personId,
      invitationId: invitation.invitation_id,
      acceptedAt,
    });
    await transaction`
      update households set status = 'active', updated_at = ${acceptedAt}
      where id = ${membership.householdId} and status = 'onboarding'
    `;
    await this.selectAcceptedOnboardingHousehold(transaction, personId, membership.householdId, acceptedAt);
    const familyAuthority = await new FamilyGroupAuthority(transaction).reconcile({
      conversationId: invitation.source_conversation_id,
      occurredAt: acceptedAt,
    });
    const onboardingOffered = await this.queueParentOnboardingOffer(
      transaction,
      personId,
      "invitation_accepted",
      record,
      invitation.invitation_id,
    );
    if (!onboardingOffered) {
      const inviterName =
        decryptPersonName(this.secretBox, invitation.inviter_person_id, invitation.inviter_name_ciphertext) ??
        "Your family member";
      await this.queueAuthorizedConversationMessage(
        transaction,
        record,
        snapshot,
        `You’re all set. You joined ${inviterName}’s Florence family as ${relationshipLabel(invitation.requested_role)}. I’ll reuse the family details already there instead of asking you to repeat them.`,
        "direct_response",
        "family_invitation_accepted",
        null,
        new Date(Date.now() + 24 * 60 * 60_000),
        `family-invitation-accepted:${invitation.invitation_id}`,
      );
    }
    if (familyAuthority.activatedNow && familyAuthority.ruleId && familyAuthority.householdId) {
      await this.queueFamilyGroupActivationAcknowledgement(transaction, {
        invitationId: invitation.invitation_id,
        actorPersonId: personId,
        conversationId: invitation.source_conversation_id,
        householdId: familyAuthority.householdId,
        householdControlEpoch: familyAuthority.householdControlEpoch,
        participantEpochId: familyAuthority.participantEpochId,
        participantSetDigest: familyAuthority.participantSetDigest,
        ruleId: familyAuthority.ruleId,
        snapshot: familyAuthority.snapshot,
      });
    }
    return "household_invitation_accepted";
  }

  private async queueFamilyGroupActivationAcknowledgement(
    transaction: Transaction,
    input: {
      readonly invitationId: string;
      readonly actorPersonId: string;
      readonly conversationId: string;
      readonly householdId: string;
      readonly householdControlEpoch: number | null;
      readonly participantEpochId: string;
      readonly participantSetDigest: string;
      readonly ruleId: string;
      readonly snapshot: ConversationAuthoritySnapshot;
    },
  ): Promise<void> {
    if (input.householdControlEpoch === null) return;
    const audience = await transaction<
      {
        identity_id: string;
        external_channel_id: string;
        latest_participant_digest: string;
      }[]
    >`
      select participant.person_identity_id as identity_id,
        channel.external_channel_id, channel.latest_participant_digest
      from conversations conversation
      join conversation_channels channel on channel.conversation_id = conversation.id
        and channel.provider = 'linq' and channel.status = 'active'
        and channel.latest_participant_digest is not null
      join participant_epochs epoch on epoch.id = conversation.current_epoch_id
        and epoch.id = ${input.participantEpochId} and epoch.ended_at is null
        and epoch.participant_set_digest = ${input.participantSetDigest}
      join epoch_participants participant on participant.participant_epoch_id = epoch.id
      where conversation.id = ${input.conversationId}
        and conversation.household_id = ${input.householdId}
        and conversation.kind = 'group' and conversation.status = 'active'
      order by participant.person_identity_id
    `;
    const first = audience[0];
    if (!first || audience.length !== input.snapshot.participants.length) return;
    const authorization = await new PostgresConversationAuthority(transaction).authorizeSend({
      conversationId: input.conversationId,
      expectedParticipantEpochId: input.participantEpochId,
      expectedParticipantSetDigest: input.participantSetDigest,
      liveParticipantIdentityIds: audience.map((participant) => participant.identity_id),
      sendKind: "transactional",
      operation: "family_group_activation",
      ruleId: input.ruleId,
    });
    if (!authorization.allowed) return;
    const text = "I’m all set. I can help coordinate family logistics here now.";
    await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
      actorPersonId: input.actorPersonId,
      ...(await personFence(transaction, input.actorPersonId)),
      household: { id: input.householdId, controlEpoch: input.householdControlEpoch },
      conversation: {
        id: input.conversationId,
        authorityVersion: authorization.authorityVersion,
      },
      participantEpochId: input.participantEpochId,
      expectedParticipantDigest: input.participantSetDigest,
      effectKind: "linq.message",
      idempotencyKey: `group-activation:${input.invitationId}:${input.participantEpochId}`,
      data: { invitationId: input.invitationId, textDigest: sha256Hex(text) },
      policy: {
        exactCurrentAudience: true,
        standingHouseholdMembership: true,
        operation: "family_group_activation",
      },
      target: {
        providerChatId: first.external_channel_id,
        participantEpochId: input.participantEpochId,
      },
      payload: {
        providerChatId: first.external_channel_id,
        expectedProviderParticipantDigest: first.latest_participant_digest,
        text,
      },
      reasonCodes: [
        "exact_current_group_epoch",
        "all_current_participants_active_household_members",
        "standing_household_membership",
      ],
      authorizationExpiresAt: new Date(Date.now() + 5 * 60_000),
    });
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
      const affectedConversations = await transaction<{ conversation_id: string }[]>`
        select conversation.id as conversation_id
        from conversations conversation
        join participant_epochs epoch on epoch.id = conversation.current_epoch_id
          and epoch.ended_at is null
        join epoch_participants participant on participant.participant_epoch_id = epoch.id
        where participant.person_id = ${personId}
        order by conversation.id
        for update of conversation
      `;
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
    if (
      record.routing.chatKind === "direct" &&
      person &&
      record.routing.senderIdentityId &&
      isExplicitFamilyInvitationDecline(text)
    ) {
      const latestEffect = await this.latestRelevantPrivateEffect(
        transaction,
        record,
        record.routing.senderIdentityId,
      );
      const declined =
        latestEffect?.kind === "family_invitation" && latestEffect.invitationId
          ? await this.declineExactPendingFamilyInvitation(
              transaction,
              record,
              record.routing.senderIdentityId,
              true,
              latestEffect.invitationId,
            )
          : null;
      if (declined) return declined;
    }
    if (
      record.routing.chatKind === "direct" &&
      person &&
      record.routing.senderPersonId &&
      record.routing.senderIdentityId &&
      isExplicitEnrollmentConsent(text)
    ) {
      const latestEffect = await this.latestRelevantPrivateEffect(
        transaction,
        record,
        record.routing.senderIdentityId,
      );
      const invitationAcceptance =
        latestEffect?.kind === "family_invitation" && latestEffect.invitationId
          ? await this.acceptExactPendingFamilyInvitation(
              transaction,
              record,
              record.routing.senderPersonId,
              record.routing.senderIdentityId,
              latestEffect.invitationId,
            )
          : null;
      if (invitationAcceptance) return invitationAcceptance;
    }
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
      const onboardingOffered = await this.queueParentOnboardingOffer(
        transaction,
        personId,
        "household_resolved",
        record,
      );
      if (!onboardingOffered) {
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
      (/^(?:stop reminding me (?:about|to finish) (?:setup|onboarding)|don['’]t remind me (?:about|to finish) (?:setup|onboarding))$/u.test(
        normalizedCommand,
      ) ||
        (/^(?:not now|later)$/u.test(normalizedCommand) &&
          (await this.latestPrivateOnboardingPromptWasDelivered(transaction, record))))
    ) {
      const personId = record.routing.senderPersonId;
      if (!personId) return "ignored";
      const onboarding = new PostgresFamilyOnboarding(this.secretBox);
      const projection = await onboarding.project(transaction, {
        actorPersonId: personId,
        personId,
      });
      await onboarding.suppressReminders(transaction, {
        actorPersonId: personId,
        personId,
        expectedPersonOnboardingVersion: projection.profile.onboardingVersion,
        suppressedAt: new Date(),
      });
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        record.routing.conversationId,
      );
      await this.queueAuthorizedConversationMessage(
        transaction,
        record,
        snapshot,
        "No problem—I saved your setup and won’t remind you again. Ask me to continue setup whenever you’re ready.",
        "direct_response",
        "onboarding_reminders_suppressed",
        null,
        new Date(Date.now() + 24 * 60 * 60_000),
      );
      return "onboarding_reminders_suppressed";
    }
    if (
      record.routing.chatKind === "direct" &&
      person &&
      /^(?:continue|finish|resume) (?:my )?(?:setup|onboarding)$|^set up florence$/u.test(normalizedCommand)
    ) {
      const personId = record.routing.senderPersonId;
      if (!personId) return "ignored";
      const resumeKey =
        record.event?.providerEventId ?? sha256Hex(`${record.messageOccurredAt ?? "unknown"}:${text}`);
      const onboarding = new PostgresFamilyOnboarding(this.secretBox);
      const projection = await onboarding.project(transaction, {
        actorPersonId: personId,
        personId,
      });
      if (projection.nextStep.kind === "complete") {
        const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
          record.routing.conversationId,
        );
        await this.queueAuthorizedConversationMessage(
          transaction,
          record,
          snapshot,
          "You’re already set up—what can I help with?",
          "direct_response",
          "onboarding_already_complete",
          null,
          new Date(Date.now() + 24 * 60 * 60_000),
          `onboarding-already-complete:${personId}:${resumeKey}`,
        );
        return "onboarding_already_complete_replied";
      }
      await this.noteOnboardingProgress(transaction, personId, new Date());
      const offered = await this.queueParentOnboardingOffer(
        transaction,
        personId,
        "resume",
        record,
        resumeKey,
      );
      return offered ? "onboarding_resumed" : "onboarding_resume_unavailable";
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
        expiresInSeconds:
          normalizedCommand === "connect google" ? GOOGLE_CONNECT_HANDOFF_TTL_SECONDS : 10 * 60,
      });
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        record.routing.conversationId,
      );
      await this.queueAuthorizedConversationMessage(
        transaction,
        record,
        snapshot,
        normalizedCommand === "connect google"
          ? `Here’s a fresh link to connect your personal Gmail and Calendar: ${this.config.publicBaseUrl}/handoff/${handoff.token}\n\nIt’s valid for 30 minutes.`
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
        const timeZone = await this.requireConfirmedPersonTimeZone(transaction, personId);
        await new PostgresIdentityRelationships(transaction).createHousehold({
          founderPersonId: personId,
          timezone: timeZone,
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
      const onboardingOffered = await this.queueParentOnboardingOffer(
        transaction,
        personId,
        "registration",
        record,
      );
      if (!onboardingOffered) {
        const handoff = await new PostgresWebAuth(
          transaction,
          this.secretBox,
          this.config.security.tokenKey,
        ).createHandoff({
          personId,
          privateIdentityId: identityId,
          privateConversationId: record.routing.conversationId,
          purpose: "web_sign_in",
          context: { onboarding: true, returnPath: "/onboarding" },
          expiresInSeconds: 10 * 60,
        });
        await this.queueAuthorizedConversationMessage(
          transaction,
          record,
          snapshot,
          `Done—your private family space is ready. Continue the family setup I saved for you here: ${this.config.publicBaseUrl}/handoff/${handoff.token}\n\nIf the link expires, ask me to continue setup.`,
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
    // Every admitted conversational job must reach a durable reply or an
    // explicit silence reason. Authority may revoke it; latency may not expire it.
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
    idempotencyKey = `enrollment:${record.routing.providerChatId}:${sha256Hex(
      `${record.messageOccurredAt ?? "no-message-time"}:${text}`,
    )}`,
  ): Promise<void> {
    await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
      effectKind: "linq.message",
      idempotencyKey,
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
    idempotencyKey = `linq:${operation}:${record.routing.conversationId}:${record.event?.providerEventId ?? sha256Hex(`${record.messageOccurredAt ?? "no-message-time"}:${text}`)}`,
    evidenceSourceRevisionIds: readonly string[] = [],
    responseHousehold: { readonly id: string; readonly controlEpoch: number } | null = null,
  ): Promise<{ readonly outboxId: string; readonly created: boolean }> {
    if (!snapshot.participantEpochId || !snapshot.participantSetDigest)
      throw new StaleAuthorityError("Conversation has no live epoch");
    const authority = await new PostgresConversationAuthority(transaction).authorizeSend({
      conversationId: record.routing.conversationId,
      expectedParticipantEpochId: snapshot.participantEpochId,
      expectedParticipantSetDigest: snapshot.participantSetDigest,
      liveParticipantIdentityIds: [...record.routing.liveIdentityIds],
      sendKind,
      operation,
      ruleId:
        ruleId ??
        (snapshot.conversationKind === "group"
          ? (snapshot.rules.find(
              (candidate) =>
                candidate.active &&
                candidate.participantSetDigest === snapshot.participantSetDigest &&
                candidate.allowedOperations.includes(operation),
            )?.ruleId ?? null)
          : null),
    });
    if (!authority.allowed) {
      throw new StaleAuthorityError("Conversation response is no longer authorized");
    }
    const household = await transaction<{ id: string; control_epoch: number | string }[]>`
      select household.id, household.control_epoch
      from conversations conversation join households household on household.id = conversation.household_id
      where conversation.id = ${record.routing.conversationId}
    `;
    const boundHousehold = household[0]
      ? { id: household[0].id, controlEpoch: Number(household[0].control_epoch) }
      : null;
    if (
      responseHousehold &&
      boundHousehold &&
      (responseHousehold.id !== boundHousehold.id ||
        responseHousehold.controlEpoch !== boundHousehold.controlEpoch)
    ) {
      throw new StaleAuthorityError("Conversation and response household authority disagree");
    }
    const householdFence = responseHousehold ?? boundHousehold;
    return new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
      ...(record.routing.senderPersonId ? { actorPersonId: record.routing.senderPersonId } : {}),
      participantEpochId: snapshot.participantEpochId,
      expectedParticipantDigest: snapshot.participantSetDigest,
      effectKind: "linq.message",
      idempotencyKey,
      data: { textDigest: sha256Hex(text), evidenceSourceRevisionIds },
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
      ...(evidenceSourceRevisionIds.length > 0
        ? {
            sourceConversation: {
              id: record.routing.conversationId,
              authorityVersion: snapshot.authorityVersion,
              participantEpochId: snapshot.participantEpochId,
              participantSetDigest: snapshot.participantSetDigest,
            },
            evidenceSourceRevisionIds,
          }
        : {}),
      ...(record.routing.senderPersonId ? await personFence(transaction, record.routing.senderPersonId) : {}),
      ...(householdFence ? { household: householdFence } : {}),
    });
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
        proposed_display_name_ciphertext: Buffer;
        requested_role: "steward" | "caregiver" | "participant";
        source_conversation_id: string;
        source_conversation_authority_version: number | string;
        source_participant_epoch_id: string;
        source_participant_digest: string;
        source_revision_id: string | null;
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
        invitation.proposed_display_name_ciphertext,
        invitation.requested_role,
        source_conversation.id as source_conversation_id,
        source_conversation.authority_version as source_conversation_authority_version,
        source_epoch.id as source_participant_epoch_id,
        source_epoch.participant_set_digest as source_participant_digest,
        invitation.source_revision_id,
        invitation.expires_at,
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
      join conversations source_conversation on source_conversation.id = invitation.source_conversation_id
        and source_conversation.kind = 'group' and source_conversation.status = 'active'
      join participant_epochs source_epoch on source_epoch.id = source_conversation.current_epoch_id
        and source_epoch.id = invitation.source_participant_epoch_id
        and source_epoch.ended_at is null
        and source_epoch.participant_set_digest = invitation.source_participant_digest
      join epoch_participants source_invitee on source_invitee.participant_epoch_id = source_epoch.id
        and source_invitee.person_identity_id = invitee_identity.id
        and source_invitee.person_id = invitee_identity.person_id
      where invitation.id = ${invitationId} and invitation.status = 'pending'
        and invitation.proposed_display_name_ciphertext is not null
        and invitation.expires_at > now()
        and (
          invitation.source_revision_id is null
          or exists(
            select 1
            from source_revisions source_revision
            join source_objects source_object on source_object.id = source_revision.source_object_id
              and source_object.status = 'active'
              and source_object.latest_revision_number = source_revision.revision_number
            where source_revision.id = invitation.source_revision_id
              and source_revision.participant_epoch_id = source_epoch.id
              and source_revision.revoked_at is null
              and source_revision.content_ciphertext is not null
              and source_revision.retention_until > now() + interval '3 minutes'
          )
        )
    `;
    const invitation = invitations[0];
    if (!invitation?.ready) return;
    const inviterName =
      decryptPersonName(this.secretBox, invitation.inviter_person_id, invitation.inviter_name_ciphertext) ??
      "Someone in your shared Florence group";
    const role =
      invitation.requested_role === "steward"
        ? "a co-parent"
        : invitation.requested_role === "caregiver"
          ? "a caregiver"
          : "a family participant";
    const proposedDisplayName = this.secretBox
      .decrypt(
        JSON.parse(invitation.proposed_display_name_ciphertext.toString("utf8")),
        `invitation-proposed-display-name:${invitationId}`,
      )
      .toString("utf8");

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
      const text = `${inviterName} says you’re ${proposedDisplayName} and invited you to join their Florence family as ${role}. Is that right? Reply yes to create your private account and join the family, or no if that isn’t right. I’ll stay silent in the group until you confirm. You can text STOP any time to stop Florence.`;
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
        sourceConversation: {
          id: invitation.source_conversation_id,
          authorityVersion: Number(invitation.source_conversation_authority_version),
          participantEpochId: invitation.source_participant_epoch_id,
          participantSetDigest: invitation.source_participant_digest,
        },
        ...(invitation.source_revision_id
          ? { evidenceSourceRevisionIds: [invitation.source_revision_id] }
          : {}),
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
    const text = `${inviterName} invited you to join their Florence family as ${role} and said you’re ${proposedDisplayName}. Is that right? Reply yes to join, or no if that isn’t right. I won’t ask you to repeat family details they already shared, and I’ll stay silent in the group until you confirm.`;
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
      sourceConversation: {
        id: invitation.source_conversation_id,
        authorityVersion: Number(invitation.source_conversation_authority_version),
        participantEpochId: invitation.source_participant_epoch_id,
        participantSetDigest: invitation.source_participant_digest,
      },
      ...(invitation.source_revision_id
        ? { evidenceSourceRevisionIds: [invitation.source_revision_id] }
        : {}),
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

  private async processWebCommand(
    actorPersonId: string,
    command: Extract<AppEnvelope, { kind: "web.command" }>["command"],
  ): Promise<ProcessReceipt> {
    return this.database.begin(async (transaction) => {
      const people = await transaction<
        { status: string; control_epoch: number | string; authority_version: number | string }[]
      >`
        select status, control_epoch, authority_version
        from people where id = ${actorPersonId} for update
      `;
      if (people[0]?.status !== "registered") throw new Error("Web command actor is not registered");
      const onboarding = new PostgresFamilyOnboarding(this.secretBox);
      switch (command.kind) {
        case "confirm_onboarding_profile": {
          const displayName = command.displayName.trim();
          const timeZone = command.timeZone.trim();
          if (!displayName || displayName.length > 80) {
            throw new ConflictError("Add the name Florence should use before continuing");
          }
          if (!validTimeZone(timeZone)) {
            throw new ConflictError("Choose a valid time zone before continuing");
          }
          const projection = await onboarding.project(transaction, {
            actorPersonId,
            personId: actorPersonId,
          });
          const encryptedName = this.secretBox.encrypt(displayName, `person-display-name:${actorPersonId}`);
          const updated = await transaction<{ readonly authority_version: number | string }[]>`
            update people
            set display_name_ciphertext = ${Buffer.from(JSON.stringify(encryptedName), "utf8")},
              display_name_key_version = ${encryptedName.kid}, timezone = ${timeZone},
              updated_at = now()
            where id = ${actorPersonId} and status = 'registered'
              and authority_version = ${projection.profile.authorityVersion}
            returning authority_version
          `;
          if (!updated[0]) throw new ConflictError("Your profile changed before it was saved");
          const result = await onboarding.confirmProfile(transaction, {
            actorPersonId,
            personId: actorPersonId,
            expectedPersonAuthorityVersion: projection.profile.authorityVersion,
            expectedProfileReviewVersion: projection.profile.reviewVersion,
            confirmedAt: new Date(),
          });
          await this.reconcileOnboardingReminder(transaction, actorPersonId, new Date());
          return {
            accepted: true,
            duplicate: false,
            disposition: "onboarding_profile_confirmed",
            ids: { version: String(result.version) },
          };
        }
        case "select_onboarding_household": {
          const projection = await onboarding.project(transaction, {
            actorPersonId,
            personId: actorPersonId,
          });
          const result = await onboarding.selectHousehold(transaction, {
            actorPersonId,
            personId: actorPersonId,
            householdId: command.householdId,
            expectedPersonOnboardingVersion: projection.profile.onboardingVersion,
            selectedAt: new Date(),
          });
          await this.reconcileOnboardingReminder(transaction, actorPersonId, new Date());
          return {
            accepted: true,
            duplicate: false,
            disposition: "onboarding_household_selected",
            ids: { householdId: command.householdId, version: String(result.version) },
          };
        }
        case "set_onboarding_coordinator": {
          const projection = await onboarding.project(transaction, {
            actorPersonId,
            personId: actorPersonId,
          });
          const household = requireOnboardingHousehold(projection, command.householdId);
          if (
            household.membershipVersion !== command.expectedMembershipVersion ||
            household.intakeVersion !== command.expectedIntakeVersion
          ) {
            throw new ConflictError("Your family setup changed before it was saved");
          }
          const result = await onboarding.setCoordinator(transaction, {
            actorPersonId,
            personId: actorPersonId,
            householdId: command.householdId,
            expectedMembershipVersion: command.expectedMembershipVersion,
            expectedIntakeVersion: command.expectedIntakeVersion,
            disposition: command.disposition,
            ...(command.proposedName ? { proposedCoordinatorName: command.proposedName } : {}),
            answeredAt: new Date(),
          });
          await this.reconcileOnboardingReminder(transaction, actorPersonId, new Date());
          return {
            accepted: true,
            duplicate: false,
            disposition: "onboarding_coordinator_recorded",
            ids: { householdId: command.householdId, version: String(result.version) },
          };
        }
        case "mark_onboarding_children_reviewed": {
          const projection = await onboarding.project(transaction, {
            actorPersonId,
            personId: actorPersonId,
          });
          const household = requireOnboardingHousehold(projection, command.householdId);
          if (
            household.membershipVersion !== command.expectedMembershipVersion ||
            household.intakeVersion !== command.expectedIntakeVersion
          ) {
            throw new ConflictError("Your child list changed before it was confirmed");
          }
          const result = await onboarding.markChildRosterReviewed(transaction, {
            actorPersonId,
            personId: actorPersonId,
            householdId: command.householdId,
            expectedMembershipVersion: command.expectedMembershipVersion,
            expectedIntakeVersion: command.expectedIntakeVersion,
            reviewedAt: new Date(),
          });
          await this.reconcileOnboardingReminder(transaction, actorPersonId, new Date());
          return {
            accepted: true,
            duplicate: false,
            disposition: "onboarding_children_reviewed",
            ids: { householdId: command.householdId, version: String(result.version) },
          };
        }
        case "defer_onboarding_coordinator_invite": {
          const projection = await onboarding.project(transaction, {
            actorPersonId,
            personId: actorPersonId,
          });
          const household = requireOnboardingHousehold(projection, command.householdId);
          if (
            household.membershipVersion !== command.expectedMembershipVersion ||
            household.intakeVersion !== command.expectedIntakeVersion
          ) {
            throw new ConflictError("Your coordinator step changed before it was deferred");
          }
          const result = await onboarding.deferCoordinatorInvite(transaction, {
            actorPersonId,
            personId: actorPersonId,
            householdId: command.householdId,
            expectedMembershipVersion: command.expectedMembershipVersion,
            expectedIntakeVersion: command.expectedIntakeVersion,
            deferredAt: new Date(),
          });
          await this.reconcileOnboardingReminder(transaction, actorPersonId, new Date());
          return {
            accepted: true,
            duplicate: false,
            disposition: "onboarding_coordinator_invite_deferred",
            ids: { householdId: command.householdId, version: String(result.version) },
          };
        }
        case "review_onboarding_shared_context": {
          const projection = await onboarding.project(transaction, {
            actorPersonId,
            personId: actorPersonId,
          });
          const household = requireOnboardingHousehold(projection, command.householdId);
          if (
            household.membershipVersion !== command.expectedMembershipVersion ||
            household.intakeVersion !== command.expectedIntakeVersion ||
            household.membershipOnboardingVersion !== command.expectedMembershipOnboardingVersion
          ) {
            throw new ConflictError("The family summary changed before you confirmed it");
          }
          const result = await onboarding.reviewSharedContext(transaction, {
            actorPersonId,
            personId: actorPersonId,
            householdId: command.householdId,
            expectedMembershipVersion: command.expectedMembershipVersion,
            expectedIntakeVersion: command.expectedIntakeVersion,
            expectedMembershipOnboardingVersion: command.expectedMembershipOnboardingVersion,
            reviewedAt: new Date(),
          });
          await this.reconcileOnboardingReminder(transaction, actorPersonId, new Date());
          return {
            accepted: true,
            duplicate: false,
            disposition: "onboarding_shared_context_reviewed",
            ids: { householdId: command.householdId, version: String(result.version) },
          };
        }
        case "skip_onboarding_google": {
          const projection = await onboarding.project(transaction, {
            actorPersonId,
            personId: actorPersonId,
          });
          const updated = await transaction<{ readonly id: string }[]>`
            update people
            set google_activation_suppressed_at = now(), updated_at = now()
            where id = ${actorPersonId} and status = 'registered'
              and control_epoch = ${projection.profile.controlEpoch}
            returning id
          `;
          if (!updated[0]) throw new ConflictError("Your Google choice changed before it was saved");
          const result = await onboarding.touchProgress(transaction, {
            actorPersonId,
            personId: actorPersonId,
            expectedPersonControlEpoch: projection.profile.controlEpoch,
            progressedAt: new Date(),
          });
          await this.reconcileOnboardingReminder(transaction, actorPersonId, new Date());
          return {
            accepted: true,
            duplicate: false,
            disposition: "onboarding_google_skipped",
            ids: { version: String(result.version) },
          };
        }
        case "complete_onboarding": {
          const projection = await onboarding.project(transaction, {
            actorPersonId,
            personId: actorPersonId,
          });
          const household = requireOnboardingHousehold(projection, command.householdId);
          if (
            household.membershipVersion !== command.expectedMembershipVersion ||
            projection.profile.reviewVersion !== command.expectedProfileReviewVersion ||
            household.intakeVersion !== command.expectedIntakeVersion ||
            household.membershipOnboardingVersion !== command.expectedMembershipOnboardingVersion
          ) {
            throw new ConflictError("Your final setup review changed before it was confirmed");
          }
          const result = await onboarding.completeMembership(transaction, {
            actorPersonId,
            personId: actorPersonId,
            householdId: command.householdId,
            expectedMembershipVersion: command.expectedMembershipVersion,
            expectedProfileReviewVersion: command.expectedProfileReviewVersion,
            expectedIntakeVersion: command.expectedIntakeVersion,
            expectedMembershipOnboardingVersion: command.expectedMembershipOnboardingVersion,
            completedAt: new Date(),
          });
          return {
            accepted: true,
            duplicate: projection.nextStep.kind === "complete",
            disposition: "onboarding_completed",
            ids: {
              householdId: command.householdId,
              membershipId: household.membershipId,
              version: String(result.version),
            },
          };
        }
        case "create_household": {
          const existing = await transaction<{ id: string }[]>`
            select household_id as id from household_memberships
            where person_id = ${actorPersonId} and status = 'active' limit 1
          `;
          if (existing[0]) {
            return {
              accepted: true,
              duplicate: true,
              disposition: "household_exists",
              ids: { householdId: existing[0].id },
            };
          }
          const timeZone = await this.requireConfirmedPersonTimeZone(transaction, actorPersonId);
          const result = await new PostgresIdentityRelationships(transaction).createHousehold({
            founderPersonId: actorPersonId,
            timezone: timeZone,
            createdAt: new Date().toISOString(),
          });
          const projection = await onboarding.project(transaction, {
            actorPersonId,
            personId: actorPersonId,
          });
          if (projection.profile.onboardingVersion > 0) {
            await onboarding.selectHousehold(transaction, {
              actorPersonId,
              personId: actorPersonId,
              householdId: result.householdId,
              expectedPersonOnboardingVersion: projection.profile.onboardingVersion,
              selectedAt: new Date(),
            });
            await this.reconcileOnboardingReminder(transaction, actorPersonId, new Date());
          }
          return {
            accepted: true,
            duplicate: false,
            disposition: "household_created",
            ids: { householdId: result.householdId },
          };
        }
        case "invite_household_participant": {
          const invitationResult = await new HouseholdOnboarding(
            transaction,
            this.secretBox,
          ).inviteCurrentParticipant({
            actorPersonId,
            householdId: command.householdId,
            conversationId: command.conversationId,
            expectedParticipantEpochId: command.expectedParticipantEpochId,
            expectedParticipantDigest: command.expectedParticipantDigest,
            inviteeIdentityId: command.inviteeIdentityId,
            inviteePersonId: command.inviteePersonId,
            proposedDisplayName: command.proposedDisplayName,
            role: command.role,
            sourceRevisionId: null,
            createdAt: new Date(),
          });
          const invitation = invitationResult.invitation;
          await this.queueHouseholdInvitationMessage(transaction, invitation.invitationId);
          await this.noteOnboardingProgress(transaction, actorPersonId, new Date());
          return {
            accepted: true,
            duplicate: invitationResult.duplicate,
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
          const invitationSources = await transaction<
            {
              source_conversation_id: string | null;
              status: string;
            }[]
          >`
            select source_conversation_id, status from invitations
            where id = ${command.invitationId}
          `;
          const invitationSource = invitationSources[0];
          if (!invitationSource) throw new NotFoundError("Family invitation does not exist");
          const sourceConversationId = invitationSource.source_conversation_id;
          const acceptedAt = new Date();
          let membership: HouseholdMembership;
          try {
            membership = await new HouseholdOnboarding(transaction, this.secretBox).acceptInvitation({
              actorPersonId,
              invitationId: command.invitationId,
              acceptedAt,
            });
          } catch (error) {
            if (!(error instanceof ConflictError) && !(error instanceof NotFoundError)) throw error;
            return {
              accepted: true,
              duplicate: invitationSource.status !== "pending",
              disposition: "household_invitation_stale",
              ids: { invitationId: command.invitationId },
            };
          }
          await transaction`
            update households set status = 'active', updated_at = ${acceptedAt}
            where id = ${membership.householdId} and status = 'onboarding'
          `;
          await this.selectAcceptedOnboardingHousehold(
            transaction,
            actorPersonId,
            membership.householdId,
            acceptedAt,
          );
          await this.queueParentOnboardingOffer(
            transaction,
            actorPersonId,
            "invitation_accepted",
            undefined,
            command.invitationId,
          );
          if (sourceConversationId) {
            const authority = await new FamilyGroupAuthority(transaction).reconcile({
              conversationId: sourceConversationId,
              occurredAt: acceptedAt,
            });
            if (authority.activatedNow && authority.ruleId && authority.householdId) {
              await this.queueFamilyGroupActivationAcknowledgement(transaction, {
                invitationId: command.invitationId,
                actorPersonId,
                conversationId: sourceConversationId,
                householdId: authority.householdId,
                householdControlEpoch: authority.householdControlEpoch,
                participantEpochId: authority.participantEpochId,
                participantSetDigest: authority.participantSetDigest,
                ruleId: authority.ruleId,
                snapshot: authority.snapshot,
              });
            }
          }
          return {
            accepted: true,
            duplicate: false,
            disposition: "household_invitation_accepted",
            ids: { householdId: membership.householdId, membershipId: membership.membershipId },
          };
        }
        case "add_dependent": {
          const changedAt = new Date();
          const roster = await lockDependentRoster(transaction, command.householdId);
          const dependent = await new HouseholdOnboarding(transaction, this.secretBox).addDependent({
            actorPersonId,
            householdId: command.householdId,
            displayName: command.displayName,
            aliases: command.aliases,
            birthYear: command.birthYear,
            school: command.school,
            activities: command.activities,
            createdAt: changedAt,
          });
          await refreshDependentRosterReview(transaction, {
            householdId: command.householdId,
            actorPersonId,
            expectedRosterVersion: roster.rosterVersion + 1,
            expectedIntakeVersion: roster.intakeVersion,
            wasReviewed: roster.reviewed,
            changedAt,
          });
          await this.noteOnboardingProgress(transaction, actorPersonId, changedAt);
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
          const changedAt = new Date();
          const roster = await lockDependentRoster(transaction, command.householdId);
          assertDependentEditVersions(roster, {
            rosterVersion: command.expectedRosterVersion,
            intakeVersion: command.expectedIntakeVersion,
          });
          await new HouseholdOnboarding(transaction, this.secretBox).updateDependent({
            actorPersonId,
            householdId: command.householdId,
            dependentPersonId: command.dependentPersonId,
            displayName: command.displayName,
            aliases: command.aliases,
            birthYear: command.birthYear,
            school: command.school,
            activities: command.activities,
            updatedAt: changedAt,
          });
          await refreshDependentRosterReview(transaction, {
            householdId: command.householdId,
            actorPersonId,
            expectedRosterVersion: roster.rosterVersion,
            expectedIntakeVersion: roster.intakeVersion,
            wasReviewed: roster.reviewed,
            changedAt,
          });
          await this.noteOnboardingProgress(transaction, actorPersonId, changedAt);
          return {
            accepted: true,
            duplicate: false,
            disposition: "dependent_updated",
            ids: { dependentPersonId: command.dependentPersonId },
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
          const stepUpContext = command.context ?? {};
          await new DurableWork(transaction, this.secretBox).enqueue({
            kind: "auth.send_step_up",
            idempotencyKey: `step-up:${actorPersonId}:${command.purpose}:${JSON.stringify(stepUpContext)}:${Math.floor(Date.now() / 60_000)}`,
            payload: { actorPersonId, purpose: command.purpose, context: stepUpContext },
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

function isExplicitFamilyInvitationDecline(value: string): boolean {
  const normalized = value
    .trim()
    .toLocaleLowerCase("en-US")
    .replace(/[’]/gu, "'")
    .replace(/[.!]+$/gu, "")
    .replace(/\s+/gu, " ");
  return /^(?:no|not me|that (?:isn't|is not) me|decline)$/u.test(normalized);
}

/** A strict social greeting, never a message that may contain family work. */
export function isNaturalPrivateGreeting(chatKind: "direct" | "group", value: string): boolean {
  if (chatKind !== "direct") return false;
  const normalized = value.trim().replace(/\s+/gu, " ");
  return /^(?:hi|hello|hey)(?:[ ,]+(?:there|florence))?[!. 👋]*$/iu.test(normalized);
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
): {
  kind: StoredLinqEvent["classification"];
  enrollmentAction?: "consent" | "decline_invitation" | "other";
  retainEvent: boolean;
} {
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
        enrollmentAction: isExplicitEnrollmentConsent(command)
          ? "consent"
          : isExplicitFamilyInvitationDecline(command)
            ? "decline_invitation"
            : "other",
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

function privateGuidanceActionCopy(step: PrivateOnboardingGuidance["recommendedNextStep"]["kind"]): string {
  switch (step) {
    case "confirm_profile":
      return "Confirm the details Florence should use for you here:";
    case "create_household":
      return "Set up your private Florence family here:";
    case "choose_household":
      return "Choose the family you want to continue with here:";
    case "coordinator":
      return "Tell Florence who else helps coordinate your family here:";
    case "children":
      return "Add the children and family context Florence should know here:";
    case "coordinator_invite":
      return "Continue your co-parent or caregiver setup here:";
    case "review_shared_context":
      return "Review the family details already shared with Florence here:";
    case "google":
      return "Continue setup and choose whether to connect your personal Google account here:";
    case "review":
      return "Review your Florence family setup here:";
    case "complete":
      throw new UnauthorizedError("This private guidance step has no handoff action");
  }
}

async function recoverPriorConversationMessageText(
  transaction: Transaction,
  secretBox: SecretBox,
  idempotencyKey: string,
): Promise<string | null> {
  const rows = await transaction<{ readonly payload_ciphertext: Buffer }[]>`
    select payload_ciphertext from outbox where idempotency_key = ${idempotencyKey}
  `;
  const ciphertext = rows[0]?.payload_ciphertext;
  if (!ciphertext) return null;
  const payload = JSON.parse(
    secretBox.decrypt(JSON.parse(ciphertext.toString("utf8")), "effect-payload").toString("utf8"),
  ) as unknown;
  if (
    typeof payload !== "object" ||
    payload === null ||
    !("text" in payload) ||
    typeof payload.text !== "string"
  ) {
    throw new UnauthorizedError("Prior conversational response payload is invalid");
  }
  return payload.text;
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

function relationshipLabel(role: "steward" | "caregiver" | "participant"): string {
  if (role === "steward") return "co-parent";
  if (role === "caregiver") return "caregiver";
  return "family participant";
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

function assertExpectedConversationAuthority(
  expected: ExpectedConversationAuthority,
  conversationId: string,
  authorityVersion: number,
  participantEpochId: string | null,
  participantSetDigest: string | null,
): void {
  if (
    expected.id !== conversationId ||
    expected.authorityVersion !== authorityVersion ||
    expected.participantEpochId !== participantEpochId ||
    expected.participantSetDigest !== participantSetDigest
  ) {
    throw new StaleAuthorityError("Conversation authority changed after model execution");
  }
}

async function assertExpectedResponseAuthority(
  transaction: Transaction,
  record: StoredLinqEvent,
  expectedPerson: { readonly id: string; readonly controlEpoch: number },
  expectedHousehold: { readonly id: string; readonly controlEpoch: number } | null,
): Promise<void> {
  if (record.routing.senderPersonId !== expectedPerson.id) {
    throw new StaleAuthorityError("Response person changed after model execution");
  }
  const people = await transaction<{ readonly id: string }[]>`
    select id from people
    where id = ${expectedPerson.id} and status = 'registered'
      and control_epoch = ${expectedPerson.controlEpoch}
    for share
  `;
  if (!people[0]) throw new StaleAuthorityError("Response person authority changed");
  if (!expectedHousehold) return;
  const households = await transaction<{ readonly id: string }[]>`
    select household.id
    from households household
    where household.id = ${expectedHousehold.id}
      and household.control_epoch = ${expectedHousehold.controlEpoch}
      and household.status in ('onboarding', 'active', 'paused')
      and (
        exists(
          select 1 from conversations conversation
          where conversation.id = ${record.routing.conversationId}
            and conversation.household_id = household.id
            and conversation.status = 'active'
        )
        or exists(
          select 1
          from conversations conversation
          join household_memberships membership on membership.household_id = household.id
            and membership.person_id = ${expectedPerson.id} and membership.status = 'active'
          join membership_capabilities read_capability on read_capability.membership_id = membership.id
            and read_capability.capability = 'household.read' and read_capability.status = 'active'
          where conversation.id = ${record.routing.conversationId}
            and conversation.kind = 'direct' and conversation.status = 'active'
            and conversation.household_id is null
        )
      )
    for share of household
  `;
  if (!households[0]) throw new StaleAuthorityError("Response household authority changed");
}

async function assertPrivateQuestionSourceAuthorities(
  transaction: Transaction,
  personId: string,
  authorities: readonly {
    readonly integrationId: string;
    readonly integrationControlEpoch: number;
    readonly status: "active" | "paused" | "reauth_required" | "error";
  }[],
): Promise<void> {
  if (
    authorities.length > 12 ||
    new Set(authorities.map((authority) => authority.integrationId)).size !== authorities.length
  ) {
    throw new UnauthorizedError("Private source authority set is outside the allowed bounds");
  }
  if (authorities.length === 0) return;
  const rows = await transaction<
    { readonly id: string; readonly control_epoch: number | string; readonly status: string }[]
  >`
    select integration.id, integration.control_epoch, integration.status
    from integrations integration
    where integration.id = any(${transaction.array(authorities.map((authority) => authority.integrationId))}::uuid[])
      and integration.person_id = ${personId} and integration.provider = 'google'
      and exists(
        select 1 from integration_capabilities capability
        where capability.integration_id = integration.id
          and capability.capability = 'mail' and capability.status = 'active'
      )
  `;
  const currentById = new Map(rows.map((row) => [row.id, row] as const));
  if (
    authorities.some((authority) => {
      const current = currentById.get(authority.integrationId);
      return (
        !current ||
        Number(current.control_epoch) !== authority.integrationControlEpoch ||
        current.status !== authority.status
      );
    })
  ) {
    throw new StaleAuthorityError("Private Gmail authority changed before answer commit");
  }
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

function requireOnboardingHousehold(
  projection: FamilyOnboardingProjection,
  householdId: string,
): NonNullable<FamilyOnboardingProjection["household"]> {
  const household = projection.household;
  if (!household || household.householdId !== householdId) {
    throw new UnauthorizedError("That family is not your current onboarding family");
  }
  return household;
}

interface LockedDependentRoster {
  readonly rosterVersion: number;
  readonly intakeVersion: number;
  readonly reviewed: boolean;
}

async function lockDependentRoster(
  transaction: Transaction,
  householdId: string,
): Promise<LockedDependentRoster> {
  const rows = await transaction<
    {
      readonly membership_version: number | string;
      readonly intake_version: number | string;
      readonly reviewed: boolean;
    }[]
  >`
    select household.membership_version, intake.version as intake_version,
      intake.child_roster_reviewed_at is not null as reviewed
    from households household
    join household_onboarding_intakes intake on intake.household_id = household.id
    where household.id = ${householdId}
      and household.status in ('onboarding', 'active', 'paused')
    for update of household, intake
  `;
  const row = rows[0];
  if (!row) throw new NotFoundError("That family’s child details are not available");
  return {
    rosterVersion: Number(row.membership_version),
    intakeVersion: Number(row.intake_version),
    reviewed: row.reviewed,
  };
}

export function assertDependentEditVersions(
  current: { readonly rosterVersion: number; readonly intakeVersion: number },
  expected: { readonly rosterVersion: number; readonly intakeVersion: number },
): void {
  if (current.rosterVersion !== expected.rosterVersion || current.intakeVersion !== expected.intakeVersion) {
    throw new ConflictError(
      "Those child details changed while you were editing. Review the latest family details and try again.",
    );
  }
}

export function dependentRosterReviewAfterMutation(input: {
  readonly wasReviewed: boolean;
  readonly actorPersonId: string;
  readonly rosterVersion: number;
  readonly changedAt: Date;
}): {
  readonly reviewedByPersonId: string | null;
  readonly reviewedAt: Date | null;
  readonly rosterVersion: number | null;
} {
  return input.wasReviewed
    ? {
        reviewedByPersonId: input.actorPersonId,
        reviewedAt: input.changedAt,
        rosterVersion: input.rosterVersion,
      }
    : { reviewedByPersonId: null, reviewedAt: null, rosterVersion: null };
}

async function refreshDependentRosterReview(
  transaction: Transaction,
  input: {
    readonly householdId: string;
    readonly actorPersonId: string;
    readonly expectedRosterVersion: number;
    readonly expectedIntakeVersion: number;
    readonly wasReviewed: boolean;
    readonly changedAt: Date;
  },
): Promise<void> {
  const review = dependentRosterReviewAfterMutation({
    wasReviewed: input.wasReviewed,
    actorPersonId: input.actorPersonId,
    rosterVersion: input.expectedRosterVersion,
    changedAt: input.changedAt,
  });
  const rows = await transaction<{ readonly version: number | string }[]>`
    update household_onboarding_intakes intake
    set child_roster_reviewed_by_person_id = ${review.reviewedByPersonId},
      child_roster_reviewed_at = ${review.reviewedAt},
      child_roster_household_membership_version = ${review.rosterVersion},
      version = version + 1,
      updated_at = ${input.changedAt}
    where intake.household_id = ${input.householdId}
      and intake.version = ${input.expectedIntakeVersion}
      and exists(
        select 1 from households household
        where household.id = intake.household_id
          and household.membership_version = ${input.expectedRosterVersion}
      )
    returning version
  `;
  if (!rows[0]) {
    throw new ConflictError(
      "Those child details changed while you were editing. Review the latest family details and try again.",
    );
  }
}

function validTimeZone(candidate: string): boolean {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: candidate }).format(0);
    return true;
  } catch {
    return false;
  }
}

function reminderStage(candidate: number): 0 | 1 | 2 {
  return candidate <= 0 ? 0 : candidate === 1 ? 1 : 2;
}

function onboardingReminderStep(projection: FamilyOnboardingProjection): OnboardingReminderStep | null {
  switch (projection.nextStep.kind) {
    case "confirm_profile":
      return "profile";
    case "create_household":
    case "choose_household":
    case "coordinator":
      return "family";
    case "children":
      return "children";
    case "coordinator_invite":
      return "invite";
    case "review_shared_context":
    case "review":
      return "review";
    case "google":
      return "google";
    case "complete":
      return null;
  }
}
