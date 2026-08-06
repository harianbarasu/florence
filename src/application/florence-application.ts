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
  GroupRuleOnboarding,
  PostgresConversationAuthority,
} from "../modules/conversations/index.js";
import { PostgresRoutines, type RoutineRevisionDraft } from "../modules/coordination/index.js";
import { PostgresDataControls } from "../modules/data-controls/index.js";
import { EffectOutbox } from "../modules/effects/index.js";
import { PostgresIdentityRelationships } from "../modules/identity/index.js";
import { HouseholdOnboarding } from "../modules/relationships/index.js";
import { PostgresSourceIntelligence } from "../modules/sources/index.js";
import { DurableWork } from "../modules/work/index.js";
import { canonicalDigest, canonicalJson } from "../shared/canonical-json.js";
import type { SecretBox } from "../shared/crypto.js";
import { NotFoundError, StaleAuthorityError, UnauthorizedError } from "../shared/errors.js";
import type {
  AppEnvelope,
  ApplicationTimerProcessor,
  ProcessReceipt,
  WebRoutineFields,
} from "./contracts.js";
import { reconcileCoverageTimers } from "./coverage-timer-reconciliation.js";

type Transaction = TransactionSql<Record<string, never>>;

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
  readonly classification: "enrollment" | "stop" | "full" | "receipt" | "routing_only";
  readonly enrollmentAction?: "start" | "other";
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
      case "web.command":
        return this.processWebCommand(input.actorPersonId, input.command);
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
        reconciled.epochSequence > 1 &&
        Date.parse(ordinaryGroupContentAt) < Date.parse(reconciled.epochStartedAt);
      const precedesParticipantConsent =
        ordinaryGroupContentAt !== null &&
        reconciled.contentEligibleAt !== null &&
        Date.parse(ordinaryGroupContentAt) < Date.parse(reconciled.contentEligibleAt);
      const isStopCommand =
        event.eventType === "linq.message.received" && messageText(event).trim().toUpperCase() === "STOP";
      const outsideCurrentContentWindow =
        !isStopCommand && (belongsToPriorParticipantEpoch || precedesParticipantConsent);
      const classification: ReturnType<typeof classifyEvent> = outsideCurrentContentWindow
        ? ({ kind: "routing_only", retainEvent: false } as const)
        : classifyEvent(event, liveChat.kind, reconciled.mode);
      const record: StoredLinqEvent = {
        schemaVersion: 1,
        classification: classification.kind,
        ...(classification.enrollmentAction ? { enrollmentAction: classification.enrollmentAction } : {}),
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
          conversation: {
            id: reconciled.conversationId,
            authorityVersion: reconciled.snapshot.authorityVersion,
          },
          ...(reconciled.householdId && reconciled.householdControlEpoch
            ? { household: { id: reconciled.householdId, controlEpoch: reconciled.householdControlEpoch } }
            : {}),
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
      observed.set(normalized, { identityId: principal.identityId, personId: principal.personId });
    }
    if (observed.size === 0) throw new Error("A Florence conversation needs at least one human participant");

    let binding = await conversations.findByChannel({
      provider: "linq",
      externalChannelId: liveChat.providerChatId,
    });
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
    const participantIdentityIds = [...observed.values()].map((entry) => entry.identityId).sort();
    const epoch = await conversations.recordParticipantEpoch({
      conversationId: binding.conversationId,
      participantIdentityIds,
      changeReason: event?.eventType.startsWith("linq.participant.")
        ? event.eventType
        : event === null
          ? "authoritative_outbound_reconciliation"
          : "authoritative_chat_reconciliation",
      observedAt: liveChat.checkedAt,
    });
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

  private async handleEnrollment(transaction: Transaction, record: StoredLinqEvent): Promise<string> {
    const identityId = record.routing.senderIdentityId;
    const personId = record.routing.senderPersonId;
    if (!identityId || !personId) return "ignored";
    if (record.enrollmentAction !== "start") {
      await this.queueSystemEnrollmentMessage(
        transaction,
        record,
        "Hi—I’m Florence, a family Chief of Staff. I can help keep logistics covered across messages, email, calendars, and PDFs. Reply START to register and agree to Florence’s privacy controls, or STOP at any time.",
      );
      return "enrollment_prompted";
    }
    const identities = await transaction<
      { authority_version: number | string; status: string; person_status: string }[]
    >`
      select identity.authority_version, identity.status, person.status as person_status
      from person_identities identity join people person on person.id = identity.person_id
      where identity.id = ${identityId} for update of identity, person
    `;
    const current = identities[0];
    if (!current) return "ignored";
    const consentedAt = new Date();
    if (current.status !== "verified") {
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
          authority_version = authority_version + 1,
          control_epoch = control_epoch + 1, updated_at = ${consentedAt}
        where id = ${personId} and status in ('provisional', 'stopped')
      `;
    }
    await this.consentPersonAcrossCurrentEpochs(transaction, personId, consentedAt);
    const handoff = await new PostgresWebAuth(
      transaction,
      this.secretBox,
      this.config.security.tokenKey,
    ).createHandoff({
      personId,
      privateIdentityId: identityId,
      privateConversationId: record.routing.conversationId,
      purpose: "web_sign_in",
      context: { onboarding: true },
      expiresInSeconds: 10 * 60,
    });
    const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
      record.routing.conversationId,
    );
    await this.queueAuthorizedConversationMessage(
      transaction,
      record,
      snapshot,
      `You’re registered. If you’d like, reply NAME followed by what I should call you. Open your private Florence companion to finish setting up your family: ${this.config.publicBaseUrl}/handoff/${handoff.token}`,
      "direct_response",
      "onboarding_welcome",
    );
    return "registered";
  }

  /**
   * A verified private START is the person's explicit Florence registration.
   * It applies their conservative default to every exact live chat audience they
   * already belong to; it never enables proactive writing for those chats.
   */
  private async consentPersonAcrossCurrentEpochs(
    transaction: Transaction,
    personId: string,
    consentedAt: Date,
  ): Promise<void> {
    const epochs = await transaction<{ participant_epoch_id: string }[]>`
      select distinct epoch.id as participant_epoch_id
      from participant_epochs epoch
      join conversations conversation on conversation.current_epoch_id = epoch.id
        and conversation.status = 'active' and epoch.ended_at is null
      join epoch_participants participant on participant.participant_epoch_id = epoch.id
      where participant.person_id = ${personId}
      order by epoch.id
    `;
    const authority = new PostgresConversationAuthority(transaction);
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
    }
  }

  private async handleStop(transaction: Transaction, record: StoredLinqEvent): Promise<string> {
    const personId = record.routing.senderPersonId;
    if (!personId) return "ignored";
    if (record.routing.chatKind === "direct") {
      await transaction`
        update people set status = 'stopped', control_epoch = control_epoch + 1,
          authority_version = authority_version + 1, updated_at = now()
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
    if (event.eventType !== "linq.message.received") return "observed";
    const text = messageText(event);
    const normalizedCommand = text.trim().toLowerCase();
    const nameMatch = record.routing.chatKind === "direct" ? /^name\s+(.{1,80})$/iu.exec(text.trim()) : null;
    if (nameMatch) {
      const personId = record.routing.senderPersonId;
      if (!personId) return "ignored";
      const displayName = nameMatch[1]?.trim();
      if (!displayName || /[\r\n]/u.test(displayName)) return "invalid_name";
      const encryptedName = this.secretBox.encrypt(displayName, `person-display-name:${personId}`);
      await transaction`
        update people
        set display_name_ciphertext = ${Buffer.from(JSON.stringify(encryptedName), "utf8")},
          display_name_key_version = ${encryptedName.kid}, updated_at = now()
        where id = ${personId} and status = 'registered'
      `;
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        record.routing.conversationId,
      );
      await this.queueAuthorizedConversationMessage(
        transaction,
        record,
        snapshot,
        `Got it—I’ll call you ${displayName}.`,
        "direct_response",
        "profile_name_updated",
      );
      return "profile_name_updated";
    }
    if (
      record.routing.chatKind === "direct" &&
      /^(sign in|settings|open florence|connect google)$/u.test(normalizedCommand)
    ) {
      const identityId = record.routing.senderIdentityId;
      const personId = record.routing.senderPersonId;
      if (!identityId || !personId) return "ignored";
      const handoff = await new PostgresWebAuth(
        transaction,
        this.secretBox,
        this.config.security.tokenKey,
      ).createHandoff({
        personId,
        privateIdentityId: identityId,
        privateConversationId: record.routing.conversationId,
        purpose: normalizedCommand === "connect google" ? "google_connect" : "web_sign_in",
        context: { returnPath: normalizedCommand === "connect google" ? "/sources" : "/home" },
        expiresInSeconds: 10 * 60,
      });
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        record.routing.conversationId,
      );
      await this.queueAuthorizedConversationMessage(
        transaction,
        record,
        snapshot,
        `Here’s your private, single-use Florence link: ${this.config.publicBaseUrl}/handoff/${handoff.token}`,
        "direct_response",
        "private_handoff",
      );
      return "handoff_created";
    }
    if (
      record.routing.chatKind === "direct" &&
      /^(set up|setup|create)( my| our)? family$/u.test(normalizedCommand)
    ) {
      const personId = record.routing.senderPersonId;
      if (!personId) return "ignored";
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
      maxAttempts: 5,
      deadlineAt: new Date(Date.now() + 5 * 60_000),
    });
    return "orchestration_queued";
  }

  private async handleProviderReceipt(transaction: Transaction, record: StoredLinqEvent): Promise<string> {
    const event = record.event;
    if (!event || (event.eventType !== "linq.outbound.sent" && event.eventType !== "linq.outbound.failed")) {
      return "observed";
    }
    const providerMessageId = event.receipt.providerMessageId;
    const providerIdempotencyKey =
      event.eventType === "linq.outbound.sent" ? (event.receipt.idempotencyKey ?? null) : null;
    const rows = await transaction<{ outbox_id: string; idempotency_key: string }[]>`
      select effect.id as outbox_id, effect.idempotency_key
      from outbox effect
      left join effect_receipts receipt on receipt.outbox_id = effect.id
      where receipt.provider_receipt_id = ${providerMessageId}
        or (${providerIdempotencyKey}::text is not null and effect.idempotency_key = ${providerIdempotencyKey})
      order by (receipt.provider_receipt_id = ${providerMessageId}) desc
      limit 1
    `;
    if (!rows[0]) return "unmatched_receipt";
    const failed = event.eventType === "linq.outbound.failed";
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
        ${failed ? "failed" : "confirmed"}, ${sha256Hex(receiptJson)},
        ${Buffer.from(JSON.stringify(encrypted), "utf8")}, ${encrypted.kid},
        ${new Date(event.occurredAt)}, ${failed ? null : new Date(event.occurredAt)},
        ${failed ? event.receipt.errorCode : null}
      ) on conflict do nothing
    `;
    await transaction`
      update outbox set status = ${failed ? "submitted" : "confirmed"},
        available_at = ${failed ? new Date(Date.now() + 60_000) : new Date()},
        lease_owner = null, lease_token = null, lease_expires_at = null,
        last_error_code = ${failed ? event.receipt.errorCode : null}, updated_at = now()
      where id = ${rows[0].outbox_id}
    `;
    return failed ? "delivery_failure_observed" : "delivery_confirmed";
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
      authorizationExpiresAt: new Date(Date.now() + 5 * 60_000),
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
  ): Promise<void> {
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
    if (!authority.allowed) return;
    const household = await transaction<{ id: string; control_epoch: number | string }[]>`
      select household.id, household.control_epoch
      from conversations conversation join households household on household.id = conversation.household_id
      where conversation.id = ${record.routing.conversationId}
    `;
    await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
      ...(record.routing.senderPersonId ? { actorPersonId: record.routing.senderPersonId } : {}),
      participantEpochId: snapshot.participantEpochId,
      expectedParticipantDigest: snapshot.participantSetDigest,
      effectKind: "linq.message",
      idempotencyKey: `linq:${operation}:${record.routing.conversationId}:${sha256Hex(text)}`,
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
      authorizationExpiresAt: new Date(Date.now() + 5 * 60_000),
      conversation: { id: record.routing.conversationId, authorityVersion: snapshot.authorityVersion },
      ...(record.routing.senderPersonId ? await personFence(transaction, record.routing.senderPersonId) : {}),
      ...(household[0]
        ? { household: { id: household[0].id, controlEpoch: Number(household[0].control_epoch) } }
        : {}),
    });
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
          await transaction`
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
          `;
          await transaction`
            update households set status = 'active', updated_at = now()
            where id = ${membership.householdId} and status = 'onboarding'
          `;
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
        case "approve_group_coverage_rule": {
          const result = await new GroupRuleOnboarding(transaction).approveFamilyCoverage({
            conversationId: command.conversationId,
            actorPersonId,
            approvedAt: new Date(),
          });
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
          const result = await new PostgresSourceIntelligence(transaction, this.secretBox, {
            rawRetentionDays: this.config.defaults.rawSourceRetentionDays,
          }).apply({
            kind: "configure_calendar_privacy",
            integrationId: command.integrationId,
            personId: actorPersonId,
            expectedIntegrationControlEpoch: Number(integration.control_epoch),
            calendarIdDigest: sha256Hex(command.calendarId),
            mode: command.mode,
            changedAt: new Date().toISOString(),
          });
          if (result.kind !== "calendar_privacy_configured")
            throw new Error("Calendar privacy did not update");
          if (command.mode !== "off") {
            await new DurableWork(transaction, this.secretBox).enqueue({
              kind: "google.calendar.poll",
              idempotencyKey: `google:calendar:${command.integrationId}:${result.calendarIdDigest}:policy-v${result.grantVersion}`,
              payload: {
                integrationId: command.integrationId,
                personId: actorPersonId,
                integrationControlEpoch: result.integrationControlEpoch,
                personControlEpoch: Number(people[0].control_epoch),
                calendarId: command.calendarId,
                calendarIdDigest: result.calendarIdDigest,
                mode: command.mode,
              },
              person: { id: actorPersonId, controlEpoch: Number(people[0].control_epoch) },
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
            idempotencyKey: `step-up:${actorPersonId}:${command.purpose}:${Math.floor(Date.now() / 60_000)}`,
            payload: { actorPersonId, purpose: command.purpose },
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

function classifyEvent(
  event: Exclude<LinqWebhookEnvelope, { eventType: "linq.ignored" }>,
  chatKind: "direct" | "group",
  mode: ReturnType<typeof evaluateConversationMode>,
): { kind: StoredLinqEvent["classification"]; enrollmentAction?: "start" | "other"; retainEvent: boolean } {
  if (event.eventType === "linq.outbound.sent" || event.eventType === "linq.outbound.failed") {
    return { kind: "receipt", retainEvent: true };
  }
  if (event.eventType === "linq.message.received") {
    const command = messageText(event).trim().toUpperCase();
    if (command === "STOP") return { kind: "stop", retainEvent: false };
    if (chatKind === "direct" && mode === "content_disabled") {
      return {
        kind: "enrollment",
        enrollmentAction: ["START", "AGREE", "I AGREE"].includes(command) ? "start" : "other",
        retainEvent: false,
      };
    }
  }
  if (mode === "content_disabled" || mode === "paused") return { kind: "routing_only", retainEvent: false };
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

function sanitizedIgnored(
  _event: Extract<LinqWebhookEnvelope, { eventType: "linq.ignored" }>,
): StoredLinqEvent {
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

function messageText(event: LinqMessageReceivedEvent): string {
  return event.message.parts
    .flatMap((part) => (part.kind === "text" ? [part.text] : part.kind === "link" ? [part.url] : []))
    .join("\n");
}

function normalizeHandle(value: string): string {
  return value
    .trim()
    .replace(/[\s()-]/gu, "")
    .toLowerCase();
}

async function inferSharedHousehold(
  transaction: Transaction,
  personIds: readonly string[],
): Promise<{ id: string; controlEpoch: number } | null> {
  const rows = await transaction<{ id: string; control_epoch: number | string }[]>`
    select household.id, household.control_epoch
    from households household
    join household_memberships membership on membership.household_id = household.id
    where membership.person_id = any(${transaction.array([...personIds])}::uuid[])
      and membership.status = 'active' and household.status in ('onboarding', 'active', 'paused')
    group by household.id
    having count(distinct membership.person_id) = ${personIds.length}
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
