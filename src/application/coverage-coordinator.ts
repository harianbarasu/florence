import { randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { LinqMessageReceivedEvent, LinqWebhookEnvelope } from "../adapters/linq/index.js";
import type { FlorenceConfig } from "../config.js";
import type { Database } from "../db/client.js";
import {
  type ConversationAuthoritySnapshot,
  evaluateConversationMode,
  PostgresConversationAuthority,
} from "../modules/conversations/index.js";
import {
  type CoverageLoop,
  createCoverageLoop,
  PostgresCoordination,
} from "../modules/coordination/index.js";
import { EffectOutbox } from "../modules/effects/index.js";
import { PostgresSourceIntelligence } from "../modules/sources/index.js";
import type { SecretBox } from "../shared/crypto.js";
import { NotFoundError, StaleAuthorityError, UnauthorizedError } from "../shared/errors.js";
import type { CoverageProposal, ProcessReceipt } from "./contracts.js";
import { reconcileCoverageTimers } from "./coverage-timer-reconciliation.js";

type Transaction = TransactionSql<Record<string, never>>;

interface StoredCoverageLinqEvent {
  readonly schemaVersion: 1;
  readonly classification: string;
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

interface ReopenedSource {
  readonly internalProviderEventId: string;
  readonly providerEventId: string;
  readonly record: StoredCoverageLinqEvent;
  readonly event: LinqMessageReceivedEvent;
  readonly actorPersonId: string;
  readonly actorIdentityId: string;
  readonly actorControlEpoch: number;
  readonly snapshot: ConversationAuthoritySnapshot;
  readonly providerChatId: string;
  readonly providerParticipantDigest: string;
  readonly householdId: string | null;
  readonly householdControlEpoch: number | null;
  readonly householdTimeZone: string | null;
  readonly mayOriginateCoverage: boolean;
  readonly mayCoordinateCoverage: boolean;
}

interface EvidenceSet {
  readonly ids: readonly string[];
  readonly anchorSourceRevisionId: string;
  readonly accessExpiresAt: Date;
}

interface CoverageTarget {
  readonly loop: CoverageLoop;
  readonly snapshot: ConversationAuthoritySnapshot;
  readonly providerChatId: string;
  readonly providerParticipantDigest: string;
  readonly householdControlEpoch: number;
}

interface ProviderEventRow {
  readonly id: string;
  readonly provider_event_id: string;
  readonly envelope_ciphertext: Buffer;
  readonly processing_status: string;
}

interface SourceAuthorityRow {
  readonly actor_control_epoch: number | string;
  readonly household_id: string | null;
  readonly household_control_epoch: number | string | null;
  readonly household_timezone: string | null;
  readonly household_status: string | null;
  readonly may_originate_coverage: boolean;
  readonly may_coordinate_coverage: boolean;
  readonly external_channel_id: string;
  readonly latest_participant_digest: string;
}

interface CandidateLoopRow {
  readonly id: string;
  readonly household_control_epoch: number | string;
  readonly external_channel_id: string;
  readonly latest_participant_digest: string;
}

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;
const MIN_EFFECT_LIFETIME_MS = 3 * 60_000;
const MAX_EFFECT_LIFETIME_MS = 24 * 60 * 60_000;

/**
 * Deep application module for the first coverage slice. Workers propose only
 * semantic meaning; this module owns source reopening, deterministic target
 * binding, authority, persistence, effects, and timer reconciliation.
 */
export class CoverageCoordinator {
  public constructor(
    private readonly database: Database,
    private readonly config: FlorenceConfig,
    private readonly secretBox: SecretBox,
  ) {}

  public async apply(proposal: CoverageProposal): Promise<ProcessReceipt> {
    validateProposal(proposal);
    return this.database.begin(async (transaction) => {
      await transaction`
        select pg_advisory_xact_lock(
          hashtextextended(${`coverage-apply:${proposal.internalProviderEventId}`}, 0)
        )
      `;
      const source = await this.reopenSource(transaction, proposal.internalProviderEventId);
      const evidence = await this.reopenEvidence(transaction, source, proposal.evidenceSourceRevisionIds);
      return proposal.kind === "need_proposed"
        ? this.applyNeed(transaction, source, evidence, proposal)
        : this.applySelfResponse(transaction, source, evidence, proposal);
    });
  }

  private async reopenSource(
    transaction: Transaction,
    internalProviderEventId: string,
  ): Promise<ReopenedSource> {
    const rows = await transaction<ProviderEventRow[]>`
      select id, provider_event_id, envelope_ciphertext, processing_status
      from provider_events
      where id = ${internalProviderEventId} and provider = 'linq'
      for update
    `;
    const row = rows[0];
    if (row?.processing_status !== "processed") {
      throw new StaleAuthorityError("Coverage source is not a processed Linq event");
    }
    const record = JSON.parse(
      this.secretBox
        .decrypt(
          JSON.parse(row.envelope_ciphertext.toString("utf8")),
          `provider-event:${row.provider_event_id}`,
        )
        .toString("utf8"),
    ) as StoredCoverageLinqEvent;
    if (
      record.schemaVersion !== 1 ||
      record.classification !== "full" ||
      record.event?.eventType !== "linq.message.received" ||
      !record.routing.senderPersonId ||
      !record.routing.senderIdentityId
    ) {
      throw new UnauthorizedError("Coverage proposals require an authenticated full Linq message");
    }

    const authority = new PostgresConversationAuthority(transaction);
    const snapshot = await authority.snapshot(record.routing.conversationId);
    if (
      snapshot.conversationStatus !== "active" ||
      snapshot.participantEpochId !== record.routing.participantEpochId ||
      snapshot.participantSetDigest !== record.routing.appParticipantDigest ||
      snapshot.conversationKind !== record.routing.chatKind
    ) {
      throw new StaleAuthorityError("Coverage source audience changed before commit");
    }
    const sourceRows = await transaction<SourceAuthorityRow[]>`
      select person.control_epoch as actor_control_epoch,
        conversation.household_id, household.control_epoch as household_control_epoch,
        household.timezone as household_timezone, household.status as household_status,
        exists(
          select 1 from household_memberships membership
          join membership_capabilities grant_row on grant_row.membership_id = membership.id
            and grant_row.capability = 'coordination.originate' and grant_row.status = 'active'
          where membership.household_id = conversation.household_id
            and membership.person_id = person.id and membership.status = 'active'
        ) as may_originate_coverage,
        exists(
          select 1 from household_memberships membership
          join membership_capabilities grant_row on grant_row.membership_id = membership.id
            and grant_row.capability = 'coordination.coordinate' and grant_row.status = 'active'
          where membership.household_id = conversation.household_id
            and membership.person_id = person.id and membership.status = 'active'
        ) as may_coordinate_coverage,
        channel.external_channel_id, channel.latest_participant_digest
      from conversations conversation
      join participant_epochs epoch on epoch.id = conversation.current_epoch_id
        and epoch.id = ${record.routing.participantEpochId} and epoch.ended_at is null
        and epoch.participant_set_digest = ${record.routing.appParticipantDigest}
      join epoch_participants participant on participant.participant_epoch_id = epoch.id
        and participant.person_identity_id = ${record.routing.senderIdentityId}
        and participant.person_id = ${record.routing.senderPersonId}
        and participant.registration_status = 'registered' and participant.consented_at is not null
      join person_identities identity on identity.id = participant.person_identity_id
        and identity.person_id = participant.person_id and identity.status = 'verified'
      join people person on person.id = participant.person_id and person.status = 'registered'
      join participant_policies policy on policy.conversation_id = conversation.id
        and policy.person_id = person.id and policy.status = 'active'
        and policy.allow_content_processing
      join conversation_channels channel on channel.conversation_id = conversation.id
        and channel.provider = 'linq' and channel.status = 'active'
        and channel.external_channel_id = ${record.routing.providerChatId}
        and channel.latest_participant_digest is not null
      left join households household on household.id = conversation.household_id
      where conversation.id = ${record.routing.conversationId}
        and conversation.status = 'active'
      limit 1
    `;
    const source = sourceRows[0];
    if (!source) throw new StaleAuthorityError("Coverage actor is no longer authenticated in this chat");
    if (
      source.external_channel_id !== record.routing.providerChatId ||
      source.latest_participant_digest !== record.routing.providerParticipantDigest
    ) {
      throw new StaleAuthorityError("Coverage source provider audience changed before commit");
    }
    if (
      source.household_id !== null &&
      (source.household_status === "paused" ||
        source.household_status === "deletion_fenced" ||
        source.household_status === "deleted")
    ) {
      throw new UnauthorizedError("Coverage household is not active");
    }
    return {
      internalProviderEventId: row.id,
      providerEventId: row.provider_event_id,
      record,
      event: record.event,
      actorPersonId: record.routing.senderPersonId,
      actorIdentityId: record.routing.senderIdentityId,
      actorControlEpoch: Number(source.actor_control_epoch),
      snapshot,
      providerChatId: source.external_channel_id,
      providerParticipantDigest: source.latest_participant_digest,
      householdId: source.household_id,
      householdControlEpoch:
        source.household_control_epoch === null ? null : Number(source.household_control_epoch),
      householdTimeZone: source.household_timezone,
      mayOriginateCoverage: source.may_originate_coverage,
      mayCoordinateCoverage: source.may_coordinate_coverage,
    };
  }

  private async reopenEvidence(
    transaction: Transaction,
    source: ReopenedSource,
    evidenceCandidates: readonly string[],
  ): Promise<EvidenceSet> {
    const evidenceIds = exactEvidenceIds(evidenceCandidates);
    const metadata = await transaction<
      {
        readonly id: string;
        readonly provider: string;
        readonly external_object_id: string;
        readonly object_kind: string;
      }[]
    >`
      select revision.id, object.provider, object.external_object_id, object.object_kind
      from source_revisions revision
      join source_objects object on object.id = revision.source_object_id
        and object.status = 'active' and object.latest_revision_number = revision.revision_number
      where revision.id = any(${transaction.array([...evidenceIds])}::uuid[])
        and revision.revoked_at is null and revision.content_ciphertext is not null
    `;
    if (metadata.length !== evidenceIds.length) {
      throw new StaleAuthorityError("Coverage evidence is incomplete or no longer current");
    }
    const anchors = metadata.filter(
      (entry) =>
        entry.provider === "linq" &&
        entry.external_object_id === source.event.message.providerMessageId &&
        entry.object_kind === "conversation_message",
    );
    if (anchors.length !== 1) {
      throw new UnauthorizedError("Coverage evidence must include the exact triggering message");
    }
    const anchor = anchors[0];
    if (!anchor) throw new UnauthorizedError("Coverage evidence anchor disappeared");
    const anchorSourceRevisionId = anchor.id;
    const sources = new PostgresSourceIntelligence(transaction, this.secretBox, {
      rawRetentionDays: this.config.defaults.rawSourceRetentionDays,
      privateCandidateRetentionDays: 7,
    });
    const asOf = new Date();
    let accessExpiresAt = new Date(asOf.getTime() + MAX_EFFECT_LIFETIME_MS);
    for (const metadataEntry of metadata) {
      const read = await sources
        .read({
          kind: "source_revision",
          sourceRevisionId: metadataEntry.id,
          scope:
            source.record.routing.chatKind === "group"
              ? {
                  kind: "conversation_epoch",
                  participantEpochId: source.record.routing.participantEpochId,
                }
              : { kind: "person", personId: source.actorPersonId },
          asOf: asOf.toISOString(),
        })
        .catch((error: unknown) => {
          if (error instanceof NotFoundError || error instanceof UnauthorizedError) {
            throw new StaleAuthorityError("Coverage evidence authority changed before commit");
          }
          throw error;
        });
      if (read.kind !== "source_revision") {
        throw new UnauthorizedError("Coverage evidence is not an exact source revision");
      }
      if (metadataEntry.id !== anchorSourceRevisionId) {
        if (
          metadataEntry.provider !== "linq.attachment" ||
          metadataEntry.object_kind !== "attachment_manifest" ||
          read.content.parentSourceRevisionId !== anchorSourceRevisionId
        ) {
          throw new UnauthorizedError("Coverage evidence includes an unrelated source revision");
        }
      }
      const revisionExpiry = new Date(read.accessExpiresAt);
      if (revisionExpiry < accessExpiresAt) accessExpiresAt = revisionExpiry;
    }
    if (accessExpiresAt <= new Date(asOf.getTime() + MIN_EFFECT_LIFETIME_MS)) {
      throw new StaleAuthorityError("Coverage evidence is too close to expiry to act on");
    }
    return { ids: evidenceIds, anchorSourceRevisionId, accessExpiresAt };
  }

  private async applyNeed(
    transaction: Transaction,
    source: ReopenedSource,
    evidence: EvidenceSet,
    proposal: Extract<CoverageProposal, { kind: "need_proposed" }>,
  ): Promise<ProcessReceipt> {
    if (
      source.record.routing.chatKind !== "group" ||
      !source.householdId ||
      !source.householdControlEpoch ||
      !source.householdTimeZone ||
      !source.mayOriginateCoverage
    ) {
      throw new UnauthorizedError("This coverage slice requires an authorized family group");
    }
    await this.requireExactHouseholdGroup(transaction, source);
    if (new Date(proposal.timing.lastResponsibleAt) <= new Date(Date.now() + MIN_EFFECT_LIFETIME_MS)) {
      throw new StaleAuthorityError("The proposed coverage window is too close to act on safely");
    }
    if (proposal.proposedHolderPersonId) {
      await this.requireEligibleProposedHolder(transaction, source, proposal.proposedHolderPersonId);
    }
    await this.requireSendAuthorization(transaction, source.snapshot, "proactive", "proactive_coverage");

    const existing = await transaction<{ readonly id: string; readonly state: string }[]>`
      select id, state from coverage_loops
      where source_evidence_refs @> ${transaction.json([evidence.anchorSourceRevisionId])}
      order by created_at, id
      limit 2
    `;
    if (existing.length > 1) {
      throw new StaleAuthorityError("Triggering evidence already maps to multiple coverage loops");
    }
    if (existing[0]) {
      return {
        accepted: true,
        duplicate: true,
        disposition: `coverage_already_${existing[0].state}`,
        ids: {
          providerEventId: source.internalProviderEventId,
          coverageLoopId: existing[0].id,
        },
      };
    }

    const now = new Date();
    const coordination = new PostgresCoordination(transaction, this.secretBox);
    let loop = await coordination.create(
      createCoverageLoop({
        loopId: randomUUID(),
        householdId: source.householdId,
        minimumSharedMeaning: proposal.minimumSharedMeaning,
        unresolvedFacts: uniqueTrimmed(proposal.unresolvedFacts),
        proposedHolderPersonId: proposal.proposedHolderPersonId,
        timing: proposal.timing,
        planVersion: 1,
        notificationMode: "always",
        destination: {
          conversationId: source.record.routing.conversationId,
          participantEpochId: source.record.routing.participantEpochId,
          participantSetDigest: source.record.routing.appParticipantDigest,
          audience: "group",
        },
        sourceEvidenceRefs: [...evidence.ids],
        occurredAt: now.toISOString(),
      }),
    );
    if (loop.state === "open" && proposal.proposedHolderPersonId) {
      loop = (
        await coordination.transition({
          loopId: loop.loopId,
          command: {
            kind: "request_coverage",
            transitionId: randomUUID(),
            expectedVersion: loop.version,
            actorPersonId: source.actorPersonId,
            requestedPersonId: proposal.proposedHolderPersonId,
            occurredAt: now.toISOString(),
            evidenceRefs: [...evidence.ids],
          },
        })
      ).loop;
    }
    const proposedHolderLabel = proposal.proposedHolderPersonId
      ? await this.openPersonLabel(transaction, proposal.proposedHolderPersonId)
      : null;
    const message = coverageOpeningText(loop, proposedHolderLabel, proposal.consequentialQuestion);
    const queued = await this.queueDestinationMessage(transaction, {
      source,
      snapshot: source.snapshot,
      providerChatId: source.providerChatId,
      providerParticipantDigest: source.providerParticipantDigest,
      householdId: source.householdId,
      householdControlEpoch: source.householdControlEpoch,
      loop,
      text: message,
      sendKind: "proactive",
      operation: "proactive_coverage",
      idempotencyKey: `coverage:${loop.loopId}:v${loop.version}:open`,
      authorizationExpiresAt: effectExpiry(now, evidence.accessExpiresAt, loop.timing.lastResponsibleAt),
      evidenceIds: evidence.ids,
    });
    const timerId = await reconcileCoverageTimers({
      transaction,
      loop,
      snapshot: source.snapshot,
      now,
      allowReminder: true,
    });
    return {
      accepted: true,
      duplicate: !queued.created,
      disposition: `coverage_${loop.state}_queued`,
      ids: {
        providerEventId: source.internalProviderEventId,
        coverageLoopId: loop.loopId,
        outboxId: queued.outboxId,
        ...(timerId ? { timerId } : {}),
      },
    };
  }

  private async applySelfResponse(
    transaction: Transaction,
    source: ReopenedSource,
    evidence: EvidenceSet,
    proposal: Extract<CoverageProposal, { kind: "self_response_proposed" }>,
  ): Promise<ProcessReceipt> {
    if (proposal.response !== "ambiguous" && (!proposal.explicitSelfStatement || proposal.confidence < 0.8)) {
      throw new UnauthorizedError("Coverage state requires a high-confidence explicit self statement");
    }
    if (source.record.routing.chatKind === "group" && !source.mayCoordinateCoverage) {
      throw new UnauthorizedError("This person cannot coordinate coverage for the family");
    }

    const priorClarifications = await transaction<
      { readonly id: string; readonly coverage_loop_id: string | null }[]
    >`
      select id, coverage_loop_id from outbox
      where idempotency_key = ${`coverage:clarify:${source.internalProviderEventId}`}
      limit 1
    `;
    const priorClarification = priorClarifications[0];
    if (priorClarification) {
      return {
        accepted: true,
        duplicate: true,
        disposition: "coverage_response_clarification_queued",
        ids: {
          providerEventId: source.internalProviderEventId,
          outboxId: priorClarification.id,
          ...(priorClarification.coverage_loop_id
            ? { coverageLoopId: priorClarification.coverage_loop_id }
            : {}),
        },
      };
    }

    const priorTransitions = await transaction<
      { readonly coverage_loop_id: string; readonly transition_kind: string }[]
    >`
      select distinct coverage_loop_id, transition_kind
      from coverage_transitions
      where actor_person_id = ${source.actorPersonId}
        and evidence_refs @> ${transaction.json([evidence.anchorSourceRevisionId])}
      order by coverage_loop_id
    `;
    if (priorTransitions.length > 0) {
      const loopIds = [...new Set(priorTransitions.map((entry) => entry.coverage_loop_id))];
      if (loopIds.length !== 1) {
        throw new StaleAuthorityError("One response message already changed multiple coverage loops");
      }
      const appliedLoopId = loopIds[0];
      if (!appliedLoopId) throw new StaleAuthorityError("Applied coverage loop disappeared");
      return {
        accepted: true,
        duplicate: true,
        disposition: "coverage_response_already_applied",
        ids: {
          providerEventId: source.internalProviderEventId,
          coverageLoopId: appliedLoopId,
        },
      };
    }

    const replyTargetId = await this.loadProvenReplyTarget(transaction, source);
    const candidates = replyTargetId
      ? await this.loadExactTarget(transaction, source, proposal.response, replyTargetId)
      : source.event.message.replyTo
        ? []
        : await this.loadUniqueEligibleTargets(transaction, source, proposal.response);
    if (proposal.response === "ambiguous" || candidates.length !== 1) {
      return this.queueClarification(transaction, source, candidates, replyTargetId);
    }
    const target = candidates[0];
    if (!target) return this.queueClarification(transaction, source, [], replyTargetId);
    const coordination = new PostgresCoordination(transaction, this.secretBox);
    let loop = await coordination.loadForUpdate(target.loop.loopId);
    if (!loop || loop.version !== target.loop.version) {
      throw new StaleAuthorityError("Coverage target changed before response commit");
    }
    const respondedAt = new Date(source.event.message.sentAt);
    if (
      !Number.isFinite(respondedAt.getTime()) ||
      respondedAt < new Date(loop.lastTransitionAt) ||
      respondedAt > new Date(loop.timing.lastResponsibleAt)
    ) {
      throw new StaleAuthorityError("Coverage response falls outside the current decision window");
    }
    const processedAt = new Date();

    if (
      proposal.response === "acknowledge" &&
      loop.proposedHolderPersonId === null &&
      (loop.state === "open" || loop.state === "at_risk")
    ) {
      loop = (
        await coordination.transition({
          loopId: loop.loopId,
          command: {
            kind: "request_coverage",
            transitionId: randomUUID(),
            expectedVersion: loop.version,
            actorPersonId: source.actorPersonId,
            requestedPersonId: source.actorPersonId,
            occurredAt: respondedAt.toISOString(),
            evidenceRefs: [...evidence.ids],
          },
        })
      ).loop;
    }

    const holderWithdrew =
      proposal.response === "decline" &&
      loop.state === "covered" &&
      loop.acknowledgment?.personId === source.actorPersonId;
    const transition = await coordination.transition({
      loopId: loop.loopId,
      command: holderWithdrew
        ? {
            kind: "record_risk",
            transitionId: randomUUID(),
            expectedVersion: loop.version,
            actorPersonId: source.actorPersonId,
            occurredAt: respondedAt.toISOString(),
            proposedHolderPersonId: null,
            evidenceRefs: [...evidence.ids],
          }
        : proposal.response === "acknowledge"
          ? {
              kind: "acknowledge_coverage",
              transitionId: randomUUID(),
              expectedVersion: loop.version,
              actorPersonId: source.actorPersonId,
              acknowledgment: "explicit_self",
              visibility:
                source.record.routing.chatKind === "group" &&
                source.record.routing.conversationId === loop.destination.conversationId
                  ? "shared"
                  : "private",
              occurredAt: respondedAt.toISOString(),
              evidenceRefs: [...evidence.ids],
            }
          : {
              kind: "decline_coverage",
              transitionId: randomUUID(),
              expectedVersion: loop.version,
              actorPersonId: source.actorPersonId,
              visibility: "private",
              occurredAt: respondedAt.toISOString(),
              evidenceRefs: [...evidence.ids],
            },
    });
    const operation = proposal.response === "acknowledge" ? "coverage_closure" : "coverage_state_change";
    const sameConversation =
      source.record.routing.conversationId === transition.loop.destination.conversationId;
    const queued = await this.queueDestinationMessage(transaction, {
      source,
      snapshot: target.snapshot,
      providerChatId: target.providerChatId,
      providerParticipantDigest: target.providerParticipantDigest,
      householdId: transition.loop.householdId,
      householdControlEpoch: target.householdControlEpoch,
      loop: transition.loop,
      text: neutralTransitionText(transition.loop, holderWithdrew),
      sendKind: sameConversation ? "direct_response" : "proactive",
      operation,
      idempotencyKey: `coverage:${transition.loop.loopId}:v${transition.loop.version}:${proposal.response}`,
      authorizationExpiresAt: effectExpiry(
        processedAt,
        evidence.accessExpiresAt,
        proposal.response === "decline" ? transition.loop.timing.lastResponsibleAt : null,
      ),
      evidenceIds: evidence.ids,
    });
    const timerId = await reconcileCoverageTimers({
      transaction,
      loop: transition.loop,
      snapshot: target.snapshot,
      now: processedAt,
      allowReminder: true,
    });
    return {
      accepted: true,
      duplicate: !queued.created,
      disposition: holderWithdrew
        ? "coverage_holder_withdrew_privately"
        : proposal.response === "acknowledge"
          ? "coverage_acknowledged"
          : "coverage_declined",
      ids: {
        providerEventId: source.internalProviderEventId,
        coverageLoopId: transition.loop.loopId,
        outboxId: queued.outboxId,
        ...(timerId ? { timerId } : {}),
      },
    };
  }

  private async loadProvenReplyTarget(
    transaction: Transaction,
    source: ReopenedSource,
  ): Promise<string | null> {
    const providerMessageId = source.event.message.replyTo?.providerMessageId;
    if (!providerMessageId) return null;
    const rows = await transaction<{ readonly coverage_loop_id: string }[]>`
      select distinct effect.coverage_loop_id
      from effect_receipts receipt
      join outbox effect on effect.id = receipt.outbox_id
      where receipt.provider_receipt_id = ${providerMessageId}
        and receipt.status in ('submitted', 'confirmed')
        and effect.status in ('submitted', 'confirmed')
        and effect.effect_kind = 'linq.message'
        and effect.conversation_id = ${source.record.routing.conversationId}
        and effect.participant_epoch_id = ${source.record.routing.participantEpochId}
        and effect.expected_participant_digest = ${source.record.routing.appParticipantDigest}
        and effect.coverage_loop_id is not null
        and not exists(
          select 1 from effect_receipts terminal
          where terminal.outbox_id = effect.id and terminal.status in ('failed', 'ambiguous')
        )
      order by effect.coverage_loop_id
      limit 2
    `;
    return rows.length === 1 ? (rows[0]?.coverage_loop_id ?? null) : null;
  }

  private async loadExactTarget(
    transaction: Transaction,
    source: ReopenedSource,
    response: Extract<CoverageProposal, { kind: "self_response_proposed" }>["response"],
    loopId: string,
  ): Promise<readonly CoverageTarget[]> {
    const candidate = await this.loadTarget(transaction, source, loopId);
    return candidate && responseEligible(candidate.loop, source.actorPersonId, response) ? [candidate] : [];
  }

  private async loadUniqueEligibleTargets(
    transaction: Transaction,
    source: ReopenedSource,
    response: Extract<CoverageProposal, { kind: "self_response_proposed" }>["response"],
  ): Promise<readonly CoverageTarget[]> {
    const rows = await transaction<CandidateLoopRow[]>`
      select loop.id, household.control_epoch as household_control_epoch,
        channel.external_channel_id, channel.latest_participant_digest
      from coverage_loops loop
      join households household on household.id = loop.household_id
        and household.status in ('onboarding', 'active')
      join household_memberships membership on membership.household_id = loop.household_id
        and membership.person_id = ${source.actorPersonId} and membership.status = 'active'
      join membership_capabilities grant_row on grant_row.membership_id = membership.id
        and grant_row.capability = 'coordination.coordinate' and grant_row.status = 'active'
      join conversations destination on destination.id = loop.destination_conversation_id
        and destination.household_id = loop.household_id
        and destination.kind = 'group' and destination.status = 'active'
      join participant_epochs epoch on epoch.id = destination.current_epoch_id
        and epoch.id = loop.participant_epoch_id and epoch.ended_at is null
        and epoch.participant_set_digest = loop.participant_set_digest
      join epoch_participants participant on participant.participant_epoch_id = epoch.id
        and participant.person_id = ${source.actorPersonId}
        and participant.registration_status = 'registered' and participant.consented_at is not null
      join conversation_channels channel on channel.conversation_id = destination.id
        and channel.provider = 'linq' and channel.status = 'active'
        and channel.latest_participant_digest is not null
      where loop.state in ('open', 'awaiting_response', 'covered', 'at_risk')
        and (
          (${source.record.routing.chatKind === "group"}
            and destination.id = ${source.record.routing.conversationId}
            and epoch.id = ${source.record.routing.participantEpochId}
            and epoch.participant_set_digest = ${source.record.routing.appParticipantDigest})
          or (${source.record.routing.chatKind === "direct"})
        )
      order by loop.last_transition_at desc, loop.id
      limit 9
    `;
    const eligible: CoverageTarget[] = [];
    for (const row of rows) {
      const candidate = await this.loadTarget(transaction, source, row.id, row);
      if (candidate && responseEligible(candidate.loop, source.actorPersonId, response)) {
        eligible.push(candidate);
      }
      if (eligible.length > 1) break;
    }
    return eligible;
  }

  private async loadTarget(
    transaction: Transaction,
    source: ReopenedSource,
    loopId: string,
    known?: CandidateLoopRow,
  ): Promise<CoverageTarget | null> {
    const rows = known
      ? [known]
      : await transaction<CandidateLoopRow[]>`
          select loop.id, household.control_epoch as household_control_epoch,
            channel.external_channel_id, channel.latest_participant_digest
          from coverage_loops loop
          join households household on household.id = loop.household_id
            and household.status in ('onboarding', 'active')
          join household_memberships membership on membership.household_id = loop.household_id
            and membership.person_id = ${source.actorPersonId} and membership.status = 'active'
          join membership_capabilities grant_row on grant_row.membership_id = membership.id
            and grant_row.capability = 'coordination.coordinate' and grant_row.status = 'active'
          join conversations destination on destination.id = loop.destination_conversation_id
            and destination.household_id = loop.household_id
            and destination.kind = 'group' and destination.status = 'active'
          join participant_epochs epoch on epoch.id = destination.current_epoch_id
            and epoch.id = loop.participant_epoch_id and epoch.ended_at is null
            and epoch.participant_set_digest = loop.participant_set_digest
          join epoch_participants participant on participant.participant_epoch_id = epoch.id
            and participant.person_id = ${source.actorPersonId}
            and participant.registration_status = 'registered' and participant.consented_at is not null
          join conversation_channels channel on channel.conversation_id = destination.id
            and channel.provider = 'linq' and channel.status = 'active'
            and channel.latest_participant_digest is not null
          where loop.id = ${loopId}
          limit 1
        `;
    const row = rows[0];
    if (!row) return null;
    const loop = await new PostgresCoordination(transaction, this.secretBox).loadForUpdate(row.id);
    if (!loop) return null;
    if (
      source.record.routing.chatKind === "group" &&
      (loop.destination.conversationId !== source.record.routing.conversationId ||
        loop.destination.participantEpochId !== source.record.routing.participantEpochId ||
        loop.destination.participantSetDigest !== source.record.routing.appParticipantDigest)
    ) {
      return null;
    }
    const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
      loop.destination.conversationId,
    );
    if (
      snapshot.conversationStatus !== "active" ||
      snapshot.conversationKind !== "group" ||
      snapshot.participantEpochId !== loop.destination.participantEpochId ||
      snapshot.participantSetDigest !== loop.destination.participantSetDigest ||
      evaluateConversationMode(snapshot) !== "trusted_write_enabled"
    ) {
      return null;
    }
    return {
      loop,
      snapshot,
      providerChatId: row.external_channel_id,
      providerParticipantDigest: row.latest_participant_digest,
      householdControlEpoch: Number(row.household_control_epoch),
    };
  }

  private async queueClarification(
    transaction: Transaction,
    source: ReopenedSource,
    candidates: readonly CoverageTarget[],
    replyTargetId: string | null,
  ): Promise<ProcessReceipt> {
    const onlyCandidate = candidates.length === 1 ? candidates[0] : null;
    const text = onlyCandidate
      ? "Just to be sure, should I record that you can handle the coverage item, or that you can’t?"
      : candidates.length > 1
        ? "I found more than one current coverage item. Please reply directly to the Florence message about the one you mean."
        : "I couldn’t safely match that to one current coverage item. Please reply directly to the Florence coverage message and say whether you can take it.";
    const authorization = await this.requireSendAuthorization(
      transaction,
      source.snapshot,
      "direct_response",
      "coverage_coordination",
    );
    const queued = await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
      actorPersonId: source.actorPersonId,
      person: { id: source.actorPersonId, controlEpoch: source.actorControlEpoch },
      ...(onlyCandidate
        ? {
            household: {
              id: onlyCandidate.loop.householdId,
              controlEpoch: onlyCandidate.householdControlEpoch,
            },
          }
        : source.householdId && source.householdControlEpoch
          ? {
              household: {
                id: source.householdId,
                controlEpoch: source.householdControlEpoch,
              },
            }
          : {}),
      conversation: {
        id: source.record.routing.conversationId,
        authorityVersion: source.snapshot.authorityVersion,
      },
      participantEpochId: source.record.routing.participantEpochId,
      expectedParticipantDigest: source.record.routing.appParticipantDigest,
      ...(onlyCandidate
        ? {
            coverageLoop: {
              id: onlyCandidate.loop.loopId,
              version: onlyCandidate.loop.version,
            },
          }
        : {}),
      effectKind: "linq.message",
      idempotencyKey: `coverage:clarify:${source.internalProviderEventId}`,
      data: {
        candidateLoopIds: candidates.map((candidate) => candidate.loop.loopId),
        replyTargetProven: replyTargetId !== null,
      },
      policy: {
        operation: "coverage_coordination",
        sendKind: "direct_response",
        failClosed: true,
      },
      target: {
        providerChatId: source.providerChatId,
        participantEpochId: source.record.routing.participantEpochId,
      },
      payload: {
        providerChatId: source.providerChatId,
        expectedProviderParticipantDigest: source.providerParticipantDigest,
        text,
      },
      reasonCodes: ["coverage_response_ambiguous", authorization.reason],
      authorizationExpiresAt: new Date(Date.now() + 5 * 60_000),
    });
    return {
      accepted: true,
      duplicate: !queued.created,
      disposition: "coverage_response_clarification_queued",
      ids: {
        providerEventId: source.internalProviderEventId,
        outboxId: queued.outboxId,
        ...(onlyCandidate ? { coverageLoopId: onlyCandidate.loop.loopId } : {}),
      },
    };
  }

  private async requireExactHouseholdGroup(transaction: Transaction, source: ReopenedSource): Promise<void> {
    const rows = await transaction<{ readonly exact_household_group: boolean }[]>`
      select count(*) = count(membership.id) as exact_household_group
      from epoch_participants participant
      left join household_memberships membership
        on membership.household_id = ${source.householdId}
        and membership.person_id = participant.person_id
        and membership.status = 'active'
      where participant.participant_epoch_id = ${source.record.routing.participantEpochId}
        and participant.registration_status = 'registered'
        and participant.consented_at is not null
    `;
    if (rows[0]?.exact_household_group !== true) {
      throw new UnauthorizedError("Every current group participant must belong to this family");
    }
  }

  private async requireEligibleProposedHolder(
    transaction: Transaction,
    source: ReopenedSource,
    proposedHolderPersonId: string,
  ): Promise<void> {
    const rows = await transaction<{ readonly eligible: boolean }[]>`
      select exists(
        select 1 from epoch_participants participant
        join people person on person.id = participant.person_id and person.status = 'registered'
        join household_memberships membership on membership.person_id = participant.person_id
          and membership.household_id = ${source.householdId} and membership.status = 'active'
        join membership_capabilities grant_row on grant_row.membership_id = membership.id
          and grant_row.capability = 'coordination.coordinate' and grant_row.status = 'active'
        where participant.participant_epoch_id = ${source.record.routing.participantEpochId}
          and participant.person_id = ${proposedHolderPersonId}
          and participant.registration_status = 'registered' and participant.consented_at is not null
      ) as eligible
    `;
    if (rows[0]?.eligible !== true) {
      throw new UnauthorizedError("Proposed holder is not eligible in this exact family group");
    }
  }

  private async requireSendAuthorization(
    transaction: Transaction,
    snapshot: ConversationAuthoritySnapshot,
    sendKind: "direct_response" | "proactive",
    operation: string,
  ) {
    if (!snapshot.participantEpochId || !snapshot.participantSetDigest) {
      throw new StaleAuthorityError("Coverage destination has no current participant epoch");
    }
    const ruleId =
      snapshot.conversationKind === "group"
        ? (snapshot.rules.find(
            (rule) =>
              rule.active &&
              rule.participantSetDigest === snapshot.participantSetDigest &&
              rule.allowedOperations.includes(operation),
          )?.ruleId ?? null)
        : null;
    const authorization = await new PostgresConversationAuthority(transaction).authorizeSend({
      conversationId: snapshot.conversationId,
      expectedParticipantEpochId: snapshot.participantEpochId,
      expectedParticipantSetDigest: snapshot.participantSetDigest,
      liveParticipantIdentityIds: snapshot.participants.map((participant) => participant.personIdentityId),
      sendKind,
      operation,
      ruleId,
    });
    if (!authorization.allowed) {
      throw new UnauthorizedError(`Coverage destination is not writable: ${authorization.reason}`);
    }
    return authorization;
  }

  private async queueDestinationMessage(
    transaction: Transaction,
    input: {
      readonly source: ReopenedSource;
      readonly snapshot: ConversationAuthoritySnapshot;
      readonly providerChatId: string;
      readonly providerParticipantDigest: string;
      readonly householdId: string;
      readonly householdControlEpoch: number;
      readonly loop: CoverageLoop;
      readonly text: string;
      readonly sendKind: "direct_response" | "proactive";
      readonly operation: string;
      readonly idempotencyKey: string;
      readonly authorizationExpiresAt: Date;
      readonly evidenceIds: readonly string[];
    },
  ) {
    const authorization = await this.requireSendAuthorization(
      transaction,
      input.snapshot,
      input.sendKind,
      input.operation,
    );
    return new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
      actorPersonId: input.source.actorPersonId,
      person: {
        id: input.source.actorPersonId,
        controlEpoch: input.source.actorControlEpoch,
      },
      household: {
        id: input.householdId,
        controlEpoch: input.householdControlEpoch,
      },
      conversation: {
        id: input.snapshot.conversationId,
        authorityVersion: input.snapshot.authorityVersion,
      },
      sourceConversation: {
        id: input.source.record.routing.conversationId,
        authorityVersion: input.source.snapshot.authorityVersion,
        participantEpochId: input.source.record.routing.participantEpochId,
        participantSetDigest: input.source.record.routing.appParticipantDigest,
      },
      evidenceSourceRevisionIds: [...input.evidenceIds],
      participantEpochId: input.loop.destination.participantEpochId,
      expectedParticipantDigest: input.loop.destination.participantSetDigest,
      coverageLoop: { id: input.loop.loopId, version: input.loop.version },
      effectKind: "linq.message",
      idempotencyKey: input.idempotencyKey,
      data: {
        minimumSharedMeaning: input.loop.minimumSharedMeaning,
        sourceEvidenceRevisionIds: [...input.evidenceIds],
      },
      policy: {
        operation: input.operation,
        sendKind: input.sendKind,
        exactParticipantEpoch: true,
      },
      target: {
        providerChatId: input.providerChatId,
        participantEpochId: input.loop.destination.participantEpochId,
      },
      payload: {
        providerChatId: input.providerChatId,
        expectedProviderParticipantDigest: input.providerParticipantDigest,
        text: input.text,
      },
      reasonCodes: ["current_conversation_authority", input.operation, authorization.reason],
      authorizationExpiresAt: input.authorizationExpiresAt,
    });
  }

  private async openPersonLabel(transaction: Transaction, personId: string): Promise<string | null> {
    const rows = await transaction<{ readonly display_name_ciphertext: Buffer | null }[]>`
      select display_name_ciphertext from people
      where id = ${personId} and status = 'registered'
    `;
    const ciphertext = rows[0]?.display_name_ciphertext;
    if (!ciphertext) return null;
    try {
      const label = this.secretBox
        .decrypt(JSON.parse(ciphertext.toString("utf8")), `person-display-name:${personId}`)
        .toString("utf8")
        .trim();
      return label || null;
    } catch {
      return null;
    }
  }
}

function validateProposal(proposal: CoverageProposal): void {
  assertUuid(proposal.internalProviderEventId, "Coverage provider event ID");
  exactEvidenceIds(proposal.evidenceSourceRevisionIds);
  if (proposal.kind === "need_proposed") {
    if (!proposal.minimumSharedMeaning.trim() || proposal.minimumSharedMeaning.trim().length > 500) {
      throw new UnauthorizedError("Coverage minimum meaning is outside the allowed bounds");
    }
    if (proposal.unresolvedFacts.length > 20) {
      throw new UnauthorizedError("Coverage proposal has too many unresolved facts");
    }
    for (const fact of proposal.unresolvedFacts) {
      const normalized = fact.trim();
      if (!normalized || fact.length > 300 || normalized.length > 300) {
        throw new UnauthorizedError("Coverage unresolved fact is outside the allowed bounds");
      }
    }
    if (proposal.proposedHolderPersonId) {
      assertUuid(proposal.proposedHolderPersonId, "Coverage proposed holder");
    }
    if (proposal.consequentialQuestion && proposal.consequentialQuestion.trim().length > 500) {
      throw new UnauthorizedError("Coverage consequential question is outside the allowed bounds");
    }
    return;
  }
  if (!Number.isFinite(proposal.confidence) || proposal.confidence < 0 || proposal.confidence > 1) {
    throw new UnauthorizedError("Coverage response confidence must be between zero and one");
  }
}

function exactEvidenceIds(candidates: readonly string[]): readonly string[] {
  const ids = [...new Set(candidates)];
  if (ids.length === 0 || ids.length > 32 || ids.length !== candidates.length) {
    throw new UnauthorizedError("Coverage evidence must contain 1–32 distinct source revisions");
  }
  for (const id of ids) assertUuid(id, "Coverage source revision ID");
  return ids.sort();
}

function assertUuid(value: string, label: string): void {
  if (!UUID_PATTERN.test(value)) throw new UnauthorizedError(`${label} is not a UUID`);
}

function uniqueTrimmed(values: readonly string[]): string[] {
  const seen = new Set<string>();
  const output: string[] = [];
  for (const candidate of values) {
    const value = candidate.trim();
    const key = value.toLocaleLowerCase("en-US");
    if (!value || value.length > 300 || seen.has(key)) continue;
    seen.add(key);
    output.push(value);
  }
  return output;
}

function responseEligible(
  loop: CoverageLoop,
  actorPersonId: string,
  response: Extract<CoverageProposal, { kind: "self_response_proposed" }>["response"],
): boolean {
  const canAcknowledge =
    ((loop.state === "awaiting_response" || loop.state === "at_risk") &&
      loop.proposedHolderPersonId === actorPersonId) ||
    ((loop.state === "open" || loop.state === "at_risk") && loop.proposedHolderPersonId === null);
  const canDecline =
    (loop.state === "awaiting_response" && loop.proposedHolderPersonId === actorPersonId) ||
    (loop.state === "covered" && loop.acknowledgment?.personId === actorPersonId);
  return response === "acknowledge"
    ? canAcknowledge
    : response === "decline"
      ? canDecline
      : canAcknowledge || canDecline;
}

function coverageOpeningText(
  loop: CoverageLoop,
  proposedHolderLabel: string | null,
  consequentialQuestion: string | null,
): string {
  if (loop.state === "provisional") {
    const question =
      consequentialQuestion?.trim() ||
      (loop.unresolvedFacts[0]
        ? `What should I use for ${sentenceFragment(loop.unresolvedFacts[0])}?`
        : "What detail am I missing?");
    return `I have “${sentenceFragment(loop.minimumSharedMeaning)}” open, but I need one detail. ${question}`;
  }
  if (loop.state === "awaiting_response") {
    return proposedHolderLabel
      ? `${proposedHolderLabel}, can you take ${sentenceFragment(loop.minimumSharedMeaning)}?`
      : `Can the person taking “${sentenceFragment(loop.minimumSharedMeaning)}” confirm it?`;
  }
  return `“${sentenceFragment(loop.minimumSharedMeaning)}” is still uncovered. Who can take it?`;
}

function neutralTransitionText(loop: CoverageLoop, holderWithdrew: boolean): string {
  const meaning = sentenceFragment(loop.minimumSharedMeaning);
  if (loop.state === "covered") return `${meaning}. Coverage is recorded.`;
  if (holderWithdrew || loop.state === "at_risk") {
    return `${meaning}. Coverage needs confirmation again. Is it handled, or should we find someone?`;
  }
  return `${meaning}. Coverage is still open. Is it handled, or should we find someone?`;
}

function sentenceFragment(value: string): string {
  return value.trim().replace(/[.!?]+$/gu, "") || "this coverage item";
}

function effectExpiry(now: Date, evidenceExpiry: Date, hardBoundary: string | null = null): Date {
  const expiresAt = new Date(
    Math.min(
      evidenceExpiry.getTime(),
      now.getTime() + MAX_EFFECT_LIFETIME_MS,
      hardBoundary ? new Date(hardBoundary).getTime() : Number.POSITIVE_INFINITY,
    ),
  );
  if (expiresAt <= new Date(now.getTime() + MIN_EFFECT_LIFETIME_MS)) {
    throw new StaleAuthorityError("Coverage effect authority or useful window is too close to expiry");
  }
  return expiresAt;
}
