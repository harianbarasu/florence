import { createHash } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { FlorenceConfig } from "../config.js";
import type { Database } from "../db/client.js";
import { PostgresWebAuth } from "../modules/auth/index.js";
import { PostgresConversationAuthority } from "../modules/conversations/index.js";
import { EffectOutbox } from "../modules/effects/index.js";
import type { IntegrationAccountKind, IntegrationCapability } from "../modules/sources/index.js";
import type { SecretBox } from "../shared/crypto.js";
import type { GoogleSyncObservationFields } from "./contracts.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

interface IntegrationScope {
  readonly integrationId: string;
  readonly personId: string;
  readonly accountKind: IntegrationAccountKind;
  readonly integrationStatus: "active" | "reauth_required" | "error";
  readonly integrationControlEpoch: number;
  readonly personControlEpoch: number;
  readonly connectedAt: Date;
  readonly capabilities: readonly IntegrationCapability[];
}

interface TriggerJob {
  readonly status: string;
  readonly jobKind: string;
  readonly integrationControlEpoch: number;
}

interface ExactPrivateRoute {
  readonly conversationId: string;
  readonly conversationAuthorityVersion: number;
  readonly participantEpochId: string;
  readonly participantSetDigest: string;
  readonly identityId: string;
  readonly providerChatId: string;
  readonly providerParticipantDigest: string;
}

export interface GoogleSyncMilestoneResult {
  readonly disposition:
    | "google_sync_milestone_queued"
    | "google_sync_milestone_already_recorded"
    | "google_sync_milestone_not_ready";
  readonly outboxIds: readonly string[];
}

export type PrivateSourceCandidateNoticeResult =
  | { readonly kind: "obsolete" }
  | { readonly kind: "route_unavailable" }
  | { readonly kind: "queued"; readonly outboxId: string; readonly created: boolean };

const RECENT_GMAIL_CAPTURE_PRIORITY_CEILING = 110;
// Thread reconciliation is scheduled one bounded step after its capture work.
const RECENT_GMAIL_RECONCILIATION_PRIORITY_CEILING = RECENT_GMAIL_CAPTURE_PRIORITY_CEILING + 5;
const EFFECT_AUTHORIZATION_MS = 10 * 60_000;

/**
 * Deep application module for Google lifecycle messages. Callers report only a
 * committed OAuth connection or a settled durable job; this module owns phase
 * calculation, exact-private routing, current authority, handoffs, and effects.
 */
export class GoogleSyncCoordinator {
  public constructor(
    private readonly database: Executor,
    private readonly config: FlorenceConfig,
    private readonly secretBox: SecretBox,
  ) {}

  public async observe(input: GoogleSyncObservationFields): Promise<GoogleSyncMilestoneResult> {
    validateObservation(input);
    return inTransaction(this.database, async (transaction) => {
      await transaction`
        select pg_advisory_xact_lock(
          hashtextextended(${`google-sync-milestone:${input.integrationId}`}, 0)
        )
      `;
      const scope = await this.reopenIntegration(transaction, input);
      if (!scope) return notReady();
      const trigger = await this.reopenTrigger(transaction, input, scope);
      if (input.triggeringJobId !== null && !trigger) return notReady();

      const route = await this.resolveExactPrivateRoute(transaction, scope.personId);
      if (!route) return notReady();
      const authority = await new PostgresConversationAuthority(transaction).authorizeSend({
        conversationId: route.conversationId,
        expectedParticipantEpochId: route.participantEpochId,
        expectedParticipantSetDigest: route.participantSetDigest,
        liveParticipantIdentityIds: [route.identityId],
        sendKind: "transactional",
        operation: "google_sync_milestone",
        ruleId: null,
      });
      if (
        !authority.allowed ||
        authority.authorityVersion !== route.conversationAuthorityVersion ||
        authority.participantEpochId !== route.participantEpochId ||
        authority.participantSetDigest !== route.participantSetDigest
      ) {
        return notReady();
      }

      const outboxIds: string[] = [];
      if (scope.integrationStatus === "reauth_required" || scope.integrationStatus === "error") {
        const queued = await this.enqueueAttentionRequired(
          transaction,
          scope,
          route,
          scope.integrationStatus === "reauth_required" ? "reauth" : "integration-error",
        );
        if (queued.created) outboxIds.push(queued.outboxId);
        return resultFor(outboxIds, queued.created);
      }

      if (await this.hasCurrentRecentTerminalFailure(transaction, scope)) {
        const queued = await this.enqueueAttentionRequired(transaction, scope, route, "recent-review-failed");
        if (queued.created) outboxIds.push(queued.outboxId);
        return resultFor(outboxIds, queued.created);
      }

      if (
        trigger !== null &&
        (trigger.status !== "succeeded" || trigger.integrationControlEpoch !== scope.integrationControlEpoch)
      ) {
        return notReady();
      }

      const connected = await this.enqueueMilestone(transaction, {
        scope,
        route,
        phase: "connected",
        text: connectedMessage(scope),
      });
      if (connected.created) outboxIds.push(connected.outboxId);

      if (await this.recentInformationIsCurrent(transaction, scope)) {
        const current = await this.enqueueMilestone(transaction, {
          scope,
          route,
          phase: "recent-current",
          text: recentCurrentMessage(scope),
        });
        if (current.created) outboxIds.push(current.outboxId);
        return resultFor(outboxIds, connected.created || current.created);
      }

      return resultFor(outboxIds, connected.created);
    });
  }

  public async notifyPrivateSourceCandidate(input: {
    readonly candidateId: string;
    readonly personId: string;
    readonly integrationId: string;
    readonly expectedIntegrationControlEpoch: number;
  }): Promise<PrivateSourceCandidateNoticeResult> {
    if (
      !UUID_PATTERN.test(input.candidateId) ||
      !UUID_PATTERN.test(input.personId) ||
      !UUID_PATTERN.test(input.integrationId) ||
      !Number.isSafeInteger(input.expectedIntegrationControlEpoch) ||
      input.expectedIntegrationControlEpoch < 1
    ) {
      throw new Error("Private source notification fence is invalid");
    }
    return inTransaction(this.database, async (transaction) => {
      await transaction`
        select pg_advisory_xact_lock(hashtextextended(${`private-source-notice:${input.candidateId}`}, 0))
      `;
      const rows = await transaction<
        {
          readonly person_control_epoch: number | string;
          readonly integration_control_epoch: number | string;
        }[]
      >`
        select person.control_epoch as person_control_epoch,
          integration.control_epoch as integration_control_epoch
        from knowledge_candidates candidate
        join private_source_frontiers frontier
          on frontier.current_candidate_id = candidate.id
          and frontier.owner_person_id = candidate.owner_person_id
          and frontier.disposition = 'candidate'
        join people person on person.id = candidate.owner_person_id
          and person.status = 'registered'
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
      if (!current) return { kind: "obsolete" };
      const route = await this.resolveExactPrivateRoute(transaction, input.personId);
      if (!route) return { kind: "route_unavailable" };
      const authority = await new PostgresConversationAuthority(transaction).authorizeSend({
        conversationId: route.conversationId,
        expectedParticipantEpochId: route.participantEpochId,
        expectedParticipantSetDigest: route.participantSetDigest,
        liveParticipantIdentityIds: [route.identityId],
        sendKind: "transactional",
        operation: "private_source_review",
        ruleId: null,
      });
      if (
        !authority.allowed ||
        authority.authorityVersion !== route.conversationAuthorityVersion ||
        authority.participantEpochId !== route.participantEpochId ||
        authority.participantSetDigest !== route.participantSetDigest
      ) {
        return { kind: "route_unavailable" };
      }
      const idempotencyKey = `private-source-review:${input.candidateId}`;
      const existing = await existingOutbox(transaction, idempotencyKey);
      if (existing) return { kind: "queued", outboxId: existing, created: false };
      const handoff = await new PostgresWebAuth(
        transaction,
        this.secretBox,
        this.config.security.tokenKey,
      ).createHandoff({
        personId: input.personId,
        privateIdentityId: route.identityId,
        privateConversationId: route.conversationId,
        purpose: "web_sign_in",
        context: { returnPath: "/sources", candidateId: input.candidateId },
        expiresInSeconds: 10 * 60,
      });
      const text = `I found something in your connected account that may need family coordination. Review it privately: ${this.config.publicBaseUrl}/handoff/${handoff.token}\n\nThis secure link expires in 10 minutes. If it expires, text me “settings” for a fresh one.`;
      const queued = await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
        actorPersonId: input.personId,
        person: { id: input.personId, controlEpoch: Number(current.person_control_epoch) },
        integration: {
          id: input.integrationId,
          controlEpoch: Number(current.integration_control_epoch),
        },
        conversation: {
          id: route.conversationId,
          authorityVersion: route.conversationAuthorityVersion,
        },
        participantEpochId: route.participantEpochId,
        expectedParticipantDigest: route.participantSetDigest,
        effectKind: "linq.message",
        idempotencyKey,
        data: {
          candidateId: input.candidateId,
          integrationId: input.integrationId,
          textDigest: sha256Hex(text),
        },
        policy: {
          exactPrivateDm: true,
          noSourceContent: true,
          operation: "private_source_review",
        },
        target: {
          providerChatId: route.providerChatId,
          participantEpochId: route.participantEpochId,
          personId: input.personId,
        },
        payload: {
          providerChatId: route.providerChatId,
          expectedProviderParticipantDigest: route.providerParticipantDigest,
          text,
        },
        reasonCodes: ["current_private_source_candidate", "exact_private_dm", "no_source_content"],
        authorizationExpiresAt: new Date(Date.now() + EFFECT_AUTHORIZATION_MS),
      });
      return { kind: "queued", ...queued };
    });
  }

  private async reopenIntegration(
    transaction: Transaction,
    input: GoogleSyncObservationFields,
  ): Promise<IntegrationScope | null> {
    const rows = await transaction<
      {
        readonly person_id: string;
        readonly account_kind: string;
        readonly integration_status: string;
        readonly integration_control_epoch: number | string;
        readonly connected_at: Date;
        readonly person_control_epoch: number | string;
      }[]
    >`
      select integration.person_id, integration.account_kind,
        integration.status as integration_status,
        integration.control_epoch as integration_control_epoch,
        integration.connected_at, person.control_epoch as person_control_epoch
      from integrations integration
      join people person on person.id = integration.person_id
        and person.status = 'registered' and person.consented_at is not null
      where integration.id = ${input.integrationId}
        and integration.person_id = ${input.personId}
        and integration.provider = 'google'
        and integration.status in ('active', 'reauth_required', 'error')
      for update of integration, person
    `;
    const row = rows[0];
    if (!row) return null;
    const capabilities = await transaction<{ readonly capability: string }[]>`
      select capability
      from integration_capabilities
      where integration_id = ${input.integrationId} and status = 'active'
        and capability in ('mail', 'calendar')
      order by capability
    `;
    if (capabilities.length === 0) return null;
    return {
      integrationId: input.integrationId,
      personId: row.person_id,
      accountKind: parseAccountKind(row.account_kind),
      integrationStatus: parseIntegrationStatus(row.integration_status),
      integrationControlEpoch: Number(row.integration_control_epoch),
      personControlEpoch: Number(row.person_control_epoch),
      connectedAt: row.connected_at,
      capabilities: capabilities.map((entry) => parseCapability(entry.capability)),
    };
  }

  private async reopenTrigger(
    transaction: Transaction,
    input: GoogleSyncObservationFields,
    scope: IntegrationScope,
  ): Promise<TriggerJob | null> {
    if (input.triggeringJobId === null) return null;
    const rows = await transaction<
      {
        readonly status: string;
        readonly job_kind: string;
        readonly person_control_epoch: number | string;
        readonly integration_control_epoch: number | string;
      }[]
    >`
      select status, job_kind, person_control_epoch, integration_control_epoch
      from jobs
      where id = ${input.triggeringJobId}
        and person_id = ${scope.personId}
        and integration_id = ${scope.integrationId}
        and person_control_epoch = ${scope.personControlEpoch}
        and (
          job_kind like 'google.%'
          or job_kind = 'orchestrate.private_source'
        )
        and status in ('succeeded', 'attention', 'dead', 'cancelled', 'retry')
    `;
    const row = rows[0];
    return row
      ? {
          status: row.status,
          jobKind: row.job_kind,
          integrationControlEpoch: Number(row.integration_control_epoch),
        }
      : null;
  }

  private async hasCurrentRecentTerminalFailure(
    transaction: Transaction,
    scope: IntegrationScope,
  ): Promise<boolean> {
    const rows = await transaction<{ readonly blocked: boolean }[]>`
      select (
        exists(
          select 1 from jobs job
          where job.person_id = ${scope.personId}
            and job.person_control_epoch = ${scope.personControlEpoch}
            and job.integration_id = ${scope.integrationId}
            and job.integration_control_epoch = ${scope.integrationControlEpoch}
            and job.status in ('attention', 'dead')
            and (
              (
                job.job_kind = 'google.gmail.message'
                and job.priority <= ${RECENT_GMAIL_CAPTURE_PRIORITY_CEILING}
              )
              or (
                job.job_kind = 'orchestrate.private_source'
                and job.priority <= ${RECENT_GMAIL_RECONCILIATION_PRIORITY_CEILING}
              )
            )
        )
        or coalesce((
          select job.status from jobs job
          where job.person_id = ${scope.personId}
            and job.person_control_epoch = ${scope.personControlEpoch}
            and job.integration_id = ${scope.integrationId}
            and job.integration_control_epoch = ${scope.integrationControlEpoch}
            and job.job_kind in ('google.bootstrap', 'google.gmail.bootstrap', 'google.gmail.poll')
          order by job.updated_at desc, job.id desc
          limit 1
        ), 'missing') = 'dead'
        or coalesce((
          select job.status from jobs job
          where job.person_id = ${scope.personId}
            and job.person_control_epoch = ${scope.personControlEpoch}
            and job.integration_id = ${scope.integrationId}
            and job.integration_control_epoch = ${scope.integrationControlEpoch}
            and job.job_kind = 'google.gmail.backfill'
            and job.idempotency_key like '%:newest_30_days:%'
          order by job.updated_at desc, job.id desc
          limit 1
        ), 'missing') = 'dead'
        or coalesce((
          select job.status from jobs job
          where job.person_id = ${scope.personId}
            and job.person_control_epoch = ${scope.personControlEpoch}
            and job.integration_id = ${scope.integrationId}
            and job.integration_control_epoch = ${scope.integrationControlEpoch}
            and job.job_kind = 'google.calendar.catalog'
          order by job.updated_at desc, job.id desc
          limit 1
        ), 'missing') = 'dead'
        or exists(
          select 1
          from integration_grants grant_row
          left join sync_cursors event_cursor
            on event_cursor.integration_id = ${scope.integrationId}
            and event_cursor.resource_kind =
              'calendar:' || (grant_row.scope->>'calendarIdDigest')
          where grant_row.integration_id = ${scope.integrationId}
            and grant_row.grant_kind = 'calendar_privacy'
            and grant_row.status = 'active'
            and grant_row.scope->>'mode' <> 'off'
            and exists(
              select 1 from jobs failed_poll
              where failed_poll.person_id = ${scope.personId}
                and failed_poll.person_control_epoch = ${scope.personControlEpoch}
                and failed_poll.integration_id = ${scope.integrationId}
                and failed_poll.integration_control_epoch = ${scope.integrationControlEpoch}
                and failed_poll.job_kind = 'google.calendar.poll'
                and failed_poll.status = 'dead'
                and failed_poll.idempotency_key like
                  'calendar:poll:' || ${scope.integrationId} || ':e' ||
                  ${scope.integrationControlEpoch} || ':' ||
                  (grant_row.scope->>'calendarIdDigest') || ':v' || grant_row.version || ':' ||
                  (grant_row.scope->>'mode') || ':%'
                and (event_cursor.updated_at is null or failed_poll.updated_at >= event_cursor.updated_at)
            )
        )
      ) as blocked
    `;
    return rows[0]?.blocked === true;
  }

  private async resolveExactPrivateRoute(
    transaction: Transaction,
    personId: string,
  ): Promise<ExactPrivateRoute | null> {
    const rows = await transaction<
      {
        readonly conversation_id: string;
        readonly authority_version: number | string;
        readonly participant_epoch_id: string;
        readonly participant_set_digest: string;
        readonly identity_id: string;
        readonly external_channel_id: string;
        readonly latest_participant_digest: string;
      }[]
    >`
      select conversation.id as conversation_id, conversation.authority_version,
        epoch.id as participant_epoch_id, epoch.participant_set_digest,
        identity.id as identity_id, channel.external_channel_id,
        channel.latest_participant_digest
      from conversations conversation
      join conversation_channels channel on channel.conversation_id = conversation.id
        and channel.provider = 'linq' and channel.status = 'active'
        and channel.latest_participant_digest is not null
        and channel.latest_participant_checked_at is not null
      join participant_epochs epoch on epoch.id = conversation.current_epoch_id
        and epoch.ended_at is null
      join epoch_participants participant on participant.participant_epoch_id = epoch.id
        and participant.person_id = ${personId}
        and participant.registration_status = 'registered'
        and participant.consented_at is not null
      join person_identities identity on identity.id = participant.person_identity_id
        and identity.person_id = participant.person_id and identity.status = 'verified'
      join participant_policies policy on policy.conversation_id = conversation.id
        and policy.person_id = participant.person_id and policy.status = 'active'
        and policy.allow_content_processing and policy.allow_direct_responses
      where conversation.kind = 'direct' and conversation.status = 'active'
        and (select count(*) from epoch_participants exact
          where exact.participant_epoch_id = epoch.id) = 1
      order by channel.latest_participant_checked_at desc, conversation.updated_at desc,
        conversation.id
      limit 1
    `;
    const row = rows[0];
    return row
      ? {
          conversationId: row.conversation_id,
          conversationAuthorityVersion: Number(row.authority_version),
          participantEpochId: row.participant_epoch_id,
          participantSetDigest: row.participant_set_digest,
          identityId: row.identity_id,
          providerChatId: row.external_channel_id,
          providerParticipantDigest: row.latest_participant_digest,
        }
      : null;
  }

  private async recentInformationIsCurrent(
    transaction: Transaction,
    scope: IntegrationScope,
  ): Promise<boolean> {
    const mailActive = scope.capabilities.includes("mail");
    const calendarActive = scope.capabilities.includes("calendar");
    if (mailActive && !(await this.recentMailIsCurrent(transaction, scope))) return false;
    if (calendarActive && !(await this.initialCalendarsAreCurrent(transaction, scope))) return false;

    const blocked = await transaction<{ readonly blocked: boolean }[]>`
      select exists(
        select 1 from jobs job
        where job.person_id = ${scope.personId}
          and job.person_control_epoch = ${scope.personControlEpoch}
          and job.integration_id = ${scope.integrationId}
          and job.integration_control_epoch = ${scope.integrationControlEpoch}
          and job.status in ('pending', 'retry', 'leased', 'attention', 'dead')
          and (
            (
              job.job_kind = 'google.gmail.message'
              and job.priority <= ${RECENT_GMAIL_CAPTURE_PRIORITY_CEILING}
            )
            or (
              job.job_kind = 'orchestrate.private_source'
              and job.priority <= ${RECENT_GMAIL_RECONCILIATION_PRIORITY_CEILING}
            )
            or (
              job.job_kind = 'google.gmail.backfill'
              and job.idempotency_key like '%:newest_30_days:%'
            )
          )
      ) as blocked
    `;
    return blocked[0]?.blocked === false;
  }

  private async recentMailIsCurrent(transaction: Transaction, scope: IntegrationScope): Promise<boolean> {
    const rows = await transaction<{ readonly current: boolean }[]>`
      select (
        exists(
          select 1 from sync_cursors cursor
          where cursor.integration_id = ${scope.integrationId}
            and cursor.resource_kind = 'gmail_history'
            and cursor.state = 'active' and cursor.checkpoint_at is not null
        )
        and exists(
          select 1 from sync_cursors cursor
          where cursor.integration_id = ${scope.integrationId}
            and cursor.resource_kind = 'gmail_backfill:newest_30_days'
            and cursor.state = 'exhausted' and cursor.checkpoint_at is not null
        )
        and exists(
          select 1 from jobs job
          where job.person_id = ${scope.personId}
            and job.person_control_epoch = ${scope.personControlEpoch}
            and job.integration_id = ${scope.integrationId}
            and job.integration_control_epoch = ${scope.integrationControlEpoch}
            and job.job_kind = 'google.gmail.poll' and job.status = 'succeeded'
            and job.updated_at >= ${scope.connectedAt}
        )
        and not exists(
          select 1 from jobs failed
          join sync_cursors frontier on frontier.integration_id = ${scope.integrationId}
            and frontier.resource_kind = 'gmail_backfill:newest_30_days'
          where failed.person_id = ${scope.personId}
            and failed.person_control_epoch = ${scope.personControlEpoch}
            and failed.integration_id = ${scope.integrationId}
            and failed.integration_control_epoch = ${scope.integrationControlEpoch}
            and failed.job_kind = 'google.gmail.backfill'
            and failed.idempotency_key like '%:newest_30_days:%'
            and failed.status = 'dead' and failed.updated_at >= frontier.checkpoint_at
        )
      ) as current
    `;
    return rows[0]?.current === true;
  }

  private async initialCalendarsAreCurrent(
    transaction: Transaction,
    scope: IntegrationScope,
  ): Promise<boolean> {
    const rows = await transaction<{ readonly current: boolean }[]>`
      select (
        exists(
          select 1 from sync_cursors catalog
          where catalog.integration_id = ${scope.integrationId}
            and catalog.resource_kind = 'calendar_catalog'
            and catalog.state = 'active' and catalog.checkpoint_at is not null
        )
        and exists(
          select 1 from jobs catalog_job
          where catalog_job.person_id = ${scope.personId}
            and catalog_job.person_control_epoch = ${scope.personControlEpoch}
            and catalog_job.integration_id = ${scope.integrationId}
            and catalog_job.integration_control_epoch = ${scope.integrationControlEpoch}
            and catalog_job.job_kind = 'google.calendar.catalog'
            and catalog_job.status = 'succeeded'
            and catalog_job.updated_at >= ${scope.connectedAt}
        )
        and not exists(
          select 1 from integration_grants grant_row
          where grant_row.integration_id = ${scope.integrationId}
            and grant_row.grant_kind = 'calendar_privacy'
            and grant_row.status = 'active' and grant_row.scope->>'mode' <> 'off'
            and (
              not exists(
                select 1 from sync_cursors cursor
                where cursor.integration_id = ${scope.integrationId}
                  and cursor.resource_kind = 'calendar:' || (grant_row.scope->>'calendarIdDigest')
                  and cursor.state in ('active', 'initial')
                  and cursor.checkpoint_at is not null
                  and cursor.checkpoint_at >= grant_row.created_at
              )
              or not exists(
                select 1 from jobs poll
                where poll.person_id = ${scope.personId}
                  and poll.person_control_epoch = ${scope.personControlEpoch}
                  and poll.integration_id = ${scope.integrationId}
                  and poll.integration_control_epoch = ${scope.integrationControlEpoch}
                  and poll.job_kind = 'google.calendar.poll' and poll.status = 'succeeded'
                  and poll.idempotency_key like
                    'calendar:poll:' || ${scope.integrationId} || ':e' ||
                    ${scope.integrationControlEpoch} || ':' ||
                    (grant_row.scope->>'calendarIdDigest') || ':v' || grant_row.version || ':' ||
                    (grant_row.scope->>'mode') || ':%'
              )
            )
        )
      ) as current
    `;
    return rows[0]?.current === true;
  }

  private async enqueueAttentionRequired(
    transaction: Transaction,
    scope: IntegrationScope,
    route: ExactPrivateRoute,
    reason: "reauth" | "integration-error" | "recent-review-failed",
  ): Promise<{ readonly outboxId: string; readonly created: boolean }> {
    const phase =
      reason === "reauth"
        ? "reauth-required"
        : reason === "integration-error"
          ? "sync-error"
          : "recent-review-failed";
    const idempotencyKey = milestoneKey(scope, phase);
    const existing = await existingOutbox(transaction, idempotencyKey);
    if (existing) return { outboxId: existing, created: false };

    const handoff = await new PostgresWebAuth(
      transaction,
      this.secretBox,
      this.config.security.tokenKey,
    ).createHandoff({
      personId: scope.personId,
      privateIdentityId: route.identityId,
      privateConversationId: route.conversationId,
      purpose: "google_connect",
      context: {
        returnPath: "/sources",
        integrationId: scope.integrationId,
        reconnect: reason !== "recent-review-failed",
      },
      expiresInSeconds: 10 * 60,
    });
    const link = `${this.config.publicBaseUrl}/handoff/${handoff.token}`;
    const text = attentionRequiredMessage(scope, link, reason);
    return this.enqueueEffect(transaction, {
      scope,
      route,
      idempotencyKey,
      phase,
      text,
      reasonCodes: ["google_sync_attention_required", reason, "exact_private_dm"],
    });
  }

  private async enqueueMilestone(
    transaction: Transaction,
    input: {
      readonly scope: IntegrationScope;
      readonly route: ExactPrivateRoute;
      readonly phase: "connected" | "recent-current";
      readonly text: string;
    },
  ): Promise<{ readonly outboxId: string; readonly created: boolean }> {
    const idempotencyKey = milestoneKey(input.scope, input.phase);
    const existing = await existingOutbox(transaction, idempotencyKey);
    if (existing) return { outboxId: existing, created: false };
    return this.enqueueEffect(transaction, {
      ...input,
      idempotencyKey,
      reasonCodes: ["google_sync_milestone", input.phase, "exact_private_dm"],
    });
  }

  private async enqueueEffect(
    transaction: Transaction,
    input: {
      readonly scope: IntegrationScope;
      readonly route: ExactPrivateRoute;
      readonly idempotencyKey: string;
      readonly phase: string;
      readonly text: string;
      readonly reasonCodes: readonly string[];
    },
  ): Promise<{ readonly outboxId: string; readonly created: boolean }> {
    return new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
      actorPersonId: input.scope.personId,
      person: {
        id: input.scope.personId,
        controlEpoch: input.scope.personControlEpoch,
      },
      integration: {
        id: input.scope.integrationId,
        controlEpoch: input.scope.integrationControlEpoch,
      },
      conversation: {
        id: input.route.conversationId,
        authorityVersion: input.route.conversationAuthorityVersion,
      },
      participantEpochId: input.route.participantEpochId,
      expectedParticipantDigest: input.route.participantSetDigest,
      effectKind: "linq.message",
      idempotencyKey: input.idempotencyKey,
      data: {
        integrationId: input.scope.integrationId,
        integrationControlEpoch: input.scope.integrationControlEpoch,
        phase: input.phase,
        textDigest: sha256Hex(input.text),
      },
      policy: {
        exactPrivateDm: true,
        currentIntegrationControlEpoch: input.scope.integrationControlEpoch,
        operation: "google_sync_milestone",
      },
      target: {
        providerChatId: input.route.providerChatId,
        participantEpochId: input.route.participantEpochId,
        personId: input.scope.personId,
      },
      payload: {
        providerChatId: input.route.providerChatId,
        expectedProviderParticipantDigest: input.route.providerParticipantDigest,
        text: input.text,
      },
      reasonCodes: input.reasonCodes,
      authorizationExpiresAt: new Date(Date.now() + EFFECT_AUTHORIZATION_MS),
    });
  }
}

function connectedMessage(scope: IntegrationScope): string {
  const sources = sourceNames(scope);
  if (scope.accountKind === "work" && isCalendarOnly(scope)) {
    return "Your work Google Calendar is connected. I’m privately reviewing its recent schedule now; you can keep using Florence while I work.";
  }
  return `Your ${scope.accountKind === "work" ? "work" : "personal"} Google account is connected. I’m privately reviewing recent ${sources} now; you can keep using Florence while I work.`;
}

function recentCurrentMessage(scope: IntegrationScope): string {
  const sources = sourceNames(scope);
  if (scope.capabilities.includes("mail")) {
    return `Your recent ${sources} ${scope.capabilities.length === 1 ? "information is" : "are"} current. I’ll keep reviewing older Gmail history in the background and stay up to date as new information arrives.`;
  }
  return `Your recent ${sources} information is current. I’ll keep it up to date as new changes arrive.`;
}

function attentionRequiredMessage(
  scope: IntegrationScope,
  link: string,
  reason: "reauth" | "integration-error" | "recent-review-failed",
): string {
  const source =
    scope.accountKind === "work" && isCalendarOnly(scope)
      ? "work Google Calendar"
      : `${scope.accountKind === "work" ? "work" : "personal"} Google account`;
  if (reason === "recent-review-failed") {
    return `I couldn’t finish reviewing part of your recent ${sourceNames(scope)} information. Your ${source} is still connected, and I’ll keep recovery bounded to your private account. Review the connection or reconnect it privately here: ${link}\n\nThis secure link expires in 10 minutes. If it expires, text me “connect Google” for a fresh one.`;
  }
  const opening =
    reason === "reauth" ? `I lost access to your ${source}.` : `I hit a problem syncing your ${source}.`;
  return `${opening} Reconnect it privately here so I can resume reviewing ${sourceNames(scope)}: ${link}\n\nThis secure link expires in 10 minutes. If it expires, text me “connect Google” for a fresh one.`;
}

function sourceNames(scope: IntegrationScope): string {
  const mail = scope.capabilities.includes("mail");
  const calendar = scope.capabilities.includes("calendar");
  if (mail && calendar) return "Gmail and Calendar";
  return mail ? "Gmail" : "Calendar";
}

function isCalendarOnly(scope: IntegrationScope): boolean {
  return scope.capabilities.length === 1 && scope.capabilities[0] === "calendar";
}

function milestoneKey(scope: IntegrationScope, phase: string): string {
  return `google-sync:${phase}:${scope.integrationId}:e${scope.integrationControlEpoch}`;
}

async function existingOutbox(transaction: Transaction, idempotencyKey: string): Promise<string | null> {
  const rows = await transaction<{ readonly id: string }[]>`
    select id from outbox where idempotency_key = ${idempotencyKey}
  `;
  return rows[0]?.id ?? null;
}

function resultFor(outboxIds: readonly string[], created: boolean): GoogleSyncMilestoneResult {
  return {
    disposition: created ? "google_sync_milestone_queued" : "google_sync_milestone_already_recorded",
    outboxIds,
  };
}

function notReady(): GoogleSyncMilestoneResult {
  return { disposition: "google_sync_milestone_not_ready", outboxIds: [] };
}

function validateObservation(input: GoogleSyncObservationFields): void {
  if (!UUID_PATTERN.test(input.integrationId) || !UUID_PATTERN.test(input.personId)) {
    throw new Error("Google sync observation identifiers are invalid");
  }
  if (input.triggeringJobId !== null && !UUID_PATTERN.test(input.triggeringJobId)) {
    throw new Error("Google sync observation job identifier is invalid");
  }
}

function parseAccountKind(value: string): IntegrationAccountKind {
  if (value !== "personal_family" && value !== "work") {
    throw new Error("Google account kind is invalid");
  }
  return value;
}

function parseIntegrationStatus(value: string): IntegrationScope["integrationStatus"] {
  if (value !== "active" && value !== "reauth_required" && value !== "error") {
    throw new Error("Google integration status is not observable");
  }
  return value;
}

function parseCapability(value: string): IntegrationCapability {
  if (value !== "mail" && value !== "calendar") {
    throw new Error("Google integration capability is invalid");
  }
  return value;
}

function inTransaction<Result>(
  executor: Executor,
  operation: (transaction: Transaction) => Promise<Result>,
): Promise<Result> {
  return "begin" in executor
    ? (executor.begin(operation) as unknown as Promise<Result>)
    : operation(executor);
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;
