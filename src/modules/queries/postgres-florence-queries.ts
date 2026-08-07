import { createHash } from "node:crypto";
import type { Database } from "../../db/client.js";
import type { SecretBox } from "../../shared/crypto.js";
import type {
  ChatView,
  DataSafetyView,
  HomeView,
  PeopleView,
  RoutineView,
  SourceView,
  Viewer,
} from "../../web/api.js";
import {
  openPrivateBridgePayload,
  PrivateSourceBridge,
  privateBridgeOutboundText,
} from "../bridges/index.js";
import { PostgresSourceIntelligence } from "../sources/index.js";

interface PersonRow {
  id: string;
  timezone: string | null;
  display_name_ciphertext: Buffer | null;
}

interface HouseholdRow {
  id: string;
  role: string;
  member_count: number | string;
}

// Google work at or below this priority is the live/recent-mail/calendar
// frontier that must settle before onboarding can call private sources ready.
const RECENT_SOURCE_PRIORITY_CEILING = 110;

/**
 * Scoped control-plane read models. Every query starts from the authenticated
 * person and joins through an active relationship; callers cannot supply a
 * household scope they have not earned.
 */
export class PostgresFlorenceQueries {
  readonly #sources: PostgresSourceIntelligence;
  readonly #secretBox: SecretBox;

  public constructor(
    private readonly database: Database,
    secretBox: SecretBox,
    rawRetentionDays = 30,
  ) {
    this.#secretBox = secretBox;
    this.#sources = new PostgresSourceIntelligence(database, secretBox, {
      rawRetentionDays,
      privateCandidateRetentionDays: 7,
    });
  }

  public async viewer(
    personId: string,
    csrfToken: string,
    session: Viewer["session"] = { assuranceKind: "base", assuranceExpiresAt: null },
  ): Promise<Viewer> {
    const people = await this.database<PersonRow[]>`
      select id, timezone, display_name_ciphertext from people
      where id = ${personId} and status = 'registered'
    `;
    const person = people[0];
    if (!person) throw new Error("Registered person does not exist");
    const households = await this.database<HouseholdRow[]>`
      select household.id, membership.role,
        count(other_members.id) filter (where other_members.status = 'active') as member_count
      from household_memberships membership
      join households household on household.id = membership.household_id
      left join household_memberships other_members on other_members.household_id = household.id
      where membership.person_id = ${personId}
        and membership.status = 'active'
        and household.status not in ('deletion_fenced', 'deleted')
      group by household.id, membership.role, membership.joined_at
      order by membership.joined_at
    `;
    return {
      person: {
        id: person.id,
        name: decryptPersonName(this.#secretBox, person.id, person.display_name_ciphertext) ?? "You",
        phone: "Verified iMessage",
        timezone: person.timezone ?? "America/Los_Angeles",
      },
      households: households.map((household, index) => ({
        id: household.id,
        name: index === 0 ? "Your family" : `Family ${index + 1}`,
        role: household.role,
        memberCount: Number(household.member_count),
      })),
      session,
      csrfToken,
    };
  }

  public async home(personId: string): Promise<HomeView> {
    const loops = await this.database<
      {
        id: string;
        state: string;
        content_ciphertext: Buffer;
        content_key_version: string;
        last_transition_at: Date;
      }[]
    >`
      select loop.id, loop.state, loop.content_ciphertext, loop.content_key_version,
        loop.last_transition_at
      from household_memberships membership
      join membership_capabilities read_grant on read_grant.membership_id = membership.id
        and read_grant.capability = 'household.read' and read_grant.status = 'active'
      join households household on household.id = membership.household_id
        and household.status = 'active'
      join coverage_loops loop on loop.household_id = household.id
      join conversations destination on destination.id = loop.destination_conversation_id
        and destination.household_id = household.id and destination.status = 'active'
      join participant_epochs epoch on epoch.id = loop.participant_epoch_id
        and destination.current_epoch_id = epoch.id and epoch.ended_at is null
        and epoch.participant_set_digest = loop.participant_set_digest
      join epoch_participants viewer_participant
        on viewer_participant.participant_epoch_id = epoch.id
        and viewer_participant.person_id = ${personId}
        and viewer_participant.registration_status = 'registered'
        and viewer_participant.consented_at is not null
      join participant_policies viewer_policy on viewer_policy.conversation_id = destination.id
        and viewer_policy.person_id = ${personId} and viewer_policy.status = 'active'
        and viewer_policy.allow_content_processing
      where membership.person_id = ${personId}
        and membership.status = 'active'
        and (
          loop.state in ('provisional', 'open', 'awaiting_response', 'at_risk')
          or (loop.state = 'covered' and loop.last_transition_at >= now() - interval '24 hours')
        )
      order by
        case loop.state
          when 'at_risk' then 0
          when 'awaiting_response' then 1
          when 'open' then 2
          when 'provisional' then 3
          else 4
        end,
        case when loop.state = 'covered' then loop.last_transition_at end desc,
        loop.last_transition_at
      limit 50
    `;
    const coverageApprovals = await this.database<{ conversation_id: string; viewer_approved: boolean }[]>`
      select conversation.id as conversation_id,
        exists(
          select 1
          from conversation_rules proposal
          join conversation_rule_approvals approval
            on approval.conversation_rule_id = proposal.id
            and approval.participant_epoch_id = epoch.id
            and approval.participant_set_digest = epoch.participant_set_digest
            and approval.person_id = ${personId}
          where proposal.conversation_id = conversation.id
            and proposal.rule_key = 'family_coverage_proposal'
            and proposal.status = 'candidate'
            and proposal.participant_set_digest = epoch.participant_set_digest
        ) as viewer_approved
      from household_memberships membership
      join membership_capabilities read_grant on read_grant.membership_id = membership.id
        and read_grant.capability = 'household.read' and read_grant.status = 'active'
      join households household on household.id = membership.household_id
        and household.status = 'active'
      join conversations conversation on conversation.household_id = household.id
        and conversation.kind = 'group' and conversation.status = 'active'
      join participant_epochs epoch on epoch.id = conversation.current_epoch_id
        and epoch.ended_at is null
      join epoch_participants viewer_participant
        on viewer_participant.participant_epoch_id = epoch.id
        and viewer_participant.person_id = ${personId}
        and viewer_participant.registration_status = 'registered'
        and viewer_participant.consented_at is not null
      where membership.person_id = ${personId} and membership.status = 'active'
        and not exists(
          select 1
          from epoch_participants participant
          left join participant_policies policy on policy.conversation_id = conversation.id
            and policy.person_id = participant.person_id and policy.status = 'active'
          where participant.participant_epoch_id = epoch.id
            and (
              participant.registration_status <> 'registered'
              or participant.consented_at is null
              or policy.id is null
              or not coalesce(policy.allow_content_processing, false)
              or not coalesce(policy.allow_direct_responses, false)
            )
        )
        and not exists(
          select 1 from channel_suppressions suppression
          where suppression.conversation_id = conversation.id and suppression.active
            and suppression.kind in ('stop', 'pause', 'read_only', 'deletion_fence', 'safety_hold')
        )
        and not exists(
          select 1 from conversation_rules active_rule
          where active_rule.conversation_id = conversation.id
            and active_rule.status = 'active'
            and active_rule.participant_set_digest = epoch.participant_set_digest
            and 'proactive_coverage' = any(active_rule.allowed_operations)
        )
      order by conversation.updated_at desc, conversation.id
      limit 25
    `;
    const candidates = await this.database<{ id: string; candidate_kind: string; proposed_at: Date }[]>`
      select id, candidate_kind, proposed_at
      from knowledge_candidates
      where owner_person_id = ${personId} and status = 'pending'
      order by proposed_at desc
      limit 25
    `;
    const integrationsNeedingAttention = await this.database<{ count: number | string }[]>`
      select count(*) as count from integrations
      where person_id = ${personId} and status in ('reauth_required', 'error')
    `;
    const items: HomeView["items"] = [
      ...loops.map((loop) => {
        const phase: NonNullable<HomeView["items"][number]["phase"]> =
          loop.state === "covered" ? "confirmed" : loop.state === "awaiting_response" ? "awaiting" : "open";
        const meaning = decryptCoverageMeaning(
          this.#secretBox,
          loop.content_ciphertext,
          loop.content_key_version,
        );
        return {
          id: loop.id,
          kind: "coverage" as const,
          phase,
          title: meaning ?? "Family coverage item",
          detail:
            phase === "confirmed"
              ? "A person explicitly confirmed coverage."
              : phase === "awaiting"
                ? "Florence is waiting for an explicit yes."
                : loop.state === "at_risk"
                  ? "This needs a confirmed handoff now."
                  : "No one has explicitly confirmed this yet.",
          urgency:
            phase === "confirmed"
              ? ("routine" as const)
              : loop.state === "at_risk"
                ? ("now" as const)
                : ("soon" as const),
          changedAt: loop.last_transition_at.toISOString(),
        };
      }),
      ...coverageApprovals.map((approval) => ({
        id: `coverage-approval:${approval.conversation_id}`,
        kind: "approval" as const,
        title: approval.viewer_approved
          ? "Waiting for the group’s coverage approval"
          : "Approve Florence for a family group",
        detail: approval.viewer_approved
          ? "Your approval is saved. Florence will not write in the group until every current person approves."
          : "Every current person must approve before Florence can open and follow coverage loops there.",
        urgency: "soon" as const,
        href: "/people",
      })),
      ...candidates.map((candidate) => ({
        id: candidate.id,
        kind: "private_review" as const,
        title: "Florence found something that may matter",
        detail: `Private ${candidate.candidate_kind.replaceAll("_", " ")} awaiting your review.`,
        urgency: "routine" as const,
        href: "/sources",
      })),
    ];
    if (Number(integrationsNeedingAttention[0]?.count ?? 0) > 0) {
      items.push({
        id: "connection-attention",
        kind: "connection",
        title: "A source needs to be reconnected",
        detail: "Florence paused that private source until you reconnect it.",
        urgency: "soon",
        href: "/sources",
      });
    }
    const attentionCount = items.filter((item) => item.phase !== "confirmed").length;
    const householdCount = await this.database<{ count: number | string }[]>`
      select count(*) as count from household_memberships
      where person_id = ${personId} and status = 'active'
    `;
    const sourceSetupRows = await this.database<
      {
        has_active_personal: boolean;
        has_personal_connection: boolean;
        personal_requires_reconnect: boolean;
        personal_ready: boolean;
      }[]
    >`
      select
        exists(
          select 1 from integrations integration
          where integration.person_id = ${personId}
            and integration.provider = 'google'
            and integration.account_kind = 'personal_family'
            and integration.status = 'active'
        ) as has_active_personal,
        exists(
          select 1 from integrations integration
          where integration.person_id = ${personId}
            and integration.provider = 'google'
            and integration.account_kind = 'personal_family'
            and integration.status <> 'revoked'
        ) as has_personal_connection,
        exists(
          select 1 from integrations integration
          where integration.person_id = ${personId}
            and integration.provider = 'google'
            and integration.account_kind = 'personal_family'
            and integration.status in ('reauth_required', 'error')
        ) as personal_requires_reconnect,
        exists(
          select 1
          from integrations integration
          where integration.person_id = ${personId}
            and integration.provider = 'google'
            and integration.account_kind = 'personal_family'
            and integration.status = 'active'
            and exists(
              select 1 from integration_capabilities capability
              where capability.integration_id = integration.id
                and capability.capability = 'mail' and capability.status = 'active'
            )
            and exists(
              select 1 from integration_capabilities capability
              where capability.integration_id = integration.id
                and capability.capability = 'calendar' and capability.status = 'active'
            )
            and exists(
              select 1 from sync_cursors cursor
              where cursor.integration_id = integration.id
                and cursor.resource_kind = 'gmail_history'
                and cursor.state = 'active'
                and cursor.updated_at >= integration.connected_at
            )
            and exists(
              select 1 from sync_cursors cursor
              where cursor.integration_id = integration.id
                and cursor.resource_kind = 'gmail_backfill:newest_30_days'
                and cursor.state = 'exhausted'
                and cursor.updated_at >= integration.connected_at
            )
            and exists(
              select 1 from sync_cursors cursor
              where cursor.integration_id = integration.id
                and cursor.resource_kind = 'calendar_catalog'
                and cursor.state = 'active'
                and cursor.updated_at >= integration.connected_at
            )
            and coalesce((
              select job.status from jobs job
              where job.integration_id = integration.id
                and job.integration_control_epoch = integration.control_epoch
                and job.job_kind in ('google.bootstrap', 'google.gmail.bootstrap', 'google.gmail.poll')
              order by job.updated_at desc, job.id desc
              limit 1
            ), 'missing') <> 'dead'
            and coalesce((
              select job.status from jobs job
              where job.integration_id = integration.id
                and job.integration_control_epoch = integration.control_epoch
                and job.job_kind = 'google.gmail.backfill'
                and job.idempotency_key like '%:newest_30_days:%'
              order by job.updated_at desc, job.id desc
              limit 1
            ), 'missing') <> 'dead'
            and coalesce((
              select job.status from jobs job
              where job.integration_id = integration.id
                and job.integration_control_epoch = integration.control_epoch
                and job.job_kind = 'google.calendar.catalog'
              order by job.updated_at desc, job.id desc
              limit 1
            ), 'missing') <> 'dead'
            and not exists(
              select 1
              from integration_grants grant_row
              where grant_row.integration_id = integration.id
                and grant_row.grant_kind = 'calendar_privacy'
                and grant_row.status = 'active'
                and grant_row.scope->>'mode' <> 'off'
                and (
                  not exists(
                    select 1 from sync_cursors cursor
                    where cursor.integration_id = integration.id
                      and cursor.resource_kind = 'calendar:' || (grant_row.scope->>'calendarIdDigest')
                      and cursor.state = 'active'
                      and cursor.updated_at >= integration.connected_at
                  )
                  or exists(
                    select 1 from jobs failed_poll
                    left join sync_cursors event_cursor
                      on event_cursor.integration_id = integration.id
                      and event_cursor.resource_kind =
                        'calendar:' || (grant_row.scope->>'calendarIdDigest')
                    where failed_poll.integration_id = integration.id
                      and failed_poll.integration_control_epoch = integration.control_epoch
                      and failed_poll.job_kind = 'google.calendar.poll'
                      and failed_poll.status = 'dead'
                      and failed_poll.idempotency_key like
                        'calendar:poll:' || integration.id || ':e' || integration.control_epoch || ':' ||
                        (grant_row.scope->>'calendarIdDigest') || ':v' || grant_row.version || ':' ||
                        (grant_row.scope->>'mode') || ':%'
                      and (event_cursor.updated_at is null or failed_poll.updated_at >= event_cursor.updated_at)
                  )
                )
            )
            and not exists(
              select 1 from jobs job
              where job.integration_id = integration.id
                and job.integration_control_epoch = integration.control_epoch
                and job.job_kind in ('google.gmail.message', 'orchestrate.private_source')
                and job.priority <= ${RECENT_SOURCE_PRIORITY_CEILING}
                and job.status in ('pending', 'retry', 'leased', 'attention', 'dead')
            )
        ) as personal_ready
    `;
    const sourceSetup = sourceSetupRows[0] ?? {
      has_active_personal: false,
      has_personal_connection: false,
      personal_requires_reconnect: false,
      personal_ready: false,
    };
    const hasHousehold = Number(householdCount[0]?.count ?? 0) > 0;
    const completed = 1 + (hasHousehold ? 1 : 0) + (sourceSetup.personal_ready ? 1 : 0);
    const onboarding: HomeView["onboarding"] = !hasHousehold
      ? {
          completed,
          total: 3,
          next: "Create or join your family",
          detail: "Florence needs your private family space before connecting sources.",
          href: "/people",
          actionLabel: "Set up your family",
        }
      : !sourceSetup.personal_ready
        ? {
            completed,
            total: 3,
            next: sourceSetup.personal_requires_reconnect
              ? "Reconnect your personal Google account"
              : sourceSetup.has_active_personal
                ? "Reviewing your recent sources"
                : sourceSetup.has_personal_connection
                  ? "Review your personal Google connection"
                  : "Connect your personal Google account",
            detail: sourceSetup.has_active_personal
              ? "Florence is reviewing recent mail and every enabled calendar before calling setup complete."
              : "Your personal Google connection brings recent family mail and calendars into private review.",
            href: "/sources",
            actionLabel: sourceSetup.has_active_personal ? "View source progress" : "Open Sources",
          }
        : null;
    return {
      monitoring: {
        status: attentionCount > 0 ? "attention" : onboarding ? "learning" : "healthy",
        label:
          attentionCount > 0
            ? `${attentionCount} thing${attentionCount === 1 ? "" : "s"} need attention`
            : onboarding
              ? "Florence is getting ready"
              : "Your family is covered",
        detail:
          attentionCount > 0
            ? "Florence is following the open loops and will stay quiet about everything else."
            : onboarding
              ? "Setup continues privately while you keep using Florence in iMessage."
              : "Nothing currently needs a handoff or decision.",
      },
      attentionCount,
      items,
      onboarding,
    };
  }

  public async chats(personId: string): Promise<ChatView[]> {
    const chats = await this.database<
      {
        id: string;
        kind: string;
        status: string;
        epoch_id: string;
        sequence: number | string;
        epoch_started_at: Date;
        participant_set_digest: string;
        hard_suppression: boolean;
        read_only_suppression: boolean;
        all_registered: boolean;
        all_content_allowed: boolean;
        all_direct_allowed: boolean;
        retention_seconds: number | string | null;
        exact_write_rule: boolean;
        proactive_rule: boolean;
      }[]
    >`
      select conversation.id, conversation.kind, conversation.status,
        epoch.id as epoch_id, epoch.sequence, epoch.started_at as epoch_started_at,
        epoch.participant_set_digest,
        exists(
          select 1 from channel_suppressions suppression
          where suppression.conversation_id = conversation.id and suppression.active
            and suppression.kind in ('stop', 'pause', 'deletion_fence', 'safety_hold')
        ) as hard_suppression,
        exists(
          select 1 from channel_suppressions suppression
          where suppression.conversation_id = conversation.id and suppression.active
            and suppression.kind = 'read_only'
        ) as read_only_suppression,
        bool_and(participant.registration_status = 'registered' and participant.consented_at is not null)
          as all_registered,
        bool_and(coalesce(policy.allow_content_processing, false)) as all_content_allowed,
        bool_and(coalesce(policy.allow_direct_responses, false)) as all_direct_allowed,
        max(policy.retention_seconds) filter (where participant.person_id = ${personId})
          as retention_seconds,
        exists(
          select 1 from conversation_rules rule
          where rule.conversation_id = conversation.id and rule.status = 'active'
            and rule.participant_set_digest = epoch.participant_set_digest
            and cardinality(rule.allowed_operations) > 0
        ) as exact_write_rule,
        exists(
          select 1 from conversation_rules rule
          where rule.conversation_id = conversation.id and rule.status = 'active'
            and 'proactive_coverage' = any(rule.allowed_operations)
            and rule.participant_set_digest = epoch.participant_set_digest
        ) as proactive_rule
      from conversations conversation
      join participant_epochs epoch on epoch.id = conversation.current_epoch_id and epoch.ended_at is null
      join epoch_participants participant on participant.participant_epoch_id = epoch.id
      left join participant_policies policy on policy.conversation_id = conversation.id
        and policy.person_id = participant.person_id and policy.status = 'active'
      where exists(
        select 1 from epoch_participants viewer_participant
        where viewer_participant.participant_epoch_id = epoch.id
          and viewer_participant.person_id = ${personId}
      )
      group by conversation.id, epoch.id
      order by conversation.updated_at desc
    `;
    const output: ChatView[] = [];
    for (const chat of chats) {
      const participants = await this.database<
        { person_id: string; registration_status: string; consented_at: Date | null }[]
      >`
        select person_id, registration_status, consented_at
        from epoch_participants where participant_epoch_id = ${chat.epoch_id}
        order by person_id
      `;
      const mode: ChatView["mode"] =
        chat.status === "paused" || chat.hard_suppression
          ? "paused"
          : chat.kind === "direct" && (!chat.all_registered || !chat.all_content_allowed)
            ? "registration_required"
            : chat.read_only_suppression ||
                !chat.all_direct_allowed ||
                (chat.kind === "group" &&
                  (!chat.all_registered || !chat.all_content_allowed || !chat.exact_write_rule))
              ? "observe_only"
              : "trusted_write_enabled";
      output.push({
        id: chat.id,
        kind: chat.kind === "direct" ? "direct" : "group",
        title: chat.kind === "direct" ? "Private conversation" : "Family group",
        mode,
        epochId: chat.epoch_id,
        epochStartedAt: chat.epoch_started_at.toISOString(),
        participants: participants.map((participant, index) => ({
          id: participant.person_id,
          name: participant.person_id === personId ? "You" : `Person ${index + 1}`,
          registered: participant.registration_status === "registered",
          consented: participant.consented_at !== null,
        })),
        retentionDays:
          chat.retention_seconds === null ? null : Math.floor(Number(chat.retention_seconds) / 86_400),
        proactive: chat.proactive_rule,
        blockedReason:
          mode === "registration_required"
            ? "Finish private registration and consent before Florence can use or answer ordinary messages here."
            : mode === "observe_only" && chat.kind === "group"
              ? "Florence stays silent here. Permitted messages after Florence joined are retained only as independent private context for each registered exact participant; they are never automatically shared with a household or another chat."
              : mode === "observe_only"
                ? "Florence will not answer while this private conversation is read-only under your current settings."
                : mode === "paused"
                  ? "A participant narrowed or paused this chat."
                  : null,
      });
    }
    return output;
  }

  public async people(personId: string): Promise<PeopleView> {
    const householdRows = await this.database<
      {
        id: string;
        status: string;
        viewer_role: string;
        joined_at: Date;
        capabilities: string[];
      }[]
    >`
      select household.id, household.status, viewer_membership.role as viewer_role,
        viewer_membership.joined_at,
        coalesce(array_agg(capability.capability order by capability.capability)
          filter (where capability.status = 'active'), '{}') as capabilities
      from household_memberships viewer_membership
      join households household on household.id = viewer_membership.household_id
      left join membership_capabilities capability
        on capability.membership_id = viewer_membership.id
      where viewer_membership.person_id = ${personId}
        and viewer_membership.status = 'active'
        and household.status in ('onboarding', 'active', 'paused')
        and exists(
          select 1 from membership_capabilities read_grant
          where read_grant.membership_id = viewer_membership.id
            and read_grant.capability = 'household.read'
            and read_grant.status = 'active'
        )
      group by household.id, viewer_membership.id
      order by viewer_membership.joined_at
    `;

    const households: PeopleView["households"] = [];
    for (const [householdIndex, household] of householdRows.entries()) {
      const canInvite = household.capabilities.includes("membership.invite");
      const canAddDependent = household.capabilities.includes("household.govern");
      const memberRows = await this.database<
        {
          person_id: string;
          role: string;
          person_status: string;
          display_name_ciphertext: Buffer | null;
          aliases_ciphertext: Buffer | null;
          birth_year: number | null;
          school_ciphertext: Buffer | null;
          activities_ciphertext: Buffer | null;
          joined_at: Date;
        }[]
      >`
        select member.person_id, member.role, person.status as person_status,
          person.display_name_ciphertext, profile.aliases_ciphertext,
          profile.birth_year, profile.school_ciphertext,
          profile.activities_ciphertext, member.joined_at
        from household_memberships member
        join people person on person.id = member.person_id
        left join dependent_profiles profile on profile.person_id = person.id
        where member.household_id = ${household.id} and member.status = 'active'
        order by case member.role when 'steward' then 0 when 'caregiver' then 1
          when 'participant' then 2 else 3 end, member.joined_at
      `;
      const eligibleRows = canInvite
        ? await this.database<
            {
              person_id: string;
              conversation_id: string;
              identity_id: string;
              person_status: string;
              display_name_ciphertext: Buffer | null;
              subject_ciphertext: Buffer | null;
            }[]
          >`
            select distinct on (candidate.person_id)
              candidate.person_id, conversation.id as conversation_id,
              identity.id as identity_id,
              person.status as person_status, person.display_name_ciphertext,
              identity.subject_ciphertext
            from conversations conversation
            join participant_epochs epoch on epoch.id = conversation.current_epoch_id
              and epoch.ended_at is null
            join epoch_participants viewer_participant
              on viewer_participant.participant_epoch_id = epoch.id
              and viewer_participant.person_id = ${personId}
            join epoch_participants candidate on candidate.participant_epoch_id = epoch.id
              and candidate.person_id <> ${personId}
            join people person on person.id = candidate.person_id
              and person.status in ('provisional', 'registered')
            join person_identities identity on identity.id = candidate.person_identity_id
              and identity.status in ('observed', 'verified')
            where conversation.kind = 'group' and conversation.status = 'active'
              and not exists(
                select 1 from household_memberships existing_member
                where existing_member.household_id = ${household.id}
                  and existing_member.person_id = candidate.person_id
                  and existing_member.status = 'active'
              )
              and not exists(
                select 1 from invitations pending_invitation
                join person_identities invited_identity
                  on invited_identity.id = pending_invitation.invitee_identity_id
                where pending_invitation.household_id = ${household.id}
                  and pending_invitation.status = 'pending'
                  and pending_invitation.expires_at > now()
                  and invited_identity.person_id = candidate.person_id
              )
            order by candidate.person_id, conversation.updated_at desc
          `
        : [];
      const groupRows = await this.database<
        {
          conversation_id: string;
          required_count: number | string;
          all_ready: boolean;
          active: boolean;
          approved_count: number | string;
          viewer_approved: boolean;
          updated_at: Date;
        }[]
      >`
        select conversation.id as conversation_id, conversation.updated_at,
          count(distinct participant.person_id) as required_count,
          coalesce(bool_and(
            participant.registration_status = 'registered'
            and participant.consented_at is not null
            and policy.id is not null
          ), false) as all_ready,
          exists(
            select 1 from conversation_rules active_rule
            where active_rule.conversation_id = conversation.id
              and active_rule.status = 'active'
              and active_rule.participant_set_digest = epoch.participant_set_digest
              and 'proactive_coverage' = any(active_rule.allowed_operations)
          ) as active,
          count(distinct approval.person_id) as approved_count,
          coalesce(bool_or(approval.person_id = ${personId}), false) as viewer_approved
        from conversations conversation
        join participant_epochs epoch on epoch.id = conversation.current_epoch_id
          and epoch.ended_at is null
        join epoch_participants participant on participant.participant_epoch_id = epoch.id
        left join participant_policies policy on policy.conversation_id = conversation.id
          and policy.person_id = participant.person_id and policy.status = 'active'
        left join lateral (
          select candidate_rule.id
          from conversation_rules candidate_rule
          where candidate_rule.conversation_id = conversation.id
            and candidate_rule.rule_key = 'family_coverage_proposal'
            and candidate_rule.status = 'candidate'
            and candidate_rule.participant_set_digest = epoch.participant_set_digest
          order by candidate_rule.version desc limit 1
        ) proposal on true
        left join conversation_rule_approvals approval
          on approval.conversation_rule_id = proposal.id
          and approval.participant_epoch_id = epoch.id
          and approval.participant_set_digest = epoch.participant_set_digest
        where conversation.household_id = ${household.id}
          and conversation.kind = 'group' and conversation.status = 'active'
          and exists(
            select 1 from epoch_participants viewer_participant
            where viewer_participant.participant_epoch_id = epoch.id
              and viewer_participant.person_id = ${personId}
          )
        group by conversation.id, epoch.id
        order by conversation.updated_at desc
      `;
      households.push({
        id: household.id,
        name: householdIndex === 0 ? "Your family" : `Your family ${householdIndex + 1}`,
        status: household.status,
        viewerRole: relationshipRole(household.viewer_role),
        canInvite,
        canAddDependent,
        members: memberRows.map((member) => ({
          id: member.person_id,
          name:
            member.person_id === personId
              ? "You"
              : (decryptPersonName(this.#secretBox, member.person_id, member.display_name_ciphertext) ??
                (member.role === "dependent" ? "Dependent" : "Family member")),
          role: relationshipRole(member.role),
          self: member.person_id === personId,
          represented: member.role === "dependent" && member.person_status !== "registered",
          context:
            member.role === "dependent" && member.person_status !== "registered"
              ? {
                  aliases: decryptDependentList(
                    this.#secretBox,
                    member.person_id,
                    "aliases",
                    member.aliases_ciphertext,
                  ),
                  birthYear: member.birth_year === null ? null : Number(member.birth_year),
                  school:
                    decryptDependentText(
                      this.#secretBox,
                      member.person_id,
                      "school",
                      member.school_ciphertext,
                    ) ?? "",
                  activities: decryptDependentList(
                    this.#secretBox,
                    member.person_id,
                    "activities",
                    member.activities_ciphertext,
                  ),
                }
              : null,
        })),
        eligibleParticipants: eligibleRows.map((participant) => ({
          personId: participant.person_id,
          conversationId: participant.conversation_id,
          name:
            decryptPersonName(this.#secretBox, participant.person_id, participant.display_name_ciphertext) ??
            maskedIdentityLabel(
              decryptIdentitySubject(
                this.#secretBox,
                participant.identity_id,
                participant.subject_ciphertext,
              ),
            ),
          registered: participant.person_status === "registered",
        })),
        coverageGroups: groupRows.map((group, groupIndex) => {
          const active = group.active;
          const allReady = group.all_ready;
          const viewerApproved = active || group.viewer_approved;
          const requiredCount = Number(group.required_count);
          return {
            conversationId: group.conversation_id,
            label: groupIndex === 0 ? "Family group" : `Family group ${groupIndex + 1}`,
            active,
            approvedCount: active ? requiredCount : Number(group.approved_count),
            requiredCount,
            viewerApproved,
            canApprove: allReady && !active && !viewerApproved,
            blockedReason: !allReady
              ? "Everyone in this group must finish private registration first."
              : !active && viewerApproved
                ? "You approved this. Florence is waiting for everyone else."
                : null,
          };
        }),
      });
    }

    const approvalRows = await this.database<
      {
        invitation_id: string;
        household_id: string;
        requested_role: string;
        expires_at: Date;
        invitee_person_id: string;
        display_name_ciphertext: Buffer | null;
      }[]
    >`
      select invitation.id as invitation_id, invitation.household_id,
        invitation.requested_role, invitation.expires_at,
        invitee.id as invitee_person_id, invitee.display_name_ciphertext
      from invitation_approvals approval
      join household_memberships approver_membership
        on approver_membership.id = approval.approver_membership_id
        and approver_membership.person_id = ${personId}
        and approver_membership.status = 'active'
      join invitations invitation on invitation.id = approval.invitation_id
      join households household on household.id = invitation.household_id
        and household.membership_version = invitation.household_membership_version
      join person_identities invitee_identity on invitee_identity.id = invitation.invitee_identity_id
      join people invitee on invitee.id = invitee_identity.person_id
      where approval.approved_at is null and invitation.status = 'pending'
        and invitation.expires_at > now()
      order by invitation.created_at
    `;
    const acceptanceRows = await this.database<
      {
        invitation_id: string;
        household_id: string;
        requested_role: string;
        expires_at: Date;
        remaining_approvals: number | string;
      }[]
    >`
      select invitation.id as invitation_id, invitation.household_id,
        invitation.requested_role, invitation.expires_at,
        count(approval.approver_membership_id)
          filter (where approval.approved_at is null) as remaining_approvals
      from invitations invitation
      join households household on household.id = invitation.household_id
        and household.membership_version = invitation.household_membership_version
      join person_identities invitee_identity on invitee_identity.id = invitation.invitee_identity_id
        and invitee_identity.person_id = ${personId} and invitee_identity.status = 'verified'
      left join invitation_approvals approval on approval.invitation_id = invitation.id
      where invitation.status = 'pending' and invitation.expires_at > now()
      group by invitation.id
      order by invitation.created_at
    `;
    const invitationContextRows = await this.database<
      {
        invitation_id: string;
        person_id: string;
        display_name_ciphertext: Buffer | null;
        aliases_ciphertext: Buffer | null;
        birth_year: number | null;
        school_ciphertext: Buffer | null;
        activities_ciphertext: Buffer | null;
      }[]
    >`
      select invitation.id as invitation_id, dependent_person.id as person_id,
        dependent_person.display_name_ciphertext, profile.aliases_ciphertext,
        profile.birth_year, profile.school_ciphertext, profile.activities_ciphertext
      from invitations invitation
      join households household on household.id = invitation.household_id
        and household.membership_version = invitation.household_membership_version
      join person_identities invitee_identity on invitee_identity.id = invitation.invitee_identity_id
        and invitee_identity.person_id = ${personId} and invitee_identity.status = 'verified'
      join people invitee on invitee.id = invitee_identity.person_id
        and invitee.status = 'registered'
      join household_memberships dependent_membership
        on dependent_membership.household_id = invitation.household_id
        and dependent_membership.role = 'dependent'
        and dependent_membership.status = 'active'
      join people dependent_person on dependent_person.id = dependent_membership.person_id
        and dependent_person.status = 'provisional'
      left join dependent_profiles profile on profile.person_id = dependent_person.id
      where invitation.status = 'pending' and invitation.expires_at > now()
      order by invitation.created_at, dependent_membership.joined_at
    `;
    const sharedChildrenByInvitation = new Map<
      string,
      NonNullable<PeopleView["invitations"][number]["sharedContext"]>["children"]
    >();
    for (const child of invitationContextRows) {
      const children = sharedChildrenByInvitation.get(child.invitation_id) ?? [];
      children.push({
        preferredName:
          decryptPersonName(this.#secretBox, child.person_id, child.display_name_ciphertext) ?? "Child",
        aliases: decryptDependentList(this.#secretBox, child.person_id, "aliases", child.aliases_ciphertext),
        birthYear: child.birth_year === null ? null : Number(child.birth_year),
        school:
          decryptDependentText(this.#secretBox, child.person_id, "school", child.school_ciphertext) ?? "",
        activities: decryptDependentList(
          this.#secretBox,
          child.person_id,
          "activities",
          child.activities_ciphertext,
        ),
      });
      sharedChildrenByInvitation.set(child.invitation_id, children);
    }
    const householdNames = new Map(households.map((household) => [household.id, household.name]));
    return {
      households,
      invitations: [
        ...approvalRows.map((invitation) => ({
          id: invitation.invitation_id,
          householdId: invitation.household_id,
          householdName: householdNames.get(invitation.household_id) ?? "Your family",
          personName:
            decryptPersonName(
              this.#secretBox,
              invitation.invitee_person_id,
              invitation.display_name_ciphertext,
            ) ?? "A registered group participant",
          role: invitationRole(invitation.requested_role),
          action: "approve" as const,
          canAct: true,
          detail: "A co-steward must be approved by every current steward.",
          expiresAt: invitation.expires_at.toISOString(),
          sharedContext: null,
        })),
        ...acceptanceRows.map((invitation) => {
          const ready = Number(invitation.remaining_approvals) === 0;
          const children = sharedChildrenByInvitation.get(invitation.invitation_id) ?? [];
          return {
            id: invitation.invitation_id,
            householdId: invitation.household_id,
            householdName: householdNames.get(invitation.household_id) ?? "A family",
            personName: "You",
            role: invitationRole(invitation.requested_role),
            action: "accept" as const,
            canAct: ready,
            detail: ready
              ? "This family is ready for you to join."
              : "The family’s current stewards are still approving this invitation.",
            expiresAt: invitation.expires_at.toISOString(),
            sharedContext: children.length > 0 ? { children } : null,
          };
        }),
      ],
    };
  }

  public async routines(personId: string): Promise<RoutineView> {
    const destinations = await this.database<
      {
        conversation_id: string;
        household_id: string;
        participant_count: number | string;
        can_create: boolean;
      }[]
    >`
      select conversation.id as conversation_id, conversation.household_id,
        count(participant.person_id) as participant_count,
        exists(
          select 1 from membership_capabilities capability
          where capability.membership_id = viewer_membership.id
            and capability.capability = 'coordination.originate'
            and capability.status = 'active'
        ) as can_create
      from household_memberships viewer_membership
      join membership_capabilities read_capability
        on read_capability.membership_id = viewer_membership.id
        and read_capability.capability = 'household.read' and read_capability.status = 'active'
      join households household on household.id = viewer_membership.household_id
        and household.status = 'active'
      join conversations conversation on conversation.household_id = household.id
        and conversation.kind = 'group' and conversation.status = 'active'
      join participant_epochs epoch on epoch.id = conversation.current_epoch_id
        and epoch.ended_at is null
      join epoch_participants participant on participant.participant_epoch_id = epoch.id
      join people current_person on current_person.id = participant.person_id
      join person_identities identity on identity.id = participant.person_identity_id
      left join participant_policies policy on policy.conversation_id = conversation.id
        and policy.person_id = participant.person_id and policy.status = 'active'
      where viewer_membership.person_id = ${personId} and viewer_membership.status = 'active'
        and exists(
          select 1 from epoch_participants viewer_participant
          where viewer_participant.participant_epoch_id = epoch.id
            and viewer_participant.person_id = ${personId}
        )
      group by conversation.id, viewer_membership.id
      having bool_and(
        participant.registration_status = 'registered'
        and participant.consented_at is not null
        and current_person.status = 'registered'
        and identity.status = 'verified'
        and coalesce(policy.allow_content_processing, false)
      )
      order by conversation.updated_at desc, conversation.id
    `;
    const destinationViews: RoutineView["destinations"] = destinations.map((destination, index) => ({
      conversationId: destination.conversation_id,
      householdId: destination.household_id,
      label: index === 0 ? "Family group" : `Family group ${index + 1}`,
      participantCount: Number(destination.participant_count),
      canCreate: destination.can_create,
    }));
    const destinationById = new Map(
      destinationViews.map((destination) => [destination.conversationId, destination]),
    );

    const people = await this.database<
      {
        household_id: string;
        person_id: string;
        display_name_ciphertext: Buffer | null;
      }[]
    >`
      select member.household_id, member.person_id, person.display_name_ciphertext
      from household_memberships viewer_membership
      join membership_capabilities read_capability
        on read_capability.membership_id = viewer_membership.id
        and read_capability.capability = 'household.read' and read_capability.status = 'active'
      join households household on household.id = viewer_membership.household_id
        and household.status = 'active'
      join household_memberships member on member.household_id = household.id
        and member.status = 'active' and member.role <> 'dependent'
      join people person on person.id = member.person_id and person.status = 'registered'
      where viewer_membership.person_id = ${personId} and viewer_membership.status = 'active'
      order by member.household_id, member.joined_at, member.person_id
    `;
    const peopleViews: RoutineView["people"] = people.map((person) => ({
      personId: person.person_id,
      householdId: person.household_id,
      name:
        person.person_id === personId
          ? "You"
          : (decryptPersonName(this.#secretBox, person.person_id, person.display_name_ciphertext) ??
            "Family member"),
      self: person.person_id === personId,
    }));
    const personNames = new Map(peopleViews.map((person) => [person.personId, person.name]));

    const rows = await this.database<
      {
        routine_id: string;
        household_id: string;
        status: string;
        current_revision: number | string;
        version: number | string;
        content_ciphertext: Buffer;
        content_key_version: string;
        recurrence: unknown;
        semantic_time_plan: unknown;
        notification_mode: string;
        destination_conversation_id: string;
        proposed_holder_person_id: string | null;
        standing_holder_person_id: string | null;
        can_revise: boolean;
        can_manage: boolean;
        participant_count: number | string;
      }[]
    >`
      select routine.id as routine_id, routine.household_id, routine.status,
        routine.current_revision, routine.version, revision.content_ciphertext,
        revision.content_key_version, revision.recurrence, revision.semantic_time_plan,
        revision.notification_mode, revision.destination_conversation_id,
        revision.proposed_holder_person_id, revision.standing_holder_person_id,
        exists(
          select 1 from membership_capabilities capability
          where capability.membership_id = viewer_membership.id
            and capability.capability = 'coordination.originate' and capability.status = 'active'
        ) as can_revise,
        exists(
          select 1 from membership_capabilities capability
          where capability.membership_id = viewer_membership.id
            and capability.capability = 'coordination.coordinate' and capability.status = 'active'
        ) as can_manage,
        (
          select count(*) from epoch_participants participant
          join conversations conversation on conversation.current_epoch_id = participant.participant_epoch_id
          where conversation.id = revision.destination_conversation_id
        ) as participant_count
      from household_memberships viewer_membership
      join membership_capabilities read_capability
        on read_capability.membership_id = viewer_membership.id
        and read_capability.capability = 'household.read' and read_capability.status = 'active'
      join households household on household.id = viewer_membership.household_id
        and household.status = 'active'
      join routines routine on routine.household_id = household.id
      join routine_revisions revision on revision.routine_id = routine.id
        and revision.revision = routine.current_revision
      where viewer_membership.person_id = ${personId} and viewer_membership.status = 'active'
      order by case routine.status when 'active' then 0 when 'paused' then 1 else 2 end,
        routine.updated_at desc, routine.id
    `;

    const routines: RoutineView["routines"] = rows.map((row) => {
      const content = decryptRoutineContent(
        this.#secretBox,
        row.routine_id,
        Number(row.current_revision),
        row.content_ciphertext,
        row.content_key_version,
      );
      const shape = editableWeeklyRoutineShape(row.recurrence, row.semantic_time_plan);
      const destination = destinationById.get(row.destination_conversation_id);
      const destinationLabel = destination?.label ?? `Family group · ${Number(row.participant_count)} people`;
      const holderId = row.proposed_holder_person_id;
      const status = routineStatus(row.status);
      return {
        id: row.routine_id,
        householdId: row.household_id,
        title: content?.title ?? "Routine details unavailable",
        sharedMeaning: content?.sharedMeaning ?? "Florence cannot currently open these details.",
        cadence: shape ? cadenceLabel(shape.weekdays) : "Recurring routine",
        time: shape ? `${friendlyLocalTime(shape.localEventTime)} · ${shape.timeZone}` : "Timing unavailable",
        destination: {
          conversationId: row.destination_conversation_id,
          label: destinationLabel,
        },
        status,
        holder: holderId
          ? {
              personId: holderId,
              name: personNames.get(holderId) ?? "Family member",
              standing: row.standing_holder_person_id === holderId,
            }
          : null,
        version: Number(row.version),
        canRevise:
          row.can_revise &&
          status !== "retired" &&
          content !== null &&
          shape !== null &&
          destination !== undefined,
        canManage: row.can_manage && status !== "retired",
        weekdays: shape?.weekdays ?? [],
        startsOn: shape?.startsOn ?? "",
        endsOn: shape?.endsOn ?? null,
        timeZone: shape?.timeZone ?? "UTC",
        localEventTime: shape?.localEventTime ?? "09:00",
        earliestUsefulMinutesBefore: shape?.earliestUsefulMinutesBefore ?? 180,
        lastResponsibleMinutesBefore: shape?.lastResponsibleMinutesBefore ?? 30,
        notificationMode: notificationMode(row.notification_mode),
        standingSelfCoverage:
          row.standing_holder_person_id === personId && row.proposed_holder_person_id === personId,
      };
    });
    return { destinations: destinationViews, people: peopleViews, routines };
  }

  public async sources(personId: string): Promise<SourceView> {
    const integrations = await this.database<
      {
        id: string;
        status: string;
        account_kind: string;
        control_epoch: number | string;
        connected_at: Date;
        updated_at: Date;
      }[]
    >`
      select id, status, account_kind, control_epoch, connected_at, updated_at
      from integrations
      where person_id = ${personId} and provider = 'google'
        and status <> 'revoked'
      order by connected_at
    `;
    const integrationIds = integrations.map((integration) => integration.id);
    const cursors =
      integrationIds.length === 0
        ? []
        : await this.database<
            {
              integration_id: string;
              resource_kind: string;
              state: string;
              checkpoint_at: Date | null;
              updated_at: Date;
            }[]
          >`
          select integration_id, resource_kind, state, checkpoint_at, updated_at
          from sync_cursors
          where integration_id = any(${this.database.array(integrationIds)}::uuid[])
        `;
    const calendarPolicies =
      integrationIds.length === 0
        ? []
        : await this.database<{ integration_id: string; calendar_id_digest: string; mode: string }[]>`
          select integration_id, scope->>'calendarIdDigest' as calendar_id_digest,
            scope->>'mode' as mode
          from integration_grants
          where integration_id = any(${this.database.array(integrationIds)}::uuid[])
            and grant_kind = 'calendar_privacy' and status = 'active'
        `;
    const googleJobHealth =
      integrationIds.length === 0
        ? []
        : await this.database<
            {
              integration_id: string;
              live_status: string | null;
              calendar_catalog_status: string | null;
              newest_status: string | null;
              middle_status: string | null;
              year_status: string | null;
              older_status: string | null;
              message_pending: boolean;
              message_dead: boolean;
              interpretation_pending: boolean;
              interpretation_dead: boolean;
              calendar_poll_failed: boolean;
            }[]
          >`
          select integration.id as integration_id,
            live.status as live_status,
            calendar_catalog.status as calendar_catalog_status,
            newest.status as newest_status,
            middle.status as middle_status,
            year_stage.status as year_status,
            older.status as older_status,
            exists(
              select 1 from jobs message
              where message.integration_id = integration.id
                and message.integration_control_epoch = integration.control_epoch
                and message.job_kind = 'google.gmail.message'
                and message.status in ('pending', 'retry', 'leased')
            ) as message_pending,
            exists(
              select 1 from jobs message
              where message.integration_id = integration.id
                and message.integration_control_epoch = integration.control_epoch
                and message.job_kind = 'google.gmail.message'
                and message.status = 'dead'
            ) as message_dead,
            exists(
              select 1 from jobs interpretation
              where interpretation.integration_id = integration.id
                and interpretation.integration_control_epoch = integration.control_epoch
                and interpretation.job_kind = 'orchestrate.private_source'
                and interpretation.status in ('pending', 'retry', 'leased')
            ) as interpretation_pending,
            exists(
              select 1 from jobs interpretation
              where interpretation.integration_id = integration.id
                and interpretation.integration_control_epoch = integration.control_epoch
                and interpretation.job_kind = 'orchestrate.private_source'
                and interpretation.status in ('attention', 'dead')
            ) as interpretation_dead,
            exists(
              select 1
              from integration_grants grant_row
              left join sync_cursors event_cursor
                on event_cursor.integration_id = integration.id
                and event_cursor.resource_kind = 'calendar:' || (grant_row.scope->>'calendarIdDigest')
              where grant_row.integration_id = integration.id
                and grant_row.grant_kind = 'calendar_privacy'
                and grant_row.status = 'active'
                and grant_row.scope->>'mode' <> 'off'
                and exists(
                  select 1 from jobs failed_poll
                  where failed_poll.integration_id = integration.id
                    and failed_poll.integration_control_epoch = integration.control_epoch
                    and failed_poll.job_kind = 'google.calendar.poll'
                    and failed_poll.status = 'dead'
                    and failed_poll.idempotency_key like
                      'calendar:poll:' || integration.id || ':e' || integration.control_epoch || ':' ||
                      (grant_row.scope->>'calendarIdDigest') || ':v' || grant_row.version || ':' ||
                      (grant_row.scope->>'mode') || ':%'
                    and (event_cursor.updated_at is null or failed_poll.updated_at >= event_cursor.updated_at)
                )
            ) as calendar_poll_failed
          from integrations integration
          left join lateral (
            select job.status from jobs job
            where job.integration_id = integration.id
              and job.integration_control_epoch = integration.control_epoch
              and job.job_kind in ('google.bootstrap', 'google.gmail.bootstrap', 'google.gmail.poll')
            order by job.updated_at desc limit 1
          ) live on true
          left join lateral (
            select job.status from jobs job
            where job.integration_id = integration.id
              and job.integration_control_epoch = integration.control_epoch
              and job.job_kind = 'google.calendar.catalog'
            order by job.updated_at desc limit 1
          ) calendar_catalog on true
          left join lateral (
            select job.status from jobs job
            where job.integration_id = integration.id
              and job.integration_control_epoch = integration.control_epoch
              and job.job_kind = 'google.gmail.backfill'
              and job.idempotency_key like '%:newest_30_days:%'
            order by job.updated_at desc limit 1
          ) newest on true
          left join lateral (
            select job.status from jobs job
            where job.integration_id = integration.id
              and job.integration_control_epoch = integration.control_epoch
              and job.job_kind = 'google.gmail.backfill'
              and job.idempotency_key like '%:days_31_to_90:%'
            order by job.updated_at desc limit 1
          ) middle on true
          left join lateral (
            select job.status from jobs job
            where job.integration_id = integration.id
              and job.integration_control_epoch = integration.control_epoch
              and job.job_kind = 'google.gmail.backfill'
              and job.idempotency_key like '%:days_91_to_365:%'
            order by job.updated_at desc limit 1
          ) year_stage on true
          left join lateral (
            select job.status from jobs job
            where job.integration_id = integration.id
              and job.integration_control_epoch = integration.control_epoch
              and job.job_kind = 'google.gmail.backfill'
              and job.idempotency_key like '%:older_history:%'
            order by job.updated_at desc limit 1
          ) older on true
          where integration.id = any(${this.database.array(integrationIds)}::uuid[])
        `;
    const jobHealthByIntegration = new Map(
      googleJobHealth.map((health) => [health.integration_id, health] as const),
    );

    const connections: SourceView["connections"] = [];
    for (const integration of integrations) {
      const controlEpoch = Number(integration.control_epoch);
      const profile = await this.#sources
        .read({
          kind: "integration_profile",
          integrationId: integration.id,
          personId,
          expectedControlEpoch: controlEpoch,
        })
        .catch(() => null);
      const accountEmail =
        profile?.kind === "integration_profile" ? profile.accountEmail : "Account email unavailable";
      const accountKind = integration.account_kind === "work" ? "work" : "personal_family";
      const activeCapabilities = new Set(
        profile?.kind === "integration_profile" ? profile.integration.activeCapabilities : [],
      );
      const allConnectionCursors = cursors.filter((cursor) => cursor.integration_id === integration.id);
      const connectionCursors = allConnectionCursors.filter(
        (cursor) => cursor.updated_at >= integration.connected_at,
      );
      const jobHealth = jobHealthByIntegration.get(integration.id);
      const gmailHistory = connectionCursors.find((cursor) => cursor.resource_kind === "gmail_history");
      const backfillKinds: Array<
        NonNullable<SourceView["connections"][number]["mail"]>["milestones"][number]["id"]
      > = ["newest_30_days", "days_31_to_90", "days_91_to_365"];
      const olderHistoryEnabled =
        allConnectionCursors.some((cursor) => cursor.resource_kind === "gmail_backfill:older_history") ||
        (jobHealth !== undefined && jobHealth.older_status !== null);
      if (olderHistoryEnabled) backfillKinds.push("older_history");
      const gmailJobFailed = jobHealth?.live_status === "dead";
      const interpretationPending = jobHealth?.interpretation_pending ?? false;
      const interpretationDead = jobHealth?.interpretation_dead ?? false;
      const backfillJobStatuses = {
        newest_30_days: jobHealth?.newest_status ?? null,
        days_31_to_90: jobHealth?.middle_status ?? null,
        days_91_to_365: jobHealth?.year_status ?? null,
        older_history: jobHealth?.older_status ?? null,
      } satisfies Record<(typeof backfillKinds)[number], string | null>;
      const historyMilestones = backfillKinds.map((stage) => {
        const cursor = connectionCursors.find(
          (candidate) => candidate.resource_kind === `gmail_backfill:${stage}`,
        );
        const state = mailHistoryMilestoneState(
          integration.status,
          cursor?.state ?? null,
          backfillJobStatuses[stage],
        );
        return {
          id: stage,
          ...mailHistoryMilestoneCopy(stage),
          state,
          stateLabel: syncMilestoneStateLabel(state),
        };
      });
      const liveState: NonNullable<SourceView["connections"][number]["mail"]>["liveState"] =
        integration.status === "paused"
          ? "paused"
          : integration.status === "reauth_required" ||
              integration.status === "error" ||
              gmailHistory?.state === "expired" ||
              gmailHistory?.state === "error" ||
              gmailJobFailed
            ? "needs_attention"
            : gmailHistory?.state === "active"
              ? "watching"
              : "waiting";
      const liveLabel =
        liveState === "watching"
          ? "Keeping up with new mail"
          : liveState === "paused"
            ? "Mail monitoring is paused"
            : liveState === "needs_attention"
              ? "Mail needs to be reconnected"
              : "Mail monitoring is starting";
      const historyState: NonNullable<SourceView["connections"][number]["mail"]>["historyState"] =
        historyMilestones.some((milestone) => milestone.state === "needs_attention") ||
        jobHealth?.message_dead ||
        interpretationDead
          ? "needs_attention"
          : historyMilestones.some((milestone) => milestone.state === "running") ||
              jobHealth?.message_pending ||
              interpretationPending
            ? "running"
            : historyMilestones.every((milestone) => milestone.state === "complete")
              ? "complete"
              : "waiting";
      const historyLabel =
        historyState === "needs_attention"
          ? interpretationDead
            ? "Some imported sources need attention"
            : "Some mail needs attention"
          : historyState === "running"
            ? interpretationPending
              ? "Florence is reviewing imported sources"
              : jobHealth?.message_pending
                ? "Florence is importing queued mail"
                : "Florence is scanning earlier mail"
            : historyState === "complete"
              ? "Earlier mail is imported"
              : integration.status === "paused"
                ? "Waiting while this connection is paused"
                : "Waiting to check earlier mail";

      let calendarCatalog: readonly CalendarCatalogEntry[] = [];
      let calendarCatalogLabel = "Looking for your calendars…";
      const calendarCatalogCursor = connectionCursors.find(
        (cursor) => cursor.resource_kind === "calendar_catalog",
      );
      try {
        const catalog = await this.#sources.read({
          kind: "sync_cursor",
          integrationId: integration.id,
          personId,
          expectedIntegrationControlEpoch: controlEpoch,
          resourceKind: "calendar_catalog",
        });
        if (catalog.kind === "sync_cursor") {
          calendarCatalog = parseCalendarCatalog(catalog.cursor);
          calendarCatalogLabel =
            calendarCatalog.length === 0
              ? "No calendars found"
              : `${calendarCatalog.length} calendar${calendarCatalog.length === 1 ? "" : "s"} found`;
        }
      } catch {
        if (integration.status === "reauth_required" || integration.status === "error") {
          calendarCatalogLabel = "Calendar access needs attention";
        }
      }
      const connectionCalendarPolicies = calendarPolicies.filter(
        (policy) => policy.integration_id === integration.id,
      );
      const enabledCalendarDigests = connectionCalendarPolicies
        .filter((policy) => calendarMode(policy.mode) !== "off")
        .map((policy) => policy.calendar_id_digest);
      const enabledCalendarCursors = enabledCalendarDigests.map((digest) =>
        connectionCursors.find((cursor) => cursor.resource_kind === `calendar:${digest}`),
      );
      const calendarResourcesFailed = enabledCalendarCursors.some(
        (cursor) => cursor?.state === "expired" || cursor?.state === "error",
      );
      const calendarResourcesReady = enabledCalendarCursors.every((cursor) => cursor?.state === "active");
      const calendarCatalogFailed = jobHealth?.calendar_catalog_status === "dead";
      const calendarPollFailed = jobHealth?.calendar_poll_failed ?? false;
      const calendarAccessFailed =
        integration.status === "reauth_required" ||
        integration.status === "error" ||
        calendarCatalogCursor?.state === "expired" ||
        calendarCatalogCursor?.state === "error" ||
        calendarResourcesFailed;
      const calendarSyncFailed = calendarCatalogFailed || calendarPollFailed;
      const calendarSyncState: NonNullable<SourceView["connections"][number]["calendar"]>["syncState"] =
        integration.status === "paused"
          ? "paused"
          : calendarAccessFailed || calendarSyncFailed || interpretationDead
            ? "needs_attention"
            : calendarCatalogCursor?.state === "active" && calendarResourcesReady && !interpretationPending
              ? "ready"
              : "waiting";
      const calendarSyncLabel =
        calendarSyncState === "ready"
          ? "Calendars are up to date"
          : calendarSyncState === "paused"
            ? "Calendar monitoring is paused"
            : calendarSyncState === "needs_attention"
              ? interpretationDead
                ? "Some imported sources need attention"
                : calendarAccessFailed
                  ? "Calendar needs to be reconnected"
                  : "Some calendar information needs attention"
              : interpretationPending
                ? "Florence is reviewing imported sources"
                : "Calendar sync is starting";
      const policyByCalendar = new Map(
        connectionCalendarPolicies.map(
          (policy) => [policy.calendar_id_digest, calendarMode(policy.mode)] as const,
        ),
      );
      const childNeedsAttention =
        (activeCapabilities.has("mail") &&
          (liveState === "needs_attention" || historyState === "needs_attention")) ||
        (activeCapabilities.has("calendar") && calendarSyncState === "needs_attention");
      connections.push({
        id: integration.id,
        label: "Google",
        email: accountEmail,
        accountKind,
        accountKindLabel: accountKind === "work" ? "Work calendar" : "Personal & family",
        status: integration.status,
        statusLabel: childNeedsAttention ? "Needs attention" : integrationStatusLabel(integration.status),
        mail: activeCapabilities.has("mail")
          ? {
              liveState,
              liveLabel,
              lastCheckedAt: gmailHistory?.checkpoint_at?.toISOString() ?? null,
              historyState,
              historyLabel,
              milestones: historyMilestones,
            }
          : null,
        calendar: activeCapabilities.has("calendar")
          ? {
              syncState: calendarSyncState,
              syncLabel: calendarSyncLabel,
              catalogLabel: calendarCatalogLabel,
              lastCheckedAt: calendarCatalogCursor?.checkpoint_at?.toISOString() ?? null,
            }
          : null,
        calendars: activeCapabilities.has("calendar")
          ? calendarCatalog
              .filter((calendar) => !calendar.deleted)
              .map((calendar) => ({
                id: calendar.id,
                name: calendar.summary,
                primary: calendar.primary,
                timezone: calendar.timezone,
                mode: policyByCalendar.get(sha256Hex(calendar.id)) ?? "off",
              }))
          : [],
      });
    }

    const reviewResult = await this.#sources.read({
      kind: "pending_private_candidates",
      personId,
      asOf: new Date().toISOString(),
      limit: 50,
    });
    const reviews = reviewResult.kind === "pending_private_candidates" ? reviewResult.candidates : [];
    const evidenceIds = [...new Set(reviews.flatMap((review) => [...review.evidenceSourceRevisionIds]))];
    const evidence =
      evidenceIds.length === 0
        ? []
        : await this.database<{ revision_id: string; provider: string; object_kind: string }[]>`
          select revision.id as revision_id, object.provider, object.object_kind
          from source_revisions revision
          join source_objects object on object.id = revision.source_object_id
          where revision.id = any(${this.database.array(evidenceIds)}::uuid[])
            and revision.owner_person_id = ${personId}
        `;
    const destinations = await new PrivateSourceBridge(this.database, this.#secretBox).listDestinations(
      personId,
    );
    const updateLoopIds = [
      ...new Set(
        reviews.flatMap((review) => {
          const loopId = optionalString(review.content.existingLoopId);
          return review.candidateKind === "coverage_loop_update_review" && loopId ? [loopId] : [];
        }),
      ),
    ];
    const updateLoopRows =
      updateLoopIds.length === 0
        ? []
        : await this.database<{ readonly id: string; readonly conversation_id: string }[]>`
          select loop.id, loop.destination_conversation_id as conversation_id
          from coverage_loops loop
          join household_memberships membership on membership.household_id = loop.household_id
            and membership.person_id = ${personId} and membership.status = 'active'
          where loop.id = any(${this.database.array(updateLoopIds)}::uuid[])
        `;
    const updateDestinationByLoop = new Map(
      updateLoopRows.map((row) => [row.id, row.conversation_id] as const),
    );
    const intentRows = await this.database<
      {
        id: string;
        action_digest: string;
        data_digest: string;
        policy_digest: string;
        target_digest: string;
        payload_ciphertext: Buffer;
        status: string;
      }[]
    >`
      select id, action_digest, data_digest, policy_digest, target_digest,
        payload_ciphertext, status
      from action_intents
      where person_id = ${personId} and action_kind = 'private_source_to_coverage_loop'
        and status in ('proposed', 'awaiting_approval', 'approved', 'executing') and expires_at > now()
      order by updated_at desc limit 100
    `;
    const intentsByCandidate = new Map<
      string,
      {
        id: string;
        status: string;
        payload: ReturnType<typeof openPrivateBridgePayload>;
        digests: {
          actionDigest: string;
          dataDigest: string;
          policyDigest: string;
          targetDigest: string;
        };
      }
    >();
    for (const intent of intentRows) {
      try {
        const payload = openPrivateBridgePayload(this.#secretBox, intent.id, intent.payload_ciphertext);
        if (!intentsByCandidate.has(payload.candidateId)) {
          intentsByCandidate.set(payload.candidateId, {
            id: intent.id,
            status: intent.status,
            payload,
            digests: {
              actionDigest: intent.action_digest,
              dataDigest: intent.data_digest,
              policyDigest: intent.policy_digest,
              targetDigest: intent.target_digest,
            },
          });
        }
      } catch {
        // A stale or old encrypted intent is not a control the owner can safely act on.
      }
    }
    const privateReviews: SourceView["privateReviews"] = reviews.map((review) => {
      const requiredOutcome = optionalString(review.content.requiredOutcome);
      const changedFact = optionalString(review.content.changedFact);
      const timeFacts = stringValues(review.content.timeFacts);
      const uncertainties = stringValues(review.content.uncertainties);
      const existingLoopId = optionalString(review.content.existingLoopId);
      const sourceLabels = [
        ...new Set(
          evidence
            .filter((item) => review.evidenceSourceRevisionIds.includes(item.revision_id))
            .map((item) => sourceLabel(item.provider, item.object_kind)),
        ),
      ];
      const intent = intentsByCandidate.get(review.candidateId);
      const proposed =
        intent?.status === "awaiting_approval" && intent.payload.phase === "awaiting_approval"
          ? intent.payload
          : null;
      const standingRuleLabel =
        proposed?.sourcePattern?.kind === "gmail_thread"
          ? "Future coverage items from this exact email thread"
          : proposed?.sourcePattern?.kind === "calendar_series"
            ? "Future coverage items from this exact recurring calendar series"
            : null;
      return {
        id: review.candidateId,
        kind: review.candidateKind,
        title: candidateTitle(review.candidateKind),
        summary: requiredOutcome ?? changedFact ?? "Florence found something that may matter to your family.",
        details: [
          ...(changedFact && changedFact !== requiredOutcome ? [changedFact] : []),
          ...timeFacts,
          ...uncertainties.map((uncertainty) => `Still unclear: ${uncertainty}`),
          ...(review.candidateKind === "coverage_proposal"
            ? ["Keeping this stays private until you choose where it should go."]
            : []),
        ],
        sourceLabel: sourceLabels.join(" + ") || "Your private source",
        proposedAt: review.proposedAt,
        expiresAt: review.expiresAt,
        destinations:
          review.candidateKind === "coverage_proposal"
            ? destinations
            : existingLoopId
              ? destinations.filter(
                  (destination) => destination.conversationId === updateDestinationByLoop.get(existingLoopId),
                )
              : [],
        preparingShare:
          intent?.status === "proposed" || intent?.status === "approved" || intent?.status === "executing",
        shareProposal:
          proposed && intent
            ? {
                actionIntentId: intent.id,
                outboundText: privateBridgeOutboundText(proposed),
                canCreateStandingRule:
                  proposed.sourcePattern !== null &&
                  !("loopUpdate" in proposed && proposed.loopUpdate !== null),
                standingRuleLabel,
                ...intent.digests,
              }
            : null,
      };
    });
    const memories = await this.database<
      { id: string; memory_key: string; scope_kind: string; effective_at: Date }[]
    >`
      select memory.id, memory.memory_key, memory.scope_kind, revision.effective_at
      from memory_records memory
      join memory_revisions revision on revision.id = memory.current_revision_id
      where memory.owner_person_id = ${personId} and memory.status = 'accepted'
      order by revision.effective_at desc limit 100
    `;
    const rules = await this.database<{ id: string; rule_key: string; destination_kind: string }[]>`
      select id, rule_key,
        case when destination_conversation_id is not null then 'one chat' else 'your family' end as destination_kind
      from bridge_rules
      where owner_person_id = ${personId} and status = 'active'
      order by updated_at desc
    `;
    return {
      connections,
      privateReviews,
      memories: memories.map((memory) => ({
        id: memory.id,
        label: memory.memory_key.split(":", 1)[0]?.replaceAll("_", " ") ?? "private memory",
        scope: memory.scope_kind,
        source: "Approved source evidence",
        asOf: memory.effective_at.toLocaleDateString(),
      })),
      rules: rules.map((rule) => ({
        id: rule.id,
        label: rule.rule_key.replaceAll("_", " "),
        source: "Your private sources",
        destination: rule.destination_kind,
      })),
    };
  }

  public async safety(personId: string, currentSessionId: string): Promise<DataSafetyView> {
    const people = await this.database<{ proactive_paused: boolean }[]>`
      select coalesce((quiet_hours ->> 'proactivePaused')::boolean, false) as proactive_paused
      from people where id = ${personId}
    `;
    const sessions = await this.database<{ id: string; created_at: Date; last_seen_at: Date }[]>`
      select id, created_at, last_seen_at from person_sessions
      where person_id = ${personId} and revoked_at is null
        and idle_expires_at > now() and absolute_expires_at > now()
      order by last_seen_at desc
    `;
    const deletion = await this.database<{ status: string; requested_at: Date }[]>`
      select status, requested_at from deletion_requests
      where target_person_id = ${personId}
      order by requested_at desc limit 1
    `;
    const integrations = await this.database<
      { id: string; provider: string; status: string; control_epoch: number | string }[]
    >`
      select id, provider, status, control_epoch
      from integrations
      where person_id = ${personId} and status <> 'revoked'
      order by connected_at
    `;
    const connections: DataSafetyView["connections"] = [];
    for (const integration of integrations) {
      if (integration.provider !== "google") continue;
      const profile = await this.#sources
        .read({
          kind: "integration_profile",
          integrationId: integration.id,
          personId,
          expectedControlEpoch: Number(integration.control_epoch),
        })
        .catch(() => null);
      connections.push({
        id: integration.id,
        provider: "google",
        email: profile?.kind === "integration_profile" ? profile.accountEmail : "Google account",
        status: integration.status,
      });
    }
    return {
      paused: people[0]?.proactive_paused ?? false,
      sessions: sessions.map((session) => ({
        id: session.id,
        createdAt: session.created_at.toISOString(),
        lastSeenAt: session.last_seen_at.toISOString(),
        current: session.id === currentSessionId,
      })),
      connections,
      deletion: deletion[0]
        ? { status: deletion[0].status, requestedAt: deletion[0].requested_at.toISOString() }
        : null,
    };
  }
}

function relationshipRole(value: string): PeopleView["households"][number]["members"][number]["role"] {
  switch (value) {
    case "steward":
    case "caregiver":
    case "participant":
    case "dependent":
      return value;
    default:
      return "participant";
  }
}

function invitationRole(value: string): PeopleView["invitations"][number]["role"] {
  return value === "steward" || value === "caregiver" ? value : "participant";
}

function decryptPersonName(secretBox: SecretBox, personId: string, ciphertext: Buffer | null): string | null {
  if (!ciphertext) return null;
  try {
    const encrypted = JSON.parse(ciphertext.toString("utf8")) as unknown;
    const name = secretBox
      .decrypt(encrypted, `person-display-name:${personId}`)
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

function maskedIdentityLabel(subject: string | null): string {
  if (!subject) return "Unregistered group participant";
  if (/^\+[1-9]\d{6,14}$/u.test(subject)) return `Group participant ending ${subject.slice(-4)}`;
  const at = subject.lastIndexOf("@");
  if (at > 0) return `Group participant at ${subject.slice(at + 1)}`;
  return "Unregistered group participant";
}

function decryptDependentText(
  secretBox: SecretBox,
  personId: string,
  field: "school",
  ciphertext: Buffer | null,
): string | null {
  if (!ciphertext) return null;
  try {
    const value = secretBox
      .decrypt(JSON.parse(ciphertext.toString("utf8")) as unknown, `dependent-${field}:${personId}`)
      .toString("utf8")
      .replace(/\s+/gu, " ")
      .trim();
    return value.length > 0 && value.length <= 160 ? value : null;
  } catch {
    return null;
  }
}

function decryptDependentList(
  secretBox: SecretBox,
  personId: string,
  field: "aliases" | "activities",
  ciphertext: Buffer | null,
): string[] {
  if (!ciphertext) return [];
  try {
    const value = JSON.parse(
      secretBox
        .decrypt(JSON.parse(ciphertext.toString("utf8")) as unknown, `dependent-${field}:${personId}`)
        .toString("utf8"),
    ) as unknown;
    if (!Array.isArray(value)) return [];
    return value.filter(
      (entry): entry is string =>
        typeof entry === "string" && entry.trim().length > 0 && entry.trim().length <= 120,
    );
  } catch {
    return [];
  }
}

function decryptCoverageMeaning(secretBox: SecretBox, ciphertext: Buffer, keyVersion: string): string | null {
  try {
    const encrypted = JSON.parse(ciphertext.toString("utf8")) as unknown;
    if (!isRecord(encrypted) || encrypted.kid !== keyVersion) return null;
    const content = JSON.parse(
      secretBox.decrypt(encrypted, "coverage-loop-content").toString("utf8"),
    ) as unknown;
    if (!isRecord(content) || typeof content.minimumSharedMeaning !== "string") return null;
    const meaning = content.minimumSharedMeaning.replace(/\s+/gu, " ").trim();
    return meaning.length > 0 && meaning.length <= 500 ? meaning : null;
  } catch {
    return null;
  }
}

function decryptRoutineContent(
  secretBox: SecretBox,
  routineId: string,
  revision: number,
  ciphertext: Buffer,
  keyVersion: string,
): { readonly title: string; readonly sharedMeaning: string } | null {
  try {
    const encrypted = JSON.parse(ciphertext.toString("utf8")) as unknown;
    if (!isRecord(encrypted) || encrypted.kid !== keyVersion) return null;
    const content = JSON.parse(
      secretBox.decrypt(encrypted, `routine-revision-content:${routineId}:${revision}`).toString("utf8"),
    ) as unknown;
    if (
      !isRecord(content) ||
      typeof content.title !== "string" ||
      typeof content.minimumSharedMeaning !== "string"
    ) {
      return null;
    }
    return { title: content.title, sharedMeaning: content.minimumSharedMeaning };
  } catch {
    return null;
  }
}

interface EditableWeeklyRoutineShape {
  readonly weekdays: number[];
  readonly startsOn: string;
  readonly endsOn: string | null;
  readonly timeZone: string;
  readonly localEventTime: string;
  readonly earliestUsefulMinutesBefore: number;
  readonly lastResponsibleMinutesBefore: number;
}

function editableWeeklyRoutineShape(
  recurrenceCandidate: unknown,
  timePlanCandidate: unknown,
): EditableWeeklyRoutineShape | null {
  if (!isRecord(recurrenceCandidate) || recurrenceCandidate.kind !== "weekly") return null;
  if (!Array.isArray(recurrenceCandidate.weekdays)) return null;
  const weekdays = recurrenceCandidate.weekdays.filter(
    (weekday): weekday is number => Number.isInteger(weekday) && Number(weekday) >= 1 && Number(weekday) <= 7,
  );
  if (weekdays.length !== recurrenceCandidate.weekdays.length || weekdays.length === 0) return null;
  if (
    typeof recurrenceCandidate.startsOn !== "string" ||
    (recurrenceCandidate.endsOn !== null && typeof recurrenceCandidate.endsOn !== "string") ||
    !isRecord(timePlanCandidate) ||
    typeof timePlanCandidate.timeZone !== "string" ||
    !isRecord(timePlanCandidate.event) ||
    timePlanCandidate.event.kind !== "local_clock" ||
    timePlanCandidate.event.dayOffset !== 0 ||
    typeof timePlanCandidate.event.time !== "string" ||
    !isRecord(timePlanCandidate.earliestUseful) ||
    timePlanCandidate.earliestUseful.kind !== "relative" ||
    timePlanCandidate.earliestUseful.anchor !== "event" ||
    typeof timePlanCandidate.earliestUseful.offsetMinutes !== "number" ||
    timePlanCandidate.earliestUseful.offsetMinutes > 0 ||
    !isRecord(timePlanCandidate.lastResponsible) ||
    timePlanCandidate.lastResponsible.kind !== "relative" ||
    timePlanCandidate.lastResponsible.anchor !== "event" ||
    typeof timePlanCandidate.lastResponsible.offsetMinutes !== "number" ||
    timePlanCandidate.lastResponsible.offsetMinutes > 0
  ) {
    return null;
  }
  return {
    weekdays,
    startsOn: recurrenceCandidate.startsOn,
    endsOn: recurrenceCandidate.endsOn,
    timeZone: timePlanCandidate.timeZone,
    localEventTime: timePlanCandidate.event.time,
    earliestUsefulMinutesBefore: -timePlanCandidate.earliestUseful.offsetMinutes,
    lastResponsibleMinutesBefore: -timePlanCandidate.lastResponsible.offsetMinutes,
  };
}

function routineStatus(value: string): RoutineView["routines"][number]["status"] {
  return value === "paused" || value === "retired" ? value : "active";
}

function notificationMode(value: string): RoutineView["routines"][number]["notificationMode"] {
  return value === "always" || value === "silent" ? value : "exceptions_only";
}

function cadenceLabel(weekdays: readonly number[]): string {
  const labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
  return `Every ${weekdays
    .map((weekday) => labels[weekday - 1])
    .filter(Boolean)
    .join(", ")}`;
}

function friendlyLocalTime(value: string): string {
  const [hourCandidate, minute = "00"] = value.split(":");
  const hour = Number(hourCandidate);
  if (!Number.isInteger(hour) || hour < 0 || hour > 23) return value;
  const suffix = hour >= 12 ? "PM" : "AM";
  const displayHour = hour % 12 || 12;
  return `${displayHour}:${minute} ${suffix}`;
}

interface CalendarCatalogEntry {
  readonly id: string;
  readonly summary: string;
  readonly primary: boolean;
  readonly timezone: string | null;
  readonly deleted: boolean;
}

function parseCalendarCatalog(candidate: unknown): readonly CalendarCatalogEntry[] {
  if (!Array.isArray(candidate)) return [];
  return candidate.flatMap((entry) => {
    if (!isRecord(entry) || typeof entry.id !== "string" || typeof entry.summary !== "string") return [];
    return [
      {
        id: entry.id,
        summary: entry.summary,
        primary: entry.primary === true,
        timezone: typeof entry.timezone === "string" ? entry.timezone : null,
        deleted: entry.deleted === true,
      },
    ];
  });
}

function integrationStatusLabel(status: string): string {
  switch (status) {
    case "active":
      return "Connected privately";
    case "paused":
      return "Paused";
    case "reauth_required":
      return "Reconnect needed";
    case "error":
      return "Needs attention";
    default:
      return "Unavailable";
  }
}

function mailHistoryMilestoneState(
  integrationStatus: string,
  cursorState: string | null,
  jobStatus: string | null,
): NonNullable<SourceView["connections"][number]["mail"]>["milestones"][number]["state"] {
  if (cursorState === "exhausted") return "complete";
  if (integrationStatus === "paused") return "waiting";
  if (
    integrationStatus === "reauth_required" ||
    integrationStatus === "error" ||
    cursorState === "expired" ||
    cursorState === "error" ||
    jobStatus === "dead"
  ) {
    return "needs_attention";
  }
  if (
    jobStatus === "pending" ||
    jobStatus === "retry" ||
    jobStatus === "leased" ||
    jobStatus === "succeeded"
  ) {
    return "running";
  }
  return "waiting";
}

function mailHistoryMilestoneCopy(
  stage: NonNullable<SourceView["connections"][number]["mail"]>["milestones"][number]["id"],
): { label: string; detail: string } {
  switch (stage) {
    case "newest_30_days":
      return { label: "Recent mail", detail: "From the last 30 days" };
    case "days_31_to_90":
      return { label: "Earlier mail", detail: "From 31 to 90 days ago" };
    case "days_91_to_365":
      return { label: "Past year", detail: "From 91 days to one year ago" };
    case "older_history":
      return { label: "Older mail", detail: "From more than one year ago" };
  }
}

function syncMilestoneStateLabel(
  state: NonNullable<SourceView["connections"][number]["mail"]>["milestones"][number]["state"],
): string {
  switch (state) {
    case "waiting":
      return "Waiting";
    case "running":
      return "Scanning";
    case "complete":
      return "Scan complete";
    case "needs_attention":
      return "Needs attention";
  }
}

function calendarMode(value: string): "full_private" | "availability_only" | "off" {
  return value === "full_private" || value === "availability_only" ? value : "off";
}

function candidateTitle(kind: string): string {
  switch (kind) {
    case "coverage_proposal":
      return "Possible family plan";
    case "coverage_needs_household":
      return "A family setup is needed";
    case "family_message_review":
      return "A message worth reviewing";
    default:
      return "Something worth a look";
  }
}

function optionalString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

function stringValues(value: unknown): string[] {
  return Array.isArray(value)
    ? value.flatMap((entry) => (typeof entry === "string" && entry.trim().length > 0 ? [entry.trim()] : []))
    : [];
}

function sourceLabel(provider: string, objectKind: string): string {
  if (provider === "gmail" || objectKind === "mail_message") return "Gmail";
  if (provider === "google.calendar" || objectKind === "calendar_event") return "Google Calendar";
  if (objectKind === "conversation_message") return "Private Florence chat";
  return "Private source";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
