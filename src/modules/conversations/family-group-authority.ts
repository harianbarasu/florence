import { randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { Database } from "../../db/client.js";
import { NotFoundError } from "../../shared/errors.js";
import { participantApprovalDigest } from "./authority.js";
import type { ConversationAuthoritySnapshot } from "./contracts.js";
import { PostgresConversationAuthority } from "./postgres-conversation-authority.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

const ReconcileFamilyGroupInputSchema = z.strictObject({
  conversationId: z.string().uuid(),
  occurredAt: z.date(),
});

const FAMILY_GROUP_OPERATIONS = [
  "proactive_coverage",
  "coverage_coordination",
  "coverage_reminder",
  "coverage_state_change",
  "coverage_closure",
  "family_group_activation",
] as const;

export interface FamilyGroupAuthorityResult {
  readonly status: "active" | "observe_only";
  readonly conversationId: string;
  readonly householdId: string | null;
  readonly householdControlEpoch: number | null;
  readonly participantEpochId: string;
  readonly participantSetDigest: string;
  readonly ruleId: string | null;
  readonly activatedNow: boolean;
  readonly snapshot: ConversationAuthoritySnapshot;
}

/**
 * Derives exact-group write authority from current accepted household
 * relationships. Callers provide no approvals or participant choices: this
 * module owns the complete current-audience decision and its audit trail.
 */
export class FamilyGroupAuthority {
  public constructor(private readonly database: Executor) {}

  public async reconcile(inputCandidate: {
    readonly conversationId: string;
    readonly occurredAt: Date;
  }): Promise<FamilyGroupAuthorityResult> {
    const input = ReconcileFamilyGroupInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      const conversations = await transaction<
        {
          id: string;
          household_id: string | null;
          current_epoch_id: string | null;
          participant_set_digest: string | null;
        }[]
      >`
        select conversation.id, conversation.household_id, conversation.current_epoch_id,
          epoch.participant_set_digest
        from conversations conversation
        left join participant_epochs epoch on epoch.id = conversation.current_epoch_id
          and epoch.ended_at is null
        where conversation.id = ${input.conversationId}
          and conversation.kind = 'group' and conversation.status = 'active'
        for update of conversation
      `;
      const conversation = conversations[0];
      if (!conversation) throw new NotFoundError("Active group conversation does not exist");
      if (!conversation.current_epoch_id || !conversation.participant_set_digest) {
        throw new NotFoundError("Group conversation has no current participant audience");
      }

      const participants = await transaction<
        {
          person_id: string;
          registration_status: string;
          consented_at: Date | null;
          policy_id: string | null;
          policy_version: number | string | null;
          approval_participant_digest: string | null;
          allow_content_processing: boolean | null;
          allow_direct_responses: boolean | null;
          allow_proactive_writes: boolean | null;
          retention_seconds: number | null;
        }[]
      >`
        select participant.person_id, participant.registration_status, participant.consented_at,
          policy.id as policy_id, policy.version as policy_version,
          policy.approval_participant_digest,
          policy.allow_content_processing, policy.allow_direct_responses,
          policy.allow_proactive_writes, policy.retention_seconds
        from epoch_participants participant
        left join participant_policies policy on policy.conversation_id = ${input.conversationId}
          and policy.person_id = participant.person_id and policy.status = 'active'
        where participant.participant_epoch_id = ${conversation.current_epoch_id}
        order by participant.person_id
      `;
      const personIds = [...new Set(participants.map((participant) => participant.person_id))];
      const baseEligible =
        personIds.length > 1 &&
        participants.length === personIds.length &&
        participants.every(
          (participant) =>
            participant.registration_status === "registered" &&
            participant.consented_at !== null &&
            participant.policy_id !== null &&
            participant.allow_content_processing === true &&
            participant.allow_direct_responses === true &&
            (participant.allow_proactive_writes === true ||
              (Number(participant.policy_version) === 1 && participant.approval_participant_digest === null)),
        );

      const commonHouseholds = baseEligible
        ? await transaction<
            {
              household_id: string;
              household_control_epoch: number | string;
              steward_person_id: string;
            }[]
          >`
            select household.id as household_id,
              household.control_epoch as household_control_epoch,
              (
                select steward.person_id
                from household_memberships steward
                where steward.household_id = household.id
                  and steward.status = 'active' and steward.role = 'steward'
                order by steward.person_id
                limit 1
              ) as steward_person_id
            from households household
            join household_memberships membership on membership.household_id = household.id
              and membership.status = 'active'
              and membership.person_id = any(${transaction.array(personIds)}::uuid[])
            where household.status in ('onboarding', 'active')
            group by household.id, household.control_epoch
            having count(distinct membership.person_id) = ${personIds.length}
              and exists(
                select 1 from household_memberships steward
                where steward.household_id = household.id
                  and steward.status = 'active' and steward.role = 'steward'
              )
            order by household.id
            limit 2
          `
        : [];
      const common = commonHouseholds.length === 1 ? commonHouseholds[0] : null;
      if (!common) {
        await lockHouseholds(transaction, [conversation.household_id]);
        const changed = await makeObserveOnly(
          transaction,
          input.conversationId,
          conversation.household_id,
          input.occurredAt,
        );
        if (changed) {
          await appendAudit(transaction, {
            conversationId: input.conversationId,
            householdId: conversation.household_id,
            eventType: "family_group_authority_removed",
            targetId: null,
            reasonCodes: [
              baseEligible ? "no_unique_common_household" : "participant_not_ready",
              "observe_only",
            ],
            occurredAt: input.occurredAt,
          });
        }
        const snapshot = await new PostgresConversationAuthority(transaction).snapshot(input.conversationId);
        return {
          status: "observe_only",
          conversationId: input.conversationId,
          householdId: null,
          householdControlEpoch: null,
          participantEpochId: conversation.current_epoch_id,
          participantSetDigest: conversation.participant_set_digest,
          ruleId: null,
          activatedNow: false,
          snapshot,
        };
      }

      await lockHouseholds(transaction, [conversation.household_id, common.household_id]);
      const lockedCommonHouseholds = await transaction<
        {
          household_id: string;
          household_control_epoch: number | string;
          steward_person_id: string;
        }[]
      >`
        select household.id as household_id,
          household.control_epoch as household_control_epoch,
          (
            select steward.person_id
            from household_memberships steward
            where steward.household_id = household.id
              and steward.status = 'active' and steward.role = 'steward'
            order by steward.person_id
            limit 1
          ) as steward_person_id
        from households household
        where household.id = ${common.household_id}
          and household.status in ('onboarding', 'active')
          and (
            select count(distinct membership.person_id)
            from household_memberships membership
            where membership.household_id = household.id
              and membership.status = 'active'
              and membership.person_id = any(${transaction.array(personIds)}::uuid[])
          ) = ${personIds.length}
          and exists(
            select 1 from household_memberships steward
            where steward.household_id = household.id
              and steward.status = 'active' and steward.role = 'steward'
          )
      `;
      const lockedCommon = lockedCommonHouseholds[0] ?? null;
      if (!lockedCommon) {
        const changed = await makeObserveOnly(
          transaction,
          input.conversationId,
          conversation.household_id,
          input.occurredAt,
        );
        if (changed) {
          await appendAudit(transaction, {
            conversationId: input.conversationId,
            householdId: conversation.household_id,
            eventType: "family_group_authority_removed",
            targetId: null,
            reasonCodes: ["household_membership_changed", "observe_only"],
            occurredAt: input.occurredAt,
          });
        }
        const snapshot = await new PostgresConversationAuthority(transaction).snapshot(input.conversationId);
        return {
          status: "observe_only",
          conversationId: input.conversationId,
          householdId: null,
          householdControlEpoch: null,
          participantEpochId: conversation.current_epoch_id,
          participantSetDigest: conversation.participant_set_digest,
          ruleId: null,
          activatedNow: false,
          snapshot,
        };
      }

      let changed = conversation.household_id !== lockedCommon.household_id;
      if (changed) {
        await transaction`
          update conversations set household_id = ${lockedCommon.household_id}
          where id = ${input.conversationId}
        `;
      }

      const approvalDigest = participantApprovalDigest(personIds);
      for (const participant of participants) {
        if (participant.allow_proactive_writes === true) continue;
        if (
          !participant.policy_id ||
          participant.policy_version === null ||
          participant.retention_seconds === null
        ) {
          throw new NotFoundError("An eligible family group participant policy disappeared");
        }
        await transaction`
          update participant_policies
          set status = 'superseded', superseded_at = ${input.occurredAt}
          where id = ${participant.policy_id} and status = 'active'
        `;
        await transaction`
          insert into participant_policies (
            id, conversation_id, person_id, version, status,
            allow_content_processing, allow_direct_responses, allow_proactive_writes,
            retention_seconds, changed_by_person_id, approval_participant_digest,
            effective_at
          ) values (
            ${randomUUID()}, ${input.conversationId}, ${participant.person_id},
            ${Number(participant.policy_version) + 1}, 'active', true, true, true,
            ${participant.retention_seconds}, ${participant.person_id}, ${approvalDigest},
            ${input.occurredAt}
          )
        `;
        changed = true;
      }

      const existingRules = await transaction<
        { id: string; allowed_operations: string[]; participant_set_digest: string }[]
      >`
        select id, allowed_operations, participant_set_digest
        from conversation_rules
        where conversation_id = ${input.conversationId}
          and rule_key = 'family_coverage' and status = 'active'
        for update
      `;
      const existingRule = existingRules.find(
        (rule) =>
          rule.participant_set_digest === conversation.participant_set_digest &&
          FAMILY_GROUP_OPERATIONS.every((operation) => rule.allowed_operations.includes(operation)),
      );
      let ruleId = existingRule?.id ?? null;
      let activatedNow = false;
      if (!existingRule) {
        await transaction`
          update conversation_rules
          set status = case when status = 'active' then 'superseded' else 'revoked' end,
            ended_at = coalesce(ended_at, ${input.occurredAt})
          where conversation_id = ${input.conversationId}
            and rule_key in ('family_coverage', 'family_coverage_proposal')
            and status in ('active', 'candidate')
        `;
        const versions = await transaction<{ next_version: number | string }[]>`
          select coalesce(max(version), 0) + 1 as next_version
          from conversation_rules
          where conversation_id = ${input.conversationId} and rule_key = 'family_coverage'
        `;
        ruleId = randomUUID();
        await transaction`
          insert into conversation_rules (
            id, conversation_id, rule_key, version, status, purpose,
            allowed_operations, participant_set_digest, approval_participant_digest,
            created_by_person_id, effective_at, created_at
          ) values (
            ${ruleId}, ${input.conversationId}, 'family_coverage',
            ${Number(versions[0]?.next_version ?? 1)}, 'active',
            'Coordinate family coverage in an exact all-household group',
            ${transaction.array([...FAMILY_GROUP_OPERATIONS])},
            ${conversation.participant_set_digest}, ${approvalDigest},
            ${lockedCommon.steward_person_id}, ${input.occurredAt}, ${input.occurredAt}
          )
        `;
        changed = true;
        activatedNow = true;
      }

      if (changed) {
        await transaction`
          update conversations
          set authority_version = authority_version + 1, updated_at = ${input.occurredAt}
          where id = ${input.conversationId}
        `;
      }
      if (activatedNow) {
        await appendAudit(transaction, {
          conversationId: input.conversationId,
          householdId: lockedCommon.household_id,
          eventType: "family_group_authority_activated",
          targetId: ruleId,
          reasonCodes: [
            "all_current_participants_registered",
            "one_common_household",
            "standing_household_membership",
          ],
          occurredAt: input.occurredAt,
        });
      }
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(input.conversationId);
      return {
        status: "active",
        conversationId: input.conversationId,
        householdId: lockedCommon.household_id,
        householdControlEpoch: Number(lockedCommon.household_control_epoch),
        participantEpochId: conversation.current_epoch_id,
        participantSetDigest: conversation.participant_set_digest,
        ruleId,
        activatedNow,
        snapshot,
      };
    });
  }
}

async function lockHouseholds(
  transaction: Transaction,
  householdIds: readonly (string | null)[],
): Promise<void> {
  const ids = [...new Set(householdIds.filter((id): id is string => id !== null))].sort();
  if (ids.length === 0) return;
  await transaction`
    select id from households
    where id = any(${transaction.array(ids)}::uuid[])
    order by id
    for update
  `;
}

async function makeObserveOnly(
  transaction: Transaction,
  conversationId: string,
  existingHouseholdId: string | null,
  occurredAt: Date,
): Promise<boolean> {
  const revoked = await transaction<{ id: string }[]>`
    update conversation_rules
    set status = case when status = 'active' then 'superseded' else 'revoked' end,
      ended_at = coalesce(ended_at, ${occurredAt})
    where conversation_id = ${conversationId}
      and rule_key in ('family_coverage', 'family_coverage_proposal')
      and status in ('active', 'candidate')
    returning id
  `;
  const changed = existingHouseholdId !== null || revoked.length > 0;
  if (changed) {
    await transaction`
      update conversations
      set household_id = null, authority_version = authority_version + 1,
        updated_at = ${occurredAt}
      where id = ${conversationId}
    `;
  }
  return changed;
}

async function appendAudit(
  transaction: Transaction,
  input: {
    readonly conversationId: string;
    readonly householdId: string | null;
    readonly eventType: string;
    readonly targetId: string | null;
    readonly reasonCodes: readonly string[];
    readonly occurredAt: Date;
  },
): Promise<void> {
  const sequenceRows = input.householdId
    ? await transaction<{ next_sequence: number | string }[]>`
        select coalesce(max(sequence), 0) + 1 as next_sequence
        from audit_events where household_id = ${input.householdId}
      `
    : await transaction<{ next_sequence: number | string }[]>`
        select coalesce(max(sequence), 0) + 1 as next_sequence
        from audit_events where household_id is null and conversation_id = ${input.conversationId}
      `;
  await transaction`
    insert into audit_events (
      id, household_id, conversation_id, sequence, actor_kind, actor_id,
      event_type, target_type, target_id, reason_codes, occurred_at
    ) values (
      ${randomUUID()}, ${input.householdId}, ${input.conversationId},
      ${Number(sequenceRows[0]?.next_sequence ?? 1)}, 'application', null,
      ${input.eventType}, 'conversation_rule', ${input.targetId},
      ${transaction.array([...input.reasonCodes])}, ${input.occurredAt}
    )
  `;
}

function inTransaction<Result>(
  executor: Executor,
  operation: (transaction: Transaction) => Promise<Result>,
): Promise<Result> {
  return "begin" in executor
    ? (executor.begin(operation) as unknown as Promise<Result>)
    : operation(executor);
}
