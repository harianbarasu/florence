import { randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { Database } from "../../db/client.js";
import { StaleAuthorityError, UnauthorizedError } from "../../shared/errors.js";
import { participantApprovalDigest } from "./authority.js";
import { PostgresConversationAuthority } from "./postgres-conversation-authority.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

export interface GroupRuleApprovalResult {
  readonly status: "pending" | "active";
  readonly approvedCount: number;
  readonly requiredCount: number;
  readonly conversationId: string;
}

/** Collects one explicit approval per exact live participant before widening group proactivity. */
export class GroupRuleOnboarding {
  public constructor(private readonly database: Executor) {}

  public async approveFamilyCoverage(input: {
    readonly conversationId: string;
    readonly actorPersonId: string;
    readonly expectedParticipantEpochId: string;
    readonly expectedParticipantSetDigest: string;
    readonly approvedAt: Date;
  }): Promise<GroupRuleApprovalResult> {
    return inTransaction(this.database, async (transaction) => {
      const locked = await transaction<{ id: string }[]>`
        select id from conversations where id = ${input.conversationId}
          and kind = 'group' and status = 'active' and household_id is not null for update
      `;
      if (!locked[0]) {
        throw new UnauthorizedError(
          "This chat must belong to your family before proactive help can be approved",
        );
      }
      const authority = new PostgresConversationAuthority(transaction);
      let snapshot = await authority.snapshot(input.conversationId);
      if (!snapshot.participantEpochId || !snapshot.participantSetDigest) {
        throw new UnauthorizedError("This group has no current participant set");
      }
      if (
        snapshot.participantEpochId !== input.expectedParticipantEpochId ||
        snapshot.participantSetDigest !== input.expectedParticipantSetDigest
      ) {
        throw new StaleAuthorityError(
          "This group changed after the approval link was issued; review the current group again",
        );
      }
      if (
        snapshot.participants.some(
          (participant) =>
            participant.registrationStatus !== "registered" ||
            participant.consentedAt === null ||
            participant.policy === null,
        )
      ) {
        throw new UnauthorizedError(
          "Everyone in this group must register before proactive help can be approved",
        );
      }
      const participantIds = [
        ...new Set(snapshot.participants.map((participant) => participant.personId)),
      ].sort();
      if (!participantIds.includes(input.actorPersonId)) {
        throw new UnauthorizedError("Only a current group participant can approve this rule");
      }
      const alreadyActive = snapshot.rules.some(
        (rule) =>
          rule.active &&
          rule.participantSetDigest === snapshot.participantSetDigest &&
          rule.allowedOperations.includes("proactive_coverage"),
      );
      if (alreadyActive) {
        return {
          status: "active",
          approvedCount: participantIds.length,
          requiredCount: participantIds.length,
          conversationId: input.conversationId,
        };
      }

      const proposals = await transaction<{ id: string }[]>`
        select id from conversation_rules
        where conversation_id = ${input.conversationId}
          and rule_key = 'family_coverage_proposal' and status = 'candidate'
          and participant_set_digest = ${snapshot.participantSetDigest}
        order by version desc limit 1
      `;
      let proposalId = proposals[0]?.id;
      if (!proposalId) {
        const version = await transaction<{ version: number | string }[]>`
          select coalesce(max(version), 0) + 1 as version from conversation_rules
          where conversation_id = ${input.conversationId}
            and rule_key = 'family_coverage_proposal'
        `;
        proposalId = randomUUID();
        await transaction`
          insert into conversation_rules (
            id, conversation_id, rule_key, version, status, purpose,
            allowed_operations, participant_set_digest, approval_participant_digest,
            created_by_person_id, created_at
          ) values (
            ${proposalId}, ${input.conversationId}, 'family_coverage_proposal',
            ${Number(version[0]?.version ?? 1)}, 'candidate',
            'Let Florence proactively keep family coverage loops visible in this exact group',
            ${transaction.array(["proactive_coverage"])}, ${snapshot.participantSetDigest},
            ${participantApprovalDigest([])}, ${input.actorPersonId}, ${input.approvedAt}
          )
        `;
      }
      await transaction`
        insert into conversation_rule_approvals (
          conversation_rule_id, participant_epoch_id, person_id,
          participant_set_digest, approved_at
        ) values (
          ${proposalId}, ${snapshot.participantEpochId}, ${input.actorPersonId},
          ${snapshot.participantSetDigest}, ${input.approvedAt}
        ) on conflict (conversation_rule_id, person_id) do nothing
      `;
      const approved = await transaction<{ person_id: string }[]>`
        select person_id from conversation_rule_approvals
        where conversation_rule_id = ${proposalId}
          and participant_epoch_id = ${snapshot.participantEpochId}
          and participant_set_digest = ${snapshot.participantSetDigest}
        order by person_id
      `;
      const approvedIds = approved.map((entry) => entry.person_id);
      if (participantApprovalDigest(approvedIds) !== participantApprovalDigest(participantIds)) {
        return {
          status: "pending",
          approvedCount: approvedIds.length,
          requiredCount: participantIds.length,
          conversationId: input.conversationId,
        };
      }

      for (const participantId of participantIds) {
        const current = snapshot.participants.find(
          (participant) => participant.personId === participantId,
        )?.policy;
        if (!current) throw new UnauthorizedError("A current group policy disappeared");
        if (!current.allowProactiveWrites) {
          snapshot = await authority.setParticipantPolicy({
            conversationId: input.conversationId,
            actorPersonId: input.actorPersonId,
            targetPersonId: participantId,
            policy: { ...current, allowProactiveWrites: true },
            expectedAuthorityVersion: snapshot.authorityVersion,
            approvedByPersonIds: participantIds,
            changedAt: input.approvedAt.toISOString(),
          });
        }
      }
      snapshot = await authority.activateRule({
        conversationId: input.conversationId,
        actorPersonId: input.actorPersonId,
        ruleKey: "family_coverage",
        purpose: "Proactively open, remind on, and close family coverage loops in this exact group",
        allowedOperations: [
          "proactive_coverage",
          "coverage_coordination",
          "coverage_reminder",
          "coverage_state_change",
          "coverage_closure",
        ],
        approvedByPersonIds: participantIds,
        expectedAuthorityVersion: snapshot.authorityVersion,
        activatedAt: input.approvedAt.toISOString(),
      });
      await transaction`
        update conversation_rules set status = 'superseded', ended_at = ${input.approvedAt},
          approval_participant_digest = ${participantApprovalDigest(participantIds)}
        where id = ${proposalId} and status = 'candidate'
      `;
      await appendConversationAudit(transaction, {
        conversationId: input.conversationId,
        actorPersonId: input.actorPersonId,
        eventType: "group_family_coverage_rule_activated",
        targetId: snapshot.rules.find((rule) => rule.ruleKey === "family_coverage")?.ruleId ?? null,
        occurredAt: input.approvedAt,
        reasons: ["unanimous_exact_epoch_approval", "proactive_coverage"],
      });
      return {
        status: "active",
        approvedCount: participantIds.length,
        requiredCount: participantIds.length,
        conversationId: input.conversationId,
      };
    });
  }
}

async function appendConversationAudit(
  transaction: Transaction,
  input: {
    readonly conversationId: string;
    readonly actorPersonId: string;
    readonly eventType: string;
    readonly targetId: string | null;
    readonly occurredAt: Date;
    readonly reasons: readonly string[];
  },
): Promise<void> {
  const households = await transaction<{ household_id: string | null }[]>`
    select household_id from conversations where id = ${input.conversationId}
  `;
  const householdId = households[0]?.household_id ?? null;
  const sequence = householdId
    ? await transaction<{ next_sequence: number | string }[]>`
        select coalesce(max(sequence), 0) + 1 as next_sequence
        from audit_events where household_id = ${householdId}
      `
    : [{ next_sequence: 1 }];
  await transaction`
    insert into audit_events (
      id, household_id, person_id, conversation_id, sequence, actor_kind, actor_id,
      event_type, target_type, target_id, reason_codes, occurred_at
    ) values (
      ${randomUUID()}, ${householdId}, ${input.actorPersonId}, ${input.conversationId},
      ${Number(sequence[0]?.next_sequence ?? 1)}, 'person', ${input.actorPersonId},
      ${input.eventType}, 'conversation_rule', ${input.targetId},
      ${transaction.array([...input.reasons])}, ${input.occurredAt}
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
