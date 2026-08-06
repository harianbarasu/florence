import { createHash, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { Database } from "../../db/client.js";
import type { SecretBox } from "../../shared/crypto.js";
import { NotFoundError, UnauthorizedError } from "../../shared/errors.js";
import { PostgresCoordination } from "../coordination/index.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

/** Immediate authority fencing and cryptographic erasure for a Florence person. */
export class PostgresDataControls {
  public constructor(
    private readonly database: Executor,
    private readonly secretBox: SecretBox,
  ) {}

  public async deletePerson(input: {
    readonly actorPersonId: string;
    readonly deletedAt: Date;
  }): Promise<{ deletionRequestId: string; receiptDigest: string; duplicate: boolean }> {
    return inTransaction(this.database, async (transaction) => {
      const people = await transaction<{ status: string; control_epoch: number | string }[]>`
        select status, control_epoch from people where id = ${input.actorPersonId} for update
      `;
      const person = people[0];
      if (!person) throw new NotFoundError("Florence person does not exist");
      const existing = await transaction<{ id: string; receipt_digest: string | null }[]>`
        select id, receipt_digest from deletion_requests
        where target_person_id = ${input.actorPersonId} and status = 'completed'
        order by requested_at desc limit 1
      `;
      if (person.status === "deleted" && existing[0]?.receipt_digest) {
        return {
          deletionRequestId: existing[0].id,
          receiptDigest: existing[0].receipt_digest,
          duplicate: true,
        };
      }
      if (person.status !== "registered" && person.status !== "stopped") {
        throw new UnauthorizedError("This person cannot be deleted from the current state");
      }
      const deletionRequestId = randomUUID();
      const nextEpoch = Number(person.control_epoch) + 1;
      const affectedMemberships = await transaction<{ readonly id: string; readonly household_id: string }[]>`
        select id, household_id from household_memberships
        where person_id = ${input.actorPersonId} and status in ('active', 'invited', 'suspended')
        order by household_id, id for update
      `;
      const affectedHouseholdIds = [...new Set(affectedMemberships.map((entry) => entry.household_id))];
      if (affectedHouseholdIds.length > 0) {
        await transaction`
          select id from households
          where id = any(${transaction.array(affectedHouseholdIds)}::uuid[])
          order by id for update
        `;
      }
      const affectedLoops = await transaction<{ readonly id: string }[]>`
        select id from coverage_loops
        where state in ('provisional', 'open', 'awaiting_response', 'covered', 'at_risk')
          and (
            proposed_holder_person_id = ${input.actorPersonId}
            or acknowledged_by_person_id = ${input.actorPersonId}
          )
        order by id for update
      `;
      await transaction`
        insert into deletion_requests (
          id, requested_by_person_id, target_kind, target_person_id,
          target_control_epoch, status, requested_at
        ) values (
          ${deletionRequestId}, ${input.actorPersonId}, 'person', ${input.actorPersonId},
          ${nextEpoch}, 'fenced', ${input.deletedAt}
        )
      `;
      await transaction`
        update people set status = 'deletion_fenced', control_epoch = ${nextEpoch},
          authority_version = authority_version + 1,
          display_name_ciphertext = null, display_name_key_version = null,
          quiet_hours = '{}'::jsonb, updated_at = ${input.deletedAt}
        where id = ${input.actorPersonId}
      `;
      await transaction`
        update person_sessions set revoked_at = coalesce(revoked_at, ${input.deletedAt})
        where person_id = ${input.actorPersonId}
      `;
      await transaction`
        delete from oauth_attempts where person_id = ${input.actorPersonId}
      `;
      await transaction`
        update integrations set status = 'revoked', credential_ciphertext = null,
          credential_key_version = null, control_epoch = control_epoch + 1,
          revoked_at = coalesce(revoked_at, ${input.deletedAt}), updated_at = ${input.deletedAt}
        where person_id = ${input.actorPersonId} and status <> 'revoked'
      `;
      await transaction`
        update integration_grants grant_row set status = 'revoked', revoked_at = ${input.deletedAt}
        from integrations integration
        where grant_row.integration_id = integration.id
          and integration.person_id = ${input.actorPersonId} and grant_row.status = 'active'
      `;
      await transaction`
        update sync_cursors cursor set cursor_ciphertext = null, cursor_key_version = null,
          state = 'exhausted', checkpoint_at = ${input.deletedAt}, updated_at = ${input.deletedAt}
        from integrations integration
        where cursor.integration_id = integration.id and integration.person_id = ${input.actorPersonId}
      `;
      const revisions = await transaction<{ id: string }[]>`
        select id from source_revisions where owner_person_id = ${input.actorPersonId}
      `;
      const revisionIds = revisions.map((entry) => entry.id);
      if (revisionIds.length > 0) {
        await transaction`
          delete from source_blobs where source_revision_id = any(${transaction.array(revisionIds)}::uuid[])
        `;
        await transaction`
          delete from source_derivatives
          where parent_source_revision_id = any(${transaction.array(revisionIds)}::uuid[])
        `;
        await transaction`
          update source_revisions set content_ciphertext = null, content_key_version = null,
            revoked_at = coalesce(revoked_at, ${input.deletedAt})
          where id = any(${transaction.array(revisionIds)}::uuid[])
        `;
      }
      await transaction`
        delete from knowledge_candidates where owner_person_id = ${input.actorPersonId}
      `;
      await transaction`
        delete from memory_records where owner_person_id = ${input.actorPersonId}
      `;
      await transaction`
        update bridge_rules set status = 'revoked', updated_at = ${input.deletedAt}
        where owner_person_id = ${input.actorPersonId} and status <> 'revoked'
      `;
      await transaction`
        update routine_revisions set proposed_holder_person_id = null,
          standing_holder_person_id = null, standing_authorized_by_person_id = null,
          standing_authorization_kind = null, standing_authorized_at = null
        where proposed_holder_person_id = ${input.actorPersonId}
          or standing_holder_person_id = ${input.actorPersonId}
      `;
      await transaction`
        update routine_occurrences set proposed_holder_person_id = null,
          standing_holder_person_id = null, standing_authorized_by_person_id = null,
          standing_authorization_kind = null, standing_authorized_at = null
        where proposed_holder_person_id = ${input.actorPersonId}
          or standing_holder_person_id = ${input.actorPersonId}
      `;
      const coordination = new PostgresCoordination(transaction, this.secretBox);
      for (const affected of affectedLoops) {
        const loop = await coordination.loadForUpdate(affected.id);
        if (!loop) continue;
        await coordination.transition({
          loopId: loop.loopId,
          command: {
            kind: "revoke_participant",
            transitionId: randomUUID(),
            expectedVersion: loop.version,
            actorPersonId: null,
            affectedPersonId: input.actorPersonId,
            occurredAt: input.deletedAt.toISOString(),
            evidenceRefs: [`deletion-request:${deletionRequestId}`],
          },
        });
      }
      await transaction`
        update membership_capabilities grant_row set status = 'revoked', revoked_at = ${input.deletedAt}
        from household_memberships membership
        where grant_row.membership_id = membership.id and membership.person_id = ${input.actorPersonId}
          and grant_row.status = 'active'
      `;
      await transaction`
        update household_memberships set status = 'left', ended_at = ${input.deletedAt},
          version = version + 1, updated_at = ${input.deletedAt}
        where person_id = ${input.actorPersonId} and status in ('active', 'invited', 'suspended')
      `;
      if (affectedHouseholdIds.length > 0) {
        await transaction`
          update households set membership_version = membership_version + 1,
            control_epoch = control_epoch + 1, updated_at = ${input.deletedAt}
          where id = any(${transaction.array(affectedHouseholdIds)}::uuid[])
            and status not in ('deletion_fenced', 'deleted')
        `;
        await transaction`
          update invitations set status = 'revoked', updated_at = ${input.deletedAt}
          where household_id = any(${transaction.array(affectedHouseholdIds)}::uuid[])
            and status = 'pending'
        `;
      }
      const conversations = await transaction<{ id: string }[]>`
        select distinct conversation.id
        from conversations conversation
        join epoch_participants participant on participant.participant_epoch_id = conversation.current_epoch_id
        where participant.person_id = ${input.actorPersonId}
      `;
      const conversationIds = conversations.map((entry) => entry.id);
      if (conversationIds.length > 0) {
        await transaction`
          update epoch_participants participant set registration_status = 'provisional', consented_at = null
          from conversations conversation
          where conversation.id = any(${transaction.array(conversationIds)}::uuid[])
            and participant.participant_epoch_id = conversation.current_epoch_id
            and participant.person_id = ${input.actorPersonId}
        `;
        await transaction`
          update participant_policies set status = 'revoked', superseded_at = ${input.deletedAt}
          where conversation_id = any(${transaction.array(conversationIds)}::uuid[])
            and person_id = ${input.actorPersonId} and status = 'active'
        `;
        await transaction`
          update conversations set authority_version = authority_version + 1, updated_at = ${input.deletedAt}
          where id = any(${transaction.array(conversationIds)}::uuid[])
        `;
      }
      await transaction`
        update invitations set status = 'revoked', updated_at = ${input.deletedAt}
        where status = 'pending' and invitee_identity_id in (
          select id from person_identities where person_id = ${input.actorPersonId}
        )
      `;
      await transaction`
        update person_identities set status = 'revoked', revoked_at = ${input.deletedAt},
          authority_version = authority_version + 1, updated_at = ${input.deletedAt}
        where person_id = ${input.actorPersonId} and status <> 'revoked'
      `;
      await transaction`
        update people set status = 'deleted', consented_at = null, timezone = null,
          authority_version = authority_version + 1, updated_at = ${input.deletedAt}
        where id = ${input.actorPersonId}
      `;
      const receiptDigest = sha256Hex(
        `${deletionRequestId}:${input.actorPersonId}:${nextEpoch}:${input.deletedAt.toISOString()}`,
      );
      await transaction`
        insert into revocation_tombstones (
          id, target_kind, target_id_digest, control_epoch, reason_code, revoked_at
        ) values (
          ${randomUUID()}, 'person', ${sha256Hex(input.actorPersonId)}, ${nextEpoch},
          'person_deletion_completed', ${input.deletedAt}
        ) on conflict do nothing
      `;
      await transaction`
        update deletion_requests set status = 'completed', completed_at = ${input.deletedAt},
          receipt_digest = ${receiptDigest} where id = ${deletionRequestId}
      `;
      return { deletionRequestId, receiptDigest, duplicate: false };
    });
  }
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
