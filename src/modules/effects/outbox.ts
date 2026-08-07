import { createHash, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { Database } from "../../db/client.js";
import { canonicalJson } from "../../shared/canonical-json.js";
import type { SecretBox } from "../../shared/crypto.js";
import { UnauthorizedError } from "../../shared/errors.js";
import { gmailThreadFrontierLockKey, privateSourceIntegrationLockKey } from "../sources/policy.js";
import type { AuthorityFence } from "../work/index.js";

export interface PrivateSourceEffectFrontier {
  readonly frontierId: string;
  readonly integrationId: string;
  readonly caseKeyDigest: string;
  readonly version: number;
  readonly frontierDigest: string;
  readonly sourceGeneration: number;
}

export interface AuthorizedEffectInput extends AuthorityFence {
  readonly actionIntentId?: string;
  readonly actorPersonId?: string;
  readonly participantEpochId?: string;
  readonly expectedParticipantDigest?: string;
  readonly coverageLoop?: { readonly id: string; readonly version: number };
  readonly invitation?: { readonly id: string; readonly inviteeIdentityAuthorityVersion: number };
  readonly sourceConversation?: {
    readonly id: string;
    readonly authorityVersion: number;
    readonly participantEpochId: string;
    readonly participantSetDigest: string;
  };
  readonly sourceFrontier?: PrivateSourceEffectFrontier;
  readonly evidenceSourceRevisionIds?: readonly string[];
  readonly effectKind: "linq.message";
  readonly idempotencyKey: string;
  readonly data: unknown;
  readonly policy: unknown;
  readonly target: unknown;
  readonly payload: unknown;
  readonly reasonCodes: readonly string[];
  readonly authorizationExpiresAt: Date;
}

export interface ClaimedEffect<Payload = unknown> {
  readonly outboxId: string;
  readonly effectKind: "linq.message";
  readonly idempotencyKey: string;
  readonly payload: Payload;
  readonly attemptCount: number;
  readonly leaseToken: string;
}

export interface ClaimedSubmittedEffect<Payload = unknown> extends ClaimedEffect<Payload> {
  readonly providerReceiptId: string | null;
  readonly submittedAt: Date;
  readonly reconciliationAttemptCount: number;
}

interface OutboxRow {
  id: string;
  effect_kind: "linq.message";
  idempotency_key: string;
  payload_ciphertext: Buffer;
  attempt_count: number;
  lease_token: string;
}

interface SubmittedReceiptRow {
  provider_receipt_id: string | null;
  submitted_at: Date;
}

interface EffectSourceLockTarget {
  readonly authorization_decision_id: string;
  readonly person_id: string | null;
  readonly integration_id: string | null;
  readonly private_source_frontier_id: string | null;
  readonly private_source_case_key_digest: string | null;
}

export type GuardedSubmissionResult<Result> =
  | { readonly authorized: false }
  | { readonly authorized: true; readonly result: Result };

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

/** Exact-authority effect queue. It is impossible to insert without a committed allow decision. */
export class EffectOutbox {
  public constructor(
    private readonly database: Executor,
    private readonly secretBox: SecretBox,
  ) {}

  public async authorizeAndEnqueue(
    input: AuthorizedEffectInput,
    now = new Date(),
  ): Promise<{ outboxId: string; created: boolean }> {
    validateEffectScope(input);
    if (input.authorizationExpiresAt <= now)
      throw new Error("Effect authorization must expire in the future");
    if (
      (input.evidenceSourceRevisionIds?.length ?? 0) > 0 &&
      input.authorizationExpiresAt <= new Date(now.getTime() + 3 * 60_000)
    ) {
      throw new Error("Evidence-backed effect authorization is too close to expiry");
    }
    const payloadJson = canonicalJson(input.payload);
    const payloadDigest = sha256Hex(payloadJson);
    const effectDigests = {
      actionDigest: digest({ effectKind: input.effectKind, idempotencyKey: input.idempotencyKey }),
      dataDigest: digest({ data: input.data, payloadDigest }),
      policyDigest: digest(input.policy),
      targetDigest: digest(input.target),
    };
    const encrypted = this.secretBox.encrypt(payloadJson, "effect-payload");
    return inTransaction(this.database, async (transaction) => {
      const existing = await transaction<{ id: string }[]>`
        select id from outbox where idempotency_key = ${input.idempotencyKey}
      `;
      if (existing[0]) return { outboxId: existing[0].id, created: false };

      const integrationFence = input.sourceFrontier
        ? await authorizePrivateSourceFrontier(transaction, input)
        : input.integration;

      if (input.actionIntentId) {
        const approvedDigests = await loadCurrentActionIntentDigests(
          transaction,
          input.actionIntentId,
          input,
          now,
        );
        if (
          approvedDigests.actionDigest !== effectDigests.actionDigest ||
          approvedDigests.dataDigest !== effectDigests.dataDigest ||
          approvedDigests.policyDigest !== effectDigests.policyDigest ||
          approvedDigests.targetDigest !== effectDigests.targetDigest
        ) {
          throw new UnauthorizedError("Effect content does not match its exact approved action intent");
        }
      }

      const evidenceSourceRevisionIds = [...(input.evidenceSourceRevisionIds ?? [])];
      if (evidenceSourceRevisionIds.length > 0) {
        const evidence = await transaction<{ readonly authorized: boolean }[]>`
          select source_evidence_set_is_current(
            ${transaction.array(evidenceSourceRevisionIds)}::uuid[],
            ${input.sourceConversation?.participantEpochId ?? null}::uuid,
            ${input.person?.id ?? null}::uuid,
            ${now}
          ) as authorized
        `;
        if (evidence[0]?.authorized !== true) {
          throw new Error("Effect source evidence is no longer authorized");
        }
      }

      const decisionId = randomUUID();
      await transaction`
        insert into disclosure_decisions (
          id, outcome, actor_person_id, household_id, conversation_id, participant_epoch_id,
          action_digest, data_digest, policy_digest, target_digest, reason_codes,
          decided_at, expires_at
        ) values (
          ${decisionId}, 'allow', ${input.actorPersonId ?? null}, ${input.household?.id ?? null},
          ${input.conversation?.id ?? null}, ${input.participantEpochId ?? null},
          ${effectDigests.actionDigest}, ${effectDigests.dataDigest},
          ${effectDigests.policyDigest}, ${effectDigests.targetDigest},
          ${transaction.array([...input.reasonCodes])}, ${now}, ${input.authorizationExpiresAt}
        )
      `;
      const outboxId = randomUUID();
      await transaction`
        insert into outbox (
          id, authorization_decision_id, action_intent_id, household_id, person_id, conversation_id,
          integration_id, integration_control_epoch,
          participant_epoch_id, expected_participant_digest, coverage_loop_id, coverage_loop_version,
          invitation_id, invitee_identity_authority_version,
          source_conversation_id, source_participant_epoch_id,
          source_expected_participant_digest, source_conversation_authority_version,
          private_source_frontier_id, private_source_frontier_version,
          private_source_frontier_digest, private_source_generation,
          private_source_case_key_digest,
          evidence_source_revision_ids,
          effect_kind, idempotency_key,
          payload_digest, payload_ciphertext, payload_key_version, status, available_at,
          person_control_epoch, household_control_epoch, conversation_authority_version
        ) values (
          ${outboxId}, ${decisionId}, ${input.actionIntentId ?? null},
          ${input.household?.id ?? null}, ${input.person?.id ?? null},
          ${input.conversation?.id ?? null}, ${integrationFence?.id ?? null},
          ${integrationFence?.controlEpoch ?? null}, ${input.participantEpochId ?? null},
          ${input.expectedParticipantDigest ?? null}, ${input.coverageLoop?.id ?? null},
          ${input.coverageLoop?.version ?? null}, ${input.invitation?.id ?? null},
          ${input.invitation?.inviteeIdentityAuthorityVersion ?? null},
          ${input.sourceConversation?.id ?? null}, ${input.sourceConversation?.participantEpochId ?? null},
          ${input.sourceConversation?.participantSetDigest ?? null},
          ${input.sourceConversation?.authorityVersion ?? null},
          ${input.sourceFrontier?.frontierId ?? null}, ${input.sourceFrontier?.version ?? null},
          ${input.sourceFrontier?.frontierDigest ?? null},
          ${input.sourceFrontier?.sourceGeneration ?? null},
          ${input.sourceFrontier?.caseKeyDigest ?? null},
          ${transaction.array(evidenceSourceRevisionIds)}::uuid[],
          ${input.effectKind}, ${input.idempotencyKey},
          ${payloadDigest}, ${Buffer.from(JSON.stringify(encrypted), "utf8")}, ${encrypted.kid},
          'pending', ${now}, ${input.person?.controlEpoch ?? null},
          ${input.household?.controlEpoch ?? null}, ${input.conversation?.authorityVersion ?? null}
        )
      `;
      return { outboxId, created: true };
    });
  }

  public async claim(workerId: string, limit = 10, now = new Date()): Promise<ClaimedEffect[]> {
    const leaseUntil = new Date(now.getTime() + 60_000);
    return inTransaction(this.database, async (transaction) => {
      const candidates = await transaction<{ id: string }[]>`
        select effect.id
        from outbox effect
        join disclosure_decisions decision on decision.id = effect.authorization_decision_id
        left join people person on person.id = effect.person_id
        left join households household on household.id = effect.household_id
        left join conversations conversation on conversation.id = effect.conversation_id
        left join integrations integration on integration.id = effect.integration_id
        left join participant_epochs epoch on epoch.id = effect.participant_epoch_id
        left join coverage_loops coverage on coverage.id = effect.coverage_loop_id
        left join invitations invitation on invitation.id = effect.invitation_id
        left join person_identities invitee_identity on invitee_identity.id = invitation.invitee_identity_id
        left join households invitation_household on invitation_household.id = invitation.household_id
        left join conversations invitation_source on invitation_source.id = invitation.source_conversation_id
        left join participant_epochs invitation_epoch on invitation_epoch.id = invitation.source_participant_epoch_id
        left join conversations source_conversation on source_conversation.id = effect.source_conversation_id
        left join participant_epochs source_epoch on source_epoch.id = effect.source_participant_epoch_id
        left join private_source_frontiers source_frontier
          on source_frontier.id = effect.private_source_frontier_id
        where effect.status in ('pending', 'retry', 'leased')
          and effect.available_at <= ${now}
          and (effect.status <> 'leased' or effect.lease_expires_at <= ${now})
          and decision.outcome = 'allow' and decision.revoked_at is null and decision.expires_at > ${now}
          and (
            cardinality(effect.evidence_source_revision_ids) = 0
            or decision.expires_at > ${new Date(now.getTime() + 3 * 60_000)}
          )
          and (effect.person_id is null or (
            person.status = 'registered' and person.control_epoch = effect.person_control_epoch
          ))
          and (effect.household_id is null or household.control_epoch = effect.household_control_epoch)
          and (effect.integration_id is null or (
            integration.status <> 'revoked'
            and integration.control_epoch = effect.integration_control_epoch
          ))
          and (effect.conversation_id is null or (
            conversation.status = 'active'
            and conversation.authority_version = effect.conversation_authority_version
            and conversation.current_epoch_id = effect.participant_epoch_id
            and epoch.ended_at is null
            and epoch.participant_set_digest = effect.expected_participant_digest
          ))
          and (effect.coverage_loop_id is null or (
            coverage.version = effect.coverage_loop_version
          ))
          and (effect.invitation_id is null or (
            invitation.status = 'pending' and invitation.expires_at > ${now}
            and invitation_household.membership_version = invitation.household_membership_version
            and invitee_identity.status in ('observed', 'verified')
            and invitee_identity.authority_version = effect.invitee_identity_authority_version
            and invitation_source.status = 'active'
            and invitation_source.current_epoch_id = invitation.source_participant_epoch_id
            and invitation_epoch.ended_at is null
            and invitation_epoch.participant_set_digest = invitation.source_participant_digest
          ))
          and (effect.source_conversation_id is null or (
            source_conversation.status = 'active'
            and source_conversation.authority_version = effect.source_conversation_authority_version
            and source_conversation.current_epoch_id = effect.source_participant_epoch_id
            and source_epoch.ended_at is null
            and source_epoch.participant_set_digest = effect.source_expected_participant_digest
          ))
          and (effect.private_source_frontier_id is null or (
            source_frontier.owner_person_id = effect.person_id
            and source_frontier.integration_id = effect.integration_id
            and source_frontier.case_kind = 'gmail_thread'
            and source_frontier.case_key_digest = effect.private_source_case_key_digest
            and source_frontier.version = effect.private_source_frontier_version
            and source_frontier.frontier_digest = effect.private_source_frontier_digest
            and source_frontier.source_generation = effect.private_source_generation
            and source_frontier.reconciled_generation = effect.private_source_generation
          ))
          and source_evidence_set_is_current(
            effect.evidence_source_revision_ids,
            effect.source_participant_epoch_id,
            effect.person_id,
            ${now}
          )
        order by effect.available_at, effect.created_at
        for update of effect skip locked
        limit ${Math.max(1, Math.min(limit, 100))}
      `;
      const claimed: ClaimedEffect[] = [];
      for (const candidate of candidates) {
        const leaseToken = randomUUID();
        const rows = await transaction<OutboxRow[]>`
          update outbox set status = 'leased', lease_owner = ${workerId}, lease_token = ${leaseToken},
            lease_expires_at = ${leaseUntil}, attempt_count = attempt_count + 1, updated_at = ${now}
          where id = ${candidate.id}
          returning id, effect_kind, idempotency_key, payload_ciphertext, attempt_count, lease_token
        `;
        const row = rows[0];
        if (!row) continue;
        const payload = JSON.parse(
          this.secretBox
            .decrypt(JSON.parse(row.payload_ciphertext.toString("utf8")), "effect-payload")
            .toString("utf8"),
        ) as unknown;
        claimed.push({
          outboxId: row.id,
          effectKind: row.effect_kind,
          idempotencyKey: row.idempotency_key,
          payload,
          attemptCount: row.attempt_count,
          leaseToken: row.lease_token,
        });
      }
      return claimed;
    });
  }

  /**
   * Leases provider-accepted effects for status lookup only. A submitted effect is
   * deliberately excluded from the send claim above, so reconciliation cannot
   * accidentally invoke the external mutation a second time.
   */
  public async claimSubmittedForReconciliation(
    workerId: string,
    limit = 10,
    now = new Date(),
  ): Promise<ClaimedSubmittedEffect[]> {
    const leaseUntil = new Date(now.getTime() + 60_000);
    return inTransaction(this.database, async (transaction) => {
      const candidates = await transaction<{ id: string }[]>`
        select effect.id
        from outbox effect
        where effect.status = 'submitted'
          and effect.available_at <= ${now}
          and (effect.lease_token is null or effect.lease_expires_at <= ${now})
        order by effect.available_at, effect.created_at
        for update of effect skip locked
        limit ${Math.max(1, Math.min(limit, 100))}
      `;
      const claimed: ClaimedSubmittedEffect[] = [];
      for (const candidate of candidates) {
        const leaseToken = randomUUID();
        const rows = await transaction<(OutboxRow & { reconciliation_attempt_count: number })[]>`
          update outbox set lease_owner = ${workerId}, lease_token = ${leaseToken},
            lease_expires_at = ${leaseUntil},
            reconciliation_attempt_count = reconciliation_attempt_count + 1, updated_at = ${now}
          where id = ${candidate.id} and status = 'submitted'
            and (lease_token is null or lease_expires_at <= ${now})
          returning id, effect_kind, idempotency_key, payload_ciphertext, attempt_count,
            lease_token, reconciliation_attempt_count
        `;
        const row = rows[0];
        if (!row) continue;
        const receiptRows = await transaction<SubmittedReceiptRow[]>`
          select provider_receipt_id, min(occurred_at) over () as submitted_at
          from effect_receipts where outbox_id = ${row.id}
          order by (provider_receipt_id is null), occurred_at
          limit 1
        `;
        const firstReceipt = receiptRows[0];
        const payload = JSON.parse(
          this.secretBox
            .decrypt(JSON.parse(row.payload_ciphertext.toString("utf8")), "effect-payload")
            .toString("utf8"),
        ) as unknown;
        claimed.push({
          outboxId: row.id,
          effectKind: row.effect_kind,
          idempotencyKey: row.idempotency_key,
          payload,
          attemptCount: row.attempt_count,
          leaseToken: row.lease_token,
          providerReceiptId: firstReceipt?.provider_receipt_id ?? null,
          submittedAt: firstReceipt?.submitted_at ?? now,
          reconciliationAttemptCount: row.reconciliation_attempt_count,
        });
      }
      return claimed;
    });
  }

  /**
   * Rechecks every app-owned authority fence after Linq's live-audience read and
   * invokes the irreversible provider POST while the authorization transaction
   * is still open. Private-source effects additionally hold integration then
   * Gmail-case advisory locks across both the final check and the provider call.
   */
  public async reauthorizeForSubmission<Result>(
    effect: Pick<ClaimedEffect, "outboxId" | "leaseToken">,
    submit: () => Promise<Result>,
    now?: Date,
  ): Promise<GuardedSubmissionResult<Result>> {
    return inTransaction(this.database, async (transaction) => {
      // Discover advisory-lock identity without first taking the outbox row
      // lock. The authoritative query below requires these exact same values,
      // so a concurrent cancellation or unsupported fence mutation fails closed.
      const targets = await transaction<EffectSourceLockTarget[]>`
        select authorization_decision_id, person_id, integration_id, private_source_frontier_id,
          private_source_case_key_digest
        from outbox
        where id = ${effect.outboxId}
          and status = 'leased' and lease_token = ${effect.leaseToken}
      `;
      const target = targets[0];
      if (!target) return { authorized: false };
      if (target.private_source_frontier_id !== null) {
        if (
          target.person_id === null ||
          target.integration_id === null ||
          target.private_source_case_key_digest === null
        ) {
          const cancelledAt = now ?? new Date();
          await cancelLeasedEffect(transaction, effect, cancelledAt);
          return { authorized: false };
        }
        await acquirePrivateSourceEffectLocks(transaction, {
          ownerPersonId: target.person_id,
          integrationId: target.integration_id,
          caseKeyDigest: target.private_source_case_key_digest,
        });
      }
      await lockEffectSubmissionAuthority(transaction, target);
      const authorizedAt = now ?? new Date();
      const rows = await transaction<{ readonly id: string }[]>`
        select candidate.id
        from outbox candidate
        join disclosure_decisions decision on decision.id = candidate.authorization_decision_id
        left join people person on person.id = candidate.person_id
        left join households household on household.id = candidate.household_id
        left join conversations conversation on conversation.id = candidate.conversation_id
        left join integrations integration on integration.id = candidate.integration_id
        left join participant_epochs epoch on epoch.id = candidate.participant_epoch_id
        left join coverage_loops coverage on coverage.id = candidate.coverage_loop_id
        left join invitations invitation on invitation.id = candidate.invitation_id
        left join person_identities invitee_identity on invitee_identity.id = invitation.invitee_identity_id
        left join households invitation_household on invitation_household.id = invitation.household_id
        left join conversations invitation_source on invitation_source.id = invitation.source_conversation_id
        left join participant_epochs invitation_epoch on invitation_epoch.id = invitation.source_participant_epoch_id
        left join conversations source_conversation on source_conversation.id = candidate.source_conversation_id
        left join participant_epochs source_epoch on source_epoch.id = candidate.source_participant_epoch_id
        left join private_source_frontiers source_frontier
          on source_frontier.id = candidate.private_source_frontier_id
        where candidate.id = ${effect.outboxId}
          and candidate.status = 'leased' and candidate.lease_token = ${effect.leaseToken}
          and candidate.authorization_decision_id = ${target.authorization_decision_id}
          and candidate.person_id is not distinct from ${target.person_id}::uuid
          and candidate.integration_id is not distinct from ${target.integration_id}::uuid
          and candidate.private_source_frontier_id is not distinct from
            ${target.private_source_frontier_id}::uuid
          and candidate.private_source_case_key_digest is not distinct from
            ${target.private_source_case_key_digest}::text
          and decision.outcome = 'allow' and decision.revoked_at is null
          and decision.expires_at > ${authorizedAt}
          and (
            cardinality(candidate.evidence_source_revision_ids) = 0
            or decision.expires_at > ${new Date(authorizedAt.getTime() + 3 * 60_000)}
          )
          and (candidate.person_id is null or (
            person.status = 'registered' and person.control_epoch = candidate.person_control_epoch
          ))
          and (candidate.household_id is null or (
            household.status in ('onboarding', 'active', 'paused')
            and household.control_epoch = candidate.household_control_epoch
          ))
          and (candidate.integration_id is null or (
            integration.status <> 'revoked'
            and integration.control_epoch = candidate.integration_control_epoch
          ))
          and (candidate.conversation_id is null or (
            conversation.status = 'active'
            and conversation.authority_version = candidate.conversation_authority_version
            and conversation.current_epoch_id = candidate.participant_epoch_id
            and epoch.ended_at is null
            and epoch.participant_set_digest = candidate.expected_participant_digest
          ))
          and (candidate.coverage_loop_id is null or (
            coverage.version = candidate.coverage_loop_version
          ))
          and (candidate.invitation_id is null or (
            invitation.status = 'pending' and invitation.expires_at > ${authorizedAt}
            and invitation_household.membership_version = invitation.household_membership_version
            and invitee_identity.status in ('observed', 'verified')
            and invitee_identity.authority_version = candidate.invitee_identity_authority_version
            and invitation_source.status = 'active'
            and invitation_source.current_epoch_id = invitation.source_participant_epoch_id
            and invitation_epoch.ended_at is null
            and invitation_epoch.participant_set_digest = invitation.source_participant_digest
          ))
          and (candidate.source_conversation_id is null or (
            source_conversation.status = 'active'
            and source_conversation.authority_version = candidate.source_conversation_authority_version
            and source_conversation.current_epoch_id = candidate.source_participant_epoch_id
            and source_epoch.ended_at is null
            and source_epoch.participant_set_digest = candidate.source_expected_participant_digest
          ))
          and (candidate.private_source_frontier_id is null or (
            source_frontier.owner_person_id = candidate.person_id
            and source_frontier.integration_id = candidate.integration_id
            and source_frontier.case_kind = 'gmail_thread'
            and source_frontier.case_key_digest = candidate.private_source_case_key_digest
            and source_frontier.version = candidate.private_source_frontier_version
            and source_frontier.frontier_digest = candidate.private_source_frontier_digest
            and source_frontier.source_generation = candidate.private_source_generation
            and source_frontier.reconciled_generation = candidate.private_source_generation
          ))
          and source_evidence_set_is_current(
            candidate.evidence_source_revision_ids,
            candidate.source_participant_epoch_id,
            candidate.person_id,
            ${authorizedAt}
          )
        for update of candidate
      `;
      if (!rows[0]) {
        await cancelLeasedEffect(transaction, effect, authorizedAt);
        return { authorized: false };
      }
      const result = await submit();
      return { authorized: true, result };
    });
  }

  public async recordReceipt(input: {
    effect: Pick<ClaimedEffect, "outboxId" | "leaseToken" | "idempotencyKey">;
    status: "submitted" | "confirmed" | "failed" | "ambiguous";
    providerReceiptId?: string;
    receipt: unknown;
    errorCode?: string;
    now?: Date;
  }): Promise<boolean> {
    const now = input.now ?? new Date();
    const receiptJson = canonicalJson(input.receipt);
    const receiptDigest = sha256Hex(receiptJson);
    const encrypted = this.secretBox.encrypt(receiptJson, "effect-receipt");
    return inTransaction(this.database, async (transaction) => {
      const effects = await transaction<{ id: string }[]>`
        select id from outbox where id = ${input.effect.outboxId}
          and status = 'leased' and lease_token = ${input.effect.leaseToken}
        for update
      `;
      if (!effects[0]) return false;
      await transaction`
        insert into effect_receipts (
          id, outbox_id, idempotency_key, provider_receipt_id, status,
          receipt_digest, receipt_ciphertext, receipt_key_version, occurred_at,
          reconciled_at, error_code
        ) values (
          ${randomUUID()}, ${input.effect.outboxId}, ${input.effect.idempotencyKey},
          ${input.providerReceiptId ?? null}, ${input.status}, ${receiptDigest},
          ${Buffer.from(JSON.stringify(encrypted), "utf8")}, ${encrypted.kid}, ${now},
          ${input.status === "submitted" ? null : now}, ${input.errorCode ?? null}
        ) on conflict do nothing
      `;
      const terminal =
        input.status === "confirmed"
          ? "confirmed"
          : input.status === "failed"
            ? "dead"
            : input.status === "ambiguous"
              ? "ambiguous"
              : "submitted";
      await transaction`
        update outbox set status = ${terminal}, lease_owner = null, lease_token = null,
          lease_expires_at = null,
          available_at = ${input.status === "submitted" ? new Date(now.getTime() + 60_000) : now},
          last_error_code = ${input.errorCode ?? null}, updated_at = ${now}
        where id = ${input.effect.outboxId}
      `;
      return true;
    });
  }

  public async recordReconciliation(input: {
    effect: Pick<
      ClaimedSubmittedEffect,
      "outboxId" | "leaseToken" | "idempotencyKey" | "reconciliationAttemptCount"
    >;
    status: "submitted" | "confirmed" | "failed" | "ambiguous";
    providerReceiptId?: string;
    receipt: unknown;
    errorCode?: string;
    nextAttemptAt?: Date;
    now?: Date;
  }): Promise<boolean> {
    const now = input.now ?? new Date();
    if (input.nextAttemptAt && input.nextAttemptAt <= now) {
      throw new Error("A reconciliation retry time must be in the future");
    }
    if (input.status === "submitted" && !input.nextAttemptAt) {
      throw new Error("An unresolved reconciliation needs a future retry time");
    }
    if (input.nextAttemptAt && input.status !== "submitted" && input.status !== "failed") {
      throw new Error("Only unresolved or provisionally failed delivery can be rechecked");
    }
    const receiptJson = canonicalJson({
      reconciliationAttempt: input.effect.reconciliationAttemptCount,
      result: input.receipt,
    });
    const receiptDigest = sha256Hex(receiptJson);
    const encrypted = this.secretBox.encrypt(receiptJson, "effect-receipt");
    return inTransaction(this.database, async (transaction) => {
      const effects = await transaction<{ id: string }[]>`
        select id from outbox where id = ${input.effect.outboxId}
          and status = 'submitted' and lease_token = ${input.effect.leaseToken}
        for update
      `;
      if (!effects[0]) return false;
      await transaction`
        insert into effect_receipts (
          id, outbox_id, idempotency_key, provider_receipt_id, status,
          receipt_digest, receipt_ciphertext, receipt_key_version, occurred_at,
          reconciled_at, error_code
        ) values (
          ${randomUUID()}, ${input.effect.outboxId}, ${input.effect.idempotencyKey},
          ${input.providerReceiptId ?? null}, ${input.status}, ${receiptDigest},
          ${Buffer.from(JSON.stringify(encrypted), "utf8")}, ${encrypted.kid}, ${now},
          ${input.status === "submitted" ? null : now}, ${input.errorCode ?? null}
        ) on conflict do nothing
      `;
      const terminal = input.nextAttemptAt
        ? "submitted"
        : input.status === "confirmed"
          ? "confirmed"
          : input.status === "failed"
            ? "dead"
            : input.status === "ambiguous"
              ? "ambiguous"
              : "submitted";
      await transaction`
        update outbox set status = ${terminal}, available_at = ${input.nextAttemptAt ?? now},
          lease_owner = null, lease_token = null, lease_expires_at = null,
          last_error_code = ${input.errorCode ?? null}, updated_at = ${now}
        where id = ${input.effect.outboxId}
      `;
      return true;
    });
  }

  public async retry(
    effect: Pick<ClaimedEffect, "outboxId" | "leaseToken" | "attemptCount">,
    errorCode: string,
    retryable: boolean,
    now = new Date(),
  ): Promise<"retry" | "dead" | "stale"> {
    // A provider outage must not turn an already-authorized family message into
    // an operator repair. Retryable failures keep backing off until their
    // authority fence expires or changes; non-retryable failures still stop.
    const retry = retryable;
    const delay = Math.min(15 * 60_000, 1_000 * 2 ** Math.min(10, Math.max(0, effect.attemptCount - 1)));
    const rows = await this.database<{ status: "retry" | "dead" }[]>`
      update outbox set status = ${retry ? "retry" : "dead"},
        available_at = ${retry ? new Date(now.getTime() + delay) : now}, lease_owner = null,
        lease_token = null, lease_expires_at = null, last_error_code = ${errorCode.slice(0, 200)},
        updated_at = ${now}
      where id = ${effect.outboxId} and status = 'leased' and lease_token = ${effect.leaseToken}
      returning status
    `;
    return rows[0]?.status ?? "stale";
  }

  /**
   * Starts a fresh provider attempt only after Linq has definitively reported
   * failure. The original attempt and receipts remain immutable, and every
   * person/household/conversation/loop/invitation fence must still match.
   */
  public async redriveFailed(now = new Date(), limit = 20): Promise<number> {
    return inTransaction(this.database, async (transaction) => {
      const candidates = await transaction<
        {
          id: string;
          authorization_decision_id: string;
          root_id: string;
          redrive_sequence: number | string;
          updated_at: Date;
        }[]
      >`
        select effect.id, effect.authorization_decision_id,
          coalesce(effect.redrive_root_id, effect.id) as root_id,
          coalesce(effect.redrive_sequence, 0) as redrive_sequence,
          effect.updated_at
        from outbox effect
        join disclosure_decisions decision on decision.id = effect.authorization_decision_id
        left join people person on person.id = effect.person_id
        left join households household on household.id = effect.household_id
        left join conversations conversation on conversation.id = effect.conversation_id
        left join integrations integration on integration.id = effect.integration_id
        left join participant_epochs epoch on epoch.id = effect.participant_epoch_id
        left join coverage_loops coverage on coverage.id = effect.coverage_loop_id
        left join invitations invitation on invitation.id = effect.invitation_id
        left join person_identities invitee_identity on invitee_identity.id = invitation.invitee_identity_id
        left join households invitation_household on invitation_household.id = invitation.household_id
        left join conversations invitation_source on invitation_source.id = invitation.source_conversation_id
        left join participant_epochs invitation_epoch on invitation_epoch.id = invitation.source_participant_epoch_id
        left join conversations source_conversation on source_conversation.id = effect.source_conversation_id
        left join participant_epochs source_epoch on source_epoch.id = effect.source_participant_epoch_id
        left join private_source_frontiers source_frontier
          on source_frontier.id = effect.private_source_frontier_id
        where effect.status = 'dead' and effect.last_error_code = 'linq_delivery_failed'
          and effect.action_intent_id is null
          and decision.outcome = 'allow' and decision.revoked_at is null and decision.expires_at > ${now}
          and (
            cardinality(effect.evidence_source_revision_ids) = 0
            or decision.expires_at > ${new Date(now.getTime() + 3 * 60_000)}
          )
          and (effect.person_id is null or (
            person.status = 'registered' and person.control_epoch = effect.person_control_epoch
          ))
          and (effect.household_id is null or (
            household.status in ('onboarding', 'active', 'paused')
            and household.control_epoch = effect.household_control_epoch
          ))
          and (effect.integration_id is null or (
            integration.status <> 'revoked'
            and integration.control_epoch = effect.integration_control_epoch
          ))
          and (effect.conversation_id is null or (
            conversation.status = 'active'
            and conversation.authority_version = effect.conversation_authority_version
            and conversation.current_epoch_id = effect.participant_epoch_id
            and epoch.ended_at is null
            and epoch.participant_set_digest = effect.expected_participant_digest
          ))
          and (effect.coverage_loop_id is null or coverage.version = effect.coverage_loop_version)
          and (effect.invitation_id is null or (
            invitation.status = 'pending' and invitation.expires_at > ${now}
            and invitation_household.membership_version = invitation.household_membership_version
            and invitee_identity.status in ('observed', 'verified')
            and invitee_identity.authority_version = effect.invitee_identity_authority_version
            and invitation_source.status = 'active'
            and invitation_source.current_epoch_id = invitation.source_participant_epoch_id
            and invitation_epoch.ended_at is null
            and invitation_epoch.participant_set_digest = invitation.source_participant_digest
          ))
          and (effect.source_conversation_id is null or (
            source_conversation.status = 'active'
            and source_conversation.authority_version = effect.source_conversation_authority_version
            and source_conversation.current_epoch_id = effect.source_participant_epoch_id
            and source_epoch.ended_at is null
            and source_epoch.participant_set_digest = effect.source_expected_participant_digest
          ))
          and (effect.private_source_frontier_id is null or (
            source_frontier.owner_person_id = effect.person_id
            and source_frontier.integration_id = effect.integration_id
            and source_frontier.case_kind = 'gmail_thread'
            and source_frontier.case_key_digest = effect.private_source_case_key_digest
            and source_frontier.version = effect.private_source_frontier_version
            and source_frontier.frontier_digest = effect.private_source_frontier_digest
            and source_frontier.source_generation = effect.private_source_generation
            and source_frontier.reconciled_generation = effect.private_source_generation
          ))
          and source_evidence_set_is_current(
            effect.evidence_source_revision_ids,
            effect.source_participant_epoch_id,
            effect.person_id,
            ${now}
          )
          and not exists (
            select 1 from outbox later
            where later.redrive_root_id = coalesce(effect.redrive_root_id, effect.id)
              and later.redrive_sequence > coalesce(effect.redrive_sequence, 0)
          )
        order by effect.updated_at
        for update of effect skip locked
        limit ${Math.max(1, Math.min(limit * 4, 100))}
      `;
      let redriven = 0;
      for (const candidate of candidates) {
        if (redriven >= limit) break;
        const nextSequence = Number(candidate.redrive_sequence) + 1;
        const delayMs = Math.min(6 * 60 * 60_000, 60_000 * 2 ** Math.min(nextSequence - 1, 9));
        if (candidate.updated_at.getTime() + delayMs > now.getTime()) continue;
        const idempotencyKey = `linq-redrive:${candidate.root_id}:attempt-${nextSequence}`;
        const decisionId = randomUUID();
        const decisions = await transaction<{ id: string }[]>`
          insert into disclosure_decisions (
            id, outcome, actor_person_id, household_id, conversation_id, participant_epoch_id,
            action_digest, data_digest, policy_digest, target_digest, reason_codes,
            decided_at, expires_at
          )
          select ${decisionId}, decision.outcome, decision.actor_person_id, decision.household_id,
            decision.conversation_id, decision.participant_epoch_id,
            ${digest({ effectKind: "linq.message", idempotencyKey })}, decision.data_digest,
            decision.policy_digest, decision.target_digest,
            array_append(decision.reason_codes, 'automatic_delivery_redrive'), ${now}, decision.expires_at
          from disclosure_decisions decision
          where decision.id = ${candidate.authorization_decision_id}
            and decision.outcome = 'allow' and decision.revoked_at is null
            and decision.expires_at > ${now}
          returning id
        `;
        if (!decisions[0]) continue;
        const rows = await transaction<{ id: string }[]>`
          insert into outbox (
            id, authorization_decision_id, action_intent_id, household_id, person_id, conversation_id,
            integration_id, integration_control_epoch,
            participant_epoch_id, expected_participant_digest, coverage_loop_id, coverage_loop_version,
            invitation_id, invitee_identity_authority_version, redrive_root_id, redrive_sequence,
            source_conversation_id, source_participant_epoch_id,
            source_expected_participant_digest, source_conversation_authority_version,
            private_source_frontier_id, private_source_frontier_version,
            private_source_frontier_digest, private_source_generation,
            private_source_case_key_digest,
            evidence_source_revision_ids,
            effect_kind, idempotency_key, payload_digest, payload_ciphertext, payload_key_version,
            status, attempt_count, reconciliation_attempt_count, available_at,
            person_control_epoch, household_control_epoch, conversation_authority_version,
            created_at, updated_at
          )
          select ${randomUUID()}, ${decisionId}, effect.action_intent_id, effect.household_id,
            effect.person_id, effect.conversation_id, effect.integration_id,
            effect.integration_control_epoch, effect.participant_epoch_id,
            effect.expected_participant_digest, effect.coverage_loop_id, effect.coverage_loop_version,
            effect.invitation_id, effect.invitee_identity_authority_version,
            ${candidate.root_id}, ${nextSequence}, effect.source_conversation_id,
            effect.source_participant_epoch_id, effect.source_expected_participant_digest,
            effect.source_conversation_authority_version, effect.private_source_frontier_id,
            effect.private_source_frontier_version, effect.private_source_frontier_digest,
            effect.private_source_generation, effect.private_source_case_key_digest,
            effect.evidence_source_revision_ids,
            effect.effect_kind, ${idempotencyKey},
            effect.payload_digest, effect.payload_ciphertext, effect.payload_key_version,
            'pending', 0, 0, ${now}, effect.person_control_epoch, effect.household_control_epoch,
            effect.conversation_authority_version, ${now}, ${now}
          from outbox effect
          where effect.id = ${candidate.id} and effect.status = 'dead'
          on conflict do nothing
          returning id
        `;
        redriven += rows.length;
      }
      return redriven;
    });
  }

  /** Removes effects that can no longer be claimed so queue health reflects reality. */
  public async cancelStale(now = new Date()): Promise<number> {
    const rows = await this.database<{ readonly id: string }[]>`
      update outbox effect set status = 'cancelled', lease_owner = null, lease_token = null,
        lease_expires_at = null, last_error_code = 'authority_fence_changed', updated_at = ${now}
      where effect.status in ('pending', 'retry', 'leased') and (
        not exists (
          select 1 from disclosure_decisions decision
          where decision.id = effect.authorization_decision_id
            and decision.outcome = 'allow' and decision.revoked_at is null
            and decision.expires_at > ${now}
            and (
              cardinality(effect.evidence_source_revision_ids) = 0
              or decision.expires_at > ${new Date(now.getTime() + 3 * 60_000)}
            )
        )
        or (effect.person_id is not null and not exists (
          select 1 from people person where person.id = effect.person_id
            and person.status = 'registered' and person.control_epoch = effect.person_control_epoch
        ))
        or (effect.household_id is not null and not exists (
          select 1 from households household where household.id = effect.household_id
            and household.status in ('onboarding', 'active', 'paused')
            and household.control_epoch = effect.household_control_epoch
        ))
        or (effect.integration_id is not null and not exists (
          select 1 from integrations integration where integration.id = effect.integration_id
            and integration.status <> 'revoked'
            and integration.control_epoch = effect.integration_control_epoch
        ))
        or (effect.conversation_id is not null and not exists (
          select 1
          from conversations conversation
          join participant_epochs epoch on epoch.id = conversation.current_epoch_id
          where conversation.id = effect.conversation_id and conversation.status = 'active'
            and conversation.authority_version = effect.conversation_authority_version
            and conversation.current_epoch_id = effect.participant_epoch_id
            and epoch.ended_at is null
            and epoch.participant_set_digest = effect.expected_participant_digest
        ))
        or (effect.coverage_loop_id is not null and not exists (
          select 1 from coverage_loops coverage
          where coverage.id = effect.coverage_loop_id
            and coverage.version = effect.coverage_loop_version
        ))
        or (effect.invitation_id is not null and not exists (
          select 1
          from invitations invitation
          join person_identities invitee_identity on invitee_identity.id = invitation.invitee_identity_id
          join households household on household.id = invitation.household_id
          join conversations source on source.id = invitation.source_conversation_id
          join participant_epochs epoch on epoch.id = invitation.source_participant_epoch_id
          where invitation.id = effect.invitation_id
            and invitation.status = 'pending' and invitation.expires_at > ${now}
            and household.membership_version = invitation.household_membership_version
            and invitee_identity.status in ('observed', 'verified')
            and invitee_identity.authority_version = effect.invitee_identity_authority_version
            and source.status = 'active' and source.current_epoch_id = epoch.id
            and epoch.ended_at is null
            and epoch.participant_set_digest = invitation.source_participant_digest
        ))
        or (effect.source_conversation_id is not null and not exists (
          select 1
          from conversations source
          join participant_epochs epoch on epoch.id = source.current_epoch_id
          where source.id = effect.source_conversation_id and source.status = 'active'
            and source.authority_version = effect.source_conversation_authority_version
            and source.current_epoch_id = effect.source_participant_epoch_id
            and epoch.ended_at is null
            and epoch.participant_set_digest = effect.source_expected_participant_digest
        ))
        or (effect.private_source_frontier_id is not null and not exists (
          select 1
          from private_source_frontiers source_frontier
          where source_frontier.id = effect.private_source_frontier_id
            and source_frontier.owner_person_id = effect.person_id
            and source_frontier.integration_id = effect.integration_id
            and source_frontier.case_kind = 'gmail_thread'
            and source_frontier.case_key_digest = effect.private_source_case_key_digest
            and source_frontier.version = effect.private_source_frontier_version
            and source_frontier.frontier_digest = effect.private_source_frontier_digest
            and source_frontier.source_generation = effect.private_source_generation
            and source_frontier.reconciled_generation = effect.private_source_generation
        ))
        or not source_evidence_set_is_current(
          effect.evidence_source_revision_ids,
          effect.source_participant_epoch_id,
          effect.person_id,
          ${now}
        )
      )
      returning id
    `;
    return rows.length;
  }
}

async function authorizePrivateSourceFrontier(
  transaction: Transaction,
  input: AuthorizedEffectInput,
): Promise<{ readonly id: string; readonly controlEpoch: number }> {
  const frontier = input.sourceFrontier;
  const person = input.person;
  if (!frontier || !person || input.actorPersonId !== person.id) {
    throw new UnauthorizedError("Private source effects require their exact source owner");
  }
  await acquirePrivateSourceEffectLocks(transaction, {
    ownerPersonId: person.id,
    integrationId: frontier.integrationId,
    caseKeyDigest: frontier.caseKeyDigest,
  });
  const rows = await transaction<{ readonly integration_control_epoch: number | string }[]>`
    select integration.control_epoch as integration_control_epoch
    from private_source_frontiers source_frontier
    join people person on person.id = source_frontier.owner_person_id
    join integrations integration on integration.id = source_frontier.integration_id
      and integration.person_id = source_frontier.owner_person_id
    where source_frontier.id = ${frontier.frontierId}
      and source_frontier.owner_person_id = ${person.id}
      and source_frontier.integration_id = ${frontier.integrationId}
      and source_frontier.case_kind = 'gmail_thread'
      and source_frontier.case_key_digest = ${frontier.caseKeyDigest}
      and source_frontier.version = ${frontier.version}
      and source_frontier.frontier_digest = ${frontier.frontierDigest}
      and source_frontier.source_generation = ${frontier.sourceGeneration}
      and source_frontier.reconciled_generation = ${frontier.sourceGeneration}
      and person.status = 'registered' and person.control_epoch = ${person.controlEpoch}
      and integration.provider = 'google' and integration.status = 'active'
    for share of source_frontier, person, integration
  `;
  const current = rows[0];
  if (!current) {
    throw new UnauthorizedError("Private source effect frontier is no longer exact, clean, and current");
  }
  const integrationControlEpoch = Number(current.integration_control_epoch);
  if (
    input.integration &&
    (input.integration.id !== frontier.integrationId ||
      input.integration.controlEpoch !== integrationControlEpoch)
  ) {
    throw new UnauthorizedError("Private source effect integration fence does not match its frontier");
  }
  return { id: frontier.integrationId, controlEpoch: integrationControlEpoch };
}

async function acquirePrivateSourceEffectLocks(
  transaction: Transaction,
  input: {
    readonly ownerPersonId: string;
    readonly integrationId: string;
    readonly caseKeyDigest: string;
  },
): Promise<void> {
  await transaction`
    select pg_advisory_xact_lock(
      hashtextextended(${privateSourceIntegrationLockKey(input.integrationId)}, 0)
    )
  `;
  await transaction`
    select pg_advisory_xact_lock(
      hashtextextended(${gmailThreadFrontierLockKey(input)}, 0)
    )
  `;
}

async function lockEffectSubmissionAuthority(
  transaction: Transaction,
  target: EffectSourceLockTarget,
): Promise<void> {
  if (target.private_source_frontier_id === null) return;
  if (target.person_id !== null) {
    await transaction`select id from people where id = ${target.person_id} for share`;
  }
  if (target.integration_id !== null) {
    await transaction`select id from integrations where id = ${target.integration_id} for share`;
  }
  await transaction`
    select id from private_source_frontiers
    where id = ${target.private_source_frontier_id}
    for share
  `;
  // Revocation paths update the decision before waiting on the outbox row.
  // Match that order so authority cannot be revoked during the provider POST.
  await transaction`
    select id from disclosure_decisions
    where id = ${target.authorization_decision_id}
    for share
  `;
}

async function cancelLeasedEffect(
  transaction: Transaction,
  effect: Pick<ClaimedEffect, "outboxId" | "leaseToken">,
  cancelledAt: Date,
): Promise<void> {
  await transaction`
    update outbox set status = 'cancelled', lease_owner = null, lease_token = null,
      lease_expires_at = null, last_error_code = 'authority_fence_changed', updated_at = ${cancelledAt}
    where id = ${effect.outboxId} and status = 'leased' and lease_token = ${effect.leaseToken}
  `;
}

async function loadCurrentActionIntentDigests(
  transaction: Transaction,
  actionIntentId: string,
  input: AuthorizedEffectInput,
  now: Date,
): Promise<{
  readonly actionDigest: string;
  readonly dataDigest: string;
  readonly policyDigest: string;
  readonly targetDigest: string;
}> {
  const rows = await transaction<
    {
      readonly person_id: string;
      readonly household_id: string;
      readonly conversation_id: string;
      readonly participant_epoch_id: string;
      readonly action_digest: string;
      readonly data_digest: string;
      readonly policy_digest: string;
      readonly target_digest: string;
    }[]
  >`
    select person_id, household_id, conversation_id, participant_epoch_id,
      action_digest, data_digest, policy_digest, target_digest
    from action_intents
    where id = ${actionIntentId}
      and status in ('approved', 'executing') and expires_at > ${now}
    for share
  `;
  const intent = rows[0];
  if (
    !intent ||
    input.actorPersonId !== intent.person_id ||
    input.person?.id !== intent.person_id ||
    input.household?.id !== intent.household_id ||
    input.conversation?.id !== intent.conversation_id ||
    input.participantEpochId !== intent.participant_epoch_id
  ) {
    throw new UnauthorizedError("Effect scope does not match its current approved action intent");
  }
  return {
    actionDigest: intent.action_digest,
    dataDigest: intent.data_digest,
    policyDigest: intent.policy_digest,
    targetDigest: intent.target_digest,
  };
}

function inTransaction<Result>(
  executor: Executor,
  operation: (transaction: Transaction) => Promise<Result>,
): Promise<Result> {
  return "begin" in executor
    ? (executor.begin(operation) as unknown as Promise<Result>)
    : operation(executor);
}

function validateEffectScope(input: AuthorizedEffectInput): void {
  const evidenceSourceRevisionIds = [...(input.evidenceSourceRevisionIds ?? [])];
  if (
    input.integration &&
    (!Number.isSafeInteger(input.integration.controlEpoch) || input.integration.controlEpoch < 1)
  ) {
    throw new Error("Integration effects require a positive control epoch");
  }
  if (
    evidenceSourceRevisionIds.length > 32 ||
    new Set(evidenceSourceRevisionIds).size !== evidenceSourceRevisionIds.length ||
    evidenceSourceRevisionIds.some(
      (sourceRevisionId) =>
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu.test(sourceRevisionId),
    )
  ) {
    throw new Error("Effect source evidence must contain at most 32 distinct revision IDs");
  }
  if (evidenceSourceRevisionIds.length > 0 && (!input.person || !input.sourceConversation)) {
    throw new Error("Effect source evidence requires exact viewer and source-conversation fences");
  }
  if (input.sourceFrontier) {
    const frontier = input.sourceFrontier;
    if (
      !input.person ||
      input.actorPersonId !== input.person.id ||
      (input.integration !== undefined && input.integration.id !== frontier.integrationId) ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu.test(frontier.frontierId) ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu.test(frontier.integrationId) ||
      !/^[a-f0-9]{64}$/u.test(frontier.caseKeyDigest) ||
      !/^[a-f0-9]{64}$/u.test(frontier.frontierDigest) ||
      !Number.isSafeInteger(frontier.version) ||
      frontier.version < 1 ||
      !Number.isSafeInteger(frontier.sourceGeneration) ||
      frontier.sourceGeneration < 0
    ) {
      throw new Error("Private source effects require a complete exact owner and frontier fence");
    }
  }
  if (
    input.coverageLoop &&
    (!Number.isSafeInteger(input.coverageLoop.version) || input.coverageLoop.version < 1)
  ) {
    throw new Error("Coverage effect versions must be positive integers");
  }
  if (
    input.invitation &&
    (!Number.isSafeInteger(input.invitation.inviteeIdentityAuthorityVersion) ||
      input.invitation.inviteeIdentityAuthorityVersion < 1)
  ) {
    throw new Error("Invitation effects require an exact identity authority version");
  }
  if (
    input.sourceConversation &&
    (!Number.isSafeInteger(input.sourceConversation.authorityVersion) ||
      input.sourceConversation.authorityVersion < 1 ||
      !input.sourceConversation.participantEpochId ||
      !/^[a-f0-9]{64}$/u.test(input.sourceConversation.participantSetDigest))
  ) {
    throw new Error("Source conversation effects require an exact authority fence");
  }
  if (input.conversation) {
    if (!input.participantEpochId || !input.expectedParticipantDigest) {
      throw new Error("Conversation effects require an exact participant epoch and digest");
    }
  } else if (input.participantEpochId || input.expectedParticipantDigest) {
    throw new Error("Participant authority cannot exist without a conversation effect scope");
  }
}

function digest(value: unknown): string {
  return sha256Hex(canonicalJson(value));
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
