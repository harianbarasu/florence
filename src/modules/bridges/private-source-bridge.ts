import { randomUUID } from "node:crypto";
import { Temporal } from "@js-temporal/polyfill";
import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { Database } from "../../db/client.js";
import { canonicalDigest, canonicalJson } from "../../shared/canonical-json.js";
import type { SecretBox } from "../../shared/crypto.js";
import { ConflictError, NotFoundError, StaleAuthorityError, UnauthorizedError } from "../../shared/errors.js";
import { evaluateConversationMode, PostgresConversationAuthority } from "../conversations/index.js";
import {
  type CoverageLoop,
  type CoverageState,
  createCoverageLoop,
  PostgresCoordination,
  planCoverageFollowUpTimer,
} from "../coordination/index.js";
import { EffectOutbox } from "../effects/index.js";
import { commitmentProposalSchema, minimumDisclosureSchema } from "../orchestration/skills.js";
import {
  gmailThreadFrontierLockKey,
  JsonObjectSchema,
  PostgresSourceIntelligence,
  privateSourceIntegrationLockKey,
} from "../sources/index.js";
import { DurableTimers, DurableWork } from "../work/index.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

const DigestSchema = z.string().regex(/^[a-f0-9]{64}$/u);
const ProviderDigestSchema = z.string().regex(/^linq-v1:[a-f0-9]{64}$/u);

const SourcePatternSchema = z.discriminatedUnion("kind", [
  z.strictObject({
    version: z.literal(1),
    kind: z.literal("gmail_thread"),
    integrationId: z.string().uuid(),
    provider: z.literal("gmail"),
    objectKind: z.literal("mail_message"),
    classification: z.literal("coverage_proposal"),
    threadDigest: DigestSchema,
    senderDigest: DigestSchema,
  }),
  z.strictObject({
    version: z.literal(1),
    kind: z.literal("calendar_series"),
    integrationId: z.string().uuid(),
    provider: z.literal("google.calendar"),
    objectKind: z.literal("calendar_event"),
    classification: z.literal("coverage_proposal"),
    recurrenceDigest: DigestSchema,
  }),
]);
export type PrivateBridgeSourcePattern = z.infer<typeof SourcePatternSchema>;

const DestinationSchema = z.strictObject({
  householdId: z.string().uuid(),
  householdControlEpoch: z.number().int().positive(),
  conversationId: z.string().uuid(),
  participantEpochId: z.string().uuid(),
  participantSetDigest: DigestSchema,
  conversationAuthorityVersion: z.number().int().positive(),
  providerChatId: z.string().uuid(),
  providerParticipantDigest: ProviderDigestSchema,
  liveIdentityIds: z.array(z.string().uuid()).min(1).max(100),
  proactiveRuleId: z.string().uuid(),
  timeZone: z.string().min(1).max(100),
});

const StandingRuleFenceSchema = z.strictObject({
  ruleId: z.string().uuid(),
  ruleRevisionId: z.string().uuid(),
  ruleVersion: z.number().int().positive(),
});

const LoopUpdateReviewContentSchema = z.object({
  existingLoopId: z.string().uuid(),
  sourceActionIntentId: z.string().uuid(),
  priorCandidateId: z.string().uuid(),
});

const IntentDigestFenceSchema = z.strictObject({
  actionDigest: DigestSchema,
  dataDigest: DigestSchema,
  policyDigest: DigestSchema,
  targetDigest: DigestSchema,
});

const SourceFrontierFenceSchema = z.strictObject({
  frontierId: z.string().uuid(),
  integrationId: z.string().uuid(),
  integrationControlEpoch: z.number().int().positive(),
  caseKind: z.literal("gmail_thread"),
  caseKeyDigest: DigestSchema,
  version: z.number().int().positive(),
  frontierDigest: DigestSchema,
  sourceGeneration: z.number().int().nonnegative(),
});
export type PrivateBridgeSourceFrontier = z.infer<typeof SourceFrontierFenceSchema>;

const LoopUpdateFenceSchema = z.strictObject({
  existingLoopId: z.string().uuid(),
  expectedLoopVersion: z.number().int().positive(),
  expectedLoopDestinationDigest: DigestSchema,
  priorCandidateId: z.string().uuid(),
  priorCandidateContentDigest: DigestSchema,
  sourceActionIntentId: z.string().uuid(),
  sourceActionIntentDigests: IntentDigestFenceSchema,
});

const PreparedPayloadObjectSchema = z.strictObject({
  schemaVersion: z.literal(1),
  phase: z.literal("prepared"),
  candidateId: z.string().uuid(),
  candidateContentDigest: DigestSchema,
  evidenceSourceRevisionIds: z.array(z.string().uuid()).min(1).max(100),
  evidenceDigest: DigestSchema,
  sourcePattern: SourcePatternSchema.nullable(),
  sourceFrontier: SourceFrontierFenceSchema.nullable(),
  destination: DestinationSchema,
  personControlEpoch: z.number().int().positive(),
  standingRule: StandingRuleFenceSchema.nullable(),
  loopUpdate: LoopUpdateFenceSchema.nullable(),
});

const PreparedPayloadSchema = PreparedPayloadObjectSchema.superRefine(requirePayloadSourceFrontier);

const ProposedPayloadSchema = PreparedPayloadObjectSchema.omit({ phase: true })
  .extend({
    phase: z.literal("awaiting_approval"),
    loopId: z.string().uuid(),
    minimumDisclosure: minimumDisclosureSchema,
    commitment: commitmentProposalSchema,
    approvalMode: z.enum(["once", "standing"]).nullable(),
  })
  .superRefine(requirePayloadSourceFrontier);

function requirePayloadSourceFrontier(
  payload: Pick<
    z.infer<typeof PreparedPayloadObjectSchema>,
    "sourcePattern" | "sourceFrontier" | "loopUpdate"
  >,
  context: z.RefinementCtx,
): void {
  if ((payload.sourcePattern || payload.loopUpdate) && !payload.sourceFrontier) {
    context.addIssue({
      code: "custom",
      path: ["sourceFrontier"],
      message: "Integration-backed private source actions require their exact current frontier",
    });
    return;
  }
  if (
    payload.sourcePattern &&
    payload.sourceFrontier &&
    payload.sourcePattern.integrationId !== payload.sourceFrontier.integrationId
  ) {
    context.addIssue({
      code: "custom",
      path: ["sourceFrontier", "integrationId"],
      message: "Private source pattern and frontier must use the same integration",
    });
  }
}

export const PrivateBridgePayloadSchema = z.discriminatedUnion("phase", [
  PreparedPayloadSchema,
  ProposedPayloadSchema,
]);
export type PrivateBridgePayload = z.infer<typeof PrivateBridgePayloadSchema>;

export interface PrivateBridgeDestinationChoice {
  readonly conversationId: string;
  readonly label: string;
  readonly participantCount: number;
}

export interface PrivateBridgeProposalContext {
  readonly actionIntentId: string;
  readonly ownerPersonId: string;
  readonly candidate: Record<string, unknown>;
  readonly evidenceSourceRevisionIds: readonly string[];
  readonly destination: z.infer<typeof DestinationSchema>;
  readonly currentParticipantPersonIds: readonly string[];
  readonly standingRule: z.infer<typeof StandingRuleFenceSchema> | null;
  readonly loopUpdate: z.infer<typeof LoopUpdateFenceSchema> | null;
}

export interface PrivateBridgeProposalInput {
  readonly actionIntentId: string;
  readonly minimumDisclosure: z.infer<typeof minimumDisclosureSchema>;
  readonly commitment: z.infer<typeof commitmentProposalSchema>;
}

export interface PrivateBridgeApprovalInput {
  readonly actorPersonId: string;
  readonly actionIntentId: string;
  readonly actionDigest: string;
  readonly dataDigest: string;
  readonly policyDigest: string;
  readonly targetDigest: string;
  readonly mode: "once" | "standing";
}

export interface AcceptedPrivateBridgeCandidateInput {
  readonly ownerPersonId: string;
  readonly candidateId: string;
  readonly candidateContentDigest: string;
}

export interface AcceptedPrivateBridgeLoopReference {
  readonly actionIntentId: string;
  readonly loopId: string;
}

export interface AcceptedPrivateBridgeWithdrawalInput extends AcceptedPrivateBridgeCandidateInput {
  readonly intent: "cancel" | "supersede";
  readonly evidenceSourceRevisionIds: readonly string[];
  readonly replacementSourceFrontier: PrivateBridgeSourceFrontier;
  readonly withdrawnAt: Date;
}

export type AcceptedPrivateBridgeCorrection =
  | { readonly kind: "not_needed" }
  | { readonly kind: "not_authorized" }
  | { readonly kind: "queued"; readonly outboxId: string; readonly duplicate: boolean };

export type AcceptedPrivateBridgeWithdrawalResult =
  | { readonly kind: "not_found" }
  | {
      readonly kind: "withdrawn" | "already_terminal";
      readonly actionIntentId: string;
      readonly loopId: string;
      readonly loopState: CoverageState;
      readonly cancelledOpeningEffectCount: number;
      readonly supersededTimerCount: number;
      readonly openingMayHaveEscaped: boolean;
      readonly correction: AcceptedPrivateBridgeCorrection;
    };

interface IntentRow {
  readonly id: string;
  readonly household_id: string;
  readonly person_id: string;
  readonly conversation_id: string;
  readonly participant_epoch_id: string;
  readonly action_digest: string;
  readonly data_digest: string;
  readonly policy_digest: string;
  readonly target_digest: string;
  readonly payload_ciphertext: Buffer;
  readonly status: string;
  readonly person_control_epoch: number | string;
  readonly household_control_epoch: number | string;
  readonly conversation_authority_version: number | string;
  readonly expires_at: Date;
}

interface CandidateRow {
  readonly id: string;
  readonly owner_person_id: string;
  readonly candidate_kind: string;
  readonly content_digest: string;
  readonly content_ciphertext: Buffer;
  readonly evidence_refs: unknown;
  readonly status: string;
  readonly reviewed_by_person_id: string | null;
  readonly expires_at: Date | null;
}

/**
 * The only bridge from person-scoped source evidence to shared coordination.
 * Raw source content is used only to form a proposal. The destination receives
 * the exact, digest-bound minimum meaning approved by the source owner.
 */
export class PrivateSourceBridge {
  public constructor(
    private readonly database: Executor,
    private readonly secretBox: SecretBox,
    private readonly rawRetentionDays = 30,
  ) {}

  public async listDestinations(personId: string): Promise<PrivateBridgeDestinationChoice[]> {
    const rows = await this.database<
      { readonly conversation_id: string; readonly participant_count: number | string }[]
    >`
      select conversation.id as conversation_id, count(participant.person_id) as participant_count
      from conversations conversation
      join household_memberships membership
        on membership.household_id = conversation.household_id
        and membership.person_id = ${personId} and membership.status = 'active'
      join participant_epochs epoch on epoch.id = conversation.current_epoch_id and epoch.ended_at is null
      join epoch_participants participant on participant.participant_epoch_id = epoch.id
      join conversation_channels channel on channel.conversation_id = conversation.id
        and channel.provider = 'linq' and channel.status = 'active'
      where conversation.kind = 'group' and conversation.status = 'active'
      group by conversation.id, conversation.updated_at
      order by conversation.updated_at desc
      limit 25
    `;
    const choices: PrivateBridgeDestinationChoice[] = [];
    for (const row of rows) {
      try {
        const destination = await this.resolveDestination(personId, row.conversation_id);
        const snapshot = await new PostgresConversationAuthority(this.database).snapshot(
          destination.conversationId,
        );
        const names = await this.loadParticipantNames(
          snapshot.participants
            .map((participant) => participant.personId)
            .filter((participantId) => participantId !== personId),
        );
        choices.push({
          conversationId: destination.conversationId,
          label:
            names.length > 0
              ? `With ${names.slice(0, 3).join(", ")}${names.length > 3 ? ` +${names.length - 3}` : ""}`
              : `Family group · ${Number(row.participant_count)} people`,
          participantCount: Number(row.participant_count),
        });
      } catch {
        // A control-plane choice is shown only when it can pass the same exact checks as preparation.
      }
    }
    return choices;
  }

  private async loadParticipantNames(personIds: readonly string[]): Promise<string[]> {
    const exactIds = [...new Set(personIds)];
    if (exactIds.length === 0) return [];
    const rows = await this.database<
      { readonly id: string; readonly display_name_ciphertext: Buffer | null }[]
    >`
      select id, display_name_ciphertext from people
      where id = any(${this.database.array(exactIds)}::uuid[]) and status = 'registered'
    `;
    return rows
      .flatMap((row) => {
        if (!row.display_name_ciphertext) return [];
        try {
          const name = this.secretBox
            .decrypt(
              JSON.parse(row.display_name_ciphertext.toString("utf8")),
              `person-display-name:${row.id}`,
            )
            .toString("utf8")
            .trim();
          return name ? [name] : [];
        } catch {
          return [];
        }
      })
      .sort((left, right) => left.localeCompare(right));
  }

  public async prepare(input: {
    readonly actorPersonId: string;
    readonly candidateId: string;
    readonly conversationId: string;
    readonly standingRule?: z.infer<typeof StandingRuleFenceSchema> | null;
    readonly enqueueProposal?: boolean;
  }): Promise<{ actionIntentId: string; duplicate: boolean }> {
    return inTransaction(this.database, async (transaction) => {
      const now = new Date();
      const discoveredSourceFrontier = await discoverCandidateSourceFrontier(
        transaction,
        input.actorPersonId,
        input.candidateId,
      );
      if (discoveredSourceFrontier) {
        await acquireCandidateSourceLocks(transaction, input.actorPersonId, discoveredSourceFrontier);
      }
      const candidate = await loadCandidateForOwner(
        transaction,
        input.candidateId,
        input.actorPersonId,
        now,
        true,
      );
      if (!["coverage_proposal", "coverage_loop_update_review"].includes(candidate.candidate_kind)) {
        throw new ConflictError("Only a family coverage proposal or update can be shared");
      }
      const personRows = await transaction<{ readonly control_epoch: number | string }[]>`
        select control_epoch from people
        where id = ${input.actorPersonId} and status = 'registered' for share
      `;
      const person = personRows[0];
      if (!person) throw new UnauthorizedError("Only the source owner can prepare a disclosure");
      const destination = await new PrivateSourceBridge(
        transaction,
        this.secretBox,
        this.rawRetentionDays,
      ).resolveDestination(input.actorPersonId, input.conversationId);
      const evidenceIds = stringArray(candidate.evidence_refs).sort();
      const evidenceAuthority = await requireCurrentEvidence(
        transaction,
        input.actorPersonId,
        evidenceIds,
        now,
      );
      const sourceFrontier = evidenceAuthority.integrationId
        ? await requirePreparedCandidateSourceFrontier(
            transaction,
            input.actorPersonId,
            candidate,
            evidenceAuthority.integrationId,
            discoveredSourceFrontier,
          )
        : requireDirectCandidateHasNoSourceFrontier(discoveredSourceFrontier);
      const sourcePattern = await this.deriveSourcePattern(input.actorPersonId, evidenceIds);
      const standingRule = input.standingRule ?? null;
      const loopUpdate =
        candidate.candidate_kind === "coverage_loop_update_review"
          ? await prepareLoopUpdateFence(
              transaction,
              this.secretBox,
              input.actorPersonId,
              candidate,
              destination,
            )
          : null;
      if (loopUpdate && standingRule) {
        throw new ConflictError("An existing coverage loop update always requires one exact approval");
      }
      if (standingRule) {
        if (!sourcePattern) throw new StaleAuthorityError("Standing source pattern is no longer available");
        await requireStandingRule(transaction, input.actorPersonId, standingRule, sourcePattern, destination);
      }
      const payload: z.infer<typeof PreparedPayloadSchema> = {
        schemaVersion: 1,
        phase: "prepared",
        candidateId: candidate.id,
        candidateContentDigest: candidate.content_digest,
        evidenceSourceRevisionIds: evidenceIds,
        evidenceDigest: canonicalDigest(evidenceIds),
        sourcePattern,
        sourceFrontier,
        destination,
        personControlEpoch: Number(person.control_epoch),
        standingRule,
        loopUpdate,
      };
      const preparedActionDigest = canonicalDigest({
        actionKind: "private_source_to_coverage_loop",
        candidateId: candidate.id,
        conversationId: destination.conversationId,
        standingRuleId: standingRule?.ruleId ?? null,
        sourceFrontier,
        loopUpdate,
      });
      await transaction`
        select pg_advisory_xact_lock(hashtextextended(${`private-bridge:${preparedActionDigest}`}, 0))
      `;
      const existing = await transaction<{ readonly id: string }[]>`
        select id from action_intents
        where action_kind = 'private_source_to_coverage_loop'
          and action_digest = ${preparedActionDigest}
          and status in ('proposed', 'awaiting_approval', 'approved', 'executing', 'succeeded')
          and expires_at > ${now}
        order by created_at desc limit 1
      `;
      if (existing[0]) return { actionIntentId: existing[0].id, duplicate: true };

      const actionIntentId = randomUUID();
      const sealed = sealPayload(this.secretBox, actionIntentId, payload);
      const expiresAt = candidate.expires_at ?? new Date(now.getTime() + 7 * 86_400_000);
      await transaction`
        insert into action_intents (
          id, household_id, person_id, conversation_id, participant_epoch_id,
          action_kind, action_digest, data_digest, policy_digest, target_digest,
          payload_ciphertext, payload_key_version, status,
          person_control_epoch, household_control_epoch, conversation_authority_version,
          expires_at, created_at, updated_at
        ) values (
          ${actionIntentId}, ${destination.householdId}, ${input.actorPersonId},
          ${destination.conversationId}, ${destination.participantEpochId},
          'private_source_to_coverage_loop', ${preparedActionDigest},
          ${canonicalDigest({ candidateContentDigest: candidate.content_digest, evidenceIds })},
          ${canonicalDigest({ ownerApprovalRequired: true, standingRule, sourceFrontier, loopUpdate })},
          ${destinationDigest(destination)}, ${sealed.ciphertext}, ${sealed.keyVersion}, 'proposed',
          ${Number(person.control_epoch)}, ${destination.householdControlEpoch},
          ${destination.conversationAuthorityVersion}, ${expiresAt}, ${now}, ${now}
        )
      `;
      if (input.enqueueProposal !== false) {
        await enqueueProposal(
          transaction,
          this.secretBox,
          input.actorPersonId,
          actionIntentId,
          payload,
          expiresAt,
        );
      }
      return { actionIntentId, duplicate: false };
    });
  }

  public async tryPrepareStandingCandidate(
    ownerPersonId: string,
    candidateId: string,
  ): Promise<string | null> {
    const candidates = await this.database<CandidateRow[]>`
      select id, owner_person_id, candidate_kind, content_digest, content_ciphertext,
        evidence_refs, status, reviewed_by_person_id, expires_at
      from knowledge_candidates
      where id = ${candidateId} and owner_person_id = ${ownerPersonId}
        and scope_kind = 'person' and candidate_kind = 'coverage_proposal'
        and status = 'pending' and (expires_at is null or expires_at > now())
    `;
    const candidate = candidates[0];
    if (!candidate) return null;
    const pattern = await this.deriveSourcePattern(ownerPersonId, stringArray(candidate.evidence_refs));
    if (!pattern) return null;
    const scopeDigest = canonicalDigest(pattern);
    const rules = await this.database<
      {
        readonly id: string;
        readonly version: number | string;
        readonly current_revision_id: string;
        readonly destination_conversation_id: string;
      }[]
    >`
      select rule.id, rule.version, rule.current_revision_id, rule.destination_conversation_id
      from bridge_rules rule
      join bridge_rule_revisions revision on revision.id = rule.current_revision_id
      where rule.owner_person_id = ${ownerPersonId} and rule.status = 'active'
        and rule.destination_conversation_id is not null
        and revision.ended_at is null and revision.source_scope_digest = ${scopeDigest}
      order by revision.effective_at desc limit 10
    `;
    for (const rule of rules) {
      try {
        const prepared = await this.prepare({
          actorPersonId: ownerPersonId,
          candidateId,
          conversationId: rule.destination_conversation_id,
          standingRule: {
            ruleId: rule.id,
            ruleRevisionId: rule.current_revision_id,
            ruleVersion: Number(rule.version),
          },
          enqueueProposal: false,
        });
        return prepared.actionIntentId;
      } catch (error) {
        if (
          !(
            error instanceof StaleAuthorityError ||
            error instanceof UnauthorizedError ||
            error instanceof NotFoundError ||
            error instanceof ConflictError
          )
        ) {
          throw error;
        }
      }
    }
    return null;
  }

  public async loadProposalContext(actionIntentId: string): Promise<PrivateBridgeProposalContext> {
    const rows = await this.database<IntentRow[]>`
      select id, household_id, person_id, conversation_id, participant_epoch_id,
        action_digest, data_digest, policy_digest, target_digest, payload_ciphertext,
        status, person_control_epoch, household_control_epoch,
        conversation_authority_version, expires_at
      from action_intents where id = ${actionIntentId}
        and action_kind = 'private_source_to_coverage_loop'
    `;
    const intent = rows[0];
    if (intent?.status !== "proposed" || intent.expires_at <= new Date()) {
      throw new StaleAuthorityError("Private bridge proposal is no longer current");
    }
    const payload = openPrivateBridgePayload(this.secretBox, intent.id, intent.payload_ciphertext);
    if (payload.phase !== "prepared") throw new ConflictError("Private bridge was already proposed");
    try {
      await this.revalidate(intent, payload, false);
    } catch (error) {
      if (
        error instanceof StaleAuthorityError ||
        error instanceof UnauthorizedError ||
        error instanceof NotFoundError ||
        error instanceof ConflictError
      ) {
        await this.cancelPendingProposal(intent.id);
      }
      throw error;
    }
    const candidates = await this.database<CandidateRow[]>`
      select id, owner_person_id, candidate_kind, content_digest, content_ciphertext,
        evidence_refs, status, reviewed_by_person_id, expires_at
      from knowledge_candidates where id = ${payload.candidateId}
    `;
    const candidate = candidates[0];
    if (!candidate) throw new NotFoundError("Private source proposal disappeared");
    const content = openCandidate(this.secretBox, candidate);
    const snapshot = await new PostgresConversationAuthority(this.database).snapshot(
      payload.destination.conversationId,
    );
    return {
      actionIntentId: intent.id,
      ownerPersonId: intent.person_id,
      candidate: content,
      evidenceSourceRevisionIds: payload.evidenceSourceRevisionIds,
      destination: payload.destination,
      currentParticipantPersonIds: snapshot.participants.map((participant) => participant.personId),
      standingRule: payload.standingRule,
      loopUpdate: payload.loopUpdate,
    };
  }

  public async recordProposal(
    input: PrivateBridgeProposalInput,
  ): Promise<{ status: "awaiting_approval" | "approved"; actionIntentId: string }> {
    const minimumDisclosure = minimumDisclosureSchema.parse(input.minimumDisclosure);
    const commitment = commitmentProposalSchema.parse(input.commitment);
    return inTransaction(this.database, async (transaction) => {
      const observedRows = await transaction<IntentRow[]>`
        select id, household_id, person_id, conversation_id, participant_epoch_id,
          action_digest, data_digest, policy_digest, target_digest, payload_ciphertext,
          status, person_control_epoch, household_control_epoch,
          conversation_authority_version, expires_at
        from action_intents where id = ${input.actionIntentId}
          and action_kind = 'private_source_to_coverage_loop'
      `;
      const observedIntent = observedRows[0];
      if (!observedIntent) throw new NotFoundError("Private bridge intent does not exist");
      const observedPayload = openPrivateBridgePayload(
        this.secretBox,
        observedIntent.id,
        observedIntent.payload_ciphertext,
      );
      if (observedPayload.sourceFrontier) {
        await acquireCandidateSourceLocks(
          transaction,
          observedIntent.person_id,
          observedPayload.sourceFrontier,
        );
      }
      const rows = await transaction<IntentRow[]>`
        select id, household_id, person_id, conversation_id, participant_epoch_id,
          action_digest, data_digest, policy_digest, target_digest, payload_ciphertext,
          status, person_control_epoch, household_control_epoch,
          conversation_authority_version, expires_at
        from action_intents where id = ${input.actionIntentId}
          and action_kind = 'private_source_to_coverage_loop' for update
      `;
      const intent = rows[0];
      if (!intent) throw new NotFoundError("Private bridge intent does not exist");
      const prepared = openPrivateBridgePayload(this.secretBox, intent.id, intent.payload_ciphertext);
      requireSameObservedSourceFrontier(observedPayload, prepared);
      if (intent.status !== "proposed") {
        if (["awaiting_approval", "approved", "executing", "succeeded"].includes(intent.status)) {
          return {
            status: intent.status === "awaiting_approval" ? "awaiting_approval" : "approved",
            actionIntentId: intent.id,
          };
        }
        throw new StaleAuthorityError("Private bridge intent is no longer proposable");
      }
      if (prepared.phase !== "prepared") throw new ConflictError("Private bridge proposal phase changed");
      await new PrivateSourceBridge(transaction, this.secretBox, this.rawRetentionDays).revalidate(
        intent,
        prepared,
        false,
        prepared.loopUpdate !== null,
      );
      requireExactProposalEvidence(prepared, minimumDisclosure.evidence, commitment.evidence);
      if (minimumDisclosure.destinationEpochId !== prepared.destination.participantEpochId) {
        throw new StaleAuthorityError("Minimum disclosure targeted a different group epoch");
      }
      if (!minimumDisclosure.sourceOwnerApprovalRequired) {
        throw new ConflictError("Private source disclosure must require its owner's approval");
      }
      const proposed: z.infer<typeof ProposedPayloadSchema> = {
        ...prepared,
        phase: "awaiting_approval",
        loopId: prepared.loopUpdate?.existingLoopId ?? randomUUID(),
        minimumDisclosure,
        commitment,
        approvalMode: prepared.standingRule ? "standing" : null,
      };
      const digests = proposalDigests(intent.id, proposed);
      const sealed = sealPayload(this.secretBox, intent.id, proposed);
      const standingRule = prepared.standingRule;
      const autoApproved = standingRule !== null && prepared.loopUpdate === null;
      if (standingRule && prepared.sourcePattern) {
        await requireStandingRule(
          transaction,
          intent.person_id,
          standingRule,
          prepared.sourcePattern,
          prepared.destination,
        );
      }
      await transaction`
        update action_intents set action_digest = ${digests.actionDigest},
          data_digest = ${digests.dataDigest}, policy_digest = ${digests.policyDigest},
          target_digest = ${digests.targetDigest}, payload_ciphertext = ${sealed.ciphertext},
          payload_key_version = ${sealed.keyVersion},
          status = ${autoApproved ? "approved" : "awaiting_approval"}, updated_at = now()
        where id = ${intent.id}
      `;
      if (autoApproved) {
        await insertApproval(transaction, intent.person_id, intent.id, digests, intent.expires_at);
        await enqueueCommit(transaction, this.secretBox, intent);
      }
      return {
        status: autoApproved ? "approved" : "awaiting_approval",
        actionIntentId: intent.id,
      };
    });
  }

  public async cancelPendingProposal(actionIntentId: string): Promise<boolean> {
    const rows = await this.database<{ readonly id: string }[]>`
      update action_intents set status = 'cancelled', updated_at = now()
      where id = ${actionIntentId} and action_kind = 'private_source_to_coverage_loop'
        and status = 'proposed'
      returning id
    `;
    return rows.length === 1;
  }

  /**
   * Withdraws only bridge work whose encrypted payload is bound to this exact
   * candidate and content digest. Construct this bridge with the caller's
   * transaction to make withdrawal atomic with candidate replacement.
   */
  public async cancelPendingCandidateWork(input: {
    readonly ownerPersonId: string;
    readonly candidateId: string;
    readonly candidateContentDigest: string;
    readonly cancelledAt: Date;
  }): Promise<{ readonly cancelledActionIntentIds: readonly string[] }> {
    return inTransaction(this.database, async (transaction) => {
      const intents = await transaction<
        { readonly id: string; readonly payload_ciphertext: Buffer; readonly status: string }[]
      >`
        select id, payload_ciphertext, status
        from action_intents
        where person_id = ${input.ownerPersonId}
          and action_kind = 'private_source_to_coverage_loop'
          and status in ('proposed', 'awaiting_approval', 'approved')
        for update
      `;
      const exactIntentIds: string[] = [];
      for (const intent of intents) {
        let payload: PrivateBridgePayload;
        try {
          payload = openPrivateBridgePayload(this.secretBox, intent.id, intent.payload_ciphertext);
        } catch {
          continue;
        }
        if (
          payload.candidateId === input.candidateId &&
          payload.candidateContentDigest === input.candidateContentDigest
        ) {
          exactIntentIds.push(intent.id);
        }
      }
      if (exactIntentIds.length === 0) return { cancelledActionIntentIds: [] };

      await transaction`
        update action_approvals
        set revoked_at = coalesce(revoked_at, ${input.cancelledAt})
        where action_intent_id = any(${transaction.array(exactIntentIds)}::uuid[])
      `;
      await transaction`
        update disclosure_decisions decision
        set revoked_at = coalesce(decision.revoked_at, ${input.cancelledAt})
        from outbox effect
        where effect.action_intent_id = any(${transaction.array(exactIntentIds)}::uuid[])
          and effect.authorization_decision_id = decision.id
          and effect.status in ('pending', 'retry', 'leased')
      `;
      await transaction`
        update outbox
        set status = 'cancelled', lease_owner = null, lease_token = null,
          lease_expires_at = null, updated_at = ${input.cancelledAt}
        where action_intent_id = any(${transaction.array(exactIntentIds)}::uuid[])
          and status in ('pending', 'retry', 'leased')
      `;
      await transaction`
        update jobs
        set status = 'cancelled', lease_owner = null, lease_token = null,
          lease_expires_at = null, updated_at = ${input.cancelledAt}
        where idempotency_key = any(${transaction.array(
          exactIntentIds.flatMap((id) => [`private-bridge:proposal:${id}`, `private-bridge:commit:${id}`]),
        )}::text[])
          and status in ('pending', 'retry', 'leased')
      `;
      await transaction`
        update action_intents
        set status = 'cancelled', updated_at = ${input.cancelledAt}
        where id = any(${transaction.array(exactIntentIds)}::uuid[])
          and status in ('proposed', 'awaiting_approval', 'approved')
      `;
      return { cancelledActionIntentIds: exactIntentIds };
    });
  }

  /** Resolves only the canonical loop already created from this exact accepted candidate. */
  public async resolveAcceptedCandidateLoop(
    inputCandidate: AcceptedPrivateBridgeCandidateInput,
  ): Promise<AcceptedPrivateBridgeLoopReference | null> {
    const input = parseAcceptedCandidateInput(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      const exact = await resolveExactAcceptedBridge(transaction, this.secretBox, input, false);
      return exact ? { actionIntentId: exact.intent.id, loopId: exact.loop.loopId } : null;
    });
  }

  /**
   * Withdraws the one shared loop created from an exact accepted private candidate.
   * The caller owns the surrounding application transaction; this module keeps the
   * encrypted bridge lookup, opening-effect race, loop transition, timer cleanup,
   * and optional minimum-meaning correction behind one interface.
   */
  public async withdrawAcceptedCandidate(
    inputCandidate: AcceptedPrivateBridgeWithdrawalInput,
  ): Promise<AcceptedPrivateBridgeWithdrawalResult> {
    const input = parseAcceptedWithdrawalInput(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      await acquireCandidateSourceLocks(transaction, input.ownerPersonId, input.replacementSourceFrontier);
      const exact = await resolveExactAcceptedBridge(transaction, this.secretBox, input, true);
      if (!exact) return { kind: "not_found" };

      const openingKey = `private-bridge:${exact.intent.id}:open`;
      const openingEffects = await transaction<
        {
          readonly id: string;
          readonly authorization_decision_id: string;
          readonly status: string;
        }[]
      >`
        select effect.id, effect.authorization_decision_id, effect.status
        from outbox effect
        where effect.action_intent_id = ${exact.intent.id}
          and effect.effect_kind = 'linq.message'
          and (
            effect.idempotency_key = ${openingKey}
            or exists (
              select 1 from outbox root
              where root.id = effect.redrive_root_id and root.idempotency_key = ${openingKey}
            )
          )
        for update of effect
      `;
      const openingMayHaveEscaped = openingEffects.some((effect) =>
        ["leased", "retry", "submitted", "confirmed", "ambiguous"].includes(effect.status),
      );
      const openingIds = openingEffects.map((effect) => effect.id);
      const decisionIds = openingEffects.map((effect) => effect.authorization_decision_id);
      let cancelledOpeningEffectCount = 0;
      if (openingIds.length > 0) {
        const cancelled = await transaction<{ readonly id: string }[]>`
          update outbox
          set status = 'cancelled', lease_owner = null, lease_token = null,
            lease_expires_at = null, last_error_code = 'private_bridge_withdrawn',
            updated_at = ${input.withdrawnAt}
          where id = any(${transaction.array(openingIds)}::uuid[])
            and status in ('pending', 'retry', 'leased')
          returning id
        `;
        cancelledOpeningEffectCount = cancelled.length;
      }
      if (decisionIds.length > 0) {
        await transaction`
          update disclosure_decisions
          set revoked_at = coalesce(revoked_at, ${input.withdrawnAt})
          where id = any(${transaction.array(decisionIds)}::uuid[])
        `;
      }
      await transaction`
        update action_approvals
        set revoked_at = coalesce(revoked_at, ${input.withdrawnAt})
        where action_intent_id = ${exact.intent.id}
      `;

      const coordination = new PostgresCoordination(transaction, this.secretBox);
      const loop = exact.loop;
      const wasLive = isLiveCoverageState(loop.state);
      const resolvedLoop = wasLive
        ? (
            await coordination.transition({
              loopId: loop.loopId,
              command: {
                kind: input.intent,
                transitionId: randomUUID(),
                expectedVersion: loop.version,
                actorPersonId: null,
                occurredAt: input.withdrawnAt.toISOString(),
                evidenceRefs: [...input.evidenceSourceRevisionIds],
              },
            })
          ).loop
        : loop;
      const supersededTimerCount = await new DurableTimers(transaction).supersedeCoverageTimers(
        resolvedLoop.loopId,
        wasLive ? resolvedLoop.version : resolvedLoop.version + 1,
      );
      const correction =
        wasLive && openingMayHaveEscaped
          ? await new PrivateSourceBridge(
              transaction,
              this.secretBox,
              this.rawRetentionDays,
            ).queueAcceptedWithdrawalCorrection({
              ownerPersonId: input.ownerPersonId,
              actionIntentId: exact.intent.id,
              payload: exact.payload,
              loop: resolvedLoop,
              intent: input.intent,
              replacementSourceFrontier: input.replacementSourceFrontier,
            })
          : ({ kind: "not_needed" } as const);

      await appendBridgeAudit(transaction, {
        personId: input.ownerPersonId,
        householdId: exact.intent.household_id,
        conversationId: exact.intent.conversation_id,
        targetId: exact.intent.id,
        eventType: "accepted_private_source_bridge_withdrawn",
        reasons: [input.intent, `correction_${correction.kind}`],
        manifest: {
          candidateId: input.candidateId,
          loopId: resolvedLoop.loopId,
          loopState: resolvedLoop.state,
          openingMayHaveEscaped,
          cancelledOpeningEffectCount,
          supersededTimerCount,
          rawContentDisclosed: false,
        },
      });
      return {
        kind: wasLive ? "withdrawn" : "already_terminal",
        actionIntentId: exact.intent.id,
        loopId: resolvedLoop.loopId,
        loopState: resolvedLoop.state,
        cancelledOpeningEffectCount,
        supersededTimerCount,
        openingMayHaveEscaped,
        correction,
      };
    });
  }

  public async approve(
    input: PrivateBridgeApprovalInput,
  ): Promise<{ actionIntentId: string; bridgeRuleId: string | null; duplicate: boolean }> {
    const supplied = {
      actionDigest: DigestSchema.parse(input.actionDigest),
      dataDigest: DigestSchema.parse(input.dataDigest),
      policyDigest: DigestSchema.parse(input.policyDigest),
      targetDigest: DigestSchema.parse(input.targetDigest),
    };
    return inTransaction(this.database, async (transaction) => {
      const observedRows = await transaction<IntentRow[]>`
        select id, household_id, person_id, conversation_id, participant_epoch_id,
          action_digest, data_digest, policy_digest, target_digest, payload_ciphertext,
          status, person_control_epoch, household_control_epoch,
          conversation_authority_version, expires_at
        from action_intents where id = ${input.actionIntentId}
          and action_kind = 'private_source_to_coverage_loop'
      `;
      const observedIntent = observedRows[0];
      if (!observedIntent || observedIntent.person_id !== input.actorPersonId) {
        throw new UnauthorizedError("Only the private source owner can approve sharing");
      }
      const observedPayload = openPrivateBridgePayload(
        this.secretBox,
        observedIntent.id,
        observedIntent.payload_ciphertext,
      );
      if (observedPayload.sourceFrontier) {
        await acquireCandidateSourceLocks(
          transaction,
          observedIntent.person_id,
          observedPayload.sourceFrontier,
        );
      }
      const rows = await transaction<IntentRow[]>`
        select id, household_id, person_id, conversation_id, participant_epoch_id,
          action_digest, data_digest, policy_digest, target_digest, payload_ciphertext,
          status, person_control_epoch, household_control_epoch,
          conversation_authority_version, expires_at
        from action_intents where id = ${input.actionIntentId}
          and action_kind = 'private_source_to_coverage_loop' for update
      `;
      const intent = rows[0];
      if (!intent || intent.person_id !== input.actorPersonId) {
        throw new UnauthorizedError("Only the private source owner can approve sharing");
      }
      const payload = openPrivateBridgePayload(this.secretBox, intent.id, intent.payload_ciphertext);
      requireSameObservedSourceFrontier(observedPayload, payload);
      if (payload.phase !== "awaiting_approval") throw new ConflictError("Sharing proposal is not ready");
      if (["approved", "executing", "succeeded"].includes(intent.status)) {
        if (
          payload.approvalMode !== input.mode ||
          intent.action_digest !== supplied.actionDigest ||
          intent.data_digest !== supplied.dataDigest ||
          intent.target_digest !== supplied.targetDigest
        ) {
          throw new StaleAuthorityError("Approval no longer matches the exact proposed action");
        }
        return { actionIntentId: intent.id, bridgeRuleId: null, duplicate: true };
      }
      requireMatchingDigests(intent, supplied);
      if (intent.status !== "awaiting_approval" || intent.expires_at <= new Date()) {
        throw new StaleAuthorityError("Sharing proposal is no longer awaiting approval");
      }
      await new PrivateSourceBridge(transaction, this.secretBox, this.rawRetentionDays).revalidate(
        intent,
        payload,
        false,
        payload.loopUpdate !== null,
      );
      if (payload.loopUpdate && input.mode !== "once") {
        throw new ConflictError("An existing coverage loop update requires one exact approval");
      }
      if (input.mode === "standing" && payload.sourcePattern === null) {
        throw new ConflictError("This source cannot support a narrow standing rule");
      }
      const approvedPayload: z.infer<typeof ProposedPayloadSchema> = {
        ...payload,
        approvalMode: input.mode,
      };
      const approvedDigests = proposalDigests(intent.id, approvedPayload);
      const sealed = sealPayload(this.secretBox, intent.id, approvedPayload);
      await transaction`
        update action_intents set policy_digest = ${approvedDigests.policyDigest},
          payload_ciphertext = ${sealed.ciphertext}, payload_key_version = ${sealed.keyVersion},
          status = 'approved', updated_at = now()
        where id = ${intent.id}
      `;
      await insertApproval(transaction, input.actorPersonId, intent.id, approvedDigests, intent.expires_at);
      const bridgeRuleId =
        input.mode === "standing" && approvedPayload.sourcePattern
          ? await createStandingRule(transaction, input.actorPersonId, intent.id, approvedPayload)
          : null;
      await enqueueCommit(transaction, this.secretBox, intent);
      return { actionIntentId: intent.id, bridgeRuleId, duplicate: false };
    });
  }

  public async commit(
    actionIntentId: string,
  ): Promise<{ loopId: string; duplicate: boolean; cancelled: boolean }> {
    return inTransaction(this.database, async (transaction) => {
      const observedRows = await transaction<IntentRow[]>`
        select id, household_id, person_id, conversation_id, participant_epoch_id,
          action_digest, data_digest, policy_digest, target_digest, payload_ciphertext,
          status, person_control_epoch, household_control_epoch,
          conversation_authority_version, expires_at
        from action_intents where id = ${actionIntentId}
          and action_kind = 'private_source_to_coverage_loop'
      `;
      const observedIntent = observedRows[0];
      if (!observedIntent) throw new NotFoundError("Private bridge intent does not exist");
      const observedPayload = openPrivateBridgePayload(
        this.secretBox,
        observedIntent.id,
        observedIntent.payload_ciphertext,
      );
      if (observedPayload.sourceFrontier) {
        await acquireCandidateSourceLocks(
          transaction,
          observedIntent.person_id,
          observedPayload.sourceFrontier,
        );
      }
      const rows = await transaction<IntentRow[]>`
        select id, household_id, person_id, conversation_id, participant_epoch_id,
          action_digest, data_digest, policy_digest, target_digest, payload_ciphertext,
          status, person_control_epoch, household_control_epoch,
          conversation_authority_version, expires_at
        from action_intents where id = ${actionIntentId}
          and action_kind = 'private_source_to_coverage_loop' for update
      `;
      const intent = rows[0];
      if (!intent) throw new NotFoundError("Private bridge intent does not exist");
      const payload = openPrivateBridgePayload(this.secretBox, intent.id, intent.payload_ciphertext);
      requireSameObservedSourceFrontier(observedPayload, payload);
      if (payload.phase !== "awaiting_approval")
        throw new ConflictError("Private bridge has no approved meaning");
      if (intent.status === "succeeded") {
        return { loopId: payload.loopId, duplicate: true, cancelled: false };
      }
      if (intent.status !== "approved" || intent.expires_at <= new Date()) {
        throw new StaleAuthorityError("Private bridge approval is no longer executable");
      }
      const approvals = await transaction<
        {
          readonly action_digest: string;
          readonly data_digest: string;
          readonly policy_digest: string;
          readonly target_digest: string;
        }[]
      >`
        select action_digest, data_digest, policy_digest, target_digest
        from action_approvals
        where action_intent_id = ${intent.id} and approved_by_person_id = ${intent.person_id}
          and revoked_at is null and expires_at > now()
      `;
      const approval = approvals[0];
      if (!approval) {
        await transaction`
          update action_intents set status = 'cancelled', updated_at = now() where id = ${intent.id}
        `;
        await appendBridgeAudit(transaction, {
          personId: intent.person_id,
          householdId: intent.household_id,
          conversationId: intent.conversation_id,
          targetId: intent.id,
          eventType: "private_source_disclosure_cancelled_stale",
          reasons: ["exact_approval_missing_or_expired"],
          manifest: { candidateId: payload.candidateId, rawContentDisclosed: false },
        });
        return { loopId: payload.loopId, duplicate: false, cancelled: true };
      }
      requireMatchingDigests(intent, {
        actionDigest: approval.action_digest,
        dataDigest: approval.data_digest,
        policyDigest: approval.policy_digest,
        targetDigest: approval.target_digest,
      });
      let current: {
        destination: z.infer<typeof DestinationSchema>;
        updateLoop: CoverageLoop | null;
      };
      try {
        current = await new PrivateSourceBridge(
          transaction,
          this.secretBox,
          this.rawRetentionDays,
        ).revalidate(intent, payload, true, true);
        const authorization = await new PostgresConversationAuthority(transaction).authorizeSend({
          conversationId: payload.destination.conversationId,
          expectedParticipantEpochId: payload.destination.participantEpochId,
          expectedParticipantSetDigest: payload.destination.participantSetDigest,
          liveParticipantIdentityIds: payload.destination.liveIdentityIds,
          sendKind: "proactive",
          operation: "proactive_coverage",
          ruleId: payload.destination.proactiveRuleId,
        });
        if (!authorization.allowed) throw new StaleAuthorityError("Group write authority changed");
      } catch (error) {
        if (
          !(
            error instanceof StaleAuthorityError ||
            error instanceof UnauthorizedError ||
            error instanceof NotFoundError ||
            error instanceof ConflictError
          )
        ) {
          throw error;
        }
        await transaction`
          update action_intents set status = 'cancelled', updated_at = now() where id = ${intent.id}
        `;
        await transaction`
          update action_approvals set revoked_at = coalesce(revoked_at, now())
          where action_intent_id = ${intent.id}
        `;
        await appendBridgeAudit(transaction, {
          personId: intent.person_id,
          householdId: intent.household_id,
          conversationId: intent.conversation_id,
          targetId: intent.id,
          eventType: "private_source_disclosure_cancelled_stale",
          reasons: ["commit_revalidation_failed", error.name],
          manifest: { candidateId: payload.candidateId, rawContentDisclosed: false },
        });
        return { loopId: payload.loopId, duplicate: false, cancelled: true };
      }

      const coordination = new PostgresCoordination(transaction, this.secretBox);
      const committedAt = new Date();
      let loop: CoverageLoop;
      if (payload.loopUpdate) {
        const updateLoop = current.updateLoop;
        if (!updateLoop) throw new StaleAuthorityError("Coverage update lost its exact live loop");
        loop = (
          await coordination.transition({
            loopId: updateLoop.loopId,
            command: {
              kind: "revise",
              transitionId: randomUUID(),
              expectedVersion: payload.loopUpdate.expectedLoopVersion,
              actorPersonId: intent.person_id,
              occurredAt: committedAt.toISOString(),
              minimumSharedMeaning: payload.minimumDisclosure.minimumMeaning,
              timing: resolveTiming(payload.commitment, payload.destination.timeZone),
              evidenceRefs: payload.evidenceSourceRevisionIds,
            },
          })
        ).loop;
        await new DurableTimers(transaction).supersedeCoverageTimers(loop.loopId, loop.version);
      } else {
        const existing = await coordination.load(payload.loopId);
        loop =
          existing ??
          (await coordination.create(
            createCoverageLoop({
              loopId: payload.loopId,
              householdId: payload.destination.householdId,
              minimumSharedMeaning: payload.minimumDisclosure.minimumMeaning,
              // The owner approved an actionable minimum meaning. Timing uncertainty remains
              // in the private intent; it must not block opening or monitoring the coverage loop.
              unresolvedFacts: [],
              proposedHolderPersonId: null,
              timing: resolveTiming(payload.commitment, payload.destination.timeZone),
              planVersion: 1,
              notificationMode: "always",
              destination: {
                conversationId: payload.destination.conversationId,
                participantEpochId: payload.destination.participantEpochId,
                participantSetDigest: payload.destination.participantSetDigest,
                audience: "group",
              },
              sourceEvidenceRefs: payload.evidenceSourceRevisionIds,
              occurredAt: committedAt.toISOString(),
            }),
          ));
      }
      const opening = openingEffectPlan(intent.id, payload);
      await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue({
        actionIntentId: intent.id,
        actorPersonId: intent.person_id,
        person: { id: intent.person_id, controlEpoch: Number(intent.person_control_epoch) },
        ...(payload.sourceFrontier ? { sourceFrontier: payload.sourceFrontier } : {}),
        ...(payload.sourceFrontier
          ? {
              integration: {
                id: payload.sourceFrontier.integrationId,
                controlEpoch: payload.sourceFrontier.integrationControlEpoch,
              },
            }
          : {}),
        household: { id: intent.household_id, controlEpoch: Number(intent.household_control_epoch) },
        conversation: {
          id: intent.conversation_id,
          authorityVersion: Number(intent.conversation_authority_version),
        },
        coverageLoop: { id: loop.loopId, version: loop.version },
        participantEpochId: intent.participant_epoch_id,
        expectedParticipantDigest: payload.destination.participantSetDigest,
        ...opening,
        reasonCodes: ["exact_source_owner_approval", "minimum_disclosure", "current_group_epoch"],
        authorizationExpiresAt: openingAuthorizationExpiry(intent, loop),
      });
      const followUp = planCoverageFollowUpTimer({
        loop,
        now: committedAt.toISOString(),
        remindersAuthorized: true,
      });
      if (followUp) {
        await new DurableTimers(transaction).scheduleCoverage({
          timer: followUp,
          household: {
            id: payload.destination.householdId,
            controlEpoch: payload.destination.householdControlEpoch,
          },
          conversation: {
            id: payload.destination.conversationId,
            authorityVersion: payload.destination.conversationAuthorityVersion,
          },
        });
      }
      await transaction`
        update action_intents set status = 'succeeded', updated_at = now() where id = ${intent.id}
      `;
      await transaction`
        update knowledge_candidates set status = 'accepted',
          reviewed_by_person_id = ${intent.person_id}, reviewed_at = now()
        where id = ${payload.candidateId} and owner_person_id = ${intent.person_id}
          and status = 'pending'
      `;
      await appendBridgeAudit(transaction, {
        personId: intent.person_id,
        householdId: intent.household_id,
        conversationId: intent.conversation_id,
        targetId: intent.id,
        eventType: "private_source_minimum_disclosure_committed",
        reasons: ["exact_owner_approval", "exact_current_epoch", "raw_content_not_disclosed"],
        manifest: {
          loopId: loop.loopId,
          operation: payload.loopUpdate ? "coverage_loop_revised" : "coverage_loop_created",
          priorLoopVersion: payload.loopUpdate?.expectedLoopVersion ?? null,
          committedLoopVersion: loop.version,
          sourceRevisionCount: payload.evidenceSourceRevisionIds.length,
          destinationEpochId: payload.destination.participantEpochId,
          standingRuleId: payload.standingRule?.ruleId ?? null,
          currentAuthorityVersion: current.destination.conversationAuthorityVersion,
        },
      });
      return { loopId: loop.loopId, duplicate: false, cancelled: false };
    });
  }

  private async queueAcceptedWithdrawalCorrection(input: {
    readonly ownerPersonId: string;
    readonly actionIntentId: string;
    readonly payload: z.infer<typeof ProposedPayloadSchema>;
    readonly loop: CoverageLoop;
    readonly intent: "cancel" | "supersede";
    readonly replacementSourceFrontier: PrivateBridgeSourceFrontier;
  }): Promise<AcceptedPrivateBridgeCorrection> {
    try {
      if (
        !input.payload.sourceFrontier ||
        !sameSourceFrontierLockTarget(input.payload.sourceFrontier, input.replacementSourceFrontier)
      ) {
        return { kind: "not_authorized" };
      }
      await requireExactCurrentSourceFrontier(
        this.database,
        input.ownerPersonId,
        input.replacementSourceFrontier,
        false,
      );
      const destination = await this.resolveDestination(
        input.ownerPersonId,
        input.loop.destination.conversationId,
      );
      if (
        destination.householdId !== input.loop.householdId ||
        destination.conversationId !== input.loop.destination.conversationId ||
        destination.participantEpochId !== input.loop.destination.participantEpochId ||
        destination.participantSetDigest !== input.loop.destination.participantSetDigest ||
        destination.providerChatId !== input.payload.destination.providerChatId
      ) {
        return { kind: "not_authorized" };
      }
      const authorization = await new PostgresConversationAuthority(this.database).authorizeSend({
        conversationId: destination.conversationId,
        expectedParticipantEpochId: destination.participantEpochId,
        expectedParticipantSetDigest: destination.participantSetDigest,
        liveParticipantIdentityIds: destination.liveIdentityIds,
        sendKind: "proactive",
        operation: "coverage_closure",
        ruleId: destination.proactiveRuleId,
      });
      if (!authorization.allowed) return { kind: "not_authorized" };
      const people = await this.database<{ readonly control_epoch: number | string }[]>`
        select control_epoch from people
        where id = ${input.ownerPersonId} and status = 'registered'
      `;
      const person = people[0];
      if (!person) return { kind: "not_authorized" };
      const now = new Date();
      const queued = await new EffectOutbox(this.database, this.secretBox).authorizeAndEnqueue(
        {
          actorPersonId: input.ownerPersonId,
          person: { id: input.ownerPersonId, controlEpoch: Number(person.control_epoch) },
          sourceFrontier: input.replacementSourceFrontier,
          integration: {
            id: input.replacementSourceFrontier.integrationId,
            controlEpoch: input.replacementSourceFrontier.integrationControlEpoch,
          },
          household: {
            id: destination.householdId,
            controlEpoch: destination.householdControlEpoch,
          },
          conversation: {
            id: destination.conversationId,
            authorityVersion: destination.conversationAuthorityVersion,
          },
          participantEpochId: destination.participantEpochId,
          expectedParticipantDigest: destination.participantSetDigest,
          coverageLoop: { id: input.loop.loopId, version: input.loop.version },
          effectKind: "linq.message",
          idempotencyKey: `private-bridge:${input.actionIntentId}:loop-v${input.loop.version}:${input.intent}:correction`,
          data: {
            coverageLoopId: input.loop.loopId,
            loopVersion: input.loop.version,
            minimumSharedMeaning: input.loop.minimumSharedMeaning,
          },
          policy: {
            operation: "coverage_closure",
            sendKind: "proactive",
            exactParticipantEpoch: true,
            minimumMeaningOnly: true,
          },
          target: {
            providerChatId: destination.providerChatId,
            participantEpochId: destination.participantEpochId,
          },
          payload: {
            providerChatId: destination.providerChatId,
            expectedProviderParticipantDigest: destination.providerParticipantDigest,
            text: neutralAcceptedWithdrawalText(input.loop.minimumSharedMeaning, input.intent),
          },
          reasonCodes: ["current_conversation_authority", "neutral_coverage_closure"],
          authorizationExpiresAt: new Date(now.getTime() + 5 * 60_000),
        },
        now,
      );
      return { kind: "queued", outboxId: queued.outboxId, duplicate: !queued.created };
    } catch (error) {
      if (
        error instanceof StaleAuthorityError ||
        error instanceof UnauthorizedError ||
        error instanceof NotFoundError ||
        error instanceof ConflictError
      ) {
        return { kind: "not_authorized" };
      }
      throw error;
    }
  }

  private async revalidate(
    intent: IntentRow,
    payload: PrivateBridgePayload,
    requireApprovedCandidate: boolean,
    lockUpdateLoop = false,
  ): Promise<{
    destination: z.infer<typeof DestinationSchema>;
    updateLoop: CoverageLoop | null;
  }> {
    if (intent.expires_at <= new Date()) throw new StaleAuthorityError("Private bridge expired");
    const destination = await this.resolveDestination(intent.person_id, intent.conversation_id);
    if (
      Number(intent.person_control_epoch) !== payload.personControlEpoch ||
      Number(intent.household_control_epoch) !== destination.householdControlEpoch ||
      Number(intent.conversation_authority_version) !== destination.conversationAuthorityVersion ||
      canonicalDigest(destination) !== canonicalDigest(payload.destination)
    ) {
      throw new StaleAuthorityError("Private bridge authority changed");
    }
    const candidate = await loadCandidateForOwner(
      this.database,
      payload.candidateId,
      intent.person_id,
      new Date(),
      !requireApprovedCandidate,
    );
    if (
      candidate.content_digest !== payload.candidateContentDigest ||
      canonicalDigest(stringArray(candidate.evidence_refs).sort()) !== payload.evidenceDigest
    ) {
      throw new StaleAuthorityError("Private bridge source candidate changed");
    }
    if (requireApprovedCandidate && !["pending", "accepted"].includes(candidate.status)) {
      throw new StaleAuthorityError("Private bridge source candidate was withdrawn");
    }
    const evidenceAuthority = await requireCurrentEvidence(
      this.database,
      intent.person_id,
      payload.evidenceSourceRevisionIds,
      new Date(),
    );
    await requireCurrentPayloadSourceFrontier(
      this.database,
      intent.person_id,
      candidate,
      payload.sourceFrontier,
      evidenceAuthority.integrationId,
      lockUpdateLoop,
    );
    if (payload.standingRule && payload.sourcePattern) {
      await requireStandingRule(
        this.database,
        intent.person_id,
        payload.standingRule,
        payload.sourcePattern,
        destination,
      );
    }
    if (payload.loopUpdate && payload.standingRule) {
      throw new StaleAuthorityError("A coverage-loop update cannot use standing approval");
    }
    const updateLoop = payload.loopUpdate
      ? await requireCurrentLoopUpdate(
          this.database,
          this.secretBox,
          intent,
          payload,
          candidate,
          lockUpdateLoop,
        )
      : null;
    return { destination, updateLoop };
  }

  private async resolveDestination(
    personId: string,
    conversationId: string,
  ): Promise<z.infer<typeof DestinationSchema>> {
    const rows = await this.database<
      {
        readonly household_id: string;
        readonly household_control_epoch: number | string;
        readonly timezone: string | null;
        readonly external_channel_id: string;
      }[]
    >`
      select household.id as household_id, household.control_epoch as household_control_epoch,
        household.timezone, channel.external_channel_id
      from conversations conversation
      join households household on household.id = conversation.household_id
        and household.status in ('onboarding', 'active')
      join household_memberships membership on membership.household_id = household.id
        and membership.person_id = ${personId} and membership.status = 'active'
      join conversation_channels channel on channel.conversation_id = conversation.id
        and channel.provider = 'linq' and channel.status = 'active'
      where conversation.id = ${conversationId} and conversation.kind = 'group'
        and conversation.status = 'active'
      limit 1
    `;
    const row = rows[0];
    if (!row) throw new UnauthorizedError("Destination is not a current family group");
    const snapshot = await new PostgresConversationAuthority(this.database).snapshot(conversationId);
    if (
      evaluateConversationMode(snapshot) !== "trusted_write_enabled" ||
      !snapshot.participantEpochId ||
      !snapshot.participantSetDigest ||
      !snapshot.participants.some((participant) => participant.personId === personId) ||
      snapshot.participants.some((participant) => participant.policy?.allowProactiveWrites !== true)
    ) {
      throw new UnauthorizedError("Every current registered group member must allow proactive family help");
    }
    const participantPersonIds = [
      ...new Set(snapshot.participants.map((participant) => participant.personId)),
    ];
    const householdParticipants = await this.database<{ readonly person_id: string }[]>`
      select person_id from household_memberships
      where household_id = ${row.household_id} and status = 'active'
        and person_id = any(${this.database.array(participantPersonIds)}::uuid[])
    `;
    if (householdParticipants.length !== participantPersonIds.length) {
      throw new UnauthorizedError(
        "Private family sources can go only to a group made entirely of this family",
      );
    }
    const proactiveRule = snapshot.rules.find(
      (rule) =>
        rule.active &&
        rule.participantSetDigest === snapshot.participantSetDigest &&
        rule.allowedOperations.includes("proactive_coverage"),
    );
    if (!proactiveRule) throw new UnauthorizedError("This exact group has no active family coverage rule");
    const routing = await loadLatestRouting(
      this.database,
      this.secretBox,
      row.external_channel_id,
      conversationId,
      snapshot.participantEpochId,
      snapshot.participantSetDigest,
    );
    return DestinationSchema.parse({
      householdId: row.household_id,
      householdControlEpoch: Number(row.household_control_epoch),
      conversationId,
      participantEpochId: snapshot.participantEpochId,
      participantSetDigest: snapshot.participantSetDigest,
      conversationAuthorityVersion: snapshot.authorityVersion,
      providerChatId: row.external_channel_id,
      providerParticipantDigest: routing.providerParticipantDigest,
      liveIdentityIds: routing.liveIdentityIds,
      proactiveRuleId: proactiveRule.ruleId,
      timeZone: row.timezone ?? "America/Los_Angeles",
    });
  }

  private async deriveSourcePattern(
    ownerPersonId: string,
    evidenceIds: readonly string[],
  ): Promise<PrivateBridgeSourcePattern | null> {
    const sources = new PostgresSourceIntelligence(this.database, this.secretBox, {
      rawRetentionDays: this.rawRetentionDays,
      privateCandidateRetentionDays: 7,
    });
    for (const initialId of evidenceIds) {
      let sourceRevisionId = initialId;
      for (let depth = 0; depth < 2; depth += 1) {
        const rows = await this.database<
          {
            readonly integration_id: string | null;
            readonly provider: string;
            readonly object_kind: string;
          }[]
        >`
          select object.integration_id, object.provider, object.object_kind
          from source_revisions revision
          join source_objects object on object.id = revision.source_object_id
          where revision.id = ${sourceRevisionId} and revision.owner_person_id = ${ownerPersonId}
            and revision.revoked_at is null and revision.retention_until > now()
        `;
        const row = rows[0];
        if (!row) break;
        const read = await sources.read({
          kind: "source_revision",
          sourceRevisionId,
          scope: { kind: "person", personId: ownerPersonId },
          asOf: new Date().toISOString(),
        });
        if (read.kind !== "source_revision") break;
        const content = JsonObjectSchema.parse(read.content);
        if (
          row.provider === "gmail" &&
          row.object_kind === "mail_message" &&
          row.integration_id &&
          typeof content.threadId === "string" &&
          typeof content.from === "string"
        ) {
          return SourcePatternSchema.parse({
            version: 1,
            kind: "gmail_thread",
            integrationId: row.integration_id,
            provider: "gmail",
            objectKind: "mail_message",
            classification: "coverage_proposal",
            threadDigest: canonicalDigest(normalizeToken(content.threadId)),
            senderDigest: canonicalDigest(normalizeMailbox(content.from)),
          });
        }
        if (
          row.provider === "google.calendar" &&
          row.object_kind === "calendar_event" &&
          row.integration_id &&
          typeof content.recurrenceId === "string"
        ) {
          return SourcePatternSchema.parse({
            version: 1,
            kind: "calendar_series",
            integrationId: row.integration_id,
            provider: "google.calendar",
            objectKind: "calendar_event",
            classification: "coverage_proposal",
            recurrenceDigest: canonicalDigest(normalizeToken(content.recurrenceId)),
          });
        }
        if (row.provider === "gmail.attachment" && typeof content.parentSourceRevisionId === "string") {
          const parsed = z.string().uuid().safeParse(content.parentSourceRevisionId);
          if (parsed.success) {
            sourceRevisionId = parsed.data;
            continue;
          }
        }
        break;
      }
    }
    return null;
  }
}

export function openPrivateBridgePayload(
  secretBox: SecretBox,
  actionIntentId: string,
  ciphertext: Buffer,
): PrivateBridgePayload {
  return PrivateBridgePayloadSchema.parse(
    JSON.parse(
      secretBox
        .decrypt(JSON.parse(ciphertext.toString("utf8")), actionIntentPurpose(actionIntentId))
        .toString("utf8"),
    ),
  );
}

function sealPayload(secretBox: SecretBox, actionIntentId: string, payload: PrivateBridgePayload) {
  const encrypted = secretBox.encrypt(canonicalJson(payload), actionIntentPurpose(actionIntentId));
  return {
    ciphertext: Buffer.from(JSON.stringify(encrypted), "utf8"),
    keyVersion: encrypted.kid,
  };
}

function actionIntentPurpose(actionIntentId: string): string {
  return `florence:action-intent:${actionIntentId}:payload`;
}

function openCandidate(secretBox: SecretBox, candidate: CandidateRow): Record<string, unknown> {
  return JsonObjectSchema.parse(
    JSON.parse(
      secretBox
        .decrypt(
          JSON.parse(candidate.content_ciphertext.toString("utf8")),
          `florence:knowledge-candidate:${candidate.id}:content`,
        )
        .toString("utf8"),
    ),
  );
}

async function loadCandidateForOwner(
  executor: Executor,
  candidateId: string,
  personId: string,
  now: Date,
  requirePending: boolean,
): Promise<CandidateRow> {
  const rows = await executor<CandidateRow[]>`
    select id, owner_person_id, candidate_kind, content_digest, content_ciphertext,
      evidence_refs, status, reviewed_by_person_id, expires_at
    from knowledge_candidates
    where id = ${candidateId} and scope_kind = 'person' and owner_person_id = ${personId}
    ${requirePending ? executor`and status = 'pending'` : executor`and status in ('pending', 'accepted')`}
    and (expires_at is null or expires_at > ${now})
    for share
  `;
  const candidate = rows[0];
  if (!candidate) throw new NotFoundError("Current private review does not exist for this owner");
  return candidate;
}

async function requireCurrentEvidence(
  executor: Executor,
  ownerPersonId: string,
  evidenceIds: readonly string[],
  now: Date,
): Promise<{ readonly integrationId: string | null }> {
  const ids = [...new Set(evidenceIds)].sort();
  const rows = await executor<{ readonly id: string; readonly integration_id: string | null }[]>`
    select revision.id, object.integration_id
    from source_revisions revision
    join source_objects object on object.id = revision.source_object_id
    left join integrations integration on integration.id = object.integration_id
    where revision.id = any(${executor.array(ids)}::uuid[])
      and revision.owner_person_id = ${ownerPersonId}
      and object.status = 'active'
      and revision.revision_number = object.latest_revision_number
      and revision.revoked_at is null and revision.retention_until > ${now}
      and revision.content_ciphertext is not null
      and (object.integration_id is null or integration.status in ('active', 'paused'))
    for share of revision, object
  `;
  if (rows.length !== ids.length) {
    throw new StaleAuthorityError("Private source evidence is no longer current and retained");
  }
  const integrationIds = [
    ...new Set(rows.flatMap((row) => (row.integration_id ? [row.integration_id] : []))),
  ];
  const hasIntegrationFreeEvidence = rows.some((row) => row.integration_id === null);
  if (integrationIds.length > 1 || (integrationIds.length === 1 && hasIntegrationFreeEvidence)) {
    throw new StaleAuthorityError("Private source evidence crossed integration authority boundaries");
  }
  return { integrationId: integrationIds[0] ?? null };
}

async function loadLatestRouting(
  executor: Executor,
  secretBox: SecretBox,
  providerChatId: string,
  conversationId: string,
  participantEpochId: string,
  participantSetDigest: string,
): Promise<{ providerParticipantDigest: string; liveIdentityIds: string[] }> {
  const rows = await executor<{ readonly provider_event_id: string; readonly envelope_ciphertext: Buffer }[]>`
    select provider_event_id, envelope_ciphertext from provider_events
    where provider = 'linq' and external_channel_id = ${providerChatId}
      and admission_status = 'verified'
    order by received_at desc limit 25
  `;
  for (const row of rows) {
    try {
      const record = z
        .strictObject({
          routing: z
            .strictObject({
              conversationId: z.string().uuid(),
              participantEpochId: z.string().uuid(),
              appParticipantDigest: DigestSchema,
              providerParticipantDigest: ProviderDigestSchema,
              liveIdentityIds: z.array(z.string().uuid()).min(1).max(100),
            })
            .passthrough(),
        })
        .passthrough()
        .parse(
          JSON.parse(
            secretBox
              .decrypt(
                JSON.parse(row.envelope_ciphertext.toString("utf8")),
                `provider-event:${row.provider_event_id}`,
              )
              .toString("utf8"),
          ),
        );
      if (
        record.routing.conversationId === conversationId &&
        record.routing.participantEpochId === participantEpochId &&
        record.routing.appParticipantDigest === participantSetDigest
      ) {
        return {
          providerParticipantDigest: record.routing.providerParticipantDigest,
          liveIdentityIds: record.routing.liveIdentityIds,
        };
      }
    } catch {
      // Ignore an old/unsupported envelope and continue to the next exact event.
    }
  }
  throw new StaleAuthorityError("Florence needs a fresh message from this exact group before sharing");
}

function requireExactProposalEvidence(
  payload: z.infer<typeof PreparedPayloadSchema>,
  ...evidenceLists: readonly { readonly sourceRevisionId: string }[][]
): void {
  const allowed = new Set(payload.evidenceSourceRevisionIds);
  for (const evidence of evidenceLists.flat()) {
    if (!allowed.has(evidence.sourceRevisionId)) {
      throw new UnauthorizedError("Worker proposal cited evidence outside the approved private source");
    }
  }
}

function proposalDigests(actionIntentId: string, payload: z.infer<typeof ProposedPayloadSchema>) {
  const opening = openingEffectPlan(actionIntentId, payload);
  return {
    actionDigest: canonicalDigest({
      effectKind: opening.effectKind,
      idempotencyKey: opening.idempotencyKey,
    }),
    dataDigest: canonicalDigest({
      data: opening.data,
      payloadDigest: canonicalDigest(opening.payload),
    }),
    policyDigest: canonicalDigest(opening.policy),
    targetDigest: canonicalDigest(opening.target),
  };
}

function openingEffectPlan(actionIntentId: string, payload: z.infer<typeof ProposedPayloadSchema>) {
  const loopUpdate = payload.loopUpdate;
  return {
    effectKind: "linq.message" as const,
    idempotencyKey: loopUpdate
      ? `private-bridge:${actionIntentId}:loop-v${loopUpdate.expectedLoopVersion + 1}:update`
      : `private-bridge:${actionIntentId}:open`,
    data: {
      candidateContentDigest: payload.candidateContentDigest,
      evidenceDigest: payload.evidenceDigest,
      sourceFrontier: payload.sourceFrontier,
      minimumMeaning: payload.minimumDisclosure.minimumMeaning,
      commitment: payload.commitment,
      loopUpdate,
    },
    policy: {
      exactSourceOwnerApproval: true,
      noRawContent: true,
      standingRule: payload.standingRule,
      approvalMode: payload.approvalMode,
      operation: "proactive_coverage",
      transition: loopUpdate ? "coverage_revised" : "coverage_created",
    },
    target: {
      destination: payload.destination,
      loopId: payload.loopId,
      expectedLoopVersion: loopUpdate?.expectedLoopVersion ?? null,
      resultingLoopVersion: loopUpdate ? loopUpdate.expectedLoopVersion + 1 : 1,
    },
    payload: {
      providerChatId: payload.destination.providerChatId,
      expectedProviderParticipantDigest: payload.destination.providerParticipantDigest,
      text: privateBridgeOutboundText(payload),
    },
  } as const;
}

/** The exact complete text rendered for approval and later sealed into the outbound effect. */
export function privateBridgeOutboundText(
  payload: Extract<PrivateBridgePayload, { readonly phase: "awaiting_approval" }>,
): string {
  return payload.loopUpdate
    ? neutralCoverageUpdateText(payload.minimumDisclosure.minimumMeaning)
    : `${payload.minimumDisclosure.minimumMeaning} Coverage is open. Who can cover it?`;
}

function neutralCoverageUpdateText(minimumSharedMeaning: string): string {
  const trimmed = minimumSharedMeaning.trim();
  const meaning = /[.!?]$/u.test(trimmed) ? trimmed : `${trimmed}.`;
  return `${meaning} This coverage request was updated. Coverage is open. Who can cover it?`;
}

function openingAuthorizationExpiry(intent: IntentRow, loop: CoverageLoop): Date {
  const now = Date.now();
  const lastResponsibleAt = new Date(loop.timing.lastResponsibleAt).getTime();
  const expiresAt = new Date(Math.min(intent.expires_at.getTime(), lastResponsibleAt));
  if (!Number.isFinite(lastResponsibleAt) || expiresAt.getTime() <= now) {
    throw new StaleAuthorityError("Coverage opening is already too late to send");
  }
  return expiresAt;
}

function destinationDigest(destination: z.infer<typeof DestinationSchema>): string {
  return canonicalDigest({
    householdId: destination.householdId,
    householdControlEpoch: destination.householdControlEpoch,
    conversationId: destination.conversationId,
    participantEpochId: destination.participantEpochId,
    participantSetDigest: destination.participantSetDigest,
    conversationAuthorityVersion: destination.conversationAuthorityVersion,
    providerChatId: destination.providerChatId,
    providerParticipantDigest: destination.providerParticipantDigest,
    liveIdentityIds: [...destination.liveIdentityIds].sort(),
    proactiveRuleId: destination.proactiveRuleId,
  });
}

function requireMatchingDigests(
  intent: IntentRow,
  supplied: {
    readonly actionDigest: string;
    readonly dataDigest: string;
    readonly policyDigest: string;
    readonly targetDigest: string;
  },
): void {
  if (
    intent.action_digest !== supplied.actionDigest ||
    intent.data_digest !== supplied.dataDigest ||
    intent.policy_digest !== supplied.policyDigest ||
    intent.target_digest !== supplied.targetDigest
  ) {
    throw new StaleAuthorityError("Approval no longer matches the exact proposed action");
  }
}

async function insertApproval(
  transaction: Transaction,
  personId: string,
  actionIntentId: string,
  digests: {
    readonly actionDigest: string;
    readonly dataDigest: string;
    readonly policyDigest: string;
    readonly targetDigest: string;
  },
  intentExpiresAt: Date,
): Promise<void> {
  const approvedAt = new Date();
  const expiresAt = new Date(Math.min(intentExpiresAt.getTime(), approvedAt.getTime() + 24 * 60 * 60_000));
  await transaction`
    insert into action_approvals (
      id, action_intent_id, approved_by_person_id, action_digest, data_digest,
      policy_digest, target_digest, approved_at, expires_at
    ) values (
      ${randomUUID()}, ${actionIntentId}, ${personId}, ${digests.actionDigest},
      ${digests.dataDigest}, ${digests.policyDigest}, ${digests.targetDigest},
      ${approvedAt}, ${expiresAt}
    ) on conflict (action_intent_id, approved_by_person_id) do update set
      action_digest = excluded.action_digest, data_digest = excluded.data_digest,
      policy_digest = excluded.policy_digest, target_digest = excluded.target_digest,
      approved_at = excluded.approved_at, expires_at = excluded.expires_at, revoked_at = null
  `;
}

async function createStandingRule(
  transaction: Transaction,
  ownerPersonId: string,
  actionIntentId: string,
  payload: z.infer<typeof ProposedPayloadSchema>,
): Promise<string> {
  if (!payload.sourcePattern) throw new ConflictError("Standing rule requires an exact source pattern");
  const now = new Date();
  const scopeDigest = canonicalDigest(payload.sourcePattern);
  await transaction`
    select pg_advisory_xact_lock(hashtextextended(${`bridge-rule:${ownerPersonId}:${scopeDigest}:${payload.destination.conversationId}`}, 0))
  `;
  const prior = await transaction<{ readonly id: string; readonly current_revision_id: string | null }[]>`
    select rule.id, rule.current_revision_id
    from bridge_rules rule
    join bridge_rule_revisions revision on revision.id = rule.current_revision_id
    where rule.owner_person_id = ${ownerPersonId} and rule.status = 'active'
      and rule.destination_conversation_id = ${payload.destination.conversationId}
      and revision.source_scope_digest = ${scopeDigest}
    for update of rule
  `;
  for (const rule of prior) {
    await transaction`
      update bridge_rules set status = 'revoked', version = version + 1, updated_at = ${now}
      where id = ${rule.id}
    `;
    if (rule.current_revision_id) {
      await transaction`
        update bridge_rule_revisions set ended_at = coalesce(ended_at, ${now})
        where id = ${rule.current_revision_id}
      `;
    }
  }
  const ruleId = randomUUID();
  const revisionId = randomUUID();
  const humanLabel =
    payload.sourcePattern.kind === "gmail_thread"
      ? "Future coverage items from this exact email thread"
      : "Future coverage items from this exact recurring calendar series";
  await transaction`
    insert into bridge_rules (
      id, owner_person_id, destination_conversation_id, rule_key, status,
      current_revision_id, version, created_at, updated_at
    ) values (
      ${ruleId}, ${ownerPersonId}, ${payload.destination.conversationId},
      ${humanLabel}, 'active', null, 1, ${now}, ${now}
    )
  `;
  await transaction`
    insert into bridge_rule_revisions (
      id, bridge_rule_id, revision, source_scope_digest, purpose_digest,
      destination_digest, minimum_meaning_schema, retention_seconds,
      approved_by_person_id, effective_at
    ) values (
      ${revisionId}, ${ruleId}, 1, ${scopeDigest},
      ${canonicalDigest({ purpose: "family_coverage", classification: "coverage_proposal" })},
      ${destinationDigest(payload.destination)},
      ${transaction.json({
        schemaVersion: 1,
        humanLabel,
        sourcePattern: payload.sourcePattern,
        destination: {
          conversationId: payload.destination.conversationId,
          participantEpochId: payload.destination.participantEpochId,
          participantSetDigest: payload.destination.participantSetDigest,
          conversationAuthorityVersion: payload.destination.conversationAuthorityVersion,
          proactiveRuleId: payload.destination.proactiveRuleId,
        },
        disclosure: { minimumMeaningOnly: true, rawContentAllowed: false },
        createdFromActionIntentId: actionIntentId,
      })},
      2592000, ${ownerPersonId}, ${now}
    )
  `;
  await transaction`
    update bridge_rules set current_revision_id = ${revisionId} where id = ${ruleId}
  `;
  await appendBridgeAudit(transaction, {
    personId: ownerPersonId,
    householdId: payload.destination.householdId,
    conversationId: payload.destination.conversationId,
    targetId: ruleId,
    eventType: "narrow_private_source_bridge_approved",
    reasons: ["exact_source_pattern", "owner_approved", "minimum_meaning_only"],
    manifest: { humanLabel, sourcePatternKind: payload.sourcePattern.kind, rawContentAllowed: false },
  });
  return ruleId;
}

async function requireStandingRule(
  executor: Executor,
  ownerPersonId: string,
  fence: z.infer<typeof StandingRuleFenceSchema>,
  pattern: PrivateBridgeSourcePattern,
  destination: z.infer<typeof DestinationSchema>,
): Promise<void> {
  const rows = await executor<
    {
      readonly version: number | string;
      readonly current_revision_id: string;
      readonly source_scope_digest: string;
      readonly destination_digest: string;
      readonly minimum_meaning_schema: unknown;
    }[]
  >`
    select rule.version, rule.current_revision_id, revision.source_scope_digest,
      revision.destination_digest, revision.minimum_meaning_schema
    from bridge_rules rule
    join bridge_rule_revisions revision on revision.id = rule.current_revision_id
    where rule.id = ${fence.ruleId} and rule.owner_person_id = ${ownerPersonId}
      and rule.destination_conversation_id = ${destination.conversationId}
      and rule.status = 'active' and revision.ended_at is null
  `;
  const row = rows[0];
  const schema = row?.minimum_meaning_schema as { sourcePattern?: unknown } | undefined;
  if (
    !row ||
    row.current_revision_id !== fence.ruleRevisionId ||
    Number(row.version) !== fence.ruleVersion ||
    row.source_scope_digest !== canonicalDigest(pattern) ||
    row.destination_digest !== destinationDigest(destination) ||
    canonicalDigest(schema?.sourcePattern) !== canonicalDigest(pattern)
  ) {
    throw new StaleAuthorityError("Standing sharing rule no longer matches this source and group");
  }
}

async function enqueueProposal(
  transaction: Transaction,
  secretBox: SecretBox,
  ownerPersonId: string,
  actionIntentId: string,
  payload: z.infer<typeof PreparedPayloadSchema>,
  expiresAt: Date,
): Promise<void> {
  await new DurableWork(transaction, secretBox).enqueue({
    kind: "orchestrate.private_bridge_proposal",
    idempotencyKey: `private-bridge:proposal:${actionIntentId}`,
    payload: { actionIntentId },
    person: { id: ownerPersonId, controlEpoch: payload.personControlEpoch },
    household: {
      id: payload.destination.householdId,
      controlEpoch: payload.destination.householdControlEpoch,
    },
    conversation: {
      id: payload.destination.conversationId,
      authorityVersion: payload.destination.conversationAuthorityVersion,
    },
    deadlineAt: expiresAt,
    maxAttempts: 5,
  });
}

async function enqueueCommit(
  transaction: Transaction,
  secretBox: SecretBox,
  intent: IntentRow,
): Promise<void> {
  await new DurableWork(transaction, secretBox).enqueue({
    kind: "private_bridge.commit",
    idempotencyKey: `private-bridge:commit:${intent.id}`,
    payload: { actionIntentId: intent.id },
    person: { id: intent.person_id, controlEpoch: Number(intent.person_control_epoch) },
    household: { id: intent.household_id, controlEpoch: Number(intent.household_control_epoch) },
    conversation: {
      id: intent.conversation_id,
      authorityVersion: Number(intent.conversation_authority_version),
    },
    deadlineAt: intent.expires_at,
    maxAttempts: 5,
  });
}

async function prepareLoopUpdateFence(
  transaction: Transaction,
  secretBox: SecretBox,
  ownerPersonId: string,
  candidate: CandidateRow,
  destination: z.infer<typeof DestinationSchema>,
): Promise<z.infer<typeof LoopUpdateFenceSchema>> {
  const review = LoopUpdateReviewContentSchema.parse(openCandidate(secretBox, candidate));
  const priorCandidates = await transaction<CandidateRow[]>`
    select id, owner_person_id, candidate_kind, content_digest, content_ciphertext,
      evidence_refs, status, reviewed_by_person_id, expires_at
    from knowledge_candidates
    where id = ${review.priorCandidateId} and owner_person_id = ${ownerPersonId}
      and scope_kind = 'person' and candidate_kind = 'coverage_proposal'
      and status = 'accepted' and reviewed_by_person_id = ${ownerPersonId}
    for share
  `;
  const priorCandidate = priorCandidates[0];
  if (!priorCandidate) {
    throw new StaleAuthorityError("Coverage update lost its exact accepted source candidate");
  }
  const priorIntent = await loadSucceededBridgeIntent(
    transaction,
    review.sourceActionIntentId,
    ownerPersonId,
  );
  const priorPayload = openPrivateBridgePayload(secretBox, priorIntent.id, priorIntent.payload_ciphertext);
  if (
    priorPayload.phase !== "awaiting_approval" ||
    priorPayload.candidateId !== priorCandidate.id ||
    priorPayload.candidateContentDigest !== priorCandidate.content_digest ||
    priorPayload.loopId !== review.existingLoopId
  ) {
    throw new StaleAuthorityError("Coverage update no longer matches its accepted source action");
  }
  const loop = await new PostgresCoordination(transaction, secretBox).load(review.existingLoopId);
  if (!loop) throw new StaleAuthorityError("Coverage update lost its existing loop");
  requireExactAcceptedLoop(priorIntent, priorPayload, loop);
  requireLiveUpdateDestination(loop, destination);
  if (!isLiveCoverageState(loop.state)) {
    throw new StaleAuthorityError("Only a live coverage loop can be updated");
  }
  return LoopUpdateFenceSchema.parse({
    existingLoopId: loop.loopId,
    expectedLoopVersion: loop.version,
    expectedLoopDestinationDigest: canonicalDigest(loop.destination),
    priorCandidateId: priorCandidate.id,
    priorCandidateContentDigest: priorCandidate.content_digest,
    sourceActionIntentId: priorIntent.id,
    sourceActionIntentDigests: intentDigests(priorIntent),
  });
}

async function requireCurrentLoopUpdate(
  executor: Executor,
  secretBox: SecretBox,
  intent: IntentRow,
  payload: PrivateBridgePayload,
  candidate: CandidateRow,
  lockLoop: boolean,
): Promise<CoverageLoop> {
  const fence = payload.loopUpdate;
  if (!fence || (payload.phase === "prepared" && payload.standingRule)) {
    throw new StaleAuthorityError("Coverage update fence is invalid");
  }
  if (candidate.candidate_kind !== "coverage_loop_update_review") {
    throw new StaleAuthorityError("Coverage update candidate kind changed");
  }
  const review = LoopUpdateReviewContentSchema.parse(openCandidate(secretBox, candidate));
  if (
    review.existingLoopId !== fence.existingLoopId ||
    review.existingLoopId !==
      (payload.phase === "awaiting_approval" ? payload.loopId : review.existingLoopId) ||
    review.sourceActionIntentId !== fence.sourceActionIntentId ||
    review.priorCandidateId !== fence.priorCandidateId
  ) {
    throw new StaleAuthorityError("Coverage update review no longer matches its authenticated fence");
  }
  const priorCandidates = await executor<CandidateRow[]>`
    select id, owner_person_id, candidate_kind, content_digest, content_ciphertext,
      evidence_refs, status, reviewed_by_person_id, expires_at
    from knowledge_candidates
    where id = ${fence.priorCandidateId} and owner_person_id = ${intent.person_id}
      and scope_kind = 'person' and candidate_kind = 'coverage_proposal'
      and status = 'accepted' and reviewed_by_person_id = ${intent.person_id}
    for share
  `;
  const priorCandidate = priorCandidates[0];
  if (priorCandidate?.content_digest !== fence.priorCandidateContentDigest) {
    throw new StaleAuthorityError("Coverage update's accepted source candidate changed");
  }
  const priorIntent = await loadSucceededBridgeIntent(executor, fence.sourceActionIntentId, intent.person_id);
  if (canonicalDigest(intentDigests(priorIntent)) !== canonicalDigest(fence.sourceActionIntentDigests)) {
    throw new StaleAuthorityError("Coverage update's accepted source action changed");
  }
  const priorPayload = openPrivateBridgePayload(secretBox, priorIntent.id, priorIntent.payload_ciphertext);
  if (
    priorPayload.phase !== "awaiting_approval" ||
    priorPayload.candidateId !== priorCandidate.id ||
    priorPayload.candidateContentDigest !== priorCandidate.content_digest ||
    priorPayload.loopId !== fence.existingLoopId
  ) {
    throw new StaleAuthorityError("Coverage update source action no longer matches the loop");
  }
  const coordination = new PostgresCoordination(executor, secretBox);
  const loop = lockLoop
    ? await coordination.loadForUpdate(fence.existingLoopId)
    : await coordination.load(fence.existingLoopId);
  if (!loop) throw new StaleAuthorityError("Coverage update lost its existing loop");
  requireExactAcceptedLoop(priorIntent, priorPayload, loop);
  requireLiveUpdateDestination(loop, payload.destination);
  if (
    !isLiveCoverageState(loop.state) ||
    loop.version !== fence.expectedLoopVersion ||
    canonicalDigest(loop.destination) !== fence.expectedLoopDestinationDigest
  ) {
    throw new StaleAuthorityError("Coverage loop changed after this update was prepared");
  }
  return loop;
}

async function loadCurrentCandidateFrontierFence(
  executor: Executor,
  ownerPersonId: string,
  candidate: CandidateRow,
  lock: boolean,
): Promise<PrivateBridgeSourceFrontier> {
  const lockClause = lock
    ? executor`for update of frontier, integration`
    : executor`for share of frontier, integration`;
  const rows = await executor<
    {
      readonly id: string;
      readonly integration_id: string;
      readonly integration_control_epoch: number | string;
      readonly case_kind: string;
      readonly case_key_digest: string;
      readonly version: number | string;
      readonly frontier_digest: string;
      readonly source_generation: number | string;
      readonly reconciled_generation: number | string;
      readonly current_candidate_id: string | null;
      readonly disposition: string;
    }[]
  >`
    select frontier.id, frontier.integration_id,
      integration.control_epoch as integration_control_epoch,
      frontier.case_kind, frontier.case_key_digest, frontier.version, frontier.frontier_digest,
      frontier.source_generation, frontier.reconciled_generation,
      frontier.current_candidate_id, frontier.disposition
    from private_source_frontiers frontier
    join integrations integration on integration.id = frontier.integration_id
      and integration.person_id = frontier.owner_person_id
      and integration.provider = 'google' and integration.status = 'active'
      and integration.information_current_control_epoch = integration.control_epoch
    where frontier.owner_person_id = ${ownerPersonId}
      and frontier.current_candidate_id = ${candidate.id}
      and frontier.disposition = 'candidate'
    ${lockClause}
  `;
  if (rows.length !== 1) {
    throw new StaleAuthorityError("Private source candidate lost its exact current frontier");
  }
  const row = rows[0];
  if (!row) throw new StaleAuthorityError("Private source candidate frontier disappeared");
  if (
    row.case_kind !== "gmail_thread" ||
    row.current_candidate_id !== candidate.id ||
    row.disposition !== "candidate" ||
    Number(row.source_generation) !== Number(row.reconciled_generation)
  ) {
    throw new StaleAuthorityError("Private source candidate frontier is no longer clean and current");
  }
  return SourceFrontierFenceSchema.parse({
    frontierId: row.id,
    integrationId: row.integration_id,
    integrationControlEpoch: Number(row.integration_control_epoch),
    caseKind: row.case_kind,
    caseKeyDigest: row.case_key_digest,
    version: Number(row.version),
    frontierDigest: row.frontier_digest,
    sourceGeneration: Number(row.source_generation),
  });
}

async function requireExactCurrentSourceFrontier(
  executor: Executor,
  ownerPersonId: string,
  expected: PrivateBridgeSourceFrontier,
  lock: boolean,
): Promise<void> {
  const lockClause = lock
    ? executor`for update of frontier, integration`
    : executor`for share of frontier, integration`;
  const rows = await executor<{ readonly id: string }[]>`
    select frontier.id
    from private_source_frontiers frontier
    join integrations integration on integration.id = frontier.integration_id
      and integration.person_id = frontier.owner_person_id
      and integration.provider = 'google' and integration.status = 'active'
    where frontier.id = ${expected.frontierId}
      and frontier.owner_person_id = ${ownerPersonId}
      and frontier.integration_id = ${expected.integrationId}
      and integration.control_epoch = ${expected.integrationControlEpoch}
      and integration.information_current_control_epoch = ${expected.integrationControlEpoch}
      and frontier.case_kind = ${expected.caseKind}
      and frontier.case_key_digest = ${expected.caseKeyDigest}
      and frontier.version = ${expected.version}
      and frontier.frontier_digest = ${expected.frontierDigest}
      and frontier.source_generation = ${expected.sourceGeneration}
      and frontier.reconciled_generation = ${expected.sourceGeneration}
      and frontier.disposition in ('candidate', 'quiet')
    ${lockClause}
  `;
  if (rows.length !== 1) {
    throw new StaleAuthorityError("Replacement private source frontier is no longer exact and current");
  }
}

async function requirePreparedCandidateSourceFrontier(
  executor: Executor,
  ownerPersonId: string,
  candidate: CandidateRow,
  evidenceIntegrationId: string,
  discovered: PrivateBridgeSourceFrontier | null,
): Promise<PrivateBridgeSourceFrontier> {
  if (!discovered || discovered.integrationId !== evidenceIntegrationId) {
    throw new StaleAuthorityError(
      "Integration-backed private source candidate lost its exact current frontier",
    );
  }
  const current = await loadCurrentCandidateFrontierFence(executor, ownerPersonId, candidate, false);
  if (current.integrationId !== evidenceIntegrationId || !sameSourceFrontierLockTarget(current, discovered)) {
    throw new StaleAuthorityError("Private source candidate frontier changed during preparation");
  }
  return current;
}

function requireDirectCandidateHasNoSourceFrontier(discovered: PrivateBridgeSourceFrontier | null): null {
  if (discovered) {
    throw new StaleAuthorityError("Integration-free private source candidate has an invalid frontier");
  }
  return null;
}

async function requireCurrentPayloadSourceFrontier(
  executor: Executor,
  ownerPersonId: string,
  candidate: CandidateRow,
  expected: PrivateBridgeSourceFrontier | null,
  evidenceIntegrationId: string | null,
  lock: boolean,
): Promise<void> {
  if (evidenceIntegrationId === null) {
    if (expected) {
      throw new StaleAuthorityError("Integration-free private source action carried a foreign frontier");
    }
    return;
  }
  if (!expected || expected.integrationId !== evidenceIntegrationId) {
    throw new StaleAuthorityError("Integration-backed private source action lost its exact frontier fence");
  }
  const current = await loadCurrentCandidateFrontierFence(executor, ownerPersonId, candidate, lock);
  if (canonicalDigest(current) !== canonicalDigest(expected)) {
    throw new StaleAuthorityError("New private source evidence arrived after this action was prepared");
  }
}

async function discoverCandidateSourceFrontier(
  executor: Executor,
  ownerPersonId: string,
  candidateId: string,
): Promise<PrivateBridgeSourceFrontier | null> {
  const rows = await executor<
    {
      readonly id: string;
      readonly integration_id: string;
      readonly integration_control_epoch: number | string;
      readonly case_kind: string;
      readonly case_key_digest: string;
      readonly version: number | string;
      readonly frontier_digest: string;
      readonly source_generation: number | string;
    }[]
  >`
    select frontier.id, frontier.integration_id, frontier.case_kind,
      frontier.case_key_digest, frontier.version, frontier.frontier_digest,
      frontier.source_generation, integration.control_epoch as integration_control_epoch
    from private_source_frontiers frontier
    join integrations integration on integration.id = frontier.integration_id
      and integration.person_id = frontier.owner_person_id
      and integration.provider = 'google' and integration.status = 'active'
      and integration.information_current_control_epoch = integration.control_epoch
    join knowledge_candidates candidate on candidate.id = frontier.current_candidate_id
    where candidate.id = ${candidateId} and candidate.owner_person_id = ${ownerPersonId}
      and candidate.scope_kind = 'person'
      and candidate.candidate_kind in ('coverage_proposal', 'coverage_loop_update_review')
      and candidate.status = 'pending' and frontier.disposition = 'candidate'
  `;
  if (rows.length > 1) {
    throw new ConflictError("Private source candidate is attached to multiple current frontiers");
  }
  const row = rows[0];
  return row
    ? SourceFrontierFenceSchema.parse({
        frontierId: row.id,
        integrationId: row.integration_id,
        integrationControlEpoch: Number(row.integration_control_epoch),
        caseKind: row.case_kind,
        caseKeyDigest: row.case_key_digest,
        version: Number(row.version),
        frontierDigest: row.frontier_digest,
        sourceGeneration: Number(row.source_generation),
      })
    : null;
}

function sameSourceFrontierLockTarget(
  left: PrivateBridgeSourceFrontier,
  right: PrivateBridgeSourceFrontier,
): boolean {
  return (
    left.frontierId === right.frontierId &&
    left.integrationId === right.integrationId &&
    left.caseKind === right.caseKind &&
    left.caseKeyDigest === right.caseKeyDigest
  );
}

export function requireSameObservedSourceFrontier(
  observed: PrivateBridgePayload,
  authoritative: PrivateBridgePayload,
): void {
  if (canonicalDigest(observed.sourceFrontier) !== canonicalDigest(authoritative.sourceFrontier)) {
    throw new StaleAuthorityError("Private source action changed while acquiring its exact locks");
  }
}

async function acquireCandidateSourceLocks(
  transaction: Transaction,
  ownerPersonId: string,
  frontier: PrivateBridgeSourceFrontier,
): Promise<void> {
  await transaction`
    select pg_advisory_xact_lock(
      hashtextextended(${privateSourceIntegrationLockKey(frontier.integrationId)}, 0)
    )
  `;
  await transaction`
    select pg_advisory_xact_lock(
      hashtextextended(${gmailThreadFrontierLockKey({
        ownerPersonId,
        integrationId: frontier.integrationId,
        caseKeyDigest: frontier.caseKeyDigest,
      })}, 0)
    )
  `;
}

async function loadSucceededBridgeIntent(
  executor: Executor,
  actionIntentId: string,
  ownerPersonId: string,
): Promise<IntentRow> {
  const rows = await executor<IntentRow[]>`
    select id, household_id, person_id, conversation_id, participant_epoch_id,
      action_digest, data_digest, policy_digest, target_digest, payload_ciphertext,
      status, person_control_epoch, household_control_epoch,
      conversation_authority_version, expires_at
    from action_intents
    where id = ${actionIntentId} and person_id = ${ownerPersonId}
      and action_kind = 'private_source_to_coverage_loop' and status = 'succeeded'
    for share
  `;
  const intent = rows[0];
  if (!intent) throw new StaleAuthorityError("Accepted private source action is no longer current");
  return intent;
}

function intentDigests(intent: IntentRow): z.infer<typeof IntentDigestFenceSchema> {
  return IntentDigestFenceSchema.parse({
    actionDigest: intent.action_digest,
    dataDigest: intent.data_digest,
    policyDigest: intent.policy_digest,
    targetDigest: intent.target_digest,
  });
}

function requireLiveUpdateDestination(
  loop: CoverageLoop,
  destination: z.infer<typeof DestinationSchema>,
): void {
  if (
    loop.householdId !== destination.householdId ||
    loop.destination.audience !== "group" ||
    loop.destination.conversationId !== destination.conversationId ||
    loop.destination.participantEpochId !== destination.participantEpochId ||
    loop.destination.participantSetDigest !== destination.participantSetDigest
  ) {
    throw new StaleAuthorityError("Coverage update targets a different live group");
  }
}

async function resolveExactAcceptedBridge(
  transaction: Transaction,
  secretBox: SecretBox,
  input: AcceptedPrivateBridgeCandidateInput,
  lock: boolean,
): Promise<{
  readonly intent: IntentRow;
  readonly payload: z.infer<typeof ProposedPayloadSchema>;
  readonly loop: CoverageLoop;
} | null> {
  const candidates = await transaction<{ readonly id: string; readonly content_digest: string }[]>`
    select id, content_digest
    from knowledge_candidates
    where id = ${input.candidateId} and owner_person_id = ${input.ownerPersonId}
      and scope_kind = 'person' and candidate_kind = 'coverage_proposal'
      and status = 'accepted' and reviewed_by_person_id = ${input.ownerPersonId}
  `;
  if (candidates[0]?.content_digest !== input.candidateContentDigest) return null;

  const intentLockClause = lock ? transaction`for update` : transaction``;
  const intentRows = await transaction<IntentRow[]>`
    select id, household_id, person_id, conversation_id, participant_epoch_id,
      action_digest, data_digest, policy_digest, target_digest, payload_ciphertext,
      status, person_control_epoch, household_control_epoch,
      conversation_authority_version, expires_at
    from action_intents
    where person_id = ${input.ownerPersonId}
      and action_kind = 'private_source_to_coverage_loop' and status = 'succeeded'
    ${intentLockClause}
  `;
  const exactIntents: {
    readonly intent: IntentRow;
    readonly payload: z.infer<typeof ProposedPayloadSchema>;
  }[] = [];
  for (const intent of intentRows) {
    let payload: PrivateBridgePayload;
    try {
      payload = openPrivateBridgePayload(secretBox, intent.id, intent.payload_ciphertext);
    } catch {
      continue;
    }
    if (
      payload.phase === "awaiting_approval" &&
      payload.candidateId === input.candidateId &&
      payload.candidateContentDigest === input.candidateContentDigest
    ) {
      exactIntents.push({ intent, payload });
    }
  }
  if (exactIntents.length === 0) return null;
  if (exactIntents.length !== 1) {
    throw new ConflictError("Accepted private source candidate has multiple succeeded bridges");
  }
  const exact = exactIntents[0];
  if (!exact) throw new ConflictError("Accepted private source bridge disappeared");
  if (lock) {
    const lockedCandidates = await transaction<{ readonly id: string; readonly content_digest: string }[]>`
      select id, content_digest
      from knowledge_candidates
      where id = ${input.candidateId} and owner_person_id = ${input.ownerPersonId}
        and scope_kind = 'person' and candidate_kind = 'coverage_proposal'
        and status = 'accepted' and reviewed_by_person_id = ${input.ownerPersonId}
      for share
    `;
    if (lockedCandidates[0]?.content_digest !== input.candidateContentDigest) return null;
  }
  const coordination = new PostgresCoordination(transaction, secretBox);
  const loop = lock
    ? await coordination.loadForUpdate(exact.payload.loopId)
    : await coordination.load(exact.payload.loopId);
  if (!loop) throw new ConflictError("Accepted private source bridge has no coverage loop");
  requireExactAcceptedLoop(exact.intent, exact.payload, loop);
  if (!isLiveCoverageState(loop.state)) return null;
  return { ...exact, loop };
}

function parseAcceptedCandidateInput(
  input: AcceptedPrivateBridgeCandidateInput,
): AcceptedPrivateBridgeCandidateInput {
  return {
    ownerPersonId: z.string().uuid().parse(input.ownerPersonId),
    candidateId: z.string().uuid().parse(input.candidateId),
    candidateContentDigest: DigestSchema.parse(input.candidateContentDigest),
  };
}

function parseAcceptedWithdrawalInput(
  input: AcceptedPrivateBridgeWithdrawalInput,
): AcceptedPrivateBridgeWithdrawalInput {
  const evidenceSourceRevisionIds = z
    .array(z.string().uuid())
    .min(1)
    .max(20)
    .parse([...input.evidenceSourceRevisionIds]);
  if (!(input.withdrawnAt instanceof Date) || Number.isNaN(input.withdrawnAt.getTime())) {
    throw new ConflictError("Accepted bridge withdrawal requires a valid instant");
  }
  const candidate = parseAcceptedCandidateInput(input);
  return {
    ...candidate,
    intent: z.enum(["cancel", "supersede"]).parse(input.intent),
    evidenceSourceRevisionIds: [...new Set(evidenceSourceRevisionIds)].sort(),
    replacementSourceFrontier: SourceFrontierFenceSchema.parse(input.replacementSourceFrontier),
    withdrawnAt: new Date(input.withdrawnAt),
  };
}

function requireExactAcceptedLoop(
  intent: IntentRow,
  payload: z.infer<typeof ProposedPayloadSchema>,
  loop: CoverageLoop,
): void {
  if (
    loop.loopId !== payload.loopId ||
    loop.householdId !== intent.household_id ||
    loop.householdId !== payload.destination.householdId ||
    loop.destination.audience !== "group" ||
    loop.destination.conversationId !== intent.conversation_id ||
    loop.destination.conversationId !== payload.destination.conversationId ||
    loop.destination.participantEpochId !== intent.participant_epoch_id ||
    loop.destination.participantEpochId !== payload.destination.participantEpochId ||
    loop.destination.participantSetDigest !== payload.destination.participantSetDigest
  ) {
    throw new ConflictError("Succeeded private source bridge does not match its coverage loop");
  }
}

function isLiveCoverageState(state: CoverageState): boolean {
  return ["provisional", "open", "awaiting_response", "covered", "at_risk"].includes(state);
}

function neutralAcceptedWithdrawalText(minimumSharedMeaning: string, intent: "cancel" | "supersede"): string {
  const trimmed = minimumSharedMeaning.trim();
  const meaning = /[.!?]$/u.test(trimmed) ? trimmed : `${trimmed}.`;
  return intent === "cancel"
    ? `${meaning} This coverage request was cancelled.`
    : `${meaning} This coverage request is no longer current.`;
}

function resolveTiming(proposal: z.infer<typeof commitmentProposalSchema>, fallbackTimeZone: string) {
  const now = Temporal.Now.instant();
  const timeZone = safeTimeZone(proposal.timeZone, fallbackTimeZone);
  const event = proposal.eventAt ? Temporal.Instant.from(proposal.eventAt) : null;
  const deadline = proposal.deadlineAt ? Temporal.Instant.from(proposal.deadlineAt) : null;
  let lastResponsible = deadline ?? (event ? event.subtract({ minutes: 30 }) : now.add({ hours: 24 }));
  if (Temporal.Instant.compare(lastResponsible, now) <= 0) lastResponsible = now.add({ minutes: 15 });
  const anchor = event ?? deadline ?? lastResponsible;
  return {
    timeZone,
    localDate: anchor.toZonedDateTimeISO(timeZone).toPlainDate().toString(),
    eventAt: event?.toString() ?? null,
    deadlineAt: deadline?.toString() ?? null,
    preparationMinutes: 0,
    travelMinutes: 0,
    earliestUsefulAt: now.toString(),
    lastResponsibleAt: lastResponsible.toString(),
    resolutionPolicy: "wall_clock_compatible" as const,
  };
}

function safeTimeZone(candidate: string, fallback: string): string {
  try {
    Temporal.Now.zonedDateTimeISO(candidate);
    return candidate;
  } catch {
    return fallback;
  }
}

function normalizeMailbox(value: string): string {
  const match = value.match(/<([^>]+)>/u);
  return (match?.[1] ?? value).trim().toLowerCase();
}

function normalizeToken(value: string): string {
  return value.trim().toLowerCase();
}

function stringArray(value: unknown): string[] {
  return z.array(z.string().uuid()).parse(value);
}

async function appendBridgeAudit(
  transaction: Transaction,
  input: {
    readonly personId: string;
    readonly householdId: string | null;
    readonly conversationId: string | null;
    readonly eventType: string;
    readonly targetId: string;
    readonly reasons: readonly string[];
    readonly manifest: unknown;
  },
): Promise<void> {
  const sequenceRows = await transaction<{ readonly sequence: number | string }[]>`
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
      ${Number(sequenceRows[0]?.sequence ?? 1)}, 'application', ${input.personId},
      ${input.eventType}, 'action_intent', ${input.targetId},
      ${transaction.array([...input.reasons])},
      ${transaction.json(JSON.parse(canonicalJson(input.manifest)))}, now()
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
