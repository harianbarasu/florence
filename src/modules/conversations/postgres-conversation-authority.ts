import { randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { Database } from "../../db/client.js";
import { ConflictError, NotFoundError, StaleAuthorityError, UnauthorizedError } from "../../shared/errors.js";
import {
  authorizeSendFromSnapshot,
  isPolicyWidening,
  participantApprovalDigest,
  participantEpochAuthorityDigest,
  participantSetDigest,
} from "./authority.js";
import {
  type ActivateConversationRuleInput,
  ActivateConversationRuleInputSchema,
  type ApplyNarrowingInput,
  ApplyNarrowingInputSchema,
  type AuthorizeSendInput,
  AuthorizeSendInputSchema,
  type BindConversationChannelInput,
  BindConversationChannelInputSchema,
  type ConsentToEpochInput,
  ConsentToEpochInputSchema,
  type ConversationAuthority,
  ConversationAuthoritySnapshotSchema,
  ConversationChannelBindingSchema,
  type ConversationChannelLookup,
  ConversationChannelLookupSchema,
  type CreateConversationInput,
  CreateConversationInputSchema,
  EntityIdSchema,
  type LiftNarrowingInput,
  LiftNarrowingInputSchema,
  ParticipantEpochSchema,
  type ParticipantPolicyValue,
  type RecordParticipantEpochInput,
  RecordParticipantEpochInputSchema,
  type SetParticipantPolicyInput,
  SetParticipantPolicyInputSchema,
} from "./contracts.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

export class PostgresConversationAuthority implements ConversationAuthority {
  public constructor(private readonly database: Executor) {}

  public async createConversation(inputCandidate: CreateConversationInput) {
    const input = CreateConversationInputSchema.parse(inputCandidate);
    const conversationId = randomUUID();
    const createdAt = new Date(input.createdAt);
    await this.database`
      insert into conversations (
        id, household_id, kind, purpose, status, authority_version, control_epoch,
        created_at, updated_at
      ) values (
        ${conversationId}, ${input.householdId}, ${input.kind}, ${input.purpose},
        'active', 1, 1, ${createdAt}, ${createdAt}
      )
    `;
    return { conversationId };
  }

  public async bindChannel(inputCandidate: BindConversationChannelInput) {
    const input = BindConversationChannelInputSchema.parse(inputCandidate);
    const channelId = randomUUID();
    const boundAt = new Date(input.boundAt);
    try {
      await this.database`
        insert into conversation_channels (
          id, conversation_id, provider, external_channel_id, status, bound_at
        ) values (
          ${channelId}, ${input.conversationId}, ${input.provider},
          ${input.externalChannelId}, 'active', ${boundAt}
        )
      `;
    } catch (error) {
      if (postgresErrorCode(error) === "23505") {
        throw new ConflictError("Provider channel is already bound to a conversation");
      }
      if (postgresErrorCode(error) === "23503") throw new NotFoundError("Conversation does not exist");
      throw error;
    }
    return { channelId };
  }

  public async findByChannel(inputCandidate: ConversationChannelLookup) {
    const input = ConversationChannelLookupSchema.parse(inputCandidate);
    const rows = await this.database<
      {
        readonly channel_id: string;
        readonly conversation_id: string;
        readonly provider: string;
        readonly external_channel_id: string;
        readonly status: string;
      }[]
    >`
      select id as channel_id, conversation_id, provider, external_channel_id, status
      from conversation_channels
      where provider = ${input.provider} and external_channel_id = ${input.externalChannelId}
    `;
    const row = rows[0];
    return row
      ? ConversationChannelBindingSchema.parse({
          channelId: row.channel_id,
          conversationId: row.conversation_id,
          provider: row.provider,
          externalChannelId: row.external_channel_id,
          status: row.status,
        })
      : null;
  }

  public async snapshot(conversationIdCandidate: string) {
    const conversationId = EntityIdSchema.parse(conversationIdCandidate);
    return inTransaction(this.database, (transaction) => loadSnapshot(transaction, conversationId));
  }

  public async recordParticipantEpoch(inputCandidate: RecordParticipantEpochInput) {
    const input = RecordParticipantEpochInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      const conversations = await transaction<
        { readonly id: string; readonly current_epoch_id: string | null }[]
      >`
        select id, current_epoch_id from conversations
        where id = ${input.conversationId} and status not in ('deletion_fenced', 'deleted')
        for update
      `;
      const conversation = conversations[0];
      if (!conversation) throw new NotFoundError("Writable conversation does not exist");

      const identities = await transaction<
        {
          readonly identity_id: string;
          readonly identity_status: string;
          readonly person_id: string;
          readonly person_status: string;
          readonly person_consented_at: Date | null;
        }[]
      >`
        select identity.id as identity_id, identity.status as identity_status,
          person.id as person_id, person.status as person_status,
          person.consented_at as person_consented_at
        from person_identities identity
        join people person on person.id = identity.person_id
        where identity.id = any(${transaction.array(input.participantIdentityIds)}::uuid[])
        order by identity.id
      `;
      if (identities.length !== input.participantIdentityIds.length) {
        throw new NotFoundError("Every live participant must resolve to an observed Florence identity");
      }
      if (identities.some((identity) => identity.identity_status === "revoked")) {
        throw new UnauthorizedError("A revoked identity cannot join a participant epoch");
      }
      const setDigest = participantSetDigest(input.participantIdentityIds);
      if (conversation.current_epoch_id) {
        const current = await transaction<{ readonly participant_set_digest: string }[]>`
          select participant_set_digest from participant_epochs
          where id = ${conversation.current_epoch_id}
        `;
        if (current[0]?.participant_set_digest === setDigest) {
          return loadEpoch(transaction, conversation.current_epoch_id);
        }
      }

      const sequenceRows = await transaction<{ readonly sequence: number | string }[]>`
        select coalesce(max(sequence), 0) + 1 as sequence
        from participant_epochs where conversation_id = ${input.conversationId}
      `;
      const sequence = Number(sequenceRows[0]?.sequence ?? 1);
      const epochId = randomUUID();
      const observedAt = new Date(input.observedAt);
      if (conversation.current_epoch_id) {
        await transaction`
          update participant_epochs set ended_at = ${observedAt}
          where id = ${conversation.current_epoch_id} and ended_at is null
        `;
      }
      await transaction`
        insert into participant_epochs (
          id, conversation_id, sequence, participant_set_digest, authority_digest,
          change_reason, started_at
        ) values (
          ${epochId}, ${input.conversationId}, ${sequence}, ${setDigest},
          ${participantEpochAuthorityDigest({ conversationId: input.conversationId, sequence, participantSetDigest: setDigest })},
          ${input.changeReason}, ${observedAt}
        )
      `;
      for (const identity of identities) {
        // START is the explicit global Florence consent the product promises. Project that
        // conservative consent into each exact audience, but never synthesize it from observation.
        const registered =
          identity.identity_status === "verified" &&
          identity.person_status === "registered" &&
          identity.person_consented_at !== null;
        await transaction`
          insert into epoch_participants (
            participant_epoch_id, person_identity_id, person_id, registration_status,
            consented_at, added_at
          ) values (
            ${epochId}, ${identity.identity_id}, ${identity.person_id},
            ${registered ? "registered" : "provisional"},
            ${registered ? identity.person_consented_at : null},
            ${observedAt}
          )
        `;
        if (
          registered &&
          (await loadActivePolicy(transaction, input.conversationId, identity.person_id)) === null
        ) {
          await insertPolicy(transaction, {
            conversationId: input.conversationId,
            personId: identity.person_id,
            actorPersonId: identity.person_id,
            version: 1,
            policy: {
              allowContentProcessing: true,
              allowDirectResponses: true,
              allowProactiveWrites: false,
              retentionSeconds: 2_592_000,
            },
            approvalDigest: null,
            changedAt: observedAt,
          });
        }
      }
      await transaction`
        update conversations
        set current_epoch_id = ${epochId}, authority_version = authority_version + 1,
            updated_at = ${observedAt}
        where id = ${input.conversationId}
      `;
      return loadEpoch(transaction, epochId);
    });
  }

  public async consentToEpoch(inputCandidate: ConsentToEpochInput) {
    const input = ConsentToEpochInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      const epochs = await transaction<{ readonly conversation_id: string }[]>`
        select epoch.conversation_id
        from participant_epochs epoch
        join conversations conversation on conversation.current_epoch_id = epoch.id
        where epoch.id = ${input.participantEpochId}
        for update of conversation
      `;
      const epoch = epochs[0];
      if (!epoch) throw new StaleAuthorityError("Consent must target the current participant epoch");
      const participants = await transaction<{ readonly person_id: string }[]>`
        select participant.person_id
        from epoch_participants participant
        join people person on person.id = participant.person_id
        join person_identities identity on identity.id = participant.person_identity_id
        where participant.participant_epoch_id = ${input.participantEpochId}
          and participant.person_id = ${input.personId}
          and person.status = 'registered' and identity.status = 'verified'
        for update of participant
      `;
      if (!participants[0]) {
        throw new UnauthorizedError("Only a registered, verified current participant may consent");
      }
      const consentedAt = new Date(input.consentedAt);
      await transaction`
        update epoch_participants
        set registration_status = 'registered', consented_at = ${consentedAt}
        where participant_epoch_id = ${input.participantEpochId} and person_id = ${input.personId}
      `;
      const activePolicy = await loadActivePolicy(transaction, epoch.conversation_id, input.personId);
      if (activePolicy === null) {
        await insertPolicy(transaction, {
          conversationId: epoch.conversation_id,
          personId: input.personId,
          actorPersonId: input.personId,
          version: 1,
          policy: input.policy,
          approvalDigest: null,
          changedAt: consentedAt,
        });
      }
      await incrementAuthority(transaction, epoch.conversation_id, consentedAt);
      return loadSnapshot(transaction, epoch.conversation_id);
    });
  }

  public async setParticipantPolicy(inputCandidate: SetParticipantPolicyInput) {
    const input = SetParticipantPolicyInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      await lockConversationVersion(transaction, input.conversationId, input.expectedAuthorityVersion);
      const snapshot = await loadSnapshot(transaction, input.conversationId);
      const participantIds = snapshot.participants.map((participant) => participant.personId);
      if (!participantIds.includes(input.targetPersonId) || !participantIds.includes(input.actorPersonId)) {
        throw new UnauthorizedError("Policy actors and targets must be current participants");
      }
      const current =
        snapshot.participants.find((entry) => entry.personId === input.targetPersonId)?.policy ?? null;
      const widening = isPolicyWidening(current, input.policy);
      if (widening) {
        requireExactApprovals(participantIds, input.approvedByPersonIds);
      } else if (input.actorPersonId !== input.targetPersonId) {
        throw new UnauthorizedError("A participant may narrow only their own policy");
      }
      const previous = await transaction<{ readonly id: string; readonly version: number | string }[]>`
        select id, version from participant_policies
        where conversation_id = ${input.conversationId} and person_id = ${input.targetPersonId}
          and status = 'active'
        for update
      `;
      const changedAt = new Date(input.changedAt);
      if (previous[0]) {
        await transaction`
          update participant_policies
          set status = 'superseded', superseded_at = ${changedAt}
          where id = ${previous[0].id}
        `;
      }
      await insertPolicy(transaction, {
        conversationId: input.conversationId,
        personId: input.targetPersonId,
        actorPersonId: input.actorPersonId,
        version: Number(previous[0]?.version ?? 0) + 1,
        policy: input.policy,
        approvalDigest: widening ? participantApprovalDigest(input.approvedByPersonIds) : null,
        changedAt,
      });
      await incrementAuthority(transaction, input.conversationId, changedAt);
      return loadSnapshot(transaction, input.conversationId);
    });
  }

  public async applyNarrowing(inputCandidate: ApplyNarrowingInput) {
    const input = ApplyNarrowingInputSchema.parse(inputCandidate);
    if ((input.kind === "retention_cap") !== (input.retentionSeconds !== null)) {
      throw new ConflictError("Only a retention cap carries a retention duration");
    }
    return inTransaction(this.database, async (transaction) => {
      await lockConversation(transaction, input.conversationId);
      await requireCurrentParticipant(transaction, input.conversationId, input.actorPersonId);
      const appliedAt = new Date(input.appliedAt);
      await transaction`
        insert into channel_suppressions (
          id, conversation_id, created_by_person_id, kind, retention_seconds,
          reason, active, created_at
        ) values (
          ${randomUUID()}, ${input.conversationId}, ${input.actorPersonId}, ${input.kind},
          ${input.retentionSeconds}, ${input.reason}, true, ${appliedAt}
        )
      `;
      if (input.kind === "stop") {
        const conversations = await transaction<{ readonly kind: string }[]>`
          select kind from conversations where id = ${input.conversationId}
        `;
        if (conversations[0]?.kind === "direct") {
          await transaction`
            update people
            set control_epoch = control_epoch + 1, updated_at = ${appliedAt}
            where id = ${input.actorPersonId}
          `;
          await transaction`
            update person_sessions set revoked_at = ${appliedAt}
            where person_id = ${input.actorPersonId} and revoked_at is null
          `;
        }
      }
      await incrementAuthority(transaction, input.conversationId, appliedAt);
      return loadSnapshot(transaction, input.conversationId);
    });
  }

  public async liftNarrowing(inputCandidate: LiftNarrowingInput) {
    const input = LiftNarrowingInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      const rows = await transaction<{ readonly conversation_id: string; readonly active: boolean }[]>`
        select conversation_id, active from channel_suppressions
        where id = ${input.suppressionId} for update
      `;
      const suppression = rows[0];
      if (!suppression) throw new NotFoundError("Conversation narrowing does not exist");
      await lockConversationVersion(transaction, suppression.conversation_id, input.expectedAuthorityVersion);
      const snapshot = await loadSnapshot(transaction, suppression.conversation_id);
      const participantIds = snapshot.participants.map((participant) => participant.personId);
      if (!participantIds.includes(input.actorPersonId))
        throw new UnauthorizedError("Actor is not a current participant");
      requireExactApprovals(participantIds, input.approvedByPersonIds);
      if (!suppression.active) return snapshot;
      const liftedAt = new Date(input.liftedAt);
      await transaction`
        update channel_suppressions set active = false, lifted_at = ${liftedAt}
        where id = ${input.suppressionId}
      `;
      await incrementAuthority(transaction, suppression.conversation_id, liftedAt);
      return loadSnapshot(transaction, suppression.conversation_id);
    });
  }

  public async activateRule(inputCandidate: ActivateConversationRuleInput) {
    const input = ActivateConversationRuleInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      await lockConversationVersion(transaction, input.conversationId, input.expectedAuthorityVersion);
      const snapshot = await loadSnapshot(transaction, input.conversationId);
      if (snapshot.participantSetDigest === null)
        throw new UnauthorizedError("Conversation has no live epoch");
      const participantIds = snapshot.participants.map((participant) => participant.personId);
      if (!participantIds.includes(input.actorPersonId))
        throw new UnauthorizedError("Actor is not a current participant");
      requireExactApprovals(participantIds, input.approvedByPersonIds);
      const previous = await transaction<{ readonly id: string; readonly version: number | string }[]>`
        select id, version from conversation_rules
        where conversation_id = ${input.conversationId} and rule_key = ${input.ruleKey}
          and status = 'active'
        for update
      `;
      const activatedAt = new Date(input.activatedAt);
      if (previous[0]) {
        await transaction`
          update conversation_rules set status = 'superseded', ended_at = ${activatedAt}
          where id = ${previous[0].id}
        `;
      }
      await transaction`
        insert into conversation_rules (
          id, conversation_id, rule_key, version, status, purpose, allowed_operations,
          participant_set_digest, approval_participant_digest, created_by_person_id,
          effective_at, created_at
        ) values (
          ${randomUUID()}, ${input.conversationId}, ${input.ruleKey},
          ${Number(previous[0]?.version ?? 0) + 1}, 'active', ${input.purpose},
          ${transaction.array(input.allowedOperations)}, ${snapshot.participantSetDigest},
          ${participantApprovalDigest(input.approvedByPersonIds)}, ${input.actorPersonId},
          ${activatedAt}, ${activatedAt}
        )
      `;
      await incrementAuthority(transaction, input.conversationId, activatedAt);
      return loadSnapshot(transaction, input.conversationId);
    });
  }

  public async authorizeSend(inputCandidate: AuthorizeSendInput) {
    const input = AuthorizeSendInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      await lockConversation(transaction, input.conversationId);
      return authorizeSendFromSnapshot(await loadSnapshot(transaction, input.conversationId), input);
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

async function loadEpoch(transaction: Transaction, epochId: string) {
  const epochs = await transaction<
    {
      readonly id: string;
      readonly conversation_id: string;
      readonly sequence: number | string;
      readonly participant_set_digest: string;
      readonly authority_digest: string;
      readonly started_at: Date;
      readonly ended_at: Date | null;
    }[]
  >`select * from participant_epochs where id = ${epochId}`;
  const epoch = epochs[0];
  if (!epoch) throw new NotFoundError("Participant epoch does not exist");
  const participants = await transaction<
    {
      readonly person_identity_id: string;
      readonly person_id: string;
      readonly registration_status: string;
      readonly consented_at: Date | null;
    }[]
  >`
    select person_identity_id, person_id, registration_status, consented_at
    from epoch_participants where participant_epoch_id = ${epochId}
    order by person_identity_id
  `;
  return ParticipantEpochSchema.parse({
    participantEpochId: epoch.id,
    conversationId: epoch.conversation_id,
    sequence: Number(epoch.sequence),
    participantSetDigest: epoch.participant_set_digest,
    authorityDigest: epoch.authority_digest,
    startedAt: epoch.started_at.toISOString(),
    endedAt: epoch.ended_at?.toISOString() ?? null,
    participants: participants.map((participant) => ({
      personIdentityId: participant.person_identity_id,
      personId: participant.person_id,
      registrationStatus: participant.registration_status,
      consentedAt: participant.consented_at?.toISOString() ?? null,
    })),
  });
}

async function loadSnapshot(transaction: Transaction, conversationId: string) {
  const conversations = await transaction<
    {
      readonly id: string;
      readonly status: string;
      readonly authority_version: number | string;
      readonly current_epoch_id: string | null;
      readonly participant_set_digest: string | null;
    }[]
  >`
    select conversation.id, conversation.status, conversation.authority_version,
      conversation.current_epoch_id, epoch.participant_set_digest
    from conversations conversation
    left join participant_epochs epoch on epoch.id = conversation.current_epoch_id
    where conversation.id = ${conversationId}
  `;
  const conversation = conversations[0];
  if (!conversation) throw new NotFoundError("Conversation does not exist");
  const participants = conversation.current_epoch_id
    ? await transaction<
        {
          readonly person_identity_id: string;
          readonly person_id: string;
          readonly registration_status: string;
          readonly consented_at: Date | null;
          readonly allow_content_processing: boolean | null;
          readonly allow_direct_responses: boolean | null;
          readonly allow_proactive_writes: boolean | null;
          readonly retention_seconds: number | null;
          readonly quiet_hours: unknown;
        }[]
      >`
        select participant.person_identity_id, participant.person_id,
          participant.registration_status, participant.consented_at,
          policy.allow_content_processing, policy.allow_direct_responses,
          policy.allow_proactive_writes, policy.retention_seconds, person.quiet_hours
        from epoch_participants participant
        join people person on person.id = participant.person_id
        left join participant_policies policy
          on policy.conversation_id = ${conversationId}
          and policy.person_id = participant.person_id and policy.status = 'active'
        where participant.participant_epoch_id = ${conversation.current_epoch_id}
        order by participant.person_identity_id
      `
    : [];
  const suppressions = await transaction<
    { readonly id: string; readonly kind: string; readonly retention_seconds: number | null }[]
  >`
    select id, kind, retention_seconds from channel_suppressions
    where conversation_id = ${conversationId} and active order by created_at, id
  `;
  const rules = await transaction<
    {
      readonly id: string;
      readonly rule_key: string;
      readonly participant_set_digest: string;
      readonly allowed_operations: string[];
    }[]
  >`
    select id, rule_key, participant_set_digest, allowed_operations
    from conversation_rules where conversation_id = ${conversationId} and status = 'active'
  `;
  return ConversationAuthoritySnapshotSchema.parse({
    conversationId: conversation.id,
    conversationStatus: conversation.status,
    authorityVersion: Number(conversation.authority_version),
    participantEpochId: conversation.current_epoch_id,
    participantSetDigest: conversation.participant_set_digest,
    participants: participants.map((participant) => ({
      personIdentityId: participant.person_identity_id,
      personId: participant.person_id,
      registrationStatus: participant.registration_status,
      consentedAt: participant.consented_at?.toISOString() ?? null,
      proactivePaused: proactivePausedFromQuietHours(participant.quiet_hours),
      policy:
        participant.allow_content_processing === null
          ? null
          : {
              allowContentProcessing: participant.allow_content_processing,
              allowDirectResponses: participant.allow_direct_responses,
              allowProactiveWrites: participant.allow_proactive_writes,
              retentionSeconds: participant.retention_seconds,
            },
    })),
    activeSuppressions: suppressions.map((suppression) => ({
      id: suppression.id,
      kind: suppression.kind,
      retentionSeconds: suppression.retention_seconds,
    })),
    rules: rules.map((rule) => ({
      ruleId: rule.id,
      ruleKey: rule.rule_key,
      participantSetDigest: rule.participant_set_digest,
      allowedOperations: rule.allowed_operations,
      active: true,
    })),
  });
}

function proactivePausedFromQuietHours(candidate: unknown): boolean {
  if (!isRecord(candidate)) return true;
  if (!("proactivePaused" in candidate)) return false;
  return typeof candidate.proactivePaused === "boolean" ? candidate.proactivePaused : true;
}

function isRecord(candidate: unknown): candidate is Record<string, unknown> {
  return typeof candidate === "object" && candidate !== null && !Array.isArray(candidate);
}

async function loadActivePolicy(
  transaction: Transaction,
  conversationId: string,
  personId: string,
): Promise<ParticipantPolicyValue | null> {
  const rows = await transaction<
    {
      readonly allow_content_processing: boolean;
      readonly allow_direct_responses: boolean;
      readonly allow_proactive_writes: boolean;
      readonly retention_seconds: number;
    }[]
  >`
    select allow_content_processing, allow_direct_responses, allow_proactive_writes, retention_seconds
    from participant_policies
    where conversation_id = ${conversationId} and person_id = ${personId} and status = 'active'
  `;
  const row = rows[0];
  return row
    ? {
        allowContentProcessing: row.allow_content_processing,
        allowDirectResponses: row.allow_direct_responses,
        allowProactiveWrites: row.allow_proactive_writes,
        retentionSeconds: row.retention_seconds,
      }
    : null;
}

async function insertPolicy(
  transaction: Transaction,
  input: {
    readonly conversationId: string;
    readonly personId: string;
    readonly actorPersonId: string;
    readonly version: number;
    readonly policy: ParticipantPolicyValue;
    readonly approvalDigest: string | null;
    readonly changedAt: Date;
  },
) {
  await transaction`
    insert into participant_policies (
      id, conversation_id, person_id, version, status, allow_content_processing,
      allow_direct_responses, allow_proactive_writes, retention_seconds,
      changed_by_person_id, approval_participant_digest, effective_at
    ) values (
      ${randomUUID()}, ${input.conversationId}, ${input.personId}, ${input.version}, 'active',
      ${input.policy.allowContentProcessing}, ${input.policy.allowDirectResponses},
      ${input.policy.allowProactiveWrites}, ${input.policy.retentionSeconds},
      ${input.actorPersonId}, ${input.approvalDigest}, ${input.changedAt}
    )
  `;
}

async function lockConversation(transaction: Transaction, conversationId: string) {
  const rows = await transaction<{ readonly authority_version: number | string }[]>`
    select authority_version from conversations where id = ${conversationId} for update
  `;
  if (!rows[0]) throw new NotFoundError("Conversation does not exist");
  return Number(rows[0].authority_version);
}

async function lockConversationVersion(
  transaction: Transaction,
  conversationId: string,
  expectedVersion: number,
) {
  const current = await lockConversation(transaction, conversationId);
  if (current !== expectedVersion) throw new StaleAuthorityError("Conversation authority version changed");
}

async function incrementAuthority(transaction: Transaction, conversationId: string, changedAt: Date) {
  await transaction`
    update conversations set authority_version = authority_version + 1, updated_at = ${changedAt}
    where id = ${conversationId}
  `;
}

async function requireCurrentParticipant(transaction: Transaction, conversationId: string, personId: string) {
  const rows = await transaction<{ readonly person_id: string }[]>`
    select participant.person_id
    from conversations conversation
    join epoch_participants participant on participant.participant_epoch_id = conversation.current_epoch_id
    where conversation.id = ${conversationId} and participant.person_id = ${personId}
  `;
  if (!rows[0]) throw new UnauthorizedError("Actor is not a current participant");
}

function requireExactApprovals(currentPersonIds: readonly string[], approvedPersonIds: readonly string[]) {
  if (
    new Set(approvedPersonIds).size !== approvedPersonIds.length ||
    participantApprovalDigest(currentPersonIds) !== participantApprovalDigest(approvedPersonIds)
  ) {
    throw new UnauthorizedError("Widening requires every exact current participant");
  }
}

function postgresErrorCode(error: unknown): string | undefined {
  return typeof error === "object" && error !== null && "code" in error
    ? String((error as { readonly code?: unknown }).code)
    : undefined;
}
