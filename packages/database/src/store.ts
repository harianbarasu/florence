import { createHash, randomUUID } from "node:crypto";
import type {
  AcceptanceReceipt,
  ConversationSnapshot,
  GmailCalendarDraft,
  HouseholdSignal,
  WorkerProposal,
} from "@florence/contracts";
import {
  adultEnrollmentRedeemedSignalSchema,
  conversationBoundSignalSchema,
  conversationMessageSignalSchema,
  gmailCalendarDraftSchema,
} from "@florence/contracts";
import type {
  ClaimedSignal,
  FlorenceRepository,
  HouseholdCommit,
  HouseholdDomainEvent,
  LinqEnrollmentRedemptionReceipt,
  LinqEnrollmentRedemptionRequest,
  LinqGroupBootstrapReceipt,
  LinqGroupBootstrapRequest,
  PersistedHouseholdEvent,
} from "@florence/control-plane";
import type {
  ActiveGoogleCredential,
  ClaimedGmailSync,
  GoogleConnectionStore,
  GoogleConnectionView,
  GoogleScope,
  PendingGoogleConnection,
} from "@florence/google";
import { linqIdentitySubjectDigest } from "@florence/linq";
import postgres from "postgres";

export { linqIdentitySubjectDigest } from "@florence/linq";

export type PostgresFlorenceRepositoryOptions = {
  connectionString: string;
  schema: string;
  applicationName?: string;
  maxConnections?: number;
  ssl?: boolean;
};

export type WorkerLease = {
  heartbeat(): Promise<void>;
  release(): Promise<void>;
};

export type EncryptedImageAssetRecord = {
  assetId: string;
  householdId: string;
  signalId: string;
  expiresAt: string;
  envelope: Uint8Array;
};

export class SignalConflictError extends Error {
  constructor(key: string) {
    super(`Signal idempotency key was reused with different content: ${key}`);
    this.name = "SignalConflictError";
  }
}

export class HouseholdConcurrencyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "HouseholdConcurrencyError";
  }
}

export class DeliberationConflictError extends Error {
  constructor(signalId: string) {
    super(`Signal cognition is already fixed to different evidence: ${signalId}`);
    this.name = "DeliberationConflictError";
  }
}

export type PersistedDeliberation = {
  inputDigest: string;
  proposals: readonly WorkerProposal[];
};

export type DueTimer = {
  id: string;
  householdId: string;
  episodeId: string;
  episodeVersion: number;
  scheduledFor: string;
};

export type ClaimedConversationEffect = {
  id: string;
  householdId: string;
  idempotencyKey: string;
  kind: "conversation.message";
  conversationId: string;
  conversationAuthorityVersion: number;
  participantSetDigest: string;
  providerConversationId: string | null;
  audience: "private" | "group" | null;
  expectedParticipantIdentityDigests: readonly string[] | null;
  episodeId: string | null;
  payload: { text: string };
  occurredAt: string;
  attempt: number;
  leaseOwner: string;
};

export type ClaimedCalendarEffect = {
  id: string;
  householdId: string;
  idempotencyKey: string;
  kind: "google.calendar.create";
  connectionId: string;
  ownerAdultId: string;
  actionId: string;
  approvalDigest: string;
  candidateId: string;
  candidateVersion: 1;
  candidateDigest: string;
  episodeId: null;
  payload: GmailCalendarDraft;
  occurredAt: string;
  attempt: number;
  leaseOwner: string;
};

export type ClaimedEffect = ClaimedConversationEffect | ClaimedCalendarEffect;

type StoredCalendarEffectPayload = {
  connectionId: string;
  ownerAdultId: string;
  actionId: string;
  approvalDigest: string;
  candidateId: string;
  candidateVersion: 1;
  candidateDigest: string;
  calendar: GmailCalendarDraft;
};

export type LinqIngressAuthority = {
  householdId: string;
  conversationId: string;
  audience: "private" | "group";
  authorityVersion: number;
  participantSetDigest: string;
  expectedParticipantIdentityDigests: readonly string[];
  senderAdultId: string;
  replyToSignalId: string | null;
};

type SignalRow = {
  signal_id: string;
  household_id: string;
  idempotency_key: string;
  payload: HouseholdSignal;
  status: string;
  attempt_count: number;
  accepted_at: Date;
};

type GoogleConnectionRow = {
  connection_id: string;
  household_id: string;
  owner_adult_id: string;
  status: "pending" | "active" | "disconnected";
  state_digest: string;
  session_binding_digest: string | null;
  state_expires_at: Date;
  state_consumed_at: Date | null;
  google_subject_digest: string | null;
  email_label: string | null;
  granted_scopes: GoogleScope[];
  refresh_token_envelope: string | null;
  gmail_cursor: string | null;
  sync_available_at: Date;
  sync_lease_owner: string | null;
  sync_lease_until: Date | null;
  last_error: string | null;
  created_at: Date;
  updated_at: Date;
};

export class PostgresFlorenceRepository implements FlorenceRepository, GoogleConnectionStore {
  readonly #sql: ReturnType<typeof postgres>;
  readonly #schema: string;
  #closed = false;

  constructor(options: PostgresFlorenceRepositoryOptions) {
    const {
      connectionString,
      schema,
      applicationName = "florence",
      maxConnections = 10,
      ssl = false,
    } = options;
    this.#schema = validatedSchema(schema);
    this.#sql = postgres(connectionString, {
      max: maxConnections,
      idle_timeout: 20,
      connect_timeout: 15,
      ssl: ssl ? "require" : false,
      connection: {
        application_name: applicationName,
        search_path: this.#schema,
        statement_timeout: 30_000,
        lock_timeout: 5_000,
        idle_in_transaction_session_timeout: 30_000,
        TimeZone: "UTC",
      },
      onnotice: () => undefined,
    });
  }

  async ready(): Promise<void> {
    const [migration] = await this.#sql<{ present: boolean }[]>`
      select exists (
        select 1 from schema_migrations where name = '007_household_signal_core.sql'
      ) as present
    `;
    if (!migration?.present) throw new Error("Florence household schema is not current");
    await this.#sql`select 1 from household_streams limit 1`;
  }

  async acquireWorkerLease(input: { workerId: string }): Promise<WorkerLease | null> {
    const workerId = input.workerId.trim();
    if (!workerId || workerId.length > 200) throw new Error("Invalid Florence worker ID");
    const connection = await this.#sql.reserve();
    const releaseId = randomUUID();
    let released = false;
    try {
      const [lock] = await connection<{ acquired: boolean }[]>`
        select pg_try_advisory_lock(4607346623) as acquired
      `;
      if (!lock?.acquired) {
        connection.release();
        return null;
      }
      await connection`
        insert into worker_leases (
          lease_name, worker_id, release_id, started_at, last_seen_at, stopped_at
        ) values (
          'florence-worker-singleton', ${workerId}, ${releaseId},
          clock_timestamp(), clock_timestamp(), null
        )
        on conflict (lease_name) do update
        set worker_id = excluded.worker_id,
            release_id = excluded.release_id,
            started_at = excluded.started_at,
            last_seen_at = excluded.last_seen_at,
            stopped_at = null
      `;
    } catch (error) {
      connection.release();
      throw error;
    }

    return {
      heartbeat: async () => {
        if (released) throw new Error("Florence worker lease is released");
        const rows = await connection`
          update worker_leases
          set last_seen_at = clock_timestamp()
          where lease_name = 'florence-worker-singleton'
            and release_id = ${releaseId}
            and stopped_at is null
          returning lease_name
        `;
        if (rows.length !== 1) throw new Error("Florence worker lease is no longer current");
      },
      release: async () => {
        if (released) return;
        released = true;
        try {
          await connection`
            update worker_leases
            set last_seen_at = clock_timestamp(), stopped_at = clock_timestamp()
            where lease_name = 'florence-worker-singleton'
              and release_id = ${releaseId}
              and stopped_at is null
          `;
          await connection`select pg_advisory_unlock(4607346623)`;
        } finally {
          connection.release();
        }
      },
    };
  }

  async readImageAsset(assetId: string): Promise<EncryptedImageAssetRecord | null> {
    const [row] = await this.#sql<
      {
        asset_id: string;
        household_id: string;
        signal_id: string;
        expires_at: Date;
        envelope: Uint8Array;
      }[]
    >`
      select asset_id, household_id, signal_id, expires_at, envelope
      from image_assets
      where asset_id = ${assetId}
    `;
    return row
      ? {
          assetId: row.asset_id,
          householdId: row.household_id,
          signalId: row.signal_id,
          expiresAt: row.expires_at.toISOString(),
          envelope: Uint8Array.from(row.envelope),
        }
      : null;
  }

  async insertImageAsset(record: EncryptedImageAssetRecord): Promise<boolean> {
    const inserted = await this.#sql`
      insert into image_assets (
        asset_id, household_id, signal_id, expires_at, envelope
      ) values (
        ${record.assetId}, ${record.householdId}, ${record.signalId},
        ${new Date(record.expiresAt)}, ${record.envelope}
      )
      on conflict (asset_id) do nothing
      returning asset_id
    `;
    return inserted.length === 1;
  }

  async deleteImageAsset(assetId: string): Promise<void> {
    await this.#sql`delete from image_assets where asset_id = ${assetId}`;
  }

  async deleteExpiredImageAssets(now: Date): Promise<number> {
    const deleted = await this.#sql`
      delete from image_assets where expires_at <= ${now} returning asset_id
    `;
    return deleted.length;
  }

  async accept(signal: HouseholdSignal, acceptedAt: string): Promise<AcceptanceReceipt> {
    return this.#sql.begin(async (sql) => {
      await sql`select pg_advisory_xact_lock(hashtextextended(${signal.idempotencyKey}, 0))`;
      const [existing] = await sql<SignalRow[]>`
        select signal_id, household_id, idempotency_key, payload, status, attempt_count, accepted_at
        from household_signals
        where idempotency_key = ${signal.idempotencyKey}
      `;
      if (existing) {
        if (!sameJson(existing.payload, signal)) throw new SignalConflictError(signal.idempotencyKey);
        return {
          signalId: existing.signal_id,
          householdId: existing.household_id,
          disposition: "duplicate",
          acceptedAt: existing.accepted_at.toISOString(),
        };
      }

      const accepted = new Date(acceptedAt);
      await sql`
        insert into household_streams (household_id)
        values (${signal.householdId})
        on conflict (household_id) do nothing
      `;
      await sql`
        insert into household_signals (
          signal_id, household_id, idempotency_key, type, payload, status, available_at, accepted_at
        ) values (
          ${signal.signalId}, ${signal.householdId}, ${signal.idempotencyKey}, ${signal.type},
          ${sql.json(signal)}, 'pending', ${accepted}, ${accepted}
        )
      `;
      return {
        signalId: signal.signalId,
        householdId: signal.householdId,
        disposition: "accepted",
        acceptedAt: accepted.toISOString(),
      };
    });
  }

  async redeemLinqEnrollment(
    input: LinqEnrollmentRedemptionRequest,
    acceptedAt: string,
  ): Promise<LinqEnrollmentRedemptionReceipt | null> {
    return this.#sql.begin(async (sql) => {
      await sql`select pg_advisory_xact_lock(hashtextextended(${input.challengeDigest}, 0))`;
      await sql`select pg_advisory_xact_lock(hashtextextended(${input.identitySubjectDigest}, 2))`;
      await sql`select pg_advisory_xact_lock(hashtextextended(${input.providerConversationId}, 1))`;

      const existingRows = await sql<{ household_id: string; payload: HouseholdSignal; accepted_at: Date }[]>`
        select household_id, payload, accepted_at
        from household_signals
        where signal_id = ${input.signalId} or idempotency_key = ${input.idempotencyKey}
        limit 2
      `;
      if (existingRows.length > 1) throw new SignalConflictError(input.idempotencyKey);
      const existing = existingRows[0];
      if (existing) {
        const payload = existing.payload;
        if (
          payload.type !== "adult.enrollment.redeemed" ||
          payload.signalId !== input.signalId ||
          payload.idempotencyKey !== input.idempotencyKey ||
          payload.occurredAt !== input.occurredAt ||
          payload.challengeDigest !== input.challengeDigest ||
          payload.identitySubjectDigest !== input.identitySubjectDigest ||
          payload.consentVersion !== input.consentVersion ||
          payload.consentedAt !== input.consentedAt ||
          payload.providerConversationId !== input.providerConversationId
        ) {
          throw new SignalConflictError(input.idempotencyKey);
        }
        return {
          signalId: payload.signalId,
          householdId: payload.householdId,
          disposition: "duplicate",
          acceptedAt: existing.accepted_at.toISOString(),
          adultId: payload.adultId,
          conversationId: payload.conversationId,
        };
      }

      const candidates = await sql<{ household_id: string; adult_id: string; issued_accepted_at: Date }[]>`
        with latest_issues as (
          select distinct on (household_id, payload ->> 'adultId')
                 household_id, payload, accepted_at, signal_id
          from household_signals
          where type = 'adult.enrollment.issued' and status <> 'dead'
          order by household_id, payload ->> 'adultId', accepted_at desc, signal_id desc
        )
        select issue.household_id, issue.payload ->> 'adultId' as adult_id,
               issue.accepted_at as issued_accepted_at
        from latest_issues issue
        where issue.payload ->> 'challengeDigest' = ${input.challengeDigest}
          and (issue.payload ->> 'expiresAt')::timestamptz >= ${new Date(input.occurredAt)}
          and not exists (
            select 1 from household_events verified
            where verified.household_id = issue.household_id
              and verified.type = 'adult.verified'
              and verified.payload ->> 'adultId' = issue.payload ->> 'adultId'
          )
          and not exists (
            select 1 from household_signals redeemed
            where redeemed.type = 'adult.enrollment.redeemed'
              and redeemed.status <> 'dead'
              and redeemed.household_id = issue.household_id
              and redeemed.payload ->> 'adultId' = issue.payload ->> 'adultId'
          )
          and not exists (
            select 1 from household_events identity
            where identity.type = 'adult.verified'
              and identity.payload ->> 'identitySubjectDigest' = ${input.identitySubjectDigest}
          )
          and not exists (
            select 1 from household_signals identity
            where identity.type = 'adult.enrollment.redeemed'
              and identity.status <> 'dead'
              and identity.payload ->> 'identitySubjectDigest' = ${input.identitySubjectDigest}
          )
          and not exists (
            select 1 from household_events binding
            where binding.type = 'conversation.bound'
              and binding.payload -> 'conversation' ->> 'providerConversationId' =
                ${input.providerConversationId}
          )
          and not exists (
            select 1 from household_signals redemption
            where redemption.type = 'adult.enrollment.redeemed'
              and redemption.status <> 'dead'
              and redemption.payload ->> 'providerConversationId' = ${input.providerConversationId}
          )
        limit 2
      `;
      if (candidates.length !== 1) return null;
      const candidate = candidates[0];
      if (!candidate) return null;

      const signal = adultEnrollmentRedeemedSignalSchema.parse({
        type: "adult.enrollment.redeemed",
        signalId: input.signalId,
        householdId: candidate.household_id,
        idempotencyKey: input.idempotencyKey,
        occurredAt: input.occurredAt,
        adultId: candidate.adult_id,
        challengeDigest: input.challengeDigest,
        identitySubjectDigest: input.identitySubjectDigest,
        consentVersion: input.consentVersion,
        consentedAt: input.consentedAt,
        conversationId: deterministicUuid(`linq-v3\0private\0${input.providerConversationId}`),
        providerConversationId: input.providerConversationId,
      });
      const requestedAcceptedAt = new Date(acceptedAt);
      const accepted = new Date(
        Math.max(requestedAcceptedAt.getTime(), candidate.issued_accepted_at.getTime() + 1),
      );
      await sql`
        insert into household_streams (household_id)
        values (${signal.householdId})
        on conflict (household_id) do nothing
      `;
      await sql`
        insert into household_signals (
          signal_id, household_id, idempotency_key, type, payload, status, available_at, accepted_at
        ) values (
          ${signal.signalId}, ${signal.householdId}, ${signal.idempotencyKey}, ${signal.type},
          ${sql.json(signal)}, 'pending', ${requestedAcceptedAt}, ${accepted}
        )
      `;
      return {
        signalId: signal.signalId,
        householdId: signal.householdId,
        disposition: "accepted",
        acceptedAt: accepted.toISOString(),
        adultId: signal.adultId,
        conversationId: signal.conversationId,
      };
    });
  }

  async bootstrapLinqHouseholdGroup(
    input: LinqGroupBootstrapRequest,
    acceptedAt: string,
  ): Promise<LinqGroupBootstrapReceipt | null> {
    const participantIdentityDigests = [...input.participantIdentityDigests].sort();
    if (
      participantIdentityDigests.length !== 2 ||
      new Set(participantIdentityDigests).size !== 2 ||
      !participantIdentityDigests.every((digest) => /^[a-f0-9]{64}$/.test(digest)) ||
      !participantIdentityDigests.includes(input.senderIdentitySubjectDigest)
    ) {
      return null;
    }

    return this.#sql.begin(async (sql) => {
      await sql`select pg_advisory_xact_lock(hashtextextended(${input.providerConversationId}, 3))`;
      await sql`select pg_advisory_xact_lock(hashtextextended(${input.messageIdempotencyKey}, 0))`;

      const candidates = await sql<
        { household_id: string; adult_ids: string[]; sender_adult_id: string; steward_adult_id: string }[]
      >`
        with identity_sets as (
          select event.household_id,
                 jsonb_agg(event.payload ->> 'identitySubjectDigest'
                           order by event.payload ->> 'identitySubjectDigest') as identity_digests,
                 array_agg(event.payload ->> 'adultId' order by event.payload ->> 'adultId') as adult_ids,
                 min(event.payload ->> 'adultId') filter (
                   where event.payload ->> 'identitySubjectDigest' = ${input.senderIdentitySubjectDigest}
                 ) as sender_adult_id
          from household_events event
          where event.type = 'adult.verified'
            and (event.payload ->> 'consentedAt')::timestamptz <= ${new Date(input.occurredAt)}
          group by event.household_id
          having count(*) = 2
             and count(distinct event.payload ->> 'adultId') = 2
             and count(distinct event.payload ->> 'identitySubjectDigest') = 2
        ), member_facts as (
          select event.household_id, event.version,
                 event.payload -> 'foundingAdult' ->> 'id' as adult_id,
                 event.payload -> 'foundingAdult' ->> 'role' as role
          from household_events event where event.type = 'household.created'
          union all
          select event.household_id, event.version,
                 event.payload -> 'member' ->> 'id' as adult_id,
                 event.payload -> 'member' ->> 'role' as role
          from household_events event where event.type = 'family.member.upserted'
        ), latest_members as (
          select distinct on (household_id, adult_id) household_id, adult_id, role
          from member_facts
          order by household_id, adult_id, version desc
        )
        select identities.household_id, identities.adult_ids, identities.sender_adult_id,
               steward.adult_id as steward_adult_id
        from identity_sets identities
        join lateral (
          select member.adult_id
          from latest_members member
          where member.household_id = identities.household_id
            and member.role = 'steward'
            and member.adult_id = any(identities.adult_ids)
          order by member.adult_id
          limit 1
        ) steward on true
        where identities.identity_digests = ${sql.json(participantIdentityDigests)}
          and identities.sender_adult_id is not null
          and not exists (
            select 1 from household_events binding
            where binding.type = 'conversation.bound'
              and binding.signal_id <> ${input.bindingSignalId}
              and binding.payload -> 'conversation' ->> 'providerConversationId' =
                  ${input.providerConversationId}
          )
          and not exists (
            select 1 from household_events household_group
            where household_group.household_id = identities.household_id
              and household_group.type = 'conversation.bound'
              and household_group.signal_id <> ${input.bindingSignalId}
              and household_group.payload -> 'conversation' ->> 'audience' = 'group'
          )
          and not exists (
            select 1 from household_signals binding
            where binding.type in ('conversation.bound', 'adult.enrollment.redeemed')
              and binding.status <> 'dead'
              and binding.signal_id <> ${input.bindingSignalId}
              and coalesce(
                binding.payload ->> 'providerConversationId',
                binding.payload -> 'conversation' ->> 'providerConversationId'
              ) = ${input.providerConversationId}
          )
          and not exists (
            select 1 from household_signals household_group
            where household_group.household_id = identities.household_id
              and household_group.type = 'conversation.bound'
              and household_group.status <> 'dead'
              and household_group.signal_id <> ${input.bindingSignalId}
              and household_group.payload ->> 'audience' = 'group'
          )
        limit 2
      `;
      if (candidates.length !== 1) return null;
      const candidate = candidates[0];
      if (!candidate) return null;

      const conversationId = deterministicUuid(`linq-v3\0group\0${input.providerConversationId}`);
      const authorizedAdultIds = [...candidate.adult_ids].sort();
      const participantSetDigest = digestStrings(authorizedAdultIds);
      const existingBindings = await sql<SignalRow[]>`
        select signal_id, household_id, idempotency_key, payload, status, attempt_count, accepted_at
        from household_signals
        where signal_id = ${input.bindingSignalId} or idempotency_key = ${input.bindingIdempotencyKey}
        limit 2
      `;
      if (existingBindings.length > 1) throw new SignalConflictError(input.bindingIdempotencyKey);
      const existingBinding = existingBindings[0];
      if (
        existingBinding &&
        !sameGroupBinding(existingBinding.payload, {
          ...input,
          householdId: candidate.household_id,
          conversationId,
          actorAdultId: candidate.steward_adult_id,
          authorizedAdultIds,
          participantSetDigest,
        })
      ) {
        throw new SignalConflictError(input.bindingIdempotencyKey);
      }

      const binding = conversationBoundSignalSchema.parse({
        type: "conversation.bound",
        signalId: input.bindingSignalId,
        householdId: candidate.household_id,
        idempotencyKey: input.bindingIdempotencyKey,
        occurredAt: input.occurredAt,
        actorAdultId: candidate.steward_adult_id,
        conversationId,
        audience: "group",
        authorityVersion: 1,
        participantSetDigest,
        authorizedAdultIds,
        providerConversationId: input.providerConversationId,
      });
      const message = conversationMessageSignalSchema.parse({
        type: "conversation.message",
        signalId: input.messageSignalId,
        householdId: candidate.household_id,
        idempotencyKey: input.messageIdempotencyKey,
        occurredAt: input.occurredAt,
        conversationId,
        audience: "group",
        authorityVersion: 1,
        participantSetDigest,
        senderAdultId: candidate.sender_adult_id,
        text: input.text,
        images: [],
        replyToSignalId: null,
        source: {
          system: "linq-v3",
          providerEventId: input.providerEventId,
          providerMessageId: input.providerMessageId,
        },
      });
      const existingMessages = await sql<SignalRow[]>`
        select signal_id, household_id, idempotency_key, payload, status, attempt_count, accepted_at
        from household_signals
        where signal_id = ${message.signalId} or idempotency_key = ${message.idempotencyKey}
        limit 2
      `;
      if (existingMessages.length > 1) throw new SignalConflictError(message.idempotencyKey);
      const existingMessage = existingMessages[0];
      if (existingMessage) {
        if (!sameJson(existingMessage.payload, message))
          throw new SignalConflictError(message.idempotencyKey);
        if (!existingBinding) throw new SignalConflictError(input.bindingIdempotencyKey);
        return {
          signalId: message.signalId,
          householdId: message.householdId,
          disposition: "duplicate",
          acceptedAt: existingMessage.accepted_at.toISOString(),
          conversationId,
        };
      }

      const requestedAcceptedAt = new Date(acceptedAt);
      const [latest] = await sql<{ accepted_at: Date | null }[]>`
        select max(accepted_at) as accepted_at from household_signals
        where household_id = ${message.householdId}
      `;
      const latestAcceptedAt = latest?.accepted_at?.getTime() ?? Number.NEGATIVE_INFINITY;
      const bindingAcceptedAt =
        existingBinding?.accepted_at ??
        new Date(Math.max(requestedAcceptedAt.getTime(), latestAcceptedAt + 1));
      const messageAcceptedAt = new Date(
        Math.max(requestedAcceptedAt.getTime(), latestAcceptedAt + 1, bindingAcceptedAt.getTime() + 1),
      );
      await sql`
        insert into household_streams (household_id) values (${message.householdId})
        on conflict (household_id) do nothing
      `;
      if (!existingBinding) {
        await sql`
          insert into household_signals (
            signal_id, household_id, idempotency_key, type, payload, status, available_at, accepted_at
          ) values (
            ${binding.signalId}, ${binding.householdId}, ${binding.idempotencyKey}, ${binding.type},
            ${sql.json(binding)}, 'pending', ${requestedAcceptedAt}, ${bindingAcceptedAt}
          )
        `;
      }
      await sql`
        insert into household_signals (
          signal_id, household_id, idempotency_key, type, payload, status, available_at, accepted_at
        ) values (
          ${message.signalId}, ${message.householdId}, ${message.idempotencyKey}, ${message.type},
          ${sql.json(message)}, 'pending', ${requestedAcceptedAt}, ${messageAcceptedAt}
        )
      `;
      return {
        signalId: message.signalId,
        householdId: message.householdId,
        disposition: "accepted",
        acceptedAt: messageAcceptedAt.toISOString(),
        conversationId,
      };
    });
  }

  async createPending(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
    stateDigest: string;
    sessionBindingDigest: string;
    stateExpiresAt: string;
    now: string;
  }): Promise<GoogleConnectionView> {
    const [row] = await this.#sql<GoogleConnectionRow[]>`
      insert into google_connections (
        connection_id, household_id, owner_adult_id, status, state_digest,
        session_binding_digest, state_expires_at,
        sync_available_at, created_at, updated_at
      ) values (
        ${input.connectionId}, ${input.householdId}, ${input.ownerAdultId}, 'pending',
        ${input.stateDigest}, ${input.sessionBindingDigest}, ${new Date(input.stateExpiresAt)},
        ${new Date(input.now)},
        ${new Date(input.now)}, ${new Date(input.now)}
      ) returning *
    `;
    if (!row) throw new Error("Google connection was not created");
    return googleConnectionView(row);
  }

  async consumePendingState(input: {
    stateDigest: string;
    sessionBindingDigest: string;
    now: string;
  }): Promise<PendingGoogleConnection | null> {
    const [row] = await this.#sql<GoogleConnectionRow[]>`
      update google_connections
      set state_consumed_at = ${new Date(input.now)}, updated_at = ${new Date(input.now)}
      where status = 'pending'
        and state_digest = ${input.stateDigest}
        and session_binding_digest = ${input.sessionBindingDigest}
        and state_consumed_at is null
        and state_expires_at >= ${new Date(input.now)}
      returning *
    `;
    return row
      ? {
          connectionId: row.connection_id,
          householdId: row.household_id,
          ownerAdultId: row.owner_adult_id,
          stateDigest: row.state_digest,
          sessionBindingDigest: row.session_binding_digest as string,
        }
      : null;
  }

  async activate(input: {
    connectionId: string;
    stateDigest: string;
    googleSubjectDigest: string;
    emailLabel: string;
    grantedScopes: readonly GoogleScope[];
    refreshTokenEnvelope: string;
    now: string;
  }): Promise<GoogleConnectionView> {
    const [row] = await this.#sql<GoogleConnectionRow[]>`
      update google_connections
      set status = 'active', google_subject_digest = ${input.googleSubjectDigest},
          email_label = ${input.emailLabel}, granted_scopes = ${this.#sql.json(input.grantedScopes)},
          refresh_token_envelope = ${input.refreshTokenEnvelope}, last_error = null,
          session_binding_digest = null,
          sync_available_at = ${new Date(input.now)}, updated_at = ${new Date(input.now)}
      where connection_id = ${input.connectionId}
        and state_digest = ${input.stateDigest}
        and state_consumed_at is not null
        and status = 'pending'
      returning *
    `;
    if (!row) throw new Error("Google OAuth state is no longer current");
    return googleConnectionView(row);
  }

  async markPendingFailure(input: {
    connectionId: string;
    stateDigest: string;
    error: string;
    now: string;
  }): Promise<void> {
    await this.#sql`
      update google_connections
      set last_error = ${input.error}, updated_at = ${new Date(input.now)}
      where connection_id = ${input.connectionId}
        and state_digest = ${input.stateDigest}
        and status = 'pending'
    `;
  }

  async listActive(input: {
    householdId: string;
    ownerAdultId: string;
  }): Promise<readonly GoogleConnectionView[]> {
    const rows = await this.#sql<GoogleConnectionRow[]>`
      select * from google_connections
      where household_id = ${input.householdId}
        and owner_adult_id = ${input.ownerAdultId}
        and status = 'active'
      order by created_at, connection_id
    `;
    return rows.map(googleConnectionView);
  }

  async disconnect(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
    now: string;
  }): Promise<{ view: GoogleConnectionView; refreshTokenEnvelope: string | null } | null> {
    return this.#sql.begin(async (sql) => {
      const [current] = await sql<GoogleConnectionRow[]>`
        select * from google_connections
        where connection_id = ${input.connectionId}
          and household_id = ${input.householdId}
          and owner_adult_id = ${input.ownerAdultId}
          and status <> 'disconnected'
        for update
      `;
      if (!current) return null;
      const [row] = await sql<GoogleConnectionRow[]>`
        update google_connections
        set status = 'disconnected', refresh_token_envelope = null,
            sync_lease_owner = null, sync_lease_until = null, updated_at = ${new Date(input.now)}
        where connection_id = ${input.connectionId}
        returning *
      `;
      if (!row) throw new Error("Google connection was not disconnected");
      return {
        view: googleConnectionView(row),
        refreshTokenEnvelope: current.refresh_token_envelope,
      };
    });
  }

  async readActiveGoogleCredential(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
  }): Promise<ActiveGoogleCredential | null> {
    const [row] = await this.#sql<GoogleConnectionRow[]>`
      select * from google_connections
      where connection_id = ${input.connectionId}
        and household_id = ${input.householdId}
        and owner_adult_id = ${input.ownerAdultId}
        and status = 'active'
    `;
    return row ? activeGoogleCredential(row) : null;
  }

  async claimNextGmailSync(input: {
    owner: string;
    now: string;
    leaseUntil: string;
  }): Promise<ClaimedGmailSync | null> {
    return this.#sql.begin(async (sql) => {
      const [row] = await sql<GoogleConnectionRow[]>`
        select * from google_connections
        where status = 'active'
          and sync_available_at <= ${new Date(input.now)}
          and (sync_lease_until is null or sync_lease_until <= ${new Date(input.now)})
        order by sync_available_at, created_at, connection_id
        for update skip locked
        limit 1
      `;
      if (!row) return null;
      await sql`
        update google_connections
        set sync_lease_owner = ${input.owner}, sync_lease_until = ${new Date(input.leaseUntil)},
            updated_at = ${new Date(input.now)}
        where connection_id = ${row.connection_id}
      `;
      return { ...activeGoogleCredential(row), leaseOwner: input.owner };
    });
  }

  async releaseGmailSync(input: {
    connectionId: string;
    owner: string;
    nextAt: string;
    cursor?: string;
    error?: string | null;
  }): Promise<void> {
    const [released] = await this.#sql<{ connection_id: string }[]>`
      update google_connections
      set gmail_cursor = coalesce(${input.cursor ?? null}, gmail_cursor),
          sync_available_at = ${new Date(input.nextAt)}, sync_lease_owner = null,
          sync_lease_until = null, last_error = ${input.error ?? null}, updated_at = now()
      where connection_id = ${input.connectionId}
        and status = 'active'
        and sync_lease_owner = ${input.owner}
      returning connection_id
    `;
    if (!released) throw new HouseholdConcurrencyError("The Gmail sync claim is no longer current");
  }

  async listHouseholdIdsForAdult(adultId: string): Promise<readonly string[]> {
    const rows = await this.#sql<{ household_id: string }[]>`
      with member_facts as (
        select event.household_id, event.version,
               event.payload -> 'foundingAdult' ->> 'id' as adult_id,
               event.payload -> 'foundingAdult' ->> 'status' as status
        from household_events event where event.type = 'household.created'
        union all
        select event.household_id, event.version,
               event.payload -> 'plannedAdult' ->> 'id' as adult_id,
               event.payload -> 'plannedAdult' ->> 'status' as status
        from household_events event
        where event.type = 'household.created'
          and event.payload ? 'plannedAdult'
        union all
        select event.household_id, event.version,
               event.payload -> 'member' ->> 'id' as adult_id,
               event.payload -> 'member' ->> 'status' as status
        from household_events event where event.type = 'family.member.upserted'
      ), latest_member as (
        select distinct on (household_id, adult_id) household_id, adult_id, status
        from member_facts
        order by household_id, adult_id, version desc
      )
      select member.household_id
      from latest_member member
      where member.adult_id = ${adultId}
        and (
          member.status = 'verified'
          or exists (
            select 1 from household_events verification
            where verification.household_id = member.household_id
              and verification.type = 'adult.verified'
              and verification.payload ->> 'adultId' = member.adult_id
          )
        )
      order by member.household_id
    `;
    return rows.map((row) => row.household_id);
  }

  async claimNext(input: {
    leaseOwner: string;
    now: string;
    leaseUntil: string;
  }): Promise<ClaimedSignal | null> {
    const now = new Date(input.now);
    const leaseUntil = new Date(input.leaseUntil);
    if (leaseUntil <= now) throw new Error("A signal lease must end after it starts");

    return this.#sql.begin(async (sql) => {
      const [row] = await sql<SignalRow[]>`
        select s.signal_id, s.household_id, s.idempotency_key, s.payload, s.status,
               s.attempt_count, s.accepted_at
        from household_signals s
        join household_streams h on h.household_id = s.household_id
        where s.status in ('pending', 'retry', 'processing')
          and s.available_at <= ${now}
          and (s.lease_until is null or s.lease_until <= ${now})
          and (h.lease_until is null or h.lease_until <= ${now})
          and not exists (
            select 1
            from household_signals earlier
            where earlier.household_id = s.household_id
              and earlier.status not in ('completed', 'dead')
              and (earlier.accepted_at, earlier.signal_id) < (s.accepted_at, s.signal_id)
          )
        order by s.accepted_at, s.signal_id
        for update of s, h skip locked
        limit 1
      `;
      if (!row) return null;

      await sql`
        update household_streams
        set lease_owner = ${input.leaseOwner}, lease_until = ${leaseUntil}, updated_at = ${now}
        where household_id = ${row.household_id}
      `;
      await sql`
        update household_signals
        set status = 'processing', attempt_count = attempt_count + 1,
            lease_owner = ${input.leaseOwner}, lease_until = ${leaseUntil}, last_error = null
        where signal_id = ${row.signal_id}
      `;
      return { signal: row.payload, attempt: row.attempt_count + 1, leaseOwner: input.leaseOwner };
    });
  }

  async loadEvents(householdId: string): Promise<readonly PersistedHouseholdEvent[]> {
    const rows = await this.#sql<
      {
        id: string;
        household_id: string;
        signal_id: string;
        version: number;
        payload: HouseholdDomainEvent;
      }[]
    >`
      select id, household_id, signal_id, version, payload
      from household_events
      where household_id = ${householdId}
      order by version
    `;
    return rows.map((row) => ({
      ...row.payload,
      id: row.id,
      householdId: row.household_id,
      signalId: row.signal_id,
      version: row.version,
    })) as PersistedHouseholdEvent[];
  }

  async resolveLinqIngressAuthority(input: {
    providerConversationId: string;
    providerHandleId: string;
    replyToProviderMessageId: string | null;
    occurredAt: string;
  }): Promise<LinqIngressAuthority | null> {
    const identitySubjectDigest = linqIdentitySubjectDigest(input.providerHandleId);
    const occurredAt = new Date(input.occurredAt);
    if (Number.isNaN(occurredAt.getTime())) throw new Error("Linq occurredAt must be a timestamp");
    const rows = await this.#sql<
      {
        household_id: string;
        conversation_id: string;
        audience: "private" | "group";
        authority_version: number;
        participant_set_digest: string;
        expected_participant_identity_digests: string[];
        sender_adult_id: string;
        reply_to_signal_id: string | null;
      }[]
    >`
      with latest_bindings as (
        select distinct on (
          event.household_id,
          event.payload -> 'conversation' ->> 'conversationId'
        ) event.household_id, event.payload,
          (binding_signal.payload ->> 'occurredAt')::timestamptz as binding_occurred_at
        from household_events event
        join household_signals binding_signal on binding_signal.signal_id = event.signal_id
        where event.type = 'conversation.bound'
        order by event.household_id,
          event.payload -> 'conversation' ->> 'conversationId',
          event.version desc
      ), authority_candidates as (
        select
          binding.household_id,
          binding.payload -> 'conversation' ->> 'conversationId' as conversation_id,
          binding.payload -> 'conversation' ->> 'audience' as audience,
          (binding.payload -> 'conversation' ->> 'authorityVersion')::integer as authority_version,
          binding.payload -> 'conversation' ->> 'participantSetDigest' as participant_set_digest,
          identities.expected_participant_identity_digests,
          identities.sender_adult_id
        from latest_bindings binding
        join lateral (
          select
            array_agg(
              verification.payload ->> 'identitySubjectDigest'
              order by verification.payload ->> 'identitySubjectDigest'
            ) as expected_participant_identity_digests,
            min(verification.payload ->> 'adultId') filter (
              where verification.payload ->> 'identitySubjectDigest' = ${identitySubjectDigest}
            ) as sender_adult_id,
            count(*) filter (
              where verification.payload ->> 'identitySubjectDigest' = ${identitySubjectDigest}
            ) as sender_matches,
            count(*) as verification_count,
            count(distinct verification.payload ->> 'adultId') as verified_adult_count
          from jsonb_array_elements_text(
            binding.payload -> 'conversation' -> 'authorizedAdultIds'
          ) authorized(adult_id)
          join household_events verification
            on verification.household_id = binding.household_id
            and verification.type = 'adult.verified'
            and verification.payload ->> 'adultId' = authorized.adult_id
            and (verification.payload ->> 'consentedAt')::timestamptz <= ${occurredAt}
        ) identities on
          identities.verification_count = jsonb_array_length(
            binding.payload -> 'conversation' -> 'authorizedAdultIds'
          )
          and identities.verified_adult_count = identities.verification_count
          and identities.sender_matches = 1
        where binding.payload -> 'conversation' ->> 'providerConversationId' =
          ${input.providerConversationId}
          and binding.binding_occurred_at <= ${occurredAt}
      )
      select authority.household_id, authority.conversation_id, authority.audience,
             authority.authority_version, authority.participant_set_digest,
             authority.expected_participant_identity_digests, authority.sender_adult_id,
             reply.reply_id as reply_to_signal_id
      from authority_candidates authority
      left join lateral (
        select candidate.reply_id
        from (
          select inbound.signal_id::text as reply_id
          from household_signals inbound
          where inbound.household_id = authority.household_id
            and inbound.type = 'conversation.message'
            and inbound.status = 'completed'
            and inbound.payload ->> 'conversationId' = authority.conversation_id
            and inbound.payload -> 'source' ->> 'system' = 'linq-v3'
            and inbound.payload -> 'source' ->> 'providerMessageId' =
              ${input.replyToProviderMessageId}
            and (inbound.payload ->> 'occurredAt')::timestamptz <= ${occurredAt}
          union
          select effect.id::text as reply_id
          from outbox_effects effect
          join household_signals receipt
            on receipt.household_id = effect.household_id
            and receipt.type = 'effect.receipt'
            and receipt.payload ->> 'effectId' = effect.id::text
            and receipt.payload ->> 'status' = 'committed'
            and receipt.payload ->> 'providerReceiptId' = ${input.replyToProviderMessageId}
            and (receipt.payload ->> 'occurredAt')::timestamptz <= ${occurredAt}
          where effect.household_id = authority.household_id
            and effect.conversation_id::text = authority.conversation_id
          union
          select effect.id::text as reply_id
          from outbox_effects effect
          where effect.household_id = authority.household_id
            and effect.conversation_id::text = authority.conversation_id
            and effect.status = 'committed'
            and effect.provider_receipt_id = ${input.replyToProviderMessageId}
            and effect.occurred_at <= ${occurredAt}
        ) candidate
        limit 2
      ) reply on true
      limit 2
    `;
    if (rows.length === 0) return null;
    if (rows.length !== 1) throw new Error("Ambiguous Linq ingress authority");

    const [authority] = rows;
    if (!authority) return null;
    return {
      householdId: authority.household_id,
      conversationId: authority.conversation_id,
      audience: authority.audience,
      authorityVersion: authority.authority_version,
      participantSetDigest: authority.participant_set_digest,
      expectedParticipantIdentityDigests: authority.expected_participant_identity_digests,
      senderAdultId: authority.sender_adult_id,
      replyToSignalId: authority.reply_to_signal_id,
    };
  }

  async loadRecentConversationTurns(
    householdId: string,
    conversationId: string,
  ): Promise<ConversationSnapshot["recentTurns"]> {
    const rows = await this.#sql<
      {
        signal_id: string;
        speaker: "florence" | string;
        text: string;
        occurred_at: Date;
        accepted_at: Date;
        turn_kind: number;
      }[]
    >`
      select signal_id, speaker, text, occurred_at, accepted_at, turn_kind
      from (
        select s.signal_id, s.payload ->> 'senderAdultId' as speaker,
               coalesce(nullif(s.payload ->> 'text', ''), '[Image attached]') as text,
               (s.payload ->> 'occurredAt')::timestamptz as occurred_at,
               s.accepted_at, 0 as turn_kind
        from household_signals s
        where s.household_id = ${householdId}
          and s.type = 'conversation.message'
          and s.status = 'completed'
          and s.payload ->> 'conversationId' = ${conversationId}
        union all
        select e.id as signal_id, 'florence' as speaker, e.payload ->> 'text' as text,
               e.occurred_at, s.accepted_at, 1 as turn_kind
        from outbox_effects e
        join household_signals s on s.signal_id = e.signal_id
        where e.household_id = ${householdId}
          and e.conversation_id = ${conversationId}
          and e.kind = 'conversation.message'
          and e.status = 'committed'
        order by occurred_at desc, accepted_at desc, turn_kind desc, signal_id desc
        limit 100
      ) recent
      order by occurred_at, accepted_at, turn_kind, signal_id
    `;
    return rows.map((row) => ({
      signalId: row.signal_id,
      speaker: row.speaker,
      text: row.text,
      occurredAt: row.occurred_at.toISOString(),
    }));
  }

  async loadDeliberation(signalId: string): Promise<PersistedDeliberation | null> {
    const [row] = await this.#sql<
      { deliberation_input_digest: string | null; deliberation_result: WorkerProposal[] | null }[]
    >`
      select deliberation_input_digest, deliberation_result
      from household_signals
      where signal_id = ${signalId}
    `;
    if (!row?.deliberation_input_digest || !row.deliberation_result) return null;
    return { inputDigest: row.deliberation_input_digest, proposals: row.deliberation_result };
  }

  async saveDeliberation(input: {
    signalId: string;
    leaseOwner: string;
    inputDigest: string;
    proposals: readonly WorkerProposal[];
  }): Promise<void> {
    await this.#sql.begin(async (sql) => {
      const [row] = await sql<
        { deliberation_input_digest: string | null; deliberation_result: WorkerProposal[] | null }[]
      >`
        select s.deliberation_input_digest, s.deliberation_result
        from household_signals s
        join household_streams h on h.household_id = s.household_id
        where s.signal_id = ${input.signalId}
          and s.status = 'processing'
          and s.lease_owner = ${input.leaseOwner}
          and h.lease_owner = ${input.leaseOwner}
          and s.lease_until > clock_timestamp()
          and h.lease_until > clock_timestamp()
        for update of s, h
      `;
      if (!row) throw new HouseholdConcurrencyError("The signal no longer owns the household lease");
      if (row.deliberation_input_digest !== null || row.deliberation_result !== null) {
        if (
          row.deliberation_input_digest === input.inputDigest &&
          sameJson(row.deliberation_result, input.proposals)
        ) {
          return;
        }
        throw new DeliberationConflictError(input.signalId);
      }
      await sql`
        update household_signals
        set deliberation_input_digest = ${input.inputDigest},
            deliberation_result = ${sql.json(input.proposals)}
        where signal_id = ${input.signalId}
      `;
    });
  }

  async commit(input: HouseholdCommit): Promise<void> {
    await this.#sql.begin(async (sql) => {
      const [current] = await sql<{ household_id: string; version: number; payload: HouseholdSignal }[]>`
        select s.household_id, h.version, s.payload
        from household_signals s
        join household_streams h on h.household_id = s.household_id
        where s.signal_id = ${input.signalId}
          and s.status = 'processing'
          and s.lease_owner = ${input.leaseOwner}
          and h.lease_owner = ${input.leaseOwner}
          and s.lease_until > clock_timestamp()
          and h.lease_until > clock_timestamp()
        for update of s, h
      `;
      if (!current || current.household_id !== input.householdId) {
        throw new HouseholdConcurrencyError("The signal no longer owns the household lease");
      }
      if (current.version !== input.expectedVersion) {
        throw new HouseholdConcurrencyError(
          `Household version changed from ${input.expectedVersion} to ${current.version}`,
        );
      }

      let version = current.version;
      for (const event of input.events) {
        version += 1;
        await sql`
          insert into household_events (id, household_id, signal_id, version, type, payload)
          values (
            ${randomUUID()}, ${input.householdId}, ${input.signalId}, ${version}, ${event.type},
            ${sql.json(event)}
          )
        `;
      }

      const signalOccurredAt = new Date(current.payload.occurredAt);
      for (const effect of input.effects) {
        const conversation =
          effect.kind === "conversation.message"
            ? {
                id: effect.conversationId,
                authorityVersion: effect.conversationAuthorityVersion,
                participantSetDigest: effect.participantSetDigest,
                episodeId: effect.episodeId,
                payload: effect.payload,
              }
            : {
                id: null,
                authorityVersion: null,
                participantSetDigest: null,
                episodeId: null,
                payload: {
                  connectionId: effect.connectionId,
                  ownerAdultId: effect.ownerAdultId,
                  actionId: effect.actionId,
                  approvalDigest: effect.approvalDigest,
                  candidateId: effect.candidateId,
                  candidateVersion: effect.candidateVersion,
                  candidateDigest: effect.candidateDigest,
                  calendar: effect.payload,
                } satisfies StoredCalendarEffectPayload,
              };
        await sql`
          insert into outbox_effects (
            id, household_id, signal_id, idempotency_key, kind, conversation_id,
            conversation_authority_version, participant_set_digest,
            episode_id, payload, status, occurred_at
          ) values (
            ${effect.id}, ${input.householdId}, ${input.signalId}, ${effect.idempotencyKey},
            ${effect.kind}, ${conversation.id}, ${conversation.authorityVersion},
            ${conversation.participantSetDigest}, ${conversation.episodeId}, ${sql.json(conversation.payload)},
            'pending', ${signalOccurredAt}
          )
        `;
      }
      if (input.firedTimerId) {
        const fired = await sql`
          update episode_timers
          set status = 'fired', fired_at = ${signalOccurredAt}
          where id = ${input.firedTimerId}
            and household_id = ${input.householdId}
            and status = 'scheduled'
          returning id
        `;
        if (fired.length !== 1) throw new Error("The fired episode timer is not current");
      }
      if (input.cancelEpisodeIds.length > 0) {
        await sql`
          update episode_timers
          set status = 'cancelled', cancelled_at = ${signalOccurredAt}
          where household_id = ${input.householdId}
            and episode_id in ${sql(input.cancelEpisodeIds)}
            and status = 'scheduled'
        `;
      }
      for (const timer of input.timers) {
        await sql`
          insert into episode_timers (
            id, household_id, signal_id, idempotency_key, episode_id,
            episode_version, scheduled_for, status
          ) values (
            ${timer.id}, ${input.householdId}, ${input.signalId}, ${timer.idempotencyKey},
            ${timer.episodeId}, ${timer.episodeVersion}, ${new Date(timer.scheduledFor)}, 'scheduled'
          )
        `;
      }

      if (input.effectReceipt) {
        const receipt = input.effectReceipt;
        const updated = await sql`
          update outbox_effects
          set status = ${receipt.status}, provider_receipt_id = ${receipt.providerReceiptId},
              receipt_detail = ${receipt.detail}, receipt_at = ${new Date(receipt.occurredAt)}
          where id = ${receipt.effectId}
            and household_id = ${input.householdId}
            and episode_id is not distinct from ${receipt.episodeId}
            and status = 'pending'
          returning id
        `;
        if (updated.length !== 1) {
          const [persisted] = await sql<
            { status: string; provider_receipt_id: string | null; receipt_detail: string | null }[]
          >`
            select status, provider_receipt_id, receipt_detail
            from outbox_effects
            where id = ${receipt.effectId}
              and household_id = ${input.householdId}
              and episode_id is not distinct from ${receipt.episodeId}
          `;
          if (
            !persisted ||
            persisted.status !== receipt.status ||
            persisted.provider_receipt_id !== receipt.providerReceiptId ||
            persisted.receipt_detail !== receipt.detail
          ) {
            throw new Error("The effect receipt conflicts with its current delivery state");
          }
        }
      }

      await sql`
        update household_streams
        set version = ${version}, lease_owner = null, lease_until = null, updated_at = clock_timestamp()
        where household_id = ${input.householdId}
      `;
      await sql`
        update household_signals
        set status = 'completed', lease_owner = null, lease_until = null,
            completed_at = clock_timestamp(), last_error = null
        where signal_id = ${input.signalId}
      `;
    });
  }

  async fail(input: {
    signalId: string;
    leaseOwner: string;
    retryAt: string | null;
    error: string;
  }): Promise<void> {
    await this.#sql.begin(async (sql) => {
      const [claimed] = await sql<{ household_id: string }[]>`
        select s.household_id
        from household_signals s
        join household_streams h on h.household_id = s.household_id
        where s.signal_id = ${input.signalId}
          and s.status = 'processing'
          and s.lease_owner = ${input.leaseOwner}
          and h.lease_owner = ${input.leaseOwner}
        for update of s, h
      `;
      if (!claimed) throw new HouseholdConcurrencyError("The signal is not leased by this worker");

      const retryAt = input.retryAt ? new Date(input.retryAt) : null;
      await sql`
        update household_signals
        set status = ${retryAt ? "retry" : "dead"},
            available_at = coalesce(${retryAt}, available_at),
            lease_owner = null, lease_until = null, last_error = ${input.error},
            completed_at = ${retryAt ? null : new Date()}
        where signal_id = ${input.signalId}
      `;
      await sql`
        update household_streams
        set lease_owner = null, lease_until = null, updated_at = clock_timestamp()
        where household_id = ${claimed.household_id}
      `;
    });
  }

  async claimNextDueTimer(input: {
    leaseOwner: string;
    now: string;
    leaseUntil: string;
  }): Promise<DueTimer | null> {
    const now = new Date(input.now);
    const leaseUntil = new Date(input.leaseUntil);
    if (leaseUntil <= now) throw new Error("A timer lease must end after it starts");
    const claimed = await this.#sql.begin(async (sql) => {
      const [row] = await sql<
        {
          id: string;
          household_id: string;
          episode_id: string;
          episode_version: number;
          scheduled_for: Date;
        }[]
      >`
        select t.id, t.household_id, t.episode_id, t.episode_version, t.scheduled_for
        from episode_timers t
        where t.status = 'scheduled'
          and t.scheduled_for <= ${now}
          and (t.lease_until is null or t.lease_until <= ${now})
          and not exists (
            select 1 from household_signals s
            where s.household_id = t.household_id
              and s.type = 'timer.fired'
              and s.payload ->> 'timerId' = t.id::text
          )
        order by t.scheduled_for, t.id
        for update of t skip locked
        limit 1
      `;
      if (!row) return null;
      await sql`
        update episode_timers
        set lease_owner = ${input.leaseOwner}, lease_until = ${leaseUntil}
        where id = ${row.id}
      `;
      return row;
    });
    return claimed
      ? {
          id: claimed.id,
          householdId: claimed.household_id,
          episodeId: claimed.episode_id,
          episodeVersion: claimed.episode_version,
          scheduledFor: claimed.scheduled_for.toISOString(),
        }
      : null;
  }

  async releaseTimerClaim(input: { timerId: string; leaseOwner: string }): Promise<void> {
    const released = await this.#sql`
      update episode_timers
      set lease_owner = null, lease_until = null
      where id = ${input.timerId} and lease_owner = ${input.leaseOwner}
      returning id
    `;
    if (released.length === 0) throw new HouseholdConcurrencyError("The timer claim is no longer current");
  }

  async claimNextEffect(input: {
    leaseOwner: string;
    now: string;
    leaseUntil: string;
  }): Promise<ClaimedEffect | null> {
    const now = new Date(input.now);
    const leaseUntil = new Date(input.leaseUntil);
    if (leaseUntil <= now) throw new Error("An effect lease must end after it starts");
    const row = await this.#sql.begin(async (sql) => {
      const [effect] = await sql<
        {
          id: string;
          household_id: string;
          idempotency_key: string;
          kind: "conversation.message" | "google.calendar.create";
          conversation_id: string | null;
          conversation_authority_version: number | null;
          participant_set_digest: string | null;
          provider_conversation_id: string | null;
          audience: "private" | "group" | null;
          expected_participant_identity_digests: string[] | null;
          episode_id: string | null;
          payload: Record<string, unknown>;
          occurred_at: Date;
          attempt_count: number;
        }[]
      >`
        select e.id, e.household_id, e.idempotency_key, e.kind, e.conversation_id,
               e.conversation_authority_version, e.participant_set_digest, e.episode_id,
               e.payload, e.occurred_at, e.attempt_count,
               case when
                 (binding.payload -> 'conversation' ->> 'authorityVersion')::integer =
                   e.conversation_authority_version
                 and binding.payload -> 'conversation' ->> 'participantSetDigest' =
                   e.participant_set_digest
               then binding.payload -> 'conversation' ->> 'providerConversationId'
               else null end as provider_conversation_id,
               case when
                 (binding.payload -> 'conversation' ->> 'authorityVersion')::integer =
                   e.conversation_authority_version
                 and binding.payload -> 'conversation' ->> 'participantSetDigest' =
                   e.participant_set_digest
               then binding.payload -> 'conversation' ->> 'audience'
               else null end as audience,
               case when
                 (binding.payload -> 'conversation' ->> 'authorityVersion')::integer =
                   e.conversation_authority_version
                 and binding.payload -> 'conversation' ->> 'participantSetDigest' =
                   e.participant_set_digest
               then binding.expected_participant_identity_digests
               else null end as expected_participant_identity_digests
        from outbox_effects e
        left join lateral (
          select current_binding.payload, identities.expected_participant_identity_digests
          from (
            select event.payload
            from household_events event
            where event.household_id = e.household_id
              and event.type = 'conversation.bound'
              and event.payload -> 'conversation' ->> 'conversationId' = e.conversation_id::text
            order by event.version desc
            limit 1
          ) current_binding
          join lateral (
            select array_agg(
              verification.payload ->> 'identitySubjectDigest'
              order by verification.payload ->> 'identitySubjectDigest'
            ) as expected_participant_identity_digests,
            count(*) as verification_count,
            count(distinct verification.payload ->> 'adultId') as verified_adult_count
            from jsonb_array_elements_text(
              current_binding.payload -> 'conversation' -> 'authorizedAdultIds'
            ) authorized(adult_id)
            join household_events verification
              on verification.household_id = e.household_id
              and verification.type = 'adult.verified'
              and verification.payload ->> 'adultId' = authorized.adult_id
          ) identities on
            identities.verification_count = jsonb_array_length(
              current_binding.payload -> 'conversation' -> 'authorizedAdultIds'
            )
            and identities.verified_adult_count = identities.verification_count
        ) binding on true
        where e.status = 'pending'
          and e.kind in ('conversation.message', 'google.calendar.create')
          and e.available_at <= ${now}
          and (e.lease_until is null or e.lease_until <= ${now})
          and not exists (
            select 1 from household_signals s
            where s.household_id = e.household_id
              and s.type = 'effect.receipt'
              and s.payload ->> 'effectId' = e.id::text
          )
        order by e.available_at, e.occurred_at, e.id
        for update of e skip locked
        limit 1
      `;
      if (!effect) return null;
      await sql`
        update outbox_effects
        set attempt_count = attempt_count + 1, lease_owner = ${input.leaseOwner},
            lease_until = ${leaseUntil}, last_error = null
        where id = ${effect.id}
      `;
      return effect;
    });
    if (!row) return null;
    if (row.kind === "google.calendar.create") {
      const { calendar, ...payload } = parseStoredCalendarEffect(row.payload);
      return {
        id: row.id,
        householdId: row.household_id,
        idempotencyKey: row.idempotency_key,
        kind: row.kind,
        ...payload,
        episodeId: null,
        payload: calendar,
        occurredAt: row.occurred_at.toISOString(),
        attempt: row.attempt_count + 1,
        leaseOwner: input.leaseOwner,
      };
    }
    if (
      !row.conversation_id ||
      row.conversation_authority_version === null ||
      !row.participant_set_digest ||
      typeof row.payload.text !== "string"
    ) {
      throw new Error("A queued conversation effect has an invalid durable authority shape");
    }
    return {
      id: row.id,
      householdId: row.household_id,
      idempotencyKey: row.idempotency_key,
      kind: row.kind,
      conversationId: row.conversation_id,
      conversationAuthorityVersion: row.conversation_authority_version,
      participantSetDigest: row.participant_set_digest,
      providerConversationId: row.provider_conversation_id,
      audience: row.audience,
      expectedParticipantIdentityDigests: row.expected_participant_identity_digests,
      episodeId: row.episode_id,
      payload: { text: row.payload.text },
      occurredAt: row.occurred_at.toISOString(),
      attempt: row.attempt_count + 1,
      leaseOwner: input.leaseOwner,
    };
  }

  async releaseEffectClaim(input: {
    effectId: string;
    leaseOwner: string;
    availableAt: string;
    lastError: string | null;
  }): Promise<void> {
    const released = await this.#sql`
      update outbox_effects
      set lease_owner = null, lease_until = null, available_at = ${new Date(input.availableAt)},
          last_error = ${input.lastError}
      where id = ${input.effectId} and lease_owner = ${input.leaseOwner}
      returning id
    `;
    if (released.length === 0) throw new HouseholdConcurrencyError("The effect claim is no longer current");
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    await this.#sql.end({ timeout: 5 });
  }
}

function sameJson(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (left === null || right === null || typeof left !== "object" || typeof right !== "object") {
    return false;
  }
  if (Array.isArray(left) || Array.isArray(right)) {
    return (
      Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((value, index) => sameJson(value, right[index]))
    );
  }
  const leftRecord = left as Record<string, unknown>;
  const rightRecord = right as Record<string, unknown>;
  const keys = Object.keys(leftRecord);
  return (
    keys.length === Object.keys(rightRecord).length &&
    keys.every((key) => Object.hasOwn(rightRecord, key) && sameJson(leftRecord[key], rightRecord[key]))
  );
}

function validatedSchema(value: string): string {
  if (!/^[a-z][a-z0-9_]{0,62}$/u.test(value)) throw new Error("Invalid Postgres schema name");
  return value;
}

function parseStoredCalendarEffect(value: Record<string, unknown>): StoredCalendarEffectPayload {
  const keys = [
    "actionId",
    "approvalDigest",
    "calendar",
    "candidateDigest",
    "candidateId",
    "candidateVersion",
    "connectionId",
    "ownerAdultId",
  ];
  if (!sameJson(Object.keys(value).sort(), keys)) {
    throw new Error("A queued Calendar effect has an invalid durable payload shape");
  }
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  const digest = /^[0-9a-f]{64}$/;
  if (
    typeof value.connectionId !== "string" ||
    !uuid.test(value.connectionId) ||
    typeof value.ownerAdultId !== "string" ||
    !uuid.test(value.ownerAdultId) ||
    typeof value.actionId !== "string" ||
    !uuid.test(value.actionId) ||
    typeof value.approvalDigest !== "string" ||
    !digest.test(value.approvalDigest) ||
    typeof value.candidateId !== "string" ||
    !uuid.test(value.candidateId) ||
    value.candidateVersion !== 1 ||
    typeof value.candidateDigest !== "string" ||
    !digest.test(value.candidateDigest)
  ) {
    throw new Error("A queued Calendar effect has invalid durable authority coordinates");
  }
  return {
    connectionId: value.connectionId,
    ownerAdultId: value.ownerAdultId,
    actionId: value.actionId,
    approvalDigest: value.approvalDigest,
    candidateId: value.candidateId,
    candidateVersion: value.candidateVersion,
    candidateDigest: value.candidateDigest,
    calendar: gmailCalendarDraftSchema.parse(value.calendar),
  };
}

function googleConnectionView(row: GoogleConnectionRow): GoogleConnectionView {
  return {
    connectionId: row.connection_id,
    householdId: row.household_id,
    ownerAdultId: row.owner_adult_id,
    status: row.status,
    emailLabel: row.email_label,
    grantedScopes: row.granted_scopes,
    lastError: row.last_error,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}

function activeGoogleCredential(row: GoogleConnectionRow): ActiveGoogleCredential {
  if (row.status !== "active" || !row.refresh_token_envelope) {
    throw new Error("Google connection does not contain an active credential");
  }
  return {
    connectionId: row.connection_id,
    householdId: row.household_id,
    ownerAdultId: row.owner_adult_id,
    refreshTokenEnvelope: row.refresh_token_envelope,
    gmailCursor: row.gmail_cursor,
  };
}

function deterministicUuid(identity: string): string {
  const bytes = createHash("sha256").update(identity).digest().subarray(0, 16);
  bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x50;
  bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function digestStrings(values: readonly string[]): string {
  return createHash("sha256")
    .update(JSON.stringify([...values].sort()))
    .digest("hex");
}

function sameGroupBinding(
  signal: HouseholdSignal,
  expected: LinqGroupBootstrapRequest & {
    householdId: string;
    conversationId: string;
    actorAdultId: string;
    authorizedAdultIds: readonly string[];
    participantSetDigest: string;
  },
): boolean {
  return (
    signal.type === "conversation.bound" &&
    signal.signalId === expected.bindingSignalId &&
    signal.idempotencyKey === expected.bindingIdempotencyKey &&
    signal.householdId === expected.householdId &&
    signal.actorAdultId === expected.actorAdultId &&
    signal.conversationId === expected.conversationId &&
    signal.audience === "group" &&
    signal.authorityVersion === 1 &&
    signal.participantSetDigest === expected.participantSetDigest &&
    sameJson(signal.authorizedAdultIds, expected.authorizedAdultIds) &&
    signal.providerConversationId === expected.providerConversationId
  );
}
