import { createHmac, randomUUID } from "node:crypto";
import { z } from "zod";
import { GOOGLE_CALENDAR_EVENTS_SCOPE, GOOGLE_CALENDAR_READONLY_SCOPE } from "../adapters/google/index.js";
import {
  createApplicationProjection,
  createOnboardingProjection,
  type HouseholdApplicationSnapshot,
} from "../application/index.js";
import {
  type ApplicationStore,
  ApplicationStoreError,
  type ChannelResolution,
  type ExternalConnectionRecord,
} from "../db/application-store.js";
import type { Database } from "../db/client.js";
import {
  AdultIdSchema,
  type DurableScope,
  HouseholdAggregateSchema,
  HouseholdIdSchema,
} from "../domain/index.js";
import { canonicalJson } from "../security/canonical-json.js";
import {
  type CalendarBusyWindow,
  type CalendarPushTarget,
  type CalendarSyncState,
  type CalendarSyncWork,
  calendarSyncStateSchema,
  calendarSyncWorkSchema,
  type PersistPersonalCalendarSourceInput,
} from "./google-calendar-sync.js";
import type {
  GmailSyncState,
  GoogleSyncConnection,
  PersistPersonalGmailSourceInput,
  PersistPersonalGmailSourceResult,
  ScopedMutationResult,
} from "./google-sync.js";
import { type GmailSyncWork, gmailSyncStateSchema, gmailSyncWorkSchema } from "./google-sync.js";

const instantSchema = z.iso.datetime({ offset: true });
const handleSchema = z.string().trim().min(3).max(320);

export type PreparedInvitation = {
  invitationId: string;
  householdId: string;
  adultId: string;
  expiresAt: string;
};

export type PendingInvitation = PreparedInvitation & {
  invitedByAdultId: string;
};

export type GroupIdentity = {
  householdId: string;
  adultsByHandle: ReadonlyMap<string, string>;
};

export type OutboundChannel = {
  householdId: string;
  targetScope: DurableScope;
  chatId: string;
  status: "active" | "inactive";
};

export type ClaimedGoogleSyncWork = {
  rowId: string;
  leaseToken: string;
  work: GmailSyncWork;
  attempt: number;
  maxAttempts: number;
};

export type ClaimedCalendarSyncWork = {
  rowId: string;
  leaseToken: string;
  work: CalendarSyncWork;
  attempt: number;
  maxAttempts: number;
};

export type PersonalCalendarBusyWindowPage = {
  windows: CalendarBusyWindow[];
  complete: boolean;
  synchronizedAt: string | null;
};

export type HouseholdCalendarCreatePreparation =
  | {
      status: "ready";
      targetConnectionId: string;
      calendarId: "primary";
      relevantDataDigest: string;
      hasConflict: boolean;
    }
  | {
      status: "unavailable";
      reason: "no_write_connection" | "ambiguous_write_connection" | "projection_incomplete";
    };

type InvitationRow = {
  id: string;
  household_id: string;
  invited_by_adult_id: string;
  accepted_by_adult_id: string | null;
  invitee_adult_id: string | null;
  expires_at: Date;
};

type ConnectionRow = {
  id: string;
  household_id: string;
  adult_id: string;
  provider: "google";
  label: string;
  external_account_id: string;
  email: string | null;
  encrypted_credentials: string | null;
  granted_scopes: string[];
  status: ExternalConnectionRecord["status"];
  cursor: Record<string, unknown>;
  metadata: Record<string, unknown>;
  last_synced_at: Date | null;
};

/**
 * Operations needed by the process hosts but deliberately kept outside the
 * domain repository. Raw provider identities never enter the household
 * aggregate; pending invitation lookups use a keyed digest.
 */
export class FlorenceRuntimeStore {
  public constructor(
    private readonly database: Database,
    private readonly applicationStore: ApplicationStore,
    private readonly identityKey: string,
  ) {
    if (Buffer.byteLength(identityKey, "utf8") < 32) {
      throw new Error("Florence identity key must contain at least 32 bytes");
    }
  }

  public initialSnapshot(input: {
    householdId: string;
    initiatorAdultId: string;
    timeZone: string;
  }): HouseholdApplicationSnapshot {
    const householdId = HouseholdIdSchema.parse(input.householdId);
    const adultId = AdultIdSchema.parse(input.initiatorAdultId);
    return {
      revision: 0,
      aggregate: HouseholdAggregateSchema.parse({
        schemaVersion: 1,
        householdId,
        version: 0,
        policyVersion: 0,
        lastProcessedSequence: 0,
        timeZone: input.timeZone,
        verifiedAdultIds: [adultId],
        routineAnchors: [],
        episodes: [],
        policies: [],
        policyCandidates: [],
        approvals: [],
        memoryCandidates: [],
        memories: [],
        pendingActions: [],
      }),
      projection: createApplicationProjection(createOnboardingProjection({ initiatorAdultId: adultId })),
    };
  }

  public async provisionFoundingAdult(input: {
    externalChatId: string;
    externalHandle: string;
    timeZone: string;
    occurredAt: string;
  }): Promise<ChannelResolution> {
    const handle = canonicalizeLinqHandle(input.externalHandle);
    const occurredAt = instantSchema.parse(input.occurredAt);
    const existing = await this.applicationStore.resolveChannel({
      provider: "linq",
      externalChatId: input.externalChatId,
      externalHandle: handle,
    });
    if (existing) {
      await this.ensureFoundingSnapshot(existing, input.timeZone);
      return existing;
    }
    if (await this.isSuppressed(input.externalChatId, handle)) {
      throw new ApplicationStoreError("not_authorized", "This Linq identity is opted out");
    }

    const householdId = randomUUID();
    const adultId = randomUUID();
    try {
      await this.applicationStore.onboardFoundingAdult({
        householdId,
        adultId,
        householdName: "My family",
        adultDisplayName: "Founding adult",
        timeZone: input.timeZone,
        consent: { status: "pending" },
        projectionSchemaVersion: 1,
        initialProjection: { phase: "application_snapshot" },
        privateChannel: {
          externalChatId: input.externalChatId,
          externalHandle: handle,
          metadata: { inboundFirstAt: occurredAt, service: "iMessage" },
        },
      });
      await this.ensureFoundingSnapshot(
        {
          bindingId: "provisional",
          provider: "linq",
          channelType: "private",
          householdId,
          adultId,
          bindingStatus: "pending",
          membershipStatus: "invited",
          metadata: {},
        },
        input.timeZone,
      );
    } catch (error) {
      const raced = await this.applicationStore.resolveChannel({
        provider: "linq",
        externalChatId: input.externalChatId,
        externalHandle: handle,
      });
      if (raced) {
        await this.ensureFoundingSnapshot(raced, input.timeZone);
        return raced;
      }
      throw error;
    }
    const created = await this.applicationStore.resolveChannel({
      provider: "linq",
      externalChatId: input.externalChatId,
      externalHandle: handle,
    });
    if (!created) throw new ApplicationStoreError("invalid_state", "Founding channel was not created");
    return created;
  }

  public async finalizeFoundingAdult(input: {
    householdId: string;
    adultId: string;
    externalChatId: string;
    externalHandle: string;
    consentedAt: string;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        externalChatId: z.string().min(1).max(500),
        externalHandle: handleSchema,
        consentedAt: instantSchema,
      })
      .parse(input);
    const handle = canonicalizeLinqHandle(parsed.externalHandle);
    return this.database.begin(async (transaction) => {
      const rows = await transaction<
        { membership_status: "invited" | "active" | "revoked"; binding_status: string }[]
      >`
        select hm.status as membership_status, cb.status as binding_status
        from household_memberships hm
        join channel_bindings cb
          on cb.household_id = hm.household_id and cb.adult_id = hm.adult_id
        where hm.household_id = ${parsed.householdId} and hm.adult_id = ${parsed.adultId}
          and hm.role = 'owner'
          and cb.provider = 'linq' and cb.channel_type = 'private'
          and cb.external_chat_id = ${parsed.externalChatId} and cb.external_handle = ${handle}
        for update of hm, cb
      `;
      const identity = rows[0];
      if (!identity) return false;
      if (
        !["invited", "active"].includes(identity.membership_status) ||
        !["pending", "active"].includes(identity.binding_status)
      ) {
        throw new ApplicationStoreError("invalid_state", "Founding identity cannot be activated");
      }
      await transaction`
        update household_memberships
        set status = 'active', consented_at = coalesce(consented_at, ${parsed.consentedAt}),
            updated_at = now()
        where household_id = ${parsed.householdId} and adult_id = ${parsed.adultId}
          and role = 'owner'
      `;
      await transaction`
        update channel_bindings
        set status = 'active', updated_at = now()
        where household_id = ${parsed.householdId} and adult_id = ${parsed.adultId}
          and provider = 'linq' and channel_type = 'private'
          and external_chat_id = ${parsed.externalChatId} and external_handle = ${handle}
      `;
      return true;
    });
  }

  private async ensureFoundingSnapshot(resolution: ChannelResolution, timeZone: string): Promise<void> {
    if (!resolution.adultId) {
      throw new ApplicationStoreError("invalid_state", "Founding channel has no adult identity");
    }
    if (await this.applicationStore.load(resolution.householdId)) return;
    const snapshot = this.initialSnapshot({
      householdId: resolution.householdId,
      initiatorAdultId: resolution.adultId,
      timeZone,
    });
    try {
      await this.applicationStore.initializeApplicationSnapshot({ snapshot });
    } catch (error) {
      if (await this.applicationStore.load(resolution.householdId)) return;
      throw error;
    }
  }

  public async prepareInvitation(input: {
    householdId: string;
    invitedByAdultId: string;
    inviteeHandle: string;
    displayName?: string;
    expiresAt: string;
  }): Promise<PreparedInvitation> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        invitedByAdultId: z.uuid(),
        inviteeHandle: handleSchema,
        displayName: z.string().trim().min(1).max(200).optional(),
        expiresAt: instantSchema,
      })
      .parse(input);
    const handleHash = this.digestHandle(parsed.inviteeHandle);
    return this.database.begin(async (transaction) => {
      await transaction`select pg_advisory_xact_lock(hashtextextended(${`invite:${handleHash}`}, 0))`;
      const owners = await transaction<{ adult_id: string }[]>`
        select adult_id from household_memberships
        where household_id = ${parsed.householdId} and adult_id = ${parsed.invitedByAdultId}
          and role = 'owner' and status = 'active'
      `;
      if (!owners[0]) throw new ApplicationStoreError("not_authorized", "Only an active owner can invite");
      const existing = await transaction<InvitationRow[]>`
        select id, household_id, invited_by_adult_id, accepted_by_adult_id,
          invitee_adult_id, expires_at
        from invitations
        where invitee_handle_hash = ${handleHash} and status = 'pending' and expires_at > now()
        for update
      `;
      const prior = existing[0];
      if (prior) {
        if (prior.household_id !== parsed.householdId) {
          throw new ApplicationStoreError(
            "not_authorized",
            "That identity already has a live household invitation",
          );
        }
        if (!prior.invitee_adult_id) {
          throw new ApplicationStoreError("invalid_state", "Pending invitation has no adult identity");
        }
        return {
          invitationId: prior.id,
          householdId: prior.household_id,
          adultId: prior.invitee_adult_id,
          expiresAt: prior.expires_at.toISOString(),
        };
      }

      await transaction`
        update invitations set status = 'expired', updated_at = now()
        where invitee_handle_hash = ${handleHash} and status = 'pending' and expires_at <= now()
      `;
      const adultId = randomUUID();
      const invitationId = randomUUID();
      const household = await transaction<{ timezone: string }[]>`
        select timezone from households where id = ${parsed.householdId} for update
      `;
      if (!household[0]) throw new ApplicationStoreError("not_found", "Unknown household");
      await transaction`
        insert into adults (id, display_name, timezone)
        values (${adultId}, ${parsed.displayName ?? "Invited adult"}, ${household[0].timezone})
      `;
      await transaction`
        insert into household_memberships (household_id, adult_id, role, status)
        values (${parsed.householdId}, ${adultId}, 'adult', 'invited')
      `;
      await transaction`
        insert into invitations (
          id, household_id, invited_by_adult_id, invitee_handle_hash, status,
          expires_at, invitee_adult_id
        ) values (
          ${invitationId}, ${parsed.householdId}, ${parsed.invitedByAdultId}, ${handleHash},
          'pending', ${parsed.expiresAt}, ${adultId}
        )
      `;
      return {
        invitationId,
        householdId: parsed.householdId,
        adultId,
        expiresAt: new Date(parsed.expiresAt).toISOString(),
      };
    });
  }

  public async findPendingInvitation(inviteeHandle: string): Promise<PendingInvitation | null> {
    const handleHash = this.digestHandle(inviteeHandle);
    const rows = await this.database<InvitationRow[]>`
      select id, household_id, invited_by_adult_id, accepted_by_adult_id,
        invitee_adult_id, expires_at
      from invitations
      where invitee_handle_hash = ${handleHash} and status = 'pending' and expires_at > now()
      limit 1
    `;
    const row = rows[0];
    if (!row?.invitee_adult_id) return null;
    return {
      invitationId: row.id,
      householdId: row.household_id,
      invitedByAdultId: row.invited_by_adult_id,
      adultId: row.invitee_adult_id,
      expiresAt: row.expires_at.toISOString(),
    };
  }

  public async bindPendingInvitee(input: {
    invitation: PendingInvitation;
    externalChatId: string;
    externalHandle: string;
    occurredAt: string;
  }): Promise<ChannelResolution> {
    const handle = canonicalizeLinqHandle(input.externalHandle);
    if (await this.isSuppressed(input.externalChatId, handle)) {
      throw new ApplicationStoreError("not_authorized", "This Linq identity is opted out");
    }
    await this.applicationStore.upsertChannelBinding({
      householdId: input.invitation.householdId,
      adultId: input.invitation.adultId,
      provider: "linq",
      channelType: "private",
      externalChatId: input.externalChatId,
      externalHandle: handle,
      status: "pending",
      metadata: { inboundFirstAt: instantSchema.parse(input.occurredAt) },
    });
    const resolution = await this.applicationStore.resolveChannel({
      provider: "linq",
      externalChatId: input.externalChatId,
      externalHandle: handle,
    });
    if (!resolution) throw new ApplicationStoreError("invalid_state", "Invitee channel was not bound");
    return resolution;
  }

  public async finalizeInvitation(input: {
    householdId: string;
    adultId: string;
    externalChatId: string;
    externalHandle: string;
    consentedAt: string;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        externalChatId: z.string().min(1).max(500),
        externalHandle: handleSchema,
        consentedAt: instantSchema,
      })
      .parse(input);
    const handleHash = this.digestHandle(parsed.externalHandle);
    return this.database.begin(async (transaction) => {
      const invitations = await transaction<{ id: string }[]>`
        update invitations
        set status = 'accepted', accepted_by_adult_id = ${parsed.adultId}, updated_at = now()
        where household_id = ${parsed.householdId} and invitee_adult_id = ${parsed.adultId}
          and invitee_handle_hash = ${handleHash} and status = 'pending' and expires_at > now()
        returning id
      `;
      if (!invitations[0]) return false;
      await transaction`
        update household_memberships
        set status = 'active', consented_at = ${parsed.consentedAt}, updated_at = now()
        where household_id = ${parsed.householdId} and adult_id = ${parsed.adultId}
          and status = 'invited'
      `;
      await transaction`
        update channel_bindings
        set status = 'active', updated_at = now()
        where provider = 'linq' and channel_type = 'private'
          and household_id = ${parsed.householdId} and adult_id = ${parsed.adultId}
          and external_chat_id = ${parsed.externalChatId}
          and external_handle = ${canonicalizeLinqHandle(parsed.externalHandle)}
          and status = 'pending'
      `;
      return true;
    });
  }

  public async resolveExactGroup(participantHandles: readonly string[]): Promise<GroupIdentity | null> {
    const handles = [...new Set(participantHandles.map(canonicalizeLinqHandle))].sort();
    if (handles.length !== 2) return null;
    const rows = await this.database<{ household_id: string; adult_id: string; external_handle: string }[]>`
      select cb.household_id, cb.adult_id, cb.external_handle
      from channel_bindings cb
      join household_memberships hm
        on hm.household_id = cb.household_id and hm.adult_id = cb.adult_id
      where cb.provider = 'linq' and cb.channel_type = 'private'
        and cb.status = 'active' and hm.status = 'active'
        and cb.external_handle = any(${handles})
    `;
    if (rows.length !== handles.length || rows.some((row) => !row.adult_id || !row.external_handle)) {
      return null;
    }
    const households = new Set(rows.map((row) => row.household_id));
    if (households.size !== 1) return null;
    const expected = rows.map((row) => canonicalizeLinqHandle(row.external_handle)).sort();
    if (expected.some((handle, index) => handle !== handles[index])) return null;
    const householdId = rows[0]?.household_id;
    if (!householdId) return null;
    const memberships = await this.database<{ count: string }[]>`
      select count(*)::text as count from household_memberships
      where household_id = ${householdId} and status = 'active'
    `;
    if (Number(memberships[0]?.count ?? 0) !== 2) return null;
    return {
      householdId,
      adultsByHandle: new Map(rows.map((row) => [canonicalizeLinqHandle(row.external_handle), row.adult_id])),
    };
  }

  public async bindHouseholdGroup(input: {
    householdId: string;
    externalChatId: string;
    participantHandles: readonly string[];
    healthStatus: string;
  }): Promise<ChannelResolution> {
    await this.applicationStore.upsertChannelBinding({
      householdId: input.householdId,
      provider: "linq",
      channelType: "group",
      externalChatId: input.externalChatId,
      status: "active",
      metadata: {
        participantHandleDigests: input.participantHandles.map((handle) => this.digestHandle(handle)).sort(),
        healthStatus: input.healthStatus,
      },
    });
    const resolution = await this.applicationStore.resolveChannel({
      provider: "linq",
      externalChatId: input.externalChatId,
    });
    if (!resolution) throw new ApplicationStoreError("invalid_state", "Household group was not bound");
    return resolution;
  }

  public async resolveAdultForHandle(input: {
    householdId: string;
    externalHandle: string;
  }): Promise<string | null> {
    const householdId = z.uuid().parse(input.householdId);
    const handle = canonicalizeLinqHandle(input.externalHandle);
    const rows = await this.database<{ adult_id: string }[]>`
      select cb.adult_id
      from channel_bindings cb
      join household_memberships hm
        on hm.household_id = cb.household_id and hm.adult_id = cb.adult_id
      where cb.household_id = ${householdId} and cb.provider = 'linq'
        and cb.channel_type = 'private' and cb.external_handle = ${handle}
        and cb.status in ('active', 'pending') and hm.status in ('active', 'invited')
      limit 1
    `;
    return rows[0]?.adult_id ?? null;
  }

  public async resolveTarget(input: {
    householdId: string;
    targetScope: DurableScope;
  }): Promise<OutboundChannel | null> {
    const householdId = z.uuid().parse(input.householdId);
    const rows =
      input.targetScope.kind === "household"
        ? await this.database<{ external_chat_id: string; status: string }[]>`
            select external_chat_id,
              case
                when status = 'active'
                  and coalesce(metadata->>'healthStatus', 'HEALTHY') not in ('CRITICAL', 'OPTED_OUT')
                then 'active'
                else 'inactive'
              end as status
            from channel_bindings
            where household_id = ${householdId} and provider = 'linq' and channel_type = 'group'
            order by updated_at desc limit 1
          `
        : await this.database<{ external_chat_id: string; status: string }[]>`
            select cb.external_chat_id,
              case
                when (cb.status = 'active' and hm.status = 'active')
                  or (
                    cb.status = 'pending' and hm.status = 'invited'
                    and cb.metadata ? 'inboundFirstAt'
                  )
                then 'active'
                else 'inactive'
              end as status
            from channel_bindings cb
            left join household_memberships hm
              on hm.household_id = cb.household_id and hm.adult_id = cb.adult_id
            where cb.household_id = ${householdId} and cb.adult_id = ${input.targetScope.adultId}
              and cb.provider = 'linq' and cb.channel_type = 'private'
            order by cb.updated_at desc limit 1
          `;
    const row = rows[0];
    if (!row) return null;
    const suppressed = await this.isSuppressed(
      row.external_chat_id,
      input.targetScope.kind === "personal"
        ? await this.privateHandle(householdId, input.targetScope.adultId)
        : undefined,
    );
    return {
      householdId,
      targetScope: input.targetScope,
      chatId: row.external_chat_id,
      status: row.status === "active" && !suppressed ? "active" : "inactive",
    };
  }

  public async setSuppression(input: {
    externalChatId: string;
    externalHandle?: string;
    scope: "private" | "group";
    suppressed: boolean;
    occurredAt: string;
    reason: string;
  }): Promise<void> {
    const parsed = z
      .strictObject({
        externalChatId: z.string().min(1).max(500),
        externalHandle: handleSchema.optional(),
        scope: z.enum(["private", "group"]),
        suppressed: z.boolean(),
        occurredAt: instantSchema,
        reason: z.string().min(1).max(200),
      })
      .superRefine((value, context) => {
        if ((value.scope === "private") !== (value.externalHandle !== undefined)) {
          context.addIssue({ code: "custom", message: "Private suppression requires one handle" });
        }
      })
      .parse(input);
    const hash = parsed.externalHandle ? this.digestHandle(parsed.externalHandle) : null;
    await this.database`
      insert into channel_suppressions (
        id, provider, external_chat_id, external_handle_hash, scope, status, reason, changed_at
      ) values (
        ${randomUUID()}, 'linq', ${parsed.externalChatId}, ${hash}, ${parsed.scope},
        ${parsed.suppressed ? "suppressed" : "released"}, ${parsed.reason}, ${parsed.occurredAt}
      )
      on conflict (provider, external_chat_id, external_handle_hash)
      do update set status = excluded.status, reason = excluded.reason,
        changed_at = excluded.changed_at, updated_at = now()
    `;
    await this.database`
      update channel_bindings
      set status = case
        when ${parsed.suppressed} then 'paused'
        when channel_type = 'private' and exists (
          select 1 from household_memberships hm
          where hm.household_id = channel_bindings.household_id
            and hm.adult_id = channel_bindings.adult_id and hm.status = 'invited'
        ) then 'pending'
        else 'active'
      end,
      updated_at = now()
      where provider = 'linq' and external_chat_id = ${parsed.externalChatId}
        and (${hash}::text is null or external_handle = ${parsed.externalHandle ?? null})
        and status <> 'revoked'
    `;
  }

  public async isSuppressed(externalChatId: string, externalHandle?: string): Promise<boolean> {
    const chatId = z.string().min(1).max(500).parse(externalChatId);
    const hash = externalHandle ? this.digestHandle(externalHandle) : null;
    const rows = await this.database<{ status: string }[]>`
      select status from channel_suppressions
      where provider = 'linq' and external_chat_id = ${chatId}
        and external_handle_hash is not distinct from ${hash}
      limit 1
    `;
    return rows[0]?.status === "suppressed";
  }

  public async purgeExpiredProviderInbox(asOf: string): Promise<number> {
    const at = instantSchema.parse(asOf);
    const rows = await this.database<{ id: string }[]>`
      delete from provider_inbox
      where retention_until <= ${at} and status in ('resolved', 'quarantined', 'dead')
      returning id
    `;
    return rows.length;
  }

  public async findActiveGoogleConnectionByEmail(email: string): Promise<ExternalConnectionRecord | null> {
    const normalized = z.email().parse(email).toLowerCase();
    const rows = await this.database<ConnectionRow[]>`
      select id, household_id, adult_id, provider, label, external_account_id, email,
        encrypted_credentials, granted_scopes, status, cursor, metadata, last_synced_at
      from external_connections
      where provider = 'google' and status = 'active' and lower(email) = ${normalized}
      limit 2
    `;
    if (rows.length !== 1) return null;
    return mapConnection(rows[0] as ConnectionRow);
  }

  public async findActiveGmailConnections(input: {
    normalizedMailboxEmail: string;
    subscription: string;
  }): Promise<readonly GoogleSyncConnection[]> {
    const email = z.email().parse(input.normalizedMailboxEmail).toLowerCase();
    const subscription = z.string().min(1).max(1_000).parse(input.subscription);
    const rows = await this.database<ConnectionRow[]>`
      select id, household_id, adult_id, provider, label, external_account_id, email,
        encrypted_credentials, granted_scopes, status, cursor, metadata, last_synced_at
      from external_connections
      where provider = 'google' and status = 'active' and lower(email) = ${email}
        and cursor->'gmail'->'watch'->>'subscription' = ${subscription}
      order by created_at
      limit 2
    `;
    return rows.map(mapGoogleSyncConnection);
  }

  public async getOwnedGoogleConnection(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
  }): Promise<GoogleSyncConnection | null> {
    const parsed = z
      .strictObject({ householdId: z.uuid(), adultId: z.uuid(), connectionId: z.uuid() })
      .parse(input);
    const rows = await this.database<ConnectionRow[]>`
      select id, household_id, adult_id, provider, label, external_account_id, email,
        encrypted_credentials, granted_scopes, status, cursor, metadata, last_synced_at
      from external_connections
      where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
        and adult_id = ${parsed.adultId} and provider = 'google'
      limit 1
    `;
    return rows[0] ? mapGoogleSyncConnection(rows[0]) : null;
  }

  public async listOwnedGoogleConnections(input: {
    householdId: string;
    adultId: string;
    includeRevoked?: boolean;
  }): Promise<readonly GoogleSyncConnection[]> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        includeRevoked: z.boolean().default(false),
      })
      .parse(input);
    const rows = await this.database<ConnectionRow[]>`
      select id, household_id, adult_id, provider, label, external_account_id, email,
        encrypted_credentials, granted_scopes, status, cursor, metadata, last_synced_at
      from external_connections
      where household_id = ${parsed.householdId} and adult_id = ${parsed.adultId}
        and provider = 'google'
        and (${parsed.includeRevoked} or status <> 'revoked')
      order by created_at
    `;
    return rows.map(mapGoogleSyncConnection);
  }

  public async replaceEncryptedCredentials(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    expectedCiphertext: string;
    encryptedCredentials: string;
    grantedScopes: readonly string[];
  }): Promise<ScopedMutationResult> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        connectionId: z.uuid(),
        expectedCiphertext: z.string(),
        encryptedCredentials: z.string().min(1),
        grantedScopes: z.array(z.string().min(1).max(500)).max(100),
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update external_connections
      set encrypted_credentials = ${parsed.encryptedCredentials},
        granted_scopes = ${parsed.grantedScopes}, updated_at = now()
      where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
        and adult_id = ${parsed.adultId} and provider = 'google' and status = 'active'
        and encrypted_credentials = ${parsed.expectedCiphertext}
      returning id
    `;
    if (rows[0]) return "updated";
    return this.googleMutationFailure(parsed);
  }

  public async saveGmailSyncState(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    expectedRevision: number;
    state: GmailSyncState;
  }): Promise<ScopedMutationResult> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        connectionId: z.uuid(),
        expectedRevision: z.number().int().nonnegative(),
        state: z.record(z.string(), z.unknown()),
      })
      .parse(input);
    const stateJson = JSON.parse(JSON.stringify(parsed.state));
    const rows = await this.database<{ id: string }[]>`
      update external_connections
      set cursor = jsonb_set(cursor, '{gmail}', ${this.database.json(stateJson)}, true),
        last_synced_at = now(), updated_at = now()
      where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
        and adult_id = ${parsed.adultId} and provider = 'google' and status = 'active'
        and coalesce((cursor->'gmail'->>'revision')::integer, 0) = ${parsed.expectedRevision}
      returning id
    `;
    if (rows[0]) return "updated";
    return this.googleMutationFailure(parsed);
  }

  public async saveCalendarSyncState(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    expectedRevision: number;
    state: CalendarSyncState;
  }): Promise<ScopedMutationResult> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        connectionId: z.uuid(),
        expectedRevision: z.number().int().nonnegative(),
        state: calendarSyncStateSchema,
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update external_connections
      set cursor = jsonb_set(
            cursor, '{calendar}', ${this.database.json(JSON.parse(JSON.stringify(parsed.state)))}, true
          ),
          last_synced_at = now(), updated_at = now()
      where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
        and adult_id = ${parsed.adultId} and provider = 'google' and status = 'active'
        and coalesce((cursor->'calendar'->>'revision')::integer, 0) = ${parsed.expectedRevision}
      returning id
    `;
    if (rows[0]) return "updated";
    return this.googleMutationFailure(parsed);
  }

  public async restartCalendarSync(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    expectedRevision: number;
    state: CalendarSyncState;
  }): Promise<ScopedMutationResult> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        connectionId: z.uuid(),
        expectedRevision: z.number().int().nonnegative(),
        state: calendarSyncStateSchema,
      })
      .parse(input);
    const updated = await this.database.begin(async (transaction) => {
      const rows = await transaction<{ id: string }[]>`
        update external_connections
        set cursor = jsonb_set(
              cursor, '{calendar}', ${this.database.json(JSON.parse(JSON.stringify(parsed.state)))}, true
            ),
            last_synced_at = now(), updated_at = now()
        where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and adult_id = ${parsed.adultId} and provider = 'google' and status = 'active'
          and coalesce((cursor->'calendar'->>'revision')::integer, 0) = ${parsed.expectedRevision}
        returning id
      `;
      if (!rows[0]) return false;
      await transaction`
        delete from calendar_busy_windows
        where connection_id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and owner_adult_id = ${parsed.adultId}
      `;
      return true;
    });
    if (updated) return "updated";
    return this.googleMutationFailure(parsed);
  }

  public async replaceCalendarWatch(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    calendarId: string;
    expectedRevision: number;
    state: CalendarSyncState;
    channel: {
      channelId: string;
      resourceId: string;
      resourceUri: string;
      channelToken: string;
      expiresAt: string;
    };
  }): Promise<ScopedMutationResult> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        connectionId: z.uuid(),
        calendarId: z.string().min(1).max(1_000),
        expectedRevision: z.number().int().nonnegative(),
        state: calendarSyncStateSchema,
        channel: z.strictObject({
          channelId: z.string().min(1).max(500),
          resourceId: z.string().min(1).max(1_000),
          resourceUri: z.string().min(1).max(4_096),
          channelToken: z.string().min(16).max(2_048),
          expiresAt: instantSchema,
        }),
      })
      .parse(input);
    if (parsed.state.calendarId !== parsed.calendarId) {
      throw new ApplicationStoreError("invalid_state", "Calendar watch and cursor do not match");
    }
    const tokenDigest = this.calendarChannelTokenDigest(parsed.channel.channelToken);
    const updated = await this.database.begin(async (transaction) => {
      const rows = await transaction<{ id: string }[]>`
        update external_connections
        set cursor = jsonb_set(
              cursor, '{calendar}', ${this.database.json(JSON.parse(JSON.stringify(parsed.state)))}, true
            ),
            updated_at = now()
        where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and adult_id = ${parsed.adultId} and provider = 'google' and status = 'active'
          and coalesce((cursor->'calendar'->>'revision')::integer, 0) = ${parsed.expectedRevision}
        returning id
      `;
      if (!rows[0]) return false;
      await transaction`
        update google_calendar_channels
        set status = 'retiring', updated_at = now()
        where connection_id = ${parsed.connectionId} and calendar_id = ${parsed.calendarId}
          and status = 'active'
      `;
      await transaction`
        insert into google_calendar_channels (
          channel_id, connection_id, household_id, adult_id, calendar_id,
          resource_id, resource_uri, token_digest, expires_at, status
        ) values (
          ${parsed.channel.channelId}, ${parsed.connectionId}, ${parsed.householdId},
          ${parsed.adultId}, ${parsed.calendarId}, ${parsed.channel.resourceId},
          ${parsed.channel.resourceUri}, ${tokenDigest}, ${parsed.channel.expiresAt}, 'active'
        )
      `;
      return true;
    });
    if (updated) return "updated";
    return this.googleMutationFailure(parsed);
  }

  public async markCalendarWatchStopped(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    channelId: string;
  }): Promise<"updated" | "not_found"> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        connectionId: z.uuid(),
        channelId: z.string().min(1).max(500),
      })
      .parse(input);
    const rows = await this.database<{ channel_id: string }[]>`
      update google_calendar_channels set status = 'stopped', updated_at = now()
      where channel_id = ${parsed.channelId} and connection_id = ${parsed.connectionId}
        and household_id = ${parsed.householdId} and adult_id = ${parsed.adultId}
        and status = 'retiring'
      returning channel_id
    `;
    return rows[0] ? "updated" : "not_found";
  }

  public async authenticateCalendarPush(input: {
    channelId: string;
    resourceId: string;
    resourceUri: string;
    channelToken: string;
    messageNumber: string;
    receivedAt: string;
  }): Promise<CalendarPushTarget | null> {
    const parsed = z
      .strictObject({
        channelId: z.string().min(1).max(500),
        resourceId: z.string().min(1).max(1_000),
        resourceUri: z.string().min(1).max(4_096),
        channelToken: z.string().min(1).max(2_048),
        messageNumber: z.string().regex(/^\d{1,40}$/u),
        receivedAt: instantSchema,
      })
      .parse(input);
    const tokenDigest = this.calendarChannelTokenDigest(parsed.channelToken);
    return this.database.begin(async (transaction) => {
      const rows = await transaction<
        {
          household_id: string;
          adult_id: string;
          connection_id: string;
          calendar_id: string;
          last_message_number: string | null;
        }[]
      >`
        select channel.household_id, channel.adult_id, channel.connection_id,
          channel.calendar_id, channel.last_message_number::text
        from google_calendar_channels channel
        join external_connections connection on connection.id = channel.connection_id
        where channel.channel_id = ${parsed.channelId}
          and channel.resource_id = ${parsed.resourceId}
          and channel.resource_uri = ${parsed.resourceUri}
          and channel.token_digest = ${tokenDigest}
          and channel.status in ('active', 'retiring') and channel.expires_at > ${parsed.receivedAt}
          and connection.status = 'active' and connection.provider = 'google'
          and connection.household_id = channel.household_id
          and connection.adult_id = channel.adult_id
        for update of channel
      `;
      const row = rows[0];
      if (!row) return null;
      if (
        row.last_message_number === null ||
        BigInt(parsed.messageNumber) > BigInt(row.last_message_number)
      ) {
        await transaction`
          update google_calendar_channels
          set last_message_number = ${parsed.messageNumber}::numeric, updated_at = now()
          where channel_id = ${parsed.channelId}
        `;
      }
      return {
        householdId: row.household_id,
        adultId: row.adult_id,
        connectionId: row.connection_id,
        calendarId: row.calendar_id,
      };
    });
  }

  public async persistPersonalCalendarSource(
    input: PersistPersonalCalendarSourceInput,
  ): Promise<{ sourceItemId: string; disposition: "inserted" | "unchanged" | "revised"; revision: number }> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        connectionId: z.uuid(),
        calendarId: z.string().min(1).max(1_000),
        externalId: z.string().min(1).max(1_000),
        kind: z.enum(["calendar_event", "calendar_event_deleted"]),
        occurredAt: instantSchema,
        contentHash: z.string().regex(/^sha256:[a-f0-9]{64}$/u),
        encryptedContent: z.string().min(1),
        metadata: z.record(z.string(), z.unknown()),
        busyWindow: z
          .strictObject({ startsAt: instantSchema, endsAt: instantSchema, allDay: z.boolean() })
          .nullable(),
      })
      .parse(input);
    const persisted = await this.applicationStore.persistSourceItem({
      householdId: parsed.householdId,
      connectionId: parsed.connectionId,
      ownerAdultId: parsed.adultId,
      visibility: "personal",
      provider: "google-calendar",
      externalId: `${parsed.calendarId}:${parsed.externalId}`,
      kind: parsed.kind,
      occurredAt: parsed.occurredAt,
      contentHash: parsed.contentHash,
      encryptedContent: parsed.encryptedContent,
      metadata: parsed.metadata,
    });
    if (parsed.busyWindow === null) {
      await this.database`
        delete from calendar_busy_windows
        where connection_id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and owner_adult_id = ${parsed.adultId} and calendar_id = ${parsed.calendarId}
          and external_event_id = ${parsed.externalId}
          and source_revision <= ${persisted.revision}
      `;
      return persisted;
    }
    const rows = await this.database<{ connection_id: string }[]>`
      insert into calendar_busy_windows (
        connection_id, household_id, owner_adult_id, calendar_id, external_event_id,
        source_item_id, source_revision, starts_at, ends_at, all_day
      )
      select connection.id, connection.household_id, connection.adult_id,
        ${parsed.calendarId}, ${parsed.externalId}, ${persisted.sourceItemId},
        ${persisted.revision}, ${parsed.busyWindow.startsAt}, ${parsed.busyWindow.endsAt},
        ${parsed.busyWindow.allDay}
      from external_connections connection
      where connection.id = ${parsed.connectionId} and connection.household_id = ${parsed.householdId}
        and connection.adult_id = ${parsed.adultId} and connection.provider = 'google'
        and connection.status = 'active'
      on conflict (connection_id, calendar_id, external_event_id)
      do update set source_item_id = excluded.source_item_id,
        source_revision = excluded.source_revision, starts_at = excluded.starts_at,
        ends_at = excluded.ends_at, all_day = excluded.all_day, updated_at = now()
      where calendar_busy_windows.source_revision <= excluded.source_revision
      returning connection_id
    `;
    if (!rows[0]) {
      throw new ApplicationStoreError("not_authorized", "Calendar projection owner is inactive");
    }
    return persisted;
  }

  public async listPersonalCalendarBusyWindows(input: {
    householdId: string;
    adultId: string;
    asOf: string;
    from: string;
    to: string;
    limit: number;
  }): Promise<PersonalCalendarBusyWindowPage> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        asOf: instantSchema,
        from: instantSchema,
        to: instantSchema,
        limit: z.number().int().min(1).max(1_000),
      })
      .parse(input);
    if (Date.parse(parsed.from) >= Date.parse(parsed.to)) {
      throw new ApplicationStoreError("invalid_state", "Calendar projection range is invalid");
    }
    const connections = await this.database<{ cursor: Record<string, unknown> }[]>`
      select cursor from external_connections
      where household_id = ${parsed.householdId} and adult_id = ${parsed.adultId}
        and provider = 'google' and status = 'active'
        and granted_scopes && ${[GOOGLE_CALENDAR_READONLY_SCOPE, GOOGLE_CALENDAR_EVENTS_SCOPE]}::text[]
      order by id
    `;
    if (connections.length === 0) {
      return { windows: [], complete: true, synchronizedAt: null };
    }
    const states = connections.map((connection) =>
      calendarSyncStateSchema.safeParse(connection.cursor.calendar),
    );
    if (states.some((state) => !state.success || !state.data.projectionReady)) {
      return { windows: [], complete: false, synchronizedAt: null };
    }
    const readyStates = states.flatMap((state) => (state.success ? [state.data] : []));
    const earliestSynchronization = readyStates
      .map((state) => state.lastSuccessfulSyncAt)
      .filter((value): value is string => value !== null)
      .sort()[0];
    const synchronizedAt = earliestSynchronization ? new Date(earliestSynchronization).toISOString() : null;
    if (synchronizedAt === null || Date.parse(synchronizedAt) < Date.parse(parsed.asOf) - 30 * 60_000) {
      return { windows: [], complete: false, synchronizedAt };
    }
    const rows = await this.database<{ starts_at: Date; ends_at: Date; all_day: boolean }[]>`
      select busy.starts_at, busy.ends_at, busy.all_day
      from calendar_busy_windows busy
      join external_connections connection on connection.id = busy.connection_id
      where busy.household_id = ${parsed.householdId} and busy.owner_adult_id = ${parsed.adultId}
        and connection.household_id = busy.household_id
        and connection.adult_id = busy.owner_adult_id
        and connection.status = 'active' and connection.provider = 'google'
        and connection.cursor->'calendar'->>'phase' = 'live'
        and coalesce((connection.cursor->'calendar'->>'projectionReady')::boolean, false)
        and busy.starts_at < ${parsed.to} and busy.ends_at > ${parsed.from}
      order by busy.starts_at, busy.ends_at, busy.connection_id, busy.external_event_id
      limit ${parsed.limit + 1}
    `;
    return {
      windows: rows.slice(0, parsed.limit).map((row) => ({
        startsAt: row.starts_at.toISOString(),
        endsAt: row.ends_at.toISOString(),
        allDay: row.all_day,
      })),
      complete: rows.length <= parsed.limit,
      synchronizedAt,
    };
  }

  /**
   * Selects one owned write target and reduces every adult's private projection to
   * an opaque availability version. Personal event identities never leave this store.
   */
  public async prepareCreate(input: {
    householdId: string;
    verifiedAdultIds: readonly string[];
    requestedByAdultId: string;
    asOf: string;
    startsAt: string;
    endsAt: string;
    accountLabel?: string;
    targetConnectionId?: string;
  }): Promise<HouseholdCalendarCreatePreparation> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        verifiedAdultIds: z.array(z.uuid()).min(1).max(20),
        requestedByAdultId: z.uuid(),
        asOf: instantSchema,
        startsAt: instantSchema,
        endsAt: instantSchema,
        accountLabel: z.string().trim().min(1).max(200).optional(),
        targetConnectionId: z.uuid().optional(),
      })
      .parse(input);
    if (Date.parse(parsed.startsAt) >= Date.parse(parsed.endsAt)) {
      throw new ApplicationStoreError("invalid_state", "Calendar availability range is invalid");
    }
    if (!parsed.verifiedAdultIds.includes(parsed.requestedByAdultId)) {
      throw new ApplicationStoreError("not_authorized", "Calendar requester is not a verified adult");
    }
    const activeMembers = await this.database<{ adult_id: string }[]>`
      select adult_id from household_memberships
      where household_id = ${parsed.householdId} and status = 'active'
      order by adult_id
    `;
    const expectedAdults = [...new Set(parsed.verifiedAdultIds)].sort();
    const storedAdults = activeMembers.map((row) => row.adult_id).sort();
    if (
      expectedAdults.length !== storedAdults.length ||
      expectedAdults.some((adultId, index) => adultId !== storedAdults[index])
    ) {
      return { status: "unavailable", reason: "projection_incomplete" };
    }

    const writeTargets = await this.database<
      { id: string; label: string; metadata: Record<string, unknown> }[]
    >`
      select id, label, metadata
      from external_connections
      where household_id = ${parsed.householdId} and adult_id = ${parsed.requestedByAdultId}
        and provider = 'google' and status = 'active'
        and granted_scopes && ${[GOOGLE_CALENDAR_EVENTS_SCOPE]}::text[]
      order by created_at, id
    `;
    const normalizedLabel = parsed.accountLabel?.toLocaleLowerCase("en-US");
    const candidates = writeTargets.filter((target) => {
      if (parsed.targetConnectionId !== undefined) return target.id === parsed.targetConnectionId;
      if (normalizedLabel === undefined) return true;
      const label = String(target.metadata.accountLabel ?? target.label)
        .trim()
        .toLocaleLowerCase("en-US");
      return label === normalizedLabel;
    });
    if (candidates.length === 0) return { status: "unavailable", reason: "no_write_connection" };
    if (candidates.length !== 1) {
      return { status: "unavailable", reason: "ambiguous_write_connection" };
    }
    const target = candidates[0];
    if (target === undefined) return { status: "unavailable", reason: "no_write_connection" };

    const connections = await this.database<
      { id: string; adult_id: string; cursor: Record<string, unknown> }[]
    >`
      select id, adult_id, cursor
      from external_connections
      where household_id = ${parsed.householdId}
        and adult_id = any(${expectedAdults}::uuid[])
        and provider = 'google' and status = 'active'
        and granted_scopes && ${[GOOGLE_CALENDAR_READONLY_SCOPE, GOOGLE_CALENDAR_EVENTS_SCOPE]}::text[]
      order by adult_id, id
    `;
    const coverage: Array<{ connectionId: string; adultId: string; calendarId: string }> = [];
    for (const connection of connections) {
      const state = calendarSyncStateSchema.safeParse(connection.cursor.calendar);
      if (
        !state.success ||
        state.data.phase !== "live" ||
        !state.data.projectionReady ||
        state.data.calendarId !== "primary" ||
        state.data.lastSuccessfulSyncAt === null ||
        Date.parse(state.data.lastSuccessfulSyncAt) < Date.parse(parsed.asOf) - 30 * 60_000 ||
        Date.parse(state.data.initialTimeMin) > Date.parse(parsed.startsAt) ||
        Date.parse(state.data.initialTimeMax) < Date.parse(parsed.endsAt)
      ) {
        return { status: "unavailable", reason: "projection_incomplete" };
      }
      coverage.push({
        connectionId: connection.id,
        adultId: connection.adult_id,
        calendarId: state.data.calendarId,
      });
    }
    if (!coverage.some((item) => item.connectionId === target.id)) {
      return { status: "unavailable", reason: "projection_incomplete" };
    }

    const overlaps = await this.database<
      {
        connection_id: string;
        owner_adult_id: string;
        calendar_id: string;
        external_event_id: string;
        source_revision: number;
        starts_at: Date;
        ends_at: Date;
      }[]
    >`
      select busy.connection_id, busy.owner_adult_id, busy.calendar_id,
        busy.external_event_id, busy.source_revision, busy.starts_at, busy.ends_at
      from calendar_busy_windows busy
      join external_connections connection on connection.id = busy.connection_id
      where busy.household_id = ${parsed.householdId}
        and busy.owner_adult_id = any(${expectedAdults}::uuid[])
        and connection.household_id = busy.household_id
        and connection.adult_id = busy.owner_adult_id
        and connection.provider = 'google' and connection.status = 'active'
        and connection.granted_scopes && ${[
          GOOGLE_CALENDAR_READONLY_SCOPE,
          GOOGLE_CALENDAR_EVENTS_SCOPE,
        ]}::text[]
        and busy.starts_at < ${parsed.endsAt} and busy.ends_at > ${parsed.startsAt}
      order by busy.owner_adult_id, busy.connection_id, busy.calendar_id,
        busy.external_event_id, busy.source_revision
    `;
    const digestMaterial = canonicalJson({
      schemaVersion: 1,
      householdId: parsed.householdId,
      verifiedAdultIds: expectedAdults,
      targetConnectionId: target.id,
      calendarId: "primary",
      startsAt: parsed.startsAt,
      endsAt: parsed.endsAt,
      coverage,
      overlaps: overlaps.map((row) => ({
        connectionId: row.connection_id,
        adultId: row.owner_adult_id,
        calendarId: row.calendar_id,
        externalEventId: row.external_event_id,
        sourceRevision: row.source_revision,
        startsAt: row.starts_at.toISOString(),
        endsAt: row.ends_at.toISOString(),
      })),
    });
    return {
      status: "ready",
      targetConnectionId: target.id,
      calendarId: "primary",
      relevantDataDigest: `sha256:${createHmac("sha256", this.identityKey)
        .update(digestMaterial)
        .digest("hex")}`,
      hasConflict: overlaps.length > 0,
    };
  }

  public async persistPersonalGmailSource(
    input: PersistPersonalGmailSourceInput,
  ): Promise<PersistPersonalGmailSourceResult> {
    return this.applicationStore.persistSourceItem({
      householdId: input.householdId,
      connectionId: input.connectionId,
      ownerAdultId: input.adultId,
      visibility: "personal",
      provider: "gmail",
      externalId: input.externalId,
      kind: input.kind,
      occurredAt: input.occurredAt,
      contentHash: input.contentHash,
      encryptedContent: input.encryptedContent,
      metadata: input.metadata,
    });
  }

  public async markConnectionStatus(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    status: "reauth_required" | "error";
  }): Promise<"updated" | "not_found"> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        connectionId: z.uuid(),
        status: z.enum(["reauth_required", "error"]),
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update external_connections set status = ${parsed.status}, updated_at = now()
      where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
        and adult_id = ${parsed.adultId} and provider = 'google' and status <> 'revoked'
      returning id
    `;
    return rows[0] ? "updated" : "not_found";
  }

  public async revokeConnection(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    revokedAt: string;
  }): Promise<"revoked" | "not_found"> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        connectionId: z.uuid(),
        revokedAt: instantSchema,
      })
      .parse(input);
    return this.database.begin(async (transaction) => {
      const rows = await transaction<{ id: string }[]>`
        update external_connections
        set status = 'revoked', encrypted_credentials = null, granted_scopes = '{}',
          cursor = '{}'::jsonb,
          metadata = metadata || ${this.database.json({ revokedAt: parsed.revokedAt })},
          updated_at = now()
        where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and adult_id = ${parsed.adultId} and provider = 'google'
        returning id
      `;
      if (!rows[0]) return "not_found" as const;
      await transaction`
        update google_calendar_channels set status = 'stopped', updated_at = now()
        where connection_id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and adult_id = ${parsed.adultId} and status in ('active', 'retiring')
      `;
      await transaction`
        delete from calendar_busy_windows
        where connection_id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and owner_adult_id = ${parsed.adultId}
      `;
      return "revoked" as const;
    });
  }

  public async updateGoogleConnectionState(input: {
    connectionId: string;
    householdId: string;
    adultId: string;
    encryptedCredentials?: string;
    cursor: Record<string, unknown>;
    metadata: Record<string, unknown>;
    lastSyncedAt: string;
    status?: "active" | "reauth_required" | "error";
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        connectionId: z.uuid(),
        householdId: z.uuid(),
        adultId: z.uuid(),
        encryptedCredentials: z.string().min(1).optional(),
        cursor: z.record(z.string(), z.unknown()),
        metadata: z.record(z.string(), z.unknown()),
        lastSyncedAt: instantSchema,
        status: z.enum(["active", "reauth_required", "error"]).default("active"),
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update external_connections
      set encrypted_credentials = coalesce(${parsed.encryptedCredentials ?? null}, encrypted_credentials),
        cursor = ${this.database.json(JSON.parse(JSON.stringify(parsed.cursor)))},
        metadata = ${this.database.json(JSON.parse(JSON.stringify(parsed.metadata)))},
        last_synced_at = ${parsed.lastSyncedAt}, status = ${parsed.status}, updated_at = now()
      where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
        and adult_id = ${parsed.adultId} and provider = 'google' and status <> 'revoked'
      returning id
    `;
    return rows.length === 1;
  }

  public async enqueueCalendarSyncWork(input: {
    idempotencyKey: string;
    householdId: string;
    work: CalendarSyncWork;
  }): Promise<{ jobId: string; created: boolean }> {
    const parsed = z
      .strictObject({
        idempotencyKey: z.string().min(1).max(512),
        householdId: z.uuid(),
        work: calendarSyncWorkSchema,
      })
      .parse(input);
    if (parsed.work.householdId !== parsed.householdId) {
      throw new ApplicationStoreError("invalid_state", "Calendar work household does not match");
    }
    const id = randomUUID();
    const rows = await this.database<{ id: string }[]>`
      insert into jobs (id, household_id, kind, status, payload, max_attempts, idempotency_key)
      values (
        ${id}, ${parsed.householdId}, 'google.calendar.sync', 'pending',
        ${this.database.json(JSON.parse(JSON.stringify(parsed.work)))}, 8, ${parsed.idempotencyKey}
      )
      on conflict (idempotency_key) where idempotency_key is not null do nothing
      returning id
    `;
    if (rows[0]) return { jobId: rows[0].id, created: true };
    const existing = await this.database<{ id: string; household_id: string; payload: unknown }[]>`
      select id, household_id, payload from jobs where idempotency_key = ${parsed.idempotencyKey}
    `;
    const row = existing[0];
    if (
      !row ||
      row.household_id !== parsed.householdId ||
      canonicalJson(row.payload) !== canonicalJson(parsed.work)
    ) {
      throw new ApplicationStoreError("invalid_state", "Calendar sync idempotency key conflict");
    }
    return { jobId: row.id, created: false };
  }

  public async reconcileCalendarSyncWork(asOf: string): Promise<number> {
    const now = instantSchema.parse(asOf);
    const rows = await this.database<
      {
        id: string;
        household_id: string;
        adult_id: string;
        cursor: Record<string, unknown>;
        updated_at: Date;
      }[]
    >`
      select id, household_id, adult_id, cursor, updated_at
      from external_connections
      where provider = 'google' and status = 'active'
        and granted_scopes && ${[GOOGLE_CALENDAR_READONLY_SCOPE, GOOGLE_CALENDAR_EVENTS_SCOPE]}::text[]
      order by updated_at
    `;
    let created = 0;
    for (const row of rows) {
      const stateResult = calendarSyncStateSchema.safeParse(row.cursor.calendar);
      if (row.cursor.calendar !== undefined && !stateResult.success) {
        await this.markConnectionStatus({
          householdId: row.household_id,
          adultId: row.adult_id,
          connectionId: row.id,
          status: "error",
        });
        continue;
      }
      const planned = calendarWorkForState(row, stateResult.success ? stateResult.data : null, now);
      if (planned === null) continue;
      const receipt = await this.enqueueCalendarSyncWork({
        householdId: row.household_id,
        idempotencyKey: calendarWorkIdempotencyKey(
          row.id,
          planned.work,
          planned.revision,
          now,
          row.updated_at.toISOString(),
        ),
        work: planned.work,
      });
      if (receipt.created) created += 1;
    }
    return created;
  }

  public async claimCalendarSyncWork(input: {
    owner: string;
    limit: number;
    leaseSeconds: number;
  }): Promise<ClaimedCalendarSyncWork[]> {
    const parsed = z
      .strictObject({
        owner: z.string().min(1).max(200),
        limit: z.number().int().positive().max(100),
        leaseSeconds: z.number().int().positive().max(3_600),
      })
      .parse(input);
    const leaseToken = randomUUID();
    return this.database.begin(async (transaction) => {
      await transaction`
        update jobs set status = 'dead', lease_owner = null, lease_token = null,
          lease_expires_at = null, last_error_code = 'lease_expired_after_max_attempts', updated_at = now()
        where kind = 'google.calendar.sync' and status = 'leased' and lease_expires_at < now()
          and attempt >= max_attempts
      `;
      const rows = await transaction<
        {
          id: string;
          payload: unknown;
          attempt: number;
          max_attempts: number;
          lease_token: string;
        }[]
      >`
        with candidates as (
          select id from jobs
          where kind = 'google.calendar.sync' and (
            (status in ('pending', 'retry') and available_at <= now())
            or (status = 'leased' and lease_expires_at < now())
          )
          order by available_at, created_at
          for update skip locked
          limit ${parsed.limit}
        )
        update jobs
        set status = 'leased', lease_owner = ${parsed.owner}, lease_token = ${leaseToken},
          lease_expires_at = now() + (${parsed.leaseSeconds} * interval '1 second'),
          attempt = attempt + 1, updated_at = now()
        from candidates where jobs.id = candidates.id
        returning jobs.id, jobs.payload, jobs.attempt, jobs.max_attempts, jobs.lease_token
      `;
      return rows.map((row) => ({
        rowId: row.id,
        leaseToken: row.lease_token,
        work: calendarSyncWorkSchema.parse(row.payload),
        attempt: row.attempt,
        maxAttempts: row.max_attempts,
      }));
    });
  }

  public async completeCalendarSyncWork(input: { rowId: string; leaseToken: string }): Promise<boolean> {
    return this.settleCalendarSyncWork(input, "succeeded");
  }

  public async retryCalendarSyncWork(input: {
    rowId: string;
    leaseToken: string;
    retryAt: string;
    errorCode: string;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        retryAt: instantSchema,
        errorCode: z.string().regex(/^[a-z0-9_.-]{1,100}$/u),
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update jobs set status = case when attempt >= max_attempts then 'dead' else 'retry' end,
        available_at = ${parsed.retryAt}, last_error_code = ${parsed.errorCode},
        lease_owner = null, lease_token = null, lease_expires_at = null, updated_at = now()
      where id = ${parsed.rowId} and kind = 'google.calendar.sync' and status = 'leased'
        and lease_token = ${parsed.leaseToken}
      returning id
    `;
    return rows.length === 1;
  }

  public async deadLetterCalendarSyncWork(input: {
    rowId: string;
    leaseToken: string;
    errorCode: string;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        errorCode: z.string().regex(/^[a-z0-9_.-]{1,100}$/u),
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update jobs set status = 'dead', last_error_code = ${parsed.errorCode},
        lease_owner = null, lease_token = null, lease_expires_at = null, updated_at = now()
      where id = ${parsed.rowId} and kind = 'google.calendar.sync' and status = 'leased'
        and lease_token = ${parsed.leaseToken}
      returning id
    `;
    return rows.length === 1;
  }

  public async enqueueGoogleSyncWork(input: {
    idempotencyKey: string;
    householdId: string;
    work: GmailSyncWork;
  }): Promise<{ jobId: string; created: boolean }> {
    const parsed = z
      .strictObject({
        idempotencyKey: z.string().min(1).max(512),
        householdId: z.uuid(),
        work: gmailSyncWorkSchema,
      })
      .parse(input);
    const id = randomUUID();
    const rows = await this.database<{ id: string }[]>`
      insert into jobs (
        id, household_id, kind, status, payload, max_attempts, idempotency_key
      ) values (
        ${id}, ${parsed.householdId}, 'google.sync', 'pending',
        ${this.database.json(JSON.parse(JSON.stringify(parsed.work)))}, 8,
        ${parsed.idempotencyKey}
      )
      on conflict (idempotency_key) where idempotency_key is not null do nothing
      returning id
    `;
    if (rows[0]) return { jobId: rows[0].id, created: true };
    const existing = await this.database<{ id: string; household_id: string; payload: unknown }[]>`
      select id, household_id, payload from jobs where idempotency_key = ${parsed.idempotencyKey}
    `;
    const row = existing[0];
    if (
      !row ||
      row.household_id !== parsed.householdId ||
      canonicalJson(row.payload) !== canonicalJson(parsed.work)
    ) {
      throw new ApplicationStoreError("invalid_state", "Google sync idempotency key conflict");
    }
    return { jobId: row.id, created: false };
  }

  public async reconcileGoogleSyncWork(asOf: string): Promise<number> {
    const now = instantSchema.parse(asOf);
    const rows = await this.database<
      { id: string; household_id: string; adult_id: string; cursor: Record<string, unknown> }[]
    >`
      select id, household_id, adult_id, cursor
      from external_connections
      where provider = 'google' and status = 'active' and cursor ? 'gmail'
      order by updated_at
    `;
    let created = 0;
    for (const row of rows) {
      const state = gmailSyncStateSchema.safeParse(row.cursor.gmail);
      if (!state.success) continue;
      const work = googleWorkForState(row, state.data, now);
      if (!work) continue;
      const receipt = await this.enqueueGoogleSyncWork({
        householdId: row.household_id,
        idempotencyKey: `google:${row.id}:${work.kind}:revision:${state.data.revision}`,
        work,
      });
      if (receipt.created) created += 1;
    }
    return created;
  }

  public async claimGoogleSyncWork(input: {
    owner: string;
    limit: number;
    leaseSeconds: number;
  }): Promise<ClaimedGoogleSyncWork[]> {
    const parsed = z
      .strictObject({
        owner: z.string().min(1).max(200),
        limit: z.number().int().positive().max(100),
        leaseSeconds: z.number().int().positive().max(3_600),
      })
      .parse(input);
    const leaseToken = randomUUID();
    return this.database.begin(async (transaction) => {
      await transaction`
        update jobs set status = 'dead', lease_owner = null, lease_token = null,
          lease_expires_at = null, last_error_code = 'lease_expired_after_max_attempts', updated_at = now()
        where kind = 'google.sync' and status = 'leased' and lease_expires_at < now()
          and attempt >= max_attempts
      `;
      const rows = await transaction<
        {
          id: string;
          payload: unknown;
          attempt: number;
          max_attempts: number;
          lease_token: string;
        }[]
      >`
        with candidates as (
          select id from jobs
          where kind = 'google.sync' and (
            (status in ('pending', 'retry') and available_at <= now())
            or (status = 'leased' and lease_expires_at < now())
          )
          order by available_at, created_at
          for update skip locked
          limit ${parsed.limit}
        )
        update jobs
        set status = 'leased', lease_owner = ${parsed.owner}, lease_token = ${leaseToken},
          lease_expires_at = now() + (${parsed.leaseSeconds} * interval '1 second'),
          attempt = attempt + 1, updated_at = now()
        from candidates where jobs.id = candidates.id
        returning jobs.id, jobs.payload, jobs.attempt, jobs.max_attempts, jobs.lease_token
      `;
      return rows.map((row) => ({
        rowId: row.id,
        leaseToken: row.lease_token,
        work: gmailSyncWorkSchema.parse(row.payload),
        attempt: row.attempt,
        maxAttempts: row.max_attempts,
      }));
    });
  }

  public async completeGoogleSyncWork(input: { rowId: string; leaseToken: string }): Promise<boolean> {
    const parsed = z.strictObject({ rowId: z.uuid(), leaseToken: z.uuid() }).parse(input);
    const rows = await this.database<{ id: string }[]>`
      update jobs set status = 'succeeded', lease_owner = null, lease_token = null,
        lease_expires_at = null, updated_at = now()
      where id = ${parsed.rowId} and kind = 'google.sync' and status = 'leased'
        and lease_token = ${parsed.leaseToken}
      returning id
    `;
    return rows.length === 1;
  }

  public async retryGoogleSyncWork(input: {
    rowId: string;
    leaseToken: string;
    retryAt: string;
    errorCode: string;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        retryAt: instantSchema,
        errorCode: z.string().regex(/^[a-z0-9_.-]{1,100}$/u),
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update jobs set status = case when attempt >= max_attempts then 'dead' else 'retry' end,
        available_at = ${parsed.retryAt}, last_error_code = ${parsed.errorCode},
        lease_owner = null, lease_token = null, lease_expires_at = null, updated_at = now()
      where id = ${parsed.rowId} and kind = 'google.sync' and status = 'leased'
        and lease_token = ${parsed.leaseToken}
      returning id
    `;
    return rows.length === 1;
  }

  public async deadLetterGoogleSyncWork(input: {
    rowId: string;
    leaseToken: string;
    errorCode: string;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: z.uuid(),
        leaseToken: z.uuid(),
        errorCode: z.string().regex(/^[a-z0-9_.-]{1,100}$/u),
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update jobs set status = 'dead', last_error_code = ${parsed.errorCode},
        lease_owner = null, lease_token = null, lease_expires_at = null, updated_at = now()
      where id = ${parsed.rowId} and kind = 'google.sync' and status = 'leased'
        and lease_token = ${parsed.leaseToken}
      returning id
    `;
    return rows.length === 1;
  }

  public async firstActiveOwner(householdId: string): Promise<string | null> {
    const rows = await this.database<{ adult_id: string }[]>`
      select adult_id from household_memberships
      where household_id = ${z.uuid().parse(householdId)} and role = 'owner' and status = 'active'
      order by created_at limit 1
    `;
    return rows[0]?.adult_id ?? null;
  }

  public digestHandle(handle: string): string {
    return createHmac("sha256", this.identityKey)
      .update(`linq-handle:v1:${canonicalizeLinqHandle(handle)}`)
      .digest("hex");
  }

  private async privateHandle(householdId: string, adultId: string): Promise<string | undefined> {
    const rows = await this.database<{ external_handle: string }[]>`
      select external_handle from channel_bindings
      where household_id = ${householdId} and adult_id = ${adultId}
        and provider = 'linq' and channel_type = 'private'
      order by updated_at desc limit 1
    `;
    return rows[0]?.external_handle;
  }

  private calendarChannelTokenDigest(channelToken: string): string {
    return createHmac("sha256", this.identityKey)
      .update(`google-calendar-channel:v1:${channelToken}`)
      .digest("hex");
  }

  private async settleCalendarSyncWork(
    input: { rowId: string; leaseToken: string },
    status: "succeeded",
  ): Promise<boolean> {
    const parsed = z.strictObject({ rowId: z.uuid(), leaseToken: z.uuid() }).parse(input);
    const rows = await this.database<{ id: string }[]>`
      update jobs set status = ${status}, lease_owner = null, lease_token = null,
        lease_expires_at = null, updated_at = now()
      where id = ${parsed.rowId} and kind = 'google.calendar.sync' and status = 'leased'
        and lease_token = ${parsed.leaseToken}
      returning id
    `;
    return rows.length === 1;
  }

  private async googleMutationFailure(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
  }): Promise<Exclude<ScopedMutationResult, "updated">> {
    const rows = await this.database<{ status: string }[]>`
      select status from external_connections
      where id = ${input.connectionId} and household_id = ${input.householdId}
        and adult_id = ${input.adultId} and provider = 'google'
      limit 1
    `;
    if (!rows[0]) return "not_found";
    return rows[0].status === "active" ? "conflict" : "inactive";
  }
}

function calendarWorkForState(
  row: { id: string; household_id: string; adult_id: string },
  state: CalendarSyncState | null,
  asOf: string,
): { work: CalendarSyncWork; revision: number } | null {
  const identity = {
    householdId: row.household_id,
    adultId: row.adult_id,
    connectionId: row.id,
    calendarId: state?.calendarId ?? "primary",
  };
  if (state === null) {
    return { work: calendarSyncWorkSchema.parse({ kind: "start", ...identity }), revision: 0 };
  }
  if (state.phase === "initial" || state.pageToken !== null || !state.projectionReady) {
    return {
      work: calendarSyncWorkSchema.parse({ kind: "continue", ...identity }),
      revision: state.revision,
    };
  }
  const horizonRefreshThreshold = Date.parse(asOf) + 365 * 24 * 60 * 60_000;
  if (Date.parse(state.initialTimeMax) <= horizonRefreshThreshold) {
    return {
      work: calendarSyncWorkSchema.parse({ kind: "refresh_horizon", ...identity }),
      revision: state.revision,
    };
  }
  const renewalThreshold = Date.parse(asOf) + 24 * 60 * 60_000;
  if (state.watch === null || Date.parse(state.watch.expiresAt) <= renewalThreshold) {
    return {
      work: calendarSyncWorkSchema.parse({ kind: "renew_watch", ...identity }),
      revision: state.revision,
    };
  }
  const staleThreshold = Date.parse(asOf) - 15 * 60_000;
  if (state.lastSuccessfulSyncAt === null || Date.parse(state.lastSuccessfulSyncAt) <= staleThreshold) {
    return {
      work: calendarSyncWorkSchema.parse({ kind: "scheduled", ...identity }),
      revision: state.revision,
    };
  }
  return null;
}

function calendarWorkIdempotencyKey(
  connectionId: string,
  work: CalendarSyncWork,
  revision: number,
  asOf: string,
  connectionEpoch: string,
): string {
  if (work.kind === "start") {
    return `google-calendar:${connectionId}:start:${connectionEpoch}`;
  }
  if (work.kind === "scheduled") {
    const bucket = Math.floor(Date.parse(asOf) / (15 * 60_000));
    return `google-calendar:${connectionId}:scheduled:${bucket}`;
  }
  return `google-calendar:${connectionId}:${work.kind}:revision:${revision}`;
}

export function canonicalizeLinqHandle(raw: string): string {
  const handle = handleSchema.parse(raw).normalize("NFKC").trim();
  if (handle.includes("@")) return handle.toLowerCase();
  const digits = handle.replace(/[\s().-]/gu, "");
  if (!/^\+[1-9]\d{7,14}$/u.test(digits)) {
    throw new Error("Linq phone handles must use E.164 format");
  }
  return digits;
}

function mapConnection(row: ConnectionRow): ExternalConnectionRecord {
  return {
    id: row.id,
    householdId: row.household_id,
    adultId: row.adult_id,
    provider: row.provider,
    label: row.label,
    externalAccountId: row.external_account_id,
    email: row.email,
    encryptedCredentials: row.encrypted_credentials,
    grantedScopes: row.granted_scopes,
    status: row.status,
    cursor: row.cursor,
    metadata: row.metadata,
    lastSyncedAt: row.last_synced_at?.toISOString() ?? null,
  };
}

function mapGoogleSyncConnection(row: ConnectionRow): GoogleSyncConnection {
  return {
    id: row.id,
    householdId: row.household_id,
    adultId: row.adult_id,
    provider: "google",
    externalAccountId: row.external_account_id,
    email: row.email,
    encryptedCredentials: row.encrypted_credentials,
    grantedScopes: row.granted_scopes,
    status: row.status,
    cursor: row.cursor,
    metadata: row.metadata,
  };
}

function googleWorkForState(
  connection: { id: string; household_id: string; adult_id: string },
  state: GmailSyncState,
  asOf: string,
): GmailSyncWork | null {
  const identity = {
    householdId: connection.household_id,
    adultId: connection.adult_id,
    connectionId: connection.id,
  };
  if (
    ["recent_90_days", "one_year_backfill", "full_history_backfill"].includes(state.phase) ||
    (state.phase === "live" && (state.history.pageToken !== null || state.history.targetId !== null))
  ) {
    return gmailSyncWorkSchema.parse({ kind: "continue", ...identity });
  }
  if (state.phase !== "live") return null;
  const renewBefore = new Date(asOf).getTime() + 24 * 60 * 60_000;
  if (state.watch === null || new Date(state.watch.expiresAt).getTime() <= renewBefore) {
    return gmailSyncWorkSchema.parse({ kind: "renew_watch", ...identity });
  }
  return null;
}
