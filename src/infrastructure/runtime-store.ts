import { createHmac, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import { z } from "zod";
import {
  GOOGLE_CALENDAR_EVENTS_SCOPE,
  GOOGLE_CALENDAR_READONLY_SCOPE,
  GOOGLE_GMAIL_READONLY_SCOPE,
  googleCalendarAccessRoleSchema,
} from "../adapters/google/index.js";
import { LinqApiError, type LinqChat, type LinqSendReceipt } from "../adapters/linq/index.js";
import {
  type ApplicationOutboxIntent,
  ApplicationOutboxIntentSchema,
  type ConversationResponseContext,
  ConversationResponseContextSchema,
  createApplicationProjection,
  createOnboardingProjection,
  type HouseholdApplicationSnapshot,
  HouseholdApplicationSnapshotSchema,
  type ProjectDeliveryGuard,
  ProjectDeliveryGuardSchema,
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
  DurableScopeSchema,
  HouseholdAggregateSchema,
  HouseholdIdSchema,
} from "../domain/index.js";
import { canonicalJson } from "../security/canonical-json.js";
import { SecretBox } from "../security/secret-box.js";
import {
  type CalendarBusyWindow,
  type CalendarCatalogState,
  type CalendarPushTarget,
  type CalendarSyncRecord,
  type CalendarSyncState,
  type CalendarSyncWork,
  calendarCatalogNameAad,
  calendarCatalogStateSchema,
  calendarSyncStateSchema,
  calendarSyncWorkSchema,
  type PersistPersonalCalendarSourceInput,
  type PersistPersonalCalendarSourceResult,
} from "./google-calendar-sync.js";
import type {
  GmailSyncState,
  GoogleSyncConnection,
  PersistPersonalGmailSourceInput,
  PersistPersonalGmailSourceResult,
  ScopedMutationResult,
} from "./google-sync.js";
import {
  type GmailSyncWork,
  gmailSourceMetadataSchema,
  gmailSyncStateSchema,
  gmailSyncWorkSchema,
} from "./google-sync.js";
import type { LinqChannelTarget, SerializedLinqSendResult } from "./worker-services.js";

const instantSchema = z.iso.datetime({ offset: true });
const handleSchema = z.string().trim().min(3).max(320);
const groupBindingMetadataSchema = z
  .object({
    participantHandleDigests: z.array(z.string().min(1)).length(2),
    selfHandleDigests: z.array(z.string().min(1)).length(1),
    service: z.literal("imessage"),
    healthStatus: z.literal("HEALTHY"),
  })
  .passthrough();
const invitationTransferIdentitySchema = z.strictObject({
  invitationId: z.uuid().optional(),
  sourceHouseholdId: z.uuid(),
  sourceAdultId: z.uuid(),
  sourceBindingId: z.uuid(),
  externalChatId: z.string().min(1).max(500),
  externalHandle: handleSchema,
});

export type PreparedInvitation = {
  invitationId: string;
  householdId: string;
  adultId: string;
  expiresAt: string;
};

export type PendingInvitation = PreparedInvitation & {
  invitedByAdultId: string;
};

export type InvitationTransferResolution =
  | {
      status: "ready";
      invitationId: string;
      householdId: string;
      adultId: string;
      requiresApplicationAcceptance: boolean;
    }
  | {
      status: "unavailable";
      reason:
        | "no_single_invitation"
        | "source_identity_changed"
        | "source_connections_active"
        | "same_household"
        | "target_state_changed";
    };

export type InvitationTransferFinalization =
  | { status: "activated"; resolution: ChannelResolution }
  | { status: "unavailable"; reason: Exclude<InvitationTransferResolution, { status: "ready" }>["reason"] };

export type GroupIdentity = {
  householdId: string;
  adultsByHandle: ReadonlyMap<string, string>;
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
    const deletionDigests = [
      this.deletionIdentityDigest("linq-chat", input.externalChatId),
      this.deletionIdentityDigest("linq-handle", handle),
    ];
    const deleted = await this.database<{ request_id: string }[]>`
      select request_id from customer_deletion_tombstones
      where routing_digests && ${deletionDigests}::text[] limit 1
    `;
    if (deleted[0]) {
      throw new ApplicationStoreError("not_authorized", "This Linq identity was deleted");
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

  /**
   * Resolves one pending invitation without changing which household owns the Linq identity.
   * Callers must carry the returned invitation and current binding IDs in an app-owned reply context.
   */
  public async resolveInvitationTransfer(input: {
    invitationId?: string;
    sourceHouseholdId: string;
    sourceAdultId: string;
    sourceBindingId: string;
    externalChatId: string;
    externalHandle: string;
  }): Promise<InvitationTransferResolution> {
    const parsed = invitationTransferIdentitySchema.parse(input);
    const identity = {
      ...parsed,
      externalHandle: canonicalizeLinqHandle(parsed.externalHandle),
    };
    return this.database.begin((transaction) => this.inspectInvitationTransfer(transaction, identity));
  }

  /**
   * Retires the source identity and activates the invited identity in one transaction. Household
   * history is retained under the original adult and binding rows; no data is copied or merged.
   */
  public async finalizeInvitationTransfer(input: {
    invitationId: string;
    sourceHouseholdId: string;
    sourceAdultId: string;
    sourceBindingId: string;
    externalChatId: string;
    externalHandle: string;
    consentedAt: string;
  }): Promise<InvitationTransferFinalization> {
    const parsed = invitationTransferIdentitySchema
      .extend({ invitationId: z.uuid(), consentedAt: instantSchema })
      .parse(input);
    const identity = {
      ...parsed,
      externalHandle: canonicalizeLinqHandle(parsed.externalHandle),
    };
    return this.database.begin(async (transaction) => {
      const transfer = await this.inspectInvitationTransfer(transaction, identity);
      if (transfer.status === "unavailable") return transfer;
      if (transfer.requiresApplicationAcceptance) {
        return { status: "unavailable", reason: "target_state_changed" } as const;
      }

      const revokedBindings = await transaction<{ id: string }[]>`
        update channel_bindings
        set status = 'revoked',
          metadata = metadata || ${this.database.json({ revocationReason: "invitation_transfer" })},
          updated_at = now()
        where id = ${identity.sourceBindingId}
          and household_id = ${identity.sourceHouseholdId}
          and adult_id = ${identity.sourceAdultId}
          and provider = 'linq' and channel_type = 'private'
          and external_chat_id = ${identity.externalChatId}
          and external_handle = ${identity.externalHandle}
          and status = 'active'
        returning id
      `;
      if (revokedBindings.length !== 1) {
        throw new ApplicationStoreError("invalid_state", "Invitation transfer source binding changed");
      }

      const revokedMemberships = await transaction<{ adult_id: string }[]>`
        update household_memberships
        set status = 'revoked', updated_at = now()
        where household_id = ${identity.sourceHouseholdId}
          and adult_id = ${identity.sourceAdultId} and status = 'active'
        returning adult_id
      `;
      if (revokedMemberships.length !== 1) {
        throw new ApplicationStoreError("invalid_state", "Invitation transfer source membership changed");
      }

      await transaction`
        update channel_bindings
        set status = 'paused',
          metadata = metadata || ${this.database.json({ pauseReason: "membership_transferred" })},
          updated_at = now()
        where household_id = ${identity.sourceHouseholdId}
          and provider = 'linq' and channel_type = 'group'
          and status in ('pending', 'active')
      `;
      await transaction`
        update households
        set status = 'paused', updated_at = now()
        where id = ${identity.sourceHouseholdId}
          and not exists (
            select 1 from household_memberships
            where household_id = ${identity.sourceHouseholdId}
              and role = 'owner' and status = 'active'
          )
      `;

      const acceptedInvitations = await transaction<{ id: string }[]>`
        update invitations
        set status = 'accepted', accepted_by_adult_id = ${transfer.adultId}, updated_at = now()
        where id = ${transfer.invitationId}
          and household_id = ${transfer.householdId}
          and invitee_adult_id = ${transfer.adultId}
          and invitee_handle_hash = ${this.digestHandle(identity.externalHandle)}
          and status = 'pending'
        returning id
      `;
      if (acceptedInvitations.length !== 1) {
        throw new ApplicationStoreError("invalid_state", "Invitation transfer invitation changed");
      }

      const activatedMemberships = await transaction<{ adult_id: string }[]>`
        update household_memberships
        set status = 'active', consented_at = ${parsed.consentedAt}, updated_at = now()
        where household_id = ${transfer.householdId} and adult_id = ${transfer.adultId}
          and status = 'invited'
        returning adult_id
      `;
      if (activatedMemberships.length !== 1) {
        throw new ApplicationStoreError("invalid_state", "Invitation transfer target membership changed");
      }

      const bindingId = randomUUID();
      const metadata = {
        inboundFirstAt: parsed.consentedAt,
        transferInvitationId: transfer.invitationId,
        transferredAt: parsed.consentedAt,
      };
      await transaction`
        insert into channel_bindings (
          id, household_id, adult_id, provider, channel_type, external_chat_id,
          external_handle, status, metadata
        ) values (
          ${bindingId}, ${transfer.householdId}, ${transfer.adultId}, 'linq', 'private',
          ${identity.externalChatId}, ${identity.externalHandle}, 'active',
          ${this.database.json(metadata)}
        )
      `;
      return {
        status: "activated",
        resolution: {
          bindingId,
          provider: "linq",
          channelType: "private",
          householdId: transfer.householdId,
          adultId: transfer.adultId,
          bindingStatus: "active",
          membershipStatus: "active",
          metadata,
        },
      };
    });
  }

  private async inspectInvitationTransfer(
    transaction: TransactionSql<Record<string, never>>,
    input: z.output<typeof invitationTransferIdentitySchema>,
  ): Promise<InvitationTransferResolution> {
    const handleHash = this.digestHandle(input.externalHandle);
    await transaction`select pg_advisory_xact_lock(hashtextextended(${`invite:${handleHash}`}, 0))`;

    const invitations = await transaction<
      Array<
        InvitationRow & {
          invitee_status: string;
          inviter_status: string;
          inviter_role: string;
          target_household_status: string;
          unexpired: boolean;
        }
      >
    >`
      select invitation.id, invitation.household_id, invitation.invited_by_adult_id,
        invitation.accepted_by_adult_id, invitation.invitee_adult_id, invitation.expires_at,
        invitee.status as invitee_status, inviter.status as inviter_status,
        inviter.role as inviter_role, household.status as target_household_status,
        invitation.expires_at > now() as unexpired
      from invitations invitation
      join household_memberships invitee
        on invitee.household_id = invitation.household_id
        and invitee.adult_id = invitation.invitee_adult_id
      join household_memberships inviter
        on inviter.household_id = invitation.household_id
        and inviter.adult_id = invitation.invited_by_adult_id
      join households household on household.id = invitation.household_id
      where invitation.invitee_handle_hash = ${handleHash}
        and invitation.status = 'pending'
      for update of invitation, invitee, inviter, household
    `;
    const invitation = invitations[0];
    if (
      invitations.length !== 1 ||
      invitation === undefined ||
      (input.invitationId !== undefined && invitation.id !== input.invitationId)
    ) {
      return { status: "unavailable", reason: "no_single_invitation" };
    }
    if (
      invitation.invitee_adult_id === null ||
      invitation.accepted_by_adult_id !== null ||
      invitation.invitee_status !== "invited" ||
      invitation.inviter_status !== "active" ||
      invitation.inviter_role !== "owner" ||
      invitation.target_household_status === "deleting"
    ) {
      return { status: "unavailable", reason: "target_state_changed" };
    }
    const inviteeAdultId = AdultIdSchema.parse(invitation.invitee_adult_id);
    if (invitation.household_id === input.sourceHouseholdId) {
      return { status: "unavailable", reason: "same_household" };
    }

    const sources = await transaction<
      Array<{
        id: string;
        household_id: string;
        adult_id: string | null;
        external_chat_id: string;
        status: ChannelResolution["bindingStatus"];
        membership_status: ChannelResolution["membershipStatus"];
        household_status: string;
      }>
    >`
      select binding.id, binding.household_id, binding.adult_id, binding.external_chat_id,
        binding.status, membership.status as membership_status,
        household.status as household_status
      from channel_bindings binding
      join household_memberships membership
        on membership.household_id = binding.household_id
        and membership.adult_id = binding.adult_id
      join households household on household.id = binding.household_id
      where binding.provider = 'linq' and binding.channel_type = 'private'
        and binding.external_handle = ${input.externalHandle}
        and binding.status in ('pending', 'active', 'paused')
      for update of binding, membership, household
    `;
    const source = sources[0];
    if (
      sources.length !== 1 ||
      source === undefined ||
      source.id !== input.sourceBindingId ||
      source.household_id !== input.sourceHouseholdId ||
      source.adult_id !== input.sourceAdultId ||
      source.external_chat_id !== input.externalChatId ||
      source.status !== "active" ||
      source.membership_status !== "active" ||
      source.household_status === "deleting"
    ) {
      return { status: "unavailable", reason: "source_identity_changed" };
    }
    const snapshots = await transaction<{ revision: string; aggregate: unknown; projection: unknown }[]>`
      select revision, aggregate, projection
      from application_snapshots
      where household_id = ${invitation.household_id}
      for update
    `;
    const snapshotRow = snapshots[0];
    const snapshot = HouseholdApplicationSnapshotSchema.safeParse(
      snapshotRow === undefined
        ? null
        : {
            revision: Number(snapshotRow.revision),
            aggregate: snapshotRow.aggregate,
            projection: snapshotRow.projection,
          },
    );
    if (
      !snapshot.success ||
      !snapshot.data.aggregate.verifiedAdultIds.includes(inviteeAdultId) ||
      snapshot.data.projection.onboarding.invitedAdultId !== inviteeAdultId
    ) {
      return { status: "unavailable", reason: "target_state_changed" };
    }
    const onboarding = snapshot.data.projection.onboarding;
    const alreadyConsented = onboarding.consentedAdultIds.includes(inviteeAdultId);
    let requiresApplicationAcceptance: boolean;
    if (onboarding.phase === "awaiting_invitee_consent" && !alreadyConsented) {
      if (!invitation.unexpired) {
        return { status: "unavailable", reason: "no_single_invitation" };
      }
      requiresApplicationAcceptance = true;
    } else if (onboarding.phase === "awaiting_group" && alreadyConsented) {
      if (input.invitationId === undefined && !invitation.unexpired) {
        return { status: "unavailable", reason: "no_single_invitation" };
      }
      requiresApplicationAcceptance = false;
    } else {
      return { status: "unavailable", reason: "target_state_changed" };
    }
    const activeConnections = await transaction<{ id: string }[]>`
      select id from external_connections
      where household_id = ${input.sourceHouseholdId}
        and adult_id = ${input.sourceAdultId}
        and status = 'active'
      for update
    `;
    if (activeConnections.length > 0) {
      return { status: "unavailable", reason: "source_connections_active" };
    }
    return {
      status: "ready",
      invitationId: invitation.id,
      householdId: invitation.household_id,
      adultId: invitation.invitee_adult_id,
      requiresApplicationAcceptance,
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
      const memberships = await transaction<{ adult_id: string }[]>`
        update household_memberships
        set status = 'active', consented_at = ${parsed.consentedAt}, updated_at = now()
        where household_id = ${parsed.householdId} and adult_id = ${parsed.adultId}
          and status = 'invited'
        returning adult_id
      `;
      if (memberships.length !== 1) {
        throw new ApplicationStoreError("invalid_state", "Invitation membership could not be activated");
      }
      const bindings = await transaction<{ id: string }[]>`
        update channel_bindings
        set status = 'active', updated_at = now()
        where provider = 'linq' and channel_type = 'private'
          and household_id = ${parsed.householdId} and adult_id = ${parsed.adultId}
          and external_chat_id = ${parsed.externalChatId}
          and external_handle = ${canonicalizeLinqHandle(parsed.externalHandle)}
          and status = 'pending'
        returning id
      `;
      if (bindings.length !== 1) {
        throw new ApplicationStoreError("invalid_state", "Invitation channel could not be activated");
      }
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
        and cb.status in ('active', 'paused') and hm.status = 'active'
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
    selfHandles: readonly string[];
    service: string;
    healthStatus: string;
  }): Promise<ChannelResolution | null> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        externalChatId: z.string().min(1).max(500),
        participantHandles: z.array(handleSchema).length(2),
        selfHandles: z.array(handleSchema).length(1),
        service: z.string().min(1).max(100),
        healthStatus: z.string().min(1).max(100),
      })
      .parse(input);
    const participants = uniqueCanonicalHandles(parsed.participantHandles);
    const selfHandles = uniqueCanonicalHandles(parsed.selfHandles);
    if (
      participants.length !== 2 ||
      selfHandles.length !== 1 ||
      parsed.service.toLowerCase() !== "imessage" ||
      parsed.healthStatus !== "HEALTHY"
    ) {
      await this.pauseGroupBinding({
        externalChatId: parsed.externalChatId,
        reason: "live_group_identity_mismatch",
      });
      return null;
    }
    const metadata = {
      participantHandleDigests: participants.map((handle) => this.digestHandle(handle)).sort(),
      selfHandleDigests: selfHandles.map((handle) => this.digestHandle(handle)).sort(),
      service: "imessage" as const,
      healthStatus: "HEALTHY" as const,
    };

    return this.database.begin(async (transaction) => {
      await lockLinqChat(transaction, parsed.externalChatId);
      const bindings = await transaction<
        { id: string; household_id: string; status: ChannelResolution["bindingStatus"]; metadata: unknown }[]
      >`
        select id, household_id, status, metadata
        from channel_bindings
        where provider = 'linq' and channel_type = 'group'
          and external_chat_id = ${parsed.externalChatId} and external_handle is null
        for update
      `;
      const binding = bindings[0];
      if (binding && binding.household_id !== parsed.householdId) {
        throw new ApplicationStoreError(
          "not_authorized",
          "External group is already bound to another household",
        );
      }
      if (binding?.status === "revoked") return null;
      if (binding && !groupMetadataMatches(binding.metadata, metadata)) {
        await transaction`
          update channel_bindings
          set status = 'paused',
            metadata = metadata || ${this.database.json({ pauseReason: "stored_group_identity_mismatch" })},
            updated_at = now()
          where id = ${binding.id} and status <> 'revoked'
        `;
        return null;
      }

      const otherBindings = await transaction<{ id: string }[]>`
        select id from channel_bindings
        where provider = 'linq' and channel_type = 'group'
          and household_id = ${parsed.householdId}
          and external_chat_id <> ${parsed.externalChatId}
          and status in ('pending', 'active', 'paused')
        for update
      `;
      if (otherBindings[0]) {
        await transaction`
          update channel_bindings
          set status = 'paused',
            metadata = metadata || ${this.database.json({ pauseReason: "competing_group_identity" })},
            updated_at = now()
          where id = ${otherBindings[0].id} and status <> 'revoked'
        `;
        return null;
      }

      if (
        !(await activeHouseholdHandlesMatch(transaction, parsed.householdId, participants)) ||
        (await isChatSuppressed(transaction, parsed.externalChatId, null))
      ) {
        if (binding) {
          await transaction`
            update channel_bindings set status = 'paused', updated_at = now()
            where id = ${binding.id} and status <> 'revoked'
          `;
        }
        return null;
      }

      const bindingId = binding?.id ?? randomUUID();
      if (binding) {
        await transaction`
          update channel_bindings
          set status = 'active', metadata = ${this.database.json(metadata)}, updated_at = now()
          where id = ${bindingId}
        `;
      } else {
        await transaction`
          insert into channel_bindings (
            id, household_id, adult_id, provider, channel_type, external_chat_id,
            external_handle, status, metadata
          ) values (
            ${bindingId}, ${parsed.householdId}, null, 'linq', 'group',
            ${parsed.externalChatId}, null, 'active', ${this.database.json(metadata)}
          )
        `;
      }
      return {
        bindingId,
        provider: "linq" as const,
        channelType: "group" as const,
        householdId: parsed.householdId,
        adultId: null,
        bindingStatus: "active" as const,
        membershipStatus: null,
        metadata,
      };
    });
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

  public async executeSerializedSend(input: {
    householdId: string;
    targetScope: DurableScope;
    responseContext?: ConversationResponseContext;
    deliveryGuard?: ProjectDeliveryGuard;
    loadGroupChat: (chatId: string) => Promise<LinqChat>;
    send: (chatId: string) => Promise<LinqSendReceipt>;
    allowWhileDeleting?: boolean;
  }): Promise<SerializedLinqSendResult> {
    const householdId = HouseholdIdSchema.parse(input.householdId);
    const targetScope = DurableScopeSchema.parse(input.targetScope);
    const responseContext =
      input.responseContext === undefined
        ? undefined
        : ConversationResponseContextSchema.parse(input.responseContext);
    const deliveryGuard =
      input.deliveryGuard === undefined ? undefined : ProjectDeliveryGuardSchema.parse(input.deliveryGuard);
    const candidateRows =
      targetScope.kind === "household"
        ? await this.database<{ external_chat_id: string }[]>`
            select external_chat_id from channel_bindings
            where household_id = ${householdId} and provider = 'linq' and channel_type = 'group'
            order by updated_at desc limit 1
          `
        : await this.database<{ external_chat_id: string }[]>`
            select external_chat_id from channel_bindings
            where household_id = ${householdId} and adult_id = ${targetScope.adultId}
              and provider = 'linq' and channel_type = 'private'
            order by updated_at desc limit 1
          `;
    const chatId = candidateRows[0]?.external_chat_id;
    if (!chatId) return { status: "inactive" };

    return this.database.begin(async (transaction) => {
      await lockLinqChat(transaction, chatId);
      const households = await transaction<{ status: string }[]>`
        select status from households where id = ${householdId} for update
      `;
      const household = households[0];
      if (!household) return { status: "inactive" };
      if (
        household.status === "deleting" &&
        !(input.allowWhileDeleting === true && targetScope.kind === "personal")
      ) {
        return { status: "inactive" };
      }
      const hasEpisodeResponseContext =
        responseContext?.kind === "episode_ownership" || responseContext?.kind === "episode_follow_up";
      if (hasEpisodeResponseContext || deliveryGuard !== undefined) {
        const snapshots = await transaction<{ revision: string; aggregate: unknown; projection: unknown }[]>`
          select revision, aggregate, projection
          from application_snapshots where household_id = ${householdId}
          limit 1
        `;
        const snapshotRow = snapshots[0];
        if (!snapshotRow) return { status: "obsolete" };
        const snapshot = HouseholdApplicationSnapshotSchema.safeParse({
          revision: Number(snapshotRow.revision),
          aggregate: snapshotRow.aggregate,
          projection: snapshotRow.projection,
        });
        if (!snapshot.success) {
          return { status: "obsolete" };
        }

        if (hasEpisodeResponseContext) {
          const episode = snapshot.data.aggregate.episodes.find(
            (candidate) => candidate.episodeId === responseContext.episodeId,
          );
          if (
            episode === undefined ||
            episode.version !== responseContext.episodeVersion ||
            canonicalJson(episode.scope) !== canonicalJson(targetScope)
          ) {
            return { status: "obsolete" };
          }
        }

        if (deliveryGuard !== undefined) {
          if (
            responseContext?.kind !== "episode_follow_up" ||
            responseContext.episodeId !== deliveryGuard.episodeId
          ) {
            return { status: "obsolete" };
          }
          const worker = snapshot.data.projection.workers.find(
            (candidate) =>
              candidate.episodeId === deliveryGuard.episodeId && candidate.job.jobId === deliveryGuard.jobId,
          );
          if (worker === undefined || worker.deliveryGeneration !== deliveryGuard.generation) {
            return { status: "obsolete" };
          }
        }
      }
      type SerializedBindingRow = {
        id: string;
        external_chat_id: string;
        status: ChannelResolution["bindingStatus"];
        metadata: unknown;
        external_handle: string | null;
        membership_status: ChannelResolution["membershipStatus"];
      };
      const rows =
        targetScope.kind === "household"
          ? await transaction<SerializedBindingRow[]>`
              select id, external_chat_id, status, metadata, external_handle,
                null::text as membership_status
              from channel_bindings
              where household_id = ${householdId} and provider = 'linq' and channel_type = 'group'
              order by updated_at desc limit 1
              for update
            `
          : await transaction<SerializedBindingRow[]>`
              select cb.id, cb.external_chat_id, cb.status, cb.metadata, cb.external_handle,
                hm.status as membership_status
              from channel_bindings cb
              join household_memberships hm
                on hm.household_id = cb.household_id and hm.adult_id = cb.adult_id
              where cb.household_id = ${householdId} and cb.adult_id = ${targetScope.adultId}
                and cb.provider = 'linq' and cb.channel_type = 'private'
              order by cb.updated_at desc limit 1
              for update of cb, hm
            `;
      const row = rows[0];
      if (!row || row.external_chat_id !== chatId) return { status: "inactive" };
      const privateEligible =
        targetScope.kind === "personal" &&
        ((row.status === "active" && row.membership_status === "active") ||
          (row.status === "pending" &&
            row.membership_status === "invited" &&
            typeof row.metadata === "object" &&
            row.metadata !== null &&
            "inboundFirstAt" in row.metadata));
      if (targetScope.kind === "household" ? row.status !== "active" : !privateEligible) {
        return { status: "inactive" };
      }
      if (targetScope.kind === "personal" && row.external_handle === null) {
        return { status: "inactive" };
      }
      const handleHash =
        targetScope.kind === "personal" ? this.digestHandle(row.external_handle as string) : null;
      if (await isChatSuppressed(transaction, chatId, handleHash)) return { status: "inactive" };

      if (targetScope.kind === "household") {
        let chat: LinqChat;
        try {
          chat = await input.loadGroupChat(chatId);
        } catch (error) {
          if (!(error instanceof LinqApiError) || error.retryable) throw error;
          await transaction`
            update channel_bindings
            set status = 'paused',
              metadata = metadata || ${this.database.json({ pauseReason: "provider_group_lookup_permanent" })},
              updated_at = now()
            where id = ${row.id} and status <> 'revoked'
          `;
          return { status: "binding_paused" };
        }
        if (!(await this.liveGroupAuthorityMatches(transaction, householdId, row.metadata, chat, chatId))) {
          await transaction`
            update channel_bindings
            set status = 'paused',
              metadata = metadata || ${this.database.json({ pauseReason: "live_group_authority_mismatch" })},
              updated_at = now()
            where id = ${row.id} and status <> 'revoked'
          `;
          return { status: "binding_paused" };
        }
      }

      const target: LinqChannelTarget = {
        householdId,
        targetScope,
        chatId,
        status: "active",
      };
      const receipt = await input.send(chatId);
      return { status: "sent", target, receipt };
    });
  }

  private async liveGroupAuthorityMatches(
    transaction: TransactionSql<Record<string, never>>,
    householdId: string,
    storedMetadata: unknown,
    chat: LinqChat,
    expectedChatId: string,
  ): Promise<boolean> {
    const live = normalizeHealthyLinqGroup(chat, expectedChatId);
    const stored = groupBindingMetadataSchema.safeParse(storedMetadata);
    if (!live || !stored.success) return false;
    const participantHandleDigests = live.participants.map((handle) => this.digestHandle(handle)).sort();
    const selfHandleDigests = live.selfHandles.map((handle) => this.digestHandle(handle)).sort();
    if (
      !sameStrings(participantHandleDigests, stored.data.participantHandleDigests) ||
      !sameStrings(selfHandleDigests, stored.data.selfHandleDigests)
    ) {
      return false;
    }
    return activeHouseholdHandlesMatch(transaction, householdId, live.participants);
  }

  public async pauseGroupBinding(input: { externalChatId: string; reason: string }): Promise<boolean> {
    const parsed = z
      .strictObject({
        externalChatId: z.string().min(1).max(500),
        reason: z.string().regex(/^[a-z][a-z0-9_]{0,99}$/u),
      })
      .parse(input);
    return this.database.begin(async (transaction) => {
      await lockLinqChat(transaction, parsed.externalChatId);
      const rows = await transaction<{ id: string }[]>`
        update channel_bindings
        set status = 'paused',
          metadata = metadata || ${this.database.json({ pauseReason: parsed.reason })},
          updated_at = now()
        where provider = 'linq' and channel_type = 'group'
          and external_chat_id = ${parsed.externalChatId} and status <> 'revoked'
        returning id
      `;
      return rows.length > 0;
    });
  }

  public async setSuppression(input: {
    externalChatId: string;
    externalHandle?: string;
    scope: "private" | "group";
    suppressed: boolean;
    occurredAt: string;
    sourceEventId: string;
    reason: string;
  }): Promise<{ applied: boolean; suppressed: boolean }> {
    const parsed = z
      .strictObject({
        externalChatId: z.string().min(1).max(500),
        externalHandle: handleSchema.optional(),
        scope: z.enum(["private", "group"]),
        suppressed: z.boolean(),
        occurredAt: instantSchema,
        sourceEventId: z.string().min(1).max(512),
        reason: z.string().min(1).max(200),
      })
      .superRefine((value, context) => {
        if ((value.scope === "private") !== (value.externalHandle !== undefined)) {
          context.addIssue({ code: "custom", message: "Private suppression requires one handle" });
        }
      })
      .parse(input);
    const hash = parsed.externalHandle ? this.digestHandle(parsed.externalHandle) : null;
    return this.database.begin(async (transaction) => {
      await lockLinqChat(transaction, parsed.externalChatId);
      const rows = await transaction<{ status: "suppressed" | "released" }[]>`
        insert into channel_suppressions (
          id, provider, external_chat_id, external_handle_hash, scope, status, reason,
          changed_at, source_event_id
        ) values (
          ${randomUUID()}, 'linq', ${parsed.externalChatId}, ${hash}, ${parsed.scope},
          ${parsed.suppressed ? "suppressed" : "released"}, ${parsed.reason},
          ${parsed.occurredAt}, ${parsed.sourceEventId}
        )
        on conflict (provider, external_chat_id, external_handle_hash)
        do update set status = excluded.status, reason = excluded.reason,
          changed_at = excluded.changed_at, source_event_id = excluded.source_event_id,
          updated_at = now()
        where excluded.changed_at > channel_suppressions.changed_at
          or (
            excluded.changed_at = channel_suppressions.changed_at
            and (
              (excluded.status = 'suppressed' and channel_suppressions.status <> 'suppressed')
              or (
                excluded.status = channel_suppressions.status
                and excluded.source_event_id > channel_suppressions.source_event_id
              )
            )
          )
        returning status
      `;
      const applied = rows.length === 1;
      if (applied) {
        await transaction`
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
      const current =
        rows[0] ??
        (
          await transaction<{ status: "suppressed" | "released" }[]>`
            select status from channel_suppressions
            where provider = 'linq' and external_chat_id = ${parsed.externalChatId}
              and external_handle_hash is not distinct from ${hash}
            limit 1
          `
        )[0];
      if (!current) throw new ApplicationStoreError("invalid_state", "Suppression state disappeared");
      if (!applied && current.status === "suppressed") {
        await transaction`
          update channel_bindings set status = 'paused', updated_at = now()
          where provider = 'linq' and external_chat_id = ${parsed.externalChatId}
            and (${hash}::text is null or external_handle = ${parsed.externalHandle ?? null})
            and status <> 'revoked'
        `;
      }
      return { applied, suppressed: current.status === "suppressed" };
    });
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
        state: gmailSyncStateSchema,
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

  public async publishGmailDiscoveryCompletion(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    expectedRevision: number;
    state: GmailSyncState;
    intent: ApplicationOutboxIntent;
  }): Promise<ScopedMutationResult> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        connectionId: z.uuid(),
        expectedRevision: z.number().int().nonnegative(),
        state: gmailSyncStateSchema,
        intent: ApplicationOutboxIntentSchema,
      })
      .parse(input);
    if (
      parsed.state.revision !== parsed.expectedRevision + 1 ||
      parsed.state.discovery?.status !== "published"
    ) {
      throw new ApplicationStoreError("invalid_state", "Gmail discovery publication state is invalid");
    }
    if (
      parsed.intent.kind !== "conversation.send" ||
      parsed.intent.householdId !== parsed.householdId ||
      parsed.intent.targetScope.kind !== "personal" ||
      parsed.intent.targetScope.adultId !== parsed.adultId ||
      parsed.intent.messageClass !== "status"
    ) {
      throw new ApplicationStoreError("not_authorized", "Gmail completion status is not owner-private");
    }

    const discovery = parsed.state.discovery;
    const stateJson = JSON.parse(JSON.stringify(parsed.state));
    const updated = await this.database.begin(async (transaction) => {
      const rows = await transaction<{ id: string }[]>`
        update external_connections
        set cursor = jsonb_set(cursor, '{gmail}', ${this.database.json(stateJson)}, true),
          last_synced_at = now(), updated_at = now()
        where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and adult_id = ${parsed.adultId} and provider = 'google' and status = 'active'
          and coalesce((cursor->'gmail'->>'revision')::integer, 0) = ${parsed.expectedRevision}
          and cursor->'gmail'->'discovery'->>'runId' = ${discovery.runId}
          and cursor->'gmail'->'discovery'->>'status' = 'pending'
        returning id
      `;
      if (!rows[0]) return false;
      await this.applicationStore.insertApplicationIntent(transaction, parsed.intent);
      return true;
    });
    if (updated) return "updated";
    return this.googleMutationFailure(parsed);
  }

  public async getCalendarSyncRecord(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    calendarId: string;
  }): Promise<CalendarSyncRecord | null> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        connectionId: z.uuid(),
        calendarId: z.string().min(1).max(1_000),
      })
      .parse(input);
    const rows = await this.database<
      {
        calendar_id: string;
        provider_calendar_id: string;
        status: "active" | "excluded" | "deleted";
        selection_source: "provider" | "adult";
        availability_only: boolean;
        access_role: string | null;
        sync_state: unknown;
      }[]
    >`
      select state.calendar_id, state.provider_calendar_id, state.status,
        state.selection_source, state.availability_only, state.access_role, state.sync_state
      from google_calendar_sync_states state
      join external_connections connection on connection.id = state.connection_id
      where state.connection_id = ${parsed.connectionId}
        and state.household_id = ${parsed.householdId} and state.adult_id = ${parsed.adultId}
        and state.calendar_id = ${parsed.calendarId}
        and connection.household_id = state.household_id and connection.adult_id = state.adult_id
        and connection.provider = 'google' and connection.status = 'active'
      limit 1
    `;
    const row = rows[0];
    if (!row) return null;
    const syncState = row.sync_state === null ? null : calendarSyncStateSchema.safeParse(row.sync_state);
    if (syncState !== null && !syncState.success) {
      throw new ApplicationStoreError("invalid_state", "Calendar sync state is invalid");
    }
    const accessRole =
      row.access_role === null ? null : googleCalendarAccessRoleSchema.parse(row.access_role);
    return {
      calendarId: row.calendar_id,
      providerCalendarId: row.provider_calendar_id,
      status: row.status,
      selectionSource: row.selection_source,
      availabilityOnly: row.availability_only,
      accessRole,
      state: syncState === null ? null : syncState.data,
    };
  }

  public async listOwnedGoogleCalendars(input: { householdId: string; adultId: string }): Promise<
    readonly {
      connectionId: string;
      connectionLabel: string;
      calendarId: string;
      displayName: string;
      status: "active" | "excluded" | "deleted";
      primary: boolean;
    }[]
  > {
    const parsed = z.strictObject({ householdId: z.uuid(), adultId: z.uuid() }).parse(input);
    const rows = await this.database<
      {
        connection_id: string;
        connection_label: string;
        calendar_id: string;
        provider_calendar_id: string;
        display_name_ciphertext: string;
        status: "active" | "excluded" | "deleted";
        is_primary: boolean;
      }[]
    >`
      select state.connection_id, connection.label as connection_label, state.calendar_id,
        state.provider_calendar_id, state.display_name_ciphertext, state.status, state.is_primary
      from google_calendar_sync_states state
      join external_connections connection on connection.id = state.connection_id
      where state.household_id = ${parsed.householdId} and state.adult_id = ${parsed.adultId}
        and connection.household_id = state.household_id and connection.adult_id = state.adult_id
        and connection.provider = 'google' and connection.status = 'active'
      order by connection.created_at, state.is_primary desc, state.calendar_id
    `;
    const secretBox = new SecretBox(this.identityKey);
    return rows.map((row) => {
      let displayName: string;
      try {
        displayName = secretBox.open(
          row.display_name_ciphertext,
          calendarCatalogNameAad(
            { householdId: parsed.householdId, adultId: parsed.adultId, id: row.connection_id },
            row.provider_calendar_id,
          ),
        );
      } catch {
        throw new ApplicationStoreError("invalid_state", "Calendar display name could not be decrypted");
      }
      return {
        connectionId: row.connection_id,
        connectionLabel: row.connection_label,
        calendarId: row.calendar_id,
        displayName,
        status: row.status,
        primary: row.is_primary,
      };
    });
  }

  public async setOwnedGoogleCalendarEnabled(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    calendarId: string;
    enabled: boolean;
  }): Promise<"updated" | "not_found" | "primary_required" | "unavailable"> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        connectionId: z.uuid(),
        calendarId: z.string().min(1).max(1_000),
        enabled: z.boolean(),
      })
      .parse(input);
    return this.database.begin(async (transaction) => {
      const currentRows = await transaction<
        { status: "active" | "excluded" | "deleted"; is_primary: boolean; access_role: string | null }[]
      >`
        select state.status, state.is_primary, state.access_role
        from google_calendar_sync_states state
        join external_connections connection on connection.id = state.connection_id
        where state.connection_id = ${parsed.connectionId}
          and state.household_id = ${parsed.householdId} and state.adult_id = ${parsed.adultId}
          and state.calendar_id = ${parsed.calendarId}
          and connection.household_id = state.household_id and connection.adult_id = state.adult_id
          and connection.provider = 'google' and connection.status = 'active'
        for update of state
      `;
      const current = currentRows[0];
      if (!current) return "not_found" as const;
      if (!parsed.enabled && current.is_primary) return "primary_required" as const;
      if (parsed.enabled && (current.status === "deleted" || current.access_role === null)) {
        return "unavailable" as const;
      }
      await transaction`
        update google_calendar_sync_states
        set status = ${parsed.enabled ? "active" : "excluded"}, selection_source = 'adult',
          adult_enabled = ${parsed.enabled},
          sync_state = case
            when ${parsed.enabled} and status = 'active' then sync_state
            else null
          end,
          updated_at = now()
        where connection_id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and adult_id = ${parsed.adultId} and calendar_id = ${parsed.calendarId}
      `;
      if (!parsed.enabled) {
        await this.removeCalendarProjection(transaction, parsed, parsed.calendarId);
      }
      return "updated" as const;
    });
  }

  public async saveCalendarCatalogState(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    expectedRevision: number;
    state: CalendarCatalogState;
  }): Promise<ScopedMutationResult> {
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        connectionId: z.uuid(),
        expectedRevision: z.number().int().nonnegative(),
        state: calendarCatalogStateSchema,
      })
      .parse(input);
    const rows = await this.database<{ id: string }[]>`
      update external_connections
      set cursor = jsonb_set(
            cursor, '{calendarCatalog}',
            ${this.database.json(JSON.parse(JSON.stringify(parsed.state)))}, true
          ), updated_at = now()
      where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
        and adult_id = ${parsed.adultId} and provider = 'google' and status = 'active'
        and coalesce((cursor->'calendarCatalog'->>'revision')::integer, 0) = ${parsed.expectedRevision}
      returning id
    `;
    if (rows[0]) return "updated";
    return this.googleMutationFailure(parsed);
  }

  public async applyCalendarCatalogPage(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    expectedRevision: number;
    state: CalendarCatalogState;
    fullScan: boolean;
    entries: readonly {
      calendarId: string;
      providerCalendarId: string;
      encryptedDisplayName: string;
      accessRole: z.infer<typeof googleCalendarAccessRoleSchema> | null;
      primary: boolean;
      selected: boolean;
      hidden: boolean;
      deleted: boolean;
      defaultEnabled: boolean;
      availabilityOnly: boolean;
      initialState: CalendarSyncState;
    }[];
  }): Promise<ScopedMutationResult> {
    const entrySchema = z.strictObject({
      calendarId: z.string().min(1).max(1_000),
      providerCalendarId: z.string().min(1).max(1_000),
      encryptedDisplayName: z.string().min(1),
      accessRole: googleCalendarAccessRoleSchema.nullable(),
      primary: z.boolean(),
      selected: z.boolean(),
      hidden: z.boolean(),
      deleted: z.boolean(),
      defaultEnabled: z.boolean(),
      availabilityOnly: z.boolean(),
      initialState: calendarSyncStateSchema,
    });
    const parsed = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        connectionId: z.uuid(),
        expectedRevision: z.number().int().nonnegative(),
        state: calendarCatalogStateSchema,
        fullScan: z.boolean(),
        entries: z.array(entrySchema).max(250),
      })
      .parse(input);
    const updated = await this.database.begin(async (transaction) => {
      const connectionRows = await transaction<{ id: string }[]>`
        update external_connections
        set cursor = jsonb_set(
              cursor, '{calendarCatalog}',
              ${this.database.json(JSON.parse(JSON.stringify(parsed.state)))}, true
            ), updated_at = now()
        where id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and adult_id = ${parsed.adultId} and provider = 'google' and status = 'active'
          and coalesce((cursor->'calendarCatalog'->>'revision')::integer, 0) = ${parsed.expectedRevision}
        returning id
      `;
      if (!connectionRows[0]) return false;

      for (const entry of parsed.entries) {
        const existingRows = await transaction<
          {
            status: "active" | "excluded" | "deleted";
            selection_source: "provider" | "adult";
            adult_enabled: boolean | null;
            availability_only: boolean;
            sync_state: unknown;
          }[]
        >`
          select status, selection_source, adult_enabled, availability_only, sync_state
          from google_calendar_sync_states
          where connection_id = ${parsed.connectionId} and calendar_id = ${entry.calendarId}
          for update
        `;
        const existing = existingRows[0];
        const adultEnabled = existing?.selection_source === "adult" ? existing.adult_enabled : null;
        const enabled = entry.accessRole !== null && (adultEnabled ?? entry.defaultEnabled);
        const status: "active" | "excluded" | "deleted" = entry.deleted
          ? "deleted"
          : enabled
            ? "active"
            : "excluded";
        const retainedState = calendarSyncStateSchema.safeParse(existing?.sync_state);
        const modeChanged = existing !== undefined && existing.availability_only !== entry.availabilityOnly;
        const syncState =
          status === "active"
            ? existing?.status === "active" && !modeChanged && retainedState.success
              ? retainedState.data
              : entry.initialState
            : null;
        const selectionSource = existing?.selection_source === "adult" ? "adult" : "provider";
        await transaction`
          insert into google_calendar_sync_states (
            connection_id, household_id, adult_id, calendar_id, provider_calendar_id,
            display_name_ciphertext, access_role, is_primary, selected, hidden, status,
            selection_source, adult_enabled, availability_only, last_seen_scan_id,
            catalog_revision, sync_state
          ) values (
            ${parsed.connectionId}, ${parsed.householdId}, ${parsed.adultId}, ${entry.calendarId},
            ${entry.providerCalendarId}, ${entry.encryptedDisplayName}, ${entry.accessRole},
            ${entry.primary}, ${entry.selected}, ${entry.hidden}, ${status}, ${selectionSource},
            ${selectionSource === "adult" ? adultEnabled : null}, ${entry.availabilityOnly},
            ${parsed.state.scanId}, ${parsed.state.revision},
            ${syncState === null ? null : this.database.json(JSON.parse(JSON.stringify(syncState)))}
          )
          on conflict (connection_id, calendar_id) do update set
            provider_calendar_id = excluded.provider_calendar_id,
            display_name_ciphertext = excluded.display_name_ciphertext,
            access_role = excluded.access_role, is_primary = excluded.is_primary,
            selected = excluded.selected, hidden = excluded.hidden, status = excluded.status,
            selection_source = excluded.selection_source, adult_enabled = excluded.adult_enabled,
            availability_only = excluded.availability_only,
            last_seen_scan_id = excluded.last_seen_scan_id,
            catalog_revision = excluded.catalog_revision, sync_state = excluded.sync_state,
            updated_at = now()
        `;
        if (status !== "active" || modeChanged) {
          await this.removeCalendarProjection(transaction, parsed, entry.calendarId);
        }
      }

      if (parsed.fullScan && parsed.state.phase === "live" && parsed.state.projectionReady) {
        const absent = await transaction<{ calendar_id: string }[]>`
          select calendar_id from google_calendar_sync_states
          where connection_id = ${parsed.connectionId} and household_id = ${parsed.householdId}
            and adult_id = ${parsed.adultId} and last_seen_scan_id <> ${parsed.state.scanId}
          for update
        `;
        if (absent.length > 0) {
          await transaction`
            update google_calendar_sync_states
            set status = 'deleted', sync_state = null, updated_at = now()
            where connection_id = ${parsed.connectionId} and household_id = ${parsed.householdId}
              and adult_id = ${parsed.adultId} and last_seen_scan_id <> ${parsed.state.scanId}
          `;
          for (const row of absent) {
            await this.removeCalendarProjection(transaction, parsed, row.calendar_id);
          }
        }
      }
      return true;
    });
    if (updated) return "updated";
    return this.googleMutationFailure(parsed);
  }

  private async removeCalendarProjection(
    transaction: TransactionSql<Record<string, never>>,
    scope: { householdId: string; adultId: string; connectionId: string },
    calendarId: string,
  ): Promise<void> {
    await transaction`
      update google_calendar_channels set status = 'stopped', updated_at = now()
      where connection_id = ${scope.connectionId} and household_id = ${scope.householdId}
        and adult_id = ${scope.adultId} and calendar_id = ${calendarId}
        and status in ('active', 'retiring')
    `;
    await transaction`
      delete from calendar_busy_windows
      where connection_id = ${scope.connectionId} and household_id = ${scope.householdId}
        and owner_adult_id = ${scope.adultId} and calendar_id = ${calendarId}
    `;
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
    const rows = await this.database<{ connection_id: string }[]>`
      update google_calendar_sync_states state
      set sync_state = ${this.database.json(JSON.parse(JSON.stringify(parsed.state)))},
        updated_at = now()
      from external_connections connection
      where state.connection_id = ${parsed.connectionId}
        and state.household_id = ${parsed.householdId} and state.adult_id = ${parsed.adultId}
        and state.calendar_id = ${parsed.state.calendarId} and state.status = 'active'
        and coalesce((state.sync_state->>'revision')::integer, 0) = ${parsed.expectedRevision}
        and connection.id = state.connection_id and connection.household_id = state.household_id
        and connection.adult_id = state.adult_id and connection.provider = 'google'
        and connection.status = 'active'
      returning state.connection_id
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
      const rows = await transaction<{ connection_id: string }[]>`
        update google_calendar_sync_states state
        set sync_state = ${this.database.json(JSON.parse(JSON.stringify(parsed.state)))},
          updated_at = now()
        from external_connections connection
        where state.connection_id = ${parsed.connectionId}
          and state.household_id = ${parsed.householdId} and state.adult_id = ${parsed.adultId}
          and state.calendar_id = ${parsed.state.calendarId} and state.status = 'active'
          and coalesce((state.sync_state->>'revision')::integer, 0) = ${parsed.expectedRevision}
          and connection.id = state.connection_id and connection.household_id = state.household_id
          and connection.adult_id = state.adult_id and connection.provider = 'google'
          and connection.status = 'active'
        returning state.connection_id
      `;
      if (!rows[0]) return false;
      await transaction`
        delete from calendar_busy_windows
        where connection_id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and owner_adult_id = ${parsed.adultId} and calendar_id = ${parsed.state.calendarId}
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
      const rows = await transaction<{ connection_id: string }[]>`
        update google_calendar_sync_states state
        set sync_state = ${this.database.json(JSON.parse(JSON.stringify(parsed.state)))},
          updated_at = now()
        from external_connections connection
        where state.connection_id = ${parsed.connectionId}
          and state.household_id = ${parsed.householdId} and state.adult_id = ${parsed.adultId}
          and state.calendar_id = ${parsed.calendarId} and state.status = 'active'
          and coalesce((state.sync_state->>'revision')::integer, 0) = ${parsed.expectedRevision}
          and connection.id = state.connection_id and connection.household_id = state.household_id
          and connection.adult_id = state.adult_id and connection.provider = 'google'
          and connection.status = 'active'
        returning state.connection_id
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

  public async listCalendarWatches(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
  }): Promise<readonly { channelId: string; resourceId: string }[]> {
    const parsed = z
      .strictObject({ householdId: z.uuid(), adultId: z.uuid(), connectionId: z.uuid() })
      .parse(input);
    const rows = await this.database<{ channel_id: string; resource_id: string }[]>`
      select channel.channel_id, channel.resource_id
      from google_calendar_channels channel
      join external_connections connection on connection.id = channel.connection_id
      where channel.connection_id = ${parsed.connectionId}
        and channel.household_id = ${parsed.householdId} and channel.adult_id = ${parsed.adultId}
        and channel.status in ('active', 'retiring')
        and connection.household_id = channel.household_id
        and connection.adult_id = channel.adult_id and connection.provider = 'google'
      order by channel.created_at, channel.channel_id
    `;
    return rows.map((row) => ({ channelId: row.channel_id, resourceId: row.resource_id }));
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
        join google_calendar_sync_states state
          on state.connection_id = channel.connection_id and state.calendar_id = channel.calendar_id
        where channel.channel_id = ${parsed.channelId}
          and channel.resource_id = ${parsed.resourceId}
          and channel.resource_uri = ${parsed.resourceUri}
          and channel.token_digest = ${tokenDigest}
          and channel.status in ('active', 'retiring') and channel.expires_at > ${parsed.receivedAt}
          and connection.status = 'active' and connection.provider = 'google'
          and connection.household_id = channel.household_id
          and connection.adult_id = channel.adult_id
          and state.household_id = channel.household_id and state.adult_id = channel.adult_id
          and state.status = 'active'
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
  ): Promise<PersistPersonalCalendarSourceResult> {
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
    if (persisted.retainedExisting === "stale") {
      return {
        sourceItemId: persisted.sourceItemId,
        disposition: persisted.disposition,
        revision: persisted.revision,
        createdByApprovedActionId: persisted.createdByApprovedActionId ?? null,
        retainedExisting: "stale",
      };
    }
    if (parsed.busyWindow === null) {
      await this.database`
        delete from calendar_busy_windows
        where connection_id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and owner_adult_id = ${parsed.adultId} and calendar_id = ${parsed.calendarId}
          and external_event_id = ${parsed.externalId}
          and source_revision <= ${persisted.revision}
      `;
      return {
        sourceItemId: persisted.sourceItemId,
        disposition: persisted.disposition,
        revision: persisted.revision,
        createdByApprovedActionId: persisted.createdByApprovedActionId ?? null,
      };
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
      join source_items source on source.id = ${persisted.sourceItemId}
        and source.household_id = connection.household_id
        and source.connection_id = connection.id and source.revision = ${persisted.revision}
      join google_calendar_sync_states state on state.connection_id = connection.id
        and state.calendar_id = ${parsed.calendarId} and state.status = 'active'
      where connection.id = ${parsed.connectionId} and connection.household_id = ${parsed.householdId}
        and connection.adult_id = ${parsed.adultId} and connection.provider = 'google'
        and connection.status = 'active'
        and state.household_id = connection.household_id and state.adult_id = connection.adult_id
      on conflict (connection_id, calendar_id, external_event_id)
      do update set source_item_id = excluded.source_item_id,
        source_revision = excluded.source_revision, starts_at = excluded.starts_at,
        ends_at = excluded.ends_at, all_day = excluded.all_day, updated_at = now()
      where calendar_busy_windows.source_revision <= excluded.source_revision
      returning connection_id
    `;
    if (!rows[0]) {
      const current = await this.database<{ revision: string }[]>`
        select revision from source_items
        where id = ${persisted.sourceItemId} and household_id = ${parsed.householdId}
          and connection_id = ${parsed.connectionId} and owner_adult_id = ${parsed.adultId}
      `;
      if (current[0] && Number(current[0].revision) > persisted.revision) {
        return {
          sourceItemId: persisted.sourceItemId,
          disposition: "unchanged",
          revision: Number(current[0].revision),
          createdByApprovedActionId: null,
          retainedExisting: "stale",
        };
      }
      throw new ApplicationStoreError("not_authorized", "Calendar projection owner is inactive");
    }
    return {
      sourceItemId: persisted.sourceItemId,
      disposition: persisted.disposition,
      revision: persisted.revision,
      createdByApprovedActionId: persisted.createdByApprovedActionId ?? null,
    };
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
    const connections = await this.database<{ id: string; cursor: Record<string, unknown> }[]>`
      select id, cursor from external_connections
      where household_id = ${parsed.householdId} and adult_id = ${parsed.adultId}
        and provider = 'google' and status = 'active'
        and granted_scopes @> ${[GOOGLE_CALENDAR_READONLY_SCOPE]}::text[]
      order by id
    `;
    if (connections.length === 0) {
      return { windows: [], complete: true, synchronizedAt: null };
    }
    const catalogs = connections.map((connection) =>
      calendarCatalogStateSchema.safeParse(connection.cursor.calendarCatalog),
    );
    const freshnessBoundary = Date.parse(parsed.asOf) - 30 * 60_000;
    if (
      catalogs.some(
        (catalog) =>
          !catalog.success ||
          catalog.data.phase !== "live" ||
          !catalog.data.projectionReady ||
          catalog.data.lastSuccessfulSyncAt === null ||
          Date.parse(catalog.data.lastSuccessfulSyncAt) < freshnessBoundary,
      )
    ) {
      return { windows: [], complete: false, synchronizedAt: null };
    }
    const connectionIds = connections.map((connection) => connection.id);
    const stateRows = await this.database<{ connection_id: string; sync_state: unknown }[]>`
      select connection_id, sync_state
      from google_calendar_sync_states
      where household_id = ${parsed.householdId} and adult_id = ${parsed.adultId}
        and connection_id = any(${connectionIds}::uuid[]) and status = 'active'
      order by connection_id, calendar_id
    `;
    const states = stateRows.map((row) => ({
      connectionId: row.connection_id,
      state: calendarSyncStateSchema.safeParse(row.sync_state),
    }));
    if (
      connections.some((connection) => !states.some((state) => state.connectionId === connection.id)) ||
      states.some(
        ({ state }) =>
          !state.success ||
          state.data.phase !== "live" ||
          !state.data.projectionReady ||
          state.data.lastSuccessfulSyncAt === null ||
          Date.parse(state.data.lastSuccessfulSyncAt) < freshnessBoundary ||
          Date.parse(state.data.initialTimeMin) > Date.parse(parsed.from) ||
          Date.parse(state.data.initialTimeMax) < Date.parse(parsed.to),
      )
    ) {
      return { windows: [], complete: false, synchronizedAt: null };
    }
    const readyStates = states.flatMap(({ state }) => (state.success ? [state.data] : []));
    const earliestSynchronization = [
      ...catalogs.flatMap((catalog) =>
        catalog.success && catalog.data.lastSuccessfulSyncAt !== null
          ? [catalog.data.lastSuccessfulSyncAt]
          : [],
      ),
      ...readyStates
        .map((state) => state.lastSuccessfulSyncAt)
        .filter((value): value is string => value !== null),
    ].sort()[0];
    const synchronizedAt = earliestSynchronization ? new Date(earliestSynchronization).toISOString() : null;
    if (synchronizedAt === null || Date.parse(synchronizedAt) < freshnessBoundary) {
      return { windows: [], complete: false, synchronizedAt };
    }
    const rows = await this.database<{ starts_at: Date; ends_at: Date; all_day: boolean }[]>`
      select distinct busy.starts_at, busy.ends_at, busy.all_day
      from calendar_busy_windows busy
      join external_connections connection on connection.id = busy.connection_id
      join google_calendar_sync_states state
        on state.connection_id = busy.connection_id and state.calendar_id = busy.calendar_id
      where busy.household_id = ${parsed.householdId} and busy.owner_adult_id = ${parsed.adultId}
        and connection.household_id = busy.household_id
        and connection.adult_id = busy.owner_adult_id
        and connection.status = 'active' and connection.provider = 'google'
        and state.household_id = busy.household_id and state.adult_id = busy.owner_adult_id
        and state.status = 'active'
        and busy.starts_at < ${parsed.to} and busy.ends_at > ${parsed.from}
      order by busy.starts_at, busy.ends_at, busy.all_day
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
        and granted_scopes @> ${[GOOGLE_CALENDAR_READONLY_SCOPE]}::text[]
      order by adult_id, id
    `;
    if (
      connections.length === 0 ||
      expectedAdults.some((adultId) => !connections.some((connection) => connection.adult_id === adultId))
    ) {
      return { status: "unavailable", reason: "projection_incomplete" };
    }
    const freshnessBoundary = Date.parse(parsed.asOf) - 30 * 60_000;
    const catalogByConnection = new Map<string, CalendarCatalogState>();
    for (const connection of connections) {
      const catalog = calendarCatalogStateSchema.safeParse(connection.cursor.calendarCatalog);
      if (
        !catalog.success ||
        catalog.data.phase !== "live" ||
        !catalog.data.projectionReady ||
        catalog.data.lastSuccessfulSyncAt === null ||
        Date.parse(catalog.data.lastSuccessfulSyncAt) < freshnessBoundary
      ) {
        return { status: "unavailable", reason: "projection_incomplete" };
      }
      catalogByConnection.set(connection.id, catalog.data);
    }
    const connectionIds = connections.map((connection) => connection.id);
    const calendarRows = await this.database<
      { connection_id: string; adult_id: string; calendar_id: string; sync_state: unknown }[]
    >`
      select connection_id, adult_id, calendar_id, sync_state
      from google_calendar_sync_states
      where household_id = ${parsed.householdId} and adult_id = any(${expectedAdults}::uuid[])
        and connection_id = any(${connectionIds}::uuid[]) and status = 'active'
      order by adult_id, connection_id, calendar_id
    `;
    const coverage: Array<{
      connectionId: string;
      adultId: string;
      calendarId: string;
      catalogRevision: number;
      stateRevision: number;
      synchronizedAt: string;
    }> = [];
    for (const row of calendarRows) {
      const state = calendarSyncStateSchema.safeParse(row.sync_state);
      const catalog = catalogByConnection.get(row.connection_id);
      if (
        !state.success ||
        catalog === undefined ||
        state.data.phase !== "live" ||
        !state.data.projectionReady ||
        state.data.lastSuccessfulSyncAt === null ||
        Date.parse(state.data.lastSuccessfulSyncAt) < freshnessBoundary ||
        Date.parse(state.data.initialTimeMin) > Date.parse(parsed.startsAt) ||
        Date.parse(state.data.initialTimeMax) < Date.parse(parsed.endsAt)
      ) {
        return { status: "unavailable", reason: "projection_incomplete" };
      }
      coverage.push({
        connectionId: row.connection_id,
        adultId: row.adult_id,
        calendarId: state.data.calendarId,
        catalogRevision: catalog.revision,
        stateRevision: state.data.revision,
        synchronizedAt: state.data.lastSuccessfulSyncAt,
      });
    }
    if (
      !connections.every((connection) =>
        coverage.some((item) => item.connectionId === connection.id && item.adultId === connection.adult_id),
      ) ||
      !expectedAdults.every((adultId) => coverage.some((item) => item.adultId === adultId)) ||
      !coverage.some((item) => item.connectionId === target.id)
    ) {
      return { status: "unavailable", reason: "projection_incomplete" };
    }
    const coveredConnectionIds = [...new Set(coverage.map((item) => item.connectionId))];

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
      join google_calendar_sync_states state
        on state.connection_id = busy.connection_id and state.calendar_id = busy.calendar_id
      where busy.household_id = ${parsed.householdId}
        and busy.owner_adult_id = any(${expectedAdults}::uuid[])
        and busy.connection_id = any(${coveredConnectionIds}::uuid[])
        and connection.household_id = busy.household_id
        and connection.adult_id = busy.owner_adult_id
        and connection.provider = 'google' and connection.status = 'active'
        and state.household_id = busy.household_id and state.adult_id = busy.owner_adult_id
        and state.status = 'active'
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
    const metadata = gmailSourceMetadataSchema.parse(input.metadata);
    if (
      (input.kind === "gmail_message_deleted" &&
        (metadata.contentCompleteness !== "metadata" || metadata.deleted !== true)) ||
      (input.kind === "gmail_message" && metadata.deleted === true)
    ) {
      throw new ApplicationStoreError("invalid_state", "Gmail source completeness is inconsistent");
    }
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
      metadata,
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
      await transaction`
        delete from google_calendar_sync_states
        where connection_id = ${parsed.connectionId} and household_id = ${parsed.householdId}
          and adult_id = ${parsed.adultId}
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
      insert into jobs (
        id, household_id, kind, status, payload, max_attempts, idempotency_key, control_epoch
      )
      select ${id}, household.id, 'google.calendar.sync', 'pending',
        ${this.database.json(JSON.parse(JSON.stringify(parsed.work)))}, 8,
        ${parsed.idempotencyKey}, household.control_epoch
      from households household
      where household.id = ${parsed.householdId} and household.status <> 'deleting'
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
      select connection.id, connection.household_id, connection.adult_id,
        connection.cursor, connection.updated_at
      from external_connections connection
      join households household on household.id = connection.household_id
      where connection.provider = 'google' and connection.status = 'active'
        and household.status <> 'deleting'
        and connection.granted_scopes @> ${[GOOGLE_CALENDAR_READONLY_SCOPE]}::text[]
      order by connection.updated_at
    `;
    const connectionIds = rows.map((row) => row.id);
    const calendarRows =
      connectionIds.length === 0
        ? []
        : await this.database<
            { connection_id: string; calendar_id: string; availability_only: boolean; sync_state: unknown }[]
          >`
            select connection_id, calendar_id, availability_only, sync_state
            from google_calendar_sync_states
            where connection_id = any(${connectionIds}::uuid[]) and status = 'active'
            order by connection_id, calendar_id
          `;
    let created = 0;
    for (const row of rows) {
      const catalogResult = calendarCatalogStateSchema.safeParse(row.cursor.calendarCatalog);
      if (row.cursor.calendarCatalog !== undefined && !catalogResult.success) {
        await this.markConnectionStatus({
          householdId: row.household_id,
          adultId: row.adult_id,
          connectionId: row.id,
          status: "error",
        });
        continue;
      }
      const catalogPlan = calendarCatalogWorkForState(
        row,
        catalogResult.success ? catalogResult.data : null,
        now,
      );
      if (catalogPlan !== null) {
        const receipt = await this.enqueueCalendarSyncWork({
          householdId: row.household_id,
          idempotencyKey: calendarWorkIdempotencyKey(
            row.id,
            catalogPlan.work,
            catalogPlan.revision,
            now,
            row.updated_at.toISOString(),
          ),
          work: catalogPlan.work,
        });
        if (receipt.created) created += 1;
        continue;
      }

      const ownedCalendars = calendarRows.filter((calendar) => calendar.connection_id === row.id);
      for (const calendar of ownedCalendars) {
        const stateResult = calendarSyncStateSchema.safeParse(calendar.sync_state);
        if (calendar.sync_state !== null && !stateResult.success) {
          await this.markConnectionStatus({
            householdId: row.household_id,
            adultId: row.adult_id,
            connectionId: row.id,
            status: "error",
          });
          break;
        }
        const planned = calendarWorkForState(
          row,
          calendar.calendar_id,
          calendar.availability_only,
          stateResult.success ? stateResult.data : null,
          now,
        );
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
          select job.id from jobs job
          join households household on household.id = job.household_id
          where job.kind = 'google.calendar.sync' and (
            (job.status in ('pending', 'retry') and job.available_at <= now())
            or (job.status = 'leased' and job.lease_expires_at < now())
          )
            and household.status <> 'deleting'
            and job.control_epoch = household.control_epoch
          order by job.available_at, job.created_at
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
        id, household_id, kind, status, payload, max_attempts, idempotency_key, control_epoch
      )
      select ${id}, household.id, 'google.sync', 'pending',
        ${this.database.json(JSON.parse(JSON.stringify(parsed.work)))}, 8,
        ${parsed.idempotencyKey}, household.control_epoch
      from households household
      where household.id = ${parsed.householdId} and household.status <> 'deleting'
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
      {
        id: string;
        household_id: string;
        adult_id: string;
        cursor: Record<string, unknown>;
      }[]
    >`
      select connection.id, connection.household_id, connection.adult_id, connection.cursor
      from external_connections connection
      join households household on household.id = connection.household_id
      where connection.provider = 'google' and connection.status = 'active'
        and connection.granted_scopes && ${[GOOGLE_GMAIL_READONLY_SCOPE]}::text[]
        and household.status <> 'deleting'
      order by connection.updated_at
    `;
    let created = 0;
    for (const row of rows) {
      const storedState = row.cursor.gmail;
      const state = gmailSyncStateSchema.safeParse(storedState);
      if (storedState !== undefined && !state.success) {
        await this.markConnectionStatus({
          householdId: row.household_id,
          adultId: row.adult_id,
          connectionId: row.id,
          status: "error",
        });
        continue;
      }
      const work = state.success
        ? googleWorkForState(row, state.data, now)
        : gmailSyncWorkSchema.parse({
            kind: "start",
            householdId: row.household_id,
            adultId: row.adult_id,
            connectionId: row.id,
            depth: "full_history",
          });
      if (!work) continue;
      const revision = state.success ? state.data.revision : 0;
      const receipt = await this.enqueueGoogleSyncWork({
        householdId: row.household_id,
        idempotencyKey:
          work.kind === "start"
            ? `google:${row.id}:start`
            : `google:${row.id}:${work.kind}:revision:${revision}`,
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
          select job.id from jobs job
          join households household on household.id = job.household_id
          where job.kind = 'google.sync' and (
            (job.status in ('pending', 'retry') and job.available_at <= now())
            or (job.status = 'leased' and job.lease_expires_at < now())
          )
            and household.status <> 'deleting'
            and job.control_epoch = household.control_epoch
          order by job.available_at, job.created_at
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

  private deletionIdentityDigest(kind: "linq-chat" | "linq-handle", value: string): string {
    return createHmac("sha256", this.identityKey)
      .update(`florence:customer-deletion:${kind}:v1\0`)
      .update(value.normalize("NFKC"))
      .digest("hex");
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

async function lockLinqChat(
  transaction: TransactionSql<Record<string, never>>,
  externalChatId: string,
): Promise<void> {
  await transaction`
    select pg_advisory_xact_lock(
      hashtextextended(${`linq-chat:${externalChatId}`}, 0::bigint)
    )
  `;
}

async function isChatSuppressed(
  transaction: TransactionSql<Record<string, never>>,
  externalChatId: string,
  externalHandleHash: string | null,
): Promise<boolean> {
  const rows = await transaction<{ status: "suppressed" | "released" }[]>`
    select status from channel_suppressions
    where provider = 'linq' and external_chat_id = ${externalChatId}
      and external_handle_hash is not distinct from ${externalHandleHash}
    limit 1
  `;
  return rows[0]?.status === "suppressed";
}

async function activeHouseholdHandlesMatch(
  transaction: TransactionSql<Record<string, never>>,
  householdId: string,
  expectedHandles: readonly string[],
): Promise<boolean> {
  const membershipCounts = await transaction<{ count: string }[]>`
    select count(*)::text as count from household_memberships
    where household_id = ${householdId} and status = 'active'
  `;
  if (Number(membershipCounts[0]?.count ?? 0) !== 2) return false;
  const rows = await transaction<{ adult_id: string; external_handle: string }[]>`
    select cb.adult_id, cb.external_handle
    from channel_bindings cb
    join household_memberships hm
      on hm.household_id = cb.household_id and hm.adult_id = cb.adult_id
    where cb.household_id = ${householdId} and cb.provider = 'linq'
      and cb.channel_type = 'private' and cb.status in ('active', 'paused') and hm.status = 'active'
    order by cb.adult_id, cb.external_handle
  `;
  if (
    rows.length !== 2 ||
    new Set(rows.map((row) => row.adult_id)).size !== 2 ||
    rows.some((row) => !row.external_handle)
  ) {
    return false;
  }
  try {
    return sameStrings(
      uniqueCanonicalHandles(rows.map((row) => row.external_handle)),
      uniqueCanonicalHandles(expectedHandles),
    );
  } catch {
    return false;
  }
}

function normalizeHealthyLinqGroup(
  chat: LinqChat,
  expectedChatId: string,
): { participants: string[]; selfHandles: string[] } | null {
  if (
    chat.id !== expectedChatId ||
    !chat.isGroup ||
    chat.service?.toLowerCase() !== "imessage" ||
    chat.healthStatus !== "HEALTHY"
  ) {
    return null;
  }
  try {
    const participants = uniqueCanonicalHandles(chat.participantHandles);
    const selfHandles = uniqueCanonicalHandles(chat.selfHandles);
    const active = uniqueCanonicalHandles(chat.activeHandles);
    if (participants.length !== 2 || selfHandles.length !== 1 || active.length !== 3) return null;
    return sameStrings(active, [...participants, ...selfHandles].sort())
      ? { participants, selfHandles }
      : null;
  } catch {
    return null;
  }
}

function groupMetadataMatches(
  storedMetadata: unknown,
  expectedMetadata: z.infer<typeof groupBindingMetadataSchema>,
): boolean {
  const stored = groupBindingMetadataSchema.safeParse(storedMetadata);
  return (
    stored.success &&
    stored.data.service === expectedMetadata.service &&
    stored.data.healthStatus === expectedMetadata.healthStatus &&
    sameStrings(stored.data.participantHandleDigests, expectedMetadata.participantHandleDigests) &&
    sameStrings(stored.data.selfHandleDigests, expectedMetadata.selfHandleDigests)
  );
}

function uniqueCanonicalHandles(handles: readonly string[]): string[] {
  return [...new Set(handles.map(canonicalizeLinqHandle))].sort();
}

function sameStrings(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function calendarWorkForState(
  row: { id: string; household_id: string; adult_id: string },
  calendarId: string,
  availabilityOnly: boolean,
  state: CalendarSyncState | null,
  asOf: string,
): { work: CalendarSyncWork; revision: number } | null {
  const identity = {
    householdId: row.household_id,
    adultId: row.adult_id,
    connectionId: row.id,
    calendarId,
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
  if (availabilityOnly) {
    const staleThreshold = Date.parse(asOf) - 15 * 60_000;
    if (state.lastSuccessfulSyncAt === null || Date.parse(state.lastSuccessfulSyncAt) <= staleThreshold) {
      return {
        work: calendarSyncWorkSchema.parse({ kind: "scheduled", ...identity }),
        revision: state.revision,
      };
    }
    return null;
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

function calendarCatalogWorkForState(
  row: { id: string; household_id: string; adult_id: string },
  state: CalendarCatalogState | null,
  asOf: string,
): { work: CalendarSyncWork; revision: number } | null {
  const work = calendarSyncWorkSchema.parse({
    kind: "catalog",
    householdId: row.household_id,
    adultId: row.adult_id,
    connectionId: row.id,
  });
  if (state === null) return { work, revision: 0 };
  if (state.phase === "initial" || state.pageToken !== null || !state.projectionReady) {
    return { work, revision: state.revision };
  }
  const fullRefreshThreshold = Date.parse(asOf) - 24 * 60 * 60_000;
  if (state.lastFullScanAt === null || Date.parse(state.lastFullScanAt) <= fullRefreshThreshold) {
    return {
      work: calendarSyncWorkSchema.parse({
        kind: "catalog_refresh",
        householdId: row.household_id,
        adultId: row.adult_id,
        connectionId: row.id,
      }),
      revision: state.revision,
    };
  }
  const staleThreshold = Date.parse(asOf) - 15 * 60_000;
  if (state.lastSuccessfulSyncAt === null || Date.parse(state.lastSuccessfulSyncAt) <= staleThreshold) {
    return { work, revision: state.revision };
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
  if (work.kind === "catalog") {
    if (revision === 0) return `google-calendar:${connectionId}:catalog:start:${connectionEpoch}`;
    const bucket = Math.floor(Date.parse(asOf) / (15 * 60_000));
    return `google-calendar:${connectionId}:catalog:revision:${revision}:bucket:${bucket}`;
  }
  if (work.kind === "catalog_refresh") {
    const bucket = Math.floor(Date.parse(asOf) / (24 * 60 * 60_000));
    return `google-calendar:${connectionId}:catalog-refresh:revision:${revision}:bucket:${bucket}`;
  }
  if (work.kind === "revoke") {
    return `google-calendar:${connectionId}:revoke:${connectionEpoch}`;
  }
  const calendarDigest = createHmac("sha256", connectionId)
    .update(work.calendarId)
    .digest("hex")
    .slice(0, 24);
  if (work.kind === "start") {
    return `google-calendar:${connectionId}:${calendarDigest}:start:${connectionEpoch}`;
  }
  if (work.kind === "scheduled") {
    const bucket = Math.floor(Date.parse(asOf) / (15 * 60_000));
    return `google-calendar:${connectionId}:${calendarDigest}:scheduled:${bucket}`;
  }
  return `google-calendar:${connectionId}:${calendarDigest}:${work.kind}:revision:${revision}`;
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
    state.discovery?.status === "pending" ||
    (state.phase === "live" &&
      (state.history.pageToken !== null ||
        (state.history.targetId !== null &&
          (state.history.cursorId === null ||
            BigInt(state.history.targetId) > BigInt(state.history.cursorId)))))
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
