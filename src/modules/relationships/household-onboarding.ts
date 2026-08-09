import { createHash, randomBytes, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { Database } from "../../db/client.js";
import type { SecretBox } from "../../shared/crypto.js";
import { ConflictError, NotFoundError, UnauthorizedError } from "../../shared/errors.js";
import {
  type HouseholdInvitation,
  HouseholdInvitationSchema,
  type HouseholdMembership,
  MembershipCapabilitySchema,
  PostgresIdentityRelationships,
  StewardCapabilities,
} from "../identity/index.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

const RoleSchema = z.enum(["steward", "caregiver", "participant"]);
const DependentContextSchema = z.strictObject({
  aliases: z.array(z.string().trim().min(1).max(80)).max(12).default([]),
  birthYear: z.number().int().min(1900).max(2100).nullable().default(null),
  school: z.string().trim().max(160).default(""),
  activities: z.array(z.string().trim().min(1).max(120)).max(24).default([]),
});

export interface DependentContext {
  readonly aliases: readonly string[];
  readonly birthYear: number | null;
  readonly school: string;
  readonly activities: readonly string[];
}

export interface HouseholdInvitationInput {
  readonly actorPersonId: string;
  readonly householdId: string;
  readonly conversationId: string;
  readonly expectedParticipantEpochId: string;
  readonly expectedParticipantDigest: string;
  readonly inviteeIdentityId: string;
  readonly inviteePersonId: string;
  readonly proposedDisplayName: string;
  readonly role: z.infer<typeof RoleSchema>;
  readonly sourceRevisionId?: string | null;
  readonly createdAt: Date;
}

export interface HouseholdInvitationResult {
  readonly invitation: HouseholdInvitation;
  readonly duplicate: boolean;
}

/**
 * Keeps relationship onboarding behind one authority seam. Invitations target
 * a person already present in the exact live Florence group, so the UI never
 * accepts an arbitrary phone number. Unregistered people receive only a private
 * enrollment request; family context stays unavailable until they opt in.
 */
export class HouseholdOnboarding {
  public constructor(
    private readonly database: Executor,
    private readonly secretBox: SecretBox,
  ) {}

  public async inviteCurrentParticipant(
    inputCandidate: HouseholdInvitationInput,
  ): Promise<HouseholdInvitationResult> {
    const input = z
      .strictObject({
        actorPersonId: z.string().uuid(),
        householdId: z.string().uuid(),
        conversationId: z.string().uuid(),
        expectedParticipantEpochId: z.string().uuid(),
        expectedParticipantDigest: z.string().regex(/^[a-f0-9]{64}$/u),
        inviteeIdentityId: z.string().uuid(),
        inviteePersonId: z.string().uuid(),
        proposedDisplayName: z.string().trim().min(1).max(80),
        role: RoleSchema,
        sourceRevisionId: z.string().uuid().nullable().optional().default(null),
        createdAt: z.date(),
      })
      .parse(inputCandidate);
    if (input.actorPersonId === input.inviteePersonId) {
      throw new ConflictError("You are already part of your family");
    }
    return inTransaction(this.database, async (transaction) => {
      const targets = await transaction<
        {
          identity_id: string;
          subject_digest: string;
          registration_status: string;
          consented_at: Date | null;
          participant_epoch_id: string;
          participant_set_digest: string;
        }[]
      >`
        select identity.id as identity_id, identity.subject_digest,
          participant.registration_status, participant.consented_at,
          epoch.id as participant_epoch_id, epoch.participant_set_digest
        from conversations conversation
        join participant_epochs epoch on epoch.id = conversation.current_epoch_id
          and epoch.id = ${input.expectedParticipantEpochId}
          and epoch.participant_set_digest = ${input.expectedParticipantDigest}
          and epoch.ended_at is null
        join epoch_participants participant on participant.participant_epoch_id = epoch.id
          and participant.person_id = ${input.inviteePersonId}
        join person_identities identity on identity.id = participant.person_identity_id
          and identity.id = ${input.inviteeIdentityId}
          and identity.person_id = ${input.inviteePersonId}
          and identity.status in ('observed', 'verified')
        where conversation.id = ${input.conversationId}
          and conversation.kind = 'group' and conversation.status = 'active'
          and exists(
            select 1 from epoch_participants actor
            where actor.participant_epoch_id = epoch.id and actor.person_id = ${input.actorPersonId}
          )
          and (
            ${input.sourceRevisionId}::uuid is null
            or exists(
              select 1
              from source_revisions source_revision
              join source_objects source_object on source_object.id = source_revision.source_object_id
                and source_object.status = 'active'
                and source_object.latest_revision_number = source_revision.revision_number
              where source_revision.id = ${input.sourceRevisionId}
                and source_revision.participant_epoch_id = epoch.id
                and source_revision.revoked_at is null
                and source_revision.retention_until > now()
            )
          )
        for update of conversation
      `;
      const target = targets.length === 1 ? targets[0] : null;
      if (!target) throw new UnauthorizedError("That person is no longer in the current group");
      await lockHousehold(transaction, input.householdId);
      const lockedIdentities = await transaction<{ readonly id: string }[]>`
        select id from person_identities
        where id = ${input.inviteeIdentityId}
          and person_id = ${input.inviteePersonId}
          and subject_digest = ${target.subject_digest}
          and status in ('observed', 'verified')
        for update
      `;
      if (!lockedIdentities[0]) {
        throw new UnauthorizedError("That person’s identity changed before the invitation was created");
      }
      await requireHouseholdCapability(
        transaction,
        input.householdId,
        input.actorPersonId,
        "membership.invite",
      );
      if (input.role === "steward") {
        await requireHouseholdCapability(
          transaction,
          input.householdId,
          input.actorPersonId,
          "household.govern",
        );
      }
      const existing = await transaction<{ status: string }[]>`
        select status from household_memberships
        where household_id = ${input.householdId} and person_id = ${input.inviteePersonId}
      `;
      if (existing[0]?.status === "active") throw new ConflictError("That person is already in this family");

      const capabilities = capabilitiesForRole(input.role);
      const households = await transaction<{ membership_version: number | string; status: string }[]>`
        select membership_version, status from households where id = ${input.householdId}
      `;
      const household = households[0];
      if (!household || !["onboarding", "active"].includes(household.status)) {
        throw new NotFoundError("Active household does not exist");
      }
      const pendingRows = await transaction<
        {
          invitation_id: string;
          invitee_identity_id: string | null;
          requested_role: string;
          requested_capabilities: string[];
          household_membership_version: number | string;
          proposed_display_name_ciphertext: Buffer | null;
          source_conversation_id: string | null;
          source_participant_epoch_id: string | null;
          source_participant_digest: string | null;
          source_revision_id: string | null;
          expires_at: Date;
        }[]
      >`
        select invitation.id as invitation_id, invitation.invitee_identity_id,
          invitation.requested_role, invitation.requested_capabilities,
          invitation.household_membership_version,
          invitation.proposed_display_name_ciphertext,
          invitation.source_conversation_id, invitation.source_participant_epoch_id,
          invitation.source_participant_digest, invitation.source_revision_id,
          invitation.expires_at
        from invitations invitation
        where invitation.household_id = ${input.householdId}
          and invitation.invitee_subject_digest = ${target.subject_digest}
          and invitation.status = 'pending'
        order by invitation.created_at
        limit 1
        for update
      `;
      const pending = pendingRows[0];
      const evaluatedAt = new Date();
      const duplicate =
        pending !== undefined &&
        pending.expires_at > evaluatedAt &&
        pending.invitee_identity_id === input.inviteeIdentityId &&
        pending.requested_role === input.role &&
        sameSet(pending.requested_capabilities, capabilities) &&
        Number(pending.household_membership_version) === Number(household.membership_version) &&
        pending.source_conversation_id === input.conversationId &&
        pending.source_participant_epoch_id === input.expectedParticipantEpochId &&
        pending.source_participant_digest === input.expectedParticipantDigest &&
        pending.source_revision_id === input.sourceRevisionId &&
        decryptProposedDisplayName(
          this.secretBox,
          pending.invitation_id,
          pending.proposed_display_name_ciphertext,
        ) === input.proposedDisplayName;
      if (pending && duplicate) {
        return {
          invitation: await loadInvitation(transaction, pending.invitation_id),
          duplicate: true,
        };
      }
      if (pending) {
        await transaction`
          update invitations
          set status = ${pending.expires_at <= evaluatedAt ? "expired" : "revoked"},
            updated_at = ${evaluatedAt}
          where id = ${pending.invitation_id} and status = 'pending'
        `;
      }
      const tokenDigest = sha256Hex(randomBytes(32));
      const invitation = await new PostgresIdentityRelationships(transaction).inviteMember({
        householdId: input.householdId,
        inviterPersonId: input.actorPersonId,
        inviteeIdentityId: target.identity_id,
        inviteeSubjectDigest: target.subject_digest,
        tokenDigest,
        requestedRole: input.role,
        requestedCapabilities: capabilities,
        expiresAt: new Date(input.createdAt.getTime() + 7 * 86_400_000).toISOString(),
        createdAt: input.createdAt.toISOString(),
      });
      const encryptedProposedDisplayName = this.secretBox.encrypt(
        input.proposedDisplayName,
        `invitation-proposed-display-name:${invitation.invitationId}`,
      );
      await transaction`
        update invitations
        set proposed_display_name_ciphertext = ${Buffer.from(
          JSON.stringify(encryptedProposedDisplayName),
          "utf8",
        )},
          proposed_display_name_key_version = ${encryptedProposedDisplayName.kid},
          source_conversation_id = ${input.conversationId},
          source_participant_epoch_id = ${input.expectedParticipantEpochId},
          source_participant_digest = ${input.expectedParticipantDigest},
          source_revision_id = ${input.sourceRevisionId},
          updated_at = ${input.createdAt}
        where id = ${invitation.invitationId}
      `;
      await appendHouseholdAudit(transaction, {
        householdId: input.householdId,
        actorPersonId: input.actorPersonId,
        eventType: "household_invitation_created",
        targetId: invitation.invitationId,
        reasons: [
          input.role,
          "exact_current_group_participant",
          target.registration_status === "registered" && target.consented_at
            ? "registered_invitee"
            : "private_enrollment_required",
        ],
        occurredAt: input.createdAt,
      });
      return { invitation, duplicate: false };
    });
  }

  public async approveInvitation(input: {
    readonly actorPersonId: string;
    readonly invitationId: string;
    readonly approvedAt: Date;
  }): Promise<HouseholdInvitation> {
    return inTransaction(this.database, async (transaction) => {
      const source = await lockCurrentInvitationSource(transaction, input.invitationId, null);
      await lockHousehold(transaction, source.householdId);
      await lockInvitationEvidence(transaction, input.invitationId, source);
      return new PostgresIdentityRelationships(transaction).approveInvitation({
        invitationId: input.invitationId,
        approverPersonId: input.actorPersonId,
        approvedAt: input.approvedAt.toISOString(),
      });
    });
  }

  public async acceptInvitation(input: {
    readonly actorPersonId: string;
    readonly invitationId: string;
    readonly expectedHouseholdMembershipVersion: number;
    readonly acceptedAt: Date;
  }): Promise<HouseholdMembership> {
    return inTransaction(this.database, async (transaction) => {
      const source = await lockCurrentInvitationSource(transaction, input.invitationId, input.actorPersonId);
      await lockHousehold(transaction, source.householdId);
      await lockInvitationEvidence(transaction, input.invitationId, source);
      const invitations = await transaction<{ token_digest: string; household_id: string }[]>`
        select invitation.token_digest, invitation.household_id
        from invitations invitation
        join person_identities identity on identity.id = invitation.invitee_identity_id
          and identity.person_id = ${input.actorPersonId} and identity.status = 'verified'
        where invitation.id = ${input.invitationId}
          and invitation.household_membership_version = ${input.expectedHouseholdMembershipVersion}
        for update of invitation
      `;
      const invitation = invitations[0];
      if (!invitation) throw new NotFoundError("This family invitation is not available to you");
      const membership = await new PostgresIdentityRelationships(transaction).acceptInvitation({
        invitationId: input.invitationId,
        inviteePersonId: input.actorPersonId,
        tokenDigest: invitation.token_digest,
        acceptedAt: input.acceptedAt.toISOString(),
      });
      await appendHouseholdAudit(transaction, {
        householdId: invitation.household_id,
        actorPersonId: input.actorPersonId,
        eventType: "household_invitation_accepted",
        targetId: membership.membershipId,
        reasons: [membership.role, "exact_verified_invitee"],
        occurredAt: input.acceptedAt,
      });
      return membership;
    });
  }

  public async addDependent(inputCandidate: {
    readonly actorPersonId: string;
    readonly householdId: string;
    readonly displayName: string;
    readonly aliases?: readonly string[];
    readonly birthYear?: number | null;
    readonly school?: string;
    readonly activities?: readonly string[];
    readonly createdAt: Date;
  }): Promise<{ dependentPersonId: string; membershipId: string }> {
    const input = z
      .strictObject({
        actorPersonId: z.string().uuid(),
        householdId: z.string().uuid(),
        displayName: z.string().trim().min(1).max(80),
        ...DependentContextSchema.shape,
        createdAt: z.date(),
      })
      .parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      await lockHousehold(transaction, input.householdId);
      await requireHouseholdCapability(
        transaction,
        input.householdId,
        input.actorPersonId,
        "household.govern",
      );
      const personId = randomUUID();
      const membershipId = randomUUID();
      const encrypted = this.secretBox.encrypt(input.displayName, `person-display-name:${personId}`);
      await transaction`
        insert into people (
          id, status, display_name_ciphertext, display_name_key_version,
          authority_version, control_epoch, created_at, updated_at
        ) values (
          ${personId}, 'provisional', ${Buffer.from(JSON.stringify(encrypted), "utf8")},
          ${encrypted.kid}, 1, 1, ${input.createdAt}, ${input.createdAt}
        )
      `;
      await transaction`
        insert into household_memberships (
          id, household_id, person_id, role, status, joined_at, version, created_at, updated_at
        ) values (
          ${membershipId}, ${input.householdId}, ${personId}, 'dependent', 'active',
          ${input.createdAt}, 1, ${input.createdAt}, ${input.createdAt}
        )
      `;
      await saveDependentContext(transaction, this.secretBox, {
        actorPersonId: input.actorPersonId,
        dependentPersonId: personId,
        context: input,
        changedAt: input.createdAt,
      });
      await transaction`
        update households set membership_version = membership_version + 1, updated_at = ${input.createdAt}
        where id = ${input.householdId}
      `;
      await appendHouseholdAudit(transaction, {
        householdId: input.householdId,
        actorPersonId: input.actorPersonId,
        eventType: "dependent_added",
        targetId: personId,
        reasons: ["represented_dependent", "no_account_created"],
        occurredAt: input.createdAt,
      });
      return { dependentPersonId: personId, membershipId };
    });
  }

  public async updateDependent(inputCandidate: {
    readonly actorPersonId: string;
    readonly householdId: string;
    readonly dependentPersonId: string;
    readonly displayName: string;
    readonly aliases?: readonly string[];
    readonly birthYear?: number | null;
    readonly school?: string;
    readonly activities?: readonly string[];
    readonly updatedAt: Date;
  }): Promise<void> {
    const input = z
      .strictObject({
        actorPersonId: z.string().uuid(),
        householdId: z.string().uuid(),
        dependentPersonId: z.string().uuid(),
        displayName: z.string().trim().min(1).max(80),
        ...DependentContextSchema.shape,
        updatedAt: z.date(),
      })
      .parse(inputCandidate);
    await inTransaction(this.database, async (transaction) => {
      await lockHousehold(transaction, input.householdId);
      await requireHouseholdCapability(
        transaction,
        input.householdId,
        input.actorPersonId,
        "household.govern",
      );
      const dependents = await transaction<{ person_id: string }[]>`
        select membership.person_id
        from household_memberships membership
        join people person on person.id = membership.person_id
        where membership.household_id = ${input.householdId}
          and membership.person_id = ${input.dependentPersonId}
          and membership.role = 'dependent' and membership.status = 'active'
          and person.status = 'provisional'
        for update of membership, person
      `;
      if (!dependents[0]) throw new NotFoundError("That represented child is no longer in this family");
      const encryptedName = this.secretBox.encrypt(
        input.displayName,
        `person-display-name:${input.dependentPersonId}`,
      );
      await transaction`
        update people
        set display_name_ciphertext = ${Buffer.from(JSON.stringify(encryptedName), "utf8")},
          display_name_key_version = ${encryptedName.kid}, updated_at = ${input.updatedAt}
        where id = ${input.dependentPersonId}
      `;
      await saveDependentContext(transaction, this.secretBox, {
        actorPersonId: input.actorPersonId,
        dependentPersonId: input.dependentPersonId,
        context: input,
        changedAt: input.updatedAt,
      });
      await appendHouseholdAudit(transaction, {
        householdId: input.householdId,
        actorPersonId: input.actorPersonId,
        eventType: "dependent_context_updated",
        targetId: input.dependentPersonId,
        reasons: ["represented_dependent", "family_context"],
        occurredAt: input.updatedAt,
      });
    });
  }
}

async function lockHousehold(transaction: Transaction, householdId: string): Promise<void> {
  const rows = await transaction<{ id: string }[]>`
    select id from households where id = ${householdId} for update
  `;
  if (!rows[0]) throw new NotFoundError("Household does not exist");
}

async function lockCurrentInvitationSource(
  transaction: Transaction,
  invitationId: string,
  inviteePersonId: string | null,
): Promise<{ readonly householdId: string; readonly sourceRevisionId: string | null }> {
  const rows = await transaction<
    { conversation_id: string; household_id: string; source_revision_id: string | null }[]
  >`
    select source_conversation.id as conversation_id, invitation.household_id,
      invitation.source_revision_id
    from invitations invitation
    join households household on household.id = invitation.household_id
      and household.status in ('onboarding', 'active')
      and household.membership_version = invitation.household_membership_version
    join household_memberships inviter_membership
      on inviter_membership.id = invitation.invited_by_membership_id
      and inviter_membership.household_id = household.id
      and inviter_membership.status = 'active'
    join person_identities invitee_identity on invitee_identity.id = invitation.invitee_identity_id
      and invitee_identity.status in ('observed', 'verified')
    join conversations source_conversation on source_conversation.id = invitation.source_conversation_id
      and source_conversation.kind = 'group' and source_conversation.status = 'active'
    join participant_epochs source_epoch on source_epoch.id = source_conversation.current_epoch_id
      and source_epoch.id = invitation.source_participant_epoch_id
      and source_epoch.ended_at is null
      and source_epoch.participant_set_digest = invitation.source_participant_digest
    join epoch_participants exact_invitee on exact_invitee.participant_epoch_id = source_epoch.id
      and exact_invitee.person_identity_id = invitee_identity.id
      and exact_invitee.person_id = invitee_identity.person_id
    where invitation.id = ${invitationId}
      and invitation.status = 'pending' and invitation.expires_at > now()
      and (${inviteePersonId}::uuid is null or invitee_identity.person_id = ${inviteePersonId})
      and (
        invitation.source_revision_id is null
        or exists(
          select 1
          from source_revisions source_revision
          join source_objects source_object on source_object.id = source_revision.source_object_id
            and source_object.status = 'active'
            and source_object.latest_revision_number = source_revision.revision_number
          where source_revision.id = invitation.source_revision_id
            and source_revision.participant_epoch_id = source_epoch.id
            and source_revision.revoked_at is null
            and source_revision.retention_until > now()
        )
      )
    for update of source_conversation
  `;
  if (!rows[0]) {
    throw new ConflictError(
      "This family invitation is no longer current because the group or family changed",
    );
  }
  return {
    householdId: rows[0].household_id,
    sourceRevisionId: rows[0].source_revision_id,
  };
}

async function lockInvitationEvidence(
  transaction: Transaction,
  invitationId: string,
  source: { readonly householdId: string; readonly sourceRevisionId: string | null },
): Promise<void> {
  const invitations = await transaction<{ readonly id: string }[]>`
    select id from invitations
    where id = ${invitationId}
      and household_id = ${source.householdId}
      and source_revision_id is not distinct from ${source.sourceRevisionId}::uuid
      and status = 'pending' and expires_at > now()
    for update
  `;
  if (!invitations[0]) {
    throw new ConflictError("This family invitation changed before it could be accepted");
  }
  if (source.sourceRevisionId === null) return;
  const revisions = await transaction<{ readonly id: string }[]>`
    select source_revision.id
    from source_revisions source_revision
    join source_objects source_object on source_object.id = source_revision.source_object_id
      and source_object.status = 'active'
      and source_object.latest_revision_number = source_revision.revision_number
    where source_revision.id = ${source.sourceRevisionId}
      and source_revision.revoked_at is null
      and source_revision.retention_until > now()
    for update of source_revision
  `;
  if (!revisions[0]) {
    throw new ConflictError("The message that created this family invitation is no longer current");
  }
}

async function loadInvitation(transaction: Transaction, invitationId: string): Promise<HouseholdInvitation> {
  const rows = await transaction<
    {
      invitation_id: string;
      household_id: string;
      requested_role: string;
      requested_capabilities: string[];
      status: string;
      household_membership_version: number | string;
      expires_at: Date;
      required_approver_person_ids: string[];
      approved_by_person_ids: string[];
    }[]
  >`
    select invitation.id as invitation_id, invitation.household_id,
      invitation.requested_role, invitation.requested_capabilities,
      invitation.status, invitation.household_membership_version,
      invitation.expires_at,
      coalesce(array_agg(membership.person_id order by membership.person_id)
        filter (where approval.approver_membership_id is not null), '{}')
        as required_approver_person_ids,
      coalesce(array_agg(membership.person_id order by membership.person_id)
        filter (where approval.approved_at is not null), '{}')
        as approved_by_person_ids
    from invitations invitation
    left join invitation_approvals approval on approval.invitation_id = invitation.id
    left join household_memberships membership on membership.id = approval.approver_membership_id
    where invitation.id = ${invitationId}
    group by invitation.id
  `;
  const invitation = rows[0];
  if (!invitation) throw new NotFoundError("Invitation does not exist");
  return HouseholdInvitationSchema.parse({
    invitationId: invitation.invitation_id,
    householdId: invitation.household_id,
    requestedRole: invitation.requested_role,
    requestedCapabilities: invitation.requested_capabilities,
    status: invitation.status,
    householdMembershipVersion: Number(invitation.household_membership_version),
    requiredApproverPersonIds: invitation.required_approver_person_ids,
    approvedByPersonIds: invitation.approved_by_person_ids,
    expiresAt: invitation.expires_at.toISOString(),
  });
}

function decryptProposedDisplayName(
  secretBox: SecretBox,
  invitationId: string,
  ciphertext: Buffer | null,
): string | null {
  if (!ciphertext) return null;
  try {
    const value = secretBox
      .decrypt(
        JSON.parse(ciphertext.toString("utf8")) as unknown,
        `invitation-proposed-display-name:${invitationId}`,
      )
      .toString("utf8")
      .trim();
    return value.length > 0 && value.length <= 80 ? value : null;
  } catch {
    return null;
  }
}

function sameSet(left: readonly string[], right: readonly string[]): boolean {
  if (left.length !== right.length) return false;
  const expected = new Set(right);
  return expected.size === right.length && left.every((value) => expected.has(value));
}

async function saveDependentContext(
  transaction: Transaction,
  secretBox: SecretBox,
  input: {
    readonly actorPersonId: string;
    readonly dependentPersonId: string;
    readonly context: DependentContext;
    readonly changedAt: Date;
  },
): Promise<void> {
  const aliases = uniqueValues(input.context.aliases);
  const activities = uniqueValues(input.context.activities);
  const encryptedAliases = aliases.length
    ? secretBox.encrypt(JSON.stringify(aliases), `dependent-aliases:${input.dependentPersonId}`)
    : null;
  const encryptedSchool = input.context.school
    ? secretBox.encrypt(input.context.school, `dependent-school:${input.dependentPersonId}`)
    : null;
  const encryptedActivities = activities.length
    ? secretBox.encrypt(JSON.stringify(activities), `dependent-activities:${input.dependentPersonId}`)
    : null;
  await transaction`
    insert into dependent_profiles (
      person_id, aliases_ciphertext, aliases_key_version, birth_year,
      school_ciphertext, school_key_version, activities_ciphertext,
      activities_key_version, updated_by_person_id, created_at, updated_at
    ) values (
      ${input.dependentPersonId},
      ${encryptedAliases ? Buffer.from(JSON.stringify(encryptedAliases), "utf8") : null},
      ${encryptedAliases?.kid ?? null}, ${input.context.birthYear},
      ${encryptedSchool ? Buffer.from(JSON.stringify(encryptedSchool), "utf8") : null},
      ${encryptedSchool?.kid ?? null},
      ${encryptedActivities ? Buffer.from(JSON.stringify(encryptedActivities), "utf8") : null},
      ${encryptedActivities?.kid ?? null}, ${input.actorPersonId},
      ${input.changedAt}, ${input.changedAt}
    )
    on conflict (person_id) do update set
      aliases_ciphertext = excluded.aliases_ciphertext,
      aliases_key_version = excluded.aliases_key_version,
      birth_year = excluded.birth_year,
      school_ciphertext = excluded.school_ciphertext,
      school_key_version = excluded.school_key_version,
      activities_ciphertext = excluded.activities_ciphertext,
      activities_key_version = excluded.activities_key_version,
      updated_by_person_id = excluded.updated_by_person_id,
      updated_at = excluded.updated_at
  `;
}

function uniqueValues(values: readonly string[]): string[] {
  const output: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const trimmed = value.trim();
    const key = trimmed.toLocaleLowerCase("en-US");
    if (!trimmed || seen.has(key)) continue;
    seen.add(key);
    output.push(trimmed);
  }
  return output;
}

function capabilitiesForRole(role: z.infer<typeof RoleSchema>) {
  if (role === "steward") return [...StewardCapabilities];
  if (role === "caregiver") {
    return MembershipCapabilitySchema.array().parse([
      "household.read",
      "coordination.originate",
      "coordination.coordinate",
    ]);
  }
  return MembershipCapabilitySchema.array().parse(["household.read", "coordination.originate"]);
}

async function requireHouseholdCapability(
  transaction: Transaction,
  householdId: string,
  personId: string,
  capability: string,
): Promise<void> {
  const rows = await transaction<{ allowed: boolean }[]>`
    select exists(
      select 1 from household_memberships membership
      join membership_capabilities grant_row on grant_row.membership_id = membership.id
        and grant_row.capability = ${capability} and grant_row.status = 'active'
      where membership.household_id = ${householdId}
        and membership.person_id = ${personId} and membership.status = 'active'
    ) as allowed
  `;
  if (!rows[0]?.allowed) throw new UnauthorizedError("You cannot change this family");
}

async function appendHouseholdAudit(
  transaction: Transaction,
  input: {
    readonly householdId: string;
    readonly actorPersonId: string;
    readonly eventType: string;
    readonly targetId: string;
    readonly reasons: readonly string[];
    readonly occurredAt: Date;
  },
): Promise<void> {
  const sequence = await transaction<{ next_sequence: number | string }[]>`
    select coalesce(max(sequence), 0) + 1 as next_sequence
    from audit_events where household_id = ${input.householdId}
  `;
  await transaction`
    insert into audit_events (
      id, household_id, person_id, sequence, actor_kind, actor_id,
      event_type, target_type, target_id, reason_codes, occurred_at
    ) values (
      ${randomUUID()}, ${input.householdId}, ${input.actorPersonId},
      ${Number(sequence[0]?.next_sequence ?? 1)}, 'person', ${input.actorPersonId},
      ${input.eventType}, 'household_relationship', ${input.targetId},
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

function sha256Hex(value: Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}
