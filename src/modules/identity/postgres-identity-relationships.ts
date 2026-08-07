import { randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { Database } from "../../db/client.js";
import { secureDigestEquals } from "../../shared/crypto.js";
import { ConflictError, NotFoundError, StaleAuthorityError, UnauthorizedError } from "../../shared/errors.js";
import {
  AcceptInvitationInputSchema,
  type ApproveInvitationInput,
  ApproveInvitationInputSchema,
  type ClaimIdentityInput,
  ClaimIdentityInputSchema,
  type CreateHouseholdInput,
  CreateHouseholdInputSchema,
  CreateHouseholdResultSchema,
  HouseholdInvitationSchema,
  HouseholdMembershipSchema,
  type IdentityPrincipal,
  IdentityPrincipalSchema,
  type IdentityRelationships,
  type InviteHouseholdMemberInput,
  InviteHouseholdMemberInputSchema,
  type LeaveHouseholdInput,
  LeaveHouseholdInputSchema,
  type MembershipCapability,
  type ObserveIdentityInput,
  ObserveIdentityInputSchema,
  StewardCapabilities,
  type SuspendCaregiverInput,
  SuspendCaregiverInputSchema,
} from "./contracts.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

interface PrincipalRow {
  readonly person_id: string;
  readonly identity_id: string;
  readonly person_status: string;
  readonly identity_status: string;
  readonly identity_authority_version: number | string;
}

interface MembershipRow {
  readonly membership_id: string;
  readonly household_id: string;
  readonly person_id: string;
  readonly role: string;
  readonly status: string;
  readonly version: number | string;
  readonly capabilities: string[];
}

interface InvitationRow {
  readonly invitation_id: string;
  readonly household_id: string;
  readonly requested_role: string;
  readonly requested_capabilities: string[];
  readonly status: string;
  readonly household_membership_version: number | string;
  readonly expires_at: Date;
  readonly required_approver_person_ids: string[];
  readonly approved_by_person_ids: string[];
}

/**
 * The identity and relationship authority module. Every method owns its full
 * transaction; callers never assemble partial identity or membership writes.
 */
export class PostgresIdentityRelationships implements IdentityRelationships {
  public constructor(private readonly database: Executor) {}

  public async observeIdentity(inputCandidate: ObserveIdentityInput): Promise<IdentityPrincipal> {
    const input = ObserveIdentityInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      const lockKey = `${input.issuer}:${input.kind}:${input.subjectDigest}`;
      await transaction`select pg_advisory_xact_lock(hashtextextended(${lockKey}, 0))`;

      const existing = await transaction<PrincipalRow[]>`
        select
          identity.person_id,
          identity.id as identity_id,
          person.status as person_status,
          identity.status as identity_status,
          identity.authority_version as identity_authority_version
        from person_identities identity
        join people person on person.id = identity.person_id
        where identity.issuer = ${input.issuer}
          and identity.kind = ${input.kind}
          and identity.subject_digest = ${input.subjectDigest}
      `;
      if (existing[0]) return principalFromRow(existing[0]);

      const personId = randomUUID();
      const identityId = randomUUID();
      const observedAt = new Date(input.observedAt);
      await transaction`
        insert into people (id, status, created_at, updated_at)
        values (${personId}, 'provisional', ${observedAt}, ${observedAt})
      `;
      await transaction`
        insert into person_identities (
          id, person_id, kind, issuer, subject_digest, status,
          authority_version, observed_at, created_at, updated_at
        ) values (
          ${identityId}, ${personId}, ${input.kind}, ${input.issuer}, ${input.subjectDigest},
          'observed', 1, ${observedAt}, ${observedAt}, ${observedAt}
        )
      `;
      return IdentityPrincipalSchema.parse({
        personId,
        identityId,
        personStatus: "provisional",
        identityStatus: "observed",
        identityAuthorityVersion: 1,
      });
    });
  }

  public async claimIdentity(inputCandidate: ClaimIdentityInput): Promise<IdentityPrincipal> {
    const input = ClaimIdentityInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      const rows = await transaction<(PrincipalRow & { readonly person_control_epoch: number | string })[]>`
        select
          identity.person_id,
          identity.id as identity_id,
          person.status as person_status,
          person.control_epoch as person_control_epoch,
          identity.status as identity_status,
          identity.authority_version as identity_authority_version
        from person_identities identity
        join people person on person.id = identity.person_id
        where identity.id = ${input.identityId}
        for update of identity, person
      `;
      const current = rows[0];
      if (!current) throw new NotFoundError("Identity does not exist");
      if (Number(current.identity_authority_version) !== input.expectedIdentityAuthorityVersion) {
        throw new StaleAuthorityError("Identity authority changed before confirmation");
      }
      if (current.identity_status === "revoked") {
        throw new UnauthorizedError("A revoked identity cannot be claimed");
      }

      const targetPersonId = input.targetPersonId ?? current.person_id;
      if (current.identity_status === "verified") {
        if (targetPersonId !== current.person_id) {
          throw new ConflictError("A verified identity already has a claimed owner");
        }
        return principalFromRow(current);
      }
      if (current.person_status !== "provisional") {
        throw new ConflictError("Only a provisional identity may be claimed");
      }

      if (targetPersonId !== current.person_id) {
        const targets = await transaction<{ readonly status: string }[]>`
          select status from people where id = ${targetPersonId} for update
        `;
        const target = targets[0];
        if (!target) throw new NotFoundError("Claim target person does not exist");
        if (target.status !== "registered") {
          throw new UnauthorizedError("An identity may only join a registered person");
        }
      }

      const confirmedAt = new Date(input.consentedAt);
      await transaction`
        update person_identities
        set person_id = ${targetPersonId},
            status = 'verified',
            verified_at = ${confirmedAt},
            authority_version = authority_version + 1,
            updated_at = ${confirmedAt}
        where id = ${input.identityId}
      `;

      if (targetPersonId !== current.person_id) {
        // Exact epochs are keyed by immutable identities. The attached person may
        // follow a verified identity claim, including when the same person owns
        // more than one handle in that historical audience.
        await transaction`
          update epoch_participants participant
          set person_id = ${targetPersonId}
          where participant.person_identity_id = ${input.identityId}
            and participant.person_id = ${current.person_id}
        `;
      }

      if (targetPersonId === current.person_id) {
        await transaction`
          update people
          set status = 'registered',
              timezone = ${input.timezone},
              consented_at = ${confirmedAt},
              registered_at = ${confirmedAt},
              authority_version = authority_version + 1,
              control_epoch = control_epoch + 1,
              updated_at = ${confirmedAt}
          where id = ${targetPersonId}
        `;
      } else {
        await transaction`
          update people
          set status = 'merged',
              merged_into_person_id = ${targetPersonId},
              authority_version = authority_version + 1,
              control_epoch = control_epoch + 1,
              updated_at = ${confirmedAt}
          where id = ${current.person_id}
        `;
        await transaction`
          update people
          set authority_version = authority_version + 1,
              control_epoch = control_epoch + 1,
              updated_at = ${confirmedAt}
          where id = ${targetPersonId}
        `;
      }
      await transaction`
        update person_sessions
        set revoked_at = ${confirmedAt}
        where person_id in (${current.person_id}, ${targetPersonId}) and revoked_at is null
      `;

      return IdentityPrincipalSchema.parse({
        personId: targetPersonId,
        identityId: input.identityId,
        personStatus: "registered",
        identityStatus: "verified",
        identityAuthorityVersion: input.expectedIdentityAuthorityVersion + 1,
      });
    });
  }

  public async createHousehold(inputCandidate: CreateHouseholdInput) {
    const input = CreateHouseholdInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      await requireRegisteredPerson(transaction, input.founderPersonId);
      const householdId = randomUUID();
      const membershipId = randomUUID();
      const createdAt = new Date(input.createdAt);
      await transaction`
        insert into households (id, timezone, status, created_at, updated_at)
        values (${householdId}, ${input.timezone}, 'onboarding', ${createdAt}, ${createdAt})
      `;
      await transaction`
        insert into household_memberships (
          id, household_id, person_id, role, status, consented_at, joined_at,
          version, created_at, updated_at
        ) values (
          ${membershipId}, ${householdId}, ${input.founderPersonId}, 'steward', 'active',
          ${createdAt}, ${createdAt}, 1, ${createdAt}, ${createdAt}
        )
      `;
      for (const capability of StewardCapabilities) {
        await transaction`
          insert into membership_capabilities (
            membership_id, capability, status, granted_by_membership_id, granted_at
          ) values (${membershipId}, ${capability}, 'active', ${membershipId}, ${createdAt})
        `;
      }
      return CreateHouseholdResultSchema.parse({
        householdId,
        membership: {
          membershipId,
          householdId,
          personId: input.founderPersonId,
          role: "steward",
          status: "active",
          capabilities: StewardCapabilities,
          version: 1,
        },
        membershipVersion: 1,
      });
    });
  }

  public async inviteMember(inputCandidate: InviteHouseholdMemberInput) {
    const input = InviteHouseholdMemberInputSchema.parse(inputCandidate);
    if (new Date(input.expiresAt) <= new Date(input.createdAt)) {
      throw new ConflictError("An invitation must expire after it is created");
    }
    return inTransaction(this.database, async (transaction) => {
      const households = await transaction<{ readonly membership_version: number | string }[]>`
        select membership_version from households
        where id = ${input.householdId} and status in ('onboarding', 'active')
        for update
      `;
      const household = households[0];
      if (!household) throw new NotFoundError("Active household does not exist");
      const inviter = await loadActiveMembership(transaction, input.householdId, input.inviterPersonId);
      requireCapability(inviter.capabilities, "membership.invite");
      assertDelegableCapabilities(inviter.capabilities, input.requestedCapabilities);
      if (input.requestedRole === "steward") {
        requireCapability(inviter.capabilities, "household.govern");
        if (!sameSet(input.requestedCapabilities, StewardCapabilities)) {
          throw new ConflictError("A co-equal steward invitation requires the complete steward grants");
        }
      }

      if (input.inviteeIdentityId) {
        const identities = await transaction<{ readonly subject_digest: string; readonly status: string }[]>`
          select subject_digest, status from person_identities where id = ${input.inviteeIdentityId}
        `;
        const identity = identities[0];
        if (
          !identity ||
          identity.subject_digest !== input.inviteeSubjectDigest ||
          identity.status === "revoked"
        ) {
          throw new ConflictError("Invitation target does not match the selected identity");
        }
      }

      await transaction`
        update invitations
        set status = 'expired', updated_at = ${new Date(input.createdAt)}
        where household_id = ${input.householdId}
          and invitee_subject_digest = ${input.inviteeSubjectDigest}
          and status = 'pending' and expires_at <= ${new Date(input.createdAt)}
      `;

      const invitationId = randomUUID();
      const createdAt = new Date(input.createdAt);
      await transaction`
        insert into invitations (
          id, household_id, invited_by_membership_id, invitee_identity_id,
          invitee_subject_digest, token_digest, requested_role, requested_capabilities,
          household_membership_version, status, expires_at, created_at, updated_at
        ) values (
          ${invitationId}, ${input.householdId}, ${inviter.membershipId},
          ${input.inviteeIdentityId ?? null}, ${input.inviteeSubjectDigest}, ${input.tokenDigest},
          ${input.requestedRole}, ${transaction.array(input.requestedCapabilities)},
          ${Number(household.membership_version)}, 'pending', ${new Date(input.expiresAt)},
          ${createdAt}, ${createdAt}
        )
      `;

      if (input.requestedRole === "steward") {
        const stewards = await transaction<{ readonly membership_id: string }[]>`
          select id as membership_id
          from household_memberships
          where household_id = ${input.householdId} and role = 'steward' and status = 'active'
          order by id
        `;
        for (const steward of stewards) {
          await transaction`
            insert into invitation_approvals (invitation_id, approver_membership_id, approved_at)
            values (
              ${invitationId}, ${steward.membership_id},
              ${steward.membership_id === inviter.membershipId ? createdAt : null}
            )
          `;
        }
      }
      return loadInvitation(transaction, invitationId);
    });
  }

  public async approveInvitation(inputCandidate: ApproveInvitationInput) {
    const input = ApproveInvitationInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      const invitations = await transaction<
        {
          readonly household_id: string;
          readonly status: string;
          readonly expires_at: Date;
          readonly household_membership_version: number | string;
          readonly current_membership_version: number | string;
        }[]
      >`
        select invitation.household_id, invitation.status, invitation.expires_at,
          invitation.household_membership_version,
          household.membership_version as current_membership_version
        from invitations invitation
        join households household on household.id = invitation.household_id
        where invitation.id = ${input.invitationId}
        for update of invitation, household
      `;
      const invitation = invitations[0];
      if (!invitation) throw new NotFoundError("Invitation does not exist");
      assertInvitationCurrent(invitation, new Date(input.approvedAt));

      const approvals = await transaction<{ readonly approver_membership_id: string }[]>`
        select approval.approver_membership_id
        from invitation_approvals approval
        join household_memberships membership on membership.id = approval.approver_membership_id
        where approval.invitation_id = ${input.invitationId}
          and membership.person_id = ${input.approverPersonId}
          and membership.role = 'steward'
          and membership.status = 'active'
        for update of approval
      `;
      const approval = approvals[0];
      if (!approval)
        throw new UnauthorizedError("Only a required current steward may approve this invitation");
      await transaction`
        update invitation_approvals
        set approved_at = coalesce(approved_at, ${new Date(input.approvedAt)})
        where invitation_id = ${input.invitationId}
          and approver_membership_id = ${approval.approver_membership_id}
      `;
      return loadInvitation(transaction, input.invitationId);
    });
  }

  public async acceptInvitation(inputCandidate: unknown) {
    const input = AcceptInvitationInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      const invitations = await transaction<
        {
          readonly household_id: string;
          readonly invitee_identity_id: string | null;
          readonly invitee_subject_digest: string;
          readonly token_digest: string;
          readonly requested_role: string;
          readonly requested_capabilities: string[];
          readonly status: string;
          readonly expires_at: Date;
          readonly household_membership_version: number | string;
          readonly current_membership_version: number | string;
        }[]
      >`
        select invitation.household_id, invitation.invitee_identity_id,
          invitation.invitee_subject_digest, invitation.token_digest,
          invitation.requested_role, invitation.requested_capabilities,
          invitation.status, invitation.expires_at, invitation.household_membership_version,
          household.membership_version as current_membership_version
        from invitations invitation
        join households household on household.id = invitation.household_id
        where invitation.id = ${input.invitationId}
        for update of invitation, household
      `;
      const invitation = invitations[0];
      if (!invitation) throw new NotFoundError("Invitation does not exist");
      assertInvitationCurrent(invitation, new Date(input.acceptedAt));
      if (!secureDigestEquals(invitation.token_digest, input.tokenDigest)) {
        throw new UnauthorizedError("Invitation token does not match");
      }
      await requireRegisteredPerson(transaction, input.inviteePersonId);
      const matchingIdentities = await transaction<{ readonly id: string }[]>`
        select id from person_identities
        where person_id = ${input.inviteePersonId}
          and status = 'verified'
          and subject_digest = ${invitation.invitee_subject_digest}
          and (${invitation.invitee_identity_id}::uuid is null or id = ${invitation.invitee_identity_id})
      `;
      if (!matchingIdentities[0]) {
        throw new UnauthorizedError("Invitation must be accepted by its exact verified identity");
      }
      const pendingApprovals = await transaction<{ readonly count: number | string }[]>`
        select count(*) as count
        from invitation_approvals
        where invitation_id = ${input.invitationId} and approved_at is null
      `;
      if (Number(pendingApprovals[0]?.count ?? 0) > 0) {
        throw new UnauthorizedError("Every required steward must approve a co-steward invitation");
      }

      const acceptedAt = new Date(input.acceptedAt);
      const existing = await transaction<
        { readonly membership_id: string; readonly status: string; readonly version: number | string }[]
      >`
        select id as membership_id, status, version
        from household_memberships
        where household_id = ${invitation.household_id}
          and person_id = ${input.inviteePersonId}
        for update
      `;
      let membershipId: string;
      let version: number;
      if (existing[0]) {
        if (existing[0].status === "active") throw new ConflictError("Person is already a household member");
        membershipId = existing[0].membership_id;
        version = Number(existing[0].version) + 1;
        await transaction`
          update household_memberships
          set role = ${invitation.requested_role}, status = 'active', consented_at = ${acceptedAt},
              joined_at = ${acceptedAt}, ended_at = null, version = ${version}, updated_at = ${acceptedAt}
          where id = ${membershipId}
        `;
        await transaction`
          update membership_capabilities
          set status = 'revoked', revoked_at = ${acceptedAt}
          where membership_id = ${membershipId} and status = 'active'
        `;
      } else {
        membershipId = randomUUID();
        version = 1;
        await transaction`
          insert into household_memberships (
            id, household_id, person_id, role, status, consented_at, joined_at,
            version, created_at, updated_at
          ) values (
            ${membershipId}, ${invitation.household_id}, ${input.inviteePersonId},
            ${invitation.requested_role}, 'active', ${acceptedAt}, ${acceptedAt},
            1, ${acceptedAt}, ${acceptedAt}
          )
        `;
      }
      for (const capability of invitation.requested_capabilities) {
        await transaction`
          insert into membership_capabilities (
            membership_id, capability, status, granted_by_membership_id, granted_at, revoked_at
          ) values (
            ${membershipId}, ${capability}, 'active', null, ${acceptedAt}, null
          )
          on conflict (membership_id, capability) do update
          set status = 'active', granted_at = excluded.granted_at, revoked_at = null
        `;
      }
      await transaction`
        update invitations
        set status = 'accepted', accepted_by_person_id = ${input.inviteePersonId},
            accepted_at = ${acceptedAt}, updated_at = ${acceptedAt}
        where id = ${input.invitationId}
      `;
      await transaction`
        update households
        set membership_version = membership_version + 1, updated_at = ${acceptedAt}
        where id = ${invitation.household_id}
      `;
      return HouseholdMembershipSchema.parse({
        membershipId,
        householdId: invitation.household_id,
        personId: input.inviteePersonId,
        role: invitation.requested_role,
        status: "active",
        capabilities: invitation.requested_capabilities,
        version,
      });
    });
  }

  public async leaveHousehold(inputCandidate: LeaveHouseholdInput) {
    const input = LeaveHouseholdInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      const membership = await loadActiveMembership(transaction, input.householdId, input.personId, true);
      const leftAt = new Date(input.leftAt);
      const nextVersion = membership.version + 1;
      await transaction`
        update household_memberships
        set status = 'left', ended_at = ${leftAt}, version = ${nextVersion}, updated_at = ${leftAt}
        where id = ${membership.membershipId}
      `;
      await transaction`
        update membership_capabilities
        set status = 'revoked', revoked_at = ${leftAt}
        where membership_id = ${membership.membershipId} and status = 'active'
      `;
      await transaction`
        update households
        set membership_version = membership_version + 1,
            status = case when not exists (
              select 1 from household_memberships
              where household_id = ${input.householdId} and role = 'steward' and status = 'active'
                and id <> ${membership.membershipId}
            ) then 'paused' else status end,
            updated_at = ${leftAt}
        where id = ${input.householdId}
      `;
      return HouseholdMembershipSchema.parse({
        ...membership,
        status: "left",
        capabilities: [],
        version: nextVersion,
      });
    });
  }

  public async suspendCaregiver(inputCandidate: SuspendCaregiverInput) {
    const input = SuspendCaregiverInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      const steward = await loadActiveMembership(transaction, input.householdId, input.stewardPersonId);
      requireCapability(steward.capabilities, "membership.suspend");
      const caregiver = await loadActiveMembership(
        transaction,
        input.householdId,
        input.caregiverPersonId,
        true,
      );
      if (caregiver.role !== "caregiver") {
        throw new UnauthorizedError("A co-equal steward cannot be suspended unilaterally");
      }
      const suspendedAt = new Date(input.suspendedAt);
      const nextVersion = caregiver.version + 1;
      await transaction`
        update household_memberships
        set status = 'suspended', ended_at = ${suspendedAt}, version = ${nextVersion},
            updated_at = ${suspendedAt}
        where id = ${caregiver.membershipId}
      `;
      await transaction`
        update membership_capabilities
        set status = 'revoked', revoked_at = ${suspendedAt}
        where membership_id = ${caregiver.membershipId} and status = 'active'
      `;
      await transaction`
        update households
        set membership_version = membership_version + 1, updated_at = ${suspendedAt}
        where id = ${input.householdId}
      `;
      return HouseholdMembershipSchema.parse({
        ...caregiver,
        status: "suspended",
        capabilities: [],
        version: nextVersion,
      });
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

async function requireRegisteredPerson(transaction: Transaction, personId: string): Promise<void> {
  const rows = await transaction<{ readonly status: string }[]>`
    select status from people where id = ${personId} for update
  `;
  if (!rows[0]) throw new NotFoundError("Person does not exist");
  if (rows[0].status !== "registered") throw new UnauthorizedError("Person is not registered");
}

async function loadActiveMembership(
  transaction: Transaction,
  householdId: string,
  personId: string,
  forUpdate = false,
) {
  if (forUpdate) {
    await transaction`
      select id from household_memberships
      where household_id = ${householdId} and person_id = ${personId}
      for update
    `;
  }
  const rows = await transaction<MembershipRow[]>`
    select membership.id as membership_id, membership.household_id, membership.person_id,
      membership.role, membership.status, membership.version,
      coalesce(array_agg(capability.capability order by capability.capability)
        filter (where capability.status = 'active'), '{}') as capabilities
    from household_memberships membership
    left join membership_capabilities capability on capability.membership_id = membership.id
    where membership.household_id = ${householdId} and membership.person_id = ${personId}
    group by membership.id
  `;
  const membership = rows[0];
  if (!membership) throw new NotFoundError("Household membership does not exist");
  if (membership.status !== "active") throw new UnauthorizedError("Household membership is inactive");
  return HouseholdMembershipSchema.parse({
    membershipId: membership.membership_id,
    householdId: membership.household_id,
    personId: membership.person_id,
    role: membership.role,
    status: membership.status,
    capabilities: membership.capabilities,
    version: Number(membership.version),
  });
}

async function loadInvitation(transaction: Transaction, invitationId: string) {
  const rows = await transaction<InvitationRow[]>`
    select invitation.id as invitation_id, invitation.household_id,
      invitation.requested_role, invitation.requested_capabilities, invitation.status,
      invitation.household_membership_version, invitation.expires_at,
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

function principalFromRow(row: PrincipalRow): IdentityPrincipal {
  return IdentityPrincipalSchema.parse({
    personId: row.person_id,
    identityId: row.identity_id,
    personStatus: row.person_status,
    identityStatus: row.identity_status,
    identityAuthorityVersion: Number(row.identity_authority_version),
  });
}

function requireCapability(
  capabilities: readonly MembershipCapability[],
  required: MembershipCapability,
): void {
  if (!capabilities.includes(required)) throw new UnauthorizedError(`Missing ${required} capability`);
}

function assertDelegableCapabilities(
  inviterCapabilities: readonly MembershipCapability[],
  requestedCapabilities: readonly MembershipCapability[],
): void {
  const allowed = new Set(inviterCapabilities);
  if (requestedCapabilities.some((capability) => !allowed.has(capability))) {
    throw new UnauthorizedError("A membership cannot delegate authority its inviter does not hold");
  }
}

function sameSet(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value) => right.includes(value));
}

function assertInvitationCurrent(
  invitation: {
    readonly status: string;
    readonly expires_at: Date;
    readonly household_membership_version: number | string;
    readonly current_membership_version: number | string;
  },
  now: Date,
): void {
  if (invitation.status !== "pending") throw new ConflictError("Invitation is no longer pending");
  if (invitation.expires_at <= now) throw new UnauthorizedError("Invitation has expired");
  if (Number(invitation.household_membership_version) !== Number(invitation.current_membership_version)) {
    throw new StaleAuthorityError("Household membership changed after the invitation was created");
  }
}
