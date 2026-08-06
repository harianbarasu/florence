import { createHash, randomBytes, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { Database } from "../../db/client.js";
import type { SecretBox } from "../../shared/crypto.js";
import { ConflictError, NotFoundError, UnauthorizedError } from "../../shared/errors.js";
import {
  type HouseholdInvitation,
  type HouseholdMembership,
  MembershipCapabilitySchema,
  PostgresIdentityRelationships,
  StewardCapabilities,
} from "../identity/index.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

const RoleSchema = z.enum(["steward", "caregiver", "participant"]);

export interface HouseholdInvitationInput {
  readonly actorPersonId: string;
  readonly householdId: string;
  readonly conversationId: string;
  readonly inviteePersonId: string;
  readonly role: z.infer<typeof RoleSchema>;
  readonly createdAt: Date;
}

/**
 * Keeps relationship onboarding behind one authority seam. Invitations target
 * a verified person already present in an exact live Florence group, so the UI
 * never needs to reveal or accept somebody else's phone number.
 */
export class HouseholdOnboarding {
  public constructor(
    private readonly database: Executor,
    private readonly secretBox: SecretBox,
  ) {}

  public async inviteCurrentParticipant(
    inputCandidate: HouseholdInvitationInput,
  ): Promise<HouseholdInvitation> {
    const input = z
      .strictObject({
        actorPersonId: z.string().uuid(),
        householdId: z.string().uuid(),
        conversationId: z.string().uuid(),
        inviteePersonId: z.string().uuid(),
        role: RoleSchema,
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
        }[]
      >`
        select identity.id as identity_id, identity.subject_digest,
          participant.registration_status, participant.consented_at
        from conversations conversation
        join participant_epochs epoch on epoch.id = conversation.current_epoch_id
          and epoch.ended_at is null
        join epoch_participants participant on participant.participant_epoch_id = epoch.id
          and participant.person_id = ${input.inviteePersonId}
        join person_identities identity on identity.id = participant.person_identity_id
          and identity.status = 'verified'
        where conversation.id = ${input.conversationId}
          and conversation.kind = 'group' and conversation.status = 'active'
          and exists(
            select 1 from epoch_participants actor
            where actor.participant_epoch_id = epoch.id and actor.person_id = ${input.actorPersonId}
          )
      `;
      const target = targets[0];
      if (target?.registration_status !== "registered" || !target.consented_at) {
        throw new UnauthorizedError("That group participant must register privately before joining a family");
      }
      const existing = await transaction<{ status: string }[]>`
        select status from household_memberships
        where household_id = ${input.householdId} and person_id = ${input.inviteePersonId}
      `;
      if (existing[0]?.status === "active") throw new ConflictError("That person is already in this family");

      const capabilities = capabilitiesForRole(input.role);
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
      await appendHouseholdAudit(transaction, {
        householdId: input.householdId,
        actorPersonId: input.actorPersonId,
        eventType: "household_invitation_created",
        targetId: invitation.invitationId,
        reasons: [input.role, "exact_current_group_participant"],
        occurredAt: input.createdAt,
      });
      return invitation;
    });
  }

  public async approveInvitation(input: {
    readonly actorPersonId: string;
    readonly invitationId: string;
    readonly approvedAt: Date;
  }): Promise<HouseholdInvitation> {
    return new PostgresIdentityRelationships(this.database).approveInvitation({
      invitationId: input.invitationId,
      approverPersonId: input.actorPersonId,
      approvedAt: input.approvedAt.toISOString(),
    });
  }

  public async acceptInvitation(input: {
    readonly actorPersonId: string;
    readonly invitationId: string;
    readonly acceptedAt: Date;
  }): Promise<HouseholdMembership> {
    return inTransaction(this.database, async (transaction) => {
      const invitations = await transaction<{ token_digest: string; household_id: string }[]>`
        select invitation.token_digest, invitation.household_id
        from invitations invitation
        join person_identities identity on identity.id = invitation.invitee_identity_id
          and identity.person_id = ${input.actorPersonId} and identity.status = 'verified'
        where invitation.id = ${input.invitationId}
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
    readonly createdAt: Date;
  }): Promise<{ dependentPersonId: string; membershipId: string }> {
    const input = z
      .strictObject({
        actorPersonId: z.string().uuid(),
        householdId: z.string().uuid(),
        displayName: z.string().trim().min(1).max(80),
        createdAt: z.date(),
      })
      .parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
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
