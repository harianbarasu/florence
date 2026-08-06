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
  readonly inviteePersonId: string;
  readonly role: z.infer<typeof RoleSchema>;
  readonly createdAt: Date;
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
          participant_epoch_id: string;
          participant_set_digest: string;
        }[]
      >`
        select identity.id as identity_id, identity.subject_digest,
          participant.registration_status, participant.consented_at,
          epoch.id as participant_epoch_id, epoch.participant_set_digest
        from conversations conversation
        join participant_epochs epoch on epoch.id = conversation.current_epoch_id
          and epoch.ended_at is null
        join epoch_participants participant on participant.participant_epoch_id = epoch.id
          and participant.person_id = ${input.inviteePersonId}
        join person_identities identity on identity.id = participant.person_identity_id
          and identity.status in ('observed', 'verified')
        where conversation.id = ${input.conversationId}
          and conversation.kind = 'group' and conversation.status = 'active'
          and exists(
            select 1 from epoch_participants actor
            where actor.participant_epoch_id = epoch.id and actor.person_id = ${input.actorPersonId}
          )
      `;
      const target = targets[0];
      if (!target) throw new UnauthorizedError("That person is no longer in the current group");
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
      await transaction`
        update invitations
        set source_conversation_id = ${input.conversationId},
          source_participant_epoch_id = ${target.participant_epoch_id},
          source_participant_digest = ${target.participant_set_digest},
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
