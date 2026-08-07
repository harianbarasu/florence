import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { Database } from "../../db/client.js";
import type { SecretBox } from "../../shared/crypto.js";
import {
  type RoutineRecurrence,
  RoutineRecurrenceSchema,
  type SemanticTimePlan,
  SemanticTimePlanSchema,
} from "../coordination/index.js";

type Transaction = TransactionSql<Record<string, never>>;

const MAX_CHILDREN = 16;
const MAX_ROUTINES = 16;

const ProjectionInputSchema = z.strictObject({
  householdId: z.string().uuid(),
  conversationId: z.string().uuid(),
  participantEpochId: z.string().uuid(),
  participantSetDigest: z.string().regex(/^[a-f0-9]{64}$/u),
  senderPersonId: z.string().uuid(),
});

export interface HouseholdContextProjectionInput {
  readonly householdId: string;
  readonly conversationId: string;
  readonly participantEpochId: string;
  readonly participantSetDigest: string;
  readonly senderPersonId: string;
}

export interface AuthorizedHouseholdContextProjection {
  readonly householdId: string;
  readonly representedChildren: readonly {
    readonly personId: string;
    readonly displayName: string | null;
    readonly aliases: readonly string[];
    readonly school: string | null;
    readonly activities: readonly string[];
  }[];
  readonly activeRoutines: readonly {
    readonly routineId: string;
    readonly title: string;
    readonly minimumSharedMeaning: string;
    readonly recurrence: RoutineRecurrence;
    readonly timePlan: SemanticTimePlan;
    readonly proposedHolderPersonId: string | null;
    readonly standingHolderPersonId: string | null;
    readonly effectiveFrom: string;
    readonly effectiveThrough: string | null;
  }[];
  readonly truncated: {
    readonly representedChildren: boolean;
    readonly activeRoutines: boolean;
  };
}

interface ChildRow {
  readonly person_id: string;
  readonly display_name_ciphertext: Buffer | null;
  readonly display_name_key_version: string | null;
  readonly aliases_ciphertext: Buffer | null;
  readonly aliases_key_version: string | null;
  readonly school_ciphertext: Buffer | null;
  readonly school_key_version: string | null;
  readonly activities_ciphertext: Buffer | null;
  readonly activities_key_version: string | null;
}

interface RoutineRow {
  readonly routine_id: string;
  readonly revision: number | string;
  readonly content_ciphertext: Buffer;
  readonly content_key_version: string;
  readonly recurrence: unknown;
  readonly semantic_time_plan: unknown;
  readonly proposed_holder_person_id: string | null;
  readonly standing_holder_person_id: string | null;
  readonly effective_from: string;
  readonly effective_through: string | null;
}

const RoutineContentSchema = z.strictObject({
  title: z.string().trim().min(1).max(200),
  minimumSharedMeaning: z.string().trim().min(1).max(500),
});

/**
 * Projects only normalized household facts authorized for one exact current
 * conversation epoch. No source records or provider content cross this seam.
 */
export class PostgresHouseholdContextProjection {
  public constructor(
    private readonly database: Database,
    private readonly secretBox: SecretBox,
  ) {}

  public async project(
    inputCandidate: HouseholdContextProjectionInput,
  ): Promise<AuthorizedHouseholdContextProjection | null> {
    const input = ProjectionInputSchema.parse(inputCandidate);
    return this.database.begin("isolation level repeatable read read only", async (transaction) => {
      if (!(await isAuthorized(transaction, input))) return null;
      const parentOnlyAudience = await isParentOnlyAudience(transaction, input);

      const childRows = await transaction<ChildRow[]>`
        select person.id as person_id, person.display_name_ciphertext,
          person.display_name_key_version, profile.aliases_ciphertext,
          profile.aliases_key_version, profile.school_ciphertext,
          profile.school_key_version, profile.activities_ciphertext,
          profile.activities_key_version
        from household_memberships membership
        join people person on person.id = membership.person_id
          and person.status in ('provisional', 'registered')
        left join dependent_profiles profile on profile.person_id = person.id
        where membership.household_id = ${input.householdId}
          and membership.role = 'dependent' and membership.status = 'active'
        order by membership.joined_at, person.id
        limit ${MAX_CHILDREN + 1}
      `;
      const routineRows = await transaction<RoutineRow[]>`
        select routine.id as routine_id, revision.revision,
          revision.content_ciphertext, revision.content_key_version,
          revision.recurrence, revision.semantic_time_plan,
          revision.proposed_holder_person_id, revision.standing_holder_person_id,
          revision.effective_from::text, revision.effective_through::text
        from routines routine
        join households household on household.id = routine.household_id
        join routine_revisions revision on revision.routine_id = routine.id
          and revision.revision = routine.current_revision
        where routine.household_id = ${input.householdId} and routine.status = 'active'
          and revision.destination_conversation_id = ${input.conversationId}
          and revision.participant_epoch_id = ${input.participantEpochId}
          and revision.participant_set_digest = ${input.participantSetDigest}
          and revision.effective_from <= timezone(household.timezone, now())::date
          and (
            revision.effective_through is null
            or revision.effective_through >= timezone(household.timezone, now())::date
          )
        order by routine.updated_at desc, routine.id
        limit ${MAX_ROUTINES + 1}
      `;

      return {
        householdId: input.householdId,
        // Caregiver/participant chats receive only routines explicitly scoped
        // to that exact destination. Broad child profiles stay parent-only.
        representedChildren: parentOnlyAudience
          ? childRows.slice(0, MAX_CHILDREN).map((row) => ({
              personId: row.person_id,
              displayName: openText(
                this.secretBox,
                row.display_name_ciphertext,
                row.display_name_key_version,
                `person-display-name:${row.person_id}`,
                80,
              ),
              aliases: openList(
                this.secretBox,
                row.aliases_ciphertext,
                row.aliases_key_version,
                `dependent-aliases:${row.person_id}`,
                12,
                80,
              ),
              school: openText(
                this.secretBox,
                row.school_ciphertext,
                row.school_key_version,
                `dependent-school:${row.person_id}`,
                160,
              ),
              activities: openList(
                this.secretBox,
                row.activities_ciphertext,
                row.activities_key_version,
                `dependent-activities:${row.person_id}`,
                24,
                120,
              ),
            }))
          : [],
        activeRoutines: routineRows.slice(0, MAX_ROUTINES).flatMap((row) => {
          const content = openRoutine(this.secretBox, row);
          const recurrence = RoutineRecurrenceSchema.safeParse(row.recurrence);
          const timePlan = SemanticTimePlanSchema.safeParse(row.semantic_time_plan);
          if (!content || !recurrence.success || !timePlan.success) return [];
          return [
            {
              routineId: row.routine_id,
              ...content,
              recurrence: recurrence.data,
              timePlan: timePlan.data,
              proposedHolderPersonId: row.proposed_holder_person_id,
              standingHolderPersonId: row.standing_holder_person_id,
              effectiveFrom: row.effective_from,
              effectiveThrough: row.effective_through,
            },
          ];
        }),
        truncated: {
          representedChildren: parentOnlyAudience && childRows.length > MAX_CHILDREN,
          activeRoutines: routineRows.length > MAX_ROUTINES,
        },
      };
    });
  }
}

async function isParentOnlyAudience(
  transaction: Transaction,
  input: z.infer<typeof ProjectionInputSchema>,
): Promise<boolean> {
  const rows = await transaction<{ readonly parent_only: boolean }[]>`
    select not exists(
      select 1
      from conversations conversation
      join participant_epochs epoch on epoch.id = conversation.current_epoch_id
        and epoch.ended_at is null
      join epoch_participants audience on audience.participant_epoch_id = epoch.id
      join household_memberships membership on membership.household_id = conversation.household_id
        and membership.person_id = audience.person_id and membership.status = 'active'
      where conversation.id = ${input.conversationId}
        and conversation.household_id = ${input.householdId}
        and epoch.id = ${input.participantEpochId}
        and epoch.participant_set_digest = ${input.participantSetDigest}
        and membership.role <> 'steward'
    ) as parent_only
  `;
  return rows[0]?.parent_only === true;
}

async function isAuthorized(
  transaction: Transaction,
  input: z.infer<typeof ProjectionInputSchema>,
): Promise<boolean> {
  const rows = await transaction<{ readonly authorized: boolean }[]>`
    select exists(
      select 1
      from conversations conversation
      join households household on household.id = conversation.household_id
        and household.status in ('onboarding', 'active', 'paused')
      join participant_epochs epoch on epoch.id = conversation.current_epoch_id
        and epoch.ended_at is null
      join epoch_participants sender on sender.participant_epoch_id = epoch.id
        and sender.person_id = ${input.senderPersonId}
        and sender.registration_status = 'registered' and sender.consented_at is not null
      join household_memberships sender_membership
        on sender_membership.household_id = household.id
        and sender_membership.person_id = sender.person_id and sender_membership.status = 'active'
      join membership_capabilities sender_read on sender_read.membership_id = sender_membership.id
        and sender_read.capability = 'household.read' and sender_read.status = 'active'
      where conversation.id = ${input.conversationId}
        and conversation.household_id = ${input.householdId}
        and conversation.status = 'active'
        and epoch.id = ${input.participantEpochId}
        and epoch.participant_set_digest = ${input.participantSetDigest}
        and not exists (
          select 1 from epoch_participants audience
          where audience.participant_epoch_id = epoch.id
            and (
              audience.registration_status <> 'registered' or audience.consented_at is null
              or not exists (
                select 1 from people person
                join person_identities identity on identity.id = audience.person_identity_id
                  and identity.person_id = person.id and identity.status = 'verified'
                where person.id = audience.person_id and person.status = 'registered'
              )
              or not exists (
                select 1 from participant_policies policy
                where policy.conversation_id = conversation.id
                  and policy.person_id = audience.person_id and policy.status = 'active'
                  and policy.allow_content_processing
              )
              or not exists (
                select 1 from household_memberships membership
                join membership_capabilities read_grant on read_grant.membership_id = membership.id
                  and read_grant.capability = 'household.read' and read_grant.status = 'active'
                where membership.household_id = household.id
                  and membership.person_id = audience.person_id and membership.status = 'active'
              )
            )
        )
        and not exists (
          select 1 from channel_suppressions suppression
          where suppression.conversation_id = conversation.id and suppression.active
            and suppression.kind in ('stop', 'pause', 'deletion_fence', 'safety_hold')
        )
    ) as authorized
  `;
  return rows[0]?.authorized === true;
}

function openText(
  secretBox: SecretBox,
  ciphertext: Buffer | null,
  keyVersion: string | null,
  purpose: string,
  maxLength: number,
): string | null {
  if (!ciphertext || !keyVersion) return null;
  try {
    const encrypted = JSON.parse(ciphertext.toString("utf8")) as { readonly kid?: unknown };
    if (encrypted.kid !== keyVersion) return null;
    const value = secretBox.decrypt(encrypted, purpose).toString("utf8").replace(/\s+/gu, " ").trim();
    return value.length > 0 && value.length <= maxLength ? value : null;
  } catch {
    return null;
  }
}

function openList(
  secretBox: SecretBox,
  ciphertext: Buffer | null,
  keyVersion: string | null,
  purpose: string,
  maxItems: number,
  maxItemLength: number,
): readonly string[] {
  if (!ciphertext || !keyVersion) return [];
  try {
    const encrypted = JSON.parse(ciphertext.toString("utf8")) as { readonly kid?: unknown };
    if (encrypted.kid !== keyVersion) return [];
    const parsed = JSON.parse(secretBox.decrypt(encrypted, purpose).toString("utf8")) as unknown;
    if (!Array.isArray(parsed)) return [];
    const values: string[] = [];
    const seen = new Set<string>();
    for (const candidate of parsed) {
      if (typeof candidate !== "string") continue;
      const value = candidate.replace(/\s+/gu, " ").trim();
      const key = value.toLocaleLowerCase("en-US");
      if (!value || value.length > maxItemLength || seen.has(key)) continue;
      seen.add(key);
      values.push(value);
      if (values.length === maxItems) break;
    }
    return values;
  } catch {
    return [];
  }
}

function openRoutine(
  secretBox: SecretBox,
  row: RoutineRow,
): { readonly title: string; readonly minimumSharedMeaning: string } | null {
  try {
    const encrypted = JSON.parse(row.content_ciphertext.toString("utf8")) as {
      readonly kid?: unknown;
    };
    if (encrypted.kid !== row.content_key_version) return null;
    return RoutineContentSchema.parse(
      JSON.parse(
        secretBox
          .decrypt(encrypted, `routine-revision-content:${row.routine_id}:${Number(row.revision)}`)
          .toString("utf8"),
      ),
    );
  } catch {
    return null;
  }
}
