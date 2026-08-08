import { createHash, randomUUID } from "node:crypto";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  FamilyGroupAuthority,
  participantEpochAuthorityDigest,
  participantSetDigest,
} from "../../src/modules/conversations/index.js";
import { PostgresIdentityRelationships } from "../../src/modules/identity/index.js";

const databaseUrl = process.env.TEST_DATABASE_URL;
const schema = process.env.TEST_POSTGRES_SCHEMA ?? "florence_v4";
const describeDatabase = databaseUrl ? describe : describe.skip;

interface Fixture {
  readonly conversationId: string;
  readonly householdId: string;
  readonly personIds: readonly [string, string];
}

describeDatabase("family group authority", () => {
  let database: Sql<Record<string, never>>;
  let fixture: Fixture | null = null;

  beforeAll(async () => {
    database = postgres(databaseUrl as string, {
      max: 2,
      connection: { search_path: schema, TimeZone: "UTC" },
    });
    const tables = await database<{ readonly present: string | null }[]>`
      select to_regclass(${`${schema}.conversation_rules`})::text as present
    `;
    if (!tables[0]?.present) throw new Error(`Expected migrated conversation tables in ${schema}`);
  });

  afterEach(async () => {
    if (!fixture) return;
    await database`delete from conversations where id = ${fixture.conversationId}`;
    await database`delete from households where id = ${fixture.householdId}`;
    await database`
      delete from person_identities
      where person_id = any(${database.array([...fixture.personIds])}::uuid[])
    `;
    await database`delete from people where id = any(${database.array([...fixture.personIds])}::uuid[])`;
    fixture = null;
  });

  afterAll(async () => {
    await database.end();
  });

  it("activates one exact all-household rule idempotently and removes it when membership changes", async () => {
    const now = new Date();
    fixture = await seedFamilyGroup(database, now);
    const authority = new FamilyGroupAuthority(database);

    const activated = await authority.reconcile({
      conversationId: fixture.conversationId,
      occurredAt: now,
    });
    expect(activated).toMatchObject({
      status: "active",
      conversationId: fixture.conversationId,
      householdId: fixture.householdId,
      activatedNow: true,
    });
    expect(activated.ruleId).not.toBeNull();
    expect(activated.snapshot.rules).toEqual([
      expect.objectContaining({
        ruleId: activated.ruleId,
        ruleKey: "family_coverage",
        active: true,
      }),
    ]);

    const replay = await authority.reconcile({
      conversationId: fixture.conversationId,
      occurredAt: new Date(now.getTime() + 1_000),
    });
    expect(replay).toMatchObject({
      status: "active",
      householdId: fixture.householdId,
      ruleId: activated.ruleId,
      activatedNow: false,
    });
    const replayCounts = await database<{ readonly rules: number; readonly activation_audits: number }[]>`
      select
        (select count(*)::int from conversation_rules
          where conversation_id = ${fixture.conversationId}
            and rule_key = 'family_coverage') as rules,
        (select count(*)::int from audit_events
          where conversation_id = ${fixture.conversationId}
            and event_type = 'family_group_authority_activated') as activation_audits
    `;
    expect(replayCounts[0]).toEqual({ rules: 1, activation_audits: 1 });

    const membershipChangedAt = new Date(now.getTime() + 2_000);
    await new PostgresIdentityRelationships(database).leaveHousehold({
      householdId: fixture.householdId,
      personId: fixture.personIds[1],
      leftAt: membershipChangedAt.toISOString(),
    });
    const immediatelyRevoked = await database<{ readonly active_rules: number }[]>`
      select count(*)::int as active_rules from conversation_rules
      where conversation_id = ${fixture.conversationId}
        and rule_key = 'family_coverage' and status = 'active'
    `;
    expect(immediatelyRevoked[0]?.active_rules).toBe(0);
    const observeOnly = await authority.reconcile({
      conversationId: fixture.conversationId,
      occurredAt: membershipChangedAt,
    });
    expect(observeOnly).toMatchObject({
      status: "observe_only",
      householdId: null,
      ruleId: null,
      activatedNow: false,
    });
    expect(observeOnly.snapshot.rules).toEqual([]);

    const revoked = await database<
      {
        readonly conversation_household_id: string | null;
        readonly active_rules: number;
        readonly rule_status: string;
        readonly ended_at: Date | null;
        readonly removal_audits: number;
      }[]
    >`
      select conversation.household_id as conversation_household_id,
        count(*) filter (where rule.status = 'active')::int as active_rules,
        max(rule.status) as rule_status,
        max(rule.ended_at) as ended_at,
        (select count(*)::int from audit_events audit
          where audit.conversation_id = conversation.id
            and audit.event_type = 'family_group_authority_removed') as removal_audits
      from conversations conversation
      join conversation_rules rule on rule.conversation_id = conversation.id
        and rule.rule_key = 'family_coverage'
      where conversation.id = ${fixture.conversationId}
      group by conversation.id
    `;
    expect(revoked[0]).toMatchObject({
      conversation_household_id: null,
      active_rules: 0,
      rule_status: "superseded",
      removal_audits: 1,
    });
    expect(revoked[0]?.ended_at).not.toBeNull();
  });
});

async function seedFamilyGroup(database: Sql<Record<string, never>>, now: Date): Promise<Fixture> {
  const householdId = randomUUID();
  const conversationId = randomUUID();
  const epochId = randomUUID();
  const personIds = [randomUUID(), randomUUID()] as const;
  const identityIds = [randomUUID(), randomUUID()] as const;
  const participants = [
    { personId: personIds[0], identityId: identityIds[0], role: "steward" },
    { personId: personIds[1], identityId: identityIds[1], role: "caregiver" },
  ] as const;
  const setDigest = participantSetDigest(identityIds);

  await database.begin(async (transaction) => {
    for (const { identityId, personId } of participants) {
      await transaction`
        insert into people (
          id, status, timezone, consented_at, registered_at, created_at, updated_at
        ) values (
          ${personId}, 'registered', 'America/Los_Angeles', ${now}, ${now}, ${now}, ${now}
        )
      `;
      await transaction`
        insert into person_identities (
          id, person_id, kind, issuer, subject_digest, status,
          observed_at, verified_at, created_at, updated_at
        ) values (
          ${identityId}, ${personId}, 'phone', 'linq', ${sha256(identityId)}, 'verified',
          ${now}, ${now}, ${now}, ${now}
        )
      `;
    }
    await transaction`
      insert into households (id, timezone, status, created_at, updated_at)
      values (${householdId}, 'America/Los_Angeles', 'active', ${now}, ${now})
    `;
    for (const { personId, role } of participants) {
      await transaction`
        insert into household_memberships (
          id, household_id, person_id, role, status, consented_at, joined_at,
          created_at, updated_at
        ) values (
          ${randomUUID()}, ${householdId}, ${personId}, ${role},
          'active', ${now}, ${now}, ${now}, ${now}
        )
      `;
    }
    await transaction`
      insert into conversations (
        id, household_id, kind, purpose, status, created_at, updated_at
      ) values (
        ${conversationId}, null, 'group', 'family coordination', 'active', ${now}, ${now}
      )
    `;
    await transaction`
      insert into participant_epochs (
        id, conversation_id, sequence, participant_set_digest, authority_digest,
        change_reason, started_at
      ) values (
        ${epochId}, ${conversationId}, 1, ${setDigest},
        ${participantEpochAuthorityDigest({
          conversationId,
          sequence: 1,
          participantSetDigest: setDigest,
        })},
        'test exact family audience', ${now}
      )
    `;
    for (const { identityId, personId } of participants) {
      await transaction`
        insert into epoch_participants (
          participant_epoch_id, person_identity_id, person_id,
          registration_status, consented_at, added_at
        ) values (
          ${epochId}, ${identityId}, ${personId}, 'registered', ${now}, ${now}
        )
      `;
      await transaction`
        insert into participant_policies (
          id, conversation_id, person_id, version, status,
          allow_content_processing, allow_direct_responses, allow_proactive_writes,
          retention_seconds, changed_by_person_id, effective_at
        ) values (
          ${randomUUID()}, ${conversationId}, ${personId}, 1, 'active',
          true, true, false, 2592000, ${personId}, ${now}
        )
      `;
    }
    await transaction`
      update conversations set current_epoch_id = ${epochId}
      where id = ${conversationId}
    `;
  });

  return { conversationId, householdId, personIds };
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
