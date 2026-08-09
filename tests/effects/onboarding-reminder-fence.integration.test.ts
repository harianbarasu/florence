import { randomUUID } from "node:crypto";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { EffectOutbox } from "../../src/modules/effects/index.js";
import { SecretBox } from "../../src/shared/crypto.js";

const databaseUrl = process.env.TEST_DATABASE_URL;
const schema = process.env.TEST_POSTGRES_SCHEMA ?? "florence_v4";
const describeDatabase = databaseUrl ? describe : describe.skip;

describeDatabase("onboarding reminder delivery fence", () => {
  let database: Sql<Record<string, never>>;
  const personIds: string[] = [];
  const householdIds: string[] = [];
  const prefixes: string[] = [];
  const secretBox = new SecretBox(
    "test-v1",
    JSON.stringify({ "test-v1": Buffer.alloc(32, 17).toString("base64") }),
  );

  beforeAll(async () => {
    database = postgres(databaseUrl as string, {
      max: 3,
      connection: { search_path: schema, TimeZone: "UTC" },
    });
    const columns = await database<{ readonly present: string | null }[]>`
      select column_name as present from information_schema.columns
      where table_schema = ${schema} and table_name = 'outbox'
        and column_name = 'person_onboarding_version'
    `;
    if (!columns[0]?.present) throw new Error(`Expected onboarding reminder fence in ${schema}`);
  });

  afterEach(async () => {
    for (const prefix of prefixes.splice(0)) {
      const effects = await database<{ id: string; authorization_decision_id: string }[]>`
        select id, authorization_decision_id from outbox
        where idempotency_key like ${`${prefix}%`}
      `;
      if (effects.length > 0) {
        await database`delete from outbox where id = any(${database.array(effects.map((row) => row.id))}::uuid[])`;
        await database`
          delete from disclosure_decisions
          where id = any(${database.array(effects.map((row) => row.authorization_decision_id))}::uuid[])
        `;
      }
    }
    for (const householdId of householdIds.splice(0)) {
      await database`delete from households where id = ${householdId}`;
    }
    for (const personId of personIds.splice(0)) {
      await database`delete from people where id = ${personId}`;
    }
  });

  afterAll(async () => {
    await database.end();
  });

  it("blocks a queued reminder after progress, suppression, or relationship completion", async () => {
    const now = new Date();
    const personId = randomUUID();
    const prefix = `onboarding-reminder-fence:${randomUUID()}`;
    personIds.push(personId);
    prefixes.push(prefix);
    await database`
      insert into people (
        id, status, timezone, consented_at, registered_at, authority_version,
        control_epoch, created_at, updated_at
      ) values (
        ${personId}, 'registered', 'America/Los_Angeles', ${now}, ${now}, 1, 1, ${now}, ${now}
      )
    `;
    await database`
      insert into person_onboarding (
        person_id, reminders_sent, last_progressed_at, version, created_at, updated_at
      ) values (${personId}, 0, ${now}, 1, ${now}, ${now})
    `;
    const outbox = new EffectOutbox(database, secretBox);
    const enqueue = async (suffix: string, version: number) =>
      outbox.authorizeAndEnqueue(
        {
          actorPersonId: personId,
          person: { id: personId, controlEpoch: 1 },
          personOnboarding: { version },
          effectKind: "linq.message",
          idempotencyKey: `${prefix}:${suffix}`,
          data: { stage: suffix },
          policy: { operation: "family_onboarding_reminder" },
          target: { providerChatId: randomUUID(), personId },
          payload: { providerChatId: randomUUID(), text: "Continue setting up Florence." },
          reasonCodes: ["onboarding_incomplete", "bounded_private_reminder"],
          authorizationExpiresAt: new Date(now.getTime() + 60 * 60_000),
        },
        now,
      );

    await enqueue("progress", 1);
    await database`
      update person_onboarding set version = 2, last_progressed_at = ${new Date(now.getTime() + 1_000)}
      where person_id = ${personId}
    `;
    expect(await outbox.claim("reminder-test", 10, new Date(now.getTime() + 2_000))).toEqual([]);
    expect(await outbox.cancelStale(new Date(now.getTime() + 2_000))).toBe(1);

    await enqueue("suppressed", 2);
    await database`
      update person_onboarding set reminders_suppressed_at = ${new Date(now.getTime() + 3_000)}
      where person_id = ${personId}
    `;
    expect(await outbox.claim("reminder-test", 10, new Date(now.getTime() + 4_000))).toEqual([]);
    expect(await outbox.cancelStale(new Date(now.getTime() + 4_000))).toBe(1);

    await database`
      update person_onboarding
      set reminders_suppressed_at = null, version = 3
      where person_id = ${personId}
    `;
    await enqueue("final-submit", 3);
    const [leased] = await outbox.claim("reminder-test", 10, new Date(now.getTime() + 5_000));
    expect(leased).toBeDefined();
    await database`
      update person_onboarding set version = 4 where person_id = ${personId}
    `;
    let submitted = false;
    await expect(
      outbox.reauthorizeForSubmission(
        leased as NonNullable<typeof leased>,
        async () => {
          submitted = true;
          return { receipt: "unexpected" };
        },
        new Date(now.getTime() + 6_000),
      ),
    ).resolves.toEqual({ authorized: false });
    expect(submitted).toBe(false);

    const householdId = randomUUID();
    const membershipId = randomUUID();
    householdIds.push(householdId);
    await database`
      insert into households (
        id, timezone, status, membership_version, control_epoch, created_at, updated_at
      ) values (${householdId}, 'America/Los_Angeles', 'active', 1, 1, ${now}, ${now})
    `;
    await database`
      insert into household_memberships (
        id, household_id, person_id, role, status, consented_at, joined_at,
        version, created_at, updated_at
      ) values (
        ${membershipId}, ${householdId}, ${personId}, 'steward', 'active', ${now}, ${now},
        1, ${now}, ${now}
      )
    `;
    await database`
      update person_onboarding
      set selected_household_id = ${householdId}, reminders_suppressed_at = null, version = 5
      where person_id = ${personId}
    `;
    await enqueue("complete", 5);
    await database`
      insert into membership_onboarding (
        membership_id, completed_by_person_id, completed_membership_version,
        completed_profile_review_version, completed_household_intake_version,
        completed_google_decision, version, completed_at, created_at, updated_at
      ) values (
        ${membershipId}, ${personId}, 1, 1, 1, 'limited', 1, ${new Date(now.getTime() + 7_000)},
        ${now}, ${new Date(now.getTime() + 7_000)}
      )
    `;
    expect(await outbox.claim("reminder-test", 10, new Date(now.getTime() + 8_000))).toEqual([]);
    expect(await outbox.cancelStale(new Date(now.getTime() + 8_000))).toBe(1);
  });
});
