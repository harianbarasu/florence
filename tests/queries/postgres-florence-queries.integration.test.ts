import { randomUUID } from "node:crypto";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { PostgresIdentityRelationships } from "../../src/modules/identity/index.js";
import { PostgresFlorenceQueries } from "../../src/modules/queries/postgres-florence-queries.js";
import { PostgresFamilyOnboarding } from "../../src/modules/relationships/family-onboarding.js";
import { SecretBox } from "../../src/shared/crypto.js";

const databaseUrl = process.env.TEST_DATABASE_URL;
const schema = process.env.TEST_POSTGRES_SCHEMA ?? "florence_v4";
const describeDatabase = databaseUrl ? describe : describe.skip;

describeDatabase("starter family read model", () => {
  let database: Sql<Record<string, never>>;
  let personId: string | null = null;
  const secretBox = new SecretBox(
    "test-v1",
    JSON.stringify({ "test-v1": Buffer.alloc(32, 23).toString("base64") }),
  );

  beforeAll(async () => {
    database = postgres(databaseUrl as string, {
      max: 2,
      connection: { search_path: schema, TimeZone: "UTC" },
    });
    const tables = await database<{ readonly present: string | null }[]>`
      select to_regclass(${`${schema}.household_onboarding_intakes`})::text as present
    `;
    if (!tables[0]?.present) throw new Error(`Expected migrated onboarding tables in ${schema}`);
  });

  afterEach(async () => {
    if (!personId) return;
    await database`delete from households where id in (
      select household_id from household_memberships where person_id = ${personId}
    )`;
    await database`delete from people where id = ${personId}`;
    personId = null;
  });

  afterAll(async () => {
    await database.end();
  });

  it("keeps a just-created family visible before its coordinator intake exists", async () => {
    const now = new Date("2026-08-08T20:00:00.000Z");
    const createdPersonId = randomUUID();
    personId = createdPersonId;
    const sealedName = secretBox.encrypt("Hari", `person-display-name:${createdPersonId}`);
    await database`
      insert into people (
        id, status, timezone, display_name_ciphertext, display_name_key_version,
        consented_at, registered_at, created_at, updated_at
      ) values (
        ${createdPersonId}, 'registered', 'America/Los_Angeles',
        ${Buffer.from(JSON.stringify(sealedName), "utf8")}, ${sealedName.kid},
        ${now}, ${now}, ${now}, ${now}
      )
    `;

    const createdHouseholdId = await database.begin(async (transaction) => {
      const onboarding = new PostgresFamilyOnboarding(secretBox);
      await onboarding.confirmProfile(transaction, {
        actorPersonId: createdPersonId,
        personId: createdPersonId,
        expectedPersonAuthorityVersion: 1,
        expectedProfileReviewVersion: 0,
        confirmedAt: now,
      });
      const created = await new PostgresIdentityRelationships(transaction).createHousehold({
        founderPersonId: createdPersonId,
        timezone: "America/Los_Angeles",
        createdAt: now.toISOString(),
      });
      const beforeSelection = await onboarding.project(transaction, {
        actorPersonId: createdPersonId,
        personId: createdPersonId,
      });
      await onboarding.selectHousehold(transaction, {
        actorPersonId: createdPersonId,
        personId: createdPersonId,
        householdId: created.householdId,
        expectedPersonOnboardingVersion: beforeSelection.profile.onboardingVersion,
        selectedAt: now,
      });

      const intake = await transaction<{ readonly present: boolean }[]>`
        select exists(
          select 1 from household_onboarding_intakes where household_id = ${created.householdId}
        ) as present
      `;
      expect(intake[0]?.present).toBe(false);
      const projection = await onboarding.project(transaction, {
        actorPersonId: createdPersonId,
        personId: createdPersonId,
      });
      expect(projection.nextStep).toMatchObject({
        kind: "coordinator",
        householdId: created.householdId,
      });
      return created.householdId;
    });

    const people = await new PostgresFlorenceQueries(database, secretBox).people(createdPersonId);
    expect(people.households).toEqual([
      expect.objectContaining({
        id: createdHouseholdId,
        rosterVersion: 1,
        intakeVersion: 0,
        members: [expect.objectContaining({ id: createdPersonId, self: true })],
      }),
    ]);
  });
});
