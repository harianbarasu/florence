import { createHash, randomUUID } from "node:crypto";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { StewardCapabilities } from "../../src/modules/identity/contracts.js";
import { PostgresIdentityRelationships } from "../../src/modules/identity/postgres-identity-relationships.js";
import { PostgresFlorenceQueries } from "../../src/modules/queries/postgres-florence-queries.js";
import {
  type FamilyOnboardingTransaction,
  PostgresFamilyOnboarding,
} from "../../src/modules/relationships/family-onboarding.js";
import { HouseholdOnboarding } from "../../src/modules/relationships/household-onboarding.js";
import { SecretBox } from "../../src/shared/crypto.js";

const databaseUrl = process.env.TEST_DATABASE_URL;
const schema = process.env.TEST_POSTGRES_SCHEMA ?? "florence_v4";
const describeDatabase = databaseUrl ? describe : describe.skip;

const now = new Date("2026-08-09T18:00:00.000Z");
const secretBox = new SecretBox(
  "test-v1",
  JSON.stringify({ "test-v1": Buffer.alloc(32, 29).toString("base64") }),
);

interface AdultFixture {
  readonly founderPersonId: string;
  readonly founderMembershipId: string;
  readonly householdId: string;
  readonly conversationId: string;
  readonly participantEpochId: string;
  readonly participantDigest: string;
  readonly adults: readonly {
    readonly personId: string;
    readonly identityId: string;
    readonly subjectDigest: string;
  }[];
}

describeDatabase("family onboarding adult invitation state", () => {
  let database: Sql<Record<string, never>>;
  const fixtures: AdultFixture[] = [];

  beforeAll(async () => {
    database = postgres(databaseUrl as string, {
      max: 3,
      connection: { search_path: schema, TimeZone: "UTC" },
    });
    const tables = await database<{ readonly present: string | null }[]>`
      select to_regclass(${`${schema}.household_onboarding_adult_intents`})::text as present
    `;
    if (!tables[0]?.present) throw new Error(`Expected plural adult onboarding tables in ${schema}`);
  });

  afterEach(async () => {
    for (const fixture of fixtures.splice(0)) {
      await database`
        delete from invitation_approvals
        where invitation_id in (select id from invitations where household_id = ${fixture.householdId})
      `;
      await database`delete from households where id = ${fixture.householdId}`;
      await database`delete from conversations where id = ${fixture.conversationId}`;
      const personIds = [fixture.founderPersonId, ...fixture.adults.map((adult) => adult.personId)];
      await database`delete from person_identities where person_id = any(${database.array(personIds)}::uuid[])`;
      await database`delete from people where id = any(${database.array(personIds)}::uuid[])`;
    }
  });

  afterAll(async () => {
    await database.end();
  });

  it("keeps a second exact adult invitation independently actionable after the first adult joins", async () => {
    const fixture = await seedFixture(database, 2);
    fixtures.push(fixture);
    const firstAdult = adultAt(fixture, 0);
    const secondAdult = adultAt(fixture, 1);
    const firstIntentId = await insertIntent(database, fixture, 0, "Kendall", "steward");
    const secondIntentId = await insertIntent(database, fixture, 1, "Jenny", "caregiver");
    const firstInvitationId = await insertInvitation(database, fixture, 0, "Kendall", "steward");
    const secondInvitationId = await insertInvitation(database, fixture, 1, "Jenny", "caregiver");
    await bindIntent(database, firstIntentId, firstAdult.personId, firstInvitationId);
    await bindIntent(database, secondIntentId, secondAdult.personId, secondInvitationId);

    await new PostgresIdentityRelationships(database).acceptInvitation({
      invitationId: firstInvitationId,
      inviteePersonId: firstAdult.personId,
      tokenDigest: hexDigest(`token:${firstInvitationId}`),
      acceptedAt: now.toISOString(),
    });

    const projection = await project(database, fixture);
    expect(projection.household?.adults).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: firstIntentId, status: "joined" }),
        expect.objectContaining({ id: secondIntentId, status: "invited" }),
      ]),
    );
    const secondInvitation = await database<{ readonly household_membership_version: number | string }[]>`
      select household_membership_version from invitations where id = ${secondInvitationId}
    `;
    expect(Number(secondInvitation[0]?.household_membership_version)).toBe(2);

    await new PostgresIdentityRelationships(database).acceptInvitation({
      invitationId: secondInvitationId,
      inviteePersonId: secondAdult.personId,
      tokenDigest: hexDigest(`token:${secondInvitationId}`),
      acceptedAt: new Date(now.getTime() + 1_000).toISOString(),
    });
    const afterBothAccepted = await project(database, fixture);
    expect(afterBothAccepted.household?.adults).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: firstIntentId, status: "joined" }),
        expect.objectContaining({ id: secondIntentId, status: "joined" }),
      ]),
    );
  });

  it("version-fences a declined wrong-person binding and rebinds the exact replacement", async () => {
    const fixture = await seedFixture(database, 2);
    fixtures.push(fixture);
    const firstAdult = adultAt(fixture, 0);
    const secondAdult = adultAt(fixture, 1);
    const intentId = await insertIntent(database, fixture, 0, "Kendall", "steward");
    const declinedInvitationId = await insertInvitation(database, fixture, 0, "Kendall", "steward");
    await database`update invitations set status = 'declined', updated_at = ${now} where id = ${declinedInvitationId}`;
    await bindIntent(database, intentId, firstAdult.personId, declinedInvitationId);
    const onboarding = new PostgresFamilyOnboarding(secretBox);

    await expect(
      database.begin(async (transaction) => {
        await inviteReplacement(transaction, fixture, secondAdult);
        await onboarding.prepareAdultIntentInvitation(transaction, {
          actorPersonId: fixture.founderPersonId,
          personId: fixture.founderPersonId,
          householdId: fixture.householdId,
          expectedMembershipVersion: 1,
          adultIntentId: intentId,
          expectedIntentVersion: 1,
          proposedDisplayName: "Kendall",
          role: "steward",
          matchedPersonId: secondAdult.personId,
          preparedAt: now,
        });
      }),
    ).rejects.toThrow("changed before the invitation was prepared");
    const rolledBack = await database<{ readonly pending: boolean }[]>`
      select exists(
        select 1 from invitations
        where household_id = ${fixture.householdId}
          and invitee_identity_id = ${secondAdult.identityId}
          and status = 'pending'
      ) as pending
    `;
    expect(rolledBack[0]?.pending).toBe(false);

    await database.begin(async (transaction) => {
      const invitation = await inviteReplacement(transaction, fixture, secondAdult);
      const prepared = await onboarding.prepareAdultIntentInvitation(transaction, {
        actorPersonId: fixture.founderPersonId,
        personId: fixture.founderPersonId,
        householdId: fixture.householdId,
        expectedMembershipVersion: 1,
        adultIntentId: intentId,
        expectedIntentVersion: 2,
        proposedDisplayName: "Kendall",
        role: "steward",
        matchedPersonId: secondAdult.personId,
        preparedAt: now,
      });
      await onboarding.bindAdultIntentInvitation(transaction, {
        actorPersonId: fixture.founderPersonId,
        personId: fixture.founderPersonId,
        householdId: fixture.householdId,
        expectedMembershipVersion: 1,
        adultIntentId: intentId,
        expectedIntentVersion: prepared.intentVersion,
        matchedPersonId: secondAdult.personId,
        invitationId: invitation.invitation.invitationId,
        boundAt: now,
      });
    });

    const projection = await project(database, fixture);
    expect(projection.household?.adults).toEqual([
      expect.objectContaining({
        id: intentId,
        matchedPersonId: secondAdult.personId,
        status: "invited",
        version: 4,
      }),
    ]);
  });

  it("atomically revokes a stale pending binding before it can be replaced", async () => {
    const fixture = await seedFixture(database, 2);
    fixtures.push(fixture);
    const firstAdult = adultAt(fixture, 0);
    const secondAdult = adultAt(fixture, 1);
    const intentId = await insertIntent(database, fixture, 0, "Kendall", "steward");
    const staleInvitationId = await insertInvitation(database, fixture, 0, "Kendall", "steward", 1);
    await bindIntent(database, intentId, firstAdult.personId, staleInvitationId);
    await database`update households set membership_version = 2 where id = ${fixture.householdId}`;
    const onboarding = new PostgresFamilyOnboarding(secretBox);

    await database.begin(async (transaction) => {
      const prepared = await onboarding.prepareAdultIntentInvitation(transaction, {
        actorPersonId: fixture.founderPersonId,
        personId: fixture.founderPersonId,
        householdId: fixture.householdId,
        expectedMembershipVersion: 1,
        adultIntentId: intentId,
        expectedIntentVersion: 2,
        proposedDisplayName: "Kendall",
        role: "steward",
        matchedPersonId: secondAdult.personId,
        preparedAt: now,
      });
      expect(prepared.intentVersion).toBe(3);
      const rows = await transaction<{ readonly status: string }[]>`
        select status from invitations where id = ${staleInvitationId}
      `;
      expect(rows[0]?.status).toBe("revoked");
    });
  });

  it("does not treat an accepted invitation as joined when the active membership role differs", async () => {
    const fixture = await seedFixture(database, 1);
    fixtures.push(fixture);
    const adult = adultAt(fixture, 0);
    const intentId = await insertIntent(database, fixture, 0, "Kendall", "steward");
    const invitationId = await insertInvitation(database, fixture, 0, "Kendall", "steward");
    await bindIntent(database, intentId, adult.personId, invitationId);
    await database`
      update invitations
      set status = 'accepted', accepted_by_person_id = ${adult.personId},
        accepted_at = ${now}, updated_at = ${now}
      where id = ${invitationId}
    `;
    await database`
      insert into household_memberships (
        id, household_id, person_id, role, status, consented_at, joined_at,
        version, created_at, updated_at
      ) values (
        ${randomUUID()}, ${fixture.householdId}, ${adult.personId},
        'caregiver', 'active', ${now}, ${now}, 1, ${now}, ${now}
      )
    `;

    const projection = await project(database, fixture);
    expect(projection.household?.adults).toEqual([
      expect.objectContaining({ id: intentId, status: "not_invited" }),
    ]);
  });

  it("reopens a supporting adult's private review when shared intake appears", async () => {
    const fixture = await seedFixture(database, 2);
    fixtures.push(fixture);
    const caregiver = adultAt(fixture, 0);
    const representedChild = adultAt(fixture, 1);
    const membershipId = randomUUID();
    const caregiverName = secretBox.encrypt("Jenny", `person-display-name:${caregiver.personId}`);
    await database.begin(async (transaction) => {
      await transaction`delete from household_onboarding_intakes where household_id = ${fixture.householdId}`;
      await transaction`
        update people
        set display_name_ciphertext = ${sealed(caregiverName)},
          display_name_key_version = ${caregiverName.kid},
          timezone = 'America/Los_Angeles', google_activation_suppressed_at = ${now},
          updated_at = ${now}
        where id = ${caregiver.personId}
      `;
      await transaction`
        insert into household_memberships (
          id, household_id, person_id, role, status, consented_at, joined_at,
          version, created_at, updated_at
        ) values (
          ${membershipId}, ${fixture.householdId}, ${caregiver.personId}, 'caregiver', 'active',
          ${now}, ${now}, 1, ${now}, ${now}
        )
      `;
      await transaction`
        insert into membership_capabilities (membership_id, capability, scope, status, granted_at)
        values (${membershipId}, 'household.read', '{}'::jsonb, 'active', ${now})
      `;
      await transaction`
        insert into household_memberships (
          id, household_id, person_id, role, status, joined_at, version, created_at, updated_at
        ) values (
          ${randomUUID()}, ${fixture.householdId}, ${representedChild.personId}, 'dependent', 'active',
          ${now}, 1, ${now}, ${now}
        )
      `;
      await transaction`
        insert into person_onboarding (
          person_id, profile_reviewed_by_person_id, reviewed_person_authority_version,
          profile_review_version, profile_reviewed_at, selected_household_id,
          last_progressed_at, version, created_at, updated_at
        ) values (
          ${caregiver.personId}, ${caregiver.personId}, 1, 1, ${now}, ${fixture.householdId},
          ${now}, 1, ${now}, ${now}
        )
      `;
      await transaction`
        insert into membership_onboarding (
          membership_id, completed_by_person_id, completed_membership_version,
          completed_profile_review_version, completed_household_intake_version,
          completed_google_decision, completed_at, version, created_at, updated_at
        ) values (
          ${membershipId}, ${caregiver.personId}, 1, 1, 0, 'limited', ${now}, 1, ${now}, ${now}
        )
      `;
    });

    const onboarding = new PostgresFamilyOnboarding(secretBox);
    const beforeSharedIntake = await database.begin((transaction) =>
      onboarding.project(transaction, {
        actorPersonId: caregiver.personId,
        personId: caregiver.personId,
      }),
    );
    expect(beforeSharedIntake.nextStep.kind).toBe("complete");

    await database`
      insert into household_onboarding_intakes (
        household_id, adult_roster_reviewed_by_person_id, adult_roster_reviewed_at,
        child_roster_reviewed_by_person_id, child_roster_reviewed_at,
        child_roster_household_membership_version, version, created_at, updated_at
      ) values (
        ${fixture.householdId}, ${fixture.founderPersonId}, ${now},
        ${fixture.founderPersonId}, ${now}, 1, 1, ${now}, ${now}
      )
    `;

    const staleCompletion = await database.begin((transaction) =>
      onboarding.project(transaction, {
        actorPersonId: caregiver.personId,
        personId: caregiver.personId,
      }),
    );
    expect(staleCompletion.household?.completed).toBe(false);
    expect(staleCompletion.nextStep.kind).toBe("review_shared_context");
    const privatePeopleBeforeReview = await new PostgresFlorenceQueries(database, secretBox).people(
      caregiver.personId,
    );
    expect(
      privatePeopleBeforeReview.households[0]?.members.some(
        (member) => member.id === representedChild.personId,
      ),
    ).toBe(false);
    const currentBeforeReview = await database<{ readonly current: boolean }[]>`
      select family_membership_onboarding_is_current(${membershipId}) as current
    `;
    expect(currentBeforeReview[0]?.current).toBe(false);

    await database.begin(async (transaction) => {
      await onboarding.reviewSharedContext(transaction, {
        actorPersonId: caregiver.personId,
        personId: caregiver.personId,
        householdId: fixture.householdId,
        expectedMembershipVersion: 1,
        expectedIntakeVersion: 1,
        expectedMembershipOnboardingVersion: 1,
        reviewedAt: new Date(now.getTime() + 1_000),
      });
      await onboarding.completeMembership(transaction, {
        actorPersonId: caregiver.personId,
        personId: caregiver.personId,
        householdId: fixture.householdId,
        expectedMembershipVersion: 1,
        expectedProfileReviewVersion: 1,
        expectedIntakeVersion: 1,
        expectedMembershipOnboardingVersion: 2,
        completedAt: new Date(now.getTime() + 2_000),
      });
    });

    const currentAfterReview = await database<{ readonly current: boolean }[]>`
      select family_membership_onboarding_is_current(${membershipId}) as current
    `;
    expect(currentAfterReview[0]?.current).toBe(true);
    const privatePeopleAfterReview = await new PostgresFlorenceQueries(database, secretBox).people(
      caregiver.personId,
    );
    expect(
      privatePeopleAfterReview.households[0]?.members.some(
        (member) => member.id === representedChild.personId,
      ),
    ).toBe(true);
  });
});

async function seedFixture(database: Sql<Record<string, never>>, adultCount: number): Promise<AdultFixture> {
  const founderPersonId = randomUUID();
  const founderIdentityId = randomUUID();
  const founderMembershipId = randomUUID();
  const householdId = randomUUID();
  const conversationId = randomUUID();
  const participantEpochId = randomUUID();
  const participantDigest = hexDigest(`participants:${conversationId}`);
  const adults = Array.from({ length: adultCount }, () => ({
    personId: randomUUID(),
    identityId: randomUUID(),
    subjectDigest: hexDigest(`adult:${randomUUID()}`),
  }));
  const founderName = secretBox.encrypt("Hari", `person-display-name:${founderPersonId}`);
  await database.begin(async (transaction) => {
    await transaction`
      insert into people (
        id, status, display_name_ciphertext, display_name_key_version, timezone,
        consented_at, registered_at, created_at, updated_at
      ) values (
        ${founderPersonId}, 'registered', ${sealed(founderName)}, ${founderName.kid},
        'America/Los_Angeles', ${now}, ${now}, ${now}, ${now}
      )
    `;
    for (const adult of adults) {
      await transaction`
        insert into people (id, status, consented_at, registered_at, created_at, updated_at)
        values (${adult.personId}, 'registered', ${now}, ${now}, ${now}, ${now})
      `;
    }
    await transaction`
      insert into person_identities (
        id, person_id, kind, issuer, subject_digest, status, observed_at,
        verified_at, created_at, updated_at
      ) values (
        ${founderIdentityId}, ${founderPersonId}, 'phone', 'linq',
        ${hexDigest(`founder:${founderPersonId}`)}, 'verified', ${now}, ${now}, ${now}, ${now}
      )
    `;
    for (const adult of adults) {
      await transaction`
        insert into person_identities (
          id, person_id, kind, issuer, subject_digest, status, observed_at,
          verified_at, created_at, updated_at
        ) values (
          ${adult.identityId}, ${adult.personId}, 'phone', 'linq', ${adult.subjectDigest},
          'verified', ${now}, ${now}, ${now}, ${now}
        )
      `;
    }
    await transaction`
      insert into households (
        id, timezone, status, membership_version, control_epoch, created_at, updated_at
      ) values (${householdId}, 'America/Los_Angeles', 'onboarding', 1, 1, ${now}, ${now})
    `;
    await transaction`
      insert into household_memberships (
        id, household_id, person_id, role, status, consented_at, joined_at,
        version, created_at, updated_at
      ) values (
        ${founderMembershipId}, ${householdId}, ${founderPersonId}, 'steward', 'active',
        ${now}, ${now}, 1, ${now}, ${now}
      )
    `;
    for (const capability of StewardCapabilities) {
      await transaction`
        insert into membership_capabilities (
          membership_id, capability, scope, status, granted_at
        ) values (${founderMembershipId}, ${capability}, '{}'::jsonb, 'active', ${now})
      `;
    }
    await transaction`
      insert into person_onboarding (
        person_id, profile_reviewed_by_person_id, reviewed_person_authority_version,
        profile_review_version, profile_reviewed_at, selected_household_id,
        last_progressed_at, version, created_at, updated_at
      ) values (
        ${founderPersonId}, ${founderPersonId}, 1, 1, ${now}, ${householdId},
        ${now}, 1, ${now}, ${now}
      )
    `;
    await transaction`
      insert into household_onboarding_intakes (
        household_id, adult_roster_reviewed_by_person_id, adult_roster_reviewed_at,
        version, created_at, updated_at
      ) values (${householdId}, ${founderPersonId}, ${now}, 1, ${now}, ${now})
    `;
    await transaction`
      insert into conversations (
        id, household_id, kind, purpose, status, created_at, updated_at
      ) values (${conversationId}, null, 'group', 'family introduction', 'active', ${now}, ${now})
    `;
    await transaction`
      insert into participant_epochs (
        id, conversation_id, sequence, participant_set_digest, authority_digest,
        change_reason, started_at
      ) values (
        ${participantEpochId}, ${conversationId}, 1, ${participantDigest},
        ${hexDigest(`authority:${conversationId}`)}, 'test', ${now}
      )
    `;
    await transaction`update conversations set current_epoch_id = ${participantEpochId} where id = ${conversationId}`;
    await transaction`
      insert into epoch_participants (
        participant_epoch_id, person_identity_id, person_id, registration_status,
        consented_at, added_at
      ) values (
        ${participantEpochId}, ${founderIdentityId}, ${founderPersonId}, 'registered', ${now}, ${now}
      )
    `;
    for (const adult of adults) {
      await transaction`
        insert into epoch_participants (
          participant_epoch_id, person_identity_id, person_id, registration_status,
          consented_at, added_at
        ) values (
          ${participantEpochId}, ${adult.identityId}, ${adult.personId}, 'registered', ${now}, ${now}
        )
      `;
    }
  });
  return {
    founderPersonId,
    founderMembershipId,
    householdId,
    conversationId,
    participantEpochId,
    participantDigest,
    adults,
  };
}

async function insertIntent(
  database: Sql<Record<string, never>>,
  fixture: AdultFixture,
  adultIndex: number,
  displayName: string,
  role: "steward" | "caregiver",
): Promise<string> {
  const id = randomUUID();
  const name = secretBox.encrypt(displayName, `household-onboarding-adult-intent-name:${id}`);
  await database`
    insert into household_onboarding_adult_intents (
      id, household_id, display_name_ciphertext, display_name_key_version,
      role, recorded_by_person_id, version, created_at, updated_at
    ) values (
      ${id}, ${fixture.householdId}, ${sealed(name)}, ${name.kid}, ${role},
      ${fixture.founderPersonId}, 1, ${new Date(now.getTime() + adultIndex)},
      ${new Date(now.getTime() + adultIndex)}
    )
  `;
  return id;
}

async function insertInvitation(
  database: Sql<Record<string, never>>,
  fixture: AdultFixture,
  adultIndex: number,
  displayName: string,
  role: "steward" | "caregiver",
  membershipVersion = 1,
): Promise<string> {
  const adult = adultAt(fixture, adultIndex);
  const id = randomUUID();
  const name = secretBox.encrypt(displayName, `invitation-proposed-display-name:${id}`);
  await database`
    insert into invitations (
      id, household_id, invited_by_membership_id, invitee_identity_id,
      invitee_subject_digest, token_digest, requested_role, requested_capabilities,
      household_membership_version, status, expires_at,
      source_conversation_id, source_participant_epoch_id, source_participant_digest,
      proposed_display_name_ciphertext, proposed_display_name_key_version,
      created_at, updated_at
    ) values (
      ${id}, ${fixture.householdId}, ${fixture.founderMembershipId}, ${adult.identityId},
      ${adult.subjectDigest}, ${hexDigest(`token:${id}`)}, ${role},
      ${role === "steward" ? ["household.read", "household.govern"] : ["household.read"]},
      ${membershipVersion}, 'pending', ${new Date(now.getTime() + 86_400_000)},
      ${fixture.conversationId}, ${fixture.participantEpochId}, ${fixture.participantDigest},
      ${sealed(name)}, ${name.kid}, ${now}, ${now}
    )
  `;
  return id;
}

async function bindIntent(
  database: Sql<Record<string, never>>,
  intentId: string,
  personId: string,
  invitationId: string,
): Promise<void> {
  await database`
    update household_onboarding_adult_intents
    set matched_person_id = ${personId}, invitation_id = ${invitationId}, version = 2
    where id = ${intentId}
  `;
}

async function project(database: Sql<Record<string, never>>, fixture: AdultFixture) {
  return database.begin((transaction) =>
    new PostgresFamilyOnboarding(secretBox).project(transaction, {
      actorPersonId: fixture.founderPersonId,
      personId: fixture.founderPersonId,
    }),
  );
}

function sealed(value: ReturnType<SecretBox["encrypt"]>): Buffer {
  return Buffer.from(JSON.stringify(value), "utf8");
}

function hexDigest(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function adultAt(fixture: AdultFixture, index: number): AdultFixture["adults"][number] {
  const adult = fixture.adults[index];
  if (!adult) throw new Error(`Missing adult fixture ${index}`);
  return adult;
}

async function inviteReplacement(
  transaction: FamilyOnboardingTransaction,
  fixture: AdultFixture,
  adult: AdultFixture["adults"][number],
): Promise<Awaited<ReturnType<HouseholdOnboarding["inviteCurrentParticipant"]>>> {
  return new HouseholdOnboarding(transaction, secretBox).inviteCurrentParticipant({
    actorPersonId: fixture.founderPersonId,
    householdId: fixture.householdId,
    conversationId: fixture.conversationId,
    expectedParticipantEpochId: fixture.participantEpochId,
    expectedParticipantDigest: fixture.participantDigest,
    inviteeIdentityId: adult.identityId,
    inviteePersonId: adult.personId,
    proposedDisplayName: "Kendall",
    role: "steward",
    createdAt: now,
  });
}
