import { randomUUID } from "node:crypto";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  PostgresRoutines,
  planCoverageOpeningTimer,
  type RoutineRevisionDraft,
} from "../../src/modules/coordination/index.js";
import { DurableTimers } from "../../src/modules/work/index.js";
import { canonicalDigest } from "../../src/shared/canonical-json.js";
import { SecretBox } from "../../src/shared/crypto.js";
import { ConflictError, UnauthorizedError } from "../../src/shared/errors.js";

const databaseUrl = process.env.TEST_DATABASE_URL;
const schema = process.env.TEST_POSTGRES_SCHEMA ?? "florence_v4";
const describeDatabase = databaseUrl ? describe : describe.skip;

interface Fixture {
  readonly actorId: string;
  readonly actorMembershipId: string;
  readonly holderId: string;
  readonly householdId: string;
  readonly conversationId: string;
  readonly epochId: string;
  readonly participantSetDigest: string;
}

describeDatabase("Postgres routines", () => {
  let database: Sql<Record<string, never>>;
  let fixture: Fixture | null = null;
  const secretBox = new SecretBox(
    "test-v1",
    JSON.stringify({
      "test-v1": Buffer.alloc(32, 7).toString("base64"),
    }),
  );

  beforeAll(async () => {
    database = postgres(databaseUrl as string, {
      max: 2,
      connection: { search_path: schema, TimeZone: "UTC" },
    });
    const tables = await database<{ readonly present: string | null }[]>`
      select to_regclass(${`${schema}.routines`})::text as present
    `;
    if (!tables[0]?.present) throw new Error(`Expected migrated routine tables in ${schema}`);
  });

  afterEach(async () => {
    if (!fixture) return;
    await database`delete from coverage_loops where household_id = ${fixture.householdId}`;
    await database`delete from routines where household_id = ${fixture.householdId}`;
    await database`delete from households where id = ${fixture.householdId}`;
    await database`delete from person_identities where person_id in (${fixture.actorId}, ${fixture.holderId})`;
    await database`delete from people where id in (${fixture.actorId}, ${fixture.holderId})`;
    fixture = null;
  });

  afterAll(async () => {
    await database.end();
  });

  it("supersedes future work on revision and materializes the replacement idempotently", async () => {
    fixture = await seedFixture(database);
    const routines = new PostgresRoutines(database, secretBox);
    const createdAt = new Date("2026-08-05T16:00:00.000Z");
    const routineId = randomUUID();
    const draft = routineDraft(fixture);
    const created = await routines.save({
      kind: "create",
      routineId,
      householdId: fixture.householdId,
      actorPersonId: fixture.actorId,
      occurredAt: createdAt,
      revision: draft,
    });
    const firstMaterialization = await routines.materializeDue({
      fromLocalDate: "2026-08-05",
      throughLocalDate: "2026-08-05",
      materializedAt: new Date("2026-08-05T17:00:00.000Z"),
    });
    const oldCoverage = firstMaterialization.coverage.find(
      (entry) => entry.occurrence.routineId === routineId,
    );
    if (!oldCoverage) throw new Error("Expected original routine coverage");

    const revised = await routines.save({
      kind: "revise",
      routineId,
      householdId: fixture.householdId,
      expectedVersion: created.routine.version,
      actorPersonId: fixture.actorId,
      occurredAt: new Date("2026-08-05T18:00:00.000Z"),
      revision: { ...draft, title: "Updated Wednesday pickup" },
    });
    expect(revised.routine).toMatchObject({ version: 2, currentRevision: 2 });
    const replaced = await database<
      {
        readonly occurrence_status: string;
        readonly occurrence_version: number;
        readonly loop_state: string;
        readonly loop_version: number;
      }[]
    >`
      select occurrence.status as occurrence_status, occurrence.version::int as occurrence_version,
        loop.state as loop_state, loop.version::int as loop_version
      from routine_occurrences occurrence
      join coverage_loops loop on loop.routine_occurrence_id = occurrence.id
      where occurrence.id = ${oldCoverage.occurrence.occurrenceId}
    `;
    expect(replaced[0]).toEqual({
      occurrence_status: "cancelled",
      occurrence_version: 2,
      loop_state: "superseded",
      loop_version: 2,
    });

    const input = {
      fromLocalDate: "2026-08-05",
      throughLocalDate: "2026-08-05",
      materializedAt: new Date("2026-08-05T18:05:00.000Z"),
    } as const;
    const first = await routines.materializeDue(input);
    const replay = await routines.materializeDue(input);
    const firstCoverage = first.coverage.filter((entry) => entry.occurrence.routineId === routineId);
    const replayCoverage = replay.coverage.filter((entry) => entry.occurrence.routineId === routineId);

    expect(firstCoverage).toHaveLength(1);
    expect(firstCoverage[0]).toMatchObject({
      occurrenceCreated: true,
      loopCreated: true,
      occurrence: { routineRevision: 2 },
      loop: { state: "awaiting_response" },
    });
    expect(replayCoverage[0]).toMatchObject({
      occurrenceCreated: false,
      loopCreated: false,
      occurrence: { occurrenceId: firstCoverage[0]?.occurrence.occurrenceId },
      loop: { loopId: firstCoverage[0]?.loop.loopId },
    });
    const counts = await database<{ readonly occurrences: number; readonly loops: number }[]>`
      select
        (select count(*)::int from routine_occurrences where routine_id = ${routineId}) as occurrences,
        (select count(*)::int from coverage_loops where routine_id = ${routineId}) as loops
    `;
    expect(counts[0]).toEqual({ occurrences: 2, loops: 2 });
    const encrypted = await database<
      { readonly revision_content: Buffer; readonly occurrence_content: Buffer }[]
    >`
      select revision.content_ciphertext as revision_content,
        occurrence.content_ciphertext as occurrence_content
      from routine_revisions revision
      join routine_occurrences occurrence on occurrence.routine_id = revision.routine_id
        and occurrence.routine_revision = revision.revision
      where revision.routine_id = ${routineId} and revision.revision = 2
    `;
    expect(encrypted[0]?.revision_content.includes(Buffer.from("Updated Wednesday pickup"))).toBe(false);
    expect(encrypted[0]?.occurrence_content.includes(Buffer.from("Updated Wednesday pickup"))).toBe(false);
  });

  it("auto-covers only when the exact holder creates the standing authorization", async () => {
    fixture = await seedFixture(database);
    const routines = new PostgresRoutines(database, secretBox);
    const occurredAt = new Date("2026-08-05T16:00:00.000Z");
    const authorization = {
      holderPersonId: fixture.holderId,
      authorizedByPersonId: fixture.holderId,
      authorizationKind: "created" as const,
      authorizedAt: occurredAt.toISOString(),
    };
    await expect(
      routines.save({
        kind: "create",
        routineId: randomUUID(),
        householdId: fixture.householdId,
        actorPersonId: fixture.actorId,
        occurredAt,
        revision: {
          ...routineDraft(fixture),
          proposedHolderPersonId: fixture.holderId,
          standingCoverage: authorization,
        },
      }),
    ).rejects.toThrow(UnauthorizedError);

    const routineId = randomUUID();
    await routines.save({
      kind: "create",
      routineId,
      householdId: fixture.householdId,
      actorPersonId: fixture.holderId,
      occurredAt,
      revision: {
        ...routineDraft(fixture),
        proposedHolderPersonId: fixture.holderId,
        standingCoverage: authorization,
      },
    });
    const materialized = await routines.materializeDue({
      fromLocalDate: "2026-08-05",
      throughLocalDate: "2026-08-05",
      materializedAt: new Date("2026-08-05T17:00:00.000Z"),
    });
    const coverage = materialized.coverage.find((entry) => entry.occurrence.routineId === routineId);
    expect(coverage?.loop).toMatchObject({
      state: "covered",
      acknowledgment: {
        personId: fixture.holderId,
        kind: "standing_routine_self_authorized",
      },
    });
  });

  it("pauses, resumes, and terminally retires only future routine work", async () => {
    fixture = await seedFixture(database);
    const routines = new PostgresRoutines(database, secretBox);
    const routineId = randomUUID();
    const created = await routines.save({
      kind: "create",
      routineId,
      householdId: fixture.householdId,
      actorPersonId: fixture.actorId,
      occurredAt: new Date("2026-08-05T16:00:00.000Z"),
      revision: routineDraft(fixture),
    });
    const materialized = await routines.materializeDue({
      fromLocalDate: "2026-08-05",
      throughLocalDate: "2026-08-12",
      materializedAt: new Date("2026-08-05T17:00:00.000Z"),
    });
    const initialCoverage = materialized.coverage
      .filter((entry) => entry.occurrence.routineId === routineId)
      .sort((left, right) => left.occurrence.localDate.localeCompare(right.occurrence.localDate));
    expect(initialCoverage).toHaveLength(2);
    const future = initialCoverage[1];
    if (!future) throw new Error("Expected future routine coverage");
    await scheduleOpeningTimer(database, fixture, future.loop);

    await database`
      delete from membership_capabilities
      where membership_id = ${fixture.actorMembershipId}
        and capability = 'coordination.coordinate'
    `;
    const pauseAt = new Date("2026-08-05T20:00:00.000Z");
    await expect(
      routines.setStatus({
        routineId,
        householdId: fixture.householdId,
        actorPersonId: fixture.actorId,
        expectedVersion: created.routine.version,
        status: "paused",
        occurredAt: pauseAt,
      }),
    ).rejects.toThrow(UnauthorizedError);
    await database`
      insert into membership_capabilities (
        membership_id, capability, status, granted_by_membership_id, granted_at
      ) values (
        ${fixture.actorMembershipId}, 'coordination.coordinate', 'active',
        ${fixture.actorMembershipId}, ${pauseAt}
      )
    `;

    const paused = await routines.setStatus({
      routineId,
      householdId: fixture.householdId,
      actorPersonId: fixture.actorId,
      expectedVersion: created.routine.version,
      status: "paused",
      occurredAt: pauseAt,
    });
    expect(paused).toMatchObject({
      duplicate: false,
      affectedOccurrenceCount: 1,
      routine: { status: "paused", version: 2 },
      transitionedLoopIds: [future.loop.loopId],
    });
    await expectOccurrenceStates(database, routineId, [
      { local_date: "2026-08-05", status: "materialized", version: 1 },
      { local_date: "2026-08-12", status: "skipped", version: 2 },
    ]);
    await expectTimerStatus(database, future.loop.loopId, "superseded");

    const resumed = await routines.setStatus({
      routineId,
      householdId: fixture.householdId,
      actorPersonId: fixture.actorId,
      expectedVersion: paused.routine.version,
      status: "active",
      occurredAt: new Date("2026-08-05T20:05:00.000Z"),
    });
    expect(resumed).toMatchObject({
      affectedOccurrenceCount: 1,
      routine: { status: "active", version: 3 },
    });
    expect(resumed.coverage).toHaveLength(1);
    const resumedFuture = resumed.coverage[0];
    if (!resumedFuture) throw new Error("Expected resumed future coverage");
    expect(resumedFuture).toMatchObject({
      occurrence: { localDate: "2026-08-12", status: "materialized", version: 3 },
      loop: { state: "awaiting_response", version: 1 },
      occurrenceCreated: false,
      loopCreated: true,
    });
    await scheduleOpeningTimer(database, fixture, resumedFuture.loop);

    const retired = await routines.setStatus({
      routineId,
      householdId: fixture.householdId,
      actorPersonId: fixture.actorId,
      expectedVersion: resumed.routine.version,
      status: "retired",
      occurredAt: new Date("2026-08-05T20:10:00.000Z"),
    });
    expect(retired).toMatchObject({
      affectedOccurrenceCount: 1,
      routine: { status: "retired", version: 4 },
      transitionedLoopIds: [resumedFuture.loop.loopId],
    });
    await expectOccurrenceStates(database, routineId, [
      { local_date: "2026-08-05", status: "materialized", version: 1 },
      { local_date: "2026-08-12", status: "cancelled", version: 4 },
    ]);
    await expectTimerStatus(database, resumedFuture.loop.loopId, "superseded");
    await expect(
      routines.setStatus({
        routineId,
        householdId: fixture.householdId,
        actorPersonId: fixture.actorId,
        expectedVersion: retired.routine.version,
        status: "active",
        occurredAt: new Date("2026-08-05T20:15:00.000Z"),
      }),
    ).rejects.toThrow(ConflictError);
  });
});

async function scheduleOpeningTimer(
  database: Sql<Record<string, never>>,
  fixture: Fixture,
  loop: Parameters<typeof planCoverageOpeningTimer>[0]["loop"],
): Promise<void> {
  const timer = planCoverageOpeningTimer({ loop, openingAuthorized: true });
  if (!timer) throw new Error("Expected an opening timer");
  await new DurableTimers(database).scheduleCoverage({
    timer,
    household: { id: fixture.householdId, controlEpoch: 1 },
    conversation: { id: fixture.conversationId, authorityVersion: 1 },
  });
}

async function expectOccurrenceStates(
  database: Sql<Record<string, never>>,
  routineId: string,
  expected: readonly { readonly local_date: string; readonly status: string; readonly version: number }[],
): Promise<void> {
  const rows = await database<
    { readonly local_date: string; readonly status: string; readonly version: number }[]
  >`
    select local_date::text, status, version::int
    from routine_occurrences where routine_id = ${routineId}
    order by local_date
  `;
  expect(rows).toEqual(expected);
}

async function expectTimerStatus(
  database: Sql<Record<string, never>>,
  coverageLoopId: string,
  expected: string,
): Promise<void> {
  const rows = await database<{ readonly status: string }[]>`
    select status from timers where coverage_loop_id = ${coverageLoopId}
  `;
  expect(rows[0]?.status).toBe(expected);
}

function routineDraft(fixture: Fixture): RoutineRevisionDraft {
  return {
    title: "Wednesday pickup",
    minimumSharedMeaning: "Wednesday pickup",
    recurrence: {
      kind: "weekly",
      weekdays: [3],
      intervalWeeks: 1,
      startsOn: "2026-08-05",
      endsOn: null,
      excludedDates: [],
    },
    timePlan: {
      timeZone: "America/Los_Angeles",
      event: { kind: "local_clock", time: "15:00", dayOffset: 0 },
      deadline: null,
      preparationMinutes: 30,
      travelMinutes: 15,
      earliestUseful: { kind: "relative", anchor: "event", offsetMinutes: -180 },
      lastResponsible: { kind: "relative", anchor: "event", offsetMinutes: -30 },
    },
    notificationMode: "exceptions_only",
    destination: {
      conversationId: fixture.conversationId,
      participantEpochId: fixture.epochId,
      participantSetDigest: fixture.participantSetDigest,
      audience: "group",
    },
    proposedHolderPersonId: fixture.holderId,
    standingCoverage: null,
    sourceRevisionRefs: ["source-revision:pickup"],
    effectiveFrom: "2026-08-05",
    effectiveThrough: null,
  };
}

async function seedFixture(database: Sql<Record<string, never>>): Promise<Fixture> {
  const actorId = randomUUID();
  const holderId = randomUUID();
  const actorIdentityId = randomUUID();
  const holderIdentityId = randomUUID();
  const householdId = randomUUID();
  const actorMembershipId = randomUUID();
  const holderMembershipId = randomUUID();
  const conversationId = randomUUID();
  const epochId = randomUUID();
  const participantSetDigest = canonicalDigest([actorIdentityId, holderIdentityId].sort());
  const now = new Date("2026-08-05T15:00:00.000Z");

  await database.begin(async (transaction) => {
    for (const personId of [actorId, holderId]) {
      await transaction`
        insert into people (
          id, status, timezone, consented_at, registered_at, created_at, updated_at
        ) values (
          ${personId}, 'registered', 'America/Los_Angeles', ${now}, ${now}, ${now}, ${now}
        )
      `;
    }
    for (const [identityId, personId] of [
      [actorIdentityId, actorId],
      [holderIdentityId, holderId],
    ] as const) {
      await transaction`
        insert into person_identities (
          id, person_id, kind, issuer, subject_digest, status,
          observed_at, verified_at, created_at, updated_at
        ) values (
          ${identityId}, ${personId}, 'phone', 'test', ${canonicalDigest(identityId)},
          'verified', ${now}, ${now}, ${now}, ${now}
        )
      `;
    }
    await transaction`
      insert into households (id, timezone, status, created_at, updated_at)
      values (${householdId}, 'America/Los_Angeles', 'active', ${now}, ${now})
    `;
    for (const [membershipId, personId] of [
      [actorMembershipId, actorId],
      [holderMembershipId, holderId],
    ] as const) {
      await transaction`
        insert into household_memberships (
          id, household_id, person_id, role, status, consented_at, joined_at,
          created_at, updated_at
        ) values (
          ${membershipId}, ${householdId}, ${personId}, 'steward', 'active',
          ${now}, ${now}, ${now}, ${now}
        )
      `;
      await transaction`
        insert into membership_capabilities (
          membership_id, capability, status, granted_by_membership_id, granted_at
        ) values (
          ${membershipId}, 'coordination.originate', 'active', ${membershipId}, ${now}
        )
      `;
      await transaction`
        insert into membership_capabilities (
          membership_id, capability, status, granted_by_membership_id, granted_at
        ) values (
          ${membershipId}, 'coordination.coordinate', 'active', ${membershipId}, ${now}
        )
      `;
    }
    await transaction`
      insert into conversations (
        id, household_id, kind, purpose, status, created_at, updated_at
      ) values (
        ${conversationId}, ${householdId}, 'group', 'family coordination', 'active', ${now}, ${now}
      )
    `;
    await transaction`
      insert into participant_epochs (
        id, conversation_id, sequence, participant_set_digest, authority_digest,
        change_reason, started_at
      ) values (
        ${epochId}, ${conversationId}, 1, ${participantSetDigest},
        ${canonicalDigest({ conversationId, participantSetDigest })}, 'test fixture', ${now}
      )
    `;
    await transaction`
      update conversations set current_epoch_id = ${epochId} where id = ${conversationId}
    `;
    for (const [identityId, personId] of [
      [actorIdentityId, actorId],
      [holderIdentityId, holderId],
    ] as const) {
      await transaction`
        insert into epoch_participants (
          participant_epoch_id, person_identity_id, person_id, registration_status,
          consented_at, added_at
        ) values (
          ${epochId}, ${identityId}, ${personId}, 'registered', ${now}, ${now}
        )
      `;
      await transaction`
        insert into participant_policies (
          id, conversation_id, person_id, version, status, allow_content_processing,
          allow_direct_responses, allow_proactive_writes, retention_seconds,
          changed_by_person_id, effective_at
        ) values (
          ${randomUUID()}, ${conversationId}, ${personId}, 1, 'active', true,
          true, false, 2592000, ${personId}, ${now}
        )
      `;
    }
  });
  return {
    actorId,
    actorMembershipId,
    holderId,
    householdId,
    conversationId,
    epochId,
    participantSetDigest,
  };
}
