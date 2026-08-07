import { createHash, randomInt, randomUUID } from "node:crypto";
import postgres, { type Sql } from "postgres";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import type { LinqChatSnapshot, LinqParticipant } from "../../src/adapters/linq/index.js";
import {
  participantEpochAuthorityDigest,
  participantSetDigest,
} from "../../src/modules/conversations/index.js";
import {
  createCoverageLoop,
  createCoverageTimer,
  PostgresCoordination,
} from "../../src/modules/coordination/index.js";
import { DurableTimers } from "../../src/modules/work/index.js";
import { TimerRuntime } from "../../src/runtime/timer-runtime.js";
import { SecretBox } from "../../src/shared/crypto.js";

const databaseUrl = process.env.TEST_DATABASE_URL;
const schema = process.env.TEST_POSTGRES_SCHEMA ?? "florence_v4";
const describeDatabase = databaseUrl ? describe : describe.skip;

describeDatabase("parent-steward coverage escalation", () => {
  let database: Sql<Record<string, never>>;
  const householdIds: string[] = [];
  const personIds: string[] = [];
  const secretBox = new SecretBox(
    "test-v1",
    JSON.stringify({ "test-v1": Buffer.alloc(32, 19).toString("base64") }),
  );

  beforeAll(async () => {
    database = postgres(databaseUrl as string, {
      max: 3,
      connection: { search_path: schema, TimeZone: "UTC" },
    });
    const tables = await database<{ readonly present: string | null }[]>`
      select to_regclass(${`${schema}.coverage_reliance_audiences`})::text as present
    `;
    if (!tables[0]?.present) throw new Error(`Expected parent escalation migration in ${schema}`);
  });

  afterAll(async () => {
    for (const householdId of householdIds) {
      const decisions = await database<{ readonly id: string }[]>`
        select authorization_decision_id as id from outbox where household_id = ${householdId}
      `;
      await database`delete from coverage_reliance_audiences where household_id = ${householdId}`;
      await database`delete from outbox where household_id = ${householdId}`;
      if (decisions.length > 0) {
        await database`
          delete from disclosure_decisions
          where id = any(${database.array(decisions.map((entry) => entry.id))}::uuid[])
        `;
      }
      await database`delete from households where id = ${householdId}`;
    }
    if (personIds.length > 0) {
      await database`delete from person_identities where person_id = any(${database.array(personIds)}::uuid[])`;
      await database`delete from people where id = any(${database.array(personIds)}::uuid[])`;
    }
    await database.end();
  });

  it("fans out only to current stewards, records a missing DM, and replays without duplication", async () => {
    const now = new Date();
    const fixture = await seedEscalationFixture(database, now, secretBox);
    householdIds.push(fixture.householdId);
    personIds.push(fixture.firstStewardId, fixture.secondStewardId, fixture.caregiverId);

    const snapshots = new Map<string, LinqChatSnapshot>([
      [
        fixture.groupChatId,
        chatSnapshot(
          fixture.groupChatId,
          "group",
          fixture.groupProviderDigest,
          [fixture.firstStewardPhone, fixture.secondStewardPhone, fixture.caregiverPhone],
          now,
        ),
      ],
      [
        fixture.firstStewardChatId,
        chatSnapshot(
          fixture.firstStewardChatId,
          "direct",
          fixture.firstStewardProviderDigest,
          [fixture.firstStewardPhone],
          now,
        ),
      ],
      [
        fixture.caregiverChatId,
        chatSnapshot(
          fixture.caregiverChatId,
          "direct",
          fixture.caregiverProviderDigest,
          [fixture.caregiverPhone],
          now,
        ),
      ],
    ]);
    const getChat = vi.fn(async (providerChatId: string) => {
      const snapshot = snapshots.get(providerChatId);
      if (!snapshot) throw new Error("unknown test chat");
      return snapshot;
    });
    const runtime = new TimerRuntime(database, secretBox, { getChat });
    const timers = new DurableTimers(database);

    const firstClaim = (await timers.claimDue(10, new Date(now.getTime() + 1_000)))[0];
    if (!firstClaim) throw new Error("Expected the escalation timer");
    await runtime.process(
      {
        id: firstClaim.id,
        kind: firstClaim.kind,
        coverageLoopId: firstClaim.coverageLoopId,
        expectedDomainVersion: firstClaim.expectedDomainVersion,
        dueAt: firstClaim.dueAt,
      },
      new Date(now.getTime() + 1_000),
    );

    const audiences = await database<
      {
        readonly person_id: string;
        readonly dispatch_state: string;
        readonly unavailable_reason: string | null;
        readonly outbox_id: string | null;
      }[]
    >`
      select person_id, dispatch_state, unavailable_reason, outbox_id
      from coverage_reliance_audiences
      where coverage_loop_id = ${fixture.loopId}
      order by person_id
    `;
    expect(audiences).toEqual(
      [
        {
          person_id: fixture.firstStewardId,
          dispatch_state: "queued",
          unavailable_reason: null,
          outbox_id: expect.any(String),
        },
        {
          person_id: fixture.secondStewardId,
          dispatch_state: "unavailable",
          unavailable_reason: "no_exact_private_dm",
          outbox_id: null,
        },
      ].sort((left, right) => left.person_id.localeCompare(right.person_id)),
    );
    expect(audiences.some((entry) => entry.person_id === fixture.caregiverId)).toBe(false);
    expect(getChat).not.toHaveBeenCalledWith(fixture.caregiverChatId);

    const effects = await database<
      { readonly person_id: string | null; readonly idempotency_key: string; readonly status: string }[]
    >`
      select person_id, idempotency_key, status from outbox
      where coverage_loop_id = ${fixture.loopId}
    `;
    expect(effects).toEqual([
      expect.objectContaining({ person_id: fixture.firstStewardId, status: "pending" }),
    ]);

    const rescheduled = await database<{ readonly due_at: Date }[]>`
      select due_at from timers where id = ${fixture.timerId} and status = 'scheduled'
    `;
    const retryAt = rescheduled[0]?.due_at;
    if (!retryAt) throw new Error("Expected missing-DM recheck");
    const secondClaim = (await timers.claimDue(10, new Date(retryAt.getTime() + 1)))[0];
    if (!secondClaim) throw new Error("Expected the escalation recheck");
    await runtime.process(
      {
        id: secondClaim.id,
        kind: secondClaim.kind,
        coverageLoopId: secondClaim.coverageLoopId,
        expectedDomainVersion: secondClaim.expectedDomainVersion,
        dueAt: secondClaim.dueAt,
      },
      new Date(retryAt.getTime() + 1),
    );
    const replayCounts = await database<{ readonly audiences: number; readonly effects: number }[]>`
      select
        (select count(*)::int from coverage_reliance_audiences
          where coverage_loop_id = ${fixture.loopId}) as audiences,
        (select count(*)::int from outbox
          where coverage_loop_id = ${fixture.loopId}) as effects
    `;
    expect(replayCounts[0]).toEqual({ audiences: 2, effects: 1 });
  });
});

async function seedEscalationFixture(database: Sql<Record<string, never>>, now: Date, secretBox: SecretBox) {
  const householdId = randomUUID();
  const firstStewardId = randomUUID();
  const secondStewardId = randomUUID();
  const caregiverId = randomUUID();
  const firstStewardIdentityId = randomUUID();
  const secondStewardIdentityId = randomUUID();
  const caregiverIdentityId = randomUUID();
  const phoneSeed = randomInt(0, 9_999_997);
  const firstStewardPhone = `+1415${phoneSeed.toString().padStart(7, "0")}`;
  const secondStewardPhone = `+1415${(phoneSeed + 1).toString().padStart(7, "0")}`;
  const caregiverPhone = `+1415${(phoneSeed + 2).toString().padStart(7, "0")}`;
  const groupConversationId = randomUUID();
  const groupEpochId = randomUUID();
  const groupChatId = randomUUID();
  const groupProviderDigest = `linq-v1:${"1".repeat(64)}`;
  const firstStewardConversationId = randomUUID();
  const firstStewardEpochId = randomUUID();
  const firstStewardChatId = randomUUID();
  const firstStewardProviderDigest = `linq-v1:${"2".repeat(64)}`;
  const caregiverConversationId = randomUUID();
  const caregiverEpochId = randomUUID();
  const caregiverChatId = randomUUID();
  const caregiverProviderDigest = `linq-v1:${"3".repeat(64)}`;
  const loopId = randomUUID();
  const lastResponsibleAt = new Date(now.getTime() + 60 * 60_000);

  await database.begin(async (transaction) => {
    for (const [personId, identityId, phone] of [
      [firstStewardId, firstStewardIdentityId, firstStewardPhone],
      [secondStewardId, secondStewardIdentityId, secondStewardPhone],
      [caregiverId, caregiverIdentityId, caregiverPhone],
    ] as const) {
      await transaction`
        insert into people (
          id, status, timezone, consented_at, registered_at, onboarding_step,
          created_at, updated_at
        ) values (
          ${personId}, 'registered', 'America/Los_Angeles', ${now}, ${now}, 'complete',
          ${now}, ${now}
        )
      `;
      await transaction`
        insert into person_identities (
          id, person_id, kind, issuer, subject_digest, status,
          observed_at, verified_at, created_at, updated_at
        ) values (
          ${identityId}, ${personId}, 'phone', 'linq', ${sha256(phone)}, 'verified',
          ${now}, ${now}, ${now}, ${now}
        )
      `;
    }
    await transaction`
      insert into households (id, timezone, status, created_at, updated_at)
      values (${householdId}, 'America/Los_Angeles', 'active', ${now}, ${now})
    `;
    for (const [personId, role] of [
      [firstStewardId, "steward"],
      [secondStewardId, "steward"],
      [caregiverId, "caregiver"],
    ] as const) {
      const membershipId = randomUUID();
      await transaction`
        insert into household_memberships (
          id, household_id, person_id, role, status, consented_at, joined_at,
          created_at, updated_at
        ) values (
          ${membershipId}, ${householdId}, ${personId}, ${role}, 'active',
          ${now}, ${now}, ${now}, ${now}
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

    await seedConversation(transaction, {
      householdId,
      conversationId: groupConversationId,
      epochId: groupEpochId,
      providerChatId: groupChatId,
      providerDigest: groupProviderDigest,
      kind: "group",
      participants: [
        [firstStewardId, firstStewardIdentityId],
        [secondStewardId, secondStewardIdentityId],
        [caregiverId, caregiverIdentityId],
      ],
      now,
    });
    await seedConversation(transaction, {
      householdId,
      conversationId: firstStewardConversationId,
      epochId: firstStewardEpochId,
      providerChatId: firstStewardChatId,
      providerDigest: firstStewardProviderDigest,
      kind: "direct",
      participants: [[firstStewardId, firstStewardIdentityId]],
      now,
    });
    await seedConversation(transaction, {
      householdId,
      conversationId: caregiverConversationId,
      epochId: caregiverEpochId,
      providerChatId: caregiverChatId,
      providerDigest: caregiverProviderDigest,
      kind: "direct",
      participants: [[caregiverId, caregiverIdentityId]],
      now,
    });
  });

  const groupParticipantDigest = participantSetDigest([
    firstStewardIdentityId,
    secondStewardIdentityId,
    caregiverIdentityId,
  ]);
  const loop = createCoverageLoop({
    loopId,
    householdId,
    minimumSharedMeaning: "Wednesday school pickup at 3 PM",
    unresolvedFacts: [],
    proposedHolderPersonId: null,
    timing: {
      timeZone: "America/Los_Angeles",
      localDate: now.toISOString().slice(0, 10),
      eventAt: new Date(now.getTime() + 90 * 60_000).toISOString(),
      deadlineAt: null,
      preparationMinutes: 0,
      travelMinutes: 0,
      earliestUsefulAt: new Date(now.getTime() - 60_000).toISOString(),
      lastResponsibleAt: lastResponsibleAt.toISOString(),
      resolutionPolicy: "wall_clock_compatible",
    },
    notificationMode: "exceptions_only",
    destination: {
      conversationId: groupConversationId,
      participantEpochId: groupEpochId,
      participantSetDigest: groupParticipantDigest,
      audience: "group",
    },
    sourceEvidenceRefs: [],
    occurredAt: new Date(now.getTime() - 60_000).toISOString(),
  });
  await new PostgresCoordination(database, secretBox).create(loop);
  const timer = createCoverageTimer({
    timerId: randomUUID(),
    loop,
    category: "coverage_steward_escalation",
    dueAt: now.toISOString(),
  });
  await new DurableTimers(database).scheduleCoverage({
    timer,
    household: { id: householdId, controlEpoch: 1 },
    conversation: { id: groupConversationId, authorityVersion: 1 },
  });
  return {
    householdId,
    firstStewardId,
    secondStewardId,
    caregiverId,
    firstStewardPhone,
    secondStewardPhone,
    caregiverPhone,
    groupChatId,
    groupProviderDigest,
    firstStewardChatId,
    firstStewardProviderDigest,
    caregiverChatId,
    caregiverProviderDigest,
    loopId,
    timerId: timer.timerId,
  };
}

async function seedConversation(
  transaction: postgres.TransactionSql<Record<string, never>>,
  input: {
    readonly householdId: string;
    readonly conversationId: string;
    readonly epochId: string;
    readonly providerChatId: string;
    readonly providerDigest: string;
    readonly kind: "direct" | "group";
    readonly participants: readonly (readonly [string, string])[];
    readonly now: Date;
  },
): Promise<void> {
  const appDigest = participantSetDigest(input.participants.map((entry) => entry[1]));
  await transaction`
    insert into conversations (
      id, household_id, kind, purpose, status, created_at, updated_at
    ) values (
      ${input.conversationId}, ${input.householdId}, ${input.kind}, 'test coordination',
      'active', ${input.now}, ${input.now}
    )
  `;
  await transaction`
    insert into participant_epochs (
      id, conversation_id, sequence, participant_set_digest, authority_digest,
      change_reason, started_at
    ) values (
      ${input.epochId}, ${input.conversationId}, 1, ${appDigest},
      ${participantEpochAuthorityDigest({
        conversationId: input.conversationId,
        sequence: 1,
        participantSetDigest: appDigest,
      })},
      'test fixture', ${input.now}
    )
  `;
  for (const [personId, identityId] of input.participants) {
    await transaction`
      insert into epoch_participants (
        participant_epoch_id, person_identity_id, person_id,
        registration_status, consented_at, added_at
      ) values (
        ${input.epochId}, ${identityId}, ${personId}, 'registered', ${input.now}, ${input.now}
      )
    `;
    await transaction`
      insert into participant_policies (
        id, conversation_id, person_id, version, status,
        allow_content_processing, allow_direct_responses, allow_proactive_writes,
        retention_seconds, changed_by_person_id, effective_at
      ) values (
        ${randomUUID()}, ${input.conversationId}, ${personId}, 1, 'active',
        true, true, false, 2592000, ${personId}, ${input.now}
      )
    `;
  }
  await transaction`
    update conversations set current_epoch_id = ${input.epochId}
    where id = ${input.conversationId}
  `;
  await transaction`
    insert into conversation_channels (
      id, conversation_id, provider, external_channel_id, status, bound_at,
      latest_participant_digest, latest_participant_checked_at
    ) values (
      ${randomUUID()}, ${input.conversationId}, 'linq', ${input.providerChatId},
      'active', ${input.now}, ${input.providerDigest}, ${input.now}
    )
  `;
}

function chatSnapshot(
  providerChatId: string,
  kind: "direct" | "group",
  activeParticipantDigest: string,
  humanPhones: readonly string[],
  now: Date,
): LinqChatSnapshot {
  const participant = (address: string, isSelf: boolean): LinqParticipant => ({
    providerParticipantId: randomUUID(),
    address,
    service: "imessage",
    isSelf,
    status: "active",
    joinedAt: now.toISOString(),
  });
  return {
    providerChatId,
    kind,
    health: "healthy",
    participants: [
      participant("+17865806076", true),
      ...humanPhones.map((phone) => participant(phone, false)),
    ],
    configuredLineActive: true,
    activeParticipantDigest,
    createdAt: now.toISOString(),
    updatedAt: now.toISOString(),
    checkedAt: now.toISOString(),
  };
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
