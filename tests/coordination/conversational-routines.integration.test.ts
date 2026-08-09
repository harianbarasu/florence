import { createHash, randomUUID } from "node:crypto";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import type { LinqMessageReceivedEvent, LinqParticipant } from "../../src/adapters/linq/index.js";
import { FlorenceApplication, type StoredLinqEvent } from "../../src/application/index.js";
import { loadConfig } from "../../src/config.js";
import {
  FamilyGroupAuthority,
  participantEpochAuthorityDigest,
  participantSetDigest,
} from "../../src/modules/conversations/index.js";
import { EffectOutbox } from "../../src/modules/effects/index.js";
import { PostgresSourceIntelligence } from "../../src/modules/sources/index.js";
import { canonicalDigest, canonicalJson } from "../../src/shared/canonical-json.js";
import { SecretBox } from "../../src/shared/crypto.js";

const databaseUrl = process.env.TEST_DATABASE_URL;
const schema = process.env.TEST_POSTGRES_SCHEMA ?? "florence_v4";
const describeDatabase = databaseUrl ? describe : describe.skip;

interface Fixture {
  readonly householdId: string;
  readonly conversationId: string;
  readonly epochId: string;
  readonly actorId: string;
  readonly holderId: string;
  readonly actorIdentityId: string;
  readonly holderIdentityId: string;
  readonly providerChatId: string;
  readonly providerParticipantDigest: string;
  readonly sourceObjectIds: string[];
  readonly providerEventIds: string[];
}

describeDatabase("conversational routine transaction", () => {
  let database: Sql<Record<string, never>>;
  let fixture: Fixture | null = null;
  const keyring = JSON.stringify({ "test-v1": Buffer.alloc(32, 23).toString("base64") });
  const secretBox = new SecretBox("test-v1", keyring);

  beforeAll(async () => {
    database = postgres(databaseUrl as string, {
      max: 3,
      connection: { search_path: schema, TimeZone: "UTC" },
    });
    const columns = await database<{ readonly present: string | null }[]>`
      select column_name as present from information_schema.columns
      where table_schema = ${schema} and table_name = 'outbox'
        and column_name = 'routine_pattern_candidate_id'
    `;
    if (!columns[0]?.present) throw new Error(`Expected routine-pattern migrations in ${schema}`);
  });

  afterEach(async () => {
    if (!fixture) return;
    const effects = await database<{ readonly id: string; readonly decision_id: string }[]>`
      select id, authorization_decision_id as decision_id from outbox
      where conversation_id = ${fixture.conversationId}
        or source_conversation_id = ${fixture.conversationId}
    `;
    if (effects.length > 0) {
      const effectIds = effects.map((entry) => entry.id);
      await database`delete from effect_receipts where outbox_id = any(${database.array(effectIds)}::uuid[])`;
      await database`delete from outbox where id = any(${database.array(effectIds)}::uuid[])`;
      await database`
        delete from disclosure_decisions
        where id = any(${database.array(effects.map((entry) => entry.decision_id))}::uuid[])
      `;
    }
    await database`delete from routines where household_id = ${fixture.householdId}`;
    await database`
      delete from knowledge_candidates
      where conversation_id = ${fixture.conversationId} and candidate_kind = 'routine_pattern'
    `;
    if (fixture.sourceObjectIds.length > 0) {
      await database`
        delete from source_objects
        where id = any(${database.array(fixture.sourceObjectIds)}::uuid[])
      `;
    }
    if (fixture.providerEventIds.length > 0) {
      await database`
        delete from provider_events
        where id = any(${database.array(fixture.providerEventIds)}::uuid[])
      `;
    }
    await database`delete from conversations where id = ${fixture.conversationId}`;
    await database`delete from households where id = ${fixture.householdId}`;
    await database`
      delete from person_identities
      where person_id in (${fixture.actorId}, ${fixture.holderId})
    `;
    await database`delete from people where id in (${fixture.actorId}, ${fixture.holderId})`;
    fixture = null;
  });

  afterAll(async () => {
    await database.end();
  });

  it("lets only the exact holder create and later revise one source-derived routine", async () => {
    const now = new Date();
    fixture = await seedFixture(database, now);
    const authority = await new FamilyGroupAuthority(database).reconcile({
      conversationId: fixture.conversationId,
      occurredAt: now,
    });
    expect(authority.status).toBe("active");

    const sources = new PostgresSourceIntelligence(database, secretBox, {
      rawRetentionDays: 30,
      privateCandidateRetentionDays: 7,
    });
    const sourceText = "Jenny picks Violet up every Tuesday at 3 PM.";
    const proposalMessage = linqMessage({
      providerChatId: fixture.providerChatId,
      providerMessageId: randomUUID(),
      senderAddress: "+15555550101",
      text: sourceText,
      now,
    });
    const proposalSource = await sources.apply({
      kind: "ingest_source",
      integrationId: null,
      expectedIntegrationControlEpoch: null,
      artifactKind: "conversation_message",
      origin: { system: "linq", remoteObjectId: proposalMessage.message.providerMessageId },
      scope: { kind: "conversation_epoch", participantEpochId: fixture.epochId },
      conversationAccessMode: "unanimously_shared",
      content: { text: sourceText },
      occurredAt: now.toISOString(),
      capturedAt: now.toISOString(),
      requestedRetentionUntil: new Date(now.getTime() + 30 * 86_400_000).toISOString(),
    });
    if (proposalSource.kind !== "source_ingested") throw new Error("Expected source ingestion");
    fixture.sourceObjectIds.push(proposalSource.sourceObjectId);

    const proposalEventId = await insertProcessedEvent(database, secretBox, {
      event: proposalMessage,
      fixture,
      senderIdentityId: fixture.actorIdentityId,
      senderPersonId: fixture.actorId,
    });
    fixture.providerEventIds.push(proposalEventId);
    const app = new FlorenceApplication(
      database,
      testConfig(databaseUrl as string, schema, keyring),
      secretBox,
    );
    const proposed = await app.process({
      kind: "linq.routine_pattern_proposal",
      internalProviderEventId: proposalEventId,
      proposal: {
        title: "Violet's Tuesday pickup",
        minimumSharedMeaning: "Jenny handles Violet's Tuesday pickup",
        semanticTiming: "Every Tuesday at 3 PM",
        timeZone: "America/New_York",
        eventAt: null,
        deadlineAt: null,
        proposedHolderPersonId: fixture.holderId,
        evidence: [
          { sourceRevisionId: proposalSource.sourceRevisionId, support: "Explicit recurring pickup" },
        ],
        uncertainties: [],
        confidence: 0.96,
      },
    });
    expect(proposed.disposition).toBe("routine_pattern_confirmation_queued");
    const candidateId = proposed.ids.candidateId;
    const rootPromptId = proposed.ids.responseOutboxId;
    if (!candidateId || !rootPromptId) throw new Error("Expected bound candidate prompt");

    const promptRows = await database<
      {
        readonly id: string;
        readonly routine_pattern_candidate_id: string | null;
        readonly payload_ciphertext: Buffer;
      }[]
    >`
      select id, routine_pattern_candidate_id, payload_ciphertext from outbox where id = ${rootPromptId}
    `;
    expect(promptRows[0]?.routine_pattern_candidate_id).toBe(candidateId);
    const prompt = JSON.parse(
      secretBox
        .decrypt(JSON.parse(promptRows[0]?.payload_ciphertext.toString("utf8") ?? "{}"), "effect-payload")
        .toString("utf8"),
    ) as { readonly text: string };
    expect(prompt.text).toContain("Every Tuesday at 3:00 PM (America/Los_Angeles)");
    expect(prompt.text).toContain("Exceptions: none");
    const routineBeforeConfirmation = await database<{ readonly count: number }[]>`
      select count(*)::int as count from routines where id = ${candidateId}
    `;
    expect(routineBeforeConfirmation[0]?.count).toBe(0);

    const failedAt = new Date(now.getTime() + 1_000);
    await database`
      update outbox set status = 'dead', last_error_code = 'linq_delivery_failed', updated_at = ${failedAt}
      where id = ${rootPromptId}
    `;
    const redriveAt = new Date(failedAt.getTime() + 61_000);
    expect(await new EffectOutbox(database, secretBox).redriveFailed(redriveAt, 1)).toBe(1);
    const successors = await database<
      { readonly id: string; readonly routine_pattern_candidate_id: string | null }[]
    >`
      select id, routine_pattern_candidate_id from outbox where redrive_root_id = ${rootPromptId}
    `;
    expect(successors).toHaveLength(1);
    expect(successors[0]?.routine_pattern_candidate_id).toBe(candidateId);
    const deliveredPromptId = successors[0]?.id;
    if (!deliveredPromptId) throw new Error("Expected redriven prompt");
    const providerPromptMessageId = randomUUID();
    const confirmedAt = new Date(redriveAt.getTime() + 1_000);
    await database`
      update outbox set status = 'confirmed', updated_at = ${confirmedAt}
      where id = ${deliveredPromptId}
    `;
    await database`
      insert into effect_receipts (
        id, outbox_id, idempotency_key, provider_receipt_id, status,
        receipt_digest, occurred_at, reconciled_at
      ) select ${randomUUID()}, id, idempotency_key, ${providerPromptMessageId}, 'confirmed',
        ${canonicalDigest({ status: "confirmed", providerPromptMessageId })}, ${confirmedAt}, ${confirmedAt}
      from outbox where id = ${deliveredPromptId}
    `;

    const confirmationMessage = linqMessage({
      providerChatId: fixture.providerChatId,
      providerMessageId: randomUUID(),
      senderAddress: "+15555550102",
      text: "yes",
      now: new Date(confirmedAt.getTime() + 1_000),
      replyToProviderMessageId: providerPromptMessageId,
    });
    const confirmationSource = await sources.apply({
      kind: "ingest_source",
      integrationId: null,
      expectedIntegrationControlEpoch: null,
      artifactKind: "conversation_message",
      origin: { system: "linq", remoteObjectId: confirmationMessage.message.providerMessageId },
      scope: { kind: "conversation_epoch", participantEpochId: fixture.epochId },
      conversationAccessMode: "unanimously_shared",
      content: { text: "yes" },
      occurredAt: confirmationMessage.occurredAt,
      capturedAt: confirmationMessage.receivedAt,
      requestedRetentionUntil: new Date(now.getTime() + 30 * 86_400_000).toISOString(),
    });
    if (confirmationSource.kind !== "source_ingested") throw new Error("Expected confirmation source");
    fixture.sourceObjectIds.push(confirmationSource.sourceObjectId);
    const confirmationEventId = await insertProcessedEvent(database, secretBox, {
      event: confirmationMessage,
      fixture,
      senderIdentityId: fixture.holderIdentityId,
      senderPersonId: fixture.holderId,
    });
    fixture.providerEventIds.push(confirmationEventId);

    const accepted = await app.process({
      kind: "linq.routine_pattern_confirmation",
      internalProviderEventId: confirmationEventId,
      candidateId,
    });
    expect(accepted).toMatchObject({
      accepted: true,
      disposition: "routine_pattern_accepted",
      ids: { candidateId, routineId: candidateId },
    });
    const result = await database<
      {
        readonly routine_id: string;
        readonly holder_person_id: string;
        readonly authorized_by_person_id: string;
        readonly source_revision_refs: string[];
        readonly candidate_status: string;
      }[]
    >`
      select routine.id as routine_id,
        revision.standing_holder_person_id as holder_person_id,
        revision.standing_authorized_by_person_id as authorized_by_person_id,
        revision.source_revision_refs, candidate.status as candidate_status
      from routines routine
      join routine_revisions revision on revision.routine_id = routine.id
        and revision.revision = routine.current_revision
      join knowledge_candidates candidate on candidate.id = routine.id
      where routine.id = ${candidateId}
    `;
    expect(result[0]).toMatchObject({
      routine_id: candidateId,
      holder_person_id: fixture.holderId,
      authorized_by_person_id: fixture.holderId,
      candidate_status: "accepted",
    });
    expect(result[0]?.source_revision_refs).toEqual(
      expect.arrayContaining([proposalSource.sourceRevisionId, confirmationSource.sourceRevisionId]),
    );
    expect(
      await new EffectOutbox(database, secretBox).redriveFailed(new Date(confirmedAt.getTime() + 120_000), 5),
    ).toBe(0);

    const overlapAt = new Date(confirmedAt.getTime() + 150_000);
    const overlapMessage = linqMessage({
      providerChatId: fixture.providerChatId,
      providerMessageId: randomUUID(),
      senderAddress: "+15555550101",
      text: "Jenny handles another responsibility every Tuesday at 3 PM.",
      now: overlapAt,
    });
    const overlapSource = await sources.apply({
      kind: "ingest_source",
      integrationId: null,
      expectedIntegrationControlEpoch: null,
      artifactKind: "conversation_message",
      origin: { system: "linq", remoteObjectId: overlapMessage.message.providerMessageId },
      scope: { kind: "conversation_epoch", participantEpochId: fixture.epochId },
      conversationAccessMode: "unanimously_shared",
      content: { text: "Jenny handles another responsibility every Tuesday at 3 PM." },
      occurredAt: overlapMessage.occurredAt,
      capturedAt: overlapMessage.receivedAt,
      requestedRetentionUntil: new Date(overlapAt.getTime() + 30 * 86_400_000).toISOString(),
    });
    if (overlapSource.kind !== "source_ingested") throw new Error("Expected overlap source");
    fixture.sourceObjectIds.push(overlapSource.sourceObjectId);
    const overlapEventId = await insertProcessedEvent(database, secretBox, {
      event: overlapMessage,
      fixture,
      senderIdentityId: fixture.actorIdentityId,
      senderPersonId: fixture.actorId,
    });
    fixture.providerEventIds.push(overlapEventId);
    const overlappingProposal = await app.process({
      kind: "linq.routine_pattern_proposal",
      internalProviderEventId: overlapEventId,
      proposal: {
        title: "Another Tuesday responsibility",
        minimumSharedMeaning: "Jenny handles another Tuesday responsibility",
        semanticTiming: "Every Tuesday at 3 PM",
        timeZone: "America/New_York",
        eventAt: null,
        deadlineAt: null,
        proposedHolderPersonId: fixture.holderId,
        evidence: [
          { sourceRevisionId: overlapSource.sourceRevisionId, support: "Explicit weekly statement" },
        ],
        uncertainties: [],
        confidence: 0.96,
      },
    });
    expect(overlappingProposal).toMatchObject({
      accepted: false,
      disposition: "routine_pattern_existing_routine_ambiguous",
    });
    expect(overlappingProposal.ids.candidateId).toBeUndefined();

    const unrelatedChangeAt = new Date(confirmedAt.getTime() + 165_000);
    const unrelatedChangeText = "Jenny now handles soccer every Friday at 4 PM.";
    const unrelatedChangeMessage = linqMessage({
      providerChatId: fixture.providerChatId,
      providerMessageId: randomUUID(),
      senderAddress: "+15555550101",
      text: unrelatedChangeText,
      now: unrelatedChangeAt,
    });
    const unrelatedChangeSource = await sources.apply({
      kind: "ingest_source",
      integrationId: null,
      expectedIntegrationControlEpoch: null,
      artifactKind: "conversation_message",
      origin: { system: "linq", remoteObjectId: unrelatedChangeMessage.message.providerMessageId },
      scope: { kind: "conversation_epoch", participantEpochId: fixture.epochId },
      conversationAccessMode: "unanimously_shared",
      content: { text: unrelatedChangeText },
      occurredAt: unrelatedChangeMessage.occurredAt,
      capturedAt: unrelatedChangeMessage.receivedAt,
      requestedRetentionUntil: new Date(unrelatedChangeAt.getTime() + 30 * 86_400_000).toISOString(),
    });
    if (unrelatedChangeSource.kind !== "source_ingested") {
      throw new Error("Expected unrelated change source");
    }
    fixture.sourceObjectIds.push(unrelatedChangeSource.sourceObjectId);
    const unrelatedChangeEventId = await insertProcessedEvent(database, secretBox, {
      event: unrelatedChangeMessage,
      fixture,
      senderIdentityId: fixture.actorIdentityId,
      senderPersonId: fixture.actorId,
    });
    fixture.providerEventIds.push(unrelatedChangeEventId);
    const unrelatedChangeProposal = await app.process({
      kind: "linq.routine_pattern_proposal",
      internalProviderEventId: unrelatedChangeEventId,
      proposal: {
        title: "Friday soccer",
        minimumSharedMeaning: "Jenny handles Friday soccer",
        semanticTiming: "Every Friday at 4 PM",
        timeZone: "America/New_York",
        eventAt: null,
        deadlineAt: null,
        proposedHolderPersonId: fixture.holderId,
        evidence: [
          {
            sourceRevisionId: unrelatedChangeSource.sourceRevisionId,
            support: "Explicit weekly soccer statement",
          },
        ],
        uncertainties: [],
        confidence: 0.96,
      },
    });
    expect(unrelatedChangeProposal).toMatchObject({
      accepted: false,
      disposition: "routine_pattern_existing_routine_ambiguous",
    });
    expect(unrelatedChangeProposal.ids.candidateId).toBeUndefined();

    const changedAt = new Date(confirmedAt.getTime() + 180_000);
    const changeText = "Jenny now picks Violet up every Tuesday at 4 PM instead of 3 PM.";
    const changeMessage = linqMessage({
      providerChatId: fixture.providerChatId,
      providerMessageId: randomUUID(),
      senderAddress: "+15555550101",
      text: changeText,
      now: changedAt,
    });
    const changeSource = await sources.apply({
      kind: "ingest_source",
      integrationId: null,
      expectedIntegrationControlEpoch: null,
      artifactKind: "conversation_message",
      origin: { system: "linq", remoteObjectId: changeMessage.message.providerMessageId },
      scope: { kind: "conversation_epoch", participantEpochId: fixture.epochId },
      conversationAccessMode: "unanimously_shared",
      content: { text: changeText },
      occurredAt: changeMessage.occurredAt,
      capturedAt: changeMessage.receivedAt,
      requestedRetentionUntil: new Date(changedAt.getTime() + 30 * 86_400_000).toISOString(),
    });
    if (changeSource.kind !== "source_ingested") throw new Error("Expected change source");
    fixture.sourceObjectIds.push(changeSource.sourceObjectId);
    const changeEventId = await insertProcessedEvent(database, secretBox, {
      event: changeMessage,
      fixture,
      senderIdentityId: fixture.actorIdentityId,
      senderPersonId: fixture.actorId,
    });
    fixture.providerEventIds.push(changeEventId);

    const proposedChange = await app.process({
      kind: "linq.routine_pattern_proposal",
      internalProviderEventId: changeEventId,
      proposal: {
        title: "Violet's Tuesday pickup",
        minimumSharedMeaning: "Jenny handles Violet's Tuesday pickup",
        semanticTiming: "Every Tuesday at 4 PM instead of 3 PM",
        timeZone: "America/New_York",
        eventAt: null,
        deadlineAt: null,
        proposedHolderPersonId: fixture.holderId,
        evidence: [
          { sourceRevisionId: changeSource.sourceRevisionId, support: "Explicit changed pickup time" },
        ],
        uncertainties: [],
        confidence: 0.97,
      },
    });
    expect(proposedChange.disposition).toBe("routine_pattern_confirmation_queued");
    const changeCandidateId = proposedChange.ids.candidateId;
    const changePromptId = proposedChange.ids.responseOutboxId;
    if (!changeCandidateId || !changePromptId) throw new Error("Expected change candidate prompt");
    const changePromptRows = await database<
      { readonly payload_ciphertext: Buffer; readonly routine_pattern_candidate_id: string | null }[]
    >`
      select payload_ciphertext, routine_pattern_candidate_id
      from outbox where id = ${changePromptId}
    `;
    const changePrompt = JSON.parse(
      secretBox
        .decrypt(
          JSON.parse(changePromptRows[0]?.payload_ciphertext.toString("utf8") ?? "{}"),
          "effect-payload",
        )
        .toString("utf8"),
    ) as { readonly text: string };
    expect(changePromptRows[0]?.routine_pattern_candidate_id).toBe(changeCandidateId);
    expect(changePrompt.text).toContain("update your existing routine “Violet's Tuesday pickup”");
    expect(changePrompt.text).toContain("Every Tuesday at 4:00 PM (America/Los_Angeles)");

    const changeProviderPromptId = randomUUID();
    const changePromptDeliveredAt = new Date(changedAt.getTime() + 1_000);
    await database`
      update outbox set status = 'confirmed', updated_at = ${changePromptDeliveredAt}
      where id = ${changePromptId}
    `;
    await database`
      insert into effect_receipts (
        id, outbox_id, idempotency_key, provider_receipt_id, status,
        receipt_digest, occurred_at, reconciled_at
      ) select ${randomUUID()}, id, idempotency_key, ${changeProviderPromptId}, 'confirmed',
        ${canonicalDigest({ status: "confirmed", changeProviderPromptId })},
        ${changePromptDeliveredAt}, ${changePromptDeliveredAt}
      from outbox where id = ${changePromptId}
    `;

    const changeConfirmationMessage = linqMessage({
      providerChatId: fixture.providerChatId,
      providerMessageId: randomUUID(),
      senderAddress: "+15555550102",
      text: "yes",
      now: new Date(changePromptDeliveredAt.getTime() + 1_000),
      replyToProviderMessageId: changeProviderPromptId,
    });
    const changeConfirmationSource = await sources.apply({
      kind: "ingest_source",
      integrationId: null,
      expectedIntegrationControlEpoch: null,
      artifactKind: "conversation_message",
      origin: {
        system: "linq",
        remoteObjectId: changeConfirmationMessage.message.providerMessageId,
      },
      scope: { kind: "conversation_epoch", participantEpochId: fixture.epochId },
      conversationAccessMode: "unanimously_shared",
      content: { text: "yes" },
      occurredAt: changeConfirmationMessage.occurredAt,
      capturedAt: changeConfirmationMessage.receivedAt,
      requestedRetentionUntil: new Date(changedAt.getTime() + 30 * 86_400_000).toISOString(),
    });
    if (changeConfirmationSource.kind !== "source_ingested") {
      throw new Error("Expected change confirmation source");
    }
    fixture.sourceObjectIds.push(changeConfirmationSource.sourceObjectId);
    const changeConfirmationEventId = await insertProcessedEvent(database, secretBox, {
      event: changeConfirmationMessage,
      fixture,
      senderIdentityId: fixture.holderIdentityId,
      senderPersonId: fixture.holderId,
    });
    fixture.providerEventIds.push(changeConfirmationEventId);

    const revised = await app.process({
      kind: "linq.routine_pattern_confirmation",
      internalProviderEventId: changeConfirmationEventId,
      candidateId: changeCandidateId,
    });
    expect(revised).toMatchObject({
      accepted: true,
      disposition: "routine_pattern_revised",
      ids: { candidateId: changeCandidateId, routineId: candidateId },
    });
    const revisedRows = await database<
      {
        readonly version: number;
        readonly current_revision: number;
        readonly semantic_time_plan: { readonly event?: { readonly time?: string } };
        readonly source_revision_refs: string[];
        readonly candidate_status: string;
      }[]
    >`
      select routine.version::int as version, routine.current_revision::int as current_revision,
        revision.semantic_time_plan,
        revision.source_revision_refs, candidate.status as candidate_status
      from routines routine
      join routine_revisions revision on revision.routine_id = routine.id
        and revision.revision = routine.current_revision
      join knowledge_candidates candidate on candidate.id = ${changeCandidateId}
      where routine.id = ${candidateId}
    `;
    expect(revisedRows[0]).toMatchObject({
      version: 2,
      current_revision: 2,
      semantic_time_plan: { event: { time: "16:00" } },
      candidate_status: "accepted",
    });
    expect(revisedRows[0]?.source_revision_refs).toEqual(
      expect.arrayContaining([
        proposalSource.sourceRevisionId,
        confirmationSource.sourceRevisionId,
        changeSource.sourceRevisionId,
        changeConfirmationSource.sourceRevisionId,
      ]),
    );
    const routineCount = await database<{ readonly count: number }[]>`
      select count(*)::int as count from routines where household_id = ${fixture.householdId}
    `;
    expect(routineCount[0]?.count).toBe(1);
  });
});

async function seedFixture(database: Sql<Record<string, never>>, now: Date): Promise<Fixture> {
  const fixture: Fixture = {
    householdId: randomUUID(),
    conversationId: randomUUID(),
    epochId: randomUUID(),
    actorId: randomUUID(),
    holderId: randomUUID(),
    actorIdentityId: randomUUID(),
    holderIdentityId: randomUUID(),
    providerChatId: randomUUID(),
    providerParticipantDigest: `linq-v1:${"9".repeat(64)}`,
    sourceObjectIds: [],
    providerEventIds: [],
  };
  const participantDigest = participantSetDigest([fixture.actorIdentityId, fixture.holderIdentityId]);
  await database.begin(async (transaction) => {
    for (const personId of [fixture.actorId, fixture.holderId]) {
      await transaction`
        insert into people (id, status, timezone, consented_at, registered_at, created_at, updated_at)
        values (${personId}, 'registered', 'America/Los_Angeles', ${now}, ${now}, ${now}, ${now})
      `;
    }
    for (const [identityId, personId, phone] of [
      [fixture.actorIdentityId, fixture.actorId, "+15555550101"],
      [fixture.holderIdentityId, fixture.holderId, "+15555550102"],
    ] as const) {
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
      values (${fixture.householdId}, 'America/Los_Angeles', 'active', ${now}, ${now})
    `;
    for (const [membershipId, personId, role] of [
      [randomUUID(), fixture.actorId, "steward"],
      [randomUUID(), fixture.holderId, "caregiver"],
    ] as const) {
      await transaction`
        insert into household_memberships (
          id, household_id, person_id, role, status, consented_at, joined_at, created_at, updated_at
        ) values (${membershipId}, ${fixture.householdId}, ${personId}, ${role}, 'active', ${now}, ${now}, ${now}, ${now})
      `;
      await transaction`
        insert into membership_capabilities (
          membership_id, capability, status, granted_by_membership_id, granted_at
        ) values (${membershipId}, 'coordination.originate', 'active', ${membershipId}, ${now})
      `;
    }
    await transaction`
      insert into conversations (id, kind, purpose, status, created_at, updated_at)
      values (${fixture.conversationId}, 'group', 'family coordination', 'active', ${now}, ${now})
    `;
    await transaction`
      insert into participant_epochs (
        id, conversation_id, sequence, participant_set_digest, authority_digest, change_reason, started_at
      ) values (
        ${fixture.epochId}, ${fixture.conversationId}, 1, ${participantDigest},
        ${participantEpochAuthorityDigest({
          conversationId: fixture.conversationId,
          sequence: 1,
          participantSetDigest: participantDigest,
        })}, 'routine integration fixture', ${now}
      )
    `;
    for (const [identityId, personId] of [
      [fixture.actorIdentityId, fixture.actorId],
      [fixture.holderIdentityId, fixture.holderId],
    ] as const) {
      await transaction`
        insert into epoch_participants (
          participant_epoch_id, person_identity_id, person_id, registration_status, consented_at, added_at
        ) values (${fixture.epochId}, ${identityId}, ${personId}, 'registered', ${now}, ${now})
      `;
      await transaction`
        insert into participant_policies (
          id, conversation_id, person_id, version, status, allow_content_processing,
          allow_direct_responses, allow_proactive_writes, retention_seconds,
          changed_by_person_id, effective_at
        ) values (
          ${randomUUID()}, ${fixture.conversationId}, ${personId}, 1, 'active', true, true, false,
          2592000, ${personId}, ${now}
        )
      `;
    }
    await transaction`update conversations set current_epoch_id = ${fixture.epochId} where id = ${fixture.conversationId}`;
    await transaction`
      insert into conversation_channels (
        id, conversation_id, provider, external_channel_id, status, bound_at,
        latest_participant_digest, latest_participant_checked_at
      ) values (
        ${randomUUID()}, ${fixture.conversationId}, 'linq', ${fixture.providerChatId}, 'active', ${now},
        ${fixture.providerParticipantDigest}, ${now}
      )
    `;
  });
  return fixture;
}

async function insertProcessedEvent(
  database: Sql<Record<string, never>>,
  secretBox: SecretBox,
  input: {
    readonly event: LinqMessageReceivedEvent;
    readonly fixture: Fixture;
    readonly senderIdentityId: string;
    readonly senderPersonId: string;
  },
): Promise<string> {
  const id = randomUUID();
  const providerEventId = input.event.providerEventId;
  const record: StoredLinqEvent = {
    schemaVersion: 1,
    classification: "full",
    messageOccurredAt: input.event.occurredAt,
    event: input.event,
    routing: {
      conversationId: input.fixture.conversationId,
      participantEpochId: input.fixture.epochId,
      appParticipantDigest: participantSetDigest([
        input.fixture.actorIdentityId,
        input.fixture.holderIdentityId,
      ]),
      providerParticipantDigest: input.fixture.providerParticipantDigest,
      liveIdentityIds: [input.fixture.actorIdentityId, input.fixture.holderIdentityId],
      senderIdentityId: input.senderIdentityId,
      senderPersonId: input.senderPersonId,
      providerChatId: input.fixture.providerChatId,
      chatKind: "group",
    },
  };
  const encrypted = secretBox.encrypt(canonicalJson(record), `provider-event:${providerEventId}`);
  await database`
    insert into provider_events (
      id, provider, provider_event_id, event_type, external_channel_id, occurred_at,
      received_at, payload_digest, envelope_ciphertext, envelope_key_version,
      admission_status, processing_status, processed_at
    ) values (
      ${id}, 'linq', ${providerEventId}, ${input.event.eventType}, ${input.fixture.providerChatId},
      ${new Date(input.event.occurredAt)}, ${new Date(input.event.receivedAt)},
      ${canonicalDigest(input.event)}, ${Buffer.from(JSON.stringify(encrypted), "utf8")}, ${encrypted.kid},
      'verified', 'processed', ${new Date(input.event.receivedAt)}
    )
  `;
  return id;
}

function linqMessage(input: {
  readonly providerChatId: string;
  readonly providerMessageId: string;
  readonly senderAddress: string;
  readonly text: string;
  readonly now: Date;
  readonly replyToProviderMessageId?: string;
}): LinqMessageReceivedEvent {
  const sender: LinqParticipant = {
    providerParticipantId: randomUUID(),
    address: input.senderAddress,
    service: "imessage",
    isSelf: false,
    status: "active",
    joinedAt: input.now.toISOString(),
  };
  return {
    provider: "linq",
    providerEventId: randomUUID(),
    providerTraceId: randomUUID(),
    providerCreatedAt: input.now.toISOString(),
    occurredAt: input.now.toISOString(),
    receivedAt: input.now.toISOString(),
    eventType: "linq.message.received",
    channel: { providerChatId: input.providerChatId, kind: "group" },
    message: {
      providerMessageId: input.providerMessageId,
      sender,
      service: "imessage",
      parts: [{ kind: "text", text: input.text }],
      sentAt: input.now.toISOString(),
      ...(input.replyToProviderMessageId
        ? { replyTo: { providerMessageId: input.replyToProviderMessageId, partIndex: 0 } }
        : {}),
    },
  };
}

function testConfig(databaseUrl: string, schema: string, keyring: string) {
  return loadConfig({
    NODE_ENV: "test",
    FLORENCE_DATABASE_URL: databaseUrl,
    FLORENCE_POSTGRES_SCHEMA: schema,
    FLORENCE_WEB_BASE_URL: "http://localhost:3000",
    FLORENCE_TOKEN_ENCRYPTION_KEY: "routine-pattern-test-token-key-123456789",
    FLORENCE_DATA_ACTIVE_KEY_ID: "test-v1",
    FLORENCE_DATA_KEYRING_JSON: keyring,
    LINQ_API_KEY: "test-linq-key",
    LINQ_FROM_PHONE: "+15555550100",
    LINQ_WEBHOOK_SECRET: `whsec_${Buffer.alloc(32, 7).toString("base64")}`,
    GOOGLE_CLIENT_ID: "test-google-client",
    GOOGLE_CLIENT_SECRET: "test-google-secret",
    MODEL_PROVIDER: "openai",
    OPENAI_API_KEY: "test-openai-key",
  });
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
