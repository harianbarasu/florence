import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { Database } from "../../db/client.js";
import { canonicalDigest } from "../../shared/canonical-json.js";
import type { SecretBox } from "../../shared/crypto.js";
import { ConflictError } from "../../shared/errors.js";
import {
  type CoverageLoop,
  CoverageLoopSchema,
  type CoverageTransition,
  CoverageTransitionSchema,
  EntityIdSchema,
  type NeutralNotificationPlan,
  NeutralNotificationPlanSchema,
} from "./contracts.js";
import { type CoverageCommand, type CoverageTransitionDecision, transitionCoverage } from "./coverage.js";
import type { NotificationDecision } from "./notifications.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

const coverageContentSchema = z.strictObject({
  minimumSharedMeaning: z.string(),
  unresolvedFacts: z.array(z.string()),
});

export interface PersistedCoverageCommand {
  readonly loopId: string;
  readonly command: CoverageCommand;
}

export interface PersistedCoverageNotification {
  readonly loopId: string;
  readonly expectedVersion: number;
  readonly decision: Extract<NotificationDecision, { readonly kind: "send" }>;
}

interface CoverageRow {
  readonly id: string;
  readonly household_id: string;
  readonly version: number | string;
  readonly state: string;
  readonly content_ciphertext: Buffer;
  readonly unresolved_facts: unknown;
  readonly proposed_holder_person_id: string | null;
  readonly acknowledged_by_person_id: string | null;
  readonly acknowledged_at: Date | null;
  readonly acknowledgment_kind: string | null;
  readonly holder_disclosure: string | null;
  readonly time_zone: string;
  readonly local_date: string | Date;
  readonly event_at: Date | null;
  readonly deadline_at: Date | null;
  readonly preparation_minutes: number;
  readonly travel_minutes: number;
  readonly earliest_useful_at: Date;
  readonly last_responsible_at: Date;
  readonly plan_version: number | string;
  readonly notification_mode: string;
  readonly destination_conversation_id: string;
  readonly participant_epoch_id: string;
  readonly participant_set_digest: string;
  readonly audience: string;
  readonly source_evidence_refs: unknown;
  readonly routine_occurrence_id: string | null;
  readonly routine_occurrence_version: number | string | null;
  readonly routine_id: string | null;
  readonly routine_revision: number | string | null;
  readonly attention_cycle: number;
  readonly notification_history: unknown;
  readonly last_transition_at: Date;
}

/** Transaction-participating persistence for the canonical coverage aggregate. */
export class PostgresCoordination {
  public constructor(
    private readonly database: Executor,
    private readonly secretBox: SecretBox,
  ) {}

  public async create(loopCandidate: CoverageLoop): Promise<CoverageLoop> {
    const loop = CoverageLoopSchema.parse(loopCandidate);
    return inTransaction(this.database, async (transaction) => {
      const encrypted = encryptContent(this.secretBox, loop);
      await transaction`
        insert into coverage_loops (
          id, household_id, version, state, minimum_shared_meaning_digest,
          content_ciphertext, content_key_version, unresolved_facts,
          proposed_holder_person_id, acknowledged_by_person_id, acknowledged_at,
          acknowledgment_kind, holder_disclosure, time_zone, local_date, event_at, deadline_at,
          preparation_minutes, travel_minutes, earliest_useful_at, last_responsible_at,
          plan_version, notification_mode, destination_conversation_id,
          participant_epoch_id, participant_set_digest, audience, source_evidence_refs,
          routine_occurrence_id, routine_occurrence_version, routine_id, routine_revision,
          attention_cycle, notification_history, last_transition_at
        ) values (
          ${loop.loopId}, ${loop.householdId}, ${loop.version}, ${loop.state},
          ${canonicalDigest(loop.minimumSharedMeaning)}, ${encrypted.ciphertext}, ${encrypted.keyVersion},
          ${transaction.json([])}, ${loop.proposedHolderPersonId},
          ${loop.acknowledgment?.personId ?? null},
          ${loop.acknowledgment ? new Date(loop.acknowledgment.acknowledgedAt) : null},
          ${loop.acknowledgment?.kind ?? null}, ${loop.acknowledgment?.holderDisclosure ?? null},
          ${loop.timing.timeZone}, ${loop.timing.localDate},
          ${asDate(loop.timing.eventAt)}, ${asDate(loop.timing.deadlineAt)},
          ${loop.timing.preparationMinutes}, ${loop.timing.travelMinutes},
          ${new Date(loop.timing.earliestUsefulAt)}, ${new Date(loop.timing.lastResponsibleAt)},
          ${loop.planVersion}, ${loop.notificationMode}, ${loop.destination.conversationId},
          ${loop.destination.participantEpochId}, ${loop.destination.participantSetDigest},
          ${loop.destination.audience}, ${transaction.json(loop.sourceEvidenceRefs)},
          ${loop.routineOccurrence?.occurrenceId ?? null},
          ${loop.routineOccurrence?.occurrenceVersion ?? null},
          ${loop.routineOccurrence?.routineId ?? null},
          ${loop.routineOccurrence?.routineRevision ?? null}, ${loop.attentionCycle},
          ${transaction.json(loop.notificationHistory)}, ${new Date(loop.lastTransitionAt)}
        )
      `;
      await replaceParticipants(transaction, loop);
      return loop;
    });
  }

  public async load(loopIdCandidate: string): Promise<CoverageLoop | null> {
    const loopId = EntityIdSchema.parse(loopIdCandidate);
    return inTransaction(this.database, async (transaction) => {
      const rows = await transaction<CoverageRow[]>`select * from coverage_loops where id = ${loopId}`;
      return rows[0] ? hydrateLoop(rows[0], this.secretBox) : null;
    });
  }

  /** Locks the aggregate so a timer can reauthorize and plan against one exact version. */
  public async loadForUpdate(loopIdCandidate: string): Promise<CoverageLoop | null> {
    const loopId = EntityIdSchema.parse(loopIdCandidate);
    return inTransaction(this.database, async (transaction) => {
      const rows = await transaction<CoverageRow[]>`
        select * from coverage_loops where id = ${loopId} for update
      `;
      return rows[0] ? hydrateLoop(rows[0], this.secretBox) : null;
    });
  }

  public async transition(input: PersistedCoverageCommand): Promise<CoverageTransitionDecision> {
    const loopId = EntityIdSchema.parse(input.loopId);
    return inTransaction(this.database, async (transaction) => {
      const rows = await transaction<CoverageRow[]>`
        select * from coverage_loops where id = ${loopId} for update
      `;
      const row = rows[0];
      if (!row) throw new ConflictError("Coverage loop does not exist");
      const current = hydrateLoop(row, this.secretBox);
      const decision = transitionCoverage(current, input.command);
      const encrypted = encryptContent(this.secretBox, decision.loop);
      const updated = await transaction<{ readonly id: string }[]>`
        update coverage_loops set
          version = ${decision.loop.version}, state = ${decision.loop.state},
          minimum_shared_meaning_digest = ${canonicalDigest(decision.loop.minimumSharedMeaning)},
          content_ciphertext = ${encrypted.ciphertext}, content_key_version = ${encrypted.keyVersion},
          unresolved_facts = ${transaction.json([])},
          proposed_holder_person_id = ${decision.loop.proposedHolderPersonId},
          acknowledged_by_person_id = ${decision.loop.acknowledgment?.personId ?? null},
          acknowledged_at = ${decision.loop.acknowledgment ? new Date(decision.loop.acknowledgment.acknowledgedAt) : null},
          acknowledgment_kind = ${decision.loop.acknowledgment?.kind ?? null},
          holder_disclosure = ${decision.loop.acknowledgment?.holderDisclosure ?? null},
          plan_version = ${decision.loop.planVersion},
          notification_mode = ${decision.loop.notificationMode},
          destination_conversation_id = ${decision.loop.destination.conversationId},
          participant_epoch_id = ${decision.loop.destination.participantEpochId},
          participant_set_digest = ${decision.loop.destination.participantSetDigest},
          audience = ${decision.loop.destination.audience},
          source_evidence_refs = ${transaction.json(decision.loop.sourceEvidenceRefs)},
          attention_cycle = ${decision.loop.attentionCycle},
          notification_history = ${transaction.json(decision.loop.notificationHistory)},
          last_transition_at = ${new Date(decision.loop.lastTransitionAt)},
          updated_at = ${new Date(decision.loop.lastTransitionAt)}
        where id = ${loopId} and version = ${current.version}
        returning id
      `;
      if (!updated[0]) throw new ConflictError("Coverage loop version changed");
      await insertTransition(transaction, decision.transition);
      await replaceParticipants(transaction, decision.loop);
      return decision;
    });
  }

  /** Persists only the exact notification-history advance produced by the pure planner. */
  public async recordNotification(inputCandidate: PersistedCoverageNotification): Promise<CoverageLoop> {
    const loopId = EntityIdSchema.parse(inputCandidate.loopId);
    const planned = CoverageLoopSchema.parse(inputCandidate.decision.loop);
    const plan = NeutralNotificationPlanSchema.parse(inputCandidate.decision.plan);
    if (planned.loopId !== loopId || plan.loopId !== loopId) {
      throw new ConflictError("Coverage notification targets a different loop");
    }
    return inTransaction(this.database, async (transaction) => {
      const rows = await transaction<CoverageRow[]>`
        select * from coverage_loops where id = ${loopId} for update
      `;
      const row = rows[0];
      if (!row) throw new ConflictError("Coverage loop does not exist");
      const current = hydrateLoop(row, this.secretBox);
      assertNotificationAdvance(current, planned, plan, inputCandidate.expectedVersion);
      const updated = await transaction<{ readonly id: string }[]>`
        update coverage_loops set
          version = ${planned.version},
          notification_history = ${transaction.json(planned.notificationHistory)},
          updated_at = now()
        where id = ${loopId} and version = ${current.version}
        returning id
      `;
      if (!updated[0]) throw new ConflictError("Coverage loop version changed");
      await replaceParticipants(transaction, planned);
      return planned;
    });
  }
}

function assertNotificationAdvance(
  current: CoverageLoop,
  planned: CoverageLoop,
  plan: NeutralNotificationPlan,
  expectedVersion: number,
): void {
  if (current.version !== expectedVersion || planned.version !== current.version + 1) {
    throw new ConflictError("Coverage notification version changed");
  }
  const currentWithoutMutable = notificationImmutableView(current);
  const plannedWithoutMutable = notificationImmutableView(planned);
  if (canonicalDigest(currentWithoutMutable) !== canonicalDigest(plannedWithoutMutable)) {
    throw new ConflictError("Coverage notification attempted a state change");
  }
  if (
    planned.notificationHistory.length !== current.notificationHistory.length + 1 ||
    canonicalDigest(planned.notificationHistory.slice(0, -1)) !== canonicalDigest(current.notificationHistory)
  ) {
    throw new ConflictError("Coverage notification history is not append-only");
  }
  const appended = planned.notificationHistory.at(-1);
  if (
    !appended ||
    plan.loopVersion !== planned.version ||
    plan.planVersion !== planned.planVersion ||
    plan.attentionCycle !== planned.attentionCycle ||
    canonicalDigest(plan.destination) !== canonicalDigest(planned.destination) ||
    appended.notificationId !== plan.notificationId ||
    appended.category !== plan.category ||
    appended.cycle !== plan.attentionCycle ||
    appended.sentAt !== plan.sendAt
  ) {
    throw new ConflictError("Coverage notification plan does not match its history record");
  }
}

function notificationImmutableView(loop: CoverageLoop) {
  const { version: _version, notificationHistory: _notificationHistory, ...immutable } = loop;
  return immutable;
}

function encryptContent(secretBox: SecretBox, loop: CoverageLoop) {
  const encrypted = secretBox.encrypt(
    JSON.stringify({
      minimumSharedMeaning: loop.minimumSharedMeaning,
      unresolvedFacts: loop.unresolvedFacts,
    }),
    "coverage-loop-content",
  );
  return {
    ciphertext: Buffer.from(JSON.stringify(encrypted), "utf8"),
    keyVersion: encrypted.kid,
  };
}

function hydrateLoop(row: CoverageRow, secretBox: SecretBox): CoverageLoop {
  const content = coverageContentSchema.parse(
    JSON.parse(
      secretBox
        .decrypt(JSON.parse(row.content_ciphertext.toString("utf8")), "coverage-loop-content")
        .toString("utf8"),
    ),
  );
  return CoverageLoopSchema.parse({
    loopId: row.id,
    householdId: row.household_id,
    version: Number(row.version),
    state: row.state,
    minimumSharedMeaning: content.minimumSharedMeaning,
    unresolvedFacts: content.unresolvedFacts,
    proposedHolderPersonId: row.proposed_holder_person_id,
    acknowledgment: row.acknowledged_by_person_id
      ? {
          personId: row.acknowledged_by_person_id,
          acknowledgedAt: requiredDate(row.acknowledged_at).toISOString(),
          kind: row.acknowledgment_kind,
          holderDisclosure: row.holder_disclosure,
        }
      : null,
    timing: {
      timeZone: row.time_zone,
      localDate:
        typeof row.local_date === "string" ? row.local_date : row.local_date.toISOString().slice(0, 10),
      eventAt: row.event_at?.toISOString() ?? null,
      deadlineAt: row.deadline_at?.toISOString() ?? null,
      preparationMinutes: row.preparation_minutes,
      travelMinutes: row.travel_minutes,
      earliestUsefulAt: row.earliest_useful_at.toISOString(),
      lastResponsibleAt: row.last_responsible_at.toISOString(),
      resolutionPolicy: "wall_clock_compatible",
    },
    planVersion: Number(row.plan_version),
    notificationMode: row.notification_mode,
    destination: {
      conversationId: row.destination_conversation_id,
      participantEpochId: row.participant_epoch_id,
      participantSetDigest: row.participant_set_digest,
      audience: row.audience,
    },
    sourceEvidenceRefs: row.source_evidence_refs,
    routineOccurrence: row.routine_occurrence_id
      ? {
          occurrenceId: row.routine_occurrence_id,
          occurrenceVersion: Number(row.routine_occurrence_version),
          routineId: row.routine_id,
          routineRevision: Number(row.routine_revision),
        }
      : null,
    attentionCycle: row.attention_cycle,
    notificationHistory: row.notification_history,
    lastTransitionAt: row.last_transition_at.toISOString(),
  });
}

async function insertTransition(transaction: Transaction, transitionCandidate: CoverageTransition) {
  const transition = CoverageTransitionSchema.parse(transitionCandidate);
  await transaction`
    insert into coverage_transitions (
      id, coverage_loop_id, from_state, to_state, from_version, to_version,
      transition_kind, actor_person_id, evidence_refs, occurred_at
    ) values (
      ${transition.transitionId}, ${transition.loopId}, ${transition.fromState}, ${transition.toState},
      ${transition.fromVersion}, ${transition.toVersion}, ${transition.kind},
      ${transition.actorPersonId}, ${transaction.json(transition.evidenceRefs)},
      ${new Date(transition.occurredAt)}
    )
  `;
}

async function replaceParticipants(transaction: Transaction, loop: CoverageLoop) {
  await transaction`
    update coverage_participants set active = false
    where coverage_loop_id = ${loop.loopId} and active
  `;
  if (loop.proposedHolderPersonId) {
    await transaction`
      insert into coverage_participants (
        coverage_loop_id, person_id, participation_kind, loop_version, active, recorded_at
      ) values (
        ${loop.loopId}, ${loop.proposedHolderPersonId}, 'proposed_holder',
        ${loop.version}, true, ${new Date(loop.lastTransitionAt)}
      ) on conflict do nothing
    `;
  }
  if (loop.acknowledgment) {
    await transaction`
      insert into coverage_participants (
        coverage_loop_id, person_id, participation_kind, loop_version, active, recorded_at
      ) values (
        ${loop.loopId}, ${loop.acknowledgment.personId}, 'acknowledged_holder',
        ${loop.version}, true, ${new Date(loop.lastTransitionAt)}
      ) on conflict do nothing
    `;
  }
}

function inTransaction<Result>(
  executor: Executor,
  operation: (transaction: Transaction) => Promise<Result>,
): Promise<Result> {
  return "begin" in executor
    ? (executor.begin(operation) as unknown as Promise<Result>)
    : operation(executor);
}

function asDate(value: string | null): Date | null {
  return value === null ? null : new Date(value);
}

function requiredDate(value: Date | null): Date {
  if (value === null) throw new ConflictError("Coverage acknowledgment is incomplete");
  return value;
}
