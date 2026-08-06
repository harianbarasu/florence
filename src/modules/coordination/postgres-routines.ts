import { randomUUID } from "node:crypto";
import { Temporal } from "@js-temporal/polyfill";
import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { Database } from "../../db/client.js";
import { canonicalDigest } from "../../shared/canonical-json.js";
import type { SecretBox } from "../../shared/crypto.js";
import { ConflictError, NotFoundError, StaleAuthorityError, UnauthorizedError } from "../../shared/errors.js";
import { DurableTimers } from "../work/timers.js";
import {
  type CoverageLoop,
  DestinationEpochSchema,
  EntityIdSchema,
  type Routine,
  type RoutineOccurrence,
  RoutineOccurrenceSchema,
  type RoutineRevision,
  RoutineRevisionSchema,
  RoutineSchema,
} from "./contracts.js";
import { createCoverageLoopFromOccurrence } from "./coverage.js";
import { PostgresCoordination } from "./postgres-coordination.js";
import {
  materializeRoutineOccurrence,
  reviseRoutineOccurrence,
  routineRevisionIncludesDate,
} from "./routines.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

const MAX_WINDOW_DAYS = 31;
const ROUTINE_SCAN_PAGE_SIZE = 500;

const routineRevisionContentSchema = z.strictObject({
  title: z.string(),
  minimumSharedMeaning: z.string(),
});

const RoutineRevisionDraftSchema = z.strictObject(RoutineRevisionSchema.shape).omit({
  routineId: true,
  revision: true,
  createdAt: true,
  createdByPersonId: true,
});
export type RoutineRevisionDraft = z.input<typeof RoutineRevisionDraftSchema>;

export const SaveRoutineInputSchema = z.discriminatedUnion("kind", [
  z.strictObject({
    kind: z.literal("create"),
    routineId: EntityIdSchema,
    householdId: EntityIdSchema,
    actorPersonId: EntityIdSchema,
    occurredAt: z.date(),
    revision: RoutineRevisionDraftSchema,
  }),
  z.strictObject({
    kind: z.literal("revise"),
    routineId: EntityIdSchema,
    householdId: EntityIdSchema,
    expectedVersion: z.number().int().positive(),
    actorPersonId: EntityIdSchema,
    occurredAt: z.date(),
    revision: RoutineRevisionDraftSchema,
  }),
]);
export type SaveRoutineInput = z.input<typeof SaveRoutineInputSchema>;

export interface SavedRoutine {
  readonly routine: Routine;
  readonly revision: RoutineRevision;
}

export const SetRoutineStatusInputSchema = z.strictObject({
  routineId: EntityIdSchema,
  householdId: EntityIdSchema,
  actorPersonId: EntityIdSchema,
  expectedVersion: z.number().int().positive(),
  status: z.enum(["active", "paused", "retired"]),
  occurredAt: z.date(),
});
export type SetRoutineStatusInput = z.input<typeof SetRoutineStatusInputSchema>;

export interface SetRoutineStatusResult {
  readonly routine: Routine;
  readonly duplicate: boolean;
  readonly affectedOccurrenceCount: number;
  readonly transitionedLoopIds: readonly string[];
  /** Newly reactivated coverage loops that need their opening timers reconciled. */
  readonly coverage: readonly MaterializedRoutineCoverage[];
}

export const MaterializeRoutineWindowInputSchema = z
  .strictObject({
    fromLocalDate: z.iso.date(),
    throughLocalDate: z.iso.date(),
    materializedAt: z.date(),
    afterRoutineId: EntityIdSchema.nullable().default(null),
    maxOccurrences: z.number().int().min(MAX_WINDOW_DAYS).max(2_000).default(500),
  })
  .superRefine((input, context) => {
    const from = Temporal.PlainDate.from(input.fromLocalDate);
    const through = Temporal.PlainDate.from(input.throughLocalDate);
    const days = from.until(through, { largestUnit: "days" }).days;
    if (days < 0) {
      context.addIssue({
        code: "custom",
        path: ["throughLocalDate"],
        message: "Routine materialization cannot end before it starts",
      });
    } else if (days >= MAX_WINDOW_DAYS) {
      context.addIssue({
        code: "custom",
        path: ["throughLocalDate"],
        message: `Routine materialization is limited to ${MAX_WINDOW_DAYS} inclusive days`,
      });
    }
  });
export type MaterializeRoutineWindowInput = z.input<typeof MaterializeRoutineWindowInputSchema>;

export interface MaterializedRoutineCoverage {
  readonly occurrence: RoutineOccurrence;
  readonly loop: CoverageLoop;
  readonly occurrenceCreated: boolean;
  readonly loopCreated: boolean;
}

export interface StaleRoutineDestination {
  readonly routineId: string;
  readonly reason: "destination_epoch_stale" | "standing_holder_inactive";
}

export interface MaterializeRoutineWindowResult {
  readonly coverage: readonly MaterializedRoutineCoverage[];
  readonly stale: readonly StaleRoutineDestination[];
  /** Pass this exact value as `afterRoutineId` until it returns null. */
  readonly nextRoutineCursor: string | null;
}

interface RoutineRow {
  readonly id: string;
  readonly household_id: string;
  readonly status: string;
  readonly current_revision: number | string;
  readonly version: number | string;
}

interface RoutineRevisionRow {
  readonly routine_id: string;
  readonly revision: number | string;
  readonly content_ciphertext: Buffer;
  readonly content_key_version: string;
  readonly recurrence: unknown;
  readonly semantic_time_plan: unknown;
  readonly notification_mode: string;
  readonly destination_conversation_id: string;
  readonly participant_epoch_id: string;
  readonly participant_set_digest: string;
  readonly audience: string;
  readonly proposed_holder_person_id: string | null;
  readonly standing_holder_person_id: string | null;
  readonly standing_authorized_by_person_id: string | null;
  readonly standing_authorization_kind: string | null;
  readonly standing_authorized_at: Date | null;
  readonly source_revision_refs: unknown;
  readonly effective_from: string | Date;
  readonly effective_through: string | Date | null;
  readonly created_at: Date;
  readonly created_by_person_id: string;
}

interface RoutineOccurrenceRow {
  readonly id: string;
  readonly materialization_key: string;
  readonly routine_id: string;
  readonly routine_revision: number | string;
  readonly local_date: string | Date;
  readonly version: number | string;
  readonly supersedes_version: number | string | null;
  readonly plan_version: number | string;
  readonly status: string;
  readonly content_ciphertext: Buffer;
  readonly content_key_version: string;
  readonly time_zone: string;
  readonly event_at: Date | null;
  readonly deadline_at: Date | null;
  readonly preparation_minutes: number;
  readonly travel_minutes: number;
  readonly earliest_useful_at: Date;
  readonly last_responsible_at: Date;
  readonly notification_mode: string;
  readonly destination_conversation_id: string;
  readonly participant_epoch_id: string;
  readonly participant_set_digest: string;
  readonly audience: string;
  readonly proposed_holder_person_id: string | null;
  readonly standing_holder_person_id: string | null;
  readonly standing_authorized_by_person_id: string | null;
  readonly standing_authorization_kind: string | null;
  readonly standing_authorized_at: Date | null;
  readonly source_revision_refs: unknown;
  readonly materialized_at: Date;
}

interface CoverageLoopVersionRow {
  readonly id: string;
  readonly version: number | string;
}

interface OccurrenceReconciliation {
  readonly affectedOccurrenceCount: number;
  readonly transitionedLoopIds: readonly string[];
  readonly coverage: readonly MaterializedRoutineCoverage[];
}

type ProcessRoutineResult =
  | {
      readonly kind: "processed";
      readonly coverage: readonly MaterializedRoutineCoverage[];
      readonly stale: readonly StaleRoutineDestination[];
    }
  | { readonly kind: "budget_exhausted" };

/**
 * Canonical routine persistence and materialization. The interface deliberately
 * hides revision numbering, encryption, destination fencing, occurrence
 * idempotency, and coverage-loop creation from every caller.
 */
export class PostgresRoutines {
  public constructor(
    private readonly database: Executor,
    private readonly secretBox: SecretBox,
  ) {}

  public async save(inputCandidate: SaveRoutineInput): Promise<SavedRoutine> {
    const input = SaveRoutineInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      await lockActiveHousehold(transaction, input.householdId);
      await requireOriginator(transaction, input.householdId, input.actorPersonId);

      const current = await loadCurrentRoutine(transaction, input.routineId, this.secretBox, true);
      if (input.kind === "create" && current !== null) {
        throw new ConflictError("Routine already exists");
      }
      if (input.kind === "revise") {
        if (current === null || current.routine.householdId !== input.householdId) {
          throw new NotFoundError("Routine does not exist in this family");
        }
        if (current.routine.version !== input.expectedVersion) {
          throw new ConflictError("Routine version changed");
        }
        if (current.routine.status === "retired") {
          throw new ConflictError("A retired routine cannot be revised");
        }
      }

      await requireExactDestination(transaction, {
        householdId: input.householdId,
        actorPersonId: input.actorPersonId,
        destination: input.revision.destination,
      });
      await requireStandingAuthorization(transaction, {
        kind: input.kind,
        householdId: input.householdId,
        actorPersonId: input.actorPersonId,
        occurredAt: input.occurredAt,
        proposed: input.revision.standingCoverage,
      });

      const revisionNumber = current === null ? 1 : current.routine.currentRevision + 1;
      const routineVersion = current === null ? 1 : current.routine.version + 1;
      const revision = RoutineRevisionSchema.parse({
        ...input.revision,
        routineId: input.routineId,
        revision: revisionNumber,
        createdAt: input.occurredAt.toISOString(),
        createdByPersonId: input.actorPersonId,
      });
      const routine = RoutineSchema.parse({
        routineId: input.routineId,
        householdId: input.householdId,
        version: routineVersion,
        currentRevision: revisionNumber,
        status: current?.routine.status ?? "active",
      });

      if (current === null) {
        await transaction`
          insert into routines (
            id, household_id, status, current_revision, version, created_at, updated_at
          ) values (
            ${routine.routineId}, ${routine.householdId}, ${routine.status},
            ${routine.currentRevision}, ${routine.version}, ${input.occurredAt}, ${input.occurredAt}
          )
        `;
      }
      await insertRevision(transaction, this.secretBox, revision);
      if (current !== null) {
        const updated = await transaction<{ readonly id: string }[]>`
          update routines set
            current_revision = ${routine.currentRevision}, version = ${routine.version},
            updated_at = ${input.occurredAt}
          where id = ${routine.routineId} and household_id = ${routine.householdId}
            and version = ${current.routine.version}
          returning id
        `;
        if (!updated[0]) throw new ConflictError("Routine version changed");
      }
      const reconciliation =
        current === null
          ? emptyReconciliation()
          : await reconcileFutureRevisionOccurrences({
              transaction,
              secretBox: this.secretBox,
              routine,
              previousRevision: current.revision.revision,
              actorPersonId: input.actorPersonId,
              occurredAt: input.occurredAt,
            });
      await appendRoutineAudit(transaction, {
        householdId: routine.householdId,
        actorKind: "person",
        actorPersonId: input.actorPersonId,
        eventType: current === null ? "routine_created" : "routine_revised",
        targetType: "routine",
        targetId: routine.routineId,
        reasonCodes: [
          `routine_revision:${revision.revision}`,
          `destination_epoch:${revision.destination.participantEpochId}`,
          `occurrences_reconciled:${reconciliation.affectedOccurrenceCount}`,
          `coverage_loops_reconciled:${reconciliation.transitionedLoopIds.length}`,
        ],
        occurredAt: input.occurredAt,
      });
      return { routine, revision };
    });
  }

  /**
   * Pauses, resumes, or permanently retires a routine while keeping every
   * already-useful occurrence intact. All future occurrence, loop, and timer
   * changes commit atomically with the routine status.
   */
  public async setStatus(inputCandidate: SetRoutineStatusInput): Promise<SetRoutineStatusResult> {
    const input = SetRoutineStatusInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      await lockActiveHousehold(transaction, input.householdId);
      await requireCoordinator(transaction, input.householdId, input.actorPersonId);

      const current = await loadCurrentRoutine(transaction, input.routineId, this.secretBox, true);
      if (current === null || current.routine.householdId !== input.householdId) {
        throw new NotFoundError("Routine does not exist in this family");
      }
      if (current.routine.version !== input.expectedVersion) {
        throw new ConflictError("Routine version changed");
      }
      if (current.routine.status === input.status) {
        return {
          routine: current.routine,
          duplicate: true,
          affectedOccurrenceCount: 0,
          transitionedLoopIds: [],
          coverage: [],
        };
      }
      if (current.routine.status === "retired") {
        throw new ConflictError("A retired routine cannot be resumed or paused");
      }
      if (input.status === "active") {
        await requireExactDestination(transaction, {
          householdId: input.householdId,
          actorPersonId: input.actorPersonId,
          destination: current.revision.destination,
        });
      }

      const routine = RoutineSchema.parse({
        ...current.routine,
        version: current.routine.version + 1,
        status: input.status,
      });
      const updated = await transaction<{ readonly id: string }[]>`
        update routines set status = ${routine.status}, version = ${routine.version},
          updated_at = ${input.occurredAt}
        where id = ${routine.routineId} and household_id = ${routine.householdId}
          and version = ${current.routine.version}
        returning id
      `;
      if (!updated[0]) throw new ConflictError("Routine version changed");

      const reconciliation = await reconcileFutureStatusOccurrences({
        transaction,
        secretBox: this.secretBox,
        routine,
        revision: current.revision,
        actorPersonId: input.actorPersonId,
        occurredAt: input.occurredAt,
      });
      await appendRoutineAudit(transaction, {
        householdId: routine.householdId,
        actorKind: "person",
        actorPersonId: input.actorPersonId,
        eventType:
          routine.status === "active"
            ? "routine_resumed"
            : routine.status === "paused"
              ? "routine_paused"
              : "routine_retired",
        targetType: "routine",
        targetId: routine.routineId,
        reasonCodes: [
          `routine_status:${routine.status}`,
          `routine_version:${routine.version}`,
          `occurrences_reconciled:${reconciliation.affectedOccurrenceCount}`,
          `coverage_loops_reconciled:${reconciliation.transitionedLoopIds.length}`,
        ],
        occurredAt: input.occurredAt,
      });
      return {
        routine,
        duplicate: false,
        ...reconciliation,
      };
    });
  }

  public async materializeDue(
    inputCandidate: MaterializeRoutineWindowInput,
  ): Promise<MaterializeRoutineWindowResult> {
    const input = MaterializeRoutineWindowInputSchema.parse(inputCandidate);
    const afterRoutineId = input.afterRoutineId ?? null;
    const candidates = await this.database<{ readonly id: string }[]>`
      select routine.id
      from routines routine
      join households household on household.id = routine.household_id
      where routine.status = 'active' and household.status = 'active'
        and (${afterRoutineId}::uuid is null or routine.id > ${afterRoutineId})
      order by routine.id
      limit ${ROUTINE_SCAN_PAGE_SIZE + 1}
    `;

    const coverage: MaterializedRoutineCoverage[] = [];
    const stale: StaleRoutineDestination[] = [];
    let lastProcessedRoutineId = afterRoutineId;
    let stoppedForBudget = false;
    const page = candidates.slice(0, ROUTINE_SCAN_PAGE_SIZE);

    for (const candidate of page) {
      const result = await inTransaction(this.database, async (transaction) =>
        this.processRoutine(transaction, candidate.id, input, input.maxOccurrences - coverage.length),
      );
      if (result.kind === "budget_exhausted") {
        stoppedForBudget = true;
        break;
      }
      coverage.push(...result.coverage);
      stale.push(...result.stale);
      lastProcessedRoutineId = candidate.id;
    }

    const hasMore = stoppedForBudget || candidates.length > ROUTINE_SCAN_PAGE_SIZE;
    return {
      coverage,
      stale,
      nextRoutineCursor: hasMore ? lastProcessedRoutineId : null,
    };
  }

  private async processRoutine(
    transaction: Transaction,
    routineId: string,
    input: z.output<typeof MaterializeRoutineWindowInputSchema>,
    remainingBudget: number,
  ): Promise<ProcessRoutineResult> {
    const householdRows = await transaction<{ readonly household_id: string }[]>`
      select routine.household_id
      from routines routine
      join households household on household.id = routine.household_id
      where routine.id = ${routineId} and routine.status = 'active' and household.status = 'active'
    `;
    const householdId = householdRows[0]?.household_id;
    if (!householdId) return { kind: "processed", coverage: [], stale: [] };
    await lockActiveHousehold(transaction, householdId);

    const current = await loadCurrentRoutine(transaction, routineId, this.secretBox, true);
    if (current === null || current.routine.status !== "active") {
      return { kind: "processed", coverage: [], stale: [] };
    }
    const dates = enumerateDates(input.fromLocalDate, input.throughLocalDate).filter((date) =>
      routineRevisionIncludesDate(current.revision, date),
    );
    if (dates.length > remainingBudget) return { kind: "budget_exhausted" };

    const destinationReady = await isExactDestinationCurrent(transaction, {
      householdId,
      actorPersonId: current.revision.createdByPersonId,
      destination: current.revision.destination,
      requireActor: false,
    });
    if (!destinationReady) {
      return {
        kind: "processed",
        coverage: [],
        stale: [{ routineId, reason: "destination_epoch_stale" }],
      };
    }

    const standingHolderActive = await isStandingHolderActive(
      transaction,
      householdId,
      current.revision.standingCoverage?.holderPersonId ?? null,
    );
    const revisionForMaterialization =
      current.revision.standingCoverage !== null && !standingHolderActive
        ? RoutineRevisionSchema.parse({ ...current.revision, standingCoverage: null })
        : current.revision;
    const materializedCoverage: MaterializedRoutineCoverage[] = [];

    for (const localDate of dates) {
      const occurrenceCandidate = materializeRoutineOccurrence({
        occurrenceId: randomUUID(),
        routine: current.routine,
        revision: revisionForMaterialization,
        localDate,
        materializedAt: input.materializedAt.toISOString(),
      });
      const persisted = await persistOccurrence(transaction, this.secretBox, occurrenceCandidate);
      if (persisted.occurrence.status !== "materialized") continue;
      const coordination = new PostgresCoordination(transaction, this.secretBox);
      const existingLoopRows = await transaction<{ readonly id: string }[]>`
        select id from coverage_loops
        where routine_occurrence_id = ${persisted.occurrence.occurrenceId}
          and routine_occurrence_version = ${persisted.occurrence.version}
      `;
      let loop: CoverageLoop;
      let loopCreated = false;
      if (existingLoopRows[0]) {
        const loaded = await coordination.load(existingLoopRows[0].id);
        if (!loaded) throw new ConflictError("Routine coverage loop disappeared");
        loop = loaded;
      } else {
        loop = createCoverageLoopFromOccurrence({
          loopId: randomUUID(),
          householdId,
          occurrence: persisted.occurrence,
        });
        loop = await coordination.create(loop);
        loopCreated = true;
      }
      if (persisted.created || loopCreated) {
        await appendRoutineAudit(transaction, {
          householdId,
          actorKind: "application",
          actorPersonId: null,
          eventType: "routine_occurrence_materialized",
          targetType: "routine_occurrence",
          targetId: persisted.occurrence.occurrenceId,
          reasonCodes: [
            `routine:${routineId}`,
            `routine_revision:${persisted.occurrence.routineRevision}`,
            `coverage_loop:${loop.loopId}`,
          ],
          occurredAt: input.materializedAt,
        });
      }
      materializedCoverage.push({
        occurrence: persisted.occurrence,
        loop,
        occurrenceCreated: persisted.created,
        loopCreated,
      });
    }

    return {
      kind: "processed",
      coverage: materializedCoverage,
      stale:
        current.revision.standingCoverage !== null && !standingHolderActive
          ? [{ routineId, reason: "standing_holder_inactive" }]
          : [],
    };
  }
}

async function insertRevision(
  transaction: Transaction,
  secretBox: SecretBox,
  revision: RoutineRevision,
): Promise<void> {
  const encrypted = sealRoutineRevision(secretBox, revision);
  await transaction`
    insert into routine_revisions (
      id, routine_id, revision, title_digest, minimum_shared_meaning_digest,
      content_ciphertext, content_key_version, recurrence, semantic_time_plan,
      notification_mode, destination_conversation_id, participant_epoch_id,
      participant_set_digest, audience, proposed_holder_person_id,
      standing_holder_person_id, standing_authorized_by_person_id,
      standing_authorization_kind, standing_authorized_at, source_revision_refs,
      effective_from, effective_through, created_at, created_by_person_id
    ) values (
      ${randomUUID()}, ${revision.routineId}, ${revision.revision},
      ${canonicalDigest(revision.title)}, ${canonicalDigest(revision.minimumSharedMeaning)},
      ${encrypted.ciphertext}, ${encrypted.keyVersion},
      ${transaction.json(revision.recurrence)}, ${transaction.json(revision.timePlan)},
      ${revision.notificationMode}, ${revision.destination.conversationId},
      ${revision.destination.participantEpochId}, ${revision.destination.participantSetDigest},
      ${revision.destination.audience}, ${revision.proposedHolderPersonId},
      ${revision.standingCoverage?.holderPersonId ?? null},
      ${revision.standingCoverage?.authorizedByPersonId ?? null},
      ${revision.standingCoverage?.authorizationKind ?? null},
      ${revision.standingCoverage ? new Date(revision.standingCoverage.authorizedAt) : null},
      ${transaction.json(revision.sourceRevisionRefs)}, ${revision.effectiveFrom},
      ${revision.effectiveThrough}, ${new Date(revision.createdAt)}, ${revision.createdByPersonId}
    )
  `;
}

async function persistOccurrence(
  transaction: Transaction,
  secretBox: SecretBox,
  occurrence: RoutineOccurrence,
): Promise<{ readonly occurrence: RoutineOccurrence; readonly created: boolean }> {
  const encrypted = sealRoutineOccurrence(secretBox, occurrence);
  const inserted = await transaction<{ readonly id: string }[]>`
    insert into routine_occurrences (
      id, routine_id, materialization_key, routine_revision, local_date, version,
      supersedes_version, plan_version, status, title_digest,
      minimum_shared_meaning_digest, content_ciphertext, content_key_version,
      time_zone, event_at, deadline_at, preparation_minutes, travel_minutes,
      earliest_useful_at, last_responsible_at, notification_mode,
      destination_conversation_id, participant_epoch_id, participant_set_digest,
      audience, proposed_holder_person_id, standing_holder_person_id,
      standing_authorized_by_person_id, standing_authorization_kind,
      standing_authorized_at, source_revision_refs, materialized_at
    ) values (
      ${occurrence.occurrenceId}, ${occurrence.routineId}, ${occurrence.materializationKey},
      ${occurrence.routineRevision}, ${occurrence.localDate}, ${occurrence.version},
      ${occurrence.supersedesVersion}, ${occurrence.planVersion}, ${occurrence.status},
      ${canonicalDigest(occurrence.title)}, ${canonicalDigest(occurrence.minimumSharedMeaning)},
      ${encrypted.ciphertext}, ${encrypted.keyVersion}, ${occurrence.timing.timeZone},
      ${asDate(occurrence.timing.eventAt)}, ${asDate(occurrence.timing.deadlineAt)},
      ${occurrence.timing.preparationMinutes}, ${occurrence.timing.travelMinutes},
      ${new Date(occurrence.timing.earliestUsefulAt)},
      ${new Date(occurrence.timing.lastResponsibleAt)}, ${occurrence.notificationMode},
      ${occurrence.destination.conversationId}, ${occurrence.destination.participantEpochId},
      ${occurrence.destination.participantSetDigest}, ${occurrence.destination.audience},
      ${occurrence.proposedHolderPersonId},
      ${occurrence.standingCoverage?.holderPersonId ?? null},
      ${occurrence.standingCoverage?.authorizedByPersonId ?? null},
      ${occurrence.standingCoverage?.authorizationKind ?? null},
      ${occurrence.standingCoverage ? new Date(occurrence.standingCoverage.authorizedAt) : null},
      ${transaction.json(occurrence.sourceRevisionRefs)}, ${new Date(occurrence.materializedAt)}
    ) on conflict (materialization_key) do nothing
    returning id
  `;
  if (inserted[0]) return { occurrence, created: true };
  const rows = await transaction<RoutineOccurrenceRow[]>`
    select * from routine_occurrences where materialization_key = ${occurrence.materializationKey}
  `;
  const existing = rows[0];
  if (!existing) throw new ConflictError("Routine occurrence idempotency record disappeared");
  return { occurrence: hydrateOccurrence(existing, secretBox), created: false };
}

async function persistOccurrenceUpdate(
  transaction: Transaction,
  secretBox: SecretBox,
  previousVersion: number,
  occurrenceCandidate: RoutineOccurrence,
): Promise<void> {
  const occurrence = RoutineOccurrenceSchema.parse(occurrenceCandidate);
  if (occurrence.version !== previousVersion + 1 || occurrence.supersedesVersion !== previousVersion) {
    throw new ConflictError("Routine occurrence revision is not consecutive");
  }
  const encrypted = sealRoutineOccurrence(secretBox, occurrence);
  const updated = await transaction<{ readonly id: string }[]>`
    update routine_occurrences set
      version = ${occurrence.version}, supersedes_version = ${occurrence.supersedesVersion},
      plan_version = ${occurrence.planVersion}, status = ${occurrence.status},
      title_digest = ${canonicalDigest(occurrence.title)},
      minimum_shared_meaning_digest = ${canonicalDigest(occurrence.minimumSharedMeaning)},
      content_ciphertext = ${encrypted.ciphertext}, content_key_version = ${encrypted.keyVersion},
      time_zone = ${occurrence.timing.timeZone}, event_at = ${asDate(occurrence.timing.eventAt)},
      deadline_at = ${asDate(occurrence.timing.deadlineAt)},
      preparation_minutes = ${occurrence.timing.preparationMinutes},
      travel_minutes = ${occurrence.timing.travelMinutes},
      earliest_useful_at = ${new Date(occurrence.timing.earliestUsefulAt)},
      last_responsible_at = ${new Date(occurrence.timing.lastResponsibleAt)},
      notification_mode = ${occurrence.notificationMode},
      destination_conversation_id = ${occurrence.destination.conversationId},
      participant_epoch_id = ${occurrence.destination.participantEpochId},
      participant_set_digest = ${occurrence.destination.participantSetDigest},
      audience = ${occurrence.destination.audience},
      proposed_holder_person_id = ${occurrence.proposedHolderPersonId},
      standing_holder_person_id = ${occurrence.standingCoverage?.holderPersonId ?? null},
      standing_authorized_by_person_id = ${occurrence.standingCoverage?.authorizedByPersonId ?? null},
      standing_authorization_kind = ${occurrence.standingCoverage?.authorizationKind ?? null},
      standing_authorized_at = ${occurrence.standingCoverage ? new Date(occurrence.standingCoverage.authorizedAt) : null},
      source_revision_refs = ${transaction.json(occurrence.sourceRevisionRefs)},
      materialized_at = ${new Date(occurrence.materializedAt)}
    where id = ${occurrence.occurrenceId} and version = ${previousVersion}
    returning id
  `;
  if (!updated[0]) throw new ConflictError("Routine occurrence version changed");
}

async function reconcileFutureRevisionOccurrences(input: {
  readonly transaction: Transaction;
  readonly secretBox: SecretBox;
  readonly routine: Routine;
  readonly previousRevision: number;
  readonly actorPersonId: string;
  readonly occurredAt: Date;
}): Promise<OccurrenceReconciliation> {
  const candidates = await loadFutureOccurrenceRows(
    input.transaction,
    input.routine.routineId,
    input.occurredAt,
  );
  const marker = `routine-revision:${input.routine.routineId}:${input.previousRevision}->${input.routine.currentRevision}`;
  let affectedOccurrenceCount = 0;
  const transitionedLoopIds: string[] = [];

  for (const row of candidates) {
    if (Number(row.routine_revision) === input.routine.currentRevision) continue;
    const occurrence = hydrateOccurrence(row, input.secretBox);
    let cancelled: RoutineOccurrence | null = null;
    if (occurrence.status === "materialized") {
      cancelled = reviseOccurrenceForLifecycle(occurrence, "cancel", input.occurredAt, marker);
    } else if (occurrence.status === "skipped" && hasRoutinePauseMarker(occurrence)) {
      cancelled = reviseNonMaterializedOccurrence(occurrence, "cancelled", input.occurredAt, marker);
    }
    if (cancelled === null) continue;

    await persistOccurrenceUpdate(input.transaction, input.secretBox, occurrence.version, cancelled);
    affectedOccurrenceCount += 1;
    if (occurrence.status === "materialized") {
      transitionedLoopIds.push(
        ...(await transitionLiveCoverageLoops({
          transaction: input.transaction,
          secretBox: input.secretBox,
          occurrence,
          command: "supersede",
          actorPersonId: input.actorPersonId,
          occurredAt: input.occurredAt,
          evidenceRef: marker,
        })),
      );
    }
  }
  return { affectedOccurrenceCount, transitionedLoopIds, coverage: [] };
}

async function reconcileFutureStatusOccurrences(input: {
  readonly transaction: Transaction;
  readonly secretBox: SecretBox;
  readonly routine: Routine;
  readonly revision: RoutineRevision;
  readonly actorPersonId: string;
  readonly occurredAt: Date;
}): Promise<OccurrenceReconciliation> {
  const candidates = await loadFutureOccurrenceRows(
    input.transaction,
    input.routine.routineId,
    input.occurredAt,
  );
  const marker = `routine-status:${input.routine.status}:${input.routine.routineId}:v${input.routine.version}`;
  let affectedOccurrenceCount = 0;
  const transitionedLoopIds: string[] = [];
  const coverage: MaterializedRoutineCoverage[] = [];
  const standingHolderActive = await isStandingHolderActive(
    input.transaction,
    input.routine.householdId,
    input.revision.standingCoverage?.holderPersonId ?? null,
  );

  for (const row of candidates) {
    const occurrence = hydrateOccurrence(row, input.secretBox);
    if (input.routine.status === "paused") {
      if (occurrence.status !== "materialized") continue;
      const skipped = reviseOccurrenceForLifecycle(occurrence, "skip", input.occurredAt, marker);
      await persistOccurrenceUpdate(input.transaction, input.secretBox, occurrence.version, skipped);
      affectedOccurrenceCount += 1;
      transitionedLoopIds.push(
        ...(await transitionLiveCoverageLoops({
          transaction: input.transaction,
          secretBox: input.secretBox,
          occurrence,
          command: "supersede",
          actorPersonId: input.actorPersonId,
          occurredAt: input.occurredAt,
          evidenceRef: marker,
        })),
      );
      continue;
    }

    if (input.routine.status === "retired") {
      if (occurrence.status === "materialized") {
        const cancelled = reviseOccurrenceForLifecycle(occurrence, "cancel", input.occurredAt, marker);
        await persistOccurrenceUpdate(input.transaction, input.secretBox, occurrence.version, cancelled);
        affectedOccurrenceCount += 1;
        transitionedLoopIds.push(
          ...(await transitionLiveCoverageLoops({
            transaction: input.transaction,
            secretBox: input.secretBox,
            occurrence,
            command: "cancel",
            actorPersonId: input.actorPersonId,
            occurredAt: input.occurredAt,
            evidenceRef: marker,
          })),
        );
      } else if (occurrence.status === "skipped" && hasRoutinePauseMarker(occurrence)) {
        const cancelled = reviseNonMaterializedOccurrence(occurrence, "cancelled", input.occurredAt, marker);
        await persistOccurrenceUpdate(input.transaction, input.secretBox, occurrence.version, cancelled);
        affectedOccurrenceCount += 1;
      }
      continue;
    }

    if (
      occurrence.status !== "skipped" ||
      occurrence.routineRevision !== input.revision.revision ||
      !hasRoutinePauseMarker(occurrence)
    ) {
      continue;
    }
    const reactivated = reactivatePausedOccurrence({
      occurrence,
      revision: input.revision,
      standingHolderActive,
      occurredAt: input.occurredAt,
      evidenceRef: marker,
    });
    await persistOccurrenceUpdate(input.transaction, input.secretBox, occurrence.version, reactivated);
    const coordination = new PostgresCoordination(input.transaction, input.secretBox);
    let loop = createCoverageLoopFromOccurrence({
      loopId: randomUUID(),
      householdId: input.routine.householdId,
      occurrence: reactivated,
    });
    loop = await coordination.create(loop);
    affectedOccurrenceCount += 1;
    coverage.push({ occurrence: reactivated, loop, occurrenceCreated: false, loopCreated: true });
  }

  return { affectedOccurrenceCount, transitionedLoopIds, coverage };
}

async function loadFutureOccurrenceRows(
  transaction: Transaction,
  routineId: string,
  occurredAt: Date,
): Promise<RoutineOccurrenceRow[]> {
  return transaction<RoutineOccurrenceRow[]>`
    select * from routine_occurrences
    where routine_id = ${routineId} and earliest_useful_at > ${occurredAt}
      and status in ('materialized', 'skipped')
    order by earliest_useful_at, id
    for update
  `;
}

function reviseOccurrenceForLifecycle(
  occurrence: RoutineOccurrence,
  kind: "skip" | "cancel",
  occurredAt: Date,
  evidenceRef: string,
): RoutineOccurrence {
  return reviseRoutineOccurrence(prepareOccurrenceEvidence(occurrence, evidenceRef), {
    kind,
    expectedVersion: occurrence.version,
    occurredAt: occurredAt.toISOString(),
    evidenceRef,
  });
}

function reviseNonMaterializedOccurrence(
  occurrence: RoutineOccurrence,
  status: "materialized" | "cancelled",
  occurredAt: Date,
  evidenceRef: string,
  standingCoverage: RoutineRevision["standingCoverage"] = null,
  proposedHolderPersonId: string | null = occurrence.proposedHolderPersonId,
): RoutineOccurrence {
  return RoutineOccurrenceSchema.parse({
    ...occurrence,
    version: occurrence.version + 1,
    supersedesVersion: occurrence.version,
    status,
    proposedHolderPersonId,
    standingCoverage,
    sourceRevisionRefs: appendEvidence(occurrence.sourceRevisionRefs, evidenceRef),
    materializedAt: occurredAt.toISOString(),
  });
}

function reactivatePausedOccurrence(input: {
  readonly occurrence: RoutineOccurrence;
  readonly revision: RoutineRevision;
  readonly standingHolderActive: boolean;
  readonly occurredAt: Date;
  readonly evidenceRef: string;
}): RoutineOccurrence {
  return reviseNonMaterializedOccurrence(
    input.occurrence,
    "materialized",
    input.occurredAt,
    input.evidenceRef,
    input.standingHolderActive ? input.revision.standingCoverage : null,
    input.revision.proposedHolderPersonId,
  );
}

async function transitionLiveCoverageLoops(input: {
  readonly transaction: Transaction;
  readonly secretBox: SecretBox;
  readonly occurrence: RoutineOccurrence;
  readonly command: "cancel" | "supersede";
  readonly actorPersonId: string;
  readonly occurredAt: Date;
  readonly evidenceRef: string;
}): Promise<string[]> {
  const rows = await input.transaction<CoverageLoopVersionRow[]>`
    select id, version from coverage_loops
    where routine_occurrence_id = ${input.occurrence.occurrenceId}
      and routine_occurrence_version = ${input.occurrence.version}
      and state in ('provisional', 'open', 'awaiting_response', 'covered', 'at_risk')
    order by id
  `;
  const coordination = new PostgresCoordination(input.transaction, input.secretBox);
  const timers = new DurableTimers(input.transaction);
  const transitioned: string[] = [];
  for (const row of rows) {
    const decision = await coordination.transition({
      loopId: row.id,
      command: {
        kind: input.command,
        transitionId: randomUUID(),
        expectedVersion: Number(row.version),
        actorPersonId: input.actorPersonId,
        occurredAt: input.occurredAt.toISOString(),
        evidenceRefs: [input.evidenceRef],
      },
    });
    await timers.supersedeCoverageTimers(decision.loop.loopId, decision.loop.version);
    transitioned.push(decision.loop.loopId);
  }
  return transitioned;
}

function hasRoutinePauseMarker(occurrence: RoutineOccurrence): boolean {
  const prefix = `routine-status:paused:${occurrence.routineId}:v`;
  return occurrence.sourceRevisionRefs.some((reference) => reference.startsWith(prefix));
}

function prepareOccurrenceEvidence(occurrence: RoutineOccurrence, evidenceRef: string): RoutineOccurrence {
  if (occurrence.sourceRevisionRefs.includes(evidenceRef) || occurrence.sourceRevisionRefs.length < 100) {
    return occurrence;
  }
  return RoutineOccurrenceSchema.parse({
    ...occurrence,
    sourceRevisionRefs: occurrence.sourceRevisionRefs.slice(-99),
  });
}

function appendEvidence(references: readonly string[], evidenceRef: string): string[] {
  if (references.includes(evidenceRef)) return [...references];
  return [...references.slice(-99), evidenceRef];
}

function emptyReconciliation(): OccurrenceReconciliation {
  return { affectedOccurrenceCount: 0, transitionedLoopIds: [], coverage: [] };
}

async function loadCurrentRoutine(
  transaction: Transaction,
  routineId: string,
  secretBox: SecretBox,
  forUpdate: boolean,
): Promise<SavedRoutine | null> {
  const rows = forUpdate
    ? await transaction<RoutineRow[]>`select * from routines where id = ${routineId} for update`
    : await transaction<RoutineRow[]>`select * from routines where id = ${routineId}`;
  const row = rows[0];
  if (!row) return null;
  const revisionRows = await transaction<RoutineRevisionRow[]>`
    select * from routine_revisions
    where routine_id = ${routineId} and revision = ${Number(row.current_revision)}
  `;
  const revisionRow = revisionRows[0];
  if (!revisionRow) throw new ConflictError("Routine current revision is missing");
  return {
    routine: RoutineSchema.parse({
      routineId: row.id,
      householdId: row.household_id,
      status: row.status,
      currentRevision: Number(row.current_revision),
      version: Number(row.version),
    }),
    revision: hydrateRevision(revisionRow, secretBox),
  };
}

function sealRoutineRevision(secretBox: SecretBox, revision: RoutineRevision) {
  const encrypted = secretBox.encrypt(
    JSON.stringify({
      title: revision.title,
      minimumSharedMeaning: revision.minimumSharedMeaning,
    }),
    `routine-revision-content:${revision.routineId}:${revision.revision}`,
  );
  return {
    ciphertext: Buffer.from(JSON.stringify(encrypted), "utf8"),
    keyVersion: encrypted.kid,
  };
}

function sealRoutineOccurrence(secretBox: SecretBox, occurrence: RoutineOccurrence) {
  const encrypted = secretBox.encrypt(
    JSON.stringify({
      title: occurrence.title,
      minimumSharedMeaning: occurrence.minimumSharedMeaning,
    }),
    `routine-occurrence-content:${occurrence.occurrenceId}`,
  );
  return {
    ciphertext: Buffer.from(JSON.stringify(encrypted), "utf8"),
    keyVersion: encrypted.kid,
  };
}

function hydrateRevision(row: RoutineRevisionRow, secretBox: SecretBox): RoutineRevision {
  const encrypted = JSON.parse(row.content_ciphertext.toString("utf8")) as { readonly kid?: unknown };
  if (encrypted.kid !== row.content_key_version) {
    throw new ConflictError("Routine revision encryption metadata does not match");
  }
  const content = routineRevisionContentSchema.parse(
    JSON.parse(
      secretBox
        .decrypt(encrypted, `routine-revision-content:${row.routine_id}:${Number(row.revision)}`)
        .toString("utf8"),
    ),
  );
  return RoutineRevisionSchema.parse({
    routineId: row.routine_id,
    revision: Number(row.revision),
    title: content.title,
    minimumSharedMeaning: content.minimumSharedMeaning,
    recurrence: row.recurrence,
    timePlan: row.semantic_time_plan,
    notificationMode: row.notification_mode,
    destination: {
      conversationId: row.destination_conversation_id,
      participantEpochId: row.participant_epoch_id,
      participantSetDigest: row.participant_set_digest,
      audience: row.audience,
    },
    proposedHolderPersonId: row.proposed_holder_person_id,
    standingCoverage: row.standing_holder_person_id
      ? {
          holderPersonId: row.standing_holder_person_id,
          authorizedByPersonId: row.standing_authorized_by_person_id,
          authorizationKind: row.standing_authorization_kind,
          authorizedAt: requiredDate(row.standing_authorized_at).toISOString(),
        }
      : null,
    sourceRevisionRefs: row.source_revision_refs,
    effectiveFrom: localDate(row.effective_from),
    effectiveThrough: row.effective_through ? localDate(row.effective_through) : null,
    createdAt: row.created_at.toISOString(),
    createdByPersonId: row.created_by_person_id,
  });
}

function hydrateOccurrence(row: RoutineOccurrenceRow, secretBox: SecretBox): RoutineOccurrence {
  const encrypted = JSON.parse(row.content_ciphertext.toString("utf8")) as { readonly kid?: unknown };
  if (encrypted.kid !== row.content_key_version) {
    throw new ConflictError("Routine occurrence encryption metadata does not match");
  }
  const content = routineRevisionContentSchema.parse(
    JSON.parse(secretBox.decrypt(encrypted, `routine-occurrence-content:${row.id}`).toString("utf8")),
  );
  return RoutineOccurrenceSchema.parse({
    occurrenceId: row.id,
    materializationKey: row.materialization_key,
    routineId: row.routine_id,
    routineRevision: Number(row.routine_revision),
    localDate: localDate(row.local_date),
    version: Number(row.version),
    supersedesVersion: row.supersedes_version === null ? null : Number(row.supersedes_version),
    planVersion: Number(row.plan_version),
    status: row.status,
    title: content.title,
    minimumSharedMeaning: content.minimumSharedMeaning,
    timing: {
      timeZone: row.time_zone,
      localDate: localDate(row.local_date),
      eventAt: row.event_at?.toISOString() ?? null,
      deadlineAt: row.deadline_at?.toISOString() ?? null,
      preparationMinutes: row.preparation_minutes,
      travelMinutes: row.travel_minutes,
      earliestUsefulAt: row.earliest_useful_at.toISOString(),
      lastResponsibleAt: row.last_responsible_at.toISOString(),
      resolutionPolicy: "wall_clock_compatible",
    },
    notificationMode: row.notification_mode,
    destination: {
      conversationId: row.destination_conversation_id,
      participantEpochId: row.participant_epoch_id,
      participantSetDigest: row.participant_set_digest,
      audience: row.audience,
    },
    proposedHolderPersonId: row.proposed_holder_person_id,
    standingCoverage: row.standing_holder_person_id
      ? {
          holderPersonId: row.standing_holder_person_id,
          authorizedByPersonId: row.standing_authorized_by_person_id,
          authorizationKind: row.standing_authorization_kind,
          authorizedAt: requiredDate(row.standing_authorized_at).toISOString(),
        }
      : null,
    sourceRevisionRefs: row.source_revision_refs,
    materializedAt: row.materialized_at.toISOString(),
  });
}

async function lockActiveHousehold(transaction: Transaction, householdId: string): Promise<void> {
  const rows = await transaction<{ readonly id: string }[]>`
    select id from households where id = ${householdId} and status = 'active' for update
  `;
  if (!rows[0]) throw new NotFoundError("Active family does not exist");
}

async function requireOriginator(
  transaction: Transaction,
  householdId: string,
  actorPersonId: string,
): Promise<void> {
  const rows = await transaction<{ readonly allowed: boolean }[]>`
    select exists(
      select 1 from household_memberships membership
      join membership_capabilities capability on capability.membership_id = membership.id
        and capability.capability = 'coordination.originate' and capability.status = 'active'
      join people person on person.id = membership.person_id and person.status = 'registered'
      where membership.household_id = ${householdId}
        and membership.person_id = ${actorPersonId} and membership.status = 'active'
    ) as allowed
  `;
  if (!rows[0]?.allowed) throw new UnauthorizedError("You cannot create family routines");
}

async function requireCoordinator(
  transaction: Transaction,
  householdId: string,
  actorPersonId: string,
): Promise<void> {
  const rows = await transaction<{ readonly allowed: boolean }[]>`
    select exists(
      select 1 from household_memberships membership
      join membership_capabilities capability on capability.membership_id = membership.id
        and capability.capability = 'coordination.coordinate' and capability.status = 'active'
      join people person on person.id = membership.person_id and person.status = 'registered'
      where membership.household_id = ${householdId}
        and membership.person_id = ${actorPersonId} and membership.status = 'active'
    ) as allowed
  `;
  if (!rows[0]?.allowed) throw new UnauthorizedError("You cannot manage family routines");
}

async function requireExactDestination(
  transaction: Transaction,
  input: {
    readonly householdId: string;
    readonly actorPersonId: string;
    readonly destination: z.input<typeof DestinationEpochSchema>;
  },
): Promise<void> {
  const ready = await isExactDestinationCurrent(transaction, { ...input, requireActor: true });
  if (!ready) throw new StaleAuthorityError("Routine destination is not the exact live consenting chat");
}

async function isExactDestinationCurrent(
  transaction: Transaction,
  input: {
    readonly householdId: string;
    readonly actorPersonId: string;
    readonly destination: z.input<typeof DestinationEpochSchema>;
    readonly requireActor: boolean;
  },
): Promise<boolean> {
  const destination = DestinationEpochSchema.parse(input.destination);
  const conversations = await transaction<
    {
      readonly kind: string;
      readonly household_id: string | null;
      readonly current_epoch_id: string | null;
      readonly participant_set_digest: string | null;
    }[]
  >`
    select conversation.kind, conversation.household_id, conversation.current_epoch_id,
      epoch.participant_set_digest
    from conversations conversation
    left join participant_epochs epoch on epoch.id = conversation.current_epoch_id
      and epoch.ended_at is null
    where conversation.id = ${destination.conversationId} and conversation.status = 'active'
  `;
  const conversation = conversations[0];
  if (
    !conversation ||
    conversation.current_epoch_id !== destination.participantEpochId ||
    conversation.participant_set_digest !== destination.participantSetDigest ||
    (destination.audience === "group" &&
      (conversation.kind !== "group" || conversation.household_id !== input.householdId)) ||
    (destination.audience === "private" &&
      (conversation.kind !== "direct" ||
        (conversation.household_id !== null && conversation.household_id !== input.householdId)))
  ) {
    return false;
  }
  const participants = await transaction<
    {
      readonly person_id: string;
      readonly registration_status: string;
      readonly consented_at: Date | null;
      readonly person_status: string;
      readonly identity_status: string;
      readonly allow_content_processing: boolean | null;
    }[]
  >`
    select participant.person_id, participant.registration_status, participant.consented_at,
      person.status as person_status, identity.status as identity_status,
      policy.allow_content_processing
    from epoch_participants participant
    join people person on person.id = participant.person_id
    join person_identities identity on identity.id = participant.person_identity_id
    left join participant_policies policy on policy.conversation_id = ${destination.conversationId}
      and policy.person_id = participant.person_id and policy.status = 'active'
    where participant.participant_epoch_id = ${destination.participantEpochId}
  `;
  if (participants.length === 0) return false;
  if (input.requireActor && !participants.some((entry) => entry.person_id === input.actorPersonId)) {
    return false;
  }
  return participants.every(
    (participant) =>
      participant.registration_status === "registered" &&
      participant.consented_at !== null &&
      participant.person_status === "registered" &&
      participant.identity_status === "verified" &&
      participant.allow_content_processing === true,
  );
}

async function requireStandingAuthorization(
  transaction: Transaction,
  input: {
    readonly kind: "create" | "revise";
    readonly householdId: string;
    readonly actorPersonId: string;
    readonly occurredAt: Date;
    readonly proposed: RoutineRevisionDraft["standingCoverage"];
  },
): Promise<void> {
  if (input.proposed === null) return;
  const expectedKind = input.kind === "create" ? "created" : "approved";
  if (
    input.proposed.holderPersonId !== input.actorPersonId ||
    input.proposed.authorizedByPersonId !== input.actorPersonId ||
    input.proposed.authorizationKind !== expectedKind ||
    input.proposed.authorizedAt !== input.occurredAt.toISOString()
  ) {
    throw new UnauthorizedError(
      "Every standing-coverage revision requires the exact holder's current approval",
    );
  }
  if (!(await isStandingHolderActive(transaction, input.householdId, input.proposed.holderPersonId))) {
    throw new UnauthorizedError("Standing coverage holder is not an active registered family member");
  }
}

async function isStandingHolderActive(
  transaction: Transaction,
  householdId: string,
  holderPersonId: string | null,
): Promise<boolean> {
  if (holderPersonId === null) return true;
  const rows = await transaction<{ readonly active: boolean }[]>`
    select exists(
      select 1 from household_memberships membership
      join people person on person.id = membership.person_id and person.status = 'registered'
      where membership.household_id = ${householdId}
        and membership.person_id = ${holderPersonId} and membership.status = 'active'
    ) as active
  `;
  return rows[0]?.active === true;
}

async function appendRoutineAudit(
  transaction: Transaction,
  input: {
    readonly householdId: string;
    readonly actorKind: "person" | "application";
    readonly actorPersonId: string | null;
    readonly eventType: string;
    readonly targetType: string;
    readonly targetId: string;
    readonly reasonCodes: readonly string[];
    readonly occurredAt: Date;
  },
): Promise<void> {
  const sequenceRows = await transaction<{ readonly next_sequence: number | string }[]>`
    select coalesce(max(sequence), 0) + 1 as next_sequence
    from audit_events where household_id = ${input.householdId}
  `;
  await transaction`
    insert into audit_events (
      id, household_id, person_id, sequence, actor_kind, actor_id,
      event_type, target_type, target_id, reason_codes, occurred_at
    ) values (
      ${randomUUID()}, ${input.householdId}, ${input.actorPersonId},
      ${Number(sequenceRows[0]?.next_sequence ?? 1)}, ${input.actorKind}, ${input.actorPersonId},
      ${input.eventType}, ${input.targetType}, ${input.targetId},
      ${transaction.array([...input.reasonCodes])}, ${input.occurredAt}
    )
  `;
}

function enumerateDates(fromCandidate: string, throughCandidate: string): string[] {
  const through = Temporal.PlainDate.from(throughCandidate);
  const values: string[] = [];
  for (
    let cursor = Temporal.PlainDate.from(fromCandidate);
    Temporal.PlainDate.compare(cursor, through) <= 0;
    cursor = cursor.add({ days: 1 })
  ) {
    values.push(cursor.toString());
  }
  return values;
}

function inTransaction<Result>(
  executor: Executor,
  operation: (transaction: Transaction) => Promise<Result>,
): Promise<Result> {
  return "begin" in executor
    ? (executor.begin(operation) as unknown as Promise<Result>)
    : operation(executor);
}

function localDate(value: string | Date): string {
  return typeof value === "string" ? value : value.toISOString().slice(0, 10);
}

function asDate(value: string | null): Date | null {
  return value === null ? null : new Date(value);
}

function requiredDate(value: Date | null): Date {
  if (value === null) throw new ConflictError("Expected persisted routine timestamp");
  return value;
}
