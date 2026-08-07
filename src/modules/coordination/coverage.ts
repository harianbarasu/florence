import { z } from "zod";
import {
  CoordinationError,
  type CoverageLoop,
  CoverageLoopSchema,
  type CoverageTransition,
  CoverageTransitionSchema,
  DestinationEpochSchema,
  EntityIdSchema,
  EvidenceRefSchema,
  InstantSchema,
  type MinimumSharedCoverageStatus,
  MinimumSharedCoverageStatusSchema,
  ResolvedTimePlanSchema,
  type RoutineOccurrence,
  RoutineOccurrenceSchema,
} from "./contracts.js";
import { compareInstants } from "./household-time.js";

export const CreateCoverageLoopInputSchema = z.strictObject({
  loopId: EntityIdSchema,
  householdId: EntityIdSchema,
  minimumSharedMeaning: z.string().trim().min(1).max(500),
  unresolvedFacts: z.array(z.string().trim().min(1).max(300)).max(20).default([]),
  proposedHolderPersonId: EntityIdSchema.nullable(),
  timing: ResolvedTimePlanSchema,
  planVersion: z.number().int().positive().default(1),
  notificationMode: z.enum(["exceptions_only", "always", "silent"]).default("always"),
  destination: DestinationEpochSchema,
  sourceEvidenceRefs: z.array(EvidenceRefSchema).max(100),
  occurredAt: InstantSchema,
});

export const CoverageCommandSchema = z.discriminatedUnion("kind", [
  z.strictObject({
    kind: z.literal("revise"),
    transitionId: EntityIdSchema,
    expectedVersion: z.number().int().positive(),
    actorPersonId: EntityIdSchema,
    occurredAt: InstantSchema,
    minimumSharedMeaning: z.string().trim().min(1).max(500),
    timing: ResolvedTimePlanSchema,
    evidenceRefs: z.array(EvidenceRefSchema).min(1).max(100),
  }),
  z.strictObject({
    kind: z.literal("resolve_facts"),
    transitionId: EntityIdSchema,
    expectedVersion: z.number().int().positive(),
    actorPersonId: EntityIdSchema,
    occurredAt: InstantSchema,
    minimumSharedMeaning: z.string().trim().min(1).max(500),
    unresolvedFacts: z.array(z.string().trim().min(1).max(300)).max(20),
    proposedHolderPersonId: EntityIdSchema.nullable(),
    timing: ResolvedTimePlanSchema,
    evidenceRefs: z.array(EvidenceRefSchema).min(1).max(20),
  }),
  z.strictObject({
    kind: z.literal("request_coverage"),
    transitionId: EntityIdSchema,
    expectedVersion: z.number().int().positive(),
    actorPersonId: EntityIdSchema,
    requestedPersonId: EntityIdSchema,
    occurredAt: InstantSchema,
    evidenceRefs: z.array(EvidenceRefSchema).max(20).default([]),
  }),
  z.strictObject({
    kind: z.literal("acknowledge_coverage"),
    transitionId: EntityIdSchema,
    expectedVersion: z.number().int().positive(),
    actorPersonId: EntityIdSchema,
    acknowledgment: z.literal("explicit_self"),
    visibility: z.enum(["private", "shared"]),
    occurredAt: InstantSchema,
    evidenceRefs: z.array(EvidenceRefSchema).max(20).default([]),
  }),
  z.strictObject({
    kind: z.literal("decline_coverage"),
    transitionId: EntityIdSchema,
    expectedVersion: z.number().int().positive(),
    actorPersonId: EntityIdSchema,
    visibility: z.literal("private"),
    privateReason: z.string().trim().min(1).max(500).optional(),
    occurredAt: InstantSchema,
    evidenceRefs: z.array(EvidenceRefSchema).max(20).default([]),
  }),
  z.strictObject({
    kind: z.literal("record_risk"),
    transitionId: EntityIdSchema,
    expectedVersion: z.number().int().positive(),
    actorPersonId: EntityIdSchema.nullable(),
    occurredAt: InstantSchema,
    proposedHolderPersonId: EntityIdSchema.nullable(),
    evidenceRefs: z.array(EvidenceRefSchema).min(1).max(20),
  }),
  z.strictObject({
    kind: z.literal("revoke_participant"),
    transitionId: EntityIdSchema,
    expectedVersion: z.number().int().positive(),
    actorPersonId: z.null(),
    affectedPersonId: EntityIdSchema,
    occurredAt: InstantSchema,
    evidenceRefs: z.array(EvidenceRefSchema).min(1).max(20),
  }),
  z.strictObject({
    kind: z.enum(["cancel", "supersede", "dismiss"]),
    transitionId: EntityIdSchema,
    expectedVersion: z.number().int().positive(),
    actorPersonId: EntityIdSchema.nullable(),
    occurredAt: InstantSchema,
    evidenceRefs: z.array(EvidenceRefSchema).max(20).default([]),
  }),
  z.strictObject({
    kind: z.literal("expire_uncovered"),
    transitionId: EntityIdSchema,
    expectedVersion: z.number().int().positive(),
    occurredAt: InstantSchema,
    evidenceRefs: z.array(EvidenceRefSchema).max(20).default([]),
  }),
]);
export type CoverageCommand = z.input<typeof CoverageCommandSchema>;
type ParsedCoverageCommand = z.output<typeof CoverageCommandSchema>;

export interface CoverageTransitionDecision {
  readonly loop: CoverageLoop;
  readonly transition: CoverageTransition;
  readonly minimumSharedStatus: MinimumSharedCoverageStatus | null;
}

const LIVE_STATES = new Set<CoverageLoop["state"]>([
  "provisional",
  "open",
  "awaiting_response",
  "covered",
  "at_risk",
]);

export function createCoverageLoop(
  inputCandidate: z.input<typeof CreateCoverageLoopInputSchema>,
): CoverageLoop {
  const input = CreateCoverageLoopInputSchema.parse(inputCandidate);
  return CoverageLoopSchema.parse({
    loopId: input.loopId,
    householdId: input.householdId,
    version: 1,
    state: input.unresolvedFacts.length > 0 ? "provisional" : "open",
    minimumSharedMeaning: input.minimumSharedMeaning,
    unresolvedFacts: input.unresolvedFacts,
    proposedHolderPersonId: input.proposedHolderPersonId,
    acknowledgment: null,
    timing: input.timing,
    planVersion: input.planVersion,
    notificationMode: input.notificationMode,
    destination: input.destination,
    sourceEvidenceRefs: input.sourceEvidenceRefs,
    routineOccurrence: null,
    attentionCycle: 1,
    notificationHistory: [],
    lastTransitionAt: input.occurredAt,
  });
}

export function createCoverageLoopFromOccurrence(input: {
  readonly loopId: string;
  readonly householdId: string;
  readonly occurrence: RoutineOccurrence;
}): CoverageLoop {
  const loopId = EntityIdSchema.parse(input.loopId);
  const householdId = EntityIdSchema.parse(input.householdId);
  const occurrence = RoutineOccurrenceSchema.parse(input.occurrence);
  if (occurrence.status !== "materialized") throw new CoordinationError("invalid_transition");
  const standingCoverage = occurrence.standingCoverage;

  return CoverageLoopSchema.parse({
    loopId,
    householdId,
    version: 1,
    state:
      standingCoverage !== null
        ? "covered"
        : occurrence.proposedHolderPersonId !== null
          ? "awaiting_response"
          : "open",
    minimumSharedMeaning: occurrence.minimumSharedMeaning,
    unresolvedFacts: [],
    proposedHolderPersonId: occurrence.proposedHolderPersonId,
    acknowledgment:
      standingCoverage === null
        ? null
        : {
            personId: standingCoverage.holderPersonId,
            acknowledgedAt: standingCoverage.authorizedAt,
            kind: "standing_routine_self_authorized",
            holderDisclosure: "minimum_only",
          },
    timing: occurrence.timing,
    planVersion: occurrence.planVersion,
    notificationMode: occurrence.notificationMode,
    destination: occurrence.destination,
    sourceEvidenceRefs: occurrence.sourceRevisionRefs,
    routineOccurrence: {
      occurrenceId: occurrence.occurrenceId,
      occurrenceVersion: occurrence.version,
      routineId: occurrence.routineId,
      routineRevision: occurrence.routineRevision,
    },
    attentionCycle: 1,
    notificationHistory: [],
    lastTransitionAt: occurrence.materializedAt,
  });
}

/** Applies the complete coverage state machine with optimistic versioning. */
export function transitionCoverage(
  loopCandidate: CoverageLoop,
  commandCandidate: CoverageCommand,
): CoverageTransitionDecision {
  const loop = CoverageLoopSchema.parse(loopCandidate);
  const command = CoverageCommandSchema.parse(commandCandidate);
  if (loop.version !== command.expectedVersion) throw new CoordinationError("version_conflict");
  if (!LIVE_STATES.has(loop.state)) throw new CoordinationError("invalid_transition");

  switch (command.kind) {
    case "revise":
      return decision(
        loop,
        command,
        "coverage_revised",
        {
          state: "open",
          minimumSharedMeaning: command.minimumSharedMeaning,
          unresolvedFacts: [],
          proposedHolderPersonId: null,
          acknowledgment: null,
          timing: command.timing,
          planVersion: loop.planVersion + 1,
          attentionCycle: loop.attentionCycle + 1,
        },
        "coverage_still_open",
      );

    case "resolve_facts":
      requireState(loop, "provisional");
      return decision(
        loop,
        command,
        "facts_resolved",
        {
          state: command.unresolvedFacts.length === 0 ? "open" : "provisional",
          unresolvedFacts: command.unresolvedFacts,
          minimumSharedMeaning: command.minimumSharedMeaning,
          proposedHolderPersonId: command.proposedHolderPersonId,
          timing: command.timing,
        },
        "coverage_still_open",
      );

    case "request_coverage":
      requireOneOfStates(loop, ["open", "awaiting_response", "at_risk"]);
      return decision(
        loop,
        command,
        "coverage_requested",
        {
          state: "awaiting_response",
          proposedHolderPersonId: command.requestedPersonId,
          acknowledgment: null,
        },
        "coverage_still_open",
      );

    case "acknowledge_coverage":
      requireOneOfStates(loop, ["awaiting_response", "at_risk"]);
      if (loop.proposedHolderPersonId !== command.actorPersonId) {
        throw new CoordinationError("not_proposed_holder");
      }
      return decision(
        loop,
        command,
        "coverage_acknowledged",
        {
          state: "covered",
          acknowledgment: {
            personId: command.actorPersonId,
            acknowledgedAt: command.occurredAt,
            kind: "explicit_self",
            holderDisclosure: command.visibility === "shared" ? "shared" : "minimum_only",
          },
        },
        "coverage_recorded",
      );

    case "decline_coverage":
      requireState(loop, "awaiting_response");
      if (loop.proposedHolderPersonId !== command.actorPersonId) {
        throw new CoordinationError("not_proposed_holder");
      }
      return decision(
        loop,
        command,
        "coverage_declined_privately",
        { state: "open", proposedHolderPersonId: null, acknowledgment: null },
        "coverage_still_open",
      );

    case "record_risk":
      requireState(loop, "covered");
      return decision(
        loop,
        command,
        "coverage_at_risk",
        {
          state: "at_risk",
          acknowledgment: null,
          proposedHolderPersonId: command.proposedHolderPersonId,
          attentionCycle: loop.attentionCycle + 1,
        },
        "coverage_at_risk",
      );

    case "revoke_participant": {
      const removesAcknowledgment = loop.acknowledgment?.personId === command.affectedPersonId;
      const removesProposal = loop.proposedHolderPersonId === command.affectedPersonId;
      if (!removesAcknowledgment && !removesProposal) {
        throw new CoordinationError("invalid_transition");
      }
      const wasAwaitingRevokedHolder = removesProposal && loop.state === "awaiting_response";
      return decision(
        loop,
        command,
        "coverage_participant_revoked",
        {
          state: removesAcknowledgment ? "at_risk" : wasAwaitingRevokedHolder ? "open" : loop.state,
          proposedHolderPersonId: removesProposal ? null : loop.proposedHolderPersonId,
          acknowledgment: removesAcknowledgment ? null : loop.acknowledgment,
          attentionCycle: removesAcknowledgment ? loop.attentionCycle + 1 : loop.attentionCycle,
        },
        removesAcknowledgment ? "coverage_at_risk" : wasAwaitingRevokedHolder ? "coverage_still_open" : null,
      );
    }

    case "cancel":
      return decision(
        loop,
        command,
        "cancelled",
        { state: "cancelled", acknowledgment: null },
        "coverage_cancelled",
      );
    case "supersede":
      return decision(loop, command, "superseded", { state: "superseded", acknowledgment: null }, null);
    case "dismiss":
      return decision(
        loop,
        command,
        "dismissed",
        { state: "dismissed", acknowledgment: null },
        "coverage_dismissed",
      );
    case "expire_uncovered":
      if (loop.state === "covered") throw new CoordinationError("invalid_transition");
      if (compareInstants(command.occurredAt, loop.timing.lastResponsibleAt) < 0) {
        throw new CoordinationError("too_early_to_expire");
      }
      return decision(
        loop,
        command,
        "expired_uncovered",
        { state: "expired_uncovered", acknowledgment: null },
        "coverage_expired_uncovered",
      );
  }
}

type TransitionKind = CoverageTransition["kind"];
type SharedStatusKind = MinimumSharedCoverageStatus["kind"];

function decision(
  loop: CoverageLoop,
  command: ParsedCoverageCommand,
  transitionKind: TransitionKind,
  changes: Partial<CoverageLoop>,
  sharedStatusKind: SharedStatusKind | null,
): CoverageTransitionDecision {
  const next = CoverageLoopSchema.parse({
    ...loop,
    ...changes,
    version: loop.version + 1,
    sourceEvidenceRefs:
      command.kind === "revoke_participant"
        ? loop.sourceEvidenceRefs
        : unique([...loop.sourceEvidenceRefs, ...command.evidenceRefs]),
    lastTransitionAt: command.occurredAt,
  });
  const actorPersonId = command.kind === "expire_uncovered" ? null : command.actorPersonId;
  const transition = CoverageTransitionSchema.parse({
    transitionId: command.transitionId,
    loopId: loop.loopId,
    fromState: loop.state,
    toState: next.state,
    fromVersion: loop.version,
    toVersion: next.version,
    kind: transitionKind,
    actorPersonId,
    evidenceRefs: command.evidenceRefs,
    occurredAt: command.occurredAt,
  });
  return {
    loop: next,
    transition,
    minimumSharedStatus:
      sharedStatusKind === null
        ? null
        : MinimumSharedCoverageStatusSchema.parse({
            kind: sharedStatusKind,
            minimumSharedMeaning: next.minimumSharedMeaning,
            holderPersonId:
              sharedStatusKind === "coverage_recorded" && next.acknowledgment?.holderDisclosure === "shared"
                ? next.acknowledgment.personId
                : null,
            destination: next.destination,
            loopVersion: next.version,
          }),
  };
}

function requireState(loop: CoverageLoop, expected: CoverageLoop["state"]): void {
  if (loop.state !== expected) throw new CoordinationError("invalid_transition");
}

function requireOneOfStates(loop: CoverageLoop, expected: readonly CoverageLoop["state"][]): void {
  if (!expected.includes(loop.state)) throw new CoordinationError("invalid_transition");
}

function unique(values: readonly string[]): string[] {
  return [...new Set(values)];
}
