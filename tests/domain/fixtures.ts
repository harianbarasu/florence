import type { z } from "zod";
import {
  AdultIdSchema,
  type DataScopeSchema,
  EpisodeProposalSchema,
  EvidenceRefSchema,
  type HouseholdAggregate,
  HouseholdAggregateSchema,
  HouseholdIdSchema,
  type HouseholdSignal,
  HouseholdSignalSchema,
  SemanticTimePlanSchema,
  WorkerJobIdSchema,
  WorkerProposalSchema,
} from "../../src/domain/index.js";

export const HOUSEHOLD_ID = HouseholdIdSchema.parse("household_family");
export const ADULT_A = AdultIdSchema.parse("adult_alex");
export const ADULT_B = AdultIdSchema.parse("adult_bailey");
export const WORKER_JOB_ID = WorkerJobIdSchema.parse("job_interpret_1");

export const DIGEST_A = `sha256:${"a".repeat(64)}`;
export const DIGEST_B = `sha256:${"b".repeat(64)}`;
export const DIGEST_C = `sha256:${"c".repeat(64)}`;
export const T0 = "2026-01-01T08:00:00Z";

type AggregateInput = z.input<typeof HouseholdAggregateSchema>;
type EvidenceInput = z.input<typeof EvidenceRefSchema>;
type ScopeInput = z.input<typeof DataScopeSchema>;
type EpisodeProposalInput = z.input<typeof EpisodeProposalSchema>;
type TimePlanInput = z.input<typeof SemanticTimePlanSchema>;
type WorkerProposalInput = z.input<typeof WorkerProposalSchema>;

export function aggregate(overrides: Partial<AggregateInput> = {}): HouseholdAggregate {
  return HouseholdAggregateSchema.parse({
    schemaVersion: 1,
    householdId: HOUSEHOLD_ID,
    version: 0,
    policyVersion: 0,
    lastProcessedSequence: 0,
    timeZone: "America/Los_Angeles",
    verifiedAdultIds: [ADULT_A, ADULT_B],
    routineAnchors: [],
    episodes: [],
    policies: [],
    policyCandidates: [],
    approvals: [],
    memoryCandidates: [],
    memories: [],
    pendingActions: [],
    ...overrides,
  });
}

export function evidence(scope: ScopeInput = { kind: "household" }, overrides: Partial<EvidenceInput> = {}) {
  return EvidenceRefSchema.parse({
    evidenceId: "evidence_1",
    source: "linq",
    sourceRef: "message_1",
    scope,
    observedAt: T0,
    revision: 1,
    contentDigest: DIGEST_A,
    ...overrides,
  });
}

export function timePlan(overrides: Partial<TimePlanInput> = {}) {
  return SemanticTimePlanSchema.parse({
    planId: "plan_1",
    version: 1,
    timeZone: "America/Los_Angeles",
    deadline: { kind: "instant", at: "2026-01-02T17:00:00Z" },
    usefulLeadMinutes: 1_440,
    preparationMinutes: 30,
    finalBufferMinutes: 30,
    triggers: [
      {
        triggerId: "trigger_1",
        timerId: "timer_1",
        kind: "reminder",
        at: { kind: "instant", at: "2026-01-02T09:00:00Z" },
      },
    ],
    ...overrides,
  });
}

export function episodeProposal(overrides: Partial<EpisodeProposalInput> = {}) {
  return EpisodeProposalSchema.parse({
    episodeId: "episode_field_trip",
    type: "commitment",
    targetScope: { kind: "household" },
    title: "Submit the field-trip form",
    requiredOutcome: "The field-trip form is submitted",
    proposedOwnerAdultId: ADULT_A,
    evidence: [evidence()],
    sourceClass: "school.notice",
    sensitivity: "ordinary",
    temporalPlan: timePlan(),
    ...overrides,
  });
}

export function workerProposal(overrides: Partial<WorkerProposalInput> = {}) {
  return WorkerProposalSchema.parse({
    resultId: "worker_result_1",
    jobId: WORKER_JOB_ID,
    householdId: HOUSEHOLD_ID,
    baseHouseholdVersion: 0,
    basePolicyVersion: 0,
    completedAt: T0,
    confidence: 0.95,
    evidence: [],
    episodeProposals: [],
    messageProposals: [],
    actionProposals: [],
    memoryCandidates: [],
    policyCandidates: [],
    unresolvedQuestions: [],
    diagnostics: { warnings: [] },
    ...overrides,
  });
}

export function signal(value: z.input<typeof HouseholdSignalSchema>): HouseholdSignal {
  return HouseholdSignalSchema.parse(value);
}
