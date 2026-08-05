import { createHash } from "node:crypto";
import type { DurableScope, HouseholdAggregate } from "../domain/index.js";
import type { ApplicationProjection, WorkerPurpose } from "./contracts.js";

function scopeCanRead(scope: DurableScope, candidate: DurableScope): boolean {
  if (scope.kind === "household") return candidate.kind === "household";
  return candidate.kind === "household" || candidate.adultId === scope.adultId;
}

/**
 * Hashes only the durable family context exposed to one ephemeral worker.
 * Unrelated household activity can advance the aggregate without invalidating
 * a long-running project, while profile, routine, memory, project, evidence, or
 * meal-planning schedule changes always require a fresh attempt.
 */
export function workerContextFingerprint(input: {
  aggregate: HouseholdAggregate;
  projection: ApplicationProjection;
  episodeId: string;
  purpose: WorkerPurpose;
  scope: DurableScope;
  evidenceRefs: readonly string[];
  asOf: string;
}): string {
  const episode = input.aggregate.episodes.find((candidate) => candidate.episodeId === input.episodeId);
  if (episode === undefined || !scopeCanRead(input.scope, episode.scope)) {
    throw new Error("Cannot fingerprint an unavailable worker episode");
  }
  const evidenceIds = new Set(input.evidenceRefs);
  const context = {
    schemaVersion: 1,
    householdTimeZone: input.aggregate.timeZone,
    scope: input.scope,
    householdAdults: [...input.projection.onboarding.adultNames].sort((left, right) =>
      left.adultId.localeCompare(right.adultId),
    ),
    sharedProfileFacts: [...input.projection.sharedProfile.facts].sort((left, right) =>
      left.factKey.localeCompare(right.factKey),
    ),
    routineAnchors: [...input.aggregate.routineAnchors].sort((left, right) =>
      left.anchorId.localeCompare(right.anchorId),
    ),
    confirmedMemories: input.aggregate.memories
      .filter(
        (memory) =>
          memory.status === "active" &&
          scopeCanRead(input.scope, memory.scope) &&
          (memory.expiresAt === undefined || Date.parse(memory.expiresAt) > Date.parse(input.asOf)),
      )
      .sort((left, right) => left.memoryId.localeCompare(right.memoryId)),
    scheduleEpisodes:
      input.purpose === "meal_plan"
        ? input.aggregate.episodes
            .filter((candidate) => scopeCanRead(input.scope, candidate.scope))
            .filter((candidate) => candidate.temporalPlan !== undefined)
            .map((candidate) => ({
              episodeId: candidate.episodeId,
              type: candidate.type,
              state: candidate.state,
              title: candidate.title,
              requiredOutcome: candidate.requiredOutcome,
              scope: candidate.scope,
              ownerStatus: candidate.owner.status,
              temporalPlan: candidate.temporalPlan,
            }))
            .sort((left, right) => left.episodeId.localeCompare(right.episodeId))
        : [],
    episode: {
      episodeId: episode.episodeId,
      version: episode.version,
      type: episode.type,
      state: episode.state,
      title: episode.title,
      requiredOutcome: episode.requiredOutcome,
      scope: episode.scope,
      delegation: episode.delegation,
      evidence: episode.evidence
        .filter((evidence) => evidenceIds.has(evidence.evidenceId))
        .sort((left, right) => left.evidenceId.localeCompare(right.evidenceId)),
    },
  };
  return `sha256:${createHash("sha256").update(JSON.stringify(context)).digest("hex")}`;
}
