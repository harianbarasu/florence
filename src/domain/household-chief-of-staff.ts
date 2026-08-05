import { createHash } from "node:crypto";
import { Temporal } from "@js-temporal/polyfill";
import {
  type AcceptanceResult,
  AcceptanceResultSchema,
  ApprovalIdSchema,
  type ApprovalRecord,
  type DomainChange,
  DomainChangeSchema,
  DurableMemorySchema,
  type DurableScope,
  EpisodeIdSchema,
  type EpisodeProposal,
  type EvidenceRef,
  type ExternalAction,
  type FamilyEpisode,
  FamilyEpisodeSchema,
  type HouseholdAggregate,
  HouseholdAggregateSchema,
  type HouseholdChiefOfStaffInput,
  HouseholdChiefOfStaffInputSchema,
  type HouseholdSignal,
  type InstantString,
  MemoryCandidateIdSchema,
  MemoryIdSchema,
  type OutboxIntent,
  OutboxIntentIdSchema,
  OutboxIntentSchema,
  type PendingExternalAction,
  PolicyCandidateIdSchema,
  type PromotionAuthority,
  type RejectionReason,
  type ResolvedTimePlan,
  ResolvedTimePlanSchema,
  type RoutineAnchor,
  TimerIdSchema,
} from "./contracts.js";
import { HouseholdTime, HouseholdTimeError } from "./household-time.js";

type IgnoredReason = "delivery_is_not_approval" | "timer_no_longer_relevant";

type MutationResult =
  | {
      disposition: "accepted";
      aggregate: HouseholdAggregate;
      changes: DomainChange[];
      effects: OutboxIntent[];
    }
  | { disposition: "rejected"; reason: RejectionReason }
  | { disposition: "ignored"; reason: IgnoredReason };

interface HandlerContext {
  before: HouseholdAggregate;
  draft: HouseholdAggregate;
  signal: HouseholdSignal;
}

interface AuthorityCheck {
  ok: boolean;
  reason?: RejectionReason;
  approvalIndex?: number;
}

const TERMINAL_STATES = new Set(["completed", "dismissed", "superseded", "failed"]);
const SENSITIVITY_RANK = {
  ordinary: 0,
  sensitive: 1,
  highly_sensitive: 2,
} as const;

function accepted(
  aggregate: HouseholdAggregate,
  changes: DomainChange[],
  effects: OutboxIntent[],
): MutationResult {
  return { disposition: "accepted", aggregate, changes, effects };
}

function rejected(reason: RejectionReason): MutationResult {
  return { disposition: "rejected", reason };
}

function ignored(reason: IgnoredReason): MutationResult {
  return { disposition: "ignored", reason };
}

function isVerifiedAdult(aggregate: HouseholdAggregate, adultId: string): boolean {
  return aggregate.verifiedAdultIds.some((candidate) => candidate === adultId);
}

function isAdultActor(
  aggregate: HouseholdAggregate,
  actor: HouseholdSignal["actor"],
): actor is Extract<HouseholdSignal["actor"], { kind: "adult" }> {
  return actor.kind === "adult" && isVerifiedAdult(aggregate, actor.adultId);
}

function instantCompare(left: string, right: string): number {
  return Temporal.Instant.compare(Temporal.Instant.from(left), Temporal.Instant.from(right));
}

function sortedUnique(values: readonly string[]): string[] {
  return [...new Set(values)].sort((left, right) => left.localeCompare(right));
}

function sameStrings(left: readonly string[], right: readonly string[]): boolean {
  const sortedLeft = sortedUnique(left);
  const sortedRight = sortedUnique(right);
  return (
    sortedLeft.length === sortedRight.length &&
    sortedLeft.every((value, index) => value === sortedRight[index])
  );
}

function outboxBase(signal: HouseholdSignal, suffix: string) {
  const digest = createHash("sha256")
    .update(`${signal.householdId}\u0000${signal.signalId}\u0000${suffix}`)
    .digest("hex");
  return {
    intentId: OutboxIntentIdSchema.parse(`outbox_${digest}`),
    householdId: signal.householdId,
    idempotencyKey: `florence:${digest}`,
    createdFromSignalId: signal.signalId,
  };
}

function scheduleTimerEffects(signal: HouseholdSignal, episode: FamilyEpisode): OutboxIntent[] {
  if (episode.temporalPlan === undefined) {
    return [];
  }

  return episode.temporalPlan.triggers
    .filter((trigger) => trigger.status === "pending")
    .map((trigger) =>
      OutboxIntentSchema.parse({
        ...outboxBase(signal, `schedule:${trigger.timerId}`),
        kind: "schedule_timer",
        timerId: trigger.timerId,
        episodeId: episode.episodeId,
        temporalPlanVersion: episode.temporalPlan?.definition.version,
        triggerId: trigger.triggerId,
        at: trigger.at,
      }),
    );
}

function cancelTimerEffects(signal: HouseholdSignal, episode: FamilyEpisode): OutboxIntent[] {
  if (episode.temporalPlan === undefined) {
    return [];
  }

  return episode.temporalPlan.triggers
    .filter((trigger) => trigger.status === "pending")
    .map((trigger) =>
      OutboxIntentSchema.parse({
        ...outboxBase(signal, `cancel:${trigger.timerId}`),
        kind: "cancel_timer",
        timerId: trigger.timerId,
        episodeId: episode.episodeId,
        temporalPlanVersion: episode.temporalPlan?.definition.version,
      }),
    );
}

function evidencePersonalAdults(evidence: EvidenceRef[]): string[] {
  return sortedUnique(
    evidence.flatMap((item) => (item.scope.kind === "personal" ? [item.scope.adultId] : [])),
  );
}

function containsJobEvidence(evidence: EvidenceRef[]): boolean {
  return evidence.some((item) => item.scope.kind === "job");
}

function checkScopeWithoutPromotion(evidence: EvidenceRef[], targetScope: DurableScope): boolean {
  if (containsJobEvidence(evidence)) {
    return false;
  }

  const personalAdults = evidencePersonalAdults(evidence);
  if (targetScope.kind === "household") {
    return personalAdults.length === 0;
  }

  return personalAdults.every((adultId) => adultId === targetScope.adultId);
}

function checkPromotionAuthority(
  aggregate: HouseholdAggregate,
  evidence: EvidenceRef[],
  targetScope: DurableScope,
  sourceClass: string,
  sensitivity: keyof typeof SENSITIVITY_RANK,
  authority: PromotionAuthority | undefined,
  now: InstantString,
): AuthorityCheck {
  if (checkScopeWithoutPromotion(evidence, targetScope)) {
    return { ok: true };
  }

  if (containsJobEvidence(evidence) || targetScope.kind !== "household") {
    return { ok: false, reason: "invalid_promotion_authority" };
  }

  const personalAdults = evidencePersonalAdults(evidence);
  if (personalAdults.length !== 1) {
    return { ok: false, reason: "invalid_promotion_authority" };
  }

  if (authority === undefined) {
    return { ok: false, reason: "privacy_promotion_requires_authority" };
  }

  const sourceAdultId = personalAdults[0];
  if (authority.kind === "approval") {
    const index = aggregate.approvals.findIndex((approval) => approval.approvalId === authority.approvalId);
    const approval = aggregate.approvals[index];
    if (
      approval === undefined ||
      approval.status !== "active" ||
      approval.policyVersion !== aggregate.policyVersion ||
      approval.target.kind !== "scope_promotion" ||
      approval.target.from.adultId !== sourceAdultId ||
      approval.grantedByAdultId !== sourceAdultId ||
      !sameStrings(
        approval.target.evidenceIds,
        evidence.map((item) => item.evidenceId),
      )
    ) {
      return { ok: false, reason: "invalid_promotion_authority" };
    }
    if (instantCompare(now, approval.expiresAt) >= 0) {
      return { ok: false, reason: "approval_expired" };
    }
    return { ok: true, approvalIndex: index };
  }

  const policy = aggregate.policies.find((candidate) => candidate.policyId === authority.policyId);
  if (
    policy === undefined ||
    policy.status !== "active" ||
    policy.version !== authority.policyVersion ||
    authority.policyVersion > aggregate.policyVersion ||
    policy.rule.kind !== "sharing" ||
    policy.rule.from.adultId !== sourceAdultId ||
    policy.rule.sourceClass !== sourceClass ||
    SENSITIVITY_RANK[sensitivity] > SENSITIVITY_RANK[policy.rule.maximumSensitivity]
  ) {
    return { ok: false, reason: "invalid_promotion_authority" };
  }

  return { ok: true };
}

function consumeApproval(aggregate: HouseholdAggregate, approvalIndex: number | undefined): void {
  if (approvalIndex === undefined) {
    return;
  }
  const approval = aggregate.approvals[approvalIndex];
  if (approval !== undefined) {
    aggregate.approvals[approvalIndex] = { ...approval, status: "consumed" };
  }
}

function resolveEpisodeProposal(
  aggregate: HouseholdAggregate,
  proposal: EpisodeProposal,
  now: InstantString,
): { episode?: FamilyEpisode; authority?: AuthorityCheck; reason?: RejectionReason } {
  if (aggregate.episodes.some((episode) => episode.episodeId === proposal.episodeId)) {
    return { reason: "episode_already_exists" };
  }
  if (
    proposal.proposedOwnerAdultId !== undefined &&
    !isVerifiedAdult(aggregate, proposal.proposedOwnerAdultId)
  ) {
    return { reason: "owner_mismatch" };
  }
  if (
    proposal.targetScope.kind === "personal" &&
    proposal.proposedOwnerAdultId !== undefined &&
    proposal.targetScope.adultId !== proposal.proposedOwnerAdultId
  ) {
    return { reason: "owner_mismatch" };
  }

  const authority = checkPromotionAuthority(
    aggregate,
    proposal.evidence,
    proposal.targetScope,
    proposal.sourceClass,
    proposal.sensitivity,
    proposal.promotionAuthority,
    now,
  );
  if (!authority.ok) {
    return { authority, reason: authority.reason ?? "invalid_promotion_authority" };
  }

  let temporalPlan: ResolvedTimePlan | undefined;
  if (proposal.temporalPlan !== undefined) {
    if (proposal.temporalPlan.version !== 1) {
      return { authority, reason: "stale_temporal_plan" };
    }
    try {
      temporalPlan = HouseholdTime.resolve({
        plan: proposal.temporalPlan,
        routineAnchors: aggregate.routineAnchors,
      });
    } catch (error) {
      if (error instanceof HouseholdTimeError) {
        return { authority, reason: "invalid_transition" };
      }
      throw error;
    }
  }

  const owner =
    proposal.proposedOwnerAdultId === undefined
      ? ({ status: "unassigned" } as const)
      : ({ status: "proposed", adultId: proposal.proposedOwnerAdultId, proposedAt: now } as const);

  const episode = FamilyEpisodeSchema.parse({
    episodeId: proposal.episodeId,
    householdId: aggregate.householdId,
    type: proposal.type,
    version: 1,
    scope: proposal.targetScope,
    state: owner.status === "proposed" ? "awaiting_acknowledgement" : "proposed",
    title: proposal.title,
    requiredOutcome: proposal.requiredOutcome,
    owner,
    evidence: proposal.evidence,
    sourceClass: proposal.sourceClass,
    sensitivity: proposal.sensitivity,
    ...(proposal.promotionAuthority === undefined ? {} : { promotionAuthority: proposal.promotionAuthority }),
    ...(temporalPlan === undefined ? {} : { temporalPlan }),
    createdAt: now,
    updatedAt: now,
  });

  return { episode, authority };
}

function handleEpisodeProposed(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "episode.proposed") {
    throw new Error("wrong handler");
  }
  if (!isAdultActor(draft, signal.actor)) {
    return rejected("unauthorized_actor");
  }
  if (
    signal.proposal.targetScope.kind === "personal" &&
    signal.proposal.targetScope.adultId !== signal.actor.adultId
  ) {
    return rejected("unauthorized_actor");
  }

  const resolved = resolveEpisodeProposal(draft, signal.proposal, signal.occurredAt);
  if (resolved.episode === undefined) {
    return rejected(resolved.reason ?? "invalid_transition");
  }

  consumeApproval(draft, resolved.authority?.approvalIndex);
  draft.episodes.push(resolved.episode);
  return accepted(
    draft,
    [
      DomainChangeSchema.parse({
        kind: "episode_created",
        episodeId: resolved.episode.episodeId,
        state: resolved.episode.state,
      }),
    ],
    scheduleTimerEffects(signal, resolved.episode),
  );
}

function findEpisode(aggregate: HouseholdAggregate, episodeId: string): [number, FamilyEpisode] | undefined {
  const index = aggregate.episodes.findIndex((candidate) => candidate.episodeId === episodeId);
  const episode = aggregate.episodes[index];
  return episode === undefined ? undefined : [index, episode];
}

function handleOwnerAcknowledged(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "commitment.owner_acknowledged") {
    throw new Error("wrong handler");
  }
  if (!isAdultActor(draft, signal.actor)) {
    return rejected("unauthorized_actor");
  }

  const found = findEpisode(draft, signal.episodeId);
  if (found === undefined) {
    return rejected("episode_not_found");
  }
  const [index, episode] = found;
  if (episode.version !== signal.baseEpisodeVersion) {
    return rejected("stale_episode_version");
  }
  if (
    episode.state !== "awaiting_acknowledgement" ||
    episode.owner.status !== "proposed" ||
    episode.owner.adultId !== signal.actor.adultId
  ) {
    return rejected("owner_mismatch");
  }

  const updated = FamilyEpisodeSchema.parse({
    ...episode,
    version: episode.version + 1,
    state: "active",
    owner: {
      ...episode.owner,
      status: "acknowledged",
      acknowledgedAt: signal.occurredAt,
    },
    updatedAt: signal.occurredAt,
  });
  draft.episodes[index] = updated;
  return accepted(
    draft,
    [
      DomainChangeSchema.parse({
        kind: "episode_state_changed",
        episodeId: episode.episodeId,
        from: episode.state,
        to: updated.state,
        episodeVersion: updated.version,
      }),
    ],
    [],
  );
}

function handleOwnerReassigned(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "commitment.owner_reassigned") {
    throw new Error("wrong handler");
  }
  if (!isAdultActor(draft, signal.actor)) {
    return rejected("unauthorized_actor");
  }
  if (!isVerifiedAdult(draft, signal.proposedOwnerAdultId)) {
    return rejected("owner_mismatch");
  }

  const found = findEpisode(draft, signal.episodeId);
  if (found === undefined) {
    return rejected("episode_not_found");
  }
  const [index, episode] = found;
  if (episode.version !== signal.baseEpisodeVersion) {
    return rejected("stale_episode_version");
  }
  if (
    TERMINAL_STATES.has(episode.state) ||
    (episode.owner.status === "proposed" && episode.owner.adultId === signal.proposedOwnerAdultId) ||
    (episode.scope.kind === "personal" && episode.scope.adultId !== signal.proposedOwnerAdultId)
  ) {
    return rejected("invalid_transition");
  }

  const episodeWithoutBlockedReason = { ...episode };
  delete episodeWithoutBlockedReason.blockedReason;
  const updated = FamilyEpisodeSchema.parse({
    ...episodeWithoutBlockedReason,
    version: episode.version + 1,
    state: "awaiting_acknowledgement",
    owner: {
      status: "proposed",
      adultId: signal.proposedOwnerAdultId,
      proposedAt: signal.occurredAt,
    },
    updatedAt: signal.occurredAt,
  });
  draft.episodes[index] = updated;
  return accepted(
    draft,
    [
      DomainChangeSchema.parse({
        kind: "episode_state_changed",
        episodeId: episode.episodeId,
        from: episode.state,
        to: updated.state,
        episodeVersion: updated.version,
      }),
    ],
    [],
  );
}

function handleDeliveryObserved(context: HandlerContext): MutationResult {
  const { signal } = context;
  if (signal.kind !== "conversation.delivery_observed") {
    throw new Error("wrong handler");
  }
  if (signal.actor.kind !== "source_adapter" || signal.actor.source !== "linq") {
    return rejected("unauthorized_actor");
  }
  return ignored("delivery_is_not_approval");
}

function handleEpisodeClosed(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "episode.closed") {
    throw new Error("wrong handler");
  }
  const authorizedActor =
    isAdultActor(draft, signal.actor) ||
    (signal.actor.kind === "source_adapter" && signal.actor.source === "effect_executor");
  if (!authorizedActor) {
    return rejected("unauthorized_actor");
  }

  const found = findEpisode(draft, signal.episodeId);
  if (found === undefined) {
    return rejected("episode_not_found");
  }
  const [index, episode] = found;
  if (episode.version !== signal.baseEpisodeVersion) {
    return rejected("stale_episode_version");
  }
  if (TERMINAL_STATES.has(episode.state) || signal.outcome.recordedAt !== signal.occurredAt) {
    return rejected("invalid_transition");
  }
  if (
    signal.actor.kind === "source_adapter" &&
    !signal.outcome.evidence.some((item) => item.source === "effect")
  ) {
    return rejected("invalid_transition");
  }

  const effects = cancelTimerEffects(signal, episode);
  const temporalPlan =
    episode.temporalPlan === undefined
      ? undefined
      : {
          ...episode.temporalPlan,
          triggers: episode.temporalPlan.triggers.map((trigger) =>
            trigger.status === "pending" ? { ...trigger, status: "skipped" as const } : trigger,
          ),
        };
  const episodeWithoutBlockedReason = { ...episode };
  delete episodeWithoutBlockedReason.blockedReason;
  const updated = FamilyEpisodeSchema.parse({
    ...episodeWithoutBlockedReason,
    version: episode.version + 1,
    state: signal.outcome.kind,
    outcome: signal.outcome,
    ...(temporalPlan === undefined ? {} : { temporalPlan }),
    updatedAt: signal.occurredAt,
  });
  draft.episodes[index] = updated;

  return accepted(
    draft,
    [
      DomainChangeSchema.parse({
        kind: "episode_state_changed",
        episodeId: episode.episodeId,
        from: episode.state,
        to: updated.state,
        episodeVersion: updated.version,
      }),
    ],
    effects,
  );
}

function handleEpisodeSourceSuperseded(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "episode.source_superseded") {
    throw new Error("wrong handler");
  }
  if (
    signal.actor.kind !== "source_adapter" ||
    (signal.actor.source !== "gmail" && signal.actor.source !== "calendar")
  ) {
    return rejected("unauthorized_actor");
  }
  const source = signal.actor.source;

  const found = findEpisode(draft, signal.episodeId);
  if (found === undefined) {
    return rejected("episode_not_found");
  }
  const [index, episode] = found;
  if (episode.version !== signal.baseEpisodeVersion) {
    return rejected("stale_episode_version");
  }
  if (TERMINAL_STATES.has(episode.state)) {
    return rejected("invalid_transition");
  }

  const matchingEvidence = episode.evidence.filter(
    (evidence) => evidence.source === source && evidence.sourceRef === signal.supersedingEvidence.sourceRef,
  );
  const scopeMatches = matchingEvidence.every((evidence) =>
    evidence.scope.kind === "personal"
      ? signal.supersedingEvidence.scope.kind === "personal" &&
        signal.supersedingEvidence.scope.adultId === evidence.scope.adultId
      : signal.supersedingEvidence.scope.kind === evidence.scope.kind,
  );
  if (
    signal.supersedingEvidence.source !== source ||
    matchingEvidence.length === 0 ||
    !scopeMatches ||
    matchingEvidence.some((evidence) => evidence.revision >= signal.supersedingEvidence.revision) ||
    signal.supersedingEvidence.observedAt !== signal.occurredAt
  ) {
    return rejected("invalid_transition");
  }

  const effects = cancelTimerEffects(signal, episode);
  const temporalPlan =
    episode.temporalPlan === undefined
      ? undefined
      : {
          ...episode.temporalPlan,
          triggers: episode.temporalPlan.triggers.map((trigger) =>
            trigger.status === "pending" ? { ...trigger, status: "skipped" as const } : trigger,
          ),
        };
  const withoutBlockedReason = { ...episode };
  delete withoutBlockedReason.blockedReason;
  const updated = FamilyEpisodeSchema.parse({
    ...withoutBlockedReason,
    version: episode.version + 1,
    state: "superseded",
    outcome: {
      kind: "superseded",
      summary: "A newer source revision replaced this episode.",
      evidence: [signal.supersedingEvidence],
      recordedAt: signal.occurredAt,
    },
    ...(temporalPlan === undefined ? {} : { temporalPlan }),
    updatedAt: signal.occurredAt,
  });
  draft.episodes[index] = updated;

  return accepted(
    draft,
    [
      DomainChangeSchema.parse({
        kind: "episode_state_changed",
        episodeId: episode.episodeId,
        from: episode.state,
        to: "superseded",
        episodeVersion: updated.version,
      }),
    ],
    effects,
  );
}

function handleTemporalPlanReplaced(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "episode.temporal_plan_replaced") {
    throw new Error("wrong handler");
  }
  const authorizedActor =
    isAdultActor(draft, signal.actor) ||
    (signal.actor.kind === "source_adapter" && signal.actor.source === "calendar");
  if (!authorizedActor) {
    return rejected("unauthorized_actor");
  }

  const found = findEpisode(draft, signal.episodeId);
  if (found === undefined) {
    return rejected("episode_not_found");
  }
  const [index, episode] = found;
  if (episode.version !== signal.baseEpisodeVersion) {
    return rejected("stale_episode_version");
  }
  if (TERMINAL_STATES.has(episode.state)) {
    return rejected("invalid_transition");
  }
  const expectedVersion = (episode.temporalPlan?.definition.version ?? 0) + 1;
  if (signal.plan.version !== expectedVersion) {
    return rejected("stale_temporal_plan");
  }

  let temporalPlan: ResolvedTimePlan;
  try {
    temporalPlan = HouseholdTime.resolve({ plan: signal.plan, routineAnchors: draft.routineAnchors });
  } catch (error) {
    if (error instanceof HouseholdTimeError) {
      return rejected("invalid_transition");
    }
    throw error;
  }

  const effects = cancelTimerEffects(signal, episode);
  const updated = FamilyEpisodeSchema.parse({
    ...episode,
    version: episode.version + 1,
    temporalPlan,
    updatedAt: signal.occurredAt,
  });
  draft.episodes[index] = updated;
  effects.push(...scheduleTimerEffects(signal, updated));

  return accepted(
    draft,
    [
      DomainChangeSchema.parse({
        kind: "temporal_plan_replaced",
        episodeId: episode.episodeId,
        ...(episode.temporalPlan === undefined
          ? {}
          : { fromVersion: episode.temporalPlan.definition.version }),
        toVersion: temporalPlan.definition.version,
      }),
    ],
    effects,
  );
}

function episodeUsesAnyRoutineAnchor(episode: FamilyEpisode, anchorIds: ReadonlySet<string>): boolean {
  const definition = episode.temporalPlan?.definition;
  if (definition === undefined) return false;
  const moments = [
    definition.event,
    definition.deadline,
    definition.earliestUseful,
    definition.lastResponsible,
    ...definition.triggers.map((trigger) => trigger.at),
  ];
  return moments.some((moment) => moment?.kind === "routine_anchor" && anchorIds.has(moment.anchorId));
}

function timerIdForAnchorReplan(episodeId: string, triggerId: string, planVersion: number) {
  const digest = createHash("sha256")
    .update(`${episodeId}\u0000${triggerId}\u0000${planVersion}`)
    .digest("hex");
  return TimerIdSchema.parse(`timer_${digest}`);
}

function sameRoutineAnchor(left: RoutineAnchor, right: RoutineAnchor): boolean {
  return (
    left.anchorId === right.anchorId &&
    left.label === right.label &&
    left.timeZone === right.timeZone &&
    left.localTime === right.localTime &&
    sameStrings(left.daysOfWeek.map(String), right.daysOfWeek.map(String))
  );
}

function routineAnchorTimingChanged(left: RoutineAnchor, right: RoutineAnchor): boolean {
  return (
    left.timeZone !== right.timeZone ||
    left.localTime !== right.localTime ||
    !sameStrings(left.daysOfWeek.map(String), right.daysOfWeek.map(String))
  );
}

function handleRoutineAnchorsReplaced(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "routine_anchors.replaced") {
    throw new Error("wrong handler");
  }
  if (!isAdultActor(draft, signal.actor)) {
    return rejected("unauthorized_actor");
  }

  const incomingIds = signal.anchors.map((anchor) => anchor.anchorId);
  if (new Set(incomingIds).size !== incomingIds.length) {
    return rejected("duplicate_entity");
  }
  const anchors = [...signal.anchors].sort((left, right) => left.anchorId.localeCompare(right.anchorId));
  const beforeAnchors = [...draft.routineAnchors].sort((left, right) =>
    left.anchorId.localeCompare(right.anchorId),
  );
  if (
    anchors.length === beforeAnchors.length &&
    anchors.every((anchor, index) => {
      const before = beforeAnchors[index];
      return before !== undefined && sameRoutineAnchor(before, anchor);
    })
  ) {
    return rejected("duplicate_entity");
  }

  const nextById = new Map(anchors.map((anchor) => [anchor.anchorId, anchor] as const));
  const timingChangedIds = new Set<string>();
  for (const before of beforeAnchors) {
    const next = nextById.get(before.anchorId);
    if (next === undefined || routineAnchorTimingChanged(before, next)) {
      timingChangedIds.add(before.anchorId);
    }
  }

  const replacements: Array<{
    index: number;
    before: FamilyEpisode;
    after: FamilyEpisode;
  }> = [];
  for (const [index, episode] of draft.episodes.entries()) {
    if (
      TERMINAL_STATES.has(episode.state) ||
      !episodeUsesAnyRoutineAnchor(episode, timingChangedIds) ||
      episode.temporalPlan === undefined
    ) {
      continue;
    }
    const priorPlan = episode.temporalPlan;
    const planVersion = priorPlan.definition.version + 1;
    const definition = {
      ...priorPlan.definition,
      version: planVersion,
      triggers: priorPlan.definition.triggers.map((trigger) => ({
        ...trigger,
        timerId: timerIdForAnchorReplan(episode.episodeId, trigger.triggerId, planVersion),
      })),
    };
    let resolved: ResolvedTimePlan;
    try {
      const fresh = HouseholdTime.resolve({ plan: definition, routineAnchors: anchors });
      const priorStatus = new Map(
        priorPlan.triggers.map((trigger) => [trigger.triggerId, trigger.status] as const),
      );
      resolved = ResolvedTimePlanSchema.parse({
        ...fresh,
        triggers: fresh.triggers.map((trigger) => ({
          ...trigger,
          status: priorStatus.get(trigger.triggerId) ?? "pending",
        })),
      });
    } catch (error) {
      if (error instanceof HouseholdTimeError) {
        return rejected("invalid_transition");
      }
      throw error;
    }
    replacements.push({
      index,
      before: episode,
      after: FamilyEpisodeSchema.parse({
        ...episode,
        version: episode.version + 1,
        temporalPlan: resolved,
        updatedAt: signal.occurredAt,
      }),
    });
  }

  draft.routineAnchors = anchors;
  const changes: DomainChange[] = [
    DomainChangeSchema.parse({
      kind: "routine_anchors_replaced",
      anchorIds: anchors.map((anchor) => anchor.anchorId),
    }),
  ];
  const effects: OutboxIntent[] = [];
  for (const replacement of replacements) {
    effects.push(...cancelTimerEffects(signal, replacement.before));
    draft.episodes[replacement.index] = replacement.after;
    effects.push(...scheduleTimerEffects(signal, replacement.after));
    changes.push(
      DomainChangeSchema.parse({
        kind: "temporal_plan_replaced",
        episodeId: replacement.after.episodeId,
        fromVersion: replacement.before.temporalPlan?.definition.version,
        toVersion: replacement.after.temporalPlan?.definition.version,
      }),
    );
  }
  return accepted(draft, changes, effects);
}

function handleEpisodeBlocked(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "episode.blocked") {
    throw new Error("wrong handler");
  }
  if (!isAdultActor(draft, signal.actor)) {
    return rejected("unauthorized_actor");
  }
  const found = findEpisode(draft, signal.episodeId);
  if (found === undefined) {
    return rejected("episode_not_found");
  }
  const [index, episode] = found;
  if (episode.version !== signal.baseEpisodeVersion) {
    return rejected("stale_episode_version");
  }
  if (episode.state !== "active" || episode.owner.status !== "acknowledged") {
    return rejected("invalid_transition");
  }

  const updated = FamilyEpisodeSchema.parse({
    ...episode,
    version: episode.version + 1,
    state: "blocked",
    blockedReason: signal.reason,
    updatedAt: signal.occurredAt,
  });
  draft.episodes[index] = updated;
  return accepted(
    draft,
    [
      DomainChangeSchema.parse({
        kind: "episode_state_changed",
        episodeId: episode.episodeId,
        from: episode.state,
        to: updated.state,
        episodeVersion: updated.version,
      }),
    ],
    [],
  );
}

function handleEpisodeResumed(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "episode.resumed") {
    throw new Error("wrong handler");
  }
  if (!isAdultActor(draft, signal.actor)) {
    return rejected("unauthorized_actor");
  }
  const found = findEpisode(draft, signal.episodeId);
  if (found === undefined) {
    return rejected("episode_not_found");
  }
  const [index, episode] = found;
  if (episode.version !== signal.baseEpisodeVersion) {
    return rejected("stale_episode_version");
  }
  if (episode.state !== "blocked" || episode.owner.status !== "acknowledged") {
    return rejected("invalid_transition");
  }

  const episodeWithoutBlockedReason = { ...episode };
  delete episodeWithoutBlockedReason.blockedReason;
  const updated = FamilyEpisodeSchema.parse({
    ...episodeWithoutBlockedReason,
    version: episode.version + 1,
    state: "active",
    updatedAt: signal.occurredAt,
  });
  draft.episodes[index] = updated;
  return accepted(
    draft,
    [
      DomainChangeSchema.parse({
        kind: "episode_state_changed",
        episodeId: episode.episodeId,
        from: episode.state,
        to: updated.state,
        episodeVersion: updated.version,
      }),
    ],
    [],
  );
}

function validateApprovalRecord(
  aggregate: HouseholdAggregate,
  approval: ApprovalRecord,
  actorAdultId: string,
  now: InstantString,
): RejectionReason | undefined {
  if (
    approval.householdId !== aggregate.householdId ||
    approval.grantedByAdultId !== actorAdultId ||
    approval.status !== "active" ||
    approval.grantedAt !== now
  ) {
    return "approval_invalid";
  }
  if (approval.policyVersion !== aggregate.policyVersion) {
    return "stale_policy_version";
  }
  if (instantCompare(now, approval.expiresAt) >= 0) {
    return "approval_expired";
  }
  if (
    approval.target.kind === "scope_promotion" &&
    (approval.target.from.adultId !== actorAdultId ||
      !sameStrings(approval.target.evidenceIds, sortedUnique(approval.target.evidenceIds)))
  ) {
    return "approval_invalid";
  }
  return undefined;
}

function checkActionApproval(
  aggregate: HouseholdAggregate,
  action: ExternalAction,
  approvalId: string,
  now: InstantString,
): { index?: number; reason?: RejectionReason } {
  const index = aggregate.approvals.findIndex((approval) => approval.approvalId === approvalId);
  const approval = aggregate.approvals[index];
  if (approval === undefined || approval.status !== "active") {
    return { reason: "approval_invalid" };
  }
  if (approval.policyVersion !== aggregate.policyVersion) {
    return { reason: "stale_policy_version" };
  }
  if (instantCompare(now, approval.expiresAt) >= 0) {
    return { reason: "approval_expired" };
  }
  if (
    approval.target.kind !== "external_action" ||
    approval.target.actionId !== action.actionId ||
    approval.target.actionDigest !== action.actionDigest ||
    approval.target.relevantDataDigest !== action.relevantDataDigest
  ) {
    return { reason: "action_digest_mismatch" };
  }
  if (action.requestedFor.kind === "personal" && approval.grantedByAdultId !== action.requestedFor.adultId) {
    return { reason: "approval_invalid" };
  }
  return { index };
}

function executeActionEffect(
  signal: HouseholdSignal,
  action: ExternalAction,
  approvalId: string,
): OutboxIntent {
  return OutboxIntentSchema.parse({
    ...outboxBase(signal, `execute:${action.actionId}`),
    kind: "execute_external_action",
    action,
    approvalId: ApprovalIdSchema.parse(approvalId),
  });
}

function handleApprovalGranted(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "approval.granted") {
    throw new Error("wrong handler");
  }
  if (!isAdultActor(draft, signal.actor)) {
    return rejected("unauthorized_actor");
  }
  if (draft.approvals.some((candidate) => candidate.approvalId === signal.approval.approvalId)) {
    return rejected("duplicate_entity");
  }
  const invalid = validateApprovalRecord(draft, signal.approval, signal.actor.adultId, signal.occurredAt);
  if (invalid !== undefined) {
    return rejected(invalid);
  }

  const approvalIndex = draft.approvals.push(signal.approval) - 1;
  const changes: DomainChange[] = [];
  const effects: OutboxIntent[] = [];
  let status: "active" | "consumed" = "active";

  if (signal.approval.target.kind === "external_action") {
    const target = signal.approval.target;
    const pendingIndex = draft.pendingActions.findIndex(
      (candidate) => candidate.action.actionId === target.actionId,
    );
    const pending = draft.pendingActions[pendingIndex];
    if (pending !== undefined && pending.state === "awaiting_approval") {
      if (
        pending.action.requestedFor.kind === "personal" &&
        pending.action.requestedFor.adultId !== signal.actor.adultId
      ) {
        return rejected("approval_invalid");
      }
      if (
        pending.action.actionDigest !== target.actionDigest ||
        pending.action.relevantDataDigest !== target.relevantDataDigest
      ) {
        return rejected("action_digest_mismatch");
      }
      draft.approvals[approvalIndex] = { ...signal.approval, status: "consumed" };
      draft.pendingActions[pendingIndex] = {
        ...pending,
        state: "executing",
        approvalId: signal.approval.approvalId,
        updatedAt: signal.occurredAt,
      };
      status = "consumed";
      effects.push(executeActionEffect(signal, pending.action, signal.approval.approvalId));
      changes.push(
        DomainChangeSchema.parse({
          kind: "action_state_changed",
          actionId: pending.action.actionId,
          state: "executing",
        }),
      );
    }
  }

  changes.unshift(
    DomainChangeSchema.parse({
      kind: "approval_recorded",
      approvalId: signal.approval.approvalId,
      status,
    }),
  );
  return accepted(draft, changes, effects);
}

function handleApprovalRevoked(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "approval.revoked") {
    throw new Error("wrong handler");
  }
  if (!isAdultActor(draft, signal.actor)) {
    return rejected("unauthorized_actor");
  }
  const index = draft.approvals.findIndex((approval) => approval.approvalId === signal.approvalId);
  const approval = draft.approvals[index];
  if (
    approval === undefined ||
    approval.status !== "active" ||
    approval.grantedByAdultId !== signal.actor.adultId
  ) {
    return rejected("approval_invalid");
  }

  draft.approvals[index] = { ...approval, status: "revoked" };
  return accepted(
    draft,
    [
      DomainChangeSchema.parse({
        kind: "approval_recorded",
        approvalId: approval.approvalId,
        status: "revoked",
      }),
    ],
    [],
  );
}

function samePolicyRule(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function adultOwnsPersonalPolicy(
  rule: HouseholdAggregate["policies"][number]["rule"],
  adultId: string,
): boolean {
  switch (rule.kind) {
    case "sharing":
      return rule.from.adultId === adultId;
    case "routing":
    case "timing":
      return rule.scope.kind !== "personal" || rule.scope.adultId === adultId;
    case "internal_action":
      return true;
  }
}

function handlePolicyApproved(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "policy.approved") {
    throw new Error("wrong handler");
  }
  if (!isAdultActor(draft, signal.actor)) {
    return rejected("unauthorized_actor");
  }
  if (
    signal.policy.householdId !== draft.householdId ||
    signal.policy.status !== "active" ||
    signal.policy.approvedByAdultId !== signal.actor.adultId ||
    signal.policy.approvedAt !== signal.occurredAt ||
    signal.policy.version !== draft.policyVersion + 1 ||
    draft.policies.some((candidate) => candidate.policyId === signal.policy.policyId)
  ) {
    return rejected("policy_invalid");
  }
  if (!adultOwnsPersonalPolicy(signal.policy.rule, signal.actor.adultId)) {
    return rejected("policy_invalid");
  }

  if (signal.candidateId !== undefined) {
    const candidateIndex = draft.policyCandidates.findIndex(
      (candidate) => candidate.candidateId === signal.candidateId,
    );
    const candidate = draft.policyCandidates[candidateIndex];
    if (
      candidate === undefined ||
      candidate.basePolicyVersion !== draft.policyVersion ||
      !samePolicyRule(candidate.rule, signal.policy.rule)
    ) {
      return rejected("candidate_not_found");
    }
    draft.policyCandidates.splice(candidateIndex, 1);
  }

  draft.policies.push(signal.policy);
  draft.policyVersion += 1;
  return accepted(
    draft,
    [
      DomainChangeSchema.parse({
        kind: "policy_changed",
        policyId: signal.policy.policyId,
        status: "active",
        policyVersion: draft.policyVersion,
      }),
    ],
    [],
  );
}

function handlePolicyRevoked(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "policy.revoked") {
    throw new Error("wrong handler");
  }
  if (!isAdultActor(draft, signal.actor)) {
    return rejected("unauthorized_actor");
  }
  if (signal.expectedPolicyVersion !== draft.policyVersion) {
    return rejected("stale_policy_version");
  }

  const index = draft.policies.findIndex((candidate) => candidate.policyId === signal.policyId);
  const policy = draft.policies[index];
  if (policy === undefined || policy.status !== "active") {
    return rejected("policy_invalid");
  }
  if (!adultOwnsPersonalPolicy(policy.rule, signal.actor.adultId)) {
    return rejected("unauthorized_actor");
  }

  draft.policies[index] = { ...policy, status: "revoked", revokedAt: signal.occurredAt };
  draft.policyVersion += 1;
  return accepted(
    draft,
    [
      DomainChangeSchema.parse({
        kind: "policy_changed",
        policyId: policy.policyId,
        status: "revoked",
        policyVersion: draft.policyVersion,
      }),
    ],
    [],
  );
}

function handleMemoryConfirmed(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "memory.confirmed") {
    throw new Error("wrong handler");
  }
  if (!isAdultActor(draft, signal.actor)) {
    return rejected("unauthorized_actor");
  }
  if (draft.memories.some((candidate) => candidate.memoryId === signal.memoryId)) {
    return rejected("duplicate_entity");
  }

  const candidateIndex = draft.memoryCandidates.findIndex(
    (candidate) => candidate.candidateId === signal.candidateId,
  );
  const candidate = draft.memoryCandidates[candidateIndex];
  if (candidate === undefined) {
    return rejected("candidate_not_found");
  }
  if (candidate.scope.kind === "personal" && candidate.scope.adultId !== signal.actor.adultId) {
    return rejected("unauthorized_actor");
  }

  const authority = checkPromotionAuthority(
    draft,
    candidate.evidence,
    signal.targetScope,
    candidate.sourceClass,
    candidate.sensitivity,
    signal.promotionAuthority,
    signal.occurredAt,
  );
  if (!authority.ok) {
    return rejected(authority.reason ?? "invalid_promotion_authority");
  }

  consumeApproval(draft, authority.approvalIndex);
  const memory = DurableMemorySchema.parse({
    memoryId: MemoryIdSchema.parse(signal.memoryId),
    householdId: draft.householdId,
    kind: candidate.kind,
    statement: candidate.statement,
    scope: signal.targetScope,
    sourceClass: candidate.sourceClass,
    evidence: candidate.evidence,
    confidence: candidate.confidence,
    sensitivity: candidate.sensitivity,
    validFrom: candidate.validFrom,
    ...(candidate.expiresAt === undefined ? {} : { expiresAt: candidate.expiresAt }),
    confirmedByAdultId: signal.actor.adultId,
    confirmedAt: signal.occurredAt,
    ...(signal.promotionAuthority === undefined ? {} : { promotionAuthority: signal.promotionAuthority }),
    status: "active",
  });
  draft.memoryCandidates.splice(candidateIndex, 1);
  draft.memories.push(memory);

  return accepted(
    draft,
    [DomainChangeSchema.parse({ kind: "memory_confirmed", memoryId: memory.memoryId })],
    [],
  );
}

function approvalRequestEffect(
  signal: HouseholdSignal,
  action: ExternalAction,
  promotionAuthority: PromotionAuthority | undefined,
): OutboxIntent {
  const body =
    action.kind === "calendar_update"
      ? `I’m ready to add “${action.title}” from ${action.startsAt} to ${action.endsAt} (${action.timeZone}) on the requesting adult’s primary calendar. ${
          action.hasConflict
            ? "One or more private household calendars are busy then; no private calendar details were shared. "
            : "The current private household availability projection is clear. "
        }Reply “approve ${action.actionId}” to create this exact event.`
      : `Approval is required before Florence can ${action.summary}.`;
  return OutboxIntentSchema.parse({
    ...outboxBase(signal, `approval-request:${action.actionId}`),
    kind: "send_message",
    targetScope: action.requestedFor,
    messageClass: "approval_request",
    body,
    evidenceIds: action.evidence.map((item) => item.evidenceId),
    ...(promotionAuthority === undefined ? {} : { promotionAuthority }),
  });
}

function handleExternalActionProposed(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "external_action.proposed") {
    throw new Error("wrong handler");
  }
  if (
    !isAdultActor(draft, signal.actor) ||
    signal.action.kind !== "calendar_update" ||
    signal.action.householdId !== draft.householdId ||
    signal.action.requestedByAdultId !== signal.actor.adultId ||
    !sameStrings(signal.action.availabilityAdultIds, draft.verifiedAdultIds) ||
    signal.action.requestedFor.kind !== "household" ||
    !checkScopeWithoutPromotion(signal.action.evidence, signal.action.requestedFor)
  ) {
    return rejected("unauthorized_actor");
  }
  if (draft.pendingActions.some((pending) => pending.action.actionId === signal.action.actionId)) {
    return rejected("duplicate_entity");
  }
  draft.pendingActions.push({
    action: signal.action,
    state: "awaiting_approval",
    proposedAt: signal.occurredAt,
    updatedAt: signal.occurredAt,
  });
  return accepted(
    draft,
    [
      DomainChangeSchema.parse({
        kind: "action_state_changed",
        actionId: signal.action.actionId,
        state: "awaiting_approval",
      }),
    ],
    [approvalRequestEffect(signal, signal.action, undefined)],
  );
}

function handleWorkerProposal(context: HandlerContext): MutationResult {
  const { signal, before, draft } = context;
  if (signal.kind !== "worker.proposal_received") {
    throw new Error("wrong handler");
  }
  const proposal = signal.proposal;
  if (
    signal.actor.kind !== "worker" ||
    signal.actor.jobId !== proposal.jobId ||
    proposal.householdId !== draft.householdId
  ) {
    return rejected("unauthorized_actor");
  }
  if (proposal.baseHouseholdVersion !== before.version) {
    return rejected("stale_household_version");
  }
  if (proposal.basePolicyVersion !== before.policyVersion) {
    return rejected("stale_policy_version");
  }

  const changes: DomainChange[] = [];
  const effects: OutboxIntent[] = [];

  for (const episodeProposal of proposal.episodeProposals) {
    const resolved = resolveEpisodeProposal(draft, episodeProposal, signal.occurredAt);
    if (resolved.episode === undefined) {
      return rejected(resolved.reason ?? "invalid_transition");
    }
    consumeApproval(draft, resolved.authority?.approvalIndex);
    draft.episodes.push(resolved.episode);
    changes.push(
      DomainChangeSchema.parse({
        kind: "episode_created",
        episodeId: resolved.episode.episodeId,
        state: resolved.episode.state,
      }),
    );
    effects.push(...scheduleTimerEffects(signal, resolved.episode));
  }

  for (const message of proposal.messageProposals) {
    const authority = checkPromotionAuthority(
      draft,
      message.evidence,
      message.targetScope,
      message.sourceClass,
      message.sensitivity,
      message.promotionAuthority,
      signal.occurredAt,
    );
    if (!authority.ok) {
      return rejected(authority.reason ?? "invalid_promotion_authority");
    }
    consumeApproval(draft, authority.approvalIndex);
    effects.push(
      OutboxIntentSchema.parse({
        ...outboxBase(signal, `message:${message.proposalId}`),
        kind: "send_message",
        targetScope: message.targetScope,
        messageClass: message.purpose === "clarifying_question" ? "clarifying_question" : "status",
        body: message.body,
        evidenceIds: message.evidence.map((item) => item.evidenceId),
        ...(message.promotionAuthority === undefined
          ? {}
          : { promotionAuthority: message.promotionAuthority }),
      }),
    );
  }

  for (const proposed of proposal.actionProposals) {
    if (proposed.action.kind === "calendar_update") {
      return rejected("unauthorized_actor");
    }
    if (draft.pendingActions.some((pending) => pending.action.actionId === proposed.action.actionId)) {
      return rejected("duplicate_entity");
    }
    const authority = checkPromotionAuthority(
      draft,
      proposed.action.evidence,
      proposed.action.requestedFor,
      "external_action",
      "sensitive",
      proposed.promotionAuthority,
      signal.occurredAt,
    );
    if (!authority.ok) {
      return rejected(authority.reason ?? "invalid_promotion_authority");
    }
    consumeApproval(draft, authority.approvalIndex);

    let state: PendingExternalAction["state"] = "awaiting_approval";
    let approvalId: ApprovalRecord["approvalId"] | undefined;
    if (proposed.approvalId !== undefined) {
      const actionApproval = checkActionApproval(
        draft,
        proposed.action,
        proposed.approvalId,
        signal.occurredAt,
      );
      if (actionApproval.index === undefined) {
        return rejected(actionApproval.reason ?? "approval_invalid");
      }
      consumeApproval(draft, actionApproval.index);
      state = "executing";
      approvalId = proposed.approvalId;
      effects.push(executeActionEffect(signal, proposed.action, proposed.approvalId));
    } else {
      effects.push(approvalRequestEffect(signal, proposed.action, proposed.promotionAuthority));
    }

    draft.pendingActions.push({
      action: proposed.action,
      state,
      ...(approvalId === undefined ? {} : { approvalId }),
      ...(proposed.promotionAuthority === undefined
        ? {}
        : { promotionAuthority: proposed.promotionAuthority }),
      proposedAt: signal.occurredAt,
      updatedAt: signal.occurredAt,
    });
    changes.push(
      DomainChangeSchema.parse({
        kind: "action_state_changed",
        actionId: proposed.action.actionId,
        state,
      }),
    );
  }

  for (const candidate of proposal.memoryCandidates) {
    if (
      candidate.householdId !== draft.householdId ||
      candidate.proposedBy.kind !== "worker" ||
      candidate.proposedBy.jobId !== proposal.jobId ||
      draft.memoryCandidates.some((current) => current.candidateId === candidate.candidateId) ||
      !checkScopeWithoutPromotion(candidate.evidence, candidate.scope)
    ) {
      return rejected("worker_cannot_promote");
    }
    draft.memoryCandidates.push(candidate);
    changes.push(
      DomainChangeSchema.parse({
        kind: "memory_candidate_recorded",
        candidateId: MemoryCandidateIdSchema.parse(candidate.candidateId),
      }),
    );
  }

  for (const candidate of proposal.policyCandidates) {
    if (
      candidate.householdId !== draft.householdId ||
      candidate.proposedByJobId !== proposal.jobId ||
      candidate.basePolicyVersion !== before.policyVersion ||
      draft.policyCandidates.some((current) => current.candidateId === candidate.candidateId)
    ) {
      return rejected("duplicate_entity");
    }
    draft.policyCandidates.push(candidate);
    changes.push(
      DomainChangeSchema.parse({
        kind: "policy_candidate_recorded",
        candidateId: PolicyCandidateIdSchema.parse(candidate.candidateId),
      }),
    );
  }

  return accepted(draft, changes, effects);
}

function reminderBody(episode: FamilyEpisode, missedWindow: boolean): string {
  if (missedWindow) {
    return `The “${episode.title}” commitment has passed its last responsible moment and is still open. Is it handled, or should we reassign it?`;
  }
  if (episode.state === "awaiting_acknowledgement") {
    return `The “${episode.title}” commitment is awaiting an owner acknowledgment. Is it handled, or should we reassign it?`;
  }
  return `The “${episode.title}” commitment is still open. Is it handled, or should we reassign it?`;
}

function handleTimerFired(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "timer.fired") {
    throw new Error("wrong handler");
  }
  if (signal.actor.kind !== "source_adapter" || signal.actor.source !== "system_clock") {
    return rejected("unauthorized_actor");
  }

  const found = findEpisode(draft, signal.episodeId);
  if (found === undefined) {
    return rejected("episode_not_found");
  }
  const [episodeIndex, episode] = found;
  if (episode.temporalPlan === undefined) {
    return rejected("stale_temporal_plan");
  }
  if (episode.temporalPlan.definition.version !== signal.temporalPlanVersion) {
    return rejected("stale_temporal_plan");
  }
  if (TERMINAL_STATES.has(episode.state)) {
    return ignored("timer_no_longer_relevant");
  }
  const triggerIndex = episode.temporalPlan.triggers.findIndex(
    (trigger) => trigger.triggerId === signal.triggerId && trigger.timerId === signal.timerId,
  );
  const trigger = episode.temporalPlan.triggers[triggerIndex];
  if (trigger === undefined || trigger.status !== "pending") {
    return ignored("timer_no_longer_relevant");
  }

  const evaluation = HouseholdTime.evaluateTimer({
    plan: episode.temporalPlan,
    triggerId: signal.triggerId,
    now: signal.firedAt,
  });
  if (evaluation.decision === "not_due") {
    return rejected("timer_not_due");
  }
  if (evaluation.decision === "obsolete") {
    return ignored("timer_no_longer_relevant");
  }

  const updatedTriggers = episode.temporalPlan.triggers.map((candidate, index) =>
    index === triggerIndex ? { ...candidate, status: "emitted" as const } : candidate,
  );
  const updated = FamilyEpisodeSchema.parse({
    ...episode,
    version: episode.version + 1,
    temporalPlan: { ...episode.temporalPlan, triggers: updatedTriggers },
    updatedAt: signal.firedAt,
  });
  draft.episodes[episodeIndex] = updated;

  const missedWindow = evaluation.decision === "missed_window";
  return accepted(
    draft,
    [
      DomainChangeSchema.parse({
        kind: "reminder_decided",
        episodeId: episode.episodeId,
        triggerId: signal.triggerId,
        decision: missedWindow ? "missed_window" : "remind",
      }),
    ],
    [
      OutboxIntentSchema.parse({
        ...outboxBase(signal, `reminder:${signal.timerId}`),
        kind: "send_message",
        targetScope: episode.scope,
        messageClass: missedWindow ? "missed_window" : "reminder",
        body: reminderBody(episode, missedWindow),
        episodeId: EpisodeIdSchema.parse(episode.episodeId),
      }),
    ],
  );
}

function handleEffectReceipt(context: HandlerContext): MutationResult {
  const { signal, draft } = context;
  if (signal.kind !== "effect.receipt_received") {
    throw new Error("wrong handler");
  }
  if (signal.actor.kind !== "source_adapter" || signal.actor.source !== "effect_executor") {
    return rejected("unauthorized_actor");
  }
  const index = draft.pendingActions.findIndex((pending) => pending.action.actionId === signal.actionId);
  const pending = draft.pendingActions[index];
  if (
    pending === undefined ||
    pending.action.actionDigest !== signal.actionDigest ||
    !["executing", "unknown"].includes(pending.state) ||
    signal.recordedAt !== signal.occurredAt
  ) {
    return rejected("effect_receipt_invalid");
  }

  draft.pendingActions[index] = {
    ...pending,
    state: signal.outcome,
    updatedAt: signal.recordedAt,
    effectReceipt: {
      receiptId: signal.receiptId,
      outcome: signal.outcome,
      recordedAt: signal.recordedAt,
      ...(signal.providerReference === undefined ? {} : { providerReference: signal.providerReference }),
    },
  };
  const effects: OutboxIntent[] = [];
  if (pending.action.kind === "calendar_update") {
    const body =
      signal.outcome === "succeeded"
        ? `Added “${pending.action.title}” to the calendar from ${pending.action.startsAt} to ${pending.action.endsAt} (${pending.action.timeZone}).`
        : signal.outcome === "failed"
          ? `I couldn’t add “${pending.action.title}” to the calendar. No calendar event was confirmed.`
          : `I couldn’t confirm whether “${pending.action.title}” was added to the calendar. Please check before trying again.`;
    effects.push(
      OutboxIntentSchema.parse({
        ...outboxBase(signal, `action-status:${pending.action.actionId}:${signal.outcome}`),
        kind: "send_message",
        targetScope: pending.action.requestedFor,
        messageClass: "status",
        body,
        evidenceIds: pending.action.evidence.map((item) => item.evidenceId),
      }),
    );
  }
  return accepted(
    draft,
    [
      DomainChangeSchema.parse({
        kind: "action_state_changed",
        actionId: pending.action.actionId,
        state: signal.outcome,
      }),
    ],
    effects,
  );
}

function dispatch(context: HandlerContext): MutationResult {
  switch (context.signal.kind) {
    case "episode.proposed":
      return handleEpisodeProposed(context);
    case "commitment.owner_acknowledged":
      return handleOwnerAcknowledged(context);
    case "commitment.owner_reassigned":
      return handleOwnerReassigned(context);
    case "conversation.delivery_observed":
      return handleDeliveryObserved(context);
    case "episode.closed":
      return handleEpisodeClosed(context);
    case "episode.source_superseded":
      return handleEpisodeSourceSuperseded(context);
    case "episode.temporal_plan_replaced":
      return handleTemporalPlanReplaced(context);
    case "routine_anchors.replaced":
      return handleRoutineAnchorsReplaced(context);
    case "episode.blocked":
      return handleEpisodeBlocked(context);
    case "episode.resumed":
      return handleEpisodeResumed(context);
    case "external_action.proposed":
      return handleExternalActionProposed(context);
    case "approval.granted":
      return handleApprovalGranted(context);
    case "approval.revoked":
      return handleApprovalRevoked(context);
    case "policy.approved":
      return handlePolicyApproved(context);
    case "policy.revoked":
      return handlePolicyRevoked(context);
    case "memory.confirmed":
      return handleMemoryConfirmed(context);
    case "worker.proposal_received":
      return handleWorkerProposal(context);
    case "timer.fired":
      return handleTimerFired(context);
    case "effect.receipt_received":
      return handleEffectReceipt(context);
  }
}

function sequenceRejection(
  current: HouseholdAggregate,
  signal: HouseholdSignal,
  reason: RejectionReason,
): AcceptanceResult {
  return AcceptanceResultSchema.parse({
    receipt: {
      householdId: current.householdId,
      signalId: signal.signalId,
      sequence: signal.sequence,
      aggregateVersion: current.version,
      disposition: "rejected",
      reason,
    },
    aggregate: current,
    changes: [{ kind: "signal_rejected", reason }],
    effects: [],
  });
}

function acceptSignal(input: HouseholdChiefOfStaffInput): AcceptanceResult {
  const parsed = HouseholdChiefOfStaffInputSchema.parse(input);
  const { current, signal } = parsed;

  if (signal.householdId !== current.householdId) {
    return sequenceRejection(current, signal, "household_mismatch");
  }
  if (signal.sequence <= current.lastProcessedSequence) {
    return sequenceRejection(current, signal, "duplicate_signal");
  }
  if (signal.sequence !== current.lastProcessedSequence + 1) {
    return sequenceRejection(current, signal, "out_of_order_signal");
  }

  const consumed = HouseholdAggregateSchema.parse({
    ...current,
    version: current.version + 1,
    lastProcessedSequence: signal.sequence,
  });
  const draft = HouseholdAggregateSchema.parse(consumed);
  const mutation = dispatch({ before: current, draft, signal });

  if (mutation.disposition === "rejected") {
    return AcceptanceResultSchema.parse({
      receipt: {
        householdId: current.householdId,
        signalId: signal.signalId,
        sequence: signal.sequence,
        aggregateVersion: consumed.version,
        disposition: "rejected",
        reason: mutation.reason,
      },
      aggregate: consumed,
      changes: [{ kind: "signal_rejected", reason: mutation.reason }],
      effects: [],
    });
  }

  if (mutation.disposition === "ignored") {
    return AcceptanceResultSchema.parse({
      receipt: {
        householdId: current.householdId,
        signalId: signal.signalId,
        sequence: signal.sequence,
        aggregateVersion: consumed.version,
        disposition: "ignored",
        ...(mutation.reason === "timer_no_longer_relevant" ? { reason: "timer_no_longer_relevant" } : {}),
      },
      aggregate: consumed,
      changes: [{ kind: "signal_ignored", reason: mutation.reason }],
      effects: [],
    });
  }

  return AcceptanceResultSchema.parse({
    receipt: {
      householdId: current.householdId,
      signalId: signal.signalId,
      sequence: signal.sequence,
      aggregateVersion: mutation.aggregate.version,
      disposition: "accepted",
    },
    aggregate: mutation.aggregate,
    changes: mutation.changes,
    effects: mutation.effects,
  });
}

export interface HouseholdChiefOfStaffModule {
  accept(input: HouseholdChiefOfStaffInput): AcceptanceResult;
}

/**
 * The only domain writer. It performs no I/O: ordered signals and current state in,
 * validated aggregate changes and app-owned outbox intents out.
 */
export const HouseholdChiefOfStaff: HouseholdChiefOfStaffModule = Object.freeze({
  accept: acceptSignal,
});
