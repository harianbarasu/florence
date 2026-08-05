import { createHash } from "node:crypto";
import type { HouseholdApplicationSnapshot, SharedProfileFact } from "../application/index.js";
import type {
  DurableMemory,
  FamilyEpisode,
  HouseholdAggregate,
  PolicyRecord,
  RoutineAnchor,
} from "../domain/index.js";

export type PrivateControlKind = "memory" | "profile" | "routine" | "sharing_rule" | "sharing_choice";

export interface PrivateKnowledgeItem {
  readonly controlId: string;
  readonly kind: "memory" | "profile" | "routine";
  readonly scope: "personal" | "household";
  readonly statement: string;
  readonly sourceLabel: string;
  readonly asOf: string | null;
}

export interface PrivateSharingRuleItem {
  readonly controlId: string;
  readonly policyId: string;
  readonly policyVersion: number;
  readonly sourceLabel: "Gmail" | "Calendar";
  readonly sourceClass: string;
  readonly maximumSensitivity: "ordinary" | "sensitive";
  readonly asOf: string;
}

export interface PrivateSharingChoiceItem {
  readonly controlId: string;
  readonly episodeId: string;
  readonly summary: string;
  readonly sourceLabel: string;
  readonly authorityLabel: string;
  readonly asOf: string;
}

export type PrivateControlResolution<T> =
  | { readonly status: "active"; readonly value: T }
  | { readonly status: "inactive" }
  | { readonly status: "unknown" }
  | { readonly status: "ambiguous" };

const PREFIXES = {
  memory: "MEM",
  profile: "FACT",
  routine: "ROUTINE",
  sharing_rule: "RULE",
  sharing_choice: "SHARE",
} as const satisfies Record<PrivateControlKind, string>;

/** Stable, non-reversible IDs let people control records without exposing canonical database IDs. */
export function privateControlId(kind: PrivateControlKind, canonicalId: string): string {
  const digest = createHash("sha256")
    .update("florence:private-control:v1\0")
    .update(kind)
    .update("\0")
    .update(canonicalId)
    .digest("hex")
    .slice(0, 16)
    .toUpperCase();
  return `${PREFIXES[kind]}-${digest}`;
}

/**
 * Read-only, owner-scoped view over canonical application state. It deliberately
 * has no connector dependencies and never exposes another adult's private records.
 */
export class PrivateControlCatalog {
  public listKnowledge(
    snapshot: HouseholdApplicationSnapshot,
    adultId: string,
    asOf: string,
  ): readonly PrivateKnowledgeItem[] {
    if (!isVerifiedAdult(snapshot.aggregate, adultId)) return [];

    const memories = snapshot.aggregate.memories
      .filter((memory) => isMemoryActive(memory, asOf) && canViewMemory(memory, adultId))
      .map((memory) => knowledgeFromMemory(memory));
    const facts = snapshot.projection.sharedProfile.facts
      .filter((fact) => fact.category !== "routine_anchor")
      .map((fact) => knowledgeFromProfile(fact));
    const routines = snapshot.aggregate.routineAnchors.map((anchor) =>
      knowledgeFromRoutine(anchor, snapshot.projection.sharedProfile.facts),
    );

    return ensureUniqueControlIds([...memories, ...facts, ...routines]).sort(compareKnowledge);
  }

  public listSharingRules(
    snapshot: HouseholdApplicationSnapshot,
    adultId: string,
  ): readonly PrivateSharingRuleItem[] {
    if (!isVerifiedAdult(snapshot.aggregate, adultId)) return [];
    const rules = snapshot.aggregate.policies.flatMap((policy) =>
      isOwnedActiveSharingPolicy(policy, adultId) ? [sharingRule(policy)] : [],
    );
    return ensureUniqueControlIds(rules).sort(
      (left, right) => right.asOf.localeCompare(left.asOf) || left.controlId.localeCompare(right.controlId),
    );
  }

  public listRecentSharingChoices(
    snapshot: HouseholdApplicationSnapshot,
    adultId: string,
    limit = 5,
  ): readonly PrivateSharingChoiceItem[] {
    if (!isVerifiedAdult(snapshot.aggregate, adultId)) return [];
    const choices = snapshot.aggregate.episodes.flatMap((episode) =>
      isOwnerPrivatePromotion(episode, adultId) ? [sharingChoice(episode, snapshot.aggregate.policies)] : [],
    );
    return ensureUniqueControlIds(choices)
      .sort(
        (left, right) => right.asOf.localeCompare(left.asOf) || left.controlId.localeCompare(right.controlId),
      )
      .slice(0, Math.max(0, Math.min(20, Math.trunc(limit))));
  }

  public resolveMemory(
    snapshot: HouseholdApplicationSnapshot,
    adultId: string,
    rawControlId: string,
    asOf: string,
  ): PrivateControlResolution<DurableMemory> {
    if (!isVerifiedAdult(snapshot.aggregate, adultId)) return { status: "unknown" };
    return resolveVisible(
      snapshot.aggregate.memories.filter((memory) => canViewMemory(memory, adultId)),
      rawControlId,
      (memory) => privateControlId("memory", memory.memoryId),
      (memory) => isMemoryActive(memory, asOf),
    );
  }

  public resolveSharingRule(
    snapshot: HouseholdApplicationSnapshot,
    adultId: string,
    rawControlId: string,
  ): PrivateControlResolution<PolicyRecord> {
    if (!isVerifiedAdult(snapshot.aggregate, adultId)) return { status: "unknown" };
    return resolveVisible(
      snapshot.aggregate.policies.filter(
        (policy) => policy.rule.kind === "sharing" && policy.rule.from.adultId === adultId,
      ),
      rawControlId,
      (policy) => privateControlId("sharing_rule", policy.policyId),
      (policy) => policy.status === "active",
    );
  }

  public explainSharingChoice(
    snapshot: HouseholdApplicationSnapshot,
    adultId: string,
    rawControlId: string,
  ): PrivateControlResolution<PrivateSharingChoiceItem> {
    if (!isVerifiedAdult(snapshot.aggregate, adultId)) return { status: "unknown" };
    const episodes = snapshot.aggregate.episodes.filter((episode) =>
      isOwnerPrivatePromotion(episode, adultId),
    );
    return resolveVisible(
      episodes,
      rawControlId,
      (episode) => privateControlId("sharing_choice", episode.episodeId),
      () => true,
      (episode) => sharingChoice(episode, snapshot.aggregate.policies),
    );
  }
}

function isVerifiedAdult(aggregate: HouseholdAggregate, adultId: string): boolean {
  return aggregate.verifiedAdultIds.some((candidate) => candidate === adultId);
}

function canViewMemory(memory: DurableMemory, adultId: string): boolean {
  return memory.scope.kind === "household" || memory.scope.adultId === adultId;
}

function isMemoryActive(memory: DurableMemory, asOf: string): boolean {
  return (
    memory.status === "active" &&
    (memory.expiresAt === undefined || Date.parse(memory.expiresAt) > Date.parse(asOf))
  );
}

function knowledgeFromMemory(memory: DurableMemory): PrivateKnowledgeItem {
  return {
    controlId: privateControlId("memory", memory.memoryId),
    kind: "memory",
    scope: memory.scope.kind,
    statement: memory.statement,
    sourceLabel: sourceLabels(memory.evidence.map((evidence) => evidence.source)),
    asOf: memory.confirmedAt,
  };
}

function knowledgeFromProfile(fact: SharedProfileFact): PrivateKnowledgeItem {
  return {
    controlId: privateControlId("profile", fact.factKey),
    kind: "profile",
    scope: "household",
    statement: `${fact.subject}: ${fact.detail}`,
    sourceLabel: "Shared family profile",
    asOf: fact.recordedAt,
  };
}

function knowledgeFromRoutine(
  anchor: RoutineAnchor,
  facts: readonly SharedProfileFact[],
): PrivateKnowledgeItem {
  const fact = facts.find(
    (candidate) => candidate.category === "routine_anchor" && candidate.anchorId === anchor.anchorId,
  );
  const days = anchor.daysOfWeek.join(",");
  return {
    controlId: privateControlId("routine", anchor.anchorId),
    kind: "routine",
    scope: "household",
    statement:
      fact === undefined
        ? `${anchor.label}: ${anchor.localTime} (${anchor.timeZone}; ISO weekdays ${days})`
        : `${fact.subject}: ${fact.detail} (${anchor.localTime}, ${anchor.timeZone}; ISO weekdays ${days})`,
    sourceLabel: "Confirmed household routine",
    asOf: fact?.recordedAt ?? null,
  };
}

function isOwnedActiveSharingPolicy(policy: PolicyRecord, adultId: string): boolean {
  return policy.status === "active" && policy.rule.kind === "sharing" && policy.rule.from.adultId === adultId;
}

function sharingRule(policy: PolicyRecord): PrivateSharingRuleItem {
  if (policy.rule.kind !== "sharing") throw new Error("Expected a sharing policy");
  return {
    controlId: privateControlId("sharing_rule", policy.policyId),
    policyId: policy.policyId,
    policyVersion: policy.version,
    sourceLabel: policy.rule.sourceMatcher.source === "gmail" ? "Gmail" : "Calendar",
    sourceClass: policy.rule.sourceClass,
    maximumSensitivity: policy.rule.maximumSensitivity,
    asOf: policy.approvedAt,
  };
}

function isOwnerPrivatePromotion(episode: FamilyEpisode, adultId: string): boolean {
  return (
    episode.scope.kind === "household" &&
    episode.promotionAuthority !== undefined &&
    episode.evidence.some(
      (evidence) => evidence.scope.kind === "personal" && evidence.scope.adultId === adultId,
    )
  );
}

function sharingChoice(episode: FamilyEpisode, policies: readonly PolicyRecord[]): PrivateSharingChoiceItem {
  const authority = episode.promotionAuthority;
  if (authority === undefined) throw new Error("Expected promotion authority");
  const authorityLabel =
    authority.kind === "approval"
      ? "You approved this one-time minimum household meaning."
      : policyAuthorityLabel(authority.policyId, authority.policyVersion, policies);
  return {
    controlId: privateControlId("sharing_choice", episode.episodeId),
    episodeId: episode.episodeId,
    summary: episode.title,
    sourceLabel: sourceLabels(
      episode.evidence
        .filter((evidence) => evidence.scope.kind === "personal")
        .map((evidence) => evidence.source),
    ),
    authorityLabel,
    asOf: episode.createdAt,
  };
}

function policyAuthorityLabel(
  policyId: string,
  policyVersion: number,
  policies: readonly PolicyRecord[],
): string {
  const policy = policies.find(
    (candidate) => candidate.policyId === policyId && candidate.version === policyVersion,
  );
  const controlId = privateControlId("sharing_rule", policyId);
  return policy?.rule.kind === "sharing"
    ? `Your ${controlId} rule authorized this minimum household meaning at version ${policyVersion}.`
    : `Your ${controlId} rule authorized this minimum household meaning at version ${policyVersion}; that historical rule is no longer active.`;
}

function sourceLabels(sources: readonly string[]): string {
  const labels = [...new Set(sources.map(sourceLabel))].sort();
  return labels.length === 0 ? "Authoritative household record" : labels.join(" + ");
}

function sourceLabel(source: string): string {
  switch (source) {
    case "gmail":
      return "Gmail";
    case "calendar":
      return "Calendar";
    case "linq":
      return "iMessage";
    case "system":
      return "Florence";
    default:
      return "Household source";
  }
}

function normalizeControlId(raw: string): string {
  return raw.normalize("NFKC").trim().toUpperCase();
}

function resolveVisible<T, R = T>(
  records: readonly T[],
  rawControlId: string,
  idFor: (record: T) => string,
  isActive: (record: T) => boolean,
  map: (record: T) => R = (record) => record as unknown as R,
): PrivateControlResolution<R> {
  const controlId = normalizeControlId(rawControlId);
  const matches = records.filter((record) => idFor(record) === controlId);
  if (matches.length === 0) return { status: "unknown" };
  if (matches.length > 1) return { status: "ambiguous" };
  const match = matches[0] as T;
  return isActive(match) ? { status: "active", value: map(match) } : { status: "inactive" };
}

function ensureUniqueControlIds<T extends { readonly controlId: string }>(items: readonly T[]): T[] {
  const ids = new Set<string>();
  for (const item of items) {
    if (ids.has(item.controlId)) {
      throw new Error("Private control ID collision");
    }
    ids.add(item.controlId);
  }
  return [...items];
}

function compareKnowledge(left: PrivateKnowledgeItem, right: PrivateKnowledgeItem): number {
  const kindOrder = { memory: 0, profile: 1, routine: 2 } as const;
  return (
    kindOrder[left.kind] - kindOrder[right.kind] ||
    compareOptionalInstants(right.asOf, left.asOf) ||
    left.controlId.localeCompare(right.controlId)
  );
}

function compareOptionalInstants(left: string | null, right: string | null): number {
  if (left === null) return right === null ? 0 : -1;
  if (right === null) return 1;
  return left.localeCompare(right);
}
