import { createHash } from "node:crypto";
import { Temporal } from "@js-temporal/polyfill";
import {
  type AcceptanceReceipt,
  type AcceptanceResult,
  AdultIdSchema,
  ApprovalIdSchema,
  CalendarEventCreateActionSchema,
  calendarEventCreateActionDigest,
  type DomainChange,
  WorkerJobIdSchema as DomainWorkerJobIdSchema,
  type DurableScope,
  EpisodeIdSchema,
  EpisodeProposalSchema,
  EvidenceRefSchema,
  ExternalActionIdSchema,
  type HouseholdAggregate,
  HouseholdAggregateSchema,
  HouseholdChiefOfStaff,
  type HouseholdSignal,
  HouseholdSignalSchema,
  InstantStringSchema,
  type OutboxIntent,
  PolicyIdSchema,
  type PrivateSourceMatcher,
  PrivateSourceMatcherSchema,
  RoutineAnchorIdSchema,
  RoutineAnchorSchema,
  SemanticTimePlanSchema,
  WorkerProposalSchema,
} from "../domain/index.js";
import {
  asWorkerRuntimeError,
  verifyWorkerResultCompletion,
  type WorkerJob,
  WorkerJobSchema,
  type WorkerResult,
  WorkerResultSchema,
  type WorkerToolReceipt,
} from "../runtime/index.js";
import { ActiveWorkerAttempts } from "./active-worker-attempts.js";
import {
  CONSENT_DISCLOSURE_VERSION,
  FOUNDING_ADULT_CONSENT_DISCLOSURE,
  INVITED_ADULT_CONSENT_DISCLOSURE,
} from "./consent-disclosures.js";
import {
  type ApplicationAuditEntry,
  ApplicationAuditEntrySchema,
  type ApplicationInput,
  ApplicationInputSchema,
  type ApplicationOutboxIntent,
  ApplicationOutboxIntentSchema,
  ApplicationOutcomeSchema,
  type ApplicationProjection,
  ApplicationProjectionSchema,
  type ApplicationResult,
  ApplicationResultSchema,
  type CalendarEventDeletedInboxItem,
  type CalendarEventInboxItem,
  CalendarTriageResultSchema,
  type ConversationClassification,
  ConversationClassificationSchema,
  type ConversationInboxItem,
  type ConversationResponseContext,
  EffectExecutionReceiptSchema,
  type GmailInboxItem,
  type GmailMessageDeletedInboxItem,
  GmailTriageResultSchema,
  type HouseholdApplicationSnapshot,
  HouseholdApplicationSnapshotSchema,
  OutboxExecutionResultSchema,
  type PendingPromotion,
  type PrivateReviewItem,
  PrivateReviewItemSchema,
  type ProjectDeliveryGuard,
  projectArtifactContentDigest,
  SharedProfileFactSchema,
  type WorkerCommand,
  WorkerCommandSchema,
  type WorkerPurpose,
  type WorkerRoutes,
  WorkerRoutesSchema,
} from "./contracts.js";
import type { ApplicationCommit, FlorenceApplication, FlorenceApplicationDependencies } from "./ports.js";
import { workerContextFingerprint } from "./worker-context-fingerprint.js";

export const DEFAULT_WORKER_ROUTES: WorkerRoutes = WorkerRoutesSchema.parse({
  family_research: {
    modelRouteId: "route.family_research.v1",
    outputContractRef: "contract.family_research.v1",
    capabilityIds: ["capability.research.read"],
    allowedToolNames: ["research_sources"],
    maxDurationMs: 600_000,
    maxModelCalls: 24,
    maxToolCalls: 80,
    modelCapabilityProfile: "long_context_research",
  },
  meal_plan: {
    modelRouteId: "route.meal_plan.v1",
    outputContractRef: "contract.meal_plan.v1",
    capabilityIds: ["capability.household_schedule.read"],
    allowedToolNames: ["household_schedule"],
    maxDurationMs: 300_000,
    maxModelCalls: 16,
    maxToolCalls: 40,
    modelCapabilityProfile: "tool_planning",
  },
  family_project: {
    modelRouteId: "route.meal_plan.v1",
    outputContractRef: "contract.family_project.v1",
    capabilityIds: ["capability.research.read"],
    allowedToolNames: ["research_sources"],
    maxDurationMs: 600_000,
    maxModelCalls: 24,
    maxToolCalls: 80,
    modelCapabilityProfile: "tool_planning",
  },
});

export class HouseholdApplicationNotFoundError extends Error {
  override readonly name = "HouseholdApplicationNotFoundError";
}

export class ApplicationRepositoryConflictError extends Error {
  override readonly name = "ApplicationRepositoryConflictError";

  constructor(
    readonly householdId: string,
    readonly expectedRevision: number,
    readonly actualRevision: number,
  ) {
    super(`Household application projection changed (expected ${expectedRevision}, found ${actualRevision})`);
  }
}

interface Work {
  readonly input: Exclude<ApplicationInput, { kind: "run_worker" }>;
  readonly initial: HouseholdApplicationSnapshot;
  aggregate: HouseholdAggregate;
  projection: ApplicationProjection;
  readonly signals: HouseholdSignal[];
  readonly changes: DomainChange[];
  readonly outbox: ApplicationOutboxIntent[];
  readonly audit: ApplicationAuditEntry[];
  readonly privateReviewItems: PrivateReviewItem[];
  readonly receipts: AcceptanceReceipt[];
}

function stableId(prefix: string, ...parts: readonly string[]): string {
  const digest = createHash("sha256").update(parts.join("\u0000")).digest("hex");
  return `${prefix}_${digest}`;
}

function contentDigest(...parts: readonly string[]): string {
  return `sha256:${createHash("sha256").update(parts.join("\u0000")).digest("hex")}`;
}

function privateAccountDigest(accountRef: string): string {
  return contentDigest("florence.private-source-account.v1", accountRef);
}

function normalizedSenderIdentity(sender: string | undefined): string | undefined {
  const normalized = sender?.normalize("NFKC").trim().toLocaleLowerCase("en-US").replace(/\s+/gu, " ");
  if (normalized === undefined || normalized.length === 0) return undefined;
  const email = normalized.match(
    /[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+/u,
  )?.[0];
  return email === undefined ? `header:${normalized}` : `email:${email}`;
}

function senderDisplayName(sender: string | undefined): string | undefined {
  const displayName = sender
    ?.normalize("NFKC")
    .replace(/<[^>]*>/gu, "")
    .trim()
    .replace(/^["']|["']$/gu, "")
    .trim();
  return displayName === undefined || displayName.length === 0 ? undefined : displayName;
}

function gmailSourceMatcher(item: GmailInboxItem): PrivateSourceMatcher | undefined {
  const senderIdentity = normalizedSenderIdentity(item.sender);
  if (senderIdentity === undefined) return undefined;
  return PrivateSourceMatcherSchema.parse({
    source: "gmail",
    accountRefDigest: privateAccountDigest(item.accountRef),
    senderIdentityDigest: contentDigest("florence.private-source-sender.v1", senderIdentity),
  });
}

function calendarSourceMatcher(item: CalendarEventInboxItem): PrivateSourceMatcher {
  return PrivateSourceMatcherSchema.parse({
    source: "calendar",
    accountRefDigest: privateAccountDigest(item.accountRef),
  });
}

function sourceMatchersEqual(left: PrivateSourceMatcher, right: PrivateSourceMatcher): boolean {
  if (left.source !== right.source || left.accountRefDigest !== right.accountRefDigest) return false;
  return left.source === "calendar"
    ? true
    : right.source === "gmail" && left.senderIdentityDigest === right.senderIdentityDigest;
}

function normalizedLeakText(value: string): string {
  return value
    .normalize("NFKC")
    .toLocaleLowerCase("en-US")
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .replace(/\s+/gu, " ");
}

const MINIMUM_MEANING_FORBIDDEN_PATTERNS = [
  /\b(?:https?:\/\/|www\.)\S+/iu,
  /\b(?:[a-z0-9-]+\.)+[a-z]{2,63}(?:\/\S*)?\b/iu,
  /\b[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9-]+(?:\.[a-z0-9-]+)+\b/iu,
  /(?:\+?\d[\d .()-]{7,}\d)/u,
  /\b(?:access|auth(?:entication)?|confirmation|login|one[ -]?time|pass(?:code|word)?|pin|security|verification)\s+(?:code|number|token|key)?\s*[:#-]?\s*\d{3,}\b/iu,
  /\b(?:access|auth(?:entication)?|confirmation|login|one[ -]?time|pass(?:code|word)?|security|verification)\s+(?:code|token|key)\s*[:#-]?\s*[a-z0-9-]{4,}\b/iu,
  /\b(?:otp|2fa|mfa)\s*[:#-]?\s*\d{3,}\b/iu,
  /(?:[$£€¥₹]\s*\d|\b\d[\d,.]*\s*(?:dollars?|usd|eur|gbp|yen|rupees?)\b)/iu,
  /\b(?:amount|balance|bill|cost|fee|invoice|payment|price|total)\s*(?:due|is|of|:)?\s*[$£€¥₹]?\s*\d/iu,
  /\b(?:affair|attorney|breakup|cancer|clinic|court|custody|diagnos(?:is|ed)|divorce|doctor|employer|employment|hospital|job offer|lawsuit|lawyer|layoff|laid off|legal|medical|medication|performance review|prescription|relationship counseling|salary|subpoena|surgery|terminated|termination|therapist|therapy)\b/iu,
  /\b(?:act as|developer message|disregard (?:all |any )?(?:previous|prior)|ignore (?:all |any )?(?:previous|prior)|jailbreak|override (?:the |your )?(?:rules|instructions)|reveal (?:the |your )?(?:prompt|instructions)|system prompt|tool call)\b/iu,
  /["“”'][^"“”'\n]{8,}["“”']/u,
] as const;

function hasMeaningfulPrivateOverlap(
  output: string,
  privateValues: readonly (string | null | undefined)[],
): boolean {
  const normalizedOutput = normalizedLeakText(output);
  if (normalizedOutput.length === 0) return false;
  for (const value of privateValues) {
    if (value === null || value === undefined) continue;
    const privateText = normalizedLeakText(value);
    if (privateText.length < 8) continue;
    if (normalizedOutput.includes(privateText)) return true;
    const tokens = privateText.split(" ").filter((token) => token.length > 1);
    for (let index = 0; index <= tokens.length - 3; index += 1) {
      const phrase = tokens.slice(index, index + 3).join(" ");
      if (phrase.length >= 12 && normalizedOutput.includes(phrase)) return true;
    }
  }
  return false;
}

function minimumMeaningPassesLeakGuard(
  outputValues: readonly string[],
  privateValues: readonly (string | null | undefined)[],
): boolean {
  const output = outputValues.join("\n");
  if (MINIMUM_MEANING_FORBIDDEN_PATTERNS.some((pattern) => pattern.test(output))) return false;
  const numericGroups = output.match(/\d+/gu) ?? [];
  if (
    numericGroups.some((group) => group.length >= 4) ||
    numericGroups.length >= 4 ||
    numericGroups.reduce((total, group) => total + group.length, 0) >= 8
  ) {
    return false;
  }
  return !hasMeaningfulPrivateOverlap(output, privateValues);
}

function plusMilliseconds(instant: string, milliseconds: number): string {
  return Temporal.Instant.from(instant).add({ milliseconds }).toString();
}

function unique<T>(values: readonly T[]): T[] {
  return [...new Set(values)];
}

function targetScope(item: ConversationInboxItem): DurableScope {
  if (item.channel.scope === "household") {
    return { kind: "household" };
  }
  return {
    kind: "personal",
    adultId: AdultIdSchema.parse(item.channel.adultId),
  };
}

function scopeCanSeeMemory(scope: DurableScope, memoryScope: DurableScope): boolean {
  if (scope.kind === "household") return memoryScope.kind === "household";
  return memoryScope.kind === "household" || memoryScope.adultId === scope.adultId;
}

function activeMemoryContext(aggregate: HouseholdAggregate, scope: DurableScope, asOf: string) {
  return aggregate.memories.flatMap((memory) => {
    if (
      memory.status !== "active" ||
      !scopeCanSeeMemory(scope, memory.scope) ||
      (memory.expiresAt !== undefined && Date.parse(memory.expiresAt) <= Date.parse(asOf))
    ) {
      return [];
    }
    return [
      {
        memoryId: memory.memoryId,
        kind: memory.kind,
        statement: memory.statement,
        scope: memory.scope.kind,
        confirmedAt: memory.confirmedAt,
      },
    ];
  });
}

function pendingMemoryContext(aggregate: HouseholdAggregate, scope: DurableScope) {
  return aggregate.memoryCandidates.flatMap((candidate) =>
    scopeCanSeeMemory(scope, candidate.scope)
      ? [
          {
            candidateId: candidate.candidateId,
            kind: candidate.kind,
            statement: candidate.statement,
            scope: candidate.scope.kind,
          },
        ]
      : [],
  );
}

function conversationEvidence(item: ConversationInboxItem) {
  return EvidenceRefSchema.parse({
    evidenceId: stableId("evidence", item.householdId, item.messageRef),
    source: "linq",
    sourceRef: item.messageRef,
    scope: targetScope(item),
    observedAt: item.occurredAt,
    revision: 1,
    contentDigest: contentDigest(
      item.text,
      ...item.attachmentRefs,
      ...item.attachmentContents.map((attachment) => attachment.contentDigest),
    ),
  });
}

function gmailSourceKey(item: GmailInboxItem | GmailMessageDeletedInboxItem): string {
  return stableId("gmail_source", item.householdId, item.ownerAdultId, item.accountRef, item.messageRef);
}

function gmailEvidence(item: GmailInboxItem | GmailMessageDeletedInboxItem) {
  const sourceRef = gmailSourceKey(item);
  return EvidenceRefSchema.parse({
    evidenceId: stableId("evidence", sourceRef, String(item.revision)),
    source: "gmail",
    sourceRef,
    scope: { kind: "personal", adultId: item.ownerAdultId },
    observedAt: item.occurredAt,
    revision: item.revision,
    ...(item.kind === "gmail_message_deleted"
      ? {}
      : {
          contentDigest: contentDigest(
            item.labels.some((label) => label === "SPAM" || label === "TRASH")
              ? "excluded:spam_or_trash"
              : "eligible",
            item.sender ?? "",
            item.subject ?? "",
            item.snippet ?? "",
            item.bodyText ?? "",
            ...item.attachmentRefs,
            ...item.attachmentContents.map((attachment) => attachment.contentDigest),
          ),
        }),
  });
}

function calendarSourceKey(item: CalendarEventInboxItem | CalendarEventDeletedInboxItem): string {
  return stableId(
    "calendar_source",
    item.householdId,
    item.ownerAdultId,
    item.accountRef,
    item.eventRef,
    item.providerRef,
  );
}

function calendarEvidence(item: CalendarEventInboxItem | CalendarEventDeletedInboxItem) {
  const sourceRef = calendarSourceKey(item);
  return EvidenceRefSchema.parse({
    evidenceId: stableId("evidence", sourceRef, String(item.revision)),
    source: "calendar",
    sourceRef,
    scope: { kind: "personal", adultId: item.ownerAdultId },
    observedAt: item.occurredAt,
    revision: item.revision,
    ...(item.kind === "calendar_event" ? { contentDigest: item.contentDigest } : {}),
  });
}

function appOutboxBase(
  input: {
    householdId: string;
    idempotencyKey: string;
  },
  suffix: string,
) {
  const intentId = stableId("app_outbox", input.householdId, input.idempotencyKey, suffix);
  return {
    intentId,
    householdId: input.householdId,
    idempotencyKey: `florence:${intentId}`,
  };
}

function queueMessage(
  work: Work,
  suffix: string,
  scope: DurableScope,
  messageClass:
    | "onboarding"
    | "private_review"
    | "private_interrupt"
    | "promotion_request"
    | "clarifying_question"
    | "status"
    | "daily_brief",
  body: string,
  responseContext?: ConversationResponseContext,
  deliveryGuard?: ProjectDeliveryGuard,
): void {
  work.outbox.push(
    ApplicationOutboxIntentSchema.parse({
      ...appOutboxBase(work.input, suffix),
      kind: "conversation.send",
      targetScope: scope,
      messageClass,
      ...(responseContext === undefined ? {} : { responseContext }),
      ...(deliveryGuard === undefined ? {} : { deliveryGuard }),
      body,
    }),
  );
}

function queuePrivateReviewItem(
  work: Work,
  input: {
    source: PrivateReviewItem["source"];
    sourceKey: string;
    revision: number;
    adultId: string;
    summary: string;
    observedAt: string;
  },
): void {
  work.privateReviewItems.push(
    PrivateReviewItemSchema.parse({
      itemKey: stableId("private_review", input.source, input.sourceKey, String(input.revision)),
      adultId: input.adultId,
      source: input.source,
      summary: input.summary,
      observedAt: input.observedAt,
    }),
  );
}

function wrapDomainEffect(effect: OutboxIntent): ApplicationOutboxIntent {
  return ApplicationOutboxIntentSchema.parse({
    intentId: stableId("app_domain", effect.intentId),
    householdId: effect.householdId,
    idempotencyKey: effect.idempotencyKey,
    kind: "domain.effect",
    effect,
  });
}

function createWork(
  input: Exclude<ApplicationInput, { kind: "run_worker" }>,
  snapshot: HouseholdApplicationSnapshot,
): Work {
  return {
    input,
    initial: snapshot,
    aggregate: snapshot.aggregate,
    projection: ApplicationProjectionSchema.parse(snapshot.projection),
    signals: [],
    changes: [],
    outbox: [],
    audit: [],
    privateReviewItems: [],
    receipts: [],
  };
}

function acceptDomain(
  work: Work,
  suffix: string,
  occurredAt: string,
  actor: HouseholdSignal["actor"],
  payload: Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
): AcceptanceResult {
  const signal = HouseholdSignalSchema.parse({
    ...payload,
    householdId: work.aggregate.householdId,
    signalId: stableId("signal", work.input.idempotencyKey, suffix),
    sequence: work.aggregate.lastProcessedSequence + 1,
    occurredAt,
    actor,
  });
  const result = HouseholdChiefOfStaff.accept({ current: work.aggregate, signal });
  work.aggregate = result.aggregate;
  work.signals.push(signal);
  work.changes.push(...result.changes);
  work.outbox.push(...result.effects.map(wrapDomainEffect));
  work.receipts.push(result.receipt);
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "domain_accepted",
      occurredAt,
      decision: `${signal.kind}:${result.receipt.disposition}`,
      containsPrivateData: false,
    }),
  );
  return result;
}

function workerObjective(
  purpose: WorkerPurpose,
  title: string,
  requiredOutcome: string,
  details: readonly string[],
): string {
  const label =
    purpose === "meal_plan"
      ? "Prepare the requested household meal plan"
      : purpose === "family_project"
        ? "Build a decision-ready plan for the requested family project"
        : "Research";
  const fixed = [
    `${label}: ${title}.`,
    `Completion contract: ${requiredOutcome}.`,
    "Return only app-owned proposed commands. Do not send messages or perform external actions.",
  ].join(" ");
  if (details.length === 0) return fixed;
  const maximumLength = 20_000;
  let objective = `${fixed} Context and constraints:`;
  for (const detail of details) {
    const addition = ` ${detail.trim()};`;
    const remaining = maximumLength - objective.length;
    if (remaining <= 1) break;
    if (addition.length <= remaining) {
      objective += addition;
      continue;
    }
    objective += `${addition.slice(0, Math.max(0, remaining - 1))}…`;
    break;
  }
  return objective;
}

function capabilityGrantsForAttempt(input: {
  jobId: string;
  attemptId: string;
  householdId: string;
  scopeGrantId: string;
  scope: DurableScope;
  purpose: WorkerPurpose;
  capabilities: readonly string[];
  issuedAt: string;
  expiresAt: string;
}) {
  return input.capabilities.map((capability) => ({
    grantId: stableId("capability_grant", input.jobId, input.attemptId, capability),
    capability,
    householdId: input.householdId,
    jobId: input.jobId,
    attemptId: input.attemptId,
    scopeGrantId: input.scopeGrantId,
    scope: {
      visibility: input.scope.kind,
      ...(input.scope.kind === "personal" ? { adultId: input.scope.adultId } : {}),
    },
    purpose: input.purpose,
    issuedAt: input.issuedAt,
    expiresAt: input.expiresAt,
  }));
}

function buildWorkerJob(input: {
  jobId?: string;
  attemptNumber?: number;
  purpose: WorkerPurpose;
  work: Work;
  episodeId: string;
  evidenceId: string;
  title: string;
  requiredOutcome: string;
  details: readonly string[];
  scope: DurableScope;
  routes: WorkerRoutes;
  occurredAt: string;
}): WorkerJob {
  const route = input.routes[input.purpose];
  const jobId = input.jobId ?? stableId("job", input.work.input.idempotencyKey, input.purpose);
  const attemptNumber = input.attemptNumber ?? 1;
  const attemptId = `${jobId}.attempt.${attemptNumber}`;
  const householdId = input.work.aggregate.householdId;
  const scopeGrantId = stableId("context_grant", jobId, attemptId);
  const deadline = plusMilliseconds(input.occurredAt, 15 * 60_000);
  return WorkerJobSchema.parse({
    jobId,
    attemptId,
    householdId,
    baseHouseholdVersion: input.work.aggregate.version,
    policyVersion: input.work.aggregate.policyVersion,
    objective: workerObjective(input.purpose, input.title, input.requiredOutcome, input.details),
    scopeGrant: {
      grantId: scopeGrantId,
      visibility: input.scope.kind,
      ...(input.scope.kind === "personal" ? { adultId: input.scope.adultId } : {}),
      purpose: input.purpose,
      expiresAt: plusMilliseconds(input.occurredAt, 20 * 60_000),
    },
    evidenceRefs: [input.evidenceId],
    capabilityGrants: capabilityGrantsForAttempt({
      jobId,
      attemptId,
      householdId,
      scopeGrantId,
      scope: input.scope,
      purpose: input.purpose,
      capabilities: route.capabilityIds,
      issuedAt: input.occurredAt,
      expiresAt: deadline,
    }),
    modelRouteId: route.modelRouteId,
    modelCapabilityProfile: route.modelCapabilityProfile,
    budget: {
      maxDurationMs: route.maxDurationMs,
      maxModelCalls: route.maxModelCalls,
      maxToolCalls: route.maxToolCalls,
    },
    deadline,
    outputContractRef: route.outputContractRef,
    allowedToolNames: route.allowedToolNames,
  });
}

function enqueueWorker(
  work: Work,
  purpose: WorkerPurpose,
  episodeId: string,
  job: WorkerJob,
  createdAt: string,
): void {
  const episode = work.aggregate.episodes.find((candidate) => candidate.episodeId === episodeId);
  if (episode === undefined) {
    throw new Error(`Cannot delegate missing family episode: ${episodeId}`);
  }
  work.projection.workers.push({
    purpose,
    episodeId: EpisodeIdSchema.parse(episodeId),
    baseEpisodeVersion: episode.version,
    contextFingerprint: workerContextFingerprint({
      aggregate: work.aggregate,
      projection: work.projection,
      episodeId,
      purpose,
      scope: episode.scope,
      evidenceRefs: job.evidenceRefs,
      asOf: createdAt,
    }),
    job,
    status: "queued",
    attemptNumber: 1,
    automaticRetryCount: 0,
    deliveryGeneration: 1,
    projectBrief: job.objective,
    outstandingQuestions: [],
    createdAt,
    updatedAt: createdAt,
  });
  work.outbox.push(
    ApplicationOutboxIntentSchema.parse({
      ...appOutboxBase(work.input, `run:${job.attemptId}`),
      kind: "worker.run",
      job,
    }),
  );
}

function auditClassification(
  work: Work,
  kind:
    | "conversation_classified"
    | "gmail_triaged"
    | "gmail_reconciled"
    | "calendar_triaged"
    | "calendar_reconciled",
  decision: string,
  sourceRef: string,
  adultId: string,
  privateData: boolean,
  occurredAt: string,
): void {
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind,
      occurredAt,
      decision,
      sourceRef,
      adultId,
      containsPrivateData: privateData,
    }),
  );
}

function onboardingParticipants(
  projection: ApplicationProjection["onboarding"],
): Array<ApplicationProjection["onboarding"]["initiatorAdultId"]> {
  return unique(
    [projection.initiatorAdultId, projection.invitedAdultId].filter(
      (value): value is ApplicationProjection["onboarding"]["initiatorAdultId"] => value !== undefined,
    ),
  );
}

function personal(adultId: string): DurableScope {
  return { kind: "personal", adultId: AdultIdSchema.parse(adultId) };
}

function invalidOnboarding(work: Work, item: ConversationInboxItem, body: string): boolean {
  queueMessage(work, "onboarding-invalid", targetScope(item), "onboarding", body);
  return false;
}

function onboardingRecoveryBody(onboarding: ApplicationProjection["onboarding"]): string {
  switch (onboarding.phase) {
    case "awaiting_initiator_consent":
      return FOUNDING_ADULT_CONSENT_DISCLOSURE;
    case "awaiting_invitation":
      return "In the initiating adult’s private DM, reply “invite +1…” with the second adult’s full phone number, or “invite name@example.com” with their iMessage email.";
    case "awaiting_invitee_consent":
      return `${INVITED_ADULT_CONSENT_DISCLOSURE} If that identity already uses Florence, send exactly “join my pending Florence household” in the existing private Florence DM instead.`;
    case "awaiting_group":
      return "Create one iMessage group with both adults and Florence, then send “connect this family” in that group.";
    case "naming_adults":
      return "In the household group, each adult should send “I’m [name]” or “Call me [name].”";
    case "building_profile":
      return "In the household group, add any useful family coordination details. If there are none, reply “None”; otherwise review the summary and reply “I confirm the profile.” Both adults must confirm.";
    case "connecting_sources":
      return "To finish setup, each adult who has not connected yet should DM Florence “connect my Google account.” Google links and account details stay private to that adult.";
    case "active":
      return "Florence is ready.";
  }
}

function presentRequiredConsentDisclosure(work: Work, item: ConversationInboxItem): boolean {
  if (item.channel.scope !== "personal") return false;
  const onboarding = work.projection.onboarding;
  const audience =
    onboarding.phase === "awaiting_initiator_consent" && item.senderAdultId === onboarding.initiatorAdultId
      ? ("initiator" as const)
      : onboarding.phase === "awaiting_invitee_consent" && item.senderAdultId === onboarding.invitedAdultId
        ? ("invitee" as const)
        : undefined;
  if (audience === undefined || onboarding.privateDmAdultIds.includes(item.senderAdultId)) {
    return false;
  }
  const disclosure =
    audience === "initiator" ? FOUNDING_ADULT_CONSENT_DISCLOSURE : INVITED_ADULT_CONSENT_DISCLOSURE;

  const responseContext = item.replyTo?.responseContext;
  const boundDirectDisclosure =
    item.replyTo?.messageClass === "onboarding" &&
    responseContext?.kind === "consent_disclosure" &&
    responseContext.adultId === item.senderAdultId &&
    responseContext.audience === audience &&
    responseContext.consentDisclosureVersion === CONSENT_DISCLOSURE_VERSION;
  const sourceBoundTransferDisclosure =
    audience === "invitee" &&
    item.replyTo?.messageClass === "onboarding" &&
    responseContext?.kind === "invitation_transfer" &&
    responseContext.consentDisclosureVersion === CONSENT_DISCLOSURE_VERSION;
  const delivered = boundDirectDisclosure || sourceBoundTransferDisclosure;

  if (delivered) {
    work.projection.onboarding = ApplicationProjectionSchema.shape.onboarding.parse({
      ...onboarding,
      privateDmAdultIds: unique([...onboarding.privateDmAdultIds, item.senderAdultId]),
    });
  } else {
    queueMessage(
      work,
      "onboarding-consent-disclosure",
      personal(item.senderAdultId),
      "onboarding",
      disclosure,
      {
        kind: "consent_disclosure",
        adultId: item.senderAdultId,
        audience,
        consentDisclosureVersion: CONSENT_DISCLOSURE_VERSION,
      },
    );
  }
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "onboarding_transition",
      occurredAt: item.occurredAt,
      decision: `${onboarding.phase}:${onboarding.phase}:consent_disclosure_${
        delivered ? "delivered" : "queued"
      }${sourceBoundTransferDisclosure ? ":source_bound_transfer" : ""}`,
      sourceRef: item.messageRef,
      adultId: item.senderAdultId,
      containsPrivateData: true,
    }),
  );
  return !delivered;
}

function applyOnboarding(
  work: Work,
  item: ConversationInboxItem,
  classification: Extract<ConversationClassification, { intent: "onboarding" }>,
): boolean {
  const onboarding = work.projection.onboarding;
  let next = onboarding;
  switch (classification.action) {
    case "consent": {
      if (
        onboarding.phase !== "awaiting_initiator_consent" ||
        item.channel.scope !== "personal" ||
        item.senderAdultId !== onboarding.initiatorAdultId ||
        !onboarding.privateDmAdultIds.includes(item.senderAdultId)
      ) {
        return invalidOnboarding(work, item, "Florence still needs the initiating adult's consent.");
      }
      next = {
        ...onboarding,
        phase: "awaiting_invitation",
        consentedAdultIds: [onboarding.initiatorAdultId],
        privateDmAdultIds: [onboarding.initiatorAdultId],
      };
      queueMessage(
        work,
        "onboarding-consented",
        personal(item.senderAdultId),
        "onboarding",
        "Consent recorded. From this private conversation, reply “invite +1…” with the second adult’s full phone number, or “invite name@example.com” with their iMessage email.",
      );
      break;
    }
    case "invite_adult": {
      const invitedAdultId = classification.invitedAdultId;
      if (
        onboarding.phase !== "awaiting_invitation" ||
        item.channel.scope !== "personal" ||
        item.senderAdultId !== onboarding.initiatorAdultId ||
        invitedAdultId === undefined ||
        invitedAdultId === onboarding.initiatorAdultId
      ) {
        return invalidOnboarding(work, item, "That adult cannot be invited to this household.");
      }
      work.aggregate = HouseholdAggregateSchema.parse({
        ...work.aggregate,
        verifiedAdultIds: unique([...work.aggregate.verifiedAdultIds, invitedAdultId]),
      });
      next = { ...onboarding, phase: "awaiting_invitee_consent", invitedAdultId };
      queueMessage(
        work,
        "onboarding-inviter",
        personal(item.senderAdultId),
        "onboarding",
        "The invitation is ready. Ask the second adult to text Florence from that invited iMessage identity. Florence will send the full privacy and consent disclosure, then ask them to accept by replying to that exact message. If they already use Florence, ask them to send exactly “join my pending Florence household” in their existing private Florence DM. Florence will not contact them first.",
      );
      break;
    }
    case "accept_invite": {
      if (
        onboarding.phase !== "awaiting_invitee_consent" ||
        item.channel.scope !== "personal" ||
        item.senderAdultId !== onboarding.invitedAdultId ||
        !onboarding.privateDmAdultIds.includes(item.senderAdultId)
      ) {
        return invalidOnboarding(work, item, "The pending invite must be accepted by its invitee in a DM.");
      }
      next = {
        ...onboarding,
        phase: "awaiting_group",
        consentedAdultIds: unique([...onboarding.consentedAdultIds, item.senderAdultId]),
        privateDmAdultIds: unique([...onboarding.privateDmAdultIds, item.senderAdultId]),
      };
      for (const adultId of onboardingParticipants(next)) {
        queueMessage(
          work,
          `onboarding-group-${adultId}`,
          personal(adultId),
          "onboarding",
          "Both adults have consented. Create one iMessage group with both adults and Florence, then send “connect this family” in that group.",
        );
      }
      break;
    }
    case "register_group": {
      if (
        onboarding.phase !== "awaiting_group" ||
        item.channel.scope !== "household" ||
        !onboardingParticipants(onboarding).includes(item.senderAdultId) ||
        onboarding.consentedAdultIds.length !== 2
      ) {
        return invalidOnboarding(work, item, "The shared group can be registered after both adults consent.");
      }
      next = {
        ...onboarding,
        phase: "naming_adults",
        groupChannelId: item.channel.channelId,
      };
      queueMessage(
        work,
        "onboarding-names",
        { kind: "household" },
        "onboarding",
        "The household group is connected. Before we build the family profile, each adult should send one message here saying “I’m [name]” or “Call me [name].” I’ll use those names to understand ownership without guessing.",
      );
      break;
    }
    case "set_name": {
      if (
        !["naming_adults", "building_profile", "connecting_sources", "active"].includes(onboarding.phase) ||
        item.channel.scope !== "household" ||
        item.channel.channelId !== onboarding.groupChannelId ||
        !onboardingParticipants(onboarding).includes(item.senderAdultId) ||
        classification.displayName === undefined
      ) {
        return invalidOnboarding(
          work,
          item,
          "Each verified adult can set or correct only their own name in the household group.",
        );
      }
      const adultNames = [
        ...onboarding.adultNames.filter((adult) => adult.adultId !== item.senderAdultId),
        { adultId: item.senderAdultId, displayName: classification.displayName },
      ].sort((left, right) => left.adultId.localeCompare(right.adultId));
      const allNamed = onboardingParticipants(onboarding).every((adultId) =>
        adultNames.some((adult) => adult.adultId === adultId),
      );
      next = {
        ...onboarding,
        phase: onboarding.phase === "naming_adults" && allNamed ? "building_profile" : onboarding.phase,
        adultNames,
      };
      queueMessage(
        work,
        allNamed ? "onboarding-profile" : "onboarding-name-recorded",
        { kind: "household" },
        "onboarding",
        allNamed
          ? `Thanks—I'll use ${adultNames.map((adult) => adult.displayName).join(" and ")} for ownership. Let's make a light shared profile—not a meticulous tracker. In one or several messages, share only what helps coordination: children or other dependents and their schools or childcare, recurring activities, normal morning/pickup/bedtime anchors, and dietary constraints. If you have nothing to add, reply “None”; I’ll count that as this adult’s empty-profile confirmation, and the other adult must confirm too. Florence currently uses ${work.aggregate.timeZone}. I’ll summarize any details I record before both adults confirm them.`
          : `Thanks, ${classification.displayName}. I still need the other adult to share the name they want me to use.`,
      );
      break;
    }
    case "update_profile": {
      if (
        !["building_profile", "connecting_sources", "active"].includes(onboarding.phase) ||
        item.channel.scope !== "household" ||
        item.channel.channelId !== onboarding.groupChannelId ||
        !onboardingParticipants(onboarding).includes(item.senderAdultId) ||
        classification.profileFacts === undefined
      ) {
        return invalidOnboarding(
          work,
          item,
          "Shared profile details can be added by a verified adult in the household group.",
        );
      }
      const merge = mergeSharedProfileFacts(work, item, classification.profileFacts);
      if (merge === "invalid_anchor") {
        queueMessage(
          work,
          `onboarding-profile-invalid-anchor-${item.messageRef}`,
          { kind: "household" },
          "onboarding",
          "I couldn't match that routine correction to a confirmed routine. Please review the routine IDs in the profile summary and try again.",
        );
        break;
      }
      const changed = merge === "changed";
      next = changed ? { ...onboarding, profileConfirmedAdultIds: [] } : onboarding;
      queueMessage(
        work,
        `onboarding-profile-update-${item.messageRef}`,
        { kind: "household" },
        "onboarding",
        changed
          ? `${sharedProfileSummary(work)} Both adults can reply “I confirm the profile” after reviewing this summary.`
          : `Those details are already in the shared profile. ${sharedProfileSummary(work)}`,
      );
      break;
    }
    case "remove_profile": {
      if (
        !["building_profile", "connecting_sources", "active"].includes(onboarding.phase) ||
        item.channel.scope !== "household" ||
        item.channel.channelId !== onboarding.groupChannelId ||
        !onboardingParticipants(onboarding).includes(item.senderAdultId) ||
        classification.profileFactKeys === undefined
      ) {
        return invalidOnboarding(
          work,
          item,
          "Shared profile details can only be removed by a verified adult in the household group.",
        );
      }
      const removal = removeSharedProfileFacts(work, item, classification.profileFactKeys);
      if (removal === "unknown") {
        queueMessage(
          work,
          `onboarding-profile-remove-unknown-${item.messageRef}`,
          { kind: "household" },
          "onboarding",
          "I couldn’t match that request to the current shared profile, so I removed nothing.",
        );
        break;
      }
      if (removal === "blocked") {
        queueMessage(
          work,
          `onboarding-profile-remove-blocked-${item.messageRef}`,
          { kind: "household" },
          "onboarding",
          "That routine is still used by an open family reminder, so I left it in place. Update or close the affected commitment first.",
        );
        break;
      }
      next = { ...onboarding, profileConfirmedAdultIds: [] };
      queueMessage(
        work,
        `onboarding-profile-removed-${item.messageRef}`,
        { kind: "household" },
        "onboarding",
        "I removed that shared profile detail. Both adults can review the updated profile and confirm it again.",
      );
      break;
    }
    case "confirm_profile": {
      if (
        !["building_profile", "connecting_sources", "active"].includes(onboarding.phase) ||
        item.channel.scope !== "household" ||
        item.channel.channelId !== onboarding.groupChannelId ||
        !onboardingParticipants(onboarding).includes(item.senderAdultId)
      ) {
        return invalidOnboarding(
          work,
          item,
          "The shared profile can only be confirmed by a verified adult in the household group.",
        );
      }
      const confirmed = unique([...onboarding.profileConfirmedAdultIds, item.senderAdultId]);
      if (confirmed.length === 2) {
        const anchors = work.projection.sharedProfile.facts
          .flatMap((fact) =>
            fact.category === "routine_anchor"
              ? [
                  RoutineAnchorSchema.parse({
                    anchorId: fact.anchorId,
                    label: fact.subject,
                    timeZone: fact.timeZone,
                    localTime: fact.localTime,
                    daysOfWeek: fact.daysOfWeek,
                  }),
                ]
              : [],
          )
          .sort((left, right) => left.anchorId.localeCompare(right.anchorId));
        const current = [...work.aggregate.routineAnchors].sort((left, right) =>
          left.anchorId.localeCompare(right.anchorId),
        );
        if (JSON.stringify(anchors) !== JSON.stringify(current)) {
          const result = acceptDomain(
            work,
            "routine-anchors-confirmed",
            item.occurredAt,
            { kind: "adult", adultId: item.senderAdultId },
            {
              kind: "routine_anchors.replaced",
              anchors,
            } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
          );
          if (result.receipt.disposition !== "accepted") {
            queueMessage(
              work,
              `onboarding-routine-rejected-${item.messageRef}`,
              { kind: "household" },
              "onboarding",
              "I couldn't safely apply the confirmed routine changes because an existing reminder would become invalid. The previous routines and reminders are unchanged; please correct the routine timing and confirm again.",
            );
            break;
          }
        }
      }
      const sourcesReady = onboardingParticipants(onboarding).every((adultId) =>
        onboarding.googleConnectedAdultIds.includes(adultId),
      );
      const ready = confirmed.length === 2 && sourcesReady;
      next = {
        ...onboarding,
        phase:
          onboarding.phase === "active" || ready
            ? "active"
            : confirmed.length === 2
              ? "connecting_sources"
              : onboarding.phase === "connecting_sources"
                ? "connecting_sources"
                : "building_profile",
        profileConfirmedAdultIds: confirmed,
      };
      queueMessage(
        work,
        `onboarding-confirm-${item.senderAdultId}`,
        { kind: "household" },
        "onboarding",
        ready
          ? "Both adults confirmed the shared profile and connected Google. Florence is ready."
          : confirmed.length === 2
            ? "Both adults confirmed the shared profile. To finish setup, each adult should DM Florence “connect my Google account.” The private link and each account’s mail and calendar review stay in that adult’s DM."
            : "One adult confirmed the shared profile. The other adult can review it and reply “I confirm the profile,” or reply “None” if there are no details to add.",
      );
      if (confirmed.length === 2 && !sourcesReady) {
        for (const adultId of onboardingParticipants(onboarding)) {
          if (onboarding.googleConnectedAdultIds.includes(adultId)) continue;
          queueMessage(
            work,
            `onboarding-google-${adultId}`,
            personal(adultId),
            "onboarding",
            "To finish Florence setup, reply in this private DM: “connect my Google account.” You can connect more accounts later. Mail and calendar content remain private unless you explicitly share minimum family meaning or approve a narrow rule.",
          );
        }
      }
      break;
    }
  }
  work.projection.onboarding = ApplicationProjectionSchema.shape.onboarding.parse(next);
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "onboarding_transition",
      occurredAt: item.occurredAt,
      decision: `${onboarding.phase}:${next.phase}`,
      sourceRef: item.messageRef,
      adultId: item.senderAdultId,
      containsPrivateData: item.channel.scope === "personal",
    }),
  );
  return true;
}

function mergeSharedProfileFacts(
  work: Work,
  item: ConversationInboxItem,
  candidates: NonNullable<Extract<ConversationClassification, { intent: "onboarding" }>["profileFacts"]>,
): "changed" | "unchanged" | "invalid_anchor" {
  const facts = new Map(work.projection.sharedProfile.facts.map((fact) => [fact.factKey, fact] as const));
  const routineFactsByAnchorId = new Map(
    work.projection.sharedProfile.facts.flatMap((fact) =>
      fact.category === "routine_anchor" ? [[fact.anchorId, fact] as const] : [],
    ),
  );
  let changed = false;
  for (const candidate of candidates) {
    const normalizedSubject = candidate.subject.normalize("NFKC").trim().replace(/\s+/gu, " ").toLowerCase();
    const suppliedRoutine =
      candidate.category === "routine_anchor" && candidate.anchorId !== undefined
        ? routineFactsByAnchorId.get(candidate.anchorId)
        : undefined;
    if (
      candidate.category === "routine_anchor" &&
      candidate.anchorId !== undefined &&
      suppliedRoutine === undefined
    ) {
      return "invalid_anchor";
    }
    const anchorId =
      candidate.category === "routine_anchor"
        ? (candidate.anchorId ??
          RoutineAnchorIdSchema.parse(stableId("anchor", work.aggregate.householdId, normalizedSubject)))
        : undefined;
    const factKey =
      suppliedRoutine?.factKey ??
      `profile:${createHash("sha256")
        .update(
          candidate.category === "routine_anchor"
            ? `routine_anchor\u0000${anchorId}`
            : `${candidate.category}\u0000${normalizedSubject}`,
        )
        .digest("hex")
        .slice(0, 32)}`;
    const existing = facts.get(factKey);
    if (
      existing?.category === candidate.category &&
      existing.subject === candidate.subject &&
      existing.detail === candidate.detail &&
      (candidate.category !== "routine_anchor" ||
        (existing.category === "routine_anchor" &&
          existing.anchorId === anchorId &&
          existing.timeZone === candidate.timeZone &&
          existing.localTime === candidate.localTime &&
          JSON.stringify(existing.daysOfWeek) === JSON.stringify(candidate.daysOfWeek)))
    ) {
      continue;
    }
    const fact = SharedProfileFactSchema.parse({
      factKey,
      category: candidate.category,
      subject: candidate.subject,
      detail: candidate.detail,
      ...(candidate.category === "routine_anchor"
        ? {
            anchorId,
            timeZone: candidate.timeZone,
            localTime: candidate.localTime,
            daysOfWeek: candidate.daysOfWeek,
          }
        : {}),
      sourceRef: item.messageRef,
      recordedByAdultId: item.senderAdultId,
      recordedAt: item.occurredAt,
    });
    facts.set(factKey, fact);
    if (fact.category === "routine_anchor") {
      routineFactsByAnchorId.set(fact.anchorId, fact);
    }
    changed = true;
  }
  if (!changed) return "unchanged";
  work.projection.sharedProfile = ApplicationProjectionSchema.shape.sharedProfile.parse({
    facts: [...facts.values()].sort(
      (left, right) =>
        left.category.localeCompare(right.category) || left.subject.localeCompare(right.subject),
    ),
  });
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "onboarding_transition",
      occurredAt: item.occurredAt,
      decision: "shared_profile:updated",
      sourceRef: item.messageRef,
      adultId: item.senderAdultId,
      containsPrivateData: false,
    }),
  );
  return "changed";
}

function removeSharedProfileFacts(
  work: Work,
  item: ConversationInboxItem,
  factKeys: readonly string[],
): "changed" | "unknown" | "blocked" {
  const requested = new Set(factKeys);
  const removed = work.projection.sharedProfile.facts.filter((fact) => requested.has(fact.factKey));
  if (removed.length !== requested.size) return "unknown";
  const removedAnchorIds = new Set(
    removed.flatMap((fact) => (fact.category === "routine_anchor" ? [fact.anchorId] : [])),
  );
  if (removedAnchorIds.size > 0) {
    const anchors = work.aggregate.routineAnchors.filter((anchor) => !removedAnchorIds.has(anchor.anchorId));
    const result = acceptDomain(
      work,
      "routine-anchors-removed",
      item.occurredAt,
      { kind: "adult", adultId: item.senderAdultId },
      {
        kind: "routine_anchors.replaced",
        anchors,
      } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
    );
    if (result.receipt.disposition !== "accepted") return "blocked";
  }
  work.projection.sharedProfile = ApplicationProjectionSchema.shape.sharedProfile.parse({
    facts: work.projection.sharedProfile.facts.filter((fact) => !requested.has(fact.factKey)),
  });
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "onboarding_transition",
      occurredAt: item.occurredAt,
      decision: "shared_profile:removed",
      sourceRef: item.messageRef,
      adultId: item.senderAdultId,
      containsPrivateData: false,
    }),
  );
  return "changed";
}

function sharedProfileSummary(work: Work): string {
  const labels: Readonly<
    Record<ApplicationProjection["sharedProfile"]["facts"][number]["category"], string>
  > = {
    dependent: "Dependents",
    school_childcare: "School/childcare",
    recurring_activity: "Activities",
    routine_anchor: "Routine anchors",
    dietary_constraint: "Dietary constraints",
  };
  const lines = work.projection.sharedProfile.facts
    .slice(0, 30)
    .map((fact) =>
      fact.category === "routine_anchor"
        ? `${labels[fact.category]} — ${fact.subject} [${fact.anchorId}]: ${fact.detail} (${fact.localTime} ${fact.timeZone}; ISO weekdays ${fact.daysOfWeek.join(",")})`
        : `${labels[fact.category]} — ${fact.subject}: ${fact.detail}`,
    );
  const summary = `Shared profile (${work.aggregate.timeZone}):\n${lines
    .map((line) => `• ${line}`)
    .join("\n")}`;
  return summary.length <= 3_000 ? summary : `${summary.slice(0, 2_997)}…`;
}

function briefBody(work: Work, asOf: string): string {
  const aggregate = work.aggregate;
  const terminal = new Set(["completed", "dismissed", "superseded", "failed"]);
  const episodes = aggregate.episodes
    .filter((episode) => episode.scope.kind === "household" && !terminal.has(episode.state))
    .sort((left, right) => {
      const leftTime = left.temporalPlan?.referenceAt ?? "9999-12-31T23:59:59Z";
      const rightTime = right.temporalPlan?.referenceAt ?? "9999-12-31T23:59:59Z";
      return leftTime.localeCompare(rightTime) || left.title.localeCompare(right.title);
    })
    .slice(0, 30);
  const now = Temporal.Instant.from(asOf);
  const localNow = now.toZonedDateTimeISO(aggregate.timeZone);
  const todayStart = localNow.startOfDay().toInstant();
  const tomorrowStart = localNow.startOfDay().add({ days: 1 }).toInstant();
  const weekEnd = localNow.startOfDay().add({ days: 8 }).toInstant();
  const reference = (episode: (typeof episodes)[number]) =>
    episode.temporalPlan?.deadlineAt ?? episode.temporalPlan?.eventAt;
  const adultNames = new Map(
    work.projection.onboarding.adultNames.map((adult) => [adult.adultId, adult.displayName] as const),
  );
  const line = (episode: (typeof episodes)[number]) => {
    const state =
      episode.state === "awaiting_acknowledgement"
        ? "needs an owner acknowledgment"
        : episode.state === "blocked"
          ? "blocked and needs a decision"
          : episode.state.replaceAll("_", " ");
    const timing = reference(episode);
    const owner =
      episode.owner.status === "unassigned"
        ? ""
        : `; owner: ${adultNames.get(episode.owner.adultId) ?? "assigned adult"}`;
    if (timing === undefined) return `• ${episode.title} — ${state}${owner}`;
    const local = Temporal.Instant.from(timing).toZonedDateTimeISO(aggregate.timeZone);
    const minute = String(local.minute).padStart(2, "0");
    return `• ${episode.title} — ${state}${owner}; ${local.month}/${local.day} at ${local.hour}:${minute}`;
  };
  const used = new Set<string>();
  const select = (predicate: (episode: (typeof episodes)[number]) => boolean) =>
    episodes
      .filter((episode) => !used.has(episode.episodeId) && predicate(episode))
      .map((episode) => {
        used.add(episode.episodeId);
        return line(episode);
      });
  const attention = select((episode) => {
    const timing = reference(episode);
    return (
      ["awaiting_acknowledgement", "blocked"].includes(episode.state) ||
      (timing !== undefined && Temporal.Instant.compare(Temporal.Instant.from(timing), now) < 0)
    );
  });
  const today = select((episode) => {
    const timing = reference(episode);
    return (
      timing !== undefined &&
      Temporal.Instant.compare(Temporal.Instant.from(timing), todayStart) >= 0 &&
      Temporal.Instant.compare(Temporal.Instant.from(timing), tomorrowStart) < 0
    );
  });
  const upcoming = select((episode) => {
    const timing = reference(episode);
    return (
      timing !== undefined &&
      Temporal.Instant.compare(Temporal.Instant.from(timing), tomorrowStart) >= 0 &&
      Temporal.Instant.compare(Temporal.Instant.from(timing), weekEnd) < 0
    );
  });
  const projects = select((episode) => ["research", "meal_plan", "project"].includes(episode.type));
  const open = select(() => true).slice(0, 5);
  const pendingApprovals = aggregate.pendingActions
    .filter(
      (action) =>
        action.state === "awaiting_approval" &&
        Temporal.Instant.compare(Temporal.Instant.from(action.expiresAt), now) > 0,
    )
    .slice(0, 5)
    .map((pending) =>
      pending.action.kind === "calendar_update"
        ? `• Approve or decline “${pending.action.title}” before Florence writes it to the calendar.`
        : `• Approve or decline: ${pending.action.summary}.`,
    );
  const sectionEntries: Array<[string, string[]]> = [
    ["Needs attention", attention],
    ["Today", today],
    ["Next 7 days", upcoming],
    ["Projects", projects],
    ["Decisions", pendingApprovals],
    ["Other open items", open],
  ];
  const sections = sectionEntries
    .filter((section) => section[1].length > 0)
    .map(([title, lines]) => `${title}:\n${lines.join("\n")}`);
  const body =
    sections.length === 0
      ? "Daily brief: the shared family plan is clear today—no open commitments, projects, or decisions need attention."
      : `Daily brief for ${localNow.month}/${localNow.day}:\n\n${sections.join("\n\n")}`;
  return body.length <= 4_000 ? body : `${body.slice(0, 3_999)}…`;
}

function statusForDomain(result: AcceptanceResult): "processed" | "rejected" {
  return result.receipt.disposition === "rejected" ? "rejected" : "processed";
}

type ModelTemporalPlan = NonNullable<
  Extract<ConversationClassification, { intent: "propose_commitment" }>["temporalPlan"]
>;

function materializeInitialTemporalPlan(episodeId: string, candidate: ModelTemporalPlan) {
  return SemanticTimePlanSchema.parse({
    ...candidate,
    planId: stableId("temporal_plan", episodeId),
    version: 1,
    triggers: candidate.triggers.map((trigger, index) => ({
      ...trigger,
      triggerId: stableId("temporal_trigger", episodeId, "1", String(index)),
      timerId: stableId("timer", episodeId, "1", String(index)),
    })),
  });
}

function proposalForConversation(
  item: ConversationInboxItem,
  classification: Extract<ConversationClassification, { intent: "propose_commitment" }>,
) {
  const evidence = conversationEvidence(item);
  const episodeId = stableId("episode", item.idempotencyKey, "commitment");
  return EpisodeProposalSchema.parse({
    episodeId,
    type: "commitment",
    targetScope: targetScope(item),
    title: classification.title,
    requiredOutcome: classification.requiredOutcome,
    ...(classification.proposedOwnerAdultId === undefined
      ? {}
      : { proposedOwnerAdultId: classification.proposedOwnerAdultId }),
    evidence: [evidence],
    sourceClass: classification.sourceClass,
    sensitivity: classification.sensitivity,
    ...(classification.temporalPlan === undefined
      ? {}
      : { temporalPlan: materializeInitialTemporalPlan(episodeId, classification.temporalPlan) }),
  });
}

function processProjectRequest(
  work: Work,
  item: ConversationInboxItem,
  classification: Extract<
    ConversationClassification,
    { intent: "research_request" | "meal_plan_request" | "project_request" }
  >,
  routes: WorkerRoutes,
): "processed" | "rejected" {
  if (classification.scopeAssessment.decision === "out_of_scope") {
    queueMessage(
      work,
      `scope-declined-${classification.intent}`,
      targetScope(item),
      "status",
      "Florence handles work whose outcome affects the household. This request does not establish a household consequence.",
    );
    return "rejected";
  }

  const scope = targetScope(item);
  const evidence = conversationEvidence(item);
  const purpose: WorkerPurpose =
    classification.intent === "meal_plan_request"
      ? "meal_plan"
      : classification.intent === "project_request"
        ? "family_project"
        : "family_research";
  const episodeId = stableId("episode", item.idempotencyKey, purpose);
  const requiredOutcome =
    classification.scopeAssessment.decision === "narrow"
      ? classification.scopeAssessment.householdConsequence
      : classification.requiredOutcome;
  const details =
    classification.intent === "meal_plan_request"
      ? [`Horizon: ${classification.horizon}`, ...classification.constraints]
      : classification.constraints;
  const jobId = stableId("job", work.input.idempotencyKey, purpose);
  const result = acceptDomain(
    work,
    `project:${purpose}`,
    item.occurredAt,
    { kind: "adult", adultId: item.senderAdultId },
    {
      kind: "episode.proposed",
      proposal: EpisodeProposalSchema.parse({
        episodeId,
        type: purpose === "meal_plan" ? "meal_plan" : purpose === "family_project" ? "project" : "research",
        targetScope: scope,
        title: classification.title,
        requiredOutcome,
        evidence: [evidence],
        sourceClass:
          purpose === "meal_plan"
            ? "household.meal_request"
            : purpose === "family_project"
              ? "household.project_request"
              : "household.research_request",
        sensitivity: "ordinary",
        delegation: {
          jobId: DomainWorkerJobIdSchema.parse(jobId),
          purpose,
        },
        ...(classification.intent === "project_request" && classification.temporalPlan !== undefined
          ? { temporalPlan: materializeInitialTemporalPlan(episodeId, classification.temporalPlan) }
          : {}),
      }),
    } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
  );
  if (result.receipt.disposition !== "accepted") {
    return "rejected";
  }
  const job = buildWorkerJob({
    jobId,
    purpose,
    work,
    episodeId,
    evidenceId: evidence.evidenceId,
    title: classification.title,
    requiredOutcome,
    details,
    scope,
    routes,
    occurredAt: item.occurredAt,
  });
  enqueueWorker(work, purpose, episodeId, job, item.occurredAt);
  const startedWorker = latestProjectWorker(work, episodeId);
  queueMessage(
    work,
    `project-started-${purpose}`,
    scope,
    "status",
    purpose === "meal_plan"
      ? "The requested meal-planning project is open. Florence will return a practical plan and grocery list."
      : purpose === "family_project"
        ? "The family project is open. Florence will return a decision-ready plan with phases, next actions, decisions, and risks."
        : "The requested household research project is open. Florence will return a sourced comparison.",
    {
      kind: "episode_follow_up",
      episodeId,
      episodeVersion:
        work.aggregate.episodes.find((episode) => episode.episodeId === episodeId)?.version ?? 1,
    },
    startedWorker === undefined ? undefined : projectDeliveryGuard(startedWorker),
  );
  return "processed";
}

function latestProjectWorker(work: Work, episodeId: string) {
  return work.projection.workers.find((worker) => worker.episodeId === episodeId);
}

function projectDeliveryGuard(worker: ApplicationProjection["workers"][number]): ProjectDeliveryGuard {
  return {
    kind: "project_worker",
    episodeId: worker.episodeId,
    jobId: worker.job.jobId,
    generation: worker.deliveryGeneration,
  };
}

function advanceProjectDeliveryGeneration(work: Work, episodeId: string): ProjectDeliveryGuard | undefined {
  const workerIndex = work.projection.workers.findIndex((worker) => worker.episodeId === episodeId);
  const worker = work.projection.workers[workerIndex];
  if (worker === undefined) return undefined;
  const updated = {
    ...worker,
    deliveryGeneration: worker.deliveryGeneration + 1,
  };
  work.projection.workers[workerIndex] = updated;
  return projectDeliveryGuard(updated);
}

function replyBindsEpisode(
  item: ConversationInboxItem,
  kinds: readonly ("episode_follow_up" | "episode_ownership")[],
  episodeId: string,
  episodeVersion: number,
): boolean {
  const context = item.replyTo?.responseContext;
  return (
    context !== undefined &&
    (context.kind === "episode_follow_up" || context.kind === "episode_ownership") &&
    kinds.includes(context.kind) &&
    context.episodeId === episodeId &&
    context.episodeVersion === episodeVersion
  );
}

function scopeMatchesConversation(
  episode: HouseholdAggregate["episodes"][number],
  item: ConversationInboxItem,
) {
  const scope = targetScope(item);
  return (
    episode.scope.kind === scope.kind &&
    (episode.scope.kind === "household" ||
      (scope.kind === "personal" && episode.scope.adultId === scope.adultId))
  );
}

function projectPurposeForEpisode(
  type: HouseholdAggregate["episodes"][number]["type"],
): WorkerPurpose | undefined {
  switch (type) {
    case "research":
      return "family_research";
    case "meal_plan":
      return "meal_plan";
    case "project":
      return "family_project";
    case "commitment":
      return undefined;
  }
}

function processProjectFollowUp(
  work: Work,
  item: ConversationInboxItem,
  classification: Extract<ConversationClassification, { intent: "project_follow_up" }>,
  routes: WorkerRoutes,
): "processed" | "rejected" {
  const episode = work.aggregate.episodes.find(
    (candidate) => candidate.episodeId === classification.episodeId,
  );
  const scope = targetScope(item);
  const boundReply = replyBindsEpisode(
    item,
    ["episode_follow_up"],
    classification.episodeId,
    classification.baseEpisodeVersion,
  );
  if (
    episode === undefined ||
    projectPurposeForEpisode(episode.type) === undefined ||
    episode.version !== classification.baseEpisodeVersion ||
    episode.scope.kind !== scope.kind ||
    (episode.scope.kind === "personal" &&
      (scope.kind !== "personal" || episode.scope.adultId !== scope.adultId)) ||
    !boundReply
  ) {
    const repliedContext = item.replyTo?.responseContext;
    const repliedEpisode =
      repliedContext?.kind === "episode_follow_up"
        ? work.aggregate.episodes.find((candidate) => candidate.episodeId === repliedContext.episodeId)
        : undefined;
    const currentContext =
      repliedEpisode === undefined ||
      projectPurposeForEpisode(repliedEpisode.type) === undefined ||
      !scopeMatchesConversation(repliedEpisode, item)
        ? undefined
        : ({
            kind: "episode_follow_up" as const,
            episodeId: repliedEpisode.episodeId,
            episodeVersion: repliedEpisode.version,
          } as const);
    const deliveryGuard =
      currentContext === undefined
        ? undefined
        : advanceProjectDeliveryGeneration(work, currentContext.episodeId);
    queueMessage(
      work,
      "project-follow-up-stale",
      scope,
      "status",
      "That project reply no longer points to the current project version. Ask me for its status or send the updated instruction again.",
      currentContext,
      deliveryGuard,
    );
    return "rejected";
  }

  const latestWorker = latestProjectWorker(work, episode.episodeId);
  if (classification.action === "status") {
    const body =
      latestWorker === undefined
        ? "I can’t find a delegated run for that project yet. Reply here with what you want me to investigate next."
        : latestWorker.status === "queued"
          ? "The Project Lead is still working on this. I’ll return the result here when it is ready."
          : latestWorker.status === "awaiting_input"
            ? (latestWorker.latestSummary ??
              `The Project Lead is waiting for: ${latestWorker.outstandingQuestions.join("; ")}`)
            : latestWorker.status === "completed"
              ? (latestWorker.latestSummary ?? "The latest Project Lead run completed.")
              : latestWorker.status === "cancelled"
                ? "That project is closed. Start a new family project if more work is needed."
                : "The last Project Lead run did not complete safely. Reply here with a follow-up instruction and I’ll restart from the current family context.";
    const deliveryGuard = advanceProjectDeliveryGeneration(work, episode.episodeId);
    queueMessage(
      work,
      "project-follow-up-status",
      episode.scope,
      "status",
      body,
      {
        kind: "episode_follow_up",
        episodeId: episode.episodeId,
        episodeVersion: episode.version,
      },
      deliveryGuard,
    );
    return "processed";
  }

  const evidence = conversationEvidence(item);
  if (classification.action === "cancel" || classification.action === "complete") {
    if (["completed", "dismissed", "superseded", "failed"].includes(episode.state)) {
      const deliveryGuard = advanceProjectDeliveryGeneration(work, episode.episodeId);
      queueMessage(
        work,
        "project-follow-up-already-closed",
        episode.scope,
        "status",
        "That project is already closed. Start a new request if more work is needed.",
        {
          kind: "episode_follow_up",
          episodeId: episode.episodeId,
          episodeVersion: episode.version,
        },
        deliveryGuard,
      );
      return "processed";
    }
    const completed = classification.action === "complete";
    const result = acceptDomain(
      work,
      completed ? "project-completed" : "project-cancelled",
      item.occurredAt,
      { kind: "adult", adultId: item.senderAdultId },
      {
        kind: "episode.closed",
        episodeId: episode.episodeId,
        baseEpisodeVersion: episode.version,
        outcome: {
          kind: completed ? "completed" : "dismissed",
          summary: completed
            ? "An adult completed the delegated family project."
            : "An adult cancelled the delegated family project.",
          evidence: [evidence],
          recordedAt: item.occurredAt,
        },
      } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
    );
    if (result.receipt.disposition === "accepted") {
      if (
        latestWorker !== undefined &&
        (!completed || ["queued", "awaiting_input"].includes(latestWorker.status))
      ) {
        work.projection.workers[work.projection.workers.indexOf(latestWorker)] = {
          ...latestWorker,
          status: "cancelled",
          updatedAt: item.occurredAt,
        };
      }
      const closedEpisode = work.aggregate.episodes.find(
        (candidate) => candidate.episodeId === episode.episodeId,
      );
      const deliveryGuard = advanceProjectDeliveryGeneration(work, episode.episodeId);
      queueMessage(
        work,
        completed ? "project-follow-up-completed" : "project-follow-up-cancelled",
        episode.scope,
        "status",
        completed ? "I recorded that project as complete." : "I stopped that project.",
        {
          kind: "episode_follow_up",
          episodeId: episode.episodeId,
          episodeVersion: closedEpisode?.version ?? episode.version,
        },
        deliveryGuard,
      );
    }
    return statusForDomain(result);
  }

  if (classification.instruction === undefined) {
    return "rejected";
  }
  if (
    episode.type === "project" &&
    ["completed", "dismissed", "superseded", "failed"].includes(episode.state)
  ) {
    const deliveryGuard = advanceProjectDeliveryGeneration(work, episode.episodeId);
    queueMessage(
      work,
      "project-follow-up-closed",
      episode.scope,
      "status",
      "That project is closed. Start a new family project if more work is needed.",
      {
        kind: "episode_follow_up",
        episodeId: episode.episodeId,
        episodeVersion: episode.version,
      },
      deliveryGuard,
    );
    return "processed";
  }
  const purpose = projectPurposeForEpisode(episode.type);
  if (purpose === undefined) {
    return "rejected";
  }
  if (latestWorker === undefined || episode.delegation === undefined) {
    return "rejected";
  }
  const jobId = episode.delegation.jobId;
  const delegated = acceptDomain(
    work,
    "project-follow-up",
    item.occurredAt,
    { kind: "adult", adultId: item.senderAdultId },
    {
      kind: "episode.project_delegated",
      episodeId: episode.episodeId,
      baseEpisodeVersion: episode.version,
      delegation: { jobId: DomainWorkerJobIdSchema.parse(jobId), purpose },
      instructionEvidence: evidence,
    } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
  );
  if (delegated.receipt.disposition !== "accepted") {
    return "rejected";
  }
  const updatedEpisode = work.aggregate.episodes.find(
    (candidate) => candidate.episodeId === episode.episodeId,
  );
  if (updatedEpisode === undefined) {
    throw new Error("Delegated project disappeared from the household aggregate");
  }
  const job = buildWorkerJob({
    jobId,
    attemptNumber: latestWorker.attemptNumber + 1,
    purpose,
    work,
    episodeId: updatedEpisode.episodeId,
    evidenceId: evidence.evidenceId,
    title: updatedEpisode.title,
    requiredOutcome: updatedEpisode.requiredOutcome,
    details: [
      `Project Lead follow-up: ${classification.instruction}`,
      `Original project brief: ${latestWorker.projectBrief}`,
      ...(latestWorker.latestSummary === undefined
        ? []
        : [`Previous Project Lead result: ${latestWorker.latestSummary}`]),
      ...(latestWorker.artifact === undefined
        ? []
        : [`Current structured project artifact: ${JSON.stringify(latestWorker.artifact)}`]),
      ...latestWorker.outstandingQuestions.map((question) => `Earlier open question: ${question}`),
    ],
    scope: updatedEpisode.scope,
    routes,
    occurredAt: item.occurredAt,
  });
  const workerIndex = work.projection.workers.indexOf(latestWorker);
  const { lastErrorCode: _lastErrorCode, resultRef: _resultRef, ...worker } = latestWorker;
  work.projection.workers[workerIndex] = {
    ...worker,
    baseEpisodeVersion: updatedEpisode.version,
    contextFingerprint: workerContextFingerprint({
      aggregate: work.aggregate,
      projection: work.projection,
      episodeId: updatedEpisode.episodeId,
      purpose,
      scope: updatedEpisode.scope,
      evidenceRefs: job.evidenceRefs,
      asOf: item.occurredAt,
    }),
    job,
    status: "queued",
    attemptNumber: latestWorker.attemptNumber + 1,
    automaticRetryCount: 0,
    deliveryGeneration: latestWorker.deliveryGeneration + 1,
    outstandingQuestions: [],
    updatedAt: item.occurredAt,
  };
  work.outbox.push(
    ApplicationOutboxIntentSchema.parse({
      ...appOutboxBase(work.input, `run:${job.attemptId}`),
      kind: "worker.run",
      job,
    }),
  );
  queueMessage(
    work,
    "project-follow-up-started",
    updatedEpisode.scope,
    "status",
    "I updated the Project Lead with that instruction. I’ll return the next result here.",
    {
      kind: "episode_follow_up",
      episodeId: updatedEpisode.episodeId,
      episodeVersion: updatedEpisode.version,
    },
    projectDeliveryGuard(work.projection.workers[workerIndex] as ApplicationProjection["workers"][number]),
  );
  return "processed";
}

function queueCalendarUnavailable(
  work: Work,
  item: ConversationInboxItem,
  reason:
    | "no_write_connection"
    | "ambiguous_write_connection"
    | "no_write_calendar"
    | "ambiguous_write_calendar"
    | "projection_incomplete",
): void {
  const body = (
    {
      no_write_connection:
        "I need the requesting adult to connect a Google account with Calendar access in a private DM before I can prepare this event.",
      ambiguous_write_connection:
        "More than one writable Google account matches. Name the exact linked account and calendar in the event request.",
      no_write_calendar:
        "I couldn’t find an active writable calendar matching that request. Say the exact “account / calendar” name from “show my calendars.”",
      ambiguous_write_calendar:
        "More than one writable calendar matches. Say the exact “account / calendar” name from “show my calendars.”",
      projection_incomplete:
        "I’m still synchronizing private calendar availability, so I won’t ask for approval yet. Try the request again after synchronization finishes.",
    } satisfies Record<typeof reason, string>
  )[reason];
  queueMessage(work, `calendar-unavailable:${reason}`, targetScope(item), "status", body);
}

async function proposeCalendarEvent(
  work: Work,
  item: ConversationInboxItem,
  classification: Extract<ConversationClassification, { intent: "calendar_event_create_request" }>,
  dependencies: FlorenceApplicationDependencies,
): Promise<"processed" | "rejected"> {
  if (item.channel.scope !== "household" || dependencies.calendarActions === undefined) {
    queueMessage(
      work,
      "calendar-household-only",
      targetScope(item),
      "status",
      item.channel.scope !== "household"
        ? "Ask for household calendar creation in the household group so the proposed event and approval stay shared."
        : "Calendar creation is not available until Google Calendar is connected.",
    );
    return "rejected";
  }
  const preparation = await dependencies.calendarActions.prepareCreate({
    householdId: item.householdId,
    verifiedAdultIds: work.aggregate.verifiedAdultIds,
    requestedByAdultId: item.senderAdultId,
    asOf: item.occurredAt,
    startsAt: classification.startsAt,
    endsAt: classification.endsAt,
    ...(classification.calendarAccountLabel === undefined
      ? {}
      : { accountLabel: classification.calendarAccountLabel }),
    ...(classification.calendarName === undefined ? {} : { calendarName: classification.calendarName }),
  });
  if (preparation.status === "unavailable") {
    queueCalendarUnavailable(work, item, preparation.reason);
    return "rejected";
  }
  const evidence = conversationEvidence(item);
  const actionWithoutDigest = {
    actionId: ExternalActionIdSchema.parse(stableId("action", item.idempotencyKey, "calendar-create")),
    kind: "calendar_update" as const,
    calendarActionVersion: 1 as const,
    operation: "create" as const,
    householdId: item.householdId,
    summary: "create the approved household calendar event",
    relevantDataDigest: preparation.relevantDataDigest,
    requestedFor: { kind: "household" as const },
    evidence: [evidence],
    title: classification.title,
    startsAt: classification.startsAt,
    endsAt: classification.endsAt,
    timeZone: classification.timeZone,
    requestedByAdultId: item.senderAdultId,
    availabilityAdultIds: [...work.aggregate.verifiedAdultIds].sort(),
    targetConnectionId: preparation.targetConnectionId,
    calendarId: preparation.calendarId,
    hasConflict: preparation.hasConflict,
  };
  const action = CalendarEventCreateActionSchema.parse({
    ...actionWithoutDigest,
    actionDigest: calendarEventCreateActionDigest(actionWithoutDigest),
  });
  const result = acceptDomain(
    work,
    "calendar-action-proposed",
    item.occurredAt,
    { kind: "adult", adultId: item.senderAdultId },
    { kind: "external_action.proposed", action } as Omit<
      HouseholdSignal,
      "householdId" | "signalId" | "sequence" | "occurredAt" | "actor"
    >,
  );
  return statusForDomain(result);
}

function askForCalendarFields(
  work: Work,
  item: ConversationInboxItem,
  missingFields: readonly ("title" | "start" | "end" | "timeZone")[],
): "processed" | "rejected" {
  if (item.channel.scope !== "household") {
    queueMessage(
      work,
      "calendar-clarification-household-only",
      targetScope(item),
      "clarifying_question",
      "Ask for household calendar creation in the household group so the proposed event and approval stay shared.",
    );
    return "rejected";
  }
  const labels = {
    title: "an event title",
    start: "a start date and time",
    end: "an end date and time",
    timeZone: "a time zone",
  } as const;
  const needed = missingFields.map((field) => labels[field]);
  const fieldList =
    needed.length === 1
      ? needed[0]
      : needed.length === 2
        ? `${needed[0]} and ${needed[1]}`
        : `${needed.slice(0, -1).join(", ")}, and ${needed.at(-1)}`;
  queueMessage(
    work,
    "calendar-clarification",
    { kind: "household" },
    "clarifying_question",
    `To prepare one exact calendar proposal, please provide ${fieldList}.`,
  );
  return "processed";
}

async function approveCalendarEvent(
  work: Work,
  item: ConversationInboxItem,
  actionId: string,
  dependencies: FlorenceApplicationDependencies,
): Promise<"processed" | "rejected"> {
  if (item.channel.scope !== "household" || dependencies.calendarActions === undefined) {
    return "rejected";
  }
  const pending = work.aggregate.pendingActions.find(
    (candidate) => candidate.action.actionId === actionId && candidate.action.kind === "calendar_update",
  );
  if (
    pending === undefined ||
    pending.action.kind !== "calendar_update" ||
    pending.state !== "awaiting_approval"
  ) {
    queueMessage(
      work,
      "calendar-approval-missing",
      { kind: "household" },
      "status",
      "That exact calendar proposal is no longer awaiting approval.",
    );
    return "rejected";
  }
  const repliedContext = item.replyTo?.responseContext;
  if (repliedContext?.kind !== "calendar_approval" || repliedContext.actionId !== actionId) {
    queueMessage(
      work,
      "calendar-approval-unbound",
      { kind: "household" },
      "status",
      "I couldn’t bind that approval to this exact current calendar proposal, so I changed nothing. Reply directly to its Florence approval message.",
    );
    return "rejected";
  }
  if (Temporal.Instant.compare(Temporal.Instant.from(item.occurredAt), pending.expiresAt) >= 0) {
    const expired = acceptDomain(
      work,
      "calendar-action-expired",
      item.occurredAt,
      { kind: "adult", adultId: item.senderAdultId },
      { kind: "external_action.expired", actionId: pending.action.actionId } as Omit<
        HouseholdSignal,
        "householdId" | "signalId" | "sequence" | "occurredAt" | "actor"
      >,
    );
    if (expired.receipt.disposition === "accepted") {
      queueMessage(
        work,
        "calendar-approval-expired",
        { kind: "household" },
        "status",
        "That calendar proposal expired, so I did not write anything. Send the event request again for a fresh availability check and approval.",
      );
    }
    return "rejected";
  }
  const preparation = await dependencies.calendarActions.prepareCreate({
    householdId: item.householdId,
    verifiedAdultIds: work.aggregate.verifiedAdultIds,
    requestedByAdultId: pending.action.requestedByAdultId,
    asOf: item.occurredAt,
    startsAt: pending.action.startsAt,
    endsAt: pending.action.endsAt,
    targetConnectionId: pending.action.targetConnectionId,
    calendarId: pending.action.calendarId,
  });
  if (preparation.status === "unavailable") {
    queueCalendarUnavailable(work, item, preparation.reason);
    return "rejected";
  }
  if (
    preparation.targetConnectionId !== pending.action.targetConnectionId ||
    preparation.calendarId !== pending.action.calendarId ||
    preparation.relevantDataDigest !== pending.action.relevantDataDigest ||
    preparation.hasConflict !== pending.action.hasConflict
  ) {
    queueMessage(
      work,
      "calendar-approval-invalidated",
      { kind: "household" },
      "status",
      "The selected calendar or household availability changed after this proposal, so I did not create the event. Send the event request again for an updated conflict check and approval.",
    );
    return "rejected";
  }
  const approvalId = ApprovalIdSchema.parse(stableId("approval", item.idempotencyKey, actionId));
  const result = acceptDomain(
    work,
    "calendar-action-approved",
    item.occurredAt,
    { kind: "adult", adultId: item.senderAdultId },
    {
      kind: "approval.granted",
      approval: {
        approvalId,
        householdId: item.householdId,
        grantedByAdultId: item.senderAdultId,
        target: {
          kind: "external_action",
          actionId: pending.action.actionId,
          actionDigest: pending.action.actionDigest,
          relevantDataDigest: pending.action.relevantDataDigest,
        },
        policyVersion: work.aggregate.policyVersion,
        grantedAt: item.occurredAt,
        expiresAt: plusMilliseconds(item.occurredAt, 15 * 60_000),
        status: "active",
      },
    } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
  );
  return statusForDomain(result);
}

function declineCalendarEvent(
  work: Work,
  item: ConversationInboxItem,
  actionId: string,
): "processed" | "rejected" {
  if (item.channel.scope !== "household") {
    return "rejected";
  }
  const repliedContext = item.replyTo?.responseContext;
  if (repliedContext?.kind !== "calendar_approval" || repliedContext.actionId !== actionId) {
    queueMessage(
      work,
      "calendar-decline-unbound",
      { kind: "household" },
      "status",
      "I couldn’t bind that decline to one exact current calendar proposal, so I changed nothing. Reply directly to its Florence approval message.",
    );
    return "rejected";
  }
  const pending = work.aggregate.pendingActions.find(
    (candidate) =>
      candidate.action.actionId === actionId &&
      candidate.action.kind === "calendar_update" &&
      candidate.state === "awaiting_approval",
  );
  if (pending === undefined || pending.action.kind !== "calendar_update") {
    queueMessage(
      work,
      "calendar-decline-missing",
      { kind: "household" },
      "status",
      "That exact calendar proposal is no longer awaiting a decision.",
    );
    return "rejected";
  }
  const expired = Temporal.Instant.compare(Temporal.Instant.from(item.occurredAt), pending.expiresAt) >= 0;
  const result = acceptDomain(
    work,
    expired ? "calendar-action-expired" : "calendar-action-declined",
    item.occurredAt,
    { kind: "adult", adultId: item.senderAdultId },
    {
      kind: expired ? "external_action.expired" : "external_action.declined",
      actionId: pending.action.actionId,
    } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
  );
  if (result.receipt.disposition === "accepted") {
    queueMessage(
      work,
      expired ? "calendar-decline-expired" : "calendar-declined",
      { kind: "household" },
      "status",
      expired
        ? "That calendar proposal had already expired, so nothing was written."
        : `Declined “${pending.action.title}.” Nothing was written to the calendar.`,
    );
  }
  return statusForDomain(result);
}

function commitmentForBoundReply(
  work: Work,
  item: ConversationInboxItem,
  episodeId: string,
  episodeVersion: number,
  kinds: readonly ("episode_follow_up" | "episode_ownership")[],
): HouseholdAggregate["episodes"][number] | undefined {
  const episode = work.aggregate.episodes.find((candidate) => candidate.episodeId === episodeId);
  return episode !== undefined &&
    episode.type === "commitment" &&
    episode.version === episodeVersion &&
    scopeMatchesConversation(episode, item) &&
    replyBindsEpisode(item, kinds, episodeId, episodeVersion)
    ? episode
    : undefined;
}

function rejectUnboundCommitment(work: Work, item: ConversationInboxItem): "rejected" {
  queueMessage(
    work,
    "commitment-reply-unbound",
    targetScope(item),
    "status",
    "I couldn’t bind that reply to one current commitment, so I changed nothing. Reply directly to the relevant Florence commitment message.",
  );
  return "rejected";
}

function correctedTemporalPlan(
  item: ConversationInboxItem,
  episode: HouseholdAggregate["episodes"][number],
  correction: Extract<ConversationClassification, { intent: "replace_temporal_plan" }>["temporalPlan"],
) {
  const nextPlanVersion = (episode.temporalPlan?.definition.version ?? 0) + 1;
  return SemanticTimePlanSchema.parse({
    ...correction,
    planId: episode.temporalPlan?.definition.planId ?? stableId("temporal_plan", episode.episodeId),
    version: nextPlanVersion,
    triggers: correction.triggers.map((trigger, index) => ({
      ...trigger,
      triggerId: stableId(
        "temporal_trigger",
        episode.episodeId,
        String(nextPlanVersion),
        item.idempotencyKey,
        String(index),
      ),
      timerId: stableId(
        "timer",
        episode.episodeId,
        String(nextPlanVersion),
        item.idempotencyKey,
        String(index),
      ),
    })),
  });
}

async function processActiveConversation(
  work: Work,
  item: ConversationInboxItem,
  classification: ConversationClassification,
  routes: WorkerRoutes,
  dependencies: FlorenceApplicationDependencies,
): Promise<"processed" | "rejected"> {
  switch (classification.intent) {
    case "ignore":
      return "processed";
    case "onboarding":
      return applyOnboarding(work, item, classification) ? "processed" : "rejected";
    case "propose_commitment": {
      const proposal = proposalForConversation(item, classification);
      const result = acceptDomain(
        work,
        "commitment-proposed",
        item.occurredAt,
        { kind: "adult", adultId: item.senderAdultId },
        { kind: "episode.proposed", proposal } as Omit<
          HouseholdSignal,
          "householdId" | "signalId" | "sequence" | "occurredAt" | "actor"
        >,
      );
      if (result.receipt.disposition === "accepted") {
        const episode = work.aggregate.episodes.find(
          (candidate) => candidate.episodeId === proposal.episodeId,
        );
        if (episode === undefined) {
          throw new Error("Accepted commitment is missing from the household aggregate");
        }
        queueMessage(
          work,
          "commitment-captured",
          targetScope(item),
          "status",
          classification.proposedOwnerAdultId === undefined
            ? `Captured “${proposal.title}” as an open proposed commitment. Use iMessage’s Reply on this Florence message for an update or completion.`
            : `Captured “${proposal.title}”. The proposed owner should use iMessage’s Reply on this Florence message and say “I accept.”`,
          {
            kind:
              classification.proposedOwnerAdultId === undefined ? "episode_follow_up" : "episode_ownership",
            episodeId: episode.episodeId,
            episodeVersion: episode.version,
          },
        );
      }
      return statusForDomain(result);
    }
    case "acknowledge_owner": {
      if (
        commitmentForBoundReply(work, item, classification.episodeId, classification.baseEpisodeVersion, [
          "episode_ownership",
        ]) === undefined
      ) {
        return rejectUnboundCommitment(work, item);
      }
      const result = acceptDomain(
        work,
        "owner-acknowledged",
        item.occurredAt,
        { kind: "adult", adultId: item.senderAdultId },
        {
          kind: "commitment.owner_acknowledged",
          episodeId: classification.episodeId,
          baseEpisodeVersion: classification.baseEpisodeVersion,
        } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
      );
      if (result.receipt.disposition === "accepted") {
        const episode = work.aggregate.episodes.find(
          (candidate) => candidate.episodeId === classification.episodeId,
        );
        if (episode === undefined) {
          throw new Error("Acknowledged commitment is missing from the household aggregate");
        }
        queueMessage(
          work,
          "owner-acknowledged-status",
          targetScope(item),
          "status",
          "The commitment owner is acknowledged.",
          {
            kind: "episode_follow_up",
            episodeId: episode.episodeId,
            episodeVersion: episode.version,
          },
        );
      }
      return statusForDomain(result);
    }
    case "reassign_owner": {
      if (
        commitmentForBoundReply(work, item, classification.episodeId, classification.baseEpisodeVersion, [
          "episode_follow_up",
          "episode_ownership",
        ]) === undefined
      ) {
        return rejectUnboundCommitment(work, item);
      }
      const result = acceptDomain(
        work,
        "owner-reassigned",
        item.occurredAt,
        { kind: "adult", adultId: item.senderAdultId },
        {
          kind: "commitment.owner_reassigned",
          episodeId: classification.episodeId,
          baseEpisodeVersion: classification.baseEpisodeVersion,
          proposedOwnerAdultId: classification.proposedOwnerAdultId,
        } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
      );
      if (result.receipt.disposition === "accepted") {
        const episode = work.aggregate.episodes.find(
          (candidate) => candidate.episodeId === classification.episodeId,
        );
        if (episode === undefined) {
          throw new Error("Reassigned commitment is missing from the household aggregate");
        }
        queueMessage(
          work,
          "owner-reassigned-status",
          targetScope(item),
          "status",
          "The proposed owner was updated. They should use iMessage’s Reply on this Florence message and say “I accept.”",
          {
            kind: "episode_ownership",
            episodeId: episode.episodeId,
            episodeVersion: episode.version,
          },
        );
      }
      return statusForDomain(result);
    }
    case "close_episode": {
      if (
        commitmentForBoundReply(work, item, classification.episodeId, classification.baseEpisodeVersion, [
          "episode_follow_up",
        ]) === undefined
      ) {
        return rejectUnboundCommitment(work, item);
      }
      const evidence = conversationEvidence(item);
      const result = acceptDomain(
        work,
        "episode-closed",
        item.occurredAt,
        { kind: "adult", adultId: item.senderAdultId },
        {
          kind: "episode.closed",
          episodeId: classification.episodeId,
          baseEpisodeVersion: classification.baseEpisodeVersion,
          outcome: {
            kind: classification.outcome,
            summary: classification.summary,
            evidence: [evidence],
            recordedAt: item.occurredAt,
          },
        } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
      );
      if (result.receipt.disposition === "accepted") {
        queueMessage(
          work,
          "episode-closed-status",
          targetScope(item),
          "status",
          classification.outcome === "completed"
            ? "The commitment is recorded as completed."
            : "The commitment is recorded as dismissed.",
        );
      }
      return statusForDomain(result);
    }
    case "replace_temporal_plan": {
      const episode = commitmentForBoundReply(
        work,
        item,
        classification.episodeId,
        classification.baseEpisodeVersion,
        ["episode_follow_up", "episode_ownership"],
      );
      if (episode === undefined) {
        return rejectUnboundCommitment(work, item);
      }
      const result = acceptDomain(
        work,
        "commitment-temporal-plan-replaced",
        item.occurredAt,
        { kind: "adult", adultId: item.senderAdultId },
        {
          kind: "episode.temporal_plan_replaced",
          episodeId: episode.episodeId,
          baseEpisodeVersion: episode.version,
          plan: correctedTemporalPlan(item, episode, classification.temporalPlan),
        } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
      );
      if (result.receipt.disposition === "accepted") {
        const updated = work.aggregate.episodes.find(
          (candidate) => candidate.episodeId === episode.episodeId,
        );
        if (updated === undefined) {
          throw new Error("Retimed commitment is missing from the household aggregate");
        }
        queueMessage(
          work,
          "commitment-temporal-plan-updated",
          targetScope(item),
          "status",
          `Updated the date, time, and reminder plan for “${updated.title}.”`,
          updated.state === "awaiting_acknowledgement"
            ? {
                kind: "episode_ownership",
                episodeId: updated.episodeId,
                episodeVersion: updated.version,
              }
            : {
                kind: "episode_follow_up",
                episodeId: updated.episodeId,
                episodeVersion: updated.version,
              },
        );
      } else {
        queueMessage(
          work,
          "commitment-temporal-plan-invalid",
          targetScope(item),
          "status",
          "I couldn’t safely apply that timing correction, so the existing timing and reminders are unchanged. Reply to the current commitment message with an exact date, time, and reminder request.",
        );
      }
      return statusForDomain(result);
    }
    case "research_request":
    case "meal_plan_request":
    case "project_request":
      return processProjectRequest(work, item, classification, routes);
    case "project_follow_up":
      return processProjectFollowUp(work, item, classification, routes);
    case "calendar_event_create_request":
      return proposeCalendarEvent(work, item, classification, dependencies);
    case "calendar_event_clarification":
      return askForCalendarFields(work, item, classification.missingFields);
    case "approve_calendar_event":
      return approveCalendarEvent(work, item, classification.actionId, dependencies);
    case "decline_calendar_event":
      return declineCalendarEvent(work, item, classification.actionId);
    case "daily_brief_request":
      queueMessage(work, "brief-request", targetScope(item), "daily_brief", briefBody(work, item.occurredAt));
      return "processed";
    case "approve_promotion":
      return approvePromotion(
        work,
        item,
        classification.promotionId,
        classification.rememberForMatchingSource === true,
      );
    case "decline_promotion":
      return declinePromotion(work, item, classification.promotionId);
    case "memory_candidate_decision":
      return decideMemoryCandidate(work, item, classification.candidateId, classification.decision);
    case "revoke_policy":
      return revokePolicy(work, item, classification.policyId, classification.expectedPolicyVersion);
  }
}

function decideMemoryCandidate(
  work: Work,
  item: ConversationInboxItem,
  candidateId: string,
  decision: "remember" | "reject",
): "processed" | "rejected" {
  const candidate = work.aggregate.memoryCandidates.find((current) => current.candidateId === candidateId);
  const visible =
    candidate !== undefined &&
    ((candidate.scope.kind === "household" && item.channel.scope === "household") ||
      (candidate.scope.kind === "personal" &&
        item.channel.scope === "personal" &&
        candidate.scope.adultId === item.senderAdultId));
  if (!visible || candidate === undefined) {
    queueMessage(
      work,
      "memory-candidate-stale",
      targetScope(item),
      "status",
      "That learning suggestion is no longer pending, so I didn’t change what Florence remembers.",
    );
    return "rejected";
  }

  const signal =
    decision === "remember"
      ? ({
          kind: "memory.confirmed",
          memoryId: stableId("memory", candidate.candidateId),
          candidateId: candidate.candidateId,
          targetScope: candidate.scope,
        } as Omit<
          Extract<HouseholdSignal, { kind: "memory.confirmed" }>,
          "householdId" | "signalId" | "sequence" | "occurredAt" | "actor"
        >)
      : ({
          kind: "memory.candidate_rejected",
          candidateId: candidate.candidateId,
        } as Omit<
          Extract<HouseholdSignal, { kind: "memory.candidate_rejected" }>,
          "householdId" | "signalId" | "sequence" | "occurredAt" | "actor"
        >);
  const result = acceptDomain(
    work,
    `memory-candidate:${decision}`,
    item.occurredAt,
    { kind: "adult", adultId: item.senderAdultId },
    signal,
  );
  queueMessage(
    work,
    `memory-candidate-${decision}-status`,
    candidate.scope,
    "status",
    result.receipt.disposition !== "accepted"
      ? "That learning suggestion changed before I could apply your answer, so I didn’t change what Florence remembers."
      : decision === "remember"
        ? "Got it. I’ll use that in future family work, and you can tell me to forget it at any time."
        : "Understood. I won’t remember that suggestion.",
  );
  return statusForDomain(result);
}

function queuePromotedCommitment(work: Work, suffix: string, pending: PendingPromotion): void {
  const episode = work.aggregate.episodes.find(
    (candidate) => candidate.episodeId === pending.proposal.episodeId,
  );
  if (episode === undefined || episode.type !== "commitment") {
    throw new Error("Accepted private-source promotion is missing its commitment episode");
  }
  const awaitingOwner = episode.owner.status === "proposed";
  const body = awaitingOwner
    ? `${pending.minimumHouseholdMeaning} The proposed owner should use iMessage’s Reply on this Florence message and say “I accept.”`
    : episode.owner.status === "unassigned"
      ? `${pending.minimumHouseholdMeaning} This shared commitment is unassigned. Use iMessage’s Reply on this Florence message to say who should own it.`
      : pending.minimumHouseholdMeaning;
  queueMessage(work, suffix, { kind: "household" }, "status", body, {
    kind: awaitingOwner ? "episode_ownership" : "episode_follow_up",
    episodeId: episode.episodeId,
    episodeVersion: episode.version,
  });
}

function approvePromotion(
  work: Work,
  item: ConversationInboxItem,
  promotionId: string,
  rememberForMatchingSource: boolean,
): "processed" | "rejected" {
  if (item.channel.scope !== "personal") {
    return "rejected";
  }
  const index = work.projection.pendingPromotions.findIndex(
    (candidate) => candidate.promotionId === promotionId && candidate.ownerAdultId === item.senderAdultId,
  );
  const pending = work.projection.pendingPromotions[index];
  if (pending === undefined) {
    queueMessage(
      work,
      "promotion-missing",
      personal(item.senderAdultId),
      "status",
      "That private sharing proposal is no longer pending.",
    );
    return "rejected";
  }

  const approvalId = ApprovalIdSchema.parse(stableId("approval", item.idempotencyKey, promotionId));
  const approval = acceptDomain(
    work,
    "promotion-approved",
    item.occurredAt,
    { kind: "adult", adultId: item.senderAdultId },
    {
      kind: "approval.granted",
      approval: {
        approvalId,
        householdId: item.householdId,
        grantedByAdultId: item.senderAdultId,
        target: {
          kind: "scope_promotion",
          from: { kind: "personal", adultId: item.senderAdultId },
          to: { kind: "household" },
          evidenceIds: [pending.evidence.evidenceId],
        },
        policyVersion: work.aggregate.policyVersion,
        grantedAt: item.occurredAt,
        expiresAt: plusMilliseconds(item.occurredAt, 24 * 60 * 60_000),
        status: "active",
      },
    } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
  );
  if (approval.receipt.disposition !== "accepted") {
    return "rejected";
  }
  const promoted = acceptDomain(
    work,
    "promotion-created-episode",
    item.occurredAt,
    { kind: "adult", adultId: item.senderAdultId },
    {
      kind: "episode.proposed",
      proposal: {
        ...pending.proposal,
        promotionAuthority: { kind: "approval", approvalId },
      },
    } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
  );
  if (promoted.receipt.disposition !== "accepted") {
    return "rejected";
  }
  const calendarSource = work.projection.calendarSources.find(
    (candidate) => candidate.pendingPromotionId === pending.promotionId,
  );
  if (calendarSource !== undefined) {
    delete calendarSource.pendingPromotionId;
    calendarSource.episodeId = pending.proposal.episodeId;
    calendarSource.recordedAt = item.occurredAt;
  }
  const gmailSource = work.projection.gmailSources.find(
    (candidate) => candidate.pendingPromotionId === pending.promotionId,
  );
  if (gmailSource !== undefined) {
    delete gmailSource.pendingPromotionId;
    gmailSource.episodeId = pending.proposal.episodeId;
    gmailSource.recordedAt = item.occurredAt;
  }
  let remembered = false;
  if (
    rememberForMatchingSource &&
    pending.standingRuleEligible &&
    pending.proposal.sourceMatcher !== undefined &&
    pending.proposal.sensitivity !== "highly_sensitive"
  ) {
    const policy = acceptDomain(
      work,
      "promotion-sharing-policy",
      item.occurredAt,
      { kind: "adult", adultId: item.senderAdultId },
      {
        kind: "policy.approved",
        policy: {
          policyId: PolicyIdSchema.parse(stableId("policy", promotionId, item.idempotencyKey)),
          householdId: item.householdId,
          version: work.aggregate.policyVersion + 1,
          status: "active",
          rule: {
            kind: "sharing",
            from: { kind: "personal", adultId: item.senderAdultId },
            to: { kind: "household" },
            sourceClass: pending.proposal.sourceClass,
            maximumSensitivity: pending.proposal.sensitivity,
            sourceMatcher: pending.proposal.sourceMatcher,
          },
          approvedByAdultId: item.senderAdultId,
          approvedAt: item.occurredAt,
        },
      } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
    );
    remembered = policy.receipt.disposition === "accepted";
  }
  work.projection.pendingPromotions.splice(index, 1);
  queuePromotedCommitment(work, "promotion-household", pending);
  queueMessage(
    work,
    "promotion-confirmed",
    personal(item.senderAdultId),
    "status",
    remembered
      ? pending.proposal.sourceMatcher?.source === "gmail"
        ? `Only the approved minimum household meaning was shared. Future ${pending.proposal.sourceClass} items from this exact sender in this exact connected inbox, up to ${pending.proposal.sensitivity} sensitivity, can use this rule without asking again.`
        : `Only the approved minimum household meaning was shared. Future ${pending.proposal.sourceClass} items from this exact connected calendar account, up to ${pending.proposal.sensitivity} sensitivity, can use this rule without asking again.`
      : rememberForMatchingSource
        ? "Only this approved minimum household meaning was shared once. Florence did not create a standing rule because this proposal did not meet every exact private-source safety requirement."
        : "Only the approved minimum household meaning was shared once.",
  );
  return "processed";
}

function declinePromotion(
  work: Work,
  item: ConversationInboxItem,
  promotionId: string,
): "processed" | "rejected" {
  if (item.channel.scope !== "personal") {
    return "rejected";
  }
  const index = work.projection.pendingPromotions.findIndex(
    (candidate) => candidate.promotionId === promotionId && candidate.ownerAdultId === item.senderAdultId,
  );
  if (index < 0) {
    return "rejected";
  }
  work.projection.pendingPromotions.splice(index, 1);
  const calendarSource = work.projection.calendarSources.find(
    (candidate) => candidate.pendingPromotionId === promotionId,
  );
  if (calendarSource !== undefined) {
    delete calendarSource.pendingPromotionId;
    calendarSource.recordedAt = item.occurredAt;
  }
  const gmailSource = work.projection.gmailSources.find(
    (candidate) => candidate.pendingPromotionId === promotionId,
  );
  if (gmailSource !== undefined) {
    delete gmailSource.pendingPromotionId;
    gmailSource.recordedAt = item.occurredAt;
  }
  queueMessage(
    work,
    "promotion-declined",
    personal(item.senderAdultId),
    "status",
    "The private item will not be shared with the household.",
  );
  return "processed";
}

function revokePolicy(
  work: Work,
  item: ConversationInboxItem,
  policyId: string,
  expectedPolicyVersion: number,
): "processed" | "rejected" {
  if (item.channel.scope !== "personal") return "rejected";
  const policy = work.aggregate.policies.find(
    (candidate) =>
      candidate.policyId === policyId &&
      candidate.status === "active" &&
      candidate.rule.kind === "sharing" &&
      candidate.rule.from.adultId === item.senderAdultId,
  );
  if (policy === undefined) return "rejected";
  const result = acceptDomain(
    work,
    "sharing-policy-revoked",
    item.occurredAt,
    { kind: "adult", adultId: item.senderAdultId },
    {
      kind: "policy.revoked",
      policyId: PolicyIdSchema.parse(policyId),
      expectedPolicyVersion,
    } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
  );
  if (result.receipt.disposition !== "accepted") return "rejected";
  queueMessage(
    work,
    "sharing-policy-revoked",
    personal(item.senderAdultId),
    "status",
    "The standing sharing rule is revoked. Matching private items will require review again.",
  );
  return "processed";
}

function sensitivityRank(value: "ordinary" | "sensitive" | "highly_sensitive"): number {
  return { ordinary: 0, sensitive: 1, highly_sensitive: 2 }[value];
}

type HouseholdPolicy = HouseholdAggregate["policies"][number];
type SharingPolicy = Omit<HouseholdPolicy, "rule"> & {
  rule: Extract<HouseholdPolicy["rule"], { kind: "sharing" }>;
};

function matchingSharingPolicy(
  aggregate: HouseholdAggregate,
  adultId: string,
  sourceClass: string,
  sensitivity: "ordinary" | "sensitive" | "highly_sensitive",
  sourceMatcher: PrivateSourceMatcher | undefined,
) {
  if (sourceMatcher === undefined || sensitivity === "highly_sensitive") return undefined;
  return aggregate.policies.find(
    (policy): policy is SharingPolicy =>
      policy.status === "active" &&
      policy.rule.kind === "sharing" &&
      policy.rule.from.adultId === adultId &&
      policy.rule.sourceClass === sourceClass &&
      sourceMatchersEqual(policy.rule.sourceMatcher, sourceMatcher) &&
      sensitivityRank(sensitivity) <= sensitivityRank(policy.rule.maximumSensitivity),
  );
}

function standingRuleEligible(input: {
  confidence: number;
  materialException: boolean;
  sensitivity: "ordinary" | "sensitive" | "highly_sensitive";
  sourceMatcher: PrivateSourceMatcher | undefined;
  outputValues: readonly string[];
  privateValues: readonly (string | null | undefined)[];
}): boolean {
  return (
    input.confidence >= 0.95 &&
    !input.materialException &&
    input.sensitivity !== "highly_sensitive" &&
    input.sourceMatcher !== undefined &&
    minimumMeaningPassesLeakGuard(input.outputValues, input.privateValues)
  );
}

function minimumPromotion(
  item: GmailInboxItem,
  triage: Extract<ReturnType<typeof GmailTriageResultSchema.parse>, { decision: "propose_family_episode" }>,
): PendingPromotion {
  const evidence = gmailEvidence(item);
  const promotionId = stableId(
    "promotion",
    item.householdId,
    item.ownerAdultId,
    item.messageRef,
    String(item.revision),
  );
  const sourceMatcher = gmailSourceMatcher(item);
  const episodeId = stableId("episode", promotionId);
  const proposal = EpisodeProposalSchema.parse({
    episodeId,
    type: "commitment",
    targetScope: { kind: "household" },
    title: triage.minimumHouseholdMeaning,
    requiredOutcome: triage.minimumHouseholdMeaning,
    ...(triage.proposedOwnerAdultId === undefined
      ? {}
      : { proposedOwnerAdultId: triage.proposedOwnerAdultId }),
    evidence: [evidence],
    sourceClass: triage.sourceClass,
    sensitivity: triage.sensitivity,
    ...(triage.temporalPlan === undefined
      ? {}
      : { temporalPlan: materializeInitialTemporalPlan(episodeId, triage.temporalPlan) }),
    ...(sourceMatcher === undefined ? {} : { sourceMatcher }),
  });
  return {
    promotionId,
    ownerAdultId: item.ownerAdultId,
    evidence,
    proposal,
    minimumHouseholdMeaning: triage.minimumHouseholdMeaning,
    standingRuleEligible: standingRuleEligible({
      confidence: triage.confidence,
      materialException: triage.materialException,
      sensitivity: triage.sensitivity,
      sourceMatcher,
      outputValues: [proposal.title, proposal.requiredOutcome],
      privateValues: [item.sender, senderDisplayName(item.sender), item.subject, item.snippet, item.bodyText],
    }),
    createdAt: item.occurredAt,
  };
}

function promoteByPolicy(
  work: Work,
  item: GmailInboxItem,
  pending: PendingPromotion,
  policy: NonNullable<ReturnType<typeof matchingSharingPolicy>>,
): "processed" | "rejected" {
  if (
    !pending.standingRuleEligible ||
    pending.proposal.sourceMatcher === undefined ||
    !sourceMatchersEqual(policy.rule.sourceMatcher, pending.proposal.sourceMatcher)
  ) {
    return "rejected";
  }
  const jobId = DomainWorkerJobIdSchema.parse(stableId("job", item.idempotencyKey, "gmail-triage"));
  const proposal = WorkerProposalSchema.parse({
    resultId: stableId("worker_result", item.idempotencyKey, "gmail-triage"),
    jobId,
    householdId: item.householdId,
    baseHouseholdVersion: work.aggregate.version,
    basePolicyVersion: work.aggregate.policyVersion,
    completedAt: item.occurredAt,
    confidence: work.projection.gmailTriage.at(-1)?.confidence ?? 0,
    evidence: [pending.evidence],
    episodeProposals: [
      {
        ...pending.proposal,
        promotionAuthority: {
          kind: "policy",
          policyId: policy.policyId,
          policyVersion: policy.version,
        },
      },
    ],
    messageProposals: [],
    actionProposals: [],
    memoryCandidates: [],
    policyCandidates: [],
    unresolvedQuestions: [],
    diagnostics: { warnings: [] },
  });
  const result = acceptDomain(work, "gmail-policy-promotion", item.occurredAt, { kind: "worker", jobId }, {
    kind: "worker.proposal_received",
    proposal,
  } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">);
  if (result.receipt.disposition === "accepted") {
    queuePromotedCommitment(work, "gmail-policy-household", pending);
  }
  return statusForDomain(result);
}

type GmailRevisionDisposition =
  | { kind: "fresh"; sourceKey: string; index: number }
  | { kind: "duplicate" | "metadata" | "stale" | "conflict"; sourceKey: string };

function auditGmailReconciliation(
  work: Work,
  item: GmailInboxItem | GmailMessageDeletedInboxItem,
  sourceKey: string,
  decision: string,
): void {
  auditClassification(
    work,
    "gmail_reconciled",
    decision,
    sourceKey,
    item.ownerAdultId,
    true,
    item.occurredAt,
  );
}

function reconcileGmailRevision(
  work: Work,
  item: GmailInboxItem | GmailMessageDeletedInboxItem,
): GmailRevisionDisposition {
  const sourceKey = gmailSourceKey(item);
  const index = work.projection.gmailSources.findIndex(
    (candidate) => candidate.sourceKey === sourceKey && candidate.ownerAdultId === item.ownerAdultId,
  );
  const prior = work.projection.gmailSources[index];
  if (prior === undefined) {
    return { kind: "fresh", sourceKey, index: work.projection.gmailSources.length };
  }
  if (item.revision < prior.latestRevision) {
    auditGmailReconciliation(work, item, sourceKey, "stale_revision");
    return { kind: "stale", sourceKey };
  }
  if (item.revision === prior.latestRevision) {
    const digest = gmailEvidence(item).contentDigest;
    const sameState =
      (item.kind === "gmail_message_deleted" && prior.status === "deleted") ||
      (item.kind === "gmail_message" && prior.status === "active" && prior.contentDigest === digest);
    auditGmailReconciliation(work, item, sourceKey, sameState ? "duplicate_revision" : "revision_conflict");
    return { kind: sameState ? "duplicate" : "conflict", sourceKey };
  }

  const currentDigest = gmailEvidence(item).contentDigest;
  if (
    item.kind === "gmail_message" &&
    prior.status === "active" &&
    currentDigest !== undefined &&
    prior.contentDigest === currentDigest
  ) {
    prior.latestRevision = item.revision;
    prior.recordedAt = item.occurredAt;
    auditGmailReconciliation(work, item, sourceKey, "metadata_only_revision");
    return { kind: "metadata", sourceKey };
  }

  if (prior.pendingPromotionId !== undefined) {
    const pendingIndex = work.projection.pendingPromotions.findIndex(
      (candidate) =>
        candidate.promotionId === prior.pendingPromotionId && candidate.ownerAdultId === item.ownerAdultId,
    );
    if (pendingIndex >= 0) {
      work.projection.pendingPromotions.splice(pendingIndex, 1);
      queueMessage(
        work,
        "gmail-promotion-invalidated",
        personal(item.ownerAdultId),
        "status",
        "That private email changed or was removed, so its earlier sharing proposal is no longer pending.",
      );
    }
  }

  if (prior.episodeId !== undefined) {
    const episode = work.aggregate.episodes.find((candidate) => candidate.episodeId === prior.episodeId);
    if (episode === undefined) {
      throw new Error(`Gmail source references an unknown episode: ${prior.episodeId}`);
    }
    if (!["completed", "dismissed", "superseded", "failed"].includes(episode.state)) {
      const result = acceptDomain(
        work,
        `gmail-source-superseded:${sourceKey}:${item.revision}`,
        item.occurredAt,
        { kind: "source_adapter", source: "gmail" },
        {
          kind: "episode.source_superseded",
          episodeId: episode.episodeId,
          baseEpisodeVersion: episode.version,
          supersedingEvidence: gmailEvidence(item),
        } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
      );
      if (result.receipt.disposition !== "accepted") {
        throw new Error(`Gmail source could not supersede episode: ${result.receipt.reason}`);
      }
    }
  }

  return { kind: "fresh", sourceKey, index };
}

function saveGmailSource(
  work: Work,
  index: number,
  record: ApplicationProjection["gmailSources"][number],
): void {
  if (index === work.projection.gmailSources.length) {
    work.projection.gmailSources.push(record);
    return;
  }
  work.projection.gmailSources[index] = record;
}

function gmailRevisionOutcome(disposition: GmailRevisionDisposition): {
  status: "processed" | "rejected";
  classification: string;
} | null {
  switch (disposition.kind) {
    case "fresh":
      return null;
    case "duplicate":
      return { status: "processed", classification: "gmail:duplicate_revision" };
    case "metadata":
      return { status: "processed", classification: "gmail:metadata_revision" };
    case "stale":
      return { status: "processed", classification: "gmail:stale_revision" };
    case "conflict":
      return { status: "rejected", classification: "gmail:revision_conflict" };
  }
}

function processGmailDeleted(
  work: Work,
  item: GmailMessageDeletedInboxItem,
): { status: "processed" | "rejected"; classification: string } {
  if (!work.aggregate.verifiedAdultIds.includes(item.ownerAdultId)) {
    return { status: "rejected", classification: "gmail_unknown_owner" };
  }
  const revision = reconcileGmailRevision(work, item);
  if (revision.kind !== "fresh") {
    return gmailRevisionOutcome(revision) as Exclude<ReturnType<typeof gmailRevisionOutcome>, null>;
  }
  saveGmailSource(work, revision.index, {
    sourceKey: revision.sourceKey,
    ownerAdultId: item.ownerAdultId,
    latestRevision: item.revision,
    status: "deleted",
    recordedAt: item.occurredAt,
  });
  auditGmailReconciliation(work, item, revision.sourceKey, "deleted");
  return { status: "processed", classification: "gmail:deleted" };
}

async function processGmail(
  work: Work,
  item: GmailInboxItem,
  dependencies: FlorenceApplicationDependencies,
): Promise<{ status: "processed" | "rejected"; classification: string }> {
  if (!work.aggregate.verifiedAdultIds.includes(item.ownerAdultId)) {
    return { status: "rejected", classification: "gmail_unknown_owner" };
  }
  const revision = reconcileGmailRevision(work, item);
  if (revision.kind !== "fresh") {
    return gmailRevisionOutcome(revision) as Exclude<ReturnType<typeof gmailRevisionOutcome>, null>;
  }
  const evidence = gmailEvidence(item);
  if (evidence.contentDigest === undefined) {
    throw new Error("An active Gmail source must carry a content digest");
  }
  saveGmailSource(work, revision.index, {
    sourceKey: revision.sourceKey,
    ownerAdultId: item.ownerAdultId,
    latestRevision: item.revision,
    status: "active",
    contentDigest: evidence.contentDigest,
    recordedAt: item.occurredAt,
  });
  const sourceMatcher = gmailSourceMatcher(item);
  const rules = work.aggregate.policies.flatMap((policy) =>
    policy.status === "active" &&
    policy.rule.kind === "sharing" &&
    policy.rule.from.adultId === item.ownerAdultId &&
    sourceMatcher !== undefined &&
    sourceMatchersEqual(policy.rule.sourceMatcher, sourceMatcher)
      ? [
          {
            policyId: policy.policyId,
            policyVersion: policy.version,
            sourceClass: policy.rule.sourceClass,
            maximumSensitivity: policy.rule.maximumSensitivity,
          },
        ]
      : [],
  );
  const triage = GmailTriageResultSchema.parse(
    await dependencies.interpreter.triageGmail(item, {
      currentTime: item.occurredAt,
      householdTimeZone: work.aggregate.timeZone,
      confirmedRoutineAnchors: work.aggregate.routineAnchors,
      activeMemories: activeMemoryContext(work.aggregate, personal(item.ownerAdultId), item.occurredAt),
      activeSharingRules: rules,
    }),
  );
  work.projection.gmailTriage.push({
    messageRef: item.messageRef,
    ownerAdultId: item.ownerAdultId,
    decision: triage.decision,
    sourceClass: triage.sourceClass,
    sensitivity: triage.sensitivity,
    familyImpact: triage.familyImpact,
    confidence: triage.confidence,
    recordedAt: item.occurredAt,
  });
  auditClassification(
    work,
    "gmail_triaged",
    triage.decision,
    item.messageRef,
    item.ownerAdultId,
    true,
    item.occurredAt,
  );

  switch (triage.decision) {
    case "ignore":
    case "retain_private":
      return { status: "processed", classification: `gmail:${triage.decision}` };
    case "private_review":
      queuePrivateReviewItem(work, {
        source: "gmail",
        sourceKey: revision.sourceKey,
        revision: item.revision,
        adultId: item.ownerAdultId,
        summary: triage.privateSummary,
        observedAt: item.occurredAt,
      });
      return { status: "processed", classification: "gmail:private_review" };
    case "private_interrupt":
      queueMessage(
        work,
        "gmail-private-interrupt",
        personal(item.ownerAdultId),
        "private_interrupt",
        triage.privateSummary,
      );
      return { status: "processed", classification: "gmail:private_interrupt" };
    case "propose_family_episode": {
      if (!triage.familyImpact) {
        return { status: "rejected", classification: "gmail:family_impact_missing" };
      }
      const pending = minimumPromotion(item, triage);
      const policy = matchingSharingPolicy(
        work.aggregate,
        item.ownerAdultId,
        triage.sourceClass,
        triage.sensitivity,
        sourceMatcher,
      );
      if (policy !== undefined && pending.standingRuleEligible) {
        const status = promoteByPolicy(work, item, pending, policy);
        if (status === "processed") {
          const source = work.projection.gmailSources[revision.index];
          if (source === undefined) throw new Error("Gmail source disappeared during promotion");
          source.episodeId = pending.proposal.episodeId;
          source.recordedAt = item.occurredAt;
        }
        return {
          status,
          classification: "gmail:policy_promotion",
        };
      }
      work.projection.pendingPromotions.push(pending);
      const source = work.projection.gmailSources[revision.index];
      if (source === undefined) throw new Error("Gmail source disappeared during private review");
      source.pendingPromotionId = pending.promotionId;
      source.recordedAt = item.occurredAt;
      queueMessage(
        work,
        "gmail-promotion-request",
        personal(item.ownerAdultId),
        "promotion_request",
        `${triage.privateSummary} Share only this household meaning: “${triage.minimumHouseholdMeaning}”? Reply “share once ${pending.promotionId}” or, only if you want a standing rule for this exact sender in this exact connected inbox and matching ${triage.sourceClass} items, “always share ${pending.promotionId}”.`,
        { kind: "promotion_decision", promotionId: pending.promotionId },
      );
      return { status: "processed", classification: "gmail:promotion_pending" };
    }
  }
}

function calendarMinimumPromotion(
  item: CalendarEventInboxItem,
  triage: Extract<
    ReturnType<typeof CalendarTriageResultSchema.parse>,
    { decision: "propose_family_episode" }
  >,
  routineAnchors: HouseholdAggregate["routineAnchors"],
): PendingPromotion {
  const sourceKey = calendarSourceKey(item);
  const evidence = calendarEvidence(item);
  const promotionId = stableId("promotion", sourceKey, String(item.revision));
  const episodeId = stableId("episode", promotionId);
  const temporalPlan = calendarTimingPlan(item, episodeId, routineAnchors);
  const sourceMatcher = calendarSourceMatcher(item);
  const proposal = EpisodeProposalSchema.parse({
    episodeId,
    type: "commitment",
    targetScope: { kind: "household" },
    title: triage.minimumHouseholdMeaning,
    requiredOutcome: triage.minimumRequiredOutcome,
    evidence: [evidence],
    sourceClass: triage.sourceClass,
    sensitivity: triage.sensitivity,
    temporalPlan,
    sourceMatcher,
  });
  return {
    promotionId,
    ownerAdultId: item.ownerAdultId,
    evidence,
    proposal,
    minimumHouseholdMeaning: triage.minimumHouseholdMeaning,
    standingRuleEligible: standingRuleEligible({
      confidence: triage.confidence,
      materialException: triage.materialException,
      sensitivity: triage.sensitivity,
      sourceMatcher,
      outputValues: [proposal.title, proposal.requiredOutcome],
      privateValues: [item.title, item.description, item.location],
    }),
    createdAt: item.occurredAt,
  };
}

const CALENDAR_USEFUL_LEAD_MINUTES = 8 * 24 * 60;
const CALENDAR_FINAL_BUFFER_MINUTES = 30;
const CALENDAR_MINIMUM_TIMER_DELAY_MINUTES = 5;

function calendarTimingPlan(
  item: CalendarEventInboxItem,
  episodeId: string,
  routineAnchors: HouseholdAggregate["routineAnchors"],
) {
  const eventAt = Temporal.Instant.from(item.startsAt);
  const observedAt = Temporal.Instant.from(item.occurredAt);
  const eventZoned = eventAt.toZonedDateTimeISO(item.timeZone);
  const eventDate = eventZoned.toPlainDate();

  const morningAnchor = item.allDay
    ? routineAnchors
        .filter(
          (anchor) =>
            anchor.timeZone === item.timeZone &&
            anchor.daysOfWeek.includes(eventDate.dayOfWeek) &&
            anchor.localTime >= "05:00" &&
            anchor.localTime <= "12:00",
        )
        .sort((left, right) => left.localTime.localeCompare(right.localTime))[0]
    : undefined;
  const deadlineMoment = item.allDay
    ? morningAnchor === undefined
      ? ({
          kind: "local" as const,
          date: eventDate.toString(),
          time: "08:00",
          timeZone: item.timeZone,
          disambiguation: "compatible" as const,
        } as const)
      : ({
          kind: "routine_anchor" as const,
          anchorId: morningAnchor.anchorId,
          date: eventDate.toString(),
          offsetMinutes: 0,
          disambiguation: "compatible" as const,
        } as const)
    : undefined;
  const deadlineAt = item.allDay
    ? Temporal.PlainDateTime.from(`${eventDate.toString()}T${morningAnchor?.localTime ?? "08:00"}`)
        .toZonedDateTime(morningAnchor?.timeZone ?? item.timeZone, {
          disambiguation: "compatible",
        })
        .toInstant()
    : eventAt;
  const earliestUsefulAt = deadlineAt.subtract({ minutes: CALENDAR_USEFUL_LEAD_MINUTES });
  const lastResponsibleAt = deadlineAt.subtract({ minutes: CALENDAR_FINAL_BUFFER_MINUTES });
  const earliestUsefulLocal = earliestUsefulAt.toZonedDateTimeISO(item.timeZone);
  const lastResponsibleLocal = lastResponsibleAt.toZonedDateTimeISO(item.timeZone);
  const earliestUsefulMoment =
    item.allDay && morningAnchor !== undefined
      ? {
          kind: "routine_anchor" as const,
          anchorId: morningAnchor.anchorId,
          date: eventDate.toString(),
          offsetMinutes: -CALENDAR_USEFUL_LEAD_MINUTES,
          disambiguation: "compatible" as const,
        }
      : item.allDay
        ? {
            kind: "local" as const,
            date: earliestUsefulLocal.toPlainDate().toString(),
            time: `${String(earliestUsefulLocal.hour).padStart(2, "0")}:${String(
              earliestUsefulLocal.minute,
            ).padStart(2, "0")}`,
            timeZone: item.timeZone,
            disambiguation: "compatible" as const,
          }
        : { kind: "instant" as const, at: earliestUsefulAt.toString() };
  const lastResponsibleMoment =
    item.allDay && morningAnchor !== undefined
      ? {
          kind: "routine_anchor" as const,
          anchorId: morningAnchor.anchorId,
          date: eventDate.toString(),
          offsetMinutes: -CALENDAR_FINAL_BUFFER_MINUTES,
          disambiguation: "compatible" as const,
        }
      : item.allDay
        ? {
            kind: "local" as const,
            date: lastResponsibleLocal.toPlainDate().toString(),
            time: `${String(lastResponsibleLocal.hour).padStart(2, "0")}:${String(
              lastResponsibleLocal.minute,
            ).padStart(2, "0")}`,
            timeZone: item.timeZone,
            disambiguation: "compatible" as const,
          }
        : { kind: "instant" as const, at: lastResponsibleAt.toString() };

  const previousDate = eventDate.subtract({ days: 1 });
  const previousEvening = Temporal.PlainDateTime.from(`${previousDate.toString()}T18:00`)
    .toZonedDateTime(item.timeZone, { disambiguation: "compatible" })
    .toInstant();
  const finalLeadMinutes = item.allDay ? 60 : eventZoned.hour < 9 ? 60 : 120;
  const finalReminderAt = deadlineAt.subtract({ minutes: finalLeadMinutes });
  const finalReminderLocal = finalReminderAt.toZonedDateTimeISO(item.timeZone);
  const includeFinalReminder = item.allDay || finalReminderLocal.hour >= 6;
  const candidates = [
    ...(item.allDay || eventZoned.hour < 9
      ? [
          {
            key: "previous_evening",
            at: previousEvening,
            moment: {
              kind: "local" as const,
              date: previousDate.toString(),
              time: "18:00",
              timeZone: item.timeZone,
              disambiguation: "compatible" as const,
            },
          },
        ]
      : [
          {
            key: "day_before",
            at: eventZoned.subtract({ days: 1 }).toInstant(),
            moment: {
              kind: "instant" as const,
              at: eventZoned.subtract({ days: 1 }).toInstant().toString(),
            },
          },
        ]),
    ...(includeFinalReminder
      ? [
          {
            key: item.allDay ? "before_family_day" : "final_preparation",
            at: finalReminderAt,
            moment:
              item.allDay && morningAnchor !== undefined
                ? {
                    kind: "routine_anchor" as const,
                    anchorId: morningAnchor.anchorId,
                    date: eventDate.toString(),
                    offsetMinutes: -finalLeadMinutes,
                    disambiguation: "compatible" as const,
                  }
                : {
                    kind: "instant" as const,
                    at: finalReminderAt.toString(),
                  },
          },
        ]
      : []),
  ].filter(
    (candidate) =>
      Temporal.Instant.compare(candidate.at, observedAt) > 0 &&
      Temporal.Instant.compare(candidate.at, earliestUsefulAt) >= 0 &&
      Temporal.Instant.compare(candidate.at, lastResponsibleAt) <= 0,
  );

  if (candidates.length === 0) {
    const catchUpAt = observedAt.add({ minutes: CALENDAR_MINIMUM_TIMER_DELAY_MINUTES });
    if (
      Temporal.Instant.compare(catchUpAt, earliestUsefulAt) >= 0 &&
      Temporal.Instant.compare(catchUpAt, lastResponsibleAt) <= 0
    ) {
      candidates.push({
        key: "next_safe_time",
        at: catchUpAt,
        moment: { kind: "instant", at: catchUpAt.toString() },
      });
    }
  }

  return materializeInitialTemporalPlan(episodeId, {
    timeZone: item.timeZone,
    ...(item.allDay
      ? { deadline: deadlineMoment }
      : { event: { kind: "instant" as const, at: eventAt.toString() } }),
    earliestUseful: earliestUsefulMoment,
    lastResponsible: lastResponsibleMoment,
    usefulLeadMinutes: CALENDAR_USEFUL_LEAD_MINUTES,
    preparationMinutes: 0,
    finalBufferMinutes: CALENDAR_FINAL_BUFFER_MINUTES,
    triggers: candidates.map((candidate) => ({
      kind: "reminder" as const,
      at: candidate.moment,
    })),
  });
}

function promoteCalendarByPolicy(
  work: Work,
  item: CalendarEventInboxItem,
  pending: PendingPromotion,
  policy: NonNullable<ReturnType<typeof matchingSharingPolicy>>,
  confidence: number,
): "processed" | "rejected" {
  if (
    !pending.standingRuleEligible ||
    pending.proposal.sourceMatcher === undefined ||
    !sourceMatchersEqual(policy.rule.sourceMatcher, pending.proposal.sourceMatcher)
  ) {
    return "rejected";
  }
  const jobId = DomainWorkerJobIdSchema.parse(stableId("job", item.idempotencyKey, "calendar-triage"));
  const proposal = WorkerProposalSchema.parse({
    resultId: stableId("worker_result", item.idempotencyKey, "calendar-triage"),
    jobId,
    householdId: item.householdId,
    baseHouseholdVersion: work.aggregate.version,
    basePolicyVersion: work.aggregate.policyVersion,
    completedAt: item.occurredAt,
    confidence,
    evidence: [pending.evidence],
    episodeProposals: [
      {
        ...pending.proposal,
        promotionAuthority: {
          kind: "policy",
          policyId: policy.policyId,
          policyVersion: policy.version,
        },
      },
    ],
    messageProposals: [],
    actionProposals: [],
    memoryCandidates: [],
    policyCandidates: [],
    unresolvedQuestions: [],
    diagnostics: { warnings: [] },
  });
  const result = acceptDomain(work, "calendar-policy-promotion", item.occurredAt, { kind: "worker", jobId }, {
    kind: "worker.proposal_received",
    proposal,
  } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">);
  if (result.receipt.disposition === "accepted") {
    queuePromotedCommitment(work, "calendar-policy-household", pending);
  }
  return statusForDomain(result);
}

type CalendarRevisionDisposition =
  | { kind: "fresh"; sourceKey: string; index: number }
  | { kind: "duplicate" | "stale" | "conflict"; sourceKey: string };

function auditCalendarReconciliation(
  work: Work,
  item: CalendarEventInboxItem | CalendarEventDeletedInboxItem,
  sourceKey: string,
  decision: string,
): void {
  auditClassification(
    work,
    "calendar_reconciled",
    decision,
    sourceKey,
    item.ownerAdultId,
    true,
    item.occurredAt,
  );
}

function reconcileCalendarRevision(
  work: Work,
  item: CalendarEventInboxItem | CalendarEventDeletedInboxItem,
): CalendarRevisionDisposition {
  const sourceKey = calendarSourceKey(item);
  const index = work.projection.calendarSources.findIndex(
    (candidate) => candidate.sourceKey === sourceKey && candidate.ownerAdultId === item.ownerAdultId,
  );
  const prior = work.projection.calendarSources[index];
  if (prior === undefined) {
    return { kind: "fresh", sourceKey, index: work.projection.calendarSources.length };
  }
  if (item.revision < prior.latestRevision) {
    auditCalendarReconciliation(work, item, sourceKey, "stale_revision");
    return { kind: "stale", sourceKey };
  }
  if (item.revision === prior.latestRevision) {
    const sameState =
      (item.kind === "calendar_event_deleted" && prior.status === "deleted") ||
      (item.kind === "calendar_event" &&
        prior.status === "active" &&
        prior.contentDigest === item.contentDigest);
    auditCalendarReconciliation(
      work,
      item,
      sourceKey,
      sameState ? "duplicate_revision" : "revision_conflict",
    );
    return { kind: sameState ? "duplicate" : "conflict", sourceKey };
  }

  if (prior.pendingPromotionId !== undefined) {
    const pendingIndex = work.projection.pendingPromotions.findIndex(
      (candidate) =>
        candidate.promotionId === prior.pendingPromotionId && candidate.ownerAdultId === item.ownerAdultId,
    );
    if (pendingIndex >= 0) {
      work.projection.pendingPromotions.splice(pendingIndex, 1);
      queueMessage(
        work,
        "calendar-promotion-invalidated",
        personal(item.ownerAdultId),
        "status",
        "A private Calendar item changed, so its earlier sharing proposal is no longer pending.",
      );
    }
  }

  if (prior.episodeId !== undefined) {
    const episode = work.aggregate.episodes.find((candidate) => candidate.episodeId === prior.episodeId);
    if (episode === undefined) {
      throw new Error(`Calendar source references an unknown episode: ${prior.episodeId}`);
    }
    if (!["completed", "dismissed", "superseded", "failed"].includes(episode.state)) {
      const result = acceptDomain(
        work,
        `calendar-source-superseded:${sourceKey}:${item.revision}`,
        item.occurredAt,
        { kind: "source_adapter", source: "calendar" },
        {
          kind: "episode.source_superseded",
          episodeId: episode.episodeId,
          baseEpisodeVersion: episode.version,
          supersedingEvidence: calendarEvidence(item),
        } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
      );
      if (result.receipt.disposition !== "accepted") {
        throw new Error(`Calendar source could not supersede episode: ${result.receipt.reason}`);
      }
    }
  }

  return { kind: "fresh", sourceKey, index };
}

function saveCalendarSource(
  work: Work,
  index: number,
  record: ApplicationProjection["calendarSources"][number],
): void {
  if (index === work.projection.calendarSources.length) {
    work.projection.calendarSources.push(record);
    return;
  }
  work.projection.calendarSources[index] = record;
}

function calendarRevisionOutcome(disposition: CalendarRevisionDisposition): {
  status: "processed" | "rejected";
  classification: string;
} | null {
  switch (disposition.kind) {
    case "fresh":
      return null;
    case "duplicate":
      return { status: "processed", classification: "calendar:duplicate_revision" };
    case "stale":
      return { status: "processed", classification: "calendar:stale_revision" };
    case "conflict":
      return { status: "rejected", classification: "calendar:revision_conflict" };
  }
}

function processCalendarDeleted(
  work: Work,
  item: CalendarEventDeletedInboxItem,
): { status: "processed" | "rejected"; classification: string } {
  if (!work.aggregate.verifiedAdultIds.includes(item.ownerAdultId)) {
    return { status: "rejected", classification: "calendar_unknown_owner" };
  }
  const revision = reconcileCalendarRevision(work, item);
  if (revision.kind !== "fresh") {
    return calendarRevisionOutcome(revision) as Exclude<ReturnType<typeof calendarRevisionOutcome>, null>;
  }
  saveCalendarSource(work, revision.index, {
    sourceKey: revision.sourceKey,
    ownerAdultId: item.ownerAdultId,
    latestRevision: item.revision,
    status: "deleted",
    recordedAt: item.occurredAt,
  });
  auditCalendarReconciliation(work, item, revision.sourceKey, "deleted");
  return { status: "processed", classification: "calendar:deleted" };
}

async function processCalendar(
  work: Work,
  item: CalendarEventInboxItem,
  dependencies: FlorenceApplicationDependencies,
): Promise<{ status: "processed" | "rejected"; classification: string }> {
  if (!work.aggregate.verifiedAdultIds.includes(item.ownerAdultId)) {
    return { status: "rejected", classification: "calendar_unknown_owner" };
  }
  const revision = reconcileCalendarRevision(work, item);
  if (revision.kind !== "fresh") {
    return calendarRevisionOutcome(revision) as Exclude<ReturnType<typeof calendarRevisionOutcome>, null>;
  }

  const sourceMatcher = calendarSourceMatcher(item);
  const rules = work.aggregate.policies.flatMap((policy) =>
    policy.status === "active" &&
    policy.rule.kind === "sharing" &&
    policy.rule.from.adultId === item.ownerAdultId &&
    sourceMatchersEqual(policy.rule.sourceMatcher, sourceMatcher)
      ? [
          {
            policyId: policy.policyId,
            policyVersion: policy.version,
            sourceClass: policy.rule.sourceClass,
            maximumSensitivity: policy.rule.maximumSensitivity,
          },
        ]
      : [],
  );
  const triage = CalendarTriageResultSchema.parse(
    await dependencies.interpreter.triageCalendar(item, {
      currentTime: item.occurredAt,
      householdTimeZone: work.aggregate.timeZone,
      activeMemories: activeMemoryContext(work.aggregate, personal(item.ownerAdultId), item.occurredAt),
      activeSharingRules: rules,
    }),
  );
  work.projection.calendarTriage.push({
    sourceKey: revision.sourceKey,
    ownerAdultId: item.ownerAdultId,
    revision: item.revision,
    decision: triage.decision,
    sourceClass: triage.sourceClass,
    sensitivity: triage.sensitivity,
    familyImpact: triage.familyImpact,
    confidence: triage.confidence,
    recordedAt: item.occurredAt,
  });
  auditClassification(
    work,
    "calendar_triaged",
    triage.decision,
    revision.sourceKey,
    item.ownerAdultId,
    true,
    item.occurredAt,
  );

  const sourceRecord = {
    sourceKey: revision.sourceKey,
    ownerAdultId: item.ownerAdultId,
    latestRevision: item.revision,
    status: "active" as const,
    contentDigest: item.contentDigest,
    recordedAt: item.occurredAt,
  };
  switch (triage.decision) {
    case "ignore":
    case "retain_private":
      saveCalendarSource(work, revision.index, sourceRecord);
      return { status: "processed", classification: `calendar:${triage.decision}` };
    case "private_review":
      saveCalendarSource(work, revision.index, sourceRecord);
      queuePrivateReviewItem(work, {
        source: "calendar",
        sourceKey: revision.sourceKey,
        revision: item.revision,
        adultId: item.ownerAdultId,
        summary: triage.privateSummary,
        observedAt: item.occurredAt,
      });
      return { status: "processed", classification: "calendar:private_review" };
    case "private_interrupt":
      saveCalendarSource(work, revision.index, sourceRecord);
      queueMessage(
        work,
        "calendar-private-interrupt",
        personal(item.ownerAdultId),
        "private_interrupt",
        triage.privateSummary,
      );
      return { status: "processed", classification: "calendar:private_interrupt" };
    case "propose_family_episode": {
      if (!triage.familyImpact) {
        saveCalendarSource(work, revision.index, sourceRecord);
        return { status: "rejected", classification: "calendar:family_impact_missing" };
      }
      const pending = calendarMinimumPromotion(item, triage, work.aggregate.routineAnchors);
      const policy = matchingSharingPolicy(
        work.aggregate,
        item.ownerAdultId,
        triage.sourceClass,
        triage.sensitivity,
        sourceMatcher,
      );
      if (policy !== undefined && pending.standingRuleEligible) {
        const status = promoteCalendarByPolicy(work, item, pending, policy, triage.confidence);
        if (status !== "processed") {
          throw new Error("Calendar policy promotion was rejected by the household domain");
        }
        saveCalendarSource(work, revision.index, {
          ...sourceRecord,
          episodeId: pending.proposal.episodeId,
        });
        return { status: "processed", classification: "calendar:policy_promotion" };
      }
      work.projection.pendingPromotions.push(pending);
      saveCalendarSource(work, revision.index, {
        ...sourceRecord,
        pendingPromotionId: pending.promotionId,
      });
      queueMessage(
        work,
        "calendar-promotion-request",
        personal(item.ownerAdultId),
        "promotion_request",
        `${triage.privateSummary} Share only this household meaning: “${triage.minimumHouseholdMeaning}” with this required outcome: “${triage.minimumRequiredOutcome}”? The event's start time will anchor follow-through; its raw title, description, and location stay private. Reply “share once ${pending.promotionId}” or, only if you want a standing rule for this exact connected calendar account and matching ${triage.sourceClass} items, “always share ${pending.promotionId}”.`,
        { kind: "promotion_decision", promotionId: pending.promotionId },
      );
      return { status: "processed", classification: "calendar:promotion_pending" };
    }
  }
}

async function processConversation(
  work: Work,
  item: ConversationInboxItem,
  dependencies: FlorenceApplicationDependencies,
  routes: WorkerRoutes,
): Promise<{ status: "processed" | "rejected"; classification: string }> {
  if (!work.aggregate.verifiedAdultIds.includes(item.senderAdultId)) {
    return { status: "rejected", classification: "conversation_unknown_adult" };
  }
  if (presentRequiredConsentDisclosure(work, item)) {
    return { status: "processed", classification: "onboarding:consent_disclosed" };
  }
  if (
    item.text.trim().length === 0 &&
    item.attachmentContents.length > 0 &&
    item.attachmentContents.every((attachment) => attachment.kind === "unavailable")
  ) {
    queueMessage(
      work,
      "attachment-unavailable",
      targetScope(item),
      "status",
      "I couldn't securely read that attachment. Please resend it as a PDF, common image, or text document, with a short note about what you want me to do.",
    );
    return { status: "processed", classification: "conversation:attachment_unavailable" };
  }
  const visibleEpisodes = work.aggregate.episodes.flatMap((episode) => {
    const visible =
      episode.scope.kind === "household" ||
      (item.channel.scope === "personal" &&
        episode.scope.kind === "personal" &&
        episode.scope.adultId === item.senderAdultId);
    if (!visible) {
      return [];
    }
    return [
      {
        episodeId: episode.episodeId,
        type: episode.type,
        state: episode.state,
        title: episode.title,
        ...(episode.owner.status === "unassigned" ? {} : { ownerAdultId: episode.owner.adultId }),
        version: episode.version,
        ...(episode.temporalPlan === undefined ? {} : { temporalPlan: episode.temporalPlan.definition }),
      },
    ];
  });
  const pendingPromotionIds =
    item.channel.scope === "personal"
      ? work.projection.pendingPromotions
          .filter((candidate) => candidate.ownerAdultId === item.senderAdultId)
          .map((candidate) => candidate.promotionId)
      : [];
  const activePolicies =
    item.channel.scope === "personal"
      ? work.aggregate.policies.flatMap((policy) => {
          if (
            policy.status !== "active" ||
            policy.rule.kind !== "sharing" ||
            policy.rule.from.adultId !== item.senderAdultId
          ) {
            return [];
          }
          return [
            {
              policyId: policy.policyId,
              policyVersion: policy.version,
              kind: policy.rule.kind,
              description:
                policy.rule.sourceMatcher.source === "gmail"
                  ? `Share minimum household meaning for ${policy.rule.sourceClass} from one exact sender in one exact connected inbox through ${policy.rule.maximumSensitivity} sensitivity.`
                  : `Share minimum household meaning for ${policy.rule.sourceClass} from one exact connected calendar account through ${policy.rule.maximumSensitivity} sensitivity.`,
            },
          ];
        })
      : [];
  const pendingCalendarActions = work.aggregate.pendingActions.flatMap((pending) => {
    if (
      pending.state !== "awaiting_approval" ||
      pending.action.kind !== "calendar_update" ||
      item.channel.scope !== "household" ||
      Temporal.Instant.compare(Temporal.Instant.from(pending.expiresAt), item.occurredAt) <= 0
    ) {
      return [];
    }
    return [
      {
        actionId: pending.action.actionId,
        title: pending.action.title,
        startsAt: pending.action.startsAt,
        endsAt: pending.action.endsAt,
        timeZone: pending.action.timeZone,
        hasConflict: pending.action.hasConflict,
        expiresAt: pending.expiresAt,
      },
    ];
  });
  const classification = ConversationClassificationSchema.parse(
    await dependencies.interpreter.interpretConversation(item, {
      currentTime: item.occurredAt,
      householdTimeZone: work.aggregate.timeZone,
      onboarding: work.projection.onboarding,
      sharedProfile: work.projection.sharedProfile,
      confirmedRoutineAnchors: work.aggregate.routineAnchors,
      activeMemories: activeMemoryContext(work.aggregate, targetScope(item), item.occurredAt),
      pendingMemoryCandidates: pendingMemoryContext(work.aggregate, targetScope(item)),
      openEpisodes: visibleEpisodes,
      pendingPromotionIds,
      activePolicies,
      pendingCalendarActions,
    }),
  );
  auditClassification(
    work,
    "conversation_classified",
    classification.intent,
    item.messageRef,
    item.senderAdultId,
    item.channel.scope === "personal",
    item.occurredAt,
  );
  if (work.projection.onboarding.phase !== "active" && classification.intent !== "onboarding") {
    queueMessage(
      work,
      "onboarding-required",
      targetScope(item),
      "onboarding",
      onboardingRecoveryBody(work.projection.onboarding),
    );
    return { status: "rejected", classification: "onboarding_required" };
  }
  return {
    status: await processActiveConversation(work, item, classification, routes, dependencies),
    classification: `conversation:${classification.intent}`,
  };
}

function commandEvidence(command: WorkerCommand) {
  switch (command.kind) {
    case "episode.propose":
      return command.payload.evidence;
    case "message.propose":
      return command.payload.evidence;
    case "action.propose":
      return command.payload.action.evidence;
    case "memory.candidate":
      return command.payload.evidence;
    case "policy.candidate":
      return [];
  }
}

function commandScope(command: WorkerCommand): DurableScope | undefined {
  switch (command.kind) {
    case "episode.propose":
      return command.payload.targetScope;
    case "message.propose":
      return command.payload.targetScope;
    case "action.propose":
      return command.payload.action.requestedFor;
    case "memory.candidate":
      return command.payload.scope;
    case "policy.candidate":
      return undefined;
  }
}

function scopeFitsJob(scope: DurableScope | undefined, job: WorkerJob): boolean {
  if (scope === undefined) {
    return true;
  }
  if (job.scopeGrant.visibility === "household") {
    return scope.kind === "household";
  }
  return scope.kind === "personal" && scope.adultId === job.scopeGrant.adultId;
}

function workerIdentityMatches(result: WorkerResult, job: WorkerJob): boolean {
  return (
    result.jobId === job.jobId &&
    result.attemptId === job.attemptId &&
    result.householdId === job.householdId &&
    result.baseHouseholdVersion === job.baseHouseholdVersion &&
    result.policyVersion === job.policyVersion &&
    result.modelRouteId === job.modelRouteId &&
    result.modelCapabilityProfile === job.modelCapabilityProfile &&
    result.outputContractRef === job.outputContractRef &&
    result.purpose === job.scopeGrant.purpose
  );
}

function parseWorkerCommands(result: WorkerResult, job: WorkerJob): WorkerCommand[] | null {
  const commands: WorkerCommand[] = [];
  const evidenceRefs = new Set(job.evidenceRefs);
  for (const proposed of result.proposedCommands) {
    const parsed = WorkerCommandSchema.safeParse(proposed);
    if (!parsed.success || !scopeFitsJob(commandScope(parsed.data), job)) {
      return null;
    }
    if (commandEvidence(parsed.data).some((evidence) => !evidenceRefs.has(evidence.evidenceId))) {
      return null;
    }
    commands.push(parsed.data);
  }
  return commands;
}

function workerProposalFromCommands(
  result: WorkerResult,
  commands: readonly WorkerCommand[],
  completedAt: string,
  baseHouseholdVersion: number,
  basePolicyVersion: number,
) {
  const evidence = unique(commands.flatMap(commandEvidence).map((item) => JSON.stringify(item))).map((item) =>
    EvidenceRefSchema.parse(JSON.parse(item)),
  );
  return WorkerProposalSchema.parse({
    resultId: stableId("worker_result", result.jobId, result.attemptId),
    jobId: DomainWorkerJobIdSchema.parse(result.jobId),
    householdId: result.householdId,
    baseHouseholdVersion,
    basePolicyVersion,
    completedAt,
    confidence: result.confidence,
    evidence,
    episodeProposals: commands.flatMap((command) =>
      command.kind === "episode.propose" ? [command.payload] : [],
    ),
    messageProposals: commands.flatMap((command) =>
      command.kind === "message.propose" ? [command.payload] : [],
    ),
    actionProposals: commands.flatMap((command) =>
      command.kind === "action.propose" ? [command.payload] : [],
    ),
    memoryCandidates: commands.flatMap((command) =>
      command.kind === "memory.candidate" ? [command.payload] : [],
    ),
    policyCandidates: commands.flatMap((command) =>
      command.kind === "policy.candidate" ? [command.payload] : [],
    ),
    unresolvedQuestions: workerQuestions(result),
    diagnostics: { warnings: result.warnings },
  });
}

function failWorkerRecord(
  work: Work,
  workerIndex: number,
  receivedAt: string,
  decision: string,
  body: string,
): { status: "processed"; classification: string } {
  const worker = work.projection.workers[workerIndex];
  if (worker !== undefined) {
    work.projection.workers[workerIndex] = {
      ...worker,
      status: "failed",
      deliveryGeneration: worker.deliveryGeneration + 1,
      updatedAt: receivedAt,
      lastErrorCode: decision,
      resultRef: stableId("worker_result_ref", worker.job.attemptId, decision),
    };
    const episode = work.aggregate.episodes.find((candidate) => candidate.episodeId === worker.episodeId);
    if (episode !== undefined) {
      queueMessage(
        work,
        `worker-project-${decision}`,
        episode.scope,
        "status",
        body,
        {
          kind: "episode_follow_up",
          episodeId: episode.episodeId,
          episodeVersion: episode.version,
        },
        projectDeliveryGuard(
          work.projection.workers[workerIndex] as ApplicationProjection["workers"][number],
        ),
      );
    }
  }
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "worker_reconciled",
      occurredAt: receivedAt,
      decision,
      containsPrivateData: worker?.job.scopeGrant.visibility === "personal",
    }),
  );
  return { status: "processed", classification: decision };
}

function requeueWorkerForCurrentContext(
  work: Work,
  workerIndex: number,
  receivedAt: string,
  reason: string,
): { status: "processed"; classification: string } {
  const worker = work.projection.workers[workerIndex];
  if (worker === undefined) {
    throw new Error("Cannot requeue a missing worker record");
  }
  const episode = work.aggregate.episodes.find((candidate) => candidate.episodeId === worker.episodeId);
  if (
    episode === undefined ||
    episode.delegation?.jobId !== worker.job.jobId ||
    ["completed", "dismissed", "superseded", "failed"].includes(episode.state)
  ) {
    work.projection.workers[workerIndex] = {
      ...worker,
      status: "cancelled",
      updatedAt: receivedAt,
      lastErrorCode: "worker_project_no_longer_active",
    };
    return ignoreWorkerAttempt(
      work,
      receivedAt,
      "worker_project_no_longer_active",
      worker.job.scopeGrant.visibility === "personal",
    );
  }
  if (worker.automaticRetryCount >= 3) {
    return failWorkerRecord(
      work,
      workerIndex,
      receivedAt,
      "worker_retry_exhausted",
      "I couldn’t complete this project safely after several fresh attempts. Reply here with an updated instruction and I’ll restart from the current family context.",
    );
  }

  const attemptNumber = worker.attemptNumber + 1;
  const attemptId = `${worker.job.jobId}.attempt.${attemptNumber}`;
  const scopeGrantId = stableId("context_grant", worker.job.jobId, attemptId);
  const deadline = plusMilliseconds(receivedAt, 15 * 60_000);
  const job = WorkerJobSchema.parse({
    ...worker.job,
    attemptId,
    baseHouseholdVersion: work.aggregate.version,
    policyVersion: work.aggregate.policyVersion,
    scopeGrant: {
      ...worker.job.scopeGrant,
      grantId: scopeGrantId,
      expiresAt: plusMilliseconds(receivedAt, 20 * 60_000),
    },
    capabilityGrants: capabilityGrantsForAttempt({
      jobId: worker.job.jobId,
      attemptId,
      householdId: worker.job.householdId,
      scopeGrantId,
      scope: episode.scope,
      purpose: worker.purpose,
      capabilities: worker.job.capabilityGrants.map((grant) => grant.capability),
      issuedAt: receivedAt,
      expiresAt: deadline,
    }),
    deadline,
  });
  const { resultRef: _resultRef, ...record } = worker;
  work.projection.workers[workerIndex] = {
    ...record,
    baseEpisodeVersion: episode.version,
    contextFingerprint: workerContextFingerprint({
      aggregate: work.aggregate,
      projection: work.projection,
      episodeId: episode.episodeId,
      purpose: worker.purpose,
      scope: episode.scope,
      evidenceRefs: job.evidenceRefs,
      asOf: receivedAt,
    }),
    job,
    status: "queued",
    attemptNumber,
    automaticRetryCount: worker.automaticRetryCount + 1,
    updatedAt: receivedAt,
    lastErrorCode: reason,
  };
  work.outbox.push(
    ApplicationOutboxIntentSchema.parse({
      ...appOutboxBase(work.input, `rerun:${job.attemptId}`),
      kind: "worker.run",
      job,
    }),
  );
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "worker_reconciled",
      occurredAt: receivedAt,
      decision: `worker_requeued:${reason}`,
      containsPrivateData: job.scopeGrant.visibility === "personal",
    }),
  );
  return { status: "processed", classification: `worker_requeued:${reason}` };
}

function truncateMessagePart(value: string, maximumLength: number): string {
  if (maximumLength <= 0) return "";
  if (value.length <= maximumLength) return value;
  if (maximumLength === 1) return "…";
  return `${value.slice(0, maximumLength - 1)}…`;
}

function workerResultMessage(result: WorkerResult): string {
  if (result.completion.status !== "complete") {
    return truncateMessagePart(result.summary.trim(), 4_000);
  }
  if (result.purpose === "meal_plan") {
    const artifact = result.completion.artifact;
    const mealLines = artifact.meals.map(
      (meal) => `• ${meal.when}: ${meal.meal} — ${meal.scheduleRationale}`,
    );
    const substitutionLines = artifact.substitutions.map(
      (substitution) =>
        `• Instead of ${substitution.insteadOf}, use ${substitution.use}: ${substitution.reason}`,
    );
    const groceryLines = artifact.groceryGroups.map((group) => `• ${group.group}: ${group.items.join(", ")}`);
    return truncateMessagePart(
      [
        result.summary.trim(),
        `Plan horizon: ${artifact.horizon} (schedule checked ${artifact.asOf})`,
        "Meals:",
        ...mealLines,
        ...(substitutionLines.length === 0 ? [] : ["Substitutions:", ...substitutionLines]),
        "Grocery list:",
        ...groceryLines,
        ...(artifact.assumptions.length === 0
          ? []
          : ["Assumptions:", ...artifact.assumptions.map((item) => `• ${item}`)]),
        ...(artifact.uncertainties.length === 0
          ? []
          : ["Uncertainties:", ...artifact.uncertainties.map((item) => `• ${item}`)]),
      ].join("\n"),
      4_000,
    );
  }

  if (result.purpose === "family_project") {
    const artifact = result.completion.artifact;
    const receipts = new Map(result.toolReceipts.map((receipt) => [receipt.receiptId, receipt]));
    const sourceUrls = unique(
      (artifact.citationReceiptIds ?? []).flatMap((receiptId) => {
        const receipt = receipts.get(receiptId);
        return receipt?.kind === "research_sources" ? receipt.sources.map((source) => source.url) : [];
      }),
    );
    return truncateMessagePart(
      [
        result.summary.trim(),
        artifact.plan,
        `As of ${artifact.asOf}`,
        "Phases:",
        ...artifact.phases.flatMap((phase) => [
          `• ${phase.name}: ${phase.outcome}`,
          ...phase.actions.map((action) => `  - ${action}`),
        ]),
        "Next actions:",
        ...artifact.nextActions.map((action) => `• ${action}`),
        ...(artifact.decisions.length === 0
          ? []
          : [
              "Decisions:",
              ...artifact.decisions.map(
                (decision) => `• ${decision.decision}: ${decision.recommendation} — ${decision.rationale}`,
              ),
            ]),
        ...(artifact.risks.length === 0
          ? []
          : ["Risks:", ...artifact.risks.map((risk) => `• ${risk.risk} — Mitigation: ${risk.mitigation}`)]),
        ...(artifact.assumptions.length === 0
          ? []
          : ["Assumptions:", ...artifact.assumptions.map((assumption) => `• ${assumption}`)]),
        ...(sourceUrls.length === 0 ? [] : [`Sources: ${sourceUrls.join(", ")}`]),
      ].join("\n"),
      4_000,
    );
  }

  const artifact = result.completion.artifact;
  const receipts = new Map(result.toolReceipts.map((receipt) => [receipt.receiptId, receipt]));
  const sourceUrls = (receiptIds: readonly string[]) =>
    unique(
      receiptIds.flatMap((receiptId) => {
        const receipt = receipts.get(receiptId);
        return receipt?.kind === "research_sources" ? receipt.sources.map((source) => source.url) : [];
      }),
    );
  const cited = (statement: string, receiptIds: readonly string[]) => {
    const urls = sourceUrls(receiptIds);
    return `${statement}${urls.length === 0 ? "" : ` [Sources: ${urls.join(", ")}]`}`;
  };
  return truncateMessagePart(
    [
      result.summary.trim(),
      `As of ${artifact.asOf}`,
      ...(artifact.comparison.length === 0
        ? []
        : [
            "Comparison:",
            ...artifact.comparison.map(
              (item) => `• ${item.option}: ${cited(item.assessment, item.sourceReceiptIds)}`,
            ),
          ]),
      "Findings:",
      ...artifact.findings.map((item) => `• ${cited(item.statement, item.sourceReceiptIds)}`),
      `Recommendation: ${cited(artifact.recommendation.statement, artifact.recommendation.sourceReceiptIds)}`,
      ...(artifact.uncertainties.length === 0
        ? []
        : ["Uncertainties:", ...artifact.uncertainties.map((item) => `• ${item}`)]),
    ].join("\n"),
    4_000,
  );
}

function workerQuestions(result: WorkerResult): readonly string[] {
  return result.completion.status === "needs_input" ? result.completion.questions : [];
}

function workerQuestionMessage(result: WorkerResult): string {
  const questions = workerQuestions(result);
  const blockingQuestion = questions[0]?.trim();
  if (blockingQuestion === undefined || blockingQuestion.length === 0) {
    throw new Error("An awaiting-input worker result must contain a blocking question");
  }
  const remaining = questions.length - 1;
  const questionBlock = [
    "Florence still needs:",
    `• ${blockingQuestion}`,
    ...(remaining === 0
      ? []
      : [
          `I’ll work through the other ${remaining} open ${remaining === 1 ? "question" : "questions"} after this answer.`,
        ]),
    "Use iMessage’s Reply on this Florence message with your answer.",
  ].join("\n");
  const separator = "\n\n";
  const summaryBudget = Math.max(0, 4_000 - questionBlock.length - separator.length);
  const summary = truncateMessagePart(result.summary.trim(), summaryBudget);
  return summary.length === 0 ? questionBlock : `${summary}${separator}${questionBlock}`;
}

function workerCompletionEvidence(
  receipt: WorkerToolReceipt,
  worker: ApplicationProjection["workers"][number],
  scope: DurableScope,
) {
  return EvidenceRefSchema.parse({
    evidenceId: stableId("evidence", worker.job.jobId, worker.job.attemptId, receipt.receiptId),
    source: "worker",
    sourceRef: worker.job.jobId,
    scope,
    observedAt: receipt.issuedAt,
    revision: 1,
    contentDigest: receipt.outputDigest,
  });
}

function ignoreWorkerAttempt(
  work: Work,
  receivedAt: string,
  decision: string,
  personal: boolean,
): { status: "processed"; classification: string } {
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "worker_reconciled",
      occurredAt: receivedAt,
      decision,
      containsPrivateData: personal,
    }),
  );
  return { status: "processed", classification: decision };
}

function processWorkerFailure(
  work: Work,
  input: Extract<ApplicationInput, { kind: "worker_failure" }>,
): { status: "processed" | "rejected"; classification: string } {
  const workerIndex = work.projection.workers.findIndex((candidate) => candidate.job.jobId === input.jobId);
  const worker = work.projection.workers[workerIndex];
  if (worker === undefined || worker.job.attemptId !== input.attemptId || worker.status !== "queued") {
    return ignoreWorkerAttempt(
      work,
      input.receivedAt,
      "worker_failure_obsolete",
      worker?.job.scopeGrant.visibility === "personal",
    );
  }
  if (input.retryable) {
    return requeueWorkerForCurrentContext(work, workerIndex, input.receivedAt, input.errorCode);
  }
  return failWorkerRecord(
    work,
    workerIndex,
    input.receivedAt,
    input.errorCode,
    "I couldn’t complete this project safely. Reply here with an updated instruction and I’ll restart from the current family context.",
  );
}

function processWorkerResult(
  work: Work,
  input: Extract<ApplicationInput, { kind: "worker_result" }>,
): { status: "processed" | "rejected"; classification: string } {
  const result = WorkerResultSchema.parse(input.result);
  const workerIndex = work.projection.workers.findIndex((candidate) => candidate.job.jobId === result.jobId);
  const worker = work.projection.workers[workerIndex];
  if (worker === undefined || worker.job.attemptId !== result.attemptId || worker.status !== "queued") {
    return ignoreWorkerAttempt(
      work,
      input.receivedAt,
      "worker_result_obsolete",
      worker?.job.scopeGrant.visibility === "personal",
    );
  }
  if (!workerIdentityMatches(result, worker.job)) {
    return failWorkerRecord(
      work,
      workerIndex,
      input.receivedAt,
      "worker_result_identity_mismatch",
      "The Project Lead returned a result that could not be authenticated to this exact attempt, so I discarded it. Reply here if you want me to restart the project.",
    );
  }
  const originatingEpisode = work.aggregate.episodes.find(
    (candidate) => candidate.episodeId === worker.episodeId,
  );
  if (originatingEpisode === undefined || originatingEpisode.delegation?.jobId !== worker.job.jobId) {
    return failWorkerRecord(
      work,
      workerIndex,
      input.receivedAt,
      "worker_episode_identity_mismatch",
      "The Project Lead result no longer matches this family project, so I discarded it. Reply here if you want to restart from the current project.",
    );
  }
  if (["completed", "dismissed", "superseded", "failed"].includes(originatingEpisode.state)) {
    return ignoreWorkerAttempt(
      work,
      input.receivedAt,
      "worker_result_obsolete",
      worker.job.scopeGrant.visibility === "personal",
    );
  }
  const currentContextFingerprint = workerContextFingerprint({
    aggregate: work.aggregate,
    projection: work.projection,
    episodeId: originatingEpisode.episodeId,
    purpose: worker.purpose,
    scope: originatingEpisode.scope,
    evidenceRefs: worker.job.evidenceRefs,
    asOf: input.receivedAt,
  });
  if (
    worker.contextFingerprint !== currentContextFingerprint ||
    worker.job.policyVersion !== work.aggregate.policyVersion
  ) {
    return requeueWorkerForCurrentContext(work, workerIndex, input.receivedAt, "worker_context_changed");
  }

  const verification = verifyWorkerResultCompletion(result, input.receivedAt);
  if (verification.status === "invalid") {
    return requeueWorkerForCurrentContext(
      work,
      workerIndex,
      input.receivedAt,
      `worker_result_unverified_${verification.reason}`,
    );
  }

  if (verification.status === "needs_input") {
    const questions = workerQuestions(result);
    const message = workerQuestionMessage(result);
    const { lastErrorCode: _lastErrorCode, ...current } = worker;
    work.projection.workers[workerIndex] = {
      ...current,
      status: "awaiting_input",
      deliveryGeneration: worker.deliveryGeneration + 1,
      latestSummary: message,
      outstandingQuestions: [...questions],
      updatedAt: input.receivedAt,
      resultRef: stableId("worker_result", result.jobId, result.attemptId),
    };
    queueMessage(
      work,
      "worker-project-questions",
      originatingEpisode.scope,
      "clarifying_question",
      message,
      {
        kind: "episode_follow_up",
        episodeId: originatingEpisode.episodeId,
        episodeVersion: originatingEpisode.version,
      },
      projectDeliveryGuard(work.projection.workers[workerIndex] as ApplicationProjection["workers"][number]),
    );
    work.audit.push(
      ApplicationAuditEntrySchema.parse({
        kind: "worker_reconciled",
        occurredAt: input.receivedAt,
        decision: "awaiting_input",
        containsPrivateData: worker.job.scopeGrant.visibility === "personal",
      }),
    );
    return { status: "processed", classification: "worker_result:awaiting_input" };
  }

  const commands = parseWorkerCommands(result, worker.job);
  if (commands === null) {
    return requeueWorkerForCurrentContext(
      work,
      workerIndex,
      input.receivedAt,
      "worker_result_invalid_commands",
    );
  }
  let retainedMemoryCandidates = 0;
  const reconciledCommands = commands.filter((command) => {
    if (command.kind === "message.propose" || command.kind === "policy.candidate") return false;
    if (command.kind === "action.propose") return false;
    if (command.kind !== "memory.candidate") return true;
    if (command.payload.confidence < 0.8 || retainedMemoryCandidates >= 3) return false;
    retainedMemoryCandidates += 1;
    return true;
  });
  const proposal = workerProposalFromCommands(
    result,
    reconciledCommands,
    input.receivedAt,
    work.aggregate.version,
    work.aggregate.policyVersion,
  );
  const accepted = acceptDomain(
    work,
    "worker-result",
    input.receivedAt,
    { kind: "worker", jobId: DomainWorkerJobIdSchema.parse(result.jobId) },
    { kind: "worker.proposal_received", proposal } as Omit<
      HouseholdSignal,
      "householdId" | "signalId" | "sequence" | "occurredAt" | "actor"
    >,
  );
  if (accepted.receipt.disposition !== "accepted") {
    return requeueWorkerForCurrentContext(work, workerIndex, input.receivedAt, "worker_domain_rejected");
  }

  const episode = work.aggregate.episodes.find((candidate) => candidate.episodeId === worker.episodeId);
  if (episode === undefined) {
    throw new Error("Reconciled worker episode is missing from the household aggregate");
  }
  const message = workerResultMessage(result);
  const projectArtifact =
    result.purpose === "family_project" && result.completion.status === "complete"
      ? result.completion.artifact
      : undefined;
  if (projectArtifact !== undefined) {
    const artifactRecorded = acceptDomain(
      work,
      "worker-project-artifact",
      input.receivedAt,
      { kind: "worker", jobId: DomainWorkerJobIdSchema.parse(worker.job.jobId) },
      {
        kind: "episode.artifact_recorded",
        episodeId: episode.episodeId,
        baseEpisodeVersion: episode.version,
        artifact: {
          artifactRef: proposal.resultId,
          contentDigest: projectArtifactContentDigest(projectArtifact),
          recordedAt: input.receivedAt,
        },
      } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
    );
    if (artifactRecorded.receipt.disposition !== "accepted") {
      return requeueWorkerForCurrentContext(work, workerIndex, input.receivedAt, "worker_artifact_rejected");
    }
  } else {
    const receipts = new Map(result.toolReceipts.map((receipt) => [receipt.receiptId, receipt]));
    const evidence = verification.proofReceiptIds.map((receiptId) => {
      const receipt = receipts.get(receiptId);
      if (receipt === undefined) {
        throw new Error("Verified worker proof receipt is missing");
      }
      return workerCompletionEvidence(receipt, worker, episode.scope);
    });
    const closure = acceptDomain(
      work,
      "worker-project-completed",
      input.receivedAt,
      { kind: "worker", jobId: DomainWorkerJobIdSchema.parse(worker.job.jobId) },
      {
        kind: "episode.closed",
        episodeId: episode.episodeId,
        baseEpisodeVersion: episode.version,
        outcome: {
          kind: "completed",
          summary:
            worker.purpose === "meal_plan"
              ? "Florence completed the requested meal plan and grocery list."
              : "Florence completed the requested household research.",
          evidence,
          recordedAt: input.receivedAt,
        },
      } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
    );
    if (closure.receipt.disposition !== "accepted") {
      throw new Error(`Worker episode closure was rejected: ${closure.receipt.reason ?? "unknown"}`);
    }
  }
  const completedEpisode = work.aggregate.episodes.find(
    (candidate) => candidate.episodeId === worker.episodeId,
  );
  if (completedEpisode === undefined) {
    throw new Error("Completed worker episode is missing from the household aggregate");
  }
  const { lastErrorCode: _lastErrorCode, ...current } = worker;
  work.projection.workers[workerIndex] = {
    ...current,
    baseEpisodeVersion: completedEpisode.version,
    status: "completed",
    deliveryGeneration: worker.deliveryGeneration + 1,
    latestSummary: message,
    ...(projectArtifact === undefined ? {} : { artifactRef: proposal.resultId, artifact: projectArtifact }),
    outstandingQuestions: [],
    updatedAt: input.receivedAt,
    resultRef: proposal.resultId,
  };
  queueMessage(
    work,
    "worker-project-result",
    episode.scope,
    "status",
    message,
    {
      kind: "episode_follow_up",
      episodeId: episode.episodeId,
      episodeVersion: completedEpisode.version,
    },
    projectDeliveryGuard(work.projection.workers[workerIndex] as ApplicationProjection["workers"][number]),
  );

  const recordedCandidateIds = new Set(
    accepted.changes.flatMap((change) =>
      change.kind === "memory_candidate_recorded" ? [change.candidateId] : [],
    ),
  );
  for (const candidate of work.aggregate.memoryCandidates) {
    if (!recordedCandidateIds.has(candidate.candidateId)) continue;
    queueMessage(
      work,
      `memory-candidate:${candidate.candidateId}`,
      candidate.scope,
      "clarifying_question",
      `I noticed a possible ${candidate.kind.replace("_", " ")}: “${candidate.statement}” Should I remember this for future family work? Reply “remember” or “don’t remember.”`,
      { kind: "memory_confirmation", candidateId: candidate.candidateId },
    );
  }
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "worker_reconciled",
      occurredAt: input.receivedAt,
      decision: "completed",
      containsPrivateData: worker.job.scopeGrant.visibility === "personal",
    }),
  );
  return {
    status: "processed",
    classification: "worker_result:completed",
  };
}

function processTimer(
  work: Work,
  input: Extract<ApplicationInput, { kind: "timer_fired" }>,
): { status: "processed" | "rejected"; classification: string } {
  const result = acceptDomain(
    work,
    "timer-fired",
    input.firedAt,
    { kind: "source_adapter", source: "system_clock" },
    {
      kind: "timer.fired",
      timerId: input.timerId,
      episodeId: input.episodeId,
      temporalPlanVersion: input.temporalPlanVersion,
      triggerId: input.triggerId,
      firedAt: input.firedAt,
    } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
  );
  return {
    status: statusForDomain(result),
    classification: `timer:${result.receipt.disposition}`,
  };
}

function processEffectReceipt(
  work: Work,
  input: Extract<ApplicationInput, { kind: "effect_receipt" }>,
): { status: "processed" | "rejected"; classification: string } {
  const result = acceptDomain(
    work,
    "effect-receipt",
    input.recordedAt,
    { kind: "source_adapter", source: "effect_executor" },
    {
      kind: "effect.receipt_received",
      receiptId: input.receiptId,
      actionId: input.actionId,
      actionDigest: input.actionDigest,
      outcome: input.outcome,
      recordedAt: input.recordedAt,
      ...(input.providerReference === undefined ? {} : { providerReference: input.providerReference }),
    } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
  );
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "external_action_reconciled",
      occurredAt: input.recordedAt,
      decision: input.outcome,
      sourceRef: input.actionId,
      containsPrivateData: false,
    }),
  );
  return {
    status: statusForDomain(result),
    classification: `effect_receipt:${result.receipt.disposition}`,
  };
}

function processDailyBrief(
  work: Work,
  input: Extract<ApplicationInput, { kind: "daily_brief" }>,
): { status: "processed"; classification: string } {
  const scope: DurableScope =
    input.reason === "scheduled"
      ? { kind: "household" }
      : { kind: "personal", adultId: AdultIdSchema.parse(input.requestedByAdultId) };
  queueMessage(work, "daily-brief", scope, "daily_brief", briefBody(work, input.occurredAt));
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "daily_brief_built",
      occurredAt: input.occurredAt,
      decision: input.reason,
      ...(input.requestedByAdultId === undefined ? {} : { adultId: input.requestedByAdultId }),
      containsPrivateData: false,
    }),
  );
  return { status: "processed", classification: `daily_brief:${input.reason}` };
}

function compactPrivateReviewSummary(value: string): string {
  const limit = 55;
  return value.length <= limit ? value : `${value.slice(0, limit - 1).trimEnd()}…`;
}

function privateReviewDigestBody(
  input: Extract<ApplicationInput, { kind: "private_review_digest" }>,
): string {
  const localDate = Temporal.PlainDate.from(input.localDate);
  const findings = input.items.map(
    (item) =>
      `- ${item.source === "gmail" ? "Gmail" : "Calendar"}: ${compactPrivateReviewSummary(item.summary)}`,
  );
  return `Private daily review for ${localDate.month}/${localDate.day}:\n\n${findings.join("\n")}\n\nThese findings stayed private to you and were not included in the household brief.`;
}

function processPrivateReviewDigest(
  work: Work,
  input: Extract<ApplicationInput, { kind: "private_review_digest" }>,
): { status: "processed" | "rejected"; classification: string } {
  if (!work.aggregate.verifiedAdultIds.includes(input.adultId)) {
    return { status: "rejected", classification: "private_review_digest:unknown_adult" };
  }
  queueMessage(
    work,
    `private-review-digest:${input.adultId}:${input.localDate}`,
    personal(input.adultId),
    "private_review",
    privateReviewDigestBody(input),
  );
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "private_review_digest_built",
      occurredAt: input.occurredAt,
      decision: "scheduled",
      adultId: input.adultId,
      containsPrivateData: true,
    }),
  );
  return { status: "processed", classification: "private_review_digest:scheduled" };
}

function processGoogleConnected(
  work: Work,
  input: Extract<ApplicationInput, { kind: "google_connected" }>,
): { status: "processed" | "rejected"; classification: string } {
  const onboarding = work.projection.onboarding;
  if (
    !work.aggregate.verifiedAdultIds.includes(input.adultId) ||
    !onboardingParticipants(onboarding).includes(input.adultId)
  ) {
    return { status: "rejected", classification: "google_connected:unknown_adult" };
  }
  if (!input.gmailReady || !input.calendarReady) {
    work.audit.push(
      ApplicationAuditEntrySchema.parse({
        kind: "onboarding_transition",
        occurredAt: input.occurredAt,
        decision: "google_connection_incomplete",
        sourceRef: input.connectionId,
        adultId: input.adultId,
        containsPrivateData: true,
      }),
    );
    return { status: "processed", classification: "google_connected:incomplete" };
  }
  const connected = unique([...onboarding.googleConnectedAdultIds, input.adultId]);
  const allConnected = onboardingParticipants(onboarding).every((adultId) => connected.includes(adultId));
  const ready =
    onboarding.phase === "connecting_sources" &&
    onboarding.profileConfirmedAdultIds.length === 2 &&
    allConnected;
  work.projection.onboarding = ApplicationProjectionSchema.shape.onboarding.parse({
    ...onboarding,
    phase: ready ? "active" : onboarding.phase,
    googleConnectedAdultIds: connected,
  });
  if (ready) {
    queueMessage(
      work,
      "onboarding-ready-after-google",
      { kind: "household" },
      "onboarding",
      "Both adults are connected. Florence is ready: I’ll privately filter mail and calendars, surface only family-relevant meaning with the agreed privacy boundaries, and follow shared work through in this group.",
    );
  }
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "onboarding_transition",
      occurredAt: input.occurredAt,
      decision: `${onboarding.phase}:${ready ? "active" : onboarding.phase}:google_connected`,
      sourceRef: input.connectionId,
      adultId: input.adultId,
      containsPrivateData: true,
    }),
  );
  return {
    status: "processed",
    classification: ready ? "google_connected:onboarding_complete" : "google_connected:recorded",
  };
}

function processPrivateControl(
  work: Work,
  input: Extract<ApplicationInput, { kind: "private_control" }>,
): { status: "processed" | "rejected"; classification: string } {
  const target: DurableScope = { kind: "personal", adultId: input.requesterAdultId };
  if (!work.aggregate.verifiedAdultIds.some((candidate) => candidate === input.requesterAdultId)) {
    queueMessage(
      work,
      "private-control-unverified",
      target,
      "status",
      "I couldn't verify this private household control. Nothing was changed.",
    );
    return { status: "rejected", classification: "private_control:unknown_adult" };
  }

  if (input.action.kind === "revoke_memory") {
    const result = acceptDomain(
      work,
      "private-control-memory-revoked",
      input.occurredAt,
      { kind: "adult", adultId: input.requesterAdultId },
      {
        kind: "memory.revoked",
        memoryId: input.action.memoryId,
      } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
    );
    queueMessage(
      work,
      "private-control-memory-status",
      target,
      "status",
      result.receipt.disposition === "accepted"
        ? "That learned memory is revoked. Florence will no longer treat it as active household knowledge."
        : "That memory changed before the revocation was applied. Nothing else was changed; ask “what do you remember?” for the current list.",
    );
    return {
      status: statusForDomain(result),
      classification: `private_control:memory:${result.receipt.disposition}`,
    };
  }

  const result = acceptDomain(
    work,
    "private-control-sharing-policy-revoked",
    input.occurredAt,
    { kind: "adult", adultId: input.requesterAdultId },
    {
      kind: "policy.revoked",
      policyId: input.action.policyId,
      expectedPolicyVersion: input.action.expectedPolicyVersion,
    } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
  );
  queueMessage(
    work,
    "private-control-sharing-policy-status",
    target,
    "status",
    result.receipt.disposition === "accepted"
      ? "That automatic-sharing rule is revoked. Future matching private items will require review again."
      : "That sharing rule changed before the revocation was applied. Nothing else was changed; ask “show my sharing rules” for the current list.",
  );
  return {
    status: statusForDomain(result),
    classification: `private_control:sharing_policy:${result.receipt.disposition}`,
  };
}

async function commitWork(
  dependencies: FlorenceApplicationDependencies,
  work: Work,
  status: "processed" | "rejected",
  classification: string,
) {
  const outcome = ApplicationOutcomeSchema.parse({
    status,
    classification,
    domainReceipts: work.receipts,
    outboxIntentIds: work.outbox.map((intent) => intent.intentId),
  });
  const commit: ApplicationCommit = {
    householdId: work.aggregate.householdId,
    idempotencyKey: work.input.idempotencyKey,
    expectedRevision: work.initial.revision,
    aggregate: work.aggregate,
    projection: ApplicationProjectionSchema.parse(work.projection),
    signals: work.signals,
    changes: work.changes,
    outbox: work.outbox,
    audit: work.audit,
    privateReviewItems: work.privateReviewItems,
    outcome,
  };
  const result = await dependencies.repository.commit(commit);
  if (result.disposition === "conflict") {
    throw new ApplicationRepositoryConflictError(
      work.aggregate.householdId,
      work.initial.revision,
      result.actualRevision,
    );
  }
  return ApplicationResultSchema.parse({
    householdId: work.aggregate.householdId,
    idempotencyKey: work.input.idempotencyKey,
    disposition: result.disposition,
    revision: result.revision,
    outcome: result.outcome,
  });
}

async function loadSnapshot(
  dependencies: FlorenceApplicationDependencies,
  householdId: string,
): Promise<HouseholdApplicationSnapshot> {
  const snapshot = await dependencies.repository.load(householdId);
  if (snapshot === null) {
    throw new HouseholdApplicationNotFoundError(`Unknown household application: ${householdId}`);
  }
  return HouseholdApplicationSnapshotSchema.parse(snapshot);
}

async function runWorker(
  dependencies: FlorenceApplicationDependencies,
  input: Extract<ApplicationInput, { kind: "run_worker" }>,
  activeAttempts: ActiveWorkerAttempts,
  upstreamSignal?: AbortSignal,
): Promise<ApplicationResult> {
  const duplicate = await dependencies.repository.findProcessed(input.householdId, input.idempotencyKey);
  if (duplicate !== null) {
    return duplicate;
  }
  const snapshot = await loadSnapshot(dependencies, input.householdId);
  const record = snapshot.projection.workers.find((candidate) => candidate.job.jobId === input.jobId);
  if (record === undefined || record.job.attemptId !== input.attemptId || record.status !== "queued") {
    return ApplicationResultSchema.parse({
      householdId: input.householdId,
      idempotencyKey: input.idempotencyKey,
      disposition: "duplicate",
      revision: snapshot.revision,
      outcome: {
        status: "processed",
        classification: "worker_run_obsolete",
        domainReceipts: [],
        outboxIntentIds: [],
      },
    });
  }
  const routes = WorkerRoutesSchema.parse(dependencies.workerRoutes ?? DEFAULT_WORKER_ROUTES);
  const completedAt = () => new Date(Math.max(Date.now(), Date.parse(input.requestedAt))).toISOString();
  const failAttempt = (errorCode: string, retryable: boolean) =>
    processApplicationInput(
      dependencies,
      routes,
      activeAttempts,
      ApplicationInputSchema.parse({
        kind: "worker_failure",
        householdId: input.householdId,
        idempotencyKey: input.idempotencyKey,
        receivedAt: completedAt(),
        jobId: input.jobId,
        attemptId: input.attemptId,
        errorCode,
        retryable,
      }),
    );
  const episode = snapshot.aggregate.episodes.find((candidate) => candidate.episodeId === record.episodeId);
  const contextChanged =
    episode === undefined ||
    record.contextFingerprint !==
      workerContextFingerprint({
        aggregate: snapshot.aggregate,
        projection: snapshot.projection,
        episodeId: record.episodeId,
        purpose: record.purpose,
        scope: episode?.scope ?? { kind: "household" },
        evidenceRefs: record.job.evidenceRefs,
        asOf: new Date().toISOString(),
      });
  if (
    contextChanged ||
    record.job.policyVersion !== snapshot.aggregate.policyVersion ||
    Date.parse(record.job.deadline) <= Date.now() ||
    Date.parse(record.job.scopeGrant.expiresAt) <= Date.now()
  ) {
    return failAttempt("worker_context_stale", true);
  }
  const activeAttempt = activeAttempts.begin(record.job, {
    ...(upstreamSignal === undefined ? {} : { upstream: upstreamSignal }),
    isStillQueued: async () => {
      const current = await dependencies.repository.load(input.householdId);
      const currentRecord = current?.projection.workers.find(
        (candidate) => candidate.job.jobId === input.jobId && candidate.job.attemptId === input.attemptId,
      );
      return currentRecord?.status === "queued";
    },
  });
  let result: WorkerResult;
  try {
    const options = await dependencies.workerContext.contextFor(record.job, snapshot);
    result = await dependencies.workerRuntime.run(record.job, {
      ...options,
      signal:
        options.signal === undefined
          ? activeAttempt.signal
          : AbortSignal.any([activeAttempt.signal, options.signal]),
    });
    if (options.validateBeforeAccept !== undefined && !(await options.validateBeforeAccept())) {
      return failAttempt("worker_context_stale", true);
    }
  } catch (error) {
    const runtimeError = asWorkerRuntimeError(error);
    const retryable =
      runtimeError.retryable ||
      [
        "cancelled",
        "invalid_output",
        "deadline_exceeded",
        "budget_exceeded",
        "tool_failed",
        "cleanup_failed",
      ].includes(runtimeError.code);
    return failAttempt(`worker_${runtimeError.code}`, retryable);
  } finally {
    activeAttempt.finish();
  }
  return processApplicationInput(
    dependencies,
    routes,
    activeAttempts,
    ApplicationInputSchema.parse({
      kind: "worker_result",
      householdId: input.householdId,
      idempotencyKey: input.idempotencyKey,
      receivedAt: completedAt(),
      result,
    }),
  );
}

async function processApplicationInput(
  dependencies: FlorenceApplicationDependencies,
  routes: WorkerRoutes,
  activeAttempts: ActiveWorkerAttempts,
  input: ApplicationInput,
  signal?: AbortSignal,
): Promise<ApplicationResult> {
  if (input.kind === "run_worker") {
    return runWorker(dependencies, input, activeAttempts, signal);
  }
  const duplicate = await dependencies.repository.findProcessed(input.householdId, input.idempotencyKey);
  if (duplicate !== null) {
    return ApplicationResultSchema.parse({ ...duplicate, disposition: "duplicate" });
  }
  const snapshot = await loadSnapshot(dependencies, input.householdId);
  const work = createWork(input, snapshot);
  let processed: { status: "processed" | "rejected"; classification: string };
  switch (input.kind) {
    case "conversation_message":
      processed = await processConversation(work, input, dependencies, routes);
      break;
    case "gmail_message":
      processed = await processGmail(work, input, dependencies);
      break;
    case "gmail_message_deleted":
      processed = processGmailDeleted(work, input);
      break;
    case "calendar_event":
      processed = await processCalendar(work, input, dependencies);
      break;
    case "calendar_event_deleted":
      processed = processCalendarDeleted(work, input);
      break;
    case "worker_result":
      processed = processWorkerResult(work, input);
      break;
    case "worker_failure":
      processed = processWorkerFailure(work, input);
      break;
    case "timer_fired":
      processed = processTimer(work, input);
      break;
    case "effect_receipt":
      processed = processEffectReceipt(work, input);
      break;
    case "daily_brief":
      processed = processDailyBrief(work, input);
      break;
    case "private_review_digest":
      processed = processPrivateReviewDigest(work, input);
      break;
    case "google_connected":
      processed = processGoogleConnected(work, input);
      break;
    case "private_control":
      processed = processPrivateControl(work, input);
      break;
  }
  return commitWork(dependencies, work, processed.status, processed.classification);
}

export function createOnboardingProjection(input: {
  initiatorAdultId: string;
  phase?: "awaiting_initiator_consent" | "active";
  invitedAdultId?: string;
  groupChannelId?: string;
}): ApplicationProjection["onboarding"] {
  if (input.phase === "active") {
    if (input.invitedAdultId === undefined || input.groupChannelId === undefined) {
      throw new Error("Active onboarding requires the second adult and household group");
    }
    return ApplicationProjectionSchema.shape.onboarding.parse({
      phase: "active",
      initiatorAdultId: input.initiatorAdultId,
      invitedAdultId: input.invitedAdultId,
      consentedAdultIds: [input.initiatorAdultId, input.invitedAdultId],
      privateDmAdultIds: [input.initiatorAdultId, input.invitedAdultId],
      groupChannelId: input.groupChannelId,
      adultNames: [
        { adultId: input.initiatorAdultId, displayName: "Adult 1" },
        { adultId: input.invitedAdultId, displayName: "Adult 2" },
      ],
      profileConfirmedAdultIds: [input.initiatorAdultId, input.invitedAdultId],
      googleConnectedAdultIds: [input.initiatorAdultId, input.invitedAdultId],
    });
  }
  return ApplicationProjectionSchema.shape.onboarding.parse({
    phase: "awaiting_initiator_consent",
    initiatorAdultId: input.initiatorAdultId,
    consentedAdultIds: [],
    privateDmAdultIds: [],
    adultNames: [],
    profileConfirmedAdultIds: [],
    googleConnectedAdultIds: [],
  });
}

export function createApplicationProjection(
  onboarding: ApplicationProjection["onboarding"],
): ApplicationProjection {
  return ApplicationProjectionSchema.parse({
    onboarding,
    sharedProfile: { facts: [] },
    gmailTriage: [],
    gmailSources: [],
    calendarTriage: [],
    calendarSources: [],
    pendingPromotions: [],
    workers: [],
  });
}

export function createFlorenceApplication(
  dependencies: FlorenceApplicationDependencies,
): FlorenceApplication {
  const routes = WorkerRoutesSchema.parse(dependencies.workerRoutes ?? DEFAULT_WORKER_ROUTES);
  const activeAttempts = new ActiveWorkerAttempts();
  return Object.freeze({
    async process(rawInput: unknown) {
      const input = ApplicationInputSchema.parse(rawInput);
      const result = await processApplicationInput(dependencies, routes, activeAttempts, input);
      if (input.kind === "conversation_message") {
        try {
          const latest = await dependencies.repository.load(input.householdId);
          if (latest !== null) activeAttempts.reconcile(input.householdId, latest.projection.workers);
        } catch {
          // Durable polling still observes cancellation if this best-effort fast path cannot reread.
        }
      }
      return result;
    },

    async executeOutbox(rawIntent: unknown, executedAt: string, signal?: AbortSignal) {
      const intent = ApplicationOutboxIntentSchema.parse(rawIntent);
      const recordedAt = InstantStringSchema.parse(executedAt);
      if (intent.kind === "worker.run") {
        const applicationResult = await processApplicationInput(
          dependencies,
          routes,
          activeAttempts,
          ApplicationInputSchema.parse({
            kind: "run_worker",
            householdId: intent.householdId,
            idempotencyKey: `${intent.idempotencyKey}:result`,
            jobId: intent.job.jobId,
            attemptId: intent.job.attemptId,
            requestedAt: recordedAt,
          }),
          signal,
        );
        return OutboxExecutionResultSchema.parse({
          intentId: intent.intentId,
          status: "succeeded",
          applicationResult,
        });
      }

      const receipt = EffectExecutionReceiptSchema.parse(await dependencies.effectExecutor.execute(intent));
      let applicationResult: ApplicationResult | undefined;
      if (receipt.externalAction !== undefined) {
        applicationResult = await processApplicationInput(
          dependencies,
          routes,
          activeAttempts,
          ApplicationInputSchema.parse({
            kind: "effect_receipt",
            householdId: intent.householdId,
            idempotencyKey: `${intent.idempotencyKey}:${receipt.externalAction.receiptId}`,
            receiptId: receipt.externalAction.receiptId,
            actionId: receipt.externalAction.actionId,
            actionDigest: receipt.externalAction.actionDigest,
            outcome: receipt.externalAction.outcome,
            recordedAt: receipt.recordedAt,
            ...(receipt.externalAction.providerReference === undefined
              ? {}
              : { providerReference: receipt.externalAction.providerReference }),
          }),
        );
      }
      return OutboxExecutionResultSchema.parse({
        intentId: intent.intentId,
        status: receipt.status,
        ...(applicationResult === undefined ? {} : { applicationResult }),
      });
    },
  });
}
