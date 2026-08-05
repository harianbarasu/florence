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
  WorkerProposalSchema,
} from "../domain/index.js";
import { type WorkerJob, WorkerJobSchema, type WorkerResult, WorkerResultSchema } from "../runtime/index.js";
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
  EffectExecutionReceiptSchema,
  type GmailInboxItem,
  GmailTriageResultSchema,
  type HouseholdApplicationSnapshot,
  HouseholdApplicationSnapshotSchema,
  OutboxExecutionResultSchema,
  type PendingPromotion,
  SharedProfileFactSchema,
  type WorkerCommand,
  WorkerCommandSchema,
  type WorkerPurpose,
  type WorkerRoutes,
  WorkerRoutesSchema,
} from "./contracts.js";
import type { ApplicationCommit, FlorenceApplication, FlorenceApplicationDependencies } from "./ports.js";

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

function gmailEvidence(item: GmailInboxItem) {
  return EvidenceRefSchema.parse({
    evidenceId: stableId("evidence", item.householdId, item.ownerAdultId, item.messageRef),
    source: "gmail",
    sourceRef: item.messageRef,
    scope: { kind: "personal", adultId: item.ownerAdultId },
    observedAt: item.occurredAt,
    revision: item.revision,
    contentDigest: contentDigest(
      item.sender ?? "",
      item.subject ?? "",
      item.snippet ?? "",
      item.bodyText ?? "",
      ...item.attachmentRefs,
      ...item.attachmentContents.map((attachment) => attachment.contentDigest),
    ),
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
): void {
  work.outbox.push(
    ApplicationOutboxIntentSchema.parse({
      ...appOutboxBase(work.input, suffix),
      kind: "conversation.send",
      targetScope: scope,
      messageClass,
      body,
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
  const label = purpose === "meal_plan" ? "Prepare the requested household meal plan" : "Research";
  return [
    `${label}: ${title}.`,
    `Completion contract: ${requiredOutcome}.`,
    ...(details.length === 0 ? [] : [`Constraints: ${details.join("; ")}.`]),
    "Return only app-owned proposed commands. Do not send messages or perform external actions.",
  ].join(" ");
}

function buildWorkerJob(input: {
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
  const jobId = stableId("job", input.work.input.idempotencyKey, input.purpose);
  return WorkerJobSchema.parse({
    jobId,
    attemptId: `${jobId}.attempt.1`,
    householdId: input.work.aggregate.householdId,
    baseHouseholdVersion: input.work.aggregate.version,
    policyVersion: input.work.aggregate.policyVersion,
    objective: workerObjective(input.purpose, input.title, input.requiredOutcome, input.details),
    scopeGrant: {
      grantId: stableId("context_grant", jobId),
      visibility: input.scope.kind,
      ...(input.scope.kind === "personal" ? { adultId: input.scope.adultId } : {}),
      purpose: input.purpose,
      expiresAt: plusMilliseconds(input.occurredAt, 20 * 60_000),
    },
    evidenceRefs: [input.evidenceId],
    capabilityIds: route.capabilityIds,
    modelRouteId: route.modelRouteId,
    modelCapabilityProfile: route.modelCapabilityProfile,
    budget: {
      maxDurationMs: route.maxDurationMs,
      maxModelCalls: route.maxModelCalls,
      maxToolCalls: route.maxToolCalls,
    },
    deadline: plusMilliseconds(input.occurredAt, 15 * 60_000),
    outputContractRef: route.outputContractRef,
    allowedToolNames: route.allowedToolNames,
  });
}

function enqueueWorker(
  work: Work,
  purpose: WorkerPurpose,
  episodeId: string,
  evidenceId: string,
  title: string,
  requiredOutcome: string,
  details: readonly string[],
  scope: DurableScope,
  routes: WorkerRoutes,
  occurredAt: string,
): void {
  const job = buildWorkerJob({
    purpose,
    work,
    episodeId,
    evidenceId,
    title,
    requiredOutcome,
    details,
    scope,
    routes,
    occurredAt,
  });
  work.projection.workers.push({
    purpose,
    episodeId: EpisodeProposalSchema.shape.episodeId.parse(episodeId),
    job,
    status: "queued",
    createdAt: occurredAt,
  });
  work.outbox.push(
    ApplicationOutboxIntentSchema.parse({
      ...appOutboxBase(work.input, `run:${job.jobId}`),
      kind: "worker.run",
      job,
    }),
  );
}

function auditClassification(
  work: Work,
  kind: "conversation_classified" | "gmail_triaged" | "calendar_triaged" | "calendar_reconciled",
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

function onboardingParticipants(projection: ApplicationProjection["onboarding"]): string[] {
  return unique(
    [projection.initiatorAdultId, projection.invitedAdultId].filter(
      (value): value is NonNullable<typeof value> => value !== undefined,
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
        item.senderAdultId !== onboarding.initiatorAdultId
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
        "Consent recorded. Invite the second adult from this private conversation.",
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
        "The invitation is ready. For inbound-first consent, ask the second adult to text Florence from their own iMessage number and explicitly accept. Florence will not contact them first.",
      );
      break;
    }
    case "accept_invite": {
      if (
        onboarding.phase !== "awaiting_invitee_consent" ||
        item.channel.scope !== "personal" ||
        item.senderAdultId !== onboarding.invitedAdultId
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
          "Both adults have consented. Create one group with both adults and Florence.",
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
        phase: "building_profile",
        groupChannelId: item.channel.channelId,
      };
      queueMessage(
        work,
        "onboarding-profile",
        { kind: "household" },
        "onboarding",
        `The household group is connected. Let's make a light shared profile—not a meticulous tracker. In one or several messages, share only what helps coordination: children or other dependents and their schools or childcare, recurring activities, normal morning/pickup/bedtime anchors, and dietary constraints. “None” or “unknown” is fine. Florence currently uses ${work.aggregate.timeZone}. I’ll summarize what I record before both adults confirm it.`,
      );
      break;
    }
    case "update_profile": {
      if (
        !["building_profile", "active"].includes(onboarding.phase) ||
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
    case "confirm_profile": {
      if (
        !["building_profile", "active"].includes(onboarding.phase) ||
        item.channel.scope !== "household" ||
        item.channel.channelId !== onboarding.groupChannelId ||
        !onboardingParticipants(onboarding).includes(item.senderAdultId) ||
        work.projection.sharedProfile.facts.length === 0
      ) {
        return invalidOnboarding(
          work,
          item,
          "Add at least one useful shared profile detail before confirming it.",
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
      next = {
        ...onboarding,
        phase: onboarding.phase === "active" || confirmed.length === 2 ? "active" : "building_profile",
        profileConfirmedAdultIds: confirmed,
      };
      queueMessage(
        work,
        `onboarding-confirm-${item.senderAdultId}`,
        { kind: "household" },
        "onboarding",
        confirmed.length === 2
          ? "Both adults confirmed the shared profile. Florence is ready."
          : "One adult confirmed the shared profile. The other adult can review and confirm it.",
      );
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

function briefBody(aggregate: HouseholdAggregate): string {
  const terminal = new Set(["completed", "dismissed", "superseded", "failed"]);
  const episodes = aggregate.episodes
    .filter((episode) => episode.scope.kind === "household" && !terminal.has(episode.state))
    .sort((left, right) => {
      const leftTime = left.temporalPlan?.referenceAt ?? "9999-12-31T23:59:59Z";
      const rightTime = right.temporalPlan?.referenceAt ?? "9999-12-31T23:59:59Z";
      return leftTime.localeCompare(rightTime) || left.title.localeCompare(right.title);
    })
    .slice(0, 10);
  if (episodes.length === 0) {
    return "Daily brief: there are no open household commitments or requested projects.";
  }
  const lines = episodes.map((episode) => {
    const state =
      episode.state === "awaiting_acknowledgement"
        ? "awaiting owner acknowledgement"
        : episode.state.replaceAll("_", " ");
    const timing = episode.temporalPlan?.deadlineAt ?? episode.temporalPlan?.eventAt;
    return `• ${episode.title} — ${state}${timing === undefined ? "" : `; reference ${timing}`}`;
  });
  return `Daily brief:\n${lines.join("\n")}`;
}

function statusForDomain(result: AcceptanceResult): "processed" | "rejected" {
  return result.receipt.disposition === "rejected" ? "rejected" : "processed";
}

function proposalForConversation(
  item: ConversationInboxItem,
  classification: Extract<ConversationClassification, { intent: "propose_commitment" }>,
) {
  const evidence = conversationEvidence(item);
  return EpisodeProposalSchema.parse({
    episodeId: stableId("episode", item.idempotencyKey, "commitment"),
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
    ...(classification.temporalPlan === undefined ? {} : { temporalPlan: classification.temporalPlan }),
  });
}

function processProjectRequest(
  work: Work,
  item: ConversationInboxItem,
  classification: Extract<ConversationClassification, { intent: "research_request" | "meal_plan_request" }>,
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
    classification.intent === "meal_plan_request" ? "meal_plan" : "family_research";
  const episodeId = stableId("episode", item.idempotencyKey, purpose);
  const requiredOutcome =
    classification.scopeAssessment.decision === "narrow"
      ? classification.scopeAssessment.householdConsequence
      : classification.requiredOutcome;
  const result = acceptDomain(
    work,
    `project:${purpose}`,
    item.occurredAt,
    { kind: "adult", adultId: item.senderAdultId },
    {
      kind: "episode.proposed",
      proposal: EpisodeProposalSchema.parse({
        episodeId,
        type: purpose === "meal_plan" ? "meal_plan" : "research",
        targetScope: scope,
        title: classification.title,
        requiredOutcome,
        evidence: [evidence],
        sourceClass: purpose === "meal_plan" ? "household.meal_request" : "household.research_request",
        sensitivity: "ordinary",
      }),
    } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
  );
  if (result.receipt.disposition !== "accepted") {
    return "rejected";
  }
  const details =
    classification.intent === "meal_plan_request"
      ? [`Horizon: ${classification.horizon}`, ...classification.constraints]
      : classification.constraints;
  enqueueWorker(
    work,
    purpose,
    episodeId,
    evidence.evidenceId,
    classification.title,
    requiredOutcome,
    details,
    scope,
    routes,
    item.occurredAt,
  );
  queueMessage(
    work,
    `project-started-${purpose}`,
    scope,
    "status",
    purpose === "meal_plan"
      ? "The requested meal-planning project is open. Florence will return a practical plan and grocery list."
      : "The requested household research project is open. Florence will return a sourced comparison.",
  );
  return "processed";
}

function queueCalendarUnavailable(
  work: Work,
  item: ConversationInboxItem,
  reason: "no_write_connection" | "ambiguous_write_connection" | "projection_incomplete",
): void {
  const body =
    reason === "no_write_connection"
      ? "I need the requesting adult to connect a Google account with Calendar access in a private DM before I can prepare this event."
      : reason === "ambiguous_write_connection"
        ? "More than one writable calendar account matches. Name the linked account in the event request so I can prepare one exact action."
        : "I’m still synchronizing private calendar availability, so I won’t ask for approval yet. Try the request again after synchronization finishes.";
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
    (candidate) =>
      candidate.action.actionId === actionId &&
      candidate.action.kind === "calendar_update" &&
      candidate.state === "awaiting_approval",
  );
  if (pending === undefined || pending.action.kind !== "calendar_update") {
    queueMessage(
      work,
      "calendar-approval-missing",
      { kind: "household" },
      "status",
      "That exact calendar proposal is no longer awaiting approval.",
    );
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
  });
  if (preparation.status === "unavailable") {
    queueCalendarUnavailable(work, item, preparation.reason);
    return "rejected";
  }
  if (
    preparation.relevantDataDigest !== pending.action.relevantDataDigest ||
    preparation.hasConflict !== pending.action.hasConflict
  ) {
    queueMessage(
      work,
      "calendar-approval-invalidated",
      { kind: "household" },
      "status",
      "Household calendar availability changed after this proposal, so I did not create the event. Send the event request again for an updated conflict check and approval.",
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
        queueMessage(
          work,
          "commitment-captured",
          targetScope(item),
          "status",
          classification.proposedOwnerAdultId === undefined
            ? `Captured “${proposal.title}” as an open proposed commitment.`
            : `Captured “${proposal.title}”. The proposed owner needs to acknowledge it explicitly.`,
        );
      }
      return statusForDomain(result);
    }
    case "acknowledge_owner": {
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
        queueMessage(
          work,
          "owner-acknowledged-status",
          targetScope(item),
          "status",
          "The commitment owner is acknowledged.",
        );
      }
      return statusForDomain(result);
    }
    case "reassign_owner": {
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
      return statusForDomain(result);
    }
    case "close_episode": {
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
    case "research_request":
    case "meal_plan_request":
      return processProjectRequest(work, item, classification, routes);
    case "calendar_event_create_request":
      return proposeCalendarEvent(work, item, classification, dependencies);
    case "calendar_event_clarification":
      return askForCalendarFields(work, item, classification.missingFields);
    case "approve_calendar_event":
      return approveCalendarEvent(work, item, classification.actionId, dependencies);
    case "daily_brief_request":
      queueMessage(work, "brief-request", targetScope(item), "daily_brief", briefBody(work.aggregate));
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
    case "revoke_policy":
      return revokePolicy(work, item, classification.policyId, classification.expectedPolicyVersion);
  }
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
  queueMessage(work, "promotion-household", { kind: "household" }, "status", pending.minimumHouseholdMeaning);
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
  const promotionId = stableId("promotion", item.householdId, item.ownerAdultId, item.messageRef);
  const sourceMatcher = gmailSourceMatcher(item);
  const proposal = EpisodeProposalSchema.parse({
    episodeId: stableId("episode", promotionId),
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
    ...(triage.temporalPlan === undefined ? {} : { temporalPlan: triage.temporalPlan }),
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
    queueMessage(
      work,
      "gmail-policy-household",
      { kind: "household" },
      "status",
      pending.minimumHouseholdMeaning,
    );
  }
  return statusForDomain(result);
}

async function processGmail(
  work: Work,
  item: GmailInboxItem,
  dependencies: FlorenceApplicationDependencies,
): Promise<{ status: "processed" | "rejected"; classification: string }> {
  if (!work.aggregate.verifiedAdultIds.includes(item.ownerAdultId)) {
    return { status: "rejected", classification: "gmail_unknown_owner" };
  }
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
      confirmedRoutineAnchors: work.aggregate.routineAnchors,
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
      queueMessage(
        work,
        "gmail-private-review",
        personal(item.ownerAdultId),
        "private_review",
        triage.privateSummary,
      );
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
        return {
          status: promoteByPolicy(work, item, pending, policy),
          classification: "gmail:policy_promotion",
        };
      }
      work.projection.pendingPromotions.push(pending);
      queueMessage(
        work,
        "gmail-promotion-request",
        personal(item.ownerAdultId),
        "promotion_request",
        `${triage.privateSummary} Share only this household meaning: “${triage.minimumHouseholdMeaning}”? Reply “share once ${pending.promotionId}” or, only if you want a standing rule for this exact sender in this exact connected inbox and matching ${triage.sourceClass} items, “always share ${pending.promotionId}”.`,
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
): PendingPromotion {
  const sourceKey = calendarSourceKey(item);
  const evidence = calendarEvidence(item);
  const promotionId = stableId("promotion", sourceKey, String(item.revision));
  const temporalPlan = calendarTimingPlan(item, promotionId);
  const sourceMatcher = calendarSourceMatcher(item);
  const proposal = EpisodeProposalSchema.parse({
    episodeId: stableId("episode", promotionId),
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

const CALENDAR_USEFUL_LEAD_MINUTES = 7 * 24 * 60;
const CALENDAR_FINAL_BUFFER_MINUTES = 30;
const CALENDAR_MINIMUM_TIMER_DELAY_MINUTES = 5;

function calendarTimingPlan(item: CalendarEventInboxItem, promotionId: string) {
  const eventAt = Temporal.Instant.from(item.startsAt);
  const observedAt = Temporal.Instant.from(item.occurredAt);
  const earliestUsefulAt = eventAt.subtract({ minutes: CALENDAR_USEFUL_LEAD_MINUTES });
  const lastResponsibleAt = eventAt.subtract({ minutes: CALENDAR_FINAL_BUFFER_MINUTES });
  const candidates = [
    { key: "day_before", at: eventAt.subtract({ hours: 24 }) },
    { key: "two_hours_before", at: eventAt.subtract({ hours: 2 }) },
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
      candidates.push({ key: "next_safe_time", at: catchUpAt });
    }
  }

  return {
    planId: stableId("plan", promotionId),
    version: 1 as const,
    timeZone: item.timeZone,
    event: { kind: "instant" as const, at: eventAt.toString() },
    earliestUseful: { kind: "instant" as const, at: earliestUsefulAt.toString() },
    lastResponsible: { kind: "instant" as const, at: lastResponsibleAt.toString() },
    usefulLeadMinutes: CALENDAR_USEFUL_LEAD_MINUTES,
    preparationMinutes: 0,
    finalBufferMinutes: CALENDAR_FINAL_BUFFER_MINUTES,
    triggers: candidates.map((candidate) => ({
      triggerId: stableId("trigger", promotionId, candidate.key),
      timerId: stableId("timer", promotionId, candidate.key),
      kind: "reminder" as const,
      at: { kind: "instant" as const, at: candidate.at.toString() },
    })),
  };
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
    queueMessage(
      work,
      "calendar-policy-household",
      { kind: "household" },
      "status",
      pending.minimumHouseholdMeaning,
    );
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
      queueMessage(
        work,
        "calendar-private-review",
        personal(item.ownerAdultId),
        "private_review",
        triage.privateSummary,
      );
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
      const pending = calendarMinimumPromotion(item, triage);
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
      item.channel.scope !== "household"
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
      "I'm Florence, an adult-only family Chief of Staff. I can notice family obligations, keep private accounts private by default, and follow shared work through without assigning blame. I will not contact third parties, purchase, book, submit, or expose private information without the required approval. Reply “I consent” in this DM to continue; you can text STOP at any time.",
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
    result.outputContractRef === job.outputContractRef
  );
}

function parseWorkerCommands(result: WorkerResult, job: WorkerJob): WorkerCommand[] | null {
  const commands: WorkerCommand[] = [];
  const evidenceRefs = new Set([...job.evidenceRefs, ...result.evidenceRefs]);
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
) {
  const evidence = unique(commands.flatMap(commandEvidence).map((item) => JSON.stringify(item))).map((item) =>
    EvidenceRefSchema.parse(JSON.parse(item)),
  );
  return WorkerProposalSchema.parse({
    resultId: stableId("worker_result", result.jobId, result.attemptId),
    jobId: DomainWorkerJobIdSchema.parse(result.jobId),
    householdId: result.householdId,
    baseHouseholdVersion: result.baseHouseholdVersion,
    basePolicyVersion: result.policyVersion,
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
    unresolvedQuestions: result.questions,
    diagnostics: { warnings: result.warnings },
  });
}

function rejectWorkerRecord(
  work: Work,
  workerIndex: number,
  receivedAt: string,
  decision: string,
): { status: "rejected"; classification: string } {
  const worker = work.projection.workers[workerIndex];
  if (worker !== undefined) {
    work.projection.workers[workerIndex] = {
      ...worker,
      status: "rejected",
      resultRef: stableId("worker_result_ref", worker.job.jobId, decision),
    };
  }
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "worker_reconciled",
      occurredAt: receivedAt,
      decision,
      containsPrivateData: false,
    }),
  );
  return { status: "rejected", classification: decision };
}

function processWorkerResult(
  work: Work,
  input: Extract<ApplicationInput, { kind: "worker_result" }>,
): { status: "processed" | "rejected"; classification: string } {
  const result = WorkerResultSchema.parse(input.result);
  const workerIndex = work.projection.workers.findIndex((candidate) => candidate.job.jobId === result.jobId);
  const worker = work.projection.workers[workerIndex];
  if (worker === undefined || worker.status !== "queued") {
    return rejectWorkerRecord(work, workerIndex, input.receivedAt, "worker_result_unknown");
  }
  if (!workerIdentityMatches(result, worker.job)) {
    return rejectWorkerRecord(work, workerIndex, input.receivedAt, "worker_result_identity_mismatch");
  }
  const commands = parseWorkerCommands(result, worker.job);
  if (commands === null) {
    return rejectWorkerRecord(work, workerIndex, input.receivedAt, "worker_result_invalid_commands");
  }
  const proposal = workerProposalFromCommands(result, commands, input.receivedAt);
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
  work.projection.workers[workerIndex] = {
    ...worker,
    status: accepted.receipt.disposition === "accepted" ? "reconciled" : "rejected",
    resultRef: proposal.resultId,
  };
  work.audit.push(
    ApplicationAuditEntrySchema.parse({
      kind: "worker_reconciled",
      occurredAt: input.receivedAt,
      decision: accepted.receipt.disposition,
      containsPrivateData: worker.job.scopeGrant.visibility === "personal",
    }),
  );
  return {
    status: statusForDomain(accepted),
    classification: `worker_result:${accepted.receipt.disposition}`,
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
  queueMessage(work, "daily-brief", scope, "daily_brief", briefBody(work.aggregate));
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
): Promise<ApplicationResult> {
  const duplicate = await dependencies.repository.findProcessed(input.householdId, input.idempotencyKey);
  if (duplicate !== null) {
    return duplicate;
  }
  const snapshot = await loadSnapshot(dependencies, input.householdId);
  const record = snapshot.projection.workers.find((candidate) => candidate.job.jobId === input.jobId);
  if (record === undefined || record.status !== "queued") {
    throw new Error(`Worker job is not queued: ${input.jobId}`);
  }
  const options = await dependencies.workerContext.contextFor(record.job, snapshot);
  const result = await dependencies.workerRuntime.run(record.job, options);
  return processApplicationInput(
    dependencies,
    WorkerRoutesSchema.parse(dependencies.workerRoutes ?? DEFAULT_WORKER_ROUTES),
    ApplicationInputSchema.parse({
      kind: "worker_result",
      householdId: input.householdId,
      idempotencyKey: input.idempotencyKey,
      receivedAt: input.requestedAt,
      result,
    }),
  );
}

async function processApplicationInput(
  dependencies: FlorenceApplicationDependencies,
  routes: WorkerRoutes,
  input: ApplicationInput,
): Promise<ApplicationResult> {
  if (input.kind === "run_worker") {
    return runWorker(dependencies, input);
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
    case "calendar_event":
      processed = await processCalendar(work, input, dependencies);
      break;
    case "calendar_event_deleted":
      processed = processCalendarDeleted(work, input);
      break;
    case "worker_result":
      processed = processWorkerResult(work, input);
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
      profileConfirmedAdultIds: [input.initiatorAdultId, input.invitedAdultId],
    });
  }
  return ApplicationProjectionSchema.shape.onboarding.parse({
    phase: "awaiting_initiator_consent",
    initiatorAdultId: input.initiatorAdultId,
    consentedAdultIds: [],
    privateDmAdultIds: [],
    profileConfirmedAdultIds: [],
  });
}

export function createApplicationProjection(
  onboarding: ApplicationProjection["onboarding"],
): ApplicationProjection {
  return ApplicationProjectionSchema.parse({
    onboarding,
    sharedProfile: { facts: [] },
    gmailTriage: [],
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
  return Object.freeze({
    async process(rawInput: unknown) {
      const input = ApplicationInputSchema.parse(rawInput);
      return processApplicationInput(dependencies, routes, input);
    },

    async executeOutbox(rawIntent: unknown, executedAt: string) {
      const intent = ApplicationOutboxIntentSchema.parse(rawIntent);
      const recordedAt = InstantStringSchema.parse(executedAt);
      if (intent.kind === "worker.run") {
        const applicationResult = await processApplicationInput(
          dependencies,
          routes,
          ApplicationInputSchema.parse({
            kind: "run_worker",
            householdId: intent.householdId,
            idempotencyKey: `${intent.idempotencyKey}:result`,
            jobId: intent.job.jobId,
            requestedAt: recordedAt,
          }),
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
