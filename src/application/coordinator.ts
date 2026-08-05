import { createHash } from "node:crypto";
import { Temporal } from "@js-temporal/polyfill";
import {
  type AcceptanceReceipt,
  type AcceptanceResult,
  AdultIdSchema,
  ApprovalIdSchema,
  type DomainChange,
  WorkerJobIdSchema as DomainWorkerJobIdSchema,
  type DurableScope,
  EpisodeProposalSchema,
  EvidenceRefSchema,
  type HouseholdAggregate,
  HouseholdChiefOfStaff,
  type HouseholdSignal,
  HouseholdSignalSchema,
  InstantStringSchema,
  type OutboxIntent,
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
    contentDigest: contentDigest(item.text, ...item.attachmentRefs),
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
    ),
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
  kind: "conversation_classified" | "gmail_triaged",
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
        invitedAdultId === onboarding.initiatorAdultId ||
        !work.aggregate.verifiedAdultIds.includes(invitedAdultId)
      ) {
        return invalidOnboarding(work, item, "That adult cannot be invited to this household.");
      }
      next = { ...onboarding, phase: "awaiting_invitee_consent", invitedAdultId };
      queueMessage(
        work,
        "onboarding-invitee",
        personal(invitedAdultId),
        "onboarding",
        "You were invited to join this adult-only household. Reply privately to accept and consent.",
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
        "The household group is connected. Add the shared routines and time anchors, then both adults can confirm the profile.",
      );
      break;
    }
    case "confirm_profile": {
      if (
        onboarding.phase !== "building_profile" ||
        item.channel.scope !== "household" ||
        item.channel.channelId !== onboarding.groupChannelId ||
        !onboardingParticipants(onboarding).includes(item.senderAdultId)
      ) {
        return invalidOnboarding(work, item, "The shared profile is not ready for that confirmation.");
      }
      const confirmed = unique([...onboarding.profileConfirmedAdultIds, item.senderAdultId]);
      next = {
        ...onboarding,
        phase: confirmed.length === 2 ? "active" : "building_profile",
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

function processActiveConversation(
  work: Work,
  item: ConversationInboxItem,
  classification: ConversationClassification,
  routes: WorkerRoutes,
): "processed" | "rejected" {
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
    case "daily_brief_request":
      queueMessage(work, "brief-request", targetScope(item), "daily_brief", briefBody(work.aggregate));
      return "processed";
    case "approve_promotion":
      return approvePromotion(work, item, classification.promotionId);
    case "decline_promotion":
      return declinePromotion(work, item, classification.promotionId);
  }
}

function approvePromotion(
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
  work.projection.pendingPromotions.splice(index, 1);
  queueMessage(work, "promotion-household", { kind: "household" }, "status", pending.minimumHouseholdMeaning);
  queueMessage(
    work,
    "promotion-confirmed",
    personal(item.senderAdultId),
    "status",
    "Only the approved minimum household meaning was shared.",
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
  queueMessage(
    work,
    "promotion-declined",
    personal(item.senderAdultId),
    "status",
    "The private item will not be shared with the household.",
  );
  return "processed";
}

function sensitivityRank(value: "ordinary" | "sensitive" | "highly_sensitive"): number {
  return { ordinary: 0, sensitive: 1, highly_sensitive: 2 }[value];
}

function matchingSharingPolicy(
  aggregate: HouseholdAggregate,
  adultId: string,
  sourceClass: string,
  sensitivity: "ordinary" | "sensitive" | "highly_sensitive",
) {
  return aggregate.policies.find(
    (policy) =>
      policy.status === "active" &&
      policy.rule.kind === "sharing" &&
      policy.rule.from.adultId === adultId &&
      policy.rule.sourceClass === sourceClass &&
      sensitivityRank(sensitivity) <= sensitivityRank(policy.rule.maximumSensitivity),
  );
}

function minimumPromotion(
  item: GmailInboxItem,
  triage: Extract<ReturnType<typeof GmailTriageResultSchema.parse>, { decision: "propose_family_episode" }>,
): PendingPromotion {
  const evidence = gmailEvidence(item);
  const promotionId = stableId("promotion", item.householdId, item.ownerAdultId, item.messageRef);
  return {
    promotionId,
    ownerAdultId: item.ownerAdultId,
    evidence,
    proposal: EpisodeProposalSchema.parse({
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
    }),
    minimumHouseholdMeaning: triage.minimumHouseholdMeaning,
    createdAt: item.occurredAt,
  };
}

function promoteByPolicy(
  work: Work,
  item: GmailInboxItem,
  pending: PendingPromotion,
  policy: NonNullable<ReturnType<typeof matchingSharingPolicy>>,
): "processed" | "rejected" {
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
  const rules = work.aggregate.policies.flatMap((policy) =>
    policy.status === "active" &&
    policy.rule.kind === "sharing" &&
    policy.rule.from.adultId === item.ownerAdultId
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
    await dependencies.interpreter.triageGmail(item, { activeSharingRules: rules }),
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
      );
      if (policy !== undefined) {
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
        `${triage.privateSummary} Share only this household meaning: “${triage.minimumHouseholdMeaning}”? Reference ${pending.promotionId}.`,
      );
      return { status: "processed", classification: "gmail:promotion_pending" };
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
  const classification = ConversationClassificationSchema.parse(
    await dependencies.interpreter.interpretConversation(item, {
      onboarding: work.projection.onboarding,
      openEpisodes: visibleEpisodes,
      pendingPromotionIds,
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
      "Finish the adult consent and household-group setup before starting household work.",
    );
    return { status: "rejected", classification: "onboarding_required" };
  }
  return {
    status: processActiveConversation(work, item, classification, routes),
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
    } as Omit<HouseholdSignal, "householdId" | "signalId" | "sequence" | "occurredAt" | "actor">,
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
    gmailTriage: [],
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
      if (receipt.status === "succeeded" && receipt.externalAction !== undefined) {
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
