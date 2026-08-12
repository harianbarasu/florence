import { createHash, randomUUID } from "node:crypto";
import {
  type AcceptanceReceipt,
  acceptanceReceiptSchema,
  type ConversationSnapshot,
  type FamilyMemberProfile,
  type GmailCalendarDraft,
  type GmailEvidence,
  gmailEvidenceSchema,
  type HouseholdProfile,
  type HouseholdSignal,
  type HouseholdSnapshot,
  householdSignalSchema,
  householdSnapshotSchema,
  type WorkerInput,
  type WorkerProposal,
  workerInputSchema,
  workerProposalsSchema,
} from "@florence/contracts";
import { type WorkerRuntime, WorkerRuntimeError } from "@florence/runtime";

export type BoundConversation = {
  conversationId: string;
  audience: "private" | "group";
  authorityVersion: number;
  participantSetDigest: string;
  providerConversationId: string;
  authorizedAdultIds: readonly string[];
};

export type HouseholdDomainEvent =
  | {
      type: "household.created";
      householdName: string;
      timeZone: string;
      foundingAdult: FamilyMemberProfile;
      plannedAdult?: FamilyMemberProfile;
    }
  | { type: "family.member.upserted"; member: FamilyMemberProfile }
  | {
      type: "adult.verified";
      adultId: string;
      identitySubjectDigest: string;
      consentVersion: string;
      consentedAt: string;
    }
  | {
      type: "adult.enrollment.issued";
      adultId: string;
      challengeDigest: string;
      expiresAt: string;
      issuedAt: string;
    }
  | { type: "conversation.bound"; conversation: BoundConversation }
  | {
      type: "gmail.candidate.staged";
      candidate: {
        candidateId: string;
        version: 1;
        candidateDigest: string;
        ownerAdultId: string;
        connectionId: string;
        messageId: string;
        threadId: string;
        historyId: string;
        privateSummary: string;
        householdMeaning: string;
        calendarDraft: GmailCalendarDraft | null;
        sourceSignalIds: readonly string[];
        presentedConversationId: string;
        presentedConversationAuthorityVersion: number;
        presentedParticipantSetDigest: string;
      };
      stagedAt: string;
    }
  | {
      type: "gmail.candidate.promoted";
      candidate: { candidateId: string; version: 1; candidateDigest: string };
      promotedByAdultId: string;
      sourceSignalIds: readonly string[];
      promotedAt: string;
    }
  | {
      type: "gmail.calendar.approved";
      candidate: { candidateId: string; version: 1; candidateDigest: string };
      approvedByAdultId: string;
      conversationId: string;
      conversationAuthorityVersion: number;
      sourceSignalIds: readonly string[];
      approvedAt: string;
    }
  | {
      type: "memory.remembered";
      memoryId: string;
      statement: string;
      scope: "household" | "personal";
      personId: string | null;
      sourceSignalIds: readonly string[];
      supersedesMemoryId: string | null;
      recordedAt: string;
    }
  | {
      type: "episode.proposed";
      episodeId: string;
      title: string;
      outcome: string;
      dueAt: string | null;
      suggestedOwnerAdultId: string | null;
      sourceSignalIds: readonly string[];
      occurredAt: string;
    }
  | {
      type: "episode.owner.changed";
      episodeId: string;
      ownerAdultId: string | null;
      changedByAdultId: string;
      sourceSignalIds: readonly string[];
      occurredAt: string;
    }
  | {
      type: "episode.updated";
      episodeId: string;
      outcome: string;
      dueAt: string | null;
      updatedByAdultId: string;
      sourceSignalIds: readonly string[];
      occurredAt: string;
    }
  | {
      type: "episode.cancelled";
      episodeId: string;
      cancelledByAdultId: string;
      reason: string;
      sourceSignalIds: readonly string[];
      occurredAt: string;
    }
  | {
      type: "episode.completed";
      episodeId: string;
      completedByAdultId: string;
      result: string;
      sourceSignalIds: readonly string[];
      occurredAt: string;
    };

export type PersistedHouseholdEvent = HouseholdDomainEvent & {
  id: string;
  householdId: string;
  signalId: string;
  version: number;
};

export type ClaimedSignal = {
  signal: HouseholdSignal;
  attempt: number;
  leaseOwner: string;
};

export interface PersonalSourceReader {
  readGmailMessage(input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    messageId: string;
    threadId: string;
    historyId: string;
  }): Promise<GmailEvidence>;
}

export type LinqEnrollmentRedemptionRequest = {
  signalId: string;
  idempotencyKey: string;
  occurredAt: string;
  challengeDigest: string;
  identitySubjectDigest: string;
  consentVersion: string;
  consentedAt: string;
  providerConversationId: string;
};

export type LinqEnrollmentRedemptionReceipt = AcceptanceReceipt & {
  adultId: string;
  conversationId: string;
};

export type LinqGroupBootstrapRequest = {
  bindingSignalId: string;
  bindingIdempotencyKey: string;
  messageSignalId: string;
  messageIdempotencyKey: string;
  occurredAt: string;
  providerConversationId: string;
  participantIdentityDigests: readonly string[];
  senderIdentitySubjectDigest: string;
  text: string;
  providerEventId: string;
  providerMessageId: string;
};

export type LinqGroupBootstrapReceipt = AcceptanceReceipt & {
  conversationId: string;
};

export type HouseholdIngress =
  | HouseholdSignal
  | { command: "linq.enrollment.redeem"; input: LinqEnrollmentRedemptionRequest }
  | { command: "linq.group.bootstrap"; input: LinqGroupBootstrapRequest };

export type HouseholdIngressReceipt =
  | AcceptanceReceipt
  | LinqEnrollmentRedemptionReceipt
  | LinqGroupBootstrapReceipt
  | null;

export type EffectDraft =
  | {
      id: string;
      idempotencyKey: string;
      kind: "conversation.message";
      conversationId: string;
      conversationAuthorityVersion: number;
      participantSetDigest: string;
      episodeId: string | null;
      payload: { text: string };
    }
  | {
      id: string;
      idempotencyKey: string;
      kind: "google.calendar.create";
      connectionId: string;
      ownerAdultId: string;
      actionId: string;
      approvalDigest: string;
      candidateId: string;
      candidateVersion: 1;
      candidateDigest: string;
      payload: GmailCalendarDraft;
    };

export type TimerDraft = {
  id: string;
  idempotencyKey: string;
  episodeId: string;
  episodeVersion: number;
  scheduledFor: string;
};

export type HouseholdCommit = {
  signalId: string;
  householdId: string;
  leaseOwner: string;
  expectedVersion: number;
  events: readonly HouseholdDomainEvent[];
  effects: readonly EffectDraft[];
  timers: readonly TimerDraft[];
  cancelEpisodeIds: readonly string[];
  firedTimerId: string | null;
  effectReceipt: {
    effectId: string;
    episodeId: string | null;
    status: "committed" | "failed";
    providerReceiptId: string | null;
    detail: string | null;
    occurredAt: string;
  } | null;
};

export interface FlorenceRepository {
  accept(signal: HouseholdSignal, acceptedAt: string): Promise<AcceptanceReceipt>;
  claimNext(input: { leaseOwner: string; now: string; leaseUntil: string }): Promise<ClaimedSignal | null>;
  loadEvents(householdId: string): Promise<readonly PersistedHouseholdEvent[]>;
  loadRecentConversationTurns(
    householdId: string,
    conversationId: string,
  ): Promise<ConversationSnapshot["recentTurns"]>;
  loadDeliberation(signalId: string): Promise<{
    inputDigest: string;
    proposals: readonly WorkerProposal[];
  } | null>;
  saveDeliberation(input: {
    signalId: string;
    leaseOwner: string;
    inputDigest: string;
    proposals: readonly WorkerProposal[];
  }): Promise<void>;
  commit(input: HouseholdCommit): Promise<void>;
  fail(input: { signalId: string; leaseOwner: string; retryAt: string | null; error: string }): Promise<void>;
  redeemLinqEnrollment(
    input: LinqEnrollmentRedemptionRequest,
    acceptedAt: string,
  ): Promise<LinqEnrollmentRedemptionReceipt | null>;
  bootstrapLinqHouseholdGroup(
    input: LinqGroupBootstrapRequest,
    acceptedAt: string,
  ): Promise<LinqGroupBootstrapReceipt | null>;
}

export type FlorenceWorkerRepository = Omit<
  FlorenceRepository,
  "redeemLinqEnrollment" | "bootstrapLinqHouseholdGroup"
>;

type GmailCandidate = Extract<HouseholdDomainEvent, { type: "gmail.candidate.staged" }>["candidate"];

type HouseholdState = {
  id: string;
  version: number;
  name: string | null;
  timeZone: string | null;
  members: Map<string, FamilyMemberProfile>;
  memberSourceSignalIds: Map<string, string[]>;
  adultVerifications: Map<
    string,
    { identitySubjectDigest: string; consentVersion: string; consentedAt: string }
  >;
  adultEnrollments: Map<string, { challengeDigest: string; expiresAt: string; issuedAt: string }>;
  conversations: Map<string, BoundConversation>;
  gmailCandidates: Map<
    string,
    GmailCandidate & {
      stagedAt: string;
      promoted: boolean;
      calendarApproved: boolean;
    }
  >;
  memories: Map<string, Extract<HouseholdDomainEvent, { type: "memory.remembered" }> & { active: boolean }>;
  episodes: Map<
    string,
    {
      id: string;
      title: string;
      outcome: string;
      dueAt: string | null;
      suggestedOwnerAdultId: string | null;
      ownerAdultId: string | null;
      status: "proposed" | "owned" | "completed" | "cancelled";
      sourceSignalIds: string[];
      version: number;
      updatedAt: string;
      result: string | null;
    }
  >;
};

export class HouseholdChiefOfStaff {
  constructor(
    private readonly repository: FlorenceWorkerRepository &
      Partial<Pick<FlorenceRepository, "redeemLinqEnrollment" | "bootstrapLinqHouseholdGroup">>,
    private readonly runtime: WorkerRuntime,
    private readonly now: () => Date = () => new Date(),
    private readonly personalSourceReader: PersonalSourceReader | null = null,
  ) {}

  async accept(input: HouseholdSignal): Promise<AcceptanceReceipt>;
  async accept(input: {
    command: "linq.enrollment.redeem";
    input: LinqEnrollmentRedemptionRequest;
  }): Promise<LinqEnrollmentRedemptionReceipt | null>;
  async accept(input: {
    command: "linq.group.bootstrap";
    input: LinqGroupBootstrapRequest;
  }): Promise<LinqGroupBootstrapReceipt | null>;
  async accept(input: HouseholdIngress): Promise<HouseholdIngressReceipt>;
  async accept(input: HouseholdIngress): Promise<HouseholdIngressReceipt> {
    if ("command" in input) {
      if (input.command === "linq.enrollment.redeem") {
        if (!this.repository.redeemLinqEnrollment) {
          throw new Error("This Florence process cannot redeem Linq enrollment");
        }
        return this.repository.redeemLinqEnrollment(input.input, this.now().toISOString());
      }
      if (!this.repository.bootstrapLinqHouseholdGroup) {
        throw new Error("This Florence process cannot bootstrap a Linq household group");
      }
      return this.repository.bootstrapLinqHouseholdGroup(input.input, this.now().toISOString());
    }
    const signal = householdSignalSchema.parse(input);
    return acceptanceReceiptSchema.parse(await this.repository.accept(signal, this.now().toISOString()));
  }

  async processNext(leaseOwner: string = randomUUID()): Promise<boolean> {
    const claimed = await this.repository.claimNext({
      leaseOwner,
      now: this.now().toISOString(),
      leaseUntil: new Date(this.now().getTime() + 30_000).toISOString(),
    });
    if (!claimed) return false;

    try {
      const persistedEvents = await this.repository.loadEvents(claimed.signal.householdId);
      const state = reduceHousehold(claimed.signal.householdId, persistedEvents);
      const commit = await this.plan(claimed, state);
      await this.repository.commit(commit);
      return true;
    } catch (error) {
      const canRetry = !(error instanceof WorkerRuntimeError) || error.retryable;
      const retryAt =
        canRetry && claimed.attempt < 5 ? new Date(this.now().getTime() + 5_000).toISOString() : null;
      await this.repository.fail({
        signalId: claimed.signal.signalId,
        leaseOwner: claimed.leaseOwner,
        retryAt,
        error: error instanceof Error ? error.message : "Unknown Florence processing failure",
      });
      throw error;
    }
  }

  async profile(householdId: string): Promise<HouseholdProfile | null> {
    const state = reduceHousehold(householdId, await this.repository.loadEvents(householdId));
    if (!state.name || !state.timeZone) return null;
    return {
      householdId: state.id,
      name: state.name,
      timeZone: state.timeZone,
      version: state.version,
      onboardingComplete:
        identityBoundAdults(state).length === 2 &&
        [...state.conversations.values()].some((conversation) => conversation.audience === "group"),
      members: [...state.members.values()].map((member) => ({
        ...member,
        ...(member.activities ? { activities: [...member.activities] } : {}),
      })),
      identityBoundAdultIds: identityBoundAdults(state).map((adult) => adult.id),
    };
  }

  private async plan(claimed: ClaimedSignal, state: HouseholdState): Promise<HouseholdCommit> {
    const signal = claimed.signal;
    const events: HouseholdDomainEvent[] = [];
    const effects: EffectDraft[] = [];
    const timers: TimerDraft[] = [];
    const cancelEpisodeIds: string[] = [];
    let firedTimerId: string | null = null;
    let effectReceipt: HouseholdCommit["effectReceipt"] = null;

    switch (signal.type) {
      case "household.created": {
        if (state.name) throw new Error("Household already exists");
        events.push({
          type: "household.created",
          householdName: signal.name,
          timeZone: signal.timeZone,
          foundingAdult: {
            id: signal.foundingAdult.id,
            kind: "adult",
            role: "steward",
            displayName: signal.foundingAdult.displayName,
            relationship: "Parent",
            status: "verified",
          },
          ...(signal.plannedAdult
            ? {
                plannedAdult: {
                  id: signal.plannedAdult.id,
                  kind: "adult" as const,
                  role: signal.plannedAdult.role,
                  displayName: signal.plannedAdult.displayName,
                  relationship: signal.plannedAdult.relationship,
                  status: "planned" as const,
                },
              }
            : {}),
        });
        break;
      }
      case "family.member.upserted": {
        requireSteward(state, signal.actorAdultId);
        const existing = state.members.get(signal.member.id);
        if (existing && existing.kind !== signal.member.kind) {
          throw new Error("A family member cannot change between adult and child");
        }
        if (existing && existing.status !== signal.status) {
          throw new Error("Family member authority changes require their dedicated identity flow");
        }
        if (existing?.status === "verified" && existing.role !== signal.member.role) {
          throw new Error("A verified adult role change requires renewed authority");
        }
        if (signal.member.kind === "child" && signal.status !== "represented") {
          throw new Error("A child is represented by the household, never a verified account");
        }
        if (signal.member.kind === "adult" && signal.status === "represented") {
          throw new Error("An adult must be planned or independently verified");
        }
        events.push({
          type: "family.member.upserted",
          member: {
            ...signal.member,
            ...(signal.member.activities ? { activities: [...signal.member.activities] } : {}),
            status: signal.status,
          },
        });
        break;
      }
      case "adult.enrollment.issued": {
        requireSteward(state, signal.actorAdultId);
        const adult = state.members.get(signal.adultId);
        if (adult?.kind !== "adult" || (adult.status !== "planned" && adult.status !== "verified")) {
          throw new Error("Enrollment must target one household adult");
        }
        if (state.adultVerifications.has(signal.adultId)) {
          throw new Error("An identity-bound adult does not need another enrollment code");
        }
        const issuedAt = new Date(signal.occurredAt).getTime();
        const expiresAt = new Date(signal.expiresAt).getTime();
        if (expiresAt <= issuedAt || expiresAt - issuedAt > 48 * 60 * 60 * 1_000) {
          throw new Error("An enrollment code must expire within 48 hours");
        }
        events.push({
          type: "adult.enrollment.issued",
          adultId: signal.adultId,
          challengeDigest: signal.challengeDigest,
          expiresAt: signal.expiresAt,
          issuedAt: signal.occurredAt,
        });
        break;
      }
      case "adult.enrollment.redeemed": {
        const enrollment = state.adultEnrollments.get(signal.adultId);
        const adult = state.members.get(signal.adultId);
        if (
          adult?.kind !== "adult" ||
          !enrollment ||
          enrollment.challengeDigest !== signal.challengeDigest ||
          new Date(enrollment.expiresAt).getTime() < new Date(signal.occurredAt).getTime() ||
          state.adultVerifications.has(signal.adultId)
        ) {
          throw new Error("The Linq enrollment challenge is not current");
        }
        if (
          [...state.adultVerifications.values()].some(
            (verification) => verification.identitySubjectDigest === signal.identitySubjectDigest,
          )
        ) {
          throw new Error("One provider identity cannot verify two household adults");
        }
        if (
          [...state.conversations.values()].some(
            (conversation) => conversation.providerConversationId === signal.providerConversationId,
          )
        ) {
          throw new Error("The enrollment conversation is already bound");
        }
        const conversation: BoundConversation = {
          conversationId: signal.conversationId,
          audience: "private",
          authorityVersion: 1,
          participantSetDigest: digestStrings([signal.adultId]),
          providerConversationId: signal.providerConversationId,
          authorizedAdultIds: [signal.adultId],
        };
        events.push(
          {
            type: "adult.verified",
            adultId: signal.adultId,
            identitySubjectDigest: signal.identitySubjectDigest,
            consentVersion: signal.consentVersion,
            consentedAt: signal.consentedAt,
          },
          { type: "conversation.bound", conversation },
        );
        effects.push(
          messageEffect(
            signal,
            conversation,
            null,
            `You’re connected to Florence as ${adult.displayName}. Your private conversation stays yours.`,
            "enrollment-confirmed",
          ),
        );
        break;
      }
      case "conversation.bound": {
        requireSteward(state, signal.actorAdultId);
        const existing = state.conversations.get(signal.conversationId);
        const expectedVersion = (existing?.authorityVersion ?? 0) + 1;
        if (signal.authorityVersion !== expectedVersion) {
          throw new Error(`Conversation authority must advance to version ${expectedVersion}`);
        }
        if (existing && existing.providerConversationId !== signal.providerConversationId) {
          throw new Error("A provider conversation cannot be rebound under a different identity");
        }
        if (
          [...state.conversations.values()].some(
            (conversation) =>
              conversation.conversationId !== signal.conversationId &&
              conversation.providerConversationId === signal.providerConversationId,
          )
        ) {
          throw new Error("A provider conversation cannot authorize two household conversations");
        }
        const adults = signal.authorizedAdultIds.map((id) => state.members.get(id));
        if (
          adults.some(
            (adult) =>
              adult?.kind !== "adult" ||
              adult.status !== "verified" ||
              !state.adultVerifications.has(adult.id),
          )
        ) {
          throw new Error("Conversation participants must have bound verified household identities");
        }
        if (signal.audience === "group" && new Set(signal.authorizedAdultIds).size !== 2) {
          throw new Error("The founding-family group requires exactly two verified adults");
        }
        if (signal.audience === "private" && new Set(signal.authorizedAdultIds).size !== 1) {
          throw new Error("A private conversation requires exactly one verified adult");
        }
        if (signal.participantSetDigest !== digestStrings(signal.authorizedAdultIds)) {
          throw new Error("Conversation participant authority digest does not match its adults");
        }
        events.push({
          type: "conversation.bound",
          conversation: {
            conversationId: signal.conversationId,
            audience: signal.audience,
            authorityVersion: signal.authorityVersion,
            participantSetDigest: signal.participantSetDigest,
            providerConversationId: signal.providerConversationId,
            authorizedAdultIds: [...signal.authorizedAdultIds],
          },
        });
        break;
      }
      case "conversation.message": {
        const snapshot = await this.workerSnapshot(state, signal);
        const input = workerInputSchema.parse({ signal, snapshot });
        const proposals = await this.deliberateOnce(claimed, input);
        this.reconcileConversation(signal, state, proposals, events, effects, timers, cancelEpisodeIds);
        break;
      }
      case "gmail.message.changed": {
        if (!this.personalSourceReader) throw new Error("Personal Gmail retrieval is not configured");
        const conversation = requirePrivateConversationForAdult(state, signal.ownerAdultId);
        const evidence = gmailEvidenceSchema.parse(
          await this.personalSourceReader.readGmailMessage({
            householdId: signal.householdId,
            ownerAdultId: signal.ownerAdultId,
            connectionId: signal.connectionId,
            messageId: signal.messageId,
            threadId: signal.threadId,
            historyId: signal.historyId,
          }),
        );
        if (
          evidence.messageId !== signal.messageId ||
          evidence.threadId !== signal.threadId ||
          evidence.historyId !== signal.historyId
        ) {
          throw new Error("Retrieved Gmail evidence does not match the immutable source signal");
        }
        const snapshot = await this.workerSnapshot(state, signal, conversation);
        const input = workerInputSchema.parse({ signal, snapshot, gmailEvidence: evidence });
        const proposals = await this.deliberateOnce(claimed, input);
        this.reconcileGmailSource(signal, conversation, proposals, events, effects);
        break;
      }
      case "timer.fired": {
        const episode = state.episodes.get(signal.episodeId);
        if (episode?.status !== "owned" || episode.version !== signal.episodeVersion) break;
        firedTimerId = signal.timerId;
        const conversation = requireHouseholdGroup(state);
        effects.push(
          messageEffect(
            signal,
            conversation,
            episode.id,
            `${episode.title} is still open. Is it handled, or should we reassign it?`,
            "reminder",
          ),
        );
        break;
      }
      case "effect.receipt": {
        effectReceipt = {
          effectId: signal.effectId,
          episodeId: signal.episodeId,
          status: signal.status,
          providerReceiptId: signal.providerReceiptId,
          detail: signal.detail,
          occurredAt: signal.occurredAt,
        };
        break;
      }
    }

    return {
      signalId: signal.signalId,
      householdId: signal.householdId,
      leaseOwner: claimed.leaseOwner,
      expectedVersion: state.version,
      events,
      effects,
      timers,
      cancelEpisodeIds,
      firedTimerId,
      effectReceipt,
    };
  }

  private async deliberateOnce(
    claimed: ClaimedSignal,
    input: WorkerInput,
  ): Promise<readonly WorkerProposal[]> {
    const inputDigest = createHash("sha256").update(canonicalJson(input)).digest("hex");
    const persisted = await this.repository.loadDeliberation(claimed.signal.signalId);
    if (persisted) {
      if (persisted.inputDigest !== inputDigest) {
        throw new Error("The persisted deliberation does not match the current household context");
      }
      return workerProposalsSchema.parse(persisted.proposals);
    }

    const proposals = workerProposalsSchema.parse(await this.runtime.deliberate(input));
    await this.repository.saveDeliberation({
      signalId: claimed.signal.signalId,
      leaseOwner: claimed.leaseOwner,
      inputDigest,
      proposals,
    });
    return proposals;
  }

  private async workerSnapshot(
    state: HouseholdState,
    signal: Extract<HouseholdSignal, { type: "conversation.message" | "gmail.message.changed" }>,
    resolvedConversation?: BoundConversation,
  ): Promise<HouseholdSnapshot> {
    const conversation =
      resolvedConversation ??
      (signal.type === "conversation.message"
        ? requireSignalConversation(state, signal)
        : requirePrivateConversationForAdult(state, signal.ownerAdultId));
    const viewerAdultId = signal.type === "conversation.message" ? signal.senderAdultId : signal.ownerAdultId;
    const adults = identityBoundAdults(state);
    if (
      !state.timeZone ||
      adults.length === 0 ||
      adults.length > 2 ||
      (conversation.audience === "group" && adults.length !== 2)
    ) {
      throw new Error("Florence requires the current verified adult authority for this audience");
    }
    const visibleGmailCandidates =
      signal.type === "conversation.message" && conversation.audience === "private"
        ? [...state.gmailCandidates.values()].filter(
            (candidate) =>
              candidate.ownerAdultId === viewerAdultId &&
              (!candidate.promoted || (candidate.calendarDraft !== null && !candidate.calendarApproved)),
          )
        : [];
    const approvalCandidates = calendarApprovalCandidates(state, viewerAdultId, conversation);
    return householdSnapshotSchema.parse({
      householdId: state.id,
      timeZone: state.timeZone,
      asOf: signal.occurredAt,
      members: [...state.members.values()].map((member) => ({
        ...member,
        ...(member.aliases ? { aliases: [...member.aliases] } : {}),
        ...(member.activities ? { activities: [...member.activities] } : {}),
        sourceSignalIds: [...(state.memberSourceSignalIds.get(member.id) ?? [])],
      })),
      conversation: {
        id: conversation.conversationId,
        audience: conversation.audience,
        authorityVersion: conversation.authorityVersion,
        participantSetDigest: conversation.participantSetDigest,
        authorizedAdultIds: [...conversation.authorizedAdultIds],
        recentTurns: await this.repository.loadRecentConversationTurns(state.id, conversation.conversationId),
      },
      memories: visibleMemories(state, conversation.audience, viewerAdultId).map(([id, memory]) => ({
        id,
        statement: memory.statement,
        sourceSignalIds: [...memory.sourceSignalIds],
        supersedesMemoryId: memory.supersedesMemoryId,
        recordedAt: memory.recordedAt,
      })),
      openEpisodes: [...state.episodes.values()]
        .filter((episode) => episode.status === "proposed" || episode.status === "owned")
        .map((episode) => ({
          id: episode.id,
          title: episode.title,
          outcome: episode.outcome,
          dueAt: episode.dueAt,
          status: episode.status,
          ownerAdultId: episode.ownerAdultId,
          sourceSignalIds: [...episode.sourceSignalIds],
          version: episode.version,
          updatedAt: episode.updatedAt,
        })),
      privateGmailCandidates: visibleGmailCandidates.map(projectPrivateGmailCandidate),
      privateCalendarApprovalCandidate:
        approvalCandidates.length === 1 && approvalCandidates[0]
          ? projectPrivateGmailCandidate(approvalCandidates[0])
          : null,
    });
  }

  private reconcileGmailSource(
    signal: Extract<HouseholdSignal, { type: "gmail.message.changed" }>,
    conversation: BoundConversation,
    proposals: readonly WorkerProposal[],
    events: HouseholdDomainEvent[],
    effects: EffectDraft[],
  ): void {
    if (proposals.length !== 1) throw new Error("Gmail triage must return exactly one bounded proposal");
    const proposal = proposals[0];
    if (proposal?.type === "ignore") return;
    if (proposal?.type !== "stage_gmail_candidate") {
      throw new Error("A private Gmail source may only be ignored or staged for its owner");
    }
    if (proposal.sourceSignalIds.length !== 1 || proposal.sourceSignalIds[0] !== signal.signalId) {
      throw new Error("A Gmail candidate must cite only its current private source signal");
    }
    const candidateId = randomUUID();
    const candidateDigest = gmailCandidateDigest({
      candidateId,
      ownerAdultId: signal.ownerAdultId,
      connectionId: signal.connectionId,
      messageId: signal.messageId,
      threadId: signal.threadId,
      historyId: signal.historyId,
      privateSummary: proposal.privateSummary,
      householdMeaning: proposal.householdMeaning,
      calendarDraft: proposal.calendarDraft,
      sourceSignalIds: proposal.sourceSignalIds,
      presentedConversationId: conversation.conversationId,
      presentedConversationAuthorityVersion: conversation.authorityVersion,
      presentedParticipantSetDigest: conversation.participantSetDigest,
    });
    const candidate: GmailCandidate = {
      candidateId,
      version: 1,
      candidateDigest,
      ownerAdultId: signal.ownerAdultId,
      connectionId: signal.connectionId,
      messageId: signal.messageId,
      threadId: signal.threadId,
      historyId: signal.historyId,
      privateSummary: proposal.privateSummary,
      householdMeaning: proposal.householdMeaning,
      calendarDraft: proposal.calendarDraft,
      sourceSignalIds: [...proposal.sourceSignalIds],
      presentedConversationId: conversation.conversationId,
      presentedConversationAuthorityVersion: conversation.authorityVersion,
      presentedParticipantSetDigest: conversation.participantSetDigest,
    };
    events.push({
      type: "gmail.candidate.staged",
      candidate,
      stagedAt: signal.occurredAt,
    });
    effects.push(
      messageEffect(
        signal,
        conversation,
        null,
        gmailCandidatePrivateMessage(candidate, conversation),
        "gmail-candidate-staged",
      ),
    );
  }

  private reconcileConversation(
    signal: Extract<HouseholdSignal, { type: "conversation.message" }>,
    state: HouseholdState,
    proposals: readonly WorkerProposal[],
    events: HouseholdDomainEvent[],
    effects: EffectDraft[],
    timers: TimerDraft[],
    cancelEpisodeIds: string[],
  ): void {
    const conversation = requireSignalConversation(state, signal);
    requireSingleConversationCapability(proposals);
    for (const proposal of proposals) {
      ensureEvidence(proposal, signal, state);
      switch (proposal.type) {
        case "ignore":
          break;
        case "ask":
          if (proposal.episodeId) requireOpenEpisode(state, proposal.episodeId);
          effects.push(messageEffect(signal, conversation, proposal.episodeId, proposal.text, "ask"));
          break;
        case "respond":
          effects.push(messageEffect(signal, conversation, null, proposal.text, "respond"));
          break;
        case "remember": {
          if (proposal.supersedesMemoryId) {
            const superseded = state.memories.get(proposal.supersedesMemoryId);
            const expectedScope = signal.audience === "group" ? "household" : "personal";
            if (
              !superseded?.active ||
              superseded.scope !== expectedScope ||
              (expectedScope === "personal" && superseded.personId !== signal.senderAdultId)
            ) {
              throw new Error("Memory correction must target a current memory in this audience");
            }
          }
          events.push({
            type: "memory.remembered",
            memoryId: randomUUID(),
            statement: proposal.statement,
            scope: signal.audience === "group" ? "household" : "personal",
            personId: signal.audience === "private" ? signal.senderAdultId : null,
            sourceSignalIds: [...proposal.sourceSignalIds],
            supersedesMemoryId: proposal.supersedesMemoryId,
            recordedAt: signal.occurredAt,
          });
          break;
        }
        case "stage_gmail_candidate":
          throw new Error("Gmail candidates can only be staged from private source evidence");
        case "promote_gmail_candidate": {
          requireCurrentMessageEvidence(proposal, signal);
          if (signal.audience !== "private") {
            throw new Error("A Gmail candidate can only be promoted by its owner in private");
          }
          const candidate = state.gmailCandidates.get(proposal.candidateId);
          if (
            !candidate ||
            candidate.promoted ||
            candidate.ownerAdultId !== signal.senderAdultId ||
            candidate.version !== proposal.version ||
            candidate.candidateDigest !== proposal.candidateDigest ||
            candidate.sourceSignalIds.some((sourceId) => !proposal.sourceSignalIds.includes(sourceId))
          ) {
            throw new Error("The private Gmail candidate is not current for this adult");
          }
          const group = requireHouseholdGroup(state);
          const sourceSignalIds = [...new Set([...candidate.sourceSignalIds, ...proposal.sourceSignalIds])];
          const episodeId = randomUUID();
          events.push(
            {
              type: "gmail.candidate.promoted",
              candidate: {
                candidateId: candidate.candidateId,
                version: candidate.version,
                candidateDigest: candidate.candidateDigest,
              },
              promotedByAdultId: signal.senderAdultId,
              sourceSignalIds,
              promotedAt: signal.occurredAt,
            },
            {
              type: "episode.proposed",
              episodeId,
              title: candidate.householdMeaning,
              outcome: candidate.householdMeaning,
              dueAt: null,
              suggestedOwnerAdultId: null,
              sourceSignalIds,
              occurredAt: signal.occurredAt,
            },
          );
          effects.push(
            messageEffect(signal, group, episodeId, candidate.householdMeaning, "gmail-candidate-promoted"),
            messageEffect(
              signal,
              conversation,
              episodeId,
              proposal.responseText,
              "gmail-promotion-confirmed",
            ),
          );
          break;
        }
        case "approve_gmail_calendar": {
          requireCurrentMessageEvidence(proposal, signal);
          if (signal.audience !== "private") {
            throw new Error("Calendar consent is only valid in its owner's private conversation");
          }
          const candidates = calendarApprovalCandidates(state, signal.senderAdultId, conversation);
          const candidate = candidates[0];
          if (
            !candidate ||
            candidates.length !== 1 ||
            proposal.candidateId !== candidate.candidateId ||
            proposal.version !== candidate.version ||
            proposal.candidateDigest !== candidate.candidateDigest
          ) {
            throw new Error("Calendar consent requires exactly one current private draft");
          }
          const requiredEvidence = [...new Set([...candidate.sourceSignalIds, signal.signalId])];
          if (!sameStringSet(proposal.sourceSignalIds, requiredEvidence)) {
            throw new Error("Calendar consent must cite its exact draft and current private reply");
          }
          const actionId = deterministicUuid(
            `google.calendar.create:${candidate.candidateId}:${candidate.version}:${candidate.candidateDigest}`,
          );
          events.push({
            type: "gmail.calendar.approved",
            candidate: {
              candidateId: candidate.candidateId,
              version: candidate.version,
              candidateDigest: candidate.candidateDigest,
            },
            approvedByAdultId: signal.senderAdultId,
            conversationId: conversation.conversationId,
            conversationAuthorityVersion: conversation.authorityVersion,
            sourceSignalIds: requiredEvidence,
            approvedAt: signal.occurredAt,
          });
          effects.push({
            id: deterministicUuid(`effect:${actionId}`),
            idempotencyKey: `google-calendar:${actionId}`,
            kind: "google.calendar.create",
            connectionId: candidate.connectionId,
            ownerAdultId: candidate.ownerAdultId,
            actionId,
            approvalDigest: createHash("sha256")
              .update(
                canonicalJson({
                  actionId,
                  connectionId: candidate.connectionId,
                  ownerAdultId: candidate.ownerAdultId,
                  approvalSignalId: signal.signalId,
                  conversationId: conversation.conversationId,
                  conversationAuthorityVersion: conversation.authorityVersion,
                  participantSetDigest: conversation.participantSetDigest,
                  candidateId: candidate.candidateId,
                  candidateVersion: candidate.version,
                  candidateDigest: candidate.candidateDigest,
                  calendarDraft: candidate.calendarDraft,
                }),
              )
              .digest("hex"),
            candidateId: candidate.candidateId,
            candidateVersion: candidate.version,
            candidateDigest: candidate.candidateDigest,
            payload: candidate.calendarDraft,
          });
          break;
        }
        case "propose_episode": {
          requireCurrentMessageEvidence(proposal, signal);
          if (signal.audience !== "group") {
            throw new Error("Private content cannot create a household episode without promotion");
          }
          if (
            proposal.suggestedOwnerAdultId &&
            !verifiedAdults(state).some((adult) => adult.id === proposal.suggestedOwnerAdultId)
          ) {
            throw new Error("A suggested owner must be a verified household adult");
          }
          const episodeId = randomUUID();
          events.push({
            type: "episode.proposed",
            episodeId,
            title: proposal.title,
            outcome: proposal.outcome,
            dueAt: proposal.dueAt ?? null,
            suggestedOwnerAdultId: proposal.suggestedOwnerAdultId ?? null,
            sourceSignalIds: [...proposal.sourceSignalIds],
            occurredAt: signal.occurredAt,
          });
          effects.push(
            messageEffect(
              signal,
              conversation,
              episodeId,
              proposal.responseText ?? `${proposal.title} needs an owner. Who can take it?`,
              "episode-proposed",
            ),
          );
          break;
        }
        case "set_episode_owner": {
          requireCurrentMessageEvidence(proposal, signal);
          if (signal.audience !== "group") {
            throw new Error("Household episode ownership must change in the household group");
          }
          const episode = requireOpenEpisode(state, proposal.episodeId);
          if (proposal.ownerAdultId !== null && proposal.ownerAdultId !== signal.senderAdultId) {
            throw new Error("An adult may only take episode ownership for themselves");
          }
          if (
            proposal.ownerAdultId === null &&
            (episode.status !== "owned" || episode.ownerAdultId !== signal.senderAdultId)
          ) {
            throw new Error("Only the current owner may decline an owned episode");
          }
          if (proposal.ownerAdultId !== null && episode.ownerAdultId === proposal.ownerAdultId) {
            throw new Error("Episode ownership is already current");
          }
          const wasOwned = episode.status === "owned";
          const event = {
            type: "episode.owner.changed",
            episodeId: episode.id,
            ownerAdultId: proposal.ownerAdultId,
            changedByAdultId: signal.senderAdultId,
            sourceSignalIds: [...proposal.sourceSignalIds],
            occurredAt: signal.occurredAt,
          } satisfies HouseholdDomainEvent;
          events.push(event);
          applyHouseholdEvent(state, event);
          if (wasOwned || proposal.ownerAdultId === null) {
            cancelEpisodeFollowUps(episode.id, timers, cancelEpisodeIds);
          }
          if (proposal.ownerAdultId !== null) {
            scheduleEpisodeFollowUp(signal, requireOpenEpisode(state, episode.id), timers);
          }
          const ownerName = state.members.get(signal.senderAdultId)?.displayName ?? "You";
          effects.push(
            messageEffect(
              signal,
              conversation,
              episode.id,
              proposal.responseText ??
                (proposal.ownerAdultId === null
                  ? `${ownerName} passed this back for reassignment.`
                  : `${ownerName} owns this.`),
              "episode-owner-changed",
            ),
          );
          break;
        }
        case "update_episode": {
          requireCurrentMessageEvidence(proposal, signal);
          if (signal.audience !== "group") {
            throw new Error("A household episode must be updated in the household group");
          }
          const episode = requireOpenEpisode(state, proposal.episodeId);
          if (episode.status === "owned" && episode.ownerAdultId !== signal.senderAdultId) {
            throw new Error("Only the current owner may update an owned episode");
          }
          if (episode.outcome === proposal.outcome && episode.dueAt === proposal.dueAt) {
            throw new Error("Episode update must change its outcome or due time");
          }
          const wasOwned = episode.status === "owned";
          const event = {
            type: "episode.updated",
            episodeId: episode.id,
            outcome: proposal.outcome,
            dueAt: proposal.dueAt,
            updatedByAdultId: signal.senderAdultId,
            sourceSignalIds: [...proposal.sourceSignalIds],
            occurredAt: signal.occurredAt,
          } satisfies HouseholdDomainEvent;
          events.push(event);
          applyHouseholdEvent(state, event);
          if (wasOwned) {
            cancelEpisodeFollowUps(episode.id, timers, cancelEpisodeIds);
            scheduleEpisodeFollowUp(signal, requireOpenEpisode(state, episode.id), timers);
          }
          effects.push(
            messageEffect(
              signal,
              conversation,
              episode.id,
              proposal.responseText ?? `Updated ${episode.title}.`,
              "episode-updated",
            ),
          );
          break;
        }
        case "cancel_episode": {
          requireCurrentMessageEvidence(proposal, signal);
          if (signal.audience !== "group") {
            throw new Error("A household episode must be cancelled in the household group");
          }
          const episode = requireOpenEpisode(state, proposal.episodeId);
          if (episode.status === "owned" && episode.ownerAdultId !== signal.senderAdultId) {
            throw new Error("Only the current owner may cancel an owned episode");
          }
          const event = {
            type: "episode.cancelled",
            episodeId: episode.id,
            cancelledByAdultId: signal.senderAdultId,
            reason: proposal.reason,
            sourceSignalIds: [...proposal.sourceSignalIds],
            occurredAt: signal.occurredAt,
          } satisfies HouseholdDomainEvent;
          events.push(event);
          applyHouseholdEvent(state, event);
          cancelEpisodeFollowUps(episode.id, timers, cancelEpisodeIds);
          effects.push(
            messageEffect(
              signal,
              conversation,
              episode.id,
              proposal.responseText ?? "Cancelled. I closed this and cancelled the remaining reminders.",
              "episode-cancelled",
            ),
          );
          break;
        }
        case "complete_episode": {
          requireCurrentMessageEvidence(proposal, signal);
          if (signal.audience !== "group") {
            throw new Error("Household episode completion must be confirmed in the household group");
          }
          const episode = requireOpenEpisode(state, proposal.episodeId);
          if (episode.ownerAdultId !== signal.senderAdultId) {
            throw new Error("Only the current owner can mark this episode handled");
          }
          const event = {
            type: "episode.completed",
            episodeId: episode.id,
            completedByAdultId: signal.senderAdultId,
            result: proposal.result,
            sourceSignalIds: [...proposal.sourceSignalIds],
            occurredAt: signal.occurredAt,
          } satisfies HouseholdDomainEvent;
          events.push(event);
          applyHouseholdEvent(state, event);
          effects.push(
            messageEffect(
              signal,
              conversation,
              episode.id,
              proposal.responseText ?? "Handled. I closed this and cancelled the remaining reminders.",
              "episode-completed",
            ),
          );
          cancelEpisodeFollowUps(episode.id, timers, cancelEpisodeIds);
          break;
        }
      }
    }
  }
}

function reduceHousehold(householdId: string, events: readonly PersistedHouseholdEvent[]): HouseholdState {
  const state: HouseholdState = {
    id: householdId,
    version: 0,
    name: null,
    timeZone: null,
    members: new Map(),
    memberSourceSignalIds: new Map(),
    adultVerifications: new Map(),
    adultEnrollments: new Map(),
    conversations: new Map(),
    gmailCandidates: new Map(),
    memories: new Map(),
    episodes: new Map(),
  };
  for (const event of events) {
    state.version = event.version;
    applyHouseholdEvent(state, event, event.signalId);
  }
  return state;
}

function applyHouseholdEvent(
  state: HouseholdState,
  event: HouseholdDomainEvent,
  sourceSignalId?: string,
): void {
  switch (event.type) {
    case "household.created":
      state.name = event.householdName;
      state.timeZone = event.timeZone;
      state.members.set(event.foundingAdult.id, event.foundingAdult);
      appendMemberSource(state, event.foundingAdult.id, sourceSignalId);
      if (event.plannedAdult) {
        state.members.set(event.plannedAdult.id, event.plannedAdult);
        appendMemberSource(state, event.plannedAdult.id, sourceSignalId);
      }
      break;
    case "family.member.upserted":
      state.members.set(event.member.id, event.member);
      appendMemberSource(state, event.member.id, sourceSignalId);
      break;
    case "adult.verified": {
      const adult = state.members.get(event.adultId);
      if (adult) state.members.set(event.adultId, { ...adult, status: "verified" });
      appendMemberSource(state, event.adultId, sourceSignalId);
      state.adultVerifications.set(event.adultId, {
        identitySubjectDigest: event.identitySubjectDigest,
        consentVersion: event.consentVersion,
        consentedAt: event.consentedAt,
      });
      break;
    }
    case "adult.enrollment.issued":
      state.adultEnrollments.set(event.adultId, {
        challengeDigest: event.challengeDigest,
        expiresAt: event.expiresAt,
        issuedAt: event.issuedAt,
      });
      break;
    case "conversation.bound":
      state.conversations.set(event.conversation.conversationId, event.conversation);
      break;
    case "gmail.candidate.staged":
      state.gmailCandidates.set(event.candidate.candidateId, {
        ...event.candidate,
        stagedAt: event.stagedAt,
        promoted: false,
        calendarApproved: false,
      });
      break;
    case "gmail.candidate.promoted": {
      const candidate = state.gmailCandidates.get(event.candidate.candidateId);
      if (candidate) candidate.promoted = true;
      break;
    }
    case "gmail.calendar.approved": {
      const candidate = state.gmailCandidates.get(event.candidate.candidateId);
      if (
        candidate &&
        candidate.version === event.candidate.version &&
        candidate.candidateDigest === event.candidate.candidateDigest
      ) {
        candidate.calendarApproved = true;
      }
      break;
    }
    case "memory.remembered": {
      if (event.supersedesMemoryId) {
        const superseded = state.memories.get(event.supersedesMemoryId);
        if (superseded) superseded.active = false;
      }
      state.memories.set(event.memoryId, { ...event, active: true });
      break;
    }
    case "episode.proposed":
      state.episodes.set(event.episodeId, {
        id: event.episodeId,
        title: event.title,
        outcome: event.outcome,
        dueAt: event.dueAt,
        suggestedOwnerAdultId: event.suggestedOwnerAdultId,
        ownerAdultId: null,
        status: "proposed",
        sourceSignalIds: [...event.sourceSignalIds],
        version: 1,
        updatedAt: event.occurredAt,
        result: null,
      });
      break;
    case "episode.owner.changed": {
      const episode = state.episodes.get(event.episodeId);
      if (episode) {
        episode.ownerAdultId = event.ownerAdultId;
        episode.status = event.ownerAdultId === null ? "proposed" : "owned";
        if (event.ownerAdultId === null) episode.suggestedOwnerAdultId = null;
        episode.version += 1;
        episode.updatedAt = event.occurredAt;
        episode.sourceSignalIds.push(...event.sourceSignalIds);
      }
      break;
    }
    case "episode.updated": {
      const episode = state.episodes.get(event.episodeId);
      if (episode) {
        episode.outcome = event.outcome;
        episode.dueAt = event.dueAt;
        episode.version += 1;
        episode.updatedAt = event.occurredAt;
        episode.sourceSignalIds.push(...event.sourceSignalIds);
      }
      break;
    }
    case "episode.cancelled": {
      const episode = state.episodes.get(event.episodeId);
      if (episode) {
        episode.status = "cancelled";
        episode.version += 1;
        episode.updatedAt = event.occurredAt;
        episode.sourceSignalIds.push(...event.sourceSignalIds);
      }
      break;
    }
    case "episode.completed": {
      const episode = state.episodes.get(event.episodeId);
      if (episode) {
        episode.status = "completed";
        episode.version += 1;
        episode.updatedAt = event.occurredAt;
        episode.result = event.result;
        episode.sourceSignalIds.push(...event.sourceSignalIds);
      }
      break;
    }
  }
}

function appendMemberSource(state: HouseholdState, memberId: string, sourceSignalId?: string): void {
  if (!sourceSignalId) return;
  const sources = state.memberSourceSignalIds.get(memberId) ?? [];
  if (!sources.includes(sourceSignalId)) {
    state.memberSourceSignalIds.set(memberId, [...sources, sourceSignalId].slice(-50));
  }
}

function verifiedAdults(state: HouseholdState): FamilyMemberProfile[] {
  return [...state.members.values()].filter(
    (member) => member.kind === "adult" && member.status === "verified",
  );
}

function identityBoundAdults(state: HouseholdState): FamilyMemberProfile[] {
  return verifiedAdults(state).filter((adult) => state.adultVerifications.has(adult.id));
}

function visibleMemories(state: HouseholdState, audience: "private" | "group", adultId: string) {
  return [...state.memories.entries()].filter(([, memory]) => {
    if (!memory.active) return false;
    if (audience === "group") return memory.scope === "household";
    return memory.scope === "household" || memory.personId === adultId;
  });
}

function requireSteward(state: HouseholdState, adultId: string): FamilyMemberProfile {
  const member = state.members.get(adultId);
  if (member?.kind !== "adult" || member.role !== "steward" || member.status !== "verified") {
    throw new Error("A verified household steward must make this change");
  }
  return member;
}

function requireHouseholdGroup(state: HouseholdState): BoundConversation {
  const conversation = [...state.conversations.values()].find((candidate) => candidate.audience === "group");
  if (!conversation) throw new Error("Household group is not currently available");
  return conversation;
}

function requirePrivateConversationForAdult(state: HouseholdState, adultId: string): BoundConversation {
  if (!state.adultVerifications.has(adultId)) {
    throw new Error("Personal sources require a currently identity-bound household adult");
  }
  const conversations = [...state.conversations.values()].filter(
    (candidate) =>
      candidate.audience === "private" &&
      candidate.authorizedAdultIds.length === 1 &&
      candidate.authorizedAdultIds[0] === adultId,
  );
  if (conversations.length !== 1) {
    throw new Error("Personal sources require exactly one current private owner conversation");
  }
  return conversations[0] as BoundConversation;
}

function requireSignalConversation(
  state: HouseholdState,
  signal: Extract<HouseholdSignal, { type: "conversation.message" }>,
): BoundConversation {
  const conversation = state.conversations.get(signal.conversationId);
  if (
    !conversation ||
    conversation.audience !== signal.audience ||
    conversation.authorityVersion !== signal.authorityVersion ||
    conversation.participantSetDigest !== signal.participantSetDigest ||
    !conversation.authorizedAdultIds.includes(signal.senderAdultId)
  ) {
    throw new Error("Conversation message is outside current household authority");
  }
  return conversation;
}

function requireOpenEpisode(
  state: HouseholdState,
  episodeId: string,
): HouseholdState["episodes"] extends Map<string, infer T> ? T : never {
  const episode = state.episodes.get(episodeId);
  if (!episode || episode.status === "completed" || episode.status === "cancelled") {
    throw new Error("Episode is no longer open");
  }
  return episode;
}

function ensureEvidence(
  proposal: WorkerProposal,
  signal: Extract<HouseholdSignal, { type: "conversation.message" }>,
  state: HouseholdState,
): void {
  if (proposal.type === "ignore") return;
  const known = new Set<string>([
    signal.signalId,
    ...visibleMemories(state, signal.audience, signal.senderAdultId).flatMap(
      ([, memory]) => memory.sourceSignalIds,
    ),
    ...[...state.memberSourceSignalIds.values()].flat(),
    ...[...state.episodes.values()].flatMap((episode) => episode.sourceSignalIds),
    ...[...state.gmailCandidates.values()]
      .filter(
        (candidate) =>
          signal.audience === "private" &&
          (!candidate.promoted || (candidate.calendarDraft !== null && !candidate.calendarApproved)) &&
          candidate.ownerAdultId === signal.senderAdultId,
      )
      .flatMap((candidate) => candidate.sourceSignalIds),
  ]);
  if (proposal.sourceSignalIds.some((sourceSignalId) => !known.has(sourceSignalId))) {
    throw new Error("Worker proposal cites evidence outside its household snapshot");
  }
}

function requireCurrentMessageEvidence(
  proposal: Exclude<WorkerProposal, { type: "ignore" }>,
  signal: Extract<HouseholdSignal, { type: "conversation.message" }>,
): void {
  if (!proposal.sourceSignalIds.includes(signal.signalId)) {
    throw new Error("Episode changes require evidence from the current household message");
  }
}

function requireSingleConversationCapability(proposals: readonly WorkerProposal[]): void {
  const capabilities = proposals.filter(({ type }) => type !== "ignore" && type !== "remember");
  if (capabilities.length > 1) {
    throw new WorkerRuntimeError(
      "invalid_output",
      "A conversation turn may perform only one consequential capability",
    );
  }
  if (proposals.length > 1 && proposals.some(({ type }) => type === "ignore")) {
    throw new WorkerRuntimeError(
      "invalid_output",
      "An ignored conversation turn cannot include another proposal",
    );
  }
}

function cancelEpisodeFollowUps(episodeId: string, timers: TimerDraft[], cancelEpisodeIds: string[]): void {
  if (!cancelEpisodeIds.includes(episodeId)) cancelEpisodeIds.push(episodeId);
  for (let index = timers.length - 1; index >= 0; index -= 1) {
    if (timers[index]?.episodeId === episodeId) timers.splice(index, 1);
  }
}

function scheduleEpisodeFollowUp(
  signal: Extract<HouseholdSignal, { type: "conversation.message" }>,
  episode: HouseholdState["episodes"] extends Map<string, infer T> ? T : never,
  timers: TimerDraft[],
): void {
  for (let index = timers.length - 1; index >= 0; index -= 1) {
    if (timers[index]?.episodeId === episode.id) timers.splice(index, 1);
  }
  timers.push({
    id: randomUUID(),
    idempotencyKey: `${signal.signalId}:episode:${episode.id}:follow-up`,
    episodeId: episode.id,
    episodeVersion: episode.version,
    scheduledFor: episode.dueAt ?? new Date(new Date(signal.occurredAt).getTime() + 86_400_000).toISOString(),
  });
}

function messageEffect(
  signal: HouseholdSignal,
  conversation: BoundConversation,
  episodeId: string | null,
  text: string,
  purpose: string,
): EffectDraft {
  return {
    id: randomUUID(),
    idempotencyKey: `${signal.signalId}:${purpose}`,
    kind: "conversation.message",
    conversationId: conversation.conversationId,
    conversationAuthorityVersion: conversation.authorityVersion,
    participantSetDigest: conversation.participantSetDigest,
    episodeId,
    payload: { text },
  };
}

type StoredGmailCandidate =
  HouseholdState["gmailCandidates"] extends Map<string, infer Candidate> ? Candidate : never;
type CalendarApprovalCandidate = StoredGmailCandidate & { calendarDraft: GmailCalendarDraft };

function calendarApprovalCandidates(
  state: HouseholdState,
  adultId: string,
  conversation: BoundConversation,
): CalendarApprovalCandidate[] {
  if (conversation.audience !== "private") return [];
  return [...state.gmailCandidates.values()].filter(
    (candidate): candidate is CalendarApprovalCandidate =>
      candidate.ownerAdultId === adultId &&
      !candidate.calendarApproved &&
      candidate.calendarDraft !== null &&
      candidate.presentedConversationId === conversation.conversationId &&
      candidate.presentedConversationAuthorityVersion === conversation.authorityVersion &&
      candidate.presentedParticipantSetDigest === conversation.participantSetDigest,
  );
}

function projectPrivateGmailCandidate(candidate: GmailCandidate) {
  return {
    candidateId: candidate.candidateId,
    version: candidate.version,
    candidateDigest: candidate.candidateDigest,
    ownerAdultId: candidate.ownerAdultId,
    privateSummary: candidate.privateSummary,
    householdMeaning: candidate.householdMeaning,
    calendarDraft: candidate.calendarDraft,
    sourceSignalIds: [...candidate.sourceSignalIds],
  };
}

function gmailCandidatePrivateMessage(candidate: GmailCandidate, _conversation: BoundConversation): string {
  const sharing = `${candidate.privateSummary}\n\nPossible family share: ${candidate.householdMeaning}\n\nReply “share” to post that exact line to the family, or “keep private”.`;
  if (candidate.calendarDraft === null) return sharing;

  const draft = candidate.calendarDraft;
  return `${sharing}\n\nCalendar draft — not added yet:\nTitle: ${draft.title}\nStart: ${draft.startsAt}\nEnd: ${draft.endsAt}\nTime zone: ${draft.timeZone}\nLocation: ${draft.location ?? "None"}\n\nSharing and Calendar approval are separate. Reply naturally to approve or decline this exact Calendar draft — for example, “Yes, add that exact event to my calendar.”`;
}

function sameStringSet(left: readonly string[], right: readonly string[]): boolean {
  return (
    left.length === right.length &&
    new Set(left).size === left.length &&
    left.every((id) => right.includes(id))
  );
}

function deterministicUuid(value: string): string {
  const digest = createHash("sha256").update(value).digest("hex");
  const variant = ((Number.parseInt(digest[16] ?? "0", 16) & 3) | 8).toString(16);
  return `${digest.slice(0, 8)}-${digest.slice(8, 12)}-5${digest.slice(13, 16)}-${variant}${digest.slice(17, 20)}-${digest.slice(20, 32)}`;
}

function digestStrings(values: readonly string[]): string {
  return createHash("sha256")
    .update(JSON.stringify([...values].sort()))
    .digest("hex");
}

function gmailCandidateDigest(candidate: {
  candidateId: string;
  ownerAdultId: string;
  connectionId: string;
  messageId: string;
  threadId: string;
  historyId: string;
  privateSummary: string;
  householdMeaning: string;
  calendarDraft: GmailCalendarDraft | null;
  sourceSignalIds: readonly string[];
  presentedConversationId: string;
  presentedConversationAuthorityVersion: number;
  presentedParticipantSetDigest: string;
}): string {
  return createHash("sha256")
    .update(canonicalJson({ ...candidate, version: 1 }))
    .digest("hex");
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
    .join(",")}}`;
}
