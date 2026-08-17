import { createHash } from "node:crypto";
import type { EncryptedImageVault } from "@florence/artifacts";
import {
  type CompleteFamilyOnboardingInput,
  completeFamilyOnboardingInputSchema,
  type FamilyMemberInput,
  familyMemberInputSchema,
  familyMemberProfileSchema,
  type PreferencesInput,
  preferencesInputSchema,
  type SetupSessionInput,
  type VaultSource,
  type WorkspaceView,
  workspaceViewSchema,
} from "@florence/contracts";
import type {
  AcceptInboundInput,
  AcceptInboundReactionInput,
  AcceptInboundResult,
  ApprovedCalendarAction,
  BootstrapMessagesGroupInput,
  CalendarActionDraft,
  CalendarOfferApproval,
  CalendarOfferDraft,
  CommitTurnInput,
  CompleteFounderOnboardingInput,
  FactDraft,
  FactRecord,
  FamilyMemberRecord,
  FollowUpDraft,
  HouseholdRecord,
  InboundTurn,
  JsonObject,
  LinqAuthority,
  MessagesEnrollmentResult,
  PostgresFlorenceStore,
  SourceRecord,
} from "@florence/database";
import {
  type BeginGoogleConnectionResult,
  GoogleCalendarTransientError,
  type GoogleConnection,
  GoogleConnectionError,
  type GoogleConnectionView,
} from "@florence/google";
import {
  type LinqClient,
  LinqError,
  type LinqMessageStatusProposal,
  type LinqReaction,
  type LinqReactionProposal,
} from "@florence/linq";
import type { EnrollmentCodes } from "./enrollment.js";
import {
  type FlorenceDecision,
  type FlorenceReadTools,
  type FlorenceReasoner,
  FlorenceReasonerError,
  type FlorenceReasonerInput,
  type FlorenceSource,
} from "./reasoner.js";

const DEFAULT_PREFERENCES: PreferencesInput = {
  appearance: "system",
};
const LOOP_IDLE_MS = 250;
const RETRY_MS = 15_000;
const WORK_CUE_MS = 6_000;
const ARTIFACT_PURGE_INTERVAL_MS = 60 * 60_000;

type TurnBoundaries = FlorenceReasonerInput["boundaries"];
type ActiveInbound = {
  sourceId: string;
  latestSourceId: string;
  channelId: string;
  providerConversationId: string;
  controller: AbortController;
  knownSourceIds: Set<string>;
};

export class Florence {
  readonly #store: PostgresFlorenceStore;
  readonly #linq: LinqClient;
  readonly #google: GoogleConnection | null;
  readonly #reasoner: FlorenceReasoner | null;
  readonly #enrollmentCodes: EnrollmentCodes;
  readonly #imageVault: EncryptedImageVault | null;
  readonly #messagesUrl: string | null;
  readonly #setupOrigin: string | null;
  readonly #now: () => Date;
  #activeRun: Promise<boolean> | null = null;
  #activeInbound: ActiveInbound | null = null;
  #pendingInboundAccepts = new Set<Promise<unknown>>();
  #nextArtifactPurgeAt = 0;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #started = false;

  constructor(input: {
    store: PostgresFlorenceStore;
    linq: LinqClient;
    google: GoogleConnection | null;
    reasoner: FlorenceReasoner | null;
    enrollmentCodes: EnrollmentCodes;
    imageVault: EncryptedImageVault | null;
    messagesUrl: string | null;
    setupOrigin?: string | null;
    now?: () => Date;
  }) {
    this.#store = input.store;
    this.#linq = input.linq;
    this.#google = input.google;
    this.#reasoner = input.reasoner;
    this.#enrollmentCodes = input.enrollmentCodes;
    this.#imageVault = input.imageVault;
    this.#messagesUrl = nullableText(input.messagesUrl);
    this.#setupOrigin = input.setupOrigin ? normalizedOrigin(input.setupOrigin) : null;
    this.#now = input.now ?? (() => new Date());
  }

  async workspaceForAdult(adultId: string): Promise<WorkspaceView> {
    const household = await this.#householdForAdultOrNull(adultId);
    const founder = household?.members.find(
      (member) => member.id === adultId && member.kind === "adult" && member.adultSlot === 1,
    );
    const google = household?.googleConnections.find(
      (connection) => connection.ownerAdultId === adultId && connection.status === "active",
    );
    if (founder && google && profileString(founder.profile, "onboardingCompletedAt")) {
      await this.#stageFounderWelcome(google);
    }
    return workspaceViewSchema.parse(workspace(adultId, household, this.#messagesUrl));
  }

  async completeFamilyOnboarding(
    adultId: string,
    untrustedInput: CompleteFamilyOnboardingInput,
  ): Promise<WorkspaceView> {
    const input = completeFamilyOnboardingInputSchema.parse(untrustedInput);
    const household = await this.#householdForAdult(adultId);
    await this.#store.completeFamilyOnboarding({
      householdId: household.id,
      founderAdultId: adultId,
      ...(input.partner ? { partner: input.partner } : {}),
      children: input.children.map((child) => ({
        id: child.id,
        displayName: child.displayName,
        ...(child.school ? { school: child.school } : {}),
        ...(child.activities ? { activities: child.activities } : {}),
      })),
      occurredAt: this.#now().toISOString(),
    });
    return this.workspaceForAdult(adultId);
  }

  async putMember(
    adultId: string,
    memberId: string,
    untrustedInput: FamilyMemberInput,
  ): Promise<WorkspaceView> {
    const input = familyMemberInputSchema.parse(untrustedInput);
    const household = await this.#householdForAdult(adultId);
    await this.#store.upsertMember({
      householdId: household.id,
      actorAdultId: adultId,
      memberId,
      member: {
        kind: input.kind,
        role: input.role,
        displayName: input.displayName,
        profile: memberProfile(input),
      },
      occurredAt: this.#now().toISOString(),
    });
    return this.workspaceForAdult(adultId);
  }

  async correctFact(adultId: string, factId: string, statement: string): Promise<WorkspaceView> {
    const household = await this.#householdForAdult(adultId);
    await this.#store.correctFact({ householdId: household.id, adultId, factId, statement });
    return this.workspaceForAdult(adultId);
  }

  async deleteFact(adultId: string, factId: string): Promise<WorkspaceView> {
    const household = await this.#householdForAdult(adultId);
    await this.#store.deleteFact({ householdId: household.id, adultId, factId });
    return this.workspaceForAdult(adultId);
  }

  async savePreferences(adultId: string, untrustedInput: PreferencesInput): Promise<WorkspaceView> {
    const preferences = preferencesInputSchema.parse(untrustedInput);
    const household = await this.#householdForAdult(adultId);
    await this.#store.savePreferences({ householdId: household.id, adultId, preferences });
    return this.workspaceForAdult(adultId);
  }

  async beginGoogle(adultId: string, sessionBindingDigest: string): Promise<BeginGoogleConnectionResult> {
    const google = this.#requiredGoogle();
    const household = await this.#householdForAdult(adultId);
    return google.begin({
      householdId: household.id,
      ownerAdultId: adultId,
      sessionBindingDigest,
      now: this.#now().toISOString(),
    });
  }

  async finishGoogle(input: {
    adultId: string;
    state: string;
    code: string;
    sessionBindingDigest: string;
  }): Promise<GoogleConnectionView> {
    const connection = await this.#requiredGoogle().finish({
      state: input.state,
      code: input.code,
      sessionBindingDigest: input.sessionBindingDigest,
      now: this.#now().toISOString(),
    });
    if (connection.ownerAdultId !== input.adultId) {
      throw new GoogleConnectionError("Google connection owner changed", "invalid_state");
    }
    const household = await this.#householdForAdult(input.adultId);
    const founder = household.members.find(
      (member) => member.id === input.adultId && member.kind === "adult" && member.adultSlot === 1,
    );
    if (founder && profileString(founder.profile, "onboardingCompletedAt")) {
      await this.#stageFounderWelcome(connection);
    }
    return connection;
  }

  async disconnectGoogle(adultId: string, connectionId: string): Promise<WorkspaceView> {
    const household = await this.#householdForAdult(adultId);
    await this.#requiredGoogle().disconnect({
      connectionId,
      householdId: household.id,
      ownerAdultId: adultId,
      now: this.#now().toISOString(),
    });
    return this.workspaceForAdult(adultId);
  }

  resolveLinqAuthority(
    input: Parameters<PostgresFlorenceStore["resolveLinqAuthority"]>[0],
  ): Promise<LinqAuthority | null> {
    return this.#store.resolveLinqAuthority(input);
  }

  async startMessagesOnboarding(input: {
    providerEventId: string;
    providerConversationId: string;
    identitySubjectDigest: string;
    occurredAt: string;
  }): Promise<boolean> {
    if (await this.#store.hasPilotHousehold()) return false;
    if (!this.#setupOrigin || !this.#google) {
      throw new Error("Google Workspace onboarding is not configured");
    }
    const setup = this.#enrollmentCodes.issueFounderSetup({
      providerEventId: input.providerEventId,
      providerConversationId: input.providerConversationId,
      identitySubjectDigest: input.identitySubjectDigest,
      occurredAt: input.occurredAt,
    });
    const setupUrl = `${this.#setupOrigin}/#setup=${encodeURIComponent(setup.token)}`;
    const expectedAuthority = {
      audience: "private" as const,
      participantIdentityDigests: [input.identitySubjectDigest],
    };
    const bubbles = [
      "Hey! Glad you’re here.",
      "I’m Florence. I help parents keep school, schedules, and the loose ends between two adults from becoming another job.",
      `Start with a quick private setup. You’ll connect your own Google account there: ${setupUrl}`,
    ];
    await this.#setTyping({
      providerConversationId: input.providerConversationId,
      expectedAuthority,
      active: true,
    });
    try {
      for (const [index, text] of bubbles.entries()) {
        if (index > 0) await pause(650);
        const result = await this.#linq.sendMessage({
          idempotencyKey: `founder-setup:${deterministicUuid(`${input.providerEventId}\0${index}`)}`,
          providerConversationId: input.providerConversationId,
          expectedAuthority,
          text,
        });
        if (result.status === "unknown") {
          throw new LinqError("provider_retryable", result.detail, true);
        }
        if (result.status === "failed") {
          throw new LinqError("provider_rejected", result.detail, false);
        }
      }
    } finally {
      await this.#setTyping({
        providerConversationId: input.providerConversationId,
        expectedAuthority,
        active: false,
      });
    }
    return true;
  }

  async redeemSetupLink(input: SetupSessionInput): Promise<MessagesEnrollmentResult | null> {
    if (!this.#google || !this.#setupOrigin) return null;
    if (input.profile.guardianAttested !== true) return null;
    const setup = this.#enrollmentCodes.verifyFounderSetup(input.setupToken, this.#now());
    if (!setup || !isIanaTimeZone(input.profile.timeZone)) return null;
    const observed = await this.#linq.observeChat(setup.providerConversationId);
    if (
      observed.audience !== "private" ||
      observed.participantIdentityDigests.length !== 1 ||
      observed.participantIdentityDigests[0] !== setup.identitySubjectDigest
    ) {
      return null;
    }
    const completedAt = this.#now();
    if (!this.#enrollmentCodes.verifyFounderSetup(input.setupToken, completedAt)) return null;
    const occurredAt = completedAt.toISOString();
    const completion: CompleteFounderOnboardingInput = {
      setupTokenDigest: this.#enrollmentCodes.digestFounderSetup(input.setupToken),
      setupExpiresAt: setup.expiresAt,
      householdId: setup.householdId,
      timeZone: input.profile.timeZone,
      adultId: setup.adultId,
      displayName: input.profile.displayName,
      identitySubjectDigest: setup.identitySubjectDigest,
      consentVersion: "linq-private-setup-v1",
      consentedAt: occurredAt,
      guardianAttestedAt: occurredAt,
      providerConversationId: setup.providerConversationId,
      occurredAt,
    };
    const result = await this.#store.completeFounderOnboarding(completion);
    return result;
  }

  async bootstrapMessagesGroup(input: BootstrapMessagesGroupInput): Promise<AcceptInboundResult | null> {
    const result = await this.#store.bootstrapMessagesGroup(input);
    if (result?.disposition === "accepted") this.#wake();
    return result;
  }

  async acceptInbound(input: AcceptInboundInput): Promise<AcceptInboundResult | null> {
    const active = this.#activeInbound;
    const sameActiveConversation = active?.providerConversationId === input.providerConversationId;
    const incomingSourceId = deterministicUuid(`linq-v3\0signal\0${input.providerEventId}`);
    const acceptance = this.#store.acceptInbound({
      ...input,
      ...(sameActiveConversation ? { supersedesSourceId: active.latestSourceId } : {}),
      ...(explicitlyProhibitsRetention(input.text) ? { discardSupersededFacts: true } : {}),
    });
    this.#pendingInboundAccepts.add(acceptance);
    void acceptance.then(
      () => this.#pendingInboundAccepts.delete(acceptance),
      () => this.#pendingInboundAccepts.delete(acceptance),
    );
    if (active && sameActiveConversation && !active.knownSourceIds.has(incomingSourceId)) {
      active.knownSourceIds.add(incomingSourceId);
      active.controller.abort();
    }
    const result = await acceptance;
    if (
      result &&
      result.sourceId !== active?.sourceId &&
      result.channelId === active?.channelId &&
      (result.disposition === "accepted" || result.disposition === "stopped")
    ) {
      active.latestSourceId = result.sourceId;
      active.controller.abort();
    }
    if (result?.disposition === "accepted") this.#wake();
    return result;
  }

  async acceptInboundReaction(input: AcceptInboundReactionInput): Promise<AcceptInboundResult | null> {
    const result = await this.#store.acceptInboundReaction(input);
    if (result?.disposition === "accepted") this.#wake();
    return result;
  }

  recordLinqObservation(
    input: LinqMessageStatusProposal | LinqReactionProposal,
  ): ReturnType<PostgresFlorenceStore["recordLinqObservation"]> {
    return this.#store.recordLinqObservation(input);
  }

  runOnce(): Promise<boolean> {
    if (this.#activeRun) return this.#activeRun;
    const run = this.#runCycle().finally(() => {
      if (this.#activeRun === run) this.#activeRun = null;
    });
    this.#activeRun = run;
    return run;
  }

  start(): void {
    if (this.#started) return;
    this.#started = true;
    this.#schedule(0);
  }

  stop(): void {
    this.#started = false;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
  }

  async #runCycle(): Promise<boolean> {
    let worked = false;
    await this.#purgeExpiredArtifacts();
    const inbound = await this.#store.readNextInbound(this.#now().toISOString());
    if (inbound) {
      await this.#handleInbound(inbound);
      worked = true;
    }
    await this.#settleInboundAccepts();
    if (await this.#store.promoteDueFollowUp({ now: this.#now().toISOString() })) worked = true;
    await this.#settleInboundAccepts();
    const outbound = await this.#store.readNextOutbound(this.#now().toISOString());
    await this.#settleInboundAccepts();
    if (outbound) {
      await this.#deliverOutbound(outbound.sourceId);
      worked = true;
    }
    await this.#settleInboundAccepts();
    const calendar = await this.#store.readNextCalendarAction(this.#now().toISOString());
    if (calendar) {
      await this.#executeCalendar(calendar);
      worked = true;
    }
    return worked;
  }

  async #settleInboundAccepts(): Promise<void> {
    while (this.#pendingInboundAccepts.size > 0) {
      await Promise.allSettled([...this.#pendingInboundAccepts]);
    }
  }

  async #purgeExpiredArtifacts(): Promise<void> {
    const now = this.#now();
    if (!this.#imageVault || now.getTime() < this.#nextArtifactPurgeAt) return;
    this.#nextArtifactPurgeAt = now.getTime() + ARTIFACT_PURGE_INTERVAL_MS;
    try {
      await this.#imageVault.purgeExpired(now);
    } catch {
      // Attachment reads still enforce expiry. Cleanup retries on the next bounded sweep.
    }
  }

  async #handleInbound(turn: InboundTurn): Promise<void> {
    const observed = await this.#linq.observeChat(turn.authority.providerConversationId);
    if (!sameAuthority(observed, turn.authority)) {
      await this.#store.commitTurn({ sourceId: turn.message.sourceId, handledAt: this.#now().toISOString() });
      return;
    }
    const sender = turn.household.members.find(
      (member) => member.id === turn.authority.senderAdultId && member.kind === "adult",
    );
    const googleActive =
      turn.authority.audience !== "private" ||
      (await this.#hasActiveGoogle(turn.authority.householdId, turn.authority.senderAdultId));
    const onboardingComplete = Boolean(
      sender && (sender.adultSlot === 2 || profileString(sender.profile, "onboardingCompletedAt")),
    );
    if (turn.authority.audience === "private" && (!googleActive || !onboardingComplete)) {
      await this.#store.commitTurn(
        decisionCommit(
          turn,
          {
            conversation: {
              replyToCurrentMessage: true,
              reaction: null,
              bubbles: [
                {
                  text: googleActive
                    ? "Finish the family setup on the web first. Nothing else is retained, scheduled, or changed yet."
                    : "Finish connecting Google from the setup page first. Nothing else is connected or changed yet.",
                  delayMs: 0,
                },
              ],
            },
            facts: [],
            followUp: null,
            calendar: null,
          },
          this.#now(),
        ),
      );
      return;
    }
    if (!this.#reasoner) {
      await this.#store.retryInbound({
        sourceId: turn.message.sourceId,
        retryAt: later(this.#now(), RETRY_MS),
        error: "Florence reasoning is not configured",
      });
      return;
    }

    const controller = new AbortController();
    const active: ActiveInbound = {
      sourceId: turn.message.sourceId,
      latestSourceId: turn.message.sourceId,
      channelId: turn.authority.channelId,
      providerConversationId: turn.authority.providerConversationId,
      controller,
      knownSourceIds: new Set([
        ...turn.supersededMessages.map((message) => message.sourceId),
        turn.message.sourceId,
      ]),
    };
    this.#activeInbound = active;
    const expectedAuthority = {
      audience: turn.authority.audience,
      participantIdentityDigests: turn.authority.expectedParticipantIdentityDigests,
    };
    const attachmentJob =
      turn.message.images.length > 0 ||
      (turn.currentDocuments?.length ?? 0) > 0 ||
      turn.supersededMessages.some((message) => message.images.length > 0);
    let typing = false;
    let workTimer: ReturnType<typeof setTimeout> | null = null;
    let workCue: Promise<void> | null = null;
    let immediateReactionStaged = false;
    try {
      if (attachmentJob) {
        workTimer = setTimeout(() => {
          if (controller.signal.aborted) return;
          workCue = this.#tryTurnCue(turn.message.sourceId, "work").then(() => undefined);
        }, WORK_CUE_MS);
        immediateReactionStaged = await this.#tryTurnCue(turn.message.sourceId, "reaction");
      }
      controller.signal.throwIfAborted();
      typing =
        turn.authority.audience === "private" &&
        (await this.#setTyping({
          providerConversationId: turn.authority.providerConversationId,
          expectedAuthority,
          active: true,
        }));
      const context = await this.#reasonerContext(turn);
      controller.signal.throwIfAborted();
      const decision = await this.#reasoner.decide(context.input, context.reads, controller.signal);
      if (workTimer) clearTimeout(workTimer);
      workTimer = null;
      if (workCue) await workCue;
      controller.signal.throwIfAborted();
      const guarded = enforceBoundaries(decision, context.input.boundaries, turn);
      await this.#store.commitTurn(
        decisionCommit(turn, guarded, this.#now(), { omitReaction: immediateReactionStaged }),
      );
    } catch (error) {
      if (controller.signal.aborted) return;
      if (error instanceof FlorenceReasonerError && !error.retryable) {
        if (workTimer) clearTimeout(workTimer);
        workTimer = null;
        if (workCue) await workCue;
        await this.#store.commitTurn(
          attachmentJob
            ? decisionCommit(
                turn,
                {
                  conversation: {
                    replyToCurrentMessage: true,
                    reaction: null,
                    bubbles: [
                      {
                        text: "I couldn’t finish reading that reliably, so I didn’t retain, schedule, or send anything.",
                        delayMs: 0,
                      },
                    ],
                  },
                  facts: [],
                  followUp: null,
                  calendar: null,
                },
                this.#now(),
                { omitReaction: immediateReactionStaged },
              )
            : { sourceId: turn.message.sourceId, handledAt: this.#now().toISOString() },
        );
        return;
      }
      await this.#store.retryInbound({
        sourceId: turn.message.sourceId,
        retryAt: later(this.#now(), RETRY_MS),
        error: errorText(error),
      });
    } finally {
      if (workTimer) clearTimeout(workTimer);
      if (workCue) await workCue;
      if (typing) {
        await this.#setTyping({
          providerConversationId: turn.authority.providerConversationId,
          expectedAuthority,
          active: false,
        });
      }
      if (this.#activeInbound === active) this.#activeInbound = null;
    }
  }

  async #reasonerContext(turn: InboundTurn): Promise<{
    input: FlorenceReasonerInput;
    reads: FlorenceReadTools;
  }> {
    const members = new Map(turn.household.members.map((member) => [member.id, member.displayName]));
    const visibleSources = memorySources(turn.facts);
    const sourceIndex = new Map(visibleSources.map((source) => [source.sourceId, source]));
    const visibility = turn.authority.audience === "group" ? "shared" : "adult_private";
    const currentDocuments = (turn.currentDocuments ?? []).slice(-3);
    const jobMessages = [...turn.supersededMessages, turn.message];
    const currentImages = jobMessages
      .flatMap((message) => message.images.map((image) => ({ ...image, sourceId: message.sourceId })))
      .slice(-10);
    const repliedMessage = turn.replyTarget;
    const indexMessage = (message: InboundTurn["message"] | InboundTurn["recentMessages"][number]) => {
      const text = turnText(message);
      sourceIndex.set(message.sourceId, {
        sourceId: message.sourceId,
        recordId: null,
        kind: "message",
        visibility,
        label:
          message.speaker === "florence" ? "Florence" : (members.get(message.speaker) ?? "Family message"),
        occurredAt: message.occurredAt,
        text,
      });
    };
    indexMessage(turn.message);
    if (repliedMessage) indexMessage(repliedMessage);
    for (const message of turn.recentMessages) indexMessage(message);
    for (const message of turn.supersededMessages) indexMessage(message);
    for (const document of currentDocuments) {
      sourceIndex.set(document.id, {
        sourceId: document.id,
        recordId: null,
        kind: "document",
        visibility,
        label: document.filename,
        occurredAt: turn.message.occurredAt,
        text: `Attached PDF: ${document.filename}`,
      });
    }

    const googleConnections =
      turn.authority.audience === "private" && this.#google
        ? await this.#google.status({
            householdId: turn.authority.householdId,
            ownerAdultId: turn.authority.senderAdultId,
          })
        : [];
    const input: FlorenceReasonerInput = {
      household: {
        householdId: turn.household.id,
        name: turn.household.name,
        timeZone: turn.household.timeZone,
        adultNames: turn.household.members
          .filter((member) => member.kind === "adult")
          .map((member) => member.displayName),
        familyProfile: JSON.stringify(
          turn.household.members.map((member) => ({
            id: member.id,
            name: member.displayName,
            kind: member.kind,
            role: member.role,
            profile: member.profile,
          })),
        ),
      },
      audience: turn.authority.audience,
      currentAdultId: turn.authority.senderAdultId,
      currentMessage: {
        sourceId: turn.message.sourceId,
        senderName: members.get(turn.message.speaker) ?? "Family member",
        moveKind: turn.message.moveKind,
        text: turnText(turn.message),
        occurredAt: turn.message.occurredAt,
        images: currentImages.map(reasonerImage),
        pdfs: currentDocuments.map((document) => ({
          documentId: document.id,
          filename: document.filename,
          mimeType: document.mimeType,
          contentDigest: document.contentDigest,
        })),
        replyTo: repliedMessage
          ? {
              sourceId: repliedMessage.sourceId,
              senderName:
                repliedMessage.speaker === "florence"
                  ? "Florence"
                  : (members.get(repliedMessage.speaker) ?? "Family member"),
              text: turnText(repliedMessage),
              occurredAt: repliedMessage.occurredAt,
            }
          : null,
      },
      recentMessages: [...turn.recentMessages, ...turn.supersededMessages]
        .sort((left, right) => left.occurredAt.localeCompare(right.occurredAt))
        .slice(-24)
        .map((message) => ({
          sourceId: message.sourceId,
          senderName:
            message.speaker === "florence" ? "Florence" : (members.get(message.speaker) ?? "Family member"),
          text: turnText(message),
          occurredAt: message.occurredAt,
        })),
      visibleSources,
      pendingFollowUps: turn.pendingFollowUps.map((followUp) => ({
        followUpId: followUp.id,
        at: followUp.dueAt,
        text: followUp.text,
        sourceIds: [...followUp.sourceIds],
      })),
      pendingCalendarOffers: turn.pendingCalendarOffers.map((offer) => ({
        proposalId: offer.id,
        connectionId: offer.connectionId,
        event: offer.event,
        sourceIds: [offer.basisSourceId],
      })),
      googleConnections: googleConnections.flatMap((connection) =>
        connection.status === "active" && connection.emailLabel
          ? [{ connectionId: connection.connectionId, emailLabel: connection.emailLabel }]
          : [],
      ),
      boundaries: turnBoundaries(turn),
    };

    const reads: FlorenceReadTools = {
      searchFamilyMemory: async ({ query, limit }) => searchSources(visibleSources, query).slice(0, limit),
      readCalendarWindow: async ({ connectionId, timeMin, timeMax, limit }) => {
        if (
          !this.#google ||
          turn.authority.audience !== "private" ||
          !googleConnections.some(
            (connection) => connection.connectionId === connectionId && connection.status === "active",
          )
        ) {
          return { status: "unavailable", events: [] };
        }
        try {
          return await this.#google.readCalendarWindow({
            householdId: turn.authority.householdId,
            ownerAdultId: turn.authority.senderAdultId,
            connectionId,
            timeMin,
            timeMax,
            limit,
          });
        } catch (error) {
          if (error instanceof GoogleCalendarTransientError) {
            throw new FlorenceReasonerError("transient", "Google Calendar is temporarily unavailable", {
              cause: error,
            });
          }
          throw error;
        }
      },
      readSource: async ({ sourceId }) => sourceIndex.get(sourceId) ?? null,
      readCurrentImage: async ({ assetId, mimeType }) => {
        const image = currentImages.find(
          (candidate) => candidate.assetId === assetId && candidate.mimeType === mimeType,
        );
        if (!image) {
          throw new Error("The image is not attached to the current message");
        }
        if (!this.#imageVault) throw new Error("Florence image reading is not configured");
        return this.#imageVault.read({
          householdId: turn.household.id,
          signalId: image.sourceId,
          image: { assetId, mimeType },
        });
      },
      readCurrentPdf: async ({ documentId, filename, mimeType, contentDigest }) => {
        const document = currentDocuments.find(
          (candidate) =>
            candidate.id === documentId &&
            candidate.filename === filename &&
            candidate.mimeType === mimeType &&
            candidate.contentDigest === contentDigest,
        );
        if (!document) throw new Error("The PDF is not attached to the current message");
        if (!this.#imageVault) throw new Error("Florence PDF reading is not configured");
        return this.#imageVault.openPdf({
          documentId: document.id,
          householdId: turn.household.id,
          signalId: document.parentSourceId,
          filename: document.filename,
          mimeType: document.mimeType,
          contentDigest: document.contentDigest,
          contentEnvelope: document.contentEnvelope,
          discardAfter: document.discardAfter,
          now: this.#now(),
        });
      },
      searchGmail: async ({ connectionId, query, limit }) => {
        if (!this.#google || turn.authority.audience !== "private") return [];
        const evidence = await this.#google.searchGmail({
          householdId: turn.authority.householdId,
          ownerAdultId: turn.authority.senderAdultId,
          connectionId,
          query,
          limit,
        });
        return Promise.all(
          evidence.map(async (message) => {
            const source = await this.#store.recordGmailEvidence({
              householdId: turn.authority.householdId,
              ownerAdultId: turn.authority.senderAdultId,
              connectionId,
              ...message,
            });
            const result: FlorenceSource = {
              sourceId: source.id,
              recordId: null,
              kind: "gmail",
              visibility: "adult_private",
              label: message.subject ?? message.from,
              occurredAt: message.sentAt,
              text: message.text,
            };
            sourceIndex.set(result.sourceId, result);
            return result;
          }),
        );
      },
    };
    return { input, reads };
  }

  async #deliverOutbound(sourceId: string, retryTransient = true): Promise<void> {
    const outbound = await this.#store.beginOutbound({ sourceId, now: this.#now().toISOString() });
    if (!outbound) return;
    try {
      const result =
        outbound.moveKind === "reaction"
          ? await this.#linq.sendReaction({
              idempotencyKey: outbound.idempotencyKey,
              providerConversationId: outbound.providerConversationId,
              expectedAuthority: outbound.expectedAuthority,
              targetProviderMessageId: requiredText(
                outbound.replyToProviderMessageId,
                "Reaction target message",
              ),
              reaction: reaction(outbound.reaction),
            })
          : await this.#linq.sendMessage({
              idempotencyKey: outbound.idempotencyKey,
              providerConversationId: outbound.providerConversationId,
              expectedAuthority: outbound.expectedAuthority,
              text: requiredText(outbound.text, "Outbound message"),
              ...(outbound.moveKind === "reply"
                ? {
                    replyTo: {
                      providerMessageId: requiredText(
                        outbound.replyToProviderMessageId,
                        "Reply target message",
                      ),
                    },
                  }
                : {}),
            });
      if (result.status === "committed") {
        await this.#store.completeOutbound({
          sourceId,
          providerMessageId: result.providerReceiptId,
          receiptDetail: {
            provider: "linq-v3",
            idempotencyKey: result.idempotencyKey,
            providerState: result.providerState,
          },
          sentAt: result.occurredAt,
        });
      } else {
        await this.#store.retryOutbound({ sourceId, retryAt: null, error: result.detail });
      }
    } catch (error) {
      await this.#store.retryOutbound({
        sourceId,
        retryAt:
          retryTransient && error instanceof LinqError && error.retryable ? later(this.#now(), 5_000) : null,
        error: errorText(error),
      });
    }
  }

  async #tryTurnCue(sourceId: string, cue: "reaction" | "work"): Promise<boolean> {
    try {
      const cueSourceId = await this.#store.stageTurnCue({
        sourceId,
        cue,
        occurredAt: this.#now().toISOString(),
      });
      if (!cueSourceId) return false;
      await this.#deliverOutbound(cueSourceId, false);
      return true;
    } catch {
      // A progress cue is optional. The substantive answer remains the product outcome.
      return false;
    }
  }

  async #executeCalendar(action: ApprovedCalendarAction): Promise<void> {
    if (!this.#google) {
      await this.#store.retryCalendarAction({
        id: action.id,
        retryAt: later(this.#now(), RETRY_MS),
        error: "Google Calendar is not configured",
      });
      return;
    }
    try {
      const result = await this.#google.executeCalendar(action);
      if (result.status === "failed") {
        await this.#store.failCalendarAction({
          id: action.id,
          error: result.detail,
          failureText: `I couldn’t confirm that “${action.event.title}” was added correctly. Please check Google Calendar before trying again.`,
          failedAt: result.occurredAt,
        });
        return;
      }
      const proof = calendarProof(result.detail, result.providerReceiptId);
      await this.#store.completeCalendarAction({
        id: action.id,
        providerEventId: result.providerReceiptId,
        providerEtag: proof.etag,
        proofDigest: proof.digest,
        proof,
        confirmationText: `Added “${action.event.title}” to your calendar.`,
        committedAt: result.occurredAt,
      });
    } catch (error) {
      await this.#store.retryCalendarAction({
        id: action.id,
        retryAt: later(this.#now(), RETRY_MS),
        error: errorText(error),
      });
    }
  }

  async #stageFounderWelcome(connection: GoogleConnectionView): Promise<void> {
    const household = await this.#householdForAdult(connection.ownerAdultId);
    const founder = household.members.find(
      (member) =>
        member.id === connection.ownerAdultId &&
        member.kind === "adult" &&
        member.adultSlot === 1 &&
        profileString(member.profile, "onboardingCompletedAt"),
    );
    const channels = household.channels.filter(
      (channel) =>
        channel.audience === "private" &&
        channel.adultIds.length === 1 &&
        channel.adultIds[0] === connection.ownerAdultId,
    );
    if (!founder || channels.length !== 1) {
      throw new Error("The Google owner does not have one exact active private Messages channel");
    }
    const channel = channels[0];
    if (!channel) throw new Error("The founder's private Messages channel is missing");
    if (channel.revokedAt || channel.stoppedAt) return;
    await this.#store.stageFounderWelcome({
      householdId: household.id,
      adultId: connection.ownerAdultId,
      channelId: channel.id,
      providerConversationId: channel.providerConversationId,
      texts: [
        `You’re in, ${founder.displayName} 🎉`,
        "Your Google account stays private to you. I’ll only bring family-relevant things into shared context with your direction.",
        "What’s one thing you’d rather not deal with yourself? I’ll take a first pass.",
      ],
      occurredAt: this.#now().toISOString(),
    });
  }

  async #hasActiveGoogle(householdId: string, adultId: string): Promise<boolean> {
    if (!this.#google) return false;
    const connections = await this.#google.status({ householdId, ownerAdultId: adultId });
    return connections.some((connection) => connection.status === "active");
  }

  async #householdForAdult(adultId: string): Promise<HouseholdRecord> {
    const household = await this.#householdForAdultOrNull(adultId);
    if (!household) throw new Error("The adult does not belong to a Florence household");
    return household;
  }

  async #householdForAdultOrNull(adultId: string): Promise<HouseholdRecord | null> {
    const ids = await this.#store.listHouseholdIdsForAdult(adultId);
    if (ids.length > 1) throw new Error("The two-adult pilot cannot span multiple households");
    return ids[0] ? await this.#store.readHousehold({ householdId: ids[0], viewerAdultId: adultId }) : null;
  }

  #requiredGoogle(): GoogleConnection {
    if (!this.#google) throw new Error("Google Workspace is not configured");
    return this.#google;
  }

  async #setTyping(input: Parameters<LinqClient["setTyping"]>[0]): Promise<boolean> {
    try {
      return await this.#linq.setTyping(input);
    } catch {
      return false;
    }
  }

  #wake(): void {
    void this.runOnce().catch((error: unknown) => console.error("Florence loop failed", error));
  }

  #schedule(delayMs: number): void {
    if (!this.#started || this.#timer) return;
    this.#timer = setTimeout(() => {
      this.#timer = null;
      void this.runOnce()
        .then((worked) => this.#schedule(worked ? 0 : LOOP_IDLE_MS))
        .catch((error: unknown) => {
          console.error("Florence loop failed", error);
          this.#schedule(RETRY_MS);
        });
    }, delayMs);
  }
}

function workspace(
  adultId: string,
  household: HouseholdRecord | null,
  messagesUrl: string | null,
): WorkspaceView {
  const viewer = household?.members.find((member) => member.id === adultId) ?? null;
  const founder = household?.members.find((member) => member.kind === "adult" && member.adultSlot === 1);
  const adults = household?.members.filter((member) => member.kind === "adult") ?? [];
  const activeChannels =
    household?.channels.filter((channel) => !channel.revokedAt && !channel.stoppedAt) ?? [];
  const contacts =
    household?.facts.flatMap((fact) => {
      const source = fact.sources[0];
      if ((fact.kind !== "address" && fact.kind !== "phone") || !source) return [];
      return [
        {
          id: fact.id,
          kind: fact.kind,
          label: fact.label,
          value: factStatement(fact),
          visibility: fact.visibility,
          source: vaultSource(source),
          editable: true,
          deletable: true,
        },
      ];
    }) ?? [];
  return {
    viewer: { adultId, displayName: viewer?.displayName ?? null },
    workspace: {
      messagesUrl,
      googleConnections:
        household?.googleConnections.flatMap((connection) =>
          connection.status === "active" && connection.emailLabel
            ? [
                {
                  connectionId: connection.connectionId,
                  status: "active" as const,
                  emailLabel: connection.emailLabel,
                  lastError: connection.lastError,
                },
              ]
            : [],
        ) ?? [],
      setup: {
        onboardingComplete: Boolean(founder && profileString(founder.profile, "onboardingCompletedAt")),
        secondAdultAdded: adults.length === 2,
        bothAdultsMessagesConnected:
          adults.length === 2 && adults.every((adult) => adult.messagesIdentity === "connected"),
        familyGroupConnected: activeChannels.some((channel) => channel.audience === "group"),
      },
    },
    vault: household
      ? {
          timeZone: household.timeZone,
          members: household.members.map(memberView),
          contacts,
          facts: household.facts.flatMap((fact) => {
            const source = fact.sources[0];
            if (!source || fact.kind === "address" || fact.kind === "phone") return [];
            return [
              {
                id: fact.id,
                statement: factStatement(fact),
                visibility: fact.visibility,
                source: vaultSource(source),
                recordedAt: source.occurredAt,
                editable: true,
                deletable: true,
              },
            ];
          }),
        }
      : null,
    preferences: preferences(viewer?.preferences),
  };
}

function memberView(member: FamilyMemberRecord) {
  const candidate = {
    id: member.id,
    kind: member.kind,
    role: member.role,
    displayName: member.displayName,
    relationship: profileString(member.profile, "relationship") ?? defaultRelationship(member),
    ...(profileStrings(member.profile, "aliases")
      ? { aliases: profileStrings(member.profile, "aliases") }
      : {}),
    ...(profileNumber(member.profile, "birthYear")
      ? { birthYear: profileNumber(member.profile, "birthYear") }
      : {}),
    ...(profileString(member.profile, "school") ? { school: profileString(member.profile, "school") } : {}),
    ...(profileString(member.profile, "currentGrade")
      ? { currentGrade: profileString(member.profile, "currentGrade") }
      : {}),
    ...(profileString(member.profile, "academicYear")
      ? { academicYear: profileString(member.profile, "academicYear") }
      : {}),
    ...(profileString(member.profile, "gradeEffectiveFrom")
      ? { gradeEffectiveFrom: profileString(member.profile, "gradeEffectiveFrom") }
      : {}),
    ...(profileStrings(member.profile, "activities")
      ? { activities: profileStrings(member.profile, "activities") }
      : {}),
    status: member.status,
    messagesIdentity: member.kind === "child" ? null : member.messagesIdentity,
  };
  return familyMemberProfileSchema.parse(candidate);
}

function memberProfile(member: FamilyMemberInput): JsonObject {
  return {
    relationship: member.relationship,
    ...(member.aliases ? { aliases: member.aliases } : {}),
    ...(member.birthYear ? { birthYear: member.birthYear } : {}),
    ...(member.school ? { school: member.school } : {}),
    ...(member.currentGrade ? { currentGrade: member.currentGrade } : {}),
    ...(member.academicYear ? { academicYear: member.academicYear } : {}),
    ...(member.gradeEffectiveFrom ? { gradeEffectiveFrom: member.gradeEffectiveFrom } : {}),
    ...(member.activities ? { activities: member.activities } : {}),
  };
}

function memorySources(facts: readonly FactRecord[]): FlorenceSource[] {
  return facts.flatMap((fact) =>
    fact.sources.map((source) => ({
      sourceId: source.id,
      recordId: fact.id,
      kind: "memory" as const,
      visibility: fact.visibility === "household" ? ("shared" as const) : ("adult_private" as const),
      label: fact.label,
      occurredAt: source.occurredAt,
      text: factStatement(fact),
    })),
  );
}

function turnBoundaries(turn: InboundTurn): TurnBoundaries {
  const boundaries: TurnBoundaries = {
    retain: true,
    schedule: true,
    consequentialAction: true,
  };
  for (const message of [...turn.supersededMessages, turn.message]) {
    applyBoundaryText(boundaries, message.text);
  }
  return boundaries;
}

function applyBoundaryText(boundaries: TurnBoundaries, text: string | null): void {
  if (!text) return;
  const request = text
    .toLocaleLowerCase()
    .replaceAll("’", "'")
    .replaceAll("–", "-")
    .replace(/\s+/g, " ")
    .trim();
  const positiveClauses = [
    ...request.matchAll(/\b(?:go ahead and|you (?:can|may)(?: now)?|please(?: now)?)\s+[^.!?\n]{0,120}/g),
  ].map(([clause]) => clause);
  const negativeClauses = [...request.matchAll(/\b(?:do not|don't|dont|never)\s+[^.!?\n]{0,120}/g)].map(
    ([clause]) => clause,
  );

  for (const clause of positiveClauses) {
    const action = clause.replace(/^.*?\b(?:go ahead and|you (?:can|may)(?: now)?|please(?: now)?)\s+/, "");
    if (/^(?:save|retain|remember|store)\b/.test(action)) boundaries.retain = true;
    if (/^(?:schedule|set (?:a )?reminder|remind)\b/.test(action)) boundaries.schedule = true;
    if (/^(?:add|put|schedule)\b[^.!?\n]{0,50}\b(?:to|on) (?:my|the) calendar\b/.test(action)) {
      boundaries.schedule = true;
      boundaries.consequentialAction = true;
    }
    if (/^(?:send|contact|submit|book|buy|purchase)\b/.test(action)) {
      boundaries.consequentialAction = true;
    }
  }

  for (const clause of negativeClauses) {
    if (/\b(?:save|retain|remember|store)\b/.test(clause)) {
      boundaries.retain = false;
      boundaries.schedule = false;
      boundaries.consequentialAction = false;
    }
    if (
      /\b(?:schedule|set (?:a )?reminder|remind|book)\b/.test(clause) ||
      /\b(?:add|put)\b[^.!?\n]{0,30}\bcalendar\b/.test(clause)
    ) {
      boundaries.schedule = false;
      boundaries.consequentialAction = false;
    }
    if (/\b(?:send|contact|submit|book|buy|purchase|act)\b/.test(clause)) {
      boundaries.consequentialAction = false;
    }
  }
  if (/\bdraft only\b/.test(request)) boundaries.consequentialAction = false;
}

function explicitlyProhibitsRetention(text: string | null): boolean {
  const request = normalizedParentText(text);
  return /\b(?:don't|do not|never)\s+(?!forget\s+to\b)(?:(?:please|ever|again)\s+)*(?:save|retain|remember|store)\b/.test(
    request,
  );
}

function enforceBoundaries(
  decision: FlorenceDecision,
  boundaries: TurnBoundaries,
  turn: InboundTurn,
): FlorenceDecision {
  const retain = boundaries.retain;
  const schedule = retain && boundaries.schedule;
  const consequentialAction = schedule && boundaries.consequentialAction;
  let calendar = consequentialAction ? decision.calendar : null;
  if (
    calendar?.mode === "direct" &&
    (turnHasAttachments(turn) || !explicitCalendarWriteInstruction(turn.message))
  ) {
    calendar = { ...calendar, mode: "offer" };
  }
  if (calendar?.mode === "approve" && !explicitCalendarOfferApproval(turn, calendar.proposalId)) {
    calendar = null;
  }
  const confirmation = !retain
    ? "I didn’t retain anything in the Vault, send anything externally, schedule a follow-up, or add anything to your calendar."
    : !schedule
      ? "I didn’t schedule a follow-up or add anything to your calendar."
      : !consequentialAction
        ? "I didn’t send, submit, book, purchase, or add anything to your calendar."
        : null;
  const bubbles = decision.conversation.bubbles.map((bubble) => ({ ...bubble }));
  if (confirmation) {
    const last = bubbles.at(-1);
    if (last) last.text = `${last.text}\n\n${confirmation}`;
    else bubbles.push({ text: confirmation, delayMs: 0 });
  }
  return {
    conversation: { ...decision.conversation, bubbles },
    facts: retain ? decision.facts : [],
    followUp: schedule ? decision.followUp : null,
    calendar,
  };
}

function turnHasAttachments(turn: InboundTurn): boolean {
  return (
    (turn.currentDocuments?.length ?? 0) > 0 ||
    turn.message.images.length > 0 ||
    turn.supersededMessages.some((message) => message.images.length > 0)
  );
}

function explicitCalendarWriteInstruction(message: InboundTurn["message"]): boolean {
  return (
    message.moveKind !== "reaction" &&
    /^(?:(?:please|kindly)\s+|(?:(?:can|could|would|will)\s+you(?:\s+please)?\s+)|(?:go ahead and\s+))?(?:add|put|schedule)\b.{1,240}\b(?:to|on|in)\s+(?:my|the)\s+calendar\b/.test(
      normalizedParentText(message.text),
    )
  );
}

function explicitCalendarOfferApproval(turn: InboundTurn, proposalId: string): boolean {
  if (
    turn.message.moveKind === "reaction" ||
    turn.pendingCalendarOffers.length !== 1 ||
    turn.pendingCalendarOffers[0]?.id !== proposalId
  ) {
    return false;
  }
  return /^(?:yes,?\s+(?:please\s+)?add it|go ahead\s+and\s+add it|please\s+add it|add it(?:,?\s+please)?)\.?$/.test(
    normalizedParentText(turn.message.text),
  );
}

function normalizedParentText(text: string | null): string {
  return (text ?? "")
    .toLocaleLowerCase()
    .replaceAll("’", "'")
    .replaceAll("–", "-")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^(?:hey\s+)?florence[,!:;\s]+/, "");
}

function decisionCommit(
  turn: InboundTurn,
  decision: FlorenceDecision,
  now: Date,
  options: { omitReaction?: boolean } = {},
): CommitTurnInput {
  if (
    turn.message.moveKind === "reaction" &&
    (decision.facts.length > 0 || decision.followUp !== null || decision.calendar !== null)
  ) {
    throw new FlorenceReasonerError(
      "invalid_output",
      "A reaction can express affect but cannot authorize durable or consequential changes",
    );
  }
  const responseTargetSourceId =
    turn.message.moveKind === "reaction" ? turn.message.replyToSourceId : turn.message.sourceId;
  if (!responseTargetSourceId) {
    throw new FlorenceReasonerError("invalid_output", "An inbound reaction has no Florence target");
  }
  const turnId = deterministicUuid(`turn\0${turn.message.sourceId}`);
  const bubbles = decision.calendar
    ? decision.calendar.mode === "offer"
      ? [{ text: calendarOfferText(decision.calendar.event), delayMs: 0 }]
      : []
    : decision.conversation.bubbles;
  const facts: FactDraft[] = [];
  const deleteFactIds: string[] = [];
  for (const [index, change] of decision.facts.entries()) {
    if (change.operation === "forget") {
      const existing = turn.facts.find((fact) => fact.id === change.factId);
      const canDelete =
        turn.authority.audience === "group"
          ? existing?.visibility === "household"
          : existing?.visibility === "private" && existing.ownerAdultId === turn.authority.senderAdultId;
      if (!canDelete) {
        throw new FlorenceReasonerError(
          "invalid_output",
          "A conversation cannot forget memory outside its write scope",
        );
      }
      deleteFactIds.push(change.factId);
      continue;
    }
    const existing =
      change.operation === "correct" ? turn.facts.find((fact) => fact.id === change.factId) : null;
    const statement = change.statement;
    const visibility = turn.authority.audience === "group" ? "household" : "private";
    const ownerAdultId = visibility === "private" ? turn.authority.senderAdultId : null;
    const sameScope = existing?.visibility === visibility && existing.ownerAdultId === ownerAdultId;
    const slot = existing?.slot ?? `general:${sha256(statement.toLocaleLowerCase())}`;
    facts.push({
      id: sameScope ? existing.id : deterministicUuid(`fact\0${turn.message.sourceId}\0${index}`),
      subjectPersonId: existing?.subjectPersonId ?? null,
      kind: existing?.kind ?? "general",
      slot,
      label: existing?.label ?? statement.slice(0, 160),
      value: { statement },
      visibility,
      ownerAdultId,
      sourceIds: change.sourceIds,
    });
  }
  const followUps: FollowUpDraft[] =
    decision.followUp?.operation === "schedule"
      ? [
          {
            id: deterministicUuid(`follow-up\0${turn.message.sourceId}`),
            dedupeKey: `follow-up:${turn.message.sourceId}`,
            text: decision.followUp.text,
            dueAt: decision.followUp.at,
            sourceIds: decision.followUp.sourceIds,
          },
        ]
      : [];
  const outbound: NonNullable<CommitTurnInput["outbound"]>[number][] = [];
  if (decision.conversation.reaction && !options.omitReaction) {
    outbound.push({
      sourceId: deterministicUuid(`outbound\0${turnId}\0reaction`),
      idempotencyKey: `turn:${turn.message.sourceId}:reaction`,
      moveKind: "reaction",
      reaction: decision.conversation.reaction,
      replyToSourceId: responseTargetSourceId,
      turnId,
      turnPart: -1,
      notBefore: now.toISOString(),
    });
  }
  let delay = 0;
  bubbles.forEach((bubble, index) => {
    delay += bubble.delayMs;
    outbound.push({
      sourceId: deterministicUuid(`outbound\0${turnId}\0${index}`),
      idempotencyKey: `turn:${turn.message.sourceId}:bubble:${index}`,
      moveKind: index === 0 && decision.conversation.replyToCurrentMessage ? "reply" : "message",
      text: bubble.text,
      ...(index === 0 && decision.conversation.replyToCurrentMessage
        ? { replyToSourceId: responseTargetSourceId }
        : {}),
      turnId,
      turnPart: index as 0 | 1 | 2,
      notBefore: new Date(now.getTime() + delay).toISOString(),
    });
  });
  const calendar = calendarCommit(turn, decision);
  return {
    sourceId: turn.message.sourceId,
    facts,
    deleteFactIds,
    followUps,
    cancelFollowUpIds: decision.followUp?.operation === "cancel" ? [decision.followUp.followUpId] : [],
    outbound,
    ...calendar,
    handledAt: now.toISOString(),
  };
}

function calendarOfferText(event: CalendarActionDraft["event"]): string {
  const format = new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: event.timeZone,
  });
  const location = event.location ? `\n${event.location}` : "";
  return `I can add this to your calendar:\n\n${event.title}\n${format.format(new Date(event.startsAt))} – ${format.format(new Date(event.endsAt))}\n${event.timeZone}${location}\n\nWant me to add it?`;
}

function calendarCommit(
  turn: InboundTurn,
  decision: FlorenceDecision,
): Pick<CommitTurnInput, "calendarOffers" | "approveCalendarOffers" | "calendarActions"> {
  if (!decision.calendar) return {};
  if (decision.calendar.mode === "approve") {
    const offer = turn.pendingCalendarOffers.find(
      (candidate) => candidate.id === decision.calendar?.proposalId,
    );
    if (!offer) throw new Error("The Calendar offer is no longer pending");
    const approval: CalendarOfferApproval = {
      offerId: offer.id,
      approvalDigest: sha256(
        JSON.stringify({
          approvalMessageId: turn.message.sourceId,
          proposalDigest: offer.proposalDigest,
          text: turn.message.text,
        }),
      ),
    };
    return { approveCalendarOffers: [approval] };
  }
  const sourceId = decision.calendar.sourceIds[0];
  if (!sourceId) throw new Error("A Calendar decision requires a source");
  const proposalDigest = sha256(
    JSON.stringify({
      connectionId: decision.calendar.connectionId,
      event: decision.calendar.event,
      sourceIds: decision.calendar.sourceIds,
    }),
  );
  const id = deterministicUuid(`calendar\0${turn.message.sourceId}`);
  const actionId = deterministicUuid(`calendar-action\0${turn.message.sourceId}`);
  if (decision.calendar.mode === "offer") {
    const offer: CalendarOfferDraft = {
      id,
      actionId,
      connectionId: decision.calendar.connectionId,
      ownerAdultId: turn.authority.senderAdultId,
      basisSourceId: sourceId,
      proposalDigest,
      event: decision.calendar.event,
    };
    return { calendarOffers: [offer] };
  }
  const action: CalendarActionDraft = {
    id,
    actionId,
    connectionId: decision.calendar.connectionId,
    ownerAdultId: turn.authority.senderAdultId,
    basisSourceId: sourceId,
    approvalMessageId: turn.message.sourceId,
    approvalDigest: sha256(
      JSON.stringify({
        approvalMessageId: turn.message.sourceId,
        proposalDigest,
        text: turn.message.text,
      }),
    ),
    proposalDigest,
    event: decision.calendar.event,
  };
  return { calendarActions: [action] };
}

function preferences(value: JsonObject | undefined): PreferencesInput {
  const parsed = preferencesInputSchema.safeParse({ ...DEFAULT_PREFERENCES, ...(value ?? {}) });
  return parsed.success ? parsed.data : DEFAULT_PREFERENCES;
}

function vaultSource(source: SourceRecord): VaultSource {
  const kind =
    source.kind === "linq_message"
      ? "message"
      : source.kind === "gmail"
        ? "gmail"
        : source.kind === "web"
          ? "web"
          : "document";
  return { id: source.id, kind, label: source.label, occurredAt: source.occurredAt };
}

function factStatement(fact: FactRecord): string {
  const value: unknown = fact.value;
  if (typeof value === "string") return value;
  if (isRecord(value) && typeof value.statement === "string") return value.statement;
  return JSON.stringify(value);
}

function reasonerImage(image: InboundTurn["message"]["images"][number]): {
  assetId: string;
  mimeType: "image/jpeg" | "image/png" | "image/webp";
} {
  if (image.mimeType === "image/heic") {
    throw new Error("Current-message HEIC bytes were not normalized before persistence");
  }
  return { assetId: image.assetId, mimeType: image.mimeType };
}

function turnText(turn: InboundTurn["message"] | InboundTurn["recentMessages"][number]): string {
  if (turn.text?.trim()) return turn.text;
  if (turn.reaction) return `Reacted ${turn.reaction}`;
  return "Shared a family attachment.";
}

function searchSources(sources: readonly FlorenceSource[], query: string): FlorenceSource[] {
  const words = query.toLocaleLowerCase().split(/\s+/).filter(Boolean);
  return sources.filter((source) => {
    const haystack = `${source.label}\n${source.text}`.toLocaleLowerCase();
    return words.every((word) => haystack.includes(word));
  });
}

function calendarProof(detail: string, eventId: string): JsonObject & { etag: string; digest: string } {
  const parsed: unknown = JSON.parse(detail);
  if (
    !isRecord(parsed) ||
    parsed.provider !== "google-calendar" ||
    parsed.eventId !== eventId ||
    typeof parsed.etag !== "string" ||
    typeof parsed.digest !== "string" ||
    !/^[0-9a-f]{64}$/.test(parsed.digest)
  ) {
    throw new Error("Google Calendar returned an invalid proof receipt");
  }
  return parsed as JsonObject & { etag: string; digest: string };
}

function sameAuthority(
  observed: { audience: "private" | "group"; participantIdentityDigests: readonly string[] },
  expected: LinqAuthority,
): boolean {
  return (
    observed.audience === expected.audience &&
    observed.participantIdentityDigests.length === expected.expectedParticipantIdentityDigests.length &&
    observed.participantIdentityDigests.every(
      (digest, index) => digest === expected.expectedParticipantIdentityDigests[index],
    )
  );
}

function normalizedOrigin(value: string): string {
  const url = new URL(value);
  if (
    (url.protocol !== "http:" && url.protocol !== "https:") ||
    url.username ||
    url.password ||
    url.origin === "null"
  ) {
    throw new Error("Florence setup origin must be an HTTP(S) origin without credentials");
  }
  return url.origin;
}

function isIanaTimeZone(value: string): boolean {
  try {
    return new Intl.DateTimeFormat("en-US", { timeZone: value }).resolvedOptions().timeZone === value;
  } catch {
    return false;
  }
}

function pause(delayMs: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, delayMs));
}

function reaction(value: string | null): LinqReaction {
  if (
    value === "love" ||
    value === "like" ||
    value === "dislike" ||
    value === "laugh" ||
    value === "emphasize" ||
    value === "question"
  ) {
    return value;
  }
  throw new Error("Outbound reaction is invalid");
}

function defaultRelationship(member: FamilyMemberRecord): string {
  if (member.kind === "child") return "Child";
  return member.role === "steward" ? "Parent" : "Caregiver";
}

function profileString(profile: JsonObject, key: string): string | null {
  const value = profile[key];
  return typeof value === "string" && value.trim() ? value : null;
}

function profileStrings(profile: JsonObject, key: string): string[] | null {
  const value = profile[key];
  return Array.isArray(value) && value.every((item) => typeof item === "string") ? value : null;
}

function profileNumber(profile: JsonObject, key: string): number | null {
  const value = profile[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function nullableText(value: string | null): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

function requiredText(value: string | null, label: string): string {
  if (!value?.trim()) throw new Error(`${label} is required`);
  return value;
}

function later(now: Date, milliseconds: number): string {
  return new Date(now.getTime() + milliseconds).toISOString();
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function deterministicUuid(value: string): string {
  const digest = sha256(value);
  const variant = ((Number.parseInt(digest[16] ?? "0", 16) & 3) | 8).toString(16);
  return `${digest.slice(0, 8)}-${digest.slice(8, 12)}-5${digest.slice(13, 16)}-${variant}${digest.slice(17, 20)}-${digest.slice(20, 32)}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : "Unknown Florence loop failure";
}
