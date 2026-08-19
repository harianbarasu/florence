import { createHash } from "node:crypto";
import type { EncryptedImageVault } from "@florence/artifacts";
import {
  type CompleteFamilyOnboardingInput,
  calendarMonthSchema,
  completeFamilyOnboardingInputSchema,
  type FamilyCalendarMonthView,
  type FamilyMemberInput,
  type FamilyMemberMutationInput,
  familyCalendarMonthViewSchema,
  familyMemberMutationInputSchema,
  familyMemberProfileSchema,
  type PatchFamilyMemberInput,
  type PatchWatchInput,
  type PreferencesInput,
  patchWatchInputSchema,
  preferencesInputSchema,
  type SetupSessionInput,
  type VaultSource,
  type WorkspaceView,
  workspaceViewSchema,
} from "@florence/contracts";
import type {
  AcceptInboundEnvelopeInput,
  AcceptInboundInput,
  AcceptInboundReactionInput,
  AcceptInboundResult,
  ApprovedCalendarAction,
  ApprovedPartnerInvitation,
  CalendarActionDraft,
  CalendarEvidenceDraft,
  CalendarOfferApproval,
  CalendarOfferDraft,
  CommitTurnInput,
  CompleteFounderOnboardingInput,
  DueProactiveWork,
  FactDraft,
  FactRecord,
  FamilyGroupCreationWork,
  FamilyMemberRecord,
  FiniteMonitorDraft,
  FiniteMonitorUpdate,
  GoogleEvidenceDraft,
  HouseholdRecord,
  InboundPreparationContext,
  InboundTurn,
  InitialIntelligenceWork,
  JsonObject,
  LinqAuthority,
  MessagesEnrollmentResult,
  PostgresFlorenceStore,
  PreparedInboundContent,
  ProactiveDelivery,
  SourceRecord,
} from "@florence/database";
import { draftCalendarEvidence, draftGmailEvidence } from "@florence/database";
import {
  type BeginGoogleConnectionResult,
  type GmailAttachmentReference,
  type GmailEvidence,
  type GoogleCalendarBoundedCursor,
  type GoogleCalendarChange,
  GoogleCalendarTransientError,
  type GoogleCalendarWindowEvent,
  type GoogleConnection,
  GoogleConnectionError,
  type GoogleConnectionView,
  GoogleFamilyCalendarProvisioningError,
  GoogleFamilyCalendarTransientError,
  type GoogleGmailCursor,
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
  type FlorenceBoundedPrivateGoogleEvidence,
  type FlorenceDecision,
  type FlorenceNarrowFamilyProfile,
  type FlorencePrivateCalendarEvent,
  type FlorencePrivateGmailSource,
  type FlorenceReadTools,
  type FlorenceReasoner,
  FlorenceReasonerError,
  type FlorenceReasonerInput,
  type FlorenceSource,
  type FlorenceVoiceNoteInput,
} from "./reasoner.js";

const DEFAULT_PREFERENCES: PreferencesInput = {
  proactiveGoogleEnabled: true,
  automaticFamilyCalendarEnabled: true,
  privateConflictBusySharingEnabled: false,
};
const LOOP_IDLE_MS = 250;
const RETRY_MS = 15_000;
const WORK_CUE_MS = 6_000;
const ARTIFACT_PURGE_INTERVAL_MS = 60 * 60_000;
const QUIET_START_HOUR = 20;
const QUIET_END_HOUR = 7;

type ActiveInbound = {
  sourceId: string;
  latestSourceId: string;
  latestOccurredAt: string;
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
  readonly #linqSenderPhoneNumber: string | null;
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
    linqSenderPhoneNumber?: string | null;
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
    this.#linqSenderPhoneNumber = nullableText(input.linqSenderPhoneNumber ?? null);
    this.#setupOrigin = input.setupOrigin ? normalizedOrigin(input.setupOrigin) : null;
    this.#now = input.now ?? (() => new Date());
  }

  async workspaceForAdult(adultId: string): Promise<WorkspaceView> {
    const household = await this.#householdForAdultOrNull(adultId);
    return workspaceViewSchema.parse(workspace(adultId, household, this.#messagesUrl));
  }

  async familyCalendarMonthForAdult(
    adultId: string,
    untrustedMonth: string,
  ): Promise<FamilyCalendarMonthView> {
    const month = calendarMonthSchema.parse(untrustedMonth);
    const household = await this.#householdForAdult(adultId);
    const timeZone = household.timeZone;
    const calendarName = household.familyCalendarLabel ?? household.name;
    if (
      !household.familyCalendarId ||
      household.familyCalendarId === "primary" ||
      !household.familyCalendarOwnerConnectionId ||
      !household.familyCalendarPartnerConnectionId ||
      !household.familyCalendarCreatedAt
    ) {
      return familyCalendarMonthViewSchema.parse({
        status: "not_ready",
        month,
        timeZone,
        calendarName,
      });
    }
    const google = this.#google;
    if (!google) {
      return familyCalendarMonthViewSchema.parse({
        status: "temporarily_unavailable",
        month,
        timeZone,
        calendarName,
      });
    }
    const credential = await this.#store.readActiveFamilyCalendarCredential({ householdId: household.id });
    if (!credential || credential.calendarId !== household.familyCalendarId) {
      return familyCalendarMonthViewSchema.parse({
        status: "temporarily_unavailable",
        month,
        timeZone,
        calendarName,
      });
    }

    try {
      const reads = await Promise.all(
        familyCalendarMonthWindows(month, timeZone).map(({ timeMin, timeMax }) =>
          google.readCalendarWindow({
            householdId: household.id,
            ownerAdultId: credential.ownerAdultId,
            connectionId: credential.connectionId,
            calendarId: credential.calendarId,
            timeMin,
            timeMax,
            limit: 50,
            eventSelection: "all",
          }),
        ),
      );
      if (reads.some((read) => read.status === "unavailable")) {
        return familyCalendarMonthViewSchema.parse({
          status: "temporarily_unavailable",
          month,
          timeZone,
          calendarName,
        });
      }
      const events = deduplicateFamilyCalendarEvents(reads.flatMap((read) => read.events));
      events.sort((left, right) => compareFamilyCalendarEvents(left, right, timeZone));
      return familyCalendarMonthViewSchema.parse({
        status: "ready",
        month,
        timeZone,
        calendarName,
        truncated: reads.some((read) => read.status === "truncated"),
        events: events.map((event) =>
          event.intervalKind === "all_day"
            ? {
                intervalKind: event.intervalKind,
                status: event.status,
                title: event.title,
                location: event.location,
                startDate: event.startDate,
                endDate: event.endDate,
              }
            : {
                intervalKind: event.intervalKind,
                status: event.status,
                title: event.title,
                location: event.location,
                startsAt: event.startsAt,
                endsAt: event.endsAt,
                timeZone: event.timeZone,
              },
        ),
      });
    } catch (error) {
      if (!(error instanceof GoogleCalendarTransientError) && !credentialInvalidGrant(error)) throw error;
      return familyCalendarMonthViewSchema.parse({
        status: "temporarily_unavailable",
        month,
        timeZone,
        calendarName,
      });
    }
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
      postalCode: input.postalCode,
      mode: input.mode,
      partner: input.partner,
      children: input.children.map((child) => ({
        firstName: child.firstName,
        ...(child.lastName ? { lastName: child.lastName } : {}),
        ...(child.school ? { school: child.school } : {}),
        ...(child.activities ? { activities: child.activities } : {}),
      })),
      occurredAt: this.#now().toISOString(),
    });
    const current = await this.#householdForAdult(adultId);
    const google = current.googleConnections.find(
      (connection) => connection.ownerAdultId === adultId && connection.status === "active",
    );
    if (google) await this.#stageFounderHandoff(google);
    await this.#store.ensureInitialIntelligence({
      householdId: current.id,
      now: this.#now().toISOString(),
    });
    this.#wake();
    return this.workspaceForAdult(adultId);
  }

  async putMember(
    adultId: string,
    memberId: string,
    untrustedInput: FamilyMemberMutationInput,
  ): Promise<WorkspaceView> {
    const input = familyMemberMutationInputSchema.parse(untrustedInput);
    const household = await this.#householdForAdult(adultId);
    const familyNameMayHaveChanged =
      !("kind" in input) &&
      input.lastName !== undefined &&
      household.members.some((member) => member.id === memberId && member.kind === "adult");
    await this.#store.upsertMember({
      householdId: household.id,
      actorAdultId: adultId,
      memberId,
      member:
        "kind" in input
          ? {
              operation: "create",
              kind: input.kind,
              firstName: input.firstName,
              lastName: input.lastName ?? null,
              profile: memberProfile(input),
            }
          : memberPatch(input),
      occurredAt: this.#now().toISOString(),
    });
    if (familyNameMayHaveChanged) await this.#ensureHouseholdActivation(household.id);
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

  async patchWatch(adultId: string, workId: string, untrustedInput: PatchWatchInput): Promise<WorkspaceView> {
    const input = patchWatchInputSchema.parse(untrustedInput);
    const household = await this.#householdForAdult(adultId);
    await this.#store.patchProactiveWork({
      householdId: household.id,
      actorAdultId: adultId,
      workId,
      kind: input.kind,
      ...(input.objective !== undefined ? { objective: input.objective } : {}),
      ...(input.kind === "monitor" && input.endCondition !== undefined
        ? { endCondition: input.endCondition }
        : {}),
      ...(input.kind === "interest" && input.genericTerms !== undefined
        ? { genericTerms: input.genericTerms }
        : {}),
      ...(input.status !== undefined ? { status: input.status } : {}),
      now: this.#now().toISOString(),
    });
    this.#wake();
    return this.workspaceForAdult(adultId);
  }

  async stopWatch(adultId: string, workId: string): Promise<WorkspaceView> {
    const household = await this.#householdForAdult(adultId);
    await this.#store.stopProactiveWork({
      householdId: household.id,
      actorAdultId: adultId,
      workId,
    });
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
    const adult = household.members.find((member) => member.id === input.adultId && member.kind === "adult");
    if (adult?.adultSlot === 2) {
      await this.#store.completePartnerOnboarding({
        householdId: household.id,
        adultId: adult.id,
        occurredAt: this.#now().toISOString(),
      });
    }
    if (adult?.adultSlot === 1 && profileString(adult.profile, "onboardingCompletedAt")) {
      await this.#stageFounderHandoff(connection);
    }
    await this.#ensureHouseholdActivation(household.id);
    await this.#store.ensureInitialIntelligence({
      householdId: household.id,
      now: this.#now().toISOString(),
    });
    this.#wake();
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
    this.#wake();
    return this.workspaceForAdult(adultId);
  }

  resolveLinqAuthority(
    input: Parameters<PostgresFlorenceStore["resolveLinqAuthority"]>[0],
  ): Promise<LinqAuthority | null> {
    return this.#store.resolveLinqAuthority(input);
  }

  transcribeVoiceNote(input: FlorenceVoiceNoteInput): Promise<string> {
    if (!this.#reasoner) {
      throw new FlorenceReasonerError("configuration", "Florence voice-note transcription is not configured");
    }
    return this.#reasoner.transcribeVoiceNote(input);
  }

  async respondBeforeEnrollment(input: {
    providerEventId: string;
    providerConversationId: string;
    identitySubjectDigest: string;
    text: string;
    occurredAt: string;
    carrierOptOut: boolean;
  }): Promise<boolean> {
    const expectedAuthority = {
      audience: "private" as const,
      participantIdentityDigests: [input.identitySubjectDigest],
    };
    const checkedAt = this.#now().toISOString();
    const invitation = await this.#store.readUnboundPartnerInvitation({
      providerConversationId: input.providerConversationId,
      identitySubjectDigest: input.identitySubjectDigest,
      now: checkedAt,
    });
    let bubbles: readonly { text: string; delayMs: number }[];
    let idempotencyPrefix: "founder-setup" | "partner-setup-expired" | "partner-setup-reply";

    if (invitation) {
      if (invitation.state === "declined") return true;
      if (input.carrierOptOut) {
        await this.#store.declinePartnerInvitation({
          adultId: invitation.adultId,
          providerConversationId: input.providerConversationId,
          identitySubjectDigest: input.identitySubjectDigest,
          occurredAt: input.occurredAt,
        });
        return true;
      }
      if (invitation.state === "expired") {
        await this.#store.expirePartnerInvitations({ now: checkedAt });
        bubbles = [
          {
            text: "That Florence setup link has expired. Ask your partner to send a fresh invitation.",
            delayMs: 0,
          },
        ];
        idempotencyPrefix = "partner-setup-expired";
      } else {
        if (!this.#reasoner) {
          throw new LinqError(
            "provider_retryable",
            "Florence setup interpretation is temporarily unavailable",
            true,
          );
        }
        let conversation: Awaited<ReturnType<FlorenceReasoner["converseDuringSetup"]>>;
        try {
          conversation = await this.#reasoner.converseDuringSetup({
            stage: "partner_invited",
            parentName: null,
            currentMessage: { text: input.text, occurredAt: input.occurredAt },
            recentMessages: [],
            nextStep: "use_existing_partner_setup_link",
          });
        } catch (error) {
          throw new LinqError(
            "provider_retryable",
            `Florence could not interpret the invited partner reply: ${errorText(error)}`,
            true,
          );
        }
        if (conversation.stopMessaging || conversation.declineInvitation) {
          await this.#store.declinePartnerInvitation({
            adultId: invitation.adultId,
            providerConversationId: input.providerConversationId,
            identitySubjectDigest: input.identitySubjectDigest,
            occurredAt: input.occurredAt,
          });
          return true;
        }
        bubbles = conversation.bubbles;
        idempotencyPrefix = "partner-setup-reply";
      }
    } else {
      if (input.carrierOptOut) return true;
      if (!this.#setupOrigin || !this.#google) {
        throw new Error("Google Workspace onboarding is not configured");
      }
      const setup = this.#enrollmentCodes.issueFounderSetup({
        providerConversationId: input.providerConversationId,
        identitySubjectDigest: input.identitySubjectDigest,
        occurredAt: input.occurredAt,
      });
      const setupUrl = `${this.#setupOrigin}/#s=${encodeURIComponent(setup.token)}`;
      const fallback = {
        stopMessaging: false,
        declineInvitation: false,
        bubbles: [
          {
            text: "Hey—I’m Florence. I help parents keep school, schedules, and family loose ends from becoming another job. Here’s your private setup link.",
            delayMs: 0,
          },
        ],
      };
      let conversation = fallback;
      if (this.#reasoner) {
        try {
          conversation = await this.#reasoner.converseDuringSetup({
            stage: "unclaimed",
            parentName: null,
            currentMessage: { text: input.text, occurredAt: input.occurredAt },
            recentMessages: [],
            nextStep: "signed_link_will_follow",
          });
        } catch {
          conversation = fallback;
        }
      }
      if (conversation.stopMessaging || conversation.declineInvitation) return true;
      bubbles = [...conversation.bubbles, { text: setupUrl, delayMs: 0 }];
      idempotencyPrefix = "founder-setup";
    }

    await this.#setTyping({
      providerConversationId: input.providerConversationId,
      expectedAuthority,
      active: true,
    });
    try {
      for (const [index, bubble] of bubbles.entries()) {
        if (index > 0) await pause(Math.max(650, bubble.delayMs));
        const result = await this.#linq.sendMessage({
          idempotencyKey: `${idempotencyPrefix}:${deterministicUuid(`${input.providerEventId}\0${index}`)}`,
          providerConversationId: input.providerConversationId,
          expectedAuthority,
          text: bubble.text,
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
    const checkedAt = this.#now();
    if (!isIanaTimeZone(input.profile.timeZone)) return null;
    const founderSetup = this.#enrollmentCodes.verifyFounderSetup(input.setupToken, checkedAt);
    const partnerSetup = founderSetup
      ? null
      : this.#enrollmentCodes.verifyPartnerSetup(input.setupToken, checkedAt);
    const setup = founderSetup ?? partnerSetup;
    if (!setup) return null;
    const observed = await this.#linq.observeChat(setup.providerConversationId);
    if (
      observed.audience !== "private" ||
      observed.participantIdentityDigests.length !== 1 ||
      observed.participantIdentityDigests[0] !== setup.identitySubjectDigest ||
      observed.participants.length !== 1 ||
      observed.participants[0]?.identitySubjectDigest !== setup.identitySubjectDigest
    ) {
      return null;
    }
    const completedAt = this.#now();
    const occurredAt = completedAt.toISOString();
    if (partnerSetup) {
      if (!this.#enrollmentCodes.verifyPartnerSetup(input.setupToken, completedAt)) return null;
      return this.#store.redeemMessagesEnrollment({
        challengeDigest: this.#enrollmentCodes.digestPartnerSetup(input.setupToken),
        identitySubjectDigest: partnerSetup.identitySubjectDigest,
        messagesAddress: observed.participants[0].phoneNumber,
        firstName: input.profile.firstName,
        lastName: input.profile.lastName,
        consentVersion: "linq-private-setup-v1",
        consentedAt: occurredAt,
        guardianAttestedAt: occurredAt,
        proactiveUseAcceptedAt: occurredAt,
        privateConflictBusySharingEnabled: input.profile.privateConflictBusySharingEnabled,
        providerConversationId: partnerSetup.providerConversationId,
        occurredAt,
      });
    }
    if (!founderSetup || !this.#enrollmentCodes.verifyFounderSetup(input.setupToken, completedAt)) {
      return null;
    }
    const completion: CompleteFounderOnboardingInput = {
      setupTokenDigest: this.#enrollmentCodes.digestFounderSetup(input.setupToken),
      setupExpiresAt: founderSetup.expiresAt,
      householdId: founderSetup.householdId,
      timeZone: input.profile.timeZone,
      adultId: founderSetup.adultId,
      firstName: input.profile.firstName,
      lastName: input.profile.lastName,
      messagesAddress: observed.participants[0].phoneNumber,
      identitySubjectDigest: founderSetup.identitySubjectDigest,
      consentVersion: "linq-private-setup-v1",
      consentedAt: occurredAt,
      guardianAttestedAt: occurredAt,
      proactiveUseAcceptedAt: occurredAt,
      privateConflictBusySharingEnabled: input.profile.privateConflictBusySharingEnabled,
      providerConversationId: founderSetup.providerConversationId,
      occurredAt,
    };
    const result = await this.#store.completeFounderOnboarding(completion);
    return result;
  }

  async reconcileObservedFamilyGroup(input: {
    providerConversationId: string;
    audience: "private" | "group";
    participantIdentityDigests: readonly string[];
    occurredAt: string;
  }) {
    const result = await this.#store.reconcileObservedFamilyGroup(input);
    if (result === "mismatch") this.#wake();
    return result;
  }

  async acceptInbound(input: AcceptInboundInput): Promise<AcceptInboundResult | null> {
    const active = this.#activeInbound;
    const sameActiveConversation = active?.providerConversationId === input.providerConversationId;
    const incomingSourceId = deterministicUuid(`linq-v3\0signal\0${input.providerEventId}`);
    const supersedesActive = Boolean(
      active &&
        sameActiveConversation &&
        isLaterInbound(input.occurredAt, incomingSourceId, active.latestOccurredAt, active.latestSourceId),
    );
    const acceptance = this.#store.acceptInbound({
      ...input,
      ...(active && supersedesActive ? { supersedesSourceId: active.latestSourceId } : {}),
    });
    this.#pendingInboundAccepts.add(acceptance);
    void acceptance.then(
      () => this.#pendingInboundAccepts.delete(acceptance),
      () => this.#pendingInboundAccepts.delete(acceptance),
    );
    if (active && sameActiveConversation && !active.knownSourceIds.has(incomingSourceId)) {
      active.knownSourceIds.add(incomingSourceId);
      if (supersedesActive) active.controller.abort();
    }
    const result = await acceptance;
    const currentActive = this.#activeInbound;
    if (
      result &&
      currentActive &&
      result.sourceId !== currentActive.sourceId &&
      result.channelId === currentActive.channelId &&
      currentActive.providerConversationId === input.providerConversationId &&
      (result.disposition === "accepted" || result.disposition === "stopped") &&
      isLaterInbound(
        input.occurredAt,
        result.sourceId,
        currentActive.latestOccurredAt,
        currentActive.latestSourceId,
      )
    ) {
      currentActive.latestSourceId = result.sourceId;
      currentActive.latestOccurredAt = input.occurredAt;
      currentActive.controller.abort();
    }
    if (result?.disposition === "accepted") this.#wake();
    return result;
  }

  async acceptInboundWithPreparation(
    input: AcceptInboundEnvelopeInput,
    prepare: (context: InboundPreparationContext) => Promise<PreparedInboundContent>,
  ): Promise<AcceptInboundResult | null> {
    const active = this.#activeInbound;
    const sameActiveConversation = active?.providerConversationId === input.providerConversationId;
    const incomingSourceId = deterministicUuid(`linq-v3\0signal\0${input.providerEventId}`);
    const supersedesActive = Boolean(
      active &&
        sameActiveConversation &&
        isLaterInbound(input.occurredAt, incomingSourceId, active.latestOccurredAt, active.latestSourceId),
    );
    const acceptance = this.#store.acceptInboundWithPreparation(input, prepare, {
      resolveSupersedesSourceId: () => {
        const currentActive = this.#activeInbound;
        if (
          !currentActive ||
          currentActive.providerConversationId !== input.providerConversationId ||
          !isLaterInbound(
            input.occurredAt,
            incomingSourceId,
            currentActive.latestOccurredAt,
            currentActive.latestSourceId,
          )
        ) {
          return null;
        }
        return currentActive.latestSourceId;
      },
    });
    this.#pendingInboundAccepts.add(acceptance);
    void acceptance.then(
      () => this.#pendingInboundAccepts.delete(acceptance),
      () => this.#pendingInboundAccepts.delete(acceptance),
    );
    if (active && sameActiveConversation && !active.knownSourceIds.has(incomingSourceId)) {
      active.knownSourceIds.add(incomingSourceId);
      if (supersedesActive) active.controller.abort();
    }
    const result = await acceptance;
    const currentActive = this.#activeInbound;
    if (
      result &&
      currentActive &&
      result.sourceId !== currentActive.sourceId &&
      result.channelId === currentActive.channelId &&
      currentActive.providerConversationId === input.providerConversationId &&
      (result.disposition === "accepted" || result.disposition === "stopped") &&
      isLaterInbound(
        input.occurredAt,
        result.sourceId,
        currentActive.latestOccurredAt,
        currentActive.latestSourceId,
      )
    ) {
      currentActive.latestSourceId = result.sourceId;
      currentActive.latestOccurredAt = input.occurredAt;
      currentActive.controller.abort();
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
    await this.#settleInboundAccepts();
    await this.#purgeExpiredArtifacts();
    if ((await this.#store.expirePartnerInvitations({ now: this.#now().toISOString() })) > 0) {
      worked = true;
    }
    const inbound = await this.#store.readNextInbound(this.#now().toISOString());
    if (inbound) {
      await this.#handleInbound(inbound);
      worked = true;
    }
    await this.#settleInboundAccepts();
    const outbound = await this.#store.readNextOutbound(this.#now().toISOString());
    await this.#settleInboundAccepts();
    if (outbound) {
      await this.#deliverOutbound(outbound.sourceId);
      worked = true;
    }
    await this.#settleInboundAccepts();
    const familyGroup = this.#linqSenderPhoneNumber
      ? await this.#store.readNextFamilyGroupCreation(this.#now().toISOString())
      : null;
    if (familyGroup) {
      await this.#createFamilyGroup(familyGroup);
      worked = true;
    }
    const incompleteActivation =
      this.#google && this.#linqSenderPhoneNumber
        ? await this.#store.readNextIncompleteHouseholdActivation()
        : null;
    if (incompleteActivation) {
      await this.#ensureHouseholdActivation(incompleteActivation);
      await this.#store.ensureInitialIntelligence({
        householdId: incompleteActivation,
        now: this.#now().toISOString(),
      });
      worked = true;
    }
    await this.#settleInboundAccepts();
    const partnerInvitation = await this.#store.readNextPartnerInvitation(this.#now().toISOString());
    if (partnerInvitation) {
      await this.#executePartnerInvitation(partnerInvitation);
      worked = true;
    }
    await this.#settleInboundAccepts();
    const calendar = await this.#store.readNextCalendarAction(this.#now().toISOString());
    if (calendar) {
      await this.#executeCalendar(calendar);
      worked = true;
    }
    const initialIntelligence = await this.#store.readNextInitialIntelligence(this.#now().toISOString());
    if (initialIntelligence) {
      await this.#executeInitialIntelligence(initialIntelligence);
      worked = true;
    }
    const proactiveWork = await this.#store.readNextDueProactiveWork(this.#now().toISOString());
    if (proactiveWork) {
      await this.#executeProactiveWork(proactiveWork);
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
    const onboardingComplete = Boolean(sender && profileString(sender.profile, "onboardingCompletedAt"));
    if (turn.authority.audience === "private" && (!googleActive || !onboardingComplete)) {
      if (turn.message.moveKind === "reaction") {
        await this.#store.commitTurn({
          sourceId: turn.message.sourceId,
          handledAt: this.#now().toISOString(),
        });
        return;
      }
      const stage = googleActive ? "family_profile" : "connect_google";
      const fallbackText = googleActive
        ? "I’m here. Finish the short family setup on the private page, then send me the first thing you want off your plate."
        : "I’m here. Connect your own Google account on the private setup page so I can help with real family logistics.";
      let bubbles = [{ text: fallbackText, delayMs: 0 }];
      let stopMessaging = false;
      if (this.#reasoner) {
        try {
          const setup = await this.#reasoner.converseDuringSetup({
            stage,
            parentName: sender?.displayName ?? null,
            currentMessage: { text: turnText(turn.message), occurredAt: turn.message.occurredAt },
            recentMessages: turn.recentMessages.slice(-8).map((message) => ({
              sender: message.speaker === "florence" ? "florence" : "parent",
              text: turnText(message),
              occurredAt: message.occurredAt,
            })),
            nextStep: googleActive ? "finish_family_profile" : "connect_google",
          });
          bubbles = [...setup.bubbles];
          stopMessaging = setup.stopMessaging;
        } catch {
          bubbles = [{ text: fallbackText, delayMs: 0 }];
        }
      }
      if (stopMessaging) {
        await this.#store.commitTurn({
          sourceId: turn.message.sourceId,
          stopChannel: true,
          handledAt: this.#now().toISOString(),
        });
        return;
      }
      if (this.#setupOrigin) bubbles.push({ text: `${this.#setupOrigin}/`, delayMs: 0 });
      await this.#store.commitTurn(
        decisionCommit(
          turn,
          {
            policy: { retain: false, schedule: false, stopMessaging: false },
            conversation: {
              replyToCurrentMessage: true,
              reaction: null,
              bubbles,
            },
            facts: [],
            followUp: null,
            calendar: null,
            householdUpdate: null,
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

    let approvedCalendarOffer: InboundTurn["pendingCalendarOffers"][number] | null = null;
    let approvedPartnerInvitation: InboundTurn["pendingPartnerInvitation"] = null;
    const typedApprovalText = turn.message.authoredText?.trim() || null;
    if (
      turn.authority.audience === "private" &&
      turn.message.moveKind !== "reaction" &&
      typedApprovalText !== null &&
      turn.pendingPartnerInvitation &&
      approvalReplyTargetsPrompt(
        turn.message.replyToSourceId,
        turn.pendingPartnerInvitation.approvalPromptSourceId,
      )
    ) {
      try {
        const interpretation = await this.#reasoner.interpretPartnerInvitationApproval({
          currentMessage: { text: typedApprovalText },
          partner: {
            adultId: turn.pendingPartnerInvitation.adultId,
            firstName: turn.pendingPartnerInvitation.firstName,
            maskedPhoneNumber: turn.pendingPartnerInvitation.maskedPhoneNumber,
          },
        });
        if (interpretation.sendInvitation) approvedPartnerInvitation = turn.pendingPartnerInvitation;
      } catch (error) {
        if (error instanceof FlorenceReasonerError && error.retryable) {
          await this.#store.retryInbound({
            sourceId: turn.message.sourceId,
            retryAt: later(this.#now(), RETRY_MS),
            error: errorText(error),
          });
          return;
        }
        if (!(error instanceof FlorenceReasonerError)) throw error;
      }
    }
    if (
      turn.authority.audience === "group" &&
      turn.message.moveKind !== "reaction" &&
      typedApprovalText !== null &&
      turn.pendingCalendarOffers.length === 1 &&
      approvalReplyTargetsPrompt(
        turn.message.replyToSourceId,
        turn.pendingCalendarOffers[0]?.approvalPromptSourceId ?? null,
      )
    ) {
      const offer = turn.pendingCalendarOffers[0];
      if (!offer) throw new Error("The sole Calendar offer disappeared");
      try {
        const interpretation = await this.#reasoner.interpretCalendarApproval({
          currentMessage: { text: typedApprovalText, occurredAt: turn.message.occurredAt },
          event: offer.event,
        });
        if (interpretation.approve) approvedCalendarOffer = offer;
      } catch (error) {
        if (error instanceof FlorenceReasonerError && error.retryable) {
          await this.#store.retryInbound({
            sourceId: turn.message.sourceId,
            retryAt: later(this.#now(), RETRY_MS),
            error: errorText(error),
          });
          return;
        }
        if (!(error instanceof FlorenceReasonerError)) throw error;
      }
    }

    const controller = new AbortController();
    const latestInbound = turn.supersededMessages.reduce(
      (latest, message) =>
        isLaterInbound(message.occurredAt, message.sourceId, latest.occurredAt, latest.sourceId)
          ? message
          : latest,
      turn.message,
    );
    const active: ActiveInbound = {
      sourceId: turn.message.sourceId,
      latestSourceId: latestInbound.sourceId,
      latestOccurredAt: latestInbound.occurredAt,
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
      const guarded = enforcePolicy(decision, turn.message.moveKind !== "reaction");
      const approval = guarded.policy.schedule ? approvedCalendarOffer : null;
      const partnerApproval = guarded.policy.stopMessaging ? null : approvedPartnerInvitation;
      const committedDecision = approval ? { ...guarded, calendar: null } : guarded;
      await this.#store.commitTurn(
        decisionCommit(turn, committedDecision, this.#now(), {
          omitReaction: immediateReactionStaged,
          approveCalendarOffer: approval,
          approvePartnerInvitation: partnerApproval,
          googleEvidence: context.googleEvidence(),
        }),
      );
    } catch (error) {
      if (controller.signal.aborted) return;
      if (error instanceof FlorenceReasonerError && !error.retryable) {
        if (workTimer) clearTimeout(workTimer);
        workTimer = null;
        if (workCue) await workCue;
        if (approvedCalendarOffer || approvedPartnerInvitation) {
          const actionText =
            approvedCalendarOffer && approvedPartnerInvitation
              ? `I’ll add the calendar item and text ${approvedPartnerInvitation.firstName} now. I couldn’t finish the rest reliably.`
              : approvedCalendarOffer
                ? "I’ll handle the calendar item you approved. I couldn’t finish the rest reliably."
                : `I’ll text ${approvedPartnerInvitation?.firstName ?? "your partner"} now. I couldn’t finish the rest reliably.`;
          await this.#store.commitTurn(
            decisionCommit(
              turn,
              {
                policy: { retain: false, schedule: true, stopMessaging: false },
                conversation: {
                  replyToCurrentMessage: true,
                  reaction: null,
                  bubbles: [
                    {
                      text: actionText,
                      delayMs: 0,
                    },
                  ],
                },
                facts: [],
                followUp: null,
                calendar: null,
                householdUpdate: null,
              },
              this.#now(),
              {
                omitReaction: immediateReactionStaged,
                approveCalendarOffer: approvedCalendarOffer,
                approvePartnerInvitation: approvedPartnerInvitation,
              },
            ),
          );
        } else {
          await this.#store.commitTurn(
            attachmentJob
              ? decisionCommit(
                  turn,
                  {
                    policy: { retain: false, schedule: false, stopMessaging: false },
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
                    householdUpdate: null,
                  },
                  this.#now(),
                  { omitReaction: immediateReactionStaged },
                )
              : { sourceId: turn.message.sourceId, handledAt: this.#now().toISOString() },
          );
        }
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
    googleEvidence: () => readonly GoogleEvidenceDraft[];
  }> {
    const members = new Map(turn.household.members.map((member) => [member.id, member.displayName]));
    const visibleSources = memorySources(turn.facts);
    const sourceIndex = new Map(visibleSources.map((source) => [source.sourceId, source]));
    const googleEvidence = new Map<string, GoogleEvidenceDraft>();
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

    const familyCalendarCredential =
      turn.authority.audience === "group"
        ? await this.#store.readActiveFamilyCalendarCredential({
            householdId: turn.authority.householdId,
          })
        : null;
    const familyCalendarOwner = turn.household.members.find(
      (member) => member.id === familyCalendarCredential?.ownerAdultId,
    );
    const googleConnections = !this.#google
      ? []
      : turn.authority.audience === "private"
        ? await this.#google.status({
            householdId: turn.authority.householdId,
            ownerAdultId: turn.authority.senderAdultId,
          })
        : familyCalendarCredential && familyCalendarOwner
          ? (
              await this.#google.status({
                householdId: turn.authority.householdId,
                ownerAdultId: familyCalendarOwner.id,
              })
            ).filter((connection) => connection.connectionId === familyCalendarCredential.connectionId)
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
            profile: familyProfileForReasoning(member),
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
        authoredText: turn.message.authoredText,
        voiceTranscriptPresent: turn.message.voiceTranscriptPresent,
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
        objective: followUp.objective,
        currentConclusion: followUp.currentConclusion,
        endCondition: followUp.endCondition,
        nextCheck: followUp.nextCheck,
        why: followUp.why,
        sourceIds: [...followUp.sourceIds],
      })),
      visibleInterests: turn.visibleInterests.map((interest) => ({
        interestWorkId: interest.interestWorkId,
        status: interest.status,
        genericTerms: [...interest.genericTerms],
        objective: interest.objective,
        why: interest.why,
      })),
      pendingCalendarOffers: turn.pendingCalendarOffers.map((offer) => ({
        proposalId: offer.id,
        event: offer.event,
        sourceIds: [offer.approvalPromptSourceId],
      })),
      googleConnections: googleConnections.flatMap((connection) =>
        connection.status === "active" && connection.emailLabel
          ? [
              {
                connectionId: connection.connectionId,
                emailLabel:
                  turn.authority.audience === "group"
                    ? (turn.household.familyCalendarLabel ?? turn.household.name)
                    : connection.emailLabel,
                calendarId: turn.authority.audience === "group" ? turn.household.familyCalendarId : null,
                kind: turn.authority.audience === "group" ? ("family" as const) : ("personal" as const),
              },
            ]
          : [],
      ),
    };

    const reads: FlorenceReadTools = {
      searchFamilyMemory: async ({ query, limit }) => searchSources(visibleSources, query).slice(0, limit),
      readCalendarWindow: async ({ connectionId, timeMin, timeMax, limit }) => {
        if (
          !this.#google ||
          !googleConnections.some(
            (connection) => connection.connectionId === connectionId && connection.status === "active",
          )
        ) {
          return { status: "unavailable", events: [] };
        }
        try {
          const read = await this.#google.readCalendarWindow({
            householdId: turn.authority.householdId,
            ownerAdultId:
              turn.authority.audience === "group"
                ? requiredText(familyCalendarOwner?.id ?? null, "Family Calendar owner")
                : turn.authority.senderAdultId,
            connectionId,
            ...(turn.authority.audience === "group" && turn.household.familyCalendarId
              ? { calendarId: turn.household.familyCalendarId }
              : {}),
            timeMin,
            timeMax,
            limit,
          });
          return {
            status: read.status,
            events: read.events.map((event) =>
              event.intervalKind === "all_day"
                ? {
                    intervalKind: event.intervalKind,
                    title: event.title,
                    startDate: event.startDate,
                    endDate: event.endDate,
                    providerEventId: event.providerEventId,
                    providerRevision: event.providerRevision,
                    location: event.location,
                  }
                : {
                    intervalKind: event.intervalKind,
                    title: event.title,
                    startsAt: event.startsAt,
                    endsAt: event.endsAt,
                    providerEventId: event.providerEventId,
                    providerRevision: event.providerRevision,
                    timeZone: event.timeZone,
                    location: event.location,
                  },
            ),
          };
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
        return evidence.messages.map((message) => {
          const source = draftGmailEvidence({
            householdId: turn.authority.householdId,
            ownerAdultId: turn.authority.senderAdultId,
            connectionId,
            ...message,
          });
          googleEvidence.set(source.id, source);
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
        });
      },
    };
    return { input, reads, googleEvidence: () => [...googleEvidence.values()] };
  }

  async #deliverOutbound(sourceId: string, retryTransient = true): Promise<void> {
    const outbound = await this.#store.beginOutbound({ sourceId, now: this.#now().toISOString() });
    if (!outbound) return;
    try {
      const observed = await this.#linq.observeChat(outbound.providerConversationId);
      const groupObservation = await this.reconcileObservedFamilyGroup({
        providerConversationId: outbound.providerConversationId,
        audience: observed.audience,
        participantIdentityDigests: observed.participantIdentityDigests,
        occurredAt: this.#now().toISOString(),
      });
      if (groupObservation === "mismatch" || groupObservation === "retired") return;
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
        id: action.actionId,
        retryAt: later(this.#now(), RETRY_MS),
        error: "Google Calendar is not configured",
      });
      return;
    }
    const title = action.mutation.event?.title ?? action.mutation.target?.observedEvent.title ?? "that event";
    try {
      const result = await this.#google.executeCalendar(action);
      if (result.status === "credential_rejected") {
        await this.#google.disconnect({
          connectionId: action.connectionId,
          householdId: action.householdId,
          ownerAdultId: action.ownerAdultId,
          now: result.occurredAt,
        });
        return;
      }
      if (result.status === "failed") {
        await this.#store.failCalendarAction({
          id: action.actionId,
          error: result.detail,
          failureText: `I couldn’t confirm the ${calendarOperationNoun(action.mutation.operation)} for “${title}.” Please check the family calendar before trying again.`,
          failedAt: result.occurredAt,
        });
        return;
      }
      await this.#store.completeCalendarAction({
        id: action.actionId,
        providerEventId: result.providerEventId,
        providerRevision: result.providerRevision,
        confirmationText: `${calendarConfirmationVerb(action.mutation.operation)} “${title}” ${action.mutation.operation === "delete" ? "from" : "on"} the family calendar.`,
        committedAt: result.occurredAt,
      });
    } catch (error) {
      if (credentialInvalidGrant(error)) return;
      await this.#store.retryCalendarAction({
        id: action.actionId,
        retryAt: later(this.#now(), RETRY_MS),
        error: errorText(error),
      });
    }
  }

  async #executeInitialIntelligence(work: InitialIntelligenceWork): Promise<void> {
    if (!this.#google || !this.#reasoner) {
      await this.#store.retryInitialIntelligence({
        workId: work.workId,
        retryAt: later(this.#now(), RETRY_MS),
        failedAt: this.#now().toISOString(),
        error: "Florence's proactive Google review is not configured",
      });
      return;
    }
    try {
      if (work.kind === "initial_household_briefing") {
        const familyCalendar = await this.#google.readInitialCalendarReview({
          householdId: work.household.householdId,
          ownerAdultId: work.familyCalendarOwnerAdultId,
          connectionId: work.familyCalendarOwnerConnectionId,
          calendarId: work.familyCalendarId,
          currentTime: this.#now().toISOString(),
          limit: 50,
        });
        if (familyCalendar.status === "unavailable" || !familyCalendar.cursor) {
          throw new Error("The Family Calendar baseline is temporarily unavailable");
        }
        const decision = await this.#reasoner.synthesizeHouseholdBriefing({
          familyProfile: initialFamilyProfile(work.household),
          candidates: work.candidates.map((candidate) => ({ ...candidate })),
        });
        await this.#store.completeHouseholdInitialBriefing({
          workId: work.workId,
          selectedCandidateIds: decision.selectedCandidateIds,
          familyCalendarCursor: JSON.stringify(familyCalendar.cursor),
          bubbles: decision.bubbles,
          occurredAt: this.#now().toISOString(),
        });
        return;
      }

      const currentTime = this.#now().toISOString();
      const gmailCursor = await this.#google.captureGmailCursor({
        householdId: work.household.householdId,
        ownerAdultId: work.adultId,
        connectionId: work.connectionId,
      });
      const attachmentIndex = new Map<string, GmailAttachmentReference>();
      const googleEvidence = new Map<string, GoogleEvidenceDraft>();
      let calendarCursor: string | null = null;
      const decision = await this.#reasoner.reviewPrivateGoogle(
        {
          familyProfile: initialFamilyProfile(work.household),
          adult: { adultId: work.adultId, firstName: work.adultFirstName },
          googleConnection: {
            connectionId: work.connectionId,
            status: "active",
            kind: "personal",
          },
          currentTime,
          currentPrivateFacts: [...work.currentPrivateFacts],
        },
        {
          searchGmail: async ({ connectionId, query, after, before, limit }) => {
            if (connectionId !== work.connectionId) {
              throw new Error("The private review requested another adult's Gmail connection");
            }
            const result = await this.#google?.searchGmail({
              householdId: work.household.householdId,
              ownerAdultId: work.adultId,
              connectionId,
              query,
              after,
              before,
              limit,
            });
            if (!result) throw new Error("Google is not configured");
            return result.messages.map((message): FlorencePrivateGmailSource => {
              const source = draftGmailEvidence({
                householdId: work.household.householdId,
                ownerAdultId: work.adultId,
                connectionId,
                messageId: message.messageId,
                threadId: message.threadId,
                historyId: message.historyId,
                from: message.from,
                subject: message.subject,
                sentAt: message.sentAt,
              });
              googleEvidence.set(source.id, source);
              for (const attachment of message.attachments) {
                attachmentIndex.set(`${source.id}\0${attachment.attachmentId}`, attachment);
              }
              return {
                sourceId: source.id,
                kind: "gmail",
                visibility: "adult_private",
                sentAt: message.sentAt,
                sender: message.from,
                subject: message.subject,
                text: message.text,
                attachments: message.attachments.map((attachment) => ({
                  attachmentId: attachment.attachmentId,
                  filename: attachment.filename,
                  mimeType: attachment.mimeType,
                  sizeBytes: attachment.sizeBytes,
                })),
              };
            });
          },
          readPersonalCalendarWindow: async ({ connectionId, timeMin, timeMax, limit }) => {
            if (connectionId !== work.connectionId) {
              throw new Error("The private review requested another adult's Calendar connection");
            }
            const expectedTimeMin = currentTime;
            const expectedTimeMax = new Date(Date.parse(currentTime) + 21 * 24 * 60 * 60_000).toISOString();
            if (timeMin !== expectedTimeMin || timeMax !== expectedTimeMax) {
              throw new Error("The private review requested a different Calendar window");
            }
            const read = await this.#google?.readInitialCalendarReview({
              householdId: work.household.householdId,
              ownerAdultId: work.adultId,
              connectionId,
              currentTime,
              limit,
            });
            if (!read || read.status === "unavailable" || !read.cursor) {
              throw new Error("The private Calendar review is temporarily unavailable");
            }
            calendarCursor = JSON.stringify(read.cursor);
            return {
              status: read.status,
              events: read.events.map((event) => {
                const interval = calendarEvidenceInterval(event, work.household.timeZone);
                const source = draftCalendarEvidence({
                  householdId: work.household.householdId,
                  ownerAdultId: work.adultId,
                  connectionId,
                  calendarId: "primary",
                  providerEventId: event.providerEventId,
                  providerRevision: event.providerRevision,
                  providerUpdatedAt: event.providerUpdatedAt,
                  status: event.status,
                  busy: event.busy,
                  title: event.title,
                  ...interval,
                });
                googleEvidence.set(source.id, source);
                return privateCalendarEvidence(source, event, "adult_private");
              }),
            };
          },
          readGmailAttachment: async ({ connectionId, sourceId, attachment }) => {
            if (connectionId !== work.connectionId) {
              throw new Error("The private review requested another adult's Gmail attachment");
            }
            const reference = attachmentIndex.get(`${sourceId}\0${attachment.attachmentId}`);
            if (!reference) throw new Error("The Gmail attachment was not returned by this review");
            const read = await this.#google?.readGmailAttachment({
              householdId: work.household.householdId,
              ownerAdultId: work.adultId,
              connectionId,
              attachment: reference,
            });
            if (!read) throw new Error("Google is not configured");
            return {
              sourceId,
              attachmentId: read.attachmentId,
              filename: read.filename,
              mimeType: read.mimeType,
              bytes: read.bytes,
            };
          },
        },
      );
      if (!calendarCursor) {
        throw new Error("The private Google review did not establish Calendar coverage");
      }
      await this.#store.completePrivateInitialReview({
        workId: work.workId,
        gmailCursor: JSON.stringify(gmailCursor),
        calendarCursor,
        bubbles: decision.bubbles,
        findings: decision.findings.map((finding) => ({
          sourceIds: finding.sourceIds,
          householdCandidate: finding.candidate,
          monitor: finding.monitor ?? null,
          familyCalendar: finding.familyCalendar ?? null,
        })),
        facts: decision.facts,
        googleEvidence: [...googleEvidence.values()],
        occurredAt: this.#now().toISOString(),
      });
    } catch (error) {
      if (credentialInvalidGrant(error)) return;
      await this.#store.retryInitialIntelligence({
        workId: work.workId,
        retryAt: later(this.#now(), RETRY_MS),
        failedAt: this.#now().toISOString(),
        error: errorText(error),
      });
    }
  }

  async #executeProactiveWork(work: DueProactiveWork): Promise<void> {
    if (!this.#google || !this.#reasoner) {
      await this.#store.retryProactiveWork({
        workId: work.workId,
        retryAt: later(this.#now(), RETRY_MS),
        error: "Florence's proactive review is not configured",
      });
      return;
    }
    const google = this.#google;
    const currentTime = this.#now().toISOString();
    const calendarTimeMax = new Date(Date.parse(currentTime) + 21 * 24 * 60 * 60_000).toISOString();
    const gmailAfter = new Date(Date.parse(currentTime) - 90 * 24 * 60 * 60_000).toISOString();
    try {
      if (work.kind === "interest_monitor") {
        const household = await this.#store.readHousehold({
          householdId: work.household.householdId,
        });
        if (!household) throw new Error("The interest household is unavailable");
        const personalConnections = household.googleConnections.filter(
          (connection) =>
            connection.status === "active" &&
            household.members.some(
              (member) =>
                member.id === connection.ownerAdultId &&
                proactiveGoogleAccessEnabled(member) &&
                member.preferences.privateConflictBusySharingEnabled === true,
            ),
        );
        const [familyCalendar, ...personalCalendars] = await Promise.all([
          google.readInitialCalendarReview({
            householdId: work.household.householdId,
            ownerAdultId: work.ownerAdultId,
            connectionId: work.connectionId,
            calendarId: work.calendarId,
            currentTime,
            limit: 50,
          }),
          ...personalConnections.map((connection) =>
            google.readInitialCalendarReview({
              householdId: work.household.householdId,
              ownerAdultId: connection.ownerAdultId,
              connectionId: connection.connectionId,
              currentTime,
              limit: 50,
            }),
          ),
        ]);
        const busyIntervals = [...personalCalendars, familyCalendar]
          .flatMap((calendar) =>
            !calendar || calendar.status === "unavailable"
              ? []
              : calendar.events
                  .map((event) => calendarWindowBounds(event, work.household.timeZone))
                  .filter((event) => event.endsAt > currentTime),
          )
          .filter(
            (interval, index, intervals) =>
              intervals.findIndex(
                (candidate) =>
                  candidate.startsAt === interval.startsAt && candidate.endsAt === interval.endsAt,
              ) === index,
          )
          .slice(0, 50);
        const result = await this.#reasoner.researchInterest({
          currentTime,
          timeZone: work.household.timeZone,
          genericInterestTerms: [...work.genericInterestTerms],
          ageBracket: "all_ages",
          location: {
            city: null,
            postalCode: work.coarseLocation,
            countryCode: "US",
          },
          busyIntervals,
        });
        const completedAt = this.#now();
        await this.#store.completeInterestMonitor({
          workId: work.workId,
          ...result,
          deliverNotBefore: proactiveDeliveryAt(completedAt, work.household.timeZone, false).toISOString(),
          occurredAt: completedAt.toISOString(),
        });
        return;
      }

      if (work.kind === "finite_monitor") {
        const googleEvidence = new Map<string, GoogleEvidenceDraft>();
        const split = new Date(Date.parse(currentTime) - 14 * 24 * 60 * 60_000).toISOString();
        const [recent, earlier] =
          work.visibility === "private"
            ? await Promise.all([
                this.#google.searchGmail({
                  householdId: work.household.householdId,
                  ownerAdultId: work.adultId,
                  connectionId: work.connectionId,
                  query: "-category:promotions -category:social",
                  after: split,
                  before: currentTime,
                  limit: 20,
                }),
                this.#google.searchGmail({
                  householdId: work.household.householdId,
                  ownerAdultId: work.adultId,
                  connectionId: work.connectionId,
                  query: "-category:promotions -category:social",
                  after: gmailAfter,
                  before: split,
                  limit: 20,
                }),
              ])
            : [null, null];
        const calendar = await this.#google.readInitialCalendarReview({
          householdId: work.household.householdId,
          ownerAdultId: work.adultId,
          connectionId: work.connectionId,
          calendarId: work.calendarId,
          currentTime,
          limit: 50,
        });
        const seenMessages = new Set<string>();
        const gmailMessages = [...(recent?.messages ?? []), ...(earlier?.messages ?? [])].filter(
          (message) => {
            if (seenMessages.has(message.messageId)) return false;
            seenMessages.add(message.messageId);
            return true;
          },
        );
        const gmailSources = gmailMessages.map((message): FlorencePrivateGmailSource => {
          const source = draftGmailEvidence({
            householdId: work.household.householdId,
            ownerAdultId: work.adultId,
            connectionId: work.connectionId,
            messageId: message.messageId,
            threadId: message.threadId,
            historyId: message.historyId,
            from: message.from,
            subject: message.subject,
            sentAt: message.sentAt,
          });
          googleEvidence.set(source.id, source);
          return {
            sourceId: source.id,
            kind: "gmail",
            visibility: "adult_private",
            sentAt: message.sentAt,
            sender: message.from,
            subject: message.subject,
            text: message.text,
            attachments: message.attachments.map((attachment) => ({
              attachmentId: attachment.attachmentId,
              filename: attachment.filename,
              mimeType: attachment.mimeType,
              sizeBytes: attachment.sizeBytes,
            })),
          };
        });
        const calendarSources =
          calendar.status === "unavailable"
            ? []
            : calendar.events.map((event) => {
                const interval = calendarEvidenceInterval(event, work.household.timeZone);
                const source = draftCalendarEvidence({
                  householdId: work.household.householdId,
                  ownerAdultId: work.adultId,
                  connectionId: work.connectionId,
                  calendarId: work.calendarId,
                  visibility: work.visibility,
                  providerEventId: event.providerEventId,
                  providerRevision: event.providerRevision,
                  providerUpdatedAt: event.providerUpdatedAt,
                  status: event.status,
                  busy: event.busy,
                  title: event.title,
                  ...interval,
                });
                googleEvidence.set(source.id, source);
                return privateCalendarEvidence(
                  source,
                  event,
                  work.visibility === "household" ? "shared" : "adult_private",
                );
              });
        const currentEvidence: FlorenceBoundedPrivateGoogleEvidence = {
          gmail: {
            status:
              recent?.status === "truncated" || earlier?.status === "truncated" ? "truncated" : "complete",
            after: gmailAfter,
            before: currentTime,
            sources: gmailSources,
          },
          calendar: {
            status: calendar.status,
            timeMin: currentTime,
            timeMax: calendarTimeMax,
            events: calendarSources,
          },
        };
        const result = await this.#reasoner.reviewFiniteMonitor({
          familyProfile: initialFamilyProfile(work.household),
          adult: { adultId: work.adultId, firstName: work.adultFirstName },
          googleConnection: {
            connectionId: work.connectionId,
            status: "active",
            kind: work.visibility === "private" ? "personal" : "family",
          },
          scope: work.visibility,
          currentTime,
          evidence: currentEvidence,
          monitor: work.monitor,
        });
        const completedAt = this.#now();
        await this.#store.completeFiniteMonitor({
          workId: work.workId,
          outcome: result.outcome,
          privateDetail: work.visibility === "private" ? result.privateDetail : null,
          householdConclusion: result.householdConclusion?.summary ?? null,
          householdCategory: result.householdConclusion?.category ?? null,
          sourceIds: result.sourceIds,
          currentConclusion:
            work.visibility === "household" && result.householdConclusion
              ? result.householdConclusion.summary
              : result.currentConclusion,
          nextCheck: result.nextCheck,
          why:
            work.visibility === "household"
              ? "Florence is watching this family coordination item."
              : result.why,
          googleEvidence: [...googleEvidence.values()],
          deliverNotBefore: proactiveDeliveryAt(
            completedAt,
            work.household.timeZone,
            result.urgency === "now",
          ).toISOString(),
          occurredAt: completedAt.toISOString(),
        });
        return;
      }

      const gmailMessages: GmailEvidence[] = [];
      let nextGmailCursor: GoogleGmailCursor | null = null;
      let gmailStatus: "complete" | "truncated" | "unavailable" = "unavailable";
      if (work.kind === "personal_google_poll") {
        const cursor = googleGmailCursor(work.gmailCursor);
        const changes = await this.#google.readGmailChanges({
          householdId: work.household.householdId,
          ownerAdultId: work.adultId,
          connectionId: work.connectionId,
          cursor,
        });
        if (changes.resyncRequired) {
          nextGmailCursor = await this.#google.captureGmailCursor({
            householdId: work.household.householdId,
            ownerAdultId: work.adultId,
            connectionId: work.connectionId,
          });
          const resyncBefore = nextGmailCursor.capturedAt;
          const split = new Date(Date.parse(resyncBefore) - 14 * 24 * 60 * 60_000).toISOString();
          const [recent, earlier] = await Promise.all([
            this.#google.searchGmail({
              householdId: work.household.householdId,
              ownerAdultId: work.adultId,
              connectionId: work.connectionId,
              query: "-category:promotions -category:social",
              after: split,
              before: resyncBefore,
              limit: 20,
            }),
            this.#google.searchGmail({
              householdId: work.household.householdId,
              ownerAdultId: work.adultId,
              connectionId: work.connectionId,
              query: "-category:promotions -category:social",
              after: gmailAfter,
              before: split,
              limit: 20,
            }),
          ]);
          if (recent.status === "truncated" || earlier.status === "truncated") {
            throw new Error("The bounded Gmail resync could not cover every matching message");
          }
          const seen = new Set<string>();
          for (const message of [...recent.messages, ...earlier.messages]) {
            if (!seen.has(message.messageId)) gmailMessages.push(message);
            seen.add(message.messageId);
          }
          gmailStatus = "complete";
        } else {
          nextGmailCursor = changes.cursor;
          gmailStatus = "complete";
          gmailMessages.push(
            ...changes.messages.filter(
              (message) => message.sentAt >= gmailAfter && message.sentAt <= currentTime,
            ),
          );
        }
      }

      const calendarCursor = googleCalendarCursor(work.calendarCursor);
      const calendarChanges = await this.#google.readCalendarChanges({
        householdId: work.household.householdId,
        ownerAdultId: work.adultId,
        connectionId: work.connectionId,
        calendarId: work.calendarId,
        cursor: calendarCursor,
        currentTime,
      });
      let nextCalendarCursor = calendarCursor;
      let calendarStatus: "complete" | "truncated" | "unavailable" = "complete";
      let calendarEvents: readonly (GoogleCalendarWindowEvent | GoogleCalendarChange)[] = [];
      if (calendarChanges.resyncRequired) {
        const baseline = await this.#google.readInitialCalendarReview({
          householdId: work.household.householdId,
          ownerAdultId: work.adultId,
          connectionId: work.connectionId,
          calendarId: work.calendarId,
          currentTime,
          limit: 50,
        });
        if (baseline.status === "unavailable" || !baseline.cursor) {
          calendarStatus = "unavailable";
        } else if (baseline.status === "truncated") {
          throw new Error("The bounded Calendar resync could not cover the rolling window");
        } else {
          nextCalendarCursor = baseline.cursor;
          calendarStatus = baseline.status;
          calendarEvents = baseline.events;
        }
      } else if (calendarChanges.status === "unavailable") {
        calendarStatus = "unavailable";
      } else {
        nextCalendarCursor = calendarChanges.cursor;
        calendarEvents = calendarChanges.events;
      }

      const attachmentIndex = new Map<string, GmailAttachmentReference>();
      const googleEvidence = new Map<string, GoogleEvidenceDraft>();
      const gmailSources = gmailMessages.slice(0, 50).map((message): FlorencePrivateGmailSource => {
        const source = draftGmailEvidence({
          householdId: work.household.householdId,
          ownerAdultId: work.adultId,
          connectionId: work.connectionId,
          messageId: message.messageId,
          threadId: message.threadId,
          historyId: message.historyId,
          from: message.from,
          subject: message.subject,
          sentAt: message.sentAt,
        });
        googleEvidence.set(source.id, source);
        for (const attachment of message.attachments) {
          attachmentIndex.set(`${source.id}\0${attachment.attachmentId}`, attachment);
        }
        return {
          sourceId: source.id,
          kind: "gmail",
          visibility: "adult_private",
          sentAt: message.sentAt,
          sender: message.from,
          subject: message.subject,
          text: message.text,
          attachments: message.attachments.map((attachment) => ({
            attachmentId: attachment.attachmentId,
            filename: attachment.filename,
            mimeType: attachment.mimeType,
            sizeBytes: attachment.sizeBytes,
          })),
        };
      });
      const calendarSources = calendarEvents.slice(0, 50).map((event) => {
        const interval = calendarEvidenceInterval(event, work.household.timeZone);
        const source = draftCalendarEvidence({
          householdId: work.household.householdId,
          ownerAdultId: work.adultId,
          connectionId: work.connectionId,
          calendarId: work.calendarId,
          visibility: work.visibility,
          providerEventId: event.providerEventId,
          providerRevision: event.providerRevision,
          providerUpdatedAt: event.providerUpdatedAt,
          status: event.status,
          busy: event.busy,
          title: event.title,
          ...interval,
        });
        googleEvidence.set(source.id, source);
        return privateCalendarEvidence(
          source,
          event,
          work.visibility === "household" ? "shared" : "adult_private",
        );
      });
      const evidence: FlorenceBoundedPrivateGoogleEvidence = {
        gmail: { status: gmailStatus, after: gmailAfter, before: currentTime, sources: gmailSources },
        calendar: {
          status: calendarStatus,
          timeMin: currentTime,
          timeMax: calendarTimeMax,
          events: calendarSources,
        },
      };
      if (gmailSources.length === 0 && calendarSources.length === 0) {
        const completedAt = this.#now().toISOString();
        if (work.kind === "personal_google_poll" && !nextGmailCursor) {
          throw new Error("The personal Google poll did not advance its Gmail cursor");
        }
        await this.#store.completeGooglePoll({
          workId: work.workId,
          gmailCursor: nextGmailCursor ? JSON.stringify(nextGmailCursor) : null,
          calendarCursor: JSON.stringify(nextCalendarCursor),
          googleEvidence: [],
          deliverNotBefore: completedAt,
          deliveries: [],
          facts: [],
          occurredAt: completedAt,
        });
        return;
      }
      const decision = await this.#reasoner.assessGoogleChanges(
        {
          familyProfile: initialFamilyProfile(work.household),
          adult: { adultId: work.adultId, firstName: work.adultFirstName },
          googleConnection: {
            connectionId: work.connectionId,
            status: "active",
            kind: work.visibility === "private" ? "personal" : "family",
          },
          currentTime,
          evidence,
          activeMonitors: [...work.activeMonitors],
          currentPrivateFacts: work.kind === "personal_google_poll" ? [...work.currentPrivateFacts] : [],
        },
        {
          readGmailAttachment: async ({ connectionId, sourceId, attachment }) => {
            if (connectionId !== work.connectionId) {
              throw new Error("The Google change review requested another adult's Gmail attachment");
            }
            const reference = attachmentIndex.get(`${sourceId}\0${attachment.attachmentId}`);
            if (!reference) throw new Error("The Gmail attachment was not in this change set");
            const read = await this.#google?.readGmailAttachment({
              householdId: work.household.householdId,
              ownerAdultId: work.adultId,
              connectionId,
              attachment: reference,
            });
            if (!read) throw new Error("Google is not configured");
            return {
              sourceId,
              attachmentId: read.attachmentId,
              filename: read.filename,
              mimeType: read.mimeType,
              bytes: read.bytes,
            };
          },
        },
      );
      const materialFindings = decision.findings.filter((finding) => finding.materialChange);
      const deliveries = materialFindings
        .filter(
          (finding) =>
            work.visibility === "private" ||
            finding.householdConclusion !== null ||
            finding.monitor !== null ||
            (finding.familyCalendar ?? null) !== null,
        )
        .map(
          (finding): ProactiveDelivery => ({
            privateDetail: work.visibility === "private" ? finding.privateDetail : null,
            householdConclusion: finding.householdConclusion?.summary ?? null,
            householdCategory: finding.householdConclusion?.category ?? null,
            sourceIds: finding.sourceIds,
            urgency: finding.urgency,
            monitor: finding.monitor,
            familyCalendar: finding.familyCalendar ?? null,
          }),
        );
      const decidedAt = this.#now();
      const urgent = deliveries.some((finding) => finding.urgency === "now");
      if (work.kind === "personal_google_poll" && !nextGmailCursor) {
        throw new Error("The personal Google poll did not advance its Gmail cursor");
      }
      await this.#store.completeGooglePoll({
        workId: work.workId,
        gmailCursor: nextGmailCursor ? JSON.stringify(nextGmailCursor) : null,
        calendarCursor: JSON.stringify(nextCalendarCursor),
        googleEvidence: [...googleEvidence.values()],
        deliverNotBefore: proactiveDeliveryAt(decidedAt, work.household.timeZone, urgent).toISOString(),
        deliveries,
        facts: decision.facts,
        occurredAt: decidedAt.toISOString(),
      });
    } catch (error) {
      if (credentialInvalidGrant(error)) return;
      await this.#store.retryProactiveWork({
        workId: work.workId,
        retryAt: later(this.#now(), RETRY_MS),
        error: errorText(error),
      });
    }
  }

  async #executePartnerInvitation(invitation: ApprovedPartnerInvitation): Promise<void> {
    if (!this.#setupOrigin || !this.#linqSenderPhoneNumber) {
      await this.#store.failPartnerInvitationPermanently({
        adultId: invitation.partnerAdultId,
        occurredAt: this.#now().toISOString(),
      });
      return;
    }
    let terminalDelivery:
      | {
          providerConversationId: string;
          identitySubjectDigest: string;
          providerMessageId: string;
          issuedAt: string;
        }
      | undefined;
    try {
      const household = await this.#store.readHousehold({ householdId: invitation.householdId });
      const founder = household?.members.find(
        (member) => member.id === invitation.founderAdultId && member.kind === "adult",
      );
      if (!founder) throw new Error("The partner invitation founder is no longer in the household");
      const founderFirstName = profileString(founder.profile, "firstName") ?? founder.displayName;
      const created = await this.#linq.createChat({
        idempotencyKey: `partner-invite-chat:${invitation.householdId}:${invitation.partnerAdultId}:${invitation.approvalSourceId}`,
        senderPhoneNumber: this.#linqSenderPhoneNumber,
        participantPhoneNumbers: [invitation.partnerPhoneNumber],
        initialText: `Hi ${invitation.partnerFirstName} — I’m Florence. ${founderFirstName} asked me to help the two of you stay ahead of school, schedules, and family loose ends. I’ll send your private setup link next.`,
      });
      const participant = created.authority.participants[0];
      if (
        created.authority.audience !== "private" ||
        created.authority.participants.length !== 1 ||
        !participant ||
        participant.phoneNumber !== invitation.partnerPhoneNumber
      ) {
        throw new LinqError(
          "provider_rejected",
          "Linq created a different private partner conversation",
          false,
        );
      }
      terminalDelivery = {
        providerConversationId: created.providerConversationId,
        identitySubjectDigest: participant.identitySubjectDigest,
        providerMessageId: created.initialMessage.providerMessageId,
        issuedAt: created.initialMessage.occurredAt,
      };
      const setup = this.#enrollmentCodes.issuePartnerSetup({
        providerConversationId: created.providerConversationId,
        identitySubjectDigest: participant.identitySubjectDigest,
        householdId: invitation.householdId,
        adultId: invitation.partnerAdultId,
        occurredAt: invitation.approvedAt,
      });
      const result = await this.#linq.sendMessage({
        idempotencyKey: `partner-invite-link:${invitation.householdId}:${invitation.partnerAdultId}:${invitation.approvalSourceId}`,
        providerConversationId: created.providerConversationId,
        expectedAuthority: created.authority,
        text: `Set up your side here:\n${this.#setupOrigin}/#s=${encodeURIComponent(setup.token)}`,
      });
      const dispatchConfirmed =
        result.status === "committed" &&
        (result.providerState === "sent" ||
          result.providerState === "delivered" ||
          result.providerState === "read");
      if (!dispatchConfirmed || !result.providerReceiptId) {
        const retryable = result.status === "unknown" || result.status === "committed";
        throw new LinqError(
          retryable ? "provider_retryable" : "provider_rejected",
          result.detail ?? "Linq has not confirmed sending the partner setup link",
          retryable,
        );
      }
      await this.#store.issueMessagesEnrollment({
        householdId: invitation.householdId,
        actorAdultId: invitation.founderAdultId,
        adultId: invitation.partnerAdultId,
        challengeDigest: this.#enrollmentCodes.digestPartnerSetup(setup.token),
        providerConversationId: created.providerConversationId,
        identitySubjectDigest: participant.identitySubjectDigest,
        messagesAddress: participant.phoneNumber,
        providerMessageId: result.providerReceiptId,
        expiresAt: setup.expiresAt,
        issuedAt: invitation.approvedAt,
      });
    } catch (error) {
      if (!(error instanceof LinqError)) throw error;
      if (error.retryable) {
        await this.#store.retryPartnerInvitation({
          adultId: invitation.partnerAdultId,
          retryAt: later(this.#now(), RETRY_MS),
          error: errorText(error),
        });
        return;
      }
      await this.#store.failPartnerInvitationPermanently({
        adultId: invitation.partnerAdultId,
        occurredAt: this.#now().toISOString(),
        ...(terminalDelivery ? { delivery: terminalDelivery } : {}),
      });
    }
  }

  async #createFamilyGroup(work: FamilyGroupCreationWork): Promise<void> {
    if (!this.#linqSenderPhoneNumber) return;
    const created = await this.#linq.createChat({
      idempotencyKey: work.createChatIdempotencyKey,
      senderPhoneNumber: this.#linqSenderPhoneNumber,
      participantPhoneNumbers: [...work.participantPhoneNumbers],
      initialText: `Hi ${work.adultFirstNames[0]} and ${work.adultFirstNames[1]} — I’m Florence. This is our family thread. I’ll keep the household picture here, take first passes, and follow up privately when something is only for one of you.`,
    });
    if (
      created.authority.audience !== "group" ||
      !sameStrings(created.authority.participantIdentityDigests, work.participantIdentityDigests)
    ) {
      throw new Error("Linq created a different family group");
    }
    await this.#store.bindCreatedMessagesGroup({
      householdId: work.householdId,
      providerConversationId: created.providerConversationId,
      participantIdentityDigests: created.authority.participantIdentityDigests,
      occurredAt: created.initialMessage.occurredAt,
    });
  }

  async #ensureHouseholdActivation(householdId: string): Promise<void> {
    if (!this.#google || !this.#linqSenderPhoneNumber) return;
    let household = await this.#store.readHousehold({ householdId });
    if (!household) return;
    const adults = household.members
      .filter((member) => member.kind === "adult")
      .sort((left, right) => (left.adultSlot ?? 0) - (right.adultSlot ?? 0));
    const founder = adults.find((adult) => adult.adultSlot === 1);
    const partner = adults.find((adult) => adult.adultSlot === 2);
    if (
      !founder ||
      !partner ||
      founder.status !== "verified" ||
      partner.status !== "verified" ||
      founder.messagesIdentity !== "connected" ||
      partner.messagesIdentity !== "connected" ||
      !founder.messagesAddress ||
      !partner.messagesAddress ||
      !profileString(founder.profile, "onboardingCompletedAt") ||
      !profileString(partner.profile, "onboardingCompletedAt")
    ) {
      return;
    }
    const group = household.channels.find(
      (channel) => channel.audience === "group" && !channel.revokedAt && !channel.stoppedAt,
    );
    if (!group) return;

    const calendarWasAlreadyCreated = Boolean(household.familyCalendarCreatedAt);
    if (!household.familyCalendarCreatedAt) {
      const founderGoogle = household.googleConnections.find(
        (connection) => connection.ownerAdultId === founder.id && connection.status === "active",
      );
      const partnerGoogle = household.googleConnections.find(
        (connection) => connection.ownerAdultId === partner.id && connection.status === "active",
      );
      if (!founderGoogle || !partnerGoogle) return;
      try {
        const result = await this.#google.provisionFamilyCalendar({
          householdId: household.id,
          founderAdultId: founder.id,
          founderConnectionId: founderGoogle.connectionId,
          partnerAdultId: partner.id,
          partnerConnectionId: partnerGoogle.connectionId,
          summary: household.name,
          timeZone: household.timeZone,
          ...(household.familyCalendarId ? { calendarId: household.familyCalendarId } : {}),
        });
        await this.#store.rememberFamilyCalendarId({
          householdId: household.id,
          calendarId: result.calendarId,
          occurredAt: result.occurredAt,
        });
        household = await this.#store.completeFamilyCalendarProvisioning({
          householdId: household.id,
          calendarId: result.calendarId,
          founderConnectionId: result.founderConnectionId,
          partnerConnectionId: result.partnerConnectionId,
          label: result.summary,
          occurredAt: result.occurredAt,
        });
      } catch (error) {
        if (credentialInvalidGrant(error)) return;
        if (
          (error instanceof GoogleFamilyCalendarTransientError ||
            error instanceof GoogleFamilyCalendarProvisioningError) &&
          error.calendarId
        ) {
          await this.#store.rememberFamilyCalendarId({
            householdId: household.id,
            calendarId: error.calendarId,
            occurredAt: this.#now().toISOString(),
          });
        }
        throw error;
      }
    }

    if (
      household.familyCalendarCreatedAt &&
      household.familyCalendarId &&
      household.familyCalendarLabel !== household.name
    ) {
      const credential = await this.#store.readActiveFamilyCalendarCredential({
        householdId: household.id,
      });
      if (!credential || credential.calendarId !== household.familyCalendarId) return;
      try {
        const renamed = await this.#google.renameFamilyCalendar({
          householdId: household.id,
          ownerAdultId: credential.ownerAdultId,
          connectionId: credential.connectionId,
          calendarId: credential.calendarId,
          summary: household.name,
        });
        household = await this.#store.confirmFamilyCalendarLabel({
          householdId: household.id,
          calendarId: credential.calendarId,
          label: renamed.summary,
          occurredAt: renamed.occurredAt,
        });
      } catch (error) {
        if (credentialInvalidGrant(error)) return;
        throw error;
      }
    }

    if (
      calendarWasAlreadyCreated &&
      household.channels.filter((channel) => channel.audience === "group").length > 1
    ) {
      return;
    }
    const calendarLabel = household.familyCalendarLabel ?? household.name;
    const result = await this.#linq.sendMessage({
      idempotencyKey: `family-calendar-ready:${household.id}:${group.id}`,
      providerConversationId: group.providerConversationId,
      expectedAuthority: {
        audience: "group",
        participantIdentityDigests: group.participantIdentityDigests,
      },
      text: `I made the ${calendarLabel} calendar too. Either of you can ask me to add or change family plans here.`,
    });
    if (result.status !== "committed") {
      throw new LinqError(
        result.status === "unknown" ? "provider_retryable" : "provider_rejected",
        result.detail ?? "Linq did not confirm the Family Calendar message",
        result.status === "unknown",
      );
    }
  }

  async #stageFounderHandoff(connection: GoogleConnectionView): Promise<void> {
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
    const partner = household.members.find(
      (member) => member.kind === "adult" && member.adultSlot === 2 && member.status === "planned",
    );
    const partnerPhone = partner ? profileString(partner.profile, "phoneNumber") : null;
    const householdMode = profileString(founder.profile, "householdMode");
    if (!householdMode || (householdMode === "two_adult" && (!partner || !partnerPhone))) return;
    const founderFirstName = profileString(founder.profile, "firstName") ?? founder.displayName;
    const texts =
      partner && partnerPhone
        ? [
            `Your side is ready, ${founderFirstName}.`,
            "I’ll use your Gmail and calendar to catch school dates, conflicts, and loose ends without sharing your private stuff.",
            `Want me to text ${profileString(partner.profile, "firstName") ?? partner.displayName} at ${maskPhoneNumber(partnerPhone)} so they can set up their side?`,
          ]
        : [`Your side is ready, ${founderFirstName}.`];
    await this.#store.stageFounderHandoff({
      householdId: household.id,
      adultId: connection.ownerAdultId,
      channelId: channel.id,
      providerConversationId: channel.providerConversationId,
      texts,
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
    if (!ids[0]) return null;
    const scoped = await this.#store.readHousehold({ householdId: ids[0], viewerAdultId: adultId });
    if (!scoped) return null;
    const system = await this.#store.readHousehold({ householdId: ids[0] });
    if (!system) return null;
    return {
      ...scoped,
      familyCalendarId: system.familyCalendarId,
      familyCalendarOwnerConnectionId: system.familyCalendarOwnerConnectionId,
      familyCalendarPartnerConnectionId: system.familyCalendarPartnerConnectionId,
      familyCalendarLabel: system.familyCalendarLabel,
      familyCalendarCreatedAt: system.familyCalendarCreatedAt,
      googleConnections: system.googleConnections,
    };
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
  const adults = household?.members.filter((member) => member.kind === "adult") ?? [];
  const founder = adults.find((adult) => adult.adultSlot === 1) ?? null;
  const partner = adults.find((adult) => adult.adultSlot === 2) ?? null;
  const activeGoogleAdultIds = new Set(
    household?.googleConnections
      .filter((connection) => connection.status === "active")
      .map((connection) => connection.ownerAdultId) ?? [],
  );
  const activeChannels =
    household?.channels.filter((channel) => !channel.revokedAt && !channel.stoppedAt) ?? [];
  return {
    viewer: {
      adultId,
      displayName: viewer?.displayName ?? null,
      lastName: viewer ? profileString(viewer.profile, "lastName") : null,
    },
    workspace: {
      messagesUrl,
      googleConnections:
        household?.googleConnections.flatMap((connection) =>
          connection.ownerAdultId === adultId && connection.status === "active" && connection.emailLabel
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
        ownOnboardingComplete: Boolean(viewer && profileString(viewer.profile, "onboardingCompletedAt")),
        secondAdultAdded: adults.length === 2,
        partnerInvitation: !partner
          ? "not_ready"
          : partner.messagesIdentity === "connected"
            ? "connected"
            : partner.messagesIdentity === "invited"
              ? "invited"
              : partner.messagesInvitationApproved
                ? "approved"
                : "ready",
        bothAdultsMessagesConnected:
          adults.length === 2 && adults.every((adult) => adult.messagesIdentity === "connected"),
        bothAdultsGoogleConnected:
          adults.length === 2 && adults.every((adult) => activeGoogleAdultIds.has(adult.id)),
        familyGroupConnected: activeChannels.some((channel) => channel.audience === "group"),
        familyCalendarConnected: Boolean(household?.familyCalendarCreatedAt),
        initialBriefing: household?.initialBriefingState ?? "not_ready",
      },
    },
    vault: household
      ? {
          timeZone: household.timeZone,
          postalCode: founder ? profileString(founder.profile, "postalCode") : null,
          members: household.members.map(memberView),
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
          watches: household.watches.map((watch) => {
            return {
              workId: watch.id,
              kind: watch.kind,
              objective: watch.objective,
              currentConclusion: watch.currentConclusion,
              visibility: watch.visibility,
              status: watch.status,
              source: watch.source ? vaultSource(watch.source) : null,
            };
          }),
        }
      : null,
    preferences: preferences(viewer?.preferences),
  };
}

function initialFamilyProfile(household: InitialIntelligenceWork["household"]): FlorenceNarrowFamilyProfile {
  return {
    familyLabel: household.familyLabel,
    timeZone: household.timeZone,
    adultFirstNames: household.adults.map((adult) => adult.firstName),
    children: household.children.map((child) => ({
      firstName: child.firstName,
      school: child.school,
      activities: [...child.activities],
    })),
    postalCode: household.postalCode,
  };
}

function memberView(member: FamilyMemberRecord) {
  const firstName = profileString(member.profile, "firstName");
  const lastName = profileString(member.profile, "lastName");
  const candidate = {
    id: member.id,
    kind: member.kind,
    firstName: firstName ?? member.displayName,
    lastName: member.kind === "adult" ? lastName : (lastName ?? null),
    displayName: member.displayName,
    relationship: profileString(member.profile, "relationship") ?? defaultRelationship(member),
    ...(member.adultSlot === 1 ? { postalCode: profileString(member.profile, "postalCode") } : {}),
    ...(profileString(member.profile, "school") ? { school: profileString(member.profile, "school") } : {}),
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
    firstName: member.firstName,
    ...(member.lastName ? { lastName: member.lastName } : {}),
    relationship: "Child",
    ...(member.school ? { school: member.school } : {}),
    ...(member.activities ? { activities: member.activities } : {}),
  };
}

function memberPatch(member: PatchFamilyMemberInput) {
  const profile: JsonObject = {
    ...(member.school !== undefined ? { school: member.school } : {}),
    ...(member.activities !== undefined ? { activities: member.activities } : {}),
    ...(member.postalCode !== undefined ? { postalCode: member.postalCode } : {}),
  };
  return {
    operation: "patch" as const,
    ...(member.firstName !== undefined ? { firstName: member.firstName } : {}),
    ...(member.lastName !== undefined ? { lastName: member.lastName } : {}),
    ...(Object.keys(profile).length > 0 ? { profile } : {}),
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

function enforcePolicy(decision: FlorenceDecision, announceRestrictions: boolean): FlorenceDecision {
  if (decision.policy.stopMessaging) {
    return {
      policy: decision.policy,
      conversation: { replyToCurrentMessage: false, reaction: null, bubbles: [] },
      facts: [],
      followUp: null,
      interest: null,
      calendar: null,
      householdUpdate: null,
    };
  }
  const retain = decision.policy.retain;
  const schedule = decision.policy.schedule;
  const calendar = schedule ? decision.calendar : null;
  const interest =
    decision.interest?.operation === "stop"
      ? decision.interest
      : retain && schedule
        ? (decision.interest ?? null)
        : null;
  const confirmation = !announceRestrictions
    ? null
    : !retain && !schedule
      ? "I didn’t retain anything in the Vault or schedule anything."
      : !retain
        ? "I didn’t retain anything in the Vault."
        : !schedule
          ? "I didn’t schedule a follow-up or propose a calendar change."
          : null;
  const bubbles = decision.conversation.bubbles.map((bubble) => ({ ...bubble }));
  if (confirmation) {
    const last = bubbles.at(-1);
    if (last) last.text = `${last.text}\n\n${confirmation}`;
    else bubbles.push({ text: confirmation, delayMs: 0 });
  }
  return {
    policy: decision.policy,
    conversation: { ...decision.conversation, bubbles },
    facts: retain ? decision.facts : decision.facts.filter((fact) => fact.operation === "forget"),
    followUp: schedule ? decision.followUp : null,
    interest,
    calendar,
    householdUpdate: decision.householdUpdate,
    ...(decision.researchUrls ? { researchUrls: [...decision.researchUrls] } : {}),
  };
}

function appendResearchSourceBubble(
  bubbles: readonly { text: string; delayMs: number }[],
  researchUrls: readonly string[],
): { text: string; delayMs: number }[] {
  const main = bubbles.map((bubble) => ({ ...bubble }));
  if (researchUrls.length === 0) return main;
  const sourceBubble = {
    text: researchUrls.length === 1 ? (researchUrls[0] ?? "") : `Sources:\n${researchUrls.join("\n")}`,
    delayMs: 0,
  };
  if (main.length < 3) return [...main, sourceBubble];
  const first = main[0] as { text: string; delayMs: number };
  const second = main[1] as { text: string; delayMs: number };
  const third = main[2] as { text: string; delayMs: number };
  return [first, { text: `${second.text}\n\n${third.text}`, delayMs: second.delayMs }, sourceBubble];
}

function decisionCommit(
  turn: InboundTurn,
  decision: FlorenceDecision,
  now: Date,
  options: {
    omitReaction?: boolean;
    approveCalendarOffer?: InboundTurn["pendingCalendarOffers"][number] | null;
    approvePartnerInvitation?: InboundTurn["pendingPartnerInvitation"];
    googleEvidence?: readonly GoogleEvidenceDraft[];
  } = {},
): CommitTurnInput {
  if (decision.policy.stopMessaging) {
    return {
      sourceId: turn.message.sourceId,
      stopChannel: true,
      handledAt: now.toISOString(),
    };
  }
  if (
    turn.message.moveKind === "reaction" &&
    (decision.facts.length > 0 ||
      decision.followUp !== null ||
      decision.interest != null ||
      decision.calendar !== null ||
      decision.householdUpdate !== null ||
      (decision.researchUrls?.length ?? 0) > 0)
  ) {
    throw new FlorenceReasonerError(
      "invalid_output",
      "A reaction can express affect but cannot authorize durable, consequential, or research output",
    );
  }
  const responseTargetSourceId =
    turn.message.moveKind === "reaction" ? turn.message.replyToSourceId : turn.message.sourceId;
  if (!responseTargetSourceId) {
    throw new FlorenceReasonerError("invalid_output", "An inbound reaction has no Florence target");
  }
  const turnId = deterministicUuid(`turn\0${turn.message.sourceId}`);
  const baseBubbles = decision.householdUpdate
    ? []
    : decision.calendar
      ? decision.calendar.mode === "offer"
        ? [
            {
              text: calendarOfferText(decision.calendar.mutation.event, "the family calendar"),
              delayMs: 0,
            },
          ]
        : []
      : decision.conversation.bubbles;
  const bubbles = decision.householdUpdate
    ? []
    : appendResearchSourceBubble(baseBubbles, decision.researchUrls ?? []);
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
      sourceIds:
        change.operation === "correct"
          ? [...new Set([...change.sourceIds, turn.message.sourceId])]
          : change.sourceIds,
    });
  }
  const finiteMonitors: FiniteMonitorDraft[] =
    decision.followUp?.operation === "schedule"
      ? [
          {
            id: deterministicUuid(`follow-up\0${turn.message.sourceId}`),
            objective: decision.followUp.objective,
            currentConclusion: decision.followUp.currentConclusion,
            endCondition: decision.followUp.endCondition,
            nextCheck: decision.followUp.nextCheck,
            why: decision.followUp.why,
            visibility: turn.authority.audience === "group" ? "household" : "private",
            ownerAdultId: turn.authority.audience === "group" ? null : turn.authority.senderAdultId,
            sourceIds: decision.followUp.sourceIds,
          },
        ]
      : [];
  const finiteMonitorUpdates: FiniteMonitorUpdate[] =
    decision.followUp?.operation === "update"
      ? [
          {
            id: decision.followUp.followUpId,
            objective: decision.followUp.objective,
            currentConclusion: decision.followUp.currentConclusion,
            endCondition: decision.followUp.endCondition,
            nextCheck: decision.followUp.nextCheck,
            why: decision.followUp.why,
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
  const approval = options.approveCalendarOffer ? [calendarApproval(options.approveCalendarOffer)] : [];
  const householdUpdate = decision.householdUpdate;
  if (householdUpdate) {
    if (
      turn.authority.audience !== "private" ||
      turn.message.moveKind === "reaction" ||
      !turn.message.authoredText?.trim() ||
      householdUpdate.sourceIds.length !== 1 ||
      householdUpdate.sourceIds[0] !== turn.message.sourceId
    ) {
      throw new FlorenceReasonerError(
        "invalid_output",
        "A household update requires only the current adult's typed private Message",
      );
    }
  }
  return {
    sourceId: turn.message.sourceId,
    googleEvidence: options.googleEvidence ?? [],
    facts,
    deleteFactIds,
    finiteMonitors,
    finiteMonitorUpdates,
    cancelMonitorIds: decision.followUp?.operation === "cancel" ? [decision.followUp.followUpId] : [],
    interestMutation: decision.interest ?? null,
    outbound,
    approveCalendarOffers: approval,
    ...(options.approvePartnerInvitation
      ? { partnerInvitationApproval: { adultId: options.approvePartnerInvitation.adultId } }
      : {}),
    ...(householdUpdate
      ? {
          householdUpdate: {
            basisSourceId: turn.message.sourceId,
            text: householdUpdate.text,
          },
        }
      : {}),
    ...calendar,
    handledAt: now.toISOString(),
  };
}

function calendarOfferText(event: CalendarOfferDraft["mutation"]["event"], calendarLabel: string): string {
  const location = event.location ? `\n${event.location}` : "";
  if (event.intervalKind === "all_day") {
    return `I can add this to ${calendarLabel}:\n\n${event.title}\n${formatAllDayCalendarInterval(event.startDate, event.endDate)}${location}\n\nWant me to add it?`;
  }
  const format = new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: event.timeZone,
  });
  return `I can add this to ${calendarLabel}:\n\n${event.title}\n${format.format(new Date(event.startsAt))} – ${format.format(new Date(event.endsAt))}\n${event.timeZone}${location}\n\nWant me to add it?`;
}

function calendarCommit(
  turn: InboundTurn,
  decision: FlorenceDecision,
): Pick<CommitTurnInput, "calendarOffers" | "calendarActions"> {
  if (!decision.calendar) return {};
  if (turn.authority.audience !== "group") {
    throw new FlorenceReasonerError(
      "invalid_output",
      "Calendar writes can originate only in the exact family group",
    );
  }
  if (!decision.calendar.sourceIds.includes(turn.message.sourceId)) {
    throw new FlorenceReasonerError(
      "invalid_output",
      "A Calendar decision must cite the current adult message",
    );
  }
  if (!turn.message.authoredText?.trim()) {
    throw new FlorenceReasonerError(
      "invalid_output",
      "A Calendar decision requires the current parent's authored instruction",
    );
  }
  if (!turn.household.familyCalendarId) {
    throw new FlorenceReasonerError(
      "invalid_output",
      "The Calendar target is not configured for this conversation",
    );
  }
  const id = deterministicUuid(`calendar\0${turn.message.sourceId}`);
  if (decision.calendar.mode === "offer") {
    const offer: CalendarOfferDraft = {
      id,
      basisSourceId: turn.message.sourceId,
      mutation: decision.calendar.mutation,
    };
    return { calendarOffers: [offer] };
  }
  const action: CalendarActionDraft = {
    id,
    basisSourceId: turn.message.sourceId,
    mutation: decision.calendar.mutation,
  };
  return { calendarActions: [action] };
}

function calendarApproval(offer: InboundTurn["pendingCalendarOffers"][number]): CalendarOfferApproval {
  return { offerId: offer.id };
}

function preferences(value: JsonObject | undefined): PreferencesInput {
  const parsed = preferencesInputSchema.safeParse({
    proactiveGoogleEnabled: value?.proactiveGoogleEnabled !== false,
    automaticFamilyCalendarEnabled: value?.automaticFamilyCalendarEnabled !== false,
    privateConflictBusySharingEnabled: value?.privateConflictBusySharingEnabled === true,
  });
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
  return { id: source.id, kind, label: source.label.slice(0, 300), occurredAt: source.occurredAt };
}

function factStatement(fact: FactRecord): string {
  return factValueStatement(fact.value);
}

function factValueStatement(value: unknown): string {
  if (typeof value === "string") return value;
  if (isRecord(value) && typeof value.statement === "string") return value.statement;
  return JSON.stringify(value);
}

function googleGmailCursor(value: string): GoogleGmailCursor {
  const parsed: unknown = JSON.parse(value);
  if (
    !isRecord(parsed) ||
    parsed.kind !== "gmail_history_v1" ||
    typeof parsed.historyId !== "string" ||
    !parsed.historyId.trim() ||
    typeof parsed.capturedAt !== "string" ||
    !Number.isFinite(Date.parse(parsed.capturedAt))
  ) {
    throw new Error("The stored Gmail monitoring cursor is invalid");
  }
  return {
    kind: "gmail_history_v1",
    historyId: parsed.historyId,
    capturedAt: parsed.capturedAt,
  };
}

function googleCalendarCursor(value: string): GoogleCalendarBoundedCursor {
  const parsed: unknown = JSON.parse(value);
  if (
    !isRecord(parsed) ||
    parsed.kind !== "calendar_updated_min_v1" ||
    typeof parsed.calendarId !== "string" ||
    !parsed.calendarId.trim() ||
    typeof parsed.updatedMin !== "string" ||
    typeof parsed.windowTimeMin !== "string" ||
    typeof parsed.windowTimeMax !== "string" ||
    parsed.overlapMs !== 5 * 60_000 ||
    !Number.isFinite(Date.parse(parsed.updatedMin)) ||
    !Number.isFinite(Date.parse(parsed.windowTimeMin)) ||
    !Number.isFinite(Date.parse(parsed.windowTimeMax))
  ) {
    throw new Error("The stored Calendar monitoring cursor is invalid");
  }
  return parsed as GoogleCalendarBoundedCursor;
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

function approvalReplyTargetsPrompt(
  replyToSourceId: string | null,
  approvalPromptSourceId: string | null,
): boolean {
  return replyToSourceId === null || replyToSourceId === approvalPromptSourceId;
}

function searchSources(sources: readonly FlorenceSource[], query: string): FlorenceSource[] {
  const words = query.toLocaleLowerCase().split(/\s+/).filter(Boolean);
  return sources.filter((source) => {
    const haystack = `${source.label}\n${source.text}`.toLocaleLowerCase();
    return words.every((word) => haystack.includes(word));
  });
}

function calendarConfirmationVerb(operation: "create" | "update" | "delete"): string {
  return operation === "create" ? "Added" : operation === "update" ? "Updated" : "Removed";
}

function calendarOperationNoun(operation: "create" | "update" | "delete"): string {
  return operation === "create" ? "addition" : operation === "update" ? "update" : "removal";
}

function calendarEvidenceInterval(
  event: GoogleCalendarWindowEvent | GoogleCalendarChange,
  householdTimeZone: string,
): Pick<CalendarEvidenceDraft, "startsAt" | "endsAt" | "allDay"> {
  if ("intervalKind" in event) {
    const bounds = calendarWindowBounds(event, householdTimeZone);
    return {
      startsAt: bounds.startsAt,
      endsAt: bounds.endsAt,
      allDay: event.intervalKind === "all_day",
    };
  }
  return { startsAt: event.startsAt, endsAt: event.endsAt, allDay: event.allDay };
}

function privateCalendarEvidence(
  source: CalendarEvidenceDraft,
  event: GoogleCalendarWindowEvent | GoogleCalendarChange,
  visibility: FlorencePrivateCalendarEvent["visibility"],
): FlorencePrivateCalendarEvent {
  if ("intervalKind" in event) {
    return event.intervalKind === "all_day"
      ? {
          sourceId: source.id,
          kind: "calendar",
          visibility,
          status: source.status,
          busy: source.busy,
          title: source.title,
          startsAt: source.startsAt,
          endsAt: source.endsAt,
          allDay: source.allDay,
          intervalKind: event.intervalKind,
          timeZone: null,
          startDate: event.startDate,
          endDate: event.endDate,
        }
      : {
          sourceId: source.id,
          kind: "calendar",
          visibility,
          status: source.status,
          busy: source.busy,
          title: source.title,
          startsAt: source.startsAt,
          endsAt: source.endsAt,
          allDay: source.allDay,
          intervalKind: event.intervalKind,
          timeZone: event.timeZone,
          startDate: null,
          endDate: null,
        };
  }
  if (event.allDay === null) {
    return {
      sourceId: source.id,
      kind: "calendar",
      visibility,
      status: source.status,
      busy: source.busy,
      title: source.title,
      startsAt: source.startsAt,
      endsAt: source.endsAt,
      allDay: source.allDay,
      intervalKind: null,
      timeZone: null,
      startDate: null,
      endDate: null,
    };
  }
  if (event.allDay) {
    if (!event.startDate || !event.endDate) {
      throw new Error("Google returned all-day Calendar evidence without exact dates");
    }
    return {
      sourceId: source.id,
      kind: "calendar",
      visibility,
      status: source.status,
      busy: source.busy,
      title: source.title,
      startsAt: source.startsAt,
      endsAt: source.endsAt,
      allDay: source.allDay,
      intervalKind: "all_day",
      timeZone: null,
      startDate: event.startDate,
      endDate: event.endDate,
    };
  }
  if (!event.timeZone) throw new Error("Google returned timed Calendar evidence without a time zone");
  return {
    sourceId: source.id,
    kind: "calendar",
    visibility,
    status: source.status,
    busy: source.busy,
    title: source.title,
    startsAt: source.startsAt,
    endsAt: source.endsAt,
    allDay: source.allDay,
    intervalKind: "timed",
    timeZone: event.timeZone,
    startDate: null,
    endDate: null,
  };
}

function calendarWindowBounds(
  event: GoogleCalendarWindowEvent,
  householdTimeZone: string,
): { startsAt: string; endsAt: string } {
  if (event.intervalKind === "timed") {
    return { startsAt: event.startsAt, endsAt: event.endsAt };
  }
  return {
    startsAt: zonedCalendarDateStart(event.startDate, householdTimeZone).toISOString(),
    endsAt: zonedCalendarDateStart(event.endDate, householdTimeZone).toISOString(),
  };
}

function familyCalendarMonthWindows(
  month: string,
  timeZone: string,
): readonly { timeMin: string; timeMax: string }[] {
  const [yearText, monthText] = month.split("-");
  const year = Number(yearText);
  const monthNumber = Number(monthText);
  if (!Number.isInteger(year) || !Number.isInteger(monthNumber)) {
    throw new Error("Family Calendar month is invalid");
  }
  const nextYear = monthNumber === 12 ? year + 1 : year;
  const nextMonth = monthNumber === 12 ? 1 : monthNumber + 1;
  const start = zonedInstant(
    { year, month: monthNumber, day: 1, hour: 0, minute: 0, second: 0 },
    timeZone,
  ).toISOString();
  const middle = zonedInstant(
    { year, month: monthNumber, day: 16, hour: 0, minute: 0, second: 0 },
    timeZone,
  ).toISOString();
  const end = zonedInstant(
    { year: nextYear, month: nextMonth, day: 1, hour: 0, minute: 0, second: 0 },
    timeZone,
  ).toISOString();
  return [
    { timeMin: start, timeMax: middle },
    { timeMin: middle, timeMax: end },
  ];
}

function deduplicateFamilyCalendarEvents(
  events: readonly GoogleCalendarWindowEvent[],
): GoogleCalendarWindowEvent[] {
  const byProviderEventId = new Map<string, GoogleCalendarWindowEvent>();
  for (const event of events) {
    const current = byProviderEventId.get(event.providerEventId);
    if (
      !current ||
      event.providerUpdatedAt > current.providerUpdatedAt ||
      (event.providerUpdatedAt === current.providerUpdatedAt &&
        event.providerRevision > current.providerRevision)
    ) {
      byProviderEventId.set(event.providerEventId, event);
    }
  }
  return [...byProviderEventId.values()];
}

function compareFamilyCalendarEvents(
  left: GoogleCalendarWindowEvent,
  right: GoogleCalendarWindowEvent,
  householdTimeZone: string,
): number {
  const leftBounds = calendarWindowBounds(left, householdTimeZone);
  const rightBounds = calendarWindowBounds(right, householdTimeZone);
  const startDifference = Date.parse(leftBounds.startsAt) - Date.parse(rightBounds.startsAt);
  if (startDifference !== 0) return startDifference;
  if (left.intervalKind !== right.intervalKind) return left.intervalKind === "all_day" ? -1 : 1;
  const endDifference = Date.parse(leftBounds.endsAt) - Date.parse(rightBounds.endsAt);
  if (endDifference !== 0) return endDifference;
  return (left.title ?? "").localeCompare(right.title ?? "", "en-US");
}

function zonedCalendarDateStart(value: string, timeZone: string): Date {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  const parsed = new Date(`${value}T00:00:00.000Z`);
  if (!match || !Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== value) {
    throw new Error("Google returned an invalid all-day Calendar date");
  }
  return zonedInstant(
    {
      year: Number(match[1]),
      month: Number(match[2]),
      day: Number(match[3]),
      hour: 0,
      minute: 0,
      second: 0,
    },
    timeZone,
  );
}

function formatAllDayCalendarInterval(startDate: string, exclusiveEndDate: string): string {
  const start = new Date(`${startDate}T00:00:00.000Z`);
  const inclusiveEnd = new Date(`${exclusiveEndDate}T00:00:00.000Z`);
  inclusiveEnd.setUTCDate(inclusiveEnd.getUTCDate() - 1);
  const format = new Intl.DateTimeFormat("en-US", { dateStyle: "medium", timeZone: "UTC" });
  return start.getTime() === inclusiveEnd.getTime()
    ? format.format(start)
    : `${format.format(start)} – ${format.format(inclusiveEnd)}`;
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

function proactiveDeliveryAt(now: Date, timeZone: string, urgent: boolean): Date {
  if (urgent) return now;
  const local = zonedParts(now, timeZone);
  if (local.hour >= QUIET_END_HOUR && local.hour < QUIET_START_HOUR) return now;
  const targetDate = new Date(
    Date.UTC(local.year, local.month - 1, local.day + (local.hour >= QUIET_START_HOUR ? 1 : 0)),
  );
  return zonedInstant(
    {
      year: targetDate.getUTCFullYear(),
      month: targetDate.getUTCMonth() + 1,
      day: targetDate.getUTCDate(),
      hour: QUIET_END_HOUR,
      minute: 0,
      second: 0,
    },
    timeZone,
  );
}

function zonedInstant(
  target: {
    year: number;
    month: number;
    day: number;
    hour: number;
    minute: number;
    second: number;
  },
  timeZone: string,
): Date {
  const targetAsUtc = Date.UTC(
    target.year,
    target.month - 1,
    target.day,
    target.hour,
    target.minute,
    target.second,
  );
  let guess = targetAsUtc;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const represented = zonedParts(new Date(guess), timeZone);
    const representedAsUtc = Date.UTC(
      represented.year,
      represented.month - 1,
      represented.day,
      represented.hour,
      represented.minute,
      represented.second,
    );
    const correction = targetAsUtc - representedAsUtc;
    guess += correction;
    if (correction === 0) break;
  }
  return new Date(guess);
}

function zonedParts(
  date: Date,
  timeZone: string,
): {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
} {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    calendar: "gregory",
    numberingSystem: "latn",
    hourCycle: "h23",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).formatToParts(date);
  const value = (type: Intl.DateTimeFormatPartTypes): number => {
    const part = parts.find((candidate) => candidate.type === type)?.value;
    const parsed = Number(part);
    if (!Number.isInteger(parsed)) throw new Error(`Could not resolve ${type} in ${timeZone}`);
    return parsed;
  };
  return {
    year: value("year"),
    month: value("month"),
    day: value("day"),
    hour: value("hour"),
    minute: value("minute"),
    second: value("second"),
  };
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
  if (member.adultSlot === 2) return "Partner";
  return member.role === "steward" ? "Parent" : "Caregiver";
}

function familyProfileForReasoning(member: FamilyMemberRecord): JsonObject {
  const firstName = profileString(member.profile, "firstName");
  const lastName = profileString(member.profile, "lastName");
  const relationship = profileString(member.profile, "relationship") ?? defaultRelationship(member);
  if (member.kind === "adult") {
    return {
      relationship,
      ...(firstName ? { firstName } : {}),
      ...(lastName ? { lastName } : {}),
      ...(member.adultSlot === 1 && profileString(member.profile, "postalCode")
        ? { postalCode: profileString(member.profile, "postalCode") as string }
        : {}),
    };
  }
  const school = profileString(member.profile, "school");
  const activities = profileStrings(member.profile, "activities");
  return {
    relationship,
    ...(firstName ? { firstName } : {}),
    ...(lastName ? { lastName } : {}),
    ...(school ? { school } : {}),
    ...(activities ? { activities } : {}),
  };
}

function profileString(profile: JsonObject, key: string): string | null {
  const value = profile[key];
  return typeof value === "string" && value.trim() ? value : null;
}

function profileStrings(profile: JsonObject, key: string): string[] | null {
  const value = profile[key];
  return Array.isArray(value) && value.every((item) => typeof item === "string") ? value : null;
}

function proactiveGoogleAccessEnabled(member: FamilyMemberRecord): boolean {
  const acceptedAt = member.preferences.proactiveUseAcceptedAt;
  return (
    typeof acceptedAt === "string" &&
    acceptedAt.trim().length > 0 &&
    Number.isFinite(Date.parse(acceptedAt)) &&
    member.preferences.proactiveGoogleEnabled !== false
  );
}

function nullableText(value: string | null): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

function maskPhoneNumber(value: string): string {
  const digits = value.replaceAll(/\D/g, "");
  return digits.length >= 4 ? `••••${digits.slice(-4)}` : "their saved number";
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

function sameStrings(left: readonly string[], right: readonly string[]): boolean {
  const expected = [...left].sort();
  const actual = [...right].sort();
  return expected.length === actual.length && expected.every((value, index) => value === actual[index]);
}

function isLaterInbound(
  incomingOccurredAt: string,
  incomingSourceId: string,
  activeOccurredAt: string,
  activeSourceId: string,
): boolean {
  const incomingTime = Date.parse(incomingOccurredAt);
  const activeTime = Date.parse(activeOccurredAt);
  if (!Number.isFinite(incomingTime) || !Number.isFinite(activeTime)) {
    throw new Error("Inbound chronological comparison requires valid timestamps");
  }
  return incomingTime > activeTime || (incomingTime === activeTime && incomingSourceId > activeSourceId);
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

function credentialInvalidGrant(error: unknown): boolean {
  return error instanceof GoogleConnectionError && error.code === "credential_invalid_grant";
}
