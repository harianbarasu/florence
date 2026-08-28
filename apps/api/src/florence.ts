import { createHash } from "node:crypto";
import type { EncryptedImageVault } from "@florence/artifacts";
import {
  type CompleteFamilyOnboardingInput,
  calendarMonthSchema,
  completeFamilyOnboardingInputSchema,
  type DeleteGoogleDerivedDataResponse,
  type DisconnectGoogleConnectionResponse,
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
  ActiveFamilyCalendarCredential,
  ApprovedCalendarAction,
  ApprovedPartnerInvitation,
  CalendarActionDraft,
  CalendarEventTarget,
  CalendarEvidenceDraft,
  CalendarOfferApproval,
  CalendarOfferDraft,
  CommitTurnInput,
  CompleteFounderOnboardingInput,
  DueProactiveWork,
  FactDraft,
  FactRecord,
  FamilyCalendarReviewProposal,
  FamilyGroupCreationWork,
  FamilyMemberRecord,
  FiniteMonitorDraft,
  FiniteMonitorUpdate,
  GoogleEvidenceDraft,
  HouseholdRecord,
  InboundPreparationContext,
  InboundTurn,
  InitialGoogleScanFact,
  InitialGoogleScanFinding,
  InitialIntelligenceWork,
  InitialPrivateGoogleScanV1,
  JsonObject,
  LinqAuthority,
  MessagesEnrollmentResult,
  PostgresFlorenceStore,
  PreparedInboundContent,
  ProactiveDelivery,
  ReviewedGoogleSourceDisposition,
  SharedBriefingCandidate,
  SourceRecord,
} from "@florence/database";
import {
  calendarEvidenceSourceId,
  draftCalendarEvidence,
  draftGmailEvidence,
  FlorenceStoreConflict,
  gmailEvidenceSourceId,
  initialPrivateGoogleScanDigest,
} from "@florence/database";
import {
  type BeginGoogleConnectionResult,
  type GmailAttachmentReference,
  type GmailEvidence,
  type GoogleCalendarBaselineTarget,
  type GoogleCalendarBoundedCursor,
  type GoogleCalendarChange,
  type GoogleCalendarNoEventCoverageTarget,
  GoogleCalendarTransientError,
  type GoogleCalendarWindowEvent,
  type GoogleConnection,
  GoogleConnectionError,
  type GoogleConnectionView,
  GoogleFamilyCalendarProvisioningError,
  GoogleFamilyCalendarTransientError,
  type GoogleGmailCursor,
  type GooglePersonalCalendarCatalogRead,
  type GooglePersonalCalendarCatalogTarget,
  type GooglePersonalCalendarWindowRead,
  type GoogleWorkspaceOperation,
} from "@florence/google";
import {
  type LinqClient,
  LinqError,
  type LinqMessageStatusProposal,
  type LinqReaction,
  type LinqReactionProposal,
} from "@florence/linq";
import type { EnrollmentCodes, WebAccessPath } from "./enrollment.js";
import type { FlorenceFlightsClient } from "./flights.js";
import type { FlorenceMapsClient } from "./maps.js";
import type { FlorencePublicPageClient } from "./public-page.js";
import {
  type FlorenceBoundedPrivateGoogleEvidence,
  type FlorenceCalendarCatalogRead,
  type FlorenceCalendarWindowRead,
  type FlorenceConversationalGmailSource,
  type FlorenceDecision,
  type FlorenceNarrowFamilyProfile,
  type FlorencePrivateCalendarEvent,
  type FlorencePrivateGmailSource,
  type FlorencePrivateGoogleBatchDecision,
  type FlorenceReadTools,
  type FlorenceReasoner,
  FlorenceReasonerError,
  type FlorenceReasonerInput,
  type FlorenceSource,
  type FlorenceVoiceNoteInput,
} from "./reasoner.js";
import type { FlorenceWeatherClient } from "./weather.js";

const DEFAULT_PREFERENCES: PreferencesInput = {
  proactiveGoogleEnabled: true,
  automaticFamilyCalendarEnabled: true,
  privateConflictBusySharingEnabled: false,
};
const GOOGLE_WORKSPACE_ACTION_SCOPES = [
  "https://www.googleapis.com/auth/gmail.modify",
  "https://www.googleapis.com/auth/drive",
  "https://www.googleapis.com/auth/tasks",
  "https://www.googleapis.com/auth/contacts",
] as const;
const LOOP_IDLE_MS = 250;
const RETRY_MS = 15_000;
const WORK_CUE_MS = 6_000;
type OutboundDeliveryDisposition = "sent_now" | "already_sent" | "claimed" | "failed";
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
  readonly #maps: FlorenceMapsClient | null;
  readonly #publicPages: FlorencePublicPageClient | null;
  readonly #weather: FlorenceWeatherClient | null;
  readonly #flights: FlorenceFlightsClient | null;
  readonly #reasoner: FlorenceReasoner | null;
  readonly #enrollmentCodes: EnrollmentCodes;
  readonly #imageVault: EncryptedImageVault | null;
  readonly #messagesUrl: string | null;
  readonly #linqSenderPhoneNumber: string | null;
  readonly #setupOrigin: string | null;
  readonly #now: () => Date;
  #activeRun: Promise<boolean> | null = null;
  #activeInbound: ActiveInbound | null = null;
  #activeFamilyWork = new Map<string, { controller: AbortController; promise: Promise<void> }>();
  #pendingInboundAccepts = new Set<Promise<unknown>>();
  #nextArtifactPurgeAt = 0;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #started = false;

  constructor(input: {
    store: PostgresFlorenceStore;
    linq: LinqClient;
    google: GoogleConnection | null;
    maps?: FlorenceMapsClient | null;
    publicPages?: FlorencePublicPageClient | null;
    weather?: FlorenceWeatherClient | null;
    flights?: FlorenceFlightsClient | null;
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
    this.#maps = input.maps ?? null;
    this.#publicPages = input.publicPages ?? null;
    this.#weather = input.weather ?? null;
    this.#flights = input.flights ?? null;
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
    const credentials = (
      await this.#store.readActiveFamilyCalendarCredentials({
        householdId: household.id,
      })
    ).filter((credential) => credential.calendarId === household.familyCalendarId);
    if (credentials.length === 0) {
      return familyCalendarMonthViewSchema.parse({
        status: "temporarily_unavailable",
        month,
        timeZone,
        calendarName,
      });
    }

    for (const credential of credentials) {
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
        if (reads.some((read) => read.status === "unavailable")) continue;
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
        if (!(error instanceof GoogleCalendarTransientError) && !credentialInvalidGrant(error)) {
          throw error;
        }
      }
    }
    return familyCalendarMonthViewSchema.parse({
      status: "temporarily_unavailable",
      month,
      timeZone,
      calendarName,
    });
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
        ...(child.age !== undefined ? { age: child.age } : {}),
        ...(child.grade !== undefined ? { grade: child.grade } : {}),
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
      const firstName = profileString(adult.profile, "firstName") ?? adult.displayName;
      const completionSourceId = await this.#store.completePartnerOnboarding({
        householdId: household.id,
        adultId: adult.id,
        completionText: `Your side is all set, ${firstName}. I’m finishing the shared family setup now, and I’ll let you both know in the family thread when it’s ready.`,
        occurredAt: this.#now().toISOString(),
      });
      if (completionSourceId) await this.#deliverOutbound(completionSourceId);
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

  async disconnectGoogle(adultId: string, connectionId: string): Promise<DisconnectGoogleConnectionResponse> {
    const household = await this.#householdForAdult(adultId);
    const result = await this.#requiredGoogle().disconnect({
      connectionId,
      householdId: household.id,
      ownerAdultId: adultId,
      notifyReconnect: false,
      now: this.#now().toISOString(),
    });
    this.#wake();
    return {
      workspace: await this.workspaceForAdult(adultId),
      localAccess: "disconnected",
      providerRevocation: result.providerRevocation,
    };
  }

  async deleteGoogleDerivedData(adultId: string): Promise<DeleteGoogleDerivedDataResponse> {
    const household = await this.#householdForAdult(adultId);
    const google = this.#google;
    const revocations: ("confirmed" | "unconfirmed" | "not-needed")[] = [];
    let disconnectedConnections = 0;
    if (google) {
      const activeConnections = await google.status({
        householdId: household.id,
        ownerAdultId: adultId,
      });
      for (const connection of activeConnections) {
        try {
          const disconnected = await google.disconnect({
            connectionId: connection.connectionId,
            householdId: household.id,
            ownerAdultId: adultId,
            notifyReconnect: false,
            now: this.#now().toISOString(),
          });
          disconnectedConnections += 1;
          revocations.push(disconnected.providerRevocation);
        } catch (error) {
          if (!(error instanceof GoogleConnectionError) || error.code !== "not_found") throw error;
        }
      }
    }
    const purged = await this.#store.deleteGoogleDerivedData({
      householdId: household.id,
      adultId,
      now: this.#now().toISOString(),
    });
    disconnectedConnections += purged.additionalActiveConnectionsDisconnected;
    if (purged.additionalActiveConnectionsDisconnected > 0) revocations.push("unconfirmed");
    const providerRevocation = revocations.includes("unconfirmed")
      ? "unconfirmed"
      : revocations.includes("confirmed")
        ? "confirmed"
        : "not-needed";
    this.#wake();
    return {
      workspace: await this.workspaceForAdult(adultId),
      providerRevocation,
      deletion: {
        disconnectedConnections,
        googleSources: purged.googleSources,
        facts: purged.facts,
        watches: purged.watches,
        calendarActions: purged.calendarActions,
        unsentMessages: purged.unsentMessages,
      },
    };
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
    messagesAddress: string;
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
    let idempotencyPrefix:
      | "founder-setup"
      | "partner-setup-expired"
      | "partner-setup-link"
      | "partner-setup-reply";
    let pendingPartnerEnrollment: {
      adultId: string;
      householdId: string;
      founderAdultId: string;
      messagesAddress: string;
      initialProviderMessageId: string;
      challengeDigest: string;
      expiresAt: string;
      linkBubbleIndex: number;
    } | null = null;

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
            text: invitation.linkIssued
              ? "That Florence setup link has expired. Ask your partner to send a fresh invitation."
              : "I couldn’t confirm delivery of your Florence setup link before it expired. Ask your partner to send a fresh invitation.",
            delayMs: 0,
          },
        ];
        idempotencyPrefix = "partner-setup-expired";
      } else {
        if (
          invitation.state === "issued" &&
          Date.parse(input.occurredAt) <= Date.parse(invitation.handshakeAt)
        ) {
          return true;
        }
        if (!this.#reasoner) {
          throw new LinqError(
            "provider_retryable",
            "Florence setup interpretation is temporarily unavailable",
            true,
          );
        }
        let conversation: Awaited<ReturnType<FlorenceReasoner["converseDuringSetup"]>>;
        try {
          const willIssueSetupLink = invitation.state === "awaiting_reply";
          conversation = await this.#reasoner.converseDuringSetup({
            stage: "partner_invited",
            parentName: null,
            currentMessage: { text: input.text, occurredAt: input.occurredAt },
            recentMessages: [],
            nextStep: willIssueSetupLink ? "signed_link_will_follow" : "use_existing_partner_setup_link",
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
        if (invitation.state === "awaiting_reply") {
          if (!this.#setupOrigin || !this.#google) {
            throw new LinqError(
              "provider_retryable",
              "Florence partner setup is temporarily unavailable",
              true,
            );
          }
          const setup = this.#enrollmentCodes.issuePartnerSetup({
            providerConversationId: invitation.providerConversationId,
            identitySubjectDigest: invitation.identitySubjectDigest,
            householdId: invitation.householdId,
            adultId: invitation.adultId,
            occurredAt: invitation.setupIssuedAt ?? input.occurredAt,
          });
          const setupUrl = `${this.#setupOrigin}/#s=${encodeURIComponent(setup.token)}`;
          bubbles = [
            { text: "Thanks—here’s your private setup link.", delayMs: 0 },
            { text: setupUrl, delayMs: 0 },
          ];
          pendingPartnerEnrollment = {
            adultId: invitation.adultId,
            householdId: invitation.householdId,
            founderAdultId: invitation.founderAdultId,
            messagesAddress: invitation.messagesAddress,
            initialProviderMessageId: invitation.initialProviderMessageId,
            challengeDigest: this.#enrollmentCodes.digestPartnerSetup(setup.token),
            expiresAt: setup.expiresAt,
            linkBubbleIndex: bubbles.length - 1,
          };
          idempotencyPrefix = "partner-setup-link";
        } else {
          bubbles = conversation.bubbles;
          idempotencyPrefix = "partner-setup-reply";
        }
      }
    } else {
      const reservation = await this.#store.classifyUnboundMessagesReservation({
        messagesAddress: input.messagesAddress,
        providerConversationId: input.providerConversationId,
        identitySubjectDigest: input.identitySubjectDigest,
      });
      if (input.carrierOptOut) {
        if (reservation === "pending_partner") {
          const declined = await this.#store.declinePendingPartnerReservation({
            messagesAddress: input.messagesAddress,
            providerConversationId: input.providerConversationId,
            identitySubjectDigest: input.identitySubjectDigest,
            occurredAt: input.occurredAt,
          });
          if (!declined) {
            const rebound = await this.#store.readUnboundPartnerInvitation({
              providerConversationId: input.providerConversationId,
              identitySubjectDigest: input.identitySubjectDigest,
              now: checkedAt,
            });
            if (rebound && rebound.state !== "declined") {
              await this.#store.declinePartnerInvitation({
                adultId: rebound.adultId,
                providerConversationId: input.providerConversationId,
                identitySubjectDigest: input.identitySubjectDigest,
                occurredAt: input.occurredAt,
              });
            }
          }
        }
        return true;
      }
      if (reservation === "pending_partner") {
        throw new LinqError(
          "provider_retryable",
          "The invited partner conversation is still being bound",
          true,
        );
      }
      if (reservation === "claimed") return true;
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
        requestsFreshLink: false,
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
      bubbles = [
        ...(conversation.requestsFreshLink
          ? [{ text: "Of course—here’s your private setup link.", delayMs: 0 }]
          : conversation.bubbles),
        { text: setupUrl, delayMs: 0 },
      ];
      idempotencyPrefix = "founder-setup";
    }

    if (pendingPartnerEnrollment) {
      await this.#store.issueMessagesEnrollment({
        householdId: pendingPartnerEnrollment.householdId,
        actorAdultId: pendingPartnerEnrollment.founderAdultId,
        adultId: pendingPartnerEnrollment.adultId,
        challengeDigest: pendingPartnerEnrollment.challengeDigest,
        providerConversationId: input.providerConversationId,
        identitySubjectDigest: input.identitySubjectDigest,
        messagesAddress: pendingPartnerEnrollment.messagesAddress,
        providerMessageId: pendingPartnerEnrollment.initialProviderMessageId,
        expiresAt: pendingPartnerEnrollment.expiresAt,
        issuedAt: input.occurredAt,
      });
    }

    await this.#setTyping({
      providerConversationId: input.providerConversationId,
      expectedAuthority,
      active: true,
    });
    try {
      for (const [index, bubble] of bubbles.entries()) {
        if (index > 0) await pause(Math.max(650, bubble.delayMs));
        const idempotencyBasis = pendingPartnerEnrollment
          ? `${pendingPartnerEnrollment.initialProviderMessageId}\0${
              index === pendingPartnerEnrollment.linkBubbleIndex ? "link" : "ack"
            }`
          : `${input.providerEventId}\0${index}`;
        const baseIdempotencyKey = `${idempotencyPrefix}:${deterministicUuid(idempotencyBasis)}`;
        const idempotencyKey = pendingPartnerEnrollment
          ? await this.#store.scopeHouseholdLinqIdempotencyKey({
              householdId: pendingPartnerEnrollment.householdId,
              idempotencyKey: baseIdempotencyKey,
            })
          : baseIdempotencyKey;
        const result = await this.#linq.sendMessage({
          idempotencyKey,
          providerConversationId: input.providerConversationId,
          expectedAuthority,
          text: bubble.text,
        });
        if (result.status !== "committed") {
          throw new LinqError(
            result.status === "unknown" ? "provider_retryable" : "provider_rejected",
            result.detail,
            result.status === "unknown",
          );
        }
        if (pendingPartnerEnrollment && index === pendingPartnerEnrollment.linkBubbleIndex) {
          if (
            result.providerState !== "sent" &&
            result.providerState !== "delivered" &&
            result.providerState !== "read"
          ) {
            throw new LinqError(
              "provider_retryable",
              "Linq has not confirmed sending the partner setup link",
              true,
            );
          }
          await this.#store.confirmMessagesEnrollmentDelivery({
            householdId: pendingPartnerEnrollment.householdId,
            adultId: pendingPartnerEnrollment.adultId,
            challengeDigest: pendingPartnerEnrollment.challengeDigest,
            providerConversationId: input.providerConversationId,
            identitySubjectDigest: input.identitySubjectDigest,
            messagesAddress: pendingPartnerEnrollment.messagesAddress,
            providerMessageId: result.providerReceiptId,
            deliveredAt: result.occurredAt,
          });
        }
      }
    } catch (error) {
      if (pendingPartnerEnrollment && error instanceof LinqError && !error.retryable) {
        await this.#store.failPartnerInvitationPermanently({
          adultId: pendingPartnerEnrollment.adultId,
          occurredAt: this.#now().toISOString(),
        });
        return true;
      }
      throw error;
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

  async redeemAccessLink(
    accessToken: string,
  ): Promise<{ adultId: string; accessPath: WebAccessPath } | null> {
    const checkedAt = this.#now();
    const access = this.#enrollmentCodes.verifyWebAccess(accessToken, checkedAt);
    if (!access) return null;
    const observed = await this.#linq.observeChat(access.providerConversationId);
    if (
      observed.audience !== "private" ||
      observed.participantIdentityDigests.length !== 1 ||
      observed.participantIdentityDigests[0] !== access.identitySubjectDigest ||
      observed.participants.length !== 1 ||
      observed.participants[0]?.identitySubjectDigest !== access.identitySubjectDigest
    ) {
      return null;
    }
    const authority = await this.#store.resolveLinqAuthority({
      providerConversationId: access.providerConversationId,
      audience: "private",
      participantIdentityDigests: [access.identitySubjectDigest],
      senderIdentitySubjectDigest: access.identitySubjectDigest,
      occurredAt: checkedAt.toISOString(),
    });
    if (
      !authority ||
      authority.stopped ||
      authority.householdId !== access.householdId ||
      authority.senderAdultId !== access.adultId ||
      authority.adultIds.length !== 1 ||
      authority.adultIds[0] !== access.adultId ||
      authority.expectedParticipantIdentityDigests.length !== 1 ||
      authority.expectedParticipantIdentityDigests[0] !== access.identitySubjectDigest
    ) {
      return null;
    }
    const household = await this.#householdForAdultOrNull(access.adultId);
    const adult = household?.members.find((member) => member.id === access.adultId);
    if (
      household?.id !== access.householdId ||
      adult?.kind !== "adult" ||
      adult.status !== "verified" ||
      !this.#enrollmentCodes.verifyWebAccess(accessToken, this.#now())
    ) {
      return null;
    }
    return { adultId: access.adultId, accessPath: access.accessPath };
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
    this.#activeInbound?.controller.abort(new Error("Florence is shutting down"));
    for (const active of this.#activeFamilyWork.values()) {
      active.controller.abort(new Error("Florence is shutting down"));
    }
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
      if (proactiveWork.kind === "family_task") this.#launchFamilyWork(proactiveWork);
      else await this.#executeProactiveWork(proactiveWork);
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
      await this.#store.commitTurn({
        sourceId: turn.message.sourceId,
        handledAt: this.#now().toISOString(),
      });
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
          bubbles = setup.requestsFreshLink
            ? [{ text: "Of course—here’s a fresh link to finish setup.", delayMs: 0 }]
            : [...setup.bubbles];
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
      const accessUrl = this.#issueWebAccessUrl(turn, "/");
      if (accessUrl) bubbles.push({ text: accessUrl, delayMs: 0 });
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
            reminder: null,
            familyWork: null,
            docketCompletions: null,
            calendar: null,
            householdUpdate: null,
          },
          this.#now(),
        ),
      );
      return;
    }
    await this.#respondEnrolled(turn);
  }

  async #respondEnrolled(turn: InboundTurn): Promise<void> {
    if (!this.#reasoner) {
      await this.#retryInbound(turn.message.sourceId, "Florence reasoning is not configured");
      return;
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

    try {
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
          const interpretation = await this.#reasoner.interpretPartnerInvitationApproval(
            {
              currentMessage: { text: typedApprovalText },
              partner: {
                adultId: turn.pendingPartnerInvitation.adultId,
                firstName: turn.pendingPartnerInvitation.firstName,
                maskedPhoneNumber: turn.pendingPartnerInvitation.maskedPhoneNumber,
              },
            },
            controller.signal,
          );
          if (interpretation.sendInvitation) approvedPartnerInvitation = turn.pendingPartnerInvitation;
        } catch (error) {
          if (error instanceof FlorenceReasonerError && error.retryable) {
            await this.#retryInbound(turn.message.sourceId, errorText(error));
            return;
          }
          if (controller.signal.aborted) return;
          if (!(error instanceof FlorenceReasonerError)) throw error;
        }
      }
      if (
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
          const interpretation = await this.#reasoner.interpretCalendarApproval(
            {
              currentMessage: { text: typedApprovalText, occurredAt: turn.message.occurredAt },
              event: offer.event,
            },
            controller.signal,
          );
          if (interpretation.approve) approvedCalendarOffer = offer;
        } catch (error) {
          if (error instanceof FlorenceReasonerError && error.retryable) {
            await this.#retryInbound(turn.message.sourceId, errorText(error));
            return;
          }
          if (controller.signal.aborted) return;
          if (!(error instanceof FlorenceReasonerError)) throw error;
        }
      }
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
      let reactionCue: Promise<void> | null = null;
      let substantiveWorkStarted = false;
      let immediateReactionStaged = false;
      const startWork = () => {
        if (substantiveWorkStarted) return;
        substantiveWorkStarted = true;
        workTimer = setTimeout(() => {
          if (controller.signal.aborted) return;
          workCue = this.#tryTurnCue(turn.message.sourceId, "work").then(() => undefined);
        }, WORK_CUE_MS);
        reactionCue = this.#tryTurnCue(turn.message.sourceId, "reaction").then((staged) => {
          immediateReactionStaged = staged;
        });
      };
      try {
        if (attachmentJob) startWork();
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
        const decision = await this.#reasoner.decide(context.input, context.reads, controller.signal, {
          onWorkStarted: startWork,
        });
        if (workTimer) clearTimeout(workTimer);
        workTimer = null;
        if (workCue) await workCue;
        if (reactionCue) await reactionCue;
        controller.signal.throwIfAborted();
        const requested = this.#appendRequestedWebAccess(
          turn,
          enforcePolicy(decision, turn.message.moveKind !== "reaction"),
        );
        const approval = approvedCalendarOffer;
        const partnerApproval = requested.policy.stopMessaging ? null : approvedPartnerInvitation;
        const committedDecision =
          approval || partnerApproval
            ? {
                ...requested,
                policy: { ...requested.policy, schedule: true, stopMessaging: false },
                conversation: {
                  replyToCurrentMessage: true,
                  reaction: null,
                  bubbles: [
                    {
                      text:
                        approval && partnerApproval
                          ? `Got it—I’ll add that calendar item and text ${partnerApproval.firstName} now.`
                          : approval
                            ? "Got it—I’ll add that calendar item now."
                            : `Got it—I’ll text ${partnerApproval?.firstName ?? "your partner"} now.`,
                      delayMs: 0,
                    },
                  ],
                },
                calendar: null,
                householdUpdate: null,
                webAccessPath: null,
              }
            : requested;
        const committed = await this.#store.commitTurn(
          decisionCommit(turn, committedDecision, this.#now(), {
            omitReaction: immediateReactionStaged,
            approveCalendarOffer: approval,
            approvePartnerInvitation: partnerApproval,
            googleEvidence: context.googleEvidence(),
            googleConnectionIdsUsed: context.googleConnectionIdsUsed(),
            resolveCalendarEventTarget: context.resolveCalendarEventTarget,
          }),
        );
        if (committed === "committed" && committedDecision.familyWork?.operation === "cancel") {
          this.#activeFamilyWork
            .get(committedDecision.familyWork.workId)
            ?.controller.abort(new Error("The family task was cancelled"));
        }
      } catch (error) {
        if (controller.signal.aborted) return;
        if (error instanceof FlorenceReasonerError && !error.retryable) {
          if (workTimer) clearTimeout(workTimer);
          workTimer = null;
          if (workCue) await workCue;
          if (reactionCue) await reactionCue;
          if (approvedCalendarOffer || approvedPartnerInvitation) {
            const actionText =
              approvedCalendarOffer && approvedPartnerInvitation
                ? `Got it—I’ll add that calendar item and text ${approvedPartnerInvitation.firstName} now.`
                : approvedCalendarOffer
                  ? "Got it—I’ll add that calendar item now."
                  : `Got it—I’ll text ${approvedPartnerInvitation?.firstName ?? "your partner"} now.`;
            await this.#store.commitTurn(
              decisionCommit(
                turn,
                {
                  policy: { retain: false, schedule: true, stopMessaging: false },
                  conversation: {
                    replyToCurrentMessage: true,
                    reaction: null,
                    bubbles: [{ text: actionText, delayMs: 0 }],
                  },
                  facts: [],
                  followUp: null,
                  reminder: null,
                  familyWork: null,
                  docketCompletions: null,
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
              substantiveWorkStarted
                ? decisionCommit(
                    turn,
                    {
                      policy: { retain: false, schedule: false, stopMessaging: false },
                      conversation: {
                        replyToCurrentMessage: true,
                        reaction: null,
                        bubbles: [
                          {
                            text: "I couldn’t finish that just now. I didn’t change anything. Want me to try again?",
                            delayMs: 0,
                          },
                        ],
                      },
                      facts: [],
                      followUp: null,
                      reminder: null,
                      familyWork: null,
                      docketCompletions: null,
                      calendar: null,
                      householdUpdate: null,
                    },
                    this.#now(),
                    { omitReaction: immediateReactionStaged },
                  )
                : turn.message.moveKind === "reaction"
                  ? { sourceId: turn.message.sourceId, handledAt: this.#now().toISOString() }
                  : decisionCommit(
                      turn,
                      {
                        policy: { retain: false, schedule: false, stopMessaging: false },
                        conversation: {
                          replyToCurrentMessage: true,
                          reaction: null,
                          bubbles: [
                            {
                              text: "Sorry—I hit a snag before I could answer. Try me again and I’ll take another run at it.",
                              delayMs: 0,
                            },
                          ],
                        },
                        facts: [],
                        followUp: null,
                        reminder: null,
                        familyWork: null,
                        docketCompletions: null,
                        calendar: null,
                        householdUpdate: null,
                      },
                      this.#now(),
                      { omitReaction: immediateReactionStaged },
                    ),
            );
          }
          return;
        }
        if (workTimer) clearTimeout(workTimer);
        workTimer = null;
        if (workCue) await workCue;
        if (reactionCue) await reactionCue;
        await this.#retryInbound(turn.message.sourceId, errorText(error));
      } finally {
        if (workTimer) clearTimeout(workTimer);
        if (workCue) await workCue;
        if (reactionCue) await reactionCue;
        if (typing) {
          await this.#setTyping({
            providerConversationId: turn.authority.providerConversationId,
            expectedAuthority,
            active: false,
          });
        }
      }
    } finally {
      if (this.#activeInbound === active) this.#activeInbound = null;
    }
  }

  async #reasonerContext(turn: InboundTurn): Promise<{
    input: FlorenceReasonerInput;
    reads: FlorenceReadTools;
    googleEvidence: () => readonly GoogleEvidenceDraft[];
    googleConnectionIdsUsed: () => readonly string[];
    resolveCalendarEventTarget: (eventRef: string) => CalendarEventTarget | null;
  }> {
    const members = new Map(turn.household.members.map((member) => [member.id, member.displayName]));
    const visibleSources = memorySources(turn.facts);
    const sourceIndex = new Map(visibleSources.map((source) => [source.sourceId, source]));
    const googleEvidence = new Map<string, GoogleEvidenceDraft>();
    const pendingGoogleEvidence = new Map<string, GoogleEvidenceDraft>();
    const gmailAttachmentIndex = new Map<string, GmailAttachmentReference>();
    const googleConnectionIdsUsed = new Set<string>();
    const calendarTargets = new Map<string, GooglePersonalCalendarCatalogTarget>();
    const calendarEventTargets = new Map<string, CalendarEventTarget>();
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

    const familyCalendarCredentials =
      turn.authority.audience === "group"
        ? await this.#store.readActiveFamilyCalendarCredentials({
            householdId: turn.authority.householdId,
          })
        : [];
    const familyCalendarCredential = familyCalendarCredentials[0] ?? null;
    const googleConnections = !this.#google
      ? []
      : turn.authority.audience === "private"
        ? await this.#google.status({
            householdId: turn.authority.householdId,
            ownerAdultId: turn.authority.senderAdultId,
          })
        : (
            await Promise.all(
              familyCalendarCredentials.map(
                async (credential) =>
                  (
                    await this.#google?.status({
                      householdId: turn.authority.householdId,
                      ownerAdultId: credential.ownerAdultId,
                    })
                  )?.filter((connection) => connection.connectionId === credential.connectionId) ?? [],
              ),
            )
          ).flat();
    const activeGoogleConnection = googleConnections.find(
      (connection) => connection.status === "active" && connection.emailLabel,
    );
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
        authoredText: turn.message.authoredText ? redactWebAccessToken(turn.message.authoredText) : null,
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
      householdDocket: {
        totalItems: turn.householdDocket.totalItems,
        items: turn.householdDocket.items.map((item) => ({ ...item })),
      },
      visibleReminders: turn.visibleReminders.map((reminder) => ({
        reminderId: reminder.reminderId,
        action: reminder.action,
        schedule:
          reminder.schedule.kind === "weekly"
            ? { ...reminder.schedule, weekdays: [...reminder.schedule.weekdays] }
            : reminder.schedule,
        status: reminder.status,
        nextAt: reminder.nextAt,
        lastRunAt: reminder.lastRunAt,
        createdAt: reminder.createdAt,
      })),
      visibleFamilyWork: turn.visibleFamilyWork.map((work) => ({
        workId: work.workId,
        objective: work.objective,
        currentProgress: work.currentProgress,
        status: work.status,
        createdAt: work.createdAt,
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
      googleConnections: activeGoogleConnection
        ? [
            {
              emailLabel:
                turn.authority.audience === "group"
                  ? (turn.household.familyCalendarLabel ?? turn.household.name)
                  : requiredText(activeGoogleConnection.emailLabel, "Google account label"),
              calendarAvailable:
                turn.authority.audience === "private" || turn.household.familyCalendarId !== null,
              kind: turn.authority.audience === "group" ? ("family" as const) : ("personal" as const),
            },
          ]
        : [],
    };

    const googleOwnerAdultId =
      turn.authority.audience === "group"
        ? (familyCalendarCredential?.ownerAdultId ?? null)
        : turn.authority.senderAdultId;
    const orderedFamilyCalendarCredentials = [...familyCalendarCredentials].sort((left, right) => {
      const leftActive = left.connectionId === activeGoogleConnection?.connectionId ? 0 : 1;
      const rightActive = right.connectionId === activeGoogleConnection?.connectionId ? 0 : 1;
      return leftActive - rightActive;
    });
    const calendarRefFor = (calendarId: string): string =>
      `calendar_${sha256(
        `${turn.message.sourceId}\0${activeGoogleConnection?.connectionId ?? "unavailable"}\0${calendarId}`,
      ).slice(0, 32)}`;
    const calendarEventRefFor = (event: GooglePersonalCalendarWindowRead["events"][number]): string =>
      `event_${sha256(
        `${turn.message.sourceId}\0${activeGoogleConnection?.connectionId ?? "unavailable"}\0${event.calendarId}\0${event.providerEventId}\0${event.providerRevision}`,
      ).slice(0, 32)}`;
    const readExactFamilyCatalog = async (
      calendarId: string,
    ): Promise<
      Readonly<{
        read: GooglePersonalCalendarCatalogRead;
        credential: ActiveFamilyCalendarCredential;
      }>
    > => {
      const google = this.#google;
      const firstCredential = orderedFamilyCalendarCredentials[0];
      if (!google || !firstCredential) throw new Error("The Family Calendar credential is unavailable");
      let lastAvailable: GooglePersonalCalendarCatalogRead | null = null;
      let lastUnavailable: GooglePersonalCalendarCatalogRead | null = null;
      let lastCredential = firstCredential;
      for (const credential of orderedFamilyCalendarCredentials) {
        try {
          const read = await google.readExactCalendarCatalog({
            householdId: turn.authority.householdId,
            ownerAdultId: credential.ownerAdultId,
            connectionId: credential.connectionId,
            calendarId,
          });
          if (read.status !== "unavailable") {
            if (read.calendars[0]?.eventCoverage === "readable") return { read, credential };
            lastAvailable = read;
            lastCredential = credential;
            continue;
          }
          lastUnavailable = read;
          lastCredential = credential;
        } catch (error) {
          if (error instanceof GoogleConnectionError && error.code === "credential_invalid_grant") {
            continue;
          }
          throw error;
        }
      }
      if (lastAvailable) return { read: lastAvailable, credential: lastCredential };
      if (lastUnavailable) return { read: lastUnavailable, credential: lastCredential };
      return {
        read: await google.readExactCalendarCatalog({
          householdId: turn.authority.householdId,
          ownerAdultId: firstCredential.ownerAdultId,
          connectionId: firstCredential.connectionId,
          calendarId,
        }),
        credential: firstCredential,
      };
    };
    const readExactFamilyWindow = async (input: {
      calendarId: string;
      timeMin: string;
      timeMax: string;
      limit: number;
    }): Promise<
      Readonly<{
        read: GooglePersonalCalendarWindowRead;
        credential: ActiveFamilyCalendarCredential;
      }>
    > => {
      const google = this.#google;
      const firstCredential = orderedFamilyCalendarCredentials[0];
      if (!google || !firstCredential) throw new Error("The Family Calendar credential is unavailable");
      let lastIncomplete: GooglePersonalCalendarWindowRead | null = null;
      let lastCredential = firstCredential;
      for (const credential of orderedFamilyCalendarCredentials) {
        try {
          const catalog = await google.readExactCalendarCatalog({
            householdId: turn.authority.householdId,
            ownerAdultId: credential.ownerAdultId,
            connectionId: credential.connectionId,
            calendarId: input.calendarId,
          });
          if (catalog.status === "unavailable") {
            lastCredential = credential;
            continue;
          }
          const read = await google.readExactCalendarWindow({
            householdId: turn.authority.householdId,
            ownerAdultId: credential.ownerAdultId,
            connectionId: credential.connectionId,
            calendarId: input.calendarId,
            timeMin: input.timeMin,
            timeMax: input.timeMax,
            limit: input.limit,
          });
          const exactTarget = read.calendars.find((calendar) => calendar.calendarId === input.calendarId);
          if (exactTarget?.status === "complete" && exactTarget.accessRole !== "freeBusyReader") {
            return { read, credential };
          }
          lastIncomplete = read;
          lastCredential = credential;
        } catch (error) {
          if (error instanceof GoogleConnectionError && error.code === "credential_invalid_grant") {
            continue;
          }
          throw error;
        }
      }
      if (lastIncomplete) return { read: lastIncomplete, credential: lastCredential };
      return {
        read: await google.readExactCalendarWindow({
          householdId: turn.authority.householdId,
          ownerAdultId: firstCredential.ownerAdultId,
          connectionId: firstCredential.connectionId,
          calendarId: input.calendarId,
          timeMin: input.timeMin,
          timeMax: input.timeMax,
          limit: input.limit,
        }),
        credential: firstCredential,
      };
    };
    const readConversationCalendarCatalog = async (): Promise<
      Readonly<{
        read: GooglePersonalCalendarCatalogRead;
        credential: Readonly<{ connectionId: string; ownerAdultId: string }>;
      }>
    > => {
      const google = this.#google;
      if (!google || !activeGoogleConnection || !googleOwnerAdultId) {
        throw new Error("Google Calendar is unavailable");
      }
      if (turn.authority.audience === "group") {
        if (!turn.household.familyCalendarId) {
          throw new Error("The Family Calendar is unavailable");
        }
        return readExactFamilyCatalog(turn.household.familyCalendarId);
      }
      return {
        read: await google.readPersonalCalendarCatalog({
          householdId: turn.authority.householdId,
          ownerAdultId: googleOwnerAdultId,
          connectionId: activeGoogleConnection.connectionId,
          excludedFamilyCalendarId: turn.household.familyCalendarId,
        }),
        credential: {
          connectionId: activeGoogleConnection.connectionId,
          ownerAdultId: googleOwnerAdultId,
        },
      };
    };
    const enumerateConversationCalendars = async () =>
      (await readConversationCalendarCatalog()).read.calendars;
    const maps = this.#maps;
    const publicPages = this.#publicPages;
    const weather = this.#weather;
    const flights = this.#flights;

    const reads: FlorenceReadTools = {
      ...(publicPages
        ? {
            runPublicPage: (request, signal) => publicPages.run(request, signal),
          }
        : {}),
      ...(maps
        ? {
            runMaps: (request, signal) => maps.run(request, signal),
          }
        : {}),
      ...(weather
        ? {
            runWeather: (request, signal) => weather.run(request, signal),
          }
        : {}),
      ...(flights
        ? {
            runFlights: (request, signal) => flights.search(request, signal),
          }
        : {}),
      ...(this.#google &&
      activeGoogleConnection &&
      turn.authority.audience === "private" &&
      GOOGLE_WORKSPACE_ACTION_SCOPES.every((scope) => activeGoogleConnection.grantedScopes.includes(scope))
        ? {
            runGoogleWorkspace: async (operation: GoogleWorkspaceOperation, signal?: AbortSignal) => {
              const result = await this.#google?.runWorkspace(
                {
                  householdId: turn.authority.householdId,
                  ownerAdultId: turn.authority.senderAdultId,
                  connectionId: activeGoogleConnection.connectionId,
                  operation,
                },
                signal,
              );
              if (!result) throw new Error("Google Workspace is unavailable");
              googleConnectionIdsUsed.add(activeGoogleConnection.connectionId);
              return result;
            },
          }
        : {}),
      settleSources: (sources) => {
        for (const source of sources) {
          sourceIndex.set(source.sourceId, source);
          const evidence = pendingGoogleEvidence.get(source.sourceId);
          if (evidence) {
            pendingGoogleEvidence.delete(source.sourceId);
            googleEvidence.set(source.sourceId, evidence);
            googleConnectionIdsUsed.add(evidence.connectionId);
          }
        }
      },
      searchFamilyMemory: async ({ query, limit }) => searchSources(visibleSources, query).slice(0, limit),
      listCalendars: async (): Promise<FlorenceCalendarCatalogRead> => {
        if (!this.#google || !activeGoogleConnection || !googleOwnerAdultId) {
          return {
            status: "unavailable",
            calendars: [],
            totalCalendarCount: 0,
          };
        }
        try {
          const catalogRead = await readConversationCalendarCatalog();
          const { read, credential } = catalogRead;
          if (read.status !== "unavailable") {
            googleConnectionIdsUsed.add(credential.connectionId);
          }
          const calendars = read.calendars.slice(0, 100).map((target) => {
            const calendarRef = calendarRefFor(target.calendarId);
            calendarTargets.set(calendarRef, target);
            return {
              calendarRef,
              label:
                turn.authority.audience === "group"
                  ? (turn.household.familyCalendarLabel ?? turn.household.name ?? "Family Calendar")
                  : (target.label ?? (target.primary ? "Primary calendar" : "Calendar")),
              timeZone: turn.authority.audience === "group" ? turn.household.timeZone : target.timeZone,
              primary: turn.authority.audience === "group" ? null : target.primary,
              accessRole: turn.authority.audience === "group" ? null : target.accessRole,
              eventCoverage: target.eventCoverage,
            };
          });
          return {
            status:
              read.status === "complete" && read.totalCalendarCount > calendars.length
                ? "truncated"
                : read.status,
            calendars,
            totalCalendarCount: read.totalCalendarCount,
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
      readCalendarWindow: async ({ timeMin, timeMax, limit, scope, calendarRefs }) => {
        if (!this.#google || !activeGoogleConnection || !googleOwnerAdultId) {
          return {
            status: "unavailable",
            calendars: [],
            totalCalendarCount: 0,
            events: [],
            totalEventCount: 0,
          };
        }
        try {
          let calendarIds: readonly string[] | undefined;
          if (turn.authority.audience === "group") {
            if (!turn.household.familyCalendarId) {
              return {
                status: "unavailable",
                calendars: [],
                totalCalendarCount: 0,
                events: [],
                totalEventCount: 0,
              };
            }
            calendarIds = [turn.household.familyCalendarId];
          } else if (scope === "all") {
            calendarIds = undefined;
          } else if (scope === "primary") {
            const targets = await enumerateConversationCalendars();
            const primary = targets.find((target) => target.primary);
            if (!primary) {
              return {
                status: "unavailable",
                calendars: [],
                totalCalendarCount: 0,
                events: [],
                totalEventCount: 0,
              };
            }
            calendarIds = [primary.calendarId];
          } else {
            calendarIds = calendarRefs.map((calendarRef) => {
              const target = calendarTargets.get(calendarRef);
              if (!target) throw new Error("A selected Calendar reference is no longer available");
              return target.calendarId;
            });
          }
          const calendarRead =
            turn.authority.audience === "group"
              ? await readExactFamilyWindow({
                  calendarId: calendarIds?.[0] ?? "",
                  timeMin,
                  timeMax,
                  limit,
                })
              : {
                  read: await this.#google.readPersonalCalendarWindow({
                    householdId: turn.authority.householdId,
                    ownerAdultId: googleOwnerAdultId,
                    connectionId: activeGoogleConnection.connectionId,
                    excludedFamilyCalendarId: turn.household.familyCalendarId,
                    timeMin,
                    timeMax,
                    ...(calendarIds === undefined ? {} : { calendarIds }),
                    limit,
                  }),
                  credential: {
                    connectionId: activeGoogleConnection.connectionId,
                    ownerAdultId: googleOwnerAdultId,
                  },
                };
          const { read, credential } = calendarRead;
          if (read.status !== "unavailable") {
            googleConnectionIdsUsed.add(credential.connectionId);
          }
          const labels = new Map(
            read.calendars.map(
              (calendar) =>
                [
                  calendar.calendarId,
                  turn.authority.audience === "group"
                    ? (turn.household.familyCalendarLabel ?? turn.household.name)
                    : calendar.label,
                ] as const,
            ),
          );
          const projectedCalendars = read.calendars.slice(0, 100);
          const modelStatus =
            read.status === "complete" && read.totalCalendarCount > projectedCalendars.length
              ? "truncated"
              : read.status;
          return {
            status: modelStatus,
            calendars: projectedCalendars.map((calendar) => ({
              calendarRef: calendarRefFor(calendar.calendarId),
              label: labels.get(calendar.calendarId) ?? null,
              timeZone: turn.authority.audience === "group" ? turn.household.timeZone : calendar.timeZone,
              primary:
                turn.authority.audience === "group" || calendar.status === "missing"
                  ? null
                  : calendar.primary,
              accessRole: turn.authority.audience === "group" ? null : calendar.accessRole,
              status: calendar.status,
              eventCount: calendar.eventCount,
            })),
            totalCalendarCount: read.totalCalendarCount,
            totalEventCount: read.totalEventCount,
            events: read.events.map((event) => {
              const eventRef = calendarEventRefFor(event);
              if (event.title) {
                calendarEventTargets.set(eventRef, {
                  providerEventId: event.providerEventId,
                  providerRevision: event.providerRevision,
                  observedEvent:
                    event.intervalKind === "all_day"
                      ? {
                          intervalKind: "all_day",
                          title: event.title,
                          startDate: event.startDate,
                          endDate: event.endDate,
                          location: event.location,
                        }
                      : {
                          intervalKind: "timed",
                          title: event.title,
                          startsAt: event.startsAt,
                          endsAt: event.endsAt,
                          timeZone: event.timeZone,
                          location: event.location,
                        },
                });
              }
              return event.intervalKind === "all_day"
                ? {
                    intervalKind: event.intervalKind,
                    calendarRef: calendarRefFor(event.calendarId),
                    calendarLabel: labels.get(event.calendarId) ?? null,
                    eventRef,
                    title: event.title,
                    startDate: event.startDate,
                    endDate: event.endDate,
                    providerUpdatedAt: event.providerUpdatedAt,
                    status: event.status,
                    busy: event.busy,
                    location: event.location,
                  }
                : {
                    intervalKind: event.intervalKind,
                    calendarRef: calendarRefFor(event.calendarId),
                    calendarLabel: labels.get(event.calendarId) ?? null,
                    eventRef,
                    title: event.title,
                    startsAt: event.startsAt,
                    endsAt: event.endsAt,
                    providerUpdatedAt: event.providerUpdatedAt,
                    status: event.status,
                    busy: event.busy,
                    timeZone: event.timeZone,
                    location: event.location,
                  };
            }),
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
      readGmailAttachment: async ({ sourceId, attachment }) => {
        if (!this.#google || !activeGoogleConnection || turn.authority.audience !== "private") {
          throw new Error("Gmail attachment reading is unavailable");
        }
        if (!googleEvidence.has(sourceId) && !pendingGoogleEvidence.has(sourceId)) {
          throw new Error("The Gmail source is not authorized in this turn");
        }
        const providerReference = gmailAttachmentIndex.get(`${sourceId}\0${attachment.attachmentRef}`);
        if (
          !providerReference ||
          providerReference.filename !== attachment.filename ||
          providerReference.mimeType !== attachment.mimeType ||
          providerReference.sizeBytes !== attachment.sizeBytes
        ) {
          throw new Error("The Gmail attachment reference changed before it could be read");
        }
        const read = await this.#google.readGmailAttachment({
          householdId: turn.authority.householdId,
          ownerAdultId: turn.authority.senderAdultId,
          connectionId: activeGoogleConnection.connectionId,
          attachment: providerReference,
        });
        return {
          sourceId,
          attachmentRef: attachment.attachmentRef,
          filename: read.filename,
          mimeType: read.mimeType,
          bytes: read.bytes,
        };
      },
      searchGmail: async ({ query, after, before, limit }) => {
        if (!this.#google || !activeGoogleConnection || turn.authority.audience !== "private") {
          return { status: "complete", sources: [] };
        }
        const evidence = await this.#google.searchGmail({
          householdId: turn.authority.householdId,
          ownerAdultId: turn.authority.senderAdultId,
          connectionId: activeGoogleConnection.connectionId,
          query,
          ...(after === null ? {} : { after }),
          ...(before === null ? {} : { before }),
          limit,
        });
        const sources = evidence.messages.map((message): FlorenceConversationalGmailSource => {
          const draft = draftGmailEvidence({
            householdId: turn.authority.householdId,
            ownerAdultId: turn.authority.senderAdultId,
            connectionId: activeGoogleConnection.connectionId,
            ...message,
          });
          pendingGoogleEvidence.set(draft.id, draft);
          for (const attachment of message.attachments) {
            gmailAttachmentIndex.set(
              `${draft.id}\0${gmailAttachmentRefFor(draft.id, attachment)}`,
              attachment,
            );
          }
          return {
            sourceId: draft.id,
            kind: "gmail",
            visibility: "adult_private",
            sentAt: message.sentAt,
            sender: modelSafeGmailText(message.from),
            subject: message.subject === null ? null : modelSafeGmailText(message.subject),
            text: modelSafeGmailText(message.text),
            textStatus: message.textStatus,
            attachments: message.attachments.map((attachment) => ({
              attachmentRef: gmailAttachmentRefFor(draft.id, attachment),
              filename: attachment.filename,
              mimeType: attachment.mimeType,
              sizeBytes: attachment.sizeBytes,
            })),
            attachmentsStatus: message.attachmentsStatus,
          };
        });
        return { status: evidence.status, sources };
      },
    };
    return {
      input,
      reads,
      googleEvidence: () => [...googleEvidence.values()],
      googleConnectionIdsUsed: () => [...googleConnectionIdsUsed],
      resolveCalendarEventTarget: (eventRef) => calendarEventTargets.get(eventRef) ?? null,
    };
  }

  async #deliverOutbound(sourceId: string, retryTransient = true): Promise<OutboundDeliveryDisposition> {
    const outbound = await this.#store.beginOutbound({ sourceId, now: this.#now().toISOString() });
    if (!outbound) {
      const status = await this.#store.outboundDeliveryStatus(sourceId);
      if (status === "sent") return "already_sent";
      if (status === "pending" || status === "sending") return "claimed";
      return "failed";
    }
    try {
      const observed = await this.#linq.observeChat(outbound.providerConversationId);
      const groupObservation = await this.reconcileObservedFamilyGroup({
        providerConversationId: outbound.providerConversationId,
        audience: observed.audience,
        participantIdentityDigests: observed.participantIdentityDigests,
        occurredAt: this.#now().toISOString(),
      });
      if (groupObservation === "mismatch" || groupObservation === "retired") return "failed";
      if (!(await this.#store.outboundSendIsCurrent(sourceId))) {
        await this.#store.failSendingOutbound(
          sourceId,
          "The progress cue was no longer current before provider delivery",
        );
        return "failed";
      }
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
        return "sent_now";
      } else {
        await this.#store.retryOutbound({ sourceId, retryAt: null, error: result.detail });
        return "failed";
      }
    } catch (error) {
      await this.#store.retryOutbound({
        sourceId,
        retryAt:
          retryTransient && error instanceof LinqError && error.retryable ? later(this.#now(), 5_000) : null,
        error: errorText(error),
      });
      return retryTransient && error instanceof LinqError && error.retryable ? "claimed" : "failed";
    }
  }

  async #tryTurnCue(sourceId: string, cue: "reaction" | "work" | "retry"): Promise<boolean> {
    try {
      const cueSourceId = await this.#store.stageTurnCue({
        sourceId,
        cue,
        occurredAt: this.#now().toISOString(),
      });
      if (!cueSourceId) return false;
      const disposition = await this.#deliverOutbound(cueSourceId, false);
      return disposition === "sent_now" || disposition === "already_sent";
    } catch {
      // A progress cue is optional. The substantive answer remains the product outcome.
      return false;
    }
  }

  async #retryInbound(sourceId: string, error: string): Promise<void> {
    await this.#tryTurnCue(sourceId, "retry");
    try {
      await this.#store.retryInbound({
        sourceId,
        retryAt: later(this.#now(), RETRY_MS),
        error,
      });
    } catch (retryError) {
      // A newer message may supersede this turn while the optional cue is being delivered.
      if (retryError instanceof FlorenceStoreConflict) return;
      throw retryError;
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
        confirmationText: action.personalCalendarOwnerApproved
          ? personalCalendarApprovalConfirmation(action.mutation)
          : `${calendarConfirmationVerb(action.mutation.operation)} “${title}” ${action.mutation.operation === "delete" ? "from" : "on"} the family calendar.`,
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
        if (familyCalendar.status !== "complete" || !familyCalendar.cursor) {
          throw new Error("The Family Calendar baseline is temporarily unavailable");
        }
        const selectedCandidates = work.candidates.slice(0, 3);
        const decision = {
          selectedCandidateIds: selectedCandidates.map((candidate) => candidate.candidateId),
          bubbles: householdInitialBriefingBubbles(
            selectedCandidates,
            work.candidates.length - selectedCandidates.length,
          ),
        };
        await this.#store.completeHouseholdInitialBriefing({
          workId: work.workId,
          selectedCandidateIds: decision.selectedCandidateIds,
          familyCalendarCursor: JSON.stringify(familyCalendar.cursor),
          bubbles: decision.bubbles,
          occurredAt: this.#now().toISOString(),
        });
        return;
      }

      await this.#executeInitialPrivateGoogleScan(work);
    } catch (error) {
      if (credentialInvalidGrant(error)) return;
      await this.#store.retryInitialIntelligence({
        workId: work.workId,
        retryAt: later(this.#now(), RETRY_MS),
        error: errorText(error),
      });
    }
  }

  async #executeInitialPrivateGoogleScan(
    work: Extract<InitialIntelligenceWork, { kind: "initial_private_review" }>,
  ): Promise<void> {
    const google = this.#google;
    const reasoner = this.#reasoner;
    if (!google || !reasoner) throw new Error("Florence's proactive Google review is not configured");
    if (!work.scan) {
      const anchoredAt = this.#now().toISOString();
      const capturedCursor = await google.captureGmailCursor({
        householdId: work.household.householdId,
        ownerAdultId: work.adultId,
        connectionId: work.connectionId,
      });
      const scan = newInitialPrivateGoogleScan(work, anchoredAt, capturedCursor);
      await this.#store.beginInitialPrivateGoogleScan({ workId: work.workId, scan });
      return;
    }

    const scan = work.scan;
    if (scan.connectionId !== work.connectionId || scan.excludedFamilyCalendarId !== work.familyCalendarId) {
      await this.#restartInitialPrivateGoogleScan(work, scan);
      return;
    }
    if (scan.phase === "calendar_targets" || scan.phase === "calendar_verify") {
      const page = await google.readCalendarBaselineTargetsPage({
        householdId: work.household.householdId,
        ownerAdultId: work.adultId,
        connectionId: work.connectionId,
        excludedFamilyCalendarId: scan.excludedFamilyCalendarId,
        ...(scan.calendar.targetPageToken ? { pageToken: scan.calendar.targetPageToken } : {}),
      });
      const seenTargetIds = new Set(scan.calendar.targets.map((target) => target.calendarId));
      const seenThisPass = new Set(
        scan.phase === "calendar_verify"
          ? scan.calendar.verificationTargetIds
          : [
              ...scan.calendar.targets.map((target) => target.calendarId),
              ...scan.calendar.noEventCoverageTargets.map((target) => target.calendarId),
            ],
      );
      const targets = [...scan.calendar.targets];
      const noEventCoverageTargets = [...scan.calendar.noEventCoverageTargets];
      for (const target of page.targets) {
        if (seenThisPass.has(target.calendarId)) {
          throw new Error("Google repeated a Calendar target during one enumeration pass");
        }
        seenThisPass.add(target.calendarId);
        if (noEventCoverageTargets.some((candidate) => candidate.calendarId === target.calendarId)) {
          await this.#restartInitialPrivateGoogleScan(work, scan);
          return;
        }
        const existing = targets.find((candidate) => candidate.calendarId === target.calendarId);
        if (existing) {
          if (
            existing.timeZone !== target.timeZone ||
            existing.accessRole !== target.accessRole ||
            existing.primary !== target.primary
          ) {
            await this.#restartInitialPrivateGoogleScan(work, scan);
            return;
          }
          continue;
        }
        seenTargetIds.add(target.calendarId);
        targets.push(initialGoogleScanTarget(target, scan));
      }
      for (const target of page.noEventCoverageTargets ?? []) {
        if (seenThisPass.has(target.calendarId)) {
          throw new Error("Google repeated a no-event Calendar target during one enumeration pass");
        }
        seenThisPass.add(target.calendarId);
        if (targets.some((candidate) => candidate.calendarId === target.calendarId)) {
          await this.#restartInitialPrivateGoogleScan(work, scan);
          return;
        }
        const existing = noEventCoverageTargets.find(
          (candidate) => candidate.calendarId === target.calendarId,
        );
        if (existing) {
          if (
            existing.timeZone !== target.timeZone ||
            existing.accessRole !== target.accessRole ||
            existing.primary !== target.primary
          ) {
            await this.#restartInitialPrivateGoogleScan(work, scan);
            return;
          }
          continue;
        }
        noEventCoverageTargets.push({
          calendarId: target.calendarId,
          timeZone: target.timeZone,
          accessRole: target.accessRole,
          primary: target.primary,
        });
      }
      const verificationTargetIds =
        scan.phase === "calendar_verify"
          ? exactDistinct([
              ...scan.calendar.verificationTargetIds,
              ...page.targets.map((target) => target.calendarId),
              ...(page.noEventCoverageTargets ?? []).map((target) => target.calendarId),
            ])
          : scan.calendar.verificationTargetIds;
      const tokenState = nextOpaquePageTokenState(
        scan.calendar.seenTargetPageTokenDigests,
        scan.calendar.targetPageToken,
        page.nextPageToken,
      );
      const finishedPass = page.status === "complete";
      const needsNewTargetCoverage =
        scan.phase === "calendar_verify" &&
        verificationTargetIds.some((calendarId) => {
          if (noEventCoverageTargets.some((candidate) => candidate.calendarId === calendarId)) {
            return false;
          }
          const target = targets.find((candidate) => candidate.calendarId === calendarId);
          return !target?.baselineComplete || !target.replayComplete || !target.finalCursor;
        });
      const verifiedTargetSet = new Set(verificationTargetIds);
      if (
        finishedPass &&
        scan.phase === "calendar_verify" &&
        [...scan.calendar.targets, ...scan.calendar.noEventCoverageTargets].some(
          (target) => !verifiedTargetSet.has(target.calendarId),
        )
      ) {
        await this.#restartInitialPrivateGoogleScan(work, scan);
        return;
      }
      const reconciledTargets =
        finishedPass && scan.phase === "calendar_verify" && !needsNewTargetCoverage
          ? targets.filter((target) => verifiedTargetSet.has(target.calendarId))
          : targets;
      const reconciledNoEventCoverageTargets =
        finishedPass && scan.phase === "calendar_verify" && !needsNewTargetCoverage
          ? noEventCoverageTargets.filter((target) => verifiedTargetSet.has(target.calendarId))
          : noEventCoverageTargets;
      if (
        finishedPass &&
        scan.phase === "calendar_verify" &&
        !needsNewTargetCoverage &&
        (reconciledTargets.length + reconciledNoEventCoverageTargets.length !== verifiedTargetSet.size ||
          [...reconciledTargets, ...reconciledNoEventCoverageTargets].some(
            (target) => !verifiedTargetSet.has(target.calendarId),
          ))
      ) {
        throw new Error("Calendar target verification did not produce one exact accessible set");
      }
      const nextPhase: InitialPrivateGoogleScanV1["phase"] = !finishedPass
        ? scan.phase
        : scan.phase === "calendar_targets"
          ? "gmail_baseline"
          : needsNewTargetCoverage
            ? "calendar_baseline"
            : scan.calendar.finalBarrierStarted &&
                reconciledTargets.every(
                  (target) => target.manifestComplete && target.replayComplete && target.finalCursor !== null,
                )
              ? "ready"
              : reconciledTargets.every((target) => target.manifestComplete)
                ? "gmail_replay"
                : "calendar_manifest";
      const nextTargets =
        nextPhase === "gmail_replay"
          ? reconciledTargets.map((target) => ({ ...target, replayComplete: false }))
          : reconciledTargets;
      const nextScan: InitialPrivateGoogleScanV1 = {
        ...scan,
        phase: nextPhase,
        calendar: {
          ...scan.calendar,
          finalBarrierStarted:
            nextPhase === "gmail_replay"
              ? true
              : nextPhase === "calendar_baseline" || nextPhase === "calendar_manifest"
                ? false
                : scan.calendar.finalBarrierStarted,
          targetPageToken: page.nextPageToken,
          seenTargetPageTokenDigests: tokenState,
          verificationTargetIds,
          targets: nextTargets,
          noEventCoverageTargets: reconciledNoEventCoverageTargets.sort((left, right) =>
            left.calendarId.localeCompare(right.calendarId),
          ),
        },
      };
      await this.#checkpointInitialGoogleScan(work, scan, nextScan, [], [], []);
      return;
    }

    if (scan.phase === "gmail_baseline") {
      const page = await google.readGmailBaselinePage({
        householdId: work.household.householdId,
        ownerAdultId: work.adultId,
        connectionId: work.connectionId,
        after: scan.gmailAfter,
        before: scan.anchoredAt,
        ...(scan.gmail.baselinePageToken ? { pageToken: scan.gmail.baselinePageToken } : {}),
      });
      const prepared = prepareInitialGmailPage(work, scan, page.messages);
      const classified = await this.#classifyInitialGoogleSources(
        work,
        scan,
        prepared.sources,
        prepared.drafts,
        prepared.attachments,
      );
      const nextScan = mergeInitialGoogleScanOutcomes(
        {
          ...scan,
          phase: page.status === "complete" ? "calendar_baseline" : "gmail_baseline",
          gmail: {
            ...scan.gmail,
            baselinePageToken: page.nextPageToken,
            baselineComplete: page.status === "complete",
            seenPageTokenDigests: nextOpaquePageTokenState(
              scan.gmail.seenPageTokenDigests,
              scan.gmail.baselinePageToken,
              page.nextPageToken,
            ),
            seenMessageIdentities: prepared.identities,
          },
        },
        classified,
      );
      await this.#checkpointInitialGoogleScan(
        work,
        scan,
        nextScan,
        prepared.drafts,
        classified.classifiedSourceIds,
        classified.dismissedSourceIds,
      );
      return;
    }

    if (scan.phase === "calendar_baseline") {
      const targetIndex = scan.calendar.targets.findIndex((target) => !target.baselineComplete);
      if (targetIndex < 0) {
        await this.#checkpointInitialGoogleScan(
          work,
          scan,
          { ...scan, phase: scan.calendar.enumerationPass > 1 ? "calendar_replay" : "gmail_replay" },
          [],
          [],
          [],
        );
        return;
      }
      const target = scan.calendar.targets[targetIndex];
      if (!target) throw new Error("The initial Calendar target disappeared");
      const page = await google.readCalendarBaselineEventsPage({
        householdId: work.household.householdId,
        ownerAdultId: work.adultId,
        connectionId: work.connectionId,
        target: googleBaselineTarget(target),
        timeMin: scan.calendarTimeMin,
        timeMax: scan.calendarTimeMax,
        ...(target.baselinePageToken ? { pageToken: target.baselinePageToken } : {}),
      });
      if (page.status === "unavailable") {
        await this.#restartInitialPrivateGoogleScan(work, scan);
        return;
      }
      const prepared = prepareInitialCalendarPage(work, scan, target, page.events);
      const classified = await this.#classifyInitialGoogleSources(
        work,
        scan,
        prepared.sources,
        prepared.drafts,
        prepared.attachments,
      );
      const nextTargets = [...scan.calendar.targets];
      nextTargets[targetIndex] = {
        ...target,
        baselinePageToken: page.nextPageToken,
        baselineComplete: page.status === "complete",
        seenPageTokenDigests: nextOpaquePageTokenState(
          target.seenPageTokenDigests,
          target.baselinePageToken,
          page.nextPageToken,
        ),
        seenEventIdentities: prepared.identities,
      };
      const nextScan = mergeInitialGoogleScanOutcomes(
        {
          ...scan,
          phase:
            page.status === "complete" && nextTargets.every((candidate) => candidate.baselineComplete)
              ? scan.calendar.enumerationPass > 1
                ? "calendar_replay"
                : "gmail_replay"
              : "calendar_baseline",
          calendar: { ...scan.calendar, finalBarrierStarted: false, targets: nextTargets },
        },
        classified,
      );
      await this.#checkpointInitialGoogleScan(
        work,
        scan,
        nextScan,
        prepared.drafts,
        classified.classifiedSourceIds,
        classified.dismissedSourceIds,
      );
      return;
    }

    if (scan.phase === "gmail_replay") {
      const changes = await google.readGmailChanges({
        householdId: work.household.householdId,
        ownerAdultId: work.adultId,
        connectionId: work.connectionId,
        cursor: googleGmailProviderCursor(scan.gmail.finalCursor ?? scan.gmail.capturedCursor),
      });
      if (changes.resyncRequired) {
        await this.#restartInitialPrivateGoogleScan(work, scan);
        return;
      }
      const replayObservedAt = this.#now().toISOString();
      const prepared = prepareInitialGmailPage(
        work,
        scan,
        changes.messages.filter(
          (message) => message.sentAt >= scan.gmailAfter && message.sentAt <= replayObservedAt,
        ),
      );
      const classified = await this.#classifyInitialGoogleSources(
        work,
        scan,
        prepared.sources,
        prepared.drafts,
        prepared.attachments,
      );
      const removedSourceIds = changes.removedMessageIds.map((messageId) =>
        gmailEvidenceSourceId(work.household.householdId, work.connectionId, messageId),
      );
      const replayClassification: InitialGooglePageClassification = {
        ...classified,
        dismissedSourceIds: exactDistinct([...classified.dismissedSourceIds, ...removedSourceIds]),
      };
      const nextScan = mergeInitialGoogleScanOutcomes(
        {
          ...scan,
          phase: "calendar_replay",
          gmail: {
            ...scan.gmail,
            finalCursor: JSON.stringify(changes.cursor),
            seenMessageIdentities: prepared.identities,
          },
        },
        replayClassification,
      );
      await this.#checkpointInitialGoogleScan(
        work,
        scan,
        nextScan,
        prepared.drafts,
        replayClassification.classifiedSourceIds,
        replayClassification.dismissedSourceIds,
        removedSourceIds,
      );
      return;
    }

    if (scan.phase === "calendar_replay") {
      const targetIndex = scan.calendar.targets.findIndex((target) => !target.replayComplete);
      if (targetIndex < 0) {
        await this.#checkpointInitialGoogleScan(
          work,
          scan,
          beginCalendarVerificationPass({
            ...scan,
            calendar: { ...scan.calendar, finalBarrierStarted: false },
          }),
          [],
          [],
          [],
        );
        return;
      }
      const target = scan.calendar.targets[targetIndex];
      if (!target) throw new Error("The initial Calendar target disappeared");
      const capturedCursor = googleCalendarCursor(target.finalCursor ?? target.capturedCursor);
      const replayEvents = new Map<string, GoogleCalendarChange>();
      let finalCursor: GoogleCalendarBoundedCursor | null = null;
      const replayStarts = target.finalCursor
        ? [this.#now().toISOString()]
        : exactCalendarReplayStarts(scan.calendarTimeMin, scan.calendarTimeMax, this.#now().toISOString());
      for (const currentTime of replayStarts) {
        const changes = await google.readCalendarChanges({
          householdId: work.household.householdId,
          ownerAdultId: work.adultId,
          connectionId: work.connectionId,
          calendarId: target.calendarId,
          cursor: capturedCursor,
          currentTime,
        });
        if (changes.resyncRequired) {
          await this.#restartInitialPrivateGoogleScan(work, scan);
          return;
        }
        if (changes.status === "unavailable") {
          await this.#restartInitialPrivateGoogleScan(work, scan);
          return;
        }
        finalCursor = changes.cursor;
        for (const event of changes.events) {
          const key = `${event.providerEventId}\0${event.providerRevision}`;
          const existing = replayEvents.get(key);
          if (existing && JSON.stringify(existing) !== JSON.stringify(event)) {
            throw new Error("Calendar returned conflicting content for one event revision");
          }
          replayEvents.set(key, event);
        }
      }
      if (!finalCursor) throw new Error("Calendar replay did not produce a rolling cursor");
      const replayValues = [...replayEvents.values()];
      const removedSourceIds = exactDistinct(
        replayValues
          .filter(
            (event) => !calendarChangeFallsInsideWindow(event, scan.calendarTimeMin, scan.calendarTimeMax),
          )
          .map((event) =>
            calendarEvidenceSourceId(
              work.household.householdId,
              work.connectionId,
              target.calendarId,
              event.providerEventId,
            ),
          ),
      );
      const prepared = prepareInitialCalendarPage(
        work,
        scan,
        target,
        replayValues.filter((event) =>
          calendarChangeFallsInsideWindow(event, scan.calendarTimeMin, scan.calendarTimeMax),
        ),
      );
      const classified = await this.#classifyInitialGoogleSources(
        work,
        scan,
        prepared.sources,
        prepared.drafts,
        prepared.attachments,
      );
      const replayClassification: InitialGooglePageClassification = {
        ...classified,
        dismissedSourceIds: exactDistinct([...classified.dismissedSourceIds, ...removedSourceIds]),
      };
      const nextTargets = [...scan.calendar.targets];
      nextTargets[targetIndex] = {
        ...target,
        replayComplete: true,
        // The initial snapshot is intentionally anchored. Preserve only updatedMin from a later
        // replay; clamping the rolling bounds prevents a long scan from claiming it already read
        // events that entered the normal +21-day horizon after the immutable manifest window.
        finalCursor: JSON.stringify({
          ...finalCursor,
          windowTimeMin: scan.calendarTimeMin,
          windowTimeMax: scan.calendarTimeMax,
        }),
        seenEventIdentities: prepared.identities,
      };
      const updated = mergeInitialGoogleScanOutcomes(
        { ...scan, calendar: { ...scan.calendar, targets: nextTargets } },
        replayClassification,
      );
      const nextScan = nextTargets.every((candidate) => candidate.replayComplete)
        ? beginCalendarVerificationPass(updated)
        : updated;
      await this.#checkpointInitialGoogleScan(
        work,
        scan,
        nextScan,
        prepared.drafts,
        replayClassification.classifiedSourceIds,
        replayClassification.dismissedSourceIds,
        removedSourceIds,
      );
      return;
    }

    if (scan.phase === "calendar_manifest") {
      const targetIndex = scan.calendar.targets.findIndex((target) => !target.manifestComplete);
      if (targetIndex < 0) {
        await this.#checkpointInitialGoogleScan(work, scan, beginCalendarVerificationPass(scan), [], [], []);
        return;
      }
      const target = scan.calendar.targets[targetIndex];
      if (!target) throw new Error("The Calendar manifest target disappeared");
      const page = await google.readCalendarBaselineEventsPage({
        householdId: work.household.householdId,
        ownerAdultId: work.adultId,
        connectionId: work.connectionId,
        target: googleBaselineTarget(target),
        timeMin: scan.calendarTimeMin,
        timeMax: scan.calendarTimeMax,
        ...(target.manifestPageToken ? { pageToken: target.manifestPageToken } : {}),
      });
      if (page.status === "unavailable") {
        await this.#restartInitialPrivateGoogleScan(work, scan);
        return;
      }
      const prepared = prepareInitialCalendarPage(work, scan, target, page.events);
      const classified = await this.#classifyInitialGoogleSources(
        work,
        scan,
        prepared.sources,
        prepared.drafts,
        prepared.attachments,
      );
      const manifestProviderEventIds = exactDistinct([
        ...target.manifestProviderEventIds,
        ...page.events.map((event) => event.providerEventId),
      ]);
      const removedProviderEventIds =
        page.status === "complete"
          ? calendarProviderEventIds(target.seenEventIdentities).filter(
              (providerEventId) => !manifestProviderEventIds.includes(providerEventId),
            )
          : [];
      const removedSourceIds = removedProviderEventIds.map((providerEventId) =>
        calendarEvidenceSourceId(
          work.household.householdId,
          work.connectionId,
          target.calendarId,
          providerEventId,
        ),
      );
      const manifestClassification: InitialGooglePageClassification = {
        ...classified,
        dismissedSourceIds: exactDistinct([...classified.dismissedSourceIds, ...removedSourceIds]),
      };
      const nextTargets = [...scan.calendar.targets];
      nextTargets[targetIndex] = {
        ...target,
        manifestPageToken: page.nextPageToken,
        manifestComplete: page.status === "complete",
        manifestProviderEventIds,
        seenManifestPageTokenDigests: nextOpaquePageTokenState(
          target.seenManifestPageTokenDigests,
          target.manifestPageToken,
          page.nextPageToken,
        ),
        seenEventIdentities: prepared.identities,
      };
      const nextScan = mergeInitialGoogleScanOutcomes(
        {
          ...scan,
          calendar: { ...scan.calendar, finalBarrierStarted: false, targets: nextTargets },
        },
        manifestClassification,
      );
      await this.#checkpointInitialGoogleScan(
        work,
        scan,
        nextScan,
        prepared.drafts,
        manifestClassification.classifiedSourceIds,
        manifestClassification.dismissedSourceIds,
        removedSourceIds,
      );
      return;
    }

    if (scan.phase !== "ready" || !scan.gmail.finalCursor || !scan.calendar.finalBarrierStarted) {
      throw new Error("The initial Google review reached an invalid completion state");
    }
    if (scan.calendar.targets.some((target) => !target.finalCursor)) {
      throw new Error("The initial Google review did not close every Calendar target");
    }
    const completedAt = this.#now();
    const finalizedFindings = prioritizeInitialGoogleFindings(scan.outcomes.findings);
    const bubbles = privateInitialReviewBubbles({
      suggested: [],
      findings: finalizedFindings.filter((finding) => finding.surfaceNow),
    });
    await this.#store.completePrivateInitialReview({
      workId: work.workId,
      generationKey: digestOpaqueProviderState(`${scan.kind}\0${scan.anchoredAt}\0${work.connectionId}`),
      gmailCursor: JSON.stringify({
        kind: "gmail_poll_cursor_v1",
        scannerVersion: scan.scannerVersion,
        connectionId: scan.connectionId,
        provider: googleGmailProviderCursor(scan.gmail.finalCursor),
      }),
      calendarCursor: JSON.stringify({
        kind: "calendar_account_cursor_v1",
        scannerVersion: scan.scannerVersion,
        connectionId: scan.connectionId,
        enumeratedAt: completedAt.toISOString(),
        targets: scan.calendar.targets.map((target) => ({
          target: googleBaselineTarget(target),
          provider: googleCalendarCursor(target.finalCursor as string),
        })),
        noEventCoverageTargets: scan.calendar.noEventCoverageTargets,
      }),
      bubbles,
      findings: finalizedFindings.map((finding) => ({
        privateSummary: finding.privateSummary,
        ...(finding.actionAnchorDigest ? { actionAnchorDigest: finding.actionAnchorDigest } : {}),
        sourceIds: finding.sourceIds,
        householdCandidate: finding.householdCandidate,
        monitor: finding.monitor ?? null,
        familyCalendar: finding.familyCalendar ?? null,
        urgency: finding.urgency,
        dueAt: finding.dueAt,
        surfaceNow: finding.surfaceNow,
      })),
      facts: resolvedInitialGoogleFacts(scan.outcomes.facts).map(
        ({ observedAt: _observedAt, sourceObservations: _sourceObservations, ...fact }) => fact,
      ),
      googleEvidence: [],
      rescanDeliverNotBefore: proactiveDeliveryAt(
        completedAt,
        work.household.timeZone,
        finalizedFindings.some((finding) => finding.surfaceNow && finding.urgency === "now"),
      ).toISOString(),
      occurredAt: completedAt.toISOString(),
    });
  }

  async #restartInitialPrivateGoogleScan(
    work: Extract<InitialIntelligenceWork, { kind: "initial_private_review" }>,
    current: InitialPrivateGoogleScanV1,
  ): Promise<void> {
    const google = this.#google;
    if (!google) throw new Error("Florence's proactive Google review is not configured");
    const anchoredAt = this.#now().toISOString();
    const captured = await google.captureGmailCursor({
      householdId: work.household.householdId,
      ownerAdultId: work.adultId,
      connectionId: work.connectionId,
    });
    await this.#store.restartInitialPrivateGoogleScan({
      workId: work.workId,
      expectedStateDigest: initialPrivateGoogleScanDigest(current),
      scan: newInitialPrivateGoogleScan(work, anchoredAt, captured),
    });
  }

  async #classifyInitialGoogleSources(
    work: Extract<InitialIntelligenceWork, { kind: "initial_private_review" }>,
    scan: InitialPrivateGoogleScanV1,
    sources: readonly (FlorencePrivateGmailSource | FlorencePrivateCalendarEvent)[],
    drafts: readonly GoogleEvidenceDraft[],
    attachments: ReadonlyMap<string, GmailAttachmentReference>,
  ): Promise<InitialGooglePageClassification> {
    if (sources.length === 0) {
      return { findings: [], facts: [], classifiedSourceIds: [], dismissedSourceIds: [] };
    }
    const evidence = new Map(drafts.map((draft) => [draft.id, draft]));
    const calendarSources = sources.filter(
      (source): source is FlorencePrivateCalendarEvent => source.kind === "calendar",
    );
    const decisions: FlorencePrivateGoogleBatchDecision[] = [];
    for (const batch of privateGoogleModelBatches(sources)) {
      const decision = await this.#reasoner?.classifyPrivateGoogleBatch(
        {
          familyProfile: initialFamilyProfile(work.household),
          adult: { adultId: work.adultId, firstName: work.adultFirstName },
          googleConnection: { connectionId: work.connectionId, status: "active", kind: "personal" },
          currentTime: scan.anchoredAt,
          currentFacts: [
            ...work.currentFacts,
            ...resolvedInitialGoogleFacts(scan.outcomes.facts).map(({ slot, statement }) => ({
              slot,
              statement,
            })),
          ].slice(-100),
          sources: batch,
          reviewKind: "initial",
        },
        {
          readGmailAttachment: async ({ connectionId, sourceId, attachment }) => {
            if (connectionId !== work.connectionId) {
              throw new Error("The Google scan requested another adult's Gmail attachment");
            }
            const source = batch.find(
              (candidate): candidate is FlorencePrivateGmailSource =>
                candidate.kind === "gmail" && candidate.sourceId === sourceId,
            );
            const reference = source?.attachments.find(
              (candidate) => candidate.attachmentRef === attachment.attachmentRef,
            );
            if (!source || !reference) throw new Error("The Gmail attachment was not in this scan batch");
            const providerReference = attachments.get(`${sourceId}\0${reference.attachmentRef}`);
            if (!providerReference) throw new Error("The Gmail attachment provider reference is unavailable");
            const read = await this.#google?.readGmailAttachment({
              householdId: work.household.householdId,
              ownerAdultId: work.adultId,
              connectionId,
              attachment: providerReference,
            });
            if (!read) throw new Error("Google is not configured");
            return {
              sourceId,
              attachmentRef: reference.attachmentRef,
              filename: read.filename,
              mimeType: read.mimeType,
              bytes: read.bytes,
            };
          },
        },
      );
      if (!decision) throw new Error("Florence's Google classifier is unavailable");
      decisions.push(decision);
    }
    const findings = decisions.flatMap((decision) =>
      decision.findings.map((finding): InitialGoogleScanFinding => {
        if (finding.familyRelevance === "adult_only") {
          throw new Error("Adult-only Google evidence cannot become a retained finding");
        }
        const sharing = privateCalendarSafeBackgroundSharing({
          familyRelevance: finding.familyRelevance,
          conclusion: finding.candidate,
          sourceIds: finding.sourceIds,
          familyCalendar: finding.familyCalendar ?? null,
          googleEvidence: evidence,
          adultFirstName: work.adultFirstName,
          timeZone: work.household.timeZone,
        });
        return {
          privateSummary: privateInitialReviewFindingSummary({
            fallback: finding.privateSummary,
            sourceIds: finding.sourceIds,
            calendarSources,
            timeZone: work.household.timeZone,
          }),
          actionAnchorDigest: googleFindingActionAnchorDigest(
            finding.sourceIds,
            finding.actionAnchor,
            evidence,
          ),
          familyRelevance: finding.familyRelevance,
          sourceIds: finding.sourceIds,
          urgency: finding.urgency,
          dueAt: finding.dueAt,
          surfaceNow: finding.surfaceNow,
          householdCandidate: sharing.conclusion,
          monitor: finding.monitor ?? null,
          familyCalendar: sharing.familyCalendar,
          observedAt: latestGoogleEvidenceTime(finding.sourceIds, evidence),
        };
      }),
    );
    const facts = decisions.flatMap((decision) =>
      decision.facts.map(
        (fact): InitialGoogleScanFact => ({
          ...fact,
          observedAt: latestGoogleEvidenceTime(fact.sourceIds, evidence),
          sourceObservations: fact.sourceIds.map((sourceId) => ({
            sourceId,
            observedAt: googleEvidenceTime(sourceId, evidence),
          })),
        }),
      ),
    );
    return {
      findings,
      facts,
      classifiedSourceIds: exactDistinct([
        ...findings.flatMap((finding) => [...finding.sourceIds]),
        ...facts.flatMap((fact) => [...fact.sourceIds]),
      ]),
      dismissedSourceIds: exactDistinct(decisions.flatMap((decision) => decision.dismissedSourceIds)),
    };
  }

  async #checkpointInitialGoogleScan(
    work: Extract<InitialIntelligenceWork, { kind: "initial_private_review" }>,
    current: InitialPrivateGoogleScanV1,
    next: InitialPrivateGoogleScanV1,
    googleEvidence: readonly GoogleEvidenceDraft[],
    classifiedSourceIds: readonly string[],
    dismissedSourceIds: readonly string[],
    removedSourceIds: readonly string[] = [],
  ): Promise<void> {
    await this.#store.commitInitialPrivateGoogleScanPage({
      workId: work.workId,
      expectedStateDigest: initialPrivateGoogleScanDigest(current),
      nextScan: next,
      googleEvidence,
      classifiedSourceIds,
      dismissedSourceIds,
      removedSourceIds,
      occurredAt: this.#now().toISOString(),
    });
  }

  #launchFamilyWork(work: Extract<DueProactiveWork, { kind: "family_task" }>): void {
    const existing = this.#activeFamilyWork.get(work.workId);
    if (existing) {
      existing.controller.abort(new Error("A newer family-task checkpoint was claimed"));
      void existing.promise.finally(() => this.#launchFamilyWork(work));
      return;
    }
    const controller = new AbortController();
    const promise = this.#executeFamilyWork(work, controller.signal)
      .catch(() => undefined)
      .finally(() => {
        const active = this.#activeFamilyWork.get(work.workId);
        if (active?.promise === promise) this.#activeFamilyWork.delete(work.workId);
        this.#wake();
      });
    this.#activeFamilyWork.set(work.workId, { controller, promise });
  }

  async #executeFamilyWork(
    work: Extract<DueProactiveWork, { kind: "family_task" }>,
    signal: AbortSignal,
  ): Promise<void> {
    const reasoner = this.#reasoner;
    if (!reasoner) {
      await this.#store.settleFamilyWorkClaim({
        workId: work.workId,
        generation: work.generation,
        claimId: work.claimId,
        settledAt: this.#now().toISOString(),
        result: {
          type: "retry",
          state: { ...work.state, claim: null },
          retryAt: later(this.#now(), RETRY_MS),
          error: "Florence's task reasoner is not configured",
        },
      });
      return;
    }
    try {
      const maps = this.#maps;
      const publicPages = this.#publicPages;
      const weather = this.#weather;
      const flights = this.#flights;
      const google = this.#google;
      const familyWorkOwnerAdultId = work.ownerAdultId;
      const familyWorkGoogleConnections =
        google && work.visibility === "private" && familyWorkOwnerAdultId
          ? await google.status({
              householdId: work.household.householdId,
              ownerAdultId: familyWorkOwnerAdultId,
            })
          : [];
      const familyWorkCalendarConnection = familyWorkGoogleConnections.find(
        (connection) => connection.status === "active",
      );
      const familyWorkWorkspaceConnection = familyWorkGoogleConnections.find(
        (connection) =>
          connection.status === "active" &&
          GOOGLE_WORKSPACE_ACTION_SCOPES.every((scope) => connection.grantedScopes.includes(scope)),
      );
      const familyWorkHousehold =
        familyWorkCalendarConnection && familyWorkOwnerAdultId
          ? await this.#store.readHousehold({
              householdId: work.household.householdId,
              viewerAdultId: familyWorkOwnerAdultId,
            })
          : null;
      const familyWorkCalendarRef = (calendarId: string): string => {
        if (!familyWorkCalendarConnection) throw new Error("Google Calendar is unavailable");
        return `calendar_${sha256(
          `${work.workId}\0${familyWorkCalendarConnection.connectionId}\0${calendarId}`,
        ).slice(0, 32)}`;
      };
      const listFamilyWorkCalendars = async (): Promise<FlorenceCalendarCatalogRead> => {
        if (!google || !familyWorkCalendarConnection || !familyWorkOwnerAdultId) {
          return { status: "unavailable", calendars: [], totalCalendarCount: 0 };
        }
        const read = await google.readPersonalCalendarCatalog({
          householdId: work.household.householdId,
          ownerAdultId: familyWorkOwnerAdultId,
          connectionId: familyWorkCalendarConnection.connectionId,
          excludedFamilyCalendarId: familyWorkHousehold?.familyCalendarId ?? null,
        });
        return {
          status: read.status,
          calendars: read.calendars.slice(0, 100).map((calendar) => ({
            calendarRef: familyWorkCalendarRef(calendar.calendarId),
            label: calendar.label ?? (calendar.primary ? "Primary calendar" : "Calendar"),
            timeZone: calendar.timeZone,
            primary: calendar.primary,
            accessRole: calendar.accessRole,
            eventCoverage: calendar.eventCoverage,
          })),
          totalCalendarCount: read.totalCalendarCount,
        };
      };
      const readFamilyWorkCalendarWindow = async (input: {
        timeMin: string;
        timeMax: string;
        limit: number;
        scope: "all" | "primary" | "selected";
        calendarRefs: readonly string[];
      }): Promise<FlorenceCalendarWindowRead> => {
        if (!google || !familyWorkCalendarConnection || !familyWorkOwnerAdultId) {
          return {
            status: "unavailable",
            calendars: [],
            totalCalendarCount: 0,
            events: [],
            totalEventCount: 0,
          };
        }
        let calendarIds: readonly string[] | undefined;
        if (input.scope === "selected") {
          const catalog = await google.readPersonalCalendarCatalog({
            householdId: work.household.householdId,
            ownerAdultId: familyWorkOwnerAdultId,
            connectionId: familyWorkCalendarConnection.connectionId,
            excludedFamilyCalendarId: familyWorkHousehold?.familyCalendarId ?? null,
          });
          const idsByRef = new Map(
            catalog.calendars.map((calendar) => [
              familyWorkCalendarRef(calendar.calendarId),
              calendar.calendarId,
            ]),
          );
          const resolved = input.calendarRefs.flatMap((calendarRef) => {
            const calendarId = idsByRef.get(calendarRef);
            return calendarId ? [calendarId] : [];
          });
          if (resolved.length !== input.calendarRefs.length) {
            return {
              status: "unavailable",
              calendars: [],
              totalCalendarCount: 0,
              events: [],
              totalEventCount: 0,
            };
          }
          calendarIds = resolved;
        } else if (input.scope === "primary") {
          const catalog = await google.readPersonalCalendarCatalog({
            householdId: work.household.householdId,
            ownerAdultId: familyWorkOwnerAdultId,
            connectionId: familyWorkCalendarConnection.connectionId,
            excludedFamilyCalendarId: familyWorkHousehold?.familyCalendarId ?? null,
          });
          const primary = catalog.calendars.find((calendar) => calendar.primary);
          if (!primary) {
            return {
              status: "unavailable",
              calendars: [],
              totalCalendarCount: 0,
              events: [],
              totalEventCount: 0,
            };
          }
          calendarIds = [primary.calendarId];
        }
        const read = await google.readPersonalCalendarWindow({
          householdId: work.household.householdId,
          ownerAdultId: familyWorkOwnerAdultId,
          connectionId: familyWorkCalendarConnection.connectionId,
          excludedFamilyCalendarId: familyWorkHousehold?.familyCalendarId ?? null,
          timeMin: input.timeMin,
          timeMax: input.timeMax,
          ...(calendarIds === undefined ? {} : { calendarIds }),
          limit: input.limit,
        });
        const labels = new Map(read.calendars.map((calendar) => [calendar.calendarId, calendar.label]));
        const eventRef = (event: GooglePersonalCalendarWindowRead["events"][number]): string =>
          `event_${sha256(
            `${work.workId}\0${familyWorkCalendarConnection.connectionId}\0${event.calendarId}\0${event.providerEventId}\0${event.providerRevision}`,
          ).slice(0, 32)}`;
        return {
          status: read.status,
          calendars: read.calendars.slice(0, 100).map((calendar) => ({
            calendarRef: familyWorkCalendarRef(calendar.calendarId),
            label: calendar.label,
            timeZone: calendar.timeZone,
            primary: calendar.status === "missing" ? null : calendar.primary,
            accessRole: calendar.accessRole,
            status: calendar.status,
            eventCount: calendar.eventCount,
          })),
          totalCalendarCount: read.totalCalendarCount,
          totalEventCount: read.totalEventCount,
          events: read.events.map((event) =>
            event.intervalKind === "all_day"
              ? {
                  intervalKind: event.intervalKind,
                  calendarRef: familyWorkCalendarRef(event.calendarId),
                  calendarLabel: labels.get(event.calendarId) ?? null,
                  eventRef: eventRef(event),
                  title: event.title,
                  startDate: event.startDate,
                  endDate: event.endDate,
                  providerUpdatedAt: event.providerUpdatedAt,
                  status: event.status,
                  busy: event.busy,
                  location: event.location,
                }
              : {
                  intervalKind: event.intervalKind,
                  calendarRef: familyWorkCalendarRef(event.calendarId),
                  calendarLabel: labels.get(event.calendarId) ?? null,
                  eventRef: eventRef(event),
                  title: event.title,
                  startsAt: event.startsAt,
                  endsAt: event.endsAt,
                  providerUpdatedAt: event.providerUpdatedAt,
                  status: event.status,
                  busy: event.busy,
                  timeZone: event.timeZone,
                  location: event.location,
                },
          ),
        };
      };
      const step = await reasoner.continueFamilyWork(
        {
          workId: work.workId,
          objective: work.objective,
          visibility: work.visibility,
          ownerAdultId: work.ownerAdultId,
          household: work.household,
          state: work.state,
          currentTime: this.#now().toISOString(),
        },
        {
          ...(publicPages
            ? { runPublicPage: (request, taskSignal) => publicPages.run(request, taskSignal) }
            : {}),
          ...(maps ? { runMaps: (request, taskSignal) => maps.run(request, taskSignal) } : {}),
          ...(weather ? { runWeather: (request, taskSignal) => weather.run(request, taskSignal) } : {}),
          ...(flights ? { runFlights: (request, taskSignal) => flights.search(request, taskSignal) } : {}),
          ...(familyWorkCalendarConnection && familyWorkOwnerAdultId
            ? {
                listCalendars: listFamilyWorkCalendars,
                readCalendarWindow: readFamilyWorkCalendarWindow,
              }
            : {}),
          ...(google && familyWorkWorkspaceConnection && familyWorkOwnerAdultId
            ? {
                runGoogleWorkspace: (operation: GoogleWorkspaceOperation, taskSignal?: AbortSignal) =>
                  google.runWorkspace(
                    {
                      householdId: work.household.householdId,
                      ownerAdultId: familyWorkOwnerAdultId,
                      connectionId: familyWorkWorkspaceConnection.connectionId,
                      operation,
                    },
                    taskSignal,
                  ),
              }
            : {}),
        },
        signal,
      );
      signal.throwIfAborted();
      const settledAt = this.#now().toISOString();
      if (step.kind === "continue") {
        await this.#store.settleFamilyWorkClaim({
          workId: work.workId,
          generation: work.generation,
          claimId: work.claimId,
          settledAt,
          result: {
            type: "continue",
            state: step.state,
            nextCheckAt: settledAt,
            ...(step.progressText ? { progressText: step.progressText } : {}),
          },
        });
      } else if (step.kind === "waiting") {
        await this.#store.settleFamilyWorkClaim({
          workId: work.workId,
          generation: work.generation,
          claimId: work.claimId,
          settledAt,
          result: { type: "waiting", state: step.state, question: step.question },
        });
      } else {
        await this.#store.settleFamilyWorkClaim({
          workId: work.workId,
          generation: work.generation,
          claimId: work.claimId,
          settledAt,
          result: { type: "terminal", state: step.state, terminalText: step.text },
        });
      }
    } catch (error) {
      if (signal.aborted) {
        const interruptedAt = this.#now();
        await this.#store.settleFamilyWorkClaim({
          workId: work.workId,
          generation: work.generation,
          claimId: work.claimId,
          settledAt: interruptedAt.toISOString(),
          result: {
            type: "retry",
            state: { ...work.state, claim: null },
            retryAt: later(interruptedAt, 1),
            error: "The task step was interrupted before its checkpoint and will resume",
          },
        });
        return;
      }
      await this.#store.settleFamilyWorkClaim({
        workId: work.workId,
        generation: work.generation,
        claimId: work.claimId,
        settledAt: this.#now().toISOString(),
        result: {
          type: "retry",
          state: { ...work.state, claim: null },
          retryAt: later(this.#now(), RETRY_MS),
          error: errorText(error),
        },
      });
    }
  }

  async #executeProactiveWork(work: Exclude<DueProactiveWork, { kind: "family_task" }>): Promise<void> {
    if (work.kind === "reminder") {
      await this.#store.fireDueReminder({
        workId: work.workId,
        occurredAt: this.#now().toISOString(),
      });
      return;
    }
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
            subject: message.subject === null ? null : modelSafeGmailText(message.subject),
            text: modelSafeGmailText(message.text),
            textStatus: message.textStatus,
            attachments: message.attachments.map((attachment) => ({
              attachmentRef: gmailAttachmentRefFor(source.id, attachment),
              filename: attachment.filename,
              mimeType: attachment.mimeType,
              sizeBytes: attachment.sizeBytes,
            })),
            attachmentsStatus: message.attachmentsStatus,
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
        const householdConclusion = privateCalendarSafeHouseholdConclusion({
          conclusion: result.householdConclusion,
          sourceIds: result.sourceIds,
          googleEvidence,
          adultFirstName: work.adultFirstName,
        });
        const completedAt = this.#now();
        await this.#store.completeFiniteMonitor({
          workId: work.workId,
          outcome: result.outcome,
          privateDetail: work.visibility === "private" ? result.privateDetail : null,
          householdConclusion: householdConclusion?.summary ?? null,
          householdCategory: householdConclusion?.category ?? null,
          sourceIds: result.sourceIds,
          currentConclusion:
            work.visibility === "household" && householdConclusion
              ? householdConclusion.summary
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
      let removedGmailSourceIds: string[] = [];
      const removedCalendarSourceIds: string[] = [];
      let nextGmailCursor: GoogleGmailCursor | null = null;
      let gmailStatus: "complete" | "truncated" | "unavailable" = "unavailable";
      if (work.kind === "personal_google_poll") {
        const cursor = googleGmailCursor(work.gmailCursor, work.connectionId);
        const changes = await this.#google.readGmailChanges({
          householdId: work.household.householdId,
          ownerAdultId: work.adultId,
          connectionId: work.connectionId,
          cursor,
        });
        if (changes.resyncRequired) {
          await this.#store.restartPersonalGooglePollAsInitialScan({
            workId: work.workId,
            connectionId: work.connectionId,
            now: currentTime,
          });
          return;
        } else {
          nextGmailCursor = changes.cursor;
          gmailStatus = "complete";
          removedGmailSourceIds = changes.removedMessageIds.map((messageId) =>
            gmailEvidenceSourceId(work.household.householdId, work.connectionId, messageId),
          );
          gmailMessages.push(
            ...changes.messages.filter(
              (message) => message.sentAt >= gmailAfter && message.sentAt <= currentTime,
            ),
          );
        }
      }

      let calendarStatus: "complete" | "truncated" | "unavailable" = "complete";
      const calendarEvents: {
        calendarId: string;
        event: GoogleCalendarWindowEvent | GoogleCalendarChange;
      }[] = [];
      let nextCalendarCursor: string;
      if (work.kind === "personal_google_poll") {
        const accountCursor = googleCalendarAccountCursor(work.calendarCursor, work.connectionId);
        const targets: GoogleCalendarBaselineTarget[] = [];
        const noEventCoverageTargets: Omit<GoogleCalendarNoEventCoverageTarget, "label">[] = [];
        const targetIds = new Set<string>();
        const pageTokens = new Set<string>();
        let pageToken: string | null = null;
        while (true) {
          const page = await google.readCalendarBaselineTargetsPage({
            householdId: work.household.householdId,
            ownerAdultId: work.adultId,
            connectionId: work.connectionId,
            excludedFamilyCalendarId: work.excludedFamilyCalendarId,
            ...(pageToken ? { pageToken } : {}),
          });
          for (const target of page.targets) {
            if (targetIds.has(target.calendarId)) {
              throw new Error("Google repeated a Calendar target during an account poll");
            }
            targetIds.add(target.calendarId);
            targets.push(target);
          }
          for (const target of page.noEventCoverageTargets ?? []) {
            if (targetIds.has(target.calendarId)) {
              throw new Error("Google repeated a no-event Calendar target during an account poll");
            }
            targetIds.add(target.calendarId);
            noEventCoverageTargets.push({
              calendarId: target.calendarId,
              timeZone: target.timeZone,
              accessRole: target.accessRole,
              primary: target.primary,
            });
          }
          if (page.status === "complete") break;
          if (pageTokens.has(page.nextPageToken)) {
            throw new Error("Google returned a cyclic Calendar target pagination sequence");
          }
          pageTokens.add(page.nextPageToken);
          pageToken = page.nextPageToken;
        }
        const existingIds = new Set(accountCursor.targets.map(({ target }) => target.calendarId));
        const existingNoEventCoverage = new Map(
          accountCursor.noEventCoverageTargets.map((target) => [target.calendarId, target]),
        );
        if (
          targets.length !== existingIds.size ||
          targets.some((target) => !existingIds.has(target.calendarId)) ||
          noEventCoverageTargets.length !== existingNoEventCoverage.size ||
          noEventCoverageTargets.some((target) => {
            const existing = existingNoEventCoverage.get(target.calendarId);
            return (
              !existing ||
              existing.timeZone !== target.timeZone ||
              existing.accessRole !== target.accessRole ||
              existing.primary !== target.primary
            );
          })
        ) {
          await this.#store.restartPersonalGooglePollAsInitialScan({
            workId: work.workId,
            connectionId: work.connectionId,
            now: currentTime,
          });
          return;
        }
        const nextTargets: GoogleCalendarAccountCursorV1["targets"] = [];
        for (const target of targets) {
          const stored = accountCursor.targets.find(
            (candidate) => candidate.target.calendarId === target.calendarId,
          );
          if (
            !stored ||
            stored.target.timeZone !== target.timeZone ||
            stored.target.accessRole !== target.accessRole ||
            stored.target.primary !== target.primary
          ) {
            await this.#store.restartPersonalGooglePollAsInitialScan({
              workId: work.workId,
              connectionId: work.connectionId,
              now: currentTime,
            });
            return;
          }
          const changes = await google.readCalendarChanges({
            householdId: work.household.householdId,
            ownerAdultId: work.adultId,
            connectionId: work.connectionId,
            calendarId: target.calendarId,
            cursor: stored.provider,
            currentTime,
          });
          if (changes.resyncRequired) {
            await this.#store.restartPersonalGooglePollAsInitialScan({
              workId: work.workId,
              connectionId: work.connectionId,
              now: currentTime,
            });
            return;
          }
          if (changes.status === "unavailable") {
            await this.#store.restartPersonalGooglePollAsInitialScan({
              workId: work.workId,
              connectionId: work.connectionId,
              now: currentTime,
            });
            return;
          }
          for (const event of changes.events) {
            if (calendarChangeFallsInsideWindow(event, currentTime, calendarTimeMax)) {
              calendarEvents.push({ calendarId: target.calendarId, event });
            } else {
              removedCalendarSourceIds.push(
                calendarEvidenceSourceId(
                  work.household.householdId,
                  work.connectionId,
                  target.calendarId,
                  event.providerEventId,
                ),
              );
            }
          }
          nextTargets.push({ target: googleBaselineTarget(target), provider: changes.cursor });
        }
        nextCalendarCursor = JSON.stringify({
          kind: "calendar_account_cursor_v1",
          scannerVersion: "complete_private_google_review_v1",
          connectionId: work.connectionId,
          enumeratedAt: currentTime,
          targets: nextTargets,
          noEventCoverageTargets: noEventCoverageTargets.sort((left, right) =>
            left.calendarId.localeCompare(right.calendarId),
          ),
        });
      } else {
        const calendarCursor = googleCalendarCursor(work.calendarCursor);
        const changes = await google.readCalendarChanges({
          householdId: work.household.householdId,
          ownerAdultId: work.adultId,
          connectionId: work.connectionId,
          calendarId: work.calendarId,
          cursor: calendarCursor,
          currentTime,
        });
        if (changes.resyncRequired) {
          const baseline = await google.readInitialCalendarReview({
            householdId: work.household.householdId,
            ownerAdultId: work.adultId,
            connectionId: work.connectionId,
            calendarId: work.calendarId,
            currentTime,
            limit: 50,
          });
          if (baseline.status !== "complete" || !baseline.cursor) {
            throw new Error("The family Calendar resync could not cover its rolling window");
          }
          calendarEvents.push(...baseline.events.map((event) => ({ calendarId: work.calendarId, event })));
          nextCalendarCursor = JSON.stringify(baseline.cursor);
        } else {
          if (changes.status === "unavailable") calendarStatus = "unavailable";
          for (const event of changes.events) {
            if (calendarChangeFallsInsideWindow(event, currentTime, calendarTimeMax)) {
              calendarEvents.push({ calendarId: work.calendarId, event });
            } else {
              removedCalendarSourceIds.push(
                calendarEvidenceSourceId(
                  work.household.householdId,
                  work.connectionId,
                  work.calendarId,
                  event.providerEventId,
                ),
              );
            }
          }
          nextCalendarCursor = JSON.stringify(changes.cursor);
        }
      }

      const attachmentIndex = new Map<string, GmailAttachmentReference>();
      const googleEvidence = new Map<string, GoogleEvidenceDraft>();
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
        for (const attachment of message.attachments) {
          attachmentIndex.set(`${source.id}\0${gmailAttachmentRefFor(source.id, attachment)}`, attachment);
        }
        return {
          sourceId: source.id,
          kind: "gmail",
          visibility: "adult_private",
          sentAt: message.sentAt,
          sender: message.from,
          subject: message.subject === null ? null : modelSafeGmailText(message.subject),
          text: modelSafeGmailText(message.text),
          textStatus: message.textStatus,
          attachments: message.attachments.map((attachment) => ({
            attachmentRef: gmailAttachmentRefFor(source.id, attachment),
            filename: attachment.filename,
            mimeType: attachment.mimeType,
            sizeBytes: attachment.sizeBytes,
          })),
          attachmentsStatus: message.attachmentsStatus,
        };
      });
      const calendarSources = calendarEvents.map(({ calendarId, event }) => {
        const interval = calendarEvidenceInterval(event, work.household.timeZone);
        const source = draftCalendarEvidence({
          householdId: work.household.householdId,
          ownerAdultId: work.adultId,
          connectionId: work.connectionId,
          calendarId,
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
      if (gmailSources.length === 0 && calendarSources.length === 0) {
        const completedAt = this.#now().toISOString();
        if (work.kind === "personal_google_poll" && !nextGmailCursor) {
          throw new Error("The personal Google poll did not advance its Gmail cursor");
        }
        await this.#store.completeGooglePoll({
          workId: work.workId,
          gmailCursor: nextGmailCursor ? googleGmailPollCursor(nextGmailCursor, work.connectionId) : null,
          calendarCursor: nextCalendarCursor,
          googleEvidence: [],
          reviewedGoogleSources: [],
          removedGoogleSourceIds: exactDistinct([...removedGmailSourceIds, ...removedCalendarSourceIds]),
          deliverNotBefore: completedAt,
          deliveries: [],
          facts: [],
          occurredAt: completedAt,
        });
        return;
      }
      const readTools = {
        readGmailAttachment: async ({ connectionId, sourceId, attachment }) => {
          if (connectionId !== work.connectionId) {
            throw new Error("The Google change review requested another adult's Gmail attachment");
          }
          const reference = attachmentIndex.get(`${sourceId}\0${attachment.attachmentRef}`);
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
            attachmentRef: attachment.attachmentRef,
            filename: read.filename,
            mimeType: read.mimeType,
            bytes: read.bytes,
          };
        },
      } satisfies Parameters<FlorenceReasoner["assessGoogleChanges"]>[1];
      const decisions: Awaited<ReturnType<FlorenceReasoner["assessGoogleChanges"]>>[] = [];
      for (const batch of privateGoogleModelBatches([...gmailSources, ...calendarSources])) {
        const batchGmail = batch.filter(
          (source): source is FlorencePrivateGmailSource => source.kind === "gmail",
        );
        const batchCalendar = batch.filter(
          (source): source is FlorencePrivateCalendarEvent => source.kind === "calendar",
        );
        const evidence: FlorenceBoundedPrivateGoogleEvidence = {
          gmail: { status: gmailStatus, after: gmailAfter, before: currentTime, sources: batchGmail },
          calendar: {
            status: calendarStatus,
            timeMin: currentTime,
            timeMax: calendarTimeMax,
            events: batchCalendar,
          },
        };
        decisions.push(
          await this.#reasoner.assessGoogleChanges(
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
              currentFacts: work.kind === "personal_google_poll" ? [...work.currentFacts] : [],
            },
            readTools,
          ),
        );
      }
      const mergedFacts = new Map<
        string,
        Awaited<ReturnType<FlorenceReasoner["assessGoogleChanges"]>>["facts"][number]
      >();
      for (const fact of decisions.flatMap((candidate) => candidate.facts)) {
        const current = mergedFacts.get(fact.slot);
        if (
          current &&
          (current.statement !== fact.statement || current.familyRelevance !== fact.familyRelevance)
        ) {
          throw new Error(`Google change batches disagreed about stable fact ${fact.slot}`);
        }
        mergedFacts.set(
          fact.slot,
          current
            ? { ...current, sourceIds: exactDistinct([...current.sourceIds, ...fact.sourceIds]) }
            : fact,
        );
      }
      const decision = {
        findings: decisions.flatMap((candidate) => candidate.findings),
        facts: [...mergedFacts.values()],
      };
      const activeMonitorIds = new Set(work.activeMonitors.map((monitor) => monitor.monitorId));
      const retainedFindings = decision.findings.filter((finding) => {
        if (work.kind !== "personal_google_poll") return finding.materialChange;
        const existingMonitorChange =
          finding.monitor !== null &&
          finding.monitor.operation !== "create" &&
          activeMonitorIds.has(finding.monitor.monitorId);
        return finding.familyRelevance !== "adult_only" || existingMonitorChange;
      });
      const storeFindings = retainedFindings.map((finding) => {
        const sharing = privateCalendarSafeBackgroundSharing({
          familyRelevance: finding.familyRelevance,
          conclusion: finding.householdConclusion,
          sourceIds: finding.sourceIds,
          familyCalendar: finding.familyCalendar ?? null,
          googleEvidence,
          adultFirstName: work.adultFirstName,
          timeZone: work.household.timeZone,
        });
        return {
          ...finding,
          householdConclusion: sharing.conclusion,
          familyCalendar: sharing.familyCalendar,
        };
      });
      const retainedFacts = decision.facts.filter((fact) => fact.familyRelevance !== "adult_only");
      let deliveries = storeFindings
        .filter(
          (finding) =>
            work.visibility === "private" ||
            finding.householdConclusion !== null ||
            finding.monitor !== null ||
            (finding.familyCalendar ?? null) !== null,
        )
        .map(
          (finding): ProactiveDelivery => ({
            privateDetail:
              work.visibility === "private"
                ? privateGoogleFindingDetail({
                    fallback: finding.privateDetail,
                    sourceIds: finding.sourceIds,
                    calendarSources,
                    timeZone: work.household.timeZone,
                  })
                : null,
            householdConclusion: finding.householdConclusion?.summary ?? null,
            householdCategory: finding.householdConclusion?.category ?? null,
            householdNeedsAnswer: finding.householdConclusion?.needsAnswer ?? false,
            sourceIds: finding.sourceIds,
            actionAnchorDigest: googleFindingActionAnchorDigest(
              finding.sourceIds,
              finding.actionAnchor,
              googleEvidence,
            ),
            urgency: finding.urgency,
            dueAt: finding.dueAt,
            monitor: finding.monitor,
            familyCalendar: finding.familyCalendar ?? null,
            surfaceNow: finding.materialChange,
            preserveDocket: !finding.materialChange,
          }),
        );
      if (work.kind === "personal_google_poll" && deliveries.length > 3) {
        const urgencyRank = { now: 0, soon: 1, watch: 2 } as const;
        const dueRank = (delivery: ProactiveDelivery): number =>
          delivery.dueAt ? Date.parse(delivery.dueAt) : Number.POSITIVE_INFINITY;
        const surfaced = new Set(
          deliveries
            .map((delivery, index) => ({ delivery, index }))
            .filter(({ delivery }) => delivery.surfaceNow !== false)
            .sort(
              (left, right) =>
                urgencyRank[left.delivery.urgency] - urgencyRank[right.delivery.urgency] ||
                dueRank(left.delivery) - dueRank(right.delivery) ||
                left.index - right.index,
            )
            .slice(0, 3)
            .map(({ index }) => index),
        );
        deliveries = deliveries.map((delivery, index) => ({
          ...delivery,
          surfaceNow: delivery.surfaceNow !== false && surfaced.has(index),
        }));
      }
      const decidedAt = this.#now();
      const urgent = deliveries.some((finding) => finding.surfaceNow !== false && finding.urgency === "now");
      if (work.kind === "personal_google_poll" && !nextGmailCursor) {
        throw new Error("The personal Google poll did not advance its Gmail cursor");
      }
      const retainedGoogleSourceIds = new Set([
        ...retainedFacts.flatMap((fact) => [...fact.sourceIds]),
        ...deliveries.flatMap((delivery) => [...delivery.sourceIds]),
      ]);
      const reviewedGoogleSources: ReviewedGoogleSourceDisposition[] = [...googleEvidence.keys()].map(
        (sourceId) => ({
          sourceId,
          disposition: retainedGoogleSourceIds.has(sourceId) ? "retained" : "dismissed",
        }),
      );
      await this.#store.completeGooglePoll({
        workId: work.workId,
        gmailCursor: nextGmailCursor ? googleGmailPollCursor(nextGmailCursor, work.connectionId) : null,
        calendarCursor: nextCalendarCursor,
        googleEvidence: [...googleEvidence.values()],
        reviewedGoogleSources,
        removedGoogleSourceIds: exactDistinct([...removedGmailSourceIds, ...removedCalendarSourceIds]),
        deliverNotBefore: proactiveDeliveryAt(decidedAt, work.household.timeZone, urgent).toISOString(),
        deliveries,
        facts: retainedFacts,
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
    try {
      const household = await this.#store.readHousehold({ householdId: invitation.householdId });
      const founder = household?.members.find(
        (member) => member.id === invitation.founderAdultId && member.kind === "adult",
      );
      if (!founder) throw new Error("The partner invitation founder is no longer in the household");
      const founderFirstName = profileString(founder.profile, "firstName") ?? founder.displayName;
      const createChatIdempotencyKey = await this.#store.scopeHouseholdLinqIdempotencyKey({
        householdId: invitation.householdId,
        idempotencyKey: `partner-invite-chat:${invitation.householdId}:${invitation.partnerAdultId}:${invitation.approvalSourceId}`,
      });
      const created = await this.#linq.createChat({
        idempotencyKey: createChatIdempotencyKey,
        senderPhoneNumber: this.#linqSenderPhoneNumber,
        participantPhoneNumbers: [invitation.partnerPhoneNumber],
        initialText: `Hi ${invitation.partnerFirstName} — I’m Florence. ${founderFirstName} asked me to help with family schedules and loose ends. Reply here when you’re ready, and I’ll send your private setup link.`,
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
      if (created.initialMessage.providerState === "failed") {
        throw new LinqError("provider_rejected", "Linq could not deliver the partner reply prompt", false);
      }
      const deliveryPending = created.initialMessage.providerState === "accepted";
      await this.#store.bindPartnerInvitationHandshake({
        householdId: invitation.householdId,
        actorAdultId: invitation.founderAdultId,
        adultId: invitation.partnerAdultId,
        approvalSourceId: invitation.approvalSourceId,
        messagesAddress: participant.phoneNumber,
        providerConversationId: created.providerConversationId,
        identitySubjectDigest: participant.identitySubjectDigest,
        providerMessageId: created.initialMessage.providerMessageId,
        occurredAt: created.initialMessage.occurredAt,
        retryAt: deliveryPending ? later(this.#now(), RETRY_MS) : null,
        retryError: deliveryPending ? "Linq has not confirmed sending the partner reply prompt" : null,
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
      created.authority.participants.length !== 2 ||
      !sameStrings(
        created.authority.participants.map((participant) => participant.phoneNumber).sort(),
        [...work.participantPhoneNumbers].sort(),
      )
    ) {
      throw new Error("Linq created a different family group");
    }
    requireConfirmedLinqMessage({
      providerState: created.initialMessage.providerState,
      pendingMessage: "Linq has not confirmed sending the family group introduction",
      failedMessage: "Linq could not deliver the family group introduction",
    });
    await this.#store.bindCreatedMessagesGroup({
      householdId: work.householdId,
      providerConversationId: created.providerConversationId,
      participants: created.authority.participants,
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
    const calendarReadyIdempotencyKey = await this.#store.scopeHouseholdLinqIdempotencyKey({
      householdId: household.id,
      idempotencyKey: `family-calendar-ready:${household.id}:${group.id}`,
    });
    const result = await this.#linq.sendMessage({
      idempotencyKey: calendarReadyIdempotencyKey,
      providerConversationId: group.providerConversationId,
      expectedAuthority: {
        audience: "group",
        participantIdentityDigests: group.participantIdentityDigests,
      },
      text: `I made the ${calendarLabel} calendar too. I’m checking both calendars and recent family email now, and I’ll be back with what’s on the docket.`,
    });
    if (result.status !== "committed") {
      throw new LinqError(
        result.status === "unknown" ? "provider_retryable" : "provider_rejected",
        result.detail ?? "Linq did not confirm the Family Calendar message",
        result.status === "unknown",
      );
    }
    requireConfirmedLinqMessage({
      providerState: result.providerState,
      pendingMessage: "Linq has not confirmed sending the Family Calendar announcement",
      failedMessage: "Linq could not deliver the Family Calendar announcement",
    });
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
            `Want me to text ${profileString(partner.profile, "firstName") ?? partner.displayName} at ${maskPhoneNumber(partnerPhone)} so they can set up their side?`,
          ]
        : [`Your side is ready, ${founderFirstName}.`];
    const completionSourceId = await this.#store.stageFounderHandoff({
      householdId: household.id,
      adultId: connection.ownerAdultId,
      channelId: channel.id,
      providerConversationId: channel.providerConversationId,
      texts,
      occurredAt: this.#now().toISOString(),
    });
    await this.#deliverOutbound(completionSourceId);
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

  #appendRequestedWebAccess(turn: InboundTurn, decision: FlorenceDecision): FlorenceDecision {
    const accessPath = decision.webAccessPath ?? null;
    const authorized =
      accessPath !== null &&
      turn.authority.audience === "private" &&
      turn.message.moveKind !== "reaction" &&
      Boolean(turn.message.authoredText?.trim());
    const accessUrl = authorized && accessPath ? this.#issueWebAccessUrl(turn, accessPath) : null;
    if (!accessUrl) return { ...decision, webAccessPath: null };
    const bubbles = decision.conversation.bubbles.map((bubble) => ({ ...bubble }));
    if (bubbles.length < 3) bubbles.push({ text: accessUrl, delayMs: 0 });
    else {
      const last = bubbles.at(-1);
      if (last) last.text = `${last.text}\n\n${accessUrl}`;
    }
    return {
      ...decision,
      conversation: { ...decision.conversation, bubbles },
      webAccessPath: null,
    };
  }

  #issueWebAccessUrl(turn: InboundTurn, accessPath: WebAccessPath): string | null {
    if (!this.#setupOrigin || turn.authority.audience !== "private" || turn.authority.stopped) return null;
    const identitySubjectDigest = turn.authority.expectedParticipantIdentityDigests[0];
    const adult = turn.household.members.find((member) => member.id === turn.authority.senderAdultId);
    if (
      !identitySubjectDigest ||
      turn.authority.expectedParticipantIdentityDigests.length !== 1 ||
      turn.authority.adultIds.length !== 1 ||
      turn.authority.adultIds[0] !== turn.authority.senderAdultId ||
      adult?.kind !== "adult" ||
      adult.status !== "verified"
    ) {
      return null;
    }
    const access = this.#enrollmentCodes.issueWebAccess({
      providerConversationId: turn.authority.providerConversationId,
      identitySubjectDigest,
      householdId: turn.authority.householdId,
      adultId: turn.authority.senderAdultId,
      accessPath,
      occurredAt: this.#now().toISOString(),
    });
    return `${this.#setupOrigin}${accessPath}#a=${encodeURIComponent(access.token)}`;
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
                  historyReviewReady: connection.grantedScopes.includes(
                    "https://www.googleapis.com/auth/calendar.events.readonly",
                  ),
                  assistantWorkReady: GOOGLE_WORKSPACE_ACTION_SCOPES.every((scope) =>
                    connection.grantedScopes.includes(scope),
                  ),
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
            if (fact.kind === "address" || fact.kind === "phone") return [];
            return [
              {
                id: fact.id,
                statement: factStatement(fact),
                visibility: fact.visibility,
                source: source ? vaultSource(source) : null,
                recordedAt: source?.occurredAt ?? null,
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
      age: child.age,
      grade: child.grade,
      school: child.school,
      activities: [...child.activities],
    })),
    postalCode: household.postalCode,
  };
}

function memberView(member: FamilyMemberRecord) {
  const firstName = profileString(member.profile, "firstName");
  const lastName = profileString(member.profile, "lastName");
  const age = member.kind === "child" ? profileChildAge(member.profile) : null;
  const grade = member.kind === "child" ? profileString(member.profile, "grade") : null;
  const candidate = {
    id: member.id,
    kind: member.kind,
    firstName: firstName ?? member.displayName,
    lastName: member.kind === "adult" ? lastName : (lastName ?? null),
    displayName: member.displayName,
    relationship: profileString(member.profile, "relationship") ?? defaultRelationship(member),
    ...(member.adultSlot === 1 ? { postalCode: profileString(member.profile, "postalCode") } : {}),
    ...(age !== null ? { age } : {}),
    ...(grade ? { grade } : {}),
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
    ...(member.age !== undefined ? { age: member.age } : {}),
    ...(member.grade !== undefined ? { grade: member.grade } : {}),
    ...(member.school ? { school: member.school } : {}),
    ...(member.activities ? { activities: member.activities } : {}),
  };
}

function memberPatch(member: PatchFamilyMemberInput) {
  const profile: JsonObject = {
    ...(member.age !== undefined ? { age: member.age } : {}),
    ...(member.grade !== undefined ? { grade: member.grade } : {}),
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
  return facts.flatMap((fact) => {
    const sources = fact.sources.length > 0 ? fact.sources : [null];
    return sources.map((source) => ({
      sourceId: source?.id ?? fact.id,
      recordId: fact.id,
      kind: "memory" as const,
      visibility: fact.visibility === "household" ? ("shared" as const) : ("adult_private" as const),
      label: fact.label,
      occurredAt: source?.occurredAt ?? fact.updatedAt,
      text: factStatement(fact),
    }));
  });
}

function enforcePolicy(decision: FlorenceDecision, announceRestrictions: boolean): FlorenceDecision {
  if (decision.policy.stopMessaging) {
    return {
      policy: decision.policy,
      conversation: { replyToCurrentMessage: false, reaction: null, bubbles: [] },
      facts: [],
      followUp: null,
      reminder: null,
      familyWork: null,
      docketCompletions: null,
      interest: null,
      calendar: null,
      householdUpdate: null,
      webAccessPath: null,
    };
  }
  const retain = decision.policy.retain;
  const schedule = decision.policy.schedule;
  const reminder = decision.reminder;
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
    reminder,
    familyWork: schedule ? decision.familyWork : null,
    docketCompletions: decision.docketCompletions,
    interest,
    calendar,
    householdUpdate: decision.householdUpdate,
    webAccessPath: decision.webAccessPath ?? null,
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
    text: researchUrls.length === 1 ? (researchUrls[0] ?? "") : `Links:\n${researchUrls.join("\n")}`,
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
    googleConnectionIdsUsed?: readonly string[];
    resolveCalendarEventTarget?: (eventRef: string) => CalendarEventTarget | null;
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
      decision.reminder !== null ||
      decision.familyWork !== null ||
      (decision.docketCompletions?.length ?? 0) > 0 ||
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
  const docketCompletions = decision.docketCompletions ?? [];
  if (
    new Set(docketCompletions).size !== docketCompletions.length ||
    docketCompletions.some(
      (candidateId) => !turn.householdDocket.items.some((candidate) => candidate.candidateId === candidateId),
    )
  ) {
    throw new FlorenceReasonerError(
      "invalid_output",
      "A conversation can complete only supplied household docket items",
    );
  }
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
  const reminderMutation: CommitTurnInput["reminderMutation"] =
    decision.reminder?.operation === "create"
      ? {
          operation: "create",
          reminderId: deterministicUuid(`reminder\0${turn.message.sourceId}`),
          action: decision.reminder.action,
          schedule: decision.reminder.schedule,
          visibility: turn.authority.audience === "group" ? "household" : "private",
          ownerAdultId: turn.authority.audience === "group" ? null : turn.authority.senderAdultId,
        }
      : decision.reminder?.operation === "update"
        ? {
            operation: "update",
            reminderId: decision.reminder.reminderId,
            action: decision.reminder.action,
            schedule: decision.reminder.schedule,
          }
        : decision.reminder && ["pause", "resume", "cancel", "run"].includes(decision.reminder.operation)
          ? {
              operation: decision.reminder.operation as "pause" | "resume" | "cancel" | "run",
              reminderId: decision.reminder.reminderId as string,
            }
          : null;
  const familyWorkMutation: CommitTurnInput["familyWorkMutation"] =
    decision.familyWork?.operation === "create"
      ? {
          operation: "create",
          workId: deterministicUuid(`family-work\0${turn.message.sourceId}`),
          objective: decision.familyWork.objective,
          visibility: turn.authority.audience === "group" ? "household" : "private",
          ownerAdultId: turn.authority.audience === "group" ? null : turn.authority.senderAdultId,
        }
      : decision.familyWork?.operation === "steer"
        ? {
            operation: "steer",
            workId: decision.familyWork.workId,
            instruction: decision.familyWork.instruction,
          }
        : decision.familyWork?.operation === "cancel"
          ? { operation: "cancel", workId: decision.familyWork.workId }
          : null;
  const calendar = calendarCommit(turn, decision, options.resolveCalendarEventTarget);
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
    googleConnectionIdsUsed: options.googleConnectionIdsUsed ?? [],
    facts,
    deleteFactIds,
    finiteMonitors,
    finiteMonitorUpdates,
    cancelMonitorIds: decision.followUp?.operation === "cancel" ? [decision.followUp.followUpId] : [],
    reminderMutation,
    familyWorkMutation,
    completeDocketCandidateIds: docketCompletions,
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
  resolveCalendarEventTarget?: (eventRef: string) => CalendarEventTarget | null,
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
  const requestedMutation = decision.calendar.mutation;
  const mutation: CalendarActionDraft["mutation"] =
    requestedMutation.operation === "create"
      ? requestedMutation
      : (() => {
          const target = resolveCalendarEventTarget?.(requestedMutation.target.eventRef) ?? null;
          if (
            !target ||
            JSON.stringify(target.observedEvent) !== JSON.stringify(requestedMutation.target.observedEvent)
          ) {
            throw new FlorenceReasonerError(
              "invalid_output",
              "A Calendar update or delete must resolve one exact app-scoped event reference",
            );
          }
          return requestedMutation.operation === "delete"
            ? { operation: "delete" as const, event: null, target }
            : { operation: "update" as const, event: requestedMutation.event, target };
        })();
  const action: CalendarActionDraft = {
    id,
    basisSourceId: turn.message.sourceId,
    mutation,
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

type InitialGooglePageClassification = {
  findings: readonly InitialGoogleScanFinding[];
  facts: readonly InitialGoogleScanFact[];
  classifiedSourceIds: readonly string[];
  dismissedSourceIds: readonly string[];
};

function newInitialPrivateGoogleScan(
  work: Extract<InitialIntelligenceWork, { kind: "initial_private_review" }>,
  anchoredAt: string,
  capturedCursor: GoogleGmailCursor,
): InitialPrivateGoogleScanV1 {
  return {
    kind: "initial_private_google_scan_v1",
    version: 1,
    scannerVersion: "complete_private_google_review_v1",
    connectionId: work.connectionId,
    anchoredAt,
    gmailAfter: new Date(Date.parse(anchoredAt) - 90 * 24 * 60 * 60_000).toISOString(),
    calendarTimeMin: new Date(Date.parse(anchoredAt) - 90 * 24 * 60 * 60_000).toISOString(),
    calendarTimeMax: new Date(Date.parse(anchoredAt) + 21 * 24 * 60 * 60_000).toISOString(),
    excludedFamilyCalendarId: work.familyCalendarId,
    phase: "calendar_targets",
    gmail: {
      capturedCursor: JSON.stringify(capturedCursor),
      baselinePageToken: null,
      baselineComplete: false,
      finalCursor: null,
      seenPageTokenDigests: [],
      seenMessageIdentities: [],
    },
    calendar: {
      enumerationPass: 1,
      finalBarrierStarted: false,
      targetPageToken: null,
      seenTargetPageTokenDigests: [],
      verificationTargetIds: [],
      targets: [],
      noEventCoverageTargets: [],
    },
    outcomes: { findings: [], facts: [] },
  };
}

function initialGoogleScanTarget(
  target: GoogleCalendarBaselineTarget,
  scan: InitialPrivateGoogleScanV1,
): InitialPrivateGoogleScanV1["calendar"]["targets"][number] {
  return {
    calendarId: target.calendarId,
    timeZone: target.timeZone,
    accessRole: target.accessRole,
    primary: target.primary,
    capturedCursor: JSON.stringify({
      kind: "calendar_updated_min_v1",
      calendarId: target.calendarId,
      updatedMin: new Date(Date.parse(scan.anchoredAt) - 5 * 60_000).toISOString(),
      windowTimeMin: scan.calendarTimeMin,
      windowTimeMax: scan.calendarTimeMax,
      overlapMs: 5 * 60_000,
    }),
    baselinePageToken: null,
    baselineComplete: false,
    replayComplete: false,
    finalCursor: null,
    manifestPageToken: null,
    manifestComplete: false,
    manifestProviderEventIds: [],
    seenManifestPageTokenDigests: [],
    seenPageTokenDigests: [],
    seenEventIdentities: [],
  };
}

function googleBaselineTarget(
  target: Pick<GoogleCalendarBaselineTarget, "calendarId" | "timeZone" | "accessRole" | "primary">,
): GoogleCalendarBaselineTarget {
  return {
    calendarId: target.calendarId,
    timeZone: target.timeZone,
    accessRole: target.accessRole,
    primary: target.primary,
  };
}

function beginCalendarVerificationPass(scan: InitialPrivateGoogleScanV1): InitialPrivateGoogleScanV1 {
  return {
    ...scan,
    phase: "calendar_verify",
    calendar: {
      ...scan.calendar,
      enumerationPass: scan.calendar.enumerationPass + 1,
      targetPageToken: null,
      seenTargetPageTokenDigests: [],
      verificationTargetIds: [],
    },
  };
}

function nextOpaquePageTokenState(
  seen: readonly string[],
  enteringToken: string | null,
  nextToken: string | null,
): string[] {
  const enteringDigest = digestOpaqueProviderState(enteringToken ?? "initial-page");
  const next = exactDistinct([...seen, enteringDigest]);
  if (nextToken !== null && next.includes(digestOpaqueProviderState(nextToken))) {
    throw new Error("Google returned a cyclic pagination sequence");
  }
  return next;
}

function exactCalendarReplayStarts(timeMin: string, timeMax: string, rollingAt: string): string[] {
  const start = Date.parse(timeMin);
  const end = Date.parse(timeMax);
  const windowMs = 21 * 24 * 60 * 60_000;
  const starts: string[] = [];
  for (let cursor = start; cursor < end; cursor += windowMs) {
    starts.push(new Date(Math.min(cursor, end - windowMs)).toISOString());
  }
  return exactDistinct([...starts, rollingAt]);
}

function calendarProviderEventIds(identities: readonly { key: string; digest: string }[]): string[] {
  return exactDistinct(
    identities.map(({ key }) => {
      const parsed: unknown = JSON.parse(key);
      if (
        !Array.isArray(parsed) ||
        parsed.length !== 2 ||
        typeof parsed[0] !== "string" ||
        typeof parsed[1] !== "string"
      ) {
        throw new Error("Stored Calendar provider identity is invalid");
      }
      return parsed[0];
    }),
  );
}

function privateGoogleModelBatches<T>(sources: readonly T[]): T[][] {
  const batches: T[][] = [];
  let batch: T[] = [];
  let serializedCharacters = 0;
  for (const source of sources) {
    const size = JSON.stringify(source).length;
    if (batch.length > 0 && (batch.length >= 10 || serializedCharacters + size > 80_000)) {
      batches.push(batch);
      batch = [];
      serializedCharacters = 0;
    }
    batch.push(source);
    serializedCharacters += size;
  }
  if (batch.length > 0) batches.push(batch);
  return batches;
}

function digestOpaqueProviderState(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function prepareInitialGmailPage(
  work: Extract<InitialIntelligenceWork, { kind: "initial_private_review" }>,
  scan: InitialPrivateGoogleScanV1,
  messages: readonly GmailEvidence[],
): {
  sources: FlorencePrivateGmailSource[];
  drafts: GoogleEvidenceDraft[];
  attachments: Map<string, GmailAttachmentReference>;
  identities: { key: string; digest: string }[];
} {
  const identities = new Map(
    scan.gmail.seenMessageIdentities.map((identity) => [identity.key, identity.digest]),
  );
  const sources: FlorencePrivateGmailSource[] = [];
  const drafts: GoogleEvidenceDraft[] = [];
  const attachments = new Map<string, GmailAttachmentReference>();
  for (const message of messages) {
    // A Gmail resource may legitimately acquire a newer history revision between the captured
    // baseline and replay. The same revision must stay byte-for-byte stable; a newer revision is
    // reclassified against the same durable source identity.
    const key = JSON.stringify([message.messageId, message.historyId]);
    const digest = digestOpaqueProviderState(JSON.stringify(message));
    const existing = identities.get(key);
    if (existing) {
      if (existing !== digest) throw new Error("Gmail reused a message identity with different content");
      continue;
    }
    identities.set(key, digest);
    const draft = draftGmailEvidence({
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
    drafts.push(draft);
    for (const attachment of message.attachments) {
      attachments.set(`${draft.id}\0${gmailAttachmentRefFor(draft.id, attachment)}`, attachment);
    }
    sources.push({
      sourceId: draft.id,
      kind: "gmail",
      visibility: "adult_private",
      sentAt: message.sentAt,
      sender: message.from,
      subject: message.subject === null ? null : modelSafeGmailText(message.subject),
      text: modelSafeGmailText(message.text),
      textStatus: message.textStatus,
      attachments: message.attachments.map((attachment) => ({
        attachmentRef: gmailAttachmentRefFor(draft.id, attachment),
        filename: attachment.filename,
        mimeType: attachment.mimeType,
        sizeBytes: attachment.sizeBytes,
      })),
      attachmentsStatus: message.attachmentsStatus,
    });
  }
  return {
    sources,
    drafts,
    attachments,
    identities: [...identities].map(([key, digest]) => ({ key, digest })),
  };
}

function prepareInitialCalendarPage(
  work: Extract<InitialIntelligenceWork, { kind: "initial_private_review" }>,
  _scan: InitialPrivateGoogleScanV1,
  target: InitialPrivateGoogleScanV1["calendar"]["targets"][number],
  events: readonly (GoogleCalendarWindowEvent | GoogleCalendarChange)[],
): {
  sources: FlorencePrivateCalendarEvent[];
  drafts: GoogleEvidenceDraft[];
  attachments: Map<string, GmailAttachmentReference>;
  identities: { key: string; digest: string }[];
} {
  const identities = new Map(target.seenEventIdentities.map((identity) => [identity.key, identity.digest]));
  const sources: FlorencePrivateCalendarEvent[] = [];
  const drafts: GoogleEvidenceDraft[] = [];
  for (const event of events) {
    const key = JSON.stringify([event.providerEventId, event.providerRevision]);
    const digest = digestOpaqueProviderState(JSON.stringify(event));
    const existing = identities.get(key);
    if (existing) {
      if (existing !== digest) throw new Error("Calendar reused an event revision with different content");
      continue;
    }
    identities.set(key, digest);
    const interval = calendarEvidenceInterval(event, work.household.timeZone);
    const draft = draftCalendarEvidence({
      householdId: work.household.householdId,
      ownerAdultId: work.adultId,
      connectionId: work.connectionId,
      calendarId: target.calendarId,
      providerEventId: event.providerEventId,
      providerRevision: event.providerRevision,
      providerUpdatedAt: event.providerUpdatedAt,
      status: event.status,
      busy: event.busy,
      title: event.title,
      ...interval,
    });
    drafts.push(draft);
    sources.push(privateCalendarEvidence(draft, event, "adult_private"));
  }
  return {
    sources,
    drafts,
    attachments: new Map(),
    identities: [...identities].map(([key, digest]) => ({ key, digest })),
  };
}

function mergeInitialGoogleScanOutcomes(
  scan: InitialPrivateGoogleScanV1,
  classified: InitialGooglePageClassification,
): InitialPrivateGoogleScanV1 {
  const reclassifiedSourceIds = new Set([
    ...classified.classifiedSourceIds,
    ...classified.dismissedSourceIds,
  ]);
  const findings = scan.outcomes.findings.filter(
    (finding) => !finding.sourceIds.some((sourceId) => reclassifiedSourceIds.has(sourceId)),
  );
  const findingKeys = new Set(
    findings.map((finding) => JSON.stringify([finding.privateSummary, [...finding.sourceIds].sort()])),
  );
  for (const finding of classified.findings) {
    const key = JSON.stringify([finding.privateSummary, [...finding.sourceIds].sort()]);
    if (!findingKeys.has(key)) {
      findingKeys.add(key);
      findings.push(finding);
    }
  }
  const facts = new Map<string, InitialGoogleScanFact>(
    scan.outcomes.facts.flatMap((fact) => {
      const retainedObservations = fact.sourceObservations.filter(
        ({ sourceId }) => !reclassifiedSourceIds.has(sourceId),
      );
      return retainedObservations.length > 0
        ? [
            [
              JSON.stringify([fact.slot, fact.statement]),
              initialGoogleFactWithObservations(fact, retainedObservations),
            ] as const,
          ]
        : [];
    }),
  );
  for (const fact of classified.facts) {
    const key = JSON.stringify([fact.slot, fact.statement]);
    const existing = facts.get(key);
    facts.set(
      key,
      initialGoogleFactWithObservations(fact, [
        ...fact.sourceObservations,
        ...(existing?.sourceObservations ?? []),
      ]),
    );
  }
  return { ...scan, outcomes: { findings, facts: [...facts.values()] } };
}

function initialGoogleFactWithObservations(
  fact: InitialGoogleScanFact,
  observations: readonly { sourceId: string; observedAt: string }[],
): InitialGoogleScanFact {
  const newestBySource = new Map<string, string>();
  for (const observation of observations) {
    const existing = newestBySource.get(observation.sourceId);
    if (!existing || Date.parse(observation.observedAt) > Date.parse(existing)) {
      newestBySource.set(observation.sourceId, observation.observedAt);
    }
  }
  const retained = [...newestBySource]
    .map(([sourceId, observedAt]) => ({ sourceId, observedAt }))
    .sort(
      (left, right) =>
        Date.parse(right.observedAt) - Date.parse(left.observedAt) ||
        left.sourceId.localeCompare(right.sourceId),
    )
    .slice(0, 10);
  const observedAt = retained[0]?.observedAt;
  if (!observedAt) throw new Error("An initial Google fact lost every current support");
  return {
    ...fact,
    sourceIds: retained.map(({ sourceId }) => sourceId).sort(),
    observedAt,
    sourceObservations: retained,
  };
}

function resolvedInitialGoogleFacts(facts: readonly InitialGoogleScanFact[]): InitialGoogleScanFact[] {
  const bySlot = new Map<string, InitialGoogleScanFact>();
  for (const fact of facts) {
    const existing = bySlot.get(fact.slot);
    if (
      !existing ||
      Date.parse(fact.observedAt) > Date.parse(existing.observedAt) ||
      (fact.observedAt === existing.observedAt && fact.statement.localeCompare(existing.statement) > 0)
    ) {
      bySlot.set(fact.slot, fact);
    }
  }
  return [...bySlot.values()].sort((left, right) => left.slot.localeCompare(right.slot));
}

function latestGoogleEvidenceTime(
  sourceIds: readonly string[],
  evidence: ReadonlyMap<string, GoogleEvidenceDraft>,
): string {
  const times = sourceIds.map((sourceId) => {
    const source = evidence.get(sourceId);
    if (!source) throw new Error("A Google classification cited evidence outside its batch");
    return source.kind === "gmail" ? source.sentAt : source.providerUpdatedAt;
  });
  return times.sort((left, right) => Date.parse(right) - Date.parse(left))[0] as string;
}

function googleEvidenceTime(sourceId: string, evidence: ReadonlyMap<string, GoogleEvidenceDraft>): string {
  const source = evidence.get(sourceId);
  if (!source) throw new Error("A Google classification cited evidence outside its batch");
  return source.kind === "gmail" ? source.sentAt : source.providerUpdatedAt;
}

function googleFindingActionAnchorDigest(
  sourceIds: readonly string[],
  modelAnchor: string | undefined,
  evidence: ReadonlyMap<string, GoogleEvidenceDraft>,
): string {
  const cited = sourceIds.map((sourceId) => {
    const source = evidence.get(sourceId);
    if (!source) throw new Error("A Google finding cited evidence outside its batch");
    return source;
  });
  if (cited.length === 1 && cited[0]?.kind === "calendar") {
    // A Calendar event has one material event lifecycle. Hash its complete provider title instead
    // of a model-selected substring so paraphrasing cannot manufacture a second action identity;
    // an actual title change still remains a material provider revision.
    return digestOpaqueProviderState(`calendar-event-title-v1\0${cited[0].title ?? "untitled"}`);
  }
  return digestOpaqueProviderState(requiredText(modelAnchor ?? null, "Google action anchor"));
}

/**
 * A complete scan can discover far more useful work than belongs in one arrival message. Pick a
 * small globally ranked arrival message while retaining every household-safe candidate on the
 * docket. Real monitors and Calendar proposals keep their own lifecycle; lower-ranked findings do
 * not become artificial timers. This runs after every page has been classified, so a late-page
 * deadline can outrank an early-page watch item.
 */
function prioritizeInitialGoogleFindings(
  findings: readonly InitialGoogleScanFinding[],
): InitialGoogleScanFinding[] {
  const urgencyRank = { now: 0, soon: 1, watch: 2 } as const;
  const eligible = findings
    .map((finding, index) => ({ finding, index }))
    .filter(({ finding }) => finding.surfaceNow)
    .sort((left, right) => {
      const urgency = urgencyRank[left.finding.urgency] - urgencyRank[right.finding.urgency];
      if (urgency !== 0) return urgency;
      const leftDue = left.finding.dueAt ? Date.parse(left.finding.dueAt) : Number.POSITIVE_INFINITY;
      const rightDue = right.finding.dueAt ? Date.parse(right.finding.dueAt) : Number.POSITIVE_INFINITY;
      return (
        leftDue - rightDue ||
        Date.parse(right.finding.observedAt) - Date.parse(left.finding.observedAt) ||
        left.index - right.index
      );
    });
  const selected = new Set<number>();
  const selectedSummaries = new Set<string>();
  for (const candidate of eligible) {
    if (selected.size >= 3 || selectedSummaries.has(candidate.finding.privateSummary)) continue;
    selected.add(candidate.index);
    selectedSummaries.add(candidate.finding.privateSummary);
  }
  return findings.map((finding, index) => ({ ...finding, surfaceNow: selected.has(index) }));
}

function googleGmailProviderCursor(value: string): GoogleGmailCursor {
  const parsed: unknown = JSON.parse(value);
  if (isRecord(parsed) && parsed.kind === "gmail_poll_cursor_v1") {
    return googleGmailProviderCursor(JSON.stringify(parsed.provider));
  }
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

function googleGmailCursor(value: string, connectionId: string): GoogleGmailCursor {
  const parsed: unknown = JSON.parse(value);
  if (
    !isRecord(parsed) ||
    parsed.kind !== "gmail_poll_cursor_v1" ||
    parsed.scannerVersion !== "complete_private_google_review_v1" ||
    parsed.connectionId !== connectionId
  ) {
    throw new Error("The stored Gmail account cursor is invalid");
  }
  return googleGmailProviderCursor(JSON.stringify(parsed.provider));
}

function googleGmailPollCursor(cursor: GoogleGmailCursor, connectionId: string): string {
  return JSON.stringify({
    kind: "gmail_poll_cursor_v1",
    scannerVersion: "complete_private_google_review_v1",
    connectionId,
    provider: cursor,
  });
}

type GoogleCalendarAccountCursorV1 = {
  kind: "calendar_account_cursor_v1";
  scannerVersion: "complete_private_google_review_v1";
  connectionId: string;
  enumeratedAt: string;
  targets: { target: GoogleCalendarBaselineTarget; provider: GoogleCalendarBoundedCursor }[];
  noEventCoverageTargets: readonly Omit<GoogleCalendarNoEventCoverageTarget, "label">[];
};

function googleCalendarAccountCursor(value: string, connectionId: string): GoogleCalendarAccountCursorV1 {
  const parsed: unknown = JSON.parse(value);
  if (
    !isRecord(parsed) ||
    parsed.kind !== "calendar_account_cursor_v1" ||
    parsed.scannerVersion !== "complete_private_google_review_v1" ||
    parsed.connectionId !== connectionId ||
    typeof parsed.enumeratedAt !== "string" ||
    !Number.isFinite(Date.parse(parsed.enumeratedAt)) ||
    !Array.isArray(parsed.targets)
  ) {
    throw new Error("The stored Calendar account cursor is invalid");
  }
  const targets = parsed.targets.map((value) => {
    if (!isRecord(value) || !isRecord(value.target) || !isRecord(value.provider)) {
      throw new Error("A stored Calendar account target is invalid");
    }
    const target = value.target;
    if (
      typeof target.calendarId !== "string" ||
      !target.calendarId ||
      typeof target.timeZone !== "string" ||
      !target.timeZone ||
      (target.accessRole !== "reader" &&
        target.accessRole !== "writerWithoutPrivateAccess" &&
        target.accessRole !== "writer" &&
        target.accessRole !== "owner") ||
      typeof target.primary !== "boolean"
    ) {
      throw new Error("A stored Calendar account target identity is invalid");
    }
    return {
      target: {
        calendarId: target.calendarId,
        timeZone: target.timeZone,
        accessRole: target.accessRole,
        primary: target.primary,
      } as GoogleCalendarBaselineTarget,
      provider: googleCalendarCursor(JSON.stringify(value.provider)),
    };
  });
  if (new Set(targets.map(({ target }) => target.calendarId)).size !== targets.length) {
    throw new Error("The stored Calendar account cursor repeated a target");
  }
  const rawNoEventCoverageTargets = parsed.noEventCoverageTargets ?? [];
  if (!Array.isArray(rawNoEventCoverageTargets)) {
    throw new Error("The stored no-event Calendar account targets are invalid");
  }
  const noEventCoverageTargets = rawNoEventCoverageTargets.map((value) => {
    if (
      !isRecord(value) ||
      typeof value.calendarId !== "string" ||
      !value.calendarId ||
      typeof value.timeZone !== "string" ||
      !value.timeZone ||
      value.accessRole !== "freeBusyReader" ||
      typeof value.primary !== "boolean"
    ) {
      throw new Error("A stored no-event Calendar account target identity is invalid");
    }
    return {
      calendarId: value.calendarId,
      timeZone: value.timeZone,
      accessRole: "freeBusyReader" as const,
      primary: value.primary,
    };
  });
  const allTargetIds = [
    ...targets.map(({ target }) => target.calendarId),
    ...noEventCoverageTargets.map((target) => target.calendarId),
  ];
  if (new Set(allTargetIds).size !== allTargetIds.length) {
    throw new Error("The stored Calendar account cursor repeated a coverage target");
  }
  return {
    kind: "calendar_account_cursor_v1",
    scannerVersion: "complete_private_google_review_v1",
    connectionId,
    enumeratedAt: parsed.enumeratedAt,
    targets,
    noEventCoverageTargets,
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
  if (turn.text?.trim()) return redactWebAccessToken(turn.text);
  if (turn.reaction) return `Reacted ${turn.reaction}`;
  return "Shared a family attachment.";
}

function redactWebAccessToken(value: string): string {
  return value.replace(/wa1\.[A-Za-z0-9_-]+/gu, "[secure web link]");
}

function modelSafeGmailText(value: string): string {
  return redactWebAccessToken(value)
    .replace(/https?:\/\/[^\s<>"']+/giu, "[link removed]")
    .replace(/\b(?:\d[\s-]?){6,8}\b/gu, "[code removed]")
    .replace(/\b[A-Za-z0-9][A-Za-z0-9._~+/=-]{23,}\b/gu, "[secret removed]")
    .replace(
      /\b((?:password|passcode|one[- ]time code|verification code|security code|otp)\b\s*(?:is|:|=|-)\s*)\S+/giu,
      "$1[secret removed]",
    );
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

function personalCalendarApprovalConfirmation(mutation: ApprovedCalendarAction["mutation"]): string {
  if (mutation.operation !== "create") {
    throw new Error("A personal Calendar owner approval can only add an event");
  }
  const event = mutation.event;
  if (event.intervalKind === "all_day") {
    return `Added “${event.title}” to the family calendar for ${formatAllDayCalendarInterval(event.startDate, event.endDate)}.`;
  }
  const format = new Intl.DateTimeFormat("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZone: event.timeZone,
    timeZoneName: "short",
  });
  return `Added “${event.title}” to the family calendar for ${format.format(new Date(event.startsAt))}.`;
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

function calendarChangeFallsInsideWindow(
  event: GoogleCalendarChange,
  timeMin: string,
  timeMax: string,
): boolean {
  if (event.status === "cancelled" || event.startsAt === null || event.endsAt === null) return false;
  return Date.parse(event.endsAt) > Date.parse(timeMin) && Date.parse(event.startsAt) < Date.parse(timeMax);
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

function requireConfirmedLinqMessage(input: {
  providerState: string;
  pendingMessage: string;
  failedMessage: string;
}): void {
  if (
    input.providerState === "sent" ||
    input.providerState === "delivered" ||
    input.providerState === "read"
  ) {
    return;
  }
  if (input.providerState === "failed") {
    throw new LinqError("provider_rejected", input.failedMessage, false);
  }
  throw new LinqError("provider_retryable", input.pendingMessage, true);
}

function privateGoogleFindingDetail(input: {
  fallback: string;
  sourceIds: readonly string[];
  calendarSources: readonly FlorencePrivateCalendarEvent[];
  timeZone: string;
}): string {
  const seen = new Set<string>();
  const calendarDetails = input.sourceIds.flatMap((sourceId) => {
    if (seen.has(sourceId)) return [];
    seen.add(sourceId);
    const event = input.calendarSources.find((candidate) => candidate.sourceId === sourceId);
    if (!event) return [];
    const interval = privateCalendarIntervalText(event, input.timeZone);
    if (!interval) return [];
    const title = event.title?.trim() ? `“${event.title.trim()}”` : "A private calendar commitment";
    if (event.status === "cancelled") return [`${title} was canceled for ${interval}.`];
    if (!event.busy) return [`${title} no longer blocks your calendar on ${interval}.`];
    if (event.status === "tentative") return [`${title} is tentatively on your calendar ${interval}.`];
    return [`${title} is on your calendar ${interval}.`];
  });
  return calendarDetails.length > 0 ? calendarDetails.join("\n") : input.fallback;
}

function gmailBackedFamilyCalendarProposal(
  proposal: FamilyCalendarReviewProposal | null,
  findingSourceIds: readonly string[],
  googleEvidence: ReadonlyMap<string, GoogleEvidenceDraft>,
): FamilyCalendarReviewProposal | null {
  if (!proposal) return null;
  return proposal.sourceIds.every((sourceId) => {
    const source = googleEvidence.get(sourceId);
    return findingSourceIds.includes(sourceId) && source?.kind === "gmail" && source.visibility === "private";
  })
    ? proposal
    : null;
}

function privateCalendarSafeBackgroundSharing<
  TConclusion extends { category: string; summary: string; dueAt: string | null },
>(input: {
  familyRelevance: string;
  conclusion: TConclusion | null;
  sourceIds: readonly string[];
  familyCalendar: FamilyCalendarReviewProposal | null;
  googleEvidence: ReadonlyMap<string, GoogleEvidenceDraft>;
  adultFirstName: string;
  timeZone: string;
}): { conclusion: TConclusion | null; familyCalendar: FamilyCalendarReviewProposal | null } {
  const familyCalendar = input.familyCalendar;
  const personalCalendarSourceId = familyCalendar?.sourceIds[0];
  const personalCalendarSource = personalCalendarSourceId
    ? input.googleEvidence.get(personalCalendarSourceId)
    : null;
  if (
    input.familyRelevance !== "adult_only" &&
    input.conclusion?.category === "family_date" &&
    familyCalendar !== null &&
    familyCalendar.sourceIds.length === 1 &&
    personalCalendarSourceId !== undefined &&
    input.sourceIds.includes(personalCalendarSourceId) &&
    personalCalendarSource?.kind === "calendar" &&
    personalCalendarSource.visibility === "private" &&
    personalCalendarSource.status === "confirmed" &&
    personalCalendarSource.title === familyCalendar.event.title &&
    familyCalendarProposalMatchesSource(familyCalendar, personalCalendarSource, input.timeZone)
  ) {
    return {
      conclusion: null,
      familyCalendar: {
        ...familyCalendar,
        disposition: "suggest",
        event:
          familyCalendar.event.intervalKind === "timed"
            ? { ...familyCalendar.event, timeZone: input.timeZone, location: null }
            : { ...familyCalendar.event, location: null },
      },
    };
  }
  const findingContainsPrivateCalendar = input.sourceIds.some((sourceId) => {
    const source = input.googleEvidence.get(sourceId);
    return source?.kind === "calendar" && source.visibility === "private";
  });
  return {
    conclusion: privateCalendarSafeHouseholdConclusion(input),
    familyCalendar: findingContainsPrivateCalendar
      ? null
      : gmailBackedFamilyCalendarProposal(input.familyCalendar, input.sourceIds, input.googleEvidence),
  };
}

function familyCalendarProposalMatchesSource(
  proposal: FamilyCalendarReviewProposal,
  source: CalendarEvidenceDraft,
  timeZone: string,
): boolean {
  if (!source.startsAt || !source.endsAt) return false;
  if (proposal.event.intervalKind === "timed") {
    return (
      source.allDay === false &&
      Date.parse(source.startsAt) === Date.parse(proposal.event.startsAt) &&
      Date.parse(source.endsAt) === Date.parse(proposal.event.endsAt)
    );
  }
  return (
    source.allDay === true &&
    source.startsAt === zonedCalendarDateStart(proposal.event.startDate, timeZone).toISOString() &&
    source.endsAt === zonedCalendarDateStart(proposal.event.endDate, timeZone).toISOString()
  );
}

function privateCalendarSafeHouseholdConclusion<
  T extends { category: string; summary: string; dueAt: string | null },
>(input: {
  conclusion: T | null;
  sourceIds: readonly string[];
  googleEvidence: ReadonlyMap<string, GoogleEvidenceDraft>;
  adultFirstName: string;
}): T | null {
  if (!input.conclusion) return null;
  const privateCalendarSources = input.sourceIds.flatMap((sourceId): CalendarEvidenceDraft[] => {
    const source = input.googleEvidence.get(sourceId);
    return source?.kind === "calendar" && source.visibility === "private" ? [source] : [];
  });
  if (privateCalendarSources.length === 0) return input.conclusion;
  if (
    input.conclusion.category !== "conflict" ||
    !privateCalendarSources.some((source) => source.status !== "cancelled" && source.busy)
  ) {
    return null;
  }
  return {
    ...input.conclusion,
    summary: `${input.adultFirstName} has a calendar conflict then.`,
    dueAt: earliestCalendarStart(privateCalendarSources),
  };
}

function earliestCalendarStart(sources: readonly CalendarEvidenceDraft[]): string | null {
  return (
    sources
      .filter((source) => source.status !== "cancelled" && source.busy && source.startsAt !== null)
      .map((source) => source.startsAt as string)
      .sort((left, right) => Date.parse(left) - Date.parse(right))[0] ?? null
  );
}

function privateInitialReviewBubbles(input: {
  suggested: readonly { text: string; delayMs: number }[];
  findings: readonly { privateSummary: string }[];
}): readonly { text: string; delayMs: number }[] {
  const attention = exactDistinct(input.findings.map((finding) => finding.privateSummary));
  if (attention.length === 0) {
    return [
      {
        text: "I finished reviewing the last 90 days of the Gmail and Calendar details I can access. I don’t have anything to flag right now.",
        delayMs: input.suggested[0]?.delayMs ?? 0,
      },
    ];
  }
  return renderInitialBriefingSections(
    [{ heading: "What needs attention:", items: attention }],
    null,
    input.suggested.map((bubble) => bubble.delayMs),
  );
}

function privateInitialReviewFindingSummary(input: {
  fallback: string;
  sourceIds: readonly string[];
  calendarSources: readonly FlorencePrivateCalendarEvent[];
  timeZone: string;
}): string {
  return privateGoogleFindingDetail(input).replaceAll(/\s*\n\s*/gu, " ");
}

function householdInitialBriefingBubbles(
  candidates: readonly SharedBriefingCandidate[],
  deferredCount = 0,
): readonly { text: string; delayMs: number }[] {
  if (candidates.length === 0) {
    return [
      {
        text: "I don’t have a household item to flag right now. I’ll keep watching.",
        delayMs: 0,
      },
    ];
  }
  const deferred =
    deferredCount > 0
      ? `I kept ${deferredCount} lower-priority ${deferredCount === 1 ? "item" : "items"} on the docket too. Ask me anytime.`
      : null;
  return renderInitialBriefingSections(
    [{ heading: "Here’s what’s on the docket:", items: candidates.map((candidate) => candidate.summary) }],
    [deferred, "Did I get that right? If I missed something, tell me here."].filter(Boolean).join("\n\n"),
    [],
  );
}

function renderInitialBriefingSections(
  sections: readonly { heading: string; items: readonly string[] }[],
  ending: string | null,
  suggestedDelays: readonly number[],
): readonly { text: string; delayMs: number }[] {
  const texts: string[] = [];
  for (const section of sections) {
    if (section.items.length === 0) continue;
    let text = section.heading;
    for (const item of section.items) {
      const line = `• ${item}`;
      if (`${section.heading}\n${line}`.length > 2_000) {
        throw new Error("An initial briefing item cannot fit in one iMessage bubble");
      }
      if (`${text}\n${line}`.length > 2_000) {
        texts.push(text);
        text = `${section.heading}\n${line}`;
      } else {
        text = `${text}\n${line}`;
      }
    }
    texts.push(text);
  }
  if (ending !== null) {
    const lastIndex = texts.length - 1;
    const last = texts[lastIndex];
    if (last === undefined) {
      texts.push(ending);
    } else if (`${last}\n\n${ending}`.length <= 2_000) {
      texts[lastIndex] = `${last}\n\n${ending}`;
    } else {
      texts.push(ending);
    }
  }
  if (texts.length < 1 || texts.length > 3) {
    throw new Error("The complete initial briefing cannot fit in one to three iMessage bubbles");
  }
  return texts.map((text, index) => ({ text, delayMs: suggestedDelays[index] ?? 0 }));
}

function exactDistinct(values: readonly string[]): string[] {
  return [...new Set(values)];
}

function privateCalendarIntervalText(event: FlorencePrivateCalendarEvent, timeZone: string): string | null {
  if (event.intervalKind === "all_day" && event.startDate && event.endDate) {
    return formatAllDayCalendarInterval(event.startDate, event.endDate);
  }
  if (!event.startsAt || !event.endsAt) return null;
  const startsAt = new Date(event.startsAt);
  const endsAt = new Date(event.endsAt);
  const date = new Intl.DateTimeFormat("en-US", {
    weekday: "long",
    month: "short",
    day: "numeric",
    timeZone,
  }).format(startsAt);
  const time = new Intl.DateTimeFormat("en-US", {
    hour: "numeric",
    minute: "2-digit",
    timeZone,
    timeZoneName: "short",
  }).formatRange(startsAt, endsAt);
  return `${date}, ${time}`;
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
  const age = profileChildAge(member.profile);
  const grade = profileString(member.profile, "grade");
  const school = profileString(member.profile, "school");
  const activities = profileStrings(member.profile, "activities");
  return {
    relationship,
    ...(firstName ? { firstName } : {}),
    ...(lastName ? { lastName } : {}),
    ...(age !== null ? { age } : {}),
    ...(grade ? { grade } : {}),
    ...(school ? { school } : {}),
    ...(activities ? { activities } : {}),
  };
}

function profileString(profile: JsonObject, key: string): string | null {
  const value = profile[key];
  return typeof value === "string" && value.trim() ? value : null;
}

function profileChildAge(profile: JsonObject): number | null {
  const value = profile.age;
  return typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= 120 ? value : null;
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

function gmailAttachmentRefFor(sourceId: string, attachment: GmailAttachmentReference): string {
  return `attachment_${sha256(
    `${sourceId}\0${attachment.attachmentId}\0${attachment.filename}\0${attachment.mimeType}\0${attachment.sizeBytes}`,
  ).slice(0, 32)}`;
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
