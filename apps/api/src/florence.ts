import { createHash } from "node:crypto";
import {
  decodeFactFileArtifacts,
  type EncryptedImageVault,
  type FileArtifactReference,
  ImageVaultError,
} from "@florence/artifacts";
import {
  type CompleteFamilyOnboardingInput,
  calendarMonthSchema,
  completeFamilyOnboardingInputSchema,
  type DeleteGoogleDerivedDataResponse,
  type DisconnectGoogleConnectionResponse,
  decodeMemoryPresentation,
  type FamilyCalendarMonthView,
  type FamilyMemberInput,
  type FamilyMemberMutationInput,
  familyCalendarMonthViewSchema,
  familyMemberMutationInputSchema,
  familyMemberProfileSchema,
  type MemoryPresentation,
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
  ClaimedFamilyWorkCapabilityIdentity,
  CommitTurnInput,
  CompleteFounderOnboardingInput,
  ConversationHistoryMessage,
  DueProactiveWork,
  FactDraft,
  FactRecord,
  FamilyCalendarMutation,
  FamilyCalendarReviewProposal,
  FamilyGroupCreationWork,
  FamilyMemberRecord,
  FamilyWorkSelectedFile,
  FamilyWorkSelectedImage,
  FamilyWorkStateV1,
  FiniteMonitorUpdate,
  GoogleEvidenceDraft,
  GoogleStableFactContext,
  HouseholdDocket,
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
  OutboundNativeMove,
  OutboundNativeMoveDraft,
  PostgresFlorenceStore,
  PreparedInboundContent,
  PrivateGoogleSourceReadResult,
  ProactiveDelivery,
  ReminderMutation,
  ReviewedGoogleSourceDisposition,
  SourceRecord,
  VisibleActiveFamilyWork,
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
  type LinqMessagePart,
  type LinqMessageStatusProposal,
  type LinqNativeMove,
  type LinqReaction,
  type LinqReactionProposal,
} from "@florence/linq";
import type { FlorenceBrowserClient, FlorenceBrowserOperation } from "./browser.js";
import { CapabilityAdapterError } from "./capability-lifecycle.js";
import type { EnrollmentCodes, WebAccessPath } from "./enrollment.js";
import { gmailFileAssetId, vaultFileArtifactId } from "./file-assets.js";
import type { FlorenceFlightsClient } from "./flights.js";
import type { FlorenceMapsClient } from "./maps.js";
import type { FlorencePublicPageClient } from "./public-page.js";
import {
  type FlorenceBoundedPrivateGoogleEvidence,
  type FlorenceCalendarCatalogRead,
  type FlorenceCalendarWindowRead,
  type FlorenceConversationalGmailSource,
  type FlorenceConversationHistoryMessage,
  type FlorenceDecision,
  type FlorenceFamilyCalendarWorkRequest,
  type FlorenceFamilyCalendarWorkResult,
  type FlorenceHouseholdAvailabilityRead,
  type FlorenceHouseholdNextActionInput,
  type FlorenceNarrowFamilyProfile,
  type FlorenceParticipantRequest,
  type FlorenceParticipantRequestResult,
  type FlorencePrivateCalendarEvent,
  type FlorencePrivateGmailSource,
  type FlorencePrivateGoogleBatchDecision,
  type FlorenceReadTools,
  type FlorenceReasoner,
  FlorenceReasonerError,
  type FlorenceReasonerInput,
  type FlorenceReminderWorkRequest,
  type FlorenceReminderWorkResult,
  type FlorenceSource,
  type FlorenceVaultWorkRequest,
  type FlorenceVaultWorkResult,
  type FlorenceVoiceNoteInput,
} from "./reasoner.js";
import type {
  FlorenceTelephonyClient,
  FlorenceTelephonyOperation,
  FlorenceTelephonyResult,
} from "./telephony.js";
import { VaultRecall } from "./vault-recall.js";
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
const GOOGLE_GMAIL_READ_SCOPES = [
  "https://www.googleapis.com/auth/gmail.modify",
  "https://www.googleapis.com/auth/gmail.readonly",
] as const;
const GOOGLE_CALENDAR_READ_SCOPES = [
  "https://www.googleapis.com/auth/calendar.events.readonly",
  "https://www.googleapis.com/auth/calendar.calendarlist",
] as const;
const PRIVATE_GOOGLE_CORPUS_VERSION = "retained_private_google_corpus_v2" as const;
const LOOP_IDLE_MS = 250;
const RETRY_MS = 15_000;
const LINQ_TYPING_REFRESH_MS = 60_000;
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

type InboundPresence = { stop: () => void };

export class Florence {
  readonly #store: PostgresFlorenceStore;
  readonly #linq: LinqClient;
  readonly #google: GoogleConnection | null;
  readonly #maps: FlorenceMapsClient | null;
  readonly #publicPages: FlorencePublicPageClient | null;
  readonly #weather: FlorenceWeatherClient | null;
  readonly #flights: FlorenceFlightsClient | null;
  readonly #browser: FlorenceBrowserClient | null;
  readonly #telephony: FlorenceTelephonyClient | null;
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
    browser?: FlorenceBrowserClient | null;
    telephony?: FlorenceTelephonyClient | null;
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
    this.#browser = input.browser ?? null;
    this.#telephony = input.telephony ?? null;
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
    const now = this.#now().toISOString();
    const [docket, activeWork] = household
      ? await Promise.all([
          workspaceDocket(this.#store, household.id, adultId, now),
          this.#store.readVisibleActiveFamilyWork({
            householdId: household.id,
            viewerAdultId: adultId,
            now,
          }),
        ])
      : [{ totalItems: 0, items: [] }, []];
    const activeCandidateIds = new Set(activeWork.flatMap((item) => [...item.candidateIds]));
    const docketItems = docket.items.filter((item) => !activeCandidateIds.has(item.candidateId));
    const visibleActiveWork = activeWork.map(({ candidateIds: _candidateIds, ...item }) => item);
    return workspaceViewSchema.parse(
      workspace(
        adultId,
        household,
        { totalItems: docketItems.length, items: docketItems },
        visibleActiveWork,
        this.#messagesUrl,
      ),
    );
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
    const files = household.facts
      .filter((fact) => fact.id === factId)
      .flatMap((fact) => [...decodeFactFileArtifacts(fact.value)]);
    await this.#store.deleteFact({ householdId: household.id, adultId, factId });
    if (this.#imageVault) {
      await Promise.allSettled(
        files.map((artifact) =>
          this.#imageVault?.deleteHouseholdFileArtifact({ householdId: household.id, artifact }),
        ),
      );
    }
    return this.workspaceForAdult(adultId);
  }

  async vaultFileForAdult(
    adultId: string,
    factId: string,
    artifactId: string,
  ): Promise<{ filename: string; mimeType: string; bytes: Uint8Array } | null> {
    if (!this.#imageVault) return null;
    const household = await this.#householdForAdult(adultId);
    const fact = household.facts.find((candidate) => candidate.id === factId);
    const artifact = fact
      ? decodeFactFileArtifacts(fact.value).find((candidate) => candidate.artifactId === artifactId)
      : null;
    if (!artifact) return null;
    try {
      const bytes = await this.#imageVault.readHouseholdFileArtifact({
        householdId: household.id,
        artifact,
      });
      return { filename: artifact.filename, mimeType: artifact.mimeType, bytes };
    } catch (error) {
      if (error instanceof ImageVaultError && error.code === "unauthorized_or_missing") return null;
      throw error;
    }
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
      | "partner-setup-link"
      | "partner-setup-refresh"
      | "partner-setup-reply";
    let pendingPartnerEnrollment: {
      operation: "issue" | "refresh";
      adultId: string;
      householdId: string;
      founderAdultId: string;
      messagesAddress: string;
      initialProviderMessageId: string;
      challengeDigest: string;
      expiresAt: string;
      linkBubbleIndex: number;
      refreshProviderEventId?: string;
    } | null = null;

    if (invitation) {
      if (invitation.state === "declined") return true;
      if (
        (invitation.state === "issued" ||
          invitation.state === "expired" ||
          invitation.refreshProviderEventId !== undefined) &&
        Date.parse(input.occurredAt) <= Date.parse(invitation.handshakeAt)
      ) {
        return true;
      }
      if (input.carrierOptOut) {
        await this.#store.declinePartnerInvitation({
          adultId: invitation.adultId,
          providerConversationId: input.providerConversationId,
          identitySubjectDigest: input.identitySubjectDigest,
          occurredAt: input.occurredAt,
        });
        return true;
      }
      if (
        invitation.state === "expired" ||
        (invitation.state === "awaiting_reply" && invitation.refreshProviderEventId !== undefined)
      ) {
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
            nextStep: "signed_link_will_follow",
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
          occurredAt:
            invitation.state === "awaiting_reply" ? (invitation.setupIssuedAt ?? checkedAt) : checkedAt,
        });
        const setupUrl = `${this.#setupOrigin}/#s=${encodeURIComponent(setup.token)}`;
        bubbles = [
          ...(conversation.requestsFreshLink
            ? [{ text: "Of course—here’s a fresh private setup link.", delayMs: 0 }]
            : conversation.bubbles),
          { text: setupUrl, delayMs: 0 },
        ];
        pendingPartnerEnrollment = {
          operation: "refresh",
          adultId: invitation.adultId,
          householdId: invitation.householdId,
          founderAdultId: invitation.founderAdultId,
          messagesAddress: invitation.messagesAddress,
          initialProviderMessageId: invitation.initialProviderMessageId,
          challengeDigest: this.#enrollmentCodes.digestPartnerSetup(setup.token),
          expiresAt: setup.expiresAt,
          linkBubbleIndex: bubbles.length - 1,
          refreshProviderEventId: invitation.refreshProviderEventId ?? input.providerEventId,
        };
        idempotencyPrefix = "partner-setup-refresh";
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
            operation: "issue",
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
      const enrollment = {
        householdId: pendingPartnerEnrollment.householdId,
        actorAdultId: pendingPartnerEnrollment.founderAdultId,
        adultId: pendingPartnerEnrollment.adultId,
        challengeDigest: pendingPartnerEnrollment.challengeDigest,
        providerConversationId: input.providerConversationId,
        identitySubjectDigest: input.identitySubjectDigest,
        messagesAddress: pendingPartnerEnrollment.messagesAddress,
        providerMessageId: pendingPartnerEnrollment.initialProviderMessageId,
        expiresAt: pendingPartnerEnrollment.expiresAt,
        issuedAt: pendingPartnerEnrollment.operation === "refresh" ? checkedAt : input.occurredAt,
        ...(pendingPartnerEnrollment.operation === "refresh"
          ? { refreshProviderEventId: pendingPartnerEnrollment.refreshProviderEventId }
          : {}),
      };
      if (pendingPartnerEnrollment.operation === "refresh") {
        await this.#store.refreshMessagesEnrollment(enrollment);
      } else {
        await this.#store.issueMessagesEnrollment(enrollment);
      }
    }

    void this.#setTyping({
      providerConversationId: input.providerConversationId,
      expectedAuthority,
      active: true,
    });
    try {
      for (const [index, bubble] of bubbles.entries()) {
        if (index > 0) await pause(Math.max(650, bubble.delayMs));
        const enrollmentBasis =
          pendingPartnerEnrollment?.operation === "refresh"
            ? `${pendingPartnerEnrollment.initialProviderMessageId}\0${pendingPartnerEnrollment.refreshProviderEventId}`
            : pendingPartnerEnrollment?.initialProviderMessageId;
        const idempotencyBasis = pendingPartnerEnrollment
          ? `${enrollmentBasis}\0${index === pendingPartnerEnrollment.linkBubbleIndex ? "link" : "ack"}`
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
      void this.#setTyping({
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
    let inboundPresence: InboundPresence | null = null;
    await this.#settleInboundAccepts();
    await this.#purgeExpiredArtifacts();
    const inbound = await this.#store.readNextInbound(this.#now().toISOString());
    if (inbound) {
      inboundPresence = await this.#handleInbound(inbound);
      worked = true;
    }
    try {
      await this.#settleInboundAccepts();
      const outbound = await this.#store.readNextOutbound(this.#now().toISOString());
      await this.#settleInboundAccepts();
      if (outbound) {
        await this.#deliverOutbound(outbound.sourceId);
        worked = true;
      }
    } finally {
      inboundPresence?.stop();
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

  async #handleInbound(turn: InboundTurn): Promise<InboundPresence | null> {
    const observed = await this.#linq.observeChat(turn.authority.providerConversationId);
    if (!sameAuthority(observed, turn.authority)) {
      await this.#store.commitTurn({
        sourceId: turn.message.sourceId,
        handledAt: this.#now().toISOString(),
      });
      return null;
    }
    const familyWorkReplyCandidate = await this.#store.readFamilyWorkReplyCandidate({
      sourceId: turn.message.sourceId,
      readAt: this.#now().toISOString(),
    });
    if (familyWorkReplyCandidate) {
      if (!this.#reasoner) {
        await this.#retryInbound(
          turn.message.sourceId,
          "Florence family-work reply reasoning is not configured",
        );
        return null;
      }
      let familyWorkReplyDecision: Awaited<ReturnType<FlorenceReasoner["interpretParticipantReply"]>>;
      try {
        familyWorkReplyDecision = await this.#reasoner.interpretParticipantReply({
          pendingRequest: {
            replyContext: familyWorkReplyCandidate.kind,
            targetAdultName: familyWorkReplyCandidate.targetAdultName,
            question: familyWorkReplyCandidate.question,
            askedAt: familyWorkReplyCandidate.askedAt,
            taskObjective: familyWorkReplyCandidate.taskObjective,
          },
          currentMessage: {
            text: turnText(turn.message),
            occurredAt: turn.message.occurredAt,
            explicitlyRepliesToQuestion: familyWorkReplyCandidate.explicitlyRepliesToQuestion,
          },
        });
      } catch (error) {
        await this.#retryInbound(turn.message.sourceId, errorText(error));
        return null;
      }
      if (familyWorkReplyDecision.belongsToRequest) {
        if (!familyWorkReplyDecision.acknowledgement) {
          await this.#retryInbound(
            turn.message.sourceId,
            "Florence returned no family-work reply acknowledgement",
          );
          return null;
        }
        const familyWorkReply = await this.#store.commitFamilyWorkReply({
          sourceId: turn.message.sourceId,
          kind: familyWorkReplyCandidate.kind,
          workId: familyWorkReplyCandidate.workId,
          generation: familyWorkReplyCandidate.generation,
          progressRevision: familyWorkReplyCandidate.progressRevision,
          questionSourceId: familyWorkReplyCandidate.questionSourceId,
          requestId: familyWorkReplyCandidate.requestId,
          acknowledgement: familyWorkReplyDecision.acknowledgement,
          handledAt: this.#now().toISOString(),
        });
        if (familyWorkReply !== "not_family_work") {
          if (familyWorkReply === "committed") this.#wake();
          return null;
        }
      }
    }
    const presence =
      turn.message.moveKind === "reaction"
        ? null
        : this.#beginInboundPresence({
            providerConversationId: turn.authority.providerConversationId,
            expectedAuthority: {
              audience: turn.authority.audience,
              participantIdentityDigests: turn.authority.expectedParticipantIdentityDigests,
            },
            observedAuthority: {
              providerConversationId: turn.authority.providerConversationId,
              authority: observed,
            },
          });
    try {
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
          return presence;
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
          return presence;
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
                nativeMoves: null,
              },
              facts: [],
              followUp: null,
              reminder: null,
              familyWork: null,
              docketUpsert: null,
              docketCompletions: null,
              calendar: null,
              householdUpdate: null,
            },
            this.#now(),
          ),
        );
        return presence;
      }
      await this.#respondEnrolled(turn);
      return presence;
    } catch (error) {
      presence?.stop();
      throw error;
    }
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
      const attachmentJob =
        turn.message.images.length > 0 ||
        (turn.currentDocuments?.length ?? 0) > 0 ||
        turn.supersededMessages.some((message) => message.images.length > 0) ||
        (turn.replyTarget?.images.length ?? 0) > 0 ||
        turn.replyTargetSupersededMessages.some((message) => message.images.length > 0);
      let substantiveWorkStarted = false;
      let turnCommitted = false;
      const retainedImageClaims: Array<{
        householdId: string;
        signalId: string;
        image: InboundTurn["message"]["images"][number];
      }> = [];
      const releaseUncommittedImageClaims = async () => {
        const claims = retainedImageClaims.splice(0).reverse();
        if (!this.#imageVault) return;
        await Promise.allSettled(
          claims.map((claim) =>
            this.#imageVault?.releaseRetention({
              ...claim,
              claimId: turn.message.sourceId,
            }),
          ),
        );
      };
      const startWork = () => {
        substantiveWorkStarted = true;
      };
      try {
        if (attachmentJob) startWork();
        controller.signal.throwIfAborted();
        const context = await this.#reasonerContext(turn);
        controller.signal.throwIfAborted();
        const decision = await this.#reasoner.decide(context.input, context.reads, controller.signal, {
          onWorkStarted: startWork,
        });
        controller.signal.throwIfAborted();
        const requested = this.#appendRequestedWebAccess(turn, enforcePolicy(decision));
        const approval = approvedCalendarOffer;
        const partnerApproval = requested.policy.stopMessaging ? null : approvedPartnerInvitation;
        const committedDecision =
          approval || partnerApproval
            ? {
                ...requested,
                policy: { ...requested.policy, schedule: true, stopMessaging: false },
                conversation: approvedActionConversation(approval, partnerApproval),
                docketUpsert: null,
                calendar: null,
                householdUpdate: null,
                webAccessPath: null,
              }
            : requested;
        const retainedMessageDocuments: Array<{
          documentId: string;
          contentDigest: string;
          contentEnvelope: Uint8Array;
        }> = [];
        const createsFamilyWork = committedDecision.familyWork?.operation === "create";
        if (committedDecision.docketUpsert || createsFamilyWork) {
          const replyEvidenceMessages =
            turn.replyTarget &&
            (createsFamilyWork ||
              committedDecision.docketUpsert?.sourceIds.includes(turn.replyTarget.sourceId))
              ? [...turn.replyTargetSupersededMessages, turn.replyTarget]
              : [];
          const evidenceMessages = [...turn.supersededMessages, turn.message, ...replyEvidenceMessages];
          const evidenceMessageIds = new Set(evidenceMessages.map((message) => message.sourceId));
          const currentDocuments = (turn.currentDocuments ?? []).filter((document) =>
            evidenceMessageIds.has(document.parentSourceId),
          );
          if (
            (evidenceMessages.some((message) => message.images.length > 0) || currentDocuments.length > 0) &&
            !this.#imageVault
          ) {
            throw new Error("Florence attachment retention is not configured");
          }
          if (this.#imageVault) {
            for (const message of evidenceMessages) {
              for (const image of message.images) {
                const retainInput = {
                  householdId: turn.household.id,
                  signalId: image.signalId ?? message.sourceId,
                  image,
                  now: this.#now(),
                };
                try {
                  await this.#imageVault.retain({
                    ...retainInput,
                    claimId: turn.message.sourceId,
                  });
                  retainedImageClaims.push({
                    householdId: retainInput.householdId,
                    signalId: retainInput.signalId,
                    image,
                  });
                } catch (error) {
                  if (
                    error instanceof ImageVaultError &&
                    (error.code === "expired" || error.code === "unauthorized_or_missing")
                  ) {
                    continue;
                  }
                  throw error;
                }
              }
            }
            for (const document of currentDocuments) {
              if (document.discardAfter === null) {
                continue;
              }
              const retained = this.#imageVault.retainPdf({
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
              retainedMessageDocuments.push({
                documentId: document.id,
                contentDigest: document.contentDigest,
                contentEnvelope: retained.contentEnvelope,
              });
            }
          }
        }
        const committed = await this.#store.commitTurn(
          decisionCommit(turn, committedDecision, this.#now(), {
            approveCalendarOffer: approval,
            approvePartnerInvitation: partnerApproval,
            googleEvidence: context.googleEvidence(),
            googleConnectionIdsUsed: context.googleConnectionIdsUsed(),
            retainedDocketDocuments: committedDecision.docketUpsert ? retainedMessageDocuments : [],
            retainedFamilyWorkDocuments: createsFamilyWork ? retainedMessageDocuments : [],
            resolveCalendarEventTarget: context.resolveCalendarEventTarget,
          }),
        );
        turnCommitted = committed === "committed";
        if (!turnCommitted) await releaseUncommittedImageClaims();
        if (committed === "committed" && committedDecision.familyWork?.operation === "cancel") {
          this.#activeFamilyWork
            .get(committedDecision.familyWork.workId)
            ?.controller.abort(new Error("The family task was cancelled"));
          const resources = await this.#store.takeCancelledFamilyWorkResources(
            committedDecision.familyWork.workId,
          );
          if (resources?.browserSession && this.#browser) {
            try {
              await this.#browser.close(resources.browserSession);
            } catch {
              // Browserbase still expires the bounded session if release is unavailable.
            }
          }
          if (resources?.activePhoneCall) {
            const stopped = await this.#stopFamilyWorkPhoneCall(
              committedDecision.familyWork.workId,
              resources.activePhoneCall,
            );
            if (stopped) {
              await this.#store.clearCancelledFamilyWorkPhoneCall(
                committedDecision.familyWork.workId,
                resources.activePhoneCall,
              );
            }
          }
        }
      } catch (error) {
        if (!turnCommitted) await releaseUncommittedImageClaims();
        if (controller.signal.aborted) return;
        if (error instanceof FlorenceReasonerError && !error.retryable) {
          if (approvedCalendarOffer || approvedPartnerInvitation) {
            await this.#store.commitTurn(
              decisionCommit(
                turn,
                {
                  policy: { retain: false, schedule: true, stopMessaging: false },
                  conversation: approvedActionConversation(approvedCalendarOffer, approvedPartnerInvitation),
                  facts: [],
                  followUp: null,
                  reminder: null,
                  familyWork: null,
                  docketUpsert: null,
                  docketCompletions: null,
                  calendar: null,
                  householdUpdate: null,
                },
                this.#now(),
                {
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
                        nativeMoves: null,
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
                      docketUpsert: null,
                      docketCompletions: null,
                      calendar: null,
                      householdUpdate: null,
                    },
                    this.#now(),
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
                          nativeMoves: null,
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
                        docketUpsert: null,
                        docketCompletions: null,
                        calendar: null,
                        householdUpdate: null,
                      },
                      this.#now(),
                    ),
            );
          }
          return;
        }
        await this.#retryInbound(turn.message.sourceId, errorText(error));
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
    const memorySourceCorpus = memorySources(turn.facts);
    const searchableMemorySources = representativeMemorySources(memorySourceCorpus);
    const vaultRecall = new VaultRecall(turn.facts);
    const visibleSources = selectVisibleMemorySources(searchableMemorySources, {
      primary: turnText(turn.message),
      context: [
        ...(turn.replyTarget ? [turnText(turn.replyTarget)] : []),
        ...turn.recentMessages.slice(-6).map(turnText),
      ],
    });
    const sourceIndex = new Map(memorySourceCorpus.map((source) => [source.sourceId, source]));
    const googleEvidence = new Map<string, GoogleEvidenceDraft>();
    const pendingGoogleEvidence = new Map<string, GoogleEvidenceDraft>();
    const gmailAttachmentIndex = new Map<string, GmailAttachmentReference>();
    const googleConnectionIdsUsed = new Set<string>();
    const calendarTargets = new Map<string, GooglePersonalCalendarCatalogTarget>();
    const calendarEventTargets = new Map<string, CalendarEventTarget>();
    const visibility = turn.authority.audience === "group" ? "shared" : "adult_private";
    const conversationHistoryObservedAt = this.#now().toISOString();
    const currentDocuments = turn.currentDocuments ?? [];
    const jobMessages = [
      ...turn.supersededMessages,
      turn.message,
      ...turn.replyTargetSupersededMessages,
      ...(turn.replyTarget ? [turn.replyTarget] : []),
    ];
    const candidateImages = jobMessages.flatMap((message) =>
      message.images.map((image) => ({
        ...image,
        sourceId: message.sourceId,
        signalId: image.signalId ?? message.sourceId,
      })),
    );
    const currentImageReads = new Map<
      string,
      { mimeType: "image/jpeg" | "image/png" | "image/webp"; bytes: Uint8Array }
    >();
    const currentImages = [] as typeof candidateImages;
    for (const image of candidateImages) {
      if (!this.#imageVault) {
        currentImages.push(image);
        continue;
      }
      try {
        const read = await this.#imageVault.read({
          householdId: turn.household.id,
          signalId: image.signalId,
          image,
        });
        currentImages.push(image);
        currentImageReads.set(image.assetId, read);
      } catch (error) {
        if (
          error instanceof ImageVaultError &&
          (error.code === "expired" || error.code === "unauthorized_or_missing")
        ) {
          continue;
        }
        throw error;
      }
    }
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
    for (const message of turn.replyTargetSupersededMessages) indexMessage(message);
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
      currentTime: this.#now().toISOString(),
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
      recentMessages: [
        ...turn.recentMessages,
        ...turn.supersededMessages,
        ...turn.replyTargetSupersededMessages,
      ]
        .sort((left, right) => left.occurredAt.localeCompare(right.occurredAt))
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
        candidateIds: [...work.candidateIds],
        currentProgress: work.currentProgress,
        schedule:
          work.schedule?.kind === "weekly"
            ? { ...work.schedule, weekdays: [...work.schedule.weekdays] }
            : work.schedule,
        paused: work.paused,
        status: work.status,
        nextAt: work.nextAt,
        lastRunAt: work.lastRunAt,
        lastResult: work.lastResult,
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
    const readExactFamilyCatalog = (calendarId: string) =>
      this.#readExactFamilyCalendarCatalog({
        householdId: turn.authority.householdId,
        calendarId,
        credentials: orderedFamilyCalendarCredentials,
      });
    const readExactFamilyWindow = async (input: {
      calendarId: string;
      timeMin: string;
      timeMax: string;
      limit: number;
      cursor: string | null;
    }): Promise<
      Readonly<{
        read: GooglePersonalCalendarWindowRead;
        credential: ActiveFamilyCalendarCredential;
      }>
    > =>
      this.#readExactFamilyCalendarWindow({
        householdId: turn.authority.householdId,
        credentials: orderedFamilyCalendarCredentials,
        ...input,
      });
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
    const indexConversationalGmailMessage = (
      message: GmailEvidence,
      connectionId: string,
    ): FlorenceConversationalGmailSource => {
      const prepared = conversationalGmailEvidence({
        householdId: turn.authority.householdId,
        ownerAdultId: turn.authority.senderAdultId,
        connectionId,
        message,
      });
      pendingGoogleEvidence.set(prepared.draft.id, prepared.draft);
      for (const [key, attachment] of prepared.attachments) {
        gmailAttachmentIndex.set(key, attachment);
      }
      return prepared.source;
    };

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
            readWorkspaceGmailSource: async (identity) => {
              const message = await this.#google?.readGmailMessage({
                householdId: turn.authority.householdId,
                ownerAdultId: turn.authority.senderAdultId,
                connectionId: activeGoogleConnection.connectionId,
                ...identity,
              });
              if (!message) throw new Error("Google Workspace Gmail reading is unavailable");
              return indexConversationalGmailMessage(message, activeGoogleConnection.connectionId);
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
      searchConversationHistory: async ({ query, after, before, cursor }) => {
        const page = await this.#store.searchConversationHistory({
          authority: turn.authority,
          currentSourceId: turn.message.sourceId,
          observedAt: conversationHistoryObservedAt,
          query,
          after,
          before,
          cursor,
        });
        const messages = page.messages.map((message) => modelConversationHistoryMessage(message));
        for (const message of messages) {
          sourceIndex.set(message.sourceId, conversationHistorySource(message));
        }
        return { ...page, messages };
      },
      readConversationHistory: async ({ anchor, cursor }) => {
        const page = await this.#store.readConversationHistory({
          authority: turn.authority,
          currentSourceId: turn.message.sourceId,
          observedAt: conversationHistoryObservedAt,
          anchor,
          cursor,
        });
        const messages = page.messages.map((message) => modelConversationHistoryMessage(message));
        for (const message of messages) {
          sourceIndex.set(message.sourceId, conversationHistorySource(message));
        }
        return { ...page, messages };
      },
      ...(turn.authority.audience === "private"
        ? {
            searchSources: async ({ query, cursor }: { query: string | null; cursor: string | null }) => {
              const page = await this.#store.searchPrivateGoogleSources({
                authority: turn.authority,
                currentSourceId: turn.message.sourceId,
                observedAt: conversationHistoryObservedAt,
                query,
                cursor,
              });
              return {
                results: page.sources.map((source) => ({
                  sourceId: source.sourceId,
                  kind: source.kind,
                  label: modelSafeGmailText(source.label),
                  occurredAt: source.occurredAt,
                  match: modelSafeGmailText(source.match),
                })),
                complete: page.complete,
                nextCursor: page.nextCursor,
              };
            },
          }
        : {}),
      searchVault: async (input) => vaultRecall.search(input),
      readVault: async (input) => vaultRecall.read(input),
      searchFamilyMemory: async ({ query, limit }) =>
        searchMemorySources(searchableMemorySources, query).slice(0, limit),
      ...(turn.authority.audience === "group"
        ? {
            readHouseholdAvailability: async (window: { timeMin: string; timeMax: string }) => {
              const household = await this.#store.readHousehold({
                householdId: turn.authority.householdId,
              });
              if (!household) throw new Error("The household is unavailable");
              return this.#readHouseholdAvailability({ household, ...window });
            },
          }
        : {}),
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
      readCalendarWindow: async ({ timeMin, timeMax, pageSize, cursor, scope, calendarRefs }) => {
        if (!this.#google || !activeGoogleConnection || !googleOwnerAdultId) {
          return {
            status: "unavailable",
            calendars: [],
            totalCalendarCount: 0,
            events: [],
            totalEventCount: 0,
            nextCursor: null,
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
                nextCursor: null,
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
                nextCursor: null,
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
                  limit: pageSize,
                  cursor,
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
                    limit: pageSize,
                    ...(cursor === null ? {} : { cursor }),
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
            nextCursor: read.nextCursor,
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
      readSource: async ({ sourceId }) => {
        const indexed = sourceIndex.get(sourceId);
        if (indexed) return indexed;
        if (turn.authority.audience !== "private") return null;
        const retained = await this.#store.readPrivateGoogleSource({
          authority: turn.authority,
          currentSourceId: turn.message.sourceId,
          observedAt: conversationHistoryObservedAt,
          sourceId,
        });
        const source = retainedPrivateGoogleSource(retained);
        sourceIndex.set(source.sourceId, source);
        if (retained.activeConnectionId) {
          googleConnectionIdsUsed.add(retained.activeConnectionId);
        }
        return source;
      },
      readCurrentImage: async ({ assetId, mimeType }) => {
        const image = currentImages.find(
          (candidate) => candidate.assetId === assetId && candidate.mimeType === mimeType,
        );
        if (!image) {
          throw new Error("The image is not attached to the current message");
        }
        const cached = currentImageReads.get(assetId);
        if (cached?.mimeType === mimeType) return cached;
        if (!this.#imageVault) throw new Error("Florence image reading is not configured");
        return this.#imageVault.read({
          householdId: turn.household.id,
          signalId: image.signalId,
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
        const sources = evidence.messages.map((message) =>
          indexConversationalGmailMessage(message, activeGoogleConnection.connectionId),
        );
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
      const preparedNativeMove = outbound.nativeMove
        ? await this.#prepareLinqNativeMove(outbound.nativeMove, outbound.householdId, outbound.sourceId)
        : null;
      const nativeMove =
        preparedNativeMove?.type === "message" && outbound.moveKind === "reply"
          ? {
              ...preparedNativeMove,
              replyTo: {
                providerMessageId: requiredText(outbound.replyToProviderMessageId, "Reply target message"),
              },
            }
          : preparedNativeMove;
      const result =
        nativeMove !== null
          ? await this.#linq.sendMove({
              idempotencyKey: outbound.idempotencyKey,
              providerConversationId: outbound.providerConversationId,
              expectedAuthority: outbound.expectedAuthority,
              move: nativeMove,
            })
          : outbound.moveKind === "reaction"
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
        await this.#store.retryOutbound({
          sourceId,
          retryAt: null,
          error: result.detail,
          occurredAt: this.#now().toISOString(),
        });
        return "failed";
      }
    } catch (error) {
      await this.#store.retryOutbound({
        sourceId,
        retryAt:
          retryTransient && error instanceof LinqError && error.retryable ? later(this.#now(), 5_000) : null,
        error: errorText(error),
        occurredAt: this.#now().toISOString(),
      });
      return retryTransient && error instanceof LinqError && error.retryable ? "claimed" : "failed";
    }
  }

  async #prepareLinqNativeMove(
    move: OutboundNativeMove,
    householdId: string,
    sourceId: string,
  ): Promise<LinqNativeMove> {
    if (move.type !== "message") return move;
    const parts: LinqMessagePart[] = [];
    let failedArtifactCount = 0;
    for (const part of move.parts) {
      if (part.type !== "media") {
        parts.push(part);
        continue;
      }
      if (part.source.type === "url") {
        parts.push({ type: "media", source: part.source });
        continue;
      }
      let providerAttachmentId = part.source.providerAttachmentId;
      if (!providerAttachmentId) {
        try {
          if (!this.#imageVault) {
            throw new Error("Florence attachment delivery is not configured");
          }
          if (part.source.type === "florence_file_artifact") {
            const bytes = await this.#imageVault.readFileArtifact({
              householdId,
              workId: part.source.workId,
              artifact: {
                artifactId: part.source.assetId,
                workId: part.source.workId,
                filename: part.source.filename,
                mimeType: part.source.mimeType,
                byteLength: part.source.byteLength,
                sha256: part.source.sha256,
              },
            });
            providerAttachmentId = await this.#linq.uploadAttachment({
              filename: part.source.filename,
              mimeType: part.source.mimeType,
              bytes,
            });
          } else {
            if (part.source.mimeType === "image/webp") {
              throw new Error("Messages does not accept the captured image format");
            }
            const image = await this.#imageVault.read({
              householdId,
              signalId: part.source.signalId,
              image: { assetId: part.source.assetId, mimeType: part.source.mimeType },
            });
            if (image.mimeType !== part.source.mimeType) {
              throw new Error("The selected browser image changed before delivery");
            }
            providerAttachmentId = await this.#linq.uploadAttachment({
              filename: part.source.filename,
              mimeType: image.mimeType,
              bytes: image.bytes,
            });
          }
        } catch {
          failedArtifactCount += 1;
          continue;
        }
        await this.#store.checkpointOutboundArtifactAttachment({
          sourceId,
          assetId: part.source.assetId,
          providerAttachmentId,
        });
      }
      parts.push({
        type: "media",
        source: { type: "attachment", providerAttachmentId },
      });
    }
    if (failedArtifactCount > 0) {
      const note =
        failedArtifactCount === 1
          ? "I found an attachment too, but it wouldn’t send just now."
          : "I found a few attachments too, but they wouldn’t send just now.";
      const textIndex = parts.findIndex((part) => part.type === "text");
      const textPart = textIndex >= 0 ? parts[textIndex] : undefined;
      if (textPart?.type === "text") {
        parts[textIndex] = { ...textPart, text: `${textPart.text}\n\n${note}` };
      } else {
        parts.unshift({ type: "text", text: note });
      }
    }
    return { type: "message", parts };
  }

  async #tryRetryCue(sourceId: string): Promise<boolean> {
    try {
      const cueSourceId = await this.#store.stageRetryCue({
        sourceId,
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
    await this.#tryRetryCue(sourceId);
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
        const currentTime = this.#now().toISOString();
        const familyCalendar = await this.#google.readInitialCalendarReview({
          householdId: work.household.householdId,
          ownerAdultId: work.familyCalendarOwnerAdultId,
          connectionId: work.familyCalendarOwnerConnectionId,
          calendarId: work.familyCalendarId,
          currentTime,
          limit: 50,
        });
        if (familyCalendar.status !== "complete" || !familyCalendar.cursor) {
          throw new Error("The Family Calendar baseline is temporarily unavailable");
        }
        const household = await this.#store.readHousehold({ householdId: work.household.householdId });
        if (!household) throw new Error("The initial briefing household is unavailable");
        const decision = await this.#reasoner.synthesizeHouseholdBriefing({
          currentTime,
          familyProfile: initialFamilyProfile(work.household),
          memory: proactiveMemoryContext(household.facts, {
            primary: [
              ...work.candidates.map((candidate) => candidate.summary),
              ...familyCalendar.events.flatMap((event) => (event.title ? [event.title] : [])),
            ].join("\n"),
            context: [],
          }),
          familyCalendar: familyCalendar.events.map((event) =>
            event.intervalKind === "timed"
              ? {
                  intervalKind: event.intervalKind,
                  title: event.title,
                  startsAt: event.startsAt,
                  endsAt: event.endsAt,
                  timeZone: event.timeZone,
                }
              : {
                  intervalKind: event.intervalKind,
                  title: event.title,
                  startDate: event.startDate,
                  endDate: event.endDate,
                },
          ),
          candidates: [...work.candidates],
        });
        await this.#store.completeHouseholdInitialBriefing({
          workId: work.workId,
          selectedCandidateIds: decision.selectedCandidateIds,
          familyCalendarCursor: JSON.stringify(familyCalendar.cursor),
          bubbles: decision.bubbles,
          nextJob: decision.nextJob,
          executionAdultId: work.familyCalendarOwnerAdultId,
          occurredAt: currentTime,
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
          .filter(calendarChangeRepresentsRemoval)
          .map((event) =>
            calendarEvidenceSourceId(
              work.household.householdId,
              work.connectionId,
              target.calendarId,
              event.providerEventId,
            ),
          ),
      );
      const activeReplayValues = replayValues.filter((event) => !calendarChangeRepresentsRemoval(event));
      const prepared = prepareInitialCalendarPage(
        work,
        scan,
        target,
        activeReplayValues.filter((event) =>
          calendarChangeFallsInsideWindow(event, scan.calendarTimeMin, scan.calendarTimeMax),
        ),
      );
      const outsideWindow = prepareInitialCalendarPage(
        work,
        scan,
        target,
        activeReplayValues.filter(
          (event) => !calendarChangeFallsInsideWindow(event, scan.calendarTimeMin, scan.calendarTimeMax),
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
        dismissedSourceIds: exactDistinct([
          ...classified.dismissedSourceIds,
          ...outsideWindow.drafts.map((draft) => draft.id),
          ...removedSourceIds,
        ]),
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
        [...prepared.drafts, ...outsideWindow.drafts],
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
        scannerVersion: PRIVATE_GOOGLE_CORPUS_VERSION,
        connectionId: scan.connectionId,
        provider: googleGmailProviderCursor(scan.gmail.finalCursor),
      }),
      calendarCursor: JSON.stringify({
        kind: "calendar_account_cursor_v1",
        scannerVersion: PRIVATE_GOOGLE_CORPUS_VERSION,
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
        privateDocket: finding.privateDocket,
        ...(finding.actionAnchorDigest ? { actionAnchorDigest: finding.actionAnchorDigest } : {}),
        sourceIds: finding.sourceIds,
        familyRelevance: finding.familyRelevance,
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
          currentFacts: relevantGoogleFacts(
            [
              ...work.currentFacts,
              ...resolvedInitialGoogleFacts(scan.outcomes.facts).map(({ slot, statement, memory }) => ({
                slot,
                statement,
                memory,
              })),
            ],
            privateGoogleMemoryQuery(batch),
          ),
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
          privateDocket: finding.privateDocket,
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
      decision.facts.flatMap((fact): InitialGoogleScanFact[] => {
        if (fact.familyRelevance === "owner_private") return [];
        return [
          {
            ...fact,
            familyRelevance: fact.familyRelevance as InitialGoogleScanFact["familyRelevance"],
            observedAt: latestGoogleEvidenceTime(fact.sourceIds, evidence),
            sourceObservations: fact.sourceIds.map((sourceId) => ({
              sourceId,
              observedAt: googleEvidenceTime(sourceId, evidence),
            })),
          },
        ];
      }),
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
      void existing.promise
        .then(async () => {
          const state = await this.#store.readClaimedFamilyWorkState({
            workId: work.workId,
            generation: work.generation,
            claimId: work.claimId,
          });
          if (state) this.#launchFamilyWork({ ...work, state });
          else this.#wake();
        })
        .catch(() => this.#wake());
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

  async #stopFamilyWorkPhoneCall(
    workId: string,
    activePhoneCall: NonNullable<FamilyWorkStateV1["activePhoneCall"]>,
  ): Promise<boolean> {
    const telephony = this.#telephony;
    if (!telephony || !activePhoneCall) return false;
    const operation: FlorenceTelephonyOperation =
      activePhoneCall.kind === "agent"
        ? {
            kind: "ai_call_cancel",
            provider: "bland",
            providerCallId: activePhoneCall.providerCallId,
          }
        : {
            kind: "call_cancel",
            provider: "twilio",
            callSid: activePhoneCall.providerCallId,
          };
    try {
      const cancellation = await telephony.run(
        {
          workId,
          callId: `cancel-${activePhoneCall.providerCallId}`.slice(0, 500),
          attempt: 1,
          operation,
        },
        new AbortController().signal,
      );
      if (cancellation.kind === "completed" || cancellation.kind === "failed") return true;
      const statusOperation: FlorenceTelephonyOperation =
        activePhoneCall.kind === "agent"
          ? {
              kind: "ai_call_status",
              provider: "bland",
              providerCallId: activePhoneCall.providerCallId,
            }
          : {
              kind: "call_status",
              provider: "twilio",
              callSid: activePhoneCall.providerCallId,
            };
      const status = await telephony.run(
        {
          workId,
          callId: `status-${activePhoneCall.providerCallId}`.slice(0, 500),
          attempt: 1,
          operation: statusOperation,
        },
        new AbortController().signal,
      );
      return status.kind === "completed" || status.kind === "failed";
    } catch {
      return false;
    }
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
    const familyWorkExecutionAdultId = work.ownerAdultId ?? work.initiatingAdultId;
    const familyWorkGoogleAdultId = familyWorkExecutionAdultId;
    if (work.visibility === "private" && !familyWorkExecutionAdultId) {
      throw new Error("Private family work lost its adult owner");
    }
    const browser = this.#browser && familyWorkExecutionAdultId ? this.#browser : null;
    const familyWorkLinkedMessages = work.linkedSources.flatMap((source) =>
      source.kind === "message" ? [source] : [],
    );
    const familyWorkLinkedDocuments = work.linkedSources.flatMap((source) =>
      source.kind === "document" ? [source.document] : [],
    );
    const familyWorkOriginImages = [
      ...(work.origin.replyTarget ? [work.origin.replyTarget] : []),
      ...work.origin.supersededMessages,
      work.origin.message,
      ...familyWorkLinkedMessages.map((source) => source.message),
    ].flatMap((message) =>
      message.images.map((image) => ({
        ...image,
        sourceId: message.sourceId,
        signalId: image.signalId ?? message.sourceId,
      })),
    );
    const familyWorkOriginDocuments = [...work.origin.currentDocuments, ...familyWorkLinkedDocuments];
    const claimedBrowserSession = work.state.browserSession;
    let browserSession = claimedBrowserSession;
    let uncheckpointedPhoneCall: FamilyWorkStateV1["activePhoneCall"] = null;
    let uncheckpointedTextMessage: FamilyWorkStateV1["activeTextMessage"] = null;
    const closeBrowserSession = async (session: NonNullable<typeof browserSession>): Promise<void> => {
      if (!browser) return;
      try {
        await browser.close(session);
      } catch {
        // Browserbase sessions have a bounded expiry when release is unavailable.
      }
    };
    const closeUncheckpointedBrowserSession = async (): Promise<void> => {
      if (browserSession && browserSession.sessionId !== claimedBrowserSession?.sessionId) {
        await closeBrowserSession(browserSession);
      }
    };
    const stopUncheckpointedPhoneCall = async (retainForCancellation: boolean): Promise<void> => {
      const activePhoneCall = uncheckpointedPhoneCall;
      if (!activePhoneCall) return;
      if (retainForCancellation) {
        const retained = await this.#store.retainCancelledFamilyWorkPhoneCall(
          work.workId,
          activePhoneCall,
          this.#now().toISOString(),
        );
        if (!retained) return;
      }
      const stopped = await this.#stopFamilyWorkPhoneCall(work.workId, activePhoneCall);
      if (stopped && retainForCancellation) {
        await this.#store.clearCancelledFamilyWorkPhoneCall(work.workId, activePhoneCall);
      }
      if (stopped) uncheckpointedPhoneCall = null;
    };
    const adoptOrStopUncheckpointedPhoneCall = async (): Promise<void> => {
      const activePhoneCall = uncheckpointedPhoneCall;
      if (!activePhoneCall) return;
      try {
        const adopted = await this.#store.adoptFamilyWorkPhoneCall(
          work.workId,
          activePhoneCall,
          this.#now().toISOString(),
        );
        if (adopted) {
          uncheckpointedPhoneCall = null;
          return;
        }
      } catch {
        // If the latest task state cannot adopt the provider ID, stop the call below.
      }
      await stopUncheckpointedPhoneCall(false);
    };
    const adoptUncheckpointedTextMessage = async (): Promise<void> => {
      const activeTextMessage = uncheckpointedTextMessage;
      if (!activeTextMessage) return;
      try {
        const adopted = await this.#store.adoptFamilyWorkTextMessage(work.workId, activeTextMessage);
        if (adopted) uncheckpointedTextMessage = null;
      } catch {
        // Twilio does not expose a supported cancel operation for an immediate SMS.
        // Keep the durable task retryable; its exact create request will reconcile by provider log.
      }
    };
    const latestClaimedFamilyWorkState = async (): Promise<FamilyWorkStateV1> => {
      try {
        return (
          (await this.#store.readClaimedFamilyWorkState({
            workId: work.workId,
            generation: work.generation,
            claimId: work.claimId,
          })) ?? work.state
        );
      } catch {
        return work.state;
      }
    };
    try {
      const maps = this.#maps;
      const publicPages = this.#publicPages;
      const weather = this.#weather;
      const flights = this.#flights;
      const telephony = this.#telephony;
      const google = this.#google;
      const familyWorkHousehold = await this.#store.readHousehold({
        householdId: work.household.householdId,
        ...(work.visibility === "private" && familyWorkExecutionAdultId
          ? { viewerAdultId: familyWorkExecutionAdultId }
          : {}),
      });
      if (!familyWorkHousehold) throw new Error("The family task household is unavailable");
      const familyWorkFacts =
        work.visibility === "household"
          ? familyWorkHousehold.facts.filter((fact) => fact.visibility === "household")
          : familyWorkHousehold.facts;
      const familyWorkMemoryCorpus = memorySources(familyWorkFacts);
      const familyWorkSearchableMemory = representativeMemorySources(familyWorkMemoryCorpus);
      const familyWorkVaultRecall = new VaultRecall(familyWorkFacts);
      const familyWorkVisibleSources = selectVisibleMemorySources(familyWorkSearchableMemory, {
        primary: work.objective,
        context: [
          turnText(work.origin.message),
          ...(work.origin.replyTarget ? [turnText(work.origin.replyTarget)] : []),
          ...work.origin.supersededMessages.slice(-6).map(turnText),
        ],
      });
      const familyWorkSourceIndex = new Map(
        familyWorkMemoryCorpus.map((source) => [source.sourceId, source] as const),
      );
      const familyWorkLinkedGoogleSourceIds = new Set(
        work.linkedSources.flatMap((source) =>
          source.kind === "gmail" || source.kind === "calendar" ? [source.sourceId] : [],
        ),
      );
      for (const source of work.linkedSources) {
        if (source.kind === "message") {
          familyWorkSourceIndex.set(source.sourceId, {
            sourceId: source.sourceId,
            recordId: null,
            kind: "message",
            visibility: source.visibility,
            label:
              source.message.speaker === "florence"
                ? "Florence"
                : (familyWorkHousehold.members.find((member) => member.id === source.message.speaker)
                    ?.displayName ?? "Family message"),
            occurredAt: source.message.occurredAt,
            text: turnText(source.message),
          });
        } else if (source.kind === "document") {
          familyWorkSourceIndex.set(source.sourceId, {
            sourceId: source.sourceId,
            recordId: null,
            kind: "document",
            visibility: source.visibility,
            label: source.document.filename,
            occurredAt: null,
            text: `Attached PDF: ${source.document.filename}. Its exact contents are attached to this task.`,
          });
        }
      }
      const familyWorkConversationHistoryObservedAt = this.#now().toISOString();
      const familyWorkGoogleConnections =
        google && familyWorkGoogleAdultId
          ? await google.status({
              householdId: work.household.householdId,
              ownerAdultId: familyWorkGoogleAdultId,
            })
          : [];
      const familyWorkFamilyCalendarCredentials =
        google && familyWorkHousehold.familyCalendarId
          ? await this.#store.readActiveFamilyCalendarCredentials({
              householdId: work.household.householdId,
            })
          : [];
      const familyWorkCalendarConnection =
        work.visibility === "private"
          ? familyWorkGoogleConnections.find((connection) => connection.status === "active")
          : undefined;
      const familyWorkWorkspaceConnection = familyWorkGoogleConnections.find(
        (connection) =>
          connection.status === "active" &&
          GOOGLE_WORKSPACE_ACTION_SCOPES.every((scope) => connection.grantedScopes.includes(scope)),
      );
      const familyWorkGmailConnection = familyWorkGoogleConnections.find(
        (connection) =>
          connection.status === "active" &&
          GOOGLE_GMAIL_READ_SCOPES.some((scope) => connection.grantedScopes.includes(scope)),
      );
      const familyWorkGoogleModelConnections =
        work.visibility === "household"
          ? familyWorkFamilyCalendarCredentials.length > 0 && familyWorkHousehold.familyCalendarId
            ? [
                {
                  emailLabel:
                    familyWorkHousehold.familyCalendarLabel ?? familyWorkHousehold.name ?? "Family Calendar",
                  calendarAvailable: true,
                  kind: "family" as const,
                  writesEnabled: true,
                },
              ]
            : []
          : familyWorkCalendarConnection
            ? [
                {
                  emailLabel: familyWorkCalendarConnection.emailLabel ?? "Connected Google account",
                  calendarAvailable: true,
                  kind: "personal" as const,
                  writesEnabled: familyWorkWorkspaceConnection !== undefined,
                },
              ]
            : [];
      const familyWorkGmailAttachmentIndex = new Map<
        string,
        Readonly<{
          connectionId: string;
          sourceId: string;
          fileAssetId: string;
          attachment: GmailAttachmentReference;
        }>
      >();
      const indexFamilyWorkGmailMessage = (
        message: GmailEvidence,
        connectionId: string,
      ): FlorenceConversationalGmailSource => {
        if (!familyWorkExecutionAdultId) {
          throw new Error("Durable Gmail reading lost its initiating parent");
        }
        const prepared = conversationalGmailEvidence({
          householdId: work.household.householdId,
          ownerAdultId: familyWorkExecutionAdultId,
          connectionId,
          message,
        });
        for (const [key, attachment] of prepared.attachments) {
          familyWorkGmailAttachmentIndex.set(key, {
            connectionId,
            sourceId: prepared.source.sourceId,
            fileAssetId: gmailFileAssetId(
              prepared.source.sourceId,
              key.slice(prepared.source.sourceId.length + 1),
            ),
            attachment,
          });
        }
        return prepared.source;
      };
      const familyWorkCalendarTargets = new Map<string, CalendarEventTarget>();
      const familyWorkCalendarRef = (calendarId: string): string => {
        const connectionIdentity =
          calendarId === familyWorkHousehold.familyCalendarId
            ? "family"
            : familyWorkCalendarConnection?.connectionId;
        if (!connectionIdentity) throw new Error("Google Calendar is unavailable");
        return `calendar_${sha256(`${work.workId}\0${connectionIdentity}\0${calendarId}`).slice(0, 32)}`;
      };
      const familyWorkCalendarEventRef = (
        event: GooglePersonalCalendarWindowRead["events"][number],
      ): string =>
        `event_${sha256(
          `${work.workId}\0${event.calendarId === familyWorkHousehold.familyCalendarId ? "family" : familyWorkCalendarConnection?.connectionId}\0${event.calendarId}\0${event.providerEventId}\0${event.providerRevision}`,
        ).slice(0, 32)}`;
      const rememberFamilyWorkCalendarEvent = (
        event: GooglePersonalCalendarWindowRead["events"][number],
      ): string => {
        const eventRef = familyWorkCalendarEventRef(event);
        if (event.title) {
          familyWorkCalendarTargets.set(eventRef, {
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
        return eventRef;
      };
      const readFamilyWorkExactCalendarCatalog = (calendarId: string) =>
        this.#readExactFamilyCalendarCatalog({
          householdId: work.household.householdId,
          calendarId,
          credentials: familyWorkFamilyCalendarCredentials,
        });
      const readFamilyWorkExactCalendarWindow = async (input: {
        calendarId: string;
        timeMin: string;
        timeMax: string;
        limit: number;
        cursor: string | null;
      }) =>
        this.#readExactFamilyCalendarWindow({
          householdId: work.household.householdId,
          credentials: familyWorkFamilyCalendarCredentials,
          ...input,
        });
      const listFamilyWorkCalendars = async (): Promise<FlorenceCalendarCatalogRead> => {
        if (!google) {
          return { status: "unavailable", calendars: [], totalCalendarCount: 0 };
        }
        if (work.visibility === "household") {
          const calendarId = familyWorkHousehold.familyCalendarId;
          if (!calendarId || familyWorkFamilyCalendarCredentials.length === 0) {
            return { status: "unavailable", calendars: [], totalCalendarCount: 0 };
          }
          const { read } = await readFamilyWorkExactCalendarCatalog(calendarId);
          return {
            status: read.status,
            calendars: read.calendars.map((calendar) => ({
              calendarRef: familyWorkCalendarRef(calendar.calendarId),
              label: familyWorkHousehold.familyCalendarLabel ?? familyWorkHousehold.name,
              timeZone: familyWorkHousehold.timeZone,
              primary: null,
              accessRole: null,
              eventCoverage: calendar.eventCoverage,
            })),
            totalCalendarCount: read.totalCalendarCount,
          };
        }
        if (!familyWorkCalendarConnection || !familyWorkGoogleAdultId) {
          return { status: "unavailable", calendars: [], totalCalendarCount: 0 };
        }
        const personalRead = await google.readPersonalCalendarCatalog({
          householdId: work.household.householdId,
          ownerAdultId: familyWorkGoogleAdultId,
          connectionId: familyWorkCalendarConnection.connectionId,
          excludedFamilyCalendarId: familyWorkHousehold?.familyCalendarId ?? null,
        });
        const familyRead =
          familyWorkHousehold.familyCalendarId && familyWorkFamilyCalendarCredentials.length > 0
            ? (await readFamilyWorkExactCalendarCatalog(familyWorkHousehold.familyCalendarId)).read
            : null;
        const statuses = [personalRead.status, ...(familyRead ? [familyRead.status] : [])];
        const status: FlorenceCalendarCatalogRead["status"] = statuses.every(
          (candidate) => candidate === "complete",
        )
          ? "complete"
          : statuses.every((candidate) => candidate === "unavailable")
            ? "unavailable"
            : "partial";
        return {
          status,
          calendars: [
            ...personalRead.calendars.slice(0, 99).map((calendar) => ({
              calendarRef: familyWorkCalendarRef(calendar.calendarId),
              label: calendar.label ?? (calendar.primary ? "Primary calendar" : "Calendar"),
              timeZone: calendar.timeZone,
              primary: calendar.primary,
              accessRole: calendar.accessRole,
              eventCoverage: calendar.eventCoverage,
            })),
            ...(familyRead
              ? familyRead.calendars.slice(0, 1).map((calendar) => ({
                  calendarRef: familyWorkCalendarRef(calendar.calendarId),
                  label:
                    familyWorkHousehold.familyCalendarLabel ?? familyWorkHousehold.name ?? "Family Calendar",
                  timeZone: familyWorkHousehold.timeZone,
                  primary: null,
                  accessRole: null,
                  eventCoverage: calendar.eventCoverage,
                }))
              : []),
          ],
          totalCalendarCount: personalRead.totalCalendarCount + (familyRead?.totalCalendarCount ?? 0),
        };
      };
      const readFamilyWorkCalendarWindow = async (input: {
        timeMin: string;
        timeMax: string;
        pageSize: number;
        cursor: string | null;
        scope: "all" | "primary" | "selected";
        calendarRefs: readonly string[];
      }): Promise<FlorenceCalendarWindowRead> => {
        if (!google) {
          return {
            status: "unavailable",
            calendars: [],
            totalCalendarCount: 0,
            events: [],
            totalEventCount: 0,
            nextCursor: null,
          };
        }
        let read: GooglePersonalCalendarWindowRead;
        if (work.visibility === "household") {
          const calendarId = familyWorkHousehold.familyCalendarId;
          if (!calendarId || familyWorkFamilyCalendarCredentials.length === 0) {
            return {
              status: "unavailable",
              calendars: [],
              totalCalendarCount: 0,
              events: [],
              totalEventCount: 0,
              nextCursor: null,
            };
          }
          if (
            input.scope === "selected" &&
            (input.calendarRefs.length !== 1 || input.calendarRefs[0] !== familyWorkCalendarRef(calendarId))
          ) {
            return {
              status: "unavailable",
              calendars: [],
              totalCalendarCount: 0,
              events: [],
              totalEventCount: 0,
              nextCursor: null,
            };
          }
          read = (
            await readFamilyWorkExactCalendarWindow({
              calendarId,
              timeMin: input.timeMin,
              timeMax: input.timeMax,
              limit: input.pageSize,
              cursor: input.cursor,
            })
          ).read;
        } else {
          if (!familyWorkCalendarConnection || !familyWorkGoogleAdultId) {
            return {
              status: "unavailable",
              calendars: [],
              totalCalendarCount: 0,
              events: [],
              totalEventCount: 0,
              nextCursor: null,
            };
          }
          const familyCalendarId = familyWorkHousehold.familyCalendarId;
          const selectedFamilyCalendar =
            input.scope === "selected" &&
            familyCalendarId !== null &&
            input.calendarRefs.length === 1 &&
            input.calendarRefs[0] === familyWorkCalendarRef(familyCalendarId);
          if (selectedFamilyCalendar) {
            if (familyWorkFamilyCalendarCredentials.length === 0) {
              return {
                status: "unavailable",
                calendars: [],
                totalCalendarCount: 0,
                events: [],
                totalEventCount: 0,
                nextCursor: null,
              };
            }
            read = (
              await readFamilyWorkExactCalendarWindow({
                calendarId: familyCalendarId,
                timeMin: input.timeMin,
                timeMax: input.timeMax,
                limit: input.pageSize,
                cursor: input.cursor,
              })
            ).read;
          } else {
            let calendarIds: readonly string[] | undefined;
            const catalog = async () =>
              google.readPersonalCalendarCatalog({
                householdId: work.household.householdId,
                ownerAdultId: familyWorkGoogleAdultId,
                connectionId: familyWorkCalendarConnection.connectionId,
                excludedFamilyCalendarId: familyCalendarId,
              });
            if (input.scope === "selected") {
              const idsByRef = new Map(
                (await catalog()).calendars.map((calendar) => [
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
                  nextCursor: null,
                };
              }
              calendarIds = resolved;
            } else if (input.scope === "primary") {
              const primary = (await catalog()).calendars.find((calendar) => calendar.primary);
              if (!primary) {
                return {
                  status: "unavailable",
                  calendars: [],
                  totalCalendarCount: 0,
                  events: [],
                  totalEventCount: 0,
                  nextCursor: null,
                };
              }
              calendarIds = [primary.calendarId];
            }
            read = await google.readPersonalCalendarWindow({
              householdId: work.household.householdId,
              ownerAdultId: familyWorkGoogleAdultId,
              connectionId: familyWorkCalendarConnection.connectionId,
              excludedFamilyCalendarId: familyCalendarId,
              timeMin: input.timeMin,
              timeMax: input.timeMax,
              ...(calendarIds === undefined ? {} : { calendarIds }),
              limit: input.pageSize,
              ...(input.cursor === null ? {} : { cursor: input.cursor }),
            });
          }
        }
        const labels = new Map(
          read.calendars.map((calendar) => [
            calendar.calendarId,
            calendar.calendarId === familyWorkHousehold.familyCalendarId
              ? (familyWorkHousehold.familyCalendarLabel ?? familyWorkHousehold.name)
              : calendar.label,
          ]),
        );
        const projectedCalendars = read.calendars.slice(0, 100);
        const modelStatus =
          read.status === "complete" && read.totalCalendarCount > projectedCalendars.length
            ? "truncated"
            : read.status;
        return {
          status: modelStatus,
          calendars: projectedCalendars.map((calendar) => ({
            calendarRef: familyWorkCalendarRef(calendar.calendarId),
            label: labels.get(calendar.calendarId) ?? null,
            timeZone:
              calendar.calendarId === familyWorkHousehold.familyCalendarId
                ? familyWorkHousehold.timeZone
                : calendar.timeZone,
            primary:
              calendar.calendarId === familyWorkHousehold.familyCalendarId || calendar.status === "missing"
                ? null
                : calendar.primary,
            accessRole:
              calendar.calendarId === familyWorkHousehold.familyCalendarId ? null : calendar.accessRole,
            status: calendar.status,
            eventCount: calendar.eventCount,
          })),
          totalCalendarCount: read.totalCalendarCount,
          totalEventCount: read.totalEventCount,
          nextCursor: read.nextCursor,
          events: read.events.map((event) => {
            const ref = rememberFamilyWorkCalendarEvent(event);
            return event.intervalKind === "all_day"
              ? {
                  intervalKind: event.intervalKind,
                  calendarRef: familyWorkCalendarRef(event.calendarId),
                  calendarLabel: labels.get(event.calendarId) ?? null,
                  eventRef: ref,
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
                  eventRef: ref,
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
      };
      const familyWorkPendingCapabilityIdentity = (
        name: ClaimedFamilyWorkCapabilityIdentity["name"],
        occurredAt = this.#now().toISOString(),
      ): ClaimedFamilyWorkCapabilityIdentity => {
        const pending = work.state.pendingCall;
        if (!pending || pending.name !== name) {
          throw new Error(`Durable ${name} lost its pending capability metadata`);
        }
        return {
          workId: work.workId,
          generation: work.generation,
          claimId: work.claimId,
          callId: pending.callId,
          name,
          argumentsJson: pending.argumentsJson,
          occurredAt,
        };
      };
      const indexFamilyWorkConversationHistory = (
        messages: readonly ConversationHistoryMessage[],
      ): readonly FlorenceConversationHistoryMessage[] =>
        messages.map((message) => {
          const modelMessage = modelConversationHistoryMessage(message);
          familyWorkSourceIndex.set(modelMessage.sourceId, conversationHistorySource(modelMessage));
          return modelMessage;
        });
      const readFamilyWorkFileAsset = async (
        assetId: string,
        taskSignal?: AbortSignal,
      ): Promise<{ filename: string; mimeType: string; bytes: Uint8Array }> => {
        taskSignal?.throwIfAborted();
        if (!this.#imageVault) throw new Error("Florence file storage is not configured");
        const browserFile = (work.state.browserFiles ?? []).find(
          (candidate) => candidate.assetId === assetId && candidate.workId === work.workId,
        );
        if (browserFile) {
          const bytes = await this.#imageVault.readFileArtifact({
            householdId: work.household.householdId,
            workId: work.workId,
            artifact: {
              artifactId: browserFile.assetId,
              workId: browserFile.workId,
              filename: browserFile.filename,
              mimeType: browserFile.mimeType,
              byteLength: browserFile.byteLength,
              sha256: browserFile.sha256,
            },
          });
          return { filename: browserFile.filename, mimeType: browserFile.mimeType, bytes };
        }
        const image = familyWorkOriginImages.find((candidate) => candidate.assetId === assetId);
        if (image) {
          const read = await this.#imageVault.read({
            householdId: work.household.householdId,
            signalId: image.signalId,
            image: { assetId: image.assetId, mimeType: image.mimeType },
          });
          return {
            filename: browserImageUploadFilename(image.assetId, read.mimeType),
            mimeType: read.mimeType,
            bytes: read.bytes,
          };
        }
        const document = familyWorkOriginDocuments.find((candidate) => candidate.id === assetId);
        if (document) {
          const read = this.#imageVault.openPdf({
            documentId: document.id,
            householdId: work.household.householdId,
            signalId: document.parentSourceId,
            filename: document.filename,
            mimeType: document.mimeType,
            contentDigest: document.contentDigest,
            contentEnvelope: document.contentEnvelope,
            discardAfter: document.discardAfter,
            now: this.#now(),
          });
          return {
            filename: browserPdfUploadFilename(document.filename, document.id),
            mimeType: document.mimeType,
            bytes: read.bytes,
          };
        }
        if (google && familyWorkExecutionAdultId) {
          const gmailFile = [...familyWorkGmailAttachmentIndex.values()].find(
            (candidate) => candidate.fileAssetId === assetId,
          );
          if (gmailFile) {
            const read = await google.readGmailAttachment({
              householdId: work.household.householdId,
              ownerAdultId: familyWorkExecutionAdultId,
              connectionId: gmailFile.connectionId,
              attachment: gmailFile.attachment,
            });
            return { filename: read.filename, mimeType: read.mimeType, bytes: read.bytes };
          }
        }
        throw new Error("The selected file is no longer available to this task");
      };
      const runVaultWork = async (
        request: FlorenceVaultWorkRequest,
        taskSignal?: AbortSignal,
      ): Promise<FlorenceVaultWorkResult> => {
        taskSignal?.throwIfAborted();
        const identity = familyWorkPendingCapabilityIdentity("vault_work");
        const replay = work.state.pendingCall?.receipt;
        if (replay) return replay.output as FlorenceVaultWorkResult;
        if (request.operation === "forget") {
          const existing = familyWorkFacts.find((fact) => fact.id === request.factId);
          if (
            !existing ||
            (work.visibility === "household" && existing.visibility !== "household") ||
            (work.visibility === "private" &&
              existing.visibility === "private" &&
              existing.ownerAdultId !== familyWorkExecutionAdultId)
          ) {
            throw new Error("Durable work cannot forget memory outside its conversation");
          }
          if (request.expectedUpdatedAt !== existing.updatedAt) {
            throw new FlorenceStoreConflict("Vault memory changed before this task could forget it");
          }
          const receipt = await this.#store.runClaimedFamilyWorkDatabaseCapability({
            ...identity,
            capability: {
              name: "vault_work",
              mutation: {
                operation: "forget",
                factId: request.factId,
                expectedUpdatedAt: request.expectedUpdatedAt,
              },
            },
          });
          if (this.#imageVault) {
            await Promise.allSettled(
              decodeFactFileArtifacts(existing.value).map((artifact) =>
                this.#imageVault?.deleteHouseholdFileArtifact({
                  householdId: work.household.householdId,
                  artifact,
                }),
              ),
            );
          }
          return receipt.output as FlorenceVaultWorkResult;
        }

        const existing =
          request.operation === "correct" ? familyWorkFacts.find((fact) => fact.id === request.factId) : null;
        if (request.operation === "correct" && !existing) {
          throw new Error("Durable work cannot correct memory it has not read");
        }
        if (request.operation === "correct" && request.expectedUpdatedAt !== existing?.updatedAt) {
          throw new FlorenceStoreConflict("Vault memory changed before this task could correct it");
        }
        if (work.visibility === "household" && request.visibility !== "household") {
          throw new Error("Household-visible work cannot create private memory");
        }
        if (existing && existing.visibility !== request.visibility) {
          throw new Error("A memory correction cannot change who can see it");
        }
        const ownerAdultId = request.visibility === "private" ? familyWorkExecutionAdultId : null;
        if (request.visibility === "private" && !ownerAdultId) {
          throw new Error("Private durable memory lost its adult owner");
        }
        const factId =
          existing?.id ?? deterministicUuid(`family-work-fact\0${work.workId}\0${identity.callId}`);
        const existingFiles = existing ? decodeFactFileArtifacts(existing.value) : [];
        const selectedAssetIds = request.fileAssetIds === null ? null : [...new Set(request.fileAssetIds)];
        if (selectedAssetIds && selectedAssetIds.length !== request.fileAssetIds?.length) {
          throw new Error("A Vault artifact cannot retain the same file twice");
        }
        const stagedFiles: readonly FileArtifactReference[] =
          selectedAssetIds === null
            ? []
            : await Promise.all(
                selectedAssetIds.map(async (assetId) => {
                  if (!this.#imageVault) throw new Error("Florence file storage is not configured");
                  const file = await readFamilyWorkFileAsset(assetId, taskSignal);
                  return this.#imageVault.storeFileArtifact({
                    artifactId: vaultFileArtifactId(factId, `${identity.callId}\0${assetId}`),
                    householdId: work.household.householdId,
                    workId: work.workId,
                    callId: identity.callId,
                    filename: file.filename,
                    declaredMimeType: file.mimeType,
                    bytes: file.bytes,
                  });
                }),
              );
        const files: readonly FileArtifactReference[] =
          selectedAssetIds === null ? existingFiles : stagedFiles;
        const slot =
          existing?.slot ??
          (request.memory.memoryKind === "artifact" && request.memory.title
            ? `artifact:${request.memory.artifactKind}:${sha256(request.memory.title.toLocaleLowerCase())}`
            : `${request.memory.memoryKind}:${sha256(request.statement.toLocaleLowerCase())}`);
        const sourceIds = [...new Set([...request.sourceIds, work.origin.message.sourceId])];
        const fact: FactDraft = {
          id: factId,
          subjectPersonId: existing?.subjectPersonId ?? null,
          kind: existing?.kind ?? (request.memory.memoryKind === "preference" ? "preference" : "general"),
          slot,
          label: request.memory.title ?? existing?.label ?? request.statement.slice(0, 160),
          value: {
            statement: request.statement,
            memoryKind: request.memory.memoryKind,
            artifactKind: request.memory.artifactKind,
            title: request.memory.title,
            details: request.memory.details,
            tags: request.memory.tags,
            files,
          },
          visibility: request.visibility,
          ownerAdultId,
          sourceIds,
        };
        const mutation =
          request.operation === "correct"
            ? (() => {
                if (!existing) throw new Error("Durable work lost the memory it was correcting");
                return {
                  operation: "correct" as const,
                  fact,
                  expectedUpdatedAt: request.expectedUpdatedAt,
                };
              })()
            : { operation: "remember" as const, fact };
        const receipt = await (async () => {
          try {
            return await this.#store.runClaimedFamilyWorkDatabaseCapability({
              ...identity,
              capability: {
                name: "vault_work",
                mutation,
              },
            });
          } catch (error) {
            if (error instanceof FlorenceStoreConflict && this.#imageVault) {
              await Promise.allSettled(
                stagedFiles.map((artifact) =>
                  this.#imageVault?.deleteHouseholdFileArtifact({
                    householdId: work.household.householdId,
                    artifact,
                  }),
                ),
              );
            }
            throw error;
          }
        })();
        if (this.#imageVault) {
          const retainedArtifactIds = new Set(files.map((artifact) => artifact.artifactId));
          await Promise.allSettled(
            existingFiles
              .filter((artifact) => !retainedArtifactIds.has(artifact.artifactId))
              .map((artifact) =>
                this.#imageVault?.deleteHouseholdFileArtifact({
                  householdId: work.household.householdId,
                  artifact,
                }),
              ),
          );
        }
        return receipt.output as FlorenceVaultWorkResult;
      };
      const runReminderWork = async (
        request: FlorenceReminderWorkRequest,
        taskSignal?: AbortSignal,
      ): Promise<FlorenceReminderWorkResult> => {
        taskSignal?.throwIfAborted();
        if (request.operation === "list") {
          const reminders = await this.#store.readClaimedFamilyWorkReminders({
            workId: work.workId,
            generation: work.generation,
            claimId: work.claimId,
            occurredAt: this.#now().toISOString(),
          });
          return {
            status: "listed",
            reminders: reminders.map((reminder) => ({
              reminderId: reminder.reminderId,
              action: reminder.action,
              schedule:
                reminder.schedule.kind === "weekly"
                  ? { ...reminder.schedule, weekdays: [...reminder.schedule.weekdays] }
                  : reminder.schedule,
              state: reminder.status,
              nextAt: reminder.nextAt,
              lastRunAt: reminder.lastRunAt,
              createdAt: reminder.createdAt,
            })),
          };
        }
        const identity = familyWorkPendingCapabilityIdentity("reminder_work");
        const mutation: ReminderMutation =
          request.operation === "create"
            ? {
                operation: "create",
                reminderId: deterministicUuid(`family-work-reminder\0${work.workId}\0${identity.callId}`),
                action: request.action,
                schedule: request.schedule,
                visibility: work.visibility,
                ownerAdultId: work.visibility === "private" ? familyWorkExecutionAdultId : null,
              }
            : request.operation === "update"
              ? {
                  operation: "update",
                  reminderId: request.reminderId,
                  action: request.action,
                  schedule: request.schedule,
                }
              : { operation: request.operation, reminderId: request.reminderId };
        const receipt = await this.#store.runClaimedFamilyWorkDatabaseCapability({
          ...identity,
          capability: { name: "reminder_work", mutation },
        });
        return receipt.output as FlorenceReminderWorkResult;
      };
      const runParticipantRequest = async (
        request: FlorenceParticipantRequest,
        taskSignal?: AbortSignal,
      ): Promise<FlorenceParticipantRequestResult> => {
        taskSignal?.throwIfAborted();
        if (work.visibility !== "household") {
          throw new Error("Only household work can ask another enrolled adult");
        }
        const identity = familyWorkPendingCapabilityIdentity("participant_request");
        const receipt = await this.#store.runClaimedFamilyWorkDatabaseCapability({
          ...identity,
          capability: {
            name: "participant_request",
            targetAdultName: request.targetAdultName,
            question: request.question,
          },
        });
        return receipt.output as FlorenceParticipantRequestResult;
      };
      const resolveFamilyWorkCalendarTarget = async (
        request: Exclude<FlorenceFamilyCalendarWorkRequest, { operation: "create" }>,
      ): Promise<CalendarEventTarget> => {
        let target = familyWorkCalendarTargets.get(request.target.eventRef) ?? null;
        if (!target) {
          const calendarId = familyWorkHousehold.familyCalendarId;
          if (!calendarId) throw new Error("The Family Calendar is unavailable");
          const targetStart =
            request.target.observedEvent.intervalKind === "all_day"
              ? zonedCalendarDateStart(request.target.observedEvent.startDate, familyWorkHousehold.timeZone)
              : new Date(request.target.observedEvent.startsAt);
          let pageCursor: string | null = null;
          do {
            const read: GooglePersonalCalendarWindowRead = (
              await readFamilyWorkExactCalendarWindow({
                calendarId,
                timeMin: new Date(targetStart.getTime() - 60_000).toISOString(),
                timeMax: new Date(targetStart.getTime() + 60_000).toISOString(),
                limit: 50,
                cursor: pageCursor,
              })
            ).read;
            if (
              (read.status !== "complete" && read.status !== "truncated") ||
              read.calendars.some((calendar) => calendar.status !== "complete")
            ) {
              throw new Error("The exact Family Calendar event could not be completely re-read");
            }
            for (const event of read.events) rememberFamilyWorkCalendarEvent(event);
            target = familyWorkCalendarTargets.get(request.target.eventRef) ?? null;
            pageCursor = read.nextCursor;
          } while (!target && pageCursor !== null);
        }
        if (
          !target ||
          JSON.stringify(target.observedEvent) !== JSON.stringify(request.target.observedEvent)
        ) {
          throw new Error("The Family Calendar event changed before it could be updated");
        }
        return target;
      };
      const assertCompleteFamilyCalendarCoverage = async (
        event: Extract<FlorenceFamilyCalendarWorkRequest, { operation: "create" }>["event"],
      ): Promise<void> => {
        const calendarId = familyWorkHousehold.familyCalendarId;
        if (!calendarId) throw new Error("The Family Calendar is unavailable");
        const intervalStart =
          event.intervalKind === "all_day"
            ? zonedCalendarDateStart(event.startDate, familyWorkHousehold.timeZone)
            : new Date(event.startsAt);
        const intervalEnd =
          event.intervalKind === "all_day"
            ? zonedCalendarDateStart(event.endDate, familyWorkHousehold.timeZone)
            : new Date(event.endsAt);
        const maxWindowMs = 31 * 24 * 60 * 60_000;
        for (let cursor = intervalStart.getTime(); cursor < intervalEnd.getTime(); ) {
          const end = Math.min(cursor + maxWindowMs, intervalEnd.getTime());
          let pageCursor: string | null = null;
          let expectedEventCount: number | null = null;
          let observedEventCount = 0;
          const seenCursors = new Set<string>();
          const seenEventIds = new Set<string>();
          do {
            const read: GooglePersonalCalendarWindowRead = (
              await readFamilyWorkExactCalendarWindow({
                calendarId,
                timeMin: new Date(cursor).toISOString(),
                timeMax: new Date(end).toISOString(),
                limit: 50,
                cursor: pageCursor,
              })
            ).read;
            if (
              (read.status !== "complete" && read.status !== "truncated") ||
              read.calendars.length !== 1 ||
              read.calendars[0]?.calendarId !== calendarId ||
              read.calendars[0].status !== "complete" ||
              (read.status === "complete" && read.nextCursor !== null) ||
              (read.status === "truncated" && read.nextCursor === null)
            ) {
              throw new Error("The Family Calendar interval could not be completely re-read");
            }
            if (expectedEventCount === null) expectedEventCount = read.totalEventCount;
            if (expectedEventCount !== read.totalEventCount) {
              throw new Error("The Family Calendar changed while it was being re-read");
            }
            for (const event of read.events) {
              if (seenEventIds.has(event.providerEventId)) {
                throw new Error("The Family Calendar repeated an event while it was being re-read");
              }
              seenEventIds.add(event.providerEventId);
              observedEventCount += 1;
            }
            const nextCursor = read.nextCursor;
            if (nextCursor === null) {
              pageCursor = null;
            } else {
              if (seenCursors.has(nextCursor)) {
                throw new Error("The Family Calendar repeated a continuation while it was being re-read");
              }
              seenCursors.add(nextCursor);
              pageCursor = nextCursor;
            }
          } while (pageCursor !== null);
          if (expectedEventCount !== observedEventCount) {
            throw new Error("The Family Calendar interval was not completely re-read");
          }
          cursor = end;
        }
      };
      const runFamilyCalendarWork = async (
        request: FlorenceFamilyCalendarWorkRequest,
        taskSignal?: AbortSignal,
      ): Promise<FlorenceFamilyCalendarWorkResult> => {
        taskSignal?.throwIfAborted();
        if (
          !google ||
          !familyWorkHousehold.familyCalendarId ||
          familyWorkFamilyCalendarCredentials.length === 0
        ) {
          throw new Error("Shared Family Calendar work is unavailable");
        }
        const identity = familyWorkPendingCapabilityIdentity("family_calendar_work");
        const initialPreparation = await this.#store.prepareClaimedFamilyWorkCalendarCapability(identity);
        if (initialPreparation.status === "replayed") {
          return initialPreparation.receipt.output as FlorenceFamilyCalendarWorkResult;
        }
        if (request.operation === "create") {
          await assertCompleteFamilyCalendarCoverage(request.event);
        }
        const mutation: FamilyCalendarMutation =
          request.operation === "create"
            ? request
            : request.operation === "delete"
              ? {
                  operation: "delete",
                  event: null,
                  target: await resolveFamilyWorkCalendarTarget(request),
                }
              : {
                  operation: "update",
                  event: request.event,
                  target: await resolveFamilyWorkCalendarTarget(request),
                };
        const prepared = await this.#store.prepareClaimedFamilyWorkCalendarCapability({
          ...identity,
          occurredAt: this.#now().toISOString(),
        });
        if (prepared.status === "replayed") {
          return prepared.receipt.output as FlorenceFamilyCalendarWorkResult;
        }
        taskSignal?.throwIfAborted();
        let result: Awaited<ReturnType<GoogleConnection["executeCalendar"]>>;
        try {
          result = await google.executeCalendar({
            actionId: deterministicUuid(
              `family-work-calendar\0${work.workId}\0${sha256(identity.argumentsJson)}`,
            ),
            householdId: work.household.householdId,
            connectionId: prepared.credential.connectionId,
            ownerAdultId: prepared.credential.ownerAdultId,
            calendarId: prepared.credential.calendarId,
            mutation,
          });
        } catch (error) {
          taskSignal?.throwIfAborted();
          if (error instanceof GoogleCalendarTransientError) {
            throw new CapabilityAdapterError("transient", "Google Calendar is temporarily unavailable.");
          }
          if (credentialInvalidGrant(error)) {
            throw new CapabilityAdapterError(
              "permanent",
              "Google Calendar needs to be reconnected before Florence can make that change.",
            );
          }
          throw new CapabilityAdapterError("permanent", "Google Calendar could not complete that change.");
        }
        if (result.status === "credential_rejected") {
          await google.disconnect({
            connectionId: prepared.credential.connectionId,
            householdId: work.household.householdId,
            ownerAdultId: prepared.credential.ownerAdultId,
            now: result.occurredAt,
          });
          throw new CapabilityAdapterError(
            "permanent",
            "Google Calendar needs to be reconnected before Florence can make that change.",
          );
        }
        if (result.status === "failed") {
          throw new CapabilityAdapterError(
            "permanent",
            result.detail || "Google Calendar could not complete that change.",
          );
        }
        const output: FlorenceFamilyCalendarWorkResult = {
          status: "committed",
          operation: request.operation,
          providerEventId: result.providerEventId,
          providerRevision: result.providerRevision,
        };
        const receipt = await this.#store.checkpointClaimedFamilyWorkCapabilityReceipt({
          ...identity,
          occurredAt: result.occurredAt,
          output,
        });
        return receipt.output as FlorenceFamilyCalendarWorkResult;
      };
      const step = await reasoner.continueFamilyWork(
        {
          workId: work.workId,
          objective: work.objective,
          visibility: work.visibility,
          ownerAdultId: work.ownerAdultId,
          initiatingAdultId: work.initiatingAdultId,
          origin: work.origin,
          household: work.household,
          linkedSources: work.linkedSources.map((source) => ({ ...source })),
          visibleSources: familyWorkVisibleSources,
          googleConnections: familyWorkGoogleModelConnections,
          lastDeliveredProgress: work.lastDeliveredProgress,
          state: work.state,
          scheduledOccurrence: work.scheduledOccurrence
            ? {
                schedule:
                  work.scheduledOccurrence.schedule.kind === "weekly"
                    ? {
                        ...work.scheduledOccurrence.schedule,
                        weekdays: [...work.scheduledOccurrence.schedule.weekdays],
                      }
                    : work.scheduledOccurrence.schedule,
                previousResult: work.scheduledOccurrence.previousResult,
                previousRunAt: work.scheduledOccurrence.previousRunAt,
              }
            : null,
          currentTime: this.#now().toISOString(),
        },
        {
          runVaultWork,
          runReminderWork,
          ...(work.visibility === "household" ? { runParticipantRequest } : {}),
          ...(google && familyWorkHousehold.familyCalendarId && familyWorkFamilyCalendarCredentials.length > 0
            ? { runFamilyCalendarWork }
            : {}),
          searchVault: async (input) => familyWorkVaultRecall.search(input),
          readVault: async (input) => familyWorkVaultRecall.read(input),
          searchConversationHistory: async ({ query, after, before, cursor }) => {
            const page = await this.#store.searchClaimedFamilyWorkConversationHistory({
              workId: work.workId,
              generation: work.generation,
              claimId: work.claimId,
              occurredAt: familyWorkConversationHistoryObservedAt,
              query,
              after,
              before,
              cursor,
            });
            return {
              ...page,
              messages: indexFamilyWorkConversationHistory(page.messages),
            };
          },
          readConversationHistory: async ({ anchor, cursor }) => {
            const page = await this.#store.readClaimedFamilyWorkConversationHistory({
              workId: work.workId,
              generation: work.generation,
              claimId: work.claimId,
              occurredAt: familyWorkConversationHistoryObservedAt,
              anchor,
              cursor,
            });
            return {
              ...page,
              messages: indexFamilyWorkConversationHistory(page.messages),
            };
          },
          ...(work.visibility === "private"
            ? {
                searchSources: async ({ query, cursor }: { query: string | null; cursor: string | null }) => {
                  const page = await this.#store.searchClaimedFamilyWorkPrivateGoogleSources({
                    workId: work.workId,
                    generation: work.generation,
                    claimId: work.claimId,
                    occurredAt: familyWorkConversationHistoryObservedAt,
                    query,
                    cursor,
                  });
                  return {
                    results: page.sources.map((source) => ({
                      sourceId: source.sourceId,
                      kind: source.kind,
                      label: modelSafeGmailText(source.label),
                      occurredAt: source.occurredAt,
                      match: modelSafeGmailText(source.match),
                    })),
                    complete: page.complete,
                    nextCursor: page.nextCursor,
                  };
                },
              }
            : {}),
          searchFamilyMemory: async ({ query, limit }) =>
            searchMemorySources(familyWorkSearchableMemory, query).slice(0, limit),
          readSource: async ({ sourceId }) => {
            const indexed = familyWorkSourceIndex.get(sourceId);
            if (indexed) return indexed;
            if (familyWorkLinkedGoogleSourceIds.has(sourceId)) {
              const retained = await this.#store.readClaimedFamilyWorkLinkedGoogleSource({
                workId: work.workId,
                generation: work.generation,
                claimId: work.claimId,
                occurredAt: familyWorkConversationHistoryObservedAt,
                sourceId,
              });
              const source = retainedPrivateGoogleSource(retained);
              familyWorkSourceIndex.set(source.sourceId, source);
              return source;
            }
            const retained =
              work.visibility === "private"
                ? await this.#store.readClaimedFamilyWorkPrivateGoogleSource({
                    workId: work.workId,
                    generation: work.generation,
                    claimId: work.claimId,
                    occurredAt: familyWorkConversationHistoryObservedAt,
                    sourceId,
                  })
                : null;
            if (!retained) return null;
            const source = retainedPrivateGoogleSource(retained);
            familyWorkSourceIndex.set(source.sourceId, source);
            return source;
          },
          ...(work.visibility === "household"
            ? {
                readHouseholdAvailability: (window: { timeMin: string; timeMax: string }) =>
                  this.#readHouseholdAvailability({
                    household: familyWorkHousehold,
                    ...window,
                  }),
              }
            : {}),
          ...(google && familyWorkGmailConnection && familyWorkExecutionAdultId
            ? {
                searchGmail: async ({ query, after, before, limit }) => {
                  const evidence = await google.searchGmail({
                    householdId: work.household.householdId,
                    ownerAdultId: familyWorkExecutionAdultId,
                    connectionId: familyWorkGmailConnection.connectionId,
                    query,
                    ...(after === null ? {} : { after }),
                    ...(before === null ? {} : { before }),
                    limit,
                  });
                  const sources = evidence.messages.map((message) =>
                    indexFamilyWorkGmailMessage(message, familyWorkGmailConnection.connectionId),
                  );
                  return { status: evidence.status, sources };
                },
                readGmailAttachment: async ({ sourceId, attachment }) => {
                  const indexed = familyWorkGmailAttachmentIndex.get(
                    `${sourceId}\0${attachment.attachmentRef}`,
                  );
                  if (
                    !indexed ||
                    indexed.attachment.filename !== attachment.filename ||
                    indexed.attachment.mimeType !== attachment.mimeType ||
                    indexed.attachment.sizeBytes !== attachment.sizeBytes
                  ) {
                    throw new Error("The Gmail attachment reference changed before it could be read");
                  }
                  const read = await google.readGmailAttachment({
                    householdId: work.household.householdId,
                    ownerAdultId: familyWorkExecutionAdultId,
                    connectionId: indexed.connectionId,
                    attachment: indexed.attachment,
                  });
                  return {
                    sourceId,
                    attachmentRef: attachment.attachmentRef,
                    filename: read.filename,
                    mimeType: read.mimeType,
                    bytes: read.bytes,
                  };
                },
                ...(familyWorkWorkspaceConnection
                  ? {
                      resolveWorkspaceGmailAttachment: async ({
                        sourceId,
                        attachmentRef,
                      }: {
                        sourceId: string;
                        attachmentRef: string;
                      }) => {
                        const indexed = familyWorkGmailAttachmentIndex.get(`${sourceId}\0${attachmentRef}`);
                        if (!indexed || indexed.connectionId !== familyWorkWorkspaceConnection.connectionId) {
                          throw new Error("The Gmail attachment is not in the active Workspace account");
                        }
                        return {
                          messageId: indexed.attachment.messageId,
                          attachmentId: indexed.attachment.attachmentId,
                        };
                      },
                    }
                  : {}),
              }
            : {}),
          ...(publicPages
            ? { runPublicPage: (request, taskSignal) => publicPages.run(request, taskSignal) }
            : {}),
          ...(maps ? { runMaps: (request, taskSignal) => maps.run(request, taskSignal) } : {}),
          ...(weather ? { runWeather: (request, taskSignal) => weather.run(request, taskSignal) } : {}),
          ...(flights ? { runFlights: (request, taskSignal) => flights.search(request, taskSignal) } : {}),
          ...(telephony
            ? {
                telephonyProviders: telephony.configuredProviders,
                runTelephony: (operation: FlorenceTelephonyOperation, taskSignal?: AbortSignal) => {
                  const pendingCall = work.state.pendingCall;
                  if (
                    !pendingCall ||
                    !["phone_agent_call", "sms_work", "phone_announcement"].includes(pendingCall.name)
                  ) {
                    throw new Error("Durable phone work lost its pending call metadata");
                  }
                  return telephony
                    .run(
                      {
                        workId: work.workId,
                        callId: pendingCall.callId,
                        attempt: pendingCall.attempt,
                        operation,
                      },
                      operation.kind === "ai_call_start" ? undefined : taskSignal,
                    )
                    .then((result: FlorenceTelephonyResult) => {
                      if (
                        (operation.kind === "ai_call_start" || operation.kind === "call_start") &&
                        result.providerId &&
                        (result.kind === "accepted" ||
                          result.kind === "progress" ||
                          result.kind === "uncertain_effect")
                      ) {
                        uncheckpointedPhoneCall = {
                          provider: result.provider,
                          kind: operation.kind === "ai_call_start" ? "agent" : "announcement",
                          providerCallId: result.providerId,
                        };
                      }
                      if (
                        (operation.kind === "sms_send" || operation.kind === "sms_status") &&
                        result.provider === "twilio" &&
                        result.providerId &&
                        (result.kind === "accepted" ||
                          result.kind === "progress" ||
                          result.kind === "uncertain_effect")
                      ) {
                        uncheckpointedTextMessage = {
                          provider: "twilio",
                          messageSid: result.providerId,
                        };
                      }
                      return result;
                    });
                },
              }
            : {}),
          ...(browser && familyWorkExecutionAdultId
            ? {
                runBrowser: async (operation: FlorenceBrowserOperation, taskSignal?: AbortSignal) => {
                  const pendingCall = work.state.pendingCall;
                  if (pendingCall?.name !== "browser_work") {
                    throw new Error("Durable browser work lost its pending call metadata");
                  }
                  const uploadFile =
                    operation.kind === "upload" && pendingCall.attempt === 1
                      ? await (async () => {
                          if (operation.sourceId) {
                            if (!google || !familyWorkExecutionAdultId) {
                              throw new Error("The Gmail attachment is unavailable for browser upload");
                            }
                            const indexed = familyWorkGmailAttachmentIndex.get(
                              `${operation.sourceId}\0${operation.attachmentRef}`,
                            );
                            if (!indexed) {
                              throw new Error("The Gmail attachment changed before browser upload");
                            }
                            const read = await google.readGmailAttachment({
                              householdId: work.household.householdId,
                              ownerAdultId: familyWorkExecutionAdultId,
                              connectionId: indexed.connectionId,
                              attachment: indexed.attachment,
                            });
                            return { filename: read.filename, bytes: read.bytes };
                          }
                          if (!this.#imageVault) {
                            throw new Error("Florence attachment upload is not configured");
                          }
                          const storedFile = (work.state.browserFiles ?? []).find(
                            (candidate) =>
                              candidate.assetId === operation.attachmentRef &&
                              candidate.workId === work.workId &&
                              candidate.signalId === work.workId,
                          );
                          if (storedFile) {
                            const bytes = await this.#imageVault.readFileArtifact({
                              householdId: work.household.householdId,
                              workId: work.workId,
                              artifact: {
                                artifactId: storedFile.assetId,
                                workId: storedFile.workId,
                                filename: storedFile.filename,
                                mimeType: storedFile.mimeType,
                                byteLength: storedFile.byteLength,
                                sha256: storedFile.sha256,
                              },
                            });
                            return { filename: storedFile.filename, bytes };
                          }
                          const savedVaultFile = familyWorkVaultRecall.resolveFile(operation.attachmentRef);
                          if (savedVaultFile) {
                            const bytes = await this.#imageVault.readHouseholdFileArtifact({
                              householdId: work.household.householdId,
                              artifact: savedVaultFile.artifact,
                            });
                            return { filename: savedVaultFile.artifact.filename, bytes };
                          }
                          const image = familyWorkOriginImages.find(
                            (candidate) => candidate.assetId === operation.attachmentRef,
                          );
                          if (image) {
                            const read = await this.#imageVault.read({
                              householdId: work.household.householdId,
                              signalId: image.signalId,
                              image: { assetId: image.assetId, mimeType: image.mimeType },
                            });
                            return {
                              filename: browserImageUploadFilename(image.assetId, read.mimeType),
                              bytes: read.bytes,
                            };
                          }
                          const document = familyWorkOriginDocuments.find(
                            (candidate) => candidate.id === operation.attachmentRef,
                          );
                          if (!document) {
                            throw new Error(
                              "Browser upload requires a downloaded file or an image or PDF from the initiating Messages context",
                            );
                          }
                          const read = this.#imageVault.openPdf({
                            documentId: document.id,
                            householdId: work.household.householdId,
                            signalId: document.parentSourceId,
                            filename: document.filename,
                            mimeType: document.mimeType,
                            contentDigest: document.contentDigest,
                            contentEnvelope: document.contentEnvelope,
                            discardAfter: document.discardAfter,
                            now: this.#now(),
                          });
                          return {
                            filename: browserPdfUploadFilename(document.filename, document.id),
                            bytes: read.bytes,
                          };
                        })()
                      : undefined;
                  const selectedAssetId =
                    operation.kind === "capture"
                      ? deterministicUuid(`family-work-browser-image\0${work.workId}\0${pendingCall.callId}`)
                      : null;
                  const selectedFileAssetId =
                    operation.kind === "download"
                      ? deterministicUuid(`family-work-browser-file\0${work.workId}\0${pendingCall.callId}`)
                      : null;
                  if (
                    operation.kind === "capture" &&
                    selectedAssetId &&
                    pendingCall.attempt > 1 &&
                    browserSession &&
                    this.#imageVault
                  ) {
                    for (const mimeType of ["image/png", "image/jpeg", "image/webp"] as const) {
                      try {
                        const read = await this.#imageVault.read({
                          householdId: work.household.householdId,
                          signalId: work.workId,
                          image: { assetId: selectedAssetId, mimeType },
                        });
                        const selectedImage: FamilyWorkSelectedImage = {
                          assetId: selectedAssetId,
                          signalId: work.workId,
                          workId: work.workId,
                          mimeType: read.mimeType,
                          filename: browserSelectedImageFilename(
                            operation.label,
                            selectedAssetId,
                            read.mimeType,
                          ),
                        };
                        return {
                          kind: "page" as const,
                          reason: "This exact browser image was already captured for the family result.",
                          url: "",
                          title: operation.label,
                          snapshot: "",
                          refCount: 0,
                          truncated: false,
                          screenshot: read,
                          selectedImage,
                        };
                      } catch {
                        // The exact selected image may not have reached durable storage before interruption.
                      }
                    }
                  }
                  if (
                    operation.kind === "download" &&
                    selectedFileAssetId &&
                    pendingCall.attempt > 1 &&
                    this.#imageVault
                  ) {
                    try {
                      const read = await this.#imageVault.readFileArtifactById({
                        householdId: work.household.householdId,
                        workId: work.workId,
                        artifactId: selectedFileAssetId,
                      });
                      const selectedFile: FamilyWorkSelectedFile = {
                        assetId: read.artifact.artifactId,
                        signalId: work.workId,
                        workId: read.artifact.workId,
                        mimeType: read.artifact.mimeType,
                        filename: read.artifact.filename,
                        byteLength: read.artifact.byteLength,
                        sha256: read.artifact.sha256,
                      };
                      return {
                        kind: "page" as const,
                        reason: "This exact browser file was already saved for the family result.",
                        url: "",
                        title: selectedFile.filename,
                        snapshot: "",
                        refCount: 0,
                        truncated: false,
                        selectedFile,
                      };
                    } catch {
                      // The exact file may not have reached durable storage before interruption.
                    }
                  }
                  const result = await browser.run(
                    {
                      householdId: work.household.householdId,
                      workId: work.workId,
                      ownerAdultId: familyWorkExecutionAdultId,
                      callId: pendingCall.callId,
                      attempt: pendingCall.attempt,
                      session: browserSession,
                      operation,
                      ...(uploadFile ? { uploadFile } : {}),
                    },
                    taskSignal,
                  );
                  browserSession = result.session;
                  if (operation.kind === "capture") {
                    if (!selectedAssetId || !result.observation.screenshot) {
                      throw new Error("Kernel did not return the selected browser image");
                    }
                    if (!this.#imageVault) {
                      throw new Error("Florence image persistence is not configured");
                    }
                    const stored = await this.#imageVault.store({
                      assetId: selectedAssetId,
                      householdId: work.household.householdId,
                      signalId: work.workId,
                      declaredMimeType: result.observation.screenshot.mimeType,
                      bytes: result.observation.screenshot.bytes,
                    });
                    await this.#imageVault.retain({
                      householdId: work.household.householdId,
                      signalId: work.workId,
                      image: stored.image,
                      claimId: work.workId,
                      now: this.#now(),
                    });
                    const selectedImage: FamilyWorkSelectedImage = {
                      assetId: stored.image.assetId,
                      signalId: work.workId,
                      workId: work.workId,
                      mimeType: stored.image.mimeType,
                      filename: browserSelectedImageFilename(
                        operation.label,
                        stored.image.assetId,
                        stored.image.mimeType,
                      ),
                    };
                    return { ...result.observation, selectedImage };
                  }
                  if (operation.kind === "download") {
                    if (!selectedFileAssetId || !result.observation.downloadedFile) {
                      throw new Error("Kernel did not return the selected browser file");
                    }
                    if (!this.#imageVault) {
                      throw new Error("Florence file persistence is not configured");
                    }
                    const stored = await this.#imageVault.storeFileArtifact({
                      artifactId: selectedFileAssetId,
                      householdId: work.household.householdId,
                      workId: work.workId,
                      callId: pendingCall.callId,
                      filename: result.observation.downloadedFile.filename,
                      declaredMimeType: result.observation.downloadedFile.mimeType,
                      bytes: result.observation.downloadedFile.bytes,
                    });
                    const selectedFile: FamilyWorkSelectedFile = {
                      assetId: stored.artifactId,
                      signalId: work.workId,
                      workId: stored.workId,
                      mimeType: stored.mimeType,
                      filename: stored.filename,
                      byteLength: stored.byteLength,
                      sha256: stored.sha256,
                    };
                    return { ...result.observation, selectedFile };
                  }
                  return result.observation;
                },
              }
            : {}),
          ...(familyWorkCalendarConnection || familyWorkFamilyCalendarCredentials.length > 0
            ? {
                listCalendars: listFamilyWorkCalendars,
                readCalendarWindow: readFamilyWorkCalendarWindow,
              }
            : {}),
          readCurrentImage: async ({ assetId, mimeType }) => {
            const image = familyWorkOriginImages.find(
              (candidate) => candidate.assetId === assetId && candidate.mimeType === mimeType,
            );
            if (!image) throw new Error("The image is not attached to the durable task request");
            if (!this.#imageVault) throw new Error("Florence image reading is not configured");
            return this.#imageVault.read({
              householdId: work.household.householdId,
              signalId: image.signalId,
              image: { assetId, mimeType },
            });
          },
          readCurrentPdf: async ({ documentId, filename, mimeType, contentDigest }) => {
            const document = familyWorkOriginDocuments.find(
              (candidate) =>
                candidate.id === documentId &&
                candidate.filename === filename &&
                candidate.mimeType === mimeType &&
                candidate.contentDigest === contentDigest,
            );
            if (!document) throw new Error("The PDF is not attached to the durable task request");
            if (!this.#imageVault) throw new Error("Florence PDF reading is not configured");
            return this.#imageVault.openPdf({
              documentId: document.id,
              householdId: work.household.householdId,
              signalId: document.parentSourceId,
              filename: document.filename,
              mimeType: document.mimeType,
              contentDigest: document.contentDigest,
              contentEnvelope: document.contentEnvelope,
              discardAfter: document.discardAfter,
              now: this.#now(),
            });
          },
          ...(google && familyWorkGoogleAdultId && familyWorkWorkspaceConnection
            ? {
                runGoogleWorkspace: (operation: GoogleWorkspaceOperation, taskSignal?: AbortSignal) =>
                  google.runWorkspace(
                    {
                      householdId: work.household.householdId,
                      ownerAdultId: familyWorkGoogleAdultId,
                      connectionId: familyWorkWorkspaceConnection.connectionId,
                      operation,
                    },
                    taskSignal,
                  ),
                readWorkspaceGmailSource: async (identity: {
                  messageId: string;
                  threadId: string;
                  historyId: string;
                }) => {
                  const message = await google.readGmailMessage({
                    householdId: work.household.householdId,
                    ownerAdultId: familyWorkGoogleAdultId,
                    connectionId: familyWorkWorkspaceConnection.connectionId,
                    ...identity,
                  });
                  return indexFamilyWorkGmailMessage(message, familyWorkWorkspaceConnection.connectionId);
                },
              }
            : {}),
        },
        signal,
      );
      signal.throwIfAborted();
      const settledAt = this.#now().toISOString();
      if (step.kind === "continue") {
        const settlement = await this.#store.settleFamilyWorkClaim({
          workId: work.workId,
          generation: work.generation,
          claimId: work.claimId,
          settledAt,
          result: {
            type: "continue",
            state: { ...step.state, browserSession },
            nextCheckAt: later(new Date(settledAt), step.nextCheckDelayMs),
            ...(step.progressText ? { progressText: step.progressText } : {}),
          },
        });
        if (settlement === "stale") {
          await closeUncheckpointedBrowserSession();
          if (familyWorkWasExplicitlyCancelled(signal)) await stopUncheckpointedPhoneCall(true);
          else await adoptOrStopUncheckpointedPhoneCall();
          await adoptUncheckpointedTextMessage();
        }
      } else if (step.kind === "deferred") {
        const deferredBrowserSession = browserSession;
        const resumeAt = Date.parse(step.resumeAt) < Date.parse(settledAt) ? settledAt : step.resumeAt;
        const settlement = await this.#store.settleFamilyWorkClaim({
          workId: work.workId,
          generation: work.generation,
          claimId: work.claimId,
          settledAt,
          result: {
            type: "continue",
            state: { ...step.state, browserSession: null },
            nextCheckAt: resumeAt,
            ...(step.progressText ? { progressText: step.progressText } : {}),
          },
        });
        if (settlement === "settled" && deferredBrowserSession) {
          await closeBrowserSession(deferredBrowserSession);
        } else if (settlement === "stale") {
          await closeUncheckpointedBrowserSession();
          if (familyWorkWasExplicitlyCancelled(signal)) await stopUncheckpointedPhoneCall(true);
          else await adoptOrStopUncheckpointedPhoneCall();
          await adoptUncheckpointedTextMessage();
        }
      } else if (step.kind === "participant_waiting") {
        const settlement = await this.#store.settleFamilyWorkClaim({
          workId: work.workId,
          generation: work.generation,
          claimId: work.claimId,
          settledAt,
          result: {
            type: "participant_waiting",
            state: { ...step.state, browserSession },
          },
        });
        if (settlement === "stale") {
          await closeUncheckpointedBrowserSession();
          await adoptOrStopUncheckpointedPhoneCall();
          await adoptUncheckpointedTextMessage();
        }
      } else if (step.kind === "waiting") {
        const settlement = await this.#store.settleFamilyWorkClaim({
          workId: work.workId,
          generation: work.generation,
          claimId: work.claimId,
          settledAt,
          result: {
            type: "waiting",
            state: { ...step.state, browserSession },
            question: step.question,
          },
        });
        if (settlement === "stale") {
          await closeUncheckpointedBrowserSession();
          await adoptOrStopUncheckpointedPhoneCall();
          await adoptUncheckpointedTextMessage();
        }
      } else {
        const terminalBrowserSession = browserSession;
        const terminalPhoneCall = step.state.activePhoneCall;
        if (terminalPhoneCall) {
          const stopped = await this.#stopFamilyWorkPhoneCall(work.workId, terminalPhoneCall);
          if (!stopped) {
            await this.#store.settleFamilyWorkClaim({
              workId: work.workId,
              generation: work.generation,
              claimId: work.claimId,
              settledAt,
              result: {
                type: "retry",
                state: {
                  ...work.state,
                  claim: null,
                  activePhoneCall: terminalPhoneCall,
                  browserSession,
                },
                retryAt: later(new Date(settledAt), 5_000),
                error: "Florence is still stopping the active provider call before finishing this task",
              },
            });
            return;
          }
        }
        const settlement = await this.#store.settleFamilyWorkClaim({
          workId: work.workId,
          generation: work.generation,
          claimId: work.claimId,
          settledAt,
          result: {
            type: "terminal",
            state: {
              ...step.state,
              browserSession: null,
              activePhoneCall: null,
              terminal: step.state.terminal
                ? {
                    ...step.state.terminal,
                    docket: step.docket ?? step.state.terminal.docket ?? null,
                  }
                : null,
            },
            terminalText: step.text,
            completionEvidenceOutputs: step.completionEvidenceOutputs,
          },
        });
        if (settlement === "settled" && terminalBrowserSession) {
          await closeBrowserSession(terminalBrowserSession);
        } else if (settlement === "stale") {
          await closeUncheckpointedBrowserSession();
          await adoptOrStopUncheckpointedPhoneCall();
          await adoptUncheckpointedTextMessage();
          if (familyWorkWasExplicitlyCancelled(signal) && terminalBrowserSession) {
            await closeBrowserSession(terminalBrowserSession);
          }
        }
      }
    } catch (error) {
      if (signal.aborted) {
        const cancelled = familyWorkWasExplicitlyCancelled(signal);
        if (cancelled && browserSession) await closeBrowserSession(browserSession);
        if (cancelled) await stopUncheckpointedPhoneCall(true);
        if (cancelled) await adoptUncheckpointedTextMessage();
        const interruptedAt = this.#now();
        const retryState = await latestClaimedFamilyWorkState();
        const settlement = await this.#store.settleFamilyWorkClaim({
          workId: work.workId,
          generation: work.generation,
          claimId: work.claimId,
          settledAt: interruptedAt.toISOString(),
          result: {
            type: "retry",
            state: {
              ...retryState,
              claim: null,
              activePhoneCall: cancelled
                ? work.state.activePhoneCall
                : (uncheckpointedPhoneCall ?? work.state.activePhoneCall),
              activeTextMessage: uncheckpointedTextMessage ?? work.state.activeTextMessage,
              browserSession: cancelled ? null : browserSession,
            },
            retryAt: later(interruptedAt, 1),
            error: "The task step was interrupted before its checkpoint and will resume",
          },
        });
        if (settlement === "stale") {
          await closeUncheckpointedBrowserSession();
          if (!cancelled) await adoptOrStopUncheckpointedPhoneCall();
          await adoptUncheckpointedTextMessage();
        }
        return;
      }
      const errorCheckpoint =
        error instanceof FlorenceReasonerError &&
        error.familyWorkCheckpoint?.generation === work.generation &&
        error.familyWorkCheckpoint.claim?.claimId === work.claimId
          ? error.familyWorkCheckpoint
          : null;
      const retryState = errorCheckpoint ?? (await latestClaimedFamilyWorkState());
      const settlement = await this.#store.settleFamilyWorkClaim({
        workId: work.workId,
        generation: work.generation,
        claimId: work.claimId,
        settledAt: this.#now().toISOString(),
        result: {
          type: "retry",
          state: {
            ...retryState,
            claim: null,
            activePhoneCall: uncheckpointedPhoneCall ?? work.state.activePhoneCall,
            activeTextMessage: uncheckpointedTextMessage ?? work.state.activeTextMessage,
            browserSession,
          },
          retryAt: later(this.#now(), RETRY_MS),
          error: errorText(error),
        },
      });
      if (settlement === "stale") {
        await closeUncheckpointedBrowserSession();
        await adoptOrStopUncheckpointedPhoneCall();
        await adoptUncheckpointedTextMessage();
      }
    }
  }

  async #executeHouseholdNextAction(
    work: Extract<DueProactiveWork, { kind: "household_next_action" }>,
  ): Promise<void> {
    if (!this.#google || !this.#reasoner) {
      await this.#store.retryHouseholdNextAction({
        workId: work.workId,
        revision: work.revision,
        claimId: work.claimId,
        retryAt: later(this.#now(), RETRY_MS),
        error: "Florence's household next-action pass is not configured",
      });
      return;
    }
    const currentTime = this.#now().toISOString();
    try {
      const [household, docket, visibleWork, familyCalendar] = await Promise.all([
        this.#store.readHousehold({ householdId: work.household.householdId }),
        this.#store.readHouseholdDocket({
          householdId: work.household.householdId,
          limit: null,
          now: currentTime,
        }),
        this.#store.readVisibleActiveFamilyWork({
          householdId: work.household.householdId,
          viewerAdultId: work.executionAdultId,
          now: currentTime,
        }),
        this.#google.readInitialCalendarReview({
          householdId: work.household.householdId,
          ownerAdultId: work.executionAdultId,
          connectionId: work.connectionId,
          calendarId: work.familyCalendarId,
          currentTime,
          // This is Google's maximum transport page size; readInitialCalendarReview exhausts pages.
          limit: 50,
        }),
      ]);
      if (!household) throw new Error("The household next-action family is unavailable");
      if (familyCalendar.status !== "complete" || !familyCalendar.cursor) {
        throw new Error("The household next-action Family Calendar window is incomplete");
      }
      const householdFacts = household.facts.filter((fact) => fact.visibility === "household");
      const activeWork = visibleWork.filter((item) => item.visibility === "household");
      const familyCalendarEvents = familyCalendar.events.map((event) =>
        event.intervalKind === "timed"
          ? {
              intervalKind: event.intervalKind,
              title: event.title,
              startsAt: event.startsAt,
              endsAt: event.endsAt,
              timeZone: event.timeZone,
            }
          : {
              intervalKind: event.intervalKind,
              title: event.title,
              startDate: event.startDate,
              endDate: event.endDate,
            },
      );
      const stateDigest = householdNextActionStateDigest({
        docket,
        activeWork,
        familyCalendar: familyCalendarEvents,
        facts: householdFacts,
      });
      if (stateDigest === work.lastStateDigest) {
        await this.#store.completeHouseholdNextAction({
          workId: work.workId,
          revision: work.revision,
          claimId: work.claimId,
          stateDigest,
          message: null,
          nextJob: null,
          executionAdultId: work.executionAdultId,
          occurredAt: currentTime,
        });
        return;
      }
      const vault = new VaultRecall(householdFacts);
      const input: FlorenceHouseholdNextActionInput = {
        currentTime,
        familyProfile: initialFamilyProfile(work.household),
        householdDocket: {
          totalItems: docket.totalItems,
          items: docket.items.map(({ visibility: _visibility, ...item }) => item),
        },
        activeWork: activeWork.map(({ visibility: _visibility, candidateIds, ...item }) => ({
          ...item,
          candidateIds: [...candidateIds],
        })),
        lastInterruption: work.lastInterruption
          ? { ...work.lastInterruption, candidateIds: [...work.lastInterruption.candidateIds] }
          : null,
        familyCalendar: {
          timeMin: currentTime,
          timeMax: new Date(Date.parse(currentTime) + 21 * 24 * 60 * 60_000).toISOString(),
          events: familyCalendarEvents,
        },
      };
      const decision = await this.#reasoner.decideHouseholdNextAction(input, {
        searchVault: async (request) => vault.search(request),
        readVault: async (request) => vault.read(request),
      });
      await this.#store.completeHouseholdNextAction({
        workId: work.workId,
        revision: work.revision,
        claimId: work.claimId,
        stateDigest,
        message: decision.message,
        nextJob: decision.nextJob,
        executionAdultId: work.executionAdultId,
        occurredAt: this.#now().toISOString(),
      });
    } catch (error) {
      await this.#store.retryHouseholdNextAction({
        workId: work.workId,
        revision: work.revision,
        claimId: work.claimId,
        retryAt: later(this.#now(), RETRY_MS),
        error: errorText(error),
      });
    }
  }

  async #executeProactiveWork(work: Exclude<DueProactiveWork, { kind: "family_task" }>): Promise<void> {
    if (work.kind === "household_next_action") {
      await this.#executeHouseholdNextAction(work);
      return;
    }
    if (work.kind === "cancelled_family_task") {
      const stopped = await this.#stopFamilyWorkPhoneCall(work.workId, work.activePhoneCall);
      if (stopped) {
        await this.#store.clearCancelledFamilyWorkPhoneCall(work.workId, work.activePhoneCall);
      } else {
        await this.#store.retryCancelledFamilyWorkPhoneCall(
          work.workId,
          work.activePhoneCall,
          later(this.#now(), RETRY_MS),
          "Florence is still stopping the cancelled task's provider call",
        );
      }
      return;
    }
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
        const busyIntervals = unionBusyIntervals(
          [...personalCalendars, familyCalendar].flatMap((calendar) =>
            !calendar || calendar.status === "unavailable"
              ? []
              : calendar.events
                  .map((event) => calendarWindowBounds(event, work.household.timeZone))
                  .filter((event) => event.endsAt > currentTime),
          ),
        );
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
        // The cursor-backed personal Google poll owns exhaustive new-mail discovery. A timed
        // monitor review rereads only the exact Gmail resources already linked to this monitor.
        const gmailMessages =
          work.visibility === "private"
            ? await Promise.all(
                work.gmailSourceAnchors.map((anchor) =>
                  google.readGmailMessage({
                    householdId: work.household.householdId,
                    ownerAdultId: work.adultId,
                    connectionId: work.connectionId,
                    messageId: anchor.messageId,
                    threadId: anchor.threadId,
                    historyId: anchor.historyId,
                  }),
                ),
              )
            : [];
        const calendar = await this.#google.readInitialCalendarReview({
          householdId: work.household.householdId,
          ownerAdultId: work.adultId,
          connectionId: work.connectionId,
          calendarId: work.calendarId,
          currentTime,
          limit: 50,
        });
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
            text: message.text,
            textStatus: message.textStatus,
            attachments: message.attachments,
            attachmentsStatus: message.attachmentsStatus,
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
                  ...calendarEvidenceContentDetails(event),
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
            status: "complete",
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
          householdDocket: householdConclusion
            ? {
                owner: householdConclusion.owner,
                nextAction: householdConclusion.nextAction,
                waitingOn: householdConclusion.waitingOn,
                completionCondition: householdConclusion.completionCondition,
              }
            : null,
          householdCategory: householdConclusion?.category ?? null,
          householdNeedsAnswer: householdConclusion?.needsAnswer ?? false,
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
          reviewStartedAt: currentTime,
          deliverNotBefore: proactiveDeliveryAt(
            completedAt,
            work.household.timeZone,
            result.urgency === "now",
          ).toISOString(),
          occurredAt: completedAt.toISOString(),
        });
        return;
      }

      const household = await this.#store.readHousehold({
        householdId: work.household.householdId,
        ...(work.visibility === "private" ? { viewerAdultId: work.adultId } : {}),
      });
      if (!household) throw new Error("The proactive Google review household is unavailable");

      const gmailMessages: GmailEvidence[] = [];
      let removedGmailSourceIds: string[] = [];
      const removedCalendarSourceIds: string[] = [];
      const dismissedCalendarSourceIds = new Set<string>();
      let nextGmailCursor: GoogleGmailCursor | null = null;
      let gmailStatus: "complete" | "truncated" | "unavailable" = "unavailable";
      if (work.kind === "personal_google_poll") {
        if (!privateGoogleCorpusCursorIsCurrent(work.gmailCursor, work.calendarCursor, work.connectionId)) {
          await this.#store.restartPersonalGooglePollAsInitialScan({
            workId: work.workId,
            connectionId: work.connectionId,
            now: currentTime,
          });
          return;
        }
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
            const sourceId = calendarEvidenceSourceId(
              work.household.householdId,
              work.connectionId,
              target.calendarId,
              event.providerEventId,
            );
            if (calendarChangeRepresentsRemoval(event)) {
              removedCalendarSourceIds.push(sourceId);
              continue;
            }
            calendarEvents.push({ calendarId: target.calendarId, event });
            if (!calendarChangeFallsInsideWindow(event, currentTime, calendarTimeMax)) {
              dismissedCalendarSourceIds.add(sourceId);
            }
          }
          nextTargets.push({ target: googleBaselineTarget(target), provider: changes.cursor });
        }
        nextCalendarCursor = JSON.stringify({
          kind: "calendar_account_cursor_v1",
          scannerVersion: PRIVATE_GOOGLE_CORPUS_VERSION,
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
            const sourceId = calendarEvidenceSourceId(
              work.household.householdId,
              work.connectionId,
              work.calendarId,
              event.providerEventId,
            );
            if (calendarChangeRepresentsRemoval(event)) {
              removedCalendarSourceIds.push(sourceId);
              continue;
            }
            calendarEvents.push({ calendarId: work.calendarId, event });
            if (!calendarChangeFallsInsideWindow(event, currentTime, calendarTimeMax)) {
              dismissedCalendarSourceIds.add(sourceId);
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
          text: message.text,
          textStatus: message.textStatus,
          attachments: message.attachments,
          attachmentsStatus: message.attachmentsStatus,
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
          ...calendarEvidenceContentDetails(event),
        });
        googleEvidence.set(source.id, source);
        return privateCalendarEvidence(
          source,
          event,
          work.visibility === "household" ? "shared" : "adult_private",
        );
      });
      const reviewableCalendarSources = calendarSources.filter(
        (source) => !dismissedCalendarSourceIds.has(source.sourceId),
      );
      if (gmailSources.length === 0 && reviewableCalendarSources.length === 0) {
        const completedAt = this.#now().toISOString();
        if (work.kind === "personal_google_poll" && !nextGmailCursor) {
          throw new Error("The personal Google poll did not advance its Gmail cursor");
        }
        await this.#store.completeGooglePoll({
          workId: work.workId,
          gmailCursor: nextGmailCursor ? googleGmailPollCursor(nextGmailCursor, work.connectionId) : null,
          calendarCursor: nextCalendarCursor,
          googleEvidence: [...googleEvidence.values()],
          reviewedGoogleSources: [...googleEvidence.keys()].map((sourceId) => ({
            sourceId,
            disposition: "dismissed" as const,
          })),
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
      for (const batch of privateGoogleModelBatches([...gmailSources, ...reviewableCalendarSources])) {
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
              memory: proactiveMemoryContext(household.facts, privateGoogleMemoryQuery(batch)),
              currentFacts:
                work.kind === "personal_google_poll"
                  ? relevantGoogleFacts(work.currentFacts, privateGoogleMemoryQuery(batch))
                  : [],
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
          (current.statement !== fact.statement ||
            current.familyRelevance !== fact.familyRelevance ||
            !sameMemoryPresentation(current.memory, fact.memory))
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
      const proactiveNextJobs = decisions.flatMap((candidate, decisionIndex) => {
        const nextJob = candidate.nextJob ?? null;
        const finding = nextJob ? candidate.findings[nextJob.findingIndex] : null;
        return nextJob && finding ? [{ nextJob, finding, decisionIndex }] : [];
      });
      const proactiveUrgencyRank = { now: 0, soon: 1, watch: 2 } as const;
      const decision = {
        findings: decisions.flatMap((candidate) => candidate.findings),
        facts: [...mergedFacts.values()],
      };
      const retainedFindings = decision.findings.filter(
        (finding) => work.kind === "personal_google_poll" || finding.materialChange,
      );
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
        const proposedNextJob = proactiveNextJobs.find((candidate) => candidate.finding === finding) ?? null;
        const nextJobCandidate =
          proposedNextJob &&
          (proposedNextJob.nextJob.visibility === "private"
            ? work.visibility === "private" && finding.privateDetail.trim().length > 0
            : sharing.conclusion !== null)
            ? proposedNextJob
            : null;
        return {
          ...finding,
          householdConclusion: sharing.conclusion,
          familyCalendar: sharing.familyCalendar,
          nextJobCandidate,
        };
      });
      const selectedNextJobFindingIndex = storeFindings
        .flatMap((finding, index) =>
          finding.nextJobCandidate ? [{ finding, index, candidate: finding.nextJobCandidate }] : [],
        )
        .sort(
          (left, right) =>
            proactiveUrgencyRank[left.finding.urgency] - proactiveUrgencyRank[right.finding.urgency] ||
            (left.finding.dueAt ? Date.parse(left.finding.dueAt) : Number.POSITIVE_INFINITY) -
              (right.finding.dueAt ? Date.parse(right.finding.dueAt) : Number.POSITIVE_INFINITY) ||
            left.candidate.decisionIndex - right.candidate.decisionIndex,
        )[0]?.index;
      const retainedFacts = decision.facts.filter((fact) => fact.familyRelevance !== "owner_private");
      let deliveries = storeFindings
        .map((finding, index) => ({ finding, index }))
        .filter(
          ({ finding }) =>
            work.visibility === "private" ||
            finding.householdConclusion !== null ||
            finding.monitor !== null ||
            (finding.familyCalendar ?? null) !== null,
        )
        .map(
          ({ finding, index }): ProactiveDelivery => ({
            privateDetail:
              work.visibility === "private"
                ? privateGoogleFindingDetail({
                    fallback: finding.privateDetail,
                    sourceIds: finding.sourceIds,
                    calendarSources,
                    timeZone: work.household.timeZone,
                  })
                : null,
            privateDocket: work.visibility === "private" ? finding.privateDocket : null,
            householdConclusion: finding.householdConclusion?.summary ?? null,
            householdDocket: finding.householdConclusion
              ? {
                  owner: finding.householdConclusion.owner,
                  nextAction: finding.householdConclusion.nextAction,
                  waitingOn: finding.householdConclusion.waitingOn,
                  completionCondition: finding.householdConclusion.completionCondition,
                }
              : null,
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
            nextJob:
              index === selectedNextJobFindingIndex && finding.nextJobCandidate
                ? {
                    objective: finding.nextJobCandidate.nextJob.objective,
                    visibility: finding.nextJobCandidate.nextJob.visibility,
                  }
                : null,
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
                Number(right.delivery.nextJob !== null) - Number(left.delivery.nextJob !== null) ||
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
        executionAdultId: work.adultId,
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

  async #readHouseholdAvailability(input: {
    household: HouseholdRecord;
    timeMin: string;
    timeMax: string;
  }): Promise<FlorenceHouseholdAvailabilityRead> {
    const timeMinMs = Date.parse(input.timeMin);
    const timeMaxMs = Date.parse(input.timeMax);
    if (!Number.isFinite(timeMinMs) || !Number.isFinite(timeMaxMs) || timeMaxMs <= timeMinMs) {
      throw new Error("Household availability requires a valid half-open time window");
    }

    const google = this.#google;
    const adults = input.household.members
      .filter((member) => member.kind === "adult" && member.status === "verified")
      .sort((left, right) => (left.adultSlot ?? 3) - (right.adultSlot ?? 3));
    const readAdult = async (
      adult: FamilyMemberRecord,
    ): Promise<FlorenceHouseholdAvailabilityRead["participants"][number]> => {
      if (adult.preferences.privateConflictBusySharingEnabled !== true) {
        return { adultName: adult.displayName, coverage: "not_shared", busyIntervals: [] };
      }
      const activeConnections = input.household.googleConnections.filter(
        (connection) => connection.ownerAdultId === adult.id && connection.status === "active",
      );
      if (activeConnections.length === 0) {
        return { adultName: adult.displayName, coverage: "not_connected", busyIntervals: [] };
      }
      if (!google) {
        return { adultName: adult.displayName, coverage: "unavailable", busyIntervals: [] };
      }
      const readableConnections = activeConnections.filter((connection) =>
        GOOGLE_CALENDAR_READ_SCOPES.every((scope) => connection.grantedScopes.includes(scope)),
      );
      if (readableConnections.length === 0) {
        return { adultName: adult.displayName, coverage: "unavailable", busyIntervals: [] };
      }
      const hasUnreadableActiveConnection = readableConnections.length !== activeConnections.length;

      const settled = await Promise.allSettled(
        readableConnections.map((connection) =>
          readTitleFreeAvailabilityRange({
            timeMin: input.timeMin,
            timeMax: input.timeMax,
            readPage: ({ timeMin, timeMax, cursor }) =>
              google.readPersonalCalendarWindow({
                householdId: input.household.id,
                ownerAdultId: adult.id,
                connectionId: connection.connectionId,
                excludedFamilyCalendarId: input.household.familyCalendarId,
                timeMin,
                timeMax,
                limit: 50,
                ...(cursor === null ? {} : { cursor }),
              }),
          }),
        ),
      );
      const reads = settled.flatMap((result) => (result.status === "fulfilled" ? [result.value] : []));
      const busyIntervals = unionBusyIntervals(reads.flatMap((read) => read.busyIntervals));
      const allComplete =
        !hasUnreadableActiveConnection &&
        settled.length > 0 &&
        settled.every((result) => result.status === "fulfilled" && result.value.coverage === "complete");
      const anyReliable = reads.some((read) => read.coverage !== "unavailable");
      return {
        adultName: adult.displayName,
        coverage: allComplete ? "complete" : anyReliable ? "partial" : "unavailable",
        busyIntervals,
      };
    };

    const readFamilyCalendar = async (): Promise<FlorenceHouseholdAvailabilityRead["familyCalendar"]> => {
      const calendarId = input.household.familyCalendarId;
      if (!calendarId) return { coverage: "not_configured", busyIntervals: [] };
      if (!google) return { coverage: "unavailable", busyIntervals: [] };
      const credentials = await this.#store.readActiveFamilyCalendarCredentials({
        householdId: input.household.id,
      });
      if (credentials.length === 0) return { coverage: "unavailable", busyIntervals: [] };
      const read = await readTitleFreeAvailabilityRange({
        timeMin: input.timeMin,
        timeMax: input.timeMax,
        readPage: async ({ timeMin, timeMax, cursor }) =>
          (
            await this.#readExactFamilyCalendarWindow({
              householdId: input.household.id,
              calendarId,
              timeMin,
              timeMax,
              limit: 50,
              cursor,
              credentials,
            })
          ).read,
      });
      return { coverage: read.coverage, busyIntervals: [...read.busyIntervals] };
    };

    const [participantSettlements, familySettlements] = await Promise.all([
      Promise.allSettled(adults.map(readAdult)),
      Promise.allSettled([readFamilyCalendar()]),
    ]);
    const participants = participantSettlements.map((settlement, index) => {
      if (settlement.status === "fulfilled") return settlement.value;
      const adult = adults[index];
      if (!adult) throw new Error("Household availability lost an adult result");
      return { adultName: adult.displayName, coverage: "unavailable" as const, busyIntervals: [] };
    });
    const familySettlement = familySettlements[0];
    const familyCalendar =
      familySettlement?.status === "fulfilled"
        ? familySettlement.value
        : { coverage: "unavailable" as const, busyIntervals: [] };
    const mergedBusyIntervals = unionBusyIntervals([
      ...participants.flatMap((participant) => participant.busyIntervals),
      ...familyCalendar.busyIntervals,
    ]);
    const allComplete =
      participants.every((participant) => participant.coverage === "complete") &&
      familyCalendar.coverage === "complete";
    const anyReliable =
      participants.some(
        (participant) => participant.coverage === "complete" || participant.coverage === "partial",
      ) ||
      familyCalendar.coverage === "complete" ||
      familyCalendar.coverage === "partial";
    return {
      status: allComplete ? "complete" : anyReliable ? "partial" : "unavailable",
      timeMin: input.timeMin,
      timeMax: input.timeMax,
      timeZone: input.household.timeZone,
      participants,
      familyCalendar,
      mergedBusyIntervals,
    };
  }

  async #readExactFamilyCalendarCatalog(input: {
    householdId: string;
    calendarId: string;
    credentials: readonly ActiveFamilyCalendarCredential[];
  }): Promise<
    Readonly<{
      read: GooglePersonalCalendarCatalogRead;
      credential: ActiveFamilyCalendarCredential;
    }>
  > {
    const google = this.#google;
    const firstCredential = input.credentials[0];
    if (!google || !firstCredential) throw new Error("The Family Calendar credential is unavailable");
    let lastAvailable: Readonly<{
      read: GooglePersonalCalendarCatalogRead;
      credential: ActiveFamilyCalendarCredential;
    }> | null = null;
    let lastUnavailable: Readonly<{
      read: GooglePersonalCalendarCatalogRead;
      credential: ActiveFamilyCalendarCredential;
    }> | null = null;
    for (const credential of input.credentials) {
      try {
        const read = await google.readExactCalendarCatalog({
          householdId: input.householdId,
          ownerAdultId: credential.ownerAdultId,
          connectionId: credential.connectionId,
          calendarId: input.calendarId,
        });
        if (read.status !== "unavailable") {
          if (read.calendars[0]?.eventCoverage === "readable") return { read, credential };
          lastAvailable = { read, credential };
          continue;
        }
        lastUnavailable = { read, credential };
      } catch (error) {
        if (error instanceof GoogleConnectionError && error.code === "credential_invalid_grant") continue;
        throw error;
      }
    }
    if (lastAvailable) return lastAvailable;
    if (lastUnavailable) return lastUnavailable;
    return {
      read: await google.readExactCalendarCatalog({
        householdId: input.householdId,
        ownerAdultId: firstCredential.ownerAdultId,
        connectionId: firstCredential.connectionId,
        calendarId: input.calendarId,
      }),
      credential: firstCredential,
    };
  }

  async #readExactFamilyCalendarWindow(input: {
    householdId: string;
    calendarId: string;
    timeMin: string;
    timeMax: string;
    limit: number;
    cursor: string | null;
    credentials: readonly ActiveFamilyCalendarCredential[];
  }): Promise<
    Readonly<{
      read: GooglePersonalCalendarWindowRead;
      credential: ActiveFamilyCalendarCredential;
    }>
  > {
    const google = this.#google;
    const firstCredential = input.credentials[0];
    if (!google || !firstCredential) throw new Error("The Family Calendar credential is unavailable");
    let lastIncomplete: Readonly<{
      read: GooglePersonalCalendarWindowRead;
      credential: ActiveFamilyCalendarCredential;
    }> | null = null;
    for (const credential of input.credentials) {
      try {
        const catalog = await google.readExactCalendarCatalog({
          householdId: input.householdId,
          ownerAdultId: credential.ownerAdultId,
          connectionId: credential.connectionId,
          calendarId: input.calendarId,
        });
        if (catalog.status === "unavailable") continue;
        const read = await google.readExactCalendarWindow({
          householdId: input.householdId,
          ownerAdultId: credential.ownerAdultId,
          connectionId: credential.connectionId,
          calendarId: input.calendarId,
          timeMin: input.timeMin,
          timeMax: input.timeMax,
          limit: input.limit,
          ...(input.cursor === null ? {} : { cursor: input.cursor }),
        });
        const exactTarget = read.calendars.find((calendar) => calendar.calendarId === input.calendarId);
        if (exactTarget?.status === "complete" && exactTarget.accessRole !== "freeBusyReader") {
          return { read, credential };
        }
        lastIncomplete = { read, credential };
      } catch (error) {
        if (error instanceof GoogleConnectionError && error.code === "credential_invalid_grant") continue;
        throw error;
      }
    }
    if (lastIncomplete) return lastIncomplete;
    return {
      read: await google.readExactCalendarWindow({
        householdId: input.householdId,
        ownerAdultId: firstCredential.ownerAdultId,
        connectionId: firstCredential.connectionId,
        calendarId: input.calendarId,
        timeMin: input.timeMin,
        timeMax: input.timeMax,
        limit: input.limit,
        ...(input.cursor === null ? {} : { cursor: input.cursor }),
      }),
      credential: firstCredential,
    };
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

  #beginInboundPresence(input: {
    providerConversationId: string;
    expectedAuthority: Parameters<LinqClient["setTyping"]>[0]["expectedAuthority"];
    observedAuthority: NonNullable<Parameters<LinqClient["setTyping"]>[0]["observedAuthority"]>;
  }): InboundPresence {
    let stopped = false;
    let refreshTimer: ReturnType<typeof setTimeout> | null = null;
    let refreshInFlight: Promise<boolean> | null = null;
    const typingInput = { ...input, active: true } as const;
    const refresh = (): void => {
      if (stopped) return;
      const request = this.#setTyping(typingInput);
      refreshInFlight = request;
      void request.finally(() => {
        if (refreshInFlight === request) refreshInFlight = null;
        if (!stopped) refreshTimer = setTimeout(refresh, LINQ_TYPING_REFRESH_MS);
      });
    };
    refresh();
    void this.#linq.markRead(input).catch(() => false);
    return {
      stop: () => {
        if (stopped) return;
        stopped = true;
        if (refreshTimer) clearTimeout(refreshTimer);
        const finish = refreshInFlight ?? Promise.resolve(false);
        void finish.finally(() => this.#setTyping({ ...input, active: false }));
      },
    };
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
  docket: NonNullable<WorkspaceView["vault"]>["docket"],
  activeWork: readonly NonNullable<WorkspaceView["vault"]>["activeWork"][number][],
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
          docket,
          activeWork: [...activeWork],
          facts: household.facts.flatMap((fact) => {
            const source = fact.sources[0];
            if (fact.kind === "address" || fact.kind === "phone") return [];
            return [
              {
                id: fact.id,
                statement: factStatement(fact),
                ...vaultMemoryFields(fact),
                files: decodeFactFileArtifacts(fact.value).map(
                  ({ workId: _workId, ...artifact }) => artifact,
                ),
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

async function workspaceDocket(
  store: PostgresFlorenceStore,
  householdId: string,
  adultId: string,
  now: string,
): Promise<NonNullable<WorkspaceView["vault"]>["docket"]> {
  const [householdDocket, visibleDocket] = await Promise.all([
    store.readHouseholdDocket({ householdId, limit: null, now }),
    store.readHouseholdDocket({ householdId, viewerAdultId: adultId, limit: null, now }),
  ]);
  const householdCandidateIds = new Set(householdDocket.items.map((item) => item.candidateId));
  return {
    totalItems: visibleDocket.totalItems,
    items: visibleDocket.items.map((item) => ({
      ...item,
      visibility: householdCandidateIds.has(item.candidateId) ? "household" : "private",
    })),
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

function proactiveMemoryContext(
  facts: readonly FactRecord[],
  query: MemorySearchContext,
): {
  slot: string;
  label: string;
  text: string;
}[] {
  const factsById = new Map(facts.map((fact) => [fact.id, fact]));
  return selectVisibleMemorySources(representativeMemorySources(memorySources(facts)), query).flatMap(
    (source) => {
      const fact = source.recordId ? factsById.get(source.recordId) : null;
      if (!fact) return [];
      const presentation = vaultMemoryFields(fact);
      return [
        {
          slot: fact.slot,
          label: presentation.title ?? fact.label,
          text: vaultMemoryText(fact, presentation),
        },
      ];
    },
  );
}

function relevantGoogleFacts(
  facts: readonly GoogleStableFactContext[],
  query: MemorySearchContext,
): GoogleStableFactContext[] {
  const bySlot = new Map(facts.map((fact) => [fact.slot, fact]));
  const sources: FlorenceSource[] = [...bySlot.values()].map((fact) => ({
    sourceId: deterministicUuid(`google-stable-fact-context\0${fact.slot}`),
    recordId: fact.slot,
    kind: "memory",
    visibility: "adult_private",
    label: fact.memory.title ?? fact.statement,
    occurredAt: null,
    text: memoryPresentationText(fact.statement, fact.memory),
  }));
  return selectVisibleMemorySources(sources, query).flatMap((source) => {
    const fact = source.recordId ? bySlot.get(source.recordId) : null;
    return fact ? [fact] : [];
  });
}

function memorySources(facts: readonly FactRecord[]): FlorenceSource[] {
  return facts.flatMap((fact) => {
    const presentation = vaultMemoryFields(fact);
    const sources = fact.sources.length > 0 ? fact.sources : [null];
    return sources.map((source) => ({
      sourceId: source?.id ?? fact.id,
      recordId: fact.id,
      kind: "memory" as const,
      visibility: fact.visibility === "household" ? ("shared" as const) : ("adult_private" as const),
      label: presentation.title ?? fact.label,
      occurredAt: source?.occurredAt ?? fact.updatedAt,
      text: vaultMemoryText(fact, presentation),
    }));
  });
}

function vaultMemoryText(fact: FactRecord, presentation = vaultMemoryFields(fact)): string {
  return memoryPresentationText(factStatement(fact), presentation);
}

function memoryPresentationText(statement: string, presentation: MemoryPresentation): string {
  const parts = [statement];
  parts.push(`Memory kind: ${presentation.memoryKind}`);
  if (presentation.title) parts.push(`Title: ${presentation.title}`);
  if (presentation.artifactKind) parts.push(`Artifact: ${presentation.artifactKind}`);
  if (presentation.details && presentation.details !== statement) {
    parts.push(presentation.details);
  }
  if (presentation.tags.length > 0) parts.push(`Tags: ${presentation.tags.join(", ")}`);
  return parts.join("\n");
}

const VISIBLE_MEMORY_SOURCE_LIMIT = 50;

type MemorySearchContext = Readonly<{
  primary: string;
  context: readonly string[];
}>;

type RankedMemorySource = Readonly<{
  source: FlorenceSource;
  score: number;
}>;

const MEMORY_FUNCTION_WORDS = new Set([
  "a",
  "an",
  "and",
  "are",
  "as",
  "at",
  "be",
  "by",
  "for",
  "from",
  "in",
  "is",
  "it",
  "me",
  "my",
  "of",
  "on",
  "or",
  "our",
  "the",
  "that",
  "these",
  "this",
  "those",
  "to",
  "us",
  "was",
  "we",
  "were",
  "with",
  "you",
  "your",
]);

function normalizedMemoryTokens(value: string): string[] {
  return (
    value
      .normalize("NFKD")
      .replace(/\p{M}+/gu, "")
      .toLocaleLowerCase()
      .match(/[\p{L}\p{N}]+/gu) ?? []
  ).filter((token) => !MEMORY_FUNCTION_WORDS.has(token) && (/^\p{N}+$/u.test(token) || token.length >= 2));
}

function memoryTokenVariants(token: string): ReadonlySet<string> {
  const variants = new Set([token]);
  if (!/^\p{L}+$/u.test(token)) return variants;

  if (token.endsWith("ies") && token.length > 4) {
    variants.add(`${token.slice(0, -3)}y`);
  } else {
    if (token.endsWith("s") && !token.endsWith("ss") && token.length > 3) {
      variants.add(token.slice(0, -1));
    }
    if (token.endsWith("es") && /(?:s|x|z|ch|sh)es$/u.test(token)) {
      variants.add(token.slice(0, -2));
    }
  }

  if (token.endsWith("y") && token.length > 2) {
    variants.add(`${token.slice(0, -1)}ies`);
  } else {
    variants.add(`${token}s`);
    if (/(?:s|x|z|ch|sh)$/u.test(token)) variants.add(`${token}es`);
  }
  return variants;
}

function memoryTermIndex(tokens: readonly string[]): Set<string> {
  const index = new Set<string>();
  for (const token of tokens) {
    for (const variant of memoryTokenVariants(token)) index.add(variant);
  }
  return index;
}

function hasMemoryTerm(index: ReadonlySet<string>, token: string): boolean {
  for (const variant of memoryTokenVariants(token)) {
    if (index.has(variant)) return true;
  }
  return false;
}

function exactAdjacentMemoryMatches(query: readonly string[], target: readonly string[]): number {
  if (query.length < 2 || target.length < 2) return 0;
  const targetPairs = new Set<string>();
  for (let index = 0; index < target.length - 1; index += 1) {
    targetPairs.add(`${target[index]}\u0000${target[index + 1]}`);
  }
  let matches = 0;
  for (let index = 0; index < query.length - 1; index += 1) {
    if (targetPairs.has(`${query[index]}\u0000${query[index + 1]}`)) matches += 1;
  }
  return matches;
}

function rankMemorySources(
  sources: readonly FlorenceSource[],
  query: MemorySearchContext,
): RankedMemorySource[] {
  const indexedSources = sources.map((source) => {
    const labelTokens = normalizedMemoryTokens(source.label);
    const bodyTokens = normalizedMemoryTokens(source.text);
    return {
      source,
      labelTokens,
      bodyTokens,
      labelTerms: memoryTermIndex(labelTokens),
      bodyTerms: memoryTermIndex(bodyTokens),
    };
  });
  const primaryTokens = normalizedMemoryTokens(query.primary);
  const contextTokens = query.context.map(normalizedMemoryTokens);
  const weightedQueryTerms = new Map<string, number>();
  for (const token of primaryTokens) weightedQueryTerms.set(token, 3);
  for (const tokens of contextTokens) {
    for (const token of tokens) {
      if (!weightedQueryTerms.has(token)) weightedQueryTerms.set(token, 1);
    }
  }
  const corpusSize = indexedSources.length;
  const inverseDocumentFrequency = new Map<string, number>();
  for (const token of weightedQueryTerms.keys()) {
    const documentFrequency = indexedSources.reduce(
      (count, source) =>
        count + (hasMemoryTerm(source.labelTerms, token) || hasMemoryTerm(source.bodyTerms, token) ? 1 : 0),
      0,
    );
    inverseDocumentFrequency.set(token, Math.log((corpusSize + 1) / (documentFrequency + 1)) + 1);
  }

  return indexedSources
    .map((indexed): RankedMemorySource => {
      let score = 0;
      for (const [token, contextWeight] of weightedQueryTerms) {
        const fieldWeight = hasMemoryTerm(indexed.labelTerms, token)
          ? 2
          : hasMemoryTerm(indexed.bodyTerms, token)
            ? 1
            : 0;
        score += contextWeight * fieldWeight * (inverseDocumentFrequency.get(token) ?? 1);
      }
      score += Math.min(3, exactAdjacentMemoryMatches(primaryTokens, indexed.labelTokens)) * 0.3;
      score += Math.min(3, exactAdjacentMemoryMatches(primaryTokens, indexed.bodyTokens)) * 0.15;
      for (const tokens of contextTokens) {
        score += Math.min(2, exactAdjacentMemoryMatches(tokens, indexed.labelTokens)) * 0.1;
        score += Math.min(2, exactAdjacentMemoryMatches(tokens, indexed.bodyTokens)) * 0.05;
      }
      return { source: indexed.source, score };
    })
    .sort(
      (left, right) =>
        right.score - left.score ||
        (right.source.occurredAt ?? "").localeCompare(left.source.occurredAt ?? "") ||
        right.source.sourceId.localeCompare(left.source.sourceId),
    );
}

function representativeMemorySources(sources: readonly FlorenceSource[]): FlorenceSource[] {
  const byFact = new Map<string, FlorenceSource>();
  for (const source of sources) {
    const key = source.recordId ?? source.sourceId;
    const existing = byFact.get(key);
    if (
      !existing ||
      (source.occurredAt ?? "") > (existing.occurredAt ?? "") ||
      (source.occurredAt === existing.occurredAt && source.sourceId > existing.sourceId)
    ) {
      byFact.set(key, source);
    }
  }
  return [...byFact.values()];
}

function selectVisibleMemorySources(
  sources: readonly FlorenceSource[],
  query: MemorySearchContext,
): FlorenceSource[] {
  return rankMemorySources(sources, query)
    .map(({ source }) => source)
    .slice(0, VISIBLE_MEMORY_SOURCE_LIMIT);
}

function searchMemorySources(sources: readonly FlorenceSource[], query: string): FlorenceSource[] {
  return rankMemorySources(sources, { primary: query, context: [] })
    .filter(({ score }) => score > 0)
    .map(({ source }) => source);
}

function enforcePolicy(decision: FlorenceDecision): FlorenceDecision {
  if (decision.policy.stopMessaging) {
    return {
      policy: decision.policy,
      conversation: { replyToCurrentMessage: false, reaction: null, bubbles: [], nativeMoves: null },
      facts: [],
      followUp: null,
      reminder: null,
      familyWork: null,
      docketUpsert: null,
      docketCompletions: null,
      interest: null,
      calendar: null,
      householdUpdate: null,
      webAccessPath: null,
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
  return {
    policy: decision.policy,
    conversation: decision.conversation,
    facts: retain ? decision.facts : decision.facts.filter((fact) => fact.operation === "forget"),
    followUp:
      decision.followUp?.operation === "update" ? (schedule ? decision.followUp : null) : decision.followUp,
    reminder: decision.reminder,
    familyWork: decision.familyWork,
    docketUpsert: retain ? decision.docketUpsert : null,
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
    retainedDocketDocuments?: NonNullable<CommitTurnInput["docketMutation"]>["retainedDocuments"];
    retainedFamilyWorkDocuments?: NonNullable<CommitTurnInput["familyWorkRetainedDocuments"]>;
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
      decision.docketUpsert !== null ||
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
  const nativeResearchUrls = new Set(
    (decision.conversation.nativeMoves ?? []).flatMap((move) =>
      move.type === "rich_link" || move.type === "media" ? [move.url] : [],
    ),
  );
  const bubbles = decision.householdUpdate
    ? []
    : appendResearchSourceBubble(
        baseBubbles,
        (decision.researchUrls ?? []).filter((url) => !nativeResearchUrls.has(url)),
      );
  const facts: FactDraft[] = [];
  const deleteFactIds: string[] = [];
  for (const [index, change] of decision.facts.entries()) {
    if (change.operation === "forget") {
      const existing = turn.facts.find((fact) => fact.id === change.factId);
      const canDelete =
        turn.authority.audience === "group"
          ? existing?.visibility === "household"
          : existing?.visibility === "household" ||
            (existing?.visibility === "private" && existing.ownerAdultId === turn.authority.senderAdultId);
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
    const visibility = change.visibility;
    if (turn.authority.audience === "group" && visibility !== "household") {
      throw new FlorenceReasonerError("invalid_output", "A family-group turn cannot create private memory");
    }
    if (existing && existing.visibility !== visibility) {
      throw new FlorenceReasonerError(
        "invalid_output",
        "A memory correction cannot silently change who can see it",
      );
    }
    const ownerAdultId = visibility === "private" ? turn.authority.senderAdultId : null;
    const sameScope = existing?.visibility === visibility && existing.ownerAdultId === ownerAdultId;
    const slot =
      existing?.slot ??
      (change.memory.memoryKind === "artifact" && change.memory.title
        ? `artifact:${change.memory.artifactKind}:${sha256(change.memory.title.toLocaleLowerCase())}`
        : `${change.memory.memoryKind}:${sha256(statement.toLocaleLowerCase())}`);
    facts.push({
      id: sameScope ? existing.id : deterministicUuid(`fact\0${turn.message.sourceId}\0${index}`),
      subjectPersonId: existing?.subjectPersonId ?? null,
      kind: existing?.kind ?? (change.memory.memoryKind === "preference" ? "preference" : "general"),
      slot,
      label: change.memory.title ?? existing?.label ?? statement.slice(0, 160),
      value: {
        statement,
        memoryKind: change.memory.memoryKind,
        artifactKind: change.memory.artifactKind,
        title: change.memory.title,
        details: change.memory.details,
        tags: change.memory.tags,
      },
      visibility,
      ownerAdultId,
      sourceIds:
        change.operation === "correct"
          ? [...new Set([...change.sourceIds, turn.message.sourceId])]
          : change.sourceIds,
    });
  }
  const finiteMonitors: readonly [] = [];
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
  let messagePart = 0;
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
      turnPart: messagePart as 0 | 1 | 2,
      notBefore: new Date(now.getTime() + delay).toISOString(),
    });
    messagePart += 1;
  });
  for (const [nativeIndex, move] of (decision.conversation.nativeMoves ?? []).entries()) {
    if (move.type === "reaction") {
      outbound.push({
        sourceId: deterministicUuid(`outbound\0${turnId}\0native\0${nativeIndex}`),
        idempotencyKey: `turn:${turn.message.sourceId}:native:${nativeIndex}`,
        moveKind: "reaction",
        reaction: move.reaction.type === "tapback" ? move.reaction.reaction : move.reaction.emoji,
        replyToSourceId: move.targetSourceId,
        nativeMove: {
          type: "reaction",
          operation: move.operation,
          ...(move.partIndex === null || move.partIndex === undefined ? {} : { partIndex: move.partIndex }),
          reaction: move.reaction,
        },
        turnId,
        turnPart: -1,
        notBefore: new Date(now.getTime() + delay).toISOString(),
      });
      continue;
    }
    if (move.type === "poll") {
      const questionMove: OutboundNativeMoveDraft = {
        type: "message",
        parts: [{ type: "text", text: move.question }],
      };
      outbound.push({
        sourceId: deterministicUuid(`outbound\0${turnId}\0native\0${nativeIndex}\0question`),
        idempotencyKey: `turn:${turn.message.sourceId}:native:${nativeIndex}:question`,
        moveKind: "message",
        text: move.question,
        nativeMove: questionMove,
        turnId,
        turnPart: messagePart as 0 | 1 | 2,
        notBefore: new Date(now.getTime() + delay).toISOString(),
      });
      messagePart += 1;
      outbound.push({
        sourceId: deterministicUuid(`outbound\0${turnId}\0native\0${nativeIndex}\0poll`),
        idempotencyKey: `turn:${turn.message.sourceId}:native:${nativeIndex}:poll`,
        moveKind: "message",
        text: `Poll: ${move.options.join(" / ")}`,
        nativeMove: { type: "poll", options: move.options },
        turnId,
        turnPart: messagePart as 0 | 1 | 2,
        notBefore: new Date(now.getTime() + delay).toISOString(),
      });
      messagePart += 1;
      continue;
    }
    const nativeMove: OutboundNativeMoveDraft =
      move.type === "mention"
        ? (() => {
            if (turn.authority.audience !== "group") {
              throw new FlorenceReasonerError("invalid_output", "A mention requires the family group");
            }
            const adults = turn.household.members.filter(
              (member) =>
                member.kind === "adult" &&
                member.status === "verified" &&
                member.messagesIdentity === "connected" &&
                member.messagesAddress !== null &&
                member.displayName === move.adultDisplayName &&
                turn.authority.adultIds.includes(member.id),
            );
            if (adults.length !== 1 || !adults[0]?.messagesAddress) {
              throw new FlorenceReasonerError(
                "invalid_output",
                "A mention must resolve one exact enrolled adult",
              );
            }
            const start = move.text.indexOf(move.adultDisplayName);
            if (start < 0) {
              throw new FlorenceReasonerError(
                "invalid_output",
                "A mention must contain the adult's exact display name",
              );
            }
            return {
              type: "message" as const,
              parts: [
                {
                  type: "text" as const,
                  text: move.text,
                  mention: {
                    handle: adults[0].messagesAddress,
                    range: [start, start + move.adultDisplayName.length] as const,
                  },
                },
              ],
            };
          })()
        : move.type === "rich_link"
          ? { type: "message", parts: [{ type: "link", url: move.url }] }
          : { type: "message", parts: [{ type: "media", source: { type: "url", url: move.url } }] };
    const text = move.type === "mention" ? move.text : move.url;
    outbound.push({
      sourceId: deterministicUuid(`outbound\0${turnId}\0native\0${nativeIndex}`),
      idempotencyKey: `turn:${turn.message.sourceId}:native:${nativeIndex}`,
      moveKind: "message",
      text,
      nativeMove,
      turnId,
      turnPart: messagePart as 0 | 1 | 2,
      notBefore: new Date(now.getTime() + delay).toISOString(),
    });
    messagePart += 1;
  }
  if (outbound.length > 3 || messagePart > 3) {
    throw new FlorenceReasonerError(
      "invalid_output",
      "A Florence turn can make at most three physical Messages sends",
    );
  }
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
  const familyWorkAcknowledgementText = [
    ...bubbles.map((bubble) => bubble.text),
    ...(decision.conversation.nativeMoves ?? []).flatMap((move) =>
      move.type === "mention" ? [move.text] : [],
    ),
  ].join("\n");
  const familyWorkMutation: CommitTurnInput["familyWorkMutation"] =
    decision.familyWork?.operation === "create"
      ? {
          operation: "create",
          workId: deterministicUuid(`family-work\0${turn.message.sourceId}`),
          objective: decision.familyWork.objective,
          completionCondition: decision.familyWork.completionCondition,
          ...(familyWorkAcknowledgementText ? { acknowledgementText: familyWorkAcknowledgementText } : {}),
          schedule: decision.familyWork.schedule,
          visibility: turn.authority.audience === "group" ? "household" : "private",
          ownerAdultId: turn.authority.audience === "group" ? null : turn.authority.senderAdultId,
          candidateIds: [...decision.familyWork.candidateIds],
        }
      : decision.familyWork?.operation === "update"
        ? {
            operation: "update",
            workId: decision.familyWork.workId,
            objective: decision.familyWork.objective,
            completionCondition: decision.familyWork.completionCondition,
            schedule: decision.familyWork.schedule,
          }
        : decision.familyWork?.operation === "steer"
          ? {
              operation: "steer",
              workId: decision.familyWork.workId,
              instruction: decision.familyWork.instruction,
              completionCondition: decision.familyWork.completionCondition,
              ...(familyWorkAcknowledgementText
                ? { acknowledgementText: familyWorkAcknowledgementText }
                : {}),
            }
          : decision.familyWork?.operation === "run"
            ? {
                operation: "run",
                workId: decision.familyWork.workId,
                ...(familyWorkAcknowledgementText
                  ? { acknowledgementText: familyWorkAcknowledgementText }
                  : {}),
              }
            : decision.familyWork && ["pause", "resume", "cancel"].includes(decision.familyWork.operation)
              ? {
                  operation: decision.familyWork.operation as "pause" | "resume" | "cancel",
                  workId: decision.familyWork.workId as string,
                }
              : null;
  const docketMutation: NonNullable<CommitTurnInput["docketMutation"]> | null = decision.docketUpsert
    ? decision.docketUpsert.operation === "create"
      ? {
          operation: "create",
          candidateId: null,
          candidate: { ...decision.docketUpsert.candidate },
          sourceIds: [...new Set([turn.message.sourceId, ...decision.docketUpsert.sourceIds])],
          retainedDocuments: [...(options.retainedDocketDocuments ?? [])],
        }
      : {
          operation: "update",
          candidateId: decision.docketUpsert.candidateId,
          candidate: { ...decision.docketUpsert.candidate },
          sourceIds: [...new Set([turn.message.sourceId, ...decision.docketUpsert.sourceIds])],
          retainedDocuments: [...(options.retainedDocketDocuments ?? [])],
        }
    : null;
  const calendar = calendarCommit(turn, decision, options.resolveCalendarEventTarget);
  const approval = options.approveCalendarOffer ? [calendarApproval(options.approveCalendarOffer)] : [];
  const householdUpdate = decision.householdUpdate;
  if (householdUpdate) {
    if (
      turn.authority.audience !== "private" ||
      turn.message.moveKind === "reaction" ||
      !hasVerifiedInstruction(turn.message) ||
      householdUpdate.sourceIds.length !== 1 ||
      householdUpdate.sourceIds[0] !== turn.message.sourceId
    ) {
      throw new FlorenceReasonerError(
        "invalid_output",
        "A household update requires only the current adult's verified private instruction",
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
    familyWorkRetainedDocuments: [...(options.retainedFamilyWorkDocuments ?? [])],
    docketMutation,
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

function approvedActionConversation(
  calendarOffer: InboundTurn["pendingCalendarOffers"][number] | null,
  partnerInvitation: InboundTurn["pendingPartnerInvitation"],
): FlorenceDecision["conversation"] {
  const bubbles: FlorenceDecision["conversation"]["bubbles"] = [];
  if (partnerInvitation) {
    bubbles.push({ text: `Got it—I’ll text ${partnerInvitation.firstName} now.`, delayMs: 0 });
  }
  if (calendarOffer) {
    bubbles.push({
      text: `Got it—I’ll add “${calendarOffer.event.title}” to the family calendar.`,
      delayMs: bubbles.length === 0 ? 0 : 250,
    });
  }
  if (bubbles.length === 0) throw new Error("An approved action acknowledgement requires an action");
  return { replyToCurrentMessage: true, reaction: null, bubbles, nativeMoves: null };
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
  if (!hasVerifiedInstruction(turn.message)) {
    throw new FlorenceReasonerError(
      "invalid_output",
      "A Calendar decision requires the current parent's verified instruction",
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

function vaultMemoryFields(fact: FactRecord): MemoryPresentation {
  return decodeMemoryPresentation(fact.value);
}

function sameMemoryPresentation(left: MemoryPresentation, right: MemoryPresentation): boolean {
  return (
    left.memoryKind === right.memoryKind &&
    left.artifactKind === right.artifactKind &&
    left.title === right.title &&
    left.details === right.details &&
    left.tags.length === right.tags.length &&
    left.tags.every((tag, index) => tag === right.tags[index])
  );
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

function privateGoogleMemoryQuery(
  sources: readonly (FlorencePrivateGmailSource | FlorencePrivateCalendarEvent)[],
): MemorySearchContext {
  return {
    primary: sources
      .map((source) =>
        source.kind === "gmail"
          ? [source.sender, source.subject, source.text].filter((value) => value != null).join("\n")
          : [source.title, source.startsAt, source.startDate].filter((value) => value != null).join("\n"),
      )
      .join("\n"),
    context: [],
  };
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
      text: message.text,
      textStatus: message.textStatus,
      attachments: message.attachments,
      attachmentsStatus: message.attachmentsStatus,
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
      ...calendarEvidenceContentDetails(event),
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
    );
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

function privateGoogleCorpusCursorIsCurrent(
  gmailCursor: string | null,
  calendarCursor: string,
  connectionId: string,
): boolean {
  if (!gmailCursor) return false;
  try {
    googleGmailCursor(gmailCursor, connectionId);
    googleCalendarAccountCursor(calendarCursor, connectionId);
    return true;
  } catch {
    return false;
  }
}

function googleGmailCursor(value: string, connectionId: string): GoogleGmailCursor {
  const parsed: unknown = JSON.parse(value);
  if (
    !isRecord(parsed) ||
    parsed.kind !== "gmail_poll_cursor_v1" ||
    parsed.scannerVersion !== PRIVATE_GOOGLE_CORPUS_VERSION ||
    parsed.connectionId !== connectionId
  ) {
    throw new Error("The stored Gmail account cursor is invalid");
  }
  return googleGmailProviderCursor(JSON.stringify(parsed.provider));
}

function googleGmailPollCursor(cursor: GoogleGmailCursor, connectionId: string): string {
  return JSON.stringify({
    kind: "gmail_poll_cursor_v1",
    scannerVersion: PRIVATE_GOOGLE_CORPUS_VERSION,
    connectionId,
    provider: cursor,
  });
}

type GoogleCalendarAccountCursorV1 = {
  kind: "calendar_account_cursor_v1";
  scannerVersion: typeof PRIVATE_GOOGLE_CORPUS_VERSION;
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
    parsed.scannerVersion !== PRIVATE_GOOGLE_CORPUS_VERSION ||
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
    scannerVersion: PRIVATE_GOOGLE_CORPUS_VERSION,
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

function browserImageUploadFilename(
  assetId: string,
  mimeType: "image/jpeg" | "image/png" | "image/webp",
): string {
  const extension = mimeType === "image/jpeg" ? "jpg" : mimeType === "image/png" ? "png" : "webp";
  return `attachment-${assetId.slice(0, 8)}.${extension}`;
}

function browserSelectedImageFilename(
  label: string,
  assetId: string,
  mimeType: "image/jpeg" | "image/png" | "image/webp",
): string {
  const extension = mimeType === "image/jpeg" ? "jpg" : mimeType === "image/png" ? "png" : "webp";
  const basename = label
    .normalize("NFKC")
    .trim()
    .replace(/[^\p{L}\p{N}]+/gu, "-")
    .replace(/^-+|-+$/gu, "")
    .slice(0, 80);
  return `${basename || `florence-result-${assetId.slice(0, 8)}`}.${extension}`;
}

function browserPdfUploadFilename(filename: string, documentId: string): string {
  const basename = filename
    .normalize("NFKC")
    .trim()
    .replace(/\.pdf$/iu, "")
    .trim()
    .slice(0, 50);
  return `${basename || `attachment-${documentId.slice(0, 8)}`}.pdf`;
}

function turnText(turn: InboundTurn["message"] | InboundTurn["recentMessages"][number]): string {
  if (turn.text?.trim()) return redactWebAccessToken(turn.text);
  if (turn.reaction) return `Reacted ${turn.reaction}`;
  return "Shared a family attachment.";
}

function modelConversationHistoryMessage(
  message: ConversationHistoryMessage,
): FlorenceConversationHistoryMessage {
  return {
    anchor: message.anchor,
    sourceId: message.sourceId,
    conversation: message.conversation,
    senderName: message.senderName,
    moveKind: message.moveKind,
    text: conversationHistoryText(message),
    occurredAt: message.occurredAt,
    replyToSourceId: message.replyToSourceId,
    supersedesSourceId: message.supersedesSourceId,
    hasAttachments: message.hasAttachments,
  };
}

function conversationHistorySource(message: FlorenceConversationHistoryMessage): FlorenceSource {
  return {
    sourceId: message.sourceId,
    recordId: null,
    kind: "message",
    visibility: message.conversation === "family_group" ? "shared" : "adult_private",
    label: message.senderName,
    occurredAt: message.occurredAt,
    text: message.text,
  };
}

function conversationHistoryText(message: ConversationHistoryMessage): string {
  if (message.moveKind === "reaction") {
    return message.reaction?.trim()
      ? `Reacted ${redactWebAccessToken(message.reaction)} to an earlier message.`
      : "Reacted to an earlier message.";
  }
  const text = message.text?.trim() ? redactWebAccessToken(message.text) : null;
  if (text && message.hasAttachments) return `${text}\nShared an attachment with this message.`;
  if (text) return text;
  if (message.hasAttachments) return "Shared an attachment.";
  return message.moveKind === "reply" ? "Replied to an earlier message." : "Sent a message.";
}

function hasVerifiedInstruction(turn: InboundTurn["message"]): boolean {
  return Boolean(turn.authoredText?.trim() || (turn.voiceTranscriptPresent && turn.text?.trim()));
}

function redactWebAccessToken(value: string): string {
  return value.replace(
    /(?:fs2\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+|(?:wa1|ps1)\.[A-Za-z0-9_-]+)/gu,
    "[secure web link]",
  );
}

function modelSafeGmailText(value: string): string {
  return redactWebAccessToken(value);
}

function retainedPrivateGoogleSource(source: PrivateGoogleSourceReadResult): FlorenceSource {
  if (source.kind === "gmail") {
    const bodyCoverage =
      source.content.textStatus === "complete"
        ? null
        : source.content.textStatus === "truncated"
          ? "Body coverage: truncated historical snapshot; run a fresh Gmail search if omitted detail matters."
          : "Body coverage: no retained inline body; run a fresh Gmail search or inspect the attachment if needed.";
    const attachmentSummary =
      source.content.attachments.length === 0
        ? null
        : `Attachments: ${source.content.attachments.map(({ filename }) => filename).join(", ")}${
            source.content.attachmentsStatus === "truncated" ? " (additional attachments not listed)" : ""
          }`;
    const body = source.content.text.trim() || "This retained Gmail message has no inline body.";
    const context = [
      `From: ${source.content.sender}`,
      source.content.subject ? `Subject: ${source.content.subject}` : null,
      bodyCoverage,
      source.content.attachmentsStatus === "truncated"
        ? "Attachment coverage: the historical attachment list was truncated; recheck Gmail if attachments matter."
        : null,
      attachmentSummary,
    ]
      .filter((value): value is string => value !== null)
      .join("\n\n");
    const completeText = modelSafeGmailText(`${context}\n\n${body}`);
    const text =
      completeText.length <= 50_000
        ? completeText
        : (() => {
            const displayCoverage =
              "Display coverage: this exact retained body exceeds one model read; run a fresh Gmail search if the omitted tail matters.";
            const prefix = modelSafeGmailText(`${context}\n\n${displayCoverage}\n\n`);
            return `${prefix}${modelSafeGmailText(body).slice(0, Math.max(0, 50_000 - prefix.length))}`;
          })();
    return {
      sourceId: source.sourceId,
      recordId: null,
      kind: "gmail",
      visibility: "adult_private",
      label: modelSafeGmailText(source.label),
      occurredAt: source.occurredAt,
      text,
    };
  }
  const content = source.content;
  const interval =
    content.intervalKind === "all_day" && content.startDate && content.endDate
      ? `All day: ${content.startDate} through ${content.endDate} (exclusive end)`
      : content.startsAt && content.endsAt
        ? `When: ${content.startsAt} to ${content.endsAt}${content.timeZone ? ` (${content.timeZone})` : ""}`
        : "When: no retained interval";
  const text = [
    `Calendar event: ${content.title ?? source.label}`,
    `Status: ${content.status}`,
    `Availability: ${content.busy ? "busy" : "free"}`,
    interval,
    content.location ? `Location: ${content.location}` : null,
  ]
    .filter((value): value is string => value !== null)
    .join("\n");
  return {
    sourceId: source.sourceId,
    recordId: null,
    kind: "calendar",
    visibility: "adult_private",
    label: modelSafeGmailText(source.label),
    occurredAt: source.occurredAt,
    text: modelSafeGmailText(text).slice(0, 50_000),
  };
}

function approvalReplyTargetsPrompt(
  replyToSourceId: string | null,
  approvalPromptSourceId: string | null,
): boolean {
  return replyToSourceId === null || replyToSourceId === approvalPromptSourceId;
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
  if (calendarChangeRepresentsRemoval(event)) return false;
  if (event.startsAt === null || event.endsAt === null) return false;
  return Date.parse(event.endsAt) > Date.parse(timeMin) && Date.parse(event.startsAt) < Date.parse(timeMax);
}

function calendarChangeRepresentsRemoval(event: GoogleCalendarChange): boolean {
  return event.status === "cancelled" || event.startsAt === null || event.endsAt === null;
}

function calendarEvidenceContentDetails(event: GoogleCalendarWindowEvent | GoogleCalendarChange): {
  location: string | null;
  locationObserved: boolean;
  intervalKind: "timed" | "all_day" | null;
  timeZone: string | null;
  startDate: string | null;
  endDate: string | null;
} {
  if ("intervalKind" in event) {
    return event.intervalKind === "all_day"
      ? {
          location: event.location,
          locationObserved: true,
          intervalKind: "all_day",
          timeZone: null,
          startDate: event.startDate,
          endDate: event.endDate,
        }
      : {
          location: event.location,
          locationObserved: true,
          intervalKind: "timed",
          timeZone: event.timeZone,
          startDate: null,
          endDate: null,
        };
  }
  return {
    location: event.location ?? null,
    locationObserved: "location" in event,
    intervalKind: event.allDay === null ? null : event.allDay ? "all_day" : "timed",
    timeZone: event.timeZone,
    startDate: event.startDate,
    endDate: event.endDate,
  };
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
  TConclusion extends {
    category: string;
    summary: string;
    dueAt: string | null;
    needsAnswer: boolean;
    owner: string | null;
    nextAction: string;
    waitingOn: string | null;
    completionCondition: string;
  },
>(input: {
  familyRelevance: string;
  conclusion: TConclusion | null;
  sourceIds: readonly string[];
  familyCalendar: FamilyCalendarReviewProposal | null;
  googleEvidence: ReadonlyMap<string, GoogleEvidenceDraft>;
  adultFirstName: string;
  timeZone: string;
}): { conclusion: TConclusion | null; familyCalendar: FamilyCalendarReviewProposal | null } {
  if (input.familyRelevance === "owner_private") {
    return { conclusion: null, familyCalendar: null };
  }
  const familyCalendar = input.familyCalendar;
  const personalCalendarSourceId = familyCalendar?.sourceIds[0];
  const personalCalendarSource = personalCalendarSourceId
    ? input.googleEvidence.get(personalCalendarSourceId)
    : null;
  if (
    input.familyRelevance !== "owner_private" &&
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
  T extends {
    category: string;
    summary: string;
    dueAt: string | null;
    needsAnswer: boolean;
    owner: string | null;
    nextAction: string;
    waitingOn: string | null;
    completionCondition: string;
  },
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
    owner: "Family",
    nextAction: `Coordinate around ${input.adultFirstName}’s calendar conflict.`,
    completionCondition: `${input.adultFirstName}’s calendar conflict has a confirmed family plan.`,
    waitingOn:
      input.conclusion.waitingOn === null
        ? null
        : input.conclusion.needsAnswer
          ? "A family scheduling decision"
          : "The schedule conflict to be resolved",
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

function unionBusyIntervals(
  intervals: readonly { startsAt: string; endsAt: string }[],
): { startsAt: string; endsAt: string }[] {
  const sorted = intervals
    .map((interval) => ({ ...interval }))
    .sort((left, right) => {
      const startDifference = Date.parse(left.startsAt) - Date.parse(right.startsAt);
      return startDifference === 0 ? Date.parse(left.endsAt) - Date.parse(right.endsAt) : startDifference;
    });
  const union: { startsAt: string; endsAt: string }[] = [];
  for (const interval of sorted) {
    const previous = union.at(-1);
    if (!previous || Date.parse(interval.startsAt) > Date.parse(previous.endsAt)) {
      union.push(interval);
      continue;
    }
    if (Date.parse(interval.endsAt) > Date.parse(previous.endsAt)) previous.endsAt = interval.endsAt;
  }
  return union;
}

type TitleFreeAvailabilityRead = Readonly<{
  coverage: "complete" | "partial" | "unavailable";
  busyIntervals: readonly { startsAt: string; endsAt: string }[];
}>;

const MAX_GOOGLE_CALENDAR_WINDOW_MS = 31 * 24 * 60 * 60_000;

/**
 * Directly adapts OpenInstinct's check_calendar_availability contract and
 * per-calendar failure handling (480045d, google_workspace_read.ts and
 * calendar.ts), with Hermes's exact half-open window and complete-scope
 * discipline (daily-brief.md). Florence uses its existing exhaustive Google
 * adapter instead of OpenInstinct's bounded freebusy request.
 */
async function readTitleFreeAvailabilityRange(input: {
  timeMin: string;
  timeMax: string;
  readPage: (input: {
    timeMin: string;
    timeMax: string;
    cursor: string | null;
  }) => Promise<GooglePersonalCalendarWindowRead>;
}): Promise<TitleFreeAvailabilityRead> {
  const rangeStart = Date.parse(input.timeMin);
  const rangeEnd = Date.parse(input.timeMax);
  if (!Number.isFinite(rangeStart) || !Number.isFinite(rangeEnd) || rangeEnd <= rangeStart) {
    throw new Error("Availability range is invalid");
  }

  const busyIntervals: { startsAt: string; endsAt: string }[] = [];
  let complete = true;
  let reliable = false;
  for (let chunkStart = rangeStart; chunkStart < rangeEnd; ) {
    const chunkEnd = Math.min(rangeEnd, chunkStart + MAX_GOOGLE_CALENDAR_WINDOW_MS);
    const [settlement] = await Promise.allSettled([
      readTitleFreeAvailabilityChunk({
        timeMin: new Date(chunkStart).toISOString(),
        timeMax: new Date(chunkEnd).toISOString(),
        readPage: input.readPage,
      }),
    ]);
    if (!settlement || settlement.status === "rejected") {
      complete = false;
    } else {
      busyIntervals.push(...settlement.value.busyIntervals);
      if (settlement.value.coverage !== "complete") complete = false;
      if (settlement.value.coverage !== "unavailable") reliable = true;
    }
    chunkStart = chunkEnd;
  }
  return {
    coverage: complete && reliable ? "complete" : reliable ? "partial" : "unavailable",
    busyIntervals: unionBusyIntervals(busyIntervals),
  };
}

async function readTitleFreeAvailabilityChunk(input: {
  timeMin: string;
  timeMax: string;
  readPage: (input: {
    timeMin: string;
    timeMax: string;
    cursor: string | null;
  }) => Promise<GooglePersonalCalendarWindowRead>;
}): Promise<TitleFreeAvailabilityRead> {
  const expectedTimeMin = Date.parse(input.timeMin);
  const expectedTimeMax = Date.parse(input.timeMax);
  const busyIntervals: { startsAt: string; endsAt: string }[] = [];
  const seenCursors = new Set<string>();
  let cursor: string | null = null;
  let complete = true;
  let reliable = false;
  let expectedTotalEventCount: number | null = null;
  let observedEventCount = 0;

  while (true) {
    let read: GooglePersonalCalendarWindowRead;
    try {
      read = await input.readPage({ timeMin: input.timeMin, timeMax: input.timeMax, cursor });
    } catch {
      complete = false;
      break;
    }
    if (Date.parse(read.timeMin) !== expectedTimeMin || Date.parse(read.timeMax) !== expectedTimeMax) {
      complete = false;
      break;
    }

    const completeCalendars = read.calendars.filter(
      (calendar) =>
        calendar.status === "complete" &&
        calendar.accessRole !== "freeBusyReader" &&
        calendar.timeZone !== null,
    );
    const calendarTimeZones = new Map(
      read.calendars.flatMap((calendar) =>
        calendar.timeZone === null ? [] : ([[calendar.calendarId, calendar.timeZone]] as const),
      ),
    );
    if (expectedTotalEventCount === null) expectedTotalEventCount = read.totalEventCount;
    if (
      expectedTotalEventCount !== read.totalEventCount ||
      read.totalCalendarCount !== read.calendars.length
    ) {
      complete = false;
    }
    observedEventCount += read.events.length;
    const pageReliable =
      read.status === "complete" || read.status === "truncated" || completeCalendars.length > 0;
    if (pageReliable) reliable = true;
    if (
      read.status === "partial" ||
      read.status === "unavailable" ||
      completeCalendars.length !== read.calendars.length
    ) {
      complete = false;
    }

    for (const event of read.events) {
      if (!event.busy) continue;
      const calendarTimeZone = calendarTimeZones.get(event.calendarId);
      if (event.intervalKind === "all_day" && !calendarTimeZone) {
        complete = false;
        continue;
      }
      let bounds: { startsAt: string; endsAt: string };
      try {
        bounds = calendarWindowBounds(event, calendarTimeZone ?? "UTC");
      } catch {
        complete = false;
        continue;
      }
      const startsAt = Math.max(expectedTimeMin, Date.parse(bounds.startsAt));
      const endsAt = Math.min(expectedTimeMax, Date.parse(bounds.endsAt));
      if (Number.isFinite(startsAt) && Number.isFinite(endsAt) && endsAt > startsAt) {
        busyIntervals.push({
          startsAt: new Date(startsAt).toISOString(),
          endsAt: new Date(endsAt).toISOString(),
        });
      }
    }

    const nextCursor = read.nextCursor;
    if (nextCursor === null) {
      if (read.status === "truncated") complete = false;
      if (expectedTotalEventCount !== observedEventCount) complete = false;
      break;
    }
    if (read.status === "complete" || seenCursors.has(nextCursor)) {
      complete = false;
      break;
    }
    seenCursors.add(nextCursor);
    cursor = nextCursor;
  }

  return {
    coverage: complete && reliable ? "complete" : reliable ? "partial" : "unavailable",
    busyIntervals: unionBusyIntervals(busyIntervals),
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

function conversationalGmailEvidence(input: {
  householdId: string;
  ownerAdultId: string;
  connectionId: string;
  message: GmailEvidence;
}): Readonly<{
  draft: ReturnType<typeof draftGmailEvidence>;
  source: FlorenceConversationalGmailSource;
  attachments: readonly (readonly [string, GmailAttachmentReference])[];
}> {
  const draft = draftGmailEvidence({
    householdId: input.householdId,
    ownerAdultId: input.ownerAdultId,
    connectionId: input.connectionId,
    ...input.message,
  });
  return {
    draft,
    source: {
      sourceId: draft.id,
      kind: "gmail",
      visibility: "adult_private",
      sentAt: input.message.sentAt,
      sender: modelSafeGmailText(input.message.from),
      subject: input.message.subject === null ? null : modelSafeGmailText(input.message.subject),
      text: modelSafeGmailText(input.message.text),
      textStatus: input.message.textStatus,
      attachments: input.message.attachments.map((attachment) => ({
        attachmentRef: gmailAttachmentRefFor(draft.id, attachment),
        filename: attachment.filename,
        mimeType: attachment.mimeType,
        sizeBytes: attachment.sizeBytes,
      })),
      attachmentsStatus: input.message.attachmentsStatus,
    },
    attachments: input.message.attachments.map(
      (attachment) => [`${draft.id}\0${gmailAttachmentRefFor(draft.id, attachment)}`, attachment] as const,
    ),
  };
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function householdNextActionStateDigest(input: {
  docket: HouseholdDocket;
  activeWork: readonly VisibleActiveFamilyWork[];
  familyCalendar: FlorenceHouseholdNextActionInput["familyCalendar"]["events"];
  facts: readonly FactRecord[];
}): string {
  const semanticRows = (values: readonly unknown[]): string[] =>
    [...new Set(values.map((value) => JSON.stringify(canonicalHouseholdState(value))))].sort();
  return sha256(
    JSON.stringify({
      docket: semanticRows(
        input.docket.items.map(({ candidateId: _candidateId, visibility: _visibility, ...item }) => item),
      ),
      activeWork: semanticRows(
        input.activeWork.map(
          ({ workId: _workId, candidateIds: _candidateIds, visibility: _visibility, ...item }) => item,
        ),
      ),
      familyCalendar: semanticRows(input.familyCalendar),
      vault: semanticRows(
        input.facts.map(({ id: _id, householdId: _householdId, sources: _sources, ...fact }) => ({
          ...fact,
          correctedAt: null,
          updatedAt: null,
        })),
      ),
    }),
  );
}

function canonicalHouseholdState(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalHouseholdState);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, entry]) => [key, canonicalHouseholdState(entry)]),
    );
  }
  return value;
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

function familyWorkWasExplicitlyCancelled(signal: AbortSignal): boolean {
  return (
    signal.aborted &&
    signal.reason instanceof Error &&
    signal.reason.message === "The family task was cancelled"
  );
}

function credentialInvalidGrant(error: unknown): boolean {
  return error instanceof GoogleConnectionError && error.code === "credential_invalid_grant";
}
