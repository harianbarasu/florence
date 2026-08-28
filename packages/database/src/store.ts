import { createHash, randomUUID } from "node:crypto";
import postgres from "postgres";

export type JsonValue = postgres.JSONValue;
export type JsonObject = Readonly<Record<string, JsonValue>>;
export type Visibility = "private" | "household";
export type Audience = "private" | "group";
export type ImageReference = {
  assetId: string;
  mimeType: "image/jpeg" | "image/png" | "image/webp" | "image/heic";
};
const MAX_CURRENT_PDFS = 3;
const MAX_PDF_ENVELOPE_BYTES = 20 * 1024 * 1024 + 16 * 1024;
const GOOGLE_POLL_INTERVAL_MS = 2 * 60_000;
const INTEREST_CHECK_INTERVAL_MS = 7 * 24 * 60 * 60_000;
const LINQ_RECEIPT_CLOCK_SKEW_MS = 5 * 60_000;
const FAMILY_WORK_INITIAL_DELAY_MS = 1_000;
const FAMILY_WORK_CLAIM_LEASE_MS = 2 * 60_000;
const MAX_FAMILY_WORK_STATE_BYTES = 256 * 1024;
const MAX_FAMILY_WORK_COUNTER = 999_999_999;
// Kept as an on-disk compatibility marker for replicas from before the reply gate stopped expiring.
// New code only treats this timestamp as a deadline after a signed-link digest has been stored.
const LEGACY_PARTNER_HANDSHAKE_WINDOW_MS = 24 * 60 * 60_000;
const PROACTIVE_CONSENT_PAUSE_REASON = "Paused because proactive Google use is disabled";
const HOUSEHOLD_SAFE_MONITOR_WHY = "Florence is watching this family coordination item.";
const COMPLETE_CALENDAR_HISTORY_SCOPE = "https://www.googleapis.com/auth/calendar.events.readonly" as const;
const GOOGLE_ACTION_WORK_MARKER_PREFIX = "google_action_v2:";
const GOOGLE_ACTION_WORK_MARKER_LENGTH = GOOGLE_ACTION_WORK_MARKER_PREFIX.length + 64;
const CALENDAR_ACTION_EXECUTION_MARKER = "calendar_action_execution_claim_v1";
const CALENDAR_ACTION_EXECUTION_LEASE_MS = 15 * 60_000;
const GOOGLE_SOURCE_REMOVED_BEFORE_DELIVERY =
  "The underlying Google item was removed before this message was delivered";
const GOOGLE_SOURCE_REVISED_BEFORE_DELIVERY =
  "The underlying Google item changed before this message was delivered";
export type GoogleScope =
  | "openid"
  | "email"
  | "https://www.googleapis.com/auth/gmail.readonly"
  | "https://www.googleapis.com/auth/calendar.events.readonly"
  | "https://www.googleapis.com/auth/calendar.events.owned"
  | "https://www.googleapis.com/auth/calendar.app.created"
  | "https://www.googleapis.com/auth/calendar.acls"
  | "https://www.googleapis.com/auth/calendar.calendarlist";

export class FlorenceStoreConflict extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FlorenceStoreConflict";
  }
}

export class FlorenceStoreUnauthorized extends Error {
  constructor(message = "This adult cannot change that household data") {
    super(message);
    this.name = "FlorenceStoreUnauthorized";
  }
}

export type PostgresFlorenceStoreOptions = {
  connectionString: string;
  maxConnections?: number;
};

export type ProductionResetCalendarTarget = Readonly<{
  householdId: string;
  /** Null only when Google creation was durably latched but its returned ID was never stored. */
  calendarId: string | null;
  creator: Readonly<{
    adultId: string;
    connectionId: string;
  }> | null;
}>;

export type ProductionResetGoogleCredentialTarget = Readonly<{
  householdId: string;
  adultId: string;
  connectionId: string;
}>;

export type ProductionResetCalendarRecovery = Readonly<{
  householdId: string;
  calendarId: string;
}>;

/**
 * Exact, immutable database state that an operator inspected before a production reset.
 * The guard fingerprints every household-data row without exposing that data to the CLI.
 */
export type ProductionResetSnapshot = Readonly<{
  guard: string;
  householdCount: number;
  calendars: readonly ProductionResetCalendarTarget[];
  activeGoogleCredentials: readonly ProductionResetGoogleCredentialTarget[];
}>;

export type ProductionResetResult = Readonly<{
  deletedHouseholds: number;
  deletedCalendars: number;
  deletedActiveGoogleCredentials: number;
}>;

export type FamilyMemberRecord = {
  id: string;
  householdId: string;
  kind: "adult" | "child";
  role: "steward" | "caregiver" | "dependent";
  adultSlot: 1 | 2 | null;
  displayName: string;
  status: "planned" | "verified" | "represented";
  messagesIdentity: "not_invited" | "invited" | "connected" | null;
  messagesInvitationApproved: boolean | null;
  messagesAddress: string | null;
  profile: JsonObject;
  preferences: JsonObject;
};

export type LinqChannelRecord = {
  id: string;
  householdId: string;
  audience: Audience;
  providerConversationId: string;
  adultIds: readonly string[];
  participantIdentityDigests: readonly string[];
  authorityDigest: string;
  boundAt: string;
  revokedAt: string | null;
  stoppedAt: string | null;
};

export type SourceRecord = {
  id: string;
  kind: "linq_message" | "gmail" | "google_file" | "document" | "web" | "setup" | "calendar";
  visibility: Visibility;
  ownerAdultId: string | null;
  label: string;
  metadata: JsonObject;
  occurredAt: string;
};

export type CalendarEvidenceRecord = SourceRecord & {
  status: "confirmed" | "tentative" | "cancelled";
  busy: boolean;
  title: string | null;
  startsAt: string | null;
  endsAt: string | null;
  allDay: boolean | null;
};

type GoogleEvidenceDraftBase = {
  id: string;
  householdId: string;
  visibility: Visibility;
  ownerAdultId: string | null;
  connectionOwnerAdultId: string;
  connectionId: string;
  externalKey: string;
  label: string;
  metadata: JsonObject;
  occurredAt: string;
};

export type GmailEvidenceDraft = GoogleEvidenceDraftBase & {
  kind: "gmail";
  visibility: "private";
  ownerAdultId: string;
  messageId: string;
  threadId: string;
  historyId: string;
  from: string;
  subject: string | null;
  sentAt: string;
};

export type CalendarEvidenceDraft = GoogleEvidenceDraftBase & {
  kind: "calendar";
  calendarId: string;
  providerEventId: string;
  providerRevision: string;
  providerUpdatedAt: string;
  status: "confirmed" | "tentative" | "cancelled";
  busy: boolean;
  title: string | null;
  startsAt: string | null;
  endsAt: string | null;
  allDay: boolean | null;
};

export type GoogleEvidenceDraft = GmailEvidenceDraft | CalendarEvidenceDraft;

export type ReviewedGoogleSourceDisposition = Readonly<{
  sourceId: string;
  disposition: "retained" | "dismissed";
}>;

export type FactRecord = {
  id: string;
  householdId: string;
  subjectPersonId: string | null;
  kind:
    | "identity"
    | "school"
    | "caregiver"
    | "activity"
    | "schedule"
    | "address"
    | "phone"
    | "contact"
    | "preference"
    | "safety"
    | "general";
  slot: string;
  label: string;
  value: JsonValue;
  visibility: Visibility;
  ownerAdultId: string | null;
  sources: readonly SourceRecord[];
  correctedAt: string | null;
  updatedAt: string;
};

export type CurrentMessageDocument = {
  id: string;
  parentSourceId: string;
  filename: string;
  mimeType: "application/pdf";
  contentDigest: string;
  contentEnvelope: Uint8Array;
  discardAfter: string;
};

export type GoogleConnectionStatus = "pending" | "active" | "disconnected";
export type GoogleConnectionView = {
  connectionId: string;
  householdId: string;
  ownerAdultId: string;
  status: GoogleConnectionStatus;
  emailLabel: string | null;
  grantedScopes: readonly GoogleScope[];
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
};

export type HouseholdRecord = {
  id: string;
  name: string;
  timeZone: string;
  initialBriefingState: "not_ready" | "preparing" | "sent";
  familyCalendarId: string | null;
  familyCalendarOwnerConnectionId: string | null;
  familyCalendarPartnerConnectionId: string | null;
  familyCalendarLabel: string | null;
  familyCalendarCreatedAt: string | null;
  members: readonly FamilyMemberRecord[];
  channels: readonly LinqChannelRecord[];
  facts: readonly FactRecord[];
  watches: readonly ProactiveWatchRecord[];
  googleConnections: readonly GoogleConnectionView[];
};

export type ProactiveWatchRecord = {
  id: string;
  kind: "monitor" | "interest";
  objective: string;
  currentConclusion: string | null;
  status: "active" | "paused";
  visibility: Visibility;
  ownerAdultId: string | null;
  source: SourceRecord | null;
};

export type SharedFamilyProfile = {
  householdId: string;
  familyLabel: string;
  timeZone: string;
  postalCode: string | null;
  adults: readonly { adultId: string; firstName: string; displayName: string }[];
  children: readonly {
    childId: string;
    firstName: string;
    displayName: string;
    age: number | null;
    grade: string | null;
    school: string | null;
    activities: readonly string[];
  }[];
};

export type SharedBriefingCandidate = {
  candidateId: string;
  category: "deadline" | "conflict" | "handoff" | "family_date" | "loose_end";
  summary: string;
  urgency: "now" | "soon" | "watch";
  dueAt: string | null;
  needsAnswer: boolean;
};

export type HouseholdDocket = {
  totalItems: number;
  items: readonly SharedBriefingCandidate[];
};

export type PrivateReviewFinding = {
  privateSummary: string;
  actionAnchorDigest?: string;
  sourceIds: readonly string[];
  householdCandidate: Omit<SharedBriefingCandidate, "candidateId"> | null;
  monitor?: InitialFiniteMonitorDraft | null;
  familyCalendar?: FamilyCalendarReviewProposal | null;
  urgency?: "now" | "soon" | "watch";
  dueAt?: string | null;
  surfaceNow?: boolean;
};

export type FamilyCalendarReviewProposal = {
  disposition: "automatic" | "suggest";
  sourceIds: readonly string[];
  event: CalendarEventDraft;
};

export type InitialFiniteMonitorDraft = {
  objective: string;
  currentConclusion: string;
  endCondition: string;
  nextCheck: string;
  why: string;
};

export type FamilyRelevance =
  | "child_care_school_or_activity"
  | "household_logistics"
  | "enrolled_adult_coordination"
  | "adult_only";

export type GoogleStableFactContext = {
  slot: string;
  statement: string;
};

export type GoogleStableFactDraft = GoogleStableFactContext & {
  familyRelevance: FamilyRelevance;
  sourceIds: readonly string[];
};

export type InitialGoogleScanFinding = PrivateReviewFinding & {
  familyRelevance: Exclude<FamilyRelevance, "adult_only">;
  urgency: "now" | "soon" | "watch";
  dueAt: string | null;
  surfaceNow: boolean;
  observedAt: string;
};

export type InitialGoogleScanFact = GoogleStableFactDraft & {
  observedAt: string;
  /** Per-support timestamps let a replay remove the newest support and recompute the true winner. */
  sourceObservations: readonly { sourceId: string; observedAt: string }[];
};

export type InitialGoogleScanCalendarTarget = {
  calendarId: string;
  timeZone: string;
  accessRole: "reader" | "writerWithoutPrivateAccess" | "writer" | "owner";
  primary: boolean;
  capturedCursor: string;
  baselinePageToken: string | null;
  baselineComplete: boolean;
  replayComplete: boolean;
  finalCursor: string | null;
  manifestPageToken: string | null;
  manifestComplete: boolean;
  manifestProviderEventIds: readonly string[];
  seenManifestPageTokenDigests: readonly string[];
  seenPageTokenDigests: readonly string[];
  seenEventIdentities: readonly { key: string; digest: string }[];
};

export type InitialGoogleScanNoEventCoverageTarget = {
  calendarId: string;
  timeZone: string;
  accessRole: "freeBusyReader";
  primary: boolean;
};

/**
 * Persistence-only envelope for one complete, resumable Google review. Opaque provider page tokens
 * live here and nowhere in model input, source metadata, Messages copy, or logs.
 */
export type InitialPrivateGoogleScanV1 = {
  kind: "initial_private_google_scan_v1";
  version: 1;
  scannerVersion: "complete_private_google_review_v1";
  connectionId: string;
  anchoredAt: string;
  gmailAfter: string;
  calendarTimeMin: string;
  calendarTimeMax: string;
  excludedFamilyCalendarId: string | null;
  phase:
    | "calendar_targets"
    | "gmail_baseline"
    | "calendar_baseline"
    | "gmail_replay"
    | "calendar_replay"
    | "calendar_verify"
    | "calendar_manifest"
    | "ready";
  gmail: {
    capturedCursor: string;
    baselinePageToken: string | null;
    baselineComplete: boolean;
    finalCursor: string | null;
    seenPageTokenDigests: readonly string[];
    seenMessageIdentities: readonly { key: string; digest: string }[];
  };
  calendar: {
    enumerationPass: number;
    /** True only after a complete post-manifest Gmail + every-Calendar replay has begun. */
    finalBarrierStarted: boolean;
    targetPageToken: string | null;
    seenTargetPageTokenDigests: readonly string[];
    verificationTargetIds: readonly string[];
    targets: readonly InitialGoogleScanCalendarTarget[];
    noEventCoverageTargets: readonly InitialGoogleScanNoEventCoverageTarget[];
  };
  outcomes: {
    findings: readonly InitialGoogleScanFinding[];
    facts: readonly InitialGoogleScanFact[];
  };
};

export type InitialIntelligenceWork =
  | {
      kind: "initial_private_review";
      workId: string;
      household: SharedFamilyProfile;
      adultId: string;
      adultFirstName: string;
      connectionId: string;
      familyCalendarId: string | null;
      currentFacts: readonly GoogleStableFactContext[];
      scan: InitialPrivateGoogleScanV1 | null;
    }
  | {
      kind: "initial_household_briefing";
      workId: string;
      household: SharedFamilyProfile;
      familyCalendarId: string;
      familyCalendarOwnerAdultId: string;
      familyCalendarOwnerConnectionId: string;
      candidates: readonly SharedBriefingCandidate[];
    };

export type InitialBriefingBubble = { text: string; delayMs: number };

export type CompletePrivateInitialReviewInput = {
  workId: string;
  generationKey: string;
  gmailCursor: string;
  calendarCursor: string;
  bubbles: readonly InitialBriefingBubble[];
  findings: readonly PrivateReviewFinding[];
  facts: readonly GoogleStableFactDraft[];
  googleEvidence: readonly GoogleEvidenceDraft[];
  rescanDeliverNotBefore: string;
  occurredAt: string;
};

export type CommitInitialPrivateGoogleScanPageInput = {
  workId: string;
  expectedStateDigest: string;
  nextScan: InitialPrivateGoogleScanV1;
  googleEvidence: readonly GoogleEvidenceDraft[];
  classifiedSourceIds: readonly string[];
  dismissedSourceIds: readonly string[];
  removedSourceIds: readonly string[];
  occurredAt: string;
};

export type CompleteHouseholdInitialBriefingInput = {
  workId: string;
  selectedCandidateIds: readonly string[];
  familyCalendarCursor: string;
  bubbles: readonly InitialBriefingBubble[];
  occurredAt: string;
};

export type ActiveFiniteMonitor = {
  monitorId: string;
  status: "active";
  objective: string;
  currentConclusion: string;
  endCondition: string;
  nextCheck: string;
  why: string;
};

export type ProactiveDelivery = {
  privateDetail: string | null;
  householdConclusion: string | null;
  householdCategory: SharedBriefingCandidate["category"] | null;
  householdNeedsAnswer?: boolean;
  sourceIds: readonly string[];
  /** Store-owned idempotency basis when committed Calendar echoes are only supporting context. */
  actionSourceIds?: readonly string[];
  actionAnchorDigest?: string;
  urgency: "now" | "soon" | "watch";
  dueAt?: string | null;
  /** A non-surfaced delivery still updates durable state and concrete work without sending prose. */
  surfaceNow?: boolean;
  /** Preserve the matching open docket item for an unchanged provider action. */
  preserveDocket?: boolean;
  monitor: ProactiveMonitorChange | null;
  familyCalendar?: FamilyCalendarReviewProposal | null;
};

export type ProactiveMonitorChange =
  | {
      operation: "create";
      monitorId: null;
      objective: string;
      currentConclusion: string;
      endCondition: string;
      nextCheck: string;
      why: string;
    }
  | {
      operation: "update" | "complete";
      monitorId: string;
      objective: string;
      currentConclusion: string;
      endCondition: string;
      nextCheck: string | null;
      why: string;
    };

/**
 * Directly adapts Hermes Agent's tagged cron schedule and action lifecycle
 * (6dcebea7, cron/jobs.py and tools/cronjob_tools.py) to family-local time.
 * Florence keeps the behavior here in PostgreSQL instead of importing
 * Hermes's JSON scheduler, agent sessions, or coding-job runtime.
 */
export type ReminderSchedule =
  | { kind: "once"; at: string }
  | { kind: "interval"; everyMinutes: number; anchorAt: string }
  | { kind: "daily"; everyDays: number; localTime: string; startsOn: string }
  | {
      kind: "weekly";
      everyWeeks: number;
      weekdays: readonly number[];
      localTime: string;
      startsOn: string;
    }
  | {
      kind: "monthly";
      everyMonths: number;
      dayOfMonth: number;
      localTime: string;
      startsOn: string;
    }
  | {
      kind: "yearly";
      everyYears: number;
      month: number;
      dayOfMonth: number;
      localTime: string;
      startsOn: string;
    };

export type VisibleReminder = {
  reminderId: string;
  action: string;
  schedule: ReminderSchedule;
  status: "active" | "paused" | "completed" | "cancelled";
  nextAt: string | null;
  lastRunAt: string | null;
  createdAt: string;
};

export type FamilyWorkStateV1 = {
  readonly kind: "family_work_v1";
  readonly version: 1;
  readonly generation: number;
  readonly phase: "ready" | "tool_pending" | "waiting" | "terminal";
  readonly claim: { readonly claimId: string; readonly leaseUntil: string } | null;
  readonly continuationItems: readonly JsonValue[];
  readonly pendingCall: {
    readonly callId: string;
    readonly name: string;
    readonly argumentsJson: string;
  } | null;
  readonly steering: readonly {
    readonly sourceId: string;
    readonly text: string;
    readonly occurredAt: string;
  }[];
  readonly publicMapResearchContext: readonly string[];
  readonly progressRevision: number;
  readonly terminal: {
    readonly outcome: "succeeded" | "partial" | "failed" | "cancelled";
    readonly text: string;
  } | null;
};

export type VisibleFamilyWork = {
  workId: string;
  objective: string;
  currentProgress: string;
  status: "active" | "waiting" | "delivering" | "completed" | "cancelled";
  createdAt: string;
};

export type SettleFamilyWorkClaimInput = {
  workId: string;
  generation: number;
  claimId: string;
  settledAt: string;
  result:
    | {
        type: "continue";
        state: FamilyWorkStateV1;
        nextCheckAt: string;
        progressText?: string | null;
      }
    | { type: "waiting"; state: FamilyWorkStateV1; question: string }
    | { type: "terminal"; state: FamilyWorkStateV1; terminalText: string }
    | { type: "retry"; state: FamilyWorkStateV1; retryAt: string; error: string };
};

export type DueProactiveWork =
  | { kind: "reminder"; workId: string }
  | {
      kind: "family_task";
      workId: string;
      household: SharedFamilyProfile;
      visibility: Visibility;
      ownerAdultId: string | null;
      objective: string;
      state: FamilyWorkStateV1;
      claimId: string;
      generation: number;
    }
  | {
      kind: "personal_google_poll";
      workId: string;
      household: SharedFamilyProfile;
      visibility: "private";
      adultId: string;
      adultFirstName: string;
      connectionId: string;
      gmailCursor: string;
      calendarId: "primary";
      excludedFamilyCalendarId: string | null;
      calendarCursor: string;
      activeMonitors: readonly ActiveFiniteMonitor[];
      currentFacts: readonly GoogleStableFactContext[];
    }
  | {
      kind: "family_calendar_poll";
      workId: string;
      household: SharedFamilyProfile;
      visibility: "household";
      adultId: string;
      adultFirstName: string;
      connectionId: string;
      calendarId: string;
      calendarCursor: string;
      activeMonitors: readonly ActiveFiniteMonitor[];
    }
  | {
      kind: "finite_monitor";
      workId: string;
      household: SharedFamilyProfile;
      visibility: Visibility;
      adultId: string;
      adultFirstName: string;
      connectionId: string;
      calendarId: string;
      monitor: ActiveFiniteMonitor;
    }
  | {
      kind: "interest_monitor";
      workId: string;
      household: SharedFamilyProfile;
      connectionId: string;
      ownerAdultId: string;
      calendarId: string;
      genericInterestTerms: readonly string[];
      coarseLocation: string;
    };

export type CompleteFounderOnboardingInput = {
  setupTokenDigest: string;
  setupExpiresAt: string;
  householdId: string;
  timeZone: string;
  adultId: string;
  firstName: string;
  lastName: string;
  messagesAddress: string;
  identitySubjectDigest: string;
  consentVersion: string;
  consentedAt: string;
  guardianAttestedAt: string;
  proactiveUseAcceptedAt: string;
  privateConflictBusySharingEnabled: boolean;
  providerConversationId: string;
  occurredAt: string;
};

export type CompleteFamilyOnboardingInput = {
  householdId: string;
  founderAdultId: string;
  postalCode: string;
  mode: "two_adult" | "solo";
  partner: { firstName: string; lastName: string; phoneNumber: string } | null;
  children: readonly {
    firstName: string;
    lastName?: string;
    age?: number;
    grade?: string;
    school?: string;
    activities?: readonly string[];
  }[];
  occurredAt: string;
};

export type UpsertMemberInput = {
  householdId: string;
  actorAdultId: string;
  memberId: string;
  member:
    | {
        operation: "create";
        kind: "child";
        firstName: string;
        lastName: string | null;
        profile?: JsonObject;
      }
    | {
        operation: "patch";
        firstName?: string;
        lastName?: string | null;
        profile?: JsonObject;
      };
  occurredAt: string;
};

export type IssueMessagesEnrollmentInput = {
  householdId: string;
  actorAdultId: string;
  adultId: string;
  challengeDigest: string;
  providerConversationId: string;
  identitySubjectDigest: string;
  messagesAddress: string;
  providerMessageId: string;
  expiresAt: string;
  issuedAt: string;
};

export type RedeemMessagesEnrollmentInput = {
  challengeDigest: string;
  identitySubjectDigest: string;
  firstName: string;
  lastName: string;
  messagesAddress: string;
  guardianAttestedAt: string;
  proactiveUseAcceptedAt: string;
  privateConflictBusySharingEnabled: boolean;
  consentVersion: string;
  consentedAt: string;
  providerConversationId: string;
  occurredAt: string;
};

export type MessagesEnrollmentResult = {
  disposition: "accepted" | "duplicate";
  householdId: string;
  adultId: string;
  channel: LinqChannelRecord;
};

export type InboundDocumentInput = {
  documentId: string;
  externalKey: string;
  filename: string;
  mimeType: string;
  contentDigest: string;
  contentEnvelope: Uint8Array;
  discardAfter?: string;
};

export type AcceptInboundInput = {
  providerConversationId: string;
  audience: Audience;
  participantIdentityDigests: readonly string[];
  senderIdentitySubjectDigest: string;
  providerEventId: string;
  providerMessageId: string;
  replyToProviderMessageId?: string | null;
  text: string | null;
  authoredText?: string | null;
  voiceTranscriptPresent?: boolean;
  images?: readonly ImageReference[];
  documents?: readonly InboundDocumentInput[];
  providerPayloadDigest?: string;
  supersedesSourceId?: string | null;
  occurredAt: string;
};

export type AcceptInboundEnvelopeInput = Omit<
  AcceptInboundInput,
  | "text"
  | "authoredText"
  | "voiceTranscriptPresent"
  | "images"
  | "documents"
  | "providerPayloadDigest"
  | "supersedesSourceId"
> & {
  providerPayloadDigest: string;
};

export type InboundPreparationContext = {
  sourceId: string;
  householdId: string;
};

export type PreparedInboundContent = Pick<
  AcceptInboundInput,
  "text" | "authoredText" | "voiceTranscriptPresent" | "images" | "documents"
>;

export type AcceptInboundReactionInput = {
  providerConversationId: string;
  audience: Audience;
  participantIdentityDigests: readonly string[];
  senderIdentitySubjectDigest: string;
  providerEventId: string;
  targetProviderMessageId: string;
  reaction: string;
  partIndex: number;
  occurredAt: string;
};

export type AcceptInboundResult = {
  disposition: "accepted" | "duplicate" | "stopped";
  sourceId: string;
  householdId: string;
  channelId: string;
};

export type FamilyGroupObservationResult = "current" | "mismatch" | "retired";

export type FamilyGroupCreationWork = {
  householdId: string;
  createChatIdempotencyKey: string;
  participantPhoneNumbers: readonly [string, string];
  adultFirstNames: readonly [string, string];
};

export type LinqAuthority = {
  householdId: string;
  channelId: string;
  audience: Audience;
  providerConversationId: string;
  senderAdultId: string;
  adultIds: readonly string[];
  expectedParticipantIdentityDigests: readonly string[];
  authorityDigest: string;
  replyToSourceId: string | null;
  stopped: boolean;
};

export type ConversationTurn = {
  sourceId: string;
  speaker: "florence" | string;
  moveKind: "message" | "reply" | "reaction";
  text: string | null;
  authoredText: string | null;
  voiceTranscriptPresent: boolean;
  reaction: string | null;
  images: readonly ImageReference[];
  replyToSourceId: string | null;
  occurredAt: string;
};

export type PendingFollowUp = {
  id: string;
  objective: string;
  currentConclusion: string;
  endCondition: string;
  nextCheck: string;
  why: string;
  sourceIds: readonly string[];
  googleBacked: boolean;
};

export type VisibleHouseholdInterest = {
  interestWorkId: string;
  status: "active" | "paused";
  genericTerms: readonly string[];
  objective: string;
  why: string;
};

export type PendingPartnerInvitation = {
  adultId: string;
  firstName: string;
  maskedPhoneNumber: string;
  approvalPromptSourceId: string;
  /** Internal delivery address. Never include this field in reasoner input. */
  phoneNumber: string;
};

export type UnboundPartnerInvitation =
  | {
      adultId: string;
      state: "declined";
    }
  | {
      adultId: string;
      state: "expired";
      linkIssued: boolean;
    }
  | {
      adultId: string;
      state: "awaiting_reply" | "issued";
      householdId: string;
      founderAdultId: string;
      messagesAddress: string;
      providerConversationId: string;
      identitySubjectDigest: string;
      initialProviderMessageId: string;
      handshakeAt: string;
      setupIssuedAt: string | null;
    };

export type InboundTurn = {
  message: ConversationTurn & { speaker: string };
  supersededMessages: readonly (ConversationTurn & { speaker: string })[];
  replyTarget: ConversationTurn | null;
  authority: LinqAuthority;
  household: {
    id: string;
    name: string;
    timeZone: string;
    familyCalendarId: string | null;
    familyCalendarOwnerConnectionId: string | null;
    familyCalendarPartnerConnectionId: string | null;
    familyCalendarLabel: string | null;
    familyCalendarCreatedAt: string | null;
    members: readonly FamilyMemberRecord[];
  };
  facts: readonly FactRecord[];
  currentDocuments?: readonly CurrentMessageDocument[];
  recentMessages: readonly ConversationTurn[];
  pendingFollowUps: readonly PendingFollowUp[];
  householdDocket: HouseholdDocket;
  visibleFamilyWork: readonly VisibleFamilyWork[];
  visibleReminders: readonly VisibleReminder[];
  visibleInterests: readonly VisibleHouseholdInterest[];
  pendingCalendarOffers: readonly CalendarOffer[];
  pendingPartnerInvitation: PendingPartnerInvitation | null;
};

export type FactDraft = Omit<FactRecord, "householdId" | "sources" | "correctedAt" | "updatedAt"> & {
  sourceIds: readonly string[];
};

export type FiniteMonitorDraft = {
  id: string;
  objective: string;
  currentConclusion: string;
  endCondition: string;
  nextCheck: string;
  why: string;
  visibility: Visibility;
  ownerAdultId: string | null;
  sourceIds: readonly string[];
};

export type FiniteMonitorUpdate = {
  id: string;
  objective: string;
  currentConclusion: string;
  endCondition: string;
  nextCheck: string;
  why: string;
  sourceIds: readonly string[];
};

export type DurableInterestMutation =
  | {
      operation: "create";
      interestWorkId: null;
      genericTerms: readonly string[];
      objective: string;
      why: string;
      sourceIds: readonly string[];
    }
  | {
      operation: "update";
      interestWorkId: string;
      genericTerms: readonly string[];
      objective: string;
      why: string;
      sourceIds: readonly string[];
    }
  | {
      operation: "stop";
      interestWorkId: string;
      genericTerms: null;
      objective: null;
      why: string;
      sourceIds: readonly string[];
    };

export type OutboundDraft = {
  sourceId: string;
  idempotencyKey: string;
  moveKind: "message" | "reply" | "reaction";
  text?: string | null;
  reaction?: string | null;
  replyToSourceId?: string | null;
  turnId: string;
  turnPart: -1 | 0 | 1 | 2;
  notBefore: string;
};

export type CalendarEventDraft =
  | {
      intervalKind: "timed";
      title: string;
      startsAt: string;
      endsAt: string;
      timeZone: string;
      location: string | null;
    }
  | {
      intervalKind: "all_day";
      title: string;
      startDate: string;
      endDate: string;
      location: string | null;
    };

export type CalendarEventTarget = {
  providerEventId: string;
  providerRevision: string;
  observedEvent: CalendarEventDraft;
};

export type FamilyCalendarMutation =
  | { operation: "create"; event: CalendarEventDraft; target: null }
  | { operation: "update"; event: CalendarEventDraft; target: CalendarEventTarget }
  | { operation: "delete"; event: null; target: CalendarEventTarget };

export type CalendarActionDraft = {
  id: string;
  basisSourceId: string;
  mutation: FamilyCalendarMutation;
};

export type CalendarOfferDraft = {
  id: string;
  basisSourceId: string;
  mutation: Extract<FamilyCalendarMutation, { operation: "create" }>;
};

export type CalendarOfferApproval = { offerId: string };

export type PartnerInvitationApproval = { adultId: string };

export type ReminderMutation =
  | {
      operation: "create";
      reminderId: string;
      action: string;
      schedule: ReminderSchedule;
      visibility: Visibility;
      ownerAdultId: string | null;
    }
  | {
      operation: "update";
      reminderId: string;
      action: string | null;
      schedule: ReminderSchedule | null;
    }
  | {
      operation: "pause" | "resume" | "cancel" | "run";
      reminderId: string;
    };

export type FamilyWorkMutation =
  | {
      operation: "create";
      workId: string;
      objective: string;
      visibility: Visibility;
      ownerAdultId: string | null;
    }
  | { operation: "steer"; workId: string; instruction: string }
  | { operation: "cancel"; workId: string };

export type ApprovedPartnerInvitation = {
  householdId: string;
  founderAdultId: string;
  founderChannelId: string;
  founderProviderConversationId: string;
  partnerAdultId: string;
  partnerFirstName: string;
  partnerPhoneNumber: string;
  approvalSourceId: string;
  approvedAt: string;
};

export type CalendarOffer = {
  id: string;
  approvalPromptSourceId: string;
  event: CalendarEventDraft;
};

export type CommitTurnInput = {
  sourceId: string;
  googleEvidence?: readonly GoogleEvidenceDraft[];
  googleConnectionIdsUsed?: readonly string[];
  facts?: readonly FactDraft[];
  deleteFactIds?: readonly string[];
  finiteMonitors?: readonly FiniteMonitorDraft[];
  finiteMonitorUpdates?: readonly FiniteMonitorUpdate[];
  cancelMonitorIds?: readonly string[];
  interestMutation?: DurableInterestMutation | null;
  reminderMutation?: ReminderMutation | null;
  familyWorkMutation?: FamilyWorkMutation | null;
  completeDocketCandidateIds?: readonly string[];
  outbound?: readonly OutboundDraft[];
  calendarOffers?: readonly CalendarOfferDraft[];
  approveCalendarOffers?: readonly CalendarOfferApproval[];
  calendarActions?: readonly CalendarActionDraft[];
  partnerInvitationApproval?: PartnerInvitationApproval;
  householdUpdate?: { basisSourceId: string; text: string };
  stopChannel?: boolean;
  handledAt: string;
};

export type CommitTurnResult = "committed" | "superseded";

export type OutboundMessage = {
  sourceId: string;
  idempotencyKey: string;
  providerConversationId: string;
  expectedAuthority: { audience: Audience; participantIdentityDigests: readonly string[] };
  moveKind: "message" | "reply" | "reaction";
  text: string | null;
  reaction: string | null;
  replyToProviderMessageId: string | null;
};

export type LinqOutboundObservation =
  | {
      kind: "message_status";
      providerEventId: string;
      providerConversationId: string;
      providerMessageId: string;
      idempotencyKey: string | null;
      status: "sent" | "delivered" | "read" | "failed";
      occurredAt: string;
      traceId: string;
      failure: { code: number; reason: string | null } | null;
    }
  | {
      kind: "reaction";
      providerEventId: string;
      providerConversationId: string;
      targetProviderMessageId: string;
      operation: "added" | "removed";
      reaction: string;
      partIndex: number;
      isFromMe: boolean;
      occurredAt: string;
      traceId: string;
    };

export type LinqObservationResult = "applied" | "duplicate" | "unmatched";

export type ApprovedCalendarAction = {
  actionId: string;
  householdId: string;
  connectionId: string;
  ownerAdultId: string;
  calendarId: string;
  mutation: FamilyCalendarMutation;
  personalCalendarOwnerApproved: boolean;
};

export type ActiveFamilyCalendarCredential = {
  householdId: string;
  connectionId: string;
  ownerAdultId: string;
  calendarId: string;
};

export type PendingGoogleConnection = {
  connectionId: string;
  householdId: string;
  ownerAdultId: string;
  stateDigest: string;
  sessionBindingDigest: string;
};

export type ActiveGoogleCredential = {
  connectionId: string;
  householdId: string;
  ownerAdultId: string;
  refreshTokenEnvelope: string;
};

export type GoogleDataPurgeResult = {
  additionalActiveConnectionsDisconnected: number;
  googleSources: number;
  facts: number;
  watches: number;
  calendarActions: number;
  unsentMessages: number;
};

type PersonRow = {
  id: string;
  household_id: string;
  kind: "adult" | "child";
  role: "steward" | "caregiver" | "dependent";
  adult_slot: 1 | 2 | null;
  display_name: string;
  status: "planned" | "verified" | "represented";
  identity_subject_digest: string | null;
  consent_version: string | null;
  consented_at: Date | null;
  guardian_attested_at: Date | null;
  invitation_digest: string | null;
  invitation_expires_at: Date | null;
  invitation_consumed_at: Date | null;
  messages_address: string | null;
  invitation_conversation_id: string | null;
  invitation_identity_digest: string | null;
  invitation_message_id: string | null;
  invitation_issued_at: Date | null;
  invitation_approval_source_id: string | null;
  invitation_approved_at: Date | null;
  invitation_retry_at: Date | null;
  invitation_last_error: string | null;
  profile: JsonObject;
  preferences: JsonObject;
};

type IssuedPartnerInvitationRow = {
  adult_id: string;
  household_id: string;
  first_name: string;
  invitation_message_id: string;
  link_issued: boolean;
  founder_adult_id: string;
  founder_channel_id: string;
};

type ChannelRow = {
  id: string;
  household_id: string;
  audience: Audience;
  provider_conversation_id: string;
  adult_one_id: string;
  identity_one_digest: string;
  adult_two_id: string | null;
  identity_two_digest: string | null;
  authority_digest: string;
  bound_at: Date;
  revoked_at: Date | null;
  stopped_at: Date | null;
};

type SourceRow = {
  id: string;
  kind: SourceRecord["kind"];
  visibility: Visibility;
  owner_adult_id: string | null;
  label: string;
  metadata: JsonObject;
  occurred_at: Date;
};

type GoogleConnectionRow = {
  id: string;
  household_id: string;
  owner_adult_id: string;
  status: GoogleConnectionStatus;
  state_digest: string;
  session_binding_digest: string | null;
  state_expires_at: Date;
  state_consumed_at: Date | null;
  google_subject_digest: string | null;
  email_label: string | null;
  granted_scopes: GoogleScope[];
  refresh_token_envelope: string | null;
  last_error: string | null;
  created_at: Date;
  updated_at: Date;
};

type ProactiveWorkRow = {
  id: string;
  household_id: string;
  kind:
    | "initial_private_review"
    | "initial_household_briefing"
    | "personal_google_poll"
    | "family_calendar_poll"
    | "finite_monitor"
    | "interest_monitor"
    | "reminder"
    | "family_task";
  visibility: Visibility;
  owner_adult_id: string | null;
  objective: string | null;
  why: string | null;
  current_conclusion: string | null;
  end_condition: string | null;
  discovery_terms: string[];
  gmail_cursor: string | null;
  calendar_cursor: string | null;
  briefing_candidates: JsonValue;
  status: "active" | "paused" | "delivering" | "completed" | "cancelled";
  next_check_at: Date | null;
  reminder_schedule: JsonValue | null;
  last_run_at: Date | null;
  task_state: JsonValue | null;
  last_error: string | null;
  created_at: Date;
};

type LinqObservationRow = {
  source_id: string;
  status: "pending" | "sending" | "sent" | "failed";
  move_kind: "message" | "reply" | "reaction";
  provider_message_id: string | null;
  sent_at: Date | null;
  receipt_detail: JsonValue | null;
  not_before: Date;
  source_occurred_at: Date;
};

type FamilyCalendarAuthorityRow = {
  household_id: string;
  family_calendar_id: string | null;
  family_calendar_owner_connection_id: string | null;
  family_calendar_partner_connection_id: string | null;
  family_calendar_created_at: Date | null;
  founder_adult_id: string | null;
  founder_identity_digest: string | null;
  founder_status: "planned" | "verified" | "represented" | null;
  founder_connection_status: GoogleConnectionStatus | null;
  founder_preferences: JsonValue | null;
  partner_adult_id: string | null;
  partner_identity_digest: string | null;
  partner_status: "planned" | "verified" | "represented" | null;
  partner_connection_status: GoogleConnectionStatus | null;
  partner_preferences: JsonValue | null;
};

type FamilyGroupAuthorityRow = Pick<
  FamilyCalendarAuthorityRow,
  | "founder_adult_id"
  | "founder_identity_digest"
  | "founder_status"
  | "partner_adult_id"
  | "partner_identity_digest"
  | "partner_status"
>;

type CalendarActionAuthorityRow = {
  id: string;
  status: "offered" | "pending" | "committed" | "failed";
  household_id: string;
  basis_source_id: string | null;
  approval_source_id: string | null;
  approval_prompt_source_id: string | null;
  google_action_key: string | null;
  legacy_google_review_basis: boolean;
  payload: JsonValue;
  provider_event_id: string | null;
  provider_etag: string | null;
  committed_at: Date | null;
  retry_at: Date;
  channel_id: string | null;
  direction: "inbound" | "outbound" | null;
  sender_adult_id: string | null;
  channel_audience: Audience | null;
  provider_conversation_id: string | null;
  adult_one_id: string | null;
  identity_one_digest: string | null;
  adult_two_id: string | null;
  identity_two_digest: string | null;
  authority_digest: string | null;
  bound_at: Date | null;
  revoked_at: Date | null;
  stopped_at: Date | null;
};

export class PostgresFlorenceStore {
  readonly #sql: ReturnType<typeof postgres>;
  #closed = false;

  constructor(options: string | PostgresFlorenceStoreOptions) {
    const { connectionString, maxConnections = 10 } =
      typeof options === "string" ? { connectionString: options, maxConnections: 10 } : options;
    this.#sql = postgres(connectionString, { max: maxConnections });
  }

  async ready(): Promise<void> {
    const [row] = await this.#sql<{ ready: boolean }[]>`
      select to_regclass('public.households') is not null
        and to_regclass('public.messages') is not null
        and to_regclass('public.calendar_actions') is not null as ready
    `;
    if (!row?.ready) throw new Error("The direct Florence database baseline is not installed");
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    await this.#sql.end({ timeout: 5 });
  }

  readProductionResetSnapshot(): Promise<ProductionResetSnapshot> {
    return this.#sql.begin("isolation level repeatable read read only", (sql) =>
      readProductionResetSnapshot(sql),
    );
  }

  /**
   * Makes an exact marker discovery durable before deletion. This is the only mutation between a
   * reviewed reset snapshot and Calendar deletion, and it is conditioned on that complete snapshot
   * so a failed reset can safely resume by provider ID.
   */
  recoverProductionResetCalendarIds(
    expected: ProductionResetSnapshot,
    recoveries: readonly ProductionResetCalendarRecovery[],
  ): Promise<ProductionResetSnapshot> {
    return this.#sql.begin(async (sql) => {
      await lockProductionHouseholdData(sql);
      const current = await readProductionResetSnapshot(sql);
      if (!sameProductionResetSnapshot(current, expected)) {
        throw new FlorenceStoreConflict(
          "Production household data changed after the reset snapshot was inspected",
        );
      }
      const unresolved = current.calendars.filter((calendar) => calendar.calendarId === null);
      if (recoveries.length > unresolved.length) {
        throw new FlorenceStoreConflict(
          "Production reset Calendar recoveries exceed the ambiguous create attempts",
        );
      }
      const expectedHouseholds = new Set(unresolved.map((calendar) => calendar.householdId));
      const seenHouseholds = new Set<string>();
      const seenCalendarIds = new Set<string>();
      for (const recovery of recoveries) {
        assertUuid(recovery.householdId, "Production reset Calendar recovery household ID");
        const calendarId = required(recovery.calendarId, "Production reset recovered Calendar ID");
        if (calendarId === "primary") {
          throw new FlorenceStoreConflict("A production reset cannot target a primary Calendar");
        }
        if (
          !expectedHouseholds.has(recovery.householdId) ||
          seenHouseholds.has(recovery.householdId) ||
          seenCalendarIds.has(calendarId)
        ) {
          throw new FlorenceStoreConflict("Production reset Calendar recovery is not one-to-one");
        }
        seenHouseholds.add(recovery.householdId);
        seenCalendarIds.add(calendarId);
        const updated = await sql`
          update households set family_calendar_id=${calendarId},updated_at=now()
          where id=${recovery.householdId} and family_calendar_id is null
            and family_calendar_create_attempted_at is not null
            and family_calendar_owner_connection_id is null
            and family_calendar_partner_connection_id is null
            and family_calendar_label is null and family_calendar_created_at is null
          returning id
        `;
        if (updated.length !== 1) {
          throw new FlorenceStoreConflict("The ambiguous Family Calendar create state changed");
        }
      }
      const recovered = await readProductionResetSnapshot(sql);
      for (const recovery of recoveries) {
        const target = recovered.calendars.find((calendar) => calendar.householdId === recovery.householdId);
        if (target?.calendarId !== recovery.calendarId) {
          throw new FlorenceStoreConflict("A discovered Family Calendar ID was not recovered exactly");
        }
      }
      return recovered;
    });
  }

  /**
   * The caller must first confirm every snapshot calendar is absent at Google. This transaction
   * then excludes writers, proves the inspected snapshot is still exact, and removes only product
   * data. `florence_schema_migrations` and the installed baseline remain intact.
   */
  truncateProductionHouseholdData(expected: ProductionResetSnapshot): Promise<ProductionResetResult> {
    return this.#sql.begin(async (sql) => {
      await lockProductionHouseholdData(sql);
      const current = await readProductionResetSnapshot(sql);
      if (!sameProductionResetSnapshot(current, expected)) {
        throw new FlorenceStoreConflict(
          "Production household data changed after the reset snapshot was inspected",
        );
      }
      await sql.unsafe(`
        truncate table proactive_work_sources,fact_sources,documents,messages,calendar_actions,
          proactive_work,sources,linq_channels,google_connections,facts,people,households
      `);
      return Object.freeze({
        deletedHouseholds: current.householdCount,
        deletedCalendars: current.calendars.length,
        deletedActiveGoogleCredentials: current.activeGoogleCredentials.length,
      });
    });
  }

  async scopeHouseholdLinqIdempotencyKey(input: {
    householdId: string;
    idempotencyKey: string;
  }): Promise<string> {
    assertUuid(input.householdId, "Household ID");
    return householdLinqIdempotencyKey(this.#sql, input.householdId, input.idempotencyKey);
  }

  async listHouseholdIdsForAdult(adultId: string): Promise<readonly string[]> {
    const rows = await this.#sql<{ household_id: string }[]>`
      select household_id from people
      where id=${adultId} and kind='adult' and status='verified'
      order by household_id
    `;
    return rows.map((row) => row.household_id);
  }

  async readHousehold(input: {
    householdId: string;
    viewerAdultId?: string;
  }): Promise<HouseholdRecord | null> {
    if (input.viewerAdultId) {
      const [viewer] = await this.#sql<{ id: string }[]>`
        select id from people where household_id=${input.householdId}
          and id=${input.viewerAdultId} and kind='adult' and status='verified'
      `;
      if (!viewer) return null;
    }
    const [household] = await this.#sql<
      {
        id: string;
        name: string;
        time_zone: string;
        family_calendar_id: string | null;
        family_calendar_owner_connection_id: string | null;
        family_calendar_partner_connection_id: string | null;
        family_calendar_label: string | null;
        family_calendar_created_at: Date | null;
      }[]
    >`
      select id,name,time_zone,family_calendar_id,family_calendar_owner_connection_id,
             family_calendar_partner_connection_id,family_calendar_label,family_calendar_created_at
      from households where id=${input.householdId}
    `;
    if (!household) return null;

    const members = await this.#sql<PersonRow[]>`
      select id,household_id,kind,role,adult_slot,display_name,status,
             identity_subject_digest,consent_version,consented_at,guardian_attested_at,
             invitation_digest,invitation_expires_at,invitation_consumed_at,messages_address,
             invitation_conversation_id,invitation_identity_digest,invitation_message_id,
             invitation_issued_at,invitation_approval_source_id,
             invitation_approved_at,invitation_retry_at,invitation_last_error,profile,preferences
      from people where household_id=${input.householdId}
      order by adult_slot nulls last,created_at,id
    `;
    const channels = await this.#sql<ChannelRow[]>`
      select * from linq_channels where household_id=${input.householdId}
      order by bound_at,id
    `;
    const facts = await this.#readFacts(input.householdId, input.viewerAdultId ?? null);
    const watches = await this.#readProactiveWatches(input.householdId, input.viewerAdultId ?? null);
    const [initialBriefing] = await this.#sql<{ id: string; status: "active" | "paused" | "completed" }[]>`
      select id,status from proactive_work where household_id=${input.householdId}
        and kind='initial_household_briefing'
      order by created_at,id limit 1
    `;
    let initialBriefingState: HouseholdRecord["initialBriefingState"] = "not_ready";
    if (initialBriefing) {
      initialBriefingState = "preparing";
      if (initialBriefing.status === "completed") {
        const briefingMessages = await this.#sql<{ status: string }[]>`
          select status from messages
          where idempotency_key like ${`initial-household-briefing:${initialBriefing.id}:%`}
          order by turn_part
        `;
        if (
          briefingMessages.length >= 1 &&
          briefingMessages.length <= 3 &&
          briefingMessages.every((message) => message.status === "sent")
        ) {
          initialBriefingState = "sent";
        }
      }
    }
    const googleRows = await this.#sql<GoogleConnectionRow[]>`
      select * from google_connections where household_id=${input.householdId}
        ${input.viewerAdultId ? this.#sql`and owner_adult_id=${input.viewerAdultId}` : this.#sql``}
      order by created_at,id
    `;
    const memberRecords = members.map(personRecord);
    const channelRecords = channels.map(channelRecord);
    return {
      id: household.id,
      name: household.name,
      timeZone: household.time_zone,
      initialBriefingState,
      familyCalendarId: household.family_calendar_id,
      familyCalendarOwnerConnectionId: household.family_calendar_owner_connection_id,
      familyCalendarPartnerConnectionId: household.family_calendar_partner_connection_id,
      familyCalendarLabel: household.family_calendar_label,
      familyCalendarCreatedAt: household.family_calendar_created_at?.toISOString() ?? null,
      members: memberRecords,
      channels: channelRecords,
      facts,
      watches,
      googleConnections: googleRows.map(googleConnectionView),
    };
  }

  async readHouseholdDocket(input: {
    householdId: string;
    limit?: number;
    now?: string;
  }): Promise<HouseholdDocket> {
    assertUuid(input.householdId, "Household ID");
    const now = instant(input.now ?? new Date().toISOString());
    const limit = input.limit ?? 20;
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) {
      throw new FlorenceStoreConflict("A household docket limit must be between one and one hundred");
    }
    const [household] = await this.#sql<{ time_zone: string }[]>`
      select time_zone from households where id=${input.householdId}
    `;
    if (!household) throw new FlorenceStoreConflict("The household docket no longer exists");
    const reviews = await this.#sql<
      (ProactiveWorkRow & { private_conflict_busy_sharing_enabled: boolean })[]
    >`
      select work.*,
        coalesce(person.preferences->'privateConflictBusySharingEnabled'='true'::jsonb,false)
          as private_conflict_busy_sharing_enabled
      from proactive_work work join people person
        on person.household_id=work.household_id and person.id=work.owner_adult_id
      where work.household_id=${input.householdId}
        and work.kind='initial_private_review' and work.status='completed'
        and person.kind='adult' and person.status='verified'
        and nullif(person.preferences->>'proactiveUseAcceptedAt','') is not null
        and coalesce(person.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
      order by work.owner_adult_id,work.id
    `;
    const groups = rankedBriefingCandidateGroups(
      reviews.flatMap((review) => {
        if (!review.owner_adult_id) return [];
        return storedBriefingCandidates(review.briefing_candidates)
          .filter(
            (candidate) =>
              isCurrentDocketCandidate(candidate, now, household.time_zone) &&
              (candidate.category !== "conflict" || review.private_conflict_busy_sharing_enabled),
          )
          .map((candidate) => ({ candidate, ownerAdultId: review.owner_adult_id as string }));
      }),
    );
    return {
      totalItems: groups.length,
      items: groups
        .slice(0, limit)
        .map(
          ({
            candidate: {
              sourceIds: _sourceIds,
              actionAnchorDigest: _actionAnchorDigest,
              actionKey: _actionKey,
              ...candidate
            },
          }) => candidate,
        ),
    };
  }

  async ensureInitialIntelligence(input: { householdId: string; now: string }): Promise<void> {
    assertUuid(input.householdId, "Household ID");
    const now = instant(input.now);
    await this.#sql.begin(async (sql) => {
      const [household] = await sql<
        {
          id: string;
          family_calendar_id: string | null;
          family_calendar_created_at: Date | null;
        }[]
      >`
        select id,family_calendar_id,family_calendar_created_at
        from households where id=${input.householdId} for update
      `;
      if (!household) return;
      const eligible = await sql<
        {
          adult_id: string;
          connection_created_at: Date;
        }[]
      >`
        select p.id as adult_id,g.created_at as connection_created_at
        from people p
        join google_connections g on g.household_id=p.household_id
          and g.owner_adult_id=p.id and g.status='active'
        join linq_channels c on c.household_id=p.household_id and c.audience='private'
          and c.adult_one_id=p.id and c.adult_two_id is null
          and c.revoked_at is null and c.stopped_at is null
        where p.household_id=${input.householdId} and p.kind='adult' and p.status='verified'
          and p.guardian_attested_at is not null
          and nullif(p.profile->>'onboardingCompletedAt','') is not null
          and nullif(p.preferences->>'proactiveUseAcceptedAt','') is not null
          and coalesce(p.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
          and g.granted_scopes @> ${sql.array([COMPLETE_CALENDAR_HISTORY_SCOPE])}
        order by p.adult_slot,p.id,g.created_at desc,g.id desc
      `;
      const latestEligible = eligible.filter(
        (adult, index) => eligible.findIndex((candidate) => candidate.adult_id === adult.adult_id) === index,
      );
      for (const adult of latestEligible) {
        const reviewId = deterministicUuid(`initial-private-review\0${adult.adult_id}`);
        const [existing] = await sql<{ status: string; created_at: Date }[]>`
          select status,created_at from proactive_work where id=${reviewId} for update
        `;
        if (
          (existing?.status === "completed" || existing?.status === "active") &&
          adult.connection_created_at.getTime() > existing.created_at.getTime()
        ) {
          // A same-household reauthorization upgrades an existing adult in place. Reusing the
          // review identity preserves its already-sent idempotency keys, while resetting the
          // durable scan envelope and pausing the old cursor until complete coverage closes.
          await sql`
            update proactive_work set status='active',next_check_at=${now},created_at=${now},
              briefing_candidates='[]'::jsonb,last_error=null where id=${reviewId}
          `;
          await sql`
            update proactive_work set status='paused',next_check_at=null,last_error=null
            where household_id=${input.householdId} and owner_adult_id=${adult.adult_id}
              and kind='personal_google_poll' and status='active'
          `;
          continue;
        }
        await sql`
          insert into proactive_work (
            id,household_id,kind,visibility,owner_adult_id,status,next_check_at,created_at
          ) values (${reviewId},${input.householdId},'initial_private_review','private',
            ${adult.adult_id},'active',${now},${now})
          on conflict do nothing
        `;
      }
      const [group] = await sql<{ id: string }[]>`
        select id from linq_channels where household_id=${input.householdId} and audience='group'
          and adult_two_id is not null and revoked_at is null and stopped_at is null
        order by bound_at,id limit 1 for share
      `;
      if (group && household.family_calendar_id && household.family_calendar_created_at) {
        await sql`
          insert into proactive_work (
            id,household_id,kind,visibility,status,next_check_at,created_at
          ) values (${deterministicUuid(`initial-household-briefing\0${input.householdId}`)},
            ${input.householdId},'initial_household_briefing','household','active',${now},${now})
          on conflict do nothing
        `;
      }
    });
  }

  async readNextInitialIntelligence(nowInput: string): Promise<InitialIntelligenceWork | null> {
    const now = instant(nowInput);
    return this.#sql.begin(async (sql) => {
      const [privateWork] = await sql<
        (ProactiveWorkRow & {
          adult_first_name: string | null;
          adult_display_name: string;
          connection_id: string;
          family_calendar_id: string | null;
        })[]
      >`
        select w.*,p.profile->>'firstName' as adult_first_name,p.display_name as adult_display_name,
          g.id as connection_id,h.family_calendar_id
        from proactive_work w
        join households h on h.id=w.household_id
        join people p on p.household_id=w.household_id and p.id=w.owner_adult_id
        join google_connections g on g.household_id=w.household_id
          and g.owner_adult_id=w.owner_adult_id
        join linq_channels c on c.household_id=w.household_id and c.audience='private'
          and c.adult_one_id=w.owner_adult_id and c.adult_two_id is null
        where w.kind='initial_private_review' and w.status='active'
          and w.next_check_at<=${now}
          and p.kind='adult' and p.status='verified' and p.guardian_attested_at is not null
          and nullif(p.profile->>'onboardingCompletedAt','') is not null
          and nullif(p.preferences->>'proactiveUseAcceptedAt','') is not null
          and coalesce(p.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
          and g.status='active'
          and g.granted_scopes @> ${sql.array([COMPLETE_CALENDAR_HISTORY_SCOPE])}
          and c.revoked_at is null and c.stopped_at is null
        order by w.next_check_at,w.id,g.created_at desc,g.id desc limit 1
      `;
      if (privateWork?.owner_adult_id && privateWork.connection_id) {
        return {
          kind: "initial_private_review" as const,
          workId: privateWork.id,
          household: await sharedFamilyProfile(sql, privateWork.household_id),
          adultId: privateWork.owner_adult_id,
          adultFirstName: privateWork.adult_first_name ?? privateWork.adult_display_name,
          connectionId: privateWork.connection_id,
          familyCalendarId: privateWork.family_calendar_id,
          currentFacts: await currentGoogleFacts(sql, privateWork.household_id, privateWork.owner_adult_id),
          scan: storedInitialPrivateGoogleScan(privateWork.briefing_candidates),
        };
      }

      const [householdWork] = await sql<ProactiveWorkRow[]>`
        select w.* from proactive_work w
        join households h on h.id=w.household_id
        join linq_channels group_channel on group_channel.household_id=w.household_id
          and group_channel.audience='group' and group_channel.adult_two_id is not null
          and group_channel.revoked_at is null and group_channel.stopped_at is null
        where w.kind='initial_household_briefing' and w.status='active' and w.next_check_at<=${now}
          and h.family_calendar_id is not null and h.family_calendar_created_at is not null
          and (select count(distinct private_work.owner_adult_id) from proactive_work private_work
            where private_work.household_id=w.household_id
              and private_work.kind='initial_private_review' and private_work.status='completed'
              and private_work.owner_adult_id in (
                group_channel.adult_one_id,group_channel.adult_two_id
              ))=2
          and not exists (
            select 1 from proactive_work private_work
            where private_work.household_id=w.household_id
              and private_work.kind='initial_private_review' and private_work.status='completed'
              and private_work.owner_adult_id in (
                group_channel.adult_one_id,group_channel.adult_two_id
              )
              and (
                (select count(*) from messages private_message
                  where private_message.idempotency_key like
                    'initial-private-review:' || private_work.id::text || ':%') not between 1 and 3
                or exists (
                  select 1 from messages private_message
                  left join sources outbound_source on outbound_source.id=private_message.source_id
                  where private_message.idempotency_key like
                    'initial-private-review:' || private_work.id::text || ':%'
                    and not (
                      private_message.status='sent'
                      or (
                        private_message.status='failed'
                        and private_message.last_error=${GOOGLE_SOURCE_REMOVED_BEFORE_DELIVERY}
                        and case
                          when jsonb_typeof(outbound_source.metadata->'googleSourceIds')='array'
                          then jsonb_array_length(outbound_source.metadata->'googleSourceIds')>0
                          else false
                        end
                        and not exists (
                          select 1
                          from jsonb_array_elements_text(
                            case
                              when jsonb_typeof(outbound_source.metadata->'googleSourceIds')='array'
                              then outbound_source.metadata->'googleSourceIds'
                              else '[]'::jsonb
                            end
                          ) linked(id)
                          join proactive_work_sources evidence
                            on evidence.work_id=private_work.id
                              and evidence.source_id::text=linked.id
                        )
                      )
                    )
                )
              )
          )
        order by w.created_at,w.id for update of w limit 1
      `;
      if (!householdWork) return null;
      const [group] = await sql<ChannelRow[]>`
        select * from linq_channels where household_id=${householdWork.household_id}
          and audience='group' and adult_two_id is not null and revoked_at is null and stopped_at is null
        order by bound_at,id limit 1 for share
      `;
      if (!group?.adult_two_id) return null;
      const calendar = activeFamilyCalendarCredential(
        await readFamilyCalendarAuthority(sql, householdWork.household_id),
      );
      if (!calendar) return null;
      const completedReviews = await sql<
        (ProactiveWorkRow & { private_conflict_busy_sharing_enabled: boolean })[]
      >`
        select w.*,
          coalesce(p.preferences->'privateConflictBusySharingEnabled'='true'::jsonb,false)
            as private_conflict_busy_sharing_enabled
        from proactive_work w join people p on p.household_id=w.household_id
          and p.id=w.owner_adult_id
        where w.household_id=${householdWork.household_id}
          and w.kind='initial_private_review' and w.status='completed'
          and w.owner_adult_id in (${group.adult_one_id},${group.adult_two_id})
        order by w.owner_adult_id,w.id for share of w,p
      `;
      if (completedReviews.length !== 2) return null;
      for (const review of completedReviews) {
        if (!(await isCompletedPrivateReviewReadyForHousehold(sql, review.id))) return null;
      }
      const householdProfile = await sharedFamilyProfile(sql, householdWork.household_id);
      const candidates = completedReviews.flatMap((review) =>
        storedBriefingCandidates(review.briefing_candidates)
          .filter(
            (candidate) =>
              isCurrentDocketCandidate(candidate, now, householdProfile.timeZone) &&
              (candidate.category !== "conflict" || review.private_conflict_busy_sharing_enabled),
          )
          .map((candidate) => ({ candidate, ownerAdultId: review.owner_adult_id as string })),
      );
      const candidateGroups = rankedBriefingCandidateGroups(candidates);
      return {
        kind: "initial_household_briefing" as const,
        workId: householdWork.id,
        household: householdProfile,
        familyCalendarId: calendar.calendarId,
        familyCalendarOwnerAdultId: calendar.ownerAdultId,
        familyCalendarOwnerConnectionId: calendar.connectionId,
        candidates: candidateGroups.map(
          ({
            candidate: {
              sourceIds: _sourceIds,
              actionAnchorDigest: _actionAnchorDigest,
              actionKey: _actionKey,
              ...candidate
            },
          }) => candidate,
        ),
      };
    });
  }

  async beginInitialPrivateGoogleScan(input: {
    workId: string;
    scan: InitialPrivateGoogleScanV1;
  }): Promise<void> {
    assertUuid(input.workId, "Initial private Google scan work ID");
    const scan = initialPrivateGoogleScan(input.scan);
    await this.#sql.begin(async (sql) => {
      const [work] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where id=${input.workId} and kind='initial_private_review'
          and status='active' for update
      `;
      if (!work?.owner_adult_id) {
        throw new FlorenceStoreConflict("The initial private Google scan is no longer active");
      }
      const current = storedInitialPrivateGoogleScan(work.briefing_candidates);
      if (current) {
        if (initialPrivateGoogleScanDigest(current) !== initialPrivateGoogleScanDigest(scan)) {
          throw new FlorenceStoreConflict("The initial private Google scan was already initialized");
        }
        return;
      }
      await sql`
        update proactive_work set briefing_candidates=${sql.json([scan])},last_error=null
        where id=${work.id}
      `;
    });
  }

  /**
   * Commits one fully classified provider page. Cursor progress and retained outcomes move in the
   * same transaction, so a crash before this commit rereads the same opaque provider page.
   */
  async commitInitialPrivateGoogleScanPage(input: CommitInitialPrivateGoogleScanPageInput): Promise<void> {
    assertUuid(input.workId, "Initial private Google scan work ID");
    assertDigest(input.expectedStateDigest, "Initial private Google scan state");
    const nextScan = initialPrivateGoogleScan(input.nextScan);
    const occurredAt = instant(input.occurredAt);
    const draftSourceIds = unique(input.googleEvidence.map((source) => source.id));
    const removedSourceIds = unique(input.removedSourceIds);
    const pageSourceIds = unique([...draftSourceIds, ...removedSourceIds]);
    if (
      draftSourceIds.length !== input.googleEvidence.length ||
      removedSourceIds.length !== input.removedSourceIds.length ||
      draftSourceIds.some((sourceId) => removedSourceIds.includes(sourceId))
    ) {
      throw new FlorenceStoreConflict("A Google scan page repeated a source identity");
    }
    const classifiedSourceIds = unique(input.classifiedSourceIds);
    const dismissedSourceIds = unique(input.dismissedSourceIds);
    if (
      classifiedSourceIds.length !== input.classifiedSourceIds.length ||
      dismissedSourceIds.length !== input.dismissedSourceIds.length ||
      classifiedSourceIds.some((sourceId) => dismissedSourceIds.includes(sourceId)) ||
      classifiedSourceIds.some((sourceId) => !draftSourceIds.includes(sourceId)) ||
      removedSourceIds.some((sourceId) => !dismissedSourceIds.includes(sourceId)) ||
      !sameStringSet([...classifiedSourceIds, ...dismissedSourceIds], pageSourceIds)
    ) {
      throw new FlorenceStoreConflict(
        "Every Google scan source needs exactly one retained or dismissed disposition",
      );
    }
    await this.#sql.begin(async (sql) => {
      const [work] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where id=${input.workId} and kind='initial_private_review'
          and status='active' for update
      `;
      if (!work?.owner_adult_id) {
        throw new FlorenceStoreConflict("The initial private Google scan is no longer active");
      }
      const current = storedInitialPrivateGoogleScan(work.briefing_candidates);
      if (!current || initialPrivateGoogleScanDigest(current) !== input.expectedStateDigest) {
        throw new FlorenceStoreConflict("The initial private Google scan changed before page commit");
      }
      assertInitialPrivateGoogleScanContinuation(current, nextScan, [
        ...classifiedSourceIds,
        ...dismissedSourceIds,
      ]);
      await persistGoogleEvidenceDrafts(sql, {
        householdId: work.household_id,
        drafts: input.googleEvidence,
        sourceIds: classifiedSourceIds,
      });
      if (classifiedSourceIds.length > 0) {
        await assertProactiveSources(
          sql,
          work.household_id,
          "private",
          work.owner_adult_id,
          classifiedSourceIds,
        );
        await linkProactiveWorkSources(sql, work.id, classifiedSourceIds);
      }
      const retainedSourceIds = unique([
        ...nextScan.outcomes.findings.flatMap((finding) => [...finding.sourceIds]),
        ...nextScan.outcomes.facts.flatMap((fact) => [...fact.sourceIds]),
      ]);
      const unlinked =
        retainedSourceIds.length === 0
          ? await sql<{ source_id: string }[]>`
              delete from proactive_work_sources link using sources source
              where link.work_id=${work.id} and source.id=link.source_id
                and source.metadata->>'connectionId'=${nextScan.connectionId}
              returning link.source_id
            `
          : await sql<{ source_id: string }[]>`
              delete from proactive_work_sources link using sources source
              where link.work_id=${work.id} and source.id=link.source_id
                and source.metadata->>'connectionId'=${nextScan.connectionId}
                and link.source_id not in ${sql(retainedSourceIds)} returning link.source_id
            `;
      if (unlinked.length > 0) {
        const unlinkedSourceIds = unique(unlinked.map(({ source_id }) => source_id));
        await sql`
          delete from sources source where source.id in ${sql(unlinkedSourceIds)}
            and source.household_id=${work.household_id}
            and source.kind in ('gmail','calendar','google_file')
            and not exists (select 1 from proactive_work_sources link where link.source_id=source.id)
            and not exists (select 1 from fact_sources link where link.source_id=source.id)
            and not exists (select 1 from calendar_actions action where action.basis_source_id=source.id)
        `;
      }
      await sql`
        update proactive_work set briefing_candidates=${sql.json([nextScan])},
          next_check_at=${occurredAt},last_error=null where id=${work.id}
      `;
    });
  }

  async restartInitialPrivateGoogleScan(input: {
    workId: string;
    expectedStateDigest: string;
    scan: InitialPrivateGoogleScanV1;
  }): Promise<void> {
    assertUuid(input.workId, "Initial private Google scan work ID");
    assertDigest(input.expectedStateDigest, "Initial private Google scan state");
    const scan = initialPrivateGoogleScan(input.scan);
    await this.#sql.begin(async (sql) => {
      const [work] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where id=${input.workId} and kind='initial_private_review'
          and status='active' for update
      `;
      const current = work ? storedInitialPrivateGoogleScan(work.briefing_candidates) : null;
      if (!work || !current || initialPrivateGoogleScanDigest(current) !== input.expectedStateDigest) {
        throw new FlorenceStoreConflict("The initial private Google scan changed before restart");
      }
      if (
        scan.connectionId !== current.connectionId ||
        scan.scannerVersion !== current.scannerVersion ||
        scan.phase !== "calendar_targets" ||
        scan.outcomes.findings.length > 0 ||
        scan.outcomes.facts.length > 0
      ) {
        throw new FlorenceStoreConflict("The initial private Google scan restart is invalid");
      }
      const linked = await sql<{ source_id: string }[]>`
        delete from proactive_work_sources link using sources source
        where link.work_id=${work.id} and source.id=link.source_id
          and source.metadata->>'connectionId'=${current.connectionId}
        returning link.source_id
      `;
      if (linked.length > 0) {
        const sourceIds = linked.map((row) => row.source_id);
        await sql`
          delete from sources source where source.id in ${sql(sourceIds)}
            and source.household_id=${work.household_id}
            and source.kind in ('gmail','calendar','google_file')
            and not exists (select 1 from proactive_work_sources link where link.source_id=source.id)
            and not exists (select 1 from fact_sources link where link.source_id=source.id)
            and not exists (select 1 from calendar_actions action where action.basis_source_id=source.id)
        `;
      }
      await sql`
        update proactive_work set briefing_candidates=${sql.json([scan])},next_check_at=now(),
          last_error=null where id=${work.id}
      `;
    });
  }

  async completePrivateInitialReview(input: CompletePrivateInitialReviewInput): Promise<void> {
    assertUuid(input.workId, "Initial private review work ID");
    assertDigest(input.generationKey, "Initial private review generation");
    const gmailCursor = required(input.gmailCursor, "Gmail review cursor");
    const calendarCursor = required(input.calendarCursor, "Calendar review cursor");
    const bubbles = initialBriefingBubbles(input.bubbles, "private Google review");
    const occurredAt = instant(input.occurredAt);
    const rescanDeliverNotBefore = proactiveDeliveryTime(input.rescanDeliverNotBefore, occurredAt);
    await this.#sql.begin(async (sql) => {
      const [work] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where id=${input.workId} and kind='initial_private_review'
          and status='active' for update
      `;
      if (!work?.owner_adult_id) {
        throw new FlorenceStoreConflict("The private Google review is no longer active");
      }
      const completedScan = storedInitialPrivateGoogleScan(work.briefing_candidates);
      if (completedScan?.phase !== "ready") {
        throw new FlorenceStoreConflict("The private Google review has not completed full coverage");
      }
      const [channel] = await sql<ChannelRow[]>`
        select * from linq_channels where household_id=${work.household_id} and audience='private'
          and adult_one_id=${work.owner_adult_id} and adult_two_id is null
          and revoked_at is null and stopped_at is null
        order by bound_at,id limit 1 for share
      `;
      const [connection] = await sql<{ id: string }[]>`
        select id from google_connections where household_id=${work.household_id}
          and owner_adult_id=${work.owner_adult_id} and id=${completedScan.connectionId}
          and status='active' for share
      `;
      const [household] = await sql<{ family_calendar_id: string | null }[]>`
        select family_calendar_id from households where id=${work.household_id} for share
      `;
      const [consent] = await sql<{ id: string; conflict_sharing_enabled: boolean }[]>`
        select id,
          coalesce(preferences->'privateConflictBusySharingEnabled'='true'::jsonb,false)
            as conflict_sharing_enabled
        from people where household_id=${work.household_id} and id=${work.owner_adult_id}
          and kind='adult' and status='verified'
          and nullif(preferences->>'proactiveUseAcceptedAt','') is not null
          and coalesce(preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
        for share
      `;
      if (
        !channel ||
        !connection ||
        !consent ||
        !household ||
        household.family_calendar_id !== completedScan.excludedFamilyCalendarId
      ) {
        throw new FlorenceStoreUnauthorized("The private Google review authority is no longer active");
      }
      const facts = googleStableFacts(input.facts);
      assertPrivateInitialReviewAccounting(input.findings, bubbles);
      if (input.findings.some((finding) => finding.monitor && finding.familyCalendar)) {
        throw new FlorenceStoreConflict(
          "One Google finding cannot create both a Calendar action and a reminder monitor",
        );
      }
      const allSourceIds = [
        ...new Set([
          ...input.findings.flatMap((finding) => [...finding.sourceIds]),
          ...facts.flatMap((fact) => [...fact.sourceIds]),
        ]),
      ];
      if (input.googleEvidence.length > 0) {
        await persistGoogleEvidenceDrafts(sql, {
          householdId: work.household_id,
          drafts: input.googleEvidence,
          sourceIds: allSourceIds,
        });
      }
      if (allSourceIds.length > 0) {
        const sources = await sql<{ id: string }[]>`
          select id from sources where household_id=${work.household_id}
            and visibility='private' and owner_adult_id=${work.owner_adult_id}
            and id in ${sql(allSourceIds)} for share
        `;
        if (sources.length !== allSourceIds.length) {
          throw new FlorenceStoreUnauthorized("A private review finding cited unavailable evidence");
        }
      }
      const stableGoogleSourceDigests = await readStableGoogleSourceDigestMap(
        sql,
        work.household_id,
        allSourceIds,
      );
      const resolvedGoogleActionKeys = await readResolvedGoogleActionKeys(sql, work.household_id);
      const findingActionKeys = input.findings.map((finding) =>
        googleActionKeyForFinding(finding, stableGoogleSourceDigests),
      );
      const findingIsResolved = (findingIndex: number): boolean => {
        const actionKey = findingActionKeys[findingIndex];
        return typeof actionKey === "string" && resolvedGoogleActionKeys.has(actionKey);
      };
      const visibleGoogleActions = input.findings.flatMap((finding, findingIndex) => {
        if (finding.surfaceNow === false || findingIsResolved(findingIndex)) return [];
        const key = findingActionKeys[findingIndex];
        return key
          ? [
              {
                key,
                summary: finding.privateSummary,
                sourceIds: finding.sourceIds,
                urgency: finding.householdCandidate?.urgency ?? finding.urgency ?? "soon",
              },
            ]
          : [];
      });
      const visibleGoogleActionKeys = unique(visibleGoogleActions.map(({ key }) => key)).sort();
      if (
        input.findings.some(
          (finding, findingIndex) => finding.surfaceNow !== false && !findingIsResolved(findingIndex),
        ) &&
        visibleGoogleActionKeys.length === 0
      ) {
        throw new FlorenceStoreConflict(
          `A surfaced Google finding needs provider-stable evidence (${stableGoogleSourceDigests.size}/${allSourceIds.length})`,
        );
      }
      const candidates = input.findings.flatMap((finding, findingIndex) => {
        const actionKey = findingActionKeys[findingIndex] ?? null;
        if (
          findingIsResolved(findingIndex) ||
          !finding.householdCandidate ||
          (finding.householdCandidate.category === "conflict" && !consent.conflict_sharing_enabled)
        ) {
          return [];
        }
        return [
          privateReviewCandidate(
            deterministicUuid(`briefing-candidate\0${work.id}\0${input.generationKey}\0${findingIndex}`),
            finding.householdCandidate,
            finding.sourceIds,
            finding.actionAnchorDigest ?? null,
            actionKey,
          ),
        ];
      });
      for (const [findingIndex, finding] of input.findings.entries()) {
        if (!finding.monitor || findingIsResolved(findingIndex)) continue;
        if (finding.sourceIds.length === 0) {
          throw new FlorenceStoreConflict("An initial-review finite monitor requires finding evidence");
        }
        const googleActionKey = findingActionKeys[findingIndex] ?? null;
        if (
          googleActionKey &&
          (await wasGoogleActionTerminallyDelivered(sql, {
            householdId: work.household_id,
            visibility: "private",
            ownerAdultId: work.owner_adult_id,
            actionKey: googleActionKey,
          }))
        ) {
          continue;
        }
        await applyProactiveMonitorChange(sql, {
          householdId: work.household_id,
          visibility: "private",
          ownerAdultId: work.owner_adult_id,
          sourceIds: finding.sourceIds,
          change: {
            operation: "create",
            monitorId: null,
            objective: finding.monitor.objective,
            currentConclusion: finding.monitor.currentConclusion,
            endCondition: finding.monitor.endCondition,
            nextCheck: finding.monitor.nextCheck,
            why: finding.monitor.why,
          },
          basisWorkId: `${work.id}:${input.generationKey}:${findingIndex}`,
          occurredAt,
          googleActionKey,
        });
      }
      for (const [findingIndex, finding] of input.findings.entries()) {
        if (!finding.familyCalendar || findingIsResolved(findingIndex)) continue;
        const googleActionKey = findingActionKeys[findingIndex];
        if (!googleActionKey) {
          throw new FlorenceStoreConflict("A Calendar proposal needs a stable Google action identity");
        }
        await stageFamilyCalendarReviewProposal(sql, {
          householdId: work.household_id,
          ownerAdultId: work.owner_adult_id,
          proposal: finding.familyCalendar,
          googleActionKey,
          occurredAt,
        });
      }
      const desiredCalendarProposals = new Set(
        input.findings.flatMap((finding, findingIndex) => {
          if (findingIsResolved(findingIndex)) return [];
          const proposal = finding.familyCalendar;
          if (!proposal) return [];
          return proposal.sourceIds.flatMap((sourceId) => {
            const identity = stableGoogleSourceDigests.get(sourceId);
            return identity ? [calendarProposalBasisSignature(identity.providerDigest, proposal.event)] : [];
          });
        }),
      );
      await reconcileAuthoritativeGoogleCalendarProposals(sql, {
        householdId: work.household_id,
        ownerAdultId: work.owner_adult_id,
        connectionId: connection.id,
        gmailAfter: completedScan.gmailAfter,
        reviewedThrough: occurredAt,
        calendarTimeMin: completedScan.calendarTimeMin,
        calendarTimeMax: completedScan.calendarTimeMax,
        desiredSignatures: desiredCalendarProposals,
      });
      const desiredMonitorActions = new Map<string, readonly string[]>();
      for (const [findingIndex, finding] of input.findings.entries()) {
        if (!finding.monitor || findingIsResolved(findingIndex)) continue;
        const actionKey = findingActionKeys[findingIndex] ?? null;
        if (actionKey) desiredMonitorActions.set(actionKey, finding.sourceIds);
      }
      await reconcileAuthoritativeGoogleMonitors(sql, {
        householdId: work.household_id,
        ownerAdultId: work.owner_adult_id,
        connectionId: connection.id,
        gmailAfter: completedScan.gmailAfter,
        reviewedThrough: occurredAt,
        calendarTimeMin: completedScan.calendarTimeMin,
        calendarTimeMax: completedScan.calendarTimeMax,
        desiredActions: desiredMonitorActions,
      });
      await reconcileAuthoritativeGoogleFactSupports(sql, {
        householdId: work.household_id,
        ownerAdultId: work.owner_adult_id,
        connectionId: connection.id,
        gmailAfter: completedScan.gmailAfter,
        reviewedThrough: occurredAt,
        calendarTimeMin: completedScan.calendarTimeMin,
        calendarTimeMax: completedScan.calendarTimeMax,
        facts,
      });
      await upsertGoogleStableFacts(sql, {
        householdId: work.household_id,
        ownerAdultId: work.owner_adult_id,
        connectionId: connection.id,
        currentEvidenceSourceIds:
          input.googleEvidence.length > 0 ? input.googleEvidence.map((source) => source.id) : allSourceIds,
        facts,
        occurredAt,
      });
      const turnId = deterministicUuid(`initial-private-review-turn\0${work.id}\0${input.generationKey}`);
      const existing = await sql<{ source_id: string }[]>`
        select source_id from messages where turn_id=${turnId} order by turn_part for update
      `;
      const priorInitialMessages = await sql<{ text: string | null }[]>`
        select text from messages where idempotency_key like ${`initial-private-review:${work.id}:%`}
          and direction='outbound' and status='sent' order by not_before,turn_part
      `;
      const isRescan = priorInitialMessages.length > 0;
      if (existing.length === 0 && !isRescan) {
        for (const [index, bubble] of bubbles.entries()) {
          const bubbleItems = new Set(initialBriefingBulletItems([bubble]));
          const bubbleGoogleActionKeys = unique(
            visibleGoogleActions.filter(({ summary }) => bubbleItems.has(summary)).map(({ key }) => key),
          ).sort();
          const bubbleGoogleSourceIds = unique(
            visibleGoogleActions
              .filter(({ summary }) => bubbleItems.has(summary))
              .flatMap(({ sourceIds }) => [...sourceIds]),
          ).sort();
          const bubbleGoogleActionUrgencies = googleActionUrgencies(
            visibleGoogleActions.filter(({ summary }) => bubbleItems.has(summary)),
          );
          await insertOutbound(sql, {
            sourceId: deterministicUuid(
              `initial-private-review-message\0${work.id}\0${input.generationKey}\0${index}`,
            ),
            idempotencyKey: `initial-private-review:${work.id}:${input.generationKey}:${index}`,
            moveKind: "message",
            text: bubble.text,
            turnId,
            turnPart: index as 0 | 1 | 2,
            notBefore: new Date(occurredAt.getTime() + bubble.delayMs).toISOString(),
            householdId: work.household_id,
            channelId: channel.id,
            visibility: "private",
            ownerAdultId: work.owner_adult_id,
            ...(bubbleGoogleActionKeys.length > 0 || bubbleGoogleSourceIds.length > 0
              ? {
                  metadata: {
                    ...(bubbleGoogleActionKeys.length > 0
                      ? { googleActionKeys: bubbleGoogleActionKeys }
                      : {}),
                    ...(Object.keys(bubbleGoogleActionUrgencies).length > 0
                      ? { googleActionUrgencies: bubbleGoogleActionUrgencies }
                      : {}),
                    ...(bubbleGoogleSourceIds.length > 0 ? { googleSourceIds: bubbleGoogleSourceIds } : {}),
                  },
                }
              : {}),
            occurredAt,
          });
        }
      } else if (existing.length > 0 && (existing.length < 1 || existing.length > 3)) {
        throw new FlorenceStoreConflict("The private Google review output is incomplete");
      }
      for (const [index, bubble] of bubbles.entries()) {
        const bubbleItems = new Set(initialBriefingBulletItems([bubble]));
        const bubbleGoogleActionKeys = unique(
          visibleGoogleActions.filter(({ summary }) => bubbleItems.has(summary)).map(({ key }) => key),
        ).sort();
        const bubbleGoogleSourceIds = unique(
          visibleGoogleActions
            .filter(({ summary }) => bubbleItems.has(summary))
            .flatMap(({ sourceIds }) => [...sourceIds]),
        ).sort();
        const bubbleGoogleActionUrgencies = googleActionUrgencies(
          visibleGoogleActions.filter(({ summary }) => bubbleItems.has(summary)),
        );
        await sql`
          update sources source set metadata=
            (source.metadata-'googleActionKeys'-'googleActionUrgencies'-'googleSourceIds')||${sql.json({
              ...(bubbleGoogleActionKeys.length > 0 ? { googleActionKeys: bubbleGoogleActionKeys } : {}),
              ...(Object.keys(bubbleGoogleActionUrgencies).length > 0
                ? { googleActionUrgencies: bubbleGoogleActionUrgencies }
                : {}),
              ...(bubbleGoogleSourceIds.length > 0 ? { googleSourceIds: bubbleGoogleSourceIds } : {}),
            })}
          from messages message where message.source_id=source.id and message.turn_id=${turnId}
            and message.turn_part=${index}
        `;
      }
      if (isRescan) {
        const deliveries: ProactiveDelivery[] = input.findings.flatMap((finding, findingIndex) => {
          if (finding.surfaceNow === false || findingIsResolved(findingIndex)) return [];
          const candidate = finding.householdCandidate;
          return [
            {
              privateDetail: finding.privateSummary,
              ...(finding.actionAnchorDigest ? { actionAnchorDigest: finding.actionAnchorDigest } : {}),
              householdConclusion: candidate?.summary ?? null,
              householdCategory: candidate?.category ?? null,
              householdNeedsAnswer: candidate?.needsAnswer ?? false,
              sourceIds: finding.sourceIds,
              urgency: candidate?.urgency ?? finding.urgency ?? "soon",
              dueAt: finding.dueAt ?? candidate?.dueAt ?? null,
              monitor: null,
              familyCalendar: null,
            },
          ];
        });
        await applyGooglePollDeliveries(sql, {
          work,
          deliveries,
          deliverNotBefore: rescanDeliverNotBefore,
          occurredAt,
        });
      }
      for (const sourceId of allSourceIds) {
        await sql`
          insert into proactive_work_sources (work_id,source_id) values (${work.id},${sourceId})
          on conflict do nothing
        `;
      }
      await sql`
        update proactive_work set status='completed',next_check_at=null,
          briefing_candidates=${sql.json(candidates)},last_error=null
        where id=${work.id}
      `;
      await sql`
        insert into proactive_work (
          id,household_id,kind,visibility,owner_adult_id,gmail_cursor,calendar_cursor,
          status,next_check_at,created_at
        ) values (${deterministicUuid(`personal-google-poll\0${work.owner_adult_id}`)},
          ${work.household_id},'personal_google_poll','private',${work.owner_adult_id},
          ${gmailCursor},${calendarCursor},'active',
          ${new Date(occurredAt.getTime() + GOOGLE_POLL_INTERVAL_MS)},${occurredAt})
        on conflict (id) do update set gmail_cursor=excluded.gmail_cursor,
          calendar_cursor=excluded.calendar_cursor,status='active',
          next_check_at=excluded.next_check_at,last_error=null
      `;
    });
  }

  async completeHouseholdInitialBriefing(input: CompleteHouseholdInitialBriefingInput): Promise<void> {
    assertUuid(input.workId, "Initial household briefing work ID");
    if (
      input.selectedCandidateIds.length > 3 ||
      new Set(input.selectedCandidateIds).size !== input.selectedCandidateIds.length
    ) {
      throw new FlorenceStoreConflict("A household briefing may select at most three current items");
    }
    for (const candidateId of input.selectedCandidateIds) assertUuid(candidateId, "Briefing candidate ID");
    const familyCalendarCursor = required(input.familyCalendarCursor, "Family Calendar review cursor");
    const bubbles = initialBriefingBubbles(input.bubbles, "household briefing");
    const occurredAt = instant(input.occurredAt);
    await this.#sql.begin(async (sql) => {
      const [work] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where id=${input.workId} and kind='initial_household_briefing'
          and status='active' for update
      `;
      if (!work) throw new FlorenceStoreConflict("The household briefing is no longer active");
      const [channel] = await sql<(ChannelRow & { household_time_zone: string })[]>`
        select c.*,h.time_zone as household_time_zone
        from linq_channels c join households h on h.id=c.household_id
        where c.household_id=${work.household_id} and c.audience='group'
          and c.adult_two_id is not null and c.revoked_at is null and c.stopped_at is null
          and h.family_calendar_id is not null and h.family_calendar_created_at is not null
        order by c.bound_at,c.id limit 1 for share of c,h
      `;
      if (!channel?.adult_two_id) {
        throw new FlorenceStoreUnauthorized("The family group is no longer active");
      }
      const consentingAdults = await sql<{ id: string }[]>`
        select id from people where household_id=${work.household_id}
          and id in (${channel.adult_one_id},${channel.adult_two_id})
          and kind='adult' and status='verified'
          and nullif(preferences->>'proactiveUseAcceptedAt','') is not null
          and coalesce(preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
        for share
      `;
      if (consentingAdults.length !== 2) {
        throw new FlorenceStoreUnauthorized("Both parents must still allow proactive Google use");
      }
      const reviews = await sql<(ProactiveWorkRow & { private_conflict_busy_sharing_enabled: boolean })[]>`
        select w.*,
          coalesce(p.preferences->'privateConflictBusySharingEnabled'='true'::jsonb,false)
            as private_conflict_busy_sharing_enabled
        from proactive_work w join people p on p.household_id=w.household_id
          and p.id=w.owner_adult_id
        where w.household_id=${work.household_id}
          and w.kind='initial_private_review' and w.status='completed'
          and w.owner_adult_id in (${channel.adult_one_id},${channel.adult_two_id})
        order by w.owner_adult_id,w.id for share of w,p
      `;
      if (reviews.length !== 2) {
        throw new FlorenceStoreConflict("Both private Google reviews must finish first");
      }
      for (const review of reviews) {
        if (!(await isCompletedPrivateReviewReadyForHousehold(sql, review.id))) {
          throw new FlorenceStoreConflict("Both private Google briefings must be delivered first");
        }
      }
      const candidates = reviews.flatMap((review) => {
        if (!review.owner_adult_id) {
          throw new FlorenceStoreConflict("An initial private review has no owning parent");
        }
        return storedBriefingCandidates(review.briefing_candidates)
          .filter(
            (candidate) =>
              isCurrentDocketCandidate(candidate, occurredAt, channel.household_time_zone) &&
              (candidate.category !== "conflict" || review.private_conflict_busy_sharing_enabled),
          )
          .map((candidate) => ({ candidate, ownerAdultId: review.owner_adult_id as string }));
      });
      const candidateGroups = rankedBriefingCandidateGroups(candidates);
      const selectedCandidateGroups = candidateGroups.slice(0, 3);
      const representativeCandidateIds = selectedCandidateGroups.map(
        ({ candidate }) => candidate.candidateId,
      );
      if (!sameStringSet(input.selectedCandidateIds, representativeCandidateIds)) {
        throw new FlorenceStoreConflict("The household briefing did not select the ranked current docket");
      }
      assertHouseholdInitialBriefingAccounting(
        selectedCandidateGroups.map(({ candidate }) => candidate),
        bubbles,
      );
      let nextCandidateGroupIndex = 0;
      const bubbleCandidateGroupRanges = bubbles.map((bubble) => {
        const startIndex = nextCandidateGroupIndex;
        nextCandidateGroupIndex += initialBriefingBulletItems([bubble]).length;
        return {
          startIndex,
          endIndex: nextCandidateGroupIndex,
          groups: selectedCandidateGroups.slice(startIndex, nextCandidateGroupIndex),
        };
      });
      const candidateSourceIds = unique(
        selectedCandidateGroups.flatMap(({ members }) =>
          members.flatMap((candidate) => [...candidate.sourceIds]),
        ),
      );
      const stableGoogleSourceDigests = await readStableGoogleSourceDigestMap(
        sql,
        work.household_id,
        candidateSourceIds,
      );
      const visibleGoogleActions = selectedCandidateGroups.flatMap(({ members }, candidateGroupIndex) =>
        members.flatMap((member) => {
          const key = googleActionKeyForCandidate(member, stableGoogleSourceDigests);
          return key
            ? [{ key, candidateGroupIndex, sourceIds: member.sourceIds, urgency: member.urgency }]
            : [];
        }),
      );
      const visibleGoogleActionKeys = unique(visibleGoogleActions.map(({ key }) => key)).sort();
      if (selectedCandidateGroups.length > 0 && visibleGoogleActionKeys.length === 0) {
        throw new FlorenceStoreConflict("A household Google briefing needs provider-stable evidence");
      }
      const briefingBubbleMetadata = bubbleCandidateGroupRanges.map(({ startIndex, endIndex, groups }) => {
        const googleActions = visibleGoogleActions.filter(
          ({ candidateGroupIndex }) => candidateGroupIndex >= startIndex && candidateGroupIndex < endIndex,
        );
        return {
          conflictOwnerAdultIds: unique(
            groups.flatMap(({ candidate, ownerAdultIds }) =>
              candidate.category === "conflict" ? ownerAdultIds : [],
            ),
          ),
          googleActionKeys: unique(googleActions.map(({ key }) => key)).sort(),
          googleActionUrgencies: googleActionUrgencies(googleActions),
          googleSourceIds: unique(googleActions.flatMap(({ sourceIds }) => [...sourceIds])).sort(),
        };
      });
      const turnId = deterministicUuid(`initial-household-briefing-turn\0${work.id}`);
      const existing = await sql<{ source_id: string }[]>`
        select source_id from messages where turn_id=${turnId} order by turn_part for update
      `;
      if (existing.length === 0) {
        for (const [index, bubble] of bubbles.entries()) {
          const bubbleMetadata = briefingBubbleMetadata[index];
          if (!bubbleMetadata) {
            throw new FlorenceStoreConflict("The household briefing metadata partition is incomplete");
          }
          const {
            conflictOwnerAdultIds: bubbleConflictOwnerAdultIds,
            googleActionKeys: bubbleGoogleActionKeys,
            googleActionUrgencies: bubbleGoogleActionUrgencies,
            googleSourceIds: bubbleGoogleSourceIds,
          } = bubbleMetadata;
          await insertOutbound(sql, {
            sourceId: deterministicUuid(`initial-household-briefing-message\0${work.id}\0${index}`),
            idempotencyKey: `initial-household-briefing:${work.id}:${index}`,
            moveKind: "message",
            text: bubble.text,
            turnId,
            turnPart: index as 0 | 1 | 2,
            notBefore: new Date(occurredAt.getTime() + bubble.delayMs).toISOString(),
            householdId: work.household_id,
            channelId: channel.id,
            visibility: "household",
            ownerAdultId: null,
            ...(bubbleConflictOwnerAdultIds.length > 0 ||
            bubbleGoogleActionKeys.length > 0 ||
            bubbleGoogleSourceIds.length > 0
              ? {
                  metadata: {
                    ...(bubbleConflictOwnerAdultIds.length > 0
                      ? { privateConflictOwnerAdultIds: bubbleConflictOwnerAdultIds }
                      : {}),
                    ...(bubbleGoogleActionKeys.length > 0
                      ? { googleActionKeys: bubbleGoogleActionKeys }
                      : {}),
                    ...(Object.keys(bubbleGoogleActionUrgencies).length > 0
                      ? { googleActionUrgencies: bubbleGoogleActionUrgencies }
                      : {}),
                    ...(bubbleGoogleSourceIds.length > 0 ? { googleSourceIds: bubbleGoogleSourceIds } : {}),
                  },
                }
              : {}),
            occurredAt,
          });
        }
      } else if (existing.length < 1 || existing.length > 3) {
        throw new FlorenceStoreConflict("The household briefing output is incomplete");
      }
      for (const [index] of bubbles.entries()) {
        const bubbleMetadata = briefingBubbleMetadata[index];
        if (!bubbleMetadata) {
          throw new FlorenceStoreConflict("The household briefing metadata partition is incomplete");
        }
        const {
          conflictOwnerAdultIds: bubbleConflictOwnerAdultIds,
          googleActionKeys: bubbleGoogleActionKeys,
          googleActionUrgencies: bubbleGoogleActionUrgencies,
          googleSourceIds: bubbleGoogleSourceIds,
        } = bubbleMetadata;
        await sql`
          update sources source set metadata=(
            source.metadata-'googleActionKeys'-'googleActionUrgencies'-'privateConflictOwnerAdultIds'-'googleSourceIds'
          )||${sql.json({
            ...(bubbleGoogleActionKeys.length > 0 ? { googleActionKeys: bubbleGoogleActionKeys } : {}),
            ...(Object.keys(bubbleGoogleActionUrgencies).length > 0
              ? { googleActionUrgencies: bubbleGoogleActionUrgencies }
              : {}),
            ...(bubbleConflictOwnerAdultIds.length > 0
              ? { privateConflictOwnerAdultIds: bubbleConflictOwnerAdultIds }
              : {}),
            ...(bubbleGoogleSourceIds.length > 0 ? { googleSourceIds: bubbleGoogleSourceIds } : {}),
          })}
          from messages message where message.source_id=source.id and message.turn_id=${turnId}
            and message.turn_part=${index}
        `;
      }
      await sql`
        update proactive_work set status='completed',next_check_at=null,last_error=null
        where id=${work.id}
      `;
      await sql`
        insert into proactive_work (
          id,household_id,kind,visibility,calendar_cursor,status,next_check_at,created_at
        ) values (${deterministicUuid(`family-calendar-poll\0${work.household_id}`)},
          ${work.household_id},'family_calendar_poll','household',${familyCalendarCursor},'active',
          ${new Date(occurredAt.getTime() + GOOGLE_POLL_INTERVAL_MS)},${occurredAt})
        on conflict do nothing
      `;
    });
  }

  async retryInitialIntelligence(input: { workId: string; retryAt: string; error: string }): Promise<void> {
    assertUuid(input.workId, "Initial intelligence work ID");
    const retryAt = instant(input.retryAt);
    const updated = await this.#sql`
      update proactive_work set next_check_at=${retryAt},last_error=${bounded(input.error, 2_000)}
      where id=${input.workId}
        and kind in ('initial_private_review','initial_household_briefing')
        and status='active'
      returning id
    `;
    if (updated.length !== 1) {
      throw new FlorenceStoreConflict("The initial intelligence work is no longer retryable");
    }
  }

  async readNextDueProactiveWork(nowInput: string): Promise<DueProactiveWork | null> {
    const now = instant(nowInput);
    return this.#sql.begin(async (sql) => {
      await sql`
        update proactive_work w set status='paused',next_check_at=null,
          last_error=case
            when w.kind='finite_monitor'
              and left(coalesce(w.last_error,''),${GOOGLE_ACTION_WORK_MARKER_PREFIX.length})=${GOOGLE_ACTION_WORK_MARKER_PREFIX}
            then left(w.last_error,${GOOGLE_ACTION_WORK_MARKER_LENGTH})||E'\n'||${PROACTIVE_CONSENT_PAUSE_REASON}
            else ${PROACTIVE_CONSENT_PAUSE_REASON}
          end
        where w.visibility='private' and w.status='active'
          and w.kind in ('personal_google_poll','finite_monitor')
          and not exists (
            select 1 from people p where p.household_id=w.household_id and p.id=w.owner_adult_id
              and p.kind='adult' and p.status='verified'
              and nullif(p.preferences->>'proactiveUseAcceptedAt','') is not null
              and coalesce(p.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
          )
      `;
      await sql`
        update proactive_work w set status='paused',next_check_at=null,
          last_error='Paused until Google is reconnected with complete Calendar access'
        where w.kind='personal_google_poll' and w.status='active'
          and not exists (
            select 1 from google_connections g where g.household_id=w.household_id
              and g.owner_adult_id=w.owner_adult_id and g.status='active'
              and g.granted_scopes @> ${sql.array([COMPLETE_CALENDAR_HISTORY_SCOPE])}
          )
      `;
      await sql`
        update proactive_work w set status='paused',next_check_at=null,
          last_error=case
            when w.kind='finite_monitor'
              and left(coalesce(w.last_error,''),${GOOGLE_ACTION_WORK_MARKER_PREFIX.length})=${GOOGLE_ACTION_WORK_MARKER_PREFIX}
            then left(w.last_error,${GOOGLE_ACTION_WORK_MARKER_LENGTH})||E'\n'||${PROACTIVE_CONSENT_PAUSE_REASON}
            else ${PROACTIVE_CONSENT_PAUSE_REASON}
          end
        where w.visibility='household' and w.status='active'
          and w.kind in ('family_calendar_poll','finite_monitor','interest_monitor')
          and not exists (
            select 1 from households h
            join google_connections g on g.household_id=h.id and g.status='active'
              and g.id in (h.family_calendar_owner_connection_id,h.family_calendar_partner_connection_id)
            join people p on p.household_id=h.id and p.id=g.owner_adult_id
              and p.kind='adult' and p.status='verified'
            where h.id=w.household_id and h.family_calendar_id is not null
              and h.family_calendar_created_at is not null
              and nullif(p.preferences->>'proactiveUseAcceptedAt','') is not null
              and coalesce(p.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
          )
      `;

      const due = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where status='active' and next_check_at<=${now}
          and kind in (
            'reminder','family_task','personal_google_poll','family_calendar_poll',
            'finite_monitor','interest_monitor'
          )
        order by next_check_at,id
      `;
      for (const work of due) {
        if (work.kind === "family_task") {
          const [familyTask] = await sql<ProactiveWorkRow[]>`
            select * from proactive_work
            where id=${work.id} and kind='family_task' and status='active'
              and next_check_at<=${now}
            for update skip locked
          `;
          if (!familyTask) continue;
          const state = familyWorkState(familyTask.task_state);
          if (state.phase === "waiting" || state.phase === "terminal") {
            throw new FlorenceStoreConflict("Due family work has an invalid durable phase");
          }
          if (state.claim && instant(state.claim.leaseUntil) > now) {
            await sql`
              update proactive_work set next_check_at=${instant(state.claim.leaseUntil)}
              where id=${familyTask.id}
            `;
            continue;
          }
          const claimId = randomUUID();
          const leaseUntil = new Date(now.getTime() + FAMILY_WORK_CLAIM_LEASE_MS);
          const claimedState = familyWorkState({
            ...state,
            claim: { claimId, leaseUntil: leaseUntil.toISOString() },
          });
          await sql`
            update proactive_work set task_state=${sql.json(claimedState)},next_check_at=${leaseUntil},
              last_error=null where id=${familyTask.id}
          `;
          return {
            kind: "family_task",
            workId: familyTask.id,
            household: await sharedFamilyProfile(sql, familyTask.household_id),
            visibility: familyTask.visibility,
            ownerAdultId: familyTask.owner_adult_id,
            objective: required(familyTask.objective ?? "", "Family work objective"),
            state: claimedState,
            claimId,
            generation: claimedState.generation,
          };
        }
        if (work.kind === "reminder") return { kind: "reminder", workId: work.id };
        if (work.kind === "personal_google_poll") {
          const [context] = await sql<
            {
              adult_id: string;
              adult_first_name: string | null;
              adult_display_name: string;
              connection_id: string;
              family_calendar_id: string | null;
            }[]
          >`
            select p.id as adult_id,p.profile->>'firstName' as adult_first_name,
              p.display_name as adult_display_name,g.id as connection_id,
              h.family_calendar_id
            from people p join households h on h.id=p.household_id
            join google_connections g on g.household_id=p.household_id
              and g.owner_adult_id=p.id and g.status='active'
            join linq_channels c on c.household_id=p.household_id and c.audience='private'
              and c.adult_one_id=p.id and c.adult_two_id is null
              and c.revoked_at is null and c.stopped_at is null
            where p.household_id=${work.household_id} and p.id=${work.owner_adult_id}
              and p.kind='adult' and p.status='verified'
              and nullif(p.preferences->>'proactiveUseAcceptedAt','') is not null
              and coalesce(p.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
              and g.granted_scopes @> ${sql.array([COMPLETE_CALENDAR_HISTORY_SCOPE])}
            order by g.created_at desc,g.id desc,c.bound_at,c.id limit 1
          `;
          if (context && work.gmail_cursor && work.calendar_cursor) {
            return {
              kind: "personal_google_poll",
              workId: work.id,
              household: await sharedFamilyProfile(sql, work.household_id),
              visibility: "private",
              adultId: context.adult_id,
              adultFirstName: context.adult_first_name ?? context.adult_display_name,
              connectionId: context.connection_id,
              gmailCursor: work.gmail_cursor,
              calendarId: "primary",
              excludedFamilyCalendarId: context.family_calendar_id,
              calendarCursor: work.calendar_cursor,
              activeMonitors: await activeFiniteMonitors(sql, work.household_id, "private", context.adult_id),
              currentFacts: await currentGoogleFacts(sql, work.household_id, context.adult_id),
            };
          }
          continue;
        }

        if (work.kind === "family_calendar_poll") {
          const [context] = await sql<
            {
              adult_id: string;
              adult_first_name: string | null;
              adult_display_name: string;
              connection_id: string;
              calendar_id: string;
            }[]
          >`
            select p.id as adult_id,p.profile->>'firstName' as adult_first_name,
              p.display_name as adult_display_name,g.id as connection_id,
              h.family_calendar_id as calendar_id
            from households h join google_connections g on g.household_id=h.id and g.status='active'
              and g.id in (h.family_calendar_owner_connection_id,h.family_calendar_partner_connection_id)
            join people p on p.household_id=h.id and p.id=g.owner_adult_id
              and p.kind='adult' and p.status='verified'
            join linq_channels c on c.household_id=h.id and c.audience='group'
              and c.adult_two_id is not null and c.revoked_at is null and c.stopped_at is null
            where h.id=${work.household_id} and h.family_calendar_id is not null
              and h.family_calendar_created_at is not null
              and nullif(p.preferences->>'proactiveUseAcceptedAt','') is not null
              and coalesce(p.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
            order by p.adult_slot,g.created_at,g.id,c.bound_at,c.id limit 1
          `;
          if (context && work.calendar_cursor) {
            return {
              kind: "family_calendar_poll",
              workId: work.id,
              household: await sharedFamilyProfile(sql, work.household_id),
              visibility: "household",
              adultId: context.adult_id,
              adultFirstName: context.adult_first_name ?? context.adult_display_name,
              connectionId: context.connection_id,
              calendarId: context.calendar_id,
              calendarCursor: work.calendar_cursor,
              activeMonitors: await activeFiniteMonitors(sql, work.household_id, "household", null),
            };
          }
          continue;
        }

        if (work.kind === "finite_monitor") {
          const [context] = await sql<
            {
              adult_id: string;
              adult_first_name: string | null;
              adult_display_name: string;
              connection_id: string;
              calendar_id: string;
            }[]
          >`
            select p.id as adult_id,p.profile->>'firstName' as adult_first_name,
              p.display_name as adult_display_name,g.id as connection_id,
              case when ${work.visibility}='private' then 'primary' else h.family_calendar_id end
                as calendar_id
            from households h join people p on p.household_id=h.id
              and p.kind='adult' and p.status='verified'
              and ((${work.visibility}='private' and p.id=${work.owner_adult_id})
                or ${work.visibility}='household')
            join google_connections g on g.household_id=h.id and g.owner_adult_id=p.id
              and g.status='active' and (${work.visibility}='private'
                or g.id in (h.family_calendar_owner_connection_id,h.family_calendar_partner_connection_id))
            join linq_channels c on c.household_id=h.id and c.revoked_at is null and c.stopped_at is null
              and ((${work.visibility}='private' and c.audience='private' and c.adult_one_id=p.id
                  and c.adult_two_id is null)
                or (${work.visibility}='household' and c.audience='group' and c.adult_two_id is not null))
            where h.id=${work.household_id}
              and (${work.visibility}='private' or (h.family_calendar_id is not null
                and h.family_calendar_created_at is not null))
              and nullif(p.preferences->>'proactiveUseAcceptedAt','') is not null
              and coalesce(p.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
            order by p.adult_slot,g.created_at,g.id,c.bound_at,c.id limit 1
          `;
          if (context?.calendar_id) {
            return {
              kind: "finite_monitor",
              workId: work.id,
              household: await sharedFamilyProfile(sql, work.household_id),
              visibility: work.visibility,
              adultId: context.adult_id,
              adultFirstName: context.adult_first_name ?? context.adult_display_name,
              connectionId: context.connection_id,
              calendarId: context.calendar_id,
              monitor: activeFiniteMonitor(work),
            };
          }
          continue;
        }

        const [context] = await sql<
          {
            owner_adult_id: string;
            connection_id: string;
            calendar_id: string;
            postal_code: string;
          }[]
        >`
          select adult.id as owner_adult_id,g.id as connection_id,
            h.family_calendar_id as calendar_id,
            founder.profile->>'postalCode' as postal_code
          from households h join people founder on founder.household_id=h.id
            and founder.kind='adult' and founder.role='steward' and founder.adult_slot=1
            and founder.status='verified' and nullif(founder.profile->>'postalCode','') is not null
          join google_connections g on g.household_id=h.id and g.status='active'
            and g.id in (h.family_calendar_owner_connection_id,h.family_calendar_partner_connection_id)
          join people adult on adult.household_id=h.id and adult.id=g.owner_adult_id
            and adult.kind='adult' and adult.status='verified'
          join linq_channels c on c.household_id=h.id and c.audience='group'
            and c.adult_two_id is not null and c.revoked_at is null and c.stopped_at is null
          where h.id=${work.household_id} and h.family_calendar_id is not null
            and h.family_calendar_created_at is not null
            and nullif(adult.preferences->>'proactiveUseAcceptedAt','') is not null
            and coalesce(adult.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
          order by adult.adult_slot,g.created_at,g.id,c.bound_at,c.id limit 1
        `;
        if (context && work.discovery_terms.length > 0) {
          return {
            kind: "interest_monitor",
            workId: work.id,
            household: await sharedFamilyProfile(sql, work.household_id),
            connectionId: context.connection_id,
            ownerAdultId: context.owner_adult_id,
            calendarId: context.calendar_id,
            genericInterestTerms: [...work.discovery_terms],
            coarseLocation: context.postal_code,
          };
        }
      }
      return null;
    });
  }

  /**
   * One durable effect-sandwich boundary: a claimed step is checkpointed with
   * its outbound message in the same transaction. Generation plus claim ID is
   * the CAS token, so steering, cancellation, and lease takeover make late
   * workers harmless (Pi 4e494929; Hermes 6dcebea7). This does not imply that
   * the in-process worker itself is restart resumable.
   */
  async settleFamilyWorkClaim(input: SettleFamilyWorkClaimInput): Promise<"settled" | "stale"> {
    assertUuid(input.workId, "Family work ID");
    assertUuid(input.claimId, "Family work claim ID");
    familyWorkCounter(input.generation, "Family work generation");
    const settledAt = instant(input.settledAt);
    return this.#sql.begin(async (sql) => {
      const [work] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where id=${input.workId} and kind='family_task' for update
      `;
      if (work?.status !== "active") return "stale";
      const currentState = familyWorkState(work.task_state);
      if (
        currentState.generation !== input.generation ||
        currentState.claim?.claimId !== input.claimId ||
        settledAt > instant(currentState.claim.leaseUntil)
      ) {
        return "stale";
      }
      const nextState = familyWorkState(input.result.state);
      if (
        nextState.generation !== input.generation ||
        nextState.claim !== null ||
        !sameFamilyWorkSteering(currentState.steering, nextState.steering)
      ) {
        throw new FlorenceStoreConflict("A family work settlement changed its claim generation or steering");
      }
      if (input.result.type === "continue") {
        if (nextState.phase === "waiting" || nextState.phase === "terminal") {
          throw new FlorenceStoreConflict("Continuing family work needs a runnable durable phase");
        }
        const nextCheckAt = instant(input.result.nextCheckAt);
        if (nextCheckAt < settledAt) {
          throw new FlorenceStoreConflict("A family work continuation cannot be due in the past");
        }
        const progressText =
          input.result.progressText == null
            ? null
            : bounded(required(input.result.progressText, "Family work progress"), 10_000);
        if (progressText && nextState.progressRevision <= currentState.progressRevision) {
          throw new FlorenceStoreConflict("Family work progress must advance its revision");
        }
        const updated = await sql`
          update proactive_work set task_state=${sql.json(nextState)},status='active',
            next_check_at=${nextCheckAt},current_conclusion=${progressText ?? work.current_conclusion},
            last_error=null where id=${work.id} and kind='family_task' and status='active'
            and task_state->>'generation'=${String(input.generation)}
            and task_state->'claim'->>'claimId'=${input.claimId} returning id
        `;
        if (updated.length !== 1) return "stale";
        if (progressText) {
          await insertFamilyWorkOutbound(sql, work, nextState, "progress", progressText, settledAt);
        }
        return "settled";
      }

      if (input.result.type === "waiting") {
        if (nextState.phase !== "waiting" || nextState.pendingCall !== null || nextState.terminal !== null) {
          throw new FlorenceStoreConflict("Waiting family work needs a waiting checkpoint");
        }
        if (nextState.progressRevision <= currentState.progressRevision) {
          throw new FlorenceStoreConflict("A family work question must advance its progress revision");
        }
        const question = bounded(required(input.result.question, "Family work question"), 10_000);
        const updated = await sql`
          update proactive_work set task_state=${sql.json(nextState)},status='paused',next_check_at=null,
            current_conclusion=${question},last_error=null
          where id=${work.id} and kind='family_task' and status='active'
            and task_state->>'generation'=${String(input.generation)}
            and task_state->'claim'->>'claimId'=${input.claimId} returning id
        `;
        if (updated.length !== 1) return "stale";
        await insertFamilyWorkOutbound(sql, work, nextState, "waiting", question, settledAt);
        return "settled";
      }

      if (input.result.type === "terminal") {
        const terminalText = bounded(required(input.result.terminalText, "Family work result"), 10_000);
        if (
          nextState.phase !== "terminal" ||
          nextState.pendingCall !== null ||
          nextState.terminal?.text !== terminalText
        ) {
          throw new FlorenceStoreConflict("Terminal family work needs its exact terminal checkpoint");
        }
        if (nextState.progressRevision <= currentState.progressRevision) {
          throw new FlorenceStoreConflict("A family work result must advance its progress revision");
        }
        const updated = await sql`
          update proactive_work set task_state=${sql.json(nextState)},status='delivering',
            next_check_at=null,current_conclusion=${terminalText},last_error=null
          where id=${work.id} and kind='family_task' and status='active'
            and task_state->>'generation'=${String(input.generation)}
            and task_state->'claim'->>'claimId'=${input.claimId} returning id
        `;
        if (updated.length !== 1) return "stale";
        await insertFamilyWorkOutbound(sql, work, nextState, "terminal", terminalText, settledAt);
        return "settled";
      }

      if (nextState.phase === "waiting" || nextState.phase === "terminal") {
        throw new FlorenceStoreConflict("Retrying family work needs a runnable durable phase");
      }
      const retryAt = instant(input.result.retryAt);
      if (retryAt <= settledAt) {
        throw new FlorenceStoreConflict("A family work retry must be scheduled in the future");
      }
      const updated = await sql`
        update proactive_work set task_state=${sql.json(nextState)},status='active',
          next_check_at=${retryAt},last_error=${bounded(required(input.result.error, "Family work retry"), 2_000)}
        where id=${work.id} and kind='family_task' and status='active'
          and task_state->>'generation'=${String(input.generation)}
          and task_state->'claim'->>'claimId'=${input.claimId} returning id
      `;
      return updated.length === 1 ? "settled" : "stale";
    });
  }

  async fireDueReminder(input: { workId: string; occurredAt: string }): Promise<void> {
    assertUuid(input.workId, "Reminder ID");
    const occurredAt = instant(input.occurredAt);
    await this.#sql.begin(async (sql) => {
      const [work] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work
        where id=${input.workId} and kind='reminder' and status='active'
          and next_check_at<=${occurredAt}
        for update
      `;
      if (!work?.objective || !work.next_check_at) return;
      const schedule = reminderSchedule(work.reminder_schedule);
      const [household] = await sql<{ time_zone: string }[]>`
        select time_zone from households where id=${work.household_id} for share
      `;
      if (!household) throw new FlorenceStoreConflict("The reminder household no longer exists");
      const channel = await activeReminderChannel(sql, work);
      if (!channel) {
        await sql`
          update proactive_work set status='paused',next_check_at=null,
            last_error='Paused because this reminder no longer has an active Messages conversation'
          where id=${work.id}
        `;
        return;
      }
      const scheduledAt = work.next_check_at;
      await insertProactiveOutbound(sql, {
        workId: work.id,
        suffix: `reminder:${scheduledAt.toISOString()}`,
        householdId: work.household_id,
        channel,
        visibility: work.visibility,
        ownerAdultId: work.owner_adult_id,
        text: reminderText(work.objective),
        metadata: { reminderId: work.id, scheduledAt: scheduledAt.toISOString() },
        notBefore: occurredAt,
        occurredAt,
      });
      const nextAt = nextReminderOccurrence(schedule, occurredAt, household.time_zone);
      await sql`
        update proactive_work set
          status=${schedule.kind === "once" ? "delivering" : "active"},
          next_check_at=${nextAt},last_run_at=${occurredAt},last_error=null
        where id=${work.id}
      `;
    });
  }

  async completeGooglePoll(input: {
    workId: string;
    gmailCursor: string | null;
    calendarCursor: string;
    googleEvidence: readonly GoogleEvidenceDraft[];
    reviewedGoogleSources: readonly ReviewedGoogleSourceDisposition[];
    removedGoogleSourceIds: readonly string[];
    deliveries: readonly ProactiveDelivery[];
    facts: readonly GoogleStableFactDraft[];
    deliverNotBefore: string;
    occurredAt: string;
  }): Promise<void> {
    assertUuid(input.workId, "Google poll work ID");
    const calendarCursor = required(input.calendarCursor, "Google Calendar cursor");
    const occurredAt = instant(input.occurredAt);
    const deliverNotBefore = proactiveDeliveryTime(input.deliverNotBefore, occurredAt);
    const facts = googleStableFacts(input.facts);
    const reviewedGoogleSources = exactReviewedGoogleSources({
      evidence: input.googleEvidence,
      review: input.reviewedGoogleSources,
      desiredSourceIds: unique([
        ...facts.flatMap((fact) => [...fact.sourceIds]),
        ...input.deliveries.flatMap((delivery) => [...delivery.sourceIds]),
      ]),
    });
    await this.#sql.begin(async (sql) => {
      const [work] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where id=${input.workId} and status='active'
          and kind in ('personal_google_poll','family_calendar_poll') for update
      `;
      if (!work) throw new FlorenceStoreConflict("The Google poll is no longer active");
      if (work.kind === "personal_google_poll") {
        if (!work.owner_adult_id || !input.gmailCursor) {
          throw new FlorenceStoreConflict("A personal Google poll requires both provider cursors");
        }
        const [authority] = await sql<{ id: string }[]>`
          select p.id from people p join google_connections g on g.household_id=p.household_id
            and g.owner_adult_id=p.id and g.status='active'
          join linq_channels c on c.household_id=p.household_id and c.audience='private'
            and c.adult_one_id=p.id and c.adult_two_id is null
            and c.revoked_at is null and c.stopped_at is null
          where p.household_id=${work.household_id} and p.id=${work.owner_adult_id}
            and p.kind='adult' and p.status='verified'
            and nullif(p.preferences->>'proactiveUseAcceptedAt','') is not null
            and coalesce(p.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
            and g.granted_scopes @> ${sql.array([COMPLETE_CALENDAR_HISTORY_SCOPE])}
          limit 1 for share of p,g,c
        `;
        if (!authority) {
          throw new FlorenceStoreUnauthorized("The personal Google poll authority is no longer active");
        }
      } else {
        if (facts.length > 0) {
          throw new FlorenceStoreUnauthorized("A family Calendar poll cannot create stable facts");
        }
        if (input.gmailCursor !== null) {
          throw new FlorenceStoreConflict("A family Calendar poll cannot advance a Gmail cursor");
        }
        const [authority] = await sql<{ id: string }[]>`
          select h.id from households h
          join google_connections g on g.household_id=h.id and g.status='active'
            and g.id in (h.family_calendar_owner_connection_id,h.family_calendar_partner_connection_id)
          join people p on p.household_id=h.id and p.id=g.owner_adult_id
            and p.kind='adult' and p.status='verified'
          join linq_channels c on c.household_id=h.id and c.audience='group'
            and c.adult_two_id is not null and c.revoked_at is null and c.stopped_at is null
          where h.id=${work.household_id} and h.family_calendar_id is not null
            and h.family_calendar_created_at is not null
            and nullif(p.preferences->>'proactiveUseAcceptedAt','') is not null
            and coalesce(p.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
          limit 1 for share of h,g,p,c
        `;
        if (!authority) {
          throw new FlorenceStoreUnauthorized("The family Calendar poll authority is no longer active");
        }
      }
      const echoFilter =
        work.kind === "family_calendar_poll"
          ? await withoutExactCommittedCalendarEchoes(
              sql,
              work.household_id,
              input.googleEvidence,
              input.deliveries,
            )
          : { deliveries: input.deliveries, echoedSourceIds: [] };
      const deliveries = echoFilter.deliveries;
      const removedSourceIds = unique([...input.removedGoogleSourceIds, ...echoFilter.echoedSourceIds]);
      if (input.removedGoogleSourceIds.some((sourceId) => reviewedGoogleSources.has(sourceId))) {
        throw new FlorenceStoreConflict(
          "A Google provider item cannot be both reviewed and removed in one completion",
        );
      }
      const echoedSourceIds = new Set(echoFilter.echoedSourceIds);
      const reconciledSourceIds = [...reviewedGoogleSources.keys()].filter(
        (sourceId) => !echoedSourceIds.has(sourceId),
      );
      await reconcileRemovedGoogleSources(sql, {
        householdId: work.household_id,
        ownerAdultId: work.owner_adult_id,
        sourceIds: removedSourceIds,
      });
      await persistGoogleEvidenceDrafts(sql, {
        householdId: work.household_id,
        drafts: input.googleEvidence,
        sourceIds: reconciledSourceIds,
      });
      if (work.kind === "personal_google_poll") {
        if (!work.owner_adult_id) {
          throw new FlorenceStoreConflict("A personal Google poll requires an adult owner");
        }
        const [connection] = await sql<{ id: string }[]>`
          select id from google_connections where household_id=${work.household_id}
            and owner_adult_id=${work.owner_adult_id} and status='active'
            and granted_scopes @> ${sql.array([COMPLETE_CALENDAR_HISTORY_SCOPE])}
          order by created_at desc,id desc limit 1 for share
        `;
        if (!connection) {
          throw new FlorenceStoreUnauthorized("The personal Google fact authority is no longer active");
        }
        await upsertGoogleStableFacts(sql, {
          householdId: work.household_id,
          ownerAdultId: work.owner_adult_id,
          connectionId: connection.id,
          currentEvidenceSourceIds: input.googleEvidence.map((source) => source.id),
          facts,
          occurredAt,
        });
      }
      const desiredState = await applyGooglePollDeliveries(sql, {
        work,
        deliveries,
        deliverNotBefore,
        occurredAt,
      });
      await reconcileHouseholdDocketCandidates(sql, {
        work,
        reviewedSourceIds: reconciledSourceIds,
        candidates: desiredState.householdDocketCandidates,
        preservedActions: desiredState.preservedHouseholdDocketActions,
        completedActionKeys: desiredState.completedMonitorActionKeys,
      });
      await reconcileReviewedGoogleSources(sql, {
        work,
        reviewedSourceIds: reconciledSourceIds,
        facts,
        desiredState,
      });
      const gmailCursor =
        work.kind === "personal_google_poll" ? required(input.gmailCursor ?? "", "Gmail cursor") : null;
      const [updated] = await sql<{ id: string }[]>`
        update proactive_work set gmail_cursor=${gmailCursor},calendar_cursor=${calendarCursor},
          next_check_at=${new Date(occurredAt.getTime() + GOOGLE_POLL_INTERVAL_MS)},
          last_error=null
        where id=${work.id} and status='active' returning id
      `;
      if (!updated) throw new FlorenceStoreConflict("The Google poll changed before completion");
    });
  }

  async restartPersonalGooglePollAsInitialScan(input: {
    workId: string;
    connectionId: string;
    now: string;
  }): Promise<void> {
    assertUuid(input.workId, "Personal Google poll work ID");
    assertUuid(input.connectionId, "Personal Google poll connection ID");
    const now = instant(input.now);
    await this.#sql.begin(async (sql) => {
      const [poll] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where id=${input.workId} and kind='personal_google_poll'
          and status='active' for update
      `;
      if (!poll?.owner_adult_id) {
        throw new FlorenceStoreConflict("The personal Google poll is no longer active");
      }
      const [connection] = await sql<{ id: string }[]>`
        select id from google_connections where id=${input.connectionId}
          and household_id=${poll.household_id} and owner_adult_id=${poll.owner_adult_id}
          and status='active'
          and granted_scopes @> ${sql.array([COMPLETE_CALENDAR_HISTORY_SCOPE])}
        for share
      `;
      const [review] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where household_id=${poll.household_id}
          and owner_adult_id=${poll.owner_adult_id} and kind='initial_private_review'
        for update
      `;
      if (!connection || !review) {
        throw new FlorenceStoreUnauthorized("A complete Google rescan is no longer authorized");
      }
      await sql`
        update proactive_work set status='active',next_check_at=${now},created_at=${now},
          briefing_candidates='[]'::jsonb,last_error=null where id=${review.id}
      `;
      await sql`
        update proactive_work set status='paused',next_check_at=null,
          last_error='Paused while Florence rebuilds complete Google coverage' where id=${poll.id}
      `;
    });
  }

  async completeFiniteMonitor(input: {
    workId: string;
    outcome: "silent" | "update" | "complete";
    privateDetail: string | null;
    householdConclusion: string | null;
    householdCategory: SharedBriefingCandidate["category"] | null;
    sourceIds: readonly string[];
    currentConclusion: string;
    nextCheck: string | null;
    why: string;
    googleEvidence: readonly GoogleEvidenceDraft[];
    deliverNotBefore: string;
    occurredAt: string;
  }): Promise<void> {
    const occurredAt = instant(input.occurredAt);
    const deliverNotBefore = proactiveDeliveryTime(input.deliverNotBefore, occurredAt);
    await this.#sql.begin(async (sql) => {
      const [work] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where id=${input.workId} and kind='finite_monitor'
          and status='active' for update
      `;
      if (!work) throw new FlorenceStoreConflict("The finite monitor is no longer active");
      const googleActionKey = googleActionKeyFromWorkMarker(work.last_error);
      if (
        (input.householdConclusion === null) !== (input.householdCategory === null) ||
        (input.householdCategory !== null && !isSharedBriefingCategory(input.householdCategory))
      ) {
        throw new FlorenceStoreConflict("A finite monitor household conclusion needs one valid category");
      }
      const [privateOwner] =
        work.visibility === "private" && work.owner_adult_id
          ? await sql<{ conflict_sharing_enabled: boolean }[]>`
              select coalesce(preferences->'privateConflictBusySharingEnabled'='true'::jsonb,false)
                as conflict_sharing_enabled
              from people where household_id=${work.household_id} and id=${work.owner_adult_id}
                and kind='adult' and status='verified'
                and nullif(preferences->>'proactiveUseAcceptedAt','') is not null
                and coalesce(preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
              for share
            `
          : [];
      if (work.visibility === "private" && !privateOwner) {
        throw new FlorenceStoreUnauthorized("The private monitor owner is no longer active");
      }
      if (work.visibility === "household") {
        const [householdAuthority] = await sql<{ id: string }[]>`
          select h.id from households h
          join google_connections g on g.household_id=h.id and g.status='active'
            and g.id in (h.family_calendar_owner_connection_id,h.family_calendar_partner_connection_id)
          join people p on p.household_id=h.id and p.id=g.owner_adult_id
            and p.kind='adult' and p.status='verified'
          where h.id=${work.household_id} and h.family_calendar_id is not null
            and h.family_calendar_created_at is not null
            and nullif(p.preferences->>'proactiveUseAcceptedAt','') is not null
            and coalesce(p.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
          limit 1 for share of h,g,p
        `;
        if (!householdAuthority) {
          throw new FlorenceStoreUnauthorized("The household monitor authority is no longer active");
        }
      }
      const householdConclusion =
        work.visibility === "private" &&
        input.householdCategory === "conflict" &&
        !privateOwner?.conflict_sharing_enabled
          ? null
          : input.householdConclusion;
      const sourceIds = unique(input.sourceIds);
      if (input.outcome === "silent" && sourceIds.length > 0) {
        throw new FlorenceStoreConflict("A silent monitor cannot retain current evidence");
      }
      if (input.outcome === "complete" && googleActionKey) {
        // Remove against the pre-update provider identity. Calendar cancellation and time changes
        // can otherwise mutate the source before a legacy candidate's action key is reconstructed.
        await removeHouseholdDocketCandidateForGoogleAction(sql, {
          householdId: work.household_id,
          ownerAdultId: work.owner_adult_id,
          actionKey: googleActionKey,
        });
      }
      await persistGoogleEvidenceDrafts(sql, {
        householdId: work.household_id,
        drafts: input.googleEvidence,
        sourceIds:
          input.outcome !== "silent" && (input.privateDetail || householdConclusion) ? sourceIds : [],
      });
      if (sourceIds.length > 0) {
        await assertProactiveSources(sql, work.household_id, work.visibility, work.owner_adult_id, sourceIds);
        for (const sourceId of sourceIds) {
          await sql`
            insert into proactive_work_sources (work_id,source_id) values (${work.id},${sourceId})
            on conflict do nothing
          `;
        }
      }
      if (input.outcome === "silent" && (input.privateDetail || householdConclusion)) {
        throw new FlorenceStoreConflict("A silent monitor cannot send an update");
      }
      if (
        input.outcome !== "silent" &&
        (input.privateDetail || householdConclusion) &&
        sourceIds.length === 0
      ) {
        throw new FlorenceStoreConflict("A finite monitor notification requires current evidence");
      }
      const persistedConclusion =
        work.visibility === "household"
          ? input.outcome === "silent"
            ? required(work.current_conclusion ?? "", "Stored household monitor conclusion")
            : bounded(required(householdConclusion ?? "", "Household-safe monitor conclusion"), 4_000)
          : bounded(required(input.currentConclusion, "Monitor conclusion"), 4_000);
      const persistedWhy =
        work.visibility === "household"
          ? input.outcome === "silent"
            ? required(work.why ?? "", "Stored household monitor reason")
            : HOUSEHOLD_SAFE_MONITOR_WHY
          : bounded(required(input.why, "Monitor reason"), 2_000);
      if (input.outcome !== "silent") {
        const [privateChannel] =
          work.visibility === "private" && work.owner_adult_id
            ? await sql<ChannelRow[]>`
                select * from linq_channels where household_id=${work.household_id}
                  and audience='private' and adult_one_id=${work.owner_adult_id}
                  and adult_two_id is null and revoked_at is null and stopped_at is null
                order by bound_at,id limit 1 for share
              `
            : [];
        const [groupChannel] = await sql<ChannelRow[]>`
          select * from linq_channels where household_id=${work.household_id}
            and audience='group' and adult_two_id is not null
            and revoked_at is null and stopped_at is null
          order by bound_at,id limit 1 for share
        `;
        const privateTerminalAlreadyDelivered =
          input.outcome === "complete" && googleActionKey && work.owner_adult_id
            ? await wasGoogleActionTerminallyDelivered(sql, {
                householdId: work.household_id,
                visibility: "private",
                ownerAdultId: work.owner_adult_id,
                actionKey: googleActionKey,
              })
            : false;
        if (
          work.visibility === "private" &&
          input.privateDetail &&
          privateChannel &&
          !privateTerminalAlreadyDelivered
        ) {
          await insertProactiveOutbound(sql, {
            workId: work.id,
            suffix: `${input.outcome}:private:${googleActionKey ?? sha256(persistedConclusion)}`,
            householdId: work.household_id,
            channel: privateChannel,
            visibility: "private",
            ownerAdultId: work.owner_adult_id,
            text: input.privateDetail,
            ...(googleActionKey
              ? {
                  metadata: {
                    googleActionKeys: [googleActionKey],
                    googleSourceIds: sourceIds,
                    ...(input.outcome === "complete" ? { googleActionTerminal: true } : {}),
                  },
                }
              : {}),
            notBefore: deliverNotBefore,
            occurredAt,
          });
        }
        const householdTerminalAlreadyDelivered =
          input.outcome === "complete" && googleActionKey
            ? await wasGoogleActionTerminallyDelivered(sql, {
                householdId: work.household_id,
                visibility: "household",
                ownerAdultId: null,
                actionKey: googleActionKey,
              })
            : false;
        if (householdConclusion && groupChannel && !householdTerminalAlreadyDelivered) {
          await insertProactiveOutbound(sql, {
            workId: work.id,
            suffix: `${input.outcome}:household:${googleActionKey ?? sha256(persistedConclusion)}`,
            householdId: work.household_id,
            channel: groupChannel,
            visibility: "household",
            ownerAdultId: null,
            text: householdConclusion,
            ...(googleActionKey ||
            (work.visibility === "private" && input.householdCategory === "conflict" && work.owner_adult_id)
              ? {
                  metadata: {
                    ...(googleActionKey
                      ? {
                          googleActionKeys: [googleActionKey],
                          googleSourceIds: sourceIds,
                          ...(input.outcome === "complete" ? { googleActionTerminal: true } : {}),
                        }
                      : {}),
                    ...(work.visibility === "private" &&
                    input.householdCategory === "conflict" &&
                    work.owner_adult_id
                      ? { privateConflictOwnerAdultIds: [work.owner_adult_id] }
                      : {}),
                  },
                }
              : {}),
            notBefore: deliverNotBefore,
            occurredAt,
          });
        }
        if (work.visibility === "household" && !householdConclusion) {
          throw new FlorenceStoreConflict("A household monitor update needs household-safe copy");
        }
      }
      const nextCheck = input.nextCheck ? instant(input.nextCheck) : null;
      if (input.outcome === "complete" ? nextCheck !== null : !nextCheck || nextCheck <= occurredAt) {
        throw new FlorenceStoreConflict("A finite monitor returned an invalid next check");
      }
      if (input.outcome === "complete") {
        await sql`
          update proactive_work set status='completed',current_conclusion=${persistedConclusion},
            why=${persistedWhy},next_check_at=null
          where id=${work.id} and kind='finite_monitor' and status='active'
        `;
      } else {
        await sql`
          update proactive_work set status='active',current_conclusion=${persistedConclusion},
            why=${persistedWhy},next_check_at=${nextCheck},
            last_error=${googleActionKey ? googleActionWorkMarker(googleActionKey) : null}
          where id=${work.id}
        `;
      }
    });
  }

  async completeInterestMonitor(input: {
    workId: string;
    judgment: "recommend" | "consider" | "skip";
    summary: string;
    urls: readonly string[];
    deliverNotBefore: string;
    occurredAt: string;
  }): Promise<void> {
    const occurredAt = instant(input.occurredAt);
    const deliverNotBefore = proactiveDeliveryTime(input.deliverNotBefore, occurredAt);
    if (input.urls.length < 1 || input.urls.length > 3) {
      throw new FlorenceStoreConflict("Interest research needs one to three sources");
    }
    const summary = bounded(required(input.summary, "Interest research summary"), 4_000);
    await this.#sql.begin(async (sql) => {
      const [work] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where id=${input.workId} and kind='interest_monitor'
          and status='active' for update
      `;
      if (!work) throw new FlorenceStoreConflict("The interest monitor is no longer active");
      const [household] = await sql<{ id: string }[]>`
        select id from households where id=${work.household_id} for update
      `;
      if (!household) throw new FlorenceStoreConflict("The interest household is no longer active");
      const [groupChannel] = await sql<ChannelRow[]>`
        select c.* from linq_channels c join households h on h.id=c.household_id
        join google_connections g on g.household_id=h.id and g.status='active'
          and g.id in (h.family_calendar_owner_connection_id,h.family_calendar_partner_connection_id)
        join people p on p.household_id=h.id and p.id=g.owner_adult_id
          and p.kind='adult' and p.status='verified'
        where c.household_id=${work.household_id} and c.audience='group'
          and c.adult_two_id is not null and c.revoked_at is null and c.stopped_at is null
          and h.family_calendar_id is not null and h.family_calendar_created_at is not null
          and nullif(p.preferences->>'proactiveUseAcceptedAt','') is not null
          and coalesce(p.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
        order by c.bound_at,c.id,p.adult_slot limit 1 for share of c,h,g,p
      `;
      if (!groupChannel) throw new FlorenceStoreConflict("The family group is unavailable");
      const urls = unique(input.urls.map((url) => required(url, "Interest research URL")));
      const sourceIds: string[] = [];
      for (const url of urls) {
        const parsed = new URL(url);
        if (parsed.protocol !== "https:" && parsed.protocol !== "http:") {
          throw new FlorenceStoreConflict("An interest source must use HTTP(S)");
        }
        const sourceId = deterministicUuid(`interest-web-source\0${work.household_id}\0${url}`);
        await sql`
          insert into sources (
            id,household_id,kind,visibility,owner_adult_id,external_key,label,metadata,occurred_at
          ) values (${sourceId},${work.household_id},'web','household',null,${url},
            ${bounded(parsed.hostname, 500)},${sql.json({ url })},${occurredAt})
          on conflict (household_id,kind,external_key) do nothing
        `;
        sourceIds.push(sourceId);
        await sql`
          insert into proactive_work_sources (work_id,source_id) values (${work.id},${sourceId})
          on conflict do nothing
        `;
      }
      const [recentSuggestions] = await sql<{ count: number }[]>`
        select count(*)::int as count from messages m join sources s on s.id=m.source_id
        where s.household_id=${work.household_id} and m.direction='outbound'
          and m.status in ('pending','sending','sent')
          and s.metadata->>'kind'='interest_suggestion'
          and coalesce(m.sent_at,s.occurred_at)>=${new Date(occurredAt.getTime() - 7 * 24 * 60 * 60_000)}
      `;
      const shouldNotify = input.judgment !== "skip" && (recentSuggestions?.count ?? 0) < 2;
      if (shouldNotify) {
        await insertProactiveOutbound(sql, {
          workId: work.id,
          suffix: `interest:${sha256(JSON.stringify({ summary, urls }))}`,
          householdId: work.household_id,
          channel: groupChannel,
          visibility: "household",
          ownerAdultId: null,
          text: `${summary}\n\n${urls.join("\n")}`,
          metadata: { kind: "interest_suggestion", workId: work.id },
          notBefore: deliverNotBefore,
          occurredAt,
        });
      }
      await sql`
        update proactive_work set status='active',
          next_check_at=${new Date(occurredAt.getTime() + INTEREST_CHECK_INTERVAL_MS)},
          current_conclusion=${summary},last_error=null where id=${work.id}
      `;
    });
  }

  async retryProactiveWork(input: { workId: string; retryAt: string; error: string }): Promise<void> {
    const retryAt = instant(input.retryAt);
    const error = bounded(input.error, 2_000);
    const [row] = await this.#sql<{ id: string }[]>`
      update proactive_work set next_check_at=${retryAt},
        last_error=case
          when kind='finite_monitor'
            and left(coalesce(last_error,''),${GOOGLE_ACTION_WORK_MARKER_PREFIX.length})=${GOOGLE_ACTION_WORK_MARKER_PREFIX}
            and length(coalesce(last_error,''))>=${GOOGLE_ACTION_WORK_MARKER_LENGTH}
          then left(last_error,${GOOGLE_ACTION_WORK_MARKER_LENGTH})||E'\n'||${error}
          else ${error}
        end
      where id=${input.workId} and kind in (
        'personal_google_poll','family_calendar_poll','finite_monitor','interest_monitor'
      ) and status='active' returning id
    `;
    if (!row) throw new FlorenceStoreConflict("The proactive work is no longer retryable");
  }

  async patchProactiveWork(
    input: {
      householdId: string;
      actorAdultId: string;
      workId: string;
      status?: "active" | "paused";
      now: string;
    } & (
      | {
          kind: "monitor";
          objective?: string;
          endCondition?: string;
        }
      | {
          kind: "interest";
          objective?: string;
          genericTerms?: readonly string[];
        }
    ),
  ): Promise<void> {
    assertUuid(input.householdId, "Household ID");
    assertUuid(input.actorAdultId, "Adult ID");
    assertUuid(input.workId, "Proactive work ID");
    const hasTypeSpecificCorrection =
      input.kind === "monitor" ? input.endCondition !== undefined : input.genericTerms !== undefined;
    if (input.objective === undefined && input.status === undefined && !hasTypeSpecificCorrection) {
      throw new FlorenceStoreConflict("A watch correction or status is required");
    }
    const objective =
      input.objective === undefined
        ? undefined
        : bounded(required(input.objective, "Watch objective"), 2_000);
    const now = instant(input.now);
    await this.#sql.begin(async (sql) => {
      const [work] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where id=${input.workId} and household_id=${input.householdId}
          and kind in ('finite_monitor','interest_monitor') and status in ('active','paused')
        for update
      `;
      if (!work) throw new FlorenceStoreUnauthorized();
      const expectedKind = input.kind === "monitor" ? "finite_monitor" : "interest_monitor";
      if (work.kind !== expectedKind) {
        throw new FlorenceStoreConflict("The watch type does not match this correction");
      }
      const [actor] = await sql<{ id: string }[]>`
        select id from people where household_id=${input.householdId} and id=${input.actorAdultId}
          and kind='adult' and status='verified' for share
      `;
      if (!actor || (work.visibility === "private" && work.owner_adult_id !== input.actorAdultId)) {
        throw new FlorenceStoreUnauthorized();
      }
      const nextCheckAt =
        input.status === "paused"
          ? null
          : input.status === "active" && work.status === "paused"
            ? now
            : work.next_check_at;
      const endCondition =
        input.kind === "monitor" && input.endCondition !== undefined
          ? bounded(required(input.endCondition, "Monitor end condition"), 2_000)
          : work.end_condition;
      const genericTerms =
        input.kind === "interest" && input.genericTerms !== undefined
          ? normalizedInterestTerms(input.genericTerms)
          : work.discovery_terms;
      await sql`
        update proactive_work set
          objective=${objective ?? work.objective},
          end_condition=${endCondition},
          discovery_terms=${genericTerms},
          status=${input.status ?? work.status},
          next_check_at=${nextCheckAt},
          last_error=case
            when kind='finite_monitor'
              and left(coalesce(last_error,''),${GOOGLE_ACTION_WORK_MARKER_PREFIX.length})=${GOOGLE_ACTION_WORK_MARKER_PREFIX}
            then left(last_error,${GOOGLE_ACTION_WORK_MARKER_LENGTH})
            else null
          end
        where id=${work.id}
      `;
    });
  }

  async stopProactiveWork(input: {
    householdId: string;
    actorAdultId: string;
    workId: string;
  }): Promise<void> {
    assertUuid(input.householdId, "Household ID");
    assertUuid(input.actorAdultId, "Adult ID");
    assertUuid(input.workId, "Proactive work ID");
    await this.#sql.begin(async (sql) => {
      const [work] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where id=${input.workId} and household_id=${input.householdId}
          and kind in ('finite_monitor','interest_monitor') and status in ('active','paused')
        for update
      `;
      if (!work) throw new FlorenceStoreUnauthorized();
      const [actor] = await sql<{ id: string }[]>`
        select id from people where household_id=${input.householdId} and id=${input.actorAdultId}
          and kind='adult' and status='verified' for share
      `;
      if (!actor || (work.visibility === "private" && work.owner_adult_id !== input.actorAdultId)) {
        throw new FlorenceStoreUnauthorized();
      }
      await sql`delete from proactive_work where id=${work.id}`;
    });
  }

  async completeFounderOnboarding(
    input: CompleteFounderOnboardingInput,
  ): Promise<MessagesEnrollmentResult | null> {
    assertDigest(input.setupTokenDigest, "Founder setup token");
    assertDigest(input.identitySubjectDigest, "Messages identity");
    const setupExpiresAt = instant(input.setupExpiresAt);
    const consentedAt = instant(input.consentedAt);
    const guardianAttestedAt = instant(input.guardianAttestedAt);
    const proactiveUseAcceptedAt = instant(input.proactiveUseAcceptedAt);
    if (typeof input.privateConflictBusySharingEnabled !== "boolean") {
      throw new FlorenceStoreConflict("Private conflict sharing permission must be explicit");
    }
    const occurredAt = instant(input.occurredAt);
    if (setupExpiresAt <= occurredAt) return null;
    const timeZone = required(input.timeZone, "Household time zone");
    const firstName = required(input.firstName, "Founder first name");
    const lastName = required(input.lastName, "Founder last name");
    const displayName = memberDisplayName(firstName, lastName);
    const messagesAddress = required(input.messagesAddress, "Founder Messages address");
    if (!/^\+[1-9]\d{7,14}$/.test(messagesAddress)) {
      throw new FlorenceStoreConflict("Founder Messages address must be E.164");
    }
    const consentVersion = required(input.consentVersion, "Messages consent version");
    const providerConversationId = required(input.providerConversationId, "Linq conversation ID");

    const completion = this.#sql.begin(async (sql) => {
      const [replay] = await sql<
        {
          id: string;
          household_id: string;
          kind: "adult" | "child";
          role: "steward" | "caregiver" | "dependent";
          adult_slot: 1 | 2 | null;
          display_name: string;
          status: "planned" | "verified" | "represented";
          identity_subject_digest: string | null;
          consent_version: string | null;
          consented_at: Date | null;
          guardian_attested_at: Date | null;
          invitation_expires_at: Date | null;
          invitation_consumed_at: Date | null;
          first_name: string | null;
          last_name: string | null;
          proactive_use_accepted_at: string | null;
          private_conflict_busy_sharing_enabled: JsonValue | null;
          messages_address: string | null;
          household_name: string;
          time_zone: string;
        }[]
      >`
        select p.id,p.household_id,p.kind,p.role,p.adult_slot,p.display_name,p.status,
               p.identity_subject_digest,p.consent_version,p.consented_at,p.guardian_attested_at,
               p.invitation_expires_at,p.invitation_consumed_at,
               p.profile->>'firstName' as first_name,p.profile->>'lastName' as last_name,
               p.preferences->>'proactiveUseAcceptedAt' as proactive_use_accepted_at,
               p.preferences->'privateConflictBusySharingEnabled' as private_conflict_busy_sharing_enabled,
               p.messages_address,
               h.name as household_name,h.time_zone
        from people p join households h on h.id=p.household_id
        where p.invitation_digest=${input.setupTokenDigest}
        for update of p
      `;
      if (replay) {
        const [channel] = await sql<ChannelRow[]>`
          select * from linq_channels where household_id=${replay.household_id}
            and adult_one_id=${replay.id} and audience='private' and revoked_at is null
          limit 1
        `;
        const recoveryAgeMs = replay.invitation_consumed_at
          ? occurredAt.getTime() - replay.invitation_consumed_at.getTime()
          : Number.POSITIVE_INFINITY;
        if (
          replay.id !== input.adultId ||
          replay.household_id !== input.householdId ||
          replay.kind !== "adult" ||
          replay.role !== "steward" ||
          replay.adult_slot !== 1 ||
          replay.status !== "verified" ||
          replay.display_name !== displayName ||
          replay.first_name !== firstName ||
          replay.last_name !== lastName ||
          replay.proactive_use_accepted_at !== proactiveUseAcceptedAt.toISOString() ||
          replay.private_conflict_busy_sharing_enabled !== input.privateConflictBusySharingEnabled ||
          replay.messages_address !== messagesAddress ||
          replay.household_name !== "Family" ||
          replay.time_zone !== timeZone ||
          replay.identity_subject_digest !== input.identitySubjectDigest ||
          replay.consent_version !== consentVersion ||
          replay.consented_at === null ||
          replay.guardian_attested_at === null ||
          replay.invitation_expires_at?.getTime() !== setupExpiresAt.getTime() ||
          replay.invitation_consumed_at === null ||
          recoveryAgeMs < 0 ||
          recoveryAgeMs > 60_000 ||
          channel?.provider_conversation_id !== providerConversationId ||
          channel.identity_one_digest !== input.identitySubjectDigest
        ) {
          return null;
        }
        return {
          disposition: "duplicate" as const,
          householdId: replay.household_id,
          adultId: replay.id,
          channel: channelRecord(channel),
        };
      }

      const [reservedMessagesIdentity] = await sql<{ id: string }[]>`
        select id from people where messages_address=${messagesAddress}
          or invitation_conversation_id=${providerConversationId}
          or invitation_identity_digest=${input.identitySubjectDigest}
          or (kind='adult' and role='steward' and adult_slot=2
            and status='planned' and identity_subject_digest is null
            and profile->>'phoneNumber'=${messagesAddress})
        limit 1 for update
      `;
      if (reservedMessagesIdentity) return null;
      const [identityOwner] = await sql<{ id: string }[]>`
        select id from people where identity_subject_digest=${input.identitySubjectDigest} limit 1
      `;
      if (identityOwner) return null;
      const [conversation] = await sql<{ id: string }[]>`
        select id from linq_channels where provider_conversation_id=${providerConversationId}
          and revoked_at is null limit 1
      `;
      if (conversation) return null;

      await sql`
        insert into households (id,name,time_zone,created_at,updated_at)
        values (${input.householdId},'Family',${timeZone},${occurredAt},${occurredAt})
      `;
      await sql`
        insert into people (
          id,household_id,kind,role,adult_slot,display_name,status,identity_subject_digest,
          consent_version,consented_at,guardian_attested_at,invitation_digest,
          invitation_expires_at,invitation_consumed_at,messages_address,profile,preferences,
          created_at,updated_at
        ) values (${input.adultId},${input.householdId},'adult','steward',1,${displayName},'verified',
          ${input.identitySubjectDigest},${consentVersion},${consentedAt},${guardianAttestedAt},
          ${input.setupTokenDigest},${setupExpiresAt},${occurredAt},${messagesAddress},
          ${sql.json({ firstName, lastName })},
          ${sql.json({
            proactiveUseAcceptedAt: proactiveUseAcceptedAt.toISOString(),
            privateConflictBusySharingEnabled: input.privateConflictBusySharingEnabled,
          })},
          ${occurredAt},${occurredAt})
      `;
      const authorityDigest = digestStrings([input.adultId, input.identitySubjectDigest]);
      const [channel] = await sql<ChannelRow[]>`
        insert into linq_channels (
          id,household_id,audience,provider_conversation_id,adult_one_id,identity_one_digest,
          authority_digest,bound_at
        ) values (${deterministicUuid(`linq-private\0${providerConversationId}`)},${input.householdId},
          'private',${providerConversationId},${input.adultId},${input.identitySubjectDigest},
          ${authorityDigest},${occurredAt})
        returning *
      `;
      if (!channel) throw new Error("The founder's private Messages channel was not bound");
      return {
        disposition: "accepted" as const,
        householdId: input.householdId,
        adultId: input.adultId,
        channel: channelRecord(channel),
      };
    });
    return completion.catch((error: unknown) => {
      if (typeof error === "object" && error !== null && "code" in error && error.code === "23505") {
        return null;
      }
      throw error;
    });
  }

  async completeFamilyOnboarding(input: CompleteFamilyOnboardingInput): Promise<void> {
    assertUuid(input.householdId, "Household ID");
    assertUuid(input.founderAdultId, "Founder adult ID");
    const occurredAt = instant(input.occurredAt);
    const postalCode = required(input.postalCode, "Home ZIP");
    if (!/^\d{5}(?:-\d{4})?$/.test(postalCode)) {
      throw new FlorenceStoreConflict("Home ZIP must be a US ZIP code");
    }
    if ((input.mode === "two_adult" && !input.partner) || (input.mode === "solo" && input.partner)) {
      throw new FlorenceStoreConflict("Family onboarding mode and partner must agree");
    }
    const partner = input.partner
      ? (() => {
          const firstName = required(input.partner.firstName, "Partner first name");
          const lastName = required(input.partner.lastName, "Partner last name");
          return {
            id: deterministicUuid(`family-partner\0${input.householdId}`),
            firstName,
            lastName,
            displayName: memberDisplayName(firstName, lastName),
            phoneNumber: required(input.partner.phoneNumber, "Partner phone number"),
          };
        })()
      : null;
    if (partner && !/^\+[1-9]\d{7,14}$/.test(partner.phoneNumber)) {
      throw new FlorenceStoreConflict("Partner phone number must be E.164");
    }
    if (input.children.length < 1 || input.children.length > 20) {
      throw new FlorenceStoreConflict("Family onboarding needs between one and twenty children");
    }
    const children = input.children.map((child, childIndex) => {
      if (child.activities && child.activities.length > 50) {
        throw new FlorenceStoreConflict("A child cannot have more than fifty activities");
      }
      const firstName = required(child.firstName, `Child ${childIndex + 1} first name`);
      const lastName = child.lastName ? required(child.lastName, `Child ${childIndex + 1} last name`) : null;
      const age = child.age === undefined ? null : validChildAge(child.age, `Child ${childIndex + 1} age`);
      const grade =
        child.grade === undefined ? null : validChildGrade(child.grade, `Child ${childIndex + 1} grade`);
      return {
        id: deterministicUuid(`family-child\0${input.householdId}\0${childIndex}`),
        firstName,
        lastName,
        displayName: memberDisplayName(firstName, lastName),
        ...(age !== null ? { age } : {}),
        ...(grade !== null ? { grade } : {}),
        ...(child.school ? { school: required(child.school, `Child ${childIndex + 1} school`) } : {}),
        ...(child.activities
          ? {
              activities: child.activities.map((activity, activityIndex) =>
                required(activity, `Child ${childIndex + 1} activity ${activityIndex + 1}`),
              ),
            }
          : {}),
      };
    });
    const memberIds = [...(partner ? [partner.id] : []), ...children.map((child) => child.id)];
    if (memberIds.includes(input.founderAdultId) || new Set(memberIds).size !== memberIds.length) {
      throw new FlorenceStoreConflict("Family onboarding member IDs must be distinct");
    }

    await this.#sql.begin(async (sql) => {
      const [household] = await sql<{ id: string }[]>`
        select id from households where id=${input.householdId} for update
      `;
      if (!household) throw new FlorenceStoreConflict("The household no longer exists");
      const [founder] = await sql<PersonRow[]>`
        select * from people where household_id=${input.householdId} and id=${input.founderAdultId}
          and kind='adult' and role='steward' and adult_slot=1 and status='verified'
          and guardian_attested_at is not null
        for update
      `;
      if (!founder) {
        throw new FlorenceStoreUnauthorized(
          "A verified, guardian-attested founder must complete family onboarding",
        );
      }
      const [google] = await sql<{ id: string }[]>`
        select id from google_connections where household_id=${input.householdId}
          and owner_adult_id=${input.founderAdultId} and status='active'
        order by created_at,id limit 1 for share
      `;
      if (!google) {
        throw new FlorenceStoreUnauthorized("Family onboarding requires the founder's active Google account");
      }

      if (partner) {
        const [adultTwo] = await sql<PersonRow[]>`
          select * from people where household_id=${input.householdId} and adult_slot=2 for update
        `;
        if (adultTwo && adultTwo.id !== partner.id) {
          throw new FlorenceStoreConflict("The household already has a different second adult");
        }
        const [existingPartner] = await sql<
          PersonRow[]
        >`select * from people where id=${partner.id} for update`;
        if (existingPartner) {
          if (
            existingPartner.household_id !== input.householdId ||
            existingPartner.kind !== "adult" ||
            existingPartner.adult_slot !== 2 ||
            existingPartner.status !== "planned" ||
            existingPartner.identity_subject_digest !== null
          ) {
            throw new FlorenceStoreConflict("Family onboarding can only update a planned second adult");
          }
          await sql`
            update people set display_name=${partner.displayName},role='steward',
              profile=${sql.json({
                relationship: "Partner",
                firstName: partner.firstName,
                lastName: partner.lastName,
                phoneNumber: partner.phoneNumber,
              })},updated_at=${occurredAt}
            where id=${partner.id}
          `;
        } else {
          await sql`
            insert into people (
              id,household_id,kind,role,adult_slot,display_name,status,profile,created_at,updated_at
            ) values (${partner.id},${input.householdId},'adult','steward',2,${partner.displayName},
              'planned',${sql.json({
                relationship: "Partner",
                firstName: partner.firstName,
                lastName: partner.lastName,
                phoneNumber: partner.phoneNumber,
              })},${occurredAt},${occurredAt})
          `;
        }
      }

      for (const child of children) {
        const [existing] = await sql<PersonRow[]>`select * from people where id=${child.id} for update`;
        const profile = {
          relationship: "Child",
          firstName: child.firstName,
          ...(child.lastName ? { lastName: child.lastName } : {}),
          ...(child.age !== undefined ? { age: child.age } : {}),
          ...(child.grade !== undefined ? { grade: child.grade } : {}),
          ...(child.school ? { school: child.school } : {}),
          ...(child.activities ? { activities: child.activities } : {}),
        };
        if (existing) {
          if (
            existing.household_id !== input.householdId ||
            existing.kind !== "child" ||
            existing.status !== "represented" ||
            existing.adult_slot !== null ||
            existing.identity_subject_digest !== null
          ) {
            throw new FlorenceStoreConflict("Family onboarding can only update represented children");
          }
          await sql`
            update people set display_name=${child.displayName},role='dependent',
              profile=${sql.json(profile)},updated_at=${occurredAt}
            where id=${child.id}
          `;
        } else {
          await sql`
            insert into people (
              id,household_id,kind,role,adult_slot,display_name,status,profile,created_at,updated_at
            ) values (${child.id},${input.householdId},'child','dependent',null,${child.displayName},
              'represented',${sql.json(profile)},${occurredAt},${occurredAt})
          `;
        }
      }

      await updateHouseholdNameFromLockedAdults(sql, input.householdId, occurredAt);
      await sql`
        update people set
          profile=profile || ${sql.json({ postalCode, householdMode: input.mode })} ||
            case when nullif(profile->>'onboardingCompletedAt','') is null
              then ${sql.json({ onboardingCompletedAt: occurredAt.toISOString() })}
              else '{}'::jsonb end,
          updated_at=${occurredAt}
        where household_id=${input.householdId} and id=${input.founderAdultId}
      `;
    });
  }

  async upsertMember(input: UpsertMemberInput): Promise<FamilyMemberRecord> {
    const occurredAt = instant(input.occurredAt);
    const row = await this.#sql.begin(async (sql) => {
      await requireSteward(sql, input.householdId, input.actorAdultId);
      const [household] = await sql<{ id: string }[]>`
        select id from households where id=${input.householdId} for update
      `;
      if (!household) throw new FlorenceStoreConflict("The household no longer exists");
      const [existing] = await sql<PersonRow[]>`
        select * from people where household_id=${input.householdId} and id=${input.memberId} for update
      `;
      if (existing) {
        if (input.member.operation !== "patch") {
          throw new FlorenceStoreConflict("That family member already exists");
        }
        if (
          input.member.firstName === undefined &&
          input.member.lastName === undefined &&
          input.member.profile === undefined
        ) {
          throw new FlorenceStoreConflict("A family member edit needs at least one change");
        }
        const profilePatch = input.member.profile ?? {};
        if (
          Object.hasOwn(profilePatch, "postalCode") &&
          (existing.kind !== "adult" || existing.adult_slot !== 1)
        ) {
          throw new FlorenceStoreUnauthorized("Home ZIP belongs to the founding parent profile");
        }
        if (
          existing.kind !== "child" &&
          (Object.hasOwn(profilePatch, "age") || Object.hasOwn(profilePatch, "grade"))
        ) {
          throw new FlorenceStoreUnauthorized("Age and grade can only be edited for children");
        }
        const currentProfile = jsonRecord(existing.profile);
        const firstName = required(
          input.member.firstName ?? jsonString(currentProfile, "firstName") ?? "",
          "First name",
        );
        const lastName =
          input.member.lastName === undefined
            ? jsonString(currentProfile, "lastName")
            : input.member.lastName === null
              ? null
              : required(input.member.lastName, "Last name");
        if (existing.kind === "adult" && !lastName) {
          throw new FlorenceStoreConflict("An adult needs a last name");
        }
        const profile = {
          ...applyEditableMemberProfilePatch(existing.profile, profilePatch),
          firstName,
          ...(lastName ? { lastName } : {}),
          relationship: existing.kind === "child" ? "Child" : defaultStoredRelationship(existing),
        };
        if (!lastName) delete (profile as Record<string, JsonValue>).lastName;
        const [updated] = await sql<PersonRow[]>`
          update people set
            display_name=${memberDisplayName(firstName, lastName)},
            profile=${sql.json(profile)},updated_at=${occurredAt}
          where id=${existing.id} returning *
        `;
        if (existing.kind === "adult" && input.member.lastName !== undefined) {
          await updateHouseholdNameFromLockedAdults(sql, input.householdId, occurredAt);
        }
        return updated;
      }
      if (input.member.operation !== "create") {
        throw new FlorenceStoreConflict("That family member no longer exists");
      }
      if (Object.hasOwn(input.member.profile ?? {}, "postalCode")) {
        throw new FlorenceStoreUnauthorized("Home ZIP belongs to the founding parent profile");
      }
      const firstName = required(input.member.firstName, "First name");
      const lastName = input.member.lastName ? required(input.member.lastName, "Last name") : null;
      const profile = {
        ...applyEditableMemberProfilePatch({}, input.member.profile ?? {}),
        firstName,
        ...(lastName ? { lastName } : {}),
        relationship: "Child",
      };
      const [inserted] = await sql<PersonRow[]>`
        insert into people (id,household_id,kind,role,adult_slot,display_name,status,profile,created_at,updated_at)
        values (${input.memberId},${input.householdId},'child','dependent',null,
          ${memberDisplayName(firstName, lastName)},'represented',${sql.json(profile)},${occurredAt},${occurredAt})
        returning *
      `;
      return inserted;
    });
    if (!row) throw new Error("The family member was not saved");
    return personRecord(row);
  }

  async savePreferences(input: {
    householdId: string;
    adultId: string;
    preferences: JsonObject;
  }): Promise<HouseholdRecord> {
    const publicPatch = validatedPublicPreferencePatch(input.preferences);
    const now = new Date();
    await this.#sql.begin(async (sql) => {
      const [adult] = await sql<PersonRow[]>`
        select * from people where household_id=${input.householdId} and id=${input.adultId}
          and kind='adult' and status='verified' for update
      `;
      if (!adult) throw new FlorenceStoreUnauthorized();
      const preferences = { ...jsonRecord(adult.preferences), ...publicPatch };
      await sql`
        update people set preferences=${sql.json(preferences)},updated_at=${now}
        where id=${adult.id}
      `;
      await reconcileProactiveConsentState(sql, input.householdId, input.adultId, preferences, now);
      await reconcilePrivateConflictSharingState(
        sql,
        input.householdId,
        input.adultId,
        preferences.privateConflictBusySharingEnabled === true,
      );
    });
    return requiredHousehold(
      await this.readHousehold({ householdId: input.householdId, viewerAdultId: input.adultId }),
    );
  }

  async correctFact(input: {
    householdId: string;
    adultId: string;
    factId: string;
    statement: string;
  }): Promise<HouseholdRecord> {
    const statement = required(input.statement, "Fact statement");
    const correctedAt = new Date();
    const sourceId = deterministicUuid(
      `vault-correction\0${input.householdId}\0${input.factId}\0${correctedAt.toISOString()}`,
    );
    await this.#sql.begin(async (sql) => {
      const [adult] = await sql<{ id: string }[]>`
        select id from people where household_id=${input.householdId} and id=${input.adultId}
          and kind='adult' and status='verified' for share
      `;
      if (!adult) throw new FlorenceStoreUnauthorized();
      const [updated] = await sql<{ id: string; visibility: Visibility; owner_adult_id: string | null }[]>`
        update facts set value=${sql.json({ statement })},corrected_at=${correctedAt},updated_at=${correctedAt}
        where id=${input.factId} and household_id=${input.householdId}
          and (visibility='household'
            or (visibility='private' and owner_adult_id=${input.adultId}))
        returning id,visibility,owner_adult_id
      `;
      if (!updated) throw new FlorenceStoreUnauthorized("That fact is not visible to this adult");
      await sql`
        insert into sources (
          id,household_id,kind,visibility,owner_adult_id,external_key,label,metadata,occurred_at
        ) values (${sourceId},${input.householdId},'web',${updated.visibility},${updated.owner_adult_id},
          ${`vault-correction:${input.factId}:${correctedAt.toISOString()}`},'Corrected in Vault',
          ${sql.json({ kind: "vault_correction" })},${correctedAt})
      `;
      await sql`delete from fact_sources where fact_id=${input.factId}`;
      await sql`insert into fact_sources (fact_id,source_id) values (${input.factId},${sourceId})`;
    });
    return requiredHousehold(
      await this.readHousehold({ householdId: input.householdId, viewerAdultId: input.adultId }),
    );
  }

  async deleteFact(input: {
    householdId: string;
    adultId: string;
    factId: string;
  }): Promise<HouseholdRecord> {
    await this.#sql.begin(async (sql) => {
      const [adult] = await sql<{ id: string }[]>`
        select id from people where household_id=${input.householdId} and id=${input.adultId}
          and kind='adult' and status='verified' for share
      `;
      if (!adult) throw new FlorenceStoreUnauthorized();
      const deleted = await sql`
        delete from facts where id=${input.factId} and household_id=${input.householdId}
          and (visibility='household'
            or (visibility='private' and owner_adult_id=${input.adultId}))
        returning id
      `;
      if (deleted.length !== 1) {
        throw new FlorenceStoreUnauthorized("That fact is not visible to this adult");
      }
    });
    return requiredHousehold(
      await this.readHousehold({ householdId: input.householdId, viewerAdultId: input.adultId }),
    );
  }

  async readUnboundPartnerInvitation(input: {
    providerConversationId: string;
    identitySubjectDigest: string;
    now?: string;
  }): Promise<UnboundPartnerInvitation | null> {
    const providerConversationId = required(
      input.providerConversationId,
      "Partner invitation conversation ID",
    );
    assertDigest(input.identitySubjectDigest, "Partner invitation identity");
    const checkedAt = instant(input.now ?? new Date().toISOString());
    const [row] = await this.#sql<
      {
        adult_id: string;
        household_id: string;
        founder_adult_id: string | null;
        messages_address: string | null;
        invitation_conversation_id: string;
        invitation_identity_digest: string;
        invitation_message_id: string;
        invitation_issued_at: Date;
        setup_issued_at: Date | null;
        link_issued: boolean;
        state: "awaiting_reply" | "issued" | "expired" | "declined";
      }[]
    >`
      select partner.id as adult_id,partner.household_id,
        approval.sender_adult_id as founder_adult_id,partner.messages_address,
        partner.invitation_conversation_id,partner.invitation_identity_digest,
        partner.invitation_message_id,partner.invitation_issued_at,
        case when partner.invitation_digest is not null
          then partner.invitation_retry_at else null end as setup_issued_at,
        (partner.invitation_digest is not null and partner.invitation_retry_at is null) as link_issued,
        case
          when partner.invitation_consumed_at is not null then 'declined'
          when partner.invitation_digest is not null
            and partner.invitation_expires_at<=${checkedAt} then 'expired'
          when partner.invitation_retry_at is not null then 'awaiting_reply'
          when partner.invitation_digest is null then 'awaiting_reply'
          else 'issued'
        end as state
      from people partner
      left join messages approval on approval.source_id=partner.invitation_approval_source_id
      left join linq_channels channel on channel.id=approval.channel_id
      left join people founder on founder.id=approval.sender_adult_id
        and founder.household_id=partner.household_id
      where partner.kind='adult' and partner.role='steward' and partner.adult_slot=2
        and partner.status='planned' and partner.identity_subject_digest is null
        and partner.invitation_conversation_id=${providerConversationId}
        and partner.invitation_identity_digest=${input.identitySubjectDigest}
        and partner.invitation_message_id is not null and partner.invitation_issued_at is not null
        and (
          (
            partner.invitation_consumed_at is null and partner.messages_address is not null
            and partner.invitation_approval_source_id is not null
            and partner.invitation_approved_at is not null
            and (partner.invitation_digest is null or partner.invitation_expires_at is not null)
            and founder.kind='adult' and founder.role='steward' and founder.adult_slot=1
            and founder.status='verified' and founder.identity_subject_digest is not null
            and channel.household_id=partner.household_id and channel.audience='private'
            and channel.adult_one_id=founder.id and channel.adult_two_id is null
            and channel.identity_one_digest=founder.identity_subject_digest
            and channel.revoked_at is null and channel.stopped_at is null
            and approval.direction='inbound' and approval.sender_adult_id=founder.id
            and approval.status='handled'
          )
          or
          (
            partner.invitation_digest is null and partner.invitation_expires_at is null
            and partner.invitation_consumed_at is not null and partner.messages_address is null
            and partner.invitation_approval_source_id is null
            and partner.invitation_approved_at is null
            and partner.invitation_retry_at is null and partner.invitation_last_error is null
          )
        )
      limit 1
    `;
    if (!row) return null;
    if (row.state === "declined") {
      const expiryNoticeSourceId = deterministicUuid(
        `partner-invitation-expired\0${row.adult_id}\0${row.invitation_message_id}`,
      );
      const [expiryNotice] = await this.#sql<{ source_id: string; link_issued: boolean }[]>`
        select source_id,(text like '%setup link expired%') as link_issued
        from messages where source_id=${expiryNoticeSourceId}
          and direction='outbound'
          and idempotency_key like ${`partner-invitation-expired:${expiryNoticeSourceId}:h:%`}
      `;
      if (expiryNotice) {
        return {
          adultId: row.adult_id,
          state: "expired",
          linkIssued: expiryNotice.link_issued,
        };
      }
    }
    if (row.state === "expired") {
      return { adultId: row.adult_id, state: row.state, linkIssued: row.link_issued };
    }
    if (row.state === "declined") {
      return { adultId: row.adult_id, state: row.state };
    }
    return {
      adultId: row.adult_id,
      state: row.state,
      householdId: row.household_id,
      founderAdultId: required(row.founder_adult_id ?? "", "Partner invitation founder"),
      messagesAddress: required(row.messages_address ?? "", "Partner invitation address"),
      providerConversationId: row.invitation_conversation_id,
      identitySubjectDigest: row.invitation_identity_digest,
      initialProviderMessageId: row.invitation_message_id,
      handshakeAt: row.invitation_issued_at.toISOString(),
      setupIssuedAt: row.setup_issued_at?.toISOString() ?? null,
    };
  }

  async classifyUnboundMessagesReservation(input: {
    messagesAddress: string;
    providerConversationId: string;
    identitySubjectDigest: string;
  }): Promise<"pending_partner" | "claimed" | "none"> {
    const messagesAddress = required(input.messagesAddress, "Observed Messages address");
    if (!/^\+[1-9]\d{7,14}$/.test(messagesAddress)) {
      throw new FlorenceStoreConflict("Observed Messages address must be E.164");
    }
    const providerConversationId = required(input.providerConversationId, "Observed Linq conversation ID");
    assertDigest(input.identitySubjectDigest, "Observed Messages identity");
    const [reservation] = await this.#sql<{ id: string }[]>`
      select partner.id
      from people partner
      join messages approval on approval.source_id=partner.invitation_approval_source_id
      join linq_channels channel on channel.id=approval.channel_id
      join people founder on founder.id=approval.sender_adult_id
        and founder.household_id=partner.household_id
      where partner.kind='adult' and partner.role='steward' and partner.adult_slot=2
        and partner.status='planned' and partner.identity_subject_digest is null
        and partner.profile->>'phoneNumber'=${messagesAddress}
        and partner.invitation_consumed_at is null
        and partner.invitation_approval_source_id is not null
        and partner.invitation_approved_at is not null
        and founder.kind='adult' and founder.role='steward' and founder.adult_slot=1
        and founder.status='verified' and founder.identity_subject_digest is not null
        and channel.household_id=partner.household_id and channel.audience='private'
        and channel.adult_one_id=founder.id and channel.adult_two_id is null
        and channel.identity_one_digest=founder.identity_subject_digest
        and channel.revoked_at is null and channel.stopped_at is null
        and approval.direction='inbound' and approval.sender_adult_id=founder.id
        and approval.status='handled'
        and (
          (
            partner.invitation_digest is null and partner.invitation_expires_at is null
            and partner.messages_address is null and partner.invitation_conversation_id is null
            and partner.invitation_identity_digest is null and partner.invitation_message_id is null
            and partner.invitation_issued_at is null and partner.invitation_retry_at is not null
          )
          or
          (
            partner.messages_address=${messagesAddress}
            and partner.invitation_conversation_id is not null
            and partner.invitation_identity_digest is not null
            and partner.invitation_message_id is not null and partner.invitation_issued_at is not null
            and (partner.invitation_digest is null or partner.invitation_expires_at is not null)
          )
        )
      limit 1
    `;
    if (reservation) return "pending_partner";
    const [claim] = await this.#sql<{ id: string }[]>`
      select id from people where
        identity_subject_digest=${input.identitySubjectDigest}
        or messages_address=${messagesAddress}
        or invitation_conversation_id=${providerConversationId}
        or invitation_identity_digest=${input.identitySubjectDigest}
        or (kind='adult' and adult_slot=2 and status='planned'
          and profile->>'phoneNumber'=${messagesAddress})
      union all
      select id from linq_channels where provider_conversation_id=${providerConversationId}
        and revoked_at is null
      limit 1
    `;
    return claim ? "claimed" : "none";
  }

  async declinePendingPartnerReservation(input: {
    messagesAddress: string;
    providerConversationId: string;
    identitySubjectDigest: string;
    occurredAt: string;
  }): Promise<boolean> {
    const messagesAddress = required(input.messagesAddress, "Observed Messages address");
    if (!/^\+[1-9]\d{7,14}$/.test(messagesAddress)) {
      throw new FlorenceStoreConflict("Observed Messages address must be E.164");
    }
    required(input.providerConversationId, "Observed Linq conversation ID");
    assertDigest(input.identitySubjectDigest, "Observed Messages identity");
    const occurredAt = instant(input.occurredAt);
    const latestEligibleApproval = new Date(occurredAt.getTime() + LINQ_RECEIPT_CLOCK_SKEW_MS);
    return this.#sql.begin(async (sql) => {
      const [invitation] = await sql<
        {
          adult_id: string;
          household_id: string;
          first_name: string;
          approval_source_id: string;
          founder_adult_id: string;
          founder_channel_id: string;
        }[]
      >`
        select partner.id as adult_id,partner.household_id,
          partner.profile->>'firstName' as first_name,
          partner.invitation_approval_source_id as approval_source_id,
          founder.id as founder_adult_id,channel.id as founder_channel_id
        from people partner
        join messages approval on approval.source_id=partner.invitation_approval_source_id
        join linq_channels channel on channel.id=approval.channel_id
        join people founder on founder.id=approval.sender_adult_id
          and founder.household_id=partner.household_id
        where partner.kind='adult' and partner.role='steward' and partner.adult_slot=2
          and partner.status='planned' and partner.identity_subject_digest is null
          and partner.profile->>'phoneNumber'=${messagesAddress}
          and partner.invitation_digest is null and partner.invitation_expires_at is null
          and partner.invitation_consumed_at is null and partner.messages_address is null
          and partner.invitation_conversation_id is null
          and partner.invitation_identity_digest is null
          and partner.invitation_message_id is null and partner.invitation_issued_at is null
          and partner.invitation_approval_source_id is not null
          and partner.invitation_approved_at is not null and partner.invitation_retry_at is not null
          and partner.invitation_approved_at<=${latestEligibleApproval}
          and founder.kind='adult' and founder.role='steward' and founder.adult_slot=1
          and founder.status='verified' and founder.identity_subject_digest is not null
          and channel.household_id=partner.household_id and channel.audience='private'
          and channel.adult_one_id=founder.id and channel.adult_two_id is null
          and channel.identity_one_digest=founder.identity_subject_digest
          and channel.revoked_at is null and channel.stopped_at is null
          and approval.direction='inbound' and approval.sender_adult_id=founder.id
          and approval.status='handled'
        limit 1 for update of partner
      `;
      if (!invitation) return false;
      const stopped = await sql`
        update people set invitation_consumed_at=${occurredAt},
          invitation_approval_source_id=null,invitation_approved_at=null,
          invitation_retry_at=null,invitation_last_error=null,updated_at=${occurredAt}
        where id=${invitation.adult_id} and invitation_consumed_at is null
          and invitation_issued_at is null
        returning id
      `;
      if (stopped.length !== 1) return false;
      await stagePartnerInvitationTerminalNotice(sql, {
        invitation,
        reason: "declined",
        linkIssued: false,
        occurredAt,
        stableKey: invitation.approval_source_id,
      });
      return true;
    });
  }

  async declinePartnerInvitation(input: {
    adultId: string;
    providerConversationId: string;
    identitySubjectDigest: string;
    occurredAt: string;
  }): Promise<boolean> {
    assertUuid(input.adultId, "Invited partner adult ID");
    const providerConversationId = required(
      input.providerConversationId,
      "Partner invitation conversation ID",
    );
    assertDigest(input.identitySubjectDigest, "Partner invitation identity");
    const occurredAt = instant(input.occurredAt);
    return this.#sql.begin(async (sql) => {
      const [invitation] = await sql<IssuedPartnerInvitationRow[]>`
        select partner.id as adult_id,partner.household_id,
          partner.profile->>'firstName' as first_name,
          partner.invitation_message_id,
          (partner.invitation_digest is not null and partner.invitation_retry_at is null) as link_issued,
          founder.id as founder_adult_id,
          channel.id as founder_channel_id
        from people partner
        join messages approval on approval.source_id=partner.invitation_approval_source_id
        join linq_channels channel on channel.id=approval.channel_id
        join people founder on founder.id=approval.sender_adult_id
          and founder.household_id=partner.household_id
        where partner.id=${input.adultId} and partner.kind='adult' and partner.role='steward'
          and partner.adult_slot=2 and partner.status='planned'
          and partner.identity_subject_digest is null
          and partner.invitation_conversation_id=${providerConversationId}
          and partner.invitation_identity_digest=${input.identitySubjectDigest}
          and partner.invitation_consumed_at is null and partner.messages_address is not null
          and partner.invitation_message_id is not null and partner.invitation_issued_at is not null
          and (partner.invitation_digest is null or partner.invitation_expires_at is not null)
          and partner.invitation_approval_source_id is not null
          and partner.invitation_approved_at is not null
          and founder.kind='adult' and founder.role='steward' and founder.adult_slot=1
          and founder.status='verified' and founder.identity_subject_digest is not null
          and channel.household_id=partner.household_id and channel.audience='private'
          and channel.adult_one_id=founder.id and channel.adult_two_id is null
          and channel.identity_one_digest=founder.identity_subject_digest
          and channel.revoked_at is null and channel.stopped_at is null
          and approval.direction='inbound' and approval.sender_adult_id=founder.id
          and nullif(partner.profile->>'firstName','') is not null
        for update of partner
      `;
      if (!invitation) return false;
      return terminalizeIssuedPartnerInvitation(sql, invitation, occurredAt, "declined");
    });
  }

  async expirePartnerInvitations(input: { now: string }): Promise<number> {
    const expiredAt = instant(input.now);
    return this.#sql.begin(async (sql) => {
      const invitations = await sql<IssuedPartnerInvitationRow[]>`
        select partner.id as adult_id,partner.household_id,
          partner.profile->>'firstName' as first_name,
          partner.invitation_message_id,
          (partner.invitation_digest is not null and partner.invitation_retry_at is null) as link_issued,
          founder.id as founder_adult_id,
          channel.id as founder_channel_id
        from people partner
        join messages approval on approval.source_id=partner.invitation_approval_source_id
        join linq_channels channel on channel.id=approval.channel_id
        join people founder on founder.id=approval.sender_adult_id
          and founder.household_id=partner.household_id
        where partner.kind='adult' and partner.role='steward' and partner.adult_slot=2
          and partner.status='planned' and partner.identity_subject_digest is null
          and partner.invitation_digest is not null and partner.invitation_expires_at is not null
          and partner.invitation_expires_at<=${expiredAt}
          and partner.invitation_consumed_at is null and partner.messages_address is not null
          and partner.invitation_conversation_id is not null
          and partner.invitation_identity_digest is not null
          and partner.invitation_message_id is not null and partner.invitation_issued_at is not null
          and partner.invitation_approval_source_id is not null
          and partner.invitation_approved_at is not null
          and founder.kind='adult' and founder.role='steward' and founder.adult_slot=1
          and founder.status='verified' and founder.identity_subject_digest is not null
          and channel.household_id=partner.household_id and channel.audience='private'
          and channel.adult_one_id=founder.id and channel.adult_two_id is null
          and channel.identity_one_digest=founder.identity_subject_digest
          and channel.revoked_at is null and channel.stopped_at is null
          and approval.direction='inbound' and approval.sender_adult_id=founder.id
          and nullif(partner.profile->>'firstName','') is not null
        order by partner.invitation_expires_at,partner.id
        for update of partner
      `;
      let expired = 0;
      for (const invitation of invitations) {
        if (await terminalizeIssuedPartnerInvitation(sql, invitation, expiredAt, "expired")) {
          expired += 1;
        }
      }
      return expired;
    });
  }

  async failPartnerInvitationPermanently(input: {
    adultId: string;
    occurredAt: string;
    delivery?: {
      providerConversationId: string;
      identitySubjectDigest: string;
      providerMessageId: string;
      issuedAt: string;
    };
  }): Promise<void> {
    assertUuid(input.adultId, "Invited partner adult ID");
    const occurredAt = instant(input.occurredAt);
    const suppliedDelivery = input.delivery
      ? {
          providerConversationId: required(
            input.delivery.providerConversationId,
            "Failed partner invitation conversation ID",
          ),
          identitySubjectDigest: input.delivery.identitySubjectDigest,
          providerMessageId: required(
            input.delivery.providerMessageId,
            "Failed partner invitation message ID",
          ),
          issuedAt: instant(input.delivery.issuedAt),
        }
      : null;
    if (suppliedDelivery) {
      assertDigest(suppliedDelivery.identitySubjectDigest, "Failed partner invitation identity");
      if (suppliedDelivery.issuedAt > occurredAt) {
        throw new FlorenceStoreConflict("A failed partner invitation cannot finish before its chat exists");
      }
    }
    await this.#sql.begin(async (sql) => {
      const [invitation] = await sql<
        {
          adult_id: string;
          household_id: string;
          first_name: string;
          approval_source_id: string;
          founder_adult_id: string;
          founder_channel_id: string;
          messages_address: string | null;
          invitation_conversation_id: string | null;
          invitation_identity_digest: string | null;
          invitation_message_id: string | null;
          invitation_issued_at: Date | null;
          invitation_digest: string | null;
          invitation_retry_at: Date | null;
        }[]
      >`
        select partner.id as adult_id,partner.household_id,
          partner.profile->>'firstName' as first_name,
          partner.invitation_approval_source_id as approval_source_id,
          founder.id as founder_adult_id,channel.id as founder_channel_id,
          partner.messages_address,partner.invitation_conversation_id,
          partner.invitation_identity_digest,partner.invitation_message_id,
          partner.invitation_issued_at,partner.invitation_digest,partner.invitation_retry_at
        from people partner
        join messages approval on approval.source_id=partner.invitation_approval_source_id
        join linq_channels channel on channel.id=approval.channel_id
        join people founder on founder.id=approval.sender_adult_id
          and founder.household_id=partner.household_id
        where partner.id=${input.adultId} and partner.kind='adult' and partner.role='steward'
          and partner.adult_slot=2 and partner.status='planned'
          and partner.identity_subject_digest is null
          and partner.invitation_consumed_at is null
          and (
            (partner.invitation_digest is null and partner.invitation_expires_at is null
              and partner.messages_address is null
              and partner.invitation_conversation_id is null
              and partner.invitation_identity_digest is null
              and partner.invitation_message_id is null and partner.invitation_issued_at is null)
            or
            (partner.messages_address is not null
              and partner.invitation_conversation_id is not null
              and partner.invitation_identity_digest is not null
              and partner.invitation_message_id is not null and partner.invitation_issued_at is not null
              and (partner.invitation_digest is null
                or (partner.invitation_expires_at is not null
                  and partner.invitation_retry_at is not null)))
          )
          and partner.invitation_approval_source_id is not null
          and partner.invitation_approved_at is not null
          and founder.kind='adult' and founder.role='steward' and founder.adult_slot=1
          and founder.status='verified' and founder.identity_subject_digest is not null
          and channel.household_id=partner.household_id and channel.audience='private'
          and channel.adult_one_id=founder.id and channel.adult_two_id is null
          and channel.identity_one_digest=founder.identity_subject_digest
          and channel.revoked_at is null and channel.stopped_at is null
          and approval.direction='inbound' and approval.sender_adult_id=founder.id
          and nullif(partner.profile->>'firstName','') is not null
        for update of partner
      `;
      if (!invitation) {
        throw new FlorenceStoreConflict("The partner invitation is no longer pending");
      }
      if (suppliedDelivery) {
        if (
          invitation.invitation_issued_at !== null &&
          (invitation.messages_address === null ||
            invitation.invitation_conversation_id !== suppliedDelivery.providerConversationId ||
            invitation.invitation_identity_digest !== suppliedDelivery.identitySubjectDigest ||
            invitation.invitation_message_id !== suppliedDelivery.providerMessageId ||
            invitation.invitation_issued_at.getTime() !== suppliedDelivery.issuedAt.getTime())
        ) {
          throw new FlorenceStoreConflict("The failed partner conversation does not match its handshake");
        }
        const [collision] = await sql<{ id: string }[]>`
          select id from people where id<>${invitation.adult_id}
            and invitation_conversation_id=${suppliedDelivery.providerConversationId}
          union all
          select id from linq_channels
          where provider_conversation_id=${suppliedDelivery.providerConversationId}
            and revoked_at is null
          limit 1
        `;
        if (collision) {
          throw new FlorenceStoreConflict("The failed partner conversation is already bound elsewhere");
        }
      }
      const terminalized = await sql`
        update people set invitation_digest=null,invitation_expires_at=null,
          invitation_consumed_at=${occurredAt},messages_address=null,
          invitation_conversation_id=${suppliedDelivery?.providerConversationId ?? invitation.invitation_conversation_id},
          invitation_identity_digest=${suppliedDelivery?.identitySubjectDigest ?? invitation.invitation_identity_digest},
          invitation_message_id=${suppliedDelivery?.providerMessageId ?? invitation.invitation_message_id},
          invitation_issued_at=${suppliedDelivery?.issuedAt ?? invitation.invitation_issued_at},
          invitation_approval_source_id=null,invitation_approved_at=null,
          invitation_retry_at=null,invitation_last_error=null,updated_at=${occurredAt}
        where id=${invitation.adult_id} and invitation_approval_source_id=${invitation.approval_source_id}
          and invitation_consumed_at is null
          and (invitation_digest is null or invitation_retry_at is not null)
        returning id
      `;
      if (terminalized.length !== 1) {
        throw new FlorenceStoreConflict("The partner invitation changed before it could be stopped");
      }
      await stagePartnerInvitationTerminalNotice(sql, {
        invitation,
        reason: "delivery_failed",
        linkIssued: false,
        occurredAt,
        stableKey: invitation.approval_source_id,
      });
    });
  }

  async bindPartnerInvitationHandshake(input: {
    householdId: string;
    actorAdultId: string;
    adultId: string;
    approvalSourceId: string;
    messagesAddress: string;
    providerConversationId: string;
    identitySubjectDigest: string;
    providerMessageId: string;
    occurredAt: string;
    retryAt: string | null;
    retryError: string | null;
  }): Promise<FamilyMemberRecord> {
    assertUuid(input.householdId, "Partner invitation household ID");
    assertUuid(input.actorAdultId, "Partner invitation founder ID");
    assertUuid(input.adultId, "Invited partner adult ID");
    assertUuid(input.approvalSourceId, "Partner invitation approval source");
    assertDigest(input.identitySubjectDigest, "Invited Messages identity");
    const messagesAddress = required(input.messagesAddress, "Invited Messages address");
    if (!/^\+[1-9]\d{7,14}$/.test(messagesAddress)) {
      throw new FlorenceStoreConflict("Invited Messages address must be E.164");
    }
    const providerConversationId = required(input.providerConversationId, "Invited Linq conversation ID");
    const providerMessageId = required(input.providerMessageId, "Partner handshake message ID");
    const occurredAt = instant(input.occurredAt);
    const retryAt = input.retryAt === null ? null : instant(input.retryAt);
    const retryError = input.retryError === null ? null : bounded(input.retryError, 500);
    if ((retryAt === null) !== (retryError === null) || (retryAt !== null && retryAt < occurredAt)) {
      throw new FlorenceStoreConflict("A pending partner handshake needs a valid retry state");
    }
    const expiresAt = new Date(occurredAt.getTime() + LEGACY_PARTNER_HANDSHAKE_WINDOW_MS);
    const partner = await this.#sql.begin(async (sql) => {
      const [row] = await sql<PersonRow[]>`
        select * from people where household_id=${input.householdId} and id=${input.adultId}
          and kind='adult' and role='steward' and adult_slot=2 for update
      `;
      if (row?.status !== "planned" || row.identity_subject_digest !== null) {
        throw new FlorenceStoreConflict("The invited partner is not waiting for Messages setup");
      }
      if (jsonRecord(row.profile).phoneNumber !== messagesAddress) {
        throw new FlorenceStoreConflict("The invitation address does not match the planned partner");
      }
      if (
        row.invitation_consumed_at !== null &&
        row.invitation_approval_source_id === null &&
        row.invitation_expires_at === null &&
        row.messages_address === null &&
        row.invitation_conversation_id === null &&
        row.invitation_identity_digest === null &&
        row.invitation_message_id === null &&
        row.invitation_issued_at === null
      ) {
        return row;
      }
      const [approval] = await sql<
        {
          source_id: string;
          sender_adult_id: string;
          status: "received" | "handled";
        }[]
      >`
        select approval.source_id,approval.sender_adult_id,approval.status
        from messages approval
        join linq_channels channel on channel.id=approval.channel_id
        join people founder on founder.id=approval.sender_adult_id
          and founder.household_id=${input.householdId}
        where approval.source_id=${input.approvalSourceId}
          and approval.direction='inbound' and approval.move_kind in ('message','reply')
          and founder.id=${input.actorAdultId} and founder.kind='adult'
          and founder.role='steward' and founder.adult_slot=1 and founder.status='verified'
          and founder.identity_subject_digest is not null
          and channel.household_id=${input.householdId} and channel.audience='private'
          and channel.adult_one_id=founder.id and channel.adult_two_id is null
          and channel.identity_one_digest=founder.identity_subject_digest
          and channel.revoked_at is null and channel.stopped_at is null
        for share of approval,channel,founder
      `;
      if (
        !approval ||
        approval.sender_adult_id !== input.actorAdultId ||
        approval.status !== "handled" ||
        row.invitation_approval_source_id !== input.approvalSourceId ||
        row.invitation_approved_at === null
      ) {
        throw new FlorenceStoreUnauthorized(
          "The founding adult has not approved this exact partner invitation",
        );
      }
      if (occurredAt.getTime() < row.invitation_approved_at.getTime() - LINQ_RECEIPT_CLOCK_SKEW_MS) {
        throw new FlorenceStoreConflict("The partner handshake cannot predate its approval");
      }
      if (row.invitation_issued_at !== null) {
        if (
          row.messages_address === messagesAddress &&
          row.invitation_conversation_id === providerConversationId &&
          row.invitation_identity_digest === input.identitySubjectDigest &&
          row.invitation_message_id === providerMessageId &&
          row.invitation_expires_at !== null &&
          row.invitation_consumed_at === null &&
          row.invitation_digest !== null
        ) {
          return row;
        }
        if (
          row.invitation_digest !== null ||
          row.invitation_consumed_at !== null ||
          row.messages_address !== messagesAddress ||
          row.invitation_conversation_id !== providerConversationId ||
          row.invitation_identity_digest !== input.identitySubjectDigest ||
          row.invitation_message_id !== providerMessageId ||
          occurredAt.getTime() < row.invitation_issued_at.getTime() - LINQ_RECEIPT_CLOCK_SKEW_MS
        ) {
          throw new FlorenceStoreConflict("The partner handshake was already bound differently");
        }
        const [reconciled] = await sql<PersonRow[]>`
          update people set invitation_expires_at=coalesce(invitation_expires_at,${expiresAt}),
            invitation_retry_at=${retryAt},invitation_last_error=${retryError},
            updated_at=greatest(updated_at,${retryAt ?? occurredAt})
          where id=${row.id} and invitation_digest is null
            and invitation_conversation_id=${providerConversationId}
            and invitation_identity_digest=${input.identitySubjectDigest}
            and invitation_message_id=${providerMessageId}
            and invitation_issued_at=${row.invitation_issued_at}
          returning *
        `;
        if (!reconciled) {
          throw new FlorenceStoreConflict("The partner handshake changed while delivery was reconciled");
        }
        return reconciled;
      }
      if (
        row.invitation_digest !== null ||
        row.invitation_expires_at !== null ||
        row.invitation_consumed_at !== null ||
        row.messages_address !== null ||
        row.invitation_conversation_id !== null ||
        row.invitation_identity_digest !== null ||
        row.invitation_message_id !== null ||
        row.invitation_retry_at === null
      ) {
        throw new FlorenceStoreConflict("The planned partner has incomplete invitation state");
      }
      const [collision] = await sql<{ id: string }[]>`
        select p.id from people p where p.id<>${row.id} and (
          p.messages_address=${messagesAddress}
          or p.invitation_conversation_id=${providerConversationId}
          or p.invitation_message_id=${providerMessageId}
          or p.identity_subject_digest=${input.identitySubjectDigest}
          or p.invitation_identity_digest=${input.identitySubjectDigest}
        )
        union all
        select c.id from linq_channels c where c.provider_conversation_id=${providerConversationId}
          and c.revoked_at is null
        limit 1
      `;
      if (collision) {
        throw new FlorenceStoreConflict("The partner handshake is already bound elsewhere");
      }
      const [updated] = await sql<PersonRow[]>`
        -- Preserve the legacy timestamp shape so older rolling-deploy replicas can still
        -- recognize the bound chat. New readers ignore it until a signed link is stored.
        update people set invitation_digest=null,invitation_expires_at=${expiresAt},
          invitation_consumed_at=null,messages_address=${messagesAddress},
          invitation_conversation_id=${providerConversationId},
          invitation_identity_digest=${input.identitySubjectDigest},
          invitation_message_id=${providerMessageId},invitation_issued_at=${occurredAt},
          invitation_retry_at=${retryAt},invitation_last_error=${retryError},updated_at=${occurredAt}
        where id=${row.id} and invitation_approval_source_id=${input.approvalSourceId}
          and invitation_issued_at is null returning *
      `;
      if (!updated) {
        throw new FlorenceStoreConflict("The partner invitation changed before its chat was bound");
      }
      return updated;
    });
    return personRecord(partner);
  }

  async issueMessagesEnrollment(input: IssueMessagesEnrollmentInput): Promise<FamilyMemberRecord> {
    assertDigest(input.challengeDigest, "Messages enrollment challenge");
    assertDigest(input.identitySubjectDigest, "Invited Messages identity");
    const issuedAt = instant(input.issuedAt);
    const expiresAt = instant(input.expiresAt);
    if (expiresAt <= issuedAt)
      throw new FlorenceStoreConflict("A Messages invitation must expire after issue");
    const providerConversationId = required(input.providerConversationId, "Invited Linq conversation ID");
    const providerMessageId = required(input.providerMessageId, "Invited Linq message ID");
    const messagesAddress = required(input.messagesAddress, "Invited Messages address");
    if (!/^\+[1-9]\d{7,14}$/.test(messagesAddress)) {
      throw new FlorenceStoreConflict("Invited Messages address must be E.164");
    }
    const adult = await this.#sql.begin(async (sql) => {
      const [founder] = await sql<{ id: string }[]>`
        select id from people where household_id=${input.householdId} and id=${input.actorAdultId}
          and kind='adult' and role='steward' and adult_slot=1 and status='verified'
        for share
      `;
      if (!founder) {
        throw new FlorenceStoreUnauthorized("Only the verified founding adult can invite the partner");
      }
      const [row] = await sql<PersonRow[]>`
        select * from people where household_id=${input.householdId} and id=${input.adultId}
          and kind='adult' and role='steward' and adult_slot=2 for update
      `;
      if (row?.status !== "planned" || row.identity_subject_digest !== null) {
        throw new FlorenceStoreConflict("The invited partner is not waiting for Messages setup");
      }
      const plannedPhone = jsonRecord(row.profile).phoneNumber;
      if (plannedPhone !== messagesAddress) {
        throw new FlorenceStoreConflict("The invitation address does not match the planned partner");
      }
      if (row.invitation_digest !== null) {
        if (
          row.invitation_digest !== input.challengeDigest ||
          row.invitation_expires_at?.getTime() !== expiresAt.getTime() ||
          row.invitation_consumed_at !== null ||
          row.messages_address !== messagesAddress ||
          row.invitation_conversation_id !== providerConversationId ||
          row.invitation_identity_digest !== input.identitySubjectDigest ||
          row.invitation_message_id !== providerMessageId ||
          row.invitation_issued_at === null ||
          row.invitation_retry_at === null
        ) {
          throw new FlorenceStoreConflict("The partner invitation was already issued differently");
        }
        return row;
      }
      const [approval] = row.invitation_approval_source_id
        ? await sql<
            {
              source_id: string;
              sender_adult_id: string;
              channel_id: string;
              status: "received" | "handled";
            }[]
          >`
            select message.source_id,message.sender_adult_id,message.channel_id,message.status
            from messages message join linq_channels channel on channel.id=message.channel_id
            where message.source_id=${row.invitation_approval_source_id}
              and message.direction='inbound' and message.move_kind in ('message','reply')
              and channel.household_id=${input.householdId} and channel.audience='private'
              and channel.adult_one_id=${input.actorAdultId}
              and channel.revoked_at is null and channel.stopped_at is null
            for share of message,channel
          `
        : [];
      if (
        !approval ||
        approval.sender_adult_id !== input.actorAdultId ||
        approval.status !== "handled" ||
        row.invitation_approved_at === null
      ) {
        throw new FlorenceStoreUnauthorized(
          "The founding adult has not approved this exact partner invitation",
        );
      }
      if (
        row.invitation_digest !== null ||
        row.invitation_consumed_at !== null ||
        row.messages_address !== messagesAddress ||
        row.invitation_conversation_id !== providerConversationId ||
        row.invitation_identity_digest !== input.identitySubjectDigest ||
        row.invitation_message_id !== providerMessageId ||
        row.invitation_issued_at === null ||
        issuedAt < row.invitation_issued_at
      ) {
        throw new FlorenceStoreConflict("The planned partner is not awaiting a reply in this chat");
      }
      const [collision] = await sql<{ id: string }[]>`
        select p.id from people p where p.id<>${row.id} and (
          p.invitation_digest=${input.challengeDigest}
          or p.messages_address=${messagesAddress}
          or p.invitation_conversation_id=${providerConversationId}
          or p.invitation_message_id=${providerMessageId}
          or p.identity_subject_digest=${input.identitySubjectDigest}
          or p.invitation_identity_digest=${input.identitySubjectDigest}
        )
        union all
        select c.id from linq_channels c where c.provider_conversation_id=${providerConversationId}
          and c.revoked_at is null
        limit 1
      `;
      if (collision) {
        throw new FlorenceStoreConflict("The partner invitation is already bound elsewhere");
      }
      const [updated] = await sql<PersonRow[]>`
        update people set invitation_digest=${input.challengeDigest},invitation_expires_at=${expiresAt},
          invitation_consumed_at=null,invitation_retry_at=${issuedAt},invitation_last_error=null,
          updated_at=${issuedAt}
        where id=${row.id} and invitation_digest is null and invitation_consumed_at is null
          and messages_address=${messagesAddress}
          and invitation_conversation_id=${providerConversationId}
          and invitation_identity_digest=${input.identitySubjectDigest}
          and invitation_message_id=${row.invitation_message_id}
          and invitation_issued_at=${row.invitation_issued_at}
        returning *
      `;
      return updated;
    });
    if (!adult) throw new Error("The partner invitation was not stored");
    return personRecord(adult);
  }

  async confirmMessagesEnrollmentDelivery(input: {
    householdId: string;
    adultId: string;
    challengeDigest: string;
    providerConversationId: string;
    identitySubjectDigest: string;
    messagesAddress: string;
    providerMessageId: string;
    deliveredAt: string;
  }): Promise<FamilyMemberRecord> {
    assertUuid(input.householdId, "Partner invitation household ID");
    assertUuid(input.adultId, "Invited partner adult ID");
    assertDigest(input.challengeDigest, "Messages enrollment challenge");
    assertDigest(input.identitySubjectDigest, "Invited Messages identity");
    const providerConversationId = required(input.providerConversationId, "Invited Linq conversation ID");
    const providerMessageId = required(input.providerMessageId, "Invited Linq message ID");
    const messagesAddress = required(input.messagesAddress, "Invited Messages address");
    if (!/^\+[1-9]\d{7,14}$/.test(messagesAddress)) {
      throw new FlorenceStoreConflict("Invited Messages address must be E.164");
    }
    const deliveredAt = instant(input.deliveredAt);
    const adult = await this.#sql.begin(async (sql) => {
      const [row] = await sql<PersonRow[]>`
        select * from people where household_id=${input.householdId} and id=${input.adultId}
          and kind='adult' and role='steward' and adult_slot=2 for update
      `;
      if (
        !row ||
        row.invitation_digest !== input.challengeDigest ||
        row.invitation_expires_at === null ||
        row.messages_address !== messagesAddress ||
        row.invitation_conversation_id !== providerConversationId ||
        row.invitation_identity_digest !== input.identitySubjectDigest ||
        row.invitation_message_id === null ||
        row.invitation_issued_at === null
      ) {
        throw new FlorenceStoreConflict("The partner setup link is no longer awaiting delivery");
      }
      if (
        row.status === "verified" &&
        row.identity_subject_digest === input.identitySubjectDigest &&
        row.invitation_consumed_at !== null
      ) {
        return row;
      }
      if (
        row.status !== "planned" ||
        row.identity_subject_digest !== null ||
        row.invitation_consumed_at !== null ||
        deliveredAt.getTime() < row.invitation_issued_at.getTime() - LINQ_RECEIPT_CLOCK_SKEW_MS ||
        deliveredAt >= row.invitation_expires_at
      ) {
        throw new FlorenceStoreConflict("The partner setup delivery receipt is outside its invitation");
      }
      if (row.invitation_retry_at === null) {
        if (row.invitation_message_id !== providerMessageId) {
          throw new FlorenceStoreConflict("The partner setup link was already delivered differently");
        }
        return row;
      }
      const [collision] = await sql<{ id: string }[]>`
        select id from people where id<>${row.id} and invitation_message_id=${providerMessageId}
        limit 1
      `;
      if (collision) {
        throw new FlorenceStoreConflict("The partner setup delivery is already bound elsewhere");
      }
      const [updated] = await sql<PersonRow[]>`
        update people set invitation_message_id=${providerMessageId},invitation_issued_at=${deliveredAt},
          invitation_retry_at=null,invitation_last_error=null,updated_at=${deliveredAt}
        where id=${row.id} and invitation_digest=${input.challengeDigest}
          and invitation_retry_at is not null
        returning *
      `;
      if (!updated) {
        throw new FlorenceStoreConflict("The partner setup delivery changed before confirmation");
      }
      return updated;
    });
    return personRecord(adult);
  }

  async redeemMessagesEnrollment(
    input: RedeemMessagesEnrollmentInput,
  ): Promise<MessagesEnrollmentResult | null> {
    assertDigest(input.challengeDigest, "Messages enrollment challenge");
    assertDigest(input.identitySubjectDigest, "Messages identity");
    const firstName = required(input.firstName, "Partner first name");
    const lastName = required(input.lastName, "Partner last name");
    const displayName = memberDisplayName(firstName, lastName);
    const messagesAddress = required(input.messagesAddress, "Partner Messages address");
    if (!/^\+[1-9]\d{7,14}$/.test(messagesAddress)) {
      throw new FlorenceStoreConflict("Partner Messages address must be E.164");
    }
    const providerConversationId = required(input.providerConversationId, "Partner Linq conversation ID");
    const consentVersion = required(input.consentVersion, "Messages consent version");
    const occurredAt = instant(input.occurredAt);
    const consentedAt = instant(input.consentedAt);
    const guardianAttestedAt = instant(input.guardianAttestedAt);
    const proactiveUseAcceptedAt = instant(input.proactiveUseAcceptedAt);
    if (typeof input.privateConflictBusySharingEnabled !== "boolean") {
      throw new FlorenceStoreConflict("Private conflict sharing permission must be explicit");
    }
    if (consentedAt > occurredAt || guardianAttestedAt > occurredAt || proactiveUseAcceptedAt > occurredAt) {
      throw new FlorenceStoreConflict("Partner consent cannot occur after Messages enrollment");
    }
    return this.#sql.begin(async (sql) => {
      const [candidate] = await sql<{ household_id: string }[]>`
        select household_id from people where invitation_digest=${input.challengeDigest}
      `;
      if (!candidate) return null;
      await sql`select id from households where id=${candidate.household_id} for update`;
      const [adult] = await sql<PersonRow[]>`
        select * from people where invitation_digest=${input.challengeDigest}
          and household_id=${candidate.household_id} for update
      `;
      if (!adult) return null;
      const [existingChannel] = await sql<ChannelRow[]>`
        select * from linq_channels where household_id=${adult.household_id}
          and audience='private' and adult_one_id=${adult.id} and revoked_at is null
        limit 1 for update
      `;
      if (adult.identity_subject_digest !== null || adult.status === "verified") {
        const profile = jsonRecord(adult.profile);
        const preferences = jsonRecord(adult.preferences);
        if (
          adult.kind === "adult" &&
          adult.role === "steward" &&
          adult.adult_slot === 2 &&
          adult.status === "verified" &&
          adult.invitation_digest === input.challengeDigest &&
          adult.invitation_consumed_at !== null &&
          adult.invitation_conversation_id === providerConversationId &&
          adult.invitation_identity_digest === input.identitySubjectDigest &&
          adult.messages_address === messagesAddress &&
          adult.identity_subject_digest === input.identitySubjectDigest &&
          adult.consent_version === consentVersion &&
          adult.consented_at !== null &&
          adult.guardian_attested_at !== null &&
          adult.display_name === displayName &&
          profile.firstName === firstName &&
          profile.lastName === lastName &&
          preferences.proactiveUseAcceptedAt === proactiveUseAcceptedAt.toISOString() &&
          preferences.privateConflictBusySharingEnabled === input.privateConflictBusySharingEnabled &&
          existingChannel?.provider_conversation_id === providerConversationId &&
          existingChannel.identity_one_digest === input.identitySubjectDigest &&
          existingChannel.adult_two_id === null &&
          existingChannel.stopped_at === null
        ) {
          return {
            disposition: "duplicate" as const,
            householdId: adult.household_id,
            adultId: adult.id,
            channel: channelRecord(existingChannel),
          };
        }
        return null;
      }
      if (
        adult.kind !== "adult" ||
        adult.role !== "steward" ||
        adult.adult_slot !== 2 ||
        adult.status !== "planned" ||
        adult.invitation_digest !== input.challengeDigest ||
        adult.invitation_expires_at === null ||
        adult.invitation_expires_at <= occurredAt ||
        adult.invitation_consumed_at !== null ||
        adult.invitation_conversation_id !== providerConversationId ||
        adult.invitation_identity_digest !== input.identitySubjectDigest ||
        adult.messages_address !== messagesAddress ||
        existingChannel !== undefined
      ) {
        return null;
      }
      const [identityOwner] = await sql<{ id: string }[]>`
        select id from people where identity_subject_digest=${input.identitySubjectDigest} and id<>${adult.id}
      `;
      if (identityOwner) return null;
      const [conversationOwner] = await sql<{ id: string }[]>`
        select id from linq_channels where provider_conversation_id=${providerConversationId}
          and revoked_at is null limit 1
      `;
      if (conversationOwner) return null;
      const [updated] = await sql<PersonRow[]>`
        update people set status='verified',identity_subject_digest=${input.identitySubjectDigest},
          display_name=${displayName},consent_version=${consentVersion},consented_at=${consentedAt},
          guardian_attested_at=${guardianAttestedAt},invitation_consumed_at=${occurredAt},
          invitation_retry_at=null,invitation_last_error=null,
          profile=profile||${sql.json({ firstName, lastName, relationship: "Partner" })},
          preferences=preferences||${sql.json({
            proactiveUseAcceptedAt: proactiveUseAcceptedAt.toISOString(),
            privateConflictBusySharingEnabled: input.privateConflictBusySharingEnabled,
          })},updated_at=${occurredAt}
        where id=${adult.id} returning *
      `;
      if (!updated) throw new Error("The partner Messages enrollment was not stored");
      await updateHouseholdNameFromLockedAdults(sql, adult.household_id, occurredAt);
      const channelId = deterministicUuid(`linq-private\0${providerConversationId}`);
      const authorityDigest = digestStrings([adult.id, input.identitySubjectDigest]);
      const [channel] = await sql<ChannelRow[]>`
        insert into linq_channels (
          id,household_id,audience,provider_conversation_id,adult_one_id,identity_one_digest,
          authority_digest,bound_at
        ) values (${channelId},${adult.household_id},'private',${providerConversationId},
          ${adult.id},${input.identitySubjectDigest},${authorityDigest},${occurredAt})
        returning *
      `;
      if (!channel) throw new Error("The private Messages channel was not bound");
      return {
        disposition: "accepted" as const,
        householdId: adult.household_id,
        adultId: adult.id,
        channel: channelRecord(channel),
      };
    });
  }

  async completePartnerOnboarding(input: {
    householdId: string;
    adultId: string;
    completionText: string;
    occurredAt: string;
  }): Promise<string | null> {
    const occurredAt = instant(input.occurredAt);
    const completionText = required(input.completionText, "Partner setup completion message");
    return this.#sql.begin(async (sql) => {
      const [row] = await sql<PersonRow[]>`
        select * from people where household_id=${input.householdId} and id=${input.adultId}
          and kind='adult' and role='steward' and adult_slot=2 for update
      `;
      const preferences = row ? jsonRecord(row.preferences) : {};
      const proactiveUseAcceptedAt = preferences.proactiveUseAcceptedAt;
      if (
        row?.status !== "verified" ||
        row.identity_subject_digest === null ||
        row.guardian_attested_at === null ||
        typeof proactiveUseAcceptedAt !== "string"
      ) {
        throw new FlorenceStoreUnauthorized(
          "Partner onboarding requires verified Messages consent and proactive-use permission",
        );
      }
      instant(proactiveUseAcceptedAt);
      const channels = await sql<ChannelRow[]>`
        select * from linq_channels where household_id=${input.householdId}
          and audience='private' and adult_one_id=${input.adultId} and revoked_at is null
        for share
      `;
      const channel = channels[0];
      if (
        channels.length !== 1 ||
        !channel ||
        channel.stopped_at !== null ||
        channel.adult_two_id !== null ||
        channel.identity_one_digest !== row.identity_subject_digest
      ) {
        throw new FlorenceStoreUnauthorized(
          "Partner onboarding requires the exact active private Messages thread",
        );
      }
      const [google] = await sql<{ id: string }[]>`
        select id from google_connections where household_id=${input.householdId}
          and owner_adult_id=${input.adultId} and status='active'
        order by created_at,id limit 1 for share
      `;
      if (!google) {
        throw new FlorenceStoreUnauthorized("Partner onboarding requires an active Google connection");
      }
      const completionSourceId = deterministicUuid(
        `partner-onboarding-complete\0${input.householdId}\0${input.adultId}`,
      );
      const completedAt = jsonRecord(row.profile).onboardingCompletedAt;
      if (typeof completedAt === "string" && completedAt.length > 0) {
        instant(completedAt);
        const [staged] = await sql<{ source_id: string }[]>`
          select source_id from messages where source_id=${completionSourceId}
            and direction='outbound' and channel_id=${channel.id}
        `;
        return staged?.source_id ?? null;
      }
      const [adult] = await sql<PersonRow[]>`
        update people set profile=profile||${sql.json({
          onboardingCompletedAt: occurredAt.toISOString(),
        })},updated_at=${occurredAt}
        where id=${row.id} returning *
      `;
      if (!adult) throw new Error("Partner onboarding completion was not stored");
      await insertOutbound(sql, {
        sourceId: completionSourceId,
        idempotencyKey: `partner-onboarding-complete:${input.householdId}:${input.adultId}`,
        moveKind: "message",
        text: completionText,
        turnId: completionSourceId,
        turnPart: 0,
        notBefore: occurredAt.toISOString(),
        householdId: input.householdId,
        channelId: channel.id,
        visibility: "private",
        ownerAdultId: input.adultId,
        occurredAt,
      });
      return completionSourceId;
    });
  }

  async reconcileObservedFamilyGroup(input: {
    providerConversationId: string;
    audience: Audience;
    participantIdentityDigests: readonly string[];
    occurredAt: string;
  }): Promise<FamilyGroupObservationResult | null> {
    const providerConversationId = required(
      input.providerConversationId,
      "Observed family group Linq conversation ID",
    );
    const observed = sortedDigests(input.participantIdentityDigests);
    if (
      (input.audience !== "private" && input.audience !== "group") ||
      observed.length > 31 ||
      new Set(observed).size !== observed.length
    ) {
      throw new FlorenceStoreConflict("Observed family group membership is invalid");
    }
    const occurredAt = instant(input.occurredAt);
    return this.#sql.begin(async (sql) => {
      const [channel] = await sql<ChannelRow[]>`
        select * from linq_channels where provider_conversation_id=${providerConversationId}
          and audience='group' order by bound_at desc,id desc limit 1 for update
      `;
      if (!channel) return null;
      if (channel.revoked_at !== null) return "retired";
      if (input.audience === "group" && sameStrings(channelIdentityDigests(channel), observed)) {
        return "current";
      }

      await sql`
        update linq_channels set revoked_at=${occurredAt},stopped_at=coalesce(stopped_at,${occurredAt})
        where id=${channel.id} and revoked_at is null
      `;
      await sql`
        update messages set status='failed',retry_at=null,
          last_error='The family thread participants changed; this thread was retired'
        where channel_id=${channel.id} and direction='outbound' and status in ('pending','sending')
      `;
      const adults = await sql<
        (PersonRow & { private_channel_id: string | null; private_stopped_at: Date | null })[]
      >`
        select p.*,private_channel.id as private_channel_id,
               private_channel.stopped_at as private_stopped_at
        from people p left join linq_channels private_channel
          on private_channel.household_id=p.household_id and private_channel.audience='private'
            and private_channel.adult_one_id=p.id and private_channel.revoked_at is null
        where p.household_id=${channel.household_id} and p.kind='adult' and p.status='verified'
          and p.id in ${sql([channel.adult_one_id, channel.adult_two_id].filter(Boolean) as string[])}
        order by p.adult_slot,p.id for share of p
      `;
      const noticeText =
        "The people in our family thread changed, so I stopped using it. I’ll make a fresh thread with just the two of you.";
      for (const adult of adults) {
        if (!adult.private_channel_id || adult.private_stopped_at !== null) continue;
        const suffix = `${channel.id}:${adult.id}`;
        await insertOutbound(sql, {
          sourceId: deterministicUuid(`family-group-repair-notice\0${suffix}`),
          idempotencyKey: `family-group-repair-notice:${suffix}`,
          moveKind: "message",
          text: noticeText,
          turnId: deterministicUuid(`family-group-repair-notice-turn\0${suffix}`),
          turnPart: 0,
          notBefore: occurredAt.toISOString(),
          householdId: channel.household_id,
          channelId: adult.private_channel_id,
          visibility: "private",
          ownerAdultId: adult.id,
          occurredAt,
        });
      }
      return "mismatch";
    });
  }

  async readNextFamilyGroupCreation(
    _now: string = new Date().toISOString(),
  ): Promise<FamilyGroupCreationWork | null> {
    const households = await this.#sql<{ id: string }[]>`
      select h.id from households h
      where not exists (
        select 1 from linq_channels active
        where active.household_id=h.id and active.audience='group' and active.revoked_at is null
      )
      order by h.created_at,h.id
    `;
    for (const household of households) {
      const adults = await this.#sql<PersonRow[]>`
        select * from people where household_id=${household.id} and kind='adult'
          and status='verified' order by adult_slot,id
      `;
      if (
        adults.length !== 2 ||
        adults.some(
          (adult) =>
            adult.identity_subject_digest === null ||
            adult.messages_address === null ||
            (adult.adult_slot !== 1 && adult.adult_slot !== 2),
        )
      ) {
        continue;
      }
      const [previous] = await this.#sql<{ id: string }[]>`
        select id from linq_channels where household_id=${household.id} and audience='group'
        order by bound_at desc,id desc limit 1
      `;
      if (!previous) {
        if (adults.some((adult) => typeof jsonRecord(adult.profile).onboardingCompletedAt !== "string")) {
          continue;
        }
        const google = await this.#sql<{ owner_adult_id: string }[]>`
          select owner_adult_id from google_connections where household_id=${household.id}
            and status='active' and owner_adult_id in ${this.#sql(adults.map((adult) => adult.id))}
        `;
        if (new Set(google.map((connection) => connection.owner_adult_id)).size !== 2) continue;
      }
      const [first, second] = adults;
      if (
        !first?.messages_address ||
        !second?.messages_address ||
        !first.identity_subject_digest ||
        !second.identity_subject_digest
      ) {
        continue;
      }
      const createChatIdempotencyKey = await householdLinqIdempotencyKey(
        this.#sql,
        household.id,
        previous
          ? `family-group:${household.id}:replace:${previous.id}`
          : `family-group:${household.id}:initial`,
      );
      return {
        householdId: household.id,
        createChatIdempotencyKey,
        participantPhoneNumbers: [first.messages_address, second.messages_address],
        adultFirstNames: [
          jsonString(first.profile, "firstName") ?? first.display_name,
          jsonString(second.profile, "firstName") ?? second.display_name,
        ],
      };
    }
    return null;
  }

  async readNextIncompleteHouseholdActivation(): Promise<string | null> {
    const rows = await this.#sql<
      {
        household_id: string;
        founder_adult_id: string;
        partner_adult_id: string;
        group_adult_one_id: string;
        group_identity_one_digest: string;
        group_adult_two_id: string;
        group_identity_two_digest: string;
        group_authority_digest: string;
      }[]
    >`
      select h.id as household_id,
        founder.id as founder_adult_id,
        partner.id as partner_adult_id,
        family_group.adult_one_id as group_adult_one_id,
        family_group.identity_one_digest as group_identity_one_digest,
        family_group.adult_two_id as group_adult_two_id,
        family_group.identity_two_digest as group_identity_two_digest,
        family_group.authority_digest as group_authority_digest
      from households h
      join people founder on founder.household_id=h.id and founder.kind='adult'
        and founder.role='steward' and founder.adult_slot=1 and founder.status='verified'
        and founder.identity_subject_digest is not null and founder.messages_address is not null
        and nullif(founder.profile->>'onboardingCompletedAt','') is not null
      join people partner on partner.household_id=h.id and partner.kind='adult'
        and partner.role='steward' and partner.adult_slot=2 and partner.status='verified'
        and partner.identity_subject_digest is not null and partner.messages_address is not null
        and nullif(partner.profile->>'onboardingCompletedAt','') is not null
      join linq_channels family_group on family_group.household_id=h.id
        and family_group.audience='group' and family_group.adult_two_id is not null
        and family_group.identity_two_digest is not null
        and family_group.revoked_at is null and family_group.stopped_at is null
      where (
        h.family_calendar_id is null or h.family_calendar_id='primary'
        or h.family_calendar_owner_connection_id is null
        or h.family_calendar_partner_connection_id is null
        or h.family_calendar_created_at is null
        or h.family_calendar_label is distinct from h.name
        or not exists (
          select 1 from proactive_work briefing
          where briefing.household_id=h.id and briefing.kind='initial_household_briefing'
        )
      )
        and exists (
          select 1 from google_connections founder_google
          where founder_google.household_id=h.id and founder_google.owner_adult_id=founder.id
            and founder_google.status='active'
        )
        and exists (
          select 1 from google_connections partner_google
          where partner_google.household_id=h.id and partner_google.owner_adult_id=partner.id
            and partner_google.status='active'
        )
      order by h.created_at,h.id
    `;
    for (const row of rows) {
      const adultIds = [row.founder_adult_id, row.partner_adult_id].sort();
      const groupAdultIds = [row.group_adult_one_id, row.group_adult_two_id].sort();
      const groupIdentityDigests = [row.group_identity_one_digest, row.group_identity_two_digest].sort();
      if (
        sameStrings(adultIds, groupAdultIds) &&
        row.group_authority_digest === digestStrings([...adultIds, ...groupIdentityDigests])
      ) {
        return row.household_id;
      }
    }
    return null;
  }

  async bindCreatedMessagesGroup(input: {
    householdId: string;
    providerConversationId: string;
    participants: readonly {
      identitySubjectDigest: string;
      phoneNumber: string;
    }[];
    occurredAt: string;
  }): Promise<LinqChannelRecord> {
    const providerConversationId = required(
      input.providerConversationId,
      "Family group Linq conversation ID",
    );
    const observed = sortedDigests(
      input.participants.map((participant) => participant.identitySubjectDigest),
    );
    if (observed.length !== 2 || new Set(observed).size !== 2) {
      throw new FlorenceStoreConflict("The family group requires both distinct adult identities");
    }
    const observedPhoneNumbers = input.participants.map((participant) =>
      required(participant.phoneNumber, "Family group participant phone number"),
    );
    if (new Set(observedPhoneNumbers).size !== 2) {
      throw new FlorenceStoreConflict("The family group requires both distinct adult phone numbers");
    }
    const occurredAt = instant(input.occurredAt);
    const channel = await this.#sql.begin(async (sql) => {
      const [household] = await sql<{ id: string }[]>`
        select id from households where id=${input.householdId} for update
      `;
      if (!household) throw new FlorenceStoreConflict("The family group household does not exist");
      const adults = await sql<PersonRow[]>`
        select * from people where household_id=${input.householdId} and kind='adult'
        order by id for share
      `;
      if (
        adults.length !== 2 ||
        adults.some(
          (adult) =>
            adult.status !== "verified" ||
            adult.identity_subject_digest === null ||
            (adult.adult_slot !== 1 && adult.adult_slot !== 2),
        ) ||
        !adults.some((adult) => adult.adult_slot === 1) ||
        !adults.some((adult) => adult.adult_slot === 2) ||
        !sameStrings(
          adults.flatMap((adult) => adult.messages_address ?? []).sort(),
          [...observedPhoneNumbers].sort(),
        )
      ) {
        throw new FlorenceStoreConflict("The family group requires exactly both verified adults");
      }
      const mapped = adults.map((adult) => {
        const participant = input.participants.find(
          (candidate) => candidate.phoneNumber === adult.messages_address,
        );
        if (!participant) {
          throw new FlorenceStoreConflict("The family group participant mapping is incomplete");
        }
        return { adult, identitySubjectDigest: participant.identitySubjectDigest };
      });
      const [currentGroup] = await sql<ChannelRow[]>`
        select * from linq_channels where household_id=${input.householdId}
          and audience='group' and revoked_at is null for update
      `;
      if (currentGroup) {
        if (
          currentGroup.provider_conversation_id !== providerConversationId ||
          !sameStrings(channelIdentityDigests(currentGroup), observed) ||
          mapped.some(
            ({ adult, identitySubjectDigest }) =>
              !(
                (currentGroup.adult_one_id === adult.id &&
                  currentGroup.identity_one_digest === identitySubjectDigest) ||
                (currentGroup.adult_two_id === adult.id &&
                  currentGroup.identity_two_digest === identitySubjectDigest)
              ),
          ) ||
          currentGroup.authority_digest !==
            digestStrings([...mapped.map(({ adult }) => adult.id), ...observed])
        ) {
          throw new FlorenceStoreConflict("A different family group is already connected");
        }
        return currentGroup;
      }
      const [providerOwner] = await sql<{ id: string }[]>`
        select id from linq_channels where provider_conversation_id=${providerConversationId}
        union all
        select id from people where invitation_conversation_id=${providerConversationId}
        limit 1
      `;
      if (providerOwner) {
        throw new FlorenceStoreConflict("That Linq conversation is already bound to another channel");
      }
      const first = mapped[0];
      const second = mapped[1];
      if (!first || !second) {
        throw new FlorenceStoreConflict("The verified family identities are incomplete");
      }
      const [inserted] = await sql<ChannelRow[]>`
        insert into linq_channels (
          id,household_id,audience,provider_conversation_id,adult_one_id,identity_one_digest,
          adult_two_id,identity_two_digest,authority_digest,bound_at
        ) values (${deterministicUuid(`linq-group\0${providerConversationId}`)},${input.householdId},
          'group',${providerConversationId},${first.adult.id},${first.identitySubjectDigest},
          ${second.adult.id},${second.identitySubjectDigest},
          ${digestStrings([first.adult.id, second.adult.id, ...observed])},${occurredAt})
        returning *
      `;
      return inserted;
    });
    if (!channel) throw new Error("The family group was not bound");
    return channelRecord(channel);
  }

  async beginFamilyCalendarCreation(input: {
    householdId: string;
    now: string;
  }): Promise<{ createAllowed: boolean; calendarId: string | null }> {
    assertUuid(input.householdId, "Family Calendar household ID");
    const now = instant(input.now);
    return this.#sql.begin(async (sql) => {
      const [household] = await sql<
        {
          family_calendar_id: string | null;
          family_calendar_create_attempted_at: Date | null;
        }[]
      >`
        select family_calendar_id,family_calendar_create_attempted_at
        from households where id=${input.householdId} for update
      `;
      if (!household) {
        throw new FlorenceStoreConflict("The Family Calendar household does not exist");
      }
      if (household.family_calendar_id !== null) {
        return { createAllowed: false, calendarId: household.family_calendar_id };
      }
      if (household.family_calendar_create_attempted_at !== null) {
        return { createAllowed: false, calendarId: null };
      }
      await sql`
        update households set family_calendar_create_attempted_at=${now},updated_at=${now}
        where id=${input.householdId}
      `;
      return { createAllowed: true, calendarId: null };
    });
  }

  async rememberFamilyCalendarId(input: {
    householdId: string;
    calendarId: string;
    occurredAt: string;
  }): Promise<HouseholdRecord> {
    const calendarId = required(input.calendarId, "Family Calendar ID");
    assertUuid(input.householdId, "Family Calendar household ID");
    if (calendarId === "primary") {
      throw new FlorenceStoreConflict("Family Calendar must be a separate calendar");
    }
    const occurredAt = instant(input.occurredAt);
    await this.#sql.begin(async (sql) => {
      const [household] = await sql<
        {
          family_calendar_id: string | null;
          family_calendar_owner_connection_id: string | null;
          family_calendar_partner_connection_id: string | null;
          family_calendar_label: string | null;
          family_calendar_created_at: Date | null;
          family_calendar_create_attempted_at: Date | null;
        }[]
      >`
        select family_calendar_id,family_calendar_owner_connection_id,
          family_calendar_partner_connection_id,family_calendar_label,family_calendar_created_at,
          family_calendar_create_attempted_at
        from households where id=${input.householdId} for update
      `;
      if (!household) throw new FlorenceStoreConflict("The Family Calendar household does not exist");
      if (household.family_calendar_create_attempted_at === null) {
        throw new FlorenceStoreConflict("Family Calendar creation has not been attempted");
      }
      if (household.family_calendar_id !== null) {
        if (household.family_calendar_id !== calendarId) {
          throw new FlorenceStoreConflict("A different Family Calendar ID is already stored");
        }
        return;
      }
      if (
        household.family_calendar_owner_connection_id !== null ||
        household.family_calendar_partner_connection_id !== null ||
        household.family_calendar_label !== null ||
        household.family_calendar_created_at !== null
      ) {
        throw new FlorenceStoreConflict("The Family Calendar resume state is incomplete");
      }
      await sql`
        update households set family_calendar_id=${calendarId},updated_at=${occurredAt}
        where id=${input.householdId}
      `;
    });
    return requiredHousehold(await this.readHousehold({ householdId: input.householdId }));
  }

  async completeFamilyCalendarProvisioning(input: {
    householdId: string;
    calendarId: string;
    founderConnectionId: string;
    partnerConnectionId: string;
    label: string;
    occurredAt: string;
  }): Promise<HouseholdRecord> {
    const calendarId = required(input.calendarId, "Family Calendar ID");
    assertUuid(input.householdId, "Family Calendar household ID");
    if (calendarId === "primary") {
      throw new FlorenceStoreConflict("Family Calendar must be a separate calendar");
    }
    assertUuid(input.founderConnectionId, "Founder Google connection ID");
    assertUuid(input.partnerConnectionId, "Partner Google connection ID");
    if (input.founderConnectionId === input.partnerConnectionId) {
      throw new FlorenceStoreConflict("Family Calendar adults need separate Google connections");
    }
    const label = required(input.label, "Family Calendar label");
    const occurredAt = instant(input.occurredAt);
    await this.#sql.begin(async (sql) => {
      const [household] = await sql<
        {
          family_calendar_id: string | null;
          family_calendar_owner_connection_id: string | null;
          family_calendar_partner_connection_id: string | null;
          family_calendar_label: string | null;
          family_calendar_created_at: Date | null;
          family_calendar_create_attempted_at: Date | null;
        }[]
      >`
        select family_calendar_id,family_calendar_owner_connection_id,
          family_calendar_partner_connection_id,family_calendar_label,family_calendar_created_at,
          family_calendar_create_attempted_at
        from households where id=${input.householdId} for update
      `;
      if (
        !household ||
        household.family_calendar_id !== calendarId ||
        household.family_calendar_create_attempted_at === null
      ) {
        throw new FlorenceStoreConflict("The exact Family Calendar ID was not stored for resume");
      }
      if (household.family_calendar_created_at !== null) {
        if (
          household.family_calendar_owner_connection_id !== input.founderConnectionId ||
          household.family_calendar_partner_connection_id !== input.partnerConnectionId ||
          household.family_calendar_label !== label
        ) {
          throw new FlorenceStoreConflict("Family Calendar provisioning was already completed differently");
        }
        return;
      }
      if (
        household.family_calendar_owner_connection_id !== null ||
        household.family_calendar_partner_connection_id !== null ||
        household.family_calendar_label !== null
      ) {
        throw new FlorenceStoreConflict("The Family Calendar provisioning state is incomplete");
      }
      const connections = await sql<
        { id: string; owner_adult_id: string; adult_slot: 1 | 2 | null; status: GoogleConnectionStatus }[]
      >`
        select g.id,g.owner_adult_id,p.adult_slot,g.status
        from google_connections g join people p
          on p.household_id=g.household_id and p.id=g.owner_adult_id
        where g.household_id=${input.householdId}
          and g.id in ${sql([input.founderConnectionId, input.partnerConnectionId])}
        for share of g,p
      `;
      const founder = connections.find((connection) => connection.id === input.founderConnectionId);
      const partner = connections.find((connection) => connection.id === input.partnerConnectionId);
      if (
        connections.length !== 2 ||
        founder?.status !== "active" ||
        founder.adult_slot !== 1 ||
        partner?.status !== "active" ||
        partner.adult_slot !== 2 ||
        founder.owner_adult_id === partner.owner_adult_id
      ) {
        throw new FlorenceStoreUnauthorized(
          "Family Calendar provisioning requires each adult's exact active Google connection",
        );
      }
      await sql`
        update households set
          family_calendar_owner_connection_id=${input.founderConnectionId},
          family_calendar_partner_connection_id=${input.partnerConnectionId},
          family_calendar_label=${label},family_calendar_created_at=${occurredAt},updated_at=${occurredAt}
        where id=${input.householdId}
      `;
      await reconcileHouseholdProactiveConsentState(sql, input.householdId, occurredAt);
    });
    return requiredHousehold(await this.readHousehold({ householdId: input.householdId }));
  }

  async confirmFamilyCalendarLabel(input: {
    householdId: string;
    calendarId: string;
    label: string;
    occurredAt: string;
  }): Promise<HouseholdRecord> {
    assertUuid(input.householdId, "Family Calendar household ID");
    const calendarId = required(input.calendarId, "Family Calendar ID");
    const label = required(input.label, "Family Calendar label");
    const occurredAt = instant(input.occurredAt);
    const updated = await this.#sql`
      update households set family_calendar_label=${label},updated_at=${occurredAt}
      where id=${input.householdId} and family_calendar_id=${calendarId}
        and family_calendar_created_at is not null
      returning id
    `;
    if (updated.length !== 1) {
      throw new FlorenceStoreConflict("The confirmed Family Calendar is no longer active");
    }
    return requiredHousehold(await this.readHousehold({ householdId: input.householdId }));
  }

  async resolveLinqAuthority(input: {
    providerConversationId: string;
    audience: Audience;
    participantIdentityDigests: readonly string[];
    senderIdentitySubjectDigest: string;
    replyToProviderMessageId?: string | null;
    occurredAt: string;
  }): Promise<LinqAuthority | null> {
    return resolveLinqAuthorityOn(this.#sql, input);
  }

  async acceptInbound(input: AcceptInboundInput): Promise<AcceptInboundResult | null> {
    const authority = await this.resolveLinqAuthority(input);
    if (!authority) return null;
    const [channel] = await this.#sql<ChannelRow[]>`
      select * from linq_channels where id=${authority.channelId} and authority_digest=${authority.authorityDigest}
        and revoked_at is null
    `;
    if (!channel) return null;
    return this.#sql.begin((sql) => insertInbound(sql, channel, authority.senderAdultId, input));
  }

  async acceptInboundWithPreparation(
    input: AcceptInboundEnvelopeInput,
    prepare: (context: InboundPreparationContext) => Promise<PreparedInboundContent>,
    options: {
      supersedesSourceId?: string | null;
      resolveSupersedesSourceId?: () => string | null;
    } = {},
  ): Promise<AcceptInboundResult | null> {
    assertDigest(input.providerPayloadDigest, "Linq provider payload");
    const envelope: AcceptInboundEnvelopeInput = Object.freeze({
      ...input,
      participantIdentityDigests: Object.freeze([...input.participantIdentityDigests]),
    });
    const sourceId = deterministicUuid(`linq-v3\0signal\0${envelope.providerEventId}`);
    const authority = await resolveLinqAuthorityOn(this.#sql, envelope);
    if (!authority) return null;

    const [existing] = await this.#sql<
      {
        source_id: string;
        provider_message_id: string;
        channel_id: string;
        metadata: JsonValue;
      }[]
    >`
      select m.source_id,m.provider_message_id,m.channel_id,s.metadata
      from messages m join sources s on s.id=m.source_id
      where m.provider_event_id=${envelope.providerEventId} limit 1
    `;
    if (existing) {
      const storedDigest = jsonRecord(existing.metadata).providerPayloadDigest;
      if (storedDigest !== undefined) {
        if (
          typeof storedDigest !== "string" ||
          existing.source_id !== sourceId ||
          existing.provider_message_id !== envelope.providerMessageId ||
          existing.channel_id !== authority.channelId ||
          storedDigest !== envelope.providerPayloadDigest
        ) {
          throw new FlorenceStoreConflict("A Linq event ID was reused with different content");
        }
        return {
          disposition: "duplicate",
          sourceId,
          householdId: authority.householdId,
          channelId: authority.channelId,
        };
      }
    }
    if (authority.stopped && !existing) {
      return {
        disposition: "stopped",
        sourceId,
        householdId: authority.householdId,
        channelId: authority.channelId,
      };
    }

    const prepared = await prepare({ sourceId, householdId: authority.householdId });
    return this.#sql.begin(async (sql) => {
      const currentAuthority = await resolveLinqAuthorityOn(sql, envelope);
      if (!currentAuthority) return null;
      const [channel] = await sql<ChannelRow[]>`
        select * from linq_channels where id=${currentAuthority.channelId}
          and authority_digest=${currentAuthority.authorityDigest} and revoked_at is null
        for update
      `;
      if (!channel) return null;
      const supersedesSourceId = options.resolveSupersedesSourceId?.() ?? options.supersedesSourceId ?? null;
      return insertInbound(sql, channel, currentAuthority.senderAdultId, {
        ...envelope,
        text: prepared.text,
        ...(prepared.authoredText !== undefined ? { authoredText: prepared.authoredText } : {}),
        ...(prepared.voiceTranscriptPresent !== undefined
          ? { voiceTranscriptPresent: prepared.voiceTranscriptPresent }
          : {}),
        ...(prepared.images ? { images: prepared.images } : {}),
        ...(prepared.documents ? { documents: prepared.documents } : {}),
        providerPayloadDigest: envelope.providerPayloadDigest,
        ...(supersedesSourceId ? { supersedesSourceId } : {}),
      });
    });
  }

  async acceptInboundReaction(input: AcceptInboundReactionInput): Promise<AcceptInboundResult | null> {
    const authority = await this.resolveLinqAuthority({
      ...input,
      replyToProviderMessageId: input.targetProviderMessageId,
    });
    if (!authority) return null;
    const [channel] = await this.#sql<ChannelRow[]>`
      select * from linq_channels where id=${authority.channelId} and authority_digest=${authority.authorityDigest}
        and revoked_at is null
    `;
    if (!channel) return null;
    return this.#sql.begin((sql) => insertInboundReaction(sql, channel, authority.senderAdultId, input));
  }

  async stageTurnCue(input: {
    sourceId: string;
    cue: "reaction" | "work" | "retry";
    occurredAt: string;
  }): Promise<string | null> {
    assertUuid(input.sourceId, "Inbound source ID");
    const occurredAt = instant(input.occurredAt);
    return this.#sql.begin(async (sql) => {
      const [turn] = await sql<
        {
          source_id: string;
          household_id: string;
          channel_id: string;
          visibility: Visibility;
          owner_adult_id: string | null;
          metadata: JsonValue;
        }[]
      >`
        select m.source_id,s.household_id,m.channel_id,s.visibility,s.owner_adult_id,s.metadata
        from messages m join sources s on s.id=m.source_id join linq_channels c on c.id=m.channel_id
        where m.source_id=${input.sourceId} and m.direction='inbound' and m.status='received'
          and coalesce(m.retry_at,m.not_before)<=${occurredAt}
          and c.revoked_at is null and c.stopped_at is null
        for update of m
      `;
      if (!turn) return null;
      const rootSourceId = await supersessionRoot(sql, turn.channel_id, turn.source_id, turn.metadata);
      const cueTurnId = deterministicUuid(`cue-turn\0${rootSourceId}`);
      const sourceId = deterministicUuid(`cue\0${rootSourceId}\0${input.cue}`);
      await insertOutbound(sql, {
        sourceId,
        idempotencyKey: `cue:${rootSourceId}:${input.cue}`,
        moveKind: input.cue === "reaction" ? "reaction" : "message",
        ...(input.cue === "reaction"
          ? { reaction: "like", replyToSourceId: turn.source_id, turnPart: -1 as const }
          : {
              text:
                input.cue === "work"
                  ? "I’m on it—checking now."
                  : "I hit a temporary snag. I’m trying again now.",
              replyToSourceId: turn.source_id,
              turnPart: input.cue === "work" ? (0 as const) : (1 as const),
            }),
        turnId: cueTurnId,
        notBefore: occurredAt.toISOString(),
        householdId: turn.household_id,
        channelId: turn.channel_id,
        visibility: turn.visibility,
        ownerAdultId: turn.owner_adult_id,
        occurredAt,
      });
      await sql`
        update messages set status='pending',sending_at=null,retry_at=null,last_error=null,
          reply_to_source_id=${turn.source_id},not_before=${occurredAt}
        where source_id=${sourceId} and direction='outbound' and status='failed'
          and last_error='Superseded before delivery by a newer message in this conversation'
      `;
      return sourceId;
    });
  }

  async readNextInbound(now: string = new Date().toISOString()): Promise<InboundTurn | null> {
    const current = instant(now);
    await this.#sql`
      update documents set content_envelope=null
      where retained=false and discard_after<=${current} and content_envelope is not null
    `;
    const [row] = await this.#sql<
      {
        source_id: string;
        household_id: string;
        channel_id: string;
        sender_adult_id: string;
        move_kind: "message" | "reply" | "reaction";
        text: string | null;
        reaction: string | null;
        images: JsonValue;
        reply_to_source_id: string | null;
        metadata: JsonValue;
        occurred_at: Date;
      }[]
    >`
      select m.source_id,s.household_id,m.channel_id,m.sender_adult_id,m.move_kind,m.text,m.reaction,m.images,
             m.reply_to_source_id,s.metadata,s.occurred_at
      from messages m join sources s on s.id=m.source_id
      join linq_channels c on c.id=m.channel_id
      where m.direction='inbound' and m.status='received'
        and coalesce(m.retry_at,m.not_before)<=${current}
        and c.revoked_at is null and c.stopped_at is null
      order by coalesce(m.retry_at,m.not_before),s.occurred_at,m.source_id limit 1
    `;
    if (!row) return null;
    const [channel] = await this.#sql<ChannelRow[]>`select * from linq_channels where id=${row.channel_id}`;
    const [household] = await this.#sql<
      {
        id: string;
        name: string;
        time_zone: string;
        family_calendar_id: string | null;
        family_calendar_owner_connection_id: string | null;
        family_calendar_partner_connection_id: string | null;
        family_calendar_label: string | null;
        family_calendar_created_at: Date | null;
      }[]
    >`
      select id,name,time_zone,family_calendar_id,family_calendar_owner_connection_id,
             family_calendar_partner_connection_id,family_calendar_label,family_calendar_created_at
      from households where id=${row.household_id}
    `;
    if (!channel || !household) return null;
    const members = await this.#sql<PersonRow[]>`
      select * from people where household_id=${row.household_id} order by adult_slot nulls last,created_at,id
    `;
    const supersededRows = await this.#sql<
      {
        source_id: string;
        sender_adult_id: string;
        move_kind: "message" | "reply" | "reaction";
        text: string | null;
        reaction: string | null;
        images: JsonValue;
        reply_to_source_id: string | null;
        metadata: JsonValue;
        occurred_at: Date;
        depth: number;
      }[]
    >`
      with recursive superseded as (
        select prior_message.source_id,prior_message.sender_adult_id,prior_message.move_kind,
          prior_message.text,prior_message.reaction,prior_message.images,prior_message.reply_to_source_id,
          prior_source.occurred_at,prior_source.metadata,1 as depth,
          array[current_source.id,prior_source.id] as path
        from sources current_source
        join sources prior_source
          on prior_source.id::text=current_source.metadata->>'supersedesSourceId'
        join messages prior_message on prior_message.source_id=prior_source.id
        where current_source.id=${row.source_id} and prior_message.channel_id=${row.channel_id}
          and prior_message.direction='inbound'
        union all
        select prior_message.source_id,prior_message.sender_adult_id,prior_message.move_kind,
          prior_message.text,prior_message.reaction,prior_message.images,prior_message.reply_to_source_id,
          prior_source.occurred_at,prior_source.metadata,superseded.depth+1,
          superseded.path||prior_source.id
        from superseded
        join sources prior_source on prior_source.id::text=superseded.metadata->>'supersedesSourceId'
        join messages prior_message on prior_message.source_id=prior_source.id
        where prior_message.channel_id=${row.channel_id} and prior_message.direction='inbound'
          and not prior_source.id=any(superseded.path) and superseded.depth<100
      )
      select source_id,sender_adult_id,move_kind,text,reaction,images,reply_to_source_id,
        metadata,occurred_at,depth
      from superseded order by depth desc,occurred_at,source_id
    `;
    const activeSourceIds = [...supersededRows.map((message) => message.source_id), row.source_id];
    const currentDocumentRows = await this.#sql<
      {
        id: string;
        parent_source_id: string;
        filename: string;
        mime_type: string;
        content_digest: string;
        content_envelope: Uint8Array;
        discard_after: Date;
      }[]
    >`
      select s.id,s.parent_source_id,d.filename,d.mime_type,d.content_digest,d.content_envelope,d.discard_after
      from sources s join sources parent on parent.id=s.parent_source_id
      join documents d on d.source_id=s.id
      where s.household_id=${row.household_id} and s.parent_source_id in ${this.#sql(activeSourceIds)}
        and s.visibility=parent.visibility and s.owner_adult_id is not distinct from parent.owner_adult_id
        and s.kind='document' and d.mime_type='application/pdf' and d.retained=false
        and d.content_envelope is not null and d.discard_after>${current}
      order by parent.occurred_at,s.id
    `;
    const privateViewer = channel.audience === "private" ? row.sender_adult_id : null;
    const recentRows = await this.#sql<
      {
        source_id: string;
        sender_adult_id: string | null;
        direction: "inbound" | "outbound";
        move_kind: "message" | "reply" | "reaction";
        text: string | null;
        reaction: string | null;
        images: JsonValue;
        reply_to_source_id: string | null;
        metadata: JsonValue;
        occurred_at: Date;
      }[]
    >`
      select m.source_id,m.sender_adult_id,m.direction,m.move_kind,m.text,m.reaction,m.images,
             m.reply_to_source_id,s.metadata,s.occurred_at
      from messages m join sources s on s.id=m.source_id
      where m.channel_id=${row.channel_id} and m.source_id not in ${this.#sql(activeSourceIds)}
        and m.status in ('handled','sent')
      order by s.occurred_at desc,m.turn_part desc,m.source_id desc limit 100
    `;
    const [replyTargetRow] = row.reply_to_source_id
      ? await this.#sql<
          {
            source_id: string;
            sender_adult_id: string | null;
            direction: "inbound" | "outbound";
            move_kind: "message" | "reply" | "reaction";
            text: string | null;
            reaction: string | null;
            images: JsonValue;
            reply_to_source_id: string | null;
            metadata: JsonValue;
            occurred_at: Date;
          }[]
        >`
          select m.source_id,m.sender_adult_id,m.direction,m.move_kind,m.text,m.reaction,m.images,
                 m.reply_to_source_id,s.metadata,s.occurred_at
          from messages m join sources s on s.id=m.source_id
          where m.source_id=${row.reply_to_source_id} and m.channel_id=${row.channel_id}
          limit 1
        `
      : [];
    const [groupCalendarAuthority] =
      channel.audience === "group"
        ? await this.#sql<FamilyCalendarAuthorityRow[]>`
            select h.family_calendar_id,h.family_calendar_owner_connection_id,
              h.family_calendar_partner_connection_id,h.family_calendar_created_at,
              founder.id as founder_adult_id,founder.identity_subject_digest as founder_identity_digest,
              founder.status as founder_status,founder_connection.status as founder_connection_status,
              partner.id as partner_adult_id,partner.identity_subject_digest as partner_identity_digest,
              partner.status as partner_status,partner_connection.status as partner_connection_status
            from households h
            left join google_connections founder_connection
              on founder_connection.id=h.family_calendar_owner_connection_id
                and founder_connection.household_id=h.id
            left join people founder on founder.household_id=h.id
              and founder.id=founder_connection.owner_adult_id and founder.kind='adult'
                and founder.role='steward' and founder.adult_slot=1
            left join google_connections partner_connection
              on partner_connection.id=h.family_calendar_partner_connection_id
                and partner_connection.household_id=h.id
            left join people partner on partner.household_id=h.id
              and partner.id=partner_connection.owner_adult_id and partner.kind='adult'
                and partner.role='steward' and partner.adult_slot=2
            where h.id=${row.household_id}
          `
        : [];
    const groupCalendarIsActive =
      groupCalendarAuthority !== undefined &&
      isExactFamilyCalendarAuthority(groupCalendarAuthority, channel, row.sender_adult_id);
    const privateCalendarOfferOwner = members.find(
      (member) =>
        member.id === row.sender_adult_id && member.kind === "adult" && member.status === "verified",
    );
    const privateCalendarOfferOwnerIsActive = Boolean(
      channel.audience === "private" &&
        privateCalendarOfferOwner?.identity_subject_digest &&
        channel.adult_one_id === row.sender_adult_id &&
        channel.adult_two_id === null &&
        channel.identity_one_digest === privateCalendarOfferOwner.identity_subject_digest &&
        channel.identity_two_digest === null &&
        channel.authority_digest ===
          digestStrings([row.sender_adult_id, privateCalendarOfferOwner.identity_subject_digest]) &&
        channel.revoked_at === null &&
        channel.stopped_at === null &&
        household.family_calendar_id &&
        household.family_calendar_id !== "primary" &&
        household.family_calendar_owner_connection_id &&
        household.family_calendar_partner_connection_id &&
        household.family_calendar_created_at,
    );
    const offerRows = await this.#sql<
      {
        id: string;
        approval_prompt_source_id: string;
        payload: JsonValue;
      }[]
    >`
      select action.id,approval_prompt.source_id as approval_prompt_source_id,action.payload
      from calendar_actions action
      join sources basis_source on basis_source.id=action.basis_source_id
      join messages approval_prompt on approval_prompt.source_id=action.approval_prompt_source_id
        and approval_prompt.channel_id=${channel.id} and approval_prompt.direction='outbound'
        and approval_prompt.move_kind in ('message','reply') and approval_prompt.turn_part=0
        and approval_prompt.status='sent'
      where action.household_id=${row.household_id} and action.status='offered'
        and (
          ${groupCalendarIsActive}
          or (
            ${privateCalendarOfferOwnerIsActive}
            and basis_source.kind='calendar' and basis_source.visibility='private'
            and basis_source.owner_adult_id=${row.sender_adult_id}
          )
        )
      order by action.created_at,action.id
    `;
    const followUpRows = await this.#sql<
      {
        id: string;
        objective: string;
        current_conclusion: string;
        end_condition: string;
        next_check_at: Date;
        why: string;
        source_ids: string[];
        google_backed: boolean;
      }[]
    >`
      select w.id,w.objective,w.current_conclusion,w.end_condition,w.next_check_at,w.why,
        array_agg(pws.source_id order by pws.source_id) as source_ids,
        bool_or(source.kind in ('gmail','calendar','google_file')) as google_backed
      from proactive_work w join proactive_work_sources pws on pws.work_id=w.id
      join sources source on source.id=pws.source_id
      where w.household_id=${row.household_id} and w.kind='finite_monitor' and w.status='active'
        and (
          (${channel.audience === "private"} and w.visibility='private'
            and w.owner_adult_id=${row.sender_adult_id})
          or (${channel.audience === "group"} and w.visibility='household')
        )
      group by w.id,w.objective,w.current_conclusion,w.end_condition,w.next_check_at,w.why
      order by w.next_check_at,w.id
    `;
    const reminderRows = await this.#sql<
      {
        id: string;
        objective: string;
        reminder_schedule: JsonValue;
        status: "active" | "paused" | "delivering" | "completed" | "cancelled";
        next_check_at: Date | null;
        last_run_at: Date | null;
        created_at: Date;
      }[]
    >`
      select id,objective,reminder_schedule,status,next_check_at,last_run_at,created_at
      from proactive_work
      where household_id=${row.household_id} and kind='reminder'
        and (
          (${channel.audience === "private"} and visibility='private'
            and owner_adult_id=${row.sender_adult_id})
          or (${channel.audience === "group"} and visibility='household'
            and owner_adult_id is null)
        )
      order by
        case when status in ('active','paused') then 0 else 1 end,
        next_check_at nulls last,created_at desc,id
      limit 100
    `;
    const familyWorkRows = await this.#sql<
      {
        id: string;
        objective: string;
        current_conclusion: string;
        status: "active" | "paused" | "delivering" | "completed" | "cancelled";
        created_at: Date;
      }[]
    >`
      with current_work as (
        select id,objective,current_conclusion,status,created_at
        from proactive_work
        where household_id=${row.household_id} and kind='family_task'
          and status in ('active','paused','delivering')
          and (
            (${channel.audience === "private"} and visibility='private'
              and owner_adult_id=${row.sender_adult_id})
            or (${channel.audience === "group"} and visibility='household'
              and owner_adult_id is null)
          )
        order by created_at,id limit 100
      ), recent_work as (
        select id,objective,current_conclusion,status,created_at
        from proactive_work
        where household_id=${row.household_id} and kind='family_task'
          and status in ('completed','cancelled')
          and (
            (${channel.audience === "private"} and visibility='private'
              and owner_adult_id=${row.sender_adult_id})
            or (${channel.audience === "group"} and visibility='household'
              and owner_adult_id is null)
          )
        order by created_at desc,id desc limit 10
      )
      select * from current_work
      union all
      select * from recent_work
    `;
    const interestRows = await this.#sql<
      {
        id: string;
        status: "active" | "paused";
        discovery_terms: string[];
        objective: string;
        why: string;
      }[]
    >`
      select id,status,discovery_terms,objective,why from proactive_work
      where household_id=${row.household_id} and kind='interest_monitor'
        and visibility='household' and owner_adult_id is null and status in ('active','paused')
        and cardinality(discovery_terms)>0 and objective is not null and why is not null
      order by created_at,id
    `;
    let pendingPartnerInvitation: PendingPartnerInvitation | null = null;
    const founder = members.find(
      (member) =>
        member.id === row.sender_adult_id &&
        member.kind === "adult" &&
        member.role === "steward" &&
        member.adult_slot === 1 &&
        member.status === "verified",
    );
    if (
      channel.audience === "private" &&
      founder &&
      channel.adult_one_id === founder.id &&
      channel.adult_two_id === null
    ) {
      const handoff = founderHandoffIdentity(
        row.household_id,
        founder.id,
        await householdLinqIncarnationScope(this.#sql, row.household_id),
      );
      const handoffRows = await this.#sql<
        {
          source_id: string;
          channel_id: string;
          status: "pending" | "sending" | "sent" | "failed";
          move_kind: "message" | "reply" | "reaction";
          turn_part: -1 | 0 | 1 | 2;
          idempotency_key: string | null;
        }[]
      >`
        select source_id,channel_id,status,move_kind,turn_part,idempotency_key
        from messages where turn_id=${handoff.turnId} order by turn_part
      `;
      const handoffSent =
        handoffRows.length === 2 &&
        handoffRows.every((message, index) => {
          const part = handoff.part(index);
          return (
            message.source_id === part.sourceId &&
            message.channel_id === channel.id &&
            message.status === "sent" &&
            message.move_kind === "message" &&
            message.turn_part === index &&
            message.idempotency_key === part.idempotencyKey
          );
        });
      if (handoffSent) {
        const [partner] = await this.#sql<{ id: string; first_name: string; phone_number: string }[]>`
          select id,profile->>'firstName' as first_name,profile->>'phoneNumber' as phone_number
          from people where household_id=${row.household_id} and kind='adult' and role='steward'
            and adult_slot=2 and status='planned' and identity_subject_digest is null
            and invitation_digest is null and invitation_expires_at is null
            and messages_address is null
            and invitation_approval_source_id is null and invitation_approved_at is null
            and invitation_retry_at is null and invitation_last_error is null
            and (
              (invitation_consumed_at is null and invitation_conversation_id is null
                and invitation_identity_digest is null and invitation_message_id is null
                and invitation_issued_at is null)
              or
              (invitation_consumed_at is not null and invitation_conversation_id is not null
                and invitation_identity_digest is not null and invitation_message_id is not null
                and invitation_issued_at is not null)
              or
              (invitation_consumed_at is not null and invitation_conversation_id is null
                and invitation_identity_digest is null and invitation_message_id is null
                and invitation_issued_at is null)
            )
            and nullif(profile->>'firstName','') is not null
            and (profile->>'phoneNumber') ~ '^[+][1-9][0-9]{7,14}$'
          limit 1
        `;
        if (partner) {
          const approvalPromptSourceId = handoffRows.at(-1)?.source_id;
          if (!approvalPromptSourceId) {
            throw new FlorenceStoreConflict("The founder handoff approval prompt is unavailable");
          }
          pendingPartnerInvitation = {
            adultId: partner.id,
            firstName: partner.first_name,
            maskedPhoneNumber: maskPhoneNumber(partner.phone_number),
            approvalPromptSourceId,
            phoneNumber: partner.phone_number,
          };
        }
      }
    }
    const authority = authorityRecord(channel, row.sender_adult_id, row.reply_to_source_id);
    return {
      message: {
        sourceId: row.source_id,
        speaker: row.sender_adult_id,
        moveKind: row.move_kind,
        text: row.text,
        ...conversationAuthorship(row.metadata),
        reaction: row.reaction,
        images: imageReferences(row.images),
        replyToSourceId: row.reply_to_source_id,
        occurredAt: row.occurred_at.toISOString(),
      },
      supersededMessages: supersededRows.map((message) => ({
        sourceId: message.source_id,
        speaker: message.sender_adult_id,
        moveKind: message.move_kind,
        text: message.text,
        ...conversationAuthorship(message.metadata),
        reaction: message.reaction,
        images: imageReferences(message.images),
        replyToSourceId: message.reply_to_source_id,
        occurredAt: message.occurred_at.toISOString(),
      })),
      replyTarget: replyTargetRow
        ? {
            sourceId: replyTargetRow.source_id,
            speaker:
              replyTargetRow.direction === "outbound"
                ? "florence"
                : (replyTargetRow.sender_adult_id as string),
            moveKind: replyTargetRow.move_kind,
            text: replyTargetRow.text,
            ...conversationAuthorship(replyTargetRow.metadata),
            reaction: replyTargetRow.reaction,
            images: imageReferences(replyTargetRow.images),
            replyToSourceId: replyTargetRow.reply_to_source_id,
            occurredAt: replyTargetRow.occurred_at.toISOString(),
          }
        : null,
      authority,
      household: {
        id: household.id,
        name: household.name,
        timeZone: household.time_zone,
        familyCalendarId: household.family_calendar_id,
        familyCalendarOwnerConnectionId: household.family_calendar_owner_connection_id,
        familyCalendarPartnerConnectionId: household.family_calendar_partner_connection_id,
        familyCalendarLabel: household.family_calendar_label,
        familyCalendarCreatedAt: household.family_calendar_created_at?.toISOString() ?? null,
        members: members.map(personRecord),
      },
      facts: await this.#readFacts(row.household_id, privateViewer, channel.audience === "group"),
      currentDocuments: currentDocumentRows.map((document) => ({
        id: document.id,
        parentSourceId: document.parent_source_id,
        filename: document.filename,
        mimeType: pdfMimeType(document.mime_type),
        contentDigest: document.content_digest,
        contentEnvelope: document.content_envelope,
        discardAfter: document.discard_after.toISOString(),
      })),
      recentMessages: recentRows.reverse().map((turn) => ({
        sourceId: turn.source_id,
        speaker: turn.direction === "outbound" ? "florence" : (turn.sender_adult_id as string),
        moveKind: turn.move_kind,
        text: turn.text,
        ...conversationAuthorship(turn.metadata),
        reaction: turn.reaction,
        images: imageReferences(turn.images),
        replyToSourceId: turn.reply_to_source_id,
        occurredAt: turn.occurred_at.toISOString(),
      })),
      pendingFollowUps: followUpRows.map((followUp) => ({
        id: followUp.id,
        objective: followUp.objective,
        currentConclusion: followUp.current_conclusion,
        endCondition: followUp.end_condition,
        nextCheck: followUp.next_check_at.toISOString(),
        why: followUp.why,
        sourceIds: followUp.source_ids,
        googleBacked: followUp.google_backed,
      })),
      householdDocket: await this.readHouseholdDocket({
        householdId: row.household_id,
        limit: 20,
        now: current.toISOString(),
      }),
      visibleFamilyWork: familyWorkRows.map((work) => ({
        workId: work.id,
        objective: work.objective,
        currentProgress: work.current_conclusion,
        status: work.status === "paused" ? "waiting" : work.status,
        createdAt: work.created_at.toISOString(),
      })),
      visibleReminders: reminderRows.map((reminder) => ({
        reminderId: reminder.id,
        action: reminder.objective,
        schedule: reminderSchedule(reminder.reminder_schedule),
        status: reminder.status === "delivering" ? "active" : reminder.status,
        nextAt: reminder.next_check_at?.toISOString() ?? null,
        lastRunAt: reminder.last_run_at?.toISOString() ?? null,
        createdAt: reminder.created_at.toISOString(),
      })),
      visibleInterests: interestRows.map((interest) => ({
        interestWorkId: interest.id,
        status: interest.status,
        genericTerms: interest.discovery_terms,
        objective: interest.objective,
        why: interest.why,
      })),
      pendingCalendarOffers: offerRows.map((offer) => ({
        id: offer.id,
        approvalPromptSourceId: offer.approval_prompt_source_id,
        event: calendarOfferEvent(offer.payload),
      })),
      pendingPartnerInvitation,
    };
  }

  async commitTurn(input: CommitTurnInput): Promise<CommitTurnResult> {
    const handledAt = instant(input.handledAt);
    const completeDocketCandidateIds = input.completeDocketCandidateIds ?? [];
    if (
      completeDocketCandidateIds.length > 20 ||
      new Set(completeDocketCandidateIds).size !== completeDocketCandidateIds.length
    ) {
      throw new FlorenceStoreConflict(
        "A conversation cannot complete repeated or excessive household docket items",
      );
    }
    for (const candidateId of completeDocketCandidateIds) {
      assertUuid(candidateId, "Household docket candidate ID");
    }
    return this.#sql.begin(async (sql) => {
      const [turn] = await sql<
        {
          source_id: string;
          household_id: string;
          channel_id: string;
          sender_adult_id: string;
          audience: Audience;
          visibility: Visibility;
          move_kind: "message" | "reply" | "reaction";
          status: "received" | "handled";
          occurred_at: Date;
          revoked_at: Date | null;
          stopped_at: Date | null;
          provider_conversation_id: string;
          adult_one_id: string;
          identity_one_digest: string;
          adult_two_id: string | null;
          identity_two_digest: string | null;
          authority_digest: string;
          bound_at: Date;
          source_metadata: JsonValue;
        }[]
      >`
        select m.source_id,s.household_id,m.channel_id,m.sender_adult_id,c.audience,s.visibility,
          m.move_kind,m.status,s.occurred_at,c.revoked_at,c.stopped_at,
          c.provider_conversation_id,c.adult_one_id,c.identity_one_digest,c.adult_two_id,
          c.identity_two_digest,c.authority_digest,c.bound_at,s.metadata as source_metadata
        from messages m join sources s on s.id=m.source_id join linq_channels c on c.id=m.channel_id
        where m.source_id=${input.sourceId} and m.direction='inbound'
          and coalesce(m.retry_at,m.not_before)<=${handledAt}
        for update of m,s
      `;
      if (!turn) throw new FlorenceStoreConflict("The inbound message is no longer awaiting a turn");
      if (turn.revoked_at || turn.stopped_at) {
        throw new FlorenceStoreConflict("The inbound message is no longer awaiting a turn");
      }
      const [newer] = await sql<{ source_id: string }[]>`
        select newer.source_id from messages newer
        join sources newer_source on newer_source.id=newer.source_id
        where newer.channel_id=${turn.channel_id} and newer.direction='inbound'
          and newer.move_kind in ('message','reply') and newer.source_id<>${turn.source_id}
          and (newer_source.occurred_at,newer_source.id) > (${turn.occurred_at},${turn.source_id}::uuid)
        order by newer_source.occurred_at,newer_source.id limit 1
        for share of newer,newer_source
      `;
      if (newer) {
        await sql`
          update messages set status='handled',handled_at=${handledAt},retry_at=null,
            last_error='Superseded by a newer message in this conversation'
          where source_id=${turn.source_id} and direction='inbound' and status='received'
        `;
        return "superseded";
      }
      if (turn.status !== "received") {
        throw new FlorenceStoreConflict("The inbound message is no longer awaiting a turn");
      }
      if (input.stopChannel) {
        if (
          (input.facts?.length ?? 0) > 0 ||
          (input.deleteFactIds?.length ?? 0) > 0 ||
          (input.finiteMonitors?.length ?? 0) > 0 ||
          (input.cancelMonitorIds?.length ?? 0) > 0 ||
          input.interestMutation != null ||
          input.reminderMutation != null ||
          input.familyWorkMutation != null ||
          (input.completeDocketCandidateIds?.length ?? 0) > 0 ||
          (input.outbound?.length ?? 0) > 0 ||
          (input.calendarOffers?.length ?? 0) > 0 ||
          (input.approveCalendarOffers?.length ?? 0) > 0 ||
          (input.calendarActions?.length ?? 0) > 0 ||
          input.partnerInvitationApproval !== undefined ||
          input.householdUpdate !== undefined
        ) {
          throw new FlorenceStoreConflict("Stopping Messages cannot commit any other turn mutations");
        }
        await stopMessagesChannel(sql, turn.channel_id, handledAt);
        const handled = await sql`
          update messages set status='handled',handled_at=${handledAt},retry_at=null,last_error=null
          where source_id=${turn.source_id} and direction='inbound' and status='received'
          returning source_id
        `;
        if (handled.length !== 1) throw new FlorenceStoreConflict("The inbound turn changed before commit");
        return "committed";
      }
      if (turn.audience !== "private" && input.partnerInvitationApproval !== undefined) {
        throw new FlorenceStoreUnauthorized("Partner invitations require a private adult thread");
      }
      if (completeDocketCandidateIds.length > 0) {
        // A Google poll takes its work row before reconciling sources, monitors, Calendar actions,
        // and the review. Take that same leading lock before this turn mutates Google-backed state.
        await lockHouseholdGooglePolls(sql, turn.household_id);
      }

      let householdUpdateGroup: (ChannelRow & FamilyGroupAuthorityRow) | null = null;
      let householdUpdateText: string | null = null;
      if (input.householdUpdate) {
        const authorship = conversationAuthorship(turn.source_metadata);
        if (
          turn.audience !== "private" ||
          turn.visibility !== "private" ||
          turn.move_kind === "reaction" ||
          !authorship.authoredText?.trim() ||
          input.householdUpdate.basisSourceId !== turn.source_id
        ) {
          throw new FlorenceStoreUnauthorized(
            "A household update requires the current adult's typed private Message",
          );
        }
        if (input.outbound?.some((outbound) => outbound.moveKind !== "reaction")) {
          throw new FlorenceStoreConflict(
            "A private turn cannot send conversation bubbles alongside a household update",
          );
        }
        householdUpdateText = required(input.householdUpdate.text, "Household update");
        if (householdUpdateText.length > 1_000) {
          throw new FlorenceStoreConflict("A household update is too long");
        }
        const [group] = await sql<(ChannelRow & FamilyGroupAuthorityRow)[]>`
          select family_group.*,founder.id as founder_adult_id,
            founder.identity_subject_digest as founder_identity_digest,
            founder.status as founder_status,partner.id as partner_adult_id,
            partner.identity_subject_digest as partner_identity_digest,
            partner.status as partner_status
          from linq_channels family_group
          join people founder on founder.household_id=family_group.household_id
            and founder.kind='adult' and founder.role='steward' and founder.adult_slot=1
          join people partner on partner.household_id=family_group.household_id
            and partner.kind='adult' and partner.role='steward' and partner.adult_slot=2
          where family_group.household_id=${turn.household_id} and family_group.audience='group'
            and family_group.adult_two_id is not null and family_group.revoked_at is null
            and family_group.stopped_at is null
          order by family_group.bound_at desc limit 1
          for share of family_group,founder,partner
        `;
        if (!group || !isExactFamilyGroupAuthority(group, group, turn.sender_adult_id)) {
          throw new FlorenceStoreUnauthorized("A household update requires the exact active family group");
        }
        householdUpdateGroup = group;
      }

      const calendarMutations =
        (input.calendarOffers?.length ?? 0) > 0 ||
        (input.approveCalendarOffers?.length ?? 0) > 0 ||
        (input.calendarActions?.length ?? 0) > 0;
      if (
        turn.audience === "private" &&
        ((input.calendarOffers?.length ?? 0) > 0 || (input.calendarActions?.length ?? 0) > 0)
      ) {
        throw new FlorenceStoreUnauthorized(
          "New Calendar offers and direct changes belong in the exact family Messages group",
        );
      }
      const groupCalendarAuthority =
        turn.audience === "group" && calendarMutations
          ? await readFamilyCalendarAuthority(sql, turn.household_id)
          : undefined;
      const groupChannel: ChannelRow = {
        id: turn.channel_id,
        household_id: turn.household_id,
        audience: turn.audience,
        provider_conversation_id: turn.provider_conversation_id,
        adult_one_id: turn.adult_one_id,
        identity_one_digest: turn.identity_one_digest,
        adult_two_id: turn.adult_two_id,
        identity_two_digest: turn.identity_two_digest,
        authority_digest: turn.authority_digest,
        bound_at: turn.bound_at,
        revoked_at: turn.revoked_at,
        stopped_at: turn.stopped_at,
      };
      const googleConnectionIdsUsed = unique(input.googleConnectionIdsUsed ?? []).sort();
      if (googleConnectionIdsUsed.length > 10) {
        throw new FlorenceStoreConflict("A conversation turn used too many Google connections");
      }
      for (const connectionId of googleConnectionIdsUsed) {
        assertUuid(connectionId, "Used Google connection ID");
      }
      if (googleConnectionIdsUsed.length > 0) {
        const authorizedConnections = await sql<{ id: string }[]>`
          select connection.id from google_connections connection
          join people owner on owner.household_id=connection.household_id
            and owner.id=connection.owner_adult_id and owner.kind='adult' and owner.status='verified'
          join households household on household.id=connection.household_id
          where connection.household_id=${turn.household_id} and connection.status='active'
            and connection.id in ${sql(googleConnectionIdsUsed)}
            and (
              (${turn.audience}='private' and connection.owner_adult_id=${turn.sender_adult_id})
              or (${turn.audience}='group'
                and connection.id in (
                  household.family_calendar_owner_connection_id,
                  household.family_calendar_partner_connection_id
                )
                and household.family_calendar_id is not null
                and household.family_calendar_created_at is not null)
            )
          order by connection.id for share of connection,owner,household
        `;
        if (authorizedConnections.length !== googleConnectionIdsUsed.length) {
          throw new FlorenceStoreUnauthorized(
            "A conversational Google read no longer belongs to this active audience",
          );
        }
      }
      const googleFactConnections = await sql<{ id: string }[]>`
        select active_connection.id from facts fact
        join fact_sources link on link.fact_id=fact.id
        join sources source on source.id=link.source_id
        join google_connections source_connection on source_connection.household_id=source.household_id
          and source_connection.id::text=source.metadata->>'connectionId'
        join google_connections active_connection
          on active_connection.household_id=source_connection.household_id
          and active_connection.owner_adult_id=source_connection.owner_adult_id
          and source_connection.google_subject_digest is not null
          and active_connection.google_subject_digest is not null
          and active_connection.google_subject_digest=source_connection.google_subject_digest
          and active_connection.status='active'
        where fact.household_id=${turn.household_id}
          and source.kind in ('gmail','calendar','google_file')
          and (
            (${turn.audience}='group' and fact.visibility='household')
            or (${turn.audience}='private' and (
              fact.visibility='household'
              or (fact.visibility='private' and fact.owner_adult_id=${turn.sender_adult_id})
            ))
          )
        order by active_connection.id
      `;
      const googleDependencyConnectionIds = unique([
        ...googleConnectionIdsUsed,
        ...googleFactConnections.map((connection) => connection.id),
      ]).sort();
      if (
        turn.audience === "group" &&
        calendarMutations &&
        (!groupCalendarAuthority ||
          !isExactFamilyCalendarAuthority(groupCalendarAuthority, groupChannel, turn.sender_adult_id))
      ) {
        throw new FlorenceStoreUnauthorized(
          "Household Calendar changes require the exact active family group and Family Calendar",
        );
      }

      await persistGoogleEvidenceDrafts(sql, {
        householdId: turn.household_id,
        drafts: input.googleEvidence ?? [],
        sourceIds: unique([
          ...(input.facts ?? []).flatMap((fact) => [...fact.sourceIds]),
          ...(input.finiteMonitors ?? []).flatMap((monitor) => [...monitor.sourceIds]),
          ...(input.finiteMonitorUpdates ?? []).flatMap((monitor) => [...monitor.sourceIds]),
        ]),
      });

      if (input.partnerInvitationApproval) {
        assertUuid(input.partnerInvitationApproval.adultId, "Partner invitation adult ID");
        const [founder] = await sql<{ id: string }[]>`
          select id from people where household_id=${turn.household_id} and id=${turn.sender_adult_id}
            and kind='adult' and role='steward' and adult_slot=1 and status='verified'
          for share
        `;
        const [partner] = await sql<PersonRow[]>`
          select * from people where household_id=${turn.household_id}
            and id=${input.partnerInvitationApproval.adultId}
            and kind='adult' and role='steward' and adult_slot=2 and status='planned'
          for update
        `;
        if (!founder || !partner || !awaitingPartnerInvitationApproval(partner)) {
          throw new FlorenceStoreConflict("The exact planned partner is no longer awaiting an invitation");
        }
        const handoff = founderHandoffIdentity(
          turn.household_id,
          founder.id,
          await householdLinqIncarnationScope(sql, turn.household_id),
        );
        const handoffRows = await sql<
          {
            source_id: string;
            channel_id: string;
            status: "pending" | "sending" | "sent" | "failed";
            move_kind: "message" | "reply" | "reaction";
            turn_part: -1 | 0 | 1 | 2;
            idempotency_key: string | null;
          }[]
        >`
          select source_id,channel_id,status,move_kind,turn_part,idempotency_key
          from messages where turn_id=${handoff.turnId} order by turn_part for share
        `;
        if (
          handoffRows.length !== 2 ||
          handoffRows.some((message, index) => {
            const part = handoff.part(index);
            return (
              message.source_id !== part.sourceId ||
              message.channel_id !== turn.channel_id ||
              message.status !== "sent" ||
              message.move_kind !== "message" ||
              message.turn_part !== index ||
              message.idempotency_key !== part.idempotencyKey
            );
          })
        ) {
          throw new FlorenceStoreUnauthorized(
            "The founder must approve the exact partner after Florence's handoff question was sent",
          );
        }
        const approved = await sql`
          update people set invitation_digest=null,invitation_expires_at=null,
            invitation_consumed_at=null,messages_address=null,
            invitation_conversation_id=null,invitation_identity_digest=null,
            invitation_message_id=null,invitation_issued_at=null,
            invitation_approval_source_id=${turn.source_id},
            invitation_approved_at=${handledAt},invitation_retry_at=${handledAt},
            invitation_last_error=null,updated_at=${handledAt}
          where id=${partner.id} and invitation_approval_source_id is null returning id
        `;
        if (approved.length !== 1) {
          throw new FlorenceStoreConflict("The partner invitation approval changed before commit");
        }
      }

      for (const fact of input.facts ?? []) {
        assertUuid(fact.id, "Fact ID");
        if (fact.sourceIds.length === 0) throw new FlorenceStoreConflict("A fact requires a source");
        await assertSourcesVisible(
          sql,
          turn.household_id,
          turn.audience,
          turn.sender_adult_id,
          fact.sourceIds,
        );
        if (turn.audience === "group" && fact.visibility !== "household") {
          throw new FlorenceStoreUnauthorized("A group message cannot create a private fact");
        }
        if (fact.visibility === "private" && fact.ownerAdultId !== turn.sender_adult_id) {
          throw new FlorenceStoreUnauthorized("A private fact belongs to the adult in this conversation");
        }
        if (turn.audience === "private" && fact.visibility === "household") {
          throw new FlorenceStoreUnauthorized("A private message cannot create or change household memory");
        }
        const [existing] = await sql<{ id: string }[]>`
          select id from facts where household_id=${turn.household_id} and slot=${fact.slot}
            and visibility=${fact.visibility} and owner_adult_id is not distinct from ${fact.ownerAdultId}
          for update
        `;
        const factId = existing?.id ?? fact.id;
        if (existing) {
          await sql`
            update facts set subject_person_id=${fact.subjectPersonId},kind=${fact.kind},label=${fact.label},
              value=${sql.json(fact.value)},corrected_at=${handledAt},updated_at=${handledAt}
            where id=${factId}
          `;
          await sql`delete from fact_sources where fact_id=${factId}`;
        } else {
          await sql`
            insert into facts (id,household_id,subject_person_id,kind,slot,label,value,visibility,
              owner_adult_id,created_at,updated_at)
            values (${factId},${turn.household_id},${fact.subjectPersonId},${fact.kind},${fact.slot},${fact.label},
              ${sql.json(fact.value)},${fact.visibility},${fact.ownerAdultId},${handledAt},${handledAt})
          `;
        }
        for (const sourceId of unique(fact.sourceIds)) {
          await sql`insert into fact_sources (fact_id,source_id) values (${factId},${sourceId})`;
        }
      }
      for (const factId of unique(input.deleteFactIds ?? [])) {
        assertUuid(factId, "Forgotten fact ID");
        const deleted = await sql`
          delete from facts where id=${factId} and household_id=${turn.household_id}
            and ((${turn.audience}='group' and visibility='household')
              or (${turn.audience}='private' and visibility='private'
                and owner_adult_id=${turn.sender_adult_id}))
          returning id
        `;
        if (deleted.length !== 1) {
          throw new FlorenceStoreUnauthorized(
            "A turn tried to forget a fact outside its conversation audience",
          );
        }
      }

      for (const monitor of input.finiteMonitors ?? []) {
        assertUuid(monitor.id, "Finite monitor ID");
        if (monitor.sourceIds.length === 0) {
          throw new FlorenceStoreConflict("A finite monitor requires a source");
        }
        const expectedVisibility = turn.audience === "group" ? "household" : "private";
        const expectedOwnerAdultId = expectedVisibility === "private" ? turn.sender_adult_id : null;
        if (monitor.visibility !== expectedVisibility || monitor.ownerAdultId !== expectedOwnerAdultId) {
          throw new FlorenceStoreUnauthorized("A finite monitor must stay inside its conversation audience");
        }
        await assertSourcesVisible(
          sql,
          turn.household_id,
          turn.audience,
          turn.sender_adult_id,
          monitor.sourceIds,
        );
        const nextCheck = instant(monitor.nextCheck);
        if (nextCheck <= handledAt) {
          throw new FlorenceStoreConflict("A finite monitor must check again in the future");
        }
        const inserted = await sql<{ id: string }[]>`
          insert into proactive_work (
            id,household_id,kind,visibility,owner_adult_id,objective,why,
            current_conclusion,end_condition,status,next_check_at,created_at
          ) values (${monitor.id},${turn.household_id},'finite_monitor',${monitor.visibility},
            ${monitor.ownerAdultId},${bounded(required(monitor.objective, "Finite monitor objective"), 2_000)},
            ${bounded(required(monitor.why, "Finite monitor reason"), 2_000)},
            ${bounded(required(monitor.currentConclusion, "Finite monitor conclusion"), 4_000)},
            ${bounded(required(monitor.endCondition, "Finite monitor end condition"), 2_000)},
            'active',${nextCheck},${handledAt})
          on conflict do nothing returning id
        `;
        if (inserted.length > 0) {
          for (const sourceId of unique(monitor.sourceIds)) {
            await sql`
              insert into proactive_work_sources (work_id,source_id) values (${monitor.id},${sourceId})
            `;
          }
        }
      }
      for (const monitor of input.finiteMonitorUpdates ?? []) {
        assertUuid(monitor.id, "Finite monitor ID");
        if (monitor.sourceIds.length === 0) {
          throw new FlorenceStoreConflict("A finite monitor update requires a source");
        }
        await assertSourcesVisible(
          sql,
          turn.household_id,
          turn.audience,
          turn.sender_adult_id,
          monitor.sourceIds,
        );
        const nextCheck = instant(monitor.nextCheck);
        if (nextCheck <= handledAt) {
          throw new FlorenceStoreConflict("A finite monitor must check again in the future");
        }
        const updated = await sql<{ id: string }[]>`
          update proactive_work set objective=${bounded(
            required(monitor.objective, "Finite monitor objective"),
            2_000,
          )},current_conclusion=${bounded(
            required(monitor.currentConclusion, "Finite monitor conclusion"),
            4_000,
          )},end_condition=${bounded(
            required(monitor.endCondition, "Finite monitor end condition"),
            2_000,
          )},why=${bounded(required(monitor.why, "Finite monitor reason"), 2_000)},
            next_check_at=${nextCheck},status='active',
            last_error=case
              when left(coalesce(last_error,''),${GOOGLE_ACTION_WORK_MARKER_PREFIX.length})=${GOOGLE_ACTION_WORK_MARKER_PREFIX}
              then left(last_error,${GOOGLE_ACTION_WORK_MARKER_LENGTH})
              else null
            end
          where id=${monitor.id} and household_id=${turn.household_id} and kind='finite_monitor'
            and status='active'
            and ((${turn.audience}='group' and visibility='household' and owner_adult_id is null)
              or (${turn.audience}='private' and visibility='private'
                and owner_adult_id=${turn.sender_adult_id}))
          returning id
        `;
        if (updated.length !== 1) {
          throw new FlorenceStoreConflict("A monitor is no longer editable in this conversation");
        }
        for (const sourceId of unique(monitor.sourceIds)) {
          await sql`
            insert into proactive_work_sources (work_id,source_id)
            values (${monitor.id},${sourceId}) on conflict do nothing
          `;
        }
      }
      const cancelMonitorIds = unique(input.cancelMonitorIds ?? []);
      if (cancelMonitorIds.length > 0) {
        const cancelled = await sql`
          delete from proactive_work
          where household_id=${turn.household_id} and kind='finite_monitor'
            and id in ${sql(cancelMonitorIds)} and status in ('active','paused')
            and ((${turn.audience}='group' and visibility='household')
              or (${turn.audience}='private' and visibility='private'
                and owner_adult_id=${turn.sender_adult_id}))
          returning id
        `;
        if (cancelled.length !== cancelMonitorIds.length) {
          throw new FlorenceStoreConflict("A monitor is no longer active in this conversation");
        }
      }

      if (input.interestMutation) {
        await applyConversationalInterestMutation(sql, {
          householdId: turn.household_id,
          audience: turn.audience,
          senderAdultId: turn.sender_adult_id,
          currentSourceId: turn.source_id,
          moveKind: turn.move_kind,
          mutation: input.interestMutation,
          occurredAt: handledAt,
        });
      }

      if (input.familyWorkMutation) {
        const mutation = input.familyWorkMutation;
        if (turn.move_kind === "reaction") {
          throw new FlorenceStoreUnauthorized("Family work control requires a current parent request");
        }
        const expectedVisibility: Visibility = turn.audience === "group" ? "household" : "private";
        const expectedOwnerAdultId = expectedVisibility === "private" ? turn.sender_adult_id : null;
        assertUuid(mutation.workId, "Family work ID");
        if (mutation.operation === "create") {
          if (mutation.visibility !== expectedVisibility || mutation.ownerAdultId !== expectedOwnerAdultId) {
            throw new FlorenceStoreUnauthorized(
              "Family work must stay inside the conversation where it was requested",
            );
          }
          const objective = bounded(required(mutation.objective, "Family work objective"), 4_000);
          const [sameId] = await sql<ProactiveWorkRow[]>`
            select * from proactive_work where id=${mutation.workId} for update
          `;
          if (sameId) throw new FlorenceStoreConflict("A family work ID was already used");
          const state = initialFamilyWorkState();
          await sql`
            insert into proactive_work (
              id,household_id,kind,visibility,owner_adult_id,objective,current_conclusion,
              task_state,status,next_check_at,created_at
            ) values (${mutation.workId},${turn.household_id},'family_task',${mutation.visibility},
              ${mutation.ownerAdultId},${objective},'Starting now.',${sql.json(state)},'active',
              ${new Date(handledAt.getTime() + FAMILY_WORK_INITIAL_DELAY_MS)},${handledAt})
          `;
          await sql`
            insert into proactive_work_sources (work_id,source_id)
            values (${mutation.workId},${turn.source_id})
          `;
        } else {
          const [work] = await sql<ProactiveWorkRow[]>`
            select * from proactive_work
            where id=${mutation.workId} and household_id=${turn.household_id} and kind='family_task'
              and ((${turn.audience}='group' and visibility='household' and owner_adult_id is null)
                or (${turn.audience}='private' and visibility='private'
                  and owner_adult_id=${turn.sender_adult_id}))
            for update
          `;
          if (!work) {
            throw new FlorenceStoreUnauthorized("That family work does not belong to this conversation");
          }
          await sql`
            insert into proactive_work_sources (work_id,source_id)
            values (${work.id},${turn.source_id}) on conflict do nothing
          `;
          const state = familyWorkState(work.task_state);
          if (mutation.operation === "steer") {
            if (work.status === "completed" || work.status === "cancelled") {
              throw new FlorenceStoreConflict("Finished family work is no longer steerable");
            }
            const instruction = bounded(
              required(mutation.instruction, "Family work steering instruction"),
              4_000,
            );
            const nextState = steerFamilyWorkState(state, {
              sourceId: turn.source_id,
              text: instruction,
              occurredAt: turn.occurred_at.toISOString(),
            });
            await terminalizeUnsentFamilyWorkOutbounds(sql, work.id, "steering");
            await sql`
              update proactive_work set task_state=${sql.json(nextState)},status='active',
                next_check_at=${handledAt},last_error=null where id=${work.id}
            `;
          } else if (work.status !== "cancelled") {
            if (work.status === "completed") {
              throw new FlorenceStoreConflict("Completed family work cannot be cancelled");
            }
            const generation = incrementFamilyWorkCounter(state.generation, "Family work generation");
            const progressRevision = incrementFamilyWorkCounter(
              state.progressRevision,
              "Family work progress revision",
            );
            const nextState = familyWorkState({
              ...state,
              generation,
              phase: "terminal",
              claim: null,
              pendingCall: null,
              progressRevision,
              terminal: { outcome: "cancelled", text: "Cancelled." },
            });
            await terminalizeUnsentFamilyWorkOutbounds(sql, work.id, "cancellation");
            await sql`
              update proactive_work set task_state=${sql.json(nextState)},status='cancelled',
                next_check_at=null,current_conclusion='Cancelled.',last_error=null where id=${work.id}
            `;
          }
        }
      }

      if (input.reminderMutation) {
        const mutation = input.reminderMutation;
        if (turn.move_kind === "reaction") {
          throw new FlorenceStoreUnauthorized("Reminder control requires a current parent request");
        }
        assertUuid(mutation.reminderId, "Reminder ID");
        if (mutation.operation === "create") {
          const expectedVisibility = turn.audience === "group" ? "household" : "private";
          const expectedOwnerAdultId = expectedVisibility === "private" ? turn.sender_adult_id : null;
          if (mutation.visibility !== expectedVisibility || mutation.ownerAdultId !== expectedOwnerAdultId) {
            throw new FlorenceStoreUnauthorized(
              "A reminder must stay inside the conversation where it was requested",
            );
          }
          const action = bounded(required(mutation.action, "Reminder action"), 1_000);
          const schedule = validateReminderSchedule(mutation.schedule);
          const [sameId] = await sql<ProactiveWorkRow[]>`
            select * from proactive_work where id=${mutation.reminderId} for update
          `;
          let storedReminderId = mutation.reminderId;
          if (sameId) {
            if (
              sameId.household_id !== turn.household_id ||
              sameId.kind !== "reminder" ||
              sameId.visibility !== mutation.visibility ||
              sameId.owner_adult_id !== mutation.ownerAdultId ||
              sameId.objective !== action ||
              JSON.stringify(reminderSchedule(sameId.reminder_schedule)) !== JSON.stringify(schedule)
            ) {
              throw new FlorenceStoreConflict("A reminder ID was reused for a different reminder");
            }
          } else {
            const [household] = await sql<{ time_zone: string }[]>`
              select time_zone from households where id=${turn.household_id} for share
            `;
            if (!household) throw new FlorenceStoreConflict("The reminder household no longer exists");
            const nextAt = nextReminderOccurrence(schedule, handledAt, household.time_zone);
            if (!nextAt) throw new FlorenceStoreConflict("A one-time reminder must still be in the future");
            const [duplicate] = await sql<{ id: string }[]>`
              select id from proactive_work
              where household_id=${turn.household_id} and kind='reminder'
                and visibility=${mutation.visibility}
                and owner_adult_id is not distinct from ${mutation.ownerAdultId}
                and objective=${action} and reminder_schedule=${sql.json(schedule)}
                and status in ('active','paused','delivering')
              limit 1 for update
            `;
            if (duplicate) {
              storedReminderId = duplicate.id;
            } else {
              await sql`
                insert into proactive_work (
                  id,household_id,kind,visibility,owner_adult_id,objective,reminder_schedule,
                  status,next_check_at,created_at
                ) values (${mutation.reminderId},${turn.household_id},'reminder',${mutation.visibility},
                  ${mutation.ownerAdultId},${action},${sql.json(schedule)},'active',${nextAt},${handledAt})
              `;
            }
          }
          await sql`
            insert into proactive_work_sources (work_id,source_id)
            values (${storedReminderId},${turn.source_id}) on conflict do nothing
          `;
        } else {
          const [reminder] = await sql<ProactiveWorkRow[]>`
            select * from proactive_work
            where id=${mutation.reminderId} and household_id=${turn.household_id} and kind='reminder'
              and ((${turn.audience}='group' and visibility='household' and owner_adult_id is null)
                or (${turn.audience}='private' and visibility='private'
                  and owner_adult_id=${turn.sender_adult_id}))
            for update
          `;
          if (!reminder) {
            throw new FlorenceStoreUnauthorized("That reminder does not belong to this conversation");
          }
          await sql`
            insert into proactive_work_sources (work_id,source_id)
            values (${reminder.id},${turn.source_id}) on conflict do nothing
          `;
          if (mutation.operation === "update") {
            if (reminder.status === "completed" || reminder.status === "cancelled") {
              throw new FlorenceStoreConflict("A finished reminder is no longer editable");
            }
            if (mutation.action === null && mutation.schedule === null) {
              throw new FlorenceStoreConflict("A reminder update must change its action or schedule");
            }
            const schedule =
              mutation.schedule === null
                ? reminderSchedule(reminder.reminder_schedule)
                : validateReminderSchedule(mutation.schedule);
            const action =
              mutation.action === null
                ? required(reminder.objective ?? "", "Reminder action")
                : bounded(required(mutation.action, "Reminder action"), 1_000);
            if (reminder.status === "delivering" && mutation.schedule === null) {
              if (
                mutation.action === null ||
                !(await updateQueuedReminderOccurrenceText(sql, reminder.id, reminderText(action), handledAt))
              ) {
                throw new FlorenceStoreConflict("That due reminder is already being delivered");
              }
              await sql`
                update proactive_work set objective=${action},last_error=null where id=${reminder.id}
              `;
            } else {
              let nextAt = reminder.next_check_at;
              if (
                (reminder.status === "active" || reminder.status === "delivering") &&
                mutation.schedule !== null
              ) {
                const [household] = await sql<{ time_zone: string }[]>`
                  select time_zone from households where id=${turn.household_id} for share
                `;
                if (!household) {
                  throw new FlorenceStoreConflict("The reminder household no longer exists");
                }
                nextAt = nextReminderOccurrence(schedule, handledAt, household.time_zone);
                if (!nextAt) {
                  throw new FlorenceStoreConflict("A one-time reminder must still be in the future");
                }
              }
              await terminalizeUnsentReminderOccurrences(sql, reminder.id);
              await sql`
                update proactive_work set objective=${action},reminder_schedule=${sql.json(schedule)},
                  status=${reminder.status === "delivering" ? "active" : reminder.status},
                  next_check_at=${nextAt},last_error=null where id=${reminder.id}
              `;
            }
          } else if (mutation.operation === "pause") {
            if (reminder.status === "active" || reminder.status === "delivering") {
              await terminalizeUnsentReminderOccurrences(sql, reminder.id);
              await sql`
                update proactive_work set status='paused',next_check_at=null,last_error=null
                where id=${reminder.id}
              `;
            } else if (reminder.status !== "paused") {
              throw new FlorenceStoreConflict("A finished reminder cannot be paused");
            }
          } else if (mutation.operation === "resume") {
            if (reminder.status === "paused") {
              const [household] = await sql<{ time_zone: string }[]>`
                select time_zone from households where id=${turn.household_id} for share
              `;
              if (!household) throw new FlorenceStoreConflict("The reminder household no longer exists");
              const nextAt = nextReminderOccurrence(
                reminderSchedule(reminder.reminder_schedule),
                handledAt,
                household.time_zone,
              );
              if (!nextAt) throw new FlorenceStoreConflict("That one-time reminder has already expired");
              await sql`
                update proactive_work set status='active',next_check_at=${nextAt},last_error=null
                where id=${reminder.id}
              `;
            } else if (reminder.status !== "active") {
              throw new FlorenceStoreConflict("A finished reminder cannot be resumed");
            }
          } else if (mutation.operation === "cancel") {
            if (
              reminder.status === "active" ||
              reminder.status === "paused" ||
              reminder.status === "delivering"
            ) {
              await terminalizeUnsentReminderOccurrences(sql, reminder.id);
              await sql`
                update proactive_work set status='cancelled',next_check_at=null,last_error=null
                where id=${reminder.id}
              `;
            } else if (reminder.status !== "cancelled") {
              throw new FlorenceStoreConflict("A completed reminder cannot be cancelled");
            }
          } else {
            if (reminder.status === "completed" || reminder.status === "cancelled") {
              throw new FlorenceStoreConflict("A finished reminder cannot run again");
            }
            const schedule = reminderSchedule(reminder.reminder_schedule);
            const reusedQueuedOccurrence = await rearmQueuedReminderOccurrence(sql, reminder.id, handledAt);
            const consumesDue =
              reminder.status === "active" &&
              reminder.next_check_at !== null &&
              reminder.next_check_at <= handledAt;
            const scheduledAt = consumesDue ? (reminder.next_check_at as Date) : handledAt;
            if (!reusedQueuedOccurrence) {
              await insertProactiveOutbound(sql, {
                workId: reminder.id,
                suffix: consumesDue
                  ? `reminder:${scheduledAt.toISOString()}`
                  : `reminder-run:${turn.source_id}`,
                householdId: reminder.household_id,
                channel: groupChannel,
                visibility: reminder.visibility,
                ownerAdultId: reminder.owner_adult_id,
                text: reminderText(required(reminder.objective ?? "", "Reminder action")),
                metadata: { reminderId: reminder.id, scheduledAt: scheduledAt.toISOString() },
                notBefore: handledAt,
                occurredAt: handledAt,
              });
            }
            let nextAt = reminder.next_check_at;
            let nextStatus: ProactiveWorkRow["status"] = reminder.status;
            if (!reusedQueuedOccurrence && schedule.kind === "once") {
              nextAt = null;
              nextStatus = "delivering";
            } else if (!reusedQueuedOccurrence && consumesDue) {
              const [household] = await sql<{ time_zone: string }[]>`
                select time_zone from households where id=${turn.household_id} for share
              `;
              if (!household) throw new FlorenceStoreConflict("The reminder household no longer exists");
              nextAt = nextReminderOccurrence(schedule, handledAt, household.time_zone);
            }
            await sql`
              update proactive_work set status=${nextStatus},next_check_at=${nextAt},
                last_run_at=${handledAt},last_error=null where id=${reminder.id}
            `;
          }
        }
      }

      if (completeDocketCandidateIds.length > 0) {
        await completeHouseholdDocketCandidates(sql, {
          householdId: turn.household_id,
          candidateIds: completeDocketCandidateIds,
          basisSourceId: turn.source_id,
          handledAt,
        });
      }

      for (const outbound of input.outbound ?? []) {
        if (outbound.replyToSourceId) {
          await assertSourcesVisible(sql, turn.household_id, turn.audience, turn.sender_adult_id, [
            outbound.replyToSourceId,
          ]);
        }
        await insertOutbound(sql, {
          ...outbound,
          householdId: turn.household_id,
          channelId: turn.channel_id,
          parentSourceId: turn.source_id,
          visibility: turn.visibility,
          ownerAdultId: turn.visibility === "private" ? turn.sender_adult_id : null,
          ...(googleDependencyConnectionIds.length > 0
            ? { metadata: { googleConnectionIds: [...googleDependencyConnectionIds] } }
            : {}),
          occurredAt: handledAt,
        });
      }

      if (householdUpdateGroup && householdUpdateText) {
        const updateTurnId = deterministicUuid(`household-update-turn\0${turn.source_id}`);
        await insertOutbound(sql, {
          sourceId: deterministicUuid(`household-update\0${turn.source_id}`),
          idempotencyKey: `household-update:${turn.source_id}`,
          moveKind: "message",
          text: householdUpdateText,
          turnId: updateTurnId,
          turnPart: 0,
          notBefore: handledAt.toISOString(),
          householdId: turn.household_id,
          channelId: householdUpdateGroup.id,
          visibility: "household",
          ownerAdultId: null,
          occurredAt: handledAt,
        });
      }

      for (const offer of input.calendarOffers ?? []) {
        if (turn.audience !== "group" || !groupCalendarAuthority) {
          throw new FlorenceStoreUnauthorized("A Calendar offer requires the exact family group");
        }
        assertUuid(offer.id, "Calendar offer ID");
        if (offer.basisSourceId !== turn.source_id) {
          throw new FlorenceStoreUnauthorized("A conversational Calendar offer needs this Message");
        }
        validateFamilyCalendarMutation(offer.mutation);
        if (offer.mutation.operation !== "create") {
          throw new FlorenceStoreConflict("A Calendar offer can only add a family event");
        }
        await assertSourcesVisible(sql, turn.household_id, turn.audience, turn.sender_adult_id, [
          offer.basisSourceId,
        ]);
        const [approvalPrompt] = await sql<{ source_id: string }[]>`
          select message.source_id from messages message join sources source on source.id=message.source_id
          where source.parent_source_id=${turn.source_id} and message.channel_id=${turn.channel_id}
            and message.direction='outbound' and message.move_kind in ('message','reply')
            and message.turn_part=0 and message.status='pending'
          order by source.occurred_at,message.source_id limit 1 for share of message,source
        `;
        if (!approvalPrompt) {
          throw new FlorenceStoreConflict("A Calendar suggestion requires its exact group prompt");
        }
        await sql`
          delete from calendar_actions action using messages prompt
          where action.household_id=${turn.household_id} and action.status='offered'
            and prompt.source_id=action.approval_prompt_source_id
            and prompt.channel_id=${turn.channel_id}
        `;
        await sql`
          insert into calendar_actions (
            id,household_id,basis_source_id,approval_prompt_source_id,payload,status,retry_at,created_at
          ) values (${offer.id},${turn.household_id},${offer.basisSourceId},
            ${approvalPrompt.source_id},${sql.json(offer.mutation)},'offered',${handledAt},${handledAt})
        `;
      }

      for (const approval of input.approveCalendarOffers ?? []) {
        assertUuid(approval.offerId, "Calendar offer ID");
        const privateOwnerApproval =
          turn.audience === "private" &&
          turn.visibility === "private" &&
          turn.adult_one_id === turn.sender_adult_id &&
          turn.adult_two_id === null &&
          turn.identity_one_digest !== null &&
          turn.identity_two_digest === null &&
          turn.authority_digest === digestStrings([turn.sender_adult_id, turn.identity_one_digest]) &&
          turn.revoked_at === null &&
          turn.stopped_at === null;
        const groupApproval =
          turn.audience === "group" &&
          groupCalendarAuthority !== undefined &&
          isExactFamilyCalendarAuthority(groupCalendarAuthority, groupChannel, turn.sender_adult_id);
        if (!privateOwnerApproval && !groupApproval) {
          throw new FlorenceStoreUnauthorized(
            "A Calendar approval requires its exact active family group or owner-private thread",
          );
        }
        const approved = await sql`
          update calendar_actions action set status='pending',approval_source_id=${turn.source_id},
            retry_at=${handledAt},last_error=null
          where action.id=${approval.offerId} and action.household_id=${turn.household_id}
            and action.status='offered' and action.approval_prompt_source_id is not null
            and (
              ${groupApproval}
              or (
                ${privateOwnerApproval}
                and exists (
                  select 1 from sources basis_source
                  where basis_source.id=action.basis_source_id
                    and basis_source.household_id=${turn.household_id}
                    and basis_source.kind='calendar' and basis_source.visibility='private'
                    and basis_source.owner_adult_id=${turn.sender_adult_id}
                )
                and exists (
                  select 1 from people owner
                  where owner.id=${turn.sender_adult_id} and owner.household_id=${turn.household_id}
                    and owner.kind='adult' and owner.status='verified'
                    and owner.identity_subject_digest=${turn.identity_one_digest}
                )
              )
            )
            and exists (
              select 1 from messages offer_message join sources offer_source
                on offer_source.id=offer_message.source_id
              where offer_message.source_id=action.approval_prompt_source_id
                and offer_message.channel_id=${turn.channel_id} and offer_message.direction='outbound'
                and offer_message.move_kind in ('message','reply') and offer_message.status='sent'
                and offer_message.turn_part=0
                and (offer_source.occurred_at,offer_source.id)
                  < (${turn.occurred_at},${turn.source_id}::uuid)
            )
          returning action.id
        `;
        if (approved.length !== 1) {
          throw new FlorenceStoreConflict(
            "The Family Calendar offer is no longer awaiting approval in this group",
          );
        }
      }

      for (const action of input.calendarActions ?? []) {
        assertUuid(action.id, "Calendar action ID");
        if (action.basisSourceId !== turn.source_id) {
          throw new FlorenceStoreUnauthorized(
            "A direct Calendar action needs this exact group Message as its basis",
          );
        }
        if (turn.audience !== "group" || !groupCalendarAuthority) {
          throw new FlorenceStoreUnauthorized(
            "A group Calendar action must use Florence's exact Family Calendar",
          );
        }
        validateFamilyCalendarMutation(action.mutation);
        await assertSourcesVisible(sql, turn.household_id, turn.audience, turn.sender_adult_id, [
          action.basisSourceId,
        ]);
        await sql`
          delete from calendar_actions action using messages prompt
          where action.household_id=${turn.household_id} and action.status='offered'
            and prompt.source_id=action.approval_prompt_source_id
            and prompt.channel_id=${turn.channel_id}
        `;
        await sql`
          insert into calendar_actions (
            id,household_id,basis_source_id,approval_source_id,payload,status,retry_at,created_at
          ) values (${action.id},${turn.household_id},${action.basisSourceId},${turn.source_id},
            ${sql.json(action.mutation)},'pending',${handledAt},${handledAt})
        `;
      }

      const handled = await sql`
        update messages set status='handled',handled_at=${handledAt},retry_at=null,last_error=null
        where source_id=${turn.source_id} and status='received' returning source_id
      `;
      if (handled.length !== 1) throw new FlorenceStoreConflict("The inbound turn changed before commit");
      return "committed";
    });
  }

  async retryInbound(input: { sourceId: string; retryAt: string; error: string }): Promise<void> {
    const updated = await this.#sql`
      update messages set retry_at=${instant(input.retryAt)},last_error=${bounded(input.error, 2_000)}
      where source_id=${input.sourceId} and direction='inbound' and status='received' returning source_id
    `;
    if (updated.length !== 1) throw new FlorenceStoreConflict("The inbound message is no longer retryable");
  }

  async readNextOutbound(now: string = new Date().toISOString()): Promise<OutboundMessage | null> {
    const current = instant(now);
    const stale = new Date(current.getTime() - 2 * 60_000);
    await this.#sql`
      update messages set status='failed',last_error='Reaction delivery became ambiguous and was not retried'
      where direction='outbound' and move_kind='reaction' and status='sending' and sending_at<=${stale}
    `;
    await this.#sql`
      update messages set status='failed',sending_at=null,retry_at=null,
        last_error='A stale work cue was suppressed after interruption'
      where direction='outbound' and move_kind in ('message','reply') and status='sending'
        and sending_at<=${stale} and idempotency_key like 'cue:%'
    `;
    await this.#sql`
      update messages cue set status='failed',sending_at=null,retry_at=null,
        last_error='The progress cue was no longer current before delivery'
      where cue.direction='outbound' and cue.status='pending' and cue.idempotency_key like 'cue:%'
        and not exists (
          select 1 from messages inbound
          where inbound.source_id=cue.reply_to_source_id and inbound.direction='inbound'
            and inbound.status='received'
        )
    `;
    await this.#sql`
      update messages set status='pending',sending_at=null,retry_at=${current},
        last_error='Recovering an idempotent Linq send after interruption'
      where direction='outbound' and move_kind in ('message','reply')
        and status='sending' and sending_at<=${stale} and idempotency_key not like 'cue:%'
    `;
    await this.#sql`
      update messages m set status='failed',last_error='Messages authority is no longer active'
      from linq_channels c where c.id=m.channel_id and m.direction='outbound' and m.status='pending'
        and (c.revoked_at is not null or c.stopped_at is not null)
    `;
    await this.#sql`
      update messages m set status='failed',sending_at=null,retry_at=null,
        last_error='Private conflict sharing was turned off before delivery'
      from sources s where s.id=m.source_id and m.direction='outbound' and m.status='pending'
        and exists (
          select 1
          from jsonb_array_elements_text(
            case when jsonb_typeof(s.metadata->'privateConflictOwnerAdultIds')='array'
              then s.metadata->'privateConflictOwnerAdultIds' else '[]'::jsonb end
          ) conflict_owner(adult_id)
          where not exists (
            select 1 from people p where p.household_id=s.household_id
              and p.id=conflict_owner.adult_id::uuid and p.kind='adult' and p.status='verified'
              and p.preferences->'privateConflictBusySharingEnabled'='true'::jsonb
          )
        )
    `;
    await this.#sql`
      update messages later set status='failed',sending_at=null,retry_at=null,
        last_error='An earlier setup message failed before delivery'
      where later.direction='outbound' and later.status='pending'
        and later.idempotency_key like 'founder-handoff:%'
        and exists (
          select 1 from messages earlier
          where earlier.turn_id=later.turn_id and earlier.turn_part<later.turn_part
            and earlier.direction='outbound' and earlier.status='failed'
        )
    `;
    const [row] = await this.#sql<{ source_id: string }[]>`
      select m.source_id from messages m join sources s on s.id=m.source_id
      where m.direction='outbound' and m.status='pending'
        and coalesce(m.retry_at,m.not_before)<=${current}
        and m.idempotency_key not like 'cue:%'
        and (
          m.idempotency_key not like 'founder-handoff:%'
          or not exists (
            select 1 from messages earlier
            where earlier.turn_id=m.turn_id and earlier.turn_part<m.turn_part
              and earlier.direction='outbound' and earlier.status<>'sent'
          )
        )
      order by coalesce(m.retry_at,m.not_before),s.occurred_at,m.turn_part,m.source_id limit 1
    `;
    return row ? this.#readOutbound(row.source_id) : null;
  }

  async beginOutbound(input: { sourceId: string; now: string }): Promise<OutboundMessage | null> {
    const started = await this.#sql`
      update messages m set status='sending',sending_at=${instant(input.now)},last_error=null
      from linq_channels c,sources s where m.source_id=${input.sourceId} and m.channel_id=c.id
        and s.id=m.source_id
        and m.direction='outbound' and m.status='pending'
        and c.revoked_at is null and c.stopped_at is null
        and (
          m.idempotency_key not like 'founder-handoff:%'
          or not exists (
            select 1 from messages earlier
            where earlier.turn_id=m.turn_id and earlier.turn_part<m.turn_part
              and earlier.direction='outbound' and earlier.status<>'sent'
          )
        )
        and not exists (
          select 1
          from jsonb_array_elements_text(
            case when jsonb_typeof(s.metadata->'privateConflictOwnerAdultIds')='array'
              then s.metadata->'privateConflictOwnerAdultIds' else '[]'::jsonb end
          ) conflict_owner(adult_id)
          where not exists (
            select 1 from people p where p.household_id=s.household_id
              and p.id=conflict_owner.adult_id::uuid and p.kind='adult' and p.status='verified'
              and p.preferences->'privateConflictBusySharingEnabled'='true'::jsonb
          )
        ) returning m.source_id
    `;
    return started.length === 1 ? this.#readOutbound(input.sourceId) : null;
  }

  async outboundSendIsCurrent(sourceId: string): Promise<boolean> {
    assertUuid(sourceId, "Outbound source ID");
    const rows = await this.#sql`
      select 1 from messages outbound
      join linq_channels channel on channel.id=outbound.channel_id
      join sources source on source.id=outbound.source_id
      where outbound.source_id=${sourceId} and outbound.direction='outbound'
        and outbound.status='sending'
        and channel.revoked_at is null and channel.stopped_at is null
        and (
          outbound.idempotency_key not like 'cue:%'
          or exists (
            select 1 from messages inbound
            where inbound.source_id=outbound.reply_to_source_id
              and inbound.direction='inbound' and inbound.status='received'
          )
        )
        and (
          source.metadata->>'familyWorkId' is null
          or exists (
            select 1 from proactive_work work
            where work.id::text=source.metadata->>'familyWorkId' and work.kind='family_task'
              and work.task_state->>'generation'=source.metadata->>'familyWorkGeneration'
              and work.task_state->>'progressRevision'=
                source.metadata->>'familyWorkProgressRevision'
              and (
                (source.metadata->>'familyWorkDeliveryKind'='progress' and work.status='active')
                or (source.metadata->>'familyWorkDeliveryKind'='waiting' and work.status='paused')
                or (source.metadata->>'familyWorkDeliveryKind'='terminal'
                  and work.status='delivering')
              )
          )
        )
    `;
    return rows.length === 1;
  }

  async outboundDeliveryStatus(sourceId: string): Promise<"pending" | "sending" | "sent" | "failed" | null> {
    assertUuid(sourceId, "Outbound source ID");
    const [row] = await this.#sql<{ status: "pending" | "sending" | "sent" | "failed" }[]>`
      select status from messages
      where source_id=${sourceId} and direction='outbound'
    `;
    return row?.status ?? null;
  }

  async failSendingOutbound(sourceId: string, error: string): Promise<void> {
    assertUuid(sourceId, "Outbound source ID");
    await this.#sql`
      update messages set status='failed',sending_at=null,retry_at=null,
        last_error=${bounded(required(error, "Outbound failure"), 2_000)}
      where source_id=${sourceId} and direction='outbound' and status='sending'
    `;
  }

  async completeOutbound(input: {
    sourceId: string;
    providerMessageId: string;
    receiptDetail?: JsonObject | null;
    sentAt: string;
  }): Promise<void> {
    const sentAt = instant(input.sentAt);
    await this.#sql.begin(async (sql) => {
      const [current] = await sql<
        {
          status: "pending" | "sending" | "sent" | "failed";
          move_kind: "message" | "reply" | "reaction";
          provider_message_id: string | null;
          receipt_detail: JsonValue | null;
          not_before: Date;
          source_occurred_at: Date;
        }[]
      >`
        select m.status,m.move_kind,m.provider_message_id,m.receipt_detail,m.not_before,
          s.occurred_at as source_occurred_at
        from messages m join sources s on s.id=m.source_id
        where m.source_id=${input.sourceId} and m.direction='outbound' for update of m
      `;
      if (!current) throw new FlorenceStoreConflict("The outbound message does not exist");
      const stagedAt = Math.max(current.source_occurred_at.getTime(), current.not_before.getTime());
      if (sentAt.getTime() < stagedAt - LINQ_RECEIPT_CLOCK_SKEW_MS) {
        throw new FlorenceStoreConflict("The Linq receipt predates this staged outbound message");
      }
      if (
        current.move_kind !== "reaction" &&
        current.provider_message_id !== null &&
        current.provider_message_id !== input.providerMessageId
      ) {
        throw new FlorenceStoreConflict("The Linq receipt conflicts with the committed message");
      }
      const receiptDetail = mergeLinqAcceptance(
        current.receipt_detail,
        input.providerMessageId,
        input.sentAt,
        input.receiptDetail ?? {},
      );
      if (current.status === "sent") {
        await sql`
          update messages set receipt_detail=${sql.json(receiptDetail)} where source_id=${input.sourceId}
        `;
        await completeDeliveredOneShotReminder(sql, input.sourceId);
        await completeDeliveredFamilyWorkTerminal(sql, input.sourceId);
        return;
      }
      const reactionConfirmed =
        current.move_kind === "reaction" && receiptDetail.providerState === "reaction_added";
      if (current.status === "failed") {
        if (!reactionConfirmed) {
          await sql`
            update messages set receipt_detail=${sql.json(receiptDetail)} where source_id=${input.sourceId}
          `;
          return;
        }
      } else if (current.status !== "sending") {
        throw new FlorenceStoreConflict("The outbound message was not begun");
      }
      await sql`
        update messages set status='sent',
          provider_message_id=${current.provider_message_id ?? input.providerMessageId},sent_at=${sentAt},
          receipt_detail=${sql.json(receiptDetail)},sending_at=null,retry_at=null,last_error=null
        where source_id=${input.sourceId}
      `;
      await completeDeliveredOneShotReminder(sql, input.sourceId);
      await completeDeliveredFamilyWorkTerminal(sql, input.sourceId);
    });
  }

  async retryOutbound(input: { sourceId: string; retryAt: string | null; error: string }): Promise<void> {
    const [current] = await this.#sql<{ move_kind: "message" | "reply" | "reaction" }[]>`
      select move_kind from messages where source_id=${input.sourceId}
        and direction='outbound' and status='sending'
    `;
    if (!current) throw new FlorenceStoreConflict("The outbound message is no longer retryable");
    const retryAt = input.retryAt;
    const updated = await this.#sql`
      update messages set status=${retryAt ? "pending" : "failed"},sending_at=null,
        retry_at=${retryAt ? instant(retryAt) : null},last_error=${bounded(input.error, 2_000)}
      where source_id=${input.sourceId} and direction='outbound' and status='sending' returning source_id
    `;
    if (updated.length !== 1) throw new FlorenceStoreConflict("The outbound message is no longer retryable");
  }

  async recordLinqObservation(input: LinqOutboundObservation): Promise<LinqObservationResult> {
    const occurredAt = instant(input.occurredAt);
    required(input.providerEventId, "Linq event ID");
    required(input.providerConversationId, "Linq conversation ID");
    required(input.traceId, "Linq trace ID");
    if (input.kind === "reaction") {
      required(input.targetProviderMessageId, "Linq reaction target message ID");
      if (!input.isFromMe || input.partIndex !== 0) return "unmatched";
    } else {
      required(input.providerMessageId, "Linq message ID");
    }
    return this.#sql.begin(async (sql) => {
      const rows =
        input.kind === "message_status"
          ? await sql<LinqObservationRow[]>`
              select m.source_id,m.status,m.move_kind,m.provider_message_id,m.sent_at,m.receipt_detail,
                m.not_before,s.occurred_at as source_occurred_at
              from messages m join linq_channels c on c.id=m.channel_id
              join sources s on s.id=m.source_id
              where m.direction='outbound' and m.move_kind in ('message','reply')
                and c.provider_conversation_id=${input.providerConversationId}
                and (m.provider_message_id=${input.providerMessageId}
                  or (${input.idempotencyKey}::text is not null and m.idempotency_key=${input.idempotencyKey}))
              for update of m
            `
          : await sql<LinqObservationRow[]>`
              select reaction.source_id,reaction.status,reaction.move_kind,reaction.provider_message_id,
                reaction.sent_at,reaction.receipt_detail,reaction.not_before,
                source.occurred_at as source_occurred_at
              from messages reaction join linq_channels c on c.id=reaction.channel_id
              join sources source on source.id=reaction.source_id
              join messages target on target.source_id=reaction.reply_to_source_id
                and target.channel_id=reaction.channel_id
              where reaction.direction='outbound' and reaction.move_kind='reaction'
                and c.provider_conversation_id=${input.providerConversationId}
                and target.provider_message_id=${input.targetProviderMessageId}
                and reaction.reaction=${input.reaction}
              for update of reaction
            `;
      if (rows.length !== 1) return "unmatched";
      const current = rows[0];
      if (!current) return "unmatched";
      const stagedAt = Math.max(current.source_occurred_at.getTime(), current.not_before.getTime());
      if (occurredAt.getTime() < stagedAt - LINQ_RECEIPT_CLOCK_SKEW_MS) return "unmatched";
      if (
        input.kind === "message_status" &&
        current.provider_message_id !== null &&
        current.provider_message_id !== input.providerMessageId
      ) {
        return "unmatched";
      }
      const merged = mergeLinqObservation(current.receipt_detail, input);
      if (merged.duplicate) return "duplicate";
      const succeeded =
        merged.providerState === "sent" ||
        merged.providerState === "delivered" ||
        merged.providerState === "read" ||
        merged.providerState === "reaction_added";
      const failed = merged.providerState === "failed" || merged.providerState === "reaction_removed";
      const status = succeeded ? "sent" : failed ? "failed" : current.status;
      const providerMessageId =
        current.provider_message_id ??
        (input.kind === "message_status"
          ? input.providerMessageId
          : `reaction-event:${input.providerEventId}`);
      await sql`
        update messages set status=${status},provider_message_id=${providerMessageId},
          sent_at=${succeeded ? (current.sent_at ?? occurredAt) : current.sent_at},
          receipt_detail=${sql.json(merged.detail)},sending_at=null,retry_at=null,
          last_error=${failed ? merged.lastError : null}
        where source_id=${current.source_id}
      `;
      return "applied";
    });
  }

  async readNextPartnerInvitation(
    now: string = new Date().toISOString(),
  ): Promise<ApprovedPartnerInvitation | null> {
    const dueAt = instant(now);
    const [row] = await this.#sql<
      {
        household_id: string;
        founder_adult_id: string;
        founder_channel_id: string;
        founder_provider_conversation_id: string;
        partner_adult_id: string;
        partner_first_name: string;
        partner_phone_number: string;
        approval_source_id: string;
        approved_at: Date;
      }[]
    >`
      select partner.household_id,founder.id as founder_adult_id,
        channel.id as founder_channel_id,
        channel.provider_conversation_id as founder_provider_conversation_id,
        partner.id as partner_adult_id,partner.profile->>'firstName' as partner_first_name,
        partner.profile->>'phoneNumber' as partner_phone_number,
        partner.invitation_approval_source_id as approval_source_id,
        partner.invitation_approved_at as approved_at
      from people partner
      join messages approval on approval.source_id=partner.invitation_approval_source_id
      join linq_channels channel on channel.id=approval.channel_id
      join people founder on founder.household_id=partner.household_id
        and founder.id=approval.sender_adult_id
      where partner.kind='adult' and partner.role='steward' and partner.adult_slot=2
        and partner.status='planned' and partner.identity_subject_digest is null
        and partner.invitation_approval_source_id is not null
        and partner.invitation_approved_at is not null
        and partner.invitation_retry_at<=${dueAt}
        and partner.invitation_digest is null and partner.invitation_consumed_at is null
        and (
          (
            partner.invitation_expires_at is null and partner.messages_address is null
            and partner.invitation_conversation_id is null
            and partner.invitation_identity_digest is null
            and partner.invitation_message_id is null and partner.invitation_issued_at is null
          )
          or
          (
            partner.messages_address=partner.profile->>'phoneNumber'
            and partner.invitation_conversation_id is not null
            and partner.invitation_identity_digest is not null
            and partner.invitation_message_id is not null and partner.invitation_issued_at is not null
          )
        )
        and founder.kind='adult' and founder.role='steward' and founder.adult_slot=1
        and founder.status='verified'
        and approval.direction='inbound' and approval.move_kind in ('message','reply')
        and approval.status='handled'
        and channel.audience='private' and channel.adult_one_id=founder.id
        and channel.revoked_at is null and channel.stopped_at is null
        and nullif(partner.profile->>'firstName','') is not null
        and (partner.profile->>'phoneNumber') ~ '^[+][1-9][0-9]{7,14}$'
      order by partner.invitation_retry_at,partner.id
      limit 1
    `;
    return row
      ? {
          householdId: row.household_id,
          founderAdultId: row.founder_adult_id,
          founderChannelId: row.founder_channel_id,
          founderProviderConversationId: row.founder_provider_conversation_id,
          partnerAdultId: row.partner_adult_id,
          partnerFirstName: row.partner_first_name,
          partnerPhoneNumber: row.partner_phone_number,
          approvalSourceId: row.approval_source_id,
          approvedAt: row.approved_at.toISOString(),
        }
      : null;
  }

  async retryPartnerInvitation(input: { adultId: string; retryAt: string; error: string }): Promise<void> {
    const updated = await this.#sql`
      update people set invitation_retry_at=${instant(input.retryAt)},
        invitation_last_error=${bounded(input.error, 500)},updated_at=now()
      where id=${input.adultId} and kind='adult' and adult_slot=2 and status='planned'
        and invitation_approval_source_id is not null and invitation_digest is null
        and invitation_consumed_at is null
      returning id
    `;
    if (updated.length !== 1) {
      throw new FlorenceStoreConflict("The partner invitation is no longer pending");
    }
  }

  async readNextCalendarAction(
    now: string = new Date().toISOString(),
  ): Promise<ApprovedCalendarAction | null> {
    const dueAt = instant(now);
    return this.#sql.begin(async (sql) => {
      const [row] = await sql<CalendarActionAuthorityRow[]>`
        select a.id,a.status,a.household_id,a.basis_source_id,a.approval_source_id,
               a.approval_prompt_source_id,a.google_action_key,
               coalesce(basis.kind in ('gmail','calendar') and basis.visibility='private',false)
                 as legacy_google_review_basis,
               a.payload,a.provider_event_id,a.provider_etag,
               a.committed_at,a.retry_at,message.channel_id,message.direction,
               message.sender_adult_id,c.audience as channel_audience,c.provider_conversation_id,
               c.adult_one_id,c.identity_one_digest,c.adult_two_id,c.identity_two_digest,
               c.authority_digest,c.bound_at,c.revoked_at,c.stopped_at
        from calendar_actions a left join messages message on message.source_id=a.approval_source_id
        left join linq_channels c on c.id=message.channel_id
        left join sources basis on basis.id=a.basis_source_id
        where a.status='pending' and a.retry_at<=${dueAt}
        order by a.retry_at,a.created_at,a.id limit 1
        for update of a skip locked
      `;
      if (!row) return null;
      const retireAction = row.google_action_key
        ? (await readResolvedGoogleActionKeys(sql, row.household_id)).has(row.google_action_key)
        : row.legacy_google_review_basis && row.approval_source_id === null;
      if (retireAction) {
        // Tagged work already handled by the family must never be reclaimed after a provider
        // failure. Untagged, unapproved private-Google actions predate stable docket identity, so
        // retire them and let the next authoritative review recreate anything still relevant.
        await sql`
          delete from calendar_actions
          where id=${row.id} and status='pending' and retry_at<=${dueAt}
        `;
        return null;
      }
      const familyCalendarAuthority = await readFamilyCalendarAuthority(sql, row.household_id);
      const mutation = familyCalendarMutation(row.payload);
      let personalCalendarOwnerApproved = false;
      if (row.approval_source_id !== null) {
        const approvalChannel = calendarApprovalChannel(row);
        const groupApproval =
          familyCalendarAuthority !== undefined &&
          isExactFamilyCalendarAuthority(familyCalendarAuthority, approvalChannel, row.sender_adult_id ?? "");
        const personalOwnerApproval =
          row.sender_adult_id !== null &&
          familyCalendarAuthority !== undefined &&
          (await isExactPersonalCalendarApprovalAuthority(
            sql,
            row,
            familyCalendarAuthority,
            approvalChannel,
            row.sender_adult_id,
          ));
        if (
          row.direction !== "inbound" ||
          row.sender_adult_id === null ||
          !familyCalendarAuthority ||
          (!groupApproval && !personalOwnerApproval) ||
          !(await isCalendarAdultApprovalBound(sql, row))
        ) {
          await failCalendarAuthority(sql, row.id, dueAt, "Calendar authority is no longer active");
          return null;
        }
        personalCalendarOwnerApproved = personalOwnerApproval;
      } else {
        const [groupChannel] = await sql<ChannelRow[]>`
          select * from linq_channels where household_id=${row.household_id}
            and audience='group' and adult_two_id is not null
            and revoked_at is null and stopped_at is null
          order by bound_at,id limit 1 for share
        `;
        if (
          !row.basis_source_id ||
          row.approval_prompt_source_id !== null ||
          !familyCalendarAuthority ||
          !groupChannel ||
          !familyCalendarAuthority.founder_adult_id ||
          !isExactFamilyCalendarAuthority(
            familyCalendarAuthority,
            groupChannel,
            familyCalendarAuthority.founder_adult_id,
          ) ||
          mutation.operation !== "create" ||
          !familyCalendarAutomaticCreationEnabled(familyCalendarAuthority) ||
          !(await isOfficialPrivateGmailBasis(
            sql,
            row.household_id,
            row.basis_source_id,
            familyCalendarAuthority,
          ))
        ) {
          await failCalendarAuthority(
            sql,
            row.id,
            dueAt,
            "Automatic family Calendar authority is no longer active",
          );
          return null;
        }
      }
      const credential = activeFamilyCalendarCredential(familyCalendarAuthority);
      if (!credential) return null;
      const claimed = await sql<{ id: string }[]>`
        update calendar_actions set
          retry_at=${new Date(dueAt.getTime() + CALENDAR_ACTION_EXECUTION_LEASE_MS)},
          last_error=${CALENDAR_ACTION_EXECUTION_MARKER}
        where id=${row.id} and status='pending' and retry_at<=${dueAt}
        returning id
      `;
      if (claimed.length !== 1) return null;
      return {
        actionId: row.id,
        householdId: row.household_id,
        connectionId: credential.connectionId,
        ownerAdultId: credential.ownerAdultId,
        calendarId: credential.calendarId,
        mutation,
        personalCalendarOwnerApproved,
      };
    });
  }

  async completeCalendarAction(input: {
    id: string;
    providerEventId: string;
    providerRevision: string | null;
    confirmationText: string;
    committedAt: string;
  }): Promise<void> {
    const providerEventId = bounded(required(input.providerEventId, "Google Calendar event ID"), 1_024);
    const providerRevision = input.providerRevision
      ? bounded(required(input.providerRevision, "Google Calendar revision"), 500)
      : `cancelled:${providerEventId}`;
    const committedAt = instant(input.committedAt);
    const confirmationText = bounded(required(input.confirmationText, "Calendar confirmation"), 10_000);
    await this.#sql.begin(async (sql) => {
      const current = await readCalendarActionAuthority(sql, input.id);
      if (!current) throw new FlorenceStoreConflict("The Calendar action does not exist");
      const familyCalendarAuthority = await readFamilyCalendarAuthority(sql, current.household_id);
      const notification = await calendarActionNotification(sql, current, familyCalendarAuthority, "success");
      const mutation = familyCalendarMutation(current.payload);
      if (mutation.operation !== "create" && mutation.target.providerEventId !== providerEventId) {
        throw new FlorenceStoreConflict("Google Calendar returned a different event");
      }
      if (current.status === "committed") {
        if (current.provider_event_id !== providerEventId || current.provider_etag !== providerRevision) {
          throw new FlorenceStoreConflict("The Calendar result conflicts with the committed action");
        }
      } else if (current.status !== "pending") {
        throw new FlorenceStoreConflict("The Calendar action cannot be completed");
      } else {
        await sql`
          update calendar_actions set status='committed',
            provider_event_id=${providerEventId},provider_etag=${providerRevision},
            committed_at=${committedAt},last_error=null
          where id=${input.id}
        `;
      }
      const confirmationAt = current.committed_at ?? committedAt;
      if (notification) {
        await insertOutbound(sql, {
          sourceId: deterministicUuid(`calendar-confirmation\0${current.id}`),
          idempotencyKey: `calendar-confirmation:${current.id}`,
          moveKind: notification.replyToSourceId ? "reply" : "message",
          text: confirmationText,
          reaction: null,
          replyToSourceId: notification.replyToSourceId,
          turnId: deterministicUuid(`calendar-confirmation-turn\0${current.id}`),
          turnPart: 0,
          notBefore: confirmationAt.toISOString(),
          householdId: current.household_id,
          channelId: notification.channel.id,
          visibility: notification.visibility,
          ownerAdultId: notification.ownerAdultId,
          occurredAt: confirmationAt,
        });
      }
    });
  }

  async failCalendarAction(input: {
    id: string;
    error: string;
    failureText: string;
    failedAt: string;
  }): Promise<void> {
    const failedAt = instant(input.failedAt);
    const error = bounded(required(input.error, "Calendar failure"), 2_000);
    const failureText = bounded(required(input.failureText, "Calendar failure message"), 10_000);
    await this.#sql.begin(async (sql) => {
      const current = await readCalendarActionAuthority(sql, input.id);
      if (!current) throw new FlorenceStoreConflict("The Calendar action does not exist");
      const familyCalendarAuthority = await readFamilyCalendarAuthority(sql, current.household_id);
      const notification = await calendarActionNotification(sql, current, familyCalendarAuthority, "failure");
      if (current.status === "pending") {
        await sql`
          update calendar_actions set status='failed',retry_at=${failedAt},last_error=${error}
          where id=${input.id}
        `;
      } else if (current.status !== "failed") {
        throw new FlorenceStoreConflict("The Calendar action cannot fail from its current state");
      }
      const notificationAt = current.status === "failed" ? current.retry_at : failedAt;
      if (notification) {
        await insertOutbound(sql, {
          sourceId: deterministicUuid(`calendar-failure\0${current.id}`),
          idempotencyKey: `calendar-failure:${current.id}`,
          moveKind: notification.replyToSourceId ? "reply" : "message",
          text: failureText,
          reaction: null,
          replyToSourceId: notification.replyToSourceId,
          turnId: deterministicUuid(`calendar-failure-turn\0${current.id}`),
          turnPart: 0,
          notBefore: notificationAt.toISOString(),
          householdId: current.household_id,
          channelId: notification.channel.id,
          visibility: notification.visibility,
          ownerAdultId: notification.ownerAdultId,
          occurredAt: notificationAt,
        });
      }
    });
  }

  async retryCalendarAction(input: { id: string; retryAt: string; error: string }): Promise<void> {
    const updated = await this.#sql`
      update calendar_actions set retry_at=${instant(input.retryAt)},last_error=${bounded(input.error, 2_000)}
      where id=${input.id} and status='pending' returning id
    `;
    if (updated.length !== 1) throw new FlorenceStoreConflict("The Calendar action is no longer retryable");
  }

  async createPending(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
    stateDigest: string;
    sessionBindingDigest: string;
    stateExpiresAt: string;
    now: string;
  }): Promise<GoogleConnectionView> {
    assertDigest(input.stateDigest, "Google OAuth state");
    assertDigest(input.sessionBindingDigest, "Google OAuth session binding");
    return this.#sql.begin(async (sql) => {
      const now = instant(input.now);
      const [adult] = await sql<{ id: string }[]>`
        select id from people where household_id=${input.householdId} and id=${input.ownerAdultId}
          and kind='adult' and status='verified' for update
      `;
      if (!adult) throw new FlorenceStoreUnauthorized();
      // Only the newest OAuth attempt may be consumed. Superseding a pending attempt is safe: it
      // never touches the still-active credential or anything Florence derived from it.
      await sql`
        update google_connections set status='disconnected',refresh_token_envelope=null,
          session_binding_digest=null,state_consumed_at=null,last_error=null,updated_at=${now}
        where household_id=${input.householdId} and owner_adult_id=${input.ownerAdultId}
          and status='pending'
      `;
      const [row] = await sql<GoogleConnectionRow[]>`
        insert into google_connections (
          id,household_id,owner_adult_id,status,state_digest,session_binding_digest,state_expires_at,
          created_at,updated_at
        ) values (${input.connectionId},${input.householdId},${input.ownerAdultId},'pending',${input.stateDigest},
          ${input.sessionBindingDigest},${instant(input.stateExpiresAt)},${now},${now})
        returning *
      `;
      if (!row) throw new Error("The Google connection was not created");
      return googleConnectionView(row);
    });
  }

  async consumePendingState(input: {
    stateDigest: string;
    sessionBindingDigest: string;
    now: string;
  }): Promise<PendingGoogleConnection | null> {
    const [row] = await this.#sql<GoogleConnectionRow[]>`
      update google_connections set state_consumed_at=${instant(input.now)},updated_at=${instant(input.now)}
      where status='pending' and state_digest=${input.stateDigest}
        and session_binding_digest=${input.sessionBindingDigest} and state_consumed_at is null
        and state_expires_at>=${instant(input.now)} returning *
    `;
    return row?.session_binding_digest
      ? {
          connectionId: row.id,
          householdId: row.household_id,
          ownerAdultId: row.owner_adult_id,
          stateDigest: row.state_digest,
          sessionBindingDigest: row.session_binding_digest,
        }
      : null;
  }

  async activate(input: {
    connectionId: string;
    stateDigest: string;
    googleSubjectDigest: string;
    emailLabel: string;
    grantedScopes: readonly GoogleScope[];
    refreshTokenEnvelope: string;
    now: string;
  }): Promise<GoogleConnectionView> {
    assertDigest(input.googleSubjectDigest, "Google identity");
    const now = instant(input.now);
    return this.#sql.begin(async (sql) => {
      const [candidate] = await sql<GoogleConnectionRow[]>`
        select * from google_connections
        where id=${input.connectionId} and state_digest=${input.stateDigest}
          and state_consumed_at is not null and status='pending'
      `;
      if (!candidate) throw new FlorenceStoreConflict("Google OAuth state is no longer current");
      const [owner] = await sql<{ id: string }[]>`
        select id from people where household_id=${candidate.household_id}
          and id=${candidate.owner_adult_id} and kind='adult' and status='verified' for update
      `;
      if (!owner) throw new FlorenceStoreUnauthorized();
      const connections = await sql<GoogleConnectionRow[]>`
        select * from google_connections
        where household_id=${candidate.household_id} and owner_adult_id=${candidate.owner_adult_id}
        order by created_at,id for update
      `;
      const pending = connections.find(
        (connection) =>
          connection.id === input.connectionId &&
          connection.state_digest === input.stateDigest &&
          connection.state_consumed_at !== null &&
          connection.status === "pending",
      );
      if (!pending) throw new FlorenceStoreConflict("Google OAuth state is no longer current");
      const priorConnections = connections.filter((connection) => connection.id !== pending.id);
      if (
        priorConnections.some(
          (connection) =>
            connection.google_subject_digest !== null &&
            connection.google_subject_digest !== input.googleSubjectDigest,
        )
      ) {
        throw Object.assign(
          new FlorenceStoreConflict(
            "Reconnect the same Google account, or delete Florence’s Google-derived data before switching accounts",
          ),
          { code: "google_identity_conflict" },
        );
      }
      await sql`
        update google_connections set status='disconnected',refresh_token_envelope=null,updated_at=${now}
        where household_id=${pending.household_id} and owner_adult_id=${pending.owner_adult_id}
          and id<>${pending.id} and status='active'
      `;
      const [row] = await sql<GoogleConnectionRow[]>`
        update google_connections set status='active',google_subject_digest=${input.googleSubjectDigest},
          email_label=${required(input.emailLabel, "Google account email")},
          granted_scopes=${sql.array([...input.grantedScopes])},
          refresh_token_envelope=${required(input.refreshTokenEnvelope, "Google refresh token envelope")},
          session_binding_digest=null,last_error=null,updated_at=${now}
        where id=${pending.id} and status='pending' returning *
      `;
      if (!row) throw new FlorenceStoreConflict("Google OAuth state is no longer current");
      const [binding] = await sql<
        {
          adult_slot: 1 | 2 | null;
          founder_connection_id: string | null;
          partner_connection_id: string | null;
          founder_connection_status: GoogleConnectionStatus | null;
          partner_connection_status: GoogleConnectionStatus | null;
          founder_subject_digest: string | null;
          partner_subject_digest: string | null;
        }[]
      >`
        select person.adult_slot,
          household.family_calendar_owner_connection_id as founder_connection_id,
          household.family_calendar_partner_connection_id as partner_connection_id,
          founder.status as founder_connection_status,
          partner.status as partner_connection_status,
          founder.google_subject_digest as founder_subject_digest,
          partner.google_subject_digest as partner_subject_digest
        from people person join households household on household.id=person.household_id
        left join google_connections founder
          on founder.id=household.family_calendar_owner_connection_id
        left join google_connections partner
          on partner.id=household.family_calendar_partner_connection_id
        where person.id=${row.owner_adult_id} and person.household_id=${row.household_id}
          and person.kind='adult' and person.status='verified'
        for update of household
      `;
      if (
        binding?.adult_slot === 1 &&
        binding.founder_connection_id !== null &&
        binding.founder_connection_status === "disconnected" &&
        (binding.founder_subject_digest === null ||
          binding.founder_subject_digest === input.googleSubjectDigest)
      ) {
        await sql`
          update households set family_calendar_owner_connection_id=${row.id},updated_at=${now}
          where id=${row.household_id}
        `;
      } else if (
        binding?.adult_slot === 2 &&
        binding.partner_connection_id !== null &&
        binding.partner_connection_status === "disconnected" &&
        (binding.partner_subject_digest === null ||
          binding.partner_subject_digest === input.googleSubjectDigest)
      ) {
        await sql`
          update households set family_calendar_partner_connection_id=${row.id},updated_at=${now}
          where id=${row.household_id}
        `;
      }
      if (input.grantedScopes.includes(COMPLETE_CALENDAR_HISTORY_SCOPE)) {
        await sql`
          update proactive_work set status='active',next_check_at=${now},created_at=${now},
            briefing_candidates='[]'::jsonb,last_error=null
          where household_id=${row.household_id} and owner_adult_id=${row.owner_adult_id}
            and kind='initial_private_review'
        `;
        await sql`
          update proactive_work set status='paused',next_check_at=null,
            last_error='Paused while Florence rebuilds complete Google coverage'
          where household_id=${row.household_id} and owner_adult_id=${row.owner_adult_id}
            and kind='personal_google_poll' and status='active'
        `;
      }
      return googleConnectionView(row);
    });
  }

  async stageFounderHandoff(input: {
    householdId: string;
    adultId: string;
    channelId: string;
    providerConversationId: string;
    texts: readonly string[];
    occurredAt: string;
  }): Promise<string> {
    if (input.texts.length < 1 || input.texts.length > 3) {
      throw new FlorenceStoreConflict("The founder handoff needs one to three message bubbles");
    }
    const texts = input.texts.map((text, index) => required(text, `Founder handoff bubble ${index + 1}`));
    const providerConversationId = required(input.providerConversationId, "Linq conversation ID");
    const occurredAt = instant(input.occurredAt);
    const completionSourceId = deterministicUuid(
      `founder-handoff\0${input.householdId}\0${input.adultId}\0${0}`,
    );

    await this.#sql.begin(async (sql) => {
      const handoff = founderHandoffIdentity(
        input.householdId,
        input.adultId,
        await householdLinqIncarnationScope(sql, input.householdId),
      );
      const [channel] = await sql<ChannelRow[]>`
        select c.* from linq_channels c
        join people p on p.household_id=c.household_id and p.id=c.adult_one_id
        where c.id=${input.channelId} and c.household_id=${input.householdId}
          and c.audience='private' and c.adult_one_id=${input.adultId}
          and c.provider_conversation_id=${providerConversationId}
          and c.revoked_at is null and c.stopped_at is null
          and p.kind='adult' and p.status='verified' and p.guardian_attested_at is not null
          and nullif(p.profile->>'onboardingCompletedAt','') is not null
        for update of c
      `;
      if (!channel) {
        throw new FlorenceStoreUnauthorized(
          "The founder handoff requires completed onboarding in the verified adult's private thread",
        );
      }
      const [google] = await sql<{ id: string }[]>`
        select id from google_connections where household_id=${input.householdId}
          and owner_adult_id=${input.adultId} and status='active'
        order by created_at,id limit 1 for share
      `;
      if (!google) {
        throw new FlorenceStoreUnauthorized(
          "The founder handoff requires the adult's active Google connection",
        );
      }

      const existing = await sql<
        {
          source_id: string;
          channel_id: string;
          move_kind: "message" | "reply" | "reaction";
          text: string | null;
          reply_to_source_id: string | null;
          turn_part: -1 | 0 | 1 | 2;
          idempotency_key: string | null;
        }[]
      >`
        select source_id,channel_id,move_kind,text,reply_to_source_id,turn_part,idempotency_key
        from messages where turn_id=${handoff.turnId} order by turn_part for update
      `;
      if (existing.length > 0) {
        if (
          existing.length > 3 ||
          existing.some((message, index) => {
            const part = handoff.part(index);
            return (
              message.source_id !== part.sourceId ||
              message.channel_id !== channel.id ||
              message.move_kind !== "message" ||
              message.text !== texts[index] ||
              message.reply_to_source_id !== null ||
              message.turn_part !== index ||
              message.idempotency_key !== part.idempotencyKey
            );
          })
        ) {
          throw new FlorenceStoreConflict("The founder handoff was already staged with different content");
        }
        return;
      }

      for (const [index, text] of texts.entries()) {
        const part = handoff.part(index);
        await insertOutbound(sql, {
          sourceId: part.sourceId,
          idempotencyKey: part.rawIdempotencyKey,
          moveKind: "message",
          text,
          turnId: handoff.turnId,
          turnPart: index as 0 | 1 | 2,
          notBefore: new Date(occurredAt.getTime() + index * 700).toISOString(),
          householdId: input.householdId,
          channelId: channel.id,
          visibility: "private",
          ownerAdultId: input.adultId,
          occurredAt,
        });
      }
    });
    return completionSourceId;
  }

  async markPendingFailure(input: {
    connectionId: string;
    stateDigest: string;
    error: string;
    now: string;
  }): Promise<void> {
    await this.#sql`
      update google_connections set last_error=${bounded(input.error, 2_000)},updated_at=${instant(input.now)}
      where id=${input.connectionId} and state_digest=${input.stateDigest} and status='pending'
    `;
  }

  async listActive(input: {
    householdId: string;
    ownerAdultId: string;
  }): Promise<readonly GoogleConnectionView[]> {
    const rows = await this.#sql<GoogleConnectionRow[]>`
      select * from google_connections where household_id=${input.householdId}
        and owner_adult_id=${input.ownerAdultId} and status='active' order by created_at,id
    `;
    return rows.map(googleConnectionView);
  }

  async disconnect(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
    notifyReconnect?: boolean;
    now: string;
  }): Promise<{ view: GoogleConnectionView; refreshTokenEnvelope: string | null } | null> {
    const now = instant(input.now);
    return this.#sql.begin(async (sql) => {
      const [owner] = await sql<{ id: string }[]>`
        select id from people where household_id=${input.householdId} and id=${input.ownerAdultId}
          and kind='adult' and status='verified' for update
      `;
      if (!owner) return null;
      const connections = await sql<GoogleConnectionRow[]>`
        select * from google_connections where household_id=${input.householdId}
          and owner_adult_id=${input.ownerAdultId} order by created_at,id for update
      `;
      const current = connections.find(
        (connection) => connection.id === input.connectionId && connection.status !== "disconnected",
      );
      if (!current) return null;
      const lineage =
        current.google_subject_digest === null
          ? [current]
          : connections.filter(
              (connection) => connection.google_subject_digest === current.google_subject_digest,
            );
      const lineageConnectionIds = unique(lineage.map((connection) => connection.id));
      const queuedGoogleMessages = await googleDerivedUnsentMessageSourceIds(sql, {
        householdId: input.householdId,
        adultId: input.ownerAdultId,
        connectionIds: lineageConnectionIds,
        statuses: ["pending"],
      });
      if (queuedGoogleMessages.length > 0) {
        await sql`
          update messages set status='failed',sending_at=null,retry_at=null,
            last_error='Google was disconnected before this message was delivered'
          where source_id in ${sql(queuedGoogleMessages)} and direction='outbound'
            and status='pending'
        `;
        await sql`
          update sources set metadata=jsonb_set(
            metadata,'{googleConnectionIds}',${sql.json(lineageConnectionIds)},true
          )
          where id in ${sql(queuedGoogleMessages)}
        `;
      }
      await sql`
        delete from calendar_actions action using sources basis
        where action.household_id=${input.householdId}
          and action.basis_source_id=basis.id
          and action.status in ('offered','pending','failed')
          and basis.kind in ('gmail','calendar','google_file')
          and basis.metadata->>'connectionId' in ${sql(lineageConnectionIds)}
      `;
      await sql`
        update google_connections set status='disconnected',refresh_token_envelope=null,
          session_binding_digest=null,state_consumed_at=null,last_error=null,updated_at=${now}
        where household_id=${input.householdId} and owner_adult_id=${input.ownerAdultId}
          and status='pending'
      `;
      const [row] = await sql<GoogleConnectionRow[]>`
        update google_connections set status='disconnected',refresh_token_envelope=null,
          updated_at=${now} where id=${input.connectionId} returning *
      `;
      if (!row) throw new Error("The Google connection was not disconnected");
      if (current.status === "active" && (input.notifyReconnect ?? true)) {
        const [privateChannel] = await sql<ChannelRow[]>`
          select * from linq_channels where household_id=${input.householdId}
            and audience='private' and adult_one_id=${input.ownerAdultId} and adult_two_id is null
            and revoked_at is null and stopped_at is null
          order by bound_at,id limit 1 for share
        `;
        if (privateChannel) {
          await insertOutbound(sql, {
            sourceId: deterministicUuid(`google-reconnect\0${input.connectionId}`),
            idempotencyKey: `google-reconnect:${input.connectionId}`,
            moveKind: "message",
            text: "Your Google connection stopped working. Reconnect it in Florence settings so I can keep helping with family plans.",
            turnId: deterministicUuid(`google-reconnect-turn\0${input.connectionId}`),
            turnPart: 0,
            notBefore: now.toISOString(),
            householdId: input.householdId,
            channelId: privateChannel.id,
            visibility: "private",
            ownerAdultId: input.ownerAdultId,
            occurredAt: now,
          });
        }
        const calendarAuthority = await readFamilyCalendarAuthority(sql, input.householdId);
        if (
          calendarAuthority &&
          calendarAuthority.founder_connection_status !== "active" &&
          calendarAuthority.partner_connection_status !== "active" &&
          calendarAuthority.founder_adult_id
        ) {
          const [familyGroup] = await sql<ChannelRow[]>`
            select * from linq_channels where household_id=${input.householdId}
              and audience='group' and adult_two_id is not null
              and revoked_at is null and stopped_at is null
            order by bound_at desc,id desc limit 1 for share
          `;
          if (
            familyGroup &&
            isExactFamilyCalendarAuthority(calendarAuthority, familyGroup, calendarAuthority.founder_adult_id)
          ) {
            await insertOutbound(sql, {
              sourceId: deterministicUuid(`family-calendar-reconnect\0${input.householdId}`),
              idempotencyKey: `family-calendar-reconnect:${input.householdId}`,
              moveKind: "message",
              text: "The family calendar is paused because neither Google account is connected. Either of you can reconnect in Florence settings.",
              turnId: deterministicUuid(`family-calendar-reconnect-turn\0${input.householdId}`),
              turnPart: 0,
              notBefore: now.toISOString(),
              householdId: input.householdId,
              channelId: familyGroup.id,
              visibility: "household",
              ownerAdultId: null,
              occurredAt: now,
            });
          }
        }
      }
      return { view: googleConnectionView(row), refreshTokenEnvelope: current.refresh_token_envelope };
    });
  }

  async deleteGoogleDerivedData(input: {
    householdId: string;
    adultId: string;
    now: string;
  }): Promise<GoogleDataPurgeResult> {
    const now = instant(input.now);
    return this.#sql.begin(async (sql) => {
      const [adult] = await sql<{ id: string }[]>`
        select id from people where household_id=${input.householdId} and id=${input.adultId}
          and kind='adult' and status='verified' for update
      `;
      if (!adult) throw new FlorenceStoreUnauthorized();

      const connections = await sql<GoogleConnectionRow[]>`
        select * from google_connections where household_id=${input.householdId}
          and owner_adult_id=${input.adultId} order by created_at,id for update
      `;
      const additionalActiveConnectionsDisconnected = connections.filter(
        (connection) => connection.status === "active",
      ).length;

      const googleSources = await sql<{ id: string }[]>`
        select evidence.id from sources evidence
        where evidence.household_id=${input.householdId}
          and evidence.kind in ('gmail','calendar','google_file')
          and (
            exists (
              select 1 from google_connections connection
              where connection.household_id=${input.householdId}
                and connection.owner_adult_id=${input.adultId}
                and evidence.metadata->>'connectionId'=connection.id::text
            )
            or (
              evidence.visibility='private' and evidence.owner_adult_id=${input.adultId}
              and evidence.metadata->>'connectionId' is null
            )
          )
        order by evidence.id for update of evidence
      `;
      const googleSourceIds = googleSources.map((source) => source.id);

      const unsentMessageSourceIds = await googleDerivedUnsentMessageSourceIds(sql, {
        householdId: input.householdId,
        adultId: input.adultId,
        connectionId: null,
        statuses: ["pending", "failed"],
      });

      const impactedFacts =
        googleSourceIds.length === 0
          ? []
          : await sql<{ id: string }[]>`
              select fact.id from facts fact where fact.household_id=${input.householdId}
                and exists (
                  select 1 from fact_sources link where link.fact_id=fact.id
                    and link.source_id in ${sql(googleSourceIds)}
                )
              order by fact.id for update of fact
            `;
      if (googleSourceIds.length > 0) {
        await sql`delete from fact_sources where source_id in ${sql(googleSourceIds)}`;
      }
      const factRows =
        impactedFacts.length === 0
          ? []
          : await sql<{ id: string }[]>`
              delete from facts fact where fact.household_id=${input.householdId}
                and fact.id in ${sql(impactedFacts.map((fact) => fact.id))}
                and not exists (select 1 from fact_sources link where link.fact_id=fact.id)
              returning fact.id
            `;

      const workRows =
        googleSourceIds.length === 0
          ? await sql<{ id: string; kind: ProactiveWorkRow["kind"] }[]>`
              select work.id,work.kind from proactive_work work
              where work.household_id=${input.householdId}
                and work.owner_adult_id=${input.adultId}
                and work.kind in ('initial_private_review','personal_google_poll')
              order by work.id for update of work
            `
          : await sql<{ id: string; kind: ProactiveWorkRow["kind"] }[]>`
              select work.id,work.kind from proactive_work work
              where work.household_id=${input.householdId}
                and (
                  (work.owner_adult_id=${input.adultId}
                    and work.kind in ('initial_private_review','personal_google_poll'))
                  or (
                    work.kind in ('finite_monitor','interest_monitor')
                    and exists (
                      select 1 from proactive_work_sources link
                      where link.work_id=work.id and link.source_id in ${sql(googleSourceIds)}
                    )
                  )
                )
              order by work.id for update of work
            `;
      if (workRows.length > 0) {
        await sql`delete from proactive_work where id in ${sql(workRows.map((work) => work.id))}`;
      }

      const calendarActionRows = await sql<{ id: string }[]>`
        delete from calendar_actions action where action.household_id=${input.householdId}
          and (
            ${
              googleSourceIds.length === 0
                ? sql`false`
                : sql`action.basis_source_id in ${sql(googleSourceIds)}`
            }
            or ${
              unsentMessageSourceIds.length === 0
                ? sql`false`
                : sql`action.approval_prompt_source_id in ${sql(unsentMessageSourceIds)}`
            }
          )
        returning action.id
      `;

      let deletedUnsentMessages = 0;
      if (unsentMessageSourceIds.length > 0) {
        const rows = await sql<{ id: string }[]>`
          delete from sources where household_id=${input.householdId}
            and id in ${sql(unsentMessageSourceIds)} returning id
        `;
        deletedUnsentMessages = rows.length;
      }

      let deletedGoogleSources = 0;
      if (googleSourceIds.length > 0) {
        const rows = await sql<{ id: string }[]>`
          delete from sources where household_id=${input.householdId}
            and id in ${sql(googleSourceIds)} returning id
        `;
        deletedGoogleSources = rows.length;
      }

      for (const connection of connections) {
        await sql`
          update google_connections set status='disconnected',
            state_digest=${sha256(`deleted-google-state\0${connection.id}`)},
            session_binding_digest=null,state_consumed_at=null,google_subject_digest=null,
            email_label=null,granted_scopes='{}'::text[],refresh_token_envelope=null,last_error=null,
            updated_at=${now}
          where id=${connection.id}
        `;
      }

      return {
        additionalActiveConnectionsDisconnected,
        googleSources: deletedGoogleSources,
        facts: factRows.length,
        watches: workRows.filter((work) => work.kind === "finite_monitor" || work.kind === "interest_monitor")
          .length,
        calendarActions: calendarActionRows.length,
        unsentMessages: deletedUnsentMessages,
      };
    });
  }

  async readActiveGoogleCredential(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
  }): Promise<ActiveGoogleCredential | null> {
    const [row] = await this.#sql<GoogleConnectionRow[]>`
      select * from google_connections where id=${input.connectionId}
        and household_id=${input.householdId} and owner_adult_id=${input.ownerAdultId} and status='active'
    `;
    return row?.refresh_token_envelope
      ? {
          connectionId: row.id,
          householdId: row.household_id,
          ownerAdultId: row.owner_adult_id,
          refreshTokenEnvelope: row.refresh_token_envelope,
        }
      : null;
  }

  async readActiveFamilyCalendarCredential(input: {
    householdId: string;
  }): Promise<ActiveFamilyCalendarCredential | null> {
    return this.#sql.begin(async (sql) => {
      const authority = await readFamilyCalendarAuthority(sql, input.householdId);
      return activeFamilyCalendarCredential(authority);
    });
  }

  async readActiveFamilyCalendarCredentials(input: {
    householdId: string;
  }): Promise<readonly ActiveFamilyCalendarCredential[]> {
    return this.#sql.begin(async (sql) => {
      const authority = await readFamilyCalendarAuthority(sql, input.householdId);
      return activeFamilyCalendarCredentials(authority);
    });
  }

  async #readProactiveWatches(
    householdId: string,
    viewerAdultId: string | null,
  ): Promise<readonly ProactiveWatchRecord[]> {
    const rows = await this.#sql<ProactiveWorkRow[]>`
      select * from proactive_work where household_id=${householdId}
        and kind in ('finite_monitor','interest_monitor')
        and status in ('active','paused')
        and (visibility='household' or owner_adult_id=${viewerAdultId})
      order by created_at,id
    `;
    if (rows.length === 0) return [];
    const sourceRows = await this.#sql<({ work_id: string } & SourceRow)[]>`
      select pws.work_id,s.id,s.kind,s.visibility,s.owner_adult_id,s.label,s.metadata,s.occurred_at
      from proactive_work_sources pws join sources s on s.id=pws.source_id
      where pws.work_id in ${this.#sql(rows.map((row) => row.id))}
        and (s.visibility='household' or s.owner_adult_id=${viewerAdultId})
      order by s.occurred_at desc,s.id desc
    `;
    const firstSource = new Map<string, SourceRecord>();
    for (const row of sourceRows) {
      if (!firstSource.has(row.work_id)) firstSource.set(row.work_id, sourceRecord(row));
    }
    return rows.map((row) => {
      if (!row.objective) {
        throw new FlorenceStoreConflict("A visible watch is missing its objective");
      }
      return {
        id: row.id,
        kind: row.kind === "finite_monitor" ? "monitor" : "interest",
        objective: row.objective,
        currentConclusion: row.current_conclusion,
        status: row.status as "active" | "paused",
        visibility: row.visibility,
        ownerAdultId: row.owner_adult_id,
        source: firstSource.get(row.id) ?? null,
      };
    });
  }

  async #readFacts(
    householdId: string,
    viewerAdultId: string | null,
    householdOnly = false,
  ): Promise<readonly FactRecord[]> {
    const rows = await this.#sql<
      {
        id: string;
        household_id: string;
        subject_person_id: string | null;
        kind: FactRecord["kind"];
        slot: string;
        label: string;
        value: JsonValue;
        visibility: Visibility;
        owner_adult_id: string | null;
        corrected_at: Date | null;
        updated_at: Date;
      }[]
    >`
      select id,household_id,subject_person_id,kind,slot,label,value,visibility,owner_adult_id,
             corrected_at,updated_at
      from facts where household_id=${householdId}
        and (${householdOnly} or visibility='household' or owner_adult_id=${viewerAdultId})
        and (not ${householdOnly} or visibility='household')
      order by kind,label,id
    `;
    if (rows.length === 0) return [];
    const sourceRows = await this.#sql<({ fact_id: string } & SourceRow)[]>`
      select fs.fact_id,s.id,s.kind,s.visibility,s.owner_adult_id,s.label,s.metadata,s.occurred_at
      from fact_sources fs join sources s on s.id=fs.source_id
      where fs.fact_id in ${this.#sql(rows.map((row) => row.id))}
        and (s.visibility='household' or s.owner_adult_id=${viewerAdultId})
      order by s.occurred_at,s.id
    `;
    const sources = new Map<string, SourceRecord[]>();
    for (const row of sourceRows) {
      const list = sources.get(row.fact_id) ?? [];
      list.push(sourceRecord(row));
      sources.set(row.fact_id, list);
    }
    return rows.map((row) => ({
      id: row.id,
      householdId: row.household_id,
      subjectPersonId: row.subject_person_id,
      kind: row.kind,
      slot: row.slot,
      label: row.label,
      value: row.value,
      visibility: row.visibility,
      ownerAdultId: row.owner_adult_id,
      sources: sources.get(row.id) ?? [],
      correctedAt: row.corrected_at?.toISOString() ?? null,
      updatedAt: row.updated_at.toISOString(),
    }));
  }

  async #readOutbound(sourceId: string): Promise<OutboundMessage | null> {
    const [row] = await this.#sql<
      {
        source_id: string;
        idempotency_key: string;
        provider_conversation_id: string;
        audience: Audience;
        identity_one_digest: string;
        identity_two_digest: string | null;
        move_kind: "message" | "reply" | "reaction";
        text: string | null;
        reaction: string | null;
        reply_provider_message_id: string | null;
      }[]
    >`
      select m.source_id,m.idempotency_key,c.provider_conversation_id,c.audience,
             c.identity_one_digest,c.identity_two_digest,m.move_kind,m.text,m.reaction,
             reply.provider_message_id as reply_provider_message_id
      from messages m join linq_channels c on c.id=m.channel_id
      left join messages reply on reply.source_id=m.reply_to_source_id
      where m.source_id=${sourceId} and m.direction='outbound' and m.status in ('pending','sending')
        and c.revoked_at is null and c.stopped_at is null
    `;
    if (!row) return null;
    return {
      sourceId: row.source_id,
      idempotencyKey: row.idempotency_key,
      providerConversationId: row.provider_conversation_id,
      expectedAuthority: {
        audience: row.audience,
        participantIdentityDigests: [row.identity_one_digest, row.identity_two_digest]
          .filter((value): value is string => value !== null)
          .sort(),
      },
      moveKind: row.move_kind,
      text: row.text,
      reaction: row.reaction,
      replyToProviderMessageId: row.reply_provider_message_id,
    };
  }
}

export function draftGmailEvidence(input: {
  householdId: string;
  ownerAdultId: string;
  connectionId: string;
  messageId: string;
  threadId: string;
  historyId: string;
  from: string;
  subject: string | null;
  sentAt: string;
}): GmailEvidenceDraft {
  const connectionId = required(input.connectionId, "Google connection ID");
  const messageId = required(input.messageId, "Gmail message ID");
  const threadId = required(input.threadId, "Gmail thread ID");
  const historyId = required(input.historyId, "Gmail history ID");
  const from = required(input.from, "Gmail sender");
  const occurredAt = instant(input.sentAt).toISOString();
  const externalKey = `${connectionId}:${messageId}`;
  const metadata = {
    connectionId,
    messageId,
    threadId,
    historyId,
    from,
    subject: input.subject,
    sentAt: occurredAt,
  } satisfies JsonObject;
  return {
    id: gmailEvidenceSourceId(input.householdId, connectionId, messageId),
    householdId: input.householdId,
    kind: "gmail",
    visibility: "private",
    ownerAdultId: input.ownerAdultId,
    connectionOwnerAdultId: input.ownerAdultId,
    connectionId,
    externalKey,
    label: bounded(input.subject ?? `Email from ${from}`, 500),
    metadata,
    occurredAt,
    messageId,
    threadId,
    historyId,
    from,
    subject: input.subject,
    sentAt: occurredAt,
  };
}

export function gmailEvidenceSourceId(householdId: string, connectionId: string, messageId: string): string {
  const externalKey = `${required(connectionId, "Google connection ID")}:${required(
    messageId,
    "Gmail message ID",
  )}`;
  return deterministicUuid(`gmail-source\0${householdId}\0${externalKey}`);
}

export function draftCalendarEvidence(input: {
  householdId: string;
  ownerAdultId: string;
  connectionId: string;
  calendarId: string;
  providerEventId: string;
  providerRevision: string;
  providerUpdatedAt: string;
  status: "confirmed" | "tentative" | "cancelled";
  busy: boolean;
  title: string | null;
  startsAt: string | null;
  endsAt: string | null;
  allDay: boolean | null;
  visibility?: Visibility;
}): CalendarEvidenceDraft {
  const connectionId = required(input.connectionId, "Google connection ID");
  const calendarId = required(input.calendarId, "Google Calendar ID");
  const providerEventId = required(input.providerEventId, "Google Calendar event ID");
  const providerRevision = required(input.providerRevision, "Google Calendar event revision");
  const providerUpdatedAt = instant(input.providerUpdatedAt).toISOString();
  if (!(["confirmed", "tentative", "cancelled"] as const).includes(input.status)) {
    throw new FlorenceStoreConflict("Calendar evidence has an invalid status");
  }
  if (typeof input.busy !== "boolean") {
    throw new FlorenceStoreConflict("Calendar evidence has an invalid busy state");
  }
  if (
    (input.startsAt === null) !== (input.endsAt === null) ||
    (input.startsAt === null) !== (input.allDay === null)
  ) {
    throw new FlorenceStoreConflict("Calendar evidence needs all interval fields or none");
  }
  const startsAt = input.startsAt === null ? null : instant(input.startsAt).toISOString();
  const endsAt = input.endsAt === null ? null : instant(input.endsAt).toISOString();
  if (startsAt && endsAt && Date.parse(endsAt) <= Date.parse(startsAt)) {
    throw new FlorenceStoreConflict("Calendar evidence has an invalid interval");
  }
  if (input.status !== "cancelled" && (!startsAt || !endsAt)) {
    throw new FlorenceStoreConflict("A current Calendar event needs an interval");
  }
  if (input.status === "cancelled" && input.busy) {
    throw new FlorenceStoreConflict("A cancelled Calendar event cannot be busy");
  }
  const visibility = input.visibility ?? "private";
  const sourceOwnerAdultId = visibility === "private" ? input.ownerAdultId : null;
  const externalKey = `${connectionId}:${calendarId}:${providerEventId}`;
  const metadata = {
    connectionId,
    calendarId,
    providerEventId,
    providerRevision,
    providerUpdatedAt,
    status: input.status,
    busy: input.busy,
    title: input.title,
    startsAt,
    endsAt,
    allDay: input.allDay,
  } satisfies JsonObject;
  return {
    id: calendarEvidenceSourceId(input.householdId, connectionId, calendarId, providerEventId),
    householdId: input.householdId,
    kind: "calendar",
    visibility,
    ownerAdultId: sourceOwnerAdultId,
    connectionOwnerAdultId: input.ownerAdultId,
    connectionId,
    externalKey,
    label: bounded(
      input.title ?? (input.status === "cancelled" ? "Cancelled calendar event" : "Private calendar event"),
      500,
    ),
    metadata,
    occurredAt: startsAt ?? providerUpdatedAt,
    calendarId,
    providerEventId,
    providerRevision,
    providerUpdatedAt,
    status: input.status,
    busy: input.busy,
    title: input.title,
    startsAt,
    endsAt,
    allDay: input.allDay,
  };
}

export function calendarEvidenceSourceId(
  householdId: string,
  connectionId: string,
  calendarId: string,
  providerEventId: string,
): string {
  return deterministicUuid(
    `calendar-source\0${householdId}\0${required(connectionId, "Google connection ID")}\0${required(
      calendarId,
      "Google Calendar ID",
    )}\0${required(providerEventId, "Google Calendar event ID")}`,
  );
}

type OutboundInsert = OutboundDraft & {
  householdId: string;
  channelId: string;
  parentSourceId?: string;
  visibility: Visibility;
  ownerAdultId: string | null;
  metadata?: JsonObject;
  occurredAt: Date;
};

type ProductionResetCalendarTargetRow = {
  household_id: string;
  calendar_id: string | null;
  creator_adult_id: string | null;
  creator_connection_id: string | null;
};

type ProductionResetGoogleCredentialTargetRow = {
  household_id: string;
  adult_id: string;
  connection_id: string;
};

async function lockProductionHouseholdData(sql: postgres.TransactionSql): Promise<void> {
  await sql.unsafe(`
    lock table households,people,linq_channels,sources,messages,documents,facts,
      fact_sources,google_connections,calendar_actions,proactive_work,proactive_work_sources
    in access exclusive mode
  `);
}

async function readProductionResetSnapshot(
  sql: postgres.Sql | postgres.TransactionSql,
): Promise<ProductionResetSnapshot> {
  const [fingerprint] = await sql<{ state: string; household_count: number }[]>`
    select jsonb_build_object(
      'households',coalesce((select jsonb_agg(to_jsonb(h) order by h.id) from households h),'[]'),
      'people',coalesce((select jsonb_agg(to_jsonb(p) order by p.id) from people p),'[]'),
      'linq_channels',coalesce((select jsonb_agg(to_jsonb(c) order by c.id) from linq_channels c),'[]'),
      'sources',coalesce((select jsonb_agg(to_jsonb(s) order by s.id) from sources s),'[]'),
      'messages',coalesce((select jsonb_agg(to_jsonb(m) order by m.source_id) from messages m),'[]'),
      'documents',coalesce((select jsonb_agg(to_jsonb(d) order by d.source_id) from documents d),'[]'),
      'facts',coalesce((select jsonb_agg(to_jsonb(f) order by f.id) from facts f),'[]'),
      'fact_sources',coalesce((
        select jsonb_agg(to_jsonb(fs) order by fs.fact_id,fs.source_id) from fact_sources fs
      ),'[]'),
      'google_connections',coalesce((
        select jsonb_agg(to_jsonb(g) order by g.id) from google_connections g
      ),'[]'),
      'calendar_actions',coalesce((
        select jsonb_agg(to_jsonb(a) order by a.id) from calendar_actions a
      ),'[]'),
      'proactive_work',coalesce((
        select jsonb_agg(to_jsonb(w) order by w.id) from proactive_work w
      ),'[]'),
      'proactive_work_sources',coalesce((
        select jsonb_agg(to_jsonb(ws) order by ws.work_id,ws.source_id)
        from proactive_work_sources ws
      ),'[]')
    )::text as state,
    (select count(*)::integer from households) as household_count
  `;
  if (!fingerprint || typeof fingerprint.state !== "string") {
    throw new FlorenceStoreConflict("Production reset snapshot could not be read");
  }
  const calendarRows = await sql<ProductionResetCalendarTargetRow[]>`
    select h.id as household_id,h.family_calendar_id as calendar_id,
      creator.id as creator_adult_id,credential.id as creator_connection_id
    from households h
    left join people creator on creator.household_id=h.id and creator.kind='adult'
      and creator.adult_slot=1
    left join google_connections credential on credential.household_id=h.id
      and credential.owner_adult_id=creator.id and credential.status='active'
      and credential.google_subject_digest is not null
      and h.family_calendar_create_attempted_at is not null
      and exists (
        select 1 from google_connections original
        where original.household_id=h.id and original.owner_adult_id=creator.id
          and original.created_at<=h.family_calendar_create_attempted_at
          and original.google_subject_digest is not null
          and original.google_subject_digest=credential.google_subject_digest
      )
      and (
        credential.id=h.family_calendar_owner_connection_id
        or (
          h.family_calendar_owner_connection_id is null
          and h.family_calendar_create_attempted_at is not null
        )
      )
    where h.family_calendar_id is not null
      or h.family_calendar_create_attempted_at is not null
    order by h.created_at,h.id
  `;
  const credentialRows = await sql<ProductionResetGoogleCredentialTargetRow[]>`
    select household_id,owner_adult_id as adult_id,id as connection_id
    from google_connections where status='active'
    order by created_at,id
  `;
  const calendars = calendarRows.map((row): ProductionResetCalendarTarget => {
    assertUuid(row.household_id, "Production reset household ID");
    const calendarId = row.calendar_id;
    if (calendarId === "primary") {
      throw new FlorenceStoreConflict("A production reset cannot target a primary Calendar");
    }
    const creator =
      row.creator_adult_id === null || row.creator_connection_id === null
        ? null
        : Object.freeze({
            adultId: validProductionResetUuid(row.creator_adult_id, "creator adult ID"),
            connectionId: validProductionResetUuid(row.creator_connection_id, "creator connection ID"),
          });
    return Object.freeze({ householdId: row.household_id, calendarId, creator });
  });
  const activeGoogleCredentials = credentialRows.map(
    (row): ProductionResetGoogleCredentialTarget =>
      Object.freeze({
        householdId: validProductionResetUuid(row.household_id, "credential household ID"),
        adultId: validProductionResetUuid(row.adult_id, "credential adult ID"),
        connectionId: validProductionResetUuid(row.connection_id, "credential connection ID"),
      }),
  );
  return Object.freeze({
    guard: createHash("sha256")
      .update("florence-production-reset-v1\0")
      .update(fingerprint.state)
      .digest("hex"),
    householdCount: fingerprint.household_count,
    calendars: Object.freeze(calendars),
    activeGoogleCredentials: Object.freeze(activeGoogleCredentials),
  });
}

function validProductionResetUuid(value: string, label: string): string {
  assertUuid(value, `Production reset ${label}`);
  return value;
}

function sameProductionResetSnapshot(
  current: ProductionResetSnapshot,
  expected: ProductionResetSnapshot,
): boolean {
  return (
    current.guard === expected.guard &&
    current.householdCount === expected.householdCount &&
    current.calendars.length === expected.calendars.length &&
    current.calendars.every((calendar, index) => {
      const other = expected.calendars[index];
      return (
        other !== undefined &&
        calendar.householdId === other.householdId &&
        calendar.calendarId === other.calendarId &&
        calendar.creator?.adultId === other.creator?.adultId &&
        calendar.creator?.connectionId === other.creator?.connectionId
      );
    }) &&
    current.activeGoogleCredentials.length === expected.activeGoogleCredentials.length &&
    current.activeGoogleCredentials.every((credential, index) => {
      const other = expected.activeGoogleCredentials[index];
      return (
        other !== undefined &&
        credential.householdId === other.householdId &&
        credential.adultId === other.adultId &&
        credential.connectionId === other.connectionId
      );
    })
  );
}

type HouseholdLinqSql = postgres.Sql | postgres.TransactionSql;

async function householdLinqIncarnationScope(sql: HouseholdLinqSql, householdId: string): Promise<string> {
  const [household] = await sql<{ created_at_exact: string }[]>`
    select to_char(created_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as created_at_exact
    from households where id=${householdId}
  `;
  if (!household) throw new FlorenceStoreConflict("The household no longer exists");
  return deterministicUuid(`household-linq-incarnation\0${householdId}\0${household.created_at_exact}`);
}

function scopeLinqIdempotencyKey(idempotencyKey: string, incarnationScope: string): string {
  const key = required(idempotencyKey, "Linq idempotency key");
  const scoped = `${key}:h:${incarnationScope}`;
  if (scoped.length > 255) {
    throw new FlorenceStoreConflict("A household Linq idempotency key is too long");
  }
  return scoped;
}

async function householdLinqIdempotencyKey(
  sql: HouseholdLinqSql,
  householdId: string,
  idempotencyKey: string,
): Promise<string> {
  return scopeLinqIdempotencyKey(idempotencyKey, await householdLinqIncarnationScope(sql, householdId));
}

function founderHandoffIdentity(
  householdId: string,
  adultId: string,
  incarnationScope: string,
): {
  turnId: string;
  part(index: number): {
    sourceId: string;
    rawIdempotencyKey: string;
    idempotencyKey: string;
  };
} {
  return {
    turnId: deterministicUuid(`founder-handoff-turn\0${householdId}\0${adultId}`),
    part: (index) => {
      const rawIdempotencyKey = `founder-handoff:${householdId}:${adultId}:${index}`;
      return {
        sourceId: deterministicUuid(`founder-handoff\0${householdId}\0${adultId}\0${index}`),
        rawIdempotencyKey,
        idempotencyKey: scopeLinqIdempotencyKey(rawIdempotencyKey, incarnationScope),
      };
    },
  };
}

function personRecord(row: PersonRow): FamilyMemberRecord {
  const messagesIdentity =
    row.kind === "child"
      ? null
      : row.identity_subject_digest !== null
        ? "connected"
        : row.invitation_consumed_at === null && row.invitation_issued_at !== null
          ? "invited"
          : "not_invited";
  return {
    id: row.id,
    householdId: row.household_id,
    kind: row.kind,
    role: row.role,
    adultSlot: row.adult_slot,
    displayName: row.display_name,
    status: row.status,
    messagesIdentity,
    messagesInvitationApproved: row.kind === "adult" ? row.invitation_approval_source_id !== null : null,
    messagesAddress: row.messages_address,
    profile: row.profile,
    preferences: row.preferences,
  };
}

async function terminalizeIssuedPartnerInvitation(
  sql: postgres.TransactionSql,
  invitation: IssuedPartnerInvitationRow,
  occurredAt: Date,
  reason: "declined" | "expired",
): Promise<boolean> {
  const invalidated = await sql`
    update people set invitation_digest=null,invitation_expires_at=null,
      invitation_consumed_at=${occurredAt},messages_address=null,
      invitation_approval_source_id=null,invitation_approved_at=null,
      invitation_retry_at=null,invitation_last_error=null,updated_at=${occurredAt}
    where id=${invitation.adult_id} and invitation_consumed_at is null
      and messages_address is not null
      and invitation_message_id=${invitation.invitation_message_id}
      and (invitation_digest is null or invitation_expires_at is not null)
    returning id
  `;
  if (invalidated.length !== 1) return false;
  await stagePartnerInvitationTerminalNotice(sql, {
    invitation,
    reason,
    linkIssued: invitation.link_issued,
    occurredAt,
    stableKey: invitation.invitation_message_id,
  });
  return true;
}

async function stagePartnerInvitationTerminalNotice(
  sql: postgres.TransactionSql,
  input: {
    invitation: {
      adult_id: string;
      household_id: string;
      first_name: string;
      founder_adult_id: string;
      founder_channel_id: string;
    };
    reason: "declined" | "expired" | "delivery_failed";
    linkIssued?: boolean;
    occurredAt: Date;
    stableKey: string;
  },
): Promise<void> {
  const sourceId = deterministicUuid(
    `partner-invitation-${input.reason}\0${input.invitation.adult_id}\0${input.stableKey}`,
  );
  const text =
    input.reason === "expired"
      ? input.linkIssued === false
        ? `I couldn’t confirm delivery of ${input.invitation.first_name}’s Florence setup link before it expired, so I stopped the invitation. I won’t message them again unless you ask me to try again.`
        : `${input.invitation.first_name}’s Florence setup link expired, so I stopped the invitation. I won’t message them again unless you ask me to send a fresh one.`
      : input.reason === "delivery_failed"
        ? `I couldn’t deliver ${input.invitation.first_name}’s Florence setup link, so I stopped the invitation. I won’t message them again unless you ask me to try again.`
        : input.linkIssued === false
          ? `${input.invitation.first_name} didn’t want to continue with Florence, so I stopped the invitation. I won’t message them again unless you ask me to.`
          : `${input.invitation.first_name} didn’t complete Florence setup, so I stopped the invitation. I won’t message them again unless you ask me to.`;
  await insertOutbound(sql, {
    sourceId,
    idempotencyKey: `partner-invitation-${input.reason}:${sourceId}`,
    moveKind: "message",
    text,
    turnId: sourceId,
    turnPart: 0,
    notBefore: input.occurredAt.toISOString(),
    householdId: input.invitation.household_id,
    channelId: input.invitation.founder_channel_id,
    visibility: "private",
    ownerAdultId: input.invitation.founder_adult_id,
    occurredAt: input.occurredAt,
  });
}

function awaitingPartnerInvitationApproval(row: PersonRow): boolean {
  if (
    row.identity_subject_digest !== null ||
    row.invitation_digest !== null ||
    row.invitation_expires_at !== null ||
    row.messages_address !== null ||
    row.invitation_approval_source_id !== null ||
    row.invitation_approved_at !== null ||
    row.invitation_retry_at !== null ||
    row.invitation_last_error !== null
  ) {
    return false;
  }
  const firstInvitation =
    row.invitation_consumed_at === null &&
    row.invitation_conversation_id === null &&
    row.invitation_identity_digest === null &&
    row.invitation_message_id === null &&
    row.invitation_issued_at === null;
  const declinedInvitation =
    row.invitation_consumed_at !== null &&
    row.invitation_conversation_id !== null &&
    row.invitation_identity_digest !== null &&
    row.invitation_message_id !== null &&
    row.invitation_issued_at !== null;
  const failedBeforeConversation =
    row.invitation_consumed_at !== null &&
    row.invitation_conversation_id === null &&
    row.invitation_identity_digest === null &&
    row.invitation_message_id === null &&
    row.invitation_issued_at === null;
  return firstInvitation || declinedInvitation || failedBeforeConversation;
}

function channelRecord(row: ChannelRow): LinqChannelRecord {
  return {
    id: row.id,
    householdId: row.household_id,
    audience: row.audience,
    providerConversationId: row.provider_conversation_id,
    adultIds: [row.adult_one_id, row.adult_two_id].filter((value): value is string => value !== null),
    participantIdentityDigests: channelIdentityDigests(row),
    authorityDigest: row.authority_digest,
    boundAt: row.bound_at.toISOString(),
    revokedAt: row.revoked_at?.toISOString() ?? null,
    stoppedAt: row.stopped_at?.toISOString() ?? null,
  };
}

function authorityRecord(
  row: ChannelRow,
  senderAdultId: string,
  replyToSourceId: string | null,
): LinqAuthority {
  return {
    householdId: row.household_id,
    channelId: row.id,
    audience: row.audience,
    providerConversationId: row.provider_conversation_id,
    senderAdultId,
    adultIds: [row.adult_one_id, row.adult_two_id].filter((value): value is string => value !== null),
    expectedParticipantIdentityDigests: channelIdentityDigests(row),
    authorityDigest: row.authority_digest,
    replyToSourceId,
    stopped: row.stopped_at !== null,
  };
}

async function resolveLinqAuthorityOn(
  sql: postgres.Sql | postgres.TransactionSql,
  input: {
    providerConversationId: string;
    audience: Audience;
    participantIdentityDigests: readonly string[];
    senderIdentitySubjectDigest: string;
    replyToProviderMessageId?: string | null;
    occurredAt: string;
  },
): Promise<LinqAuthority | null> {
  const occurredAt = instant(input.occurredAt);
  const [channel] = await sql<ChannelRow[]>`
    select * from linq_channels where provider_conversation_id=${input.providerConversationId}
      and audience=${input.audience} and revoked_at is null and bound_at<=${occurredAt}
    limit 1
  `;
  if (!channel) return null;
  const expected = channelIdentityDigests(channel);
  if (!sameStrings(expected, sortedDigests(input.participantIdentityDigests))) return null;
  const senderAdultId =
    channel.identity_one_digest === input.senderIdentitySubjectDigest
      ? channel.adult_one_id
      : channel.identity_two_digest === input.senderIdentitySubjectDigest
        ? channel.adult_two_id
        : null;
  if (!senderAdultId) return null;
  const [reply] = input.replyToProviderMessageId
    ? await sql<{ source_id: string }[]>`
        select source_id from messages where channel_id=${channel.id}
          and provider_message_id=${input.replyToProviderMessageId} limit 1
      `
    : [];
  return authorityRecord(channel, senderAdultId, reply?.source_id ?? null);
}

function channelIdentityDigests(row: ChannelRow): string[] {
  return [row.identity_one_digest, row.identity_two_digest]
    .filter((value): value is string => value !== null)
    .sort();
}

function calendarApprovalChannel(row: {
  household_id: string;
  channel_id: string | null;
  channel_audience: Audience | null;
  provider_conversation_id: string | null;
  adult_one_id: string | null;
  identity_one_digest: string | null;
  adult_two_id: string | null;
  identity_two_digest: string | null;
  authority_digest: string | null;
  bound_at: Date | null;
  revoked_at: Date | null;
  stopped_at: Date | null;
}): ChannelRow {
  if (
    row.channel_id === null ||
    row.channel_audience === null ||
    row.provider_conversation_id === null ||
    row.adult_one_id === null ||
    row.identity_one_digest === null ||
    row.authority_digest === null ||
    row.bound_at === null
  ) {
    throw new FlorenceStoreConflict("The Calendar approval channel is incomplete");
  }
  return {
    id: row.channel_id,
    household_id: row.household_id,
    audience: row.channel_audience,
    provider_conversation_id: row.provider_conversation_id,
    adult_one_id: row.adult_one_id,
    identity_one_digest: row.identity_one_digest,
    adult_two_id: row.adult_two_id,
    identity_two_digest: row.identity_two_digest,
    authority_digest: row.authority_digest,
    bound_at: row.bound_at,
    revoked_at: row.revoked_at,
    stopped_at: row.stopped_at,
  };
}

function isExactFamilyCalendarAuthority(
  authority: FamilyCalendarAuthorityRow,
  channel: ChannelRow,
  senderAdultId: string,
): boolean {
  if (
    authority.family_calendar_id === null ||
    authority.family_calendar_id === "primary" ||
    authority.family_calendar_owner_connection_id === null ||
    authority.family_calendar_partner_connection_id === null ||
    authority.family_calendar_created_at === null
  ) {
    return false;
  }
  return isExactFamilyGroupAuthority(authority, channel, senderAdultId);
}

function isExactFamilyGroupAuthority(
  authority: FamilyGroupAuthorityRow,
  channel: ChannelRow,
  senderAdultId: string,
): boolean {
  if (
    authority.founder_adult_id === null ||
    authority.founder_identity_digest === null ||
    authority.founder_status !== "verified" ||
    authority.partner_adult_id === null ||
    authority.partner_identity_digest === null ||
    authority.partner_status !== "verified" ||
    authority.founder_adult_id === authority.partner_adult_id ||
    channel.revoked_at !== null ||
    channel.stopped_at !== null
  ) {
    return false;
  }
  return isMatchingFamilyGroupAuthority(authority, channel, senderAdultId);
}

function isExactPrivateAdultCalendarAuthority(
  authority: FamilyCalendarAuthorityRow,
  channel: ChannelRow,
  senderAdultId: string,
): boolean {
  const expectedIdentity =
    senderAdultId === authority.founder_adult_id && authority.founder_status === "verified"
      ? authority.founder_identity_digest
      : senderAdultId === authority.partner_adult_id && authority.partner_status === "verified"
        ? authority.partner_identity_digest
        : null;
  return Boolean(
    expectedIdentity &&
      activeFamilyCalendarCredential(authority) &&
      channel.audience === "private" &&
      channel.adult_one_id === senderAdultId &&
      channel.identity_one_digest === expectedIdentity &&
      channel.adult_two_id === null &&
      channel.identity_two_digest === null &&
      channel.authority_digest === digestStrings([senderAdultId, expectedIdentity]) &&
      channel.revoked_at === null &&
      channel.stopped_at === null,
  );
}

function isMatchingFamilyGroupAuthority(
  authority: FamilyGroupAuthorityRow,
  channel: ChannelRow,
  senderAdultId: string,
): boolean {
  if (
    authority.founder_adult_id === null ||
    authority.founder_identity_digest === null ||
    authority.partner_adult_id === null ||
    authority.partner_identity_digest === null ||
    channel.audience !== "group" ||
    channel.adult_two_id === null ||
    channel.identity_two_digest === null
  ) {
    return false;
  }
  const adults = [authority.founder_adult_id, authority.partner_adult_id].sort();
  const channelAdults = [channel.adult_one_id, channel.adult_two_id].sort();
  const channelIdentities = channelIdentityDigests(channel);
  return (
    adults.includes(senderAdultId) &&
    sameStrings(adults, channelAdults) &&
    channel.authority_digest === digestStrings([...adults, ...channelIdentities])
  );
}

async function readFamilyCalendarAuthority(
  sql: postgres.TransactionSql,
  householdId: string,
): Promise<FamilyCalendarAuthorityRow | undefined> {
  const [authority] = await sql<FamilyCalendarAuthorityRow[]>`
    select h.id as household_id,h.family_calendar_id,h.family_calendar_owner_connection_id,
      h.family_calendar_partner_connection_id,h.family_calendar_created_at,
      founder.id as founder_adult_id,founder.identity_subject_digest as founder_identity_digest,
      founder.status as founder_status,founder.preferences as founder_preferences,
      founder_connection.status as founder_connection_status,
      partner.id as partner_adult_id,partner.identity_subject_digest as partner_identity_digest,
      partner.status as partner_status,partner.preferences as partner_preferences,
      partner_connection.status as partner_connection_status
    from households h
    left join google_connections founder_connection
      on founder_connection.id=h.family_calendar_owner_connection_id
        and founder_connection.household_id=h.id
    left join people founder on founder.household_id=h.id
      and founder.id=founder_connection.owner_adult_id and founder.kind='adult'
        and founder.role='steward' and founder.adult_slot=1
    left join google_connections partner_connection
      on partner_connection.id=h.family_calendar_partner_connection_id
        and partner_connection.household_id=h.id
    left join people partner on partner.household_id=h.id
      and partner.id=partner_connection.owner_adult_id and partner.kind='adult'
        and partner.role='steward' and partner.adult_slot=2
    where h.id=${householdId}
    for share of h
  `;
  return authority;
}

async function readCalendarActionAuthority(
  sql: postgres.TransactionSql,
  actionId: string,
): Promise<CalendarActionAuthorityRow | undefined> {
  const [row] = await sql<CalendarActionAuthorityRow[]>`
    select a.id,a.status,a.household_id,a.basis_source_id,a.approval_source_id,
      a.approval_prompt_source_id,a.google_action_key,
      coalesce(basis.kind in ('gmail','calendar') and basis.visibility='private',false)
        as legacy_google_review_basis,
      a.payload,a.provider_event_id,a.provider_etag,a.committed_at,a.retry_at,
      approval.channel_id,approval.direction,approval.sender_adult_id,
      channel.audience as channel_audience,channel.provider_conversation_id,channel.adult_one_id,
      channel.identity_one_digest,channel.adult_two_id,channel.identity_two_digest,
      channel.authority_digest,channel.bound_at,channel.revoked_at,channel.stopped_at
    from calendar_actions a
    left join messages approval on approval.source_id=a.approval_source_id
    left join linq_channels channel on channel.id=approval.channel_id
    left join sources basis on basis.id=a.basis_source_id
    where a.id=${actionId} for update of a
  `;
  return row;
}

async function isCalendarAdultApprovalBound(
  sql: postgres.TransactionSql,
  action: CalendarActionAuthorityRow,
): Promise<boolean> {
  if (!action.approval_source_id) return false;
  if (!action.approval_prompt_source_id) {
    return action.approval_source_id === action.basis_source_id;
  }
  const [bound] = await sql<{ id: string }[]>`
    select approval_source.id
    from messages approval
    join sources approval_source on approval_source.id=approval.source_id
    join messages prompt on prompt.source_id=${action.approval_prompt_source_id}
    join sources prompt_source on prompt_source.id=prompt.source_id
    where approval.source_id=${action.approval_source_id}
      and approval.direction='inbound'
      and prompt.direction='outbound' and prompt.move_kind in ('message','reply')
      and prompt.status='sent' and prompt.turn_part=0
      and prompt.channel_id=approval.channel_id
      and (prompt_source.occurred_at,prompt_source.id)
        < (approval_source.occurred_at,approval_source.id)
  `;
  return bound !== undefined;
}

async function isExactPersonalCalendarApprovalAuthority(
  sql: postgres.TransactionSql,
  action: CalendarActionAuthorityRow,
  authority: FamilyCalendarAuthorityRow,
  channel: ChannelRow,
  senderAdultId: string,
): Promise<boolean> {
  if (
    !action.basis_source_id ||
    familyCalendarMutation(action.payload).operation !== "create" ||
    !isExactPrivateAdultCalendarAuthority(authority, channel, senderAdultId)
  ) {
    return false;
  }
  const ownerAdultId = await personalCalendarBasisOwner(
    sql,
    action.household_id,
    action.basis_source_id,
    authority,
  );
  return ownerAdultId === senderAdultId;
}

async function calendarActionNotification(
  sql: postgres.TransactionSql,
  action: CalendarActionAuthorityRow,
  authority: FamilyCalendarAuthorityRow | undefined,
  outcome: "success" | "failure",
): Promise<{
  channel: ChannelRow;
  replyToSourceId: string | null;
  visibility: Visibility;
  ownerAdultId: string | null;
} | null> {
  if (!authority) {
    throw new FlorenceStoreConflict("The Calendar action authority is incomplete");
  }
  if (action.approval_source_id !== null) {
    if (!action.approval_source_id || action.direction !== "inbound" || !action.sender_adult_id) {
      throw new FlorenceStoreConflict("The Calendar approval is not bound to Messages");
    }
    if (!(await isCalendarAdultApprovalBound(sql, action))) {
      throw new FlorenceStoreConflict("The Calendar approval is not bound to its action or prompt");
    }
    const channel = calendarApprovalChannel(action);
    if (channel.audience === "private") {
      if (
        !(await isExactPersonalCalendarApprovalAuthority(
          sql,
          action,
          authority,
          channel,
          action.sender_adult_id,
        ))
      ) {
        throw new FlorenceStoreConflict(
          "The stored personal Calendar approval was not from its exact owner-private thread",
        );
      }
      if (channel.revoked_at !== null || channel.stopped_at !== null) return null;
      if (outcome === "failure") {
        return {
          channel,
          replyToSourceId: action.approval_source_id,
          visibility: "private",
          ownerAdultId: action.sender_adult_id,
        };
      }
      const [groupChannel] = await sql<ChannelRow[]>`
        select * from linq_channels where household_id=${action.household_id}
          and audience='group' and adult_two_id is not null
          and revoked_at is null and stopped_at is null
        order by bound_at,id limit 1 for share
      `;
      if (
        !groupChannel ||
        !authority.founder_adult_id ||
        !isExactFamilyCalendarAuthority(authority, groupChannel, authority.founder_adult_id)
      ) {
        throw new FlorenceStoreConflict("The exact family group is unavailable after Calendar approval");
      }
      return {
        channel: groupChannel,
        replyToSourceId: null,
        visibility: "household",
        ownerAdultId: null,
      };
    }
    if (!isMatchingFamilyGroupAuthority(authority, channel, action.sender_adult_id)) {
      throw new FlorenceStoreConflict("The stored Calendar approval was not from the exact family group");
    }
    if (channel.revoked_at !== null || channel.stopped_at !== null) return null;
    if (!isExactFamilyCalendarAuthority(authority, channel, action.sender_adult_id)) {
      throw new FlorenceStoreConflict("The Family Calendar is no longer ready");
    }
    return {
      channel,
      replyToSourceId: action.approval_source_id,
      visibility: "household",
      ownerAdultId: null,
    };
  }
  if (
    !action.basis_source_id ||
    action.approval_prompt_source_id !== null ||
    !(await isOfficialPrivateGmailBasis(sql, action.household_id, action.basis_source_id, authority))
  ) {
    throw new FlorenceStoreConflict("The automatic Calendar basis is no longer available");
  }
  const [channel] = await sql<ChannelRow[]>`
    select * from linq_channels where household_id=${action.household_id}
      and audience='group' and adult_two_id is not null and revoked_at is null and stopped_at is null
    order by bound_at,id limit 1 for share
  `;
  if (!channel || !authority.founder_adult_id) return null;
  if (!isExactFamilyCalendarAuthority(authority, channel, authority.founder_adult_id)) return null;
  return { channel, replyToSourceId: null, visibility: "household", ownerAdultId: null };
}

function activeFamilyCalendarCredential(
  authority: FamilyCalendarAuthorityRow | undefined,
): ActiveFamilyCalendarCredential | null {
  return activeFamilyCalendarCredentials(authority)[0] ?? null;
}

function activeFamilyCalendarCredentials(
  authority: FamilyCalendarAuthorityRow | undefined,
): readonly ActiveFamilyCalendarCredential[] {
  if (
    !authority ||
    authority.family_calendar_id === null ||
    authority.family_calendar_id === "primary" ||
    authority.family_calendar_owner_connection_id === null ||
    authority.family_calendar_partner_connection_id === null ||
    authority.family_calendar_created_at === null ||
    authority.founder_adult_id === null ||
    authority.founder_identity_digest === null ||
    authority.founder_status !== "verified" ||
    authority.partner_adult_id === null ||
    authority.partner_identity_digest === null ||
    authority.partner_status !== "verified" ||
    authority.founder_adult_id === authority.partner_adult_id
  ) {
    return [];
  }
  const credentials: ActiveFamilyCalendarCredential[] = [];
  if (authority.founder_connection_status === "active") {
    credentials.push({
      householdId: authority.household_id,
      connectionId: authority.family_calendar_owner_connection_id,
      ownerAdultId: authority.founder_adult_id,
      calendarId: authority.family_calendar_id,
    });
  }
  if (authority.partner_connection_status === "active") {
    credentials.push({
      householdId: authority.household_id,
      connectionId: authority.family_calendar_partner_connection_id,
      ownerAdultId: authority.partner_adult_id,
      calendarId: authority.family_calendar_id,
    });
  }
  return credentials;
}

function familyCalendarAutomaticCreationEnabled(authority: FamilyCalendarAuthorityRow): boolean {
  const founder = jsonRecord(authority.founder_preferences);
  const partner = jsonRecord(authority.partner_preferences);
  return (
    authority.founder_status === "verified" &&
    authority.partner_status === "verified" &&
    founder.automaticFamilyCalendarEnabled !== false &&
    partner.automaticFamilyCalendarEnabled !== false
  );
}

async function isOfficialPrivateCalendarBasis(
  sql: postgres.TransactionSql,
  householdId: string,
  sourceId: string,
  authority: FamilyCalendarAuthorityRow,
): Promise<boolean> {
  const adultIds = [authority.founder_adult_id, authority.partner_adult_id].filter(
    (value): value is string => value !== null,
  );
  if (adultIds.length !== 2) return false;
  const [source] = await sql<{ id: string }[]>`
    select id from sources where id=${sourceId} and household_id=${householdId}
      and kind in ('gmail','calendar') and visibility='private'
      and owner_adult_id in ${sql(adultIds)} for share
  `;
  return source !== undefined;
}

async function personalCalendarBasisOwner(
  sql: postgres.TransactionSql,
  householdId: string,
  sourceId: string,
  authority: FamilyCalendarAuthorityRow,
): Promise<string | null> {
  const adultIds = [authority.founder_adult_id, authority.partner_adult_id].filter(
    (value): value is string => value !== null,
  );
  if (adultIds.length !== 2) return null;
  const [source] = await sql<{ owner_adult_id: string }[]>`
    select source.owner_adult_id from sources source
    join google_connections source_connection
      on source_connection.id::text=source.metadata->>'connectionId'
      and source_connection.household_id=source.household_id
      and source_connection.owner_adult_id=source.owner_adult_id
    join google_connections active_connection
      on active_connection.household_id=source_connection.household_id
      and active_connection.owner_adult_id=source_connection.owner_adult_id
      and source_connection.google_subject_digest is not null
      and active_connection.google_subject_digest is not null
      and active_connection.google_subject_digest=source_connection.google_subject_digest
      and active_connection.status='active'
    where source.id=${sourceId} and source.household_id=${householdId}
      and source.kind='calendar' and source.visibility='private'
      and source.owner_adult_id in ${sql(adultIds)}
    for share of source,source_connection,active_connection
  `;
  return source?.owner_adult_id ?? null;
}

async function isOfficialPrivateGmailBasis(
  sql: postgres.TransactionSql,
  householdId: string,
  sourceId: string,
  authority: FamilyCalendarAuthorityRow,
): Promise<boolean> {
  const adultIds = [authority.founder_adult_id, authority.partner_adult_id].filter(
    (value): value is string => value !== null,
  );
  if (adultIds.length !== 2) return false;
  const [source] = await sql<{ id: string }[]>`
    select source.id from sources source
    join google_connections source_connection
      on source_connection.id::text=source.metadata->>'connectionId'
      and source_connection.household_id=source.household_id
      and source_connection.owner_adult_id=source.owner_adult_id
    join google_connections active_connection
      on active_connection.household_id=source_connection.household_id
      and active_connection.owner_adult_id=source_connection.owner_adult_id
      and source_connection.google_subject_digest is not null
      and active_connection.google_subject_digest is not null
      and active_connection.google_subject_digest=source_connection.google_subject_digest
      and active_connection.status='active'
    where source.id=${sourceId} and source.household_id=${householdId}
      and source.kind='gmail' and source.visibility='private'
      and source.owner_adult_id in ${sql(adultIds)}
    for share of source,source_connection,active_connection
  `;
  return source !== undefined;
}

async function failCalendarAuthority(
  sql: postgres.TransactionSql,
  actionId: string,
  failedAt: Date,
  reason: string,
): Promise<void> {
  await sql`
    update calendar_actions set status='failed',retry_at=${failedAt},last_error=${bounded(reason, 2_000)}
    where id=${actionId} and status='pending'
  `;
}

async function persistGoogleEvidenceDrafts(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    drafts: readonly GoogleEvidenceDraft[];
    sourceIds: readonly string[];
  },
): Promise<void> {
  const sourceIds = unique(input.sourceIds);
  if (sourceIds.length === 0) return;
  const selected = new Set(sourceIds);
  for (const sourceId of sourceIds) assertUuid(sourceId, "Cited Google source ID");
  const drafts = new Map<string, GoogleEvidenceDraft>();
  for (const supplied of input.drafts) {
    if (!selected.has(supplied.id)) continue;
    const draft = normalizedGoogleEvidenceDraft(supplied);
    if (draft.id !== supplied.id || draft.householdId !== input.householdId) {
      throw new FlorenceStoreUnauthorized("Google evidence belongs to another household or provider item");
    }
    const existingDraft = drafts.get(draft.id);
    if (existingDraft && JSON.stringify(existingDraft) !== JSON.stringify(draft)) {
      throw new FlorenceStoreConflict("A Google source changed within one completion");
    }
    drafts.set(draft.id, draft);
  }
  const ordered = [...drafts.values()].sort((left, right) => {
    if (left.kind === "calendar" && right.kind === "calendar") {
      return left.providerUpdatedAt.localeCompare(right.providerUpdatedAt) || left.id.localeCompare(right.id);
    }
    return left.id.localeCompare(right.id);
  });
  for (const draft of ordered) {
    const [connection] = await sql<{ id: string }[]>`
      select id from google_connections where id=${draft.connectionId}
        and household_id=${draft.householdId}
        and owner_adult_id=${draft.connectionOwnerAdultId} and status='active'
      for share
    `;
    if (!connection) {
      throw new FlorenceStoreUnauthorized("Cited Google evidence is no longer from an active parent account");
    }
    if (draft.kind === "gmail") {
      const [current] = await sql<SourceRow[]>`
        select id,kind,visibility,owner_adult_id,label,metadata,occurred_at from sources
        where household_id=${draft.householdId} and kind='gmail' and external_key=${draft.externalKey}
        for update
      `;
      if (
        current &&
        (current.id !== draft.id ||
          current.visibility !== "private" ||
          current.owner_adult_id !== draft.ownerAdultId ||
          jsonString(current.metadata, "connectionId") !== draft.connectionId ||
          jsonString(current.metadata, "messageId") !== draft.messageId)
      ) {
        throw new FlorenceStoreConflict("A Gmail provider item conflicts with retained family evidence");
      }
      const currentHistoryId = current ? jsonString(current.metadata, "historyId") : null;
      if (currentHistoryId === null || BigInt(draft.historyId) >= BigInt(currentHistoryId)) {
        await sql`
          insert into sources (
            id,household_id,kind,visibility,owner_adult_id,external_key,label,metadata,occurred_at
          ) values (${draft.id},${draft.householdId},'gmail','private',${draft.ownerAdultId},
            ${draft.externalKey},${draft.label},${sql.json(draft.metadata)},${instant(draft.occurredAt)})
          on conflict (household_id,kind,external_key) do update set
            label=excluded.label,metadata=excluded.metadata,occurred_at=excluded.occurred_at
        `;
      }
      await assertStoredGoogleEvidence(sql, draft);
      continue;
    }

    const [current] = await sql<SourceRow[]>`
      select id,kind,visibility,owner_adult_id,label,metadata,occurred_at from sources
      where household_id=${draft.householdId} and kind='calendar' and external_key=${draft.externalKey}
      for update
    `;
    if (
      current &&
      (current.id !== draft.id ||
        current.visibility !== draft.visibility ||
        current.owner_adult_id !== draft.ownerAdultId ||
        jsonString(current.metadata, "connectionId") !== draft.connectionId ||
        jsonString(current.metadata, "calendarId") !== draft.calendarId ||
        jsonString(current.metadata, "providerEventId") !== draft.providerEventId)
    ) {
      throw new FlorenceStoreConflict("A Calendar provider item conflicts with retained family evidence");
    }
    const currentUpdatedAt = current
      ? instant(
          required(
            jsonString(current.metadata, "providerUpdatedAt") ?? "",
            "Stored Calendar provider update time",
          ),
        ).toISOString()
      : null;
    if (currentUpdatedAt && currentUpdatedAt > draft.providerUpdatedAt) {
      await assertStoredGoogleEvidence(sql, draft);
      continue;
    }
    const priorEvidence = current ? calendarEvidenceRecord(current) : null;
    const recoveredStartsAt =
      draft.status === "cancelled" && draft.startsAt === null ? (priorEvidence?.startsAt ?? null) : null;
    const recoveredEndsAt =
      draft.status === "cancelled" && draft.endsAt === null ? (priorEvidence?.endsAt ?? null) : null;
    const recoveredAllDay =
      draft.status === "cancelled" && draft.allDay === null ? (priorEvidence?.allDay ?? null) : null;
    if (
      (recoveredStartsAt === null) !== (recoveredEndsAt === null) ||
      (recoveredStartsAt === null) !== (recoveredAllDay === null)
    ) {
      throw new FlorenceStoreConflict("Stored Calendar evidence has an incomplete interval");
    }
    const startsAt = draft.startsAt ?? recoveredStartsAt;
    const endsAt = draft.endsAt ?? recoveredEndsAt;
    const allDay = draft.allDay ?? recoveredAllDay;
    const title = draft.title ?? (draft.status === "cancelled" ? (priorEvidence?.title ?? null) : null);
    const occurredAt = startsAt ?? draft.providerUpdatedAt;
    await sql`
      insert into sources (
        id,household_id,kind,visibility,owner_adult_id,external_key,label,metadata,occurred_at
      ) values (${draft.id},${draft.householdId},'calendar',${draft.visibility},${draft.ownerAdultId},
        ${draft.externalKey},
        ${bounded(title ?? (draft.status === "cancelled" ? "Cancelled calendar event" : "Private calendar event"), 500)},
        ${sql.json({
          connectionId: draft.connectionId,
          calendarId: draft.calendarId,
          providerEventId: draft.providerEventId,
          providerRevision: draft.providerRevision,
          providerUpdatedAt: draft.providerUpdatedAt,
          status: draft.status,
          busy: draft.busy,
          title,
          startsAt,
          endsAt,
          allDay,
        })},${instant(occurredAt)})
      on conflict (household_id,kind,external_key) do update set
        parent_source_id=null,label=excluded.label,metadata=excluded.metadata,occurred_at=excluded.occurred_at
    `;
    await assertStoredGoogleEvidence(sql, draft);
  }
}

function normalizedGoogleEvidenceDraft(draft: GoogleEvidenceDraft): GoogleEvidenceDraft {
  return draft.kind === "gmail"
    ? draftGmailEvidence({
        householdId: draft.householdId,
        ownerAdultId: draft.connectionOwnerAdultId,
        connectionId: draft.connectionId,
        messageId: draft.messageId,
        threadId: draft.threadId,
        historyId: draft.historyId,
        from: draft.from,
        subject: draft.subject,
        sentAt: draft.sentAt,
      })
    : draftCalendarEvidence({
        householdId: draft.householdId,
        ownerAdultId: draft.connectionOwnerAdultId,
        connectionId: draft.connectionId,
        calendarId: draft.calendarId,
        providerEventId: draft.providerEventId,
        providerRevision: draft.providerRevision,
        providerUpdatedAt: draft.providerUpdatedAt,
        status: draft.status,
        busy: draft.busy,
        title: draft.title,
        startsAt: draft.startsAt,
        endsAt: draft.endsAt,
        allDay: draft.allDay,
        visibility: draft.visibility,
      });
}

async function assertStoredGoogleEvidence(
  sql: postgres.TransactionSql,
  draft: GoogleEvidenceDraft,
): Promise<void> {
  const [stored] = await sql<{ id: string; visibility: Visibility; owner_adult_id: string | null }[]>`
    select id,visibility,owner_adult_id from sources
    where household_id=${draft.householdId} and kind=${draft.kind} and external_key=${draft.externalKey}
    for share
  `;
  if (
    !stored ||
    stored.id !== draft.id ||
    stored.visibility !== draft.visibility ||
    stored.owner_adult_id !== draft.ownerAdultId
  ) {
    throw new FlorenceStoreConflict("A Google provider item conflicts with retained family evidence");
  }
}

function sourceRecord(row: SourceRow): SourceRecord {
  return {
    id: row.id,
    kind: row.kind,
    visibility: row.visibility,
    ownerAdultId: row.owner_adult_id,
    label: row.label,
    metadata: row.metadata,
    occurredAt: row.occurred_at.toISOString(),
  };
}

function conversationAuthorship(
  metadataValue: JsonValue,
): Pick<ConversationTurn, "authoredText" | "voiceTranscriptPresent"> {
  const metadata = jsonRecord(metadataValue);
  const rawAuthoredText = metadata.authoredText;
  const rawVoiceTranscriptPresent = metadata.voiceTranscriptPresent;
  if (rawAuthoredText !== null && typeof rawAuthoredText !== "string") {
    throw new FlorenceStoreConflict("Stored Messages authorship is invalid");
  }
  if (typeof rawVoiceTranscriptPresent !== "boolean") {
    throw new FlorenceStoreConflict("Stored voice transcript state is invalid");
  }
  return { authoredText: rawAuthoredText, voiceTranscriptPresent: rawVoiceTranscriptPresent };
}

function calendarEvidenceRecord(row: SourceRow): CalendarEvidenceRecord {
  if (row.kind !== "calendar") {
    throw new FlorenceStoreConflict("Stored Calendar evidence has the wrong source kind");
  }
  const metadata = jsonRecord(row.metadata);
  const rawStatus = metadata.status;
  const status =
    rawStatus === "confirmed" || rawStatus === "tentative" || rawStatus === "cancelled" ? rawStatus : null;
  if (status === null) {
    throw new FlorenceStoreConflict("Stored Calendar evidence has an invalid status");
  }
  const rawBusy = metadata.busy;
  if (typeof rawBusy !== "boolean" || (status === "cancelled" && rawBusy)) {
    throw new FlorenceStoreConflict("Stored Calendar evidence has an invalid busy state");
  }
  const rawTitle = metadata.title;
  const title = rawTitle === null || typeof rawTitle === "string" ? rawTitle : undefined;
  if (title === undefined) {
    throw new FlorenceStoreConflict("Stored Calendar evidence has an invalid title");
  }
  const rawStartsAt = metadata.startsAt;
  const rawEndsAt = metadata.endsAt;
  if (
    (rawStartsAt !== null && typeof rawStartsAt !== "string") ||
    (rawEndsAt !== null && typeof rawEndsAt !== "string") ||
    (rawStartsAt === null) !== (rawEndsAt === null)
  ) {
    throw new FlorenceStoreConflict("Stored Calendar evidence has an invalid interval");
  }
  const startsAt = rawStartsAt === null ? null : instant(rawStartsAt).toISOString();
  const endsAt = rawEndsAt === null ? null : instant(rawEndsAt).toISOString();
  if (
    (startsAt !== null && endsAt !== null && instant(endsAt) <= instant(startsAt)) ||
    (status !== "cancelled" && (startsAt === null || endsAt === null))
  ) {
    throw new FlorenceStoreConflict("Stored Calendar evidence has an invalid interval");
  }
  const allDay = metadata.allDay;
  if ((allDay !== null && typeof allDay !== "boolean") || (startsAt === null) !== (allDay === null)) {
    throw new FlorenceStoreConflict("Stored Calendar evidence has an invalid all-day state");
  }
  return {
    ...sourceRecord(row),
    status,
    busy: rawBusy,
    title,
    startsAt,
    endsAt,
    allDay,
  };
}

function googleConnectionView(row: GoogleConnectionRow): GoogleConnectionView {
  return {
    connectionId: row.id,
    householdId: row.household_id,
    ownerAdultId: row.owner_adult_id,
    status: row.status,
    emailLabel: row.email_label,
    grantedScopes: [...row.granted_scopes],
    lastError: row.last_error,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}

async function sharedFamilyProfile(
  sql: postgres.TransactionSql,
  householdId: string,
): Promise<SharedFamilyProfile> {
  const [household] = await sql<{ id: string; name: string; time_zone: string }[]>`
    select id,name,time_zone from households where id=${householdId} for share
  `;
  if (!household) throw new FlorenceStoreConflict("The initial intelligence household is missing");
  const people = await sql<
    {
      id: string;
      kind: "adult" | "child";
      adult_slot: 1 | 2 | null;
      display_name: string;
      profile: JsonValue;
    }[]
  >`
    select id,kind,adult_slot,display_name,profile from people where household_id=${householdId}
    order by adult_slot nulls last,created_at,id
  `;
  const founderProfile = jsonRecord(
    people.find((person) => person.kind === "adult" && person.adult_slot === 1)?.profile,
  );
  const postalCode = founderProfile.postalCode;
  return {
    householdId: household.id,
    familyLabel: household.name,
    timeZone: household.time_zone,
    postalCode: typeof postalCode === "string" && postalCode.trim() ? postalCode : null,
    adults: people.flatMap((person) => {
      if (person.kind !== "adult") return [];
      const firstName = jsonRecord(person.profile).firstName;
      return [
        {
          adultId: person.id,
          firstName:
            typeof firstName === "string" && firstName.trim()
              ? firstName.trim()
              : person.display_name.split(/\s+/u)[0] || person.display_name,
          displayName: person.display_name,
        },
      ];
    }),
    children: people.flatMap((person) => {
      if (person.kind !== "child") return [];
      const profile = jsonRecord(person.profile);
      const firstName = profile.firstName;
      const age = profile.age;
      const grade = profile.grade;
      const school = profile.school;
      const activities = profile.activities;
      return [
        {
          childId: person.id,
          firstName:
            typeof firstName === "string" && firstName.trim()
              ? firstName.trim()
              : person.display_name.split(/\s+/u)[0] || person.display_name,
          displayName: person.display_name,
          age: typeof age === "number" && Number.isInteger(age) && age >= 0 && age <= 120 ? age : null,
          grade: typeof grade === "string" && grade.trim() && grade.trim().length <= 80 ? grade.trim() : null,
          school: typeof school === "string" && school.trim() ? school.trim() : null,
          activities: Array.isArray(activities)
            ? activities.filter(
                (value): value is string => typeof value === "string" && value.trim().length > 0,
              )
            : [],
        },
      ];
    }),
  };
}

function proactiveDeliveryTime(value: string, occurredAt: Date): Date {
  const deliverNotBefore = instant(value);
  if (deliverNotBefore < occurredAt) {
    throw new FlorenceStoreConflict("A proactive delivery cannot precede its finding");
  }
  return deliverNotBefore;
}

function isProactiveUrgency(value: string): value is ProactiveDelivery["urgency"] {
  return value === "now" || value === "soon" || value === "watch";
}

async function currentGoogleFacts(
  sql: postgres.TransactionSql,
  householdId: string,
  ownerAdultId: string,
): Promise<readonly GoogleStableFactContext[]> {
  const rows = await sql<{ slot: string; value: JsonValue }[]>`
    select distinct on (slot) slot,value from facts where household_id=${householdId}
      and (visibility='household' or (visibility='private' and owner_adult_id=${ownerAdultId}))
    order by slot,
      case when visibility='household' then 0 else 1 end,
      updated_at desc,id
    limit 100
  `;
  return rows.flatMap((row) => {
    const statement = jsonString(row.value, "statement");
    return statement ? [{ slot: row.slot, statement }] : [];
  });
}

function googleStableFacts(input: readonly GoogleStableFactDraft[]): GoogleStableFactDraft[] {
  const slots = new Set<string>();
  return input.map((fact, index) => {
    const slot = required(fact.slot, `Google fact ${index + 1} slot`);
    if (slot.length > 160 || !/^[a-z0-9][a-z0-9:_-]*$/.test(slot)) {
      throw new FlorenceStoreConflict("A Google fact needs a stable lowercase semantic slot");
    }
    if (slots.has(slot)) {
      throw new FlorenceStoreConflict("A Google review cannot repeat a fact slot");
    }
    slots.add(slot);
    const statement = required(fact.statement, `Google fact ${index + 1} statement`);
    if (statement.length > 2_000) {
      throw new FlorenceStoreConflict("A Google fact statement is too long");
    }
    if (!isHouseholdFactRelevance(fact.familyRelevance)) {
      throw new FlorenceStoreUnauthorized("Adult-only Google evidence cannot become stable memory");
    }
    const sourceIds = unique(fact.sourceIds);
    if (sourceIds.length < 1 || sourceIds.length > 10) {
      throw new FlorenceStoreConflict("A Google fact needs one to ten current sources");
    }
    for (const sourceId of sourceIds) assertUuid(sourceId, "Google fact source ID");
    return { slot, statement, familyRelevance: fact.familyRelevance, sourceIds };
  });
}

function exactReviewedGoogleSources(input: {
  evidence: readonly GoogleEvidenceDraft[];
  review: readonly ReviewedGoogleSourceDisposition[];
  desiredSourceIds: readonly string[];
}): ReadonlyMap<string, ReviewedGoogleSourceDisposition["disposition"]> {
  const evidenceIds = input.evidence.map(({ id }) => id);
  if (new Set(evidenceIds).size !== evidenceIds.length) {
    throw new FlorenceStoreConflict("A Google poll repeated provider evidence");
  }
  const dispositions = new Map<string, ReviewedGoogleSourceDisposition["disposition"]>();
  for (const reviewed of input.review) {
    assertUuid(reviewed.sourceId, "Reviewed Google source ID");
    if (reviewed.disposition !== "retained" && reviewed.disposition !== "dismissed") {
      throw new FlorenceStoreConflict("A Google poll returned an invalid source disposition");
    }
    if (dispositions.has(reviewed.sourceId)) {
      throw new FlorenceStoreConflict("A Google poll repeated a source disposition");
    }
    dispositions.set(reviewed.sourceId, reviewed.disposition);
  }
  if (!sameStringSet([...dispositions.keys()].sort(), [...evidenceIds].sort())) {
    throw new FlorenceStoreConflict("A Google poll did not disposition its exact reviewed evidence");
  }
  const desiredSourceIds = unique(input.desiredSourceIds).sort();
  for (const sourceId of desiredSourceIds) assertUuid(sourceId, "Desired Google source ID");
  const retainedSourceIds = [...dispositions]
    .flatMap(([sourceId, disposition]) => (disposition === "retained" ? [sourceId] : []))
    .sort();
  if (!sameStringSet(retainedSourceIds, desiredSourceIds)) {
    throw new FlorenceStoreConflict(
      "A Google poll source disposition did not match its final approved outcomes",
    );
  }
  return dispositions;
}

function isHouseholdFactRelevance(value: FamilyRelevance): value is Exclude<FamilyRelevance, "adult_only"> {
  return (
    value === "child_care_school_or_activity" ||
    value === "household_logistics" ||
    value === "enrolled_adult_coordination"
  );
}

async function reconcileRemovedGoogleSources(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    ownerAdultId: string | null;
    sourceIds: readonly string[];
  },
): Promise<void> {
  const requested = unique(input.sourceIds);
  for (const sourceId of requested) assertUuid(sourceId, "Removed Google source ID");
  if (requested.length === 0) return;
  const rows = await sql<{ id: string }[]>`
    select id from sources where household_id=${input.householdId}
      and id in ${sql(requested)} and kind in ('gmail','calendar','google_file')
      and owner_adult_id is not distinct from ${input.ownerAdultId}
    order by id for update
  `;
  const sourceIds = rows.map(({ id }) => id);
  if (sourceIds.length === 0) return;
  const removed = new Set(sourceIds);
  const completedPrivateReviews = await sql<ProactiveWorkRow[]>`
    select * from proactive_work work where work.household_id=${input.householdId}
      and work.kind='initial_private_review' and work.status='completed'
    order by work.id for update
  `;
  for (const review of completedPrivateReviews) {
    const candidates = storedBriefingCandidates(review.briefing_candidates);
    const retained = candidates.filter(
      (candidate) => !candidate.sourceIds.some((sourceId) => removed.has(sourceId)),
    );
    if (retained.length === candidates.length) continue;
    await sql`
      update proactive_work set briefing_candidates=${sql.json(retained)}
      where id=${review.id} and kind='initial_private_review' and status='completed'
    `;
  }
  // Delivery metadata keeps the exact provider evidence set even after stored candidates or a
  // finite monitor are gone. A deterministic provider tombstone can therefore cancel every
  // not-yet-delivered copy without relying on mutable model prose or a still-live work row.
  await sql`
    update messages message set status='failed',sending_at=null,retry_at=null,
      last_error=${GOOGLE_SOURCE_REMOVED_BEFORE_DELIVERY}
    from sources outbound_source
    where message.source_id=outbound_source.id
      and outbound_source.household_id=${input.householdId}
      and message.direction='outbound' and message.status='pending'
      and jsonb_typeof(outbound_source.metadata->'googleSourceIds')='array'
      and exists (
        select 1 from jsonb_array_elements_text(outbound_source.metadata->'googleSourceIds') linked(id)
        where linked.id in ${sql(sourceIds)}
      )
  `;
  const impactedFacts = await sql<{ id: string }[]>`
    select distinct fact.id from facts fact join fact_sources link on link.fact_id=fact.id
    where fact.household_id=${input.householdId} and link.source_id in ${sql(sourceIds)}
    order by fact.id for update of fact
  `;
  await sql`delete from fact_sources where source_id in ${sql(sourceIds)}`;
  if (impactedFacts.length > 0) {
    await sql`
      delete from facts fact where fact.id in ${sql(impactedFacts.map(({ id }) => id))}
        and not exists (select 1 from fact_sources link where link.fact_id=fact.id)
    `;
  }
  const impactedWork = await sql<{ id: string; last_error: string | null }[]>`
    select distinct work.id,work.last_error from proactive_work work
    join proactive_work_sources link on link.work_id=work.id
    where work.household_id=${input.householdId} and work.kind in ('finite_monitor','interest_monitor')
      and link.source_id in ${sql(sourceIds)}
    order by work.id for update of work
  `;
  await sql`delete from proactive_work_sources where source_id in ${sql(sourceIds)}`;
  if (impactedWork.length > 0) {
    const orphaned = await sql<{ id: string; last_error: string | null }[]>`
      select id,last_error from proactive_work work
      where work.id in ${sql(impactedWork.map(({ id }) => id))}
        and not exists (select 1 from proactive_work_sources link where link.work_id=work.id)
      order by id for update
    `;
    for (const work of orphaned) {
      const actionKey = googleActionKeyFromWorkMarker(work.last_error);
      if (actionKey) {
        await failPendingGoogleActionOutbounds(sql, {
          householdId: input.householdId,
          actionKey,
          reason: GOOGLE_SOURCE_REMOVED_BEFORE_DELIVERY,
        });
      }
    }
    if (orphaned.length > 0) {
      await sql`
        delete from proactive_work work where work.id in ${sql(orphaned.map(({ id }) => id))}
      `;
    }
  }
  // A committed mutation, or one explicitly approved by a parent, is user-owned history. A
  // pending action with no approval source is automatic, so it must disappear with stale provider
  // evidence before the Calendar executor can act on it.
  const obsoleteActions = await sql<{ id: string; approval_prompt_source_id: string | null }[]>`
    select action.id,action.approval_prompt_source_id from calendar_actions action
    where action.household_id=${input.householdId}
      and action.basis_source_id in ${sql(sourceIds)}
      and action.status in ('offered','pending','failed')
      and action.approval_source_id is null
      and not (
        action.status='pending' and action.last_error=${CALENDAR_ACTION_EXECUTION_MARKER}
        and action.retry_at>now()
      )
    order by action.id for update
  `;
  const promptSourceIds = obsoleteActions.flatMap(({ approval_prompt_source_id }) =>
    approval_prompt_source_id ? [approval_prompt_source_id] : [],
  );
  if (promptSourceIds.length > 0) {
    await sql`
      update messages set status='failed',sending_at=null,retry_at=null,
        last_error='The underlying Google item no longer requires this Calendar offer'
      where source_id in ${sql(unique(promptSourceIds))} and direction='outbound'
        and status='pending'
    `;
  }
  if (obsoleteActions.length > 0) {
    await sql`
      delete from calendar_actions where id in ${sql(obsoleteActions.map(({ id }) => id))}
        and household_id=${input.householdId} and approval_source_id is null
        and status in ('offered','pending','failed')
        and not (
          status='pending' and last_error=${CALENDAR_ACTION_EXECUTION_MARKER} and retry_at>now()
        )
    `;
  }
  await sql`
    delete from sources source where source.id in ${sql(sourceIds)}
      and not exists (select 1 from fact_sources link where link.source_id=source.id)
      and not exists (select 1 from proactive_work_sources link where link.source_id=source.id)
      and not exists (select 1 from calendar_actions action where action.basis_source_id=source.id)
  `;
}

async function reconcileReviewedGoogleSources(
  sql: postgres.TransactionSql,
  input: {
    work: ProactiveWorkRow;
    reviewedSourceIds: readonly string[];
    facts: readonly GoogleStableFactDraft[];
    desiredState: GooglePollDesiredState;
  },
): Promise<void> {
  const sourceIds = unique(input.reviewedSourceIds);
  for (const sourceId of sourceIds) assertUuid(sourceId, "Reviewed Google source ID");
  if (sourceIds.length === 0) return;
  const sources = await sql<{ id: string }[]>`
    select id from sources where household_id=${input.work.household_id}
      and id in ${sql(sourceIds)} and kind in ('gmail','calendar','google_file')
      and owner_adult_id is not distinct from ${input.work.owner_adult_id}
    order by id for update
  `;
  if (!sameStringSet(sources.map(({ id }) => id).sort(), [...sourceIds].sort())) {
    throw new FlorenceStoreConflict(
      "A reviewed Google revision was not retained under its exact polling authority",
    );
  }

  const desiredFacts = new Map(
    input.facts.map((fact) => [`${fact.slot}\0${fact.statement}`, new Set(fact.sourceIds)] as const),
  );
  const existingFactSupports = await sql<
    { fact_id: string; slot: string; statement: string | null; source_id: string }[]
  >`
    select fact.id as fact_id,fact.slot,fact.value->>'statement' as statement,
      link.source_id
    from facts fact join fact_sources link on link.fact_id=fact.id
    where fact.household_id=${input.work.household_id} and link.source_id in ${sql(sourceIds)}
    order by fact.id,link.source_id for update of fact
  `;
  const staleFactSupports = existingFactSupports.filter(
    (support) =>
      support.statement === null ||
      !desiredFacts.get(`${support.slot}\0${support.statement}`)?.has(support.source_id),
  );
  for (const support of staleFactSupports) {
    await sql`
      delete from fact_sources where fact_id=${support.fact_id} and source_id=${support.source_id}
    `;
  }
  const impactedFactIds = unique(staleFactSupports.map(({ fact_id }) => fact_id));
  if (impactedFactIds.length > 0) {
    await sql`
      delete from facts fact where fact.id in ${sql(impactedFactIds)}
        and not exists (select 1 from fact_sources link where link.fact_id=fact.id)
    `;
  }

  const desiredPollSourceIds = input.desiredState.deliverySourceIds;
  const stalePollSourceIds = sourceIds.filter((sourceId) => !desiredPollSourceIds.has(sourceId));
  if (stalePollSourceIds.length > 0) {
    await sql`
      delete from proactive_work_sources where work_id=${input.work.id}
        and source_id in ${sql(stalePollSourceIds)}
    `;
  }

  const pendingOutbounds = await sql<
    {
      message_id: string;
      text: string | null;
      visibility: Visibility;
      owner_adult_id: string | null;
      metadata: JsonValue;
    }[]
  >`
    select message.source_id as message_id,message.text,outbound_source.visibility,
      outbound_source.owner_adult_id,outbound_source.metadata
    from messages message join sources outbound_source on outbound_source.id=message.source_id
    where outbound_source.household_id=${input.work.household_id}
      and message.direction='outbound' and message.status='pending'
      and jsonb_typeof(outbound_source.metadata->'googleSourceIds')='array'
      and exists (
        select 1 from jsonb_array_elements_text(outbound_source.metadata->'googleSourceIds') linked(id)
        where linked.id in ${sql(sourceIds)}
      )
    order by message.source_id for update of message,outbound_source
  `;
  const obsoleteMessageIds = pendingOutbounds.flatMap((outbound) => {
    const metadata = jsonRecord(outbound.metadata);
    const actionKeys = Array.isArray(metadata.googleActionKeys)
      ? metadata.googleActionKeys.filter(
          (value): value is string => typeof value === "string" && /^[a-f0-9]{64}$/u.test(value),
        )
      : [];
    const desiredOutcomes =
      outbound.visibility === "private" && outbound.owner_adult_id === input.work.owner_adult_id
        ? input.desiredState.privateOutboundOutcomes
        : outbound.visibility === "household" && outbound.owner_adult_id === null
          ? input.desiredState.householdOutboundOutcomes
          : new Set<string>();
    return outbound.text !== null &&
      actionKeys.some((actionKey) => {
        const urgency = googleActionUrgencyFromMetadata(metadata, actionKey);
        return urgency
          ? desiredOutcomes.has(googleOutboundOutcomeIdentity(actionKey, outbound.text as string, urgency))
          : (["now", "soon", "watch"] as const).some((legacyUrgency) =>
              desiredOutcomes.has(
                googleOutboundOutcomeIdentity(actionKey, outbound.text as string, legacyUrgency),
              ),
            );
      })
      ? []
      : [outbound.message_id];
  });
  if (obsoleteMessageIds.length > 0) {
    await sql`
      update messages set status='failed',sending_at=null,retry_at=null,
        last_error=${GOOGLE_SOURCE_REVISED_BEFORE_DELIVERY}
      where source_id in ${sql(obsoleteMessageIds)} and direction='outbound' and status='pending'
    `;
  }

  const linkedMonitors = await sql<{ id: string; last_error: string | null }[]>`
    select work.id,work.last_error from proactive_work work
    where work.household_id=${input.work.household_id}
      and work.kind='finite_monitor' and work.status in ('active','paused')
      and left(coalesce(work.last_error,''),${GOOGLE_ACTION_WORK_MARKER_PREFIX.length})=
        ${GOOGLE_ACTION_WORK_MARKER_PREFIX}
      and exists (
        select 1 from proactive_work_sources link where link.work_id=work.id
          and link.source_id in ${sql(sourceIds)}
      )
    order by work.id for update of work
  `;
  for (const monitor of linkedMonitors) {
    const actionKey = googleActionKeyFromWorkMarker(monitor.last_error);
    const desiredSourceIds = actionKey ? input.desiredState.monitorActions.get(actionKey) : undefined;
    if (!actionKey || !desiredSourceIds) {
      if (actionKey) {
        await failPendingGoogleActionOutbounds(sql, {
          householdId: input.work.household_id,
          actionKey,
          reason: GOOGLE_SOURCE_REVISED_BEFORE_DELIVERY,
        });
      }
      await sql`
        delete from proactive_work where id=${monitor.id}
          and household_id=${input.work.household_id} and kind='finite_monitor'
          and status in ('active','paused')
      `;
      continue;
    }
    const desired = new Set(desiredSourceIds);
    const staleSourceIds = sourceIds.filter((sourceId) => !desired.has(sourceId));
    if (staleSourceIds.length > 0) {
      await sql`
        delete from proactive_work_sources where work_id=${monitor.id}
          and source_id in ${sql(staleSourceIds)}
      `;
    }
  }

  const calendarActions = await sql<
    {
      id: string;
      approval_prompt_source_id: string | null;
      payload: JsonValue;
      source_id: string;
      kind: SourceRow["kind"];
      owner_adult_id: string | null;
      metadata: JsonValue;
    }[]
  >`
    select action.id,action.approval_prompt_source_id,action.payload,
      basis.id as source_id,basis.kind,basis.owner_adult_id,basis.metadata
    from calendar_actions action join sources basis on basis.id=action.basis_source_id
    where action.household_id=${input.work.household_id}
      and action.basis_source_id in ${sql(sourceIds)}
      and action.status in ('offered','pending','failed')
      and action.approval_source_id is null
      and not (
        action.status='pending' and action.last_error=${CALENDAR_ACTION_EXECUTION_MARKER}
        and action.retry_at>now()
      )
    order by action.created_at,action.id for update of action,basis
  `;
  for (const action of calendarActions) {
    const providerDigest = stableGoogleProviderDigest({
      id: action.source_id,
      kind: action.kind,
      owner_adult_id: action.owner_adult_id,
      metadata: jsonRecord(action.metadata),
    });
    const mutation = familyCalendarMutation(action.payload);
    if (
      providerDigest &&
      mutation.operation === "create" &&
      input.desiredState.calendarProposalSignatures.has(
        calendarProposalBasisSignature(providerDigest, mutation.event),
      )
    ) {
      continue;
    }
    if (action.approval_prompt_source_id) {
      await sql`
        update messages set status='failed',sending_at=null,retry_at=null,
          last_error='The underlying Google item no longer requires this Calendar offer'
        where source_id=${action.approval_prompt_source_id} and direction='outbound'
          and status='pending'
      `;
    }
    await sql`
      delete from calendar_actions where id=${action.id}
        and household_id=${input.work.household_id} and approval_source_id is null
        and status in ('offered','pending','failed')
        and not (
          status='pending' and last_error=${CALENDAR_ACTION_EXECUTION_MARKER} and retry_at>now()
        )
    `;
  }

  await sql`
    delete from sources source where source.id in ${sql(sourceIds)}
      and source.household_id=${input.work.household_id}
      and source.kind in ('gmail','calendar','google_file')
      and not exists (select 1 from fact_sources link where link.source_id=source.id)
      and not exists (select 1 from proactive_work_sources link where link.source_id=source.id)
      and not exists (select 1 from calendar_actions action where action.basis_source_id=source.id)
  `;
}

async function reconcileAuthoritativeGoogleFactSupports(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    gmailAfter: string;
    reviewedThrough: Date;
    calendarTimeMin: string;
    calendarTimeMax: string;
    facts: readonly GoogleStableFactDraft[];
  },
): Promise<void> {
  const desiredBySlot = new Map(input.facts.map((fact) => [fact.slot, new Set(fact.sourceIds)] as const));
  const supports = await sql<
    {
      fact_id: string;
      slot: string;
      source_id: string;
      kind: "gmail" | "calendar" | "google_file";
      metadata: JsonValue;
      occurred_at: Date;
    }[]
  >`
    select fact.id as fact_id,fact.slot,source.id as source_id,source.kind,source.metadata,
      source.occurred_at
    from facts fact join fact_sources link on link.fact_id=fact.id
    join sources source on source.id=link.source_id
    join google_connections historical
      on historical.id::text=source.metadata->>'connectionId'
      and historical.household_id=source.household_id
      and historical.owner_adult_id=source.owner_adult_id
    join google_connections current on current.id=${input.connectionId}
      and current.household_id=historical.household_id
      and current.owner_adult_id=historical.owner_adult_id and current.status='active'
      and historical.google_subject_digest is not null
      and current.google_subject_digest is not null
      and current.google_subject_digest=historical.google_subject_digest
    where fact.household_id=${input.householdId} and source.owner_adult_id=${input.ownerAdultId}
      and source.kind in ('gmail','calendar','google_file')
    order by fact.id,source.id for update of fact,source,historical,current
  `;
  const staleByFact = new Map<string, string[]>();
  for (const support of supports) {
    const retainedWindowSupport = googleSourceFallsInsideCompletedScan(support, input);
    if (!retainedWindowSupport || desiredBySlot.get(support.slot)?.has(support.source_id)) continue;
    const stale = staleByFact.get(support.fact_id) ?? [];
    stale.push(support.source_id);
    staleByFact.set(support.fact_id, stale);
  }
  for (const [factId, sourceIds] of staleByFact) {
    await sql`
      delete from fact_sources where fact_id=${factId} and source_id in ${sql(unique(sourceIds))}
    `;
  }
  if (staleByFact.size > 0) {
    await sql`
      delete from facts fact where fact.id in ${sql([...staleByFact.keys()])}
        and not exists (select 1 from fact_sources link where link.fact_id=fact.id)
    `;
  }
}

async function reconcileAuthoritativeGoogleMonitors(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    gmailAfter: string;
    reviewedThrough: Date;
    calendarTimeMin: string;
    calendarTimeMax: string;
    desiredActions: ReadonlyMap<string, readonly string[]>;
  },
): Promise<void> {
  const [current] = await sql<{ google_subject_digest: string | null }[]>`
    select google_subject_digest from google_connections where id=${input.connectionId}
      and household_id=${input.householdId} and owner_adult_id=${input.ownerAdultId}
      and status='active' for share
  `;
  if (!current?.google_subject_digest) {
    throw new FlorenceStoreUnauthorized("Google monitor reconciliation needs the active account identity");
  }
  const works = await sql<{ id: string; status: "active" | "paused"; last_error: string | null }[]>`
    select id,status,last_error from proactive_work where household_id=${input.householdId}
      and owner_adult_id=${input.ownerAdultId} and visibility='private'
      and kind='finite_monitor' and status in ('active','paused')
    order by created_at,id for update
  `;
  const tagged = works.flatMap((work) => {
    const actionKey = googleActionKeyFromWorkMarker(work.last_error);
    return actionKey ? [{ id: work.id, status: work.status, actionKey }] : [];
  });
  if (tagged.length === 0) return;
  const workIds = tagged.map(({ id }) => id);
  const links = await sql<
    {
      work_id: string;
      source_id: string;
      kind: SourceRow["kind"];
      metadata: JsonValue;
      occurred_at: Date;
      historical_subject_digest: string | null;
    }[]
  >`
    select link.work_id,source.id as source_id,source.kind,source.metadata,source.occurred_at,
      historical.google_subject_digest as historical_subject_digest
    from proactive_work_sources link join sources source on source.id=link.source_id
    join google_connections historical
      on historical.id::text=source.metadata->>'connectionId'
      and historical.household_id=source.household_id
      and historical.owner_adult_id=source.owner_adult_id
    where link.work_id in ${sql(workIds)}
    order by link.work_id,source.id for update of source,historical
  `;
  const linksByWork = new Map<string, (typeof links)[number][]>();
  for (const link of links) {
    const currentLinks = linksByWork.get(link.work_id) ?? [];
    currentLinks.push(link);
    linksByWork.set(link.work_id, currentLinks);
  }
  const sourceIdsToCollect: string[] = [];
  for (const work of tagged) {
    const workLinks = linksByWork.get(work.id) ?? [];
    if (
      workLinks.length === 0 ||
      workLinks.some(
        (link) =>
          link.historical_subject_digest !== current.google_subject_digest ||
          !googleSourceFallsInsideCompletedScan(link, input),
      )
    ) {
      continue;
    }
    const desiredSourceIds = input.desiredActions.get(work.actionKey);
    if (!desiredSourceIds) {
      await failPendingGoogleActionOutbounds(sql, {
        householdId: input.householdId,
        actionKey: work.actionKey,
        reason: "The underlying Google item no longer requires this message",
      });
      sourceIdsToCollect.push(...workLinks.map(({ source_id }) => source_id));
      await sql`
        delete from proactive_work where id=${work.id} and household_id=${input.householdId}
          and kind='finite_monitor' and status in ('active','paused')
      `;
      continue;
    }
    const desired = new Set(desiredSourceIds);
    const staleSourceIds = workLinks
      .map(({ source_id }) => source_id)
      .filter((sourceId) => !desired.has(sourceId));
    if (staleSourceIds.length > 0) {
      sourceIdsToCollect.push(...staleSourceIds);
      await sql`
        delete from proactive_work_sources where work_id=${work.id}
          and source_id in ${sql(unique(staleSourceIds))}
      `;
    }
  }
  if (sourceIdsToCollect.length > 0) {
    const sourceIds = unique(sourceIdsToCollect);
    await sql`
      delete from sources source where source.id in ${sql(sourceIds)}
        and source.household_id=${input.householdId}
        and source.kind in ('gmail','calendar','google_file')
        and not exists (select 1 from proactive_work_sources link where link.source_id=source.id)
        and not exists (select 1 from fact_sources link where link.source_id=source.id)
        and not exists (select 1 from calendar_actions action where action.basis_source_id=source.id)
    `;
  }
}

function calendarProposalBasisSignature(providerDigest: string, event: CalendarEventDraft): string {
  assertDigest(providerDigest, "Google Calendar proposal provider");
  validateCalendarEvent(event);
  return sha256(JSON.stringify([providerDigest, calendarEventIdentity(event)]));
}

function calendarEventIdentity(event: CalendarEventDraft): readonly (string | null)[] {
  return event.intervalKind === "all_day"
    ? [event.intervalKind, event.title, event.startDate, event.endDate, event.location]
    : [
        event.intervalKind,
        event.title,
        new Date(event.startsAt).toISOString(),
        new Date(event.endsAt).toISOString(),
        event.timeZone,
        event.location,
      ];
}

async function reconcileAuthoritativeGoogleCalendarProposals(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    gmailAfter: string;
    reviewedThrough: Date;
    calendarTimeMin: string;
    calendarTimeMax: string;
    desiredSignatures: ReadonlySet<string>;
  },
): Promise<void> {
  const [current] = await sql<{ google_subject_digest: string | null }[]>`
    select google_subject_digest from google_connections where id=${input.connectionId}
      and household_id=${input.householdId} and owner_adult_id=${input.ownerAdultId}
      and status='active' for share
  `;
  if (!current?.google_subject_digest) {
    throw new FlorenceStoreUnauthorized("Google Calendar proposal reconciliation needs account identity");
  }
  const actions = await sql<
    {
      id: string;
      approval_prompt_source_id: string | null;
      payload: JsonValue;
      source_id: string;
      kind: SourceRow["kind"];
      metadata: JsonValue;
      occurred_at: Date;
      historical_subject_digest: string | null;
      owner_adult_id: string | null;
    }[]
  >`
    select action.id,action.approval_prompt_source_id,action.payload,basis.id as source_id,
      basis.kind,basis.metadata,basis.occurred_at,
      historical.google_subject_digest as historical_subject_digest,basis.owner_adult_id
    from calendar_actions action join sources basis on basis.id=action.basis_source_id
    join google_connections historical
      on historical.id::text=basis.metadata->>'connectionId'
      and historical.household_id=basis.household_id
      and historical.owner_adult_id=basis.owner_adult_id
    where action.household_id=${input.householdId}
      and action.status in ('offered','pending','failed')
      and action.approval_source_id is null and basis.visibility='private'
      and basis.owner_adult_id=${input.ownerAdultId}
      and not (
        action.status='pending' and action.last_error=${CALENDAR_ACTION_EXECUTION_MARKER}
        and action.retry_at>now()
      )
    order by action.created_at,action.id for update of action,basis,historical
  `;
  for (const action of actions) {
    if (
      action.historical_subject_digest !== current.google_subject_digest ||
      !googleSourceFallsInsideCompletedScan(action, input)
    ) {
      continue;
    }
    const basisDigest = stableGoogleProviderDigest({
      id: action.source_id,
      kind: action.kind,
      owner_adult_id: action.owner_adult_id,
      metadata: jsonRecord(action.metadata),
    });
    const mutation = familyCalendarMutation(action.payload);
    if (
      !basisDigest ||
      mutation.operation !== "create" ||
      input.desiredSignatures.has(calendarProposalBasisSignature(basisDigest, mutation.event))
    ) {
      continue;
    }
    if (action.approval_prompt_source_id) {
      await sql`
        update messages set status='failed',sending_at=null,retry_at=null,
          last_error='The underlying Google item no longer requires this Calendar offer'
        where source_id=${action.approval_prompt_source_id} and direction='outbound'
          and status='pending'
      `;
    }
    await sql`
      delete from calendar_actions where id=${action.id} and household_id=${input.householdId}
        and status in ('offered','pending','failed') and approval_source_id is null
        and not (
          status='pending' and last_error=${CALENDAR_ACTION_EXECUTION_MARKER} and retry_at>now()
        )
    `;
  }
}

async function failPendingGoogleActionOutbounds(
  sql: postgres.TransactionSql,
  input: { householdId: string; actionKey: string; reason: string },
): Promise<void> {
  assertDigest(input.actionKey, "Google action key");
  await sql`
    update messages message set status='failed',sending_at=null,retry_at=null,
      last_error=${bounded(input.reason, 500)}
    from sources source where source.id=message.source_id
      and source.household_id=${input.householdId} and message.direction='outbound'
      and message.status='pending'
      and jsonb_typeof(source.metadata->'googleActionKeys')='array'
      and jsonb_exists(source.metadata->'googleActionKeys',${input.actionKey})
  `;
}

function googleSourceFallsInsideCompletedScan(
  source: { kind: SourceRow["kind"]; metadata: JsonValue; occurred_at: Date },
  bounds: {
    gmailAfter: string;
    reviewedThrough: Date;
    calendarTimeMin: string;
    calendarTimeMax: string;
  },
): boolean {
  const metadata = jsonRecord(source.metadata);
  if (source.kind === "gmail") {
    return (
      typeof metadata.sentAt === "string" &&
      Date.parse(metadata.sentAt) >= Date.parse(bounds.gmailAfter) &&
      Date.parse(metadata.sentAt) <= bounds.reviewedThrough.getTime()
    );
  }
  if (source.kind !== "calendar") return false;
  const intervalStart = typeof metadata.startsAt === "string" ? Date.parse(metadata.startsAt) : Number.NaN;
  const intervalEnd = typeof metadata.endsAt === "string" ? Date.parse(metadata.endsAt) : Number.NaN;
  return (
    Number.isFinite(intervalStart) &&
    Number.isFinite(intervalEnd) &&
    intervalEnd > Date.parse(bounds.calendarTimeMin) &&
    intervalStart < Date.parse(bounds.calendarTimeMax)
  );
}

async function upsertGoogleStableFacts(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    currentEvidenceSourceIds: readonly string[];
    facts: readonly GoogleStableFactDraft[];
    occurredAt: Date;
  },
): Promise<void> {
  if (input.facts.length === 0) return;
  const [activeConnection] = await sql<{ id: string }[]>`
    select id from google_connections where id=${input.connectionId}
      and household_id=${input.householdId} and owner_adult_id=${input.ownerAdultId}
      and status='active' for share
  `;
  if (!activeConnection) {
    throw new FlorenceStoreUnauthorized("Stable Google facts require the exact active parent account");
  }
  const currentEvidenceSourceIds = new Set(input.currentEvidenceSourceIds);
  const factSourceIds = unique(input.facts.flatMap((fact) => [...fact.sourceIds]));
  if (factSourceIds.some((sourceId) => !currentEvidenceSourceIds.has(sourceId))) {
    throw new FlorenceStoreUnauthorized("A stable Google fact cited evidence outside this exact review");
  }
  await assertProactiveSources(sql, input.householdId, "private", input.ownerAdultId, factSourceIds);
  const sourceRows = await sql<
    {
      id: string;
      kind: SourceRecord["kind"];
      visibility: Visibility;
      owner_adult_id: string | null;
      connection_id: string | null;
    }[]
  >`
    select id,kind,visibility,owner_adult_id,metadata->>'connectionId' as connection_id
    from sources where household_id=${input.householdId} and id in ${sql(factSourceIds)}
    for share
  `;
  if (sourceRows.length !== factSourceIds.length) {
    throw new FlorenceStoreUnauthorized("A stable Google fact cited unavailable evidence");
  }
  const sourceAuthority = new Map(sourceRows.map((source) => [source.id, source]));
  for (const fact of input.facts) {
    const householdEligible = fact.sourceIds.every((sourceId) => {
      const source = sourceAuthority.get(sourceId);
      return (
        source?.kind === "gmail" &&
        source.visibility === "private" &&
        source.owner_adult_id === input.ownerAdultId &&
        source.connection_id === input.connectionId
      );
    });
    if (!householdEligible) {
      const [shared] = await sql<{ id: string }[]>`
        select id from facts where household_id=${input.householdId} and slot=${fact.slot}
          and visibility='household' and owner_adult_id is null for update
      `;
      if (shared) continue;
      const [existing] = await sql<{ id: string; corrected_at: Date | null }[]>`
        select id,corrected_at from facts where household_id=${input.householdId} and slot=${fact.slot}
          and visibility='private' and owner_adult_id=${input.ownerAdultId}
        for update
      `;
      const factId =
        existing?.id ?? deterministicUuid(`private-google-fact\0${input.ownerAdultId}\0${fact.slot}`);
      if (existing && (await factHasExplicitCorrection(sql, existing.id, existing.corrected_at))) {
        continue;
      }
      if (existing) {
        await sql`
          update facts set label=${fact.statement.slice(0, 160)},value=${sql.json({
            statement: fact.statement,
          })},corrected_at=null,updated_at=${input.occurredAt}
          where id=${factId}
        `;
        await sql`delete from fact_sources where fact_id=${factId}`;
      } else {
        await sql`
          insert into facts (
            id,household_id,subject_person_id,kind,slot,label,value,visibility,owner_adult_id,
            created_at,updated_at
          ) values (${factId},${input.householdId},null,'general',${fact.slot},
            ${fact.statement.slice(0, 160)},${sql.json({ statement: fact.statement })},'private',
            ${input.ownerAdultId},${input.occurredAt},${input.occurredAt})
        `;
      }
      for (const sourceId of fact.sourceIds) {
        await sql`insert into fact_sources (fact_id,source_id) values (${factId},${sourceId})`;
      }
      continue;
    }

    const [privateFact] = await sql<{ id: string; corrected_at: Date | null }[]>`
      select id,corrected_at from facts where household_id=${input.householdId} and slot=${fact.slot}
        and visibility='private' and owner_adult_id=${input.ownerAdultId}
      for update
    `;
    if (privateFact && (await factHasExplicitCorrection(sql, privateFact.id, privateFact.corrected_at))) {
      continue;
    }
    const [existing] = await sql<{ id: string; value: JsonValue; corrected_at: Date | null }[]>`
      select id,value,corrected_at from facts where household_id=${input.householdId}
        and slot=${fact.slot} and visibility='household' and owner_adult_id is null
      for update
    `;
    const factId =
      existing?.id ?? deterministicUuid(`household-google-fact\0${input.householdId}\0${fact.slot}`);
    if (existing && (await factHasExplicitCorrection(sql, existing.id, existing.corrected_at))) {
      continue;
    }
    if (existing) {
      const existingStatement = jsonString(existing.value, "statement");
      if (existingStatement !== fact.statement) {
        const existingSourceOwners = await sql<{ owner_adult_id: string | null }[]>`
          select source.owner_adult_id from fact_sources link
          join sources source on source.id=link.source_id
          where link.fact_id=${existing.id} for share of source
        `;
        if (existingSourceOwners.some((source) => source.owner_adult_id !== input.ownerAdultId)) {
          continue;
        }
        await sql`delete from fact_sources where fact_id=${factId}`;
      }
      await sql`
        update facts set label=${fact.statement.slice(0, 160)},value=${sql.json({
          statement: fact.statement,
        })},corrected_at=null,updated_at=${input.occurredAt}
        where id=${factId}
      `;
    } else {
      await sql`
        insert into facts (
          id,household_id,subject_person_id,kind,slot,label,value,visibility,owner_adult_id,
          created_at,updated_at
        ) values (${factId},${input.householdId},null,'general',${fact.slot},
          ${fact.statement.slice(0, 160)},${sql.json({ statement: fact.statement })},'household',
          null,${input.occurredAt},${input.occurredAt})
      `;
    }
    for (const sourceId of fact.sourceIds) {
      await sql`
        insert into fact_sources (fact_id,source_id) values (${factId},${sourceId})
        on conflict do nothing
      `;
    }
    if (privateFact) {
      await sql`delete from facts where id=${privateFact.id}`;
    }
  }
}

async function factHasExplicitCorrection(
  sql: postgres.TransactionSql,
  factId: string,
  correctedAt: Date | null,
): Promise<boolean> {
  if (correctedAt === null) return false;
  const [correction] = await sql<{ id: string }[]>`
    select source.id from fact_sources link join sources source on source.id=link.source_id
    where link.fact_id=${factId} and source.kind not in ('gmail','calendar','google_file')
    order by source.occurred_at desc,source.id limit 1 for share of source
  `;
  return correction !== undefined;
}

async function activeFiniteMonitors(
  sql: postgres.TransactionSql,
  householdId: string,
  visibility: Visibility,
  ownerAdultId: string | null,
): Promise<readonly ActiveFiniteMonitor[]> {
  const rows = await sql<ProactiveWorkRow[]>`
    select * from proactive_work where household_id=${householdId} and kind='finite_monitor'
      and status='active' and visibility=${visibility}
      and owner_adult_id is not distinct from ${ownerAdultId}
    order by next_check_at,id limit 20 for share
  `;
  return rows.map(activeFiniteMonitor);
}

function activeFiniteMonitor(row: ProactiveWorkRow): ActiveFiniteMonitor {
  if (!row.objective || !row.current_conclusion || !row.end_condition || !row.next_check_at || !row.why) {
    throw new FlorenceStoreConflict("A finite monitor has incomplete state");
  }
  return {
    monitorId: row.id,
    status: "active",
    objective: row.objective,
    currentConclusion: row.current_conclusion,
    endCondition: row.end_condition,
    nextCheck: row.next_check_at.toISOString(),
    why: row.why,
  };
}

async function assertProactiveSources(
  sql: postgres.TransactionSql,
  householdId: string,
  visibility: Visibility,
  ownerAdultId: string | null,
  sourceIds: readonly string[],
): Promise<void> {
  const ids = unique(sourceIds);
  const rows = await sql<{ id: string }[]>`
    select id from sources where household_id=${householdId} and id in ${sql(ids)}
      and ((${visibility}='household' and visibility='household' and owner_adult_id is null)
        or (${visibility}='private' and visibility='private' and owner_adult_id=${ownerAdultId}))
    for share
  `;
  if (rows.length !== ids.length) {
    throw new FlorenceStoreUnauthorized("Proactive work cited evidence outside its audience");
  }
}

async function withoutExactCommittedCalendarEchoes(
  sql: postgres.TransactionSql,
  householdId: string,
  drafts: readonly GoogleEvidenceDraft[],
  deliveries: readonly ProactiveDelivery[],
): Promise<{
  deliveries: readonly ProactiveDelivery[];
  echoedSourceIds: readonly string[];
}> {
  const latestCalendarDraftBySource = new Map<string, CalendarEvidenceDraft>();
  for (const draft of drafts) {
    if (draft.kind !== "calendar") continue;
    const current = latestCalendarDraftBySource.get(draft.id);
    if (
      !current ||
      draft.providerUpdatedAt > current.providerUpdatedAt ||
      (draft.providerUpdatedAt === current.providerUpdatedAt &&
        draft.providerRevision > current.providerRevision)
    ) {
      latestCalendarDraftBySource.set(draft.id, draft);
    }
  }
  const calendarDrafts = [...latestCalendarDraftBySource.values()];
  if (calendarDrafts.length === 0) return { deliveries, echoedSourceIds: [] };
  const providerEventIds = unique(calendarDrafts.map((draft) => draft.providerEventId));
  const committed = await sql<
    {
      provider_event_id: string;
      payload: JsonValue;
      time_zone: string;
      calendar_id: string;
    }[]
  >`
    select distinct on (action.provider_event_id)
      action.provider_event_id,action.payload,household.time_zone,
      household.family_calendar_id as calendar_id
    from calendar_actions action
    join households household on household.id=action.household_id
    where action.household_id=${householdId} and action.status='committed'
      and action.provider_event_id in ${sql(providerEventIds)}
      and action.provider_event_id is not null and action.provider_etag is not null
      and household.family_calendar_id is not null
    order by action.provider_event_id,action.committed_at desc,action.created_at desc,action.id desc
  `;
  const latestCommittedByEvent = new Map(
    committed.map((action) => [action.provider_event_id, action] as const),
  );
  const echoedSourceIds = unique(
    calendarDrafts.flatMap((draft) => {
      const action = latestCommittedByEvent.get(draft.providerEventId);
      if (!action) return [];
      const mutation = familyCalendarMutation(action.payload);
      return (mutation.operation === "create" || mutation.operation === "update") &&
        sameCommittedCalendarEvidence(draft, mutation.event, action.time_zone, action.calendar_id)
        ? [draft.id]
        : [];
    }),
  );
  const echoed = new Set(echoedSourceIds);
  const retainedDeliveries = deliveries.flatMap((delivery) => {
    const sourceIds = unique(delivery.sourceIds);
    if (sourceIds.length === 0) return [delivery];
    const nonEchoSourceIds = sourceIds.filter((sourceId) => !echoed.has(sourceId));
    if (nonEchoSourceIds.length === 0) return [];
    return nonEchoSourceIds.length === sourceIds.length
      ? [delivery]
      : [{ ...delivery, actionSourceIds: nonEchoSourceIds }];
  });
  const retainedSourceIds = new Set(retainedDeliveries.flatMap((delivery) => [...delivery.sourceIds]));
  return {
    deliveries: retainedDeliveries,
    echoedSourceIds: echoedSourceIds.filter((sourceId) => !retainedSourceIds.has(sourceId)),
  };
}

function sameCommittedCalendarEvidence(
  observed: CalendarEvidenceDraft,
  committed: CalendarEventDraft,
  householdTimeZone: string,
  familyCalendarId: string,
): boolean {
  if (
    observed.calendarId !== familyCalendarId ||
    observed.status !== "confirmed" ||
    !observed.busy ||
    observed.title !== committed.title ||
    !observed.startsAt ||
    !observed.endsAt ||
    observed.allDay === null
  ) {
    return false;
  }
  if (committed.intervalKind === "all_day") {
    return (
      observed.allDay &&
      calendarDateInTimeZone(observed.startsAt, householdTimeZone) === committed.startDate &&
      calendarDateInTimeZone(observed.endsAt, householdTimeZone) === committed.endDate
    );
  }
  return (
    !observed.allDay &&
    Date.parse(observed.startsAt) === Date.parse(committed.startsAt) &&
    Date.parse(observed.endsAt) === Date.parse(committed.endsAt)
  );
}

type StableGoogleSourceRow = {
  id: string;
  kind: SourceRow["kind"];
  owner_adult_id: string | null;
  metadata: JsonObject;
};

type StableGoogleSourceIdentity = {
  providerDigest: string;
  calendarStateDigest: string | null;
};

async function readStableGoogleSourceDigestMap(
  sql: postgres.TransactionSql,
  householdId: string,
  sourceIds: readonly string[],
): Promise<ReadonlyMap<string, StableGoogleSourceIdentity>> {
  const ids = unique(sourceIds);
  if (ids.length === 0) return new Map();
  const sources = await sql<StableGoogleSourceRow[]>`
    select id,kind,owner_adult_id,metadata from sources
    where household_id=${householdId} and id in ${sql(ids)}
  `;
  return new Map(
    sources.flatMap((source): [string, StableGoogleSourceIdentity][] => {
      const providerDigest = stableGoogleProviderDigest(source);
      if (!providerDigest) return [];
      return [
        [
          source.id,
          {
            providerDigest,
            calendarStateDigest:
              source.kind === "calendar" ? stableGoogleCalendarStateDigest(source, providerDigest) : null,
          },
        ],
      ];
    }),
  );
}

function stableGoogleProviderDigest(source: StableGoogleSourceRow): string | null {
  if (source.kind === "gmail") {
    if (!source.owner_adult_id) return null;
    const messageId = source.metadata.messageId;
    return typeof messageId === "string" && messageId
      ? sha256(`gmail\0${source.owner_adult_id}\0${messageId}`)
      : null;
  }
  if (source.kind === "calendar") {
    const calendarId = source.metadata.calendarId;
    const providerEventId = source.metadata.providerEventId;
    return typeof calendarId === "string" &&
      calendarId &&
      typeof providerEventId === "string" &&
      providerEventId
      ? sha256(`calendar\0${source.owner_adult_id ?? "household"}\0${calendarId}\0${providerEventId}`)
      : null;
  }
  return null;
}

function stableGoogleCalendarStateDigest(
  source: StableGoogleSourceRow,
  providerDigest: string,
): string | null {
  if (source.kind !== "calendar") return null;
  const calendarId = source.metadata.calendarId;
  const providerEventId = source.metadata.providerEventId;
  const status = source.metadata.status;
  const busy = source.metadata.busy;
  const title = source.metadata.title;
  const startsAt = source.metadata.startsAt;
  const endsAt = source.metadata.endsAt;
  const allDay = source.metadata.allDay;
  if (
    typeof calendarId !== "string" ||
    !calendarId ||
    typeof providerEventId !== "string" ||
    !providerEventId ||
    (status !== "confirmed" && status !== "tentative" && status !== "cancelled") ||
    typeof busy !== "boolean" ||
    (title !== null && typeof title !== "string") ||
    (startsAt !== null && typeof startsAt !== "string") ||
    (endsAt !== null && typeof endsAt !== "string") ||
    (allDay !== null && typeof allDay !== "boolean") ||
    (startsAt === null) !== (endsAt === null) ||
    (startsAt === null) !== (allDay === null)
  ) {
    return null;
  }
  let canonicalStartsAt: string | null = null;
  let canonicalEndsAt: string | null = null;
  try {
    canonicalStartsAt = startsAt === null ? null : instant(startsAt).toISOString();
    canonicalEndsAt = endsAt === null ? null : instant(endsAt).toISOString();
  } catch {
    return null;
  }
  if (
    (status !== "cancelled" && canonicalStartsAt === null) ||
    (status === "cancelled" && busy) ||
    (canonicalStartsAt !== null &&
      canonicalEndsAt !== null &&
      Date.parse(canonicalEndsAt) <= Date.parse(canonicalStartsAt))
  ) {
    return null;
  }
  return sha256(
    JSON.stringify({
      version: 1,
      providerDigest,
      calendarId,
      providerEventId,
      status,
      busy,
      title,
      startsAt: canonicalStartsAt,
      endsAt: canonicalEndsAt,
      allDay,
    }),
  );
}

function googleActionKey(
  sourceIds: readonly string[],
  sourceDigests: ReadonlyMap<string, StableGoogleSourceIdentity>,
  semantics: {
    category: SharedBriefingCandidate["category"] | "owner_private";
    dueAt: string | null;
    actionAnchorDigest: string | null;
  },
): string | null {
  const uniqueSourceIds = unique(sourceIds);
  const identities = uniqueSourceIds.flatMap((sourceId) => {
    const identity = sourceDigests.get(sourceId);
    return identity ? [identity] : [];
  });
  if (
    uniqueSourceIds.length > 0 &&
    identities.length === uniqueSourceIds.length &&
    identities.every((identity) => identity.calendarStateDigest !== null)
  ) {
    return sha256(
      JSON.stringify({
        version: 3,
        calendarStates: unique(identities.map((identity) => identity.calendarStateDigest as string)).sort(),
      }),
    );
  }
  const providers = unique(identities.map((identity) => identity.providerDigest)).sort();
  if (providers.length === 0) return null;
  return sha256(
    JSON.stringify({
      version: 2,
      providers,
      category: semantics.category,
      dueAt: semantics.dueAt === null ? null : instant(semantics.dueAt).toISOString(),
      actionAnchorDigest: semantics.actionAnchorDigest,
    }),
  );
}

function googleActionKeyForFinding(
  finding: PrivateReviewFinding,
  sourceDigests: ReadonlyMap<string, StableGoogleSourceIdentity>,
): string | null {
  const candidate = finding.householdCandidate;
  return googleActionKey(finding.sourceIds, sourceDigests, {
    category: candidate?.category ?? "owner_private",
    dueAt: finding.dueAt ?? candidate?.dueAt ?? null,
    actionAnchorDigest: finding.actionAnchorDigest ?? null,
  });
}

function googleActionKeyForCandidate(
  candidate: StoredBriefingCandidate,
  sourceDigests: ReadonlyMap<string, StableGoogleSourceIdentity>,
): string | null {
  return (
    candidate.actionKey ??
    googleActionKey(candidate.sourceIds, sourceDigests, {
      category: candidate.category,
      dueAt: candidate.dueAt,
      actionAnchorDigest: candidate.actionAnchorDigest ?? null,
    })
  );
}

function googleActionKeyForDelivery(
  delivery: ProactiveDelivery,
  sourceDigests: ReadonlyMap<string, StableGoogleSourceIdentity>,
): string | null {
  return googleActionKey(delivery.actionSourceIds ?? delivery.sourceIds, sourceDigests, {
    category: delivery.householdCategory ?? "owner_private",
    dueAt: delivery.dueAt ?? null,
    actionAnchorDigest: delivery.actionAnchorDigest ?? null,
  });
}

function googleActionWorkMarker(actionKey: string): string {
  assertDigest(actionKey, "Google action key");
  return `${GOOGLE_ACTION_WORK_MARKER_PREFIX}${actionKey}`;
}

function googleActionKeyFromWorkMarker(value: string | null): string | null {
  if (!value?.startsWith(GOOGLE_ACTION_WORK_MARKER_PREFIX)) return null;
  const actionKey = value.slice(
    GOOGLE_ACTION_WORK_MARKER_PREFIX.length,
    GOOGLE_ACTION_WORK_MARKER_PREFIX.length + 64,
  );
  assertDigest(actionKey, "Stored Google action key");
  return actionKey;
}

async function readResolvedGoogleActionKeys(
  sql: postgres.TransactionSql,
  householdId: string,
): Promise<ReadonlySet<string>> {
  const sources = await sql<{ metadata: JsonValue }[]>`
    select metadata from sources
    where household_id=${householdId}
      and (
        jsonb_typeof(metadata->'completedGoogleActionKeys')='array'
        or metadata->'googleActionTerminal'='true'::jsonb
      )
  `;
  const keys = sources.flatMap(({ metadata }) => {
    const record = jsonRecord(metadata);
    const values =
      record.completedGoogleActionKeys ??
      (record.googleActionTerminal === true ? record.googleActionKeys : undefined);
    if (!Array.isArray(values)) return [];
    return values.flatMap((value) => {
      if (typeof value !== "string") {
        throw new FlorenceStoreConflict("A completed Google action key is invalid");
      }
      assertDigest(value, "Completed Google action key");
      return [value];
    });
  });
  const monitors = await sql<{ last_error: string | null }[]>`
    select last_error from proactive_work
    where household_id=${householdId} and kind='finite_monitor' and status='completed'
      and left(coalesce(last_error,''),${GOOGLE_ACTION_WORK_MARKER_PREFIX.length})=
        ${GOOGLE_ACTION_WORK_MARKER_PREFIX}
  `;
  for (const monitor of monitors) {
    const actionKey = googleActionKeyFromWorkMarker(monitor.last_error);
    if (actionKey) keys.push(actionKey);
  }
  return new Set(keys);
}

function googleActionUrgencies(
  actions: readonly { key: string; urgency: ProactiveDelivery["urgency"] }[],
): JsonObject {
  const result: Record<string, JsonValue> = {};
  for (const action of actions) {
    assertDigest(action.key, "Google action key");
    if (!isProactiveUrgency(action.urgency)) {
      throw new FlorenceStoreConflict("A Google action urgency is invalid");
    }
    result[action.key] = action.urgency;
  }
  return result;
}

async function wasGoogleActionDelivered(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    visibility: Visibility;
    ownerAdultId: string | null;
    actionKey: string;
    urgency: ProactiveDelivery["urgency"];
    pendingText?: string;
  },
): Promise<boolean> {
  const [delivered] = await sql<{ exists: boolean }[]>`
    select exists (
      select 1 from messages message join sources source on source.id=message.source_id
      where source.household_id=${input.householdId} and source.visibility=${input.visibility}
        and source.owner_adult_id is not distinct from ${input.ownerAdultId}
        and message.direction='outbound' and message.status in ('pending','sending','sent')
        and (message.status in ('sending','sent')
          or (${input.pendingText ?? null}::text is null or message.text=${input.pendingText ?? null}))
        and jsonb_typeof(source.metadata->'googleActionKeys')='array'
        and jsonb_exists(source.metadata->'googleActionKeys',${input.actionKey})
        and (
          source.metadata->'googleActionUrgencies'->>${input.actionKey}=${input.urgency}
          or not jsonb_exists(
            case
              when jsonb_typeof(source.metadata->'googleActionUrgencies')='object'
              then source.metadata->'googleActionUrgencies'
              else '{}'::jsonb
            end,
            ${input.actionKey}
          )
        )
    ) as exists
  `;
  return delivered?.exists ?? false;
}

async function wasGoogleActionTerminallyDelivered(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    visibility: Visibility;
    ownerAdultId: string | null;
    actionKey: string;
  },
): Promise<boolean> {
  const [delivered] = await sql<{ exists: boolean }[]>`
    select exists (
      select 1 from messages message join sources source on source.id=message.source_id
      where source.household_id=${input.householdId} and source.visibility=${input.visibility}
        and source.owner_adult_id is not distinct from ${input.ownerAdultId}
        and message.direction='outbound' and message.status in ('pending','sending','sent')
        and source.metadata->'googleActionTerminal'='true'::jsonb
        and jsonb_typeof(source.metadata->'googleActionKeys')='array'
        and jsonb_exists(source.metadata->'googleActionKeys',${input.actionKey})
    ) as exists
  `;
  return delivered?.exists ?? false;
}

type GooglePollDesiredState = Readonly<{
  privateOutboundOutcomes: ReadonlySet<string>;
  householdOutboundOutcomes: ReadonlySet<string>;
  monitorActions: ReadonlyMap<string, readonly string[]>;
  calendarProposalSignatures: ReadonlySet<string>;
  deliverySourceIds: ReadonlySet<string>;
  householdDocketCandidates: readonly GooglePollDocketCandidate[];
  preservedHouseholdDocketActions: readonly GooglePollDocketPreservation[];
  completedMonitorActionKeys: ReadonlySet<string>;
}>;

type GooglePollDocketCandidate = Readonly<{
  actionKey: string;
  category: SharedBriefingCandidate["category"];
  summary: string;
  urgency: SharedBriefingCandidate["urgency"];
  dueAt: string | null;
  needsAnswer: boolean;
  sourceIds: readonly string[];
  actionAnchorDigest: string | null;
}>;

type GooglePollDocketPreservation = Readonly<{
  sourceIds: readonly string[];
  actionAnchorDigest: string;
}>;

function googleOutboundOutcomeIdentity(
  actionKey: string,
  text: string,
  urgency: ProactiveDelivery["urgency"],
): string {
  assertDigest(actionKey, "Google outbound action key");
  if (!isProactiveUrgency(urgency)) {
    throw new FlorenceStoreConflict("A Google outbound urgency is invalid");
  }
  return JSON.stringify([actionKey, required(text, "Google outbound message"), urgency]);
}

function googleActionUrgencyFromMetadata(
  metadata: JsonObject,
  actionKey: string,
): ProactiveDelivery["urgency"] | null {
  const urgencies = metadata.googleActionUrgencies;
  if (urgencies === undefined || !isRecord(urgencies)) return null;
  const urgency = urgencies[actionKey];
  return typeof urgency === "string" && isProactiveUrgency(urgency) ? urgency : null;
}

async function applyGooglePollDeliveries(
  sql: postgres.TransactionSql,
  input: {
    work: ProactiveWorkRow;
    deliveries: readonly ProactiveDelivery[];
    deliverNotBefore: Date;
    occurredAt: Date;
  },
): Promise<GooglePollDesiredState> {
  const work = input.work;
  const [privateChannel] =
    work.visibility === "private" && work.owner_adult_id
      ? await sql<ChannelRow[]>`
          select * from linq_channels where household_id=${work.household_id}
            and audience='private' and adult_one_id=${work.owner_adult_id}
            and adult_two_id is null and revoked_at is null and stopped_at is null
          order by bound_at,id limit 1 for share
        `
      : [];
  const [groupChannel] = await sql<ChannelRow[]>`
    select * from linq_channels where household_id=${work.household_id}
      and audience='group' and adult_two_id is not null
      and revoked_at is null and stopped_at is null
    order by bound_at,id limit 1 for share
  `;
  if (work.visibility === "private" && !privateChannel) {
    throw new FlorenceStoreConflict("The private Google poll thread is unavailable");
  }
  if (work.visibility === "household" && !groupChannel) {
    throw new FlorenceStoreConflict("The family Google poll thread is unavailable");
  }
  const [privateOwner] =
    work.visibility === "private" && work.owner_adult_id
      ? await sql<{ conflict_sharing_enabled: boolean }[]>`
          select coalesce(preferences->'privateConflictBusySharingEnabled'='true'::jsonb,false)
            as conflict_sharing_enabled
          from people where household_id=${work.household_id} and id=${work.owner_adult_id}
            and kind='adult' and status='verified' for share
        `
      : [];
  if (work.visibility === "private" && !privateOwner) {
    throw new FlorenceStoreUnauthorized("The private Google sync owner is no longer active");
  }

  const deliverySources: string[][] = [];
  const householdConclusions: (string | null)[] = [];
  for (const delivery of input.deliveries) {
    if (!isProactiveUrgency(delivery.urgency)) {
      throw new FlorenceStoreConflict("A proactive finding urgency is invalid");
    }
    const sourceIds = unique(delivery.sourceIds).sort();
    if (sourceIds.length === 0) {
      throw new FlorenceStoreConflict("A proactive finding requires evidence");
    }
    if (
      (delivery.householdConclusion === null) !== (delivery.householdCategory === null) ||
      (delivery.householdCategory !== null && !isSharedBriefingCategory(delivery.householdCategory))
    ) {
      throw new FlorenceStoreConflict("A proactive household conclusion needs one valid category");
    }
    if (delivery.monitor && delivery.familyCalendar) {
      throw new FlorenceStoreConflict(
        "One Google finding cannot create both a Calendar action and a reminder monitor",
      );
    }
    householdConclusions.push(
      work.visibility === "private" &&
        delivery.householdCategory === "conflict" &&
        !privateOwner?.conflict_sharing_enabled
        ? null
        : delivery.householdConclusion,
    );
    deliverySources.push(sourceIds);
  }
  const citedSourceIds = unique(deliverySources.flat()).sort();
  if (citedSourceIds.length > 0) {
    await assertProactiveSources(
      sql,
      work.household_id,
      work.visibility,
      work.owner_adult_id,
      citedSourceIds,
    );
  }
  await linkProactiveWorkSources(sql, work.id, citedSourceIds);
  const stableGoogleSourceDigests = await readStableGoogleSourceDigestMap(
    sql,
    work.household_id,
    citedSourceIds,
  );
  const deliveryActionKeys = input.deliveries.map((delivery) =>
    googleActionKeyForDelivery(delivery, stableGoogleSourceDigests),
  );
  if (deliveryActionKeys.some((actionKey) => actionKey === null)) {
    throw new FlorenceStoreConflict("A Google poll outcome requires an exact stable provider identity");
  }
  const resolvedGoogleActionKeys = await readResolvedGoogleActionKeys(sql, work.household_id);
  const activeDeliveryIndexes = input.deliveries.flatMap((delivery, index) => {
    const actionKey = deliveryActionKeys[index];
    return delivery.monitor?.operation === "complete" ||
      !actionKey ||
      !resolvedGoogleActionKeys.has(actionKey)
      ? [index]
      : [];
  });
  const completedMonitorActionKeysByIndex = new Map<number, string>();
  for (const index of activeDeliveryIndexes) {
    const monitor = input.deliveries[index]?.monitor;
    if (monitor?.operation !== "complete") continue;
    const [completedMonitor] = await sql<{ last_error: string | null }[]>`
      select last_error from proactive_work
      where id=${monitor.monitorId} and household_id=${work.household_id}
        and kind='finite_monitor' and status='active' and visibility=${work.visibility}
        and owner_adult_id is not distinct from ${work.owner_adult_id}
      for share
    `;
    const completedActionKey = googleActionKeyFromWorkMarker(completedMonitor?.last_error ?? null);
    if (completedActionKey) completedMonitorActionKeysByIndex.set(index, completedActionKey);
  }
  const privateOutboundOutcomes = new Set<string>();
  const householdOutboundOutcomes = new Set<string>();
  const monitorActions = new Map<string, readonly string[]>();
  const calendarProposalSignatures = new Set<string>();
  const completedMonitorActionKeys = new Set(completedMonitorActionKeysByIndex.values());
  const householdDocketCandidates = activeDeliveryIndexes.flatMap((index) => {
    const delivery = input.deliveries[index];
    if (!delivery) return [];
    const summary = householdConclusions[index];
    const actionKey = deliveryActionKeys[index];
    if (!summary || !delivery.householdCategory || !actionKey || delivery.monitor?.operation === "complete") {
      return [];
    }
    return [
      {
        actionKey,
        category: delivery.householdCategory,
        summary,
        urgency: delivery.urgency,
        dueAt: delivery.dueAt ?? null,
        needsAnswer: delivery.householdNeedsAnswer ?? false,
        sourceIds: deliverySources[index] ?? [],
        actionAnchorDigest: delivery.actionAnchorDigest ?? null,
      },
    ];
  });
  const preservedHouseholdDocketActions = activeDeliveryIndexes.flatMap((index) => {
    const delivery = input.deliveries[index];
    if (!delivery) return [];
    const actionAnchorDigest = delivery.actionAnchorDigest ?? null;
    if (!delivery.preserveDocket || !actionAnchorDigest) return [];
    return [
      {
        sourceIds: deliverySources[index] ?? [],
        actionAnchorDigest,
      },
    ];
  });

  for (const index of activeDeliveryIndexes) {
    const delivery = input.deliveries[index];
    if (!delivery) continue;
    const sourceIds = deliverySources[index] ?? [];
    const householdConclusion = householdConclusions[index] ?? null;
    const deliveryKey = sha256(
      JSON.stringify({
        sourceIds,
        privateDetail: delivery.privateDetail,
        household: householdConclusion,
        urgency: delivery.urgency,
      }),
    );
    const findingNotBefore = new Date(input.deliverNotBefore.getTime() + index * 250);
    const googleActionKey = deliveryActionKeys[index];
    if (!googleActionKey) {
      throw new FlorenceStoreConflict("A Google poll outcome lost its provider identity");
    }
    const completedMonitorActionKey = completedMonitorActionKeysByIndex.get(index) ?? null;
    const outboundGoogleActionKeys = unique([
      googleActionKey,
      ...(completedMonitorActionKey ? [completedMonitorActionKey] : []),
    ]).sort();
    const outboundGoogleActionUrgencies = googleActionUrgencies(
      outboundGoogleActionKeys.map((key) => ({ key, urgency: delivery.urgency })),
    );
    const surfaceNow = delivery.surfaceNow !== false;
    const privateAlreadyDelivered =
      surfaceNow && work.visibility === "private" && work.owner_adult_id && delivery.privateDetail
        ? completedMonitorActionKey
          ? await wasGoogleActionTerminallyDelivered(sql, {
              householdId: work.household_id,
              visibility: "private",
              ownerAdultId: work.owner_adult_id,
              actionKey: completedMonitorActionKey,
            })
          : await wasGoogleActionDelivered(sql, {
              householdId: work.household_id,
              visibility: "private",
              ownerAdultId: work.owner_adult_id,
              actionKey: googleActionKey,
              urgency: delivery.urgency,
              pendingText: delivery.privateDetail,
            })
        : false;
    if (
      work.visibility === "private" &&
      surfaceNow &&
      delivery.privateDetail &&
      privateChannel &&
      !privateAlreadyDelivered
    ) {
      await insertProactiveOutbound(sql, {
        workId: work.id,
        suffix: `private:${deliveryKey}`,
        householdId: work.household_id,
        channel: privateChannel,
        visibility: "private",
        ownerAdultId: work.owner_adult_id,
        text: delivery.privateDetail,
        metadata: {
          googleSourceIds: sourceIds,
          googleActionKeys: outboundGoogleActionKeys,
          googleActionUrgencies: outboundGoogleActionUrgencies,
          ...(completedMonitorActionKey ? { googleActionTerminal: true } : {}),
        },
        notBefore: findingNotBefore,
        occurredAt: input.occurredAt,
      });
    }
    if (work.visibility === "private" && surfaceNow && delivery.privateDetail && privateChannel) {
      privateOutboundOutcomes.add(
        googleOutboundOutcomeIdentity(googleActionKey, delivery.privateDetail, delivery.urgency),
      );
    }
    const householdAlreadyDelivered =
      surfaceNow && householdConclusion
        ? completedMonitorActionKey
          ? await wasGoogleActionTerminallyDelivered(sql, {
              householdId: work.household_id,
              visibility: "household",
              ownerAdultId: null,
              actionKey: completedMonitorActionKey,
            })
          : await wasGoogleActionDelivered(sql, {
              householdId: work.household_id,
              visibility: "household",
              ownerAdultId: null,
              actionKey: googleActionKey,
              urgency: delivery.urgency,
              pendingText: householdConclusion,
            })
        : false;
    if (surfaceNow && householdConclusion && groupChannel && !householdAlreadyDelivered) {
      await insertProactiveOutbound(sql, {
        workId: work.id,
        suffix: `household:${deliveryKey}`,
        householdId: work.household_id,
        channel: groupChannel,
        visibility: "household",
        ownerAdultId: null,
        text: householdConclusion,
        ...(sourceIds.length > 0 ||
        googleActionKey ||
        (work.visibility === "private" && delivery.householdCategory === "conflict" && work.owner_adult_id)
          ? {
              metadata: {
                googleSourceIds: sourceIds,
                googleActionKeys: outboundGoogleActionKeys,
                googleActionUrgencies: outboundGoogleActionUrgencies,
                ...(completedMonitorActionKey ? { googleActionTerminal: true } : {}),
                ...(work.visibility === "private" &&
                delivery.householdCategory === "conflict" &&
                work.owner_adult_id
                  ? { privateConflictOwnerAdultIds: [work.owner_adult_id] }
                  : {}),
              },
            }
          : {}),
        notBefore: findingNotBefore,
        occurredAt: input.occurredAt,
      });
    }
    if (surfaceNow && householdConclusion && groupChannel) {
      householdOutboundOutcomes.add(
        googleOutboundOutcomeIdentity(googleActionKey, householdConclusion, delivery.urgency),
      );
    }
    if (delivery.monitor) {
      if (work.visibility === "household" && !householdConclusion) {
        throw new FlorenceStoreConflict("A household monitor change requires a household-safe conclusion");
      }
      const monitorChange =
        work.visibility === "household" && householdConclusion
          ? householdSafeMonitorChange(delivery.monitor, householdConclusion)
          : delivery.monitor;
      const terminalActionKey = completedMonitorActionKey ?? googleActionKey;
      const terminalAlreadyDelivered = terminalActionKey
        ? await wasGoogleActionTerminallyDelivered(sql, {
            householdId: work.household_id,
            visibility: work.visibility,
            ownerAdultId: work.owner_adult_id,
            actionKey: terminalActionKey,
          })
        : false;
      if (monitorChange.operation === "complete" || !terminalAlreadyDelivered) {
        await applyProactiveMonitorChange(sql, {
          householdId: work.household_id,
          visibility: work.visibility,
          ownerAdultId: work.owner_adult_id,
          sourceIds,
          change: monitorChange,
          basisWorkId: work.id,
          occurredAt: input.occurredAt,
          googleActionKey: googleActionKey,
        });
        if (monitorChange.operation !== "complete") {
          monitorActions.set(googleActionKey, sourceIds);
        }
      }
    }
    if (delivery.familyCalendar) {
      if (work.visibility !== "private" || !work.owner_adult_id) {
        throw new FlorenceStoreUnauthorized(
          "Automatic family Calendar proposals require one adult's private official source",
        );
      }
      const staged = await stageFamilyCalendarReviewProposal(sql, {
        householdId: work.household_id,
        ownerAdultId: work.owner_adult_id,
        proposal: delivery.familyCalendar,
        googleActionKey,
        occurredAt: input.occurredAt,
      });
      if (staged) {
        const proposalSourceIds = unique(delivery.familyCalendar.sourceIds);
        const proposalSourceId = proposalSourceIds[0];
        const providerDigest = proposalSourceId
          ? stableGoogleSourceDigests.get(proposalSourceId)?.providerDigest
          : null;
        if (proposalSourceIds.length !== 1 || !providerDigest) {
          throw new FlorenceStoreConflict("A Google Calendar proposal requires one stable provider source");
        }
        calendarProposalSignatures.add(
          calendarProposalBasisSignature(providerDigest, delivery.familyCalendar.event),
        );
      }
    }
  }
  return {
    privateOutboundOutcomes,
    householdOutboundOutcomes,
    monitorActions,
    calendarProposalSignatures,
    deliverySourceIds: new Set(activeDeliveryIndexes.flatMap((index) => deliverySources[index] ?? [])),
    householdDocketCandidates,
    preservedHouseholdDocketActions,
    completedMonitorActionKeys,
  };
}

async function reconcileHouseholdDocketCandidates(
  sql: postgres.TransactionSql,
  input: {
    work: ProactiveWorkRow;
    reviewedSourceIds: readonly string[];
    candidates: readonly GooglePollDocketCandidate[];
    preservedActions: readonly GooglePollDocketPreservation[];
    completedActionKeys: ReadonlySet<string>;
  },
): Promise<void> {
  if (input.work.kind !== "personal_google_poll" || !input.work.owner_adult_id) return;
  const [review] = await sql<ProactiveWorkRow[]>`
    select * from proactive_work where household_id=${input.work.household_id}
      and owner_adult_id=${input.work.owner_adult_id}
      and kind='initial_private_review' and status='completed'
    order by created_at desc,id desc limit 1 for update
  `;
  if (!review) {
    throw new FlorenceStoreConflict("A personal Google poll lost its completed private review");
  }
  const resolvedActionKeys = new Set([
    ...(await readResolvedGoogleActionKeys(sql, input.work.household_id)),
    ...input.completedActionKeys,
  ]);
  const storedCandidates = storedBriefingCandidates(review.briefing_candidates);
  const candidateSourceDigests = await readStableGoogleSourceDigestMap(
    sql,
    input.work.household_id,
    unique(storedCandidates.flatMap((candidate) => [...candidate.sourceIds])),
  );
  const reviewedSourceIds = new Set(input.reviewedSourceIds);
  const retained = storedCandidates.filter((candidate) => {
    const actionKey = googleActionKeyForCandidate(candidate, candidateSourceDigests);
    if (actionKey && resolvedActionKeys.has(actionKey)) return false;
    const touchesReviewedSource = candidate.sourceIds.some((sourceId) => reviewedSourceIds.has(sourceId));
    if (!touchesReviewedSource) return true;
    return input.preservedActions.some(
      (action) =>
        action.actionAnchorDigest === candidate.actionAnchorDigest &&
        action.sourceIds.some((sourceId) => candidate.sourceIds.includes(sourceId)),
    );
  });
  const current = input.candidates.flatMap((candidate) =>
    resolvedActionKeys.has(candidate.actionKey)
      ? []
      : [
          privateReviewCandidate(
            deterministicUuid(`briefing-candidate\0${review.id}\0${candidate.actionKey}`),
            {
              category: candidate.category,
              summary: candidate.summary,
              urgency: candidate.urgency,
              dueAt: candidate.dueAt,
              needsAnswer: candidate.needsAnswer,
            },
            candidate.sourceIds,
            candidate.actionAnchorDigest,
            candidate.actionKey,
          ),
        ],
  );
  await sql`
    update proactive_work set briefing_candidates=${sql.json(
      mergedBriefingCandidates([...retained, ...current]),
    )}
    where id=${review.id} and kind='initial_private_review' and status='completed'
  `;
}

async function removeHouseholdDocketCandidateForGoogleAction(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    ownerAdultId: string | null;
    actionKey: string;
  },
): Promise<void> {
  if (!input.ownerAdultId) return;
  const [review] = await sql<ProactiveWorkRow[]>`
    select * from proactive_work where household_id=${input.householdId}
      and owner_adult_id=${input.ownerAdultId}
      and kind='initial_private_review' and status='completed'
    order by created_at desc,id desc limit 1 for update
  `;
  if (!review) return;
  const candidates = storedBriefingCandidates(review.briefing_candidates);
  const sourceDigests = await readStableGoogleSourceDigestMap(
    sql,
    input.householdId,
    unique(candidates.flatMap((candidate) => [...candidate.sourceIds])),
  );
  const retained = candidates.filter(
    (candidate) => googleActionKeyForCandidate(candidate, sourceDigests) !== input.actionKey,
  );
  if (retained.length === candidates.length) return;
  await sql`
    update proactive_work set briefing_candidates=${sql.json(retained)}
    where id=${review.id} and kind='initial_private_review' and status='completed'
  `;
}

async function stageFamilyCalendarReviewProposal(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    ownerAdultId: string;
    proposal: FamilyCalendarReviewProposal;
    googleActionKey: string;
    occurredAt: Date;
  },
): Promise<boolean> {
  assertDigest(input.googleActionKey, "Calendar proposal Google action key");
  const sourceIds = unique(input.proposal.sourceIds).sort();
  if (sourceIds.length !== 1) {
    throw new FlorenceStoreConflict("A family Calendar review proposal requires exactly one official source");
  }
  if (input.proposal.disposition !== "automatic" && input.proposal.disposition !== "suggest") {
    throw new FlorenceStoreConflict("A family Calendar review proposal has an invalid disposition");
  }
  validateCalendarEvent(input.proposal.event);
  const authority = await readFamilyCalendarAuthority(sql, input.householdId);
  if (!authority) {
    throw new FlorenceStoreConflict("The Family Calendar household no longer exists");
  }
  const [groupChannel] = await sql<ChannelRow[]>`
    select * from linq_channels where household_id=${input.householdId}
      and audience='group' and adult_two_id is not null and revoked_at is null and stopped_at is null
    order by bound_at,id limit 1 for share
  `;
  const calendarLifecycleReady = Boolean(
    authority.family_calendar_id &&
      authority.family_calendar_id !== "primary" &&
      authority.family_calendar_owner_connection_id &&
      authority.family_calendar_partner_connection_id &&
      authority.family_calendar_created_at &&
      groupChannel,
  );
  if (!calendarLifecycleReady) return false;
  if (
    !groupChannel ||
    !authority.founder_adult_id ||
    !isExactFamilyCalendarAuthority(authority, groupChannel, authority.founder_adult_id)
  ) {
    throw new FlorenceStoreConflict("The exact family group is not ready for this proposal");
  }
  for (const sourceId of sourceIds) {
    if (!(await isOfficialPrivateCalendarBasis(sql, input.householdId, sourceId, authority))) {
      throw new FlorenceStoreUnauthorized(
        "A family Calendar review proposal cited something other than private official Google evidence",
      );
    }
  }
  const ownedSources = await sql<SourceRow[]>`
    select id,kind,visibility,owner_adult_id,label,metadata,occurred_at
    from sources where household_id=${input.householdId} and id in ${sql(sourceIds)}
      and visibility='private' and owner_adult_id=${input.ownerAdultId} for share
  `;
  if (ownedSources.length !== sourceIds.length) {
    throw new FlorenceStoreUnauthorized("A Calendar review can use only this adult's private evidence");
  }
  const personalCalendarSources = ownedSources.filter((source) => source.kind === "calendar");
  if (personalCalendarSources.length > 0 && sourceIds.length !== 1) {
    throw new FlorenceStoreUnauthorized(
      "A personal Calendar suggestion requires exactly one owner-private Calendar source",
    );
  }
  const personalCalendarSource = personalCalendarSources[0] ?? null;
  const [household] = personalCalendarSource
    ? await sql<{ time_zone: string }[]>`
        select time_zone from households where id=${input.householdId} for share
      `
    : [];
  const event = personalCalendarSource
    ? exactPersonalCalendarProposalEvent(
        personalCalendarSource,
        input.proposal.event,
        required(household?.time_zone ?? "", "Household time zone"),
      )
    : input.proposal.event;
  const mutation: Extract<FamilyCalendarMutation, { operation: "create" }> = {
    operation: "create",
    event,
    target: null,
  };
  const gmailMessageIds = unique(
    ownedSources.flatMap((source) => {
      const messageId = source.kind === "gmail" ? jsonString(source.metadata, "messageId") : null;
      return messageId ? [messageId] : [];
    }),
  );
  const calendarEventIds = unique(
    ownedSources.flatMap((source) => {
      const eventId = source.kind === "calendar" ? jsonString(source.metadata, "providerEventId") : null;
      return eventId ? [eventId] : [];
    }),
  );
  if (gmailMessageIds.length > 0 || calendarEventIds.length > 0) {
    const handled = await sql<{ id: string; google_action_key: string | null; payload: JsonValue }[]>`
      select action.id,action.google_action_key,action.payload from calendar_actions action
      join sources basis on basis.id=action.basis_source_id
      where action.household_id=${input.householdId}
        and basis.visibility='private' and basis.owner_adult_id=${input.ownerAdultId}
        and (
          ${gmailMessageIds.length > 0 ? sql`(basis.kind='gmail' and basis.metadata->>'messageId' in ${sql(gmailMessageIds)})` : sql`false`}
          or
          ${calendarEventIds.length > 0 ? sql`(basis.kind='calendar' and basis.metadata->>'providerEventId' in ${sql(calendarEventIds)})` : sql`false`}
        )
        and action.status in ('offered','pending','committed','failed')
      order by action.created_at,action.id for share of action,basis
    `;
    const existingAction = handled.find((action) => {
      const prior = familyCalendarMutation(action.payload);
      return prior.operation === "create" && sameCalendarEventSchedule(prior.event, event);
    });
    if (existingAction) {
      if (existingAction.google_action_key === null) {
        await sql`
          update calendar_actions set google_action_key=${input.googleActionKey}
          where id=${existingAction.id} and household_id=${input.householdId}
            and google_action_key is null
        `;
      }
      return true;
    }
  }
  const id = deterministicUuid(
    `calendar-review\0${input.householdId}\0${input.ownerAdultId}\0${sourceIds.join("\0")}\0${JSON.stringify(mutation)}`,
  );
  const basisSourceId = sourceIds[0] as string;
  const automatic =
    personalCalendarSource === null &&
    input.proposal.disposition === "automatic" &&
    familyCalendarAutomaticCreationEnabled(authority);
  if (automatic) {
    await sql`
      insert into calendar_actions (
        id,household_id,basis_source_id,google_action_key,payload,status,retry_at,created_at
      ) values (${id},${input.householdId},${basisSourceId},${input.googleActionKey},
        ${sql.json(mutation)},'pending',
        ${input.occurredAt},${input.occurredAt})
      on conflict (id) do nothing
    `;
    return true;
  }

  const [ownerPrivateChannel] = personalCalendarSource
    ? await sql<ChannelRow[]>`
        select * from linq_channels where household_id=${input.householdId}
          and audience='private' and adult_one_id=${input.ownerAdultId} and adult_two_id is null
          and revoked_at is null and stopped_at is null
        order by bound_at,id limit 1 for share
      `
    : [];
  if (
    personalCalendarSource &&
    (!ownerPrivateChannel ||
      !isExactPrivateAdultCalendarAuthority(authority, ownerPrivateChannel, input.ownerAdultId))
  ) {
    throw new FlorenceStoreUnauthorized(
      "A personal Calendar suggestion requires its owner's exact private Messages thread",
    );
  }
  const promptChannel = personalCalendarSource ? (ownerPrivateChannel as ChannelRow) : groupChannel;
  const promptSourceId = deterministicUuid(`calendar-review-prompt\0${id}`);
  await insertOutbound(sql, {
    sourceId: promptSourceId,
    idempotencyKey: `calendar-review-prompt:${id}`,
    moveKind: "message",
    text: sanitizedFamilyCalendarSuggestion(event),
    turnId: deterministicUuid(`calendar-review-prompt-turn\0${id}`),
    turnPart: 0,
    notBefore: input.occurredAt.toISOString(),
    householdId: input.householdId,
    channelId: promptChannel.id,
    visibility: personalCalendarSource ? "private" : "household",
    ownerAdultId: personalCalendarSource ? input.ownerAdultId : null,
    metadata: {
      googleActionKeys: [input.googleActionKey],
      googleSourceIds: sourceIds,
    },
    occurredAt: input.occurredAt,
  });
  await sql`
    with removed as (
      delete from calendar_actions action using messages prompt
      where action.household_id=${input.householdId} and action.status='offered'
        and prompt.source_id=action.approval_prompt_source_id
        and prompt.channel_id=${promptChannel.id}
      returning action.approval_prompt_source_id
    )
    update messages message set status='failed',sending_at=null,retry_at=null,
      last_error='A newer Calendar offer replaced this one'
    from removed where message.source_id=removed.approval_prompt_source_id
      and message.direction='outbound' and message.status='pending'
  `;
  await sql`
    insert into calendar_actions (
      id,household_id,basis_source_id,approval_prompt_source_id,google_action_key,
      payload,status,retry_at,created_at
    ) values (${id},${input.householdId},${basisSourceId},${promptSourceId},${input.googleActionKey},
      ${sql.json(mutation)},'offered',${input.occurredAt},${input.occurredAt})
    on conflict (id) do nothing
  `;
  return true;
}

function exactPersonalCalendarProposalEvent(
  sourceRow: SourceRow,
  proposal: CalendarEventDraft,
  householdTimeZone: string,
): CalendarEventDraft {
  const source = calendarEvidenceRecord(sourceRow);
  if (
    source.visibility !== "private" ||
    source.ownerAdultId === null ||
    source.status !== "confirmed" ||
    !source.title ||
    !source.startsAt ||
    !source.endsAt ||
    source.allDay === null ||
    source.title !== proposal.title
  ) {
    throw new FlorenceStoreUnauthorized(
      "A personal Calendar suggestion must copy one exact confirmed owner-private event",
    );
  }
  if (source.allDay) {
    const startDate = calendarDateInTimeZone(source.startsAt, householdTimeZone);
    const endDate = calendarDateInTimeZone(source.endsAt, householdTimeZone);
    if (
      proposal.intervalKind !== "all_day" ||
      proposal.startDate !== startDate ||
      proposal.endDate !== endDate
    ) {
      throw new FlorenceStoreUnauthorized(
        "A personal Calendar suggestion changed the source event's all-day interval",
      );
    }
    return { intervalKind: "all_day", title: source.title, startDate, endDate, location: null };
  }
  if (
    proposal.intervalKind !== "timed" ||
    Date.parse(proposal.startsAt) !== Date.parse(source.startsAt) ||
    Date.parse(proposal.endsAt) !== Date.parse(source.endsAt)
  ) {
    throw new FlorenceStoreUnauthorized(
      "A personal Calendar suggestion changed the source event's timed interval",
    );
  }
  return {
    intervalKind: "timed",
    title: source.title,
    startsAt: source.startsAt,
    endsAt: source.endsAt,
    timeZone: householdTimeZone,
    location: null,
  };
}

function sameCalendarEventSchedule(left: CalendarEventDraft, right: CalendarEventDraft): boolean {
  if (
    left.intervalKind !== right.intervalKind ||
    left.title !== right.title ||
    left.location !== right.location
  ) {
    return false;
  }
  if (left.intervalKind === "all_day" && right.intervalKind === "all_day") {
    return left.startDate === right.startDate && left.endDate === right.endDate;
  }
  if (left.intervalKind === "timed" && right.intervalKind === "timed") {
    return (
      Date.parse(left.startsAt) === Date.parse(right.startsAt) &&
      Date.parse(left.endsAt) === Date.parse(right.endsAt) &&
      left.timeZone === right.timeZone
    );
  }
  return false;
}

function calendarDateInTimeZone(value: string, timeZone: string): string {
  const parts = new Intl.DateTimeFormat("en-US", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    timeZone,
  }).formatToParts(new Date(value));
  const year = parts.find((part) => part.type === "year")?.value;
  const month = parts.find((part) => part.type === "month")?.value;
  const day = parts.find((part) => part.type === "day")?.value;
  return calendarDate(
    `${required(year ?? "", "Calendar year")}-${required(month ?? "", "Calendar month")}-${required(day ?? "", "Calendar day")}`,
  );
}

function sanitizedFamilyCalendarSuggestion(event: CalendarEventDraft): string {
  const location = event.location ? ` at ${event.location}` : "";
  if (event.intervalKind === "all_day") {
    return `I found a family date: “${event.title}” ${formatAllDayInterval(event.startDate, event.endDate)}${location}. Want me to add it to the family calendar?`;
  }
  const format = new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: event.timeZone,
  });
  return `I found a family date: “${event.title}” on ${format.format(new Date(event.startsAt))}${location}. Want me to add it to the family calendar?`;
}

async function stableGoogleMonitorMatch(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    visibility: Visibility;
    ownerAdultId: string | null;
    sourceIds: readonly string[];
    why: string;
    nextCheck: Date;
    actionKey: string | null;
  },
): Promise<{ id: string; status: "active" | "paused"; lastError: string | null } | null> {
  const currentDigests = unique(
    [...(await readStableGoogleSourceDigestMap(sql, input.householdId, input.sourceIds)).values()].map(
      (identity) => identity.providerDigest,
    ),
  ).sort();
  if (currentDigests.length === 0) return null;
  const candidates = await sql<
    {
      id: string;
      status: "active" | "paused";
      why: string;
      next_check_at: Date | null;
      last_error: string | null;
    }[]
  >`
    select id,status,why,next_check_at,last_error from proactive_work
    where household_id=${input.householdId} and kind='finite_monitor'
      and status in ('active','paused')
      and visibility=${input.visibility} and owner_adult_id is not distinct from ${input.ownerAdultId}
    order by created_at,id
  `;
  if (candidates.length === 0) return null;
  const candidateIds = candidates.map(({ id }) => id);
  const links = await sql<(StableGoogleSourceRow & { work_id: string })[]>`
    select link.work_id,source.id,source.kind,source.owner_adult_id,source.metadata
    from proactive_work_sources link join sources source on source.id=link.source_id
    where link.work_id in ${sql(candidateIds)}
  `;
  const digestsByWork = new Map<string, string[]>();
  for (const link of links) {
    const digest = stableGoogleProviderDigest(link);
    if (!digest) continue;
    const digests = digestsByWork.get(link.work_id) ?? [];
    digests.push(digest);
    digestsByWork.set(link.work_id, digests);
  }
  const providerMatches = candidates.filter((candidate) =>
    sameStringSet(unique(digestsByWork.get(candidate.id) ?? []).sort(), currentDigests),
  );
  if (providerMatches.length === 0) return null;
  if (input.actionKey) {
    const exact = providerMatches.find(
      (candidate) => googleActionKeyFromWorkMarker(candidate.last_error) === input.actionKey,
    );
    if (exact) {
      const [locked] = await sql<{ id: string; status: "active" | "paused"; last_error: string | null }[]>`
        select id,status,last_error from proactive_work where id=${exact.id}
          and kind='finite_monitor' and status in ('active','paused')
          and household_id=${input.householdId} and visibility=${input.visibility}
          and owner_adult_id is not distinct from ${input.ownerAdultId}
        for update
      `;
      return locked ? { id: locked.id, status: locked.status, lastError: locked.last_error } : null;
    }
    const untagged = providerMatches.filter(
      (candidate) =>
        candidate.status === "active" && googleActionKeyFromWorkMarker(candidate.last_error) === null,
    );
    const legacy = untagged.length === 1 ? untagged[0] : undefined;
    if (legacy) {
      const [locked] = await sql<{ id: string; status: "active" | "paused"; last_error: string | null }[]>`
        select id,status,last_error from proactive_work where id=${legacy.id}
          and kind='finite_monitor' and status='active'
          and household_id=${input.householdId} and visibility=${input.visibility}
          and owner_adult_id is not distinct from ${input.ownerAdultId}
        for update
      `;
      return locked ? { id: locked.id, status: locked.status, lastError: locked.last_error } : null;
    }
    return null;
  }
  const schedule = stableMonitorSchedule(input.why, input.nextCheck);
  const scheduleMatches = providerMatches.filter(
    (candidate) =>
      candidate.status === "active" &&
      candidate.next_check_at !== null &&
      stableMonitorSchedule(candidate.why, candidate.next_check_at) === schedule,
  );
  const selected = scheduleMatches[0] ?? null;
  if (!selected) return null;
  const [locked] = await sql<{ id: string; status: "active" | "paused"; last_error: string | null }[]>`
    select id,status,last_error from proactive_work where id=${selected.id}
      and kind='finite_monitor' and status='active'
      and household_id=${input.householdId} and visibility=${input.visibility}
      and owner_adult_id is not distinct from ${input.ownerAdultId}
    for update
  `;
  return locked ? { id: locked.id, status: locked.status, lastError: locked.last_error } : null;
}

function stableMonitorSchedule(_why: string, nextCheck: Date): string {
  return `monitor:${nextCheck.toISOString()}`;
}

function formatAllDayInterval(startDate: string, exclusiveEndDate: string): string {
  const format = new Intl.DateTimeFormat("en-US", { dateStyle: "medium", timeZone: "UTC" });
  const start = new Date(`${calendarDate(startDate)}T00:00:00.000Z`);
  const inclusiveEnd = new Date(
    new Date(`${calendarDate(exclusiveEndDate)}T00:00:00.000Z`).getTime() - 24 * 60 * 60_000,
  );
  return start.getTime() === inclusiveEnd.getTime()
    ? `on ${format.format(start)} (all day)`
    : `from ${format.format(start)} through ${format.format(inclusiveEnd)} (all day)`;
}

async function applyProactiveMonitorChange(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    visibility: Visibility;
    ownerAdultId: string | null;
    sourceIds: readonly string[];
    change: ProactiveMonitorChange;
    basisWorkId: string;
    occurredAt: Date;
    googleActionKey?: string | null;
  },
): Promise<void> {
  const change = input.change;
  const nextCheck = change.nextCheck ? instant(change.nextCheck) : null;
  if (change.operation === "complete" ? nextCheck !== null : !nextCheck || nextCheck <= input.occurredAt) {
    throw new FlorenceStoreConflict("A monitor change returned an invalid next check");
  }
  const objective = bounded(required(change.objective, "Monitor objective"), 2_000);
  const conclusion = bounded(required(change.currentConclusion, "Monitor conclusion"), 4_000);
  const endCondition = bounded(required(change.endCondition, "Monitor end condition"), 2_000);
  const why = bounded(required(change.why, "Monitor reason"), 2_000);
  let monitorId: string;
  if (change.operation === "create") {
    const providerMatch = await stableGoogleMonitorMatch(sql, {
      householdId: input.householdId,
      visibility: input.visibility,
      ownerAdultId: input.ownerAdultId,
      sourceIds: input.sourceIds,
      why,
      nextCheck: nextCheck as Date,
      actionKey: input.googleActionKey ?? null,
    });
    const [wordingMatch] =
      providerMatch || input.googleActionKey
        ? []
        : await sql<{ id: string }[]>`
          select id from proactive_work where household_id=${input.householdId}
            and kind='finite_monitor' and status='active' and visibility=${input.visibility}
            and owner_adult_id is not distinct from ${input.ownerAdultId}
            and objective=${objective} and end_condition=${endCondition}
          order by created_at,id limit 1 for update
        `;
    const semanticMatch = providerMatch ?? wordingMatch ?? null;
    if (semanticMatch) {
      monitorId = semanticMatch.id;
      const preservePaused = "status" in semanticMatch && semanticMatch.status === "paused";
      const preservedLastError =
        "lastError" in semanticMatch && typeof semanticMatch.lastError === "string"
          ? semanticMatch.lastError
          : null;
      await sql`
        update proactive_work set objective=${objective},why=${why},
          current_conclusion=${conclusion},end_condition=${endCondition},
          status=${preservePaused ? "paused" : "active"},
          next_check_at=${preservePaused ? null : nextCheck},
          last_error=${
            preservePaused
              ? preservedLastError
              : input.googleActionKey
                ? googleActionWorkMarker(input.googleActionKey)
                : null
          }
        where id=${monitorId}
      `;
    } else {
      monitorId = deterministicUuid(
        `proactive-monitor\0${input.basisWorkId}\0${sha256(
          JSON.stringify({
            objective,
            sourceIds: unique(input.sourceIds),
            googleActionKey: input.googleActionKey ?? null,
          }),
        )}`,
      );
      await sql`
        insert into proactive_work (
          id,household_id,kind,visibility,owner_adult_id,objective,why,
          current_conclusion,end_condition,status,next_check_at,last_error,created_at
        ) values (${monitorId},${input.householdId},'finite_monitor',${input.visibility},
          ${input.ownerAdultId},${objective},${why},${conclusion},${endCondition},
          'active',${nextCheck},
          ${input.googleActionKey ? googleActionWorkMarker(input.googleActionKey) : null},
          ${input.occurredAt})
        on conflict do nothing
      `;
    }
  } else {
    assertUuid(change.monitorId, "Finite monitor ID");
    monitorId = change.monitorId;
    const updated =
      change.operation === "complete"
        ? await sql`
            update proactive_work set objective=${objective},why=${why},
              current_conclusion=${conclusion},end_condition=${endCondition},status='completed',
              next_check_at=null
            where id=${monitorId} and household_id=${input.householdId} and kind='finite_monitor'
              and status='active' and visibility=${input.visibility}
              and owner_adult_id is not distinct from ${input.ownerAdultId}
            returning id
          `
        : await sql`
            update proactive_work set objective=${objective},why=${why},current_conclusion=${conclusion},
              end_condition=${endCondition},status='active',next_check_at=${nextCheck},
              last_error=${input.googleActionKey ? googleActionWorkMarker(input.googleActionKey) : null}
            where id=${monitorId} and household_id=${input.householdId} and kind='finite_monitor'
              and status='active' and visibility=${input.visibility}
              and owner_adult_id is not distinct from ${input.ownerAdultId}
            returning id
          `;
    if (updated.length !== 1) {
      throw new FlorenceStoreConflict("A changed monitor is no longer active in this audience");
    }
  }
  if (change.operation !== "complete") {
    for (const sourceId of unique(input.sourceIds)) {
      await sql`
        insert into proactive_work_sources (work_id,source_id) values (${monitorId},${sourceId})
        on conflict do nothing
      `;
    }
  }
}

function householdSafeMonitorChange(
  change: ProactiveMonitorChange,
  householdConclusion: string,
): ProactiveMonitorChange {
  const currentConclusion = bounded(
    required(householdConclusion, "Household-safe monitor conclusion"),
    4_000,
  );
  return {
    ...change,
    currentConclusion,
    why: HOUSEHOLD_SAFE_MONITOR_WHY,
  };
}

async function applyConversationalInterestMutation(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    audience: Audience;
    senderAdultId: string;
    currentSourceId: string;
    moveKind: "message" | "reply" | "reaction";
    mutation: DurableInterestMutation;
    occurredAt: Date;
  },
): Promise<void> {
  const mutation = input.mutation;
  if (input.moveKind === "reaction") {
    throw new FlorenceStoreUnauthorized("A reaction cannot change household interests");
  }
  if (
    mutation.sourceIds.length < 1 ||
    mutation.sourceIds.length > 10 ||
    !mutation.sourceIds.includes(input.currentSourceId)
  ) {
    throw new FlorenceStoreUnauthorized("A household interest change requires the current parent's Message");
  }
  await requireSteward(sql, input.householdId, input.senderAdultId);
  await assertSourcesVisible(sql, input.householdId, input.audience, input.senderAdultId, mutation.sourceIds);

  const objective = mutation.objective
    ? bounded(required(mutation.objective, "Interest objective"), 2_000)
    : null;
  const why = bounded(required(mutation.why, "Interest reason"), 2_000);
  const genericTerms = mutation.genericTerms ? normalizedInterestTerms(mutation.genericTerms) : null;
  const activeRows = await sql<ProactiveWorkRow[]>`
    select * from proactive_work where household_id=${input.householdId}
      and kind='interest_monitor' and visibility='household' and owner_adult_id is null
      and status in ('active','paused') for update
  `;

  if (mutation.operation === "stop") {
    assertUuid(mutation.interestWorkId, "Interest work ID");
    if (!activeRows.some((row) => row.id === mutation.interestWorkId)) {
      throw new FlorenceStoreConflict("The household interest is no longer active");
    }
    const stopped = await sql`
      delete from proactive_work
      where id=${mutation.interestWorkId} and household_id=${input.householdId}
        and kind='interest_monitor' and visibility='household' and owner_adult_id is null
        and status in ('active','paused') returning id
    `;
    if (stopped.length !== 1) {
      throw new FlorenceStoreConflict("The household interest changed before it could be stopped");
    }
    return;
  }

  if (!objective || !genericTerms) {
    throw new FlorenceStoreConflict("An active household interest requires an objective and generic terms");
  }
  const duplicate = activeRows.find(
    (row) => row.id !== mutation.interestWorkId && sameStrings([...row.discovery_terms].sort(), genericTerms),
  );
  if (duplicate) {
    if (mutation.operation === "create") {
      await linkProactiveWorkSources(sql, duplicate.id, mutation.sourceIds);
      return;
    }
    throw new FlorenceStoreConflict("The household already has this active interest");
  }
  const nextCheck = new Date(input.occurredAt.getTime() + 5 * 60_000);

  let workId: string;
  if (mutation.operation === "create") {
    workId = deterministicUuid(`conversation-interest\0${input.householdId}\0${input.currentSourceId}`);
    const inserted = await sql`
      insert into proactive_work (
        id,household_id,kind,visibility,owner_adult_id,objective,why,
        current_conclusion,discovery_terms,status,next_check_at,created_at
      ) values (${workId},${input.householdId},'interest_monitor','household',null,
        ${objective},${why},'No suggestion yet.',${genericTerms},'active',${nextCheck},${input.occurredAt})
      on conflict do nothing returning id
    `;
    if (inserted.length !== 1) {
      throw new FlorenceStoreConflict("The household interest was already created from this Message");
    }
  } else {
    assertUuid(mutation.interestWorkId, "Interest work ID");
    if (!activeRows.some((row) => row.id === mutation.interestWorkId)) {
      throw new FlorenceStoreConflict("The household interest is no longer visible");
    }
    workId = mutation.interestWorkId;
    const updated = await sql`
      update proactive_work set objective=${objective},why=${why},discovery_terms=${genericTerms},
        current_conclusion='No suggestion yet.',status='active',next_check_at=${nextCheck},
        last_error=null
      where id=${workId} and household_id=${input.householdId} and kind='interest_monitor'
        and visibility='household' and owner_adult_id is null and status in ('active','paused')
      returning id
    `;
    if (updated.length !== 1) {
      throw new FlorenceStoreConflict("The household interest changed before it could be updated");
    }
  }
  await linkProactiveWorkSources(sql, workId, mutation.sourceIds);
}

async function linkProactiveWorkSources(
  sql: postgres.TransactionSql,
  workId: string,
  sourceIds: readonly string[],
): Promise<void> {
  for (const sourceId of unique(sourceIds)) {
    await sql`
      insert into proactive_work_sources (work_id,source_id) values (${workId},${sourceId})
      on conflict do nothing
    `;
  }
}

function normalizedInterestTerms(values: readonly string[]): string[] {
  if (values.length < 1 || values.length > 8) {
    throw new FlorenceStoreConflict("Household interest discovery needs one to eight generic terms");
  }
  const terms = values.map((value) => {
    const term = required(value, "Generic interest term");
    if (
      term.length > 100 ||
      term.split(/\s+/u).length > 6 ||
      /(?:https?:\/\/|www\.|[\p{L}\p{N}._%+-]+@[\p{L}\p{N}.-]+\.[\p{L}]{2,})/iu.test(term) ||
      /\b[\p{L}\p{N}-]+\.[\p{L}]{2,24}(?:\/|\b)/iu.test(term) ||
      /\b\d{5}(?:-\d{4})?\b/u.test(term) ||
      /\d(?:\D*\d){6}/u.test(term)
    ) {
      throw new FlorenceStoreConflict("Interest search terms must be generic and non-identifying");
    }
    return term.toLocaleLowerCase("en-US");
  });
  if (new Set(terms).size !== terms.length) {
    throw new FlorenceStoreConflict("Household interest terms must be unique");
  }
  return terms.sort();
}

function validateReminderSchedule(value: ReminderSchedule): ReminderSchedule {
  return reminderSchedule(value as unknown as JsonValue);
}

function reminderSchedule(value: JsonValue | null): ReminderSchedule {
  if (!isRecord(value) || typeof value.kind !== "string") {
    throw new FlorenceStoreConflict("A reminder schedule is invalid");
  }
  const exactKeys = (keys: readonly string[]): void => {
    if (!sameStrings(Object.keys(value).sort(), [...keys].sort())) {
      throw new FlorenceStoreConflict("A reminder schedule has unexpected fields");
    }
  };
  const integer = (key: string, minimum: number, maximum: number): number => {
    const field = value[key];
    if (!Number.isInteger(field) || (field as number) < minimum || (field as number) > maximum) {
      throw new FlorenceStoreConflict(`Reminder ${key} is invalid`);
    }
    return field as number;
  };
  const text = (key: string): string => {
    const field = value[key];
    if (typeof field !== "string") throw new FlorenceStoreConflict(`Reminder ${key} is invalid`);
    return field;
  };
  const localTime = (): string => {
    const field = text("localTime");
    if (!/^(?:[01]\d|2[0-3]):[0-5]\d$/.test(field)) {
      throw new FlorenceStoreConflict("A reminder local time must use HH:mm");
    }
    return field;
  };
  const startsOn = (): string => {
    const field = text("startsOn");
    if (!isReminderDate(field)) throw new FlorenceStoreConflict("A reminder start date is invalid");
    return field;
  };
  if (value.kind === "once") {
    exactKeys(["kind", "at"]);
    const at = text("at");
    explicitInstant(at);
    return { kind: "once", at };
  }
  if (value.kind === "interval") {
    exactKeys(["kind", "everyMinutes", "anchorAt"]);
    const anchorAt = text("anchorAt");
    explicitInstant(anchorAt);
    return { kind: "interval", everyMinutes: integer("everyMinutes", 1, 525_600), anchorAt };
  }
  if (value.kind === "daily") {
    exactKeys(["kind", "everyDays", "localTime", "startsOn"]);
    return {
      kind: "daily",
      everyDays: integer("everyDays", 1, 3_650),
      localTime: localTime(),
      startsOn: startsOn(),
    };
  }
  if (value.kind === "weekly") {
    exactKeys(["kind", "everyWeeks", "weekdays", "localTime", "startsOn"]);
    if (!Array.isArray(value.weekdays) || value.weekdays.length === 0 || value.weekdays.length > 7) {
      throw new FlorenceStoreConflict("A weekly reminder needs one to seven weekdays");
    }
    const weekdays = value.weekdays.map((weekday) => {
      if (!Number.isInteger(weekday) || (weekday as number) < 1 || (weekday as number) > 7) {
        throw new FlorenceStoreConflict("A weekly reminder weekday is invalid");
      }
      return weekday as number;
    });
    if (new Set(weekdays).size !== weekdays.length) {
      throw new FlorenceStoreConflict("Weekly reminder weekdays must be unique");
    }
    return {
      kind: "weekly",
      everyWeeks: integer("everyWeeks", 1, 520),
      weekdays: weekdays.sort((a, b) => a - b),
      localTime: localTime(),
      startsOn: startsOn(),
    };
  }
  if (value.kind === "monthly") {
    exactKeys(["kind", "everyMonths", "dayOfMonth", "localTime", "startsOn"]);
    return {
      kind: "monthly",
      everyMonths: integer("everyMonths", 1, 1_200),
      dayOfMonth: integer("dayOfMonth", 1, 31),
      localTime: localTime(),
      startsOn: startsOn(),
    };
  }
  if (value.kind === "yearly") {
    exactKeys(["kind", "everyYears", "month", "dayOfMonth", "localTime", "startsOn"]);
    return {
      kind: "yearly",
      everyYears: integer("everyYears", 1, 100),
      month: integer("month", 1, 12),
      dayOfMonth: integer("dayOfMonth", 1, 31),
      localTime: localTime(),
      startsOn: startsOn(),
    };
  }
  throw new FlorenceStoreConflict("A reminder schedule kind is invalid");
}

function nextReminderOccurrence(schedule: ReminderSchedule, after: Date, timeZone: string): Date | null {
  assertReminderTimeZone(timeZone);
  if (schedule.kind === "once") {
    const candidate = explicitInstant(schedule.at);
    return candidate > after ? candidate : null;
  }
  if (schedule.kind === "interval") {
    const anchor = explicitInstant(schedule.anchorAt);
    const period = schedule.everyMinutes * 60_000;
    if (anchor > after) return anchor;
    return new Date(
      anchor.getTime() + (Math.floor((after.getTime() - anchor.getTime()) / period) + 1) * period,
    );
  }
  const afterLocalDate = reminderLocalDate(after, timeZone);
  const start = schedule.startsOn > afterLocalDate ? schedule.startsOn : afterLocalDate;
  for (let offset = 0; offset < 366 * 101 + 2; offset += 1) {
    const date = addReminderDays(start, offset);
    if (!reminderMatchesDate(schedule, date)) continue;
    const candidate = reminderLocalInstant(date, schedule.localTime, timeZone);
    if (candidate > after) return candidate;
  }
  throw new FlorenceStoreConflict("A reminder recurrence is too far in the future");
}

function reminderMatchesDate(
  schedule: Exclude<ReminderSchedule, { kind: "once" | "interval" }>,
  date: string,
): boolean {
  if (date < schedule.startsOn) return false;
  const days = reminderDaysBetween(schedule.startsOn, date);
  if (schedule.kind === "daily") return days % schedule.everyDays === 0;
  if (schedule.kind === "weekly") {
    return (
      Math.floor(days / 7) % schedule.everyWeeks === 0 && schedule.weekdays.includes(reminderIsoWeekday(date))
    );
  }
  const [year, month, day] = reminderDateParts(date);
  const [startYear, startMonth] = reminderDateParts(schedule.startsOn);
  if (schedule.kind === "monthly") {
    const months = (year - startYear) * 12 + month - startMonth;
    return (
      months >= 0 &&
      months % schedule.everyMonths === 0 &&
      day === Math.min(schedule.dayOfMonth, reminderDaysInMonth(year, month))
    );
  }
  return (
    year >= startYear &&
    (year - startYear) % schedule.everyYears === 0 &&
    month === schedule.month &&
    day === Math.min(schedule.dayOfMonth, reminderDaysInMonth(year, schedule.month))
  );
}

function isReminderDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const [year, month, day] = value.split("-").map(Number) as [number, number, number];
  return month >= 1 && month <= 12 && day >= 1 && day <= reminderDaysInMonth(year, month);
}

function reminderDateParts(value: string): [number, number, number] {
  if (!isReminderDate(value)) throw new FlorenceStoreConflict("A reminder date is invalid");
  return value.split("-").map(Number) as [number, number, number];
}

function addReminderDays(value: string, days: number): string {
  const [year, month, day] = reminderDateParts(value);
  return new Date(Date.UTC(year, month - 1, day + days)).toISOString().slice(0, 10);
}

function reminderDaysBetween(left: string, right: string): number {
  const [ly, lm, ld] = reminderDateParts(left);
  const [ry, rm, rd] = reminderDateParts(right);
  return Math.round((Date.UTC(ry, rm - 1, rd) - Date.UTC(ly, lm - 1, ld)) / 86_400_000);
}

function reminderIsoWeekday(value: string): number {
  const [year, month, day] = reminderDateParts(value);
  return new Date(Date.UTC(year, month - 1, day)).getUTCDay() || 7;
}

function reminderDaysInMonth(year: number, month: number): number {
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

function assertReminderTimeZone(timeZone: string): void {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone }).format(new Date(0));
  } catch {
    throw new FlorenceStoreConflict("The household time zone is invalid");
  }
}

function reminderZonedParts(value: Date, timeZone: string): { date: string; hour: number; minute: number } {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).formatToParts(value);
  const read = (type: Intl.DateTimeFormatPartTypes): string =>
    parts.find((part) => part.type === type)?.value ?? "";
  return {
    date: `${read("year")}-${read("month")}-${read("day")}`,
    hour: Number(read("hour")),
    minute: Number(read("minute")),
  };
}

function reminderLocalDate(value: Date, timeZone: string): string {
  return reminderZonedParts(value, timeZone).date;
}

function reminderLocalInstant(date: string, time: string, timeZone: string): Date {
  const [year, month, day] = reminderDateParts(date);
  const [hour, minute] = time.split(":").map(Number) as [number, number];
  const rough = Date.UTC(year, month - 1, day, hour, minute);
  const exact: Date[] = [];
  let firstAfter: Date | null = null;
  for (let delta = -18 * 60; delta <= 18 * 60; delta += 1) {
    const candidate = new Date(rough + delta * 60_000);
    const local = reminderZonedParts(candidate, timeZone);
    if (local.date !== date) continue;
    if (local.hour === hour && local.minute === minute) exact.push(candidate);
    if (firstAfter === null && (local.hour > hour || (local.hour === hour && local.minute > minute)))
      firstAfter = candidate;
  }
  if (exact.length > 0) return exact[0] as Date;
  if (firstAfter) return firstAfter;
  throw new FlorenceStoreConflict("A reminder local time cannot be resolved");
}

function reminderText(action: string): string {
  const normalized = required(action, "Reminder action");
  return `Reminder: ${normalized}${/[.!?]$/u.test(normalized) ? "" : "."}`;
}

const FAMILY_WORK_STATE_KEYS = [
  "claim",
  "continuationItems",
  "generation",
  "kind",
  "pendingCall",
  "phase",
  "progressRevision",
  "publicMapResearchContext",
  "steering",
  "terminal",
  "version",
] as const;

function initialFamilyWorkState(): FamilyWorkStateV1 {
  return {
    kind: "family_work_v1",
    version: 1,
    generation: 0,
    phase: "ready",
    claim: null,
    continuationItems: [],
    pendingCall: null,
    steering: [],
    publicMapResearchContext: [],
    progressRevision: 0,
    terminal: null,
  };
}

function familyWorkState(value: JsonValue | FamilyWorkStateV1 | null): FamilyWorkStateV1 {
  let serialized: string | undefined;
  try {
    serialized = JSON.stringify(value);
  } catch {
    throw new FlorenceStoreConflict("Family work state is not JSON serializable");
  }
  if (!serialized || Buffer.byteLength(serialized, "utf8") > MAX_FAMILY_WORK_STATE_BYTES) {
    throw new FlorenceStoreConflict("Family work state exceeds its durable size limit");
  }
  const canonical = JSON.parse(serialized) as JsonValue;
  if (!isRecord(canonical)) {
    throw new FlorenceStoreConflict("Stored family work state is invalid");
  }
  const state = canonical;
  assertExactJsonKeys(state, FAMILY_WORK_STATE_KEYS, "Family work state");
  if (state.kind !== "family_work_v1" || state.version !== 1) {
    throw new FlorenceStoreConflict("Family work state has an unsupported version");
  }
  const generation = familyWorkCounter(state.generation, "Family work generation");
  const progressRevision = familyWorkCounter(state.progressRevision, "Family work progress revision");
  const phase = state.phase;
  if (phase !== "ready" && phase !== "tool_pending" && phase !== "waiting" && phase !== "terminal") {
    throw new FlorenceStoreConflict("Family work state has an invalid phase");
  }

  let claim: FamilyWorkStateV1["claim"] = null;
  if (state.claim === undefined) throw new FlorenceStoreConflict("Family work claim state is invalid");
  if (state.claim !== null) {
    const storedClaim = strictJsonRecord(state.claim, "Family work claim");
    assertExactJsonKeys(storedClaim, ["claimId", "leaseUntil"], "Family work claim");
    const claimId = requiredStringField(storedClaim, "claimId", "Family work claim ID");
    assertUuid(claimId, "Family work claim ID");
    claim = {
      claimId,
      leaseUntil: instant(
        requiredStringField(storedClaim, "leaseUntil", "Family work claim lease"),
      ).toISOString(),
    };
  }

  const continuationItems = jsonArrayField(state, "continuationItems", "Family work continuation");
  let pendingCall: FamilyWorkStateV1["pendingCall"] = null;
  if (state.pendingCall === undefined) {
    throw new FlorenceStoreConflict("Family work pending call state is invalid");
  }
  if (state.pendingCall !== null) {
    const storedCall = strictJsonRecord(state.pendingCall, "Family work pending call");
    assertExactJsonKeys(storedCall, ["argumentsJson", "callId", "name"], "Family work pending call");
    const callId = limitedRequiredString(
      requiredStringField(storedCall, "callId", "Family work call ID"),
      200,
      "Family work call ID",
    );
    const name = limitedRequiredString(
      requiredStringField(storedCall, "name", "Family work capability name"),
      200,
      "Family work capability name",
    );
    const argumentsJson = storedCall.argumentsJson;
    if (typeof argumentsJson !== "string" || argumentsJson.length > 65_536) {
      throw new FlorenceStoreConflict("Family work call arguments are invalid");
    }
    try {
      JSON.parse(argumentsJson);
    } catch {
      throw new FlorenceStoreConflict("Family work call arguments are not JSON");
    }
    pendingCall = { callId, name, argumentsJson };
  }

  const steering = jsonArrayField(state, "steering", "Family work steering").map((item) => {
    const entry = strictJsonRecord(item, "Family work steering entry");
    assertExactJsonKeys(entry, ["occurredAt", "sourceId", "text"], "Family work steering entry");
    const sourceId = requiredStringField(entry, "sourceId", "Family work steering source ID");
    assertUuid(sourceId, "Family work steering source ID");
    return {
      sourceId,
      text: limitedRequiredString(
        requiredStringField(entry, "text", "Family work steering text"),
        4_000,
        "Family work steering text",
      ),
      occurredAt: instant(
        requiredStringField(entry, "occurredAt", "Family work steering time"),
      ).toISOString(),
    };
  });
  const steeringSourceIds = steering.map((entry) => entry.sourceId);
  if (unique(steeringSourceIds).length !== steeringSourceIds.length) {
    throw new FlorenceStoreConflict("Family work steering contains a duplicate source");
  }
  for (let index = 1; index < steering.length; index += 1) {
    const previous = steering[index - 1];
    const current = steering[index];
    if (previous && current && instant(previous.occurredAt) > instant(current.occurredAt)) {
      throw new FlorenceStoreConflict("Family work steering is out of order");
    }
  }
  const publicMapResearchContext = familyWorkStringArray(
    state,
    "publicMapResearchContext",
    20_000,
    "Family work map context",
    false,
    false,
  );
  let terminal: FamilyWorkStateV1["terminal"] = null;
  if (state.terminal === undefined) {
    throw new FlorenceStoreConflict("Family work terminal state is invalid");
  }
  if (state.terminal !== null) {
    const storedTerminal = strictJsonRecord(state.terminal, "Family work terminal result");
    assertExactJsonKeys(storedTerminal, ["outcome", "text"], "Family work terminal result");
    const outcome = storedTerminal.outcome;
    if (outcome !== "succeeded" && outcome !== "partial" && outcome !== "failed" && outcome !== "cancelled") {
      throw new FlorenceStoreConflict("Family work terminal outcome is invalid");
    }
    terminal = {
      outcome,
      text: limitedRequiredString(
        requiredStringField(storedTerminal, "text", "Family work terminal text"),
        10_000,
        "Family work terminal text",
      ),
    };
  }
  if ((phase === "tool_pending") !== (pendingCall !== null)) {
    throw new FlorenceStoreConflict("Family work pending-call state is inconsistent");
  }
  if ((phase === "terminal") !== (terminal !== null)) {
    throw new FlorenceStoreConflict("Family work terminal state is inconsistent");
  }
  if ((phase === "waiting" || phase === "terminal") && claim !== null) {
    throw new FlorenceStoreConflict("Waiting or terminal family work cannot remain claimed");
  }

  return {
    kind: "family_work_v1",
    version: 1,
    generation,
    phase,
    claim,
    continuationItems: [...continuationItems],
    pendingCall,
    steering,
    publicMapResearchContext,
    progressRevision,
    terminal,
  };
}

function familyWorkCounter(value: unknown, name: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0 || (value as number) > MAX_FAMILY_WORK_COUNTER) {
    throw new FlorenceStoreConflict(`${name} is invalid`);
  }
  return value as number;
}

function familyWorkContinuationWithoutPendingCall(state: FamilyWorkStateV1): readonly JsonValue[] {
  const pendingCall = state.pendingCall;
  if (!pendingCall) return [...state.continuationItems];
  let removed = false;
  const continuationItems = state.continuationItems.filter((item) => {
    if (removed || !isRecord(item) || item.type !== "function_call" || item.call_id !== pendingCall.callId) {
      return true;
    }
    removed = true;
    return false;
  });
  if (!removed) {
    throw new FlorenceStoreConflict("Steered family work lost its pending capability call");
  }
  return continuationItems;
}

export function steerFamilyWorkState(
  stateInput: FamilyWorkStateV1,
  steering: FamilyWorkStateV1["steering"][number],
): FamilyWorkStateV1 {
  const state = familyWorkState(stateInput);
  return familyWorkState({
    ...state,
    generation: incrementFamilyWorkCounter(state.generation, "Family work generation"),
    phase: "ready",
    claim: null,
    continuationItems: familyWorkContinuationWithoutPendingCall(state),
    pendingCall: null,
    steering: [...state.steering, steering],
    terminal: null,
  });
}

function incrementFamilyWorkCounter(value: number, name: string): number {
  familyWorkCounter(value, name);
  if (value >= MAX_FAMILY_WORK_COUNTER) throw new FlorenceStoreConflict(`${name} is exhausted`);
  return value + 1;
}

function strictJsonRecord(value: JsonValue, name: string): Record<string, JsonValue> {
  if (!isRecord(value)) throw new FlorenceStoreConflict(`${name} is invalid`);
  return { ...value };
}

function assertExactJsonKeys(record: Record<string, JsonValue>, keys: readonly string[], name: string): void {
  if (!sameStrings(Object.keys(record).sort(), [...keys].sort())) {
    throw new FlorenceStoreConflict(`${name} has an invalid shape`);
  }
}

function jsonArrayField(record: Record<string, JsonValue>, key: string, name: string): JsonValue[] {
  const value = record[key];
  if (!Array.isArray(value)) throw new FlorenceStoreConflict(`${name} is invalid`);
  return [...value];
}

function familyWorkStringArray(
  record: Record<string, JsonValue>,
  key: string,
  maximumLength: number,
  name: string,
  requireNonempty = true,
  requireUnique = true,
): string[] {
  const values = jsonArrayField(record, key, name).map((value) => {
    if (typeof value !== "string" || value.length > maximumLength || (requireNonempty && !value.trim())) {
      throw new FlorenceStoreConflict(`${name} contains invalid text`);
    }
    return value;
  });
  if (requireUnique && unique(values).length !== values.length) {
    throw new FlorenceStoreConflict(`${name} contains a duplicate`);
  }
  return values;
}

function limitedRequiredString(value: string, maximum: number, name: string): string {
  const normalized = required(value, name);
  if (normalized.length > maximum) throw new FlorenceStoreConflict(`${name} is too long`);
  return normalized;
}

function sameFamilyWorkSteering(
  left: FamilyWorkStateV1["steering"],
  right: FamilyWorkStateV1["steering"],
): boolean {
  return (
    left.length === right.length &&
    left.every(
      (entry, index) =>
        entry.sourceId === right[index]?.sourceId &&
        entry.text === right[index]?.text &&
        entry.occurredAt === right[index]?.occurredAt,
    )
  );
}

async function activeReminderChannel(
  sql: postgres.TransactionSql,
  work: ProactiveWorkRow,
): Promise<ChannelRow | null> {
  if (work.visibility === "private") {
    const [channel] = await sql<(ChannelRow & { current_identity_digest: string | null })[]>`
      select channel.*,adult.identity_subject_digest as current_identity_digest
      from linq_channels channel join people adult on adult.household_id=channel.household_id
        and adult.id=${work.owner_adult_id} and adult.kind='adult' and adult.status='verified'
      where channel.household_id=${work.household_id} and channel.audience='private'
        and channel.adult_one_id=${work.owner_adult_id} and channel.adult_two_id is null
        and channel.revoked_at is null and channel.stopped_at is null
      order by channel.bound_at desc,channel.id desc limit 1 for share of channel,adult
    `;
    if (
      !channel?.current_identity_digest ||
      channel.identity_one_digest !== channel.current_identity_digest ||
      channel.identity_two_digest !== null ||
      channel.authority_digest !==
        digestStrings([work.owner_adult_id as string, channel.current_identity_digest])
    )
      return null;
    return channel;
  }
  const [channel] = await sql<(ChannelRow & FamilyGroupAuthorityRow)[]>`
    select family_group.*,founder.id as founder_adult_id,founder.identity_subject_digest as founder_identity_digest,
      founder.status as founder_status,partner.id as partner_adult_id,
      partner.identity_subject_digest as partner_identity_digest,partner.status as partner_status
    from linq_channels family_group
    join people founder on founder.household_id=family_group.household_id and founder.kind='adult' and founder.role='steward' and founder.adult_slot=1
    join people partner on partner.household_id=family_group.household_id and partner.kind='adult' and partner.role='steward' and partner.adult_slot=2
    where family_group.household_id=${work.household_id} and family_group.audience='group'
      and family_group.adult_two_id is not null and family_group.revoked_at is null and family_group.stopped_at is null
    order by family_group.bound_at desc,family_group.id desc limit 1 for share of family_group,founder,partner
  `;
  if (!channel || !isExactFamilyGroupAuthority(channel, channel, channel.founder_adult_id ?? "")) return null;
  return channel;
}

async function terminalizeUnsentReminderOccurrences(
  sql: postgres.TransactionSql,
  reminderId: string,
): Promise<void> {
  await sql`
    update messages message set status='failed',sending_at=null,retry_at=null,
      last_error='Superseded by a reminder change before delivery'
    from sources source
    where source.id=message.source_id and source.metadata->>'reminderId'=${reminderId}
      and message.direction='outbound' and message.status in ('pending','failed')
  `;
}

async function rearmQueuedReminderOccurrence(
  sql: postgres.TransactionSql,
  reminderId: string,
  occurredAt: Date,
): Promise<boolean> {
  const [message] = await sql<{ source_id: string; status: "pending" | "sending" | "failed" }[]>`
    select message.source_id,message.status
    from messages message join sources source on source.id=message.source_id
    where source.metadata->>'reminderId'=${reminderId}
      and message.direction='outbound' and message.status in ('pending','sending','failed')
      and not (
        message.status='failed'
        and message.last_error='Superseded by a reminder change before delivery'
      )
    order by case message.status when 'sending' then 0 else 1 end,
      message.not_before,source.occurred_at,message.source_id
    limit 1 for update of message
  `;
  if (!message) return false;
  if (message.status !== "sending") {
    await sql`
      update messages set status='pending',not_before=${occurredAt},sending_at=null,retry_at=null,
        last_error=null where source_id=${message.source_id}
    `;
  }
  return true;
}

async function updateQueuedReminderOccurrenceText(
  sql: postgres.TransactionSql,
  reminderId: string,
  text: string,
  occurredAt: Date,
): Promise<boolean> {
  const [message] = await sql<{ source_id: string }[]>`
    select message.source_id
    from messages message join sources source on source.id=message.source_id
    where source.metadata->>'reminderId'=${reminderId}
      and message.direction='outbound' and message.status='pending'
      and message.sending_at is null and message.last_error is null
      and message.provider_message_id is null and message.receipt_detail is null
    order by message.not_before,source.occurred_at,message.source_id
    limit 1 for update of message
  `;
  if (!message) return false;
  await sql`
    update messages set text=${text},status='pending',not_before=${occurredAt},sending_at=null,
      retry_at=null,last_error=null where source_id=${message.source_id}
  `;
  await sql`
    update sources set label=${bounded(text, 500)},
      metadata=metadata || ${sql.json({ authoredText: text })}
    where id=${message.source_id}
  `;
  return true;
}

async function completeDeliveredOneShotReminder(
  sql: postgres.TransactionSql,
  sourceId: string,
): Promise<void> {
  await sql`
    update proactive_work work set status='completed',next_check_at=null,last_error=null
    from sources source
    where source.id=${sourceId} and source.metadata->>'reminderId'=work.id::text
      and work.kind='reminder' and work.status='delivering'
      and work.reminder_schedule->>'kind'='once'
  `;
}

type FamilyWorkDeliveryKind = "progress" | "waiting" | "terminal";

async function terminalizeUnsentFamilyWorkOutbounds(
  sql: postgres.TransactionSql,
  workId: string,
  change: "steering" | "cancellation",
): Promise<void> {
  await sql`
    update messages message set status='failed',sending_at=null,retry_at=null,
      last_error=${`Superseded by family work ${change} before delivery`}
    from sources source
    where source.id=message.source_id and source.metadata->>'familyWorkId'=${workId}
      and message.direction='outbound' and message.status in ('pending','failed')
  `;
}

async function completeDeliveredFamilyWorkTerminal(
  sql: postgres.TransactionSql,
  sourceId: string,
): Promise<void> {
  await sql`
    update proactive_work work set
      status=case when work.task_state->'terminal'->>'outcome'='cancelled'
        then 'cancelled' else 'completed' end,
      next_check_at=null,last_error=null
    from sources source
    where source.id=${sourceId} and source.metadata->>'familyWorkId'=work.id::text
      and source.metadata->>'familyWorkDeliveryKind'='terminal'
      and work.kind='family_task' and work.status='delivering'
      and work.task_state->>'generation'=source.metadata->>'familyWorkGeneration'
      and work.task_state->>'progressRevision'=
        source.metadata->>'familyWorkProgressRevision'
  `;
}

async function insertFamilyWorkOutbound(
  sql: postgres.TransactionSql,
  work: ProactiveWorkRow,
  state: FamilyWorkStateV1,
  deliveryKind: FamilyWorkDeliveryKind,
  text: string,
  occurredAt: Date,
): Promise<void> {
  const channel = await activeReminderChannel(sql, work);
  if (!channel) {
    throw new FlorenceStoreConflict("Family work no longer has an active delivery conversation");
  }
  await insertProactiveOutbound(sql, {
    workId: work.id,
    suffix: `family-work:${deliveryKind}:${state.generation}:${state.progressRevision}`,
    householdId: work.household_id,
    channel,
    visibility: work.visibility,
    ownerAdultId: work.owner_adult_id,
    text,
    metadata: {
      familyWorkId: work.id,
      familyWorkGeneration: state.generation,
      familyWorkProgressRevision: state.progressRevision,
      familyWorkDeliveryKind: deliveryKind,
    },
    notBefore: occurredAt,
    occurredAt,
  });
}

async function insertProactiveOutbound(
  sql: postgres.TransactionSql,
  input: {
    workId: string;
    suffix: string;
    householdId: string;
    channel: ChannelRow;
    visibility: Visibility;
    ownerAdultId: string | null;
    text: string;
    metadata?: JsonObject;
    notBefore: Date;
    occurredAt: Date;
  },
): Promise<void> {
  if (input.notBefore < input.occurredAt) {
    throw new FlorenceStoreConflict("A proactive delivery cannot precede its finding");
  }
  const stable = sha256(input.suffix);
  await insertOutbound(sql, {
    sourceId: deterministicUuid(`proactive-outbound\0${input.workId}\0${stable}`),
    idempotencyKey: `proactive:${input.workId}:${stable}`,
    moveKind: "message",
    text: bounded(required(input.text, "Proactive message"), 10_000),
    turnId: deterministicUuid(`proactive-turn\0${input.workId}\0${stable}`),
    turnPart: 0,
    notBefore: input.notBefore.toISOString(),
    householdId: input.householdId,
    channelId: input.channel.id,
    visibility: input.visibility,
    ownerAdultId: input.ownerAdultId,
    ...(input.metadata ? { metadata: input.metadata } : {}),
    occurredAt: input.occurredAt,
  });
}

type StoredBriefingCandidate = SharedBriefingCandidate & {
  sourceIds: readonly string[];
  actionAnchorDigest: string | null;
  actionKey: string | null;
};

type StoredBriefingCandidateGroup = {
  candidate: StoredBriefingCandidate;
  members: readonly StoredBriefingCandidate[];
  ownerAdultIds: readonly string[];
};

async function lockHouseholdGooglePolls(sql: postgres.TransactionSql, householdId: string): Promise<void> {
  await sql`
    select id from proactive_work
    where household_id=${householdId}
      and kind in ('personal_google_poll','family_calendar_poll')
      and status in ('active','paused')
    order by id for update
  `;
}

async function lockHouseholdDocketMonitors(
  sql: postgres.TransactionSql,
  householdId: string,
): Promise<readonly { id: string; last_error: string | null }[]> {
  return sql<{ id: string; last_error: string | null }[]>`
    select id,last_error from proactive_work
    where household_id=${householdId} and kind='finite_monitor'
      and status in ('active','paused')
    order by id for update
  `;
}

async function completeHouseholdDocketCandidates(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    candidateIds: readonly string[];
    basisSourceId: string;
    handledAt: Date;
  },
): Promise<void> {
  assertUuid(input.basisSourceId, "Household docket completion source ID");
  const [household] = await sql<{ time_zone: string }[]>`
    select time_zone from households where id=${input.householdId} for share
  `;
  if (!household) throw new FlorenceStoreConflict("The household docket no longer exists");
  await lockHouseholdGooglePolls(sql, input.householdId);
  const linkedMonitors = await lockHouseholdDocketMonitors(sql, input.householdId);
  const reviews = await sql<(ProactiveWorkRow & { private_conflict_busy_sharing_enabled: boolean })[]>`
    select work.*,
      coalesce(person.preferences->'privateConflictBusySharingEnabled'='true'::jsonb,false)
        as private_conflict_busy_sharing_enabled
    from proactive_work work join people person
      on person.household_id=work.household_id and person.id=work.owner_adult_id
    where work.household_id=${input.householdId}
      and work.kind='initial_private_review' and work.status='completed'
      and person.kind='adult' and person.status='verified'
      and nullif(person.preferences->>'proactiveUseAcceptedAt','') is not null
      and coalesce(person.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
    order by work.owner_adult_id,work.id for update of work
  `;
  const groups = rankedBriefingCandidateGroups(
    reviews.flatMap((review) => {
      if (!review.owner_adult_id) return [];
      return storedBriefingCandidates(review.briefing_candidates)
        .filter(
          (candidate) =>
            isCurrentDocketCandidate(candidate, input.handledAt, household.time_zone) &&
            (candidate.category !== "conflict" || review.private_conflict_busy_sharing_enabled),
        )
        .map((candidate) => ({ candidate, ownerAdultId: review.owner_adult_id as string }));
    }),
  );
  const completedGroups = input.candidateIds.map((candidateId) =>
    groups.find((group) => group.members.some((candidate) => candidate.candidateId === candidateId)),
  );
  if (completedGroups.some((group) => group === undefined)) {
    throw new FlorenceStoreConflict("A household docket item changed before it was completed");
  }
  const completedCandidates = completedGroups.flatMap((group) => (group ? [...group.members] : []));
  const completedCandidateSourceDigests = await readStableGoogleSourceDigestMap(
    sql,
    input.householdId,
    unique(completedCandidates.flatMap((candidate) => [...candidate.sourceIds])),
  );
  const completedActionKeys = unique(
    completedCandidates.map((candidate) => {
      const actionKey = googleActionKeyForCandidate(candidate, completedCandidateSourceDigests);
      if (!actionKey) {
        throw new FlorenceStoreConflict("A household docket item no longer has a stable provider identity");
      }
      return actionKey;
    }),
  ).sort();
  const [basisSource] = await sql<{ metadata: JsonValue }[]>`
    select metadata from sources where id=${input.basisSourceId}
      and household_id=${input.householdId} for update
  `;
  if (!basisSource) {
    throw new FlorenceStoreConflict("The household docket completion Message no longer exists");
  }
  const priorCompletedActionKeys = jsonRecord(basisSource.metadata).completedGoogleActionKeys;
  if (
    priorCompletedActionKeys !== undefined &&
    (!Array.isArray(priorCompletedActionKeys) ||
      priorCompletedActionKeys.some((value) => typeof value !== "string"))
  ) {
    throw new FlorenceStoreConflict("Stored completed Google action keys are invalid");
  }
  const durableCompletedActionKeys = unique([
    ...((priorCompletedActionKeys ?? []) as string[]),
    ...completedActionKeys,
  ]).sort();
  for (const actionKey of durableCompletedActionKeys) {
    assertDigest(actionKey, "Completed Google action key");
  }
  await sql`
    update sources set metadata=metadata||${sql.json({
      completedGoogleActionKeys: durableCompletedActionKeys,
    })}
    where id=${input.basisSourceId} and household_id=${input.householdId}
  `;
  const completedMonitorIds = linkedMonitors.flatMap((monitor) => {
    const actionKey = googleActionKeyFromWorkMarker(monitor.last_error);
    return actionKey && completedActionKeys.includes(actionKey) ? [monitor.id] : [];
  });
  for (const actionKey of completedActionKeys) {
    await failPendingGoogleActionOutbounds(sql, {
      householdId: input.householdId,
      actionKey,
      reason: "The family marked this item handled",
    });
  }
  const obsoleteCalendarActions = await sql<{ id: string; approval_prompt_source_id: string | null }[]>`
    select id,approval_prompt_source_id from calendar_actions
    where household_id=${input.householdId}
      and google_action_key in ${sql(completedActionKeys)}
      and status in ('offered','pending','failed')
      and not (
        status='pending' and last_error=${CALENDAR_ACTION_EXECUTION_MARKER}
        and retry_at>${input.handledAt}
      )
    order by id for update
  `;
  const obsoleteCalendarPromptSourceIds = unique(
    obsoleteCalendarActions.flatMap(({ approval_prompt_source_id }) =>
      approval_prompt_source_id ? [approval_prompt_source_id] : [],
    ),
  );
  if (obsoleteCalendarPromptSourceIds.length > 0) {
    await sql`
      update messages set status='failed',sending_at=null,retry_at=null,
        last_error='The family marked this Calendar item handled'
      where source_id in ${sql(obsoleteCalendarPromptSourceIds)}
        and direction='outbound' and status='pending'
    `;
  }
  if (obsoleteCalendarActions.length > 0) {
    await sql`
      delete from calendar_actions where id in ${sql(obsoleteCalendarActions.map(({ id }) => id))}
        and household_id=${input.householdId} and status in ('offered','pending','failed')
        and not (
          status='pending' and last_error=${CALENDAR_ACTION_EXECUTION_MARKER}
          and retry_at>${input.handledAt}
        )
    `;
  }
  if (completedMonitorIds.length > 0) {
    await sql`
      update proactive_work set status='completed',next_check_at=null
      where id in ${sql(completedMonitorIds)} and household_id=${input.householdId}
        and kind='finite_monitor' and status in ('active','paused')
    `;
  }
  const completedCandidateIds = new Set(
    completedGroups.flatMap((group) =>
      group ? group.members.map((candidate) => candidate.candidateId) : [],
    ),
  );
  for (const review of reviews) {
    const candidates = storedBriefingCandidates(review.briefing_candidates);
    const retained = candidates.filter((candidate) => !completedCandidateIds.has(candidate.candidateId));
    if (retained.length === candidates.length) continue;
    await sql`
      update proactive_work set briefing_candidates=${sql.json(retained)}
      where id=${review.id} and kind='initial_private_review' and status='completed'
    `;
  }
}

function distinctBriefingCandidateGroups(
  candidates: readonly { candidate: StoredBriefingCandidate; ownerAdultId: string }[],
): StoredBriefingCandidateGroup[] {
  const byTuple = new Map<
    string,
    {
      candidate: StoredBriefingCandidate;
      members: StoredBriefingCandidate[];
      ownerAdultIds: string[];
    }[]
  >();
  for (const { candidate, ownerAdultId } of candidates) {
    const tuple = briefingCandidateIdentity(candidate);
    const groups = byTuple.get(tuple) ?? [];
    const existing = groups.find((group) =>
      group.members.some((member) =>
        member.sourceIds.some((sourceId) => candidate.sourceIds.includes(sourceId)),
      ),
    );
    if (existing) {
      existing.members.push(candidate);
      if (!existing.ownerAdultIds.includes(ownerAdultId)) existing.ownerAdultIds.push(ownerAdultId);
      if (compareBriefingCandidates(candidate, existing.candidate) < 0) {
        existing.candidate = candidate;
      }
      continue;
    }
    groups.push({ candidate, members: [candidate], ownerAdultIds: [ownerAdultId] });
    byTuple.set(tuple, groups);
  }
  return [...byTuple.values()].flat();
}

function rankedBriefingCandidateGroups(
  candidates: readonly { candidate: StoredBriefingCandidate; ownerAdultId: string }[],
): StoredBriefingCandidateGroup[] {
  return distinctBriefingCandidateGroups(candidates).sort((left, right) =>
    compareBriefingCandidates(left.candidate, right.candidate),
  );
}

function briefingCandidateIdentity(candidate: StoredBriefingCandidate): string {
  return JSON.stringify(
    candidate.actionAnchorDigest
      ? {
          category: candidate.category,
          dueAt: candidate.dueAt,
          needsAnswer: candidate.needsAnswer,
          actionAnchorDigest: candidate.actionAnchorDigest,
        }
      : {
          category: candidate.category,
          summary: candidate.summary,
          dueAt: candidate.dueAt,
          needsAnswer: candidate.needsAnswer,
        },
  );
}

function compareBriefingCandidates(left: StoredBriefingCandidate, right: StoredBriefingCandidate): number {
  const urgencyRank = { now: 0, soon: 1, watch: 2 } as const;
  const categoryRank = { deadline: 0, conflict: 1, handoff: 2, loose_end: 3, family_date: 4 } as const;
  const leftDue = left.dueAt ? Date.parse(left.dueAt) : Number.POSITIVE_INFINITY;
  const rightDue = right.dueAt ? Date.parse(right.dueAt) : Number.POSITIVE_INFINITY;
  return (
    urgencyRank[left.urgency] - urgencyRank[right.urgency] ||
    leftDue - rightDue ||
    Number(right.needsAnswer) - Number(left.needsAnswer) ||
    categoryRank[left.category] - categoryRank[right.category] ||
    left.candidateId.localeCompare(right.candidateId)
  );
}

function isCurrentDocketCandidate(candidate: StoredBriefingCandidate, now: Date, timeZone: string): boolean {
  if (
    (candidate.category !== "family_date" && candidate.category !== "conflict") ||
    candidate.dueAt === null
  ) {
    return true;
  }
  return (
    calendarDateInTimeZone(candidate.dueAt, timeZone) >= calendarDateInTimeZone(now.toISOString(), timeZone)
  );
}

function mergedBriefingCandidates(candidates: readonly StoredBriefingCandidate[]): StoredBriefingCandidate[] {
  const merged = new Map<string, StoredBriefingCandidate[]>();
  for (const candidate of candidates) {
    const identity = briefingCandidateIdentity(candidate);
    const matches = merged.get(identity) ?? [];
    const existing = matches.find((match) =>
      match.sourceIds.some((sourceId) => candidate.sourceIds.includes(sourceId)),
    );
    if (!existing) {
      matches.push(candidate);
      merged.set(identity, matches);
      continue;
    }
    const representative = compareBriefingCandidates(candidate, existing) < 0 ? candidate : existing;
    matches[matches.indexOf(existing)] = {
      ...representative,
      sourceIds: unique([...existing.sourceIds, ...candidate.sourceIds]).sort(),
    };
  }
  return [...merged.values()].flat().sort(compareBriefingCandidates);
}

function isSharedBriefingCategory(value: string): value is SharedBriefingCandidate["category"] {
  return ["deadline", "conflict", "handoff", "family_date", "loose_end"].includes(value);
}

function privateReviewCandidate(
  candidateId: string,
  candidate: Omit<SharedBriefingCandidate, "candidateId">,
  sourceIds: readonly string[],
  actionAnchorDigest: string | null = null,
  actionKey: string | null = null,
): StoredBriefingCandidate {
  assertUuid(candidateId, "Briefing candidate ID");
  const category = candidate.category;
  if (!isSharedBriefingCategory(category)) {
    throw new FlorenceStoreConflict("A briefing candidate category is invalid");
  }
  const urgency = candidate.urgency;
  if (!["now", "soon", "watch"].includes(urgency)) {
    throw new FlorenceStoreConflict("A briefing candidate urgency is invalid");
  }
  const summary = required(candidate.summary, "Household-safe briefing summary");
  if (summary.length > 2_000) throw new FlorenceStoreConflict("A briefing summary is too long");
  if (/[\r\n]/u.test(summary)) {
    throw new FlorenceStoreConflict("A briefing summary must be one visible line");
  }
  const cited = unique(sourceIds);
  if (cited.length < 1 || cited.length > 20) {
    throw new FlorenceStoreConflict("A briefing candidate needs one to twenty private sources");
  }
  for (const sourceId of cited) assertUuid(sourceId, "Briefing source ID");
  if (actionAnchorDigest !== null) assertDigest(actionAnchorDigest, "Briefing action anchor");
  if (actionKey !== null) assertDigest(actionKey, "Briefing Google action key");
  return {
    candidateId,
    category,
    summary,
    urgency,
    dueAt: candidate.dueAt === null ? null : instant(candidate.dueAt).toISOString(),
    needsAnswer: candidate.needsAnswer,
    sourceIds: cited,
    actionAnchorDigest,
    actionKey,
  };
}

function storedBriefingCandidates(value: JsonValue): StoredBriefingCandidate[] {
  if (!Array.isArray(value)) throw new FlorenceStoreConflict("Stored briefing candidates are invalid");
  return value.map((item) => {
    const record = jsonRecord(item);
    const sourceIds = record.sourceIds;
    if (!Array.isArray(sourceIds) || sourceIds.some((sourceId) => typeof sourceId !== "string")) {
      throw new FlorenceStoreConflict("Stored briefing candidate sources are invalid");
    }
    const dueAt = record.dueAt;
    const needsAnswer = record.needsAnswer;
    if (dueAt !== null && typeof dueAt !== "string") {
      throw new FlorenceStoreConflict("Stored briefing due date is invalid");
    }
    if (typeof needsAnswer !== "boolean") {
      throw new FlorenceStoreConflict("Stored briefing answer state is invalid");
    }
    return privateReviewCandidate(
      requiredStringField(record, "candidateId", "Stored briefing candidate ID"),
      {
        category: briefingCategory(record.category),
        summary: requiredStringField(record, "summary", "Stored briefing summary"),
        urgency: briefingUrgency(record.urgency),
        dueAt,
        needsAnswer,
      },
      sourceIds as string[],
      record.actionAnchorDigest === undefined || record.actionAnchorDigest === null
        ? null
        : requiredStringField(record, "actionAnchorDigest", "Stored briefing action anchor"),
      record.actionKey === undefined || record.actionKey === null
        ? null
        : requiredStringField(record, "actionKey", "Stored briefing Google action key"),
    );
  });
}

async function isCompletedPrivateReviewReadyForHousehold(
  sql: postgres.TransactionSql,
  reviewId: string,
): Promise<boolean> {
  const messages = await sql<
    {
      status: string;
      last_error: string | null;
      has_google_source_ids: boolean;
      has_surviving_google_source: boolean;
    }[]
  >`
    select message.status,message.last_error,
      case
        when jsonb_typeof(outbound_source.metadata->'googleSourceIds')='array'
        then jsonb_array_length(outbound_source.metadata->'googleSourceIds')>0
        else false
      end as has_google_source_ids,
      exists (
        select 1
        from jsonb_array_elements_text(
          case
            when jsonb_typeof(outbound_source.metadata->'googleSourceIds')='array'
            then outbound_source.metadata->'googleSourceIds'
            else '[]'::jsonb
          end
        ) linked(id)
        join proactive_work_sources evidence
          on evidence.work_id=${reviewId} and evidence.source_id::text=linked.id
      ) as has_surviving_google_source
    from messages message
    left join sources outbound_source on outbound_source.id=message.source_id
    where message.idempotency_key like ${`initial-private-review:${reviewId}:%`}
    order by message.turn_part
    for share of message
  `;
  if (messages.length < 1 || messages.length > 3) return false;
  return messages.every(
    (message) =>
      message.status === "sent" ||
      (message.status === "failed" &&
        message.last_error === GOOGLE_SOURCE_REMOVED_BEFORE_DELIVERY &&
        message.has_google_source_ids &&
        !message.has_surviving_google_source),
  );
}

export function initialPrivateGoogleScanDigest(scan: InitialPrivateGoogleScanV1): string {
  return createHash("sha256")
    .update(JSON.stringify(initialPrivateGoogleScan(scan)))
    .digest("hex");
}

function storedInitialPrivateGoogleScan(value: JsonValue): InitialPrivateGoogleScanV1 | null {
  if (!Array.isArray(value)) throw new FlorenceStoreConflict("Stored briefing candidates are invalid");
  if (value.length === 0) return null;
  const first = value[0];
  if (!isRecord(first) || first.kind !== "initial_private_google_scan_v1") return null;
  if (value.length !== 1) {
    throw new FlorenceStoreConflict("An active initial Google scan needs exactly one state envelope");
  }
  return initialPrivateGoogleScan(first);
}

function initialPrivateGoogleScan(value: JsonValue): InitialPrivateGoogleScanV1 {
  if (!isRecord(value) || value.kind !== "initial_private_google_scan_v1" || value.version !== 1) {
    throw new FlorenceStoreConflict("Initial private Google scan state is invalid");
  }
  if (value.scannerVersion !== "complete_private_google_review_v1") {
    throw new FlorenceStoreConflict("Initial private Google scanner version is invalid");
  }
  const connectionId = requiredStringField(value, "connectionId", "Google scan connection");
  assertUuid(connectionId, "Google scan connection ID");
  const anchoredAt = instant(requiredStringField(value, "anchoredAt", "Google scan anchor")).toISOString();
  const gmailAfter = instant(
    requiredStringField(value, "gmailAfter", "Google scan Gmail start"),
  ).toISOString();
  const calendarTimeMin = instant(
    requiredStringField(value, "calendarTimeMin", "Google scan Calendar start"),
  ).toISOString();
  const calendarTimeMax = instant(
    requiredStringField(value, "calendarTimeMax", "Google scan Calendar end"),
  ).toISOString();
  if (
    Date.parse(gmailAfter) >= Date.parse(anchoredAt) ||
    Date.parse(calendarTimeMin) >= Date.parse(anchoredAt) ||
    Date.parse(calendarTimeMax) <= Date.parse(anchoredAt) ||
    Date.parse(anchoredAt) - Date.parse(gmailAfter) !== 90 * 24 * 60 * 60_000 ||
    Date.parse(anchoredAt) - Date.parse(calendarTimeMin) !== 90 * 24 * 60 * 60_000 ||
    Date.parse(calendarTimeMax) - Date.parse(anchoredAt) !== 21 * 24 * 60 * 60_000
  ) {
    throw new FlorenceStoreConflict("Initial private Google scan bounds are invalid");
  }
  const excludedFamilyCalendarId = value.excludedFamilyCalendarId;
  if (excludedFamilyCalendarId !== null && typeof excludedFamilyCalendarId !== "string") {
    throw new FlorenceStoreConflict("Initial private Google scan exclusion is invalid");
  }
  const phase = value.phase;
  if (
    phase !== "calendar_targets" &&
    phase !== "gmail_baseline" &&
    phase !== "calendar_baseline" &&
    phase !== "gmail_replay" &&
    phase !== "calendar_replay" &&
    phase !== "calendar_verify" &&
    phase !== "calendar_manifest" &&
    phase !== "ready"
  ) {
    throw new FlorenceStoreConflict("Initial private Google scan phase is invalid");
  }
  const gmail = jsonRecord(value.gmail as JsonValue);
  const calendar = jsonRecord(value.calendar as JsonValue);
  const outcomes = jsonRecord(value.outcomes as JsonValue);
  const capturedCursor = requiredStringField(gmail, "capturedCursor", "Google scan Gmail cursor");
  const baselinePageToken = nullableScanToken(gmail.baselinePageToken);
  if (typeof gmail.baselineComplete !== "boolean") {
    throw new FlorenceStoreConflict("Initial Gmail baseline state is invalid");
  }
  const finalCursor = nullableScanCursor(gmail.finalCursor);
  const seenPageTokenDigests = scanDigestArray(gmail.seenPageTokenDigests, "Gmail page tokens");
  const seenMessageIdentities = scanIdentityArray(gmail.seenMessageIdentities, "Gmail messages");
  const enumerationPass = calendar.enumerationPass;
  if (!Number.isSafeInteger(enumerationPass) || (enumerationPass as number) < 1) {
    throw new FlorenceStoreConflict("Initial Calendar enumeration pass is invalid");
  }
  if (typeof calendar.finalBarrierStarted !== "boolean") {
    throw new FlorenceStoreConflict("Initial Calendar final barrier state is invalid");
  }
  const finalBarrierStarted = calendar.finalBarrierStarted;
  const targetPageToken = nullableScanToken(calendar.targetPageToken);
  const seenTargetPageTokenDigests = scanDigestArray(
    calendar.seenTargetPageTokenDigests,
    "Calendar target pages",
  );
  const verificationTargetIds = scanStringArray(calendar.verificationTargetIds, "Calendar target IDs");
  if (!Array.isArray(calendar.targets)) {
    throw new FlorenceStoreConflict("Initial Calendar targets are invalid");
  }
  const targets = calendar.targets.map(initialGoogleScanCalendarTarget);
  if (new Set(targets.map((target) => target.calendarId)).size !== targets.length) {
    throw new FlorenceStoreConflict("Initial Calendar scan repeated a target");
  }
  const noEventCoverageValues = calendar.noEventCoverageTargets ?? [];
  if (!Array.isArray(noEventCoverageValues)) {
    throw new FlorenceStoreConflict("Initial no-event Calendar targets are invalid");
  }
  const noEventCoverageTargets = noEventCoverageValues.map(initialGoogleScanNoEventCoverageTarget);
  const allCalendarTargetIds = [
    ...targets.map((target) => target.calendarId),
    ...noEventCoverageTargets.map((target) => target.calendarId),
  ];
  if (new Set(allCalendarTargetIds).size !== allCalendarTargetIds.length) {
    throw new FlorenceStoreConflict("Initial Calendar scan repeated a coverage target");
  }
  if (!Array.isArray(outcomes.findings) || !Array.isArray(outcomes.facts)) {
    throw new FlorenceStoreConflict("Initial Google scan outcomes are invalid");
  }
  const findings = outcomes.findings.map(initialGoogleScanFinding);
  const facts = outcomes.facts.map(initialGoogleScanFact);
  return {
    kind: "initial_private_google_scan_v1",
    version: 1,
    scannerVersion: "complete_private_google_review_v1",
    connectionId,
    anchoredAt,
    gmailAfter,
    calendarTimeMin,
    calendarTimeMax,
    excludedFamilyCalendarId,
    phase,
    gmail: {
      capturedCursor,
      baselinePageToken,
      baselineComplete: gmail.baselineComplete,
      finalCursor,
      seenPageTokenDigests,
      seenMessageIdentities,
    },
    calendar: {
      enumerationPass: enumerationPass as number,
      finalBarrierStarted,
      targetPageToken,
      seenTargetPageTokenDigests,
      verificationTargetIds,
      targets,
      noEventCoverageTargets,
    },
    outcomes: { findings, facts },
  };
}

function initialGoogleScanNoEventCoverageTarget(value: JsonValue): InitialGoogleScanNoEventCoverageTarget {
  const target = jsonRecord(value);
  if (target.accessRole !== "freeBusyReader" || typeof target.primary !== "boolean") {
    throw new FlorenceStoreConflict("Initial no-event Calendar target access is invalid");
  }
  return {
    calendarId: requiredStringField(target, "calendarId", "Initial no-event Calendar target ID"),
    timeZone: requiredStringField(target, "timeZone", "Initial no-event Calendar target time zone"),
    accessRole: "freeBusyReader",
    primary: target.primary,
  };
}

function initialGoogleScanCalendarTarget(value: JsonValue): InitialGoogleScanCalendarTarget {
  const target = jsonRecord(value);
  const accessRole = target.accessRole;
  if (
    accessRole !== "reader" &&
    accessRole !== "writerWithoutPrivateAccess" &&
    accessRole !== "writer" &&
    accessRole !== "owner"
  ) {
    throw new FlorenceStoreConflict("Initial Calendar target access is invalid");
  }
  if (
    typeof target.primary !== "boolean" ||
    typeof target.baselineComplete !== "boolean" ||
    typeof target.replayComplete !== "boolean" ||
    typeof target.manifestComplete !== "boolean"
  ) {
    throw new FlorenceStoreConflict("Initial Calendar target state is invalid");
  }
  return {
    calendarId: requiredStringField(target, "calendarId", "Initial Calendar target ID"),
    timeZone: requiredStringField(target, "timeZone", "Initial Calendar target time zone"),
    accessRole,
    primary: target.primary,
    capturedCursor: requiredStringField(target, "capturedCursor", "Initial Calendar target cursor"),
    baselinePageToken: nullableScanToken(target.baselinePageToken),
    baselineComplete: target.baselineComplete,
    replayComplete: target.replayComplete,
    finalCursor: nullableScanCursor(target.finalCursor),
    manifestPageToken: nullableScanToken(target.manifestPageToken),
    manifestComplete: target.manifestComplete,
    manifestProviderEventIds: scanStringArray(target.manifestProviderEventIds, "Calendar manifest event IDs"),
    seenManifestPageTokenDigests: scanDigestArray(
      target.seenManifestPageTokenDigests,
      "Calendar manifest pages",
    ),
    seenPageTokenDigests: scanDigestArray(target.seenPageTokenDigests, "Calendar event pages"),
    seenEventIdentities: scanIdentityArray(target.seenEventIdentities, "Calendar events"),
  };
}

function initialGoogleScanFinding(value: JsonValue): InitialGoogleScanFinding {
  const finding = jsonRecord(value);
  const familyRelevance = finding.familyRelevance;
  if (!isHouseholdFactRelevance(familyRelevance as FamilyRelevance)) {
    throw new FlorenceStoreConflict("Initial Google scan finding relevance is invalid");
  }
  const retainedFamilyRelevance = familyRelevance as Exclude<FamilyRelevance, "adult_only">;
  const sourceIds = scanSourceIds(finding.sourceIds, "Initial Google scan finding");
  if (typeof finding.surfaceNow !== "boolean") {
    throw new FlorenceStoreConflict("Initial Google scan surface state is invalid");
  }
  const householdCandidateValue = finding.householdCandidate;
  const householdCandidate =
    householdCandidateValue === null
      ? null
      : (() => {
          const record = jsonRecord(householdCandidateValue);
          const dueAt = record.dueAt;
          if (dueAt !== null && typeof dueAt !== "string") {
            throw new FlorenceStoreConflict("Initial Google scan candidate due date is invalid");
          }
          if (typeof record.needsAnswer !== "boolean") {
            throw new FlorenceStoreConflict("Initial Google scan candidate answer state is invalid");
          }
          const stored = privateReviewCandidate(
            deterministicUuid(
              `scan-candidate\0${requiredStringField(finding, "privateSummary", "Google scan summary")}`,
            ),
            {
              category: briefingCategory(record.category),
              summary: requiredStringField(record, "summary", "Google scan candidate summary"),
              urgency: briefingUrgency(record.urgency),
              dueAt,
              needsAnswer: record.needsAnswer,
            },
            sourceIds,
          );
          const { candidateId: _candidateId, sourceIds: _sourceIds, ...candidate } = stored;
          return candidate;
        })();
  const monitor =
    finding.monitor === undefined || finding.monitor === null ? null : scanMonitor(finding.monitor);
  const familyCalendar =
    finding.familyCalendar === undefined || finding.familyCalendar === null
      ? null
      : (finding.familyCalendar as unknown as FamilyCalendarReviewProposal);
  return {
    privateSummary: requiredStringField(finding, "privateSummary", "Initial Google scan finding summary"),
    ...(finding.actionAnchorDigest === undefined || finding.actionAnchorDigest === null
      ? {}
      : {
          actionAnchorDigest: (() => {
            const digest = requiredStringField(
              finding,
              "actionAnchorDigest",
              "Initial Google scan action anchor",
            );
            assertDigest(digest, "Initial Google scan action anchor");
            return digest;
          })(),
        }),
    sourceIds,
    householdCandidate,
    monitor,
    familyCalendar,
    familyRelevance: retainedFamilyRelevance,
    urgency: briefingUrgency(finding.urgency),
    dueAt:
      finding.dueAt === null
        ? null
        : instant(requiredStringField(finding, "dueAt", "Initial Google scan due date")).toISOString(),
    surfaceNow: finding.surfaceNow,
    observedAt: instant(
      requiredStringField(finding, "observedAt", "Initial Google scan observation"),
    ).toISOString(),
  };
}

function initialGoogleScanFact(value: JsonValue): InitialGoogleScanFact {
  const fact = jsonRecord(value);
  const familyRelevance = fact.familyRelevance;
  if (!isHouseholdFactRelevance(familyRelevance as FamilyRelevance)) {
    throw new FlorenceStoreConflict("Initial Google scan fact relevance is invalid");
  }
  const retainedFamilyRelevance = familyRelevance as Exclude<FamilyRelevance, "adult_only">;
  const sourceIds = scanSourceIds(fact.sourceIds, "Initial Google scan fact");
  const observedAt = instant(
    requiredStringField(fact, "observedAt", "Initial Google scan fact observation"),
  ).toISOString();
  if (fact.sourceObservations === undefined) {
    throw new FlorenceStoreConflict("Initial Google scan fact observations are invalid");
  }
  const sourceObservations = scanFactSourceObservations(fact.sourceObservations, sourceIds);
  return {
    slot: requiredStringField(fact, "slot", "Initial Google scan fact slot"),
    statement: requiredStringField(fact, "statement", "Initial Google scan fact statement"),
    familyRelevance: retainedFamilyRelevance,
    sourceIds,
    observedAt,
    sourceObservations,
  };
}

function scanFactSourceObservations(
  value: JsonValue,
  sourceIds: readonly string[],
): { sourceId: string; observedAt: string }[] {
  if (!Array.isArray(value)) {
    throw new FlorenceStoreConflict("Initial Google scan fact observations are invalid");
  }
  const observations = value.map((entry) => {
    const observation = jsonRecord(entry);
    const sourceId = requiredStringField(
      observation,
      "sourceId",
      "Initial Google scan fact observation source",
    );
    assertUuid(sourceId, "Initial Google scan fact observation source");
    return {
      sourceId,
      observedAt: instant(
        requiredStringField(observation, "observedAt", "Initial Google scan fact observation time"),
      ).toISOString(),
    };
  });
  if (
    observations.length !== sourceIds.length ||
    !sameStringSet(
      observations.map(({ sourceId }) => sourceId),
      sourceIds,
    )
  ) {
    throw new FlorenceStoreConflict("Initial Google scan fact observations lost their sources");
  }
  return observations;
}

function scanMonitor(value: JsonValue): InitialFiniteMonitorDraft {
  const monitor = jsonRecord(value);
  return {
    objective: requiredStringField(monitor, "objective", "Initial Google scan monitor objective"),
    currentConclusion: requiredStringField(
      monitor,
      "currentConclusion",
      "Initial Google scan monitor conclusion",
    ),
    endCondition: requiredStringField(monitor, "endCondition", "Initial Google scan monitor end"),
    nextCheck: instant(
      requiredStringField(monitor, "nextCheck", "Initial Google scan monitor check"),
    ).toISOString(),
    why: requiredStringField(monitor, "why", "Initial Google scan monitor reason"),
  };
}

function nullableScanToken(value: JsonValue | undefined): string | null {
  if (value === null) return null;
  if (typeof value !== "string" || !value) {
    throw new FlorenceStoreConflict("Stored Google page token is invalid");
  }
  return value;
}

function nullableScanCursor(value: JsonValue | undefined): string | null {
  if (value === null) return null;
  if (typeof value !== "string" || !value) {
    throw new FlorenceStoreConflict("Stored Google scan cursor is invalid");
  }
  return value;
}

function scanDigestArray(value: JsonValue | undefined, name: string): string[] {
  const values = scanStringArray(value, name);
  for (const digest of values) assertDigest(digest, name);
  return values;
}

function scanIdentityArray(value: JsonValue | undefined, name: string): { key: string; digest: string }[] {
  if (!Array.isArray(value)) throw new FlorenceStoreConflict(`${name} state is invalid`);
  const identities = value.map((item) => {
    const identity = jsonRecord(item);
    const digest = requiredStringField(identity, "digest", `${name} digest`);
    assertDigest(digest, name);
    return { key: requiredStringField(identity, "key", `${name} key`), digest };
  });
  if (new Set(identities.map((identity) => identity.key)).size !== identities.length) {
    throw new FlorenceStoreConflict(`${name} repeated an identity`);
  }
  return identities;
}

function scanStringArray(value: JsonValue | undefined, name: string): string[] {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string" || !item)) {
    throw new FlorenceStoreConflict(`${name} state is invalid`);
  }
  const values = value as string[];
  if (new Set(values).size !== values.length) throw new FlorenceStoreConflict(`${name} state repeated`);
  return [...values];
}

function scanSourceIds(value: JsonValue | undefined, name: string): string[] {
  const values = scanStringArray(value, `${name} sources`);
  if (values.length < 1 || values.length > 10) {
    throw new FlorenceStoreConflict(`${name} needs one to ten sources`);
  }
  for (const sourceId of values) assertUuid(sourceId, `${name} source ID`);
  return values;
}

function assertInitialPrivateGoogleScanContinuation(
  current: InitialPrivateGoogleScanV1,
  next: InitialPrivateGoogleScanV1,
  reclassifiedSourceIds: readonly string[],
): void {
  if (
    current.kind !== next.kind ||
    current.version !== next.version ||
    current.scannerVersion !== next.scannerVersion ||
    current.connectionId !== next.connectionId ||
    current.anchoredAt !== next.anchoredAt ||
    current.gmailAfter !== next.gmailAfter ||
    current.calendarTimeMin !== next.calendarTimeMin ||
    current.calendarTimeMax !== next.calendarTimeMax ||
    current.excludedFamilyCalendarId !== next.excludedFamilyCalendarId ||
    current.gmail.capturedCursor !== next.gmail.capturedCursor
  ) {
    throw new FlorenceStoreConflict("Initial Google scan immutable coverage changed");
  }
  const nextTargetIds = new Set(next.calendar.targets.map((target) => target.calendarId));
  const nextNoEventCoverageTargetIds = new Set(
    next.calendar.noEventCoverageTargets.map((target) => target.calendarId),
  );
  const verifiedRemoval =
    current.phase === "calendar_verify" &&
    next.phase === "ready" &&
    current.calendar.targets
      .filter((target) => !nextTargetIds.has(target.calendarId))
      .every((target) => !next.calendar.verificationTargetIds.includes(target.calendarId));
  if (current.calendar.targets.some((target) => !nextTargetIds.has(target.calendarId)) && !verifiedRemoval) {
    throw new FlorenceStoreConflict("Initial Google scan dropped a Calendar target");
  }
  const verifiedNoEventCoverageRemoval =
    current.phase === "calendar_verify" &&
    next.phase === "ready" &&
    current.calendar.noEventCoverageTargets
      .filter((target) => !nextNoEventCoverageTargetIds.has(target.calendarId))
      .every((target) => !next.calendar.verificationTargetIds.includes(target.calendarId));
  if (
    current.calendar.noEventCoverageTargets.some(
      (target) => !nextNoEventCoverageTargetIds.has(target.calendarId),
    ) &&
    !verifiedNoEventCoverageRemoval
  ) {
    throw new FlorenceStoreConflict("Initial Google scan dropped a no-event Calendar target");
  }
  const nextFindings = new Set(
    next.outcomes.findings.map((finding) =>
      JSON.stringify([finding.privateSummary, [...finding.sourceIds].sort()]),
    ),
  );
  const reclassified = new Set(reclassifiedSourceIds);
  if (
    current.outcomes.findings.some((finding) => {
      const identity = JSON.stringify([finding.privateSummary, [...finding.sourceIds].sort()]);
      return !nextFindings.has(identity) && !finding.sourceIds.some((sourceId) => reclassified.has(sourceId));
    })
  ) {
    throw new FlorenceStoreConflict("Initial Google scan dropped a retained finding");
  }
}

function briefingCategory(value: JsonValue | undefined): SharedBriefingCandidate["category"] {
  if (
    value === "deadline" ||
    value === "conflict" ||
    value === "handoff" ||
    value === "family_date" ||
    value === "loose_end"
  ) {
    return value;
  }
  throw new FlorenceStoreConflict("Stored briefing category is invalid");
}

function briefingUrgency(value: JsonValue | undefined): SharedBriefingCandidate["urgency"] {
  if (value === "now" || value === "soon" || value === "watch") return value;
  throw new FlorenceStoreConflict("Stored briefing urgency is invalid");
}

function requiredStringField(record: Record<string, JsonValue>, key: string, name: string): string {
  const value = record[key];
  if (typeof value !== "string") throw new FlorenceStoreConflict(`${name} is invalid`);
  return required(value, name);
}

function initialBriefingBubbles(
  input: readonly InitialBriefingBubble[],
  name: string,
): InitialBriefingBubble[] {
  if (input.length < 1 || input.length > 3) {
    throw new FlorenceStoreConflict(`The ${name} needs one to three bubbles`);
  }
  return input.map((bubble, index) => {
    const text = required(bubble.text, `${name} bubble ${index + 1}`);
    if (text.length > 2_000) throw new FlorenceStoreConflict(`The ${name} bubble is too long`);
    if (!Number.isSafeInteger(bubble.delayMs) || bubble.delayMs < 0 || bubble.delayMs > 5_000) {
      throw new FlorenceStoreConflict(`The ${name} bubble delay is invalid`);
    }
    return { text, delayMs: bubble.delayMs };
  });
}

function assertPrivateInitialReviewAccounting(
  findings: readonly PrivateReviewFinding[],
  bubbles: readonly InitialBriefingBubble[],
): void {
  const summaries = findings
    .filter((finding) => finding.surfaceNow !== false)
    .map((finding, index) => {
      const summary = required(finding.privateSummary, `Private Google finding ${index + 1} summary`);
      if (summary.length > 2_000 || /[\r\n]/u.test(summary)) {
        throw new FlorenceStoreConflict("A private Google finding summary must fit on one visible line");
      }
      return summary;
    });
  if (new Set(summaries).size !== summaries.length) {
    throw new FlorenceStoreConflict("A private Google review repeated an actionable thread");
  }
  const expected = summaries;
  const visible = initialBriefingBulletItems(bubbles);
  if (expected.length === 0) {
    if (
      bubbles.length !== 1 ||
      bubbles[0]?.text !==
        "I finished reviewing the last 90 days of the Gmail and Calendar details I can access. I don’t have anything to flag right now."
    ) {
      throw new FlorenceStoreConflict(
        "A private Google all-clear must use the complete deterministic output",
      );
    }
    return;
  }
  if (visible.length !== expected.length || visible.some((item, index) => item !== expected[index])) {
    throw new FlorenceStoreConflict("The private Google review output omitted or added a finding or fact");
  }
}

function assertHouseholdInitialBriefingAccounting(
  candidates: readonly StoredBriefingCandidate[],
  bubbles: readonly InitialBriefingBubble[],
): void {
  if (candidates.length === 0) {
    if (
      bubbles.length !== 1 ||
      bubbles[0]?.text !== "I don’t have a household item to flag right now. I’ll keep watching."
    ) {
      throw new FlorenceStoreConflict("A household all-clear must use the complete deterministic output");
    }
    return;
  }
  const expected = candidates.map((candidate) => candidate.summary);
  const visible = initialBriefingBulletItems(bubbles);
  if (visible.length !== expected.length || visible.some((item, index) => item !== expected[index])) {
    throw new FlorenceStoreConflict("The household briefing output omitted or added a distinct finding");
  }
  if (!bubbles.at(-1)?.text.endsWith("Did I get that right? If I missed something, tell me here.")) {
    throw new FlorenceStoreConflict("The household briefing must end with its correction invitation");
  }
}

function initialBriefingBulletItems(bubbles: readonly InitialBriefingBubble[]): string[] {
  return bubbles.flatMap((bubble) =>
    bubble.text.split("\n").flatMap((line) => (line.startsWith("• ") ? [line.slice(2)] : [])),
  );
}

function sameStringSet(left: readonly string[], right: readonly string[]): boolean {
  return (
    left.length === right.length &&
    new Set(left).size === left.length &&
    left.every((value) => right.includes(value))
  );
}

async function requireSteward(
  sql: postgres.TransactionSql,
  householdId: string,
  adultId: string,
): Promise<void> {
  const [actor] = await sql<{ id: string }[]>`
    select id from people where household_id=${householdId} and id=${adultId}
      and kind='adult' and role='steward' and status='verified'
  `;
  if (!actor) throw new FlorenceStoreUnauthorized("A verified household steward must make this change");
}

async function assertSourcesVisible(
  sql: postgres.TransactionSql,
  householdId: string,
  audience: Audience,
  adultId: string,
  sourceIds: readonly string[],
): Promise<void> {
  const ids = unique(sourceIds);
  if (ids.length === 0) throw new FlorenceStoreConflict("At least one source is required");
  const rows = await sql<{ id: string }[]>`
    select id from sources where household_id=${householdId} and id in ${sql(ids)}
      and (visibility='household' or (${audience}='private' and owner_adult_id=${adultId}))
  `;
  if (rows.length !== ids.length) {
    throw new FlorenceStoreUnauthorized("A turn cited a source outside its conversation audience");
  }
}

async function requireSupersededInbound(
  sql: postgres.TransactionSql,
  channelId: string,
  requestedSourceId: string,
  incomingSourceId: string,
): Promise<string> {
  if (requestedSourceId === incomingSourceId) {
    throw new FlorenceStoreConflict("An inbound message cannot supersede itself");
  }
  const [message] = await sql<{ source_id: string }[]>`
    select source_id from messages where source_id=${requestedSourceId} and channel_id=${channelId}
      and direction='inbound' for update
  `;
  if (!message) {
    throw new FlorenceStoreUnauthorized("A message can only supersede an inbound turn in its conversation");
  }
  return message.source_id;
}

async function supersessionRoot(
  sql: postgres.TransactionSql,
  channelId: string,
  currentSourceId: string,
  currentMetadata: JsonValue,
): Promise<string> {
  const seen = new Set([currentSourceId]);
  let rootSourceId = currentSourceId;
  let metadata = currentMetadata;
  for (let depth = 0; depth < 100; depth += 1) {
    const priorSourceId = supersedesSourceId(metadata);
    if (!priorSourceId) return rootSourceId;
    if (seen.has(priorSourceId)) {
      throw new FlorenceStoreConflict("An inbound supersession chain contains a cycle");
    }
    seen.add(priorSourceId);
    const [prior] = await sql<{ source_id: string; metadata: JsonValue }[]>`
      select m.source_id,s.metadata from messages m join sources s on s.id=m.source_id
      where m.source_id=${priorSourceId} and m.channel_id=${channelId} and m.direction='inbound'
      for share of m,s
    `;
    if (!prior) throw new FlorenceStoreConflict("An inbound supersession chain is incomplete");
    rootSourceId = prior.source_id;
    metadata = prior.metadata;
  }
  throw new FlorenceStoreConflict("An inbound supersession chain is too long");
}

async function markInboundSuperseded(
  sql: postgres.TransactionSql,
  priorSourceId: string,
  newerSourceId: string,
  handledAt: Date,
): Promise<void> {
  const [prior] = await sql<{ channel_id: string; metadata: JsonValue }[]>`
    select m.channel_id,s.metadata from messages m join sources s on s.id=m.source_id
    where m.source_id=${priorSourceId} and m.direction='inbound' for update of m
  `;
  if (!prior) throw new FlorenceStoreConflict("The superseded inbound message does not exist");
  const [newer] = await sql<{ metadata: JsonValue }[]>`
    select metadata from sources where id=${newerSourceId} for update
  `;
  if (!newer) throw new FlorenceStoreConflict("The superseding inbound message does not exist");
  const existingPrior = supersedesSourceId(newer.metadata);
  if (existingPrior && existingPrior !== priorSourceId) {
    throw new FlorenceStoreConflict("The superseding inbound message belongs to another turn chain");
  }
  if (!existingPrior) {
    await sql`
      update sources set metadata=metadata||${sql.json({ supersedesSourceId: priorSourceId })}
      where id=${newerSourceId}
    `;
  }
  await sql`
    update messages set status='handled',handled_at=${handledAt},retry_at=null,
      last_error='Superseded by a newer message in this conversation'
    where source_id=${priorSourceId} and direction='inbound' and status='received'
  `;
  const finalTurnId = deterministicUuid(`turn\0${priorSourceId}`);
  const rootSourceId = await supersessionRoot(sql, prior.channel_id, priorSourceId, prior.metadata);
  const cueTurnId = deterministicUuid(`cue-turn\0${rootSourceId}`);
  const calendarId = deterministicUuid(`calendar\0${priorSourceId}`);
  await sql`
    update messages set status='failed',sending_at=null,retry_at=null,
      last_error='Superseded before delivery by a newer message in this conversation'
    where direction='outbound' and status in ('pending','sending')
      and turn_id in (${finalTurnId},${cueTurnId})
  `;
  await sql`
    delete from calendar_actions action where action.status='offered'
      and (action.id=${calendarId} or action.basis_source_id in (
          select source.id from sources source
          where source.id=${priorSourceId} or source.parent_source_id=${priorSourceId}
        ))
  `;
  await sql`
    update calendar_actions set status='failed',retry_at=${handledAt},
      last_error='Superseded before provider execution by a newer message in this conversation'
    where status='pending' and approval_source_id=${priorSourceId}
  `;
  await sql`
    update people set invitation_approval_source_id=null,invitation_approved_at=null,
      invitation_retry_at=null,invitation_last_error=null,updated_at=${handledAt}
    where invitation_issued_at is null and invitation_approval_source_id=${priorSourceId}
  `;
}

function supersedesSourceId(metadata: JsonValue): string | null {
  return supersessionMetadataId(metadata, "supersedesSourceId");
}

function supersessionMetadataId(metadata: JsonValue, key: string): string | null {
  const value = jsonRecord(metadata)[key];
  if (value === undefined) return null;
  if (typeof value !== "string") {
    throw new FlorenceStoreConflict("Stored inbound supersession metadata is invalid");
  }
  assertUuid(value, "Stored inbound supersession source ID");
  return value;
}

async function stopMessagesChannel(
  sql: postgres.TransactionSql,
  channelId: string,
  stoppedAt: Date,
): Promise<void> {
  const stopped = await sql`
    update linq_channels set stopped_at=coalesce(stopped_at,${stoppedAt})
    where id=${channelId} and revoked_at is null returning id
  `;
  if (stopped.length !== 1) {
    throw new FlorenceStoreConflict("The Messages channel is no longer active");
  }
  await sql`
    update messages set status='failed',sending_at=null,retry_at=null,
      last_error='Messages were stopped by this adult'
    where channel_id=${channelId} and direction='outbound' and status='pending'
  `;
  await sql`
    update people partner set invitation_approval_source_id=null,invitation_approved_at=null,
      invitation_retry_at=null,invitation_last_error=null,updated_at=${stoppedAt}
    from messages approval
    where approval.source_id=partner.invitation_approval_source_id
      and approval.channel_id=${channelId} and partner.invitation_issued_at is null
  `;
}

async function readExistingInboundDuplicate(
  sql: postgres.TransactionSql,
  channel: ChannelRow,
  input: AcceptInboundInput,
  sourceId: string,
  authoredText: string | null,
  voiceTranscriptPresent: boolean,
  images: readonly ImageReference[],
  documents: readonly ValidatedInboundDocument[],
): Promise<AcceptInboundResult | null> {
  const [existing] = await sql<
    {
      source_id: string;
      provider_message_id: string;
      channel_id: string;
      text: string | null;
      images: JsonValue;
      metadata: JsonValue;
    }[]
  >`
    select m.source_id,m.provider_message_id,m.channel_id,m.text,m.images,s.metadata
    from messages m join sources s on s.id=m.source_id
    where m.provider_event_id=${input.providerEventId} limit 1
  `;
  if (!existing) return null;
  const storedPayloadDigest = jsonRecord(existing.metadata).providerPayloadDigest;
  const storedAuthorship = conversationAuthorship(existing.metadata);
  const existingDocuments = await sql<
    {
      document_id: string;
      external_key: string;
      filename: string;
      mime_type: string;
      content_digest: string;
      retained: boolean;
      discard_after: Date | null;
    }[]
  >`
    select s.id as document_id,s.external_key,d.filename,d.mime_type,d.content_digest,
           d.retained,d.discard_after
    from sources s join documents d on d.source_id=s.id
    where s.parent_source_id=${sourceId} order by s.id
  `;
  if (
    existing.source_id !== sourceId ||
    existing.provider_message_id !== input.providerMessageId ||
    existing.channel_id !== channel.id ||
    (input.providerPayloadDigest !== undefined &&
      storedPayloadDigest !== undefined &&
      storedPayloadDigest !== input.providerPayloadDigest) ||
    storedAuthorship.authoredText !== authoredText ||
    storedAuthorship.voiceTranscriptPresent !== voiceTranscriptPresent ||
    existing.text !== input.text ||
    !sameImageReferences(imageReferences(existing.images), images) ||
    !sameInboundDocuments(existingDocuments, documents)
  ) {
    throw new FlorenceStoreConflict("A Linq event ID was reused with different content");
  }
  return {
    disposition: "duplicate",
    sourceId,
    householdId: channel.household_id,
    channelId: channel.id,
  };
}

async function insertInbound(
  sql: postgres.TransactionSql,
  channel: ChannelRow,
  senderAdultId: string,
  input: AcceptInboundInput,
): Promise<AcceptInboundResult> {
  if (
    input.audience !== channel.audience ||
    !sameStrings(channelIdentityDigests(channel), sortedDigests(input.participantIdentityDigests))
  ) {
    throw new FlorenceStoreUnauthorized("Current Linq participants do not match the bound channel");
  }
  const expectedSenderDigest =
    channel.adult_one_id === senderAdultId ? channel.identity_one_digest : channel.identity_two_digest;
  if (expectedSenderDigest !== input.senderIdentitySubjectDigest) {
    throw new FlorenceStoreUnauthorized("The Linq sender is not a bound household adult");
  }
  if (input.providerPayloadDigest !== undefined) {
    assertDigest(input.providerPayloadDigest, "Linq provider payload");
  }
  const authoredText = input.authoredText === undefined ? input.text : input.authoredText;
  const voiceTranscriptPresent = input.voiceTranscriptPresent ?? false;
  if (authoredText !== null && typeof authoredText !== "string") {
    throw new FlorenceStoreConflict("Authored Messages text must be text or null");
  }
  if (typeof voiceTranscriptPresent !== "boolean") {
    throw new FlorenceStoreConflict("Voice transcript presence must be true or false");
  }
  const images = validateImageReferences(input.images ?? []);
  const occurredAt = instant(input.occurredAt);
  const documents = validateInboundDocuments(input.documents ?? [], occurredAt);
  const sourceId = deterministicUuid(`linq-v3\0signal\0${input.providerEventId}`);
  let requestedSupersedesSourceId = input.supersedesSourceId ?? null;
  if (requestedSupersedesSourceId) {
    assertUuid(requestedSupersedesSourceId, "Superseded inbound source ID");
  }
  const duplicate = await readExistingInboundDuplicate(
    sql,
    channel,
    input,
    sourceId,
    authoredText,
    voiceTranscriptPresent,
    images,
    documents,
  );
  if (duplicate) return duplicate;
  const stop = isCarrierMessagesOptOut(input.text);
  if (channel.stopped_at && !stop) {
    return {
      disposition: "stopped",
      sourceId,
      householdId: channel.household_id,
      channelId: channel.id,
    };
  }
  if (input.text === null && images.length === 0 && documents.length === 0) {
    throw new FlorenceStoreConflict("An inbound message needs text, an image, or a document");
  }
  const [nearestLater] = !stop
    ? await sql<{ source_id: string }[]>`
        select later.source_id from messages later
        join sources later_source on later_source.id=later.source_id
        where later.channel_id=${channel.id} and later.direction='inbound'
          and later.move_kind in ('message','reply')
          and (later_source.occurred_at,later_source.id) > (${occurredAt},${sourceId}::uuid)
        order by later_source.occurred_at,later_source.id limit 1
        for update of later,later_source
      `
    : [];
  if (!nearestLater) {
    const [latestEarlierReceived] = await sql<{ source_id: string }[]>`
      select earlier.source_id from messages earlier
      join sources earlier_source on earlier_source.id=earlier.source_id
      where earlier.channel_id=${channel.id} and earlier.direction='inbound'
        and earlier.move_kind in ('message','reply') and earlier.status='received'
        and (earlier_source.occurred_at,earlier_source.id) < (${occurredAt},${sourceId}::uuid)
      order by earlier_source.occurred_at desc,earlier_source.id desc limit 1
      for update of earlier,earlier_source
    `;
    requestedSupersedesSourceId = latestEarlierReceived?.source_id ?? requestedSupersedesSourceId;
  }
  if (!nearestLater && !requestedSupersedesSourceId) {
    const [pendingTurn] = await sql<{ source_id: string }[]>`
      select inbound.source_id
      from messages pending
      join sources pending_source on pending_source.id=pending.source_id
      join messages inbound on inbound.source_id=pending_source.parent_source_id
      join sources inbound_source on inbound_source.id=inbound.source_id
      where pending.channel_id=${channel.id} and pending.direction='outbound' and pending.status='pending'
        and inbound.direction='inbound' and inbound.move_kind in ('message','reply')
      order by inbound_source.occurred_at desc,inbound.source_id desc limit 1
      for update of inbound,inbound_source
    `;
    requestedSupersedesSourceId = pendingTurn?.source_id ?? null;
  }
  if (!nearestLater && !requestedSupersedesSourceId) {
    const [pendingCalendarTurn] = await sql<{ source_id: string }[]>`
      select inbound.source_id
      from calendar_actions action
      join messages inbound on inbound.source_id=action.approval_source_id
      join sources inbound_source on inbound_source.id=inbound.source_id
      where action.status='pending' and inbound.channel_id=${channel.id} and inbound.direction='inbound'
        and inbound.move_kind in ('message','reply')
      order by inbound_source.occurred_at desc,inbound.source_id desc limit 1
      for update of action,inbound,inbound_source
    `;
    requestedSupersedesSourceId = pendingCalendarTurn?.source_id ?? null;
  }
  if (requestedSupersedesSourceId === sourceId) {
    throw new FlorenceStoreConflict("An inbound message cannot supersede itself");
  }
  const supersededSourceId =
    !nearestLater && requestedSupersedesSourceId
      ? await requireSupersededInbound(sql, channel.id, requestedSupersedesSourceId, sourceId)
      : null;
  const visibility: Visibility = channel.audience === "group" ? "household" : "private";
  const ownerAdultId = visibility === "private" ? senderAdultId : null;
  const [reply] = input.replyToProviderMessageId
    ? await sql<{ source_id: string }[]>`
        select source_id from messages where channel_id=${channel.id}
          and provider_message_id=${input.replyToProviderMessageId} limit 1
      `
    : [];
  const insertedSource = await sql<{ id: string }[]>`
    insert into sources (id,household_id,kind,visibility,owner_adult_id,external_key,label,metadata,occurred_at)
    values (${sourceId},${channel.household_id},'linq_message',${visibility},${ownerAdultId},
      ${`inbound:${input.providerEventId}`},${bounded(input.text ?? "Family attachment", 500)},
      ${sql.json({
        providerMessageId: input.providerMessageId,
        authoredText,
        voiceTranscriptPresent,
        ...(input.providerPayloadDigest ? { providerPayloadDigest: input.providerPayloadDigest } : {}),
        ...(supersededSourceId ? { supersedesSourceId: supersededSourceId } : {}),
      })},${occurredAt})
    on conflict do nothing returning id
  `;
  if (insertedSource.length === 0) {
    const concurrentDuplicate = await readExistingInboundDuplicate(
      sql,
      channel,
      input,
      sourceId,
      authoredText,
      voiceTranscriptPresent,
      images,
      documents,
    );
    if (concurrentDuplicate) return concurrentDuplicate;
    throw new FlorenceStoreConflict("A Linq event conflicts with existing source data");
  }
  await sql`
    insert into messages (
      source_id,channel_id,direction,sender_adult_id,move_kind,text,images,has_attachments,
      provider_event_id,provider_message_id,reply_to_source_id,turn_id,turn_part,not_before,status,
      handled_at,last_error
    ) values (${sourceId},${channel.id},'inbound',${senderAdultId},'message',${input.text},
      ${sql.json(images)},${images.length > 0 || documents.length > 0},
      ${input.providerEventId},${input.providerMessageId},
      ${reply?.source_id ?? null},${sourceId},0,${occurredAt},${stop || nearestLater ? "handled" : "received"},
      ${stop || nearestLater ? occurredAt : null},
      ${nearestLater ? "Superseded by a newer message in this conversation" : null})
  `;
  for (const document of documents) {
    const discardAfter = instant(document.discardAfter);
    await sql`
      insert into sources (
        id,household_id,kind,visibility,owner_adult_id,external_key,parent_source_id,label,metadata,occurred_at
      ) values (${document.documentId},${channel.household_id},'document',${visibility},${ownerAdultId},
        ${document.externalKey},${sourceId},${required(document.filename, "Document filename")},
        ${sql.json({ mimeType: document.mimeType })},${occurredAt})
    `;
    await sql`
      insert into documents (
        source_id,saved_by_adult_id,filename,mime_type,content_digest,retained,content_envelope,discard_after
      ) values (${document.documentId},${senderAdultId},${document.filename},${document.mimeType},
        ${document.contentDigest},false,${Buffer.from(document.contentEnvelope)},${discardAfter})
    `;
  }
  if (supersededSourceId) {
    await markInboundSuperseded(sql, supersededSourceId, sourceId, occurredAt);
  }
  if (stop) await stopMessagesChannel(sql, channel.id, occurredAt);
  return {
    disposition: stop ? "stopped" : "accepted",
    sourceId,
    householdId: channel.household_id,
    channelId: channel.id,
  };
}

async function insertInboundReaction(
  sql: postgres.TransactionSql,
  channel: ChannelRow,
  senderAdultId: string,
  input: AcceptInboundReactionInput,
): Promise<AcceptInboundResult | null> {
  if (
    input.audience !== channel.audience ||
    !sameStrings(channelIdentityDigests(channel), sortedDigests(input.participantIdentityDigests))
  ) {
    throw new FlorenceStoreUnauthorized("Current Linq participants do not match the bound channel");
  }
  const expectedSenderDigest =
    channel.adult_one_id === senderAdultId ? channel.identity_one_digest : channel.identity_two_digest;
  if (expectedSenderDigest !== input.senderIdentitySubjectDigest) {
    throw new FlorenceStoreUnauthorized("The Linq reaction sender is not a bound household adult");
  }
  const providerEventId = required(input.providerEventId, "Linq reaction event ID");
  const targetProviderMessageId = required(input.targetProviderMessageId, "Linq reaction target message ID");
  const reaction = required(input.reaction, "Linq reaction");
  if (reaction.length > 500) throw new FlorenceStoreConflict("A Linq reaction is too long");
  if (!Number.isSafeInteger(input.partIndex) || input.partIndex < 0) {
    throw new FlorenceStoreConflict("A Linq reaction part index is invalid");
  }
  const occurredAt = instant(input.occurredAt);
  const sourceId = deterministicUuid(`linq-v3\0signal\0${providerEventId}`);
  const providerMessageId = `inbound-reaction:${deterministicUuid(
    `linq-v3\0reaction-message\0${providerEventId}`,
  )}`;
  const [existing] = await sql<
    {
      source_id: string;
      channel_id: string;
      sender_adult_id: string;
      move_kind: "message" | "reply" | "reaction";
      text: string | null;
      reaction: string | null;
      provider_message_id: string;
      reply_to_source_id: string | null;
      turn_id: string;
      turn_part: number;
      metadata: JsonObject;
      occurred_at: Date;
    }[]
  >`
    select m.source_id,m.channel_id,m.sender_adult_id,m.move_kind,m.text,m.reaction,
      m.provider_message_id,m.reply_to_source_id,m.turn_id,m.turn_part,s.metadata,s.occurred_at
    from messages m join sources s on s.id=m.source_id
    where m.provider_event_id=${providerEventId} limit 1
  `;
  const [target] = await sql<
    {
      source_id: string;
      direction: "inbound" | "outbound";
      move_kind: "message" | "reply" | "reaction";
      status: "received" | "handled" | "pending" | "sending" | "sent" | "failed";
    }[]
  >`
    select source_id,direction,move_kind,status from messages where channel_id=${channel.id}
      and provider_message_id=${targetProviderMessageId} limit 1 for share
  `;
  const florenceTarget =
    target?.direction === "outbound" && (target.move_kind === "message" || target.move_kind === "reply");
  if (existing) {
    const metadata = jsonRecord(existing.metadata);
    if (
      !target ||
      !florenceTarget ||
      existing.source_id !== sourceId ||
      existing.channel_id !== channel.id ||
      existing.sender_adult_id !== senderAdultId ||
      existing.move_kind !== "reaction" ||
      existing.text !== null ||
      existing.reaction !== reaction ||
      existing.provider_message_id !== providerMessageId ||
      existing.reply_to_source_id !== target.source_id ||
      existing.turn_id !== sourceId ||
      existing.turn_part !== -1 ||
      existing.occurred_at.getTime() !== occurredAt.getTime() ||
      metadata.targetProviderMessageId !== targetProviderMessageId ||
      metadata.partIndex !== input.partIndex
    ) {
      throw new FlorenceStoreConflict("A Linq reaction event ID was reused with different content");
    }
    return {
      disposition: "duplicate",
      sourceId,
      householdId: channel.household_id,
      channelId: channel.id,
    };
  }
  if (channel.stopped_at) {
    return {
      disposition: "stopped",
      sourceId,
      householdId: channel.household_id,
      channelId: channel.id,
    };
  }
  if (!target || !florenceTarget || target.status !== "sent") return null;
  const visibility: Visibility = channel.audience === "group" ? "household" : "private";
  const ownerAdultId = visibility === "private" ? senderAdultId : null;
  await sql`
    insert into sources (id,household_id,kind,visibility,owner_adult_id,external_key,label,metadata,occurred_at)
    values (${sourceId},${channel.household_id},'linq_message',${visibility},${ownerAdultId},
      ${`inbound:${providerEventId}`},${bounded(`Reacted ${reaction}`, 500)},
      ${sql.json({
        providerMessageId,
        targetProviderMessageId,
        partIndex: input.partIndex,
        authoredText: null,
        voiceTranscriptPresent: false,
      })},${occurredAt})
  `;
  await sql`
    insert into messages (
      source_id,channel_id,direction,sender_adult_id,move_kind,text,reaction,images,has_attachments,
      provider_event_id,provider_message_id,reply_to_source_id,turn_id,turn_part,not_before,status
    ) values (${sourceId},${channel.id},'inbound',${senderAdultId},'reaction',null,${reaction},'[]'::jsonb,
      false,${providerEventId},${providerMessageId},${target.source_id},${sourceId},-1,${occurredAt},'received')
  `;
  return {
    disposition: "accepted",
    sourceId,
    householdId: channel.household_id,
    channelId: channel.id,
  };
}

export function isCarrierMessagesOptOut(text: string | null | undefined): boolean {
  return /^(?:STOP|UNSUBSCRIBE|QUIT|END|CANCEL)$/i.test(text?.trim() ?? "");
}

async function insertOutbound(sql: postgres.TransactionSql, input: OutboundInsert): Promise<void> {
  if (input.moveKind === "reaction") {
    if (input.turnPart !== -1 || !input.reaction || input.text) {
      throw new FlorenceStoreConflict("A reaction uses turn part -1 and contains only a reaction");
    }
  } else if (input.turnPart < 0 || !input.text?.trim()) {
    throw new FlorenceStoreConflict("A message bubble needs text and turn part 0 through 2");
  }
  const idempotencyKey = await householdLinqIdempotencyKey(sql, input.householdId, input.idempotencyKey);
  const [existing] = await sql<{ source_id: string }[]>`
    select source_id from messages where idempotency_key=${idempotencyKey}
  `;
  if (existing) {
    if (existing.source_id !== input.sourceId) {
      throw new FlorenceStoreConflict("A Linq idempotency key was reused for another message");
    }
    return;
  }
  await sql`
    insert into sources (
      id,household_id,kind,visibility,owner_adult_id,external_key,parent_source_id,label,metadata,occurred_at
    )
    values (${input.sourceId},${input.householdId},'linq_message',${input.visibility},${input.ownerAdultId},
      ${`outbound:${idempotencyKey}`},${input.parentSourceId ?? null},
      ${bounded(input.text ?? input.reaction ?? "Florence response", 500)},
      ${sql.json({
        ...(input.metadata ?? {}),
        authoredText: input.moveKind === "reaction" ? null : (input.text ?? null),
        voiceTranscriptPresent: false,
      })},${input.occurredAt})
  `;
  await sql`
    insert into messages (
      source_id,channel_id,direction,move_kind,text,reaction,reply_to_source_id,turn_id,turn_part,
      idempotency_key,not_before,status
    ) values (${input.sourceId},${input.channelId},'outbound',${input.moveKind},${input.text ?? null},
      ${input.reaction ?? null},${input.replyToSourceId ?? null},${input.turnId},${input.turnPart},
      ${idempotencyKey},${instant(input.notBefore)},'pending')
  `;
}

function imageReferences(value: JsonValue): ImageReference[] {
  if (!Array.isArray(value)) throw new FlorenceStoreConflict("Stored message images are invalid");
  return validateImageReferences(
    value.map((item) => {
      if (!isRecord(item) || typeof item.assetId !== "string" || typeof item.mimeType !== "string") {
        throw new FlorenceStoreConflict("Stored message images are invalid");
      }
      return { assetId: item.assetId, mimeType: item.mimeType as ImageReference["mimeType"] };
    }),
  );
}

function validateImageReferences(values: readonly ImageReference[]): ImageReference[] {
  if (values.length > 10) throw new FlorenceStoreConflict("A message can contain at most ten images");
  const result: ImageReference[] = [];
  const assetIds = new Set<string>();
  for (const value of values) {
    assertUuid(value.assetId, "Image asset ID");
    if (!isImageMimeType(value.mimeType)) throw new FlorenceStoreConflict("An image MIME type is invalid");
    if (assetIds.has(value.assetId))
      throw new FlorenceStoreConflict("An image cannot appear twice in one message");
    assetIds.add(value.assetId);
    result.push({ assetId: value.assetId, mimeType: value.mimeType });
  }
  return result;
}

type ValidatedInboundDocument = Omit<InboundDocumentInput, "discardAfter"> & {
  discardAfter: string;
};

function validateInboundDocuments(
  values: readonly InboundDocumentInput[],
  occurredAt: Date,
): ValidatedInboundDocument[] {
  if (values.length > MAX_CURRENT_PDFS) {
    throw new FlorenceStoreConflict("A message can contain at most three PDFs");
  }
  const documentIds = new Set<string>();
  const externalKeys = new Set<string>();
  const documents = values.map((document) => {
    assertUuid(document.documentId, "Document ID");
    if (documentIds.has(document.documentId)) {
      throw new FlorenceStoreConflict("A document cannot appear twice in one message");
    }
    documentIds.add(document.documentId);
    const externalKey = required(document.externalKey, "Document provider key");
    if (externalKeys.has(externalKey)) {
      throw new FlorenceStoreConflict("A document provider key cannot appear twice in one message");
    }
    externalKeys.add(externalKey);
    if (!document.filename.trim() || document.filename.length > 500) {
      throw new FlorenceStoreConflict("A document filename is invalid");
    }
    if (document.mimeType !== "application/pdf") {
      throw new FlorenceStoreConflict("An inbound document must be a PDF");
    }
    assertDigest(document.contentDigest, "Document content");
    if (
      document.contentEnvelope.byteLength < 1 ||
      document.contentEnvelope.byteLength > MAX_PDF_ENVELOPE_BYTES
    ) {
      throw new FlorenceStoreConflict("A PDF envelope exceeds its storage limit");
    }
    const discardAfter = instant(
      document.discardAfter ?? new Date(occurredAt.getTime() + 24 * 60 * 60_000).toISOString(),
    );
    if (discardAfter <= occurredAt) {
      throw new FlorenceStoreConflict("An inbound PDF discard time must follow its message");
    }
    return {
      documentId: document.documentId,
      externalKey,
      filename: document.filename,
      mimeType: "application/pdf" as const,
      contentDigest: document.contentDigest,
      contentEnvelope: document.contentEnvelope,
      discardAfter: discardAfter.toISOString(),
    };
  });
  return documents.sort((left, right) => left.documentId.localeCompare(right.documentId));
}

function sameInboundDocuments(
  existing: readonly {
    document_id: string;
    external_key: string;
    filename: string;
    mime_type: string;
    content_digest: string;
    retained: boolean;
    discard_after: Date | null;
  }[],
  received: readonly ValidatedInboundDocument[],
): boolean {
  return (
    existing.length === received.length &&
    existing.every((document, index) => {
      const candidate = received[index];
      return (
        candidate !== undefined &&
        document.document_id === candidate.documentId &&
        document.external_key === candidate.externalKey &&
        document.filename === candidate.filename &&
        document.mime_type === candidate.mimeType &&
        document.content_digest === candidate.contentDigest &&
        document.retained === false &&
        document.discard_after?.toISOString() === candidate.discardAfter
      );
    })
  );
}

type LinqProviderState =
  | "accepted"
  | "sent"
  | "delivered"
  | "read"
  | "failed"
  | "reaction_added"
  | "reaction_removed";

type LinqProviderTruth = {
  state: LinqProviderState;
  occurredAt: string;
  receipt: Record<string, JsonValue>;
};

const LINQ_OBSERVATION_SLOTS = [
  "messageSent",
  "messageDelivered",
  "messageRead",
  "messageFailed",
  "reactionAdded",
  "reactionRemoved",
] as const;

function mergeLinqAcceptance(
  current: JsonValue | null,
  providerReceiptId: string,
  occurredAt: string,
  detail: JsonObject,
): JsonObject {
  const existing = jsonRecord(current);
  const requestedState = linqProviderState(detail.providerState) ?? "accepted";
  const existingTruth = latestLinqTruth(existing);
  const incomingTruth =
    requestedState === "accepted" ? null : { state: requestedState, occurredAt, receipt: jsonRecord(detail) };
  const truth = newestLinqTruth(existingTruth, incomingTruth);
  return {
    ...existing,
    ...detail,
    provider: "linq-v3",
    acceptance: { providerReceiptId, occurredAt },
    providerState: truth?.state ?? requestedState,
    providerStateAt: truth?.occurredAt ?? occurredAt,
  };
}

function mergeLinqObservation(
  current: JsonValue | null,
  input: LinqOutboundObservation,
): {
  detail: JsonObject;
  providerState: LinqProviderState;
  duplicate: boolean;
  lastError: string | null;
} {
  const existing = jsonRecord(current);
  const observations = jsonRecord(existing.observations);
  const slot =
    input.kind === "reaction"
      ? input.operation === "added"
        ? "reactionAdded"
        : "reactionRemoved"
      : input.status === "sent"
        ? "messageSent"
        : input.status === "delivered"
          ? "messageDelivered"
          : input.status === "read"
            ? "messageRead"
            : "messageFailed";
  const state: LinqProviderState =
    input.kind === "reaction"
      ? input.operation === "added"
        ? "reaction_added"
        : "reaction_removed"
      : input.status;
  const prior = jsonRecord(observations[slot]);
  const priorEventId = prior.providerEventId;
  const priorOccurredAt = prior.occurredAt;
  const duplicate =
    priorEventId === input.providerEventId ||
    (typeof priorOccurredAt === "string" && Date.parse(priorOccurredAt) >= Date.parse(input.occurredAt));
  if (!duplicate) {
    observations[slot] =
      input.kind === "reaction"
        ? {
            providerEventId: input.providerEventId,
            traceId: input.traceId,
            occurredAt: input.occurredAt,
            state,
            targetProviderMessageId: input.targetProviderMessageId,
            reaction: input.reaction,
            partIndex: input.partIndex,
          }
        : {
            providerEventId: input.providerEventId,
            traceId: input.traceId,
            occurredAt: input.occurredAt,
            state,
            providerMessageId: input.providerMessageId,
            idempotencyKey: input.idempotencyKey,
            ...(input.failure ? { failure: input.failure } : {}),
          };
  }
  const detail: JsonObject = { ...existing, provider: "linq-v3", observations };
  const truth = latestLinqTruth(detail);
  if (!truth || truth.state === "accepted") {
    throw new FlorenceStoreConflict("The Linq observation has no provider delivery state");
  }
  const merged: JsonObject = {
    ...detail,
    providerState: truth.state,
    providerStateAt: truth.occurredAt,
  };
  return {
    detail: merged,
    providerState: truth.state,
    duplicate,
    lastError: linqTruthError(truth),
  };
}

function latestLinqTruth(detail: Record<string, JsonValue>): LinqProviderTruth | null {
  const state = linqProviderState(detail.providerState);
  const occurredAt = detail.providerStateAt;
  let latest =
    state && typeof occurredAt === "string" && !Number.isNaN(Date.parse(occurredAt))
      ? { state, occurredAt, receipt: detail }
      : null;
  const observations = jsonRecord(detail.observations);
  for (const slot of LINQ_OBSERVATION_SLOTS) {
    const receipt = jsonRecord(observations[slot]);
    const observedState = linqProviderState(receipt.state);
    const observedAt = receipt.occurredAt;
    if (!observedState || typeof observedAt !== "string" || Number.isNaN(Date.parse(observedAt))) {
      continue;
    }
    latest = newestLinqTruth(latest, { state: observedState, occurredAt: observedAt, receipt });
  }
  return latest;
}

function newestLinqTruth(
  left: LinqProviderTruth | null,
  right: LinqProviderTruth | null,
): LinqProviderTruth | null {
  if (!left) return right;
  if (!right) return left;
  const timeDifference = Date.parse(right.occurredAt) - Date.parse(left.occurredAt);
  if (timeDifference !== 0) return timeDifference > 0 ? right : left;
  return linqStatePriority(right.state) >= linqStatePriority(left.state) ? right : left;
}

function linqProviderState(value: JsonValue | undefined): LinqProviderState | null {
  return value === "accepted" ||
    value === "sent" ||
    value === "delivered" ||
    value === "read" ||
    value === "failed" ||
    value === "reaction_added" ||
    value === "reaction_removed"
    ? value
    : null;
}

function linqStatePriority(state: LinqProviderState): number {
  if (state === "read") return 7;
  if (state === "delivered" || state === "reaction_removed") return 6;
  if (state === "sent" || state === "reaction_added") return 5;
  if (state === "failed") return 4;
  return 0;
}

function linqTruthError(truth: LinqProviderTruth): string | null {
  if (truth.state === "reaction_removed") return "Linq reaction was removed";
  if (truth.state !== "failed") return null;
  const failure = jsonRecord(truth.receipt.failure);
  const reason = failure.reason;
  const code = failure.code;
  if (typeof reason === "string" && reason.trim()) return bounded(reason, 2_000);
  return typeof code === "number" ? `Linq delivery failed (${code})` : "Linq delivery failed";
}

const EDITABLE_MEMBER_PROFILE_KEYS = new Set(["age", "grade", "school", "activities", "postalCode"]);

function defaultStoredRelationship(member: PersonRow): string {
  if (member.kind === "child") return "Child";
  if (member.adult_slot === 2) return "Partner";
  return member.role === "steward" ? "Parent" : "Caregiver";
}

function memberDisplayName(firstName: string, lastName: string | null): string {
  if (!lastName) return boundedCharacters(firstName, 160);
  const separatorBudget = 1;
  const available = 160 - separatorBudget;
  let firstBudget = Math.min([...firstName].length, Math.floor(available / 2));
  const lastBudget = Math.min([...lastName].length, available - firstBudget);
  firstBudget = Math.min([...firstName].length, available - lastBudget);
  return `${boundedCharacters(firstName, firstBudget)} ${boundedCharacters(lastName, lastBudget)}`;
}

function familyLabelFromSurnames(founderSurname: string, partnerSurname: string | null): string {
  const founder = required(founderSurname, "Founding adult last name");
  const partner = partnerSurname ? required(partnerSurname, "Partner last name") : null;
  if (!partner || founder.localeCompare(partner, "en-US", { sensitivity: "accent" }) === 0) {
    return `${boundedCharacters(founder, 153)} Family`;
  }
  const available = 152;
  let founderBudget = Math.min([...founder].length, Math.floor(available / 2));
  const partnerBudget = Math.min([...partner].length, available - founderBudget);
  founderBudget = Math.min([...founder].length, available - partnerBudget);
  return `${boundedCharacters(founder, founderBudget)}–${boundedCharacters(partner, partnerBudget)} Family`;
}

async function updateHouseholdNameFromLockedAdults(
  sql: postgres.TransactionSql,
  householdId: string,
  occurredAt: Date,
): Promise<string> {
  const adults = await sql<{ adult_slot: 1 | 2; last_name: string | null }[]>`
    select adult_slot,profile->>'lastName' as last_name from people
    where household_id=${householdId} and kind='adult' and adult_slot in (1,2)
    order by adult_slot for update
  `;
  const founderSurname = adults.find((adult) => adult.adult_slot === 1)?.last_name;
  if (!founderSurname) {
    throw new FlorenceStoreConflict("The founding adult needs a last name");
  }
  const partnerSurname = adults.find((adult) => adult.adult_slot === 2)?.last_name ?? null;
  const familyLabel = familyLabelFromSurnames(founderSurname, partnerSurname);
  await sql`
    update households set name=${familyLabel},updated_at=${occurredAt} where id=${householdId}
  `;
  return familyLabel;
}

function boundedCharacters(value: string, limit: number): string {
  return [...value].slice(0, limit).join("");
}

function applyEditableMemberProfilePatch(current: JsonObject, patch: JsonObject): JsonObject {
  const next: Record<string, JsonValue> = { ...current };
  for (const [key, value] of Object.entries(patch)) {
    if (!EDITABLE_MEMBER_PROFILE_KEYS.has(key)) {
      throw new FlorenceStoreUnauthorized(`The ${key} profile field cannot be edited here`);
    }
    if (key === "postalCode" && (typeof value !== "string" || !/^\d{5}(?:-\d{4})?$/.test(value))) {
      throw new FlorenceStoreConflict("Home ZIP must be five digits, with an optional four-digit suffix");
    }
    if (value === null) {
      delete next[key];
      continue;
    }
    if (key === "age") next[key] = validChildAge(value, "Child age");
    else if (key === "grade") next[key] = validChildGrade(value, "Child grade");
    else next[key] = value;
  }
  return next;
}

function validChildAge(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0 || value > 120) {
    throw new FlorenceStoreConflict(`${label} must be a whole number from zero to 120`);
  }
  return value;
}

function validChildGrade(value: unknown, label: string): string {
  if (typeof value !== "string") {
    throw new FlorenceStoreConflict(`${label} must be text`);
  }
  const grade = value.trim();
  if (!grade || grade.length > 80) {
    throw new FlorenceStoreConflict(`${label} must be between one and eighty characters`);
  }
  return grade;
}

function validatedPublicPreferencePatch(preferences: JsonObject): JsonObject {
  const allowed = new Set([
    "proactiveGoogleEnabled",
    "automaticFamilyCalendarEnabled",
    "privateConflictBusySharingEnabled",
  ]);
  const entries = Object.entries(preferences);
  if (entries.length === 0) {
    throw new FlorenceStoreConflict("At least one preference is required");
  }
  for (const [key, value] of entries) {
    if (!allowed.has(key)) {
      throw new FlorenceStoreUnauthorized(`The ${key} preference cannot be edited here`);
    }
    if (typeof value !== "boolean") {
      throw new FlorenceStoreConflict(`The ${key} preference must be true or false`);
    }
  }
  return Object.fromEntries(entries);
}

function proactiveConsentEnabled(preferences: Record<string, JsonValue>): boolean {
  const acceptedAt = preferences.proactiveUseAcceptedAt;
  if (typeof acceptedAt !== "string" || !acceptedAt.trim() || Number.isNaN(Date.parse(acceptedAt))) {
    return false;
  }
  const enabled = preferences.proactiveGoogleEnabled;
  return enabled === undefined || enabled === true;
}

async function reconcileProactiveConsentState(
  sql: postgres.TransactionSql,
  householdId: string,
  adultId: string,
  preferences: Record<string, JsonValue>,
  now: Date,
): Promise<void> {
  const enabled = proactiveConsentEnabled(preferences);
  if (enabled) {
    await sql`
      update proactive_work set status='active',next_check_at=${now},
        last_error=case
          when kind='finite_monitor'
            and left(coalesce(last_error,''),${GOOGLE_ACTION_WORK_MARKER_PREFIX.length})=${GOOGLE_ACTION_WORK_MARKER_PREFIX}
          then left(last_error,${GOOGLE_ACTION_WORK_MARKER_LENGTH})
          else null
        end
      where household_id=${householdId} and owner_adult_id=${adultId}
        and kind in ('personal_google_poll','finite_monitor') and status='paused'
        and (last_error=${PROACTIVE_CONSENT_PAUSE_REASON}
          or (kind='finite_monitor'
            and left(coalesce(last_error,''),${GOOGLE_ACTION_WORK_MARKER_PREFIX.length})=${GOOGLE_ACTION_WORK_MARKER_PREFIX}
            and right(last_error,${PROACTIVE_CONSENT_PAUSE_REASON.length})=${PROACTIVE_CONSENT_PAUSE_REASON}))
    `;
  } else {
    await sql`
      update proactive_work set status='paused',next_check_at=null,
        last_error=case
          when kind='finite_monitor'
            and left(coalesce(last_error,''),${GOOGLE_ACTION_WORK_MARKER_PREFIX.length})=${GOOGLE_ACTION_WORK_MARKER_PREFIX}
          then left(last_error,${GOOGLE_ACTION_WORK_MARKER_LENGTH})||E'\n'||${PROACTIVE_CONSENT_PAUSE_REASON}
          else ${PROACTIVE_CONSENT_PAUSE_REASON}
        end
      where household_id=${householdId} and owner_adult_id=${adultId}
        and kind in ('personal_google_poll','finite_monitor') and status='active'
    `;
  }

  await reconcileHouseholdProactiveConsentState(sql, householdId, now);
}

async function reconcileHouseholdProactiveConsentState(
  sql: postgres.TransactionSql,
  householdId: string,
  now: Date,
): Promise<void> {
  const [householdConsent] = await sql<{ eligible: boolean }[]>`
    select exists (
      select 1 from households h
      join google_connections g on g.household_id=h.id and g.status='active'
        and g.id in (h.family_calendar_owner_connection_id,h.family_calendar_partner_connection_id)
      join people p on p.household_id=h.id and p.id=g.owner_adult_id
        and p.kind='adult' and p.status='verified'
      where h.id=${householdId} and h.family_calendar_id is not null
        and h.family_calendar_created_at is not null
        and nullif(p.preferences->>'proactiveUseAcceptedAt','') is not null
        and coalesce(p.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
    ) as eligible
  `;
  if (householdConsent?.eligible) {
    await sql`
      update proactive_work set status='active',next_check_at=${now},
        last_error=case
          when kind='finite_monitor'
            and left(coalesce(last_error,''),${GOOGLE_ACTION_WORK_MARKER_PREFIX.length})=${GOOGLE_ACTION_WORK_MARKER_PREFIX}
          then left(last_error,${GOOGLE_ACTION_WORK_MARKER_LENGTH})
          else null
        end
      where household_id=${householdId} and visibility='household'
        and kind in ('family_calendar_poll','finite_monitor','interest_monitor') and status='paused'
        and (last_error=${PROACTIVE_CONSENT_PAUSE_REASON}
          or (kind='finite_monitor'
            and left(coalesce(last_error,''),${GOOGLE_ACTION_WORK_MARKER_PREFIX.length})=${GOOGLE_ACTION_WORK_MARKER_PREFIX}
            and right(last_error,${PROACTIVE_CONSENT_PAUSE_REASON.length})=${PROACTIVE_CONSENT_PAUSE_REASON}))
    `;
  } else {
    await sql`
      update proactive_work set status='paused',next_check_at=null,
        last_error=case
          when kind='finite_monitor'
            and left(coalesce(last_error,''),${GOOGLE_ACTION_WORK_MARKER_PREFIX.length})=${GOOGLE_ACTION_WORK_MARKER_PREFIX}
          then left(last_error,${GOOGLE_ACTION_WORK_MARKER_LENGTH})||E'\n'||${PROACTIVE_CONSENT_PAUSE_REASON}
          else ${PROACTIVE_CONSENT_PAUSE_REASON}
        end
      where household_id=${householdId} and visibility='household'
        and kind in ('family_calendar_poll','finite_monitor','interest_monitor') and status='active'
    `;
  }
}

async function reconcilePrivateConflictSharingState(
  sql: postgres.TransactionSql,
  householdId: string,
  adultId: string,
  enabled: boolean,
): Promise<void> {
  if (enabled) return;

  const reviews = await sql<ProactiveWorkRow[]>`
    select * from proactive_work where household_id=${householdId}
      and kind='initial_private_review' and owner_adult_id=${adultId}
      and status='completed' for update
  `;
  for (const review of reviews) {
    const retained = storedBriefingCandidates(review.briefing_candidates).filter(
      (candidate) => candidate.category !== "conflict",
    );
    await sql`
      update proactive_work set briefing_candidates=${sql.json(retained)}
      where id=${review.id}
    `;
  }

  await sql`
    update messages m set status='failed',sending_at=null,retry_at=null,
      last_error='Private conflict sharing was turned off before delivery'
    from sources s where s.id=m.source_id and s.household_id=${householdId}
      and m.direction='outbound' and m.status='pending'
      and jsonb_typeof(s.metadata->'privateConflictOwnerAdultIds')='array'
      and (s.metadata->'privateConflictOwnerAdultIds') ? ${adultId}
  `;
}

async function googleDerivedUnsentMessageSourceIds(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    adultId: string;
    connectionId?: string | null;
    connectionIds?: readonly string[];
    statuses: readonly ("pending" | "sending" | "failed")[];
  },
): Promise<string[]> {
  if (input.statuses.length === 0) return [];
  const connectionIds = unique(input.connectionIds ?? (input.connectionId ? [input.connectionId] : []));
  const rows = await sql<{ source_id: string }[]>`
    with scoped_connections as (
      select connection.id from google_connections connection
      where connection.household_id=${input.householdId}
        and connection.owner_adult_id=${input.adultId}
        and (${connectionIds.length}=0 or connection.id in ${sql(
          connectionIds.length > 0 ? connectionIds : ["00000000-0000-0000-0000-000000000000"],
        )})
    ), google_sources as (
      select evidence.id from sources evidence
      where evidence.household_id=${input.householdId}
        and evidence.kind in ('gmail','calendar','google_file')
        and (
          exists (
            select 1 from scoped_connections connection
            where evidence.metadata->>'connectionId'=connection.id::text
          )
          or (
            evidence.visibility='private' and evidence.owner_adult_id=${input.adultId}
            and evidence.metadata->>'connectionId' is null
          )
        )
    ), google_work as (
      select work.id,work.kind from proactive_work work
      where work.household_id=${input.householdId}
        and (
          (work.owner_adult_id=${input.adultId}
            and work.kind in ('initial_private_review','personal_google_poll'))
          or exists (
            select 1 from proactive_work_sources link
            join google_sources evidence on evidence.id=link.source_id
            where link.work_id=work.id
          )
        )
    )
    select message.source_id from messages message
    where message.direction='outbound' and message.status in ${sql([...input.statuses])}
      and (message.status<>'failed' or message.receipt_detail->'acceptance' is null)
      and (
        exists (
          select 1 from google_work work
          where message.idempotency_key like ('proactive:' || work.id::text || ':%')
            or (work.kind='initial_private_review'
              and message.idempotency_key like ('initial-private-review:' || work.id::text || ':%'))
        )
        or (
          message.idempotency_key like 'initial-household-briefing:%'
          and exists (select 1 from scoped_connections)
        )
        or exists (
          select 1 from scoped_connections connection
          where message.idempotency_key like
            ('google-reconnect:' || connection.id::text || ':h:%')
        )
        or exists (
          select 1 from sources outbound_source
          join scoped_connections connection
            on jsonb_typeof(outbound_source.metadata->'googleConnectionIds')='array'
            and (outbound_source.metadata->'googleConnectionIds') ? connection.id::text
          where outbound_source.id=message.source_id
        )
        or exists (
          select 1 from calendar_actions action
          join google_sources evidence on evidence.id=action.basis_source_id
          where action.approval_prompt_source_id=message.source_id
            or (action.status='failed'
              and message.idempotency_key like
                ('calendar-failure:' || action.id::text || ':h:%'))
        )
      )
    order by message.source_id for update of message
  `;
  return unique(rows.map((row) => row.source_id));
}

function jsonRecord(value: JsonValue | null | undefined): Record<string, JsonValue> {
  return value !== undefined && isRecord(value) ? { ...value } : {};
}

function jsonString(value: JsonValue | null | undefined, key: string): string | null {
  const item = jsonRecord(value)[key];
  return typeof item === "string" && item.trim().length > 0 ? item : null;
}

function pdfMimeType(value: string): "application/pdf" {
  if (value !== "application/pdf") throw new FlorenceStoreConflict("Stored PDF type is invalid");
  return value;
}

function sameImageReferences(left: readonly ImageReference[], right: readonly ImageReference[]): boolean {
  return (
    left.length === right.length &&
    left.every(
      (image, index) => image.assetId === right[index]?.assetId && image.mimeType === right[index]?.mimeType,
    )
  );
}

function isImageMimeType(value: string): value is ImageReference["mimeType"] {
  return ["image/jpeg", "image/png", "image/webp", "image/heic"].includes(value);
}

function calendarEvent(value: JsonValue): CalendarEventDraft {
  if (!isRecord(value)) throw new FlorenceStoreConflict("A Calendar action has invalid event data");
  const intervalKind = value.intervalKind;
  const title = stringField(value, "title");
  const locationValue = value.location;
  if (locationValue !== null && typeof locationValue !== "string") {
    throw new FlorenceStoreConflict("A Calendar location must be text or null");
  }
  let event: CalendarEventDraft;
  if (intervalKind === "timed") {
    assertExactCalendarKeys(value, ["intervalKind", "title", "startsAt", "endsAt", "timeZone", "location"]);
    event = {
      intervalKind,
      title,
      startsAt: stringField(value, "startsAt"),
      endsAt: stringField(value, "endsAt"),
      timeZone: stringField(value, "timeZone"),
      location: locationValue,
    };
  } else if (intervalKind === "all_day") {
    assertExactCalendarKeys(value, ["intervalKind", "title", "startDate", "endDate", "location"]);
    event = {
      intervalKind,
      title,
      startDate: calendarDate(stringField(value, "startDate")),
      endDate: calendarDate(stringField(value, "endDate")),
      location: locationValue,
    };
  } else {
    throw new FlorenceStoreConflict("A Calendar event has an invalid interval kind");
  }
  validateCalendarEvent(event);
  return event;
}

function familyCalendarMutation(value: JsonValue): FamilyCalendarMutation {
  if (!isRecord(value)) throw new FlorenceStoreConflict("A Calendar action has invalid mutation data");
  assertExactCalendarKeys(value, ["operation", "event", "target"]);
  const operation = value.operation;
  if (operation !== "create" && operation !== "update" && operation !== "delete") {
    throw new FlorenceStoreConflict("A Calendar operation is invalid");
  }
  const targetValue = value.target;
  if (targetValue === undefined) throw new FlorenceStoreConflict("A Calendar target is required");
  const target = targetValue === null ? null : calendarEventTarget(targetValue);
  const eventValue = value.event;
  if (eventValue === undefined) throw new FlorenceStoreConflict("Calendar event data is required");
  const event = eventValue === null ? null : calendarEvent(eventValue);
  const mutation =
    operation === "create"
      ? ({ operation, event, target } as const)
      : operation === "update"
        ? ({ operation, event, target } as const)
        : ({ operation, event, target } as const);
  validateFamilyCalendarMutation(mutation as FamilyCalendarMutation);
  return mutation as FamilyCalendarMutation;
}

function calendarEventTarget(value: JsonValue): CalendarEventTarget {
  if (!isRecord(value)) throw new FlorenceStoreConflict("A Calendar target is invalid");
  assertExactCalendarKeys(value, ["providerEventId", "providerRevision", "observedEvent"]);
  if (value.observedEvent === undefined) {
    throw new FlorenceStoreConflict("A Calendar target needs its observed event");
  }
  const observedEvent = calendarEvent(value.observedEvent);
  return {
    providerEventId: bounded(required(stringField(value, "providerEventId"), "Calendar event ID"), 1_024),
    providerRevision: bounded(
      required(stringField(value, "providerRevision"), "Calendar event revision"),
      500,
    ),
    observedEvent,
  };
}

function validateFamilyCalendarMutation(mutation: FamilyCalendarMutation): void {
  assertExactCalendarKeys(mutation, ["operation", "event", "target"]);
  if (mutation.operation === "create") {
    if (mutation.target !== null) throw new FlorenceStoreConflict("A Calendar create cannot have a target");
    validateCalendarEvent(mutation.event);
    return;
  }
  if (mutation.target === null) {
    throw new FlorenceStoreConflict("A Calendar update or removal requires an observed target");
  }
  required(mutation.target.providerEventId, "Calendar event ID");
  required(mutation.target.providerRevision, "Calendar event revision");
  assertExactCalendarKeys(mutation.target, ["providerEventId", "providerRevision", "observedEvent"]);
  validateCalendarEvent(mutation.target.observedEvent);
  if (mutation.operation === "update") {
    validateCalendarEvent(mutation.event);
  } else if (mutation.event !== null) {
    throw new FlorenceStoreConflict("A Calendar removal cannot include replacement event data");
  }
}

function calendarOfferEvent(value: JsonValue): CalendarEventDraft {
  const mutation = familyCalendarMutation(value);
  if (mutation.operation !== "create") {
    throw new FlorenceStoreConflict("A Calendar suggestion can only add a family event");
  }
  return mutation.event;
}

function validateCalendarEvent(event: CalendarEventDraft): void {
  bounded(required(event.title, "Calendar title"), 500);
  if (event.location !== null) bounded(required(event.location, "Calendar location"), 500);
  if (event.intervalKind === "all_day") {
    assertExactCalendarKeys(event, ["intervalKind", "title", "startDate", "endDate", "location"]);
    const startDate = calendarDate(event.startDate);
    const endDate = calendarDate(event.endDate);
    if (endDate <= startDate) {
      throw new FlorenceStoreConflict("An all-day Calendar event must end after it starts");
    }
    return;
  }
  assertExactCalendarKeys(event, ["intervalKind", "title", "startsAt", "endsAt", "timeZone", "location"]);
  bounded(required(event.timeZone, "Calendar time zone"), 100);
  if (explicitInstant(event.endsAt) <= explicitInstant(event.startsAt)) {
    throw new FlorenceStoreConflict("A Calendar event must end after it starts");
  }
}

function calendarDate(value: string): string {
  const parsed = new Date(`${value}T00:00:00.000Z`);
  if (
    !/^\d{4}-\d{2}-\d{2}$/.test(value) ||
    !Number.isFinite(parsed.getTime()) ||
    parsed.toISOString().slice(0, 10) !== value
  ) {
    throw new FlorenceStoreConflict("An all-day Calendar date must be a real YYYY-MM-DD date");
  }
  return value;
}

function assertExactCalendarKeys(value: object, expected: readonly string[]): void {
  const actual = Object.keys(value).sort();
  const canonical = [...expected].sort();
  if (actual.length !== canonical.length || actual.some((key, index) => key !== canonical[index])) {
    throw new FlorenceStoreConflict("Calendar data contains non-canonical fields");
  }
}

function isRecord(value: JsonValue): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringField(value: Record<string, JsonValue>, key: string): string {
  const field = value[key];
  if (typeof field !== "string" || !field.trim()) {
    throw new FlorenceStoreConflict(`Calendar ${key} is required`);
  }
  return field;
}

function instant(value: string): Date {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) throw new FlorenceStoreConflict("A timestamp is invalid");
  return date;
}

function explicitInstant(value: string): Date {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/.test(value)) {
    throw new FlorenceStoreConflict("A Calendar timestamp must include Z or a UTC offset");
  }
  return instant(value);
}

function required(value: string, name: string): string {
  const trimmed = value.trim();
  if (!trimmed) throw new FlorenceStoreConflict(`${name} is required`);
  return trimmed;
}

function maskPhoneNumber(value: string): string {
  if (!/^\+[1-9]\d{7,14}$/.test(value)) {
    throw new FlorenceStoreConflict("Stored partner phone number is invalid");
  }
  return `••• ••• ${value.slice(-4)}`;
}

function bounded(value: string, limit: number): string {
  return value.length <= limit ? value : value.slice(0, limit);
}

function assertDigest(value: string, name: string): void {
  if (!/^[0-9a-f]{64}$/.test(value)) throw new FlorenceStoreConflict(`${name} digest is invalid`);
}

function assertUuid(value: string, name: string): void {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)) {
    throw new FlorenceStoreConflict(`${name} is invalid`);
  }
}

function sortedDigests(values: readonly string[]): string[] {
  const result = [...values];
  for (const value of result) assertDigest(value, "Linq participant identity");
  return result.sort();
}

function digestStrings(values: readonly string[]): string {
  return createHash("sha256")
    .update(JSON.stringify([...values].sort()))
    .digest("hex");
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function deterministicUuid(value: string): string {
  const digest = createHash("sha256").update(value).digest("hex");
  const variant = ((Number.parseInt(digest[16] ?? "0", 16) & 3) | 8).toString(16);
  return `${digest.slice(0, 8)}-${digest.slice(8, 12)}-5${digest.slice(13, 16)}-${variant}${digest.slice(17, 20)}-${digest.slice(20, 32)}`;
}

function sameStrings(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function unique<T>(values: readonly T[]): T[] {
  return [...new Set(values)];
}

function requiredHousehold(value: HouseholdRecord | null): HouseholdRecord {
  if (!value) throw new Error("The household was not found after its committed change");
  return value;
}
