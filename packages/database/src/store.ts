import { createHash } from "node:crypto";
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
const PROACTIVE_CONSENT_PAUSE_REASON = "Paused because proactive Google use is disabled";
const HOUSEHOLD_SAFE_MONITOR_WHY = "Florence is watching this family coordination item.";
const INITIAL_PRIVATE_REVIEW_OUTAGE_NOTICE =
  "I couldn’t finish checking your Gmail and calendar just now, so I’m not calling it all clear. I’ll keep trying.";
export type GoogleScope =
  | "openid"
  | "email"
  | "https://www.googleapis.com/auth/gmail.readonly"
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

export type PrivateReviewFinding = {
  sourceIds: readonly string[];
  householdCandidate: Omit<SharedBriefingCandidate, "candidateId"> | null;
  monitor?: InitialFiniteMonitorDraft | null;
  familyCalendar?: FamilyCalendarReviewProposal | null;
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

export type PrivateFactContext = {
  slot: string;
  statement: string;
};

export type PrivateStableFactDraft = PrivateFactContext & {
  sourceIds: readonly string[];
};

export type InitialIntelligenceWork =
  | {
      kind: "initial_private_review";
      workId: string;
      household: SharedFamilyProfile;
      adultId: string;
      adultFirstName: string;
      connectionId: string;
      currentPrivateFacts: readonly PrivateFactContext[];
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
  gmailCursor: string;
  calendarCursor: string;
  bubbles: readonly InitialBriefingBubble[];
  findings: readonly PrivateReviewFinding[];
  facts: readonly PrivateStableFactDraft[];
  googleEvidence: readonly GoogleEvidenceDraft[];
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
  sourceIds: readonly string[];
  urgency: "now" | "soon" | "watch";
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

export type DueProactiveWork =
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
      calendarCursor: string;
      activeMonitors: readonly ActiveFiniteMonitor[];
      currentPrivateFacts: readonly PrivateFactContext[];
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
  participantIdentityDigests: readonly [string, string];
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

export type UnboundPartnerInvitation = {
  adultId: string;
  state: "issued" | "expired" | "declined";
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
  facts?: readonly FactDraft[];
  deleteFactIds?: readonly string[];
  finiteMonitors?: readonly FiniteMonitorDraft[];
  finiteMonitorUpdates?: readonly FiniteMonitorUpdate[];
  cancelMonitorIds?: readonly string[];
  interestMutation?: DurableInterestMutation | null;
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
    | "interest_monitor";
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
  status: "active" | "paused" | "completed";
  next_check_at: Date | null;
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
        }[]
      >`
        select p.id as adult_id
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
        order by p.adult_slot,p.id
      `;
      for (const adult of eligible) {
        const reviewId = deterministicUuid(`initial-private-review\0${adult.adult_id}`);
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
        })[]
      >`
        select w.*,p.profile->>'firstName' as adult_first_name,p.display_name as adult_display_name,
          g.id as connection_id
        from proactive_work w
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
          and g.status='active' and c.revoked_at is null and c.stopped_at is null
        order by w.next_check_at,w.id,g.created_at,g.id limit 1
      `;
      if (privateWork?.owner_adult_id && privateWork.connection_id) {
        return {
          kind: "initial_private_review" as const,
          workId: privateWork.id,
          household: await sharedFamilyProfile(sql, privateWork.household_id),
          adultId: privateWork.owner_adult_id,
          adultFirstName: privateWork.adult_first_name ?? privateWork.adult_display_name,
          connectionId: privateWork.connection_id,
          currentPrivateFacts: await currentPrivateFacts(
            sql,
            privateWork.household_id,
            privateWork.owner_adult_id,
          ),
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
                  where private_message.idempotency_key like
                    'initial-private-review:' || private_work.id::text || ':%'
                    and private_message.status<>'sent'
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
        const messages = await sql<{ status: string }[]>`
          select status from messages
          where idempotency_key like ${`initial-private-review:${review.id}:%`}
          order by turn_part
        `;
        if (
          messages.length < 1 ||
          messages.length > 3 ||
          messages.some((message) => message.status !== "sent")
        ) {
          return null;
        }
      }
      const candidates = completedReviews.flatMap((review) =>
        storedBriefingCandidates(review.briefing_candidates).filter(
          (candidate) => candidate.category !== "conflict" || review.private_conflict_busy_sharing_enabled,
        ),
      );
      return {
        kind: "initial_household_briefing" as const,
        workId: householdWork.id,
        household: await sharedFamilyProfile(sql, householdWork.household_id),
        familyCalendarId: calendar.calendarId,
        familyCalendarOwnerAdultId: calendar.ownerAdultId,
        familyCalendarOwnerConnectionId: calendar.connectionId,
        candidates: candidates.map(({ sourceIds: _sourceIds, ...candidate }) => candidate),
      };
    });
  }

  async completePrivateInitialReview(input: CompletePrivateInitialReviewInput): Promise<void> {
    assertUuid(input.workId, "Initial private review work ID");
    const gmailCursor = required(input.gmailCursor, "Gmail review cursor");
    const calendarCursor = required(input.calendarCursor, "Calendar review cursor");
    const bubbles = initialBriefingBubbles(input.bubbles, "private Google review");
    const occurredAt = instant(input.occurredAt);
    await this.#sql.begin(async (sql) => {
      const [work] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where id=${input.workId} and kind='initial_private_review'
          and status='active' for update
      `;
      if (!work?.owner_adult_id) {
        throw new FlorenceStoreConflict("The private Google review is no longer active");
      }
      const [channel] = await sql<ChannelRow[]>`
        select * from linq_channels where household_id=${work.household_id} and audience='private'
          and adult_one_id=${work.owner_adult_id} and adult_two_id is null
          and revoked_at is null and stopped_at is null
        order by bound_at,id limit 1 for share
      `;
      const [connection] = await sql<{ id: string }[]>`
        select id from google_connections where household_id=${work.household_id}
          and owner_adult_id=${work.owner_adult_id} and status='active'
        order by created_at,id limit 1 for share
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
      if (!channel || !connection || !consent) {
        throw new FlorenceStoreUnauthorized("The private Google review authority is no longer active");
      }
      const facts = privateStableFacts(input.facts);
      const allSourceIds = [
        ...new Set([
          ...input.findings.flatMap((finding) => [...finding.sourceIds]),
          ...facts.flatMap((fact) => [...fact.sourceIds]),
        ]),
      ];
      await persistGoogleEvidenceDrafts(sql, {
        householdId: work.household_id,
        drafts: input.googleEvidence,
        sourceIds: allSourceIds,
      });
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
      const candidates = input.findings.flatMap((finding, findingIndex) => {
        if (
          !finding.householdCandidate ||
          (finding.householdCandidate.category === "conflict" && !consent.conflict_sharing_enabled)
        ) {
          return [];
        }
        return [
          privateReviewCandidate(
            deterministicUuid(`briefing-candidate\0${work.id}\0${findingIndex}`),
            finding.householdCandidate,
            finding.sourceIds,
          ),
        ];
      });
      for (const [findingIndex, finding] of input.findings.entries()) {
        if (!finding.monitor) continue;
        if (finding.sourceIds.length === 0) {
          throw new FlorenceStoreConflict("An initial-review finite monitor requires finding evidence");
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
          basisWorkId: `${work.id}:${findingIndex}`,
          occurredAt,
        });
      }
      for (const finding of input.findings) {
        if (!finding.familyCalendar) continue;
        await stageFamilyCalendarReviewProposal(sql, {
          householdId: work.household_id,
          ownerAdultId: work.owner_adult_id,
          proposal: finding.familyCalendar,
          occurredAt,
        });
      }
      await upsertPrivateStableFacts(sql, {
        householdId: work.household_id,
        ownerAdultId: work.owner_adult_id,
        facts,
        occurredAt,
      });
      const turnId = deterministicUuid(`initial-private-review-turn\0${work.id}`);
      const existing = await sql<{ source_id: string }[]>`
        select source_id from messages where turn_id=${turnId} order by turn_part for update
      `;
      if (existing.length === 0) {
        for (const [index, bubble] of bubbles.entries()) {
          await insertOutbound(sql, {
            sourceId: deterministicUuid(`initial-private-review-message\0${work.id}\0${index}`),
            idempotencyKey: `initial-private-review:${work.id}:${index}`,
            moveKind: "message",
            text: bubble.text,
            turnId,
            turnPart: index as 0 | 1 | 2,
            notBefore: new Date(occurredAt.getTime() + bubble.delayMs).toISOString(),
            householdId: work.household_id,
            channelId: channel.id,
            visibility: "private",
            ownerAdultId: work.owner_adult_id,
            occurredAt,
          });
        }
      } else if (existing.length < 1 || existing.length > 3) {
        throw new FlorenceStoreConflict("The private Google review output is incomplete");
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
        on conflict do nothing
      `;
    });
  }

  async completeHouseholdInitialBriefing(input: CompleteHouseholdInitialBriefingInput): Promise<void> {
    assertUuid(input.workId, "Initial household briefing work ID");
    if (
      input.selectedCandidateIds.length > 3 ||
      new Set(input.selectedCandidateIds).size !== input.selectedCandidateIds.length
    ) {
      throw new FlorenceStoreConflict("A household briefing may select at most three distinct findings");
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
      const [channel] = await sql<ChannelRow[]>`
        select c.* from linq_channels c join households h on h.id=c.household_id
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
        const messages = await sql<{ status: string }[]>`
          select status from messages
          where idempotency_key like ${`initial-private-review:${review.id}:%`}
          order by turn_part for share
        `;
        if (
          messages.length < 1 ||
          messages.length > 3 ||
          messages.some((message) => message.status !== "sent")
        ) {
          throw new FlorenceStoreConflict("Both private Google briefings must be delivered first");
        }
      }
      const candidates = reviews.flatMap((review) => {
        if (!review.owner_adult_id) {
          throw new FlorenceStoreConflict("An initial private review has no owning parent");
        }
        return storedBriefingCandidates(review.briefing_candidates)
          .filter(
            (candidate) => candidate.category !== "conflict" || review.private_conflict_busy_sharing_enabled,
          )
          .map((candidate) => ({ candidate, ownerAdultId: review.owner_adult_id as string }));
      });
      const candidateIds = new Set(candidates.map(({ candidate }) => candidate.candidateId));
      if (input.selectedCandidateIds.some((candidateId) => !candidateIds.has(candidateId))) {
        throw new FlorenceStoreUnauthorized("The household briefing selected an unavailable finding");
      }
      const selectedCandidateIds = new Set(input.selectedCandidateIds);
      const privateConflictOwnerAdultIds = unique(
        candidates.flatMap(({ candidate, ownerAdultId }) =>
          selectedCandidateIds.has(candidate.candidateId) && candidate.category === "conflict"
            ? [ownerAdultId]
            : [],
        ),
      );
      const turnId = deterministicUuid(`initial-household-briefing-turn\0${work.id}`);
      const existing = await sql<{ source_id: string }[]>`
        select source_id from messages where turn_id=${turnId} order by turn_part for update
      `;
      if (existing.length === 0) {
        for (const [index, bubble] of bubbles.entries()) {
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
            ...(privateConflictOwnerAdultIds.length > 0
              ? { metadata: { privateConflictOwnerAdultIds } }
              : {}),
            occurredAt,
          });
        }
      } else if (existing.length < 1 || existing.length > 3) {
        throw new FlorenceStoreConflict("The household briefing output is incomplete");
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

  async retryInitialIntelligence(input: {
    workId: string;
    retryAt: string;
    failedAt: string;
    error: string;
  }): Promise<void> {
    assertUuid(input.workId, "Initial intelligence work ID");
    const retryAt = instant(input.retryAt);
    const failedAt = instant(input.failedAt);
    if (retryAt < failedAt) {
      throw new FlorenceStoreConflict("An initial intelligence retry cannot precede its failure");
    }
    await this.#sql.begin(async (sql) => {
      const [work] = await sql<ProactiveWorkRow[]>`
        select * from proactive_work where id=${input.workId}
          and kind in ('initial_private_review','initial_household_briefing')
          and status='active' for update
      `;
      if (!work) throw new FlorenceStoreConflict("The initial intelligence work is no longer retryable");
      if (work.kind === "initial_private_review") {
        if (!work.owner_adult_id) {
          throw new FlorenceStoreConflict("The private Google review is missing its adult");
        }
        const [channel] = await sql<ChannelRow[]>`
          select * from linq_channels where household_id=${work.household_id} and audience='private'
            and adult_one_id=${work.owner_adult_id} and adult_two_id is null
            and revoked_at is null and stopped_at is null
          order by bound_at,id limit 1 for share
        `;
        if (!channel) {
          throw new FlorenceStoreUnauthorized("The private Google review thread is no longer active");
        }
        await insertOutbound(sql, {
          sourceId: deterministicUuid(`initial-private-review-outage-message\0${work.id}`),
          idempotencyKey: `initial-private-review-outage:${work.id}`,
          moveKind: "message",
          text: INITIAL_PRIVATE_REVIEW_OUTAGE_NOTICE,
          turnId: deterministicUuid(`initial-private-review-outage-turn\0${work.id}`),
          turnPart: 0,
          notBefore: failedAt.toISOString(),
          householdId: work.household_id,
          channelId: channel.id,
          visibility: "private",
          ownerAdultId: work.owner_adult_id,
          occurredAt: failedAt,
        });
      }
      await sql`
        update proactive_work set status='active',next_check_at=${retryAt},
          last_error=${bounded(input.error, 2_000)}
        where id=${work.id}
      `;
    });
  }

  async readNextDueProactiveWork(nowInput: string): Promise<DueProactiveWork | null> {
    const now = instant(nowInput);
    return this.#sql.begin(async (sql) => {
      await sql`
        update proactive_work w set status='paused',next_check_at=null,
          last_error=${PROACTIVE_CONSENT_PAUSE_REASON}
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
          last_error=${PROACTIVE_CONSENT_PAUSE_REASON}
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
          and kind in ('personal_google_poll','family_calendar_poll','finite_monitor','interest_monitor')
        order by next_check_at,id
      `;
      for (const work of due) {
        if (work.kind === "personal_google_poll") {
          const [context] = await sql<
            {
              adult_id: string;
              adult_first_name: string | null;
              adult_display_name: string;
              connection_id: string;
            }[]
          >`
            select p.id as adult_id,p.profile->>'firstName' as adult_first_name,
              p.display_name as adult_display_name,g.id as connection_id
            from people p join google_connections g on g.household_id=p.household_id
              and g.owner_adult_id=p.id and g.status='active'
            join linq_channels c on c.household_id=p.household_id and c.audience='private'
              and c.adult_one_id=p.id and c.adult_two_id is null
              and c.revoked_at is null and c.stopped_at is null
            where p.household_id=${work.household_id} and p.id=${work.owner_adult_id}
              and p.kind='adult' and p.status='verified'
              and nullif(p.preferences->>'proactiveUseAcceptedAt','') is not null
              and coalesce(p.preferences->'proactiveGoogleEnabled'='true'::jsonb,true)
            order by g.created_at,g.id,c.bound_at,c.id limit 1
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
              calendarCursor: work.calendar_cursor,
              activeMonitors: await activeFiniteMonitors(sql, work.household_id, "private", context.adult_id),
              currentPrivateFacts: await currentPrivateFacts(sql, work.household_id, context.adult_id),
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

  async completeGooglePoll(input: {
    workId: string;
    gmailCursor: string | null;
    calendarCursor: string;
    googleEvidence: readonly GoogleEvidenceDraft[];
    deliveries: readonly ProactiveDelivery[];
    facts: readonly PrivateStableFactDraft[];
    deliverNotBefore: string;
    occurredAt: string;
  }): Promise<void> {
    assertUuid(input.workId, "Google poll work ID");
    const calendarCursor = required(input.calendarCursor, "Google Calendar cursor");
    const occurredAt = instant(input.occurredAt);
    const deliverNotBefore = proactiveDeliveryTime(input.deliverNotBefore, occurredAt);
    const facts = privateStableFacts(input.facts);
    if (input.deliveries.length > 3) {
      throw new FlorenceStoreConflict("A Google poll can surface at most three findings");
    }
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
          limit 1 for share of p,g,c
        `;
        if (!authority) {
          throw new FlorenceStoreUnauthorized("The personal Google poll authority is no longer active");
        }
      } else {
        if (facts.length > 0) {
          throw new FlorenceStoreUnauthorized("A family Calendar poll cannot create private facts");
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
      const retainedSourceIds = unique([
        ...facts.flatMap((fact) => [...fact.sourceIds]),
        ...input.deliveries.flatMap((delivery) =>
          (work.visibility === "private" && delivery.privateDetail) ||
          delivery.householdConclusion ||
          delivery.monitor ||
          delivery.familyCalendar
            ? [...delivery.sourceIds]
            : [],
        ),
      ]);
      await persistGoogleEvidenceDrafts(sql, {
        householdId: work.household_id,
        drafts: input.googleEvidence,
        sourceIds: retainedSourceIds,
      });
      if (work.kind === "personal_google_poll") {
        if (!work.owner_adult_id) {
          throw new FlorenceStoreConflict("A personal Google poll requires an adult owner");
        }
        await upsertPrivateStableFacts(sql, {
          householdId: work.household_id,
          ownerAdultId: work.owner_adult_id,
          facts,
          occurredAt,
        });
      }
      await applyGooglePollDeliveries(sql, {
        work,
        deliveries: input.deliveries,
        deliverNotBefore,
        occurredAt,
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
        if (work.visibility === "private" && input.privateDetail && privateChannel) {
          await insertProactiveOutbound(sql, {
            workId: work.id,
            suffix: `${input.outcome}:private:${sha256(persistedConclusion)}`,
            householdId: work.household_id,
            channel: privateChannel,
            visibility: "private",
            ownerAdultId: work.owner_adult_id,
            text: input.privateDetail,
            notBefore: deliverNotBefore,
            occurredAt,
          });
        }
        if (householdConclusion && groupChannel) {
          await insertProactiveOutbound(sql, {
            workId: work.id,
            suffix: `${input.outcome}:household:${sha256(persistedConclusion)}`,
            householdId: work.household_id,
            channel: groupChannel,
            visibility: "household",
            ownerAdultId: null,
            text: householdConclusion,
            ...(work.visibility === "private" && input.householdCategory === "conflict" && work.owner_adult_id
              ? { metadata: { privateConflictOwnerAdultIds: [work.owner_adult_id] } }
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
        await sql`delete from proactive_work where id=${work.id}`;
      } else {
        await sql`
          update proactive_work set status='active',current_conclusion=${persistedConclusion},
            why=${persistedWhy},next_check_at=${nextCheck},last_error=null where id=${work.id}
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
    const [row] = await this.#sql<{ id: string }[]>`
      update proactive_work set next_check_at=${retryAt},
        last_error=${bounded(input.error, 2_000)}
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
          last_error=null
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

    return this.#sql.begin(async (sql) => {
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
      return {
        id: deterministicUuid(`family-child\0${input.householdId}\0${childIndex}`),
        firstName,
        lastName,
        displayName: memberDisplayName(firstName, lastName),
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
        invitation_message_id: string;
        state: "issued" | "expired" | "declined";
      }[]
    >`
      select id as adult_id,invitation_message_id,
        case
          when invitation_digest is null then 'declined'
          when invitation_expires_at<=${checkedAt} then 'expired'
          else 'issued'
        end as state
      from people where kind='adult' and role='steward' and adult_slot=2 and status='planned'
        and identity_subject_digest is null
        and invitation_conversation_id=${providerConversationId}
        and invitation_identity_digest=${input.identitySubjectDigest}
        and invitation_message_id is not null and invitation_issued_at is not null
        and (
          (invitation_digest is not null and invitation_expires_at is not null
            and invitation_consumed_at is null and messages_address is not null
            and invitation_approval_source_id is not null and invitation_approved_at is not null)
          or
          (invitation_digest is null and invitation_expires_at is null
            and invitation_consumed_at is not null and messages_address is null
            and invitation_approval_source_id is null and invitation_approved_at is null
            and invitation_retry_at is null and invitation_last_error is null)
        )
      limit 1
    `;
    if (!row) return null;
    if (row.state === "declined") {
      const expiryNoticeSourceId = deterministicUuid(
        `partner-invitation-expired\0${row.adult_id}\0${row.invitation_message_id}`,
      );
      const [expiryNotice] = await this.#sql<{ source_id: string }[]>`
        select source_id from messages where source_id=${expiryNoticeSourceId}
          and direction='outbound' and idempotency_key=${`partner-invitation-expired:${expiryNoticeSourceId}`}
      `;
      if (expiryNotice) return { adultId: row.adult_id, state: "expired" };
    }
    return { adultId: row.adult_id, state: row.state };
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
          partner.invitation_message_id,founder.id as founder_adult_id,
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
          and partner.invitation_digest is not null and partner.invitation_expires_at is not null
          and partner.invitation_consumed_at is null and partner.messages_address is not null
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
          partner.invitation_message_id,founder.id as founder_adult_id,
          channel.id as founder_channel_id
        from people partner
        join messages approval on approval.source_id=partner.invitation_approval_source_id
        join linq_channels channel on channel.id=approval.channel_id
        join people founder on founder.id=approval.sender_adult_id
          and founder.household_id=partner.household_id
        where partner.kind='adult' and partner.role='steward' and partner.adult_slot=2
          and partner.status='planned' and partner.identity_subject_digest is null
          and partner.invitation_digest is not null
          and partner.invitation_expires_at is not null
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
        where partner.id=${input.adultId} and partner.kind='adult' and partner.role='steward'
          and partner.adult_slot=2 and partner.status='planned'
          and partner.identity_subject_digest is null
          and partner.invitation_digest is null and partner.invitation_expires_at is null
          and partner.invitation_consumed_at is null and partner.messages_address is null
          and partner.invitation_conversation_id is null
          and partner.invitation_identity_digest is null
          and partner.invitation_message_id is null and partner.invitation_issued_at is null
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
          invitation_conversation_id=${suppliedDelivery?.providerConversationId ?? null},
          invitation_identity_digest=${suppliedDelivery?.identitySubjectDigest ?? null},
          invitation_message_id=${suppliedDelivery?.providerMessageId ?? null},
          invitation_issued_at=${suppliedDelivery?.issuedAt ?? null},
          invitation_approval_source_id=null,invitation_approved_at=null,
          invitation_retry_at=null,invitation_last_error=null,updated_at=${occurredAt}
        where id=${invitation.adult_id} and invitation_approval_source_id=${invitation.approval_source_id}
          and invitation_issued_at is null returning id
      `;
      if (terminalized.length !== 1) {
        throw new FlorenceStoreConflict("The partner invitation changed before it could be stopped");
      }
      await stagePartnerInvitationTerminalNotice(sql, {
        invitation,
        reason: "delivery_failed",
        occurredAt,
        stableKey: invitation.approval_source_id,
      });
    });
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
      if (row.invitation_issued_at) {
        if (
          row.invitation_digest !== input.challengeDigest ||
          row.invitation_expires_at?.getTime() !== expiresAt.getTime() ||
          row.invitation_consumed_at !== null ||
          row.messages_address !== messagesAddress ||
          row.invitation_conversation_id !== providerConversationId ||
          row.invitation_identity_digest !== input.identitySubjectDigest ||
          row.invitation_message_id !== providerMessageId ||
          row.invitation_issued_at.getTime() !== issuedAt.getTime()
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
        row.invitation_approved_at === null ||
        row.invitation_retry_at === null
      ) {
        throw new FlorenceStoreUnauthorized(
          "The founding adult has not approved this exact partner invitation",
        );
      }
      if (
        row.invitation_digest !== null ||
        row.invitation_expires_at !== null ||
        row.invitation_consumed_at !== null ||
        row.messages_address !== null ||
        row.invitation_conversation_id !== null ||
        row.invitation_identity_digest !== null ||
        row.invitation_message_id !== null
      ) {
        throw new FlorenceStoreConflict("The planned partner has incomplete invitation state");
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
          invitation_consumed_at=null,messages_address=${messagesAddress},
          invitation_conversation_id=${providerConversationId},
          invitation_identity_digest=${input.identitySubjectDigest},
          invitation_message_id=${providerMessageId},invitation_issued_at=${issuedAt},
          invitation_retry_at=null,invitation_last_error=null,
          updated_at=${issuedAt}
        where id=${row.id} returning *
      `;
      return updated;
    });
    if (!adult) throw new Error("The partner invitation was not stored");
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
    occurredAt: string;
  }): Promise<FamilyMemberRecord> {
    const occurredAt = instant(input.occurredAt);
    const adult = await this.#sql.begin(async (sql) => {
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
      const completedAt = jsonRecord(row.profile).onboardingCompletedAt;
      if (typeof completedAt === "string" && completedAt.length > 0) return row;
      const [updated] = await sql<PersonRow[]>`
        update people set profile=profile||${sql.json({
          onboardingCompletedAt: occurredAt.toISOString(),
        })},updated_at=${occurredAt}
        where id=${row.id} returning *
      `;
      return updated;
    });
    if (!adult) throw new Error("Partner onboarding completion was not stored");
    return personRecord(adult);
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
      return {
        householdId: household.id,
        createChatIdempotencyKey: previous
          ? `family-group:${household.id}:replace:${previous.id}`
          : `family-group:${household.id}:initial`,
        participantPhoneNumbers: [first.messages_address, second.messages_address],
        participantIdentityDigests: [
          first.identity_subject_digest,
          second.identity_subject_digest,
        ].sort() as [string, string],
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
        founder_identity_digest: string;
        partner_adult_id: string;
        partner_identity_digest: string;
        group_adult_one_id: string;
        group_identity_one_digest: string;
        group_adult_two_id: string;
        group_identity_two_digest: string;
        group_authority_digest: string;
      }[]
    >`
      select h.id as household_id,
        founder.id as founder_adult_id,
        founder.identity_subject_digest as founder_identity_digest,
        partner.id as partner_adult_id,
        partner.identity_subject_digest as partner_identity_digest,
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
      const identityDigests = [row.founder_identity_digest, row.partner_identity_digest].sort();
      const groupIdentityDigests = [row.group_identity_one_digest, row.group_identity_two_digest].sort();
      if (
        sameStrings(adultIds, groupAdultIds) &&
        sameStrings(identityDigests, groupIdentityDigests) &&
        row.group_authority_digest === digestStrings([...adultIds, ...identityDigests])
      ) {
        return row.household_id;
      }
    }
    return null;
  }

  async bindCreatedMessagesGroup(input: {
    householdId: string;
    providerConversationId: string;
    participantIdentityDigests: readonly string[];
    occurredAt: string;
  }): Promise<LinqChannelRecord> {
    const providerConversationId = required(
      input.providerConversationId,
      "Family group Linq conversation ID",
    );
    const observed = sortedDigests(input.participantIdentityDigests);
    if (observed.length !== 2 || new Set(observed).size !== 2) {
      throw new FlorenceStoreConflict("The family group requires both distinct adult identities");
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
        !sameStrings(adults.flatMap((adult) => adult.identity_subject_digest ?? []).sort(), observed)
      ) {
        throw new FlorenceStoreConflict("The family group requires exactly both verified adults");
      }
      const [currentGroup] = await sql<ChannelRow[]>`
        select * from linq_channels where household_id=${input.householdId}
          and audience='group' and revoked_at is null for update
      `;
      if (currentGroup) {
        if (
          currentGroup.provider_conversation_id !== providerConversationId ||
          !sameStrings(channelIdentityDigests(currentGroup), observed)
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
      const first = adults[0];
      const second = adults[1];
      if (!first?.identity_subject_digest || !second?.identity_subject_digest) {
        throw new FlorenceStoreConflict("The verified family identities are incomplete");
      }
      const [inserted] = await sql<ChannelRow[]>`
        insert into linq_channels (
          id,household_id,audience,provider_conversation_id,adult_one_id,identity_one_digest,
          adult_two_id,identity_two_digest,authority_digest,bound_at
        ) values (${deterministicUuid(`linq-group\0${providerConversationId}`)},${input.householdId},
          'group',${providerConversationId},${first.id},${first.identity_subject_digest},${second.id},
          ${second.identity_subject_digest},${digestStrings([first.id, second.id, ...observed])},${occurredAt})
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
    cue: "reaction" | "work";
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
          ? { reaction: "emphasize", replyToSourceId: rootSourceId, turnPart: -1 as const }
          : { text: "I’m looking through this now.", turnPart: 0 as const }),
        turnId: cueTurnId,
        notBefore: occurredAt.toISOString(),
        householdId: turn.household_id,
        channelId: turn.channel_id,
        visibility: turn.visibility,
        ownerAdultId: turn.owner_adult_id,
        occurredAt,
      });
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
    const offerRows = await this.#sql<
      {
        id: string;
        approval_prompt_source_id: string;
        payload: JsonValue;
      }[]
    >`
      select action.id,approval_prompt.source_id as approval_prompt_source_id,action.payload
      from calendar_actions action
      join messages approval_prompt on approval_prompt.source_id=action.approval_prompt_source_id
        and approval_prompt.channel_id=${channel.id} and approval_prompt.direction='outbound'
        and approval_prompt.move_kind in ('message','reply') and approval_prompt.turn_part=0
        and approval_prompt.status='sent'
      where action.household_id=${row.household_id} and action.status='offered'
        and ${groupCalendarIsActive}
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
      }[]
    >`
      select w.id,w.objective,w.current_conclusion,w.end_condition,w.next_check_at,w.why,
        array_agg(pws.source_id order by pws.source_id) as source_ids
      from proactive_work w join proactive_work_sources pws on pws.work_id=w.id
      where w.household_id=${row.household_id} and w.kind='finite_monitor' and w.status='active'
        and (
          (${channel.audience === "private"} and w.visibility='private'
            and w.owner_adult_id=${row.sender_adult_id})
          or (${channel.audience === "group"} and w.visibility='household')
        )
      group by w.id,w.objective,w.current_conclusion,w.end_condition,w.next_check_at,w.why
      order by w.next_check_at,w.id
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
      const handoffTurnId = deterministicUuid(`founder-handoff-turn\0${row.household_id}\0${founder.id}`);
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
        from messages where turn_id=${handoffTurnId} order by turn_part
      `;
      const handoffSent =
        handoffRows.length === 3 &&
        handoffRows.every(
          (message, index) =>
            message.source_id ===
              deterministicUuid(`founder-handoff\0${row.household_id}\0${founder.id}\0${index}`) &&
            message.channel_id === channel.id &&
            message.status === "sent" &&
            message.move_kind === "message" &&
            message.turn_part === index &&
            message.idempotency_key === `founder-handoff:${row.household_id}:${founder.id}:${index}`,
        );
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
          const approvalPromptSourceId = handoffRows[2]?.source_id;
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
        where m.source_id=${input.sourceId} and m.direction='inbound' for update of m,s
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
      if (turn.audience === "private" && calendarMutations) {
        throw new FlorenceStoreUnauthorized("Calendar changes belong in the exact family Messages group");
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
        const handoffTurnId = deterministicUuid(`founder-handoff-turn\0${turn.household_id}\0${founder.id}`);
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
          from messages where turn_id=${handoffTurnId} order by turn_part for share
        `;
        if (
          handoffRows.length !== 3 ||
          handoffRows.some(
            (message, index) =>
              message.source_id !==
                deterministicUuid(`founder-handoff\0${turn.household_id}\0${founder.id}\0${index}`) ||
              message.channel_id !== turn.channel_id ||
              message.status !== "sent" ||
              message.move_kind !== "message" ||
              message.turn_part !== index ||
              message.idempotency_key !== `founder-handoff:${turn.household_id}:${founder.id}:${index}`,
          )
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
            next_check_at=${nextCheck},status='active',last_error=null
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
          delete from calendar_actions where household_id=${turn.household_id}
            and status='offered'
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
        const approved = await sql`
          update calendar_actions action set status='pending',approval_source_id=${turn.source_id},
            retry_at=${handledAt},last_error=null
          where action.id=${approval.offerId} and action.household_id=${turn.household_id}
            and action.status='offered' and action.approval_prompt_source_id is not null
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
          delete from calendar_actions where household_id=${turn.household_id}
            and status='offered'
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
    const [row] = await this.#sql<{ source_id: string }[]>`
      select m.source_id from messages m join sources s on s.id=m.source_id
      where m.direction='outbound' and m.status='pending'
        and coalesce(m.retry_at,m.not_before)<=${current}
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
        }[]
      >`
        select status,move_kind,provider_message_id,receipt_detail from messages where source_id=${input.sourceId}
          and direction='outbound' for update
      `;
      if (!current) throw new FlorenceStoreConflict("The outbound message does not exist");
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
              select m.source_id,m.status,m.move_kind,m.provider_message_id,m.sent_at,m.receipt_detail
              from messages m join linq_channels c on c.id=m.channel_id
              where m.direction='outbound' and m.move_kind in ('message','reply')
                and c.provider_conversation_id=${input.providerConversationId}
                and (m.provider_message_id=${input.providerMessageId}
                  or (${input.idempotencyKey}::text is not null and m.idempotency_key=${input.idempotencyKey}))
              for update of m
            `
          : await sql<LinqObservationRow[]>`
              select reaction.source_id,reaction.status,reaction.move_kind,reaction.provider_message_id,
                reaction.sent_at,reaction.receipt_detail
              from messages reaction join linq_channels c on c.id=reaction.channel_id
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
        and partner.invitation_digest is null and partner.invitation_expires_at is null
        and partner.invitation_consumed_at is null and partner.messages_address is null
        and partner.invitation_conversation_id is null
        and partner.invitation_identity_digest is null
        and partner.invitation_message_id is null and partner.invitation_issued_at is null
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
        and invitation_approval_source_id is not null and invitation_issued_at is null
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
               a.approval_prompt_source_id,a.payload,a.provider_event_id,a.provider_etag,
               a.committed_at,a.retry_at,message.channel_id,message.direction,
               message.sender_adult_id,c.audience as channel_audience,c.provider_conversation_id,
               c.adult_one_id,c.identity_one_digest,c.adult_two_id,c.identity_two_digest,
               c.authority_digest,c.bound_at,c.revoked_at,c.stopped_at
        from calendar_actions a left join messages message on message.source_id=a.approval_source_id
        left join linq_channels c on c.id=message.channel_id
        where a.status='pending' and a.retry_at<=${dueAt}
        order by a.retry_at,a.created_at,a.id limit 1
      `;
      if (!row) return null;
      const familyCalendarAuthority = await readFamilyCalendarAuthority(sql, row.household_id);
      const mutation = familyCalendarMutation(row.payload);
      if (row.approval_source_id !== null) {
        const approvalChannel = calendarApprovalChannel(row);
        if (
          row.direction !== "inbound" ||
          row.sender_adult_id === null ||
          !familyCalendarAuthority ||
          !isExactFamilyCalendarAuthority(familyCalendarAuthority, approvalChannel, row.sender_adult_id) ||
          !(await isCalendarAdultApprovalBound(sql, row))
        ) {
          await failCalendarAuthority(sql, row.id, dueAt, "Calendar authority is no longer active");
          return null;
        }
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
          !(await isOfficialPrivateCalendarBasis(
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
      return {
        actionId: row.id,
        householdId: row.household_id,
        connectionId: credential.connectionId,
        ownerAdultId: credential.ownerAdultId,
        calendarId: credential.calendarId,
        mutation,
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
      const notification = await calendarActionNotification(sql, current, familyCalendarAuthority);
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
          visibility: "household",
          ownerAdultId: null,
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
      const notification = await calendarActionNotification(sql, current, familyCalendarAuthority);
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
          visibility: "household",
          ownerAdultId: null,
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
    await this.#requireVerifiedAdult(input.householdId, input.ownerAdultId);
    const [row] = await this.#sql<GoogleConnectionRow[]>`
      insert into google_connections (
        id,household_id,owner_adult_id,status,state_digest,session_binding_digest,state_expires_at,
        created_at,updated_at
      ) values (${input.connectionId},${input.householdId},${input.ownerAdultId},'pending',${input.stateDigest},
        ${input.sessionBindingDigest},${instant(input.stateExpiresAt)},${instant(input.now)},${instant(input.now)})
      returning *
    `;
    if (!row) throw new Error("The Google connection was not created");
    return googleConnectionView(row);
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
      const [row] = await sql<GoogleConnectionRow[]>`
        update google_connections set status='active',google_subject_digest=${input.googleSubjectDigest},
          email_label=${required(input.emailLabel, "Google account email")},
          granted_scopes=${sql.array([...input.grantedScopes])},
          refresh_token_envelope=${required(input.refreshTokenEnvelope, "Google refresh token envelope")},
          session_binding_digest=null,last_error=null,updated_at=${now}
        where id=${input.connectionId} and state_digest=${input.stateDigest}
          and state_consumed_at is not null and status='pending' returning *
      `;
      if (!row) throw new FlorenceStoreConflict("Google OAuth state is no longer current");
      const [binding] = await sql<
        {
          adult_slot: 1 | 2 | null;
          founder_connection_id: string | null;
          partner_connection_id: string | null;
          founder_subject_digest: string | null;
          partner_subject_digest: string | null;
        }[]
      >`
        select person.adult_slot,
          household.family_calendar_owner_connection_id as founder_connection_id,
          household.family_calendar_partner_connection_id as partner_connection_id,
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
        binding.founder_subject_digest === input.googleSubjectDigest
      ) {
        await sql`
          update households set family_calendar_owner_connection_id=${row.id},updated_at=${now}
          where id=${row.household_id}
        `;
      } else if (
        binding?.adult_slot === 2 &&
        binding.partner_connection_id !== null &&
        binding.partner_subject_digest === input.googleSubjectDigest
      ) {
        await sql`
          update households set family_calendar_partner_connection_id=${row.id},updated_at=${now}
          where id=${row.household_id}
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
  }): Promise<readonly string[]> {
    if (input.texts.length < 1 || input.texts.length > 3) {
      throw new FlorenceStoreConflict("The founder handoff needs one to three message bubbles");
    }
    const texts = input.texts.map((text, index) => required(text, `Founder handoff bubble ${index + 1}`));
    const providerConversationId = required(input.providerConversationId, "Linq conversation ID");
    const occurredAt = instant(input.occurredAt);
    const turnId = deterministicUuid(`founder-handoff-turn\0${input.householdId}\0${input.adultId}`);
    const sourceIds = texts.map((_, index) =>
      deterministicUuid(`founder-handoff\0${input.householdId}\0${input.adultId}\0${index}`),
    );

    return this.#sql.begin(async (sql) => {
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
        from messages where turn_id=${turnId} order by turn_part for update
      `;
      if (existing.length > 0) {
        if (
          existing.length > 3 ||
          existing.some(
            (message, index) =>
              message.source_id !==
                deterministicUuid(`founder-handoff\0${input.householdId}\0${input.adultId}\0${index}`) ||
              message.channel_id !== channel.id ||
              message.move_kind !== "message" ||
              message.text !== texts[index] ||
              message.reply_to_source_id !== null ||
              message.turn_part !== index ||
              message.idempotency_key !== `founder-handoff:${input.householdId}:${input.adultId}:${index}`,
          )
        ) {
          throw new FlorenceStoreConflict("The founder handoff was already staged with different content");
        }
        return existing.map((message) => message.source_id);
      }

      for (const [index, text] of texts.entries()) {
        await insertOutbound(sql, {
          sourceId: sourceIds[index] as string,
          idempotencyKey: `founder-handoff:${input.householdId}:${input.adultId}:${index}`,
          moveKind: "message",
          text,
          turnId,
          turnPart: index as 0 | 1 | 2,
          notBefore: new Date(occurredAt.getTime() + index * 700).toISOString(),
          householdId: input.householdId,
          channelId: channel.id,
          visibility: "private",
          ownerAdultId: input.adultId,
          occurredAt,
        });
      }
      return sourceIds;
    });
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
    now: string;
  }): Promise<{ view: GoogleConnectionView; refreshTokenEnvelope: string | null } | null> {
    const now = instant(input.now);
    return this.#sql.begin(async (sql) => {
      const [household] = await sql<{ id: string }[]>`
        select id from households where id=${input.householdId} for share
      `;
      if (!household) return null;
      const [current] = await sql<GoogleConnectionRow[]>`
        select * from google_connections where id=${input.connectionId}
          and household_id=${input.householdId} and owner_adult_id=${input.ownerAdultId}
          and status<>'disconnected' for update
      `;
      if (!current) return null;
      const [row] = await sql<GoogleConnectionRow[]>`
        update google_connections set status='disconnected',refresh_token_envelope=null,
          updated_at=${now} where id=${input.connectionId} returning *
      `;
      if (!row) throw new Error("The Google connection was not disconnected");
      if (current.status === "active") {
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

  async #requireVerifiedAdult(householdId: string, adultId: string): Promise<void> {
    const [adult] = await this.#sql<{ id: string }[]>`
      select id from people where household_id=${householdId} and id=${adultId}
        and kind='adult' and status='verified'
    `;
    if (!adult) throw new FlorenceStoreUnauthorized();
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
    id: deterministicUuid(`gmail-source\0${input.householdId}\0${externalKey}`),
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
    id: deterministicUuid(
      `calendar-source\0${input.householdId}\0${connectionId}\0${calendarId}\0${providerEventId}`,
    ),
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

type OutboundInsert = OutboundDraft & {
  householdId: string;
  channelId: string;
  parentSourceId?: string;
  visibility: Visibility;
  ownerAdultId: string | null;
  metadata?: JsonObject;
  occurredAt: Date;
};

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
    where id=${invitation.adult_id} and invitation_digest is not null
      and invitation_consumed_at is null
      and invitation_message_id=${invitation.invitation_message_id}
    returning id
  `;
  if (invalidated.length !== 1) return false;
  await stagePartnerInvitationTerminalNotice(sql, {
    invitation,
    reason,
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
    occurredAt: Date;
    stableKey: string;
  },
): Promise<void> {
  const sourceId = deterministicUuid(
    `partner-invitation-${input.reason}\0${input.invitation.adult_id}\0${input.stableKey}`,
  );
  const text =
    input.reason === "expired"
      ? `${input.invitation.first_name}’s Florence setup link expired, so I stopped the invitation. I won’t message them again unless you ask me to send a fresh one.`
      : input.reason === "delivery_failed"
        ? `I couldn’t deliver ${input.invitation.first_name}’s Florence setup link, so I stopped the invitation. I won’t message them again unless you ask me to try again.`
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
  const identities = [authority.founder_identity_digest, authority.partner_identity_digest].sort();
  const channelIdentities = channelIdentityDigests(channel);
  return (
    adults.includes(senderAdultId) &&
    sameStrings(adults, channelAdults) &&
    sameStrings(identities, channelIdentities) &&
    channel.authority_digest === digestStrings([...adults, ...identities])
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
      a.approval_prompt_source_id,a.payload,a.provider_event_id,a.provider_etag,a.committed_at,a.retry_at,
      approval.channel_id,approval.direction,approval.sender_adult_id,
      channel.audience as channel_audience,channel.provider_conversation_id,channel.adult_one_id,
      channel.identity_one_digest,channel.adult_two_id,channel.identity_two_digest,
      channel.authority_digest,channel.bound_at,channel.revoked_at,channel.stopped_at
    from calendar_actions a
    left join messages approval on approval.source_id=a.approval_source_id
    left join linq_channels channel on channel.id=approval.channel_id
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

async function calendarActionNotification(
  sql: postgres.TransactionSql,
  action: CalendarActionAuthorityRow,
  authority: FamilyCalendarAuthorityRow | undefined,
): Promise<{ channel: ChannelRow; replyToSourceId: string | null } | null> {
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
    if (!isMatchingFamilyGroupAuthority(authority, channel, action.sender_adult_id)) {
      throw new FlorenceStoreConflict("The stored Calendar approval was not from the exact family group");
    }
    if (channel.revoked_at !== null || channel.stopped_at !== null) return null;
    if (!isExactFamilyCalendarAuthority(authority, channel, action.sender_adult_id)) {
      throw new FlorenceStoreConflict("The Family Calendar is no longer ready");
    }
    return { channel, replyToSourceId: action.approval_source_id };
  }
  if (
    !action.basis_source_id ||
    action.approval_prompt_source_id !== null ||
    !(await isOfficialPrivateCalendarBasis(sql, action.household_id, action.basis_source_id, authority))
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
  return { channel, replyToSourceId: null };
}

function activeFamilyCalendarCredential(
  authority: FamilyCalendarAuthorityRow | undefined,
): ActiveFamilyCalendarCredential | null {
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
    return null;
  }
  if (authority.founder_connection_status === "active") {
    return {
      householdId: authority.household_id,
      connectionId: authority.family_calendar_owner_connection_id,
      ownerAdultId: authority.founder_adult_id,
      calendarId: authority.family_calendar_id,
    };
  }
  if (authority.partner_connection_status === "active") {
    return {
      householdId: authority.household_id,
      connectionId: authority.family_calendar_partner_connection_id,
      ownerAdultId: authority.partner_adult_id,
      calendarId: authority.family_calendar_id,
    };
  }
  return null;
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
  if (sourceIds.length > 100) {
    throw new FlorenceStoreConflict("Too many Google sources were cited at once");
  }
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
      await sql`
        insert into sources (
          id,household_id,kind,visibility,owner_adult_id,external_key,label,metadata,occurred_at
        ) values (${draft.id},${draft.householdId},'gmail','private',${draft.ownerAdultId},
          ${draft.externalKey},${draft.label},${sql.json(draft.metadata)},${instant(draft.occurredAt)})
        on conflict (household_id,kind,external_key) do nothing
      `;
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

async function currentPrivateFacts(
  sql: postgres.TransactionSql,
  householdId: string,
  ownerAdultId: string,
): Promise<readonly PrivateFactContext[]> {
  const rows = await sql<{ slot: string; value: JsonValue }[]>`
    select slot,value from facts where household_id=${householdId}
      and visibility='private' and owner_adult_id=${ownerAdultId}
    order by updated_at desc,slot limit 100
  `;
  return rows.flatMap((row) => {
    const statement = jsonString(row.value, "statement");
    return statement ? [{ slot: row.slot, statement }] : [];
  });
}

function privateStableFacts(input: readonly PrivateStableFactDraft[]): PrivateStableFactDraft[] {
  if (input.length > 6) {
    throw new FlorenceStoreConflict("A private Google review can retain at most six stable facts");
  }
  const slots = new Set<string>();
  return input.map((fact, index) => {
    const slot = required(fact.slot, `Private Google fact ${index + 1} slot`);
    if (slot.length > 160 || !/^[a-z0-9][a-z0-9:_-]*$/.test(slot)) {
      throw new FlorenceStoreConflict("A private Google fact needs a stable lowercase semantic slot");
    }
    if (slots.has(slot)) {
      throw new FlorenceStoreConflict("A private Google review cannot repeat a fact slot");
    }
    slots.add(slot);
    const statement = required(fact.statement, `Private Google fact ${index + 1} statement`);
    if (statement.length > 2_000) {
      throw new FlorenceStoreConflict("A private Google fact statement is too long");
    }
    const sourceIds = unique(fact.sourceIds);
    if (sourceIds.length < 1 || sourceIds.length > 10) {
      throw new FlorenceStoreConflict("A private Google fact needs one to ten current sources");
    }
    for (const sourceId of sourceIds) assertUuid(sourceId, "Private Google fact source ID");
    return { slot, statement, sourceIds };
  });
}

async function upsertPrivateStableFacts(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    ownerAdultId: string;
    facts: readonly PrivateStableFactDraft[];
    occurredAt: Date;
  },
): Promise<void> {
  if (input.facts.length === 0) return;
  await assertProactiveSources(
    sql,
    input.householdId,
    "private",
    input.ownerAdultId,
    input.facts.flatMap((fact) => [...fact.sourceIds]),
  );
  for (const fact of input.facts) {
    const [existing] = await sql<{ id: string }[]>`
      select id from facts where household_id=${input.householdId} and slot=${fact.slot}
        and visibility='private' and owner_adult_id=${input.ownerAdultId}
      for update
    `;
    const factId =
      existing?.id ?? deterministicUuid(`private-google-fact\0${input.ownerAdultId}\0${fact.slot}`);
    if (existing) {
      await sql`
        update facts set label=${fact.statement.slice(0, 160)},value=${sql.json({
          statement: fact.statement,
        })},corrected_at=${input.occurredAt},updated_at=${input.occurredAt}
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
  }
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

async function applyGooglePollDeliveries(
  sql: postgres.TransactionSql,
  input: {
    work: ProactiveWorkRow;
    deliveries: readonly ProactiveDelivery[];
    deliverNotBefore: Date;
    occurredAt: Date;
  },
): Promise<void> {
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

  for (const [index, delivery] of input.deliveries.entries()) {
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
    if (work.visibility === "private" && delivery.privateDetail && privateChannel) {
      await insertProactiveOutbound(sql, {
        workId: work.id,
        suffix: `private:${deliveryKey}`,
        householdId: work.household_id,
        channel: privateChannel,
        visibility: "private",
        ownerAdultId: work.owner_adult_id,
        text: delivery.privateDetail,
        notBefore: findingNotBefore,
        occurredAt: input.occurredAt,
      });
    }
    if (householdConclusion && groupChannel) {
      await insertProactiveOutbound(sql, {
        workId: work.id,
        suffix: `household:${deliveryKey}`,
        householdId: work.household_id,
        channel: groupChannel,
        visibility: "household",
        ownerAdultId: null,
        text: householdConclusion,
        ...(work.visibility === "private" && delivery.householdCategory === "conflict" && work.owner_adult_id
          ? { metadata: { privateConflictOwnerAdultIds: [work.owner_adult_id] } }
          : {}),
        notBefore: findingNotBefore,
        occurredAt: input.occurredAt,
      });
    }
    if (delivery.monitor) {
      if (work.visibility === "household" && !householdConclusion) {
        throw new FlorenceStoreConflict("A household monitor change requires a household-safe conclusion");
      }
      const monitorChange =
        work.visibility === "household" && householdConclusion
          ? householdSafeMonitorChange(delivery.monitor, householdConclusion)
          : delivery.monitor;
      await applyProactiveMonitorChange(sql, {
        householdId: work.household_id,
        visibility: work.visibility,
        ownerAdultId: work.owner_adult_id,
        sourceIds,
        change: monitorChange,
        basisWorkId: work.id,
        occurredAt: input.occurredAt,
      });
    }
    if (delivery.familyCalendar) {
      if (work.visibility !== "private" || !work.owner_adult_id) {
        throw new FlorenceStoreUnauthorized(
          "Automatic family Calendar proposals require one adult's private official source",
        );
      }
      await stageFamilyCalendarReviewProposal(sql, {
        householdId: work.household_id,
        ownerAdultId: work.owner_adult_id,
        proposal: delivery.familyCalendar,
        occurredAt: input.occurredAt,
      });
    }
  }
}

async function stageFamilyCalendarReviewProposal(
  sql: postgres.TransactionSql,
  input: {
    householdId: string;
    ownerAdultId: string;
    proposal: FamilyCalendarReviewProposal;
    occurredAt: Date;
  },
): Promise<boolean> {
  const sourceIds = unique(input.proposal.sourceIds).sort();
  if (sourceIds.length < 1 || sourceIds.length > 10) {
    throw new FlorenceStoreConflict("A family Calendar review proposal requires official evidence");
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
  const ownedSources = await sql<{ id: string }[]>`
    select id from sources where household_id=${input.householdId} and id in ${sql(sourceIds)}
      and visibility='private' and owner_adult_id=${input.ownerAdultId} for share
  `;
  if (ownedSources.length !== sourceIds.length) {
    throw new FlorenceStoreUnauthorized("A Calendar review can use only this adult's private evidence");
  }
  const mutation: Extract<FamilyCalendarMutation, { operation: "create" }> = {
    operation: "create",
    event: input.proposal.event,
    target: null,
  };
  const id = deterministicUuid(`calendar-review\0${input.householdId}\0${JSON.stringify(mutation)}`);
  const basisSourceId = sourceIds[0] as string;
  const automatic =
    input.proposal.disposition === "automatic" && familyCalendarAutomaticCreationEnabled(authority);
  if (automatic) {
    await sql`
      insert into calendar_actions (
        id,household_id,basis_source_id,payload,status,retry_at,created_at
      ) values (${id},${input.householdId},${basisSourceId},${sql.json(mutation)},'pending',
        ${input.occurredAt},${input.occurredAt})
      on conflict (id) do nothing
    `;
    return true;
  }

  const promptSourceId = deterministicUuid(`calendar-review-prompt\0${id}`);
  await insertOutbound(sql, {
    sourceId: promptSourceId,
    idempotencyKey: `calendar-review-prompt:${id}`,
    moveKind: "message",
    text: sanitizedFamilyCalendarSuggestion(input.proposal.event),
    turnId: deterministicUuid(`calendar-review-prompt-turn\0${id}`),
    turnPart: 0,
    notBefore: input.occurredAt.toISOString(),
    householdId: input.householdId,
    channelId: groupChannel.id,
    visibility: "household",
    ownerAdultId: null,
    occurredAt: input.occurredAt,
  });
  await sql`
    delete from calendar_actions where household_id=${input.householdId}
      and status='offered'
  `;
  await sql`
    insert into calendar_actions (
      id,household_id,basis_source_id,approval_prompt_source_id,payload,status,retry_at,created_at
    ) values (${id},${input.householdId},${basisSourceId},${promptSourceId},
      ${sql.json(mutation)},'offered',${input.occurredAt},${input.occurredAt})
    on conflict (id) do nothing
  `;
  return true;
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
    monitorId = deterministicUuid(
      `proactive-monitor\0${input.basisWorkId}\0${sha256(
        JSON.stringify({ objective, sourceIds: unique(input.sourceIds) }),
      )}`,
    );
    await sql`
      insert into proactive_work (
        id,household_id,kind,visibility,owner_adult_id,objective,why,
        current_conclusion,end_condition,status,next_check_at,created_at
      ) values (${monitorId},${input.householdId},'finite_monitor',${input.visibility},
        ${input.ownerAdultId},${objective},${why},${conclusion},${endCondition},'active',${nextCheck},
        ${input.occurredAt})
      on conflict do nothing
    `;
  } else {
    assertUuid(change.monitorId, "Finite monitor ID");
    monitorId = change.monitorId;
    const updated =
      change.operation === "complete"
        ? await sql`
            delete from proactive_work
            where id=${monitorId} and household_id=${input.householdId} and kind='finite_monitor'
              and status='active' and visibility=${input.visibility}
              and owner_adult_id is not distinct from ${input.ownerAdultId}
            returning id
          `
        : await sql`
            update proactive_work set objective=${objective},why=${why},current_conclusion=${conclusion},
              end_condition=${endCondition},status='active',next_check_at=${nextCheck}
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

type StoredBriefingCandidate = SharedBriefingCandidate & { sourceIds: readonly string[] };

function isSharedBriefingCategory(value: string): value is SharedBriefingCandidate["category"] {
  return ["deadline", "conflict", "handoff", "family_date", "loose_end"].includes(value);
}

function privateReviewCandidate(
  candidateId: string,
  candidate: Omit<SharedBriefingCandidate, "candidateId">,
  sourceIds: readonly string[],
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
  const cited = unique(sourceIds);
  if (cited.length < 1 || cited.length > 20) {
    throw new FlorenceStoreConflict("A briefing candidate needs one to twenty private sources");
  }
  for (const sourceId of cited) assertUuid(sourceId, "Briefing source ID");
  return {
    candidateId,
    category,
    summary,
    urgency,
    dueAt: candidate.dueAt === null ? null : instant(candidate.dueAt).toISOString(),
    needsAnswer: candidate.needsAnswer,
    sourceIds: cited,
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
    );
  });
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
  const calendarId = deterministicUuid(`calendar\0${priorSourceId}`);
  await sql`
    update messages set status='failed',retry_at=null,
      last_error='Superseded before delivery by a newer message in this conversation'
    where direction='outbound' and status='pending' and turn_id=${finalTurnId}
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
  const [existing] = await sql<{ source_id: string }[]>`
    select source_id from messages where idempotency_key=${input.idempotencyKey}
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
      ${`outbound:${input.idempotencyKey}`},${input.parentSourceId ?? null},
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
      ${input.idempotencyKey},${instant(input.notBefore)},'pending')
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

const EDITABLE_MEMBER_PROFILE_KEYS = new Set(["school", "activities", "postalCode"]);

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
    if (value === null) delete next[key];
    else next[key] = value;
  }
  return next;
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
      update proactive_work set status='active',next_check_at=${now},last_error=null
      where household_id=${householdId} and owner_adult_id=${adultId}
        and kind in ('personal_google_poll','finite_monitor') and status='paused'
        and last_error=${PROACTIVE_CONSENT_PAUSE_REASON}
    `;
  } else {
    await sql`
      update proactive_work set status='paused',next_check_at=null,
        last_error=${PROACTIVE_CONSENT_PAUSE_REASON}
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
      update proactive_work set status='active',next_check_at=${now},last_error=null
      where household_id=${householdId} and visibility='household'
        and kind in ('family_calendar_poll','finite_monitor','interest_monitor') and status='paused'
        and last_error=${PROACTIVE_CONSENT_PAUSE_REASON}
    `;
  } else {
    await sql`
      update proactive_work set status='paused',next_check_at=null,
        last_error=${PROACTIVE_CONSENT_PAUSE_REASON}
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
      and m.direction='outbound' and m.status in ('pending','sending')
      and jsonb_typeof(s.metadata->'privateConflictOwnerAdultIds')='array'
      and (s.metadata->'privateConflictOwnerAdultIds') ? ${adultId}
  `;
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
