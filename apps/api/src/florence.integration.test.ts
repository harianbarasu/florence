import { createHash, createHmac, randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { EncryptedImageVault } from "@florence/artifacts";
import { migrateDatabase, migrationFiles, PostgresFlorenceStore } from "@florence/database";
import {
  type GmailAttachmentReference,
  GOOGLE_SCOPES,
  type GoogleCalendarExecutionResult,
  GoogleCalendarTransientError,
  type GoogleConnection,
  GoogleConnectionError,
  type GoogleFamilyCalendarProvisioningResult,
  type GoogleFamilyCalendarRenameResult,
  GoogleFamilyCalendarTransientError,
} from "@florence/google";
import {
  type LinqClient,
  type LinqConversationAuthority,
  type LinqCreateChat,
  type LinqCreatedChat,
  LinqError,
  type LinqMediaReference,
  type LinqSendMessage,
  type LinqSendReaction,
  linqIdentitySubjectDigest,
} from "@florence/linq";
import { describe, expect, onTestFinished, test } from "vitest";
import { buildApp, createSessionCallerResolver } from "./app.js";
import { EnrollmentCodes } from "./enrollment.js";
import { Florence } from "./florence.js";
import { createLinqIngress } from "./linq-ingress.js";
import { type FlorenceDecision, FlorenceReasoner, FlorenceReasonerError } from "./reasoner.js";

const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL;
const NOW = Date.parse("2026-08-16T18:00:00.000Z");
const ENROLLMENT_SECRET = "release-journey-secret-is-at-least-thirty-two-bytes";
const SESSION_SECRET = "release-journey-browser-session-secret-is-at-least-thirty-two-bytes";
const FLORENCE_PHONE = "+15555550000";
const FOUNDER_PHONE = "+15555550101";
const PARTNER_PHONE = "+15555550202";
const OUTSIDER_PHONE = "+15555550999";
const FOUNDER_HANDLE = "messages-founder";
const PARTNER_HANDLE = "messages-partner";
const FOUNDER_GROUP_HANDLE = "messages-founder-family-group";
const PARTNER_GROUP_HANDLE = "messages-partner-family-group";
const FOUNDER_REPLACEMENT_GROUP_HANDLE = "messages-founder-family-group-replacement";
const PARTNER_REPLACEMENT_GROUP_HANDLE = "messages-partner-family-group-replacement";
const FOUNDER_IDENTITY = linqIdentitySubjectDigest(FOUNDER_HANDLE);
const PARTNER_IDENTITY = linqIdentitySubjectDigest(PARTNER_HANDLE);
const FOUNDER_GROUP_IDENTITY = linqIdentitySubjectDigest(FOUNDER_GROUP_HANDLE);
const PARTNER_GROUP_IDENTITY = linqIdentitySubjectDigest(PARTNER_GROUP_HANDLE);
const FOUNDER_REPLACEMENT_GROUP_IDENTITY = linqIdentitySubjectDigest(FOUNDER_REPLACEMENT_GROUP_HANDLE);
const PARTNER_REPLACEMENT_GROUP_IDENTITY = linqIdentitySubjectDigest(PARTNER_REPLACEMENT_GROUP_HANDLE);
const PRIVATE_FOUNDER = "linq-private-founder";
const PRIVATE_PARTNER = "linq-private-partner";
const FAMILY_GROUP = "linq-family-group";
const REPLACEMENT_GROUP = "linq-family-group-replacement";
const FAMILY_CALENDAR = "anbarasu-family@group.calendar.google.com";
const FOUNDER_GOOGLE = "44444444-4444-4444-8444-444444444444";
const PARTNER_GOOGLE = "44444444-4444-4444-8444-444444444445";
const RECONNECTED_FOUNDER_GOOGLE = "44444444-4444-4444-8444-444444444446";
const POST_DELETION_FOUNDER_GOOGLE = "44444444-4444-4444-8444-444444444447";
const WRONG_FOUNDER_GOOGLE = "44444444-4444-4444-8444-444444444448";
const CANCELLED_FOUNDER_GOOGLE = "44444444-4444-4444-8444-444444444449";
const SAFE_REAUTHORIZED_FOUNDER_GOOGLE = "44444444-4444-4444-8444-444444444450";
const ABANDONED_FOUNDER_GOOGLE = "44444444-4444-4444-8444-444444444451";
const LINQ_PARTNER = "partner-florence";
const LINQ_SIGNING_KEY = Buffer.from("florence-release-webhook-key-32b", "utf8");
const LINQ_SIGNING_SECRET = `whsec_${LINQ_SIGNING_KEY.toString("base64")}`;
const INVITE_APPROVAL = "Yes, please text Alex.";
const REINVITE_APPROVAL = "Please invite Alex again.";
const PARTNER_SETUP_QUESTION = "What is this setup for?";
const PARTNER_SETUP_HANDSHAKE_REPLY = "Hi Florence";
const PARTNER_SETUP_REFUSAL = "I don’t want to join this.";
const PARTNER_SETUP_EXPLANATION =
  "That link sets up your own private side of Florence. Use the setup link just above when you’re ready.";
const PARTNER_SETUP_HANDSHAKE_ACK = "Thanks—here’s your private setup link.";
const PARTNER_SETUP_EXPIRED_REPLY =
  "That Florence setup link has expired. Ask your partner to send a fresh invitation.";
const PARTNER_SETUP_EXPIRED_NOTICE =
  "Alex’s Florence setup link expired, so I stopped the invitation. I won’t message them again unless you ask me to send a fresh one.";
const PARTNER_SETUP_DELIVERY_FAILURE_NOTICE =
  "I couldn’t deliver Alex’s Florence setup link, so I stopped the invitation. I won’t message them again unless you ask me to try again.";
const PARTNER_SETUP_COMPLETE_ACK =
  "Your side is all set, Alex. I’m finishing the shared family setup now, and I’ll let you both know in the family thread when it’s ready.";
const FOUNDER_SETUP_COMPLETE_ACK = "Your side is ready, Hari.";
const NATIVE_TEXT = "Forwarded from school: Maya’s field-trip form is due Tuesday.";
const NATIVE_LINK = "https://school.example/fall-field-trip";
const VOICE_TRANSCRIPT = "The teacher said the form still needs one parent signature.";
const INTEREST_REQUEST = "Maya likes soccer. Keep an eye out for a good family match we could attend.";
const INTEREST_RECOMMENDATION =
  "The Bay City women’s match this Saturday fits the family calendar and looks worth considering.";
const INTEREST_URL = "https://example.com/bay-city-family-soccer";
const PUBLIC_RESEARCH_REQUEST =
  "My wife Alex Anbarasu’s flight is delayed tonight. Can you find the best alternatives? DL 747 is her original flight. Email me at hari@example.com.";
const PUBLIC_RESEARCH_REPLY =
  "DL 747 is JFK to LAX. The nonstop is delayed, and the earliest practical Delta alternative leaves later tonight.";
const PUBLIC_RESEARCH_URL = "https://www.delta.com/flight-status/search";
const PUBLIC_NO_RESULT_REQUEST = "Can you verify public product identifier 9780143127796 on 2026-08-27?";
const PUBLIC_NO_RESULT_REPLY =
  "I checked, but I couldn’t verify a useful public match for that identifier and date.";
const PUBLIC_NO_RESULT_SOURCE = "https://example.com/products/9780143127796";
const PUBLIC_SHORT_IDENTIFIER_REQUEST = "Search X";
const PUBLIC_SHORT_IDENTIFIER_REPLY = "X is the public social platform formerly known as Twitter.";
const PUBLIC_SHORT_IDENTIFIER_URL = "https://x.com/";
const PUBLIC_CONCEPT_REQUEST =
  "What is an access token, what are the best password managers, and what is confirmation code format? " +
  "https://www.youtube.com/watch?v=dQw4w9WgXcQ";
const PUBLIC_CONCEPT_REPLY = "Those are public security concepts, and the supplied video is identifiable.";
const PUBLIC_CONCEPT_URL = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";
const PRIVATE_ONLY_PUBLIC_RESEARCH_REQUEST =
  `Search for Hari Anbarasu at hari@example.com, ${FOUNDER_PHONE}, 310-555-1212, ` +
  "http://localhost:3000/reset?token=secret123, " +
  "https://user:pass@example.com/reset?token=secret456, " +
  "file:///Users/Hari/secrets.txt, ftp://user:pass@10.0.0.2/file, " +
  "localhost:3000/private?token=secret789, http://[::ffff:127.0.0.1]/private, " +
  "confirmation code ABC123.";
const PRIVATE_ONLY_PUBLIC_RESEARCH_REPLY =
  "I can’t put those private details into public search. What public subject should I look up?";
const CLARIFICATION_ONLY_REQUEST = "Can you compare the two choices?";
const CLARIFICATION_ONLY_REPLY = "Which two choices do you mean?";
const GROUP_REPAIR_NOTICE =
  "The people in our family thread changed, so I stopped using it. I’ll make a fresh thread with just the two of you.";
const PRIVATE_SCHOOL_FACT_SLOT = "child:maya:school";
const INITIAL_PRIVATE_SCHOOL_FACT = "Maya attends Muir Elementary.";
const UPDATED_PRIVATE_SCHOOL_FACT = "Maya attends Muir Academy.";
const SHARED_SCHOOL_CONTACT_SLOT = "child:maya:school_office_contact";
const SHARED_SCHOOL_CONTACT_FACT = "Maya’s school office is the family’s primary school contact.";
const GOOGLE_CORRECTION_SLOT = "child:maya:school_office_phone";
const GOOGLE_CORRECTION_FACT = "Maya’s school office phone ends in 1000.";
const GOOGLE_CORRECTED_FACT = "Maya’s school office phone ends in 2000.";
const ORDINARY_UNUSED_GMAIL_QUERY = "ordinary-unused-family-email";
const ORDINARY_UNUSED_GMAIL_QUESTION = "Is there anything useful in that family email?";
const GOOGLE_CITED_REPLY_QUERY = "latest-school-office-update";
const GOOGLE_CITED_REPLY_QUESTION = "What did the latest school office email say?";
const GOOGLE_CITED_REPLY = "The school office says Maya’s emergency card still needs a signature.";
const GOOGLE_MEMORY_REPLY_QUESTION = "What school did you already have saved for Maya?";
const GOOGLE_MEMORY_REPLY = "I have Maya at Muir Academy.";
const WEB_CALENDAR_ACCESS_REQUEST = "Can you send me a fresh link to my Florence calendar?";
const WEB_ACCESS_FOLLOW_UP = "Thanks — is that private to me?";
const INCOMPLETE_SETUP_FRESH_LINK_REQUEST = "Can you send me a new link?";
const INCOMPLETE_SETUP_FALSE_DENIAL =
  "I can’t resend a setup link from here. If you’re already on the setup page, keep going there.";
const INCOMPLETE_SETUP_FRESH_LINK_ACKNOWLEDGEMENT = "Of course—here’s a fresh link to finish setup.";
const PRIVATE_INITIAL_ALL_CLEAR =
  "I finished reviewing the last 90 days of your Gmail and Calendar. Nothing needs attention right now.";
const PAGINATED_CALENDAR_TITLE = "Maya’s archived school open-house planning note";
const PAGINATED_CALENDAR_FOLLOW_UP = "Maya’s school open-house plan still needs a family decision.";
const HOUSEHOLD_INITIAL_ALL_CLEAR =
  "I finished reviewing both parents’ last 90 days of Gmail and Calendar. Nothing needs attention right now, and I’ll keep watching.";
const CONVERSATION_RECOVERY_REPLY =
  "I hit a snag answering that. I didn’t make or send any changes—please try once more.";
const TRANSIENT_RETRY_REQUEST = "Can you check in with me?";
const TRANSIENT_RETRY_CUE = "I hit a temporary snag. I’m trying again now.";
const TRANSIENT_RETRY_REPLY = "I’m back—what would you like me to check?";
const STALE_RECEIPT_QUESTION = "Can you confirm this delivery is current?";
const STALE_RECEIPT_REPLY = "This reply must have a current provider receipt.";
const ONE_SHOT_REMINDER_REQUEST = "Remind me to pick up the kids at 2:45 today.";
const ONE_SHOT_REMINDER_ACK = "Absolutely—I’ll remind you to pick up the kids at 2:45 PM.";
const ONE_SHOT_REMINDER_ACTION = "pick up the kids";
const ONE_SHOT_REMINDER_TEXT = "Reminder: pick up the kids.";
const ONE_SHOT_REMINDER_AT = "2026-08-19T21:45:00.000Z";
const PRIVATE_CALENDAR_ONLY_TITLE = "Maya’s soccer clinic";
const PRIVATE_CALENDAR_CONFLICT_TITLE = "School volunteer shift";
const PRIVATE_CALENDAR_ANNIVERSARY_TITLE = "Private anniversary dinner";
const PRIVATE_CALENDAR_ADULT_TITLE = "Private medical appointment";
const FAMILY_CALENDAR_MIXED_CHANGE_TITLE = "Maya’s school photo day";
const FAMILY_CALENDAR_MIXED_CHANGE_SUMMARY = "Maya’s school photo day is September 3.";
const PRIVATE_CALENDAR_OWNER_APPROVAL = "Yes, add that exact event.";
const PRIVATE_CALENDAR_GENERIC_TODAY_REPLY = "I confirmed a private calendar commitment today.";
const PRIVATE_CALENDAR_FACT_SLOT = "child:maya:private_soccer_clinic";
const PRIVATE_CALENDAR_FACT = "Maya’s private soccer clinic is on the parent’s calendar.";
const OVERLAP_GMAIL_SUBJECT = "School bus route reminder";
const OVERLAP_GMAIL_FACT_SLOT = "child:maya:school_bus";
const PARTNER_PRIVATE_GOOGLE_FACT_SLOT = "child:maya:partner_school_handoff";
const PARTNER_PRIVATE_GOOGLE_FACT = "Alex’s recurring school pickup handoff is Friday.";
const GOOGLE_DELETION_GMAIL_SUBJECT = "Maya emergency-card reminder";
const GOOGLE_DELETION_FACT_SLOT = "child:maya:school_office_hours";
const GOOGLE_DELETION_FACT = "Muir Elementary’s school office closes at 4:00 p.m.";
const GOOGLE_DELETION_PRIVATE_ALERT = "Muir says Maya’s emergency card still needs a signature.";
const UNRELATED_ACCOUNT_EMAIL_SUBJECT = "Your retail account password has changed";
const UNRELATED_ACCOUNT_EMAIL_ALERT =
  "A retail account password-change alert arrived. Verify the change or secure the account.";
const UNRELATED_ACCOUNT_MONITOR_OBJECTIVE =
  "Resolve whether the retail account password change was authorized.";
const UNRELATED_ACCOUNT_FACT_SLOT = "adult:retail_account_security";
const UNRELATED_ACCOUNT_FACT = "The retail account password changed.";
const PRIVATE_INITIAL_ONLY_FINDING = "The school office contact stays private to this parent.";
const DISTINCT_CANDIDATE_SHARED_SUMMARY = "Maya’s school item needs family attention.";
const SHARED_DUPLICATE_CONFLICT_SUMMARY = DISTINCT_CANDIDATE_SHARED_SUMMARY;
const FOUNDER_FORM_SUMMARY = DISTINCT_CANDIDATE_SHARED_SUMMARY;
const PARTNER_PERMISSION_SUMMARY = DISTINCT_CANDIDATE_SHARED_SUMMARY;
const FAMILY_MEETING_SUMMARY = "The family meeting is Tuesday at 8:00 PM.";
const SCHOOL_HANDOFF_SUMMARY = "Friday’s school pickup handoff still needs an owner.";

const AUTOMATIC_FAMILY_DATE = {
  intervalKind: "all_day" as const,
  title: "Maya’s field-trip form deadline",
  startDate: "2026-08-19",
  endDate: "2026-08-20",
  location: "Muir Elementary",
};
const PRE_ACTIVATION_FAMILY_DATE = {
  intervalKind: "all_day" as const,
  title: "Maya’s field-trip volunteer briefing",
  startDate: "2026-08-22",
  endDate: "2026-08-23",
  location: "Muir Elementary",
};
const GOOGLE_DELETION_FAMILY_DATE = {
  intervalKind: "all_day" as const,
  title: "Maya’s emergency-card deadline",
  startDate: "2026-08-24",
  endDate: "2026-08-25",
  location: "Muir Elementary",
};
const PRIVATE_CALENDAR_ONLY_EVENT = {
  providerEventId: "private-calendar-only-soccer-clinic",
  providerRevision: "private-calendar-only-revision-1",
  providerUpdatedAt: "2026-08-23T18:00:00.000Z",
  status: "confirmed" as const,
  busy: true,
  title: PRIVATE_CALENDAR_ONLY_TITLE,
  intervalKind: "timed" as const,
  startsAt: "2026-08-24T14:30:00.000Z",
  endsAt: "2026-08-24T16:00:00.000Z",
  allDay: false,
  timeZone: "America/Los_Angeles",
  location: "Muir Elementary",
};
const PRIVATE_CALENDAR_CONFLICT_EVENT = {
  ...PRIVATE_CALENDAR_ONLY_EVENT,
  providerEventId: "private-calendar-conflict-volunteer-shift",
  providerRevision: "private-calendar-conflict-revision-1",
  title: PRIVATE_CALENDAR_CONFLICT_TITLE,
  startsAt: "2026-08-24T15:00:00.000Z",
  endsAt: "2026-08-24T16:30:00.000Z",
};
const PRIVATE_CALENDAR_ANNIVERSARY_EVENT = {
  ...PRIVATE_CALENDAR_ONLY_EVENT,
  providerEventId: "private-calendar-anniversary-dinner",
  providerRevision: "private-calendar-anniversary-revision-1",
  title: PRIVATE_CALENDAR_ANNIVERSARY_TITLE,
  busy: false,
  startsAt: "2026-08-24T19:00:00.000Z",
  endsAt: "2026-08-24T20:00:00.000Z",
  location: "Private restaurant",
};
const PRIVATE_CALENDAR_ADULT_EVENT = {
  ...PRIVATE_CALENDAR_ANNIVERSARY_EVENT,
  providerEventId: "private-calendar-adult-appointment",
  providerRevision: "private-calendar-adult-appointment-revision-1",
  title: PRIVATE_CALENDAR_ADULT_TITLE,
  startsAt: "2026-08-25T19:00:00.000Z",
  endsAt: "2026-08-25T20:00:00.000Z",
  location: "Private clinic",
};
const FAMILY_CALENDAR_MIXED_CHANGE_EVENT = {
  ...PRIVATE_CALENDAR_ONLY_EVENT,
  providerEventId: "family-calendar-school-photo-day",
  providerRevision: "family-calendar-school-photo-day-revision-1",
  title: FAMILY_CALENDAR_MIXED_CHANGE_TITLE,
  startsAt: "2026-09-03T15:00:00.000Z",
  endsAt: "2026-09-03T16:00:00.000Z",
  location: "Muir Elementary",
};
const PRIVATE_INITIAL_CALENDAR_ONLY_EVENT = {
  ...PRIVATE_CALENDAR_ONLY_EVENT,
  providerEventId: "private-initial-calendar-only-soccer-clinic",
  providerRevision: "private-initial-calendar-only-revision-1",
  providerUpdatedAt: "2026-08-16T18:00:00.000Z",
  startsAt: "2026-08-17T14:30:00.000Z",
  endsAt: "2026-08-17T16:00:00.000Z",
};
const PRIVATE_INITIAL_CALENDAR_CONFLICT_EVENT = {
  ...PRIVATE_CALENDAR_CONFLICT_EVENT,
  providerEventId: "private-initial-calendar-conflict-volunteer-shift",
  providerRevision: "private-initial-calendar-conflict-revision-1",
  providerUpdatedAt: "2026-08-16T18:00:00.000Z",
  startsAt: "2026-08-17T15:00:00.000Z",
  endsAt: "2026-08-17T16:30:00.000Z",
};
const PICKUP_EVENT = {
  intervalKind: "timed" as const,
  title: "Maya pickup",
  startsAt: "2026-08-18T21:45:00.000Z",
  endsAt: "2026-08-18T22:00:00.000Z",
  timeZone: "America/Los_Angeles",
  location: "Muir Elementary",
};
const UPDATED_PICKUP_EVENT = {
  ...PICKUP_EVENT,
  startsAt: "2026-08-18T22:00:00.000Z",
  endsAt: "2026-08-18T22:15:00.000Z",
};
const JPEG_BYTES = Uint8Array.from([0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46]);
const PDF_BYTES = new TextEncoder().encode("%PDF-1.7\nMuir field-trip form. Return by Tuesday.\n%%EOF\n");
const WAV_BYTES = Uint8Array.from([
  0x52, 0x49, 0x46, 0x46, 0x25, 0x00, 0x00, 0x00, 0x57, 0x41, 0x56, 0x45, 0x66, 0x6d, 0x74, 0x20, 0x10, 0x00,
  0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x40, 0x1f, 0x00, 0x00, 0x80, 0x3e, 0x00, 0x00, 0x02, 0x00, 0x10, 0x00,
  0x64, 0x61, 0x74, 0x61, 0x01, 0x00, 0x00, 0x00, 0x00,
]);
const SCHOOL_ATTACHMENT: GmailAttachmentReference = {
  messageId: "gmail-school-form",
  threadId: "gmail-school-thread",
  historyId: "101",
  partId: "2",
  attachmentId: "gmail-school-pdf",
  storage: "external",
  filename: "field-trip-form.pdf",
  mimeType: "application/pdf",
  sizeBytes: PDF_BYTES.byteLength,
};

type Reason = FlorenceReasoner["decide"];
type CalendarExecutionInput = Parameters<GoogleConnection["executeCalendar"]>[0];
type CalendarMutation = CalendarExecutionInput["mutation"];
type FamilyCalendarProvisioningInput = Parameters<GoogleConnection["provisionFamilyCalendar"]>[0];
type CalendarReadInput = Parameters<GoogleConnection["readCalendarWindow"]>[0];
type TestPart =
  | { type: "text" | "link"; value: string }
  | {
      type: "media";
      id: string;
      filename: string;
      mimeType: string;
      bytes: Uint8Array;
    };
type FakeCalendarEvent = {
  providerEventId: string;
  providerRevision: string;
  providerUpdatedAt: string;
  status: "confirmed";
  busy: true;
  title: string;
  location: string | null;
} & (
  | {
      intervalKind: "timed";
      allDay: false;
      startsAt: string;
      endsAt: string;
      timeZone: string;
    }
  | {
      intervalKind: "all_day";
      allDay: true;
      startDate: string;
      endDate: string;
    }
);
type HarnessState = {
  now: number;
  privateReviews: Parameters<FlorenceReasoner["classifyPrivateGoogleBatch"]>[0][];
  googleAssessments: Parameters<FlorenceReasoner["assessGoogleChanges"]>[0][];
  briefings: Parameters<FlorenceReasoner["synthesizeHouseholdBriefing"]>[0][];
  provisionings: FamilyCalendarProvisioningInput[];
  calendarRenames: Parameters<GoogleConnection["renameFamilyCalendar"]>[0][];
  providerCalendarSummary: string | null;
  calendarExecutions: CalendarExecutionInput[];
  calendarReads: CalendarReadInput[];
  calendarEvents: Map<string, FakeCalendarEvent>;
  uncertainCalendarCreateTitle: string | null;
  timeline: string[];
  finiteReviews: number;
  interestResearches: number;
  voiceTranscriptions: number;
  initialGoogleFailuresRemaining: number;
  initialClassifierFailuresRemaining: number;
  initialGoogleFailureAdultId: string | null;
  completeScanPaginationExercise: boolean;
  wrongGoogleSubjectNext: boolean;
  baselinePageReads: {
    kind: "gmail" | "calendar_targets" | "calendar_events";
    ownerAdultId: string;
    connectionId: string;
    pageToken: string | null;
    calendarId: string | null;
  }[];
  initialHouseholdCalendarFailuresRemaining: number;
  privateFactUpdatePending: boolean;
  privateFactUpdateDelivered: boolean;
  overlapGmailReadsRemaining: number;
  overlapGmailAssessments: number;
  overlapGmailSourceId: string | null;
  monitorEvidenceExercise: boolean;
  monitorCancellationActive: boolean;
  silentMonitorSourceId: string | null;
  voicedMonitorSourceId: string | null;
  cancelledMonitorSourceId: string | null;
  setupConversationFailuresRemaining: number;
  founderProductRecenterReview: boolean;
  familyCalendarProvisioningFailuresRemaining: number;
  invalidGrantAdultId: string | null;
  invalidGrantTriggered: boolean;
  googleDeletionEvidencePending: boolean;
  googleDeletionEvidenceDelivered: boolean;
  googleDeletionSourceId: string | null;
  googleChangeReads: { ownerAdultId: string; kind: "gmail" | "calendar" }[];
  interactiveGoogleReads: number;
  providerRevocations: ("confirmed" | "unconfirmed" | "not-needed")[];
  setupConversations: Parameters<FlorenceReasoner["converseDuringSetup"]>[0][];
  initialNoAttentionReview: boolean;
  initialCalendarOnlyReview: boolean;
  initialUnrelatedAccountReview: boolean;
  initialUnrelatedAccountFactOnlyReview: boolean;
  calendarOnlyChangePending: boolean;
  calendarOnlyChangeDelivered: boolean;
  privateCalendarAnniversaryPending: boolean;
  privateCalendarAnniversaryDelivered: boolean;
  familyCalendarEchoPending: boolean;
  familyCalendarEchoDelivered: boolean;
  familyCalendarMixedChangePending: boolean;
  familyCalendarMixedChangeDelivered: boolean;
  familyCalendarRealOnlyOverlapPending: boolean;
  familyCalendarRealOnlyOverlapDelivered: boolean;
  privateCalendarAdultEventPending: boolean;
  privateCalendarAdultEventDelivered: boolean;
  unrelatedAccountEmailPending: boolean;
  unrelatedAccountEmailDelivered: boolean;
};

const release = TEST_DATABASE_URL ? describe : describe.skip;

release("Florence parent journeys", () => {
  test("keeps private setup useful and recovers one two-parent family loop without duplicate work", async () => {
    let accessFollowUpHistory: readonly string[] = [];
    let accessFollowUpAuthoredText: string | null = null;
    let failNextReinviteConversation = false;
    let contradictNextSuccessfulPartnerInvitation = false;
    let failNextGroupGreeting = false;
    let transientRetryAttempts = 0;
    const harness = await createHarness(async (input) => {
      if (input.currentMessage.text === TRANSIENT_RETRY_REQUEST) {
        transientRetryAttempts += 1;
        if (transientRetryAttempts === 1) {
          throw new FlorenceReasonerError("transient", "Fake temporary model failure");
        }
        return decision({ bubbles: [{ text: TRANSIENT_RETRY_REPLY, delayMs: 0 }] });
      }
      if (
        failNextGroupGreeting &&
        input.audience === "group" &&
        input.currentMessage.text === "Hi Florence"
      ) {
        failNextGroupGreeting = false;
        throw new FlorenceReasonerError(
          "invalid_output",
          "Fake normal family-thread greeting returned no usable decision",
        );
      }
      if (failNextReinviteConversation && input.currentMessage.text === REINVITE_APPROVAL) {
        failNextReinviteConversation = false;
        throw new FlorenceReasonerError(
          "invalid_output",
          "Fake normal conversation output failed after isolated invitation approval",
        );
      }
      if (contradictNextSuccessfulPartnerInvitation && input.currentMessage.text === REINVITE_APPROVAL) {
        contradictNextSuccessfulPartnerInvitation = false;
        return decision({
          bubbles: [
            {
              text: "I can’t send a setup text myself, but Alex can open Florence from their own phone to get started.",
              delayMs: 0,
            },
          ],
        });
      }
      if (input.currentMessage.text === WEB_CALENDAR_ACCESS_REQUEST) {
        return decision({
          bubbles: [{ text: "Here’s a fresh private link.", delayMs: 0 }],
          webAccessPath: "/calendar",
        });
      }
      if (input.currentMessage.text === ONE_SHOT_REMINDER_REQUEST) {
        return decision({
          bubbles: [{ text: ONE_SHOT_REMINDER_ACK, delayMs: 0 }],
          followUp: {
            operation: "remind",
            followUpId: null,
            reminderAt: ONE_SHOT_REMINDER_AT,
            reminderAction: ONE_SHOT_REMINDER_ACTION,
            sourceIds: [input.currentMessage.sourceId],
          },
        });
      }
      if (input.currentMessage.text.startsWith(WEB_ACCESS_FOLLOW_UP)) {
        accessFollowUpHistory = input.recentMessages.map((message) => message.text);
        accessFollowUpAuthoredText = input.currentMessage.authoredText;
      }
      return decision();
    });
    harness.state.founderProductRecenterReview = true;
    await harness.setupFounder();
    await harness.accept("private", "resume-incomplete-setup", "How do I finish connecting Google?");
    await harness.drain();
    const setupAccessUrl = harness.accessLinkFor(PRIVATE_FOUNDER);
    expect(setupAccessUrl.pathname).toBe("/");
    expect(setupAccessUrl.hash).toMatch(/^#a=wa1\./);
    expect(
      harness.linq.messages.some(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER && message.text === "https://florence.test/",
      ),
    ).toBe(false);
    const setupAccessApp = await harness.webApp(false);
    const setupAccessResponse = await setupAccessApp.inject({
      method: "POST",
      url: "/api/v1/session",
      payload: { accessToken: new URLSearchParams(setupAccessUrl.hash.slice(1)).get("a") },
    });
    await setupAccessApp.close();
    expect(setupAccessResponse.statusCode).toBe(200);
    expect(setupAccessResponse.json()).toEqual({ adultId: harness.founderAdultId, accessPath: "/" });
    const compoundSurnamePreview = await harness.florence.putMember(
      harness.founderAdultId,
      harness.founderAdultId,
      { lastName: "De la Cruz" },
    );
    expect(compoundSurnamePreview.viewer).toMatchObject({
      displayName: "Hari De la Cruz",
      lastName: "De la Cruz",
    });
    await harness.activateFounderGoogle();
    const setupMessagesBeforeFreshLinkRequest = harness.linq.messages.length;
    const setupLinksBeforeFreshLinkRequest = harness.accessLinksFor(PRIVATE_FOUNDER).length;
    const priorSetupAccessUrl = harness.accessLinkFor(PRIVATE_FOUNDER).toString();
    await harness.accept("private", "fresh-incomplete-setup-link", INCOMPLETE_SETUP_FRESH_LINK_REQUEST);
    await harness.drain();
    const freshSetupMessages = harness.linq.messages.slice(setupMessagesBeforeFreshLinkRequest);
    expect(harness.state.setupConversations.at(-1)).toMatchObject({
      stage: "family_profile",
      currentMessage: { text: INCOMPLETE_SETUP_FRESH_LINK_REQUEST },
      nextStep: "finish_family_profile",
    });
    expect(freshSetupMessages.map((message) => message.text)).toContain(
      INCOMPLETE_SETUP_FRESH_LINK_ACKNOWLEDGEMENT,
    );
    expect(freshSetupMessages.some((message) => message.text === INCOMPLETE_SETUP_FALSE_DENIAL)).toBe(false);
    expect(harness.accessLinksFor(PRIVATE_FOUNDER)).toHaveLength(setupLinksBeforeFreshLinkRequest + 1);
    expect(harness.accessLinkFor(PRIVATE_FOUNDER)).toMatchObject({ pathname: "/" });
    expect(harness.accessLinkFor(PRIVATE_FOUNDER).toString()).not.toBe(priorSetupAccessUrl);
    await expect(
      harness.florence.completeFamilyOnboarding(harness.founderAdultId, {
        ...familyProfileInput(),
        familyLabel: "Client-chosen Family",
      } as never),
    ).rejects.toThrow();
    const founderMessagesBeforeFamilyProfile = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_FOUNDER,
    ).length;
    await harness.completeFamilyProfile();
    expect(
      harness.linq.messages
        .filter((message) => message.providerConversationId === PRIVATE_FOUNDER)
        .slice(founderMessagesBeforeFamilyProfile)
        .map((message) => message.text),
    ).toEqual([FOUNDER_SETUP_COMPLETE_ACK]);
    const founderAfterFamilyProfile = await harness.florence.workspaceForAdult(harness.founderAdultId);
    expect(founderAfterFamilyProfile.vault?.members).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: "adult",
          firstName: "Hari",
          lastName: "De la Cruz",
          displayName: "Hari De la Cruz",
          relationship: "Parent",
        }),
        expect.objectContaining({
          kind: "child",
          firstName: "Maya",
          lastName: "Anbarasu",
          displayName: "Maya Anbarasu",
          relationship: "Child",
          age: 8,
          grade: "3rd grade",
        }),
      ]),
    );
    const mayaId = founderAfterFamilyProfile.vault?.members.find(
      (member) => member.kind === "child" && member.firstName === "Maya",
    )?.id;
    if (!mayaId) throw new Error("Family setup did not create Maya");
    await harness.drain();

    expect(harness.state.privateReviews.map((review) => review.adult.firstName)).toEqual(["Hari"]);
    expect(harness.state.privateReviews[0]?.familyProfile.children).toEqual(
      expect.arrayContaining([expect.objectContaining({ firstName: "Maya", age: 8, grade: "3rd grade" })]),
    );
    await expect(harness.florence.putMember(harness.founderAdultId, mayaId, { age: 121 })).rejects.toThrow();
    let founderAfterChildEdit = await harness.florence.putMember(harness.founderAdultId, mayaId, {
      age: 9,
      grade: "4th grade",
    });
    expect(founderAfterChildEdit.vault?.members.find((member) => member.id === mayaId)).toMatchObject({
      age: 9,
      grade: "4th grade",
    });
    founderAfterChildEdit = await harness.florence.putMember(harness.founderAdultId, mayaId, {
      age: null,
      grade: null,
    });
    const mayaAfterClear = founderAfterChildEdit.vault?.members.find((member) => member.id === mayaId);
    expect(mayaAfterClear).not.toHaveProperty("age");
    expect(mayaAfterClear).not.toHaveProperty("grade");
    founderAfterChildEdit = await harness.florence.putMember(harness.founderAdultId, mayaId, {
      age: 9,
      grade: "4th grade",
    });
    expect(founderAfterChildEdit.vault?.members.find((member) => member.id === mayaId)).toMatchObject({
      age: 9,
      grade: "4th grade",
    });
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER &&
          message.text.includes("What needs attention:") &&
          message.text.includes("Hari’s private school email has the original form."),
      ),
    ).toHaveLength(1);
    const founderBeforePartner = await harness.florence.workspaceForAdult(harness.founderAdultId);
    expect(founderBeforePartner.workspace.setup.initialBriefing).toBe("not_ready");
    expect(founderBeforePartner.vault?.facts).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          statement: INITIAL_PRIVATE_SCHOOL_FACT,
          visibility: "household",
          source: expect.objectContaining({ kind: "gmail" }),
        }),
      ]),
    );
    expect(founderBeforePartner.vault?.watches).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: "monitor",
          objective: "Watch for confirmation that Maya’s field-trip form is signed.",
          status: "active",
          visibility: "private",
        }),
      ]),
    );
    await harness.assertDatabase(
      "The founder review did not complete before partner activation",
      `(select count(*)=1 from proactive_work where kind='initial_private_review' and status='completed')`,
    );
    await harness.assertDatabase(
      "The founder's personal Google poll did not start after the initial review",
      `(select count(*)=1 from proactive_work where kind='personal_google_poll' and status='active')`,
    );
    await harness.assertDatabase(
      "The founder review did not retain both expected follow-throughs",
      `(select count(*)=2 from proactive_work where kind='finite_monitor' and status='active')`,
    );
    await harness.assertDatabase(
      "The founder review staged a family Calendar action before family activation",
      `not exists (select 1 from calendar_actions)`,
    );
    expect(harness.linq.createdChats).toHaveLength(0);
    expect(harness.state.calendarExecutions).toHaveLength(0);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER && message.text === FOUNDER_SETUP_COMPLETE_ACK,
      ),
    ).toHaveLength(1);
    expect(
      harness.linq.messages.some((message) =>
        message.text.includes("I’ll use your Gmail and calendar to catch school dates"),
      ),
    ).toBe(false);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER &&
          message.text === "Want me to text Alex at ••••0202 so they can set up their side?",
      ),
    ).toHaveLength(1);

    const founderMessagesBeforePermanentFailure = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_FOUNDER,
    ).length;
    harness.linq.partnerChatFailuresRemaining = 1;
    await harness.accept("private", "approve-partner", INVITE_APPROVAL);
    await harness.drain();
    expect(
      harness.linq.createdChats.filter((chat) => chat.result.authority.audience === "private"),
    ).toHaveLength(0);
    const permanentFailureNotices = harness.linq.messages
      .filter((message) => message.providerConversationId === PRIVATE_FOUNDER)
      .slice(founderMessagesBeforePermanentFailure);
    expect(permanentFailureNotices.map((message) => message.text)).toEqual([
      "Got it—I’ll text Alex now.",
      PARTNER_SETUP_DELIVERY_FAILURE_NOTICE,
    ]);
    expect((await harness.florence.workspaceForAdult(harness.founderAdultId)).workspace.setup).toMatchObject({
      partnerInvitation: "ready",
    });
    const linkAttemptsAfterPermanentFailure = harness.linq.partnerSetupLinkAttempts;
    expect(linkAttemptsAfterPermanentFailure).toBe(0);
    await harness.assertDatabase(
      "A pre-chat invitation failure fabricated provider history",
      `exists (
        select 1 from people where adult_slot=2 and status='planned'
          and invitation_consumed_at is not null
          and invitation_conversation_id is null and invitation_identity_digest is null
          and invitation_message_id is null and invitation_issued_at is null
      )`,
    );
    harness.state.now += 15_001;
    await harness.drain();
    expect(harness.linq.partnerSetupLinkAttempts).toBe(linkAttemptsAfterPermanentFailure);
    expect(
      harness.linq.messages
        .filter((message) => message.providerConversationId === PRIVATE_FOUNDER)
        .slice(founderMessagesBeforePermanentFailure),
    ).toHaveLength(2);

    failNextReinviteConversation = true;
    const founderMessagesBeforeReinvite = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_FOUNDER,
    ).length;
    harness.linq.partnerInitialPromptAcceptedRemaining = 1;
    const stoppedPartnerPromptVisible = harness.linq.pauseNextPartnerInitialPrompt();
    await harness.accept("private", "reinvite-partner-after-rejection", REINVITE_APPROVAL);
    const stoppedInvitationDrain = harness.drain();
    await stoppedPartnerPromptVisible;
    try {
      expect(
        await harness.receiveParts(
          "partner-handshake-stop-before-binding",
          [{ type: "text", value: "STOP" }],
          PRIVATE_PARTNER,
          "partner",
        ),
      ).toEqual({ disposition: "acknowledged", reason: "opted_out" });
    } finally {
      harness.linq.releasePartnerInitialPrompt();
    }
    await stoppedInvitationDrain;
    expect(harness.linq.partnerSetupLinkAttempts).toBe(0);
    expect(
      harness.linq.messages.some(
        (message) => message.providerConversationId === PRIVATE_PARTNER && message.text.includes("#s="),
      ),
    ).toBe(false);
    await harness.assertDatabase(
      "A STOP received before partner binding was forgotten",
      `exists (
        select 1 from people where adult_slot=2 and status='planned'
          and invitation_consumed_at is not null and invitation_approval_source_id is null
          and invitation_digest is null and invitation_expires_at is null
          and messages_address is null and invitation_conversation_id is null
          and invitation_identity_digest is null and invitation_message_id is null
          and invitation_issued_at is null and invitation_retry_at is null
      )`,
    );

    contradictNextSuccessfulPartnerInvitation = true;
    const founderMessagesBeforeSuccessfulReinvite = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_FOUNDER,
    ).length;
    await harness.accept("private", "reinvite-partner-after-fast-stop", REINVITE_APPROVAL);
    await harness.drain();
    const reboundPartnerPrompt = harness.linq.createdChats.at(-1);
    expect(reboundPartnerPrompt).toMatchObject({
      input: {
        participantPhoneNumbers: [PARTNER_PHONE],
        initialText: expect.stringMatching(/Reply here.*private setup link/i),
      },
      result: { providerConversationId: PRIVATE_PARTNER, authority: { audience: "private" } },
    });
    expect(reboundPartnerPrompt?.input.initialText).not.toContain("#s=");
    const successfulReinviteFounderMessages = harness.linq.messages
      .filter((message) => message.providerConversationId === PRIVATE_FOUNDER)
      .slice(founderMessagesBeforeSuccessfulReinvite);
    expect(successfulReinviteFounderMessages.map((message) => message.text)).toEqual([
      "Got it—I’ll text Alex now.",
    ]);
    const reinviteFounderMessages = harness.linq.messages
      .filter((message) => message.providerConversationId === PRIVATE_FOUNDER)
      .slice(founderMessagesBeforeReinvite);
    expect(reinviteFounderMessages.map((message) => message.text)).toContain("Got it—I’ll text Alex now.");
    expect(reinviteFounderMessages.some((message) => /finish the rest reliably/i.test(message.text))).toBe(
      false,
    );
    expect((await harness.florence.workspaceForAdult(harness.founderAdultId)).workspace.setup).toMatchObject({
      partnerInvitation: "invited",
    });
    expect(harness.linq.partnerSetupLinkAttempts).toBe(0);
    await harness.assertDatabase(
      "The partner invitation did not pause for an inbound reply before sending the setup link",
      `exists (
        select 1 from people where adult_slot=2 and status='planned'
          and invitation_digest is null and invitation_expires_at is not null
          and invitation_consumed_at is null and messages_address=${sqlLiteral(PARTNER_PHONE)}
          and invitation_conversation_id=${sqlLiteral(PRIVATE_PARTNER)}
          and invitation_identity_digest=${sqlLiteral(PARTNER_IDENTITY)}
          and invitation_message_id is not null and invitation_issued_at is not null
          and invitation_approval_source_id is not null and invitation_approved_at is not null
      )`,
    );
    harness.state.now += 2 * 24 * 60 * 60_000 + 1_000;
    await harness.drain();
    await harness.assertDatabase(
      "The background expiry sweep stopped a partner who had not replied yet",
      `exists (
        select 1 from people where adult_slot=2 and status='planned'
          and invitation_digest is null and invitation_consumed_at is null
          and invitation_conversation_id=${sqlLiteral(PRIVATE_PARTNER)}
          and invitation_approval_source_id is not null
      ) and not exists (
        select 1 from messages where direction='outbound'
          and idempotency_key like 'partner-invitation-expired:%'
      )`,
    );
    harness.linq.partnerSetupLinkState = "accepted";
    const linkSendAttemptsBeforeRetry = harness.linq.sendMessageAttempts.filter((message) =>
      message.text.includes("#s="),
    ).length;
    await expect(
      harness.receiveParts(
        "partner-handshake-delayed-first-reply",
        [{ type: "text", value: PARTNER_SETUP_HANDSHAKE_REPLY }],
        PRIVATE_PARTNER,
        "partner",
      ),
    ).rejects.toMatchObject({ code: "provider_retryable", retryable: true });
    const stagedPartnerSetupToken = harness.setupTokenFor(PRIVATE_PARTNER);
    expect(stagedPartnerSetupToken).toMatch(/^ps1\./);
    harness.state.now += 60_000;
    harness.linq.partnerSetupLinkState = "sent";
    await harness.receiveParts(
      "partner-handshake-delivery-retry",
      [{ type: "text", value: PARTNER_SETUP_HANDSHAKE_REPLY }],
      PRIVATE_PARTNER,
      "partner",
    );
    expect(harness.state.setupConversations.at(-1)).toMatchObject({
      stage: "partner_invited",
      currentMessage: { text: PARTNER_SETUP_HANDSHAKE_REPLY },
      nextStep: "signed_link_will_follow",
    });
    const firstPartnerSetupLinks = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_PARTNER && message.text.includes("#s="),
    );
    expect(firstPartnerSetupLinks).toHaveLength(1);
    const retriedLinkAttempts = harness.linq.sendMessageAttempts
      .filter((message) => message.text.includes("#s="))
      .slice(linkSendAttemptsBeforeRetry);
    expect(retriedLinkAttempts).toHaveLength(2);
    expect(retriedLinkAttempts[1]).toMatchObject({
      idempotencyKey: retriedLinkAttempts[0]?.idempotencyKey,
      text: retriedLinkAttempts[0]?.text,
    });
    expect(
      harness.linq.messages.some(
        (message) =>
          message.providerConversationId === PRIVATE_PARTNER &&
          message.text.includes("invitation expired before you replied"),
      ),
    ).toBe(false);
    expect(firstPartnerSetupLinks[0]?.text).toContain("#s=ps1.");
    expect(firstPartnerSetupLinks[0]?.text).not.toContain("#s=fs2.");
    const expiringPartnerSetupToken = harness.setupTokenFor(PRIVATE_PARTNER);
    expect(expiringPartnerSetupToken).toBe(stagedPartnerSetupToken);
    expect(expiringPartnerSetupToken).toMatch(/^ps1\./);
    await harness.assertDatabase(
      "The retried partner setup link was not confirmed against its original token",
      `exists (
        select 1 from people where adult_slot=2 and status='planned'
          and invitation_digest is not null and invitation_expires_at is not null
          and invitation_consumed_at is null and invitation_retry_at is null
          and invitation_conversation_id=${sqlLiteral(PRIVATE_PARTNER)}
      )`,
    );
    const linkAttemptsBeforeExpiry = harness.linq.partnerSetupLinkAttempts;
    expect(linkAttemptsBeforeExpiry).toBe(2);
    const partnerMessagesBeforeReplyReplay = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_PARTNER,
    ).length;
    await harness.receiveParts(
      "partner-handshake-delayed-first-reply",
      [{ type: "text", value: PARTNER_SETUP_HANDSHAKE_REPLY }],
      PRIVATE_PARTNER,
      "partner",
    );
    expect(harness.linq.partnerSetupLinkAttempts).toBe(linkAttemptsBeforeExpiry);
    expect(
      harness.linq.messages.filter((message) => message.providerConversationId === PRIVATE_PARTNER),
    ).toHaveLength(partnerMessagesBeforeReplyReplay);
    harness.state.now += 24 * 60 * 60_000 + 1_000;
    expect(await harness.redeemPartnerSetup(expiringPartnerSetupToken)).toBeNull();
    const founderMessagesBeforeExpiry = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_FOUNDER,
    ).length;
    await harness.receiveParts(
      "expired-partner-link-question",
      [{ type: "text", value: PARTNER_SETUP_QUESTION }],
      PRIVATE_PARTNER,
      "partner",
    );
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === PRIVATE_PARTNER && message.text === PARTNER_SETUP_EXPIRED_REPLY,
      ),
    ).toHaveLength(1);
    await harness.drain();
    const expirationNotices = harness.linq.messages
      .filter((message) => message.providerConversationId === PRIVATE_FOUNDER)
      .slice(founderMessagesBeforeExpiry)
      .filter((message) => message.text === PARTNER_SETUP_EXPIRED_NOTICE);
    expect(expirationNotices).toHaveLength(1);
    expect(expirationNotices[0]?.text).toBe(PARTNER_SETUP_EXPIRED_NOTICE);
    expect((await harness.florence.workspaceForAdult(harness.founderAdultId)).workspace.setup).toMatchObject({
      partnerInvitation: "ready",
    });
    expect(harness.linq.partnerSetupLinkAttempts).toBe(linkAttemptsBeforeExpiry);
    harness.state.now += 15_001;
    await harness.drain();
    expect(harness.linq.partnerSetupLinkAttempts).toBe(linkAttemptsBeforeExpiry);

    harness.linq.partnerInitialPromptAcceptedRemaining = 1;
    const partnerPromptVisible = harness.linq.pauseNextPartnerInitialPrompt();
    await harness.accept("private", "reinvite-partner-after-expiry", REINVITE_APPROVAL);
    const invitationDrain = harness.drain();
    await partnerPromptVisible;
    try {
      const acceptedPartnerPrompt = harness.linq.createdChats.at(-1);
      expect(acceptedPartnerPrompt).toMatchObject({
        input: {
          participantPhoneNumbers: [PARTNER_PHONE],
          initialText: expect.stringMatching(/Reply here.*private setup link/i),
        },
        result: {
          providerConversationId: PRIVATE_PARTNER,
          authority: { audience: "private" },
          initialMessage: { providerState: "accepted" },
        },
      });
      expect(
        harness.linq.messages.filter(
          (message) => message.idempotencyKey === acceptedPartnerPrompt?.input.idempotencyKey,
        ),
      ).toHaveLength(1);
      await expect(
        harness.receiveParts(
          "partner-handshake-fast-reply-before-binding",
          [{ type: "text", value: PARTNER_SETUP_HANDSHAKE_REPLY }],
          PRIVATE_PARTNER,
          "partner",
        ),
      ).rejects.toMatchObject({ code: "provider_retryable", retryable: true });
      expect(
        harness.linq.messages.some(
          (message) => message.providerConversationId === PRIVATE_PARTNER && message.text.includes("#s=fs2."),
        ),
      ).toBe(false);
      expect(harness.linq.partnerSetupLinkAttempts).toBe(linkAttemptsBeforeExpiry);
    } finally {
      harness.linq.releasePartnerInitialPrompt();
    }
    await invitationDrain;
    expect((await harness.florence.workspaceForAdult(harness.founderAdultId)).workspace.setup).toMatchObject({
      partnerInvitation: "invited",
    });
    const partnerSetupLinksBeforeFastReply = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_PARTNER && message.text.includes("#s="),
    ).length;
    await harness.receiveParts(
      "partner-handshake-fast-reply-before-binding",
      [{ type: "text", value: PARTNER_SETUP_HANDSHAKE_REPLY }],
      PRIVATE_PARTNER,
      "partner",
    );
    expect(harness.linq.partnerSetupLinkAttempts).toBe(linkAttemptsBeforeExpiry + 1);
    const finalPartnerSetupToken = harness.setupTokenFor(PRIVATE_PARTNER);
    expect(finalPartnerSetupToken).not.toBe(expiringPartnerSetupToken);
    expect(finalPartnerSetupToken).toMatch(/^ps1\./);
    const fastReplySetupLinks = harness.linq.messages
      .filter((message) => message.providerConversationId === PRIVATE_PARTNER && message.text.includes("#s="))
      .slice(partnerSetupLinksBeforeFastReply);
    expect(fastReplySetupLinks).toHaveLength(1);
    expect(fastReplySetupLinks[0]?.text).toContain("#s=ps1.");
    expect(
      harness.linq.messages.some(
        (message) => message.providerConversationId === PRIVATE_PARTNER && message.text.includes("#s=fs2."),
      ),
    ).toBe(false);

    harness.linq.familyGroupPromptAcceptedRemaining = 1;
    const partnerSetupApp = await harness.webApp(false);
    const partnerProfile = {
      firstName: "Alex",
      lastName: "Anbarasu",
      timeZone: "America/Los_Angeles",
      guardianAttested: true,
      proactiveUseAccepted: true,
      privateConflictBusySharingEnabled: true,
    };
    const collidingFounderSetup = harness.enrollmentCodes.issueFounderSetup({
      providerConversationId: PRIVATE_PARTNER,
      identitySubjectDigest: PARTNER_IDENTITY,
      occurredAt: harness.iso(),
    });
    const collidingFounderResponse = await partnerSetupApp.inject({
      method: "POST",
      url: "/api/v1/session",
      payload: { setupToken: collidingFounderSetup.token, profile: partnerProfile },
    });
    expect(collidingFounderResponse.statusCode).toBe(401);
    expect(collidingFounderResponse.json()).toEqual({ error: "invalid_or_expired_setup_link" });
    expect(collidingFounderResponse.body).not.toContain("internal_error");
    await harness.assertDatabase(
      "A founder token for the reserved partner identity mutated the family",
      `(select count(*)=1 from households)
        and (select count(*)=2 from people where kind='adult')
        and (select count(*)=1 from people where kind='adult' and adult_slot=2 and status='planned')`,
    );
    const firstPartnerSetupResponse = await partnerSetupApp.inject({
      method: "POST",
      url: "/api/v1/session",
      payload: { setupToken: finalPartnerSetupToken, profile: partnerProfile },
    });
    const repeatedPartnerSetupResponse = await partnerSetupApp.inject({
      method: "POST",
      url: "/api/v1/session",
      payload: { setupToken: finalPartnerSetupToken, profile: partnerProfile },
    });
    await partnerSetupApp.close();
    for (const response of [firstPartnerSetupResponse, repeatedPartnerSetupResponse]) {
      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ adultId: harness.partnerAdultId });
      expect(response.body).not.toContain("internal_error");
    }
    const founderHouseholds = await harness.store.listHouseholdIdsForAdult(harness.founderAdultId);
    expect(founderHouseholds).toHaveLength(1);
    expect(await harness.store.listHouseholdIdsForAdult(harness.partnerAdultId)).toEqual(founderHouseholds);
    await harness.assertDatabase(
      "The accepted partner prompt reply created a duplicate family or partner",
      `(select count(*)=1 from households)
        and (select count(*)=2 from people where kind='adult')
        and (select count(*)=1 from people where kind='adult' and adult_slot=2 and status='verified')`,
    );
    const partnerMessagesBeforeGoogleCompletion = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_PARTNER,
    ).length;
    await harness.activatePartnerGoogle();
    expect(
      harness.linq.messages
        .filter((message) => message.providerConversationId === PRIVATE_PARTNER)
        .slice(partnerMessagesBeforeGoogleCompletion)
        .map((message) => message.text),
    ).toEqual([PARTNER_SETUP_COMPLETE_ACK]);
    harness.state.familyCalendarProvisioningFailuresRemaining = 1;
    await expect(harness.drain()).rejects.toThrow(
      "Linq has not confirmed sending the family group introduction",
    );
    await harness.assertDatabase(
      "An accepted-only family group introduction was bound as delivered",
      "not exists (select 1 from linq_channels where audience='group')",
    );
    expect(
      harness.linq.createChatAttempts.filter((attempt) => attempt.participantPhoneNumbers.length === 2),
    ).toHaveLength(1);
    await expect(harness.drain()).rejects.toBeInstanceOf(GoogleFamilyCalendarTransientError);

    const householdId = (await harness.store.listHouseholdIdsForAdult(harness.founderAdultId))[0];
    if (!householdId) throw new Error("The activated family household is missing");
    const stalledHousehold = await harness.store.readHousehold({ householdId });
    expect(stalledHousehold).toMatchObject({
      familyCalendarId: FAMILY_CALENDAR,
      familyCalendarCreatedAt: null,
    });
    expect(
      stalledHousehold?.channels.filter(
        (channel) => channel.audience === "group" && !channel.revokedAt && !channel.stoppedAt,
      ),
    ).toHaveLength(1);
    expect(harness.state.provisionings).toEqual([
      expect.not.objectContaining({ calendarId: expect.any(String) }),
    ]);

    harness.linq.familyCalendarReadyFailuresRemaining = 1;
    harness.linq.familyCalendarReadyAcceptedReplaysRemaining = 1;
    await expect(harness.drain()).rejects.toThrow(
      "The Family Calendar announcement outcome is temporarily unknown",
    );
    const calendarReadyButUnannounced = await harness.store.readHousehold({ householdId });
    expect(calendarReadyButUnannounced).toMatchObject({
      familyCalendarId: FAMILY_CALENDAR,
      familyCalendarCreatedAt: expect.any(String),
      initialBriefingState: "not_ready",
    });

    await expect(harness.drain()).rejects.toThrow(
      "Linq has not confirmed sending the Family Calendar announcement",
    );
    expect((await harness.store.readHousehold({ householdId }))?.initialBriefingState).toBe("not_ready");
    await harness.drain();
    const calendarReadyAttempts = harness.linq.sendMessageAttempts.filter((message) =>
      message.idempotencyKey.startsWith("family-calendar-ready:"),
    );
    expect(calendarReadyAttempts).toHaveLength(3);
    expect(new Set(calendarReadyAttempts.map((message) => message.idempotencyKey)).size).toBe(1);
    expect(
      harness.linq.messages.filter((message) => message.idempotencyKey.startsWith("family-calendar-ready:")),
    ).toHaveLength(1);

    const groups = harness.linq.createdChats.filter((chat) => chat.result.authority.audience === "group");
    expect(groups).toHaveLength(1);
    expect(groups[0]).toMatchObject({
      input: { participantPhoneNumbers: [FOUNDER_PHONE, PARTNER_PHONE] },
      result: {
        providerConversationId: FAMILY_GROUP,
        authority: {
          audience: "group",
          participantIdentityDigests: [FOUNDER_GROUP_IDENTITY, PARTNER_GROUP_IDENTITY].sort(),
        },
      },
    });
    expect(harness.state.provisionings).toEqual([
      expect.objectContaining({
        founderConnectionId: FOUNDER_GOOGLE,
        partnerConnectionId: PARTNER_GOOGLE,
        summary: "De la Cruz–Anbarasu Family",
        timeZone: "America/Los_Angeles",
      }),
      expect.objectContaining({
        calendarId: FAMILY_CALENDAR,
        founderConnectionId: FOUNDER_GOOGLE,
        partnerConnectionId: PARTNER_GOOGLE,
      }),
    ]);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          message.text ===
            "I made the De la Cruz–Anbarasu Family calendar too. I’m checking both calendars and recent family email now, and I’ll be back with what’s on the docket.",
      ),
    ).toHaveLength(1);
    expect(harness.state.briefings).toHaveLength(0);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          message.text.startsWith("Here’s what’s on the docket:"),
      ),
    ).toHaveLength(1);
    expect(
      harness.state.calendarExecutions.filter(
        (execution) =>
          execution.mutation.operation === "create" &&
          execution.mutation.event.title === PRE_ACTIVATION_FAMILY_DATE.title,
      ),
    ).toHaveLength(0);
    await harness.assertDatabase(
      "The discarded pre-activation Calendar proposal was resurrected after activation",
      `not exists (
        select 1 from calendar_actions
        where payload->'event'->>'title'=${sqlLiteral(PRE_ACTIVATION_FAMILY_DATE.title)}
      )`,
    );

    const founder = await harness.florence.workspaceForAdult(harness.founderAdultId);
    const partner = await harness.florence.workspaceForAdult(harness.partnerAdultId);
    expect(founder.workspace.setup).toEqual({
      ownOnboardingComplete: true,
      secondAdultAdded: true,
      partnerInvitation: "connected",
      bothAdultsMessagesConnected: true,
      bothAdultsGoogleConnected: true,
      familyGroupConnected: true,
      familyCalendarConnected: true,
      initialBriefing: "sent",
    });
    expect(partner.workspace.setup).toEqual(founder.workspace.setup);

    harness.state.now = Date.parse("2026-08-19T21:44:00.000Z");
    await harness.drain();
    const finiteReviewsBeforeReminder = harness.state.finiteReviews;
    await harness.accept("private", "one-shot-pickup-reminder", ONE_SHOT_REMINDER_REQUEST);
    await harness.drain();
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER && message.text === ONE_SHOT_REMINDER_ACK,
      ),
    ).toHaveLength(1);
    expect(harness.linq.messages.some((message) => message.text === ONE_SHOT_REMINDER_TEXT)).toBe(false);
    expect(harness.state.finiteReviews).toBe(finiteReviewsBeforeReminder);
    await harness.assertDatabase(
      "A one-shot reminder became a monitor or did not preserve the parent's exact delivery time",
      `(select count(*)=1 from messages
          where direction='outbound' and text=${sqlLiteral(ONE_SHOT_REMINDER_TEXT)}
            and status='pending' and not_before=${sqlLiteral(ONE_SHOT_REMINDER_AT)}::timestamptz)
        and not exists (
          select 1 from proactive_work_sources link
          join proactive_work work on work.id=link.work_id
          where link.source_id=${sqlLiteral(
            inboundSourceId("event-one-shot-pickup-reminder"),
          )}::uuid and work.kind='finite_monitor'
        )`,
    );

    harness.state.now = Date.parse(ONE_SHOT_REMINDER_AT) - 1;
    await harness.drain();
    expect(harness.linq.sendMessageAttempts.some((message) => message.text === ONE_SHOT_REMINDER_TEXT)).toBe(
      false,
    );

    harness.linq.oneShotReminderDeliveryFailuresRemaining = 1;
    harness.state.now = Date.parse(ONE_SHOT_REMINDER_AT);
    await harness.drain();
    const failedReminderAttempts = harness.linq.sendMessageAttempts.filter(
      (message) => message.text === ONE_SHOT_REMINDER_TEXT,
    );
    expect(failedReminderAttempts).toHaveLength(1);
    expect(harness.linq.messages.some((message) => message.text === ONE_SHOT_REMINDER_TEXT)).toBe(false);
    expect(harness.state.finiteReviews).toBe(finiteReviewsBeforeReminder);

    harness.state.now += 5_000;
    await harness.drain();
    const completedReminderAttempts = harness.linq.sendMessageAttempts.filter(
      (message) => message.text === ONE_SHOT_REMINDER_TEXT,
    );
    expect(completedReminderAttempts).toHaveLength(2);
    expect(new Set(completedReminderAttempts.map((message) => message.idempotencyKey)).size).toBe(1);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER && message.text === ONE_SHOT_REMINDER_TEXT,
      ),
    ).toHaveLength(1);
    expect(harness.state.finiteReviews).toBe(finiteReviewsBeforeReminder);
    await harness.drain();
    expect(
      harness.linq.sendMessageAttempts.filter((message) => message.text === ONE_SHOT_REMINDER_TEXT),
    ).toHaveLength(2);
    await harness.assertDatabase(
      "A retried one-shot reminder did not finish exactly once",
      `(select count(*)=1 from messages
          where direction='outbound' and text=${sqlLiteral(ONE_SHOT_REMINDER_TEXT)}
            and status='sent' and not_before=${sqlLiteral(ONE_SHOT_REMINDER_AT)}::timestamptz
            and sent_at>=not_before)
        and not exists (
          select 1 from proactive_work_sources link
          join proactive_work work on work.id=link.work_id
          where link.source_id=${sqlLiteral(
            inboundSourceId("event-one-shot-pickup-reminder"),
          )}::uuid and work.kind='finite_monitor'
        )`,
    );

    failNextGroupGreeting = true;
    expect(await harness.accept("group", "group-greeting-invalid-output", "Hi Florence")).toMatchObject({
      disposition: "accepted",
    });
    await harness.drain();
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP && message.text === CONVERSATION_RECOVERY_REPLY,
      ),
    ).toHaveLength(1);
    await harness.assertDatabase(
      "A recovered ordinary group turn was not handled exactly once",
      `exists (
        select 1 from messages
        where source_id=${sqlLiteral(inboundSourceId("event-group-greeting-invalid-output"))}::uuid
          and direction='inbound' and status='handled' and retry_at is null and last_error is null
      )`,
    );

    await harness.accept("group", "group-transient-retry", TRANSIENT_RETRY_REQUEST);
    await harness.drain();
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === FAMILY_GROUP && message.text === TRANSIENT_RETRY_CUE,
      ),
    ).toHaveLength(1);
    expect(harness.linq.messages.some((message) => message.text === TRANSIENT_RETRY_REPLY)).toBe(false);
    harness.state.now += 16_000;
    await harness.drain();
    expect(transientRetryAttempts).toBe(2);
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === FAMILY_GROUP && message.text === TRANSIENT_RETRY_CUE,
      ),
    ).toHaveLength(1);
    expect(harness.linq.messages.some((message) => message.text === TRANSIENT_RETRY_REPLY)).toBe(true);

    const privateAccessLinksBeforeRequest = harness.accessLinksFor(PRIVATE_FOUNDER).length;
    await harness.accept("private", "calendar-web-access", WEB_CALENDAR_ACCESS_REQUEST);
    await harness.drain();
    const calendarAccessUrl = harness.accessLinkFor(PRIVATE_FOUNDER);
    expect(calendarAccessUrl.pathname).toBe("/calendar");
    expect(harness.accessLinksFor(PRIVATE_FOUNDER)).toHaveLength(privateAccessLinksBeforeRequest + 1);

    const groupAccessLinksBeforeRequest = harness.accessLinksFor(FAMILY_GROUP).length;
    await harness.accept("group", "group-calendar-web-access", WEB_CALENDAR_ACCESS_REQUEST);
    await harness.drain();
    expect(harness.accessLinksFor(FAMILY_GROUP)).toHaveLength(groupAccessLinksBeforeRequest);

    const privateAccessLinksBeforeVoiceEvidence = harness.accessLinksFor(PRIVATE_FOUNDER).length;
    const voiceOnlyAccess = await harness.florence.acceptInbound({
      ...harness.inbound("private", "voice-only-calendar-web-access", WEB_CALENDAR_ACCESS_REQUEST),
      authoredText: null,
      voiceTranscriptPresent: true,
    });
    expect(voiceOnlyAccess).not.toBeNull();
    await harness.drain();
    expect(harness.accessLinksFor(PRIVATE_FOUNDER)).toHaveLength(privateAccessLinksBeforeVoiceEvidence);

    await harness.accept(
      "private",
      "calendar-web-access-follow-up",
      `${WEB_ACCESS_FOLLOW_UP} ${calendarAccessUrl.toString()}`,
    );
    await harness.drain();
    expect(accessFollowUpHistory.some((text) => text.includes("wa1."))).toBe(false);
    expect(accessFollowUpHistory.some((text) => text.includes("[secure web link]"))).toBe(true);
    expect(accessFollowUpAuthoredText).not.toContain("wa1.");
    expect(accessFollowUpAuthoredText).toContain("[secure web link]");

    const calendarAccessToken = new URLSearchParams(calendarAccessUrl.hash.slice(1)).get("a");
    if (!calendarAccessToken) throw new Error("The calendar access token was not sent");
    const originalFounderAuthority = harness.linq.authorities.get(PRIVATE_FOUNDER);
    if (!originalFounderAuthority) throw new Error("The founder private authority is unavailable");
    harness.linq.authorities.set(PRIVATE_FOUNDER, {
      audience: "group",
      participantIdentityDigests: [FOUNDER_IDENTITY],
    });
    const rejectedAccessApp = await harness.webApp(false);
    const rejectedAccessResponse = await rejectedAccessApp.inject({
      method: "POST",
      url: "/api/v1/session",
      payload: { accessToken: calendarAccessToken },
    });
    await rejectedAccessApp.close();
    expect(rejectedAccessResponse.statusCode).toBe(401);
    harness.linq.authorities.set(PRIVATE_FOUNDER, originalFounderAuthority);
    const calendarAccessApp = await harness.webApp(false);
    const calendarAccessResponse = await calendarAccessApp.inject({
      method: "POST",
      url: "/api/v1/session",
      payload: { accessToken: calendarAccessToken },
    });
    await calendarAccessApp.close();
    expect(calendarAccessResponse.statusCode).toBe(200);
    expect(calendarAccessResponse.json()).toEqual({
      adultId: harness.founderAdultId,
      accessPath: "/calendar",
    });

    const visibleCounts = {
      chats: harness.linq.createdChats.length,
      messages: harness.linq.messages.length,
      provisionings: harness.state.provisionings.length,
      briefings: harness.state.briefings.length,
    };
    await harness.drain();
    expect({
      chats: harness.linq.createdChats.length,
      messages: harness.linq.messages.length,
      provisionings: harness.state.provisionings.length,
      briefings: harness.state.briefings.length,
    }).toEqual(visibleCounts);

    const firstIncarnation = linqIncarnationSnapshot(harness.linq);
    const resetHarness = await createHarness(
      async (input) =>
        input.currentMessage.text === STALE_RECEIPT_QUESTION
          ? decision({ bubbles: [{ text: STALE_RECEIPT_REPLY, delayMs: 0 }] })
          : decision(),
      { now: NOW + 10 * 60_000, linqLedger: harness.linq.ledger },
    );
    resetHarness.state.initialNoAttentionReview = true;
    await resetHarness.readyHousehold();
    expect(resetHarness.state.privateReviews.map((review) => review.adult.firstName).sort()).toEqual([
      "Alex",
      "Hari",
    ]);
    const quietPrivateReviews = resetHarness.linq.messages.filter((message) =>
      message.idempotencyKey.startsWith("initial-private-review:"),
    );
    expect(quietPrivateReviews).toHaveLength(2);
    expect(quietPrivateReviews.map((message) => message.text)).toEqual([
      PRIVATE_INITIAL_ALL_CLEAR,
      PRIVATE_INITIAL_ALL_CLEAR,
    ]);
    expect(new Set(quietPrivateReviews.map((message) => message.providerConversationId))).toEqual(
      new Set([PRIVATE_FOUNDER, PRIVATE_PARTNER]),
    );
    expect(resetHarness.state.briefings).toHaveLength(0);
    await resetHarness.assertDatabase(
      "A quiet private review retained a finding or family fact",
      `(select count(*)=2 from proactive_work
          where kind='initial_private_review' and status='completed'
            and briefing_candidates='[]'::jsonb)
        and not exists (
          select 1 from proactive_work_sources source
          join proactive_work work on work.id=source.work_id
          where work.kind='initial_private_review'
        )
        and not exists (select 1 from facts)`,
    );
    const quietActivationMessages = resetHarness.linq.messages.filter(
      (message) => message.expectedAuthority.audience === "group",
    );
    const quietIntroductionIndex = quietActivationMessages.findIndex((message) =>
      message.idempotencyKey.startsWith("family-group:"),
    );
    const quietCalendarIndex = quietActivationMessages.findIndex((message) =>
      message.idempotencyKey.startsWith("family-calendar-ready:"),
    );
    const quietDocketIndex = quietActivationMessages.findIndex((message) =>
      message.idempotencyKey.startsWith("initial-household-briefing:"),
    );
    expect(quietIntroductionIndex).toBeGreaterThanOrEqual(0);
    expect(quietCalendarIndex).toBeGreaterThan(quietIntroductionIndex);
    expect(quietDocketIndex).toBeGreaterThan(quietCalendarIndex);
    expect(quietActivationMessages[quietDocketIndex]?.text).toBe(HOUSEHOLD_INITIAL_ALL_CLEAR);
    const resetIncarnation = linqIncarnationSnapshot(resetHarness.linq);
    expectFreshLinqIncarnation(firstIncarnation, resetIncarnation);

    resetHarness.linq.staleReceiptForNextMessage = true;
    await resetHarness.accept("private", "stale-provider-receipt", STALE_RECEIPT_QUESTION);
    await resetHarness.drain();
    expect(resetHarness.linq.messages.some((message) => message.text === STALE_RECEIPT_REPLY)).toBe(false);
    await resetHarness.assertDatabase(
      "A stale provider receipt was allowed to mark the current outbound message sent",
      `exists (
        select 1 from messages
        where direction='outbound' and text=${sqlLiteral(STALE_RECEIPT_REPLY)}
          and status='failed' and provider_message_id is null and sent_at is null
      )`,
    );
  }, 20_000);

  test("gets ahead from both parents’ context, native inputs, a monitor, and the read-only calendar", async () => {
    let nativeInputWasRead = false;
    let ordinaryUnusedSourceId: string | null = null;
    let conversationalGoogleSourceId: string | null = null;
    let retainedGoogleMemorySourceId: string | null = null;
    let groupHouseholdFactWasVisible = false;
    const publicResearchCapture: {
      mainRequest: Record<string, unknown> | null;
      publicRequests: Record<string, unknown>[];
    } = { mainRequest: null, publicRequests: [] };
    let publicResearchModelTurns = 0;
    let publicSearchTurns = 0;
    let publicResearchNeedsFinal = false;
    let publicFinalReply = PUBLIC_RESEARCH_REPLY;
    let publicFinalUrls = [PUBLIC_RESEARCH_URL];
    const publicResearchReasoner = new FlorenceReasoner({ apiKey: "test-openai-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          publicResearchModelTurns += 1;
          if (!publicResearchNeedsFinal) {
            publicResearchNeedsFinal = true;
            publicResearchCapture.mainRequest ??= request;
            const call = {
              id: "public-research-tool-output",
              type: "function_call",
              call_id: "public-research-tool-call",
              name: "research_public_web",
              arguments: "{}",
              status: "completed",
            };
            return fakeResponseStream(
              [
                {
                  type: "response.output_item.added",
                  item: call,
                  output_index: 0,
                  sequence_number: 1,
                },
              ],
              {
                output_parsed: null,
                output: [call],
              },
            );
          }
          publicResearchNeedsFinal = false;
          return fakeResponseStream([], {
            output_parsed: decision({
              bubbles: [{ text: publicFinalReply, delayMs: 0 }],
              researchUrls: publicFinalUrls,
            }),
            output: [],
          });
        },
        parse: (request: Record<string, unknown>) => {
          publicResearchCapture.publicRequests.push(request);
          publicSearchTurns += 1;
          const serializedInput = JSON.stringify(request.input);
          const noResult = serializedInput.includes("9780143127796");
          const shortIdentifier = serializedInput.includes(PUBLIC_SHORT_IDENTIFIER_REQUEST);
          const publicConcept = serializedInput.includes("dQw4w9WgXcQ");
          publicFinalReply = noResult
            ? PUBLIC_NO_RESULT_REPLY
            : publicConcept
              ? PUBLIC_CONCEPT_REPLY
              : shortIdentifier
                ? PUBLIC_SHORT_IDENTIFIER_REPLY
                : PUBLIC_RESEARCH_REPLY;
          publicFinalUrls = noResult
            ? []
            : [
                publicConcept
                  ? PUBLIC_CONCEPT_URL
                  : shortIdentifier
                    ? PUBLIC_SHORT_IDENTIFIER_URL
                    : PUBLIC_RESEARCH_URL,
              ];
          return {
            output_parsed: {
              outcome: noResult ? "no_result" : "result",
              summary: publicFinalReply,
              urls: publicFinalUrls,
            },
            output: [
              {
                id: "web-search-flight-options",
                type: "web_search_call",
                status: "completed",
                action: {
                  type: "search",
                  query: noResult
                    ? "9780143127796 2026-08-27"
                    : publicConcept
                      ? "access token password managers confirmation code format YouTube video"
                      : shortIdentifier
                        ? "X public identifier"
                        : "DL 747 current route status alternatives tonight",
                  sources: [
                    {
                      type: "url",
                      url: noResult
                        ? PUBLIC_NO_RESULT_SOURCE
                        : publicConcept
                          ? PUBLIC_CONCEPT_URL
                          : shortIdentifier
                            ? PUBLIC_SHORT_IDENTIFIER_URL
                            : PUBLIC_RESEARCH_URL,
                    },
                  ],
                },
              },
            ],
          };
        },
      },
    } as never);
    const clarificationReasoner = new FlorenceReasoner({ apiKey: "test-openai-key", model: "test-model" }, {
      responses: {
        stream: () =>
          fakeResponseStream([], {
            output_parsed: decision({
              bubbles: [{ text: CLARIFICATION_ONLY_REPLY, delayMs: 0 }],
            }),
            output: [],
          }),
      },
    } as never);
    let privacyBoundarySearchCalls = 0;
    const privateOnlyCapture: {
      mainRequest: Record<string, unknown> | null;
      publicRequest: Record<string, unknown> | null;
    } = { mainRequest: null, publicRequest: null };
    let privateOnlyNeedsFinal = false;
    const privacyBoundaryReasoner = new FlorenceReasoner({ apiKey: "test-openai-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          privateOnlyCapture.mainRequest ??= request;
          if (!privateOnlyNeedsFinal) {
            privateOnlyNeedsFinal = true;
            const call = {
              id: "private-only-public-tool-output",
              type: "function_call",
              call_id: "private-only-public-tool-call",
              name: "research_public_web",
              arguments: "{}",
              status: "completed",
            };
            return fakeResponseStream(
              [
                {
                  type: "response.output_item.added",
                  item: call,
                  output_index: 0,
                  sequence_number: 1,
                },
              ],
              { output_parsed: null, output: [call] },
            );
          }
          return fakeResponseStream([], {
            output_parsed: decision({
              bubbles: [{ text: PRIVATE_ONLY_PUBLIC_RESEARCH_REPLY, delayMs: 0 }],
            }),
            output: [],
          });
        },
        parse: (request: Record<string, unknown>) => {
          privacyBoundarySearchCalls += 1;
          privateOnlyCapture.publicRequest = request;
          return {
            output_parsed: {
              outcome: "no_result",
              summary: "The sanitized request did not contain a useful public subject.",
              urls: [],
            },
            output: [
              {
                id: "private-boundary-web-search",
                type: "web_search_call",
                status: "completed",
                action: { type: "search", query: "private detail omitted", sources: [] },
              },
            ],
          };
        },
      },
    } as never);
    const nativeObservation: {
      audience: string | null;
      text: string | null;
      imageCount: number;
      pdfCount: number;
      imageBytes: Uint8Array | null;
      pdfBytes: Uint8Array | null;
    } = {
      audience: null,
      text: null,
      imageCount: 0,
      pdfCount: 0,
      imageBytes: null,
      pdfBytes: null,
    };
    const harness = await createHarness(async (input, reads, signal, hooks) => {
      if (
        input.currentMessage.text === PUBLIC_RESEARCH_REQUEST ||
        input.currentMessage.text === PUBLIC_NO_RESULT_REQUEST ||
        input.currentMessage.text === PUBLIC_SHORT_IDENTIFIER_REQUEST ||
        input.currentMessage.text === PUBLIC_CONCEPT_REQUEST
      ) {
        return publicResearchReasoner.decide(input, reads, signal, hooks);
      }
      if (input.currentMessage.text === CLARIFICATION_ONLY_REQUEST) {
        return clarificationReasoner.decide(input, reads, signal, hooks);
      }
      if (input.currentMessage.text === PRIVATE_ONLY_PUBLIC_RESEARCH_REQUEST) {
        return privacyBoundaryReasoner.decide(input, reads, signal, hooks);
      }
      if (input.currentMessage.text === GOOGLE_MEMORY_REPLY_QUESTION) {
        const retained = input.visibleSources.find(
          (source) => source.kind === "memory" && source.text === UPDATED_PRIVATE_SCHOOL_FACT,
        );
        if (!retained?.recordId) {
          throw new Error("The retained Gmail-derived school fact was not visible to the parent turn");
        }
        retainedGoogleMemorySourceId = retained.sourceId;
        return decision({ bubbles: [{ text: GOOGLE_MEMORY_REPLY, delayMs: 5_000 }] });
      }
      if (input.currentMessage.text === GOOGLE_CITED_REPLY_QUESTION) {
        const connection = input.googleConnections.find((candidate) => candidate.kind === "personal");
        if (!connection) throw new Error("The private Gmail connection is missing");
        const [source] = await reads.searchGmail({
          connectionId: connection.connectionId,
          query: GOOGLE_CITED_REPLY_QUERY,
          limit: 10,
        });
        if (!source?.text.includes("emergency card")) {
          throw new Error("The conversational Gmail read returned no usable evidence");
        }
        conversationalGoogleSourceId = source.sourceId;
        return decision({ bubbles: [{ text: GOOGLE_CITED_REPLY, delayMs: 5_000 }] });
      }
      if (input.currentMessage.text === ORDINARY_UNUSED_GMAIL_QUESTION) {
        const connection = input.googleConnections.find((candidate) => candidate.kind === "personal");
        if (!connection) throw new Error("The private Gmail connection is missing");
        const [source] = await reads.searchGmail({
          connectionId: connection.connectionId,
          query: ORDINARY_UNUSED_GMAIL_QUERY,
          limit: 10,
        });
        if (!source) throw new Error("The ordinary Gmail search returned no evidence");
        ordinaryUnusedSourceId = source.sourceId;
        return decision({
          bubbles: [{ text: "Nothing in that email needs you to do anything.", delayMs: 0 }],
        });
      }
      if (input.currentMessage.text === INTEREST_REQUEST) {
        return decision({
          bubbles: [
            { text: "I’ll keep an eye out and only bring you something genuinely useful.", delayMs: 0 },
          ],
          facts: [remember("Maya likes soccer", input.currentMessage.sourceId)],
          interest: {
            operation: "create",
            interestWorkId: null,
            genericTerms: ["family soccer matches"],
            objective: "Find a worthwhile local soccer outing for the family.",
            why: "Maya likes soccer and the family asked Florence to keep watch.",
            sourceIds: [input.currentMessage.sourceId],
          },
        });
      }
      if (input.currentMessage.sourceId !== inboundSourceId("event-native-school-update")) {
        return decision();
      }
      groupHouseholdFactWasVisible = input.visibleSources.some(
        (source) =>
          source.kind === "memory" &&
          source.visibility === "shared" &&
          source.text === UPDATED_PRIVATE_SCHOOL_FACT,
      );
      nativeInputWasRead = true;
      nativeObservation.audience = input.audience;
      nativeObservation.text = input.currentMessage.text;
      nativeObservation.imageCount = input.currentMessage.images.length;
      nativeObservation.pdfCount = input.currentMessage.pdfs?.length ?? 0;
      const imageReference = input.currentMessage.images[0];
      const pdfReference = input.currentMessage.pdfs?.[0];
      if (!imageReference || !pdfReference || !reads.readCurrentPdf) return decision();
      const image = await reads.readCurrentImage(imageReference);
      const pdf = await reads.readCurrentPdf(pdfReference);
      nativeObservation.imageBytes = image.bytes;
      nativeObservation.pdfBytes = pdf.bytes;
      return decision({
        bubbles: [{ text: "I found the deadline and I’ll keep an eye on it.", delayMs: 0 }],
        facts: [remember("Maya’s field-trip form is due Tuesday", input.currentMessage.sourceId)],
        followUp: {
          operation: "schedule",
          followUpId: null,
          objective: "Watch for confirmation that Maya’s field-trip form is signed.",
          currentConclusion: "The form still needs a parent signature.",
          endCondition: "A parent or the school confirms the form is signed.",
          nextCheck: new Date(Date.parse(input.currentMessage.occurredAt) + 60 * 60_000).toISOString(),
          why: "The forwarded school message has a live deadline.",
          sourceIds: [input.currentMessage.sourceId],
        },
      });
    });
    harness.state.initialGoogleFailuresRemaining = 2;
    harness.state.initialClassifierFailuresRemaining = 1;
    harness.state.completeScanPaginationExercise = true;
    harness.state.founderProductRecenterReview = true;
    harness.state.initialHouseholdCalendarFailuresRemaining = 1;
    await harness.readyHousehold();

    expect(
      (await harness.florence.workspaceForAdult(harness.founderAdultId)).vault?.watches.filter(
        (watch) => watch.kind === "interest",
      ),
    ).toHaveLength(0);

    expect(harness.state.briefings).toHaveLength(0);
    expect(harness.state.initialGoogleFailuresRemaining).toBe(1);
    const messagesAfterFirstSilentReviewFailure = harness.linq.messages.length;
    harness.state.now += 16_000;
    await harness.drain();
    expect(harness.state.briefings).toHaveLength(0);
    expect(harness.state.initialGoogleFailuresRemaining).toBe(0);
    expect(harness.linq.messages).toHaveLength(messagesAfterFirstSilentReviewFailure);
    harness.state.now += 16_000;
    await harness.drain();
    expect(harness.state.initialClassifierFailuresRemaining).toBe(0);
    expect(harness.linq.messages).toHaveLength(messagesAfterFirstSilentReviewFailure);
    harness.state.now += 16_000;
    await harness.drain();

    expect(harness.state.privateReviews.map((review) => review.adult.firstName).sort()).toEqual([
      "Alex",
      "Hari",
    ]);
    const founderModelReview = harness.state.privateReviews.find(
      (review) => review.adult.adultId === harness.founderAdultId,
    );
    if (!founderModelReview) throw new Error("The founder's model-safe Gmail batch is missing");
    const founderModelReviewJson = JSON.stringify(founderModelReview);
    expect(founderModelReviewJson).toContain("[code removed]");
    expect(founderModelReviewJson).toContain("[link removed]");
    expect(founderModelReviewJson).not.toContain("123456");
    expect(founderModelReviewJson).not.toContain("example.test/reset");
    const founderBaselineReads = harness.state.baselinePageReads.filter(
      (read) => read.ownerAdultId === harness.founderAdultId && read.connectionId === FOUNDER_GOOGLE,
    );
    expect(
      founderBaselineReads.filter((read) => read.kind === "gmail").map((read) => read.pageToken),
    ).toEqual([null, "gmail-baseline-page-2", "gmail-baseline-page-2"]);
    expect(
      founderBaselineReads.filter((read) => read.kind === "calendar_targets").map((read) => read.pageToken),
    ).toEqual([
      null,
      "calendar-targets-page-2",
      null,
      "calendar-targets-page-2",
      null,
      "calendar-targets-page-2",
      null,
      "calendar-targets-page-2",
    ]);
    expect(
      founderBaselineReads
        .filter((read) => read.kind === "calendar_events" && read.calendarId === "primary")
        .map((read) => read.pageToken),
    ).toEqual([null, "calendar-events-page-2", null, "calendar-events-page-2"]);
    expect(
      founderBaselineReads.filter(
        (read) => read.kind === "calendar_events" && read.calendarId?.startsWith("secondary-calendar-"),
      ),
    ).toHaveLength(100);
    expect(harness.state.briefings).toHaveLength(0);
    expect(harness.state.initialHouseholdCalendarFailuresRemaining).toBe(0);
    expect(
      harness.linq.messages.some((message) =>
        message.idempotencyKey.startsWith("initial-household-briefing:"),
      ),
    ).toBe(false);

    harness.state.now += 16_000;
    const incompleteWork = await harness.store.readNextInitialIntelligence(harness.iso());
    if (incompleteWork?.kind !== "initial_household_briefing") {
      throw new Error("The complete private reviews did not produce household briefing work");
    }
    expect(incompleteWork.candidates).toHaveLength(5);
    expect(incompleteWork.candidates.length).toBeGreaterThan(3);
    expect(
      incompleteWork.candidates.filter(
        (candidate) =>
          candidate.category === "conflict" && candidate.summary === SHARED_DUPLICATE_CONFLICT_SUMMARY,
      ),
    ).toHaveLength(1);
    const incompleteCandidate = incompleteWork.candidates[0];
    if (!incompleteCandidate) throw new Error("The incomplete briefing regression needs one candidate");
    await expect(
      harness.store.completeHouseholdInitialBriefing({
        workId: incompleteWork.workId,
        selectedCandidateIds: [incompleteCandidate.candidateId],
        familyCalendarCursor: "{}",
        bubbles: [
          {
            text: `Here’s what’s on the docket:\n• ${incompleteCandidate.summary}\n\nDid I get that right? If I missed something, tell me here.`,
            delayMs: 0,
          },
        ],
        occurredAt: harness.iso(),
      }),
    ).rejects.toThrow(/omitted or added a distinct finding/i);
    await expect(
      harness.store.completeHouseholdInitialBriefing({
        workId: incompleteWork.workId,
        selectedCandidateIds: incompleteWork.candidates.map((candidate) => candidate.candidateId),
        familyCalendarCursor: "{}",
        bubbles: [{ text: HOUSEHOLD_INITIAL_ALL_CLEAR, delayMs: 0 }],
        occurredAt: harness.iso(),
      }),
    ).rejects.toThrow(/omitted or added a distinct finding/i);
    await harness.assertDatabase(
      "An incomplete household docket advanced work, sent an all-clear, or started polling",
      `not exists (
        select 1 from messages where idempotency_key like 'initial-household-briefing:%'
      ) and not exists (
        select 1 from proactive_work where kind='family_calendar_poll'
      ) and exists (
        select 1 from proactive_work
        where id=${sqlLiteral(incompleteWork.workId)}::uuid and status='active'
      )`,
    );
    const conflictIndex = incompleteWork.candidates.findIndex(
      (candidate) => candidate.category === "conflict",
    );
    const secondParentIndex = incompleteWork.candidates.findIndex(
      (candidate, index) => index > conflictIndex && candidate.summary === DISTINCT_CANDIDATE_SHARED_SUMMARY,
    );
    if (conflictIndex < 1 || secondParentIndex <= conflictIndex) {
      throw new Error("The briefing metadata regression needs three ordered summary occurrences");
    }
    const briefingCandidateSlices = [
      incompleteWork.candidates.slice(0, conflictIndex),
      incompleteWork.candidates.slice(conflictIndex, secondParentIndex),
      incompleteWork.candidates.slice(secondParentIndex),
    ];
    await harness.store.completeHouseholdInitialBriefing({
      workId: incompleteWork.workId,
      selectedCandidateIds: incompleteWork.candidates.map((candidate) => candidate.candidateId),
      familyCalendarCursor: "{}",
      bubbles: briefingCandidateSlices.map((candidates, index) => ({
        text: `${index === 0 ? "Here’s what’s on the docket:" : "And:"}\n${candidates
          .map((candidate) => `• ${candidate.summary}`)
          .join("\n")}${
          index === briefingCandidateSlices.length - 1
            ? "\n\nDid I get that right? If I missed something, tell me here."
            : ""
        }`,
        delayMs: 0,
      })),
      occurredAt: harness.iso(),
    });
    await harness.drain();

    const briefingMessages = harness.linq.messages.filter(
      (message) =>
        message.providerConversationId === FAMILY_GROUP &&
        message.idempotencyKey.startsWith("initial-household-briefing:"),
    );
    expect(briefingMessages).toHaveLength(3);
    const briefing = briefingMessages.map((message) => message.text).join("\n");
    expect(briefing.split(DISTINCT_CANDIDATE_SHARED_SUMMARY)).toHaveLength(4);
    for (const summary of [SCHOOL_HANDOFF_SUMMARY, FAMILY_MEETING_SUMMARY]) {
      expect(briefing.split(summary)).toHaveLength(2);
    }
    expect(briefing).toContain("Did I get that right? If I missed something, tell me here.");
    expect(briefing).not.toMatch(/@example\.com|private calendar detail/i);
    expect(briefing).not.toContain(PRIVATE_INITIAL_ONLY_FINDING);
    expect(briefing).not.toContain(INITIAL_PRIVATE_SCHOOL_FACT);
    expect(briefing).not.toContain(PARTNER_PRIVATE_GOOGLE_FACT);
    expect(briefing).not.toBe(HOUSEHOLD_INITIAL_ALL_CLEAR);
    expect(
      harness.linq.messages.some(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER &&
          message.text.includes(PRIVATE_INITIAL_ONLY_FINDING) &&
          message.text.includes("What needs attention:"),
      ),
    ).toBe(false);
    expect(
      harness.linq.messages.some(
        (message) =>
          message.text.includes(INITIAL_PRIVATE_SCHOOL_FACT) ||
          message.text.includes(PARTNER_PRIVATE_GOOGLE_FACT),
      ),
    ).toBe(false);
    await harness.assertDatabase(
      "Identical briefing text crossed candidate metadata boundaries",
      `exists (
        select 1
        from messages first_message join sources first_source on first_source.id=first_message.source_id
        join messages middle_message on middle_message.turn_id=first_message.turn_id
          and middle_message.turn_part=1
        join sources middle_source on middle_source.id=middle_message.source_id
        join messages last_message on last_message.turn_id=first_message.turn_id
          and last_message.turn_part=2
        join sources last_source on last_source.id=last_message.source_id
        where first_message.idempotency_key like 'initial-household-briefing:%'
          and first_message.turn_part=0
          and first_source.metadata->'privateConflictOwnerAdultIds' is null
          and last_source.metadata->'privateConflictOwnerAdultIds' is null
          and middle_source.metadata->'privateConflictOwnerAdultIds'
            @> ${sqlLiteral(JSON.stringify([harness.founderAdultId, harness.partnerAdultId]))}::jsonb
          and not exists (
            select 1
            from jsonb_array_elements_text(first_source.metadata->'googleSourceIds') first_id(value)
            join jsonb_array_elements_text(last_source.metadata->'googleSourceIds') last_id(value)
              on last_id.value=first_id.value
          )
          and not exists (
            select 1
            from jsonb_array_elements_text(first_source.metadata->'googleActionKeys') first_key(value)
            join jsonb_array_elements_text(last_source.metadata->'googleActionKeys') last_key(value)
              on last_key.value=first_key.value
          )
      )`,
    );

    const founderAfterInitialReview = await harness.florence.workspaceForAdult(harness.founderAdultId);
    expect(founderAfterInitialReview.vault?.watches).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: "monitor",
          currentConclusion: PRIVATE_INITIAL_ONLY_FINDING,
          status: "active",
          visibility: "private",
        }),
        expect.objectContaining({
          kind: "monitor",
          currentConclusion: PAGINATED_CALENDAR_FOLLOW_UP,
          status: "active",
          visibility: "private",
        }),
      ]),
    );
    const initialHouseholdFact = founderAfterInitialReview.vault?.facts.find(
      (fact) => fact.statement === INITIAL_PRIVATE_SCHOOL_FACT,
    );
    expect(initialHouseholdFact).toMatchObject({
      visibility: "household",
      source: { kind: "gmail" },
    });
    if (!initialHouseholdFact) throw new Error("The initial household Gmail fact was not retained");
    const partnerAfterInitialReview = await harness.florence.workspaceForAdult(harness.partnerAdultId);
    expect(
      partnerAfterInitialReview.vault?.facts.find((fact) => fact.id === initialHouseholdFact.id),
    ).toEqual(
      expect.objectContaining({
        statement: INITIAL_PRIVATE_SCHOOL_FACT,
        visibility: "household",
        source: null,
        recordedAt: null,
      }),
    );
    expect(JSON.stringify(partnerAfterInitialReview.vault)).not.toMatch(
      /hari-private@example\.com|Private school form/,
    );
    await harness.assertDatabase(
      "A household Gmail fact duplicated its slot or lost either parent's private support",
      `(select count(*)=1 from facts
          where household_id=(select household_id from people
            where id=${sqlLiteral(harness.founderAdultId)}::uuid)
            and slot=${sqlLiteral(SHARED_SCHOOL_CONTACT_SLOT)}
            and visibility='household' and owner_adult_id is null)
        and (select count(distinct source.owner_adult_id)=2 from facts fact
          join fact_sources link on link.fact_id=fact.id
          join sources source on source.id=link.source_id
          where fact.household_id=(select household_id from people
            where id=${sqlLiteral(harness.founderAdultId)}::uuid)
            and fact.slot=${sqlLiteral(SHARED_SCHOOL_CONTACT_SLOT)}
            and source.kind='gmail' and source.visibility='private')`,
    );
    const googleCorrectableFact = founderAfterInitialReview.vault?.facts.find(
      (fact) => fact.statement === GOOGLE_CORRECTION_FACT,
    );
    if (!googleCorrectableFact) throw new Error("The Google-backed correction fact is missing");
    expect(
      (
        await harness.florence.correctFact(
          harness.partnerAdultId,
          googleCorrectableFact.id,
          GOOGLE_CORRECTED_FACT,
        )
      ).vault?.facts.find((fact) => fact.id === googleCorrectableFact.id),
    ).toEqual(expect.objectContaining({ statement: GOOGLE_CORRECTED_FACT, visibility: "household" }));

    await harness.assertDatabase(
      "Unused initial Gmail and Calendar evidence was retained",
      `not exists (
        select 1 from sources
        where (kind='gmail' and metadata->>'messageId' in (
          ${sqlLiteral(`gmail-${harness.founderAdultId}-false`)},
          ${sqlLiteral(`gmail-${harness.partnerAdultId}-false`)}
        )) or (kind='calendar' and metadata->>'providerEventId' in (
          ${sqlLiteral(`private-event-${harness.founderAdultId}`)},
          ${sqlLiteral(`private-event-${harness.partnerAdultId}`)}
        ))
      )`,
    );

    const publicResearchReactionsBefore = harness.linq.reactions.length;
    const publicResearchTimelineBefore = harness.state.timeline.length;
    await harness.accept("group", "public-identifier-research", PUBLIC_RESEARCH_REQUEST, "partner");
    await harness.drain();
    expect(publicResearchModelTurns).toBe(2);
    expect(publicSearchTurns).toBe(1);
    expect(publicResearchCapture.mainRequest?.tools).toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "function", name: "research_public_web" })]),
    );
    expect(publicResearchCapture.mainRequest?.tools).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "web_search" })]),
    );
    expect(publicResearchCapture.publicRequests[0]?.tools).toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "web_search" })]),
    );
    expect(publicResearchCapture.publicRequests[0]?.include).toEqual(
      expect.arrayContaining(["web_search_call.action.sources"]),
    );
    expect(publicResearchCapture.publicRequests[0]?.tool_choice).toBe("required");
    const isolatedPublicInput = JSON.stringify(publicResearchCapture.publicRequests[0]?.input);
    expect(isolatedPublicInput).toContain("DL 747");
    expect(isolatedPublicInput).not.toMatch(/Alex|Maya|Anbarasu|hari@example\.com|familyProfile|gmail/iu);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          (message.text === PUBLIC_RESEARCH_REPLY || message.text === PUBLIC_RESEARCH_URL),
      ),
    ).toHaveLength(2);
    expect(harness.linq.reactions.slice(publicResearchReactionsBefore)).toEqual([
      expect.objectContaining({
        providerConversationId: FAMILY_GROUP,
        targetProviderMessageId: "message-public-identifier-research",
        reaction: "like",
      }),
    ]);
    const publicResearchTimeline = harness.state.timeline.slice(publicResearchTimelineBefore);
    expect(publicResearchTimeline.indexOf("reaction:like:message-public-identifier-research")).toBeLessThan(
      publicResearchTimeline.indexOf(`message:${PUBLIC_RESEARCH_REPLY}`),
    );
    await harness.assertDatabase(
      "A foreground public lookup created fake background work",
      `not exists (
        select 1 from proactive_work work
        join proactive_work_sources link on link.work_id=work.id
        where link.source_id=${sqlLiteral(inboundSourceId("event-public-identifier-research"))}::uuid
      )`,
    );
    const publicSearchesBeforeNoResult = publicSearchTurns;
    const reactionsBeforeNoResult = harness.linq.reactions.length;
    await harness.accept("group", "public-no-result-research", PUBLIC_NO_RESULT_REQUEST, "partner");
    await harness.drain();
    expect(publicSearchTurns).toBe(publicSearchesBeforeNoResult + 1);
    const isolatedNumericPublicInput = JSON.stringify(publicResearchCapture.publicRequests[1]?.input);
    expect(isolatedNumericPublicInput).toContain("9780143127796");
    expect(isolatedNumericPublicInput).toContain("2026-08-27");
    expect(harness.linq.messages.some((message) => message.text === PUBLIC_NO_RESULT_REPLY)).toBe(true);
    expect(harness.linq.messages.some((message) => message.text === PUBLIC_NO_RESULT_SOURCE)).toBe(false);
    expect(harness.linq.reactions.slice(reactionsBeforeNoResult)).toEqual([
      expect.objectContaining({
        providerConversationId: FAMILY_GROUP,
        targetProviderMessageId: "message-public-no-result-research",
        reaction: "like",
      }),
    ]);
    const searchesBeforeShortIdentifier = publicSearchTurns;
    await harness.accept("group", "public-short-identifier", PUBLIC_SHORT_IDENTIFIER_REQUEST, "partner");
    await harness.drain();
    expect(publicSearchTurns).toBe(searchesBeforeShortIdentifier + 1);
    expect(JSON.stringify(publicResearchCapture.publicRequests[2]?.input)).toContain(
      PUBLIC_SHORT_IDENTIFIER_REQUEST,
    );
    expect(harness.linq.messages.some((message) => message.text === PUBLIC_SHORT_IDENTIFIER_REPLY)).toBe(
      true,
    );
    expect(harness.linq.messages.some((message) => message.text === PUBLIC_SHORT_IDENTIFIER_URL)).toBe(true);
    const searchesBeforePublicConcept = publicSearchTurns;
    await harness.accept("group", "public-concept-search", PUBLIC_CONCEPT_REQUEST, "partner");
    await harness.drain();
    expect(publicSearchTurns).toBe(searchesBeforePublicConcept + 1);
    const isolatedPublicConceptInput = JSON.stringify(publicResearchCapture.publicRequests[3]?.input);
    expect(isolatedPublicConceptInput).toContain("access token");
    expect(isolatedPublicConceptInput).toContain("password managers");
    expect(isolatedPublicConceptInput).toContain("confirmation code format");
    expect(isolatedPublicConceptInput).toContain(PUBLIC_CONCEPT_URL);
    expect(harness.linq.messages.some((message) => message.text === PUBLIC_CONCEPT_REPLY)).toBe(true);
    const reactionsAfterPublicResearch = harness.linq.reactions.length;
    await harness.accept("group", "clarification-only", CLARIFICATION_ONLY_REQUEST, "partner");
    await harness.drain();
    expect(harness.linq.reactions).toHaveLength(reactionsAfterPublicResearch);
    expect(harness.linq.messages.some((message) => message.text === CLARIFICATION_ONLY_REPLY)).toBe(true);

    await harness.accept("private", "private-only-public-research", PRIVATE_ONLY_PUBLIC_RESEARCH_REQUEST);
    await harness.drain();
    expect(privacyBoundarySearchCalls).toBe(1);
    expect(privateOnlyCapture.mainRequest?.tools).toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "function", name: "research_public_web" })]),
    );
    const privateBoundaryInput = JSON.stringify(privateOnlyCapture.publicRequest?.input);
    expect(privateBoundaryInput).not.toMatch(
      /Hari|Anbarasu|hari@example\.com|15555550101|310-555-1212|localhost|secret123|secret456|secret789|user:pass|Users\/Hari|10\.0\.0\.2|127\.0\.0\.1|ABC123/iu,
    );
    expect(privateBoundaryInput).toMatch(/private (?:detail|URL) omitted/iu);
    expect(harness.linq.messages.some((message) => message.text === PRIVATE_ONLY_PUBLIC_RESEARCH_REPLY)).toBe(
      true,
    );

    await harness.accept("private", "ordinary-unused-email", ORDINARY_UNUSED_GMAIL_QUESTION);
    await harness.drain();
    if (!ordinaryUnusedSourceId) throw new Error("The ordinary Gmail source was not observed");
    await harness.assertDatabase(
      "An uncited ordinary Gmail answer retained its source",
      `not exists (select 1 from sources where id=${sqlLiteral(ordinaryUnusedSourceId)}::uuid)`,
    );

    const messagesBeforeUnrelatedAccountEmail = harness.linq.messages.length;
    harness.state.unrelatedAccountEmailPending = true;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(
      harness.state.googleAssessments.some((assessment) =>
        assessment.evidence.gmail.sources.some(
          (source) => source.subject === UNRELATED_ACCOUNT_EMAIL_SUBJECT,
        ),
      ),
    ).toBe(true);
    expect(harness.linq.messages).toHaveLength(messagesBeforeUnrelatedAccountEmail);
    expect(harness.linq.messages.some((message) => message.text === UNRELATED_ACCOUNT_EMAIL_ALERT)).toBe(
      false,
    );
    await harness.assertDatabase(
      "An unrelated adult account email source was retained",
      `not exists (
        select 1 from sources
        where kind='gmail' and metadata->>'messageId'='gmail-unrelated-retail-account-alert'
      )`,
    );
    await harness.assertDatabase(
      "An unrelated adult account email became a monitor",
      `not exists (
        select 1 from proactive_work
        where kind='finite_monitor' and objective=${sqlLiteral(UNRELATED_ACCOUNT_MONITOR_OBJECTIVE)}
      )`,
    );
    await harness.assertDatabase(
      "An unrelated adult account email became a fact",
      `not exists (select 1 from facts where slot=${sqlLiteral(UNRELATED_ACCOUNT_FACT_SLOT)})`,
    );
    await harness.assertDatabase(
      "An unrelated adult account email blocked the Gmail cursor",
      `exists (
        select 1 from proactive_work
        where kind='personal_google_poll'
          and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and (gmail_cursor::jsonb->'provider'->>'historyId')::bigint>=102
      )`,
    );

    harness.state.privateFactUpdatePending = true;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    const founderAfterFactUpdate = await harness.florence.workspaceForAdult(harness.founderAdultId);
    const updatedPrivateFact = founderAfterFactUpdate.vault?.facts.find(
      (fact) => fact.statement === UPDATED_PRIVATE_SCHOOL_FACT,
    );
    if (!updatedPrivateFact) {
      throw new Error("The incremental household Gmail fact was not updated");
    }
    expect(updatedPrivateFact).toMatchObject({
      id: initialHouseholdFact.id,
      visibility: "household",
      source: { kind: "gmail" },
    });
    expect(updatedPrivateFact?.source?.id).not.toBe(initialHouseholdFact.source?.id);
    expect(
      (await harness.florence.workspaceForAdult(harness.partnerAdultId)).vault?.facts.find(
        (fact) => fact.id === initialHouseholdFact.id,
      ),
    ).toEqual(
      expect.objectContaining({
        statement: UPDATED_PRIVATE_SCHOOL_FACT,
        visibility: "household",
        source: null,
      }),
    );
    expect(
      harness.state.googleAssessments.find(
        (assessment) => assessment.adult.adultId === harness.founderAdultId,
      )?.currentFacts,
    ).toEqual(
      expect.arrayContaining([{ slot: PRIVATE_SCHOOL_FACT_SLOT, statement: INITIAL_PRIVATE_SCHOOL_FACT }]),
    );
    expect(founderAfterFactUpdate.vault?.facts.find((fact) => fact.id === googleCorrectableFact.id)).toEqual(
      expect.objectContaining({
        statement: GOOGLE_CORRECTED_FACT,
        visibility: "household",
        source: expect.objectContaining({ kind: "web" }),
      }),
    );
    await harness.assertDatabase(
      "The cited incremental Gmail fact did not update its household slot exactly once",
      `(
        select count(*)=1 from sources
        where kind='gmail' and metadata->>'messageId'='gmail-maya-school-enrollment-update'
      ) and (select count(*)=1 from facts
        where household_id=(select household_id from people
          where id=${sqlLiteral(harness.founderAdultId)}::uuid)
          and slot=${sqlLiteral(PRIVATE_SCHOOL_FACT_SLOT)}
          and visibility='household')`,
    );

    harness.state.overlapGmailReadsRemaining = 2;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(harness.state.overlapGmailAssessments).toBe(1);
    if (!harness.state.overlapGmailSourceId) throw new Error("The overlap Gmail source was not observed");
    await harness.assertDatabase(
      "An uncited incremental Gmail overlap was retained or linked as assessed",
      `not exists (
        select 1 from sources where id=${sqlLiteral(harness.state.overlapGmailSourceId)}::uuid
      ) and not exists (
        select 1 from proactive_work_sources link
        join proactive_work work on work.id=link.work_id
        where work.kind='personal_google_poll'
          and work.owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
      ) and exists (
        select 1 from proactive_work
        where kind='personal_google_poll'
          and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and gmail_cursor::jsonb->'provider'->>'historyId'='104'
      )`,
    );

    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(harness.state.overlapGmailAssessments).toBe(2);
    await harness.assertDatabase(
      "A reconsidered and cited Gmail overlap was not retained exactly once",
      `(
        select count(*)=1 from sources
        where id=${sqlLiteral(harness.state.overlapGmailSourceId)}::uuid
      ) and exists (
        select 1 from fact_sources fact_source
        join facts fact on fact.id=fact_source.fact_id
        where fact.slot=${sqlLiteral(OVERLAP_GMAIL_FACT_SLOT)}
          and fact_source.source_id=${sqlLiteral(harness.state.overlapGmailSourceId)}::uuid
      )`,
    );

    const automaticExecution = harness.state.calendarExecutions.find(
      (execution) =>
        execution.mutation.operation === "create" &&
        execution.mutation.event.title === AUTOMATIC_FAMILY_DATE.title,
    );
    expect(automaticExecution).toBeDefined();
    const automaticConfirmation = `Added “${AUTOMATIC_FAMILY_DATE.title}” on the family calendar.`;
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP && message.text === automaticConfirmation,
      ),
    ).toHaveLength(1);
    expect(harness.state.timeline.indexOf(`provider:create:${AUTOMATIC_FAMILY_DATE.title}`)).toBeLessThan(
      harness.state.timeline.indexOf(`message:${automaticConfirmation}`),
    );

    const nativeParts: TestPart[] = [
      { type: "text", value: NATIVE_TEXT },
      { type: "link", value: NATIVE_LINK },
      {
        type: "media",
        id: "school-photo",
        filename: "school-message.jpg",
        mimeType: "image/jpeg",
        bytes: JPEG_BYTES,
      },
      {
        type: "media",
        id: "school-pdf",
        filename: "field-trip-form.pdf",
        mimeType: "application/pdf",
        bytes: PDF_BYTES,
      },
      {
        type: "media",
        id: "school-voice",
        filename: "teacher-note.wav",
        mimeType: "audio/wav",
        bytes: WAV_BYTES,
      },
    ];
    expect(await harness.receiveParts("native-school-update", nativeParts, FAMILY_GROUP)).toEqual({
      disposition: "accepted",
      sourceId: inboundSourceId("event-native-school-update"),
    });
    await harness.drain();
    const visibleAfterFirstDelivery = harness.linq.messages.length;
    expect(await harness.receiveParts("native-school-update", nativeParts, FAMILY_GROUP)).toEqual({
      disposition: "duplicate",
      sourceId: inboundSourceId("event-native-school-update"),
    });
    await harness.drain();
    expect(harness.linq.messages).toHaveLength(visibleAfterFirstDelivery);
    expect(nativeInputWasRead).toBe(true);
    expect(groupHouseholdFactWasVisible).toBe(true);
    expect(nativeObservation).toMatchObject({
      audience: "group",
      imageCount: 1,
      pdfCount: 1,
    });
    expect(Uint8Array.from(nativeObservation.imageBytes ?? [])).toEqual(JPEG_BYTES);
    expect(Uint8Array.from(nativeObservation.pdfBytes ?? [])).toEqual(PDF_BYTES);
    expect(nativeObservation.text).toContain(NATIVE_TEXT);
    expect(nativeObservation.text).toContain(NATIVE_LINK);
    expect(nativeObservation.text).toContain(VOICE_TRANSCRIPT);
    expect(harness.state.voiceTranscriptions).toBe(1);

    const workspace = await harness.florence.workspaceForAdult(harness.founderAdultId);
    expect(workspace.vault?.facts).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          statement: "Maya’s field-trip form is due Tuesday",
          visibility: "household",
          source: expect.objectContaining({
            id: inboundSourceId("event-native-school-update"),
            kind: "message",
          }),
        }),
      ]),
    );
    expect(workspace.vault?.watches).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: "monitor",
          objective: "Watch for confirmation that Maya’s field-trip form is signed.",
          status: "active",
        }),
      ]),
    );

    await harness.accept("group", "soccer-interest", INTEREST_REQUEST, "partner");
    await harness.drain();
    const interests = (
      await harness.florence.workspaceForAdult(harness.founderAdultId)
    ).vault?.watches.filter((watch) => watch.kind === "interest");
    expect(interests).toHaveLength(1);
    expect(interests).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: "interest",
          objective: "Find a worthwhile local soccer outing for the family.",
          status: "active",
        }),
      ]),
    );
    expect(
      await harness.accept("group", "soccer-interest-repeat", INTEREST_REQUEST, "partner"),
    ).toMatchObject({ disposition: "accepted" });
    await harness.drain();
    expect(
      (await harness.florence.workspaceForAdult(harness.founderAdultId)).vault?.watches.filter(
        (watch) => watch.kind === "interest",
      ),
    ).toHaveLength(1);
    await harness.assertDatabase(
      "The repeated interest request was not handled as one ordinary turn",
      `exists (
        select 1 from messages
        where source_id=${sqlLiteral(inboundSourceId("event-soccer-interest-repeat"))}::uuid
          and direction='inbound' and status='handled' and retry_at is null and last_error is null
      )`,
    );

    harness.state.monitorEvidenceExercise = true;
    harness.state.now += 2 * 60 * 60_000;
    await harness.drain();
    expect(harness.state.finiteReviews).toBe(1);
    if (!harness.state.silentMonitorSourceId) {
      throw new Error("The silent monitor did not receive current Calendar evidence");
    }
    expect(
      harness.linq.messages.some(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          message.text === "Maya’s field-trip form is signed—nothing else to do.",
      ),
    ).toBe(false);
    await harness.assertDatabase(
      "A silent monitor retained or linked the Calendar revision it read",
      `not exists (
        select 1 from sources where id=${sqlLiteral(harness.state.silentMonitorSourceId)}::uuid
      ) and not exists (
        select 1 from proactive_work_sources
        where source_id=${sqlLiteral(harness.state.silentMonitorSourceId)}::uuid
      )`,
    );
    expect(harness.state.interestResearches).toBe(1);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          message.text === `${INTEREST_RECOMMENDATION}\n\n${INTEREST_URL}`,
      ),
    ).toHaveLength(1);

    harness.state.now += 2 * 60 * 60_000;
    await harness.drain();
    expect(harness.state.finiteReviews).toBe(2);
    if (!harness.state.voicedMonitorSourceId) {
      throw new Error("The voiced monitor did not cite current Calendar evidence");
    }
    expect(harness.state.voicedMonitorSourceId).toBe(harness.state.silentMonitorSourceId);
    await harness.assertDatabase(
      "A voiced monitor did not retain and link its one current Calendar source",
      `(
        select count(*)=1 from sources
        where id=${sqlLiteral(harness.state.voicedMonitorSourceId)}::uuid
      ) and (
        select count(*)=1 from proactive_work_sources
        where source_id=${sqlLiteral(harness.state.voicedMonitorSourceId)}::uuid
      )`,
    );

    harness.state.monitorCancellationActive = true;
    harness.state.now += 2 * 60 * 60_000;
    await harness.drain();
    expect(harness.state.finiteReviews).toBe(3);
    if (!harness.state.cancelledMonitorSourceId) {
      throw new Error("The completed monitor did not cite the Calendar tombstone");
    }
    expect(harness.state.cancelledMonitorSourceId).toBe(harness.state.voicedMonitorSourceId);
    expect(
      harness.linq.messages.some(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          message.text === "Maya’s field-trip form is signed—nothing else to do.",
      ),
    ).toBe(true);
    const monitoredEvent = [...harness.state.calendarEvents.values()].find(
      (event) => event.title === AUTOMATIC_FAMILY_DATE.title,
    );
    if (!monitoredEvent) throw new Error("The monitored family Calendar event is missing");
    await harness.assertDatabase(
      "The Calendar source did not converge in place to the current cancellation",
      `(
        select count(*)=1 from sources current
        where current.id=${sqlLiteral(harness.state.cancelledMonitorSourceId)}::uuid
          and current.kind='calendar' and current.parent_source_id is null
          and current.metadata->>'providerEventId'=${sqlLiteral(monitoredEvent.providerEventId)}
          and current.metadata->>'providerRevision'=${sqlLiteral(`${monitoredEvent.providerRevision}-cancelled`)}
          and current.metadata->>'status'='cancelled'
          and current.metadata->>'title'=${sqlLiteral(AUTOMATIC_FAMILY_DATE.title)}
          and nullif(current.metadata->>'startsAt','') is not null
          and nullif(current.metadata->>'endsAt','') is not null
          and current.metadata->>'allDay'='true'
          and current.occurred_at=(current.metadata->>'startsAt')::timestamptz
      ) and (
        select count(*)=1 from sources
        where kind='calendar'
          and metadata->>'providerEventId'=${sqlLiteral(monitoredEvent.providerEventId)}
      )`,
    );
    expect((await harness.florence.workspaceForAdult(harness.partnerAdultId)).vault?.watches).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: "interest",
          currentConclusion: INTEREST_RECOMMENDATION,
          status: "active",
        }),
      ]),
    );

    const reconnectText =
      "Your Google connection stopped working. Reconnect it in Florence settings so I can keep helping with family plans.";
    const familyCalendarReconnectText =
      "The family calendar is paused because neither Google account is connected. Either of you can reconnect in Florence settings.";
    harness.state.googleDeletionEvidencePending = true;
    harness.state.now += 2 * 60_000;
    await harness.accept("private", "google-cited-conversational-reply", GOOGLE_CITED_REPLY_QUESTION);
    expect(await harness.florence.runOnce()).toBe(true);
    if (!conversationalGoogleSourceId) {
      throw new Error("The ordinary conversational turn did not read Gmail evidence");
    }
    await harness.assertDatabase(
      "The ordinary Gmail-grounded reply was not waiting with its Google dependency",
      `exists (
        select 1 from messages message join sources response on response.id=message.source_id
        where message.direction='outbound' and message.status='pending'
          and message.text=${sqlLiteral(GOOGLE_CITED_REPLY)}
          and response.parent_source_id=${sqlLiteral(inboundSourceId("event-google-cited-conversational-reply"))}::uuid
          and response.metadata->'googleConnectionIds'
            @> ${sqlLiteral(JSON.stringify([FOUNDER_GOOGLE]))}::jsonb
      )`,
    );
    for (let index = 0; index < 20 && !harness.state.googleDeletionSourceId; index += 1) {
      expect(await harness.florence.runOnce()).toBe(true);
    }
    if (!harness.state.googleDeletionSourceId) {
      throw new Error("The Google-derived deletion exercise was not staged");
    }
    await harness.assertDatabase(
      "The Google-derived alert, watch, fact, and Calendar suggestion were not staged together",
      `exists (
        select 1 from sources
        where id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
          and kind='gmail' and visibility='private'
          and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
      ) and exists (
        select 1 from facts fact join fact_sources link on link.fact_id=fact.id
          join sources source on source.id=link.source_id
        where fact.visibility='household' and fact.owner_adult_id is null
          and fact.slot=${sqlLiteral(PRIVATE_SCHOOL_FACT_SLOT)}
          and source.kind='gmail'
          and source.owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
      ) and exists (
        select 1 from fact_sources link join facts fact on fact.id=link.fact_id
        where link.source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
          and fact.slot=${sqlLiteral(GOOGLE_DELETION_FACT_SLOT)}
      ) and exists (
        select 1 from proactive_work_sources link join proactive_work work on work.id=link.work_id
        where link.source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
          and work.kind='finite_monitor' and work.status='active'
      ) and exists (
        select 1 from calendar_actions
        where basis_source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
          and status='offered'
          and payload->'event'->>'title'=${sqlLiteral(GOOGLE_DELETION_FAMILY_DATE.title)}
      ) and exists (
        select 1 from messages where direction='outbound' and status='pending'
          and text=${sqlLiteral(GOOGLE_DELETION_PRIVATE_ALERT)}
      ) and exists (
        select 1 from messages where direction='outbound' and status='pending'
          and idempotency_key like 'calendar-review-prompt:%'
          and text like ${sqlLiteral(`%${GOOGLE_DELETION_FAMILY_DATE.title}%`)}
      )`,
    );
    const interactiveGoogleReadsBeforeMemory = harness.state.interactiveGoogleReads;
    harness.linq.googleDeletionDeliveryFailuresRemaining = 1;
    await harness.accept("private", "google-memory-conversational-reply", GOOGLE_MEMORY_REPLY_QUESTION);
    expect(await harness.florence.runOnce()).toBe(true);
    if (!retainedGoogleMemorySourceId) {
      throw new Error("The ordinary conversational turn did not use retained Gmail-derived memory");
    }
    expect(harness.state.interactiveGoogleReads).toBe(interactiveGoogleReadsBeforeMemory);
    await harness.assertDatabase(
      "The retained-memory reply was not waiting with its inherited Google dependency",
      `exists (
        select 1 from messages message join sources response on response.id=message.source_id
        where message.direction='outbound' and message.status='pending'
          and message.text=${sqlLiteral(GOOGLE_MEMORY_REPLY)}
          and response.parent_source_id=${sqlLiteral(inboundSourceId("event-google-memory-conversational-reply"))}::uuid
          and response.metadata->'googleConnectionIds'
            @> ${sqlLiteral(JSON.stringify([FOUNDER_GOOGLE]))}::jsonb
      )`,
    );
    const founderHouseholdId = (await harness.store.listHouseholdIdsForAdult(harness.founderAdultId))[0];
    if (!founderHouseholdId) throw new Error("The wrong-account reconnect check needs a household");
    const sessionToken = decodeURIComponent(harness.sessionCookie.split("=")[1] ?? "");
    const browserSessionBinding = digest(`florence-google-session-v1\0${sessionToken}`);
    const cancelledAccountState = "founder-cancelled-google-account-state";
    await harness.store.createPending({
      connectionId: CANCELLED_FOUNDER_GOOGLE,
      householdId: founderHouseholdId,
      ownerAdultId: harness.founderAdultId,
      stateDigest: digest(cancelledAccountState),
      sessionBindingDigest: browserSessionBinding,
      stateExpiresAt: new Date(harness.state.now + 60_000).toISOString(),
      now: harness.iso(),
    });
    const cancelledAccountApp = await harness.webApp();
    const cancelledAccountResponse = await cancelledAccountApp.inject({
      method: "GET",
      url: "/oauth/google/callback?error=access_denied",
      headers: { cookie: harness.sessionCookie },
    });
    await cancelledAccountApp.close();
    expect(cancelledAccountResponse.statusCode).toBe(302);
    expect(cancelledAccountResponse.headers.location).toBe("/?google=authorization_cancelled");
    await harness.assertDatabase(
      "Cancelling reauthorization changed the active account or its derived work",
      `exists (
          select 1 from google_connections where id=${sqlLiteral(FOUNDER_GOOGLE)}::uuid
            and status='active'
        ) and exists (
          select 1 from google_connections where id=${sqlLiteral(CANCELLED_FOUNDER_GOOGLE)}::uuid
            and status='pending' and state_consumed_at is null
        ) and exists (
          select 1 from facts fact join fact_sources link on link.fact_id=fact.id
          where fact.slot=${sqlLiteral(GOOGLE_DELETION_FACT_SLOT)}
            and link.source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
        ) and exists (
          select 1 from proactive_work_sources link join proactive_work work on work.id=link.work_id
          where link.source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
            and work.kind='finite_monitor' and work.status='active'
        ) and exists (
          select 1 from calendar_actions
          where basis_source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
            and status='offered'
        ) and exists (
          select 1 from messages where direction='outbound' and status='pending'
            and text=${sqlLiteral(GOOGLE_MEMORY_REPLY)}
        )`,
    );
    const wrongAccountState = "founder-wrong-google-account-state";
    await harness.store.createPending({
      connectionId: WRONG_FOUNDER_GOOGLE,
      householdId: founderHouseholdId,
      ownerAdultId: harness.founderAdultId,
      stateDigest: digest(wrongAccountState),
      sessionBindingDigest: browserSessionBinding,
      stateExpiresAt: new Date(harness.state.now + 60_000).toISOString(),
      now: harness.iso(),
    });
    harness.state.wrongGoogleSubjectNext = true;
    const wrongAccountApp = await harness.webApp();
    const wrongAccountResponse = await wrongAccountApp.inject({
      method: "GET",
      url: `/oauth/google/callback?state=${encodeURIComponent(wrongAccountState)}&code=wrong-account-code`,
      headers: { cookie: harness.sessionCookie },
    });
    await wrongAccountApp.close();
    expect(wrongAccountResponse.statusCode).toBe(302);
    expect(wrongAccountResponse.headers.location).toBe("/?google=identity_conflict");
    await harness.assertDatabase(
      "A cancelled wrong-account reconnect replaced the active account or destroyed queued work",
      `not exists (
          select 1 from google_connections where id=${sqlLiteral(WRONG_FOUNDER_GOOGLE)}::uuid
            and status='active'
        ) and exists (
          select 1 from google_connections where id=${sqlLiteral(FOUNDER_GOOGLE)}::uuid
            and status='active'
        ) and exists (
          select 1 from messages where direction='outbound' and status='pending'
            and text=${sqlLiteral(GOOGLE_MEMORY_REPLY)}
        ) and exists (
          select 1 from calendar_actions
          where basis_source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
            and status='offered'
        ) and exists (
          select 1 from facts fact join fact_sources link on link.fact_id=fact.id
          where fact.slot=${sqlLiteral(GOOGLE_DELETION_FACT_SLOT)}
            and link.source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
        ) and exists (
          select 1 from proactive_work_sources link join proactive_work work on work.id=link.work_id
          where link.source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
            and work.kind='finite_monitor' and work.status='active'
        ) and exists (
          select 1 from google_connections where id=${sqlLiteral(WRONG_FOUNDER_GOOGLE)}::uuid
            and status='pending' and state_consumed_at is not null
        )`,
    );
    await harness.activateGoogle(
      harness.founderAdultId,
      SAFE_REAUTHORIZED_FOUNDER_GOOGLE,
      "founder-safe-reauthorization-state",
    );
    await harness.assertDatabase(
      "A successful same-account reauthorization did not replace the credential atomically",
      `exists (
          select 1 from google_connections where id=${sqlLiteral(FOUNDER_GOOGLE)}::uuid
            and status='disconnected' and refresh_token_envelope is null
        ) and exists (
          select 1 from google_connections where id=${sqlLiteral(SAFE_REAUTHORIZED_FOUNDER_GOOGLE)}::uuid
            and status='active' and google_subject_digest is not null
        ) and not exists (
          select 1 from google_connections
          where id in (
            ${sqlLiteral(CANCELLED_FOUNDER_GOOGLE)}::uuid,
            ${sqlLiteral(WRONG_FOUNDER_GOOGLE)}::uuid
          ) and (status='pending' or session_binding_digest is not null)
        )`,
    );
    await harness.assertDatabase(
      "A successful same-account reauthorization lost Family Calendar or evidence lineage",
      `exists (
          select 1 from households
          where id=${sqlLiteral(founderHouseholdId)}::uuid
            and family_calendar_owner_connection_id=${sqlLiteral(SAFE_REAUTHORIZED_FOUNDER_GOOGLE)}::uuid
        ) and exists (
          select 1 from sources evidence
          join google_connections historical
            on historical.id::text=evidence.metadata->>'connectionId'
          join google_connections active
            on active.id=${sqlLiteral(SAFE_REAUTHORIZED_FOUNDER_GOOGLE)}::uuid
            and active.household_id=historical.household_id
            and active.owner_adult_id=historical.owner_adult_id
            and active.google_subject_digest=historical.google_subject_digest
            and active.google_subject_digest is not null and active.status='active'
          where evidence.id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
        )`,
    );
    await harness.assertDatabase(
      "A successful same-account reauthorization did not reset its private review atomically",
      `exists (
          select 1 from proactive_work
          where household_id=${sqlLiteral(founderHouseholdId)}::uuid
            and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
            and kind='initial_private_review' and status='active'
            and next_check_at is not null
        )`,
    );
    await harness.assertDatabase(
      "A successful same-account reauthorization did not pause its old Google poll atomically",
      `exists (
          select 1 from proactive_work
          where household_id=${sqlLiteral(founderHouseholdId)}::uuid
            and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
            and kind='personal_google_poll' and status='paused' and next_check_at is null
        )`,
    );
    await harness.assertDatabase(
      "A successful same-account reauthorization destroyed retained Google-derived work",
      `exists (
          select 1 from facts fact join fact_sources link on link.fact_id=fact.id
          where fact.slot=${sqlLiteral(GOOGLE_DELETION_FACT_SLOT)}
            and link.source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
        ) and exists (
          select 1 from proactive_work_sources link join proactive_work work on work.id=link.work_id
          where link.source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
            and work.kind='finite_monitor' and work.status='active'
        ) and exists (
          select 1 from calendar_actions
          where basis_source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
            and status='offered'
        ) and exists (
          select 1 from messages where direction='outbound' and status='pending'
            and text=${sqlLiteral(GOOGLE_MEMORY_REPLY)}
        )`,
    );
    await harness.store.createPending({
      connectionId: ABANDONED_FOUNDER_GOOGLE,
      householdId: founderHouseholdId,
      ownerAdultId: harness.founderAdultId,
      stateDigest: digest("founder-abandoned-google-state"),
      sessionBindingDigest: browserSessionBinding,
      stateExpiresAt: new Date(harness.state.now + 60_000).toISOString(),
      now: harness.iso(),
    });
    const founderGoogleReadsBeforeDisconnect = harness.state.googleChangeReads.filter(
      (read) => read.ownerAdultId === harness.founderAdultId,
    ).length;
    harness.state.providerRevocations.push("unconfirmed");
    const disconnectApp = await harness.webApp();
    const disconnectResponse = await disconnectApp.inject({
      method: "DELETE",
      url: "/api/v1/workspace/google-connections",
      headers: { cookie: harness.sessionCookie },
      payload: { connectionId: SAFE_REAUTHORIZED_FOUNDER_GOOGLE },
    });
    await disconnectApp.close();
    const linqMessagesAfterDisconnect = harness.linq.messages.length;
    expect(disconnectResponse.statusCode).toBe(200);
    expect(disconnectResponse.json()).toMatchObject({
      localAccess: "disconnected",
      providerRevocation: "unconfirmed",
      workspace: {
        workspace: { googleConnections: [] },
        vault: {
          facts: expect.arrayContaining([
            expect.objectContaining({ statement: GOOGLE_DELETION_FACT, visibility: "household" }),
          ]),
          watches: expect.arrayContaining([
            expect.objectContaining({
              kind: "monitor",
              objective: "Watch for confirmation that Maya’s emergency card is signed.",
            }),
          ]),
        },
      },
    });
    await harness.assertDatabase(
      "Ordinary disconnect deleted retained Google-derived data",
      `exists (
        select 1 from sources
        where id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid and kind='gmail'
      ) and exists (
        select 1 from fact_sources where source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
      ) and exists (
        select 1 from proactive_work_sources link join proactive_work work on work.id=link.work_id
        where link.source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
          and work.kind='finite_monitor'
      )`,
    );
    await harness.assertDatabase(
      "Ordinary disconnect did not remove only noncommitted Google Calendar work",
      `not exists (
        select 1 from calendar_actions
        where basis_source_id=${sqlLiteral(harness.state.googleDeletionSourceId)}::uuid
          and status in ('offered','pending','failed')
      ) and exists (
        select 1 from calendar_actions
        where status='committed'
          and payload->'event'->>'title'=${sqlLiteral(AUTOMATIC_FAMILY_DATE.title)}
      )`,
    );
    await harness.assertDatabase(
      "Ordinary disconnect left a stale reauthorization state consumable",
      `not exists (
        select 1 from google_connections
        where household_id=${sqlLiteral(founderHouseholdId)}::uuid
          and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and (status='pending' or session_binding_digest is not null)
      )`,
    );
    await harness.assertDatabase(
      "Ordinary disconnect left a Google-dependent outbound deliverable",
      `exists (
        select 1 from messages
        where direction='outbound' and status='failed'
          and text=${sqlLiteral(GOOGLE_MEMORY_REPLY)}
      ) and not exists (
        select 1 from messages where direction='outbound' and status in ('pending','sending')
          and (
            text=${sqlLiteral(GOOGLE_DELETION_PRIVATE_ALERT)}
            or text=${sqlLiteral(GOOGLE_CITED_REPLY)}
            or text=${sqlLiteral(GOOGLE_MEMORY_REPLY)}
            or (
              idempotency_key like 'calendar-review-prompt:%'
              and text like ${sqlLiteral(`%${GOOGLE_DELETION_FAMILY_DATE.title}%`)}
            )
          )
      )`,
    );
    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(
      harness.state.googleChangeReads.filter((read) => read.ownerAdultId === harness.founderAdultId),
    ).toHaveLength(founderGoogleReadsBeforeDisconnect);
    expect(
      harness.linq.messages
        .slice(linqMessagesAfterDisconnect)
        .some(
          (message) =>
            message.text === GOOGLE_DELETION_PRIVATE_ALERT ||
            message.text === GOOGLE_CITED_REPLY ||
            message.text === GOOGLE_MEMORY_REPLY ||
            message.text.includes(GOOGLE_DELETION_FAMILY_DATE.title),
        ),
    ).toBe(false);
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === PRIVATE_FOUNDER && message.text === reconnectText,
      ),
    ).toHaveLength(0);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP && message.text === familyCalendarReconnectText,
      ),
    ).toHaveLength(0);
    expect(
      [...harness.state.calendarEvents.values()].some((event) => event.title === AUTOMATIC_FAMILY_DATE.title),
    ).toBe(true);

    const stableSchoolSourceDigest = digest(
      `gmail\0${harness.founderAdultId}\0${SCHOOL_ATTACHMENT.messageId}`,
    );
    const stableSchoolActionKey = digest(
      JSON.stringify({
        version: 2,
        providers: [stableSchoolSourceDigest],
        category: "loose_end",
        dueAt: "2026-08-19T16:00:00.000Z",
        actionAnchorDigest: digest("field-trip form"),
      }),
    );
    await harness.assertDatabase(
      "The delivered initial review is no longer recorded as sent",
      `exists (
        select 1 from messages
        where status='sent' and idempotency_key like 'initial-private-review:%'
      )`,
    );
    await harness.assertDatabase(
      "The delivered initial review did not retain any provider-stable action keys",
      `exists (
        select 1 from messages message join sources source on source.id=message.source_id
        where message.status='sent'
          and jsonb_typeof(source.metadata->'googleActionKeys')='array'
      )`,
    );
    await harness.assertDatabase(
      "The delivered initial review did not retain its expected provider-stable action key",
      `exists (
        select 1 from messages message join sources source on source.id=message.source_id
        where message.status='sent' and source.visibility='private'
          and jsonb_typeof(source.metadata->'googleActionKeys')='array'
          and jsonb_exists(source.metadata->'googleActionKeys',${sqlLiteral(stableSchoolActionKey)})
      )`,
    );

    const messagesBeforeSameSubjectRescan = harness.linq.messages.length;
    const preActivationExecutionsBeforeRescan = harness.state.calendarExecutions.filter(
      (execution) =>
        execution.mutation.operation === "create" &&
        execution.mutation.event.title === PRE_ACTIVATION_FAMILY_DATE.title,
    );
    expect(preActivationExecutionsBeforeRescan).toHaveLength(1);
    const preActivationActionId = preActivationExecutionsBeforeRescan[0]?.actionId;
    if (!preActivationActionId) throw new Error("The provider-stable Calendar action is missing its ID");
    await harness.activateGoogle(
      harness.founderAdultId,
      RECONNECTED_FOUNDER_GOOGLE,
      "founder-google-reconnected-state",
    );
    await harness.drain();
    expect(
      harness.linq.messages
        .slice(messagesBeforeSameSubjectRescan)
        .filter((message) => !message.idempotencyKey.startsWith("calendar-confirmation:")),
    ).toEqual([]);
    expect(
      harness.state.calendarExecutions
        .filter(
          (execution) =>
            execution.mutation.operation === "create" &&
            execution.mutation.event.title === PRE_ACTIVATION_FAMILY_DATE.title,
        )
        .map((execution) => execution.actionId),
    ).toEqual([preActivationActionId]);
    await harness.assertDatabase(
      "A same-account reconnect duplicated a provider-stable watch",
      `(select count(distinct work.id)<=2 from proactive_work work
          join proactive_work_sources link on link.work_id=work.id
          join sources source on source.id=link.source_id
          where work.kind='finite_monitor' and work.status='active'
            and work.owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
            and source.kind='gmail'
            and source.metadata->>'messageId'=${sqlLiteral(SCHOOL_ATTACHMENT.messageId)})`,
    );
    await harness.assertDatabase(
      "A same-account reconnect dropped a provider-stable watch",
      `(select count(distinct work.id)>=2 from proactive_work work
          join proactive_work_sources link on link.work_id=work.id
          join sources source on source.id=link.source_id
          where work.kind='finite_monitor' and work.status='active'
            and work.owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
            and source.kind='gmail'
            and source.metadata->>'messageId'=${sqlLiteral(SCHOOL_ATTACHMENT.messageId)})`,
    );
    await harness.assertDatabase(
      "A same-account reconnect duplicated or dropped a handled Calendar action",
      `(select count(*)=1 from calendar_actions
        where payload->'event'->>'title'=${sqlLiteral(PRE_ACTIVATION_FAMILY_DATE.title)})
        and not exists (
          select 1 from calendar_actions action join sources basis on basis.id=action.basis_source_id
          where action.payload->'event'->>'title'=${sqlLiteral(PRE_ACTIVATION_FAMILY_DATE.title)}
            and basis.metadata->>'connectionId'=${sqlLiteral(RECONNECTED_FOUNDER_GOOGLE)}
        )`,
    );
    await harness.assertDatabase(
      "A same-account reconnect repeated a provider-stable delivered finding",
      `not exists (
          select 1 from messages message join sources source on source.id=message.source_id
          where message.status='sent' and source.metadata->'googleActionKeys' is not null
            and message.text like '%still waiting on Hari%'
        )`,
    );
    harness.state.providerRevocations.push("confirmed");
    const deleteApp = await harness.webApp();
    const deleteResponse = await deleteApp.inject({
      method: "DELETE",
      url: "/api/v1/workspace/google-derived-data",
      headers: { cookie: harness.sessionCookie },
    });
    expect(deleteResponse.statusCode).toBe(200);
    const deleted = deleteResponse.json();
    expect(deleted).toMatchObject({
      providerRevocation: "confirmed",
      workspace: { workspace: { googleConnections: [] } },
    });
    expect(deleted.deletion.disconnectedConnections).toBe(1);
    expect(deleted.deletion.googleSources).toBeGreaterThan(0);
    expect(deleted.deletion.facts).toBeGreaterThan(0);
    expect(deleted.deletion.watches).toBeGreaterThan(0);
    expect(deleted.deletion.calendarActions).toBe(1);
    expect(deleted.deletion.unsentMessages).toBeGreaterThan(0);
    expect(
      harness.state.calendarExecutions
        .filter(
          (execution) =>
            execution.mutation.operation === "create" &&
            execution.mutation.event.title === PRE_ACTIVATION_FAMILY_DATE.title,
        )
        .map((execution) => execution.actionId),
    ).toEqual([preActivationActionId]);
    expect(
      [...harness.state.calendarEvents.values()].some(
        (event) => event.title === PRE_ACTIVATION_FAMILY_DATE.title,
      ),
    ).toBe(true);
    expect(
      deleted.workspace.vault.facts.some(
        (fact: { statement: string }) => fact.statement === GOOGLE_DELETION_FACT,
      ),
    ).toBe(false);
    expect(deleted.workspace.vault.facts).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          statement: SHARED_SCHOOL_CONTACT_FACT,
          visibility: "household",
          source: null,
        }),
        expect.objectContaining({
          statement: GOOGLE_CORRECTED_FACT,
          visibility: "household",
          source: expect.objectContaining({ kind: "web" }),
        }),
      ]),
    );
    expect(
      deleted.workspace.vault.watches.some(
        (watch: { objective: string }) =>
          watch.objective === "Watch for confirmation that Maya’s emergency card is signed.",
      ),
    ).toBe(false);
    await harness.assertDatabase(
      "Deleting one adult's Google-derived data crossed its boundary or left linked data behind",
      `not exists (
        select 1 from sources
        where owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and kind in ('gmail','calendar')
      ) and not exists (
        select 1 from facts
        where slot in (${sqlLiteral(PRIVATE_SCHOOL_FACT_SLOT)},${sqlLiteral(GOOGLE_DELETION_FACT_SLOT)})
      ) and not exists (
        select 1 from proactive_work
        where owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and kind in ('initial_private_review','personal_google_poll','finite_monitor')
      ) and not exists (
        select 1 from calendar_actions
        where payload->'event'->>'title'=${sqlLiteral(GOOGLE_DELETION_FAMILY_DATE.title)}
      ) and not exists (
        select 1 from messages
        where direction='outbound' and status<>'sent' and (
          text=${sqlLiteral(GOOGLE_DELETION_PRIVATE_ALERT)}
            or text=${sqlLiteral(GOOGLE_CITED_REPLY)}
            or text=${sqlLiteral(GOOGLE_MEMORY_REPLY)}
            or (
              idempotency_key like 'calendar-review-prompt:%'
              and text like ${sqlLiteral(`%${GOOGLE_DELETION_FAMILY_DATE.title}%`)}
            )
        )
      ) and exists (
        select 1 from messages
        where direction='outbound' and status='sent'
          and text=${sqlLiteral(GOOGLE_DELETION_PRIVATE_ALERT)}
      ) and exists (
        select 1 from sources
        where owner_adult_id=${sqlLiteral(harness.partnerAdultId)}::uuid and kind='gmail'
      ) and exists (
        select 1 from facts
        where visibility='household' and owner_adult_id is null
          and slot=${sqlLiteral(PARTNER_PRIVATE_GOOGLE_FACT_SLOT)}
      ) and exists (
        select 1 from facts fact join fact_sources link on link.fact_id=fact.id
          join sources source on source.id=link.source_id
        where fact.visibility='household' and fact.owner_adult_id is null
          and fact.slot=${sqlLiteral(SHARED_SCHOOL_CONTACT_SLOT)}
          and source.owner_adult_id=${sqlLiteral(harness.partnerAdultId)}::uuid
          and source.kind='gmail'
      ) and not exists (
        select 1 from facts fact join fact_sources link on link.fact_id=fact.id
          join sources source on source.id=link.source_id
        where fact.slot=${sqlLiteral(SHARED_SCHOOL_CONTACT_SLOT)}
          and source.owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
      ) and exists (
        select 1 from messages
        where direction='outbound' and status='sent'
          and text like 'Here’s what’s on the docket:%'
      ) and exists (
        select 1 from messages
        where direction='outbound' and status='sent'
          and text like '%Hari’s private school email has the original form.%'
      ) and exists (
        select 1 from messages
        where direction='outbound' and status='sent'
          and text=${sqlLiteral(automaticConfirmation)}
      )`,
    );
    await harness.assertDatabase(
      "Deleting Google-derived data left recoverable provider identity or credential state",
      `exists (
        select 1 from google_connections
        where owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
      ) and not exists (
        select 1 from google_connections
        where owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and (
            status<>'disconnected'
            or refresh_token_envelope is not null
            or email_label is not null
            or google_subject_digest is not null
            or session_binding_digest is not null
            or state_consumed_at is not null
            or last_error is not null
            or cardinality(granted_scopes)<>0
          )
      )`,
    );
    expect((await harness.florence.workspaceForAdult(harness.partnerAdultId)).vault?.facts).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ statement: PARTNER_PRIVATE_GOOGLE_FACT, visibility: "household" }),
      ]),
    );
    expect(
      [...harness.state.calendarEvents.values()].some((event) => event.title === AUTOMATIC_FAMILY_DATE.title),
    ).toBe(true);

    const duplicateDeleteResponse = await deleteApp.inject({
      method: "DELETE",
      url: "/api/v1/workspace/google-derived-data",
      headers: { cookie: harness.sessionCookie },
    });
    await deleteApp.close();
    expect(duplicateDeleteResponse.statusCode).toBe(200);
    expect(duplicateDeleteResponse.json()).toMatchObject({
      providerRevocation: "not-needed",
      deletion: {
        disconnectedConnections: 0,
        googleSources: 0,
        facts: 0,
        watches: 0,
        calendarActions: 0,
        unsentMessages: 0,
      },
    });

    const app = await harness.webApp();
    const response = await app.inject({
      method: "GET",
      url: "/api/v1/calendar?month=2026-08",
      headers: { cookie: harness.sessionCookie },
    });
    await app.close();
    expect(response.statusCode).toBe(200);
    expect(response.headers["cache-control"]).toBe("private, no-store");
    expect(response.json()).toMatchObject({
      status: "ready",
      month: "2026-08",
      calendarName: "Anbarasu Family",
      events: expect.arrayContaining([
        expect.objectContaining({ title: AUTOMATIC_FAMILY_DATE.title }),
        expect.objectContaining({ title: PRE_ACTIVATION_FAMILY_DATE.title }),
      ]),
    });

    harness.state.invalidGrantAdultId = harness.partnerAdultId;
    harness.state.invalidGrantTriggered = false;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(harness.state.invalidGrantTriggered).toBe(true);
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === PRIVATE_PARTNER && message.text === reconnectText,
      ),
    ).toHaveLength(1);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP && message.text === familyCalendarReconnectText,
      ),
    ).toHaveLength(1);
    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP && message.text === familyCalendarReconnectText,
      ),
    ).toHaveLength(1);
    const unavailableApp = await harness.webApp();
    const unavailableResponse = await unavailableApp.inject({
      method: "GET",
      url: "/api/v1/calendar?month=2026-08",
      headers: { cookie: harness.sessionCookie },
    });
    await unavailableApp.close();
    expect(unavailableResponse.statusCode).toBe(200);
    expect(unavailableResponse.json()).toMatchObject({
      status: "temporarily_unavailable",
      month: "2026-08",
      calendarName: "Anbarasu Family",
    });

    await harness.activateGoogle(
      harness.founderAdultId,
      POST_DELETION_FOUNDER_GOOGLE,
      "founder-google-after-deletion-state",
    );
    await harness.assertDatabase(
      "A verified founder reconnect did not safely rebind the existing Family Calendar",
      `exists (
        select 1 from households household
          join google_connections connection
            on connection.id=household.family_calendar_owner_connection_id
        where household.family_calendar_id=${sqlLiteral(FAMILY_CALENDAR)}
          and household.family_calendar_owner_connection_id=${sqlLiteral(POST_DELETION_FOUNDER_GOOGLE)}::uuid
          and connection.owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and connection.status='active'
      )`,
    );

    const calendarOnlyHarness = await createHarness();
    calendarOnlyHarness.state.initialCalendarOnlyReview = true;
    await calendarOnlyHarness.readyHousehold();
    const initialCalendarOnlyBubble = calendarOnlyHarness.linq.messages.find(
      (message) =>
        message.providerConversationId === PRIVATE_FOUNDER &&
        message.text.includes(PRIVATE_CALENDAR_ONLY_TITLE),
    )?.text;
    expect(initialCalendarOnlyBubble).toContain(PRIVATE_CALENDAR_ONLY_TITLE);
    expect(initialCalendarOnlyBubble).toContain(PRIVATE_CALENDAR_CONFLICT_TITLE);
    expect(initialCalendarOnlyBubble).toContain("Monday, Aug 17");
    expect(initialCalendarOnlyBubble).toContain("7:30");
    expect(initialCalendarOnlyBubble).toContain("9:00");
    expect(initialCalendarOnlyBubble).toContain("PDT");
    expect(initialCalendarOnlyBubble).not.toMatch(/today|confirmed private calendar commitment/i);
    expect(initialCalendarOnlyBubble).not.toMatch(/private school form|hari-private@example\.com/i);
    expect(
      calendarOnlyHarness.linq.messages
        .filter((message) => message.providerConversationId === FAMILY_GROUP)
        .map((message) => message.text)
        .join("\n"),
    ).not.toMatch(new RegExp(`${PRIVATE_CALENDAR_ONLY_TITLE}|${PRIVATE_CALENDAR_CONFLICT_TITLE}`, "i"));
    calendarOnlyHarness.state.now = Date.parse("2026-08-23T18:00:00.000Z");
    calendarOnlyHarness.state.calendarOnlyChangePending = true;
    const calendarOnlyMessagesBeforePoll = calendarOnlyHarness.linq.messages.length;
    await calendarOnlyHarness.drain();
    expect(calendarOnlyHarness.state.calendarOnlyChangeDelivered).toBe(true);
    const calendarOnlyAssessment = calendarOnlyHarness.state.googleAssessments.find((assessment) =>
      assessment.evidence.calendar.events.some((event) => event.title === PRIVATE_CALENDAR_ONLY_TITLE),
    );
    expect(calendarOnlyAssessment?.evidence.gmail.sources).toEqual([]);
    expect(calendarOnlyAssessment?.evidence.calendar.events).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          title: PRIVATE_CALENDAR_ONLY_TITLE,
          startsAt: PRIVATE_CALENDAR_ONLY_EVENT.startsAt,
          endsAt: PRIVATE_CALENDAR_ONLY_EVENT.endsAt,
        }),
        expect.objectContaining({
          title: PRIVATE_CALENDAR_CONFLICT_TITLE,
          startsAt: PRIVATE_CALENDAR_CONFLICT_EVENT.startsAt,
          endsAt: PRIVATE_CALENDAR_CONFLICT_EVENT.endsAt,
        }),
      ]),
    );
    const calendarOnlyBubble = calendarOnlyHarness.linq.messages
      .slice(calendarOnlyMessagesBeforePoll)
      .find((message) => message.providerConversationId === PRIVATE_FOUNDER)?.text;
    expect(calendarOnlyBubble).toContain(PRIVATE_CALENDAR_ONLY_TITLE);
    expect(calendarOnlyBubble).toContain(PRIVATE_CALENDAR_CONFLICT_TITLE);
    expect(calendarOnlyBubble).toContain("Monday, Aug 24");
    expect(calendarOnlyBubble).toContain("7:30");
    expect(calendarOnlyBubble).toContain("9:00");
    expect(calendarOnlyBubble).toContain("PDT");
    expect(calendarOnlyBubble).not.toMatch(/today|confirmed private calendar commitment/i);
    const founderCalendarFact = (
      await calendarOnlyHarness.florence.workspaceForAdult(calendarOnlyHarness.founderAdultId)
    ).vault?.facts.find((fact) => fact.statement === PRIVATE_CALENDAR_FACT);
    expect(founderCalendarFact).toEqual(
      expect.objectContaining({ visibility: "private", source: expect.objectContaining({}) }),
    );
    expect(
      (
        await calendarOnlyHarness.florence.workspaceForAdult(calendarOnlyHarness.partnerAdultId)
      ).vault?.facts.some((fact) => fact.statement === PRIVATE_CALENDAR_FACT),
    ).toBe(false);

    const initialBoundaryHarness = await createHarness();
    initialBoundaryHarness.state.initialUnrelatedAccountReview = true;
    await initialBoundaryHarness.readyHousehold();
    expect(
      initialBoundaryHarness.linq.messages.some(
        (message) =>
          message.text === UNRELATED_ACCOUNT_EMAIL_ALERT ||
          message.text.includes("retail account password changed"),
      ),
    ).toBe(false);
    expect(
      initialBoundaryHarness.linq.messages.some(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER && message.text === PRIVATE_INITIAL_ALL_CLEAR,
      ),
    ).toBe(true);
    const initialBoundaryWorkspace = await initialBoundaryHarness.florence.workspaceForAdult(
      initialBoundaryHarness.founderAdultId,
    );
    expect(
      initialBoundaryWorkspace.vault?.facts.some((fact) => fact.statement === UNRELATED_ACCOUNT_FACT),
    ).toBe(false);
    expect(
      initialBoundaryWorkspace.vault?.watches.some(
        (watch) => watch.objective === UNRELATED_ACCOUNT_MONITOR_OBJECTIVE,
      ),
    ).toBe(false);
    await initialBoundaryHarness.assertDatabase(
      "An adult-only initial Google finding crossed Florence's family relevance boundary",
      `not exists (
        select 1 from sources
        where kind='gmail' and metadata->>'messageId'='gmail-initial-unrelated-retail-account-alert'
      ) and not exists (
        select 1 from facts where slot=${sqlLiteral(UNRELATED_ACCOUNT_FACT_SLOT)}
      ) and not exists (
        select 1 from proactive_work
        where kind='finite_monitor' and objective=${sqlLiteral(UNRELATED_ACCOUNT_MONITOR_OBJECTIVE)}
      )`,
    );

    const initialFactOnlyBoundaryHarness = await createHarness();
    initialFactOnlyBoundaryHarness.state.initialUnrelatedAccountFactOnlyReview = true;
    await initialFactOnlyBoundaryHarness.readyHousehold();
    expect(
      initialFactOnlyBoundaryHarness.linq.messages.some(
        (message) =>
          message.text === UNRELATED_ACCOUNT_EMAIL_ALERT ||
          message.text.includes("retail account password changed"),
      ),
    ).toBe(false);
    expect(
      initialFactOnlyBoundaryHarness.linq.messages.some(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER && message.text === PRIVATE_INITIAL_ALL_CLEAR,
      ),
    ).toBe(true);
    await initialFactOnlyBoundaryHarness.assertDatabase(
      "An adult-only fact-only initial review crossed Florence's family relevance boundary",
      `not exists (
        select 1 from sources
        where kind='gmail' and metadata->>'messageId'='gmail-initial-unrelated-retail-account-alert'
      ) and not exists (
        select 1 from facts where slot=${sqlLiteral(UNRELATED_ACCOUNT_FACT_SLOT)}
      )`,
    );
  }, 90_000);

  test("keeps private context isolated while both parents can manage shared memory, Calendar, and group repair", async () => {
    const harness = await createHarness(async (input, reads) => {
      const text = input.currentMessage.text;
      if (text === "My private backup contact is Sam.") {
        return decision({ facts: [remember(text, input.currentMessage.sourceId)] });
      }
      if (text === "Tell Alex that pickup is at 3:15.") {
        return decision({
          bubbles: [{ text: "I told Alex.", delayMs: 0 }],
          householdUpdate: {
            text: "Hari says pickup is at 3:15.",
            sourceIds: [input.currentMessage.sourceId],
          },
        });
      }
      if (text === "Pickup is at 2:45.") {
        return decision({ facts: [remember(text, input.currentMessage.sourceId)] });
      }
      const pickupMemory = input.visibleSources.find(
        (source) => source.kind === "memory" && source.text.includes("Pickup is at"),
      );
      if (text === "Actually pickup is at 3:00." && pickupMemory?.recordId) {
        return decision({
          facts: [
            {
              operation: "correct",
              factId: pickupMemory.recordId,
              statement: "Pickup is at 3:00.",
              sourceIds: [input.currentMessage.sourceId],
            },
          ],
        });
      }
      if (text === "We no longer need the pickup note." && pickupMemory?.recordId) {
        return decision({
          facts: [
            {
              operation: "forget",
              factId: pickupMemory.recordId,
              statement: null,
              sourceIds: [input.currentMessage.sourceId],
            },
          ],
        });
      }
      if (text === "Add Maya pickup to the family calendar.") {
        return decision({
          calendar: calendarDecision(input.currentMessage.sourceId, {
            operation: "create",
            event: PICKUP_EVENT,
            target: null,
          }),
        });
      }
      if (text === "Move Maya pickup to 3:00.") {
        const target = await calendarTarget(reads, input, PICKUP_EVENT);
        return decision({
          calendar: calendarDecision(input.currentMessage.sourceId, {
            operation: "update",
            event: UPDATED_PICKUP_EVENT,
            target,
          }),
        });
      }
      if (text === "Remove Maya pickup from the family calendar.") {
        const target = await calendarTarget(reads, input, UPDATED_PICKUP_EVENT);
        return decision({
          calendar: calendarDecision(input.currentMessage.sourceId, {
            operation: "delete",
            event: null,
            target,
          }),
        });
      }
      return decision();
    });
    await harness.readyHousehold();

    const messagesBeforePrivateCalendarAnniversary = harness.linq.messages.length;
    harness.state.privateCalendarAnniversaryPending = true;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(harness.state.privateCalendarAnniversaryDelivered).toBe(true);
    const privateCalendarAnniversaryMessages = harness.linq.messages.slice(
      messagesBeforePrivateCalendarAnniversary,
    );
    expect(
      privateCalendarAnniversaryMessages.some(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER &&
          message.text.includes(PRIVATE_CALENDAR_ANNIVERSARY_TITLE),
      ),
    ).toBe(true);
    expect(
      privateCalendarAnniversaryMessages
        .filter((message) => message.providerConversationId === FAMILY_GROUP)
        .map((message) => message.text)
        .join("\n"),
    ).not.toContain(PRIVATE_CALENDAR_ANNIVERSARY_TITLE);
    expect(
      privateCalendarAnniversaryMessages
        .filter((message) => message.providerConversationId === PRIVATE_FOUNDER)
        .map((message) => message.text)
        .join("\n"),
    ).not.toContain(PRIVATE_CALENDAR_ANNIVERSARY_EVENT.location);
    expect(
      harness.state.calendarExecutions.filter(
        (execution) =>
          execution.mutation.operation === "create" &&
          execution.mutation.event.title === PRIVATE_CALENDAR_ANNIVERSARY_TITLE,
      ),
    ).toHaveLength(0);
    expect(
      [...harness.state.calendarEvents.values()].filter(
        (event) => event.title === PRIVATE_CALENDAR_ANNIVERSARY_TITLE,
      ),
    ).toHaveLength(0);
    await harness.assertDatabase(
      "A personal Calendar date crossed the owner-private approval boundary",
      `(select count(*)=1 from calendar_actions action
        join sources basis on basis.id=action.basis_source_id
        join messages prompt on prompt.source_id=action.approval_prompt_source_id
        join linq_channels channel on channel.id=prompt.channel_id
        where action.status='offered' and action.approval_source_id is null
          and action.payload->'event'->>'title'=${sqlLiteral(PRIVATE_CALENDAR_ANNIVERSARY_TITLE)}
          and basis.kind='calendar' and basis.visibility='private'
          and basis.owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and prompt.direction='outbound' and prompt.status='sent'
          and channel.audience='private' and channel.adult_one_id=${sqlLiteral(
            harness.founderAdultId,
          )}::uuid and channel.adult_two_id is null)`,
    );

    await harness.accept(
      "private",
      "partner-cannot-approve-founders-private-calendar",
      PRIVATE_CALENDAR_OWNER_APPROVAL,
      "partner",
    );
    await harness.drain();
    await harness.accept(
      "group",
      "group-cannot-approve-private-calendar",
      PRIVATE_CALENDAR_OWNER_APPROVAL,
      "partner",
    );
    await harness.drain();
    expect(
      harness.state.calendarExecutions.filter(
        (execution) =>
          execution.mutation.operation === "create" &&
          execution.mutation.event.title === PRIVATE_CALENDAR_ANNIVERSARY_TITLE,
      ),
    ).toHaveLength(0);
    await harness.assertDatabase(
      "Another adult or the group authorized an owner-private Calendar offer",
      `(select count(*)=1 from calendar_actions
        where status='offered' and approval_source_id is null
          and payload->'event'->>'title'=${sqlLiteral(PRIVATE_CALENDAR_ANNIVERSARY_TITLE)})`,
    );

    const messagesBeforeOwnerCalendarApproval = harness.linq.messages.length;
    await harness.accept(
      "private",
      "founder-approves-private-calendar-date",
      PRIVATE_CALENDAR_OWNER_APPROVAL,
    );
    await harness.drain();
    const ownerApprovedMessages = harness.linq.messages.slice(messagesBeforeOwnerCalendarApproval);
    const groupCalendarConfirmation = ownerApprovedMessages.find(
      (message) =>
        message.providerConversationId === FAMILY_GROUP &&
        message.text.includes(PRIVATE_CALENDAR_ANNIVERSARY_TITLE),
    );
    expect(groupCalendarConfirmation?.text).toContain("Aug 24, 2026");
    expect(groupCalendarConfirmation?.text).not.toContain(PRIVATE_CALENDAR_ANNIVERSARY_EVENT.location);
    expect(
      harness.state.calendarExecutions.filter(
        (execution) =>
          execution.mutation.operation === "create" &&
          execution.mutation.event.title === PRIVATE_CALENDAR_ANNIVERSARY_TITLE,
      ),
    ).toHaveLength(1);
    expect(
      [...harness.state.calendarEvents.values()].filter(
        (event) => event.title === PRIVATE_CALENDAR_ANNIVERSARY_TITLE,
      ),
    ).toEqual([expect.objectContaining({ location: null })]);
    expect(
      harness.state.timeline.filter(
        (entry) => entry === `provider:create:${PRIVATE_CALENDAR_ANNIVERSARY_TITLE}`,
      ),
    ).toHaveLength(1);
    await harness.assertDatabase(
      "An owner-approved personal Calendar date was not added and announced exactly once",
      `(select count(*)=1 from calendar_actions action
        join sources basis on basis.id=action.basis_source_id
        join messages approval on approval.source_id=action.approval_source_id
        join linq_channels approval_channel on approval_channel.id=approval.channel_id
        where action.status='committed'
          and action.payload->'event'->>'title'=${sqlLiteral(PRIVATE_CALENDAR_ANNIVERSARY_TITLE)}
          and basis.kind='calendar' and basis.visibility='private'
          and basis.owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and approval.direction='inbound'
          and approval.sender_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and approval_channel.audience='private'
          and approval_channel.adult_one_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and approval_channel.adult_two_id is null)
        and (select count(*)=1 from messages confirmation
          join linq_channels confirmation_channel on confirmation_channel.id=confirmation.channel_id
          where confirmation.direction='outbound' and confirmation.status='sent'
            and confirmation_channel.audience='group'
            and confirmation.text like ${sqlLiteral(`%${PRIVATE_CALENDAR_ANNIVERSARY_TITLE}%`)}
            and confirmation.text like '%Aug 24, 2026%')`,
    );

    const messagesBeforeFamilyCalendarEcho = harness.linq.messages.length;
    const createdAnniversary = [...harness.state.calendarEvents.values()].find(
      (event) => event.title === PRIVATE_CALENDAR_ANNIVERSARY_TITLE,
    );
    if (!createdAnniversary) throw new Error("The approved anniversary event was not created");
    harness.state.calendarEvents.set(createdAnniversary.providerEventId, {
      ...createdAnniversary,
      providerRevision: `${createdAnniversary.providerRevision}-benign-google-revision`,
      providerUpdatedAt: new Date(harness.state.now + 60_000).toISOString(),
    });
    harness.state.familyCalendarEchoPending = true;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(harness.state.familyCalendarEchoDelivered).toBe(true);
    expect(harness.linq.messages).toHaveLength(messagesBeforeFamilyCalendarEcho);
    await harness.assertDatabase(
      "A harmless Calendar revision retained an echo watch for Florence's own event",
      `not exists (
        select 1 from proactive_work_sources link join sources source on source.id=link.source_id
        where source.kind='calendar'
          and source.metadata->>'providerEventId'=${sqlLiteral(createdAnniversary.providerEventId)}
      )`,
    );
    const onceRevisedAnniversary = harness.state.calendarEvents.get(createdAnniversary.providerEventId);
    if (!onceRevisedAnniversary) throw new Error("The approved anniversary event disappeared");
    harness.state.calendarEvents.set(createdAnniversary.providerEventId, {
      ...onceRevisedAnniversary,
      providerRevision: `${onceRevisedAnniversary.providerRevision}-second-benign-revision`,
      providerUpdatedAt: new Date(harness.state.now + 60_000).toISOString(),
    });
    harness.state.familyCalendarEchoDelivered = false;
    harness.state.familyCalendarEchoPending = true;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(harness.state.familyCalendarEchoDelivered).toBe(true);
    expect(harness.linq.messages).toHaveLength(messagesBeforeFamilyCalendarEcho);

    const messagesBeforeMixedFamilyCalendarChange = harness.linq.messages.length;
    harness.state.familyCalendarMixedChangePending = true;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(harness.state.familyCalendarMixedChangeDelivered).toBe(true);
    expect(
      harness.linq.messages
        .slice(messagesBeforeMixedFamilyCalendarChange)
        .filter((message) => message.providerConversationId === FAMILY_GROUP)
        .map((message) => message.text),
    ).toEqual([FAMILY_CALENDAR_MIXED_CHANGE_SUMMARY]);
    await harness.assertDatabase(
      "A real Family Calendar change or its co-cited echo evidence was lost",
      `(select count(*)=2 from proactive_work_sources link
        join sources source on source.id=link.source_id
        where source.kind='calendar'
          and source.metadata->>'providerEventId' in (
            ${sqlLiteral(createdAnniversary.providerEventId)},
            ${sqlLiteral(FAMILY_CALENDAR_MIXED_CHANGE_EVENT.providerEventId)}
      ))`,
    );

    harness.state.familyCalendarRealOnlyOverlapPending = true;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(harness.state.familyCalendarRealOnlyOverlapDelivered).toBe(true);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          message.text === FAMILY_CALENDAR_MIXED_CHANGE_SUMMARY,
      ),
    ).toHaveLength(1);
    await harness.assertDatabase(
      "A real-only Calendar overlap re-announced a mixed echo finding or lost its exact evidence",
      `(select count(*)=1 from messages message
        join sources outbound on outbound.id=message.source_id
        where message.direction='outbound' and message.status='sent'
          and message.text=${sqlLiteral(FAMILY_CALENDAR_MIXED_CHANGE_SUMMARY)}
          and jsonb_typeof(outbound.metadata->'googleSourceIds')='array'
          and (select count(*)=2
            from jsonb_array_elements_text(outbound.metadata->'googleSourceIds') linked(id)
            join sources evidence on evidence.id=linked.id::uuid
            where evidence.kind='calendar'
              and evidence.metadata->>'providerEventId' in (
                ${sqlLiteral(createdAnniversary.providerEventId)},
                ${sqlLiteral(FAMILY_CALENDAR_MIXED_CHANGE_EVENT.providerEventId)}
              )))`,
    );

    const messagesBeforePrivateCalendarAdultEvent = harness.linq.messages.length;
    harness.state.privateCalendarAdultEventPending = true;
    harness.state.unrelatedAccountEmailPending = true;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(harness.state.privateCalendarAdultEventDelivered).toBe(true);
    expect(harness.state.unrelatedAccountEmailDelivered).toBe(true);
    expect(
      harness.state.googleAssessments.some(
        (assessment) =>
          assessment.evidence.calendar.events.some((event) => event.title === PRIVATE_CALENDAR_ADULT_TITLE) &&
          assessment.evidence.gmail.sources.some(
            (source) => source.subject === UNRELATED_ACCOUNT_EMAIL_SUBJECT,
          ),
      ),
    ).toBe(true);
    const privateCalendarAdultMessages = harness.linq.messages.slice(messagesBeforePrivateCalendarAdultEvent);
    expect(
      privateCalendarAdultMessages.some(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER &&
          message.text.includes(PRIVATE_CALENDAR_ADULT_TITLE),
      ),
    ).toBe(true);
    expect(
      privateCalendarAdultMessages
        .filter((message) => message.providerConversationId === FAMILY_GROUP)
        .map((message) => message.text)
        .join("\n"),
    ).not.toMatch(
      new RegExp(`${PRIVATE_CALENDAR_ADULT_TITLE}|${PRIVATE_CALENDAR_ADULT_EVENT.location}`, "i"),
    );
    expect(
      harness.state.calendarExecutions.some(
        (execution) =>
          execution.mutation.operation === "create" &&
          execution.mutation.event.title === PRIVATE_CALENDAR_ADULT_TITLE,
      ),
    ).toBe(false);
    expect(
      [...harness.state.calendarEvents.values()].some(
        (event) => event.title === PRIVATE_CALENDAR_ADULT_TITLE,
      ),
    ).toBe(false);
    await harness.assertDatabase(
      "An adult-private Calendar event crossed into the group or Family Calendar",
      `not exists (
        select 1 from calendar_actions
        where payload::text like ${sqlLiteral(`%${PRIVATE_CALENDAR_ADULT_TITLE}%`)}
      )`,
    );

    const messagesBeforePrivateCalendarConflict = harness.linq.messages.length;
    harness.state.calendarOnlyChangePending = true;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    const privateCalendarConflictMessages = harness.linq.messages.slice(
      messagesBeforePrivateCalendarConflict,
    );
    expect(
      privateCalendarConflictMessages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          message.text === "Hari has a calendar conflict then.",
      ),
    ).toHaveLength(1);
    expect(
      privateCalendarConflictMessages
        .filter((message) => message.providerConversationId === FAMILY_GROUP)
        .map((message) => message.text)
        .join("\n"),
    ).not.toMatch(new RegExp(`${PRIVATE_CALENDAR_ONLY_TITLE}|${PRIVATE_CALENDAR_CONFLICT_TITLE}`, "i"));

    let shared = await harness.florence.workspaceForAdult(harness.partnerAdultId);
    expect(shared.vault?.postalCode).toBe("94110");
    expect(shared.vault?.members.find((member) => member.id === harness.founderAdultId)).toMatchObject({
      postalCode: "94110",
    });
    shared = await harness.florence.putMember(harness.partnerAdultId, harness.founderAdultId, {
      postalCode: "94117",
    });
    expect(shared.vault?.postalCode).toBe("94117");
    await expect(
      harness.florence.putMember(harness.partnerAdultId, randomUUID(), {
        kind: "adult",
        firstName: "Another",
        lastName: "Adult",
      } as never),
    ).rejects.toThrow();
    await harness.florence.disconnectGoogle(harness.partnerAdultId, PARTNER_GOOGLE);
    await harness.drain();
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          message.text ===
            "The family calendar is paused because neither Google account is connected. Either of you can reconnect in Florence settings.",
      ),
    ).toHaveLength(0);
    shared = await harness.florence.putMember(harness.partnerAdultId, harness.founderAdultId, {
      lastName: "Barasu",
    });
    expect(shared.vault?.members.find((member) => member.id === harness.founderAdultId)).toMatchObject({
      firstName: "Hari",
      lastName: "Barasu",
      displayName: "Hari Barasu",
      relationship: "Parent",
    });
    expect(harness.state.calendarRenames).toEqual([
      expect.objectContaining({
        calendarId: FAMILY_CALENDAR,
        connectionId: FOUNDER_GOOGLE,
        ownerAdultId: harness.founderAdultId,
        summary: "Barasu–Anbarasu Family",
      }),
    ]);
    expect(harness.state.providerCalendarSummary).toBe("Barasu–Anbarasu Family");
    const householdIdAfterRename = (await harness.store.listHouseholdIdsForAdult(harness.founderAdultId))[0];
    const renamedHousehold = householdIdAfterRename
      ? await harness.store.readHousehold({ householdId: householdIdAfterRename })
      : null;
    expect(renamedHousehold).toMatchObject({
      name: "Barasu–Anbarasu Family",
      familyCalendarLabel: "Barasu–Anbarasu Family",
    });
    const calendarApp = await harness.webApp();
    const renamedCalendar = await calendarApp.inject({
      method: "GET",
      url: "/api/v1/calendar?month=2026-08",
      headers: { cookie: harness.sessionCookie },
    });
    await calendarApp.close();
    expect(renamedCalendar.statusCode).toBe(200);
    expect(renamedCalendar.json()).toMatchObject({
      status: "ready",
      calendarName: "Barasu–Anbarasu Family",
    });

    await harness.accept("private", "private-memory", "My private backup contact is Sam.");
    await harness.drain();
    const founderPrivate = await harness.florence.workspaceForAdult(harness.founderAdultId);
    const partnerPrivate = await harness.florence.workspaceForAdult(harness.partnerAdultId);
    const privateFact = founderPrivate.vault?.facts.find(
      (fact) => fact.statement === "My private backup contact is Sam.",
    );
    expect(privateFact).toMatchObject({ visibility: "private" });
    expect(partnerPrivate.vault?.facts.some((fact) => fact.id === privateFact?.id)).toBe(false);

    await harness.accept("private", "private-household-update", "Tell Alex that pickup is at 3:15.");
    await harness.drain();
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP && message.text === "Hari says pickup is at 3:15.",
      ),
    ).toHaveLength(1);
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === PRIVATE_FOUNDER && message.text === "I told Alex.",
      ),
    ).toHaveLength(0);
    expect(
      (await harness.florence.workspaceForAdult(harness.partnerAdultId)).vault?.facts.some((fact) =>
        fact.statement.includes("pickup is at 3:15"),
      ),
    ).toBe(false);

    await harness.accept("group", "pickup-start", "Pickup is at 2:45.");
    await harness.drain();
    shared = await harness.florence.workspaceForAdult(harness.partnerAdultId);
    const pickupFactId = shared.vault?.facts.find((fact) => fact.statement === "Pickup is at 2:45.")?.id;
    expect(pickupFactId).toBeDefined();
    if (!pickupFactId) throw new Error("Pickup fact was not retained");

    shared = await harness.florence.correctFact(harness.partnerAdultId, pickupFactId, "Pickup is at 2:50.");
    expect(shared.vault?.facts.find((fact) => fact.id === pickupFactId)).toMatchObject({
      statement: "Pickup is at 2:50.",
      source: { kind: "web", label: "Corrected in Vault" },
    });

    await harness.accept("group", "pickup-correct", "Actually pickup is at 3:00.", "partner");
    await harness.drain();
    shared = await harness.florence.workspaceForAdult(harness.founderAdultId);
    expect(shared.vault?.facts.find((fact) => fact.id === pickupFactId)).toMatchObject({
      statement: "Pickup is at 3:00.",
      visibility: "household",
    });

    await harness.accept("group", "pickup-forget", "We no longer need the pickup note.");
    await harness.drain();
    shared = await harness.florence.workspaceForAdult(harness.partnerAdultId);
    expect(shared.vault?.facts.some((fact) => fact.id === pickupFactId)).toBe(false);

    harness.state.uncertainCalendarCreateTitle = PICKUP_EVENT.title;
    await harness.accept("group", "calendar-create", "Add Maya pickup to the family calendar.");
    await harness.drain();
    const uncertainCreateAttempts = () =>
      harness.state.calendarExecutions.filter(
        (execution) =>
          execution.mutation.operation === "create" && execution.mutation.event.title === PICKUP_EVENT.title,
      );
    expect(uncertainCreateAttempts()).toHaveLength(1);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          message.text === `Added “${PICKUP_EVENT.title}” on the family calendar.`,
      ),
    ).toHaveLength(0);

    harness.state.now += 15_001;
    await harness.drain();
    const reconciledCreateAttempts = uncertainCreateAttempts();
    expect(reconciledCreateAttempts).toHaveLength(2);
    expect(new Set(reconciledCreateAttempts.map((execution) => execution.actionId)).size).toBe(1);
    expect(
      [...harness.state.calendarEvents.values()].filter((event) => event.title === PICKUP_EVENT.title),
    ).toHaveLength(1);
    expect(
      harness.state.timeline.filter((entry) => entry === `provider:create:${PICKUP_EVENT.title}`),
    ).toHaveLength(1);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          message.text === `Added “${PICKUP_EVENT.title}” on the family calendar.`,
      ),
    ).toHaveLength(1);

    await harness.accept("group", "calendar-update", "Move Maya pickup to 3:00.", "partner");
    await harness.drain();
    await harness.accept(
      "group",
      "calendar-delete",
      "Remove Maya pickup from the family calendar.",
      "partner",
    );
    await harness.drain();
    const pickupActions = new Map(
      harness.state.calendarExecutions
        .filter((execution) =>
          execution.mutation.operation === "create"
            ? execution.mutation.event.title === PICKUP_EVENT.title
            : execution.mutation.target.observedEvent.title === PICKUP_EVENT.title,
        )
        .map((execution) => [execution.actionId, execution.mutation.operation]),
    );
    expect([...pickupActions.values()]).toEqual(["create", "update", "delete"]);
    for (const confirmation of [
      `Added “${PICKUP_EVENT.title}” on the family calendar.`,
      `Updated “${PICKUP_EVENT.title}” on the family calendar.`,
      `Removed “${PICKUP_EVENT.title}” from the family calendar.`,
    ]) {
      expect(
        harness.linq.messages.filter(
          (message) => message.providerConversationId === FAMILY_GROUP && message.text === confirmation,
        ),
      ).toHaveLength(1);
    }

    const outsiderIdentity = digest("unexpected-participant");
    harness.linq.authorities.set(FAMILY_GROUP, {
      audience: "group",
      participantIdentityDigests: [FOUNDER_GROUP_IDENTITY, outsiderIdentity].sort(),
    });
    expect(
      await harness.receiveParts(
        "group-participants-changed",
        [{ type: "text", value: "Who joined this thread?" }],
        FAMILY_GROUP,
      ),
    ).toEqual({ disposition: "acknowledged", reason: "family_group_repairing" });
    await harness.drain();

    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER && message.text === GROUP_REPAIR_NOTICE,
      ),
    ).toHaveLength(1);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === PRIVATE_PARTNER && message.text === GROUP_REPAIR_NOTICE,
      ),
    ).toHaveLength(1);
    const groups = harness.linq.createdChats.filter((chat) => chat.result.authority.audience === "group");
    expect(groups).toHaveLength(2);
    expect(groups.at(-1)).toMatchObject({
      result: {
        providerConversationId: REPLACEMENT_GROUP,
        authority: {
          audience: "group",
          participantIdentityDigests: [
            FOUNDER_REPLACEMENT_GROUP_IDENTITY,
            PARTNER_REPLACEMENT_GROUP_IDENTITY,
          ].sort(),
        },
      },
    });
    const householdId = (await harness.store.listHouseholdIdsForAdult(harness.founderAdultId))[0];
    const household = householdId
      ? await harness.store.readHousehold({ householdId, viewerAdultId: harness.founderAdultId })
      : null;
    expect(household?.channels).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          providerConversationId: FAMILY_GROUP,
          revokedAt: expect.any(String),
          stoppedAt: expect.any(String),
        }),
        expect.objectContaining({
          providerConversationId: REPLACEMENT_GROUP,
          revokedAt: null,
          stoppedAt: null,
        }),
      ]),
    );
  }, 20_000);
});

class Harness {
  partnerId: string | null = null;
  sessionCookieValue: string | null = null;
  readonly #eventTimes = new Map<string, string>();

  constructor(
    readonly store: PostgresFlorenceStore,
    readonly florence: Florence,
    readonly linq: FakeLinq,
    readonly vault: EncryptedImageVault,
    readonly enrollmentCodes: EnrollmentCodes,
    readonly state: HarnessState,
    readonly databaseUrl: string,
    readonly assertionFile: string,
  ) {}

  get founderAdultId(): string {
    return founderSetup().adultId;
  }

  get partnerAdultId(): string {
    if (!this.partnerId) throw new Error("Partner has not completed setup");
    return this.partnerId;
  }

  get sessionCookie(): string {
    if (!this.sessionCookieValue) throw new Error("Founder web session is unavailable");
    return this.sessionCookieValue;
  }

  get participants(): string[] {
    return [FOUNDER_GROUP_IDENTITY, PARTNER_GROUP_IDENTITY].sort();
  }

  iso(): string {
    return new Date(this.state.now).toISOString();
  }

  eventTime(key: string): string {
    const existing = this.#eventTimes.get(key);
    if (existing) return existing;
    this.state.now += 1;
    const value = this.iso();
    this.#eventTimes.set(key, value);
    return value;
  }

  async setupFounder(): Promise<void> {
    this.linq.authorities.set(PRIVATE_FOUNDER, {
      audience: "private",
      participantIdentityDigests: [FOUNDER_IDENTITY],
    });
    const app = await this.webApp(false);
    const response = await app.inject({
      method: "POST",
      url: "/api/v1/session",
      payload: {
        setupToken: founderSetup().token,
        profile: {
          firstName: "Hari",
          lastName: "Anbarasu",
          timeZone: "America/Los_Angeles",
          guardianAttested: true,
          proactiveUseAccepted: true,
          privateConflictBusySharingEnabled: true,
        },
      },
    });
    await app.close();
    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ adultId: this.founderAdultId });
    const setCookie = response.headers["set-cookie"];
    this.sessionCookieValue = (Array.isArray(setCookie) ? setCookie[0] : setCookie)?.split(";")[0] ?? null;
    expect(this.sessionCookieValue).toContain("florence_session=");
  }

  async activateFounderGoogle(): Promise<void> {
    await this.activateGoogle(this.founderAdultId, FOUNDER_GOOGLE, "founder-google-state");
  }

  async completeFamilyProfile(): Promise<void> {
    const workspace = await this.florence.completeFamilyOnboarding(this.founderAdultId, familyProfileInput());
    const partner = workspace.vault?.members.find(
      (member) => member.kind === "adult" && member.relationship === "Partner",
    );
    if (!partner) throw new Error("Family setup did not create Alex");
    this.partnerId = partner.id;
    this.state.now += 1_500;
  }

  async setupPartner(): Promise<void> {
    const result = await this.redeemPartnerSetup(this.setupTokenFor(PRIVATE_PARTNER));
    expect(result).toMatchObject({ disposition: "accepted", adultId: this.partnerAdultId });
  }

  redeemPartnerSetup(setupToken: string) {
    return this.florence.redeemSetupLink({
      setupToken,
      profile: {
        firstName: "Alex",
        lastName: "Anbarasu",
        timeZone: "America/Los_Angeles",
        guardianAttested: true,
        proactiveUseAccepted: true,
        privateConflictBusySharingEnabled: true,
      },
    });
  }

  async activatePartnerGoogle(): Promise<void> {
    await this.activateGoogle(this.partnerAdultId, PARTNER_GOOGLE, "partner-google-state");
  }

  async readyHousehold(): Promise<void> {
    await this.setupFounder();
    await this.activateFounderGoogle();
    await this.completeFamilyProfile();
    await this.drain();
    await this.accept("private", "approve-partner", INVITE_APPROVAL);
    await this.drain();
    await this.receiveParts(
      "ready-household-partner-handshake",
      [{ type: "text", value: PARTNER_SETUP_HANDSHAKE_REPLY }],
      PRIVATE_PARTNER,
      "partner",
    );
    await this.setupPartner();
    await this.activatePartnerGoogle();
    await this.drain();
  }

  async activateGoogle(adultId: string, connectionId: string, state: string): Promise<void> {
    const householdId = (await this.store.listHouseholdIdsForAdult(adultId))[0];
    if (!householdId) throw new Error("Google activation needs a household");
    const sessionBindingDigest = digest(`${state}-session`);
    await this.store.createPending({
      connectionId,
      householdId,
      ownerAdultId: adultId,
      stateDigest: digest(state),
      sessionBindingDigest,
      stateExpiresAt: new Date(this.state.now + 60_000).toISOString(),
      now: this.iso(),
    });
    await this.florence.finishGoogle({
      adultId,
      state,
      code: "google-authorization-code",
      sessionBindingDigest,
    });
    this.state.now += 1_000;
  }

  inbound(
    audience: "private" | "group",
    key: string,
    text: string,
    sender: "founder" | "partner" = "founder",
  ) {
    const isGroup = audience === "group";
    const partner = sender === "partner";
    const senderIdentitySubjectDigest = isGroup
      ? partner
        ? PARTNER_GROUP_IDENTITY
        : FOUNDER_GROUP_IDENTITY
      : partner
        ? PARTNER_IDENTITY
        : FOUNDER_IDENTITY;
    return {
      providerConversationId: isGroup ? FAMILY_GROUP : partner ? PRIVATE_PARTNER : PRIVATE_FOUNDER,
      audience,
      participantIdentityDigests: isGroup
        ? this.participants
        : [partner ? PARTNER_IDENTITY : FOUNDER_IDENTITY],
      senderIdentitySubjectDigest,
      providerEventId: `event-${key}`,
      providerMessageId: `message-${key}`,
      text,
      occurredAt: this.eventTime(key),
    };
  }

  async accept(
    audience: "private" | "group",
    key: string,
    text: string,
    sender: "founder" | "partner" = "founder",
  ) {
    const result = await this.florence.acceptInbound(this.inbound(audience, key, text, sender));
    if (!result) throw new Error(`Inbound ${key} was rejected`);
    return result;
  }

  async receiveParts(
    key: string,
    parts: readonly TestPart[],
    providerConversationId: string,
    sender: "founder" | "partner" = "founder",
  ) {
    const authority = this.linq.authorities.get(providerConversationId);
    if (!authority) throw new Error(`Unknown provider conversation ${providerConversationId}`);
    const group = authority.audience === "group";
    const replacement = providerConversationId === REPLACEMENT_GROUP;
    const senderHandle = group
      ? sender === "partner"
        ? replacement
          ? PARTNER_REPLACEMENT_GROUP_HANDLE
          : PARTNER_GROUP_HANDLE
        : replacement
          ? FOUNDER_REPLACEMENT_GROUP_HANDLE
          : FOUNDER_GROUP_HANDLE
      : sender === "partner"
        ? PARTNER_HANDLE
        : FOUNDER_HANDLE;
    const senderPhone = sender === "partner" ? PARTNER_PHONE : FOUNDER_PHONE;
    for (const part of parts) {
      if (part.type !== "media") continue;
      this.linq.registerMedia(
        {
          providerAttachmentId: part.id,
          filename: part.filename,
          mimeType: part.mimeType,
          sizeBytes: part.bytes.byteLength,
        },
        part.bytes,
      );
    }
    const providerEventId = `event-${key}`;
    const occurredAt = this.eventTime(key);
    const timestamp = Math.floor(this.state.now / 1_000).toString();
    const rawBody = new TextEncoder().encode(
      JSON.stringify({
        api_version: "v3",
        webhook_version: "2026-02-03",
        event_type: "message.received",
        event_id: providerEventId,
        created_at: occurredAt,
        trace_id: `trace-${key}`,
        partner_id: LINQ_PARTNER,
        data: {
          id: `message-${key}`,
          direction: "inbound",
          chat: {
            id: providerConversationId,
            is_group: authority.audience === "group",
            owner_handle: { id: "florence-owner", handle: FLORENCE_PHONE, is_me: true },
          },
          sender_handle: { id: senderHandle, handle: senderPhone, is_me: false },
          service: "iMessage",
          parts: parts.map((part) =>
            part.type === "media"
              ? {
                  type: "media",
                  id: part.id,
                  filename: part.filename,
                  mime_type: part.mimeType,
                  size_bytes: part.bytes.byteLength,
                }
              : part,
          ),
          reply_to: null,
          sent_at: occurredAt,
          reconciled_at: null,
        },
      }),
    );
    const signature = createHmac("sha256", LINQ_SIGNING_KEY)
      .update(`${providerEventId}.${timestamp}.`, "utf8")
      .update(rawBody)
      .digest("base64");
    return createLinqIngress({
      signingSecret: LINQ_SIGNING_SECRET,
      expectedPartnerId: LINQ_PARTNER,
      linq: this.linq as unknown as LinqClient,
      imageVault: this.vault,
      florence: this.florence,
      now: () => new Date(this.state.now),
    }).receive({
      rawBody,
      headers: {
        "webhook-id": providerEventId,
        "webhook-timestamp": timestamp,
        "webhook-signature": `v1,${signature}`,
      },
      version: "2026-02-03",
    });
  }

  setupTokenFor(providerConversationId: string): string {
    const message = this.linq.messages.findLast(
      (candidate) =>
        candidate.providerConversationId === providerConversationId && candidate.text.includes("#s="),
    );
    const setupUrl = /https:\/\/\S+/.exec(message?.text ?? "")?.[0];
    const token = setupUrl ? new URLSearchParams(new URL(setupUrl).hash.slice(1)).get("s") : null;
    if (!token) throw new Error("Partner setup link was not sent");
    return token;
  }

  accessLinksFor(providerConversationId: string): URL[] {
    return this.linq.messages.flatMap((candidate) => {
      if (candidate.providerConversationId !== providerConversationId || !candidate.text.includes("#a=")) {
        return [];
      }
      const link = /https:\/\/\S+/.exec(candidate.text)?.[0];
      return link ? [new URL(link)] : [];
    });
  }

  accessLinkFor(providerConversationId: string): URL {
    const link = this.accessLinksFor(providerConversationId).at(-1);
    if (!link) throw new Error("A private Florence access link was not sent");
    return link;
  }

  async webApp(requireSession = true) {
    if (requireSession && !this.sessionCookieValue) throw new Error("Founder session has not been created");
    return buildApp(
      {
        florence: this.florence,
        callerResolver: createSessionCallerResolver({ FLORENCE_SESSION_SECRET: SESSION_SECRET }),
        ready: () => this.store.ready(),
      },
      { serveFrontend: false },
    );
  }

  async drain(): Promise<void> {
    let idle = 0;
    for (let index = 0; index < 500 && idle < 2; index += 1) {
      const worked = await this.florence.runOnce();
      idle = worked ? 0 : idle + 1;
      if (!worked) await new Promise((resolve) => setTimeout(resolve, 0));
    }
  }

  async assertDatabase(message: string, conditionSql: string): Promise<void> {
    await writeFile(
      this.assertionFile,
      `do $florence_assert$
      begin
        if not (${conditionSql}) then
          raise exception using message=${sqlLiteral(message)};
        end if;
      end
      $florence_assert$;`,
    );
    await migrateDatabase(this.databaseUrl, this.assertionFile);
  }
}

type FakeLinqLedger = {
  created: Map<string, LinqCreatedChat>;
  sent: Map<string, Awaited<ReturnType<LinqClient["sendMessage"]>>>;
  nextCreatedChatReceipt: number;
  nextMessageReceipt: number;
};

function createFakeLinqLedger(): FakeLinqLedger {
  return {
    created: new Map(),
    sent: new Map(),
    nextCreatedChatReceipt: 0,
    nextMessageReceipt: 0,
  };
}

function fakePhoneNumberForIdentity(identitySubjectDigest: string): string {
  if (
    identitySubjectDigest === FOUNDER_IDENTITY ||
    identitySubjectDigest === FOUNDER_GROUP_IDENTITY ||
    identitySubjectDigest === FOUNDER_REPLACEMENT_GROUP_IDENTITY
  ) {
    return FOUNDER_PHONE;
  }
  if (
    identitySubjectDigest === PARTNER_IDENTITY ||
    identitySubjectDigest === PARTNER_GROUP_IDENTITY ||
    identitySubjectDigest === PARTNER_REPLACEMENT_GROUP_IDENTITY
  ) {
    return PARTNER_PHONE;
  }
  return OUTSIDER_PHONE;
}

class FakeLinq {
  readonly authorities = new Map<string, LinqConversationAuthority>();
  readonly createdChats: { input: LinqCreateChat; result: LinqCreatedChat }[] = [];
  readonly createChatAttempts: LinqCreateChat[] = [];
  readonly messages: LinqSendMessage[] = [];
  readonly sendMessageAttempts: LinqSendMessage[] = [];
  readonly reactions: LinqSendReaction[] = [];
  readonly media = new Map<string, { reference: LinqMediaReference; bytes: Uint8Array }>();
  #partnerInitialPromptBarrier: {
    reached: Promise<void>;
    markReached: () => void;
    release: Promise<void>;
    resume: () => void;
  } | null = null;
  partnerInitialPromptAcceptedRemaining = 0;
  partnerSetupLinkState: "accepted" | "sent" = "sent";
  partnerSetupLinkAttempts = 0;
  partnerChatFailuresRemaining = 0;
  familyGroupPromptAcceptedRemaining = 0;
  familyCalendarReadyFailuresRemaining = 0;
  familyCalendarReadyAcceptedReplaysRemaining = 0;
  googleDeletionDeliveryFailuresRemaining = 0;
  oneShotReminderDeliveryFailuresRemaining = 0;
  staleReceiptForNextMessage = false;

  constructor(
    readonly state: HarnessState,
    readonly ledger: FakeLinqLedger = createFakeLinqLedger(),
  ) {}

  pauseNextPartnerInitialPrompt(): Promise<void> {
    if (this.#partnerInitialPromptBarrier) throw new Error("A partner prompt barrier is already active");
    let markReached: () => void = () => undefined;
    let resume: () => void = () => undefined;
    const reached = new Promise<void>((resolve) => {
      markReached = resolve;
    });
    const release = new Promise<void>((resolve) => {
      resume = resolve;
    });
    this.#partnerInitialPromptBarrier = { reached, markReached, release, resume };
    return reached;
  }

  releasePartnerInitialPrompt(): void {
    const barrier = this.#partnerInitialPromptBarrier;
    if (!barrier) throw new Error("No partner prompt barrier is active");
    this.#partnerInitialPromptBarrier = null;
    barrier.resume();
  }

  async createChat(input: LinqCreateChat): Promise<LinqCreatedChat> {
    this.createChatAttempts.push(input);
    const prior = this.ledger.created.get(input.idempotencyKey);
    if (prior) {
      this.authorities.set(prior.providerConversationId, prior.authority);
      if (prior.initialMessage.providerState === "accepted") {
        const confirmed = {
          ...prior,
          initialMessage: {
            ...prior.initialMessage,
            providerState: "sent" as const,
            occurredAt: new Date(this.state.now).toISOString(),
          },
        };
        this.ledger.created.set(input.idempotencyKey, confirmed);
        return confirmed;
      }
      return prior;
    }
    const privateChat = input.participantPhoneNumbers.length === 1;
    if (privateChat && this.partnerChatFailuresRemaining > 0) {
      this.partnerChatFailuresRemaining -= 1;
      throw new LinqError(
        "provider_rejected",
        "The destination permanently rejected the partner conversation",
        false,
      );
    }
    const groupCount = [...this.ledger.created.values()].filter(
      (chat) => chat.authority.audience === "group",
    ).length;
    const providerConversationId = privateChat
      ? PRIVATE_PARTNER
      : groupCount === 0
        ? FAMILY_GROUP
        : REPLACEMENT_GROUP;
    const participantIdentityDigests = privateChat
      ? [PARTNER_IDENTITY]
      : groupCount === 0
        ? [FOUNDER_GROUP_IDENTITY, PARTNER_GROUP_IDENTITY].sort()
        : [FOUNDER_REPLACEMENT_GROUP_IDENTITY, PARTNER_REPLACEMENT_GROUP_IDENTITY].sort();
    const participants = participantIdentityDigests.map((identitySubjectDigest) => ({
      identitySubjectDigest,
      phoneNumber: fakePhoneNumberForIdentity(identitySubjectDigest),
    }));
    const authority = {
      audience: privateChat ? ("private" as const) : ("group" as const),
      ownerPhoneNumber: FLORENCE_PHONE,
      participantIdentityDigests,
      participants,
    };
    this.ledger.nextCreatedChatReceipt += 1;
    const acceptedPartnerPrompt = privateChat && this.partnerInitialPromptAcceptedRemaining > 0;
    if (acceptedPartnerPrompt) this.partnerInitialPromptAcceptedRemaining -= 1;
    const acceptedFamilyGroupPrompt = !privateChat && this.familyGroupPromptAcceptedRemaining > 0;
    if (acceptedFamilyGroupPrompt) this.familyGroupPromptAcceptedRemaining -= 1;
    const result: LinqCreatedChat = {
      providerConversationId,
      authority,
      initialMessage: {
        idempotencyKey: input.idempotencyKey,
        providerMessageId: `created-chat-message-${this.ledger.nextCreatedChatReceipt}`,
        providerState: acceptedPartnerPrompt || acceptedFamilyGroupPrompt ? "accepted" : "sent",
        occurredAt: new Date(this.state.now).toISOString(),
      },
    };
    this.authorities.set(providerConversationId, authority);
    this.createdChats.push({ input, result });
    this.messages.push({
      idempotencyKey: input.idempotencyKey,
      providerConversationId,
      expectedAuthority: authority,
      text: input.initialText,
    });
    this.state.timeline.push(`message:${input.initialText}`);
    this.ledger.created.set(input.idempotencyKey, result);
    const promptBarrier = privateChat ? this.#partnerInitialPromptBarrier : null;
    if (promptBarrier) {
      promptBarrier.markReached();
      await promptBarrier.release;
    }
    return result;
  }

  async observeChat(providerConversationId: string) {
    const authority = this.authorities.get(providerConversationId);
    if (!authority) throw new Error(`Unknown fake Linq conversation ${providerConversationId}`);
    return {
      ...authority,
      ownerPhoneNumber: FLORENCE_PHONE,
      participants: authority.participantIdentityDigests.map((identitySubjectDigest) => ({
        identitySubjectDigest,
        phoneNumber: fakePhoneNumberForIdentity(identitySubjectDigest),
      })),
    };
  }

  registerMedia(reference: LinqMediaReference, bytes: Uint8Array): void {
    if (!this.media.has(reference.providerAttachmentId)) {
      this.media.set(reference.providerAttachmentId, { reference, bytes });
    }
  }

  async fetchMedia(reference: LinqMediaReference) {
    const media = this.media.get(reference.providerAttachmentId);
    if (!media) throw new Error(`Unknown fake Linq media ${reference.providerAttachmentId}`);
    return { ...media.reference, bytes: media.bytes };
  }

  async setTyping(): Promise<boolean> {
    return true;
  }

  async sendMessage(input: LinqSendMessage) {
    this.sendMessageAttempts.push(input);
    const partnerSetupLink = input.text.includes("#s=");
    const familyCalendarReady = input.idempotencyKey.startsWith("family-calendar-ready:");
    if (partnerSetupLink) this.partnerSetupLinkAttempts += 1;
    const prior = this.ledger.sent.get(input.idempotencyKey);
    if (prior) {
      if (familyCalendarReady && prior.status === "committed" && prior.providerState === "accepted") {
        if (this.familyCalendarReadyAcceptedReplaysRemaining > 0) {
          this.familyCalendarReadyAcceptedReplaysRemaining -= 1;
          return prior;
        }
        const sent = {
          ...prior,
          providerState: "sent" as const,
          occurredAt: new Date(this.state.now).toISOString(),
        };
        this.ledger.sent.set(input.idempotencyKey, sent);
        return sent;
      }
      if (
        partnerSetupLink &&
        prior.status === "committed" &&
        prior.providerState === "accepted" &&
        this.partnerSetupLinkState === "sent"
      ) {
        const sent = {
          ...prior,
          providerState: "sent" as const,
          occurredAt: new Date(this.state.now).toISOString(),
        };
        this.ledger.sent.set(input.idempotencyKey, sent);
        return sent;
      }
      return prior;
    }
    if (this.staleReceiptForNextMessage) {
      this.staleReceiptForNextMessage = false;
      return {
        status: "committed" as const,
        providerState: "accepted" as const,
        idempotencyKey: input.idempotencyKey,
        providerReceiptId: "stale-provider-receipt",
        detail: null,
        occurredAt: new Date(this.state.now - 10 * 60_000).toISOString(),
      };
    }
    expect(await this.observeChat(input.providerConversationId)).toMatchObject(input.expectedAuthority);
    const familyCalendarReadyOutcomeUnknown =
      familyCalendarReady && this.familyCalendarReadyFailuresRemaining > 0;
    if (familyCalendarReadyOutcomeUnknown) {
      this.familyCalendarReadyFailuresRemaining -= 1;
    }
    if (
      this.googleDeletionDeliveryFailuresRemaining > 0 &&
      (input.text === GOOGLE_DELETION_PRIVATE_ALERT || input.text.includes(GOOGLE_DELETION_FAMILY_DATE.title))
    ) {
      this.googleDeletionDeliveryFailuresRemaining -= 1;
      throw new LinqError("provider_retryable", "The staged Google alert is still pending", true);
    }
    if (this.oneShotReminderDeliveryFailuresRemaining > 0 && input.text === ONE_SHOT_REMINDER_TEXT) {
      this.oneShotReminderDeliveryFailuresRemaining -= 1;
      throw new LinqError("provider_retryable", "The one-shot reminder delivery is still pending", true);
    }
    this.messages.push(input);
    this.state.timeline.push(`message:${input.text}`);
    this.ledger.nextMessageReceipt += 1;
    const result: Awaited<ReturnType<LinqClient["sendMessage"]>> = {
      status: "committed" as const,
      providerState: familyCalendarReady
        ? familyCalendarReadyOutcomeUnknown
          ? "accepted"
          : "sent"
        : partnerSetupLink && this.partnerSetupLinkState === "sent"
          ? "sent"
          : "accepted",
      idempotencyKey: input.idempotencyKey,
      providerReceiptId: `sent-${this.ledger.nextMessageReceipt}`,
      detail: null,
      occurredAt: new Date(this.state.now).toISOString(),
    };
    this.ledger.sent.set(input.idempotencyKey, result);
    if (familyCalendarReadyOutcomeUnknown) {
      return {
        status: "unknown" as const,
        idempotencyKey: input.idempotencyKey,
        providerReceiptId: null,
        detail: "The Family Calendar announcement outcome is temporarily unknown",
        occurredAt: result.occurredAt,
      };
    }
    return result;
  }

  async sendReaction(input: LinqSendReaction) {
    this.reactions.push(input);
    this.state.timeline.push(`reaction:${input.reaction}:${input.targetProviderMessageId}`);
    return {
      status: "committed" as const,
      providerState: "reaction_added" as const,
      idempotencyKey: input.idempotencyKey,
      providerReceiptId: `reaction-${this.reactions.length}`,
      detail: null,
      occurredAt: new Date(this.state.now).toISOString(),
    };
  }
}

type ExternalDelivery = { key: string; receipt: string };
type LinqIncarnationSnapshot = {
  founderHandoff: ExternalDelivery[];
  initialPrivateReview: ExternalDelivery[];
  familyGroupCreate: ExternalDelivery[];
  familyCalendarReady: ExternalDelivery[];
  initialHouseholdBriefing: ExternalDelivery[];
};

function linqIncarnationSnapshot(linq: FakeLinq): LinqIncarnationSnapshot {
  const sent = (prefix: string): ExternalDelivery[] =>
    linq.messages
      .filter((message) => message.idempotencyKey.startsWith(prefix))
      .map((message) => {
        const receipt = linq.ledger.sent.get(message.idempotencyKey);
        if (receipt?.status !== "committed") {
          throw new Error(`A committed FakeLinq receipt is missing for ${message.idempotencyKey}`);
        }
        return { key: message.idempotencyKey, receipt: receipt.providerReceiptId };
      });
  return {
    founderHandoff: sent("founder-handoff:"),
    initialPrivateReview: sent("initial-private-review:"),
    familyGroupCreate: linq.createdChats
      .filter((chat) => chat.input.idempotencyKey.startsWith("family-group:"))
      .map((chat) => ({
        key: chat.input.idempotencyKey,
        receipt: chat.result.initialMessage.providerMessageId,
      })),
    familyCalendarReady: sent("family-calendar-ready:"),
    initialHouseholdBriefing: sent("initial-household-briefing:"),
  };
}

function expectFreshLinqIncarnation(
  earlier: LinqIncarnationSnapshot,
  current: LinqIncarnationSnapshot,
): void {
  const expectedCounts = {
    founderHandoff: 2,
    familyGroupCreate: 1,
    familyCalendarReady: 1,
    initialHouseholdBriefing: 1,
  } as const;
  for (const category of Object.keys(expectedCounts) as (keyof typeof expectedCounts)[]) {
    const earlierDeliveries = earlier[category];
    const currentDeliveries = current[category];
    expect(earlierDeliveries).toHaveLength(expectedCounts[category]);
    expect(currentDeliveries).toHaveLength(expectedCounts[category]);
    for (const delivery of [...earlierDeliveries, ...currentDeliveries]) {
      expect(delivery.key).toMatch(/:h:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    }
    const earlierKeys = new Set(earlierDeliveries.map((delivery) => delivery.key));
    const earlierReceipts = new Set(earlierDeliveries.map((delivery) => delivery.receipt));
    expect(currentDeliveries.every((delivery) => !earlierKeys.has(delivery.key))).toBe(true);
    expect(currentDeliveries.every((delivery) => !earlierReceipts.has(delivery.receipt))).toBe(true);
  }
  expect(earlier.initialPrivateReview).toHaveLength(2);
  expect(current.initialPrivateReview).toHaveLength(2);
  const earlierPrivateKeys = new Set(earlier.initialPrivateReview.map((delivery) => delivery.key));
  const earlierPrivateReceipts = new Set(earlier.initialPrivateReview.map((delivery) => delivery.receipt));
  for (const delivery of [...earlier.initialPrivateReview, ...current.initialPrivateReview]) {
    expect(delivery.key).toMatch(/:h:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  }
  expect(current.initialPrivateReview.every((delivery) => !earlierPrivateKeys.has(delivery.key))).toBe(true);
  expect(
    current.initialPrivateReview.every((delivery) => !earlierPrivateReceipts.has(delivery.receipt)),
  ).toBe(true);
}

async function createHarness(
  reason: Reason = async () => decision(),
  options: { now?: number; linqLedger?: FakeLinqLedger } = {},
): Promise<Harness> {
  if (!TEST_DATABASE_URL) throw new Error("TEST_DATABASE_URL is required");
  const directory = await mkdtemp(join(tmpdir(), "florence-parent-journeys-"));
  const schema = `florence_${randomUUID().replaceAll("-", "")}`;
  const setupFile = join(directory, "schema.sql");
  const migrations = (await Promise.all(migrationFiles.map((file) => readFile(file, "utf8")))).join("\n");
  await writeFile(setupFile, `CREATE SCHEMA "${schema}"; SET search_path TO "${schema}";\n${migrations}`);
  const databaseUrl = withSchema(TEST_DATABASE_URL, schema);
  await migrateDatabase(databaseUrl, setupFile);
  const store = new PostgresFlorenceStore(databaseUrl);
  const state: HarnessState = {
    now: options.now ?? NOW,
    privateReviews: [],
    googleAssessments: [],
    briefings: [],
    provisionings: [],
    calendarRenames: [],
    providerCalendarSummary: null,
    calendarExecutions: [],
    calendarReads: [],
    calendarEvents: new Map(),
    uncertainCalendarCreateTitle: null,
    timeline: [],
    finiteReviews: 0,
    interestResearches: 0,
    voiceTranscriptions: 0,
    initialGoogleFailuresRemaining: 0,
    initialClassifierFailuresRemaining: 0,
    initialGoogleFailureAdultId: null,
    completeScanPaginationExercise: false,
    wrongGoogleSubjectNext: false,
    baselinePageReads: [],
    initialHouseholdCalendarFailuresRemaining: 0,
    privateFactUpdatePending: false,
    privateFactUpdateDelivered: false,
    overlapGmailReadsRemaining: 0,
    overlapGmailAssessments: 0,
    overlapGmailSourceId: null,
    monitorEvidenceExercise: false,
    monitorCancellationActive: false,
    silentMonitorSourceId: null,
    voicedMonitorSourceId: null,
    cancelledMonitorSourceId: null,
    setupConversationFailuresRemaining: 0,
    founderProductRecenterReview: false,
    familyCalendarProvisioningFailuresRemaining: 0,
    invalidGrantAdultId: null,
    invalidGrantTriggered: false,
    googleDeletionEvidencePending: false,
    googleDeletionEvidenceDelivered: false,
    googleDeletionSourceId: null,
    googleChangeReads: [],
    interactiveGoogleReads: 0,
    providerRevocations: [],
    setupConversations: [],
    initialNoAttentionReview: false,
    initialCalendarOnlyReview: false,
    initialUnrelatedAccountReview: false,
    initialUnrelatedAccountFactOnlyReview: false,
    calendarOnlyChangePending: false,
    calendarOnlyChangeDelivered: false,
    privateCalendarAnniversaryPending: false,
    privateCalendarAnniversaryDelivered: false,
    familyCalendarEchoPending: false,
    familyCalendarEchoDelivered: false,
    familyCalendarMixedChangePending: false,
    familyCalendarMixedChangeDelivered: false,
    familyCalendarRealOnlyOverlapPending: false,
    familyCalendarRealOnlyOverlapDelivered: false,
    privateCalendarAdultEventPending: false,
    privateCalendarAdultEventDelivered: false,
    unrelatedAccountEmailPending: false,
    unrelatedAccountEmailDelivered: false,
  };
  const linq = new FakeLinq(state, options.linqLedger);
  const vault = new EncryptedImageVault({
    rootDirectory: join(directory, "vault"),
    encryptionKey: new Uint8Array(32).fill(7),
  });
  const enrollmentCodes = new EnrollmentCodes(ENROLLMENT_SECRET);
  const reasoner = createReasoner(reason, state);
  const google = createGoogle(store, state);
  const florence = new Florence({
    store,
    linq: linq as unknown as LinqClient,
    google,
    reasoner,
    enrollmentCodes,
    imageVault: vault,
    messagesUrl: "https://florence.test/messages",
    setupOrigin: "https://florence.test",
    linqSenderPhoneNumber: FLORENCE_PHONE,
    now: () => new Date(state.now),
  });
  const harness = new Harness(store, florence, linq, vault, enrollmentCodes, state, databaseUrl, setupFile);
  onTestFinished(async () => {
    florence.stop();
    await store.close();
    await writeFile(setupFile, `DROP SCHEMA IF EXISTS "${schema}" CASCADE;`);
    await migrateDatabase(TEST_DATABASE_URL, setupFile);
    await rm(directory, { recursive: true, force: true });
  });
  return harness;
}

function createReasoner(reason: Reason, state: HarnessState): FlorenceReasoner {
  return {
    decide: reason,
    transcribeVoiceNote: async (input: Parameters<FlorenceReasoner["transcribeVoiceNote"]>[0]) => {
      state.voiceTranscriptions += 1;
      expect(input).toMatchObject({ filename: "teacher-note.wav", mimeType: "audio/wav" });
      expect(input.bytes).toEqual(WAV_BYTES);
      return VOICE_TRANSCRIPT;
    },
    converseDuringSetup: async (input: Parameters<FlorenceReasoner["converseDuringSetup"]>[0]) => {
      state.setupConversations.push(input);
      if (input.stage === "partner_invited" && state.setupConversationFailuresRemaining > 0) {
        state.setupConversationFailuresRemaining -= 1;
        throw new Error("Fake setup interpreter is temporarily unavailable");
      }
      if (input.currentMessage.text === INCOMPLETE_SETUP_FRESH_LINK_REQUEST) {
        return {
          stopMessaging: false,
          declineInvitation: false,
          requestsFreshLink: true,
          bubbles: [{ text: INCOMPLETE_SETUP_FALSE_DENIAL, delayMs: 0 }],
        };
      }
      const declineInvitation =
        input.stage === "partner_invited" && input.currentMessage.text === PARTNER_SETUP_REFUSAL;
      return {
        stopMessaging: false,
        declineInvitation,
        requestsFreshLink: false,
        bubbles: declineInvitation
          ? []
          : [
              {
                text:
                  input.stage === "partner_invited"
                    ? input.nextStep === "signed_link_will_follow"
                      ? PARTNER_SETUP_HANDSHAKE_ACK
                      : PARTNER_SETUP_EXPLANATION
                    : "Finish the short setup page and I’ll keep going here.",
                delayMs: 0,
              },
            ],
      };
    },
    interpretCalendarApproval: async (
      input: Parameters<FlorenceReasoner["interpretCalendarApproval"]>[0],
    ) => ({ approve: input.currentMessage.text === PRIVATE_CALENDAR_OWNER_APPROVAL }),
    interpretPartnerInvitationApproval: async (
      input: Parameters<FlorenceReasoner["interpretPartnerInvitationApproval"]>[0],
    ) => ({
      sendInvitation:
        input.currentMessage.text === INVITE_APPROVAL || input.currentMessage.text === REINVITE_APPROVAL,
    }),
    classifyPrivateGoogleBatch: async (
      input: Parameters<FlorenceReasoner["classifyPrivateGoogleBatch"]>[0],
      reads: Parameters<FlorenceReasoner["classifyPrivateGoogleBatch"]>[1],
    ) => {
      if (!state.privateReviews.some((review) => review.adult.adultId === input.adult.adultId)) {
        state.privateReviews.push(input);
      }
      const gmail = input.sources.filter(
        (source): source is Extract<(typeof input.sources)[number], { kind: "gmail" }> =>
          source.kind === "gmail",
      );
      const calendar = input.sources.filter(
        (source): source is Extract<(typeof input.sources)[number], { kind: "calendar" }> =>
          source.kind === "calendar",
      );
      if (
        state.initialClassifierFailuresRemaining > 0 &&
        input.googleConnection.connectionId === FOUNDER_GOOGLE &&
        gmail.some((source) => source.subject === "Private school form")
      ) {
        state.initialClassifierFailuresRemaining -= 1;
        throw new Error("Fake classifier failed before the provider page commit");
      }
      for (const source of gmail) {
        const attachment = source.attachments[0];
        if (attachment) {
          const opened = await reads.readGmailAttachment({
            connectionId: input.googleConnection.connectionId,
            sourceId: source.sourceId,
            attachment,
          });
          expect(opened.bytes).toEqual(PDF_BYTES);
        }
      }
      if (state.initialNoAttentionReview) {
        return {
          findings: [],
          facts: [],
          dismissedSourceIds: input.sources.map((source) => source.sourceId),
        };
      }
      const founder = input.adult.firstName === "Hari";
      const unrelated = gmail.filter((source) => source.subject === UNRELATED_ACCOUNT_EMAIL_SUBJECT);
      const eligibleGmail = gmail.filter((source) => source.subject !== UNRELATED_ACCOUNT_EMAIL_SUBJECT);
      const paginatedCalendarSource = calendar.find((source) => source.title === PAGINATED_CALENDAR_TITLE);
      if (paginatedCalendarSource) {
        return {
          findings: [
            {
              privateSummary: PAGINATED_CALENDAR_FOLLOW_UP,
              actionAnchor: PAGINATED_CALENDAR_TITLE,
              familyRelevance: "child_care_school_or_activity" as const,
              sourceIds: [paginatedCalendarSource.sourceId],
              urgency: "watch" as const,
              dueAt: "2026-09-03T17:00:00.000Z",
              surfaceNow: false,
              candidate: null,
              monitor: {
                objective: "Resolve Maya’s school open-house plan.",
                currentConclusion: PAGINATED_CALENDAR_FOLLOW_UP,
                endCondition: "The family decides who will attend the school open house.",
                nextCheck: "2026-08-24T18:00:00.000Z",
                why: "This later-page Calendar item remains unresolved.",
              },
              familyCalendar: null,
            },
          ],
          facts: [],
          dismissedSourceIds: input.sources
            .map((source) => source.sourceId)
            .filter((sourceId) => sourceId !== paginatedCalendarSource.sourceId),
        };
      }
      if (founder && state.initialCalendarOnlyReview && gmail.length > 0) {
        return {
          findings: [],
          facts: [],
          dismissedSourceIds: gmail.map((source) => source.sourceId),
        };
      }
      if (founder && state.initialCalendarOnlyReview && calendar.length > 0) {
        return {
          findings: [
            {
              privateSummary: PRIVATE_CALENDAR_GENERIC_TODAY_REPLY,
              actionAnchor: PRIVATE_INITIAL_CALENDAR_ONLY_EVENT.title,
              familyRelevance: "child_care_school_or_activity" as const,
              sourceIds: calendar.map((source) => source.sourceId),
              urgency: "soon" as const,
              dueAt: null,
              surfaceNow: true,
              candidate: null,
              monitor: null,
              familyCalendar: null,
            },
          ],
          facts: [],
          dismissedSourceIds: [],
        };
      }
      const source = eligibleGmail[0];
      if (!source) {
        return {
          findings: [],
          facts: [],
          dismissedSourceIds: [...unrelated, ...calendar].map((candidate) => candidate.sourceId),
        };
      }
      const sameSubjectRescan = input.googleConnection.connectionId === RECONNECTED_FOUNDER_GOOGLE;
      const primaryFinding = {
        privateSummary: founder
          ? sameSubjectRescan
            ? "The school form is still waiting on Hari’s side."
            : "Hari’s private school email has the original form."
          : "Alex’s private school email has the permission-slip deadline.",
        actionAnchor: founder ? "field-trip form" : "permission-slip deadline",
        familyRelevance: "child_care_school_or_activity" as const,
        sourceIds: [source.sourceId],
        urgency: "soon" as const,
        dueAt: "2026-08-19T16:00:00.000Z",
        surfaceNow: true,
        candidate: {
          category: founder ? ("loose_end" as const) : ("deadline" as const),
          summary: founder
            ? sameSubjectRescan
              ? "The school form still needs a parent response."
              : FOUNDER_FORM_SUMMARY
            : PARTNER_PERMISSION_SUMMARY,
          urgency: "soon" as const,
          dueAt: "2026-08-19T16:00:00.000Z",
          needsAnswer: true,
        },
        monitor: null,
        familyCalendar: founder
          ? state.founderProductRecenterReview
            ? {
                disposition: "automatic" as const,
                sourceIds: [source.sourceId],
                event: PRE_ACTIVATION_FAMILY_DATE,
              }
            : null
          : {
              disposition: "automatic" as const,
              sourceIds: [source.sourceId],
              event: AUTOMATIC_FAMILY_DATE,
            },
      };
      const monitorFinding =
        founder && state.founderProductRecenterReview
          ? {
              privateSummary: sameSubjectRescan
                ? "The school form still needs confirmation on Hari’s side."
                : "Florence is watching for confirmation that Maya’s field-trip form is signed.",
              actionAnchor: "Muir Elementary",
              familyRelevance: "child_care_school_or_activity" as const,
              sourceIds: [source.sourceId],
              urgency: "soon" as const,
              dueAt: "2026-08-19T16:00:00.000Z",
              surfaceNow: false,
              candidate: null,
              monitor: {
                objective: sameSubjectRescan
                  ? "Confirm that Maya’s field-trip form gets signed."
                  : "Watch for confirmation that Maya’s field-trip form is signed.",
                currentConclusion: "The form still needs a parent signature.",
                endCondition: "A parent or the school confirms the form is signed.",
                nextCheck: "2026-08-23T18:00:00.000Z",
                why: "The school form has a live deadline.",
              },
              familyCalendar: null,
            }
          : null;
      const findings = founder
        ? [
            primaryFinding,
            ...(monitorFinding ? [monitorFinding] : []),
            {
              privateSummary: "The fall activity registration window opens Wednesday.",
              actionAnchor: "activity registration window",
              familyRelevance: "child_care_school_or_activity" as const,
              sourceIds: [source.sourceId],
              urgency: "soon" as const,
              dueAt: "2026-08-20T16:00:00.000Z",
              surfaceNow: true,
              candidate: {
                category: "conflict" as const,
                summary: SHARED_DUPLICATE_CONFLICT_SUMMARY,
                urgency: "soon" as const,
                dueAt: "2026-08-20T16:00:00.000Z",
                needsAnswer: true,
              },
              monitor: null,
              familyCalendar: null,
            },
            {
              privateSummary: "Friday’s pickup handoff has not been assigned.",
              actionAnchor: "pickup handoff",
              familyRelevance: "household_logistics" as const,
              sourceIds: [source.sourceId],
              urgency: "soon" as const,
              dueAt: "2026-08-21T21:45:00.000Z",
              surfaceNow: true,
              candidate: {
                category: "handoff" as const,
                summary: SCHOOL_HANDOFF_SUMMARY,
                urgency: "soon" as const,
                dueAt: "2026-08-21T21:45:00.000Z",
                needsAnswer: true,
              },
              monitor: null,
              familyCalendar: null,
            },
            {
              privateSummary: sameSubjectRescan
                ? "One more private school detail remains unresolved."
                : PRIVATE_INITIAL_ONLY_FINDING,
              actionAnchor: "private school detail",
              familyRelevance: "child_care_school_or_activity" as const,
              sourceIds: [source.sourceId],
              urgency: "watch" as const,
              dueAt: null,
              surfaceNow: true,
              candidate: null,
              monitor: null,
              familyCalendar: null,
            },
          ]
        : [
            primaryFinding,
            {
              privateSummary: "The fall activity registration window is also on Alex’s side.",
              actionAnchor: "activity registration window",
              familyRelevance: "child_care_school_or_activity" as const,
              sourceIds: [source.sourceId],
              urgency: "soon" as const,
              dueAt: "2026-08-20T16:00:00.000Z",
              surfaceNow: true,
              candidate: {
                category: "conflict" as const,
                summary: SHARED_DUPLICATE_CONFLICT_SUMMARY,
                urgency: "soon" as const,
                dueAt: "2026-08-20T16:00:00.000Z",
                needsAnswer: true,
              },
              monitor: null,
              familyCalendar: null,
            },
            {
              privateSummary: "The family meeting is on Alex’s calendar for Tuesday evening.",
              actionAnchor: "family meeting",
              familyRelevance: "household_logistics" as const,
              sourceIds: [source.sourceId],
              urgency: "watch" as const,
              dueAt: "2026-09-01T15:00:00.000Z",
              surfaceNow: true,
              candidate: {
                category: "family_date" as const,
                summary: FAMILY_MEETING_SUMMARY,
                urgency: "watch" as const,
                dueAt: "2026-09-01T15:00:00.000Z",
                needsAnswer: false,
              },
              monitor: null,
              familyCalendar: null,
            },
          ];
      return {
        findings,
        facts: [
          {
            slot: founder ? PRIVATE_SCHOOL_FACT_SLOT : PARTNER_PRIVATE_GOOGLE_FACT_SLOT,
            statement: founder ? INITIAL_PRIVATE_SCHOOL_FACT : PARTNER_PRIVATE_GOOGLE_FACT,
            familyRelevance: "child_care_school_or_activity" as const,
            sourceIds: [source.sourceId],
          },
          {
            slot: SHARED_SCHOOL_CONTACT_SLOT,
            statement: SHARED_SCHOOL_CONTACT_FACT,
            familyRelevance: "child_care_school_or_activity" as const,
            sourceIds: [source.sourceId],
          },
          ...(founder
            ? [
                {
                  slot: GOOGLE_CORRECTION_SLOT,
                  statement: GOOGLE_CORRECTION_FACT,
                  familyRelevance: "child_care_school_or_activity" as const,
                  sourceIds: [source.sourceId],
                },
              ]
            : []),
        ],
        dismissedSourceIds: [...unrelated, ...calendar, ...eligibleGmail.slice(1)].map(
          (candidate) => candidate.sourceId,
        ),
      };
    },
    synthesizeHouseholdBriefing: async (
      input: Parameters<FlorenceReasoner["synthesizeHouseholdBriefing"]>[0],
    ) => {
      state.briefings.push(input);
      return {
        selectedCandidateIds: input.candidates.map((candidate) => candidate.candidateId),
        bubbles: [
          {
            text: `Here’s what I found:\n${input.candidates
              .map((candidate) => `– ${candidate.summary}`)
              .join("\n")}\n\nDid I get that right? If I missed something, tell me here.`,
            delayMs: 0,
          },
        ],
      };
    },
    assessGoogleChanges: async (input: Parameters<FlorenceReasoner["assessGoogleChanges"]>[0]) => {
      state.googleAssessments.push(input);
      const source = input.evidence.gmail.sources.find(
        (candidate) => candidate.subject === "Maya school enrollment update",
      );
      const overlap = input.evidence.gmail.sources.find(
        (candidate) => candidate.subject === OVERLAP_GMAIL_SUBJECT,
      );
      const deletionSource = input.evidence.gmail.sources.find(
        (candidate) => candidate.subject === GOOGLE_DELETION_GMAIL_SUBJECT,
      );
      const calendarOnlySources = input.evidence.calendar.events.filter(
        (candidate) =>
          candidate.title === PRIVATE_CALENDAR_ONLY_TITLE ||
          candidate.title === PRIVATE_CALENDAR_CONFLICT_TITLE,
      );
      const calendarOnlySource = calendarOnlySources[0];
      const privateCalendarAnniversarySource = input.evidence.calendar.events.find(
        (candidate) => candidate.title === PRIVATE_CALENDAR_ANNIVERSARY_TITLE,
      );
      const familyCalendarMixedChangeSource = input.evidence.calendar.events.find(
        (candidate) => candidate.title === FAMILY_CALENDAR_MIXED_CHANGE_TITLE,
      );
      const privateCalendarAdultSource = input.evidence.calendar.events.find(
        (candidate) => candidate.title === PRIVATE_CALENDAR_ADULT_TITLE,
      );
      const unrelatedAccountSource = input.evidence.gmail.sources.find(
        (candidate) => candidate.subject === UNRELATED_ACCOUNT_EMAIL_SUBJECT,
      );
      if (deletionSource) state.googleDeletionSourceId = deletionSource.sourceId;
      if (overlap) {
        state.overlapGmailAssessments += 1;
        state.overlapGmailSourceId = overlap.sourceId;
      }
      const decision = {
        findings: familyCalendarMixedChangeSource
          ? [
              {
                privateDetail: null,
                actionAnchor: FAMILY_CALENDAR_MIXED_CHANGE_TITLE,
                familyRelevance: "child_care_school_or_activity" as const,
                householdConclusion: {
                  category: "family_date" as const,
                  summary: FAMILY_CALENDAR_MIXED_CHANGE_SUMMARY,
                  urgency: "soon" as const,
                  dueAt: FAMILY_CALENDAR_MIXED_CHANGE_EVENT.startsAt,
                  needsAnswer: false,
                },
                sourceIds: [
                  ...(privateCalendarAnniversarySource ? [privateCalendarAnniversarySource.sourceId] : []),
                  familyCalendarMixedChangeSource.sourceId,
                ],
                urgency: "soon" as const,
                dueAt: FAMILY_CALENDAR_MIXED_CHANGE_EVENT.startsAt,
                materialChange: true,
                monitor: null,
                familyCalendar: null,
              },
            ]
          : privateCalendarAdultSource
            ? [
                {
                  privateDetail: `${PRIVATE_CALENDAR_ADULT_TITLE} is on Tuesday.`,
                  actionAnchor: PRIVATE_CALENDAR_ADULT_TITLE,
                  familyRelevance: "household_logistics" as const,
                  householdConclusion: {
                    category: "loose_end" as const,
                    summary: `${PRIVATE_CALENDAR_ADULT_TITLE} is on Tuesday at ${PRIVATE_CALENDAR_ADULT_EVENT.location}.`,
                    urgency: "watch" as const,
                    dueAt: PRIVATE_CALENDAR_ADULT_EVENT.startsAt,
                    needsAnswer: false,
                  },
                  sourceIds: [
                    privateCalendarAdultSource.sourceId,
                    ...(unrelatedAccountSource ? [unrelatedAccountSource.sourceId] : []),
                  ],
                  urgency: "watch" as const,
                  dueAt: PRIVATE_CALENDAR_ADULT_EVENT.startsAt,
                  materialChange: true,
                  monitor: null,
                  familyCalendar: {
                    disposition: "automatic" as const,
                    sourceIds: [unrelatedAccountSource?.sourceId ?? privateCalendarAdultSource.sourceId],
                    event: {
                      intervalKind: "timed" as const,
                      title: PRIVATE_CALENDAR_ADULT_TITLE,
                      startsAt: PRIVATE_CALENDAR_ADULT_EVENT.startsAt,
                      endsAt: PRIVATE_CALENDAR_ADULT_EVENT.endsAt,
                      timeZone: "America/Los_Angeles",
                      location: PRIVATE_CALENDAR_ADULT_EVENT.location,
                    },
                  },
                },
              ]
            : privateCalendarAnniversarySource
              ? [
                  {
                    privateDetail: `${PRIVATE_CALENDAR_ANNIVERSARY_TITLE} is on Monday.`,
                    actionAnchor: PRIVATE_CALENDAR_ANNIVERSARY_TITLE,
                    familyRelevance: "household_logistics" as const,
                    householdConclusion: {
                      category: "family_date" as const,
                      summary: `${PRIVATE_CALENDAR_ANNIVERSARY_TITLE} is on Monday at ${PRIVATE_CALENDAR_ANNIVERSARY_EVENT.location}.`,
                      urgency: "watch" as const,
                      dueAt: PRIVATE_CALENDAR_ANNIVERSARY_EVENT.startsAt,
                      needsAnswer: false,
                    },
                    sourceIds: [privateCalendarAnniversarySource.sourceId],
                    urgency: "watch" as const,
                    dueAt: PRIVATE_CALENDAR_ANNIVERSARY_EVENT.startsAt,
                    materialChange: true,
                    monitor: null,
                    familyCalendar: {
                      disposition: "automatic" as const,
                      sourceIds: [privateCalendarAnniversarySource.sourceId],
                      event: {
                        intervalKind: "timed" as const,
                        title: PRIVATE_CALENDAR_ANNIVERSARY_TITLE,
                        startsAt: PRIVATE_CALENDAR_ANNIVERSARY_EVENT.startsAt,
                        endsAt: PRIVATE_CALENDAR_ANNIVERSARY_EVENT.endsAt,
                        timeZone: "America/Los_Angeles",
                        location: PRIVATE_CALENDAR_ANNIVERSARY_EVENT.location,
                      },
                    },
                  },
                ]
              : unrelatedAccountSource
                ? [
                    {
                      privateDetail: UNRELATED_ACCOUNT_EMAIL_ALERT,
                      actionAnchor: "password changed",
                      familyRelevance: "adult_only" as const,
                      householdConclusion: null,
                      sourceIds: [unrelatedAccountSource.sourceId],
                      urgency: "now" as const,
                      dueAt: null,
                      materialChange: true,
                      monitor: {
                        operation: "create" as const,
                        monitorId: null,
                        objective: UNRELATED_ACCOUNT_MONITOR_OBJECTIVE,
                        currentConclusion: "The account change needs the adult’s verification.",
                        endCondition: "The adult confirms the change or secures the account.",
                        nextCheck: new Date(Date.parse(input.currentTime) + 24 * 60 * 60_000).toISOString(),
                        why: "The account alert asks for verification.",
                      },
                      familyCalendar: null,
                    },
                  ]
                : calendarOnlySource
                  ? [
                      {
                        privateDetail: PRIVATE_CALENDAR_GENERIC_TODAY_REPLY,
                        actionAnchor: PRIVATE_CALENDAR_ONLY_TITLE,
                        familyRelevance: "child_care_school_or_activity" as const,
                        householdConclusion: {
                          category: "conflict" as const,
                          summary: `${PRIVATE_CALENDAR_ONLY_TITLE} overlaps ${PRIVATE_CALENDAR_CONFLICT_TITLE}.`,
                          urgency: "now" as const,
                          dueAt: PRIVATE_CALENDAR_ONLY_EVENT.startsAt,
                          needsAnswer: true,
                        },
                        sourceIds: calendarOnlySources.map((source) => source.sourceId),
                        urgency: "now" as const,
                        dueAt: PRIVATE_CALENDAR_ONLY_EVENT.startsAt,
                        materialChange: true,
                        monitor: null,
                        familyCalendar: null,
                      },
                    ]
                  : deletionSource
                    ? [
                        {
                          privateDetail: GOOGLE_DELETION_PRIVATE_ALERT,
                          actionAnchor: "emergency card",
                          familyRelevance: "child_care_school_or_activity" as const,
                          householdConclusion: null,
                          sourceIds: [deletionSource.sourceId],
                          urgency: "soon" as const,
                          dueAt: null,
                          materialChange: true,
                          monitor: {
                            operation: "create" as const,
                            monitorId: null,
                            objective: "Watch for confirmation that Maya’s emergency card is signed.",
                            currentConclusion: "The emergency card still needs a signature.",
                            endCondition: "A parent or the school confirms the emergency card is signed.",
                            nextCheck: new Date(Date.parse(input.currentTime) + 60 * 60_000).toISOString(),
                            why: "The school reminder has a live deadline.",
                          },
                          familyCalendar: null,
                        },
                        {
                          privateDetail: null,
                          actionAnchor: GOOGLE_DELETION_FAMILY_DATE.title,
                          familyRelevance: "child_care_school_or_activity" as const,
                          householdConclusion: null,
                          sourceIds: [deletionSource.sourceId],
                          urgency: "soon" as const,
                          dueAt: null,
                          materialChange: true,
                          monitor: null,
                          familyCalendar: {
                            disposition: "suggest" as const,
                            sourceIds: [deletionSource.sourceId],
                            event: GOOGLE_DELETION_FAMILY_DATE,
                          },
                        },
                      ]
                    : [],
        facts: unrelatedAccountSource
          ? [
              {
                slot: UNRELATED_ACCOUNT_FACT_SLOT,
                statement: UNRELATED_ACCOUNT_FACT,
                familyRelevance: "adult_only" as const,
                sourceIds: [unrelatedAccountSource.sourceId],
              },
            ]
          : calendarOnlySource
            ? [
                {
                  slot: PRIVATE_CALENDAR_FACT_SLOT,
                  statement: PRIVATE_CALENDAR_FACT,
                  familyRelevance: "child_care_school_or_activity" as const,
                  sourceIds: [calendarOnlySource.sourceId],
                },
              ]
            : deletionSource
              ? [
                  {
                    slot: GOOGLE_DELETION_FACT_SLOT,
                    statement: GOOGLE_DELETION_FACT,
                    familyRelevance: "child_care_school_or_activity" as const,
                    sourceIds: [deletionSource.sourceId],
                  },
                ]
              : source
                ? [
                    {
                      slot: PRIVATE_SCHOOL_FACT_SLOT,
                      statement: UPDATED_PRIVATE_SCHOOL_FACT,
                      familyRelevance: "child_care_school_or_activity" as const,
                      sourceIds: [source.sourceId],
                    },
                    {
                      slot: GOOGLE_CORRECTION_SLOT,
                      statement: GOOGLE_CORRECTION_FACT,
                      familyRelevance: "child_care_school_or_activity" as const,
                      sourceIds: [source.sourceId],
                    },
                  ]
                : overlap && state.overlapGmailAssessments === 2
                  ? [
                      {
                        slot: OVERLAP_GMAIL_FACT_SLOT,
                        statement: "Maya’s school bus route reminder is current.",
                        familyRelevance: "child_care_school_or_activity" as const,
                        sourceIds: [overlap.sourceId],
                      },
                    ]
                  : [],
      };
      const usedSourceIds = new Set([
        ...decision.findings.flatMap((finding) => finding.sourceIds),
        ...decision.facts.flatMap((fact) => fact.sourceIds),
      ]);
      return {
        ...decision,
        dismissedSourceIds: [...input.evidence.gmail.sources, ...input.evidence.calendar.events]
          .map((candidate) => candidate.sourceId)
          .filter((sourceId) => !usedSourceIds.has(sourceId)),
      };
    },
    reviewFiniteMonitor: async (input: Parameters<FlorenceReasoner["reviewFiniteMonitor"]>[0]) => {
      state.finiteReviews += 1;
      const source = input.evidence.calendar.events[0] ?? input.evidence.gmail.sources[0];
      if (!source) throw new Error("Monitor review did not receive current evidence");
      if (state.finiteReviews === 1) {
        state.silentMonitorSourceId = source.sourceId;
        return {
          outcome: "silent" as const,
          urgency: "watch" as const,
          privateDetail: null,
          householdConclusion: null,
          sourceIds: [],
          currentConclusion: input.monitor.currentConclusion,
          nextCheck: new Date(Date.parse(input.currentTime) + 60 * 60_000).toISOString(),
          why: input.monitor.why,
        };
      }
      if (state.finiteReviews === 2) {
        state.voicedMonitorSourceId = source.sourceId;
        const summary = "The school has the form and is checking the signature.";
        return {
          outcome: "update" as const,
          urgency: "soon" as const,
          privateDetail: input.scope === "private" ? summary : null,
          householdConclusion: {
            category: "loose_end" as const,
            summary,
            urgency: "soon" as const,
            dueAt: input.currentTime,
            needsAnswer: false,
          },
          sourceIds: [source.sourceId],
          currentConclusion: summary,
          nextCheck: new Date(Date.parse(input.currentTime) + 60 * 60_000).toISOString(),
          why: input.monitor.why,
        };
      }
      state.cancelledMonitorSourceId = source.sourceId;
      return {
        outcome: "complete" as const,
        urgency: "now" as const,
        privateDetail: input.scope === "private" ? "The school confirmed the form is signed." : null,
        householdConclusion: {
          category: "loose_end" as const,
          summary: "Maya’s field-trip form is signed—nothing else to do.",
          urgency: "now" as const,
          dueAt: input.currentTime,
          needsAnswer: false,
        },
        sourceIds: [source.sourceId],
        currentConclusion: "The form is signed.",
        nextCheck: null,
        why: input.monitor.why,
      };
    },
    researchInterest: async () => {
      state.interestResearches += 1;
      return {
        judgment: "recommend" as const,
        summary: INTEREST_RECOMMENDATION,
        urls: [INTEREST_URL],
      };
    },
  } as unknown as FlorenceReasoner;
}

function createGoogle(store: PostgresFlorenceStore, state: HarnessState): GoogleConnection {
  const activeCredential = async (input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
  }) => {
    const credential = await store.readActiveGoogleCredential(input);
    if (!credential) throw new Error("Fake Google connection is not active");
    if (state.invalidGrantAdultId === input.ownerAdultId && !state.invalidGrantTriggered) {
      state.invalidGrantTriggered = true;
      await store.disconnect({
        connectionId: input.connectionId,
        householdId: input.householdId,
        ownerAdultId: input.ownerAdultId,
        now: new Date(state.now).toISOString(),
      });
      throw new GoogleConnectionError(
        "The active Google credential is no longer valid",
        "credential_invalid_grant",
      );
    }
    return credential;
  };
  const baseline = (input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    calendarId?: string;
    currentTime: string;
  }) => {
    const familyEvents = [...state.calendarEvents.values()];
    const events =
      input.calendarId && input.calendarId !== "primary"
        ? state.monitorCancellationActive
          ? familyEvents.slice(0, 1).map((event) => ({
              providerEventId: event.providerEventId,
              providerRevision: `${event.providerRevision}-cancelled`,
              providerUpdatedAt: new Date(state.now).toISOString(),
              status: "cancelled" as const,
              busy: false,
              title: null,
              startsAt: null,
              endsAt: null,
              allDay: null,
              timeZone: null,
              startDate: null,
              endDate: null,
            }))
          : state.monitorEvidenceExercise && state.finiteReviews === 0
            ? familyEvents.map((event) => ({
                ...event,
                providerRevision: `${event.providerRevision}-silent-observation`,
                providerUpdatedAt: new Date(state.now).toISOString(),
              }))
            : familyEvents
        : state.initialCalendarOnlyReview && input.ownerAdultId === founderSetup().adultId
          ? [PRIVATE_INITIAL_CALENDAR_ONLY_EVENT, PRIVATE_INITIAL_CALENDAR_CONFLICT_EVENT]
          : [
              {
                providerEventId: `private-event-${input.ownerAdultId}`,
                providerRevision: `private-revision-${input.ownerAdultId}`,
                providerUpdatedAt: new Date(state.now).toISOString(),
                status: "confirmed" as const,
                busy: true,
                title: "Private calendar detail",
                intervalKind: "timed" as const,
                startsAt: "2026-08-18T17:00:00.000Z",
                endsAt: "2026-08-18T18:00:00.000Z",
                allDay: false,
                timeZone: "America/Los_Angeles",
                location: null,
              },
            ];
    return {
      status: "complete" as const,
      events,
      cursor: {
        kind: "calendar_updated_min_v1" as const,
        calendarId: input.calendarId ?? "primary",
        updatedMin: input.currentTime,
        windowTimeMin: input.currentTime,
        windowTimeMax: new Date(Date.parse(input.currentTime) + 21 * 24 * 60 * 60_000).toISOString(),
        overlapMs: 300_000 as const,
      },
    };
  };
  return {
    status: (input: { householdId: string; ownerAdultId: string }) => store.listActive(input),
    disconnect: async (input: {
      connectionId: string;
      householdId: string;
      ownerAdultId: string;
      now: string;
    }) => {
      const disconnected = await store.disconnect(input);
      if (!disconnected) throw new Error("Fake Google connection was not found");
      return {
        connection: disconnected.view,
        providerRevocation: state.providerRevocations.shift() ?? ("not-needed" as const),
      };
    },
    finish: async (input: { state: string; sessionBindingDigest: string; now: string }) => {
      const stateDigest = digest(input.state);
      const pending = await store.consumePendingState({
        stateDigest,
        sessionBindingDigest: input.sessionBindingDigest,
        now: input.now,
      });
      if (!pending) throw new Error("Fake Google state was not pending");
      const founder = pending.ownerAdultId === founderSetup().adultId;
      try {
        return await store.activate({
          connectionId: pending.connectionId,
          stateDigest,
          googleSubjectDigest: digest(
            state.wrongGoogleSubjectNext
              ? "a-different-google-account"
              : founder
                ? "google-founder"
                : "google-partner",
          ),
          emailLabel: founder ? "hari@example.com" : "alex@example.com",
          grantedScopes: GOOGLE_SCOPES,
          refreshTokenEnvelope: founder ? "encrypted-founder-token" : "encrypted-partner-token",
          now: input.now,
        });
      } catch (error) {
        if (
          error &&
          typeof error === "object" &&
          "code" in error &&
          error.code === "google_identity_conflict"
        ) {
          throw new GoogleConnectionError("Reconnect the same Google account", "identity_conflict");
        }
        throw error;
      } finally {
        state.wrongGoogleSubjectNext = false;
      }
    },
    provisionFamilyCalendar: async (
      input: FamilyCalendarProvisioningInput,
    ): Promise<GoogleFamilyCalendarProvisioningResult> => {
      state.provisionings.push(input);
      if (input.calendarId) {
        expect(input.calendarId).toBe(FAMILY_CALENDAR);
      } else {
        const creation = await store.beginFamilyCalendarCreation({
          householdId: input.householdId,
          now: new Date(state.now).toISOString(),
        });
        expect(creation).toEqual({ createAllowed: true, calendarId: null });
      }
      state.providerCalendarSummary = input.summary;
      if (state.familyCalendarProvisioningFailuresRemaining > 0) {
        state.familyCalendarProvisioningFailuresRemaining -= 1;
        throw new GoogleFamilyCalendarTransientError(
          "Fake Calendar sharing is temporarily unavailable",
          FAMILY_CALENDAR,
        );
      }
      return {
        calendarId: FAMILY_CALENDAR,
        summary: input.summary,
        founderConnectionId: input.founderConnectionId,
        partnerConnectionId: input.partnerConnectionId,
        occurredAt: new Date(state.now).toISOString(),
      };
    },
    renameFamilyCalendar: async (
      input: Parameters<GoogleConnection["renameFamilyCalendar"]>[0],
    ): Promise<GoogleFamilyCalendarRenameResult> => {
      await activeCredential(input);
      expect(input.calendarId).toBe(FAMILY_CALENDAR);
      state.calendarRenames.push(input);
      state.providerCalendarSummary = input.summary;
      return { summary: input.summary, occurredAt: new Date(state.now).toISOString() };
    },
    captureGmailCursor: async (input: {
      householdId: string;
      ownerAdultId: string;
      connectionId: string;
    }) => {
      await activeCredential(input);
      if (
        state.initialGoogleFailuresRemaining > 0 &&
        (state.initialGoogleFailureAdultId === null ||
          state.initialGoogleFailureAdultId === input.ownerAdultId)
      ) {
        state.initialGoogleFailureAdultId = input.ownerAdultId;
        state.initialGoogleFailuresRemaining -= 1;
        throw new Error("Fake Gmail baseline is temporarily unavailable");
      }
      return {
        kind: "gmail_history_v1" as const,
        historyId: input.ownerAdultId === founderSetup().adultId ? "101" : "201",
        capturedAt: new Date(state.now).toISOString(),
      };
    },
    readGmailBaselinePage: async (input: {
      householdId: string;
      ownerAdultId: string;
      connectionId: string;
      after: string;
      before: string;
      pageToken?: string;
    }) => {
      await activeCredential(input);
      state.baselinePageReads.push({
        kind: "gmail",
        ownerAdultId: input.ownerAdultId,
        connectionId: input.connectionId,
        pageToken: input.pageToken ?? null,
        calendarId: null,
      });
      const founder = input.ownerAdultId === founderSetup().adultId;
      const initialUnrelatedAccount =
        founder && (state.initialUnrelatedAccountReview || state.initialUnrelatedAccountFactOnlyReview);
      const messageId = initialUnrelatedAccount
        ? "gmail-initial-unrelated-retail-account-alert"
        : founder
          ? SCHOOL_ATTACHMENT.messageId
          : `gmail-${input.ownerAdultId}-true`;
      const relevant = {
        messageId,
        threadId: `thread-${messageId}`,
        historyId: founder ? "101" : "201",
        from: initialUnrelatedAccount
          ? "account@example.test"
          : founder
            ? "hari-private@example.com"
            : "alex-private@example.com",
        subject: initialUnrelatedAccount
          ? UNRELATED_ACCOUNT_EMAIL_SUBJECT
          : founder
            ? "Private school form"
            : "Private family schedule",
        sentAt: new Date(Date.parse(input.before) - 1_000).toISOString(),
        text: initialUnrelatedAccount
          ? "Your retail account password was changed."
          : founder
            ? "Hari private email: Maya attends Muir Elementary and her form needs a signature."
            : "Alex private email: a personal appointment moved.",
        textStatus: "complete" as const,
        attachmentsStatus: "complete" as const,
        attachments: !initialUnrelatedAccount && founder ? [SCHOOL_ATTACHMENT] : [],
      };
      if (state.completeScanPaginationExercise && founder && input.connectionId === FOUNDER_GOOGLE) {
        if (input.pageToken === undefined) {
          return {
            status: "truncated" as const,
            nextPageToken: "gmail-baseline-page-2",
            messages: Array.from({ length: 50 }, (_, index) => ({
              ...relevant,
              messageId: `gmail-archived-irrelevant-${index}`,
              threadId: `thread-gmail-archived-irrelevant-${index}`,
              historyId: String(1_000 + index),
              subject: UNRELATED_ACCOUNT_EMAIL_SUBJECT,
              text:
                index === 0
                  ? "Archived adult-only account notice. Code 123456. https://example.test/reset?token=fake-only-token-value-1234567890"
                  : "Archived adult-only account notice.",
              attachments: [],
            })),
          };
        }
        expect(input.pageToken).toBe("gmail-baseline-page-2");
      } else {
        expect(input.pageToken).toBeUndefined();
      }
      return { status: "complete" as const, nextPageToken: null, messages: [relevant] };
    },
    readCalendarBaselineTargetsPage: async (input: {
      householdId: string;
      ownerAdultId: string;
      connectionId: string;
      excludedFamilyCalendarId: string | null;
      pageToken?: string;
    }) => {
      await activeCredential(input);
      state.baselinePageReads.push({
        kind: "calendar_targets",
        ownerAdultId: input.ownerAdultId,
        connectionId: input.connectionId,
        pageToken: input.pageToken ?? null,
        calendarId: null,
      });
      const founder = input.ownerAdultId === founderSetup().adultId;
      if (state.completeScanPaginationExercise && founder && input.connectionId === FOUNDER_GOOGLE) {
        if (input.pageToken === undefined) {
          return {
            status: "truncated" as const,
            nextPageToken: "calendar-targets-page-2",
            targets: Array.from({ length: 50 }, (_, index) => ({
              calendarId: `secondary-calendar-${index}`,
              timeZone: "America/Los_Angeles",
              accessRole: "reader" as const,
              primary: false,
            })),
          };
        }
        expect(input.pageToken).toBe("calendar-targets-page-2");
      } else {
        expect(input.pageToken).toBeUndefined();
      }
      return {
        status: "complete" as const,
        nextPageToken: null,
        targets:
          input.excludedFamilyCalendarId === "primary"
            ? []
            : [
                {
                  calendarId: "primary",
                  timeZone: "America/Los_Angeles",
                  accessRole: "owner" as const,
                  primary: true,
                },
              ],
      };
    },
    readCalendarBaselineEventsPage: async (input: {
      householdId: string;
      ownerAdultId: string;
      connectionId: string;
      target: {
        calendarId: string;
        timeZone: string;
        accessRole: "reader" | "writerWithoutPrivateAccess" | "writer" | "owner";
        primary: boolean;
      };
      timeMin: string;
      timeMax: string;
      pageToken?: string;
    }) => {
      await activeCredential(input);
      state.baselinePageReads.push({
        kind: "calendar_events",
        ownerAdultId: input.ownerAdultId,
        connectionId: input.connectionId,
        pageToken: input.pageToken ?? null,
        calendarId: input.target.calendarId,
      });
      const founder = input.ownerAdultId === founderSetup().adultId;
      if (
        state.completeScanPaginationExercise &&
        founder &&
        input.connectionId === FOUNDER_GOOGLE &&
        input.target.calendarId === "primary"
      ) {
        if (input.pageToken === undefined) {
          return {
            status: "truncated" as const,
            nextPageToken: "calendar-events-page-2",
            events: Array.from({ length: 50 }, (_, index) => ({
              providerEventId: `archived-calendar-event-${index}`,
              providerRevision: `archived-calendar-revision-${index}`,
              providerUpdatedAt: new Date(Date.parse(input.timeMin) + index * 1_000).toISOString(),
              status: "confirmed" as const,
              busy: false,
              title: `Archived adult calendar item ${index}`,
              intervalKind: "timed" as const,
              startsAt: "2026-08-20T17:00:00.000Z",
              endsAt: "2026-08-20T18:00:00.000Z",
              allDay: false,
              timeZone: "America/Los_Angeles",
              location: null,
            })),
          };
        }
        expect(input.pageToken).toBe("calendar-events-page-2");
        return {
          status: "complete" as const,
          nextPageToken: null,
          events: [
            ...baseline({ ...input, calendarId: input.target.calendarId, currentTime: input.timeMin }).events,
            {
              providerEventId: "paginated-calendar-final-event",
              providerRevision: "paginated-calendar-final-revision",
              providerUpdatedAt: new Date(Date.parse(input.timeMax) - 1_000).toISOString(),
              status: "confirmed" as const,
              busy: true,
              title: PAGINATED_CALENDAR_TITLE,
              intervalKind: "timed" as const,
              startsAt: "2026-09-03T17:00:00.000Z",
              endsAt: "2026-09-03T18:00:00.000Z",
              allDay: false,
              timeZone: "America/Los_Angeles",
              location: null,
            },
          ],
        };
      }
      expect(input.pageToken).toBeUndefined();
      return {
        status: "complete" as const,
        nextPageToken: null,
        events: baseline({ ...input, calendarId: input.target.calendarId, currentTime: input.timeMin })
          .events,
      };
    },
    searchGmail: async (input: {
      householdId: string;
      ownerAdultId: string;
      connectionId: string;
      query: string;
      after?: string;
      before?: string;
      limit?: number;
    }) => {
      await activeCredential(input);
      state.interactiveGoogleReads += 1;
      const founder = input.ownerAdultId === founderSetup().adultId;
      const recent =
        input.after && input.before
          ? Date.parse(input.before) - Date.parse(input.after) <= 15 * 24 * 60 * 60_000
          : true;
      const ordinaryUnused = input.query === ORDINARY_UNUSED_GMAIL_QUERY;
      const citedReply = input.query === GOOGLE_CITED_REPLY_QUERY;
      const initialUnrelatedAccount =
        founder &&
        recent &&
        Boolean(input.after) &&
        (state.initialUnrelatedAccountReview || state.initialUnrelatedAccountFactOnlyReview);
      const messageId = citedReply
        ? "gmail-school-office-conversation"
        : ordinaryUnused
          ? "gmail-ordinary-unused"
          : initialUnrelatedAccount
            ? "gmail-initial-unrelated-retail-account-alert"
            : founder && recent
              ? SCHOOL_ATTACHMENT.messageId
              : `gmail-${input.ownerAdultId}-${recent}`;
      return {
        status: "complete" as const,
        messages: [
          {
            messageId,
            threadId: `thread-${messageId}`,
            historyId: citedReply ? "106" : founder ? "101" : "201",
            from: citedReply
              ? "office@muir.example"
              : initialUnrelatedAccount
                ? "account@example.test"
                : founder
                  ? "hari-private@example.com"
                  : "alex-private@example.com",
            subject: citedReply
              ? "Emergency card status"
              : ordinaryUnused
                ? "Ordinary family newsletter"
                : initialUnrelatedAccount
                  ? UNRELATED_ACCOUNT_EMAIL_SUBJECT
                  : founder
                    ? "Private school form"
                    : "Private family schedule",
            sentAt: input.before ?? new Date(state.now).toISOString(),
            text: citedReply
              ? "Maya’s emergency card still needs a signature."
              : ordinaryUnused
                ? "This newsletter has no family action Florence should keep."
                : initialUnrelatedAccount
                  ? "Your retail account password was changed."
                  : founder
                    ? "Hari private email: Maya attends Muir Elementary and her form needs a signature."
                    : "Alex private email: a personal appointment moved.",
            textStatus: "complete" as const,
            attachmentsStatus: "complete" as const,
            attachments:
              !citedReply && !ordinaryUnused && !initialUnrelatedAccount && founder && recent
                ? [SCHOOL_ATTACHMENT]
                : [],
          },
        ],
      };
    },
    readGmailAttachment: async (input: {
      householdId: string;
      ownerAdultId: string;
      connectionId: string;
      attachment: GmailAttachmentReference;
    }) => {
      await activeCredential(input);
      expect(input.attachment).toEqual(SCHOOL_ATTACHMENT);
      return { ...input.attachment, bytes: PDF_BYTES };
    },
    readInitialCalendarReview: async (input: {
      householdId: string;
      ownerAdultId: string;
      connectionId: string;
      calendarId?: string;
      currentTime: string;
      limit: number;
    }) => {
      await activeCredential(input);
      if (input.calendarId && state.initialHouseholdCalendarFailuresRemaining > 0) {
        state.initialHouseholdCalendarFailuresRemaining -= 1;
        return { status: "unavailable" as const, events: [], cursor: null };
      }
      return baseline(input);
    },
    readGmailChanges: async (input: {
      householdId: string;
      ownerAdultId: string;
      connectionId: string;
      cursor: { kind: "gmail_history_v1"; historyId: string; capturedAt: string };
    }) => {
      await activeCredential(input);
      state.googleChangeReads.push({ ownerAdultId: input.ownerAdultId, kind: "gmail" });
      const hasUnrelatedAccountEmail =
        input.ownerAdultId === founderSetup().adultId &&
        state.unrelatedAccountEmailPending &&
        !state.unrelatedAccountEmailDelivered;
      if (hasUnrelatedAccountEmail) state.unrelatedAccountEmailDelivered = true;
      const hasPrivateFactUpdate =
        !hasUnrelatedAccountEmail &&
        input.ownerAdultId === founderSetup().adultId &&
        state.privateFactUpdatePending &&
        !state.privateFactUpdateDelivered;
      if (hasPrivateFactUpdate) state.privateFactUpdateDelivered = true;
      const hasGoogleDeletionEvidence =
        !hasPrivateFactUpdate &&
        input.ownerAdultId === founderSetup().adultId &&
        state.googleDeletionEvidencePending &&
        !state.googleDeletionEvidenceDelivered;
      if (hasGoogleDeletionEvidence) state.googleDeletionEvidenceDelivered = true;
      const hasOverlap =
        !hasPrivateFactUpdate &&
        !hasGoogleDeletionEvidence &&
        input.ownerAdultId === founderSetup().adultId &&
        state.overlapGmailReadsRemaining > 0;
      const overlapHistoryId = state.overlapGmailReadsRemaining === 2 ? "104" : "105";
      if (hasOverlap) state.overlapGmailReadsRemaining -= 1;
      return {
        status: "complete" as const,
        resyncRequired: false as const,
        removedMessageIds: [],
        messages: hasUnrelatedAccountEmail
          ? [
              {
                messageId: "gmail-unrelated-retail-account-alert",
                threadId: "gmail-unrelated-retail-account-alert-thread",
                historyId: "102",
                from: "account@example.test",
                subject: UNRELATED_ACCOUNT_EMAIL_SUBJECT,
                sentAt: new Date(state.now).toISOString(),
                text: "Your retail account password was changed.",
                textStatus: "complete" as const,
                attachmentsStatus: "complete" as const,
                attachments: [],
              },
            ]
          : hasPrivateFactUpdate
            ? [
                {
                  messageId: "gmail-maya-school-enrollment-update",
                  threadId: "gmail-maya-school-enrollment-thread",
                  historyId: "103",
                  from: "registrar@muir.example",
                  subject: "Maya school enrollment update",
                  sentAt: new Date(state.now).toISOString(),
                  text: UPDATED_PRIVATE_SCHOOL_FACT,
                  textStatus: "complete" as const,
                  attachmentsStatus: "complete" as const,
                  attachments: [],
                },
              ]
            : hasGoogleDeletionEvidence
              ? [
                  {
                    messageId: "gmail-maya-emergency-card-reminder",
                    threadId: "gmail-maya-emergency-card-thread",
                    historyId: "106",
                    from: "office@muir.example",
                    subject: GOOGLE_DELETION_GMAIL_SUBJECT,
                    sentAt: new Date(state.now).toISOString(),
                    text: `${GOOGLE_DELETION_FACT} Maya’s emergency card still needs a signature.`,
                    textStatus: "complete" as const,
                    attachmentsStatus: "complete" as const,
                    attachments: [],
                  },
                ]
              : hasOverlap
                ? [
                    {
                      messageId: "gmail-school-bus-overlap",
                      threadId: "gmail-school-bus-overlap-thread",
                      historyId: overlapHistoryId,
                      from: "transport@muir.example",
                      subject: OVERLAP_GMAIL_SUBJECT,
                      sentAt: new Date(state.now).toISOString(),
                      text: "Maya’s school bus route reminder is current.",
                      textStatus: "complete" as const,
                      attachmentsStatus: "complete" as const,
                      attachments: [],
                    },
                  ]
                : [],
        cursor: hasUnrelatedAccountEmail
          ? { ...input.cursor, historyId: "102", capturedAt: new Date(state.now).toISOString() }
          : hasPrivateFactUpdate
            ? { ...input.cursor, historyId: "103", capturedAt: new Date(state.now).toISOString() }
            : hasGoogleDeletionEvidence
              ? { ...input.cursor, historyId: "106", capturedAt: new Date(state.now).toISOString() }
              : hasOverlap
                ? {
                    ...input.cursor,
                    historyId: overlapHistoryId,
                    capturedAt: new Date(state.now).toISOString(),
                  }
                : input.cursor,
      };
    },
    readCalendarChanges: async (input: {
      householdId: string;
      ownerAdultId: string;
      connectionId: string;
      calendarId: string;
      cursor: {
        kind: "calendar_updated_min_v1";
        calendarId: string;
        updatedMin: string;
        windowTimeMin: string;
        windowTimeMax: string;
        overlapMs: 300_000;
      };
      currentTime: string;
    }) => {
      await activeCredential(input);
      state.googleChangeReads.push({ ownerAdultId: input.ownerAdultId, kind: "calendar" });
      const hasCalendarOnlyChange =
        input.ownerAdultId === founderSetup().adultId &&
        input.calendarId === "primary" &&
        state.calendarOnlyChangePending &&
        !state.calendarOnlyChangeDelivered;
      if (hasCalendarOnlyChange) state.calendarOnlyChangeDelivered = true;
      const hasPrivateCalendarAnniversary =
        input.ownerAdultId === founderSetup().adultId &&
        input.calendarId === "primary" &&
        state.privateCalendarAnniversaryPending &&
        !state.privateCalendarAnniversaryDelivered;
      if (hasPrivateCalendarAnniversary) state.privateCalendarAnniversaryDelivered = true;
      const hasFamilyCalendarEcho =
        input.calendarId !== "primary" &&
        state.familyCalendarEchoPending &&
        !state.familyCalendarEchoDelivered;
      if (hasFamilyCalendarEcho) state.familyCalendarEchoDelivered = true;
      const hasFamilyCalendarMixedChange =
        input.calendarId !== "primary" &&
        state.familyCalendarMixedChangePending &&
        !state.familyCalendarMixedChangeDelivered;
      if (hasFamilyCalendarMixedChange) state.familyCalendarMixedChangeDelivered = true;
      const hasFamilyCalendarRealOnlyOverlap =
        input.calendarId !== "primary" &&
        state.familyCalendarRealOnlyOverlapPending &&
        !state.familyCalendarRealOnlyOverlapDelivered;
      if (hasFamilyCalendarRealOnlyOverlap) state.familyCalendarRealOnlyOverlapDelivered = true;
      const hasPrivateCalendarAdultEvent =
        input.ownerAdultId === founderSetup().adultId &&
        input.calendarId === "primary" &&
        state.privateCalendarAdultEventPending &&
        !state.privateCalendarAdultEventDelivered;
      if (hasPrivateCalendarAdultEvent) state.privateCalendarAdultEventDelivered = true;
      return {
        status: "complete" as const,
        resyncRequired: false as const,
        events: hasFamilyCalendarRealOnlyOverlap
          ? [{ ...FAMILY_CALENDAR_MIXED_CHANGE_EVENT, providerUpdatedAt: input.currentTime }]
          : hasFamilyCalendarMixedChange
            ? [
                ...[...state.calendarEvents.values()].filter(
                  (event) => event.title === PRIVATE_CALENDAR_ANNIVERSARY_TITLE,
                ),
                { ...FAMILY_CALENDAR_MIXED_CHANGE_EVENT, providerUpdatedAt: input.currentTime },
              ]
            : hasFamilyCalendarEcho
              ? [...state.calendarEvents.values()].filter(
                  (event) => event.title === PRIVATE_CALENDAR_ANNIVERSARY_TITLE,
                )
              : hasPrivateCalendarAdultEvent
                ? [{ ...PRIVATE_CALENDAR_ADULT_EVENT, providerUpdatedAt: input.currentTime }]
                : hasPrivateCalendarAnniversary
                  ? [{ ...PRIVATE_CALENDAR_ANNIVERSARY_EVENT, providerUpdatedAt: input.currentTime }]
                  : hasCalendarOnlyChange
                    ? [
                        { ...PRIVATE_CALENDAR_ONLY_EVENT, providerUpdatedAt: input.currentTime },
                        { ...PRIVATE_CALENDAR_CONFLICT_EVENT, providerUpdatedAt: input.currentTime },
                      ]
                    : [],
        cursor: {
          ...input.cursor,
          updatedMin: input.currentTime,
          windowTimeMin: input.currentTime,
          windowTimeMax: new Date(Date.parse(input.currentTime) + 21 * 24 * 60 * 60_000).toISOString(),
        },
      };
    },
    readCalendarWindow: async (input: CalendarReadInput) => {
      await activeCredential(input);
      state.interactiveGoogleReads += 1;
      state.calendarReads.push(input);
      return {
        status: "complete" as const,
        events: [...state.calendarEvents.values()].filter((event) => overlaps(event, input)),
        cursor: {
          kind: "calendar_updated_min_v1" as const,
          calendarId: input.calendarId ?? "primary",
          updatedMin: new Date(state.now).toISOString(),
          windowTimeMin: input.timeMin,
          windowTimeMax: input.timeMax,
          overlapMs: 300_000 as const,
        },
      };
    },
    executeCalendar: async (input: CalendarExecutionInput): Promise<GoogleCalendarExecutionResult> => {
      state.calendarExecutions.push(input);
      const providerEventId =
        input.mutation.operation === "create"
          ? `google-event-${input.actionId}`
          : input.mutation.target.providerEventId;
      const existing = state.calendarEvents.get(providerEventId);
      if (input.mutation.operation === "create" && existing) {
        expect(existing).toMatchObject(input.mutation.event);
        return {
          status: "committed" as const,
          providerEventId,
          providerRevision: existing.providerRevision,
          occurredAt: new Date(state.now).toISOString(),
        };
      }
      if (input.mutation.operation !== "create") {
        expect(existing).toMatchObject({
          providerRevision: input.mutation.target.providerRevision,
          title: input.mutation.target.observedEvent.title,
        });
      }
      const title =
        input.mutation.operation === "delete"
          ? input.mutation.target.observedEvent.title
          : input.mutation.event.title;
      state.timeline.push(`provider:${input.mutation.operation}:${title}`);
      if (input.mutation.operation === "delete") {
        state.calendarEvents.delete(providerEventId);
        return {
          status: "committed" as const,
          providerEventId,
          providerRevision: null,
          occurredAt: new Date(state.now).toISOString(),
        };
      }
      const revision = `provider-revision-${state.calendarExecutions.length}`;
      const event = input.mutation.event;
      const common = {
        providerEventId,
        providerRevision: revision,
        providerUpdatedAt: new Date(state.now).toISOString(),
        status: "confirmed" as const,
        busy: true as const,
      };
      state.calendarEvents.set(
        providerEventId,
        event.intervalKind === "timed"
          ? { ...common, ...event, allDay: false }
          : { ...common, ...event, allDay: true },
      );
      if (input.mutation.operation === "create" && state.uncertainCalendarCreateTitle === title) {
        state.uncertainCalendarCreateTitle = null;
        throw new GoogleCalendarTransientError("The Calendar write committed but its response was lost");
      }
      return {
        status: "committed" as const,
        providerEventId,
        providerRevision: revision,
        occurredAt: new Date(state.now).toISOString(),
      };
    },
  } as unknown as GoogleConnection;
}

function fakeResponseStream(events: readonly unknown[], response: unknown) {
  return {
    async *[Symbol.asyncIterator]() {
      for (const event of events) yield event;
    },
    async finalResponse() {
      return response;
    },
  };
}

function decision(
  input: {
    bubbles?: FlorenceDecision["conversation"]["bubbles"];
    facts?: FlorenceDecision["facts"];
    followUp?: FlorenceDecision["followUp"];
    interest?: FlorenceDecision["interest"];
    calendar?: FlorenceDecision["calendar"];
    householdUpdate?: FlorenceDecision["householdUpdate"];
    webAccessPath?: FlorenceDecision["webAccessPath"];
    researchUrls?: FlorenceDecision["researchUrls"];
  } = {},
): FlorenceDecision {
  return {
    policy: { retain: true, schedule: true, stopMessaging: false },
    conversation: {
      replyToCurrentMessage: false,
      reaction: null,
      bubbles: input.bubbles ?? [],
    },
    facts: input.facts ?? [],
    followUp: input.followUp ?? null,
    interest: input.interest ?? null,
    calendar: input.calendar ?? null,
    householdUpdate: input.householdUpdate ?? null,
    webAccessPath: input.webAccessPath ?? null,
    researchUrls: input.researchUrls ?? null,
  };
}

function remember(statement: string, sourceId: string): FlorenceDecision["facts"][number] {
  return { operation: "remember", factId: null, statement, sourceIds: [sourceId] };
}

function calendarDecision(
  sourceId: string,
  mutation: CalendarMutation,
): NonNullable<FlorenceDecision["calendar"]> {
  return {
    mode: "direct",
    proposalId: null,
    mutation,
    sourceIds: [sourceId],
  };
}

async function calendarTarget(
  reads: Parameters<Reason>[1],
  input: Parameters<Reason>[0],
  expected: typeof PICKUP_EVENT,
) {
  const connection = input.googleConnections.find((candidate) => candidate.kind === "family");
  if (!connection) throw new Error("Family Calendar connection is missing");
  const result = await reads.readCalendarWindow({
    connectionId: connection.connectionId,
    timeMin: "2026-08-18T21:30:00.000Z",
    timeMax: "2026-08-18T22:30:00.000Z",
    limit: 50,
  });
  const event = result.events.find((candidate) => candidate.title === expected.title);
  if (event?.intervalKind !== "timed") throw new Error("Maya pickup was not found");
  return {
    providerEventId: event.providerEventId,
    providerRevision: event.providerRevision,
    observedEvent: {
      intervalKind: "timed" as const,
      title: event.title ?? expected.title,
      startsAt: event.startsAt,
      endsAt: event.endsAt,
      timeZone: event.timeZone,
      location: event.location,
    },
  };
}

function overlaps(event: FakeCalendarEvent, window: { timeMin: string; timeMax: string }): boolean {
  const startsAt = event.intervalKind === "timed" ? event.startsAt : `${event.startDate}T00:00:00.000Z`;
  const endsAt = event.intervalKind === "timed" ? event.endsAt : `${event.endDate}T00:00:00.000Z`;
  return endsAt > window.timeMin && startsAt < window.timeMax;
}

function founderSetup() {
  return new EnrollmentCodes(ENROLLMENT_SECRET).issueFounderSetup({
    providerConversationId: PRIVATE_FOUNDER,
    identitySubjectDigest: FOUNDER_IDENTITY,
    occurredAt: new Date(NOW).toISOString(),
  });
}

function familyProfileInput(): Parameters<Florence["completeFamilyOnboarding"]>[1] {
  return {
    mode: "two_adult",
    postalCode: "94110",
    partner: { firstName: "Alex", lastName: "Anbarasu", phoneNumber: PARTNER_PHONE.slice(2) },
    children: [
      {
        firstName: "Maya",
        lastName: "Anbarasu",
        age: 8,
        grade: "3rd grade",
        school: "Muir Elementary",
        activities: ["Soccer", "Piano"],
      },
    ],
  };
}

function withSchema(connectionString: string, schema: string): string {
  const url = new URL(connectionString);
  url.searchParams.set("options", `-c search_path=${schema}`);
  return url.toString();
}

function sqlLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function digest(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function inboundSourceId(providerEventId: string): string {
  const hex = digest(`linq-v3\0signal\0${providerEventId}`);
  const variant = ((Number.parseInt(hex[16] ?? "0", 16) & 3) | 8).toString(16);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-${variant}${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}
