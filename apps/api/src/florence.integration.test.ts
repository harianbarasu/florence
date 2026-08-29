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
  type GoogleCalendarWindowEvent,
  type GoogleConnection,
  GoogleConnectionError,
  type GoogleFamilyCalendarProvisioningResult,
  type GoogleFamilyCalendarRenameResult,
  GoogleFamilyCalendarTransientError,
  type GooglePersonalCalendarWindowEvent,
  type GoogleWorkspaceOperation,
  type GoogleWorkspaceResult,
} from "@florence/google";
import {
  type LinqClient,
  type LinqConversationAuthority,
  type LinqCreateChat,
  type LinqCreatedChat,
  LinqError,
  type LinqMediaReference,
  type LinqSendMessage,
  type LinqSendMove,
  type LinqSendReaction,
  linqIdentitySubjectDigest,
} from "@florence/linq";
import { describe, expect, onTestFinished, test } from "vitest";
import { buildApp, createSessionCallerResolver } from "./app.js";
import type { FlorenceBrowserClient } from "./browser.js";
import { EnrollmentCodes } from "./enrollment.js";
import { Florence } from "./florence.js";
import { createLinqIngress } from "./linq-ingress.js";
import {
  type FlorenceDecision,
  type FlorenceReadTools,
  FlorenceReasoner,
  FlorenceReasonerError,
  type FlorenceReasonerInput,
  florenceGoogleChangesAssessmentInputSchema,
  florenceReasonerInputSchema,
} from "./reasoner.js";

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
const PARTNER_INVITATION_APPROVAL_REPLY = "Got it—I’ll text Alex now.";
const PARTNER_REINVITATION_APPROVAL_REPLY = PARTNER_INVITATION_APPROVAL_REPLY;
const PARTNER_INVITATION_CONTRADICTION = "I can’t text Alex from here.";
const PARTNER_SETUP_QUESTION = "What is this setup for?";
const PARTNER_SETUP_HANDSHAKE_REPLY = "Hi Florence";
const PARTNER_SETUP_REFUSAL = "I don’t want to join this.";
const PARTNER_SETUP_EXPLANATION =
  "That link sets up your own private side of Florence. Use the setup link just above when you’re ready.";
const PARTNER_SETUP_HANDSHAKE_ACK = "Thanks—here’s your private setup link.";
const PARTNER_SETUP_REFRESH_ACK = "Of course—here’s a fresh private setup link.";
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
const NATIVE_DOCKET_SUMMARY = "Maya’s field-trip form needs one parent signature by Tuesday.";
const NATIVE_DOCKET_OWNER = "Parents";
const NATIVE_DOCKET_NEXT_ACTION = "Choose who will sign and return Maya’s field-trip form.";
const NATIVE_DOCKET_WAITING_ON = "A parent to claim the signature";
const NATIVE_DOCKET_CORRECTION =
  "Correction: Maya’s field-trip form needs one parent signature by Wednesday, not Tuesday.";
const NATIVE_DOCKET_UPDATED_SUMMARY = "Maya’s field-trip form needs one parent signature by Wednesday.";
const NATIVE_DOCKET_UPDATED_NEXT_ACTION =
  "Choose who will sign and return Maya’s field-trip form by Wednesday.";
const NATIVE_DOCKET_WORK_REQUEST = "Please handle that updated field-trip form from the docket.";
const NATIVE_DOCKET_WORK_OBJECTIVE =
  "Use the exact family Message and its photo, link, voice note, and PDF to handle Maya’s updated field-trip form.";
const NATIVE_DOCKET_WORK_ACK = "I’m on it—I’ll use the exact school update already on the docket.";
const NATIVE_DOCKET_WORK_RESULT = "I handled the updated field-trip form from the exact school materials.";
const PRIVATE_CONVERSATION_DOCKET_REQUEST =
  "Keep this private: I still need to decide whether I can volunteer at Maya’s school.";
const PRIVATE_CONVERSATION_DOCKET_SUMMARY = "Decide whether to volunteer at Maya’s school.";
const PRIVATE_CONVERSATION_DOCKET_NEXT_ACTION = "Decide whether you can volunteer at Maya’s school.";
const PRIVATE_CONVERSATION_DOCKET_WAITING_ON = "Your decision";
const PRIVATE_CONVERSATION_DOCKET_HANDLED = "I decided about the private school volunteer note.";
const PRIVATE_CONVERSATION_DOCKET_HANDLED_ACK = "Got it—I’ll take that private decision off your docket.";
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
const TRANSIENT_RETRY_CUE = "One sec—I hit a snag, but I’m trying that again.";
const TRANSIENT_RETRY_REPLY = "I’m back—what would you like me to check?";
const NO_RETENTION_OR_SCHEDULING_REQUEST =
  "Answer this, but don’t remember it or schedule anything: the temporary code is blue.";
const NO_RETENTION_OR_SCHEDULING_REPLY = "Understood—the temporary code is blue.";
const NO_RETENTION_OR_SCHEDULING_FACT = "The temporary code is blue.";
const STALE_RECEIPT_QUESTION = "Can you confirm this delivery is current?";
const STALE_RECEIPT_REPLY = "This reply must have a current provider receipt.";
const ONE_SHOT_REMINDER_REQUEST = "Remind me to pick up the kids at 2:45 today.";
const ONE_SHOT_REMINDER_ACK = "Absolutely—I’ll remind you to pick up the kids at 2:45 PM.";
const ONE_SHOT_REMINDER_ACTION = "pick up the kids";
const ONE_SHOT_REMINDER_TEXT = "Reminder: pick up the kids.";
const ONE_SHOT_REMINDER_AT = "2026-08-19T21:45:00.000Z";
const RECURRING_REMINDER_REQUEST = "Every ten minutes, remind us to stretch.";
const RECURRING_REMINDER_ACK = "Done—I’ll remind this group every ten minutes to stretch.";
const RECURRING_REMINDER_ACTION = "stretch";
const RECURRING_REMINDER_TEXT = "Reminder: stretch.";
const RECURRING_REMINDER_ANCHOR = "2026-08-19T21:46:00.000Z";
const LIST_REMINDERS_REQUEST = "What reminders are set?";
const LIST_REMINDERS_REPLY = "Stretch — every ten minutes, starting at 2:46 PM.";
const UPDATE_REMINDER_REQUEST = "Make the stretch reminder every five minutes starting at 2:47.";
const UPDATE_REMINDER_ACK = "Done—I moved the stretch reminder to every five minutes from 2:47 PM.";
const UPDATED_RECURRING_REMINDER_ANCHOR = "2026-08-19T21:47:00.000Z";
const PAUSE_REMINDER_REQUEST = "Pause the stretch reminder.";
const PAUSE_REMINDER_ACK = "Paused the stretch reminder.";
const LIST_PAUSED_REMINDERS_REQUEST = "Which reminders are paused?";
const LIST_PAUSED_REMINDERS_REPLY = "Stretch — every five minutes — paused.";
const RESUME_REMINDER_REQUEST = "Resume the stretch reminder.";
const RESUME_REMINDER_ACK = "Resumed—it’ll next remind this group at 2:52 PM.";
const RUN_REMINDER_REQUEST = "Run the stretch reminder now.";
const CANCEL_REMINDER_REQUEST = "Cancel the stretch reminder.";
const CANCEL_REMINDER_ACK = "Cancelled the stretch reminder.";
const PRIVATE_CALENDAR_ONLY_TITLE = "Maya’s soccer clinic";
const PRIVATE_CALENDAR_CONFLICT_TITLE = "School volunteer shift";
const PRIVATE_CALENDAR_ANNIVERSARY_TITLE = "Private anniversary dinner";
const PRIVATE_CALENDAR_ADULT_TITLE = "Private medical appointment";
const FAMILY_CALENDAR_MIXED_CHANGE_TITLE = "Maya’s school photo day";
const FAMILY_CALENDAR_MIXED_CHANGE_SUMMARY = "Maya’s school photo day is September 3.";
const PRIVATE_CALENDAR_OWNER_APPROVAL = "Yes, add that exact event.";
const PRIVATE_CALENDAR_OWNER_APPROVAL_REPLY =
  "Got it—I’ll add “Private anniversary dinner” to the family calendar.";
const PRIVATE_CALENDAR_OWNER_CONTRADICTION = "I can’t add that to the family calendar.";
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
const GOOGLE_RECIPE_SUBJECT = "Family sesame noodles recipe";
const GOOGLE_RECIPE_SLOT = "family:weeknight_sesame_noodles";
const GOOGLE_RECIPE_STATEMENT = "The family has a reusable weeknight sesame noodles recipe.";
const GOOGLE_RECIPE_TITLE = "Weeknight sesame noodles";
const GOOGLE_RECIPE_DETAILS =
  "Ingredients: 12 ounces spaghetti; 3 tablespoons soy sauce; 1 tablespoon sesame oil; 2 teaspoons rice vinegar. Method: cook the spaghetti and toss it with the sauce while warm. Family note: keep it mild.";
const GOOGLE_RECIPE_QUESTION = "What was that mild noodle dinner we saved for hectic nights?";
const GOOGLE_RECIPE_REPLY =
  "The weeknight sesame noodles use spaghetti, soy sauce, sesame oil, and rice vinegar—tossed warm and kept mild.";
const PROACTIVE_FAMILY_WORK_OBJECTIVE =
  "Use the shared Family Calendar and saved weeknight recipe to prepare a practical plan for the week.";
const PROACTIVE_FAMILY_WORK_KICKOFF =
  "I noticed Maya’s field-trip deadline on the family calendar and remembered your mild sesame-noodle recipe, so I’m putting together a practical plan for the week now.";
const PROACTIVE_FAMILY_WORK_RESULT =
  "I put together a practical family plan from the Family Calendar and the useful details already in the Vault.";
const GOOGLE_DELETION_PRIVATE_ALERT = "Muir says Maya’s emergency card still needs a signature.";
const UNRELATED_ACCOUNT_EMAIL_SUBJECT = "Your retail account password has changed";
const UNRELATED_ACCOUNT_EMAIL_ALERT =
  "A retail account password-change alert arrived. Verify the change or secure the account.";
const UNRELATED_ACCOUNT_MONITOR_OBJECTIVE =
  "Resolve whether the retail account password change was authorized.";
const UNRELATED_ACCOUNT_FACT_SLOT = "adult:retail_account_security";
const UNRELATED_ACCOUNT_FACT = "The retail account password changed.";
const PRIVATE_INITIAL_ONLY_FINDING = "The school office contact stays private to this parent.";
const PRIVATE_DOCKET_RECALL_REQUEST = "What was that private school-office loose end?";
const PRIVATE_DOCKET_RECALL_REPLY = "The school office contact is still on your private docket.";
const PRIVATE_DOCKET_HANDLED_REQUEST = "That private school-office loose end is handled.";
const PRIVATE_DOCKET_HANDLED_REPLY = "Got it—I’ll take that off your private docket.";
const SHARED_DUPLICATE_CONFLICT_SUMMARY = "Fall activity registration needs a family decision.";
const PARTNER_DUPLICATE_CONFLICT_SUMMARY = "The family still needs to decide on fall registration.";
const FOUNDER_FORM_SUMMARY = "Maya’s field-trip form still needs a parent response.";
const FOUNDER_FORM_NEXT_ACTION = "Choose who will sign and return Maya’s field-trip form.";
const FOUNDER_FORM_WAITING_ON = "A parent response";
const PARTNER_PERMISSION_SUMMARY = "Maya’s permission-slip deadline still needs family attention.";
const PARTNER_PERMISSION_NEXT_ACTION = "Confirm and return Maya’s permission slip.";
const PARTNER_PERMISSION_WAITING_ON = "Alex’s confirmation";
const FAMILY_MEETING_SUMMARY = "The family meeting is Tuesday at 8:00 PM.";
const SCHOOL_HANDOFF_SUMMARY = "Friday’s school pickup handoff still needs an owner.";
const HOUSEHOLD_DOCKET_REQUEST = "What’s on the docket?";
const HOUSEHOLD_DOCKET_REPLY = "The three most time-sensitive family items are at the top.";
const HOUSEHOLD_DOCKET_HANDLED = "The permission slip is handled.";
const HOUSEHOLD_DOCKET_HANDLED_ACK = "Got it—I took the permission slip off the docket.";

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
type CalendarMutation = NonNullable<FlorenceDecision["calendar"]>["mutation"];
type FamilyCalendarProvisioningInput = Parameters<GoogleConnection["provisionFamilyCalendar"]>[0];
type CalendarReadInput = Parameters<GoogleConnection["readCalendarWindow"]>[0];
type PersonalCalendarCatalogInput = Parameters<GoogleConnection["readPersonalCalendarCatalog"]>[0];
type PersonalCalendarReadInput = Parameters<GoogleConnection["readPersonalCalendarWindow"]>[0];
type ExactCalendarCatalogInput = Parameters<GoogleConnection["readExactCalendarCatalog"]>[0];
type ExactCalendarReadInput = Parameters<GoogleConnection["readExactCalendarWindow"]>[0];
type WorkspaceExecutionInput = Parameters<GoogleConnection["runWorkspace"]>[0];
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
  personalCalendarReads: PersonalCalendarReadInput[];
  calendarEvents: Map<string, FakeCalendarEvent>;
  uncertainCalendarCreateTitle: string | null;
  timeline: string[];
  finiteReviews: number;
  interestResearches: number;
  interestResearchInputs: Parameters<FlorenceReasoner["researchInterest"]>[0][];
  interestBusyUnionExercise: boolean;
  voiceTranscriptions: number;
  initialGoogleFailuresRemaining: number;
  initialClassifierFailuresRemaining: number;
  initialGoogleFailureAdultId: string | null;
  completeScanPaginationExercise: boolean;
  completeFactSupportExercise: boolean;
  retainedFactBeyondReviewWindowExercise: boolean;
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
  linkedGmailMonitorExercise: boolean;
  exactGmailReads: Parameters<GoogleConnection["readGmailMessage"]>[0][];
  monitorCancellationActive: boolean;
  silentMonitorSourceId: string | null;
  voicedMonitorSourceId: string | null;
  cancelledMonitorSourceId: string | null;
  setupConversationFailuresRemaining: number;
  founderProductRecenterReview: boolean;
  googleRecipeArtifactExercise: boolean;
  proactiveFamilyWorkExercise: boolean;
  familyCalendarProvisioningFailuresRemaining: number;
  invalidGrantAdultId: string | null;
  invalidGrantTriggered: boolean;
  googleDeletionEvidencePending: boolean;
  googleDeletionEvidenceDelivered: boolean;
  googleDeletionSourceId: string | null;
  googleChangeReads: { ownerAdultId: string; kind: "gmail" | "calendar" }[];
  interactiveGoogleReads: number;
  workspaceExecutions: WorkspaceExecutionInput[];
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

release("Durable family work store", () => {
  test("recovers one persisted task and completes only after its terminal receipt", async () => {
    if (!TEST_DATABASE_URL) throw new Error("TEST_DATABASE_URL is required");
    const directory = await mkdtemp(join(tmpdir(), "florence-family-work-store-"));
    const schema = `florence_${randomUUID().replaceAll("-", "")}`;
    const setupFile = join(directory, "schema.sql");
    const assertionFile = join(directory, "assert.sql");
    const migrations = (await Promise.all(migrationFiles.map((file) => readFile(file, "utf8")))).join("\n");
    const base = Date.parse("2026-08-27T20:00:00.000Z");
    const at = (offset: number): string => new Date(base + offset).toISOString();
    const initialState = {
      kind: "family_work_v1",
      version: 1,
      generation: 0,
      phase: "ready",
      claim: null,
      activePhoneCall: null,
      activeTextMessage: null,
      browserSession: null,
      continuationItems: [],
      pendingCall: null,
      steering: [],
      progressRevision: 0,
      terminal: null,
    } as const;
    const steeredInitialState = {
      ...initialState,
      steering: [
        {
          sourceId: "10000000-0000-4000-8000-000000000017",
          text: "Please use the latest camp instructions.",
          occurredAt: at(-9_400),
        },
      ],
    } as const;
    const cancelledBrowserSession = {
      sessionId: "browserbase-cancelled-session",
      expiresAt: at(3_600_000),
    } as const;
    const cancelledPhoneCall = {
      provider: "bland",
      kind: "agent",
      providerCallId: "bland-cancelled-call",
    } as const;
    const pendingCancelledPhoneCall = {
      ...cancelledPhoneCall,
      providerCallId: `pending_bland_${"a".repeat(64)}_KzE1NTU1MjEyMTI`,
    } as const;
    const pendingCancelledTwilioCall = {
      provider: "twilio",
      kind: "announcement",
      providerCallId: `pending_twilio_call_${base.toString(36)}_${"b".repeat(64)}_KzE1NTU1MjEyMTI`,
    } as const;
    const resolvedCancelledTwilioCall = {
      ...pendingCancelledTwilioCall,
      providerCallId: "CA-resolved-cancelled-call",
    } as const;
    const cancelledBrowserState = {
      ...initialState,
      generation: 1,
      phase: "terminal",
      activePhoneCall: cancelledPhoneCall,
      browserSession: cancelledBrowserSession,
      progressRevision: 1,
      terminal: { outcome: "cancelled", text: "Cancelled." },
    } as const;
    await writeFile(
      setupFile,
      `CREATE SCHEMA "${schema}"; SET search_path TO "${schema}";
      ${migrations}
      insert into households (id,name,time_zone)
      values ('10000000-0000-4000-8000-000000000001','Test','UTC');
      insert into people (
        id,household_id,kind,role,adult_slot,display_name,status,identity_subject_digest,
        consent_version,consented_at,profile,preferences
      ) values (
        '10000000-0000-4000-8000-000000000002',
        '10000000-0000-4000-8000-000000000001','adult','steward',1,'Parent','verified',
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        'v1',${sqlLiteral(at(-10_000))},'{"firstName":"Parent"}'::jsonb,'{}'::jsonb
      );
      insert into linq_channels (
        id,household_id,audience,provider_conversation_id,adult_one_id,identity_one_digest,
        adult_two_id,identity_two_digest,authority_digest,bound_at
      ) values (
        '10000000-0000-4000-8000-000000000004',
        '10000000-0000-4000-8000-000000000001','private','test-chat',
        '10000000-0000-4000-8000-000000000002',
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',null,null,
        '03f42c1de0ff180a3283da8a2bc750995dc47bcd5f09565f1f6b88f9ce8846c2',
        ${sqlLiteral(at(-10_000))}
      );
      insert into proactive_work (
        id,household_id,kind,visibility,owner_adult_id,objective,reminder_schedule,
        status,next_check_at,created_at
      ) values (
        '10000000-0000-4000-8000-000000000005',
        '10000000-0000-4000-8000-000000000001','reminder','private',
        '10000000-0000-4000-8000-000000000002','Check the oven',
        ${sqlLiteral(JSON.stringify({ kind: "once", at: at(-2_000) }))}::jsonb,
        'active',${sqlLiteral(at(-2_000))},${sqlLiteral(at(-10_000))}
      );
      insert into proactive_work (
        id,household_id,kind,visibility,owner_adult_id,objective,current_conclusion,
        task_state,status,next_check_at,created_at
      ) values (
        '10000000-0000-4000-8000-000000000003',
        '10000000-0000-4000-8000-000000000001','family_task','private',
        '10000000-0000-4000-8000-000000000002','Prepare the camp registration','Starting now.',
        ${sqlLiteral(JSON.stringify(steeredInitialState))}::jsonb,'active',
        ${sqlLiteral(at(-1_000))},${sqlLiteral(at(-9_450))}
      );
      insert into sources (
        id,household_id,kind,visibility,owner_adult_id,external_key,label,metadata,occurred_at
      ) values (
        '10000000-0000-4000-8000-000000000007',
        '10000000-0000-4000-8000-000000000001','linq_message','private',
        '10000000-0000-4000-8000-000000000002','family-work-request','Family work request',
        ${sqlLiteral(
          JSON.stringify({
            authoredText: "Please use this revised request.",
            voiceTranscriptPresent: true,
            supersedesSourceId: "10000000-0000-4000-8000-000000000009",
          }),
        )}::jsonb,${sqlLiteral(at(-9_500))}
      ),(
        '10000000-0000-4000-8000-000000000009',
        '10000000-0000-4000-8000-000000000001','linq_message','private',
        '10000000-0000-4000-8000-000000000002','family-work-request-original',
        'Original family work request',
        '{"authoredText":"Prepare the original camp form.","voiceTranscriptPresent":false}'::jsonb,
        ${sqlLiteral(at(-9_700))}
      ),(
        '10000000-0000-4000-8000-000000000010',
        '10000000-0000-4000-8000-000000000001','linq_message','private',
        '10000000-0000-4000-8000-000000000002','family-work-reply-target',
        'Florence question',
        '{"authoredText":"Which camp form should I use?","voiceTranscriptPresent":false}'::jsonb,
        ${sqlLiteral(at(-9_600))}
      ),(
        '10000000-0000-4000-8000-000000000017',
        '10000000-0000-4000-8000-000000000001','linq_message','private',
        '10000000-0000-4000-8000-000000000002','family-work-steering',
        'Latest family work steering',
        '{"authoredText":"Please use the latest camp instructions.","voiceTranscriptPresent":false}'::jsonb,
        ${sqlLiteral(at(-9_400))}
      );
      insert into messages (
        source_id,channel_id,direction,sender_adult_id,move_kind,text,provider_event_id,
        provider_message_id,reply_to_source_id,turn_id,turn_part,status,images,idempotency_key,
        sent_at
      ) values (
        '10000000-0000-4000-8000-000000000009',
        '10000000-0000-4000-8000-000000000004','inbound',
        '10000000-0000-4000-8000-000000000002','message','Prepare the original camp form.',
        'family-work-original-event','family-work-original-message',null,
        '10000000-0000-4000-8000-000000000013',0,'handled','[]'::jsonb,null,null
      ),(
        '10000000-0000-4000-8000-000000000010',
        '10000000-0000-4000-8000-000000000004','outbound',null,'message',
        'Which camp form should I use?',null,'family-work-reply-target-message',null,
        '10000000-0000-4000-8000-000000000014',0,'sent',
        '[{"assetId":"10000000-0000-4000-8000-000000000016","mimeType":"image/png"}]'::jsonb,
        'family-work-reply-target-idempotency',${sqlLiteral(at(-9_600))}
      ),(
        '10000000-0000-4000-8000-000000000007',
        '10000000-0000-4000-8000-000000000004','inbound',
        '10000000-0000-4000-8000-000000000002','reply',
        E'Please use this revised request.\nVoice transcript: The blue form is the right one.',
        'family-work-event','family-work-message','10000000-0000-4000-8000-000000000010',
        '10000000-0000-4000-8000-000000000008',0,'handled',
        '[{"assetId":"10000000-0000-4000-8000-000000000012","mimeType":"image/jpeg"}]'::jsonb,
        null,null
      ),(
        '10000000-0000-4000-8000-000000000017',
        '10000000-0000-4000-8000-000000000004','inbound',
        '10000000-0000-4000-8000-000000000002','message',
        'Please use the latest camp instructions.',
        'family-work-steering-event','family-work-steering-message',null,
        '10000000-0000-4000-8000-000000000018',0,'handled','[]'::jsonb,null,null
      );
      insert into sources (
        id,household_id,kind,visibility,owner_adult_id,external_key,parent_source_id,label,
        occurred_at
      ) values (
        '10000000-0000-4000-8000-000000000011',
        '10000000-0000-4000-8000-000000000001','document','private',
        '10000000-0000-4000-8000-000000000002','family-work-pdf',
        '10000000-0000-4000-8000-000000000007','revised-camp-form.pdf',
        ${sqlLiteral(at(-9_500))}
      ),(
        '10000000-0000-4000-8000-000000000015',
        '10000000-0000-4000-8000-000000000001','document','private',
        '10000000-0000-4000-8000-000000000002','family-work-reply-pdf',
        '10000000-0000-4000-8000-000000000010','original-camp-form.pdf',
        ${sqlLiteral(at(-9_600))}
      );
      insert into documents (
        source_id,saved_by_adult_id,filename,mime_type,content_digest,retained,
        content_envelope,discard_after
      ) values (
        '10000000-0000-4000-8000-000000000011',
        '10000000-0000-4000-8000-000000000002','revised-camp-form.pdf','application/pdf',
        '${"a".repeat(64)}',false,decode('010203','hex'),${sqlLiteral(at(86_400_000))}
      ),(
        '10000000-0000-4000-8000-000000000015',
        '10000000-0000-4000-8000-000000000002','original-camp-form.pdf','application/pdf',
        '${"b".repeat(64)}',false,decode('040506','hex'),${sqlLiteral(at(86_400_000))}
      );
      insert into proactive_work_sources (work_id,source_id) values (
        '10000000-0000-4000-8000-000000000003',
        '10000000-0000-4000-8000-000000000007'
      ),(
        '10000000-0000-4000-8000-000000000003',
        '10000000-0000-4000-8000-000000000017'
      );
      insert into proactive_work (
        id,household_id,kind,visibility,owner_adult_id,objective,current_conclusion,
        task_state,status,next_check_at,created_at
      ) values (
        '10000000-0000-4000-8000-000000000006',
        '10000000-0000-4000-8000-000000000001','family_task','private',
        '10000000-0000-4000-8000-000000000002','Cancelled camp registration','Cancelled.',
        ${sqlLiteral(JSON.stringify(cancelledBrowserState))}::jsonb,'cancelled',null,
        ${sqlLiteral(at(-8_000))}
      );`,
    );
    const databaseUrl = withSchema(TEST_DATABASE_URL, schema);
    await migrateDatabase(databaseUrl, setupFile);
    let store = new PostgresFlorenceStore(databaseUrl);
    const assertDatabase = async (message: string, conditionSql: string): Promise<void> => {
      await writeFile(
        assertionFile,
        `do $florence_assert$ begin
          if not (${conditionSql}) then raise exception using message=${sqlLiteral(message)}; end if;
        end $florence_assert$;`,
      );
      await migrateDatabase(databaseUrl, assertionFile);
    };
    onTestFinished(async () => {
      await store.close().catch(() => undefined);
      await writeFile(setupFile, `DROP SCHEMA IF EXISTS "${schema}" CASCADE;`);
      await migrateDatabase(TEST_DATABASE_URL, setupFile);
      await rm(directory, { recursive: true, force: true });
    });

    await assertDatabase(
      "The cancelled family-task cleanup lease is not protected by its named due-state check",
      `exists (
        select 1 from pg_constraint
        where conrelid='proactive_work'::regclass
          and conname='proactive_work_due_state_check'
      )`,
    );

    expect(await store.takeCancelledFamilyWorkResources("10000000-0000-4000-8000-000000000006")).toEqual({
      browserSession: cancelledBrowserSession,
      activePhoneCall: cancelledPhoneCall,
    });
    expect(await store.takeCancelledFamilyWorkResources("10000000-0000-4000-8000-000000000006")).toEqual({
      browserSession: null,
      activePhoneCall: cancelledPhoneCall,
    });
    expect(
      await store.clearCancelledFamilyWorkPhoneCall(
        "10000000-0000-4000-8000-000000000006",
        cancelledPhoneCall,
      ),
    ).toBe(true);
    expect(
      await store.retainCancelledFamilyWorkPhoneCall(
        "10000000-0000-4000-8000-000000000006",
        pendingCancelledTwilioCall,
        at(2),
      ),
    ).toBe(true);
    expect(
      await store.adoptFamilyWorkPhoneCall(
        "10000000-0000-4000-8000-000000000006",
        resolvedCancelledTwilioCall,
        at(2),
      ),
    ).toBe(true);
    expect(await store.takeCancelledFamilyWorkResources("10000000-0000-4000-8000-000000000006")).toEqual({
      browserSession: null,
      activePhoneCall: resolvedCancelledTwilioCall,
    });
    expect(
      await store.clearCancelledFamilyWorkPhoneCall(
        "10000000-0000-4000-8000-000000000006",
        resolvedCancelledTwilioCall,
      ),
    ).toBe(true);
    expect(await store.takeCancelledFamilyWorkResources("10000000-0000-4000-8000-000000000006")).toBeNull();
    expect(
      await store.retainCancelledFamilyWorkPhoneCall(
        "10000000-0000-4000-8000-000000000006",
        pendingCancelledPhoneCall,
        at(0),
      ),
    ).toBe(true);
    expect(
      await store.adoptFamilyWorkPhoneCall("10000000-0000-4000-8000-000000000006", cancelledPhoneCall, at(0)),
    ).toBe(true);
    expect(await store.takeCancelledFamilyWorkResources("10000000-0000-4000-8000-000000000006")).toEqual({
      browserSession: null,
      activePhoneCall: cancelledPhoneCall,
    });
    expect(await store.readNextDueProactiveWork(at(0))).toEqual({
      kind: "cancelled_family_task",
      workId: "10000000-0000-4000-8000-000000000006",
      activePhoneCall: cancelledPhoneCall,
    });
    expect(
      await store.retryCancelledFamilyWorkPhoneCall(
        "10000000-0000-4000-8000-000000000006",
        cancelledPhoneCall,
        at(1),
        "The provider call is still stopping",
      ),
    ).toBe(true);
    expect(await store.readNextDueProactiveWork(at(1))).toEqual({
      kind: "cancelled_family_task",
      workId: "10000000-0000-4000-8000-000000000006",
      activePhoneCall: cancelledPhoneCall,
    });
    expect(
      await store.clearCancelledFamilyWorkPhoneCall(
        "10000000-0000-4000-8000-000000000006",
        cancelledPhoneCall,
      ),
    ).toBe(true);
    await assertDatabase(
      "Clearing a cancelled family-task phone call retained its cleanup lease",
      `exists (
        select 1 from proactive_work
        where id='10000000-0000-4000-8000-000000000006'::uuid
          and status='cancelled' and next_check_at is null
          and task_state->'activePhoneCall'='null'::jsonb
      )`,
    );
    await writeFile(
      assertionFile,
      `update proactive_work set next_check_at=${sqlLiteral(at(2))}
        where id='10000000-0000-4000-8000-000000000006'::uuid;`,
    );
    await expect(migrateDatabase(databaseUrl, assertionFile)).rejects.toThrow(
      /proactive_work_due_state_check/,
    );

    const reminder = await store.readNextDueProactiveWork(at(0));
    expect(reminder).toEqual({ kind: "reminder", workId: "10000000-0000-4000-8000-000000000005" });
    if (reminder?.kind !== "reminder") throw new Error("The earlier reminder was not due first");
    await store.fireDueReminder({ workId: reminder.workId, occurredAt: at(0) });
    const reminderDue = await store.readNextOutbound(at(0));
    if (!reminderDue) throw new Error("The earlier reminder was not staged first");
    const reminderOutbound = await store.beginOutbound({ sourceId: reminderDue.sourceId, now: at(0) });
    if (!reminderOutbound) throw new Error("The reminder outbound was unavailable");
    await store.completeOutbound({
      sourceId: reminderOutbound.sourceId,
      providerMessageId: "provider-reminder",
      sentAt: at(1),
    });

    const first = await store.readNextDueProactiveWork(at(2));
    if (first?.kind !== "family_task") throw new Error("Family work was not claimed");
    expect(first.initiatingAdultId).toBe("10000000-0000-4000-8000-000000000002");
    expect(first.origin).toMatchObject({
      message: {
        sourceId: "10000000-0000-4000-8000-000000000007",
        moveKind: "reply",
        authoredText: "Please use this revised request.",
        voiceTranscriptPresent: true,
        replyToSourceId: "10000000-0000-4000-8000-000000000010",
        images: [
          {
            assetId: "10000000-0000-4000-8000-000000000012",
            mimeType: "image/jpeg",
          },
        ],
      },
      supersededMessages: [
        expect.objectContaining({
          sourceId: "10000000-0000-4000-8000-000000000009",
          text: "Prepare the original camp form.",
        }),
      ],
      replyTarget: expect.objectContaining({
        sourceId: "10000000-0000-4000-8000-000000000010",
        speaker: "florence",
        text: "Which camp form should I use?",
        images: [
          {
            assetId: "10000000-0000-4000-8000-000000000016",
            mimeType: "image/png",
          },
        ],
      }),
      currentDocuments: [
        expect.objectContaining({
          id: "10000000-0000-4000-8000-000000000011",
          parentSourceId: "10000000-0000-4000-8000-000000000007",
          filename: "revised-camp-form.pdf",
          contentDigest: "a".repeat(64),
        }),
        expect.objectContaining({
          id: "10000000-0000-4000-8000-000000000015",
          parentSourceId: "10000000-0000-4000-8000-000000000010",
          filename: "original-camp-form.pdf",
          contentDigest: "b".repeat(64),
        }),
      ],
    });
    expect(first.origin.currentDocuments[0]?.contentEnvelope).toEqual(Uint8Array.from([1, 2, 3]));
    expect(first.origin.currentDocuments[1]?.contentEnvelope).toEqual(Uint8Array.from([4, 5, 6]));
    const plannedState = {
      ...first.state,
      phase: "tool_pending" as const,
      claim: null,
      continuationItems: [
        {
          type: "function_call",
          call_id: "camp-portal",
          name: "browser_work",
          arguments: JSON.stringify({ operation: "navigate", url: "https://camp.example/register" }),
          status: "completed",
        },
      ],
      pendingCall: {
        callId: "camp-portal",
        name: "browser_work",
        argumentsJson: JSON.stringify({
          operation: "navigate",
          url: "https://camp.example/register",
        }),
        attempt: 0,
      },
    };
    expect(
      await store.settleFamilyWorkClaim({
        workId: first.workId,
        generation: first.generation,
        claimId: first.claimId,
        settledAt: at(10),
        result: { type: "continue", state: plannedState, nextCheckAt: at(100) },
      }),
    ).toBe("settled");
    expect(await store.readNextDueProactiveWork(at(99))).toBeNull();

    await store.close();
    store = new PostgresFlorenceStore(databaseUrl);
    const interrupted = await store.readNextDueProactiveWork(at(100));
    if (interrupted?.kind !== "family_task") {
      throw new Error("Persisted tool-pending work did not survive restart");
    }
    expect(interrupted.state.phase).toBe("tool_pending");
    expect(interrupted.state.pendingCall?.attempt).toBe(1);
    const takeover = await store.readNextDueProactiveWork(at(120_101));
    if (takeover?.kind !== "family_task") throw new Error("Expired claim was not recovered");
    expect(takeover.claimId).not.toBe(interrupted.claimId);
    expect(takeover.state.pendingCall?.attempt).toBe(2);
    expect(
      await store.settleFamilyWorkClaim({
        workId: interrupted.workId,
        generation: interrupted.generation,
        claimId: interrupted.claimId,
        settledAt: at(200),
        result: {
          type: "continue",
          state: { ...interrupted.state, claim: null },
          nextCheckAt: at(300),
        },
      }),
    ).toBe("stale");

    const completedCallState = {
      ...takeover.state,
      phase: "ready" as const,
      claim: null,
      browserSession: {
        sessionId: "browserbase-active-session",
        expiresAt: at(3_600_000),
      },
      continuationItems: [
        ...takeover.state.continuationItems,
        {
          type: "function_call_output",
          call_id: "camp-portal",
          output: '{"title":"Camp registration","snapshot":"Review registration"}',
        },
      ],
      pendingCall: null,
      progressRevision: 1,
    };
    expect(
      await store.settleFamilyWorkClaim({
        workId: takeover.workId,
        generation: takeover.generation,
        claimId: takeover.claimId,
        settledAt: at(120_200),
        result: {
          type: "continue",
          state: completedCallState,
          nextCheckAt: at(120_201),
          progressText: "I opened the camp portal and I’m preparing the registration now.",
        },
      }),
    ).toBe("settled");
    const progressDue = await store.readNextOutbound(at(120_200));
    if (!progressDue) throw new Error("Transactional progress outbound was not staged");
    const progress = await store.beginOutbound({ sourceId: progressDue.sourceId, now: at(120_200) });
    if (!progress || !(await store.outboundSendIsCurrent(progress.sourceId))) {
      throw new Error("Progress outbound was not current");
    }
    expect(progress).toMatchObject({
      moveKind: "reply",
      replyToProviderMessageId: "family-work-steering-message",
    });
    await store.completeOutbound({
      sourceId: progress.sourceId,
      providerMessageId: "provider-progress",
      sentAt: at(120_201),
    });

    const finalClaim = await store.readNextDueProactiveWork(at(120_201));
    if (finalClaim?.kind !== "family_task") throw new Error("Final work was not claimed");
    expect(finalClaim.state.browserSession).toEqual(completedCallState.browserSession);
    const terminalText = "The camp registration is filled and ready on the final review page.";
    const terminalState = {
      ...finalClaim.state,
      phase: "terminal" as const,
      claim: null,
      browserSession: null,
      continuationItems: [],
      pendingCall: null,
      progressRevision: 2,
      terminal: { outcome: "succeeded" as const, text: terminalText },
    };
    expect(
      await store.settleFamilyWorkClaim({
        workId: finalClaim.workId,
        generation: finalClaim.generation,
        claimId: finalClaim.claimId,
        settledAt: at(120_300),
        result: { type: "terminal", state: terminalState, terminalText },
      }),
    ).toBe("settled");
    await assertDatabase(
      "Family work completed before its terminal receipt",
      `exists (select 1 from proactive_work where id='10000000-0000-4000-8000-000000000003'
        and status='delivering' and task_state->>'phase'='terminal')`,
    );
    const terminalDue = await store.readNextOutbound(at(120_300));
    if (!terminalDue) throw new Error("Transactional terminal outbound was not staged");
    const terminal = await store.beginOutbound({ sourceId: terminalDue.sourceId, now: at(120_300) });
    if (!terminal || !(await store.outboundSendIsCurrent(terminal.sourceId))) {
      throw new Error("Terminal outbound was not current");
    }
    expect(terminal).toMatchObject({
      moveKind: "reply",
      replyToProviderMessageId: "family-work-steering-message",
    });
    await store.completeOutbound({
      sourceId: terminal.sourceId,
      providerMessageId: "provider-terminal",
      sentAt: at(120_301),
    });
    await assertDatabase(
      "Family work did not complete exactly once after its terminal receipt",
      `exists (select 1 from proactive_work where id='10000000-0000-4000-8000-000000000003'
        and status='completed' and task_state->>'progressRevision'='2')
       and (select count(*)=2 from messages message join sources source on source.id=message.source_id
        where source.metadata->>'familyWorkId'='10000000-0000-4000-8000-000000000003'
          and message.status='sent'
          and message.reply_to_source_id='10000000-0000-4000-8000-000000000017'::uuid
          and source.parent_source_id='10000000-0000-4000-8000-000000000017'::uuid)`,
    );
    expect(await store.readNextOutbound(at(120_400))).toBeNull();
  });
});

release("Florence parent journeys", () => {
  test("retains every cross-page source supporting one initial Google fact", async () => {
    const harness = await createHarness(async () => decision());
    harness.state.completeFactSupportExercise = true;

    await harness.readyHousehold();

    expect(
      harness.state.baselinePageReads
        .filter(
          (read) =>
            read.kind === "gmail" &&
            read.ownerAdultId === harness.founderAdultId &&
            read.connectionId === FOUNDER_GOOGLE,
        )
        .map((read) => read.pageToken),
    ).toEqual([null, "gmail-baseline-page-2"]);
    await harness.assertDatabase(
      "The initial Google fact lost cross-page supporting sources",
      `(select count(*)=12 from facts fact
        join fact_sources link on link.fact_id=fact.id
        join sources source on source.id=link.source_id
        where fact.household_id=(select household_id from people
          where id=${sqlLiteral(harness.founderAdultId)}::uuid)
          and fact.slot=${sqlLiteral(SHARED_SCHOOL_CONTACT_SLOT)}
          and source.owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and source.kind='gmail' and source.visibility='private')`,
    );
  }, 20_000);

  test("keeps durable Google memory after its source ages beyond a later review window", async () => {
    const harness = await createHarness(async () => decision());
    harness.state.retainedFactBeyondReviewWindowExercise = true;
    await harness.readyHousehold();

    const beforeRescan = await harness.florence.workspaceForAdult(harness.founderAdultId);
    const retainedBeforeRescan = beforeRescan.vault?.facts.find(
      (fact) => fact.statement === INITIAL_PRIVATE_SCHOOL_FACT,
    );
    if (!retainedBeforeRescan) throw new Error("The initial Google review did not retain its durable fact");

    harness.state.now += 91 * 24 * 60 * 60_000;
    await harness.activateGoogle(
      harness.founderAdultId,
      RECONNECTED_FOUNDER_GOOGLE,
      "founder-aged-memory-rescan-state",
    );
    await harness.drain();

    const afterRescan = await harness.florence.workspaceForAdult(harness.founderAdultId);
    expect(
      afterRescan.vault?.facts.find((fact) => fact.statement === INITIAL_PRIVATE_SCHOOL_FACT),
    ).toMatchObject({
      id: retainedBeforeRescan.id,
      visibility: "household",
      source: { kind: "gmail" },
    });
    await harness.assertDatabase(
      "A later 90-day discovery window expired durable Google memory",
      `exists (
          select 1 from proactive_work
          where kind='initial_private_review' and status='completed'
            and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
        ) and exists (
          select 1 from facts fact
          join fact_sources link on link.fact_id=fact.id
          join sources source on source.id=link.source_id
          where fact.id=${sqlLiteral(retainedBeforeRescan.id)}::uuid
            and fact.slot=${sqlLiteral(PRIVATE_SCHOOL_FACT_SLOT)}
            and source.owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
            and source.kind='gmail'
            and source.metadata->>'connectionId'=${sqlLiteral(FOUNDER_GOOGLE)}
            and (source.metadata->>'sentAt')::timestamptz
              < ${sqlLiteral(new Date(harness.state.now - 90 * 24 * 60 * 60_000).toISOString())}::timestamptz
        ) and not exists (
          select 1 from sources
          where kind='gmail'
            and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
            and metadata->>'connectionId'=${sqlLiteral(RECONNECTED_FOUNDER_GOOGLE)}
            and metadata->>'messageId'=${sqlLiteral(SCHOOL_ATTACHMENT.messageId)}
        )`,
    );
  }, 20_000);

  test("keeps every active finite monitor available to Google review and parent control", async () => {
    const objectives = Array.from({ length: 21 }, (_, index) => `Complete monitor ${index + 1}`);
    let foregroundObjectives: readonly string[] = [];
    const harness = await createHarness(async (input) => {
      const requestedObjective = objectives.find(
        (objective) => input.currentMessage.text === `Watch ${objective.toLocaleLowerCase()}`,
      );
      if (requestedObjective) {
        return decision({
          bubbles: [{ text: `I’ll watch ${requestedObjective.toLocaleLowerCase()}.`, delayMs: 0 }],
          followUp: {
            operation: "schedule",
            followUpId: null,
            objective: requestedObjective,
            currentConclusion: `${requestedObjective} is still unresolved.`,
            endCondition: `${requestedObjective} is resolved.`,
            nextCheck: "2026-12-01T18:00:00.000Z",
            why: "The parent asked Florence to keep watching.",
            sourceIds: [input.currentMessage.sourceId],
          },
        });
      }
      if (input.currentMessage.text === "Stop watching complete monitor 21") {
        const parsed = florenceReasonerInputSchema.parse(input);
        foregroundObjectives = parsed.pendingFollowUps.map((followUp) => followUp.objective);
        const target = parsed.pendingFollowUps.find((followUp) => followUp.objective === objectives.at(-1));
        if (!target) throw new Error("The last active monitor was unavailable to the parent turn");
        return decision({
          bubbles: [{ text: "Okay—I stopped watching that.", delayMs: 0 }],
          followUp: {
            operation: "cancel",
            followUpId: target.followUpId,
            objective: null,
            currentConclusion: null,
            endCondition: null,
            nextCheck: null,
            why: null,
            sourceIds: [input.currentMessage.sourceId],
          },
        });
      }
      return decision();
    });
    await harness.readyHousehold();

    for (const [index, objective] of objectives.entries()) {
      await harness.accept(
        "private",
        `complete-monitor-${index + 1}`,
        `Watch ${objective.toLocaleLowerCase()}`,
      );
      await harness.drain();
    }
    await harness.assertDatabase(
      "The household did not retain all active finite monitors",
      `(select count(*)=21 from proactive_work
        where kind='finite_monitor' and status='active'
          and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and objective like 'Complete monitor %')`,
    );

    const assessmentsBeforeChange = harness.state.googleAssessments.length;
    harness.state.privateFactUpdatePending = true;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    const assessment = harness.state.googleAssessments
      .slice(assessmentsBeforeChange)
      .findLast((candidate) => candidate.adult.adultId === harness.founderAdultId);
    if (!assessment) throw new Error("The incremental Google review did not run");
    const parsedAssessment = florenceGoogleChangesAssessmentInputSchema.parse(assessment);
    const reviewedObjectives = parsedAssessment.activeMonitors
      .map((monitor) => monitor.objective)
      .filter((objective) => objective.startsWith("Complete monitor "));
    expect(new Set(reviewedObjectives)).toEqual(new Set(objectives));

    await harness.accept("private", "cancel-complete-monitor-21", "Stop watching complete monitor 21");
    await harness.drain();
    expect(
      new Set(foregroundObjectives.filter((objective) => objective.startsWith("Complete monitor "))),
    ).toEqual(new Set(objectives));
    await harness.assertDatabase(
      "The parent could not cancel the active monitor beyond the former presentation boundary",
      `not exists (
          select 1 from proactive_work
          where kind='finite_monitor' and status='active'
            and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
            and objective=${sqlLiteral(objectives.at(-1) ?? "")}
        ) and (select count(*)=20 from proactive_work
          where kind='finite_monitor' and status='active'
            and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
            and objective like 'Complete monitor %')`,
    );
  }, 30_000);

  test("keeps every parent's and family-calendar conflict in proactive availability", async () => {
    const researchReasoner = new FlorenceReasoner({ apiKey: "test-openai-key", model: "test-model" }, {
      responses: {
        parse: () => ({
          output_parsed: {
            judgment: "recommend",
            summary: INTEREST_RECOMMENDATION,
            urls: [INTEREST_URL],
          },
          output: [
            {
              id: "interest-search",
              type: "web_search_call",
              status: "completed",
              action: {
                type: "search",
                query: "family soccer match",
                sources: [{ type: "url", url: INTEREST_URL }],
              },
            },
          ],
        }),
      },
    } as never);
    const harness = await createHarness(
      async (input) =>
        input.currentMessage.text === INTEREST_REQUEST
          ? decision({
              bubbles: [
                {
                  text: "I’ll keep an eye out and only bring you something genuinely useful.",
                  delayMs: 0,
                },
              ],
              interest: {
                operation: "create",
                interestWorkId: null,
                genericTerms: ["family soccer matches"],
                objective: "Find a worthwhile local soccer outing for the family.",
                why: "Maya likes soccer and the family asked Florence to keep watch.",
                sourceIds: [input.currentMessage.sourceId],
              },
            })
          : decision(),
      { researchInterest: researchReasoner.researchInterest.bind(researchReasoner) },
    );
    await harness.readyHousehold();
    await harness.accept("group", "busy-union-interest", INTEREST_REQUEST, "partner");
    await harness.drain();

    harness.state.interestBusyUnionExercise = true;
    harness.state.now += 6 * 60_000;
    await harness.drain();

    expect(harness.state.interestResearchInputs).toHaveLength(1);
    const researchInput = harness.state.interestResearchInputs[0];
    if (!researchInput) throw new Error("The interest monitor did not receive family availability");
    const currentTime = Date.parse(researchInput.currentTime);
    expect(researchInput.busyIntervals).toHaveLength(52);
    expect(researchInput.busyIntervals[0]).toEqual({
      startsAt: new Date(currentTime + 60 * 60_000).toISOString(),
      endsAt: new Date(currentTime + 4 * 60 * 60_000).toISOString(),
    });
    expect(researchInput.busyIntervals.at(-1)).toEqual({
      startsAt: new Date(currentTime + 405 * 60 * 60_000).toISOString(),
      endsAt: new Date(currentTime + 406 * 60 * 60_000).toISOString(),
    });
    expect(
      researchInput.busyIntervals.every(
        (interval, index) =>
          index === 0 ||
          Date.parse(researchInput.busyIntervals[index - 1]?.endsAt ?? "") < Date.parse(interval.startsAt),
      ),
    ).toBe(true);
  }, 20_000);

  test("keeps every visible Vault fact and the full tail of a stored artifact searchable", async () => {
    const saveRequest = "Save our complete weeknight noodles recipe.";
    const tailDetail = "Finish with preserved-lemon gremolata after plating.";
    const recipeDetails =
      "Preparation note: keep the sauce warm, the noodles loose, and reserve a little cooking water. ".repeat(
        120,
      ) + tailDetail;
    const recallRequest = "What was the preserved-lemon gremolata finish in our noodles recipe?";
    let fullArtifactWasSearchable = false;
    const harness = await createHarness(async (input, reads) => {
      if (input.currentMessage.text === saveRequest) {
        return decision({
          facts: [
            {
              operation: "remember",
              factId: null,
              statement: "The family has a complete weeknight noodles recipe.",
              visibility: "household",
              memory: {
                memoryKind: "artifact",
                artifactKind: "recipe",
                title: "Complete weeknight noodles",
                details: recipeDetails,
                tags: ["recipe", "noodles"],
              },
              sourceIds: [input.currentMessage.sourceId],
            },
          ],
        });
      }
      if (input.currentMessage.text === recallRequest) {
        const matches = await reads.searchFamilyMemory?.({
          query: "preserved lemon gremolata plating",
          limit: 5,
        });
        fullArtifactWasSearchable =
          matches?.some((source) => source.kind === "memory" && source.text.includes(tailDetail)) ?? false;
        return decision({
          bubbles: [{ text: "Finish it with preserved-lemon gremolata after plating.", delayMs: 0 }],
        });
      }
      return decision();
    });
    await harness.readyHousehold();

    await harness.accept("private", "save-complete-recipe", saveRequest);
    await harness.drain();

    const householdId = (await harness.store.listHouseholdIdsForAdult(harness.founderAdultId))[0];
    if (!householdId) throw new Error("The founder household was unavailable");
    await writeFile(
      harness.assertionFile,
      `insert into facts (id,household_id,kind,slot,label,value,visibility,owner_adult_id)
       select ('70000000-0000-4000-8000-' || lpad(ordinal::text,12,'0'))::uuid,
         ${sqlLiteral(householdId)}::uuid,'general','regression:visible-vault-fact:' || ordinal,
         'Visible Vault fact ' || ordinal,
         jsonb_build_object(
           'statement','Visible Vault fact ' || ordinal,
           'memoryKind','fact','artifactKind',null,'title',null,'details',null,'tags',jsonb_build_array()
         ),'household',null
       from generate_series(1,501) ordinal;`,
    );
    await migrateDatabase(harness.databaseUrl, harness.assertionFile);

    const storedHousehold = await harness.store.readHousehold({
      householdId,
      viewerAdultId: harness.founderAdultId,
    });
    expect(
      storedHousehold?.facts.filter((fact) => fact.slot.startsWith("regression:visible-vault-fact:")),
    ).toHaveLength(501);
    const workspace = await harness.florence.workspaceForAdult(harness.founderAdultId);
    expect(
      workspace.vault?.facts.filter((fact) => fact.statement.startsWith("Visible Vault fact ")),
    ).toHaveLength(501);
    expect(workspace.vault?.facts.find((fact) => fact.title === "Complete weeknight noodles")?.details).toBe(
      recipeDetails,
    );

    await harness.accept("private", "recall-complete-recipe", recallRequest, "partner");
    await harness.drain();
    expect(fullArtifactWasSearchable).toBe(true);
  }, 30_000);

  test("delivers model-selected native mention and poll moves through the persisted outbox", async () => {
    const request = "Can you ask Alex which dinner night works?";
    let mentionedName = "";
    const harness = await createHarness(async (input) => {
      if (input.currentMessage.text !== request) {
        return decision({ bubbles: [{ text: "Okay.", delayMs: 0 }] });
      }
      mentionedName =
        input.household.adultNames.find((name) => name !== input.currentMessage.senderName) ?? "Alex";
      return decision({
        nativeMoves: [
          {
            type: "mention",
            text: `${mentionedName}, which night works for dinner?`,
            adultDisplayName: mentionedName,
          },
          {
            type: "poll",
            question: "Pick a night",
            options: ["Tuesday", "Thursday"],
          },
        ],
      });
    });
    await harness.readyHousehold();

    await harness.accept("group", "native-mention-poll", request);
    await harness.drain();

    expect(harness.linq.moves).toHaveLength(3);
    expect(harness.linq.moves.map((move) => move.move.type)).toEqual(["message", "message", "poll"]);
    expect(harness.linq.moves[0]?.move).toEqual({
      type: "message",
      parts: [
        {
          type: "text",
          text: `${mentionedName}, which night works for dinner?`,
          mention: { handle: PARTNER_PHONE, range: [0, mentionedName.length] },
        },
      ],
    });
    expect(harness.linq.moves[1]?.move).toEqual({
      type: "message",
      parts: [{ type: "text", text: "Pick a night" }],
    });
    expect(harness.linq.moves[2]?.move).toEqual({
      type: "poll",
      options: ["Tuesday", "Thursday"],
    });
  }, 20_000);

  test("keeps private setup useful and recovers one two-parent family loop without duplicate work", async () => {
    let accessFollowUpHistory: readonly string[] = [];
    let accessFollowUpAuthoredText: string | null = null;
    let failNextReinviteConversation = false;
    let customizeNextSuccessfulPartnerInvitation = false;
    let failNextGroupGreeting = false;
    let transientRetryAttempts = 0;
    let retryCueLeakedIntoConversation = false;
    let activeReminderListSeen = false;
    let reminderBeyondFormerBoundaryCancelled = false;
    let pausedReminderListSeen = false;
    const harness = await createHarness(async (input) => {
      if (input.currentMessage.text === TRANSIENT_RETRY_REQUEST) {
        transientRetryAttempts += 1;
        if (transientRetryAttempts === 1) {
          throw new FlorenceReasonerError("transient", "Fake temporary model failure");
        }
        retryCueLeakedIntoConversation = input.recentMessages.some(
          (message) => message.text === TRANSIENT_RETRY_CUE,
        );
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
      if (customizeNextSuccessfulPartnerInvitation && input.currentMessage.text === REINVITE_APPROVAL) {
        customizeNextSuccessfulPartnerInvitation = false;
        return decision({
          reaction: "emphasize",
          bubbles: [{ text: PARTNER_INVITATION_CONTRADICTION, delayMs: 0 }],
        });
      }
      if (input.currentMessage.text === INVITE_APPROVAL || input.currentMessage.text === REINVITE_APPROVAL) {
        return decision({
          bubbles: [{ text: PARTNER_INVITATION_CONTRADICTION, delayMs: 0 }],
        });
      }
      if (input.currentMessage.text === NO_RETENTION_OR_SCHEDULING_REQUEST) {
        return {
          ...decision({
            bubbles: [{ text: NO_RETENTION_OR_SCHEDULING_REPLY, delayMs: 0 }],
            facts: [
              remember(
                NO_RETENTION_OR_SCHEDULING_FACT,
                input.currentMessage.sourceId,
                input.audience === "group" ? "household" : "private",
              ),
            ],
            followUp: {
              operation: "schedule",
              followUpId: null,
              objective: "Watch for use of the temporary code.",
              currentConclusion: "The temporary code has not been used.",
              endCondition: "The temporary code is used.",
              nextCheck: "2026-08-20T19:00:00.000Z",
              why: "The temporary code may need follow-through.",
              sourceIds: [input.currentMessage.sourceId],
            },
          }),
          policy: { retain: false, schedule: false, stopMessaging: false },
        };
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
          reminder: {
            operation: "create",
            reminderId: null,
            action: ONE_SHOT_REMINDER_ACTION,
            schedule: { kind: "once", at: ONE_SHOT_REMINDER_AT },
          },
        });
      }
      const stretchReminder = input.visibleReminders.find(
        (reminder) => reminder.action === RECURRING_REMINDER_ACTION,
      );
      if (input.currentMessage.text === RECURRING_REMINDER_REQUEST) {
        return decision({
          bubbles: [{ text: RECURRING_REMINDER_ACK, delayMs: 0 }],
          reminder: {
            operation: "create",
            reminderId: null,
            action: RECURRING_REMINDER_ACTION,
            schedule: {
              kind: "interval",
              everyMinutes: 10,
              anchorAt: RECURRING_REMINDER_ANCHOR,
            },
          },
        });
      }
      if (input.currentMessage.text === LIST_REMINDERS_REQUEST) {
        activeReminderListSeen = stretchReminder?.status === "active";
        return decision({
          bubbles: [{ text: LIST_REMINDERS_REPLY, delayMs: 0 }],
          reminder: {
            operation: "list",
            reminderId: null,
            action: null,
            schedule: null,
          },
        });
      }
      if (input.currentMessage.text === UPDATE_REMINDER_REQUEST && stretchReminder) {
        return decision({
          bubbles: [{ text: UPDATE_REMINDER_ACK, delayMs: 0 }],
          reminder: {
            operation: "update",
            reminderId: stretchReminder.reminderId,
            action: null,
            schedule: {
              kind: "interval",
              everyMinutes: 5,
              anchorAt: UPDATED_RECURRING_REMINDER_ANCHOR,
            },
          },
        });
      }
      if (input.currentMessage.text === PAUSE_REMINDER_REQUEST && stretchReminder) {
        return decision({
          bubbles: [{ text: PAUSE_REMINDER_ACK, delayMs: 0 }],
          reminder: {
            operation: "pause",
            reminderId: stretchReminder.reminderId,
            action: null,
            schedule: null,
          },
        });
      }
      if (input.currentMessage.text === LIST_PAUSED_REMINDERS_REQUEST) {
        pausedReminderListSeen = stretchReminder?.status === "paused";
        return decision({
          bubbles: [{ text: LIST_PAUSED_REMINDERS_REPLY, delayMs: 0 }],
          reminder: {
            operation: "list",
            reminderId: null,
            action: null,
            schedule: null,
          },
        });
      }
      if (input.currentMessage.text === RESUME_REMINDER_REQUEST && stretchReminder) {
        return decision({
          bubbles: [{ text: RESUME_REMINDER_ACK, delayMs: 0 }],
          reminder: {
            operation: "resume",
            reminderId: stretchReminder.reminderId,
            action: null,
            schedule: null,
          },
        });
      }
      if (input.currentMessage.text === RUN_REMINDER_REQUEST && stretchReminder) {
        return decision({
          reaction: "like",
          bubbles: [],
          reminder: {
            operation: "run",
            reminderId: stretchReminder.reminderId,
            action: null,
            schedule: null,
          },
        });
      }
      if (input.currentMessage.text === CANCEL_REMINDER_REQUEST && stretchReminder) {
        reminderBeyondFormerBoundaryCancelled = input.visibleReminders.length > 100;
        return decision({
          bubbles: [{ text: CANCEL_REMINDER_ACK, delayMs: 0 }],
          reminder: {
            operation: "cancel",
            reminderId: stretchReminder.reminderId,
            action: null,
            schedule: null,
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
      PARTNER_INVITATION_APPROVAL_REPLY,
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
    const reactionsBeforeFailedReinvite = harness.linq.reactions.length;
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
    expect(harness.linq.reactions.slice(reactionsBeforeFailedReinvite)).toEqual([]);
    expect(
      harness.linq.messages
        .filter((message) => message.providerConversationId === PRIVATE_FOUNDER)
        .slice(founderMessagesBeforeReinvite)
        .map((message) => message.text),
    ).toContain(PARTNER_REINVITATION_APPROVAL_REPLY);
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

    customizeNextSuccessfulPartnerInvitation = true;
    const founderMessagesBeforeSuccessfulReinvite = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_FOUNDER,
    ).length;
    const reactionsBeforeSuccessfulReinvite = harness.linq.reactions.length;
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
      PARTNER_REINVITATION_APPROVAL_REPLY,
    ]);
    expect(harness.linq.reactions.slice(reactionsBeforeSuccessfulReinvite)).toEqual([]);
    const reinviteFounderMessages = harness.linq.messages
      .filter((message) => message.providerConversationId === PRIVATE_FOUNDER)
      .slice(founderMessagesBeforeReinvite);
    expect(reinviteFounderMessages.map((message) => message.text)).toContain(
      PARTNER_REINVITATION_APPROVAL_REPLY,
    );
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
    const partnerSetupLinksBeforeRefresh = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_PARTNER && message.text.includes("#s="),
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
          message.providerConversationId === PRIVATE_PARTNER && message.text === PARTNER_SETUP_REFRESH_ACK,
      ),
    ).toHaveLength(1);
    expect(harness.linq.partnerSetupLinkAttempts).toBe(linkAttemptsBeforeExpiry + 1);
    const refreshedPartnerSetupToken = harness.setupTokenFor(PRIVATE_PARTNER);
    expect(refreshedPartnerSetupToken).toMatch(/^ps1\./);
    expect(refreshedPartnerSetupToken).not.toBe(expiringPartnerSetupToken);
    await harness.assertDatabase(
      "The confirmed refreshed link forgot its durable original expiry lineage",
      `exists (
        select 1 from people where adult_slot=2 and status='planned'
          and nullif(preferences#>>'{partnerInvitationRefresh,messageId}','') is not null
          and nullif(preferences#>>'{partnerInvitationRefresh,providerEventId}','') is not null
          and preferences#>>'{partnerInvitationRefresh,messageId}'<>invitation_message_id
      )`,
    );
    expect(
      harness.linq.messages
        .filter(
          (message) => message.providerConversationId === PRIVATE_PARTNER && message.text.includes("#s="),
        )
        .slice(partnerSetupLinksBeforeRefresh),
    ).toHaveLength(1);
    await harness.drain();
    const expirationNotices = harness.linq.messages
      .filter((message) => message.providerConversationId === PRIVATE_FOUNDER)
      .slice(founderMessagesBeforeExpiry)
      .filter((message) => message.text === PARTNER_SETUP_EXPIRED_NOTICE);
    expect(expirationNotices).toHaveLength(0);
    expect((await harness.florence.workspaceForAdult(harness.founderAdultId)).workspace.setup).toMatchObject({
      partnerInvitation: "invited",
    });
    harness.state.now += 15_001;
    await harness.drain();
    expect(harness.linq.partnerSetupLinkAttempts).toBe(linkAttemptsBeforeExpiry + 1);

    await harness.receiveParts(
      "partner-declines-refreshed-link",
      [{ type: "text", value: PARTNER_SETUP_REFUSAL }],
      PRIVATE_PARTNER,
      "partner",
    );
    await harness.drain();
    expect((await harness.florence.workspaceForAdult(harness.founderAdultId)).workspace.setup).toMatchObject({
      partnerInvitation: "ready",
    });
    const linkAttemptsBeforeReinvite = harness.linq.partnerSetupLinkAttempts;

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
      expect(harness.linq.partnerSetupLinkAttempts).toBe(linkAttemptsBeforeReinvite);
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
    expect(harness.linq.partnerSetupLinkAttempts).toBe(linkAttemptsBeforeReinvite + 1);
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

    const messagesBeforeNoRetentionRequest = harness.linq.messages.length;
    await harness.accept("private", "no-retention-or-scheduling", NO_RETENTION_OR_SCHEDULING_REQUEST);
    await harness.drain();
    const noRetentionMessages = harness.linq.messages.slice(messagesBeforeNoRetentionRequest);
    expect(noRetentionMessages.map((message) => message.text)).toEqual([NO_RETENTION_OR_SCHEDULING_REPLY]);
    expect(noRetentionMessages.map((message) => message.text).join("\n")).not.toMatch(
      /Vault|retain|schedule|follow-up|calendar change/i,
    );
    await harness.assertDatabase(
      "A no-retention, no-scheduling turn changed durable state behind its natural reply",
      `not exists (
        select 1 from facts where value->>'statement'=${sqlLiteral(NO_RETENTION_OR_SCHEDULING_FACT)}
      ) and not exists (
        select 1 from proactive_work_sources
        where source_id=${sqlLiteral(inboundSourceId("event-no-retention-or-scheduling"))}::uuid
      )`,
    );

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
      "A one-shot reminder did not become one durable, addressable reminder at the exact requested time",
      `(select count(*)=1 from proactive_work_sources link
          join proactive_work work on work.id=link.work_id
          where link.source_id=${sqlLiteral(inboundSourceId("event-one-shot-pickup-reminder"))}::uuid
            and work.kind='reminder' and work.status='active'
            and work.objective=${sqlLiteral(ONE_SHOT_REMINDER_ACTION)}
            and work.next_check_at=${sqlLiteral(ONE_SHOT_REMINDER_AT)}::timestamptz
            and work.reminder_schedule=${sqlLiteral(
              JSON.stringify({ kind: "once", at: ONE_SHOT_REMINDER_AT }),
            )}::jsonb)
        and not exists (
          select 1 from messages
          where direction='outbound' and text=${sqlLiteral(ONE_SHOT_REMINDER_TEXT)}
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
    await harness.assertDatabase(
      "A one-shot reminder was marked complete before its queued delivery succeeded",
      `(select count(*)=1 from proactive_work
        where kind='reminder' and visibility='private' and objective=${sqlLiteral(
          ONE_SHOT_REMINDER_ACTION,
        )} and status='delivering' and next_check_at is null)`,
    );

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
        and (select count(*)=1 from proactive_work_sources link
          join proactive_work work on work.id=link.work_id
          where link.source_id=${sqlLiteral(inboundSourceId("event-one-shot-pickup-reminder"))}::uuid
            and work.kind='reminder' and work.status='completed'
            and work.next_check_at is null and work.last_run_at is not null)`,
    );

    await harness.accept("group", "recurring-stretch-reminder", RECURRING_REMINDER_REQUEST);
    await harness.drain();
    await harness.accept("group", "list-active-reminders", LIST_REMINDERS_REQUEST);
    await harness.drain();
    expect(activeReminderListSeen).toBe(true);
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === FAMILY_GROUP && message.text === LIST_REMINDERS_REPLY,
      ),
    ).toHaveLength(1);

    await harness.accept("group", "update-stretch-reminder", UPDATE_REMINDER_REQUEST);
    await harness.drain();
    await harness.accept("group", "pause-stretch-reminder", PAUSE_REMINDER_REQUEST);
    await harness.drain();
    harness.state.now = Date.parse("2026-08-19T21:48:00.000Z");
    await harness.drain();
    expect(harness.linq.messages.some((message) => message.text === RECURRING_REMINDER_TEXT)).toBe(false);
    await harness.accept("group", "list-paused-reminders", LIST_PAUSED_REMINDERS_REQUEST);
    await harness.drain();
    expect(pausedReminderListSeen).toBe(true);

    await harness.accept("group", "resume-stretch-reminder", RESUME_REMINDER_REQUEST);
    await harness.drain();
    const reactionsBeforeRunNow = harness.linq.reactions.length;
    await harness.accept("group", "run-stretch-reminder", RUN_REMINDER_REQUEST);
    await harness.drain();
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP && message.text === RECURRING_REMINDER_TEXT,
      ),
    ).toHaveLength(1);
    expect(harness.linq.reactions.slice(reactionsBeforeRunNow)).toEqual([
      expect.objectContaining({
        providerConversationId: FAMILY_GROUP,
        targetProviderMessageId: "message-run-stretch-reminder",
        reaction: "like",
      }),
    ]);
    await harness.assertDatabase(
      "Run-now changed the recurring cadence or resumed a different reminder",
      `(select count(*)=1 from proactive_work
        where kind='reminder' and visibility='household' and objective=${sqlLiteral(
          RECURRING_REMINDER_ACTION,
        )} and status='active'
          and reminder_schedule=${sqlLiteral(
            JSON.stringify({
              kind: "interval",
              everyMinutes: 5,
              anchorAt: UPDATED_RECURRING_REMINDER_ANCHOR,
            }),
          )}::jsonb
          and next_check_at='2026-08-19T21:52:00.000Z'::timestamptz)`,
    );

    harness.state.now = Date.parse("2026-08-19T21:58:00.000Z");
    await harness.florence.runOnce();
    await harness.assertDatabase(
      "The due recurring occurrence was not queued before the run-now race",
      `exists (
        select 1 from messages message join sources source on source.id=message.source_id
        where message.direction='outbound' and message.status='pending'
          and message.text=${sqlLiteral(RECURRING_REMINDER_TEXT)}
          and source.metadata->>'reminderId' is not null
      )`,
    );
    await harness.accept("group", "run-due-stretch-reminder", RUN_REMINDER_REQUEST);
    await harness.drain();
    const recurringDeliveries = harness.linq.messages.filter(
      (message) =>
        message.providerConversationId === FAMILY_GROUP && message.text === RECURRING_REMINDER_TEXT,
    );
    expect(recurringDeliveries).toHaveLength(2);
    expect(new Set(recurringDeliveries.map((message) => message.idempotencyKey)).size).toBe(2);
    await harness.assertDatabase(
      "A missed recurring window burst old reminders or failed to fast-forward",
      `(select count(*)=1 from proactive_work
        where kind='reminder' and visibility='household' and objective=${sqlLiteral(
          RECURRING_REMINDER_ACTION,
        )} and status='active'
          and next_check_at='2026-08-19T22:02:00.000Z'::timestamptz)`,
    );

    await writeFile(
      harness.assertionFile,
      `insert into proactive_work (
        id,household_id,kind,visibility,owner_adult_id,objective,reminder_schedule,
        status,next_check_at,created_at
      )
      select ('30000000-0000-4000-8000-'||lpad(fixture.ordinal::text,12,'0'))::uuid,
        ${sqlLiteral(householdId)}::uuid,'reminder','household',null,
        'fixture earlier reminder '||fixture.ordinal,
        jsonb_build_object(
          'kind','once',
          'at',to_char(
            ('2026-08-19T21:59:00.000Z'::timestamptz
              + fixture.ordinal * interval '1 second') at time zone 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
          )
        ),
        'active',
        '2026-08-19T21:59:00.000Z'::timestamptz + fixture.ordinal * interval '1 second',
        '2026-08-19T20:00:00.000Z'::timestamptz + fixture.ordinal * interval '1 millisecond'
      from generate_series(1,100) as fixture(ordinal);`,
    );
    await migrateDatabase(harness.databaseUrl, harness.assertionFile);
    await harness.accept("group", "partner-cancel-stretch-reminder", CANCEL_REMINDER_REQUEST, "partner");
    await harness.drain();
    expect(reminderBeyondFormerBoundaryCancelled).toBe(true);
    await writeFile(
      harness.assertionFile,
      `delete from proactive_work
        where household_id=${sqlLiteral(householdId)}::uuid and kind='reminder'
          and objective like 'fixture earlier reminder %';`,
    );
    await migrateDatabase(harness.databaseUrl, harness.assertionFile);
    harness.state.now = Date.parse("2026-08-19T22:03:00.000Z");
    await harness.drain();
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP && message.text === RECURRING_REMINDER_TEXT,
      ),
    ).toHaveLength(2);
    await harness.assertDatabase(
      "Partner cancellation did not make the shared reminder terminal",
      `(select count(*)=1 from proactive_work
        where kind='reminder' and visibility='household' and objective=${sqlLiteral(
          RECURRING_REMINDER_ACTION,
        )} and status='cancelled' and next_check_at is null)`,
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
    expect(retryCueLeakedIntoConversation).toBe(false);
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
      "A quiet private review lost its private Google corpus or retained a family fact",
      `(select count(*)=2 from proactive_work
          where kind='initial_private_review' and status='completed'
            and briefing_candidates='[]'::jsonb)
        and (select count(*)=4 from sources
          where kind in ('gmail','calendar') and visibility='private'
            and owner_adult_id is not null)
        and (select count(*)=4 from proactive_work_sources link
          join proactive_work work on work.id=link.work_id
          join sources source on source.id=link.source_id
          where work.kind='initial_private_review'
            and work.owner_adult_id=source.owner_adult_id
            and source.kind in ('gmail','calendar') and source.visibility='private')
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

  test("starts one memory-grounded proactive job through the existing family-work loop", async () => {
    let familyWorkRuns = 0;
    const harness = await createHarness(async () => decision(), {
      continueFamilyWork: async (input) => {
        familyWorkRuns += 1;
        expect(input.objective).toBe(PROACTIVE_FAMILY_WORK_OBJECTIVE);
        expect(input.visibility).toBe("household");
        expect(input.initiatingAdultId).toBe(founderSetup().adultId);
        expect(input.origin.message).toMatchObject({
          speaker: "florence",
          text: expect.stringContaining(PROACTIVE_FAMILY_WORK_KICKOFF),
          authoredText: expect.stringContaining(PROACTIVE_FAMILY_WORK_KICKOFF),
          voiceTranscriptPresent: false,
        });
        expect(input.visibleSources?.some((source) => source.text.includes(GOOGLE_RECIPE_TITLE))).toBe(true);
        return {
          kind: "terminal",
          state: {
            ...input.state,
            phase: "terminal",
            claim: null,
            pendingCall: null,
            terminal: { outcome: "succeeded", text: PROACTIVE_FAMILY_WORK_RESULT },
          },
          outcome: "succeeded",
          text: PROACTIVE_FAMILY_WORK_RESULT,
        };
      },
    });
    harness.state.googleRecipeArtifactExercise = true;
    harness.state.proactiveFamilyWorkExercise = true;
    await harness.readyHousehold();

    expect(
      harness.linq.messages.filter((message) => message.text.includes(PROACTIVE_FAMILY_WORK_KICKOFF)),
    ).toHaveLength(1);
    harness.state.now += 1_001;
    await harness.drain();

    expect(familyWorkRuns).toBe(1);
    const proactiveResult = harness.linq.messages.find(
      (message) => message.text === PROACTIVE_FAMILY_WORK_RESULT,
    );
    expect(proactiveResult).toBeDefined();
    expect(proactiveResult?.replyTo).toBeUndefined();
    await harness.assertDatabase(
      "A proactive household judgment did not produce exactly one completed family task from its kickoff",
      `(select count(*)=1 from proactive_work
          where kind='family_task' and objective=${sqlLiteral(PROACTIVE_FAMILY_WORK_OBJECTIVE)}
            and visibility='household' and status='completed')
        and exists (
          select 1 from proactive_work work
          join proactive_work_sources work_source on work_source.work_id=work.id
          join sources source on source.id=work_source.source_id
          join messages message on message.source_id=source.id
          where work.kind='family_task'
            and work.objective=${sqlLiteral(PROACTIVE_FAMILY_WORK_OBJECTIVE)}
            and message.direction='outbound'
            and source.metadata->>'proactiveFamilyWorkId'=work.id::text
        )`,
    );

    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(familyWorkRuns).toBe(1);
    expect(
      harness.linq.messages.filter((message) => message.text.includes(PROACTIVE_FAMILY_WORK_KICKOFF)),
    ).toHaveLength(1);
    expect(
      harness.linq.messages.filter((message) => message.text === PROACTIVE_FAMILY_WORK_RESULT),
    ).toHaveLength(1);
  }, 20_000);

  test("asks the other parent privately, consumes one reply, and finishes in the family thread", async () => {
    const request = "Can you confirm with Alex whether Maya can stay for the whole field-trip day?";
    const objective = "Confirm with Alex whether Maya can stay for the whole field-trip day.";
    const question = "Can Maya stay for the whole field-trip day on Friday?";
    const answer = "No—she needs to come home right after lunch.";
    const unrelated = "Also, can you add milk to the grocery list?";
    const laterUnrelated = "And please add eggs too.";
    const acknowledgement = "Got it—thanks for letting me know. I’ll update the plan.";
    const unrelatedReply = "Sure—I’ll help with the grocery list separately.";
    const terminalText = "Alex said Maya needs to come home right after lunch, so I updated the plan.";
    let ordinaryReasonerSawParticipantReply = false;
    let ordinaryReasonerUnrelatedMessages = 0;
    let durableRuns = 0;
    const harness = await createHarness(
      async (input) => {
        if (input.currentMessage.text === request) {
          return decision({
            bubbles: [{ text: "I’ll check with Alex and close the loop here.", delayMs: 0 }],
            familyWork: {
              operation: "create",
              workId: null,
              objective,
              instruction: null,
              schedule: null,
              candidateIds: [],
            },
          });
        }
        if (input.currentMessage.text === answer) ordinaryReasonerSawParticipantReply = true;
        if (input.currentMessage.text === unrelated || input.currentMessage.text === laterUnrelated) {
          ordinaryReasonerUnrelatedMessages += 1;
          return decision({ bubbles: [{ text: unrelatedReply, delayMs: 0 }] });
        }
        return decision();
      },
      {
        interpretParticipantReply: async (input) => {
          expect(input.pendingRequest).toMatchObject({
            targetAdultName: "Alex Anbarasu",
            question,
            taskObjective: objective,
          });
          if (input.currentMessage.text === unrelated) {
            return { belongsToRequest: false, acknowledgement: null };
          }
          expect(input.currentMessage).toMatchObject({
            text: answer,
            explicitlyRepliesToQuestion: false,
          });
          return {
            belongsToRequest: true,
            acknowledgement: { kind: "text", text: acknowledgement },
          };
        },
        continueFamilyWork: async (input, reads) => {
          durableRuns += 1;
          expect(input.visibility).toBe("household");
          expect(input.ownerAdultId).toBeNull();
          if (input.state.steering.some((steering) => steering.text === answer)) {
            return {
              kind: "terminal",
              state: {
                ...input.state,
                phase: "terminal",
                claim: null,
                pendingCall: null,
                pendingParticipantRequest: null,
                waitingDocket: null,
                terminal: { outcome: "succeeded", text: terminalText },
              },
              outcome: "succeeded",
              text: terminalText,
            };
          }
          if (input.state.phase === "ready") {
            return {
              kind: "continue",
              state: {
                ...input.state,
                phase: "tool_pending",
                claim: null,
                pendingCall: {
                  callId: "ask-alex-about-field-trip",
                  name: "participant_request",
                  argumentsJson: JSON.stringify({
                    targetAdultName: "Alex Anbarasu",
                    question,
                  }),
                  attempt: 0,
                },
              },
              progressText: null,
              nextCheckDelayMs: 0,
            };
          }
          if (!reads.runParticipantRequest) {
            throw new Error("Household work did not receive private participant messaging");
          }
          const queued = await reads.runParticipantRequest({
            targetAdultName: "Alex Anbarasu",
            question,
          });
          return {
            kind: "participant_waiting",
            state: {
              ...input.state,
              phase: "waiting",
              claim: null,
              pendingCall: null,
              pendingParticipantRequest: queued,
              waitingDocket: {
                owner: "Alex Anbarasu",
                nextAction: "Answer Florence's private question.",
                waitingOn: question,
                needsAnswer: true,
              },
            },
          };
        },
      },
    );
    await harness.readyHousehold();

    await harness.accept("group", "participant-request-start", request);
    await harness.drain();
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === PRIVATE_PARTNER && message.text === question,
      ),
    ).toHaveLength(1);
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === FAMILY_GROUP && message.text === question,
      ),
    ).toHaveLength(0);

    await harness.accept("private", "participant-request-unrelated", unrelated, "partner");
    await harness.drain();
    expect(ordinaryReasonerUnrelatedMessages).toBe(1);
    expect(durableRuns).toBe(2);

    // The answer and a separate request arrive as one quick burst. The first
    // Message must still resume the correlated task; the second stays an
    // ordinary private turn rather than being swallowed by that request.
    await harness.accept("private", "participant-request-answer", answer, "partner");
    await harness.accept("private", "participant-request-later-unrelated", laterUnrelated, "partner");
    await harness.drain();
    expect(ordinaryReasonerSawParticipantReply).toBe(false);
    expect(ordinaryReasonerUnrelatedMessages).toBe(2);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === PRIVATE_PARTNER &&
          message.replyTo?.providerMessageId === "message-participant-request-answer" &&
          message.text === acknowledgement,
      ),
    ).toHaveLength(1);
    expect(
      harness.linq.reactions.filter(
        (reaction) => reaction.targetProviderMessageId === "message-participant-request-answer",
      ),
    ).toHaveLength(0);
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === PRIVATE_PARTNER && message.text === unrelatedReply,
      ),
    ).toHaveLength(2);
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === FAMILY_GROUP && message.text === terminalText,
      ),
    ).toHaveLength(1);

    await harness.accept("private", "participant-request-duplicate-answer", answer, "partner");
    await harness.drain();
    expect(ordinaryReasonerSawParticipantReply).toBe(true);
    expect(durableRuns).toBe(3);
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === FAMILY_GROUP && message.text === terminalText,
      ),
    ).toHaveLength(1);
    await harness.assertDatabase(
      "A participant reply did not complete exactly one resumed household task",
      `(select count(*)=1 from proactive_work
        where kind='family_task' and visibility='household' and objective=${sqlLiteral(objective)}
          and status='completed' and task_state->'pendingParticipantRequest'='null'::jsonb)`,
    );
  }, 20_000);

  test("plans next week's dinners from fresh family context every Sunday until either parent stops it", async () => {
    const request =
      "Every Sunday at 4 PM, look at next week’s Family Calendar and our saved recipes and preferences, plan dinners, and send us the grocery list.";
    const acknowledgement =
      "Absolutely—I’ll plan next week’s dinners from the family calendar and what you’ve saved, then send the grocery list here every Sunday at 4 PM.";
    const objective =
      "Every Sunday, use the latest Family Calendar, saved recipes, and family preferences to plan next week's dinners and send the grocery list.";
    const schedule = {
      kind: "weekly" as const,
      everyWeeks: 1,
      weekdays: [7],
      localTime: "16:00",
      startsOn: "2026-08-16",
    };
    const firstDue = "2026-08-16T23:00:00.000Z";
    const secondDue = "2026-08-23T23:00:00.000Z";
    const thirdDue = "2026-08-30T23:00:00.000Z";
    const firstResult =
      "Next week’s dinners are planned around Maya’s Wednesday field-trip deadline. Grocery list: spaghetti, soy sauce, sesame oil, rice vinegar, and easy sides.";
    const addedRecipeRequest =
      "Please save sheet-pan fajitas as another easy family dinner: peppers, onions, tortillas, and black beans; keep the kids’ portion mild.";
    const addedRecipeTitle = "Sheet-pan fajitas";
    const addedRecipeDetails =
      "Ingredients: peppers, onions, tortillas, and black beans. Family note: keep the kids’ portion mild.";
    const addedCalendarTitle = "Thursday school open house";
    const secondResult =
      "I updated next week’s plan for Thursday’s school open house and added the saved sheet-pan fajitas. Grocery list: peppers, onions, tortillas, black beans, and mild toppings.";
    const cancelRequest = "Please stop the Sunday dinner planning.";
    const cancelAcknowledgement = "Okay—I’ve stopped the Sunday dinner planning.";
    const occurrences: Array<{
      generation: number;
      currentTime: string;
      previousResult: string | null;
      previousRunAt: string | null;
      recipeTitle: string;
      calendarTitles: string[];
    }> = [];

    const harness = await createHarness(
      async (input) => {
        if (input.currentMessage.text === request) {
          return decision({
            bubbles: [{ text: acknowledgement, delayMs: 0 }],
            familyWork: {
              operation: "create",
              workId: null,
              objective,
              instruction: null,
              schedule,
              candidateIds: [],
            },
          });
        }
        if (input.currentMessage.text === addedRecipeRequest) {
          return decision({
            bubbles: [{ text: "Saved—I’ll use the fajitas in future family meal plans.", delayMs: 0 }],
            facts: [
              {
                operation: "remember",
                factId: null,
                statement: "The family has a reusable sheet-pan fajitas recipe.",
                visibility: "household",
                memory: {
                  memoryKind: "artifact",
                  artifactKind: "recipe",
                  title: addedRecipeTitle,
                  details: addedRecipeDetails,
                  tags: ["recipe", "fajitas", "weeknight"],
                },
                sourceIds: [input.currentMessage.sourceId],
              },
            ],
          });
        }
        if (input.currentMessage.text === cancelRequest) {
          const scheduledWork = input.visibleFamilyWork.find((work) => work.objective === objective);
          if (!scheduledWork) throw new Error("The partner could not see the shared Sunday dinner planning");
          expect(scheduledWork).toMatchObject({
            schedule,
            paused: false,
            status: "active",
            nextAt: thirdDue,
            lastRunAt: secondDue,
            lastResult: secondResult,
          });
          return decision({
            bubbles: [{ text: cancelAcknowledgement, delayMs: 0 }],
            familyWork: {
              operation: "cancel",
              workId: scheduledWork.workId,
              objective: null,
              instruction: null,
              schedule: null,
            },
          });
        }
        return decision();
      },
      {
        continueFamilyWork: async (input, reads) => {
          const occurrence = input.scheduledOccurrence;
          if (!occurrence) throw new Error("Recurring family work lost its scheduled occurrence");
          const runIndex = occurrences.length;
          const expectedDue = runIndex === 0 ? firstDue : secondDue;
          const expectedPreviousResult = runIndex === 0 ? null : firstResult;
          const expectedPreviousRunAt = runIndex === 0 ? null : firstDue;
          expect(input.state.generation).toBe(runIndex);
          expect(occurrence).toEqual({
            schedule,
            previousResult: expectedPreviousResult,
            previousRunAt: expectedPreviousRunAt,
          });
          expect(input.currentTime).toBe(expectedDue);

          const vaultSearch = await reads.searchVault?.({
            query: runIndex === 0 ? "mild weeknight noodle recipe" : "sheet-pan fajitas recipe",
            cursor: null,
          });
          const recipeSearchResult = vaultSearch?.results.find((result) =>
            runIndex === 0 ? result.title === GOOGLE_RECIPE_TITLE : result.title === addedRecipeTitle,
          );
          if (!recipeSearchResult || !reads.readVault) {
            throw new Error("Scheduled dinner planning could not find the current saved recipe");
          }
          const recipe = await reads.readVault({ uri: recipeSearchResult.uri, level: "full" });
          const expectedRecipeTitle = runIndex === 0 ? GOOGLE_RECIPE_TITLE : addedRecipeTitle;
          expect(recipe?.memory).toMatchObject({
            title: expectedRecipeTitle,
            artifactKind: "recipe",
            details: expect.stringContaining(runIndex === 0 ? "keep it mild" : "black beans"),
          });

          const catalog = await reads.listCalendars?.();
          const familyCalendar = catalog?.calendars.find(
            (calendar) => calendar.primary === null && calendar.accessRole === null,
          );
          if (!familyCalendar || !reads.readCalendarWindow) {
            throw new Error("Scheduled dinner planning could not resolve the Family Calendar");
          }
          const calendar = await reads.readCalendarWindow({
            timeMin: runIndex === 0 ? "2026-08-17T07:00:00.000Z" : "2026-08-24T07:00:00.000Z",
            timeMax: runIndex === 0 ? "2026-08-24T07:00:00.000Z" : "2026-08-31T07:00:00.000Z",
            pageSize: 50,
            cursor: null,
            scope: "selected",
            calendarRefs: [familyCalendar.calendarRef],
          });
          const expectedCalendarTitle = runIndex === 0 ? AUTOMATIC_FAMILY_DATE.title : addedCalendarTitle;
          expect(calendar.status).toBe("complete");
          expect(calendar.events.some((event) => event.title === expectedCalendarTitle)).toBe(true);

          occurrences.push({
            generation: input.state.generation,
            currentTime: input.currentTime,
            previousResult: occurrence.previousResult,
            previousRunAt: occurrence.previousRunAt,
            recipeTitle: expectedRecipeTitle,
            calendarTitles: calendar.events.flatMap((event) => (event.title ? [event.title] : [])),
          });
          const text = runIndex === 0 ? firstResult : secondResult;
          return {
            kind: "terminal",
            state: {
              ...input.state,
              phase: "terminal",
              claim: null,
              pendingCall: null,
              progressRevision: input.state.progressRevision + 1,
              terminal: { outcome: "succeeded", text },
            },
            outcome: "succeeded",
            text,
          };
        },
      },
    );
    harness.state.googleRecipeArtifactExercise = true;
    await harness.readyHousehold();

    await harness.accept("group", "schedule-sunday-dinner-planning", request);
    await harness.drain();
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === FAMILY_GROUP && message.text === acknowledgement,
      ),
    ).toHaveLength(1);
    expect(occurrences).toHaveLength(0);
    await harness.assertDatabase(
      "The Sunday planning request did not become one future recurring family task",
      `(select count(*)=1 from proactive_work
        where kind='family_task' and visibility='household' and owner_adult_id is null
          and objective=${sqlLiteral(objective)} and status='active'
          and reminder_schedule->'schedule'=${sqlLiteral(JSON.stringify(schedule))}::jsonb
          and reminder_schedule->'version'='1'::jsonb
          and reminder_schedule->'paused'='false'::jsonb
          and reminder_schedule->'occurrenceActive'='false'::jsonb
          and reminder_schedule->'previousResult'='null'::jsonb
          and next_check_at=${sqlLiteral(firstDue)}::timestamptz
          and last_run_at is null and task_state->>'generation'='0')`,
    );

    harness.state.now = Date.parse(firstDue) - 1;
    await harness.drain();
    expect(occurrences).toHaveLength(0);

    harness.state.now = Date.parse(firstDue);
    await harness.drain();
    expect(occurrences).toEqual([
      expect.objectContaining({
        generation: 0,
        currentTime: firstDue,
        previousResult: null,
        previousRunAt: null,
        recipeTitle: GOOGLE_RECIPE_TITLE,
        calendarTitles: expect.arrayContaining([AUTOMATIC_FAMILY_DATE.title]),
      }),
    ]);
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === FAMILY_GROUP && message.text === firstResult,
      ),
    ).toHaveLength(1);
    const firstDelivery = harness.linq.messages.find(
      (message) => message.providerConversationId === FAMILY_GROUP && message.text === firstResult,
    );
    if (!firstDelivery) throw new Error("The first scheduled dinner plan was not delivered");
    expect(firstDelivery.replyTo).toBeUndefined();
    const firstReceipt = harness.linq.ledger.sent.get(firstDelivery.idempotencyKey);
    if (firstReceipt?.status !== "committed" || !firstReceipt.providerReceiptId) {
      throw new Error("The first scheduled dinner plan lost its Linq receipt");
    }
    const requestSourceId = inboundSourceId("event-schedule-sunday-dinner-planning");
    const scheduledWorkId = deterministicUuid(`family-work\0${requestSourceId}`);
    const terminalSuffix = "family-work:terminal:0:1";
    const terminalSourceId = deterministicUuid(
      `proactive-outbound\0${scheduledWorkId}\0${digest(terminalSuffix)}`,
    );
    await harness.store.completeOutbound({
      sourceId: terminalSourceId,
      providerMessageId: firstReceipt.providerReceiptId,
      sentAt: firstReceipt.occurredAt,
    });
    await harness.drain();
    expect(occurrences).toHaveLength(1);
    expect(harness.linq.messages.filter((message) => message.text === firstResult)).toHaveLength(1);
    expect(
      harness.linq.sendMessageAttempts.filter(
        (message) => message.idempotencyKey === firstDelivery.idempotencyKey,
      ),
    ).toHaveLength(1);
    await harness.assertDatabase(
      "The first Sunday result did not rearm the same family task for the next occurrence",
      `(select count(*)=1 from proactive_work
        where kind='family_task' and objective=${sqlLiteral(objective)} and status='active'
          and reminder_schedule->'schedule'=${sqlLiteral(JSON.stringify(schedule))}::jsonb
          and reminder_schedule->>'previousResult'=${sqlLiteral(firstResult)}
          and reminder_schedule->'occurrenceActive'='false'::jsonb
          and next_check_at=${sqlLiteral(secondDue)}::timestamptz
          and last_run_at=${sqlLiteral(firstDue)}::timestamptz
          and current_conclusion=${sqlLiteral(firstResult)}
          and task_state->>'generation'='1'
          and task_state->>'phase'='ready' and task_state->'terminal'='null'::jsonb)`,
    );

    await harness.accept("group", "save-sheet-pan-fajitas", addedRecipeRequest, "partner");
    await harness.drain();
    harness.state.calendarEvents.set("family-open-house-for-second-dinner-plan", {
      providerEventId: "family-open-house-for-second-dinner-plan",
      providerRevision: "family-open-house-for-second-dinner-plan-revision-1",
      providerUpdatedAt: "2026-08-23T22:00:00.000Z",
      status: "confirmed",
      busy: true,
      title: addedCalendarTitle,
      location: "Muir Elementary",
      intervalKind: "all_day",
      allDay: true,
      startDate: "2026-08-27",
      endDate: "2026-08-28",
    });

    harness.state.now = Date.parse(secondDue);
    await harness.drain();
    expect(occurrences).toEqual([
      expect.objectContaining({ generation: 0 }),
      expect.objectContaining({
        generation: 1,
        currentTime: secondDue,
        previousResult: firstResult,
        previousRunAt: firstDue,
        recipeTitle: addedRecipeTitle,
        calendarTitles: expect.arrayContaining([addedCalendarTitle]),
      }),
    ]);
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === FAMILY_GROUP && message.text === secondResult,
      ),
    ).toHaveLength(1);
    await harness.drain();
    expect(occurrences).toHaveLength(2);
    expect(harness.linq.messages.filter((message) => message.text === secondResult)).toHaveLength(1);
    await harness.assertDatabase(
      "The second Sunday result lost its prior result or failed to advance the same task",
      `(select count(*)=1 from proactive_work
        where kind='family_task' and objective=${sqlLiteral(objective)} and status='active'
          and reminder_schedule->'schedule'=${sqlLiteral(JSON.stringify(schedule))}::jsonb
          and reminder_schedule->>'previousResult'=${sqlLiteral(secondResult)}
          and reminder_schedule->'occurrenceActive'='false'::jsonb
          and next_check_at=${sqlLiteral(thirdDue)}::timestamptz
          and last_run_at=${sqlLiteral(secondDue)}::timestamptz
          and current_conclusion=${sqlLiteral(secondResult)}
          and task_state->>'generation'='2')`,
    );

    await harness.accept("group", "partner-cancel-sunday-dinner-planning", cancelRequest, "partner");
    await harness.drain();
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP && message.text === cancelAcknowledgement,
      ),
    ).toHaveLength(1);
    harness.state.now = Date.parse(thirdDue);
    await harness.drain();
    expect(occurrences).toHaveLength(2);
    await harness.assertDatabase(
      "The partner's cancellation left another Sunday dinner-plan occurrence armed",
      `(select count(*)=1 from proactive_work
        where kind='family_task' and objective=${sqlLiteral(objective)}
          and status='cancelled' and next_check_at is null)`,
    );
  }, 20_000);

  test("keeps a private durable task private while it reads private context and commits Family Calendar work", async () => {
    const request =
      "Check my school-form email, then add a Family Calendar block for us to sign Maya’s form Tuesday.";
    const objective =
      "Use my private school-form email to add a household-safe block for signing Maya’s form to the Family Calendar.";
    const terminalText = "I added “Sign Maya’s school form” to the Family Calendar.";
    const event = {
      intervalKind: "timed" as const,
      title: "Sign Maya’s school form",
      startsAt: "2026-08-18T19:00:00.000Z",
      endsAt: "2026-08-18T19:30:00.000Z",
      timeZone: "America/Los_Angeles",
      location: null,
    };
    let privateSourceRead = false;
    let selectedFamilyCalendarRead = false;
    let providerReceiptObserved = false;
    const harness = await createHarness(
      async (input) =>
        input.currentMessage.text === request
          ? decision({
              familyWork: {
                operation: "create",
                workId: null,
                objective,
                schedule: null,
                instruction: null,
                candidateIds: [],
              },
            })
          : decision(),
      {
        continueFamilyWork: async (input, reads) => {
          expect(input.visibility).toBe("private");
          expect(input.ownerAdultId).toBe(founderSetup().adultId);
          expect(input.initiatingAdultId).toBe(founderSetup().adultId);
          if (input.state.phase === "ready") {
            const gmail = await reads.searchGmail?.({
              query: "Maya school form needs signature",
              limit: 10,
            });
            privateSourceRead =
              gmail?.status === "complete" &&
              gmail.sources.some(
                (source) =>
                  source.visibility === "adult_private" && source.text.includes("form needs a signature"),
              );
            if (!privateSourceRead) {
              throw new Error("The private durable task could not read its initiating adult's Gmail");
            }

            const catalog = await reads.listCalendars?.();
            const familyCalendar = catalog?.calendars.find(
              (calendar) => calendar.primary === null && calendar.accessRole === null,
            );
            if (catalog?.status !== "complete" || !familyCalendar) {
              throw new Error("The private durable task could not resolve the exact Family Calendar");
            }
            const personal = await reads.readCalendarWindow?.({
              timeMin: "2026-08-18T07:00:00.000Z",
              timeMax: "2026-08-19T07:00:00.000Z",
              pageSize: 50,
              cursor: null,
              scope: "primary",
              calendarRefs: [],
            });
            if (
              !personal?.events.some((calendarEvent) => calendarEvent.title === "Private calendar detail")
            ) {
              throw new Error("The private durable task lost its initiating adult's Calendar context");
            }
            const shared = await reads.readCalendarWindow?.({
              timeMin: "2026-08-18T07:00:00.000Z",
              timeMax: "2026-08-19T07:00:00.000Z",
              pageSize: 50,
              cursor: null,
              scope: "selected",
              calendarRefs: [familyCalendar.calendarRef],
            });
            selectedFamilyCalendarRead =
              shared?.status === "complete" &&
              shared.calendars.length === 1 &&
              shared.calendars[0]?.calendarRef === familyCalendar.calendarRef &&
              shared.calendars[0].status === "complete";
            if (!selectedFamilyCalendarRead) {
              throw new Error("The private durable task could not read the selected Family Calendar");
            }
            return {
              kind: "continue",
              state: {
                ...input.state,
                phase: "tool_pending",
                claim: null,
                pendingCall: {
                  callId: "private-family-calendar-create",
                  name: "family_calendar_work",
                  argumentsJson: JSON.stringify({ operation: "create", event, target: null }),
                  attempt: 0,
                },
              },
              progressText: null,
              nextCheckDelayMs: 0,
            };
          }

          if (!reads.runFamilyCalendarWork) {
            throw new Error("The private durable task did not receive Family Calendar work");
          }
          const receipt = await reads.runFamilyCalendarWork({
            operation: "create",
            event,
            target: null,
          });
          providerReceiptObserved =
            receipt.status === "committed" &&
            receipt.operation === "create" &&
            receipt.providerEventId.length > 0 &&
            receipt.providerRevision !== null;
          if (!providerReceiptObserved) {
            throw new Error("The private durable task did not receive a provider-confirmed Calendar receipt");
          }
          return {
            kind: "terminal",
            state: {
              ...input.state,
              phase: "terminal",
              claim: null,
              pendingCall: null,
              terminal: { outcome: "succeeded", text: terminalText },
            },
            outcome: "succeeded",
            text: terminalText,
          };
        },
      },
    );
    await harness.readyHousehold();

    const messagesBeforeRequest = harness.linq.messages.length;
    await harness.accept("private", "private-durable-family-calendar", request);
    await harness.drain();
    harness.state.now += 1_001;
    await harness.drain();

    expect(privateSourceRead).toBe(true);
    expect(selectedFamilyCalendarRead).toBe(true);
    expect(providerReceiptObserved).toBe(true);
    expect(
      harness.state.calendarExecutions.filter(
        (execution) =>
          execution.calendarId === FAMILY_CALENDAR &&
          execution.mutation.operation === "create" &&
          execution.mutation.event.title === event.title,
      ),
    ).toHaveLength(1);
    const taskMessages = harness.linq.messages.slice(messagesBeforeRequest);
    expect(
      taskMessages.filter(
        (message) => message.providerConversationId === PRIVATE_FOUNDER && message.text === terminalText,
      ),
    ).toHaveLength(1);
    expect(
      taskMessages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          (message.text === terminalText || message.text.includes(event.title)),
      ),
    ).toHaveLength(0);
    await harness.assertDatabase(
      "Private durable Family Calendar work did not keep its terminal delivery private",
      `(select count(*)=1 from proactive_work
          where kind='family_task' and objective=${sqlLiteral(objective)}
            and visibility='private'
            and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
            and status='completed')
        and (select count(*)=1 from messages terminal
          join sources source on source.id=terminal.source_id
          join linq_channels channel on channel.id=terminal.channel_id
          where terminal.direction='outbound' and terminal.status='sent'
            and terminal.text=${sqlLiteral(terminalText)}
            and source.visibility='private'
            and source.owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
            and channel.audience='private'
            and channel.adult_one_id=${sqlLiteral(harness.founderAdultId)}::uuid
            and channel.adult_two_id is null)
        and not exists (
          select 1 from messages message join linq_channels channel on channel.id=message.channel_id
          where channel.audience='group'
            and (message.text=${sqlLiteral(terminalText)}
              or message.text like ${sqlLiteral(`%${event.title}%`)})
        )`,
    );
  }, 20_000);

  test("uses the initiating partner's Google Workspace connection for durable work from the family thread", async () => {
    const request =
      "Add ‘Pack Maya’s camp permission slip’ to my Google Tasks, then tell us here when it’s done.";
    const objective =
      "Add Pack Maya’s camp permission slip to the initiating parent's Google Tasks and confirm completion in the family thread.";
    const taskTitle = "Pack Maya’s camp permission slip";
    const terminalText = "Done—I added “Pack Maya’s camp permission slip” to Alex’s Google Tasks.";
    const operation = {
      operation: "tasks_create",
      title: taskTitle,
      notes: "Requested in the family thread.",
    } satisfies GoogleWorkspaceOperation;
    let providerReceipt: GoogleWorkspaceResult | null = null;
    const harness = await createHarness(
      async (input) =>
        input.currentMessage.text === request
          ? decision({
              familyWork: {
                operation: "create",
                workId: null,
                objective,
                schedule: null,
                instruction: null,
                candidateIds: [],
              },
            })
          : decision(),
      {
        continueFamilyWork: async (input, reads) => {
          expect(input.visibility).toBe("household");
          expect(input.ownerAdultId).toBeNull();
          expect(input.initiatingAdultId).toBe(harness.partnerAdultId);
          if (input.state.phase === "ready") {
            return {
              kind: "continue",
              state: {
                ...input.state,
                phase: "tool_pending",
                claim: null,
                pendingCall: {
                  callId: "partner-google-task-create",
                  name: "tasks_work",
                  argumentsJson: JSON.stringify(operation),
                  attempt: 0,
                },
              },
              progressText: null,
              nextCheckDelayMs: 0,
            };
          }

          if (!reads.runGoogleWorkspace) {
            throw new Error(
              "The group-originated task did not receive the initiating partner's Workspace adapter",
            );
          }
          providerReceipt = await reads.runGoogleWorkspace(operation);
          expect(providerReceipt).toEqual({
            operation: "tasks_create",
            result: {
              status: "created",
              taskId: "google-task-1",
              title: taskTitle,
            },
          });
          return {
            kind: "terminal",
            state: {
              ...input.state,
              phase: "terminal",
              claim: null,
              pendingCall: null,
              progressRevision: input.state.progressRevision + 1,
              terminal: { outcome: "succeeded", text: terminalText },
            },
            outcome: "succeeded",
            text: terminalText,
          };
        },
      },
    );
    await harness.readyHousehold();

    const messagesBeforeRequest = harness.linq.messages.length;
    await harness.accept("group", "partner-google-task", request, "partner");
    await harness.drain();
    harness.state.now += 1_001;
    await harness.drain();

    expect(providerReceipt).not.toBeNull();
    expect(harness.state.workspaceExecutions).toEqual([
      {
        householdId: expect.any(String),
        ownerAdultId: harness.partnerAdultId,
        connectionId: PARTNER_GOOGLE,
        operation,
      },
    ]);
    const taskMessages = harness.linq.messages.slice(messagesBeforeRequest);
    const taskTerminal = taskMessages.filter(
      (message) => message.providerConversationId === FAMILY_GROUP && message.text === terminalText,
    );
    expect(taskTerminal).toHaveLength(1);
    expect(taskTerminal[0]?.replyTo).toEqual({ providerMessageId: "message-partner-google-task" });
    expect(
      taskMessages.filter(
        (message) =>
          (message.providerConversationId === PRIVATE_FOUNDER ||
            message.providerConversationId === PRIVATE_PARTNER) &&
          message.text === terminalText,
      ),
    ).toHaveLength(0);
    await harness.assertDatabase(
      "Group-originated Workspace work did not complete once in the household thread",
      `(select count(*)=1 from proactive_work
          where kind='family_task' and objective=${sqlLiteral(objective)}
            and visibility='household' and owner_adult_id is null and status='completed')
        and (select count(*)=1 from messages terminal
          join linq_channels channel on channel.id=terminal.channel_id
          join sources source on source.id=terminal.source_id
          where terminal.direction='outbound' and terminal.status='sent'
            and terminal.text=${sqlLiteral(terminalText)}
            and channel.audience='group'
            and source.visibility='household' and source.owner_adult_id is null)`,
    );
  }, 20_000);

  test("carries an exact Gmail attachment into authenticated browser work once", async () => {
    const browserRuns: Parameters<FlorenceBrowserClient["run"]>[0][] = [];
    const closedSessions: Parameters<FlorenceBrowserClient["close"]>[0][] = [];
    const browser: FlorenceBrowserClient = {
      run: async (input) => {
        browserRuns.push(input);
        return {
          session: {
            sessionId: "browserbase-family-session",
            expiresAt: "2026-08-27T21:00:00.000Z",
          },
          observation: {
            kind: "page",
            reason: null,
            url: "https://camp.example/register",
            title: "Camp registration",
            snapshot: "Registration form",
            refCount: 0,
            truncated: false,
          },
        };
      },
      close: async (session) => {
        closedSessions.push(session);
      },
      closeAll: async () => undefined,
    };
    const gmailIdentity = {
      messageId: SCHOOL_ATTACHMENT.messageId,
      threadId: SCHOOL_ATTACHMENT.threadId,
      historyId: SCHOOL_ATTACHMENT.historyId,
    };
    const harness = await createHarness(
      async (input) =>
        input.currentMessage.text === "Find Maya's field-trip form in Gmail and upload it for us."
          ? decision({
              familyWork: {
                operation: "create",
                workId: null,
                objective: "Upload Maya's field-trip form from Gmail.",
                schedule: null,
                instruction: null,
                candidateIds: [],
              },
            })
          : decision(),
      {
        browser,
        continueFamilyWork: async (input, reads) => {
          const internalReads = reads as unknown as Pick<FlorenceReadTools, "readWorkspaceGmailSource">;
          if (!internalReads.readWorkspaceGmailSource) {
            throw new Error("The family task did not receive exact Gmail reading");
          }
          const source = await internalReads.readWorkspaceGmailSource(gmailIdentity);
          const attachment = source.attachments[0];
          if (!attachment) throw new Error("The exact Gmail message lost its attachment");
          if (input.state.phase === "ready") {
            expect(input.origin.message).toMatchObject({
              speaker: expect.any(String),
              text: "Find Maya's field-trip form in Gmail and upload it for us.",
              authoredText: "Find Maya's field-trip form in Gmail and upload it for us.",
              voiceTranscriptPresent: false,
            });
            const signalId = input.workId;
            const storedImage = await harness.vault.store({
              assetId: deterministicUuid(`camp-registration-image\0${input.workId}`),
              householdId: input.household.householdId,
              signalId,
              declaredMimeType: "image/jpeg",
              bytes: JPEG_BYTES,
            });
            return {
              kind: "continue",
              state: {
                ...input.state,
                phase: "tool_pending",
                claim: null,
                browserImages: [
                  {
                    ...storedImage.image,
                    signalId,
                    workId: input.workId,
                    filename: "camp-registration.jpg",
                  },
                ],
                pendingCall: {
                  callId: "upload-field-trip-form",
                  name: "browser_work",
                  argumentsJson: JSON.stringify({
                    operation: "upload",
                    ref: "e7",
                    sourceId: source.sourceId,
                    attachmentRef: attachment.attachmentRef,
                  }),
                  attempt: 0,
                },
              },
              progressText: null,
              nextCheckDelayMs: 0,
            };
          }
          if (!reads.runBrowser)
            throw new Error("The family task did not receive authenticated browser work");
          const operation = {
            kind: "upload" as const,
            ref: "e7",
            sourceId: source.sourceId,
            attachmentRef: attachment.attachmentRef,
          };
          const observation = await reads.runBrowser(operation);
          expect(observation.title).toBe("Camp registration");
          if (input.state.pendingCall?.attempt === 1) {
            return {
              kind: "continue",
              state: {
                ...input.state,
                phase: "tool_pending",
                claim: null,
                pendingCall: { ...input.state.pendingCall, attempt: 2 },
              },
              progressText: null,
              nextCheckDelayMs: 0,
            };
          }
          const terminalText = "I uploaded Maya's field-trip form.";
          const selectedImage = input.state.browserImages?.[0];
          if (!selectedImage) throw new Error("The completed browser work lost its selected image");
          return {
            kind: "terminal",
            state: {
              ...input.state,
              phase: "terminal",
              claim: null,
              pendingCall: null,
              progressRevision: input.state.progressRevision + 1,
              terminal: { outcome: "succeeded", text: terminalText, selectedImages: [selectedImage] },
            },
            outcome: "succeeded",
            text: terminalText,
            selectedImages: [selectedImage],
          };
        },
      },
    );
    await harness.readyHousehold();

    await harness.accept(
      "group",
      "group-browser-work",
      "Find Maya's field-trip form in Gmail and upload it for us.",
      "partner",
    );
    await harness.drain();
    harness.state.now += 1_001;
    await harness.drain();

    expect(browserRuns).toHaveLength(2);
    expect(browserRuns[0]).toMatchObject({
      ownerAdultId: harness.partnerAdultId,
      operation: {
        kind: "upload",
        ref: "e7",
        sourceId: expect.any(String),
        attachmentRef: expect.any(String),
      },
      uploadFile: { filename: SCHOOL_ATTACHMENT.filename, bytes: PDF_BYTES },
    });
    expect(browserRuns[1]?.uploadFile).toBeUndefined();
    expect(closedSessions).toEqual([expect.objectContaining({ sessionId: "browserbase-family-session" })]);
    const terminalMove = harness.linq.moves.find(
      ({ move }) =>
        move.type === "message" &&
        move.parts.some((part) => part.type === "text" && part.text === "I uploaded Maya's field-trip form."),
    );
    expect(terminalMove?.move).toMatchObject({
      type: "message",
      replyTo: { providerMessageId: "message-group-browser-work" },
      parts: [
        { type: "text", text: "I uploaded Maya's field-trip form." },
        { type: "media", source: { type: "attachment", providerAttachmentId: expect.any(String) } },
      ],
    });
  }, 20_000);

  test("gets ahead from both parents’ context, native inputs, a monitor, and the read-only calendar", async () => {
    const retainedSourceRecallRequest = "What did that archived account notice with code 123456 say?";
    const retainedSourceRecallReply =
      "The archived notice said the account needed attention and included confirmation code 123456.";
    const groupRetainedSourceRequest = "Search Hari’s old Google notices for code 123456.";
    const groupRetainedSourceReply =
      "That history stays in Hari’s private thread—ask me there and I can look it up.";
    let nativeInputWasRead = false;
    let ordinaryUnusedSourceId: string | null = null;
    let conversationalGoogleSourceId: string | null = null;
    let retainedGoogleMemorySourceId: string | null = null;
    let googleRecipeSearchReturnedUsableDetails = false;
    let groupHouseholdFactWasVisible = false;
    let retainedPrivateSourceWasRead = false;
    let groupSourceSearchWasHidden = false;
    const docketWorkRequest = "Please take care of Maya’s field-trip form from the docket.";
    const docketWorkObjective =
      "Use the retained source for Maya’s field-trip form to determine and complete the next useful family action.";
    const docketWorkAcknowledgement = "I’m on it—I’ll work from the form already on the docket.";
    const docketWorkResult = "I opened the exact school-form source and worked from that family deadline.";
    const docketWorkNextAction = "Choose who will sign and return Maya’s field-trip form.";
    const docketWorkWaitingOn = "A parent to take the remaining signature step";
    let docketWorkReadExactLinkedSource = false;
    let nativeDocketWorkReadExactEvidence = false;
    let nativeDocketWorkOriginStayedCurrent = false;
    const observedHouseholdDocket: {
      value: FlorenceReasonerInput["householdDocket"] | null;
    } = { value: null };
    const publicResearchCapture: {
      mainRequest: Record<string, unknown> | null;
      queries: string[];
      sourceUrls: string[];
    } = { mainRequest: null, queries: [], sourceUrls: [] };
    let publicResearchModelTurns = 0;
    let publicSearchTurns = 0;
    const publicResearchReasoner = new FlorenceReasoner({ apiKey: "test-openai-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          publicResearchModelTurns += 1;
          publicResearchCapture.mainRequest ??= request;
          publicSearchTurns += 1;
          const currentInput = JSON.stringify(request.input);
          const noResult = currentInput.includes(PUBLIC_NO_RESULT_REQUEST);
          const shortIdentifier = currentInput.includes(PUBLIC_SHORT_IDENTIFIER_REQUEST);
          const publicConcept = currentInput.includes(PUBLIC_CONCEPT_REQUEST);
          const query = noResult
            ? "9780143127796 2026-08-27"
            : publicConcept
              ? `access token password managers confirmation code format ${PUBLIC_CONCEPT_URL}`
              : shortIdentifier
                ? "X public social platform identifier"
                : "DL 747 live route status alternatives tonight";
          const sourceUrl = noResult
            ? PUBLIC_NO_RESULT_SOURCE
            : publicConcept
              ? PUBLIC_CONCEPT_URL
              : shortIdentifier
                ? PUBLIC_SHORT_IDENTIFIER_URL
                : PUBLIC_RESEARCH_URL;
          const finalReply = noResult
            ? PUBLIC_NO_RESULT_REPLY
            : publicConcept
              ? PUBLIC_CONCEPT_REPLY
              : shortIdentifier
                ? PUBLIC_SHORT_IDENTIFIER_REPLY
                : PUBLIC_RESEARCH_REPLY;
          const finalUrls = noResult
            ? []
            : [
                publicConcept
                  ? PUBLIC_CONCEPT_URL
                  : shortIdentifier
                    ? PUBLIC_SHORT_IDENTIFIER_URL
                    : PUBLIC_RESEARCH_URL,
              ];
          publicResearchCapture.queries.push(query);
          publicResearchCapture.sourceUrls.push(sourceUrl);
          return fakeResponseStream([], {
            output_parsed: decision({
              reaction: finalReply === PUBLIC_RESEARCH_REPLY ? "emphasize" : null,
              bubbles: [{ text: finalReply, delayMs: 0 }],
              researchUrls: finalUrls,
            }),
            output: [
              {
                id: `web-search-${publicSearchTurns}`,
                type: "web_search_call",
                status: "completed",
                action: {
                  type: "search",
                  query,
                  sources: [{ type: "url", url: sourceUrl }],
                },
              },
            ],
          });
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
    const householdReasoner: Reason = async (input, reads, signal, hooks) => {
      if (input.currentMessage.text === HOUSEHOLD_DOCKET_REQUEST) {
        observedHouseholdDocket.value = input.householdDocket;
        return decision({ bubbles: [{ text: HOUSEHOLD_DOCKET_REPLY, delayMs: 0 }] });
      }
      if (input.currentMessage.text === docketWorkRequest) {
        const candidate = input.householdDocket.items.find((item) => item.summary === FOUNDER_FORM_SUMMARY);
        if (!candidate) throw new Error("The selected docket item was not supplied to the reasoner");
        return decision({
          bubbles: [{ text: docketWorkAcknowledgement, delayMs: 0 }],
          familyWork: {
            operation: "create",
            workId: null,
            objective: docketWorkObjective,
            schedule: null,
            instruction: null,
            candidateIds: [candidate.candidateId],
          },
        });
      }
      if (input.currentMessage.text === HOUSEHOLD_DOCKET_HANDLED) {
        const candidate = input.householdDocket.items.find(
          (item) => item.summary === PARTNER_PERMISSION_SUMMARY,
        );
        if (!candidate) throw new Error("The handled docket item was not supplied to the reasoner");
        return decision({
          bubbles: [{ text: HOUSEHOLD_DOCKET_HANDLED_ACK, delayMs: 0 }],
          docketCompletions: [candidate.candidateId],
        });
      }
      if (input.currentMessage.text === PRIVATE_DOCKET_RECALL_REQUEST) {
        if (!input.householdDocket.items.some((item) => item.summary === PRIVATE_INITIAL_ONLY_FINDING)) {
          throw new Error("The owner-private initial finding was not retrievable in its adult's turn");
        }
        return decision({ bubbles: [{ text: PRIVATE_DOCKET_RECALL_REPLY, delayMs: 0 }] });
      }
      if (input.currentMessage.text === PRIVATE_DOCKET_HANDLED_REQUEST) {
        const candidate = input.householdDocket.items.find(
          (item) => item.summary === PRIVATE_INITIAL_ONLY_FINDING,
        );
        if (!candidate) throw new Error("The owner-private docket item was not available to complete");
        return decision({
          bubbles: [{ text: PRIVATE_DOCKET_HANDLED_REPLY, delayMs: 0 }],
          docketCompletions: [candidate.candidateId],
        });
      }
      if (input.currentMessage.text === NATIVE_DOCKET_CORRECTION) {
        const candidate = input.householdDocket.items.find((item) => item.summary === NATIVE_DOCKET_SUMMARY);
        if (
          candidate?.visibility !== "household" ||
          input.currentMessage.replyTo?.sourceId !== inboundSourceId("event-native-school-update")
        ) {
          throw new Error("The ordinary Message docket item was not available to reconcile");
        }
        return decision({
          bubbles: [{ text: "Got it—I updated the field-trip deadline on the docket.", delayMs: 0 }],
          docketUpsert: {
            operation: "update",
            candidateId: candidate.candidateId,
            candidate: {
              category: "deadline",
              summary: NATIVE_DOCKET_UPDATED_SUMMARY,
              urgency: "soon",
              dueAt: new Date(Date.parse(input.currentMessage.occurredAt) + 60 * 60_000).toISOString(),
              needsAnswer: true,
              owner: NATIVE_DOCKET_OWNER,
              nextAction: NATIVE_DOCKET_UPDATED_NEXT_ACTION,
              waitingOn: NATIVE_DOCKET_WAITING_ON,
            },
            sourceIds: [input.currentMessage.sourceId, input.currentMessage.replyTo.sourceId],
          },
        });
      }
      if (input.currentMessage.text === NATIVE_DOCKET_WORK_REQUEST) {
        const candidate = input.householdDocket.items.find(
          (item) => item.summary === NATIVE_DOCKET_UPDATED_SUMMARY,
        );
        if (candidate?.visibility !== "household") {
          throw new Error("The reconciled Message docket item was not available for family work");
        }
        return decision({
          bubbles: [{ text: NATIVE_DOCKET_WORK_ACK, delayMs: 0 }],
          familyWork: {
            operation: "create",
            workId: null,
            objective: NATIVE_DOCKET_WORK_OBJECTIVE,
            schedule: null,
            instruction: null,
            candidateIds: [candidate.candidateId],
          },
        });
      }
      if (input.currentMessage.text === PRIVATE_CONVERSATION_DOCKET_REQUEST) {
        return decision({
          bubbles: [{ text: "I’ll keep that decision on your private docket.", delayMs: 0 }],
          docketUpsert: {
            operation: "create",
            candidateId: null,
            candidate: {
              category: "loose_end",
              summary: PRIVATE_CONVERSATION_DOCKET_SUMMARY,
              urgency: "watch",
              dueAt: null,
              needsAnswer: true,
              owner: "You",
              nextAction: PRIVATE_CONVERSATION_DOCKET_NEXT_ACTION,
              waitingOn: PRIVATE_CONVERSATION_DOCKET_WAITING_ON,
            },
            sourceIds: [input.currentMessage.sourceId],
          },
        });
      }
      if (input.currentMessage.text === PRIVATE_CONVERSATION_DOCKET_HANDLED) {
        const candidate = input.householdDocket.items.find(
          (item) => item.summary === PRIVATE_CONVERSATION_DOCKET_SUMMARY,
        );
        if (candidate?.visibility !== "private") {
          throw new Error("The owner-private Message docket item was not available to resolve");
        }
        return decision({
          bubbles: [{ text: PRIVATE_CONVERSATION_DOCKET_HANDLED_ACK, delayMs: 0 }],
          docketCompletions: [candidate.candidateId],
        });
      }
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
      if (input.currentMessage.text === retainedSourceRecallRequest) {
        if (!reads.searchSources) {
          throw new Error("The private turn did not receive retained-source search");
        }
        const search = await reads.searchSources({ query: "code 123456", cursor: null });
        const result = search.results.find((candidate) => candidate.kind === "gmail");
        if (!result?.match.includes("123456")) {
          throw new Error("The retained Gmail source was not discoverable by its exact contents");
        }
        const source = await reads.readSource({ sourceId: result.sourceId });
        retainedPrivateSourceWasRead =
          source?.kind === "gmail" &&
          source.visibility === "adult_private" &&
          source.text.includes("Archived owner-private account notice") &&
          source.text.includes("Code 123456");
        if (!retainedPrivateSourceWasRead) {
          throw new Error("The retained Gmail search hit could not be read with its private content");
        }
        return decision({ bubbles: [{ text: retainedSourceRecallReply, delayMs: 0 }] });
      }
      if (input.currentMessage.text === groupRetainedSourceRequest) {
        groupSourceSearchWasHidden = reads.searchSources === undefined;
        if (!groupSourceSearchWasHidden) {
          throw new Error("A group turn received private retained-source search");
        }
        return decision({ bubbles: [{ text: groupRetainedSourceReply, delayMs: 0 }] });
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
      if (input.currentMessage.text === GOOGLE_RECIPE_QUESTION) {
        const matches = await reads.searchFamilyMemory?.({
          query: "mild hectic night noodle dinner instructions",
          limit: 5,
        });
        googleRecipeSearchReturnedUsableDetails =
          matches?.some(
            (source) =>
              source.kind === "memory" &&
              source.text.includes(GOOGLE_RECIPE_TITLE) &&
              source.text.includes("12 ounces spaghetti") &&
              source.text.includes("keep it mild"),
          ) ?? false;
        if (!googleRecipeSearchReturnedUsableDetails) {
          throw new Error("The Google-derived recipe did not return usable details through memory search");
        }
        return decision({ bubbles: [{ text: GOOGLE_RECIPE_REPLY, delayMs: 0 }] });
      }
      if (input.currentMessage.text === GOOGLE_CITED_REPLY_QUESTION) {
        const connection = input.googleConnections.find((candidate) => candidate.kind === "personal");
        if (!connection) throw new Error("The private Gmail connection is missing");
        const [source] = (
          await reads.searchGmail({
            query: GOOGLE_CITED_REPLY_QUERY,
            limit: 10,
          })
        ).sources;
        if (!source?.text.includes("emergency card")) {
          throw new Error("The conversational Gmail read returned no usable evidence");
        }
        conversationalGoogleSourceId = source.sourceId;
        return decision({ bubbles: [{ text: GOOGLE_CITED_REPLY, delayMs: 5_000 }] });
      }
      if (input.currentMessage.text === ORDINARY_UNUSED_GMAIL_QUESTION) {
        const connection = input.googleConnections.find((candidate) => candidate.kind === "personal");
        if (!connection) throw new Error("The private Gmail connection is missing");
        const [source] = (
          await reads.searchGmail({
            query: ORDINARY_UNUSED_GMAIL_QUERY,
            limit: 10,
          })
        ).sources;
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
          facts: [
            remember(
              "Maya likes soccer",
              input.currentMessage.sourceId,
              input.audience === "group" ? "household" : "private",
            ),
          ],
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
        bubbles: [{ text: "I found the deadline and added the unsigned form to the docket.", delayMs: 0 }],
        facts: [
          remember(
            "Maya’s field-trip form is due Tuesday",
            input.currentMessage.sourceId,
            input.audience === "group" ? "household" : "private",
          ),
        ],
        docketUpsert: {
          operation: "create",
          candidateId: null,
          candidate: {
            category: "deadline",
            summary: NATIVE_DOCKET_SUMMARY,
            urgency: "soon",
            dueAt: new Date(Date.parse(input.currentMessage.occurredAt) + 60 * 60_000).toISOString(),
            needsAnswer: true,
            owner: NATIVE_DOCKET_OWNER,
            nextAction: NATIVE_DOCKET_NEXT_ACTION,
            waitingOn: NATIVE_DOCKET_WAITING_ON,
          },
          sourceIds: [input.currentMessage.sourceId],
        },
      });
    };
    const harness = await createHarness(householdReasoner, {
      continueFamilyWork: async (input, reads) => {
        if (input.objective === NATIVE_DOCKET_WORK_OBJECTIVE) {
          expect(input.visibility).toBe("household");
          expect(input.ownerAdultId).toBeNull();
          expect(input.initiatingAdultId).toBe(harness.partnerAdultId);
          nativeDocketWorkOriginStayedCurrent =
            input.origin.message.speaker === harness.partnerAdultId &&
            input.origin.message.text === NATIVE_DOCKET_WORK_REQUEST;
          if (!nativeDocketWorkOriginStayedCurrent) {
            throw new Error("Older docket evidence replaced the current work-request origin");
          }
          const linkedMessages = input.linkedSources?.filter((source) => source.kind === "message") ?? [];
          const linkedDocuments = input.linkedSources?.filter((source) => source.kind === "document") ?? [];
          const schoolMessage = linkedMessages.find((source) => source.message.text?.includes(NATIVE_TEXT));
          const correctionMessage = linkedMessages.find(
            (source) => source.message.text === NATIVE_DOCKET_CORRECTION,
          );
          const schoolPdf = linkedDocuments.find(
            (source) => source.document.filename === "field-trip-form.pdf",
          );
          if (!schoolMessage || !correctionMessage || !schoolPdf || !reads.readSource) {
            throw new Error("The reconciled docket did not preserve its exact Message/PDF lineage");
          }
          const openedMessage = await reads.readSource({ sourceId: schoolMessage.sourceId });
          const imageReference = schoolMessage.message.images[0];
          if (!imageReference || !reads.readCurrentImage || !reads.readCurrentPdf) {
            throw new Error("The durable worker lost the school photo or PDF reader");
          }
          if (imageReference.mimeType === "image/heic") {
            throw new Error("The retained school photo was not normalized");
          }
          const image = await reads.readCurrentImage({
            assetId: imageReference.assetId,
            mimeType: imageReference.mimeType,
          });
          const pdf = await reads.readCurrentPdf({
            documentId: schoolPdf.document.id,
            filename: schoolPdf.document.filename,
            mimeType: schoolPdf.document.mimeType,
            contentDigest: schoolPdf.document.contentDigest,
          });
          nativeDocketWorkReadExactEvidence =
            openedMessage?.kind === "message" &&
            openedMessage.text.includes(NATIVE_TEXT) &&
            openedMessage.text.includes(NATIVE_LINK) &&
            openedMessage.text.includes(VOICE_TRANSCRIPT) &&
            Buffer.from(image.bytes).equals(Buffer.from(JPEG_BYTES)) &&
            Buffer.from(pdf.bytes).equals(Buffer.from(PDF_BYTES));
          if (!nativeDocketWorkReadExactEvidence) {
            throw new Error("The durable worker could not reopen the exact retained family evidence");
          }
          return {
            kind: "terminal",
            state: {
              ...input.state,
              progressRevision: input.state.progressRevision + 1,
              phase: "terminal",
              claim: null,
              pendingCall: null,
              terminal: { outcome: "succeeded", text: NATIVE_DOCKET_WORK_RESULT },
            },
            outcome: "succeeded",
            text: NATIVE_DOCKET_WORK_RESULT,
          };
        }
        if (input.objective !== docketWorkObjective) {
          throw new Error(`Unexpected family work in docket narrative: ${input.objective}`);
        }
        expect(input.visibility).toBe("household");
        expect(input.ownerAdultId).toBeNull();
        expect(input.initiatingAdultId).toBe(harness.partnerAdultId);
        expect(input.state.docketCandidateIds).toHaveLength(1);
        expect(reads.searchSources).toBeUndefined();
        expect(input.visibleSources?.some((source) => source.kind === "gmail")).toBe(false);
        expect(input.linkedSources).toEqual([
          expect.objectContaining({ kind: "gmail", sourceId: expect.any(String) }),
        ]);
        const linkedSource = input.linkedSources?.[0];
        if (!linkedSource) throw new Error("The docket-grounded work lost its linked source ID");
        if (!reads.readSource) throw new Error("The durable worker did not receive exact source reading");
        const source = await reads.readSource({ sourceId: linkedSource.sourceId });
        docketWorkReadExactLinkedSource =
          source?.kind === "gmail" &&
          source.visibility === "adult_private" &&
          source.text.includes("Hari private email") &&
          source.text.includes("form needs a signature");
        if (!docketWorkReadExactLinkedSource) {
          throw new Error("The household worker could not open the exact selected docket source");
        }
        return {
          kind: "terminal",
          state: {
            ...input.state,
            progressRevision: input.state.progressRevision + 1,
            phase: "terminal",
            claim: null,
            pendingCall: null,
            waitingDocket: null,
            terminal: {
              outcome: "partial",
              text: docketWorkResult,
              docket: {
                owner: "Parents",
                nextAction: docketWorkNextAction,
                waitingOn: docketWorkWaitingOn,
                needsAnswer: true,
              },
            },
          },
          outcome: "partial",
          text: docketWorkResult,
          docket: {
            owner: "Parents",
            nextAction: docketWorkNextAction,
            waitingOn: docketWorkWaitingOn,
            needsAnswer: true,
          },
        };
      },
    });
    harness.state.initialGoogleFailuresRemaining = 2;
    harness.state.initialClassifierFailuresRemaining = 1;
    harness.state.completeScanPaginationExercise = true;
    harness.state.founderProductRecenterReview = true;
    harness.state.googleRecipeArtifactExercise = true;
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
    expect(founderModelReviewJson).toContain("Code 123456");
    expect(founderModelReviewJson).toContain(
      "https://example.test/reset?token=fake-only-token-value-1234567890",
    );
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
    expect(incompleteWork.candidates).toHaveLength(6);
    expect(incompleteWork.candidates.length).toBeGreaterThan(3);
    expect(incompleteWork.candidates.filter((candidate) => candidate.category === "conflict")).toHaveLength(
      2,
    );
    expect(incompleteWork.candidates.map((candidate) => candidate.category)).toEqual([
      "deadline",
      "loose_end",
      "conflict",
      "conflict",
      "handoff",
      "family_date",
    ]);
    expect(
      incompleteWork.candidates.find((candidate) => candidate.summary === FOUNDER_FORM_SUMMARY),
    ).toMatchObject({
      owner: "Hari",
      nextAction: FOUNDER_FORM_NEXT_ACTION,
      waitingOn: FOUNDER_FORM_WAITING_ON,
      needsAnswer: true,
    });
    expect(
      incompleteWork.candidates.find((candidate) => candidate.summary === PARTNER_PERMISSION_SUMMARY),
    ).toMatchObject({
      owner: "Alex",
      nextAction: PARTNER_PERMISSION_NEXT_ACTION,
      waitingOn: PARTNER_PERMISSION_WAITING_ON,
      needsAnswer: true,
    });
    const unknownCandidateId = "99999999-9999-4999-8999-999999999999";
    await expect(
      harness.store.completeHouseholdInitialBriefing({
        workId: incompleteWork.workId,
        selectedCandidateIds: [unknownCandidateId],
        familyCalendarCursor: "{}",
        bubbles: [
          {
            text: "I found one thing that seems worth sorting out together.",
            delayMs: 0,
          },
        ],
        occurredAt: harness.iso(),
      }),
    ).rejects.toThrow(/outside the current docket/i);
    const firstRankedCandidate = incompleteWork.candidates[0];
    const secondRankedCandidate = incompleteWork.candidates[1];
    if (!firstRankedCandidate || !secondRankedCandidate) {
      throw new Error("The household briefing order regression needs two candidates");
    }
    const outOfOrderCandidates = [secondRankedCandidate, firstRankedCandidate];
    await expect(
      harness.store.completeHouseholdInitialBriefing({
        workId: incompleteWork.workId,
        selectedCandidateIds: outOfOrderCandidates.map((candidate) => candidate.candidateId),
        familyCalendarCursor: "{}",
        bubbles: [
          {
            text: `Here’s what’s on the docket:\n${outOfOrderCandidates
              .map((candidate) => `• ${candidate.summary}`)
              .join("\n")}\n\nDid I get that right? If I missed something, tell me here.`,
            delayMs: 0,
          },
        ],
        occurredAt: harness.iso(),
      }),
    ).rejects.toThrow(/ranked order of the current docket/i);
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
    const rankedCandidates = incompleteWork.candidates.slice(0, 3);
    const rankedSummaries = rankedCandidates.map((candidate) => candidate.summary);
    const extraCandidate = incompleteWork.candidates[3];
    const firstRankedSummary = rankedSummaries[0];
    if (!extraCandidate || !firstRankedSummary) {
      throw new Error("The household briefing grounding regression needs four candidates");
    }
    const naturalBriefing = (summaries: readonly string[]): string =>
      `Here are the things I’d put at the top: ${summaries.join(" ")}\n\nAnything I missed? Tell me and I’ll fix it.`;
    for (const invalidSummaries of [
      rankedSummaries.slice(0, -1),
      [...rankedSummaries, firstRankedSummary],
      [...rankedSummaries, extraCandidate.summary],
    ]) {
      await expect(
        harness.store.completeHouseholdInitialBriefing({
          workId: incompleteWork.workId,
          selectedCandidateIds: rankedCandidates.map((candidate) => candidate.candidateId),
          familyCalendarCursor: "{}",
          bubbles: [{ text: naturalBriefing(invalidSummaries), delayMs: 0 }],
          occurredAt: harness.iso(),
        }),
      ).rejects.toThrow(/distinct finding/i);
    }
    await harness.drain();
    const [householdBriefingInput] = harness.state.briefings;
    if (!householdBriefingInput) throw new Error("The household briefing reasoner did not run");
    expect(householdBriefingInput.memory).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          slot: GOOGLE_RECIPE_SLOT,
          label: GOOGLE_RECIPE_TITLE,
          text: expect.stringContaining("12 ounces spaghetti"),
        }),
      ]),
    );

    const briefingMessages = harness.linq.messages.filter(
      (message) =>
        message.providerConversationId === FAMILY_GROUP &&
        message.idempotencyKey.startsWith("initial-household-briefing:"),
    );
    expect(briefingMessages).toHaveLength(1);
    const briefing = briefingMessages.map((message) => message.text).join("\n");
    for (const summary of rankedCandidates.map((candidate) => candidate.summary)) {
      expect(briefing.split(summary)).toHaveLength(2);
    }
    for (const summary of [SCHOOL_HANDOFF_SUMMARY, FAMILY_MEETING_SUMMARY]) {
      expect(briefing).not.toContain(summary);
    }
    expect(briefing).toContain("I kept 3 lower-priority items on the docket too. Ask me anytime.");
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
      "The ranked household briefing lost its selected Google evidence metadata",
      `exists (
        select 1 from messages message join sources source on source.id=message.source_id
        where message.idempotency_key like 'initial-household-briefing:%'
          and source.metadata->'privateConflictOwnerAdultIds'
            is not null
          and jsonb_array_length(source.metadata->'privateConflictOwnerAdultIds')=1
          and jsonb_array_length(source.metadata->'googleSourceIds')>=1
          and jsonb_array_length(source.metadata->'googleActionKeys')>=3
      )`,
    );

    const retainedDocket = await harness.store.readHouseholdDocket({
      householdId: incompleteWork.household.householdId,
      limit: 20,
      now: harness.iso(),
    });
    expect(retainedDocket.totalItems).toBe(6);
    expect(retainedDocket.items).toHaveLength(6);
    const founderPrivateDocket = await harness.store.readHouseholdDocket({
      householdId: incompleteWork.household.householdId,
      viewerAdultId: harness.founderAdultId,
      limit: 20,
      now: harness.iso(),
    });
    expect(founderPrivateDocket.items.map((candidate) => candidate.summary)).toContain(
      PRIVATE_INITIAL_ONLY_FINDING,
    );
    const partnerPrivateDocket = await harness.store.readHouseholdDocket({
      householdId: incompleteWork.household.householdId,
      viewerAdultId: harness.partnerAdultId,
      limit: 20,
      now: harness.iso(),
    });
    expect(partnerPrivateDocket.items.map((candidate) => candidate.summary)).not.toContain(
      PRIVATE_INITIAL_ONLY_FINDING,
    );
    const founderVaultDocket = (await harness.florence.workspaceForAdult(harness.founderAdultId)).vault
      ?.docket;
    expect(founderVaultDocket?.totalItems).toBe(founderPrivateDocket.totalItems);
    expect(
      founderVaultDocket?.items.find((candidate) => candidate.summary === PRIVATE_INITIAL_ONLY_FINDING),
    ).toMatchObject({
      visibility: "private",
      owner: "You",
      nextAction: "Confirm the school office contact details.",
      waitingOn: "Your confirmation",
      needsAnswer: true,
    });
    expect(
      founderVaultDocket?.items.find((candidate) => candidate.summary === FOUNDER_FORM_SUMMARY),
    ).toMatchObject({
      visibility: "household",
      owner: "Hari",
      nextAction: FOUNDER_FORM_NEXT_ACTION,
      waitingOn: FOUNDER_FORM_WAITING_ON,
      needsAnswer: true,
    });
    const partnerVaultDocket = (await harness.florence.workspaceForAdult(harness.partnerAdultId)).vault
      ?.docket;
    expect(partnerVaultDocket?.items.map((candidate) => candidate.summary)).not.toContain(
      PRIVATE_INITIAL_ONLY_FINDING,
    );
    await harness.accept("private", "private-docket-recall", PRIVATE_DOCKET_RECALL_REQUEST);
    await harness.drain();
    expect(harness.linq.messages.some((message) => message.text === PRIVATE_DOCKET_RECALL_REPLY)).toBe(true);
    await harness.accept("private", "private-docket-handled", PRIVATE_DOCKET_HANDLED_REQUEST);
    await harness.drain();
    expect(harness.linq.messages.some((message) => message.text === PRIVATE_DOCKET_HANDLED_REPLY)).toBe(true);
    expect(
      (
        await harness.store.readHouseholdDocket({
          householdId: incompleteWork.household.householdId,
          viewerAdultId: harness.founderAdultId,
          limit: 20,
          now: harness.iso(),
        })
      ).items.map((candidate) => candidate.summary),
    ).not.toContain(PRIVATE_INITIAL_ONLY_FINDING);
    await harness.accept("group", "household-docket", HOUSEHOLD_DOCKET_REQUEST, "partner");
    await harness.drain();
    expect(observedHouseholdDocket.value?.totalItems).toBe(6);
    expect(observedHouseholdDocket.value?.items).toHaveLength(6);
    expect(observedHouseholdDocket.value?.items.slice(0, 3).map((candidate) => candidate.category)).toEqual([
      "deadline",
      "loose_end",
      "conflict",
    ]);
    expect(
      observedHouseholdDocket.value?.items.find(
        (candidate) => candidate.summary === PARTNER_PERMISSION_SUMMARY,
      ),
    ).toMatchObject({
      owner: "Alex",
      nextAction: PARTNER_PERMISSION_NEXT_ACTION,
      waitingOn: PARTNER_PERMISSION_WAITING_ON,
      needsAnswer: true,
    });
    expect(harness.linq.messages.some((message) => message.text === HOUSEHOLD_DOCKET_REPLY)).toBe(true);
    await harness.accept("group", "docket-grounded-family-work", docketWorkRequest, "partner");
    await harness.drain();
    expect(harness.linq.messages.some((message) => message.text === docketWorkAcknowledgement)).toBe(true);
    harness.state.now += 1_001;
    await harness.drain();
    expect(docketWorkReadExactLinkedSource).toBe(true);
    expect(harness.linq.messages.some((message) => message.text === docketWorkResult)).toBe(true);
    expect(
      (
        await harness.store.readHouseholdDocket({
          householdId: incompleteWork.household.householdId,
          limit: 20,
          now: harness.iso(),
        })
      ).items.find((candidate) => candidate.summary === FOUNDER_FORM_SUMMARY),
    ).toMatchObject({
      owner: "Parents",
      nextAction: docketWorkNextAction,
      waitingOn: docketWorkWaitingOn,
      needsAnswer: true,
    });
    await harness.assertDatabase(
      "Docket-grounded household work lost its exact Message/source lineage or attached unrelated evidence",
      `exists (
          select 1 from proactive_work work
          where work.kind='family_task' and work.objective=${sqlLiteral(docketWorkObjective)}
            and work.visibility='household' and work.owner_adult_id is null
            and work.status='completed'
        ) and exists (
          select 1 from proactive_work work
          join proactive_work_sources link on link.work_id=work.id
          join messages message on message.source_id=link.source_id
          where work.kind='family_task' and work.objective=${sqlLiteral(docketWorkObjective)}
            and message.provider_message_id='message-docket-grounded-family-work'
            and message.direction='inbound'
        ) and (select count(*)=1 from proactive_work work
          join proactive_work_sources link on link.work_id=work.id
          join sources source on source.id=link.source_id
          where work.kind='family_task' and work.objective=${sqlLiteral(docketWorkObjective)}
            and source.kind in ('gmail','calendar')
            and source.owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
            and source.metadata->>'messageId'=${sqlLiteral(SCHOOL_ATTACHMENT.messageId)})
        and not exists (
          select 1 from proactive_work work
          join proactive_work_sources link on link.work_id=work.id
          join sources source on source.id=link.source_id
          where work.kind='family_task' and work.objective=${sqlLiteral(docketWorkObjective)}
            and source.kind in ('gmail','calendar')
            and source.owner_adult_id=${sqlLiteral(harness.partnerAdultId)}::uuid
        )`,
    );
    await harness.accept("group", "household-docket-handled", HOUSEHOLD_DOCKET_HANDLED, "partner");
    await harness.drain();
    expect(harness.linq.messages.some((message) => message.text === HOUSEHOLD_DOCKET_HANDLED_ACK)).toBe(true);
    const docketAfterCompletion = await harness.store.readHouseholdDocket({
      householdId: incompleteWork.household.householdId,
      limit: 20,
      now: harness.iso(),
    });
    expect(docketAfterCompletion.totalItems).toBe(5);
    expect(docketAfterCompletion.items.map((candidate) => candidate.summary)).not.toContain(
      PARTNER_PERMISSION_SUMMARY,
    );
    expect(docketAfterCompletion.items.map((candidate) => candidate.summary)).toContain(FOUNDER_FORM_SUMMARY);
    expect(
      (await harness.florence.workspaceForAdult(harness.partnerAdultId)).vault?.docket.items.map(
        (candidate) => candidate.summary,
      ),
    ).not.toContain(PARTNER_PERMISSION_SUMMARY);
    await harness.assertDatabase(
      "The handled docket item did not retain its durable Google action identity",
      `exists (
        select 1 from messages message join sources source on source.id=message.source_id
        where message.provider_message_id='message-household-docket-handled'
          and jsonb_typeof(source.metadata->'completedGoogleActionKeys')='array'
          and jsonb_array_length(source.metadata->'completedGoogleActionKeys')>=1
      )`,
    );

    const founderAfterInitialReview = await harness.florence.workspaceForAdult(harness.founderAdultId);
    expect(founderAfterInitialReview.vault?.watches).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: "monitor",
          currentConclusion: "The form still needs a parent signature.",
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
    expect(
      founderAfterInitialReview.vault?.watches.some(
        (watch) => watch.currentConclusion === PRIVATE_INITIAL_ONLY_FINDING,
      ),
    ).toBe(false);
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
    const founderRecipe = founderAfterInitialReview.vault?.facts.find(
      (fact) => fact.statement === GOOGLE_RECIPE_STATEMENT,
    );
    expect(founderRecipe).toMatchObject({
      memoryKind: "artifact",
      artifactKind: "recipe",
      title: GOOGLE_RECIPE_TITLE,
      details: GOOGLE_RECIPE_DETAILS,
      tags: ["recipe", "noodles", "weeknight"],
      visibility: "household",
      source: { kind: "gmail" },
    });
    if (!founderRecipe) throw new Error("The Google-derived recipe artifact was not retained");
    expect(partnerAfterInitialReview.vault?.facts.find((fact) => fact.id === founderRecipe.id)).toEqual(
      expect.objectContaining({
        memoryKind: "artifact",
        artifactKind: "recipe",
        title: GOOGLE_RECIPE_TITLE,
        details: GOOGLE_RECIPE_DETAILS,
        visibility: "household",
        source: null,
      }),
    );
    await harness.accept("private", "recall-google-recipe", GOOGLE_RECIPE_QUESTION, "partner");
    await harness.drain();
    expect(googleRecipeSearchReturnedUsableDetails).toBe(true);
    expect(harness.linq.messages.some((message) => message.text === GOOGLE_RECIPE_REPLY)).toBe(true);
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
    expect(publicResearchModelTurns).toBe(1);
    expect(publicSearchTurns).toBe(1);
    expect(publicResearchCapture.mainRequest?.tools).toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "web_search" })]),
    );
    expect(publicResearchCapture.mainRequest?.tools).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "function", name: "research_public_web" })]),
    );
    expect(publicResearchCapture.mainRequest?.include).toEqual(
      expect.arrayContaining(["web_search_call.action.sources"]),
    );
    expect(publicResearchCapture.queries[0]).toContain("DL 747");
    expect(publicResearchCapture.queries[0]).not.toMatch(
      /Alex|Maya|Anbarasu|hari@example\.com|familyProfile|gmail/iu,
    );
    expect(publicResearchCapture.sourceUrls[0]).toBe(PUBLIC_RESEARCH_URL);
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
        reaction: "emphasize",
      }),
    ]);
    const publicResearchTimeline = harness.state.timeline.slice(publicResearchTimelineBefore);
    expect(
      publicResearchTimeline.indexOf("reaction:emphasize:message-public-identifier-research"),
    ).toBeLessThan(publicResearchTimeline.indexOf(`message:${PUBLIC_RESEARCH_REPLY}`));
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
    expect(publicResearchCapture.queries[1]).toContain("9780143127796");
    expect(publicResearchCapture.queries[1]).toContain("2026-08-27");
    expect(harness.linq.messages.some((message) => message.text === PUBLIC_NO_RESULT_REPLY)).toBe(true);
    expect(harness.linq.messages.some((message) => message.text === PUBLIC_NO_RESULT_SOURCE)).toBe(false);
    expect(harness.linq.reactions.slice(reactionsBeforeNoResult)).toEqual([]);
    const searchesBeforeShortIdentifier = publicSearchTurns;
    await harness.accept("group", "public-short-identifier", PUBLIC_SHORT_IDENTIFIER_REQUEST, "partner");
    await harness.drain();
    expect(publicSearchTurns).toBe(searchesBeforeShortIdentifier + 1);
    expect(publicResearchCapture.queries[2]).toContain("X public social platform identifier");
    expect(harness.linq.messages.some((message) => message.text === PUBLIC_SHORT_IDENTIFIER_REPLY)).toBe(
      true,
    );
    expect(harness.linq.messages.some((message) => message.text === PUBLIC_SHORT_IDENTIFIER_URL)).toBe(true);
    const searchesBeforePublicConcept = publicSearchTurns;
    await harness.accept("group", "public-concept-search", PUBLIC_CONCEPT_REQUEST, "partner");
    await harness.drain();
    expect(publicSearchTurns).toBe(searchesBeforePublicConcept + 1);
    expect(publicResearchCapture.queries[3]).toContain("access token");
    expect(publicResearchCapture.queries[3]).toContain("password managers");
    expect(publicResearchCapture.queries[3]).toContain("confirmation code format");
    expect(publicResearchCapture.queries[3]).toContain(PUBLIC_CONCEPT_URL);
    expect(harness.linq.messages.some((message) => message.text === PUBLIC_CONCEPT_REPLY)).toBe(true);
    const reactionsAfterPublicResearch = harness.linq.reactions.length;
    await harness.accept("group", "clarification-only", CLARIFICATION_ONLY_REQUEST, "partner");
    await harness.drain();
    expect(harness.linq.reactions).toHaveLength(reactionsAfterPublicResearch);
    expect(harness.linq.messages.some((message) => message.text === CLARIFICATION_ONLY_REPLY)).toBe(true);

    await harness.accept("private", "retained-private-google-source", retainedSourceRecallRequest);
    await harness.drain();
    expect(retainedPrivateSourceWasRead).toBe(true);
    expect(harness.linq.messages.some((message) => message.text === retainedSourceRecallReply)).toBe(true);

    await harness.accept("group", "group-private-google-source", groupRetainedSourceRequest, "partner");
    await harness.drain();
    expect(groupSourceSearchWasHidden).toBe(true);
    expect(harness.linq.messages.some((message) => message.text === groupRetainedSourceReply)).toBe(true);

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
    const unrelatedAccountMessages = harness.linq.messages.slice(messagesBeforeUnrelatedAccountEmail);
    expect(
      harness.state.googleAssessments.some((assessment) =>
        assessment.evidence.gmail.sources.some(
          (source) => source.subject === UNRELATED_ACCOUNT_EMAIL_SUBJECT,
        ),
      ),
    ).toBe(true);
    expect(unrelatedAccountMessages).toEqual([
      expect.objectContaining({
        providerConversationId: PRIVATE_FOUNDER,
        text: UNRELATED_ACCOUNT_EMAIL_ALERT,
      }),
    ]);
    expect(
      harness.linq.messages.some(
        (message) =>
          message.providerConversationId === FAMILY_GROUP && message.text === UNRELATED_ACCOUNT_EMAIL_ALERT,
      ),
    ).toBe(false);
    await harness.assertDatabase(
      "An actionable owner-private account email was not retained for its owner",
      `exists (
        select 1 from sources
        where kind='gmail' and visibility='private'
          and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and metadata->>'messageId'='gmail-unrelated-retail-account-alert'
      )`,
    );
    await harness.assertDatabase(
      "An actionable owner-private account email did not create its private monitor",
      `exists (
        select 1 from proactive_work
        where kind='finite_monitor' and visibility='private'
          and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and objective=${sqlLiteral(UNRELATED_ACCOUNT_MONITOR_OBJECTIVE)}
      )`,
    );
    await harness.assertDatabase(
      "An owner-private account email became a shared Vault fact",
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
      expect.arrayContaining([
        expect.objectContaining({
          slot: PRIVATE_SCHOOL_FACT_SLOT,
          statement: INITIAL_PRIVATE_SCHOOL_FACT,
        }),
        expect.objectContaining({
          slot: GOOGLE_RECIPE_SLOT,
          statement: GOOGLE_RECIPE_STATEMENT,
          memory: expect.objectContaining({
            memoryKind: "artifact",
            artifactKind: "recipe",
            title: GOOGLE_RECIPE_TITLE,
            details: GOOGLE_RECIPE_DETAILS,
          }),
        }),
      ]),
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
      "A dismissed incremental Gmail overlap was lost or linked to the poll",
      `exists (
        select 1 from sources
        where id=${sqlLiteral(harness.state.overlapGmailSourceId)}::uuid
          and kind='gmail' and visibility='private'
          and owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
      ) and not exists (
        select 1 from proactive_work_sources link
        join proactive_work work on work.id=link.work_id
        where work.kind='personal_google_poll'
          and work.owner_adult_id=${sqlLiteral(harness.founderAdultId)}::uuid
          and link.source_id=${sqlLiteral(harness.state.overlapGmailSourceId)}::uuid
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
    expect(
      workspace.vault?.watches.some(
        (watch) => watch.objective === "Watch for confirmation that Maya’s field-trip form is signed.",
      ),
    ).toBe(false);

    const nativeDocketAfterCreate = await harness.store.readHouseholdDocket({
      householdId: incompleteWork.household.householdId,
      limit: null,
      now: harness.iso(),
    });
    const nativeCreatedItems = nativeDocketAfterCreate.items.filter(
      (candidate) => candidate.summary === NATIVE_DOCKET_SUMMARY,
    );
    expect(nativeCreatedItems).toHaveLength(1);
    expect(nativeCreatedItems[0]).toMatchObject({
      visibility: "household",
      owner: NATIVE_DOCKET_OWNER,
      nextAction: NATIVE_DOCKET_NEXT_ACTION,
      waitingOn: NATIVE_DOCKET_WAITING_ON,
      needsAnswer: true,
    });
    const nativeDocketCandidateId = nativeCreatedItems[0]?.candidateId;
    if (!nativeDocketCandidateId) throw new Error("The native Message created no durable docket identity");
    expect(await harness.vault.purgeExpired(new Date(Date.now() + 48 * 60 * 60_000))).toBe(0);
    await harness.assertDatabase(
      "The native Message docket item did not retain its exact PDF and source-backed identity",
      `exists (
        select 1 from sources source
        where source.id=${sqlLiteral(inboundSourceId("event-native-school-update"))}::uuid
          and source.kind='linq_message' and source.visibility='household'
          and source.metadata->'conversationDocketItem'->>'kind'='conversation_docket_item_v1'
          and source.metadata->'conversationDocketItem'->>'status'='unresolved'
          and source.metadata->'conversationDocketItem'->>'candidateId'=${sqlLiteral(nativeDocketCandidateId)}
          and length(source.metadata->'conversationDocketItem'->>'actionKey')=64
      ) and exists (
        select 1 from sources source join documents document on document.source_id=source.id
        where source.parent_source_id=${sqlLiteral(inboundSourceId("event-native-school-update"))}::uuid
          and document.filename='field-trip-form.pdf' and document.retained=true
          and document.discard_after is null and document.content_envelope is not null
      )`,
    );

    await harness.florence.acceptInbound({
      ...harness.inbound("group", "native-school-correction", NATIVE_DOCKET_CORRECTION),
      replyToProviderMessageId: "message-native-school-update",
    });
    await harness.drain();
    const nativeDocketAfterCorrection = await harness.store.readHouseholdDocket({
      householdId: incompleteWork.household.householdId,
      limit: null,
      now: harness.iso(),
    });
    expect(
      nativeDocketAfterCorrection.items.filter(
        (candidate) =>
          candidate.summary === NATIVE_DOCKET_SUMMARY || candidate.summary === NATIVE_DOCKET_UPDATED_SUMMARY,
      ),
    ).toEqual([
      expect.objectContaining({
        candidateId: nativeDocketCandidateId,
        summary: NATIVE_DOCKET_UPDATED_SUMMARY,
        visibility: "household",
        owner: NATIVE_DOCKET_OWNER,
        nextAction: NATIVE_DOCKET_UPDATED_NEXT_ACTION,
        waitingOn: NATIVE_DOCKET_WAITING_ON,
        needsAnswer: true,
      }),
    ]);
    await harness.assertDatabase(
      "The semantic correction forked the Message docket item or lost its exact evidence lineage",
      `(
        select count(*)=1 from sources source
        where source.metadata->'conversationDocketItem'->>'candidateId'=${sqlLiteral(nativeDocketCandidateId)}
      ) and exists (
        select 1 from sources source
        where source.id=${sqlLiteral(inboundSourceId("event-native-school-update"))}::uuid
          and source.metadata->'conversationDocketItem'->>'summary'=${sqlLiteral(NATIVE_DOCKET_UPDATED_SUMMARY)}
          and source.metadata->'conversationDocketItem'->'sourceIds'
            @> ${sqlLiteral(
              JSON.stringify([
                inboundSourceId("event-native-school-update"),
                inboundSourceId("event-native-school-correction"),
              ]),
            )}::jsonb
      )`,
    );

    await harness.accept("group", "native-docket-work", NATIVE_DOCKET_WORK_REQUEST, "partner");
    await harness.drain();
    expect(harness.linq.messages.some((message) => message.text === NATIVE_DOCKET_WORK_ACK)).toBe(true);
    const nativeDocketDuringWork = await harness.store.readHouseholdDocket({
      householdId: incompleteWork.household.householdId,
      limit: null,
      now: harness.iso(),
    });
    expect(
      nativeDocketDuringWork.items.find((candidate) => candidate.candidateId === nativeDocketCandidateId),
    ).toMatchObject({
      summary: NATIVE_DOCKET_UPDATED_SUMMARY,
      visibility: "household",
      owner: "Florence",
      nextAction: NATIVE_DOCKET_WORK_OBJECTIVE,
      waitingOn: null,
      needsAnswer: false,
    });
    harness.state.now += 24 * 60 * 60_000 + 1_001;
    await harness.drain();
    expect(nativeDocketWorkOriginStayedCurrent).toBe(true);
    expect(nativeDocketWorkReadExactEvidence).toBe(true);
    expect(harness.linq.messages.some((message) => message.text === NATIVE_DOCKET_WORK_RESULT)).toBe(true);
    expect(
      (
        await harness.store.readHouseholdDocket({
          householdId: incompleteWork.household.householdId,
          limit: null,
          now: harness.iso(),
        })
      ).items.some((candidate) => candidate.candidateId === nativeDocketCandidateId),
    ).toBe(false);
    await harness.assertDatabase(
      "Docket work changed the requesting adult or lost/replaced the exact Message, link, photo, or PDF lineage",
      `exists (
        select 1 from proactive_work work
        where work.kind='family_task' and work.objective=${sqlLiteral(NATIVE_DOCKET_WORK_OBJECTIVE)}
          and work.visibility='household' and work.owner_adult_id is null and work.status='completed'
      ) and (
        select count(*)=4 from proactive_work work
        join proactive_work_sources link on link.work_id=work.id
        where work.kind='family_task' and work.objective=${sqlLiteral(NATIVE_DOCKET_WORK_OBJECTIVE)}
      ) and (
        select count(*)=3 from proactive_work work
        join proactive_work_sources link on link.work_id=work.id
        join messages message on message.source_id=link.source_id
        where work.kind='family_task' and work.objective=${sqlLiteral(NATIVE_DOCKET_WORK_OBJECTIVE)}
          and message.provider_message_id in (
            'message-native-school-update','message-native-school-correction','message-native-docket-work'
          )
      ) and exists (
        select 1 from proactive_work work
        join proactive_work_sources link on link.work_id=work.id
        join documents document on document.source_id=link.source_id
        where work.kind='family_task' and work.objective=${sqlLiteral(NATIVE_DOCKET_WORK_OBJECTIVE)}
          and document.filename='field-trip-form.pdf' and document.retained=true
          and document.discard_after is null
      ) and exists (
        select 1 from messages outbound
        where outbound.direction='outbound' and outbound.text=${sqlLiteral(NATIVE_DOCKET_WORK_RESULT)}
          and outbound.reply_to_source_id=${sqlLiteral(inboundSourceId("event-native-docket-work"))}::uuid
      )`,
    );

    await harness.assertDatabase(
      "Successful family work did not resolve its conversation-origin docket item",
      `exists (
        select 1 from sources source
        where source.id=${sqlLiteral(inboundSourceId("event-native-school-update"))}::uuid
          and source.metadata->'conversationDocketItem'->>'candidateId'=${sqlLiteral(nativeDocketCandidateId)}
          and source.metadata->'conversationDocketItem'->>'status'='resolved'
          and exists (
            select 1 from messages resolution
            where resolution.source_id=(source.metadata->'conversationDocketItem'->>'resolutionSourceId')::uuid
              and resolution.direction='outbound' and resolution.text=${sqlLiteral(NATIVE_DOCKET_WORK_RESULT)}
          )
      )`,
    );

    await harness.accept("private", "private-conversation-docket", PRIVATE_CONVERSATION_DOCKET_REQUEST);
    await harness.drain();
    const founderConversationDocket = (await harness.florence.workspaceForAdult(harness.founderAdultId)).vault
      ?.docket.items;
    const partnerConversationDocket = (await harness.florence.workspaceForAdult(harness.partnerAdultId)).vault
      ?.docket.items;
    expect(
      founderConversationDocket?.find(
        (candidate) => candidate.summary === PRIVATE_CONVERSATION_DOCKET_SUMMARY,
      ),
    ).toMatchObject({
      visibility: "private",
      owner: "You",
      nextAction: PRIVATE_CONVERSATION_DOCKET_NEXT_ACTION,
      waitingOn: PRIVATE_CONVERSATION_DOCKET_WAITING_ON,
      needsAnswer: true,
    });
    expect(
      partnerConversationDocket?.some(
        (candidate) => candidate.summary === PRIVATE_CONVERSATION_DOCKET_SUMMARY,
      ),
    ).toBe(false);
    await harness.accept(
      "private",
      "private-conversation-docket-handled",
      PRIVATE_CONVERSATION_DOCKET_HANDLED,
    );
    await harness.drain();
    expect(
      harness.linq.messages.some((message) => message.text === PRIVATE_CONVERSATION_DOCKET_HANDLED_ACK),
    ).toBe(true);
    expect(
      (await harness.florence.workspaceForAdult(harness.founderAdultId)).vault?.docket.items.some(
        (candidate) => candidate.summary === PRIVATE_CONVERSATION_DOCKET_SUMMARY,
      ),
    ).toBe(false);

    const voicedInterest = await harness.florence.acceptInbound({
      ...harness.inbound("group", "soccer-interest", INTEREST_REQUEST, "partner"),
      authoredText: null,
      voiceTranscriptPresent: true,
    });
    expect(voicedInterest).not.toBeNull();
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
    expect(
      harness.linq.messages.some((message) => message.text.startsWith("One more thing from the review:")),
    ).toBe(false);

    const initialBoundaryHarness = await createHarness();
    initialBoundaryHarness.state.initialUnrelatedAccountReview = true;
    await initialBoundaryHarness.readyHousehold();
    expect(
      initialBoundaryHarness.linq.messages.some(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER &&
          message.text.includes(UNRELATED_ACCOUNT_EMAIL_ALERT),
      ),
    ).toBe(true);
    expect(
      initialBoundaryHarness.linq.messages.some(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          (message.text === UNRELATED_ACCOUNT_EMAIL_ALERT ||
            message.text.includes("retail account password changed")),
      ),
    ).toBe(false);
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
    ).toBe(true);
    await initialBoundaryHarness.assertDatabase(
      "An actionable owner-private initial Google finding was not retained privately",
      `exists (
        select 1 from sources
        where kind='gmail' and visibility='private'
          and owner_adult_id=${sqlLiteral(initialBoundaryHarness.founderAdultId)}::uuid
          and metadata->>'messageId'='gmail-initial-unrelated-retail-account-alert'
      ) and not exists (
        select 1 from facts where slot=${sqlLiteral(UNRELATED_ACCOUNT_FACT_SLOT)}
      ) and exists (
        select 1 from proactive_work
        where kind='finite_monitor' and visibility='private'
          and owner_adult_id=${sqlLiteral(initialBoundaryHarness.founderAdultId)}::uuid
          and objective=${sqlLiteral(UNRELATED_ACCOUNT_MONITOR_OBJECTIVE)}
      )`,
    );
    const interactiveReadsBeforeDueMonitor = initialBoundaryHarness.state.interactiveGoogleReads;
    initialBoundaryHarness.state.linkedGmailMonitorExercise = true;
    initialBoundaryHarness.state.now += 24 * 60 * 60_000;
    await initialBoundaryHarness.drain();
    expect(initialBoundaryHarness.state.finiteReviews).toBe(1);
    expect(initialBoundaryHarness.state.interactiveGoogleReads).toBe(interactiveReadsBeforeDueMonitor);
    expect(initialBoundaryHarness.state.exactGmailReads).toEqual([
      expect.objectContaining({
        ownerAdultId: initialBoundaryHarness.founderAdultId,
        messageId: "gmail-initial-unrelated-retail-account-alert",
        threadId: "thread-gmail-initial-unrelated-retail-account-alert",
        historyId: "101",
      }),
    ]);

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
      "An owner-private fact-only initial review lost its source or crossed the fact boundary",
      `exists (
        select 1 from sources
        where kind='gmail' and visibility='private'
          and owner_adult_id=${sqlLiteral(initialFactOnlyBoundaryHarness.founderAdultId)}::uuid
          and metadata->>'messageId'='gmail-initial-unrelated-retail-account-alert'
      ) and exists (
        select 1 from proactive_work_sources link
        join proactive_work work on work.id=link.work_id
        join sources source on source.id=link.source_id
        where work.kind='initial_private_review'
          and work.owner_adult_id=${sqlLiteral(initialFactOnlyBoundaryHarness.founderAdultId)}::uuid
          and source.owner_adult_id=work.owner_adult_id
          and source.metadata->>'messageId'='gmail-initial-unrelated-retail-account-alert'
      ) and not exists (
        select 1 from facts where slot=${sqlLiteral(UNRELATED_ACCOUNT_FACT_SLOT)}
      )`,
    );
  }, 90_000);

  test("keeps private context isolated while both parents can manage shared memory, Calendar, and group repair", async () => {
    const recipeRequest =
      "Save our weeknight sesame noodles recipe for the family: 12 ounces spaghetti, 3 tablespoons soy sauce, 1 tablespoon sesame oil, and 2 teaspoons rice vinegar. Toss while warm.";
    const recipeQuestion = "How do we make that sesame noodle dinner for hectic evenings again?";
    const recipeStatement = "Our family has a reusable weeknight sesame noodles recipe.";
    const recipeTitle = "Weeknight sesame noodles";
    const recipeDetails =
      "Ingredients: 12 ounces spaghetti; 3 tablespoons soy sauce; 1 tablespoon sesame oil; 2 teaspoons rice vinegar. Method: cook the spaghetti and toss it with the sauce ingredients while warm.";
    const recipeReply =
      "Our weeknight sesame noodles use 12 ounces of spaghetti, 3 tablespoons soy sauce, 1 tablespoon sesame oil, and 2 teaspoons rice vinegar; toss everything while warm.";
    let recipeSearchReturnedUsableDetails = false;
    const harness = await createHarness(async (input, reads) => {
      const text = input.currentMessage.text;
      if (text === recipeRequest) {
        return decision({
          facts: [
            {
              operation: "remember",
              factId: null,
              statement: recipeStatement,
              visibility: "household",
              memory: {
                memoryKind: "artifact",
                artifactKind: "recipe",
                title: recipeTitle,
                details: recipeDetails,
                tags: ["recipe", "noodles", "weeknight"],
              },
              sourceIds: [input.currentMessage.sourceId],
            },
          ],
        });
      }
      if (text === recipeQuestion) {
        const matches = await reads.searchFamilyMemory?.({
          query: "hectic evening sesame noodle dinner preparation notes",
          limit: 5,
        });
        recipeSearchReturnedUsableDetails =
          matches?.some(
            (source) =>
              source.kind === "memory" &&
              source.text.includes(recipeTitle) &&
              source.text.includes("12 ounces spaghetti") &&
              source.text.includes("toss it with the sauce ingredients while warm"),
          ) ?? false;
        if (!recipeSearchReturnedUsableDetails) {
          throw new Error("The saved recipe did not return usable details through memory search");
        }
        return decision({ bubbles: [{ text: recipeReply, delayMs: 0 }] });
      }
      if (text === "My private backup contact is Sam.") {
        return decision({ facts: [remember(text, input.currentMessage.sourceId, "private")] });
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
        return decision({ facts: [remember(text, input.currentMessage.sourceId, "household")] });
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
              visibility: "household",
              memory: {
                memoryKind: "fact",
                artifactKind: null,
                title: null,
                details: null,
                tags: [],
              },
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
              visibility: null,
              memory: null,
              sourceIds: [input.currentMessage.sourceId],
            },
          ],
        });
      }
      if (text === "What is on all of my calendars tomorrow?") {
        const catalog = await reads.listCalendars?.();
        if (catalog?.status !== "complete") {
          throw new Error("The private Calendar catalog was unavailable");
        }
        if (catalog.calendars.some((calendar) => calendar.label.includes("Family"))) {
          throw new Error("The Family Calendar leaked into the private Calendar catalog");
        }
        const calendarRead = await reads.readCalendarWindow({
          timeMin: "2026-08-18T07:00:00.000Z",
          timeMax: "2026-08-19T07:00:00.000Z",
          pageSize: 50,
          cursor: null,
          scope: "all",
          calendarRefs: [],
        });
        if (calendarRead.calendars.some((calendar) => calendar.label?.includes("Family"))) {
          throw new Error("The Family Calendar leaked into a private all-Calendar read");
        }
        return decision({
          bubbles: [
            { text: "I checked every personal calendar; your family calendar stayed separate.", delayMs: 0 },
          ],
        });
      }
      if (text === PRIVATE_CALENDAR_OWNER_APPROVAL) {
        return decision({
          bubbles: [{ text: PRIVATE_CALENDAR_OWNER_CONTRADICTION, delayMs: 0 }],
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

    await harness.accept("private", "save-family-recipe", recipeRequest);
    await harness.drain();
    const founderRecipe = (
      await harness.florence.workspaceForAdult(harness.founderAdultId)
    ).vault?.facts.find((fact) => fact.statement === recipeStatement);
    const partnerRecipe = (
      await harness.florence.workspaceForAdult(harness.partnerAdultId)
    ).vault?.facts.find((fact) => fact.id === founderRecipe?.id);
    expect(founderRecipe).toMatchObject({
      memoryKind: "artifact",
      artifactKind: "recipe",
      title: recipeTitle,
      details: recipeDetails,
      tags: ["recipe", "noodles", "weeknight"],
      visibility: "household",
    });
    expect(partnerRecipe).toMatchObject({
      memoryKind: "artifact",
      artifactKind: "recipe",
      title: recipeTitle,
      details: recipeDetails,
      tags: ["recipe", "noodles", "weeknight"],
      visibility: "household",
    });
    await harness.accept("private", "reuse-family-recipe", recipeQuestion, "partner");
    await harness.drain();
    expect(recipeSearchReturnedUsableDetails).toBe(true);
    expect(
      harness.linq.messages.some(
        (message) => message.providerConversationId === PRIVATE_PARTNER && message.text === recipeReply,
      ),
    ).toBe(true);

    await harness.accept("private", "private-all-calendar-read", "What is on all of my calendars tomorrow?");
    await harness.drain();
    const privateAllCalendarReads = harness.state.personalCalendarReads.filter(
      (read) => read.timeMin === "2026-08-18T07:00:00.000Z",
    );
    expect(privateAllCalendarReads).toHaveLength(1);
    expect(privateAllCalendarReads).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          excludedFamilyCalendarId: FAMILY_CALENDAR,
          calendarIds: undefined,
        }),
      ]),
    );
    expect(
      harness.linq.messages.some(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER &&
          message.text === "I checked every personal calendar; your family calendar stayed separate.",
      ),
    ).toBe(true);

    const messagesBeforePrivateCalendarAnniversary = harness.linq.messages.length;
    harness.state.privateCalendarAnniversaryPending = true;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(harness.state.privateCalendarAnniversaryDelivered).toBe(true);
    const privateCalendarAssessment = [...harness.state.googleAssessments]
      .reverse()
      .find((assessment) =>
        assessment.evidence.calendar.events.some(
          (event) => event.title === PRIVATE_CALENDAR_ANNIVERSARY_TITLE,
        ),
      );
    expect(privateCalendarAssessment?.memory).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          label: recipeTitle,
          text: expect.stringContaining(recipeDetails),
        }),
      ]),
    );
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
    expect(
      ownerApprovedMessages.filter(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER &&
          message.text === PRIVATE_CALENDAR_OWNER_APPROVAL_REPLY,
      ),
    ).toHaveLength(1);
    expect(ownerApprovedMessages.map((message) => message.text)).not.toContain(
      PRIVATE_CALENDAR_OWNER_CONTRADICTION,
    );
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

    const voicedHouseholdUpdate = await harness.florence.acceptInbound({
      ...harness.inbound("private", "private-household-update", "Tell Alex that pickup is at 3:15."),
      authoredText: null,
      voiceTranscriptPresent: true,
    });
    expect(voicedHouseholdUpdate).not.toBeNull();
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
    const voicedCalendarCreate = await harness.florence.acceptInbound({
      ...harness.inbound("group", "calendar-create", "Add Maya pickup to the family calendar."),
      authoredText: null,
      voiceTranscriptPresent: true,
    });
    expect(voicedCalendarCreate).not.toBeNull();
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
  readonly moves: LinqSendMove[] = [];
  readonly reactions: LinqSendReaction[] = [];
  readonly uploads: Parameters<LinqClient["uploadAttachment"]>[0][] = [];
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

  async uploadAttachment(input: Parameters<LinqClient["uploadAttachment"]>[0]): Promise<string> {
    this.uploads.push(input);
    return `uploaded-attachment-${this.uploads.length}`;
  }

  async setTyping(): Promise<boolean> {
    return true;
  }

  async markRead(): Promise<boolean> {
    return true;
  }

  async sendMove(input: LinqSendMove) {
    const prior = this.ledger.sent.get(input.idempotencyKey);
    if (prior) return prior;
    this.moves.push(input);
    this.ledger.nextMessageReceipt += 1;
    const result = {
      status: "committed" as const,
      providerState: "sent" as const,
      idempotencyKey: input.idempotencyKey,
      providerReceiptId: `native-${this.ledger.nextMessageReceipt}`,
      detail: null,
      occurredAt: new Date(this.state.now).toISOString(),
    };
    this.ledger.sent.set(input.idempotencyKey, result);
    return result;
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
  options: {
    now?: number;
    linqLedger?: FakeLinqLedger;
    browser?: FlorenceBrowserClient;
    continueFamilyWork?: FlorenceReasoner["continueFamilyWork"];
    interpretParticipantReply?: FlorenceReasoner["interpretParticipantReply"];
    researchInterest?: FlorenceReasoner["researchInterest"];
  } = {},
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
    personalCalendarReads: [],
    calendarEvents: new Map(),
    uncertainCalendarCreateTitle: null,
    timeline: [],
    finiteReviews: 0,
    interestResearches: 0,
    interestResearchInputs: [],
    interestBusyUnionExercise: false,
    voiceTranscriptions: 0,
    initialGoogleFailuresRemaining: 0,
    initialClassifierFailuresRemaining: 0,
    initialGoogleFailureAdultId: null,
    completeScanPaginationExercise: false,
    completeFactSupportExercise: false,
    retainedFactBeyondReviewWindowExercise: false,
    wrongGoogleSubjectNext: false,
    baselinePageReads: [],
    initialHouseholdCalendarFailuresRemaining: 0,
    privateFactUpdatePending: false,
    privateFactUpdateDelivered: false,
    overlapGmailReadsRemaining: 0,
    overlapGmailAssessments: 0,
    overlapGmailSourceId: null,
    monitorEvidenceExercise: false,
    linkedGmailMonitorExercise: false,
    exactGmailReads: [],
    monitorCancellationActive: false,
    silentMonitorSourceId: null,
    voicedMonitorSourceId: null,
    cancelledMonitorSourceId: null,
    setupConversationFailuresRemaining: 0,
    founderProductRecenterReview: false,
    googleRecipeArtifactExercise: false,
    proactiveFamilyWorkExercise: false,
    familyCalendarProvisioningFailuresRemaining: 0,
    invalidGrantAdultId: null,
    invalidGrantTriggered: false,
    googleDeletionEvidencePending: false,
    googleDeletionEvidenceDelivered: false,
    googleDeletionSourceId: null,
    googleChangeReads: [],
    interactiveGoogleReads: 0,
    workspaceExecutions: [],
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
  const reasoner = createReasoner(
    reason,
    state,
    options.continueFamilyWork,
    options.interpretParticipantReply,
    options.researchInterest,
  );
  const google = createGoogle(store, state);
  const florence = new Florence({
    store,
    linq: linq as unknown as LinqClient,
    google,
    ...(options.browser ? { browser: options.browser } : {}),
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

function createReasoner(
  reason: Reason,
  state: HarnessState,
  continueFamilyWork?: FlorenceReasoner["continueFamilyWork"],
  interpretParticipantReply?: FlorenceReasoner["interpretParticipantReply"],
  researchInterest?: FlorenceReasoner["researchInterest"],
): FlorenceReasoner {
  return {
    decide: reason,
    ...(continueFamilyWork ? { continueFamilyWork } : {}),
    interpretParticipantReply:
      interpretParticipantReply ??
      (async () => ({ belongsToRequest: false as const, acknowledgement: null })),
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
      const recipeSource = eligibleGmail.find((source) => source.subject === GOOGLE_RECIPE_SUBJECT);
      const schoolContactSupports = eligibleGmail.filter((source) =>
        source.subject?.startsWith("School office contact confirmation "),
      );
      if (state.retainedFactBeyondReviewWindowExercise) {
        const retainedFactSource = founder
          ? eligibleGmail.find((source) => source.subject !== GOOGLE_RECIPE_SUBJECT)
          : undefined;
        return {
          findings: [],
          facts: retainedFactSource
            ? [
                {
                  slot: PRIVATE_SCHOOL_FACT_SLOT,
                  statement: INITIAL_PRIVATE_SCHOOL_FACT,
                  memory: googleFactMemory(INITIAL_PRIVATE_SCHOOL_FACT),
                  familyRelevance: "household" as const,
                  sourceIds: [retainedFactSource.sourceId],
                },
              ]
            : [],
          dismissedSourceIds: input.sources
            .filter((source) => source.sourceId !== retainedFactSource?.sourceId)
            .map((source) => source.sourceId),
        };
      }
      const paginatedCalendarSource = calendar.find((source) => source.title === PAGINATED_CALENDAR_TITLE);
      if (paginatedCalendarSource) {
        return {
          findings: [
            {
              privateSummary: PAGINATED_CALENDAR_FOLLOW_UP,
              privateDocket: null,
              actionAnchor: PAGINATED_CALENDAR_TITLE,
              familyRelevance: "household" as const,
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
              privateDocket: null,
              actionAnchor: PRIVATE_INITIAL_CALENDAR_ONLY_EVENT.title,
              familyRelevance: "household" as const,
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
      const source = eligibleGmail.find(
        (candidate) =>
          candidate.subject !== GOOGLE_RECIPE_SUBJECT &&
          !candidate.subject?.startsWith("School office contact confirmation "),
      );
      if (!source) {
        if (schoolContactSupports.length > 0) {
          return {
            findings: [],
            facts: [
              {
                slot: SHARED_SCHOOL_CONTACT_SLOT,
                statement: SHARED_SCHOOL_CONTACT_FACT,
                memory: googleFactMemory(SHARED_SCHOOL_CONTACT_FACT),
                familyRelevance: "household" as const,
                sourceIds: schoolContactSupports.map((candidate) => candidate.sourceId),
              },
              ...(recipeSource
                ? [
                    {
                      slot: GOOGLE_RECIPE_SLOT,
                      statement: GOOGLE_RECIPE_STATEMENT,
                      memory: {
                        memoryKind: "artifact" as const,
                        artifactKind: "recipe" as const,
                        title: GOOGLE_RECIPE_TITLE,
                        details: GOOGLE_RECIPE_DETAILS,
                        tags: ["recipe", "noodles", "weeknight"],
                      },
                      familyRelevance: "household" as const,
                      sourceIds: [recipeSource.sourceId],
                    },
                  ]
                : []),
            ],
            dismissedSourceIds: [...unrelated, ...calendar].map((candidate) => candidate.sourceId),
          };
        }
        const ownerPrivateSource = unrelated[0];
        if (founder && state.initialUnrelatedAccountReview && ownerPrivateSource) {
          return {
            findings: [
              {
                privateSummary: UNRELATED_ACCOUNT_EMAIL_ALERT,
                privateDocket: null,
                actionAnchor: "password changed",
                familyRelevance: "owner_private" as const,
                sourceIds: [ownerPrivateSource.sourceId],
                urgency: "now" as const,
                dueAt: null,
                surfaceNow: true,
                candidate: null,
                monitor: {
                  objective: UNRELATED_ACCOUNT_MONITOR_OBJECTIVE,
                  currentConclusion: "The account change needs the adult’s verification.",
                  endCondition: "The adult confirms the change or secures the account.",
                  nextCheck: new Date(Date.parse(input.currentTime) + 24 * 60 * 60_000).toISOString(),
                  why: "The account alert asks for verification.",
                },
                familyCalendar: null,
              },
            ],
            facts: [],
            dismissedSourceIds: [...unrelated.slice(1), ...calendar].map((candidate) => candidate.sourceId),
          };
        }
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
        privateDocket: null,
        actionAnchor: founder ? "field-trip form" : "permission-slip deadline",
        familyRelevance: "household" as const,
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
          owner: founder ? "Hari" : "Alex",
          nextAction: founder ? FOUNDER_FORM_NEXT_ACTION : PARTNER_PERMISSION_NEXT_ACTION,
          waitingOn: founder ? FOUNDER_FORM_WAITING_ON : PARTNER_PERMISSION_WAITING_ON,
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
              privateDocket: null,
              actionAnchor: "Muir Elementary",
              familyRelevance: "household" as const,
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
              privateDocket: null,
              actionAnchor: "activity registration window",
              familyRelevance: "household" as const,
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
                owner: "Parents",
                nextAction: "Decide whether to register for the fall activity.",
                waitingOn: "A family decision",
              },
              monitor: null,
              familyCalendar: null,
            },
            {
              privateSummary: "Friday’s pickup handoff has not been assigned.",
              privateDocket: null,
              actionAnchor: "pickup handoff",
              familyRelevance: "household" as const,
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
                owner: null,
                nextAction: "Assign Friday’s school pickup.",
                waitingOn: "A parent to take pickup",
              },
              monitor: null,
              familyCalendar: null,
            },
            {
              privateSummary: sameSubjectRescan
                ? "One more private school detail remains unresolved."
                : PRIVATE_INITIAL_ONLY_FINDING,
              privateDocket: {
                owner: "You",
                nextAction: "Confirm the school office contact details.",
                waitingOn: "Your confirmation",
                needsAnswer: true,
              },
              actionAnchor: "private school detail",
              familyRelevance: "owner_private" as const,
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
              privateDocket: null,
              actionAnchor: "activity registration window",
              familyRelevance: "household" as const,
              sourceIds: [source.sourceId],
              urgency: "soon" as const,
              dueAt: "2026-08-20T16:00:00.000Z",
              surfaceNow: true,
              candidate: {
                category: "conflict" as const,
                summary: PARTNER_DUPLICATE_CONFLICT_SUMMARY,
                urgency: "soon" as const,
                dueAt: "2026-08-20T16:00:00.000Z",
                needsAnswer: true,
                owner: "Parents",
                nextAction: "Decide whether to register for the fall activity.",
                waitingOn: "A family decision",
              },
              monitor: null,
              familyCalendar: null,
            },
            {
              privateSummary: "The family meeting is on Alex’s calendar for Tuesday evening.",
              privateDocket: null,
              actionAnchor: "family meeting",
              familyRelevance: "household" as const,
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
                owner: "Family",
                nextAction: "Review Tuesday’s family-meeting plan.",
                waitingOn: null,
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
            memory: googleFactMemory(founder ? INITIAL_PRIVATE_SCHOOL_FACT : PARTNER_PRIVATE_GOOGLE_FACT),
            familyRelevance: "household" as const,
            sourceIds: [source.sourceId],
          },
          {
            slot: SHARED_SCHOOL_CONTACT_SLOT,
            statement: SHARED_SCHOOL_CONTACT_FACT,
            memory: googleFactMemory(SHARED_SCHOOL_CONTACT_FACT),
            familyRelevance: "household" as const,
            sourceIds: [source.sourceId, ...schoolContactSupports.map((candidate) => candidate.sourceId)],
          },
          ...(founder
            ? [
                {
                  slot: GOOGLE_CORRECTION_SLOT,
                  statement: GOOGLE_CORRECTION_FACT,
                  memory: googleFactMemory(GOOGLE_CORRECTION_FACT),
                  familyRelevance: "household" as const,
                  sourceIds: [source.sourceId],
                },
              ]
            : []),
          ...(recipeSource
            ? [
                {
                  slot: GOOGLE_RECIPE_SLOT,
                  statement: GOOGLE_RECIPE_STATEMENT,
                  memory: {
                    memoryKind: "artifact" as const,
                    artifactKind: "recipe" as const,
                    title: GOOGLE_RECIPE_TITLE,
                    details: GOOGLE_RECIPE_DETAILS,
                    tags: ["recipe", "noodles", "weeknight"],
                  },
                  familyRelevance: "household" as const,
                  sourceIds: [recipeSource.sourceId],
                },
              ]
            : []),
        ],
        dismissedSourceIds: [...unrelated, ...calendar, ...eligibleGmail]
          .filter(
            (candidate) =>
              candidate.sourceId !== source.sourceId &&
              candidate !== recipeSource &&
              !schoolContactSupports.some((support) => support.sourceId === candidate.sourceId),
          )
          .map((candidate) => candidate.sourceId),
      };
    },
    synthesizeHouseholdBriefing: async (
      input: Parameters<FlorenceReasoner["synthesizeHouseholdBriefing"]>[0],
    ) => {
      state.briefings.push(input);
      const selected = state.proactiveFamilyWorkExercise ? [] : input.candidates.slice(0, 3);
      const remaining = input.candidates.length - selected.length;
      if (state.proactiveFamilyWorkExercise) {
        expect(
          input.memory.some(
            (item) => item.label === GOOGLE_RECIPE_TITLE && item.text.includes("keep it mild"),
          ),
        ).toBe(true);
        expect(input.familyCalendar.some((event) => event.title === AUTOMATIC_FAMILY_DATE.title)).toBe(true);
      }
      return {
        selectedCandidateIds: selected.map((candidate) => candidate.candidateId),
        bubbles: [
          {
            text: state.proactiveFamilyWorkExercise
              ? PROACTIVE_FAMILY_WORK_KICKOFF
              : `Here’s what I found:\n${selected.map((candidate) => `– ${candidate.summary}`).join("\n")}${
                  remaining > 0
                    ? `\n\nI kept ${remaining} lower-priority items on the docket too. Ask me anytime.`
                    : ""
                }\n\nDid I get that right? If I missed something, tell me here.`,
            delayMs: 0,
          },
        ],
        nextJob: state.proactiveFamilyWorkExercise
          ? {
              objective: PROACTIVE_FAMILY_WORK_OBJECTIVE,
              kickoffBubbleIndex: 0,
              candidateIds: [],
            }
          : null,
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
                privateDocket: null,
                actionAnchor: FAMILY_CALENDAR_MIXED_CHANGE_TITLE,
                familyRelevance: "household" as const,
                householdConclusion: {
                  category: "family_date" as const,
                  summary: FAMILY_CALENDAR_MIXED_CHANGE_SUMMARY,
                  urgency: "soon" as const,
                  dueAt: FAMILY_CALENDAR_MIXED_CHANGE_EVENT.startsAt,
                  needsAnswer: false,
                  owner: "Family",
                  nextAction: "Review the updated family-calendar plan.",
                  waitingOn: null,
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
                  privateDocket: null,
                  actionAnchor: PRIVATE_CALENDAR_ADULT_TITLE,
                  familyRelevance: "household" as const,
                  householdConclusion: {
                    category: "loose_end" as const,
                    summary: `${PRIVATE_CALENDAR_ADULT_TITLE} is on Tuesday at ${PRIVATE_CALENDAR_ADULT_EVENT.location}.`,
                    urgency: "watch" as const,
                    dueAt: PRIVATE_CALENDAR_ADULT_EVENT.startsAt,
                    needsAnswer: false,
                    owner: "Family",
                    nextAction: `Review Tuesday’s plan for ${PRIVATE_CALENDAR_ADULT_TITLE}.`,
                    waitingOn: null,
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
                    privateDocket: null,
                    actionAnchor: PRIVATE_CALENDAR_ANNIVERSARY_TITLE,
                    familyRelevance: "household" as const,
                    householdConclusion: {
                      category: "family_date" as const,
                      summary: `${PRIVATE_CALENDAR_ANNIVERSARY_TITLE} is on Monday at ${PRIVATE_CALENDAR_ANNIVERSARY_EVENT.location}.`,
                      urgency: "watch" as const,
                      dueAt: PRIVATE_CALENDAR_ANNIVERSARY_EVENT.startsAt,
                      needsAnswer: false,
                      owner: "Family",
                      nextAction: `Review Monday’s plan for ${PRIVATE_CALENDAR_ANNIVERSARY_TITLE}.`,
                      waitingOn: null,
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
                      privateDocket: null,
                      actionAnchor: "password changed",
                      familyRelevance: "owner_private" as const,
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
                        privateDocket: null,
                        actionAnchor: PRIVATE_CALENDAR_ONLY_TITLE,
                        familyRelevance: "household" as const,
                        householdConclusion: {
                          category: "conflict" as const,
                          summary: `${PRIVATE_CALENDAR_ONLY_TITLE} overlaps ${PRIVATE_CALENDAR_CONFLICT_TITLE}.`,
                          urgency: "now" as const,
                          dueAt: PRIVATE_CALENDAR_ONLY_EVENT.startsAt,
                          needsAnswer: true,
                          owner: "Parents",
                          nextAction: "Decide how to cover the overlapping calendar events.",
                          waitingOn: "A parent decision",
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
                          privateDocket: null,
                          actionAnchor: "emergency card",
                          familyRelevance: "household" as const,
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
                          privateDocket: null,
                          actionAnchor: GOOGLE_DELETION_FAMILY_DATE.title,
                          familyRelevance: "household" as const,
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
                memory: googleFactMemory(UNRELATED_ACCOUNT_FACT),
                familyRelevance: "owner_private" as const,
                sourceIds: [unrelatedAccountSource.sourceId],
              },
            ]
          : calendarOnlySource
            ? [
                {
                  slot: PRIVATE_CALENDAR_FACT_SLOT,
                  statement: PRIVATE_CALENDAR_FACT,
                  memory: googleFactMemory(PRIVATE_CALENDAR_FACT),
                  familyRelevance: "household" as const,
                  sourceIds: [calendarOnlySource.sourceId],
                },
              ]
            : deletionSource
              ? [
                  {
                    slot: GOOGLE_DELETION_FACT_SLOT,
                    statement: GOOGLE_DELETION_FACT,
                    memory: googleFactMemory(GOOGLE_DELETION_FACT),
                    familyRelevance: "household" as const,
                    sourceIds: [deletionSource.sourceId],
                  },
                ]
              : source
                ? [
                    {
                      slot: PRIVATE_SCHOOL_FACT_SLOT,
                      statement: UPDATED_PRIVATE_SCHOOL_FACT,
                      memory: googleFactMemory(UPDATED_PRIVATE_SCHOOL_FACT),
                      familyRelevance: "household" as const,
                      sourceIds: [source.sourceId],
                    },
                    {
                      slot: GOOGLE_CORRECTION_SLOT,
                      statement: GOOGLE_CORRECTION_FACT,
                      memory: googleFactMemory(GOOGLE_CORRECTION_FACT),
                      familyRelevance: "household" as const,
                      sourceIds: [source.sourceId],
                    },
                  ]
                : overlap && state.overlapGmailAssessments === 2
                  ? [
                      {
                        slot: OVERLAP_GMAIL_FACT_SLOT,
                        statement: "Maya’s school bus route reminder is current.",
                        memory: googleFactMemory("Maya’s school bus route reminder is current."),
                        familyRelevance: "household" as const,
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
      if (state.linkedGmailMonitorExercise) {
        if (input.monitor.objective !== UNRELATED_ACCOUNT_MONITOR_OBJECTIVE) {
          throw new Error("The due Gmail review selected another monitor");
        }
        const [source] = input.evidence.gmail.sources;
        if (
          input.evidence.gmail.sources.length !== 1 ||
          source?.subject !== UNRELATED_ACCOUNT_EMAIL_SUBJECT ||
          input.evidence.gmail.status !== "complete"
        ) {
          throw new Error("The due Gmail review did not receive its exact linked source");
        }
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
            owner: "School office",
            nextAction: "Wait for the school to finish checking the signature.",
            waitingOn: "The school’s signature check",
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
          owner: null,
          nextAction: "No further action is needed.",
          waitingOn: null,
        },
        sourceIds: [source.sourceId],
        currentConclusion: "The form is signed.",
        nextCheck: null,
        why: input.monitor.why,
      };
    },
    researchInterest: async (input: Parameters<FlorenceReasoner["researchInterest"]>[0]) => {
      state.interestResearches += 1;
      state.interestResearchInputs.push(input);
      if (researchInterest) return researchInterest(input);
      return {
        judgment: "recommend" as const,
        summary: INTEREST_RECOMMENDATION,
        urls: [INTEREST_URL],
      };
    },
  } as unknown as FlorenceReasoner;
}

function interestBusyUnionCalendarEvents(input: {
  ownerAdultId: string;
  calendarId?: string;
  currentTime: string;
}): GoogleCalendarWindowEvent[] {
  const event = (providerEventId: string, startsAfterHours: number, endsAfterHours: number) => ({
    providerEventId,
    providerRevision: `${providerEventId}-revision`,
    providerUpdatedAt: input.currentTime,
    status: "confirmed" as const,
    busy: true,
    title: "Private calendar detail",
    location: null,
    intervalKind: "timed" as const,
    startsAt: new Date(Date.parse(input.currentTime) + startsAfterHours * 60 * 60_000).toISOString(),
    endsAt: new Date(Date.parse(input.currentTime) + endsAfterHours * 60 * 60_000).toISOString(),
    timeZone: "America/Los_Angeles",
  });
  if (input.calendarId && input.calendarId !== "primary") {
    return [event("busy-family", 3, 4)];
  }
  if (input.ownerAdultId === founderSetup().adultId) {
    return [
      event("busy-founder-first", 1, 2),
      ...Array.from({ length: 51 }, (_, index) => {
        const startsAfterHours = 5 + index * 8;
        return event(`busy-founder-${index + 2}`, startsAfterHours, startsAfterHours + 1);
      }),
    ];
  }
  return [event("busy-partner", 2, 3)];
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
    const events = state.interestBusyUnionExercise
      ? interestBusyUnionCalendarEvents(input)
      : input.calendarId && input.calendarId !== "primary"
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
    runWorkspace: async (
      input: WorkspaceExecutionInput,
      signal?: AbortSignal,
    ): Promise<GoogleWorkspaceResult> => {
      await activeCredential(input);
      signal?.throwIfAborted();
      state.workspaceExecutions.push(input);
      if (input.operation.operation === "tasks_create") {
        return {
          operation: input.operation.operation,
          result: {
            status: "created",
            taskId: `google-task-${state.workspaceExecutions.length}`,
            title: input.operation.title,
          },
        };
      }
      return { operation: input.operation.operation, result: { status: "completed" } };
    },
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
    readGmailMessage: async (input: Parameters<GoogleConnection["readGmailMessage"]>[0]) => {
      await activeCredential(input);
      state.exactGmailReads.push(input);
      if (
        input.messageId === SCHOOL_ATTACHMENT.messageId &&
        input.threadId === SCHOOL_ATTACHMENT.threadId &&
        input.historyId === SCHOOL_ATTACHMENT.historyId
      ) {
        return {
          messageId: input.messageId,
          threadId: input.threadId,
          historyId: input.historyId,
          from: "school@muir.example.test",
          subject: "Maya field-trip form",
          sentAt: new Date(NOW - 1_000).toISOString(),
          text: "Please upload Maya's attached field-trip form.",
          textStatus: "complete" as const,
          attachmentsStatus: "complete" as const,
          attachments: [SCHOOL_ATTACHMENT],
        };
      }
      if (
        input.messageId !== "gmail-initial-unrelated-retail-account-alert" ||
        input.threadId !== "thread-gmail-initial-unrelated-retail-account-alert" ||
        input.historyId !== "101"
      ) {
        throw new Error("The due monitor requested an unexpected Gmail source");
      }
      return {
        messageId: input.messageId,
        threadId: input.threadId,
        historyId: input.historyId,
        from: "account@example.test",
        subject: UNRELATED_ACCOUNT_EMAIL_SUBJECT,
        sentAt: new Date(NOW - 1_000).toISOString(),
        text: "Your retail account password was changed.",
        textStatus: "complete" as const,
        attachmentsStatus: "complete" as const,
        attachments: [],
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
        sentAt:
          state.retainedFactBeyondReviewWindowExercise && founder
            ? new Date(NOW - 1_000).toISOString()
            : new Date(Date.parse(input.before) - 1_000).toISOString(),
        text: initialUnrelatedAccount
          ? "Your retail account password was changed."
          : founder
            ? "Hari private email: Maya attends Muir Elementary and her form needs a signature."
            : "Alex private email: a personal appointment moved.",
        textStatus: "complete" as const,
        attachmentsStatus: "complete" as const,
        attachments: !initialUnrelatedAccount && founder ? [SCHOOL_ATTACHMENT] : [],
      };
      const recipe = {
        messageId: "gmail-family-sesame-noodles-recipe",
        threadId: "thread-gmail-family-sesame-noodles-recipe",
        historyId: "102",
        from: "family-recipes@example.test",
        subject: GOOGLE_RECIPE_SUBJECT,
        sentAt: new Date(Date.parse(input.before) - 500).toISOString(),
        text: `${GOOGLE_RECIPE_STATEMENT} ${GOOGLE_RECIPE_DETAILS}`,
        textStatus: "complete" as const,
        attachmentsStatus: "complete" as const,
        attachments: [],
      };
      const recipeMessages =
        state.googleRecipeArtifactExercise && founder && input.connectionId === FOUNDER_GOOGLE
          ? [recipe]
          : [];
      const relevantMessages =
        state.retainedFactBeyondReviewWindowExercise && founder
          ? Date.parse(relevant.sentAt) >= Date.parse(input.after)
            ? [relevant]
            : []
          : [relevant];
      const schoolContactSupportMessages =
        state.completeFactSupportExercise && founder && input.connectionId === FOUNDER_GOOGLE
          ? Array.from({ length: 11 }, (_, index) => ({
              ...relevant,
              messageId: `gmail-school-office-contact-confirmation-${index}`,
              threadId: `thread-gmail-school-office-contact-confirmation-${index}`,
              historyId: String(2_000 + index),
              subject: `School office contact confirmation ${index}`,
              sentAt: new Date(Date.parse(input.before) - 900 + index).toISOString(),
              text: SHARED_SCHOOL_CONTACT_FACT,
              attachments: [],
            }))
          : [];
      if (
        (state.completeScanPaginationExercise || state.completeFactSupportExercise) &&
        founder &&
        input.connectionId === FOUNDER_GOOGLE
      ) {
        if (input.pageToken === undefined) {
          return {
            status: "truncated" as const,
            nextPageToken: "gmail-baseline-page-2",
            messages: [
              ...(state.completeScanPaginationExercise
                ? Array.from({ length: 50 }, (_, index) => ({
                    ...relevant,
                    messageId: `gmail-archived-irrelevant-${index}`,
                    threadId: `thread-gmail-archived-irrelevant-${index}`,
                    historyId: String(1_000 + index),
                    subject: UNRELATED_ACCOUNT_EMAIL_SUBJECT,
                    text:
                      index === 0
                        ? "Archived owner-private account notice. Code 123456. https://example.test/reset?token=fake-only-token-value-1234567890"
                        : "Archived owner-private account notice.",
                    attachments: [],
                  }))
                : []),
              ...schoolContactSupportMessages.slice(0, 9),
            ],
          };
        }
        expect(input.pageToken).toBe("gmail-baseline-page-2");
      } else {
        expect(input.pageToken).toBeUndefined();
      }
      return {
        status: "complete" as const,
        nextPageToken: null,
        messages: [...relevantMessages, ...schoolContactSupportMessages.slice(9), ...recipeMessages],
      };
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
      if (state.linkedGmailMonitorExercise && input.query === "-category:promotions -category:social") {
        throw new Error("A due monitor used the sampled generic Gmail search");
      }
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
    readPersonalCalendarCatalog: async (input: PersonalCalendarCatalogInput) => {
      await activeCredential(input);
      state.interactiveGoogleReads += 1;
      const calendars = [
        {
          calendarId: "primary",
          label: "Primary calendar",
          timeZone: "America/Los_Angeles",
          accessRole: "owner" as const,
          primary: true,
          eventCoverage: "readable" as const,
        },
        {
          calendarId: FAMILY_CALENDAR,
          label: state.providerCalendarSummary ?? "Family Calendar",
          timeZone: "America/Los_Angeles",
          accessRole: "owner" as const,
          primary: false,
          eventCoverage: "readable" as const,
        },
      ]
        .filter((target) => target.calendarId !== input.excludedFamilyCalendarId)
        .sort((left, right) => left.calendarId.localeCompare(right.calendarId));
      return {
        status: "complete" as const,
        calendars,
        totalCalendarCount: calendars.length,
        coverageDigest: digest(JSON.stringify(calendars)),
        logicalCoverageDigest: digest(
          JSON.stringify(
            calendars.map((calendar) => ({
              calendarId: calendar.calendarId,
              timeZone: calendar.timeZone,
              eventCoverage: calendar.eventCoverage,
            })),
          ),
        ),
      };
    },
    readExactCalendarCatalog: async (input: ExactCalendarCatalogInput) => {
      await activeCredential(input);
      state.interactiveGoogleReads += 1;
      const calendars =
        input.calendarId === FAMILY_CALENDAR
          ? [
              {
                calendarId: FAMILY_CALENDAR,
                label: state.providerCalendarSummary ?? "Family Calendar",
                timeZone: "America/Los_Angeles",
                accessRole: "owner" as const,
                primary: false,
                eventCoverage: "readable" as const,
              },
            ]
          : [];
      const status = calendars.length === 1 ? ("complete" as const) : ("unavailable" as const);
      return {
        status,
        calendars,
        totalCalendarCount: calendars.length,
        coverageDigest: digest(JSON.stringify({ calendarId: input.calendarId, calendars })),
        logicalCoverageDigest: digest(
          JSON.stringify({
            calendarId: input.calendarId,
            available: calendars.length === 1,
            eventCoverage: calendars[0]?.eventCoverage ?? null,
          }),
        ),
      };
    },
    readPersonalCalendarWindow: async (input: PersonalCalendarReadInput) => {
      await activeCredential(input);
      state.interactiveGoogleReads += 1;
      state.personalCalendarReads.push(input);
      const availableTargets = [
        {
          calendarId: "primary",
          label: "Primary calendar",
          timeZone: "America/Los_Angeles",
          accessRole: "owner" as const,
          primary: true,
        },
        {
          calendarId: FAMILY_CALENDAR,
          label: state.providerCalendarSummary ?? "Family Calendar",
          timeZone: "America/Los_Angeles",
          accessRole: "owner" as const,
          primary: false,
        },
      ].filter((target) => target.calendarId !== input.excludedFamilyCalendarId);
      const selectedIds = input.calendarIds ? new Set(input.calendarIds) : null;
      const selectedTargets = availableTargets.filter(
        (target) => selectedIds === null || selectedIds.has(target.calendarId),
      );
      const fullEvents: GooglePersonalCalendarWindowEvent[] = [];
      for (const target of selectedTargets) {
        const events: readonly GoogleCalendarWindowEvent[] =
          target.calendarId === FAMILY_CALENDAR
            ? [...state.calendarEvents.values()]
            : (baseline({
                ...input,
                calendarId: target.calendarId,
                currentTime: input.timeMin,
              }).events as readonly GoogleCalendarWindowEvent[]);
        for (const event of events) {
          const startsAt =
            event.intervalKind === "timed" ? event.startsAt : `${event.startDate}T00:00:00.000Z`;
          const endsAt = event.intervalKind === "timed" ? event.endsAt : `${event.endDate}T00:00:00.000Z`;
          if (endsAt > input.timeMin && startsAt < input.timeMax) {
            fullEvents.push({ calendarId: target.calendarId, ...event });
          }
        }
      }
      const missingIds =
        selectedIds === null
          ? []
          : [...selectedIds].filter(
              (calendarId) => !selectedTargets.some((target) => target.calendarId === calendarId),
            );
      const calendars = [
        ...selectedTargets.map((target) => ({
          ...target,
          status: "complete" as const,
          eventCount: fullEvents.filter((event) => event.calendarId === target.calendarId).length,
        })),
        ...missingIds.map((calendarId) => ({
          calendarId,
          label: null,
          timeZone: null,
          accessRole: null,
          primary: false,
          status: "missing" as const,
          eventCount: 0,
        })),
      ].sort((left, right) => left.calendarId.localeCompare(right.calendarId));
      fullEvents.sort((left, right) => {
        const leftStart = left.intervalKind === "timed" ? left.startsAt : left.startDate;
        const rightStart = right.intervalKind === "timed" ? right.startsAt : right.startDate;
        return leftStart.localeCompare(rightStart) || left.calendarId.localeCompare(right.calendarId);
      });
      const coverageDigest = digest(
        JSON.stringify({
          timeMin: input.timeMin,
          timeMax: input.timeMax,
          excludedFamilyCalendarId: input.excludedFamilyCalendarId,
          calendarIds: input.calendarIds === undefined ? null : [...input.calendarIds].sort(),
          calendars,
          events: fullEvents,
        }),
      );
      const cursorQueryDigest = digest(
        JSON.stringify({
          timeMin: input.timeMin,
          timeMax: input.timeMax,
          excludedFamilyCalendarId: input.excludedFamilyCalendarId,
          calendarIds: input.calendarIds === undefined ? null : [...input.calendarIds].sort(),
        }),
      );
      const cursorPrefix = `personal-calendar:${cursorQueryDigest}:${coverageDigest}:`;
      const limit = input.limit ?? 50;
      const offset = input.cursor
        ? input.cursor.startsWith(cursorPrefix)
          ? Number(input.cursor.slice(cursorPrefix.length))
          : Number.NaN
        : 0;
      if (!Number.isInteger(offset) || offset < 0 || offset > fullEvents.length) {
        throw new Error("Invalid personal Calendar test cursor");
      }
      const nextOffset = Math.min(offset + limit, fullEvents.length);
      const nextCursor = nextOffset < fullEvents.length ? `${cursorPrefix}${nextOffset}` : null;
      return {
        status:
          missingIds.length > 0
            ? ("partial" as const)
            : nextCursor
              ? ("truncated" as const)
              : ("complete" as const),
        timeMin: input.timeMin,
        timeMax: input.timeMax,
        calendars,
        totalCalendarCount: calendars.length,
        events: fullEvents.slice(offset, nextOffset),
        totalEventCount: fullEvents.length,
        nextCursor,
        coverageDigest,
        logicalCoverageDigest: digest(
          JSON.stringify({
            timeMin: input.timeMin,
            timeMax: input.timeMax,
            calendarIds: input.calendarIds === undefined ? null : [...input.calendarIds].sort(),
            calendars: calendars.map((calendar) => ({
              calendarId: calendar.calendarId,
              status: calendar.status,
              eventCount: calendar.eventCount,
            })),
            events: fullEvents,
          }),
        ),
      };
    },
    readExactCalendarWindow: async (input: ExactCalendarReadInput) => {
      await activeCredential(input);
      state.interactiveGoogleReads += 1;
      const exists = input.calendarId === FAMILY_CALENDAR;
      const fullEvents = exists
        ? [...state.calendarEvents.values()]
            .filter((event) => overlaps(event, input))
            .map((event) => ({ calendarId: input.calendarId, ...event }))
        : [];
      const calendars = [
        {
          calendarId: input.calendarId,
          label: exists ? (state.providerCalendarSummary ?? "Family Calendar") : null,
          timeZone: exists ? "America/Los_Angeles" : null,
          accessRole: exists ? ("owner" as const) : null,
          primary: false,
          status: exists ? ("complete" as const) : ("missing" as const),
          eventCount: fullEvents.length,
        },
      ];
      const logicalCoverage = {
        calendarId: input.calendarId,
        timeMin: input.timeMin,
        timeMax: input.timeMax,
        status: calendars[0]?.status,
        events: fullEvents,
      };
      const logicalCoverageDigest = digest(JSON.stringify(logicalCoverage));
      const cursorQueryDigest = digest(
        JSON.stringify({
          calendarId: input.calendarId,
          timeMin: input.timeMin,
          timeMax: input.timeMax,
        }),
      );
      const cursorPrefix = `exact-calendar:${cursorQueryDigest}:${logicalCoverageDigest}:`;
      const limit = input.limit ?? 50;
      const offset = input.cursor
        ? input.cursor.startsWith(cursorPrefix)
          ? Number(input.cursor.slice(cursorPrefix.length))
          : Number.NaN
        : 0;
      if (!Number.isInteger(offset) || offset < 0 || offset > fullEvents.length) {
        throw new Error("Invalid exact Calendar test cursor");
      }
      const nextOffset = Math.min(offset + limit, fullEvents.length);
      const nextCursor = nextOffset < fullEvents.length ? `${cursorPrefix}${nextOffset}` : null;
      return {
        status: exists ? (nextCursor ? ("truncated" as const) : ("complete" as const)) : ("partial" as const),
        timeMin: input.timeMin,
        timeMax: input.timeMax,
        calendars,
        totalCalendarCount: 1,
        events: fullEvents.slice(offset, nextOffset),
        totalEventCount: fullEvents.length,
        nextCursor,
        coverageDigest: digest(
          JSON.stringify({
            ...logicalCoverage,
            label: calendars[0]?.label,
            accessRole: calendars[0]?.accessRole,
          }),
        ),
        logicalCoverageDigest,
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
    reaction?: FlorenceDecision["conversation"]["reaction"];
    bubbles?: FlorenceDecision["conversation"]["bubbles"];
    nativeMoves?: FlorenceDecision["conversation"]["nativeMoves"];
    facts?: FlorenceDecision["facts"];
    followUp?: FlorenceDecision["followUp"];
    reminder?: FlorenceDecision["reminder"];
    familyWork?: FlorenceDecision["familyWork"];
    docketUpsert?: FlorenceDecision["docketUpsert"];
    docketCompletions?: FlorenceDecision["docketCompletions"];
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
      reaction: input.reaction ?? null,
      bubbles: input.bubbles ?? [],
      nativeMoves: input.nativeMoves ?? null,
    },
    facts: input.facts ?? [],
    followUp: input.followUp ?? null,
    reminder: input.reminder ?? null,
    familyWork: input.familyWork ?? null,
    docketUpsert: input.docketUpsert ?? null,
    docketCompletions: input.docketCompletions ?? null,
    interest: input.interest ?? null,
    calendar: input.calendar ?? null,
    householdUpdate: input.householdUpdate ?? null,
    webAccessPath: input.webAccessPath ?? null,
    researchUrls: input.researchUrls ?? null,
  };
}

function remember(
  statement: string,
  sourceId: string,
  visibility: "private" | "household",
): FlorenceDecision["facts"][number] {
  return {
    operation: "remember",
    factId: null,
    statement,
    visibility,
    memory: {
      memoryKind: "fact",
      artifactKind: null,
      title: null,
      details: null,
      tags: [],
    },
    sourceIds: [sourceId],
  };
}

function googleFactMemory(statement: string) {
  return {
    memoryKind: "fact" as const,
    artifactKind: null,
    title: null,
    details: statement,
    tags: [],
  };
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
    timeMin: "2026-08-18T21:30:00.000Z",
    timeMax: "2026-08-18T22:30:00.000Z",
    pageSize: 50,
    cursor: null,
    scope: "all",
    calendarRefs: [],
  });
  const event = result.events.find((candidate) => candidate.title === expected.title);
  if (event?.intervalKind !== "timed") throw new Error("Maya pickup was not found");
  expect(event).not.toHaveProperty("providerEventId");
  expect(event).not.toHaveProperty("providerRevision");
  return {
    eventRef: event.eventRef,
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

function deterministicUuid(value: string): string {
  const hex = digest(value);
  const variant = ((Number.parseInt(hex[16] ?? "0", 16) & 3) | 8).toString(16);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-${variant}${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}

function inboundSourceId(providerEventId: string): string {
  const hex = digest(`linq-v3\0signal\0${providerEventId}`);
  const variant = ((Number.parseInt(hex[16] ?? "0", 16) & 3) | 8).toString(16);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-${variant}${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}
