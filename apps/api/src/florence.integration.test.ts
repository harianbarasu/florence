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
} from "@florence/google";
import {
  type LinqClient,
  type LinqConversationAuthority,
  type LinqCreateChat,
  type LinqCreatedChat,
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
import type { FlorenceDecision, FlorenceReasoner } from "./reasoner.js";

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
const FOUNDER_IDENTITY = linqIdentitySubjectDigest(FOUNDER_HANDLE);
const PARTNER_IDENTITY = linqIdentitySubjectDigest(PARTNER_HANDLE);
const PRIVATE_FOUNDER = "linq-private-founder";
const PRIVATE_PARTNER = "linq-private-partner";
const FAMILY_GROUP = "linq-family-group";
const REPLACEMENT_GROUP = "linq-family-group-replacement";
const FAMILY_CALENDAR = "anbarasu-family@group.calendar.google.com";
const FOUNDER_GOOGLE = "44444444-4444-4444-8444-444444444444";
const PARTNER_GOOGLE = "44444444-4444-4444-8444-444444444445";
const LINQ_PARTNER = "partner-florence";
const LINQ_SIGNING_KEY = Buffer.from("florence-release-webhook-key-32b", "utf8");
const LINQ_SIGNING_SECRET = `whsec_${LINQ_SIGNING_KEY.toString("base64")}`;
const INVITE_APPROVAL = "Yes, please text Alex.";
const REINVITE_APPROVAL = "Please invite Alex again.";
const PARTNER_SETUP_QUESTION = "What is this setup for?";
const PARTNER_SETUP_REFUSAL = "I don’t want to join this.";
const PARTNER_SETUP_EXPLANATION =
  "That link sets up your own private side of Florence. Use the setup link just above when you’re ready.";
const PARTNER_SETUP_INCOMPLETE_NOTICE =
  "Alex didn’t complete Florence setup, so I stopped the invitation. I won’t message them again unless you ask me to.";
const NATIVE_TEXT = "Forwarded from school: Maya’s field-trip form is due Tuesday.";
const NATIVE_LINK = "https://school.example/fall-field-trip";
const VOICE_TRANSCRIPT = "The teacher said the form still needs one parent signature.";
const INTEREST_REQUEST = "Maya likes soccer. Keep an eye out for a good family match we could attend.";
const INTEREST_RECOMMENDATION =
  "The Bay City women’s match this Saturday fits the family calendar and looks worth considering.";
const INTEREST_URL = "https://example.com/bay-city-family-soccer";
const GROUP_REPAIR_NOTICE =
  "The people in our family thread changed, so I stopped using it. I’ll make a fresh thread with just the two of you.";
const INITIAL_REVIEW_OUTAGE_NOTICE =
  "I couldn’t finish checking your Gmail and calendar just now, so I’m not calling it all clear. I’ll keep trying.";
const PRIVATE_SCHOOL_FACT_SLOT = "child:maya:school";
const INITIAL_PRIVATE_SCHOOL_FACT = "Maya attends Muir Elementary.";
const UPDATED_PRIVATE_SCHOOL_FACT = "Maya attends Muir Academy.";
const ORDINARY_UNUSED_GMAIL_QUERY = "ordinary-unused-family-email";
const ORDINARY_UNUSED_GMAIL_QUESTION = "Is there anything useful in that family email?";
const OVERLAP_GMAIL_SUBJECT = "School bus route reminder";
const OVERLAP_GMAIL_FACT_SLOT = "child:maya:school_bus";

const AUTOMATIC_FAMILY_DATE = {
  intervalKind: "all_day" as const,
  title: "Maya’s field-trip form deadline",
  startDate: "2026-08-19",
  endDate: "2026-08-20",
  location: "Muir Elementary",
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
  privateReviews: Parameters<FlorenceReasoner["reviewPrivateGoogle"]>[0][];
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
  initialGoogleFailureAdultId: string | null;
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
  invalidGrantAdultId: string | null;
  invalidGrantTriggered: boolean;
};

const release = TEST_DATABASE_URL ? describe : describe.skip;

release("Florence parent journeys", () => {
  test("sets up two parents, asks once, then creates the family group and calendar once", async () => {
    const harness = await createHarness();
    await harness.setupFounder();
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
    await expect(
      harness.florence.completeFamilyOnboarding(harness.founderAdultId, {
        ...familyProfileInput(),
        familyLabel: "Client-chosen Family",
      } as never),
    ).rejects.toThrow();
    await harness.completeFamilyProfile();
    expect((await harness.florence.workspaceForAdult(harness.founderAdultId)).vault?.members).toEqual(
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
        }),
      ]),
    );
    await harness.drain();

    expect(harness.linq.createdChats).toHaveLength(0);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER &&
          message.text === "Want me to text Alex at ••••0202 so they can set up their side?",
      ),
    ).toHaveLength(1);

    harness.linq.partnerSetupLinkState = "accepted";
    await harness.accept("private", "approve-partner", INVITE_APPROVAL);
    await harness.drain();
    expect(
      harness.linq.createdChats.filter((chat) => chat.result.authority.audience === "private"),
    ).toHaveLength(1);
    expect(harness.linq.createdChats[0]).toMatchObject({
      input: { participantPhoneNumbers: [PARTNER_PHONE] },
      result: { providerConversationId: PRIVATE_PARTNER, authority: { audience: "private" } },
    });
    expect((await harness.florence.workspaceForAdult(harness.founderAdultId)).workspace.setup).toMatchObject({
      partnerInvitation: "approved",
    });
    harness.linq.partnerSetupLinkState = "sent";
    harness.state.now += 15_001;
    await harness.drain();
    expect((await harness.florence.workspaceForAdult(harness.founderAdultId)).workspace.setup).toMatchObject({
      partnerInvitation: "invited",
    });
    const firstPartnerSetupToken = harness.setupTokenFor(PRIVATE_PARTNER);
    const issuedPartnerSetupLinks = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_PARTNER && message.text.includes("#s="),
    ).length;
    const partnerMessagesBeforeTransient = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_PARTNER,
    ).length;
    harness.state.setupConversationFailuresRemaining = 1;
    await expect(
      harness.receiveParts(
        "partner-invite-question",
        [{ type: "text", value: PARTNER_SETUP_QUESTION }],
        PRIVATE_PARTNER,
        "partner",
      ),
    ).rejects.toMatchObject({ retryable: true });
    expect(
      harness.linq.messages.filter((message) => message.providerConversationId === PRIVATE_PARTNER),
    ).toHaveLength(partnerMessagesBeforeTransient);
    expect((await harness.florence.workspaceForAdult(harness.founderAdultId)).workspace.setup).toMatchObject({
      partnerInvitation: "invited",
    });
    await harness.receiveParts(
      "partner-invite-question",
      [{ type: "text", value: PARTNER_SETUP_QUESTION }],
      PRIVATE_PARTNER,
      "partner",
    );
    await harness.drain();
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === PRIVATE_PARTNER && message.text.includes("#s="),
      ),
    ).toHaveLength(issuedPartnerSetupLinks);
    const setupExplanation = harness.linq.messages.findLast(
      (message) => message.providerConversationId === PRIVATE_PARTNER,
    );
    expect(setupExplanation?.text).toBe(PARTNER_SETUP_EXPLANATION);
    expect(setupExplanation?.text).not.toMatch(/Anbarasu|De la Cruz|Maya|school|schedule|calendar/i);

    const partnerMessagesBeforeRefusal = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_PARTNER,
    ).length;
    await harness.receiveParts(
      "partner-invite-refusal",
      [{ type: "text", value: PARTNER_SETUP_REFUSAL }],
      PRIVATE_PARTNER,
      "partner",
    );
    await harness.drain();
    expect(
      harness.linq.messages.filter((message) => message.providerConversationId === PRIVATE_PARTNER),
    ).toHaveLength(partnerMessagesBeforeRefusal);
    expect(await harness.redeemPartnerSetup(firstPartnerSetupToken)).toBeNull();
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER &&
          message.text === PARTNER_SETUP_INCOMPLETE_NOTICE,
      ),
    ).toHaveLength(1);
    expect((await harness.florence.workspaceForAdult(harness.founderAdultId)).workspace.setup).toMatchObject({
      partnerInvitation: "ready",
    });
    const privateChatsAfterRefusal = harness.linq.createdChats.filter(
      (chat) => chat.result.authority.audience === "private",
    ).length;
    await harness.drain();
    expect(
      harness.linq.createdChats.filter((chat) => chat.result.authority.audience === "private"),
    ).toHaveLength(privateChatsAfterRefusal);

    await harness.accept("private", "reinvite-partner-after-refusal", REINVITE_APPROVAL);
    await harness.drain();
    const secondPartnerSetupToken = harness.setupTokenFor(PRIVATE_PARTNER);
    expect(secondPartnerSetupToken).not.toBe(firstPartnerSetupToken);
    const partnerMessagesBeforeStop = harness.linq.messages.filter(
      (message) => message.providerConversationId === PRIVATE_PARTNER,
    ).length;
    const stop = await harness.receiveParts(
      "partner-invite-stop",
      [{ type: "text", value: "STOP" }],
      PRIVATE_PARTNER,
      "partner",
    );
    expect(stop).toEqual({ disposition: "acknowledged", reason: "opted_out" });
    await harness.drain();
    expect(
      harness.linq.messages.filter((message) => message.providerConversationId === PRIVATE_PARTNER),
    ).toHaveLength(partnerMessagesBeforeStop);
    expect(await harness.redeemPartnerSetup(secondPartnerSetupToken)).toBeNull();
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === PRIVATE_FOUNDER &&
          message.text === PARTNER_SETUP_INCOMPLETE_NOTICE,
      ),
    ).toHaveLength(2);

    await harness.accept("private", "reinvite-partner-after-stop", REINVITE_APPROVAL);
    await harness.drain();

    await harness.setupPartner();
    await harness.activatePartnerGoogle();
    await harness.drain();

    const groups = harness.linq.createdChats.filter((chat) => chat.result.authority.audience === "group");
    expect(groups).toHaveLength(1);
    expect(groups[0]).toMatchObject({
      input: { participantPhoneNumbers: [FOUNDER_PHONE, PARTNER_PHONE] },
      result: {
        providerConversationId: FAMILY_GROUP,
        authority: {
          audience: "group",
          participantIdentityDigests: [FOUNDER_IDENTITY, PARTNER_IDENTITY].sort(),
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
    ]);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          message.text ===
            "I made the De la Cruz–Anbarasu Family calendar too. Either of you can ask me to add or change family plans here.",
      ),
    ).toHaveLength(1);

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
    });
    expect(partner.workspace.setup).toEqual(founder.workspace.setup);

    const visibleCounts = {
      chats: harness.linq.createdChats.length,
      messages: harness.linq.messages.length,
      provisionings: harness.state.provisionings.length,
    };
    await harness.drain();
    expect({
      chats: harness.linq.createdChats.length,
      messages: harness.linq.messages.length,
      provisionings: harness.state.provisionings.length,
    }).toEqual(visibleCounts);
  }, 20_000);

  test("gets ahead from both parents’ context, native inputs, a monitor, and the read-only calendar", async () => {
    let nativeInputWasRead = false;
    let ordinaryUnusedSourceId: string | null = null;
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
    const harness = await createHarness(async (input, reads) => {
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
    await harness.readyHousehold();

    expect(
      (await harness.florence.workspaceForAdult(harness.founderAdultId)).vault?.watches.filter(
        (watch) => watch.kind === "interest",
      ),
    ).toHaveLength(2);

    expect(harness.state.briefings).toHaveLength(0);
    expect(
      harness.linq.messages.filter((message) => message.text === INITIAL_REVIEW_OUTAGE_NOTICE),
    ).toHaveLength(1);
    harness.state.now += 16_000;
    await harness.drain();
    expect(harness.state.briefings).toHaveLength(0);
    expect(
      harness.linq.messages.filter((message) => message.text === INITIAL_REVIEW_OUTAGE_NOTICE),
    ).toHaveLength(1);
    harness.state.now += 16_000;
    await harness.drain();

    expect(harness.state.privateReviews.map((review) => review.adult.firstName).sort()).toEqual([
      "Alex",
      "Hari",
    ]);
    expect(harness.state.briefings).toHaveLength(1);
    expect(JSON.stringify(harness.state.briefings[0])).not.toMatch(
      /hari-private@example\.com|alex-private@example\.com|private calendar detail/i,
    );
    const briefing = harness.linq.messages.find(
      (message) =>
        message.providerConversationId === FAMILY_GROUP && message.text.startsWith("Here’s what I found"),
    );
    expect(briefing?.text).toContain("Maya’s permission-slip deadline is Tuesday");
    expect(briefing?.text).toContain("Did I get that right?");
    expect(briefing?.text).not.toMatch(/@example\.com|private calendar detail/i);

    const founderAfterInitialReview = await harness.florence.workspaceForAdult(harness.founderAdultId);
    const initialPrivateFact = founderAfterInitialReview.vault?.facts.find(
      (fact) => fact.statement === INITIAL_PRIVATE_SCHOOL_FACT,
    );
    expect(initialPrivateFact).toMatchObject({
      visibility: "private",
      source: { kind: "gmail" },
    });
    if (!initialPrivateFact) throw new Error("The initial private Gmail fact was not retained");
    expect(
      (await harness.florence.workspaceForAdult(harness.partnerAdultId)).vault?.facts.some(
        (fact) => fact.id === initialPrivateFact.id,
      ),
    ).toBe(false);

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

    await harness.accept("private", "ordinary-unused-email", ORDINARY_UNUSED_GMAIL_QUESTION);
    await harness.drain();
    if (!ordinaryUnusedSourceId) throw new Error("The ordinary Gmail source was not observed");
    await harness.assertDatabase(
      "An uncited ordinary Gmail answer retained its source",
      `not exists (select 1 from sources where id=${sqlLiteral(ordinaryUnusedSourceId)}::uuid)`,
    );

    harness.state.privateFactUpdatePending = true;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    const founderAfterFactUpdate = await harness.florence.workspaceForAdult(harness.founderAdultId);
    const updatedPrivateFact = founderAfterFactUpdate.vault?.facts.find(
      (fact) => fact.statement === UPDATED_PRIVATE_SCHOOL_FACT,
    );
    expect(updatedPrivateFact).toMatchObject({
      id: initialPrivateFact.id,
      visibility: "private",
      source: { kind: "gmail" },
    });
    expect(updatedPrivateFact?.source.id).not.toBe(initialPrivateFact.source.id);
    expect(
      (await harness.florence.workspaceForAdult(harness.partnerAdultId)).vault?.facts.some(
        (fact) => fact.id === initialPrivateFact.id,
      ),
    ).toBe(false);
    expect(
      harness.state.googleAssessments.find(
        (assessment) => assessment.adult.adultId === harness.founderAdultId,
      )?.currentPrivateFacts,
    ).toEqual(
      expect.arrayContaining([{ slot: PRIVATE_SCHOOL_FACT_SLOT, statement: INITIAL_PRIVATE_SCHOOL_FACT }]),
    );
    await harness.assertDatabase(
      "The cited incremental Gmail fact was not retained exactly once",
      `(
        select count(*)=1 from sources
        where kind='gmail' and metadata->>'messageId'='gmail-maya-school-enrollment-update'
      )`,
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
          and gmail_cursor::jsonb->>'historyId'='103'
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
    expect(interests).toHaveLength(3);
    expect(interests).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: "interest",
          objective: "Find a worthwhile local soccer outing for the family.",
          status: "active",
        }),
      ]),
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
    expect(harness.state.interestResearches).toBe(3);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP &&
          message.text === `${INTEREST_RECOMMENDATION}\n\n${INTEREST_URL}`,
      ),
    ).toHaveLength(2);

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

    harness.state.invalidGrantAdultId = harness.founderAdultId;
    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(harness.state.invalidGrantTriggered).toBe(true);
    const reconnectText =
      "Your Google connection stopped working. Reconnect it in Florence settings so I can keep helping with family plans.";
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === PRIVATE_FOUNDER && message.text === reconnectText,
      ),
    ).toHaveLength(1);
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === FAMILY_GROUP && /Google|reconnect/i.test(message.text),
      ),
    ).toHaveLength(0);
    expect(
      harness.linq.messages.filter((message) => message.text === INITIAL_REVIEW_OUTAGE_NOTICE),
    ).toHaveLength(1);
    expect(
      (await harness.florence.workspaceForAdult(harness.founderAdultId)).workspace.googleConnections,
    ).toHaveLength(0);
    harness.state.now += 2 * 60_000;
    await harness.drain();
    expect(
      harness.linq.messages.filter(
        (message) => message.providerConversationId === PRIVATE_FOUNDER && message.text === reconnectText,
      ),
    ).toHaveLength(1);

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
      events: [expect.objectContaining({ title: AUTOMATIC_FAMILY_DATE.title })],
    });

    const familyCalendarReconnectText =
      "The family calendar is paused because neither Google account is connected. Either of you can reconnect in Florence settings.";
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
  }, 20_000);

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
      participantIdentityDigests: [FOUNDER_IDENTITY, outsiderIdentity].sort(),
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
          participantIdentityDigests: [FOUNDER_IDENTITY, PARTNER_IDENTITY].sort(),
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
    return [FOUNDER_IDENTITY, PARTNER_IDENTITY].sort();
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
    return {
      providerConversationId: isGroup ? FAMILY_GROUP : partner ? PRIVATE_PARTNER : PRIVATE_FOUNDER,
      audience,
      participantIdentityDigests: isGroup
        ? this.participants
        : [partner ? PARTNER_IDENTITY : FOUNDER_IDENTITY],
      senderIdentitySubjectDigest: partner ? PARTNER_IDENTITY : FOUNDER_IDENTITY,
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
    const senderHandle = sender === "partner" ? PARTNER_HANDLE : FOUNDER_HANDLE;
    const senderPhone = sender === "partner" ? PARTNER_PHONE : FOUNDER_PHONE;
    const authority = this.linq.authorities.get(providerConversationId);
    if (!authority) throw new Error(`Unknown provider conversation ${providerConversationId}`);
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
    for (let index = 0; index < 50 && idle < 2; index += 1) {
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

class FakeLinq {
  readonly authorities = new Map<string, LinqConversationAuthority>();
  readonly createdChats: { input: LinqCreateChat; result: LinqCreatedChat }[] = [];
  readonly messages: LinqSendMessage[] = [];
  readonly reactions: LinqSendReaction[] = [];
  readonly media = new Map<string, { reference: LinqMediaReference; bytes: Uint8Array }>();
  partnerSetupLinkState: "accepted" | "sent" = "sent";
  readonly #created = new Map<string, LinqCreatedChat>();
  readonly #sent = new Map<string, Awaited<ReturnType<LinqClient["sendMessage"]>>>();

  constructor(readonly state: HarnessState) {}

  async createChat(input: LinqCreateChat): Promise<LinqCreatedChat> {
    const prior = this.#created.get(input.idempotencyKey);
    if (prior) return prior;
    const privateChat = input.participantPhoneNumbers.length === 1;
    const groupCount = this.createdChats.filter((chat) => chat.result.authority.audience === "group").length;
    const providerConversationId = privateChat
      ? PRIVATE_PARTNER
      : groupCount === 0
        ? FAMILY_GROUP
        : REPLACEMENT_GROUP;
    const participantIdentityDigests = privateChat
      ? [PARTNER_IDENTITY]
      : [FOUNDER_IDENTITY, PARTNER_IDENTITY].sort();
    const participants = participantIdentityDigests.map((identitySubjectDigest) => ({
      identitySubjectDigest,
      phoneNumber: identitySubjectDigest === FOUNDER_IDENTITY ? FOUNDER_PHONE : PARTNER_PHONE,
    }));
    const authority = {
      audience: privateChat ? ("private" as const) : ("group" as const),
      ownerPhoneNumber: FLORENCE_PHONE,
      participantIdentityDigests,
      participants,
    };
    const result: LinqCreatedChat = {
      providerConversationId,
      authority,
      initialMessage: {
        idempotencyKey: input.idempotencyKey,
        providerMessageId: `created-chat-message-${this.createdChats.length + 1}`,
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
    this.#created.set(input.idempotencyKey, result);
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
        phoneNumber:
          identitySubjectDigest === FOUNDER_IDENTITY
            ? FOUNDER_PHONE
            : identitySubjectDigest === PARTNER_IDENTITY
              ? PARTNER_PHONE
              : OUTSIDER_PHONE,
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
    const partnerSetupLink = input.text.includes("#s=");
    const prior = this.#sent.get(input.idempotencyKey);
    if (prior) {
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
        this.#sent.set(input.idempotencyKey, sent);
        return sent;
      }
      return prior;
    }
    expect(await this.observeChat(input.providerConversationId)).toMatchObject(input.expectedAuthority);
    this.messages.push(input);
    this.state.timeline.push(`message:${input.text}`);
    const result: Awaited<ReturnType<LinqClient["sendMessage"]>> = {
      status: "committed" as const,
      providerState: partnerSetupLink ? this.partnerSetupLinkState : "accepted",
      idempotencyKey: input.idempotencyKey,
      providerReceiptId: `sent-${this.messages.length}`,
      detail: null,
      occurredAt: new Date(this.state.now).toISOString(),
    };
    this.#sent.set(input.idempotencyKey, result);
    return result;
  }

  async sendReaction(input: LinqSendReaction) {
    this.reactions.push(input);
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

async function createHarness(reason: Reason = async () => decision()): Promise<Harness> {
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
    now: NOW,
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
    initialGoogleFailureAdultId: null,
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
    invalidGrantAdultId: null,
    invalidGrantTriggered: false,
  };
  const linq = new FakeLinq(state);
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
      if (input.stage === "partner_invited" && state.setupConversationFailuresRemaining > 0) {
        state.setupConversationFailuresRemaining -= 1;
        throw new Error("Fake setup interpreter is temporarily unavailable");
      }
      const declineInvitation =
        input.stage === "partner_invited" && input.currentMessage.text === PARTNER_SETUP_REFUSAL;
      return {
        stopMessaging: false,
        declineInvitation,
        bubbles: declineInvitation
          ? []
          : [
              {
                text:
                  input.stage === "partner_invited"
                    ? PARTNER_SETUP_EXPLANATION
                    : "Finish the short setup page and I’ll keep going here.",
                delayMs: 0,
              },
            ],
      };
    },
    interpretCalendarApproval: async () => ({ approve: false }),
    interpretPartnerInvitationApproval: async (
      input: Parameters<FlorenceReasoner["interpretPartnerInvitationApproval"]>[0],
    ) => ({
      sendInvitation:
        input.currentMessage.text === INVITE_APPROVAL || input.currentMessage.text === REINVITE_APPROVAL,
    }),
    reviewPrivateGoogle: async (
      input: Parameters<FlorenceReasoner["reviewPrivateGoogle"]>[0],
      reads: Parameters<FlorenceReasoner["reviewPrivateGoogle"]>[1],
    ) => {
      state.privateReviews.push(input);
      const current = Date.parse(input.currentTime);
      const recent = await reads.searchGmail({
        connectionId: input.googleConnection.connectionId,
        query: "(school OR activity OR form) -category:promotions -category:social",
        after: new Date(current - 14 * 24 * 60 * 60_000).toISOString(),
        before: input.currentTime,
        limit: 10,
      });
      await reads.searchGmail({
        connectionId: input.googleConnection.connectionId,
        query: "(school OR activity OR family) -category:promotions -category:social",
        after: new Date(current - 90 * 24 * 60 * 60_000).toISOString(),
        before: new Date(current - 14 * 24 * 60 * 60_000).toISOString(),
        limit: 10,
      });
      await reads.readPersonalCalendarWindow({
        connectionId: input.googleConnection.connectionId,
        timeMin: input.currentTime,
        timeMax: new Date(current + 21 * 24 * 60 * 60_000).toISOString(),
        limit: 50,
      });
      const source = recent[0];
      if (!source) throw new Error("Initial review did not receive Gmail evidence");
      const attachment = source.attachments[0];
      if (attachment) {
        const opened = await reads.readGmailAttachment({
          connectionId: input.googleConnection.connectionId,
          sourceId: source.sourceId,
          attachment,
        });
        expect(opened.bytes).toEqual(PDF_BYTES);
      }
      const founder = input.adult.firstName === "Hari";
      return {
        bubbles: [
          {
            text: `${input.adult.firstName}, I checked your side and found one family item worth tracking.`,
            delayMs: 0,
          },
        ],
        findings: [
          {
            privateSummary: founder
              ? "Hari’s private school email has the original form."
              : "Alex’s private calendar detail stays private.",
            sourceIds: [source.sourceId],
            candidate: {
              category: founder ? ("loose_end" as const) : ("deadline" as const),
              summary: founder
                ? "Maya’s field-trip form still needs a signature."
                : "Maya’s permission-slip deadline is Tuesday.",
              urgency: "soon" as const,
              dueAt: "2026-08-19T16:00:00.000Z",
              needsAnswer: true,
            },
            monitor: null,
            familyCalendar: founder
              ? null
              : {
                  disposition: "automatic" as const,
                  sourceIds: [source.sourceId],
                  event: AUTOMATIC_FAMILY_DATE,
                },
          },
        ],
        facts: founder
          ? [
              {
                slot: PRIVATE_SCHOOL_FACT_SLOT,
                statement: INITIAL_PRIVATE_SCHOOL_FACT,
                sourceIds: [source.sourceId],
              },
            ]
          : [],
      };
    },
    synthesizeHouseholdBriefing: async (
      input: Parameters<FlorenceReasoner["synthesizeHouseholdBriefing"]>[0],
    ) => {
      state.briefings.push(input);
      return {
        selectedCandidateIds: input.candidates.map((candidate) => candidate.candidateId).slice(0, 3),
        bubbles: [
          {
            text: `Here’s what I found:\n${input.candidates
              .map((candidate) => `– ${candidate.summary}`)
              .join("\n")}\n\nDid I get that right?`,
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
      if (overlap) {
        state.overlapGmailAssessments += 1;
        state.overlapGmailSourceId = overlap.sourceId;
      }
      return {
        findings: [],
        facts: source
          ? [
              {
                slot: PRIVATE_SCHOOL_FACT_SLOT,
                statement: UPDATED_PRIVATE_SCHOOL_FACT,
                sourceIds: [source.sourceId],
              },
            ]
          : overlap && state.overlapGmailAssessments === 2
            ? [
                {
                  slot: OVERLAP_GMAIL_FACT_SLOT,
                  statement: "Maya’s school bus route reminder is current.",
                  sourceIds: [overlap.sourceId],
                },
              ]
            : [],
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
    const events = input.calendarId
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
      return { connection: disconnected.view, providerRevocation: "not-needed" as const };
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
      return store.activate({
        connectionId: pending.connectionId,
        stateDigest,
        googleSubjectDigest: digest(founder ? "google-founder" : "google-partner"),
        emailLabel: founder ? "hari@example.com" : "alex@example.com",
        grantedScopes: GOOGLE_SCOPES,
        refreshTokenEnvelope: founder ? "encrypted-founder-token" : "encrypted-partner-token",
        now: input.now,
      });
    },
    provisionFamilyCalendar: async (
      input: FamilyCalendarProvisioningInput,
    ): Promise<GoogleFamilyCalendarProvisioningResult> => {
      state.provisionings.push(input);
      const creation = await store.beginFamilyCalendarCreation({
        householdId: input.householdId,
        now: new Date(state.now).toISOString(),
      });
      expect(creation).toEqual({ createAllowed: true, calendarId: null });
      state.providerCalendarSummary = input.summary;
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
      const founder = input.ownerAdultId === founderSetup().adultId;
      const recent =
        input.after && input.before
          ? Date.parse(input.before) - Date.parse(input.after) <= 15 * 24 * 60 * 60_000
          : true;
      const ordinaryUnused = input.query === ORDINARY_UNUSED_GMAIL_QUERY;
      const messageId = ordinaryUnused
        ? "gmail-ordinary-unused"
        : founder && recent
          ? SCHOOL_ATTACHMENT.messageId
          : `gmail-${input.ownerAdultId}-${recent}`;
      return {
        status: "complete" as const,
        messages: [
          {
            messageId,
            threadId: `thread-${messageId}`,
            historyId: founder ? "101" : "201",
            from: founder ? "hari-private@example.com" : "alex-private@example.com",
            subject: ordinaryUnused
              ? "Ordinary family newsletter"
              : founder
                ? "Private school form"
                : "Private family schedule",
            sentAt: input.before ?? new Date(state.now).toISOString(),
            text: ordinaryUnused
              ? "This newsletter has no family action Florence should keep."
              : founder
                ? "Hari private email: Maya attends Muir Elementary and her form needs a signature."
                : "Alex private email: a personal appointment moved.",
            textStatus: "complete" as const,
            attachmentsStatus: "complete" as const,
            attachments: !ordinaryUnused && founder && recent ? [SCHOOL_ATTACHMENT] : [],
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
      return baseline(input);
    },
    readGmailChanges: async (input: {
      householdId: string;
      ownerAdultId: string;
      connectionId: string;
      cursor: { kind: "gmail_history_v1"; historyId: string; capturedAt: string };
    }) => {
      await activeCredential(input);
      const hasPrivateFactUpdate =
        input.ownerAdultId === founderSetup().adultId &&
        state.privateFactUpdatePending &&
        !state.privateFactUpdateDelivered;
      if (hasPrivateFactUpdate) state.privateFactUpdateDelivered = true;
      const hasOverlap =
        !hasPrivateFactUpdate &&
        input.ownerAdultId === founderSetup().adultId &&
        state.overlapGmailReadsRemaining > 0;
      const overlapHistoryId = state.overlapGmailReadsRemaining === 2 ? "103" : "104";
      if (hasOverlap) state.overlapGmailReadsRemaining -= 1;
      return {
        status: "complete" as const,
        resyncRequired: false as const,
        messages: hasPrivateFactUpdate
          ? [
              {
                messageId: "gmail-maya-school-enrollment-update",
                threadId: "gmail-maya-school-enrollment-thread",
                historyId: "102",
                from: "registrar@muir.example",
                subject: "Maya school enrollment update",
                sentAt: new Date(state.now).toISOString(),
                text: UPDATED_PRIVATE_SCHOOL_FACT,
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
        cursor: hasPrivateFactUpdate
          ? { ...input.cursor, historyId: "102", capturedAt: new Date(state.now).toISOString() }
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
      return {
        status: "complete" as const,
        resyncRequired: false as const,
        events: [],
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

function decision(
  input: {
    bubbles?: FlorenceDecision["conversation"]["bubbles"];
    facts?: FlorenceDecision["facts"];
    followUp?: FlorenceDecision["followUp"];
    interest?: FlorenceDecision["interest"];
    calendar?: FlorenceDecision["calendar"];
    householdUpdate?: FlorenceDecision["householdUpdate"];
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
    partner: { firstName: "Alex", lastName: "Anbarasu", phoneNumber: PARTNER_PHONE },
    children: [
      {
        firstName: "Maya",
        lastName: "Anbarasu",
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
