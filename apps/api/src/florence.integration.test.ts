import { createHash, createHmac, randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { EncryptedImageVault } from "@florence/artifacts";
import {
  FlorenceStoreConflict,
  FlorenceStoreUnauthorized,
  migrateDatabase,
  migrationFiles,
  PostgresFlorenceStore,
} from "@florence/database";
import {
  type GmailAttachmentReference,
  GOOGLE_SCOPES,
  type GoogleCalendarExecutionResult,
  GoogleConnection,
  type GoogleConnectionStore,
  type GoogleFamilyCalendarProvisioningResult,
} from "@florence/google";
import {
  type LinqClient,
  type LinqConversationAuthority,
  type LinqCreateChat,
  type LinqCreatedChat,
  type LinqSendMessage,
  type LinqSendReaction,
  linqIdentitySubjectDigest,
} from "@florence/linq";
import { describe, expect, onTestFinished, test } from "vitest";
import { buildApp, createSessionCallerResolver } from "./app.js";
import { EnrollmentCodes } from "./enrollment.js";
import { Florence } from "./florence.js";
import { createLinqIngress } from "./linq-ingress.js";
import { type FlorenceDecision, type FlorenceReasoner, FlorenceReasonerError } from "./reasoner.js";

const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL;
const NOW = Date.parse("2026-08-16T18:00:00.000Z");
const ENROLLMENT_SECRET = "release-journey-secret-is-at-least-thirty-two-bytes";
const SESSION_SECRET = "release-journey-browser-session-secret-is-at-least-thirty-two-bytes";
const ADULT_ONE_HANDLE = "messages-adult-one";
const ADULT_TWO_HANDLE = "messages-adult-two";
const COMPETING_FOUNDER_HANDLE = "messages-competing-founder";
const ADULT_ONE_IDENTITY = linqIdentitySubjectDigest(ADULT_ONE_HANDLE);
const ADULT_TWO_IDENTITY = linqIdentitySubjectDigest(ADULT_TWO_HANDLE);
const COMPETING_FOUNDER_IDENTITY = linqIdentitySubjectDigest(COMPETING_FOUNDER_HANDLE);
const PRIVATE_ONE = "linq-private-one";
const PRIVATE_TWO = "linq-private-two";
const PRIVATE_COMPETING_FOUNDER = "linq-private-competing-founder";
const FLORENCE_PHONE = "+15555550000";
const ADULT_ONE_PHONE = "+15555550101";
const ADULT_TWO_PHONE = "+15555550202";
const COMPETING_FOUNDER_PHONE = "+15555550303";
const FOUNDER_IDS = new EnrollmentCodes(ENROLLMENT_SECRET).issueFounderSetup({
  providerConversationId: PRIVATE_ONE,
  identitySubjectDigest: ADULT_ONE_IDENTITY,
  occurredAt: new Date(NOW).toISOString(),
});
const ADULT_ONE = FOUNDER_IDS.adultId;
const COMPETING_FOUNDER = new EnrollmentCodes(ENROLLMENT_SECRET).issueFounderSetup({
  providerConversationId: PRIVATE_COMPETING_FOUNDER,
  identitySubjectDigest: COMPETING_FOUNDER_IDENTITY,
  occurredAt: new Date(NOW).toISOString(),
}).adultId;
const GOOGLE_CONNECTION = "44444444-4444-4444-8444-444444444444";
const PARTNER_GOOGLE_CONNECTION = "44444444-4444-4444-8444-444444444445";
const FAMILY_CALENDAR = "anbarasu-family@group.calendar.google.com";
const GROUP = "linq-group-family";
const LINQ_PARTNER = "partner-florence";
const LINQ_SIGNING_KEY = Buffer.from("florence-release-webhook-key-32b", "utf8");
const LINQ_SIGNING_SECRET = `whsec_${LINQ_SIGNING_KEY.toString("base64")}`;
const EVENT = {
  title: "School assembly",
  startsAt: "2026-08-18T17:00:00.000Z",
  endsAt: "2026-08-18T18:00:00.000Z",
  timeZone: "America/Los_Angeles",
  location: "Muir Elementary",
};
const COMPOUND_CALENDAR_APPROVAL = "Yep, add it — and remind me that morning.";
const FAMILY_CALENDAR_REQUEST =
  "Alex here—add the school assembly to our family calendar exactly as written.";
const CALENDAR_MORNING_REMINDER_AT = "2026-08-18T15:00:00.000Z";
const CALENDAR_MORNING_REMINDER = "School assembly is this morning at 10:00.";
const TRIP_WINDOW = {
  timeMin: "2026-08-21T22:30:00.000Z",
  timeMax: "2026-08-21T23:30:00.000Z",
};
const PICKUP_CONFLICT = {
  title: "Both parents unavailable",
  startsAt: "2026-08-21T22:30:00.000Z",
  endsAt: "2026-08-21T23:30:00.000Z",
  allDay: false,
};
const SCHOOL_EMAIL = {
  messageId: "gmail-school-packet",
  threadId: "gmail-school-thread",
  historyId: "gmail-history-1",
  from: "Muir Elementary <office@muir.example>",
  subject: "Field trip permission slip",
  sentAt: "2026-08-15T17:00:00.000Z",
  text: "Permission slip due Tuesday. Friday's bus returns at 3:45.",
  textStatus: "complete" as const,
  attachmentsStatus: "complete" as const,
  attachments: [],
};
const SCHOOL_PDF_BYTES = new TextEncoder().encode(
  "%PDF-1.7\nMuir permission slip due Tuesday. Soccer clinic is Saturday.\n%%EOF\n",
);
const SCHOOL_PDF_ATTACHMENT: GmailAttachmentReference = {
  messageId: "gmail-initial-school-packet",
  threadId: "gmail-initial-school-thread",
  historyId: "gmail-history-initial-founder",
  partId: "2",
  attachmentId: "gmail-school-pdf",
  storage: "external",
  filename: "school-packet.pdf",
  mimeType: "application/pdf",
  sizeBytes: SCHOOL_PDF_BYTES.byteLength,
};
const NATURAL_SETUP_OPT_OUT = "I don’t want Florence to message me here anymore.";
const PARTNER_INVITATION_APPROVAL =
  "Yes—please text Alex. And once we’re both in, tell us which school date needs attention first.";

type Reason = FlorenceReasoner["decide"];
type SetupConversation = FlorenceReasoner["converseDuringSetup"];
type InterpretCalendarApproval = FlorenceReasoner["interpretCalendarApproval"];
type InterpretPartnerInvitationApproval = FlorenceReasoner["interpretPartnerInvitationApproval"];
type SetupConversationInput = Parameters<SetupConversation>[0];
type CalendarApprovalInput = Parameters<InterpretCalendarApproval>[0];
type PartnerInvitationApprovalInput = Parameters<InterpretPartnerInvitationApproval>[0];
type PrivateGoogleReview = FlorenceReasoner["reviewPrivateGoogle"];
type HouseholdBriefing = FlorenceReasoner["synthesizeHouseholdBriefing"];
type PrivateGoogleReviewInput = Parameters<PrivateGoogleReview>[0];
type HouseholdBriefingInput = Parameters<HouseholdBriefing>[0];
type FamilyCalendarProvisioningInput = Parameters<GoogleConnection["provisionFamilyCalendar"]>[0];
type CalendarReadInput = Parameters<GoogleConnection["readCalendarWindow"]>[0];
type CalendarExecutionInput = Parameters<GoogleConnection["executeCalendar"]>[0] & {
  audience: "private" | "household";
};

const release = TEST_DATABASE_URL ? describe : describe.skip;

release("Florence release journeys", () => {
  test("runs the real two-adult household journey from an interruptible document turn through Calendar proof", async () => {
    await expectCanonicalGoogleScopeAcceptance();
    let pdfWasRead = false;
    let reactionWasUnderstood = false;
    let calendarWasRead = false;
    let gmailWasRead = false;
    let obsoleteResultWasSuppressed = false;
    const harness = await freshHarness(
      async (input, reads, signal) => {
        const sourceId = input.currentMessage.sourceId;
        if (input.currentMessage.moveKind === "reaction") {
          if (input.currentMessage.replyTo?.text === "I’m looking through this now.") {
            expect(input.audience).toBe("private");
            return decision({ policy: noMutationPolicy() });
          }
          reactionWasUnderstood = true;
          expect(input.currentMessage.replyTo).toMatchObject({
            senderName: "Florence",
            text: "Family thread is connected.",
          });
          if (input.currentMessage.text === "Reacted like") {
            return decision({
              policy: noMutationPolicy(),
              reaction: "laugh",
              reply: true,
              bubbles: [{ text: "That made me smile.", delayMs: 0 }],
            });
          }
          expect(input.currentMessage.text).toBe("Reacted love");
          return decision({
            policy: noMutationPolicy(),
            bubbles: [{ text: "A reaction should only be conversational.", delayMs: 0 }],
            facts: [remember("A tapback changed family memory", sourceId)],
            followUp: {
              operation: "cancel",
              followUpId: input.pendingFollowUps[0]?.followUpId ?? "missing",
              at: null,
              text: null,
              sourceIds: [sourceId],
            },
            calendar: calendarDraft("direct", sourceId, {
              calendarId: FAMILY_CALENDAR,
              audience: "household",
            }),
          });
        }
        if (input.currentMessage.text.includes("Review this school packet")) {
          const pdf = input.currentMessage.pdfs?.[0];
          if (!pdf || !reads.readCurrentPdf) throw new Error("The attached PDF was not readable");
          const opened = await reads.readCurrentPdf(pdf);
          pdfWasRead = new TextDecoder().decode(opened.bytes).includes("bus returns at 3:45");
          expect((await reads.readSource({ sourceId }))?.visibility).toBe("adult_private");
          await waitForAbort(signal);
          return decision({
            bubbles: [{ text: "This obsolete answer must never be sent.", delayMs: 0 }],
          });
        }
        if (
          input.currentMessage.text.includes("Alex is unavailable too") ||
          input.currentMessage.text.includes("Sam can cover")
        ) {
          expect(input.recentMessages).toEqual(
            expect.arrayContaining([
              expect.objectContaining({ text: expect.stringContaining("Review this school packet") }),
            ]),
          );
          const pdf = input.currentMessage.pdfs?.[0];
          if (!pdf || !reads.readCurrentPdf) throw new Error("The superseded PDF was not carried forward");
          const opened = await reads.readCurrentPdf(pdf);
          pdfWasRead = pdfWasRead && new TextDecoder().decode(opened.bytes).includes("Ms. Chen");
          const calendar = await reads.readCalendarWindow({
            connectionId: GOOGLE_CONNECTION,
            ...TRIP_WINDOW,
            limit: 50,
          });
          expect(calendar).toEqual({ status: "complete", events: [PICKUP_CONFLICT] });
          calendarWasRead = true;
          if (input.currentMessage.text.includes("Sam can cover")) {
            const gmail = await reads.searchGmail({
              connectionId: GOOGLE_CONNECTION,
              query: 'newer_than:30d ("Muir" OR "field trip")',
              limit: 5,
            });
            expect(gmail).toEqual([
              expect.objectContaining({
                kind: "gmail",
                visibility: "adult_private",
                label: SCHOOL_EMAIL.subject,
                text: SCHOOL_EMAIL.text,
              }),
            ]);
            gmailWasRead = true;
          }
          obsoleteResultWasSuppressed = true;
          if (input.currentMessage.text.includes("Sam can cover")) {
            return decision({
              policy: noMutationPolicy(),
              bubbles: [
                {
                  text: "Updated: Sam can cover the 3:45 pickup, so the pickup conflict is cleared.",
                  delayMs: 0,
                },
                {
                  text: "Tuesday’s permission-slip deadline still needs attention. Ms. Chen is a stable school contact; Friday’s trip and return are one-offs. I saved neither.",
                  delayMs: 250,
                },
                {
                  text: "Updated draft to Ms. Chen: “Sam can cover Maya’s 3:45 pickup. Please let me know if the bus returns late.”",
                  delayMs: 400,
                },
              ],
              facts: [remember("Ms. Chen is Maya’s teacher", sourceId)],
              followUp: {
                operation: "schedule",
                followUpId: null,
                at: new Date(Date.parse(input.currentMessage.occurredAt) + 60_000).toISOString(),
                text: "This prohibited reminder must never be sent.",
                sourceIds: [sourceId],
              },
              calendar: calendarDraft("direct", sourceId),
            });
          }
          return decision({
            policy: noMutationPolicy(),
            reaction: "love",
            reply: true,
            bubbles: [
              {
                text: "The real issue is the unsupervised pickup gap after the bus returns at 3:45 — both of you are unavailable.",
                delayMs: 0,
              },
              {
                text: "Tuesday’s permission-slip deadline is actionable. Ms. Chen is a stable school contact; Friday’s trip and 3:45 return are one-offs. I saved neither.",
                delayMs: 250,
              },
              {
                text: "Draft to Ms. Chen: “Both of us are unavailable at 3:45. Can Maya stay with the bus group until 4:15?”\n\nShould I change the draft to name an alternate pickup adult?",
                delayMs: 400,
              },
            ],
            facts: [remember("Ms. Chen is Maya’s teacher", sourceId)],
            followUp: {
              operation: "schedule",
              followUpId: null,
              at: new Date(Date.parse(input.currentMessage.occurredAt) + 60_000).toISOString(),
              text: "This prohibited reminder must never be sent.",
              sourceIds: [sourceId],
            },
            calendar: calendarDraft("direct", sourceId),
          });
        }
        if (input.currentMessage.text === "Hi Florence") {
          return decision({ bubbles: [{ text: "Family thread is connected.", delayMs: 0 }] });
        }
        if (input.currentMessage.text.includes("remind both of us about pickup")) {
          return decision({
            bubbles: [{ text: "I’ll remind both of you.", delayMs: 0 }],
            facts: [remember("School dismissal is at 2:45", sourceId)],
            followUp: {
              operation: "schedule",
              followUpId: null,
              at: new Date(Date.parse(input.currentMessage.occurredAt) + 60_000).toISOString(),
              text: "Pickup reminder: dismissal is at 2:45.",
              sourceIds: [sourceId],
            },
          });
        }
        if (input.currentMessage.text === "Remember that the school gate code is 2468.") {
          return decision({
            bubbles: [
              { text: "I noted the gate code.", delayMs: 0 },
              { text: "I’ll keep it with the school logistics.", delayMs: 10_000 },
            ],
            facts: [
              remember("The school gate code is 2468", sourceId),
              {
                ...remember("The school office closes at 4", sourceId),
                sourceIds: [sourceId, inboundSourceId("event-independent-school-hours")],
              },
            ],
          });
        }
        if (input.currentMessage.text === "The school office closes at 4.") {
          return decision({ facts: [remember("The school office closes at 4", sourceId)] });
        }
        if (input.currentMessage.text === "Actually, don’t retain that.") {
          const gateCode = input.visibleSources.find(
            (source) =>
              source.kind === "memory" && source.visibility === "shared" && source.text.includes("gate code"),
          );
          return decision({
            policy: noMutationPolicy(),
            facts: [
              {
                operation: "forget",
                factId: gateCode?.recordId ?? "missing",
                statement: null,
                sourceIds: [sourceId],
              },
            ],
          });
        }
        if (input.currentMessage.text.startsWith("Could you add")) {
          const calendar = await reads.readCalendarWindow({
            connectionId: GOOGLE_CONNECTION,
            timeMin: EVENT.startsAt,
            timeMax: EVENT.endsAt,
            limit: 50,
          });
          calendarWasRead = calendarWasRead && calendar.status === "complete";
          expect(calendar.events).toEqual([]);
          return decision({
            calendar: {
              ...calendarDraft("offer", sourceId),
            },
          });
        }
        if (input.currentMessage.text === FAMILY_CALENDAR_REQUEST) {
          expect(input.audience).toBe("group");
          expect(input.currentAdultId).toBe(harness.adultTwoId);
          expect(input.googleConnections).toEqual([
            {
              connectionId: GOOGLE_CONNECTION,
              emailLabel: "Anbarasu Family",
              calendarId: FAMILY_CALENDAR,
              kind: "family",
            },
          ]);
          const calendar = await reads.readCalendarWindow({
            connectionId: GOOGLE_CONNECTION,
            timeMin: EVENT.startsAt,
            timeMax: EVENT.endsAt,
            limit: 50,
          });
          expect(calendar).toEqual({ status: "complete", events: [] });
          calendarWasRead = true;
          return decision({
            calendar: calendarDraft("direct", sourceId, {
              calendarId: FAMILY_CALENDAR,
              audience: "household",
            }),
          });
        }
        if (input.currentMessage.text === COMPOUND_CALENDAR_APPROVAL) {
          expect(input.pendingCalendarOffers).toEqual([expect.objectContaining({ event: EVENT })]);
          return decision({
            bubbles: [{ text: "I’ll remind you that morning.", delayMs: 0 }],
            followUp: {
              operation: "schedule",
              followUpId: null,
              at: CALENDAR_MORNING_REMINDER_AT,
              text: CALENDAR_MORNING_REMINDER,
              sourceIds: [sourceId],
            },
          });
        }
        return decision();
      },
      {
        interpretCalendarApproval: async (input) => ({
          approve: input.currentMessage.text === COMPOUND_CALENDAR_APPROVAL,
        }),
      },
    );
    await harness.onboard({ exerciseMessagesFirst: true });
    await harness.activateGoogle();

    expect(harness.linq.messages.map((message) => message.text)).not.toEqual(
      expect.arrayContaining([expect.stringMatching(/^You’re in, Hari 🎉$/)]),
    );
    const setupTurnsBeforeFamilyPrompt = harness.setupTurns.length;
    await harness.acceptPrivate("private-before-family-setup", "Can you check tomorrow for me?");
    await harness.drain();
    expect(harness.setupTurns).toHaveLength(setupTurnsBeforeFamilyPrompt + 1);
    expect(harness.setupTurns.at(-1)).toMatchObject({
      stage: "family_profile",
      currentMessage: { text: "Can you check tomorrow for me?" },
      nextStep: "finish_family_profile",
    });
    expect(harness.linq.messages.map((message) => message.text)).toContain("https://florence.test/");
    await harness.completeFamilyOnboarding();

    expect(harness.linq.messages.map((message) => message.text)).toEqual(
      expect.arrayContaining([
        "Your side is ready, Hari.",
        "I’ll use your Gmail and calendar to catch school dates, conflicts, and loose ends without sharing your private stuff.",
        "Want me to text Alex at ••••0202 so they can set up their side?",
        expect.stringMatching(/^Hi Alex — I’m Florence\./),
        expect.stringMatching(/^Set up your side here:\nhttps:\/\/florence\.test\/#s=ps1\./),
        expect.stringMatching(/^Hi Hari and Alex — I’m Florence\. This is our family thread\./),
        "I made the Anbarasu Family calendar too. Either of you can ask me to add or change family plans here.",
      ]),
    );

    const signalId = inboundSourceId("event-private-school-packet");
    const sealed = harness.vault.sealPdf({
      documentId: "55555555-5555-4555-8555-555555555555",
      householdId: (await harness.store.listHouseholdIdsForAdult(ADULT_ONE))[0] ?? "missing",
      signalId,
      filename: "school-packet.pdf",
      declaredMimeType: "application/pdf",
      bytes: new TextEncoder().encode(
        "%PDF-1.7\nMuir field trip with Ms. Chen. Permission slip due Tuesday. Friday trip. The bus returns at 3:45.\n%%EOF\n",
      ),
      discardAfter: new Date(harness.now + 120_000).toISOString(),
    });
    const documentMessage = harness.inbound(
      "private",
      "private-school-packet",
      "Review this school packet and draft the exact note to Ms. Chen. Don’t send it. Also don’t retain it or schedule anything.",
      { documents: [{ ...sealed, externalKey: "linq-document-school-packet" }] },
    );
    expect((await harness.florence.acceptInbound(documentMessage))?.disposition).toBe("accepted");
    await eventually(() =>
      harness.linq.reactions.some(
        (reaction) =>
          reaction.targetProviderMessageId === "message-private-school-packet" &&
          reaction.reaction === "emphasize",
      ),
    );
    await eventually(
      () => harness.linq.messages.some((message) => message.text === "I’m looking through this now."),
      8_000,
    );
    const workCueIndex = harness.linq.messages.findIndex(
      (message) => message.text === "I’m looking through this now.",
    );
    expect(workCueIndex).toBeGreaterThanOrEqual(0);
    expect(
      await harness.receiveReaction("document-work-like", `sent-${workCueIndex + 1}`, "like", "private"),
    ).toEqual({
      disposition: "accepted",
      sourceId: inboundSourceId("event-document-work-like"),
    });
    await harness.acceptPrivate(
      "private-school-correction",
      "Correction: Alex is unavailable too. Please keep going.",
    );
    await harness.drain();
    expect((await harness.florence.acceptInbound(documentMessage))?.disposition).toBe("duplicate");
    expect(harness.linq.messages.map((message) => message.text)).not.toContain(
      "Tuesday’s permission-slip deadline is actionable. Ms. Chen is a stable school contact; Friday’s trip and 3:45 return are one-offs. I saved neither.",
    );
    await harness.acceptPrivate(
      "private-school-late-correction",
      "One more correction: Sam can cover the 3:45 pickup. Please keep going.",
    );
    await harness.drain();
    harness.now += 1_000;
    await harness.drain();

    expect(pdfWasRead).toBe(true);
    expect(calendarWasRead).toBe(true);
    expect(gmailWasRead).toBe(true);
    expect(obsoleteResultWasSuppressed).toBe(true);
    expect(harness.googleReads).toBe(4);
    expect(harness.gmailReads).toBe(5);
    expect(harness.googleEffects).toBe(0);
    const storedEmail = await harness.store.recordGmailEvidence({
      householdId: (await harness.store.listHouseholdIdsForAdult(ADULT_ONE))[0] ?? "missing",
      ownerAdultId: ADULT_ONE,
      connectionId: GOOGLE_CONNECTION,
      ...SCHOOL_EMAIL,
      text: "A raw body must not be retained here.",
    });
    expect(storedEmail.visibility).toBe("private");
    expect(storedEmail.ownerAdultId).toBe(ADULT_ONE);
    expect(JSON.stringify(storedEmail.metadata)).not.toContain("A raw body must not be retained here.");
    const documentWorkspace = await harness.florence.workspaceForAdult(ADULT_ONE);
    expect(documentWorkspace.vault?.facts).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ statement: "Ms. Chen is Maya’s teacher" })]),
    );
    const documentReplies = harness.linq.messages.map((message) => message.text);
    expect(documentReplies).toEqual(
      expect.arrayContaining([
        "I’m looking through this now.",
        expect.stringContaining("unsupervised pickup gap"),
        expect.stringMatching(/stable school contact.*one-offs/),
        expect.stringMatching(/draft to Ms\. Chen/i),
      ]),
    );
    expect(documentReplies).not.toContain(
      "Tuesday’s permission-slip deadline is actionable. Ms. Chen is a stable school contact; Friday’s trip and 3:45 return are one-offs. I saved neither.",
    );
    expect(documentReplies.join("\n")).toContain(
      "I didn’t retain anything in the Vault or schedule anything.",
    );
    expect(documentReplies.join("\n")).toContain("Tuesday’s permission-slip deadline still needs attention.");
    expect(documentReplies).not.toContain("This obsolete answer must never be sent.");
    harness.now += 60_000;
    await harness.drain();
    expect(harness.linq.messages.map((message) => message.text)).not.toContain(
      "This prohibited reminder must never be sent.",
    );

    const groupStart = harness.inbound("group", "group-start", "Hi Florence");
    expect((await harness.florence.acceptInbound(groupStart))?.disposition).toBe("accepted");
    await harness.drain();
    expect((await harness.florence.acceptInbound(groupStart))?.disposition).toBe("duplicate");
    const groupReplyIndex = harness.linq.messages.findIndex(
      (message) => message.text === "Family thread is connected.",
    );
    expect(groupReplyIndex).toBeGreaterThanOrEqual(0);
    const groupReplyProviderId = `sent-${groupReplyIndex + 1}`;

    expect(
      (
        await harness.florence.acceptInbound(
          harness.inbound("group", "independent-school-hours", "The school office closes at 4."),
        )
      )?.disposition,
    ).toBe("accepted");
    await harness.drain();
    const temporaryFact = harness.inbound(
      "group",
      "temporary-school-code",
      "Remember that the school gate code is 2468.",
    );
    expect((await harness.florence.acceptInbound(temporaryFact))?.disposition).toBe("accepted");
    await harness.drain();
    expect(
      (await harness.florence.workspaceForAdult(ADULT_ONE)).vault?.facts.some((fact) =>
        fact.statement.includes("gate code"),
      ),
    ).toBe(true);
    expect(
      (
        await harness.florence.acceptInbound(
          harness.inbound("group", "forget-temporary-school-code", "Actually, don’t retain that."),
        )
      )?.disposition,
    ).toBe("accepted");
    await harness.drain();
    expect(
      (await harness.florence.workspaceForAdult(ADULT_ONE)).vault?.facts.some((fact) =>
        fact.statement.includes("gate code"),
      ),
    ).toBe(false);
    expect(
      (await harness.florence.workspaceForAdult(ADULT_ONE)).vault?.facts.some((fact) =>
        fact.statement.includes("office closes at 4"),
      ),
    ).toBe(true);
    expect(harness.linq.messages.map((message) => message.text)).not.toContain(
      "I’ll keep it with the school logistics.",
    );

    expect(await harness.receiveReaction("family-love", groupReplyProviderId, "love")).toEqual({
      disposition: "accepted",
      sourceId: inboundSourceId("event-family-love"),
    });
    await harness.drain();
    expect(await harness.receiveReaction("family-love", groupReplyProviderId, "love")).toEqual({
      disposition: "duplicate",
      sourceId: inboundSourceId("event-family-love"),
    });
    expect(await harness.receiveReaction("not-florence", "message-group-start", "love")).toEqual({
      disposition: "rejected",
      reason: "authority_not_found",
    });
    expect(await harness.receiveReaction("family-like", groupReplyProviderId, "like")).toEqual({
      disposition: "accepted",
      sourceId: inboundSourceId("event-family-like"),
    });
    await harness.drain();

    const pickupMessage = harness.inbound(
      "group",
      "group-pickup",
      "Pickup is at 2:45; please remind both of us about pickup.",
    );
    expect((await harness.florence.acceptInbound(pickupMessage))?.disposition).toBe("accepted");
    await harness.drain();
    harness.now += 60_000;
    await harness.drain();
    const beforeCorrection = await harness.florence.workspaceForAdult(harness.adultTwoId);
    const dismissal = beforeCorrection.vault?.facts.find((fact) => fact.statement.includes("dismissal"));
    expect(dismissal?.visibility).toBe("household");
    await harness.florence.correctFact(
      harness.adultTwoId,
      dismissal?.id ?? "missing",
      "School dismissal is at 3:00",
    );
    expect((await harness.florence.workspaceForAdult(ADULT_ONE)).vault?.facts).toEqual(
      expect.arrayContaining([expect.objectContaining({ statement: "School dismissal is at 3:00" })]),
    );

    await harness.acceptPrivate("calendar-offer", "Could you add the school assembly to my calendar?");
    await harness.drain();
    expect(harness.googleEffects).toBe(0);
    await harness.acceptPrivate("calendar-approval", COMPOUND_CALENDAR_APPROVAL);
    await harness.drain();
    expect(harness.calendarApprovalTurns).toEqual([
      {
        currentMessage: {
          text: COMPOUND_CALENDAR_APPROVAL,
          occurredAt: expect.any(String),
        },
        event: EVENT,
      },
    ]);
    expect(harness.googleEffects).toBe(1);
    const familyCalendarRequest = harness.inbound(
      "group",
      "family-calendar-direct",
      FAMILY_CALENDAR_REQUEST,
      {
        senderIdentitySubjectDigest: ADULT_TWO_IDENTITY,
      },
    );
    expect((await harness.florence.acceptInbound(familyCalendarRequest))?.disposition).toBe("accepted");
    await harness.drain();
    expect(harness.calendarReadCalls).toEqual(
      expect.arrayContaining([
        {
          householdId: FOUNDER_IDS.householdId,
          ownerAdultId: ADULT_ONE,
          connectionId: GOOGLE_CONNECTION,
          calendarId: FAMILY_CALENDAR,
          timeMin: EVENT.startsAt,
          timeMax: EVENT.endsAt,
          limit: 50,
        },
      ]),
    );
    expect(harness.calendarExecutions.filter((action) => action.audience === "household")).toEqual([
      expect.objectContaining({
        householdId: FOUNDER_IDS.householdId,
        connectionId: GOOGLE_CONNECTION,
        ownerAdultId: ADULT_ONE,
        calendarId: FAMILY_CALENDAR,
        audience: "household",
        approvalMessageId: inboundSourceId("event-family-calendar-direct"),
        event: EVENT,
      }),
    ]);

    const workspace = await harness.florence.workspaceForAdult(ADULT_ONE);
    expect(Object.values(workspace.workspace.setup)).not.toContain(false);
    expect(pdfWasRead).toBe(true);
    expect(reactionWasUnderstood).toBe(true);
    expect(calendarWasRead).toBe(true);
    expect(obsoleteResultWasSuppressed).toBe(true);
    expect(harness.googleReads).toBe(6);
    expect(workspace.vault?.facts).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ statement: "A tapback changed family memory" })]),
    );
    expect(harness.googleEffects).toBe(2);
    expect(harness.linq.reactions).toHaveLength(2);
    expect(harness.linq.reactions[1]).toMatchObject({
      targetProviderMessageId: groupReplyProviderId,
      reaction: "laugh",
    });
    expect(harness.linq.messages.map((message) => message.text)).toEqual(
      expect.arrayContaining([
        "I’ll remind both of you.",
        "That made me smile.",
        "Pickup reminder: dismissal is at 2:45.",
        "I’ll remind you that morning.",
        "Added “School assembly” to your calendar.",
        "Added “School assembly” to the family calendar.",
      ]),
    );
    expect(
      harness.linq.messages.filter(
        (message) =>
          message.providerConversationId === GROUP &&
          message.text === "Added “School assembly” to the family calendar.",
      ),
    ).toHaveLength(1);
    expect(harness.linq.messages.find((message) => message.text === "That made me smile.")?.replyTo).toEqual({
      providerMessageId: groupReplyProviderId,
    });
    harness.now = Date.parse(CALENDAR_MORNING_REMINDER_AT);
    await harness.drain();
    expect(harness.linq.messages.map((message) => message.text)).toContain(CALENDAR_MORNING_REMINDER);
  }, 20_000);

  test("keeps private memory and corrections private, shares only group corrections, understands replies, and rejects mismatched group authority", async () => {
    let understoodReply = false;
    let understoodNaturalOptOut = false;
    const reasonedTexts: string[] = [];
    let observedReplyTarget: unknown = null;
    let replyTargetWasInRecentMessages: boolean | null = null;
    const harness = await freshHarness(async (input) => {
      reasonedTexts.push(input.currentMessage.text);
      const sourceId = input.currentMessage.sourceId;
      if (input.currentMessage.text === "Please stop messaging me in this conversation.") {
        understoodNaturalOptOut = true;
        return decision({
          policy: { retain: false, schedule: false, stopMessaging: true },
        });
      }
      if (input.currentMessage.text === "Private doctor note") {
        throw new FlorenceReasonerError("transient", "Keep this reply target retrying");
      }
      if (input.currentMessage.text === "What note was I replying to?") {
        observedReplyTarget = input.currentMessage.replyTo;
        replyTargetWasInRecentMessages = input.recentMessages.some(
          (message) => message.sourceId === input.currentMessage.replyTo?.sourceId,
        );
        understoodReply = true;
        return decision();
      }
      if (input.currentMessage.text === "Pickup is at 2:45") {
        return decision({ facts: [remember("Pickup is at 2:45", sourceId)] });
      }
      const sharedPickup = input.visibleSources.find(
        (source) =>
          source.kind === "memory" && source.visibility === "shared" && source.text.includes("Pickup"),
      );
      if (input.currentMessage.text.includes("keep that private")) {
        return decision({
          facts: [
            {
              operation: "correct",
              factId: sharedPickup?.recordId ?? "missing",
              statement: "Pickup is at 3:00",
              sourceIds: [sourceId],
            },
          ],
        });
      }
      if (input.currentMessage.text === "Forget pickup privately") {
        return decision({
          facts: [
            {
              operation: "forget",
              factId: sharedPickup?.recordId ?? "missing",
              statement: null,
              sourceIds: [sourceId],
            },
          ],
        });
      }
      if (input.currentMessage.text === "Pickup is now 3:00 for everyone") {
        return decision({
          facts: [
            {
              operation: "correct",
              factId: sharedPickup?.recordId ?? "missing",
              statement: "Pickup is at 3:00",
              sourceIds: [sharedPickup?.sourceId ?? "missing", sourceId],
            },
          ],
        });
      }
      return decision();
    });
    await harness.onboard();
    await harness.activateGoogle();
    await harness.completeFamilyOnboarding();
    const privateMessage = await harness.acceptPrivate("private-note", "Private doctor note");
    await harness.drain();
    await harness.acceptPrivate("private-reply", "What note was I replying to?", {
      replyToProviderMessageId: "message-private-note",
    });
    await harness.drain();
    await harness.florence.acceptInbound(harness.inbound("group", "group-start", "Pickup is at 2:45"));
    await harness.drain();

    expect(reasonedTexts).toContain("What note was I replying to?");
    expect(observedReplyTarget).toMatchObject({
      sourceId: inboundSourceId("event-private-note"),
      senderName: "Hari Anbarasu",
      text: "Private doctor note",
    });
    expect(replyTargetWasInRecentMessages).toBe(false);
    expect(understoodReply).toBe(true);
    expect((await harness.florence.workspaceForAdult(harness.adultTwoId)).vault?.facts).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ statement: "Private doctor note" })]),
    );

    await harness.acceptPrivate("private-forget", "Forget pickup privately");
    await harness.drain();
    expect((await harness.florence.workspaceForAdult(harness.adultTwoId)).vault?.facts).toEqual(
      expect.arrayContaining([expect.objectContaining({ statement: "Pickup is at 2:45" })]),
    );

    await harness.acceptPrivate("private-correction", "Actually pickup is at 3:00 — keep that private");
    await harness.drain();
    expect((await harness.florence.workspaceForAdult(ADULT_ONE)).vault?.facts).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ statement: "Pickup is at 2:45", visibility: "household" }),
        expect.objectContaining({ statement: "Pickup is at 3:00", visibility: "private" }),
      ]),
    );
    expect((await harness.florence.workspaceForAdult(harness.adultTwoId)).vault?.facts).toEqual(
      expect.arrayContaining([expect.objectContaining({ statement: "Pickup is at 2:45" })]),
    );
    expect((await harness.florence.workspaceForAdult(harness.adultTwoId)).vault?.facts).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ statement: "Pickup is at 3:00" })]),
    );

    await harness.florence.acceptInbound(
      harness.inbound("group", "shared-correction", "Pickup is now 3:00 for everyone"),
    );
    await harness.drain();
    expect((await harness.florence.workspaceForAdult(harness.adultTwoId)).vault?.facts).toEqual(
      expect.arrayContaining([expect.objectContaining({ statement: "Pickup is at 3:00" })]),
    );

    expect(
      await harness.florence.resolveLinqAuthority({
        providerConversationId: GROUP,
        audience: "group",
        participantIdentityDigests: [ADULT_ONE_IDENTITY, digest("outsider")].sort(),
        senderIdentitySubjectDigest: ADULT_ONE_IDENTITY,
        occurredAt: harness.iso(),
      }),
    ).toBeNull();

    const attemptedLeak = await harness.store.acceptInbound(
      harness.inbound("group", "group-leak", "Tell everyone the private note"),
    );
    await expect(
      harness.store.commitTurn({
        sourceId: attemptedLeak?.sourceId ?? "missing",
        facts: [
          {
            id: randomUUID(),
            subjectPersonId: null,
            kind: "general",
            slot: "leak",
            label: "Leaked note",
            value: { statement: "Private doctor note" },
            visibility: "household",
            ownerAdultId: null,
            sourceIds: [privateMessage.sourceId],
          },
        ],
        handledAt: harness.iso(),
      }),
    ).rejects.toBeInstanceOf(FlorenceStoreUnauthorized);
    await expect(
      harness.store.commitTurn({
        sourceId: attemptedLeak?.sourceId ?? "missing",
        calendarActions: [storedCalendarAction(attemptedLeak?.sourceId ?? "missing")],
        handledAt: harness.iso(),
      }),
    ).rejects.toBeInstanceOf(FlorenceStoreUnauthorized);

    const sentBeforeStop = harness.linq.messages.length;
    expect(
      await harness.acceptPrivate("private-stop", "Please stop messaging me in this conversation."),
    ).toMatchObject({
      disposition: "accepted",
    });
    await harness.drain();
    expect(understoodNaturalOptOut).toBe(true);
    expect(harness.linq.messages).toHaveLength(sentBeforeStop);
    expect(await harness.receiveMessagesText("private-after-stop", "Are you still there?")).toEqual({
      disposition: "acknowledged",
      reason: "channel_stopped",
    });
    expect((await harness.florence.workspaceForAdult(ADULT_ONE)).viewer.displayName).toBe("Hari Anbarasu");
  });

  test("reconciles an ambiguous Calendar write and reports one definitive failure privately", async () => {
    const harness = await freshHarness(
      async (input, reads) => {
        if (
          input.currentMessage.text === "Review the school assembly details." ||
          input.currentMessage.text === PARTNER_INVITATION_APPROVAL
        ) {
          return decision();
        }
        const hasAttachedEvent =
          input.currentMessage.text === "Add the school assembly in this PDF to my calendar.";
        if (hasAttachedEvent) expect(input.currentMessage.pdfs).toHaveLength(1);
        expect(
          await reads.readCalendarWindow({
            connectionId: GOOGLE_CONNECTION,
            timeMin: EVENT.startsAt,
            timeMax: EVENT.endsAt,
            limit: 50,
          }),
        ).toMatchObject({ status: "complete", events: [] });
        return decision({
          bubbles: [{ text: "Done — I added it.", delayMs: 0 }],
          calendar: {
            ...calendarDraft(hasAttachedEvent ? "offer" : "direct", input.currentMessage.sourceId),
          },
        });
      },
      {
        executeCalendar: async () => {
          if (harness.googleEffects === 0) harness.googleEffects += 1;
          if (harness.googleAttempts === 1) throw new Error("connection dropped after Google committed");
          if (harness.googleAttempts === 2) return committedCalendar(harness.iso());
          return failedCalendar(harness.iso());
        },
      },
    );
    await harness.onboard();
    await harness.activateGoogle();
    await harness.completeFamilyOnboarding();
    expect(harness.googleAttempts).toBe(0);
    const attachmentSourceId = inboundSourceId("event-calendar-attachment-request");
    const attachedEvent = harness.vault.sealPdf({
      documentId: "77777777-7777-4777-8777-777777777777",
      householdId: (await harness.store.listHouseholdIdsForAdult(ADULT_ONE))[0] ?? "missing",
      signalId: attachmentSourceId,
      filename: "assembly.pdf",
      declaredMimeType: "application/pdf",
      bytes: new TextEncoder().encode("%PDF-1.7\nSchool assembly details\n%%EOF\n"),
      discardAfter: new Date(harness.now + 120_000).toISOString(),
    });
    expect(
      (
        await harness.florence.acceptInbound(
          harness.inbound(
            "private",
            "calendar-attachment-request",
            "Add the school assembly in this PDF to my calendar.",
            { documents: [{ ...attachedEvent, externalKey: "linq-calendar-attachment" }] },
          ),
        )
      )?.disposition,
    ).toBe("accepted");
    await harness.drain();
    expect(harness.googleAttempts).toBe(0);
    await harness.acceptPrivate("calendar-unrequested", "Review the school assembly details.");
    await harness.drain();
    expect(harness.googleAttempts).toBe(0);
    expect(harness.calendarApprovalTurns).toEqual([
      {
        currentMessage: {
          text: "Review the school assembly details.",
          occurredAt: expect.any(String),
        },
        event: EVENT,
      },
    ]);
    expect(
      harness.linq.messages.some((message) => message.text.startsWith("I can add this to your calendar:")),
    ).toBe(true);
    const inbound = harness.inbound(
      "private",
      "calendar-direct",
      "Add the school assembly to my calendar exactly as written",
    );
    expect((await harness.florence.acceptInbound(inbound))?.disposition).toBe("accepted");
    await harness.drain();
    expect((await harness.florence.acceptInbound(inbound))?.disposition).toBe("duplicate");
    expect(harness.googleAttempts).toBe(1);

    harness.now += 15_000;
    await harness.drain();
    expect(harness.googleAttempts).toBe(2);
    expect(harness.googleEffects).toBe(1);
    expect(harness.linq.messages.map((message) => message.text)).not.toContain("Done — I added it.");
    expect(
      harness.linq.messages.filter((message) => message.text === "Added “School assembly” to your calendar."),
    ).toHaveLength(1);

    await harness.acceptPrivate(
      "calendar-definitive-failure",
      "Add the school assembly to my calendar exactly as written again",
    );
    await harness.drain();
    expect(harness.googleAttempts).toBe(3);
    expect(harness.googleReads).toBe(5);
    const failures = harness.linq.messages.filter((message) =>
      message.text.startsWith("I couldn’t confirm that “School assembly”"),
    );
    expect(failures).toHaveLength(1);
    expect(failures[0]?.replyTo).toEqual({
      providerMessageId: "message-calendar-definitive-failure",
    });
    await harness.drain();
    expect(
      harness.linq.messages.filter((message) =>
        message.text.startsWith("I couldn’t confirm that “School assembly”"),
      ),
    ).toHaveLength(1);

    const cancelledInbound = await harness.store.acceptInbound(
      harness.inbound("private", "calendar-cancel-before-claim", "Add the school assembly to my calendar."),
    );
    const cancelledAction = storedCalendarAction(cancelledInbound?.sourceId ?? "missing");
    const cancellationInput = harness.inbound(
      "private",
      "calendar-never-mind",
      "Actually, don’t add that yet.",
    );
    harness.now += 30_000;
    await harness.store.commitTurn({
      sourceId: cancelledInbound?.sourceId ?? "missing",
      calendarActions: [cancelledAction],
      handledAt: harness.iso(),
    });
    const cancellation = await harness.store.acceptInbound(cancellationInput);
    expect(cancellation?.disposition).toBe("accepted");
    expect(await harness.store.readNextCalendarAction(harness.iso())).toBeNull();
    await harness.store.commitTurn({
      sourceId: cancellation?.sourceId ?? "missing",
      handledAt: harness.iso(),
    });

    const claimInbound = await harness.store.acceptInbound(
      harness.inbound("private", "calendar-claim", "Add one more assembly"),
    );
    const claimAction = storedCalendarAction(claimInbound?.sourceId ?? "missing");
    await expect(
      harness.store.commitTurn({
        sourceId: claimInbound?.sourceId ?? "missing",
        calendarActions: [{ ...claimAction, event: { ...EVENT, startsAt: "2026-08-18T17:00:00" } }],
        handledAt: harness.iso(),
      }),
    ).rejects.toBeInstanceOf(FlorenceStoreConflict);
    await harness.store.commitTurn({
      sourceId: claimInbound?.sourceId ?? "missing",
      calendarActions: [claimAction],
      handledAt: harness.iso(),
    });
    const concurrentClaims = await Promise.all([
      harness.store.readNextCalendarAction(harness.iso()),
      harness.store.readNextCalendarAction(harness.iso()),
    ]);
    expect(concurrentClaims.filter(Boolean)).toHaveLength(1);
    harness.now += 2 * 60_000;
    expect((await harness.store.readNextCalendarAction(harness.iso()))?.id).toBe(claimAction.id);
  });
});

async function expectCanonicalGoogleScopeAcceptance(): Promise<void> {
  const pending = {
    connectionId: GOOGLE_CONNECTION,
    householdId: FOUNDER_IDS.householdId,
    ownerAdultId: ADULT_ONE,
    status: "pending" as const,
    emailLabel: null,
    grantedScopes: [],
    lastError: null,
    createdAt: new Date(NOW).toISOString(),
    updatedAt: new Date(NOW).toISOString(),
  };
  const store = {
    consumePendingState: async () => ({
      connectionId: pending.connectionId,
      householdId: pending.householdId,
      ownerAdultId: pending.ownerAdultId,
      stateDigest: digest("google-state"),
      sessionBindingDigest: digest("browser-session"),
    }),
    activate: async (input: Parameters<GoogleConnectionStore["activate"]>[0]) => ({
      ...pending,
      status: "active" as const,
      emailLabel: input.emailLabel,
      grantedScopes: input.grantedScopes,
    }),
    markPendingFailure: async () => undefined,
  } as unknown as GoogleConnectionStore;
  const google = new GoogleConnection({
    store,
    clientId: "google-client",
    clientSecret: "google-secret",
    redirectUri: "https://florence.test/oauth/google/callback",
    encryptionKey: new Uint8Array(32),
    fetch: async (request) =>
      String(request).endsWith("/token")
        ? Response.json({
            access_token: "access-token",
            refresh_token: "refresh-token",
            scope: [
              "openid",
              "https://www.googleapis.com/auth/userinfo.email",
              "https://www.googleapis.com/auth/userinfo.profile",
              "https://www.googleapis.com/auth/gmail.readonly",
              "https://www.googleapis.com/auth/calendar.app.created",
              "https://www.googleapis.com/auth/calendar.acls",
              "https://www.googleapis.com/auth/calendar.calendarlist",
              "https://www.googleapis.com/auth/calendar.events.owned",
            ].join(" "),
            token_type: "Bearer",
          })
        : Response.json({ sub: "google-subject", email: "parent@example.com", email_verified: true }),
  });
  await expect(
    google.finish({
      state: "google-state",
      code: "authorization-code",
      sessionBindingDigest: digest("browser-session"),
      now: new Date(NOW).toISOString(),
    }),
  ).resolves.toMatchObject({
    status: "active",
    grantedScopes: GOOGLE_SCOPES,
  });
}

class Harness {
  now = NOW;
  googleAttempts = 0;
  googleEffects = 0;
  googleReads = 0;
  gmailReads = 0;
  gmailAttachmentReads = 0;
  partnerAdultId: string | null = null;
  childId: string | null = null;

  constructor(
    readonly store: PostgresFlorenceStore,
    readonly florence: Florence,
    readonly linq: FakeLinq,
    readonly vault: EncryptedImageVault,
    readonly enrollmentCodes: EnrollmentCodes,
    readonly setupTurns: SetupConversationInput[],
    readonly calendarApprovalTurns: CalendarApprovalInput[],
    readonly partnerInvitationApprovalTurns: PartnerInvitationApprovalInput[],
    readonly privateGoogleReviewTurns: PrivateGoogleReviewInput[],
    readonly householdBriefingTurns: HouseholdBriefingInput[],
    readonly familyCalendarProvisioningCalls: FamilyCalendarProvisioningInput[],
    readonly calendarReadCalls: CalendarReadInput[],
    readonly calendarExecutions: CalendarExecutionInput[],
  ) {}

  iso(): string {
    return new Date(this.now).toISOString();
  }

  async onboard(input: { exerciseMessagesFirst?: boolean } = {}): Promise<void> {
    this.linq.authorities.set(PRIVATE_ONE, {
      audience: "private",
      participantIdentityDigests: [ADULT_ONE_IDENTITY],
    });
    expect(await this.store.listHouseholdIdsForAdult(ADULT_ONE)).toEqual([]);
    let setupToken: string;
    let competingSetupToken: string | null = null;
    if (input.exerciseMessagesFirst) {
      const beforeIgnoredMessages = this.linq.messages.length;
      expect(await this.receiveMessagesText("founder-whitespace", "   ")).toEqual({
        disposition: "rejected",
        reason: "authority_not_found",
      });
      expect(await this.receiveMessagesText("founder-stop", "STOP")).toEqual({
        disposition: "acknowledged",
        reason: "opted_out",
      });
      expect(await this.receiveMessagesText("founder-natural-stop", NATURAL_SETUP_OPT_OUT)).toEqual({
        disposition: "acknowledged",
        reason: "onboarding_offered",
      });
      expect(this.setupTurns.at(-1)).toMatchObject({
        stage: "unclaimed",
        currentMessage: { text: NATURAL_SETUP_OPT_OUT },
      });
      expect(this.linq.messages).toHaveLength(beforeIgnoredMessages);
      expect(await this.store.hasPilotHousehold()).toBe(false);

      const arbitraryFirstText = randomUUID();
      expect(await this.receiveMessagesText("founder-first-message", arbitraryFirstText)).toEqual({
        disposition: "acknowledged",
        reason: "onboarding_offered",
      });
      expect(this.setupTurns).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            stage: "unclaimed",
            currentMessage: expect.objectContaining({ text: arbitraryFirstText }),
            nextStep: "signed_link_will_follow",
          }),
        ]),
      );
      const firstSetupLinkCount = this.linq.messages.filter(
        (message) => message.providerConversationId === PRIVATE_ONE && message.text.includes("#s="),
      ).length;
      const setupTurnsBeforeSecondMessage = this.setupTurns.length;
      this.now += 1_000;
      expect(
        await this.receiveMessagesText(
          "founder-second-message",
          `The thing I need help with changed ${randomUUID()}`,
        ),
      ).toEqual({ disposition: "acknowledged", reason: "onboarding_offered" });
      expect(this.setupTurns).toHaveLength(setupTurnsBeforeSecondMessage + 1);
      expect(
        this.linq.messages.filter(
          (message) => message.providerConversationId === PRIVATE_ONE && message.text.includes("#s="),
        ),
      ).toHaveLength(firstSetupLinkCount + 1);
      expect(
        await this.receiveMessagesText("competing-founder-hi", randomUUID(), {
          providerConversationId: PRIVATE_COMPETING_FOUNDER,
          providerHandleId: COMPETING_FOUNDER_HANDLE,
        }),
      ).toEqual({ disposition: "acknowledged", reason: "onboarding_offered" });
      expect(await this.store.hasPilotHousehold()).toBe(false);
      expect(await this.store.listHouseholdIdsForAdult(ADULT_ONE)).toEqual([]);
      expect(await this.store.listHouseholdIdsForAdult(COMPETING_FOUNDER)).toEqual([]);
      const setupBubble = this.linq.messages.findLast(
        (message) => message.providerConversationId === PRIVATE_ONE && message.text.includes("#s="),
      );
      const setupLink = /https:\/\/\S+$/.exec(setupBubble?.text ?? "")?.[0] ?? "";
      expect(setupLink.length).toBeLessThanOrEqual(190);
      expect(setupBubble?.text).toBe(setupLink);
      setupToken = this.setupTokenFor(PRIVATE_ONE);
      competingSetupToken = this.setupTokenFor(PRIVATE_COMPETING_FOUNDER);
    } else {
      setupToken = this.enrollmentCodes.issueFounderSetup({
        providerConversationId: PRIVATE_ONE,
        identitySubjectDigest: ADULT_ONE_IDENTITY,
        occurredAt: this.iso(),
      }).token;
    }

    const app = await buildApp(
      {
        florence: this.florence,
        callerResolver: createSessionCallerResolver({ FLORENCE_SESSION_SECRET: SESSION_SECRET }),
        ready: () => this.store.ready(),
      },
      { serveFrontend: false },
    );
    const setupBody = {
      setupToken,
      profile: {
        firstName: "Hari",
        lastName: "Anbarasu",
        timeZone: "America/Los_Angeles",
        guardianAttested: true as const,
        proactiveUseAccepted: true as const,
      },
    };
    const setup = await app.inject({ method: "POST", url: "/api/v1/session", payload: setupBody });
    expect(setup.statusCode).toBe(200);
    expect(setup.json()).toEqual({ adultId: ADULT_ONE });
    expect(setup.headers["set-cookie"]).toContain("HttpOnly");
    const replay = await app.inject({ method: "POST", url: "/api/v1/session", payload: setupBody });
    expect(replay.statusCode).toBe(200);
    expect(replay.headers["set-cookie"]).toContain("HttpOnly");
    this.now += 60_001;
    const lateReplay = await app.inject({ method: "POST", url: "/api/v1/session", payload: setupBody });
    expect(lateReplay.statusCode).toBe(401);
    if (competingSetupToken) {
      const losingSetup = await app.inject({
        method: "POST",
        url: "/api/v1/session",
        payload: {
          ...setupBody,
          setupToken: competingSetupToken,
          profile: { ...setupBody.profile, firstName: "Other", lastName: "Parent" },
        },
      });
      expect(losingSetup.statusCode).toBe(401);
      expect(losingSetup.json()).toEqual({ error: "invalid_or_expired_setup_link" });
      expect(await this.store.hasPilotHousehold()).toBe(true);
      expect(await this.store.listHouseholdIdsForAdult(COMPETING_FOUNDER)).toEqual([]);
      const sentBeforeLateGreeting = this.linq.messages.length;
      expect(
        await this.receiveMessagesText("competing-founder-after-setup", "Hi", {
          providerConversationId: PRIVATE_COMPETING_FOUNDER,
          providerHandleId: COMPETING_FOUNDER_HANDLE,
        }),
      ).toEqual({ disposition: "rejected", reason: "authority_not_found" });
      expect(this.linq.messages).toHaveLength(sentBeforeLateGreeting);
    }
    await app.close();
  }

  async completeFamilyOnboarding(): Promise<void> {
    const workspace = await this.florence.completeFamilyOnboarding(ADULT_ONE, {
      mode: "two_adult",
      familyLabel: "Anbarasu Family",
      postalCode: "94110",
      partner: { firstName: "Alex", lastName: "Anbarasu", phoneNumber: "+15555550202" },
      children: [
        {
          firstName: "Maya",
          lastName: "Anbarasu",
          school: "Muir Elementary",
          activities: ["Soccer"],
        },
      ],
    });
    expect(workspace.workspace.setup.ownOnboardingComplete).toBe(true);
    const partner = workspace.vault?.members.find(
      (member) => member.kind === "adult" && member.relationship === "Partner",
    );
    const child = workspace.vault?.members.find((member) => member.kind === "child");
    if (!partner || !child) throw new Error("Server did not create the planned family members");
    this.partnerAdultId = partner.id;
    this.childId = child.id;
    expect(workspace.vault?.members).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: partner.id,
          displayName: "Alex Anbarasu",
          messagesIdentity: "not_invited",
        }),
        expect.objectContaining({
          id: child.id,
          displayName: "Maya Anbarasu",
          school: "Muir Elementary",
          activities: ["Soccer"],
        }),
      ]),
    );
    this.now += 1_400;
    await this.drain();
    expect(this.linq.messages.map((message) => message.text)).toEqual(
      expect.arrayContaining([
        "Your side is ready, Hari.",
        "I’ll use your Gmail and calendar to catch school dates, conflicts, and loose ends without sharing your private stuff.",
        "Want me to text Alex at ••••0202 so they can set up their side?",
      ]),
    );
    expect(this.privateGoogleReviewTurns).toHaveLength(1);
    expect(this.privateGoogleReviewTurns[0]).toMatchObject({
      adult: { adultId: ADULT_ONE, firstName: "Hari" },
      googleConnection: { connectionId: GOOGLE_CONNECTION, status: "active", kind: "personal" },
      familyProfile: {
        familyLabel: "Anbarasu Family",
        adultFirstNames: ["Hari", "Alex"],
        children: [{ firstName: "Maya", school: "Muir Elementary", activities: ["Soccer"] }],
        postalCode: "94110",
      },
    });
    expect(this.householdBriefingTurns).toHaveLength(0);
    expect(this.gmailReads).toBe(2);
    expect(this.googleReads).toBe(1);
    expect(this.gmailAttachmentReads).toBe(1);
    expect(this.linq.messages).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          providerConversationId: PRIVATE_ONE,
          text: expect.stringContaining("Hari, I took a first pass through your side"),
        }),
      ]),
    );
    await this.acceptPrivate("partner-invitation-approval", PARTNER_INVITATION_APPROVAL);
    await this.drain();
    expect(this.partnerInvitationApprovalTurns).toEqual([
      {
        currentMessage: { text: PARTNER_INVITATION_APPROVAL },
        partner: {
          adultId: partner.id,
          firstName: "Alex",
          maskedPhoneNumber: "••• ••• 0202",
        },
      },
    ]);
    expect(this.linq.createdChats[0]).toMatchObject({
      input: {
        senderPhoneNumber: FLORENCE_PHONE,
        participantPhoneNumbers: [ADULT_TWO_PHONE],
        initialText: expect.stringMatching(/^Hi Alex — I’m Florence\./),
      },
      result: {
        providerConversationId: PRIVATE_TWO,
        authority: {
          audience: "private",
          ownerPhoneNumber: FLORENCE_PHONE,
          participants: [{ identitySubjectDigest: ADULT_TWO_IDENTITY, phoneNumber: ADULT_TWO_PHONE }],
        },
      },
    });
    const partnerSetupToken = this.setupTokenFor(PRIVATE_TWO);
    expect(
      await this.florence.redeemSetupLink({
        setupToken: partnerSetupToken,
        profile: {
          firstName: "Alex",
          lastName: "Anbarasu",
          timeZone: "America/Los_Angeles",
          guardianAttested: true,
          proactiveUseAccepted: true,
        },
      }),
    ).toMatchObject({ disposition: "accepted", adultId: partner.id });
    await this.activateGoogleConnection({
      adultId: partner.id,
      connectionId: PARTNER_GOOGLE_CONNECTION,
      state: "partner-google-state",
      sessionBinding: "partner-google-session",
    });
    this.now += 1_400;
    await this.drain();
    expect(this.privateGoogleReviewTurns).toHaveLength(2);
    expect(
      this.privateGoogleReviewTurns.map((turn) => ({
        adultId: turn.adult.adultId,
        connectionId: turn.googleConnection.connectionId,
      })),
    ).toEqual([
      { adultId: ADULT_ONE, connectionId: GOOGLE_CONNECTION },
      { adultId: partner.id, connectionId: PARTNER_GOOGLE_CONNECTION },
    ]);
    expect(this.gmailReads).toBe(4);
    expect(this.googleReads).toBe(2);
    expect(this.gmailAttachmentReads).toBe(1);
    expect(this.householdBriefingTurns).toHaveLength(1);
    expect(this.householdBriefingTurns[0]?.candidates).toHaveLength(2);
    const householdBriefingInput = JSON.stringify(this.householdBriefingTurns[0]);
    expect(householdBriefingInput).not.toMatch(
      /hari-private|alex-private|private body|private appointment|private errand|gmail-initial|calendar-source/i,
    );
    const combinedBriefing = this.linq.messages.find(
      (message) =>
        message.providerConversationId === GROUP &&
        message.text.includes("Here’s the household picture I found"),
    );
    expect(combinedBriefing?.text).toContain("permission-slip deadline Tuesday");
    expect(combinedBriefing?.text).toContain("handoff owner for Saturday soccer");
    expect(combinedBriefing?.text).toContain("Did I get that right?");
    expect(combinedBriefing?.text).not.toMatch(/private|@example\.com|gmail-|calendar-/i);
    expect(this.linq.createdChats).toHaveLength(2);
    expect(this.linq.createdChats[1]).toMatchObject({
      input: {
        senderPhoneNumber: FLORENCE_PHONE,
        participantPhoneNumbers: expect.arrayContaining([ADULT_ONE_PHONE, ADULT_TWO_PHONE]),
        initialText: expect.stringMatching(/^Hi Hari and Alex — I’m Florence\./),
      },
      result: {
        providerConversationId: GROUP,
        authority: {
          audience: "group",
          ownerPhoneNumber: FLORENCE_PHONE,
          participantIdentityDigests: this.participants,
        },
      },
    });
    expect(this.familyCalendarProvisioningCalls).toEqual([
      {
        householdId: (await this.store.listHouseholdIdsForAdult(ADULT_ONE))[0],
        founderAdultId: ADULT_ONE,
        founderConnectionId: GOOGLE_CONNECTION,
        partnerAdultId: partner.id,
        partnerConnectionId: PARTNER_GOOGLE_CONNECTION,
        summary: "Anbarasu Family",
        timeZone: "America/Los_Angeles",
      },
    ]);
    const founderWorkspace = await this.florence.workspaceForAdult(ADULT_ONE);
    const partnerWorkspace = await this.florence.workspaceForAdult(partner.id);
    await this.drain();
    expect(this.privateGoogleReviewTurns).toHaveLength(2);
    expect(this.householdBriefingTurns).toHaveLength(1);
    expect(founderWorkspace.workspace.setup).toEqual({
      ownOnboardingComplete: true,
      secondAdultAdded: true,
      partnerInvitation: "connected",
      bothAdultsMessagesConnected: true,
      bothAdultsGoogleConnected: true,
      familyGroupConnected: true,
      familyCalendarConnected: true,
    });
    expect(partnerWorkspace.workspace.setup).toEqual(founderWorkspace.workspace.setup);
    expect(founderWorkspace.workspace.googleConnections).toEqual([
      expect.objectContaining({ connectionId: GOOGLE_CONNECTION, emailLabel: "hari@example.com" }),
    ]);
    expect(partnerWorkspace.workspace.googleConnections).toEqual([
      expect.objectContaining({
        connectionId: PARTNER_GOOGLE_CONNECTION,
        emailLabel: "alex@example.com",
      }),
    ]);
  }

  get adultTwoId(): string {
    if (!this.partnerAdultId) throw new Error("Partner has not been created");
    return this.partnerAdultId;
  }

  get participants(): string[] {
    return [ADULT_ONE_IDENTITY, ADULT_TWO_IDENTITY].sort();
  }

  async activateGoogle(): Promise<void> {
    await this.activateGoogleConnection({
      adultId: ADULT_ONE,
      connectionId: GOOGLE_CONNECTION,
      state: "google-state",
      sessionBinding: "google-session",
    });
    this.now += 1_400;
    await this.drain();
  }

  async activateGoogleConnection(input: {
    adultId: string;
    connectionId: string;
    state: string;
    sessionBinding: string;
  }): Promise<void> {
    const stateDigest = digest(input.state);
    const sessionBindingDigest = digest(input.sessionBinding);
    await this.store.createPending({
      connectionId: input.connectionId,
      householdId: (await this.store.listHouseholdIdsForAdult(input.adultId))[0] ?? "missing",
      ownerAdultId: input.adultId,
      stateDigest,
      sessionBindingDigest,
      stateExpiresAt: new Date(this.now + 60_000).toISOString(),
      now: this.iso(),
    });
    await this.florence.finishGoogle({
      adultId: input.adultId,
      state: input.state,
      code: "google-authorization-code",
      sessionBindingDigest,
    });
  }

  inbound<Audience extends "private" | "group">(
    audience: Audience,
    key: string,
    text: string,
    extra: Record<string, unknown> = {},
  ) {
    const isGroup = audience === "group";
    return {
      providerConversationId: isGroup ? GROUP : PRIVATE_ONE,
      audience,
      participantIdentityDigests: isGroup ? this.participants : [ADULT_ONE_IDENTITY],
      senderIdentitySubjectDigest: ADULT_ONE_IDENTITY,
      providerEventId: `event-${key}`,
      providerMessageId: `message-${key}`,
      text,
      occurredAt: this.iso(),
      ...extra,
    };
  }

  async acceptPrivate(key: string, text: string, extra: Record<string, unknown> = {}) {
    const result = await this.florence.acceptInbound(this.inbound("private", key, text, extra));
    if (!result) throw new Error("Private inbound was rejected");
    return result;
  }

  setupTokenFor(providerConversationId: string): string {
    const setupMessage = this.linq.messages.findLast(
      (message) =>
        message.providerConversationId === providerConversationId &&
        (message.text.includes("#s=") || message.text.includes("#setup=")),
    );
    if (!setupMessage) throw new Error("Florence did not send the founder setup link");
    const setupUrl = /https:\/\/\S+/.exec(setupMessage.text)?.[0];
    if (!setupUrl) throw new Error("Florence did not send a setup URL");
    const fragment = new URL(setupUrl).hash.slice(1);
    const token = new URLSearchParams(fragment).get("s") ?? new URLSearchParams(fragment).get("setup");
    if (!token) throw new Error("Florence did not send the founder setup token");
    return token;
  }

  async receiveMessagesText(
    key: string,
    text: string,
    input: { providerConversationId?: string; providerHandleId?: string } = {},
  ) {
    const providerConversationId = input.providerConversationId ?? PRIVATE_ONE;
    const providerHandleId = input.providerHandleId ?? ADULT_ONE_HANDLE;
    const identitySubjectDigest = linqIdentitySubjectDigest(providerHandleId);
    this.linq.authorities.set(providerConversationId, {
      audience: "private",
      participantIdentityDigests: [identitySubjectDigest],
    });
    const providerEventId = `event-${key}`;
    const timestamp = Math.floor(this.now / 1_000).toString();
    const rawBody = new TextEncoder().encode(
      JSON.stringify({
        api_version: "v3",
        webhook_version: "2026-02-03",
        event_type: "message.received",
        event_id: providerEventId,
        created_at: this.iso(),
        trace_id: `trace-${key}`,
        partner_id: LINQ_PARTNER,
        data: {
          id: `message-${key}`,
          direction: "inbound",
          chat: {
            id: providerConversationId,
            is_group: false,
            owner_handle: {
              id: "messages-florence-owner",
              handle: "+15555550000",
              is_me: true,
            },
          },
          sender_handle: {
            id: providerHandleId,
            handle: "+15555550101",
            is_me: false,
          },
          service: "iMessage",
          parts: [{ type: "text", value: text }],
          reply_to: null,
          sent_at: this.iso(),
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
      now: () => new Date(this.now),
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

  async receiveReaction(
    key: string,
    targetProviderMessageId: string,
    reaction: "love" | "like" | "dislike" | "laugh" | "emphasize" | "question",
    audience: "private" | "group" = "group",
  ) {
    const providerEventId = `event-${key}`;
    const timestamp = Math.floor(this.now / 1_000).toString();
    const rawBody = new TextEncoder().encode(
      JSON.stringify({
        api_version: "v3",
        webhook_version: "2026-02-03",
        event_type: "reaction.added",
        event_id: providerEventId,
        created_at: this.iso(),
        trace_id: `trace-${key}`,
        partner_id: LINQ_PARTNER,
        data: {
          chat_id: audience === "group" ? GROUP : PRIVATE_ONE,
          message_id: targetProviderMessageId,
          part_index: 0,
          reaction_type: reaction,
          custom_emoji: null,
          is_from_me: false,
          from_handle: {
            id: ADULT_ONE_HANDLE,
            handle: "+15555550101",
            is_me: false,
          },
          service: "iMessage",
          reacted_at: this.iso(),
        },
      }),
    );
    const signature = createHmac("sha256", LINQ_SIGNING_KEY)
      .update(`${providerEventId}.${timestamp}.`, "utf8")
      .update(rawBody)
      .digest("base64");
    const ingress = createLinqIngress({
      signingSecret: LINQ_SIGNING_SECRET,
      expectedPartnerId: LINQ_PARTNER,
      linq: this.linq as unknown as LinqClient,
      imageVault: this.vault,
      florence: this.florence,
      now: () => new Date(this.now),
    });
    return ingress.receive({
      rawBody,
      headers: {
        "webhook-id": providerEventId,
        "webhook-timestamp": timestamp,
        "webhook-signature": `v1,${signature}`,
      },
      version: "2026-02-03",
    });
  }

  async drain(): Promise<void> {
    let consecutiveIdleCycles = 0;
    for (let index = 0; index < 40 && consecutiveIdleCycles < 2; index += 1) {
      const worked = await this.florence.runOnce();
      consecutiveIdleCycles = worked ? 0 : consecutiveIdleCycles + 1;
      if (!worked) await new Promise((resolve) => setTimeout(resolve, 0));
    }
  }
}

class FakeLinq {
  readonly authorities = new Map<string, LinqConversationAuthority>();
  readonly createdChats: { input: LinqCreateChat; result: LinqCreatedChat }[] = [];
  readonly messages: LinqSendMessage[] = [];
  readonly reactions: LinqSendReaction[] = [];
  readonly #createdByIdempotencyKey = new Map<string, { input: LinqCreateChat; result: LinqCreatedChat }>();
  readonly #sentByIdempotencyKey = new Map<
    string,
    { input: LinqSendMessage; result: Awaited<ReturnType<LinqClient["sendMessage"]>> }
  >();

  async createChat(input: LinqCreateChat): Promise<LinqCreatedChat> {
    const prior = this.#createdByIdempotencyKey.get(input.idempotencyKey);
    if (prior) {
      expect(input).toEqual(prior.input);
      return prior.result;
    }
    expect(input.senderPhoneNumber).toBe(FLORENCE_PHONE);
    expect(input.initialText.trim()).not.toBe("");
    expect(input.initialText).not.toMatch(/https?:\/\//i);
    const privateInvitation = input.participantPhoneNumbers.length === 1;
    expect([...input.participantPhoneNumbers].sort()).toEqual(
      privateInvitation ? [ADULT_TWO_PHONE] : [ADULT_ONE_PHONE, ADULT_TWO_PHONE].sort(),
    );
    const participantIdentityDigests = privateInvitation
      ? [ADULT_TWO_IDENTITY]
      : [ADULT_ONE_IDENTITY, ADULT_TWO_IDENTITY].sort();
    const participants = privateInvitation
      ? [{ identitySubjectDigest: ADULT_TWO_IDENTITY, phoneNumber: ADULT_TWO_PHONE }]
      : [
          { identitySubjectDigest: ADULT_ONE_IDENTITY, phoneNumber: ADULT_ONE_PHONE },
          { identitySubjectDigest: ADULT_TWO_IDENTITY, phoneNumber: ADULT_TWO_PHONE },
        ].sort((left, right) => left.identitySubjectDigest.localeCompare(right.identitySubjectDigest));
    const authority = {
      audience: privateInvitation ? ("private" as const) : ("group" as const),
      participantIdentityDigests,
      ownerPhoneNumber: FLORENCE_PHONE,
      participants,
    };
    const providerConversationId = privateInvitation ? PRIVATE_TWO : GROUP;
    const result: LinqCreatedChat = {
      providerConversationId,
      authority,
      initialMessage: {
        idempotencyKey: input.idempotencyKey,
        providerMessageId: `created-chat-message-${this.createdChats.length + 1}`,
        occurredAt: new Date(NOW).toISOString(),
      },
    };
    this.authorities.set(providerConversationId, authority);
    this.createdChats.push({ input, result });
    this.#createdByIdempotencyKey.set(input.idempotencyKey, { input, result });
    this.messages.push({
      idempotencyKey: input.idempotencyKey,
      providerConversationId,
      expectedAuthority: authority,
      text: input.initialText,
    });
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
          identitySubjectDigest === ADULT_ONE_IDENTITY
            ? ADULT_ONE_PHONE
            : identitySubjectDigest === ADULT_TWO_IDENTITY
              ? ADULT_TWO_PHONE
              : COMPETING_FOUNDER_PHONE,
      })),
    };
  }

  async setTyping(): Promise<boolean> {
    return true;
  }

  async sendMessage(input: LinqSendMessage) {
    const prior = this.#sentByIdempotencyKey.get(input.idempotencyKey);
    if (prior) {
      expect(input).toEqual(prior.input);
      return prior.result;
    }
    expect(await this.observeChat(input.providerConversationId)).toMatchObject(input.expectedAuthority);
    this.messages.push(input);
    const result = {
      status: "committed" as const,
      providerState: "accepted" as const,
      idempotencyKey: input.idempotencyKey,
      providerReceiptId: `sent-${this.messages.length}`,
      detail: null,
      occurredAt: new Date(NOW).toISOString(),
    };
    this.#sentByIdempotencyKey.set(input.idempotencyKey, { input, result });
    return result;
  }

  async sendReaction(input: LinqSendReaction) {
    expect(await this.observeChat(input.providerConversationId)).toMatchObject(input.expectedAuthority);
    this.reactions.push(input);
    return {
      status: "committed" as const,
      providerState: "reaction_added" as const,
      idempotencyKey: input.idempotencyKey,
      providerReceiptId: `reaction-${this.reactions.length}`,
      detail: null,
      occurredAt: new Date(NOW).toISOString(),
    };
  }
}

async function freshHarness(
  reason: Reason,
  options: {
    executeCalendar?: () => Promise<GoogleCalendarExecutionResult>;
    interpretCalendarApproval?: InterpretCalendarApproval;
    interpretPartnerInvitationApproval?: InterpretPartnerInvitationApproval;
  } = {},
): Promise<Harness> {
  if (!TEST_DATABASE_URL) throw new Error("TEST_DATABASE_URL is required");
  const directory = await mkdtemp(join(tmpdir(), "florence-release-"));
  const schema = `florence_${randomUUID().replaceAll("-", "")}`;
  const setupFile = join(directory, "setup.sql");
  const migrationSql = (await Promise.all(migrationFiles.map((file) => readFile(file, "utf8")))).join("\n");
  await writeFile(setupFile, `CREATE SCHEMA "${schema}"; SET search_path TO "${schema}";\n${migrationSql}`);
  const databaseUrl = withSchema(TEST_DATABASE_URL, schema);
  await migrateDatabase(databaseUrl, setupFile);
  const store = new PostgresFlorenceStore(databaseUrl);
  const linq = new FakeLinq();
  const vault = new EncryptedImageVault({
    rootDirectory: join(directory, "vault"),
    encryptionKey: new Uint8Array(32).fill(7),
  });
  const setupTurns: SetupConversationInput[] = [];
  const calendarApprovalTurns: CalendarApprovalInput[] = [];
  const partnerInvitationApprovalTurns: PartnerInvitationApprovalInput[] = [];
  const privateGoogleReviewTurns: PrivateGoogleReviewInput[] = [];
  const householdBriefingTurns: HouseholdBriefingInput[] = [];
  const familyCalendarProvisioningCalls: FamilyCalendarProvisioningInput[] = [];
  const calendarReadCalls: CalendarReadInput[] = [];
  const calendarExecutions: CalendarExecutionInput[] = [];
  const converseDuringSetup: SetupConversation = async (input) => {
    setupTurns.push(input);
    if (input.currentMessage.text === NATURAL_SETUP_OPT_OUT) {
      return { stopMessaging: true, bubbles: [] };
    }
    return {
      stopMessaging: false,
      bubbles: [
        {
          text:
            input.stage === "unclaimed"
              ? "I can help with that. Let’s finish your private setup, then we can keep going here."
              : input.stage === "connect_google"
                ? "Connect your Google account on the private setup page, then we’ll keep going here."
                : "Add your partner and the useful family basics on the setup page, then we’ll keep going here.",
          delayMs: 0,
        },
      ],
    };
  };
  const interpretCalendarApproval: InterpretCalendarApproval = async (input) => {
    calendarApprovalTurns.push(input);
    return options.interpretCalendarApproval?.(input) ?? { approve: false };
  };
  const interpretPartnerInvitationApproval: InterpretPartnerInvitationApproval = async (input) => {
    partnerInvitationApprovalTurns.push(input);
    return (
      options.interpretPartnerInvitationApproval?.(input) ?? {
        sendInvitation: input.currentMessage.text === PARTNER_INVITATION_APPROVAL,
      }
    );
  };
  const reviewPrivateGoogle: PrivateGoogleReview = async (input, reads) => {
    privateGoogleReviewTurns.push(input);
    const current = Date.parse(input.currentTime);
    const recent = await reads.searchGmail({
      connectionId: input.googleConnection.connectionId,
      query: "(school OR activity OR form) -category:promotions -category:social",
      after: new Date(current - 14 * 24 * 60 * 60_000).toISOString(),
      before: input.currentTime,
      limit: 10,
    });
    const prior = await reads.searchGmail({
      connectionId: input.googleConnection.connectionId,
      query: "(school OR activity OR family) -category:promotions -category:social",
      after: new Date(current - 90 * 24 * 60 * 60_000).toISOString(),
      before: new Date(current - 14 * 24 * 60 * 60_000).toISOString(),
      limit: 10,
    });
    const calendar = await reads.readPersonalCalendarWindow({
      connectionId: input.googleConnection.connectionId,
      timeMin: input.currentTime,
      timeMax: new Date(current + 21 * 24 * 60 * 60_000).toISOString(),
      limit: 50,
    });
    const firstSource = recent[0] ?? prior[0] ?? calendar.events[0];
    if (!firstSource) throw new Error("The initial review fake received no private evidence");
    const attachment = recent[0]?.attachments[0];
    if (attachment) {
      const opened = await reads.readGmailAttachment({
        connectionId: input.googleConnection.connectionId,
        sourceId: recent[0]?.sourceId ?? "missing",
        attachment,
      });
      expect(opened.bytes).toEqual(SCHOOL_PDF_BYTES);
    }
    const owner = input.adult.firstName;
    return {
      bubbles: [
        {
          text: `${owner}, I took a first pass through your side. I found one school item worth keeping on the household radar.`,
          delayMs: 0,
        },
      ],
      findings: [
        {
          privateSummary: `${owner}'s private source detail stays in this thread.`,
          sourceIds: [firstSource.sourceId],
          candidate: {
            category: "deadline",
            summary:
              owner === "Hari"
                ? "Maya’s school packet has a permission-slip deadline Tuesday."
                : "The family calendar needs a handoff owner for Saturday soccer.",
            urgency: "soon",
            dueAt: owner === "Hari" ? "2026-08-18T18:00:00.000Z" : null,
            needsAnswer: true,
          },
        },
      ],
    };
  };
  const synthesizeHouseholdBriefing: HouseholdBriefing = async (input) => {
    householdBriefingTurns.push(input);
    return {
      selectedCandidateIds: input.candidates.slice(0, 3).map((candidate) => candidate.candidateId),
      bubbles: [
        {
          text: `Here’s the household picture I found:\n${input.candidates
            .slice(0, 3)
            .map((candidate) => `– ${candidate.summary}`)
            .join("\n")}\n\nDid I get that right? What else can I take off your plate?`,
          delayMs: 0,
        },
      ],
    };
  };
  const reasoner = {
    decide: reason,
    converseDuringSetup,
    interpretCalendarApproval,
    interpretPartnerInvitationApproval,
    reviewPrivateGoogle,
    synthesizeHouseholdBriefing,
  } as unknown as FlorenceReasoner;
  const enrollmentCodes = new EnrollmentCodes(ENROLLMENT_SECRET);
  let harness: Harness;
  const google = {
    status: (input: { householdId: string; ownerAdultId: string }) => store.listActive(input),
    finish: async (input: { state: string; sessionBindingDigest: string; now: string }) => {
      const stateDigest = digest(input.state);
      const pending = await store.consumePendingState({
        stateDigest,
        sessionBindingDigest: input.sessionBindingDigest,
        now: input.now,
      });
      if (!pending) throw new Error("The fake Google state was not pending");
      const isFounder = pending.ownerAdultId === ADULT_ONE;
      return store.activate({
        connectionId: pending.connectionId,
        stateDigest,
        googleSubjectDigest: digest(isFounder ? "google-subject-founder" : "google-subject-partner"),
        emailLabel: isFounder ? "hari@example.com" : "alex@example.com",
        grantedScopes: GOOGLE_SCOPES,
        refreshTokenEnvelope: isFounder
          ? "encrypted-founder-refresh-token"
          : "encrypted-partner-refresh-token",
        now: input.now,
      });
    },
    provisionFamilyCalendar: async (input: {
      householdId: string;
      founderAdultId: string;
      founderConnectionId: string;
      partnerAdultId: string;
      partnerConnectionId: string;
      summary: string;
      timeZone: string;
      calendarId?: string;
    }): Promise<GoogleFamilyCalendarProvisioningResult> => {
      familyCalendarProvisioningCalls.push(input);
      expect(input.founderAdultId).toBe(ADULT_ONE);
      expect(input.founderConnectionId).toBe(GOOGLE_CONNECTION);
      expect(input.partnerAdultId).toBe(harness.adultTwoId);
      expect(input.partnerConnectionId).toBe(PARTNER_GOOGLE_CONNECTION);
      expect(input.summary).toBe("Anbarasu Family");
      expect(input.timeZone).toBe("America/Los_Angeles");
      expect(input.calendarId).toBeUndefined();
      return {
        status: "committed",
        calendarId: FAMILY_CALENDAR,
        summary: input.summary,
        timeZone: input.timeZone,
        founderConnectionId: input.founderConnectionId,
        founderAccessRole: "owner",
        partnerConnectionId: input.partnerConnectionId,
        partnerEmailLabel: "alex@example.com",
        partnerAccessRole: "owner",
        partnerCalendarListSelected: true,
        providerReceiptId: FAMILY_CALENDAR,
        detail: JSON.stringify({ calendarId: FAMILY_CALENDAR, partnerAccessRole: "owner" }),
        occurredAt: harness.iso(),
      };
    },
    readCalendarWindow: async (input: CalendarReadInput) => {
      calendarReadCalls.push(input);
      harness.googleReads += 1;
      const credential = await store.readActiveGoogleCredential(input);
      return credential
        ? {
            status: "complete" as const,
            events:
              input.timeMin === TRIP_WINDOW.timeMin
                ? [
                    {
                      providerEventId: "calendar-pickup-conflict",
                      providerRevision: "pickup-revision-1",
                      providerUpdatedAt: harness.iso(),
                      ...PICKUP_CONFLICT,
                    },
                  ]
                : [],
            cursor: {
              kind: "calendar_updated_min_v1" as const,
              calendarId: input.calendarId ?? "primary",
              updatedMin: harness.iso(),
              windowTimeMin: input.timeMin,
              windowTimeMax: input.timeMax,
              overlapMs: 300_000 as const,
            },
          }
        : { status: "unavailable" as const, events: [], cursor: null };
    },
    readInitialCalendarReview: async (input: {
      householdId: string;
      ownerAdultId: string;
      connectionId: string;
      currentTime: string;
      limit: number;
    }) => {
      harness.googleReads += 1;
      expect(input.limit).toBe(50);
      const credential = await store.readActiveGoogleCredential(input);
      if (!credential) return { status: "unavailable" as const, events: [], cursor: null };
      const startsAt = new Date(Date.parse(input.currentTime) + 2 * 24 * 60 * 60_000).toISOString();
      const endsAt = new Date(Date.parse(startsAt) + 60 * 60_000).toISOString();
      return {
        status: "complete" as const,
        events: [
          {
            providerEventId: `private-event-${input.ownerAdultId}`,
            providerRevision: `private-revision-${input.ownerAdultId}`,
            providerUpdatedAt: harness.iso(),
            title: input.ownerAdultId === ADULT_ONE ? "Hari private appointment" : "Alex private errand",
            startsAt,
            endsAt,
            allDay: false,
          },
        ],
        cursor: {
          kind: "calendar_updated_min_v1" as const,
          calendarId: "primary",
          updatedMin: harness.iso(),
          windowTimeMin: input.currentTime,
          windowTimeMax: new Date(Date.parse(input.currentTime) + 21 * 24 * 60 * 60_000).toISOString(),
          overlapMs: 300_000 as const,
        },
      };
    },
    captureGmailCursor: async (input: {
      householdId: string;
      ownerAdultId: string;
      connectionId: string;
    }) => {
      const credential = await store.readActiveGoogleCredential(input);
      if (!credential) throw new Error("The fake Gmail cursor owner is unavailable");
      return {
        kind: "gmail_history_v1" as const,
        historyId: input.ownerAdultId === ADULT_ONE ? "9001" : "9002",
        capturedAt: harness.iso(),
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
      harness.gmailReads += 1;
      const credential = await store.readActiveGoogleCredential(input);
      if (!credential) return { status: "complete" as const, messages: [] };
      if (!input.after || !input.before) {
        expect(input.query).toBe('newer_than:30d ("Muir" OR "field trip")');
        expect(input.limit).toBe(5);
        return { status: "complete" as const, messages: [SCHOOL_EMAIL] };
      }
      const span = Date.parse(input.before) - Date.parse(input.after);
      expect([14, 76].map((days) => days * 24 * 60 * 60_000)).toContain(span);
      const recent = span <= 15 * 24 * 60 * 60_000;
      const founderRecent = input.ownerAdultId === ADULT_ONE && recent;
      const messageId = founderRecent
        ? SCHOOL_PDF_ATTACHMENT.messageId
        : `gmail-${input.ownerAdultId}-${recent ? "recent" : "prior"}`;
      const threadId = founderRecent
        ? SCHOOL_PDF_ATTACHMENT.threadId
        : `thread-${input.ownerAdultId}-${recent ? "recent" : "prior"}`;
      const historyId = founderRecent
        ? SCHOOL_PDF_ATTACHMENT.historyId
        : `history-${input.ownerAdultId}-${recent ? "recent" : "prior"}`;
      return {
        status: "complete" as const,
        messages: [
          {
            messageId,
            threadId,
            historyId,
            from: input.ownerAdultId === ADULT_ONE ? "hari-private@example.com" : "alex-private@example.com",
            subject: input.ownerAdultId === ADULT_ONE ? "Hari private school detail" : "Alex private detail",
            sentAt: new Date((Date.parse(input.after) + Date.parse(input.before)) / 2).toISOString(),
            text:
              input.ownerAdultId === ADULT_ONE
                ? "Hari private body: the permission slip is due Tuesday."
                : "Alex private body: Saturday has a personal conflict.",
            textStatus: "complete" as const,
            attachments: founderRecent ? [SCHOOL_PDF_ATTACHMENT] : [],
            attachmentsStatus: "complete" as const,
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
      const credential = await store.readActiveGoogleCredential(input);
      if (!credential || input.ownerAdultId !== ADULT_ONE) {
        throw new Error("The fake Gmail attachment owner is unavailable");
      }
      expect(input.attachment).toEqual(SCHOOL_PDF_ATTACHMENT);
      harness.gmailAttachmentReads += 1;
      return { ...SCHOOL_PDF_ATTACHMENT, bytes: SCHOOL_PDF_BYTES };
    },
    executeCalendar: async (input: CalendarExecutionInput) => {
      calendarExecutions.push(input);
      harness.googleAttempts += 1;
      if (options.executeCalendar) return options.executeCalendar();
      harness.googleEffects += 1;
      return committedCalendar(harness.iso(), `google-event-${input.actionId}`);
    },
  } as unknown as GoogleConnection;
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
    now: () => new Date(harness.now),
  });
  harness = new Harness(
    store,
    florence,
    linq,
    vault,
    enrollmentCodes,
    setupTurns,
    calendarApprovalTurns,
    partnerInvitationApprovalTurns,
    privateGoogleReviewTurns,
    householdBriefingTurns,
    familyCalendarProvisioningCalls,
    calendarReadCalls,
    calendarExecutions,
  );
  onTestFinished(async () => {
    florence.stop();
    await store.close();
    await writeFile(setupFile, `DROP SCHEMA IF EXISTS "${schema}" CASCADE;`);
    await migrateDatabase(TEST_DATABASE_URL, setupFile);
    await rm(directory, { recursive: true, force: true });
  });
  return harness;
}

function decision(
  input: {
    policy?: FlorenceDecision["policy"];
    reaction?: FlorenceDecision["conversation"]["reaction"];
    reply?: boolean;
    bubbles?: FlorenceDecision["conversation"]["bubbles"];
    facts?: FlorenceDecision["facts"];
    followUp?: FlorenceDecision["followUp"];
    calendar?: FlorenceDecision["calendar"];
  } = {},
): FlorenceDecision {
  return {
    policy: input.policy ?? { retain: true, schedule: true, stopMessaging: false },
    conversation: {
      replyToCurrentMessage: input.reply ?? false,
      reaction: input.reaction ?? null,
      bubbles: input.bubbles ?? [],
    },
    facts: input.facts ?? [],
    followUp: input.followUp ?? null,
    calendar: input.calendar ?? null,
  };
}

function noMutationPolicy(): FlorenceDecision["policy"] {
  return { retain: false, schedule: false, stopMessaging: false };
}

async function waitForAbort(signal?: AbortSignal): Promise<never> {
  if (!signal) throw new Error("The reasoner was not given an abort signal");
  if (signal.aborted) throw signal.reason;
  return new Promise((_, reject) => {
    signal.addEventListener("abort", () => reject(signal.reason), { once: true });
  });
}

async function eventually(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`Condition was not met within ${timeoutMs}ms`);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

function committedCalendar(occurredAt: string, providerReceiptId = "google-event-school-assembly") {
  return {
    status: "committed" as const,
    providerReceiptId,
    detail: JSON.stringify({
      provider: "google-calendar",
      eventId: providerReceiptId,
      etag: '"assembly-etag"',
      digest: digest("google-calendar-proof"),
    }),
    occurredAt,
  };
}

function failedCalendar(occurredAt: string): GoogleCalendarExecutionResult {
  return {
    status: "failed",
    providerReceiptId: null,
    detail: "Google Calendar rejected the approved write",
    occurredAt,
  };
}

function remember(statement: string, sourceId: string): FlorenceDecision["facts"][number] {
  return { operation: "remember", factId: null, statement, sourceIds: [sourceId] };
}

function calendarDraft(
  mode: "offer" | "direct",
  sourceId: string,
  target: { calendarId: string; audience: "private" | "household" } = {
    calendarId: "primary",
    audience: "private",
  },
) {
  return {
    mode,
    proposalId: null,
    connectionId: GOOGLE_CONNECTION,
    calendarId: target.calendarId,
    audience: target.audience,
    event: EVENT,
    sourceIds: [sourceId],
  };
}

function storedCalendarAction(sourceId: string) {
  return {
    id: randomUUID(),
    actionId: randomUUID(),
    connectionId: GOOGLE_CONNECTION,
    calendarId: "primary",
    audience: "private" as const,
    ownerAdultId: ADULT_ONE,
    basisSourceId: sourceId,
    approvalMessageId: sourceId,
    approvalDigest: digest(`approval:${sourceId}`),
    proposalDigest: digest(`proposal:${sourceId}`),
    event: EVENT,
  };
}

function withSchema(connectionString: string, schema: string): string {
  const url = new URL(connectionString);
  url.searchParams.set("options", `-csearch_path=${schema}`);
  return url.toString();
}

function digest(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function inboundSourceId(providerEventId: string): string {
  const value = digest(`linq-v3\0signal\0${providerEventId}`);
  return `${value.slice(0, 8)}-${value.slice(8, 12)}-5${value.slice(13, 16)}-${((Number.parseInt(value[16] ?? "0", 16) & 3) | 8).toString(16)}${value.slice(17, 20)}-${value.slice(20, 32)}`;
}
