import { createHash, createHmac, randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { EncryptedImageVault } from "@florence/artifacts";
import {
  baselineFile,
  FlorenceStoreConflict,
  FlorenceStoreUnauthorized,
  migrateDatabase,
  PostgresFlorenceStore,
} from "@florence/database";
import type { GoogleCalendarExecutionResult, GoogleConnection } from "@florence/google";
import {
  type LinqClient,
  type LinqConversationAuthority,
  type LinqSendMessage,
  type LinqSendReaction,
  linqIdentitySubjectDigest,
} from "@florence/linq";
import { describe, expect, onTestFinished, test } from "vitest";
import { EnrollmentCodes } from "./enrollment.js";
import { Florence } from "./florence.js";
import { createLinqIngress } from "./linq-ingress.js";
import { type FlorenceDecision, type FlorenceReasoner, FlorenceReasonerError } from "./reasoner.js";

const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL;
const NOW = Date.parse("2026-08-16T18:00:00.000Z");
const ADULT_ONE = "11111111-1111-4111-8111-111111111111";
const ADULT_TWO = "22222222-2222-4222-8222-222222222222";
const GOOGLE_CONNECTION = "44444444-4444-4444-8444-444444444444";
const ADULT_ONE_HANDLE = "messages-adult-one";
const ADULT_TWO_HANDLE = "messages-adult-two";
const ADULT_ONE_IDENTITY = linqIdentitySubjectDigest(ADULT_ONE_HANDLE);
const ADULT_TWO_IDENTITY = linqIdentitySubjectDigest(ADULT_TWO_HANDLE);
const PARTICIPANTS = [ADULT_ONE_IDENTITY, ADULT_TWO_IDENTITY].sort();
const GROUP = "linq-group-family";
const PRIVATE_ONE = "linq-private-one";
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

type Reason = FlorenceReasoner["decide"];

const release = TEST_DATABASE_URL ? describe : describe.skip;

release("Florence release journeys", () => {
  test("runs the real two-adult household journey from an interruptible document turn through Calendar proof", async () => {
    let pdfWasRead = false;
    let reactionWasUnderstood = false;
    let calendarWasRead = false;
    let obsoleteResultWasSuppressed = false;
    const harness = await freshHarness(async (input, reads, signal) => {
      const sourceId = input.currentMessage.sourceId;
      if (input.currentMessage.moveKind === "reaction") {
        if (input.currentMessage.replyTo?.text === "I’m looking through this now.") {
          expect(input.audience).toBe("private");
          return decision();
        }
        reactionWasUnderstood = true;
        expect(input.currentMessage.replyTo).toMatchObject({
          senderName: "Florence",
          text: "Family thread is connected.",
        });
        if (input.currentMessage.text === "Reacted like") {
          return decision({
            reaction: "laugh",
            reply: true,
            bubbles: [{ text: "That made me smile.", delayMs: 0 }],
          });
        }
        expect(input.currentMessage.text).toBe("Reacted love");
        return decision({
          bubbles: [{ text: "A reaction should only be conversational.", delayMs: 0 }],
          facts: [remember("A tapback changed family memory", sourceId)],
          followUp: {
            operation: "cancel",
            followUpId: input.pendingFollowUps[0]?.followUpId ?? "missing",
            at: null,
            text: null,
            sourceIds: [sourceId],
          },
          calendar: calendarDraft("direct", sourceId),
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
        expect(input.boundaries).toEqual({
          retain: false,
          schedule: false,
          consequentialAction: false,
        });
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
        obsoleteResultWasSuppressed = true;
        if (input.currentMessage.text.includes("Sam can cover")) {
          return decision({
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
      if (input.currentMessage.text === "Yes, add it") {
        const offer = input.pendingCalendarOffers[0];
        expect(offer?.event).toEqual(EVENT);
        return decision({
          calendar: {
            mode: "approve",
            proposalId: offer?.proposalId ?? "missing",
            connectionId: null,
            event: null,
            sourceIds: [sourceId],
          },
        });
      }
      return decision();
    });
    await harness.onboard();
    await harness.activateGoogle();

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
    expect(obsoleteResultWasSuppressed).toBe(true);
    expect(harness.googleReads).toBe(2);
    expect(harness.googleEffects).toBe(0);
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
      "I didn’t retain anything in the Vault, send anything externally, schedule a follow-up, or add anything to your calendar.",
    );
    expect(documentReplies.join("\n")).toContain("Tuesday’s permission-slip deadline still needs attention.");
    expect(documentReplies).not.toContain("This obsolete answer must never be sent.");
    harness.now += 60_000;
    await harness.drain();
    expect(harness.linq.messages.map((message) => message.text)).not.toContain(
      "This prohibited reminder must never be sent.",
    );

    const groupStart = harness.inbound("group", "group-start", "Hi Florence");
    expect((await harness.florence.bootstrapMessagesGroup(groupStart))?.disposition).toBe("accepted");
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
    const beforeCorrection = await harness.florence.workspaceForAdult(ADULT_TWO);
    const dismissal = beforeCorrection.vault?.facts.find((fact) => fact.statement.includes("dismissal"));
    expect(dismissal?.visibility).toBe("household");
    await harness.florence.correctFact(ADULT_TWO, dismissal?.id ?? "missing", "School dismissal is at 3:00");
    expect((await harness.florence.workspaceForAdult(ADULT_ONE)).vault?.facts).toEqual(
      expect.arrayContaining([expect.objectContaining({ statement: "School dismissal is at 3:00" })]),
    );

    await harness.acceptPrivate("calendar-offer", "Could you add the school assembly to my calendar?");
    await harness.drain();
    await harness.acceptPrivate("calendar-approval", "Yes, add it");
    await harness.drain();

    const workspace = await harness.florence.workspaceForAdult(ADULT_ONE);
    expect(Object.values(workspace.workspace.setup)).not.toContain(false);
    expect(pdfWasRead).toBe(true);
    expect(reactionWasUnderstood).toBe(true);
    expect(calendarWasRead).toBe(true);
    expect(obsoleteResultWasSuppressed).toBe(true);
    expect(harness.googleReads).toBe(3);
    expect(workspace.vault?.facts).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ statement: "A tapback changed family memory" })]),
    );
    expect(harness.googleEffects).toBe(1);
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
        "Added “School assembly” to your calendar.",
      ]),
    );
    expect(harness.linq.messages.find((message) => message.text === "That made me smile.")?.replyTo).toEqual({
      providerMessageId: groupReplyProviderId,
    });
  }, 20_000);

  test("keeps private memory and corrections private, shares only group corrections, understands replies, and rejects mismatched group authority", async () => {
    let understoodReply = false;
    const harness = await freshHarness(async (input) => {
      const sourceId = input.currentMessage.sourceId;
      if (input.currentMessage.text === "Private doctor note") {
        throw new FlorenceReasonerError("transient", "Keep this reply target retrying");
      }
      if (input.currentMessage.text === "What note was I replying to?") {
        expect(input.currentMessage.replyTo).toMatchObject({
          sourceId: inboundSourceId("event-private-note"),
          senderName: "Hari",
          text: "Private doctor note",
        });
        expect(
          input.recentMessages.some((message) => message.sourceId === input.currentMessage.replyTo?.sourceId),
        ).toBe(false);
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
    const privateMessage = await harness.acceptPrivate("private-note", "Private doctor note");
    await harness.drain();
    await harness.acceptPrivate("private-reply", "What note was I replying to?", {
      replyToProviderMessageId: "message-private-note",
    });
    await harness.drain();
    await harness.florence.bootstrapMessagesGroup(
      harness.inbound("group", "group-start", "Pickup is at 2:45"),
    );
    await harness.drain();

    expect(understoodReply).toBe(true);
    expect((await harness.florence.workspaceForAdult(ADULT_TWO)).vault?.facts).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ statement: "Private doctor note" })]),
    );

    await harness.acceptPrivate("private-forget", "Forget pickup privately");
    await harness.drain();
    expect((await harness.florence.workspaceForAdult(ADULT_TWO)).vault?.facts).toEqual(
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
    expect((await harness.florence.workspaceForAdult(ADULT_TWO)).vault?.facts).toEqual(
      expect.arrayContaining([expect.objectContaining({ statement: "Pickup is at 2:45" })]),
    );
    expect((await harness.florence.workspaceForAdult(ADULT_TWO)).vault?.facts).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ statement: "Pickup is at 3:00" })]),
    );

    await harness.florence.acceptInbound(
      harness.inbound("group", "shared-correction", "Pickup is now 3:00 for everyone"),
    );
    await harness.drain();
    expect((await harness.florence.workspaceForAdult(ADULT_TWO)).vault?.facts).toEqual(
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
  });

  test("reconciles an ambiguous Calendar write and reports one definitive failure privately", async () => {
    const harness = await freshHarness(
      async (input, reads) => {
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
            ...calendarDraft("direct", input.currentMessage.sourceId),
          },
        });
      },
      async () => {
        if (harness.googleEffects === 0) harness.googleEffects += 1;
        if (harness.googleAttempts === 1) throw new Error("connection dropped after Google committed");
        if (harness.googleAttempts === 2) return committedCalendar(harness.iso());
        return failedCalendar(harness.iso());
      },
    );
    await harness.onboard();
    await harness.activateGoogle();
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
    expect(harness.googleReads).toBe(4);
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

class Harness {
  now = NOW;
  googleAttempts = 0;
  googleEffects = 0;
  googleReads = 0;

  constructor(
    readonly store: PostgresFlorenceStore,
    readonly florence: Florence,
    readonly linq: FakeLinq,
    readonly vault: EncryptedImageVault,
  ) {}

  iso(): string {
    return new Date(this.now).toISOString();
  }

  async onboard(): Promise<void> {
    await this.florence.putHousehold(ADULT_ONE, {
      name: "Barasu Family",
      timeZone: "America/Los_Angeles",
      foundingAdultDisplayName: "Hari",
    });
    await this.florence.putMember(ADULT_ONE, ADULT_TWO, {
      kind: "adult",
      role: "steward",
      displayName: "Alex",
      relationship: "Parent",
    });
    await this.florence.putMember(ADULT_ONE, "33333333-3333-4333-8333-333333333333", {
      kind: "child",
      role: "dependent",
      displayName: "Maya",
      relationship: "Child",
      school: "Muir Elementary",
    });
    const codes = new EnrollmentCodes("release-journey-secret-is-at-least-thirty-two-bytes");
    for (const [adultId, identity, conversation] of [
      [ADULT_ONE, ADULT_ONE_IDENTITY, PRIVATE_ONE],
      [ADULT_TWO, ADULT_TWO_IDENTITY, "linq-private-two"],
    ] as const) {
      const invite = await this.florence.issueMessagesInvite(ADULT_ONE, adultId);
      await this.florence.redeemMessagesEnrollment({
        challengeDigest: codes.digestCandidate(invite.invite.code) ?? "missing",
        identitySubjectDigest: identity,
        consentVersion: "pilot-v1",
        consentedAt: this.iso(),
        providerConversationId: conversation,
        occurredAt: this.iso(),
      });
      this.linq.authorities.set(conversation, {
        audience: "private",
        participantIdentityDigests: [identity],
      });
    }
    this.linq.authorities.set(GROUP, { audience: "group", participantIdentityDigests: PARTICIPANTS });
  }

  async activateGoogle(): Promise<void> {
    const stateDigest = digest("google-state");
    const sessionBindingDigest = digest("google-session");
    await this.store.createPending({
      connectionId: GOOGLE_CONNECTION,
      householdId: (await this.store.listHouseholdIdsForAdult(ADULT_ONE))[0] ?? "missing",
      ownerAdultId: ADULT_ONE,
      stateDigest,
      sessionBindingDigest,
      stateExpiresAt: new Date(this.now + 60_000).toISOString(),
      now: this.iso(),
    });
    await this.store.consumePendingState({ stateDigest, sessionBindingDigest, now: this.iso() });
    await this.store.activate({
      connectionId: GOOGLE_CONNECTION,
      stateDigest,
      googleSubjectDigest: digest("google-subject"),
      emailLabel: "hari@example.com",
      grantedScopes: [
        "openid",
        "email",
        "https://www.googleapis.com/auth/gmail.readonly",
        "https://www.googleapis.com/auth/calendar.events.owned",
      ],
      refreshTokenEnvelope: "encrypted-refresh-token",
      now: this.iso(),
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
      participantIdentityDigests: isGroup ? PARTICIPANTS : [ADULT_ONE_IDENTITY],
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
      enrollmentCodes: new EnrollmentCodes("release-journey-secret-is-at-least-thirty-two-bytes"),
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
    for (let index = 0; index < 20 && (await this.florence.runOnce()); index += 1);
  }
}

class FakeLinq {
  readonly authorities = new Map<string, LinqConversationAuthority>();
  readonly messages: LinqSendMessage[] = [];
  readonly reactions: LinqSendReaction[] = [];

  async observeChat(providerConversationId: string) {
    const authority = this.authorities.get(providerConversationId);
    if (!authority) throw new Error(`Unknown fake Linq conversation ${providerConversationId}`);
    return authority;
  }

  async sendMessage(input: LinqSendMessage) {
    expect(await this.observeChat(input.providerConversationId)).toEqual(input.expectedAuthority);
    this.messages.push(input);
    return {
      status: "committed" as const,
      idempotencyKey: input.idempotencyKey,
      providerReceiptId: `sent-${this.messages.length}`,
      detail: null,
      occurredAt: new Date(NOW).toISOString(),
    };
  }

  async sendReaction(input: LinqSendReaction) {
    expect(await this.observeChat(input.providerConversationId)).toEqual(input.expectedAuthority);
    this.reactions.push(input);
    return {
      status: "committed" as const,
      idempotencyKey: input.idempotencyKey,
      providerReceiptId: `reaction-${this.reactions.length}`,
      detail: null,
      occurredAt: new Date(NOW).toISOString(),
    };
  }
}

async function freshHarness(
  reason: Reason,
  executeCalendar?: () => Promise<GoogleCalendarExecutionResult>,
): Promise<Harness> {
  if (!TEST_DATABASE_URL) throw new Error("TEST_DATABASE_URL is required");
  const directory = await mkdtemp(join(tmpdir(), "florence-release-"));
  const schema = `florence_${randomUUID().replaceAll("-", "")}`;
  const setupFile = join(directory, "setup.sql");
  await writeFile(
    setupFile,
    `CREATE SCHEMA "${schema}"; SET search_path TO "${schema}";\n${await readFile(baselineFile, "utf8")}`,
  );
  const databaseUrl = withSchema(TEST_DATABASE_URL, schema);
  await migrateDatabase(databaseUrl, setupFile);
  const store = new PostgresFlorenceStore(databaseUrl);
  const linq = new FakeLinq();
  const vault = new EncryptedImageVault({
    rootDirectory: join(directory, "vault"),
    encryptionKey: new Uint8Array(32).fill(7),
  });
  const reasoner = { decide: reason } as unknown as FlorenceReasoner;
  let harness: Harness;
  const google = {
    status: (input: { householdId: string; ownerAdultId: string }) => store.listActive(input),
    readCalendarWindow: async (input: {
      householdId: string;
      ownerAdultId: string;
      connectionId: string;
      timeMin: string;
      timeMax: string;
    }) => {
      harness.googleReads += 1;
      const credential = await store.readActiveGoogleCredential(input);
      return credential
        ? {
            status: "complete" as const,
            events: input.timeMin === TRIP_WINDOW.timeMin ? [PICKUP_CONFLICT] : [],
          }
        : { status: "unavailable" as const, events: [] };
    },
    executeCalendar: async () => {
      harness.googleAttempts += 1;
      if (executeCalendar) return executeCalendar();
      harness.googleEffects += 1;
      return committedCalendar(harness.iso());
    },
  } as unknown as GoogleConnection;
  const florence = new Florence({
    store,
    linq: linq as unknown as LinqClient,
    google,
    reasoner,
    enrollmentCodes: new EnrollmentCodes("release-journey-secret-is-at-least-thirty-two-bytes"),
    imageVault: vault,
    messagesUrl: "https://florence.test/messages",
    now: () => new Date(harness.now),
  });
  harness = new Harness(store, florence, linq, vault);
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
    reaction?: FlorenceDecision["conversation"]["reaction"];
    reply?: boolean;
    bubbles?: FlorenceDecision["conversation"]["bubbles"];
    facts?: FlorenceDecision["facts"];
    followUp?: FlorenceDecision["followUp"];
    calendar?: FlorenceDecision["calendar"];
  } = {},
): FlorenceDecision {
  return {
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

function committedCalendar(occurredAt: string) {
  return {
    status: "committed" as const,
    providerReceiptId: "google-event-school-assembly",
    detail: JSON.stringify({
      provider: "google-calendar",
      eventId: "google-event-school-assembly",
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

function calendarDraft(mode: "offer" | "direct", sourceId: string) {
  return { mode, proposalId: null, connectionId: GOOGLE_CONNECTION, event: EVENT, sourceIds: [sourceId] };
}

function storedCalendarAction(sourceId: string) {
  return {
    id: randomUUID(),
    actionId: randomUUID(),
    connectionId: GOOGLE_CONNECTION,
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
