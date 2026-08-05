import { describe, expect, it } from "vitest";
import { createFlorenceApplication } from "../../src/application/index.js";
import { ADULT_A, directMessage, HOUSEHOLD_ID, setup } from "./fixtures.js";

const DIGEST_A = `sha256:${"a".repeat(64)}`;
const DIGEST_B = `sha256:${"b".repeat(64)}`;

function calendarEvent(key: string, revision = 1, overrides: Record<string, unknown> = {}) {
  return {
    kind: "calendar_event" as const,
    householdId: HOUSEHOLD_ID,
    idempotencyKey: key,
    occurredAt: `2027-01-01T0${revision + 7}:00:00Z`,
    ownerAdultId: ADULT_A,
    accountRef: "google-account-alex",
    eventRef: "calendar-event-school",
    providerRef: "google-calendar-event-school",
    revision,
    contentDigest: revision === 1 ? DIGEST_A : DIGEST_B,
    title: "RAW PRIVATE TITLE 9917",
    description: "RAW PRIVATE DESCRIPTION 5522",
    location: "RAW PRIVATE LOCATION 7733",
    startsAt: "2027-01-03T02:00:00Z",
    endsAt: "2027-01-03T03:00:00Z",
    timeZone: "America/Los_Angeles",
    allDay: false,
    status: "confirmed" as const,
    recurrence: [],
    ...overrides,
  };
}

function calendarProposal(meaning: string, outcome: string) {
  return {
    decision: "propose_family_episode" as const,
    confidence: 0.97,
    sourceClass: "calendar.school",
    sensitivity: "ordinary" as const,
    familyImpact: true,
    rationale: "The event requires household coordination.",
    privateSummary: "A private school event may need family coordination.",
    minimumHouseholdMeaning: meaning,
    minimumRequiredOutcome: outcome,
  };
}

describe("private inbound Calendar lifecycle", () => {
  it.each([
    ["retain_private", "retain_private", undefined],
    ["private_review", "private_review", "private_review"],
    ["private_interrupt", "private_interrupt", "private_interrupt"],
  ] as const)(
    "keeps %s decisions private and persists no raw event fields",
    async (decision, classification, expectedMessageClass) => {
      const harness = setup();
      const app = createFlorenceApplication(harness.dependencies);
      const key = `calendar-private-${decision}`;
      harness.interpreter.respondToCalendar(key, {
        decision,
        confidence: 0.9,
        sourceClass: "calendar.personal",
        sensitivity: "sensitive",
        familyImpact: false,
        rationale: "The item has no household consequence.",
        privateSummary: "A private Calendar item is available for review.",
        ...(decision === "private_interrupt"
          ? { urgencyReason: "The private item may need attention soon." }
          : {}),
      });

      const result = await app.process(calendarEvent(key));
      expect(result.outcome.classification).toBe(`calendar:${classification}`);
      const snapshot = await harness.repository.load(HOUSEHOLD_ID);
      expect(snapshot?.projection.calendarSources).toEqual([
        expect.objectContaining({ status: "active", latestRevision: 1 }),
      ]);
      expect(snapshot?.projection.calendarTriage).toEqual([
        expect.objectContaining({ decision, revision: 1 }),
      ]);
      const serialized = JSON.stringify(snapshot);
      expect(serialized).not.toContain("RAW PRIVATE TITLE");
      expect(serialized).not.toContain("RAW PRIVATE DESCRIPTION");
      expect(serialized).not.toContain("RAW PRIVATE LOCATION");

      const messages = harness.repository
        .intents("conversation.send")
        .filter((intent) => intent.kind === "conversation.send");
      expect(messages.some((intent) => intent.targetScope.kind === "household")).toBe(false);
      if (expectedMessageClass === undefined) {
        expect(messages).toEqual([]);
      } else {
        expect(messages).toEqual([
          expect.objectContaining({
            targetScope: { kind: "personal", adultId: ADULT_A },
            messageClass: expectedMessageClass,
          }),
        ]);
      }
    },
  );

  it.each([
    ["far-future", "2027-01-10T08:00:00Z", 2],
    ["near-future", "2027-01-01T09:30:00Z", 1],
    ["already-too-late", "2027-01-01T08:20:00Z", 0],
  ] as const)(
    "builds a bounded deterministic timing plan for a %s event",
    async (label, startsAt, expectedTriggerCount) => {
      const harness = setup();
      const app = createFlorenceApplication(harness.dependencies);
      const key = `calendar-timing-${label}`;
      harness.interpreter.respondToCalendar(
        key,
        calendarProposal("A family event needs planning.", "The family plan covers the event."),
      );
      await app.process(
        calendarEvent(key, 1, {
          eventRef: `calendar-event-${label}`,
          providerRef: `provider-event-${label}`,
          startsAt,
          endsAt: new Date(Date.parse(startsAt) + 60 * 60_000).toISOString(),
        }),
      );

      const plan = (await harness.repository.load(HOUSEHOLD_ID))?.projection.pendingPromotions[0]?.proposal
        .temporalPlan;
      expect(plan).toBeDefined();
      expect(plan?.earliestUseful).toBeDefined();
      expect(plan?.lastResponsible).toBeDefined();
      expect(plan?.triggers).toHaveLength(expectedTriggerCount);
      const observedAt = Date.parse("2027-01-01T08:00:00Z");
      const lastResponsibleAt =
        plan?.lastResponsible?.kind === "instant" ? Date.parse(plan.lastResponsible.at) : Number.NaN;
      for (const trigger of plan?.triggers ?? []) {
        expect(trigger.at.kind).toBe("instant");
        if (trigger.at.kind !== "instant") throw new Error("Expected an absolute Calendar trigger");
        expect(Date.parse(trigger.at.at)).toBeGreaterThan(observedAt);
        expect(Date.parse(trigger.at.at)).toBeLessThanOrEqual(lastResponsibleAt);
      }
      expect(new Set((plan?.triggers ?? []).map((trigger) => trigger.triggerId)).size).toBe(
        expectedTriggerCount,
      );
      expect(new Set((plan?.triggers ?? []).map((trigger) => trigger.timerId)).size).toBe(
        expectedTriggerCount,
      );
    },
  );

  it("promotes only explicit minimum meaning, then applies an explicitly remembered matching rule", async () => {
    const harness = setup();
    const app = createFlorenceApplication(harness.dependencies);
    const firstKey = "calendar-promotion-first";
    harness.interpreter.respondToCalendar(
      firstKey,
      calendarProposal(
        "School has an evening event Saturday.",
        "The family plan accounts for the school event.",
      ),
    );
    await app.process(calendarEvent(firstKey));

    let snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.aggregate.episodes).toEqual([]);
    expect(snapshot?.projection.pendingPromotions).toHaveLength(1);
    expect(
      harness.repository
        .intents("conversation.send")
        .some((intent) => intent.kind === "conversation.send" && intent.targetScope.kind === "household"),
    ).toBe(false);

    const promotionId = snapshot?.projection.pendingPromotions[0]?.promotionId;
    expect(promotionId).toBeTruthy();
    const approvalKey = "calendar-promotion-approve";
    harness.interpreter.respondToConversation(approvalKey, {
      intent: "approve_promotion",
      confidence: 1,
      rationale: "The adult explicitly approved and remembered this exact proposal.",
      promotionId,
      rememberForMatchingSource: true,
    });
    await app.process(
      directMessage(approvalKey, `Always share ${promotionId}`, ADULT_A, "2027-01-01T08:05:00Z"),
    );

    snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.projection.pendingPromotions).toEqual([]);
    expect(snapshot?.projection.calendarSources[0]).toMatchObject({
      episodeId: snapshot?.aggregate.episodes[0]?.episodeId,
    });
    expect(snapshot?.aggregate.episodes[0]).toMatchObject({
      scope: { kind: "household" },
      title: "School has an evening event Saturday.",
      requiredOutcome: "The family plan accounts for the school event.",
      promotionAuthority: { kind: "approval" },
      temporalPlan: { eventAt: "2027-01-03T02:00:00Z" },
    });
    expect(snapshot?.aggregate.policies).toEqual([
      expect.objectContaining({
        status: "active",
        rule: expect.objectContaining({ sourceClass: "calendar.school" }),
      }),
    ]);

    const secondKey = "calendar-promotion-second";
    harness.interpreter.respondToCalendar(
      secondKey,
      calendarProposal(
        "School has an evening event Sunday.",
        "The family plan accounts for Sunday's school event.",
      ),
    );
    await app.process(
      calendarEvent(secondKey, 1, {
        eventRef: "calendar-event-school-2",
        providerRef: "google-calendar-event-school-2",
        startsAt: "2027-01-04T02:00:00Z",
        endsAt: "2027-01-04T03:00:00Z",
      }),
    );
    snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.projection.pendingPromotions).toEqual([]);
    expect(snapshot?.aggregate.episodes).toHaveLength(2);
    expect(snapshot?.aggregate.episodes[1]).toMatchObject({
      title: "School has an evening event Sunday.",
      promotionAuthority: { kind: "policy" },
    });

    const householdBodies = harness.repository
      .intents("conversation.send")
      .flatMap((intent) =>
        intent.kind === "conversation.send" && intent.targetScope.kind === "household" ? [intent.body] : [],
      );
    expect(householdBodies).toEqual([
      "School has an evening event Saturday.",
      "School has an evening event Sunday.",
    ]);
    expect(JSON.stringify(snapshot)).not.toContain("RAW PRIVATE");
    expect(householdBodies.join(" ")).not.toContain("9917");
  });

  it("invalidates pending work, supersedes promoted work on a newer revision, and never resurrects stale data", async () => {
    const harness = setup();
    const app = createFlorenceApplication(harness.dependencies);
    const firstKey = "calendar-revision-first";
    harness.interpreter.respondToCalendar(
      firstKey,
      calendarProposal("A school event needs planning.", "The family plan covers the school event."),
    );
    await app.process(calendarEvent(firstKey));
    const promotionId = (await harness.repository.load(HOUSEHOLD_ID))?.projection.pendingPromotions[0]
      ?.promotionId;
    const approvalKey = "calendar-revision-approve";
    harness.interpreter.respondToConversation(approvalKey, {
      intent: "approve_promotion",
      confidence: 1,
      rationale: "The adult explicitly approved this proposal once.",
      promotionId,
    });
    await app.process(
      directMessage(approvalKey, `Share once ${promotionId}`, ADULT_A, "2027-01-01T08:05:00Z"),
    );

    const revisionKey = "calendar-revision-second";
    harness.interpreter.respondToCalendar(revisionKey, {
      decision: "retain_private",
      confidence: 0.9,
      sourceClass: "calendar.personal",
      sensitivity: "sensitive",
      familyImpact: false,
      rationale: "The revised event has no current household consequence.",
      privateSummary: "The revised event remains private.",
    });
    await app.process(calendarEvent(revisionKey, 2));

    let snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.aggregate.episodes[0]).toMatchObject({ state: "superseded", version: 2 });
    expect(snapshot?.projection.pendingPromotions).toEqual([]);
    expect(snapshot?.projection.calendarSources[0]).toMatchObject({
      latestRevision: 2,
      status: "active",
    });
    expect(snapshot?.projection.calendarSources[0]).not.toHaveProperty("episodeId");
    const cancellations = harness.repository
      .intents("domain.effect")
      .flatMap((intent) =>
        intent.kind === "domain.effect" && intent.effect.kind === "cancel_timer" ? [intent.effect] : [],
      );
    expect(cancellations).toHaveLength(2);
    expect(new Set(cancellations.map((effect) => effect.timerId)).size).toBe(2);

    const callsAfterRevision = harness.interpreter.calendarCalls.length;
    const stale = await app.process(calendarEvent("calendar-revision-stale", 1));
    expect(stale.outcome.classification).toBe("calendar:stale_revision");
    expect(harness.interpreter.calendarCalls).toHaveLength(callsAfterRevision);

    const deleted = await app.process({
      kind: "calendar_event_deleted",
      householdId: HOUSEHOLD_ID,
      idempotencyKey: "calendar-revision-deleted",
      occurredAt: "2027-01-01T10:00:00Z",
      ownerAdultId: ADULT_A,
      accountRef: "google-account-alex",
      eventRef: "calendar-event-school",
      providerRef: "google-calendar-event-school",
      revision: 3,
    });
    expect(deleted.outcome.classification).toBe("calendar:deleted");
    expect(harness.interpreter.calendarCalls).toHaveLength(callsAfterRevision);
    snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.projection.calendarSources[0]).toMatchObject({
      latestRevision: 3,
      status: "deleted",
    });
    expect(snapshot?.projection.calendarSources[0]).not.toHaveProperty("contentDigest");

    const duplicateDeletion = await app.process({
      kind: "calendar_event_deleted",
      householdId: HOUSEHOLD_ID,
      idempotencyKey: "calendar-revision-deleted-duplicate",
      occurredAt: "2027-01-01T10:01:00Z",
      ownerAdultId: ADULT_A,
      accountRef: "google-account-alex",
      eventRef: "calendar-event-school",
      providerRef: "google-calendar-event-school",
      revision: 3,
    });
    expect(duplicateDeletion.outcome.classification).toBe("calendar:duplicate_revision");
    expect(harness.interpreter.calendarCalls).toHaveLength(callsAfterRevision);

    const staleAfterDelete = await app.process(calendarEvent("calendar-revision-stale-after-delete", 2));
    expect(staleAfterDelete.outcome.classification).toBe("calendar:stale_revision");
    expect(harness.interpreter.calendarCalls).toHaveLength(callsAfterRevision);
    expect((await harness.repository.load(HOUSEHOLD_ID))?.projection.calendarSources[0]).toMatchObject({
      latestRevision: 3,
      status: "deleted",
    });
  });

  it("withdraws an unapproved Calendar promotion when a newer private revision arrives", async () => {
    const harness = setup();
    const app = createFlorenceApplication(harness.dependencies);
    const firstKey = "calendar-pending-revision-first";
    harness.interpreter.respondToCalendar(
      firstKey,
      calendarProposal("A school event may need planning.", "The family decides how to cover it."),
    );
    await app.process(calendarEvent(firstKey));
    expect((await harness.repository.load(HOUSEHOLD_ID))?.projection.pendingPromotions).toHaveLength(1);

    const secondKey = "calendar-pending-revision-second";
    harness.interpreter.respondToCalendar(secondKey, {
      decision: "retain_private",
      confidence: 0.93,
      sourceClass: "calendar.personal",
      sensitivity: "sensitive",
      familyImpact: false,
      rationale: "The revised event has no household consequence.",
      privateSummary: "The revised item remains private.",
    });
    await app.process(calendarEvent(secondKey, 2));

    const snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.projection.pendingPromotions).toEqual([]);
    expect(snapshot?.projection.calendarSources[0]).toMatchObject({ latestRevision: 2 });
    expect(snapshot?.projection.calendarSources[0]).not.toHaveProperty("pendingPromotionId");
    expect(harness.repository.intents("conversation.send").at(-1)).toMatchObject({
      targetScope: { kind: "personal", adultId: ADULT_A },
      messageClass: "status",
      body: "A private Calendar item changed, so its earlier sharing proposal is no longer pending.",
    });
  });

  it("deduplicates the exact application input before another model call", async () => {
    const harness = setup();
    const app = createFlorenceApplication(harness.dependencies);
    const key = "calendar-idempotent";
    harness.interpreter.respondToCalendar(key, {
      decision: "ignore",
      confidence: 1,
      sourceClass: "calendar.noise",
      sensitivity: "ordinary",
      familyImpact: false,
      rationale: "The event has no household consequence.",
    });
    const first = await app.process(calendarEvent(key));
    const duplicate = await app.process(calendarEvent(key));
    expect(first.disposition).toBe("committed");
    expect(duplicate.disposition).toBe("duplicate");
    expect(harness.interpreter.calendarCalls).toHaveLength(1);
  });
});
