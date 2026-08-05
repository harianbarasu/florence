import { describe, expect, it } from "vitest";
import { type ApplicationOutboxIntent, createFlorenceApplication } from "../../src/application/index.js";
import { ADULT_A, ADULT_B, classificationBase, groupMessage, HOUSEHOLD_ID, setup } from "./fixtures.js";

function domainEffects(intents: readonly ApplicationOutboxIntent[]) {
  return intents.flatMap((intent) => (intent.kind === "domain.effect" ? [intent.effect] : []));
}

describe("Florence application coordinator", () => {
  it("creates a Calendar effect only after an exact group proposal and explicit fresh approval", async () => {
    const harness = setup();
    const app = createFlorenceApplication(harness.dependencies);
    harness.calendarActions.queuePreparation({
      status: "unavailable",
      reason: "ambiguous_write_calendar",
    });
    harness.interpreter.respondToConversation("calendar-create-ambiguous", {
      ...classificationBase,
      intent: "calendar_event_create_request",
      title: "School welcome night",
      startsAt: "2027-09-08T01:00:00Z",
      endsAt: "2027-09-08T02:30:00Z",
      timeZone: "America/Los_Angeles",
      calendarAccountLabel: "Personal",
    });
    await expect(
      app.process(
        groupMessage(
          "calendar-create-ambiguous",
          "Add school welcome night next Tuesday from 6 to 7:30",
          "2027-09-01T16:59:00Z",
        ),
      ),
    ).resolves.toMatchObject({ outcome: { status: "rejected" } });
    expect((await harness.repository.load(HOUSEHOLD_ID))?.aggregate.pendingActions).toEqual([]);
    expect(
      harness.repository
        .intents("conversation.send")
        .flatMap((intent) => (intent.kind === "conversation.send" ? [intent.body] : []))
        .at(-1),
    ).toContain("account / calendar");

    const requestKey = "calendar-create-request";
    harness.calendarActions.queuePreparation({
      status: "ready",
      targetConnectionId: "connection_parent_personal",
      calendarId: "family_schedule_calendar",
      relevantDataDigest: `sha256:${"d".repeat(64)}`,
      hasConflict: true,
    });
    harness.interpreter.respondToConversation(requestKey, {
      ...classificationBase,
      intent: "calendar_event_create_request",
      title: "School welcome night",
      startsAt: "2027-09-08T01:00:00Z",
      endsAt: "2027-09-08T02:30:00Z",
      timeZone: "America/Los_Angeles",
      calendarAccountLabel: "Personal",
      calendarName: "Family schedule",
    });

    await app.process(
      groupMessage(
        requestKey,
        "Add school welcome night next Tuesday from 6 to 7:30 to Personal / Family schedule",
        "2027-09-01T17:00:00Z",
      ),
    );

    let snapshot = await harness.repository.load(HOUSEHOLD_ID);
    const pending = snapshot?.aggregate.pendingActions[0];
    expect(pending).toMatchObject({
      state: "awaiting_approval",
      action: {
        kind: "calendar_update",
        operation: "create",
        householdId: HOUSEHOLD_ID,
        title: "School welcome night",
        startsAt: "2027-09-08T01:00:00Z",
        endsAt: "2027-09-08T02:30:00Z",
        timeZone: "America/Los_Angeles",
        requestedByAdultId: ADULT_A,
        availabilityAdultIds: [ADULT_A, ADULT_B],
        targetConnectionId: "connection_parent_personal",
        calendarId: "family_schedule_calendar",
        hasConflict: true,
      },
    });
    if (pending?.action.kind !== "calendar_update") throw new Error("Expected Calendar action");
    expect(harness.calendarActions.prepareCalls[1]).toMatchObject({
      accountLabel: "Personal",
      calendarName: "Family schedule",
    });
    const approvalRequest = domainEffects(harness.repository.outbox).find(
      (effect) => effect.kind === "send_message" && effect.messageClass === "approval_request",
    );
    expect(approvalRequest).toMatchObject({ targetScope: { kind: "household" } });
    if (approvalRequest?.kind !== "send_message") throw new Error("Expected approval message");
    expect(approvalRequest.body).toContain("One or more private household calendars are busy");
    expect(approvalRequest.body).not.toMatch(/owner|attendee|location|description/iu);

    harness.calendarActions.queuePreparation({
      status: "ready",
      targetConnectionId: "connection_parent_personal",
      calendarId: "family_schedule_calendar",
      relevantDataDigest: pending.action.relevantDataDigest,
      hasConflict: true,
    });
    const approvalKey = "calendar-create-approve";
    harness.interpreter.respondToConversation(approvalKey, {
      ...classificationBase,
      intent: "approve_calendar_event",
      actionId: pending.action.actionId,
    });
    await app.process(
      groupMessage(approvalKey, `Approve ${pending.action.actionId}`, "2027-09-01T17:02:00Z"),
    );
    expect(harness.calendarActions.prepareCalls.at(-1)).toMatchObject({
      targetConnectionId: "connection_parent_personal",
      calendarId: "family_schedule_calendar",
    });

    snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.aggregate.pendingActions[0]).toMatchObject({
      state: "executing",
      approvalId: expect.any(String),
    });
    expect(snapshot?.aggregate.approvals[0]?.status).toBe("consumed");
    const execution = harness.repository
      .intents("domain.effect")
      .find((intent) => intent.kind === "domain.effect" && intent.effect.kind === "execute_external_action");
    if (execution?.kind !== "domain.effect" || execution.effect.kind !== "execute_external_action") {
      throw new Error("Expected approved Calendar effect");
    }
    harness.effectExecutor.respond(execution.intentId, {
      status: "succeeded",
      receiptRef: "calendar_receipt_ref",
      recordedAt: "2027-09-01T17:02:05Z",
      externalAction: {
        receiptId: "calendar_effect_receipt",
        actionId: pending.action.actionId,
        actionDigest: pending.action.actionDigest,
        outcome: "succeeded",
        providerReference: "google-calendar:family_schedule_calendar:event_welcome_night",
      },
    });
    await app.executeOutbox(execution, "2027-09-01T17:02:05Z");

    snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.aggregate.pendingActions[0]).toMatchObject({
      state: "succeeded",
      effectReceipt: {
        receiptId: "calendar_effect_receipt",
        outcome: "succeeded",
        providerReference: "google-calendar:family_schedule_calendar:event_welcome_night",
      },
    });
    expect(harness.repository.commits.at(-1)?.audit).toContainEqual(
      expect.objectContaining({
        kind: "external_action_reconciled",
        decision: "succeeded",
        containsPrivateData: false,
      }),
    );
    expect(
      domainEffects(harness.repository.outbox).some(
        (effect) =>
          effect.kind === "send_message" &&
          effect.messageClass === "status" &&
          effect.body.includes("Added “School welcome night”"),
      ),
    ).toBe(true);
  });

  it("invalidates Calendar approval when its exact target changes without exposing details", async () => {
    const harness = setup();
    const app = createFlorenceApplication(harness.dependencies);
    harness.calendarActions.queuePreparation({
      status: "ready",
      targetConnectionId: "connection_calendar_primary",
      calendarId: "primary",
      relevantDataDigest: `sha256:${"1".repeat(64)}`,
      hasConflict: false,
    });
    harness.interpreter.respondToConversation("calendar-stale-request", {
      ...classificationBase,
      intent: "calendar_event_create_request",
      title: "Family dinner",
      startsAt: "2027-09-10T01:00:00Z",
      endsAt: "2027-09-10T02:00:00Z",
      timeZone: "America/Los_Angeles",
    });
    await app.process(
      groupMessage("calendar-stale-request", "Add family dinner Friday at 6", "2027-09-01T18:00:00Z"),
    );
    const pending = (await harness.repository.load(HOUSEHOLD_ID))?.aggregate.pendingActions[0];
    if (pending?.action.kind !== "calendar_update") throw new Error("Expected Calendar action");
    harness.calendarActions.queuePreparation({
      status: "ready",
      targetConnectionId: pending.action.targetConnectionId,
      calendarId: "different_writable_calendar",
      relevantDataDigest: pending.action.relevantDataDigest,
      hasConflict: pending.action.hasConflict,
    });
    harness.interpreter.respondToConversation("calendar-stale-approval", {
      ...classificationBase,
      intent: "approve_calendar_event",
      actionId: pending.action.actionId,
    });
    const result = await app.process(
      groupMessage("calendar-stale-approval", "Approve it", "2027-09-01T18:01:00Z"),
    );

    expect(result.outcome.status).toBe("rejected");
    expect((await harness.repository.load(HOUSEHOLD_ID))?.aggregate.pendingActions[0]?.state).toBe(
      "awaiting_approval",
    );
    expect(
      domainEffects(harness.repository.outbox).filter((effect) => effect.kind === "execute_external_action"),
    ).toEqual([]);
    const bodies = harness.repository
      .intents("conversation.send")
      .flatMap((intent) => (intent.kind === "conversation.send" ? [intent.body] : []));
    expect(bodies.at(-1)).toContain("selected calendar");
    expect(bodies.join(" ")).not.toMatch(/private-event|calendar owner|attendee|location/iu);
  });

  it("drives a group obligation through owner acknowledgement, reminder, and closure", async () => {
    const harness = setup();
    const app = createFlorenceApplication(harness.dependencies);
    const proposalKey = "group-obligation";
    harness.interpreter.respondToConversation(proposalKey, {
      ...classificationBase,
      intent: "propose_commitment",
      title: "Return the field-trip form",
      requiredOutcome: "The signed field-trip form is returned",
      proposedOwnerAdultId: ADULT_A,
      sourceClass: "school.form",
      sensitivity: "ordinary",
      temporalPlan: {
        planId: "plan_field_trip",
        version: 1,
        timeZone: "America/Los_Angeles",
        deadline: { kind: "instant", at: "2027-01-03T17:00:00Z" },
        usefulLeadMinutes: 1_440,
        preparationMinutes: 30,
        finalBufferMinutes: 30,
        triggers: [
          {
            triggerId: "trigger_field_trip_first",
            timerId: "timer_field_trip_first",
            kind: "reminder",
            at: { kind: "instant", at: "2027-01-02T17:00:00Z" },
          },
          {
            triggerId: "trigger_field_trip_final",
            timerId: "timer_field_trip_final",
            kind: "reminder",
            at: { kind: "instant", at: "2027-01-03T16:00:00Z" },
          },
        ],
      },
    });
    await app.process(
      groupMessage(proposalKey, "Alex can return the field-trip form", "2027-01-01T08:00:00Z"),
    );
    let snapshot = await harness.repository.load(HOUSEHOLD_ID);
    const episodeId = snapshot?.aggregate.episodes[0]?.episodeId;
    expect(snapshot?.aggregate.episodes[0]).toMatchObject({
      episodeId,
      state: "awaiting_acknowledgement",
      owner: { status: "proposed", adultId: ADULT_A },
    });
    expect(
      domainEffects(harness.repository.outbox).filter((effect) => effect.kind === "schedule_timer"),
    ).toHaveLength(2);
    if (episodeId === undefined) throw new Error("Expected the proposed commitment episode");

    const ackKey = "group-obligation-ack";
    harness.interpreter.respondToConversation(ackKey, {
      ...classificationBase,
      intent: "acknowledge_owner",
      episodeId,
      baseEpisodeVersion: 1,
    });
    await app.process({
      ...groupMessage(ackKey, "I will handle it", "2027-01-01T08:01:00Z"),
      replyTo: {
        messageRef: "message_group-obligation-status",
        messageClass: "status",
        responseContext: { kind: "episode_ownership", episodeId, episodeVersion: 1 },
      },
    });
    snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.aggregate.episodes[0]?.state).toBe("active");

    const timerResult = await app.process({
      kind: "timer_fired",
      householdId: HOUSEHOLD_ID,
      idempotencyKey: "timer-fire-first",
      timerId: "timer_field_trip_first",
      episodeId,
      temporalPlanVersion: 1,
      triggerId: "trigger_field_trip_first",
      firedAt: "2027-01-02T17:00:00Z",
    });
    expect(timerResult.outcome.classification).toBe("timer:accepted");
    const duplicate = await app.process({
      kind: "timer_fired",
      householdId: HOUSEHOLD_ID,
      idempotencyKey: "timer-fire-first",
      timerId: "timer_field_trip_first",
      episodeId,
      temporalPlanVersion: 1,
      triggerId: "trigger_field_trip_first",
      firedAt: "2027-01-02T17:00:00Z",
    });
    expect(duplicate.disposition).toBe("duplicate");

    const reminders = domainEffects(harness.repository.outbox).filter(
      (effect) => effect.kind === "send_message" && effect.messageClass === "reminder",
    );
    expect(reminders).toHaveLength(1);
    expect(reminders[0]).toMatchObject({ targetScope: { kind: "household" } });
    if (reminders[0]?.kind === "send_message") {
      expect(reminders[0].body).not.toMatch(/forgot|fault|blame|careless|irresponsible/i);
    }

    const closeKey = "group-obligation-close";
    harness.interpreter.respondToConversation(closeKey, {
      ...classificationBase,
      intent: "close_episode",
      episodeId,
      baseEpisodeVersion: 3,
      outcome: "completed",
      summary: "The signed field-trip form was returned.",
    });
    await app.process({
      ...groupMessage(closeKey, "The form is returned", "2027-01-02T18:00:00Z"),
      replyTo: {
        messageRef: "message_group-obligation-reminder",
        messageClass: "reminder",
        responseContext: { kind: "episode_follow_up", episodeId, episodeVersion: 3 },
      },
    });
    snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.aggregate.episodes[0]?.state).toBe("completed");
    expect(
      domainEffects(harness.repository.outbox).filter((effect) => effect.kind === "cancel_timer"),
    ).toEqual([
      expect.objectContaining({
        timerId: "timer_field_trip_final",
        temporalPlanVersion: 1,
      }),
    ]);
  });
});
