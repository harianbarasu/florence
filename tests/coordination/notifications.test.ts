import { describe, expect, it } from "vitest";
import {
  createCoverageLoop,
  createCoverageTimer,
  planCoverageFollowUpTimer,
  reevaluateCoverageTimer,
  renderNeutralNotification,
  transitionCoverage,
} from "../../src/modules/coordination/index.js";
import { destination, IDS, timing } from "./fixtures.js";

function requestedLoop() {
  const loop = createCoverageLoop({
    loopId: IDS.loop,
    householdId: IDS.household,
    minimumSharedMeaning: "Wednesday pickup",
    unresolvedFacts: [],
    proposedHolderPersonId: IDS.holder,
    timing: timing(),
    destination,
    sourceEvidenceRefs: ["evidence:group-message"],
    occurredAt: "2026-08-05T18:00:00Z",
  });
  return transitionCoverage(loop, {
    kind: "request_coverage",
    transitionId: IDS.transition1,
    expectedVersion: 1,
    actorPersonId: IDS.actor,
    requestedPersonId: IDS.holder,
    occurredAt: "2026-08-05T18:01:00Z",
    evidenceRefs: [],
  }).loop;
}

describe("notification planning", () => {
  it("derives a deterministic reminder, then a private-steward escalation before expiry", () => {
    const requested = requestedLoop();
    const reminder = planCoverageFollowUpTimer({
      loop: requested,
      now: requested.timing.earliestUsefulAt,
      remindersAuthorized: true,
    });
    const replay = planCoverageFollowUpTimer({
      loop: requested,
      now: requested.timing.earliestUsefulAt,
      remindersAuthorized: true,
    });
    const escalation = planCoverageFollowUpTimer({
      loop: requested,
      now: requested.timing.earliestUsefulAt,
      remindersAuthorized: false,
    });
    const expiryOnly = planCoverageFollowUpTimer({
      loop: requested,
      now: requested.timing.earliestUsefulAt,
      remindersAuthorized: false,
      stewardEscalationStarted: true,
    });

    expect(reminder).not.toBeNull();
    expect(replay).toEqual(reminder);
    expect(reminder?.dueAt).not.toBe(requested.timing.earliestUsefulAt);
    expect(reminder?.dueAt).not.toBe(requested.timing.lastResponsibleAt);
    expect(escalation).toMatchObject({ category: "coverage_steward_escalation" });
    expect(escalation?.dueAt).not.toBe(requested.timing.earliestUsefulAt);
    expect(escalation?.dueAt).not.toBe(requested.timing.lastResponsibleAt);
    expect(expiryOnly).toMatchObject({
      category: "coverage_steward_escalation",
      dueAt: requested.timing.lastResponsibleAt,
    });
  });

  it("treats timers as reevaluation requests and rejects stale loop or epoch authority", () => {
    const requested = requestedLoop();
    const timer = createCoverageTimer({
      timerId: IDS.timer,
      loop: requested,
      category: "coverage_opening",
      dueAt: requested.timing.earliestUsefulAt,
    });
    const declined = transitionCoverage(requested, {
      kind: "decline_coverage",
      transitionId: IDS.transition2,
      expectedVersion: 2,
      actorPersonId: IDS.holder,
      visibility: "private",
      privateReason: "private reason",
      occurredAt: "2026-08-05T18:02:00Z",
      evidenceRefs: [],
    }).loop;

    expect(
      reevaluateCoverageTimer({
        timer,
        loop: declined,
        now: timer.dueAt,
        notificationId: IDS.notification1,
        liveDestination: destination,
        canWrite: true,
        quietHours: [],
      }),
    ).toEqual({ kind: "suppress", reason: "stale_timer" });

    expect(
      reevaluateCoverageTimer({
        timer,
        loop: requested,
        now: timer.dueAt,
        notificationId: IDS.notification1,
        liveDestination: {
          ...destination,
          participantEpochId: IDS.nextEpoch,
          participantSetDigest: "b".repeat(64),
        },
        canWrite: true,
        quietHours: [],
      }),
    ).toEqual({ kind: "suppress", reason: "stale_timer" });
  });

  it("plans fixed neutral templates and enforces one opening and reminder per risk cycle", () => {
    const requested = requestedLoop();
    const openingTimer = createCoverageTimer({
      timerId: IDS.timer,
      loop: requested,
      category: "coverage_opening",
      dueAt: requested.timing.earliestUsefulAt,
    });
    const opening = reevaluateCoverageTimer({
      timer: openingTimer,
      loop: requested,
      now: openingTimer.dueAt,
      notificationId: IDS.notification1,
      liveDestination: destination,
      canWrite: true,
      quietHours: [],
    });
    expect(opening.kind).toBe("send");
    if (opening.kind !== "send") throw new Error("Expected an opening plan");

    const rendered = renderNeutralNotification(opening.plan);
    expect(rendered).toBe(
      "Wednesday pickup Coverage is still open. Is it handled, or should we find someone?",
    );
    expect(rendered).not.toContain(IDS.holder);
    expect(rendered.toLowerCase()).not.toMatch(/failed|fault|neglect|blame/u);

    const duplicateTimer = createCoverageTimer({
      timerId: IDS.notification2,
      loop: opening.loop,
      category: "coverage_opening",
      dueAt: opening.loop.timing.earliestUsefulAt,
    });
    expect(
      reevaluateCoverageTimer({
        timer: duplicateTimer,
        loop: opening.loop,
        now: duplicateTimer.dueAt,
        notificationId: IDS.notification2,
        liveDestination: destination,
        canWrite: true,
        quietHours: [],
      }),
    ).toEqual({ kind: "suppress", reason: "duplicate_notification" });

    const reminderTimer = createCoverageTimer({
      timerId: IDS.notification2,
      loop: opening.loop,
      category: "coverage_reminder",
      dueAt: "2026-08-06T20:30:00Z",
    });
    const reminder = reevaluateCoverageTimer({
      timer: reminderTimer,
      loop: opening.loop,
      now: reminderTimer.dueAt,
      notificationId: IDS.notification2,
      liveDestination: destination,
      canWrite: true,
      quietHours: [],
    });
    expect(reminder.kind).toBe("send");
  });

  it("never converts a current covered state into a reminder", () => {
    const requested = requestedLoop();
    const timer = createCoverageTimer({
      timerId: IDS.timer,
      loop: requested,
      category: "coverage_reminder",
      dueAt: "2026-08-06T20:30:00Z",
    });
    const covered = transitionCoverage(requested, {
      kind: "acknowledge_coverage",
      transitionId: IDS.transition2,
      expectedVersion: 2,
      actorPersonId: IDS.holder,
      acknowledgment: "explicit_self",
      visibility: "shared",
      occurredAt: "2026-08-06T19:00:00Z",
      evidenceRefs: [],
    }).loop;

    expect(
      reevaluateCoverageTimer({
        timer,
        loop: covered,
        now: timer.dueAt,
        notificationId: IDS.notification1,
        liveDestination: destination,
        canWrite: true,
        quietHours: [],
      }),
    ).toEqual({ kind: "suppress", reason: "stale_timer" });
  });
});
