import { Temporal } from "@js-temporal/polyfill";
import { describe, expect, it } from "vitest";
import {
  CoordinationError,
  createCoverageLoopFromOccurrence,
  defaultQuietHours,
  materializeRoutineOccurrence,
  resolveQuietHours,
  resolveSemanticTime,
  reviseRoutineOccurrence,
} from "../../src/modules/coordination/index.js";
import { IDS, routine, routineRevision } from "./fixtures.js";

describe("routine occurrences and household time", () => {
  it("keeps weekly wall-clock meaning stable across DST", () => {
    const plan = {
      timeZone: "America/Los_Angeles",
      event: { kind: "local_clock" as const, time: "08:00" },
      deadline: null,
      preparationMinutes: 0,
      travelMinutes: 0,
      earliestUseful: { kind: "relative" as const, anchor: "event" as const, offsetMinutes: -60 },
      lastResponsible: { kind: "relative" as const, anchor: "event" as const, offsetMinutes: -15 },
    };
    const before = resolveSemanticTime(plan, "2026-03-01");
    const after = resolveSemanticTime(plan, "2026-03-08");

    const beforeLocal = Temporal.Instant.from(before.eventAt ?? "").toZonedDateTimeISO(plan.timeZone);
    const afterLocal = Temporal.Instant.from(after.eventAt ?? "").toZonedDateTimeISO(plan.timeZone);
    expect([beforeLocal.hour, afterLocal.hour]).toEqual([8, 8]);
    expect([before.eventAt, after.eventAt]).toEqual(["2026-03-01T16:00:00Z", "2026-03-08T15:00:00Z"]);
  });

  it("revises one occurrence without rewriting its standing routine or prior version", () => {
    const standingRoutine = routine();
    const occurrence = materializeRoutineOccurrence({
      occurrenceId: IDS.occurrence,
      routine: standingRoutine,
      revision: routineRevision(),
      localDate: "2026-08-05",
      materializedAt: "2026-08-05T16:00:00Z",
    });
    const revised = reviseRoutineOccurrence(occurrence, {
      kind: "reschedule",
      expectedVersion: 1,
      occurredAt: "2026-08-05T17:00:00Z",
      evidenceRef: "evidence:one-off-swap",
      timePlan: {
        timeZone: "America/Los_Angeles",
        event: { kind: "local_clock", time: "16:00" },
        deadline: null,
        preparationMinutes: 30,
        travelMinutes: 15,
        earliestUseful: { kind: "relative", anchor: "event", offsetMinutes: -120 },
        lastResponsible: { kind: "relative", anchor: "event", offsetMinutes: -30 },
      },
    });

    expect(revised).toMatchObject({ version: 2, supersedesVersion: 1, planVersion: 2 });
    expect(revised.timing.eventAt).toBe("2026-08-05T23:00:00Z");
    expect(occurrence).toMatchObject({ version: 1, supersedesVersion: null, planVersion: 1 });
    expect(standingRoutine).toMatchObject({ version: 1, currentRevision: 1 });
  });

  it("auto-covers an occurrence only from the holder's explicit standing authorization", () => {
    const occurrence = materializeRoutineOccurrence({
      occurrenceId: IDS.occurrence,
      routine: routine(),
      revision: routineRevision({
        standingCoverage: {
          holderPersonId: IDS.holder,
          authorizedByPersonId: IDS.holder,
          authorizationKind: "approved",
          authorizedAt: "2026-08-01T17:00:00Z",
        },
      }),
      localDate: "2026-08-05",
      materializedAt: "2026-08-05T16:00:00Z",
    });
    const loop = createCoverageLoopFromOccurrence({
      loopId: IDS.loop,
      householdId: IDS.household,
      occurrence,
    });

    expect(loop).toMatchObject({
      state: "covered",
      acknowledgment: {
        personId: IDS.holder,
        kind: "standing_routine_self_authorized",
      },
    });

    expect(() =>
      routineRevision({
        standingCoverage: {
          holderPersonId: IDS.holder,
          authorizedByPersonId: IDS.actor,
          authorizationKind: "approved",
          authorizedAt: "2026-08-01T17:00:00Z",
        },
      }),
    ).toThrow();
  });

  it("uses the union of group quiet hours and requires every participant for an override", () => {
    const west = defaultQuietHours(IDS.actor, "America/Los_Angeles");
    const east = defaultQuietHours(IDS.holder, "America/New_York");
    const blocked = resolveQuietHours({
      candidateAt: "2026-01-15T04:30:00Z",
      lastResponsibleAt: "2026-01-15T05:00:00Z",
      category: "coverage_reminder",
      policies: [west, east],
    });
    expect(blocked).toEqual({
      kind: "suppressed",
      reason: "quiet_hours_cross_last_responsible",
    });

    const override = resolveQuietHours({
      candidateAt: "2026-01-15T04:30:00Z",
      lastResponsibleAt: "2026-01-15T05:00:00Z",
      category: "coverage_reminder",
      policies: [
        { ...west, allowLastResponsibleOverrideFor: ["coverage_reminder"] },
        { ...east, allowLastResponsibleOverrideFor: ["coverage_reminder"] },
      ],
    });
    expect(override).toEqual({
      kind: "allowed",
      sendAt: "2026-01-15T04:30:00Z",
      usedOverride: true,
    });
  });

  it("rejects a materialization outside the routine recurrence", () => {
    expect(() =>
      materializeRoutineOccurrence({
        occurrenceId: IDS.occurrence,
        routine: routine(),
        revision: routineRevision(),
        localDate: "2026-08-06",
        materializedAt: "2026-08-05T16:00:00Z",
      }),
    ).toThrowError(new CoordinationError("date_not_in_recurrence"));
  });
});
