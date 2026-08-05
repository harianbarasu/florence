import { describe, expect, it } from "vitest";
import {
  HouseholdTime,
  type HouseholdTimeError,
  ResolvedTimePlanSchema,
  RoutineAnchorSchema,
  SemanticTimePlanSchema,
  TemporalTriggerIdSchema,
} from "../../src/domain/index.js";
import { timePlan } from "./fixtures.js";

describe("HouseholdTime interface", () => {
  it("derives a useful action window from lead, preparation, and final buffer", () => {
    const resolved = HouseholdTime.resolve({ plan: timePlan(), routineAnchors: [] });

    expect(resolved.earliestUsefulAt).toBe("2026-01-01T17:00:00Z");
    expect(resolved.lastResponsibleAt).toBe("2026-01-02T16:00:00Z");
    expect(resolved.referenceAt).toBe("2026-01-02T17:00:00Z");
    expect(resolved.triggers.map((trigger) => trigger.triggerId)).toEqual(["trigger_1"]);
  });

  it("resolves a nonexistent spring-forward local time with explicit compatible semantics", () => {
    const plan = SemanticTimePlanSchema.parse({
      planId: "plan_spring",
      version: 1,
      timeZone: "America/Los_Angeles",
      event: {
        kind: "local",
        date: "2026-03-08",
        time: "02:30",
        timeZone: "America/Los_Angeles",
        disambiguation: "compatible",
      },
      usefulLeadMinutes: 60,
      preparationMinutes: 0,
      finalBufferMinutes: 0,
      triggers: [],
    });

    expect(HouseholdTime.resolve({ plan, routineAnchors: [] }).eventAt).toBe("2026-03-08T10:30:00Z");
  });

  it("fails closed when a nonexistent local time uses reject disambiguation", () => {
    const plan = SemanticTimePlanSchema.parse({
      planId: "plan_spring_reject",
      version: 1,
      timeZone: "America/Los_Angeles",
      event: {
        kind: "local",
        date: "2026-03-08",
        time: "02:30",
        timeZone: "America/Los_Angeles",
        disambiguation: "reject",
      },
      usefulLeadMinutes: 60,
      preparationMinutes: 0,
      finalBufferMinutes: 0,
      triggers: [],
    });

    expect(() => HouseholdTime.resolve({ plan, routineAnchors: [] })).toThrowError(
      expect.objectContaining<Partial<HouseholdTimeError>>({ code: "invalid_local_time" }),
    );
  });

  it("distinguishes both occurrences of a fall-back local time", () => {
    const makePlan = (disambiguation: "earlier" | "later") =>
      SemanticTimePlanSchema.parse({
        planId: `plan_fall_${disambiguation}`,
        version: 1,
        timeZone: "America/Los_Angeles",
        event: {
          kind: "local",
          date: "2026-11-01",
          time: "01:30",
          timeZone: "America/Los_Angeles",
          disambiguation,
        },
        usefulLeadMinutes: 0,
        preparationMinutes: 0,
        finalBufferMinutes: 0,
        triggers: [],
      });

    const earlier = HouseholdTime.resolve({ plan: makePlan("earlier"), routineAnchors: [] });
    const later = HouseholdTime.resolve({ plan: makePlan("later"), routineAnchors: [] });

    expect(earlier.eventAt).toBe("2026-11-01T08:30:00Z");
    expect(later.eventAt).toBe("2026-11-01T09:30:00Z");
  });

  it("keeps a household routine at the same wall-clock time across DST", () => {
    const anchor = RoutineAnchorSchema.parse({
      anchorId: "anchor_departure",
      label: "School departure",
      timeZone: "America/Los_Angeles",
      localTime: "07:30",
      daysOfWeek: [6, 7],
    });
    const makePlan = (date: string, planId: string) =>
      SemanticTimePlanSchema.parse({
        planId,
        version: 1,
        timeZone: "America/Los_Angeles",
        event: {
          kind: "routine_anchor",
          anchorId: anchor.anchorId,
          date,
          offsetMinutes: 0,
          disambiguation: "compatible",
        },
        usefulLeadMinutes: 0,
        preparationMinutes: 0,
        finalBufferMinutes: 0,
        triggers: [],
      });

    const before = HouseholdTime.resolve({
      plan: makePlan("2026-03-07", "plan_before_dst"),
      routineAnchors: [anchor],
    });
    const after = HouseholdTime.resolve({
      plan: makePlan("2026-03-08", "plan_after_dst"),
      routineAnchors: [anchor],
    });

    expect(before.eventAt).toBe("2026-03-07T15:30:00Z");
    expect(after.eventAt).toBe("2026-03-08T14:30:00Z");
  });

  it("rejects reminders that arrive after the last responsible moment", () => {
    const plan = timePlan({
      triggers: [
        {
          triggerId: "trigger_late",
          timerId: "timer_late",
          kind: "reminder",
          at: { kind: "instant", at: "2026-01-02T16:30:00Z" },
        },
      ],
    });

    expect(() => HouseholdTime.resolve({ plan, routineAnchors: [] })).toThrowError(
      expect.objectContaining<Partial<HouseholdTimeError>>({ code: "trigger_outside_window" }),
    );
  });

  it("reevaluates timers against exact current time instead of treating them as authority", () => {
    const plan = HouseholdTime.resolve({
      plan: timePlan({
        triggers: [
          {
            triggerId: "trigger_window",
            timerId: "timer_window",
            kind: "window_check",
            at: { kind: "instant", at: "2026-01-02T16:00:00Z" },
          },
        ],
      }),
      routineAnchors: [],
    });
    const triggerId = TemporalTriggerIdSchema.parse("trigger_window");

    expect(
      HouseholdTime.evaluateTimer({
        plan,
        triggerId,
        now: "2026-01-02T15:59:59Z",
      }).decision,
    ).toBe("not_due");
    expect(
      HouseholdTime.evaluateTimer({
        plan,
        triggerId,
        now: "2026-01-02T16:00:00Z",
      }).decision,
    ).toBe("missed_window");

    const emitted = ResolvedTimePlanSchema.parse({
      ...plan,
      triggers: plan.triggers.map((trigger) => ({ ...trigger, status: "emitted" })),
    });
    expect(
      HouseholdTime.evaluateTimer({
        plan: emitted,
        triggerId,
        now: "2026-01-02T16:00:00Z",
      }).decision,
    ).toBe("obsolete");
  });
});
