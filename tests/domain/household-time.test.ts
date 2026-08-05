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
