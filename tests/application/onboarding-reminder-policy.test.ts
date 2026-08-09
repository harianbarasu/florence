import { describe, expect, it } from "vitest";
import {
  decideOnboardingReminder,
  type OnboardingReminderInput,
} from "../../src/application/onboarding-reminder-policy.js";

const BASE_INPUT: OnboardingReminderInput = {
  onboardingComplete: false,
  reminderStage: 0,
  savedNextStep: "children",
  lastProgressedAt: "2026-03-07T20:00:00Z",
  lastRemindedAt: null,
  timeZone: "America/Los_Angeles",
  suppressedAt: null,
  privateRouteAvailable: true,
  privateAuthority: "current",
  now: "2026-03-07T20:05:00Z",
};

describe("onboarding reminder policy", () => {
  it("schedules the first reminder for a sensible local time across DST", () => {
    expect(decideOnboardingReminder(BASE_INPUT)).toEqual({
      kind: "schedule",
      delivery: "private_dm",
      stage: 1,
      dueAt: "2026-03-08T17:00:00Z",
      localTiming: {
        timeZone: "America/Los_Angeles",
        localDueAt: "2026-03-08T10:00:00-07:00[America/Los_Angeles]",
        safeWindow: "09:00-19:00",
      },
      savedNextStep: "children",
      copy: {
        lead: "A quick reminder to finish setting up Florence.",
        progress: "Your progress is saved.",
        nextStep: "Next up: add your children's details.",
        action: "Open your private setup whenever you're ready.",
      },
    });
  });

  it("proposes the final reminder three local calendar days after the first target", () => {
    const decision = decideOnboardingReminder({
      ...BASE_INPUT,
      reminderStage: 1,
      lastRemindedAt: "2026-03-08T17:00:00Z",
      savedNextStep: "google",
      now: "2026-03-08T17:01:00Z",
    });

    expect(decision).toMatchObject({
      kind: "schedule",
      delivery: "private_dm",
      stage: 2,
      dueAt: "2026-03-11T17:00:00Z",
      savedNextStep: "google",
      copy: {
        lead: "One last reminder: your Florence setup is ready when you are.",
        progress: "Your progress is saved.",
        nextStep: "Next up: connect Google, or choose to skip it.",
      },
    });
  });

  it("anchors the final reminder to a delayed first delivery", () => {
    expect(
      decideOnboardingReminder({
        ...BASE_INPUT,
        reminderStage: 1,
        lastRemindedAt: "2026-03-10T17:00:00Z",
        now: "2026-03-10T17:01:00Z",
      }),
    ).toMatchObject({
      kind: "schedule",
      stage: 2,
      dueAt: "2026-03-13T17:00:00Z",
    });
  });

  it("sends only when due and inside the private local window", () => {
    expect(
      decideOnboardingReminder({
        ...BASE_INPUT,
        now: "2026-03-08T17:00:00Z",
      }),
    ).toMatchObject({
      kind: "send",
      delivery: "private_dm",
      stage: 1,
      dueAt: "2026-03-08T17:00:00Z",
    });
  });

  it("moves an overdue reminder to the next safe local window instead of sending at night", () => {
    expect(
      decideOnboardingReminder({
        ...BASE_INPUT,
        now: "2026-11-01T04:30:00Z",
      }),
    ).toMatchObject({
      kind: "schedule",
      stage: 1,
      dueAt: "2026-11-01T17:00:00Z",
      localTiming: {
        localDueAt: "2026-11-01T09:00:00-08:00[America/Los_Angeles]",
      },
    });
  });

  it.each([
    [{ onboardingComplete: true }, "onboarding_complete"],
    [{ suppressedAt: "2026-03-07T20:01:00Z" }, "reminders_suppressed"],
    [{ reminderStage: 2 }, "reminder_limit_reached"],
    [{ privateRouteAvailable: false }, "private_route_unavailable"],
    [{ privateAuthority: "absent" }, "private_authority_absent"],
    [{ privateAuthority: "invalid" }, "private_authority_invalid"],
    [{ savedNextStep: null }, "no_actionable_step"],
  ] as const)("fails closed for %s", (override, reason) => {
    expect(decideOnboardingReminder({ ...BASE_INPUT, ...override })).toEqual({
      kind: "suppress",
      reason,
    });
  });
});
