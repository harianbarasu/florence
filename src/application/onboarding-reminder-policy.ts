import { Temporal } from "@js-temporal/polyfill";
import { z } from "zod";

const SAFE_WINDOW_START_HOUR = 9;
const SAFE_WINDOW_END_HOUR = 19;
const NORMAL_REMINDER_HOUR = 10;

export const ONBOARDING_REMINDER_STEPS = [
  "profile",
  "family",
  "children",
  "invite",
  "google",
  "review",
] as const;

const OnboardingReminderInputSchema = z.strictObject({
  onboardingComplete: z.boolean(),
  reminderStage: z.union([z.literal(0), z.literal(1), z.literal(2)]),
  savedNextStep: z.enum(ONBOARDING_REMINDER_STEPS).nullable(),
  lastProgressedAt: z.iso.datetime({ offset: true }),
  lastRemindedAt: z.iso.datetime({ offset: true }).nullable(),
  timeZone: z
    .string()
    .trim()
    .min(1)
    .max(100)
    .superRefine((candidate, context) => {
      try {
        Temporal.ZonedDateTime.from({
          timeZone: candidate,
          year: 2000,
          month: 1,
          day: 1,
          hour: 12,
        });
      } catch {
        context.addIssue({ code: "custom", message: "Expected an IANA time zone" });
      }
    }),
  suppressedAt: z.iso.datetime({ offset: true }).nullable(),
  privateRouteAvailable: z.boolean(),
  privateAuthority: z.enum(["current", "absent", "invalid"]),
  now: z.iso.datetime({ offset: true }),
});

export type OnboardingReminderStep = (typeof ONBOARDING_REMINDER_STEPS)[number];
export type OnboardingReminderStage = 0 | 1 | 2;

export interface OnboardingReminderInput {
  readonly onboardingComplete: boolean;
  /** Number of onboarding reminders already sent. The initial setup link is not a reminder. */
  readonly reminderStage: OnboardingReminderStage;
  readonly savedNextStep: OnboardingReminderStep | null;
  readonly lastProgressedAt: string;
  readonly lastRemindedAt: string | null;
  readonly timeZone: string;
  readonly suppressedAt: string | null;
  readonly privateRouteAvailable: boolean;
  readonly privateAuthority: "current" | "absent" | "invalid";
  readonly now: string;
}

export type OnboardingReminderSuppressionReason =
  | "onboarding_complete"
  | "reminders_suppressed"
  | "reminder_limit_reached"
  | "private_route_unavailable"
  | "private_authority_absent"
  | "private_authority_invalid"
  | "no_actionable_step";

export interface OnboardingReminderCopy {
  readonly lead: string;
  readonly progress: "Your progress is saved.";
  readonly nextStep: string;
  readonly action: "Open your private setup whenever you're ready.";
}

export type OnboardingReminderDecision =
  | {
      readonly kind: "suppress";
      readonly reason: OnboardingReminderSuppressionReason;
    }
  | {
      readonly kind: "schedule" | "send";
      readonly delivery: "private_dm";
      readonly stage: 1 | 2;
      readonly dueAt: string;
      readonly localTiming: {
        readonly timeZone: string;
        readonly localDueAt: string;
        readonly safeWindow: "09:00-19:00";
      };
      readonly savedNextStep: OnboardingReminderStep;
      readonly copy: OnboardingReminderCopy;
    };

const STEP_COPY: Readonly<Record<OnboardingReminderStep, string>> = {
  profile: "confirm your details",
  family: "tell Florence about your family",
  children: "add your children's details",
  invite: "invite your co-parent or caregiver, or choose to do it later",
  google: "connect Google, or choose to skip it",
  review: "review and finish setup",
};

/**
 * Decides whether one private onboarding reminder should be scheduled or sent.
 *
 * The caller owns identity, routing, persistence, timers, and effects. A timer
 * must call this policy again at execution so newer completion, suppression,
 * progress, or authority state can cancel or move the reminder.
 */
export function decideOnboardingReminder(
  inputCandidate: OnboardingReminderInput,
): OnboardingReminderDecision {
  const input = OnboardingReminderInputSchema.parse(inputCandidate);

  if (input.onboardingComplete) return suppress("onboarding_complete");
  if (input.suppressedAt !== null) return suppress("reminders_suppressed");
  if (input.reminderStage === 2) return suppress("reminder_limit_reached");
  if (!input.privateRouteAvailable) return suppress("private_route_unavailable");
  if (input.privateAuthority === "absent") return suppress("private_authority_absent");
  if (input.privateAuthority === "invalid") return suppress("private_authority_invalid");
  if (input.savedNextStep === null) return suppress("no_actionable_step");

  const now = Temporal.Instant.from(input.now);
  const targetStage = input.reminderStage === 0 ? 1 : 2;
  const target = reminderTarget(input.lastProgressedAt, input.lastRemindedAt, input.timeZone, targetStage);
  const nowLocal = now.toZonedDateTimeISO(input.timeZone);

  if (Temporal.Instant.compare(now, target.toInstant()) < 0) {
    return proposal("schedule", targetStage, target, input.savedNextStep);
  }
  if (!isWithinSafeWindow(nowLocal)) {
    return proposal("schedule", targetStage, nextSafeWindowStart(nowLocal), input.savedNextStep);
  }

  return proposal("send", targetStage, nowLocal, input.savedNextStep);
}

function suppress(reason: OnboardingReminderSuppressionReason): OnboardingReminderDecision {
  return { kind: "suppress", reason };
}

function reminderTarget(
  lastProgressedAt: string,
  lastRemindedAt: string | null,
  timeZone: string,
  targetStage: 1 | 2,
): Temporal.ZonedDateTime {
  const progressed = Temporal.Instant.from(lastProgressedAt);
  const anchor =
    targetStage === 2 && lastRemindedAt !== null
      ? Temporal.Instant.compare(progressed, Temporal.Instant.from(lastRemindedAt)) >= 0
        ? progressed
        : Temporal.Instant.from(lastRemindedAt)
      : progressed;
  const anchorDate = anchor.toZonedDateTimeISO(timeZone).toPlainDate();
  const date = anchorDate.add({ days: targetStage === 1 ? 1 : 3 });
  return localDateAt(date, timeZone, NORMAL_REMINDER_HOUR);
}

function isWithinSafeWindow(local: Temporal.ZonedDateTime): boolean {
  return local.hour >= SAFE_WINDOW_START_HOUR && local.hour < SAFE_WINDOW_END_HOUR;
}

function nextSafeWindowStart(local: Temporal.ZonedDateTime): Temporal.ZonedDateTime {
  const date =
    local.hour < SAFE_WINDOW_START_HOUR ? local.toPlainDate() : local.toPlainDate().add({ days: 1 });
  return localDateAt(date, local.timeZoneId, SAFE_WINDOW_START_HOUR);
}

function localDateAt(date: Temporal.PlainDate, timeZone: string, hour: number): Temporal.ZonedDateTime {
  return Temporal.ZonedDateTime.from(
    {
      timeZone,
      year: date.year,
      month: date.month,
      day: date.day,
      hour,
    },
    { disambiguation: "compatible", offset: "prefer" },
  );
}

function proposal(
  kind: "schedule" | "send",
  stage: 1 | 2,
  due: Temporal.ZonedDateTime,
  savedNextStep: OnboardingReminderStep,
): OnboardingReminderDecision {
  return {
    kind,
    delivery: "private_dm",
    stage,
    dueAt: due.toInstant().toString(),
    localTiming: {
      timeZone: due.timeZoneId,
      localDueAt: due.toString(),
      safeWindow: "09:00-19:00",
    },
    savedNextStep,
    copy: {
      lead:
        stage === 1
          ? "A quick reminder to finish setting up Florence."
          : "One last reminder: your Florence setup is ready when you are.",
      progress: "Your progress is saved.",
      nextStep: `Next up: ${STEP_COPY[savedNextStep]}.`,
      action: "Open your private setup whenever you're ready.",
    },
  };
}
