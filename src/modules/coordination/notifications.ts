import { createHash } from "node:crypto";
import { Temporal } from "@js-temporal/polyfill";
import {
  CoordinationError,
  type CoverageLoop,
  CoverageLoopSchema,
  type CoverageTimer,
  CoverageTimerSchema,
  type DestinationEpoch,
  DestinationEpochSchema,
  EntityIdSchema,
  type MinimumSharedCoverageStatus,
  MinimumSharedCoverageStatusSchema,
  type NeutralNotificationPlan,
  NeutralNotificationPlanSchema,
  type NotificationCategory,
  type QuietHoursPolicy,
} from "./contracts.js";
import { compareInstants, resolveQuietHours } from "./household-time.js";

export type NotificationSuppressionReason =
  | "not_due"
  | "stale_timer"
  | "write_not_authorized"
  | "state_no_longer_eligible"
  | "duplicate_notification"
  | "silent_policy"
  | "outside_useful_window"
  | "quiet_hours";

export type NotificationDecision =
  | {
      readonly kind: "send";
      readonly plan: NeutralNotificationPlan;
      readonly loop: CoverageLoop;
    }
  | {
      readonly kind: "reschedule";
      readonly dueAt: string;
      readonly reason: "before_useful_window" | "quiet_hours";
    }
  | { readonly kind: "suppress"; readonly reason: NotificationSuppressionReason };

export function createCoverageTimer(input: {
  readonly timerId: string;
  readonly loop: CoverageLoop;
  readonly category: "coverage_opening" | "coverage_reminder";
  readonly dueAt: string;
}): CoverageTimer {
  const loop = CoverageLoopSchema.parse(input.loop);
  const dueAt = Temporal.Instant.from(input.dueAt).toString();
  if (
    compareInstants(dueAt, loop.timing.earliestUsefulAt) < 0 ||
    compareInstants(dueAt, loop.timing.lastResponsibleAt) > 0
  ) {
    throw new CoordinationError("notification_not_useful");
  }
  return CoverageTimerSchema.parse({
    timerId: input.timerId,
    loopId: loop.loopId,
    loopVersion: loop.version,
    planVersion: loop.planVersion,
    attentionCycle: loop.attentionCycle,
    participantEpochId: loop.destination.participantEpochId,
    participantSetDigest: loop.destination.participantSetDigest,
    category: input.category,
    dueAt,
  });
}

/**
 * Produces the one next durable check for an uncovered loop. Before a reminder
 * has been sent in the current risk cycle, the check sits at the midpoint of
 * the remaining useful window. Otherwise it sits at the last responsible
 * instant so runtime processing can deterministically expire the loop.
 */
export function planCoverageFollowUpTimer(input: {
  readonly loop: CoverageLoop;
  readonly now: string;
  readonly remindersAuthorized: boolean;
}): CoverageTimer | null {
  const loop = CoverageLoopSchema.parse(input.loop);
  const now = Temporal.Instant.from(input.now).toString();
  if (!["open", "awaiting_response", "at_risk"].includes(loop.state)) return null;
  const reminderAlreadySent = loop.notificationHistory.some(
    (record) => record.cycle === loop.attentionCycle && record.category === "coverage_reminder",
  );
  const reminderEligible =
    input.remindersAuthorized && loop.notificationMode !== "silent" && !reminderAlreadySent;
  const usefulStart =
    compareInstants(now, loop.timing.earliestUsefulAt) > 0 ? now : loop.timing.earliestUsefulAt;
  const dueAt = reminderEligible
    ? midpointWithinWindow(usefulStart, loop.timing.lastResponsibleAt)
    : loop.timing.lastResponsibleAt;
  return createCoverageTimer({
    timerId: deterministicCoverageTimerId(loop, "coverage_reminder", dueAt),
    loop,
    category: "coverage_reminder",
    dueAt,
  });
}

/** Plans the first proactive opening for a routine-created uncovered loop. */
export function planCoverageOpeningTimer(input: {
  readonly loop: CoverageLoop;
  readonly openingAuthorized: boolean;
}): CoverageTimer | null {
  const loop = CoverageLoopSchema.parse(input.loop);
  if (
    !input.openingAuthorized ||
    !["open", "awaiting_response"].includes(loop.state) ||
    loop.notificationMode === "silent" ||
    loop.notificationHistory.some((record) => record.category === "coverage_opening")
  ) {
    return null;
  }
  const earliest = Temporal.Instant.from(loop.timing.earliestUsefulAt);
  const dueAt = earliest;
  return createCoverageTimer({
    timerId: deterministicCoverageTimerId(loop, "coverage_opening", dueAt.toString()),
    loop,
    category: "coverage_opening",
    dueAt: dueAt.toString(),
  });
}

/** A timer only asks this function to reevaluate current truth and authority. */
export function reevaluateCoverageTimer(input: {
  readonly timer: CoverageTimer;
  readonly loop: CoverageLoop;
  readonly now: string;
  readonly notificationId: string;
  readonly liveDestination: DestinationEpoch;
  readonly canWrite: boolean;
  readonly quietHours: readonly QuietHoursPolicy[];
}): NotificationDecision {
  const timer = CoverageTimerSchema.parse(input.timer);
  const loop = CoverageLoopSchema.parse(input.loop);
  const liveDestination = DestinationEpochSchema.parse(input.liveDestination);
  const now = Temporal.Instant.from(input.now).toString();
  EntityIdSchema.parse(input.notificationId);

  if (compareInstants(now, timer.dueAt) < 0) return { kind: "suppress", reason: "not_due" };
  if (!timerMatchesCurrentState(timer, loop, liveDestination)) {
    return { kind: "suppress", reason: "stale_timer" };
  }
  if (!input.canWrite) return { kind: "suppress", reason: "write_not_authorized" };
  if (!canNotifyOpenCoverage(loop)) {
    return { kind: "suppress", reason: "state_no_longer_eligible" };
  }
  if (loop.notificationMode === "silent") return { kind: "suppress", reason: "silent_policy" };
  if (alreadyNotified(loop, timer.category)) {
    return { kind: "suppress", reason: "duplicate_notification" };
  }

  if (compareInstants(now, loop.timing.earliestUsefulAt) < 0) {
    return { kind: "reschedule", dueAt: loop.timing.earliestUsefulAt, reason: "before_useful_window" };
  }
  const candidateAt = now;
  if (compareInstants(candidateAt, loop.timing.lastResponsibleAt) > 0) {
    return { kind: "suppress", reason: "outside_useful_window" };
  }
  const quiet = resolveQuietHours({
    candidateAt,
    lastResponsibleAt: loop.timing.lastResponsibleAt,
    category: timer.category,
    policies: input.quietHours,
  });
  if (quiet.kind === "suppressed") return { kind: "suppress", reason: "quiet_hours" };
  if (quiet.kind === "delayed") {
    return { kind: "reschedule", dueAt: quiet.sendAt, reason: "quiet_hours" };
  }

  return recordPlannedNotification({
    loop,
    notificationId: input.notificationId,
    category: timer.category,
    sendAt: quiet.sendAt,
    template:
      timer.category === "coverage_opening" ? "coverage_open_question" : "coverage_still_open_question",
    holderPersonId: null,
  });
}

/** Plans a minimal shared status after a real state transition. */
export function planCoverageStatusNotification(input: {
  readonly loop: CoverageLoop;
  readonly status: MinimumSharedCoverageStatus;
  readonly now: string;
  readonly notificationId: string;
  readonly liveDestination: DestinationEpoch;
  readonly canWrite: boolean;
  readonly quietHours: readonly QuietHoursPolicy[];
}): NotificationDecision {
  const loop = CoverageLoopSchema.parse(input.loop);
  const status = MinimumSharedCoverageStatusSchema.parse(input.status);
  const liveDestination = DestinationEpochSchema.parse(input.liveDestination);
  const now = Temporal.Instant.from(input.now).toString();
  EntityIdSchema.parse(input.notificationId);
  const category = statusCategory(status.kind);

  if (
    status.loopVersion !== loop.version ||
    !sameDestination(status.destination, loop.destination) ||
    !sameDestination(liveDestination, loop.destination)
  ) {
    return { kind: "suppress", reason: "stale_timer" };
  }
  if (!input.canWrite) return { kind: "suppress", reason: "write_not_authorized" };
  if (loop.notificationMode === "silent") return { kind: "suppress", reason: "silent_policy" };
  if (alreadyNotified(loop, category)) {
    return { kind: "suppress", reason: "duplicate_notification" };
  }

  const isNonUrgentClosure = category === "coverage_closure" || status.kind === "coverage_recorded";
  const quietDeadline = isNonUrgentClosure
    ? Temporal.Instant.from(now)
        .add({ hours: 7 * 24 })
        .toString()
    : loop.timing.lastResponsibleAt;
  const quiet = resolveQuietHours({
    candidateAt: now,
    lastResponsibleAt: quietDeadline,
    category,
    policies: input.quietHours,
  });
  if (quiet.kind === "suppressed") return { kind: "suppress", reason: "quiet_hours" };
  if (quiet.kind === "delayed") {
    return { kind: "reschedule", dueAt: quiet.sendAt, reason: "quiet_hours" };
  }

  return recordPlannedNotification({
    loop,
    notificationId: input.notificationId,
    category,
    sendAt: quiet.sendAt,
    template: statusTemplate(status.kind),
    holderPersonId: status.holderPersonId,
  });
}

/** The fixed templates contain state and choices, never fault or a private reason. */
export function renderNeutralNotification(planCandidate: NeutralNotificationPlan): string {
  const plan = NeutralNotificationPlanSchema.parse(planCandidate);
  const meaning = sentenceFragment(plan.minimumSharedMeaning);
  switch (plan.template) {
    case "coverage_open_question":
    case "coverage_still_open_question":
      return `${meaning} Coverage is still open. Is it handled, or should we find someone?`;
    case "coverage_recorded":
      return `${meaning} Coverage is recorded.`;
    case "coverage_at_risk":
      return `${meaning} Coverage needs confirmation again. Is it handled, or should we find someone?`;
    case "coverage_cancelled":
      return `${meaning} This coverage request was cancelled.`;
    case "coverage_dismissed":
      return `${meaning} This coverage request is no longer needed.`;
    case "coverage_expired_uncovered":
      return `${meaning} The useful coverage window passed without confirmed coverage.`;
  }
}

function recordPlannedNotification(input: {
  readonly loop: CoverageLoop;
  readonly notificationId: string;
  readonly category: NotificationCategory;
  readonly sendAt: string;
  readonly template: NeutralNotificationPlan["template"];
  readonly holderPersonId: string | null;
}): Extract<NotificationDecision, { kind: "send" }> {
  const nextLoop = CoverageLoopSchema.parse({
    ...input.loop,
    version: input.loop.version + 1,
    notificationHistory: [
      ...input.loop.notificationHistory,
      {
        notificationId: input.notificationId,
        category: input.category,
        cycle: input.loop.attentionCycle,
        sentAt: input.sendAt,
      },
    ],
  });
  const dueAt = nextLoop.timing.deadlineAt ?? nextLoop.timing.eventAt ?? nextLoop.timing.lastResponsibleAt;
  const plan = NeutralNotificationPlanSchema.parse({
    notificationId: input.notificationId,
    loopId: nextLoop.loopId,
    loopVersion: nextLoop.version,
    planVersion: nextLoop.planVersion,
    attentionCycle: nextLoop.attentionCycle,
    category: input.category,
    destination: nextLoop.destination,
    sendAt: input.sendAt,
    template: input.template,
    minimumSharedMeaning: nextLoop.minimumSharedMeaning,
    dueAt,
    holderPersonId: input.holderPersonId,
  });
  return { kind: "send", plan, loop: nextLoop };
}

function timerMatchesCurrentState(
  timer: CoverageTimer,
  loop: CoverageLoop,
  liveDestination: DestinationEpoch,
): boolean {
  return (
    timer.loopId === loop.loopId &&
    timer.loopVersion === loop.version &&
    timer.planVersion === loop.planVersion &&
    timer.attentionCycle === loop.attentionCycle &&
    timer.participantEpochId === loop.destination.participantEpochId &&
    timer.participantSetDigest === loop.destination.participantSetDigest &&
    sameDestination(liveDestination, loop.destination)
  );
}

function canNotifyOpenCoverage(loop: CoverageLoop): boolean {
  return loop.state === "open" || loop.state === "awaiting_response" || loop.state === "at_risk";
}

function alreadyNotified(loop: CoverageLoop, category: NotificationCategory): boolean {
  if (category === "coverage_state_change" || category === "coverage_closure") {
    return loop.notificationHistory.some(
      (record) =>
        record.cycle === loop.attentionCycle &&
        (record.category === "coverage_state_change" || record.category === "coverage_closure"),
    );
  }
  return loop.notificationHistory.some(
    (record) => record.cycle === loop.attentionCycle && record.category === category,
  );
}

function sameDestination(left: DestinationEpoch, right: DestinationEpoch): boolean {
  return (
    left.conversationId === right.conversationId &&
    left.participantEpochId === right.participantEpochId &&
    left.participantSetDigest === right.participantSetDigest &&
    left.audience === right.audience
  );
}

function statusCategory(kind: MinimumSharedCoverageStatus["kind"]): NotificationCategory {
  switch (kind) {
    case "coverage_cancelled":
    case "coverage_dismissed":
    case "coverage_expired_uncovered":
      return "coverage_closure";
    case "coverage_still_open":
    case "coverage_recorded":
    case "coverage_at_risk":
      return "coverage_state_change";
  }
}

function statusTemplate(kind: MinimumSharedCoverageStatus["kind"]): NeutralNotificationPlan["template"] {
  switch (kind) {
    case "coverage_still_open":
      return "coverage_still_open_question";
    case "coverage_recorded":
      return "coverage_recorded";
    case "coverage_at_risk":
      return "coverage_at_risk";
    case "coverage_cancelled":
      return "coverage_cancelled";
    case "coverage_dismissed":
      return "coverage_dismissed";
    case "coverage_expired_uncovered":
      return "coverage_expired_uncovered";
  }
}

function sentenceFragment(value: string): string {
  return value.trim().replace(/[.!?]+$/u, "");
}

function midpointWithinWindow(startCandidate: string, endCandidate: string): string {
  const start = Temporal.Instant.from(startCandidate);
  const end = Temporal.Instant.from(endCandidate);
  if (Temporal.Instant.compare(start, end) >= 0) return end.toString();
  const startMilliseconds = start.epochMilliseconds;
  const endMilliseconds = end.epochMilliseconds;
  const midpoint = startMilliseconds + Math.floor((endMilliseconds - startMilliseconds) / 2);
  if (midpoint <= startMilliseconds || midpoint >= endMilliseconds) return end.toString();
  return Temporal.Instant.fromEpochMilliseconds(midpoint).toString();
}

function deterministicCoverageTimerId(
  loop: CoverageLoop,
  category: "coverage_opening" | "coverage_reminder",
  dueAt: string,
): string {
  const bytes = createHash("sha256")
    .update(`${loop.loopId}:${loop.version}:${loop.planVersion}:${loop.attentionCycle}:${category}:${dueAt}`)
    .digest()
    .subarray(0, 16);
  const byte6 = bytes[6] ?? 0;
  const byte8 = bytes[8] ?? 0;
  bytes[6] = (byte6 & 0x0f) | 0x50;
  bytes[8] = (byte8 & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
