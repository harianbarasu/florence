import type { TransactionSql } from "postgres";
import {
  type ConversationAuthoritySnapshot,
  evaluateConversationMode,
} from "../modules/conversations/index.js";
import {
  type CoverageLoop,
  planCoverageFollowUpTimer,
  planCoverageOpeningTimer,
} from "../modules/coordination/index.js";
import { DurableTimers } from "../modules/work/index.js";

type Transaction = TransactionSql<Record<string, never>>;

/**
 * Keeps exactly one durable check aligned with the loop version committed in
 * this transaction. Reminder eligibility is derived from current authority;
 * when writes are unavailable we still schedule the last-responsible check so
 * the loop can expire without sending anything.
 */
export async function reconcileCoverageTimers(input: {
  transaction: Transaction;
  loop: CoverageLoop;
  snapshot: ConversationAuthoritySnapshot;
  now: Date;
  allowReminder: boolean;
  openingRequired?: boolean;
}): Promise<string | null> {
  const { transaction, loop, snapshot } = input;
  const timers = new DurableTimers(transaction);
  await timers.supersedeCoverageTimers(loop.loopId, loop.version);

  if (
    snapshot.conversationId !== loop.destination.conversationId ||
    snapshot.conversationStatus === "deletion_fenced" ||
    snapshot.conversationStatus === "deleted" ||
    snapshot.participantEpochId !== loop.destination.participantEpochId ||
    snapshot.participantSetDigest !== loop.destination.participantSetDigest
  ) {
    return null;
  }

  const households = await transaction<
    { readonly id: string; readonly control_epoch: number | string; readonly status: string }[]
  >`
    select id, control_epoch, status from households
    where id = ${loop.householdId}
  `;
  const household = households[0];
  if (!household || ["deletion_fenced", "deleted"].includes(household.status)) return null;

  const hasReminderRule = snapshot.rules.some(
    (rule) =>
      rule.active &&
      rule.participantSetDigest === snapshot.participantSetDigest &&
      rule.allowedOperations.includes("coverage_reminder"),
  );
  const remindersAuthorized =
    input.allowReminder &&
    evaluateConversationMode(snapshot) === "trusted_write_enabled" &&
    snapshot.participants.every((participant) => participant.policy?.allowProactiveWrites === true) &&
    hasReminderRule;
  const stewardEscalationStarted = await hasCurrentStewardEscalation(transaction, loop);
  const timer = input.openingRequired
    ? (planCoverageOpeningTimer({
        loop,
        openingAuthorized: remindersAuthorized,
      }) ??
      planCoverageFollowUpTimer({
        loop,
        now: input.now.toISOString(),
        remindersAuthorized: false,
        stewardEscalationStarted,
      }))
    : planCoverageFollowUpTimer({
        loop,
        now: input.now.toISOString(),
        remindersAuthorized,
        stewardEscalationStarted,
      });
  if (!timer) return null;
  return timers.scheduleCoverage({
    timer,
    household: { id: household.id, controlEpoch: Number(household.control_epoch) },
    conversation: { id: snapshot.conversationId, authorityVersion: snapshot.authorityVersion },
  });
}

async function hasCurrentStewardEscalation(transaction: Transaction, loop: CoverageLoop): Promise<boolean> {
  const rows = await transaction<{ readonly present: boolean }[]>`
    select exists(
      select 1 from coverage_reliance_audiences audience
      where audience.coverage_loop_id = ${loop.loopId}
        and audience.loop_version = ${loop.version}
        and audience.attention_cycle = ${loop.attentionCycle}
    ) as present
  `;
  return rows[0]?.present === true;
}
