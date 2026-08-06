import { createHash, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { LinqChatSnapshot, LinqClient } from "../adapters/linq/index.js";
import type { Database } from "../db/client.js";
import {
  type ConversationAuthoritySnapshot,
  PostgresConversationAuthority,
} from "../modules/conversations/index.js";
import {
  type CoverageLoop,
  compareInstants,
  type DestinationEpoch,
  defaultQuietHours,
  PostgresCoordination,
  planCoverageFollowUpTimer,
  type QuietHoursPolicy,
  QuietHoursPolicySchema,
  reevaluateCoverageTimer,
  renderNeutralNotification,
} from "../modules/coordination/index.js";
import { EffectOutbox } from "../modules/effects/index.js";
import {
  type ClaimedDurableTimer,
  type ClaimedJob,
  DurableTimers,
  type TimerProcessPayload,
  TimerProcessPayloadSchema,
} from "../modules/work/index.js";
import type { SecretBox } from "../shared/crypto.js";

type Transaction = TransactionSql<Record<string, never>>;

export type { TimerProcessPayload } from "../modules/work/index.js";
export { TimerProcessPayloadSchema } from "../modules/work/index.js";

export interface TimerJobProcessor {
  process(payload: TimerProcessPayload): Promise<unknown>;
}

/** Keeps timer.process explicit and testable without widening the worker's existing dispatcher. */
export async function dispatchTimerProcessJob(
  job: Pick<ClaimedJob, "kind" | "payload">,
  processor: TimerJobProcessor,
): Promise<boolean> {
  if (job.kind !== "timer.process") return false;
  await processor.process(TimerProcessPayloadSchema.parse(job.payload));
  return true;
}

interface TimerPreflightTarget {
  readonly providerChatId: string;
}

interface HouseholdAuthority {
  readonly id: string;
  readonly controlEpoch: number;
}

type SendContext =
  | {
      readonly canWrite: false;
      readonly liveDestination: DestinationEpoch;
      readonly quietHours: readonly QuietHoursPolicy[];
    }
  | {
      readonly canWrite: true;
      readonly liveDestination: DestinationEpoch;
      readonly quietHours: readonly QuietHoursPolicy[];
      readonly snapshot: ConversationAuthoritySnapshot;
      readonly household: HouseholdAuthority;
      readonly providerChatId: string;
      readonly providerParticipantDigest: string;
      readonly ruleId: string;
    };

interface IdentityRow {
  readonly id: string;
  readonly kind: "phone" | "provider_handle";
  readonly subject_digest: string;
}

interface PersonControlRow {
  readonly id: string;
  readonly status: string;
  readonly timezone: string | null;
  readonly quiet_hours: unknown;
}

/**
 * Reenters current loop, household, conversation, participant, and provider truth
 * before a timer may append notification history and enqueue one authorized effect.
 */
export class TimerRuntime {
  public constructor(
    private readonly database: Database,
    private readonly secretBox: SecretBox,
    private readonly linq: Pick<LinqClient, "getChat">,
  ) {}

  public async process(payloadCandidate: TimerProcessPayload, now = new Date()): Promise<void> {
    const payload = TimerProcessPayloadSchema.parse(payloadCandidate);
    const target = await this.preflight(payload, now);
    if (target === null) return;
    const liveChat = await this.linq.getChat(target.providerChatId);
    await this.database.begin(async (transaction) => {
      const timers = new DurableTimers(transaction);
      const timer = await timers.loadClaimed(payload.id);
      if (!timer || !payloadMatchesTimer(payload, timer)) return;
      if (timer.kind !== "coverage.notification" || timer.coverageTimer === null) {
        await timers.finish(timer.id, "dead");
        return;
      }

      const coordination = new PostgresCoordination(transaction, this.secretBox);
      const loop = await coordination.loadForUpdate(timer.coverageTimer.loopId);
      if (!loop || !coverageTimerMatchesLoop(timer, loop)) {
        await timers.finish(timer.id, "superseded");
        return;
      }
      if (await expirePastWindow(coordination, timers, timer, loop, now)) return;

      const sendContext = await this.authorizeCurrentSend(transaction, timer, loop, liveChat);
      const decision = reevaluateCoverageTimer({
        timer: timer.coverageTimer,
        loop,
        now: now.toISOString(),
        notificationId: timer.id,
        liveDestination: sendContext.liveDestination,
        canWrite: sendContext.canWrite,
        quietHours: sendContext.quietHours,
      });
      if (decision.kind === "reschedule") {
        await timers.reschedule(timer.id, new Date(decision.dueAt));
        return;
      }
      if (decision.kind === "suppress") {
        if (decision.reason === "not_due") {
          await timers.reschedule(timer.id, new Date(timer.dueAt));
          return;
        }
        if (
          ["write_not_authorized", "silent_policy", "quiet_hours"].includes(decision.reason) &&
          compareInstants(now.toISOString(), loop.timing.lastResponsibleAt) < 0
        ) {
          await timers.reschedule(timer.id, new Date(loop.timing.lastResponsibleAt));
          return;
        }
        await timers.finish(timer.id, suppressionStatus(decision.reason));
        return;
      }
      if (!sendContext.canWrite) throw new Error("Authorized notification lost its send context");

      const persistedLoop = await coordination.recordNotification({
        loopId: loop.loopId,
        expectedVersion: loop.version,
        decision,
      });
      const text = renderNeutralNotification(decision.plan);
      await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue(
        {
          participantEpochId: persistedLoop.destination.participantEpochId,
          expectedParticipantDigest: persistedLoop.destination.participantSetDigest,
          coverageLoop: { id: persistedLoop.loopId, version: persistedLoop.version },
          effectKind: "linq.message",
          idempotencyKey: `coverage-timer:${timer.id}:loop-v${persistedLoop.version}`,
          data: {
            coverageLoopId: persistedLoop.loopId,
            loopVersion: persistedLoop.version,
            category: decision.plan.category,
            template: decision.plan.template,
          },
          policy: {
            authorityVersion: sendContext.snapshot.authorityVersion,
            operation:
              timer.coverageTimer.category === "coverage_opening"
                ? "proactive_coverage"
                : "coverage_reminder",
            ruleId: sendContext.ruleId,
            sendKind: "proactive",
          },
          target: {
            providerChatId: sendContext.providerChatId,
            participantEpochId: persistedLoop.destination.participantEpochId,
          },
          payload: {
            providerChatId: sendContext.providerChatId,
            expectedProviderParticipantDigest: sendContext.providerParticipantDigest,
            text,
          },
          reasonCodes: ["current_conversation_authority", "neutral_coverage_timer"],
          // The loop's last responsible moment is the semantic expiry. Keeping this
          // authorization alive until then lets a restarted worker deliver already-
          // committed logical work, while the exact authority fences and live Linq
          // audience check still prevent stale or newly unauthorized sends.
          authorizationExpiresAt: new Date(persistedLoop.timing.lastResponsibleAt),
          conversation: {
            id: persistedLoop.destination.conversationId,
            authorityVersion: sendContext.snapshot.authorityVersion,
          },
          household: sendContext.household,
        },
        now,
      );
      await timers.finish(timer.id, "fired");
      await timers.supersedeCoverageTimers(persistedLoop.loopId, persistedLoop.version);
      const nextTimer = planCoverageFollowUpTimer({
        loop: persistedLoop,
        now: now.toISOString(),
        remindersAuthorized: true,
      });
      if (nextTimer) {
        await timers.scheduleCoverage({
          timer: nextTimer,
          household: sendContext.household,
          conversation: {
            id: persistedLoop.destination.conversationId,
            authorityVersion: sendContext.snapshot.authorityVersion,
          },
        });
      }
    });
  }

  private async preflight(payload: TimerProcessPayload, now: Date): Promise<TimerPreflightTarget | null> {
    return this.database.begin(async (transaction) => {
      const timers = new DurableTimers(transaction);
      const timer = await timers.loadClaimed(payload.id);
      if (!timer || !payloadMatchesTimer(payload, timer)) return null;
      if (timer.kind !== "coverage.notification" || timer.coverageTimer === null) {
        await timers.finish(timer.id, "dead");
        return null;
      }
      const coordination = new PostgresCoordination(transaction, this.secretBox);
      const loop = await coordination.loadForUpdate(timer.coverageTimer.loopId);
      if (!loop || !coverageTimerMatchesLoop(timer, loop)) {
        await timers.finish(timer.id, "superseded");
        return null;
      }
      if (await expirePastWindow(coordination, timers, timer, loop, now)) return null;
      const channels = await transaction<{ readonly external_channel_id: string }[]>`
        select external_channel_id from conversation_channels
        where conversation_id = ${loop.destination.conversationId}
          and provider = 'linq' and status = 'active'
        order by bound_at desc limit 2
      `;
      if (channels.length !== 1) {
        await timers.reschedule(timer.id, nextCoverageRecheckAt(now, loop.timing.lastResponsibleAt));
        return null;
      }
      const channel = channels[0];
      if (!channel) return null;
      return { providerChatId: channel.external_channel_id };
    }) as unknown as Promise<TimerPreflightTarget | null>;
  }

  private async authorizeCurrentSend(
    transaction: Transaction,
    timer: ClaimedDurableTimer,
    loop: CoverageLoop,
    liveChat: LinqChatSnapshot,
  ): Promise<SendContext> {
    const fallback: SendContext = {
      canWrite: false,
      liveDestination: loop.destination,
      quietHours: [],
    };
    const binding = await transaction<{ readonly external_channel_id: string }[]>`
      select external_channel_id from conversation_channels
      where conversation_id = ${loop.destination.conversationId}
        and provider = 'linq' and status = 'active'
      order by bound_at desc limit 2
    `;
    const expectedAudience = liveChat.kind === "group" ? "group" : "private";
    const currentBinding = binding.length === 1 ? binding[0] : undefined;
    if (
      !currentBinding ||
      currentBinding.external_channel_id !== liveChat.providerChatId ||
      loop.destination.audience !== expectedAudience
    )
      return fallback;

    const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
      loop.destination.conversationId,
    );
    const liveDestination: DestinationEpoch = {
      conversationId: loop.destination.conversationId,
      participantEpochId: snapshot.participantEpochId ?? loop.destination.participantEpochId,
      participantSetDigest: snapshot.participantSetDigest ?? loop.destination.participantSetDigest,
      audience: expectedAudience,
    };
    const householdRows = await transaction<
      { readonly id: string; readonly control_epoch: number | string; readonly status: string }[]
    >`
      select id, control_epoch, status from households where id = ${loop.householdId}
    `;
    const householdRow = householdRows[0];
    if (
      !householdRow ||
      !["onboarding", "active"].includes(householdRow.status) ||
      timer.fence.household?.id !== householdRow.id ||
      timer.fence.household.controlEpoch !== Number(householdRow.control_epoch) ||
      timer.fence.conversation?.id !== loop.destination.conversationId ||
      timer.fence.conversation.authorityVersion !== snapshot.authorityVersion ||
      snapshot.participantEpochId === null ||
      snapshot.participantSetDigest === null
    )
      return { ...fallback, liveDestination };

    const liveIdentityIds = await resolveLiveIdentityIds(transaction, liveChat);
    if (liveIdentityIds === null) return { ...fallback, liveDestination };
    const controls = await loadPersonControls(transaction, snapshot, loop.timing.timeZone);
    if (controls === null || controls.paused) {
      return { ...fallback, liveDestination, quietHours: controls?.quietHours ?? [] };
    }
    const operation =
      timer.coverageTimer?.category === "coverage_opening" ? "proactive_coverage" : "coverage_reminder";
    const rule = snapshot.rules.find(
      (candidate) =>
        candidate.active &&
        candidate.participantSetDigest === snapshot.participantSetDigest &&
        candidate.allowedOperations.includes(operation),
    );
    const authorization = await new PostgresConversationAuthority(transaction).authorizeSend({
      conversationId: loop.destination.conversationId,
      expectedParticipantEpochId: loop.destination.participantEpochId,
      expectedParticipantSetDigest: loop.destination.participantSetDigest,
      liveParticipantIdentityIds: liveIdentityIds,
      sendKind: "proactive",
      operation,
      ruleId: rule?.ruleId ?? null,
    });
    if (!authorization.allowed || !rule) {
      return { ...fallback, liveDestination, quietHours: controls.quietHours };
    }
    return {
      canWrite: true,
      liveDestination,
      quietHours: controls.quietHours,
      snapshot,
      household: { id: householdRow.id, controlEpoch: Number(householdRow.control_epoch) },
      providerChatId: liveChat.providerChatId,
      providerParticipantDigest: liveChat.activeParticipantDigest,
      ruleId: rule.ruleId,
    };
  }
}

function nextCoverageRecheckAt(now: Date, lastResponsibleAt: string): Date {
  const deadline = new Date(lastResponsibleAt);
  return new Date(Math.min(now.getTime() + 5 * 60_000, deadline.getTime()));
}

async function expirePastWindow(
  coordination: PostgresCoordination,
  timers: DurableTimers,
  timer: ClaimedDurableTimer,
  loop: CoverageLoop,
  now: Date,
): Promise<boolean> {
  if (compareInstants(now.toISOString(), loop.timing.lastResponsibleAt) < 0) return false;
  let currentVersion = loop.version;
  if (["provisional", "open", "awaiting_response", "at_risk"].includes(loop.state)) {
    const decision = await coordination.transition({
      loopId: loop.loopId,
      command: {
        kind: "expire_uncovered",
        transitionId: randomUUID(),
        expectedVersion: loop.version,
        occurredAt: now.toISOString(),
        evidenceRefs: [`timer:${timer.id}`],
      },
    });
    currentVersion = decision.loop.version;
  }
  await timers.finish(timer.id, "fired");
  await timers.supersedeCoverageTimers(loop.loopId, currentVersion);
  return true;
}

function coverageTimerMatchesLoop(timer: ClaimedDurableTimer, loop: CoverageLoop): boolean {
  const coverage = timer.coverageTimer;
  return (
    coverage !== null &&
    coverage.loopId === loop.loopId &&
    coverage.loopVersion === loop.version &&
    coverage.planVersion === loop.planVersion &&
    coverage.attentionCycle === loop.attentionCycle &&
    coverage.participantEpochId === loop.destination.participantEpochId &&
    coverage.participantSetDigest === loop.destination.participantSetDigest
  );
}

function payloadMatchesTimer(payload: TimerProcessPayload, timer: ClaimedDurableTimer): boolean {
  return (
    payload.id === timer.id &&
    payload.kind === timer.kind &&
    payload.coverageLoopId === timer.coverageLoopId &&
    payload.expectedDomainVersion === timer.expectedDomainVersion &&
    Date.parse(payload.dueAt) === Date.parse(timer.dueAt)
  );
}

function suppressionStatus(
  reason: Exclude<
    Extract<ReturnType<typeof reevaluateCoverageTimer>, { readonly kind: "suppress" }>["reason"],
    "not_due"
  >,
): "fired" | "cancelled" | "superseded" {
  switch (reason) {
    case "stale_timer":
    case "state_no_longer_eligible":
    case "duplicate_notification":
      return "superseded";
    case "write_not_authorized":
    case "silent_policy":
      return "cancelled";
    case "outside_useful_window":
    case "quiet_hours":
      return "fired";
  }
}

async function resolveLiveIdentityIds(
  transaction: Transaction,
  liveChat: LinqChatSnapshot,
): Promise<string[] | null> {
  const humans = liveChat.participants.filter(
    (participant) => participant.status === "active" && !participant.isSelf,
  );
  if (humans.length === 0) return null;
  const exact = humans.map((participant) => {
    const normalized = normalizeHandle(participant.address);
    return {
      kind: /^\+[1-9]\d{6,14}$/u.test(normalized) ? ("phone" as const) : ("provider_handle" as const),
      digest: sha256Hex(normalized),
    };
  });
  const keys = exact.map((entry) => `${entry.kind}:${entry.digest}`);
  if (new Set(keys).size !== keys.length) return null;
  const phoneDigests = exact.filter((entry) => entry.kind === "phone").map((entry) => entry.digest);
  const providerDigests = exact
    .filter((entry) => entry.kind === "provider_handle")
    .map((entry) => entry.digest);
  const rows = await transaction<IdentityRow[]>`
    select identity.id, identity.kind, identity.subject_digest
    from person_identities identity
    join people person on person.id = identity.person_id
    where identity.issuer = 'linq' and identity.status = 'verified'
      and person.status = 'registered'
      and (
        (identity.kind = 'phone' and identity.subject_digest = any(${transaction.array(phoneDigests)}::text[]))
        or (identity.kind = 'provider_handle' and identity.subject_digest = any(${transaction.array(providerDigests)}::text[]))
      )
  `;
  const byKey = new Map(rows.map((row) => [`${row.kind}:${row.subject_digest}`, row.id]));
  const resolved = keys.map((key) => byKey.get(key));
  return resolved.every((identityId): identityId is string => identityId !== undefined) ? resolved : null;
}

async function loadPersonControls(
  transaction: Transaction,
  snapshot: ConversationAuthoritySnapshot,
  fallbackTimeZone: string,
): Promise<{ readonly paused: boolean; readonly quietHours: QuietHoursPolicy[] } | null> {
  const personIds = [...new Set(snapshot.participants.map((participant) => participant.personId))];
  const rows = await transaction<PersonControlRow[]>`
    select id, status, timezone, quiet_hours from people
    where id = any(${transaction.array(personIds)}::uuid[])
  `;
  if (rows.length !== personIds.length || rows.some((row) => row.status !== "registered")) return null;
  const quietHours: QuietHoursPolicy[] = [];
  let paused = false;
  for (const row of rows) {
    const raw = isRecord(row.quiet_hours) ? row.quiet_hours : {};
    if (raw.proactivePaused === true) paused = true;
    if ("proactivePaused" in raw && typeof raw.proactivePaused !== "boolean") return null;
    const defaults = defaultQuietHours(row.id, row.timezone ?? fallbackTimeZone);
    const parsed = QuietHoursPolicySchema.safeParse({
      ...defaults,
      ...(raw.startLocalTime === undefined ? {} : { startLocalTime: raw.startLocalTime }),
      ...(raw.endLocalTime === undefined ? {} : { endLocalTime: raw.endLocalTime }),
      ...(raw.allowLastResponsibleOverrideFor === undefined
        ? {}
        : { allowLastResponsibleOverrideFor: raw.allowLastResponsibleOverrideFor }),
    });
    if (!parsed.success) return null;
    quietHours.push(parsed.data);
  }
  return { paused, quietHours };
}

function normalizeHandle(value: string): string {
  return value
    .trim()
    .replace(/[\s()-]/gu, "")
    .toLowerCase();
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
