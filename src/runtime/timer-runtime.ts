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
const INBOUND_RESPONSE_PROCESSING_GRACE_MS = 5 * 60_000;

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
  readonly stewardProviderChatIds: readonly string[];
}

interface HouseholdAuthority {
  readonly id: string;
  readonly controlEpoch: number;
}

type SendContext =
  | {
      readonly sourceCurrent: false;
      readonly canWrite: false;
      readonly liveDestination: DestinationEpoch;
      readonly quietHours: readonly QuietHoursPolicy[];
    }
  | {
      readonly sourceCurrent: true;
      readonly canWrite: false;
      readonly liveDestination: DestinationEpoch;
      readonly quietHours: readonly QuietHoursPolicy[];
      readonly snapshot: ConversationAuthoritySnapshot;
      readonly household: HouseholdAuthority;
    }
  | {
      readonly sourceCurrent: true;
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

interface StewardEscalationRouteRow {
  readonly membership_id: string;
  readonly membership_version: number | string;
  readonly person_id: string;
  readonly person_control_epoch: number | string;
  readonly conversation_id: string | null;
  readonly conversation_authority_version: number | string | null;
  readonly participant_epoch_id: string | null;
  readonly participant_set_digest: string | null;
  readonly identity_id: string | null;
  readonly external_channel_id: string | null;
  readonly latest_participant_digest: string | null;
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
    const privateChats = new Map<string, LinqChatSnapshot | null>(
      await Promise.all(
        target.stewardProviderChatIds.map(async (providerChatId) => {
          try {
            const chat = await this.linq.getChat(providerChatId);
            return [providerChatId, chat] as const;
          } catch {
            return [providerChatId, null] as const;
          }
        }),
      ),
    );
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
      if (timer.coverageTimer.category === "coverage_steward_escalation") {
        if (!sendContext.sourceCurrent) {
          await timers.reschedule(timer.id, nextCoverageRecheckAt(now, loop.timing.lastResponsibleAt));
          return;
        }
        await this.escalateToCurrentStewards(
          transaction,
          timers,
          timer,
          loop,
          sendContext,
          privateChats,
          now,
        );
        return;
      }
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
      const stewards = await loadCurrentStewardRoutes(transaction, loop.householdId);
      return {
        providerChatId: channel.external_channel_id,
        stewardProviderChatIds:
          timer.coverageTimer.category === "coverage_steward_escalation"
            ? [
                ...new Set(
                  stewards.flatMap((steward) =>
                    steward.external_channel_id === null ? [] : [steward.external_channel_id],
                  ),
                ),
              ]
            : [],
      };
    }) as unknown as Promise<TimerPreflightTarget | null>;
  }

  private async authorizeCurrentSend(
    transaction: Transaction,
    timer: ClaimedDurableTimer,
    loop: CoverageLoop,
    liveChat: LinqChatSnapshot,
  ): Promise<SendContext> {
    const fallback: SendContext = {
      sourceCurrent: false,
      canWrite: false,
      liveDestination: loop.destination,
      quietHours: [],
    };
    const binding = await transaction<
      { readonly external_channel_id: string; readonly latest_participant_digest: string | null }[]
    >`
      select external_channel_id, latest_participant_digest from conversation_channels
      where conversation_id = ${loop.destination.conversationId}
        and provider = 'linq' and status = 'active'
      order by bound_at desc limit 2
    `;
    const expectedAudience = liveChat.kind === "group" ? "group" : "private";
    const currentBinding = binding.length === 1 ? binding[0] : undefined;
    if (
      !currentBinding ||
      currentBinding.external_channel_id !== liveChat.providerChatId ||
      currentBinding.latest_participant_digest !== liveChat.activeParticipantDigest ||
      !liveChat.configuredLineActive ||
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
      snapshot.conversationStatus !== "active" ||
      snapshot.conversationKind !== expectedAudience ||
      snapshot.participantEpochId === null ||
      snapshot.participantSetDigest === null ||
      snapshot.participantEpochId !== loop.destination.participantEpochId ||
      snapshot.participantSetDigest !== loop.destination.participantSetDigest
    )
      return { ...fallback, liveDestination };

    const liveIdentityIds = await resolveLiveIdentityIds(transaction, liveChat);
    if (
      liveIdentityIds === null ||
      !sameIds(
        liveIdentityIds,
        snapshot.participants.map((participant) => participant.personIdentityId),
      )
    )
      return { ...fallback, liveDestination };
    const currentSource = {
      sourceCurrent: true as const,
      liveDestination,
      snapshot,
      household: { id: householdRow.id, controlEpoch: Number(householdRow.control_epoch) },
    };
    const controls = await loadPersonControls(transaction, snapshot, loop.timing.timeZone);
    if (controls === null || controls.paused) {
      return {
        ...currentSource,
        canWrite: false,
        quietHours: controls?.quietHours ?? [],
      };
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
      return { ...currentSource, canWrite: false, quietHours: controls.quietHours };
    }
    return {
      ...currentSource,
      canWrite: true,
      quietHours: controls.quietHours,
      providerChatId: liveChat.providerChatId,
      providerParticipantDigest: liveChat.activeParticipantDigest,
      ruleId: rule.ruleId,
    };
  }

  private async escalateToCurrentStewards(
    transaction: Transaction,
    timers: DurableTimers,
    timer: ClaimedDurableTimer,
    loop: CoverageLoop,
    source: Extract<SendContext, { readonly sourceCurrent: true }>,
    privateChats: ReadonlyMap<string, LinqChatSnapshot | null>,
    now: Date,
  ): Promise<void> {
    const stewards = await loadCurrentStewardRoutes(transaction, loop.householdId);
    if (stewards.length === 0) {
      await timers.reschedule(timer.id, new Date(loop.timing.lastResponsibleAt));
      return;
    }
    const text = neutralStewardEscalationText(loop.minimumSharedMeaning);
    let hasUnavailableSteward = false;
    for (const steward of stewards) {
      const existing = await transaction<{ readonly dispatch_state: string }[]>`
        select dispatch_state from coverage_reliance_audiences
        where coverage_loop_id = ${loop.loopId}
          and loop_version = ${loop.version}
          and attention_cycle = ${loop.attentionCycle}
          and person_id = ${steward.person_id}
        for update
      `;
      if (existing[0]?.dispatch_state === "queued") continue;

      const routeUnavailable = async (
        reason: "no_exact_private_dm" | "private_authority_denied" | "provider_audience_changed",
      ) => {
        hasUnavailableSteward = true;
        await persistUnavailableReliance(transaction, {
          loop,
          source,
          steward,
          reason,
          now,
        });
      };
      if (
        steward.conversation_id === null ||
        steward.conversation_authority_version === null ||
        steward.participant_epoch_id === null ||
        steward.participant_set_digest === null ||
        steward.identity_id === null ||
        steward.external_channel_id === null ||
        steward.latest_participant_digest === null
      ) {
        await routeUnavailable("no_exact_private_dm");
        continue;
      }
      const exactSteward = {
        ...steward,
        conversation_id: steward.conversation_id,
        conversation_authority_version: steward.conversation_authority_version,
        participant_epoch_id: steward.participant_epoch_id,
        participant_set_digest: steward.participant_set_digest,
        identity_id: steward.identity_id,
        external_channel_id: steward.external_channel_id,
        latest_participant_digest: steward.latest_participant_digest,
      } as StewardEscalationRouteRow & {
        readonly conversation_id: string;
        readonly conversation_authority_version: number | string;
        readonly participant_epoch_id: string;
        readonly participant_set_digest: string;
        readonly identity_id: string;
        readonly external_channel_id: string;
        readonly latest_participant_digest: string;
      };

      const liveChat = privateChats.get(exactSteward.external_channel_id);
      if (
        liveChat?.kind !== "direct" ||
        !liveChat.configuredLineActive ||
        liveChat.activeParticipantDigest !== exactSteward.latest_participant_digest
      ) {
        await routeUnavailable("provider_audience_changed");
        continue;
      }
      const liveIdentityIds = await resolveLiveIdentityIds(transaction, liveChat);
      if (liveIdentityIds === null || !sameIds(liveIdentityIds, [exactSteward.identity_id])) {
        await routeUnavailable("provider_audience_changed");
        continue;
      }
      const privateSnapshot = await new PostgresConversationAuthority(transaction).snapshot(
        exactSteward.conversation_id,
      );
      if (
        privateSnapshot.conversationStatus !== "active" ||
        privateSnapshot.conversationKind !== "direct" ||
        privateSnapshot.authorityVersion !== Number(exactSteward.conversation_authority_version) ||
        privateSnapshot.participantEpochId !== exactSteward.participant_epoch_id ||
        privateSnapshot.participantSetDigest !== exactSteward.participant_set_digest ||
        privateSnapshot.participants.length !== 1 ||
        privateSnapshot.participants[0]?.personId !== steward.person_id
      ) {
        await routeUnavailable("private_authority_denied");
        continue;
      }
      const authorization = await new PostgresConversationAuthority(transaction).authorizeSend({
        conversationId: exactSteward.conversation_id,
        expectedParticipantEpochId: exactSteward.participant_epoch_id,
        expectedParticipantSetDigest: exactSteward.participant_set_digest,
        liveParticipantIdentityIds: liveIdentityIds,
        sendKind: "transactional",
        operation: "coverage_steward_escalation",
        ruleId: null,
      });
      if (!authorization.allowed) {
        await routeUnavailable("private_authority_denied");
        continue;
      }

      const queued = await new EffectOutbox(transaction, this.secretBox).authorizeAndEnqueue(
        {
          person: {
            id: steward.person_id,
            controlEpoch: Number(steward.person_control_epoch),
          },
          household: source.household,
          conversation: {
            id: exactSteward.conversation_id,
            authorityVersion: authorization.authorityVersion,
          },
          sourceConversation: {
            id: source.snapshot.conversationId,
            authorityVersion: source.snapshot.authorityVersion,
            participantEpochId: loop.destination.participantEpochId,
            participantSetDigest: loop.destination.participantSetDigest,
          },
          participantEpochId: exactSteward.participant_epoch_id,
          expectedParticipantDigest: exactSteward.participant_set_digest,
          coverageLoop: { id: loop.loopId, version: loop.version },
          effectKind: "linq.message",
          idempotencyKey: `coverage-escalation:${loop.loopId}:v${loop.version}:cycle${loop.attentionCycle}:person:${steward.person_id}`,
          data: {
            minimumSharedMeaning: loop.minimumSharedMeaning,
            attentionCycle: loop.attentionCycle,
          },
          policy: {
            operation: "coverage_steward_escalation",
            sendKind: "transactional",
            exactPrivateDm: true,
            explicitAcceptanceRequired: true,
          },
          target: {
            personId: steward.person_id,
            providerChatId: exactSteward.external_channel_id,
            participantEpochId: exactSteward.participant_epoch_id,
          },
          payload: {
            providerChatId: exactSteward.external_channel_id,
            expectedProviderParticipantDigest: liveChat.activeParticipantDigest,
            text,
          },
          reasonCodes: [
            "unresolved_coverage_before_last_responsible",
            "active_parent_steward",
            "exact_private_dm",
            "minimum_shared_meaning_only",
          ],
          authorizationExpiresAt: new Date(loop.timing.lastResponsibleAt),
        },
        now,
      );
      await persistQueuedReliance(transaction, {
        loop,
        source,
        steward: exactSteward,
        authorizationVersion: authorization.authorityVersion,
        providerParticipantDigest: liveChat.activeParticipantDigest,
        outboxId: queued.outboxId,
        now,
      });
    }
    // Reuse the same exact timer as an expiry check. At the boundary,
    // expirePastWindow first grants inbound messages their processing grace;
    // this branch therefore never sends the escalation twice.
    await timers.reschedule(
      timer.id,
      hasUnavailableSteward
        ? nextCoverageRecheckAt(now, loop.timing.lastResponsibleAt)
        : new Date(loop.timing.lastResponsibleAt),
    );
  }
}

async function loadCurrentStewardRoutes(
  transaction: Transaction,
  householdId: string,
): Promise<StewardEscalationRouteRow[]> {
  return transaction<StewardEscalationRouteRow[]>`
    select membership.id as membership_id, membership.version as membership_version,
      person.id as person_id, person.control_epoch as person_control_epoch,
      route.conversation_id, route.conversation_authority_version,
      route.participant_epoch_id, route.participant_set_digest, route.identity_id,
      route.external_channel_id, route.latest_participant_digest
    from household_memberships membership
    join people person on person.id = membership.person_id
      and person.status = 'registered'
    left join lateral (
      select conversation.id as conversation_id,
        conversation.authority_version as conversation_authority_version,
        epoch.id as participant_epoch_id, epoch.participant_set_digest,
        participant.person_identity_id as identity_id,
        channel.external_channel_id, channel.latest_participant_digest
      from conversations conversation
      join conversation_channels channel on channel.conversation_id = conversation.id
        and channel.provider = 'linq' and channel.status = 'active'
        and channel.latest_participant_digest is not null
        and channel.latest_participant_checked_at is not null
      join participant_epochs epoch on epoch.id = conversation.current_epoch_id
        and epoch.ended_at is null
      join epoch_participants participant on participant.participant_epoch_id = epoch.id
        and participant.person_id = membership.person_id
        and participant.registration_status = 'registered'
        and participant.consented_at is not null
      join person_identities identity on identity.id = participant.person_identity_id
        and identity.person_id = participant.person_id and identity.status = 'verified'
      join participant_policies policy on policy.conversation_id = conversation.id
        and policy.person_id = participant.person_id and policy.status = 'active'
        and policy.allow_content_processing and policy.allow_direct_responses
      where conversation.kind = 'direct' and conversation.status = 'active'
        and (conversation.household_id is null or conversation.household_id = membership.household_id)
        and (select count(*) from epoch_participants exact
          where exact.participant_epoch_id = epoch.id) = 1
        and not exists(
          select 1 from channel_suppressions suppression
          where suppression.conversation_id = conversation.id and suppression.active
            and suppression.kind in ('stop', 'pause', 'read_only', 'deletion_fence', 'safety_hold')
        )
      order by channel.latest_participant_checked_at desc,
        conversation.updated_at desc, conversation.id
      limit 1
    ) route on true
    where membership.household_id = ${householdId}
      and membership.role = 'steward' and membership.status = 'active'
    order by membership.joined_at, membership.id
  `;
}

async function persistUnavailableReliance(
  transaction: Transaction,
  input: {
    readonly loop: CoverageLoop;
    readonly source: Extract<SendContext, { readonly sourceCurrent: true }>;
    readonly steward: StewardEscalationRouteRow;
    readonly reason: "no_exact_private_dm" | "private_authority_denied" | "provider_audience_changed";
    readonly now: Date;
  },
): Promise<void> {
  await transaction`
    insert into coverage_reliance_audiences (
      id, coverage_loop_id, loop_version, attention_cycle,
      household_id, household_control_epoch, membership_id, membership_version,
      person_id, person_control_epoch, minimum_shared_meaning_digest,
      source_conversation_id, source_conversation_authority_version,
      source_participant_epoch_id, source_participant_set_digest,
      dispatch_state, unavailable_reason, created_at, updated_at
    ) values (
      ${randomUUID()}, ${input.loop.loopId}, ${input.loop.version}, ${input.loop.attentionCycle},
      ${input.loop.householdId}, ${input.source.household.controlEpoch},
      ${input.steward.membership_id}, ${Number(input.steward.membership_version)},
      ${input.steward.person_id}, ${Number(input.steward.person_control_epoch)},
      ${sha256Hex(input.loop.minimumSharedMeaning)},
      ${input.source.snapshot.conversationId}, ${input.source.snapshot.authorityVersion},
      ${input.loop.destination.participantEpochId},
      ${input.loop.destination.participantSetDigest},
      'unavailable', ${input.reason}, ${input.now}, ${input.now}
    )
    on conflict (coverage_loop_id, loop_version, attention_cycle, person_id)
    do update set
      household_control_epoch = excluded.household_control_epoch,
      membership_id = excluded.membership_id,
      membership_version = excluded.membership_version,
      person_control_epoch = excluded.person_control_epoch,
      minimum_shared_meaning_digest = excluded.minimum_shared_meaning_digest,
      source_conversation_id = excluded.source_conversation_id,
      source_conversation_authority_version = excluded.source_conversation_authority_version,
      source_participant_epoch_id = excluded.source_participant_epoch_id,
      source_participant_set_digest = excluded.source_participant_set_digest,
      dispatch_state = 'unavailable', unavailable_reason = excluded.unavailable_reason,
      target_conversation_id = null, target_conversation_authority_version = null,
      target_participant_epoch_id = null, target_participant_set_digest = null,
      target_provider_chat_id = null, target_provider_participant_digest = null,
      outbox_id = null, updated_at = excluded.updated_at
    where coverage_reliance_audiences.dispatch_state = 'unavailable'
  `;
}

async function persistQueuedReliance(
  transaction: Transaction,
  input: {
    readonly loop: CoverageLoop;
    readonly source: Extract<SendContext, { readonly sourceCurrent: true }>;
    readonly steward: StewardEscalationRouteRow & {
      readonly conversation_id: string;
      readonly participant_epoch_id: string;
      readonly participant_set_digest: string;
      readonly external_channel_id: string;
    };
    readonly authorizationVersion: number;
    readonly providerParticipantDigest: string;
    readonly outboxId: string;
    readonly now: Date;
  },
): Promise<void> {
  await transaction`
    insert into coverage_reliance_audiences (
      id, coverage_loop_id, loop_version, attention_cycle,
      household_id, household_control_epoch, membership_id, membership_version,
      person_id, person_control_epoch, minimum_shared_meaning_digest,
      source_conversation_id, source_conversation_authority_version,
      source_participant_epoch_id, source_participant_set_digest,
      target_conversation_id, target_conversation_authority_version,
      target_participant_epoch_id, target_participant_set_digest,
      target_provider_chat_id, target_provider_participant_digest,
      outbox_id, dispatch_state, created_at, updated_at
    ) values (
      ${randomUUID()}, ${input.loop.loopId}, ${input.loop.version}, ${input.loop.attentionCycle},
      ${input.loop.householdId}, ${input.source.household.controlEpoch},
      ${input.steward.membership_id}, ${Number(input.steward.membership_version)},
      ${input.steward.person_id}, ${Number(input.steward.person_control_epoch)},
      ${sha256Hex(input.loop.minimumSharedMeaning)},
      ${input.source.snapshot.conversationId}, ${input.source.snapshot.authorityVersion},
      ${input.loop.destination.participantEpochId},
      ${input.loop.destination.participantSetDigest},
      ${input.steward.conversation_id}, ${input.authorizationVersion},
      ${input.steward.participant_epoch_id}, ${input.steward.participant_set_digest},
      ${input.steward.external_channel_id}, ${input.providerParticipantDigest},
      ${input.outboxId}, 'queued', ${input.now}, ${input.now}
    )
    on conflict (coverage_loop_id, loop_version, attention_cycle, person_id)
    do update set
      household_control_epoch = excluded.household_control_epoch,
      membership_id = excluded.membership_id,
      membership_version = excluded.membership_version,
      person_control_epoch = excluded.person_control_epoch,
      minimum_shared_meaning_digest = excluded.minimum_shared_meaning_digest,
      source_conversation_id = excluded.source_conversation_id,
      source_conversation_authority_version = excluded.source_conversation_authority_version,
      source_participant_epoch_id = excluded.source_participant_epoch_id,
      source_participant_set_digest = excluded.source_participant_set_digest,
      target_conversation_id = excluded.target_conversation_id,
      target_conversation_authority_version = excluded.target_conversation_authority_version,
      target_participant_epoch_id = excluded.target_participant_epoch_id,
      target_participant_set_digest = excluded.target_participant_set_digest,
      target_provider_chat_id = excluded.target_provider_chat_id,
      target_provider_participant_digest = excluded.target_provider_participant_digest,
      outbox_id = excluded.outbox_id, dispatch_state = 'queued', unavailable_reason = null,
      updated_at = excluded.updated_at
  `;
}

function neutralStewardEscalationText(minimumSharedMeaning: string): string {
  const meaning = minimumSharedMeaning.trim().replace(/[.!?]+$/gu, "") || "This coverage item";
  return `${meaning}. Coverage is still open. Reply if you can take it; I’ll keep it open until someone explicitly confirms.`;
}

function sameIds(left: readonly string[], right: readonly string[]): boolean {
  if (left.length !== right.length) return false;
  const sortedLeft = [...left].sort();
  const sortedRight = [...right].sort();
  return sortedLeft.every((value, index) => value === sortedRight[index]);
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
  const deadline = new Date(loop.timing.lastResponsibleAt);
  if (compareInstants(now.toISOString(), loop.timing.lastResponsibleAt) < 0) return false;
  const graceEndsAt = new Date(deadline.getTime() + INBOUND_RESPONSE_PROCESSING_GRACE_MS);
  if (now < graceEndsAt) {
    await timers.reschedule(timer.id, graceEndsAt);
    return true;
  }
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
