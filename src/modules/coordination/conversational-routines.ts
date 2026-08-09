import { Temporal } from "@js-temporal/polyfill";
import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { Database } from "../../db/client.js";
import { canonicalDigest, canonicalJson } from "../../shared/canonical-json.js";
import type { SecretBox } from "../../shared/crypto.js";
import { ConflictError, NotFoundError } from "../../shared/errors.js";
import {
  DestinationEpochSchema,
  EntityIdSchema,
  SemanticTimePlanSchema,
  TimeZoneSchema,
  WeeklyRecurrenceSchema,
} from "./contracts.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

const EvidenceCitationSchema = z.strictObject({
  sourceRevisionId: EntityIdSchema,
  support: z.string().trim().min(1).max(500),
});

/** Semantic output accepted from the already-governed coverage workers. */
export const ConversationRoutineProposalSchema = z.strictObject({
  title: z.string().trim().min(1).max(200),
  minimumSharedMeaning: z.string().trim().min(1).max(500),
  semanticTiming: z.string().trim().min(1).max(500),
  timeZone: z.string().trim().min(1).max(100),
  eventAt: z.iso.datetime({ offset: true }).nullable(),
  deadlineAt: z.iso.datetime({ offset: true }).nullable(),
  proposedHolderPersonId: EntityIdSchema,
  evidence: z.array(EvidenceCitationSchema).min(1).max(12),
  uncertainties: z.array(z.string().trim().min(1).max(300)).max(8),
  confidence: z.number().min(0).max(1),
});
export type ConversationRoutineProposal = z.infer<typeof ConversationRoutineProposalSchema>;

const AuthorityVersionSchema = z.number().int().positive();

export const RoutinePatternRevisionTargetSchema = z.strictObject({
  routineId: EntityIdSchema,
  expectedVersion: AuthorityVersionSchema,
  expectedCurrentRevision: AuthorityVersionSchema,
  /** Accepted canonical title of the routine being revised, never current worker prose. */
  canonicalTitle: z.string().trim().min(1).max(200),
});
export type RoutinePatternRevisionTarget = z.infer<typeof RoutinePatternRevisionTargetSchema>;

export const RoutinePatternCandidateContentSchema = z.strictObject({
  schemaVersion: z.literal(1),
  household: z.strictObject({
    id: EntityIdSchema,
    controlEpoch: AuthorityVersionSchema,
  }),
  destination: DestinationEpochSchema.extend({
    authorityVersion: AuthorityVersionSchema,
  }),
  holder: z.strictObject({
    personId: EntityIdSchema,
    personControlEpoch: AuthorityVersionSchema,
    membershipId: EntityIdSchema,
    membershipVersion: AuthorityVersionSchema,
  }),
  title: z.string().trim().min(1).max(200),
  minimumSharedMeaning: z.string().trim().min(1).max(500),
  recurrence: WeeklyRecurrenceSchema,
  timePlan: SemanticTimePlanSchema,
  notificationMode: z.literal("exceptions_only"),
  sourceRevisionIds: z.array(EntityIdSchema).min(1).max(32),
  /** App-selected from canonical routines; workers never provide a mutation target. */
  revisionTarget: RoutinePatternRevisionTargetSchema.nullable(),
  confidence: z.number().min(0).max(1),
});
export type RoutinePatternCandidateContent = z.infer<typeof RoutinePatternCandidateContentSchema>;

export interface BuildRoutinePatternCandidateInput {
  readonly sourceMessage: string;
  readonly proposal: ConversationRoutineProposal;
  readonly household: RoutinePatternCandidateContent["household"];
  readonly destination: RoutinePatternCandidateContent["destination"];
  readonly holder: RoutinePatternCandidateContent["holder"];
  readonly sourceRevisionIds: readonly string[];
  readonly timePlanFallbackTimeZone: string;
  readonly now: Date;
}

export interface StoredRoutinePatternCandidate {
  readonly candidateId: string;
  readonly status: "pending" | "accepted" | "rejected" | "expired" | "revoked";
  readonly content: RoutinePatternCandidateContent;
  readonly proposedAt: Date;
  readonly expiresAt: Date | null;
  readonly reviewedByPersonId: string | null;
  readonly reviewedAt: Date | null;
}

export function isConversationRoutinePatternMessage(message: string): boolean {
  return (
    extractWeeklyRecurrence(message) !== null &&
    extractLocalTime(message) !== null &&
    !hasUnsupportedException(message)
  );
}

/**
 * First-release authority fence: the holder must be one exact current
 * participant in the writable source group. An absent caregiver gets no
 * candidate and no cross-chat prompt.
 */
export function selectCurrentRoutinePatternHolder(
  proposedHolderPersonId: string | null,
  currentParticipantPersonIds: readonly string[],
): string | null {
  if (!proposedHolderPersonId) return null;
  const exactMatches = currentParticipantPersonIds.filter((personId) => personId === proposedHolderPersonId);
  return exactMatches.length === 1 ? proposedHolderPersonId : null;
}

interface CandidateRow {
  readonly id: string;
  readonly status: StoredRoutinePatternCandidate["status"];
  readonly content_ciphertext: Buffer;
  readonly content_key_version: string;
  readonly proposed_at: Date;
  readonly expires_at: Date | null;
  readonly reviewed_by_person_id: string | null;
  readonly reviewed_at: Date | null;
}

/**
 * Turns only an explicit weekly statement into a bounded candidate. A model may
 * suggest meaning and a holder, but it cannot manufacture recurrence: the
 * original group message must itself contain both a recurring weekly cue and a
 * weekday. Incomplete patterns stay conversational and create no durable state.
 */
export function buildRoutinePatternCandidate(
  input: BuildRoutinePatternCandidateInput,
): RoutinePatternCandidateContent | null {
  const proposal = ConversationRoutineProposalSchema.parse(input.proposal);
  if (
    proposal.confidence < 0.8 ||
    proposal.uncertainties.length > 0 ||
    hasUnsupportedException(input.sourceMessage)
  ) {
    return null;
  }

  const recurrence = extractWeeklyRecurrence(input.sourceMessage);
  if (!recurrence) return null;
  const timeZone = safeTimeZone(input.timePlanFallbackTimeZone);
  const localTime = extractLocalTime(input.sourceMessage);
  if (!timeZone) return null;
  if (!localTime) return null;

  const today = Temporal.Instant.from(input.now.toISOString()).toZonedDateTimeISO(timeZone).toPlainDate();
  const startsOn = nextIncludedDate(today, recurrence.weekdays);
  const sourceRevisionIds = unique(input.sourceRevisionIds);
  const citedIds = new Set(proposal.evidence.map((citation) => citation.sourceRevisionId));
  if (
    sourceRevisionIds.length === 0 ||
    sourceRevisionIds.some((sourceRevisionId) => !citedIds.has(sourceRevisionId)) ||
    proposal.proposedHolderPersonId !== input.holder.personId
  ) {
    return null;
  }

  return RoutinePatternCandidateContentSchema.parse({
    schemaVersion: 1,
    household: input.household,
    destination: input.destination,
    holder: input.holder,
    title: proposal.title,
    minimumSharedMeaning: proposal.minimumSharedMeaning,
    recurrence: {
      kind: "weekly",
      weekdays: recurrence.weekdays,
      intervalWeeks: recurrence.intervalWeeks,
      startsOn: startsOn.toString(),
      endsOn: null,
      excludedDates: [],
    },
    timePlan: {
      timeZone,
      event: { kind: "local_clock", time: localTime, dayOffset: 0 },
      deadline: null,
      preparationMinutes: 0,
      travelMinutes: 0,
      earliestUseful: { kind: "relative", anchor: "event", offsetMinutes: -180 },
      lastResponsible: { kind: "relative", anchor: "event", offsetMinutes: -30 },
    },
    notificationMode: "exceptions_only",
    sourceRevisionIds,
    revisionTarget: null,
    confidence: proposal.confidence,
  });
}

/** Exact-reply yes/no may decide a pending prompt; one-time language never grants a standing routine. */
export function deterministicRoutinePatternConfirmation(message: string): "accept" | "reject" | "ambiguous" {
  const normalized = normalize(message).replace(/[.!?]+$/gu, "");
  if (hasOneTimeQualifier(normalized)) return "ambiguous";
  if (
    /^(?:no|nope|nah|don't|do not|not as a routine|not every week|no,? not every week)$/u.test(normalized)
  ) {
    return "reject";
  }
  if (
    /^(?:yes|yep|yeah|sure|ok|okay|correct|please do|remember that|that's right|that is right)$/u.test(
      normalized,
    )
  ) {
    return "accept";
  }
  return "ambiguous";
}

export function isOneTimeRoutinePatternResponse(message: string): boolean {
  return hasOneTimeQualifier(normalize(message));
}

export function routinePatternPrompt(
  candidate: RoutinePatternCandidateContent,
  holderDisplayName: string | null,
): string {
  const holder = holderDisplayName?.trim() || "I need the person who normally handles this";
  return [
    `${holder}, should I save this repeating routine for you?`,
    candidate.revisionTarget
      ? `Action: update your existing routine “${candidate.revisionTarget.canonicalTitle}” instead of adding a second one.`
      : "Action: add this as a new repeating routine.",
    `Routine: ${candidate.minimumSharedMeaning}`,
    `Schedule: ${formatRoutinePatternSchedule(candidate)}`,
    `Starts: ${candidate.recurrence.startsOn}`,
    `Exceptions: ${candidate.recurrence.excludedDates.length > 0 ? candidate.recurrence.excludedDates.join(", ") : "none"}`,
    "Please reply directly to this message with yes or no. I’ll only save it after you confirm.",
  ].join("\n");
}

export function routinePatternAcknowledgment(candidate: RoutinePatternCandidateContent): string {
  return candidate.revisionTarget
    ? `Got it—I’ve updated “${candidate.minimumSharedMeaning}” as your weekly routine. I’ll ask again if the pattern appears to change.`
    : `Got it—I’ll remember “${candidate.minimumSharedMeaning}” as your weekly routine. I’ll ask again if the pattern appears to change.`;
}

export function routinePatternDeclineAcknowledgment(candidate: RoutinePatternCandidateContent): string {
  return `Okay—I won’t save “${candidate.minimumSharedMeaning}” as a repeating routine.`;
}

/** Encrypted, conversation-scoped pending-candidate persistence. */
export class PostgresConversationalRoutines {
  public constructor(
    private readonly transaction: Executor,
    private readonly secretBox: SecretBox,
  ) {}

  public async propose(input: {
    readonly candidateId: string;
    readonly content: RoutinePatternCandidateContent;
    readonly proposedAt: Date;
    readonly expiresAt: Date;
  }): Promise<{ readonly candidate: StoredRoutinePatternCandidate; readonly created: boolean }> {
    const candidateId = EntityIdSchema.parse(input.candidateId);
    const content = RoutinePatternCandidateContentSchema.parse(input.content);
    if (input.expiresAt <= input.proposedAt) throw new ConflictError("Routine candidate already expired");
    const anchorSourceRevisionId = content.sourceRevisionIds[0];
    if (!anchorSourceRevisionId) throw new ConflictError("Routine candidate has no evidence anchor");
    await this
      .transaction`select pg_advisory_xact_lock(hashtextextended(${`routine-pattern:${anchorSourceRevisionId}`}, 0))`;
    await this.transaction`
      update knowledge_candidates set status = 'expired', reviewed_at = coalesce(reviewed_at, ${input.proposedAt})
      where scope_kind = 'conversation' and conversation_id = ${content.destination.conversationId}
        and candidate_kind = 'routine_pattern' and status = 'pending'
        and expires_at is not null and expires_at <= ${input.proposedAt}
    `;

    const existingRows = await this.transaction<CandidateRow[]>`
      select id, status, content_ciphertext, content_key_version, proposed_at, expires_at,
        reviewed_by_person_id, reviewed_at
      from knowledge_candidates
      where scope_kind = 'conversation' and conversation_id = ${content.destination.conversationId}
        and candidate_kind = 'routine_pattern'
        and evidence_refs @> ${this.transaction.json([anchorSourceRevisionId])}
      order by proposed_at desc, id
      limit 1
      for update
    `;
    if (existingRows[0]) {
      return { candidate: this.open(existingRows[0]), created: false };
    }

    const contentDigest = canonicalDigest(content);
    const encrypted = this.secretBox.encrypt(
      canonicalJson(content),
      `routine-pattern-candidate:${candidateId}`,
    );
    await this.transaction`
      insert into knowledge_candidates (
        id, scope_kind, owner_person_id, household_id, conversation_id, candidate_kind,
        content_digest, content_ciphertext, content_key_version, evidence_refs, confidence,
        status, proposed_at, reviewed_by_person_id, reviewed_at, expires_at
      ) values (
        ${candidateId}, 'conversation', null, null, ${content.destination.conversationId},
        'routine_pattern', ${contentDigest}, ${Buffer.from(JSON.stringify(encrypted), "utf8")},
        ${encrypted.kid}, ${this.transaction.json(content.sourceRevisionIds)}, ${content.confidence},
        'pending', ${input.proposedAt}, null, null, ${input.expiresAt}
      )
    `;
    const row: CandidateRow = {
      id: candidateId,
      status: "pending",
      content_ciphertext: Buffer.from(JSON.stringify(encrypted), "utf8"),
      content_key_version: encrypted.kid,
      proposed_at: input.proposedAt,
      expires_at: input.expiresAt,
      reviewed_by_person_id: null,
      reviewed_at: null,
    };
    return { candidate: this.open(row), created: true };
  }

  public async loadForUpdate(
    candidateIdCandidate: string,
    now = new Date(),
  ): Promise<StoredRoutinePatternCandidate> {
    const candidateId = EntityIdSchema.parse(candidateIdCandidate);
    const rows = await this.transaction<CandidateRow[]>`
      select id, status, content_ciphertext, content_key_version, proposed_at, expires_at,
        reviewed_by_person_id, reviewed_at
      from knowledge_candidates
      where id = ${candidateId} and scope_kind = 'conversation' and candidate_kind = 'routine_pattern'
      for update
    `;
    const row = rows[0];
    if (!row) throw new NotFoundError("Routine pattern candidate does not exist");
    if (row.status === "pending" && row.expires_at && row.expires_at <= now) {
      await this.transaction`
        update knowledge_candidates set status = 'expired', reviewed_at = coalesce(reviewed_at, ${now})
        where id = ${candidateId} and status = 'pending'
      `;
      return this.open({ ...row, status: "expired", reviewed_at: row.reviewed_at ?? now });
    }
    return this.open(row);
  }

  public async load(candidateIdCandidate: string): Promise<StoredRoutinePatternCandidate> {
    const candidateId = EntityIdSchema.parse(candidateIdCandidate);
    const rows = await this.transaction<CandidateRow[]>`
      select id, status, content_ciphertext, content_key_version, proposed_at, expires_at,
        reviewed_by_person_id, reviewed_at
      from knowledge_candidates
      where id = ${candidateId} and scope_kind = 'conversation' and candidate_kind = 'routine_pattern'
    `;
    if (!rows[0]) throw new NotFoundError("Routine pattern candidate does not exist");
    return this.open(rows[0]);
  }

  public async review(input: {
    readonly candidateId: string;
    readonly reviewerPersonId: string;
    readonly status: "accepted" | "rejected" | "expired" | "revoked";
    readonly reviewedAt: Date;
  }): Promise<boolean> {
    const updated = await this.transaction<{ readonly id: string }[]>`
      update knowledge_candidates set status = ${input.status},
        reviewed_by_person_id = ${input.reviewerPersonId}, reviewed_at = ${input.reviewedAt}
      where id = ${input.candidateId} and scope_kind = 'conversation'
        and candidate_kind = 'routine_pattern' and status = 'pending'
      returning id
    `;
    return updated.length === 1;
  }

  private open(row: CandidateRow): StoredRoutinePatternCandidate {
    const encrypted = JSON.parse(row.content_ciphertext.toString("utf8")) as { readonly kid?: unknown };
    if (encrypted.kid !== row.content_key_version) {
      throw new ConflictError("Routine candidate key version does not match ciphertext");
    }
    const content = RoutinePatternCandidateContentSchema.parse(
      JSON.parse(this.secretBox.decrypt(encrypted, `routine-pattern-candidate:${row.id}`).toString("utf8")),
    );
    return {
      candidateId: row.id,
      status: row.status,
      content,
      proposedAt: row.proposed_at,
      expiresAt: row.expires_at,
      reviewedByPersonId: row.reviewed_by_person_id,
      reviewedAt: row.reviewed_at,
    };
  }
}

function extractWeeklyRecurrence(
  message: string,
): { readonly weekdays: number[]; readonly intervalWeeks: number } | null {
  const normalized = normalize(message);
  const weekdays = extractWeekdays(normalized);
  if (weekdays.length === 0) return null;
  const intervalWeeks = /\bevery other\s+(?:mon|tues|wednes|thurs|fri|satur|sun)day\b/u.test(normalized)
    ? 2
    : 1;
  const explicit =
    /\b(?:every|each)\s+(?:other\s+)?(?:mon|tues|wednes|thurs|fri|satur|sun)day\b/u.test(normalized) ||
    /\b(?:mondays|tuesdays|wednesdays|thursdays|fridays|saturdays|sundays|weekdays|weekends)\b/u.test(
      normalized,
    ) ||
    (/\b(?:usually|normally|always|weekly|every week)\b/u.test(normalized) && weekdays.length > 0);
  return explicit ? { weekdays, intervalWeeks } : null;
}

function extractWeekdays(message: string): number[] {
  const normalized = normalize(message);
  const values = new Set<number>();
  if (/\bweekdays\b/u.test(normalized)) {
    for (const day of [1, 2, 3, 4, 5]) values.add(day);
  }
  if (/\bweekends\b/u.test(normalized)) {
    for (const day of [6, 7]) values.add(day);
  }
  const names = [
    [1, /\bmondays?\b/u],
    [2, /\btuesdays?\b/u],
    [3, /\bwednesdays?\b/u],
    [4, /\bthursdays?\b/u],
    [5, /\bfridays?\b/u],
    [6, /\bsaturdays?\b/u],
    [7, /\bsundays?\b/u],
  ] as const;
  for (const [day, pattern] of names) if (pattern.test(normalized)) values.add(day);
  return [...values].sort((left, right) => left - right);
}

function extractLocalTime(message: string): string | null {
  const normalized = normalize(message);
  const twelveHour = normalized.match(/\b(?:at|around|by)\s+(\d{1,2})(?::([0-5]\d))?\s*([ap])\.?m\.?\b/u);
  if (twelveHour) {
    let hour = Number(twelveHour[1]);
    const minute = Number(twelveHour[2] ?? "0");
    if (hour < 1 || hour > 12) return null;
    if (twelveHour[3] === "p" && hour !== 12) hour += 12;
    if (twelveHour[3] === "a" && hour === 12) hour = 0;
    return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
  }
  const twentyFourHour = normalized.match(/\b(?:at|around|by)\s+([01]?\d|2[0-3]):([0-5]\d)\b/u);
  return twentyFourHour ? `${String(Number(twentyFourHour[1])).padStart(2, "0")}:${twentyFourHour[2]}` : null;
}

function safeTimeZone(candidate: string): string | null {
  return TimeZoneSchema.safeParse(candidate).success ? candidate : null;
}

function nextIncludedDate(today: Temporal.PlainDate, weekdays: readonly number[]): Temporal.PlainDate {
  for (let offset = 0; offset < 7; offset += 1) {
    const candidate = today.add({ days: offset });
    if (weekdays.includes(candidate.dayOfWeek)) return candidate;
  }
  throw new ConflictError("Weekly routine has no reachable weekday");
}

function hasOneTimeQualifier(normalized: string): boolean {
  return /\b(?:this time|just this|only this|today|tonight|tomorrow|this (?:mon|tues|wednes|thurs|fri|satur|sun)day|next (?:mon|tues|wednes|thurs|fri|satur|sun)day)\b/u.test(
    normalized,
  );
}

function hasUnsupportedException(message: string): boolean {
  return /\b(?:except|unless|skip|not on)\b/u.test(normalize(message));
}

function formatRoutinePatternSchedule(candidate: RoutinePatternCandidateContent): string {
  if (candidate.recurrence.kind !== "weekly" || candidate.timePlan.event?.kind !== "local_clock") {
    throw new ConflictError("Conversational routine candidate is not a weekly local-clock pattern");
  }
  const weekdays = candidate.recurrence.weekdays.map(weekdayName);
  const cadence =
    candidate.recurrence.intervalWeeks === 1
      ? `Every ${joinNatural(weekdays)}`
      : candidate.recurrence.intervalWeeks === 2
        ? `Every other ${joinNatural(weekdays)}`
        : `Every ${candidate.recurrence.intervalWeeks} weeks on ${joinNatural(weekdays)}`;
  return `${cadence} at ${formatLocalClock(candidate.timePlan.event.time)} (${candidate.timePlan.timeZone})`;
}

function weekdayName(day: number): string {
  return (
    ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][day - 1] ??
    `weekday ${day}`
  );
}

function joinNatural(values: readonly string[]): string {
  if (values.length <= 1) return values[0] ?? "";
  if (values.length === 2) return `${values[0]} and ${values[1]}`;
  return `${values.slice(0, -1).join(", ")}, and ${values.at(-1)}`;
}

function formatLocalClock(localTime: string): string {
  const [hourCandidate, minute = "00"] = localTime.split(":");
  const hour = Number(hourCandidate);
  const period = hour >= 12 ? "PM" : "AM";
  const twelveHour = hour % 12 || 12;
  return `${twelveHour}:${minute} ${period}`;
}

function normalize(value: string): string {
  return value.toLocaleLowerCase("en-US").replace(/[’]/gu, "'").replace(/\s+/gu, " ").trim();
}

function unique(values: readonly string[]): string[] {
  return [...new Set(values)];
}
