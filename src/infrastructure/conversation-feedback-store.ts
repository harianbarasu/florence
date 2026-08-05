import { randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { Database } from "../db/client.js";
import { AdultIdSchema, DurableScopeSchema, HouseholdIdSchema } from "../domain/index.js";
import { canonicalJson } from "../security/canonical-json.js";

const instantSchema = z.iso.datetime({ offset: true });
const stableReferenceSchema = z.string().trim().min(1).max(500);
const messageClassSchema = z.enum([
  "onboarding",
  "private_review",
  "private_interrupt",
  "promotion_request",
  "clarifying_question",
  "status",
  "daily_brief",
  "reminder",
  "missed_window",
  "approval_request",
]);

export const ConversationResponseContextSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("promotion_decision"), promotionId: stableReferenceSchema }),
  z.strictObject({ kind: z.literal("calendar_approval"), actionId: stableReferenceSchema }),
  z.strictObject({
    kind: z.literal("episode_ownership"),
    episodeId: stableReferenceSchema,
    episodeVersion: z.number().int().positive(),
  }),
  z.strictObject({
    kind: z.literal("episode_follow_up"),
    episodeId: stableReferenceSchema,
    episodeVersion: z.number().int().positive(),
  }),
  z.strictObject({ kind: z.literal("sharing_explanation"), sharingRef: stableReferenceSchema }),
]);

export type ConversationResponseContext = z.infer<typeof ConversationResponseContextSchema>;

export const ReferencedConversationMessageSchema = z.strictObject({
  messageRef: stableReferenceSchema,
  messageClass: messageClassSchema,
  responseContext: ConversationResponseContextSchema.optional(),
});

export type ReferencedConversationMessage = z.infer<typeof ReferencedConversationMessageSchema>;

const sentMessageSchema = z.strictObject({
  messageRef: stableReferenceSchema,
  householdId: HouseholdIdSchema,
  targetScope: DurableScopeSchema,
  provider: z.literal("linq"),
  externalChatId: stableReferenceSchema,
  providerMessageId: stableReferenceSchema,
  messageClass: messageClassSchema,
  responseContext: ConversationResponseContextSchema.optional(),
  appIdempotencyKey: stableReferenceSchema,
  sentAt: instantSchema,
});

const targetSchema = z.strictObject({
  householdId: HouseholdIdSchema,
  actorAdultId: AdultIdSchema,
  channelScope: z.enum(["personal", "household"]),
  provider: z.literal("linq"),
  externalChatId: stableReferenceSchema,
  providerMessageId: stableReferenceSchema,
});

const feedbackSchema = targetSchema.extend({
  feedbackRef: z.string().regex(/^sha256:[a-f0-9]{64}$/u),
  feedbackKind: z.enum(["acknowledgement", "other"]),
  operation: z.enum(["add", "remove"]),
  occurredAt: instantSchema,
  sourceEventId: stableReferenceSchema,
});

type ConversationMessageRow = {
  message_ref: string;
  message_class: z.infer<typeof messageClassSchema>;
  response_context: unknown;
};

export type ConversationFeedbackResult =
  | { status: "unknown_target" }
  | {
      status: "recorded";
      target: ReferencedConversationMessage;
      applied: boolean;
      active: boolean;
      feedbackKind: "acknowledgement" | "other";
    };

export class ConversationMessageConflictError extends Error {
  public override readonly name = "ConversationMessageConflictError";
}

/**
 * App-owned identities for provider messages and reversible, observational feedback.
 * Message bodies, provider reaction values, and provider payloads are deliberately absent.
 */
export class PostgresConversationFeedbackStore {
  public constructor(private readonly database: Database) {}

  public async recordSentMessage(rawInput: z.input<typeof sentMessageSchema>): Promise<void> {
    const input = sentMessageSchema.parse(rawInput);
    const responseContext =
      input.responseContext === undefined
        ? null
        : this.database.json(JSON.parse(JSON.stringify(input.responseContext)));
    await this.database.begin(async (transaction) => {
      const inserted = await transaction<{ message_ref: string }[]>`
        insert into conversation_messages (
          message_ref, household_id, target_scope, target_adult_id, provider,
          external_chat_id, provider_message_id, message_class, response_context,
          app_idempotency_key, sent_at
        ) values (
          ${input.messageRef}, ${input.householdId}, ${input.targetScope.kind},
          ${input.targetScope.kind === "personal" ? input.targetScope.adultId : null},
          ${input.provider}, ${input.externalChatId}, ${input.providerMessageId},
          ${input.messageClass}, ${responseContext}, ${input.appIdempotencyKey}, ${input.sentAt}
        )
        on conflict do nothing
        returning message_ref
      `;
      if (inserted.length === 1) return;

      const existing = await transaction<
        Array<{
          message_ref: string;
          household_id: string;
          target_scope: "personal" | "household";
          target_adult_id: string | null;
          provider: "linq";
          external_chat_id: string;
          provider_message_id: string;
          message_class: z.infer<typeof messageClassSchema>;
          response_context: unknown;
          app_idempotency_key: string;
          sent_at: Date;
        }>
      >`
        select message_ref, household_id, target_scope, target_adult_id, provider,
          external_chat_id, provider_message_id, message_class, response_context,
          app_idempotency_key, sent_at
        from conversation_messages
        where message_ref = ${input.messageRef}
          or (provider = ${input.provider} and external_chat_id = ${input.externalChatId}
            and provider_message_id = ${input.providerMessageId})
          or (household_id = ${input.householdId} and app_idempotency_key = ${input.appIdempotencyKey})
        for update
      `;
      if (
        existing.length !== 1 ||
        existing[0]?.message_ref !== input.messageRef ||
        existing[0].household_id !== input.householdId ||
        existing[0].target_scope !== input.targetScope.kind ||
        existing[0].target_adult_id !==
          (input.targetScope.kind === "personal" ? input.targetScope.adultId : null) ||
        existing[0].provider !== input.provider ||
        existing[0].external_chat_id !== input.externalChatId ||
        existing[0].provider_message_id !== input.providerMessageId ||
        existing[0].message_class !== input.messageClass ||
        existing[0].app_idempotency_key !== input.appIdempotencyKey ||
        existing[0].sent_at.getTime() !== Date.parse(input.sentAt) ||
        canonicalJson(existing[0].response_context) !== canonicalJson(input.responseContext ?? null)
      ) {
        throw new ConversationMessageConflictError("Conversation message identity conflict");
      }
    });
  }

  public async resolveReply(
    rawInput: z.input<typeof targetSchema>,
  ): Promise<ReferencedConversationMessage | null> {
    const input = targetSchema.parse(rawInput);
    const rows = await this.findAuthorizedTarget(this.database, input);
    return rows[0] === undefined ? null : parseReferencedMessage(rows[0]);
  }

  public async recordFeedback(rawInput: z.input<typeof feedbackSchema>): Promise<ConversationFeedbackResult> {
    const input = feedbackSchema.parse(rawInput);
    return this.database.begin(async (transaction) => {
      const targets = await this.findAuthorizedTarget(transaction, input, true);
      const targetRow = targets[0];
      if (targetRow === undefined) return { status: "unknown_target" } as const;
      const active = input.operation === "add";
      const rows = await transaction<{ active: boolean }[]>`
        insert into conversation_feedback (
          id, message_ref, household_id, actor_adult_id, feedback_ref,
          feedback_kind, active, occurred_at, source_event_id
        ) values (
          ${randomUUID()}, ${targetRow.message_ref}, ${input.householdId},
          ${input.actorAdultId}, ${input.feedbackRef}, ${input.feedbackKind}, ${active},
          ${input.occurredAt}, ${input.sourceEventId}
        )
        on conflict (message_ref, actor_adult_id, feedback_ref)
        do update set feedback_kind = excluded.feedback_kind, active = excluded.active,
          occurred_at = excluded.occurred_at, source_event_id = excluded.source_event_id,
          updated_at = now()
        where excluded.occurred_at > conversation_feedback.occurred_at
          or (
            excluded.occurred_at = conversation_feedback.occurred_at
            and (
              (not excluded.active and conversation_feedback.active)
              or (
                excluded.active = conversation_feedback.active
                and excluded.source_event_id > conversation_feedback.source_event_id
              )
            )
          )
        returning active
      `;
      const current =
        rows[0] ??
        (
          await transaction<{ active: boolean }[]>`
            select active from conversation_feedback
            where message_ref = ${targetRow.message_ref}
              and actor_adult_id = ${input.actorAdultId}
              and feedback_ref = ${input.feedbackRef}
          `
        )[0];
      if (current === undefined) throw new Error("Conversation feedback state disappeared");
      return {
        status: "recorded",
        target: parseReferencedMessage(targetRow),
        applied: rows.length === 1,
        active: current.active,
        feedbackKind: input.feedbackKind,
      } as const;
    });
  }

  private findAuthorizedTarget(
    query: Database | TransactionSql<Record<string, never>>,
    input: z.infer<typeof targetSchema>,
    lock = false,
  ): Promise<ConversationMessageRow[]> {
    return query<ConversationMessageRow[]>`
      select message_ref, message_class, response_context
      from conversation_messages
      where household_id = ${input.householdId} and provider = ${input.provider}
        and external_chat_id = ${input.externalChatId}
        and provider_message_id = ${input.providerMessageId}
        and (
          (${input.channelScope} = 'household' and target_scope = 'household')
          or (
            ${input.channelScope} = 'personal' and target_scope = 'personal'
            and target_adult_id = ${input.actorAdultId}
          )
        )
      ${lock ? query`for update` : query``}
    `;
  }
}

function parseReferencedMessage(row: ConversationMessageRow): ReferencedConversationMessage {
  return ReferencedConversationMessageSchema.parse({
    messageRef: row.message_ref,
    messageClass: row.message_class,
    ...(row.response_context === null ? {} : { responseContext: row.response_context }),
  });
}

export type ConversationMessageRegistry = Pick<
  PostgresConversationFeedbackStore,
  "recordSentMessage" | "resolveReply" | "recordFeedback"
>;
