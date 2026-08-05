import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { closeDatabase, createDatabase, type Database } from "../../src/db/client.js";
import { migrateDatabase } from "../../src/db/migrate.js";
import {
  ConversationMessageConflictError,
  PostgresConversationFeedbackStore,
} from "../../src/infrastructure/conversation-feedback-store.js";

const databaseUrl = process.env.TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)("PostgresConversationFeedbackStore", () => {
  const schema = `conversation_feedback_${randomUUID().replaceAll("-", "").slice(0, 18)}`;
  const householdId = randomUUID();
  const adultA = randomUUID();
  const adultB = randomUUID();
  let database: Database;
  let store: PostgresConversationFeedbackStore;

  beforeAll(async () => {
    database = createDatabase(databaseUrl as string, { max: 8, schema });
    await migrateDatabase(database, schema);
    store = new PostgresConversationFeedbackStore(database);
    await database`
      insert into households (id, name, timezone, status)
      values (${householdId}, 'Feedback family', 'America/Los_Angeles', 'active')
    `;
    await database`
      insert into adults (id, display_name, timezone)
      values (${adultA}, 'Adult A', 'America/Los_Angeles'),
        (${adultB}, 'Adult B', 'America/Los_Angeles')
    `;
  });

  afterAll(async () => {
    if (!database) return;
    await database.unsafe(`drop schema if exists "${schema}" cascade`);
    await closeDatabase(database);
  });

  it("maps only exact app-owned messages in the authorized conversation scope", async () => {
    const sent = {
      messageRef: "florence:message:private-a",
      householdId,
      targetScope: { kind: "personal" as const, adultId: adultA },
      provider: "linq" as const,
      externalChatId: "dm-a",
      providerMessageId: "provider-private-a",
      messageClass: "promotion_request" as const,
      responseContext: { kind: "promotion_decision" as const, promotionId: "promotion-a" },
      appIdempotencyKey: "outbox-private-a",
      sentAt: "2026-08-05T16:00:00.000Z",
    };
    await store.recordSentMessage(sent);
    await expect(store.recordSentMessage(sent)).resolves.toBeUndefined();
    await expect(
      store.resolveReply({
        householdId,
        actorAdultId: adultA,
        channelScope: "personal",
        provider: "linq",
        externalChatId: "dm-a",
        providerMessageId: "provider-private-a",
      }),
    ).resolves.toEqual({
      messageRef: "florence:message:private-a",
      messageClass: "promotion_request",
      responseContext: { kind: "promotion_decision", promotionId: "promotion-a" },
    });

    await expect(
      store.resolveReply({
        householdId,
        actorAdultId: adultB,
        channelScope: "personal",
        provider: "linq",
        externalChatId: "dm-a",
        providerMessageId: "provider-private-a",
      }),
    ).resolves.toBeNull();
    await expect(
      store.resolveReply({
        householdId,
        actorAdultId: adultA,
        channelScope: "personal",
        provider: "linq",
        externalChatId: "dm-a",
        providerMessageId: "ordinary-adult-message",
      }),
    ).resolves.toBeNull();

    await expect(
      store.recordSentMessage({ ...sent, providerMessageId: "conflicting-provider-message" }),
    ).rejects.toBeInstanceOf(ConversationMessageConflictError);
  });

  it("records feedback idempotently with remove-winning ordered reversibility", async () => {
    await store.recordSentMessage({
      messageRef: "florence:message:group-prompt",
      householdId,
      targetScope: { kind: "household" },
      provider: "linq",
      externalChatId: "group-family",
      providerMessageId: "provider-group-prompt",
      messageClass: "approval_request",
      responseContext: { kind: "calendar_approval", actionId: "action-calendar-1" },
      appIdempotencyKey: "outbox-group-prompt",
      sentAt: "2026-08-05T16:00:00.000Z",
    });
    const base = {
      householdId,
      actorAdultId: adultA,
      channelScope: "household" as const,
      provider: "linq" as const,
      externalChatId: "group-family",
      providerMessageId: "provider-group-prompt",
      feedbackRef: `sha256:${"a".repeat(64)}`,
      feedbackKind: "acknowledgement" as const,
      sourceEventId: "linq:partner:event-add",
    };
    await expect(
      store.recordFeedback({
        ...base,
        operation: "add",
        occurredAt: "2026-08-05T16:01:00.000Z",
      }),
    ).resolves.toMatchObject({ status: "recorded", applied: true, active: true });
    await expect(
      store.recordFeedback({
        ...base,
        operation: "add",
        occurredAt: "2026-08-05T16:01:00.000Z",
      }),
    ).resolves.toMatchObject({ status: "recorded", applied: false, active: true });
    await expect(
      store.recordFeedback({
        ...base,
        operation: "remove",
        occurredAt: "2026-08-05T16:01:00.000Z",
        sourceEventId: "linq:partner:event-remove",
      }),
    ).resolves.toMatchObject({ status: "recorded", applied: true, active: false });
    await expect(
      store.recordFeedback({
        ...base,
        operation: "add",
        occurredAt: "2026-08-05T16:01:00.000Z",
        sourceEventId: "linq:partner:event-late-add",
      }),
    ).resolves.toMatchObject({ status: "recorded", applied: false, active: false });
    await expect(
      store.recordFeedback({
        ...base,
        operation: "add",
        occurredAt: "2026-08-05T16:02:00.000Z",
        sourceEventId: "linq:partner:event-new-add",
      }),
    ).resolves.toMatchObject({ status: "recorded", applied: true, active: true });

    const rows = await database<
      Array<{
        feedback_kind: string;
        active: boolean;
        feedback_ref: string;
      }>
    >`
      select feedback_kind, active, feedback_ref from conversation_feedback
      where message_ref = 'florence:message:group-prompt' and actor_adult_id = ${adultA}
    `;
    expect(rows).toEqual([
      {
        feedback_kind: "acknowledgement",
        active: true,
        feedback_ref: `sha256:${"a".repeat(64)}`,
      },
    ]);
  });

  it("silently refuses feedback for unknown and other-adult targets", async () => {
    const common = {
      householdId,
      actorAdultId: adultB,
      channelScope: "personal" as const,
      provider: "linq" as const,
      externalChatId: "dm-a",
      feedbackRef: `sha256:${"b".repeat(64)}`,
      feedbackKind: "other" as const,
      operation: "add" as const,
      occurredAt: "2026-08-05T16:05:00.000Z",
      sourceEventId: "linq:partner:event-unknown",
    };
    await expect(
      store.recordFeedback({ ...common, providerMessageId: "provider-private-a" }),
    ).resolves.toEqual({ status: "unknown_target" });
    await expect(
      store.recordFeedback({ ...common, providerMessageId: "ordinary-adult-message" }),
    ).resolves.toEqual({ status: "unknown_target" });
    await expect(database<{ count: string }[]>`select count(*) from conversation_feedback`).resolves.toEqual([
      { count: "1" },
    ]);
  });
});
