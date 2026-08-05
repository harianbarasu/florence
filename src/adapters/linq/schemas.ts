import { z } from "zod";

export const linqAttachmentSchema = z
  .object({
    kind: z.enum(["media", "link", "sticker", "unknown"]),
    partIndex: z.number().int().nonnegative(),
    providerAttachmentId: z.string().min(1).nullable(),
    url: z.string().url().nullable(),
    mimeType: z.string().min(1).nullable(),
    filename: z.string().min(1).nullable(),
    sizeBytes: z.number().int().nonnegative().nullable(),
  })
  .strict();

export type LinqAttachment = z.infer<typeof linqAttachmentSchema>;

export const linqReplyReferenceSchema = z
  .object({
    messageId: z.string().min(1),
    partIndex: z.number().int().nonnegative().nullable(),
  })
  .strict();

export type LinqReplyReference = z.infer<typeof linqReplyReferenceSchema>;

export const linqConsentCommandSchema = z.enum(["stop", "start"]).nullable();
export type LinqConsentCommand = z.infer<typeof linqConsentCommandSchema>;

const linqSenderSchema = z
  .object({
    id: z.string().min(1).nullable(),
    handle: z.string().min(1),
    service: z.string().min(1).nullable(),
  })
  .strict();

const linqMessageSchema = z
  .object({
    id: z.string().min(1),
    text: z.string(),
    attachments: z.array(linqAttachmentSchema),
    replyTo: linqReplyReferenceSchema.nullable(),
    consentCommand: linqConsentCommandSchema,
  })
  .strict();

const linqWebhookEventBaseSchema = z.object({
  schemaVersion: z.literal(1),
  source: z.literal("linq"),
  transport: z.literal("webhook"),
  providerEventId: z.string().min(1),
  dedupeKey: z.string().min(1),
  occurredAt: z.string().min(1),
  webhookVersion: z.literal("2026-02-03"),
  partnerId: z.string().min(1),
});

const linqRecoveredEventBaseSchema = z.object({
  schemaVersion: z.literal(1),
  source: z.literal("linq"),
  transport: z.literal("partner_api_reconciliation"),
  dedupeKey: z.string().min(1),
  occurredAt: z.string().min(1),
  integrationId: z.string().min(1),
  recoveredAt: z.string().min(1),
  recoveryDisposition: z.enum(["eligible", "history"]),
  providerReconciledAt: z.string().min(1).nullable(),
});

const linqMessageTransportSchema = z.union([
  linqWebhookEventBaseSchema.extend({ businessDedupeKey: z.string().min(1) }),
  linqRecoveredEventBaseSchema.extend({ businessDedupeKey: z.string().min(1) }),
]);

const linqConversationBaseSchema = z.object({
  id: z.string().min(1),
  ownerHandle: z.string().min(1).nullable(),
  knownParticipantHandles: z.array(z.string().min(1)),
});

const linqDirectMessageCoreSchema = z.object({
  eventType: z.literal("message.received"),
  scope: z.literal("direct"),
  conversation: linqConversationBaseSchema.extend({ kind: z.literal("direct") }).strict(),
  sender: linqSenderSchema,
  message: linqMessageSchema,
});

export const linqDirectMessageEventSchema = z.intersection(
  linqMessageTransportSchema,
  linqDirectMessageCoreSchema,
);

export type LinqDirectMessageEvent = z.infer<typeof linqDirectMessageEventSchema>;

const linqGroupMessageCoreSchema = z.object({
  eventType: z.literal("message.received"),
  scope: z.literal("group"),
  conversation: linqConversationBaseSchema.extend({ kind: z.literal("group") }).strict(),
  sender: linqSenderSchema,
  message: linqMessageSchema,
});

export const linqGroupMessageEventSchema = z.intersection(
  linqMessageTransportSchema,
  linqGroupMessageCoreSchema,
);

export type LinqGroupMessageEvent = z.infer<typeof linqGroupMessageEventSchema>;

export const linqReactionEventSchema = linqWebhookEventBaseSchema
  .extend({
    eventType: z.enum(["reaction.added", "reaction.removed"]),
    scope: z.enum(["direct", "group", "unknown"]),
    conversation: z
      .object({
        id: z.string().min(1),
        kind: z.enum(["direct", "group", "unknown"]),
      })
      .strict(),
    sender: linqSenderSchema,
    reaction: z
      .object({
        operation: z.enum(["add", "remove"]),
        targetMessageId: z.string().min(1),
        targetPartIndex: z.number().int().nonnegative().nullable(),
        type: z.string().min(1),
        customEmoji: z.string().min(1).nullable(),
        stickerAttachmentId: z.string().min(1).nullable(),
      })
      .strict(),
  })
  .strict();

export type LinqReactionEvent = z.infer<typeof linqReactionEventSchema>;

export const linqInboundEventSchema = z.union([
  linqDirectMessageEventSchema,
  linqGroupMessageEventSchema,
  linqReactionEventSchema,
]);

export type LinqInboundEvent = z.infer<typeof linqInboundEventSchema>;
export type LinqMessageEvent = LinqDirectMessageEvent | LinqGroupMessageEvent;
export type LinqWebhookEvent = Extract<LinqInboundEvent, { transport: "webhook" }>;
export type LinqRecoveredMessageEvent = Extract<
  LinqInboundEvent,
  { transport: "partner_api_reconciliation" }
>;
