import { z } from "zod";

export const providerServiceSchema = z.enum(["iMessage", "SMS", "RCS"]);

export const providerParticipantSchema = z.object({
  id: z.string().min(1),
  handle: z.string().min(1),
  is_me: z.boolean().nullish(),
  joined_at: z.string().min(1),
  left_at: z.string().nullish(),
  service: providerServiceSchema,
  status: z.enum(["active", "left", "removed"]).nullish(),
});

const providerHealthSchema = z.object({
  status: z.enum(["HEALTHY", "AT_RISK", "CRITICAL", "OPTED_OUT"]),
  updated_at: z.string().min(1),
});

export const providerChatSchema = z.object({
  id: z.string().min(1),
  created_at: z.string().min(1),
  display_name: z.string().nullish(),
  handles: z.array(providerParticipantSchema).min(2),
  health_status: providerHealthSchema,
  is_group: z.boolean(),
  updated_at: z.string().min(1),
  service: providerServiceSchema.nullish(),
});

export const providerChatContextSchema = z.object({
  id: z.string().min(1),
  is_group: z.boolean(),
  owner_handle: providerParticipantSchema,
  health_status: providerHealthSchema.optional(),
});

export const providerReplySchema = z.object({
  message_id: z.string().min(1),
  part_index: z.number().int().nonnegative().nullish(),
});

const providerCurrentMessageSchema = z.object({
  chat: providerChatContextSchema,
  id: z.string().min(1),
  idempotency_key: z.string().nullish(),
  direction: z.enum(["inbound", "outbound"]),
  sender_handle: providerParticipantSchema,
  parts: z.array(z.unknown()).max(100),
  reply_to: providerReplySchema.nullish(),
  sent_at: z.string().min(1),
  service: providerServiceSchema,
});

export const providerMessageReceivedSchema = providerCurrentMessageSchema.extend({
  direction: z.literal("inbound"),
});

export const providerMessageSentSchema = providerCurrentMessageSchema.extend({
  direction: z.literal("outbound"),
});

export const providerMessageEditedSchema = z.object({
  chat: providerChatContextSchema,
  id: z.string().min(1),
  direction: z.enum(["inbound", "outbound"]),
  sender_handle: providerParticipantSchema,
  part: z.object({
    index: z.number().int().nonnegative(),
    text: z.string(),
  }),
  edited_at: z.string().min(1),
});

export const providerReactionSchema = z.object({
  chat_id: z.string().min(1),
  message_id: z.string().min(1),
  part_index: z.number().int().nonnegative(),
  reaction_type: z.enum(["love", "like", "dislike", "laugh", "emphasize", "question", "custom", "sticker"]),
  custom_emoji: z.string().nullish(),
  from_handle: providerParticipantSchema,
  reacted_at: z.string().min(1),
});

export const providerParticipantChangedSchema = z.object({
  chat_id: z.string().min(1),
  participant: providerParticipantSchema,
  added_at: z.string().min(1).optional(),
  removed_at: z.string().min(1).optional(),
});

export const providerMessageFailedSchema = z.object({
  chat_id: z.string().min(1),
  message_id: z.string().min(1),
  code: z.union([z.string(), z.number()]),
  reason: z.string().min(1),
  failed_at: z.string().min(1),
});

export const providerWebhookBaseSchema = z.object({
  api_version: z.literal("v3"),
  webhook_version: z.string().min(1),
  event_type: z.string().min(1),
  event_id: z.string().min(1),
  created_at: z.string().min(1),
  trace_id: z.string().min(1),
  partner_id: z.string().min(1),
  data: z.unknown(),
});

export const providerTextPartSchema = z.object({
  type: z.literal("text"),
  value: z.string(),
});

export const providerMediaPartSchema = z.object({
  type: z.literal("media"),
  id: z.string().min(1),
  filename: z.string().nullish(),
  mime_type: z.string().nullish(),
  size_bytes: z.number().int().nonnegative().nullish(),
});

export const providerLinkPartSchema = z.object({
  type: z.literal("link"),
  value: z.string().min(1),
});

export const providerAttachmentMetadataSchema = z.object({
  id: z.string().min(1),
  content_type: z.string().min(1),
  created_at: z.string().min(1),
  filename: z.string().min(1),
  size_bytes: z.number().int().nonnegative(),
  download_url: z.string().url().nullish(),
});

const providerSendMessageSchema = z.object({
  id: z.string().min(1),
  created_at: z.string().min(1),
  delivery_status: z.enum(["pending", "queued", "sent", "delivered", "received", "read", "failed"]),
});

export const providerSendResponseSchema = z.object({
  chat_id: z.string().min(1),
  message: providerSendMessageSchema,
});

export const providerCreateChatResponseSchema = z.object({
  chat: providerChatSchema.extend({ message: providerSendMessageSchema }),
});

export const providerMessageDeliverySchema = z.object({
  id: z.string().uuid(),
  chat_id: z.string().uuid(),
  created_at: z.string().min(1),
  updated_at: z.string().min(1),
  delivery_status: z.enum(["pending", "queued", "sent", "delivered", "received", "read", "failed"]),
  is_from_me: z.literal(true),
});

export const providerErrorResponseSchema = z.object({
  error: z
    .object({
      status: z.number().int().optional(),
      code: z.union([z.string(), z.number()]).optional(),
      message: z.string().optional(),
    })
    .optional(),
  trace_id: z.string().optional(),
});
