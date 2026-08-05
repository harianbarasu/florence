import { createHmac, timingSafeEqual } from "node:crypto";
import { z } from "zod";
import { LINQ_WEBHOOK_VERSION, type LinqConfig } from "./config.js";
import {
  type LinqAttachment,
  type LinqConsentCommand,
  type LinqInboundEvent,
  linqInboundEventSchema,
} from "./schemas.js";

export type LinqWebhookHeaders = Readonly<Record<string, string | readonly string[] | undefined>>;

export class LinqWebhookVerificationError extends Error {
  override readonly name = "LinqWebhookVerificationError";
}

export class LinqWebhookPayloadError extends Error {
  override readonly name = "LinqWebhookPayloadError";
}

const linqEnvelopeSchema = z
  .object({
    api_version: z.literal("v3"),
    webhook_version: z.literal(LINQ_WEBHOOK_VERSION),
    event_type: z.string().min(1),
    event_id: z.string().min(1),
    created_at: z.string().min(1),
    trace_id: z.string().optional(),
    partner_id: z.string().min(1),
    data: z.record(z.string(), z.unknown()),
  })
  .passthrough();

export interface VerifiedLinqWebhook {
  webhookId: string;
  timestamp: Date;
}

export interface ParseLinqWebhookInput {
  rawBody: Uint8Array | string;
  headers: LinqWebhookHeaders;
  config: Pick<LinqConfig, "webhookSecret" | "webhookToleranceMs" | "webhookVersion">;
  now?: Date;
}

export function verifyLinqWebhookSignature(input: ParseLinqWebhookInput): VerifiedLinqWebhook {
  const webhookId = requiredHeader(input.headers, "webhook-id");
  const timestampValue = requiredHeader(input.headers, "webhook-timestamp");
  const signatureValue = requiredHeader(input.headers, "webhook-signature");
  const timestampSeconds = Number(timestampValue);

  if (!Number.isSafeInteger(timestampSeconds) || timestampSeconds < 0) {
    throw new LinqWebhookVerificationError("invalid webhook timestamp");
  }

  const timestamp = new Date(timestampSeconds * 1_000);
  const now = input.now ?? new Date();
  if (
    !Number.isFinite(timestamp.getTime()) ||
    Math.abs(now.getTime() - timestamp.getTime()) > input.config.webhookToleranceMs
  ) {
    throw new LinqWebhookVerificationError("webhook timestamp is outside the replay window");
  }

  const secret = decodeSigningSecret(input.config.webhookSecret);
  const body = toBuffer(input.rawBody);
  const signedContent = Buffer.concat([Buffer.from(`${webhookId}.${timestampValue}.`, "utf8"), body]);
  const expected = createHmac("sha256", secret).update(signedContent).digest();
  const valid = signatureValue.split(/\s+/).some((candidate) => {
    if (!candidate.startsWith("v1,")) {
      return false;
    }
    const supplied = decodeBase64(candidate.slice(3));
    return supplied !== null && supplied.length === expected.length && timingSafeEqual(supplied, expected);
  });

  if (!valid) {
    throw new LinqWebhookVerificationError("invalid webhook signature");
  }

  return { webhookId, timestamp };
}

export function parseVerifiedLinqWebhook(input: ParseLinqWebhookInput): LinqInboundEvent | null {
  const verified = verifyLinqWebhookSignature(input);
  let rawPayload: unknown;
  try {
    rawPayload = JSON.parse(toBuffer(input.rawBody).toString("utf8"));
  } catch {
    throw new LinqWebhookPayloadError("webhook body is not valid JSON");
  }

  const parsed = linqEnvelopeSchema.safeParse(rawPayload);
  if (!parsed.success) {
    throw new LinqWebhookPayloadError("webhook body does not match the pinned Linq v3 schema");
  }
  const envelope = parsed.data;
  if (envelope.webhook_version !== input.config.webhookVersion) {
    throw new LinqWebhookPayloadError("unexpected Linq webhook version");
  }
  if (verified.webhookId !== envelope.event_id) {
    throw new LinqWebhookVerificationError("webhook header and payload event IDs differ");
  }

  let event: LinqInboundEvent | null;
  switch (envelope.event_type) {
    case "message.received":
      event = parseMessageReceived(envelope);
      break;
    case "reaction.added":
    case "reaction.removed":
      event = parseReaction(envelope);
      break;
    default:
      return null;
  }

  const normalized = linqInboundEventSchema.safeParse(event);
  if (!normalized.success) {
    throw new LinqWebhookPayloadError("could not normalize Linq webhook payload");
  }
  return normalized.data;
}

export function normalizeLinqConsentCommand(text: string): LinqConsentCommand {
  const normalized = text.normalize("NFKC").trim().toLowerCase();
  if (["stop", "stopall", "unsubscribe", "cancel", "end", "quit"].includes(normalized)) {
    return "stop";
  }
  if (["start", "resume", "unstop", "yes"].includes(normalized)) {
    return "start";
  }
  return null;
}

function parseMessageReceived(envelope: z.infer<typeof linqEnvelopeSchema>): LinqInboundEvent {
  const data = envelope.data;
  const chat = asRecord(data.chat);
  const sender = asRecord(data.sender_handle);
  const chatId = requiredString(chat.id, "data.chat.id");
  const isGroup = requiredBoolean(chat.is_group, "data.chat.is_group");
  const messageId = requiredString(data.id, "data.id");
  const senderHandle = requiredString(sender.handle, "data.sender_handle.handle");
  const direction = optionalString(data.direction);
  if (direction !== null && direction !== "inbound") {
    throw new LinqWebhookPayloadError("message.received payload is not inbound");
  }

  const parts = Array.isArray(data.parts) ? data.parts : [];
  const textParts: string[] = [];
  const attachments: LinqAttachment[] = [];
  for (const [partIndex, rawPart] of parts.entries()) {
    const part = asRecord(rawPart);
    const type = optionalString(part.type)?.toLowerCase() ?? "unknown";
    if (type === "text") {
      const value = optionalString(part.value);
      if (value !== null) {
        textParts.push(value);
      }
      continue;
    }
    attachments.push(normalizeAttachment(part, partIndex, type));
  }

  const text = textParts.join("\n");
  if (!text && attachments.length === 0) {
    throw new LinqWebhookPayloadError("message.received payload has no supported content");
  }
  const ownerHandle = optionalString(asRecord(chat.owner_handle).handle);
  const knownParticipantHandles = uniqueStrings([ownerHandle, senderHandle]);
  const scope = isGroup ? "group" : "direct";
  const reply = asRecord(data.reply_to);
  const replyMessageId = optionalString(reply.message_id);
  const occurredAt = normalizeTimestamp(data.sent_at ?? envelope.created_at);

  return linqInboundEventSchema.parse({
    schemaVersion: 1,
    source: "linq",
    providerEventId: envelope.event_id,
    dedupeKey: `linq:${envelope.partner_id}:${envelope.event_id}`,
    occurredAt,
    webhookVersion: LINQ_WEBHOOK_VERSION,
    partnerId: envelope.partner_id,
    eventType: "message.received",
    scope,
    conversation: {
      id: chatId,
      kind: scope,
      ownerHandle,
      knownParticipantHandles,
    },
    sender: {
      id: optionalString(sender.id),
      handle: senderHandle,
      service: optionalString(sender.service) ?? optionalString(data.service),
    },
    message: {
      id: messageId,
      text,
      attachments,
      replyTo:
        replyMessageId === null
          ? null
          : {
              messageId: replyMessageId,
              partIndex: optionalNonnegativeInteger(reply.part_index),
            },
      consentCommand: normalizeLinqConsentCommand(text),
    },
  });
}

function parseReaction(envelope: z.infer<typeof linqEnvelopeSchema>): LinqInboundEvent {
  const data = envelope.data;
  const sender = asRecord(data.from_handle);
  const sticker = asRecord(data.sticker);
  const isGroup = typeof data.is_group === "boolean" ? data.is_group : null;
  const scope = isGroup === null ? "unknown" : isGroup ? "group" : "direct";
  const senderHandle = optionalString(sender.handle) ?? requiredString(data.from, "data.from");

  return {
    schemaVersion: 1,
    source: "linq",
    providerEventId: envelope.event_id,
    dedupeKey: `linq:${envelope.partner_id}:${envelope.event_id}`,
    occurredAt: normalizeTimestamp(data.reacted_at ?? envelope.created_at),
    webhookVersion: LINQ_WEBHOOK_VERSION,
    partnerId: envelope.partner_id,
    eventType: envelope.event_type as "reaction.added" | "reaction.removed",
    scope,
    conversation: {
      id: requiredString(data.chat_id, "data.chat_id"),
      kind: scope,
    },
    sender: {
      id: optionalString(sender.id),
      handle: senderHandle,
      service: optionalString(sender.service) ?? optionalString(data.service),
    },
    reaction: {
      operation: envelope.event_type === "reaction.added" ? "add" : "remove",
      targetMessageId: requiredString(data.message_id, "data.message_id"),
      targetPartIndex: optionalNonnegativeInteger(data.part_index),
      type: requiredString(data.reaction_type, "data.reaction_type"),
      customEmoji: optionalString(data.custom_emoji),
      stickerAttachmentId: optionalString(sticker.attachment_id) ?? optionalString(sticker.id),
    },
  };
}

function normalizeAttachment(
  part: Record<string, unknown>,
  partIndex: number,
  rawType: string,
): LinqAttachment {
  const kind = rawType === "media" || rawType === "link" || rawType === "sticker" ? rawType : "unknown";
  const rawUrl = optionalString(part.url) ?? (kind === "link" ? optionalString(part.value) : null);
  return {
    kind,
    partIndex,
    providerAttachmentId: optionalString(part.id) ?? optionalString(part.attachment_id),
    url: rawUrl,
    mimeType: optionalString(part.mime_type),
    filename: optionalString(part.filename),
    sizeBytes: optionalNonnegativeInteger(part.size_bytes),
  };
}

function requiredHeader(headers: LinqWebhookHeaders, name: string): string {
  const value = headerValue(headers, name);
  if (value === null) {
    throw new LinqWebhookVerificationError(`missing ${name} header`);
  }
  return value;
}

function headerValue(headers: LinqWebhookHeaders, name: string): string | null {
  const target = name.toLowerCase();
  for (const [key, rawValue] of Object.entries(headers)) {
    if (key.toLowerCase() !== target || rawValue === undefined) {
      continue;
    }
    const value = Array.isArray(rawValue) ? rawValue[0] : rawValue;
    return value?.trim() || null;
  }
  return null;
}

function decodeSigningSecret(secret: string): Buffer {
  if (!secret.startsWith("whsec_")) {
    throw new LinqWebhookVerificationError("invalid webhook signing secret format");
  }
  const decoded = decodeBase64(secret.slice("whsec_".length));
  if (decoded === null || decoded.length < 16) {
    throw new LinqWebhookVerificationError("invalid webhook signing secret format");
  }
  return decoded;
}

function decodeBase64(value: string): Buffer | null {
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(value)) {
    return null;
  }
  try {
    const decoded = Buffer.from(value, "base64");
    return decoded.length > 0 ? decoded : null;
  } catch {
    return null;
  }
}

function toBuffer(value: Uint8Array | string): Buffer {
  return typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value);
}

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
}

function requiredString(value: unknown, field: string): string {
  const normalized = optionalString(value);
  if (normalized === null) {
    throw new LinqWebhookPayloadError(`missing ${field}`);
  }
  return normalized;
}

function optionalString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function requiredBoolean(value: unknown, field: string): boolean {
  if (typeof value !== "boolean") {
    throw new LinqWebhookPayloadError(`missing ${field}`);
  }
  return value;
}

function optionalNonnegativeInteger(value: unknown): number | null {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function uniqueStrings(values: readonly (string | null)[]): string[] {
  return [...new Set(values.filter((value): value is string => value !== null))];
}

function normalizeTimestamp(value: unknown): string {
  if (typeof value !== "string") {
    throw new LinqWebhookPayloadError("missing event timestamp");
  }
  const millisecondPrecision = value.replace(/(\.\d{3})\d+(?=Z$)/, "$1");
  const timestamp = new Date(millisecondPrecision);
  if (!Number.isFinite(timestamp.getTime())) {
    throw new LinqWebhookPayloadError("invalid event timestamp");
  }
  return timestamp.toISOString();
}
