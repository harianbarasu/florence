import { createHmac, timingSafeEqual } from "node:crypto";
import type { LinqChannelRef, LinqMessagePart, LinqParticipant, LinqWebhookEnvelope } from "./contracts.js";
import { LINQ_WEBHOOK_VERSION } from "./contracts.js";
import { LinqWebhookError } from "./errors.js";
import {
  providerLinkPartSchema,
  providerMediaPartSchema,
  providerMessageEditedSchema,
  providerMessageFailedSchema,
  providerMessageReceivedSchema,
  providerMessageSentSchema,
  providerParticipantChangedSchema,
  providerReactionSchema,
  providerTextPartSchema,
  providerWebhookBaseSchema,
} from "./schemas.js";

export type LinqWebhookHeaders = Headers | Readonly<Record<string, string | readonly string[] | undefined>>;

export interface UnwrapLinqWebhookInput {
  rawBody: string | Uint8Array;
  headers: LinqWebhookHeaders;
  webhookSecret: string;
  receivedAt?: Date;
  toleranceSeconds?: number;
  maxBodyBytes?: number;
}

interface VerifiedWebhook {
  eventId: string;
  body: Uint8Array;
}

function getHeader(headers: LinqWebhookHeaders, name: string): string | undefined {
  if ("get" in headers && typeof headers.get === "function") {
    return headers.get(name) ?? undefined;
  }

  const found = Object.entries(headers).find(([key]) => key.toLowerCase() === name.toLowerCase());
  const value = found?.[1];
  return Array.isArray(value) ? value.join(" ") : value;
}

function requireHeader(headers: LinqWebhookHeaders, name: string): string {
  const value = getHeader(headers, name)?.trim();
  if (!value) {
    throw new LinqWebhookError("missing_header", `Missing ${name} webhook header`);
  }
  return value;
}

function decodeSigningSecret(secret: string): Uint8Array {
  if (!secret.startsWith("whsec_")) {
    throw new LinqWebhookError("invalid_secret", "The Linq webhook secret has an invalid format");
  }

  const encoded = secret.slice("whsec_".length);
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(encoded)) {
    throw new LinqWebhookError("invalid_secret", "The Linq webhook secret has an invalid format");
  }

  const decoded = Buffer.from(encoded, "base64");
  if (decoded.byteLength < 16) {
    throw new LinqWebhookError("invalid_secret", "The Linq webhook secret is too short");
  }
  return decoded;
}

function verifyWebhook(input: UnwrapLinqWebhookInput): VerifiedWebhook {
  const rawBody = typeof input.rawBody === "string" ? Buffer.from(input.rawBody, "utf8") : input.rawBody;
  const maxBodyBytes = input.maxBodyBytes ?? 1024 * 1024;
  if (rawBody.byteLength > maxBodyBytes) {
    throw new LinqWebhookError("body_too_large", "The Linq webhook body exceeds the configured limit");
  }

  const eventId = requireHeader(input.headers, "webhook-id");
  const timestamp = requireHeader(input.headers, "webhook-timestamp");
  const signatures = requireHeader(input.headers, "webhook-signature");
  if (!/^\d+$/.test(timestamp)) {
    throw new LinqWebhookError("invalid_timestamp", "The Linq webhook timestamp is invalid");
  }

  const timestampSeconds = Number(timestamp);
  const receivedAt = input.receivedAt ?? new Date();
  const toleranceSeconds = input.toleranceSeconds ?? 300;
  if (
    !Number.isSafeInteger(timestampSeconds) ||
    Math.abs(receivedAt.getTime() / 1000 - timestampSeconds) > toleranceSeconds
  ) {
    throw new LinqWebhookError("stale_timestamp", "The Linq webhook timestamp is outside the replay window");
  }

  const prefix = Buffer.from(`${eventId}.${timestamp}.`, "utf8");
  const expected = createHmac("sha256", decodeSigningSecret(input.webhookSecret))
    .update(prefix)
    .update(rawBody)
    .digest();

  const valid = signatures.split(/\s+/).some((candidate) => {
    if (!candidate.startsWith("v1,")) {
      return false;
    }
    try {
      const actual = Buffer.from(candidate.slice(3), "base64");
      return actual.byteLength === expected.byteLength && timingSafeEqual(actual, expected);
    } catch {
      return false;
    }
  });

  if (!valid) {
    throw new LinqWebhookError("invalid_signature", "The Linq webhook signature is invalid");
  }

  return { eventId, body: rawBody };
}

function toIsoTimestamp(value: string, field: string): string {
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) {
    throw new LinqWebhookError("invalid_payload", `The Linq ${field} timestamp is invalid`);
  }
  return new Date(parsed).toISOString();
}

function normalizeService(service: "iMessage" | "SMS" | "RCS") {
  if (service === "iMessage") {
    return "imessage" as const;
  }
  return service.toLowerCase() as "sms" | "rcs";
}

function normalizeParticipant(input: {
  id: string;
  handle: string;
  service: "iMessage" | "SMS" | "RCS";
  is_me?: boolean | null | undefined;
  status?: "active" | "left" | "removed" | null | undefined;
  joined_at: string;
  left_at?: string | null | undefined;
}): LinqParticipant {
  const leftAt = input.left_at ? toIsoTimestamp(input.left_at, "participant left_at") : undefined;
  return {
    providerParticipantId: input.id,
    address: input.handle,
    service: normalizeService(input.service),
    isSelf: input.is_me ?? false,
    status: input.status ?? (leftAt ? "left" : "active"),
    joinedAt: toIsoTimestamp(input.joined_at, "participant joined_at"),
    ...(leftAt ? { leftAt } : {}),
  };
}

function normalizeChannel(chatId: string, kind: LinqChannelRef["kind"]): LinqChannelRef {
  return { providerChatId: chatId, kind };
}

function normalizeWebhookChannel(chatId: string, isGroup: boolean | null | undefined): LinqChannelRef {
  return normalizeChannel(chatId, isGroup === true ? "group" : isGroup === false ? "direct" : "unknown");
}

function normalizePart(input: unknown): LinqMessagePart {
  const text = providerTextPartSchema.safeParse(input);
  if (text.success) {
    return { kind: "text", text: text.data.value };
  }

  const media = providerMediaPartSchema.safeParse(input);
  if (media.success) {
    return {
      kind: "attachment",
      providerAttachmentId: media.data.id,
      ...(media.data.filename ? { filename: media.data.filename } : {}),
      ...(media.data.mime_type ? { mediaType: media.data.mime_type } : {}),
      ...(media.data.size_bytes !== null && media.data.size_bytes !== undefined
        ? { sizeBytes: media.data.size_bytes }
        : {}),
    };
  }

  const link = providerLinkPartSchema.safeParse(input);
  if (link.success) {
    return { kind: "link", url: link.data.value };
  }

  const providerPartType =
    typeof input === "object" && input !== null && "type" in input && typeof input.type === "string"
      ? input.type
      : "unknown";
  return { kind: "unsupported", providerPartType };
}

function parseData<T>(
  schema: { safeParse(input: unknown): { success: true; data: T } | { success: false; error: Error } },
  input: unknown,
): T {
  const result = schema.safeParse(input);
  if (!result.success) {
    throw new LinqWebhookError("invalid_payload", "The Linq webhook payload is invalid", {
      cause: result.error,
    });
  }
  return result.data;
}

export function unwrapLinqWebhook(input: UnwrapLinqWebhookInput): LinqWebhookEnvelope {
  const verified = verifyWebhook(input);
  let decoded: unknown;
  try {
    decoded = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(verified.body));
  } catch (cause) {
    throw new LinqWebhookError("invalid_json", "The verified Linq webhook is not valid JSON", { cause });
  }

  const parsedBase = providerWebhookBaseSchema.safeParse(decoded);
  if (!parsedBase.success) {
    throw new LinqWebhookError("invalid_payload", "The Linq webhook envelope is invalid", {
      cause: parsedBase.error,
    });
  }
  const base = parsedBase.data;
  if (base.event_id !== verified.eventId) {
    throw new LinqWebhookError("event_id_mismatch", "The Linq webhook header and body event IDs differ");
  }
  if (base.webhook_version !== LINQ_WEBHOOK_VERSION) {
    throw new LinqWebhookError(
      "unsupported_version",
      `Unsupported Linq webhook version: ${base.webhook_version}`,
    );
  }

  const receivedAt = (input.receivedAt ?? new Date()).toISOString();
  const providerCreatedAt = toIsoTimestamp(base.created_at, "created_at");
  const common = {
    provider: "linq" as const,
    providerEventId: base.event_id,
    providerTraceId: base.trace_id,
    providerCreatedAt,
    receivedAt,
  };

  switch (base.event_type) {
    case "message.received": {
      const data = parseData(providerMessageReceivedSchema, base.data);
      const sentAt = toIsoTimestamp(data.sent_at, "message sent_at");
      const reconciledAt = data.reconciled_at
        ? toIsoTimestamp(data.reconciled_at, "message reconciled_at")
        : undefined;
      const replyTo = data.reply_to
        ? {
            providerMessageId: data.reply_to.message_id,
            partIndex: data.reply_to.part_index ?? 0,
          }
        : undefined;
      return {
        ...common,
        eventType: "linq.message.received",
        occurredAt: sentAt,
        channel: normalizeWebhookChannel(data.chat.id, data.chat.is_group),
        message: {
          providerMessageId: data.id,
          sender: normalizeParticipant(data.sender_handle),
          service: normalizeService(data.service),
          parts: data.parts.map(normalizePart),
          sentAt,
          ...(reconciledAt ? { reconciledAt } : {}),
          ...(replyTo ? { replyTo } : {}),
        },
      };
    }
    case "message.edited": {
      const data = parseData(providerMessageEditedSchema, base.data);
      const editedAt = toIsoTimestamp(data.edited_at, "message edited_at");
      return {
        ...common,
        eventType: "linq.message.edited",
        occurredAt: editedAt,
        channel: normalizeWebhookChannel(data.chat.id, data.chat.is_group),
        edit: {
          providerMessageId: data.id,
          editor: normalizeParticipant(data.sender_handle),
          direction: data.direction,
          partIndex: data.part.index,
          text: data.part.text,
          editedAt,
        },
      };
    }
    case "reaction.added":
    case "reaction.removed": {
      const data = parseData(providerReactionSchema, base.data);
      const changedAt = toIsoTimestamp(data.reacted_at, "reaction reacted_at");
      const kind =
        data.reaction_type === "custom"
          ? ("emoji" as const)
          : data.reaction_type === "sticker"
            ? ("sticker" as const)
            : ("tapback" as const);
      return {
        ...common,
        eventType: base.event_type === "reaction.added" ? "linq.reaction.added" : "linq.reaction.removed",
        occurredAt: changedAt,
        channel: normalizeChannel(data.chat_id, "unknown"),
        reaction: {
          providerMessageId: data.message_id,
          partIndex: data.part_index,
          reactor: normalizeParticipant(data.from_handle),
          kind,
          value: data.reaction_type === "custom" ? (data.custom_emoji ?? "custom") : data.reaction_type,
          changedAt,
        },
      };
    }
    case "participant.added":
    case "participant.removed": {
      const data = parseData(providerParticipantChangedSchema, base.data);
      const timestamp = base.event_type === "participant.added" ? data.added_at : data.removed_at;
      if (!timestamp) {
        throw new LinqWebhookError("invalid_payload", "The Linq participant event has no change time");
      }
      const changedAt = toIsoTimestamp(timestamp, "participant changed_at");
      return {
        ...common,
        eventType:
          base.event_type === "participant.added" ? "linq.participant.added" : "linq.participant.removed",
        occurredAt: changedAt,
        channel: normalizeChannel(data.chat_id, "group"),
        participant: normalizeParticipant(data.participant),
        changedAt,
      };
    }
    case "message.sent": {
      const data = parseData(providerMessageSentSchema, base.data);
      const sentAt = toIsoTimestamp(data.sent_at, "message sent_at");
      return {
        ...common,
        eventType: "linq.outbound.sent",
        occurredAt: sentAt,
        channel: normalizeWebhookChannel(data.chat.id, data.chat.is_group),
        receipt: {
          providerMessageId: data.id,
          sender: normalizeParticipant(data.sender_handle),
          sentAt,
          ...(data.idempotency_key ? { idempotencyKey: data.idempotency_key } : {}),
        },
      };
    }
    case "message.failed": {
      const data = parseData(providerMessageFailedSchema, base.data);
      const failedAt = toIsoTimestamp(data.failed_at, "message failed_at");
      return {
        ...common,
        eventType: "linq.outbound.failed",
        occurredAt: failedAt,
        channel: normalizeChannel(data.chat_id, "unknown"),
        receipt: {
          providerMessageId: data.message_id,
          failedAt,
          errorCode: String(data.code),
          reason: data.reason,
        },
      };
    }
    default:
      return {
        ...common,
        eventType: "linq.ignored",
        providerEventType: base.event_type,
        reason: "unsupported_event",
        occurredAt: providerCreatedAt,
      };
  }
}
