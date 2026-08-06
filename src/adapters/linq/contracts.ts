export const LINQ_WEBHOOK_VERSION = "2026-02-03" as const;

export const LINQ_WEBHOOK_EVENT_TYPES = [
  "message.received",
  "message.edited",
  "reaction.added",
  "reaction.removed",
  "participant.added",
  "participant.removed",
  "message.sent",
  "message.failed",
] as const;

export type LinqMessagingService = "imessage" | "sms" | "rcs";

export type LinqParticipantStatus = "active" | "left" | "removed";

export interface LinqParticipant {
  providerParticipantId: string;
  address: string;
  service: LinqMessagingService;
  isSelf: boolean;
  status: LinqParticipantStatus;
  joinedAt: string;
  leftAt?: string;
}

export interface LinqChannelRef {
  providerChatId: string;
  kind: "direct" | "group" | "unknown";
}

export type LinqChatHealth = "healthy" | "at_risk" | "critical" | "opted_out";

export interface LinqChatSnapshot {
  providerChatId: string;
  kind: "direct" | "group";
  displayName?: string;
  service?: LinqMessagingService;
  health: LinqChatHealth;
  participants: readonly LinqParticipant[];
  activeParticipantDigest: string;
  createdAt: string;
  updatedAt: string;
  checkedAt: string;
}

export type LinqMessagePart =
  | {
      kind: "text";
      text: string;
    }
  | {
      kind: "attachment";
      providerAttachmentId: string;
      filename?: string;
      mediaType?: string;
      sizeBytes?: number;
    }
  | {
      kind: "link";
      url: string;
    }
  | {
      kind: "unsupported";
      providerPartType: string;
    };

export interface LinqReplyRef {
  providerMessageId: string;
  partIndex: number;
}

interface LinqEventBase {
  provider: "linq";
  providerEventId: string;
  providerTraceId: string;
  providerCreatedAt: string;
  occurredAt: string;
  receivedAt: string;
}

export interface LinqMessageReceivedEvent extends LinqEventBase {
  eventType: "linq.message.received";
  channel: LinqChannelRef;
  message: {
    providerMessageId: string;
    sender: LinqParticipant;
    service: LinqMessagingService;
    parts: readonly LinqMessagePart[];
    sentAt: string;
    replyTo?: LinqReplyRef;
  };
}

export interface LinqMessageEditedEvent extends LinqEventBase {
  eventType: "linq.message.edited";
  channel: LinqChannelRef;
  edit: {
    providerMessageId: string;
    editor: LinqParticipant;
    direction: "inbound" | "outbound";
    partIndex: number;
    text: string;
    editedAt: string;
  };
}

export interface LinqReactionEvent extends LinqEventBase {
  eventType: "linq.reaction.added" | "linq.reaction.removed";
  channel: LinqChannelRef;
  reaction: {
    providerMessageId: string;
    partIndex: number;
    reactor: LinqParticipant;
    kind: "tapback" | "emoji" | "sticker";
    value: string;
    changedAt: string;
  };
}

export interface LinqParticipantChangedEvent extends LinqEventBase {
  eventType: "linq.participant.added" | "linq.participant.removed";
  channel: LinqChannelRef;
  participant: LinqParticipant;
  changedAt: string;
}

export interface LinqOutboundSentEvent extends LinqEventBase {
  eventType: "linq.outbound.sent";
  channel: LinqChannelRef;
  receipt: {
    providerMessageId: string;
    sender: LinqParticipant;
    sentAt: string;
    idempotencyKey?: string;
  };
}

export interface LinqOutboundFailedEvent extends LinqEventBase {
  eventType: "linq.outbound.failed";
  channel: LinqChannelRef;
  receipt: {
    providerMessageId: string;
    failedAt: string;
    errorCode: string;
    reason: string;
  };
}

export interface LinqIgnoredEvent extends LinqEventBase {
  eventType: "linq.ignored";
  providerEventType: string;
  reason: "unsupported_event";
}

export type LinqWebhookEnvelope =
  | LinqMessageReceivedEvent
  | LinqMessageEditedEvent
  | LinqReactionEvent
  | LinqParticipantChangedEvent
  | LinqOutboundSentEvent
  | LinqOutboundFailedEvent
  | LinqIgnoredEvent;

export interface LinqSendMessageRequest {
  providerChatId: string;
  expectedParticipantDigest: string;
  idempotencyKey: string;
  text?: string;
  providerAttachmentIds?: readonly string[];
  replyTo?: LinqReplyRef;
}

export type LinqProviderDeliveryStatus =
  | "pending"
  | "queued"
  | "sent"
  | "delivered"
  | "received"
  | "read"
  | "failed";

export interface LinqSendReceipt {
  providerChatId: string;
  providerMessageId: string;
  idempotencyKey: string;
  status: "accepted";
  providerDeliveryStatus: LinqProviderDeliveryStatus;
  submittedAt: string;
  audienceCheckedAt: string;
  participantDigest: string;
}

/** Minimal delivery-only projection from GET /messages/{messageId}. */
export interface LinqMessageDeliveryReceipt {
  providerChatId: string;
  providerMessageId: string;
  providerDeliveryStatus: LinqProviderDeliveryStatus;
  createdAt: string;
  updatedAt: string;
}

export interface LinqDownloadedAttachment {
  providerAttachmentId: string;
  filename: string;
  declaredMediaType: string;
  responseMediaType?: string;
  sizeBytes: number;
  sha256: string;
  bytes: Uint8Array;
  downloadedAt: string;
}
