import { createHash } from "node:crypto";
import { z } from "zod";
import type { LinqChatSnapshot, LinqMessageSnapshot } from "./client.js";
import { linqMessageBusinessDedupeKey } from "./conversation.js";
import { type LinqAttachment, type LinqRecoveredMessageEvent, linqInboundEventSchema } from "./schemas.js";
import { normalizeLinqConsentCommand } from "./webhook.js";

const reconciliationInputSchema = z.strictObject({
  integrationId: z.string().min(1).max(200),
  selfHandle: z.string().min(1).max(500),
  recoveredAt: z.iso.datetime({ offset: true }),
  liveNotBefore: z.iso.datetime({ offset: true }),
});

export interface NormalizeRecoveredLinqMessageInput {
  readonly integrationId: string;
  readonly selfHandle: string;
  readonly chat: LinqChatSnapshot;
  readonly message: LinqMessageSnapshot;
  readonly recoveredAt: string;
  readonly liveNotBefore: string;
}

/** Converts a Partner API snapshot into an internal event without inventing webhook provenance. */
export function normalizeRecoveredLinqMessage(
  input: NormalizeRecoveredLinqMessageInput,
): LinqRecoveredMessageEvent | null {
  const metadata = reconciliationInputSchema.parse({
    integrationId: input.integrationId,
    selfHandle: input.selfHandle,
    recoveredAt: input.recoveredAt,
    liveNotBefore: input.liveNotBefore,
  });
  if (input.message.chatId !== input.chat.id || input.message.isFromMe) return null;

  const activeHandles = input.chat.handles.filter((handle) => handle.status.toLowerCase() === "active");
  const selfHandle =
    activeHandles.find((handle) => handle.isSelf === true)?.handle ??
    activeHandles.find((handle) => handlesEqual(handle.handle, metadata.selfHandle))?.handle ??
    metadata.selfHandle;
  const senderHandle = input.message.sender?.handle ?? input.message.senderHandle;
  if (senderHandle === null || handlesEqual(senderHandle, selfHandle)) return null;

  const textParts: string[] = [];
  const attachments: LinqAttachment[] = [];
  for (const part of input.message.parts) {
    switch (part.type) {
      case "text":
        textParts.push(part.value);
        break;
      case "media":
        attachments.push({
          kind: "media",
          partIndex: part.partIndex,
          providerAttachmentId: part.providerAttachmentId,
          url: null,
          mimeType: part.mediaType,
          filename: part.filename || null,
          sizeBytes: part.sizeBytes,
        });
        break;
      case "link":
        attachments.push({
          kind: "link",
          partIndex: part.partIndex,
          providerAttachmentId: null,
          url: part.value,
          mimeType: null,
          filename: null,
          sizeBytes: null,
        });
        break;
      case "imessage_app":
        if (part.fallbackText) textParts.push(part.fallbackText);
        attachments.push({
          kind: "link",
          partIndex: part.partIndex,
          providerAttachmentId: null,
          url: part.url,
          mimeType: null,
          filename: null,
          sizeBytes: null,
        });
        break;
      case "unknown":
        attachments.push({
          kind: "unknown",
          partIndex: part.partIndex,
          providerAttachmentId: null,
          url: null,
          mimeType: null,
          filename: null,
          sizeBytes: null,
        });
        break;
    }
  }
  const text = textParts.join("\n");
  if (text.length === 0 && attachments.length === 0) return null;

  const participantHandles = unique(
    activeHandles.map((handle) => handle.handle).filter((handle) => !handlesEqual(handle, selfHandle)),
  );
  const knownParticipantHandles = unique([selfHandle, ...participantHandles, senderHandle]);
  const recoveryDisposition =
    input.message.reconciledAt !== null ||
    new Date(input.message.occurredAt).getTime() <= new Date(metadata.liveNotBefore).getTime()
      ? "history"
      : "eligible";
  const scope = input.chat.isGroup ? "group" : "direct";

  return linqInboundEventSchema.parse({
    schemaVersion: 1,
    source: "linq",
    transport: "partner_api_reconciliation",
    dedupeKey: transportDedupeKey(metadata.integrationId, input.message),
    businessDedupeKey: linqMessageBusinessDedupeKey(input.message.id),
    occurredAt: input.message.occurredAt,
    integrationId: metadata.integrationId,
    recoveredAt: metadata.recoveredAt,
    recoveryDisposition,
    providerReconciledAt: input.message.reconciledAt,
    eventType: "message.received",
    scope,
    conversation: {
      id: input.chat.id,
      kind: scope,
      ownerHandle: selfHandle,
      knownParticipantHandles,
    },
    sender: {
      id: input.message.sender?.id ?? null,
      handle: senderHandle,
      service: input.message.sender?.service ?? input.message.service,
    },
    message: {
      id: input.message.id,
      text,
      attachments,
      replyTo: input.message.replyTo,
      consentCommand: normalizeLinqConsentCommand(text),
    },
  }) as LinqRecoveredMessageEvent;
}

function transportDedupeKey(integrationId: string, message: LinqMessageSnapshot): string {
  const digest = createHash("sha256")
    .update(integrationId)
    .update("\0")
    .update(message.id)
    .update("\0")
    .update(message.updatedAt)
    .digest("hex");
  return `linq:reconciliation:sha256:${digest}`;
}

function handlesEqual(left: string, right: string): boolean {
  return left.normalize("NFKC").trim().toLowerCase() === right.normalize("NFKC").trim().toLowerCase();
}

function unique(values: readonly string[]): string[] {
  return [...new Set(values.map((value) => value.normalize("NFKC").trim()).filter(Boolean))];
}
