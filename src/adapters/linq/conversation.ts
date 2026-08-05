import { createHash } from "node:crypto";
import type { LinqReactionEvent } from "./schemas.js";

const CLEAR_POSITIVE_REACTIONS = new Set(["heart", "like", "love", "thumbs_up", "thumbsup", "❤️", "👍"]);

export function classifyLinqReaction(reaction: LinqReactionEvent["reaction"]): "acknowledgement" | "other" {
  if (reaction.customEmoji !== null || reaction.stickerAttachmentId !== null) return "other";
  const normalized = reaction.type.normalize("NFKC").trim().toLowerCase().replaceAll(/[- ]/gu, "_");
  return CLEAR_POSITIVE_REACTIONS.has(normalized) ? "acknowledgement" : "other";
}

/** Provider-specific values are reduced to an opaque identity before durable feedback storage. */
export function linqReactionFeedbackRef(event: LinqReactionEvent): string {
  const value = JSON.stringify({
    partnerId: event.partnerId,
    chatId: event.conversation.id,
    targetMessageId: event.reaction.targetMessageId,
    targetPartIndex: event.reaction.targetPartIndex,
    type: event.reaction.type.normalize("NFKC"),
    customEmoji: event.reaction.customEmoji,
    stickerAttachmentId: event.reaction.stickerAttachmentId,
  });
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}
