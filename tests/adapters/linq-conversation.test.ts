import { describe, expect, it } from "vitest";
import {
  classifyLinqReaction,
  type LinqReactionEvent,
  linqReactionFeedbackRef,
} from "../../src/adapters/linq/index.js";

function reaction(overrides: Partial<LinqReactionEvent["reaction"]> = {}): LinqReactionEvent {
  return {
    schemaVersion: 1,
    source: "linq",
    providerEventId: "event-reaction-1",
    dedupeKey: "linq:partner:event-reaction-1",
    occurredAt: "2026-08-05T16:00:00.000Z",
    webhookVersion: "2026-02-03",
    partnerId: "partner",
    eventType: "reaction.added",
    scope: "group",
    conversation: { id: "group-1", kind: "group" },
    sender: { id: "sender-1", handle: "+12025550101", service: "iMessage" },
    reaction: {
      operation: "add",
      targetMessageId: "florence-provider-message-1",
      targetPartIndex: 0,
      type: "like",
      customEmoji: null,
      stickerAttachmentId: null,
      ...overrides,
    },
  };
}

describe("Linq conversation semantics", () => {
  it("reduces only unambiguous built-in positive reactions to acknowledgement", () => {
    expect(classifyLinqReaction(reaction().reaction)).toBe("acknowledgement");
    expect(classifyLinqReaction(reaction({ type: "thumbs-up" }).reaction)).toBe("acknowledgement");
    expect(classifyLinqReaction(reaction({ type: "question" }).reaction)).toBe("other");
    expect(classifyLinqReaction(reaction({ type: "like", customEmoji: "custom-1" }).reaction)).toBe("other");
    expect(classifyLinqReaction(reaction({ type: "love", stickerAttachmentId: "sticker-1" }).reaction)).toBe(
      "other",
    );
  });

  it("uses an opaque stable identity without exposing provider reaction content", () => {
    const first = linqReactionFeedbackRef(reaction());
    const replay = linqReactionFeedbackRef(
      reaction({
        operation: "remove",
      }),
    );
    const different = linqReactionFeedbackRef(reaction({ type: "dislike" }));

    expect(first).toMatch(/^sha256:[a-f0-9]{64}$/u);
    expect(replay).toBe(first);
    expect(different).not.toBe(first);
    expect(first).not.toContain("like");
  });
});
