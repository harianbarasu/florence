import { describe, expect, it, vi } from "vitest";
import {
  GmailPrivateCompletionDigestAdapter,
  gmailPrivateCompletionIntent,
} from "../../src/infrastructure/gmail-completion-digest.js";
import type { PublishGmailDiscoveryCompletionInput } from "../../src/infrastructure/google-sync.js";

const HOUSEHOLD_ID = "11111111-1111-4111-8111-111111111111";
const ADULT_ID = "22222222-2222-4222-8222-222222222222";
const CONNECTION_ID = "33333333-3333-4333-8333-333333333333";

function completionInput(): PublishGmailDiscoveryCompletionInput {
  return {
    householdId: HOUSEHOLD_ID,
    adultId: ADULT_ID,
    connectionId: CONNECTION_ID,
    expectedRevision: 8,
    state: {
      schemaVersion: 2,
      revision: 9,
      phase: "live",
      requestedDepth: "full_history",
      boundaryAt: "2027-01-01T08:00:00.000Z",
      scanPageToken: null,
      scanProcessedMessageIds: [],
      history: { cursorId: "200", startId: null, pageToken: null, targetId: null },
      watch: {
        historyId: "100",
        expiresAt: "2027-01-08T08:00:00.000Z",
        subscription: "projects/florence/subscriptions/gmail",
      },
      lastSuccessfulSyncAt: "2027-01-01T08:00:00.000Z",
      discovery: { runId: "gmail-run-private", messageCount: 12_345, status: "published" },
      cancellation: null,
    },
  };
}

describe("GmailPrivateCompletionDigestAdapter", () => {
  it("builds one deterministic, owner-private, content-free status intent", () => {
    const input = completionInput();
    const first = gmailPrivateCompletionIntent(input);
    const second = gmailPrivateCompletionIntent(structuredClone(input));

    expect(second).toEqual(first);
    expect(first).toMatchObject({
      kind: "conversation.send",
      householdId: HOUSEHOLD_ID,
      targetScope: { kind: "personal", adultId: ADULT_ID },
      messageClass: "status",
      body: expect.stringContaining("12,345 messages"),
    });
    if (first.kind !== "conversation.send") throw new Error("Expected a conversation status");
    expect(first.targetScope).not.toEqual({ kind: "household" });
    expect(first.body).not.toMatch(/sender|subject|snippet|attachment|school|pickup/iu);
  });

  it("delegates the cursor and deterministic intent as one publication operation", async () => {
    const publishGmailDiscoveryCompletion = vi.fn(async () => "updated" as const);
    const adapter = new GmailPrivateCompletionDigestAdapter({ publishGmailDiscoveryCompletion });
    const input = completionInput();

    await expect(adapter.publish(input)).resolves.toBe("updated");
    expect(publishGmailDiscoveryCompletion).toHaveBeenCalledOnce();
    expect(publishGmailDiscoveryCompletion).toHaveBeenCalledWith({
      ...input,
      intent: gmailPrivateCompletionIntent(input),
    });
  });
});
