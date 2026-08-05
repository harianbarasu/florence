import { createHash } from "node:crypto";
import { type ApplicationOutboxIntent, ApplicationOutboxIntentSchema } from "../application/index.js";
import { canonicalJson } from "../security/canonical-json.js";
import {
  GMAIL_DISCOVERY_MESSAGE_COUNT_MAX,
  type GmailDiscoveryCompletionPort,
  gmailSyncStateSchema,
  type PublishGmailDiscoveryCompletionInput,
  type ScopedMutationResult,
} from "./google-sync.js";

export interface GmailCompletionDigestStore {
  publishGmailDiscoveryCompletion(
    input: PublishGmailDiscoveryCompletionInput & { intent: ApplicationOutboxIntent },
  ): Promise<ScopedMutationResult>;
}

/** Converts a completed discovery cursor into one content-free, owner-private status intent. */
export class GmailPrivateCompletionDigestAdapter implements GmailDiscoveryCompletionPort {
  public constructor(private readonly store: GmailCompletionDigestStore) {}

  public publish(input: PublishGmailDiscoveryCompletionInput): Promise<ScopedMutationResult> {
    return this.store.publishGmailDiscoveryCompletion({
      ...input,
      intent: gmailPrivateCompletionIntent(input),
    });
  }
}

export function gmailPrivateCompletionIntent(
  input: PublishGmailDiscoveryCompletionInput,
): ApplicationOutboxIntent {
  const state = gmailSyncStateSchema.parse(input.state);
  if (state.revision !== input.expectedRevision + 1 || state.discovery?.status !== "published") {
    throw new Error("Gmail completion intent requires the next published discovery revision");
  }
  const identity = canonicalJson({
    schemaVersion: 1,
    kind: "gmail.discovery.completed",
    householdId: input.householdId,
    adultId: input.adultId,
    connectionId: input.connectionId,
    runId: state.discovery.runId,
  });
  const digest = createHash("sha256").update(identity).digest("hex");
  const intentId = `gmail_discovery_${digest}`;
  return ApplicationOutboxIntentSchema.parse({
    intentId,
    householdId: input.householdId,
    idempotencyKey: `florence:${intentId}`,
    kind: "conversation.send",
    targetScope: { kind: "personal", adultId: input.adultId },
    messageClass: "status",
    body: completionBody(state.requestedDepth, state.discovery.messageCount),
  });
}

function completionBody(depth: "recent_90_days" | "one_year" | "full_history", messageCount: number): string {
  const range =
    depth === "recent_90_days"
      ? "the most recent 90 days"
      : depth === "one_year"
        ? "the past year"
        : "available history";
  const count =
    messageCount === GMAIL_DISCOVERY_MESSAGE_COUNT_MAX
      ? `at least ${formatInteger(messageCount)}`
      : formatInteger(messageCount);
  return `Private Gmail discovery is complete for ${range}: ${count} messages were imported and privately reviewed. This status contains no email content and is private to you.`;
}

function formatInteger(value: number): string {
  return String(value).replace(/\B(?=(\d{3})+(?!\d))/gu, ",");
}
