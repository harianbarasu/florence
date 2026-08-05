import { createHash } from "node:crypto";
import {
  type ApplicationOutboxIntent,
  ApplicationOutboxIntentSchema,
  type ConversationResponseContext,
  type FlorenceApplication,
  type HouseholdApplicationSnapshot,
} from "../application/index.js";
import {
  PrivateControlCatalog,
  type PrivateKnowledgeItem,
  type PrivateSharingChoiceItem,
  type PrivateSharingRuleItem,
} from "./private-control-catalog.js";
import type { PrivateCommandHandler } from "./provider-processor.js";

export interface PrivateControlSnapshotReader {
  load(householdId: string): Promise<HouseholdApplicationSnapshot | null>;
}

export interface PrivateControlOutbox {
  enqueueApplicationIntent(intent: ApplicationOutboxIntent): Promise<{ rowId: string }>;
}

/** Mutations enter the application/domain single-writer and atomically queue their own response. */
export interface PrivateControlMutator {
  revokeMemory(input: {
    householdId: string;
    adultId: string;
    channelId: string;
    memoryId: string;
    idempotencyKey: string;
    occurredAt: string;
  }): Promise<void>;

  revokeSharingPolicy(input: {
    householdId: string;
    adultId: string;
    channelId: string;
    policyId: string;
    expectedPolicyVersion: number;
    idempotencyKey: string;
    occurredAt: string;
  }): Promise<void>;
}

/** Maps an exact provider reply to the app-owned sharing explanation control ID. */
export interface PrivateSharingReferenceResolver {
  resolveSharingControlId(input: {
    householdId: string;
    adultId: string;
    providerMessageId: string;
  }): Promise<string | null>;
}

export interface PrivateControlCommandServiceOptions {
  readonly snapshots: PrivateControlSnapshotReader;
  readonly outbox: PrivateControlOutbox;
  readonly mutator: PrivateControlMutator;
  readonly sharingReferences?: PrivateSharingReferenceResolver;
  readonly catalog?: PrivateControlCatalog;
}

export class ApplicationPrivateControlMutator implements PrivateControlMutator {
  public constructor(private readonly application: FlorenceApplication) {}

  public async revokeMemory(input: {
    householdId: string;
    adultId: string;
    channelId: string;
    memoryId: string;
    idempotencyKey: string;
    occurredAt: string;
  }): Promise<void> {
    await this.application.process({
      kind: "private_control",
      householdId: input.householdId,
      idempotencyKey: input.idempotencyKey,
      occurredAt: input.occurredAt,
      channel: { channelId: input.channelId, scope: "personal", adultId: input.adultId },
      requesterAdultId: input.adultId,
      action: { kind: "revoke_memory", memoryId: input.memoryId },
    });
  }

  public async revokeSharingPolicy(input: {
    householdId: string;
    adultId: string;
    channelId: string;
    policyId: string;
    expectedPolicyVersion: number;
    idempotencyKey: string;
    occurredAt: string;
  }): Promise<void> {
    await this.application.process({
      kind: "private_control",
      householdId: input.householdId,
      idempotencyKey: input.idempotencyKey,
      occurredAt: input.occurredAt,
      channel: { channelId: input.channelId, scope: "personal", adultId: input.adultId },
      requesterAdultId: input.adultId,
      action: {
        kind: "revoke_sharing_policy",
        policyId: input.policyId,
        expectedPolicyVersion: input.expectedPolicyVersion,
      },
    });
  }
}

type PrivateCommandInput = Parameters<PrivateCommandHandler["handle"]>[0] & {
  readonly replyToMessageId?: string;
};

/** Deterministic, model-free private controls available regardless of connected integrations. */
export class PrivateControlCommandService implements PrivateCommandHandler {
  readonly #snapshots: PrivateControlSnapshotReader;
  readonly #outbox: PrivateControlOutbox;
  readonly #mutator: PrivateControlMutator;
  readonly #sharingReferences: PrivateSharingReferenceResolver | undefined;
  readonly #catalog: PrivateControlCatalog;

  public constructor(options: PrivateControlCommandServiceOptions) {
    this.#snapshots = options.snapshots;
    this.#outbox = options.outbox;
    this.#mutator = options.mutator;
    this.#sharingReferences = options.sharingReferences;
    this.#catalog = options.catalog ?? new PrivateControlCatalog();
  }

  public async handle(input: PrivateCommandInput): Promise<{ handled: boolean; classification?: string }> {
    const command = parsePrivateControlCommand(input.text);
    if (command === null) return { handled: false };

    const snapshot = await this.#snapshots.load(input.householdId);
    if (
      snapshot === null ||
      !snapshot.aggregate.verifiedAdultIds.some((candidate) => candidate === input.adultId)
    ) {
      await this.#queueMessages(input, "unavailable", [
        "I couldn't verify this private household control. Nothing was changed.",
      ]);
      return { handled: true, classification: "control:identity_unavailable" };
    }

    switch (command.kind) {
      case "list_knowledge": {
        const items = this.#catalog.listKnowledge(snapshot, input.adultId, input.occurredAt);
        await this.#queueMessages(input, "knowledge", knowledgePages(items));
        return { handled: true, classification: "control:knowledge_listed" };
      }
      case "list_sharing_rules": {
        const rules = this.#catalog.listSharingRules(snapshot, input.adultId);
        await this.#queueMessages(input, "sharing-rules", sharingRulePages(rules));
        return { handled: true, classification: "control:sharing_rules_listed" };
      }
      case "forget": {
        if (command.controlId === null) {
          await this.#queueMessages(
            input,
            "forget-needs-id",
            forgetChoicePages(this.#catalog.listKnowledge(snapshot, input.adultId, input.occurredAt)),
          );
          return { handled: true, classification: "control:forget_needs_exact_id" };
        }
        const resolution = this.#catalog.resolveMemory(
          snapshot,
          input.adultId,
          command.controlId,
          input.occurredAt,
        );
        if (resolution.status !== "active") {
          await this.#queueMessages(input, "forget-not-active", [
            resolution.status === "inactive"
              ? "That memory is already inactive. Nothing was changed."
              : "I couldn't match that exact active memory ID. Nothing was changed. Ask “what do you remember?” for your current IDs.",
          ]);
          return {
            handled: true,
            classification:
              resolution.status === "inactive"
                ? "control:memory_already_inactive"
                : "control:memory_id_unresolved",
          };
        }
        await this.#mutator.revokeMemory({
          householdId: input.householdId,
          adultId: input.adultId,
          channelId: input.channelId,
          memoryId: resolution.value.memoryId,
          idempotencyKey: input.idempotencyKey,
          occurredAt: input.occurredAt,
        });
        return { handled: true, classification: "control:memory_revocation_submitted" };
      }
      case "stop_sharing": {
        if (command.controlId === null) {
          await this.#queueMessages(
            input,
            "stop-sharing-needs-id",
            sharingRulePages(this.#catalog.listSharingRules(snapshot, input.adultId), true),
          );
          return { handled: true, classification: "control:sharing_revoke_needs_exact_id" };
        }
        const resolution = this.#catalog.resolveSharingRule(snapshot, input.adultId, command.controlId);
        if (resolution.status !== "active") {
          await this.#queueMessages(input, "sharing-rule-not-active", [
            resolution.status === "inactive"
              ? "That sharing rule is already inactive. Nothing was changed."
              : "I couldn't match that exact active sharing-rule ID. Nothing was changed. Ask “show my sharing rules” for current IDs.",
          ]);
          return {
            handled: true,
            classification:
              resolution.status === "inactive"
                ? "control:sharing_rule_already_inactive"
                : "control:sharing_rule_id_unresolved",
          };
        }
        await this.#mutator.revokeSharingPolicy({
          householdId: input.householdId,
          adultId: input.adultId,
          channelId: input.channelId,
          policyId: resolution.value.policyId,
          expectedPolicyVersion: resolution.value.version,
          idempotencyKey: input.idempotencyKey,
          occurredAt: input.occurredAt,
        });
        return { handled: true, classification: "control:sharing_rule_revocation_submitted" };
      }
      case "explain_sharing": {
        const replyControlId =
          input.replyToMessageId === undefined || this.#sharingReferences === undefined
            ? null
            : await this.#sharingReferences.resolveSharingControlId({
                householdId: input.householdId,
                adultId: input.adultId,
                providerMessageId: input.replyToMessageId,
              });
        const requestedControlId = command.controlId ?? replyControlId;
        if (
          requestedControlId === null ||
          (command.controlId !== null && replyControlId !== null && command.controlId !== replyControlId)
        ) {
          await this.#queueMessages(
            input,
            "sharing-explanation-choices",
            sharingChoicePages(this.#catalog.listRecentSharingChoices(snapshot, input.adultId)),
          );
          return { handled: true, classification: "control:sharing_explanation_needs_exact_id" };
        }
        const resolution = this.#catalog.explainSharingChoice(snapshot, input.adultId, requestedControlId);
        if (resolution.status !== "active") {
          await this.#queueMessages(input, "sharing-explanation-unresolved", [
            "I couldn't match that exact sharing reference to one of your private-source decisions. I won't guess.",
            ...sharingChoicePages(this.#catalog.listRecentSharingChoices(snapshot, input.adultId)),
          ]);
          return { handled: true, classification: "control:sharing_explanation_unresolved" };
        }
        await this.#queueMessages(input, "sharing-explanation", [sharingExplanation(resolution.value)], {
          kind: "sharing_explanation",
          sharingRef: requestedControlId,
        });
        return { handled: true, classification: "control:sharing_explained" };
      }
    }
  }

  async #queueMessages(
    input: Pick<PrivateCommandInput, "householdId" | "adultId" | "idempotencyKey">,
    suffix: string,
    bodies: readonly string[],
    responseContext?: ConversationResponseContext,
  ): Promise<void> {
    for (const [index, body] of bodies.entries()) {
      const intentId = `system_${stableId(`${input.idempotencyKey}:${suffix}:${index}`)}`;
      await this.#outbox.enqueueApplicationIntent(
        ApplicationOutboxIntentSchema.parse({
          intentId,
          householdId: input.householdId,
          idempotencyKey: `florence:${intentId}`,
          kind: "conversation.send",
          targetScope: { kind: "personal", adultId: input.adultId },
          messageClass: "status",
          ...(responseContext === undefined ? {} : { responseContext }),
          body,
        }),
      );
    }
  }
}

type ParsedPrivateControlCommand =
  | { readonly kind: "list_knowledge" }
  | { readonly kind: "list_sharing_rules" }
  | { readonly kind: "forget"; readonly controlId: string | null }
  | { readonly kind: "stop_sharing"; readonly controlId: string | null }
  | { readonly kind: "explain_sharing"; readonly controlId: string | null };

export function parsePrivateControlCommand(rawText: string): ParsedPrivateControlCommand | null {
  const text = rawText.normalize("NFKC").trim();
  const normalized = text.toLowerCase();
  if (
    /^(?:what do you (?:know|remember)|what have you remembered|show (?:me )?what you (?:know|remember)|list my memories)(?:\s+about\b[\s\S]*)?[?!.]*$/u.test(
      normalized,
    )
  ) {
    return { kind: "list_knowledge" };
  }
  if (/^(?:show|list|what are) my (?:automatic )?sharing rules[?!.]*$/u.test(normalized)) {
    return { kind: "list_sharing_rules" };
  }

  const forget = text.match(/^forget(?:\s+(MEM-[A-Fa-f0-9]{16}))?[?!.]*$/iu);
  if (forget) return { kind: "forget", controlId: forget[1]?.toUpperCase() ?? null };
  if (/^forget\b/iu.test(text)) return { kind: "forget", controlId: null };

  const stopSharing = text.match(/^stop sharing(?:\s+(RULE-[A-Fa-f0-9]{16}))?[?!.]*$/iu);
  if (stopSharing) {
    return { kind: "stop_sharing", controlId: stopSharing[1]?.toUpperCase() ?? null };
  }
  if (/^stop sharing\b/iu.test(text)) return { kind: "stop_sharing", controlId: null };

  const explain = text.match(/^why did you share(?: that)?(?:\s+(SHARE-[A-Fa-f0-9]{16}))?[?!.]*$/iu);
  if (explain) {
    return { kind: "explain_sharing", controlId: explain[1]?.toUpperCase() ?? null };
  }
  if (/^why did you share\b/iu.test(text)) return { kind: "explain_sharing", controlId: null };
  return null;
}

function knowledgePages(items: readonly PrivateKnowledgeItem[]): string[] {
  if (items.length === 0) {
    return ["I don't have any active learned memories or confirmed shared profile details visible to you."];
  }
  const lines = items.map(
    (item) =>
      `• ${item.controlId} [${item.scope}; ${item.kind}] ${clip(item.statement, 600)} — ${item.sourceLabel}; ${asOf(item.asOf)}`,
  );
  return paginate(
    "What Florence currently knows from authoritative records visible to you:",
    lines,
    "Use an exact MEM-… ID with “forget” to revoke a learned memory. Shared profile and routine corrections stay shared household decisions.",
  );
}

function forgetChoicePages(items: readonly PrivateKnowledgeItem[]): string[] {
  const memories = items.filter((item) => item.kind === "memory");
  if (memories.length === 0) {
    return ["You do not have an active learned memory that can be forgotten."];
  }
  return paginate(
    "Name the exact memory ID to forget:",
    memories.map((item) => `• ${item.controlId} ${clip(item.statement, 600)} — ${asOf(item.asOf)}`),
    "Reply with “forget MEM-…” using one complete ID. I won't infer which memory you mean.",
  );
}

function sharingRulePages(items: readonly PrivateSharingRuleItem[], needsSelection = false): string[] {
  if (items.length === 0) return ["You do not have an active automatic-sharing rule."];
  return paginate(
    needsSelection ? "Name the exact sharing-rule ID to revoke:" : "Your active automatic-sharing rules:",
    items.map(
      (item) =>
        `• ${item.controlId} [personal → household] ${item.sourceLabel} ${item.sourceClass}, up to ${item.maximumSensitivity} — version ${item.policyVersion}; as of ${item.asOf}`,
    ),
    needsSelection
      ? "Reply with “stop sharing RULE-…” using one complete ID. I won't infer which rule you mean."
      : "Use an exact RULE-… ID with “stop sharing” to revoke a rule.",
  );
}

function sharingChoicePages(items: readonly PrivateSharingChoiceItem[]): string[] {
  if (items.length === 0) {
    return ["I don't have a recent private-source sharing decision for you to inspect."];
  }
  return paginate(
    "Reply to the exact Florence message, or choose one of your recent sharing decisions:",
    items.map(
      (item) => `• ${item.controlId} ${clip(item.summary, 600)} — ${item.sourceLabel}; as of ${item.asOf}`,
    ),
    "Ask “why did you share that SHARE-…” using one complete ID. I won't guess from a topic.",
  );
}

function sharingExplanation(item: PrivateSharingChoiceItem): string {
  return [
    `${item.controlId}: ${clip(item.summary, 1_000)}`,
    item.authorityLabel,
    `Source label: ${item.sourceLabel}. Scope: personal source → minimum household meaning. As of ${item.asOf}.`,
    "Florence does not include the underlying private source text in this explanation.",
  ].join("\n");
}

function paginate(header: string, lines: readonly string[], footer: string): string[] {
  const pages: string[] = [];
  let current = header;
  for (const line of lines) {
    if (current.length + line.length + footer.length + 4 > 3_900) {
      pages.push(current);
      current = `${header} (continued)`;
    }
    current += `\n${clip(line, 3_200)}`;
  }
  if (current.length + footer.length + 2 > 3_900) {
    pages.push(current);
    current = `${header} (continued)`;
  }
  pages.push(`${current}\n\n${footer}`);
  return pages;
}

function asOf(value: string | null): string {
  return value === null ? "recorded time unavailable" : `as of ${value}`;
}

function clip(value: string, maxLength: number): string {
  const normalized = value.normalize("NFKC").replace(/\s+/gu, " ").trim();
  if (normalized.length <= maxLength) return normalized;
  return `${normalized.slice(0, maxLength - 1).trimEnd()}…`;
}

function stableId(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
