import { createHash } from "node:crypto";
import {
  type ApplicationInterpreterPort,
  type CalendarEventInboxItem,
  type CalendarTriageContext,
  type CalendarTriageResult,
  CalendarTriageResultSchema,
  type ConversationInboxItem,
  type ConversationInterpretationContext,
  type GmailInboxItem,
  type GmailTriageContext,
  type GmailTriageResult,
  GmailTriageResultSchema,
} from "../application/index.js";
import { type PersonalTriageCap, personalTriageCap } from "../domain/index.js";
import { canonicalJson } from "../security/canonical-json.js";
import type {
  ActivePersonalAttentionRule,
  PostgresPersonalAttentionStore,
} from "./personal-attention-store.js";

type TriageResult = GmailTriageResult | CalendarTriageResult;

/** Adds personal context and applies only deterministic, monotone post-triage caps. */
export class PersonalAttentionInterpreter implements ApplicationInterpreterPort {
  public constructor(
    private readonly base: ApplicationInterpreterPort,
    private readonly store: PostgresPersonalAttentionStore,
  ) {}

  public async interpretConversation(
    input: ConversationInboxItem,
    context: ConversationInterpretationContext,
  ): Promise<unknown> {
    if (input.channel.scope !== "personal") {
      return this.base.interpretConversation(input, context);
    }
    const rules = await this.#listRules(input.householdId, input.senderAdultId, input.occurredAt);
    return this.base.interpretConversation(input, addPersonalPreferences(context, rules));
  }

  public async triageGmail(input: GmailInboxItem, context: GmailTriageContext): Promise<unknown> {
    const rules = await this.#listRules(input.householdId, input.ownerAdultId, input.occurredAt);
    const baseline = GmailTriageResultSchema.parse(
      await this.base.triageGmail(input, addPersonalPreferences(context, rules)),
    );
    const applied = applyRoutingRules("gmail", baseline, rules);
    if (applied.applications.length === 0) return baseline;
    try {
      await this.store.recordApplications(
        applied.applications.map((application) => ({
          householdId: input.householdId,
          adultId: input.ownerAdultId,
          provider: "gmail",
          sourceRef: input.messageRef,
          sourceDigest: gmailDigest(input),
          appliedAt: input.occurredAt,
          ...application,
        })),
      );
      return GmailTriageResultSchema.parse(applied.result);
    } catch {
      return baseline;
    }
  }

  public async triageCalendar(
    input: CalendarEventInboxItem,
    context: CalendarTriageContext,
  ): Promise<unknown> {
    const rules = await this.#listRules(input.householdId, input.ownerAdultId, input.occurredAt);
    const baseline = CalendarTriageResultSchema.parse(
      await this.base.triageCalendar(input, addPersonalPreferences(context, rules)),
    );
    const applied = applyRoutingRules("calendar", baseline, rules);
    if (applied.applications.length === 0) return baseline;
    try {
      await this.store.recordApplications(
        applied.applications.map((application) => ({
          householdId: input.householdId,
          adultId: input.ownerAdultId,
          provider: "calendar",
          sourceRef: input.eventRef,
          sourceDigest: input.contentDigest,
          appliedAt: input.occurredAt,
          ...application,
        })),
      );
      return CalendarTriageResultSchema.parse(applied.result);
    } catch {
      return baseline;
    }
  }

  async #listRules(
    householdId: string,
    adultId: string,
    asOf: string,
  ): Promise<readonly ActivePersonalAttentionRule[]> {
    try {
      return await this.store.listActive({ householdId, adultId, asOf });
    } catch {
      return [];
    }
  }
}

function addPersonalPreferences<T extends { readonly activeMemories: readonly unknown[] }>(
  context: T,
  rules: readonly ActivePersonalAttentionRule[],
): T {
  const preferences = rules
    .filter((rule) => rule.rule.kind === "preference")
    .map((rule) => ({
      memoryId: `personal-attention:${rule.revisionId}`,
      kind: "preference" as const,
      statement: rule.statement,
      scope: "personal" as const,
      confirmedAt: rule.occurredAt,
    }));
  if (preferences.length === 0) return context;
  return { ...context, activeMemories: [...context.activeMemories, ...preferences] };
}

function applyRoutingRules(
  provider: "gmail" | "calendar",
  baseline: TriageResult,
  rules: readonly ActivePersonalAttentionRule[],
): {
  readonly result: TriageResult;
  readonly applications: readonly {
    readonly ruleRevisionId: string;
    readonly baselineDecision: TriageResult["decision"];
    readonly appliedDecision: TriageResult["decision"];
  }[];
} {
  let result = baseline;
  const applications: {
    ruleRevisionId: string;
    baselineDecision: TriageResult["decision"];
    appliedDecision: TriageResult["decision"];
  }[] = [];
  for (const active of rules) {
    const before = result.decision;
    const cap = personalTriageCap(active.rule, provider, result);
    if (cap === "unchanged") continue;
    result = narrowResult(result, cap);
    applications.push({
      ruleRevisionId: active.revisionId,
      baselineDecision: before,
      appliedDecision: result.decision,
    });
  }
  return { result, applications };
}

function narrowResult(result: TriageResult, cap: Exclude<PersonalTriageCap, "unchanged">): TriageResult {
  const base = {
    confidence: result.confidence,
    sourceClass: result.sourceClass,
    sensitivity: result.sensitivity,
    familyImpact: result.familyImpact,
    rationale: result.rationale,
  };
  if (cap === "ignore") return { ...base, decision: "ignore" };
  if (!("privateSummary" in result)) {
    throw new Error("A review cap requires an existing private summary");
  }
  return { ...base, decision: "private_review", privateSummary: result.privateSummary };
}

function gmailDigest(input: GmailInboxItem): string {
  return digest(
    canonicalJson({
      accountRef: input.accountRef,
      messageRef: input.messageRef,
      revision: input.revision,
      labels: input.labels,
      sender: input.sender,
      subject: input.subject,
      snippet: input.snippet,
      bodyText: input.bodyText,
      attachments: input.attachmentContents.map((attachment) => ({
        reference: attachment.reference,
        contentDigest: attachment.contentDigest,
      })),
    }),
  );
}

function digest(value: string): string {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}
