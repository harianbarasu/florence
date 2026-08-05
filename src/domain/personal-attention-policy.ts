import { z } from "zod";
import { NeutralFactualTextSchema, SourceClassSchema } from "./contracts.js";

const EvidenceQuoteSchema = z.string().trim().min(2).max(240);

export const PersonalAttentionRuleSchema = z.union([
  z.strictObject({
    kind: z.literal("preference"),
    dimension: z.literal("private_response_detail"),
    value: z.enum(["concise", "detailed"]),
  }),
  z.strictObject({
    kind: z.literal("preference"),
    dimension: z.literal("private_response_format"),
    value: z.enum(["bullets", "prose"]),
  }),
  z.strictObject({
    kind: z.literal("routing_narrowing"),
    provider: z.enum(["gmail", "calendar"]),
    sourceClass: SourceClassSchema,
    effect: z.enum(["suppress_noise", "never_interrupt", "keep_private"]),
  }),
]);

export type PersonalAttentionRule = z.infer<typeof PersonalAttentionRuleSchema>;

export const PersonalLearningProposalSchema = z.discriminatedUnion("decision", [
  z.strictObject({
    decision: z.literal("ignore"),
    rationale: NeutralFactualTextSchema,
  }),
  z.strictObject({
    decision: z.literal("unsupported"),
    reason: z.enum(["approval_required", "confirmation_required", "ambiguous"]),
    rationale: NeutralFactualTextSchema,
  }),
  z.strictObject({
    decision: z.literal("propose"),
    evidenceQuote: EvidenceQuoteSchema,
    rule: PersonalAttentionRuleSchema,
    rationale: NeutralFactualTextSchema,
  }),
]);

export type PersonalLearningProposal = z.infer<typeof PersonalLearningProposalSchema>;

export type PersonalLearningDisposition =
  | { readonly status: "auto_apply"; readonly rule: PersonalAttentionRule }
  | {
      readonly status: "approval_required" | "confirmation_required" | "clarify" | "ignore";
      readonly reason: string;
    };

const EXPANSION_PATTERN =
  /\b(?:always\s+share|share\s+(?:it|this|these|emails?|messages?|events?)|send|book|buy|purchase|pay|submit|rsvp|cancel|add\s+to\s+(?:the\s+)?(?:family|household)|tell\s+(?:my\s+)?(?:partner|family|household))\b/iu;
const SENSITIVE_PATTERN =
  /\b(?:diagnos(?:is|ed)|medical|medication|therapy|therapist|salary|bank|balance|debt|legal|lawyer|custody|divorce|relationship|pregnan(?:cy|t)|health|password|passcode|access\s+code)\b/iu;
const EXPLICIT_PREFERENCE_PATTERN =
  /\b(?:i\s+(?:would\s+)?prefer|please|use\s+(?:bullets|prose)|keep\s+(?:private\s+)?replies|give\s+me)\b/iu;
const EXPLICIT_NARROWING_PATTERN =
  /\b(?:do\s+not|don't|never|stop|less|fewer|not\s+relevant|keep\s+(?:it|this|these|them)\s+private|quiet|suppress|batch)\b/iu;

export function evaluatePersonalLearningProposal(input: {
  readonly rawText: string;
  readonly proposal: PersonalLearningProposal;
  readonly source: "linq_dm" | "linq_group" | "gmail" | "calendar" | "reaction";
  readonly actorAdultId: string;
  readonly targetAdultId: string;
}): PersonalLearningDisposition {
  const proposal = PersonalLearningProposalSchema.parse(input.proposal);
  if (input.source !== "linq_dm" || input.actorAdultId !== input.targetAdultId) {
    return { status: "ignore", reason: "learning_requires_the_adult's_verified_private_dm" };
  }
  if (proposal.decision === "ignore") return { status: "ignore", reason: proposal.rationale };
  if (proposal.decision === "unsupported") {
    return {
      status: proposal.reason === "ambiguous" ? "clarify" : proposal.reason,
      reason: proposal.rationale,
    };
  }

  const text = normalize(input.rawText);
  const quote = normalize(proposal.evidenceQuote);
  if (!text.includes(quote)) {
    return { status: "clarify", reason: "the proposed evidence is not an exact part of the adult's message" };
  }
  if (EXPANSION_PATTERN.test(input.rawText)) {
    return { status: "approval_required", reason: "disclosure or action authority cannot be learned" };
  }
  if (SENSITIVE_PATTERN.test(input.rawText)) {
    return { status: "confirmation_required", reason: "sensitive information is not auto-learned" };
  }

  const explicitPattern =
    proposal.rule.kind === "routing_narrowing" ? EXPLICIT_NARROWING_PATTERN : EXPLICIT_PREFERENCE_PATTERN;
  if (!explicitPattern.test(proposal.evidenceQuote)) {
    return { status: "clarify", reason: "the message does not contain an explicit supported preference" };
  }
  if (!matchesRuleEvidence(proposal.rule, proposal.evidenceQuote)) {
    return {
      status: "clarify",
      reason: "the quoted words do not deterministically support the proposed rule",
    };
  }
  return { status: "auto_apply", rule: PersonalAttentionRuleSchema.parse(proposal.rule) };
}

export type PersonalTriageSummary = {
  readonly decision:
    | "ignore"
    | "retain_private"
    | "private_review"
    | "private_interrupt"
    | "propose_family_episode";
  readonly sourceClass: string;
  readonly sensitivity: "ordinary" | "sensitive" | "highly_sensitive";
  readonly familyImpact: boolean;
};

export type PersonalTriageCap = "unchanged" | "ignore" | "private_review";

const TRIAGE_RANK = {
  ignore: 0,
  retain_private: 1,
  private_review: 2,
  private_interrupt: 3,
  propose_family_episode: 4,
} as const;

/** Returns only a monotone private-attention cap; callers retain ownership of result construction. */
export function personalTriageCap(
  rule: PersonalAttentionRule,
  provider: "gmail" | "calendar",
  triage: PersonalTriageSummary,
): PersonalTriageCap {
  const parsed = PersonalAttentionRuleSchema.parse(rule);
  if (
    parsed.kind !== "routing_narrowing" ||
    parsed.provider !== provider ||
    parsed.sourceClass !== triage.sourceClass ||
    triage.sensitivity !== "ordinary"
  ) {
    return "unchanged";
  }

  let cap: PersonalTriageCap = "unchanged";
  switch (parsed.effect) {
    case "never_interrupt":
      cap = triage.decision === "private_interrupt" ? "private_review" : "unchanged";
      break;
    case "keep_private":
      cap = triage.decision === "propose_family_episode" ? "private_review" : "unchanged";
      break;
    case "suppress_noise":
      cap = triage.familyImpact
        ? triage.decision === "private_interrupt" || triage.decision === "propose_family_episode"
          ? "private_review"
          : "unchanged"
        : triage.decision === "ignore"
          ? "unchanged"
          : "ignore";
      break;
  }

  const finalDecision = cap === "unchanged" ? triage.decision : cap;
  if (TRIAGE_RANK[finalDecision] > TRIAGE_RANK[triage.decision]) {
    throw new Error("Personal attention rules must never broaden or escalate triage");
  }
  return cap;
}

export function personalAttentionStatement(rule: PersonalAttentionRule): string {
  const parsed = PersonalAttentionRuleSchema.parse(rule);
  if (parsed.kind === "preference") {
    if (parsed.dimension === "private_response_detail") {
      return parsed.value === "concise"
        ? "For private replies, prefer concise answers."
        : "For private replies, prefer more detailed answers.";
    }
    return parsed.value === "bullets"
      ? "For private replies, prefer bullet lists when useful."
      : "For private replies, prefer short prose when useful.";
  }
  const source = `${parsed.provider} ${parsed.sourceClass}`;
  switch (parsed.effect) {
    case "suppress_noise":
      return `Treat ordinary, non-family-impact ${source} items as noise.`;
    case "never_interrupt":
      return `Do not interrupt privately for ordinary ${source} items; include them in private review instead.`;
    case "keep_private":
      return `Keep ordinary ${source} items in private review instead of proposing household disclosure.`;
  }
}

export function personalAttentionIdentity(rule: PersonalAttentionRule): string {
  const parsed = PersonalAttentionRuleSchema.parse(rule);
  return parsed.kind === "preference"
    ? `preference:${parsed.dimension}`
    : `routing:${parsed.provider}:${parsed.sourceClass}`;
}

function normalize(value: string): string {
  return value.normalize("NFKC").trim().toLocaleLowerCase("en-US").replace(/\s+/gu, " ");
}

function matchesRuleEvidence(rule: PersonalAttentionRule, evidenceQuote: string): boolean {
  const evidence = normalize(evidenceQuote);
  if (rule.kind === "preference") {
    if (rule.dimension === "private_response_detail") {
      return rule.value === "concise"
        ? /\b(?:concise|brief|short(?:er)?|less\s+detail)\b/iu.test(evidence)
        : /\b(?:detailed|more\s+detail|thorough|in[- ]depth)\b/iu.test(evidence);
    }
    return rule.value === "bullets"
      ? /\b(?:bullet(?:ed|s)?|list)\b/iu.test(evidence)
      : /\b(?:prose|paragraphs?|sentences?)\b/iu.test(evidence);
  }

  const effectMatches =
    rule.effect === "suppress_noise"
      ? /\b(?:suppress|ignore|noise|not\s+relevant|less|fewer|quiet|batch)\b/iu.test(evidence)
      : rule.effect === "never_interrupt"
        ? /\b(?:never|do\s+not|don't|stop|no)\b[\s\S]{0,80}\b(?:interrupt|notify|alert|ping)s?\b/iu.test(
            evidence,
          )
        : /\b(?:keep)\b[\s\S]{0,80}\bprivate\b|\b(?:do\s+not|don't|never)\s+share\b/iu.test(evidence);
  if (!effectMatches) return false;

  const providerMatches =
    rule.provider === "gmail"
      ? /\b(?:gmail|e-?mail|mail|newsletter)s?\b/iu.test(evidence)
      : /\b(?:calendar|event|meeting|appointment)s?\b/iu.test(evidence);
  const sourceWords = rule.sourceClass.split(/[._-]+/u).filter((word) => word.length > 2);
  const sourceMatches = sourceWords.every((word) => {
    const escaped = word.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
    return new RegExp(`\\b${escaped}(?:s|es)?\\b`, "iu").test(evidence);
  });
  return providerMatches && sourceWords.length > 0 && sourceMatches;
}
