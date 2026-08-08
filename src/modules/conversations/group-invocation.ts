export type GroupInvocationBasis = "leading_address" | "proven_reply";

export interface GroupInvocation {
  readonly basis: GroupInvocationBasis;
  readonly requestText: string;
}

const NATURAL_REQUEST_START =
  /^(?:please\b|can\b|could\b|would\b|will\b|what\b|when\b|where\b|why\b|how\b|who\b|which\b|tell\b|find\b|help\b|remind\b|create\b|add\b|check\b|look\b|plan\b|summarize\b|explain\b|introduce\b|meet\b|this\b|here\b|i\b|we\b)/iu;
const NATURAL_QUESTION = /^(?:do|does|did|is|are|was|were|should)\b[\s\S]*\?$/iu;

/**
 * A deliberately narrow, deterministic invocation for a silent group. A mere
 * mention of Florence never invokes the agent; the name must lead the message
 * and be followed by either explicit punctuation or natural request language.
 */
export function leadingGroupInvocation(text: string): GroupInvocation | null {
  const normalized = text.trim();
  const addressed = /^(?:hey[\s,]+)?florence\b(?<tail>[\s\S]*)$/iu.exec(normalized);
  const tail = addressed?.groups?.tail;
  if (tail === undefined) return null;

  const explicit = /^\s*[,:\-–—]\s*(?<request>[\s\S]+)$/u.exec(tail)?.groups?.request?.trim();
  const natural = tail.trim();
  const requestText =
    explicit || (NATURAL_REQUEST_START.test(natural) || NATURAL_QUESTION.test(natural) ? natural : "");
  if (!requestText || requestText.length > 10_000) return null;
  return { basis: "leading_address", requestText };
}
