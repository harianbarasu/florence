export interface PrivateQuestionEvidence {
  readonly sourceRevisionId: string;
  readonly occurredAt: string;
  readonly accountKind: "personal_family" | "work";
  readonly content: Readonly<Record<string, unknown>>;
}

export interface PrivateQuestionContext {
  readonly provider: "google";
  readonly searchQuery: string | null;
  readonly sourceAuthorities: readonly {
    readonly integrationId: string;
    readonly integrationControlEpoch: number;
    readonly status: "active" | "paused" | "reauth_required" | "error";
  }[];
  readonly accounts: readonly {
    readonly accountKind: "personal_family" | "work";
    readonly connection: "active" | "paused" | "reauth_required" | "error";
    readonly liveMonitoring: "watching" | "starting" | "unavailable";
    readonly recentImport: "complete" | "importing" | "recovering";
    readonly olderHistoryImport: "complete" | "importing" | "recovering";
    readonly search: "searched" | "not_requested" | "temporarily_unavailable";
  }[];
  readonly evidence: readonly PrivateQuestionEvidence[];
}

export interface PrivateQuestionContextProvider {
  compilePrivateQuestionContext(input: {
    readonly personId: string;
    readonly expectedPersonControlEpoch: number;
    readonly question: string;
    readonly maxResults: number;
  }): Promise<PrivateQuestionContext>;
}

const QUESTION_STOP_WORDS = new Set([
  "a",
  "about",
  "all",
  "am",
  "an",
  "and",
  "any",
  "anything",
  "are",
  "as",
  "at",
  "be",
  "been",
  "but",
  "by",
  "can",
  "connected",
  "connection",
  "could",
  "did",
  "do",
  "does",
  "email",
  "emails",
  "find",
  "florence",
  "for",
  "from",
  "gmail",
  "had",
  "has",
  "have",
  "i",
  "if",
  "in",
  "inbox",
  "import",
  "imported",
  "importing",
  "is",
  "it",
  "mail",
  "me",
  "my",
  "of",
  "oh",
  "on",
  "or",
  "our",
  "please",
  "see",
  "scan",
  "scanning",
  "sent",
  "still",
  "sync",
  "syncing",
  "that",
  "the",
  "their",
  "there",
  "this",
  "to",
  "was",
  "we",
  "were",
  "what",
  "when",
  "where",
  "which",
  "who",
  "why",
  "with",
  "would",
  "you",
  "your",
]);

const PRIVATE_MAIL_SOURCE_TERMS = new Set(["email", "emails", "gmail", "inbox", "mail", "mailbox"]);

/**
 * Private connectors are not ambient context for every turn. This is an
 * app-owned privacy boundary: a direct question must explicitly name mail
 * before Florence may query the person's mailbox. The model still interprets
 * the evidence and writes the answer.
 */
export function requestsPrivateMailContext(question: string): boolean {
  const tokens = question
    .normalize("NFKC")
    .toLocaleLowerCase("en-US")
    .match(/[\p{L}\p{N}][\p{L}\p{N}'’-]*/gu);
  return tokens?.some((token) => PRIVATE_MAIL_SOURCE_TERMS.has(token)) ?? false;
}

/**
 * Produces a bounded provider query, not a user-facing interpretation. The model
 * still decides what the returned private evidence means for the user's turn.
 */
export function privateMailSearchQuery(question: string): string | null {
  const tokens = question
    .normalize("NFKC")
    .toLocaleLowerCase("en-US")
    .match(/[\p{L}\p{N}][\p{L}\p{N}'’-]*/gu);
  if (!tokens) return null;
  const meaningful = [...new Set(tokens)]
    .filter((token) => token.length > 1 && !QUESTION_STOP_WORDS.has(token))
    .slice(0, 10);
  if (meaningful.length === 0) return null;
  return meaningful.map((token) => `"${token.replaceAll('"', "")}"`).join(" ");
}
