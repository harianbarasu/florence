import { createHash } from "node:crypto";
import { decodeFactFileArtifacts, type FileArtifactReference } from "@florence/artifacts";
import { decodeMemoryPresentation, type MemoryPresentation } from "@florence/contracts";
import type { FactRecord, SourceRecord } from "@florence/database";

/**
 * Search pages are bounded by their serialized UTF-8 size, not by an arbitrary
 * number of memories. Every omitted result is reachable through `nextCursor`.
 */
// The capability envelope allows 100 KB. Use the rest for protocol/error
// framing while returning as much ranked context as the turn can safely carry;
// every remaining result stays reachable through `nextCursor`.
export const VAULT_SEARCH_PAGE_BYTE_BUDGET = 80_000;
const CURSOR_VERSION = 1;
const CURSOR_MAX_BYTES = 512;
const FACT_URI_PREFIX = "vault://fact/";

export type VaultReadLevel = "abstract" | "overview" | "full";

export type VaultSearchResult = Readonly<{
  uri: string;
  score: number;
  abstract: string;
  memoryKind: MemoryPresentation["memoryKind"];
  artifactKind: MemoryPresentation["artifactKind"];
  title: string | null;
  tags: readonly string[];
  updatedAt: string;
}>;

export type VaultSearchPage = Readonly<{
  query: string;
  results: readonly VaultSearchResult[];
  total: number;
  complete: boolean;
  nextCursor: string | null;
}>;

export type VaultMemory = Readonly<{
  factId: string;
  statement: string;
  memoryKind: MemoryPresentation["memoryKind"];
  artifactKind: MemoryPresentation["artifactKind"];
  title: string | null;
  details: string | null;
  tags: readonly string[];
  files: readonly VaultFileResource[];
  visibility: FactRecord["visibility"];
  updatedAt: string;
}>;

export type VaultFileResource = Readonly<{
  uri: string;
  artifactId: string;
  filename: string;
  mimeType: string;
  byteLength: number;
  sha256: string;
}>;

export type AuthorizedVaultFile = Readonly<{
  factId: string;
  uri: string;
  artifact: FileArtifactReference;
}>;

export type VaultSupport = Readonly<{
  sourceId: string;
  kind: SourceRecord["kind"];
  label: string;
  visibility: SourceRecord["visibility"];
  occurredAt: string;
  metadata: SourceRecord["metadata"];
}>;

export type VaultReadResult = Readonly<{
  uri: string;
  level: VaultReadLevel;
  memory: VaultMemory;
  supports: readonly VaultSupport[];
}>;

type IndexedFact = Readonly<{
  fact: FactRecord;
  uri: string;
  statement: string;
  presentation: MemoryPresentation;
  files: readonly FileArtifactReference[];
  abstract: string;
  fields: readonly SearchField[];
}>;

type SearchField = Readonly<{
  text: string;
  tokens: ReadonlySet<string>;
  weight: number;
}>;

type RankedFact = Readonly<{
  indexed: IndexedFact;
  rawScore: number;
}>;

type CursorPayload = Readonly<{
  v: typeof CURSOR_VERSION;
  q: string;
  r: string;
  o: number;
  c: string;
}>;

/**
 * General household memory retrieval adapted from Hermes/OpenViking's
 * SEARCH_SCHEMA/READ_SCHEMA and _tool_search/_tool_read at hermes-agent
 * 6dcebea7: search returns lightweight URI references, then read promotes one
 * reference through abstract/overview/full detail. Callers pass only facts
 * already authorized for the requesting adult; this module never broadens
 * visibility.
 */
export class VaultRecall {
  readonly #byId: ReadonlyMap<string, IndexedFact>;
  readonly #facts: readonly IndexedFact[];
  readonly #revision: string;

  constructor(authorizedFacts: readonly FactRecord[]) {
    const byId = new Map<string, IndexedFact>();
    for (const fact of authorizedFacts) {
      if (byId.has(fact.id)) throw new Error(`Vault contains duplicate fact ${fact.id}`);
      const presentation = decodeMemoryPresentation(fact.value);
      const files = decodeFactFileArtifacts(fact.value);
      const statement = factStatement(fact);
      const indexed = {
        fact,
        uri: `${FACT_URI_PREFIX}${fact.id}`,
        statement,
        presentation,
        files,
        abstract: memoryAbstract(fact, statement, presentation),
        fields: searchFields(fact, statement, presentation),
      } satisfies IndexedFact;
      byId.set(fact.id, indexed);
    }
    this.#byId = byId;
    this.#facts = [...byId.values()];
    this.#revision = revisionOf(authorizedFacts);
  }

  search(input: Readonly<{ query: string; cursor?: string | null }>): VaultSearchPage {
    const query = normalizeText(input.query);
    if (!query) throw new Error("Vault search query is required");

    const queryDigest = digest(`vault-query-v1\0${query}`);
    const ranked = rankFacts(this.#facts, query);
    const offset = input.cursor ? decodeCursor(input.cursor, queryDigest, this.#revision, ranked.length) : 0;

    const results: VaultSearchResult[] = [];
    for (let index = offset; index < ranked.length; index += 1) {
      const rankedFact = ranked[index];
      if (!rankedFact) break;
      const maximumScore = ranked[0]?.rawScore ?? rankedFact.rawScore;
      const candidate: VaultSearchResult = {
        uri: rankedFact.indexed.uri,
        score: roundScore(rankedFact.rawScore / maximumScore),
        abstract: rankedFact.indexed.abstract,
        memoryKind: rankedFact.indexed.presentation.memoryKind,
        artifactKind: rankedFact.indexed.presentation.artifactKind,
        title: rankedFact.indexed.presentation.title,
        tags: rankedFact.indexed.presentation.tags,
        updatedAt: rankedFact.indexed.fact.updatedAt,
      };
      const candidates = [...results, candidate];
      const candidateOffset = offset + candidates.length;
      const candidatePage = page(
        candidates,
        ranked.length,
        candidateOffset,
        query,
        queryDigest,
        this.#revision,
      );
      if (serializedBytes(candidatePage) > VAULT_SEARCH_PAGE_BYTE_BUDGET) break;
      results.push(candidate);
    }

    if (results.length === 0 && offset < ranked.length) {
      throw new Error("One Vault search result exceeds the serialized page budget");
    }

    return page(results, ranked.length, offset + results.length, query, queryDigest, this.#revision);
  }

  read(input: Readonly<{ uri: string; level: VaultReadLevel }>): VaultReadResult | null {
    const id = factIdFromUri(input.uri);
    if (id === null) return null;
    const indexed = this.#byId.get(id);
    if (!indexed) return null;

    return {
      uri: indexed.uri,
      level: input.level,
      memory: exactMemory(indexed, input.level === "abstract"),
      supports: input.level === "full" ? indexed.fact.sources.map(vaultSupport) : [],
    };
  }

  resolveFile(uri: string): AuthorizedVaultFile | null {
    const parsed = factFileFromUri(uri);
    if (!parsed) return null;
    const indexed = this.#byId.get(parsed.factId);
    if (!indexed) return null;
    const artifact = indexed.files.find((candidate) => candidate.artifactId === parsed.artifactId);
    return artifact ? { factId: indexed.fact.id, uri, artifact } : null;
  }
}

function page(
  results: readonly VaultSearchResult[],
  total: number,
  nextOffset: number,
  query: string,
  queryDigest: string,
  revision: string,
): VaultSearchPage {
  const complete = nextOffset >= total;
  return {
    query,
    results,
    total,
    complete,
    nextCursor: complete ? null : encodeCursor(queryDigest, revision, nextOffset),
  };
}

function exactMemory(indexed: IndexedFact, abstract: boolean): VaultMemory {
  const { fact, presentation } = indexed;
  return {
    factId: fact.id,
    statement: indexed.statement,
    memoryKind: presentation.memoryKind,
    artifactKind: presentation.artifactKind,
    title: presentation.title,
    details: abstract ? null : presentation.details,
    tags: presentation.tags,
    files: abstract ? [] : indexed.files.map((artifact) => vaultFileResource(fact.id, artifact)),
    visibility: fact.visibility,
    updatedAt: fact.updatedAt,
  };
}

function vaultSupport(source: SourceRecord): VaultSupport {
  return {
    sourceId: source.id,
    kind: source.kind,
    label: source.label,
    visibility: source.visibility,
    occurredAt: source.occurredAt,
    metadata: source.metadata,
  };
}

function rankFacts(facts: readonly IndexedFact[], query: string): RankedFact[] {
  const terms = tokens(query);
  return facts
    .map((indexed) => ({ indexed, rawScore: relevance(indexed.fields, query, terms) }))
    .filter((candidate) => candidate.rawScore > 0)
    .sort(
      (left, right) =>
        right.rawScore - left.rawScore ||
        right.indexed.fact.updatedAt.localeCompare(left.indexed.fact.updatedAt) ||
        left.indexed.fact.id.localeCompare(right.indexed.fact.id),
    );
}

function relevance(fields: readonly SearchField[], query: string, terms: readonly string[]): number {
  let score = 0;
  const matched = new Set<string>();
  for (const field of fields) {
    if (field.text.includes(query)) score += field.weight * 2;
    for (const term of terms) {
      if (field.tokens.has(term)) {
        score += field.weight;
        matched.add(term);
        continue;
      }
      if (term.length < 4) continue;
      for (const candidate of field.tokens) {
        if (candidate.length >= 4 && (candidate.startsWith(term) || term.startsWith(candidate))) {
          score += field.weight * 0.6;
          matched.add(term);
          break;
        }
      }
    }
  }
  if (terms.length > 1 && matched.size > 0) {
    score *= 0.5 + 0.5 * (matched.size / terms.length);
  }
  return score;
}

function searchFields(
  fact: FactRecord,
  statement: string,
  presentation: MemoryPresentation,
): readonly SearchField[] {
  const files = decodeFactFileArtifacts(fact.value);
  return [
    searchField(presentation.title, 12),
    searchField(presentation.tags.join(" "), 10),
    searchField(fact.label, 8),
    searchField(fact.slot, 7),
    searchField(statement, 6),
    searchField(presentation.details, 4),
    searchField(`${presentation.memoryKind} ${presentation.artifactKind ?? ""} ${fact.kind}`, 3),
    searchField(files.map((file) => `${file.filename} ${file.mimeType}`).join(" "), 3),
    searchField(
      fact.sources.map((source) => `${source.label} ${canonicalJson(source.metadata)}`).join(" "),
      1,
    ),
  ].filter((field) => field.text.length > 0);
}

function vaultFileResource(factId: string, artifact: FileArtifactReference): VaultFileResource {
  return {
    uri: `${FACT_URI_PREFIX}${factId}/file/${artifact.artifactId}`,
    artifactId: artifact.artifactId,
    filename: artifact.filename,
    mimeType: artifact.mimeType,
    byteLength: artifact.byteLength,
    sha256: artifact.sha256,
  };
}

function searchField(value: string | null, weight: number): SearchField {
  const text = normalizeText(value ?? "");
  return { text, tokens: new Set(tokens(text)), weight };
}

function memoryAbstract(fact: FactRecord, statement: string, presentation: MemoryPresentation): string {
  const heading = presentation.title ?? fact.label;
  return heading === statement ? statement : `${heading} — ${statement}`;
}

function factStatement(fact: FactRecord): string {
  const value: unknown = fact.value;
  if (typeof value === "string") return value;
  if (isRecord(value) && typeof value.statement === "string") {
    return value.statement;
  }
  return JSON.stringify(value);
}

function factIdFromUri(uri: string): string | null {
  if (!uri.startsWith(FACT_URI_PREFIX)) return null;
  const id = uri.slice(FACT_URI_PREFIX.length);
  return id && !id.includes("/") && !id.includes("?") && !id.includes("#") ? id : null;
}

function factFileFromUri(uri: string): { factId: string; artifactId: string } | null {
  if (!uri.startsWith(FACT_URI_PREFIX)) return null;
  const [factId, segment, artifactId, ...rest] = uri.slice(FACT_URI_PREFIX.length).split("/");
  if (!factId || segment !== "file" || !artifactId || rest.length > 0) return null;
  if ([factId, artifactId].some((part) => part.includes("?") || part.includes("#"))) return null;
  return { factId, artifactId };
}

function encodeCursor(queryDigest: string, revision: string, offset: number): string {
  const core = { v: CURSOR_VERSION, q: queryDigest, r: revision, o: offset } as const;
  const cursor: CursorPayload = { ...core, c: cursorCheck(core) };
  return Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url");
}

function decodeCursor(cursor: string, queryDigest: string, revision: string, total: number): number {
  if (!cursor || cursor.length > 1024) throw new Error("Vault continuation cursor is invalid");
  let bytes: Buffer;
  try {
    bytes = Buffer.from(cursor, "base64url");
  } catch {
    throw new Error("Vault continuation cursor is invalid");
  }
  if (bytes.byteLength > CURSOR_MAX_BYTES || bytes.toString("base64url") !== cursor.replace(/=+$/u, "")) {
    throw new Error("Vault continuation cursor is invalid");
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(bytes.toString("utf8"));
  } catch {
    throw new Error("Vault continuation cursor is invalid");
  }
  if (!isCursorPayload(parsed)) throw new Error("Vault continuation cursor is invalid");
  const { c: check, ...core } = parsed;
  if (check !== cursorCheck(core)) throw new Error("Vault continuation cursor is invalid");
  if (parsed.q !== queryDigest) throw new Error("Vault continuation cursor belongs to another query");
  if (parsed.r !== revision) throw new Error("Vault changed while continuing this search; search again");
  if (parsed.o > total) throw new Error("Vault continuation cursor is out of range");
  return parsed.o;
}

function isCursorPayload(value: unknown): value is CursorPayload {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const keys = Object.keys(value).sort();
  if (keys.join(",") !== "c,o,q,r,v") return false;
  const candidate = value as Record<string, unknown>;
  return (
    candidate.v === CURSOR_VERSION &&
    typeof candidate.q === "string" &&
    /^[0-9a-f]{64}$/u.test(candidate.q) &&
    typeof candidate.r === "string" &&
    /^[0-9a-f]{64}$/u.test(candidate.r) &&
    typeof candidate.o === "number" &&
    Number.isSafeInteger(candidate.o) &&
    candidate.o >= 0 &&
    typeof candidate.c === "string" &&
    /^[0-9a-f]{64}$/u.test(candidate.c)
  );
}

function cursorCheck(core: Readonly<{ v: number; q: string; r: string; o: number }>): string {
  return digest(`florence-vault-cursor-v1\0${canonicalJson(core)}`);
}

function revisionOf(facts: readonly FactRecord[]): string {
  return digest(
    `florence-vault-revision-v1\0${canonicalJson([...facts].sort((a, b) => a.id.localeCompare(b.id)))}`,
  );
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return JSON.stringify(value);
  }
  if (typeof value === "number" && Number.isFinite(value)) return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (typeof value === "object") {
    return `{${Object.entries(value)
      .filter((entry) => entry[1] !== undefined)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, field]) => `${JSON.stringify(key)}:${canonicalJson(field)}`)
      .join(",")}}`;
  }
  throw new Error("Vault contains a value that cannot be serialized");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value) && !(value instanceof Date);
}

function normalizeText(value: string): string {
  return value.normalize("NFKC").trim().replace(/\s+/gu, " ").toLocaleLowerCase("en-US");
}

function tokens(value: string): string[] {
  return value.match(/[\p{L}\p{N}]+/gu) ?? (value ? [value] : []);
}

function roundScore(value: number): number {
  return Math.round(value * 1_000) / 1_000;
}

function serializedBytes(value: unknown): number {
  return Buffer.byteLength(JSON.stringify(value), "utf8");
}

function digest(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}
