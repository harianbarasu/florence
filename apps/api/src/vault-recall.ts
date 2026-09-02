import { createHash } from "node:crypto";
import { decodeFactFileArtifacts, type FileArtifactReference } from "@florence/artifacts";
import {
  decodeMemoryPresentation,
  decodeVaultListItems,
  isStructuredVaultList,
  type MemoryPresentation,
  type VaultListItem,
} from "@florence/contracts";
import type { FactRecord, SourceRecord } from "@florence/database";

/**
 * Search pages are bounded by their serialized UTF-8 size, not by an arbitrary
 * number of memories. Every omitted result is reachable through `nextCursor`.
 */
// The capability envelope allows 100 KB. Use the rest for protocol/error
// framing while returning as much ranked context as the turn can safely carry;
// every remaining result stays reachable through `nextCursor`.
export const VAULT_SEARCH_PAGE_BYTE_BUDGET = 80_000;
/**
 * One embedding input stays below common 8k-token provider envelopes even in
 * the worst case where each UTF-8 byte becomes a token. Oversized memories are
 * split without dropping any text; this is a per-request safety bound, not a
 * corpus or retention cutoff.
 */
export const VAULT_EMBEDDING_TEXT_BYTE_BUDGET = 8_000;
const CURSOR_VERSION = 2;
const CURSOR_MAX_BYTES = 512;
const FACT_URI_PREFIX = "vault://fact/";
const LEXICAL_BOOST_PER_MATCH = 0.05;
const MAX_LEXICAL_BOOST = 0.2;

export type VaultRetrievalMode = "hybrid" | "lexical_fallback";

/**
 * The embedding seam is deliberately narrow: the adapter owns provider batch
 * mechanics while VaultRecall owns complete-corpus chunking and ranking. Its
 * version must change whenever the model or vector semantics change.
 */
export type VaultEmbeddingAdapter = Readonly<{
  version: string;
  embed(texts: readonly string[], signal?: AbortSignal): Promise<readonly (readonly number[])[]>;
}>;

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
  retrievalMode: VaultRetrievalMode;
}>;

export type VaultMemory = Readonly<{
  factId: string;
  statement: string;
  memoryKind: MemoryPresentation["memoryKind"];
  artifactKind: MemoryPresentation["artifactKind"];
  title: string | null;
  details: string | null;
  tags: readonly string[];
  listItems?: readonly VaultListItem[];
  listStructured?: boolean;
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
  listItems: readonly VaultListItem[];
  listStructured: boolean;
  files: readonly FileArtifactReference[];
  abstract: string;
  fields: readonly SearchField[];
  semanticChunks: readonly string[];
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

type RankedSearch = Readonly<{
  facts: readonly RankedFact[];
  retrievalMode: VaultRetrievalMode;
  adapterVersion: string;
  rankDigest: string;
}>;

type DocumentVectors = Readonly<{
  dimension: number;
  byFactId: ReadonlyMap<string, readonly UnitVector[]>;
}>;

type UnitVector = readonly number[];

type CursorPayload = Readonly<{
  v: typeof CURSOR_VERSION;
  q: string;
  r: string;
  m: VaultRetrievalMode;
  a: string;
  d: string;
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
  readonly #embeddingAdapter: VaultEmbeddingAdapter | null;
  readonly #adapterVersion: string;
  #documentVectors: Promise<DocumentVectors> | null = null;
  readonly #rankings = new Map<string, Promise<RankedSearch>>();

  constructor(authorizedFacts: readonly FactRecord[], embeddingAdapter: VaultEmbeddingAdapter | null = null) {
    const byId = new Map<string, IndexedFact>();
    for (const fact of authorizedFacts) {
      if (byId.has(fact.id)) throw new Error(`Vault contains duplicate fact ${fact.id}`);
      const presentation = decodeMemoryPresentation(fact.value);
      const listItems = decodeVaultListItems(fact.value);
      const listStructured = isStructuredVaultList(fact.value);
      const files = decodeFactFileArtifacts(fact.value);
      const statement = factStatement(fact);
      const indexed = {
        fact,
        uri: `${FACT_URI_PREFIX}${fact.id}`,
        statement,
        presentation,
        listItems,
        listStructured,
        files,
        abstract: memoryAbstract(fact, statement, presentation),
        fields: searchFields(fact, statement, presentation, listItems, files),
        semanticChunks: semanticChunks(fact, statement, presentation, listItems, files),
      } satisfies IndexedFact;
      byId.set(fact.id, indexed);
    }
    this.#byId = byId;
    this.#facts = [...byId.values()];
    this.#revision = revisionOf(authorizedFacts);
    const adapterVersion = embeddingAdapter?.version.trim() ?? "";
    this.#embeddingAdapter = adapterVersion ? embeddingAdapter : null;
    this.#adapterVersion = adapterVersion || "none";
  }

  async search(
    input: Readonly<{ query: string; cursor?: string | null; signal?: AbortSignal }>,
  ): Promise<VaultSearchPage> {
    input.signal?.throwIfAborted();
    const query = normalizeText(input.query);
    if (!query) throw new Error("Vault search query is required");

    const queryDigest = digest(`vault-query-v1\0${query}`);
    let ranking = this.#rankings.get(query);
    if (!ranking) {
      ranking = this.#rank(query, input.signal);
      this.#rankings.set(query, ranking);
      void ranking.catch(() => {
        if (this.#rankings.get(query) === ranking) this.#rankings.delete(query);
      });
    }
    const rankedSearch = await ranking;
    input.signal?.throwIfAborted();
    const ranked = rankedSearch.facts;
    const offset = input.cursor
      ? decodeCursor(input.cursor, {
          queryDigest,
          revision: this.#revision,
          retrievalMode: rankedSearch.retrievalMode,
          adapterVersion: rankedSearch.adapterVersion,
          rankDigest: rankedSearch.rankDigest,
          total: ranked.length,
        })
      : 0;

    const results: VaultSearchResult[] = [];
    for (let index = offset; index < ranked.length; index += 1) {
      const rankedFact = ranked[index];
      if (!rankedFact) break;
      const maximumScore = ranked[0]?.rawScore ?? rankedFact.rawScore;
      const candidate: VaultSearchResult = {
        uri: rankedFact.indexed.uri,
        score: roundScore(maximumScore > 0 ? rankedFact.rawScore / maximumScore : 0),
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
        rankedSearch,
      );
      if (serializedBytes(candidatePage) > VAULT_SEARCH_PAGE_BYTE_BUDGET) break;
      results.push(candidate);
    }

    if (results.length === 0 && offset < ranked.length) {
      throw new Error("One Vault search result exceeds the serialized page budget");
    }

    return page(
      results,
      ranked.length,
      offset + results.length,
      query,
      queryDigest,
      this.#revision,
      rankedSearch,
    );
  }

  async #rank(query: string, signal?: AbortSignal): Promise<RankedSearch> {
    const adapter = this.#embeddingAdapter;
    if (adapter) {
      try {
        return await this.#hybridRank(query, adapter, signal);
      } catch {
        signal?.throwIfAborted();
        // Retrieval is useful even when the semantic provider is unavailable,
        // or malformed. Preserve the exact lexical behavior.
      }
    }
    return rankedSearch(rankFactsLexically(this.#facts, query), "lexical_fallback", this.#adapterVersion);
  }

  async #hybridRank(
    query: string,
    adapter: VaultEmbeddingAdapter,
    signal?: AbortSignal,
  ): Promise<RankedSearch> {
    if (this.#facts.length === 0) return rankedSearch([], "hybrid", this.#adapterVersion);

    const queryChunks = chunkUtf8(query, VAULT_EMBEDDING_TEXT_BYTE_BUDGET);
    const [documents, embeddedQuery] = await Promise.all([
      this.#cachedDocumentVectors(adapter, signal),
      adapter.embed(queryChunks, signal),
    ]);
    if (embeddedQuery.length !== queryChunks.length) {
      throw new Error("Vault embedding adapter returned the wrong number of query vectors");
    }
    const queryVectors = unitVectors(embeddedQuery);
    if (queryVectors.dimension !== documents.dimension) {
      throw new Error("Vault embedding adapter returned inconsistent dimensions");
    }

    const queryTerms = tokens(query);
    const facts = this.#facts
      .map((indexed) => {
        const vectors = documents.byFactId.get(indexed.fact.id);
        if (!vectors?.length) throw new Error("Vault embedding adapter omitted a fact vector");
        let semanticScore = 0;
        for (const queryVector of queryVectors.vectors) {
          for (const documentVector of vectors) {
            semanticScore = Math.max(semanticScore, cosineOfUnitVectors(queryVector, documentVector));
          }
        }
        const rawScore = clampScore(semanticScore) + lexicalBoost(indexed.fields, queryTerms);
        return { indexed, rawScore } satisfies RankedFact;
      })
      .sort(compareRankedFacts);

    return rankedSearch(facts, "hybrid", this.#adapterVersion);
  }

  #cachedDocumentVectors(adapter: VaultEmbeddingAdapter, signal?: AbortSignal): Promise<DocumentVectors> {
    if (this.#documentVectors) return this.#documentVectors;
    const pending = embedDocuments(this.#facts, adapter, signal);
    this.#documentVectors = pending;
    void pending.catch(() => {
      if (this.#documentVectors === pending) this.#documentVectors = null;
    });
    return pending;
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
  rankedSearch: RankedSearch,
): VaultSearchPage {
  const complete = nextOffset >= total;
  return {
    query,
    results,
    total,
    complete,
    nextCursor: complete
      ? null
      : encodeCursor(
          queryDigest,
          revision,
          rankedSearch.retrievalMode,
          rankedSearch.adapterVersion,
          rankedSearch.rankDigest,
          nextOffset,
        ),
    retrievalMode: rankedSearch.retrievalMode,
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
    listItems: abstract ? [] : indexed.listItems,
    listStructured: indexed.listStructured,
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

function rankFactsLexically(facts: readonly IndexedFact[], query: string): RankedFact[] {
  const terms = tokens(query);
  return facts
    .map((indexed) => ({ indexed, rawScore: relevance(indexed.fields, query, terms) }))
    .filter((candidate) => candidate.rawScore > 0)
    .sort(compareRankedFacts);
}

function rankedSearch(
  facts: readonly RankedFact[],
  retrievalMode: VaultRetrievalMode,
  adapterVersion: string,
): RankedSearch {
  return {
    facts,
    retrievalMode,
    adapterVersion,
    rankDigest: digest(
      `florence-vault-ranking-v1\0${canonicalJson(
        facts.map(({ indexed, rawScore }) => ({ id: indexed.fact.id, score: rawScore })),
      )}`,
    ),
  };
}

function compareRankedFacts(left: RankedFact, right: RankedFact): number {
  return (
    right.rawScore - left.rawScore ||
    right.indexed.fact.updatedAt.localeCompare(left.indexed.fact.updatedAt) ||
    left.indexed.fact.id.localeCompare(right.indexed.fact.id)
  );
}

async function embedDocuments(
  facts: readonly IndexedFact[],
  adapter: VaultEmbeddingAdapter,
  signal?: AbortSignal,
): Promise<DocumentVectors> {
  const requests = facts.flatMap((indexed) =>
    indexed.semanticChunks.map((text) => ({ factId: indexed.fact.id, text })),
  );
  const embedded = await adapter.embed(
    requests.map(({ text }) => text),
    signal,
  );
  const normalized = unitVectors(embedded);
  if (normalized.vectors.length !== requests.length) {
    throw new Error("Vault embedding adapter returned the wrong number of vectors");
  }

  const byFactId = new Map<string, UnitVector[]>();
  for (const indexed of facts) byFactId.set(indexed.fact.id, []);
  for (let index = 0; index < requests.length; index += 1) {
    const request = requests[index];
    const vector = normalized.vectors[index];
    if (!request || !vector) throw new Error("Vault embedding adapter omitted a fact vector");
    byFactId.get(request.factId)?.push(vector);
  }
  if ([...byFactId.values()].some((vectors) => vectors.length === 0)) {
    throw new Error("Vault embedding adapter omitted a fact vector");
  }
  return { dimension: normalized.dimension, byFactId };
}

function unitVectors(vectors: readonly (readonly number[])[]): Readonly<{
  vectors: readonly UnitVector[];
  dimension: number;
}> {
  if (vectors.length === 0) throw new Error("Vault embedding adapter returned no vectors");
  const dimension = vectors[0]?.length ?? 0;
  if (dimension === 0) throw new Error("Vault embedding adapter returned an empty vector");
  const normalized = vectors.map((vector) => {
    if (vector.length !== dimension || vector.some((value) => !Number.isFinite(value))) {
      throw new Error("Vault embedding adapter returned malformed vectors");
    }
    const magnitude = Math.sqrt(vector.reduce((sum, value) => sum + value * value, 0));
    if (magnitude === 0) throw new Error("Vault embedding adapter returned a zero vector");
    return vector.map((value) => value / magnitude);
  });
  return { vectors: normalized, dimension };
}

function cosineOfUnitVectors(left: UnitVector, right: UnitVector): number {
  let score = 0;
  for (let index = 0; index < left.length; index += 1) {
    score += (left[index] ?? 0) * (right[index] ?? 0);
  }
  return score;
}

function clampScore(value: number): number {
  return Math.max(0, Math.min(1, Number.isFinite(value) ? value : 0));
}

/** Adapted from OpenViking's capped query-token overlap reranker. */
function lexicalBoost(fields: readonly SearchField[], terms: readonly string[]): number {
  const matched = new Set<string>();
  for (const term of terms) {
    for (const field of fields) {
      if (field.tokens.has(term)) {
        matched.add(term);
        break;
      }
      if (term.length < 4) continue;
      if (
        [...field.tokens].some(
          (candidate) => candidate.length >= 4 && (candidate.startsWith(term) || term.startsWith(candidate)),
        )
      ) {
        matched.add(term);
        break;
      }
    }
  }
  return Math.min(MAX_LEXICAL_BOOST, matched.size * LEXICAL_BOOST_PER_MATCH);
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
  listItems: readonly VaultListItem[],
  files: readonly FileArtifactReference[],
): readonly SearchField[] {
  return [
    searchField(presentation.title, 12),
    searchField(presentation.tags.join(" "), 10),
    searchField(fact.label, 8),
    searchField(fact.slot, 7),
    searchField(statement, 6),
    searchField(presentation.details, 4),
    searchField(listItems.map((item) => item.text).join(" "), 5),
    searchField(`${presentation.memoryKind} ${presentation.artifactKind ?? ""} ${fact.kind}`, 3),
    searchField(files.map((file) => `${file.filename} ${file.mimeType}`).join(" "), 3),
    searchField(
      fact.sources.map((source) => `${source.label} ${canonicalJson(source.metadata)}`).join(" "),
      1,
    ),
  ].filter((field) => field.text.length > 0);
}

function semanticChunks(
  fact: FactRecord,
  statement: string,
  presentation: MemoryPresentation,
  listItems: readonly VaultListItem[],
  files: readonly FileArtifactReference[],
): readonly string[] {
  const text = [
    presentation.title ? `Title: ${presentation.title}` : "",
    presentation.tags.length > 0 ? `Tags: ${presentation.tags.join(", ")}` : "",
    `Label: ${fact.label}`,
    `Slot: ${fact.slot}`,
    `Memory: ${statement}`,
    presentation.details ? `Details: ${presentation.details}` : "",
    listItems.length > 0
      ? `List items:\n${listItems.map((item) => `${item.checked ? "[x]" : "[ ]"} ${item.text}`).join("\n")}`
      : "",
    `Kind: ${presentation.memoryKind} ${presentation.artifactKind ?? ""} ${fact.kind}`,
    files.length > 0 ? `Files: ${files.map((file) => `${file.filename} (${file.mimeType})`).join(", ")}` : "",
    fact.sources.length > 0
      ? `Sources: ${fact.sources
          .map((source) => `${source.label} ${canonicalJson(source.metadata)}`)
          .join("\n")}`
      : "",
  ]
    .filter(Boolean)
    .join("\n");
  return chunkUtf8(text || statement, VAULT_EMBEDDING_TEXT_BYTE_BUDGET);
}

function chunkUtf8(text: string, byteBudget: number): readonly string[] {
  if (Buffer.byteLength(text, "utf8") <= byteBudget) return [text];
  const chunks: string[] = [];
  let current = "";
  let currentBytes = 0;
  for (const codePoint of text) {
    const bytes = Buffer.byteLength(codePoint, "utf8");
    if (current && currentBytes + bytes > byteBudget) {
      chunks.push(current);
      current = "";
      currentBytes = 0;
    }
    current += codePoint;
    currentBytes += bytes;
  }
  if (current) chunks.push(current);
  return chunks;
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

function encodeCursor(
  queryDigest: string,
  revision: string,
  retrievalMode: VaultRetrievalMode,
  adapterVersion: string,
  rankDigest: string,
  offset: number,
): string {
  const core = {
    v: CURSOR_VERSION,
    q: queryDigest,
    r: revision,
    m: retrievalMode,
    a: adapterVersionDigest(adapterVersion),
    d: rankDigest,
    o: offset,
  } as const;
  const cursor: CursorPayload = { ...core, c: cursorCheck(core) };
  return Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url");
}

function decodeCursor(
  cursor: string,
  expected: Readonly<{
    queryDigest: string;
    revision: string;
    retrievalMode: VaultRetrievalMode;
    adapterVersion: string;
    rankDigest: string;
    total: number;
  }>,
): number {
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
  if (parsed.q !== expected.queryDigest) {
    throw new Error("Vault continuation cursor belongs to another query");
  }
  if (parsed.r !== expected.revision) {
    throw new Error("Vault changed while continuing this search; search again");
  }
  if (parsed.m !== expected.retrievalMode) {
    throw new Error("Vault retrieval mode changed while continuing this search; search again");
  }
  if (parsed.a !== adapterVersionDigest(expected.adapterVersion)) {
    throw new Error("Vault embedding adapter changed while continuing this search; search again");
  }
  if (parsed.d !== expected.rankDigest) {
    throw new Error("Vault ranking changed while continuing this search; search again");
  }
  if (parsed.o > expected.total) throw new Error("Vault continuation cursor is out of range");
  return parsed.o;
}

function isCursorPayload(value: unknown): value is CursorPayload {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const keys = Object.keys(value).sort();
  if (keys.join(",") !== "a,c,d,m,o,q,r,v") return false;
  const candidate = value as Record<string, unknown>;
  return (
    candidate.v === CURSOR_VERSION &&
    typeof candidate.q === "string" &&
    /^[0-9a-f]{64}$/u.test(candidate.q) &&
    typeof candidate.r === "string" &&
    /^[0-9a-f]{64}$/u.test(candidate.r) &&
    (candidate.m === "hybrid" || candidate.m === "lexical_fallback") &&
    typeof candidate.a === "string" &&
    /^[0-9a-f]{64}$/u.test(candidate.a) &&
    typeof candidate.d === "string" &&
    /^[0-9a-f]{64}$/u.test(candidate.d) &&
    typeof candidate.o === "number" &&
    Number.isSafeInteger(candidate.o) &&
    candidate.o >= 0 &&
    typeof candidate.c === "string" &&
    /^[0-9a-f]{64}$/u.test(candidate.c)
  );
}

function cursorCheck(
  core: Readonly<{
    v: number;
    q: string;
    r: string;
    m: VaultRetrievalMode;
    a: string;
    d: string;
    o: number;
  }>,
): string {
  return digest(`florence-vault-cursor-v2\0${canonicalJson(core)}`);
}

function adapterVersionDigest(adapterVersion: string): string {
  return digest(`florence-vault-adapter-version-v1\0${adapterVersion}`);
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
