import type { FileArtifactReference } from "@florence/artifacts";
import type { FactRecord, SourceRecord } from "@florence/database";
import { describe, expect, test } from "vitest";
import {
  VAULT_EMBEDDING_TEXT_BYTE_BUDGET,
  VAULT_SEARCH_PAGE_BYTE_BUDGET,
  type VaultEmbeddingAdapter,
  VaultRecall,
  type VaultSearchPage,
} from "./vault-recall.js";

describe("VaultRecall", () => {
  test("continues through every matching memory without losing tail facts", async () => {
    const facts = Array.from({ length: 75 }, (_, index) =>
      fact(index, {
        statement: `Family archive needle ${index}: ${"useful household context ".repeat(60)}`,
        title: `Archive item ${index}`,
        tags: ["archive", `item-${index}`],
      }),
    );
    const recall = new VaultRecall(facts);
    const seen = new Set<string>();
    let cursor: string | null = null;
    let pages = 0;
    let lastPage: VaultSearchPage | null = null;

    do {
      const page = await recall.search({ query: "family archive needle", cursor });
      expect(Buffer.byteLength(JSON.stringify(page), "utf8")).toBeLessThanOrEqual(
        VAULT_SEARCH_PAGE_BYTE_BUDGET,
      );
      expect(page.query).toBe("family archive needle");
      expect(page.retrievalMode).toBe("lexical_fallback");
      expect(page.total).toBe(facts.length);
      for (const result of page.results) {
        expect(seen.has(result.uri)).toBe(false);
        seen.add(result.uri);
      }
      pages += 1;
      expect(pages).toBeLessThan(20);
      cursor = page.nextCursor;
      lastPage = page;
    } while (cursor !== null);

    expect(pages).toBeGreaterThan(1);
    expect(seen.size).toBe(facts.length);
    expect(seen.has(`vault://fact/${facts.at(-1)?.id}`)).toBe(true);
    expect(lastPage?.complete).toBe(true);
    expect(lastPage?.nextCursor).toBeNull();
  });

  test("rejects continuation across a different query or Vault revision", async () => {
    const facts = Array.from({ length: 75 }, (_, index) =>
      fact(index, {
        statement: `Family archive needle ${index}: ${"context ".repeat(200)}`,
        title: `Archive item ${index}`,
      }),
    );
    const first = await new VaultRecall(facts).search({ query: "family archive needle" });
    expect(first.nextCursor).not.toBeNull();

    await expect(
      new VaultRecall(facts).search({
        query: "different family query",
        cursor: first.nextCursor,
      }),
    ).rejects.toThrow("belongs to another query");
    await expect(
      new VaultRecall([...facts, fact(999)]).search({
        query: "family archive needle",
        cursor: first.nextCursor,
      }),
    ).rejects.toThrow("Vault changed while continuing this search");
  });

  test("uses semantic similarity before the capped lexical boost for cross-wording recall", async () => {
    const postPoolDetails = `${"Ordinary routine context. ".repeat(
      430,
    )} The fast recovery meal after the pool is the useful semantic tail.`;
    const dinner = fact(100, {
      statement: "After pool nights, the family makes chicken-and-rice bowls with cucumber.",
      title: "Post-pool dinner",
      details: postPoolDetails,
      tags: ["recipe", "weeknight dinner"],
      artifactKind: "recipe",
    });
    const gear = fact(101, {
      statement: "Pack goggles, towels, dry clothes, and the blue kickboard.",
      title: "Swim lesson gear",
      details: "The family's reusable packing checklist for lessons at the pool.",
      tags: ["swim", "lessons", "packing"],
      artifactKind: "list",
    });
    const adapter = embeddingAdapter("family-semantic-v1", (text) => {
      if (text === "what should we cook following swim lessons?") return [1, 0, 0];
      if (text.includes("fast recovery meal after the pool")) return [1, 0, 0];
      if (text.includes("Post-pool dinner")) return [0, 1, 0];
      if (text.includes("Swim lesson gear")) return [0.05, 0.99, 0];
      return [0, 0, 1];
    });

    const page = await new VaultRecall([gear, dinner], adapter).search({
      query: "What should we cook following swim lessons?",
    });

    expect(page.retrievalMode).toBe("hybrid");
    expect(page.results.map(({ uri }) => uri)).toEqual([
      `vault://fact/${dinner.id}`,
      `vault://fact/${gear.id}`,
    ]);
  });

  test("embeds every fact and keeps every hybrid result reachable through byte pages", async () => {
    const oversizedDetails = `BEGIN-🧑‍🍳-${"界".repeat(4_000)}-MIDDLE-🌊-${"é".repeat(4_000)}-END-🍲`;
    const facts = Array.from({ length: 75 }, (_, index) =>
      fact(index, {
        statement: `Household archive ${index}: ${"complete retained family context ".repeat(60)}`,
        title: `Semantic archive item ${index}`,
        details: index === 0 ? oversizedDetails : null,
      }),
    );
    const calls: string[][] = [];
    const adapter = embeddingAdapter("family-semantic-v1", () => [1, 0], calls);
    const recall = new VaultRecall(facts, adapter);
    const seen = new Set<string>();
    let cursor: string | null = null;
    let pages = 0;

    do {
      const page = await recall.search({ query: "anything relevant for our household", cursor });
      expect(page.retrievalMode).toBe("hybrid");
      expect(Buffer.byteLength(JSON.stringify(page), "utf8")).toBeLessThanOrEqual(
        VAULT_SEARCH_PAGE_BYTE_BUDGET,
      );
      for (const result of page.results) {
        expect(seen.has(result.uri)).toBe(false);
        seen.add(result.uri);
      }
      cursor = page.nextCursor;
      pages += 1;
    } while (cursor !== null);

    expect(pages).toBeGreaterThan(1);
    expect(seen).toEqual(new Set(facts.map(({ id }) => `vault://fact/${id}`)));
    expect(calls).toHaveLength(2);
    const documentInputs = calls.find((texts) => texts.length > 1) ?? [];
    expect(documentInputs.length).toBeGreaterThan(facts.length);
    expect(
      facts.every((_, index) =>
        documentInputs.some((text) => text.includes(`Semantic archive item ${index}`)),
      ),
    ).toBe(true);
    expect(
      documentInputs.every((text) => Buffer.byteLength(text, "utf8") <= VAULT_EMBEDDING_TEXT_BYTE_BUDGET),
    ).toBe(true);
    const firstFactStart = documentInputs.findIndex((text) =>
      text.includes("Title: Semantic archive item 0"),
    );
    const secondFactStart = documentInputs.findIndex((text) =>
      text.includes("Title: Semantic archive item 1"),
    );
    expect(firstFactStart).toBeGreaterThanOrEqual(0);
    expect(secondFactStart).toBeGreaterThan(firstFactStart + 2);
    expect(documentInputs.slice(firstFactStart, secondFactStart).join("")).toContain(
      `Details: ${oversizedDetails}`,
    );
    await recall.search({ query: "a different household question" });
    expect(calls).toHaveLength(3);
  });

  test("caps the lexical rerank so a pile of matching words cannot swamp meaning", async () => {
    const meaning = fact(210, {
      statement: "This is the meaning-level answer for the household.",
      title: "Semantic answer",
    });
    const lexical = fact(211, {
      statement: "alpha beta gamma delta epsilon zeta eta theta",
      title: "Literal keyword pile",
    });
    const query = "alpha beta gamma delta epsilon zeta eta theta";
    const adapter = embeddingAdapter("family-semantic-v1", (text) => {
      if (text === query || text.includes("Semantic answer")) return [1, 0];
      if (text.includes("Literal keyword pile")) return [0.75, Math.sqrt(1 - 0.75 ** 2)];
      return [0, 1];
    });

    const page = await new VaultRecall([lexical, meaning], adapter).search({ query });

    expect(page.results[0]?.uri).toBe(`vault://fact/${meaning.id}`);
  });

  test("breaks equal hybrid scores by newest revision and then stable fact id", async () => {
    const older = { ...fact(300), updatedAt: "2026-08-27T12:00:00.000Z" };
    const laterId = { ...fact(302), updatedAt: "2026-08-29T12:00:00.000Z" };
    const earlierId = { ...fact(301), updatedAt: "2026-08-29T12:00:00.000Z" };
    const adapter = embeddingAdapter("family-semantic-v1", () => [1, 0]);

    const page = await new VaultRecall([older, laterId, earlierId], adapter).search({
      query: "unrelated semantic request",
    });

    expect(page.results.map(({ uri }) => uri)).toEqual([
      `vault://fact/${earlierId.id}`,
      `vault://fact/${laterId.id}`,
      `vault://fact/${older.id}`,
    ]);
  });

  test("rejects continuation after the embedding version or ordered ranking drifts", async () => {
    const facts = Array.from({ length: 75 }, (_, index) =>
      fact(index, {
        statement: `Family archive needle ${index}: ${"context ".repeat(200)}`,
        title: `Archive item ${index}`,
      }),
    );
    const vectors =
      (preferred: string) =>
      (text: string): readonly number[] => {
        if (text === "family archive needle") return [1, 0];
        return text.includes(preferred) ? [1, 0] : [0, 1];
      };
    const first = await new VaultRecall(
      facts,
      embeddingAdapter("family-semantic-v1", vectors("Archive item 0")),
    ).search({ query: "family archive needle" });
    expect(first.nextCursor).not.toBeNull();

    await expect(
      new VaultRecall(facts, embeddingAdapter("family-semantic-v2", vectors("Archive item 0"))).search({
        query: "family archive needle",
        cursor: first.nextCursor,
      }),
    ).rejects.toThrow("embedding adapter changed");
    await expect(
      new VaultRecall(facts, embeddingAdapter("family-semantic-v1", vectors("Archive item 1"))).search({
        query: "family archive needle",
        cursor: first.nextCursor,
      }),
    ).rejects.toThrow("Vault ranking changed");
    const unavailable: VaultEmbeddingAdapter = {
      version: "family-semantic-v1",
      async embed() {
        throw new Error("temporarily unavailable");
      },
    };
    await expect(
      new VaultRecall(facts, unavailable).search({
        query: "family archive needle",
        cursor: first.nextCursor,
      }),
    ).rejects.toThrow("retrieval mode changed");
  });

  test("fails open to lexical retrieval when semantic embeddings are unavailable", async () => {
    const recipe = fact(200, {
      statement: "The family's preserved lentil soup recipe.",
      title: "Weeknight lentil soup",
      tags: ["recipe", "lentils"],
    });
    const unrelated = fact(201, { statement: "The garage door remote uses a coin battery." });
    const failing: VaultEmbeddingAdapter = {
      version: "offline-semantic-v1",
      async embed() {
        throw new Error("provider unavailable");
      },
    };

    const page = await new VaultRecall([unrelated, recipe], failing).search({
      query: "lentil recipe",
    });

    expect(page.retrievalMode).toBe("lexical_fallback");
    expect(page.results.map(({ uri }) => uri)).toEqual([`vault://fact/${recipe.id}`]);

    const zeroDocument = embeddingAdapter("zero-document-v1", (text) =>
      text === "lentil recipe" ? [1, 0] : [0, 0],
    );
    const zeroQuery = embeddingAdapter("zero-query-v1", (text) =>
      text === "lentil recipe" ? [0, 0] : [1, 0],
    );
    for (const adapter of [zeroDocument, zeroQuery]) {
      const fallback = await new VaultRecall([unrelated, recipe], adapter).search({
        query: "lentil recipe",
      });
      expect(fallback.retrievalMode).toBe("lexical_fallback");
      expect(fallback.results.map(({ uri }) => uri)).toEqual([`vault://fact/${recipe.id}`]);
    }
  });

  test("propagates cancellation without poisoning a later search", async () => {
    const recipe = fact(205, {
      statement: "The family's quick after-practice dinner is vegetable fried rice.",
      title: "After-practice dinner",
      details: "Warm leftover rice with vegetables, egg, and tamari in a large skillet.",
      artifactKind: "recipe",
    });
    let cancelFirstEmbedding = true;
    const adapter: VaultEmbeddingAdapter = {
      version: "family-semantic-v1",
      async embed(texts, signal) {
        signal?.throwIfAborted();
        if (cancelFirstEmbedding) {
          cancelFirstEmbedding = false;
          await new Promise<never>((_resolve, reject) => {
            signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
          });
        }
        return texts.map(() => [1, 0]);
      },
    };
    const recall = new VaultRecall([recipe], adapter);
    const controller = new AbortController();
    const cancelledSearch = recall.search({
      query: "What should we eat after soccer?",
      signal: controller.signal,
    });
    controller.abort(new Error("stop this recall"));

    await expect(cancelledSearch).rejects.toThrow("stop this recall");

    const retry = await recall.search({ query: "What should we eat after soccer?" });
    expect(retry.retrievalMode).toBe("hybrid");
    expect(retry.results[0]?.uri).toBe(`vault://fact/${recipe.id}`);
  });

  test("prefetch exposes only the current corrected meaning and revision", async () => {
    const original = fact(125, {
      statement: "The family's noodle recipe uses soy sauce.",
      title: "Weeknight noodles",
      tags: ["noodles", "soy sauce"],
    });
    const corrected: FactRecord = {
      ...original,
      value: {
        statement: "The family's noodle recipe uses tamari instead of soy sauce.",
        memoryKind: "artifact",
        artifactKind: "recipe",
        title: "Weeknight noodles",
        details: "Use tamari in place of soy sauce.",
        tags: ["noodles", "tamari"],
      },
      correctedAt: "2026-08-29T18:00:00.000Z",
      updatedAt: "2026-08-29T18:00:00.000Z",
    };

    const page = await new VaultRecall([corrected]).search({ query: "weeknight noodle sauce" });
    expect(page.results).toHaveLength(1);
    expect(page.results[0]).toMatchObject({
      uri: `vault://fact/${original.id}`,
      abstract: "Weeknight noodles — The family's noodle recipe uses tamari instead of soy sauce.",
      updatedAt: corrected.updatedAt,
    });
    expect(JSON.stringify(page)).not.toContain("recipe uses soy sauce.");
    expect(
      new VaultRecall([corrected]).read({ uri: `vault://fact/${original.id}`, level: "overview" })?.memory
        .statement,
    ).toBe("The family's noodle recipe uses tamari instead of soy sauce.");
  });

  test("reads complete recipe details and every exact support at full depth", () => {
    const details = `Grandma's exact recipe\n${Array.from(
      { length: 280 },
      () => "Do not shorten this preparation detail.",
    ).join("\n")}`;
    const sources: readonly SourceRecord[] = Array.from({ length: 27 }, (_, index) => ({
      id: `source-${index.toString().padStart(3, "0")}`,
      kind: index % 2 === 0 ? "gmail" : "document",
      visibility: "household",
      ownerAdultId: null,
      label: `Recipe support ${index}`,
      metadata: {
        sourceOrdinal: index,
        exactNote: `${"metadata ".repeat(index + 1)}tail-${index}`,
      },
      occurredAt: `2026-08-${(index + 1).toString().padStart(2, "0")}T12:00:00.000Z`,
    }));
    const recipe = fact(500, {
      statement: "This is the family's preserved cardamom bun recipe.",
      title: "Grandma's cardamom buns",
      details,
      tags: ["recipe", "breakfast", "Grandma"],
      artifactKind: "recipe",
      sources,
    });
    const recall = new VaultRecall([recipe]);
    const uri = `vault://fact/${recipe.id}`;

    const abstract = recall.read({ uri, level: "abstract" });
    expect(abstract?.memory.details).toBeNull();
    expect(abstract?.supports).toEqual([]);

    const overview = recall.read({ uri, level: "overview" });
    expect(overview?.memory).toMatchObject({
      factId: recipe.id,
      statement: "This is the family's preserved cardamom bun recipe.",
      memoryKind: "artifact",
      artifactKind: "recipe",
      title: "Grandma's cardamom buns",
      details,
      tags: ["recipe", "breakfast", "Grandma"],
      visibility: "household",
    });
    expect(overview?.supports).toEqual([]);

    const full = recall.read({ uri, level: "full" });
    expect(full?.memory.details).toBe(details);
    expect(full?.supports).toHaveLength(sources.length);
    expect(full?.supports).toEqual(
      sources.map((source) => ({
        sourceId: source.id,
        kind: source.kind,
        label: source.label,
        visibility: source.visibility,
        occurredAt: source.occurredAt,
        metadata: source.metadata,
      })),
    );
    expect(full?.supports.at(-1)?.metadata).toEqual(sources.at(-1)?.metadata);
  });

  test("returns an opaque reusable file URI only through its authorized fact", async () => {
    const artifact: FileArtifactReference = {
      artifactId: "10000000-0000-4000-8000-000000000001",
      workId: "20000000-0000-4000-8000-000000000002",
      filename: "field-trip-form.pdf",
      mimeType: "application/pdf",
      byteLength: 2_048,
      sha256: "a".repeat(64),
    };
    const saved = fact(700, {
      statement: "The reusable field-trip form for school.",
      title: "Field-trip form",
      details: "The original school form, saved for later completion or submission.",
      artifactKind: "reference",
      files: [artifact],
    });
    const recall = new VaultRecall([saved]);
    const factUri = `vault://fact/${saved.id}`;
    const fileUri = `${factUri}/file/${artifact.artifactId}`;

    expect((await recall.search({ query: "field trip form" })).results[0]?.uri).toBe(factUri);
    expect(recall.read({ uri: factUri, level: "abstract" })?.memory.files).toEqual([]);
    expect(recall.read({ uri: factUri, level: "overview" })?.memory.files).toEqual([
      {
        uri: fileUri,
        artifactId: artifact.artifactId,
        filename: artifact.filename,
        mimeType: artifact.mimeType,
        byteLength: artifact.byteLength,
        sha256: artifact.sha256,
      },
    ]);
    expect(recall.resolveFile(fileUri)).toEqual({ factId: saved.id, uri: fileUri, artifact });
    expect(new VaultRecall([]).resolveFile(fileUri)).toBeNull();
  });
});

function embeddingAdapter(
  version: string,
  vectorFor: (text: string) => readonly number[],
  calls?: string[][],
): VaultEmbeddingAdapter {
  return {
    version,
    async embed(texts) {
      calls?.push([...texts]);
      return texts.map(vectorFor);
    },
  };
}

function fact(
  index: number,
  overrides: Readonly<{
    statement?: string;
    title?: string | null;
    details?: string | null;
    tags?: readonly string[];
    artifactKind?: "recipe" | "list" | "plan" | "note" | "reference" | "other" | null;
    sources?: readonly SourceRecord[];
    files?: readonly FileArtifactReference[];
  }> = {},
): FactRecord {
  const artifactKind = overrides.artifactKind ?? null;
  return {
    id: `fact-${index.toString().padStart(4, "0")}`,
    householdId: "household-1",
    subjectPersonId: null,
    kind: "general",
    slot: `memory:archive:${index}`,
    label: overrides.title ?? `Memory ${index}`,
    value: {
      statement: overrides.statement ?? `General family memory ${index}`,
      memoryKind: artifactKind === null ? "fact" : "artifact",
      artifactKind,
      title: overrides.title ?? null,
      details: overrides.details ?? null,
      tags: overrides.tags ? [...overrides.tags] : [],
      ...(overrides.files ? { files: [...overrides.files] } : {}),
    },
    visibility: "household",
    ownerAdultId: null,
    sources: overrides.sources ?? [],
    correctedAt: null,
    updatedAt: `2026-08-28T12:${(index % 60).toString().padStart(2, "0")}:00.000Z`,
  };
}
