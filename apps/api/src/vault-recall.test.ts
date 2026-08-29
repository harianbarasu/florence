import type { FileArtifactReference } from "@florence/artifacts";
import type { FactRecord, SourceRecord } from "@florence/database";
import { describe, expect, test } from "vitest";
import { VAULT_SEARCH_PAGE_BYTE_BUDGET, VaultRecall, type VaultSearchPage } from "./vault-recall.js";

describe("VaultRecall", () => {
  test("continues through every matching memory without losing tail facts", () => {
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
      const page = recall.search({ query: "family archive needle", cursor });
      expect(Buffer.byteLength(JSON.stringify(page), "utf8")).toBeLessThanOrEqual(
        VAULT_SEARCH_PAGE_BYTE_BUDGET,
      );
      expect(page.query).toBe("family archive needle");
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

  test("rejects continuation across a different query or Vault revision", () => {
    const facts = Array.from({ length: 75 }, (_, index) =>
      fact(index, {
        statement: `Family archive needle ${index}: ${"context ".repeat(200)}`,
        title: `Archive item ${index}`,
      }),
    );
    const first = new VaultRecall(facts).search({ query: "family archive needle" });
    expect(first.nextCursor).not.toBeNull();

    expect(() =>
      new VaultRecall(facts).search({ query: "different family query", cursor: first.nextCursor }),
    ).toThrow("belongs to another query");
    expect(() =>
      new VaultRecall([...facts, fact(999)]).search({
        query: "family archive needle",
        cursor: first.nextCursor,
      }),
    ).toThrow("Vault changed while continuing this search");
  });

  test("prefetch exposes only the current corrected meaning and revision", () => {
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

    const page = new VaultRecall([corrected]).search({ query: "weeknight noodle sauce" });
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

  test("returns an opaque reusable file URI only through its authorized fact", () => {
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

    expect(recall.search({ query: "field trip form" }).results[0]?.uri).toBe(factUri);
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
