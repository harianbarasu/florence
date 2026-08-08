import { describe, expect, it } from "vitest";
import type { Database } from "../../src/db/client.js";
import { PostgresFlorenceQueries } from "../../src/modules/queries/postgres-florence-queries.js";
import { SecretBox } from "../../src/shared/crypto.js";

describe("private conversation recovery", () => {
  it("does not move an exhausted conversational request into the web attention plane", async () => {
    const personId = "10000000-0000-4000-8000-000000000001";
    const captured: { text: string; values: readonly unknown[] }[] = [];
    const database = (async (strings: TemplateStringsArray, ...values: unknown[]) => {
      const text = strings.join("?");
      captured.push({ text, values });
      return [];
    }) as unknown as Database;
    const key = Buffer.alloc(32, 7).toString("base64");
    const queries = new PostgresFlorenceQueries(
      database,
      new SecretBox("test", JSON.stringify({ test: key })),
      30,
    );

    const home = await queries.home(personId);

    expect(home.items).not.toContainEqual(
      expect.objectContaining({
        id: expect.stringMatching(/^linq-request-attention:/u),
      }),
    );
    expect(captured).not.toContainEqual(
      expect.objectContaining({
        text: expect.stringContaining(
          "job.job_kind in ('orchestrate.linq_message', 'orchestrate.linq_observation')",
        ),
      }),
    );
  });
});
