import { describe, expect, it } from "vitest";
import type { Database } from "../../src/db/client.js";
import { PostgresFlorenceQueries } from "../../src/modules/queries/postgres-florence-queries.js";
import { SecretBox } from "../../src/shared/crypto.js";

describe("private model attention", () => {
  it("shows only a generic exact-person item for an exhausted conversational request", async () => {
    const personId = "10000000-0000-4000-8000-000000000001";
    const jobId = "10000000-0000-4000-8000-000000000002";
    const updatedAt = new Date("2026-08-07T20:00:00.000Z");
    const captured: { text: string; values: readonly unknown[] }[] = [];
    const database = (async (strings: TemplateStringsArray, ...values: unknown[]) => {
      const text = strings.join("?");
      captured.push({ text, values });
      return text.includes("job.job_kind in ('orchestrate.linq_message', 'orchestrate.linq_observation')")
        ? [{ id: jobId, updated_at: updatedAt }]
        : [];
    }) as unknown as Database;
    const key = Buffer.alloc(32, 7).toString("base64");
    const queries = new PostgresFlorenceQueries(
      database,
      new SecretBox("test", JSON.stringify({ test: key })),
      30,
    );

    const home = await queries.home(personId);

    expect(home.items).toContainEqual({
      id: `linq-request-attention:${jobId}`,
      kind: "request",
      title: "Florence needs you to retry a request",
      detail: "Send that request again in your private Florence chat so I can finish it.",
      urgency: "soon",
      changedAt: updatedAt.toISOString(),
    });
    const attentionQuery = captured.find((query) =>
      query.text.includes("job.job_kind in ('orchestrate.linq_message', 'orchestrate.linq_observation')"),
    );
    expect(attentionQuery?.text).toContain(
      "job.job_kind in ('orchestrate.linq_message', 'orchestrate.linq_observation')",
    );
    expect(attentionQuery?.text).toContain("job.status = 'attention'");
    expect(attentionQuery?.text).toContain("job.person_id = ?");
    expect(attentionQuery?.text).toContain("recovered.conversation_id = job.conversation_id");
    expect(attentionQuery?.text).toContain("recovered.status = 'succeeded'");
    expect(attentionQuery?.values).toEqual([personId]);
  });
});
