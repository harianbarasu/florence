import { describe, expect, test } from "vitest";
import { FlorenceReasoner } from "./reasoner.js";
import { fakeStream, foregroundInput, inertReads, ordinaryDecision } from "./reasoner-tool-loops.test-kit.js";

describe("Florence Google Workspace durable routing", () => {
  test("tells the foreground model to start durable work for requested provider changes", async () => {
    const requests: Record<string, unknown>[] = [];
    const reasoner = new FlorenceReasoner({ apiKey: "test-key", model: "test-model" }, {
      responses: {
        stream: (request: Record<string, unknown>) => {
          requests.push(request);
          return fakeStream({
            status: "completed",
            output_parsed: ordinaryDecision(),
            output: [],
          });
        },
      },
    } as never);

    await reasoner.decide(foregroundInput(), {
      ...inertReads(),
      async runGoogleWorkspace() {
        throw new Error("The model should not call Google Workspace in this test");
      },
    });

    const tools = (requests[0]?.tools ?? []) as {
      type: string;
      name?: string;
      description?: string;
    }[];
    for (const name of [
      "gmail_work",
      "drive_work",
      "contacts_work",
      "docs_work",
      "sheets_work",
      "slides_work",
      "tasks_work",
    ]) {
      const tool = tools.find((candidate) => candidate.type === "function" && candidate.name === name);
      expect(tool?.description, name).toContain("create familyWork");
      expect(tool?.description, name).toContain("Do not say");
    }
  });
});
