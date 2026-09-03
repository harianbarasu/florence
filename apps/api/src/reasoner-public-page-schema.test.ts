import { describe, expect, test } from "vitest";
import { FlorenceReasoner } from "./reasoner.js";
import { fakeStream, foregroundInput, inertReads, ordinaryDecision } from "./reasoner-tool-loops.test-kit.js";

describe("Florence public-page capability schema", () => {
  test("publishes every strict function property as required", async () => {
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
      async runPublicPage() {
        throw new Error("The model should not call the public-page reader in this test");
      },
    });

    const tools = (requests[0]?.tools ?? []) as {
      type: string;
      name?: string;
      strict?: boolean;
      parameters?: { properties?: Record<string, unknown>; required?: string[] };
    }[];
    const publicPage = tools.find((tool) => tool.type === "function" && tool.name === "read_public_page");
    expect(publicPage?.strict).toBe(true);
    expect(publicPage?.parameters?.required?.toSorted()).toEqual(
      Object.keys(publicPage?.parameters?.properties ?? {}).toSorted(),
    );
  });
});
