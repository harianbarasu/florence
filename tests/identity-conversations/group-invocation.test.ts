import { describe, expect, it } from "vitest";
import { leadingGroupInvocation } from "../../src/modules/conversations/index.js";

describe("observe-only group invocation", () => {
  it("accepts natural leading-name requests", () => {
    expect(leadingGroupInvocation("Florence can you summarize this?")).toEqual({
      basis: "leading_address",
      requestText: "can you summarize this?",
    });
    expect(leadingGroupInvocation("Hey Florence, what time is pickup?")?.requestText).toBe(
      "what time is pickup?",
    );
    expect(leadingGroupInvocation("Florence here is my wife Kendall")?.requestText).toBe(
      "here is my wife Kendall",
    );
    expect(leadingGroupInvocation("Florence this is our babysitter Jenny")?.requestText).toBe(
      "this is our babysitter Jenny",
    );
  });

  it("rejects mentions, name-only messages, and ordinary leading-name statements", () => {
    expect(leadingGroupInvocation("I think Florence can help")).toBeNull();
    expect(leadingGroupInvocation("Florence?")).toBeNull();
    expect(leadingGroupInvocation("Florence is in this chat now")).toBeNull();
  });
});
