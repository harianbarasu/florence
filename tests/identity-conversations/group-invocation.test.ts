import { describe, expect, it } from "vitest";
import { leadingGroupInvocation, provenReplyGroupInvocation } from "../../src/modules/conversations/index.js";

describe("group invocation admission", () => {
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
    expect(leadingGroupInvocation("Does Florence know what time pickup is?")).toBeNull();
    expect(leadingGroupInvocation("What time is pickup?")).toBeNull();
    expect(leadingGroupInvocation("Florence?")).toBeNull();
    expect(leadingGroupInvocation("Florence is in this chat now")).toBeNull();
  });

  it("admits bounded text only after a reply target is proven locally", () => {
    expect(provenReplyGroupInvocation(" What should we do next? ")).toEqual({
      basis: "proven_reply",
      requestText: "What should we do next?",
    });
    expect(provenReplyGroupInvocation("  ")).toBeNull();
    expect(provenReplyGroupInvocation("x".repeat(10_001))).toBeNull();
  });
});
