import { describe, expect, it } from "vitest";
import { googleSyncChatDisposition } from "../../src/application/google-sync-coordinator.js";

describe("Google sync chat policy", () => {
  it("messages only when the parent must reauthorize Google", () => {
    expect(googleSyncChatDisposition("reauth_required", false)).toBe("notify_reauth");
    expect(googleSyncChatDisposition("error", false)).toBe("wait_for_recovery");
    expect(googleSyncChatDisposition("active", true)).toBe("wait_for_recovery");
    expect(googleSyncChatDisposition("active", false)).toBe("continue");
  });
});
