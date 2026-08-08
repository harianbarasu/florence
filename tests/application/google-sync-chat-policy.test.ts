import { describe, expect, it } from "vitest";
import {
  googleSyncChatDisposition,
  privateSourceJobBlocksGoogleReadiness,
} from "../../src/application/google-sync-coordinator.js";

describe("Google sync chat policy", () => {
  it("messages only when the parent must reauthorize Google", () => {
    expect(googleSyncChatDisposition("reauth_required", false)).toBe("notify_reauth");
    expect(googleSyncChatDisposition("error", false)).toBe("wait_for_recovery");
    expect(googleSyncChatDisposition("active", true)).toBe("wait_for_recovery");
    expect(googleSyncChatDisposition("active", false)).toBe("continue");
  });

  it("blocks on unfinished private-source work but not a bounded model-frontier exception", () => {
    const boundedError = "private_source_not_ready_model_frontier_incomplete";

    expect([
      privateSourceJobBlocksGoogleReadiness("pending", boundedError),
      privateSourceJobBlocksGoogleReadiness("retry", boundedError),
      privateSourceJobBlocksGoogleReadiness("leased", boundedError),
      privateSourceJobBlocksGoogleReadiness("attention", null),
      privateSourceJobBlocksGoogleReadiness("dead", "private_source_failed"),
    ]).toEqual([true, true, true, true, true]);
    expect([
      privateSourceJobBlocksGoogleReadiness("attention", boundedError),
      privateSourceJobBlocksGoogleReadiness("dead", boundedError),
      privateSourceJobBlocksGoogleReadiness("succeeded", null),
    ]).toEqual([false, false, false]);
  });
});
