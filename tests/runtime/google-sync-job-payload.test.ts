import { describe, expect, it } from "vitest";
import {
  buildGmailMessageJobPayload,
  GmailBackfillPayloadSchema,
  GmailMessagePayloadSchema,
} from "../../src/runtime/google-sync.js";

describe("Google sync job payloads", () => {
  it("does not leak backfill-only fields into a Gmail message job", () => {
    const backfill = GmailBackfillPayloadSchema.parse({
      integrationId: "10000000-0000-4000-8000-000000000001",
      personId: "10000000-0000-4000-8000-000000000002",
      integrationControlEpoch: 1,
      personControlEpoch: 1,
      stage: "newest_30_days",
      afterExclusive: "2026-07-07T00:00:00.000Z",
      beforeOrEqual: "2026-08-07T00:00:00.000Z",
      pageToken: "next-page",
      runKey: "activation-1",
    });

    const message = buildGmailMessageJobPayload(backfill, "gmail-message", 110);

    expect(() => GmailMessagePayloadSchema.parse(message)).not.toThrow();
    expect(Object.keys(message).sort()).toEqual([
      "integrationControlEpoch",
      "integrationId",
      "messageId",
      "personControlEpoch",
      "personId",
      "sourcePriority",
    ]);
  });
});
