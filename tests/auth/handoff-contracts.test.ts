import { describe, expect, it } from "vitest";
import {
  CreateHandoffInputSchema,
  GOOGLE_CONNECT_HANDOFF_TTL_SECONDS,
} from "../../src/modules/auth/contracts.js";

const baseInput = {
  personId: "10000000-0000-4000-8000-000000000001",
  privateIdentityId: "10000000-0000-4000-8000-000000000002",
  privateConversationId: "10000000-0000-4000-8000-000000000003",
  context: {},
};

describe("handoff expiry contracts", () => {
  it("permits a 30-minute Google connection without extending other private handoffs", () => {
    expect(
      CreateHandoffInputSchema.safeParse({
        ...baseInput,
        purpose: "google_connect",
        expiresInSeconds: GOOGLE_CONNECT_HANDOFF_TTL_SECONDS,
      }).success,
    ).toBe(true);
    expect(
      CreateHandoffInputSchema.safeParse({
        ...baseInput,
        purpose: "web_sign_in",
        expiresInSeconds: GOOGLE_CONNECT_HANDOFF_TTL_SECONDS,
      }).success,
    ).toBe(false);
  });
});
