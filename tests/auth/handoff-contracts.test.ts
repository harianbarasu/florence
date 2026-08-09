import { describe, expect, it } from "vitest";
import {
  CreateHandoffInputSchema,
  GOOGLE_CONNECT_HANDOFF_TTL_SECONDS,
} from "../../src/modules/auth/contracts.js";
import { completedHandoffRedirect } from "../../src/server.js";

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

  it("routes web sign-in only to an exact app-owned private control path", () => {
    const session = {
      sessionId: "10000000-0000-4000-8000-000000000004",
      personId: baseInput.personId,
      sessionToken: "session-token",
      csrfToken: "csrf-token",
      idleExpiresAt: new Date("2026-08-08T02:00:00.000Z"),
      absoluteExpiresAt: new Date("2026-08-09T02:00:00.000Z"),
      assuranceKind: "base" as const,
      assuranceContext: { returnPath: "/sources" },
      assuranceExpiresAt: null,
    };

    expect(completedHandoffRedirect("web_sign_in", session)).toBe("/sources");
    expect(
      completedHandoffRedirect("web_sign_in", {
        ...session,
        assuranceContext: { returnPath: "/people" },
      }),
    ).toBe("/people");
    expect(
      completedHandoffRedirect("web_sign_in", {
        ...session,
        assuranceContext: { returnPath: "/sources?connected=1" },
      }),
    ).toBe("/people");
    expect(completedHandoffRedirect("web_sign_in", { ...session, assuranceContext: {} })).toBe("/people");
  });
});
