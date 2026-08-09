import { describe, expect, it } from "vitest";
import {
  assertDependentEditVersions,
  dependentRosterReviewAfterMutation,
} from "../../src/application/florence-application.js";

describe("dependent roster mutation policy", () => {
  it("accepts only the exact household and intake versions shown with the child", () => {
    const current = { rosterVersion: 4, intakeVersion: 7 };
    expect(() => assertDependentEditVersions(current, current)).not.toThrow();
    expect(() => assertDependentEditVersions(current, { rosterVersion: 3, intakeVersion: 7 })).toThrow(
      /changed while you were editing/u,
    );
    expect(() => assertDependentEditVersions(current, { rosterVersion: 4, intakeVersion: 6 })).toThrow(
      /changed while you were editing/u,
    );
  });

  it("refreshes an established roster review but never invents one during initial intake", () => {
    const changedAt = new Date("2026-08-08T20:00:00.000Z");
    expect(
      dependentRosterReviewAfterMutation({
        wasReviewed: true,
        actorPersonId: "10000000-0000-4000-8000-000000000001",
        rosterVersion: 5,
        changedAt,
      }),
    ).toEqual({
      reviewedByPersonId: "10000000-0000-4000-8000-000000000001",
      reviewedAt: changedAt,
      rosterVersion: 5,
    });
    expect(
      dependentRosterReviewAfterMutation({
        wasReviewed: false,
        actorPersonId: "10000000-0000-4000-8000-000000000001",
        rosterVersion: 5,
        changedAt,
      }),
    ).toEqual({ reviewedByPersonId: null, reviewedAt: null, rosterVersion: null });
  });
});
