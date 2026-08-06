import { describe, expect, it } from "vitest";
import {
  CoordinationError,
  createCoverageLoop,
  transitionCoverage,
} from "../../src/modules/coordination/index.js";
import { destination, IDS, timing } from "./fixtures.js";

function openLoop() {
  return createCoverageLoop({
    loopId: IDS.loop,
    householdId: IDS.household,
    minimumSharedMeaning: "Wednesday pickup",
    unresolvedFacts: [],
    proposedHolderPersonId: IDS.holder,
    timing: timing(),
    destination,
    sourceEvidenceRefs: ["evidence:group-message"],
    occurredAt: "2026-08-05T18:00:00Z",
  });
}

describe("coverage state machine", () => {
  it("keeps partial answers provisional and applies the resolved timing when the last fact arrives", () => {
    const provisional = createCoverageLoop({
      loopId: IDS.loop,
      householdId: IDS.household,
      minimumSharedMeaning: "School pickup",
      unresolvedFacts: ["which day", "pickup time"],
      proposedHolderPersonId: IDS.holder,
      timing: timing(),
      destination,
      sourceEvidenceRefs: ["evidence:group-message"],
      occurredAt: "2026-08-05T18:00:00Z",
    });

    const partiallyResolved = transitionCoverage(provisional, {
      kind: "resolve_facts",
      transitionId: IDS.transition1,
      expectedVersion: 1,
      actorPersonId: IDS.actor,
      occurredAt: "2026-08-05T18:01:00Z",
      minimumSharedMeaning: "Wednesday school pickup",
      unresolvedFacts: ["pickup time"],
      proposedHolderPersonId: IDS.holder,
      timing: timing(),
      evidenceRefs: ["evidence:wednesday"],
    }).loop;

    const resolvedTiming = {
      ...timing(),
      localDate: "2026-08-12",
      eventAt: "2026-08-12T22:00:00Z",
      deadlineAt: "2026-08-12T21:30:00Z",
      lastResponsibleAt: "2026-08-12T21:30:00Z",
    };
    const resolved = transitionCoverage(partiallyResolved, {
      kind: "resolve_facts",
      transitionId: IDS.transition2,
      expectedVersion: 2,
      actorPersonId: IDS.actor,
      occurredAt: "2026-08-05T18:02:00Z",
      minimumSharedMeaning: "Wednesday school pickup at 3 PM",
      unresolvedFacts: [],
      proposedHolderPersonId: IDS.holder,
      timing: resolvedTiming,
      evidenceRefs: ["evidence:three-pm"],
    });

    expect(partiallyResolved).toMatchObject({ state: "provisional", unresolvedFacts: ["pickup time"] });
    expect(resolved.loop).toMatchObject({
      state: "open",
      unresolvedFacts: [],
      timing: { eventAt: "2026-08-12T22:00:00Z" },
    });
  });

  it("requires the proposed person to explicitly acknowledge their own coverage", () => {
    const requested = transitionCoverage(openLoop(), {
      kind: "request_coverage",
      transitionId: IDS.transition1,
      expectedVersion: 1,
      actorPersonId: IDS.actor,
      requestedPersonId: IDS.holder,
      occurredAt: "2026-08-05T18:01:00Z",
      evidenceRefs: [],
    }).loop;

    expect(() =>
      transitionCoverage(requested, {
        kind: "acknowledge_coverage",
        transitionId: IDS.transition2,
        expectedVersion: 2,
        actorPersonId: IDS.actor,
        acknowledgment: "explicit_self",
        visibility: "shared",
        occurredAt: "2026-08-05T18:02:00Z",
        evidenceRefs: [],
      }),
    ).toThrowError(expect.objectContaining({ code: "not_proposed_holder" }));

    const accepted = transitionCoverage(requested, {
      kind: "acknowledge_coverage",
      transitionId: IDS.transition2,
      expectedVersion: 2,
      actorPersonId: IDS.holder,
      acknowledgment: "explicit_self",
      visibility: "shared",
      occurredAt: "2026-08-05T18:03:00Z",
      evidenceRefs: ["evidence:self-ack"],
    });

    expect(accepted.loop).toMatchObject({
      state: "covered",
      version: 3,
      acknowledgment: { personId: IDS.holder, kind: "explicit_self" },
    });
    expect(accepted.minimumSharedStatus?.holderPersonId).toBe(IDS.holder);
  });

  it("keeps a private decline and reason out of the minimum shared status", () => {
    const requested = transitionCoverage(openLoop(), {
      kind: "request_coverage",
      transitionId: IDS.transition1,
      expectedVersion: 1,
      actorPersonId: IDS.actor,
      requestedPersonId: IDS.holder,
      occurredAt: "2026-08-05T18:01:00Z",
      evidenceRefs: [],
    }).loop;
    const declined = transitionCoverage(requested, {
      kind: "decline_coverage",
      transitionId: IDS.transition2,
      expectedVersion: 2,
      actorPersonId: IDS.holder,
      visibility: "private",
      privateReason: "I have a medical appointment",
      occurredAt: "2026-08-05T18:02:00Z",
      evidenceRefs: [],
    });

    expect(declined.loop).toMatchObject({ state: "open", proposedHolderPersonId: null });
    expect(declined.minimumSharedStatus).toMatchObject({
      kind: "coverage_still_open",
      holderPersonId: null,
    });
    const shared = JSON.stringify(declined.minimumSharedStatus);
    expect(shared).not.toContain(IDS.holder);
    expect(shared).not.toContain("medical appointment");
  });

  it("reopens contradicted coverage and requires a fresh self-acknowledgment", () => {
    const requested = transitionCoverage(openLoop(), {
      kind: "request_coverage",
      transitionId: IDS.transition1,
      expectedVersion: 1,
      actorPersonId: IDS.actor,
      requestedPersonId: IDS.holder,
      occurredAt: "2026-08-05T18:01:00Z",
      evidenceRefs: [],
    }).loop;
    const covered = transitionCoverage(requested, {
      kind: "acknowledge_coverage",
      transitionId: IDS.transition2,
      expectedVersion: 2,
      actorPersonId: IDS.holder,
      acknowledgment: "explicit_self",
      visibility: "private",
      occurredAt: "2026-08-05T18:02:00Z",
      evidenceRefs: [],
    }).loop;
    const atRisk = transitionCoverage(covered, {
      kind: "record_risk",
      transitionId: IDS.transition3,
      expectedVersion: 3,
      actorPersonId: null,
      proposedHolderPersonId: IDS.holder,
      occurredAt: "2026-08-05T18:03:00Z",
      evidenceRefs: ["evidence:calendar-conflict"],
    });

    expect(atRisk.loop).toMatchObject({
      state: "at_risk",
      acknowledgment: null,
      attentionCycle: 2,
    });

    const reaffirmed = transitionCoverage(atRisk.loop, {
      kind: "acknowledge_coverage",
      transitionId: IDS.transition4,
      expectedVersion: 4,
      actorPersonId: IDS.holder,
      acknowledgment: "explicit_self",
      visibility: "private",
      occurredAt: "2026-08-05T18:04:00Z",
      evidenceRefs: ["evidence:reconfirmed"],
    });
    expect(reaffirmed.loop.state).toBe("covered");
    expect(reaffirmed.minimumSharedStatus?.holderPersonId).toBeNull();
  });

  it("cannot expire an uncovered loop before its last responsible moment", () => {
    expect(() =>
      transitionCoverage(openLoop(), {
        kind: "expire_uncovered",
        transitionId: IDS.transition1,
        expectedVersion: 1,
        occurredAt: "2026-08-06T20:00:00Z",
        evidenceRefs: [],
      }),
    ).toThrowError(new CoordinationError("too_early_to_expire"));
  });
});
