import { describe, expect, it } from "vitest";
import {
  buildRoutinePatternCandidate,
  deterministicRoutinePatternConfirmation,
  isOneTimeRoutinePatternResponse,
  routinePatternPrompt,
  selectCurrentRoutinePatternHolder,
} from "../../src/modules/coordination/index.js";

const IDS = {
  household: "10000000-0000-4000-8000-000000000001",
  conversation: "10000000-0000-4000-8000-000000000002",
  epoch: "10000000-0000-4000-8000-000000000003",
  holder: "10000000-0000-4000-8000-000000000004",
  membership: "10000000-0000-4000-8000-000000000005",
  source: "10000000-0000-4000-8000-000000000006",
} as const;

describe("conversational routine patterns", () => {
  it("builds a pending weekly pattern only from explicit recurring source language", () => {
    const candidate = buildRoutinePatternCandidate({
      sourceMessage: "Jenny usually picks Violet up every Tuesday at 3 PM.",
      proposal: {
        title: "Violet's Tuesday pickup",
        minimumSharedMeaning: "Jenny handles Violet's Tuesday 3 PM pickup",
        semanticTiming: "Every Tuesday at 3 PM",
        timeZone: "America/Los_Angeles",
        eventAt: "2026-08-11T22:00:00Z",
        deadlineAt: null,
        proposedHolderPersonId: IDS.holder,
        evidence: [{ sourceRevisionId: IDS.source, support: "Explicit recurring statement" }],
        uncertainties: [],
        confidence: 0.94,
      },
      household: { id: IDS.household, controlEpoch: 3 },
      destination: {
        conversationId: IDS.conversation,
        participantEpochId: IDS.epoch,
        participantSetDigest: "a".repeat(64),
        audience: "group",
        authorityVersion: 9,
      },
      holder: {
        personId: IDS.holder,
        personControlEpoch: 2,
        membershipId: IDS.membership,
        membershipVersion: 4,
      },
      sourceRevisionIds: [IDS.source],
      timePlanFallbackTimeZone: "America/Los_Angeles",
      now: new Date("2026-08-09T19:00:00Z"),
    });

    expect(candidate).toMatchObject({
      recurrence: { kind: "weekly", weekdays: [2], startsOn: "2026-08-11" },
      timePlan: { timeZone: "America/Los_Angeles", event: { time: "15:00" } },
      holder: { personId: IDS.holder },
      sourceRevisionIds: [IDS.source],
    });
    if (!candidate) throw new Error("Expected a routine candidate");
    expect(routinePatternPrompt(candidate, "Jenny")).toContain(
      "Schedule: Every Tuesday at 3:00 PM (America/Los_Angeles)",
    );
    expect(routinePatternPrompt(candidate, "Jenny")).toContain("Starts: 2026-08-11");
    expect(routinePatternPrompt(candidate, "Jenny")).toContain("Exceptions: none");
  });

  it("does not turn a one-time pickup statement into a standing candidate", () => {
    const candidate = buildRoutinePatternCandidate({
      sourceMessage: "Jenny can pick Violet up this Tuesday at 3 PM.",
      proposal: {
        title: "Violet's Tuesday pickup",
        minimumSharedMeaning: "Jenny handles Violet's Tuesday 3 PM pickup",
        semanticTiming: "Tuesday at 3 PM",
        timeZone: "America/Los_Angeles",
        eventAt: "2026-08-11T22:00:00Z",
        deadlineAt: null,
        proposedHolderPersonId: IDS.holder,
        evidence: [{ sourceRevisionId: IDS.source, support: "One-time statement" }],
        uncertainties: [],
        confidence: 0.99,
      },
      household: { id: IDS.household, controlEpoch: 3 },
      destination: {
        conversationId: IDS.conversation,
        participantEpochId: IDS.epoch,
        participantSetDigest: "a".repeat(64),
        audience: "group",
        authorityVersion: 9,
      },
      holder: {
        personId: IDS.holder,
        personControlEpoch: 2,
        membershipId: IDS.membership,
        membershipVersion: 4,
      },
      sourceRevisionIds: [IDS.source],
      timePlanFallbackTimeZone: "America/Los_Angeles",
      now: new Date("2026-08-09T19:00:00Z"),
    });

    expect(candidate).toBeNull();
  });

  it("accepts an exact yes but not language limited to one occurrence", () => {
    expect(deterministicRoutinePatternConfirmation("yes")).toBe("accept");
    expect(deterministicRoutinePatternConfirmation("No, not every week")).toBe("reject");
    expect(deterministicRoutinePatternConfirmation("I can do this Tuesday")).toBe("ambiguous");
    expect(isOneTimeRoutinePatternResponse("I can do this Tuesday")).toBe(true);
  });

  it("derives the interval and clock only from the exact source text", () => {
    const common = {
      proposal: {
        title: "Violet's pickup",
        minimumSharedMeaning: "Jenny handles Violet's pickup",
        semanticTiming: "Every Tuesday at 3 PM",
        timeZone: "America/New_York",
        eventAt: "2026-08-11T22:00:00Z",
        deadlineAt: null,
        proposedHolderPersonId: IDS.holder,
        evidence: [{ sourceRevisionId: IDS.source, support: "Recurring statement" }],
        uncertainties: [],
        confidence: 0.94,
      },
      household: { id: IDS.household, controlEpoch: 3 },
      destination: {
        conversationId: IDS.conversation,
        participantEpochId: IDS.epoch,
        participantSetDigest: "a".repeat(64),
        audience: "group" as const,
        authorityVersion: 9,
      },
      holder: {
        personId: IDS.holder,
        personControlEpoch: 2,
        membershipId: IDS.membership,
        membershipVersion: 4,
      },
      sourceRevisionIds: [IDS.source],
      timePlanFallbackTimeZone: "America/Los_Angeles",
      now: new Date("2026-08-09T19:00:00Z"),
    };
    const everyOther = buildRoutinePatternCandidate({
      ...common,
      sourceMessage: "Jenny picks Violet up every other Tuesday at 3 PM.",
    });
    expect(everyOther).toMatchObject({
      recurrence: { weekdays: [2], intervalWeeks: 2 },
      timePlan: { timeZone: "America/Los_Angeles", event: { time: "15:00" } },
    });
    expect(
      buildRoutinePatternCandidate({
        ...common,
        sourceMessage: "Jenny handles pickup every Tuesday.",
      }),
    ).toBeNull();
  });

  it("selects only a holder who is exactly present in the writable source group", () => {
    expect(selectCurrentRoutinePatternHolder(IDS.holder, [IDS.holder, IDS.membership])).toBe(IDS.holder);
    expect(selectCurrentRoutinePatternHolder(IDS.holder, [IDS.membership])).toBeNull();
    expect(selectCurrentRoutinePatternHolder(IDS.holder, [IDS.holder, IDS.holder])).toBeNull();
  });
});
