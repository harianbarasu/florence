import { describe, expect, it } from "vitest";
import {
  ApplicationProjectionSchema,
  type HouseholdApplicationSnapshot,
} from "../../src/application/index.js";
import {
  DurableMemorySchema,
  FamilyEpisodeSchema,
  HouseholdAggregateSchema,
  PolicyRecordSchema,
} from "../../src/domain/index.js";
import { PrivateControlCatalog, privateControlId } from "../../src/infrastructure/private-control-catalog.js";
import { ADULT_A, ADULT_B, aggregate, HOUSEHOLD_ID } from "../application/fixtures.js";

const catalog = new PrivateControlCatalog();
const T0 = "2026-08-01T08:00:00Z";
const T1 = "2026-08-02T08:00:00Z";
const T2 = "2026-08-03T08:00:00Z";
const DIGEST_A = `sha256:${"a".repeat(64)}`;
const DIGEST_B = `sha256:${"b".repeat(64)}`;

function personalEvidence(adultId: typeof ADULT_A | typeof ADULT_B, suffix: string) {
  return {
    evidenceId: `evidence_${suffix}`,
    source: "gmail" as const,
    sourceRef: `private_source_${suffix}`,
    scope: { kind: "personal" as const, adultId },
    observedAt: T0,
    revision: 1,
    contentDigest: DIGEST_A,
  };
}

function memory(input: {
  id: string;
  adultId?: typeof ADULT_A | typeof ADULT_B;
  scope?: "personal" | "household";
  statement: string;
  status?: "active" | "revoked" | "superseded";
  expiresAt?: string;
}) {
  const scope = input.scope ?? "personal";
  const adultId = input.adultId ?? ADULT_A;
  return DurableMemorySchema.parse({
    memoryId: input.id,
    householdId: HOUSEHOLD_ID,
    kind: "fact",
    statement: input.statement,
    scope: scope === "household" ? { kind: "household" } : { kind: "personal", adultId },
    sourceClass: "family.fact",
    evidence:
      scope === "household"
        ? [
            {
              evidenceId: `evidence_${input.id}`,
              source: "linq",
              sourceRef: `shared_message_${input.id}`,
              scope: { kind: "household" },
              observedAt: T0,
              revision: 1,
              contentDigest: DIGEST_A,
            },
          ]
        : [personalEvidence(adultId, input.id)],
    confidence: 1,
    sensitivity: "ordinary",
    validFrom: T0,
    ...(input.expiresAt === undefined ? {} : { expiresAt: input.expiresAt }),
    confirmedByAdultId: adultId,
    confirmedAt: T1,
    status: input.status ?? "active",
    ...(input.status === "revoked" ? { revokedAt: T1, revokedByAdultId: adultId } : {}),
  });
}

function sharingPolicy(input: {
  id: string;
  adultId: typeof ADULT_A | typeof ADULT_B;
  version: number;
  status?: "active" | "revoked";
}) {
  return PolicyRecordSchema.parse({
    policyId: input.id,
    householdId: HOUSEHOLD_ID,
    version: input.version,
    status: input.status ?? "active",
    rule: {
      kind: "sharing",
      from: { kind: "personal", adultId: input.adultId },
      to: { kind: "household" },
      sourceClass: "school.notice",
      maximumSensitivity: "ordinary",
      sourceMatcher: {
        source: "gmail",
        accountRefDigest: DIGEST_A,
        senderIdentityDigest: DIGEST_B,
      },
    },
    approvedByAdultId: input.adultId,
    approvedAt: T0,
    ...(input.status === "revoked" ? { revokedAt: T1 } : {}),
  });
}

function snapshot(): HouseholdApplicationSnapshot {
  const ownPolicy = sharingPolicy({ id: "policy_alex_school", adultId: ADULT_A, version: 1 });
  const otherPolicy = sharingPolicy({ id: "policy_bailey_school", adultId: ADULT_B, version: 2 });
  const revokedPolicy = sharingPolicy({
    id: "policy_alex_old",
    adultId: ADULT_A,
    version: 3,
    status: "revoked",
  });
  const episode = FamilyEpisodeSchema.parse({
    episodeId: "episode_private_promotion",
    householdId: HOUSEHOLD_ID,
    type: "commitment",
    version: 1,
    scope: { kind: "household" },
    state: "proposed",
    title: "School closes early Friday",
    requiredOutcome: "Plan Friday pickup",
    owner: { status: "unassigned" },
    evidence: [personalEvidence(ADULT_A, "promotion")],
    sourceClass: "school.notice",
    sensitivity: "ordinary",
    promotionAuthority: {
      kind: "policy",
      policyId: ownPolicy.policyId,
      policyVersion: ownPolicy.version,
    },
    sourceMatcher: ownPolicy.rule.kind === "sharing" ? ownPolicy.rule.sourceMatcher : undefined,
    createdAt: T1,
    updatedAt: T1,
  });
  const aggregateState = HouseholdAggregateSchema.parse({
    ...aggregate(),
    version: 8,
    policyVersion: 3,
    routineAnchors: [
      {
        anchorId: "routine_school_departure",
        label: "Leave for school",
        timeZone: "America/Los_Angeles",
        localTime: "07:45",
        daysOfWeek: [1, 2, 3, 4, 5],
      },
    ],
    policies: [ownPolicy, otherPolicy, revokedPolicy],
    memories: [
      memory({ id: "memory_alex", statement: "Alex prefers evening reviews" }),
      memory({ id: "memory_household", scope: "household", statement: "The family eats at six" }),
      memory({ id: "memory_bailey", adultId: ADULT_B, statement: "Bailey's private preference" }),
      memory({ id: "memory_old", statement: "An obsolete preference", status: "revoked" }),
    ],
    episodes: [episode],
  });
  const projection = ApplicationProjectionSchema.parse({
    onboarding: {
      phase: "active",
      initiatorAdultId: ADULT_A,
      invitedAdultId: ADULT_B,
      consentedAdultIds: [ADULT_A, ADULT_B],
      privateDmAdultIds: [ADULT_A, ADULT_B],
      groupChannelId: "group_family",
      profileConfirmedAdultIds: [ADULT_A, ADULT_B],
    },
    sharedProfile: {
      facts: [
        {
          factKey: `profile:${"c".repeat(32)}`,
          category: "school_childcare",
          subject: "School",
          detail: "Weekdays",
          sourceRef: "linq:message:shared-profile",
          recordedByAdultId: ADULT_B,
          recordedAt: T0,
        },
        {
          factKey: `profile:${"d".repeat(32)}`,
          category: "routine_anchor",
          anchorId: "routine_school_departure",
          subject: "School departure",
          detail: "Leave by 7:45",
          timeZone: "America/Los_Angeles",
          localTime: "07:45",
          daysOfWeek: [1, 2, 3, 4, 5],
          sourceRef: "linq:message:routine",
          recordedByAdultId: ADULT_A,
          recordedAt: T1,
        },
      ],
    },
    gmailTriage: [],
    calendarTriage: [],
    calendarSources: [],
    pendingPromotions: [],
    workers: [],
  });
  return { revision: 8, aggregate: aggregateState, projection };
}

describe("PrivateControlCatalog", () => {
  it("lists only active knowledge visible to the authenticated adult with stable provenance", () => {
    const state = snapshot();
    const first = catalog.listKnowledge(state, ADULT_A, T2);
    const second = catalog.listKnowledge(state, ADULT_A, T2);

    expect(second).toEqual(first);
    expect(first).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          controlId: privateControlId("memory", "memory_alex"),
          scope: "personal",
          statement: "Alex prefers evening reviews",
          sourceLabel: "Gmail",
          asOf: T1,
        }),
        expect.objectContaining({
          controlId: privateControlId("memory", "memory_household"),
          scope: "household",
          sourceLabel: "iMessage",
        }),
        expect.objectContaining({
          controlId: privateControlId("profile", `profile:${"c".repeat(32)}`),
          kind: "profile",
          sourceLabel: "Shared family profile",
        }),
        expect.objectContaining({
          controlId: privateControlId("routine", "routine_school_departure"),
          kind: "routine",
          statement: expect.stringContaining("07:45"),
          asOf: T1,
        }),
      ]),
    );
    expect(first.map((item) => item.statement)).not.toContain("Bailey's private preference");
    expect(first.map((item) => item.statement)).not.toContain("An obsolete preference");
  });

  it("never resolves another adult's private memory or sharing rule", () => {
    const state = snapshot();
    expect(catalog.resolveMemory(state, ADULT_A, privateControlId("memory", "memory_bailey"), T2)).toEqual({
      status: "unknown",
    });
    expect(
      catalog.resolveSharingRule(state, ADULT_A, privateControlId("sharing_rule", "policy_bailey_school")),
    ).toEqual({ status: "unknown" });
    expect(catalog.listSharingRules(state, ADULT_A)).toHaveLength(1);
  });

  it("distinguishes inactive, partial, unknown, and exact active control IDs", () => {
    const state = snapshot();
    const active = privateControlId("memory", "memory_alex");
    const inactive = privateControlId("memory", "memory_old");
    expect(catalog.resolveMemory(state, ADULT_A, active, T2)).toMatchObject({ status: "active" });
    expect(catalog.resolveMemory(state, ADULT_A, active.toLowerCase(), T2)).toMatchObject({
      status: "active",
    });
    expect(catalog.resolveMemory(state, ADULT_A, inactive, T2)).toEqual({ status: "inactive" });
    expect(catalog.resolveMemory(state, ADULT_A, active.slice(0, -1), T2)).toEqual({
      status: "unknown",
    });
    expect(catalog.resolveMemory(state, ADULT_A, "MEM-0000000000000000", T2)).toEqual({
      status: "unknown",
    });
  });

  it("does not present an expired record as active knowledge", () => {
    const state = snapshot();
    const expired = memory({
      id: "memory_expired",
      statement: "A time-limited preference",
      expiresAt: T1,
    });
    const withExpired = {
      ...state,
      aggregate: { ...state.aggregate, memories: [...state.aggregate.memories, expired] },
    } as HouseholdApplicationSnapshot;
    expect(catalog.listKnowledge(withExpired, ADULT_A, T2).map((item) => item.statement)).not.toContain(
      "A time-limited preference",
    );
    expect(
      catalog.resolveMemory(withExpired, ADULT_A, privateControlId("memory", "memory_expired"), T2),
    ).toEqual({ status: "inactive" });
  });

  it("explains only the requester's private promotion and never exposes raw source references", () => {
    const state = snapshot();
    const choices = catalog.listRecentSharingChoices(state, ADULT_A);
    expect(choices).toEqual([
      {
        controlId: privateControlId("sharing_choice", "episode_private_promotion"),
        episodeId: "episode_private_promotion",
        summary: "School closes early Friday",
        sourceLabel: "Gmail",
        authorityLabel: expect.stringContaining(privateControlId("sharing_rule", "policy_alex_school")),
        asOf: T1,
      },
    ]);
    expect(JSON.stringify(choices)).not.toContain("private_source_promotion");
    expect(catalog.listRecentSharingChoices(state, ADULT_B)).toEqual([]);
    expect(
      catalog.explainSharingChoice(
        state,
        ADULT_B,
        privateControlId("sharing_choice", "episode_private_promotion"),
      ),
    ).toEqual({ status: "unknown" });
  });

  it("fails closed if a corrupted snapshot creates an ambiguous human control ID", () => {
    const state = snapshot();
    const duplicated = memory({ id: "memory_alex", statement: "Conflicting record" });
    const corrupted = {
      ...state,
      aggregate: { ...state.aggregate, memories: [...state.aggregate.memories, duplicated] },
    } as HouseholdApplicationSnapshot;
    expect(catalog.resolveMemory(corrupted, ADULT_A, privateControlId("memory", "memory_alex"), T2)).toEqual({
      status: "ambiguous",
    });
  });
});
