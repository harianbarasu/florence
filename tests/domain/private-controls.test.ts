import { describe, expect, it } from "vitest";
import { DurableMemorySchema, HouseholdChiefOfStaff, PolicyRecordSchema } from "../../src/domain/index.js";
import {
  ADULT_A,
  ADULT_B,
  aggregate,
  DIGEST_A,
  DIGEST_B,
  evidence,
  HOUSEHOLD_ID,
  signal,
  T0,
} from "./fixtures.js";

function personalMemory(adultId = ADULT_A) {
  return DurableMemorySchema.parse({
    memoryId: "memory_private_preference",
    householdId: HOUSEHOLD_ID,
    kind: "preference",
    statement: "Review family notices after dinner",
    scope: { kind: "personal", adultId },
    sourceClass: "family.preference",
    evidence: [
      evidence({ kind: "personal", adultId }, { evidenceId: "evidence_private_memory", source: "gmail" }),
    ],
    confidence: 1,
    sensitivity: "ordinary",
    validFrom: T0,
    confirmedByAdultId: adultId,
    confirmedAt: T0,
    status: "active",
  });
}

function revokeMemory(current: ReturnType<typeof aggregate>, adultId = ADULT_A, sequence = 1) {
  return HouseholdChiefOfStaff.accept({
    current,
    signal: signal({
      householdId: HOUSEHOLD_ID,
      signalId: `signal_memory_revoke_${sequence}`,
      sequence,
      occurredAt: `2026-01-01T08:0${sequence}:00Z`,
      actor: { kind: "adult", adultId },
      kind: "memory.revoked",
      memoryId: "memory_private_preference",
    }),
  });
}

describe("private memory controls", () => {
  it("revokes an authenticated adult's memory while preserving its history", () => {
    const result = revokeMemory(aggregate({ memories: [personalMemory()] }));
    expect(result.receipt.disposition).toBe("accepted");
    expect(result.aggregate.memories[0]).toMatchObject({
      memoryId: "memory_private_preference",
      statement: "Review family notices after dinner",
      status: "revoked",
      revokedAt: "2026-01-01T08:01:00Z",
      revokedByAdultId: ADULT_A,
    });
    expect(result.changes).toContainEqual({
      kind: "memory_revoked",
      memoryId: "memory_private_preference",
    });
  });

  it("never lets one adult revoke the other adult's private memory", () => {
    const current = aggregate({ memories: [personalMemory(ADULT_B)] });
    const result = revokeMemory(current, ADULT_A);
    expect(result.receipt).toMatchObject({
      disposition: "rejected",
      reason: "unauthorized_actor",
    });
    expect(result.aggregate.memories).toEqual(current.memories);
  });

  it("allows either verified adult to correct shared household memory", () => {
    const shared = DurableMemorySchema.parse({
      ...personalMemory(),
      memoryId: "memory_private_preference",
      scope: { kind: "household" },
      evidence: [evidence({ kind: "household" }, { evidenceId: "evidence_shared_memory", source: "linq" })],
    });
    const result = revokeMemory(aggregate({ memories: [shared] }), ADULT_B);
    expect(result.receipt.disposition).toBe("accepted");
    expect(result.aggregate.memories[0]).toMatchObject({
      status: "revoked",
      revokedByAdultId: ADULT_B,
    });
  });

  it("rejects replay after revocation without changing the original authority record", () => {
    const first = revokeMemory(aggregate({ memories: [personalMemory()] }));
    const replay = revokeMemory(first.aggregate, ADULT_A, 2);
    expect(replay.receipt).toMatchObject({
      disposition: "rejected",
      reason: "invalid_transition",
    });
    expect(replay.aggregate.memories[0]).toMatchObject({
      revokedAt: "2026-01-01T08:01:00Z",
      revokedByAdultId: ADULT_A,
    });
  });
});

describe("private sharing-rule controls", () => {
  it("binds stale checks to the exact rule version, not an unrelated later policy", () => {
    const first = PolicyRecordSchema.parse({
      policyId: "policy_first",
      householdId: HOUSEHOLD_ID,
      version: 1,
      status: "active",
      rule: {
        kind: "sharing",
        from: { kind: "personal", adultId: ADULT_A },
        to: { kind: "household" },
        sourceClass: "school.notice",
        maximumSensitivity: "ordinary",
        sourceMatcher: {
          source: "gmail",
          accountRefDigest: DIGEST_A,
          senderIdentityDigest: DIGEST_B,
        },
      },
      approvedByAdultId: ADULT_A,
      approvedAt: T0,
    });
    const second = PolicyRecordSchema.parse({
      policyId: "policy_second",
      householdId: HOUSEHOLD_ID,
      version: 2,
      status: "active",
      rule: {
        kind: "timing",
        scope: { kind: "household" },
        localTime: "07:00",
        timeZone: "America/Los_Angeles",
      },
      approvedByAdultId: ADULT_A,
      approvedAt: T0,
    });
    const current = aggregate({ policyVersion: 2, policies: [first, second] });
    const accepted = HouseholdChiefOfStaff.accept({
      current,
      signal: signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_revoke_first_policy",
        sequence: 1,
        occurredAt: "2026-01-01T08:01:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "policy.revoked",
        policyId: "policy_first",
        expectedPolicyVersion: 1,
      }),
    });
    expect(accepted.receipt.disposition).toBe("accepted");
    expect(accepted.aggregate.policyVersion).toBe(3);

    const stale = HouseholdChiefOfStaff.accept({
      current,
      signal: signal({
        householdId: HOUSEHOLD_ID,
        signalId: "signal_stale_first_policy",
        sequence: 1,
        occurredAt: "2026-01-01T08:01:00Z",
        actor: { kind: "adult", adultId: ADULT_A },
        kind: "policy.revoked",
        policyId: "policy_first",
        expectedPolicyVersion: 2,
      }),
    });
    expect(stale.receipt).toMatchObject({
      disposition: "rejected",
      reason: "stale_policy_version",
    });
  });
});
