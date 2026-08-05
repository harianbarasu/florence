import { describe, expect, it } from "vitest";
import { createFlorenceApplication } from "../../src/application/index.js";
import { DurableMemorySchema, PolicyRecordSchema } from "../../src/domain/index.js";
import { ADULT_A, ADULT_B, aggregate, HOUSEHOLD_ID, setup } from "./fixtures.js";

const T0 = "2026-08-05T16:00:00Z";
const DIGEST_A = `sha256:${"a".repeat(64)}`;
const DIGEST_B = `sha256:${"b".repeat(64)}`;

function memory(adultId = ADULT_A) {
  return DurableMemorySchema.parse({
    memoryId: "memory_evening_review",
    householdId: HOUSEHOLD_ID,
    kind: "preference",
    statement: "Review family mail after dinner",
    scope: { kind: "personal", adultId },
    sourceClass: "family.preference",
    evidence: [
      {
        evidenceId: "evidence_memory_evening_review",
        source: "gmail",
        sourceRef: "gmail:private:message",
        scope: { kind: "personal", adultId },
        observedAt: T0,
        revision: 1,
        contentDigest: DIGEST_A,
      },
    ],
    confidence: 1,
    sensitivity: "ordinary",
    validFrom: T0,
    confirmedByAdultId: adultId,
    confirmedAt: T0,
    status: "active",
  });
}

function sharingPolicy() {
  return PolicyRecordSchema.parse({
    policyId: "policy_school_notices",
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
}

function privateControl(
  idempotencyKey: string,
  action:
    | { kind: "revoke_memory"; memoryId: string }
    | { kind: "revoke_sharing_policy"; policyId: string; expectedPolicyVersion: number },
) {
  return {
    kind: "private_control" as const,
    householdId: HOUSEHOLD_ID,
    idempotencyKey,
    occurredAt: "2026-08-05T16:01:00Z",
    channel: { channelId: "dm_alex", scope: "personal" as const, adultId: ADULT_A },
    requesterAdultId: ADULT_A,
    action,
  };
}

describe("application private controls", () => {
  it("atomically revokes an owner memory and queues a private confirmation", async () => {
    const harness = setup({ aggregate: aggregate({ memories: [memory()] }) });
    const app = createFlorenceApplication(harness.dependencies);
    const result = await app.process(
      privateControl("private-control-memory-1", {
        kind: "revoke_memory",
        memoryId: "memory_evening_review",
      }),
    );
    expect(result.outcome).toMatchObject({
      status: "processed",
      classification: "private_control:memory:accepted",
    });
    const stored = await harness.repository.load(HOUSEHOLD_ID);
    expect(stored?.aggregate.memories[0]).toMatchObject({
      status: "revoked",
      revokedByAdultId: ADULT_A,
      revokedAt: "2026-08-05T16:01:00Z",
    });
    expect(harness.repository.outbox).toContainEqual(
      expect.objectContaining({
        kind: "conversation.send",
        targetScope: { kind: "personal", adultId: ADULT_A },
        body: expect.stringContaining("learned memory is revoked"),
      }),
    );
  });

  it("deduplicates replay at the application boundary", async () => {
    const harness = setup({ aggregate: aggregate({ memories: [memory()] }) });
    const app = createFlorenceApplication(harness.dependencies);
    const command = privateControl("private-control-memory-replay", {
      kind: "revoke_memory",
      memoryId: "memory_evening_review",
    });
    const first = await app.process(command);
    const replay = await app.process(command);
    expect(first.disposition).toBe("committed");
    expect(replay.disposition).toBe("duplicate");
    expect(replay.revision).toBe(first.revision);
  });

  it("rejects cross-adult private-memory revocation without disclosure", async () => {
    const harness = setup({ aggregate: aggregate({ memories: [memory(ADULT_B)] }) });
    const app = createFlorenceApplication(harness.dependencies);
    const result = await app.process(
      privateControl("private-control-cross-adult", {
        kind: "revoke_memory",
        memoryId: "memory_evening_review",
      }),
    );
    expect(result.outcome).toMatchObject({
      status: "rejected",
      classification: "private_control:memory:rejected",
    });
    const body = harness.repository.outbox.at(-1);
    expect(body).toMatchObject({
      kind: "conversation.send",
      targetScope: { kind: "personal", adultId: ADULT_A },
    });
    expect(JSON.stringify(body)).not.toContain("Review family mail after dinner");
  });

  it("enforces exact private conversation identity before processing", async () => {
    const harness = setup({ aggregate: aggregate({ memories: [memory()] }) });
    const app = createFlorenceApplication(harness.dependencies);
    await expect(
      app.process({
        ...privateControl("private-control-wrong-dm", {
          kind: "revoke_memory",
          memoryId: "memory_evening_review",
        }),
        channel: { channelId: "dm_bailey", scope: "personal", adultId: ADULT_B },
      }),
    ).rejects.toThrow(/exact personal conversation/u);
    expect(harness.repository.outbox).toEqual([]);
  });

  it("revokes only an exact current sharing-rule version", async () => {
    const policy = sharingPolicy();
    const harness = setup({ aggregate: aggregate({ policyVersion: 1, policies: [policy] }) });
    const app = createFlorenceApplication(harness.dependencies);
    const stale = await app.process(
      privateControl("private-control-policy-stale", {
        kind: "revoke_sharing_policy",
        policyId: policy.policyId,
        expectedPolicyVersion: 2,
      }),
    );
    expect(stale.outcome).toMatchObject({
      status: "rejected",
      classification: "private_control:sharing_policy:rejected",
    });
    expect((await harness.repository.load(HOUSEHOLD_ID))?.aggregate.policies[0]?.status).toBe("active");

    const accepted = await app.process(
      privateControl("private-control-policy-current", {
        kind: "revoke_sharing_policy",
        policyId: policy.policyId,
        expectedPolicyVersion: 1,
      }),
    );
    expect(accepted.outcome).toMatchObject({
      status: "processed",
      classification: "private_control:sharing_policy:accepted",
    });
    expect((await harness.repository.load(HOUSEHOLD_ID))?.aggregate.policies[0]).toMatchObject({
      status: "revoked",
      revokedAt: "2026-08-05T16:01:00Z",
    });
  });
});
