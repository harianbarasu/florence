import { describe, expect, it, vi } from "vitest";
import {
  type ApplicationOutboxIntent,
  ApplicationProjectionSchema,
  type HouseholdApplicationSnapshot,
} from "../../src/application/index.js";
import {
  DurableMemorySchema,
  FamilyEpisodeSchema,
  HouseholdAggregateSchema,
  PolicyRecordSchema,
} from "../../src/domain/index.js";
import { privateControlId } from "../../src/infrastructure/private-control-catalog.js";
import { PrivateControlCommandService } from "../../src/infrastructure/private-control-commands.js";
import { ADULT_A, ADULT_B, aggregate, HOUSEHOLD_ID } from "../application/fixtures.js";

const T0 = "2026-08-05T16:00:00Z";
const DIGEST_A = `sha256:${"a".repeat(64)}`;
const DIGEST_B = `sha256:${"b".repeat(64)}`;

function evidence(adultId: typeof ADULT_A | typeof ADULT_B, id: string) {
  return {
    evidenceId: `evidence_${id}`,
    source: "gmail" as const,
    sourceRef: `private_${id}`,
    scope: { kind: "personal" as const, adultId },
    observedAt: T0,
    revision: 1,
    contentDigest: DIGEST_A,
  };
}

function controlSnapshot(): HouseholdApplicationSnapshot {
  const rule = PolicyRecordSchema.parse({
    policyId: "policy_alex_school",
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
  const ownMemory = DurableMemorySchema.parse({
    memoryId: "memory_alex",
    householdId: HOUSEHOLD_ID,
    kind: "preference",
    statement: "Review family mail after dinner",
    scope: { kind: "personal", adultId: ADULT_A },
    sourceClass: "family.preference",
    evidence: [evidence(ADULT_A, "memory_alex")],
    confidence: 1,
    sensitivity: "ordinary",
    validFrom: T0,
    confirmedByAdultId: ADULT_A,
    confirmedAt: T0,
    status: "active",
  });
  const otherMemory = DurableMemorySchema.parse({
    ...ownMemory,
    memoryId: "memory_bailey",
    statement: "Bailey's confidential preference",
    scope: { kind: "personal", adultId: ADULT_B },
    evidence: [evidence(ADULT_B, "memory_bailey")],
    confirmedByAdultId: ADULT_B,
  });
  const episode = FamilyEpisodeSchema.parse({
    episodeId: "episode_shared_by_rule",
    householdId: HOUSEHOLD_ID,
    type: "commitment",
    version: 1,
    scope: { kind: "household" },
    state: "proposed",
    title: "School closes early Friday",
    requiredOutcome: "Plan Friday pickup",
    owner: { status: "unassigned" },
    evidence: [evidence(ADULT_A, "episode")],
    sourceClass: "school.notice",
    sensitivity: "ordinary",
    promotionAuthority: { kind: "policy", policyId: rule.policyId, policyVersion: rule.version },
    sourceMatcher: rule.rule.kind === "sharing" ? rule.rule.sourceMatcher : undefined,
    createdAt: T0,
    updatedAt: T0,
  });
  return {
    revision: 1,
    aggregate: HouseholdAggregateSchema.parse({
      ...aggregate(),
      version: 3,
      policyVersion: 1,
      memories: [ownMemory, otherMemory],
      policies: [rule],
      episodes: [episode],
    }),
    projection: ApplicationProjectionSchema.parse({
      onboarding: {
        phase: "active",
        initiatorAdultId: ADULT_A,
        invitedAdultId: ADULT_B,
        adultNames: [
          { adultId: ADULT_A, displayName: "Alex" },
          { adultId: ADULT_B, displayName: "Bailey" },
        ],
        consentedAdultIds: [ADULT_A, ADULT_B],
        privateDmAdultIds: [ADULT_A, ADULT_B],
        groupChannelId: "group_family",
        profileConfirmedAdultIds: [ADULT_A, ADULT_B],
        googleConnectedAdultIds: [ADULT_A, ADULT_B],
      },
      sharedProfile: { facts: [] },
      gmailTriage: [],
      gmailSources: [],
      calendarTriage: [],
      calendarSources: [],
      pendingPromotions: [],
      workers: [],
    }),
  };
}

const baseCommand = {
  householdId: HOUSEHOLD_ID,
  adultId: ADULT_A,
  channelId: "dm-alex",
  messageId: "message-1",
  occurredAt: T0,
  idempotencyKey: "linq:event-1",
};

function setup(input?: { snapshot?: HouseholdApplicationSnapshot | null; replyControlId?: string | null }) {
  const enqueued: ApplicationOutboxIntent[] = [];
  const enqueueApplicationIntent = vi.fn(async (intent: ApplicationOutboxIntent) => {
    enqueued.push(intent);
    return { rowId: "outbox-1" };
  });
  const revokeMemory = vi.fn(async () => undefined);
  const revokeSharingPolicy = vi.fn(async () => undefined);
  const resolveSharingControlId = vi.fn(async () => input?.replyControlId ?? null);
  const service = new PrivateControlCommandService({
    snapshots: {
      load: vi.fn(async () => (input?.snapshot === undefined ? controlSnapshot() : input.snapshot)),
    },
    outbox: { enqueueApplicationIntent },
    mutator: { revokeMemory, revokeSharingPolicy },
    sharingReferences: { resolveSharingControlId },
  });
  return {
    service,
    enqueued,
    enqueueApplicationIntent,
    revokeMemory,
    revokeSharingPolicy,
    resolveSharingControlId,
  };
}

describe("PrivateControlCommandService", () => {
  it("requires an exact sharing choice or resolvable reply and never guesses from a topic", async () => {
    const harness = setup();
    await expect(
      harness.service.handle({ ...baseCommand, text: "Why did you share that?" }),
    ).resolves.toEqual({
      handled: true,
      classification: "control:sharing_explanation_needs_exact_id",
    });
    expect(JSON.stringify(harness.enqueued)).toContain(
      privateControlId("sharing_choice", "episode_shared_by_rule"),
    );

    const exact = setup();
    const choice = privateControlId("sharing_choice", "episode_shared_by_rule");
    await expect(
      exact.service.handle({ ...baseCommand, text: `why did you share that ${choice}` }),
    ).resolves.toEqual({ handled: true, classification: "control:sharing_explained" });
    const explanation = JSON.stringify(exact.enqueued);
    expect(explanation).toContain("personal source → minimum household meaning");
    expect(explanation).not.toContain("private_episode");
  });

  it("resolves an exact provider reply but fails closed when reply and explicit IDs disagree", async () => {
    const choice = privateControlId("sharing_choice", "episode_shared_by_rule");
    const fromReply = setup({ replyControlId: choice });
    await expect(
      fromReply.service.handle({
        ...baseCommand,
        text: "why did you share that?",
        replyToMessageId: "linq-message-9",
      }),
    ).resolves.toEqual({ handled: true, classification: "control:sharing_explained" });
    expect(fromReply.resolveSharingControlId).toHaveBeenCalledWith({
      householdId: HOUSEHOLD_ID,
      adultId: ADULT_A,
      providerMessageId: "linq-message-9",
    });

    const mismatch = setup({ replyControlId: "SHARE-0000000000000000" });
    await expect(
      mismatch.service.handle({
        ...baseCommand,
        text: `why did you share that ${choice}`,
        replyToMessageId: "linq-message-10",
      }),
    ).resolves.toEqual({
      handled: true,
      classification: "control:sharing_explanation_needs_exact_id",
    });
  });
});
