import { describe, expect, it } from "vitest";
import {
  PrivateBridgePayloadSchema,
  requireSameObservedSourceFrontier,
} from "../../src/modules/bridges/index.js";
import { StaleAuthorityError } from "../../src/shared/errors.js";

const IDS = {
  candidate: "00000000-0000-4000-8000-000000000001",
  evidence: "00000000-0000-4000-8000-000000000002",
  integration: "00000000-0000-4000-8000-000000000003",
  otherIntegration: "00000000-0000-4000-8000-000000000004",
  frontier: "00000000-0000-4000-8000-000000000005",
  household: "00000000-0000-4000-8000-000000000006",
  conversation: "00000000-0000-4000-8000-000000000007",
  epoch: "00000000-0000-4000-8000-000000000008",
  chat: "00000000-0000-4000-8000-000000000009",
  identity: "00000000-0000-4000-8000-00000000000a",
  rule: "00000000-0000-4000-8000-00000000000b",
  loop: "00000000-0000-4000-8000-00000000000c",
  priorCandidate: "00000000-0000-4000-8000-00000000000d",
  sourceIntent: "00000000-0000-4000-8000-00000000000e",
} as const;

const DIGEST = "a".repeat(64);
const OTHER_DIGEST = "b".repeat(64);

function preparedPayload() {
  return {
    schemaVersion: 1,
    phase: "prepared",
    candidateId: IDS.candidate,
    candidateContentDigest: DIGEST,
    evidenceSourceRevisionIds: [IDS.evidence],
    evidenceDigest: DIGEST,
    sourcePattern: null,
    sourceFrontier: null,
    destination: {
      householdId: IDS.household,
      householdControlEpoch: 1,
      conversationId: IDS.conversation,
      participantEpochId: IDS.epoch,
      participantSetDigest: DIGEST,
      conversationAuthorityVersion: 1,
      providerChatId: IDS.chat,
      providerParticipantDigest: `linq-v1:${DIGEST}`,
      liveIdentityIds: [IDS.identity],
      proactiveRuleId: IDS.rule,
      timeZone: "America/Los_Angeles",
    },
    personControlEpoch: 1,
    standingRule: null,
    loopUpdate: null,
  } as const;
}

function sourceFrontier() {
  return {
    frontierId: IDS.frontier,
    integrationId: IDS.integration,
    integrationControlEpoch: 1,
    caseKind: "gmail_thread",
    caseKeyDigest: DIGEST,
    version: 1,
    frontierDigest: DIGEST,
    sourceGeneration: 3,
  } as const;
}

describe("private source bridge frontier payload", () => {
  it("preserves the explicit integration-free direct-chat path", () => {
    const parsed = PrivateBridgePayloadSchema.parse(preparedPayload());

    expect(parsed.sourcePattern).toBeNull();
    expect(parsed.sourceFrontier).toBeNull();
  });

  it("requires a matching frontier for an integration-backed source pattern", () => {
    const integrated = {
      ...preparedPayload(),
      sourcePattern: {
        version: 1,
        kind: "gmail_thread",
        integrationId: IDS.integration,
        provider: "gmail",
        objectKind: "mail_message",
        classification: "coverage_proposal",
        threadDigest: DIGEST,
        senderDigest: DIGEST,
      },
    } as const;

    expect(PrivateBridgePayloadSchema.safeParse(integrated).success).toBe(false);
    expect(
      PrivateBridgePayloadSchema.safeParse({
        ...integrated,
        sourceFrontier: { ...sourceFrontier(), integrationId: IDS.otherIntegration },
      }).success,
    ).toBe(false);
    expect(
      PrivateBridgePayloadSchema.parse({ ...integrated, sourceFrontier: sourceFrontier() }).sourceFrontier,
    ).toEqual(sourceFrontier());
  });

  it("does not let an update review omit its top-level frontier", () => {
    expect(
      PrivateBridgePayloadSchema.safeParse({
        ...preparedPayload(),
        loopUpdate: {
          existingLoopId: IDS.loop,
          expectedLoopVersion: 1,
          expectedLoopDestinationDigest: DIGEST,
          priorCandidateId: IDS.priorCandidate,
          priorCandidateContentDigest: DIGEST,
          sourceActionIntentId: IDS.sourceIntent,
          sourceActionIntentDigests: {
            actionDigest: DIGEST,
            dataDigest: DIGEST,
            policyDigest: DIGEST,
            targetDigest: DIGEST,
          },
        },
      }).success,
    ).toBe(false);
  });

  it("makes any frontier version, digest, or source-generation change stale", () => {
    const current = PrivateBridgePayloadSchema.parse({
      ...preparedPayload(),
      sourceFrontier: sourceFrontier(),
    });

    for (const staleFrontier of [
      { ...sourceFrontier(), version: 2 },
      { ...sourceFrontier(), frontierDigest: OTHER_DIGEST },
      { ...sourceFrontier(), sourceGeneration: 4 },
      { ...sourceFrontier(), integrationControlEpoch: 2 },
    ]) {
      const stale = PrivateBridgePayloadSchema.parse({
        ...preparedPayload(),
        sourceFrontier: staleFrontier,
      });
      expect(() => requireSameObservedSourceFrontier(current, stale)).toThrow(StaleAuthorityError);
    }
    expect(() => requireSameObservedSourceFrontier(current, current)).not.toThrow();
  });
});
