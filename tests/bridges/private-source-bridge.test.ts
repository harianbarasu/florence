import { describe, expect, it } from "vitest";
import type { Database } from "../../src/db/client.js";
import {
  PrivateBridgePayloadSchema,
  PrivateSourceBridge,
  requireSameObservedSourceFrontier,
} from "../../src/modules/bridges/index.js";
import { SecretBox } from "../../src/shared/crypto.js";
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
  owner: "00000000-0000-4000-8000-00000000000f",
  actionIntent: "00000000-0000-4000-8000-000000000010",
  decision: "00000000-0000-4000-8000-000000000012",
  ruleRevision: "00000000-0000-4000-8000-000000000013",
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

function approvedOpeningPayload() {
  const evidence = [{ sourceRevisionId: IDS.evidence, support: "Current pickup request" }];
  return PrivateBridgePayloadSchema.parse({
    ...preparedPayload(),
    phase: "awaiting_approval",
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
    sourceFrontier: sourceFrontier(),
    standingRule: {
      ruleId: IDS.rule,
      ruleRevisionId: IDS.ruleRevision,
      ruleVersion: 1,
    },
    loopId: IDS.loop,
    minimumDisclosure: {
      destinationEpochId: IDS.epoch,
      minimumMeaning: "Avery needs pickup tomorrow",
      evidence,
      omittedSensitiveCategories: [],
      containsPersonAttribution: false,
      sourceOwnerApprovalRequired: true,
    },
    commitment: {
      outcome: "Cover Avery's pickup",
      proposedPersonId: null,
      semanticTiming: "tomorrow at 3 PM",
      timeZone: "America/Los_Angeles",
      eventAt: "2026-08-08T22:00:00.000Z",
      deadlineAt: "2026-08-08T21:30:00.000Z",
      unresolvedFacts: [],
      consequentialQuestion: null,
      followUpShape: "ask_group_neutrally",
      evidence,
      confidence: 0.95,
    },
    approvalMode: "standing",
  });
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

describe("private source bridge stale authority recovery", () => {
  it("revokes the old approval and work so the owner can retry against the new epoch", async () => {
    const actionIntentId = "00000000-0000-4000-8000-000000000010";
    const ruleId = "00000000-0000-4000-8000-000000000011";
    const revisionId = "00000000-0000-4000-8000-000000000012";
    const statements: string[] = [];
    let returnedStaleIntent = false;
    const database = Object.assign(
      async (fragments: TemplateStringsArray) => {
        const statement = fragments.join("?").replace(/\s+/gu, " ").trim();
        statements.push(statement);
        if (statement.startsWith("select intent.id")) {
          if (returnedStaleIntent) return [];
          returnedStaleIntent = true;
          return [
            {
              id: actionIntentId,
              person_id: "00000000-0000-4000-8000-000000000013",
              household_id: "00000000-0000-4000-8000-000000000014",
              conversation_id: "00000000-0000-4000-8000-000000000015",
            },
          ];
        }
        if (statement.startsWith("select rule.id")) {
          return [{ id: ruleId, current_revision_id: revisionId }];
        }
        if (statement.startsWith("select coalesce(max(sequence)")) return [{ sequence: 1 }];
        return [];
      },
      {
        array: (values: readonly unknown[]) => values,
        json: (value: unknown) => value,
      },
    ) as unknown as Database;
    const secretBox = new SecretBox(
      "test-v1",
      JSON.stringify({ "test-v1": Buffer.alloc(32, 5).toString("base64") }),
    );
    const bridge = new PrivateSourceBridge(database, secretBox);

    await expect(bridge.cancelStaleAuthorityIntents(new Date("2026-08-07T12:00:00Z"))).resolves.toBe(1);
    await expect(bridge.cancelStaleAuthorityIntents(new Date("2026-08-07T12:01:00Z"))).resolves.toBe(0);

    expect(statements).toEqual(
      expect.arrayContaining([
        expect.stringContaining("update action_approvals set revoked_at"),
        expect.stringContaining("update bridge_rules set status = 'revoked'"),
        expect.stringContaining("update jobs set status = 'cancelled'"),
        expect.stringContaining("update action_intents set status = 'cancelled'"),
        expect.stringContaining("insert into audit_events"),
      ]),
    );
  });

  it("supersedes an unsent old-epoch opening and returns it to fresh private approval", async () => {
    const recoveredAt = new Date("2026-08-07T12:00:00.000Z");
    const secretBox = new SecretBox(
      "test-v1",
      JSON.stringify({ "test-v1": Buffer.alloc(32, 6).toString("base64") }),
    );
    const payload = approvedOpeningPayload();
    const sealedIntent = secretBox.encrypt(
      JSON.stringify(payload),
      `florence:action-intent:${IDS.actionIntent}:payload`,
    );
    const sealedLoop = secretBox.encrypt(
      JSON.stringify({ minimumSharedMeaning: "Avery needs pickup tomorrow", unresolvedFacts: [] }),
      "coverage-loop-content",
    );
    const statements: string[] = [];
    let returnedRecovery = false;
    const database = Object.assign(
      async (fragments: TemplateStringsArray) => {
        const statement = fragments.join("?").replace(/\s+/gu, " ").trim();
        statements.push(statement);
        if (statement.startsWith("select intent.id")) {
          if (returnedRecovery) return [];
          returnedRecovery = true;
          return [
            {
              id: IDS.actionIntent,
              household_id: IDS.household,
              person_id: IDS.owner,
              conversation_id: IDS.conversation,
              participant_epoch_id: IDS.epoch,
              action_digest: DIGEST,
              data_digest: DIGEST,
              policy_digest: DIGEST,
              target_digest: DIGEST,
              payload_ciphertext: Buffer.from(JSON.stringify(sealedIntent), "utf8"),
              status: "succeeded",
              person_control_epoch: 1,
              household_control_epoch: 1,
              conversation_authority_version: 1,
              expires_at: new Date("2026-08-14T12:00:00.000Z"),
              authorization_decision_id: IDS.decision,
              coverage_loop_id: IDS.loop,
              expected_participant_digest: DIGEST,
            },
          ];
        }
        if (statement.startsWith("select id from knowledge_candidates")) return [{ id: IDS.candidate }];
        if (statement.startsWith("select * from coverage_loops")) {
          return [
            {
              id: IDS.loop,
              household_id: IDS.household,
              version: 1,
              state: "open",
              content_ciphertext: Buffer.from(JSON.stringify(sealedLoop), "utf8"),
              unresolved_facts: [],
              proposed_holder_person_id: null,
              acknowledged_by_person_id: null,
              acknowledged_at: null,
              acknowledgment_kind: null,
              holder_disclosure: null,
              time_zone: "America/Los_Angeles",
              local_date: "2026-08-08",
              event_at: new Date("2026-08-08T22:00:00.000Z"),
              deadline_at: new Date("2026-08-08T21:30:00.000Z"),
              preparation_minutes: 0,
              travel_minutes: 0,
              earliest_useful_at: recoveredAt,
              last_responsible_at: new Date("2026-08-08T21:30:00.000Z"),
              plan_version: 1,
              notification_mode: "always",
              destination_conversation_id: IDS.conversation,
              participant_epoch_id: IDS.epoch,
              participant_set_digest: DIGEST,
              audience: "group",
              source_evidence_refs: [IDS.evidence],
              routine_occurrence_id: null,
              routine_occurrence_version: null,
              routine_id: null,
              routine_revision: null,
              attention_cycle: 1,
              notification_history: [],
              last_transition_at: new Date("2026-08-07T11:00:00.000Z"),
            },
          ];
        }
        if (statement.startsWith("update coverage_loops set")) return [{ id: IDS.loop }];
        if (statement.startsWith("select rule.id")) {
          return [{ id: IDS.rule, current_revision_id: IDS.ruleRevision }];
        }
        if (statement.startsWith("select coalesce(max(sequence)")) return [{ sequence: 1 }];
        return [];
      },
      {
        array: (values: readonly unknown[]) => values,
        json: (value: unknown) => value,
      },
    ) as unknown as Database;
    const bridge = new PrivateSourceBridge(database, secretBox);

    await expect(bridge.recoverCancelledUnsubmittedOpenings(recoveredAt)).resolves.toBe(1);
    await expect(bridge.recoverCancelledUnsubmittedOpenings(recoveredAt)).resolves.toBe(0);

    expect(statements).toEqual(
      expect.arrayContaining([
        expect.stringContaining("effect.attempt_count = 0"),
        expect.stringContaining("update coverage_loops set"),
        expect.stringContaining("insert into coverage_transitions"),
        expect.stringContaining("update timers set status = 'superseded'"),
        expect.stringContaining("update action_approvals"),
        expect.stringContaining("update bridge_rules set status = 'revoked'"),
        expect.stringContaining("update disclosure_decisions"),
        expect.stringContaining("update knowledge_candidates set status = 'pending'"),
        expect.stringContaining("update action_intents set status = 'cancelled'"),
        expect.stringContaining("insert into audit_events"),
      ]),
    );
    expect(statements.some((statement) => statement.includes("insert into outbox"))).toBe(false);
  });
});
