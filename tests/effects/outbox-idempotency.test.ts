import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { Database } from "../../src/db/client.js";
import { type AuthorizedEffectInput, EffectOutbox } from "../../src/modules/effects/index.js";
import { canonicalDigest, canonicalJson } from "../../src/shared/canonical-json.js";
import { SecretBox } from "../../src/shared/crypto.js";

const secretBox = new SecretBox(
  "test-v1",
  JSON.stringify({ "test-v1": Buffer.alloc(32, 11).toString("base64") }),
);

describe("effect idempotency", () => {
  it("reuses only an identical deliverable effect", async () => {
    const now = new Date("2026-08-08T19:00:00Z");
    const input = effectInput();
    const identical = existingEffect(input, "confirmed");

    await expect(
      new EffectOutbox(fakeDatabase(identical), secretBox).authorizeAndEnqueue(input, now),
    ).resolves.toEqual({ outboxId: identical.id, created: false });

    const active = { ...identical, status: "pending" };
    await expect(
      new EffectOutbox(fakeDatabase(active), secretBox).authorizeAndEnqueue(input, now),
    ).resolves.toEqual({ outboxId: active.id, created: false });

    const differentPayload = existingEffect(
      {
        ...input,
        payload: {
          providerChatId: randomUUID(),
          expectedProviderParticipantDigest: `linq-v1:${"b".repeat(64)}`,
          text: "This is a different message.",
        },
      },
      "confirmed",
    );
    await expect(
      new EffectOutbox(fakeDatabase(differentPayload), secretBox).authorizeAndEnqueue(input, now),
    ).rejects.toThrow(/idempotency/i);

    await expect(
      new EffectOutbox(fakeDatabase({ ...identical, status: "dead" }), secretBox).authorizeAndEnqueue(
        input,
        now,
      ),
    ).rejects.toThrow(/idempotency/i);
  });
});

function effectInput(): AuthorizedEffectInput {
  const now = new Date("2026-08-08T19:00:00Z");
  return {
    effectKind: "linq.message",
    idempotencyKey: `test-effect:${randomUUID()}`,
    data: { notice: "family_setup" },
    policy: { operation: "private_reply" },
    target: { providerChatId: randomUUID() },
    payload: {
      providerChatId: randomUUID(),
      expectedProviderParticipantDigest: `linq-v1:${"a".repeat(64)}`,
      text: "What would you like to set up next?",
    },
    reasonCodes: ["registered_private_conversation"],
    authorizationExpiresAt: new Date(now.getTime() + 60 * 60_000),
  };
}

function existingEffect(input: AuthorizedEffectInput, status: string) {
  const payloadDigest = canonicalDigest(input.payload);
  return {
    id: randomUUID(),
    status,
    effect_kind: input.effectKind,
    action_intent_id: null,
    household_id: null,
    household_control_epoch: null,
    person_id: null,
    person_control_epoch: null,
    person_onboarding_version: null,
    conversation_id: null,
    conversation_authority_version: null,
    participant_epoch_id: null,
    expected_participant_digest: null,
    integration_id: null,
    integration_control_epoch: null,
    coverage_loop_id: null,
    coverage_loop_version: null,
    invitation_id: null,
    invitee_identity_authority_version: null,
    recipient_identity_id: null,
    recipient_identity_authority_version: null,
    recipient_identity_subject_digest: null,
    source_conversation_id: null,
    source_participant_epoch_id: null,
    source_expected_participant_digest: null,
    source_conversation_authority_version: null,
    private_source_frontier_id: null,
    private_source_frontier_version: null,
    private_source_frontier_digest: null,
    private_source_generation: null,
    private_source_case_key_digest: null,
    evidence_source_revision_ids: [],
    payload_digest: payloadDigest,
    actor_person_id: null,
    decision_household_id: null,
    decision_conversation_id: null,
    decision_participant_epoch_id: null,
    action_digest: canonicalDigest({
      effectKind: input.effectKind,
      idempotencyKey: input.idempotencyKey,
    }),
    data_digest: canonicalDigest({ data: input.data, payloadDigest }),
    policy_digest: canonicalDigest(input.policy),
    target_digest: canonicalDigest(input.target),
    reason_codes: [...input.reasonCodes],
    decision_outcome: "allow",
    decision_revoked_at: null,
    decision_expires_at: input.authorizationExpiresAt,
  };
}

function fakeDatabase(row: ReturnType<typeof existingEffect>): Database {
  return (async (strings: TemplateStringsArray) => {
    const query = canonicalJson([...strings]);
    if (!query.includes("from outbox")) throw new Error(`Unexpected query: ${query}`);
    return [row];
  }) as unknown as Database;
}
