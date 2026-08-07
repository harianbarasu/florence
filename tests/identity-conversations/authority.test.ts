import { describe, expect, it } from "vitest";
import {
  authorizeSendFromSnapshot,
  type ConversationAuthoritySnapshot,
  evaluateConversationMode,
  isPolicyWidening,
  participantSetDigest,
} from "../../src/modules/conversations/index.js";
import {
  ClaimIdentityInputSchema,
  InviteHouseholdMemberInputSchema,
  StewardCapabilities,
} from "../../src/modules/identity/index.js";

const conversationId = "10000000-0000-4000-8000-000000000001";
const epochId = "10000000-0000-4000-8000-000000000002";
const identityA = "10000000-0000-4000-8000-000000000003";
const identityB = "10000000-0000-4000-8000-000000000004";
const personA = "10000000-0000-4000-8000-000000000005";
const personB = "10000000-0000-4000-8000-000000000006";
const ruleId = "10000000-0000-4000-8000-000000000007";
const suppressionId = "10000000-0000-4000-8000-000000000008";
const digest = participantSetDigest([identityA, identityB]);
const consentedAt = "2026-08-05T12:00:00.000Z";

function trustedSnapshot(conversationKind: "direct" | "group" = "group"): ConversationAuthoritySnapshot {
  return {
    conversationId,
    conversationKind,
    conversationStatus: "active",
    authorityVersion: 7,
    participantEpochId: epochId,
    participantSetDigest: digest,
    participants: [
      {
        personIdentityId: identityA,
        personId: personA,
        registrationStatus: "registered",
        consentedAt,
        proactivePaused: false,
        policy: {
          allowContentProcessing: true,
          allowDirectResponses: true,
          allowProactiveWrites: true,
          retentionSeconds: 86_400,
        },
      },
      {
        personIdentityId: identityB,
        personId: personB,
        registrationStatus: "registered",
        consentedAt,
        proactivePaused: false,
        policy: {
          allowContentProcessing: true,
          allowDirectResponses: true,
          allowProactiveWrites: true,
          retentionSeconds: 3_600,
        },
      },
    ],
    activeSuppressions: [],
    rules: [
      {
        ruleId,
        ruleKey: "coverage_reminder",
        participantSetDigest: digest,
        allowedOperations: ["coverage_reminder", "reply"],
        active: true,
      },
    ],
  };
}

describe("identity claims", () => {
  it("requires confirmation from the exact identity being claimed", () => {
    const result = ClaimIdentityInputSchema.safeParse({
      identityId: identityA,
      confirmedByIdentityId: identityB,
      expectedIdentityAuthorityVersion: 1,
      consentedAt,
      timezone: "America/Los_Angeles",
    });
    expect(result.success).toBe(false);
  });

  it("rejects duplicate relationship-local capability grants", () => {
    const result = InviteHouseholdMemberInputSchema.safeParse({
      householdId: conversationId,
      inviterPersonId: personA,
      inviteeSubjectDigest: "a".repeat(64),
      tokenDigest: "b".repeat(64),
      requestedRole: "steward",
      requestedCapabilities: [...StewardCapabilities, StewardCapabilities[0]],
      expiresAt: "2026-08-06T12:00:00.000Z",
      createdAt: consentedAt,
    });
    expect(result.success).toBe(false);
  });
});

describe("conversation authority", () => {
  it("uses an order-independent exact participant digest", () => {
    expect(participantSetDigest([identityA, identityB])).toBe(participantSetDigest([identityB, identityA]));
  });

  it("fails closed for consent gaps and STOP", () => {
    const missingConsent = trustedSnapshot();
    missingConsent.participants = missingConsent.participants.map((participant, index) =>
      index === 1 ? { ...participant, consentedAt: null } : participant,
    );
    expect(evaluateConversationMode(missingConsent)).toBe("observe_only");

    const unregisteredDirect = trustedSnapshot("direct");
    unregisteredDirect.participants = unregisteredDirect.participants.map((participant, index) =>
      index === 1 ? { ...participant, consentedAt: null } : participant,
    );
    expect(evaluateConversationMode(unregisteredDirect)).toBe("registration_required");

    const stopped = trustedSnapshot();
    stopped.activeSuppressions = [{ id: suppressionId, kind: "stop", retentionSeconds: null }];
    expect(evaluateConversationMode(stopped)).toBe("paused");
  });

  it("reauthorizes the exact live participant set before sending", () => {
    const result = authorizeSendFromSnapshot(trustedSnapshot(), {
      conversationId,
      expectedParticipantEpochId: epochId,
      expectedParticipantSetDigest: digest,
      liveParticipantIdentityIds: [identityA],
      sendKind: "direct_response",
      operation: "reply",
      ruleId: null,
    });
    expect(result).toMatchObject({ allowed: false, reason: "live_participant_mismatch" });
  });

  it("requires both intersected policy and an epoch-bound rule for proactivity", () => {
    const allowed = authorizeSendFromSnapshot(trustedSnapshot(), {
      conversationId,
      expectedParticipantEpochId: epochId,
      expectedParticipantSetDigest: digest,
      liveParticipantIdentityIds: [identityB, identityA],
      sendKind: "proactive",
      operation: "coverage_reminder",
      ruleId,
    });
    expect(allowed).toMatchObject({ allowed: true, effectiveRetentionSeconds: 3_600 });

    const withoutRule = authorizeSendFromSnapshot(trustedSnapshot(), {
      conversationId,
      expectedParticipantEpochId: epochId,
      expectedParticipantSetDigest: digest,
      liveParticipantIdentityIds: [identityA, identityB],
      sendKind: "proactive",
      operation: "daily_brief",
      ruleId,
    });
    expect(withoutRule).toMatchObject({ allowed: false, reason: "group_write_rule_missing" });
  });

  it("requires a matching exact-audience rule for every group reply", () => {
    const withoutRule = authorizeSendFromSnapshot(trustedSnapshot(), {
      conversationId,
      expectedParticipantEpochId: epochId,
      expectedParticipantSetDigest: digest,
      liveParticipantIdentityIds: [identityA, identityB],
      sendKind: "direct_response",
      operation: "reply",
      ruleId: null,
    });
    expect(withoutRule).toMatchObject({ allowed: false, reason: "group_write_rule_missing" });

    const withRule = authorizeSendFromSnapshot(trustedSnapshot(), {
      conversationId,
      expectedParticipantEpochId: epochId,
      expectedParticipantSetDigest: digest,
      liveParticipantIdentityIds: [identityA, identityB],
      sendKind: "direct_response",
      operation: "reply",
      ruleId,
    });
    expect(withRule).toMatchObject({ allowed: true, reason: "allowed" });
  });

  it("keeps eligible direct DMs reply-capable without a group rule", () => {
    const direct = trustedSnapshot("direct");
    direct.rules = [];
    expect(evaluateConversationMode(direct)).toBe("trusted_write_enabled");

    const reply = authorizeSendFromSnapshot(direct, {
      conversationId,
      expectedParticipantEpochId: epochId,
      expectedParticipantSetDigest: digest,
      liveParticipantIdentityIds: [identityA, identityB],
      sendKind: "direct_response",
      operation: "reply",
      ruleId: null,
    });
    expect(reply).toMatchObject({ allowed: true, reason: "allowed" });
  });

  it("honors a participant's personal proactive pause without disabling replies or content", () => {
    const paused = trustedSnapshot();
    paused.participants = paused.participants.map((participant, index) =>
      index === 1 ? { ...participant, proactivePaused: true } : participant,
    );
    expect(evaluateConversationMode(paused)).toBe("trusted_write_enabled");

    const proactive = authorizeSendFromSnapshot(paused, {
      conversationId,
      expectedParticipantEpochId: epochId,
      expectedParticipantSetDigest: digest,
      liveParticipantIdentityIds: [identityA, identityB],
      sendKind: "proactive",
      operation: "coverage_reminder",
      ruleId,
    });
    expect(proactive).toMatchObject({ allowed: false, reason: "participant_proactive_paused" });

    const reply = authorizeSendFromSnapshot(paused, {
      conversationId,
      expectedParticipantEpochId: epochId,
      expectedParticipantSetDigest: digest,
      liveParticipantIdentityIds: [identityA, identityB],
      sendKind: "direct_response",
      operation: "reply",
      ruleId,
    });
    expect(reply).toMatchObject({ allowed: true, reason: "allowed" });
  });

  it("classifies any authority expansion as widening", () => {
    const current = {
      allowContentProcessing: true,
      allowDirectResponses: true,
      allowProactiveWrites: false,
      retentionSeconds: 3_600,
    };
    expect(isPolicyWidening(current, { ...current, retentionSeconds: 60 })).toBe(false);
    expect(isPolicyWidening(current, { ...current, allowProactiveWrites: true })).toBe(true);
  });
});
