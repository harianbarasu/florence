import { canonicalDigest } from "../../shared/canonical-json.js";
import {
  type AuthorizeSendInput,
  AuthorizeSendInputSchema,
  type ConversationAuthoritySnapshot,
  ConversationAuthoritySnapshotSchema,
  type ConversationMode,
  type ParticipantPolicyValue,
  SendAuthorizationSchema,
} from "./contracts.js";

export function participantSetDigest(identityIds: readonly string[]): string {
  return canonicalDigest({ participantIdentityIds: [...identityIds].sort() });
}

export function participantApprovalDigest(personIds: readonly string[]): string {
  return canonicalDigest({ approvedByPersonIds: [...personIds].sort() });
}

export function participantEpochAuthorityDigest(input: {
  readonly conversationId: string;
  readonly sequence: number;
  readonly participantSetDigest: string;
}): string {
  return canonicalDigest(input);
}

export function isPolicyWidening(
  current: ParticipantPolicyValue | null,
  next: ParticipantPolicyValue,
): boolean {
  if (current === null) {
    return (
      next.allowContentProcessing ||
      next.allowDirectResponses ||
      next.allowProactiveWrites ||
      next.retentionSeconds > 0
    );
  }
  return (
    (!current.allowContentProcessing && next.allowContentProcessing) ||
    (!current.allowDirectResponses && next.allowDirectResponses) ||
    (!current.allowProactiveWrites && next.allowProactiveWrites) ||
    next.retentionSeconds > current.retentionSeconds
  );
}

export function evaluateConversationMode(snapshotCandidate: ConversationAuthoritySnapshot): ConversationMode {
  const snapshot = ConversationAuthoritySnapshotSchema.parse(snapshotCandidate);
  if (
    snapshot.conversationStatus !== "active" ||
    snapshot.activeSuppressions.some((entry) =>
      ["stop", "pause", "deletion_fence", "safety_hold"].includes(entry.kind),
    )
  ) {
    return "paused";
  }
  if (
    snapshot.participantEpochId === null ||
    snapshot.participants.length === 0 ||
    snapshot.participants.some(
      (participant) =>
        participant.registrationStatus !== "registered" ||
        participant.consentedAt === null ||
        participant.policy === null ||
        !participant.policy.allowContentProcessing,
    )
  ) {
    return "content_disabled";
  }
  if (
    snapshot.activeSuppressions.some((entry) => entry.kind === "read_only") ||
    snapshot.participants.some((participant) => !participant.policy?.allowDirectResponses)
  ) {
    return "read_enabled_write_disabled";
  }
  return "trusted_write_enabled";
}

export function authorizeSendFromSnapshot(
  snapshotCandidate: ConversationAuthoritySnapshot,
  inputCandidate: AuthorizeSendInput,
) {
  const snapshot = ConversationAuthoritySnapshotSchema.parse(snapshotCandidate);
  const input = AuthorizeSendInputSchema.parse(inputCandidate);
  const mode = evaluateConversationMode(snapshot);
  const base = {
    mode,
    conversationId: snapshot.conversationId,
    participantEpochId: snapshot.participantEpochId,
    participantSetDigest: snapshot.participantSetDigest,
    authorityVersion: snapshot.authorityVersion,
    effectiveRetentionSeconds: effectiveRetention(snapshot),
  } as const;
  if (mode === "paused")
    return SendAuthorizationSchema.parse({ ...base, allowed: false, reason: "conversation_paused" });
  if (mode === "content_disabled") {
    return SendAuthorizationSchema.parse({ ...base, allowed: false, reason: "content_disabled" });
  }
  if (
    snapshot.participantEpochId !== input.expectedParticipantEpochId ||
    snapshot.participantSetDigest !== input.expectedParticipantSetDigest
  ) {
    return SendAuthorizationSchema.parse({ ...base, allowed: false, reason: "epoch_mismatch" });
  }
  if (participantSetDigest(input.liveParticipantIdentityIds) !== snapshot.participantSetDigest) {
    return SendAuthorizationSchema.parse({ ...base, allowed: false, reason: "live_participant_mismatch" });
  }
  if (
    mode !== "trusted_write_enabled" ||
    snapshot.participants.some((participant) => !participant.policy?.allowDirectResponses)
  ) {
    return SendAuthorizationSchema.parse({ ...base, allowed: false, reason: "participant_policy_denied" });
  }
  if (input.sendKind === "proactive") {
    if (snapshot.participants.some((participant) => participant.proactivePaused)) {
      return SendAuthorizationSchema.parse({
        ...base,
        allowed: false,
        reason: "participant_proactive_paused",
      });
    }
    if (snapshot.participants.some((participant) => !participant.policy?.allowProactiveWrites)) {
      return SendAuthorizationSchema.parse({ ...base, allowed: false, reason: "participant_policy_denied" });
    }
    const rule = snapshot.rules.find(
      (candidate) =>
        candidate.ruleId === input.ruleId &&
        candidate.active &&
        candidate.participantSetDigest === snapshot.participantSetDigest &&
        candidate.allowedOperations.includes(input.operation),
    );
    if (!rule)
      return SendAuthorizationSchema.parse({ ...base, allowed: false, reason: "proactive_rule_missing" });
  }
  return SendAuthorizationSchema.parse({ ...base, allowed: true, reason: "allowed" });
}

function effectiveRetention(snapshot: ConversationAuthoritySnapshot): number | null {
  const values = snapshot.participants.flatMap((participant) =>
    participant.policy ? [participant.policy.retentionSeconds] : [],
  );
  for (const suppression of snapshot.activeSuppressions) {
    if (suppression.kind === "retention_cap" && suppression.retentionSeconds !== null) {
      values.push(suppression.retentionSeconds);
    }
  }
  return values.length === 0 ? null : Math.min(...values);
}
