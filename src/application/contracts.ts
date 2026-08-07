import type { LinqChatSnapshot, LinqWebhookEnvelope } from "../adapters/linq/index.js";
import type { PrivateBridgeProposalInput } from "../modules/bridges/index.js";
import type { ResolvedTimePlan } from "../modules/coordination/index.js";
import type { IntegrationAccountKind, IntegrationCapability, JsonObject } from "../modules/sources/index.js";
import type { TimerProcessPayload } from "../modules/work/index.js";

export interface ApplicationTimerProcessor {
  process(payload: TimerProcessPayload): Promise<unknown>;
}

export interface WebRoutineFields {
  readonly destinationConversationId: string;
  readonly title: string;
  readonly sharedMeaning: string;
  readonly weekdays: readonly number[];
  readonly startsOn: string;
  readonly endsOn: string | null;
  readonly timeZone: string;
  readonly localEventTime: string;
  readonly earliestUsefulMinutesBefore: number;
  readonly lastResponsibleMinutesBefore: number;
  readonly notificationMode: "exceptions_only" | "always" | "silent";
  readonly usualPersonId: string | null;
  readonly standingSelfCoverage: boolean;
}

/** A committed OAuth result or settled durable job is enough to observe Google progress. */
export interface GoogleSyncObservationFields {
  readonly integrationId: string;
  readonly personId: string;
  readonly triggeringJobId: string | null;
}

/** A worker may interpret evidence, but it cannot choose mutation authority or a loop target. */
export type CoverageProposal =
  | {
      readonly kind: "need_proposed";
      readonly internalProviderEventId: string;
      readonly evidenceSourceRevisionIds: readonly string[];
      readonly minimumSharedMeaning: string;
      readonly unresolvedFacts: readonly string[];
      readonly proposedHolderPersonId: string | null;
      readonly timing: ResolvedTimePlan;
      readonly consequentialQuestion: string | null;
    }
  | {
      readonly kind: "self_response_proposed";
      readonly internalProviderEventId: string;
      readonly evidenceSourceRevisionIds: readonly string[];
      readonly response: "acknowledge" | "decline" | "ambiguous";
      readonly explicitSelfStatement: boolean;
      readonly confidence: number;
    };

export type AppEnvelope =
  | {
      readonly kind: "linq.webhook";
      readonly event: LinqWebhookEnvelope;
      readonly liveChat: LinqChatSnapshot | null;
    }
  | {
      readonly kind: "linq.process_event";
      readonly providerEventId: string;
    }
  | {
      /** Commits an ephemeral worker's response through current exact private authority. */
      readonly kind: "linq.private_invocation_response";
      readonly internalProviderEventId: string;
      readonly responseText: string;
      readonly evidenceSourceRevisionIds: readonly string[];
    }
  | {
      /** Reconciles an authoritative live audience before an outbound cross-chat action. */
      readonly kind: "linq.reconcile_chat";
      readonly liveChat: LinqChatSnapshot;
    }
  | {
      readonly kind: "timer.process";
      readonly timer: TimerProcessPayload;
    }
  | {
      readonly kind: "private_bridge.proposal";
      readonly proposal: PrivateBridgeProposalInput;
    }
  | {
      readonly kind: "private_bridge.commit";
      readonly actionIntentId: string;
    }
  | {
      /** Commits a bounded coverage proposal only after reopening current source authority. */
      readonly kind: "coverage.apply";
      readonly proposal: CoverageProposal;
    }
  | {
      readonly kind: "maintenance.materialize_routines";
      readonly fromLocalDate: string;
      readonly throughLocalDate: string;
      readonly materializedAt: string;
      readonly afterRoutineId: string | null;
      readonly maxOccurrences: number;
    }
  | {
      readonly kind: "maintenance.redrive_effects";
      readonly asOf: string;
      readonly limit: number;
    }
  | {
      readonly kind: "google.oauth.begin";
      readonly personId: string;
      readonly initiatingSessionId: string;
      readonly stateDigest: string;
      readonly pkceVerifier: string;
      readonly returnPath: string;
      readonly requestedCapabilities: readonly IntegrationCapability[];
      readonly accountKind: IntegrationAccountKind;
      readonly expectedPersonControlEpoch: number;
      readonly expiresAt: string;
      readonly createdAt: string;
    }
  | {
      readonly kind: "google.oauth.complete";
      readonly stateDigest: string;
      readonly externalSubjectDigest: string;
      readonly credentials: JsonObject;
      readonly grantedCapabilities: readonly IntegrationCapability[];
      readonly completedAt: string;
    }
  | ({ readonly kind: "google.sync.observe" } & GoogleSyncObservationFields)
  | {
      readonly kind: "web.command";
      readonly actorPersonId: string;
      readonly command:
        | { readonly kind: "create_household" }
        | {
            readonly kind: "invite_household_participant";
            readonly householdId: string;
            readonly conversationId: string;
            readonly inviteePersonId: string;
            readonly role: "steward" | "caregiver" | "participant";
          }
        | { readonly kind: "approve_household_invitation"; readonly invitationId: string }
        | { readonly kind: "accept_household_invitation"; readonly invitationId: string }
        | {
            readonly kind: "add_dependent";
            readonly householdId: string;
            readonly displayName: string;
            readonly aliases: readonly string[];
            readonly birthYear: number | null;
            readonly school: string;
            readonly activities: readonly string[];
          }
        | {
            readonly kind: "update_dependent";
            readonly householdId: string;
            readonly dependentPersonId: string;
            readonly displayName: string;
            readonly aliases: readonly string[];
            readonly birthYear: number | null;
            readonly school: string;
            readonly activities: readonly string[];
          }
        | { readonly kind: "approve_group_coverage_rule"; readonly conversationId: string }
        | ({ readonly kind: "create_routine" } & WebRoutineFields)
        | ({
            readonly kind: "revise_routine";
            readonly routineId: string;
            readonly expectedVersion: number;
          } & WebRoutineFields)
        | {
            readonly kind: "set_routine_status";
            readonly routineId: string;
            readonly expectedVersion: number;
            readonly status: "active" | "paused" | "retired";
          }
        | {
            readonly kind: "set_calendar_mode";
            readonly integrationId: string;
            readonly calendarId: string;
            readonly mode: "full_private" | "availability_only" | "off";
          }
        | {
            readonly kind: "review_private_candidate";
            readonly candidateId: string;
            readonly decision: "accepted" | "rejected";
          }
        | {
            readonly kind: "prepare_private_bridge";
            readonly candidateId: string;
            readonly conversationId: string;
          }
        | {
            readonly kind: "approve_private_bridge";
            readonly actionIntentId: string;
            readonly actionDigest: string;
            readonly dataDigest: string;
            readonly policyDigest: string;
            readonly targetDigest: string;
            readonly mode: "once" | "standing";
          }
        | { readonly kind: "forget_memory"; readonly memoryId: string }
        | { readonly kind: "revoke_bridge_rule"; readonly ruleId: string }
        | { readonly kind: "disconnect_integration"; readonly integrationId: string }
        | { readonly kind: "revoke_session"; readonly sessionId: string }
        | { readonly kind: "delete_person" }
        | { readonly kind: "pause_person"; readonly paused: boolean }
        | { readonly kind: "request_step_up"; readonly purpose: "account_controls" | "google_connect" };
    };

export interface ProcessReceipt {
  readonly accepted: boolean;
  readonly duplicate: boolean;
  readonly disposition: string;
  readonly ids: Readonly<Record<string, string>>;
}
