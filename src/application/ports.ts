import type {
  CalendarEventCreateAction,
  DomainChange,
  HouseholdAggregate,
  HouseholdSignal,
} from "../domain/index.js";
import type { WorkerAttemptOptions, WorkerJob, WorkerResult, WorkerRuntime } from "../runtime/index.js";
import type {
  ApplicationAuditEntry,
  ApplicationOutboxIntent,
  ApplicationOutcome,
  ApplicationProjection,
  ApplicationResult,
  ConversationClassification,
  ConversationInboxItem,
  EffectExecutionReceipt,
  GmailInboxItem,
  GmailTriageResult,
  HouseholdApplicationSnapshot,
  WorkerRoutes,
} from "./contracts.js";

export interface ConversationInterpretationContext {
  readonly currentTime: string;
  readonly householdTimeZone: string;
  readonly onboarding: ApplicationProjection["onboarding"];
  readonly sharedProfile: ApplicationProjection["sharedProfile"];
  readonly confirmedRoutineAnchors: ReadonlyArray<HouseholdAggregate["routineAnchors"][number]>;
  readonly openEpisodes: readonly {
    readonly episodeId: string;
    readonly type: "commitment" | "research" | "meal_plan";
    readonly state: string;
    readonly title: string;
    readonly ownerAdultId?: string;
    readonly version: number;
  }[];
  readonly pendingPromotionIds: readonly string[];
  readonly activePolicies: readonly {
    readonly policyId: string;
    readonly policyVersion: number;
    readonly kind: "sharing" | "routing" | "timing" | "internal_action";
    readonly description: string;
  }[];
  readonly pendingCalendarActions: readonly {
    readonly actionId: string;
    readonly title: string;
    readonly startsAt: string;
    readonly endsAt: string;
    readonly timeZone: string;
    readonly hasConflict: boolean;
  }[];
}

export interface GmailTriageContext {
  readonly confirmedRoutineAnchors: ReadonlyArray<HouseholdAggregate["routineAnchors"][number]>;
  readonly activeSharingRules: readonly {
    readonly policyId: string;
    readonly policyVersion: number;
    readonly sourceClass: string;
    readonly maximumSensitivity: "ordinary" | "sensitive";
  }[];
}

/** Model-backed implementations may sit behind this app-owned, strictly parsed seam. */
export interface ApplicationInterpreterPort {
  interpretConversation(
    input: ConversationInboxItem,
    context: ConversationInterpretationContext,
  ): Promise<ConversationClassification | unknown>;

  triageGmail(input: GmailInboxItem, context: GmailTriageContext): Promise<GmailTriageResult | unknown>;
}

export interface ApplicationCommit {
  readonly householdId: string;
  readonly idempotencyKey: string;
  readonly expectedRevision: number;
  readonly aggregate: HouseholdAggregate;
  readonly projection: ApplicationProjection;
  readonly signals: readonly HouseholdSignal[];
  readonly changes: readonly DomainChange[];
  readonly outbox: readonly ApplicationOutboxIntent[];
  readonly audit: readonly ApplicationAuditEntry[];
  readonly outcome: ApplicationOutcome;
}

export type ApplicationCommitResult =
  | {
      readonly disposition: "committed";
      readonly revision: number;
      readonly outcome: ApplicationOutcome;
    }
  | {
      readonly disposition: "duplicate";
      readonly revision: number;
      readonly outcome: ApplicationOutcome;
    }
  | {
      readonly disposition: "conflict";
      readonly actualRevision: number;
    };

/**
 * The implementation must atomically persist the aggregate, application projection,
 * audit entries, and all outbox intents, guarded by expectedRevision and idempotencyKey.
 */
export interface ApplicationRepositoryPort {
  load(householdId: string): Promise<HouseholdApplicationSnapshot | null>;
  findProcessed(householdId: string, idempotencyKey: string): Promise<ApplicationResult | null>;
  commit(input: ApplicationCommit): Promise<ApplicationCommitResult>;
}

export interface WorkerContextPort {
  contextFor(job: WorkerJob, snapshot: HouseholdApplicationSnapshot): Promise<WorkerAttemptOptions>;
}

export interface ApplicationEffectExecutorPort {
  execute(intent: Exclude<ApplicationOutboxIntent, { kind: "worker.run" }>): Promise<EffectExecutionReceipt>;
}

export type CalendarCreatePreparation =
  | {
      readonly status: "ready";
      readonly targetConnectionId: string;
      readonly calendarId: "primary";
      readonly relevantDataDigest: string;
      readonly hasConflict: boolean;
    }
  | {
      readonly status: "unavailable";
      readonly reason: "no_write_connection" | "ambiguous_write_connection" | "projection_incomplete";
    };

/** Trusted, model-free Calendar preflight. It exposes no personal event details. */
export interface HouseholdCalendarActionsPort {
  prepareCreate(input: {
    readonly householdId: string;
    readonly verifiedAdultIds: readonly string[];
    readonly requestedByAdultId: string;
    readonly asOf: string;
    readonly startsAt: string;
    readonly endsAt: string;
    readonly accountLabel?: string;
    readonly targetConnectionId?: string;
  }): Promise<CalendarCreatePreparation>;

  createApprovedEvent(input: {
    readonly action: CalendarEventCreateAction;
    readonly idempotencyKey: string;
    readonly asOf: string;
  }): Promise<{
    readonly provider: "google-calendar";
    readonly providerReference: string;
  }>;
}

export interface FlorenceApplicationDependencies {
  readonly repository: ApplicationRepositoryPort;
  readonly interpreter: ApplicationInterpreterPort;
  readonly workerRuntime: WorkerRuntime;
  readonly workerContext: WorkerContextPort;
  readonly effectExecutor: ApplicationEffectExecutorPort;
  readonly calendarActions?: HouseholdCalendarActionsPort;
  readonly workerRoutes?: WorkerRoutes;
}

export interface FlorenceApplication {
  process(input: unknown): Promise<ApplicationResult>;
  executeOutbox(
    intent: unknown,
    executedAt: string,
  ): Promise<{
    intentId: string;
    status: "succeeded" | "retryable_failure" | "permanent_failure";
    applicationResult?: ApplicationResult | undefined;
  }>;
}

export type { WorkerResult };
