import type { DomainChange, HouseholdAggregate, HouseholdSignal } from "../domain/index.js";
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
  readonly onboarding: ApplicationProjection["onboarding"];
  readonly sharedProfile: ApplicationProjection["sharedProfile"];
  readonly openEpisodes: readonly {
    readonly episodeId: string;
    readonly type: "commitment" | "research" | "meal_plan";
    readonly state: string;
    readonly title: string;
    readonly ownerAdultId?: string;
    readonly version: number;
  }[];
  readonly pendingPromotionIds: readonly string[];
}

export interface GmailTriageContext {
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

export interface FlorenceApplicationDependencies {
  readonly repository: ApplicationRepositoryPort;
  readonly interpreter: ApplicationInterpreterPort;
  readonly workerRuntime: WorkerRuntime;
  readonly workerContext: WorkerContextPort;
  readonly effectExecutor: ApplicationEffectExecutorPort;
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
