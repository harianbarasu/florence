import type { WorkerAttemptOptions, WorkerJob } from "../runtime/index.js";
import type { ConversationInboxItem, GmailInboxItem } from "./contracts.js";
import {
  type ApplicationOutboxIntent,
  type ApplicationResult,
  ApplicationResultSchema,
  type ConversationClassification,
  EffectExecutionReceiptSchema,
  type GmailTriageResult,
  type HouseholdApplicationSnapshot,
  HouseholdApplicationSnapshotSchema,
} from "./contracts.js";
import type {
  ApplicationCommit,
  ApplicationCommitResult,
  ApplicationEffectExecutorPort,
  ApplicationInterpreterPort,
  ApplicationRepositoryPort,
  ConversationInterpretationContext,
  GmailTriageContext,
  WorkerContextPort,
} from "./ports.js";

function clone<T>(value: T): T {
  return structuredClone(value);
}

function repositoryKey(householdId: string, idempotencyKey: string): string {
  return `${householdId}\u0000${idempotencyKey}`;
}

export class InMemoryApplicationRepository implements ApplicationRepositoryPort {
  readonly commits: ApplicationCommit[] = [];
  readonly outbox: ApplicationOutboxIntent[] = [];
  readonly snapshots = new Map<string, HouseholdApplicationSnapshot>();
  readonly #processed = new Map<string, ApplicationResult>();

  seed(snapshot: HouseholdApplicationSnapshot): void {
    const parsed = HouseholdApplicationSnapshotSchema.parse(snapshot);
    this.snapshots.set(parsed.aggregate.householdId, clone(parsed));
  }

  async load(householdId: string): Promise<HouseholdApplicationSnapshot | null> {
    const snapshot = this.snapshots.get(householdId);
    return snapshot === undefined ? null : clone(snapshot);
  }

  async findProcessed(householdId: string, idempotencyKey: string): Promise<ApplicationResult | null> {
    const result = this.#processed.get(repositoryKey(householdId, idempotencyKey));
    return result === undefined ? null : clone(result);
  }

  async commit(input: ApplicationCommit): Promise<ApplicationCommitResult> {
    const key = repositoryKey(input.householdId, input.idempotencyKey);
    const duplicate = this.#processed.get(key);
    if (duplicate !== undefined) {
      return {
        disposition: "duplicate",
        revision: duplicate.revision,
        outcome: clone(duplicate.outcome),
      };
    }
    const current = this.snapshots.get(input.householdId);
    if (current === undefined) {
      throw new Error(`Unknown household application: ${input.householdId}`);
    }
    if (current.revision !== input.expectedRevision) {
      return { disposition: "conflict", actualRevision: current.revision };
    }
    const revision = current.revision + 1;
    const next = HouseholdApplicationSnapshotSchema.parse({
      revision,
      aggregate: input.aggregate,
      projection: input.projection,
    });
    const result = ApplicationResultSchema.parse({
      householdId: input.householdId,
      idempotencyKey: input.idempotencyKey,
      disposition: "committed",
      revision,
      outcome: input.outcome,
    });
    this.snapshots.set(input.householdId, clone(next));
    this.#processed.set(key, clone(result));
    this.commits.push(clone(input));
    this.outbox.push(...clone(input.outbox));
    return { disposition: "committed", revision, outcome: clone(input.outcome) };
  }

  intents(kind?: ApplicationOutboxIntent["kind"]): ApplicationOutboxIntent[] {
    return clone(kind === undefined ? this.outbox : this.outbox.filter((intent) => intent.kind === kind));
  }
}

export class FakeApplicationInterpreter implements ApplicationInterpreterPort {
  readonly conversationCalls: Array<{
    input: ConversationInboxItem;
    context: ConversationInterpretationContext;
  }> = [];
  readonly gmailCalls: Array<{ input: GmailInboxItem; context: GmailTriageContext }> = [];
  readonly #conversation = new Map<string, ConversationClassification | unknown>();
  readonly #gmail = new Map<string, GmailTriageResult | unknown>();

  respondToConversation(idempotencyKey: string, response: ConversationClassification | unknown): void {
    this.#conversation.set(idempotencyKey, clone(response));
  }

  respondToGmail(idempotencyKey: string, response: GmailTriageResult | unknown): void {
    this.#gmail.set(idempotencyKey, clone(response));
  }

  async interpretConversation(
    input: ConversationInboxItem,
    context: ConversationInterpretationContext,
  ): Promise<ConversationClassification | unknown> {
    this.conversationCalls.push({ input: clone(input), context: clone(context) });
    if (!this.#conversation.has(input.idempotencyKey)) {
      throw new Error(`No conversation interpretation for ${input.idempotencyKey}`);
    }
    return clone(this.#conversation.get(input.idempotencyKey));
  }

  async triageGmail(
    input: GmailInboxItem,
    context: GmailTriageContext,
  ): Promise<GmailTriageResult | unknown> {
    this.gmailCalls.push({ input: clone(input), context: clone(context) });
    if (!this.#gmail.has(input.idempotencyKey)) {
      throw new Error(`No Gmail triage response for ${input.idempotencyKey}`);
    }
    return clone(this.#gmail.get(input.idempotencyKey));
  }
}

export class FakeWorkerContext implements WorkerContextPort {
  readonly calls: WorkerJob[] = [];
  readonly #options = new Map<string, WorkerAttemptOptions>();

  set(jobId: string, options: WorkerAttemptOptions): void {
    this.#options.set(jobId, options);
  }

  async contextFor(job: WorkerJob, _snapshot: HouseholdApplicationSnapshot): Promise<WorkerAttemptOptions> {
    this.calls.push(clone(job));
    return this.#options.get(job.jobId) ?? {};
  }
}

export class FakeApplicationEffectExecutor implements ApplicationEffectExecutorPort {
  readonly calls: Array<Exclude<ApplicationOutboxIntent, { kind: "worker.run" }>> = [];
  readonly #receipts = new Map<string, unknown>();

  respond(intentId: string, receipt: unknown): void {
    this.#receipts.set(intentId, clone(receipt));
  }

  async execute(intent: Exclude<ApplicationOutboxIntent, { kind: "worker.run" }>) {
    this.calls.push(clone(intent));
    const receipt = this.#receipts.get(intent.intentId) ?? {
      status: "succeeded",
      receiptRef: `receipt_${intent.intentId}`,
      recordedAt: "2026-01-01T00:00:00Z",
    };
    return EffectExecutionReceiptSchema.parse(clone(receipt));
  }
}
