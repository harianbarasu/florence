import type { z } from "zod";
import type { ModelGateway, WorkerJob, WorkerResult, WorkerRuntime } from "./contracts.js";

const RETRYABLE_WORKER_ERROR_CODES = new Set(["deadline_exceeded", "model_failed"]);

/**
 * Provider-neutral failure raised when durable orchestration cannot obtain a
 * proposal from one bounded worker attempt. The result remains persistable by
 * the governed runtime; callers decide whether the enclosing durable job has
 * enough attempts and time left to retry.
 */
export class WorkerAttemptError extends Error {
  public readonly attemptId: string;
  public readonly code: string;
  public readonly retryable: boolean;

  public constructor(result: Pick<WorkerResult, "attemptId" | "status" | "errorCode">) {
    const resultCode = result.errorCode ?? result.status;
    super(`Bounded worker attempt did not produce a proposal: ${resultCode}`);
    this.name = "WorkerAttemptError";
    this.attemptId = result.attemptId;
    this.code = `worker_${resultCode.replace(/[^a-z0-9_]+/gu, "_").slice(0, 120)}`;
    this.retryable = RETRYABLE_WORKER_ERROR_CODES.has(resultCode);
  }
}

/** Converts an ephemeral result into a proposal without hiding failed work. */
export function requireWorkerProposal<Output>(result: WorkerResult<Output>): Output {
  if (result.status === "proposed" && result.proposal !== undefined) return result.proposal;
  throw new WorkerAttemptError(result);
}

export class BoundedWorkerRuntime implements WorkerRuntime {
  public constructor(
    private readonly gateway: ModelGateway,
    private readonly runtimeRoute: string,
  ) {}

  public async run<Schema extends z.ZodType>(
    job: WorkerJob<Schema>,
  ): Promise<WorkerResult<z.output<Schema>>> {
    const startedAt = new Date();
    if (job.deadline.getTime() <= startedAt.getTime())
      return expiredResult(job, startedAt, this.runtimeRoute);
    if (job.budget.maxModelCalls < 1 || job.budget.maxOutputTokens < 1)
      return failedResult(job, startedAt, this.runtimeRoute, "budget_exhausted");

    try {
      const proposal = await this.gateway.completeStructured({
        profile:
          job.skill.id === "general.answer"
            ? "general_answer"
            : job.skill.riskClass === "high"
              ? "careful_coordination"
              : "fast_private_triage",
        system: job.skill.instructions,
        user: `Goal:\n${job.goal}\n\nAuthorized evidence context:\n${job.authorizedContext}`,
        schema: job.skill.outputSchema,
        schemaName: job.skill.outputSchemaName,
        timeoutMs: Math.max(1, job.deadline.getTime() - Date.now()),
        maxOutputTokens: job.budget.maxOutputTokens,
        ...(job.images ? { images: job.images } : {}),
      });
      return {
        attemptId: job.attemptId,
        taskVersionId: job.taskVersionId,
        skillId: job.skill.id,
        skillVersion: job.skill.version,
        evaluationRelease: job.skill.evaluationRelease,
        runtimeRoute: this.runtimeRoute,
        status: "proposed",
        proposal,
        startedAt,
        completedAt: new Date(),
      };
    } catch (error) {
      const errorCode = isModelTimeout(error) ? "deadline_exceeded" : "model_failed";
      return failedResult(job, startedAt, this.runtimeRoute, errorCode);
    }
  }

  /** The persistence-free delegate has no reconciliation ledger of its own. */
  public async reconcile(
    _attemptId: string,
    _status: "accepted" | "partially_accepted" | "rejected" | "stale",
  ): Promise<void> {}
}

function isModelTimeout(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  if (error.name === "AbortError" || error.name === "TimeoutError") return true;
  const code = "code" in error && typeof error.code === "string" ? error.code : null;
  return code === "ETIMEDOUT" || code === "ECONNABORTED";
}

function expiredResult<Schema extends z.ZodType>(
  job: WorkerJob<Schema>,
  startedAt: Date,
  runtimeRoute: string,
): WorkerResult<z.output<Schema>> {
  return {
    attemptId: job.attemptId,
    taskVersionId: job.taskVersionId,
    skillId: job.skill.id,
    skillVersion: job.skill.version,
    evaluationRelease: job.skill.evaluationRelease,
    runtimeRoute,
    status: "expired",
    errorCode: "deadline_exceeded",
    startedAt,
    completedAt: new Date(),
  };
}

function failedResult<Schema extends z.ZodType>(
  job: WorkerJob<Schema>,
  startedAt: Date,
  runtimeRoute: string,
  errorCode: string,
): WorkerResult<z.output<Schema>> {
  return {
    attemptId: job.attemptId,
    taskVersionId: job.taskVersionId,
    skillId: job.skill.id,
    skillVersion: job.skill.version,
    evaluationRelease: job.skill.evaluationRelease,
    runtimeRoute,
    status: "failed",
    errorCode,
    startedAt,
    completedAt: new Date(),
  };
}
