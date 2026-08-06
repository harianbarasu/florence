import type { z } from "zod";
import type { ModelGateway, WorkerJob, WorkerResult, WorkerRuntime } from "./contracts.js";

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
    if (job.budget.maxModelCalls < 1)
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
      const errorCode =
        error instanceof Error && error.name === "AbortError" ? "deadline_exceeded" : "model_failed";
      return failedResult(job, startedAt, this.runtimeRoute, errorCode);
    }
  }

  /** The persistence-free delegate has no reconciliation ledger of its own. */
  public async reconcile(
    _attemptId: string,
    _status: "accepted" | "partially_accepted" | "rejected" | "stale",
  ): Promise<void> {}
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
