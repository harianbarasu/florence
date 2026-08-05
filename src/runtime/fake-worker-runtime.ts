import {
  type WorkerAttemptOptions,
  WorkerContextItemSchema,
  type WorkerJob,
  WorkerJobSchema,
  type WorkerResult,
  type WorkerResultPayload,
  WorkerResultPayloadSchema,
  WorkerResultSchema,
  type WorkerRuntime,
  type WorkerToolCleanupContext,
} from "./contracts.js";
import { asWorkerRuntimeError, WorkerRuntimeError } from "./errors.js";

export type FakeWorkerResponder = (
  job: WorkerJob,
  options: WorkerAttemptOptions,
) => WorkerResultPayload | WorkerResult | Promise<WorkerResultPayload | WorkerResult>;

export class FakeWorkerRuntime implements WorkerRuntime {
  readonly #responder: FakeWorkerResponder;
  readonly #calls: WorkerJob[] = [];

  constructor(responder: FakeWorkerResponder | WorkerResultPayload) {
    this.#responder = typeof responder === "function" ? responder : () => responder;
  }

  get calls(): readonly WorkerJob[] {
    return this.#calls;
  }

  async run(jobCandidate: WorkerJob, options: WorkerAttemptOptions = {}): Promise<WorkerResult> {
    const parsedJob = WorkerJobSchema.safeParse(jobCandidate);
    if (!parsedJob.success || !validContext(options, parsedJob.data)) {
      throw new WorkerRuntimeError("invalid_job");
    }
    const job = parsedJob.data;
    this.#calls.push(job);
    const cleanupContext = cleanupContextFor(job);
    let primaryError: WorkerRuntimeError | undefined;
    let outcome: WorkerResult | undefined;
    const startedAt = Date.now();

    try {
      if (options.signal?.aborted) {
        throw new WorkerRuntimeError("cancelled");
      }
      if (Date.parse(job.deadline) <= Date.now() || Date.parse(job.scopeGrant.expiresAt) <= Date.now()) {
        throw new WorkerRuntimeError("deadline_exceeded");
      }

      const candidate = await this.#responder(job, options);
      if (options.signal?.aborted) {
        throw new WorkerRuntimeError("cancelled");
      }
      if (Date.now() - startedAt > job.budget.maxDurationMs) {
        throw new WorkerRuntimeError("budget_exceeded");
      }
      if (Date.parse(job.deadline) <= Date.now() || Date.parse(job.scopeGrant.expiresAt) <= Date.now()) {
        throw new WorkerRuntimeError("deadline_exceeded");
      }
      const full = WorkerResultSchema.safeParse(candidate);
      if (full.success) {
        if (!identityMatches(job, full.data)) {
          throw new WorkerRuntimeError("invalid_output");
        }
        outcome = full.data;
      } else {
        const payload = WorkerResultPayloadSchema.safeParse(candidate);
        if (!payload.success) {
          throw new WorkerRuntimeError("invalid_output");
        }
        outcome = WorkerResultSchema.parse({
          ...payload.data,
          jobId: job.jobId,
          attemptId: job.attemptId,
          householdId: job.householdId,
          baseHouseholdVersion: job.baseHouseholdVersion,
          policyVersion: job.policyVersion,
          modelRouteId: job.modelRouteId,
          modelCapabilityProfile: job.modelCapabilityProfile,
          outputContractRef: job.outputContractRef,
          diagnostics: {
            durationMs: 0,
            modelCalls: 0,
            toolCalls: 0,
            usage: {},
            traceReferences: [],
          },
        });
      }
    } catch (error) {
      primaryError = asWorkerRuntimeError(error, {
        ...(options.signal === undefined ? {} : { cancelled: options.signal.aborted }),
      });
    }

    let cleanupFailed = false;
    try {
      await cleanupAttempt(options, cleanupContext);
    } catch {
      cleanupFailed = true;
    }
    if (primaryError !== undefined) {
      throw primaryError;
    }
    if (cleanupFailed) {
      throw new WorkerRuntimeError("cleanup_failed");
    }
    if (outcome === undefined) {
      throw new WorkerRuntimeError("runtime_failed");
    }
    return outcome;
  }
}

function validContext(options: WorkerAttemptOptions, job: WorkerJob): boolean {
  if ((options.context?.length ?? 0) > 100) {
    return false;
  }
  if (options.context?.some((item) => !WorkerContextItemSchema.safeParse(item).success)) {
    return false;
  }
  if (
    options.context?.some(
      (item) =>
        item.visibility === "personal" &&
        (job.scopeGrant.visibility === "household" || item.adultId !== job.scopeGrant.adultId),
    )
  ) {
    return false;
  }
  const toolNames = options.tools?.map((item) => item.name) ?? [];
  return (
    new Set(toolNames).size === toolNames.length &&
    toolNames.every((name) => job.allowedToolNames.includes(name)) &&
    job.allowedToolNames.every((name) => toolNames.includes(name)) &&
    (options.tools ?? []).every((tool) =>
      (tool.requiredCapabilityIds ?? []).every((capability) =>
        job.capabilityGrants.some(
          (grant) => grant.capability === capability && grant.revokedAt === undefined,
        ),
      ),
    ) &&
    ((options.tools ?? []).every((tool) => (tool.requiredCapabilityIds?.length ?? 0) === 0) ||
      options.capabilityAuthorizer !== undefined)
  );
}

async function cleanupAttempt(
  options: WorkerAttemptOptions,
  context: WorkerToolCleanupContext,
): Promise<void> {
  const failures = await Promise.allSettled([
    ...(options.tools ?? []).map((tool) => tool.cleanup?.(context) ?? Promise.resolve()),
    options.cleanup?.(context) ?? Promise.resolve(),
  ]);
  if (failures.some((result) => result.status === "rejected")) {
    throw new WorkerRuntimeError("cleanup_failed");
  }
}

function cleanupContextFor(job: WorkerJob): WorkerToolCleanupContext {
  return { jobId: job.jobId, attemptId: job.attemptId, householdId: job.householdId };
}

function identityMatches(job: WorkerJob, result: WorkerResult): boolean {
  return (
    job.jobId === result.jobId &&
    job.attemptId === result.attemptId &&
    job.householdId === result.householdId &&
    job.baseHouseholdVersion === result.baseHouseholdVersion &&
    job.policyVersion === result.policyVersion &&
    job.modelRouteId === result.modelRouteId &&
    job.modelCapabilityProfile === result.modelCapabilityProfile &&
    job.outputContractRef === result.outputContractRef
  );
}
