import { z } from "zod";
import { ModelGatewayError } from "../models/errors.js";

export const WorkerRuntimeErrorCodeSchema = z.enum([
  "invalid_job",
  "invalid_output",
  "cancelled",
  "deadline_exceeded",
  "budget_exceeded",
  "tool_failed",
  "model_failed",
  "runtime_failed",
  "cleanup_failed",
]);

export type WorkerRuntimeErrorCode = z.infer<typeof WorkerRuntimeErrorCodeSchema>;

const ERROR_MESSAGES: Readonly<Record<WorkerRuntimeErrorCode, string>> = Object.freeze({
  invalid_job: "The worker job failed application validation.",
  invalid_output: "The worker returned output that failed application validation.",
  cancelled: "The worker attempt was cancelled.",
  deadline_exceeded: "The worker attempt exceeded its deadline.",
  budget_exceeded: "The worker attempt exceeded its resource budget.",
  tool_failed: "An allowlisted worker tool failed.",
  model_failed: "The worker model request failed.",
  runtime_failed: "The worker runtime failed.",
  cleanup_failed: "The worker attempt could not release all attempt-scoped resources.",
});

export class WorkerRuntimeError extends Error {
  readonly code: WorkerRuntimeErrorCode;
  readonly retryable: boolean;

  constructor(code: WorkerRuntimeErrorCode, options?: { readonly retryable?: boolean }) {
    super(ERROR_MESSAGES[code]);
    this.name = "WorkerRuntimeError";
    this.code = code;
    this.retryable = options?.retryable ?? (code === "model_failed" || code === "runtime_failed");
  }
}

export function asWorkerRuntimeError(
  error: unknown,
  state?: { readonly deadlineExceeded?: boolean; readonly cancelled?: boolean },
): WorkerRuntimeError {
  if (error instanceof WorkerRuntimeError) {
    return error;
  }
  if (state?.deadlineExceeded === true) {
    return new WorkerRuntimeError("deadline_exceeded");
  }
  if (state?.cancelled === true || (error instanceof Error && error.name === "AbortError")) {
    return new WorkerRuntimeError("cancelled");
  }
  if (error instanceof ModelGatewayError) {
    if (error.code === "cancelled") {
      return new WorkerRuntimeError("cancelled");
    }
    if (error.code === "invalid_output") {
      return new WorkerRuntimeError("invalid_output");
    }
    return new WorkerRuntimeError("model_failed", { retryable: error.retryable });
  }
  return new WorkerRuntimeError("runtime_failed");
}
