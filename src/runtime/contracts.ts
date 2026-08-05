import { z } from "zod";
import {
  type JsonValue,
  JsonValueSchema,
  ModelCapabilityProfileSchema,
  ModelRouteReferenceSchema,
  ModelUsageSchema,
} from "../models/contracts.js";

export const WorkerVisibilitySchema = z.enum(["personal", "household"]);
export type WorkerVisibility = z.infer<typeof WorkerVisibilitySchema>;

export const WorkerScopeGrantSchema = z
  .object({
    grantId: z.string().min(1),
    visibility: WorkerVisibilitySchema,
    adultId: z.string().min(1).optional(),
    purpose: z.string().min(1),
    expiresAt: z.iso.datetime({ offset: true }),
  })
  .strict()
  .superRefine((grant, context) => {
    if (grant.visibility === "personal" && grant.adultId === undefined) {
      context.addIssue({
        code: "custom",
        message: "A personal scope grant must identify its adult.",
        path: ["adultId"],
      });
    }
    if (grant.visibility === "household" && grant.adultId !== undefined) {
      context.addIssue({
        code: "custom",
        message: "A household scope grant cannot identify a single adult.",
        path: ["adultId"],
      });
    }
  });

export type WorkerScopeGrant = z.infer<typeof WorkerScopeGrantSchema>;

export const WorkerBudgetSchema = z
  .object({
    maxDurationMs: z.number().int().positive().max(86_400_000),
    maxModelCalls: z.number().int().positive().max(1_000),
    maxToolCalls: z.number().int().nonnegative().max(10_000),
    maxInputTokens: z.number().int().positive().max(1_000_000_000).optional(),
    maxOutputTokens: z.number().int().positive().max(1_000_000_000).optional(),
    maxTotalTokens: z.number().int().positive().max(1_000_000_000).optional(),
  })
  .strict();

export type WorkerBudget = z.infer<typeof WorkerBudgetSchema>;

export const WorkerJobSchema = z
  .object({
    jobId: z.string().min(1),
    attemptId: z.string().min(1),
    householdId: z.string().min(1),
    baseHouseholdVersion: z.number().int().nonnegative(),
    policyVersion: z.number().int().nonnegative(),
    objective: z.string().min(1).max(20_000),
    scopeGrant: WorkerScopeGrantSchema,
    evidenceRefs: z.array(z.string().min(1)).max(100),
    capabilityIds: z.array(z.string().min(1)).max(100),
    modelRouteId: z.string().min(1),
    modelCapabilityProfile: ModelCapabilityProfileSchema,
    budget: WorkerBudgetSchema,
    deadline: z.iso.datetime({ offset: true }),
    outputContractRef: z.string().min(1),
    allowedToolNames: z.array(z.string().regex(/^[A-Za-z0-9_-]{1,64}$/)).max(100),
  })
  .strict()
  .superRefine((job, context) => {
    if (new Set(job.evidenceRefs).size !== job.evidenceRefs.length) {
      context.addIssue({ code: "custom", message: "Evidence references must be unique." });
    }
    if (new Set(job.capabilityIds).size !== job.capabilityIds.length) {
      context.addIssue({ code: "custom", message: "Capability identifiers must be unique." });
    }
    if (new Set(job.allowedToolNames).size !== job.allowedToolNames.length) {
      context.addIssue({ code: "custom", message: "Allowed tool names must be unique." });
    }
  });

export type WorkerJob = z.infer<typeof WorkerJobSchema>;

export const WorkerContextItemSchema = z
  .object({
    reference: z.string().min(1),
    visibility: WorkerVisibilitySchema,
    adultId: z.string().min(1).optional(),
    content: z.string().max(1_000_000),
    mediaType: z.string().min(1).optional(),
  })
  .strict()
  .superRefine((item, context) => {
    if (item.visibility === "personal" && item.adultId === undefined) {
      context.addIssue({
        code: "custom",
        message: "Personal worker context must identify its adult.",
        path: ["adultId"],
      });
    }
    if (item.visibility === "household" && item.adultId !== undefined) {
      context.addIssue({
        code: "custom",
        message: "Household worker context cannot identify a single adult.",
        path: ["adultId"],
      });
    }
  });

export type WorkerContextItem = z.infer<typeof WorkerContextItemSchema>;

export interface WorkerToolExecutionContext {
  readonly jobId: string;
  readonly attemptId: string;
  readonly householdId: string;
  readonly capabilityIds: readonly string[];
  readonly signal: AbortSignal;
}

export interface WorkerToolCleanupContext {
  readonly jobId: string;
  readonly attemptId: string;
  readonly householdId: string;
}

export interface WorkerTool {
  readonly name: string;
  readonly description: string;
  readonly inputSchema: z.ZodObject;
  readonly requiredCapabilityIds?: readonly string[];
  execute(input: unknown, context: WorkerToolExecutionContext): Promise<JsonValue>;
  cleanup?(context: WorkerToolCleanupContext): Promise<void>;
}

export interface WorkerAttemptOptions {
  readonly context?: readonly WorkerContextItem[];
  readonly tools?: readonly WorkerTool[];
  readonly signal?: AbortSignal;
  readonly validateBeforeAccept?: () => Promise<boolean>;
  readonly cleanup?: (context: WorkerToolCleanupContext) => Promise<void>;
}

export const ProposedWorkerCommandSchema = z
  .object({
    kind: z.string().min(1).max(100),
    payload: JsonValueSchema,
  })
  .strict();

export type ProposedWorkerCommand = z.infer<typeof ProposedWorkerCommandSchema>;

/** The only model-authored result shape. It is always parsed again by the app. */
export const WorkerResultPayloadSchema = z
  .object({
    summary: z.string().trim().min(1).max(20_000),
    evidenceRefs: z.array(z.string().min(1)).max(100),
    questions: z.array(z.string().trim().min(1).max(2_000)).max(50),
    warnings: z.array(z.string().min(1).max(2_000)).max(50),
    proposedCommands: z.array(ProposedWorkerCommandSchema).max(50),
    confidence: z.number().min(0).max(1),
  })
  .strict()
  .superRefine((result, context) => {
    if (new Set(result.evidenceRefs).size !== result.evidenceRefs.length) {
      context.addIssue({ code: "custom", message: "Evidence references must be unique." });
    }
  });

export type WorkerResultPayload = z.infer<typeof WorkerResultPayloadSchema>;

export const WorkerDiagnosticsSchema = z
  .object({
    durationMs: z.number().nonnegative(),
    modelCalls: z.number().int().nonnegative(),
    toolCalls: z.number().int().nonnegative(),
    usage: ModelUsageSchema,
    modelRoute: ModelRouteReferenceSchema.optional(),
    traceReferences: z.array(z.string().min(1)),
  })
  .strict();

export type WorkerDiagnostics = z.infer<typeof WorkerDiagnosticsSchema>;

export const WorkerResultSchema = WorkerResultPayloadSchema.extend({
  jobId: z.string().min(1),
  attemptId: z.string().min(1),
  householdId: z.string().min(1),
  baseHouseholdVersion: z.number().int().nonnegative(),
  policyVersion: z.number().int().nonnegative(),
  modelRouteId: z.string().min(1),
  modelCapabilityProfile: ModelCapabilityProfileSchema,
  outputContractRef: z.string().min(1),
  diagnostics: WorkerDiagnosticsSchema,
}).strict();

export type WorkerResult = z.infer<typeof WorkerResultSchema>;

export interface WorkerRuntime {
  run(job: WorkerJob, options?: WorkerAttemptOptions): Promise<WorkerResult>;
}
