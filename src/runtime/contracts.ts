import { z } from "zod";
import {
  type JsonValue,
  JsonValueSchema,
  ModelCapabilityProfileSchema,
  ModelRouteReferenceSchema,
  ModelUsageSchema,
} from "../models/contracts.js";
import { payloadDigest } from "../security/canonical-json.js";

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

export const WorkerCapabilityScopeSchema = z
  .object({
    visibility: WorkerVisibilitySchema,
    adultId: z.string().min(1).optional(),
  })
  .strict()
  .superRefine((scope, context) => {
    if (scope.visibility === "personal" && scope.adultId === undefined) {
      context.addIssue({
        code: "custom",
        message: "A personal capability scope must identify its adult.",
        path: ["adultId"],
      });
    }
    if (scope.visibility === "household" && scope.adultId !== undefined) {
      context.addIssue({
        code: "custom",
        message: "A household capability scope cannot identify a single adult.",
        path: ["adultId"],
      });
    }
  });

export type WorkerCapabilityScope = z.infer<typeof WorkerCapabilityScopeSchema>;

export const WorkerCapabilityGrantSchema = z
  .object({
    grantId: z.string().min(1).max(256),
    capability: z.string().min(1).max(256),
    householdId: z.string().min(1),
    jobId: z.string().min(1),
    attemptId: z.string().min(1),
    scopeGrantId: z.string().min(1),
    scope: WorkerCapabilityScopeSchema,
    purpose: z.string().min(1).max(256),
    issuedAt: z.iso.datetime({ offset: true }),
    expiresAt: z.iso.datetime({ offset: true }),
    revokedAt: z.iso.datetime({ offset: true }).optional(),
  })
  .strict()
  .superRefine((grant, context) => {
    const lifetimeMs = Date.parse(grant.expiresAt) - Date.parse(grant.issuedAt);
    if (lifetimeMs <= 0) {
      context.addIssue({ code: "custom", message: "Capability grant expiry must follow issuance." });
    }
    if (lifetimeMs > 20 * 60_000) {
      context.addIssue({ code: "custom", message: "Capability grants may live for at most 20 minutes." });
    }
    if (grant.revokedAt !== undefined && Date.parse(grant.revokedAt) < Date.parse(grant.issuedAt)) {
      context.addIssue({ code: "custom", message: "Capability grant revocation cannot predate issuance." });
    }
  });

export type WorkerCapabilityGrant = z.infer<typeof WorkerCapabilityGrantSchema>;

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
    capabilityGrants: z.array(WorkerCapabilityGrantSchema).max(100),
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
    const grantIds = job.capabilityGrants.map((grant) => grant.grantId);
    const capabilities = job.capabilityGrants.map((grant) => grant.capability);
    if (new Set(grantIds).size !== grantIds.length || new Set(capabilities).size !== capabilities.length) {
      context.addIssue({ code: "custom", message: "Capability grants and capabilities must be unique." });
    }
    if (new Set(job.allowedToolNames).size !== job.allowedToolNames.length) {
      context.addIssue({ code: "custom", message: "Allowed tool names must be unique." });
    }
    for (const grant of job.capabilityGrants) {
      const expectedAdultId = job.scopeGrant.visibility === "personal" ? job.scopeGrant.adultId : undefined;
      if (
        grant.householdId !== job.householdId ||
        grant.jobId !== job.jobId ||
        grant.attemptId !== job.attemptId ||
        grant.scopeGrantId !== job.scopeGrant.grantId ||
        grant.scope.visibility !== job.scopeGrant.visibility ||
        grant.scope.adultId !== expectedAdultId ||
        grant.purpose !== job.scopeGrant.purpose ||
        Date.parse(grant.expiresAt) > Date.parse(job.deadline) ||
        Date.parse(grant.expiresAt) > Date.parse(job.scopeGrant.expiresAt)
      ) {
        context.addIssue({
          code: "custom",
          message: "Capability grants must be bound to this exact job attempt, scope, purpose, and lifetime.",
          path: ["capabilityGrants"],
        });
        break;
      }
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
  readonly signal: AbortSignal;
  authorizeCapability(capability: string): Promise<void>;
}

export interface WorkerCapabilityAuthorizationRequest {
  readonly grantId: string;
  readonly capability: string;
  readonly householdId: string;
  readonly jobId: string;
  readonly attemptId: string;
  readonly scopeGrantId: string;
  readonly scope: WorkerCapabilityScope;
  readonly purpose: string;
}

export interface WorkerCapabilityAuthorizer {
  authorize(request: WorkerCapabilityAuthorizationRequest): Promise<void>;
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
  readonly capabilityAuthorizer?: WorkerCapabilityAuthorizer;
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

const Sha256DigestSchema = z.string().regex(/^sha256:[a-f0-9]{64}$/u);
const ReceiptIdSchema = Sha256DigestSchema;
const BoundedResultTextSchema = z.string().trim().min(1).max(2_000);

const ReceiptReferenceListSchema = z
  .array(ReceiptIdSchema)
  .min(1)
  .max(20)
  .superRefine((references, context) => {
    if (new Set(references).size !== references.length) {
      context.addIssue({ code: "custom", message: "Receipt references must be unique." });
    }
  });

const WorkerToolReceiptBaseClaimsShape = {
  jobId: z.string().min(1),
  attemptId: z.string().min(1),
  callIndex: z.number().int().nonnegative(),
  issuedAt: z.iso.datetime({ offset: true }),
  outputDigest: Sha256DigestSchema,
};

const ResearchSourceReceiptClaimsShape = {
  ...WorkerToolReceiptBaseClaimsShape,
  kind: z.literal("research_sources"),
  sources: z
    .array(
      z
        .object({
          url: z.url(),
          contentDigest: Sha256DigestSchema,
        })
        .strict(),
    )
    .max(100),
};

const HouseholdScheduleReceiptClaimsShape = {
  ...WorkerToolReceiptBaseClaimsShape,
  kind: z.literal("household_schedule"),
  timeZone: z.string().trim().min(1).max(100),
  coverageMode: z.enum(["personal_owner", "household_all_adults"]),
  coverageFrom: z.iso.datetime({ offset: true }),
  coverageTo: z.iso.datetime({ offset: true }),
  coverageComplete: z.boolean(),
};

function validateHouseholdScheduleCoverage(
  receipt: {
    readonly coverageFrom: string;
    readonly coverageTo: string;
  },
  context: z.RefinementCtx,
): void {
  if (Date.parse(receipt.coverageFrom) >= Date.parse(receipt.coverageTo)) {
    context.addIssue({
      code: "custom",
      message: "Schedule coverage must have a positive horizon.",
      path: ["coverageTo"],
    });
  }
}

export const ResearchSourceReceiptClaimsSchema = z
  .object(ResearchSourceReceiptClaimsShape)
  .strict()
  .superRefine((receipt, context) => {
    const urls = receipt.sources.map((source) => source.url);
    if (new Set(urls).size !== urls.length) {
      context.addIssue({ code: "custom", message: "Receipt source URLs must be unique." });
    }
  });

export const HouseholdScheduleReceiptClaimsSchema = z
  .object(HouseholdScheduleReceiptClaimsShape)
  .strict()
  .superRefine(validateHouseholdScheduleCoverage);

export const WorkerToolReceiptClaimsSchema = z.union([
  ResearchSourceReceiptClaimsSchema,
  HouseholdScheduleReceiptClaimsSchema,
]);

export type WorkerToolReceiptClaims = z.infer<typeof WorkerToolReceiptClaimsSchema>;

export function workerToolReceiptId(rawClaims: WorkerToolReceiptClaims): string {
  const claims = WorkerToolReceiptClaimsSchema.parse(rawClaims);
  return `sha256:${payloadDigest(claims)}`;
}

const ResearchSourceReceiptSchema = z
  .object({
    ...ResearchSourceReceiptClaimsShape,
    receiptId: ReceiptIdSchema,
  })
  .strict();

const HouseholdScheduleReceiptSchema = z
  .object({
    ...HouseholdScheduleReceiptClaimsShape,
    receiptId: ReceiptIdSchema,
  })
  .strict()
  .superRefine(validateHouseholdScheduleCoverage);

export const WorkerToolReceiptSchema = z
  .union([ResearchSourceReceiptSchema, HouseholdScheduleReceiptSchema])
  .superRefine((receipt, context) => {
    const { receiptId, ...claims } = receipt;
    if (receiptId !== workerToolReceiptId(claims)) {
      context.addIssue({ code: "custom", message: "Tool receipt identity does not match its claims." });
    }
  });

export type WorkerToolReceipt = z.infer<typeof WorkerToolReceiptSchema>;

const ResearchCitationSchema = z
  .object({
    statement: BoundedResultTextSchema,
    sourceReceiptIds: ReceiptReferenceListSchema,
  })
  .strict();

export const ResearchArtifactSchema = z
  .object({
    asOf: z.iso.datetime({ offset: true }),
    question: BoundedResultTextSchema,
    comparison: z
      .array(
        z
          .object({
            option: z.string().trim().min(1).max(240),
            assessment: BoundedResultTextSchema,
            sourceReceiptIds: ReceiptReferenceListSchema,
          })
          .strict(),
      )
      .max(20),
    findings: z.array(ResearchCitationSchema).min(1).max(50),
    recommendation: ResearchCitationSchema,
    uncertainties: z.array(BoundedResultTextSchema).max(20),
  })
  .strict()
  .superRefine((artifact, context) => {
    const options = artifact.comparison.map((item) => item.option.toLocaleLowerCase());
    if (new Set(options).size !== options.length) {
      context.addIssue({ code: "custom", message: "Comparison options must be unique." });
    }
  });

export type ResearchArtifact = z.infer<typeof ResearchArtifactSchema>;

const ProjectPhaseSchema = z
  .object({
    name: z.string().trim().min(1).max(240),
    outcome: BoundedResultTextSchema,
    actions: z.array(BoundedResultTextSchema).min(1).max(50),
  })
  .strict();

const ProjectDecisionSchema = z
  .object({
    decision: BoundedResultTextSchema,
    recommendation: BoundedResultTextSchema,
    rationale: BoundedResultTextSchema,
  })
  .strict();

const ProjectRiskSchema = z
  .object({
    risk: BoundedResultTextSchema,
    mitigation: BoundedResultTextSchema,
  })
  .strict();

/** Durable, provider-neutral work product for one general family-project attempt. */
export const ProjectArtifactSchema = z
  .object({
    asOf: z.iso.datetime({ offset: true }),
    plan: BoundedResultTextSchema,
    phases: z.array(ProjectPhaseSchema).min(1).max(20),
    nextActions: z.array(BoundedResultTextSchema).min(1).max(50),
    decisions: z.array(ProjectDecisionSchema).max(20),
    risks: z.array(ProjectRiskSchema).max(20),
    assumptions: z.array(BoundedResultTextSchema).max(20),
    citationReceiptIds: ReceiptReferenceListSchema.optional(),
  })
  .strict();

export type ProjectArtifact = z.infer<typeof ProjectArtifactSchema>;

const MealPlanItemSchema = z
  .object({
    when: z.string().trim().min(1).max(240),
    meal: z.string().trim().min(1).max(500),
    scheduleRationale: BoundedResultTextSchema,
    groceryItems: z.array(z.string().trim().min(1).max(240)).min(1).max(100),
  })
  .strict();

export const MealPlanArtifactSchema = z
  .object({
    asOf: z.iso.datetime({ offset: true }),
    horizon: z.string().trim().min(1).max(500),
    horizonFrom: z.iso.datetime({ offset: true }),
    horizonTo: z.iso.datetime({ offset: true }),
    scheduleReceiptId: ReceiptIdSchema,
    meals: z.array(MealPlanItemSchema).min(1).max(31),
    substitutions: z
      .array(
        z
          .object({
            insteadOf: z.string().trim().min(1).max(500),
            use: z.string().trim().min(1).max(500),
            reason: BoundedResultTextSchema,
          })
          .strict(),
      )
      .max(50),
    groceryGroups: z
      .array(
        z
          .object({
            group: z.string().trim().min(1).max(100),
            items: z.array(z.string().trim().min(1).max(240)).min(1).max(100),
          })
          .strict(),
      )
      .min(1)
      .max(30),
    assumptions: z.array(BoundedResultTextSchema).max(20),
    uncertainties: z.array(BoundedResultTextSchema).max(20),
  })
  .strict()
  .superRefine((artifact, context) => {
    if (Date.parse(artifact.horizonFrom) >= Date.parse(artifact.horizonTo)) {
      context.addIssue({
        code: "custom",
        path: ["horizonTo"],
        message: "The meal-plan horizon must be positive.",
      });
    }
    const mealSlots = artifact.meals.map((meal) => meal.when.toLocaleLowerCase());
    if (new Set(mealSlots).size !== mealSlots.length) {
      context.addIssue({ code: "custom", message: "Meal-plan slots must be unique." });
    }
  });

export type MealPlanArtifact = z.infer<typeof MealPlanArtifactSchema>;

const NeedsInputCompletionSchema = z
  .object({
    status: z.literal("needs_input"),
    questions: z.array(BoundedResultTextSchema).min(1).max(50),
  })
  .strict();

const ResearchCompletionSchema = z.discriminatedUnion("status", [
  NeedsInputCompletionSchema,
  z
    .object({
      status: z.literal("complete"),
      artifact: ResearchArtifactSchema,
    })
    .strict(),
]);

const MealPlanCompletionSchema = z.discriminatedUnion("status", [
  NeedsInputCompletionSchema,
  z
    .object({
      status: z.literal("complete"),
      artifact: MealPlanArtifactSchema,
    })
    .strict(),
]);

const ProjectCompletionSchema = z.discriminatedUnion("status", [
  NeedsInputCompletionSchema,
  z
    .object({
      status: z.literal("complete"),
      artifact: ProjectArtifactSchema,
    })
    .strict(),
]);

const WorkerResultPayloadBaseShape = {
  summary: z.string().trim().min(1).max(20_000),
  warnings: z.array(z.string().trim().min(1).max(2_000)).max(50),
  proposedCommands: z.array(ProposedWorkerCommandSchema).max(50),
  confidence: z.number().min(0).max(1),
};

export const ResearchWorkerResultPayloadSchema = z
  .object({
    ...WorkerResultPayloadBaseShape,
    purpose: z.literal("family_research"),
    completion: ResearchCompletionSchema,
  })
  .strict();

export const MealPlanWorkerResultPayloadSchema = z
  .object({
    ...WorkerResultPayloadBaseShape,
    purpose: z.literal("meal_plan"),
    completion: MealPlanCompletionSchema,
  })
  .strict();

export const ProjectWorkerResultPayloadSchema = z
  .object({
    ...WorkerResultPayloadBaseShape,
    purpose: z.literal("family_project"),
    completion: ProjectCompletionSchema,
  })
  .strict();

/** The only model-authored result shapes. Runtime-issued receipts are deliberately absent. */
export const WorkerResultPayloadSchema = z.discriminatedUnion("purpose", [
  ResearchWorkerResultPayloadSchema,
  MealPlanWorkerResultPayloadSchema,
  ProjectWorkerResultPayloadSchema,
]);

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

const WorkerResultIdentityShape = {
  jobId: z.string().min(1),
  attemptId: z.string().min(1),
  householdId: z.string().min(1),
  baseHouseholdVersion: z.number().int().nonnegative(),
  policyVersion: z.number().int().nonnegative(),
  modelRouteId: z.string().min(1),
  modelCapabilityProfile: ModelCapabilityProfileSchema,
  outputContractRef: z.string().min(1),
  diagnostics: WorkerDiagnosticsSchema,
  toolReceipts: z.array(WorkerToolReceiptSchema).max(100).default([]),
};

const ResearchWorkerResultSchema =
  ResearchWorkerResultPayloadSchema.extend(WorkerResultIdentityShape).strict();

const MealPlanWorkerResultSchema =
  MealPlanWorkerResultPayloadSchema.extend(WorkerResultIdentityShape).strict();

const ProjectWorkerResultSchema = ProjectWorkerResultPayloadSchema.extend(WorkerResultIdentityShape).strict();

export const WorkerResultSchema = z.discriminatedUnion("purpose", [
  ResearchWorkerResultSchema,
  MealPlanWorkerResultSchema,
  ProjectWorkerResultSchema,
]);

export type WorkerResult = z.infer<typeof WorkerResultSchema>;

export type WorkerResultVerification =
  | { readonly status: "needs_input" }
  | { readonly status: "verified_complete"; readonly proofReceiptIds: readonly string[] }
  | { readonly status: "invalid"; readonly reason: string };

function invalidVerification(reason: string): WorkerResultVerification {
  return { status: "invalid", reason };
}

function citedResearchReceiptIds(artifact: ResearchArtifact): string[] {
  return [
    ...artifact.comparison.flatMap((item) => item.sourceReceiptIds),
    ...artifact.findings.flatMap((item) => item.sourceReceiptIds),
    ...artifact.recommendation.sourceReceiptIds,
  ];
}

/** Deterministic app-owned completeness check used by both runtime and coordinator. */
export function verifyWorkerResultCompletion(
  result: WorkerResult,
  acceptedAt?: string,
): WorkerResultVerification {
  const receipts = new Map<string, WorkerToolReceipt>();
  const callIndexes = new Set<number>();
  for (const receipt of result.toolReceipts) {
    if (
      receipt.jobId !== result.jobId ||
      receipt.attemptId !== result.attemptId ||
      receipts.has(receipt.receiptId) ||
      callIndexes.has(receipt.callIndex)
    ) {
      return invalidVerification("receipt_not_bound_to_attempt");
    }
    if (acceptedAt !== undefined && Date.parse(receipt.issuedAt) > Date.parse(acceptedAt)) {
      return invalidVerification("receipt_observed_after_acceptance");
    }
    receipts.set(receipt.receiptId, receipt);
    callIndexes.add(receipt.callIndex);
  }

  if (result.completion.status === "needs_input") {
    return { status: "needs_input" };
  }

  if (acceptedAt !== undefined && Date.parse(result.completion.artifact.asOf) > Date.parse(acceptedAt)) {
    return invalidVerification("artifact_as_of_after_acceptance");
  }

  if (result.purpose === "family_research") {
    const artifact = result.completion.artifact;
    const citedIds = [...new Set(citedResearchReceiptIds(artifact))];
    if (citedIds.length === 0 || citedIds.length > 20) {
      return invalidVerification("research_receipt_count_invalid");
    }
    const citedReceipts = citedIds.map((receiptId) => receipts.get(receiptId));
    if (
      citedReceipts.some(
        (receipt): receipt is undefined => receipt === undefined || receipt.kind !== "research_sources",
      )
    ) {
      return invalidVerification("research_receipt_missing");
    }
    const researchReceipts = citedReceipts as Array<Extract<WorkerToolReceipt, { kind: "research_sources" }>>;
    const sourceUrls = new Set(
      researchReceipts.flatMap((receipt) => receipt.sources.map((source) => source.url)),
    );
    if (sourceUrls.size < 2) {
      return invalidVerification("research_requires_multiple_sources");
    }
    if (researchReceipts.some((receipt) => Date.parse(receipt.issuedAt) > Date.parse(artifact.asOf))) {
      return invalidVerification("research_as_of_precedes_sources");
    }
    return { status: "verified_complete", proofReceiptIds: citedIds };
  }

  if (result.purpose === "family_project") {
    const artifact = result.completion.artifact;
    const citedIds = artifact.citationReceiptIds ?? [];
    if (citedIds.length === 0 && result.toolReceipts.some((receipt) => receipt.kind === "research_sources")) {
      return invalidVerification("project_research_receipts_uncited");
    }
    const citedReceipts = citedIds.map((receiptId) => receipts.get(receiptId));
    if (
      citedReceipts.some(
        (receipt): receipt is undefined => receipt === undefined || receipt.kind !== "research_sources",
      )
    ) {
      return invalidVerification("project_research_receipt_missing");
    }
    const researchReceipts = citedReceipts as Array<Extract<WorkerToolReceipt, { kind: "research_sources" }>>;
    if (researchReceipts.some((receipt) => Date.parse(receipt.issuedAt) > Date.parse(artifact.asOf))) {
      return invalidVerification("project_as_of_precedes_sources");
    }
    return { status: "verified_complete", proofReceiptIds: citedIds };
  }

  const scheduleReceipt = receipts.get(result.completion.artifact.scheduleReceiptId);
  if (scheduleReceipt?.kind !== "household_schedule" || !scheduleReceipt.coverageComplete) {
    return invalidVerification("meal_schedule_receipt_missing_or_incomplete");
  }
  if (Date.parse(scheduleReceipt.issuedAt) > Date.parse(result.completion.artifact.asOf)) {
    return invalidVerification("meal_as_of_precedes_schedule");
  }
  if (
    Date.parse(result.completion.artifact.horizonFrom) < Date.parse(scheduleReceipt.coverageFrom) ||
    Date.parse(result.completion.artifact.horizonTo) > Date.parse(scheduleReceipt.coverageTo)
  ) {
    return invalidVerification("meal_horizon_outside_schedule_coverage");
  }
  const groceryItems = new Set(
    result.completion.artifact.groceryGroups.flatMap((group) =>
      group.items.map((item) => item.trim().toLocaleLowerCase()),
    ),
  );
  if (
    result.completion.artifact.meals.some((meal) =>
      meal.groceryItems.some((item) => !groceryItems.has(item.trim().toLocaleLowerCase())),
    )
  ) {
    return invalidVerification("meal_grocery_list_incomplete");
  }
  return {
    status: "verified_complete",
    proofReceiptIds: [scheduleReceipt.receiptId],
  };
}

export interface WorkerRuntime {
  run(job: WorkerJob, options?: WorkerAttemptOptions): Promise<WorkerResult>;
}
