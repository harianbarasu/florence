import type { z } from "zod";

export type ModelProfile = "fast_private_triage" | "careful_coordination" | "general_answer";

export interface AuthorizedImage {
  readonly mimeType: string;
  readonly dataBase64: string;
  readonly sha256: string;
}

export interface StructuredModelRequest<Schema extends z.ZodType> {
  profile: ModelProfile;
  system: string;
  user: string;
  schema: Schema;
  schemaName: string;
  timeoutMs: number;
  maxOutputTokens: number;
  images?: readonly AuthorizedImage[];
}

export interface ModelGateway {
  completeStructured<Schema extends z.ZodType>(
    request: StructuredModelRequest<Schema>,
  ): Promise<z.output<Schema>>;
}

export interface PinnedSkill<Schema extends z.ZodType = z.ZodType> {
  id: string;
  version: number;
  purpose: string;
  instructions: string;
  outputSchema: Schema;
  outputSchemaName: string;
  riskClass: "low" | "medium" | "high";
  requestedCapabilities: readonly string[];
  evaluationRelease: string;
}

export interface WorkerJob<Schema extends z.ZodType = z.ZodType> {
  attemptId: string;
  taskVersionId: string;
  authority: {
    readonly person: { readonly id: string; readonly controlEpoch: number };
    readonly household?: { readonly id: string; readonly controlEpoch: number };
    readonly conversation?: { readonly id: string; readonly authorityVersion: number };
  };
  skill: PinnedSkill<Schema>;
  authorizedContext: string;
  images?: readonly AuthorizedImage[];
  goal: string;
  deadline: Date;
  budget: {
    maxModelCalls: number;
    maxOutputTokens: number;
  };
}

export interface WorkerResult<Output = unknown> {
  attemptId: string;
  taskVersionId: string;
  skillId: string;
  skillVersion: number;
  evaluationRelease: string;
  runtimeRoute: string;
  status: "proposed" | "expired" | "failed";
  proposal?: Output;
  errorCode?: string;
  startedAt: Date;
  completedAt: Date;
}

export interface WorkerRuntime {
  run<Schema extends z.ZodType>(job: WorkerJob<Schema>): Promise<WorkerResult<z.output<Schema>>>;
  reconcile(
    attemptId: string,
    status: "accepted" | "partially_accepted" | "rejected" | "stale",
  ): Promise<void>;
}
