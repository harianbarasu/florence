import { z } from "zod";

export const ModelCapabilityProfileSchema = z.enum([
  "classification_extraction",
  "tool_planning",
  "vision_document",
  "long_context_research",
  "private_processing",
]);

export type ModelCapabilityProfile = z.infer<typeof ModelCapabilityProfileSchema>;

export const ModelProviderKindSchema = z.enum(["openai", "anthropic", "openai-compatible"]);

export type ModelProviderKind = z.infer<typeof ModelProviderKindSchema>;

export const ModelRouteCapabilitiesSchema = z
  .object({
    structuredOutput: z.boolean(),
    toolCalling: z.boolean(),
    vision: z.boolean(),
    documentUnderstanding: z.boolean(),
    longContext: z.boolean(),
    privateProcessing: z.boolean(),
  })
  .strict();

export type ModelRouteCapabilities = z.infer<typeof ModelRouteCapabilitiesSchema>;

export const MODEL_CAPABILITY_REQUIREMENTS: Readonly<
  Record<ModelCapabilityProfile, Partial<ModelRouteCapabilities>>
> = Object.freeze({
  classification_extraction: Object.freeze({ structuredOutput: true }),
  tool_planning: Object.freeze({ structuredOutput: true, toolCalling: true }),
  vision_document: Object.freeze({
    structuredOutput: true,
    vision: true,
    documentUnderstanding: true,
  }),
  long_context_research: Object.freeze({
    structuredOutput: true,
    toolCalling: true,
    longContext: true,
  }),
  private_processing: Object.freeze({ privateProcessing: true }),
});

export const JsonValueSchema = z.json();
export type JsonValue = z.infer<typeof JsonValueSchema>;

const TextPartSchema = z.object({ type: z.literal("text"), text: z.string() }).strict();

const ImagePartSchema = z
  .object({
    type: z.literal("image"),
    mediaType: z.string().min(1),
    data: z.string().min(1).optional(),
    uri: z.string().min(1).optional(),
    alt: z.string().optional(),
  })
  .strict()
  .refine((part) => (part.data === undefined) !== (part.uri === undefined), {
    message: "An image must contain exactly one of data or uri.",
  });

const FilePartSchema = z
  .object({
    type: z.literal("file"),
    mediaType: z.string().min(1),
    data: z.string().min(1).optional(),
    uri: z.string().min(1).optional(),
    filename: z.string().min(1).optional(),
  })
  .strict()
  .refine((part) => (part.data === undefined) !== (part.uri === undefined), {
    message: "A file must contain exactly one of data or uri.",
  });

export const ModelToolRequestSchema = z
  .object({
    type: z.literal("tool_request"),
    requestId: z.string().min(1),
    name: z.string().min(1),
    arguments: z.record(z.string(), JsonValueSchema),
  })
  .strict();

export type ModelToolRequest = z.infer<typeof ModelToolRequestSchema>;

export const ModelToolResultPartSchema = z
  .object({
    type: z.literal("tool_result"),
    requestId: z.string().min(1),
    name: z.string().min(1),
    output: JsonValueSchema,
    isError: z.boolean().optional(),
  })
  .strict();

export const ModelMessagePartSchema = z.union([
  TextPartSchema,
  ImagePartSchema,
  FilePartSchema,
  ModelToolRequestSchema,
  ModelToolResultPartSchema,
]);

export type ModelMessagePart = z.infer<typeof ModelMessagePartSchema>;

export const ModelMessageSchema = z
  .object({
    role: z.enum(["system", "user", "assistant", "tool"]),
    parts: z.array(ModelMessagePartSchema).min(1),
  })
  .strict();

export type ModelMessage = z.infer<typeof ModelMessageSchema>;

export const ModelToolDefinitionSchema = z
  .object({
    name: z.string().min(1),
    description: z.string().min(1),
    inputSchema: z.record(z.string(), JsonValueSchema),
  })
  .strict();

export type ModelToolDefinition = z.infer<typeof ModelToolDefinitionSchema>;

export type ModelToolChoice = "auto" | "none" | "required" | { readonly name: string };

/**
 * Product-owned input to every model call. The optional Zod schema is used as
 * an additional application validator; native provider strict mode is never
 * treated as sufficient validation.
 */
export interface ModelCompletionRequest {
  readonly messages: readonly ModelMessage[];
  readonly tools?: readonly ModelToolDefinition[];
  readonly toolChoice?: ModelToolChoice;
  readonly responseSchema?: z.ZodType<unknown>;
  readonly responseSchemaName?: string;
  readonly maxOutputTokens?: number;
  readonly temperature?: number;
}

export interface ModelCallOptions {
  readonly signal?: AbortSignal;
}

const CitationPartSchema = z
  .object({
    type: z.literal("citation"),
    uri: z.string().min(1),
    title: z.string().optional(),
    excerpt: z.string().optional(),
  })
  .strict();

const StructuredResultPartSchema = z
  .object({ type: z.literal("structured_result"), value: JsonValueSchema })
  .strict();

export const ModelOutputPartSchema = z.union([
  TextPartSchema,
  CitationPartSchema,
  ModelToolRequestSchema,
  StructuredResultPartSchema,
]);

export type ModelOutputPart = z.infer<typeof ModelOutputPartSchema>;

export const ModelUsageSchema = z
  .object({
    inputTokens: z.number().int().nonnegative().optional(),
    outputTokens: z.number().int().nonnegative().optional(),
    totalTokens: z.number().int().nonnegative().optional(),
  })
  .strict();

export type ModelUsage = z.infer<typeof ModelUsageSchema>;

export const ModelRouteReferenceSchema = z
  .object({
    routeId: z.string().min(1),
    provider: ModelProviderKindSchema,
    model: z.string().min(1),
    version: z.string().min(1).optional(),
  })
  .strict();

export type ModelRouteReference = z.infer<typeof ModelRouteReferenceSchema>;

export const ModelCompletionResultSchema = z
  .object({
    content: z.array(ModelOutputPartSchema),
    finishReason: z.enum(["stop", "tool_request", "length", "content_filter", "unknown"]),
    usage: ModelUsageSchema,
    latencyMs: z.number().nonnegative(),
    route: ModelRouteReferenceSchema,
  })
  .strict();

export type ModelCompletionResult = z.infer<typeof ModelCompletionResultSchema>;

export interface ModelGateway {
  complete(
    profile: ModelCapabilityProfile,
    request: ModelCompletionRequest,
    options?: ModelCallOptions,
  ): Promise<ModelCompletionResult>;
}

/** Internal production/test adapter seam; it still contains only app-owned types. */
export interface ModelProviderAdapter {
  readonly route: ModelRouteReference;
  readonly capabilities: ModelRouteCapabilities;
  complete(request: ModelCompletionRequest, options?: ModelCallOptions): Promise<ModelCompletionResult>;
}
