import { workerResultSchema } from "@florence/contracts";
import OpenAI, {
  APIConnectionError,
  APIError,
  AuthenticationError,
  BadRequestError,
  ConflictError,
  InternalServerError,
  PermissionDeniedError,
  RateLimitError,
} from "openai";
import { ContentFilterFinishReasonError, LengthFinishReasonError } from "openai/core/error";
import { zodTextFormat } from "openai/helpers/zod";
import { ZodError } from "zod";
import { type ModelGateway, type ModelRequest, WorkerRuntimeError } from "./index.js";

export type OpenAIResponsesGatewayOptions = {
  apiKey: string;
  model: string;
  timeoutMs?: number;
  maxOutputTokens?: number;
};

export class OpenAIResponsesGateway implements ModelGateway {
  readonly #client: OpenAI;
  readonly #model: string;
  readonly #maxOutputTokens: number;

  constructor(options: OpenAIResponsesGatewayOptions, client?: OpenAI) {
    if (!options.apiKey.trim()) throw new WorkerRuntimeError("permanent", "OPENAI_API_KEY is required");
    if (!options.model.trim()) throw new WorkerRuntimeError("permanent", "OPENAI_MODEL is required");
    const timeoutMs = validLimit(options.timeoutMs ?? 30_000);
    this.#model = options.model;
    this.#maxOutputTokens = validLimit(options.maxOutputTokens ?? 4_000);
    this.#client =
      client ??
      new OpenAI({
        apiKey: options.apiKey,
        timeout: timeoutMs,
        maxRetries: 0,
      });
  }

  async generate(request: ModelRequest): Promise<unknown> {
    try {
      const response = await this.#client.responses.parse({
        model: this.#model,
        store: false,
        instructions: request.instructions,
        input: [
          {
            role: "user",
            content: request.content.map((part) =>
              part.type === "text"
                ? { type: "input_text" as const, text: part.text }
                : {
                    type: "input_image" as const,
                    detail: "auto" as const,
                    image_url: `data:${part.mimeType};base64,${Buffer.from(part.bytes).toString("base64")}`,
                  },
            ),
          },
        ],
        text: { format: zodTextFormat(workerResultSchema, "florence_worker_result") },
        max_output_tokens: this.#maxOutputTokens,
      });
      if (response.output_parsed === null) {
        throw new WorkerRuntimeError("invalid_output", "The model returned no structured worker result");
      }
      return response.output_parsed;
    } catch (error) {
      throw normalizeOpenAIError(error);
    }
  }
}

export function createOpenAIResponsesGatewayFromEnv(
  env: NodeJS.ProcessEnv = process.env,
): OpenAIResponsesGateway {
  return new OpenAIResponsesGateway({
    apiKey: env.OPENAI_API_KEY ?? "",
    model: env.OPENAI_MODEL ?? "",
    timeoutMs: positiveInteger(env.FLORENCE_MODEL_TIMEOUT_MS, 30_000),
    maxOutputTokens: positiveInteger(env.FLORENCE_MODEL_MAX_OUTPUT_TOKENS, 4_000),
  });
}

function normalizeOpenAIError(error: unknown): WorkerRuntimeError {
  if (error instanceof WorkerRuntimeError) return error;
  if (error instanceof ZodError) return failure("invalid_output", "OpenAI returned invalid output", error);
  if (error instanceof RateLimitError) return failure("rate_limited", "OpenAI rate limit reached", error);
  if (error instanceof LengthFinishReasonError) {
    return failure("context_exceeded", "The model response exceeded its token budget", error);
  }
  if (error instanceof ContentFilterFinishReasonError) {
    return failure("permanent", "The model refused this worker request", error);
  }
  if (
    error instanceof APIConnectionError ||
    error instanceof ConflictError ||
    error instanceof InternalServerError
  ) {
    return failure("transient", "Temporary OpenAI request failure", error);
  }
  if (error instanceof BadRequestError && error.code === "context_length_exceeded") {
    return failure("context_exceeded", "The worker context exceeds the configured model", error);
  }
  if (
    error instanceof AuthenticationError ||
    error instanceof PermissionDeniedError ||
    error instanceof BadRequestError ||
    error instanceof APIError
  ) {
    return failure("permanent", "OpenAI rejected the worker request", error);
  }
  return failure("permanent", "Unexpected OpenAI adapter failure", error);
}

function failure(
  category: ConstructorParameters<typeof WorkerRuntimeError>[0],
  message: string,
  cause: unknown,
): WorkerRuntimeError {
  return new WorkerRuntimeError(category, message, { cause });
}

function positiveInteger(value: string | undefined, fallback: number): number {
  if (value === undefined) return fallback;
  return validLimit(Number(value));
}

function validLimit(value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0)
    throw new WorkerRuntimeError("permanent", "Model limits must be positive integers");
  return value;
}
