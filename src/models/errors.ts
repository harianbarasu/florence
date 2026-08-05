import { z } from "zod";

export const ModelGatewayErrorCodeSchema = z.enum([
  "rate_limited",
  "context_exceeded",
  "unsupported_capability",
  "invalid_output",
  "transient",
  "permanent",
  "cancelled",
]);

export type ModelGatewayErrorCode = z.infer<typeof ModelGatewayErrorCodeSchema>;

const ERROR_MESSAGES: Readonly<Record<ModelGatewayErrorCode, string>> = Object.freeze({
  rate_limited: "The model provider rate limited the request.",
  context_exceeded: "The model request exceeded the configured context window.",
  unsupported_capability: "No eligible model route supports the requested capability profile.",
  invalid_output: "The model returned output that failed application validation.",
  transient: "The model provider request failed transiently.",
  permanent: "The model provider request failed permanently.",
  cancelled: "The model request was cancelled.",
});

export class ModelGatewayError extends Error {
  readonly code: ModelGatewayErrorCode;
  readonly retryable: boolean;

  constructor(code: ModelGatewayErrorCode) {
    super(ERROR_MESSAGES[code]);
    this.name = "ModelGatewayError";
    this.code = code;
    this.retryable = code === "rate_limited" || code === "transient";
  }
}

export function asModelGatewayError(error: unknown, signal?: AbortSignal): ModelGatewayError {
  if (error instanceof ModelGatewayError) {
    return error;
  }

  if (signal?.aborted || isAbortError(error)) {
    return new ModelGatewayError("cancelled");
  }

  if (error instanceof z.ZodError) {
    return new ModelGatewayError("invalid_output");
  }

  const status = readNumericProperty(error, "status") ?? readNumericProperty(error, "statusCode");
  const code = readStringProperty(error, "code")?.toLowerCase();
  const message = readStringProperty(error, "message")?.toLowerCase() ?? "";

  if (status === 429 || code === "rate_limit_exceeded" || code === "rate_limit_error") {
    return new ModelGatewayError("rate_limited");
  }

  if (
    code === "context_length_exceeded" ||
    message.includes("context length") ||
    message.includes("context window") ||
    message.includes("too many tokens")
  ) {
    return new ModelGatewayError("context_exceeded");
  }

  if (status !== undefined && [408, 409, 425, 500, 502, 503, 504].includes(status)) {
    return new ModelGatewayError("transient");
  }

  if (code !== undefined && ["econnreset", "etimedout", "eai_again", "enotfound"].includes(code)) {
    return new ModelGatewayError("transient");
  }

  return new ModelGatewayError("permanent");
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function readNumericProperty(value: unknown, property: string): number | undefined {
  if (typeof value !== "object" || value === null || !(property in value)) {
    return undefined;
  }
  const candidate = (value as Record<string, unknown>)[property];
  return typeof candidate === "number" ? candidate : undefined;
}

function readStringProperty(value: unknown, property: string): string | undefined {
  if (typeof value !== "object" || value === null || !(property in value)) {
    return undefined;
  }
  const candidate = (value as Record<string, unknown>)[property];
  return typeof candidate === "string" ? candidate : undefined;
}
