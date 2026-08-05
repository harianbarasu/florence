export type GoogleAdapterErrorCode =
  | "invalid_request"
  | "unauthorized"
  | "forbidden"
  | "not_found"
  | "conflict"
  | "rate_limited"
  | "sync_token_expired"
  | "transient"
  | "permanent";

export class GoogleAdapterError extends Error {
  override readonly name: string = "GoogleAdapterError";

  constructor(
    message: string,
    readonly code: GoogleAdapterErrorCode,
    readonly status: number | null,
    readonly retryable: boolean,
  ) {
    super(message);
  }
}

export class GoogleSyncTokenExpiredError extends GoogleAdapterError {
  override readonly name = "GoogleSyncTokenExpiredError";

  constructor(provider: "gmail" | "calendar") {
    super(
      `${provider} sync cursor expired; a full resynchronization is required`,
      "sync_token_expired",
      provider === "gmail" ? 404 : 410,
      false,
    );
  }
}

export function mapGoogleProviderError(operation: string, error: unknown): GoogleAdapterError {
  if (error instanceof GoogleAdapterError) {
    return error;
  }
  const status = providerStatus(error);
  if (status === 400) {
    return new GoogleAdapterError(`${operation} was rejected`, "invalid_request", status, false);
  }
  if (status === 401) {
    return new GoogleAdapterError(`${operation} requires reauthorization`, "unauthorized", status, false);
  }
  if (status === 403 && isRateLimitReason(providerReason(error))) {
    return new GoogleAdapterError(`${operation} should be retried`, "rate_limited", status, true);
  }
  if (status === 403) {
    return new GoogleAdapterError(`${operation} is not permitted`, "forbidden", status, false);
  }
  if (status === 404) {
    return new GoogleAdapterError(
      `${operation} did not find the requested resource`,
      "not_found",
      status,
      false,
    );
  }
  if (status === 409 || status === 412) {
    return new GoogleAdapterError(
      `${operation} conflicts with current provider state`,
      "conflict",
      status,
      false,
    );
  }
  if (status === 408 || status === 429) {
    return new GoogleAdapterError(
      `${operation} should be retried`,
      status === 429 ? "rate_limited" : "transient",
      status,
      true,
    );
  }
  if (status !== null && status >= 500) {
    return new GoogleAdapterError(`${operation} failed transiently`, "transient", status, true);
  }
  if (isTransientNetworkError(error)) {
    return new GoogleAdapterError(`${operation} failed transiently`, "transient", null, true);
  }
  return new GoogleAdapterError(`${operation} failed`, "permanent", status, false);
}

function providerReason(error: unknown): string | null {
  if (typeof error !== "object" || error === null) return null;
  const response = (error as Record<string, unknown>).response;
  if (typeof response !== "object" || response === null) return null;
  const data = (response as Record<string, unknown>).data;
  if (typeof data !== "object" || data === null) return null;
  const providerError = (data as Record<string, unknown>).error;
  if (typeof providerError !== "object" || providerError === null) return null;
  const errors = (providerError as Record<string, unknown>).errors;
  if (!Array.isArray(errors)) return null;
  for (const item of errors) {
    if (typeof item !== "object" || item === null) continue;
    const reason = (item as Record<string, unknown>).reason;
    if (typeof reason === "string") return reason;
  }
  return null;
}

function isRateLimitReason(reason: string | null): boolean {
  return reason === "rateLimitExceeded" || reason === "userRateLimitExceeded";
}

function isTransientNetworkError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  if (error.name === "AbortError" || error.name === "TimeoutError") return true;
  const code = (error as Error & { code?: unknown }).code;
  return (
    typeof code === "string" &&
    ["ECONNRESET", "ECONNREFUSED", "EAI_AGAIN", "ENETUNREACH", "ETIMEDOUT"].includes(code)
  );
}

export function providerStatus(error: unknown): number | null {
  if (typeof error !== "object" || error === null) {
    return null;
  }
  const record = error as Record<string, unknown>;
  const response =
    typeof record.response === "object" && record.response !== null
      ? (record.response as Record<string, unknown>)
      : null;
  for (const value of [response?.status, record.status, record.code]) {
    if (typeof value === "number" && Number.isInteger(value)) {
      return value;
    }
    if (typeof value === "string" && /^\d{3}$/.test(value)) {
      return Number(value);
    }
  }
  return null;
}
