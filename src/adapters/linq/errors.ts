export type LinqWebhookErrorCode =
  | "body_too_large"
  | "event_id_mismatch"
  | "invalid_json"
  | "invalid_payload"
  | "invalid_secret"
  | "invalid_signature"
  | "invalid_timestamp"
  | "missing_header"
  | "stale_timestamp"
  | "unsupported_version";

export class LinqWebhookError extends Error {
  readonly code: LinqWebhookErrorCode;

  constructor(code: LinqWebhookErrorCode, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "LinqWebhookError";
    this.code = code;
  }
}

export class LinqConfigurationError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "LinqConfigurationError";
  }
}

export class LinqApiError extends Error {
  readonly status: number;
  readonly providerCode: string | number | null;
  readonly retryable: boolean;

  constructor(
    message: string,
    details: {
      status: number;
      providerCode: string | number | null;
      retryable: boolean;
    },
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "LinqApiError";
    this.status = details.status;
    this.providerCode = details.providerCode;
    this.retryable = details.retryable;
  }
}

export class LinqAudienceChangedError extends Error {
  readonly expectedDigest: string;
  readonly actualDigest: string;

  constructor(expectedDigest: string, actualDigest: string) {
    super("The Linq participant set changed before send");
    this.name = "LinqAudienceChangedError";
    this.expectedDigest = expectedDigest;
    this.actualDigest = actualDigest;
  }
}

export type LinqAttachmentErrorCode =
  | "download_failed"
  | "download_url_not_allowed"
  | "integrity_mismatch"
  | "invalid_reference"
  | "missing_download_url"
  | "too_large";

export class LinqAttachmentError extends Error {
  readonly code: LinqAttachmentErrorCode;

  constructor(code: LinqAttachmentErrorCode, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "LinqAttachmentError";
    this.code = code;
  }
}
