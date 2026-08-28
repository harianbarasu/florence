import { createHash } from "node:crypto";

/**
 * Direct TypeScript adaptation of Hermes Agent's optional telephony skill at
 * commit 6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882
 * (`optional-skills/productivity/telephony/scripts/telephony.py`). Florence
 * keeps Bland's task-driven outbound calls and Twilio's SMS, inbox polling,
 * and TwiML `<Say>` calls. It omits Hermes's CLI, credential persistence,
 * number purchasing, provider registry, and Vapi path. Vapi is intentionally
 * deferred because its current official contract does not establish a
 * reliable cancel/reconciliation path for durable scheduled work.
 */

const BLAND_API_BASE = "https://api.bland.ai/v1";
const TWILIO_API_BASE = "https://api.twilio.com/2010-04-01/Accounts";
const BLAND_DEFAULT_VOICE = "mason";
const BLAND_DEFAULT_MODEL = "enhanced";
const TWILIO_DEFAULT_TTS_VOICE = "Polly.Joanna";
const DEFAULT_TIMEOUT_MS = 20_000;
const MAX_PROVIDER_RESPONSE_BYTES = 2 * 1_024 * 1_024;
const MAX_TASK_CHARS = 12_000;
const MAX_MESSAGE_CHARS = 10_000;
const MAX_TRANSCRIPT_CHARS = 50_000;
const MAX_SUMMARY_CHARS = 8_000;
const MAX_ERROR_CHARS = 1_000;
const BLAND_RECONCILIATION_LOOKBACK_MS = 60 * 60 * 1_000;
const BLAND_RECONCILIATION_FUTURE_MARGIN_MS = 60 * 1_000;
const BLAND_RECONCILIATION_CANDIDATE_LIMIT = 20;
const BLAND_PENDING_CALL_PREFIX = "pending_bland_";
const TWILIO_RECONCILIATION_LOOKBACK_MS = 60 * 60 * 1_000;
const TWILIO_RECONCILIATION_FUTURE_MARGIN_MS = 60 * 1_000;
const TWILIO_RECONCILIATION_CANDIDATE_LIMIT = 100;
const TWILIO_PENDING_SMS_PREFIX = "pending_twilio_sms_";
const TWILIO_PENDING_CALL_PREFIX = "pending_twilio_call_";

export type FlorenceTelephonyProvider = "bland" | "twilio";

export type FlorenceTelephonyOperation =
  | {
      readonly kind: "ai_call_start";
      readonly provider: "bland";
      readonly to: string;
      readonly task: string;
      readonly firstSentence?: string;
      readonly voice?: string;
      readonly maxDurationMinutes?: number;
      readonly record?: boolean;
      readonly summaryPrompt?: string;
      readonly dispositions?: readonly string[];
    }
  | {
      readonly kind: "ai_call_status";
      readonly provider: "bland";
      readonly providerCallId: string;
    }
  | {
      readonly kind: "ai_call_cancel";
      readonly provider: "bland";
      readonly providerCallId: string;
    }
  | {
      readonly kind: "sms_send";
      readonly provider: "twilio";
      readonly to: string;
      readonly body: string;
      readonly mediaUrls?: readonly string[];
    }
  | {
      readonly kind: "sms_status";
      readonly provider: "twilio";
      readonly messageSid: string;
    }
  | {
      readonly kind: "sms_inbox";
      readonly provider: "twilio";
      readonly from?: string;
      readonly limit?: number;
    }
  | {
      readonly kind: "call_start";
      readonly provider: "twilio";
      readonly to: string;
      readonly message: string;
      readonly voice?: string;
      readonly sendDigits?: string;
      readonly record?: boolean;
    }
  | {
      readonly kind: "call_status";
      readonly provider: "twilio";
      readonly callSid: string;
    }
  | {
      readonly kind: "call_cancel";
      readonly provider: "twilio";
      readonly callSid: string;
    };

export interface FlorenceTelephonyRunInput {
  readonly workId: string;
  readonly callId: string;
  readonly attempt: number;
  readonly operation: FlorenceTelephonyOperation;
}

export interface FlorenceTelephonyMessage {
  readonly messageSid: string;
  readonly direction: string | null;
  readonly status: string | null;
  readonly fromPhoneNumber: string | null;
  readonly toPhoneNumber: string | null;
  readonly sentAt: string | null;
  readonly body: string;
  readonly mediaCount: number;
}

export type FlorenceTelephonyResultKind =
  | "accepted"
  | "progress"
  | "completed"
  | "failed"
  | "uncertain_effect";

export interface FlorenceTelephonyResult {
  readonly kind: FlorenceTelephonyResultKind;
  readonly provider: FlorenceTelephonyProvider;
  readonly operation: FlorenceTelephonyOperation["kind"];
  readonly providerId: string | null;
  readonly providerStatus: string | null;
  readonly reason: string | null;
  readonly toPhoneNumberMasked: string | null;
  readonly answeredBy: string | null;
  readonly durationSeconds: number | null;
  readonly summary: string | null;
  readonly disposition: string | null;
  readonly transcript: string | null;
  readonly recordingUrl: string | null;
  readonly messages: readonly FlorenceTelephonyMessage[];
}

export type FlorenceTelephonyErrorCode =
  | "invalid_input"
  | "unavailable"
  | "provider_error"
  | "invalid_response"
  | "cancelled";

export class FlorenceTelephonyError extends Error {
  readonly code: FlorenceTelephonyErrorCode;
  readonly retryable: boolean;
  readonly safeMessage: string;

  constructor(
    code: FlorenceTelephonyErrorCode,
    safeMessage: string,
    options: { readonly retryable?: boolean; readonly cause?: unknown } = {},
  ) {
    super("Florence telephony operation failed", { cause: options.cause });
    this.name = "FlorenceTelephonyError";
    this.code = code;
    this.retryable = options.retryable ?? (code === "unavailable" || code === "invalid_response");
    this.safeMessage = boundedString(
      safeMessage,
      MAX_ERROR_CHARS,
      "The phone provider could not complete that step.",
    );
  }
}

export interface FlorenceTelephonyClient {
  readonly configuredProviders: readonly FlorenceTelephonyProvider[];
  run(input: FlorenceTelephonyRunInput, signal?: AbortSignal): Promise<FlorenceTelephonyResult>;
}

export interface BlandTelephonyOptions {
  readonly apiKey: string;
  readonly baseUrl?: string;
  readonly defaultVoice?: string;
}

export interface TwilioTelephonyOptions {
  readonly accountSid: string;
  readonly authToken: string;
  readonly phoneNumber: string;
  readonly baseUrl?: string;
}

export interface ProviderTelephonyClientOptions {
  readonly bland?: BlandTelephonyOptions;
  readonly twilio?: TwilioTelephonyOptions;
  readonly fetch?: typeof globalThis.fetch;
  readonly timeoutMs?: number;
}

interface BlandConfig {
  readonly apiKey: string;
  readonly baseUrl: string;
  readonly defaultVoice: string;
}

interface TwilioConfig {
  readonly accountSid: string;
  readonly authToken: string;
  readonly phoneNumber: string;
  readonly baseUrl: string;
}

interface PendingTwilioSmsHandle {
  readonly destination: string;
  readonly requestedAt: number;
  readonly fingerprint: string;
}

interface PendingTwilioCallHandle {
  readonly destination: string;
  readonly requestedAt: number;
}

const BLAND_TERMINAL_STATUSES = new Set([
  "completed",
  "failed",
  "busy",
  "no-answer",
  "canceled",
  "cancelled",
  "unknown",
]);
const BLAND_QUEUE_FAILURE_STATUSES = new Set(["pre-queue-error", "queue-error", "call-error"]);
const TWILIO_MESSAGE_COMPLETED_STATUSES = new Set(["delivered", "read", "received"]);
const TWILIO_MESSAGE_FAILED_STATUSES = new Set(["failed", "undelivered", "canceled"]);
const TWILIO_CALL_FAILED_STATUSES = new Set(["failed", "busy", "no-answer", "canceled"]);
const CREATE_OPERATIONS = new Set<FlorenceTelephonyOperation["kind"]>([
  "ai_call_start",
  "sms_send",
  "call_start",
]);
const SUPPORTED_OPERATIONS = new Set<FlorenceTelephonyOperation["kind"]>([
  "ai_call_start",
  "ai_call_status",
  "ai_call_cancel",
  "sms_send",
  "sms_status",
  "sms_inbox",
  "call_start",
  "call_status",
  "call_cancel",
]);

export class ProviderTelephonyClient implements FlorenceTelephonyClient {
  readonly configuredProviders: readonly FlorenceTelephonyProvider[];
  readonly #bland: BlandConfig | null;
  readonly #twilio: TwilioConfig | null;
  readonly #fetch: typeof globalThis.fetch;
  readonly #timeoutMs: number;

  constructor(options: ProviderTelephonyClientOptions) {
    this.#bland = options.bland
      ? {
          apiKey: requireNonEmpty(options.bland.apiKey, "Bland API key", 10_000),
          baseUrl: normalizeBaseUrl(options.bland.baseUrl ?? BLAND_API_BASE),
          defaultVoice: requireNonEmpty(
            options.bland.defaultVoice ?? BLAND_DEFAULT_VOICE,
            "Bland default voice",
            200,
          ),
        }
      : null;
    this.#twilio = options.twilio
      ? {
          accountSid: validTwilioAccountSid(options.twilio.accountSid),
          authToken: requireNonEmpty(options.twilio.authToken, "Twilio auth token", 10_000),
          phoneNumber: normalizePhone(options.twilio.phoneNumber),
          baseUrl: normalizeBaseUrl(options.twilio.baseUrl ?? TWILIO_API_BASE),
        }
      : null;
    if (!this.#bland && !this.#twilio) {
      throw new FlorenceTelephonyError(
        "invalid_input",
        "Configure Bland, Twilio, or both before starting Florence telephony.",
      );
    }
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#timeoutMs = boundedInteger(options.timeoutMs ?? DEFAULT_TIMEOUT_MS, 1_000, 120_000);
    this.configuredProviders = [
      ...(this.#bland ? (["bland"] as const) : []),
      ...(this.#twilio ? (["twilio"] as const) : []),
    ];
  }

  async run(
    untrustedInput: FlorenceTelephonyRunInput,
    signal?: AbortSignal,
  ): Promise<FlorenceTelephonyResult> {
    const input = validateRunInput(untrustedInput);
    throwIfCancelled(signal);

    if (CREATE_OPERATIONS.has(input.operation.kind) && input.attempt > 1) {
      switch (input.operation.kind) {
        case "ai_call_start": {
          const destination = normalizePhone(input.operation.to);
          const operationDigest = operationIdDigest(input.callId);
          return this.#reconcileBlandCall(
            { ...input, operation: input.operation },
            destination,
            operationDigest,
            pendingBlandCallHandle(destination, operationDigest),
            signal,
          );
        }
        case "sms_send": {
          const requestedAt = Date.now();
          const smsInput = { ...input, operation: input.operation };
          return this.#reconcileTwilioSms(
            smsInput,
            requestedAt,
            pendingTwilioSmsHandle(smsInput, requestedAt),
            signal,
          );
        }
        case "call_start": {
          const requestedAt = Date.now();
          const callInput = { ...input, operation: input.operation };
          return this.#reconcileTwilioCall(
            callInput,
            requestedAt,
            pendingTwilioCallHandle(callInput, requestedAt),
            signal,
          );
        }
      }
    }

    try {
      switch (input.operation.kind) {
        case "ai_call_start":
          return await this.#startBlandCall({ ...input, operation: input.operation }, signal);
        case "ai_call_status":
          return await this.#readBlandCall({ ...input, operation: input.operation }, signal);
        case "ai_call_cancel":
          return await this.#cancelBlandCall({ ...input, operation: input.operation }, signal);
        case "sms_send":
          return await this.#sendTwilioSms({ ...input, operation: input.operation }, signal);
        case "sms_status":
          return await this.#readTwilioSms({ ...input, operation: input.operation }, signal);
        case "sms_inbox":
          return await this.#readTwilioInbox({ ...input, operation: input.operation }, signal);
        case "call_start":
          return await this.#startTwilioCall({ ...input, operation: input.operation }, signal);
        case "call_status":
          return await this.#readTwilioCall({ ...input, operation: input.operation }, signal);
        case "call_cancel":
          return await this.#cancelTwilioCall({ ...input, operation: input.operation }, signal);
      }
    } catch (error) {
      if (error instanceof AmbiguousMutationError) {
        return uncertainResult(input, error.safeMessage, error.providerId);
      }
      throw asTelephonyError(error, signal);
    }
  }

  async #startBlandCall(
    input: FlorenceTelephonyRunInput & {
      readonly operation: Extract<FlorenceTelephonyOperation, { kind: "ai_call_start" }>;
    },
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    const config = this.#requireBland();
    const operation = input.operation;
    const destination = normalizePhone(operation.to);
    const operationDigest = operationIdDigest(input.callId);
    const body: Record<string, unknown> = {
      phone_number: destination,
      task: requireNonEmpty(operation.task, "Bland call task", MAX_TASK_CHARS),
      voice: optionalNonEmpty(operation.voice, "Bland voice", 200) ?? config.defaultVoice,
      model: BLAND_DEFAULT_MODEL,
      max_duration: boundedInteger(operation.maxDurationMinutes ?? 3, 1, 30),
      record: operation.record ?? true,
      wait_for_greeting: true,
      metadata: {
        florence_work_id: input.workId,
        florence_operation_id: operationDigest,
      },
    };
    add(body, "first_sentence", optionalNonEmpty(operation.firstSentence, "Bland first sentence", 2_000));
    add(body, "summary_prompt", optionalNonEmpty(operation.summaryPrompt, "Bland summary prompt", 4_000));
    if (operation.dispositions) {
      if (operation.dispositions.length > 20) {
        throw invalidInput("Bland calls support at most 20 dispositions.");
      }
      body.dispositions = operation.dispositions.map((value) =>
        requireNonEmpty(value, "Bland disposition", 300),
      );
    }

    let payload: Readonly<Record<string, unknown>>;
    try {
      payload = await this.#requestJson(
        config.baseUrl,
        "/calls",
        {
          method: "POST",
          headers: { authorization: config.apiKey, "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
        true,
        signal,
      );
      const providerId = requiredRecordString(payload, "call_id", 300, true);
      return result(input, {
        kind: "accepted",
        providerId,
        providerStatus: readRecordString(payload, "status", 100),
        toPhoneNumberMasked: maskPhone(operation.to),
      });
    } catch (error) {
      if (!(error instanceof AmbiguousMutationError)) throw error;
      return this.#reconcileBlandCall(
        input,
        destination,
        operationDigest,
        pendingBlandCallHandle(destination, operationDigest),
        signal,
      );
    }
  }

  async #reconcileBlandCall(
    input: FlorenceTelephonyRunInput,
    destination: string,
    operationDigest: string,
    pendingHandle: string,
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    const config = this.#requireBland();
    const now = Date.now();
    const query = new URLSearchParams({
      to_number: destination,
      start_date: new Date(now - BLAND_RECONCILIATION_LOOKBACK_MS).toISOString(),
      end_date: new Date(now + BLAND_RECONCILIATION_FUTURE_MARGIN_MS).toISOString(),
      limit: String(BLAND_RECONCILIATION_CANDIDATE_LIMIT),
      ascending: "false",
      sort_by: "created_at",
    });

    try {
      const listed = await this.#requestJson(
        config.baseUrl,
        `/calls?${query.toString()}`,
        { method: "GET", headers: { authorization: config.apiKey } },
        false,
        signal,
      );
      const matched: Array<{
        readonly providerId: string;
        readonly payload: Readonly<Record<string, unknown>>;
      }> = [];
      const seenIds = new Set<string>();

      for (const candidate of readRecordArray(listed, "calls", BLAND_RECONCILIATION_CANDIDATE_LIMIT)) {
        const rawProviderId = readRecordString(candidate, "call_id", 300);
        if (!rawProviderId || seenIds.has(rawProviderId)) continue;
        let providerId: string;
        try {
          providerId = validProviderId(rawProviderId, "Bland call ID");
        } catch {
          continue;
        }
        seenIds.add(providerId);
        const detail = await this.#requestJson(
          config.baseUrl,
          `/calls/${encodeURIComponent(providerId)}`,
          { method: "GET", headers: { authorization: config.apiKey } },
          false,
          signal,
        );
        if (!hasBlandCorrelationMetadata(detail, input.workId, operationDigest)) continue;
        matched.push({ providerId, payload: detail });
        if (matched.length > 1) break;
      }

      if (matched.length === 1) {
        const match = matched[0];
        if (!match) throw invalidResponse("Bland reconciliation returned an incomplete match.");
        return normalizeBlandCall(input, match.providerId, match.payload);
      }
      return result(input, {
        kind: "progress",
        providerId: pendingHandle,
        providerStatus: "reconciling",
        reason:
          matched.length > 1
            ? "More than one call currently matches the original request. Florence is keeping the correlation handle and will not place another call."
            : "The original call is not visible yet. Florence is keeping the correlation handle and will not place another call.",
        toPhoneNumberMasked: maskPhone(destination),
      });
    } catch (error) {
      if (signal?.aborted) throw error;
      return result(input, {
        kind: "progress",
        providerId: pendingHandle,
        providerStatus: "reconciling",
        reason:
          "Florence could not complete the recent-call recovery check yet. It kept the correlation handle and did not place another call.",
        toPhoneNumberMasked: maskPhone(destination),
      });
    }
  }

  async #readBlandCall(
    input: FlorenceTelephonyRunInput & {
      readonly operation: Extract<FlorenceTelephonyOperation, { kind: "ai_call_status" }>;
    },
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    const providerId = validProviderId(input.operation.providerCallId, "Bland call ID");
    const pending = parsePendingBlandCallHandle(providerId);
    if (pending) {
      return this.#reconcileBlandCall(
        input,
        pending.destination,
        pending.operationDigest,
        providerId,
        signal,
      );
    }
    const config = this.#requireBland();
    const payload = await this.#requestJson(
      config.baseUrl,
      `/calls/${encodeURIComponent(providerId)}`,
      { method: "GET", headers: { authorization: config.apiKey } },
      false,
      signal,
    );
    return normalizeBlandCall(input, providerId, payload);
  }

  async #cancelBlandCall(
    input: FlorenceTelephonyRunInput & {
      readonly operation: Extract<FlorenceTelephonyOperation, { kind: "ai_call_cancel" }>;
    },
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    const config = this.#requireBland();
    const requestedProviderId = validProviderId(input.operation.providerCallId, "Bland call ID");
    const pending = parsePendingBlandCallHandle(requestedProviderId);
    let providerId = requestedProviderId;
    if (pending) {
      const reconciliation = await this.#reconcileBlandCall(
        input,
        pending.destination,
        pending.operationDigest,
        requestedProviderId,
        signal,
      );
      if (!reconciliation.providerId || reconciliation.providerId === requestedProviderId) {
        return reconciliation;
      }
      providerId = reconciliation.providerId;
      if (reconciliation.kind === "completed" || reconciliation.kind === "failed") {
        return result(input, {
          kind: "completed",
          providerId,
          providerStatus: reconciliation.providerStatus,
        });
      }
    }
    if (input.attempt > 1) {
      const statusPayload = await this.#requestJson(
        config.baseUrl,
        `/calls/${encodeURIComponent(providerId)}`,
        { method: "GET", headers: { authorization: config.apiKey } },
        false,
        signal,
      );
      const status = normalizedBlandStatus(statusPayload);
      if (status && isBlandTerminalStatus(status)) {
        return result(input, { kind: "completed", providerId, providerStatus: status });
      }
    }
    let payload: Readonly<Record<string, unknown>>;
    try {
      payload = await this.#requestJson(
        config.baseUrl,
        `/calls/${encodeURIComponent(providerId)}/stop`,
        {
          method: "POST",
          headers: { authorization: config.apiKey, "Content-Type": "application/json" },
          body: "{}",
        },
        true,
        signal,
      );
    } catch (error) {
      if (error instanceof AmbiguousMutationError) {
        throw new AmbiguousMutationError(error.safeMessage, { providerId, cause: error });
      }
      throw error;
    }
    const status = normalizedBlandStatus(payload);
    const terminal = status !== null && isBlandTerminalStatus(status);
    return result(input, {
      kind: terminal ? "completed" : "progress",
      providerId,
      providerStatus: terminal ? status : "cancel-requested",
      reason: terminal
        ? null
        : "Bland accepted the stop request. Florence is waiting for the call to reach a terminal status.",
    });
  }

  async #sendTwilioSms(
    input: FlorenceTelephonyRunInput & {
      readonly operation: Extract<FlorenceTelephonyOperation, { kind: "sms_send" }>;
    },
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    const config = this.#requireTwilio();
    const destination = normalizePhone(input.operation.to);
    const form = new URLSearchParams({
      To: destination,
      From: config.phoneNumber,
      Body: requireNonEmpty(input.operation.body, "SMS body", MAX_MESSAGE_CHARS),
    });
    if ((input.operation.mediaUrls?.length ?? 0) > 10) {
      throw invalidInput("Twilio messages support at most 10 media URLs.");
    }
    for (const mediaUrl of input.operation.mediaUrls ?? []) {
      form.append("MediaUrl", validPublicHttpUrl(mediaUrl));
    }
    const requestedAt = Date.now();
    const pendingHandle = pendingTwilioSmsHandle(input, requestedAt);
    try {
      const payload = await this.#twilioRequest("Messages.json", "POST", form, true, signal);
      const providerId = requiredRecordString(payload, "sid", 100, true);
      return result(input, {
        kind: "accepted",
        providerId,
        providerStatus: readRecordString(payload, "status", 100),
        toPhoneNumberMasked: maskPhone(destination),
      });
    } catch (error) {
      if (!(error instanceof AmbiguousMutationError)) throw error;
      if (signal?.aborted) {
        return result(input, {
          kind: "progress",
          providerId: pendingHandle,
          providerStatus: "reconciling",
          reason:
            "The task changed while Twilio was accepting the text. Florence kept the correlation handle and will not send another text.",
          toPhoneNumberMasked: maskPhone(destination),
        });
      }
      return this.#reconcileTwilioSms(input, requestedAt, pendingHandle, signal);
    }
  }

  async #readTwilioSms(
    input: FlorenceTelephonyRunInput & {
      readonly operation: Extract<FlorenceTelephonyOperation, { kind: "sms_status" }>;
    },
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    const providerId = validProviderId(input.operation.messageSid, "Twilio message SID");
    const pending = parsePendingTwilioSmsHandle(providerId);
    if (pending) {
      return this.#reconcileTwilioSmsFromHandle(input, pending, providerId, signal);
    }
    const payload = await this.#twilioRequest(
      `Messages/${encodeURIComponent(providerId)}.json`,
      "GET",
      undefined,
      false,
      signal,
    );
    return normalizeTwilioSms(input, providerId, payload);
  }

  async #readTwilioInbox(
    input: FlorenceTelephonyRunInput & {
      readonly operation: Extract<FlorenceTelephonyOperation, { kind: "sms_inbox" }>;
    },
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    const config = this.#requireTwilio();
    const query = new URLSearchParams({
      To: config.phoneNumber,
      PageSize: String(boundedInteger(input.operation.limit ?? 20, 1, 100)),
    });
    if (input.operation.from) query.set("From", normalizePhone(input.operation.from));
    const payload = await this.#twilioRequest(
      `Messages.json?${query.toString()}`,
      "GET",
      undefined,
      false,
      signal,
    );
    const rows = readRecordArray(payload, "messages", 100).map(normalizeTwilioMessage);
    return result(input, { kind: "completed", providerStatus: "read", messages: rows });
  }

  async #reconcileTwilioSms(
    input: FlorenceTelephonyRunInput & {
      readonly operation: Extract<FlorenceTelephonyOperation, { kind: "sms_send" }>;
    },
    requestedAt: number,
    pendingHandle: string,
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    const destination = normalizePhone(input.operation.to);
    const fingerprint = twilioSmsFingerprint({
      destination,
      body: requireNonEmpty(input.operation.body, "SMS body", MAX_MESSAGE_CHARS),
      mediaCount: input.operation.mediaUrls?.length ?? 0,
    });
    return this.#findTwilioSms(input, { destination, requestedAt, fingerprint }, pendingHandle, signal);
  }

  async #reconcileTwilioSmsFromHandle(
    input: FlorenceTelephonyRunInput,
    pending: PendingTwilioSmsHandle,
    pendingHandle: string,
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    return this.#findTwilioSms(input, pending, pendingHandle, signal);
  }

  async #findTwilioSms(
    input: FlorenceTelephonyRunInput,
    pending: PendingTwilioSmsHandle,
    pendingHandle: string,
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    const config = this.#requireTwilio();
    const query = twilioReconciliationQuery({
      destination: pending.destination,
      source: config.phoneNumber,
      dateField: "DateSent>",
      requestedAt: pending.requestedAt,
    });
    try {
      const payload = await this.#twilioRequest(
        `Messages.json?${query.toString()}`,
        "GET",
        undefined,
        false,
        signal,
      );
      const matches = readRecordArray(payload, "messages", TWILIO_RECONCILIATION_CANDIDATE_LIMIT).filter(
        (candidate) => isMatchingTwilioSms(candidate, pending, config.phoneNumber),
      );
      if (matches.length === 1) {
        const match = matches[0];
        if (!match) throw invalidResponse("Twilio message reconciliation returned an incomplete match.");
        const providerId = requiredRecordString(match, "sid", 100);
        return normalizeTwilioSms(input, providerId, match);
      }
      return result(input, {
        kind: "progress",
        providerId: pendingHandle,
        providerStatus: "reconciling",
        reason:
          matches.length > 1
            ? "More than one recent text matches the original request. Florence kept the correlation handle and did not send another text."
            : "The original text is not visible yet. Florence kept the correlation handle and did not send another text.",
        toPhoneNumberMasked: maskPhone(pending.destination),
      });
    } catch (error) {
      if (signal?.aborted) throw error;
      return result(input, {
        kind: "progress",
        providerId: pendingHandle,
        providerStatus: "reconciling",
        reason:
          "Florence could not complete the recent-message recovery check yet. It kept the correlation handle and did not send another text.",
        toPhoneNumberMasked: maskPhone(pending.destination),
      });
    }
  }

  async #startTwilioCall(
    input: FlorenceTelephonyRunInput & {
      readonly operation: Extract<FlorenceTelephonyOperation, { kind: "call_start" }>;
    },
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    const config = this.#requireTwilio();
    const destination = normalizePhone(input.operation.to);
    const voice = optionalNonEmpty(input.operation.voice, "Twilio voice", 200) ?? TWILIO_DEFAULT_TTS_VOICE;
    const message = requireNonEmpty(input.operation.message, "Twilio call message", MAX_MESSAGE_CHARS);
    const form = new URLSearchParams({
      To: destination,
      From: config.phoneNumber,
      Twiml: `<Response><Say voice="${escapeXml(voice)}">${escapeXml(message)}</Say></Response>`,
    });
    if (input.operation.sendDigits) {
      const digits = input.operation.sendDigits.trim();
      if (!/^[0-9wW*#]{1,100}$/.test(digits)) {
        throw invalidInput("Twilio sendDigits may contain only digits, w, *, and #.");
      }
      form.set("SendDigits", digits);
    }
    if (input.operation.record) form.set("Record", "true");
    const requestedAt = Date.now();
    const pendingHandle = pendingTwilioCallHandle(input, requestedAt);
    try {
      const payload = await this.#twilioRequest("Calls.json", "POST", form, true, signal);
      const providerId = requiredRecordString(payload, "sid", 100, true);
      return result(input, {
        kind: "accepted",
        providerId,
        providerStatus: readRecordString(payload, "status", 100),
        toPhoneNumberMasked: maskPhone(destination),
      });
    } catch (error) {
      if (!(error instanceof AmbiguousMutationError)) throw error;
      if (signal?.aborted) {
        return result(input, {
          kind: "progress",
          providerId: pendingHandle,
          providerStatus: "reconciling",
          reason:
            "The task changed while Twilio was accepting the call. Florence kept the correlation handle and will not place another call.",
          toPhoneNumberMasked: maskPhone(destination),
        });
      }
      return this.#reconcileTwilioCall(input, requestedAt, pendingHandle, signal);
    }
  }

  async #reconcileTwilioCall(
    input: FlorenceTelephonyRunInput & {
      readonly operation: Extract<FlorenceTelephonyOperation, { kind: "call_start" }>;
    },
    requestedAt: number,
    pendingHandle: string,
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    return this.#findTwilioCall(
      input,
      { destination: normalizePhone(input.operation.to), requestedAt },
      pendingHandle,
      signal,
    );
  }

  async #reconcileTwilioCallFromHandle(
    input: FlorenceTelephonyRunInput,
    pending: PendingTwilioCallHandle,
    pendingHandle: string,
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    return this.#findTwilioCall(input, pending, pendingHandle, signal);
  }

  async #findTwilioCall(
    input: FlorenceTelephonyRunInput,
    pending: PendingTwilioCallHandle,
    pendingHandle: string,
    _signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    return result(input, {
      kind: "progress",
      providerId: pendingHandle,
      providerStatus: "reconciling",
      reason:
        "Twilio's recent-call log does not expose the original TwiML or an operation identifier, so Florence could not prove which call belongs to this request. It kept the correlation handle and did not place another call.",
      toPhoneNumberMasked: maskPhone(pending.destination),
    });
  }

  async #readTwilioCall(
    input: FlorenceTelephonyRunInput & {
      readonly operation: Extract<FlorenceTelephonyOperation, { kind: "call_status" }>;
    },
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    const providerId = validProviderId(input.operation.callSid, "Twilio call SID");
    const pending = parsePendingTwilioCallHandle(providerId);
    if (pending) {
      return this.#reconcileTwilioCallFromHandle(input, pending, providerId, signal);
    }
    const payload = await this.#twilioRequest(
      `Calls/${encodeURIComponent(providerId)}.json`,
      "GET",
      undefined,
      false,
      signal,
    );
    return normalizeTwilioCall(input, providerId, payload);
  }

  async #cancelTwilioCall(
    input: FlorenceTelephonyRunInput & {
      readonly operation: Extract<FlorenceTelephonyOperation, { kind: "call_cancel" }>;
    },
    signal: AbortSignal | undefined,
  ): Promise<FlorenceTelephonyResult> {
    const requestedProviderId = validProviderId(input.operation.callSid, "Twilio call SID");
    const pending = parsePendingTwilioCallHandle(requestedProviderId);
    let providerId = requestedProviderId;
    if (pending) {
      const reconciliation = await this.#reconcileTwilioCallFromHandle(
        input,
        pending,
        requestedProviderId,
        signal,
      );
      if (!reconciliation.providerId || reconciliation.providerId === requestedProviderId) {
        return reconciliation;
      }
      providerId = reconciliation.providerId;
      if (reconciliation.kind === "completed" || reconciliation.kind === "failed") {
        return result(input, {
          kind: "completed",
          providerId,
          providerStatus: reconciliation.providerStatus,
        });
      }
    }
    if (input.attempt > 1) {
      const statusPayload = await this.#twilioRequest(
        `Calls/${encodeURIComponent(providerId)}.json`,
        "GET",
        undefined,
        false,
        signal,
      );
      const status = normalizedStatus(statusPayload);
      if (status && isTwilioCallTerminalStatus(status)) {
        return result(input, { kind: "completed", providerId, providerStatus: status });
      }
    }
    let payload: Readonly<Record<string, unknown>>;
    try {
      payload = await this.#twilioRequest(
        `Calls/${encodeURIComponent(providerId)}.json`,
        "POST",
        new URLSearchParams({ Status: "canceled" }),
        true,
        signal,
      );
    } catch (error) {
      if (error instanceof AmbiguousMutationError) {
        throw new AmbiguousMutationError(error.safeMessage, { providerId, cause: error });
      }
      throw error;
    }
    const status = normalizedStatus(payload);
    const terminal = status !== null && isTwilioCallTerminalStatus(status);
    return result(input, {
      kind: terminal ? "completed" : "progress",
      providerId,
      providerStatus: status ?? "cancel-requested",
      reason: terminal
        ? null
        : "Twilio accepted the cancel request. Florence is waiting for the call to reach a terminal status.",
    });
  }

  async #twilioRequest(
    path: string,
    method: "GET" | "POST",
    form: URLSearchParams | undefined,
    mutation: boolean,
    signal: AbortSignal | undefined,
  ): Promise<Readonly<Record<string, unknown>>> {
    const config = this.#requireTwilio();
    return this.#requestJson(
      `${config.baseUrl}/${encodeURIComponent(config.accountSid)}`,
      `/${path}`,
      {
        method,
        headers: {
          Authorization: `Basic ${Buffer.from(`${config.accountSid}:${config.authToken}`).toString("base64")}`,
          ...(form ? { "Content-Type": "application/x-www-form-urlencoded" } : {}),
        },
        ...(form ? { body: form.toString() } : {}),
      },
      mutation,
      signal,
    );
  }

  async #requestJson(
    baseUrl: string,
    path: string,
    init: RequestInit,
    mutation: boolean,
    signal: AbortSignal | undefined,
  ): Promise<Readonly<Record<string, unknown>>> {
    throwIfCancelled(signal);
    const timeoutController = new AbortController();
    const timeout = setTimeout(() => timeoutController.abort(), this.#timeoutMs);
    timeout.unref?.();
    const combinedSignal = signal
      ? AbortSignal.any([signal, timeoutController.signal])
      : timeoutController.signal;

    try {
      let response: Response;
      try {
        response = await this.#fetch(`${baseUrl}${path}`, {
          ...init,
          headers: { Accept: "application/json", ...init.headers },
          signal: combinedSignal,
        });
      } catch (error) {
        if (mutation) {
          throw new AmbiguousMutationError(
            "The phone provider request may have taken effect, but Florence did not receive a confirmation. It was not repeated.",
            { cause: error },
          );
        }
        if (signal?.aborted) {
          throw new FlorenceTelephonyError("cancelled", "Phone work was cancelled.", { cause: error });
        }
        throw new FlorenceTelephonyError(
          "unavailable",
          timeoutController.signal.aborted
            ? "The phone provider took too long to respond."
            : "The phone provider is temporarily unavailable.",
          { retryable: true, cause: error },
        );
      }

      if (!response.ok) {
        const providerReason = await readProviderError(response);
        if (mutation && isAmbiguousHttpStatus(response.status)) {
          throw new AmbiguousMutationError(
            `The phone provider returned ${response.status} after the request may have taken effect. Florence did not repeat it.`,
          );
        }
        throw new FlorenceTelephonyError(
          response.status === 429 || response.status >= 500 ? "unavailable" : "provider_error",
          providerReason ?? `The phone provider rejected the request (${response.status}).`,
          { retryable: !mutation && (response.status === 429 || response.status >= 500) },
        );
      }

      try {
        const value = await readBoundedJson(response);
        if (!isRecord(value)) throw invalidResponse("The phone provider returned an incomplete response.");
        return value;
      } catch (error) {
        if (mutation) {
          throw new AmbiguousMutationError(
            "The phone provider accepted the request but returned an unreadable confirmation. Florence did not repeat it.",
            { cause: error },
          );
        }
        throw error;
      }
    } finally {
      clearTimeout(timeout);
    }
  }

  #requireBland(): BlandConfig {
    if (!this.#bland) {
      throw new FlorenceTelephonyError("unavailable", "Bland calling is not configured.");
    }
    return this.#bland;
  }

  #requireTwilio(): TwilioConfig {
    if (!this.#twilio) {
      throw new FlorenceTelephonyError("unavailable", "Twilio messaging and calling are not configured.");
    }
    return this.#twilio;
  }
}

class AmbiguousMutationError extends Error {
  readonly safeMessage: string;
  readonly providerId: string | null;

  constructor(safeMessage: string, options: { readonly providerId?: string; readonly cause?: unknown } = {}) {
    super("Telephony mutation result was ambiguous", { cause: options.cause });
    this.name = "AmbiguousMutationError";
    this.safeMessage = boundedString(safeMessage, MAX_ERROR_CHARS, "The provider result was uncertain.");
    this.providerId = options.providerId ?? null;
  }
}

function normalizeBlandCall(
  input: FlorenceTelephonyRunInput,
  providerId: string,
  payload: Readonly<Record<string, unknown>>,
): FlorenceTelephonyResult {
  const status = normalizedBlandStatus(payload);
  if (!status) throw invalidResponse("Bland returned a call without its status.");
  const summary = readRecordString(payload, "summary", MAX_SUMMARY_CHARS);
  const disposition = readRecordString(payload, "disposition", 1_000);
  const transcript = readRecordString(payload, "concatenated_transcript", MAX_TRANSCRIPT_CHARS);
  const terminal = isBlandTerminalStatus(status);
  const resultReady = Boolean(summary || disposition || transcript);
  const waitingForPostCallResult = status === "completed" && !resultReady;
  const kind = waitingForPostCallResult
    ? "progress"
    : status === "completed"
      ? "completed"
      : terminal
        ? "failed"
        : "progress";
  return result(input, {
    kind,
    providerId,
    providerStatus: status,
    reason: waitingForPostCallResult
      ? "The call has ended, but Bland is still preparing its summary, disposition, or transcript."
      : kind === "failed"
        ? providerFailureReason(payload, `Bland ended the call with status ${status}.`)
        : null,
    answeredBy: readRecordString(payload, "answered_by", 200),
    durationSeconds: minutesToSeconds(readRecordNumber(payload, "call_length")),
    summary,
    disposition,
    transcript,
    recordingUrl: readRecordUrl(payload, "recording_url"),
    toPhoneNumberMasked: maskOptionalPhone(readRecordString(payload, "to", 100)),
  });
}

function normalizeTwilioCall(
  input: FlorenceTelephonyRunInput,
  providerId: string,
  payload: Readonly<Record<string, unknown>>,
): FlorenceTelephonyResult {
  const status = normalizedStatus(payload);
  if (!status) throw invalidResponse("Twilio returned a call without its status.");
  const kind =
    status === "completed"
      ? "completed"
      : status && TWILIO_CALL_FAILED_STATUSES.has(status)
        ? "failed"
        : "progress";
  return result(input, {
    kind,
    providerId,
    providerStatus: status,
    reason:
      kind === "failed"
        ? providerFailureReason(payload, `Twilio ended the call with status ${status}.`)
        : null,
    answeredBy: readRecordString(payload, "answered_by", 200),
    durationSeconds: integerFromStringOrNumber(payload.duration),
    toPhoneNumberMasked: maskOptionalPhone(readRecordString(payload, "to", 100)),
  });
}

function normalizeTwilioSms(
  input: FlorenceTelephonyRunInput,
  providerId: string,
  payload: Readonly<Record<string, unknown>>,
): FlorenceTelephonyResult {
  const status = normalizedStatus(payload);
  if (!status) throw invalidResponse("Twilio returned a message without its status.");
  const kind = TWILIO_MESSAGE_COMPLETED_STATUSES.has(status)
    ? "completed"
    : TWILIO_MESSAGE_FAILED_STATUSES.has(status)
      ? "failed"
      : "progress";
  return result(input, {
    kind,
    providerId,
    providerStatus: status,
    reason: kind === "failed" ? providerFailureReason(payload, "Twilio did not deliver that message.") : null,
    toPhoneNumberMasked: maskOptionalPhone(readRecordString(payload, "to", 100)),
  });
}

function normalizeTwilioMessage(value: Readonly<Record<string, unknown>>): FlorenceTelephonyMessage {
  return {
    messageSid: requiredRecordString(value, "sid", 100),
    direction: readRecordString(value, "direction", 100),
    status: readRecordString(value, "status", 100),
    fromPhoneNumber: optionalNormalizedPhone(readRecordString(value, "from", 100)),
    toPhoneNumber: optionalNormalizedPhone(readRecordString(value, "to", 100)),
    sentAt: readRecordString(value, "date_sent", 100) ?? readRecordString(value, "date_created", 100),
    body: readRecordString(value, "body", MAX_MESSAGE_CHARS) ?? "",
    mediaCount: integerFromStringOrNumber(value.num_media) ?? 0,
  };
}

function result(
  input: FlorenceTelephonyRunInput,
  values: Partial<Omit<FlorenceTelephonyResult, "provider" | "operation">> & {
    readonly kind: FlorenceTelephonyResultKind;
  },
): FlorenceTelephonyResult {
  return {
    kind: values.kind,
    provider: input.operation.provider,
    operation: input.operation.kind,
    providerId: values.providerId ?? null,
    providerStatus: values.providerStatus ?? null,
    reason: values.reason ?? null,
    toPhoneNumberMasked: values.toPhoneNumberMasked ?? null,
    answeredBy: values.answeredBy ?? null,
    durationSeconds: values.durationSeconds ?? null,
    summary: values.summary ?? null,
    disposition: values.disposition ?? null,
    transcript: values.transcript ?? null,
    recordingUrl: values.recordingUrl ?? null,
    messages: values.messages ?? [],
  };
}

function uncertainResult(
  input: FlorenceTelephonyRunInput,
  reason: string,
  providerId: string | null = null,
): FlorenceTelephonyResult {
  return result(input, { kind: "uncertain_effect", providerId, reason });
}

function validateRunInput(input: FlorenceTelephonyRunInput): FlorenceTelephonyRunInput {
  if (!input || typeof input !== "object") throw invalidInput("The telephony request was invalid.");
  validCorrelationId(input.workId, "workId");
  validCorrelationId(input.callId, "callId");
  boundedInteger(input.attempt, 1, 1_000);
  if (!input.operation || typeof input.operation !== "object") {
    throw invalidInput("The telephony operation was invalid.");
  }
  if (!SUPPORTED_OPERATIONS.has(input.operation.kind)) {
    throw invalidInput("The telephony operation is not supported.");
  }
  const expectedProvider = input.operation.kind.startsWith("ai_call_") ? "bland" : "twilio";
  if (input.operation.provider !== expectedProvider) {
    throw invalidInput(`The ${input.operation.kind} operation requires ${expectedProvider}.`);
  }
  return input;
}

function validCorrelationId(value: string, name: string): string {
  const normalized = requireNonEmpty(value, name, 300);
  if (!/^[A-Za-z0-9._:-]+$/.test(normalized)) throw invalidInput(`${name} contains unsupported characters.`);
  return normalized;
}

function validProviderId(value: string, name: string): string {
  const normalized = requireNonEmpty(value, name, 300);
  if (!/^[A-Za-z0-9_-]+$/.test(normalized)) throw invalidInput(`${name} contains unsupported characters.`);
  return normalized;
}

function operationIdDigest(callId: string): string {
  return createHash("sha256").update(validCorrelationId(callId, "callId")).digest("hex");
}

function pendingBlandCallHandle(destination: string, operationDigest: string): string {
  const digest = requireNonEmpty(operationDigest, "Bland operation digest", 64);
  if (!/^[0-9a-f]{64}$/.test(digest)) throw invalidInput("The Bland operation digest is invalid.");
  return `${BLAND_PENDING_CALL_PREFIX}${digest}_${Buffer.from(normalizePhone(destination)).toString("base64url")}`;
}

function parsePendingBlandCallHandle(
  providerId: string,
): { readonly destination: string; readonly operationDigest: string } | null {
  if (!providerId.startsWith(BLAND_PENDING_CALL_PREFIX)) return null;
  const encoded = providerId.slice(BLAND_PENDING_CALL_PREFIX.length);
  const operationDigest = encoded.slice(0, 64);
  if (!/^[0-9a-f]{64}$/.test(operationDigest) || encoded[64] !== "_") {
    throw invalidInput("The pending Bland call handle is invalid.");
  }
  let destination: string;
  try {
    destination = Buffer.from(encoded.slice(65), "base64url").toString("utf8");
  } catch (error) {
    throw new FlorenceTelephonyError("invalid_input", "The pending Bland call handle is invalid.", {
      cause: error,
    });
  }
  return { destination: normalizePhone(destination), operationDigest };
}

function pendingTwilioSmsHandle(
  input: FlorenceTelephonyRunInput & {
    readonly operation: Extract<FlorenceTelephonyOperation, { kind: "sms_send" }>;
  },
  requestedAt: number,
): string {
  const destination = normalizePhone(input.operation.to);
  const fingerprint = twilioSmsFingerprint({
    destination,
    body: requireNonEmpty(input.operation.body, "SMS body", MAX_MESSAGE_CHARS),
    mediaCount: input.operation.mediaUrls?.length ?? 0,
  });
  return `${TWILIO_PENDING_SMS_PREFIX}${twilioPendingHandlePayload({
    requestedAt,
    operationDigest: operationIdDigest(input.callId),
    fingerprint,
    destination,
  })}`;
}

function pendingTwilioCallHandle(
  input: FlorenceTelephonyRunInput & {
    readonly operation: Extract<FlorenceTelephonyOperation, { kind: "call_start" }>;
  },
  requestedAt: number,
): string {
  return `${TWILIO_PENDING_CALL_PREFIX}${twilioPendingHandlePayload({
    requestedAt,
    operationDigest: operationIdDigest(input.callId),
    destination: normalizePhone(input.operation.to),
  })}`;
}

function twilioPendingHandlePayload(input: {
  readonly requestedAt: number;
  readonly operationDigest: string;
  readonly fingerprint?: string;
  readonly destination: string;
}): string {
  const requestedAt = Math.round(input.requestedAt);
  if (!Number.isSafeInteger(requestedAt) || requestedAt <= 0) {
    throw invalidInput("The Twilio request time is invalid.");
  }
  const operationDigest = validSha256(input.operationDigest, "Twilio operation digest");
  const fingerprint = input.fingerprint
    ? `${validSha256(input.fingerprint, "Twilio request fingerprint")}_`
    : "";
  return `${requestedAt.toString(36)}_${operationDigest}_${fingerprint}${Buffer.from(
    normalizePhone(input.destination),
  ).toString("base64url")}`;
}

function parsePendingTwilioSmsHandle(providerId: string): PendingTwilioSmsHandle | null {
  if (!providerId.startsWith(TWILIO_PENDING_SMS_PREFIX)) return null;
  const encoded = providerId.slice(TWILIO_PENDING_SMS_PREFIX.length);
  const pieces = encoded.split("_");
  if (pieces.length < 4) throw invalidInput("The pending Twilio message handle is invalid.");
  const [encodedTime, operationDigest, fingerprint, ...destinationParts] = pieces;
  validSha256(operationDigest ?? "", "Twilio operation digest");
  return {
    requestedAt: parseTwilioHandleTime(encodedTime ?? ""),
    fingerprint: validSha256(fingerprint ?? "", "Twilio request fingerprint"),
    destination: decodePendingTwilioDestination(destinationParts.join("_")),
  };
}

function parsePendingTwilioCallHandle(providerId: string): PendingTwilioCallHandle | null {
  if (!providerId.startsWith(TWILIO_PENDING_CALL_PREFIX)) return null;
  const encoded = providerId.slice(TWILIO_PENDING_CALL_PREFIX.length);
  const firstSeparator = encoded.indexOf("_");
  const digestStart = firstSeparator + 1;
  const destinationSeparator = digestStart + 64;
  if (
    firstSeparator <= 0 ||
    encoded[destinationSeparator] !== "_" ||
    !/^[0-9a-f]{64}$/.test(encoded.slice(digestStart, destinationSeparator))
  ) {
    throw invalidInput("The pending Twilio call handle is invalid.");
  }
  return {
    requestedAt: parseTwilioHandleTime(encoded.slice(0, firstSeparator)),
    destination: decodePendingTwilioDestination(encoded.slice(destinationSeparator + 1)),
  };
}

function parseTwilioHandleTime(value: string): number {
  if (!/^[0-9a-z]+$/.test(value)) throw invalidInput("The pending Twilio handle time is invalid.");
  const requestedAt = Number.parseInt(value, 36);
  if (!Number.isSafeInteger(requestedAt) || requestedAt <= 0) {
    throw invalidInput("The pending Twilio handle time is invalid.");
  }
  return requestedAt;
}

function decodePendingTwilioDestination(value: string): string {
  if (!value) throw invalidInput("The pending Twilio handle destination is invalid.");
  try {
    return normalizePhone(Buffer.from(value, "base64url").toString("utf8"));
  } catch (error) {
    throw new FlorenceTelephonyError("invalid_input", "The pending Twilio handle destination is invalid.", {
      cause: error,
    });
  }
}

function validSha256(value: string, name: string): string {
  if (!/^[0-9a-f]{64}$/.test(value)) throw invalidInput(`${name} is invalid.`);
  return value;
}

function twilioSmsFingerprint(input: {
  readonly destination: string;
  readonly body: string;
  readonly mediaCount: number;
}): string {
  return createHash("sha256")
    .update(
      JSON.stringify({
        to: normalizePhone(input.destination),
        body: input.body,
        mediaCount: boundedInteger(input.mediaCount, 0, 10),
      }),
    )
    .digest("hex");
}

function twilioReconciliationQuery(input: {
  readonly destination: string;
  readonly source: string;
  readonly dateField: "DateSent>" | "StartTime>";
  readonly requestedAt: number;
}): URLSearchParams {
  const earliest = new Date(input.requestedAt - TWILIO_RECONCILIATION_LOOKBACK_MS);
  return new URLSearchParams({
    To: normalizePhone(input.destination),
    From: normalizePhone(input.source),
    [input.dateField]: earliest.toISOString().slice(0, 10),
    PageSize: String(TWILIO_RECONCILIATION_CANDIDATE_LIMIT),
  });
}

function isMatchingTwilioSms(
  candidate: Readonly<Record<string, unknown>>,
  pending: PendingTwilioSmsHandle,
  source: string,
): boolean {
  const direction = normalizedStatusValue(readRecordString(candidate, "direction", 100));
  const body = readRecordString(candidate, "body", MAX_MESSAGE_CHARS);
  const mediaCount = integerFromStringOrNumber(candidate.num_media);
  if (
    direction !== "outbound-api" ||
    body === null ||
    mediaCount === null ||
    !sameNormalizedPhone(readRecordString(candidate, "to", 100), pending.destination) ||
    !sameNormalizedPhone(readRecordString(candidate, "from", 100), source) ||
    !isWithinTwilioReconciliationWindow(candidate, pending.requestedAt)
  ) {
    return false;
  }
  return twilioSmsFingerprint({ destination: pending.destination, body, mediaCount }) === pending.fingerprint;
}

function sameNormalizedPhone(candidate: string | null, expected: string): boolean {
  if (!candidate) return false;
  try {
    return normalizePhone(candidate) === normalizePhone(expected);
  } catch {
    return false;
  }
}

function isWithinTwilioReconciliationWindow(
  candidate: Readonly<Record<string, unknown>>,
  requestedAt: number,
): boolean {
  const instantValue =
    readRecordString(candidate, "date_created", 100) ??
    readRecordString(candidate, "start_time", 100) ??
    readRecordString(candidate, "date_sent", 100);
  const occurredAt = instantValue ? Date.parse(instantValue) : Number.NaN;
  return (
    Number.isFinite(occurredAt) &&
    occurredAt >= requestedAt - TWILIO_RECONCILIATION_LOOKBACK_MS &&
    occurredAt <= requestedAt + TWILIO_RECONCILIATION_FUTURE_MARGIN_MS
  );
}

function validTwilioAccountSid(value: string): string {
  const normalized = requireNonEmpty(value, "Twilio account SID", 100);
  if (!/^AC[0-9a-fA-F]{32}$/.test(normalized)) throw invalidInput("The Twilio account SID is invalid.");
  return normalized;
}

function normalizePhone(value: string): string {
  const normalized = requireNonEmpty(value, "Phone number", 50).replace(/[\s().-]/g, "");
  if (!/^\+[1-9]\d{7,14}$/.test(normalized)) {
    throw invalidInput("Use an E.164 phone number such as +14155550123.");
  }
  return normalized;
}

function optionalNormalizedPhone(value: string | null): string | null {
  if (!value) return null;
  try {
    return normalizePhone(value);
  } catch {
    return boundedString(value, 100, "") || null;
  }
}

function maskPhone(value: string): string {
  const normalized = normalizePhone(value);
  return normalized.length <= 4
    ? normalized
    : `${"•".repeat(Math.min(8, normalized.length - 4))}${normalized.slice(-4)}`;
}

function maskOptionalPhone(value: string | null): string | null {
  if (!value) return null;
  try {
    return maskPhone(value);
  } catch {
    return null;
  }
}

function normalizeBaseUrl(value: string): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch (error) {
    throw new FlorenceTelephonyError("invalid_input", "The telephony provider URL is invalid.", {
      cause: error,
    });
  }
  if (url.protocol !== "https:" && url.hostname !== "localhost" && url.hostname !== "127.0.0.1") {
    throw invalidInput("Telephony provider URLs must use HTTPS.");
  }
  url.search = "";
  url.hash = "";
  return url.toString().replace(/\/$/, "");
}

function validPublicHttpUrl(value: string): string {
  const normalized = requireNonEmpty(value, "Media URL", 8_192);
  let url: URL;
  try {
    url = new URL(normalized);
  } catch (error) {
    throw new FlorenceTelephonyError("invalid_input", "The SMS media URL is invalid.", { cause: error });
  }
  if (url.protocol !== "https:" && url.protocol !== "http:") {
    throw invalidInput("SMS media URLs must use HTTP or HTTPS.");
  }
  return url.toString();
}

function normalizedStatus(payload: Readonly<Record<string, unknown>>): string | null {
  const value = readRecordString(payload, "status", 100) ?? readRecordString(payload, "queue_status", 100);
  return normalizedStatusValue(value);
}

function normalizedBlandStatus(payload: Readonly<Record<string, unknown>>): string | null {
  const callStatus = normalizedStatusValue(readRecordString(payload, "status", 100));
  if (callStatus && BLAND_TERMINAL_STATUSES.has(callStatus)) return callStatus;

  const queueStatus = normalizedStatusValue(readRecordString(payload, "queue_status", 100));
  if (queueStatus && queueStatus !== "complete-error" && BLAND_QUEUE_FAILURE_STATUSES.has(queueStatus)) {
    return queueStatus;
  }
  if (payload.completed === true) return "completed";
  if (queueStatus && BLAND_QUEUE_FAILURE_STATUSES.has(queueStatus)) return queueStatus;
  if (payload.completed !== false && (queueStatus === "complete" || queueStatus === "completed")) {
    return "completed";
  }
  return callStatus ?? queueStatus;
}

function normalizedStatusValue(value: string | null): string | null {
  return value?.trim().toLowerCase().replaceAll("_", "-") ?? null;
}

function isBlandTerminalStatus(status: string): boolean {
  return BLAND_TERMINAL_STATUSES.has(status) || BLAND_QUEUE_FAILURE_STATUSES.has(status);
}

function isTwilioCallTerminalStatus(status: string): boolean {
  return status === "completed" || TWILIO_CALL_FAILED_STATUSES.has(status);
}

function hasBlandCorrelationMetadata(
  payload: Readonly<Record<string, unknown>>,
  workId: string,
  callId: string,
): boolean {
  const metadata = payload.metadata;
  return (
    isRecord(metadata) && metadata.florence_work_id === workId && metadata.florence_operation_id === callId
  );
}

function providerFailureReason(payload: Readonly<Record<string, unknown>>, fallback: string): string {
  return (
    readRecordString(payload, "error_message", MAX_ERROR_CHARS) ??
    readRecordString(payload, "message", MAX_ERROR_CHARS) ??
    fallback
  );
}

function add(target: Record<string, unknown>, key: string, value: unknown): void {
  if (value !== undefined) target[key] = value;
}

function escapeXml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function minutesToSeconds(value: number | null): number | null {
  return value === null ? null : Math.max(0, Math.round(value * 60));
}

function integerFromStringOrNumber(value: unknown): number | null {
  const number = typeof value === "number" ? value : typeof value === "string" ? Number(value) : Number.NaN;
  return Number.isFinite(number) && number >= 0 ? Math.round(number) : null;
}

function readRecordNumber(value: Readonly<Record<string, unknown>>, key: string): number | null {
  const candidate = value[key];
  return typeof candidate === "number" && Number.isFinite(candidate) ? candidate : null;
}

function readRecordString(
  value: Readonly<Record<string, unknown>>,
  key: string,
  maximumChars: number,
): string | null {
  const candidate = value[key];
  return typeof candidate === "string" ? boundedString(candidate, maximumChars, "") || null : null;
}

function requiredRecordString(
  value: Readonly<Record<string, unknown>>,
  key: string,
  maximumChars: number,
  ambiguousIfMissing = false,
): string {
  const candidate = readRecordString(value, key, maximumChars);
  if (candidate) return candidate;
  if (ambiguousIfMissing) {
    throw new AmbiguousMutationError(
      "The provider accepted the request but did not return its resource ID. Florence did not repeat it.",
    );
  }
  throw invalidResponse("The phone provider returned an incomplete response.");
}

function readRecordArray(
  value: Readonly<Record<string, unknown>>,
  key: string,
  maximumItems: number,
): readonly Readonly<Record<string, unknown>>[] {
  const candidate = value[key];
  if (!Array.isArray(candidate))
    throw invalidResponse("The phone provider returned an incomplete message list.");
  return candidate.slice(0, maximumItems).filter(isRecord);
}

function readRecordUrl(value: Readonly<Record<string, unknown>>, key: string): string | null {
  const candidate = readRecordString(value, key, 8_192);
  if (!candidate) return null;
  try {
    const url = new URL(candidate);
    return url.protocol === "https:" || url.protocol === "http:" ? url.toString() : null;
  } catch {
    return null;
  }
}

function requireNonEmpty(value: string, name: string, maximumChars: number): string {
  if (typeof value !== "string") throw invalidInput(`${name} is required.`);
  const normalized = value.trim();
  if (!normalized || normalized.length > maximumChars) {
    throw invalidInput(`${name} must contain between 1 and ${maximumChars} characters.`);
  }
  return normalized;
}

function optionalNonEmpty(value: string | undefined, name: string, maximumChars: number): string | undefined {
  return value === undefined ? undefined : requireNonEmpty(value, name, maximumChars);
}

function boundedInteger(value: number, minimum: number, maximum: number): number {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw invalidInput(`Expected an integer between ${minimum} and ${maximum}.`);
  }
  return value;
}

function boundedString(value: string, maximumChars: number, fallback: string): string {
  const normalized = value.trim();
  return normalized ? normalized.slice(0, maximumChars) : fallback;
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function invalidInput(message: string): FlorenceTelephonyError {
  return new FlorenceTelephonyError("invalid_input", message);
}

function invalidResponse(message: string, cause?: unknown): FlorenceTelephonyError {
  return new FlorenceTelephonyError("invalid_response", message, { retryable: true, cause });
}

function asTelephonyError(error: unknown, signal?: AbortSignal): FlorenceTelephonyError {
  if (error instanceof FlorenceTelephonyError) return error;
  if (signal?.aborted) {
    return new FlorenceTelephonyError("cancelled", "Phone work was cancelled.", { cause: error });
  }
  return new FlorenceTelephonyError("unavailable", "The phone provider is temporarily unavailable.", {
    retryable: true,
    cause: error,
  });
}

function throwIfCancelled(signal?: AbortSignal): void {
  if (signal?.aborted) throw new FlorenceTelephonyError("cancelled", "Phone work was cancelled.");
}

function isAmbiguousHttpStatus(status: number): boolean {
  return status === 408 || status === 425 || status === 429 || status >= 500;
}

async function readProviderError(response: Response): Promise<string | null> {
  try {
    const value = await readBoundedJson(response);
    if (!isRecord(value)) return null;
    return (
      readRecordString(value, "message", MAX_ERROR_CHARS) ??
      readRecordString(value, "error", MAX_ERROR_CHARS) ??
      readRecordString(value, "detail", MAX_ERROR_CHARS)
    );
  } catch {
    return null;
  }
}

async function readBoundedJson(response: Response): Promise<unknown> {
  const contentLength = Number(response.headers.get("content-length"));
  if (Number.isFinite(contentLength) && contentLength > MAX_PROVIDER_RESPONSE_BYTES) {
    throw invalidResponse("The phone provider returned too much data.");
  }
  const text = await response.text();
  if (Buffer.byteLength(text, "utf8") > MAX_PROVIDER_RESPONSE_BYTES) {
    throw invalidResponse("The phone provider returned too much data.");
  }
  try {
    return JSON.parse(text);
  } catch (error) {
    throw invalidResponse("The phone provider returned unreadable data.", error);
  }
}
