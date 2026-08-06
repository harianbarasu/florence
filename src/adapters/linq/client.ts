import { createHash } from "node:crypto";
import { z } from "zod";
import type { LinqConfig } from "./config.js";
import type {
  LinqChatHealth,
  LinqChatSnapshot,
  LinqDownloadedAttachment,
  LinqMessageDeliveryReceipt,
  LinqMessagingService,
  LinqParticipant,
  LinqSendMessageRequest,
  LinqSendReceipt,
} from "./contracts.js";
import { LinqApiError, LinqAttachmentError, LinqAudienceChangedError } from "./errors.js";
import {
  providerAttachmentMetadataSchema,
  providerChatSchema,
  providerErrorResponseSchema,
  providerMessageDeliverySchema,
  providerSendResponseSchema,
} from "./schemas.js";

const providerIdSchema = z.string().uuid();

const sendRequestSchema = z
  .object({
    providerChatId: providerIdSchema,
    expectedParticipantDigest: z.string().regex(/^linq-v1:[a-f0-9]{64}$/),
    idempotencyKey: z.string().min(1).max(255),
    text: z
      .string()
      .max(10_000)
      .refine((value) => value.trim().length > 0)
      .optional(),
    providerAttachmentIds: z.array(providerIdSchema).max(99).optional(),
    replyTo: z
      .object({
        providerMessageId: providerIdSchema,
        partIndex: z.number().int().nonnegative(),
      })
      .optional(),
  })
  .refine(
    (value) => Boolean(value.text) || Boolean(value.providerAttachmentIds?.length),
    "A Linq message needs text or an attachment",
  );

interface LinqClientOptions {
  fetch?: typeof fetch;
  now?: () => Date;
}

interface FetchAttachmentOptions {
  maxBytes?: number;
  signal?: AbortSignal;
}

function normalizeTimestamp(value: string, field: string): string {
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) {
    throw new LinqApiError(`Linq returned an invalid ${field} timestamp`, {
      status: 502,
      providerCode: "invalid_provider_response",
      retryable: true,
    });
  }
  return new Date(parsed).toISOString();
}

function normalizeService(service: "iMessage" | "SMS" | "RCS"): LinqMessagingService {
  if (service === "iMessage") {
    return "imessage";
  }
  return service.toLowerCase() as "sms" | "rcs";
}

function normalizeParticipant(input: {
  id: string;
  handle: string;
  service: "iMessage" | "SMS" | "RCS";
  is_me?: boolean | null | undefined;
  status?: "active" | "left" | "removed" | null | undefined;
  joined_at: string;
  left_at?: string | null | undefined;
}): LinqParticipant {
  const leftAt = input.left_at ? normalizeTimestamp(input.left_at, "participant left_at") : undefined;
  return {
    providerParticipantId: input.id,
    address: input.handle,
    service: normalizeService(input.service),
    isSelf: input.is_me ?? false,
    status: input.status ?? (leftAt ? "left" : "active"),
    joinedAt: normalizeTimestamp(input.joined_at, "participant joined_at"),
    ...(leftAt ? { leftAt } : {}),
  };
}

function normalizeHealth(status: "HEALTHY" | "AT_RISK" | "CRITICAL" | "OPTED_OUT"): LinqChatHealth {
  return status.toLowerCase() as LinqChatHealth;
}

export function computeLinqParticipantDigest(participants: readonly LinqParticipant[]): string {
  const exactActiveAudience = participants
    .filter((participant) => participant.status === "active")
    .map((participant) =>
      [
        participant.providerParticipantId,
        participant.address,
        participant.service,
        participant.isSelf ? "self" : "other",
        participant.joinedAt,
      ].join("\u001f"),
    )
    .sort()
    .join("\u001e");
  return `linq-v1:${createHash("sha256").update(exactActiveAudience, "utf8").digest("hex")}`;
}

async function readBoundedBody(response: Response, maxBytes: number): Promise<Uint8Array> {
  const declaredLength = response.headers.get("content-length");
  if (declaredLength && Number(declaredLength) > maxBytes) {
    throw new LinqAttachmentError("too_large", "The Linq response exceeds the configured byte limit");
  }
  if (!response.body) {
    return new Uint8Array();
  }

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) {
        break;
      }
      total += next.value.byteLength;
      if (total > maxBytes) {
        await reader.cancel();
        throw new LinqAttachmentError("too_large", "The Linq response exceeds the configured byte limit");
      }
      chunks.push(next.value);
    }
  } finally {
    reader.releaseLock();
  }

  const combined = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    combined.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return combined;
}

function combineSignals(signal: AbortSignal | undefined, timeoutMs: number): AbortSignal {
  const timeout = AbortSignal.timeout(timeoutMs);
  return signal ? AbortSignal.any([signal, timeout]) : timeout;
}

export class LinqClient {
  readonly #config: LinqConfig;
  readonly #fetch: typeof fetch;
  readonly #now: () => Date;

  constructor(config: LinqConfig, options: LinqClientOptions = {}) {
    this.#config = config;
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#now = options.now ?? (() => new Date());
  }

  async getChat(providerChatId: string, signal?: AbortSignal): Promise<LinqChatSnapshot> {
    const id = providerIdSchema.parse(providerChatId);
    const raw = await this.#requestJson(`/chats/${encodeURIComponent(id)}`, { method: "GET", signal });
    const parsed = providerChatSchema.safeParse(raw);
    if (!parsed.success) {
      throw new LinqApiError("Linq returned an invalid chat", {
        status: 502,
        providerCode: "invalid_provider_response",
        retryable: true,
      });
    }
    if (parsed.data.id !== id) {
      throw new LinqApiError("Linq returned a different chat than the one requested", {
        status: 502,
        providerCode: "chat_id_mismatch",
        retryable: true,
      });
    }

    const participants = parsed.data.handles.map(normalizeParticipant);
    const ownsChat = participants.some(
      (participant) =>
        participant.isSelf &&
        participant.status === "active" &&
        participant.address === this.#config.phoneNumber,
    );
    if (!ownsChat) {
      throw new LinqApiError("The configured Florence line is not an active participant in this chat", {
        status: 403,
        providerCode: "configured_line_not_participant",
        retryable: false,
      });
    }

    return {
      providerChatId: parsed.data.id,
      kind: parsed.data.is_group ? "group" : "direct",
      ...(parsed.data.display_name ? { displayName: parsed.data.display_name } : {}),
      ...(parsed.data.service ? { service: normalizeService(parsed.data.service) } : {}),
      health: normalizeHealth(parsed.data.health_status.status),
      participants,
      activeParticipantDigest: computeLinqParticipantDigest(participants),
      createdAt: normalizeTimestamp(parsed.data.created_at, "chat created_at"),
      updatedAt: normalizeTimestamp(parsed.data.updated_at, "chat updated_at"),
      checkedAt: this.#now().toISOString(),
    };
  }

  async sendMessage(
    request: LinqSendMessageRequest,
    signal?: AbortSignal,
    beforeSubmit?: () => Promise<void>,
  ): Promise<LinqSendReceipt> {
    const parsed = sendRequestSchema.safeParse(request);
    if (!parsed.success) {
      throw new LinqApiError("The Linq send request is invalid", {
        status: 400,
        providerCode: "invalid_send_request",
        retryable: false,
      });
    }

    const currentChat = await this.getChat(parsed.data.providerChatId, signal);
    if (currentChat.activeParticipantDigest !== parsed.data.expectedParticipantDigest) {
      throw new LinqAudienceChangedError(
        parsed.data.expectedParticipantDigest,
        currentChat.activeParticipantDigest,
      );
    }

    const parts: Array<Record<string, string>> = [];
    if (parsed.data.text) {
      parts.push({ type: "text", value: parsed.data.text });
    }
    for (const providerAttachmentId of parsed.data.providerAttachmentIds ?? []) {
      parts.push({ type: "media", attachment_id: providerAttachmentId });
    }

    const body = {
      message: {
        parts,
        idempotency_key: parsed.data.idempotencyKey,
        ...(parsed.data.replyTo
          ? {
              reply_to: {
                message_id: parsed.data.replyTo.providerMessageId,
                part_index: parsed.data.replyTo.partIndex,
              },
            }
          : {}),
      },
    };
    await beforeSubmit?.();
    const raw = await this.#requestJson(`/chats/${encodeURIComponent(parsed.data.providerChatId)}/messages`, {
      method: "POST",
      body: JSON.stringify(body),
      signal,
    });
    const response = providerSendResponseSchema.safeParse(raw);
    if (!response.success || response.data.chat_id !== parsed.data.providerChatId) {
      throw new LinqApiError("Linq returned an invalid send receipt", {
        status: 502,
        providerCode: "invalid_provider_response",
        retryable: true,
      });
    }

    return {
      providerChatId: response.data.chat_id,
      providerMessageId: response.data.message.id,
      idempotencyKey: parsed.data.idempotencyKey,
      status: "accepted",
      providerDeliveryStatus: response.data.message.delivery_status,
      submittedAt: normalizeTimestamp(response.data.message.created_at, "message created_at"),
      audienceCheckedAt: currentChat.checkedAt,
      participantDigest: currentChat.activeParticipantDigest,
    };
  }

  async getMessageDelivery(
    providerMessageId: string,
    signal?: AbortSignal,
  ): Promise<LinqMessageDeliveryReceipt> {
    const id = providerIdSchema.parse(providerMessageId);
    const raw = await this.#requestJson(`/messages/${encodeURIComponent(id)}`, { method: "GET", signal });
    const response = providerMessageDeliverySchema.safeParse(raw);
    if (!response.success || response.data.id !== id) {
      throw new LinqApiError("Linq returned an invalid message delivery receipt", {
        status: 502,
        providerCode: "invalid_provider_response",
        retryable: true,
      });
    }
    return {
      providerChatId: response.data.chat_id,
      providerMessageId: response.data.id,
      providerDeliveryStatus: response.data.delivery_status,
      createdAt: normalizeTimestamp(response.data.created_at, "message created_at"),
      updatedAt: normalizeTimestamp(response.data.updated_at, "message updated_at"),
    };
  }

  async fetchAttachment(
    providerAttachmentId: string,
    options: FetchAttachmentOptions = {},
  ): Promise<LinqDownloadedAttachment> {
    const parsedId = providerIdSchema.safeParse(providerAttachmentId);
    if (!parsedId.success) {
      throw new LinqAttachmentError("invalid_reference", "The Linq attachment ID is invalid");
    }

    const requestedMax = options.maxBytes ?? this.#config.maxAttachmentBytes;
    if (!Number.isSafeInteger(requestedMax) || requestedMax < 1) {
      throw new LinqAttachmentError("invalid_reference", "The Linq attachment byte limit is invalid");
    }
    const maxBytes = Math.min(requestedMax, this.#config.maxAttachmentBytes);

    const raw = await this.#requestJson(`/attachments/${encodeURIComponent(parsedId.data)}`, {
      method: "GET",
      signal: options.signal,
    });
    const metadata = providerAttachmentMetadataSchema.safeParse(raw);
    if (!metadata.success || metadata.data.id !== parsedId.data) {
      throw new LinqAttachmentError("invalid_reference", "Linq returned invalid attachment metadata");
    }
    if (metadata.data.size_bytes > maxBytes) {
      throw new LinqAttachmentError("too_large", "The Linq attachment exceeds the configured byte limit");
    }
    if (!metadata.data.download_url) {
      throw new LinqAttachmentError("missing_download_url", "The Linq attachment has no download URL");
    }

    const downloadUrl = new URL(metadata.data.download_url);
    if (
      downloadUrl.protocol !== "https:" ||
      downloadUrl.hostname.toLowerCase() !== "cdn.linqapp.com" ||
      downloadUrl.port ||
      downloadUrl.username ||
      downloadUrl.password
    ) {
      throw new LinqAttachmentError(
        "download_url_not_allowed",
        "The Linq attachment URL is outside the allowlisted CDN",
      );
    }

    let response: Response;
    try {
      response = await this.#fetch(downloadUrl, {
        method: "GET",
        redirect: "error",
        signal: combineSignals(options.signal, this.#config.requestTimeoutMs),
      });
    } catch (cause) {
      throw new LinqAttachmentError("download_failed", "The Linq attachment download failed", {
        cause,
      });
    }
    if (!response.ok) {
      throw new LinqAttachmentError(
        "download_failed",
        `The Linq attachment download failed with HTTP ${response.status}`,
      );
    }

    const bytes = await readBoundedBody(response, maxBytes);
    if (bytes.byteLength !== metadata.data.size_bytes) {
      throw new LinqAttachmentError(
        "integrity_mismatch",
        "The Linq attachment size did not match its metadata",
      );
    }
    const responseMediaType = response.headers.get("content-type")?.split(";", 1)[0]?.trim();
    return {
      providerAttachmentId: metadata.data.id,
      filename: metadata.data.filename,
      declaredMediaType: metadata.data.content_type,
      ...(responseMediaType ? { responseMediaType } : {}),
      sizeBytes: bytes.byteLength,
      sha256: createHash("sha256").update(bytes).digest("hex"),
      bytes,
      downloadedAt: this.#now().toISOString(),
    };
  }

  async #requestJson(
    path: string,
    init: {
      method: "GET" | "POST";
      body?: string;
      signal?: AbortSignal | undefined;
    },
  ): Promise<unknown> {
    const headers = new Headers({
      Accept: "application/json",
      Authorization: `Bearer ${this.#config.apiKey}`,
    });
    if (init.body) {
      headers.set("Content-Type", "application/json");
    }

    let response: Response;
    try {
      response = await this.#fetch(`${this.#config.baseUrl}${path}`, {
        method: init.method,
        headers,
        ...(init.body ? { body: init.body } : {}),
        signal: combineSignals(init.signal, this.#config.requestTimeoutMs),
        redirect: "error",
      });
    } catch (cause) {
      throw new LinqApiError(
        "The Linq request failed before a response was received",
        { status: 0, providerCode: "network_error", retryable: true },
        { cause },
      );
    }

    let decoded: unknown = null;
    try {
      const bytes = await readBoundedBody(response, 2 * 1024 * 1024);
      if (bytes.byteLength > 0) {
        decoded = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
      }
    } catch (cause) {
      if (cause instanceof LinqAttachmentError && cause.code === "too_large") {
        throw new LinqApiError(
          "The Linq response exceeded the safe JSON limit",
          { status: 502, providerCode: "response_too_large", retryable: true },
          { cause },
        );
      }
      throw new LinqApiError(
        "Linq returned a malformed JSON response",
        { status: 502, providerCode: "invalid_provider_response", retryable: true },
        { cause },
      );
    }

    if (!response.ok) {
      const providerError = providerErrorResponseSchema.safeParse(decoded);
      const providerCode = providerError.success ? (providerError.data.error?.code ?? null) : null;
      const providerMessage = providerError.success ? providerError.data.error?.message : undefined;
      throw new LinqApiError(providerMessage ?? `Linq returned HTTP ${response.status}`, {
        status: response.status,
        providerCode,
        retryable: response.status === 408 || response.status === 429 || response.status >= 500,
      });
    }

    return decoded;
  }
}
