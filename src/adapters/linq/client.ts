import { z } from "zod";
import type { LinqConfig } from "./config.js";

export const linqSendTextInputSchema = z
  .object({
    chatId: z.string().min(1),
    text: z.string().min(1).max(10_000),
    idempotencyKey: z.string().min(1).max(255),
    replyTo: z
      .object({
        messageId: z.string().min(1),
        partIndex: z.number().int().nonnegative().optional(),
      })
      .strict()
      .optional(),
  })
  .strict();

export type LinqSendTextInput = z.infer<typeof linqSendTextInputSchema>;

export interface LinqSendReceipt {
  provider: "linq";
  providerMessageId: string;
  chatId: string;
  idempotencyKey: string;
}

export const linqChatSchema = z
  .object({
    id: z.string().min(1),
    isGroup: z.boolean(),
    displayName: z.string().nullable(),
    service: z.string().nullable(),
    healthStatus: z.enum(["HEALTHY", "AT_RISK", "CRITICAL", "OPTED_OUT"]),
    activeHandles: z.array(z.string().min(1)).min(2),
    selfHandles: z.array(z.string().min(1)).min(1),
    participantHandles: z.array(z.string().min(1)).min(1),
  })
  .strict()
  .superRefine((chat, context) => {
    const active = new Set(chat.activeHandles);
    for (const handle of [...chat.selfHandles, ...chat.participantHandles]) {
      if (!active.has(handle)) {
        context.addIssue({ code: "custom", message: "A classified chat handle must be active" });
      }
    }
    if (new Set(chat.activeHandles).size !== chat.activeHandles.length) {
      context.addIssue({ code: "custom", message: "Chat handles must be unique" });
    }
  });

export type LinqChat = z.infer<typeof linqChatSchema>;

export interface LinqOutboundSender {
  sendText(input: LinqSendTextInput): Promise<LinqSendReceipt>;
}

export interface LinqChatReader {
  getChat(chatId: string): Promise<LinqChat>;
}

export interface LinqRetrievedAttachment {
  providerAttachmentId: string;
  kind: "image" | "file";
  mediaType: string;
  filename: string;
  sizeBytes: number;
  bytes: Uint8Array;
}

export type LinqAttachmentContentFailure =
  | "missing_reference"
  | "too_large"
  | "unsupported_type"
  | "not_found";

export interface LinqAttachmentReader {
  retrieveAttachment(attachmentId: string, maxBytes?: number): Promise<LinqRetrievedAttachment>;
}

export interface LinqFetchResponse {
  readonly ok: boolean;
  readonly status: number;
  readonly headers?: Pick<Headers, "get">;
  readonly body?: ReadableStream<Uint8Array> | null;
  json(): Promise<unknown>;
  arrayBuffer?(): Promise<ArrayBuffer>;
}

export type LinqFetch = (input: string | URL, init?: RequestInit) => Promise<LinqFetchResponse>;

export class LinqApiError extends Error {
  override readonly name: string = "LinqApiError";

  constructor(
    message: string,
    readonly status: number | null,
    readonly retryable: boolean,
  ) {
    super(message);
  }
}

export class LinqAttachmentContentError extends LinqApiError {
  override readonly name = "LinqAttachmentContentError";

  constructor(readonly reason: LinqAttachmentContentFailure) {
    super(`Linq attachment content is unavailable (${reason})`, reason === "too_large" ? 413 : 422, false);
  }
}

const MAX_MODEL_ATTACHMENT_BYTES = 10 * 1024 * 1024;
const MODEL_IMAGE_TYPES = new Set(["image/jpeg", "image/png", "image/gif", "image/webp"]);
const MODEL_FILE_TYPES = new Set([
  "application/json",
  "application/msword",
  "application/pdf",
  "application/rtf",
  "application/vnd.ms-excel",
  "application/vnd.ms-powerpoint",
  "application/vnd.openxmlformats-officedocument.presentationml.presentation",
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  "text/calendar",
  "text/csv",
  "text/html",
  "text/markdown",
  "text/plain",
  "text/rtf",
  "text/vcard",
  "text/xml",
]);

const attachmentMetadataSchema = z
  .object({
    id: z.string().min(1),
    content_type: z.string().min(1),
    filename: z.string().min(1),
    size_bytes: z.number().int().nonnegative(),
    status: z.enum(["pending", "complete", "failed"]).optional(),
    download_url: z.url().nullable().optional(),
    created_at: z.iso.datetime(),
  })
  .strict();

export class LinqClient implements LinqOutboundSender, LinqChatReader, LinqAttachmentReader {
  readonly #fetch: LinqFetch;

  constructor(
    private readonly config: Pick<LinqConfig, "apiBaseUrl" | "apiKey" | "requestTimeoutMs">,
    fetchImplementation: LinqFetch = globalThis.fetch,
  ) {
    this.#fetch = fetchImplementation;
  }

  async sendText(rawInput: LinqSendTextInput): Promise<LinqSendReceipt> {
    const input = linqSendTextInputSchema.parse(rawInput);
    const replyTo = input.replyTo
      ? {
          message_id: input.replyTo.messageId,
          ...(input.replyTo.partIndex === undefined ? {} : { part_index: input.replyTo.partIndex }),
        }
      : undefined;
    const body = {
      message: {
        parts: [{ type: "text", value: input.text }],
        idempotency_key: input.idempotencyKey,
        ...(replyTo ? { reply_to: replyTo } : {}),
      },
    };

    let response: Awaited<ReturnType<LinqFetch>>;
    try {
      response = await this.#fetch(
        `${this.config.apiBaseUrl}/chats/${encodeURIComponent(input.chatId)}/messages`,
        {
          method: "POST",
          headers: {
            authorization: `Bearer ${this.config.apiKey}`,
            "content-type": "application/json",
          },
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(this.config.requestTimeoutMs),
        },
      );
    } catch (error) {
      throw new LinqApiError(
        error instanceof Error && error.name === "TimeoutError"
          ? "Linq request timed out"
          : "Linq request failed before a response was received",
        null,
        true,
      );
    }

    if (!response.ok) {
      throw new LinqApiError(
        `Linq rejected the send with HTTP ${response.status}`,
        response.status,
        response.status === 408 || response.status === 429 || response.status >= 500,
      );
    }

    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      throw new LinqApiError("Linq returned an invalid send response", response.status, false);
    }
    const providerMessageId = extractMessageId(payload);
    if (providerMessageId === null) {
      throw new LinqApiError("Linq send response did not include a message ID", response.status, false);
    }

    return {
      provider: "linq",
      providerMessageId,
      chatId: input.chatId,
      idempotencyKey: input.idempotencyKey,
    };
  }

  async getChat(chatId: string): Promise<LinqChat> {
    const parsedChatId = z.string().min(1).max(500).parse(chatId);
    let response: Awaited<ReturnType<LinqFetch>>;
    try {
      response = await this.#fetch(`${this.config.apiBaseUrl}/chats/${encodeURIComponent(parsedChatId)}`, {
        method: "GET",
        headers: { authorization: `Bearer ${this.config.apiKey}` },
        signal: AbortSignal.timeout(this.config.requestTimeoutMs),
      });
    } catch (error) {
      throw new LinqApiError(
        error instanceof Error && error.name === "TimeoutError"
          ? "Linq chat lookup timed out"
          : "Linq chat lookup failed before a response was received",
        null,
        true,
      );
    }

    if (!response.ok) {
      throw new LinqApiError(
        `Linq rejected the chat lookup with HTTP ${response.status}`,
        response.status,
        response.status === 408 || response.status === 429 || response.status >= 500,
      );
    }

    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      throw new LinqApiError("Linq returned an invalid chat response", response.status, false);
    }
    return normalizeChat(payload, response.status);
  }

  async retrieveAttachment(
    attachmentId: string,
    requestedMaxBytes = MAX_MODEL_ATTACHMENT_BYTES,
  ): Promise<LinqRetrievedAttachment> {
    const parsedAttachmentId = z.string().trim().min(1).max(500).parse(attachmentId);
    const maxBytes = z.number().int().positive().max(MAX_MODEL_ATTACHMENT_BYTES).parse(requestedMaxBytes);
    let metadataResponse: Awaited<ReturnType<LinqFetch>>;
    try {
      metadataResponse = await this.#fetch(
        `${this.config.apiBaseUrl}/attachments/${encodeURIComponent(parsedAttachmentId)}`,
        {
          method: "GET",
          headers: { authorization: `Bearer ${this.config.apiKey}` },
          redirect: "error",
          signal: AbortSignal.timeout(this.config.requestTimeoutMs),
        },
      );
    } catch (error) {
      throw new LinqApiError(
        error instanceof Error && error.name === "TimeoutError"
          ? "Linq attachment metadata request timed out"
          : "Linq attachment metadata request failed before a response was received",
        null,
        true,
      );
    }

    if (!metadataResponse.ok) {
      if (metadataResponse.status === 404) {
        throw new LinqAttachmentContentError("not_found");
      }
      throw new LinqApiError(
        `Linq rejected the attachment lookup with HTTP ${metadataResponse.status}`,
        metadataResponse.status,
        metadataResponse.status === 408 || metadataResponse.status === 429 || metadataResponse.status >= 500,
      );
    }

    let rawMetadata: unknown;
    try {
      rawMetadata = await metadataResponse.json();
    } catch {
      throw new LinqApiError("Linq returned invalid attachment metadata", metadataResponse.status, false);
    }
    const metadata = attachmentMetadataSchema.safeParse(rawMetadata);
    if (!metadata.success || metadata.data.id !== parsedAttachmentId) {
      throw new LinqApiError(
        "Linq attachment metadata did not match the pinned contract",
        metadataResponse.status,
        false,
      );
    }
    if (metadata.data.size_bytes > maxBytes) {
      throw new LinqAttachmentContentError("too_large");
    }

    const mediaType = normalizeMediaType(metadata.data.content_type);
    const kind = MODEL_IMAGE_TYPES.has(mediaType) ? "image" : MODEL_FILE_TYPES.has(mediaType) ? "file" : null;
    if (kind === null) {
      throw new LinqAttachmentContentError("unsupported_type");
    }
    if (!metadata.data.download_url) {
      throw new LinqApiError("Linq attachment does not yet have a download URL", null, true);
    }
    const downloadUrl = validatedAttachmentDownloadUrl(metadata.data.download_url);

    let downloadResponse: Awaited<ReturnType<LinqFetch>>;
    try {
      downloadResponse = await this.#fetch(downloadUrl, {
        method: "GET",
        redirect: "error",
        signal: AbortSignal.timeout(this.config.requestTimeoutMs),
      });
    } catch (error) {
      throw new LinqApiError(
        error instanceof Error && error.name === "TimeoutError"
          ? "Linq attachment download timed out"
          : "Linq attachment download failed before a response was received",
        null,
        true,
      );
    }
    if (!downloadResponse.ok) {
      if (downloadResponse.status === 404) {
        throw new LinqAttachmentContentError("not_found");
      }
      throw new LinqApiError(
        `Linq attachment download failed with HTTP ${downloadResponse.status}`,
        downloadResponse.status,
        downloadResponse.status === 403 ||
          downloadResponse.status === 408 ||
          downloadResponse.status === 429 ||
          downloadResponse.status >= 500,
      );
    }
    const responseType = normalizeMediaType(downloadResponse.headers?.get("content-type") ?? mediaType);
    if (responseType !== mediaType && responseType !== "application/octet-stream") {
      throw new LinqApiError("Linq attachment download content type changed", downloadResponse.status, false);
    }
    const bytes = await readBoundedBody(downloadResponse, maxBytes);
    if (bytes.byteLength !== metadata.data.size_bytes) {
      throw new LinqApiError("Linq attachment download size changed", downloadResponse.status, true);
    }

    return {
      providerAttachmentId: parsedAttachmentId,
      kind,
      mediaType,
      filename: metadata.data.filename,
      sizeBytes: bytes.byteLength,
      bytes,
    };
  }
}

function normalizeMediaType(value: string): string {
  return value.split(";", 1)[0]?.trim().toLowerCase() ?? "";
}

function validatedAttachmentDownloadUrl(value: string): URL {
  const url = new URL(value);
  if (
    url.protocol !== "https:" ||
    url.hostname !== "cdn.linqapp.com" ||
    (url.port !== "" && url.port !== "443") ||
    url.username !== "" ||
    url.password !== "" ||
    url.hash !== ""
  ) {
    throw new LinqApiError("Linq returned a disallowed attachment download URL", 502, false);
  }
  return url;
}

async function readBoundedBody(response: LinqFetchResponse, maxBytes: number): Promise<Uint8Array> {
  const contentLength = Number(response.headers?.get("content-length") ?? "");
  if (Number.isFinite(contentLength) && contentLength > maxBytes) {
    throw new LinqAttachmentContentError("too_large");
  }
  if (response.body) {
    const reader = response.body.getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    try {
      while (true) {
        const chunk = await reader.read();
        if (chunk.done) break;
        total += chunk.value.byteLength;
        if (total > maxBytes) {
          await reader.cancel();
          throw new LinqAttachmentContentError("too_large");
        }
        chunks.push(chunk.value);
      }
    } finally {
      reader.releaseLock();
    }
    return Uint8Array.from(Buffer.concat(chunks, total));
  }
  if (response.arrayBuffer) {
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.byteLength > maxBytes) throw new LinqAttachmentContentError("too_large");
    return bytes;
  }
  throw new LinqApiError("Linq attachment response had no readable body", response.status, true);
}

function normalizeChat(payload: unknown, status: number): LinqChat {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    throw new LinqApiError("Linq returned an invalid chat response", status, false);
  }
  const record = payload as Record<string, unknown>;
  const rawHandles = Array.isArray(record.handles) ? record.handles : [];
  const handles = rawHandles.flatMap((value) => {
    if (typeof value !== "object" || value === null || Array.isArray(value)) return [];
    const handle = value as Record<string, unknown>;
    if (typeof handle.handle !== "string" || !handle.handle.trim()) return [];
    const state = typeof handle.status === "string" ? handle.status : "active";
    if (state !== "active") return [];
    return [{ value: handle.handle.trim(), isSelf: handle.is_me === true }];
  });
  const activeHandles = [...new Set(handles.map((handle) => handle.value))];
  const selfHandles = [...new Set(handles.filter((handle) => handle.isSelf).map((handle) => handle.value))];
  const participantHandles = [
    ...new Set(handles.filter((handle) => !handle.isSelf).map((handle) => handle.value)),
  ];
  const health =
    typeof record.health_status === "object" &&
    record.health_status !== null &&
    !Array.isArray(record.health_status)
      ? (record.health_status as Record<string, unknown>).status
      : undefined;
  const parsed = linqChatSchema.safeParse({
    id: record.id,
    isGroup: record.is_group,
    displayName: typeof record.display_name === "string" ? record.display_name : null,
    service: typeof record.service === "string" ? record.service : null,
    healthStatus: health,
    activeHandles,
    selfHandles,
    participantHandles,
  });
  if (!parsed.success) {
    throw new LinqApiError("Linq chat response did not match the pinned contract", status, false);
  }
  return parsed.data;
}

function extractMessageId(payload: unknown): string | null {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return null;
  }
  const record = payload as Record<string, unknown>;
  const message =
    typeof record.message === "object" && record.message !== null && !Array.isArray(record.message)
      ? (record.message as Record<string, unknown>)
      : null;
  for (const value of [record.id, record.message_id, message?.id]) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}
