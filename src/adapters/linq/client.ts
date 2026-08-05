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

const e164Schema = z.string().regex(/^\+[1-9]\d{1,14}$/u);
const providerValueSchema = z.string().trim().min(1).max(255);
const providerTimestampSchema = z.iso.datetime({ offset: true });

export const linqChatHandleSnapshotSchema = z
  .object({
    id: z.string().min(1).max(500),
    handle: z.string().min(1).max(500),
    service: providerValueSchema,
    status: providerValueSchema,
    joinedAt: providerTimestampSchema,
    leftAt: providerTimestampSchema.nullable(),
    isSelf: z.boolean().nullable(),
  })
  .strict();

export type LinqChatHandleSnapshot = z.infer<typeof linqChatHandleSnapshotSchema>;

export const linqChatSnapshotSchema = z
  .object({
    id: z.string().min(1).max(500),
    displayName: z.string().nullable(),
    groupChatIconUrl: z.url().nullable(),
    service: providerValueSchema.nullable(),
    handles: z.array(linqChatHandleSnapshotSchema).min(2),
    isGroup: z.boolean(),
    createdAt: providerTimestampSchema,
    updatedAt: providerTimestampSchema,
    health: z
      .object({
        status: providerValueSchema,
        docUrl: z.url(),
        updatedAt: providerTimestampSchema,
      })
      .strict(),
  })
  .strict();

export type LinqChatSnapshot = z.infer<typeof linqChatSnapshotSchema>;

export const linqMessagePartSchema = z.discriminatedUnion("type", [
  z
    .object({
      type: z.literal("text"),
      partIndex: z.number().int().nonnegative(),
      value: z.string(),
    })
    .strict(),
  z
    .object({
      type: z.literal("media"),
      partIndex: z.number().int().nonnegative(),
      providerAttachmentId: z.string().min(1).max(500),
      filename: z.string(),
      mediaType: z.string().min(1).max(500),
      sizeBytes: z.number().int().nonnegative(),
    })
    .strict(),
  z
    .object({
      type: z.literal("link"),
      partIndex: z.number().int().nonnegative(),
      value: z.string().min(1),
    })
    .strict(),
  z
    .object({
      type: z.literal("imessage_app"),
      partIndex: z.number().int().nonnegative(),
      app: z
        .object({
          name: z.string().min(1),
          teamId: z.string().min(1),
          bundleId: z.string().min(1),
          appStoreId: z.number().int().positive().nullable(),
        })
        .strict(),
      url: z.url(),
      fallbackText: z.string().nullable(),
      layout: z
        .object({
          caption: z.string().nullable(),
          subcaption: z.string().nullable(),
          trailingCaption: z.string().nullable(),
          trailingSubcaption: z.string().nullable(),
          imageUrl: z.url().nullable(),
          imageTitle: z.string().nullable(),
          imageSubtitle: z.string().nullable(),
        })
        .strict(),
    })
    .strict(),
  z
    .object({
      type: z.literal("unknown"),
      partIndex: z.number().int().nonnegative(),
      providerType: z.string().min(1).max(255),
    })
    .strict(),
]);

export type LinqMessagePart = z.infer<typeof linqMessagePartSchema>;

export const linqMessageSnapshotSchema = z
  .object({
    id: z.string().min(1).max(500),
    chatId: z.string().min(1).max(500),
    service: providerValueSchema.nullable(),
    preferredService: providerValueSchema.nullable(),
    sender: linqChatHandleSnapshotSchema.nullable(),
    senderHandle: z.string().min(1).max(500).nullable(),
    parts: z.array(linqMessagePartSchema),
    replyTo: z
      .object({
        messageId: z.string().min(1).max(500),
        partIndex: z.number().int().nonnegative().nullable(),
      })
      .strict()
      .nullable(),
    isFromMe: z.boolean(),
    deliveryStatus: providerValueSchema,
    occurredAt: providerTimestampSchema,
    createdAt: providerTimestampSchema,
    updatedAt: providerTimestampSchema,
    sentAt: providerTimestampSchema.nullable(),
    deliveredAt: providerTimestampSchema.nullable(),
    readAt: providerTimestampSchema.nullable(),
    reconciledAt: providerTimestampSchema.nullable(),
    effect: z.object({ type: providerValueSchema, name: providerValueSchema }).strict().nullable(),
  })
  .strict();

export type LinqMessageSnapshot = z.infer<typeof linqMessageSnapshotSchema>;

export const linqWebhookSubscriptionSchema = z
  .object({
    id: z.string().min(1).max(500),
    targetUrl: z.url(),
    subscribedEvents: z.array(providerValueSchema),
    phoneNumbers: z.array(e164Schema).nullable(),
    isActive: z.boolean(),
    createdAt: providerTimestampSchema,
    updatedAt: providerTimestampSchema,
  })
  .strict();

export type LinqWebhookSubscription = z.infer<typeof linqWebhookSubscriptionSchema>;

export const linqPhoneNumberSchema = z
  .object({
    id: z.string().min(1).max(500),
    phoneNumber: e164Schema,
    forwardingNumber: e164Schema.nullable(),
    reputation: z
      .object({
        status: providerValueSchema,
        docUrl: z.url(),
      })
      .strict(),
  })
  .strict();

export type LinqPhoneNumber = z.infer<typeof linqPhoneNumberSchema>;

const linqListChatsPageInputSchema = z
  .object({
    from: e164Schema.optional(),
    to: z.string().min(1).max(500).optional(),
    cursor: z.string().min(1).max(4_096).optional(),
    limit: z.number().int().min(1).max(100).optional(),
  })
  .strict();

export type LinqListChatsPageInput = z.input<typeof linqListChatsPageInputSchema>;

const linqListMessagesPageInputSchema = z
  .object({
    chatId: z.string().min(1).max(500),
    cursor: z.string().min(1).max(4_096).optional(),
    limit: z.number().int().min(1).max(100).optional(),
  })
  .strict();

export type LinqListMessagesPageInput = z.input<typeof linqListMessagesPageInputSchema>;

export interface LinqChatsPage {
  chats: LinqChatSnapshot[];
  nextCursor: string | null;
}

export interface LinqMessagesPage {
  messages: LinqMessageSnapshot[];
  nextCursor: string | null;
}

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

  readonly providerCode: number | null;
  readonly retryAfterSeconds: number | null;
  readonly traceId: string | null;

  constructor(
    message: string,
    readonly status: number | null,
    readonly retryable: boolean,
    metadata: {
      providerCode?: number | null;
      retryAfterSeconds?: number | null;
      traceId?: string | null;
    } = {},
  ) {
    super(message);
    this.providerCode = metadata.providerCode ?? null;
    this.retryAfterSeconds = metadata.retryAfterSeconds ?? null;
    this.traceId = metadata.traceId ?? null;
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

const attachmentMetadataSchema = z.object({
  id: z.string().min(1),
  content_type: z.string().min(1),
  filename: z.string().min(1),
  size_bytes: z.number().int().nonnegative(),
  status: z.enum(["pending", "complete", "failed"]).optional(),
  download_url: z.url().nullable().optional(),
  created_at: z.iso.datetime(),
});

const providerChatHandleSchema = z.object({
  id: z.string().min(1).max(500),
  handle: z.string().min(1).max(500),
  service: providerValueSchema,
  status: providerValueSchema.nullable().optional(),
  joined_at: providerTimestampSchema,
  left_at: providerTimestampSchema.nullable().optional(),
  is_me: z.boolean().nullable().optional(),
});

const providerChatSchema = z.object({
  id: z.string().min(1).max(500),
  display_name: z.string().nullable(),
  group_chat_icon: z.url().nullable().optional(),
  service: providerValueSchema.nullable().optional(),
  handles: z.array(providerChatHandleSchema).min(2),
  is_group: z.boolean(),
  created_at: providerTimestampSchema,
  updated_at: providerTimestampSchema,
  health_status: z.object({
    status: providerValueSchema,
    doc_url: z.url(),
    updated_at: providerTimestampSchema,
  }),
});

const providerTextPartSchema = z.object({
  type: z.literal("text"),
  value: z.string(),
});

const providerMediaPartSchema = z.object({
  type: z.literal("media"),
  id: z.string().min(1).max(500),
  url: z.url(),
  filename: z.string(),
  mime_type: z.string().min(1).max(500),
  size_bytes: z.number().int().nonnegative(),
});

const providerLinkPartSchema = z.object({
  type: z.literal("link"),
  value: z.string().min(1),
});

const providerIMessageAppPartSchema = z.object({
  type: z.literal("imessage_app"),
  app: z.object({
    name: z.string().min(1),
    team_id: z.string().min(1),
    bundle_id: z.string().min(1),
    app_store_id: z.number().int().positive().nullable().optional(),
  }),
  url: z.url(),
  fallback_text: z.string().nullable().optional(),
  layout: z.object({
    caption: z.string().nullable().optional(),
    subcaption: z.string().nullable().optional(),
    trailing_caption: z.string().nullable().optional(),
    trailing_subcaption: z.string().nullable().optional(),
    image_url: z.url().nullable().optional(),
    image_title: z.string().nullable().optional(),
    image_subtitle: z.string().nullable().optional(),
  }),
});

const providerMessageSchema = z.object({
  id: z.string().min(1).max(500),
  chat_id: z.string().min(1).max(500),
  service: providerValueSchema.nullable().optional(),
  preferred_service: providerValueSchema.nullable().optional(),
  from: z.string().min(1).max(500).nullable().optional(),
  from_handle: providerChatHandleSchema.nullable().optional(),
  parts: z.array(z.unknown()).nullable().optional(),
  reply_to: z
    .object({
      message_id: z.string().min(1).max(500),
      part_index: z.number().int().nonnegative().optional(),
    })
    .nullable()
    .optional(),
  is_from_me: z.boolean(),
  delivery_status: providerValueSchema,
  created_at: providerTimestampSchema,
  updated_at: providerTimestampSchema,
  sent_at: providerTimestampSchema.nullable().optional(),
  delivered_at: providerTimestampSchema.nullable().optional(),
  read_at: providerTimestampSchema.nullable().optional(),
  reconciled_at: providerTimestampSchema.nullable().optional(),
  effect: z
    .object({
      type: providerValueSchema,
      name: providerValueSchema,
    })
    .nullable()
    .optional(),
});

const providerWebhookSubscriptionSchema = z.object({
  id: z.string().min(1).max(500),
  target_url: z.url(),
  subscribed_events: z.array(providerValueSchema),
  phone_numbers: z.array(e164Schema).nullable().optional(),
  is_active: z.boolean(),
  created_at: providerTimestampSchema,
  updated_at: providerTimestampSchema,
});

const providerPhoneNumberSchema = z.object({
  id: z.string().min(1).max(500),
  phone_number: e164Schema,
  forwarding_number: e164Schema.nullable().optional(),
  reputation: z.object({
    status: providerValueSchema,
    doc_url: z.url(),
  }),
});

const providerChatsPageSchema = z.object({
  chats: z.array(z.unknown()),
  next_cursor: z.string().min(1).max(4_096).nullable().optional(),
});

const providerMessagesPageSchema = z.object({
  messages: z.array(z.unknown()),
  next_cursor: z.string().min(1).max(4_096).nullable().optional(),
});

const providerWebhookSubscriptionsResultSchema = z.object({
  subscriptions: z.array(z.unknown()),
});

const providerPhoneNumbersResultSchema = z.object({
  phone_numbers: z.array(z.unknown()),
});

const linqErrorEnvelopeSchema = z.object({
  error: z
    .object({
      code: z.number().int().safe().optional(),
      retry_after: z.number().int().safe().nonnegative().optional(),
    })
    .optional(),
  trace_id: z
    .string()
    .trim()
    .regex(/^[A-Za-z0-9._:-]{1,200}$/u)
    .optional(),
});

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
    const { payload, status } = await this.#requestJson(
      `${this.config.apiBaseUrl}/chats/${encodeURIComponent(input.chatId)}/messages`,
      "Linq send request",
      { method: "POST", body },
    );
    const providerMessageId = extractMessageId(payload);
    if (providerMessageId === null) {
      throw new LinqApiError("Linq send response did not include a message ID", status, false);
    }

    return {
      provider: "linq",
      providerMessageId,
      chatId: input.chatId,
      idempotencyKey: input.idempotencyKey,
    };
  }

  async getChat(chatId: string): Promise<LinqChat> {
    const { snapshot, status } = await this.#getChatSnapshot(chatId);
    return classifyChat(snapshot, status);
  }

  async getChatSnapshot(chatId: string): Promise<LinqChatSnapshot> {
    return (await this.#getChatSnapshot(chatId)).snapshot;
  }

  async #getChatSnapshot(chatId: string): Promise<{ snapshot: LinqChatSnapshot; status: number }> {
    const parsedChatId = z.string().min(1).max(500).parse(chatId);
    const { payload, status } = await this.#requestJson(
      `${this.config.apiBaseUrl}/chats/${encodeURIComponent(parsedChatId)}`,
      "Linq chat lookup",
    );
    const snapshot = normalizeChatSnapshot(payload, status);
    if (snapshot.id !== parsedChatId) {
      throw pinnedContractError("chat lookup", status);
    }
    return { snapshot, status };
  }

  async listChatsPage(rawInput: LinqListChatsPageInput = {}): Promise<LinqChatsPage> {
    const input = linqListChatsPageInputSchema.parse(rawInput);
    const url = withQuery(`${this.config.apiBaseUrl}/chats`, {
      from: input.from,
      to: input.to,
      cursor: input.cursor,
      limit: input.limit,
    });
    const { payload, status } = await this.#requestJson(url, "Linq chat list request");
    return normalizeChatsPage(payload, status);
  }

  async listMessagesPage(rawInput: LinqListMessagesPageInput): Promise<LinqMessagesPage> {
    const input = linqListMessagesPageInputSchema.parse(rawInput);
    const url = withQuery(`${this.config.apiBaseUrl}/chats/${encodeURIComponent(input.chatId)}/messages`, {
      cursor: input.cursor,
      limit: input.limit,
    });
    const { payload, status } = await this.#requestJson(url, "Linq message list request");
    const page = normalizeMessagesPage(payload, status);
    if (page.messages.some((message) => message.chatId !== input.chatId)) {
      throw pinnedContractError("message list", status);
    }
    return page;
  }

  async listWebhookSubscriptions(): Promise<LinqWebhookSubscription[]> {
    const { payload, status } = await this.#requestJson(
      `${this.config.apiBaseUrl}/webhook-subscriptions`,
      "Linq webhook subscription list request",
    );
    return normalizeWebhookSubscriptions(payload, status);
  }

  async listPhoneNumbers(): Promise<LinqPhoneNumber[]> {
    const { payload, status } = await this.#requestJson(
      `${this.config.apiBaseUrl}/phone_numbers`,
      "Linq phone number list request",
    );
    return normalizePhoneNumbers(payload, status);
  }

  async retrieveAttachment(
    attachmentId: string,
    requestedMaxBytes = MAX_MODEL_ATTACHMENT_BYTES,
  ): Promise<LinqRetrievedAttachment> {
    const parsedAttachmentId = z.string().trim().min(1).max(500).parse(attachmentId);
    const maxBytes = z.number().int().positive().max(MAX_MODEL_ATTACHMENT_BYTES).parse(requestedMaxBytes);
    let metadataResult: { payload: unknown; status: number };
    try {
      metadataResult = await this.#requestJson(
        `${this.config.apiBaseUrl}/attachments/${encodeURIComponent(parsedAttachmentId)}`,
        "Linq attachment metadata request",
        { redirect: "error" },
      );
    } catch (error) {
      if (error instanceof LinqApiError && error.status === 404) {
        throw new LinqAttachmentContentError("not_found");
      }
      throw error;
    }
    const metadata = attachmentMetadataSchema.safeParse(metadataResult.payload);
    if (!metadata.success || metadata.data.id !== parsedAttachmentId) {
      throw new LinqApiError(
        "Linq attachment metadata did not match the pinned contract",
        metadataResult.status,
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
        { retryAfterSeconds: retryAfterHeader(downloadResponse) },
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

  async #requestJson(
    url: string,
    operation: string,
    options: {
      method?: "GET" | "POST";
      body?: unknown;
      redirect?: RequestInit["redirect"];
    } = {},
  ): Promise<{ payload: unknown; status: number }> {
    let response: Awaited<ReturnType<LinqFetch>>;
    try {
      response = await this.#fetch(url, {
        method: options.method ?? "GET",
        headers: {
          authorization: `Bearer ${this.config.apiKey}`,
          ...(options.body === undefined ? {} : { "content-type": "application/json" }),
        },
        ...(options.body === undefined ? {} : { body: JSON.stringify(options.body) }),
        ...(options.redirect === undefined ? {} : { redirect: options.redirect }),
        signal: AbortSignal.timeout(this.config.requestTimeoutMs),
      });
    } catch (error) {
      throw new LinqApiError(
        error instanceof Error && error.name === "TimeoutError"
          ? `${operation} timed out`
          : `${operation} failed before a response was received`,
        null,
        true,
      );
    }

    if (!response.ok) {
      throw await parseLinqApiError(response, operation);
    }

    try {
      return { payload: await response.json(), status: response.status };
    } catch {
      throw new LinqApiError(`${operation} returned invalid JSON`, response.status, false);
    }
  }
}

function normalizeMediaType(value: string): string {
  return value.split(";", 1)[0]?.trim().toLowerCase() ?? "";
}

function normalizeProviderTimestamp(value: string): string {
  const millisecondPrecision = value.replace(/(\.\d{3})\d+(?=(?:Z|[+-]\d{2}:\d{2})$)/u, "$1");
  return new Date(millisecondPrecision).toISOString();
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

function withQuery(baseUrl: string, values: Readonly<Record<string, string | number | undefined>>): string {
  const url = new URL(baseUrl);
  for (const [name, value] of Object.entries(values)) {
    if (value !== undefined) url.searchParams.set(name, String(value));
  }
  return url.toString();
}

async function parseLinqApiError(response: LinqFetchResponse, operation: string): Promise<LinqApiError> {
  let providerCode: number | null = null;
  let bodyRetryAfter: number | null = null;
  let traceId: string | null = null;
  try {
    const parsed = linqErrorEnvelopeSchema.safeParse(await response.json());
    if (parsed.success) {
      providerCode = parsed.data.error?.code ?? null;
      bodyRetryAfter = parsed.data.error?.retry_after ?? null;
      traceId = parsed.data.trace_id ?? null;
    }
  } catch {
    // Error bodies are untrusted and intentionally never propagated or logged here.
  }
  return new LinqApiError(
    `${operation} failed with HTTP ${response.status}`,
    response.status,
    isRetryableStatus(response.status),
    {
      providerCode,
      retryAfterSeconds: retryAfterHeader(response) ?? bodyRetryAfter,
      traceId,
    },
  );
}

function retryAfterHeader(response: LinqFetchResponse): number | null {
  let value: string | null;
  try {
    value = response.headers?.get("retry-after") ?? null;
  } catch {
    return null;
  }
  if (value === null) return null;
  const normalized = value.trim();
  if (!/^\d{1,15}$/u.test(normalized)) return null;
  const seconds = Number(normalized);
  return Number.isSafeInteger(seconds) ? seconds : null;
}

function isRetryableStatus(status: number): boolean {
  return status === 408 || status === 429 || status >= 500;
}

function pinnedContractError(resource: string, status: number): LinqApiError {
  return new LinqApiError(`Linq ${resource} response did not match the pinned contract`, status, false);
}

function normalizeChatHandle(value: z.output<typeof providerChatHandleSchema>): LinqChatHandleSnapshot {
  return linqChatHandleSnapshotSchema.parse({
    id: value.id.trim(),
    handle: value.handle.trim(),
    service: value.service,
    status: value.status ?? "active",
    joinedAt: normalizeProviderTimestamp(value.joined_at),
    leftAt: value.left_at ? normalizeProviderTimestamp(value.left_at) : null,
    isSelf: value.is_me ?? null,
  });
}

function normalizeChatSnapshot(payload: unknown, status: number): LinqChatSnapshot {
  const chat = providerChatSchema.safeParse(payload);
  if (!chat.success) throw pinnedContractError("chat", status);
  const normalized = linqChatSnapshotSchema.safeParse({
    id: chat.data.id.trim(),
    displayName: chat.data.display_name,
    groupChatIconUrl: chat.data.group_chat_icon ?? null,
    service: chat.data.service ?? null,
    handles: chat.data.handles.map(normalizeChatHandle),
    isGroup: chat.data.is_group,
    createdAt: normalizeProviderTimestamp(chat.data.created_at),
    updatedAt: normalizeProviderTimestamp(chat.data.updated_at),
    health: {
      status: chat.data.health_status.status,
      docUrl: chat.data.health_status.doc_url,
      updatedAt: normalizeProviderTimestamp(chat.data.health_status.updated_at),
    },
  });
  if (!normalized.success) throw pinnedContractError("chat", status);
  return normalized.data;
}

function classifyChat(chat: LinqChatSnapshot, status: number): LinqChat {
  const activeHandles = chat.handles.filter((handle) => handle.status === "active");
  const normalized = linqChatSchema.safeParse({
    id: chat.id,
    isGroup: chat.isGroup,
    displayName: chat.displayName,
    service: chat.service,
    healthStatus: chat.health.status,
    activeHandles: [...new Set(activeHandles.map((handle) => handle.handle))],
    selfHandles: [
      ...new Set(activeHandles.filter((handle) => handle.isSelf === true).map((handle) => handle.handle)),
    ],
    participantHandles: [
      ...new Set(activeHandles.filter((handle) => handle.isSelf !== true).map((handle) => handle.handle)),
    ],
  });
  if (!normalized.success) throw pinnedContractError("chat", status);
  return normalized.data;
}

function normalizeChatsPage(payload: unknown, status: number): LinqChatsPage {
  const page = providerChatsPageSchema.safeParse(payload);
  if (!page.success) throw pinnedContractError("chat list", status);
  return {
    chats: page.data.chats.map((chat) => normalizeChatSnapshot(chat, status)),
    nextCursor: page.data.next_cursor ?? null,
  };
}

function normalizeMessagePart(payload: unknown, partIndex: number, status: number): LinqMessagePart {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    throw pinnedContractError("message list", status);
  }
  const providerType = (payload as Record<string, unknown>).type;
  if (typeof providerType !== "string" || !providerType.trim()) {
    throw pinnedContractError("message list", status);
  }
  if (providerType === "text") {
    const part = providerTextPartSchema.safeParse(payload);
    if (!part.success) throw pinnedContractError("message list", status);
    return { type: "text", partIndex, value: part.data.value };
  }
  if (providerType === "media") {
    const part = providerMediaPartSchema.safeParse(payload);
    if (!part.success) throw pinnedContractError("message list", status);
    return {
      type: "media",
      partIndex,
      providerAttachmentId: part.data.id.trim(),
      filename: part.data.filename,
      mediaType: normalizeMediaType(part.data.mime_type),
      sizeBytes: part.data.size_bytes,
    };
  }
  if (providerType === "link") {
    const part = providerLinkPartSchema.safeParse(payload);
    if (!part.success) throw pinnedContractError("message list", status);
    return { type: "link", partIndex, value: part.data.value };
  }
  if (providerType === "imessage_app") {
    const part = providerIMessageAppPartSchema.safeParse(payload);
    if (!part.success) throw pinnedContractError("message list", status);
    return {
      type: "imessage_app",
      partIndex,
      app: {
        name: part.data.app.name,
        teamId: part.data.app.team_id,
        bundleId: part.data.app.bundle_id,
        appStoreId: part.data.app.app_store_id ?? null,
      },
      url: part.data.url,
      fallbackText: part.data.fallback_text ?? null,
      layout: {
        caption: part.data.layout.caption ?? null,
        subcaption: part.data.layout.subcaption ?? null,
        trailingCaption: part.data.layout.trailing_caption ?? null,
        trailingSubcaption: part.data.layout.trailing_subcaption ?? null,
        imageUrl: part.data.layout.image_url ?? null,
        imageTitle: part.data.layout.image_title ?? null,
        imageSubtitle: part.data.layout.image_subtitle ?? null,
      },
    };
  }
  return {
    type: "unknown",
    partIndex,
    providerType: providerType.trim(),
  };
}

function normalizeMessageSnapshot(payload: unknown, status: number): LinqMessageSnapshot {
  const message = providerMessageSchema.safeParse(payload);
  if (!message.success) throw pinnedContractError("message list", status);
  const sender = message.data.from_handle ? normalizeChatHandle(message.data.from_handle) : null;
  const normalized = linqMessageSnapshotSchema.safeParse({
    id: message.data.id.trim(),
    chatId: message.data.chat_id.trim(),
    service: message.data.service ?? null,
    preferredService: message.data.preferred_service ?? null,
    sender,
    senderHandle: sender?.handle ?? message.data.from?.trim() ?? null,
    parts: (message.data.parts ?? []).map((part, index) => normalizeMessagePart(part, index, status)),
    replyTo: message.data.reply_to
      ? {
          messageId: message.data.reply_to.message_id.trim(),
          partIndex: message.data.reply_to.part_index ?? null,
        }
      : null,
    isFromMe: message.data.is_from_me,
    deliveryStatus: message.data.delivery_status,
    occurredAt: normalizeProviderTimestamp(message.data.sent_at ?? message.data.created_at),
    createdAt: normalizeProviderTimestamp(message.data.created_at),
    updatedAt: normalizeProviderTimestamp(message.data.updated_at),
    sentAt: message.data.sent_at ? normalizeProviderTimestamp(message.data.sent_at) : null,
    deliveredAt: message.data.delivered_at ? normalizeProviderTimestamp(message.data.delivered_at) : null,
    readAt: message.data.read_at ? normalizeProviderTimestamp(message.data.read_at) : null,
    reconciledAt: message.data.reconciled_at ? normalizeProviderTimestamp(message.data.reconciled_at) : null,
    effect: message.data.effect ?? null,
  });
  if (!normalized.success) throw pinnedContractError("message list", status);
  return normalized.data;
}

function normalizeMessagesPage(payload: unknown, status: number): LinqMessagesPage {
  const page = providerMessagesPageSchema.safeParse(payload);
  if (!page.success) throw pinnedContractError("message list", status);
  return {
    messages: page.data.messages.map((message) => normalizeMessageSnapshot(message, status)),
    nextCursor: page.data.next_cursor ?? null,
  };
}

function normalizeWebhookSubscriptions(payload: unknown, status: number): LinqWebhookSubscription[] {
  const result = providerWebhookSubscriptionsResultSchema.safeParse(payload);
  if (!result.success) throw pinnedContractError("webhook subscription list", status);
  return result.data.subscriptions.map((payloadSubscription) => {
    const subscription = providerWebhookSubscriptionSchema.safeParse(payloadSubscription);
    if (!subscription.success) throw pinnedContractError("webhook subscription list", status);
    const normalized = linqWebhookSubscriptionSchema.safeParse({
      id: subscription.data.id.trim(),
      targetUrl: subscription.data.target_url,
      subscribedEvents: subscription.data.subscribed_events,
      phoneNumbers: subscription.data.phone_numbers ?? null,
      isActive: subscription.data.is_active,
      createdAt: normalizeProviderTimestamp(subscription.data.created_at),
      updatedAt: normalizeProviderTimestamp(subscription.data.updated_at),
    });
    if (!normalized.success) throw pinnedContractError("webhook subscription list", status);
    return normalized.data;
  });
}

function normalizePhoneNumbers(payload: unknown, status: number): LinqPhoneNumber[] {
  const result = providerPhoneNumbersResultSchema.safeParse(payload);
  if (!result.success) throw pinnedContractError("phone number list", status);
  return result.data.phone_numbers.map((payloadPhoneNumber) => {
    const phoneNumber = providerPhoneNumberSchema.safeParse(payloadPhoneNumber);
    if (!phoneNumber.success) throw pinnedContractError("phone number list", status);
    const normalized = linqPhoneNumberSchema.safeParse({
      id: phoneNumber.data.id.trim(),
      phoneNumber: phoneNumber.data.phone_number,
      forwardingNumber: phoneNumber.data.forwarding_number ?? null,
      reputation: {
        status: phoneNumber.data.reputation.status,
        docUrl: phoneNumber.data.reputation.doc_url,
      },
    });
    if (!normalized.success) throw pinnedContractError("phone number list", status);
    return normalized.data;
  });
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
