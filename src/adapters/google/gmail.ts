import { createHash } from "node:crypto";
import { type gmail_v1, google } from "googleapis";
import { z } from "zod";
import { googleAuthWithAccessToken } from "./auth.js";
import type { GoogleAdapterConfig } from "./config.js";
import {
  GoogleAdapterError,
  GoogleSyncTokenExpiredError,
  mapGoogleProviderError,
  providerStatus,
} from "./errors.js";

const gmailHeaderSummarySchema = z
  .object({
    from: z.string().nullable(),
    to: z.string().nullable(),
    cc: z.string().nullable(),
    bcc: z.string().nullable(),
    replyTo: z.string().nullable(),
    subject: z.string().nullable(),
    date: z.string().nullable(),
    messageId: z.string().nullable(),
    inReplyTo: z.string().nullable(),
    references: z.string().nullable(),
  })
  .strict();

export const gmailAttachmentSchema = z
  .object({
    partId: z.string().nullable(),
    providerAttachmentId: z.string().nullable(),
    filename: z.string(),
    mimeType: z.string(),
    sizeBytes: z.number().int().nonnegative(),
    contentId: z.string().nullable(),
    inline: z.boolean(),
    embeddedDataBase64Url: z.string().nullable(),
  })
  .strict();

export type GmailAttachment = z.infer<typeof gmailAttachmentSchema>;

const gmailRetrievedAttachmentBodySchema = z
  .object({
    attachmentId: z.string().nullable().optional(),
    size: z.number().int().nonnegative(),
    data: z.string(),
  })
  .passthrough();

export const gmailMessageSchema = z
  .object({
    schemaVersion: z.literal(1),
    source: z.literal("gmail"),
    sourceScope: z.literal("personal"),
    googleSubject: z.string().min(1),
    sourceKey: z.string().min(1),
    messageId: z.string().min(1),
    threadId: z.string().min(1),
    historyId: z.string().min(1).nullable(),
    internalDate: z.string().nullable(),
    labelIds: z.array(z.string().min(1)),
    snippet: z.string(),
    headers: gmailHeaderSummarySchema,
    rawHeaders: z.record(z.string(), z.array(z.string())),
    body: z
      .object({
        text: z.string().nullable(),
        html: z.string().nullable(),
      })
      .strict(),
    attachments: z.array(gmailAttachmentSchema),
  })
  .strict();

export type GmailMessage = z.infer<typeof gmailMessageSchema>;

export const gmailHistoryChangeSchema = z
  .object({
    schemaVersion: z.literal(1),
    source: z.literal("gmail"),
    sourceScope: z.literal("personal"),
    googleSubject: z.string().min(1),
    providerEventId: z.string().min(1),
    historyId: z.string().min(1),
    changeType: z.enum(["message.added", "message.deleted", "labels.added", "labels.removed"]),
    messageId: z.string().min(1),
    threadId: z.string().min(1).nullable(),
    labelIds: z.array(z.string().min(1)),
  })
  .strict();

export type GmailHistoryChange = z.infer<typeof gmailHistoryChangeSchema>;

export const gmailPubSubEventSchema = z
  .object({
    schemaVersion: z.literal(1),
    source: z.literal("gmail"),
    sourceScope: z.literal("personal"),
    providerEventId: z.string().min(1),
    subscription: z.string().min(1),
    mailboxEmail: z.string().email(),
    historyId: z.string().regex(/^\d+$/),
    publishedAt: z.string().min(1),
    deliveryAttempt: z.number().int().positive().nullable(),
  })
  .strict();

export type GmailPubSubEvent = z.infer<typeof gmailPubSubEventSchema>;

export interface GmailHistoryPage {
  changes: GmailHistoryChange[];
  mailboxHistoryId: string;
  nextPageToken: string | null;
}

export interface GmailMessageIdPage {
  messages: Array<{ messageId: string; threadId: string | null }>;
  nextPageToken: string | null;
  resultSizeEstimate: number | null;
}

export interface GmailWatchReceipt {
  historyId: string;
  expiresAt: string;
}

export type GmailMessageFormat = "metadata" | "full";

export interface RetrieveGmailAttachmentInput {
  accessToken: string;
  messageId: string;
  attachment: GmailAttachment;
}

export interface GmailRetrievedAttachment {
  kind: "image" | "file";
  mediaType: string;
  filename: string;
  sizeBytes: number;
  bytes: Uint8Array;
}

export type GmailAttachmentContentFailure =
  | "missing_reference"
  | "too_large"
  | "unsupported_type"
  | "invalid_content";

export class GmailAttachmentContentError extends GoogleAdapterError {
  override readonly name = "GmailAttachmentContentError";

  constructor(readonly reason: GmailAttachmentContentFailure) {
    super(
      `Gmail attachment content is unavailable (${reason})`,
      "permanent",
      reason === "too_large" ? 413 : 422,
      false,
    );
  }
}

interface GmailAttachmentRequestOptions {
  maxContentLength: number;
}

const MAX_GMAIL_ATTACHMENT_BYTES = 10 * 1024 * 1024;
const MAX_GMAIL_ATTACHMENT_BASE64URL_CHARACTERS = Math.ceil((MAX_GMAIL_ATTACHMENT_BYTES * 4) / 3);
const MAX_GMAIL_ATTACHMENT_RESPONSE_BYTES = 14_000_000;
const GMAIL_IMAGE_MEDIA_TYPES = new Set(["image/jpeg", "image/png", "image/gif", "image/webp"]);
const GMAIL_TEXT_MEDIA_TYPES = new Set(["text/plain", "text/csv", "text/calendar", "text/markdown"]);
const GMAIL_FILE_MEDIA_TYPES = new Set(["application/pdf", ...GMAIL_TEXT_MEDIA_TYPES]);
const GMAIL_MEDIA_TYPE_ALIASES = new Map([
  ["application/x-pdf", "application/pdf"],
  ["image/jpg", "image/jpeg"],
  ["image/pjpeg", "image/jpeg"],
  ["text/x-markdown", "text/markdown"],
]);

interface GmailApiPort {
  getMessage(params: {
    userId: "me";
    id: string;
    format: GmailMessageFormat;
  }): Promise<{ data: gmail_v1.Schema$Message }>;
  getAttachment(
    params: {
      userId: "me";
      messageId: string;
      id: string;
    },
    options: GmailAttachmentRequestOptions,
  ): Promise<{ data: gmail_v1.Schema$MessagePartBody }>;
  listHistory(params: {
    userId: "me";
    startHistoryId: string;
    maxResults: number;
    historyTypes: string[];
    pageToken?: string;
  }): Promise<{ data: gmail_v1.Schema$ListHistoryResponse }>;
  listMessages(params: {
    userId: "me";
    maxResults: number;
    includeSpamTrash: boolean;
    q?: string;
    labelIds?: string[];
    pageToken?: string;
  }): Promise<{ data: gmail_v1.Schema$ListMessagesResponse }>;
  watch(params: {
    userId: "me";
    requestBody: {
      topicName: string;
      labelFilterBehavior?: "include" | "exclude";
      labelIds?: string[];
    };
  }): Promise<{ data: gmail_v1.Schema$WatchResponse }>;
  stop(params: { userId: "me" }): Promise<unknown>;
}

export type GmailApiFactory = (accessToken: string) => GmailApiPort;

export class GmailAdapter {
  readonly #apiFactory: GmailApiFactory;

  constructor(config: GoogleAdapterConfig, apiFactory?: GmailApiFactory) {
    this.#apiFactory = apiFactory ?? defaultGmailApiFactory(config);
  }

  async getMessage(input: {
    accessToken: string;
    googleSubject: string;
    messageId: string;
    format?: GmailMessageFormat;
  }): Promise<GmailMessage> {
    requireAccessToken(input.accessToken);
    try {
      const response = await this.#apiFactory(input.accessToken).getMessage({
        userId: "me",
        id: input.messageId,
        format: input.format ?? "full",
      });
      return parseGmailMessage(response.data, input.googleSubject);
    } catch (error) {
      throw mapGoogleProviderError("Gmail message fetch", error);
    }
  }

  async retrieveAttachment(input: RetrieveGmailAttachmentInput): Promise<GmailRetrievedAttachment> {
    requireAccessToken(input.accessToken);
    requireProviderId(input.messageId, "Gmail message ID");

    const parsedAttachment = gmailAttachmentSchema.safeParse(input.attachment);
    if (!parsedAttachment.success) {
      throw new GmailAttachmentContentError("invalid_content");
    }
    const attachment = parsedAttachment.data;
    const hasEmbeddedData = attachment.embeddedDataBase64Url !== null;
    const hasExternalReference = attachment.providerAttachmentId !== null;
    if (hasEmbeddedData === hasExternalReference) {
      throw new GmailAttachmentContentError("missing_reference");
    }
    if (attachment.partId === null || !isProviderId(attachment.partId)) {
      throw new GmailAttachmentContentError("missing_reference");
    }
    if (attachment.sizeBytes > MAX_GMAIL_ATTACHMENT_BYTES) {
      throw new GmailAttachmentContentError("too_large");
    }

    const normalizedType = normalizeGmailAttachmentMediaType(attachment.mimeType);
    const kind = GMAIL_IMAGE_MEDIA_TYPES.has(normalizedType) ? "image" : "file";
    let encodedData: string;
    let providerSize = attachment.sizeBytes;

    if (attachment.embeddedDataBase64Url !== null) {
      encodedData = attachment.embeddedDataBase64Url;
    } else {
      const attachmentId = attachment.providerAttachmentId;
      if (attachmentId === null || !isProviderId(attachmentId)) {
        throw new GmailAttachmentContentError("missing_reference");
      }

      let response: { data: gmail_v1.Schema$MessagePartBody };
      try {
        response = await this.#apiFactory(input.accessToken).getAttachment(
          {
            userId: "me",
            messageId: input.messageId,
            id: attachmentId,
          },
          { maxContentLength: MAX_GMAIL_ATTACHMENT_RESPONSE_BYTES },
        );
      } catch (error) {
        throw mapGoogleProviderError("Gmail attachment fetch", error);
      }

      const parsedResponse = gmailRetrievedAttachmentBodySchema.safeParse(response.data);
      if (
        !parsedResponse.success ||
        (parsedResponse.data.attachmentId !== null &&
          parsedResponse.data.attachmentId !== undefined &&
          parsedResponse.data.attachmentId !== attachmentId) ||
        parsedResponse.data.size !== attachment.sizeBytes
      ) {
        throw new GmailAttachmentContentError("invalid_content");
      }
      providerSize = parsedResponse.data.size;
      encodedData = parsedResponse.data.data;
    }

    const bytes = decodeCanonicalAttachmentBase64Url(encodedData, attachment.sizeBytes);
    if (bytes.byteLength !== attachment.sizeBytes || bytes.byteLength !== providerSize) {
      throw new GmailAttachmentContentError("invalid_content");
    }
    validateGmailAttachmentBytes(normalizedType, bytes);

    return {
      kind,
      mediaType: normalizedType,
      filename: attachment.filename,
      sizeBytes: bytes.byteLength,
      bytes,
    };
  }

  async listHistoryPage(input: {
    accessToken: string;
    googleSubject: string;
    startHistoryId: string;
    pageToken?: string;
    maxResults?: number;
  }): Promise<GmailHistoryPage> {
    requireAccessToken(input.accessToken);
    if (!/^\d+$/.test(input.startHistoryId)) {
      throw new GoogleAdapterError("Gmail history ID is invalid", "invalid_request", null, false);
    }
    const maxResults = boundedInteger(input.maxResults ?? 500, 1, 500, "Gmail history page size");
    try {
      const response = await this.#apiFactory(input.accessToken).listHistory({
        userId: "me",
        startHistoryId: input.startHistoryId,
        maxResults,
        historyTypes: ["messageAdded", "messageDeleted", "labelAdded", "labelRemoved"],
        ...(input.pageToken ? { pageToken: input.pageToken } : {}),
      });
      return parseGmailHistoryPage(response.data, input.googleSubject);
    } catch (error) {
      if (providerStatus(error) === 404) {
        throw new GoogleSyncTokenExpiredError("gmail");
      }
      throw mapGoogleProviderError("Gmail history fetch", error);
    }
  }

  async listMessageIdsPage(input: {
    accessToken: string;
    pageToken?: string;
    maxResults?: number;
    query?: string;
    labelIds?: string[];
    includeSpamTrash?: boolean;
  }): Promise<GmailMessageIdPage> {
    requireAccessToken(input.accessToken);
    const maxResults = boundedInteger(input.maxResults ?? 100, 1, 500, "Gmail message page size");
    try {
      const response = await this.#apiFactory(input.accessToken).listMessages({
        userId: "me",
        maxResults,
        includeSpamTrash: input.includeSpamTrash === true,
        ...(input.pageToken ? { pageToken: input.pageToken } : {}),
        ...(input.query ? { q: input.query } : {}),
        ...(input.labelIds?.length ? { labelIds: input.labelIds } : {}),
      });
      return {
        messages: (response.data.messages ?? []).flatMap((message) =>
          message.id ? [{ messageId: message.id, threadId: message.threadId ?? null }] : [],
        ),
        nextPageToken: response.data.nextPageToken ?? null,
        resultSizeEstimate:
          typeof response.data.resultSizeEstimate === "number" ? response.data.resultSizeEstimate : null,
      };
    } catch (error) {
      throw mapGoogleProviderError("Gmail message listing", error);
    }
  }

  async startWatch(input: {
    accessToken: string;
    topicName: string;
    labelIds?: string[];
    labelFilterBehavior?: "include" | "exclude";
  }): Promise<GmailWatchReceipt> {
    requireAccessToken(input.accessToken);
    if (!/^projects\/[^/]+\/topics\/[^/]+$/.test(input.topicName)) {
      throw new GoogleAdapterError("Gmail watch topic name is invalid", "invalid_request", null, false);
    }
    try {
      const response = await this.#apiFactory(input.accessToken).watch({
        userId: "me",
        requestBody: {
          topicName: input.topicName,
          ...(input.labelIds?.length ? { labelIds: input.labelIds } : {}),
          ...(input.labelFilterBehavior ? { labelFilterBehavior: input.labelFilterBehavior } : {}),
        },
      });
      if (!response.data.historyId || !response.data.expiration) {
        throw new GoogleAdapterError("Gmail watch response is incomplete", "permanent", null, false);
      }
      return {
        historyId: response.data.historyId,
        expiresAt: epochMillisecondsToIso(response.data.expiration, "Gmail watch expiration"),
      };
    } catch (error) {
      throw mapGoogleProviderError("Gmail watch creation", error);
    }
  }

  async stopWatch(accessToken: string): Promise<void> {
    requireAccessToken(accessToken);
    try {
      await this.#apiFactory(accessToken).stop({ userId: "me" });
    } catch (error) {
      throw mapGoogleProviderError("Gmail watch removal", error);
    }
  }
}

export function parseGmailMessage(message: gmail_v1.Schema$Message, googleSubject: string): GmailMessage {
  if (!message.id || !message.threadId) {
    throw new GoogleAdapterError("Gmail message is missing a stable ID", "permanent", null, false);
  }
  if (!googleSubject) {
    throw new GoogleAdapterError("Google subject is required", "invalid_request", null, false);
  }

  const rawHeaders = collectHeaders(message.payload);
  const content = collectMessageContent(message.payload);
  return gmailMessageSchema.parse({
    schemaVersion: 1,
    source: "gmail",
    sourceScope: "personal",
    googleSubject,
    sourceKey: `${googleSubject}:${message.id}`,
    messageId: message.id,
    threadId: message.threadId,
    historyId: message.historyId ?? null,
    internalDate: parseInternalDate(message.internalDate),
    labelIds: [...new Set(message.labelIds ?? [])].sort(),
    snippet: message.snippet ?? "",
    headers: {
      from: firstHeader(rawHeaders, "from"),
      to: firstHeader(rawHeaders, "to"),
      cc: firstHeader(rawHeaders, "cc"),
      bcc: firstHeader(rawHeaders, "bcc"),
      replyTo: firstHeader(rawHeaders, "reply-to"),
      subject: firstHeader(rawHeaders, "subject"),
      date: firstHeader(rawHeaders, "date"),
      messageId: firstHeader(rawHeaders, "message-id"),
      inReplyTo: firstHeader(rawHeaders, "in-reply-to"),
      references: firstHeader(rawHeaders, "references"),
    },
    rawHeaders,
    body: {
      text: joinUnique(content.text),
      html: joinUnique(content.html),
    },
    attachments: content.attachments,
  });
}

export function parseGmailHistoryPage(
  response: gmail_v1.Schema$ListHistoryResponse,
  googleSubject: string,
): GmailHistoryPage {
  if (!response.historyId) {
    throw new GoogleAdapterError("Gmail history response is missing its cursor", "permanent", null, false);
  }
  const changes: GmailHistoryChange[] = [];
  const seen = new Set<string>();

  for (const history of response.history ?? []) {
    if (!history.id) {
      continue;
    }
    appendHistoryChanges(changes, seen, googleSubject, history.id, "message.added", history.messagesAdded);
    appendHistoryChanges(
      changes,
      seen,
      googleSubject,
      history.id,
      "message.deleted",
      history.messagesDeleted,
    );
    appendHistoryChanges(changes, seen, googleSubject, history.id, "labels.added", history.labelsAdded);
    appendHistoryChanges(changes, seen, googleSubject, history.id, "labels.removed", history.labelsRemoved);
  }

  return {
    changes,
    mailboxHistoryId: response.historyId,
    nextPageToken: response.nextPageToken ?? null,
  };
}

export function parseGmailPubSubPush(payload: unknown): GmailPubSubEvent {
  const envelopeSchema = z
    .object({
      message: z
        .object({
          data: z.string().min(1),
          messageId: z.string().min(1),
          publishTime: z.string().min(1),
          deliveryAttempt: z.number().int().positive().optional(),
        })
        .passthrough(),
      subscription: z.string().min(1),
    })
    .passthrough();
  const envelope = envelopeSchema.safeParse(payload);
  if (!envelope.success) {
    throw new GoogleAdapterError("Gmail Pub/Sub envelope is invalid", "invalid_request", null, false);
  }

  let decoded: unknown;
  try {
    decoded = JSON.parse(Buffer.from(envelope.data.message.data, "base64url").toString("utf8"));
  } catch {
    throw new GoogleAdapterError("Gmail Pub/Sub data is invalid", "invalid_request", null, false);
  }
  const notification = z
    .object({
      emailAddress: z.string().email(),
      historyId: z.string().regex(/^\d+$/),
    })
    .strict()
    .safeParse(decoded);
  if (!notification.success) {
    throw new GoogleAdapterError("Gmail Pub/Sub notification is invalid", "invalid_request", null, false);
  }
  const publishedAt = normalizeIsoTimestamp(envelope.data.message.publishTime, "Gmail publish time");

  return gmailPubSubEventSchema.parse({
    schemaVersion: 1,
    source: "gmail",
    sourceScope: "personal",
    providerEventId: `gmail-pubsub:${envelope.data.subscription}:${envelope.data.message.messageId}`,
    subscription: envelope.data.subscription,
    mailboxEmail: notification.data.emailAddress,
    historyId: notification.data.historyId,
    publishedAt,
    deliveryAttempt: envelope.data.message.deliveryAttempt ?? null,
  });
}

function defaultGmailApiFactory(config: GoogleAdapterConfig): GmailApiFactory {
  return (accessToken) => {
    const api = google.gmail({
      version: "v1",
      auth: googleAuthWithAccessToken(config, accessToken),
    });
    return {
      getMessage: (params) => api.users.messages.get(params),
      getAttachment: (params, options) => api.users.messages.attachments.get(params, options),
      listHistory: (params) => api.users.history.list(params),
      listMessages: (params) => api.users.messages.list(params),
      watch: (params) => api.users.watch(params),
      stop: (params) => api.users.stop(params),
    };
  };
}

function collectHeaders(payload: gmail_v1.Schema$MessagePart | null | undefined): Record<string, string[]> {
  const headers: Record<string, string[]> = {};
  for (const header of payload?.headers ?? []) {
    const name = header.name?.trim().toLowerCase();
    if (!name || header.value === null || header.value === undefined) {
      continue;
    }
    const values = headers[name] ?? [];
    values.push(header.value);
    headers[name] = values;
  }
  return headers;
}

function firstHeader(headers: Record<string, string[]>, name: string): string | null {
  return headers[name]?.[0] ?? null;
}

function collectMessageContent(payload: gmail_v1.Schema$MessagePart | null | undefined): {
  text: string[];
  html: string[];
  attachments: z.infer<typeof gmailAttachmentSchema>[];
} {
  const text: string[] = [];
  const html: string[] = [];
  const attachments: z.infer<typeof gmailAttachmentSchema>[] = [];

  const visit = (part: gmail_v1.Schema$MessagePart): void => {
    const mimeType = (part.mimeType ?? "application/octet-stream").toLowerCase();
    const filename = part.filename ?? "";
    const data = part.body?.data ?? null;
    if (mimeType === "text/plain" && data && !filename) {
      text.push(decodeBase64UrlText(data));
    } else if (mimeType === "text/html" && data && !filename) {
      html.push(decodeBase64UrlText(data));
    } else if (filename || part.body?.attachmentId || (data && !mimeType.startsWith("multipart/"))) {
      const partHeaders = collectHeaders(part);
      attachments.push(
        gmailAttachmentSchema.parse({
          partId: part.partId ?? null,
          providerAttachmentId: part.body?.attachmentId ?? null,
          filename,
          mimeType,
          sizeBytes: part.body?.size ?? 0,
          contentId: firstHeader(partHeaders, "content-id"),
          inline: /^inline(?:;|$)/i.test(firstHeader(partHeaders, "content-disposition") ?? ""),
          embeddedDataBase64Url: data,
        }),
      );
    }
    for (const child of part.parts ?? []) {
      visit(child);
    }
  };

  if (payload) {
    visit(payload);
  }
  return { text, html, attachments };
}

function decodeBase64UrlText(data: string): string {
  const bytes = Buffer.from(data, "base64url");
  if (bytes.byteLength > 10 * 1024 * 1024) {
    throw new GoogleAdapterError("Gmail message body exceeds the parser limit", "permanent", null, false);
  }
  return bytes.toString("utf8");
}

function normalizeGmailAttachmentMediaType(value: string): string {
  if (value.length > 255) {
    throw new GmailAttachmentContentError("unsupported_type");
  }
  const [rawMediaType = "", ...parameters] = value.split(";");
  const parsedMediaType = rawMediaType.trim().toLowerCase();
  if (!/^[a-z0-9][a-z0-9!#$&^_.+-]*\/[a-z0-9][a-z0-9!#$&^_.+-]*$/u.test(parsedMediaType)) {
    throw new GmailAttachmentContentError("unsupported_type");
  }
  const mediaType = GMAIL_MEDIA_TYPE_ALIASES.get(parsedMediaType) ?? parsedMediaType;
  if (!GMAIL_IMAGE_MEDIA_TYPES.has(mediaType) && !GMAIL_FILE_MEDIA_TYPES.has(mediaType)) {
    throw new GmailAttachmentContentError("unsupported_type");
  }

  if (GMAIL_TEXT_MEDIA_TYPES.has(mediaType)) {
    for (const parameter of parameters) {
      const trimmed = parameter.trim();
      if (!/^charset\b/iu.test(trimmed)) {
        continue;
      }
      const match = /^charset\s*=\s*(?:"([^"]*)"|([^\s"]+))$/iu.exec(trimmed);
      const charset = (match?.[1] ?? match?.[2] ?? "").toLowerCase();
      if (charset !== "utf-8" && charset !== "utf8") {
        throw new GmailAttachmentContentError("unsupported_type");
      }
    }
  }
  return mediaType;
}

function decodeCanonicalAttachmentBase64Url(data: string, expectedBytes: number): Uint8Array {
  if (data.length > MAX_GMAIL_ATTACHMENT_BASE64URL_CHARACTERS) {
    throw new GmailAttachmentContentError("too_large");
  }
  const expectedCharacters = Math.floor((expectedBytes * 4 + 2) / 3);
  if (data.length !== expectedCharacters || data.length % 4 === 1 || !/^[A-Za-z0-9_-]*$/u.test(data)) {
    throw new GmailAttachmentContentError("invalid_content");
  }

  const decoded = Buffer.from(data, "base64url");
  if (decoded.byteLength > MAX_GMAIL_ATTACHMENT_BYTES) {
    throw new GmailAttachmentContentError("too_large");
  }
  if (decoded.toString("base64url") !== data) {
    throw new GmailAttachmentContentError("invalid_content");
  }
  return Uint8Array.from(decoded);
}

function validateGmailAttachmentBytes(mediaType: string, bytes: Uint8Array): void {
  const valid =
    mediaType === "application/pdf"
      ? startsWithBytes(bytes, [0x25, 0x50, 0x44, 0x46, 0x2d])
      : mediaType === "image/jpeg"
        ? startsWithBytes(bytes, [0xff, 0xd8, 0xff])
        : mediaType === "image/png"
          ? startsWithBytes(bytes, [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])
          : mediaType === "image/gif"
            ? startsWithAscii(bytes, "GIF87a") || startsWithAscii(bytes, "GIF89a")
            : mediaType === "image/webp"
              ? startsWithAscii(bytes, "RIFF") && asciiAt(bytes, 8, "WEBP")
              : validateUtf8TextAttachment(bytes);

  if (!valid) {
    throw new GmailAttachmentContentError("invalid_content");
  }
}

function validateUtf8TextAttachment(bytes: Uint8Array): boolean {
  if (hasRecognizedBinarySignature(bytes)) {
    return false;
  }

  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    return false;
  }
  if (text.includes("\0")) {
    return false;
  }

  const withoutBom = text.replace(/^\uFEFF/u, "");
  if (/^\s*#!/u.test(withoutBom)) {
    return false;
  }
  return !(
    /<\s*(?:[a-z0-9_-]+:)?svg(?:\s|:|\/?>)/iu.test(withoutBom) ||
    /<!doctype\s+html\b/iu.test(withoutBom) ||
    /<\s*\/?(?:[a-z0-9_-]+:)?(?:html|head|body|script|style|iframe|object|embed|form|meta|link|title|base|p|div|span|table|img|a)(?:\s|\/?>)/iu.test(
      withoutBom,
    )
  );
}

function hasRecognizedBinarySignature(bytes: Uint8Array): boolean {
  return (
    startsWithBytes(bytes, [0x25, 0x50, 0x44, 0x46, 0x2d]) ||
    startsWithBytes(bytes, [0xff, 0xd8, 0xff]) ||
    startsWithBytes(bytes, [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]) ||
    startsWithAscii(bytes, "GIF87a") ||
    startsWithAscii(bytes, "GIF89a") ||
    (startsWithAscii(bytes, "RIFF") && asciiAt(bytes, 8, "WEBP")) ||
    startsWithBytes(bytes, [0x50, 0x4b, 0x03, 0x04]) ||
    startsWithBytes(bytes, [0x50, 0x4b, 0x05, 0x06]) ||
    startsWithBytes(bytes, [0x50, 0x4b, 0x07, 0x08]) ||
    startsWithBytes(bytes, [0x1f, 0x8b]) ||
    startsWithBytes(bytes, [0x42, 0x5a, 0x68]) ||
    startsWithBytes(bytes, [0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00]) ||
    startsWithBytes(bytes, [0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c]) ||
    startsWithBytes(bytes, [0x52, 0x61, 0x72, 0x21, 0x1a, 0x07]) ||
    asciiAt(bytes, 257, "ustar") ||
    startsWithAscii(bytes, "MZ") ||
    startsWithBytes(bytes, [0x7f, 0x45, 0x4c, 0x46]) ||
    startsWithBytes(bytes, [0xfe, 0xed, 0xfa, 0xce]) ||
    startsWithBytes(bytes, [0xfe, 0xed, 0xfa, 0xcf]) ||
    startsWithBytes(bytes, [0xce, 0xfa, 0xed, 0xfe]) ||
    startsWithBytes(bytes, [0xcf, 0xfa, 0xed, 0xfe]) ||
    startsWithBytes(bytes, [0xca, 0xfe, 0xba, 0xbe]) ||
    startsWithBytes(bytes, [0x00, 0x61, 0x73, 0x6d])
  );
}

function startsWithAscii(bytes: Uint8Array, value: string): boolean {
  return asciiAt(bytes, 0, value);
}

function asciiAt(bytes: Uint8Array, offset: number, value: string): boolean {
  if (bytes.byteLength < offset + value.length) {
    return false;
  }
  for (let index = 0; index < value.length; index += 1) {
    if (bytes[offset + index] !== value.charCodeAt(index)) {
      return false;
    }
  }
  return true;
}

function startsWithBytes(bytes: Uint8Array, signature: readonly number[]): boolean {
  if (bytes.byteLength < signature.length) {
    return false;
  }
  return signature.every((value, index) => bytes[index] === value);
}

function joinUnique(values: readonly string[]): string | null {
  const unique = [...new Set(values.filter(Boolean))];
  return unique.length ? unique.join("\n") : null;
}

function parseInternalDate(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const milliseconds = Number(value);
  if (!Number.isFinite(milliseconds) || milliseconds < 0) {
    throw new GoogleAdapterError("Gmail internal date is invalid", "permanent", null, false);
  }
  return new Date(milliseconds).toISOString();
}

function appendHistoryChanges(
  output: GmailHistoryChange[],
  seen: Set<string>,
  googleSubject: string,
  historyId: string,
  changeType: GmailHistoryChange["changeType"],
  entries:
    | gmail_v1.Schema$HistoryMessageAdded[]
    | gmail_v1.Schema$HistoryMessageDeleted[]
    | gmail_v1.Schema$HistoryLabelAdded[]
    | gmail_v1.Schema$HistoryLabelRemoved[]
    | null
    | undefined,
): void {
  for (const entry of entries ?? []) {
    const message = entry.message;
    if (!message?.id) {
      continue;
    }
    const labels = "labelIds" in entry && Array.isArray(entry.labelIds) ? [...entry.labelIds].sort() : [];
    const providerEventId = stableGmailHistoryEventId({
      googleSubject,
      historyId,
      changeType,
      messageId: message.id,
      labels,
    });
    if (seen.has(providerEventId)) {
      continue;
    }
    seen.add(providerEventId);
    output.push(
      gmailHistoryChangeSchema.parse({
        schemaVersion: 1,
        source: "gmail",
        sourceScope: "personal",
        googleSubject,
        providerEventId,
        historyId,
        changeType,
        messageId: message.id,
        threadId: message.threadId ?? null,
        labelIds: labels,
      }),
    );
  }
}

function stableGmailHistoryEventId(input: {
  googleSubject: string;
  historyId: string;
  changeType: string;
  messageId: string;
  labels: readonly string[];
}): string {
  const digest = createHash("sha256")
    .update(
      JSON.stringify([input.googleSubject, input.historyId, input.changeType, input.messageId, input.labels]),
    )
    .digest("hex");
  return `gmail-history:${digest}`;
}

function requireAccessToken(token: string): void {
  if (!token) {
    throw new GoogleAdapterError("Google access token is required", "unauthorized", null, false);
  }
}

function requireProviderId(value: string, field: string): void {
  if (!isProviderId(value)) {
    throw new GoogleAdapterError(`${field} is invalid`, "invalid_request", null, false);
  }
}

function isProviderId(value: string): boolean {
  if (value.length === 0 || value.length > 500 || value.trim() !== value) {
    return false;
  }
  for (const character of value) {
    const codePoint = character.codePointAt(0) ?? 0;
    if (codePoint <= 0x1f || codePoint === 0x7f) {
      return false;
    }
  }
  return true;
}

function boundedInteger(value: number, min: number, max: number, field: string): number {
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new GoogleAdapterError(`${field} is invalid`, "invalid_request", null, false);
  }
  return value;
}

function epochMillisecondsToIso(value: string, field: string): string {
  const milliseconds = Number(value);
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) {
    throw new GoogleAdapterError(`${field} is invalid`, "permanent", null, false);
  }
  return new Date(milliseconds).toISOString();
}

function normalizeIsoTimestamp(value: string, field: string): string {
  const timestamp = new Date(value);
  if (!Number.isFinite(timestamp.getTime())) {
    throw new GoogleAdapterError(`${field} is invalid`, "invalid_request", null, false);
  }
  return timestamp.toISOString();
}
