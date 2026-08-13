import { createHash, createHmac, timingSafeEqual } from "node:crypto";

const LINQ_API_BASE_URL = "https://api.linqapp.com/api/partner/v3";
const LINQ_WEBHOOK_VERSION = "2026-02-03";
const MAX_WEBHOOK_BYTES = 1024 * 1024;
const DEFAULT_MAX_MEDIA_BYTES = 20 * 1024 * 1024;

type HeaderValue = string | readonly string[] | undefined;
export type LinqWebhookHeaders = Headers | Readonly<Record<string, HeaderValue>>;

export type LinqMediaReference = {
  providerAttachmentId: string;
  filename: string;
  mimeType: string;
  sizeBytes: number;
};

/** Provider evidence only. The application must resolve chat and sender authority. */
export type LinqInboundMessageProposal = {
  kind: "inbound_message";
  providerEventId: string;
  providerConversationId: string;
  providerMessageId: string;
  occurredAt: string;
  isGroup: boolean;
  service: "iMessage" | "RCS" | "SMS";
  sender: { providerHandleId: string };
  text: string | null;
  media: readonly LinqMediaReference[];
  replyTo: { providerMessageId: string } | null;
};

export type LinqWebhookProposal = LinqInboundMessageProposal | { kind: "ignored" };

export type UnwrapLinqWebhookInput = {
  signingSecret: string;
  rawBody: string | Uint8Array;
  headers: LinqWebhookHeaders;
  now?: Date;
  toleranceSeconds?: number;
  expectedPartnerId?: string;
};

export type LinqErrorCode =
  | "configuration"
  | "invalid_signature"
  | "stale_webhook"
  | "invalid_payload"
  | "provider_retryable"
  | "provider_rejected"
  | "unsafe_media";

export class LinqError extends Error {
  constructor(
    readonly code: LinqErrorCode,
    message: string,
    readonly retryable = false,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "LinqError";
  }
}

export function unwrapLinqWebhook(input: UnwrapLinqWebhookInput): LinqWebhookProposal {
  const body = rawBytes(input.rawBody);
  if (body.byteLength > MAX_WEBHOOK_BYTES) fail("invalid_payload", "Linq webhook body is too large");

  const webhookId = requiredHeader(input.headers, "webhook-id");
  const timestampText = requiredHeader(input.headers, "webhook-timestamp");
  const signature = requiredHeader(input.headers, "webhook-signature");
  const webhookTimestamp = Number(timestampText);
  const tolerance = positiveInteger(input.toleranceSeconds ?? 300, "Webhook tolerance");
  if (!Number.isSafeInteger(webhookTimestamp) || webhookTimestamp <= 0) {
    fail("invalid_signature", "Linq webhook timestamp is invalid");
  }
  const nowSeconds = Math.floor((input.now ?? new Date()).getTime() / 1000);
  if (Math.abs(nowSeconds - webhookTimestamp) > tolerance) {
    fail("stale_webhook", "Linq webhook timestamp is outside the replay window");
  }
  verifySignature(input.signingSecret, webhookId, timestampText, body, signature);

  const envelope = object(parseJson(body), "webhook envelope");
  const eventId = string(envelope.event_id, "event_id");
  if (eventId !== webhookId) fail("invalid_payload", "Webhook header and payload event IDs differ");
  literal(envelope.api_version, "v3", "api_version");
  literal(envelope.webhook_version, LINQ_WEBHOOK_VERSION, "webhook_version");
  const eventType = string(envelope.event_type, "event_type");
  timestamp(envelope.created_at, "created_at");
  string(envelope.trace_id, "trace_id");
  const partnerId = string(envelope.partner_id, "partner_id");
  if (input.expectedPartnerId !== undefined && partnerId !== input.expectedPartnerId) {
    fail("invalid_payload", "Linq webhook partner does not match this endpoint");
  }

  if (eventType === "message.received") {
    return inboundProposal(envelope, eventId);
  }
  return { kind: "ignored" };
}

type DeliverableMessage = {
  idempotencyKey: string;
  providerConversationId: string;
  payload: { text: string };
  expectedAudience?: "private" | "group";
  expectedParticipantIdentityDigests?: readonly string[];
};

export type LinqObservedChat = {
  audience: "private" | "group";
  participantIdentityDigests: readonly string[];
};

export type LinqExecutionResult =
  | { status: "committed"; providerReceiptId: string; detail: null; occurredAt: string }
  | { status: "failed"; providerReceiptId: null; detail: string; occurredAt: string };

export type LinqClientOptions = {
  apiKey: string;
  fetch?: typeof fetch;
  now?: () => Date;
  maximumMediaBytes?: number;
};

export type FetchedLinqMedia = LinqMediaReference & { bytes: Uint8Array };

/** SHA-256 of the UTF-8 bytes `linq-v3\0${providerHandleId}`. */
export function linqIdentitySubjectDigest(providerHandleId: string): string {
  const id = bounded(providerHandleId, "Linq provider handle ID", 500);
  if (id !== id.trim()) {
    fail("configuration", "Linq provider handle ID must be an exact opaque identifier");
  }
  return createHash("sha256").update(`linq-v3\0${id}`, "utf8").digest("hex");
}

export class LinqClient {
  readonly #apiKey: string;
  readonly #fetch: typeof fetch;
  readonly #now: () => Date;
  readonly #maximumMediaBytes: number;

  constructor(options: LinqClientOptions) {
    this.#apiKey = nonempty(options.apiKey, "Linq API key");
    this.#fetch = options.fetch ?? fetch;
    this.#now = options.now ?? (() => new Date());
    this.#maximumMediaBytes = positiveInteger(
      options.maximumMediaBytes ?? DEFAULT_MAX_MEDIA_BYTES,
      "Maximum media bytes",
    );
  }

  async observeChat(providerConversationId: string): Promise<LinqObservedChat> {
    const conversationId = bounded(providerConversationId, "Provider conversation ID", 500);
    const response = await this.request(
      this.endpoint(`chats/${encodeURIComponent(conversationId)}`),
      { method: "GET", headers: this.headers(), redirect: "error" },
      "verifying chat participants",
    );
    let chat: Record<string, unknown>;
    try {
      chat = object(await response.json(), "chat response");
      literal(chat.id, conversationId, "chat id");
      return observeChatAuthority(chat);
    } catch (error) {
      if (error instanceof LinqError && error.retryable) throw error;
      throw new LinqError("provider_rejected", "Linq returned invalid current chat authority", false, {
        cause: error,
      });
    }
  }

  /** Structurally satisfies Florence worker's EffectExecutor interface. */
  async execute(effect: DeliverableMessage): Promise<LinqExecutionResult> {
    const conversationId = bounded(effect.providerConversationId, "Provider conversation ID", 500);
    const idempotencyKey = bounded(effect.idempotencyKey, "Message idempotency key", 255);
    const text = bounded(effect.payload.text, "Message text", 10_000);
    const expected = expectedChatAuthority(effect);
    if (!expected) {
      return this.failed("Florence did not supply valid current chat authority.");
    }
    let observed: LinqObservedChat;
    try {
      observed = await this.observeChat(conversationId);
    } catch (error) {
      if (error instanceof LinqError && error.retryable) throw error;
      return this.failed("Linq could not verify the current chat authority.");
    }
    if (!sameChatAuthority(expected, observed)) {
      return this.failed("Linq chat participants no longer match Florence's delivery authority.");
    }
    let response: Response;
    try {
      response = await this.#fetch(this.endpoint(`chats/${encodeURIComponent(conversationId)}/messages`), {
        method: "POST",
        headers: this.headers(),
        redirect: "error",
        body: JSON.stringify({
          message: {
            parts: [{ type: "text", value: text }],
            idempotency_key: idempotencyKey,
          },
        }),
      });
    } catch (error) {
      throw retryable("Unable to reach Linq while sending a message", error);
    }
    if (!response.ok) {
      if (retryableStatus(response.status)) {
        throw retryable(`Linq message delivery is temporarily unavailable (HTTP ${response.status})`);
      }
      return {
        status: "failed",
        providerReceiptId: null,
        detail: `Linq rejected message delivery (HTTP ${response.status}).`,
        occurredAt: this.#now().toISOString(),
      };
    }
    let payload: Record<string, unknown>;
    try {
      payload = object(await response.json(), "send response");
      const message = object(payload.message, "send response message");
      const receiptId = string(message.id, "send response message id");
      const occurredAt = timestamp(message.created_at, "send response created_at");
      return { status: "committed", providerReceiptId: receiptId, detail: null, occurredAt };
    } catch (error) {
      if (error instanceof LinqError && error.retryable) throw error;
      throw retryable("Linq accepted the send but returned an invalid receipt", error);
    }
  }

  async fetchMedia(reference: LinqMediaReference): Promise<FetchedLinqMedia> {
    const attachmentId = bounded(reference.providerAttachmentId, "Provider attachment ID", 500);
    const expectedSize = positiveInteger(reference.sizeBytes, "Attachment size");
    if (expectedSize > this.#maximumMediaBytes) unsafe("Linq attachment exceeds the configured limit");
    const metadataResponse = await this.request(
      this.endpoint(`attachments/${encodeURIComponent(attachmentId)}`),
      { method: "GET", headers: this.headers(), redirect: "error" },
      "retrieving attachment metadata",
    );
    let metadata: Record<string, unknown>;
    try {
      metadata = object(await metadataResponse.json(), "attachment metadata");
    } catch (error) {
      throw retryable("Linq returned invalid attachment metadata", error);
    }
    literal(metadata.id, attachmentId, "attachment id");
    literal(metadata.status, "complete", "attachment status");
    const filename = string(metadata.filename, "attachment filename");
    const mimeType = string(metadata.content_type, "attachment content type");
    const sizeBytes = positiveInteger(metadata.size_bytes, "Attachment metadata size");
    if (filename !== reference.filename || mimeType !== reference.mimeType || sizeBytes !== expectedSize) {
      unsafe("Linq attachment metadata differs from the signed webhook");
    }
    if (sizeBytes > this.#maximumMediaBytes) unsafe("Linq attachment exceeds the configured limit");
    const downloadUrl = safeCdnUrl(string(metadata.download_url, "attachment download URL"));
    const mediaResponse = await this.request(
      downloadUrl,
      { method: "GET", headers: { Accept: mimeType }, redirect: "error" },
      "downloading attachment bytes",
    );
    const responseType = mediaResponse.headers.get("content-type")?.split(";", 1)[0]?.trim();
    if (responseType && responseType !== mimeType)
      unsafe("Linq attachment content type changed while downloading");
    const contentLength = mediaResponse.headers.get("content-length");
    if (contentLength !== null) {
      const announcedSize = Number(contentLength);
      if (!Number.isSafeInteger(announcedSize) || announcedSize !== sizeBytes) {
        unsafe("Linq attachment length differs from its metadata");
      }
    }
    const bytes = await boundedBody(mediaResponse, this.#maximumMediaBytes);
    if (bytes.byteLength !== sizeBytes) unsafe("Linq attachment length differs from its metadata");
    return { providerAttachmentId: attachmentId, filename, mimeType, sizeBytes, bytes };
  }

  private endpoint(path: string): URL {
    return new URL(`${LINQ_API_BASE_URL}/${path}`);
  }

  private failed(detail: string): LinqExecutionResult {
    return {
      status: "failed",
      providerReceiptId: null,
      detail,
      occurredAt: this.#now().toISOString(),
    };
  }

  private headers(): Record<string, string> {
    return {
      Accept: "application/json",
      Authorization: `Bearer ${this.#apiKey}`,
      "Content-Type": "application/json",
    };
  }

  private async request(url: URL, init: RequestInit, action: string): Promise<Response> {
    let response: Response;
    try {
      response = await this.#fetch(url, init);
    } catch (error) {
      throw retryable(`Unable to reach Linq while ${action}`, error);
    }
    if (!response.ok) {
      if (retryableStatus(response.status))
        throw retryable(`Linq is temporarily unavailable (HTTP ${response.status})`);
      throw new LinqError("provider_rejected", `Linq rejected ${action} (HTTP ${response.status})`);
    }
    return response;
  }
}

function observeChatAuthority(chat: Record<string, unknown>): LinqObservedChat {
  const audience = boolean(chat.is_group, "chat is_group") ? "group" : "private";
  if (chat.service !== null && chat.service !== undefined && serviceType(chat.service) !== "iMessage") {
    fail("invalid_payload", "Linq chat must use iMessage");
  }
  const handles = array(chat.handles, "chat handles");
  const seenHandleIds = new Set<string>();
  const activeParticipants: string[] = [];
  let activeSelfCount = 0;

  for (const value of handles) {
    const participant = object(value, "chat handle");
    const id = bounded(string(participant.id, "chat handle id"), "Linq provider handle ID", 500);
    if (seenHandleIds.has(id)) fail("invalid_payload", "Linq chat contains a duplicate handle id");
    seenHandleIds.add(id);

    const isMe = boolean(participant.is_me, "chat handle is_me");
    const service = serviceType(participant.service);
    const status = participantStatus(participant.status);
    const leftAt = participant.left_at;
    const hasLeftAt = leftAt !== null && leftAt !== undefined;
    if (hasLeftAt) timestamp(leftAt, "chat handle left_at");
    if ((status === "active") === hasLeftAt) {
      fail("invalid_payload", "Linq chat handle status and left_at are inconsistent");
    }
    if (status !== "active") {
      fail("invalid_payload", "Linq chat contains a former participant and cannot regain Florence authority");
    }
    if (service !== "iMessage") {
      fail("invalid_payload", "Linq chat active handles must use iMessage");
    }
    if (isMe) {
      activeSelfCount += 1;
    } else {
      activeParticipants.push(linqIdentitySubjectDigest(id));
    }
  }

  if (activeSelfCount !== 1) {
    fail("invalid_payload", "Linq chat must have exactly one active owner handle");
  }
  const requiredParticipants = audience === "private" ? 1 : 2;
  if (activeParticipants.length !== requiredParticipants) {
    fail(
      "invalid_payload",
      `Linq ${audience} chat must have exactly ${requiredParticipants} active non-owner participant${requiredParticipants === 1 ? "" : "s"}`,
    );
  }
  activeParticipants.sort();
  return { audience, participantIdentityDigests: activeParticipants };
}

function participantStatus(value: unknown): "active" | "left" | "removed" {
  if (value === "active" || value === "left" || value === "removed") return value;
  fail("invalid_payload", "Linq chat handle status must be active, left, or removed");
}

function expectedChatAuthority(effect: DeliverableMessage): LinqObservedChat | null {
  const audience = effect.expectedAudience;
  const digests = effect.expectedParticipantIdentityDigests;
  if ((audience !== "private" && audience !== "group") || !Array.isArray(digests)) return null;
  const requiredParticipants = audience === "private" ? 1 : 2;
  if (digests.length !== requiredParticipants) return null;
  const unique = new Set<string>();
  for (const digest of digests) {
    if (typeof digest !== "string" || !/^[0-9a-f]{64}$/.test(digest) || unique.has(digest)) return null;
    unique.add(digest);
  }
  return { audience, participantIdentityDigests: [...unique].sort() };
}

function sameChatAuthority(expected: LinqObservedChat, observed: LinqObservedChat): boolean {
  return (
    expected.audience === observed.audience &&
    expected.participantIdentityDigests.length === observed.participantIdentityDigests.length &&
    expected.participantIdentityDigests.every(
      (digest, index) => digest === observed.participantIdentityDigests[index],
    )
  );
}

function inboundProposal(envelope: Record<string, unknown>, eventId: string): LinqInboundMessageProposal {
  const data = object(envelope.data, "message.received data");
  literal(data.direction, "inbound", "message direction");
  const chat = object(data.chat, "message chat");
  const sender = handle(data.sender_handle, "sender_handle", false);
  handle(chat.owner_handle, "owner_handle", true);
  const service = serviceType(data.service);
  const parts = array(data.parts, "message parts");
  const textParts: string[] = [];
  const media: LinqMediaReference[] = [];
  for (const rawPart of parts) {
    const part = object(rawPart, "message part");
    const type = string(part.type, "message part type");
    if (type === "text") textParts.push(string(part.value, "text part value"));
    if (type === "media") {
      media.push({
        providerAttachmentId: string(part.id, "media part id"),
        filename: string(part.filename, "media filename"),
        mimeType: string(part.mime_type, "media MIME type"),
        sizeBytes: positiveInteger(part.size_bytes, "Media size"),
      });
    }
  }
  const reply =
    data.reply_to === null || data.reply_to === undefined ? null : object(data.reply_to, "reply_to");
  return {
    kind: "inbound_message",
    providerEventId: eventId,
    providerConversationId: string(chat.id, "chat id"),
    providerMessageId: string(data.id, "message id"),
    occurredAt: timestamp(data.sent_at, "message sent_at"),
    isGroup: boolean(chat.is_group, "chat is_group"),
    service,
    sender: { providerHandleId: sender.id },
    text: textParts.length > 0 ? textParts.join("\n") : null,
    media,
    replyTo: reply ? { providerMessageId: string(reply.message_id, "reply_to message_id") } : null,
  };
}

function verifySignature(
  secret: string,
  webhookId: string,
  timestampText: string,
  body: Uint8Array,
  header: string,
): void {
  if (!secret.startsWith("whsec_")) fail("configuration", "Linq webhook secret must use whsec_ format");
  const key = decodeBase64(secret.slice("whsec_".length), "Linq webhook secret");
  const expected = createHmac("sha256", key)
    .update(Buffer.from(`${webhookId}.${timestampText}.`, "utf8"))
    .update(body)
    .digest();
  const valid = header
    .trim()
    .split(/\s+/)
    .filter((candidate) => candidate.startsWith("v1,"))
    .some((candidate) => {
      try {
        const provided = decodeBase64(candidate.slice(3), "Linq webhook signature");
        return provided.byteLength === expected.byteLength && timingSafeEqual(provided, expected);
      } catch {
        return false;
      }
    });
  if (!valid) fail("invalid_signature", "Linq webhook signature is invalid");
}

function requiredHeader(headers: LinqWebhookHeaders, name: string): string {
  if (headers instanceof Headers) {
    const value = headers.get(name);
    if (value) return value;
  } else {
    for (const [key, rawValue] of Object.entries(headers)) {
      if (key.toLowerCase() !== name) continue;
      const value = Array.isArray(rawValue) ? rawValue.join(" ") : rawValue;
      if (typeof value === "string" && value.trim()) return value;
    }
  }
  fail("invalid_signature", `Missing ${name} header`);
}

function parseJson(body: Uint8Array): unknown {
  try {
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body));
  } catch (error) {
    throw new LinqError("invalid_payload", "Linq webhook body is not valid UTF-8 JSON", false, {
      cause: error,
    });
  }
}

function rawBytes(body: string | Uint8Array): Uint8Array {
  return typeof body === "string" ? new TextEncoder().encode(body) : body;
}

function safeCdnUrl(value: string): URL {
  let url: URL;
  try {
    url = new URL(value);
  } catch (error) {
    throw new LinqError("unsafe_media", "Linq returned an invalid attachment URL", false, { cause: error });
  }
  if (
    url.protocol !== "https:" ||
    url.hostname !== "cdn.linqapp.com" ||
    url.username ||
    url.password ||
    (url.port && url.port !== "443")
  ) {
    unsafe("Linq attachment URL is outside the approved CDN");
  }
  return url;
}

async function boundedBody(response: Response, maximumBytes: number): Promise<Uint8Array> {
  if (!response.body) unsafe("Linq attachment response had no body");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > maximumBytes) {
        await reader.cancel();
        unsafe("Linq attachment exceeds the configured limit");
      }
      chunks.push(value);
    }
  } catch (error) {
    if (error instanceof LinqError) throw error;
    throw retryable("Linq attachment download was interrupted", error);
  }
  const joined = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    joined.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return joined;
}

function decodeBase64(value: string, name: string): Buffer {
  if (!value || value.length % 4 !== 0 || !/^[A-Za-z0-9+/]+={0,2}$/.test(value)) {
    fail("invalid_signature", `${name} is not valid base64`);
  }
  return Buffer.from(value, "base64");
}

function handle(value: unknown, name: string, expectedIsMe: boolean): { id: string } {
  const parsed = object(value, name);
  if (parsed.is_me !== expectedIsMe) fail("invalid_payload", `${name} has an invalid owner marker`);
  string(parsed.handle, `${name} address`);
  return { id: string(parsed.id, `${name} id`) };
}

function serviceType(value: unknown): "iMessage" | "RCS" | "SMS" {
  if (value === "iMessage" || value === "RCS" || value === "SMS") return value;
  fail("invalid_payload", "Linq message service is invalid");
}

function object(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    fail("invalid_payload", `Linq ${name} must be an object`);
  }
  return value as Record<string, unknown>;
}

function array(value: unknown, name: string): readonly unknown[] {
  if (!Array.isArray(value)) fail("invalid_payload", `Linq ${name} must be an array`);
  return value;
}

function string(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length === 0 || value.length > 10_000) {
    fail("invalid_payload", `Linq ${name} must be a non-empty string`);
  }
  return value;
}

function bounded(value: unknown, name: string, maximum: number): string {
  const parsed = nonempty(value, name);
  if (parsed.length > maximum) fail("configuration", `${name} exceeds ${maximum} characters`);
  return parsed;
}

function nonempty(value: unknown, name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    fail("configuration", `${name} is required`);
  }
  return value;
}

function boolean(value: unknown, name: string): boolean {
  if (typeof value !== "boolean") fail("invalid_payload", `Linq ${name} must be boolean`);
  return value;
}

function timestamp(value: unknown, name: string): string {
  const parsed = string(value, name);
  if (Number.isNaN(Date.parse(parsed))) fail("invalid_payload", `Linq ${name} must be a timestamp`);
  return parsed;
}

function literal(value: unknown, expected: string, name: string): void {
  if (value !== expected) fail("invalid_payload", `Linq ${name} must be ${expected}`);
}

function positiveInteger(value: unknown, name: string): number {
  if (!Number.isSafeInteger(value) || (value as number) <= 0)
    fail("invalid_payload", `${name} must be positive`);
  return value as number;
}

function retryableStatus(status: number): boolean {
  return status === 408 || status === 425 || status === 429 || status >= 500;
}

function retryable(message: string, cause?: unknown): LinqError {
  return new LinqError("provider_retryable", message, true, cause === undefined ? undefined : { cause });
}

function unsafe(message: string): never {
  throw new LinqError("unsafe_media", message);
}

function fail(code: LinqErrorCode, message: string): never {
  throw new LinqError(code, message);
}
