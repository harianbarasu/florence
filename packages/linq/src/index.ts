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
  provider: "linq-v3";
  providerEventId: string;
  providerPartnerId: string;
  providerConversationId: string;
  providerMessageId: string;
  webhookCreatedAt: string;
  occurredAt: string;
  reconciledAt: string | null;
  traceId: string;
  isGroup: boolean | null;
  service: "iMessage" | "RCS" | "SMS";
  sender: { providerHandleId: string; address: string };
  ownerLine: { providerHandleId: string; address: string } | null;
  text: string | null;
  media: readonly LinqMediaReference[];
  replyTo: { providerMessageId: string; partIndex: number | null } | null;
};

export type LinqMessageStatusProposal = {
  kind: "message_status";
  provider: "linq-v3";
  providerEventId: string;
  providerPartnerId: string;
  providerConversationId: string;
  providerMessageId: string;
  idempotencyKey: string | null;
  status: "sent" | "delivered" | "read" | "failed";
  occurredAt: string;
  traceId: string;
  failure: { code: number; reason: string | null } | null;
};

export type LinqReactionProposal = {
  kind: "reaction";
  provider: "linq-v3";
  providerEventId: string;
  providerPartnerId: string;
  providerConversationId: string;
  targetProviderMessageId: string;
  operation: "added" | "removed";
  reaction: LinqReaction | "custom" | "sticker";
  customEmoji: string | null;
  partIndex: number;
  isFromMe: boolean;
  sender: { providerHandleId: string; address: string } | null;
  service: "iMessage" | "RCS" | "SMS";
  occurredAt: string;
  traceId: string;
};

export type LinqIgnoredProposal = {
  kind: "ignored";
  provider: "linq-v3";
  providerEventId: string;
  eventType: string;
  occurredAt: string;
};

export type LinqWebhookProposal =
  | LinqInboundMessageProposal
  | LinqMessageStatusProposal
  | LinqReactionProposal
  | LinqIgnoredProposal;

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
  const createdAt = timestamp(envelope.created_at, "created_at");
  const traceId = string(envelope.trace_id, "trace_id");
  const partnerId = string(envelope.partner_id, "partner_id");
  if (input.expectedPartnerId !== undefined && partnerId !== input.expectedPartnerId) {
    fail("invalid_payload", "Linq webhook partner does not match this endpoint");
  }

  if (eventType === "message.received") {
    return inboundProposal(envelope, { eventId, createdAt, traceId, partnerId });
  }
  if (
    eventType === "message.sent" ||
    eventType === "message.delivered" ||
    eventType === "message.read" ||
    eventType === "message.failed"
  ) {
    return statusProposal(envelope, {
      eventId,
      eventType,
      createdAt,
      traceId,
      partnerId,
    });
  }
  if (eventType === "reaction.added" || eventType === "reaction.removed") {
    return reactionProposal(envelope, {
      eventId,
      eventType,
      createdAt,
      traceId,
      partnerId,
    });
  }
  return { kind: "ignored", provider: "linq-v3", providerEventId: eventId, eventType, occurredAt: createdAt };
}

export type LinqConversationAuthority = {
  audience: "private" | "group";
  participantIdentityDigests: readonly string[];
};

export type LinqMessageReplyTarget = {
  providerMessageId: string;
  partIndex?: number;
};

export type LinqSendMessage = {
  idempotencyKey: string;
  providerConversationId: string;
  expectedAuthority: LinqConversationAuthority;
  text: string;
  replyTo?: LinqMessageReplyTarget | null;
};

export type LinqReaction = "love" | "like" | "dislike" | "laugh" | "emphasize" | "question";

export type LinqSendReaction = {
  /** Persist this key before calling. Linq's reaction endpoint has no idempotency parameter. */
  idempotencyKey: string;
  providerConversationId: string;
  expectedAuthority: LinqConversationAuthority;
  targetProviderMessageId: string;
  partIndex?: number;
  reaction: LinqReaction;
};

export type LinqObservedChat = LinqConversationAuthority;

export type LinqDeliveryResult =
  | {
      status: "committed";
      providerState: "accepted" | "reaction_added";
      idempotencyKey: string;
      providerReceiptId: string;
      detail: null;
      occurredAt: string;
    }
  | {
      status: "failed" | "unknown";
      idempotencyKey: string;
      providerReceiptId: null;
      detail: string;
      occurredAt: string;
    };

export type LinqClientOptions = {
  apiKey: string;
  fetch?: typeof fetch;
  now?: () => Date;
  maximumMediaBytes?: number;
};

export type FetchedLinqMedia = LinqMediaReference & { bytes: Uint8Array };

type ObservedReactionTarget = { hasOwnReaction: boolean };

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

  /** Best-effort presence only. Group typing is deliberately unsupported by Florence. */
  async setTyping(input: {
    providerConversationId: string;
    expectedAuthority: LinqConversationAuthority;
    active: boolean;
  }): Promise<boolean> {
    const conversationId = bounded(input.providerConversationId, "Provider conversation ID", 500);
    const expected = expectedChatAuthority(input.expectedAuthority);
    if (expected?.audience !== "private") return false;
    try {
      const observed = await this.observeChat(conversationId);
      if (!sameChatAuthority(expected, observed)) return false;
      const response = await this.#fetch(
        this.endpoint(`chats/${encodeURIComponent(conversationId)}/typing`),
        {
          method: input.active ? "POST" : "DELETE",
          headers: this.headers(),
          redirect: "error",
        },
      );
      return response.ok;
    } catch {
      return false;
    }
  }

  async sendMessage(input: LinqSendMessage): Promise<LinqDeliveryResult> {
    const conversationId = bounded(input.providerConversationId, "Provider conversation ID", 500);
    const idempotencyKey = bounded(input.idempotencyKey, "Message idempotency key", 255);
    const text = bounded(input.text, "Message text", 10_000);
    const authorityFailure = await this.authorityFailure(
      conversationId,
      expectedChatAuthority(input.expectedAuthority),
      idempotencyKey,
    );
    if (authorityFailure) return authorityFailure;

    const replyTo = input.replyTo
      ? {
          message_id: bounded(input.replyTo.providerMessageId, "Reply target message ID", 500),
          part_index: nonnegativeInteger(input.replyTo.partIndex ?? 0, "Reply target part index"),
        }
      : undefined;
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
            preferred_service: "iMessage",
            ...(replyTo ? { reply_to: replyTo } : {}),
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
        idempotencyKey,
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
      return {
        status: "committed",
        providerState: "accepted",
        idempotencyKey,
        providerReceiptId: receiptId,
        detail: null,
        occurredAt,
      };
    } catch (error) {
      if (error instanceof LinqError && error.retryable) throw error;
      throw retryable("Linq accepted the send but returned an invalid receipt", error);
    }
  }

  async sendReaction(input: LinqSendReaction): Promise<LinqDeliveryResult> {
    const conversationId = bounded(input.providerConversationId, "Provider conversation ID", 500);
    const idempotencyKey = bounded(input.idempotencyKey, "Reaction idempotency key", 255);
    const targetMessageId = bounded(input.targetProviderMessageId, "Reaction target message ID", 500);
    const authorityFailure = await this.authorityFailure(
      conversationId,
      expectedChatAuthority(input.expectedAuthority),
      idempotencyKey,
    );
    if (authorityFailure) return authorityFailure;

    const partIndex =
      input.partIndex === undefined ? 0 : nonnegativeInteger(input.partIndex, "Reaction target part index");
    const desiredReaction = reactionType(input.reaction);
    let before: ObservedReactionTarget;
    try {
      before = await this.readReactionTarget(conversationId, targetMessageId, partIndex, desiredReaction);
    } catch (error) {
      if (error instanceof LinqError && error.retryable) throw error;
      return this.failed(idempotencyKey, "The reaction target is not a message in the authorized chat.");
    }
    if (before.hasOwnReaction) {
      return {
        status: "committed",
        providerState: "reaction_added",
        idempotencyKey,
        providerReceiptId: reactionStateReceipt(targetMessageId, partIndex, desiredReaction),
        detail: null,
        occurredAt: this.#now().toISOString(),
      };
    }

    let response: Response;
    try {
      response = await this.#fetch(
        this.endpoint(`messages/${encodeURIComponent(targetMessageId)}/reactions`),
        {
          method: "POST",
          headers: this.headers(),
          redirect: "error",
          body: JSON.stringify({
            operation: "add",
            type: desiredReaction,
            part_index: partIndex,
          }),
        },
      );
    } catch {
      return this.reconcileReaction(
        conversationId,
        targetMessageId,
        partIndex,
        desiredReaction,
        idempotencyKey,
        null,
        "Linq reaction delivery could not be confirmed.",
      );
    }
    if (retryableStatus(response.status)) {
      return this.reconcileReaction(
        conversationId,
        targetMessageId,
        partIndex,
        desiredReaction,
        idempotencyKey,
        null,
        `Linq reaction delivery could not be confirmed (HTTP ${response.status}).`,
      );
    }
    if (!response.ok) {
      return this.failed(idempotencyKey, `Linq rejected reaction delivery (HTTP ${response.status}).`);
    }
    let traceId: string | null = null;
    try {
      const payload = object(await response.json(), "reaction response");
      literal(payload.status, "accepted", "reaction response status");
      traceId = string(payload.trace_id, "reaction response trace_id");
    } catch {
      // A malformed acknowledgement can still be reconciled from current message state.
    }
    return this.reconcileReaction(
      conversationId,
      targetMessageId,
      partIndex,
      desiredReaction,
      idempotencyKey,
      traceId,
      "Linq accepted the reaction, but the tapback is not yet confirmed.",
    );
  }

  private async reconcileReaction(
    conversationId: string,
    targetMessageId: string,
    partIndex: number,
    reaction: LinqReaction,
    idempotencyKey: string,
    traceId: string | null,
    unknownDetail: string,
  ): Promise<LinqDeliveryResult> {
    try {
      const observed = await this.readReactionTarget(conversationId, targetMessageId, partIndex, reaction);
      if (observed.hasOwnReaction) {
        return {
          status: "committed",
          providerState: "reaction_added",
          idempotencyKey,
          providerReceiptId: traceId ?? reactionStateReceipt(targetMessageId, partIndex, reaction),
          detail: null,
          occurredAt: this.#now().toISOString(),
        };
      }
    } catch {
      // The mutation may have happened. Never retry an unconfirmed reaction.
    }
    return this.unknown(idempotencyKey, unknownDetail);
  }

  private async readReactionTarget(
    conversationId: string,
    targetMessageId: string,
    partIndex: number,
    reaction: LinqReaction,
  ): Promise<ObservedReactionTarget> {
    const response = await this.request(
      this.endpoint(`messages/${encodeURIComponent(targetMessageId)}`),
      { method: "GET", headers: this.headers(), redirect: "error" },
      "verifying a reaction target",
    );
    const message = object(await response.json(), "reaction target message");
    literal(message.id, targetMessageId, "reaction target message id");
    literal(message.chat_id, conversationId, "reaction target chat id");
    const parts = array(message.parts, "reaction target parts");
    const part = parts[partIndex];
    if (part === undefined) fail("invalid_payload", "Linq reaction target part does not exist");
    const reactionsValue = object(part, "reaction target part").reactions;
    const reactions =
      reactionsValue === null || reactionsValue === undefined
        ? []
        : array(reactionsValue, "reaction target part reactions");
    const hasOwnReaction = reactions.some((value) => {
      const candidate = object(value, "reaction target reaction");
      return candidate.is_me === true && candidate.type === reaction;
    });
    return { hasOwnReaction };
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

  private async authorityFailure(
    conversationId: string,
    expected: LinqConversationAuthority | null,
    idempotencyKey: string,
  ): Promise<LinqDeliveryResult | null> {
    if (!expected) {
      return this.failed(idempotencyKey, "Florence did not supply valid current chat authority.");
    }
    let observed: LinqObservedChat;
    try {
      observed = await this.observeChat(conversationId);
    } catch (error) {
      if (error instanceof LinqError && error.retryable) throw error;
      return this.failed(idempotencyKey, "Linq could not verify the current chat authority.");
    }
    return sameChatAuthority(expected, observed)
      ? null
      : this.failed(idempotencyKey, "Linq chat participants no longer match Florence's delivery authority.");
  }

  private failed(idempotencyKey: string, detail: string): LinqDeliveryResult {
    return {
      status: "failed",
      idempotencyKey,
      providerReceiptId: null,
      detail,
      occurredAt: this.#now().toISOString(),
    };
  }

  private unknown(idempotencyKey: string, detail: string): LinqDeliveryResult {
    return {
      status: "unknown",
      idempotencyKey,
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
    if (status !== "active") continue;
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

function expectedChatAuthority(authority: LinqConversationAuthority): LinqObservedChat | null {
  const audience = authority.audience;
  const digests = authority.participantIdentityDigests;
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

function reactionType(value: LinqReaction): LinqReaction {
  if (
    value === "love" ||
    value === "like" ||
    value === "dislike" ||
    value === "laugh" ||
    value === "emphasize" ||
    value === "question"
  ) {
    return value;
  }
  fail("configuration", "Linq reaction type is invalid");
}

function webhookReactionType(value: unknown): LinqReaction | "custom" | "sticker" {
  if (value === "custom" || value === "sticker") return value;
  if (
    value === "love" ||
    value === "like" ||
    value === "dislike" ||
    value === "laugh" ||
    value === "emphasize" ||
    value === "question"
  ) {
    return value;
  }
  fail("invalid_payload", "Linq webhook reaction type is invalid");
}

function reactionStateReceipt(targetMessageId: string, partIndex: number, reaction: LinqReaction): string {
  return `reaction-state:${createHash("sha256")
    .update(`${targetMessageId}\0${partIndex}\0${reaction}`, "utf8")
    .digest("hex")}`;
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

function inboundProposal(
  envelope: Record<string, unknown>,
  common: { eventId: string; createdAt: string; traceId: string; partnerId: string },
): LinqInboundMessageProposal {
  const data = object(envelope.data, "message.received data");
  literal(data.direction, "inbound", "message direction");
  const chat = object(data.chat, "message chat");
  const sender = handle(data.sender_handle, "sender_handle", false);
  const owner =
    chat.owner_handle === null || chat.owner_handle === undefined
      ? null
      : handle(chat.owner_handle, "owner_handle", true);
  const service = serviceType(data.service);
  const parts = array(data.parts, "message parts");
  const textParts: string[] = [];
  const media: LinqMediaReference[] = [];
  for (const rawPart of parts) {
    const part = object(rawPart, "message part");
    const type = string(part.type, "message part type");
    if (type === "text") textParts.push(string(part.value, "text part value"));
    if (type === "link") textParts.push(httpsLink(part.value));
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
  const partIndex = reply?.part_index;
  return {
    kind: "inbound_message",
    provider: "linq-v3",
    providerEventId: common.eventId,
    providerPartnerId: common.partnerId,
    providerConversationId: string(chat.id, "chat id"),
    providerMessageId: string(data.id, "message id"),
    webhookCreatedAt: common.createdAt,
    occurredAt: timestamp(data.sent_at, "message sent_at"),
    reconciledAt:
      data.reconciled_at === null || data.reconciled_at === undefined
        ? null
        : timestamp(data.reconciled_at, "message reconciled_at"),
    traceId: common.traceId,
    isGroup: nullableBoolean(chat.is_group, "chat is_group"),
    service,
    sender: { providerHandleId: sender.id, address: sender.address },
    ownerLine: owner ? { providerHandleId: owner.id, address: owner.address } : null,
    text: textParts.length > 0 ? textParts.join("\n") : null,
    media,
    replyTo: reply
      ? {
          providerMessageId: string(reply.message_id, "reply_to message_id"),
          partIndex:
            partIndex === null || partIndex === undefined
              ? null
              : nonnegativeInteger(partIndex, "Reply part index"),
        }
      : null,
  };
}

function statusProposal(
  envelope: Record<string, unknown>,
  common: {
    eventId: string;
    eventType: "message.sent" | "message.delivered" | "message.read" | "message.failed";
    createdAt: string;
    traceId: string;
    partnerId: string;
  },
): LinqMessageStatusProposal | LinqIgnoredProposal {
  const data = object(envelope.data, `${common.eventType} data`);
  const status = common.eventType.slice("message.".length) as LinqMessageStatusProposal["status"];
  const chatValue = data.chat;
  const chatIdValue =
    chatValue === null || chatValue === undefined
      ? data.chat_id
      : object(chatValue, "message status chat").id;
  const messageIdValue = data.message_id ?? data.id;
  if (!presentString(chatIdValue) || !presentString(messageIdValue)) {
    return {
      kind: "ignored",
      provider: "linq-v3",
      providerEventId: common.eventId,
      eventType: common.eventType,
      occurredAt: common.createdAt,
    };
  }
  const chatId = string(chatIdValue, "message status chat id");
  const messageId = string(messageIdValue, "message status message id");
  const occurredField = status === "failed" ? data.failed_at : data[`${status}_at`];
  const failure =
    status === "failed"
      ? {
          code: nonnegativeInteger(data.code, "failure code"),
          reason: optionalString(data.reason, "failure reason"),
        }
      : null;
  return {
    kind: "message_status",
    provider: "linq-v3",
    providerEventId: common.eventId,
    providerPartnerId: common.partnerId,
    providerConversationId: chatId,
    providerMessageId: messageId,
    idempotencyKey:
      data.idempotency_key === null || data.idempotency_key === undefined
        ? null
        : string(data.idempotency_key, "message status idempotency_key"),
    status,
    occurredAt:
      occurredField === null || occurredField === undefined
        ? common.createdAt
        : timestamp(occurredField, `message ${status}_at`),
    traceId: common.traceId,
    failure,
  };
}

function reactionProposal(
  envelope: Record<string, unknown>,
  common: {
    eventId: string;
    eventType: "reaction.added" | "reaction.removed";
    createdAt: string;
    traceId: string;
    partnerId: string;
  },
): LinqReactionProposal | LinqIgnoredProposal {
  const data = object(envelope.data, `${common.eventType} data`);
  if (!presentString(data.chat_id) || !presentString(data.message_id)) {
    return {
      kind: "ignored",
      provider: "linq-v3",
      providerEventId: common.eventId,
      eventType: common.eventType,
      occurredAt: common.createdAt,
    };
  }
  const isFromMe = boolean(data.is_from_me, "reaction is_from_me");
  const sender =
    data.from_handle === null || data.from_handle === undefined
      ? null
      : handle(data.from_handle, "reaction from_handle", isFromMe);
  return {
    kind: "reaction",
    provider: "linq-v3",
    providerEventId: common.eventId,
    providerPartnerId: common.partnerId,
    providerConversationId: string(data.chat_id, "reaction chat_id"),
    targetProviderMessageId: string(data.message_id, "reaction message_id"),
    operation: common.eventType === "reaction.added" ? "added" : "removed",
    reaction: webhookReactionType(data.reaction_type),
    customEmoji: optionalString(data.custom_emoji, "reaction custom_emoji"),
    partIndex:
      data.part_index === null || data.part_index === undefined
        ? 0
        : nonnegativeInteger(data.part_index, "reaction part_index"),
    isFromMe,
    sender: sender ? { providerHandleId: sender.id, address: sender.address } : null,
    service: serviceType(data.service),
    occurredAt:
      data.reacted_at === null || data.reacted_at === undefined
        ? common.createdAt
        : timestamp(data.reacted_at, "reaction reacted_at"),
    traceId: common.traceId,
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

function handle(value: unknown, name: string, expectedIsMe: boolean): { id: string; address: string } {
  const parsed = object(value, name);
  if (parsed.is_me !== expectedIsMe) fail("invalid_payload", `${name} has an invalid owner marker`);
  return { id: string(parsed.id, `${name} id`), address: string(parsed.handle, `${name} address`) };
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

function optionalString(value: unknown, name: string): string | null {
  return value === null || value === undefined ? null : string(value, name);
}

function presentString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 10_000;
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

function nullableBoolean(value: unknown, name: string): boolean | null {
  return value === null || value === undefined ? null : boolean(value, name);
}

function httpsLink(value: unknown): string {
  const text = string(value, "link part value");
  if (text.length > 2_048) fail("invalid_payload", "Linq link part exceeds 2048 characters");
  let url: URL;
  try {
    url = new URL(text);
  } catch (error) {
    throw new LinqError("invalid_payload", "Linq link part is not a valid URL", false, {
      cause: error,
    });
  }
  if (url.protocol !== "https:" || url.username || url.password) {
    fail("invalid_payload", "Linq link part must use HTTPS");
  }
  return text;
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

function nonnegativeInteger(value: unknown, name: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    fail("invalid_payload", `${name} must be non-negative`);
  }
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
