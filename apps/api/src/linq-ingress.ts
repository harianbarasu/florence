import { createHash } from "node:crypto";
import type { EncryptedImageVault } from "@florence/artifacts";
import type { ImageReference } from "@florence/contracts";
import { type InboundDocumentInput, isCarrierMessagesOptOut } from "@florence/database";
import {
  type LinqClient,
  LinqError,
  type LinqInboundMessageProposal,
  type LinqMediaReference,
  type LinqReactionProposal,
  type LinqWebhookHeaders,
  linqIdentitySubjectDigest,
  unwrapLinqWebhook,
} from "@florence/linq";
import type { Florence } from "./florence.js";

const webhookVersion = "2026-02-03";
const supportedImageTypes = new Set<ImageReference["mimeType"]>(["image/jpeg", "image/png", "image/webp"]);
const PDF_MIME_TYPE = "application/pdf";
const PDF_DISCARD_MS = 24 * 60 * 60_000;

type FlorenceIngress = Pick<
  Florence,
  | "resolveLinqAuthority"
  | "respondBeforeEnrollment"
  | "bootstrapMessagesGroup"
  | "acceptInbound"
  | "acceptInboundReaction"
  | "recordLinqObservation"
>;

export type LinqIngressResult =
  | { disposition: "accepted" | "duplicate"; sourceId: string }
  | {
      disposition: "acknowledged";
      reason:
        | "event_not_supported"
        | "message_has_no_supported_content"
        | "channel_stopped"
        | "onboarding_offered"
        | "opted_out"
        | "provider_observation"
        | "reconciled_history";
    }
  | {
      disposition: "rejected";
      reason: "authority_not_found" | "authority_evidence_mismatch" | "unsupported_service";
    };

export interface LinqIngress {
  receive(input: {
    rawBody: Uint8Array;
    headers: LinqWebhookHeaders;
    version: string | undefined;
  }): Promise<LinqIngressResult>;
}

export class LinqIngressError extends Error {
  constructor(
    readonly code: string,
    readonly retryable: boolean,
    options?: ErrorOptions,
  ) {
    super("Florence could not accept the Linq webhook", options);
    this.name = "LinqIngressError";
  }
}

export function createLinqIngress(options: {
  signingSecret: string;
  expectedPartnerId: string;
  linq: LinqClient;
  imageVault: EncryptedImageVault;
  florence: FlorenceIngress;
  now?: () => Date;
}): LinqIngress {
  return {
    async receive(input) {
      try {
        if (input.version !== webhookVersion) {
          throw new LinqIngressError("invalid_webhook_version", false);
        }
        const proposal = unwrapLinqWebhook({
          signingSecret: options.signingSecret,
          expectedPartnerId: options.expectedPartnerId,
          headers: input.headers,
          rawBody: input.rawBody,
          ...(options.now ? { now: options.now() } : {}),
        });
        if (proposal.kind === "reaction" && proposal.service !== "iMessage") {
          return { disposition: "rejected", reason: "unsupported_service" };
        }
        if (proposal.kind === "reaction") {
          if (proposal.isFromMe) {
            await options.florence.recordLinqObservation(proposal);
            return { disposition: "acknowledged", reason: "provider_observation" };
          }
          if (proposal.operation === "removed") {
            return { disposition: "acknowledged", reason: "provider_observation" };
          }
          return acceptReaction(proposal, options);
        }
        if (proposal.kind === "message_status") {
          await options.florence.recordLinqObservation(proposal);
          return { disposition: "acknowledged", reason: "provider_observation" };
        }
        if (proposal.kind !== "inbound_message") {
          return { disposition: "acknowledged", reason: "event_not_supported" };
        }
        if (proposal.reconciledAt !== null) {
          return { disposition: "acknowledged", reason: "reconciled_history" };
        }
        if (proposal.service !== "iMessage") {
          return { disposition: "rejected", reason: "unsupported_service" };
        }
        return acceptMessage(proposal, options);
      } catch (error) {
        if (error instanceof LinqIngressError) throw error;
        if (error instanceof LinqError) {
          throw new LinqIngressError(error.code, error.retryable, { cause: error });
        }
        if (isRetryableError(error)) {
          throw new LinqIngressError("provider_retryable", true, { cause: error });
        }
        if (isPermanentArtifactError(error)) {
          throw new LinqIngressError("unsafe_media", false, { cause: error });
        }
        throw error;
      }
    },
  };
}

async function acceptReaction(
  proposal: LinqReactionProposal,
  options: { linq: LinqClient; florence: FlorenceIngress },
): Promise<LinqIngressResult> {
  if (!proposal.sender || proposal.partIndex !== 0) {
    return { disposition: "rejected", reason: "authority_evidence_mismatch" };
  }
  const observed = await options.linq.observeChat(proposal.providerConversationId);
  const senderIdentitySubjectDigest = linqIdentitySubjectDigest(proposal.sender.providerHandleId);
  const authority = await options.florence.resolveLinqAuthority({
    providerConversationId: proposal.providerConversationId,
    audience: observed.audience,
    participantIdentityDigests: observed.participantIdentityDigests,
    senderIdentitySubjectDigest,
    replyToProviderMessageId: proposal.targetProviderMessageId,
    occurredAt: proposal.occurredAt,
  });
  if (!authority) return { disposition: "rejected", reason: "authority_not_found" };
  if (authority.stopped) return { disposition: "acknowledged", reason: "channel_stopped" };
  const receipt = await options.florence.acceptInboundReaction({
    providerConversationId: proposal.providerConversationId,
    audience: observed.audience,
    participantIdentityDigests: observed.participantIdentityDigests,
    senderIdentitySubjectDigest,
    providerEventId: proposal.providerEventId,
    targetProviderMessageId: proposal.targetProviderMessageId,
    reaction: inboundReaction(proposal),
    partIndex: proposal.partIndex,
    occurredAt: proposal.occurredAt,
  });
  if (!receipt) return { disposition: "rejected", reason: "authority_not_found" };
  if (receipt.disposition === "stopped") {
    return { disposition: "acknowledged", reason: "channel_stopped" };
  }
  return { disposition: receipt.disposition, sourceId: receipt.sourceId };
}

async function acceptMessage(
  proposal: LinqInboundMessageProposal,
  options: {
    linq: LinqClient;
    imageVault: EncryptedImageVault;
    florence: FlorenceIngress;
  },
): Promise<LinqIngressResult> {
  const observed = await options.linq.observeChat(proposal.providerConversationId);
  if (proposal.isGroup !== null && observed.audience !== (proposal.isGroup ? "group" : "private")) {
    return { disposition: "rejected", reason: "authority_evidence_mismatch" };
  }
  const audience = observed.audience;
  const senderIdentitySubjectDigest = linqIdentitySubjectDigest(proposal.sender.providerHandleId);
  const authority = await options.florence.resolveLinqAuthority({
    providerConversationId: proposal.providerConversationId,
    audience,
    participantIdentityDigests: observed.participantIdentityDigests,
    senderIdentitySubjectDigest,
    replyToProviderMessageId: proposal.replyTo?.providerMessageId ?? null,
    occurredAt: proposal.occurredAt,
  });
  if (!authority) {
    return handleUnboundMessage(proposal, observed, options);
  }

  const sourceId = inboundSourceId(proposal.providerEventId);
  if (authority.stopped) {
    return { disposition: "acknowledged", reason: "channel_stopped" };
  }
  if (isCarrierMessagesOptOut(proposal.text)) {
    const receipt = await options.florence.acceptInbound({
      providerConversationId: proposal.providerConversationId,
      audience,
      participantIdentityDigests: observed.participantIdentityDigests,
      senderIdentitySubjectDigest,
      providerEventId: proposal.providerEventId,
      providerMessageId: proposal.providerMessageId,
      replyToProviderMessageId: proposal.replyTo?.providerMessageId ?? null,
      text: proposal.text,
      images: [],
      documents: [],
      occurredAt: proposal.occurredAt,
    });
    if (!receipt) return { disposition: "rejected", reason: "authority_not_found" };
    return receipt.disposition === "duplicate"
      ? { disposition: "duplicate", sourceId: receipt.sourceId }
      : { disposition: "acknowledged", reason: "opted_out" };
  }
  const media = await storeMedia(proposal, authority.householdId, sourceId, options);
  if (proposal.text === null && media.images.length === 0 && media.documents.length === 0) {
    return { disposition: "acknowledged", reason: "message_has_no_supported_content" };
  }
  const receipt = await options.florence.acceptInbound({
    providerConversationId: proposal.providerConversationId,
    audience,
    participantIdentityDigests: observed.participantIdentityDigests,
    senderIdentitySubjectDigest,
    providerEventId: proposal.providerEventId,
    providerMessageId: proposal.providerMessageId,
    replyToProviderMessageId: proposal.replyTo?.providerMessageId ?? null,
    text: proposal.text,
    images: media.images,
    documents: media.documents,
    occurredAt: proposal.occurredAt,
  });
  if (!receipt || receipt.disposition === "stopped") {
    return { disposition: "rejected", reason: "authority_not_found" };
  }
  return { disposition: receipt.disposition, sourceId: receipt.sourceId };
}

async function handleUnboundMessage(
  proposal: LinqInboundMessageProposal,
  observed: { audience: "private" | "group"; participantIdentityDigests: readonly string[] },
  options: {
    florence: FlorenceIngress;
  },
): Promise<LinqIngressResult> {
  const { participantIdentityDigests } = observed;
  const senderIdentitySubjectDigest = linqIdentitySubjectDigest(proposal.sender.providerHandleId);
  if (observed.audience === "private") {
    if (participantIdentityDigests.length !== 1) {
      return { disposition: "rejected", reason: "authority_not_found" };
    }
    if (participantIdentityDigests[0] !== senderIdentitySubjectDigest) {
      return { disposition: "rejected", reason: "authority_evidence_mismatch" };
    }
    if (isCarrierMessagesOptOut(proposal.text)) {
      return { disposition: "acknowledged", reason: "opted_out" };
    }
    if (!proposal.text?.trim() && proposal.media.length === 0) {
      return { disposition: "rejected", reason: "authority_not_found" };
    }
    const offered = await options.florence.respondBeforeEnrollment({
      providerEventId: proposal.providerEventId,
      providerConversationId: proposal.providerConversationId,
      identitySubjectDigest: senderIdentitySubjectDigest,
      text: proposal.text?.trim() || "Shared an attachment.",
      occurredAt: proposal.occurredAt,
    });
    return offered
      ? { disposition: "acknowledged", reason: "onboarding_offered" }
      : { disposition: "rejected", reason: "authority_not_found" };
  }

  if (proposal.media.length > 0 || proposal.text === null || participantIdentityDigests.length !== 2) {
    return { disposition: "rejected", reason: "authority_not_found" };
  }
  if (!participantIdentityDigests.includes(senderIdentitySubjectDigest)) {
    return { disposition: "rejected", reason: "authority_evidence_mismatch" };
  }
  const receipt = await options.florence.bootstrapMessagesGroup({
    providerConversationId: proposal.providerConversationId,
    audience: "group",
    participantIdentityDigests,
    senderIdentitySubjectDigest,
    providerEventId: proposal.providerEventId,
    providerMessageId: proposal.providerMessageId,
    replyToProviderMessageId: proposal.replyTo?.providerMessageId ?? null,
    text: proposal.text,
    occurredAt: proposal.occurredAt,
  });
  return receipt && receipt.disposition !== "stopped"
    ? { disposition: receipt.disposition, sourceId: receipt.sourceId }
    : { disposition: "rejected", reason: "authority_not_found" };
}

async function storeMedia(
  proposal: LinqInboundMessageProposal,
  householdId: string,
  sourceId: string,
  options: { linq: LinqClient; imageVault: EncryptedImageVault },
): Promise<{ images: ImageReference[]; documents: InboundDocumentInput[] }> {
  const imageMedia = proposal.media.filter(
    (item): item is LinqMediaReference & { mimeType: ImageReference["mimeType"] } =>
      supportedImageTypes.has(item.mimeType as ImageReference["mimeType"]),
  );
  const pdfMedia = proposal.media.filter(
    (item): item is LinqMediaReference & { mimeType: typeof PDF_MIME_TYPE } =>
      item.mimeType === PDF_MIME_TYPE,
  );
  if (imageMedia.length > 10 || pdfMedia.length > 3) {
    throw new LinqIngressError("invalid_payload", false);
  }
  const images: ImageReference[] = [];
  for (const item of imageMedia) {
    const fetched = await options.linq.fetchMedia(item);
    if (fetched.mimeType !== item.mimeType) throw new LinqIngressError("unsafe_media", false);
    const stored = await options.imageVault.store({
      assetId: deterministicUuid(`linq-v3\0asset\0${proposal.providerEventId}\0${item.providerAttachmentId}`),
      householdId,
      signalId: sourceId,
      declaredMimeType: item.mimeType,
      bytes: fetched.bytes,
    });
    images.push(stored.image);
  }
  const documents: InboundDocumentInput[] = [];
  const discardAfter = new Date(Date.parse(proposal.occurredAt) + PDF_DISCARD_MS).toISOString();
  for (const item of pdfMedia) {
    const fetched = await options.linq.fetchMedia(item);
    if (fetched.mimeType !== PDF_MIME_TYPE) throw new LinqIngressError("unsafe_media", false);
    const sealed = options.imageVault.sealPdf({
      documentId: deterministicUuid(
        `linq-v3\0document\0${proposal.providerEventId}\0${item.providerAttachmentId}`,
      ),
      householdId,
      signalId: sourceId,
      filename: item.filename,
      declaredMimeType: item.mimeType,
      bytes: fetched.bytes,
      discardAfter,
    });
    documents.push({
      documentId: sealed.documentId,
      externalKey: `linq-v3:${proposal.providerEventId}:${item.providerAttachmentId}`,
      filename: sealed.filename,
      mimeType: sealed.mimeType,
      contentDigest: sealed.contentDigest,
      contentEnvelope: sealed.contentEnvelope,
      discardAfter: sealed.discardAfter,
    });
  }
  return { images, documents };
}

function inboundSourceId(providerEventId: string): string {
  return deterministicUuid(`linq-v3\0signal\0${providerEventId}`);
}

function inboundReaction(proposal: LinqReactionProposal): string {
  if (proposal.reaction !== "custom") return proposal.reaction;
  if (!proposal.customEmoji) return "custom";
  if (proposal.customEmoji.length > 100) throw new LinqIngressError("invalid_payload", false);
  return `custom:${proposal.customEmoji}`;
}

function deterministicUuid(identity: string): string {
  const bytes = createHash("sha256").update(identity).digest().subarray(0, 16);
  bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x50;
  bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function isPermanentArtifactError(error: unknown): error is { retryable: false } {
  return Boolean(
    error &&
      typeof error === "object" &&
      "retryable" in error &&
      (error as { retryable?: unknown }).retryable === false,
  );
}

function isRetryableError(error: unknown): error is { retryable: true } {
  return Boolean(
    error &&
      typeof error === "object" &&
      "retryable" in error &&
      (error as { retryable?: unknown }).retryable === true,
  );
}
