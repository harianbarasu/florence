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
import type { FlorenceVoiceNoteInput } from "./reasoner.js";

const webhookVersion = "2026-02-03";
const supportedImageTypes = new Set<ImageReference["mimeType"]>([
  "image/jpeg",
  "image/png",
  "image/webp",
  "image/heic",
]);
const supportedAudioTypes = new Set<FlorenceVoiceNoteInput["mimeType"]>([
  "audio/flac",
  "audio/x-flac",
  "audio/aac",
  "audio/aiff",
  "audio/x-aiff",
  "audio/amr",
  "audio/x-caf",
  "audio/m4a",
  "audio/x-m4a",
  "audio/mp4",
  "audio/mp3",
  "audio/mpeg",
  "audio/ogg",
  "audio/wav",
  "audio/x-wav",
  "audio/webm",
]);
const PDF_MIME_TYPE = "application/pdf";
const PDF_DISCARD_MS = 24 * 60 * 60_000;
const MAX_MEDIA_BYTES = 20 * 1024 * 1024;
const MAX_INBOUND_TEXT_CHARS = 20_000;
const noSupportedInboundContent = Symbol("no-supported-inbound-content");

type FlorenceIngress = Pick<
  Florence,
  | "resolveLinqAuthority"
  | "respondBeforeEnrollment"
  | "reconcileObservedFamilyGroup"
  | "acceptInbound"
  | "acceptInboundWithPreparation"
  | "acceptInboundReaction"
  | "recordLinqObservation"
  | "transcribeVoiceNote"
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
        | "family_group_repairing"
        | "retired_group"
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
          return await acceptReaction(proposal, options);
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
        return await acceptMessage(proposal, options);
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
  const groupDisposition = await reconcileObservedGroup(
    proposal.providerConversationId,
    proposal.occurredAt,
    observed,
    options.florence,
  );
  if (groupDisposition) return groupDisposition;
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
  const groupDisposition = await reconcileObservedGroup(
    proposal.providerConversationId,
    proposal.occurredAt,
    observed,
    options.florence,
  );
  if (groupDisposition) return groupDisposition;
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

  const envelope = {
    providerConversationId: proposal.providerConversationId,
    audience,
    participantIdentityDigests: observed.participantIdentityDigests,
    senderIdentitySubjectDigest,
    providerEventId: proposal.providerEventId,
    providerMessageId: proposal.providerMessageId,
    replyToProviderMessageId: proposal.replyTo?.providerMessageId ?? null,
    occurredAt: proposal.occurredAt,
    providerPayloadDigest: canonicalProviderPayloadDigest(
      proposal,
      audience,
      observed.participantIdentityDigests,
      senderIdentitySubjectDigest,
    ),
  };
  if (isCarrierMessagesOptOut(proposal.text)) {
    const receipt = await options.florence.acceptInboundWithPreparation(envelope, async () => ({
      text: proposal.text,
      authoredText: proposal.text?.trim() || null,
      voiceTranscriptPresent: false,
      images: [],
      documents: [],
    }));
    if (!receipt) return { disposition: "rejected", reason: "authority_not_found" };
    return receipt.disposition === "duplicate"
      ? { disposition: "duplicate", sourceId: receipt.sourceId }
      : { disposition: "acknowledged", reason: "opted_out" };
  }
  let receipt: Awaited<ReturnType<FlorenceIngress["acceptInboundWithPreparation"]>>;
  try {
    receipt = await options.florence.acceptInboundWithPreparation(
      envelope,
      async ({ householdId, sourceId }) => {
        if (!hasSupportedContent(proposal)) throw noSupportedInboundContent;
        const media = await storeMedia(proposal, householdId, sourceId, options);
        return {
          text: inboundTextWithVoiceTranscripts(proposal.text, media.voiceTranscripts),
          authoredText: proposal.text?.trim() || null,
          voiceTranscriptPresent: media.voiceTranscripts.length > 0,
          images: media.images,
          documents: media.documents,
        };
      },
    );
  } catch (error) {
    if (error === noSupportedInboundContent) {
      return { disposition: "acknowledged", reason: "message_has_no_supported_content" };
    }
    throw error;
  }
  if (!receipt) {
    return { disposition: "rejected", reason: "authority_not_found" };
  }
  if (receipt.disposition === "stopped") {
    return { disposition: "acknowledged", reason: "channel_stopped" };
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
    const carrierOptOut = isCarrierMessagesOptOut(proposal.text);
    if (!proposal.text?.trim() && proposal.media.length === 0) {
      return { disposition: "rejected", reason: "authority_not_found" };
    }
    const offered = await options.florence.respondBeforeEnrollment({
      providerEventId: proposal.providerEventId,
      providerConversationId: proposal.providerConversationId,
      identitySubjectDigest: senderIdentitySubjectDigest,
      text: proposal.text?.trim() || "Shared an attachment.",
      occurredAt: proposal.occurredAt,
      carrierOptOut,
    });
    return offered
      ? { disposition: "acknowledged", reason: carrierOptOut ? "opted_out" : "onboarding_offered" }
      : { disposition: "rejected", reason: "authority_not_found" };
  }

  return { disposition: "rejected", reason: "authority_not_found" };
}

async function reconcileObservedGroup(
  providerConversationId: string,
  occurredAt: string,
  observed: { audience: "private" | "group"; participantIdentityDigests: readonly string[] },
  florence: FlorenceIngress,
): Promise<LinqIngressResult | null> {
  const result = await florence.reconcileObservedFamilyGroup({
    providerConversationId,
    audience: observed.audience,
    participantIdentityDigests: observed.participantIdentityDigests,
    occurredAt,
  });
  if (result === "mismatch") {
    return { disposition: "acknowledged", reason: "family_group_repairing" };
  }
  if (result === "retired") {
    return { disposition: "acknowledged", reason: "retired_group" };
  }
  return null;
}

async function storeMedia(
  proposal: LinqInboundMessageProposal,
  householdId: string,
  sourceId: string,
  options: { linq: LinqClient; imageVault: EncryptedImageVault; florence: FlorenceIngress },
): Promise<{
  images: ImageReference[];
  documents: InboundDocumentInput[];
  voiceTranscripts: string[];
}> {
  const imageMedia = proposal.media.filter(
    (item): item is LinqMediaReference & { mimeType: ImageReference["mimeType"] } =>
      supportedImageTypes.has(item.mimeType as ImageReference["mimeType"]),
  );
  const pdfMedia = proposal.media.filter(
    (item): item is LinqMediaReference & { mimeType: typeof PDF_MIME_TYPE } =>
      item.mimeType === PDF_MIME_TYPE,
  );
  const audioMedia = proposal.media.filter(
    (item): item is LinqMediaReference & { mimeType: FlorenceVoiceNoteInput["mimeType"] } =>
      supportedAudioTypes.has(item.mimeType as FlorenceVoiceNoteInput["mimeType"]),
  );
  if (imageMedia.length > 10 || pdfMedia.length > 3 || audioMedia.length > 3) {
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
  const voiceTranscripts: string[] = [];
  for (const item of audioMedia) {
    if (item.sizeBytes > MAX_MEDIA_BYTES) throw new LinqIngressError("invalid_payload", false);
    const fetched = await options.linq.fetchMedia(item);
    if (
      fetched.mimeType !== item.mimeType ||
      fetched.bytes.byteLength < 1 ||
      fetched.bytes.byteLength > MAX_MEDIA_BYTES
    ) {
      throw new LinqIngressError("unsafe_media", false);
    }
    voiceTranscripts.push(
      await options.florence.transcribeVoiceNote({
        filename: item.filename,
        mimeType: item.mimeType,
        bytes: fetched.bytes,
      }),
    );
  }
  return { images, documents, voiceTranscripts };
}

function hasSupportedContent(proposal: LinqInboundMessageProposal): boolean {
  return (
    proposal.text !== null ||
    proposal.media.some(
      (item) =>
        supportedImageTypes.has(item.mimeType as ImageReference["mimeType"]) ||
        item.mimeType === PDF_MIME_TYPE ||
        supportedAudioTypes.has(item.mimeType as FlorenceVoiceNoteInput["mimeType"]),
    )
  );
}

function canonicalProviderPayloadDigest(
  proposal: LinqInboundMessageProposal,
  audience: "private" | "group",
  participantIdentityDigests: readonly string[],
  senderIdentitySubjectDigest: string,
): string {
  const semanticPayload = [
    "linq-inbound-v1",
    proposal.providerEventId,
    proposal.providerConversationId,
    proposal.providerMessageId,
    senderIdentitySubjectDigest,
    audience,
    [...participantIdentityDigests].sort(),
    proposal.replyTo ? [proposal.replyTo.providerMessageId, proposal.replyTo.partIndex] : null,
    proposal.occurredAt,
    proposal.text,
    proposal.media.map((item) => [item.providerAttachmentId, item.filename, item.mimeType, item.sizeBytes]),
  ];
  return createHash("sha256").update(JSON.stringify(semanticPayload), "utf8").digest("hex");
}

function inboundTextWithVoiceTranscripts(
  text: string | null,
  voiceTranscripts: readonly string[],
): string | null {
  const parentText = text?.trim() ?? "";
  if (voiceTranscripts.length === 0) return text;
  const label =
    voiceTranscripts.length === 1
      ? "[Automatic voice-note transcript]"
      : `[Automatic transcripts of ${voiceTranscripts.length} voice notes]`;
  const body = voiceTranscripts.join("\n\n---\n\n");
  const prefix = `${parentText ? `${parentText}\n\n` : ""}${label}\n`;
  const shortenedMarker = "\n[Transcript shortened]";
  const available = MAX_INBOUND_TEXT_CHARS - prefix.length;
  if (available < 200) throw new LinqIngressError("invalid_payload", false);
  if (body.length <= available) return `${prefix}${body}`;
  return `${prefix}${body.slice(0, available - shortenedMarker.length).trimEnd()}${shortenedMarker}`;
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
