import { createHash } from "node:crypto";
import type { HouseholdSignal, ImageReference } from "@florence/contracts";
import { householdSignalSchema } from "@florence/contracts";
import type { HouseholdChiefOfStaff } from "@florence/control-plane";
import {
  LinqError,
  type LinqInboundMessageProposal,
  type LinqMediaReference,
  type LinqObservedChat,
  type LinqWebhookHeaders,
  linqIdentitySubjectDigest,
  unwrapLinqWebhook,
} from "@florence/linq";
import type { EnrollmentCodes } from "./enrollment.js";

const webhookVersion = "2026-02-03";
const supportedImageTypes = new Set<ImageReference["mimeType"]>([
  "image/jpeg",
  "image/png",
  "image/webp",
  "image/heic",
]);

export type LinqIngressAuthority = {
  householdId: string;
  conversationId: string;
  audience: "private" | "group";
  authorityVersion: number;
  participantSetDigest: string;
  expectedParticipantIdentityDigests: readonly string[];
  senderAdultId: string;
  replyToSignalId: string | null;
};

export interface LinqIngressAuthorityResolver {
  resolveLinqIngressAuthority(input: {
    providerConversationId: string;
    providerHandleId: string;
    replyToProviderMessageId: string | null;
    occurredAt: string;
  }): Promise<LinqIngressAuthority | null>;
}

export interface LinqProviderReader {
  observeChat(providerConversationId: string): Promise<LinqObservedChat>;
  fetchMedia(reference: LinqMediaReference): Promise<{ bytes: Uint8Array; mimeType: string }>;
}

type LinqImageVault = {
  store(input: {
    assetId: string;
    householdId: string;
    signalId: string;
    declaredMimeType: ImageReference["mimeType"];
    bytes: Uint8Array;
  }): Promise<{ image: ImageReference }>;
};

type SignalAcceptor = Pick<HouseholdChiefOfStaff, "accept">;

export type LinqIngressResult =
  | { disposition: "accepted" | "duplicate"; signalId: string }
  | {
      disposition: "acknowledged";
      reason: "event_not_supported" | "message_has_no_supported_content";
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
    super("Linq ingress could not process the webhook", options);
    this.name = "LinqIngressError";
  }
}

export function createLinqIngress(options: {
  signingSecret: string;
  expectedPartnerId: string;
  authorityResolver: LinqIngressAuthorityResolver;
  providerReader: LinqProviderReader;
  imageVault: LinqImageVault;
  chiefOfStaff: SignalAcceptor;
  enrollmentCodes?: EnrollmentCodes;
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
        if (proposal.kind === "ignored") {
          return { disposition: "acknowledged", reason: "event_not_supported" };
        }
        if (proposal.service !== "iMessage") {
          return { disposition: "rejected", reason: "unsupported_service" };
        }
        return await acceptInbound(proposal, options);
      } catch (error) {
        if (error instanceof LinqIngressError) throw error;
        if (error instanceof LinqError) {
          throw new LinqIngressError(error.code, error.retryable, { cause: error });
        }
        if (isRetryableError(error)) {
          throw new LinqIngressError("provider_retryable", true, { cause: error });
        }
        if (isPermanentImageError(error)) {
          throw new LinqIngressError("unsafe_media", false, { cause: error });
        }
        throw error;
      }
    },
  };
}

async function acceptInbound(
  proposal: LinqInboundMessageProposal,
  options: {
    authorityResolver: LinqIngressAuthorityResolver;
    providerReader: LinqProviderReader;
    imageVault: LinqImageVault;
    chiefOfStaff: SignalAcceptor;
    enrollmentCodes?: EnrollmentCodes;
  },
): Promise<LinqIngressResult> {
  const authority = await options.authorityResolver.resolveLinqIngressAuthority({
    providerConversationId: proposal.providerConversationId,
    providerHandleId: proposal.sender.providerHandleId,
    replyToProviderMessageId: proposal.replyTo?.providerMessageId ?? null,
    occurredAt: proposal.occurredAt,
  });
  if (!authority) return establishAuthority(proposal, options);

  const providerAudience = proposal.isGroup ? "group" : "private";
  if (providerAudience !== authority.audience) {
    return { disposition: "rejected", reason: "authority_evidence_mismatch" };
  }
  const observed = await options.providerReader.observeChat(proposal.providerConversationId);
  if (
    observed.audience !== authority.audience ||
    !sameSortedDigests(authority.expectedParticipantIdentityDigests, observed.participantIdentityDigests)
  ) {
    return { disposition: "rejected", reason: "authority_evidence_mismatch" };
  }

  const signalId = deterministicUuid(`linq-v3\0signal\0${proposal.providerEventId}`);
  const supportedMedia = proposal.media.flatMap((media) =>
    isSupportedImageType(media.mimeType) ? [{ ...media, mimeType: media.mimeType }] : [],
  );
  if (supportedMedia.length > 10) throw new LinqIngressError("invalid_payload", false);
  if (proposal.text === null && supportedMedia.length === 0) {
    return { disposition: "acknowledged", reason: "message_has_no_supported_content" };
  }
  const images: ImageReference[] = [];
  for (const media of supportedMedia) {
    const assetId = deterministicUuid(
      `linq-v3\0asset\0${proposal.providerEventId}\0${media.providerAttachmentId}`,
    );
    const fetched = await options.providerReader.fetchMedia(media);
    if (fetched.mimeType !== media.mimeType) {
      throw new LinqIngressError("unsafe_media", false);
    }
    const stored = await options.imageVault.store({
      assetId,
      householdId: authority.householdId,
      signalId,
      declaredMimeType: media.mimeType,
      bytes: fetched.bytes,
    });
    images.push(stored.image);
  }

  const candidate = {
    type: "conversation.message",
    signalId,
    householdId: authority.householdId,
    idempotencyKey: `linq-v3:${signalId}`,
    occurredAt: proposal.occurredAt,
    conversationId: authority.conversationId,
    audience: authority.audience,
    authorityVersion: authority.authorityVersion,
    participantSetDigest: authority.participantSetDigest,
    senderAdultId: authority.senderAdultId,
    text: proposal.text,
    images,
    replyToSignalId: authority.replyToSignalId,
    source: {
      system: "linq-v3",
      providerEventId: proposal.providerEventId,
      providerMessageId: proposal.providerMessageId,
    },
  };
  const parsed = householdSignalSchema.safeParse(candidate);
  if (!parsed.success) throw new LinqIngressError("invalid_payload", false, { cause: parsed.error });
  const signal: HouseholdSignal = parsed.data;
  const receipt = await options.chiefOfStaff.accept(signal);
  return { disposition: receipt.disposition, signalId };
}

async function establishAuthority(
  proposal: LinqInboundMessageProposal,
  options: {
    providerReader: LinqProviderReader;
    chiefOfStaff: SignalAcceptor;
    enrollmentCodes?: EnrollmentCodes;
  },
): Promise<LinqIngressResult> {
  if (!proposal.isGroup) return redeemEnrollment(proposal, options);
  if (proposal.media.length > 0 || proposal.text === null) {
    return { disposition: "rejected", reason: "authority_not_found" };
  }
  const observed = await options.providerReader.observeChat(proposal.providerConversationId);
  const senderIdentitySubjectDigest = linqIdentitySubjectDigest(proposal.sender.providerHandleId);
  if (
    observed.audience !== "group" ||
    observed.participantIdentityDigests.length !== 2 ||
    !observed.participantIdentityDigests.includes(senderIdentitySubjectDigest)
  ) {
    return { disposition: "rejected", reason: "authority_evidence_mismatch" };
  }
  const bindingSignalId = deterministicUuid(`linq-v3\0group-binding\0${proposal.providerConversationId}`);
  const messageSignalId = deterministicUuid(`linq-v3\0signal\0${proposal.providerEventId}`);
  const receipt = await options.chiefOfStaff.accept({
    command: "linq.group.bootstrap",
    input: {
      bindingSignalId,
      bindingIdempotencyKey: `linq-v3:group-binding:${bindingSignalId}`,
      messageSignalId,
      messageIdempotencyKey: `linq-v3:${messageSignalId}`,
      occurredAt: proposal.occurredAt,
      providerConversationId: proposal.providerConversationId,
      participantIdentityDigests: observed.participantIdentityDigests,
      senderIdentitySubjectDigest,
      text: proposal.text,
      providerEventId: proposal.providerEventId,
      providerMessageId: proposal.providerMessageId,
    },
  });
  return receipt
    ? { disposition: receipt.disposition, signalId: receipt.signalId }
    : { disposition: "rejected", reason: "authority_not_found" };
}

async function redeemEnrollment(
  proposal: LinqInboundMessageProposal,
  options: {
    providerReader: LinqProviderReader;
    chiefOfStaff: SignalAcceptor;
    enrollmentCodes?: EnrollmentCodes;
  },
): Promise<LinqIngressResult> {
  if (proposal.isGroup || proposal.media.length > 0) {
    return { disposition: "rejected", reason: "authority_not_found" };
  }
  const challengeDigest = options.enrollmentCodes?.digestCandidate(proposal.text) ?? null;
  if (!challengeDigest) return { disposition: "rejected", reason: "authority_not_found" };

  const identitySubjectDigest = linqIdentitySubjectDigest(proposal.sender.providerHandleId);
  const observed = await options.providerReader.observeChat(proposal.providerConversationId);
  if (
    observed.audience !== "private" ||
    observed.participantIdentityDigests.length !== 1 ||
    observed.participantIdentityDigests[0] !== identitySubjectDigest
  ) {
    return { disposition: "rejected", reason: "authority_evidence_mismatch" };
  }
  const signalId = deterministicUuid(`linq-v3\0enrollment\0${proposal.providerEventId}`);
  const receipt = await options.chiefOfStaff.accept({
    command: "linq.enrollment.redeem",
    input: {
      signalId,
      idempotencyKey: `linq-v3:enrollment:${signalId}`,
      occurredAt: proposal.occurredAt,
      challengeDigest,
      identitySubjectDigest,
      consentVersion: "linq-private-code-v1",
      consentedAt: proposal.occurredAt,
      providerConversationId: proposal.providerConversationId,
    },
  });
  return receipt
    ? { disposition: receipt.disposition, signalId: receipt.signalId }
    : { disposition: "rejected", reason: "authority_not_found" };
}

function deterministicUuid(identity: string): string {
  const bytes = createHash("sha256").update(identity).digest().subarray(0, 16);
  bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x50;
  bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function isSupportedImageType(value: string): value is ImageReference["mimeType"] {
  return supportedImageTypes.has(value as ImageReference["mimeType"]);
}

function isPermanentImageError(error: unknown): error is { retryable: false } {
  return (
    typeof error === "object" &&
    error !== null &&
    "retryable" in error &&
    (error as { retryable?: unknown }).retryable === false
  );
}

function isRetryableError(error: unknown): error is { retryable: true } {
  return (
    typeof error === "object" &&
    error !== null &&
    "retryable" in error &&
    (error as { retryable?: unknown }).retryable === true
  );
}

function sameSortedDigests(expected: readonly string[], observed: readonly string[]): boolean {
  return expected.length === observed.length && expected.every((digest, index) => digest === observed[index]);
}
