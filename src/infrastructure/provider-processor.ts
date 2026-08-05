import { createHash } from "node:crypto";
import { type GmailPubSubEvent, gmailPubSubEventSchema } from "../adapters/google/index.js";
import {
  LinqApiError,
  LinqAttachmentContentError,
  type LinqAttachmentReader,
  type LinqChatReader,
  type LinqInboundEvent,
  linqInboundEventSchema,
} from "../adapters/linq/index.js";
import type {
  ConversationAttachmentContent,
  FlorenceApplication,
  HouseholdApplicationSnapshot,
} from "../application/index.js";
import type { ChannelResolution, ClaimedProviderInboxItem } from "../db/application-store.js";
import { AdultIdSchema } from "../domain/index.js";
import { canonicalizeLinqHandle, type GroupIdentity, type PendingInvitation } from "./runtime-store.js";

export type ProviderProcessingResult = {
  householdId?: string;
  resolution: Record<string, unknown>;
};

export interface GooglePushProcessor {
  processPush(event: GmailPubSubEvent): Promise<{
    householdId: string;
    status: string;
    phase: string;
    processedMessages: number;
  }>;
}

export interface PrivateCommandHandler {
  handle(input: {
    householdId: string;
    adultId: string;
    channelId: string;
    messageId: string;
    text: string;
    occurredAt: string;
    idempotencyKey: string;
  }): Promise<{ handled: boolean; classification?: string }>;
}

export class ProviderProcessingError extends Error {
  public override readonly name = "ProviderProcessingError";

  public constructor(
    public readonly code: string,
    public readonly retryable: boolean,
    message: string,
  ) {
    super(message);
  }
}

export interface ProductionProviderProcessorOptions {
  application: FlorenceApplication;
  applicationStore: ProviderApplicationStore;
  runtimeStore: ProviderRuntimeStore;
  linqChats: LinqChatReader;
  linqAttachments: LinqAttachmentReader;
  google: GooglePushProcessor;
  privateCommands?: PrivateCommandHandler;
  defaultTimeZone: string;
}

export interface ProviderApplicationStore {
  resolveChannel(input: {
    provider: "linq";
    externalChatId: string;
    externalHandle?: string;
  }): Promise<ChannelResolution | null>;
  load(householdId: string): Promise<HouseholdApplicationSnapshot | null>;
}

export interface ProviderRuntimeStore {
  setSuppression(input: {
    externalChatId: string;
    externalHandle?: string;
    scope: "private" | "group";
    suppressed: boolean;
    occurredAt: string;
    sourceEventId: string;
    reason: string;
  }): Promise<{ applied: boolean; suppressed: boolean }>;
  isSuppressed(externalChatId: string, externalHandle?: string): Promise<boolean>;
  pauseGroupBinding(input: { externalChatId: string; reason: string }): Promise<boolean>;
  findPendingInvitation(inviteeHandle: string): Promise<PendingInvitation | null>;
  bindPendingInvitee(input: {
    invitation: PendingInvitation;
    externalChatId: string;
    externalHandle: string;
    occurredAt: string;
  }): Promise<ChannelResolution>;
  provisionFoundingAdult(input: {
    externalChatId: string;
    externalHandle: string;
    timeZone: string;
    occurredAt: string;
  }): Promise<ChannelResolution>;
  finalizeFoundingAdult(input: {
    householdId: string;
    adultId: string;
    externalChatId: string;
    externalHandle: string;
    consentedAt: string;
  }): Promise<boolean>;
  finalizeInvitation(input: {
    householdId: string;
    adultId: string;
    externalChatId: string;
    externalHandle: string;
    consentedAt: string;
  }): Promise<boolean>;
  resolveExactGroup(participantHandles: readonly string[]): Promise<GroupIdentity | null>;
  bindHouseholdGroup(input: {
    householdId: string;
    externalChatId: string;
    participantHandles: readonly string[];
    selfHandles: readonly string[];
    service: string;
    healthStatus: string;
  }): Promise<ChannelResolution | null>;
}

export class ProductionProviderProcessor {
  public constructor(private readonly options: ProductionProviderProcessorOptions) {}

  public async process(item: ClaimedProviderInboxItem): Promise<ProviderProcessingResult> {
    switch (item.provider) {
      case "linq":
        return this.processLinq(item, linqInboundEventSchema.parse(item.payload));
      case "gmail": {
        const push = gmailPubSubEventSchema.parse(item.payload);
        const result = await this.options.google.processPush(push);
        return {
          householdId: result.householdId,
          resolution: {
            classification: `gmail:${result.status}:${result.phase}`,
            processedMessages: result.processedMessages,
          },
        };
      }
      default:
        throw new ProviderProcessingError("unsupported_provider", false, "Unsupported provider inbox source");
    }
  }

  private async processLinq(
    item: ClaimedProviderInboxItem,
    event: LinqInboundEvent,
  ): Promise<ProviderProcessingResult> {
    if (event.eventType !== "message.received") {
      return {
        resolution: {
          classification: `linq:${event.eventType}:observed`,
          providerEventId: event.providerEventId,
        },
      };
    }

    const handle = canonicalizeLinqHandle(event.sender.handle);
    const knownBeforeConsent = await this.resolveKnownChannel(event, handle);
    if (event.message.consentCommand === "stop") {
      await this.options.runtimeStore.setSuppression({
        externalChatId: event.conversation.id,
        ...(event.scope === "direct" ? { externalHandle: handle } : {}),
        scope: event.scope === "direct" ? "private" : "group",
        suppressed: true,
        occurredAt: event.occurredAt,
        sourceEventId: event.dedupeKey,
        reason: "stop_command",
      });
      return {
        ...(knownBeforeConsent ? { householdId: knownBeforeConsent.householdId } : {}),
        resolution: { classification: "linq:stop:suppressed" },
      };
    }
    if (event.scope === "direct" && event.message.consentCommand === "start") {
      await this.options.runtimeStore.setSuppression({
        externalChatId: event.conversation.id,
        externalHandle: handle,
        scope: "private",
        suppressed: false,
        occurredAt: event.occurredAt,
        sourceEventId: event.dedupeKey,
        reason: "start_command",
      });
    }
    if (
      event.scope === "direct" &&
      (await this.options.runtimeStore.isSuppressed(event.conversation.id, handle))
    ) {
      return {
        ...(knownBeforeConsent ? { householdId: knownBeforeConsent.householdId } : {}),
        resolution: { classification: "linq:suppressed" },
      };
    }
    if (
      event.scope === "group" &&
      event.message.consentCommand !== "start" &&
      (await this.options.runtimeStore.isSuppressed(event.conversation.id))
    ) {
      return {
        ...(knownBeforeConsent ? { householdId: knownBeforeConsent.householdId } : {}),
        resolution: { classification: "linq:suppressed" },
      };
    }

    const route =
      event.scope === "direct"
        ? await this.routeDirect(event, handle, knownBeforeConsent)
        : await this.routeVerifiedGroup(event, handle, knownBeforeConsent);
    if (route === null) {
      const suppressed =
        event.scope === "group" && (await this.options.runtimeStore.isSuppressed(event.conversation.id));
      return {
        ...(knownBeforeConsent ? { householdId: knownBeforeConsent.householdId } : {}),
        resolution: { classification: suppressed ? "linq:suppressed" : "linq:unverified_group" },
      };
    }

    const snapshot = await this.options.applicationStore.load(route.resolution.householdId);
    if (!snapshot) {
      throw new ProviderProcessingError(
        "household_snapshot_missing",
        false,
        "Household state is unavailable",
      );
    }
    if (
      event.scope === "direct" &&
      this.options.privateCommands &&
      route.resolution.membershipStatus === "active"
    ) {
      const command = await this.options.privateCommands.handle({
        householdId: route.resolution.householdId,
        adultId: route.senderAdultId,
        channelId: event.conversation.id,
        messageId: event.message.id,
        text: event.message.text,
        occurredAt: event.occurredAt,
        idempotencyKey: item.idempotencyKey,
      });
      if (command.handled) {
        return {
          householdId: route.resolution.householdId,
          resolution: { classification: command.classification ?? "linq:private_command" },
        };
      }
    }

    const attachmentContents = await this.resolveAttachmentContents(event);
    const applicationResult = await this.options.application.process({
      kind: "conversation_message",
      householdId: route.resolution.householdId,
      idempotencyKey: item.idempotencyKey,
      occurredAt: event.occurredAt,
      channel:
        event.scope === "direct"
          ? { channelId: event.conversation.id, scope: "personal", adultId: route.senderAdultId }
          : { channelId: event.conversation.id, scope: "household" },
      senderAdultId: route.senderAdultId,
      messageRef: `linq:message:${event.message.id}`,
      text: event.message.text,
      attachmentRefs: event.message.attachments.map((attachment) =>
        attachment.providerAttachmentId
          ? `linq:attachment:${attachment.providerAttachmentId}`
          : `linq:message:${event.message.id}:part:${attachment.partIndex}`,
      ),
      attachmentContents,
    });

    if (event.scope === "direct" && route.resolution.membershipStatus === "invited") {
      const after = await this.options.applicationStore.load(route.resolution.householdId);
      const onboarding = after?.projection.onboarding;
      const adultId = AdultIdSchema.parse(route.senderAdultId);
      if (onboarding?.consentedAdultIds.includes(adultId)) {
        const activated =
          onboarding.initiatorAdultId === adultId
            ? await this.options.runtimeStore.finalizeFoundingAdult({
                householdId: route.resolution.householdId,
                adultId,
                externalChatId: event.conversation.id,
                externalHandle: handle,
                consentedAt: event.occurredAt,
              })
            : onboarding.invitedAdultId === adultId
              ? await this.options.runtimeStore.finalizeInvitation({
                  householdId: route.resolution.householdId,
                  adultId,
                  externalChatId: event.conversation.id,
                  externalHandle: handle,
                  consentedAt: event.occurredAt,
                })
              : false;
        if (!activated) {
          throw new ProviderProcessingError(
            "consented_identity_activation_failed",
            false,
            "The explicitly consented identity could not be activated",
          );
        }
      }
    }

    return {
      householdId: route.resolution.householdId,
      resolution: {
        classification: applicationResult.outcome.classification,
        disposition: applicationResult.disposition,
        revision: applicationResult.revision,
      },
    };
  }

  private async resolveAttachmentContents(
    event: Extract<LinqInboundEvent, { eventType: "message.received" }>,
  ): Promise<ConversationAttachmentContent[]> {
    const resolved: ConversationAttachmentContent[] = [];
    let remainingBytes = 15 * 1024 * 1024;
    for (const attachment of event.message.attachments) {
      const reference = attachment.providerAttachmentId
        ? `linq:attachment:${attachment.providerAttachmentId}`
        : `linq:message:${event.message.id}:part:${attachment.partIndex}`;
      const base = {
        reference,
        mediaType: attachment.mimeType,
        filename: attachment.filename,
        sizeBytes: attachment.sizeBytes,
      } as const;

      if (attachment.kind === "link" && attachment.url) {
        let link: URL;
        try {
          link = new URL(attachment.url);
        } catch {
          resolved.push(unavailableAttachment(base, "unsupported_type"));
          continue;
        }
        if (link.protocol !== "https:") {
          resolved.push(unavailableAttachment(base, "unsupported_type"));
          continue;
        }
        resolved.push({
          ...base,
          kind: "link",
          url: link.toString(),
          contentDigest: sha256(link.toString()),
        });
        continue;
      }

      if (!attachment.providerAttachmentId) {
        resolved.push(unavailableAttachment(base, "missing_reference"));
        continue;
      }
      if (remainingBytes <= 0) {
        resolved.push(unavailableAttachment(base, "too_large"));
        continue;
      }

      try {
        const content = await this.options.linqAttachments.retrieveAttachment(
          attachment.providerAttachmentId,
          Math.min(10 * 1024 * 1024, remainingBytes),
        );
        remainingBytes -= content.sizeBytes;
        const bytes = Buffer.from(content.bytes);
        resolved.push({
          reference,
          kind: content.kind,
          mediaType: content.mediaType,
          filename: content.filename,
          sizeBytes: content.sizeBytes,
          dataBase64: bytes.toString("base64"),
          contentDigest: sha256(bytes),
        });
      } catch (error) {
        if (error instanceof LinqAttachmentContentError) {
          resolved.push(unavailableAttachment(base, error.reason));
          continue;
        }
        if (error instanceof LinqApiError) {
          throw new ProviderProcessingError(
            "linq_attachment_fetch_failed",
            error.retryable,
            "Linq attachment content could not be retrieved",
          );
        }
        throw error;
      }
    }
    return resolved;
  }

  private async resolveKnownChannel(
    event: Extract<LinqInboundEvent, { eventType: "message.received" }>,
    handle: string,
  ): Promise<ChannelResolution | null> {
    return this.options.applicationStore.resolveChannel({
      provider: "linq",
      externalChatId: event.conversation.id,
      ...(event.scope === "direct" ? { externalHandle: handle } : {}),
    });
  }

  private async routeDirect(
    event: Extract<LinqInboundEvent, { eventType: "message.received"; scope: "direct" }>,
    handle: string,
    known: ChannelResolution | null,
  ): Promise<{ resolution: ChannelResolution; senderAdultId: string }> {
    let resolution = known;
    if (!resolution) {
      const invitation = await this.options.runtimeStore.findPendingInvitation(handle);
      resolution = invitation
        ? await this.options.runtimeStore.bindPendingInvitee({
            invitation,
            externalChatId: event.conversation.id,
            externalHandle: handle,
            occurredAt: event.occurredAt,
          })
        : await this.options.runtimeStore.provisionFoundingAdult({
            externalChatId: event.conversation.id,
            externalHandle: handle,
            timeZone: this.options.defaultTimeZone,
            occurredAt: event.occurredAt,
          });
    }
    if (!resolution.adultId) {
      throw new ProviderProcessingError("private_identity_missing", false, "Private channel has no adult");
    }
    const permitted =
      (resolution.bindingStatus === "active" && resolution.membershipStatus === "active") ||
      (resolution.bindingStatus === "pending" && resolution.membershipStatus === "invited");
    if (!permitted) {
      throw new ProviderProcessingError("private_identity_inactive", false, "Private identity is inactive");
    }
    return { resolution, senderAdultId: resolution.adultId };
  }

  private async routeVerifiedGroup(
    event: Extract<LinqInboundEvent, { eventType: "message.received"; scope: "group" }>,
    handle: string,
    known: ChannelResolution | null,
  ): Promise<{ resolution: ChannelResolution; senderAdultId: string } | null> {
    let chat: Awaited<ReturnType<LinqChatReader["getChat"]>>;
    try {
      chat = await this.options.linqChats.getChat(event.conversation.id);
    } catch (error) {
      if (error instanceof LinqApiError) {
        throw new ProviderProcessingError(
          "linq_group_lookup_failed",
          error.retryable,
          "Linq group identity could not be verified",
        );
      }
      throw error;
    }
    if (chat.id === event.conversation.id && chat.isGroup && chat.healthStatus === "OPTED_OUT") {
      await this.options.runtimeStore.setSuppression({
        externalChatId: event.conversation.id,
        scope: "group",
        suppressed: true,
        occurredAt: event.occurredAt,
        sourceEventId: event.dedupeKey,
        reason: "provider_opted_out",
      });
      await this.pauseKnownGroup(known, event.conversation.id, "provider_opted_out");
      return null;
    }
    if (!isExactHealthyLinqGroup(chat, event.conversation.id)) {
      await this.pauseKnownGroup(known, event.conversation.id, "live_group_identity_mismatch");
      return null;
    }
    const identity = await this.options.runtimeStore.resolveExactGroup(chat.participantHandles);
    if (!identity || (known && known.householdId !== identity.householdId)) {
      await this.pauseKnownGroup(known, event.conversation.id, "participant_identity_mismatch");
      return null;
    }
    const senderAdultId = identity.adultsByHandle.get(handle);
    if (!senderAdultId) {
      await this.pauseKnownGroup(known, event.conversation.id, "sender_identity_mismatch");
      return null;
    }

    if (event.message.consentCommand === "start") {
      const release = await this.options.runtimeStore.setSuppression({
        externalChatId: event.conversation.id,
        scope: "group",
        suppressed: false,
        occurredAt: event.occurredAt,
        sourceEventId: event.dedupeKey,
        reason: "start_command",
      });
      if (release.suppressed) return null;
    }

    const resolution = await this.options.runtimeStore.bindHouseholdGroup({
      householdId: identity.householdId,
      externalChatId: chat.id,
      participantHandles: chat.participantHandles,
      selfHandles: chat.selfHandles,
      service: chat.service as string,
      healthStatus: chat.healthStatus,
    });
    if (!resolution) return null;
    return { resolution, senderAdultId };
  }

  private async pauseKnownGroup(
    known: ChannelResolution | null,
    externalChatId: string,
    reason: string,
  ): Promise<void> {
    if (known?.channelType !== "group") return;
    await this.options.runtimeStore.pauseGroupBinding({ externalChatId, reason });
  }
}

function isExactHealthyLinqGroup(
  chat: Awaited<ReturnType<LinqChatReader["getChat"]>>,
  expectedChatId: string,
): boolean {
  if (
    chat.id !== expectedChatId ||
    !chat.isGroup ||
    chat.healthStatus !== "HEALTHY" ||
    chat.service?.toLowerCase() !== "imessage"
  ) {
    return false;
  }
  try {
    const participants = uniqueCanonicalHandles(chat.participantHandles);
    const self = uniqueCanonicalHandles(chat.selfHandles);
    const active = uniqueCanonicalHandles(chat.activeHandles);
    if (participants.length !== 2 || self.length !== 1 || active.length !== 3) return false;
    const expectedActive = [...participants, ...self].sort();
    return expectedActive.every((value, index) => value === active[index]);
  } catch {
    return false;
  }
}

function uniqueCanonicalHandles(handles: readonly string[]): string[] {
  return [...new Set(handles.map(canonicalizeLinqHandle))].sort();
}

function sha256(value: string | Uint8Array): string {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function unavailableAttachment(
  base: {
    reference: string;
    mediaType: string | null;
    filename: string | null;
    sizeBytes: number | null;
  },
  reason: "missing_reference" | "too_large" | "unsupported_type" | "not_found",
): ConversationAttachmentContent {
  return {
    ...base,
    kind: "unavailable",
    reason,
    contentDigest: sha256(
      JSON.stringify({
        reference: base.reference,
        mediaType: base.mediaType,
        filename: base.filename,
        sizeBytes: base.sizeBytes,
        reason,
      }),
    ),
  };
}
