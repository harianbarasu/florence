import { type GmailPubSubEvent, gmailPubSubEventSchema } from "../adapters/google/index.js";
import {
  type LinqChatReader,
  type LinqInboundEvent,
  linqInboundEventSchema,
} from "../adapters/linq/index.js";
import type { FlorenceApplication, HouseholdApplicationSnapshot } from "../application/index.js";
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
    reason: string;
  }): Promise<void>;
  isSuppressed(externalChatId: string, externalHandle?: string): Promise<boolean>;
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
    healthStatus: string;
  }): Promise<ChannelResolution>;
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
        reason: "stop_command",
      });
      return {
        ...(knownBeforeConsent ? { householdId: knownBeforeConsent.householdId } : {}),
        resolution: { classification: "linq:stop:suppressed" },
      };
    }
    if (event.message.consentCommand === "start") {
      await this.options.runtimeStore.setSuppression({
        externalChatId: event.conversation.id,
        ...(event.scope === "direct" ? { externalHandle: handle } : {}),
        scope: event.scope === "direct" ? "private" : "group",
        suppressed: false,
        occurredAt: event.occurredAt,
        reason: "start_command",
      });
    } else if (
      await this.options.runtimeStore.isSuppressed(
        event.conversation.id,
        event.scope === "direct" ? handle : undefined,
      )
    ) {
      return {
        ...(knownBeforeConsent ? { householdId: knownBeforeConsent.householdId } : {}),
        resolution: { classification: "linq:suppressed" },
      };
    }

    const route =
      event.scope === "direct"
        ? await this.routeDirect(event, handle, knownBeforeConsent)
        : await this.routeGroup(event, handle, knownBeforeConsent);
    if (route === null) {
      return { resolution: { classification: "linq:unverified_group" } };
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
      snapshot.projection.onboarding.phase !== "awaiting_initiator_consent"
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
    });

    if (event.scope === "direct" && route.resolution.membershipStatus === "invited") {
      const after = await this.options.applicationStore.load(route.resolution.householdId);
      const onboarding = after?.projection.onboarding;
      if (
        onboarding?.invitedAdultId === route.senderAdultId &&
        onboarding.consentedAdultIds.includes(AdultIdSchema.parse(route.senderAdultId))
      ) {
        await this.options.runtimeStore.finalizeInvitation({
          householdId: route.resolution.householdId,
          adultId: route.senderAdultId,
          externalChatId: event.conversation.id,
          externalHandle: handle,
          consentedAt: event.occurredAt,
        });
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

  private async routeGroup(
    event: Extract<LinqInboundEvent, { eventType: "message.received"; scope: "group" }>,
    handle: string,
    known: ChannelResolution | null,
  ): Promise<{ resolution: ChannelResolution; senderAdultId: string } | null> {
    const chat = await this.options.linqChats.getChat(event.conversation.id);
    if (chat.id !== event.conversation.id || !chat.isGroup) {
      throw new ProviderProcessingError("linq_group_mismatch", false, "Linq chat identity changed");
    }
    if (chat.healthStatus === "OPTED_OUT") {
      await this.options.runtimeStore.setSuppression({
        externalChatId: chat.id,
        scope: "group",
        suppressed: true,
        occurredAt: event.occurredAt,
        reason: "provider_opted_out",
      });
      return null;
    }
    const identity = await this.options.runtimeStore.resolveExactGroup(chat.participantHandles);
    if (!identity || (known && known.householdId !== identity.householdId)) return null;
    const senderAdultId = identity.adultsByHandle.get(handle);
    if (!senderAdultId) return null;
    const resolution = await this.options.runtimeStore.bindHouseholdGroup({
      householdId: identity.householdId,
      externalChatId: chat.id,
      participantHandles: chat.participantHandles,
      healthStatus: chat.healthStatus,
    });
    return { resolution, senderAdultId };
  }
}
