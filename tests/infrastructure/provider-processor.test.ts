import { describe, expect, it, vi } from "vitest";
import { LinqApiError, LinqAttachmentContentError } from "../../src/adapters/linq/index.js";
import type { HouseholdApplicationSnapshot } from "../../src/application/index.js";
import type { ChannelResolution, ClaimedProviderInboxItem } from "../../src/db/application-store.js";
import {
  ProductionProviderProcessor,
  type ProviderApplicationStore,
  type ProviderRuntimeStore,
} from "../../src/infrastructure/provider-processor.js";
import { ADULT_A, ADULT_B, aggregate, HOUSEHOLD_ID } from "../application/fixtures.js";

const NOW = "2026-08-05T16:00:00Z";

function event(overrides: Record<string, unknown> = {}) {
  return {
    schemaVersion: 1,
    source: "linq",
    transport: "webhook",
    providerEventId: "event-1",
    dedupeKey: "linq:partner:event-1",
    businessDedupeKey: "linq:message:sha256:synthetic-message-1",
    occurredAt: NOW,
    webhookVersion: "2026-02-03",
    partnerId: "partner",
    eventType: "message.received",
    scope: "direct",
    conversation: {
      id: "dm-1",
      kind: "direct",
      ownerHandle: "+16462350806",
      knownParticipantHandles: ["+16462350806", "+12025550101"],
    },
    sender: { id: "sender-1", handle: "+12025550101", service: "iMessage" },
    message: {
      id: "message-1",
      text: "Hello Florence",
      attachments: [],
      replyTo: null,
      consentCommand: null,
    },
    ...overrides,
  };
}

function claimed(payload: Record<string, unknown>, provider = "linq"): ClaimedProviderInboxItem {
  return {
    id: "00000000-0000-4000-8000-000000000001",
    provider,
    idempotencyKey: provider === "linq" ? "linq:partner:event-1" : "gmail:push:1",
    payloadHash: "hash",
    authentication: { verified: true },
    eventKind: "message.received",
    occurredAt: NOW,
    payload,
    attempt: 1,
    maxAttempts: 8,
    leaseToken: "00000000-0000-4000-8000-000000000002",
    leaseExpiresAt: "2026-08-05T16:01:00Z",
  };
}

function snapshot(overrides: Partial<HouseholdApplicationSnapshot["projection"]["onboarding"]> = {}) {
  const onboarding: HouseholdApplicationSnapshot["projection"]["onboarding"] = {
    phase: "active",
    initiatorAdultId: ADULT_A,
    invitedAdultId: ADULT_B,
    consentedAdultIds: [ADULT_A, ADULT_B],
    privateDmAdultIds: [ADULT_A, ADULT_B],
    groupChannelId: "group-1",
    adultNames: [
      { adultId: ADULT_A, displayName: "Hari" },
      { adultId: ADULT_B, displayName: "Partner" },
    ],
    profileConfirmedAdultIds: [ADULT_A, ADULT_B],
    googleConnectedAdultIds: [ADULT_A, ADULT_B],
    ...overrides,
  };

  return {
    revision: 0,
    aggregate: aggregate(),
    projection: {
      onboarding,
      sharedProfile: { facts: [] },
      gmailTriage: [],
      gmailSources: [],
      calendarTriage: [],
      calendarSources: [],
      pendingPromotions: [],
      workers: [],
    },
  } satisfies HouseholdApplicationSnapshot;
}

function privateResolution(overrides: Partial<ChannelResolution> = {}): ChannelResolution {
  return {
    bindingId: "binding-1",
    provider: "linq",
    channelType: "private",
    householdId: HOUSEHOLD_ID,
    adultId: ADULT_A,
    bindingStatus: "active",
    membershipStatus: "active",
    metadata: {},
    ...overrides,
  };
}

function setup(
  input: {
    known?: ChannelResolution | null;
    snapshots?: HouseholdApplicationSnapshot[];
    deletedIdentity?: boolean;
  } = {},
) {
  const process = vi.fn(async () => ({
    householdId: HOUSEHOLD_ID,
    idempotencyKey: "linq:partner:event-1",
    disposition: "committed" as const,
    revision: 1,
    outcome: {
      status: "processed" as const,
      classification: "conversation:ignore",
      domainReceipts: [],
      outboxIntentIds: [],
    },
  }));
  const snapshots = [...(input.snapshots ?? [snapshot()])];
  const applicationStore: ProviderApplicationStore = {
    resolveChannel: vi.fn(async () => input.known ?? null),
    load: vi.fn(async () => snapshots.shift() ?? snapshot()),
  };
  const provisionFoundingAdult = vi.fn(async () =>
    privateResolution({ bindingStatus: "pending", membershipStatus: "invited" }),
  );
  const finalizeFoundingAdult = vi.fn(async () => true);
  const isDeletedLinqIdentity = vi.fn(async () => input.deletedIdentity ?? false);
  const runtimeStore: ProviderRuntimeStore = {
    setSuppression: vi.fn(async (input) => ({ applied: true, suppressed: input.suppressed })),
    isSuppressed: vi.fn(async () => false),
    pauseGroupBinding: vi.fn(async () => true),
    findPendingInvitation: vi.fn(async () => null),
    bindPendingInvitee: vi.fn(async () => privateResolution()),
    provisionFoundingAdult,
    finalizeFoundingAdult,
    finalizeInvitation: vi.fn(async () => true),
    resolveExactGroup: vi.fn(async () => null),
    bindHouseholdGroup: vi.fn(async () => ({
      ...privateResolution(),
      channelType: "group" as const,
      adultId: null,
      membershipStatus: null,
    })),
  };
  const google = {
    processPush: vi.fn(async () => ({
      householdId: HOUSEHOLD_ID,
      status: "processed",
      phase: "live",
      processedMessages: 2,
    })),
  };
  const getChat = vi.fn();
  const retrieveAttachment = vi.fn();
  const processor = new ProductionProviderProcessor({
    application: { process, executeOutbox: vi.fn() },
    applicationStore,
    runtimeStore,
    deletedIdentities: { isDeletedLinqIdentity },
    linqChats: { getChat },
    linqAttachments: { retrieveAttachment },
    google,
    defaultTimeZone: "America/Los_Angeles",
  });
  return {
    processor,
    process,
    applicationStore,
    runtimeStore,
    provisionFoundingAdult,
    finalizeFoundingAdult,
    isDeletedLinqIdentity,
    google,
    getChat,
    retrieveAttachment,
  };
}

describe("ProductionProviderProcessor", () => {
  it("provisions an unknown inbound DM and routes it as one personal application signal", async () => {
    const awaitingConsent = snapshot({
      phase: "awaiting_initiator_consent",
      invitedAdultId: undefined,
      consentedAdultIds: [],
      privateDmAdultIds: [],
      groupChannelId: undefined,
      profileConfirmedAdultIds: [],
      googleConnectedAdultIds: [],
    });
    const harness = setup({ snapshots: [awaitingConsent, awaitingConsent] });
    await expect(harness.processor.process(claimed(event()))).resolves.toMatchObject({
      householdId: HOUSEHOLD_ID,
      resolution: { classification: "conversation:ignore" },
    });
    expect(harness.provisionFoundingAdult).toHaveBeenCalledWith({
      externalChatId: "dm-1",
      externalHandle: "+12025550101",
      timeZone: "America/Los_Angeles",
      occurredAt: NOW,
    });
    expect(harness.process).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: "conversation_message",
        channel: { channelId: "dm-1", scope: "personal", adultId: ADULT_A },
        senderAdultId: ADULT_A,
      }),
    );
    expect(harness.finalizeFoundingAdult).not.toHaveBeenCalled();
  });

  it("does not reprovision an unknown DM identity retained by a deletion tombstone", async () => {
    const harness = setup({ deletedIdentity: true });

    await expect(harness.processor.process(claimed(event()))).resolves.toMatchObject({
      resolution: { classification: "linq:deleted_identity:ignored" },
    });
    expect(harness.isDeletedLinqIdentity).toHaveBeenCalledWith({
      externalChatId: "dm-1",
      externalHandle: "+12025550101",
    });
    expect(harness.provisionFoundingAdult).not.toHaveBeenCalled();
    expect(harness.process).not.toHaveBeenCalled();
  });

  it("activates a pending founder only after the application records explicit consent", async () => {
    const before = snapshot({
      phase: "awaiting_initiator_consent",
      invitedAdultId: undefined,
      consentedAdultIds: [],
      privateDmAdultIds: [],
      groupChannelId: undefined,
      profileConfirmedAdultIds: [],
      googleConnectedAdultIds: [],
    });
    const after = snapshot({
      phase: "awaiting_invitation",
      invitedAdultId: undefined,
      consentedAdultIds: [ADULT_A],
      privateDmAdultIds: [ADULT_A],
      groupChannelId: undefined,
      profileConfirmedAdultIds: [],
      googleConnectedAdultIds: [],
    });
    const harness = setup({ snapshots: [before, after] });
    const consent = event({
      message: {
        id: "message-consent",
        text: "I consent",
        attachments: [],
        replyTo: null,
        consentCommand: null,
      },
    });

    await expect(harness.processor.process(claimed(consent))).resolves.toMatchObject({
      householdId: HOUSEHOLD_ID,
    });
    expect(harness.finalizeFoundingAdult).toHaveBeenCalledTimes(1);
    expect(harness.finalizeFoundingAdult).toHaveBeenCalledWith({
      householdId: HOUSEHOLD_ID,
      adultId: ADULT_A,
      externalChatId: "dm-1",
      externalHandle: "+12025550101",
      consentedAt: NOW,
    });
  });

  it("applies STOP durably before any model or application work", async () => {
    const harness = setup({ known: privateResolution() });
    const stop = event({
      message: {
        id: "message-stop",
        text: "STOP",
        attachments: [],
        replyTo: null,
        consentCommand: "stop",
      },
    });
    await expect(harness.processor.process(claimed(stop))).resolves.toMatchObject({
      resolution: { classification: "linq:stop:suppressed" },
    });
    expect(harness.runtimeStore.setSuppression).toHaveBeenCalledWith(
      expect.objectContaining({
        scope: "private",
        suppressed: true,
        sourceEventId: "linq:partner:event-1",
        reason: "stop_command",
      }),
    );
    expect(harness.process).not.toHaveBeenCalled();
  });

  it("revalidates the exact active group participant set and sender", async () => {
    const harness = setup();
    harness.getChat.mockResolvedValue({
      id: "group-1",
      isGroup: true,
      displayName: "Family",
      service: "iMessage",
      healthStatus: "HEALTHY",
      activeHandles: ["+16462350806", "+12025550101", "+12025550102"],
      selfHandles: ["+16462350806"],
      participantHandles: ["+12025550101", "+12025550102"],
    });
    vi.mocked(harness.runtimeStore.resolveExactGroup).mockResolvedValue({
      householdId: HOUSEHOLD_ID,
      adultsByHandle: new Map([
        ["+12025550101", ADULT_A],
        ["+12025550102", ADULT_B],
      ]),
    });
    const group = event({
      scope: "group",
      conversation: {
        id: "group-1",
        kind: "group",
        ownerHandle: "+16462350806",
        knownParticipantHandles: ["+16462350806", "+12025550101"],
      },
    });

    await expect(harness.processor.process(claimed(group))).resolves.toMatchObject({
      householdId: HOUSEHOLD_ID,
    });
    expect(harness.runtimeStore.bindHouseholdGroup).toHaveBeenCalledWith({
      householdId: HOUSEHOLD_ID,
      externalChatId: "group-1",
      participantHandles: ["+12025550101", "+12025550102"],
      selfHandles: ["+16462350806"],
      service: "iMessage",
      healthStatus: "HEALTHY",
    });
    expect(harness.process).toHaveBeenCalledWith(
      expect.objectContaining({
        channel: { channelId: "group-1", scope: "household" },
        senderAdultId: ADULT_A,
      }),
    );
  });

  it("pauses an existing group binding when live participants or the sender do not match", async () => {
    const knownGroup = privateResolution({
      channelType: "group",
      adultId: null,
      membershipStatus: null,
    });
    const changedParticipants = setup({ known: knownGroup });
    changedParticipants.getChat.mockResolvedValue({
      id: "group-1",
      isGroup: true,
      displayName: "Family",
      service: "iMessage",
      healthStatus: "HEALTHY",
      activeHandles: ["+16462350806", "+12025550101", "+12025550103"],
      selfHandles: ["+16462350806"],
      participantHandles: ["+12025550101", "+12025550103"],
    });
    const group = event({
      scope: "group",
      conversation: {
        id: "group-1",
        kind: "group",
        ownerHandle: "+16462350806",
        knownParticipantHandles: ["+16462350806", "+12025550101"],
      },
    });

    await expect(changedParticipants.processor.process(claimed(group))).resolves.toMatchObject({
      resolution: { classification: "linq:unverified_group" },
    });
    expect(changedParticipants.runtimeStore.pauseGroupBinding).toHaveBeenCalledWith({
      externalChatId: "group-1",
      reason: "participant_identity_mismatch",
    });
    expect(changedParticipants.process).not.toHaveBeenCalled();

    const unknownSender = setup({ known: knownGroup });
    unknownSender.getChat.mockResolvedValue({
      id: "group-1",
      isGroup: true,
      displayName: "Family",
      service: "iMessage",
      healthStatus: "HEALTHY",
      activeHandles: ["+16462350806", "+12025550101", "+12025550102"],
      selfHandles: ["+16462350806"],
      participantHandles: ["+12025550101", "+12025550102"],
    });
    vi.mocked(unknownSender.runtimeStore.resolveExactGroup).mockResolvedValue({
      householdId: HOUSEHOLD_ID,
      adultsByHandle: new Map([
        ["+12025550101", ADULT_A],
        ["+12025550102", ADULT_B],
      ]),
    });
    const thirdParty = event({
      scope: "group",
      conversation: {
        id: "group-1",
        kind: "group",
        ownerHandle: "+16462350806",
        knownParticipantHandles: ["+16462350806", "+12025550103"],
      },
      sender: { id: "sender-3", handle: "+12025550103", service: "iMessage" },
      message: {
        id: "message-start",
        text: "START",
        attachments: [],
        replyTo: null,
        consentCommand: "start",
      },
    });

    await expect(unknownSender.processor.process(claimed(thirdParty))).resolves.toMatchObject({
      resolution: { classification: "linq:unverified_group" },
    });
    expect(unknownSender.runtimeStore.setSuppression).not.toHaveBeenCalled();
    expect(unknownSender.runtimeStore.pauseGroupBinding).toHaveBeenCalledWith({
      externalChatId: "group-1",
      reason: "sender_identity_mismatch",
    });
  });

  it("maps group chat lookup failures to explicit retryable and permanent outcomes", async () => {
    const group = event({
      scope: "group",
      conversation: {
        id: "group-1",
        kind: "group",
        ownerHandle: "+16462350806",
        knownParticipantHandles: ["+16462350806", "+12025550101"],
      },
    });
    const retry = setup();
    retry.getChat.mockRejectedValue(new LinqApiError("private", 503, true));
    await expect(retry.processor.process(claimed(group))).rejects.toMatchObject({
      code: "linq_group_lookup_failed",
      retryable: true,
    });

    const permanent = setup();
    permanent.getChat.mockRejectedValue(new LinqApiError("private", 404, false));
    await expect(permanent.processor.process(claimed(group))).rejects.toMatchObject({
      code: "linq_group_lookup_failed",
      retryable: false,
    });
  });

  it("does not reactivate a group when an older START loses to durable STOP", async () => {
    const knownGroup = privateResolution({
      channelType: "group",
      adultId: null,
      membershipStatus: null,
      bindingStatus: "paused",
    });
    const harness = setup({ known: knownGroup });
    harness.getChat.mockResolvedValue({
      id: "group-1",
      isGroup: true,
      displayName: "Family",
      service: "iMessage",
      healthStatus: "HEALTHY",
      activeHandles: ["+16462350806", "+12025550101", "+12025550102"],
      selfHandles: ["+16462350806"],
      participantHandles: ["+12025550101", "+12025550102"],
    });
    vi.mocked(harness.runtimeStore.resolveExactGroup).mockResolvedValue({
      householdId: HOUSEHOLD_ID,
      adultsByHandle: new Map([
        ["+12025550101", ADULT_A],
        ["+12025550102", ADULT_B],
      ]),
    });
    vi.mocked(harness.runtimeStore.setSuppression).mockResolvedValue({
      applied: false,
      suppressed: true,
    });
    vi.mocked(harness.runtimeStore.isSuppressed).mockResolvedValue(true);
    const start = event({
      scope: "group",
      conversation: {
        id: "group-1",
        kind: "group",
        ownerHandle: "+16462350806",
        knownParticipantHandles: ["+16462350806", "+12025550101"],
      },
      message: {
        id: "message-start",
        text: "START",
        attachments: [],
        replyTo: null,
        consentCommand: "start",
      },
    });

    await expect(harness.processor.process(claimed(start))).resolves.toMatchObject({
      householdId: HOUSEHOLD_ID,
      resolution: { classification: "linq:suppressed" },
    });
    expect(harness.runtimeStore.bindHouseholdGroup).not.toHaveBeenCalled();
    expect(harness.process).not.toHaveBeenCalled();
  });

  it("retrieves bounded Linq attachment bytes and supplies them only to the application input", async () => {
    const harness = setup({ known: privateResolution() });
    harness.retrieveAttachment.mockResolvedValue({
      providerAttachmentId: "attachment-1",
      kind: "file",
      mediaType: "application/pdf",
      filename: "permission-slip.pdf",
      sizeBytes: 4,
      bytes: Uint8Array.from([1, 2, 3, 4]),
    });
    const withAttachment = event({
      message: {
        id: "message-attachment",
        text: "What do we need to do with this?",
        attachments: [
          {
            kind: "media",
            partIndex: 0,
            providerAttachmentId: "attachment-1",
            url: "https://cdn.linqapp.com/attachment-1",
            mimeType: "application/pdf",
            filename: "permission-slip.pdf",
            sizeBytes: 4,
          },
        ],
        replyTo: null,
        consentCommand: null,
      },
    });

    await harness.processor.process(claimed(withAttachment));

    expect(harness.retrieveAttachment).toHaveBeenCalledWith("attachment-1", 10 * 1024 * 1024);
    expect(harness.process).toHaveBeenCalledWith(
      expect.objectContaining({
        attachmentRefs: ["linq:attachment:attachment-1"],
        attachmentContents: [
          expect.objectContaining({
            reference: "linq:attachment:attachment-1",
            kind: "file",
            mediaType: "application/pdf",
            dataBase64: "AQIDBA==",
            contentDigest: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
          }),
        ],
      }),
    );
  });

  it("records terminal content limits but retries Linq transport failures", async () => {
    const unavailable = setup({ known: privateResolution() });
    unavailable.retrieveAttachment.mockRejectedValue(new LinqAttachmentContentError("too_large"));
    const attachmentEvent = event({
      message: {
        id: "message-attachment",
        text: "Please check this",
        attachments: [
          {
            kind: "media",
            partIndex: 0,
            providerAttachmentId: "attachment-1",
            url: null,
            mimeType: "application/pdf",
            filename: "large.pdf",
            sizeBytes: 20 * 1024 * 1024,
          },
        ],
        replyTo: null,
        consentCommand: null,
      },
    });
    await unavailable.processor.process(claimed(attachmentEvent));
    expect(unavailable.process).toHaveBeenCalledWith(
      expect.objectContaining({
        attachmentContents: [expect.objectContaining({ kind: "unavailable", reason: "too_large" })],
      }),
    );

    const retry = setup({ known: privateResolution() });
    retry.retrieveAttachment.mockRejectedValue(new LinqApiError("redacted", 503, true));
    await expect(retry.processor.process(claimed(attachmentEvent))).rejects.toMatchObject({
      name: "ProviderProcessingError",
      code: "linq_attachment_fetch_failed",
      retryable: true,
    });
  });
});
