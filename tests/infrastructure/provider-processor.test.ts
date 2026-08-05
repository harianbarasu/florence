import { describe, expect, it, vi } from "vitest";
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
    providerEventId: "event-1",
    dedupeKey: "linq:partner:event-1",
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
  return {
    revision: 0,
    aggregate: aggregate(),
    projection: {
      onboarding: {
        phase: "active" as const,
        initiatorAdultId: ADULT_A,
        invitedAdultId: ADULT_B,
        consentedAdultIds: [ADULT_A, ADULT_B],
        privateDmAdultIds: [ADULT_A, ADULT_B],
        groupChannelId: "group-1",
        profileConfirmedAdultIds: [ADULT_A, ADULT_B],
        ...overrides,
      },
      gmailTriage: [],
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

function setup(input: { known?: ChannelResolution | null; snapshots?: HouseholdApplicationSnapshot[] } = {}) {
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
  const provisionFoundingAdult = vi.fn(async () => privateResolution());
  const runtimeStore: ProviderRuntimeStore = {
    setSuppression: vi.fn(async () => undefined),
    isSuppressed: vi.fn(async () => false),
    findPendingInvitation: vi.fn(async () => null),
    bindPendingInvitee: vi.fn(async () => privateResolution()),
    provisionFoundingAdult,
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
  const processor = new ProductionProviderProcessor({
    application: { process, executeOutbox: vi.fn() },
    applicationStore,
    runtimeStore,
    linqChats: { getChat },
    google,
    defaultTimeZone: "America/Los_Angeles",
  });
  return {
    processor,
    process,
    applicationStore,
    runtimeStore,
    provisionFoundingAdult,
    google,
    getChat,
  };
}

describe("ProductionProviderProcessor", () => {
  it("provisions an unknown inbound DM and routes it as one personal application signal", async () => {
    const harness = setup();
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
      expect.objectContaining({ scope: "private", suppressed: true, reason: "stop_command" }),
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
    expect(harness.runtimeStore.bindHouseholdGroup).toHaveBeenCalled();
    expect(harness.process).toHaveBeenCalledWith(
      expect.objectContaining({
        channel: { channelId: "group-1", scope: "household" },
        senderAdultId: ADULT_A,
      }),
    );
  });

  it("delegates Gmail history notices without exposing their payload to Linq routing", async () => {
    const harness = setup();
    const push = {
      schemaVersion: 1,
      source: "gmail",
      sourceScope: "personal",
      providerEventId: "push-1",
      subscription: "projects/test/subscriptions/florence",
      mailboxEmail: "parent@example.test",
      historyId: "123",
      publishedAt: NOW,
      deliveryAttempt: 1,
    };
    await expect(harness.processor.process(claimed(push, "gmail"))).resolves.toMatchObject({
      householdId: HOUSEHOLD_ID,
      resolution: { classification: "gmail:processed:live", processedMessages: 2 },
    });
    expect(harness.google.processPush).toHaveBeenCalledWith(push);
    expect(harness.process).not.toHaveBeenCalled();
  });
});
