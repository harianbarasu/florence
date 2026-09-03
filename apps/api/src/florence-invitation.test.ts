import type { PostgresFlorenceStore } from "@florence/database";
import type { GoogleConnection } from "@florence/google";
import type { LinqClient, LinqSendMessage } from "@florence/linq";
import { describe, expect, test, vi } from "vitest";
import { EnrollmentCodes } from "./enrollment.js";
import { Florence } from "./florence.js";

const NOW = "2026-08-29T10:00:00.000Z";
const HOUSEHOLD_ID = "11111111-1111-4111-8111-111111111111";
const FOUNDER_ID = "22222222-2222-4222-8222-222222222222";
const PARTNER_ID = "33333333-3333-4333-8333-333333333333";
const PARTNER_CONVERSATION = "linq-partner-private-thread";
const PARTNER_IDENTITY = "a".repeat(64);
const PARTNER_PHONE = "+13105550123";
const PRIOR_LINK_MESSAGE_ID = "linq-prior-setup-link";

describe("expired partner setup", () => {
  test("reissues a fresh setup link in the exact invited partner thread", async () => {
    const enrollmentCodes = new EnrollmentCodes("test-enrollment-secret-that-is-at-least-32-bytes");
    const refreshMessagesEnrollment = vi.fn(async () => ({ id: PARTNER_ID }));
    const confirmMessagesEnrollmentDelivery = vi.fn(async () => ({ id: PARTNER_ID }));
    const converseDuringSetup = vi.fn(async () => ({
      declineInvitation: false,
      requestsFreshLink: true,
      bubbles: [],
    }));
    const invitation = {
      adultId: PARTNER_ID,
      state: "expired" as const,
      linkIssued: true,
      householdId: HOUSEHOLD_ID,
      founderAdultId: FOUNDER_ID,
      messagesAddress: PARTNER_PHONE,
      providerConversationId: PARTNER_CONVERSATION,
      identitySubjectDigest: PARTNER_IDENTITY,
      initialProviderMessageId: PRIOR_LINK_MESSAGE_ID,
      handshakeAt: "2026-08-27T10:00:00.000Z",
      setupIssuedAt: "2026-08-28T10:00:00.000Z",
    };
    const readUnboundPartnerInvitation = vi.fn(
      async (): ReturnType<PostgresFlorenceStore["readUnboundPartnerInvitation"]> => invitation,
    );
    const expirePartnerInvitations = vi.fn(async () => 1);
    const store = {
      readUnboundPartnerInvitation,
      refreshMessagesEnrollment,
      beginMessagesEnrollmentDelivery: vi.fn(async () => undefined),
      confirmMessagesEnrollmentDelivery,
      expirePartnerInvitations,
      scopeHouseholdLinqIdempotencyKey: vi.fn(
        async ({ idempotencyKey }: { idempotencyKey: string }) => idempotencyKey,
      ),
    } as unknown as PostgresFlorenceStore;
    const sent: LinqSendMessage[] = [];
    const linq = {
      setTyping: vi.fn(async () => undefined),
      sendMessage: vi.fn(async (input: LinqSendMessage) => {
        sent.push(input);
        return {
          status: "committed" as const,
          providerState: "sent" as const,
          idempotencyKey: input.idempotencyKey,
          providerReceiptId: `receipt-${sent.length}`,
          detail: null,
          occurredAt: NOW,
        };
      }),
    } as unknown as LinqClient;
    const florence = new Florence({
      store,
      linq,
      google: {} as GoogleConnection,
      reasoner: { converseDuringSetup } as never,
      enrollmentCodes,
      imageVault: null,
      messagesUrl: null,
      setupOrigin: "https://app.tryflorence.com",
      now: () => new Date(NOW),
    });

    await florence.respondBeforeEnrollment({
      providerEventId: "reply-to-expired-link",
      providerConversationId: PARTNER_CONVERSATION,
      identitySubjectDigest: PARTNER_IDENTITY,
      messagesAddress: PARTNER_PHONE,
      text: "Ready!",
      occurredAt: NOW,
      carrierOptOut: false,
    });

    expect(expirePartnerInvitations).not.toHaveBeenCalled();
    expect(converseDuringSetup).toHaveBeenCalledWith(
      expect.objectContaining({
        stage: "partner_invited",
        currentMessage: { text: "Ready!", occurredAt: NOW },
        nextStep: "signed_link_will_follow",
      }),
    );
    expect(sent.map((message) => message.text)).toEqual([
      "Of course—here’s a fresh private setup link.",
      expect.stringMatching(/^https:\/\/app\.tryflorence\.com\/#s=ps1\./),
    ]);
    const token = new URL(sent[1]?.text ?? "").hash.slice("#s=".length);
    expect(enrollmentCodes.verifyPartnerSetup(token, new Date(NOW))).toMatchObject({
      householdId: HOUSEHOLD_ID,
      adultId: PARTNER_ID,
      providerConversationId: PARTNER_CONVERSATION,
      identitySubjectDigest: PARTNER_IDENTITY,
    });
    expect(refreshMessagesEnrollment).toHaveBeenCalledWith(
      expect.objectContaining({
        householdId: HOUSEHOLD_ID,
        adultId: PARTNER_ID,
        providerConversationId: PARTNER_CONVERSATION,
        identitySubjectDigest: PARTNER_IDENTITY,
        messagesAddress: PARTNER_PHONE,
        providerMessageId: PRIOR_LINK_MESSAGE_ID,
        refreshProviderEventId: "reply-to-expired-link",
      }),
    );
    expect(confirmMessagesEnrollmentDelivery).toHaveBeenCalledWith(
      expect.objectContaining({
        householdId: HOUSEHOLD_ID,
        adultId: PARTNER_ID,
        providerConversationId: PARTNER_CONVERSATION,
        identitySubjectDigest: PARTNER_IDENTITY,
        messagesAddress: PARTNER_PHONE,
        providerMessageId: "receipt-2",
      }),
    );

    readUnboundPartnerInvitation.mockResolvedValueOnce({
      ...invitation,
      state: "awaiting_reply",
      refreshProviderEventId: "reply-to-expired-link",
    });
    await florence.respondBeforeEnrollment({
      providerEventId: "reply-to-expired-link",
      providerConversationId: PARTNER_CONVERSATION,
      identitySubjectDigest: PARTNER_IDENTITY,
      messagesAddress: PARTNER_PHONE,
      text: "Ready!",
      occurredAt: NOW,
      carrierOptOut: false,
    });
    expect(sent[3]?.idempotencyKey).toBe(sent[1]?.idempotencyKey);

    await florence.respondBeforeEnrollment({
      providerEventId: "later-reply-to-expired-link",
      providerConversationId: PARTNER_CONVERSATION,
      identitySubjectDigest: PARTNER_IDENTITY,
      messagesAddress: PARTNER_PHONE,
      text: "Please send it again",
      occurredAt: NOW,
      carrierOptOut: false,
    });
    expect(sent[5]?.idempotencyKey).not.toBe(sent[1]?.idempotencyKey);
  });

  test("ignores pre-handshake events and honors a delayed refusal without issuing a link", async () => {
    const declinePartnerInvitation = vi.fn(async () => true);
    const converseDuringSetup = vi.fn(async () => ({
      declineInvitation: true,
      requestsFreshLink: false,
      bubbles: [{ text: "Understood.", delayMs: 0 }],
    }));
    const store = {
      readUnboundPartnerInvitation: vi.fn(async () => ({
        adultId: PARTNER_ID,
        state: "expired",
        linkIssued: true,
        householdId: HOUSEHOLD_ID,
        founderAdultId: FOUNDER_ID,
        messagesAddress: PARTNER_PHONE,
        providerConversationId: PARTNER_CONVERSATION,
        identitySubjectDigest: PARTNER_IDENTITY,
        initialProviderMessageId: PRIOR_LINK_MESSAGE_ID,
        handshakeAt: "2026-08-27T10:00:00.000Z",
        setupIssuedAt: "2026-08-28T10:00:00.000Z",
      })),
      declinePartnerInvitation,
      refreshMessagesEnrollment: vi.fn(),
    } as unknown as PostgresFlorenceStore;
    const sendMessage = vi.fn();
    const florence = new Florence({
      store,
      linq: { setTyping: vi.fn(), sendMessage } as unknown as LinqClient,
      google: {} as GoogleConnection,
      reasoner: { converseDuringSetup } as never,
      enrollmentCodes: new EnrollmentCodes("test-enrollment-secret-that-is-at-least-32-bytes"),
      imageVault: null,
      messagesUrl: null,
      setupOrigin: "https://app.tryflorence.com",
      now: () => new Date(NOW),
    });

    await florence.respondBeforeEnrollment({
      providerEventId: "old-event",
      providerConversationId: PARTNER_CONVERSATION,
      identitySubjectDigest: PARTNER_IDENTITY,
      messagesAddress: PARTNER_PHONE,
      text: "Old message",
      occurredAt: "2026-08-27T09:59:59.000Z",
      carrierOptOut: false,
    });
    expect(converseDuringSetup).not.toHaveBeenCalled();

    await florence.respondBeforeEnrollment({
      providerEventId: "delayed-refusal",
      providerConversationId: PARTNER_CONVERSATION,
      identitySubjectDigest: PARTNER_IDENTITY,
      messagesAddress: PARTNER_PHONE,
      text: "No thanks",
      occurredAt: NOW,
      carrierOptOut: false,
    });

    expect(converseDuringSetup).toHaveBeenCalledOnce();
    expect(declinePartnerInvitation).toHaveBeenCalledWith(
      expect.objectContaining({ adultId: PARTNER_ID, occurredAt: NOW }),
    );
    expect(store.refreshMessagesEnrollment).not.toHaveBeenCalled();
    expect(sendMessage).not.toHaveBeenCalled();
  });
});
