import { randomBytes } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import type { GmailPubSubEvent, GoogleOAuthGrant } from "../../src/adapters/google/index.js";
import type { LinqInboundEvent } from "../../src/adapters/linq/index.js";
import {
  DurableProviderIngress,
  GoogleOAuthHandoffService,
  type GoogleOAuthStore,
  issueGoogleHandoffToken,
  ProductionReadiness,
  type ProviderIngressStore,
} from "../../src/infrastructure/index.js";
import { SecretBox } from "../../src/security/secret-box.js";

const householdId = "11111111-1111-4111-8111-111111111111";
const adultId = "22222222-2222-4222-8222-222222222222";

describe("DurableProviderIngress", () => {
  it("persists only normalized authenticated Linq and Gmail deliveries", async () => {
    const calls: unknown[] = [];
    const store: ProviderIngressStore = {
      async ingestProviderEvent(input) {
        calls.push(input);
        return { disposition: "accepted" };
      },
    };
    const ingress = new DurableProviderIngress(store);
    await ingress.acceptLinq({
      schemaVersion: 1,
      source: "linq",
      providerEventId: "evt-1",
      dedupeKey: "linq:partner:evt-1",
      occurredAt: "2027-01-01T08:00:00.000Z",
      webhookVersion: "2026-02-03",
      partnerId: "partner",
      eventType: "message.received",
      scope: "direct",
      conversation: {
        id: "chat-1",
        kind: "direct",
        ownerHandle: "+15550000000",
        knownParticipantHandles: ["+15550000001"],
      },
      sender: { id: "sender-1", handle: "+15550000001", service: "iMessage" },
      message: {
        id: "message-1",
        text: "Hello",
        attachments: [],
        replyTo: null,
        consentCommand: null,
      },
    } satisfies LinqInboundEvent);
    await ingress.acceptGmailPush({
      schemaVersion: 1,
      source: "gmail",
      sourceScope: "personal",
      providerEventId: "pubsub-1",
      subscription: "projects/p/subscriptions/florence",
      mailboxEmail: "parent@example.com",
      historyId: "42",
      publishedAt: "2027-01-01T08:01:00.000Z",
      deliveryAttempt: 1,
    } satisfies GmailPubSubEvent);

    expect(calls).toHaveLength(2);
    expect(calls[0]).toMatchObject({
      provider: "linq",
      idempotencyKey: "linq:partner:evt-1",
      authentication: { verified: true, webhookVersion: "2026-02-03" },
    });
    expect(calls[1]).toMatchObject({
      provider: "gmail",
      idempotencyKey: "gmail:pubsub:pubsub-1",
      authentication: { verified: true },
    });
  });

  it("makes authenticated STOP durable before enqueueing or acknowledging it", async () => {
    const order: string[] = [];
    const store: ProviderIngressStore = {
      async ingestProviderEvent() {
        order.push("inbox");
        return { disposition: "accepted" };
      },
    };
    const setSuppression = vi.fn(async () => {
      order.push("suppression");
    });
    const ingress = new DurableProviderIngress(store, undefined, { setSuppression });
    const stop = {
      schemaVersion: 1,
      source: "linq",
      providerEventId: "evt-stop",
      dedupeKey: "linq:partner:evt-stop",
      occurredAt: "2027-01-01T08:00:00.000Z",
      webhookVersion: "2026-02-03",
      partnerId: "partner",
      eventType: "message.received",
      scope: "direct",
      conversation: {
        id: "chat-1",
        kind: "direct",
        ownerHandle: "+15550000000",
        knownParticipantHandles: ["+15550000001"],
      },
      sender: { id: "sender-1", handle: "+15550000001", service: "iMessage" },
      message: {
        id: "message-stop",
        text: "STOP",
        attachments: [],
        replyTo: null,
        consentCommand: "stop",
      },
    } satisfies LinqInboundEvent;

    await expect(ingress.acceptLinq(stop)).resolves.toBeUndefined();
    expect(order).toEqual(["suppression", "inbox"]);
    expect(setSuppression).toHaveBeenCalledWith({
      externalChatId: "chat-1",
      externalHandle: "+15550000001",
      scope: "private",
      suppressed: true,
      occurredAt: "2027-01-01T08:00:00.000Z",
      sourceEventId: "linq:partner:evt-stop",
      reason: "stop_command",
    });

    setSuppression.mockRejectedValueOnce(new Error("safe fixture failure"));
    await expect(ingress.acceptLinq(stop)).rejects.toThrow("safe fixture failure");
    expect(order).toEqual(["suppression", "inbox"]);
  });
});

class MemoryOAuthStore implements GoogleOAuthStore {
  state:
    | {
        stateHash: string;
        householdId: string;
        adultId: string;
        returnConversationId: string;
        encryptedPayload: string;
      }
    | undefined;
  connection: Parameters<GoogleOAuthStore["upsertExternalConnection"]>[0] | undefined;

  async createOAuthState(input: Parameters<GoogleOAuthStore["createOAuthState"]>[0]) {
    this.state = input;
    return { stateId: "33333333-3333-4333-8333-333333333333" };
  }

  async consumeOAuthState(input: Parameters<GoogleOAuthStore["consumeOAuthState"]>[0]) {
    if (!this.state || this.state.stateHash !== input.stateHash) return null;
    const state = this.state;
    this.state = undefined;
    return { stateId: "33333333-3333-4333-8333-333333333333", ...state };
  }

  async upsertExternalConnection(input: Parameters<GoogleOAuthStore["upsertExternalConnection"]>[0]) {
    this.connection = input;
    return { connectionId: "44444444-4444-4444-8444-444444444444" };
  }
}

describe("GoogleOAuthHandoffService", () => {
  it("binds a signed DM handoff through one-time PKCE state to encrypted per-adult credentials", async () => {
    const now = new Date("2027-01-01T08:00:00.000Z");
    const key = randomBytes(32).toString("base64url");
    const secretBox = new SecretBox(key);
    const store = new MemoryOAuthStore();
    let createdState = "";
    const grant: GoogleOAuthGrant = {
      tokens: {
        accessToken: "access-secret",
        refreshToken: "refresh-secret",
        idToken: "identity-secret",
        expiresAt: "2027-01-01T09:00:00.000Z",
        scope: ["openid", "email", "https://www.googleapis.com/auth/gmail.readonly"],
        tokenType: "Bearer",
      },
      identity: { subject: "google-subject", email: "parent@example.com", emailVerified: true },
    };
    const completeCallback = vi.fn(async () => grant);
    const onConnected = vi.fn(async () => undefined);
    const service = new GoogleOAuthHandoffService({
      store,
      secretBox,
      handoffSecret: "h".repeat(40),
      now: () => now,
      onConnected,
      oauth: {
        createAuthorizationRequest({ state }) {
          createdState = state;
          return {
            url: `https://accounts.google.com/o/oauth2/v2/auth?state=${state}`,
            codeVerifier: "pkce-verifier-secret",
          };
        },
        completeCallback,
      },
    });
    const token = issueGoogleHandoffToken(
      {
        householdId,
        adultId,
        returnConversationId: "dm-chat-1",
        accountLabel: "Personal Gmail",
        loginHint: "parent@example.com",
        expiresAt: new Date(now.getTime() + 60_000),
      },
      "h".repeat(40),
      now,
    );

    await expect(service.start({ handoffToken: token })).resolves.toMatchObject({ kind: "redirect" });
    expect(store.state?.encryptedPayload).not.toContain("pkce-verifier-secret");
    await expect(
      service.complete({ state: createdState, code: "authorization-code", providerError: null }),
    ).resolves.toEqual({ kind: "connected" });
    expect(completeCallback).toHaveBeenCalledWith(
      expect.objectContaining({ codeVerifier: "pkce-verifier-secret" }),
    );
    expect(store.connection?.encryptedCredentials).not.toContain("refresh-secret");
    expect(
      secretBox.open(
        store.connection?.encryptedCredentials ?? "",
        `google-connection:${householdId}:${adultId}:google-subject`,
      ),
    ).toContain("refresh-secret");
    expect(onConnected).toHaveBeenCalledWith(
      expect.objectContaining({ householdId, adultId, returnConversationId: "dm-chat-1" }),
    );
    await expect(
      service.complete({ state: createdState, code: "replay", providerError: null }),
    ).resolves.toEqual({ kind: "expired" });
  });

  it("rejects expired or tampered handoff tokens without creating state", async () => {
    const now = new Date("2027-01-01T08:00:00.000Z");
    const store = new MemoryOAuthStore();
    const service = new GoogleOAuthHandoffService({
      store,
      secretBox: new SecretBox(randomBytes(32).toString("base64url")),
      handoffSecret: "s".repeat(40),
      now: () => now,
      oauth: {
        createAuthorizationRequest: () => {
          throw new Error("must not run");
        },
        completeCallback: async () => {
          throw new Error("must not run");
        },
      },
    });
    const expired = issueGoogleHandoffToken(
      {
        householdId,
        adultId,
        returnConversationId: "dm-chat-1",
        accountLabel: "Gmail",
        expiresAt: new Date(now.getTime() - 1),
      },
      "s".repeat(40),
      new Date(now.getTime() - 60_000),
    );
    await expect(service.start({ handoffToken: expired })).resolves.toEqual({ kind: "expired" });
    await expect(service.start({ handoffToken: `${expired}x` })).resolves.toEqual({ kind: "invalid" });
    expect(store.state).toBeUndefined();
  });
});

describe("ProductionReadiness", () => {
  it("requires both database reachability and every required integration", async () => {
    await expect(
      new ProductionReadiness(async () => undefined, { linq: true, model: true }).isReady(),
    ).resolves.toBe(true);
    await expect(
      new ProductionReadiness(async () => undefined, { linq: false, model: true }).isReady(),
    ).resolves.toBe(false);
    await expect(
      new ProductionReadiness(async () => Promise.reject(new Error("down")), {
        linq: true,
        model: true,
      }).isReady(),
    ).resolves.toBe(false);
  });
});
