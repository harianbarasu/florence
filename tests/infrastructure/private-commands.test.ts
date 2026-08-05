import { describe, expect, it, vi } from "vitest";
import type { GoogleSyncConnection } from "../../src/infrastructure/google-sync.js";
import { PrivateGoogleCommandService } from "../../src/infrastructure/private-commands.js";

const HOUSEHOLD = "00000000-0000-4000-8000-000000000001";
const ADULT = "00000000-0000-4000-8000-000000000002";
const CONNECTION = "00000000-0000-4000-8000-000000000003";

function connection(overrides: Partial<GoogleSyncConnection> = {}): GoogleSyncConnection {
  return {
    id: CONNECTION,
    householdId: HOUSEHOLD,
    adultId: ADULT,
    provider: "google",
    externalAccountId: "google-subject",
    email: "parent@example.test",
    encryptedCredentials: "ciphertext",
    grantedScopes: ["https://www.googleapis.com/auth/gmail.readonly"],
    status: "active",
    cursor: {},
    metadata: { credentialAadVersion: 1, accountLabel: "Personal" },
    ...overrides,
  };
}

function setup(
  connections: readonly GoogleSyncConnection[] = [connection()],
  options: { calendarQueue?: boolean; gmailSyncEnabled?: boolean } = {},
) {
  const enqueueApplicationIntent = vi.fn(async () => ({ rowId: "outbox-1" }));
  const enqueueGoogleSyncWork = vi.fn(async () => ({ jobId: "job-1", created: true }));
  const enqueueCalendarSyncWork = vi.fn(async () => ({ jobId: "calendar-job-1", created: true }));
  const issue = vi.fn(() => "https://florence.example.test/oauth/google/start?handoff=opaque");
  const service = new PrivateGoogleCommandService({
    outbox: { enqueueApplicationIntent },
    directory: { listOwnedGoogleConnections: vi.fn(async () => connections) },
    googleQueue: { enqueueGoogleSyncWork },
    ...(options.calendarQueue ? { calendarQueue: { enqueueCalendarSyncWork } } : {}),
    ...(options.gmailSyncEnabled === undefined ? {} : { gmailSyncEnabled: options.gmailSyncEnabled }),
    linkIssuer: { issue },
  });
  return { service, enqueueApplicationIntent, enqueueGoogleSyncWork, enqueueCalendarSyncWork, issue };
}

const command = {
  householdId: HOUSEHOLD,
  adultId: ADULT,
  channelId: "dm-1",
  messageId: "message-1",
  occurredAt: "2026-08-05T16:00:00Z",
  idempotencyKey: "linq:event-1",
};

describe("PrivateGoogleCommandService", () => {
  it("issues an owner-scoped, private OAuth handoff link", async () => {
    const harness = setup();
    await expect(
      harness.service.handle({
        ...command,
        text: "Connect my personal Gmail parent@example.test",
      }),
    ).resolves.toEqual({ handled: true, classification: "google:connect_link_issued" });
    expect(harness.issue).toHaveBeenCalledWith({
      householdId: HOUSEHOLD,
      adultId: ADULT,
      returnConversationId: "dm-1",
      accountLabel: "personal Google account",
      loginHint: "parent@example.test",
    });
    expect(harness.enqueueApplicationIntent).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: "conversation.send",
        targetScope: { kind: "personal", adultId: ADULT },
        body: expect.stringContaining("private, time-limited link"),
      }),
    );
  });

  it("queues durable local-first revocation for an explicit account", async () => {
    const harness = setup();
    await expect(
      harness.service.handle({ ...command, text: "Disconnect parent@example.test Google account" }),
    ).resolves.toEqual({ handled: true, classification: "google:disconnect_queued" });
    expect(harness.enqueueGoogleSyncWork).toHaveBeenCalledWith(
      expect.objectContaining({
        householdId: HOUSEHOLD,
        work: {
          kind: "revoke",
          householdId: HOUSEHOLD,
          adultId: ADULT,
          connectionId: CONNECTION,
        },
      }),
    );
  });

  it("uses Calendar-owned revocation when Calendar is the configured Google runtime", async () => {
    const harness = setup([connection()], { calendarQueue: true, gmailSyncEnabled: false });
    await harness.service.handle({ ...command, text: "Disconnect parent@example.test Google account" });
    expect(harness.enqueueGoogleSyncWork).not.toHaveBeenCalled();
    expect(harness.enqueueCalendarSyncWork).toHaveBeenCalledWith(
      expect.objectContaining({
        work: {
          kind: "revoke",
          householdId: HOUSEHOLD,
          adultId: ADULT,
          connectionId: CONNECTION,
        },
      }),
    );
  });

  it("does not guess among multiple accounts", async () => {
    const harness = setup([
      connection(),
      connection({
        id: "00000000-0000-4000-8000-000000000004",
        email: "work@example.test",
        metadata: { credentialAadVersion: 1, accountLabel: "Work" },
      }),
    ]);
    await expect(
      harness.service.handle({ ...command, text: "Disconnect my Google account" }),
    ).resolves.toEqual({ handled: true, classification: "google:disconnect_needs_account" });
    expect(harness.enqueueGoogleSyncWork).not.toHaveBeenCalled();
  });

  it("starts recent-first progressive synchronization after OAuth", async () => {
    const harness = setup([connection()], { calendarQueue: true });
    await harness.service.onGoogleConnected({
      householdId: HOUSEHOLD,
      adultId: ADULT,
      returnConversationId: "dm-1",
      connectionId: CONNECTION,
      accountLabel: "Personal",
      email: "parent@example.test",
      grantedScopes: [
        "https://www.googleapis.com/auth/gmail.readonly",
        "https://www.googleapis.com/auth/calendar.readonly",
      ],
    });
    expect(harness.enqueueGoogleSyncWork).toHaveBeenCalledWith({
      householdId: HOUSEHOLD,
      idempotencyKey: `google:${CONNECTION}:start`,
      work: {
        kind: "start",
        householdId: HOUSEHOLD,
        adultId: ADULT,
        connectionId: CONNECTION,
        depth: "full_history",
      },
    });
    expect(harness.enqueueApplicationIntent).toHaveBeenCalledWith(
      expect.objectContaining({ body: expect.stringContaining("most recent 90 days") }),
    );
  });

  it("keeps onboarding incomplete when required Gmail sync is unavailable", async () => {
    const harness = setup([connection()], { calendarQueue: true, gmailSyncEnabled: false });
    await harness.service.onGoogleConnected({
      householdId: HOUSEHOLD,
      adultId: ADULT,
      returnConversationId: "dm-1",
      connectionId: CONNECTION,
      accountLabel: "Personal",
      email: "parent@example.test",
      grantedScopes: [
        "https://www.googleapis.com/auth/gmail.readonly",
        "https://www.googleapis.com/auth/calendar.readonly",
      ],
    });
    expect(harness.enqueueGoogleSyncWork).not.toHaveBeenCalled();
    expect(harness.enqueueApplicationIntent).toHaveBeenCalledWith(
      expect.objectContaining({
        body: expect.stringContaining("Gmail synchronization service is unavailable"),
      }),
    );
  });

  it("leaves unrelated private conversation to Florence", async () => {
    const harness = setup();
    await expect(harness.service.handle({ ...command, text: "Pickup changed to 4:30" })).resolves.toEqual({
      handled: false,
    });
    expect(harness.enqueueApplicationIntent).not.toHaveBeenCalled();
  });
});
