import { describe, expect, it, vi } from "vitest";
import {
  deterministicDeletionConfirmationDigest,
  deterministicDeletionRequestId,
  type OperatorHealthChecks,
  type OperatorHouseholdStore,
  type OperatorMaintenancePort,
  type OperatorOwnerDirectory,
  PeriodicMaintenanceCoordinator,
  ProductionHouseholdOperations,
} from "../../src/infrastructure/operator-services.js";

const HOUSEHOLD_ID = "11111111-1111-4111-8111-111111111111";
const OWNER_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2027-05-01T12:00:00.000Z");
const IDEMPOTENCY_KEY = "operator-delete-household-0001";

function healthyChecks(overrides: Partial<OperatorHealthChecks> = {}): OperatorHealthChecks {
  return {
    database: async () => true,
    model: async () => true,
    linq: async () => true,
    google: async () => true,
    worker: async () => true,
    ...overrides,
  };
}

class FakeOwnerDirectory implements OperatorOwnerDirectory {
  readonly calls: string[] = [];
  owner: string | null = OWNER_ID;
  error: Error | undefined;

  async firstActiveOwner(householdId: string): Promise<string | null> {
    this.calls.push(householdId);
    if (this.error !== undefined) throw this.error;
    return this.owner;
  }
}

class FakeOperatorStore implements OperatorHouseholdStore {
  readonly exportCalls: Parameters<OperatorHouseholdStore["exportHouseholdData"]>[0][] = [];
  readonly requestCalls: Parameters<OperatorHouseholdStore["requestHouseholdDeletion"]>[0][] = [];
  readonly confirmCalls: Parameters<OperatorHouseholdStore["confirmHouseholdDeletion"]>[0][] = [];
  readonly executeCalls: Parameters<OperatorHouseholdStore["executeHouseholdDeletion"]>[0][] = [];
  readonly tombstoneCalls: string[] = [];
  rawExport: Record<string, unknown> = exportFixture();
  tombstone: { requestId: string; householdId: string } | null = null;
  exportError: Error | undefined;
  requestError: Error | undefined;
  confirmError: Error | undefined;
  executeError: Error | undefined;
  confirmResult = true;
  createTombstoneOnExecute = true;

  async exportHouseholdData(input: Parameters<OperatorHouseholdStore["exportHouseholdData"]>[0]) {
    this.exportCalls.push(input);
    if (this.exportError !== undefined) throw this.exportError;
    return this.rawExport;
  }

  async requestHouseholdDeletion(input: Parameters<OperatorHouseholdStore["requestHouseholdDeletion"]>[0]) {
    this.requestCalls.push(input);
    if (this.requestError !== undefined) throw this.requestError;
    return { requestId: input.requestId };
  }

  async confirmHouseholdDeletion(input: Parameters<OperatorHouseholdStore["confirmHouseholdDeletion"]>[0]) {
    this.confirmCalls.push(input);
    if (this.confirmError !== undefined) throw this.confirmError;
    return this.confirmResult;
  }

  async executeHouseholdDeletion(input: Parameters<OperatorHouseholdStore["executeHouseholdDeletion"]>[0]) {
    this.executeCalls.push(input);
    if (this.createTombstoneOnExecute) {
      this.tombstone = { requestId: input.requestId, householdId: HOUSEHOLD_ID };
    }
    if (this.executeError !== undefined) throw this.executeError;
    return { householdId: HOUSEHOLD_ID, adultsDeleted: 2 };
  }

  async getDeletionTombstone(requestId: string) {
    this.tombstoneCalls.push(requestId);
    return this.tombstone?.requestId === requestId ? this.tombstone : null;
  }
}

function operationsHarness(input?: {
  healthChecks?: OperatorHealthChecks;
  ownerDirectory?: FakeOwnerDirectory;
  store?: FakeOperatorStore;
}) {
  const ownerDirectory = input?.ownerDirectory ?? new FakeOwnerDirectory();
  const store = input?.store ?? new FakeOperatorStore();
  return {
    ownerDirectory,
    store,
    operations: new ProductionHouseholdOperations({
      healthChecks: input?.healthChecks ?? healthyChecks(),
      ownerDirectory,
      store,
      now: () => NOW,
    }),
  };
}

function exportFixture(): Record<string, unknown> {
  return {
    schemaVersion: 99,
    exportedAt: "untrusted-time",
    config: {
      databaseUrl: "postgres://admin:database-secret@private.test/florence",
      operatorToken: "operator-config-secret",
    },
    household: {
      id: HOUSEHOLD_ID,
      name: "Family household",
      timezone: "America/Los_Angeles",
      status: "active",
      version: 4,
      created_at: new Date("2027-01-01T00:00:00Z"),
      database_url: "postgres://database-secret",
    },
    adults: [
      {
        id: OWNER_ID,
        display_name: "Alex",
        timezone: "America/Los_Angeles",
        role: "owner",
        status: "active",
      },
    ],
    channels: [
      {
        id: "channel-1",
        provider: "linq",
        channel_type: "private",
        external_chat_id: "chat-1",
        metadata: { webhook_secret: "linq-webhook-secret", label: "Primary conversation" },
      },
    ],
    connections: [
      {
        id: "connection-1",
        adult_id: OWNER_ID,
        provider: "google",
        label: "Personal Google",
        email: "alex@example.test",
        granted_scopes: ["gmail.readonly"],
        encrypted_credentials: "encrypted-refresh-token-ciphertext",
        metadata: { access_token: "google-access-secret", connected: true },
        last_synced_at: new Date("2027-04-30T00:00:00Z"),
      },
    ],
    sources: [
      {
        id: "source-1",
        owner_adult_id: OWNER_ID,
        visibility: "personal",
        provider: "google",
        external_id: "message-1",
        kind: "gmail_message",
        subject: "Private family schedule",
        content_hash: "sha256:content",
        encrypted_content: "private-message-ciphertext",
        metadata: { category: "school", client_secret: "metadata-secret" },
        revision: 1,
      },
    ],
    projection: {
      schema_version: 1,
      version: 4,
      state: {
        activeEpisodeIds: ["episode-1"],
        modelRoute: { maxInputTokens: 1_000, apiKey: "model-api-secret" },
      },
      state_redacted: true,
      redaction_reason: "viewer_scope",
    },
    applicationSnapshot: null,
    applicationCommits: [
      {
        idempotency_key: "commit-1",
        base_revision: 3,
        revision: 4,
        outcome: { status: "processed" },
        committed_at: new Date("2027-04-01T00:00:00Z"),
      },
    ],
    audits: [
      {
        sequence: "1",
        actor_kind: "adult",
        action: "export.requested",
        target_type: "household",
        details: { purpose: "owner export", signing_key: "audit-signing-secret" },
      },
    ],
  };
}

describe("ProductionHouseholdOperations status", () => {
  it("returns only fixed safe check states and degrades on false or thrown probes", async () => {
    const operations = operationsHarness({
      healthChecks: healthyChecks({
        model: async () => false,
        google: async () => {
          throw new Error("google-client-secret private@example.test");
        },
      }),
    }).operations;

    const status = await operations.status();

    expect(status).toEqual({
      status: "degraded",
      checks: {
        database: "ok",
        model: "degraded",
        linq: "ok",
        google: "unavailable",
        worker: "ok",
      },
    });
    expect(JSON.stringify(status)).not.toContain("google-client-secret");
    expect(JSON.stringify(status)).not.toContain("private@example.test");
  });

  it("reports ok only when database, model, Linq, Google, and worker are all ready", async () => {
    await expect(operationsHarness().operations.status()).resolves.toEqual({
      status: "ok",
      checks: {
        database: "ok",
        model: "ok",
        linq: "ok",
        google: "ok",
        worker: "ok",
      },
    });
  });
});

describe("ProductionHouseholdOperations export", () => {
  it("uses the deterministic active owner and returns a JSON-safe, credential-free export", async () => {
    const harness = operationsHarness();

    const artifact = await harness.operations.exportHousehold({ householdId: HOUSEHOLD_ID });

    expect(harness.ownerDirectory.calls).toEqual([HOUSEHOLD_ID]);
    expect(harness.store.exportCalls).toEqual([
      {
        householdId: HOUSEHOLD_ID,
        requestedByAdultId: OWNER_ID,
        exportedAt: NOW.toISOString(),
      },
    ]);
    expect(artifact).toMatchObject({
      schemaVersion: 1,
      exportedAt: NOW.toISOString(),
      household: {
        id: HOUSEHOLD_ID,
        name: "Family household",
        created_at: "2027-01-01T00:00:00.000Z",
      },
      connections: [
        {
          id: "connection-1",
          email: "alex@example.test",
          last_synced_at: "2027-04-30T00:00:00.000Z",
          metadata: { connected: true },
        },
      ],
      sources: [{ subject: "Private family schedule", metadata: { category: "school" } }],
      projection: {
        state: { activeEpisodeIds: ["episode-1"], modelRoute: { maxInputTokens: 1_000 } },
      },
    });
    const serialized = JSON.stringify(artifact);
    expect(() => JSON.parse(serialized)).not.toThrow();
    for (const forbidden of [
      "database-secret",
      "operator-config-secret",
      "linq-webhook-secret",
      "encrypted-refresh-token-ciphertext",
      "google-access-secret",
      "private-message-ciphertext",
      "metadata-secret",
      "model-api-secret",
      "audit-signing-secret",
    ]) {
      expect(serialized).not.toContain(forbidden);
    }
    expect(serialized).toContain("Private family schedule");
  });

  it("returns not found without exporting when no active owner exists", async () => {
    const ownerDirectory = new FakeOwnerDirectory();
    ownerDirectory.owner = null;
    const harness = operationsHarness({ ownerDirectory });

    await expect(harness.operations.exportHousehold({ householdId: HOUSEHOLD_ID })).resolves.toBeNull();
    expect(harness.store.exportCalls).toEqual([]);
  });

  it("fails with a fixed code when the store returns non-JSON or cyclic data", async () => {
    const store = new FakeOperatorStore();
    const cycle: Record<string, unknown> = {};
    cycle.self = cycle;
    store.rawExport = { ...exportFixture(), projection: { schema_version: 1, state: cycle } };
    const operations = operationsHarness({ store }).operations;

    await expect(operations.exportHousehold({ householdId: HOUSEHOLD_ID })).rejects.toEqual(
      expect.objectContaining({ code: "invalid_export", message: "invalid_export" }),
    );
  });
});

describe("ProductionHouseholdOperations deletion", () => {
  it("derives stable confirmation material, completes deletion, and deduplicates a replay", async () => {
    const harness = operationsHarness();
    const expectedRequestId = deterministicDeletionRequestId(HOUSEHOLD_ID, IDEMPOTENCY_KEY);
    const expectedDigest = deterministicDeletionConfirmationDigest(
      HOUSEHOLD_ID,
      IDEMPOTENCY_KEY,
      expectedRequestId,
    );

    await expect(
      harness.operations.deleteHousehold({
        householdId: HOUSEHOLD_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
      }),
    ).resolves.toBe("accepted");

    expect(expectedRequestId).toMatch(
      /^[a-f0-9]{8}-[a-f0-9]{4}-5[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/,
    );
    expect(expectedDigest).toMatch(/^[a-f0-9]{64}$/);
    expect(harness.store.requestCalls).toEqual([
      {
        requestId: expectedRequestId,
        householdId: HOUSEHOLD_ID,
        requestedByAdultId: OWNER_ID,
        confirmationDigest: expectedDigest,
      },
    ]);
    expect(harness.store.confirmCalls).toEqual([
      {
        requestId: expectedRequestId,
        confirmationDigest: expectedDigest,
        confirmedAt: NOW.toISOString(),
      },
    ]);
    expect(harness.store.executeCalls).toEqual([
      { requestId: expectedRequestId, completedAt: NOW.toISOString() },
    ]);

    await expect(
      harness.operations.deleteHousehold({
        householdId: HOUSEHOLD_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
      }),
    ).resolves.toBe("already_deleted");
    expect(harness.store.requestCalls).toHaveLength(1);
    expect(harness.store.executeCalls).toHaveLength(1);
  });

  it("returns not found when neither a tombstone nor an active owner exists", async () => {
    const ownerDirectory = new FakeOwnerDirectory();
    ownerDirectory.owner = null;
    const operations = operationsHarness({ ownerDirectory }).operations;

    await expect(
      operations.deleteHousehold({
        householdId: HOUSEHOLD_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
      }),
    ).resolves.toBe("not_found");
  });

  it("resumes a deterministic pending request after an ambiguous create response", async () => {
    const store = new FakeOperatorStore();
    store.requestError = new Error("database-password ambiguous response");
    const operations = operationsHarness({ store }).operations;

    await expect(
      operations.deleteHousehold({
        householdId: HOUSEHOLD_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
      }),
    ).resolves.toBe("accepted");
    expect(store.confirmCalls).toHaveLength(1);
    expect(store.executeCalls).toHaveLength(1);
  });

  it("maps an ambiguous execution with a matching tombstone to already deleted", async () => {
    const store = new FakeOperatorStore();
    store.executeError = new Error("private deletion response was lost");
    const operations = operationsHarness({ store }).operations;

    await expect(
      operations.deleteHousehold({
        householdId: HOUSEHOLD_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
      }),
    ).resolves.toBe("already_deleted");
  });

  it("wraps terminal store errors without exposing raw implementation details", async () => {
    const store = new FakeOperatorStore();
    store.createTombstoneOnExecute = false;
    store.executeError = new Error("oauth-refresh-token private-family-data");
    const operations = operationsHarness({ store }).operations;

    await expect(
      operations.deleteHousehold({
        householdId: HOUSEHOLD_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
      }),
    ).rejects.toEqual(
      expect.objectContaining({ code: "operation_unavailable", message: "operation_unavailable" }),
    );
  });
});

class FakeMaintenance implements OperatorMaintenancePort {
  readonly sourceCalls: string[] = [];
  readonly inboxCalls: string[] = [];
  readonly deletionCalls: Array<{ completedAt: string; limit: number }> = [];
  error: Error | undefined;

  async purgeExpiredSourceContent(asOf: string): Promise<number> {
    this.sourceCalls.push(asOf);
    if (this.error !== undefined) throw this.error;
    return 2;
  }

  async purgeExpiredProviderInbox(asOf: string): Promise<number> {
    this.inboxCalls.push(asOf);
    if (this.error !== undefined) throw this.error;
    return 3;
  }

  async executeConfirmedHouseholdDeletions(input: { completedAt: string; limit: number }) {
    this.deletionCalls.push(input);
    if (this.error !== undefined) throw this.error;
    return 1;
  }
}

describe("PeriodicMaintenanceCoordinator", () => {
  it("uses one clock instant for bounded retention and confirmed-deletion work", async () => {
    const maintenance = new FakeMaintenance();
    const coordinator = new PeriodicMaintenanceCoordinator({
      maintenance,
      deletionBatchSize: 10,
      now: () => NOW,
    });

    await expect(coordinator.runOnce()).resolves.toEqual({
      ranAt: NOW.toISOString(),
      sourceItemsPurged: 2,
      providerInboxItemsPurged: 3,
      householdDeletionsCompleted: 1,
    });
    expect(maintenance.sourceCalls).toEqual([NOW.toISOString()]);
    expect(maintenance.inboxCalls).toEqual([NOW.toISOString()]);
    expect(maintenance.deletionCalls).toEqual([{ completedAt: NOW.toISOString(), limit: 10 }]);
  });

  it("runs periodically until aborted and never logs a maintenance error", async () => {
    const maintenance = new FakeMaintenance();
    const controller = new AbortController();
    const wait = vi.fn(async (_milliseconds: number, _signal: AbortSignal) => {
      controller.abort();
    });
    const coordinator = new PeriodicMaintenanceCoordinator({
      maintenance,
      intervalMs: 1_000,
      now: () => NOW,
      wait,
    });

    await expect(coordinator.run(controller.signal)).resolves.toBeUndefined();
    expect(maintenance.sourceCalls).toHaveLength(1);
    expect(wait).toHaveBeenCalledWith(1_000, controller.signal);

    maintenance.error = new Error("private-source-content connector-secret");
    await expect(coordinator.runOnce()).rejects.toEqual(
      expect.objectContaining({ code: "maintenance_unavailable", message: "maintenance_unavailable" }),
    );
  });
});
