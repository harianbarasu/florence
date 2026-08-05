import { describe, expect, it, vi } from "vitest";
import {
  adaptProviderItemProcessor,
  type BackgroundLoop,
  createProductionComposition,
  ProductionBackgroundRuntime,
} from "../src/composition.js";
import { loadConfig } from "../src/config.js";
import { GoogleSyncError } from "../src/infrastructure/google-sync.js";
import { ProviderProcessingError } from "../src/infrastructure/provider-processor.js";
import type { ProviderInboxLease } from "../src/infrastructure/worker-host.js";

const NOW = "2027-02-01T08:00:00Z";

function providerLease(): ProviderInboxLease {
  return {
    id: "00000000-0000-4000-8000-000000000001",
    provider: "linq",
    idempotencyKey: "linq:event:one",
    payloadHash: `sha256:${"0".repeat(64)}`,
    authentication: { verified: true },
    eventKind: "message.received",
    occurredAt: NOW,
    payload: { event: "fixture" },
    attempt: 1,
    maxAttempts: 8,
    leaseToken: "00000000-0000-4000-8000-000000000002",
    leaseExpiresAt: "2027-02-01T08:05:00Z",
  };
}

function inertApplication() {
  return {
    process: vi.fn(async () => {
      throw new Error("not used");
    }),
    executeOutbox: vi.fn(async () => {
      throw new Error("not used");
    }),
  };
}

function config(overrides: NodeJS.ProcessEnv = {}) {
  return loadConfig({
    NODE_ENV: "test",
    FLORENCE_DATABASE_URL: "postgres://florence:florence@127.0.0.1:55432/florence_test",
    FLORENCE_WEB_BASE_URL: "https://florence.example.test",
    FLORENCE_TOKEN_ENCRYPTION_KEY: Buffer.alloc(32, 7).toString("base64url"),
    FLORENCE_ADMIN_API_KEY: "operator-test-key-with-enough-bytes",
    OPENAI_API_KEY: "test-model-key",
    ...overrides,
  });
}

describe("production composition", () => {
  it("maps provider processing outcomes to explicit durable certainty", async () => {
    const item = providerLease();
    const application = inertApplication();
    const signal = new AbortController().signal;

    const resolved = adaptProviderItemProcessor({
      process: vi.fn(async () => ({
        householdId: "00000000-0000-4000-8000-000000000010",
        resolution: { classification: "fixture", revision: 2 },
      })),
    });
    await expect(resolved.process(item, application, signal)).resolves.toEqual({
      status: "resolved",
      householdId: "00000000-0000-4000-8000-000000000010",
      resolution: { classification: "fixture", revision: 2 },
    });

    const providerRetry = adaptProviderItemProcessor({
      process: vi.fn(async () => {
        throw new ProviderProcessingError("linq_temporarily_unavailable", true, "private detail");
      }),
    });
    await expect(providerRetry.process(item, application, signal)).resolves.toEqual({
      status: "retryable_failure",
      errorCode: "linq_temporarily_unavailable",
    });

    const googlePermanent = adaptProviderItemProcessor({
      process: vi.fn(async () => {
        throw new GoogleSyncError("private detail", "not_authorized", false);
      }),
    });
    await expect(googlePermanent.process(item, application, signal)).resolves.toEqual({
      status: "permanent_failure",
      errorCode: "google_sync.not_authorized",
    });

    const unknown = adaptProviderItemProcessor({
      process: vi.fn(async () => {
        throw new Error("credential-shaped private failure");
      }),
    });
    await expect(unknown.process(item, application, signal)).resolves.toEqual({
      status: "retryable_failure",
      errorCode: "provider_processing_failure",
    });
  });

  it("runs background loops once, shares overlapping runs, and stops all loops on abort", async () => {
    const calls: string[] = [];
    const stopped: string[] = [];
    const loop = (name: string): BackgroundLoop => ({
      async run(signal) {
        calls.push(name);
        await new Promise<void>((resolve) => {
          signal.addEventListener(
            "abort",
            () => {
              stopped.push(name);
              resolve();
            },
            { once: true },
          );
        });
      },
    });
    const runtime = new ProductionBackgroundRuntime([loop("application"), loop("google")]);
    const controller = new AbortController();

    const first = runtime.run(controller.signal);
    const overlapping = runtime.run(controller.signal);
    expect(overlapping).toBe(first);
    expect(runtime.isHealthy()).toBe(true);
    expect(calls).toEqual(["application", "google"]);
    controller.abort();
    await first;

    expect(stopped).toEqual(["application", "google"]);
    expect(runtime.isHealthy()).toBe(false);
  });

  it("aborts sibling loops and reports a fatal background failure", async () => {
    let siblingAborted = false;
    const failure = new Error("safe fixture failure");
    const runtime = new ProductionBackgroundRuntime([
      { run: async () => Promise.reject(failure) },
      {
        async run(signal) {
          await new Promise<void>((resolve) => {
            signal.addEventListener(
              "abort",
              () => {
                siblingAborted = true;
                resolve();
              },
              { once: true },
            );
          });
        },
      },
    ]);

    await expect(runtime.run(new AbortController().signal)).rejects.toBe(failure);
    expect(siblingAborted).toBe(true);
    expect(runtime.isHealthy()).toBe(false);
  });

  it("builds with Google disabled as one fail-closed capability", async () => {
    const composition = await createProductionComposition({ config: config(), migrate: false });
    try {
      await expect(
        composition.http.services.googleOAuth.start({ handoffToken: "not-a-real-token" }),
      ).resolves.toEqual({ kind: "invalid" });
      await expect(composition.http.services.readiness.isReady()).resolves.toBe(false);
      expect(composition.http.logger).toBe(false);
      expect(composition.worker.host).toBeDefined();
    } finally {
      await composition.close();
    }
  });

  it("does not expose webhook ingress for partial provider configuration", async () => {
    const composition = await createProductionComposition({
      config: config({
        GOOGLE_PUBSUB_VERIFICATION_TOKEN: "partial-google-token",
        LINQ_WEBHOOK_SECRET: "partial-linq-secret",
      }),
      migrate: false,
    });
    try {
      expect(composition.http.config.gmailPubSubVerificationToken).toBeNull();
      expect(composition.http.config.linqWebhook).toBeNull();
    } finally {
      await composition.close();
    }
  });
});
