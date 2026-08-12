import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";
import { DEFAULT_IMAGE_RETENTION_MS, decodeImageVaultKey, EncryptedImageVault } from "@florence/artifacts";
import { PostgresFlorenceRepository, type WorkerLease } from "@florence/database";
import { GoogleConnection } from "@florence/google";
import { LinqClient } from "@florence/linq";
import { GatewayWorkerRuntime } from "@florence/runtime";
import { createOpenAIResponsesGatewayFromEnv } from "@florence/runtime/openai";
import { FlorenceWorker } from "./index.js";

const dayInMilliseconds = 24 * 60 * 60 * 1_000;
const heartbeatIntervalMs = 10_000;
const workerLeaseWaitMs = 45_000;
const workerLeaseRetryMs = 1_000;

export type FlorenceWorkerProcess = {
  start(): Promise<void>;
  stop(): Promise<void>;
};

export function createWorkerFromEnv(env: NodeJS.ProcessEnv = process.env): FlorenceWorkerProcess {
  const workerId = `florence-worker:${process.pid}:${randomUUID()}`;
  const repository = new PostgresFlorenceRepository({
    connectionString: required(env.FLORENCE_DATABASE_URL, "FLORENCE_DATABASE_URL"),
    schema: required(env.FLORENCE_POSTGRES_SCHEMA, "FLORENCE_POSTGRES_SCHEMA"),
    applicationName: "florence-worker",
    ssl: env.NODE_ENV === "production",
  });
  const vault = new EncryptedImageVault({
    store: repository,
    encryptionKey: decodeImageVaultKey(env.FLORENCE_IMAGE_VAULT_KEY ?? ""),
    retentionMs: retentionMilliseconds(env.FLORENCE_IMAGE_RETENTION_DAYS),
  });
  const runtime = new GatewayWorkerRuntime(createOpenAIResponsesGatewayFromEnv(env), vault);
  const google = googleConnectionFromEnv(env, repository);
  const dispatcher = new FlorenceWorker(
    repository,
    runtime,
    new LinqClient({ apiKey: required(env.LINQ_API_KEY, "LINQ_API_KEY") }),
    { workerId },
    google,
    google,
    google,
  );

  let started = false;
  let startTask: Promise<void> | null = null;
  let stopTask: Promise<void> | null = null;
  let lease: WorkerLease | null = null;
  let heartbeatTimer: NodeJS.Timeout | null = null;
  let heartbeatTask: Promise<void> | null = null;
  let purgeTimer: NodeJS.Timeout | null = null;
  let purgeTask: Promise<void> | null = null;

  const purgeExpiredImages = () => {
    if (purgeTask) return;
    purgeTask = vault
      .purgeExpired()
      .then(() => undefined)
      .catch(() => logWorkerProcessError("image_vault_purge_failed"))
      .finally(() => {
        purgeTask = null;
      });
  };

  const heartbeat = () => {
    if (!lease || heartbeatTask) return;
    heartbeatTask = lease
      .heartbeat()
      .catch(() => {
        logWorkerProcessError("singleton_lease_heartbeat_failed");
        process.exitCode = 1;
        setImmediate(() => {
          void service.stop().catch(() => {
            logWorkerProcessError("shutdown_after_lease_failure_failed");
          });
        });
      })
      .finally(() => {
        heartbeatTask = null;
      });
  };

  const service: FlorenceWorkerProcess = {
    start() {
      if (stopTask) return Promise.reject(new Error("Florence worker process is stopped"));
      startTask ??= (async () => {
        try {
          await repository.ready();
          const leaseDeadline = Date.now() + workerLeaseWaitMs;
          while (!lease && !stopTask && Date.now() < leaseDeadline) {
            lease = await repository.acquireWorkerLease({ workerId });
            if (!lease) await pause(workerLeaseRetryMs);
          }
          if (!lease || stopTask) throw new Error("Another Florence worker owns the singleton lease");
          dispatcher.start();
          started = true;
          purgeExpiredImages();
          purgeTimer = setInterval(purgeExpiredImages, dayInMilliseconds);
          purgeTimer.unref();
          heartbeatTimer = setInterval(heartbeat, heartbeatIntervalMs);
          heartbeatTimer.unref();
        } catch (error) {
          try {
            if (started) await dispatcher.stop();
            await lease?.release();
          } finally {
            await repository.close();
          }
          throw error;
        }
      })();
      return startTask;
    },
    stop() {
      stopTask ??= (async () => {
        await startTask?.catch(() => undefined);
        if (purgeTimer) clearInterval(purgeTimer);
        if (heartbeatTimer) clearInterval(heartbeatTimer);
        purgeTimer = null;
        heartbeatTimer = null;
        try {
          if (started) await dispatcher.stop();
          await purgeTask;
          await heartbeatTask;
        } finally {
          try {
            await lease?.release();
          } finally {
            await repository.close();
          }
        }
      })();
      return stopTask;
    },
  };

  return service;
}

function googleConnectionFromEnv(
  env: NodeJS.ProcessEnv,
  repository: PostgresFlorenceRepository,
): GoogleConnection | null {
  const values = [
    env.GOOGLE_CLIENT_ID,
    env.GOOGLE_CLIENT_SECRET,
    env.GOOGLE_CREDENTIAL_KEY,
    env.FLORENCE_WEB_BASE_URL,
  ];
  const configuredCount = values.filter((value) => value?.trim()).length;
  if (configuredCount === 0) return null;
  if (configuredCount !== values.length) {
    throw new Error(
      "GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_CREDENTIAL_KEY, and FLORENCE_WEB_BASE_URL must be configured together",
    );
  }
  const [clientId, clientSecret, encodedKey, webBaseUrl] = values as string[];
  const encryptionKey = Buffer.from(encodedKey ?? "", "base64");
  if (encryptionKey.byteLength !== 32 || encryptionKey.toString("base64") !== encodedKey) {
    throw new Error("GOOGLE_CREDENTIAL_KEY must be a canonical base64-encoded 32-byte key");
  }
  return new GoogleConnection({
    store: repository,
    clientId: clientId ?? "",
    clientSecret: clientSecret ?? "",
    redirectUri: new URL("/oauth/google/callback", required(webBaseUrl, "FLORENCE_WEB_BASE_URL")).toString(),
    encryptionKey,
  });
}

function retentionMilliseconds(value: string | undefined): number {
  if (value === undefined) return DEFAULT_IMAGE_RETENTION_MS;
  const days = Number(value);
  if (!Number.isSafeInteger(days) || days < 1 || days > 365) {
    throw new Error("FLORENCE_IMAGE_RETENTION_DAYS must be an integer from 1 through 365");
  }
  return days * dayInMilliseconds;
}

function required(value: string | undefined, name: string): string {
  const trimmed = value?.trim();
  if (!trimmed) throw new Error(`${name} is required to start the Florence worker`);
  return trimmed;
}

function pause(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function runExecutable(): void {
  try {
    const service = createWorkerFromEnv();
    const shutdown = (signal: string) => {
      console.info(`Florence worker received ${signal}; shutting down.`);
      void service.stop().catch(() => {
        logWorkerProcessError("shutdown_failed");
        process.exitCode = 1;
      });
    };
    process.once("SIGTERM", () => shutdown("SIGTERM"));
    process.once("SIGINT", () => shutdown("SIGINT"));
    void service
      .start()
      .then(() => console.info("Florence worker started"))
      .catch(() => {
        logWorkerProcessError("startup_failed");
        process.exitCode = 1;
      });
  } catch {
    logWorkerProcessError("startup_failed");
    process.exitCode = 1;
  }
}

type WorkerProcessErrorCode =
  | "image_vault_purge_failed"
  | "singleton_lease_heartbeat_failed"
  | "shutdown_after_lease_failure_failed"
  | "shutdown_failed"
  | "startup_failed";

function logWorkerProcessError(code: WorkerProcessErrorCode): void {
  console.error(`[${code}] Florence worker process operation failed`);
}

const executablePath = process.argv[1];
if (executablePath && import.meta.url === pathToFileURL(executablePath).href) runExecutable();
