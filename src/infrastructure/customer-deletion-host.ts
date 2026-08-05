import { setTimeout as sleep } from "node:timers/promises";
import { z } from "zod";
import { GoogleAdapterError } from "../adapters/google/errors.js";
import { type GoogleTokenSet, googleTokenSetSchema } from "../adapters/google/oauth.js";
import type { SecretBox } from "../security/secret-box.js";
import type {
  CustomerCleanupConnection,
  CustomerDeletionCleanupLease,
  PostgresCustomerDataControlStore,
} from "./customer-data-control-store.js";
import { googleConnectionCredentialsAad } from "./google-sync.js";
import { startLeaseHeartbeat } from "./lease-heartbeat.js";

export interface CustomerDeletionGmailPort {
  stopWatch(accessToken: string): Promise<void>;
}

export interface CustomerDeletionCalendarPort {
  stopChannel(input: { accessToken: string; channelId: string; resourceId: string }): Promise<void>;
}

export interface CustomerDeletionGoogleCredentialPort {
  refresh(tokens: GoogleTokenSet): Promise<GoogleTokenSet>;
  revoke(tokens: GoogleTokenSet): Promise<void>;
}

export interface CustomerDeletionRemoteCleanup {
  execute(
    lease: CustomerDeletionCleanupLease,
    signal?: AbortSignal,
  ): Promise<{ readonly status: "succeeded" } | { readonly status: "retry"; readonly safeErrorCode: string }>;
}

export class GoogleCustomerDeletionCleanup implements CustomerDeletionRemoteCleanup {
  public constructor(
    private readonly options: {
      store: Pick<PostgresCustomerDataControlStore, "loadCleanupConnection" | "loadCleanupCalendarChannel">;
      gmail: CustomerDeletionGmailPort;
      calendar: CustomerDeletionCalendarPort;
      oauth: CustomerDeletionGoogleCredentialPort;
      secretBox: Pick<SecretBox, "open">;
      now?: () => Date;
    },
  ) {}

  public async execute(
    lease: CustomerDeletionCleanupLease,
    signal?: AbortSignal,
  ): Promise<{ status: "succeeded" } | { status: "retry"; safeErrorCode: string }> {
    if (lease.kind === "local.finalize") return { status: "succeeded" };
    if (signal?.aborted) return { status: "retry", safeErrorCode: "cleanup_aborted" };
    if (lease.connectionId === null) return { status: "retry", safeErrorCode: "connection_missing" };
    const connection = await this.options.store.loadCleanupConnection({
      requestId: lease.requestId,
      householdId: lease.householdId,
      connectionId: lease.connectionId,
    });
    if (!connection) return { status: "retry", safeErrorCode: "credential_unavailable" };
    let tokens: GoogleTokenSet;
    try {
      tokens = decryptTokens(connection, this.options.secretBox);
      if (lease.kind === "google.oauth.revoke") {
        await this.options.oauth.revoke(tokens);
        return { status: "succeeded" };
      }
      const operation = async (accessToken: string) => {
        if (lease.kind === "google.gmail_watch.stop") {
          await this.options.gmail.stopWatch(accessToken);
          return;
        }
        if (lease.calendarChannelId === null) throw new Error("calendar_channel_missing");
        const channel = await this.options.store.loadCleanupCalendarChannel({
          requestId: lease.requestId,
          householdId: lease.householdId,
          connectionId: lease.connectionId as string,
          channelId: lease.calendarChannelId,
        });
        if (!channel) return;
        await this.options.calendar.stopChannel({
          accessToken,
          channelId: channel.channelId,
          resourceId: channel.resourceId,
        });
      };
      try {
        await operation(tokens.accessToken);
      } catch (error) {
        if (error instanceof GoogleAdapterError && error.code === "not_found") {
          return { status: "succeeded" };
        }
        if (!(error instanceof GoogleAdapterError) || error.code !== "unauthorized") throw error;
        try {
          tokens = await this.options.oauth.refresh(tokens);
        } catch (refreshError) {
          if (isPermanentlyInvalidGrant(refreshError)) return { status: "succeeded" };
          throw refreshError;
        }
        if (signal?.aborted) return { status: "retry", safeErrorCode: "cleanup_aborted" };
        try {
          await operation(tokens.accessToken);
        } catch (retryError) {
          if (
            retryError instanceof GoogleAdapterError &&
            (retryError.code === "not_found" || retryError.code === "unauthorized")
          ) {
            return { status: "succeeded" };
          }
          throw retryError;
        }
      }
      return { status: "succeeded" };
    } catch (error) {
      if (
        error instanceof GoogleAdapterError &&
        (error.code === "not_found" ||
          (lease.kind === "google.oauth.revoke" && isPermanentlyInvalidGrant(error)))
      ) {
        // Missing remote state or a permanently invalid grant leaves no live authority to clean up.
        return { status: "succeeded" };
      }
      return {
        status: "retry",
        safeErrorCode:
          error instanceof GoogleAdapterError && error.retryable
            ? "google_cleanup_transient"
            : "google_cleanup_unconfirmed",
      };
    }
  }
}

function isPermanentlyInvalidGrant(error: unknown): error is GoogleAdapterError {
  return (
    error instanceof GoogleAdapterError &&
    !error.retryable &&
    (error.code === "invalid_request" || error.code === "unauthorized")
  );
}

export interface CustomerDeletionHostReport {
  readonly claimed: number;
  readonly completed: number;
  readonly retried: number;
  readonly finalized: number;
  readonly lostLease: number;
}

/** Small durable cleanup saga host. Remote uncertainty always retries while the household stays fenced. */
export class CustomerDeletionHost {
  readonly #owner: string;
  readonly #pollIntervalMs: number;
  readonly #leaseSeconds: number;
  readonly #batchSize: number;
  readonly #now: () => Date;

  public constructor(
    private readonly options: {
      store: PostgresCustomerDataControlStore;
      remote: CustomerDeletionRemoteCleanup;
      owner: string;
      pollIntervalMs?: number;
      leaseSeconds?: number;
      batchSize?: number;
      now?: () => Date;
    },
  ) {
    this.#owner = z.string().min(1).max(200).parse(options.owner);
    this.#pollIntervalMs = z
      .number()
      .int()
      .min(250)
      .max(60_000)
      .parse(options.pollIntervalMs ?? 1_000);
    this.#leaseSeconds = z
      .number()
      .int()
      .positive()
      .max(3_600)
      .parse(options.leaseSeconds ?? 120);
    this.#batchSize = z
      .number()
      .int()
      .positive()
      .max(100)
      .parse(options.batchSize ?? 10);
    this.#now = options.now ?? (() => new Date());
  }

  public async run(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      await this.runOnce(signal);
      if (!signal.aborted) await sleep(this.#pollIntervalMs, undefined, { signal });
    }
  }

  public async runOnce(signal?: AbortSignal): Promise<CustomerDeletionHostReport> {
    const report = { claimed: 0, completed: 0, retried: 0, finalized: 0, lostLease: 0 };
    const leases = await this.options.store.claimCleanupSteps({
      owner: this.#owner,
      limit: this.#batchSize,
      leaseSeconds: this.#leaseSeconds,
    });
    report.claimed = leases.length;
    for (const lease of leases) {
      if (lease.kind === "local.finalize") {
        const completedAt = this.#instantNow();
        try {
          const finalized = await this.options.store.finalizeDeletion({
            rowId: lease.rowId,
            leaseToken: lease.leaseToken,
            completedAt,
          });
          if (finalized.completed) report.finalized += 1;
          else report.lostLease += 1;
        } catch {
          const settled = await this.options.store.retryCleanupStep({
            rowId: lease.rowId,
            leaseToken: lease.leaseToken,
            retryAt: new Date(Date.parse(completedAt) + retryDelayMs(lease.attempt)).toISOString(),
            safeErrorCode: "local_cleanup_unavailable",
          });
          if (settled) report.retried += 1;
          else report.lostLease += 1;
        }
        continue;
      }
      const heartbeat = await startLeaseHeartbeat({
        leaseSeconds: this.#leaseSeconds,
        ...(signal === undefined ? {} : { upstreamSignal: signal }),
        renew: () =>
          this.options.store.renewCleanupStepLease({
            rowId: lease.rowId,
            leaseToken: lease.leaseToken,
            leaseSeconds: this.#leaseSeconds,
          }),
      });
      if (!heartbeat.owned) {
        await heartbeat.stop();
        report.lostLease += 1;
        continue;
      }
      let outcome: Awaited<ReturnType<CustomerDeletionRemoteCleanup["execute"]>>;
      try {
        outcome = await this.options.remote.execute(lease, heartbeat.signal);
      } catch {
        outcome = { status: "retry", safeErrorCode: "remote_cleanup_unavailable" };
      }
      if (!(await heartbeat.stop())) {
        report.lostLease += 1;
        continue;
      }
      const completedAt = this.#instantNow();
      if (outcome.status === "succeeded") {
        const settled = await this.options.store.completeCleanupStep({
          rowId: lease.rowId,
          leaseToken: lease.leaseToken,
          completedAt,
        });
        if (settled) report.completed += 1;
        else report.lostLease += 1;
      } else {
        const retryAt = new Date(Date.parse(completedAt) + retryDelayMs(lease.attempt)).toISOString();
        const settled = await this.options.store.retryCleanupStep({
          rowId: lease.rowId,
          leaseToken: lease.leaseToken,
          retryAt,
          safeErrorCode: outcome.safeErrorCode,
        });
        if (settled) report.retried += 1;
        else report.lostLease += 1;
      }
    }
    return report;
  }

  #instantNow(): string {
    const value = this.#now();
    if (!Number.isFinite(value.getTime())) throw new Error("Deletion host clock is invalid");
    return value.toISOString();
  }
}

function decryptTokens(
  connection: CustomerCleanupConnection,
  secretBox: Pick<SecretBox, "open">,
): GoogleTokenSet {
  if (connection.metadata.credentialAadVersion !== 1) throw new Error("credential_aad_invalid");
  return googleTokenSetSchema.parse(
    JSON.parse(secretBox.open(connection.encryptedCredentials, googleConnectionCredentialsAad(connection))),
  );
}

function retryDelayMs(attempt: number): number {
  return Math.min(60 * 60_000, 5_000 * 2 ** Math.min(10, Math.max(0, attempt - 1)));
}
