import { setTimeout as sleep } from "node:timers/promises";
import { z } from "zod";
import type { HouseholdOperations, OperatorStatus } from "../http/contracts.js";

const NonNegativeCountSchema = z.number().int().nonnegative();
const STATUS_CHECK_NAMES = ["database", "model", "linq", "google", "worker"] as const;
type StatusCheckName = (typeof STATUS_CHECK_NAMES)[number];

export type OperatorHealthProbe = () => Promise<boolean>;

export interface OperatorHealthChecks {
  readonly database: OperatorHealthProbe;
  readonly model: OperatorHealthProbe;
  readonly linq: OperatorHealthProbe;
  readonly google: OperatorHealthProbe;
  readonly worker: OperatorHealthProbe;
}

export interface OperatorStatusStore {
  countDeadSemanticTimers?(): Promise<number>;
}

export interface ProductionHouseholdOperationsOptions {
  readonly healthChecks: OperatorHealthChecks;
  readonly store: OperatorStatusStore;
}

/** Read-only operational health for the authenticated control plane. */
export class ProductionHouseholdOperations implements HouseholdOperations {
  readonly #healthChecks: OperatorHealthChecks;
  readonly #store: OperatorStatusStore;

  constructor(options: ProductionHouseholdOperationsOptions) {
    this.#healthChecks = options.healthChecks;
    this.#store = options.store;
  }

  async status(): Promise<OperatorStatus> {
    const [results, semanticTimers] = await Promise.all([
      Promise.all(
        STATUS_CHECK_NAMES.map(
          async (name) => [name, await safeHealthCheck(this.#healthChecks[name])] as const,
        ),
      ),
      semanticTimerStatus(this.#store.countDeadSemanticTimers?.bind(this.#store)),
    ]);
    const checks: Record<StatusCheckName, "ok" | "degraded" | "unavailable"> = {
      database: "unavailable",
      model: "unavailable",
      linq: "unavailable",
      google: "unavailable",
      worker: "unavailable",
    };
    for (const [name, status] of results) checks[name] = status;
    return {
      status:
        Object.values(checks).every((value) => value === "ok") && semanticTimers.status === "ok"
          ? "ok"
          : "degraded",
      checks,
      semanticTimers,
    };
  }
}

async function safeHealthCheck(probe: OperatorHealthProbe): Promise<"ok" | "degraded" | "unavailable"> {
  try {
    return (await probe()) ? "ok" : "degraded";
  } catch {
    return "unavailable";
  }
}

async function semanticTimerStatus(
  countDeadTimers: (() => Promise<number>) | undefined,
): Promise<NonNullable<OperatorStatus["semanticTimers"]>> {
  if (!countDeadTimers) return { status: "unavailable", deadCount: null };
  try {
    const deadCount = NonNegativeCountSchema.parse(await countDeadTimers());
    return { status: deadCount === 0 ? "ok" : "degraded", deadCount };
  } catch {
    return { status: "unavailable", deadCount: null };
  }
}

export interface OperatorMaintenancePort {
  purgeExpiredSourceContent(asOf: string): Promise<number>;
  purgeExpiredProviderInbox(asOf: string): Promise<number>;
  purgeExpiredOAuthStates(asOf: string): Promise<number>;
}

export interface MaintenanceRunReceipt {
  readonly ranAt: string;
  readonly sourceItemsPurged: number;
  readonly providerInboxItemsPurged: number;
  readonly oauthStatesPurged: number;
}

export type MaintenanceWait = (milliseconds: number, signal: AbortSignal) => Promise<void>;

export interface PeriodicMaintenanceCoordinatorOptions {
  readonly maintenance: OperatorMaintenancePort;
  readonly intervalMs?: number;
  readonly now?: () => Date;
  readonly wait?: MaintenanceWait;
}

/** A bounded loop over app-owned retention work. */
export class PeriodicMaintenanceCoordinator {
  readonly #maintenance: OperatorMaintenancePort;
  readonly #intervalMs: number;
  readonly #now: () => Date;
  readonly #wait: MaintenanceWait;

  constructor(options: PeriodicMaintenanceCoordinatorOptions) {
    this.#maintenance = options.maintenance;
    this.#intervalMs = z
      .number()
      .int()
      .min(1_000)
      .max(86_400_000)
      .parse(options.intervalMs ?? 60_000);
    this.#now = options.now ?? (() => new Date());
    this.#wait = options.wait ?? defaultMaintenanceWait;
  }

  async runOnce(): Promise<MaintenanceRunReceipt> {
    try {
      const ranAt = this.#now().toISOString();
      const [sourceItemsPurged, providerInboxItemsPurged, oauthStatesPurged] = await Promise.all([
        this.#maintenance.purgeExpiredSourceContent(ranAt),
        this.#maintenance.purgeExpiredProviderInbox(ranAt),
        this.#maintenance.purgeExpiredOAuthStates(ranAt),
      ]);
      return {
        ranAt,
        sourceItemsPurged: NonNegativeCountSchema.parse(sourceItemsPurged),
        providerInboxItemsPurged: NonNegativeCountSchema.parse(providerInboxItemsPurged),
        oauthStatesPurged: NonNegativeCountSchema.parse(oauthStatesPurged),
      };
    } catch {
      throw new Error("maintenance_unavailable");
    }
  }

  async run(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      await this.runOnce();
      if (signal.aborted) return;
      try {
        await this.#wait(this.#intervalMs, signal);
      } catch {
        if (signal.aborted) return;
        throw new Error("maintenance_unavailable");
      }
    }
  }
}

async function defaultMaintenanceWait(milliseconds: number, signal: AbortSignal): Promise<void> {
  await sleep(milliseconds, undefined, { signal });
}
