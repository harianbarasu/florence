import { lstat, realpath } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import path from "node:path";

export const productionResetMaintenanceMode = "production_reset_maintenance";
export const productionResetMaintenanceHeader = "x-florence-runtime-mode";

const maintenanceHealthPayload = Object.freeze({
  status: "maintenance",
  service: "florence-production-reset-maintenance",
  mode: productionResetMaintenanceMode,
});
const railwayIdentityVariables = Object.freeze([
  "RAILWAY_PROJECT_ID",
  "RAILWAY_ENVIRONMENT_ID",
  "RAILWAY_SERVICE_ID",
  "RAILWAY_DEPLOYMENT_ID",
  "RAILWAY_REPLICA_ID",
] as const);
const expectedRailwayIdentityVariables = Object.freeze([
  ["RAILWAY_PROJECT_ID", "FLORENCE_PRODUCTION_RESET_EXPECTED_RAILWAY_PROJECT_ID"],
  ["RAILWAY_ENVIRONMENT_ID", "FLORENCE_PRODUCTION_RESET_EXPECTED_RAILWAY_ENVIRONMENT_ID"],
  ["RAILWAY_SERVICE_ID", "FLORENCE_PRODUCTION_RESET_EXPECTED_RAILWAY_SERVICE_ID"],
] as const);
const opaqueRailwayIdPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;

export type FlorenceRuntimeMode = "api" | typeof productionResetMaintenanceMode;

export class ProductionRuntimeError extends Error {
  readonly failures: readonly string[];

  constructor(
    readonly code: string,
    failures: readonly string[] = [],
  ) {
    super(code);
    this.name = "ProductionRuntimeError";
    this.failures = Object.freeze([...failures]);
  }
}

export function resolveFlorenceRuntimeMode(env: NodeJS.ProcessEnv = process.env): FlorenceRuntimeMode {
  const configured = env.FLORENCE_RUNTIME_MODE?.trim();
  if (!configured || configured === "api") return "api";
  if (configured === productionResetMaintenanceMode) return productionResetMaintenanceMode;
  throw new ProductionRuntimeError("invalid_florence_runtime_mode");
}

export async function startProductionResetMaintenanceServer(
  options: Readonly<{ host?: string; port?: number }> = {},
): Promise<Server> {
  const host = options.host ?? (process.env.API_HOST?.trim() || "0.0.0.0");
  const port = options.port ?? requiredPort(process.env.PORT);
  const server = createServer((request, response) => {
    response.setHeader("Cache-Control", "no-store");
    response.setHeader(productionResetMaintenanceHeader, productionResetMaintenanceMode);
    if (request.method === "GET" && request.url === "/api/health") {
      response.statusCode = 200;
      response.setHeader("Content-Type", "application/json; charset=utf-8");
      response.end(JSON.stringify(maintenanceHealthPayload));
      return;
    }
    response.statusCode = 404;
    response.setHeader("Content-Type", "application/json; charset=utf-8");
    response.end(JSON.stringify({ error: "maintenance_mode" }));
  });
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => {
      server.off("listening", onListening);
      reject(error);
    };
    const onListening = () => {
      server.off("error", onError);
      resolve();
    };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(port, host);
  });
  return server;
}

export async function requireProductionResetMaintenanceRuntime(
  env: NodeJS.ProcessEnv = process.env,
): Promise<void> {
  if (resolveFlorenceRuntimeMode(env) !== productionResetMaintenanceMode) {
    throw new ProductionRuntimeError("production_reset_maintenance_mode_required");
  }
  requireExactRailwayRuntimeIdentity(env);
  const port = requiredPort(env.PORT);
  let response: Response;
  try {
    response = await fetch(`http://127.0.0.1:${port}/api/health`, {
      redirect: "error",
      signal: AbortSignal.timeout(2_000),
    });
  } catch {
    throw new ProductionRuntimeError("production_reset_maintenance_health_unavailable");
  }
  if (
    response.status !== 200 ||
    response.headers.get(productionResetMaintenanceHeader) !== productionResetMaintenanceMode
  ) {
    throw new ProductionRuntimeError("production_reset_maintenance_health_invalid");
  }
  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    throw new ProductionRuntimeError("production_reset_maintenance_health_invalid");
  }
  if (!isMaintenanceHealthPayload(payload)) {
    throw new ProductionRuntimeError("production_reset_maintenance_health_invalid");
  }
}

function requireExactRailwayRuntimeIdentity(env: NodeJS.ProcessEnv): void {
  const failures: string[] = [];
  const runtimeIdentities = new Map<string, string>();
  for (const name of railwayIdentityVariables) {
    const value = env[name]?.trim();
    if (!value || !opaqueRailwayIdPattern.test(value)) {
      failures.push(`${name}:missing_or_invalid`);
      continue;
    }
    runtimeIdentities.set(name, value);
  }
  for (const [runtimeName, expectedName] of expectedRailwayIdentityVariables) {
    const expected = env[expectedName]?.trim();
    if (!expected || !opaqueRailwayIdPattern.test(expected)) {
      failures.push(`${expectedName}:missing_or_invalid`);
      continue;
    }
    const actual = runtimeIdentities.get(runtimeName);
    if (actual && actual !== expected) {
      failures.push(`${runtimeName}:does_not_match:${expectedName}`);
    }
  }
  if (failures.length > 0) {
    throw new ProductionRuntimeError("railway_runtime_identity_check_failed", failures);
  }
}

export async function requireProductionResetVaultDirectory(
  env: NodeJS.ProcessEnv = process.env,
): Promise<string> {
  const configuredRoot = requiredAbsolutePath(
    env.FLORENCE_IMAGE_VAULT_DIRECTORY,
    "image_vault_directory_missing_or_invalid",
  );
  const configuredMount = requiredAbsolutePath(
    env.RAILWAY_VOLUME_MOUNT_PATH,
    "railway_volume_mount_missing_or_invalid",
  );
  if (!env.RAILWAY_VOLUME_NAME?.trim()) {
    throw new ProductionRuntimeError("railway_volume_identity_missing");
  }
  const [rootDetails, mountDetails] = await Promise.all([
    lstatDirectory(configuredRoot, "image_vault_directory_missing_or_invalid"),
    lstatDirectory(configuredMount, "railway_volume_mount_missing_or_invalid"),
  ]);
  if (rootDetails.isSymbolicLink() || mountDetails.isSymbolicLink()) {
    throw new ProductionRuntimeError("image_vault_directory_not_on_railway_volume");
  }
  let rootDirectory: string;
  let mountDirectory: string;
  try {
    [rootDirectory, mountDirectory] = await Promise.all([
      realpath(configuredRoot),
      realpath(configuredMount),
    ]);
  } catch {
    throw new ProductionRuntimeError("image_vault_directory_missing_or_invalid");
  }
  const relativeToMount = path.relative(mountDirectory, rootDirectory);
  if (
    relativeToMount === ".." ||
    relativeToMount.startsWith(`..${path.sep}`) ||
    path.isAbsolute(relativeToMount)
  ) {
    throw new ProductionRuntimeError("image_vault_directory_not_on_railway_volume");
  }
  return rootDirectory;
}

function isMaintenanceHealthPayload(value: unknown): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  return (
    record.status === maintenanceHealthPayload.status &&
    record.service === maintenanceHealthPayload.service &&
    record.mode === maintenanceHealthPayload.mode
  );
}

function requiredPort(raw: string | undefined): number {
  const port = Number(raw);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new ProductionRuntimeError("runtime_port_missing_or_invalid");
  }
  return port;
}

function requiredAbsolutePath(raw: string | undefined, code: string): string {
  const value = raw?.trim();
  if (!value || !path.isAbsolute(value)) throw new ProductionRuntimeError(code);
  return path.resolve(value);
}

async function lstatDirectory(directory: string, code: string) {
  try {
    const details = await lstat(directory);
    if (!details.isDirectory()) throw new ProductionRuntimeError(code);
    return details;
  } catch (error) {
    if (error instanceof ProductionRuntimeError) throw error;
    throw new ProductionRuntimeError(code);
  }
}
