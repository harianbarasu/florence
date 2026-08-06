import { z } from "zod";
import { loadConfig } from "../config.js";
import { migrate } from "../db/migrate.js";

class ProductionPreflightError extends Error {
  public constructor(public readonly code: string) {
    super(code);
    this.name = "ProductionPreflightError";
  }
}

let phase: "configuration" | "migration" = "configuration";

try {
  const config = loadConfig();
  if (config.environment !== "production") {
    throw new ProductionPreflightError("node_environment_not_production");
  }
  if (config.database.schema !== "florence_v4") {
    throw new ProductionPreflightError("database_schema_not_florence_v4");
  }
  if (!config.database.ssl) {
    throw new ProductionPreflightError("database_tls_not_required");
  }

  phase = "migration";
  await migrate(config);
  process.stdout.write(
    `${JSON.stringify({
      ok: true,
      event: "production_predeploy_complete",
      service: safeServiceName(process.env.RAILWAY_SERVICE_NAME),
      schema: config.database.schema,
      modelProvider: config.model.provider,
    })}\n`,
  );
} catch (error) {
  const issues =
    error instanceof z.ZodError
      ? [...new Set(error.issues.map((issue) => issue.path.join(".") || "environment"))].sort()
      : undefined;
  process.stderr.write(
    `${JSON.stringify({
      ok: false,
      event: "production_predeploy_failed",
      phase,
      errorCode:
        error instanceof ProductionPreflightError
          ? error.code
          : phase === "configuration"
            ? "runtime_configuration_invalid"
            : "database_migration_failed",
      ...(issues ? { invalidVariables: issues } : {}),
    })}\n`,
  );
  process.exitCode = 1;
}

function safeServiceName(value: string | undefined): string {
  const trimmed = value?.trim();
  return trimmed && /^[A-Za-z0-9._-]{1,64}$/u.test(trimmed) ? trimmed : "local";
}
