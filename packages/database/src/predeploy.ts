import { migrateDatabase } from "./migrate.js";

let phase: "configuration" | "migration" = "configuration";

try {
  if (process.env.NODE_ENV !== "production") throw new Error("node_environment_not_production");
  const connectionString = required("FLORENCE_DATABASE_URL");
  phase = "migration";
  await migrateDatabase(connectionString);
  process.stdout.write(
    `${JSON.stringify({
      ok: true,
      event: "production_predeploy_complete",
      service: safeServiceName(process.env.RAILWAY_SERVICE_NAME),
    })}\n`,
  );
} catch (error) {
  process.stderr.write(
    `${JSON.stringify({
      ok: false,
      event: "production_predeploy_failed",
      phase,
      errorCode: error instanceof Error ? error.message : "database_migration_failed",
    })}\n`,
  );
  process.exitCode = 1;
}

function required(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name.toLowerCase()}_missing`);
  return value;
}

function safeServiceName(value: string | undefined): string {
  const trimmed = value?.trim();
  return trimmed && /^[A-Za-z0-9._-]{1,64}$/u.test(trimmed) ? trimmed : "local";
}
