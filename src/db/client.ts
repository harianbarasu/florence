import postgres, { type Sql } from "postgres";
import type { FlorenceConfig } from "../config.js";

export type Database = Sql<Record<string, never>>;

export function createDatabase(config: FlorenceConfig, applicationName: string): Database {
  return postgres(config.database.url, {
    max: applicationName === "florence-web" ? 10 : 5,
    idle_timeout: 20,
    connect_timeout: 15,
    ssl: config.database.ssl ? "require" : false,
    connection: {
      application_name: applicationName,
      search_path: config.database.schema,
      statement_timeout: 30_000,
      lock_timeout: 5_000,
      idle_in_transaction_session_timeout: 30_000,
      TimeZone: "UTC",
    },
    onnotice: () => undefined,
  });
}

export async function verifyDatabase(database: Database): Promise<void> {
  const result = await database<{ ok: number }[]>`select 1 as ok`;
  if (result[0]?.ok !== 1) throw new Error("Database readiness query failed");
}
