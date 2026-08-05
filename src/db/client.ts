import postgres, { type Sql } from "postgres";

export type Database = Sql<Record<string, never>>;

const schemaPattern = /^[a-z][a-z0-9_]{0,62}$/u;

export function createDatabase(
  databaseUrl: string,
  options: { max?: number; schema?: string } = {},
): Database {
  const schema = options.schema ?? "florence";
  if (!schemaPattern.test(schema)) {
    throw new Error("Invalid PostgreSQL schema name");
  }
  return postgres(databaseUrl, {
    max: options.max ?? 10,
    idle_timeout: 20,
    connect_timeout: 15,
    max_lifetime: 60 * 30,
    onnotice: () => undefined,
    connection: {
      application_name: "florence",
      search_path: `${schema},public`,
    },
    transform: {
      undefined: null,
    },
  });
}

export function assertDatabaseSchemaName(schema: string): void {
  if (!schemaPattern.test(schema)) {
    throw new Error("Invalid PostgreSQL schema name");
  }
}

export async function checkDatabase(database: Database): Promise<void> {
  await database`select 1 as ok`;
}

export async function closeDatabase(database: Database): Promise<void> {
  await database.end({ timeout: 5 });
}
