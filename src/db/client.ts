import postgres, { type Sql } from "postgres";

export type Database = Sql<Record<string, never>>;

export function createDatabase(databaseUrl: string, options: { max?: number } = {}): Database {
  return postgres(databaseUrl, {
    max: options.max ?? 10,
    idle_timeout: 20,
    connect_timeout: 15,
    max_lifetime: 60 * 30,
    onnotice: () => undefined,
    transform: {
      undefined: null,
    },
  });
}

export async function checkDatabase(database: Database): Promise<void> {
  await database`select 1 as ok`;
}

export async function closeDatabase(database: Database): Promise<void> {
  await database.end({ timeout: 5 });
}
