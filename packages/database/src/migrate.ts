import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import postgres from "postgres";

const migrationLock = 4_607_346_622;

export type MigrationOptions = {
  connectionString: string;
  schema: string;
  ssl?: boolean;
  directory?: string;
};

export async function migrateDatabase(options: MigrationOptions): Promise<void> {
  const schema = validatedSchema(options.schema);
  const directory = options.directory ?? fileURLToPath(new URL("../../../migrations", import.meta.url));
  const shared = {
    max: 1,
    ssl: options.ssl ? ("require" as const) : false,
    connection: { application_name: "florence-migrate", TimeZone: "UTC" },
  };
  const admin = postgres(options.connectionString, shared);
  try {
    await admin.unsafe(`create schema if not exists "${schema}"`);
  } finally {
    await admin.end({ timeout: 5 });
  }

  const database = postgres(options.connectionString, {
    ...shared,
    connection: { ...shared.connection, search_path: schema },
  });
  try {
    await database`select pg_advisory_lock(${migrationLock})`;
    await database`
      create table if not exists schema_migrations (
        name text primary key,
        digest text not null,
        applied_at timestamptz not null default now()
      )
    `;
    const names = (await readdir(directory)).filter((name) => /^\d+_[a-z0-9_]+\.sql$/u.test(name)).sort();
    for (const name of names) {
      const sqlText = await readFile(path.join(directory, name), "utf8");
      const digest = createHash("sha256").update(sqlText).digest("hex");
      const [existing] = await database<{ digest: string }[]>`
        select digest from schema_migrations where name = ${name}
      `;
      if (existing) {
        if (existing.digest !== digest) throw new Error(`Applied migration ${name} has changed`);
        continue;
      }
      await database.begin(async (transaction) => {
        await transaction.unsafe(sqlText);
        await transaction`
          insert into schema_migrations (name, digest) values (${name}, ${digest})
        `;
      });
    }
  } finally {
    try {
      await database`select pg_advisory_unlock(${migrationLock})`;
    } finally {
      await database.end({ timeout: 5 });
    }
  }
}

function validatedSchema(value: string): string {
  if (!/^[a-z][a-z0-9_]{0,62}$/u.test(value)) throw new Error("Invalid Postgres schema name");
  return value;
}

function required(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required to run Florence migrations`);
  return value;
}

const entrypoint = process.argv[1];
if (entrypoint && import.meta.url === pathToFileURL(entrypoint).href) {
  await migrateDatabase({
    connectionString: required("FLORENCE_DATABASE_URL"),
    schema: required("FLORENCE_POSTGRES_SCHEMA"),
    ssl: process.env.NODE_ENV === "production",
  });
}
