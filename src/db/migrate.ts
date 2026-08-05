import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import type { Database } from "./client.js";
import { assertDatabaseSchemaName } from "./client.js";

const migrationsDirectory = join(process.cwd(), "migrations");

export async function migrateDatabase(database: Database, schema = "florence"): Promise<string[]> {
  assertDatabaseSchemaName(schema);
  const files = (await readdir(migrationsDirectory)).filter((file) => file.endsWith(".sql")).sort();
  const migrations = await Promise.all(
    files.map(async (file) => ({ file, sql: await readFile(join(migrationsDirectory, file), "utf8") })),
  );

  return database.begin(async (transaction) => {
    await transaction`select pg_advisory_xact_lock(hashtextextended(${`florence:migrations:${schema}`}, 0))`;
    await transaction.unsafe(`create schema if not exists "${schema}"`);
    await transaction.unsafe(`set local search_path to "${schema}", public`);
    await transaction.unsafe(`
      create table if not exists schema_migrations (
        version text primary key,
        applied_at timestamptz not null default now()
      )
    `);

    const applied: string[] = [];
    for (const migration of migrations) {
      const existing = await transaction<{ version: string }[]>`
        select version from schema_migrations where version = ${migration.file}
      `;
      if (existing.length > 0) continue;

      await transaction.unsafe(migration.sql);
      await transaction`insert into schema_migrations (version) values (${migration.file})`;
      applied.push(migration.file);
    }
    return applied;
  });
}
