import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import type { Database } from "./client.js";
import { assertDatabaseSchemaName } from "./client.js";

const migrationsDirectory = join(process.cwd(), "migrations");

export async function migrateDatabase(database: Database, schema = "florence"): Promise<string[]> {
  assertDatabaseSchemaName(schema);
  await database.unsafe(`create schema if not exists "${schema}"`);
  await database.unsafe(`
    create table if not exists schema_migrations (
      version text primary key,
      applied_at timestamptz not null default now()
    )
  `);

  const files = (await readdir(migrationsDirectory)).filter((file) => file.endsWith(".sql")).sort();
  const applied: string[] = [];

  for (const file of files) {
    const existing = await database<{ version: string }[]>`
      select version from schema_migrations where version = ${file}
    `;
    if (existing.length > 0) {
      continue;
    }

    const migration = await readFile(join(migrationsDirectory, file), "utf8");
    await database.begin(async (transaction) => {
      await transaction.unsafe(migration);
      await transaction`insert into schema_migrations (version) values (${file})`;
    });
    applied.push(file);
  }
  return applied;
}
