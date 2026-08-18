import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { basename } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import postgres from "postgres";

export const baselineFile = fileURLToPath(new URL("../sql/001_florence.sql", import.meta.url));
export const householdActivationFile = fileURLToPath(
  new URL("../sql/002_household_activation.sql", import.meta.url),
);
export const migrationFiles = [baselineFile, householdActivationFile] as const;

export async function migrateDatabase(connectionString: string, sqlFile?: string): Promise<void> {
  const client = postgres(connectionString, { max: 1 });
  try {
    if (sqlFile) {
      const migration = await readFile(sqlFile, "utf8");
      await client.unsafe(migration);
      return;
    }
    await client.unsafe(`
      create table if not exists florence_schema_migrations (
        name text primary key,
        sha256 text not null check (sha256 ~ '^[0-9a-f]{64}$'),
        applied_at timestamptz not null default now()
      )
    `);
    for (const migrationFile of migrationFiles) {
      const migration = await readFile(migrationFile, "utf8");
      const name = basename(migrationFile, ".sql");
      const digest = createHash("sha256").update(migration).digest("hex");
      const existing = await client<{ sha256: string }[]>`
        select sha256 from florence_schema_migrations where name=${name}
      `;
      if (existing[0]) {
        if (existing[0].sha256 !== digest) {
          throw new Error(`Applied Florence migration ${name} has changed`);
        }
        continue;
      }
      await client.begin(async (transaction) => {
        await transaction.unsafe(migration);
        await transaction`
          insert into florence_schema_migrations (name,sha256) values (${name},${digest})
        `;
      });
    }
  } finally {
    await client.end({ timeout: 5 });
  }
}

const entrypoint = process.argv[1];
if (entrypoint && import.meta.url === pathToFileURL(entrypoint).href) {
  const connectionString = process.env.FLORENCE_DATABASE_URL;
  if (!connectionString) {
    throw new Error("FLORENCE_DATABASE_URL is required to run database migrations");
  }
  await migrateDatabase(connectionString);
}
