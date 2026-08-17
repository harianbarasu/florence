import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { fileURLToPath, pathToFileURL } from "node:url";
import postgres from "postgres";

export const baselineFile = fileURLToPath(new URL("../sql/001_florence.sql", import.meta.url));

export async function migrateDatabase(connectionString: string, sqlFile = baselineFile): Promise<void> {
  const client = postgres(connectionString, { max: 1 });
  try {
    const migration = await readFile(sqlFile, "utf8");
    if (sqlFile !== baselineFile) {
      await client.unsafe(migration);
      return;
    }
    const digest = createHash("sha256").update(migration).digest("hex");
    await client.unsafe(`
      create table if not exists florence_schema_migrations (
        name text primary key,
        sha256 text not null check (sha256 ~ '^[0-9a-f]{64}$'),
        applied_at timestamptz not null default now()
      )
    `);
    const existing = await client<{ sha256: string }[]>`
      select sha256 from florence_schema_migrations where name = '001_florence'
    `;
    if (existing[0]) {
      if (existing[0].sha256 !== digest) throw new Error("Applied Florence baseline has changed");
      return;
    }
    await client.begin(async (transaction) => {
      await transaction.unsafe(migration);
      await transaction`
        insert into florence_schema_migrations (name,sha256) values ('001_florence',${digest})
      `;
    });
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
