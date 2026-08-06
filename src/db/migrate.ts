import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import postgres from "postgres";
import type { FlorenceDatabaseConfig } from "../config.js";

const MIGRATION_LOCK = 4_607_346_622;

export async function migrate(
  config: FlorenceDatabaseConfig,
  directory = path.resolve("migrations"),
): Promise<void> {
  const admin = postgres(config.database.url, {
    max: 1,
    ssl: config.database.ssl ? "require" : false,
    connection: { application_name: "florence-migrate", TimeZone: "UTC" },
  });

  try {
    await admin.unsafe(`create schema if not exists ${quoteIdentifier(config.database.schema)}`);
  } finally {
    await admin.end();
  }

  const database = postgres(config.database.url, {
    max: 1,
    ssl: config.database.ssl ? "require" : false,
    connection: {
      application_name: "florence-migrate",
      search_path: config.database.schema,
      TimeZone: "UTC",
    },
  });

  try {
    await database`select pg_advisory_lock(${MIGRATION_LOCK})`;
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
      const existing = await database<{ digest: string }[]>`
        select digest from schema_migrations where name = ${name}
      `;
      if (existing[0]) {
        if (existing[0].digest !== digest) throw new Error(`Applied migration ${name} has changed`);
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
      await database`select pg_advisory_unlock(${MIGRATION_LOCK})`;
    } finally {
      await database.end();
    }
  }
}

function quoteIdentifier(identifier: string): string {
  if (!/^[a-z][a-z0-9_]{0,62}$/u.test(identifier)) throw new Error("Invalid Postgres schema name");
  return `"${identifier}"`;
}
