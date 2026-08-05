import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { closeDatabase, createDatabase, type Database } from "../../src/db/client.js";
import { migrateDatabase } from "../../src/db/migrate.js";

const databaseUrl = process.env.TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)("database migration coordination", () => {
  const schema = `migration_${randomUUID().replaceAll("-", "").slice(0, 24)}`;
  let first: Database;
  let second: Database;

  beforeAll(() => {
    first = createDatabase(databaseUrl as string, { max: 1, schema });
    second = createDatabase(databaseUrl as string, { max: 1, schema });
  });

  afterAll(async () => {
    if (!first || !second) return;
    await first.unsafe(`drop schema if exists "${schema}" cascade`);
    await Promise.all([closeDatabase(first), closeDatabase(second)]);
  });

  it("serializes concurrent deploy migrations with one advisory lock", async () => {
    const [left, right] = await Promise.all([
      migrateDatabase(first, schema),
      migrateDatabase(second, schema),
    ]);

    expect([left.length, right.length].sort((a, b) => a - b)).toEqual([0, 3]);
    const rows = await first<{ version: string }[]>`
      select version from schema_migrations order by version
    `;
    expect(rows.map((row) => row.version)).toEqual([
      "001_initial.sql",
      "002_application_runtime.sql",
      "003_runtime_operations.sql",
    ]);
  });
});
