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

    expect([left.length, right.length].sort((a, b) => a - b)).toEqual([0, 6]);
    const rows = await first<{ version: string }[]>`
      select version from schema_migrations order by version
    `;
    expect(rows.map((row) => row.version)).toEqual([
      "001_initial.sql",
      "002_application_runtime.sql",
      "003_runtime_operations.sql",
      "004_daily_brief_runs.sql",
      "005_google_calendar_sync.sql",
      "006_gmail_discovery_guard.sql",
    ]);

    const householdId = randomUUID();
    const adultId = randomUUID();
    const connectionId = randomUUID();
    await first`
      insert into households (id, name, timezone, status)
      values (${householdId}, 'Migration test', 'America/Los_Angeles', 'active')
    `;
    await first`
      insert into adults (id, display_name, timezone)
      values (${adultId}, 'Migration adult', 'America/Los_Angeles')
    `;
    await first`
      insert into external_connections (
        id, household_id, adult_id, provider, label, external_account_id,
        encrypted_credentials, granted_scopes, status, cursor, metadata
      ) values (
        ${connectionId}, ${householdId}, ${adultId}, 'google', 'Migrated Gmail',
        'migration-google-subject', 'encrypted', '{}', 'active',
        ${first.json({
          gmail: {
            schemaVersion: 1,
            revision: 4,
            phase: "one_year_backfill",
            requestedDepth: "full_history",
            boundaryAt: "2027-01-01T08:00:00.000Z",
            scanPageToken: "legacy-page",
            history: { cursorId: "100", startId: null, pageToken: null, targetId: "120" },
            watch: null,
            lastSuccessfulSyncAt: null,
            cancellation: null,
          },
        })},
        ${first.json({ credentialAadVersion: 1 })}
      )
    `;
    await first`delete from schema_migrations where version = '006_gmail_discovery_guard.sql'`;
    await expect(migrateDatabase(first, schema)).resolves.toEqual(["006_gmail_discovery_guard.sql"]);
    const migrated = await first<{ cursor: Record<string, unknown> }[]>`
      select cursor from external_connections where id = ${connectionId}
    `;
    expect(migrated[0]?.cursor.gmail).toMatchObject({
      schemaVersion: 2,
      revision: 4,
      phase: "one_year_backfill",
      scanPageToken: "legacy-page",
      scanProcessedMessageIds: [],
      history: { cursorId: "100", targetId: "120" },
      discovery: null,
    });
  });
});
