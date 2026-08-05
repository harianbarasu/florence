import { randomBytes, randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createProductionComposition } from "../src/composition.js";
import { loadConfig } from "../src/config.js";
import { closeDatabase, createDatabase, type Database } from "../src/db/client.js";
import { migrateDatabase } from "../src/db/migrate.js";

const databaseUrl = process.env.TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)("production composition PostgreSQL integration", () => {
  const schema = `composition_${randomUUID().replaceAll("-", "").slice(0, 24)}`;
  let database: Database;

  beforeAll(async () => {
    database = createDatabase(databaseUrl as string, { max: 4, schema });
    await migrateDatabase(database, schema);
  });

  afterAll(async () => {
    if (!database) return;
    await database.unsafe(`drop schema if exists "${schema}" cascade`);
    await closeDatabase(database);
  });

  it("keeps an idle background runtime alive until shutdown", async () => {
    const composition = await createProductionComposition({
      config: loadConfig({
        NODE_ENV: "test",
        FLORENCE_PROCESS_ROLE: "worker",
        FLORENCE_DATABASE_URL: databaseUrl as string,
        FLORENCE_DB_SCHEMA: schema,
        FLORENCE_WEB_BASE_URL: "https://florence.example.test",
        FLORENCE_TOKEN_ENCRYPTION_KEY: randomBytes(32).toString("base64url"),
        FLORENCE_ADMIN_API_KEY: "operator-test-key-with-enough-bytes",
        GOOGLE_CLIENT_ID: "calendar-test-client-id",
        GOOGLE_CLIENT_SECRET: "calendar-test-client-secret",
        GOOGLE_OAUTH_STATE_SECRET: "calendar-test-state-secret-with-at-least-32-bytes",
        GOOGLE_REDIRECT_URI: "https://florence.example.test/oauth/google/callback",
        OPENAI_API_KEY: "test-model-key",
        WORKER_POLL_INTERVAL_MS: "100",
      }),
      migrate: false,
    });
    const controller = new AbortController();
    const running = composition.background.run(controller.signal);
    try {
      expect(composition.http.config.googleCalendarPushEnabled).toBe(true);
      expect(composition.http.config.gmailPubSubVerificationToken).toBeNull();
      const firstOutcome = await Promise.race([
        running.then(
          () => "stopped" as const,
          () => "failed" as const,
        ),
        new Promise<"alive">((resolve) => setTimeout(() => resolve("alive"), 500)),
      ]);
      expect(firstOutcome).toBe("alive");
      expect(composition.background.isHealthy()).toBe(true);
    } finally {
      controller.abort();
      await running;
      await composition.close();
    }
  });
});
