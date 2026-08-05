import { loadConfig } from "../config.js";
import { closeDatabase, createDatabase } from "../db/client.js";
import { migrateDatabase } from "../db/migrate.js";

const config = loadConfig();
const database = createDatabase(config.FLORENCE_DATABASE_URL, {
  max: 1,
  schema: config.FLORENCE_DB_SCHEMA,
});

try {
  const applied = await migrateDatabase(database, config.FLORENCE_DB_SCHEMA);
  process.stdout.write(
    applied.length > 0 ? `Applied ${applied.length} migration(s).\n` : "Database is current.\n",
  );
} finally {
  await closeDatabase(database);
}
