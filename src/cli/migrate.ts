import { loadConfig } from "../config.js";
import { closeDatabase, createDatabase } from "../db/client.js";
import { migrateDatabase } from "../db/migrate.js";

const config = loadConfig();
const database = createDatabase(config.DATABASE_URL, { max: 1 });

try {
  const applied = await migrateDatabase(database);
  process.stdout.write(
    applied.length > 0 ? `Applied ${applied.length} migration(s).\n` : "Database is current.\n",
  );
} finally {
  await closeDatabase(database);
}
