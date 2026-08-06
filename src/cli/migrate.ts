import { loadDatabaseConfig } from "../config.js";
import { migrate } from "../db/migrate.js";

await migrate(loadDatabaseConfig());
