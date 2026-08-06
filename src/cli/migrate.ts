import { loadConfig } from "../config.js";
import { migrate } from "../db/migrate.js";

await migrate(loadConfig());
