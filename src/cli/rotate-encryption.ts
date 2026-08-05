import { loadConfig } from "../config.js";
import { closeDatabase, createDatabase } from "../db/client.js";
import {
  assertEncryptionKeyringReady,
  EncryptionRotationError,
  PostgresEncryptionRotation,
} from "../infrastructure/encryption-rotation.js";
import { TenantJsonCipher } from "../security/tenant-json-cipher.js";

const config = loadConfig();
const database = createDatabase(config.FLORENCE_DATABASE_URL, {
  max: 1,
  schema: config.FLORENCE_POSTGRES_SCHEMA,
});

try {
  const cipher = new TenantJsonCipher({
    activeKeyId: config.FLORENCE_DATA_ACTIVE_KEY_ID,
    keys: config.FLORENCE_DATA_KEYRING_JSON,
  });
  await assertEncryptionKeyringReady(database, cipher);
  const rotation = new PostgresEncryptionRotation(database, cipher, config.FLORENCE_DATA_ACTIVE_KEY_ID);
  const batchSize = parseBatchSize(process.argv.slice(2));

  while (true) {
    const result = await rotation.resumeBatch(batchSize);
    if (result.status === "completed") {
      process.stdout.write(
        `Encryption rotation ${result.runId} completed after ${result.totalRowsRewrapped} row(s).\n`,
      );
      break;
    }
    if (result.rowsRewrapped === 0) {
      await new Promise<void>((resolve) => setTimeout(resolve, 100));
    }
  }
} catch (error) {
  process.exitCode = 1;
  process.stderr.write(
    `${error instanceof EncryptionRotationError ? error.message : "Encryption rotation failed."}\n`,
  );
} finally {
  await closeDatabase(database);
}

function parseBatchSize(args: readonly string[]): number {
  const normalized = args[0] === "--" ? args.slice(1) : args;
  if (normalized.length === 0) return 100;
  if (normalized.length !== 2 || normalized[0] !== "--batch-size") {
    throw new Error("Usage: pnpm data:rotate-key [--batch-size 1..1000]");
  }
  const batchSize = Number(normalized[1]);
  if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > 1_000) {
    throw new Error("Usage: pnpm data:rotate-key [--batch-size 1..1000]");
  }
  return batchSize;
}
