import { buildApp, createDefaultDependencies } from "./app.js";

const host = "0.0.0.0";
const port = Number(process.env.PORT ?? 8787);
if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) {
  throw new Error("PORT must be an integer from 1 through 65535");
}
const app = await buildApp(createDefaultDependencies());

let shuttingDown = false;
async function shutdown(signal: NodeJS.Signals): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`Received ${signal}; closing Florence API`);
  try {
    await app.close();
  } catch {
    console.error("[shutdown_failed] Florence API could not close cleanly");
    process.exitCode = 1;
  }
}

process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));

try {
  await app.listen({ host, port });
  console.log(`Florence API listening on http://${host}:${port}`);
} catch {
  app.log.error({ code: "startup_failed" }, "Florence API failed to start");
  try {
    await app.close();
  } catch {
    app.log.error({ code: "startup_cleanup_failed" }, "Florence API startup cleanup failed");
  }
  process.exitCode = 1;
}
