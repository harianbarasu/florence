import { buildApp, createDefaultDependencies } from "./app.js";

const isProduction = process.env.NODE_ENV === "production";
const host = process.env.API_HOST ?? (isProduction ? "0.0.0.0" : "127.0.0.1");
const port = Number(process.env.PORT ?? process.env.API_PORT ?? 8787);
const app = await buildApp(createDefaultDependencies());

let shuttingDown = false;
async function shutdown(signal: NodeJS.Signals): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`Received ${signal}; closing Florence API`);
  try {
    await app.close();
  } catch (error) {
    console.error("Failed to close Florence API cleanly", error);
    process.exitCode = 1;
  }
}

process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));

try {
  await app.listen({ host, port });
  console.log(`Florence API listening on http://${host}:${port}`);
} catch (error) {
  app.log.error(error);
  process.exitCode = 1;
}
