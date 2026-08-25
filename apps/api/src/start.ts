import {
  ProductionRuntimeError,
  productionResetMaintenanceMode,
  resolveFlorenceRuntimeMode,
  startProductionResetMaintenanceServer,
} from "./production-runtime.js";

try {
  const mode = resolveFlorenceRuntimeMode();
  if (mode === "api") {
    await import("./server.js");
  } else if (mode === productionResetMaintenanceMode) {
    const server = await startProductionResetMaintenanceServer();
    console.log(
      JSON.stringify({
        event: "florence_runtime_started",
        mode: productionResetMaintenanceMode,
        apiRoutesAvailable: false,
      }),
    );
    let shuttingDown = false;
    const shutdown = () => {
      if (shuttingDown) return;
      shuttingDown = true;
      server.close((error) => {
        if (error) process.exitCode = 1;
      });
    };
    process.once("SIGTERM", shutdown);
    process.once("SIGINT", shutdown);
  }
} catch (error) {
  const errorCode = error instanceof ProductionRuntimeError ? error.code : "florence_runtime_start_failed";
  process.stderr.write(`${JSON.stringify({ event: "florence_runtime_start_failed", errorCode })}\n`);
  process.exitCode = 1;
}
