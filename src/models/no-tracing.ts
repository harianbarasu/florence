import { traceable } from "langsmith/traceable";

/**
 * Establishes an explicit tracing-disabled context. Empty callback arrays do
 * not override a process-wide LangSmith environment setting on their own.
 */
export async function runWithoutModelTracing<T>(operation: () => Promise<T>): Promise<T> {
  const guarded = traceable(operation, {
    name: "florence-redacted-model-operation",
    tracingEnabled: false,
    processInputs: () => ({}),
    processOutputs: () => ({}),
  });
  return guarded();
}
