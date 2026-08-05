import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    coverage: {
      reporter: ["text", "json-summary"],
    },
    exclude: ["**/node_modules/**", "**/dist/**"],
    testTimeout: 15_000,
  },
});
