import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";
import { loadConfig } from "../../src/config.js";
import { modelGatewayConfigFromFlorenceConfig } from "../../src/infrastructure/index.js";

function baseEnvironment(): NodeJS.ProcessEnv {
  return {
    FLORENCE_DATABASE_URL: "postgres://postgres:postgres@localhost:5432/florence",
    FLORENCE_WEB_BASE_URL: "https://florence.example.com",
    FLORENCE_TOKEN_ENCRYPTION_KEY: randomBytes(32).toString("base64url"),
    FLORENCE_ADMIN_API_KEY: "an-admin-token-long-enough-for-tests",
  };
}

describe("modelGatewayConfigFromFlorenceConfig", () => {
  it("creates all app capability routes with stable worker route identities", () => {
    const config = loadConfig({
      ...baseEnvironment(),
      MODEL_PROVIDER: "openai",
      OPENAI_API_KEY: "test-key",
    });
    const gateway = modelGatewayConfigFromFlorenceConfig(config);
    expect(gateway.routes).toHaveLength(5);
    expect(gateway.routes.map((route) => route.routeId)).toContain("route.family_research.v1");
    expect(gateway.routes.map((route) => route.routeId)).toContain("route.meal_plan.v1");
    expect(new Set(gateway.routes.map((route) => route.profile)).size).toBe(5);
  });

  it("selects an allowlisted OpenAI-compatible endpoint without provider types escaping", () => {
    const config = loadConfig({
      ...baseEnvironment(),
      MODEL_PROVIDER: "open-weight",
      OPEN_WEIGHT_BASE_URL: "https://models.example.com/v1",
      OPEN_WEIGHT_MODEL: "household-model",
    });
    const gateway = modelGatewayConfigFromFlorenceConfig(config);
    expect(gateway.openAICompatibleBaseUrlAllowlist).toEqual(["https://models.example.com/v1"]);
    expect(gateway.routes.every((route) => route.provider === "openai-compatible")).toBe(true);
  });
});
