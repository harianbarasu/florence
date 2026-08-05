import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";
import { availableIntegrations, loadConfig } from "../src/config.js";

function validEnvironment(): NodeJS.ProcessEnv {
  return {
    FLORENCE_DATABASE_URL: "postgres://postgres:postgres@localhost:5432/florence",
    FLORENCE_WEB_BASE_URL: "https://florence.example.com",
    FLORENCE_TOKEN_ENCRYPTION_KEY: randomBytes(32).toString("base64url"),
    FLORENCE_ADMIN_API_KEY: "a-secure-admin-token-with-length",
  };
}

describe("configuration", () => {
  it("keeps integrations optional and reports availability", () => {
    const config = loadConfig(validEnvironment());
    expect(availableIntegrations(config)).toEqual({
      linq: false,
      google: false,
      openai: false,
      anthropic: false,
      openWeight: false,
    });
    expect(config.FLORENCE_DB_SCHEMA).toBe("florence");
  });

  it("reports only invalid field names", () => {
    expect(() => loadConfig({ ...validEnvironment(), PORT: "invalid" })).toThrow(
      "Invalid Florence configuration: PORT",
    );
  });
});
