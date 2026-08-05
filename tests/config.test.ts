import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";
import { availableIntegrations, loadConfig } from "../src/config.js";

function validEnvironment(): NodeJS.ProcessEnv {
  return {
    DATABASE_URL: "postgres://postgres:postgres@localhost:5432/florence",
    FLORENCE_PUBLIC_URL: "https://florence.example.com",
    FLORENCE_ENCRYPTION_KEY: randomBytes(32).toString("base64url"),
    FLORENCE_ADMIN_TOKEN: "a-secure-admin-token-with-length",
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
  });

  it("reports only invalid field names", () => {
    expect(() => loadConfig({ ...validEnvironment(), PORT: "invalid" })).toThrow(
      "Invalid Florence configuration: PORT",
    );
  });
});
