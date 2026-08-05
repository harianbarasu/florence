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
      googleOAuth: false,
      gmail: false,
      googleCalendar: false,
      openai: false,
      anthropic: false,
      openWeight: false,
    });
    expect(config.FLORENCE_DB_SCHEMA).toBe("florence");
    expect(config.FLORENCE_PROCESS_ROLE).toBe("all");
  });

  it("reports only invalid field names", () => {
    expect(() => loadConfig({ ...validEnvironment(), PORT: "invalid" })).toThrow(
      "Invalid Florence configuration: PORT",
    );
  });

  it("keeps partial Google configuration degraded and enables only the complete connector", () => {
    const tokenOnly = loadConfig({
      ...validEnvironment(),
      GOOGLE_PUBSUB_VERIFICATION_TOKEN: "verification-token",
    });
    expect(availableIntegrations(tokenOnly)).toMatchObject({
      googleOAuth: false,
      gmail: false,
      googleCalendar: false,
    });

    const partial = loadConfig({
      ...validEnvironment(),
      GOOGLE_CLIENT_ID: "client-id",
      GOOGLE_CLIENT_SECRET: "client-secret",
      GOOGLE_OAUTH_STATE_SECRET: "state-secret-that-is-at-least-thirty-two-bytes",
      GOOGLE_REDIRECT_URI: "https://florence.example.com/oauth/google/callback",
    });
    expect(availableIntegrations(partial)).toMatchObject({
      googleOAuth: true,
      gmail: false,
      googleCalendar: true,
    });
    const complete = loadConfig({
      ...validEnvironment(),
      GOOGLE_CLIENT_ID: "client-id",
      GOOGLE_CLIENT_SECRET: "client-secret",
      GOOGLE_OAUTH_STATE_SECRET: "state-secret-that-is-at-least-thirty-two-bytes",
      GOOGLE_REDIRECT_URI: "https://florence.example.com/oauth/google/callback",
      GOOGLE_PUBSUB_VERIFICATION_TOKEN: "verification-token",
      GOOGLE_GMAIL_TOPIC_NAME: "projects/florence/topics/gmail",
      GOOGLE_GMAIL_PUBSUB_SUBSCRIPTION: "projects/florence/subscriptions/gmail",
    });
    expect(availableIntegrations(complete)).toMatchObject({
      googleOAuth: true,
      gmail: true,
      googleCalendar: true,
    });
  });

  it("rejects a malformed Gmail Pub/Sub subscription name", () => {
    expect(() =>
      loadConfig({
        ...validEnvironment(),
        GOOGLE_GMAIL_PUBSUB_SUBSCRIPTION: "gmail-subscription",
      }),
    ).toThrow("Invalid Florence configuration: GOOGLE_GMAIL_PUBSUB_SUBSCRIPTION");
  });

  it("keeps partial Linq configuration degraded and enables only the complete connector", () => {
    const partial = loadConfig({
      ...validEnvironment(),
      LINQ_API_KEY: "linq-key",
      LINQ_FROM_PHONE: "+16465550100",
    });
    expect(availableIntegrations(partial).linq).toBe(false);

    const complete = loadConfig({
      ...validEnvironment(),
      LINQ_API_KEY: "linq-key",
      LINQ_FROM_PHONE: "+16465550100",
      LINQ_WEBHOOK_SECRET: "linq-webhook-secret",
    });
    expect(availableIntegrations(complete).linq).toBe(true);
  });

  it("treats blank optional integration settings from dotenv files as unset", () => {
    const config = loadConfig({
      ...validEnvironment(),
      LINQ_API_KEY: "",
      GOOGLE_REDIRECT_URI: "",
      GOOGLE_GMAIL_TOPIC_NAME: "",
      OPEN_WEIGHT_BASE_URL: "",
    });
    expect(config.LINQ_API_KEY).toBeUndefined();
    expect(config.GOOGLE_REDIRECT_URI).toBeUndefined();
    expect(config.GOOGLE_GMAIL_TOPIC_NAME).toBeUndefined();
    expect(config.OPEN_WEIGHT_BASE_URL).toBeUndefined();
  });

  it.each(["all", "web", "worker"] as const)("accepts the %s process role", (role) => {
    expect(loadConfig({ ...validEnvironment(), FLORENCE_PROCESS_ROLE: role }).FLORENCE_PROCESS_ROLE).toBe(
      role,
    );
  });

  it("rejects unknown process roles", () => {
    expect(() => loadConfig({ ...validEnvironment(), FLORENCE_PROCESS_ROLE: "background" })).toThrow(
      "Invalid Florence configuration: FLORENCE_PROCESS_ROLE",
    );
  });
});
