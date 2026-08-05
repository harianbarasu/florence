import type { FastifyInstance } from "fastify";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createFlorenceHttpServer, productionHttpLoggerOptions } from "../../src/http/index.js";
import { fakeHttpServices, HTTP_CONFIG, OPERATOR_TOKEN, PUBSUB_TOKEN } from "./helpers.js";

describe("health and secure browser HTTP interface", () => {
  let server: FastifyInstance;
  let services: ReturnType<typeof fakeHttpServices>;

  beforeEach(async () => {
    services = fakeHttpServices();
    server = await createFlorenceHttpServer({ config: HTTP_CONFIG, services });
  });

  afterEach(async () => {
    await server.close();
  });

  it("serves liveness, readiness, metrics, and strict security headers", async () => {
    services.readiness.isReady = vi.fn(async () => false);

    const health = await server.inject({ method: "GET", url: "/healthz" });
    expect(health.statusCode).toBe(200);
    expect(health.json()).toEqual({ status: "ok" });
    expect(health.headers["x-content-type-options"]).toBe("nosniff");
    expect(health.headers["content-security-policy"]).toContain("default-src 'none'");
    expect(health.headers["referrer-policy"]).toBe("no-referrer");
    expect(health.headers["cache-control"]).toBe("no-store");

    const unready = await server.inject({ method: "GET", url: "/readyz" });
    expect(unready.statusCode).toBe(503);
    expect(unready.json()).toEqual({ status: "not_ready" });

    services.readiness.isReady = vi.fn(async () => true);
    const ready = await server.inject({ method: "GET", url: "/readyz" });
    expect(ready.statusCode).toBe(200);
    expect(ready.json()).toEqual({ status: "ready" });

    const metrics = await server.inject({ method: "GET", url: "/metrics" });
    expect(metrics.statusCode).toBe(200);
    expect(metrics.headers["content-type"]).toContain("text/plain");
    expect(metrics.body).toContain("florence_http_requests_total");
    expect(metrics.body).not.toContain(OPERATOR_TOKEN);
    expect(metrics.body).not.toContain(PUBSUB_TOKEN);
  });

  it("redirects valid Google OAuth starts and rejects open redirects", async () => {
    services.googleOAuth.start = vi.fn(async () => ({
      kind: "redirect" as const,
      authorizationUrl: "https://accounts.google.com/o/oauth2/v2/auth?client_id=synthetic&state=opaque-state",
    }));
    const handoff = "opaque-private-handoff-000001";

    const response = await server.inject({
      method: "GET",
      url: `/oauth/google/start?handoff=${handoff}`,
    });

    expect(response.statusCode).toBe(303);
    expect(response.headers.location).toBe(
      "https://accounts.google.com/o/oauth2/v2/auth?client_id=synthetic&state=opaque-state",
    );
    expect(services.googleOAuth.start).toHaveBeenCalledWith({ handoffToken: handoff });

    services.googleOAuth.start = vi.fn(async () => ({
      kind: "redirect" as const,
      authorizationUrl: "https://attacker.example.test/steal",
    }));
    const rejected = await server.inject({
      method: "GET",
      url: `/oauth/google/start?handoff=${handoff}`,
    });
    expect(rejected.statusCode).toBe(503);
    expect(rejected.headers.location).toBeUndefined();
    expect(rejected.body).not.toContain("attacker.example.test");
  });

  it("renders minimal OAuth completion and denial pages without identity details", async () => {
    services.googleOAuth.complete = vi.fn(async () => ({ kind: "connected" as const }));

    const connected = await server.inject({
      method: "GET",
      url: "/oauth/google/callback?state=opaque-state&code=private-code&scope=openid&authuser=0",
    });
    expect(connected.statusCode).toBe(200);
    expect(connected.body).toContain("Connection complete");
    expect(connected.body).not.toContain("private-code");
    expect(services.googleOAuth.complete).toHaveBeenCalledWith({
      state: "opaque-state",
      code: "private-code",
      providerError: null,
    });

    services.googleOAuth.complete = vi.fn(async () => ({ kind: "declined" as const }));
    const declined = await server.inject({
      method: "GET",
      url: "/oauth/google/callback?state=opaque-state&error=access_denied",
    });
    expect(declined.statusCode).toBe(400);
    expect(declined.body).toContain("Nothing was connected");
    expect(declined.body).not.toContain("access_denied");
  });

  it("fails OAuth closed with generic HTML", async () => {
    services.googleOAuth.complete = vi.fn(async () => {
      throw new Error("synthetic secret token and private email");
    });

    const response = await server.inject({
      method: "GET",
      url: "/oauth/google/callback?state=opaque-state&code=private-code",
    });
    expect(response.statusCode).toBe(503);
    expect(response.body).toContain("Please try again later");
    expect(response.body).not.toContain("secret token");
    expect(response.body).not.toContain("private email");
  });

  it("rate limits repeated OAuth handoff attempts", async () => {
    services.googleOAuth.start = vi.fn(async () => ({
      kind: "redirect" as const,
      authorizationUrl: "https://accounts.google.com/o/oauth2/v2/auth?state=synthetic",
    }));

    for (let index = 0; index < 30; index += 1) {
      const response = await server.inject({
        method: "GET",
        url: `/oauth/google/start?handoff=opaque-private-handoff-${String(index).padStart(6, "0")}`,
      });
      expect(response.statusCode).toBe(303);
    }
    const limited = await server.inject({
      method: "GET",
      url: "/oauth/google/start?handoff=opaque-private-handoff-limited",
    });
    expect(limited.statusCode).toBe(429);
    expect(limited.json()).toEqual({ error: "rate_limited" });
  });
});

describe("operator HTTP interface", () => {
  let server: FastifyInstance;
  let services: ReturnType<typeof fakeHttpServices>;

  beforeEach(async () => {
    services = fakeHttpServices();
    server = await createFlorenceHttpServer({ config: HTTP_CONFIG, services });
  });

  afterEach(async () => {
    await server.close();
  });

  it("requires an exact bearer token before the operator status handler", async () => {
    services.operations.status = vi.fn(async () => ({ status: "ok" as const, checks: {} }));
    const response = await server.inject({ method: "GET", url: "/operator/status" });

    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({ error: "unauthorized" });
    expect(response.headers["www-authenticate"]).toContain("Bearer");
    expect(services.operations.status).not.toHaveBeenCalled();
  });

  it("logs only fixed metadata, never query secrets or implementation errors", async () => {
    await server.close();
    let output = "";
    services.googleOAuth.complete = vi.fn(async () => {
      throw new Error("private-parent@example.test synthetic-access-token");
    });
    server = await createFlorenceHttpServer({
      config: HTTP_CONFIG,
      services,
      logger: {
        ...productionHttpLoggerOptions("info"),
        stream: {
          write(chunk: string) {
            output += chunk;
          },
        },
      },
    });

    await server.inject({
      method: "GET",
      url: "/oauth/google/callback?state=private-state&code=synthetic-access-token",
    });

    expect(output).toContain('"route":"/oauth/google/callback"');
    expect(output).not.toContain("private-state");
    expect(output).not.toContain("synthetic-access-token");
    expect(output).not.toContain("private-parent@example.test");
  });
});
