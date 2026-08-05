import { createHmac } from "node:crypto";
import type { FastifyInstance } from "fastify";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createFlorenceHttpServer } from "../../src/http/index.js";
import {
  fakeHttpServices,
  gmailPushPayload,
  HTTP_CONFIG,
  LINQ_SIGNING_KEY,
  PUBSUB_TOKEN,
  readHttpFixture,
  signedLinqHeaders,
} from "./helpers.js";

describe("provider ingress HTTP interface", () => {
  let server: FastifyInstance;
  let services: ReturnType<typeof fakeHttpServices>;

  beforeEach(async () => {
    services = fakeHttpServices();
    server = await createFlorenceHttpServer({ config: HTTP_CONFIG, services });
  });

  afterEach(async () => {
    await server.close();
  });

  it("verifies the exact raw Linq body before durable acceptance", async () => {
    const rawBody = readHttpFixture("linq-direct-message.json");
    services.ingress.acceptLinq = vi.fn(async () => undefined);

    const response = await server.inject({
      method: "POST",
      url: "/webhooks/linq",
      headers: signedLinqHeaders(rawBody),
      payload: rawBody,
    });

    expect(response.statusCode).toBe(202);
    expect(response.body).toBe("");
    expect(services.ingress.acceptLinq).toHaveBeenCalledWith(
      expect.objectContaining({
        providerEventId: "evt-http-direct-001",
        dedupeKey: "linq:partner-http-fixture:evt-http-direct-001",
        eventType: "message.received",
      }),
    );
  });

  it("rejects invalid Linq authentication without parsing or exposing content", async () => {
    const rawBody = readHttpFixture("linq-direct-message.json");
    const headers = signedLinqHeaders(rawBody);
    headers["webhook-signature"] = "v1,invalid";
    services.ingress.acceptLinq = vi.fn(async () => undefined);

    const response = await server.inject({
      method: "POST",
      url: "/webhooks/linq",
      headers,
      payload: rawBody,
    });

    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({ error: "unauthorized" });
    expect(response.body).not.toContain("field trip");
    expect(services.ingress.acceptLinq).not.toHaveBeenCalled();
  });

  it("does not acknowledge Linq until durable ingress resolves", async () => {
    const rawBody = readHttpFixture("linq-direct-message.json");
    services.ingress.acceptLinq = vi.fn(async () => {
      throw new Error("synthetic private failure detail");
    });

    const response = await server.inject({
      method: "POST",
      url: "/webhooks/linq",
      headers: signedLinqHeaders(rawBody),
      payload: rawBody,
    });

    expect(response.statusCode).toBe(503);
    expect(response.json()).toEqual({ error: "unavailable" });
    expect(response.body).not.toContain("private failure");
  });

  it("acknowledges authenticated unsupported Linq event types without journaling", async () => {
    const payload = JSON.parse(readHttpFixture("linq-direct-message.json")) as Record<string, unknown>;
    payload.event_type = "message.delivered";
    const rawBody = JSON.stringify(payload);
    services.ingress.acceptLinq = vi.fn(async () => undefined);

    const response = await server.inject({
      method: "POST",
      url: "/webhooks/linq",
      headers: signedLinqHeaders(rawBody),
      payload: rawBody,
    });

    expect(response.statusCode).toBe(204);
    expect(services.ingress.acceptLinq).not.toHaveBeenCalled();
  });

  it("rejects malformed and over-limit Linq bodies", async () => {
    const malformed = "{not-json";
    const timestamp = String(Math.floor(Date.now() / 1_000));
    const signature = createHmac("sha256", LINQ_SIGNING_KEY)
      .update(Buffer.from(`evt-malformed.${timestamp}.${malformed}`))
      .digest("base64");

    const malformedResponse = await server.inject({
      method: "POST",
      url: "/webhooks/linq",
      headers: {
        "content-type": "application/json",
        "webhook-id": "evt-malformed",
        "webhook-timestamp": timestamp,
        "webhook-signature": `v1,${signature}`,
      },
      payload: malformed,
    });
    expect(malformedResponse.statusCode).toBe(400);
    expect(malformedResponse.json()).toEqual({ error: "invalid_request" });

    await server.close();
    server = await createFlorenceHttpServer({
      config: { ...HTTP_CONFIG, bodyLimitBytes: 1_024 },
      services,
    });
    const oversized = JSON.stringify({ content: "x".repeat(2_000) });
    const oversizedResponse = await server.inject({
      method: "POST",
      url: "/webhooks/linq",
      headers: { "content-type": "application/json" },
      payload: oversized,
    });
    expect(oversizedResponse.statusCode).toBe(413);
    expect(oversizedResponse.json()).toEqual({ error: "payload_too_large" });
  });

  it("authenticates and normalizes Gmail Pub/Sub invalidation hints", async () => {
    services.ingress.acceptGmailPush = vi.fn(async () => undefined);

    const response = await server.inject({
      method: "POST",
      url: `/webhooks/google/gmail?token=${encodeURIComponent(PUBSUB_TOKEN)}`,
      headers: { "content-type": "application/json" },
      payload: gmailPushPayload(),
    });

    expect(response.statusCode).toBe(202);
    expect(response.body).toBe("");
    expect(services.ingress.acceptGmailPush).toHaveBeenCalledWith({
      schemaVersion: 1,
      source: "gmail",
      sourceScope: "personal",
      providerEventId: "gmail-pubsub:projects/florence-test/subscriptions/gmail-http:pubsub-http-001",
      subscription: "projects/florence-test/subscriptions/gmail-http",
      mailboxEmail: "parent@example.test",
      historyId: "7654321",
      publishedAt: "2026-08-05T15:30:00.000Z",
      deliveryAttempt: null,
    });
  });

  it("rejects unauthenticated or malformed Gmail pushes without leaking mailbox data", async () => {
    services.ingress.acceptGmailPush = vi.fn(async () => undefined);
    const privatePayload = gmailPushPayload({ email: "private-parent@example.test" });

    const unauthenticated = await server.inject({
      method: "POST",
      url: "/webhooks/google/gmail?token=wrong",
      headers: { "content-type": "application/json" },
      payload: privatePayload,
    });
    expect(unauthenticated.statusCode).toBe(401);
    expect(unauthenticated.json()).toEqual({ error: "unauthorized" });
    expect(unauthenticated.body).not.toContain("private-parent");
    expect(services.ingress.acceptGmailPush).not.toHaveBeenCalled();

    const malformed = await server.inject({
      method: "POST",
      url: `/webhooks/google/gmail?token=${encodeURIComponent(PUBSUB_TOKEN)}`,
      headers: { "content-type": "application/json" },
      payload: { message: { data: "not-json" }, subscription: "synthetic" },
    });
    expect(malformed.statusCode).toBe(400);
    expect(malformed.json()).toEqual({ error: "invalid_request" });
  });

  it("returns a retryable failure when Gmail durable ingress fails", async () => {
    services.ingress.acceptGmailPush = vi.fn(async () => {
      throw new Error("synthetic private mailbox failure");
    });

    const response = await server.inject({
      method: "POST",
      url: `/webhooks/google/gmail?token=${encodeURIComponent(PUBSUB_TOKEN)}`,
      headers: { "content-type": "application/json" },
      payload: gmailPushPayload(),
    });

    expect(response.statusCode).toBe(503);
    expect(response.json()).toEqual({ error: "unavailable" });
    expect(response.body).not.toContain("mailbox failure");
  });

  it("delegates Calendar channel authentication before durable acknowledgement", async () => {
    services.ingress.acceptCalendarPush = vi.fn(async () => "accepted" as const);
    const headers = {
      "x-goog-channel-id": "calendar-channel-1",
      "x-goog-channel-token": "private-channel-token",
      "x-goog-resource-id": "calendar-resource-1",
      "x-goog-resource-uri": "https://www.googleapis.com/calendar/v3/calendars/primary/events",
      "x-goog-resource-state": "exists",
      "x-goog-message-number": "42",
    };
    const response = await server.inject({
      method: "POST",
      url: "/webhooks/google/calendar",
      headers,
    });

    expect(response.statusCode).toBe(202);
    expect(services.ingress.acceptCalendarPush).toHaveBeenCalledWith(expect.objectContaining(headers));
  });

  it("rejects unknown Calendar channels and retries store failures", async () => {
    services.ingress.acceptCalendarPush = vi.fn(async () => "unauthorized" as const);
    const unauthorized = await server.inject({
      method: "POST",
      url: "/webhooks/google/calendar",
      headers: { "x-goog-channel-id": "unknown" },
    });
    expect(unauthorized.statusCode).toBe(401);

    services.ingress.acceptCalendarPush = vi.fn(async () => {
      throw new Error("private store detail");
    });
    const unavailable = await server.inject({
      method: "POST",
      url: "/webhooks/google/calendar",
      headers: { "x-goog-channel-id": "known" },
    });
    expect(unavailable.statusCode).toBe(503);
    expect(unavailable.body).not.toContain("private store detail");
  });
});
