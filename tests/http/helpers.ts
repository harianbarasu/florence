import { createHmac } from "node:crypto";
import { readFileSync } from "node:fs";
import type { FlorenceHttpConfigInput, FlorenceHttpServices } from "../../src/http/index.js";

export const OPERATOR_TOKEN = "synthetic-operator-token-000000000001";
export const PUBSUB_TOKEN = "synthetic-pubsub-token-000000000001";
export const PUBSUB_AUTHORIZATION = "Bearer synthetic-google-id-token";
export const PUBSUB_OIDC_AUDIENCE = "https://florence.example.test/webhooks/google/gmail";
export const PUBSUB_SERVICE_ACCOUNT_EMAIL = "florence-push@example-project.iam.gserviceaccount.com";
export const LINQ_SIGNING_KEY = Buffer.alloc(32, 0x42);

export const HTTP_CONFIG: FlorenceHttpConfigInput = {
  publicUrl: "https://florence.example.test",
  operatorToken: OPERATOR_TOKEN,
  gmailPubSubAuthentication: {
    verificationToken: PUBSUB_TOKEN,
    oidcAudience: PUBSUB_OIDC_AUDIENCE,
    serviceAccountEmail: PUBSUB_SERVICE_ACCOUNT_EMAIL,
  },
  googleCalendarPushEnabled: true,
  linqWebhook: {
    webhookSecret: `whsec_${LINQ_SIGNING_KEY.toString("base64")}`,
    webhookToleranceMs: 5 * 60 * 1_000,
    webhookVersion: "2026-02-03",
  },
};

export function fakeHttpServices(): FlorenceHttpServices {
  return {
    ingress: {
      acceptLinq: async () => undefined,
      acceptGmailPush: async () => undefined,
      acceptCalendarPush: async () => "accepted",
    },
    googleOAuth: {
      start: async () => ({ kind: "invalid" }),
      complete: async () => ({ kind: "invalid" }),
    },
    customerExport: {
      consumeExportToken: async () => ({ status: "invalid" }),
    },
    readiness: {
      isReady: async () => true,
    },
    operations: {
      status: async () => ({ status: "ok", checks: { database: "ok", queue: "ok" } }),
      exportHousehold: async () => null,
      deleteHousehold: async () => "not_found",
    },
  };
}

export function readHttpFixture(name: string): string {
  return readFileSync(new URL(`./fixtures/${name}`, import.meta.url), "utf8");
}

export function signedLinqHeaders(rawBody: string, webhookId?: string): Record<string, string> {
  const payload = JSON.parse(rawBody) as { event_id: string };
  const id = webhookId ?? payload.event_id;
  const timestamp = String(Math.floor(Date.now() / 1_000));
  const signature = createHmac("sha256", LINQ_SIGNING_KEY)
    .update(Buffer.concat([Buffer.from(`${id}.${timestamp}.`), Buffer.from(rawBody)]))
    .digest("base64");
  return {
    "content-type": "application/json",
    "webhook-id": id,
    "webhook-timestamp": timestamp,
    "webhook-signature": `v1,${signature}`,
  };
}

export function gmailPushPayload(
  input: { email?: string; historyId?: string; messageId?: string } = {},
): Record<string, unknown> {
  const notification = Buffer.from(
    JSON.stringify({
      emailAddress: input.email ?? "parent@example.test",
      historyId: input.historyId ?? "7654321",
    }),
  ).toString("base64url");
  return {
    message: {
      data: notification,
      messageId: input.messageId ?? "pubsub-http-001",
      publishTime: "2026-08-05T15:30:00.000Z",
    },
    subscription: "projects/florence-test/subscriptions/gmail-http",
  };
}
