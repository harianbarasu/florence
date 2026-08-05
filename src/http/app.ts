import { createHash, timingSafeEqual } from "node:crypto";
import helmet from "@fastify/helmet";
import rateLimit from "@fastify/rate-limit";
import Fastify, {
  type FastifyInstance,
  type FastifyLoggerOptions,
  type FastifyReply,
  type FastifyRequest,
  LogController,
} from "fastify";
import rawBody from "fastify-raw-body";
import type { LoggerOptions as PinoLoggerOptions } from "pino";
import { Counter, collectDefaultMetrics, Histogram, Registry } from "prom-client";
import { z } from "zod";
import { type GmailPubSubEvent, GoogleAdapterError, parseGmailPubSubPush } from "../adapters/google/index.js";
import {
  type LinqWebhookEvent,
  LinqWebhookPayloadError,
  LinqWebhookVerificationError,
  parseVerifiedLinqWebhook,
} from "../adapters/linq/index.js";
import {
  type GooglePubSubAuthorization,
  GooglePubSubOidcAuthenticator,
} from "../security/google-pubsub-auth.js";
import {
  DEFAULT_CALENDAR_PUSH_BODY_LIMIT_BYTES,
  DEFAULT_GMAIL_PUSH_BODY_LIMIT_BYTES,
  DEFAULT_LINQ_BODY_LIMIT_BYTES,
  type FlorenceHttpConfigInput,
  parseFlorenceHttpConfig,
} from "./config.js";
import type { FlorenceHttpServices, OperatorStatus } from "./contracts.js";
import { sendCustomerExportHandoff, sendHandoffPage } from "./pages.js";

const oauthStartQuerySchema = z
  .object({
    handoff: z.string().min(16).max(2_048),
  })
  .strip();

const oauthCallbackQuerySchema = z
  .object({
    state: z.string().min(1).max(4_096),
    code: z.string().min(1).max(4_096).optional(),
    error: z
      .string()
      .min(1)
      .max(128)
      .regex(/^[A-Za-z0-9._-]+$/u)
      .optional(),
  })
  .strip()
  .refine((value) => Boolean(value.code) !== Boolean(value.error));

const gmailPushQuerySchema = z
  .object({
    token: z.string().min(1).max(2_048),
  })
  .strict();

const customerExportParamsSchema = z
  .object({
    token: z
      .string()
      .min(32)
      .max(2_048)
      .regex(/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/u),
  })
  .strict();

const customerExportFilenameSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[A-Za-z0-9][A-Za-z0-9._-]*\.json$/u);

const operatorStatusSchema = z
  .object({
    status: z.enum(["ok", "degraded"]),
    checks: z.record(
      z
        .string()
        .min(1)
        .max(64)
        .regex(/^[a-z0-9_.-]+$/u),
      z.enum(["ok", "degraded", "unavailable"]),
    ),
    semanticTimers: z
      .object({
        status: z.enum(["ok", "degraded", "unavailable"]),
        deadCount: z.number().int().nonnegative().nullable(),
      })
      .strict()
      .optional(),
  })
  .strict();

export interface CreateFlorenceHttpServerOptions {
  config: FlorenceHttpConfigInput;
  services: FlorenceHttpServices;
  googlePubSubAuthenticator?: {
    authenticate(input: GooglePubSubAuthorization): Promise<boolean>;
  };
  logger?: false | HttpLoggerOptions;
}

export type HttpLoggerOptions = FastifyLoggerOptions & PinoLoggerOptions;

export function productionHttpLoggerOptions(level: string): HttpLoggerOptions {
  return {
    level,
    redact: {
      paths: [
        "req.headers.authorization",
        "req.headers.cookie",
        "req.headers['webhook-signature']",
        "req.headers['x-goog-channel-token']",
        "request.headers.authorization",
        "request.headers.cookie",
        "headers.authorization",
        "headers.cookie",
        "body",
        "rawBody",
        "query",
      ],
      remove: true,
    },
  };
}

export async function createFlorenceHttpServer(
  options: CreateFlorenceHttpServerOptions,
): Promise<FastifyInstance> {
  const config = parseFlorenceHttpConfig(options.config);
  const googlePubSubAuthenticator = options.googlePubSubAuthenticator ?? new GooglePubSubOidcAuthenticator();
  const app = Fastify({
    bodyLimit: config.bodyLimitBytes,
    trustProxy: config.trustProxy,
    requestIdHeader: false,
    logger: options.logger ?? false,
    logController: new LogController({ disableRequestLogging: true }),
  });
  const metrics = createHttpMetrics();

  await app.register(helmet, {
    global: true,
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'none'"],
        baseUri: ["'none'"],
        formAction: ["'none'"],
        frameAncestors: ["'none'"],
      },
    },
    referrerPolicy: { policy: "no-referrer" },
  });
  await app.register(rateLimit, {
    global: true,
    max: config.rateLimit.max,
    timeWindow: config.rateLimit.timeWindowMs,
    keyGenerator: (request) => request.ip,
    errorResponseBuilder: () => ({ statusCode: 429, error: "rate_limited" }),
  });
  await app.register(rawBody, {
    global: false,
    encoding: false,
    runFirst: true,
  });

  app.addHook("onResponse", async (request, reply) => {
    const labels = {
      method: request.method,
      route: safeRouteLabel(request),
      status_class: `${Math.floor(reply.statusCode / 100)}xx`,
    };
    metrics.requests.inc(labels);
    metrics.duration.observe(labels, reply.elapsedTime / 1_000);
    request.log.info(
      {
        event: "http_request",
        method: labels.method,
        route: labels.route,
        statusCode: reply.statusCode,
        durationMs: Math.round(reply.elapsedTime),
      },
      "request completed",
    );
  });

  app.get("/", { config: { rateLimit: false } }, async (_request, reply) => sendHandoffPage(reply, "home"));
  app.get("/privacy", { config: { rateLimit: false } }, async (_request, reply) =>
    sendHandoffPage(reply, "privacy"),
  );
  app.get("/terms", { config: { rateLimit: false } }, async (_request, reply) =>
    sendHandoffPage(reply, "terms"),
  );

  app.get(
    "/control/export/:token",
    { config: { rateLimit: { max: 10, timeWindow: 60_000, groupId: "customer_export" } } },
    async (request, reply) => {
      reply.header("cache-control", "no-store").header("pragma", "no-cache");
      const params = customerExportParamsSchema.safeParse(request.params);
      if (!params.success) {
        return reply.code(404).send({ error: "not_found" });
      }
      return sendCustomerExportHandoff(reply, params.data.token);
    },
  );

  app.get(
    "/control/export/:token/download",
    { config: { rateLimit: { max: 10, timeWindow: 60_000, groupId: "customer_export_download" } } },
    async (request, reply) => {
      reply.header("cache-control", "no-store").header("pragma", "no-cache");
      const params = customerExportParamsSchema.safeParse(request.params);
      if (!params.success) {
        return reply.code(404).send({ error: "not_found" });
      }

      try {
        const result = await options.services.customerExport.consumeExportToken(params.data.token);
        switch (result.status) {
          case "download": {
            const filename = customerExportFilenameSchema.parse(result.filename);
            return reply
              .header("content-type", "application/json; charset=utf-8")
              .header("content-disposition", `attachment; filename="${filename}"`)
              .send(result.artifact);
          }
          case "invalid":
            return reply.code(404).send({ error: "not_found" });
          case "expired":
          case "consumed":
            return reply.code(410).send({ error: "link_unavailable" });
          case "unavailable":
            return reply.code(503).send({ error: "unavailable" });
        }
      } catch {
        request.log.warn({ event: "customer_export_failed" }, "customer export failed");
        return reply.code(503).send({ error: "unavailable" });
      }
    },
  );

  app.get("/healthz", { config: { rateLimit: false } }, async (_request, reply) =>
    reply.header("cache-control", "no-store").send({ status: "ok" }),
  );

  app.get("/readyz", { config: { rateLimit: false } }, async (request, reply) => {
    let ready = false;
    try {
      ready = await options.services.readiness.isReady();
    } catch {
      request.log.warn({ event: "readiness_failed" }, "readiness probe failed");
    }
    return reply
      .header("cache-control", "no-store")
      .code(ready ? 200 : 503)
      .send({ status: ready ? "ready" : "not_ready" });
  });

  app.get("/metrics", { config: { rateLimit: false } }, async (_request, reply) => {
    const body = await metrics.registry.metrics();
    return reply
      .header("content-type", metrics.registry.contentType)
      .header("cache-control", "no-store")
      .send(body);
  });

  app.post(
    "/webhooks/linq",
    {
      bodyLimit: Math.min(config.bodyLimitBytes, DEFAULT_LINQ_BODY_LIMIT_BYTES),
      config: {
        rawBody: true,
        rateLimit: { max: 300, timeWindow: 60_000, groupId: "linq_webhook" },
      },
    },
    async (request, reply) => {
      reply.header("cache-control", "no-store");
      if (config.linqWebhook === null) {
        metrics.ingress.inc({ source: "linq", outcome: "unavailable" });
        return reply.code(503).send({ error: "unavailable" });
      }
      if (request.rawBody === undefined) {
        metrics.ingress.inc({ source: "linq", outcome: "rejected" });
        return reply.code(400).send({ error: "invalid_request" });
      }

      let event: LinqWebhookEvent | null;
      try {
        event = parseVerifiedLinqWebhook({
          rawBody: request.rawBody,
          headers: request.headers,
          config: config.linqWebhook,
        });
      } catch (error) {
        metrics.ingress.inc({ source: "linq", outcome: "rejected" });
        if (error instanceof LinqWebhookVerificationError) {
          return reply.code(401).send({ error: "unauthorized" });
        }
        if (error instanceof LinqWebhookPayloadError) {
          return reply.code(400).send({ error: "invalid_request" });
        }
        request.log.warn({ event: "linq_webhook_parse_failed" }, "webhook parsing failed");
        return reply.code(400).send({ error: "invalid_request" });
      }

      if (event === null) {
        metrics.ingress.inc({ source: "linq", outcome: "ignored" });
        return reply.code(204).send();
      }

      try {
        await options.services.ingress.acceptLinq(event);
      } catch {
        metrics.ingress.inc({ source: "linq", outcome: "unavailable" });
        request.log.warn({ event: "linq_webhook_accept_failed" }, "durable ingress failed");
        return reply.code(503).send({ error: "unavailable" });
      }

      metrics.ingress.inc({ source: "linq", outcome: "accepted" });
      return reply.code(202).send();
    },
  );

  app.post(
    "/webhooks/google/gmail",
    {
      bodyLimit: Math.min(config.bodyLimitBytes, DEFAULT_GMAIL_PUSH_BODY_LIMIT_BYTES),
      config: { rateLimit: { max: 300, timeWindow: 60_000, groupId: "gmail_push" } },
    },
    async (request, reply) => {
      reply.header("cache-control", "no-store");
      const authentication = config.gmailPubSubAuthentication;
      const query = gmailPushQuerySchema.safeParse(request.query);
      if (
        authentication === null ||
        !query.success ||
        !secretsMatch(authentication.verificationToken, query.data.token)
      ) {
        metrics.ingress.inc({ source: "gmail", outcome: "rejected" });
        return reply.code(authentication === null ? 503 : 401).send({
          error: authentication === null ? "unavailable" : "unauthorized",
        });
      }

      let oidcAuthorized = false;
      try {
        oidcAuthorized = await googlePubSubAuthenticator.authenticate({
          authorizationHeader: request.headers.authorization,
          expectedAudience: authentication.oidcAudience,
          expectedServiceAccountEmail: authentication.serviceAccountEmail,
        });
      } catch {
        oidcAuthorized = false;
      }
      if (!oidcAuthorized) {
        metrics.ingress.inc({ source: "gmail", outcome: "rejected" });
        return reply.code(401).send({ error: "unauthorized" });
      }

      let event: GmailPubSubEvent;
      try {
        event = parseGmailPubSubPush(request.body);
      } catch (error) {
        metrics.ingress.inc({ source: "gmail", outcome: "rejected" });
        if (!(error instanceof GoogleAdapterError)) {
          request.log.warn({ event: "gmail_push_parse_failed" }, "push parsing failed");
        }
        return reply.code(400).send({ error: "invalid_request" });
      }

      try {
        await options.services.ingress.acceptGmailPush(event);
      } catch {
        metrics.ingress.inc({ source: "gmail", outcome: "unavailable" });
        request.log.warn({ event: "gmail_push_accept_failed" }, "durable ingress failed");
        return reply.code(503).send({ error: "unavailable" });
      }

      metrics.ingress.inc({ source: "gmail", outcome: "accepted" });
      return reply.code(202).send();
    },
  );

  app.post(
    "/webhooks/google/calendar",
    {
      bodyLimit: Math.min(config.bodyLimitBytes, DEFAULT_CALENDAR_PUSH_BODY_LIMIT_BYTES),
      config: { rateLimit: { max: 300, timeWindow: 60_000, groupId: "calendar_push" } },
    },
    async (request, reply) => {
      reply.header("cache-control", "no-store");
      if (!config.googleCalendarPushEnabled) {
        metrics.ingress.inc({ source: "calendar", outcome: "unavailable" });
        return reply.code(503).send({ error: "unavailable" });
      }
      try {
        const outcome = await options.services.ingress.acceptCalendarPush(request.headers);
        if (outcome === "unauthorized") {
          metrics.ingress.inc({ source: "calendar", outcome: "rejected" });
          return reply.code(401).send({ error: "unauthorized" });
        }
      } catch {
        metrics.ingress.inc({ source: "calendar", outcome: "unavailable" });
        request.log.warn({ event: "calendar_push_accept_failed" }, "durable ingress failed");
        return reply.code(503).send({ error: "unavailable" });
      }
      metrics.ingress.inc({ source: "calendar", outcome: "accepted" });
      return reply.code(202).send();
    },
  );

  app.get(
    "/oauth/google/start",
    { config: { rateLimit: { max: 30, timeWindow: 60_000, groupId: "oauth" } } },
    async (request, reply) => {
      const query = oauthStartQuerySchema.safeParse(request.query);
      if (!query.success) {
        return sendHandoffPage(reply, "oauth_invalid", 400);
      }

      try {
        const result = await options.services.googleOAuth.start({
          handoffToken: query.data.handoff,
        });
        if (result.kind === "expired") {
          return sendHandoffPage(reply, "oauth_expired", 400);
        }
        if (result.kind === "invalid") {
          return sendHandoffPage(reply, "oauth_invalid", 400);
        }
        if (!isGoogleAuthorizationUrl(result.authorizationUrl)) {
          request.log.error({ event: "oauth_redirect_rejected" }, "OAuth redirect was rejected");
          return sendHandoffPage(reply, "oauth_unavailable", 503);
        }
        return reply
          .header("cache-control", "no-store")
          .header("pragma", "no-cache")
          .redirect(result.authorizationUrl, 303);
      } catch {
        request.log.warn({ event: "oauth_start_failed" }, "OAuth start failed");
        return sendHandoffPage(reply, "oauth_unavailable", 503);
      }
    },
  );

  app.get(
    "/oauth/google/callback",
    { config: { rateLimit: { max: 30, timeWindow: 60_000, groupId: "oauth" } } },
    async (request, reply) => {
      const query = oauthCallbackQuerySchema.safeParse(request.query);
      if (!query.success) {
        return sendHandoffPage(reply, "oauth_invalid", 400);
      }

      try {
        const result = await options.services.googleOAuth.complete({
          state: query.data.state,
          code: query.data.code ?? null,
          providerError: query.data.error ?? null,
        });
        switch (result.kind) {
          case "connected":
            return sendHandoffPage(reply, "oauth_connected");
          case "declined":
            return sendHandoffPage(reply, "oauth_declined", 400);
          case "expired":
            return sendHandoffPage(reply, "oauth_expired", 400);
          case "invalid":
            return sendHandoffPage(reply, "oauth_invalid", 400);
        }
      } catch {
        request.log.warn({ event: "oauth_callback_failed" }, "OAuth callback failed");
        return sendHandoffPage(reply, "oauth_unavailable", 503);
      }
    },
  );

  const requireOperator = createOperatorAuthenticator(config.operatorToken);

  app.get(
    "/operator/status",
    {
      onRequest: requireOperator,
      config: { rateLimit: { max: 30, timeWindow: 60_000, groupId: "operator" } },
    },
    async (request, reply) => {
      reply.header("cache-control", "no-store");
      try {
        const status = operatorStatusSchema.parse(await options.services.operations.status());
        return reply.send(status satisfies OperatorStatus);
      } catch {
        request.log.warn({ event: "operator_status_failed" }, "operator status failed");
        return reply.code(503).send({ error: "unavailable" });
      }
    },
  );

  app.setNotFoundHandler(async (_request, reply) =>
    reply.code(404).header("cache-control", "no-store").send({ error: "not_found" }),
  );

  app.setErrorHandler(async (error, request, reply) => {
    const statusCode = safeHttpErrorStatus(error);
    request.log.warn(
      {
        event: "http_error",
        method: request.method,
        route: safeRouteLabel(request),
        statusCode,
        errorType: safeErrorType(error),
      },
      "request failed",
    );
    return reply
      .code(statusCode)
      .header("cache-control", "no-store")
      .send({ error: safeHttpErrorCode(statusCode) });
  });

  await app.ready();
  return app;
}

function createHttpMetrics() {
  const registry = new Registry();
  collectDefaultMetrics({ register: registry, prefix: "florence_process_" });
  const requests = new Counter({
    name: "florence_http_requests_total",
    help: "Total Florence HTTP requests",
    labelNames: ["method", "route", "status_class"] as const,
    registers: [registry],
  });
  const duration = new Histogram({
    name: "florence_http_request_duration_seconds",
    help: "Florence HTTP request duration in seconds",
    labelNames: ["method", "route", "status_class"] as const,
    registers: [registry],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  });
  const ingress = new Counter({
    name: "florence_http_ingress_total",
    help: "Authenticated inbound provider deliveries",
    labelNames: ["source", "outcome"] as const,
    registers: [registry],
  });
  return { registry, requests, duration, ingress };
}

function safeRouteLabel(request: FastifyRequest): string {
  const route = request.routeOptions.url;
  return route || "unmatched";
}

function secretsMatch(expected: string, supplied: string): boolean {
  const expectedDigest = createHash("sha256").update(expected, "utf8").digest();
  const suppliedDigest = createHash("sha256").update(supplied, "utf8").digest();
  return timingSafeEqual(expectedDigest, suppliedDigest);
}

function createOperatorAuthenticator(expectedToken: string) {
  return async (request: FastifyRequest, reply: FastifyReply): Promise<void> => {
    const authorization = request.headers.authorization;
    const match = typeof authorization === "string" ? /^Bearer ([^\s]+)$/u.exec(authorization) : null;
    if (match?.[1] && secretsMatch(expectedToken, match[1])) {
      reply.header("cache-control", "no-store");
      return;
    }
    reply
      .code(401)
      .header("www-authenticate", 'Bearer realm="florence-operator"')
      .header("cache-control", "no-store")
      .send({ error: "unauthorized" });
  };
}

function isGoogleAuthorizationUrl(value: string): boolean {
  try {
    const url = new URL(value);
    return (
      url.protocol === "https:" &&
      url.hostname === "accounts.google.com" &&
      url.port === "" &&
      url.username === "" &&
      url.password === "" &&
      url.pathname.startsWith("/o/oauth2/")
    );
  } catch {
    return false;
  }
}

function safeHttpErrorStatus(error: unknown): number {
  const record = errorRecord(error);
  if (record.code === "FST_ERR_CTP_BODY_TOO_LARGE") {
    return 413;
  }
  const statusCode = record.statusCode;
  return typeof statusCode === "number" && statusCode >= 400 && statusCode < 500 ? statusCode : 500;
}

function safeHttpErrorCode(statusCode: number): string {
  if (statusCode === 413) return "payload_too_large";
  if (statusCode === 415) return "unsupported_media_type";
  if (statusCode === 429) return "rate_limited";
  if (statusCode >= 400 && statusCode < 500) return "invalid_request";
  return "internal_error";
}

function safeErrorType(error: unknown): string {
  const code = errorRecord(error).code;
  if (typeof code === "string" && /^FST_[A-Z0-9_]+$/u.test(code)) {
    return code;
  }
  return "unexpected";
}

function errorRecord(error: unknown): Record<string, unknown> {
  return typeof error === "object" && error !== null ? (error as Record<string, unknown>) : {};
}
