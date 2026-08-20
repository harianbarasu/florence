import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { basename } from "node:path";
import { fileURLToPath } from "node:url";
import helmet from "@fastify/helmet";
import fastifyStatic from "@fastify/static";
import {
  decodeImageVaultKey,
  EncryptedImageVault,
  ImageVaultError,
  normalizeHeicToJpeg,
} from "@florence/artifacts";
import {
  completeFamilyOnboardingInputSchema,
  disconnectGoogleConnectionInputSchema,
  familyCalendarMonthQuerySchema,
  familyMemberMutationInputSchema,
  idSchema,
  patchFactInputSchema,
  patchWatchInputSchema,
  preferencesInputSchema,
  sessionInputSchema,
} from "@florence/contracts";
import { FlorenceStoreConflict, FlorenceStoreUnauthorized, PostgresFlorenceStore } from "@florence/database";
import { GoogleConnection, GoogleConnectionError } from "@florence/google";
import { LinqClient } from "@florence/linq";
import Fastify, { type FastifyInstance, type FastifyReply, type FastifyRequest } from "fastify";
import { z } from "zod";
import { EnrollmentCodes } from "./enrollment.js";
import { Florence } from "./florence.js";
import { createLinqIngress, type LinqIngress, LinqIngressError } from "./linq-ingress.js";
import { createFlorenceReasonerFromEnv } from "./reasoner.js";

export type AdultCaller = { adultId: string };

export interface CallerResolver {
  resolve(request: FastifyRequest): Promise<AdultCaller | null>;
  issueSession(adultId: string): { caller: AdultCaller; token: string };
}

export type AppDependencies = {
  florence: Florence;
  callerResolver: CallerResolver;
  ready: () => Promise<void>;
  linqIngress?: LinqIngress;
  close?: () => Promise<void>;
};

export type AppOptions = {
  frontendRoot?: string;
  serveFrontend?: boolean;
};

const defaultFrontendRoot = fileURLToPath(new URL("../../web/dist", import.meta.url));
const sessionCookieName = "florence_session";
const sessionLifetimeSeconds = 7 * 24 * 60 * 60;
const memberParamsSchema = z.object({ memberId: idSchema }).strict();
const factParamsSchema = z.object({ factId: idSchema }).strict();
const watchParamsSchema = z.object({ workId: idSchema }).strict();

export function createDefaultDependencies(env: NodeJS.ProcessEnv = process.env): AppDependencies {
  const databaseUrl = requiredEnv(env, "FLORENCE_DATABASE_URL");
  const linqApiKey = requiredEnv(env, "LINQ_API_KEY");
  const enrollmentCodes = new EnrollmentCodes(requiredEnv(env, "FLORENCE_ENROLLMENT_SECRET"));
  const store = new PostgresFlorenceStore(databaseUrl);
  const linq = new LinqClient({ apiKey: linqApiKey });
  const imageVault = new EncryptedImageVault({
    rootDirectory: requiredEnv(env, "FLORENCE_IMAGE_VAULT_DIRECTORY"),
    encryptionKey: decodeImageVaultKey(requiredEnv(env, "FLORENCE_IMAGE_VAULT_KEY")),
    normalizeHeic: normalizeHeicToJpeg,
  });
  const google = createDefaultGoogleConnection(env, store);
  const setupOrigin = new URL(requiredEnv(env, "GOOGLE_OAUTH_REDIRECT_URI")).origin;
  const florence = new Florence({
    store,
    linq,
    google,
    reasoner: createFlorenceReasonerFromEnv(env),
    enrollmentCodes,
    imageVault,
    messagesUrl: requiredEnv(env, "FLORENCE_MESSAGES_URL"),
    linqSenderPhoneNumber: requiredEnv(env, "LINQ_FROM_PHONE"),
    setupOrigin,
  });
  const linqIngress = createLinqIngress({
    signingSecret: requiredEnv(env, "LINQ_WEBHOOK_SECRET"),
    expectedPartnerId: requiredEnv(env, "LINQ_PARTNER_ID"),
    linq,
    imageVault,
    florence,
  });
  const callerResolver = createSessionCallerResolver(env);
  florence.start();
  return {
    florence,
    linqIngress,
    callerResolver,
    ready: () => store.ready(),
    close: async () => {
      florence.stop();
      await store.close();
    },
  };
}

export function createSessionCallerResolver(env: NodeJS.ProcessEnv = process.env): CallerResolver {
  const encodedSessionSecret = requiredEnv(env, "FLORENCE_SESSION_SECRET");
  if (Buffer.byteLength(encodedSessionSecret) < 32) {
    throw new Error("FLORENCE_SESSION_SECRET must contain at least 32 bytes");
  }
  const sessionSecret = Buffer.from(encodedSessionSecret);
  return {
    async resolve(request) {
      const adultId = verifySession(cookieValue(request, sessionCookieName), sessionSecret);
      return adultId ? { adultId } : null;
    },
    issueSession(adultId) {
      if (!idSchema.safeParse(adultId).success) throw new Error("Session adult ID must be a UUID");
      return { caller: { adultId }, token: issueSessionToken(adultId, sessionSecret) };
    },
  };
}

export async function buildApp(
  dependencies: AppDependencies = createDefaultDependencies(),
  options: AppOptions = {},
): Promise<FastifyInstance> {
  const app = Fastify({ logger: process.env.NODE_ENV === "production" });
  await app.register(helmet, { referrerPolicy: { policy: "no-referrer" } });
  if (dependencies.close) app.addHook("onClose", dependencies.close);
  app.setErrorHandler((error, request, reply) => {
    if (error instanceof FlorenceStoreUnauthorized) {
      return reply.status(403).send({ error: "forbidden" });
    }
    if (error instanceof FlorenceStoreConflict) {
      return reply.status(409).send({ error: "conflict" });
    }
    if (error instanceof GoogleConnectionError) {
      const status = error.code === "not_found" ? 404 : error.code === "identity_conflict" ? 409 : 400;
      return reply.status(status).send({ error: error.code });
    }
    if (error instanceof ImageVaultError) {
      return reply.status(400).send({ error: error.code });
    }
    if (error instanceof z.ZodError) {
      return reply.status(400).send({ error: "invalid_request" });
    }
    request.log.error({ code: "florence_request_failed", method: request.method, url: request.url });
    return reply.status(500).send({ error: "internal_error" });
  });
  await registerLinqWebhook(app, dependencies.linqIngress);

  app.get("/api/health", async () => {
    await dependencies.ready();
    return { status: "ok", service: "florence-api" };
  });

  app.get("/api/v1/session", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    return caller ? { adultId: caller.adultId } : undefined;
  });

  app.post("/api/v1/session", async (request, reply) => {
    reply.header("Cache-Control", "no-store");
    const body = sessionInputSchema.safeParse(request.body);
    if (!body.success) return invalidRequest(reply);
    if ("accessToken" in body.data) {
      const access = await dependencies.florence.redeemAccessLink(body.data.accessToken);
      if (!access) return reply.status(401).send({ error: "invalid_or_expired_access_link" });
      const session = dependencies.callerResolver.issueSession(access.adultId);
      reply.header("Set-Cookie", sessionCookie(session.token, process.env.NODE_ENV === "production"));
      return { adultId: session.caller.adultId, accessPath: access.accessPath };
    }
    const enrollment = await dependencies.florence.redeemSetupLink(body.data).catch((error: unknown) => {
      if (error instanceof FlorenceStoreConflict) return null;
      throw error;
    });
    if (!enrollment) return reply.status(401).send({ error: "invalid_or_expired_setup_link" });
    const session = dependencies.callerResolver.issueSession(enrollment.adultId);
    reply.header("Set-Cookie", sessionCookie(session.token, process.env.NODE_ENV === "production"));
    return { adultId: session.caller.adultId };
  });

  app.delete("/api/v1/session", async (_request, reply) => {
    reply.header("Set-Cookie", expiredSessionCookie(process.env.NODE_ENV === "production"));
    return { signedOut: true };
  });

  app.get("/api/v1/workspace", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    return caller ? { workspace: await dependencies.florence.workspaceForAdult(caller.adultId) } : undefined;
  });

  app.get<{ Querystring: { month?: string } }>("/api/v1/calendar", async (request, reply) => {
    reply.header("Cache-Control", "private, no-store");
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    const query = familyCalendarMonthQuerySchema.safeParse(request.query);
    if (!query.success) return invalidRequest(reply);
    return dependencies.florence.familyCalendarMonthForAdult(caller.adultId, query.data.month);
  });

  app.put("/api/v1/vault/household", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    const input = completeFamilyOnboardingInputSchema.safeParse(request.body);
    if (!input.success) return invalidRequest(reply);
    return {
      workspace: await dependencies.florence.completeFamilyOnboarding(caller.adultId, input.data),
    };
  });

  app.put("/api/v1/vault/members/:memberId", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    const params = memberParamsSchema.safeParse(request.params);
    const body = familyMemberMutationInputSchema.safeParse(request.body);
    if (!params.success || !body.success) return invalidRequest(reply);
    return {
      workspace: await dependencies.florence.putMember(caller.adultId, params.data.memberId, body.data),
    };
  });

  app.patch("/api/v1/vault/facts/:factId", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    const params = factParamsSchema.safeParse(request.params);
    const body = patchFactInputSchema.safeParse(request.body);
    if (!params.success || !body.success) return invalidRequest(reply);
    return {
      workspace: await dependencies.florence.correctFact(
        caller.adultId,
        params.data.factId,
        body.data.statement,
      ),
    };
  });

  app.delete("/api/v1/vault/facts/:factId", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    const params = factParamsSchema.safeParse(request.params);
    if (!params.success || request.body !== undefined) return invalidRequest(reply);
    return {
      workspace: await dependencies.florence.deleteFact(caller.adultId, params.data.factId),
    };
  });

  app.patch("/api/v1/vault/watches/:workId", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    const params = watchParamsSchema.safeParse(request.params);
    const body = patchWatchInputSchema.safeParse(request.body);
    if (!params.success || !body.success) return invalidRequest(reply);
    return {
      workspace: await dependencies.florence.patchWatch(caller.adultId, params.data.workId, body.data),
    };
  });

  app.delete("/api/v1/vault/watches/:workId", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    const params = watchParamsSchema.safeParse(request.params);
    if (!params.success) return invalidRequest(reply);
    return {
      workspace: await dependencies.florence.stopWatch(caller.adultId, params.data.workId),
    };
  });

  app.put("/api/v1/preferences", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    const body = preferencesInputSchema.safeParse(request.body);
    if (!body.success) return invalidRequest(reply);
    return { workspace: await dependencies.florence.savePreferences(caller.adultId, body.data) };
  });

  app.post("/api/v1/workspace/google-connections", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    const sessionBindingDigest = googleSessionBinding(request);
    if (!sessionBindingDigest) return reply.status(401).send({ error: "browser_session_required" });
    const result = await dependencies.florence.beginGoogle(caller.adultId, sessionBindingDigest);
    reply.header("Cache-Control", "no-store");
    return { authorizationUrl: result.authorizationUrl };
  });

  app.delete("/api/v1/workspace/google-connections", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    const body = disconnectGoogleConnectionInputSchema.safeParse(request.body);
    if (!body.success) return invalidRequest(reply);
    return {
      workspace: await dependencies.florence.disconnectGoogle(caller.adultId, body.data.connectionId),
    };
  });

  app.get<{ Querystring: { state?: string; code?: string; error?: string } }>(
    "/oauth/google/callback",
    async (request, reply) => {
      const caller = await requireAdult(request, reply, dependencies.callerResolver);
      if (!caller) return;
      const sessionBindingDigest = googleSessionBinding(request);
      if (!sessionBindingDigest) return reply.status(401).send({ error: "browser_session_required" });
      if (request.query.error || !request.query.state || !request.query.code) {
        return reply.redirect("/?google=authorization_cancelled");
      }
      try {
        await dependencies.florence.finishGoogle({
          adultId: caller.adultId,
          state: request.query.state,
          code: request.query.code,
          sessionBindingDigest,
        });
        return reply.redirect("/?google=connected");
      } catch (error) {
        if (error instanceof GoogleConnectionError) {
          return reply.redirect(`/?google=${encodeURIComponent(error.code)}`);
        }
        throw error;
      }
    },
  );

  if (options.serveFrontend ?? process.env.NODE_ENV === "production") {
    await registerFrontend(app, options.frontendRoot);
  }
  return app;
}

function createDefaultGoogleConnection(
  env: NodeJS.ProcessEnv,
  store: PostgresFlorenceStore,
): GoogleConnection {
  const clientId = requiredEnv(env, "GOOGLE_OAUTH_CLIENT_ID");
  const clientSecret = requiredEnv(env, "GOOGLE_OAUTH_CLIENT_SECRET");
  const redirectUri = requiredEnv(env, "GOOGLE_OAUTH_REDIRECT_URI");
  const encodedKey = requiredEnv(env, "GOOGLE_CREDENTIAL_KEY");
  const encryptionKey = Buffer.from(encodedKey, "base64");
  if (encryptionKey.byteLength !== 32 || encryptionKey.toString("base64") !== encodedKey) {
    throw new Error("GOOGLE_CREDENTIAL_KEY must be a canonical base64-encoded 32-byte key");
  }
  return new GoogleConnection({ store, clientId, clientSecret, redirectUri, encryptionKey });
}

async function registerLinqWebhook(app: FastifyInstance, ingress: LinqIngress | undefined): Promise<void> {
  await app.register(async (routes) => {
    routes.removeContentTypeParser("application/json");
    routes.addContentTypeParser("application/json", { parseAs: "buffer" }, (_request, body, done) => {
      done(null, body);
    });
    routes.post<{ Body: Buffer; Querystring: { version?: string } }>(
      "/api/v1/webhooks/linq",
      { bodyLimit: 1024 * 1024 },
      async (request, reply) => {
        if (!ingress) return reply.status(503).send({ error: "linq_ingress_not_configured" });
        try {
          const result = await ingress.receive({
            rawBody: request.body,
            headers: request.headers,
            version: request.query.version,
          });
          const accepted = result.disposition === "accepted" || result.disposition === "duplicate";
          return reply.status(accepted ? 202 : 200).send(result);
        } catch (error) {
          if (error instanceof FlorenceStoreConflict) {
            return reply.status(409).send({ error: "conflict" });
          }
          if (error instanceof LinqIngressError) {
            const status = error.retryable ? 503 : error.code === "invalid_signature" ? 401 : 400;
            return reply.status(status).send({ error: error.code });
          }
          request.log.error({ code: "linq_ingress_unavailable" });
          return reply.status(503).send({ error: "linq_ingress_unavailable" });
        }
      },
    );
  });
}

async function requireAdult(
  request: FastifyRequest,
  reply: FastifyReply,
  resolver: CallerResolver,
): Promise<AdultCaller | null> {
  const caller = await resolver.resolve(request);
  if (!caller) reply.status(401).send({ error: "unauthorized" });
  return caller;
}

function invalidRequest(reply: FastifyReply) {
  return reply.status(400).send({ error: "invalid_request" });
}

function requiredEnv(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]?.trim();
  if (!value) throw new Error(`${name} is required to start Florence`);
  return value;
}

function cookieValue(request: FastifyRequest, name: string): string | null {
  for (const part of (request.headers.cookie ?? "").split(";")) {
    const separator = part.indexOf("=");
    if (separator < 0 || part.slice(0, separator).trim() !== name) continue;
    try {
      return decodeURIComponent(part.slice(separator + 1).trim());
    } catch {
      return null;
    }
  }
  return null;
}

function googleSessionBinding(request: FastifyRequest): string | null {
  const session = cookieValue(request, sessionCookieName);
  return session ? createHash("sha256").update(`florence-google-session-v1\0${session}`).digest("hex") : null;
}

function issueSessionToken(adultId: string, secret: Buffer): string {
  const expiresAt = Math.floor(Date.now() / 1_000) + sessionLifetimeSeconds;
  const unsigned = `v2.${adultId}.${expiresAt}`;
  const signature = createHmac("sha256", secret)
    .update(`florence-browser-session-v2\0${unsigned}`)
    .digest("base64url");
  return `${unsigned}.${signature}`;
}

function verifySession(token: string | null, secret: Buffer): string | null {
  if (!token) return null;
  const [version, adultId, encodedExpiry, signature, extra] = token.split(".");
  if (version !== "v2" || !adultId || !encodedExpiry || !signature || extra) return null;
  if (!idSchema.safeParse(adultId).success) return null;
  const unsigned = `${version}.${adultId}.${encodedExpiry}`;
  const expected = createHmac("sha256", secret)
    .update(`florence-browser-session-v2\0${unsigned}`)
    .digest("base64url");
  if (!safeTokenEqual(signature, expected) || !/^\d+$/.test(encodedExpiry)) return null;
  const expiresAt = Number(encodedExpiry);
  if (!Number.isSafeInteger(expiresAt) || expiresAt <= Math.floor(Date.now() / 1_000)) return null;
  return adultId;
}

function sessionCookie(sessionToken: string, secure: boolean): string {
  return [
    `${sessionCookieName}=${encodeURIComponent(sessionToken)}`,
    "HttpOnly",
    "SameSite=Lax",
    "Path=/",
    `Max-Age=${sessionLifetimeSeconds}`,
    ...(secure ? ["Secure"] : []),
  ].join("; ");
}

function expiredSessionCookie(secure: boolean): string {
  return [
    `${sessionCookieName}=`,
    "HttpOnly",
    "SameSite=Lax",
    "Path=/",
    "Max-Age=0",
    ...(secure ? ["Secure"] : []),
  ].join("; ");
}

function safeTokenEqual(left: string, right: string): boolean {
  const leftBytes = Buffer.from(left);
  const rightBytes = Buffer.from(right);
  return leftBytes.length === rightBytes.length && timingSafeEqual(leftBytes, rightBytes);
}

async function registerFrontend(app: FastifyInstance, frontendRoot?: string): Promise<void> {
  await app.register(fastifyStatic, {
    root: frontendRoot ?? defaultFrontendRoot,
    prefix: "/",
    cacheControl: false,
    setHeaders(reply, filePath) {
      if (basename(filePath) === "index.html") reply.header("Cache-Control", "no-store");
      else if (filePath.includes("/assets/")) {
        reply.header("Cache-Control", "public, max-age=31536000, immutable");
      } else reply.header("Cache-Control", "public, max-age=3600");
    },
  });
  app.setNotFoundHandler((request, reply) => {
    const pathname = request.url.split("?", 1)[0] ?? request.url;
    if (pathname === "/api" || pathname.startsWith("/api/") || !["GET", "HEAD"].includes(request.method)) {
      return reply.status(404).send({ error: "not_found" });
    }
    reply.header("Cache-Control", "no-store");
    return reply.type("text/html; charset=utf-8").sendFile("index.html");
  });
}
