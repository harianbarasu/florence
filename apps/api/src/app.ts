import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { basename } from "node:path";
import { fileURLToPath } from "node:url";
import cors from "@fastify/cors";
import helmet from "@fastify/helmet";
import rateLimit from "@fastify/rate-limit";
import fastifyStatic from "@fastify/static";
import { DEFAULT_IMAGE_RETENTION_MS, decodeImageVaultKey, EncryptedImageVault } from "@florence/artifacts";
import {
  familyMemberProfileSchema,
  type HouseholdProfile,
  householdCreatedSignalSchema,
  idSchema,
  plannedAdultSchema,
  timestampSchema,
  verifiedAdultSchema,
} from "@florence/contracts";
import { HouseholdChiefOfStaff as ChiefOfStaff, type HouseholdChiefOfStaff } from "@florence/control-plane";
import { PostgresFlorenceRepository, SignalConflictError } from "@florence/database";
import { GoogleConnection, GoogleConnectionError } from "@florence/google";
import { LinqClient } from "@florence/linq";
import type { WorkerRuntime } from "@florence/runtime";
import Fastify, { type FastifyInstance, type FastifyReply, type FastifyRequest } from "fastify";
import { EnrollmentCodes } from "./enrollment.js";
import { createLinqIngress, type LinqIngress, LinqIngressError } from "./linq-ingress.js";

export type Caller = {
  adultId: string;
  authorizedHouseholdIds: readonly string[];
};

export interface CallerResolver {
  resolve(request: FastifyRequest): Promise<Caller | null>;
  issueSession?(credential: string): Promise<{ caller: Caller; token: string } | null>;
  otherPilotAdultId?(caller: Caller): Promise<string | null>;
  grantHousehold?(caller: Caller, householdId: string): Promise<void>;
}

type GoogleConnectionBoundary = Pick<GoogleConnection, "begin" | "finish" | "status" | "disconnect">;

type ChiefOfStaffBoundary = Pick<HouseholdChiefOfStaff, "accept" | "profile">;

export type AppDependencies = {
  chiefOfStaff: ChiefOfStaffBoundary;
  callerResolver: CallerResolver;
  readiness?: () => Promise<void>;
  databaseSchema?: string;
  webOrigin?: string;
  linqIngress?: LinqIngress;
  enrollmentCodes?: EnrollmentCodes;
  googleConnection?: GoogleConnectionBoundary;
  close?: () => Promise<void>;
};

export type AppOptions = {
  frontendRoot?: string;
  serveFrontend?: boolean;
};

const defaultFrontendRoot = fileURLToPath(new URL("../../web/dist", import.meta.url));
const defaultDatabaseSchema = "florence_v4";
const linqWebhookVersion = "2026-02-03";
const pilotSessionCookie = "florence_pilot_session";
const pilotSessionLifetimeSeconds = 7 * 24 * 60 * 60;

type PilotCredential = {
  token: string;
  adultId: string;
  householdIds: Set<string>;
  sessionSubject: string;
};

const createHouseholdCommandSchema = householdCreatedSignalSchema
  .pick({ name: true, timeZone: true })
  .extend({
    commandId: idSchema,
    occurredAt: timestampSchema,
    foundingAdultDisplayName: verifiedAdultSchema.shape.displayName,
    secondAdultDisplayName: plannedAdultSchema.shape.displayName,
    secondAdultRole: plannedAdultSchema.shape.role,
    secondAdultRelationship: plannedAdultSchema.shape.relationship,
  })
  .strict();

const memberFields = familyMemberProfileSchema.shape;
const upsertMemberCommandSchema = householdCreatedSignalSchema
  .pick({ occurredAt: true })
  .extend({
    commandId: idSchema,
    kind: memberFields.kind,
    role: memberFields.role,
    displayName: memberFields.displayName,
    relationship: memberFields.relationship,
    aliases: memberFields.aliases,
    birthYear: memberFields.birthYear,
    school: memberFields.school,
    currentGrade: memberFields.currentGrade,
    academicYear: memberFields.academicYear,
    gradeEffectiveFrom: memberFields.gradeEffectiveFrom,
    activities: memberFields.activities,
  })
  .strict()
  .superRefine((member, context) => {
    const grade = [member.currentGrade, member.academicYear, member.gradeEffectiveFrom];
    if (grade.some(Boolean) && !grade.every(Boolean)) {
      context.addIssue({
        code: "custom",
        path: ["currentGrade"],
        message: "Grade, academic year, and effective date must be provided together.",
      });
    }
  });

const householdParamsSchema = householdCreatedSignalSchema.pick({ householdId: true }).strict();
const memberParamsSchema = householdParamsSchema.extend({ memberId: idSchema }).strict();
const googleConnectionParamsSchema = householdParamsSchema.extend({ connectionId: idSchema }).strict();
const issueEnrollmentCommandSchema = householdCreatedSignalSchema
  .pick({ occurredAt: true })
  .extend({ commandId: idSchema })
  .strict();

class ApiOnlyRuntime implements WorkerRuntime {
  async deliberate(): Promise<never> {
    throw new Error("Conversation deliberation belongs in the Florence worker, not the API process");
  }
}

export function createDefaultDependencies(env: NodeJS.ProcessEnv = process.env): AppDependencies {
  if (!env.FLORENCE_DATABASE_URL) {
    throw new Error("FLORENCE_DATABASE_URL is required to start the Florence API");
  }
  const databaseSchema = env.FLORENCE_POSTGRES_SCHEMA ?? defaultDatabaseSchema;
  const webOrigin = configuredWebOrigin(env.FLORENCE_WEB_BASE_URL, env.NODE_ENV === "production");
  const repository = new PostgresFlorenceRepository({
    connectionString: env.FLORENCE_DATABASE_URL,
    schema: databaseSchema,
    applicationName: "florence-web",
    ssl: env.NODE_ENV === "production",
  });
  const chiefOfStaff = new ChiefOfStaff(repository, new ApiOnlyRuntime());
  const enrollmentCodes = env.FLORENCE_ENROLLMENT_SECRET
    ? new EnrollmentCodes(env.FLORENCE_ENROLLMENT_SECRET)
    : undefined;
  const linqIngress = createDefaultLinqIngress(env, repository, chiefOfStaff, enrollmentCodes);
  const googleConnection = createDefaultGoogleConnection(env, repository);
  return {
    chiefOfStaff,
    callerResolver: createPilotCallerResolver(env, repository),
    readiness: () => repository.ready(),
    databaseSchema,
    ...(webOrigin ? { webOrigin } : {}),
    ...(linqIngress ? { linqIngress } : {}),
    ...(enrollmentCodes ? { enrollmentCodes } : {}),
    ...(googleConnection ? { googleConnection } : {}),
    close: () => repository.close(),
  };
}

export function createPilotCallerResolver(
  env: NodeJS.ProcessEnv = process.env,
  householdAccess?: { listHouseholdIdsForAdult(adultId: string): Promise<readonly string[]> },
): CallerResolver {
  const encodedCredentials = env.FLORENCE_PILOT_CREDENTIALS;
  const encodedSessionSecret = env.FLORENCE_SESSION_SECRET;
  if (Boolean(encodedCredentials) !== Boolean(encodedSessionSecret)) {
    throw new Error("FLORENCE_PILOT_CREDENTIALS and FLORENCE_SESSION_SECRET must be configured together");
  }
  if (encodedSessionSecret && Buffer.byteLength(encodedSessionSecret) < 32) {
    throw new Error("FLORENCE_SESSION_SECRET must contain at least 32 bytes");
  }
  const sessionSecret = encodedSessionSecret ? Buffer.from(encodedSessionSecret) : null;
  const credentials = encodedCredentials
    ? parsePilotCredentials(encodedCredentials).map((credential) => ({
        ...credential,
        sessionSubject: sessionSubject(credential.adultId, sessionSecret as Buffer),
      }))
    : [];

  const callerFor = async (credential: PilotCredential): Promise<Caller> => {
    const persisted = householdAccess
      ? await householdAccess.listHouseholdIdsForAdult(credential.adultId)
      : [];
    return {
      adultId: credential.adultId,
      authorizedHouseholdIds: [...new Set([...credential.householdIds, ...persisted])],
    };
  };

  return {
    async resolve(request) {
      const accessToken = bearerToken(request);
      const credential = accessToken
        ? findPilotCredential(credentials, accessToken)
        : verifySession(cookieValue(request, pilotSessionCookie), credentials, sessionSecret);
      return credential ? callerFor(credential) : null;
    },
    async issueSession(accessToken) {
      const credential = findPilotCredential(credentials, accessToken);
      if (!credential || !sessionSecret) return null;
      return {
        caller: await callerFor(credential),
        token: issueSessionToken(credential.sessionSubject, sessionSecret),
      };
    },
    async otherPilotAdultId(caller) {
      return credentials.find((credential) => credential.adultId !== caller.adultId)?.adultId ?? null;
    },
    async grantHousehold(caller, householdId) {
      credentials.find((credential) => credential.adultId === caller.adultId)?.householdIds.add(householdId);
    },
  };
}

export async function buildApp(
  dependencies: AppDependencies = createDefaultDependencies(),
  options: AppOptions = {},
): Promise<FastifyInstance> {
  const isProduction = process.env.NODE_ENV === "production";
  const app = Fastify({
    bodyLimit: 1024 * 1024,
    logger: isProduction
      ? {
          level: process.env.LOG_LEVEL ?? "info",
          redact: {
            paths: ["req.headers.authorization", "req.headers.cookie", "request.body", "response.body"],
            censor: "[REDACTED]",
          },
        }
      : false,
    trustProxy: isProduction,
  });
  if (dependencies.close) app.addHook("onClose", dependencies.close);
  app.addHook("onRequest", async (request, reply) => {
    if (
      !dependencies.webOrigin ||
      ["GET", "HEAD", "OPTIONS"].includes(request.method) ||
      !cookieValue(request, pilotSessionCookie)
    ) {
      return;
    }
    if (request.headers.origin !== dependencies.webOrigin) {
      return reply.status(403).send({ error: "same_origin_required" });
    }
  });
  app.setErrorHandler((error, request, reply) => {
    if (error instanceof SignalConflictError) {
      return reply.status(409).send({ error: "idempotency_conflict" });
    }
    if (error instanceof GoogleConnectionError) {
      const status =
        error.code === "not_found"
          ? 404
          : error.code === "identity_conflict"
            ? 409
            : error.code === "provider_rejected"
              ? 502
              : 400;
      return reply.status(status).send({ error: error.code });
    }
    const statusCode = httpStatusCode(error);
    if (statusCode !== null && statusCode >= 400 && statusCode < 500) {
      const code =
        statusCode === 429 ? "rate_limited" : statusCode === 413 ? "payload_too_large" : "bad_request";
      return reply.status(statusCode).send({ error: code });
    }
    request.log.error({ code: "internal_error" }, "Florence request failed");
    return reply.status(500).send({ error: "internal_error" });
  });
  await app.register(rateLimit, { max: 180, timeWindow: "1 minute" });
  await app.register(helmet, {
    global: true,
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        baseUri: ["'none'"],
        connectSrc: ["'self'"],
        fontSrc: ["'self'"],
        formAction: ["'self'"],
        frameAncestors: ["'none'"],
        imgSrc: ["'self'", "data:", "blob:"],
        objectSrc: ["'none'"],
        scriptSrc: ["'self'"],
        styleSrc: ["'self'", "'unsafe-inline'"],
      },
    },
    crossOriginEmbedderPolicy: false,
    hsts: isProduction ? { maxAge: 31_536_000, includeSubDomains: true, preload: true } : false,
    referrerPolicy: { policy: "no-referrer" },
  });
  if (!isProduction) {
    await app.register(cors, {
      credentials: true,
      origin: ["http://127.0.0.1:5173", "http://localhost:5173"],
    });
  }

  await registerLinqWebhook(app, dependencies.linqIngress);

  app.get("/healthz", async () => ({ ok: true, service: "florence-web" }));

  app.get("/readyz", async (_request, reply) => {
    try {
      if (!dependencies.readiness) throw new Error("Database readiness is not configured");
      await dependencies.readiness();
      return {
        ok: true,
        database: true,
        schema: dependencies.databaseSchema ?? defaultDatabaseSchema,
      };
    } catch {
      return reply.status(503).send({ ok: false, database: false });
    }
  });

  app.get("/api/v1/session", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    return { adultId: caller.adultId };
  });

  app.post(
    "/api/v1/session",
    { config: { rateLimit: { max: 20, timeWindow: "10 minutes" } } },
    async (request, reply) => {
      const credential = bearerToken(request);
      if (!credential) return reply.status(400).send({ error: "pilot_access_code_required" });
      if (!dependencies.callerResolver.issueSession) {
        return reply.status(503).send({ error: "browser_session_not_configured" });
      }
      const session = await dependencies.callerResolver.issueSession(credential);
      if (!session) return reply.status(401).send({ error: "unauthorized" });
      reply.header("Set-Cookie", sessionCookie(session.token, process.env.NODE_ENV === "production"));
      return { adultId: session.caller.adultId };
    },
  );

  app.delete("/api/v1/session", async (_request, reply) => {
    reply.header("Set-Cookie", expiredSessionCookie(process.env.NODE_ENV === "production"));
    return { signedOut: true };
  });

  app.get("/api/v1/households", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    const profiles = await Promise.all(
      [...new Set(caller.authorizedHouseholdIds)].map((id) => dependencies.chiefOfStaff.profile(id)),
    );
    return { households: profiles.filter((profile) => profile !== null) };
  });

  app.post("/api/v1/households", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    const parsed = createHouseholdCommandSchema.safeParse(request.body);
    if (!parsed.success) return invalidRequest(reply, parsed.error.issues);
    const secondAdultId = await dependencies.callerResolver.otherPilotAdultId?.(caller);
    if (!secondAdultId) {
      return reply.status(503).send({ error: "second_pilot_adult_not_configured" });
    }
    const {
      commandId,
      occurredAt,
      name,
      timeZone,
      foundingAdultDisplayName,
      secondAdultDisplayName,
      secondAdultRole,
      secondAdultRelationship,
    } = parsed.data;
    const receipt = await dependencies.chiefOfStaff.accept({
      type: "household.created",
      signalId: commandId,
      householdId: commandId,
      idempotencyKey: `dashboard:household:${commandId}`,
      occurredAt,
      name,
      timeZone,
      foundingAdult: { id: caller.adultId, displayName: foundingAdultDisplayName },
      plannedAdult: {
        id: secondAdultId,
        displayName: secondAdultDisplayName,
        role: secondAdultRole,
        relationship: secondAdultRelationship,
      },
    });
    await dependencies.callerResolver.grantHousehold?.(caller, commandId);
    return reply.status(202).send({ receipt, householdId: commandId });
  });

  app.get("/api/v1/households/:householdId", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    const parsed = householdParamsSchema.safeParse(request.params);
    if (!parsed.success) return invalidRequest(reply, parsed.error.issues);
    if (!caller.authorizedHouseholdIds.includes(parsed.data.householdId)) {
      return reply.status(403).send({ error: "forbidden" });
    }
    const household = await dependencies.chiefOfStaff.profile(parsed.data.householdId);
    if (!household) return reply.status(404).send({ error: "not_found" });
    return { household };
  });

  app.put("/api/v1/households/:householdId/members/:memberId", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    const params = memberParamsSchema.safeParse(request.params);
    const body = upsertMemberCommandSchema.safeParse(request.body);
    if (!params.success) return invalidRequest(reply, params.error.issues);
    if (!body.success) return invalidRequest(reply, body.error.issues);
    if (!caller.authorizedHouseholdIds.includes(params.data.householdId)) {
      return reply.status(403).send({ error: "forbidden" });
    }

    const household = await dependencies.chiefOfStaff.profile(params.data.householdId);
    if (!household) return reply.status(404).send({ error: "not_found" });
    const actor = household.members.find((member) => member.id === caller.adultId);
    if (actor?.kind !== "adult" || actor.role !== "steward" || actor.status !== "verified") {
      return reply.status(403).send({ error: "steward_required" });
    }

    const existing = household.members.find((member) => member.id === params.data.memberId);
    if (!existing && body.data.kind === "adult") {
      return reply.status(409).send({ error: "adult_creation_requires_household_onboarding" });
    }
    if (existing && existing.kind !== body.data.kind) {
      return reply.status(409).send({ error: "member_kind_change_requires_dedicated_flow" });
    }
    if (body.data.kind === "child" && body.data.role !== "dependent") {
      return reply.status(400).send({ error: "child_role_must_be_dependent" });
    }
    if (body.data.kind === "adult" && body.data.role === "dependent") {
      return reply.status(400).send({ error: "adult_role_must_be_steward_or_caregiver" });
    }
    if (existing?.status === "verified" && existing.role !== body.data.role) {
      return reply.status(409).send({ error: "verified_authority_change_requires_dedicated_flow" });
    }

    const { commandId, occurredAt, ...memberDetails } = body.data;
    const receipt = await dependencies.chiefOfStaff.accept({
      type: "family.member.upserted",
      signalId: commandId,
      householdId: params.data.householdId,
      idempotencyKey: `dashboard:member:${commandId}`,
      occurredAt,
      actorAdultId: caller.adultId,
      member: { id: params.data.memberId, ...memberDetails },
      status: existing?.status ?? (memberDetails.kind === "adult" ? "planned" : "represented"),
    });
    return reply.status(202).send({ receipt });
  });

  app.post("/api/v1/households/:householdId/members/:memberId/linq-enrollment", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    if (!dependencies.enrollmentCodes) {
      return reply.status(503).send({ error: "linq_enrollment_not_configured" });
    }
    const params = memberParamsSchema.safeParse(request.params);
    const body = issueEnrollmentCommandSchema.safeParse(request.body);
    if (!params.success) return invalidRequest(reply, params.error.issues);
    if (!body.success) return invalidRequest(reply, body.error.issues);
    if (!caller.authorizedHouseholdIds.includes(params.data.householdId)) {
      return reply.status(403).send({ error: "forbidden" });
    }
    const household = await dependencies.chiefOfStaff.profile(params.data.householdId);
    if (!household) return reply.status(404).send({ error: "not_found" });
    const actor = household.members.find((member) => member.id === caller.adultId);
    if (actor?.kind !== "adult" || actor.role !== "steward" || actor.status !== "verified") {
      return reply.status(403).send({ error: "steward_required" });
    }
    const adult = household.members.find((member) => member.id === params.data.memberId);
    if (adult?.kind !== "adult") return reply.status(404).send({ error: "adult_not_found" });
    if (household.identityBoundAdultIds.includes(adult.id)) {
      return reply.status(409).send({ error: "adult_already_connected" });
    }
    const { code, challengeDigest } = dependencies.enrollmentCodes.issue({
      commandId: body.data.commandId,
      householdId: household.householdId,
      adultId: adult.id,
    });
    const expiresAt = new Date(new Date(body.data.occurredAt).getTime() + 24 * 60 * 60 * 1_000).toISOString();
    const receipt = await dependencies.chiefOfStaff.accept({
      type: "adult.enrollment.issued",
      signalId: body.data.commandId,
      householdId: household.householdId,
      idempotencyKey: `dashboard:linq-enrollment:${body.data.commandId}`,
      occurredAt: body.data.occurredAt,
      actorAdultId: caller.adultId,
      adultId: adult.id,
      challengeDigest,
      expiresAt,
    });
    reply.header("Cache-Control", "no-store");
    return reply.status(202).send({ receipt, adultId: adult.id, code, expiresAt });
  });

  app.get("/api/v1/households/:householdId/google-connections", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    if (!dependencies.googleConnection) {
      return reply.status(503).send({ error: "google_connection_not_configured" });
    }
    const params = householdParamsSchema.safeParse(request.params);
    if (!params.success) return invalidRequest(reply, params.error.issues);
    const household = await requireGoogleHousehold(
      params.data.householdId,
      caller,
      reply,
      dependencies.chiefOfStaff,
    );
    if (!household) return;
    return {
      connections: await dependencies.googleConnection.status({
        householdId: household.householdId,
        ownerAdultId: caller.adultId,
      }),
    };
  });

  app.post("/api/v1/households/:householdId/google-connections", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    if (!dependencies.googleConnection) {
      return reply.status(503).send({ error: "google_connection_not_configured" });
    }
    const params = householdParamsSchema.safeParse(request.params);
    if (!params.success) return invalidRequest(reply, params.error.issues);
    const household = await requireGoogleHousehold(
      params.data.householdId,
      caller,
      reply,
      dependencies.chiefOfStaff,
    );
    if (!household) return;
    const sessionBindingDigest = googleSessionBinding(request);
    if (!sessionBindingDigest) return reply.status(401).send({ error: "browser_session_required" });
    const result = await dependencies.googleConnection.begin({
      householdId: household.householdId,
      ownerAdultId: caller.adultId,
      sessionBindingDigest,
      now: new Date().toISOString(),
    });
    reply.header("Cache-Control", "no-store");
    return reply.status(201).send(result);
  });

  app.get<{ Querystring: { state?: string; code?: string; error?: string } }>(
    "/oauth/google/callback",
    async (request, reply) => {
      const caller = await requireAdult(request, reply, dependencies.callerResolver);
      if (!caller) return;
      if (!dependencies.googleConnection) {
        return reply.status(503).send({ error: "google_connection_not_configured" });
      }
      const sessionBindingDigest = googleSessionBinding(request);
      if (!sessionBindingDigest) return reply.status(401).send({ error: "browser_session_required" });
      if (request.query.error || !request.query.state || !request.query.code) {
        return reply.redirect("/settings?google=authorization_cancelled");
      }
      try {
        const connection = await dependencies.googleConnection.finish({
          state: request.query.state,
          code: request.query.code,
          sessionBindingDigest,
          now: new Date().toISOString(),
        });
        if (connection.ownerAdultId !== caller.adultId) {
          throw new GoogleConnectionError("Google connection owner changed", "invalid_state");
        }
        return reply.redirect("/settings?google=connected");
      } catch (error) {
        if (error instanceof GoogleConnectionError) {
          return reply.redirect(`/settings?google=${encodeURIComponent(error.code)}`);
        }
        throw error;
      }
    },
  );

  app.delete("/api/v1/households/:householdId/google-connections/:connectionId", async (request, reply) => {
    const caller = await requireAdult(request, reply, dependencies.callerResolver);
    if (!caller) return;
    if (!dependencies.googleConnection) {
      return reply.status(503).send({ error: "google_connection_not_configured" });
    }
    const params = googleConnectionParamsSchema.safeParse(request.params);
    if (!params.success) return invalidRequest(reply, params.error.issues);
    const household = await requireGoogleHousehold(
      params.data.householdId,
      caller,
      reply,
      dependencies.chiefOfStaff,
    );
    if (!household) return;
    const result = await dependencies.googleConnection.disconnect({
      connectionId: params.data.connectionId,
      householdId: household.householdId,
      ownerAdultId: caller.adultId,
      now: new Date().toISOString(),
    });
    return { connection: result.connection, providerRevocation: result.providerRevocation };
  });

  if (options.serveFrontend ?? isProduction) await registerFrontend(app, options.frontendRoot);
  return app;
}

function createDefaultGoogleConnection(
  env: NodeJS.ProcessEnv,
  repository: PostgresFlorenceRepository,
): GoogleConnection | undefined {
  const values = [env.GOOGLE_CLIENT_ID, env.GOOGLE_CLIENT_SECRET, env.GOOGLE_CREDENTIAL_KEY];
  const configuredCount = values.filter((value) => value?.trim()).length;
  if (configuredCount === 0) return undefined;
  if (configuredCount !== values.length) {
    throw new Error(
      "GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, and GOOGLE_CREDENTIAL_KEY must be configured together",
    );
  }
  if (!env.FLORENCE_WEB_BASE_URL) {
    throw new Error("FLORENCE_WEB_BASE_URL is required when Google is configured");
  }
  const redirectUri = new URL("/oauth/google/callback", env.FLORENCE_WEB_BASE_URL).toString();
  const [clientId, clientSecret, encodedKey] = values as string[];
  const encryptionKey = Buffer.from(encodedKey ?? "", "base64");
  if (encryptionKey.byteLength !== 32 || encryptionKey.toString("base64") !== encodedKey) {
    throw new Error("GOOGLE_CREDENTIAL_KEY must be a canonical base64-encoded 32-byte key");
  }
  return new GoogleConnection({
    store: repository,
    clientId: clientId ?? "",
    clientSecret: clientSecret ?? "",
    redirectUri,
    encryptionKey,
  });
}

function createDefaultLinqIngress(
  env: NodeJS.ProcessEnv,
  repository: PostgresFlorenceRepository,
  chiefOfStaff: ChiefOfStaffBoundary,
  enrollmentCodes: EnrollmentCodes | undefined,
): LinqIngress | undefined {
  const values = [
    env.LINQ_API_KEY,
    env.LINQ_WEBHOOK_SECRET,
    env.LINQ_PARTNER_ID,
    env.FLORENCE_IMAGE_VAULT_KEY,
    env.FLORENCE_ENROLLMENT_SECRET,
  ];
  const configuredCount = values.filter((value) => value?.trim()).length;
  if (configuredCount === 0) return undefined;
  if (configuredCount !== values.length) {
    throw new Error(
      "LINQ_API_KEY, LINQ_WEBHOOK_SECRET, LINQ_PARTNER_ID, FLORENCE_IMAGE_VAULT_KEY, and FLORENCE_ENROLLMENT_SECRET must be configured together",
    );
  }
  const [apiKey, signingSecret, expectedPartnerId, encodedKey] = values as string[];
  const providerReader = new LinqClient({ apiKey: apiKey ?? "" });
  const imageVault = new EncryptedImageVault({
    store: repository,
    encryptionKey: decodeImageVaultKey(encodedKey ?? ""),
    retentionMs: imageRetentionMilliseconds(env.FLORENCE_IMAGE_RETENTION_DAYS),
  });
  return createLinqIngress({
    signingSecret: signingSecret ?? "",
    expectedPartnerId: expectedPartnerId ?? "",
    authorityResolver: repository,
    providerReader,
    imageVault,
    chiefOfStaff,
    ...(enrollmentCodes ? { enrollmentCodes } : {}),
  });
}

function imageRetentionMilliseconds(value: string | undefined): number {
  if (value === undefined) return DEFAULT_IMAGE_RETENTION_MS;
  const days = Number(value);
  if (!Number.isSafeInteger(days) || days < 1 || days > 365) {
    throw new Error("FLORENCE_IMAGE_RETENTION_DAYS must be an integer from 1 through 365");
  }
  return days * 24 * 60 * 60 * 1_000;
}

function configuredWebOrigin(value: string | undefined, required: boolean): string | undefined {
  if (!value) {
    if (required) throw new Error("FLORENCE_WEB_BASE_URL is required in production");
    return undefined;
  }
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error("FLORENCE_WEB_BASE_URL must be an absolute HTTP(S) URL");
  }
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash
  ) {
    throw new Error("FLORENCE_WEB_BASE_URL must contain only an HTTP(S) origin");
  }
  return url.origin;
}

async function registerLinqWebhook(app: FastifyInstance, ingress: LinqIngress | undefined): Promise<void> {
  await app.register(async (routes) => {
    routes.removeContentTypeParser("application/json");
    routes.addContentTypeParser("application/json", { parseAs: "buffer" }, (_request, body, done) => {
      done(null, body);
    });
    routes.post<{ Body: Buffer }>(
      "/webhooks/linq",
      {
        bodyLimit: 1024 * 1024,
        config: { rateLimit: { max: 300, timeWindow: "1 minute" } },
      },
      async (request, reply) => {
        if (!ingress) return reply.status(503).send({ error: "linq_ingress_not_configured" });
        try {
          const result = await ingress.receive({
            rawBody: request.body,
            headers: request.headers,
            version: linqWebhookVersion,
          });
          return reply
            .status(result.disposition === "accepted" || result.disposition === "duplicate" ? 202 : 200)
            .send(result);
        } catch (error) {
          if (error instanceof SignalConflictError) {
            return reply.status(409).send({ error: "idempotency_conflict" });
          }
          if (error instanceof LinqIngressError) {
            const status = error.retryable ? 503 : error.code === "invalid_signature" ? 401 : 400;
            return reply.status(status).send({ error: error.code });
          }
          request.log.error({ code: "linq_ingress_unavailable" }, "Florence Linq ingress failed");
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
): Promise<Caller | null> {
  const caller = await resolver.resolve(request);
  if (!caller) reply.status(401).send({ error: "unauthorized" });
  return caller;
}

async function requireGoogleHousehold(
  householdId: string,
  caller: Caller,
  reply: FastifyReply,
  chiefOfStaff: ChiefOfStaffBoundary,
): Promise<HouseholdProfile | null> {
  if (!caller.authorizedHouseholdIds.includes(householdId)) {
    reply.status(403).send({ error: "forbidden" });
    return null;
  }
  const household = await chiefOfStaff.profile(householdId);
  if (!household) {
    reply.status(404).send({ error: "not_found" });
    return null;
  }
  const adult = household.members.find((member) => member.id === caller.adultId);
  if (
    adult?.kind !== "adult" ||
    adult.status !== "verified" ||
    !household.identityBoundAdultIds.includes(caller.adultId)
  ) {
    reply.status(403).send({ error: "verified_identity_required" });
    return null;
  }
  return household;
}

function invalidRequest(reply: FastifyReply, issues: readonly unknown[]) {
  return reply.status(400).send({ error: "invalid_request", issues });
}

function httpStatusCode(error: unknown): number | null {
  if (!error || typeof error !== "object" || !("statusCode" in error)) return null;
  const { statusCode } = error as { statusCode?: unknown };
  return typeof statusCode === "number" && Number.isInteger(statusCode) ? statusCode : null;
}

function bearerToken(request: FastifyRequest): string | null {
  const match = /^Bearer (.+)$/i.exec(request.headers.authorization ?? "");
  return match?.[1] ?? null;
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
  const session = cookieValue(request, pilotSessionCookie);
  return session ? createHash("sha256").update(`florence-google-session-v1\0${session}`).digest("hex") : null;
}

function parsePilotCredentials(encoded: string): Omit<PilotCredential, "sessionSubject">[] {
  let decoded: unknown;
  try {
    decoded = JSON.parse(encoded);
  } catch {
    throw new Error("FLORENCE_PILOT_CREDENTIALS must be valid JSON");
  }
  if (!Array.isArray(decoded) || decoded.length !== 2) {
    throw new Error("FLORENCE_PILOT_CREDENTIALS must contain exactly two adult credentials");
  }
  const credentials = decoded.map((value) => {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new Error("Each pilot credential must be an object");
    }
    const record = value as Record<string, unknown>;
    if (
      Object.keys(record).some((key) => !["token", "adultId", "householdIds"].includes(key)) ||
      typeof record.token !== "string" ||
      typeof record.adultId !== "string"
    ) {
      throw new Error("Each pilot credential must contain only token, adultId, and optional householdIds");
    }
    if (Buffer.byteLength(record.token) < 32 || !/^[\x21-\x7e]+$/.test(record.token)) {
      throw new Error("Each pilot token must be a printable secret containing at least 32 bytes");
    }
    if (!idSchema.safeParse(record.adultId).success) {
      throw new Error("Each pilot adultId must be a UUID");
    }
    if (record.householdIds !== undefined && !Array.isArray(record.householdIds)) {
      throw new Error("Pilot householdIds must be an array of UUIDs");
    }
    const householdIds = record.householdIds ?? [];
    if (householdIds.some((id) => typeof id !== "string" || !idSchema.safeParse(id).success)) {
      throw new Error("Pilot householdIds must be an array of UUIDs");
    }
    return { token: record.token, adultId: record.adultId, householdIds: new Set(householdIds) };
  });
  if (new Set(credentials.map(({ token }) => token)).size !== credentials.length) {
    throw new Error("Pilot tokens must be distinct");
  }
  if (new Set(credentials.map(({ adultId }) => adultId)).size !== credentials.length) {
    throw new Error("Pilot adultIds must be distinct");
  }
  return credentials;
}

function findPilotCredential(credentials: readonly PilotCredential[], token: string): PilotCredential | null {
  return credentials.find((credential) => safeTokenEqual(token, credential.token)) ?? null;
}

function sessionSubject(adultId: string, secret: Buffer): string {
  return createHmac("sha256", secret).update(`florence-pilot-adult\0${adultId}`).digest("base64url");
}

function issueSessionToken(subject: string, secret: Buffer): string {
  const expiresAt = Math.floor(Date.now() / 1_000) + pilotSessionLifetimeSeconds;
  const unsigned = `v1.${subject}.${expiresAt}`;
  return `${unsigned}.${createHmac("sha256", secret).update(unsigned).digest("base64url")}`;
}

function verifySession(
  token: string | null,
  credentials: readonly PilotCredential[],
  secret: Buffer | null,
): PilotCredential | null {
  if (!token || !secret) return null;
  const [version, subject, encodedExpiry, signature, extra] = token.split(".");
  if (version !== "v1" || !subject || !encodedExpiry || !signature || extra) return null;
  const unsigned = `${version}.${subject}.${encodedExpiry}`;
  const expected = createHmac("sha256", secret).update(unsigned).digest("base64url");
  if (!safeTokenEqual(signature, expected) || !/^\d+$/.test(encodedExpiry)) return null;
  const expiresAt = Number(encodedExpiry);
  if (!Number.isSafeInteger(expiresAt) || expiresAt <= Math.floor(Date.now() / 1_000)) return null;
  return credentials.find((credential) => safeTokenEqual(subject, credential.sessionSubject)) ?? null;
}

function sessionCookie(sessionToken: string, secure: boolean): string {
  return [
    `${pilotSessionCookie}=${encodeURIComponent(sessionToken)}`,
    "HttpOnly",
    "SameSite=Lax",
    "Path=/",
    `Max-Age=${pilotSessionLifetimeSeconds}`,
    ...(secure ? ["Secure"] : []),
  ].join("; ");
}

function expiredSessionCookie(secure: boolean): string {
  return [
    `${pilotSessionCookie}=`,
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
      else if (filePath.includes("/assets/"))
        reply.header("Cache-Control", "public, max-age=31536000, immutable");
      else reply.header("Cache-Control", "public, max-age=3600");
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
