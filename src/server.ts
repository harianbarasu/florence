import { createHash } from "node:crypto";
import { access, readFile } from "node:fs/promises";
import path from "node:path";
import cookie from "@fastify/cookie";
import helmet from "@fastify/helmet";
import rateLimit from "@fastify/rate-limit";
import fastifyStatic from "@fastify/static";
import Fastify, { type FastifyRequest, LogController } from "fastify";
import rawBody from "fastify-raw-body";
import { z } from "zod";
import type { GoogleCapability, GoogleConnectionProfile } from "./adapters/google/contracts.js";
import { GoogleOAuthAdapter } from "./adapters/google/oauth.js";
import { LinqClient, type LinqConfig, LinqWebhookError, unwrapLinqWebhook } from "./adapters/linq/index.js";
import { FlorenceApplication } from "./application/index.js";
import { type FlorenceConfig, loadConfig } from "./config.js";
import { createDatabase, type Database, verifyDatabase } from "./db/client.js";
import {
  type AuthenticatedSession,
  type HandoffPurpose,
  type HouseholdInvitationStepUpContext,
  HouseholdInvitationStepUpContextSchema,
  PostgresWebAuth,
  type SessionPrincipal,
} from "./modules/auth/index.js";
import { PostgresDataExporter } from "./modules/data-controls/index.js";
import { PostgresFlorenceQueries } from "./modules/queries/index.js";
import { type FamilyOnboardingProjection, PostgresFamilyOnboarding } from "./modules/relationships/index.js";
import { PostgresSourceIntelligence } from "./modules/sources/index.js";
import { randomOpaqueToken, SecretBox } from "./shared/crypto.js";
import { ConflictError, NotFoundError, StaleAuthorityError, UnauthorizedError } from "./shared/errors.js";
import type { OnboardingView, PeopleView } from "./web/api.js";

const SESSION_COOKIE_PRODUCTION = "__Host-florence_session";
const SESSION_COOKIE_DEVELOPMENT = "florence_session";
const routineFields = {
  destinationConversationId: z.string().uuid(),
  title: z.string().trim().min(1).max(200),
  sharedMeaning: z.string().trim().min(1).max(500),
  weekdays: z
    .array(z.number().int().min(1).max(7))
    .min(1)
    .max(7)
    .refine((values) => new Set(values).size === values.length, "Choose each weekday once"),
  startsOn: z.iso.date(),
  endsOn: z.iso.date().nullable().default(null),
  timeZone: z.string().trim().min(1).max(100),
  localEventTime: z.string().regex(/^(?:[01]\d|2[0-3]):[0-5]\d$/u),
  earliestUsefulMinutesBefore: z.number().int().min(0).max(43_200),
  lastResponsibleMinutesBefore: z.number().int().min(0).max(43_200),
  notificationMode: z.enum(["exceptions_only", "always", "silent"]),
  usualPersonId: z.string().uuid().nullable().default(null),
  standingSelfCoverage: z.boolean().default(false),
} as const;
const createRoutineBodySchema = z
  .strictObject(routineFields)
  .refine((input) => input.endsOn === null || input.endsOn >= input.startsOn, {
    message: "The routine cannot end before it starts",
    path: ["endsOn"],
  })
  .refine((input) => input.earliestUsefulMinutesBefore >= input.lastResponsibleMinutesBefore, {
    message: "Florence must start watching before the last responsible moment",
    path: ["earliestUsefulMinutesBefore"],
  });
const reviseRoutineBodySchema = z
  .strictObject({ ...routineFields, expectedVersion: z.number().int().positive() })
  .refine((input) => input.endsOn === null || input.endsOn >= input.startsOn, {
    message: "The routine cannot end before it starts",
    path: ["endsOn"],
  })
  .refine((input) => input.earliestUsefulMinutesBefore >= input.lastResponsibleMinutesBefore, {
    message: "Florence must start watching before the last responsible moment",
    path: ["earliestUsefulMinutesBefore"],
  });
const dependentBodySchema = z.strictObject({
  displayName: z.string().trim().min(1).max(80),
  aliases: z.array(z.string().trim().min(1).max(80)).max(12).default([]),
  birthYear: z.number().int().min(1900).max(2100).nullable().default(null),
  school: z.string().trim().max(160).default(""),
  activities: z.array(z.string().trim().min(1).max(120)).max(24).default([]),
});
const updateDependentBodySchema = z.strictObject({
  ...dependentBodySchema.shape,
  expectedRosterVersion: z.number().int().positive(),
  expectedIntakeVersion: z.number().int().nonnegative(),
});
const googleStartQuerySchema = z
  .strictObject({
    profile: z.enum(["personal_family", "work"]).default("personal_family"),
    mail: z.literal("include").optional(),
    from: z.enum(["onboarding", "sources"]).default("sources"),
  })
  .refine((query) => query.profile === "work" || query.mail === undefined, {
    message: "The work Gmail option is only valid for a work Google profile",
    path: ["mail"],
  });
export async function createServer(input?: { config?: FlorenceConfig; database?: Database }) {
  const config = input?.config ?? loadConfig();
  const database = input?.database ?? createDatabase(config, "florence-web");
  const secretBox = new SecretBox(config.security.activeDataKeyId, config.security.dataKeyringJson);
  const auth = new PostgresWebAuth(database, secretBox, config.security.tokenKey);
  const queries = new PostgresFlorenceQueries(database, secretBox, config.defaults.rawSourceRetentionDays);
  const familyOnboarding = new PostgresFamilyOnboarding(secretBox);
  const application = new FlorenceApplication(database, config, secretBox);
  const googleOAuth = new GoogleOAuthAdapter(config.google);
  const sources = new PostgresSourceIntelligence(database, secretBox, {
    rawRetentionDays: config.defaults.rawSourceRetentionDays,
    privateCandidateRetentionDays: 7,
  });
  const linqConfig: LinqConfig = {
    apiKey: config.linq.apiKey,
    baseUrl: config.linq.baseUrl,
    phoneNumber: config.linq.fromPhone,
    webhookSecret: config.linq.webhookSecret,
    requestTimeoutMs: 15_000,
    maxAttachmentBytes: 20 * 1024 * 1024,
    maxWebhookBytes: 1024 * 1024,
  };
  const linq = new LinqClient(linqConfig);
  const app = Fastify({
    logger: {
      level: config.logLevel,
      redact: {
        paths: ["req.headers.cookie", "req.headers.authorization", "request.body", "response.body"],
        censor: "[REDACTED]",
      },
    },
    logController: new LogController({ disableRequestLogging: true }),
    trustProxy: true,
    bodyLimit: 1024 * 1024,
  });

  await app.register(cookie);
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
        imgSrc: ["'self'", "data:"],
        objectSrc: ["'none'"],
        scriptSrc: ["'self'"],
        styleSrc: ["'self'", "'unsafe-inline'"],
      },
    },
    crossOriginEmbedderPolicy: false,
    hsts:
      config.environment === "production"
        ? { maxAge: 31_536_000, includeSubDomains: true, preload: true }
        : false,
    referrerPolicy: { policy: "no-referrer" },
  });
  await app.register(rawBody, {
    global: false,
    encoding: false,
    runFirst: true,
    routes: ["/webhooks/linq"],
  });

  const publicRoot = path.resolve("dist/public");
  if (await exists(publicRoot)) {
    await app.register(fastifyStatic, {
      root: publicRoot,
      index: false,
      wildcard: false,
      maxAge: config.environment === "production" ? "1h" : 0,
    });
  }
  const indexTemplate = await readTextOrFallback(
    path.join(publicRoot, "index.html"),
    '<!doctype html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Florence</title></head><body><div id="root">Florence is starting.</div></body></html>',
  );

  app.get("/healthz", async () => ({ ok: true, service: "florence-web" }));
  app.get("/readyz", async (_request, reply) => {
    try {
      await verifyDatabase(database);
      const migrations = await database<{ count: number | string }[]>`
        select count(*) as count from schema_migrations
      `;
      if (Number(migrations[0]?.count ?? 0) < 1) throw new Error("Schema migrations are missing");
      return { ok: true, database: true, schema: config.database.schema };
    } catch {
      return reply.code(503).send({ ok: false, database: false });
    }
  });

  app.post(
    "/webhooks/linq",
    { config: { rawBody: true, rateLimit: { max: 300, timeWindow: "1 minute" } } },
    async (request, reply) => {
      const raw = request.rawBody;
      if (!raw) return reply.code(400).send({ error: "Missing signed webhook body" });
      const event = unwrapLinqWebhook({
        rawBody: raw,
        headers: headersForVerification(request.headers),
        webhookSecret: linqConfig.webhookSecret,
        receivedAt: new Date(),
        maxBodyBytes: linqConfig.maxWebhookBytes,
      });
      const liveChat =
        event.eventType === "linq.ignored" ? null : await linq.getChat(event.channel.providerChatId);
      const receipt = await application.process({ kind: "linq.webhook", event, liveChat });
      return reply.code(receipt.duplicate ? 200 : 202).send({ received: true });
    },
  );

  app.get(
    "/handoff/:token",
    { config: { rateLimit: { max: 30, timeWindow: "1 minute" } } },
    async (request, reply) => {
      const token = z
        .string()
        .regex(/^[A-Za-z0-9_-]{32,128}$/u)
        .parse((request.params as { token?: unknown }).token);
      reply.header("Cache-Control", "no-store, max-age=0");
      let preview: Awaited<ReturnType<PostgresWebAuth["previewHandoff"]>>;
      try {
        preview = await auth.previewHandoff(token);
      } catch (error) {
        if (
          error instanceof NotFoundError ||
          error instanceof ConflictError ||
          error instanceof UnauthorizedError
        ) {
          return reply
            .code(410)
            .type("text/html; charset=utf-8")
            .send(unavailableHandoffPage(config.linq.fromPhone));
        }
        throw error;
      }
      return reply
        .header("Referrer-Policy", "no-referrer")
        .type("text/html; charset=utf-8")
        .send(handoffPage(token, preview.purpose));
    },
  );

  app.post(
    "/auth/consume",
    { config: { rateLimit: { max: 20, timeWindow: "10 minutes" } } },
    async (request, reply) => {
      verifySameOrigin(request, config);
      const body = z
        .strictObject({ token: z.string().regex(/^[A-Za-z0-9_-]{32,128}$/u) })
        .parse(request.body);
      const preview = await auth.previewHandoff(body.token);
      const session = await auth.consumeHandoff(body.token);
      reply.setCookie(sessionCookieName(config), session.sessionToken, {
        path: "/",
        httpOnly: true,
        secure: config.environment === "production",
        sameSite: "lax",
        expires: session.absoluteExpiresAt,
      });
      reply.header("Cache-Control", "no-store");
      return {
        redirect: completedHandoffRedirect(preview.purpose, session),
      };
    },
  );

  app.post("/auth/logout", async (request, reply) => {
    verifySameOrigin(request, config);
    const principal = await requireSession(request, config, auth);
    auth.verifyCsrf(principal, headerString(request.headers["x-csrf-token"]));
    await auth.revokeSession(principal.sessionId, principal.personId);
    reply.clearCookie(sessionCookieName(config), { path: "/" });
    return { ok: true };
  });

  app.get("/oauth/google/start", async (request, reply) => {
    const query = googleStartQuerySchema.parse(request.query);
    const principal = await requireGoogleStartSession(request, config, auth, query.from);
    const requestedCapabilities = googleCapabilitiesForProfile(query.profile, query.mail === "include");
    const pkce = googleOAuth.createPkce();
    const state = randomOpaqueToken(32);
    await application.process({
      kind: "google.oauth.begin",
      personId: principal.personId,
      initiatingSessionId: principal.sessionId,
      stateDigest: sha256Hex(state),
      pkceVerifier: pkce.verifier,
      returnPath: query.from === "onboarding" ? "/onboarding" : "/sources",
      requestedCapabilities,
      accountKind: query.profile,
      expectedPersonControlEpoch: principal.controlEpoch,
      expiresAt: new Date(Date.now() + 10 * 60_000).toISOString(),
      createdAt: new Date().toISOString(),
    });
    return reply.redirect(
      googleOAuth.authorizationUrl({
        state,
        challenge: pkce.challenge,
        requestedCapabilities,
      }),
    );
  });

  app.get("/oauth/google/callback", async (request, reply) => {
    const query = z
      .object({
        code: z.string().min(1).optional(),
        state: z.string().min(32).max(256),
        error: z.string().trim().min(1).max(200).optional(),
      })
      .passthrough()
      .parse(request.query);
    const principal = await requireSession(request, config, auth);
    const attempt = await sources.read({
      kind: "oauth_attempt_access",
      provider: "google",
      stateDigest: sha256Hex(query.state),
      asOf: new Date().toISOString(),
    });
    if (attempt.kind !== "oauth_attempt_access" || attempt.provider !== "google") {
      throw new Error("Google OAuth state did not resolve");
    }
    if (
      attempt.initiatingSessionId !== principal.sessionId ||
      attempt.personId !== principal.personId ||
      attempt.personControlEpoch !== principal.controlEpoch
    ) {
      throw new UnauthorizedError("Google authorization must finish in the browser that started it");
    }
    if (query.error) {
      if (query.error === "access_denied") return reply.redirect(`${attempt.returnPath}?google=cancelled`);
      throw new UnauthorizedError("Google authorization was not completed");
    }
    if (!query.code) throw new UnauthorizedError("Google did not return an authorization code");
    const exchange = await googleOAuth.exchange(
      query.code,
      attempt.pkceVerifier,
      attempt.requestedCapabilities,
    );
    const completed = await application.process({
      kind: "google.oauth.complete",
      stateDigest: sha256Hex(query.state),
      externalSubjectDigest: sha256Hex(exchange.subject),
      credentials: JSON.parse(
        JSON.stringify({
          ...exchange.credentials,
          accountEmail: exchange.email,
          grantedScopes: [...exchange.grantedScopes],
        }),
      ),
      grantedCapabilities: exchange.grantedCapabilities,
      completedAt: new Date().toISOString(),
    });
    if (!completed.accepted) throw new Error("Google integration was not connected");
    return reply.redirect(`${attempt.returnPath}?connected=1`);
  });

  app.get("/api/me", async (request, reply) => {
    const principal = await requireSession(request, config, auth);
    reply.header("Cache-Control", "no-store");
    return queries.viewer(principal.personId, principal.csrfToken, {
      assuranceKind: principal.assuranceKind,
      assuranceExpiresAt: principal.assuranceExpiresAt?.toISOString() ?? null,
    });
  });
  app.get("/api/onboarding", async (request, reply) => {
    const principal = await requireSession(request, config, auth);
    const projection = await database.begin(
      "isolation level repeatable read read only",
      async (transaction) =>
        familyOnboarding.project(transaction, {
          actorPersonId: principal.personId,
          personId: principal.personId,
        }),
    );
    const [people, intakeCompletion, google] = await Promise.all([
      queries.people(principal.personId),
      readHouseholdIntakeCompletion(
        database,
        projection.householdChoices.map((choice) => choice.householdId),
      ),
      readOnboardingGoogle(database, sources, principal.personId, projection),
    ]);
    reply.header("Cache-Control", "private, no-store, max-age=0");
    return projectOnboardingView(projection, people, intakeCompletion, google);
  });
  app.post("/api/onboarding/profile", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const body = z
      .strictObject({
        displayName: z.string().trim().min(1).max(80),
        timeZone: z.string().trim().min(1).max(100),
      })
      .parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "confirm_onboarding_profile", ...body },
    });
  });
  app.post("/api/onboarding/select-household", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const body = z.strictObject({ householdId: z.string().uuid() }).parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "select_onboarding_household", householdId: body.householdId },
    });
  });
  app.post("/api/onboarding/adults", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const body = z
      .strictObject({
        householdId: z.string().uuid(),
        expectedMembershipVersion: z.number().int().positive(),
        expectedIntakeVersion: z.number().int().nonnegative(),
        adults: z
          .array(
            z.strictObject({
              id: z.string().uuid().optional(),
              displayName: z.string().trim().min(1).max(80),
              role: z.enum(["steward", "caregiver"]),
            }),
          )
          .max(16),
      })
      .parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: {
        kind: "save_onboarding_adult_roster",
        ...body,
      },
    });
  });
  app.post("/api/onboarding/adults/:intentId/invite", async (request) => {
    const intentId = z
      .string()
      .uuid()
      .parse((request.params as { intentId?: unknown }).intentId);
    const body = z
      .strictObject({
        householdId: z.string().uuid(),
        conversationId: z.string().uuid(),
        expectedParticipantEpochId: z.string().uuid(),
        expectedParticipantDigest: z.string().regex(/^[a-f0-9]{64}$/u),
        inviteeIdentityId: z.string().uuid(),
        inviteePersonId: z.string().uuid(),
        expectedIntentVersion: z.number().int().positive(),
      })
      .parse(request.body);
    const principal = await requireWriteSession(request, config, auth);
    const projection = await database.begin(
      "isolation level repeatable read read only",
      async (transaction) =>
        familyOnboarding.project(transaction, {
          actorPersonId: principal.personId,
          personId: principal.personId,
        }),
    );
    const household = projection.household;
    if (!household || household.householdId !== body.householdId) {
      throw new UnauthorizedError("That family is no longer available for onboarding");
    }
    const adult = household.adults.find((candidate) => candidate.id === intentId);
    if (!adult) throw new NotFoundError("That family adult is no longer part of setup");
    if (adult.version !== body.expectedIntentVersion) {
      throw new ConflictError("That family adult changed before the invitation was prepared");
    }
    if (adult.role === "steward") {
      verifyExactStepUp(principal, "household_invitation", {
        action: "invite",
        householdId: body.householdId,
        onboardingAdultIntentId: intentId,
        onboardingAdultIntentVersion: String(body.expectedIntentVersion),
        conversationId: body.conversationId,
        expectedParticipantEpochId: body.expectedParticipantEpochId,
        expectedParticipantDigest: body.expectedParticipantDigest,
        inviteeIdentityId: body.inviteeIdentityId,
        inviteePersonId: body.inviteePersonId,
        proposedDisplayName: adult.displayName,
        role: "steward",
      });
    }
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: {
        kind: "invite_household_participant",
        ...body,
        proposedDisplayName: adult.displayName,
        role: adult.role,
        onboardingAdultIntentId: intentId,
        onboardingAdultIntentVersion: body.expectedIntentVersion,
      },
    });
  });
  app.post("/api/onboarding/children-reviewed", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const body = z
      .strictObject({
        householdId: z.string().uuid(),
        expectedMembershipVersion: z.number().int().positive(),
        expectedIntakeVersion: z.number().int().positive(),
      })
      .parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "mark_onboarding_children_reviewed", ...body },
    });
  });
  app.post("/api/onboarding/shared-review", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const body = z
      .strictObject({
        householdId: z.string().uuid(),
        expectedMembershipVersion: z.number().int().positive(),
        expectedIntakeVersion: z.number().int().positive(),
        expectedMembershipOnboardingVersion: z.number().int().nonnegative(),
      })
      .parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "review_onboarding_shared_context", ...body },
    });
  });
  app.post("/api/onboarding/google-skip", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    z.strictObject({}).parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "skip_onboarding_google" },
    });
  });
  app.post("/api/onboarding/complete", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const body = z
      .strictObject({
        householdId: z.string().uuid(),
        expectedMembershipVersion: z.number().int().positive(),
        expectedIntakeVersion: z.number().int().nonnegative(),
        expectedMembershipOnboardingVersion: z.number().int().nonnegative(),
        expectedProfileReviewVersion: z.number().int().positive(),
      })
      .parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "complete_onboarding", ...body },
    });
  });
  app.get("/api/home", async (request, reply) => {
    const principal = await requireSession(request, config, auth);
    await requireCompletedOnboarding(database, principal.personId);
    reply.header("Cache-Control", "private, no-store, max-age=0");
    return queries.home(principal.personId);
  });
  app.get("/api/people", async (request, reply) => {
    const principal = await requireSession(request, config, auth);
    await requireCompletedOnboarding(database, principal.personId);
    const [people, projection] = await Promise.all([
      queries.people(principal.personId),
      database.begin("isolation level repeatable read read only", async (transaction) =>
        familyOnboarding.project(transaction, {
          actorPersonId: principal.personId,
          personId: principal.personId,
        }),
      ),
    ]);
    reply.header("Cache-Control", "private, no-store, max-age=0");
    return projectPeopleAdultIntents(people, projection);
  });
  app.get("/api/chats", async (request) => {
    const principal = await requireSession(request, config, auth);
    await requireCompletedOnboarding(database, principal.personId);
    return queries.chats(principal.personId);
  });
  app.get("/api/sources", async (request, reply) => {
    const principal = await requireSession(request, config, auth);
    await requireCompletedOnboarding(database, principal.personId);
    reply.header("Cache-Control", "private, no-store, max-age=0");
    return queries.sources(principal.personId);
  });
  app.get("/api/routines", async (request, reply) => {
    const principal = await requireSession(request, config, auth);
    await requireCompletedOnboarding(database, principal.personId);
    reply.header("Cache-Control", "private, no-store, max-age=0");
    return queries.routines(principal.personId);
  });
  app.get("/api/safety", async (request, reply) => {
    const principal = await requireSession(request, config, auth);
    reply.header("Cache-Control", "private, no-store, max-age=0");
    return queries.safety(principal.personId, principal.sessionId);
  });

  app.post("/api/households", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "create_household" },
    });
  });
  app.post("/api/households/:householdId/invitations", async (request) => {
    const householdId = z
      .string()
      .uuid()
      .parse((request.params as { householdId?: unknown }).householdId);
    const body = z
      .strictObject({
        conversationId: z.string().uuid(),
        expectedParticipantEpochId: z.string().uuid(),
        expectedParticipantDigest: z.string().regex(/^[a-f0-9]{64}$/u),
        inviteeIdentityId: z.string().uuid(),
        inviteePersonId: z.string().uuid(),
        proposedDisplayName: z.string().trim().min(1).max(80),
        role: z.enum(["steward", "caregiver", "participant"]),
      })
      .parse(request.body);
    const principal =
      body.role === "steward"
        ? await requireExactStepUpWriteSession(request, config, auth, "household_invitation", {
            action: "invite",
            householdId,
            conversationId: body.conversationId,
            expectedParticipantEpochId: body.expectedParticipantEpochId,
            expectedParticipantDigest: body.expectedParticipantDigest,
            inviteeIdentityId: body.inviteeIdentityId,
            inviteePersonId: body.inviteePersonId,
            proposedDisplayName: body.proposedDisplayName,
            role: "steward",
            onboardingAdultIntentId: "",
            onboardingAdultIntentVersion: "",
          })
        : await requireWriteSession(request, config, auth);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "invite_household_participant", householdId, ...body },
    });
  });
  app.post("/api/invitations/:invitationId/approve", async (request) => {
    const invitationId = z
      .string()
      .uuid()
      .parse((request.params as { invitationId?: unknown }).invitationId);
    const principal = await requireWriteSession(request, config, auth);
    const invitation = await database<{ household_id: string; requested_role: string }[]>`
      select household_id, requested_role from invitations where id = ${invitationId}
    `;
    if (invitation[0]?.requested_role === "steward") {
      verifyExactStepUp(principal, "household_invitation", {
        action: "approve",
        householdId: invitation[0].household_id,
        invitationId,
      });
    }
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "approve_household_invitation", invitationId },
    });
  });
  app.post("/api/invitations/:invitationId/accept", async (request, reply) => {
    const invitationId = z
      .string()
      .uuid()
      .parse((request.params as { invitationId?: unknown }).invitationId);
    const principal = await requireWriteSession(request, config, auth);
    const invitation = await database<{ household_id: string }[]>`
      select household_id from invitations where id = ${invitationId}
    `;
    if (!invitation[0]) throw new NotFoundError("Family invitation does not exist");
    verifyExactStepUp(principal, "household_invitation", {
      action: "accept",
      householdId: invitation[0].household_id,
      invitationId,
    });
    const receipt = await application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "accept_household_invitation", invitationId },
    });
    if (receipt.disposition === "household_invitation_stale") {
      return reply.code(409).send({
        error:
          "This family invitation is no longer current because the group or family changed. Ask for a fresh introduction.",
      });
    }
    return receipt;
  });
  app.post("/api/households/:householdId/dependents", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const householdId = z
      .string()
      .uuid()
      .parse((request.params as { householdId?: unknown }).householdId);
    const body = dependentBodySchema.parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "add_dependent", householdId, ...body },
    });
  });
  app.post("/api/households/:householdId/dependents/:dependentPersonId", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const params = z
      .strictObject({ householdId: z.string().uuid(), dependentPersonId: z.string().uuid() })
      .parse(request.params);
    const body = updateDependentBodySchema.parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "update_dependent", ...params, ...body },
    });
  });
  app.post("/api/routines", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const body = createRoutineBodySchema.parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "create_routine", ...body },
    });
  });
  app.post("/api/routines/:routineId/revisions", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const routineId = z
      .string()
      .uuid()
      .parse((request.params as { routineId?: unknown }).routineId);
    const body = reviseRoutineBodySchema.parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "revise_routine", routineId, ...body },
    });
  });
  app.post("/api/routines/:routineId/status", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const routineId = z
      .string()
      .uuid()
      .parse((request.params as { routineId?: unknown }).routineId);
    const body = z
      .strictObject({
        expectedVersion: z.number().int().positive(),
        status: z.enum(["active", "paused", "retired"]),
      })
      .parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "set_routine_status", routineId, ...body },
    });
  });
  app.post("/api/sources/calendar-mode", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const body = z
      .strictObject({
        connectionId: z.string().uuid(),
        calendarId: z.string().min(1).max(2_000),
        mode: z.enum(["full_private", "availability_only", "off"]),
      })
      .parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: {
        kind: "set_calendar_mode",
        integrationId: body.connectionId,
        calendarId: body.calendarId,
        mode: body.mode,
      },
    });
  });
  app.post("/api/sources/private-reviews/:candidateId", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const candidateId = z
      .string()
      .uuid()
      .parse((request.params as { candidateId?: unknown }).candidateId);
    const body = z.strictObject({ decision: z.enum(["accepted", "rejected"]) }).parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: {
        kind: "review_private_candidate",
        candidateId,
        decision: body.decision,
      },
    });
  });
  app.post("/api/sources/private-reviews/:candidateId/prepare-share", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const candidateId = z
      .string()
      .uuid()
      .parse((request.params as { candidateId?: unknown }).candidateId);
    const body = z.strictObject({ conversationId: z.string().uuid() }).parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: {
        kind: "prepare_private_bridge",
        candidateId,
        conversationId: body.conversationId,
      },
    });
  });
  app.post("/api/sources/private-bridge/:actionIntentId/approve", async (request) => {
    const actionIntentId = z
      .string()
      .uuid()
      .parse((request.params as { actionIntentId?: unknown }).actionIntentId);
    const digest = z.string().regex(/^[a-f0-9]{64}$/u);
    const body = z
      .strictObject({
        actionDigest: digest,
        dataDigest: digest,
        policyDigest: digest,
        targetDigest: digest,
        mode: z.enum(["once", "standing"]),
      })
      .parse(request.body);
    const exactStandingContext = {
      action: "approve",
      actionIntentId,
      actionDigest: body.actionDigest,
      dataDigest: body.dataDigest,
      policyDigest: body.policyDigest,
      targetDigest: body.targetDigest,
      mode: body.mode,
    } as const;
    const principal =
      body.mode === "standing"
        ? await requireExactStepUpWriteSession(
            request,
            config,
            auth,
            "private_bridge_standing",
            exactStandingContext,
          )
        : await requireWriteSession(request, config, auth);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "approve_private_bridge", actionIntentId, ...body },
    });
  });
  app.post("/api/sources/memories/:memoryId/forget", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const memoryId = z
      .string()
      .uuid()
      .parse((request.params as { memoryId?: unknown }).memoryId);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "forget_memory", memoryId },
    });
  });
  app.post("/api/sources/bridge-rules/:ruleId/revoke", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const ruleId = z
      .string()
      .uuid()
      .parse((request.params as { ruleId?: unknown }).ruleId);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "revoke_bridge_rule", ruleId },
    });
  });
  app.post("/api/safety/pause", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const body = z.strictObject({ paused: z.boolean() }).parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "pause_person", paused: body.paused },
    });
  });
  app.post("/api/safety/request-step-up", async (request) => {
    const principal = await requireWriteSession(request, config, auth);
    const body = z
      .discriminatedUnion("purpose", [
        z.strictObject({ purpose: z.literal("account_controls") }),
        z.strictObject({
          purpose: z.literal("google_connect"),
          context: z
            .strictObject({
              profile: z.enum(["personal_family", "work"]),
              mail: z.literal("include").optional(),
              returnPath: z.literal("/onboarding").optional(),
            })
            .refine((context) => context.profile === "work" || context.mail === undefined, {
              message: "The work Gmail option is only valid for a work Google profile",
              path: ["mail"],
            })
            .optional(),
        }),
        z.strictObject({
          purpose: z.literal("household_invitation"),
          context: HouseholdInvitationStepUpContextSchema,
        }),
        z.strictObject({
          purpose: z.literal("private_bridge_standing"),
          context: z.strictObject({
            action: z.literal("approve"),
            actionIntentId: z.string().uuid(),
            actionDigest: z.string().regex(/^[a-f0-9]{64}$/u),
            dataDigest: z.string().regex(/^[a-f0-9]{64}$/u),
            policyDigest: z.string().regex(/^[a-f0-9]{64}$/u),
            targetDigest: z.string().regex(/^[a-f0-9]{64}$/u),
            mode: z.literal("standing"),
          }),
        }),
      ])
      .parse(request.body);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: {
        kind: "request_step_up",
        purpose: body.purpose,
        context: compactStringContext("context" in body ? body.context : undefined),
      },
    });
  });
  app.get("/api/pending-action", async (request, reply) => {
    const principal = await requireSession(request, config, auth);
    const action = requirePendingHouseholdInvitationAction(principal);
    reply.header("Cache-Control", "private, no-store, max-age=0");
    if (action.action === "invite") {
      return {
        purpose: "household_invitation" as const,
        action: action.action,
        title: `Invite ${action.proposedDisplayName} as a parent?`,
        detail:
          "Florence will send a private family invitation. They will still confirm their own identity and choose whether to join.",
        confirmLabel: "Send invitation",
        cancelPath: "/people",
      };
    }
    const people = await queries.people(principal.personId);
    const invitation = people.invitations.find(
      (candidate) => candidate.id === action.invitationId && candidate.action === action.action,
    );
    if (!invitation || invitation.householdId !== action.householdId || !invitation.canAct) {
      throw new ConflictError("This family invitation is no longer current");
    }
    const accepting = action.action === "accept";
    return {
      purpose: "household_invitation" as const,
      action: action.action,
      title: accepting
        ? `Join ${invitation.personName}’s family?`
        : `Approve ${invitation.personName} as ${familyRoleLabel(invitation.role)}?`,
      detail: accepting
        ? "Florence will add you to this family after you confirm. Your private accounts and sources remain yours."
        : "This is your exact approval for this person to join as a co-steward.",
      confirmLabel: accepting ? "Join family" : "Approve invitation",
      cancelPath: "/people",
    };
  });
  app.post("/api/pending-action/confirm", async (request, reply) => {
    const principal = await requireWriteSession(request, config, auth);
    z.strictObject({}).parse(request.body);
    const action = requirePendingHouseholdInvitationAction(principal);
    verifyExactStepUp(principal, "household_invitation", action);
    const receipt = await confirmHouseholdInvitationAction({
      action,
      actorPersonId: principal.personId,
      application,
      database,
    });
    if (receipt.disposition === "household_invitation_stale") {
      return reply.code(409).send({
        error:
          "This family invitation is no longer current because the group or family changed. Ask for a fresh introduction.",
      });
    }
    return {
      receipt,
      redirect: "/people",
    };
  });
  app.get("/api/safety/export", async (request, reply) => {
    const principal = await requireStepUpSession(request, config, auth, "account_controls");
    const exported = await new PostgresDataExporter(
      database,
      secretBox,
      config.defaults.rawSourceRetentionDays,
    ).exportPerson(principal.personId);
    reply.header("Cache-Control", "no-store, max-age=0");
    reply.header(
      "Content-Disposition",
      `attachment; filename="florence-export-${new Date().toISOString().slice(0, 10)}.json"`,
    );
    return reply.type("application/json; charset=utf-8").send(JSON.stringify(exported, null, 2));
  });
  app.post("/api/sources/:integrationId/disconnect", async (request) => {
    const principal = await requireStepUpWriteSession(request, config, auth, "account_controls");
    const integrationId = z
      .string()
      .uuid()
      .parse((request.params as { integrationId?: unknown }).integrationId);
    return application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "disconnect_integration", integrationId },
    });
  });
  app.post("/api/safety/sessions/:sessionId/revoke", async (request, reply) => {
    const principal = await requireStepUpWriteSession(request, config, auth, "account_controls");
    const sessionId = z
      .string()
      .uuid()
      .parse((request.params as { sessionId?: unknown }).sessionId);
    const receipt = await application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "revoke_session", sessionId },
    });
    if (sessionId === principal.sessionId) reply.clearCookie(sessionCookieName(config), { path: "/" });
    return receipt;
  });
  app.post("/api/safety/delete-person", async (request, reply) => {
    const principal = await requireStepUpWriteSession(request, config, auth, "account_controls");
    const receipt = await application.process({
      kind: "web.command",
      actorPersonId: principal.personId,
      command: { kind: "delete_person" },
    });
    reply.clearCookie(sessionCookieName(config), { path: "/" });
    return receipt;
  });

  app.get("/privacy", async (_request, reply) =>
    reply
      .type("text/html; charset=utf-8")
      .send(
        policyPage("Privacy", [
          "Florence treats every person, private source, and exact group-chat participant epoch as a separate permission boundary.",
          "From the moment Florence is added to a group, permitted new messages and attachments may be encrypted inside that exact participant epoch. Florence stays completely silent unless every current person is a registered active member of the same Florence family and their applicable settings permit writing.",
          "Each registered exact-chat participant receives an independent private view governed by their own settings. Observed group context is never automatically widened to a household, another chat, or another participant.",
          "Google accounts are private to the person who connects them. Sharing family meaning requires an explicit one-time approval or a narrow standing rule.",
          "You can pause, narrow, export, disconnect, correct, forget, or request deletion from the private control plane.",
        ]),
      ),
  );
  app.get("/terms", async (_request, reply) =>
    reply
      .type("text/html; charset=utf-8")
      .send(
        policyPage("Terms", [
          "Florence coordinates information and acknowledged coverage; it does not guarantee physical pickup, medical care, emergency response, or professional advice.",
          "Do not rely on Florence as an emergency service. Confirm consequential arrangements directly with the responsible people.",
        ]),
      ),
  );

  app.setErrorHandler((error, request, reply) => {
    const status =
      error instanceof UnauthorizedError
        ? 401
        : error instanceof NotFoundError
          ? 404
          : error instanceof ConflictError || error instanceof StaleAuthorityError
            ? 409
            : error instanceof z.ZodError || error instanceof LinqWebhookError
              ? 400
              : 500;
    if (status >= 500) {
      request.log.error(
        { errorCode: "internal_server_error", route: request.routeOptions.url },
        "request failed",
      );
    }
    const message =
      status >= 500 && config.environment === "production"
        ? "Florence could not complete that request."
        : error instanceof Error
          ? error.message
          : "Unknown request failure";
    return reply.code(status).send({ error: message });
  });

  app.setNotFoundHandler(async (request, reply) => {
    if (request.method !== "GET" || !headerString(request.headers.accept)?.includes("text/html")) {
      return reply.code(404).send({ error: "Not found" });
    }
    const html = indexTemplate.replace(
      "<html",
      `<html data-florence-phone="${escapeHtml(config.linq.fromPhone)}"`,
    );
    return reply.header("Cache-Control", "no-store").type("text/html; charset=utf-8").send(html);
  });

  app.addHook("onClose", async () => {
    if (!input?.database) await database.end();
  });
  return app;
}

async function main(): Promise<void> {
  const config = loadConfig();
  const app = await createServer({ config });
  await app.listen({ host: "0.0.0.0", port: config.port });
  let closing = false;
  const shutdown = async () => {
    if (closing) return;
    closing = true;
    try {
      await app.close();
    } catch {
      process.stderr.write(`${JSON.stringify({ level: "error", event: "graceful_shutdown_failed" })}\n`);
      process.exitCode = 1;
    }
  };
  process.once("SIGTERM", () => {
    void shutdown();
  });
  process.once("SIGINT", () => {
    void shutdown();
  });
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(new URL(import.meta.url).pathname)) {
  void main().catch(() => {
    process.stderr.write(`${JSON.stringify({ level: "fatal", errorCode: "web_startup_failed" })}\n`);
    process.exitCode = 1;
  });
}

async function readHouseholdIntakeCompletion(
  database: Database,
  householdIds: readonly string[],
): Promise<ReadonlyMap<string, boolean>> {
  if (householdIds.length === 0) return new Map();
  const rows = await database<{ readonly household_id: string; readonly completed: boolean }[]>`
    select household_id, child_roster_reviewed_at is not null as completed
    from household_onboarding_intakes
    where household_id = any(${database.array([...householdIds])}::uuid[])
  `;
  return new Map(rows.map((row) => [row.household_id, row.completed] as const));
}

async function readOnboardingGoogle(
  database: Database,
  sources: PostgresSourceIntelligence,
  personId: string,
  projection: FamilyOnboardingProjection,
): Promise<OnboardingView["google"]> {
  const rows = await database<
    {
      readonly id: string;
      readonly status: string;
      readonly control_epoch: number | string;
    }[]
  >`
    select id, status, control_epoch
    from integrations
    where person_id = ${personId} and provider = 'google'
      and account_kind = 'personal_family' and status <> 'revoked'
    order by connected_at desc, id desc
    limit 1
  `;
  const connection = rows[0];
  let accountEmail: string | null = null;
  if (connection) {
    const profile = await sources
      .read({
        kind: "integration_profile",
        integrationId: connection.id,
        personId,
        expectedControlEpoch: Number(connection.control_epoch),
      })
      .catch(() => null);
    if (profile?.kind === "integration_profile") accountEmail = profile.accountEmail;
  }
  const decision =
    projection.household?.googleDecision === "connected"
      ? "connected"
      : projection.household?.googleDecision === "limited"
        ? "skipped"
        : "undecided";
  return {
    decision,
    accountEmail,
    status: connection?.status ?? null,
  };
}

function projectOnboardingView(
  projection: FamilyOnboardingProjection,
  people: PeopleView,
  intakeCompletion: ReadonlyMap<string, boolean>,
  google: OnboardingView["google"],
): OnboardingView {
  const selectedPeople = projection.household
    ? people.households.find((household) => household.id === projection.household?.householdId)
    : undefined;
  const branch = onboardingBranch(projection);
  const step = projection.nextStep.kind;
  const household = projection.household;
  const mayReadSharedIntake =
    household?.role === "steward" ||
    household?.sharedContextReviewed === true ||
    step === "review_shared_context";
  if (household && !selectedPeople) {
    throw new UnauthorizedError("That family is no longer available for onboarding");
  }
  return {
    completed: household?.completed ?? false,
    branch,
    step,
    progress: onboardingProgress(branch, step),
    person: {
      name: projection.profile.displayName ?? "",
      timeZone: projection.profile.timezone ?? "America/Los_Angeles",
      profileReviewed: projection.profile.reviewVersion > 0,
      profileReviewVersion: projection.profile.reviewVersion,
    },
    household:
      household && selectedPeople
        ? {
            id: household.householdId,
            name: selectedPeople.name,
            versions: {
              membership: household.membershipVersion,
              roster: selectedPeople.rosterVersion,
              intake: household.intakeVersion,
              membershipOnboarding: household.membershipOnboardingVersion,
            },
            sharedIntakeComplete: household.childRosterReviewed,
            adultRosterReviewed: household.adultRosterReviewed,
            adults: mayReadSharedIntake
              ? household.adults.map((adult) => ({
                  id: adult.id,
                  version: adult.version,
                  displayName: adult.displayName,
                  role: adult.role,
                  matchedPersonId: adult.matchedPersonId,
                  invitationId: adult.invitationId,
                  progress: onboardingAdultProgress(adult, people),
                }))
              : [],
            children: mayReadSharedIntake
              ? household.children.map((child) => ({
                  id: child.personId,
                  name: child.displayName,
                  aliases: [...child.aliases],
                  birthYear: child.birthYear,
                  school: child.school || null,
                  activities: [...child.activities],
                }))
              : [],
          }
        : null,
    householdChoices: projection.householdChoices.map((choice) => {
      const matchingPeople = people.households.find(
        (householdChoice) => householdChoice.id === choice.householdId,
      );
      return {
        id: choice.householdId,
        name: matchingPeople?.name ?? "Your family",
        role: choice.role,
        sharedIntakeComplete: intakeCompletion.get(choice.householdId) ?? false,
      };
    }),
    google,
    eligibleInvitees: (selectedPeople?.eligibleParticipants ?? []).map((participant) => ({
      personId: participant.personId,
      identityId: participant.identityId,
      conversationId: participant.conversationId,
      participantEpochId: participant.participantEpochId,
      participantDigest: participant.participantDigest,
      name: participant.name,
      registered: participant.registered,
    })),
  };
}

export function projectPeopleAdultIntents(
  people: PeopleView,
  projection: FamilyOnboardingProjection,
): PeopleView {
  const selected = projection.household;
  if (!selected) return people;
  return {
    ...people,
    households: people.households.map((household) =>
      household.id === selected.householdId && household.canInvite
        ? {
            ...household,
            plannedAdults: selected.adults.map((adult) => ({
              id: adult.id,
              version: adult.version,
              displayName: adult.displayName,
              role: adult.role,
              matchedPersonId: adult.matchedPersonId,
              progress: onboardingAdultProgress(adult, people),
            })),
          }
        : household,
    ),
  };
}

function onboardingBranch(projection: FamilyOnboardingProjection): OnboardingView["branch"] {
  const selected = projection.household;
  if (selected?.role !== "steward") return selected ? "caregiver" : "starter";
  const anotherPersonAddedSharedContext =
    selected.childRosterReviewedByPersonId !== null &&
    selected.childRosterReviewedByPersonId !== projection.personId;
  return anotherPersonAddedSharedContext ? "invited_adult" : "starter";
}

function onboardingAdultProgress(
  adult: NonNullable<FamilyOnboardingProjection["household"]>["adults"][number],
  people: PeopleView,
): NonNullable<OnboardingView["household"]>["adults"][number]["progress"] {
  if (adult.status === "joined") return "joined";
  if (adult.status === "not_invited") return "not_connected";
  const invitation = people.invitations.find((candidate) => candidate.id === adult.invitationId);
  if (adult.role === "steward" && invitation?.action === "approve") {
    return "awaiting_steward_approval";
  }
  return "awaiting_acceptance";
}

function onboardingProgress(
  branch: OnboardingView["branch"],
  step: OnboardingView["step"],
): OnboardingView["progress"] {
  if (branch === "invited_adult" || branch === "caregiver") {
    const current =
      step === "confirm_profile"
        ? 1
        : step === "google"
          ? 3
          : step === "review" || step === "complete"
            ? 4
            : 2;
    return { current, total: 4 };
  }
  const current =
    step === "confirm_profile"
      ? 1
      : step === "create_household" || step === "choose_household" || step === "adults"
        ? 2
        : step === "children"
          ? 3
          : step === "review_shared_context"
            ? 4
            : step === "google"
              ? 4
              : 5;
  return { current, total: 5 };
}

function requirePendingHouseholdInvitationAction(
  principal: SessionPrincipal,
): HouseholdInvitationStepUpContext {
  if (
    principal.assuranceKind !== "household_invitation" ||
    principal.assuranceExpiresAt === null ||
    principal.assuranceExpiresAt <= new Date()
  ) {
    throw new UnauthorizedError("Open a fresh private Florence confirmation for this action");
  }
  const exactContext = Object.fromEntries(
    Object.entries(principal.assuranceContext).filter(([key]) => key !== "returnPath"),
  );
  const parsed = HouseholdInvitationStepUpContextSchema.safeParse(exactContext);
  if (!parsed.success) {
    throw new UnauthorizedError("This private Florence confirmation no longer matches an action");
  }
  verifyExactStepUp(principal, "household_invitation", parsed.data);
  return parsed.data;
}

async function confirmHouseholdInvitationAction(input: {
  readonly action: HouseholdInvitationStepUpContext;
  readonly actorPersonId: string;
  readonly application: FlorenceApplication;
  readonly database: Database;
}) {
  const { action, actorPersonId, application, database } = input;
  if (action.action === "invite") {
    return application.process({
      kind: "web.command",
      actorPersonId,
      command: {
        kind: "invite_household_participant",
        householdId: action.householdId,
        conversationId: action.conversationId,
        expectedParticipantEpochId: action.expectedParticipantEpochId,
        expectedParticipantDigest: action.expectedParticipantDigest,
        inviteeIdentityId: action.inviteeIdentityId,
        inviteePersonId: action.inviteePersonId,
        proposedDisplayName: action.proposedDisplayName,
        role: action.role,
        onboardingAdultIntentId: action.onboardingAdultIntentId || null,
        onboardingAdultIntentVersion: action.onboardingAdultIntentVersion
          ? Number(action.onboardingAdultIntentVersion)
          : null,
      },
    });
  }

  const invitations = await database<{ household_id: string; requested_role: string }[]>`
    select household_id, requested_role from invitations where id = ${action.invitationId}
  `;
  const invitation = invitations[0];
  if (!invitation || invitation.household_id !== action.householdId) {
    throw new ConflictError("This family invitation is no longer current");
  }
  if (action.action === "approve" && invitation.requested_role !== "steward") {
    throw new UnauthorizedError("This invitation does not require a co-steward approval");
  }
  return application.process({
    kind: "web.command",
    actorPersonId,
    command:
      action.action === "approve"
        ? { kind: "approve_household_invitation", invitationId: action.invitationId }
        : { kind: "accept_household_invitation", invitationId: action.invitationId },
  });
}

function familyRoleLabel(role: PeopleView["invitations"][number]["role"]): string {
  if (role === "steward") return "a parent / steward";
  if (role === "caregiver") return "a caregiver";
  return "a family participant";
}

async function requireCompletedOnboarding(database: Database, personId: string): Promise<void> {
  const rows = await database<{ readonly completed: boolean }[]>`
    select exists(
      select 1 from person_onboarding selection
      join household_memberships membership
        on membership.person_id = selection.person_id
        and membership.household_id = selection.selected_household_id
        and membership.status = 'active' and membership.role <> 'dependent'
      join households household on household.id = membership.household_id
        and household.status in ('onboarding', 'active')
      join membership_capabilities read_capability on read_capability.membership_id = membership.id
        and read_capability.capability = 'household.read' and read_capability.status = 'active'
      where selection.person_id = ${personId}
        and family_membership_onboarding_is_current(membership.id)
    ) as completed
  `;
  if (!rows[0]?.completed) {
    throw new ConflictError("Finish your private Florence setup before opening this section");
  }
}

async function requireSession(
  request: FastifyRequest,
  config: FlorenceConfig,
  auth: PostgresWebAuth,
): Promise<SessionPrincipal> {
  const token = request.cookies[sessionCookieName(config)];
  if (!token) throw new UnauthorizedError("Open a fresh private Florence link to sign in");
  return auth.authenticate(token);
}

async function requireWriteSession(
  request: FastifyRequest,
  config: FlorenceConfig,
  auth: PostgresWebAuth,
): Promise<SessionPrincipal> {
  verifySameOrigin(request, config);
  const principal = await requireSession(request, config, auth);
  auth.verifyCsrf(principal, headerString(request.headers["x-csrf-token"]));
  return principal;
}

async function requireStepUpSession(
  request: FastifyRequest,
  config: FlorenceConfig,
  auth: PostgresWebAuth,
  purpose: "google_connect" | "account_controls",
): Promise<SessionPrincipal> {
  const principal = await requireSession(request, config, auth);
  if (
    principal.assuranceKind !== purpose ||
    principal.assuranceExpiresAt === null ||
    principal.assuranceExpiresAt <= new Date()
  ) {
    throw new UnauthorizedError("Request a fresh private Florence confirmation first");
  }
  return principal;
}

async function requireGoogleStartSession(
  request: FastifyRequest,
  config: FlorenceConfig,
  auth: PostgresWebAuth,
  from: "onboarding" | "sources",
): Promise<SessionPrincipal> {
  const principal = await requireSession(request, config, auth);
  const allowed =
    principal.assuranceKind === "google_connect" ||
    (from === "onboarding" && principal.assuranceKind === "onboarding");
  if (!allowed || principal.assuranceExpiresAt === null || principal.assuranceExpiresAt <= new Date()) {
    throw new UnauthorizedError("Request a fresh private Florence confirmation first");
  }
  return principal;
}

async function requireStepUpWriteSession(
  request: FastifyRequest,
  config: FlorenceConfig,
  auth: PostgresWebAuth,
  purpose: "google_connect" | "account_controls",
): Promise<SessionPrincipal> {
  verifySameOrigin(request, config);
  const principal = await requireStepUpSession(request, config, auth, purpose);
  auth.verifyCsrf(principal, headerString(request.headers["x-csrf-token"]));
  return principal;
}

async function requireExactStepUpWriteSession(
  request: FastifyRequest,
  config: FlorenceConfig,
  auth: PostgresWebAuth,
  purpose: "household_invitation" | "private_bridge_standing",
  context: Readonly<Record<string, string>>,
): Promise<SessionPrincipal> {
  verifySameOrigin(request, config);
  const principal = await requireSession(request, config, auth);
  auth.verifyCsrf(principal, headerString(request.headers["x-csrf-token"]));
  verifyExactStepUp(principal, purpose, context);
  return principal;
}

function verifyExactStepUp(
  principal: SessionPrincipal,
  purpose: "household_invitation" | "private_bridge_standing",
  context: Readonly<Record<string, string>>,
): void {
  const serverContextKeys = new Set(["returnPath"]);
  if (
    principal.assuranceKind !== purpose ||
    principal.assuranceExpiresAt === null ||
    principal.assuranceExpiresAt <= new Date() ||
    Object.keys(context).length !==
      Object.keys(principal.assuranceContext).filter((key) => !serverContextKeys.has(key)).length ||
    Object.entries(context).some(([key, value]) => principal.assuranceContext[key] !== value)
  ) {
    throw new UnauthorizedError("Request a fresh private Florence confirmation for this exact action first");
  }
}

function verifySameOrigin(request: FastifyRequest, config: FlorenceConfig): void {
  const expected = new URL(config.publicBaseUrl);
  const origin = headerString(request.headers.origin);
  const forwardedHost = headerString(request.headers["x-forwarded-host"]);
  const actualHost = (forwardedHost ?? headerString(request.headers.host))?.split(",", 1)[0]?.trim();
  if (origin !== expected.origin || actualHost !== expected.host) {
    throw new UnauthorizedError("Cross-origin browser write was rejected");
  }
}

function sessionCookieName(config: FlorenceConfig): string {
  return config.environment === "production" ? SESSION_COOKIE_PRODUCTION : SESSION_COOKIE_DEVELOPMENT;
}

function headersForVerification(headers: FastifyRequest["headers"]): Headers {
  const output = new Headers();
  for (const [name, value] of Object.entries(headers)) {
    if (value === undefined) continue;
    output.set(name, Array.isArray(value) ? value.join(" ") : String(value));
  }
  return output;
}

function headerString(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

function compactStringContext(value: unknown): Readonly<Record<string, string>> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return Object.fromEntries(
    Object.entries(value).filter((entry): entry is [string, string] => typeof entry[1] === "string"),
  );
}

async function exists(target: string): Promise<boolean> {
  try {
    await access(target);
    return true;
  } catch {
    return false;
  }
}

async function readTextOrFallback(target: string, fallback: string): Promise<string> {
  try {
    return await readFile(target, "utf8");
  } catch {
    return fallback;
  }
}

function handoffPage(token: string, purpose: string): string {
  const googleConnect = purpose === "google_connect";
  const onboarding = purpose === "onboarding";
  const title = onboarding
    ? "Set up Florence"
    : googleConnect
      ? "Connect Google"
      : purpose === "account_controls"
        ? "Confirm private controls"
        : "Open Florence";
  const explanation = onboarding
    ? "Continue your private family setup. Florence will resume exactly where you left off."
    : googleConnect
      ? "Continue to Google to choose the account Florence should privately connect to you."
      : "This link came through your exact private Florence conversation. Continue to open your secure account.";
  const button = onboarding ? "Continue setup" : googleConnect ? "Continue to Google" : "Continue securely";
  const expiry = onboarding ? "It remains available for one day" : "It expires shortly";
  return `<!doctype html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>${title}</title><link rel="stylesheet" href="/handoff.css"></head><body><main data-handoff-token="${escapeHtml(token)}" data-handoff-purpose="${escapeHtml(purpose)}"><div class="mark">F</div><h1>${title}</h1><p>${explanation}</p><form><button type="submit">${button}</button></form><div data-status aria-live="polite"></div><small>The link is not used until you tap the button. ${expiry} and cannot be reused.</small></main><script src="/handoff.js" defer></script></body></html>`;
}

export function completedHandoffRedirect(purpose: HandoffPurpose, session: AuthenticatedSession): string {
  if (purpose === "onboarding") return "/onboarding";
  if (purpose === "household_invitation") return "/confirm-action";
  if (purpose === "invitation") return "/people";
  if (purpose === "private_review") return "/sources";
  if (purpose === "web_sign_in") {
    return session.assuranceContext.returnPath === "/sources" ? "/sources" : "/people";
  }
  if (session.assuranceKind === "google_connect") {
    const profile = session.assuranceContext.profile;
    if (profile === "personal_family" || profile === "work") {
      const mail = profile === "work" && session.assuranceContext.mail === "include" ? "&mail=include" : "";
      const from = session.assuranceContext.returnPath === "/onboarding" ? "&from=onboarding" : "";
      return `/oauth/google/start?profile=${profile}${mail}${from}`;
    }
    return "/sources?step_up=google_connect";
  }
  if (session.assuranceKind === "account_controls") return "/safety?step_up=account_controls";
  if (session.assuranceKind === "private_bridge_standing") {
    return "/sources?step_up=private_bridge_standing";
  }
  return "/people";
}

function unavailableHandoffPage(florencePhone: string): string {
  return `<!doctype html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>Get a fresh Florence link</title><link rel="stylesheet" href="/handoff.css"></head><body><main><div class="mark">F</div><h1>This private link is no longer available</h1><p>Florence links are single-use. Nothing is wrong with your account.</p><p>Text <strong>continue setup</strong> to resume onboarding, <strong>connect Google</strong> for Gmail and Calendar, or <strong>settings</strong> for private controls.</p><a href="sms:${escapeHtml(florencePhone)}">Text Florence</a></main></body></html>`;
}

function policyPage(title: string, paragraphs: readonly string[]): string {
  return `<!doctype html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Florence · ${escapeHtml(title)}</title><link rel="stylesheet" href="/handoff.css"></head><body><main><div class="mark">F</div><h1>${escapeHtml(title)}</h1>${paragraphs.map((paragraph) => `<p>${escapeHtml(paragraph)}</p>`).join("")}<a href="/">Back to Florence</a></main></body></html>`;
}

function escapeHtml(value: string): string {
  return value.replace(
    /[&<>"']/gu,
    (character) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[character] ?? character,
  );
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function googleCapabilitiesForProfile(
  profile: GoogleConnectionProfile,
  includeWorkMail = false,
): readonly GoogleCapability[] {
  return profile === "work" && !includeWorkMail ? ["calendar"] : ["mail", "calendar"];
}
