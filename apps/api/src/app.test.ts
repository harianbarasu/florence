import { createHmac } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AcceptanceReceipt, HouseholdProfile, HouseholdSignal } from "@florence/contracts";
import type {
  HouseholdIngress,
  HouseholdIngressReceipt,
  LinqEnrollmentRedemptionReceipt,
  LinqEnrollmentRedemptionRequest,
  LinqGroupBootstrapReceipt,
  LinqGroupBootstrapRequest,
} from "@florence/control-plane";
import { SignalConflictError } from "@florence/database";
import type { GoogleConnectionView } from "@florence/google";
import { LinqError, type LinqObservedChat, linqIdentitySubjectDigest } from "@florence/linq";
import { afterEach, describe, expect, it } from "vitest";
import {
  type AppDependencies,
  buildApp,
  type Caller,
  type CallerResolver,
  createPilotCallerResolver,
} from "./app.js";
import { EnrollmentCodes } from "./enrollment.js";
import {
  createLinqIngress,
  type LinqIngressAuthority,
  type LinqIngressAuthorityResolver,
} from "./linq-ingress.js";

const occurredAt = "2026-08-11T18:00:00.000Z";
const adultId = "11111111-1111-4111-8111-111111111111";
const householdId = "22222222-2222-4222-8222-222222222222";
const childId = "33333333-3333-4333-8333-333333333333";
const commandId = "44444444-4444-4444-8444-444444444444";
const conversationId = "55555555-5555-4555-8555-555555555555";
const webhookId = "66666666-6666-4666-8666-666666666666";
const providerMessageId = "provider-message-one";
const providerConversationId = "provider-conversation-one";
const providerHandleId = "provider-handle-one";
const secondProviderHandleId = "provider-handle-two";
const plannedAdultId = "77777777-7777-4777-8777-777777777777";
const secondAdultId = "88888888-8888-4888-8888-888888888888";
const secondHouseholdId = "99999999-9999-4999-8999-999999999999";
const persistedHouseholdId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const firstPilotToken = "first-pilot-access-token-with-32-plus-bytes";
const secondPilotToken = "second-pilot-access-token-with-32-plus-bytes";
const sessionSecret = "pilot-session-signing-secret-with-32-plus-bytes";
const signingKey = Buffer.from("florence-linq-ingress-test-secret");
const signingSecret = `whsec_${signingKey.toString("base64")}`;

const apps: Awaited<ReturnType<typeof buildApp>>[] = [];
const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(apps.splice(0).map((app) => app.close()));
  await Promise.all(temporaryDirectories.splice(0).map((directory) => rm(directory, { recursive: true })));
});

class RecordingChiefOfStaff {
  readonly accepted: HouseholdSignal[] = [];
  readonly profiles = new Map<string, HouseholdProfile>();
  acceptError: Error | null = null;
  readonly redemptions: LinqEnrollmentRedemptionRequest[] = [];
  redemptionReceipt: LinqEnrollmentRedemptionReceipt | null = null;
  readonly groupBootstraps: LinqGroupBootstrapRequest[] = [];
  groupBootstrapReceipt: LinqGroupBootstrapReceipt | null = null;

  async accept(signal: HouseholdSignal): Promise<AcceptanceReceipt>;
  async accept(input: {
    command: "linq.enrollment.redeem";
    input: LinqEnrollmentRedemptionRequest;
  }): Promise<LinqEnrollmentRedemptionReceipt | null>;
  async accept(input: {
    command: "linq.group.bootstrap";
    input: LinqGroupBootstrapRequest;
  }): Promise<LinqGroupBootstrapReceipt | null>;
  async accept(input: HouseholdIngress): Promise<HouseholdIngressReceipt>;
  async accept(input: HouseholdIngress): Promise<HouseholdIngressReceipt> {
    if ("command" in input) {
      if (input.command === "linq.enrollment.redeem") {
        this.redemptions.push(input.input);
        return this.redemptionReceipt;
      }
      this.groupBootstraps.push(input.input);
      return this.groupBootstrapReceipt;
    }
    if (this.acceptError) throw this.acceptError;
    this.accepted.push(input);
    return {
      signalId: input.signalId,
      householdId: input.householdId,
      disposition: "accepted",
      acceptedAt: occurredAt,
    };
  }

  async profile(id: string): Promise<HouseholdProfile | null> {
    return this.profiles.get(id) ?? null;
  }
}

class RecordingGoogleConnection {
  readonly view: GoogleConnectionView = {
    connectionId: commandId,
    householdId,
    ownerAdultId: adultId,
    status: "active",
    emailLabel: "jackson@example.com",
    grantedScopes: [
      "openid",
      "email",
      "https://www.googleapis.com/auth/gmail.readonly",
      "https://www.googleapis.com/auth/calendar.events.owned",
    ],
    lastError: null,
    createdAt: occurredAt,
    updatedAt: occurredAt,
  };
  beginInput: Record<string, unknown> | null = null;
  finishInput: Record<string, unknown> | null = null;
  disconnectInput: Record<string, unknown> | null = null;

  async begin(input: Record<string, unknown>) {
    this.beginInput = input;
    return {
      connection: { ...this.view, status: "pending" as const },
      authorizationUrl: "https://accounts.google.com/o/oauth2/v2/auth?state=secret",
      expiresAt: occurredAt,
    };
  }

  async finish(input: Record<string, unknown>) {
    this.finishInput = input;
    return this.view;
  }

  async status() {
    return [this.view];
  }

  async disconnect(input: Record<string, unknown>) {
    this.disconnectInput = input;
    return {
      connection: { ...this.view, status: "disconnected" as const },
      providerRevocation: "confirmed" as const,
    };
  }
}

function profile(): HouseholdProfile {
  return {
    householdId,
    name: "Jackson family",
    timeZone: "America/Los_Angeles",
    version: 1,
    onboardingComplete: false,
    identityBoundAdultIds: [],
    members: [
      {
        id: adultId,
        kind: "adult",
        role: "steward",
        displayName: "Jackson",
        relationship: "Parent",
        status: "verified",
      },
    ],
  };
}

function resolver(
  caller: Caller | null,
  onGrant?: (household: string) => void,
  otherPilotAdultId: string | null = null,
): CallerResolver {
  return {
    async resolve() {
      return caller;
    },
    async grantHousehold(_caller, household) {
      onGrant?.(household);
    },
    async otherPilotAdultId() {
      return otherPilotAdultId;
    },
  };
}

function dependencies(chief: RecordingChiefOfStaff, caller: Caller | null): AppDependencies {
  return { chiefOfStaff: chief, callerResolver: resolver(caller) };
}

function linqEnvelope(eventType = "message.received", includeMedia = true): Record<string, unknown> {
  return {
    api_version: "v3",
    webhook_version: "2026-02-03",
    event_type: eventType,
    event_id: webhookId,
    created_at: occurredAt,
    trace_id: "trace-one",
    partner_id: "partner-one",
    data:
      eventType === "message.received"
        ? {
            chat: {
              id: providerConversationId,
              is_group: true,
              owner_handle: { id: "owner-line-one", handle: "+12025550000", is_me: true },
            },
            id: providerMessageId,
            direction: "inbound",
            sender_handle: { id: providerHandleId, handle: "+12025550123", is_me: false },
            parts: [
              { type: "text", value: "The permission slip is attached." },
              ...(includeMedia
                ? [
                    {
                      type: "media",
                      id: "attachment-one",
                      filename: "permission.png",
                      mime_type: "image/png",
                      size_bytes: 8,
                    },
                  ]
                : []),
            ],
            reply_to: { message_id: "prior-provider-message", part_index: 0 },
            sent_at: occurredAt,
            reconciled_at: null,
            service: "iMessage",
          }
        : {
            chat_id: providerConversationId,
            message_id: providerMessageId,
            delivered_at: occurredAt,
          },
  };
}

function privateEnrollmentEnvelope(code: string): Record<string, unknown> {
  const envelope = linqEnvelope("message.received", false);
  const data = envelope.data as Record<string, unknown>;
  const chat = data.chat as Record<string, unknown>;
  chat.is_group = false;
  data.parts = [{ type: "text", value: code }];
  data.reply_to = null;
  return envelope;
}

function signedLinqWebhook(envelope: Record<string, unknown>, valid = true) {
  const body = JSON.stringify(envelope);
  const timestamp = Math.floor(new Date(occurredAt).getTime() / 1000);
  const signature = createHmac("sha256", valid ? signingKey : Buffer.from("wrong-key"))
    .update(`${webhookId}.${timestamp}.${body}`)
    .digest("base64");
  return {
    payload: Buffer.from(body),
    headers: {
      "content-type": "application/json",
      "webhook-id": webhookId,
      "webhook-timestamp": String(timestamp),
      "webhook-signature": `v1,${signature}`,
    },
  };
}

function linqAuthority(): LinqIngressAuthority {
  return {
    householdId,
    conversationId,
    audience: "group",
    authorityVersion: 3,
    participantSetDigest: "a".repeat(64),
    expectedParticipantIdentityDigests: [providerHandleId, secondProviderHandleId]
      .map(linqIdentitySubjectDigest)
      .sort(),
    senderAdultId: adultId,
    replyToSignalId: commandId,
  };
}

function linqDependencies(
  chief: RecordingChiefOfStaff,
  authority: LinqIngressAuthority | null = linqAuthority(),
  observed: LinqObservedChat | Error | null = authority
    ? {
        audience: authority.audience,
        participantIdentityDigests: authority.expectedParticipantIdentityDigests,
      }
    : {
        audience: "group",
        participantIdentityDigests: [providerHandleId, secondProviderHandleId]
          .map(linqIdentitySubjectDigest)
          .sort(),
      },
  enrollmentCodes?: EnrollmentCodes,
) {
  const resolved: Parameters<LinqIngressAuthorityResolver["resolveLinqIngressAuthority"]>[0][] = [];
  const stored: Array<{ assetId: string; signalId: string }> = [];
  const fetched: string[] = [];
  const observedChats: string[] = [];
  const ingress = createLinqIngress({
    signingSecret,
    expectedPartnerId: "partner-one",
    now: () => new Date(occurredAt),
    authorityResolver: {
      async resolveLinqIngressAuthority(input) {
        resolved.push(input);
        return authority;
      },
    },
    providerReader: {
      async observeChat(providerChatId) {
        observedChats.push(providerChatId);
        if (observed instanceof Error) throw observed;
        if (!observed) throw new Error("Unexpected provider observation");
        return observed;
      },
      async fetchMedia(reference) {
        fetched.push(reference.providerAttachmentId);
        return {
          bytes: Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
          mimeType: reference.mimeType,
        };
      },
    },
    imageVault: {
      async store(input) {
        stored.push({ assetId: input.assetId, signalId: input.signalId });
        return { image: { assetId: input.assetId, mimeType: input.declaredMimeType } };
      },
    },
    chiefOfStaff: chief,
    ...(enrollmentCodes ? { enrollmentCodes } : {}),
  });
  return { ingress, resolved, observedChats, stored, fetched };
}

describe("Florence API", () => {
  it("exposes deployment liveness and database readiness without exposing errors", async () => {
    const chief = new RecordingChiefOfStaff();
    let checks = 0;
    const readyApp = await buildApp({
      ...dependencies(chief, null),
      readiness: async () => {
        checks += 1;
      },
      databaseSchema: "florence_test",
    });
    const unavailableApp = await buildApp({
      ...dependencies(chief, null),
      readiness: async () => {
        throw new Error("credential-bearing database failure");
      },
    });
    apps.push(readyApp, unavailableApp);

    const health = await readyApp.inject({ method: "GET", url: "/healthz" });
    const ready = await readyApp.inject({ method: "GET", url: "/readyz" });
    const unavailable = await unavailableApp.inject({ method: "GET", url: "/readyz" });

    expect(health.json()).toEqual({ ok: true, service: "florence-web" });
    expect(health.headers["content-security-policy"]).toContain("default-src 'self'");
    expect(health.headers["referrer-policy"]).toBe("no-referrer");
    expect(ready.json()).toEqual({ ok: true, database: true, schema: "florence_test" });
    expect(checks).toBe(1);
    expect(unavailable.statusCode).toBe(503);
    expect(unavailable.json()).toEqual({ ok: false, database: false });
    expect(unavailable.body).not.toContain("credential-bearing");
  });

  it("requires the production web origin for cookie-authenticated mutations", async () => {
    const chief = new RecordingChiefOfStaff();
    const app = await buildApp({
      ...dependencies(chief, null),
      webOrigin: "https://florence.example",
    });
    apps.push(app);
    const request = {
      method: "DELETE" as const,
      url: "/api/v1/session",
      headers: { cookie: "florence_pilot_session=session-value" },
    };

    const missing = await app.inject(request);
    const foreign = await app.inject({
      ...request,
      headers: { ...request.headers, origin: "https://attacker.example" },
    });
    const sameOrigin = await app.inject({
      ...request,
      headers: { ...request.headers, origin: "https://florence.example" },
    });

    expect(missing.statusCode).toBe(403);
    expect(foreign.json()).toEqual({ error: "same_origin_required" });
    expect(sameOrigin.json()).toEqual({ signedOut: true });
  });

  it("issues one retry-stable code and redeems only that full token in its live private chat", async () => {
    const chief = new RecordingChiefOfStaff();
    chief.profiles.set(householdId, {
      ...profile(),
      members: [
        ...profile().members,
        {
          id: plannedAdultId,
          kind: "adult",
          role: "steward",
          displayName: "Taylor",
          relationship: "Co-parent",
          status: "planned",
        },
      ],
    });
    const enrollmentCodes = new EnrollmentCodes("api-enrollment-test-secret-that-is-long-enough");
    const configured = linqDependencies(
      chief,
      null,
      {
        audience: "private",
        participantIdentityDigests: [linqIdentitySubjectDigest(providerHandleId)],
      },
      enrollmentCodes,
    );
    const app = await buildApp({
      ...dependencies(chief, { adultId, authorizedHouseholdIds: [householdId] }),
      enrollmentCodes,
      linqIngress: configured.ingress,
    });
    apps.push(app);
    const issueRequest = {
      method: "POST" as const,
      url: `/api/v1/households/${householdId}/members/${plannedAdultId}/linq-enrollment`,
      payload: { commandId, occurredAt },
    };
    const issued = await app.inject(issueRequest);
    const retried = await app.inject(issueRequest);
    const code = issued.json().code as string;

    expect(issued.statusCode).toBe(202);
    expect(issued.headers["cache-control"]).toBe("no-store");
    expect(retried.json().code).toBe(code);
    expect(chief.accepted[0]).toMatchObject({
      type: "adult.enrollment.issued",
      adultId: plannedAdultId,
      challengeDigest: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
    expect(JSON.stringify(chief.accepted)).not.toContain(code);

    chief.redemptionReceipt = {
      signalId: webhookId,
      householdId,
      disposition: "accepted",
      acceptedAt: occurredAt,
      adultId: plannedAdultId,
      conversationId,
    };
    const redeemed = await app.inject({
      method: "POST",
      url: "/webhooks/linq",
      ...signedLinqWebhook(privateEnrollmentEnvelope(`  ${code}  `)),
    });

    expect(redeemed.statusCode).toBe(202);
    expect(chief.redemptions).toEqual([
      expect.objectContaining({
        challengeDigest: (chief.accepted[0] as Extract<HouseholdSignal, { type: "adult.enrollment.issued" }>)
          .challengeDigest,
        identitySubjectDigest: linqIdentitySubjectDigest(providerHandleId),
        providerConversationId,
      }),
    ]);
    expect(chief.accepted.some((signal) => signal.type === "conversation.message")).toBe(false);
  });

  it("fails closed without a configured or injected identity", async () => {
    const chief = new RecordingChiefOfStaff();
    const app = await buildApp({ chiefOfStaff: chief, callerResolver: createPilotCallerResolver({}) });
    apps.push(app);

    const response = await app.inject({ method: "GET", url: "/api/v1/households" });
    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({ error: "unauthorized" });
  });

  it("creates separate opaque browser sessions for two configured adults", async () => {
    const chief = new RecordingChiefOfStaff();
    chief.profiles.set(householdId, profile());
    chief.profiles.set(persistedHouseholdId, { ...profile(), householdId: persistedHouseholdId });
    chief.profiles.set(secondHouseholdId, {
      ...profile(),
      householdId: secondHouseholdId,
      members: [
        {
          id: secondAdultId,
          kind: "adult",
          role: "steward",
          displayName: "Taylor",
          relationship: "Parent",
          status: "verified",
        },
      ],
    });
    const callerResolver = createPilotCallerResolver(
      {
        FLORENCE_PILOT_CREDENTIALS: JSON.stringify([
          { token: firstPilotToken, adultId, householdIds: [householdId] },
          { token: secondPilotToken, adultId: secondAdultId },
        ]),
        FLORENCE_SESSION_SECRET: sessionSecret,
      },
      {
        async listHouseholdIdsForAdult(id) {
          return id === adultId ? [persistedHouseholdId] : [secondHouseholdId];
        },
      },
    );
    const app = await buildApp({ chiefOfStaff: chief, callerResolver });
    apps.push(app);

    const firstSignedIn = await app.inject({
      method: "POST",
      url: "/api/v1/session",
      headers: { authorization: `Bearer ${firstPilotToken}` },
    });
    const secondSignedIn = await app.inject({
      method: "POST",
      url: "/api/v1/session",
      headers: { authorization: `Bearer ${secondPilotToken}` },
    });
    expect(firstSignedIn.json()).toEqual({ adultId });
    expect(secondSignedIn.json()).toEqual({ adultId: secondAdultId });

    const firstSetCookie = String(firstSignedIn.headers["set-cookie"]);
    const secondSetCookie = String(secondSignedIn.headers["set-cookie"]);
    expect(firstSetCookie).toContain("HttpOnly");
    expect(firstSetCookie).toContain("SameSite=Lax");
    expect(firstSetCookie).toContain("Max-Age=604800");
    expect(firstSetCookie).not.toContain(firstPilotToken);
    expect(firstSetCookie).not.toContain(adultId);
    expect(secondSetCookie).not.toBe(firstSetCookie);

    const [firstCookie = ""] = firstSetCookie.split(";", 1);
    const [secondCookie = ""] = secondSetCookie.split(";", 1);
    const firstHouseholds = await app.inject({
      method: "GET",
      url: "/api/v1/households",
      headers: { cookie: firstCookie },
    });
    const secondHouseholds = await app.inject({
      method: "GET",
      url: "/api/v1/households",
      headers: { cookie: secondCookie },
    });
    expect(
      firstHouseholds.json().households.map((household: HouseholdProfile) => household.householdId),
    ).toEqual([householdId, persistedHouseholdId]);
    expect(
      secondHouseholds.json().households.map((household: HouseholdProfile) => household.householdId),
    ).toEqual([secondHouseholdId]);

    const tamperedCookie = `${firstCookie.slice(0, -1)}${firstCookie.endsWith("a") ? "b" : "a"}`;
    const tampered = await app.inject({
      method: "GET",
      url: "/api/v1/session",
      headers: { cookie: tamperedCookie },
    });
    expect(tampered.statusCode).toBe(401);

    const signedOut = await app.inject({ method: "DELETE", url: "/api/v1/session" });
    expect(signedOut.headers["set-cookie"]).toContain("Max-Age=0");
  });

  it("binds each owner Google handoff to the authenticated browser session", async () => {
    const chief = new RecordingChiefOfStaff();
    chief.profiles.set(householdId, { ...profile(), identityBoundAdultIds: [adultId] });
    const googleConnection = new RecordingGoogleConnection();
    const app = await buildApp({
      ...dependencies(chief, { adultId, authorizedHouseholdIds: [householdId] }),
      googleConnection,
    });
    apps.push(app);
    const cookie = "florence_pilot_session=signed-browser-session";

    const noSession = await app.inject({
      method: "POST",
      url: `/api/v1/households/${householdId}/google-connections`,
    });
    expect(noSession.statusCode).toBe(401);

    const started = await app.inject({
      method: "POST",
      url: `/api/v1/households/${householdId}/google-connections`,
      headers: { cookie },
    });
    expect(started.statusCode).toBe(201);
    expect(started.headers["cache-control"]).toBe("no-store");
    expect(googleConnection.beginInput).toMatchObject({
      householdId,
      ownerAdultId: adultId,
      sessionBindingDigest: expect.stringMatching(/^[a-f0-9]{64}$/),
    });

    const callback = await app.inject({
      method: "GET",
      url: "/oauth/google/callback?state=state-one&code=code-one",
      headers: { cookie },
    });
    expect(callback.statusCode).toBe(302);
    expect(callback.headers.location).toBe("/settings?google=connected");
    expect(googleConnection.finishInput?.sessionBindingDigest).toBe(
      googleConnection.beginInput?.sessionBindingDigest,
    );

    const listed = await app.inject({
      method: "GET",
      url: `/api/v1/households/${householdId}/google-connections`,
      headers: { cookie },
    });
    expect(listed.json().connections).toEqual([googleConnection.view]);

    const disconnected = await app.inject({
      method: "DELETE",
      url: `/api/v1/households/${householdId}/google-connections/${commandId}`,
      headers: { cookie },
    });
    expect(disconnected.statusCode).toBe(200);
    expect(googleConnection.disconnectInput).toMatchObject({
      connectionId: commandId,
      householdId,
      ownerAdultId: adultId,
    });
  });

  it("rejects malformed, weak, or ambiguous pilot credential configuration", () => {
    const configured =
      (credentials: unknown, secret = sessionSecret) =>
      () =>
        createPilotCallerResolver({
          FLORENCE_PILOT_CREDENTIALS: JSON.stringify(credentials),
          FLORENCE_SESSION_SECRET: secret,
        });

    expect(() => createPilotCallerResolver({ FLORENCE_PILOT_CREDENTIALS: "[]" })).toThrow(
      /configured together/,
    );
    expect(configured([{ token: firstPilotToken, adultId }])).toThrow(/exactly two adult credentials/);
    expect(
      configured([
        { token: "short", adultId },
        { token: secondPilotToken, adultId: secondAdultId },
      ]),
    ).toThrow(/at least 32 bytes/);
    expect(
      configured([
        { token: firstPilotToken, adultId, browserAdultId: secondAdultId },
        { token: secondPilotToken, adultId: secondAdultId },
      ]),
    ).toThrow(/contain only/);
    expect(
      configured([
        { token: firstPilotToken, adultId },
        { token: firstPilotToken, adultId: secondAdultId },
      ]),
    ).toThrow(/tokens must be distinct/);
    expect(
      configured(
        [
          { token: firstPilotToken, adultId },
          { token: secondPilotToken, adultId: secondAdultId },
        ],
        "short",
      ),
    ).toThrow(/at least 32 bytes/);
  });

  it("binds both onboarding adults to the two configured browser identities", async () => {
    const chief = new RecordingChiefOfStaff();
    const app = await buildApp({
      chiefOfStaff: chief,
      callerResolver: createPilotCallerResolver({
        FLORENCE_PILOT_CREDENTIALS: JSON.stringify([
          { token: firstPilotToken, adultId },
          { token: secondPilotToken, adultId: secondAdultId },
        ]),
        FLORENCE_SESSION_SECRET: sessionSecret,
      }),
    });
    apps.push(app);

    const payload = {
      commandId: householdId,
      occurredAt,
      name: "Jackson family",
      timeZone: "America/Los_Angeles",
      foundingAdultDisplayName: "Jackson",
      secondAdultDisplayName: "Kendall",
      secondAdultRole: "steward",
      secondAdultRelationship: "Co-parent",
    };
    const response = await app.inject({
      method: "POST",
      url: "/api/v1/households",
      headers: { authorization: `Bearer ${firstPilotToken}` },
      payload,
    });

    expect(response.statusCode).toBe(202);
    expect(response.json().householdId).toBe(householdId);
    expect(chief.accepted).toEqual([
      {
        type: "household.created",
        signalId: householdId,
        householdId,
        idempotencyKey: `dashboard:household:${householdId}`,
        occurredAt,
        name: "Jackson family",
        timeZone: "America/Los_Angeles",
        foundingAdult: { id: adultId, displayName: "Jackson" },
        plannedAdult: {
          id: secondAdultId,
          displayName: "Kendall",
          role: "steward",
          relationship: "Co-parent",
        },
      },
    ]);
    const secondSession = await app.inject({
      method: "POST",
      url: "/api/v1/session",
      headers: { authorization: `Bearer ${secondPilotToken}` },
    });
    expect(secondSession.json()).toEqual({ adultId: secondAdultId });
    const secondCaller = await createPilotCallerResolver(
      {
        FLORENCE_PILOT_CREDENTIALS: JSON.stringify([
          { token: firstPilotToken, adultId },
          { token: secondPilotToken, adultId: secondAdultId },
        ]),
        FLORENCE_SESSION_SECRET: sessionSecret,
      },
      {
        async listHouseholdIdsForAdult(id) {
          return id === secondAdultId ? [householdId] : [];
        },
      },
    ).issueSession?.(secondPilotToken);
    expect(secondCaller?.caller).toMatchObject({
      adultId: secondAdultId,
      authorizedHouseholdIds: [householdId],
    });
    chief.acceptError = new SignalConflictError(`dashboard:household:${householdId}`);
    const conflict = await app.inject({
      method: "POST",
      url: "/api/v1/households",
      headers: { authorization: `Bearer ${firstPilotToken}` },
      payload,
    });
    expect(conflict.statusCode).toBe(409);
  });

  it("lists and reads only the adult's authorized households", async () => {
    const chief = new RecordingChiefOfStaff();
    chief.profiles.set(householdId, profile());
    const app = await buildApp(dependencies(chief, { adultId, authorizedHouseholdIds: [householdId] }));
    apps.push(app);

    const list = await app.inject({ method: "GET", url: "/api/v1/households" });
    const read = await app.inject({ method: "GET", url: `/api/v1/households/${householdId}` });
    const forbidden = await app.inject({
      method: "GET",
      url: "/api/v1/households/99999999-9999-4999-8999-999999999999",
    });

    expect(list.json()).toEqual({ households: [profile()] });
    expect(read.json()).toEqual({ household: profile() });
    expect(forbidden.statusCode).toBe(403);
  });

  it("derives represented child status and keeps every supported family field", async () => {
    const chief = new RecordingChiefOfStaff();
    chief.profiles.set(householdId, profile());
    const app = await buildApp(dependencies(chief, { adultId, authorizedHouseholdIds: [householdId] }));
    apps.push(app);

    const response = await app.inject({
      method: "PUT",
      url: `/api/v1/households/${householdId}/members/${childId}`,
      payload: {
        commandId,
        occurredAt,
        kind: "child",
        role: "dependent",
        displayName: "Maya",
        relationship: "Child",
        aliases: ["May"],
        birthYear: 2018,
        school: "Redwood Elementary",
        currentGrade: "2",
        academicYear: "2026-27",
        gradeEffectiveFrom: "2026-08-20",
        activities: ["Soccer"],
      },
    });

    expect(response.statusCode).toBe(202);
    expect(chief.accepted[0]).toMatchObject({
      type: "family.member.upserted",
      actorAdultId: adultId,
      householdId,
      signalId: commandId,
      status: "represented",
      member: {
        id: childId,
        aliases: ["May"],
        academicYear: "2026-27",
        gradeEffectiveFrom: "2026-08-20",
        activities: ["Soccer"],
      },
    });
  });

  it("preserves verified authority and rejects profile edits that would change it", async () => {
    const chief = new RecordingChiefOfStaff();
    chief.profiles.set(householdId, profile());
    const app = await buildApp(dependencies(chief, { adultId, authorizedHouseholdIds: [householdId] }));
    apps.push(app);
    const base = {
      commandId,
      occurredAt,
      kind: "adult",
      role: "steward",
      displayName: "Jackson B.",
      relationship: "Parent",
    };

    const edit = await app.inject({
      method: "PUT",
      url: `/api/v1/households/${householdId}/members/${adultId}`,
      payload: base,
    });
    const demote = await app.inject({
      method: "PUT",
      url: `/api/v1/households/${householdId}/members/${adultId}`,
      payload: { ...base, role: "caregiver" },
    });
    const selfVerify = await app.inject({
      method: "PUT",
      url: `/api/v1/households/${householdId}/members/${childId}`,
      payload: { ...base, status: "verified" },
    });

    expect(edit.statusCode).toBe(202);
    expect(chief.accepted[0]).toMatchObject({ status: "verified" });
    expect(demote.statusCode).toBe(409);
    expect(selfVerify.statusCode).toBe(400);
  });

  it("rejects unauthenticated and unauthorized Linq messages before household mutation", async () => {
    const chief = new RecordingChiefOfStaff();
    const configured = linqDependencies(chief, null);
    const app = await buildApp({ ...dependencies(chief, null), linqIngress: configured.ingress });
    apps.push(app);

    const invalid = await app.inject({
      method: "POST",
      url: "/webhooks/linq",
      ...signedLinqWebhook(linqEnvelope(), false),
    });
    const unauthorized = await app.inject({
      method: "POST",
      url: "/webhooks/linq",
      ...signedLinqWebhook(linqEnvelope()),
    });

    expect(invalid.statusCode).toBe(401);
    expect(unauthorized.statusCode).toBe(200);
    expect(unauthorized.json()).toEqual({ disposition: "rejected", reason: "authority_not_found" });
    expect(chief.accepted).toEqual([]);
    expect(configured.observedChats).toEqual([]);
    expect(configured.fetched).toEqual([]);
  });

  it("rejects authenticated SMS and RCS before authority lookup or household mutation", async () => {
    const chief = new RecordingChiefOfStaff();
    const configured = linqDependencies(chief);
    const app = await buildApp({ ...dependencies(chief, null), linqIngress: configured.ingress });
    apps.push(app);

    for (const service of ["SMS", "RCS"] as const) {
      const envelope = linqEnvelope();
      (envelope.data as Record<string, unknown>).service = service;
      const response = await app.inject({
        method: "POST",
        url: "/webhooks/linq",
        ...signedLinqWebhook(envelope),
      });

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ disposition: "rejected", reason: "unsupported_service" });
    }
    expect(configured.resolved).toEqual([]);
    expect(configured.observedChats).toEqual([]);
    expect(configured.fetched).toEqual([]);
    expect(configured.stored).toEqual([]);
    expect(chief.redemptions).toEqual([]);
    expect(chief.groupBootstraps).toEqual([]);
    expect(chief.accepted).toEqual([]);
  });

  it("bootstraps an exact live household group once and rejects changed evidence", async () => {
    const chief = new RecordingChiefOfStaff();
    chief.groupBootstrapReceipt = {
      signalId: webhookId,
      householdId,
      disposition: "accepted",
      acceptedAt: occurredAt,
      conversationId,
    };
    const configured = linqDependencies(chief, null);
    const app = await buildApp({ ...dependencies(chief, null), linqIngress: configured.ingress });
    apps.push(app);
    const request = {
      method: "POST" as const,
      url: "/webhooks/linq",
      ...signedLinqWebhook(linqEnvelope("message.received", false)),
    };

    const accepted = await app.inject(request);
    chief.groupBootstrapReceipt = { ...chief.groupBootstrapReceipt, disposition: "duplicate" };
    const replayed = await app.inject(request);
    expect(accepted.statusCode).toBe(202);
    expect(replayed.statusCode).toBe(202);
    expect(replayed.json().disposition).toBe("duplicate");
    expect(chief.groupBootstraps).toHaveLength(2);
    expect(chief.groupBootstraps[0]).toEqual(chief.groupBootstraps[1]);
    expect(chief.groupBootstraps[0]).toMatchObject({
      text: "The permission slip is attached.",
      senderIdentitySubjectDigest: linqIdentitySubjectDigest(providerHandleId),
      participantIdentityDigests: linqAuthority().expectedParticipantIdentityDigests,
    });

    const mismatchChief = new RecordingChiefOfStaff();
    const mismatch = linqDependencies(mismatchChief, null, {
      audience: "group",
      participantIdentityDigests: [linqIdentitySubjectDigest(secondProviderHandleId), "f".repeat(64)].sort(),
    });
    const mismatchApp = await buildApp({
      ...dependencies(mismatchChief, null),
      linqIngress: mismatch.ingress,
    });
    apps.push(mismatchApp);
    const rejected = await mismatchApp.inject(request);
    expect(rejected.json()).toEqual({
      disposition: "rejected",
      reason: "authority_evidence_mismatch",
    });
    expect(mismatchChief.groupBootstraps).toEqual([]);
  });

  it("turns authenticated Linq evidence into one exact app-authorized signal", async () => {
    const chief = new RecordingChiefOfStaff();
    const configured = linqDependencies(chief);
    const app = await buildApp({ ...dependencies(chief, null), linqIngress: configured.ingress });
    apps.push(app);

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/linq",
      ...signedLinqWebhook(linqEnvelope()),
    });

    expect(response.statusCode).toBe(202);
    expect(configured.resolved).toEqual([
      {
        providerConversationId,
        providerHandleId,
        replyToProviderMessageId: "prior-provider-message",
        occurredAt,
      },
    ]);
    expect(configured.observedChats).toEqual([providerConversationId]);
    expect(chief.accepted).toHaveLength(1);
    expect(chief.accepted[0]).toMatchObject({
      type: "conversation.message",
      householdId,
      conversationId,
      audience: "group",
      authorityVersion: 3,
      senderAdultId: adultId,
      replyToSignalId: commandId,
      text: "The permission slip is attached.",
      source: { system: "linq-v3", providerEventId: webhookId, providerMessageId },
    });
    expect(configured.stored).toEqual([
      {
        assetId: (chief.accepted[0] as Extract<HouseholdSignal, { type: "conversation.message" }>).images[0]
          ?.assetId,
        signalId: chief.accepted[0]?.signalId,
      },
    ]);
  });

  it("fails closed on changed live Linq participants before reading media or accepting truth", async () => {
    const chief = new RecordingChiefOfStaff();
    const configured = linqDependencies(chief, linqAuthority(), {
      audience: "group",
      participantIdentityDigests: [linqIdentitySubjectDigest(providerHandleId), "f".repeat(64)].sort(),
    });
    const app = await buildApp({ ...dependencies(chief, null), linqIngress: configured.ingress });
    apps.push(app);

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/linq",
      ...signedLinqWebhook(linqEnvelope()),
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      disposition: "rejected",
      reason: "authority_evidence_mismatch",
    });
    expect(configured.observedChats).toEqual([providerConversationId]);
    expect(configured.fetched).toEqual([]);
    expect(configured.stored).toEqual([]);
    expect(chief.accepted).toEqual([]);
  });

  it("retries a transient live Linq authority read without reading media or accepting truth", async () => {
    const chief = new RecordingChiefOfStaff();
    const configured = linqDependencies(
      chief,
      linqAuthority(),
      new LinqError("provider_retryable", "Linq chat read unavailable", true),
    );
    const app = await buildApp({ ...dependencies(chief, null), linqIngress: configured.ingress });
    apps.push(app);

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/linq",
      ...signedLinqWebhook(linqEnvelope()),
    });

    expect(response.statusCode).toBe(503);
    expect(response.json()).toEqual({ error: "provider_retryable" });
    expect(configured.resolved).toHaveLength(1);
    expect(configured.fetched).toEqual([]);
    expect(configured.stored).toEqual([]);
    expect(chief.accepted).toEqual([]);
  });

  it("rejects a permanently invalid live Linq chat before media or household mutation", async () => {
    const chief = new RecordingChiefOfStaff();
    const configured = linqDependencies(
      chief,
      linqAuthority(),
      new LinqError("provider_rejected", "Linq returned invalid current chat authority"),
    );
    const app = await buildApp({ ...dependencies(chief, null), linqIngress: configured.ingress });
    apps.push(app);

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/linq",
      ...signedLinqWebhook(linqEnvelope()),
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: "provider_rejected" });
    expect(configured.fetched).toEqual([]);
    expect(configured.stored).toEqual([]);
    expect(chief.accepted).toEqual([]);
  });

  it("keeps provider retries byte-stable and ignores unsupported events without mutating truth", async () => {
    const chief = new RecordingChiefOfStaff();
    const configured = linqDependencies(chief);
    const app = await buildApp({ ...dependencies(chief, null), linqIngress: configured.ingress });
    apps.push(app);
    const request = {
      method: "POST" as const,
      url: "/webhooks/linq",
      ...signedLinqWebhook(linqEnvelope()),
    };

    await app.inject(request);
    await app.inject(request);
    const first = chief.accepted[0];
    const second = chief.accepted[1];
    expect(JSON.stringify(first)).toBe(JSON.stringify(second));
    expect(configured.stored[0]).toEqual(configured.stored[1]);

    const status = await app.inject({
      method: "POST",
      url: "/webhooks/linq",
      ...signedLinqWebhook(linqEnvelope("message.delivered", false)),
    });
    expect(status.statusCode).toBe(200);
    expect(status.json()).toEqual({
      disposition: "acknowledged",
      reason: "event_not_supported",
    });
    expect(chief.accepted).toHaveLength(2);

    const unsupportedEnvelope = linqEnvelope("message.received", false);
    const unsupportedData = unsupportedEnvelope.data as { parts: unknown[] };
    unsupportedData.parts = [
      { type: "media", id: "video-one", filename: "clip.mov", mime_type: "video/quicktime", size_bytes: 8 },
    ];
    const unsupported = await app.inject({
      method: "POST",
      url: "/webhooks/linq",
      ...signedLinqWebhook(unsupportedEnvelope),
    });
    expect(unsupported.json()).toEqual({
      disposition: "acknowledged",
      reason: "message_has_no_supported_content",
    });
    expect(chief.accepted).toHaveLength(2);
  });

  it("leaves an unconfigured webhook unavailable", async () => {
    const chief = new RecordingChiefOfStaff();
    const unconfiguredApp = await buildApp(dependencies(chief, null));
    apps.push(unconfiguredApp);
    const request = {
      method: "POST" as const,
      url: "/webhooks/linq",
      ...signedLinqWebhook(linqEnvelope("message.received", false)),
    };

    expect((await unconfiguredApp.inject(request)).statusCode).toBe(503);
  });

  it("serves the mobile web app without shadowing API routes", async () => {
    const frontendRoot = await mkdtemp(join(tmpdir(), "florence-web-"));
    temporaryDirectories.push(frontendRoot);
    await writeFile(join(frontendRoot, "index.html"), "<!doctype html><title>Florence</title>", "utf8");
    const app = await buildApp(dependencies(new RecordingChiefOfStaff(), null), {
      frontendRoot,
      serveFrontend: true,
    });
    apps.push(app);

    const clientRoute = await app.inject({ method: "GET", url: "/onboarding/family" });
    const unknownApi = await app.inject({ method: "GET", url: "/api/unknown" });
    expect(clientRoute.statusCode).toBe(200);
    expect(clientRoute.headers["cache-control"]).toBe("no-store");
    expect(clientRoute.body).toContain("<title>Florence</title>");
    expect(unknownApi.statusCode).toBe(404);
  });
});
