import { createHash, randomBytes, randomUUID } from "node:crypto";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { FlorenceApplication } from "../../src/application/florence-application.js";
import { googleAuthAttemptSecretPurpose, PostgresWebAuth } from "../../src/modules/auth/postgres-web-auth.js";
import { PostgresIdentityRelationships } from "../../src/modules/identity/index.js";
import { PostgresSourceIntelligence } from "../../src/modules/sources/index.js";
import { SecretBox } from "../../src/shared/crypto.js";

const databaseUrl = process.env.TEST_DATABASE_URL;
const schema = process.env.TEST_POSTGRES_SCHEMA ?? "florence_v4";
const describeDatabase = databaseUrl ? describe : describe.skip;

describeDatabase("Google provider-account identity binding", () => {
  let database: Sql<Record<string, never>>;
  let personIds: string[] = [];
  let authAttemptIds: string[] = [];
  const secretBox = new SecretBox("test", JSON.stringify({ test: randomBytes(32).toString("base64") }));

  beforeAll(async () => {
    database = postgres(databaseUrl as string, {
      max: 2,
      connection: { search_path: schema, TimeZone: "UTC" },
    });
    const tables = await database<{ readonly present: string | null }[]>`
      select to_regclass(${`${schema}.web_auth_attempts`})::text as present
    `;
    if (!tables[0]?.present) throw new Error(`Expected Google login migration in ${schema}`);
  });

  afterEach(async () => {
    if (authAttemptIds.length > 0) {
      await database`
        delete from web_auth_attempts
        where id = any(${database.array(authAttemptIds)}::uuid[])
      `;
      authAttemptIds = [];
    }
    if (personIds.length === 0) return;
    await database`delete from person_sessions where person_id = any(${database.array(personIds)}::uuid[])`;
    await database`delete from integrations where person_id = any(${database.array(personIds)}::uuid[])`;
    await database`
      delete from person_identities
      where person_id = any(${database.array(personIds)}::uuid[])
    `;
    await database`delete from people where id = any(${database.array(personIds)}::uuid[])`;
    personIds = [];
  });

  afterAll(async () => {
    await database.end();
  });

  it("is idempotent for one person, refreshes the verified label, relinks, and rejects another person", async () => {
    const now = new Date();
    personIds = [randomUUID(), randomUUID()];
    for (const personId of personIds) {
      await database`
        insert into people (
          id, status, timezone, consented_at, registered_at, created_at, updated_at
        ) values (
          ${personId}, 'registered', 'America/Los_Angeles', ${now}, ${now}, ${now}, ${now}
        )
      `;
    }

    const subject = "google-provider-subject";
    const subjectDigest = sha256(subject);
    const relationships = new PostgresIdentityRelationships(database, secretBox);
    const first = await relationships.bindProviderAccountIdentity({
      personId: personIds[0] as string,
      expectedPersonControlEpoch: 1,
      issuer: "google",
      subjectDigest,
      subject,
      verifiedEmail: "first@example.com",
      boundAt: now.toISOString(),
    });
    expect(first).toMatchObject({ duplicate: false });

    const replay = await relationships.bindProviderAccountIdentity({
      personId: personIds[0] as string,
      expectedPersonControlEpoch: 1,
      issuer: "google",
      subjectDigest,
      subject,
      verifiedEmail: "first@example.com",
      boundAt: new Date(now.getTime() + 1_000).toISOString(),
    });
    expect(replay).toMatchObject({
      duplicate: true,
      identity: { identityId: first.identity.identityId, identityAuthorityVersion: 1 },
    });

    const refreshed = await relationships.bindProviderAccountIdentity({
      personId: personIds[0] as string,
      expectedPersonControlEpoch: 1,
      issuer: "google",
      subjectDigest,
      subject,
      verifiedEmail: "new@example.com",
      boundAt: new Date(now.getTime() + 2_000).toISOString(),
    });
    expect(refreshed).toMatchObject({
      duplicate: false,
      identity: { identityId: first.identity.identityId, identityAuthorityVersion: 1 },
    });

    const sources = new PostgresSourceIntelligence(database, secretBox, { rawRetentionDays: 30 });
    await expect(
      sources.apply({
        kind: "connect_integration",
        personId: personIds[1] as string,
        provider: "google",
        externalSubjectDigest: subjectDigest,
        accountKind: "personal_family",
        activeCapabilities: ["calendar"],
        credentials: { accessToken: "test-access-token", accountEmail: "second@example.test" },
        expectedPersonControlEpoch: 1,
        reconnectTarget: null,
        connectedAt: new Date(now.getTime() + 2_500).toISOString(),
      }),
    ).rejects.toThrow("identity belongs to another Florence person");

    const sourceOnlySubject = "google-source-only-subject";
    const sourceOnlyDigest = sha256(sourceOnlySubject);
    const sourceOnlyConnection = await sources.apply({
      kind: "connect_integration",
      personId: personIds[0] as string,
      provider: "google",
      externalSubjectDigest: sourceOnlyDigest,
      accountKind: "personal_family",
      activeCapabilities: ["calendar"],
      credentials: { accessToken: "test-access-token", accountEmail: "source@example.test" },
      expectedPersonControlEpoch: 1,
      reconnectTarget: null,
      connectedAt: new Date(now.getTime() + 2_600).toISOString(),
    });
    if (sourceOnlyConnection.kind !== "integration_connected") {
      throw new Error("Google source did not connect");
    }
    await expect(
      sources.apply({
        kind: "connect_integration",
        personId: personIds[0] as string,
        provider: "google",
        externalSubjectDigest: subjectDigest,
        accountKind: "personal_family",
        activeCapabilities: ["calendar"],
        credentials: { accessToken: "wrong-account-token", accountEmail: "wrong@example.test" },
        expectedPersonControlEpoch: 1,
        reconnectTarget: {
          integrationId: sourceOnlyConnection.integrationId,
          expectedControlEpoch: sourceOnlyConnection.controlEpoch,
          externalSubjectDigest: sourceOnlyDigest,
        },
        connectedAt: new Date(now.getTime() + 2_650).toISOString(),
      }),
    ).rejects.toThrow("different provider account");
    const reconnected = await sources.apply({
      kind: "connect_integration",
      personId: personIds[0] as string,
      provider: "google",
      externalSubjectDigest: sourceOnlyDigest,
      accountKind: "personal_family",
      activeCapabilities: ["calendar"],
      credentials: { accessToken: "replacement-token", accountEmail: "source@example.test" },
      expectedPersonControlEpoch: 1,
      reconnectTarget: {
        integrationId: sourceOnlyConnection.integrationId,
        expectedControlEpoch: sourceOnlyConnection.controlEpoch,
        externalSubjectDigest: sourceOnlyDigest,
      },
      connectedAt: new Date(now.getTime() + 2_675).toISOString(),
    });
    expect(reconnected).toMatchObject({
      kind: "integration_connected",
      integrationId: sourceOnlyConnection.integrationId,
      controlEpoch: sourceOnlyConnection.controlEpoch + 1,
    });
    await expect(
      relationships.bindProviderAccountIdentity({
        personId: personIds[1] as string,
        expectedPersonControlEpoch: 1,
        issuer: "google",
        subjectDigest: sourceOnlyDigest,
        subject: sourceOnlySubject,
        verifiedEmail: "source-only@example.com",
        boundAt: new Date(now.getTime() + 2_700).toISOString(),
      }),
    ).rejects.toThrow("source already belongs to another Florence person");
    await database`
      update integrations
      set status = 'revoked', revoked_at = ${new Date(now.getTime() + 2_750)},
        credential_ciphertext = null, credential_key_version = null,
        control_epoch = control_epoch + 1
      where id = ${sourceOnlyConnection.integrationId}
    `;
    await expect(
      relationships.bindProviderAccountIdentity({
        personId: personIds[1] as string,
        expectedPersonControlEpoch: 1,
        issuer: "google",
        subjectDigest: sourceOnlyDigest,
        subject: sourceOnlySubject,
        verifiedEmail: "source-only@example.com",
        boundAt: new Date(now.getTime() + 2_800).toISOString(),
      }),
    ).rejects.toThrow("source already belongs to another Florence person");

    await expect(
      relationships.bindProviderAccountIdentity({
        personId: personIds[1] as string,
        expectedPersonControlEpoch: 1,
        issuer: "google",
        subjectDigest,
        subject,
        verifiedEmail: "new@example.com",
        boundAt: new Date(now.getTime() + 3_000).toISOString(),
      }),
    ).rejects.toThrow("already belongs to another Florence person");

    await database`
      update person_identities
      set status = 'revoked', revoked_at = ${new Date(now.getTime() + 4_000)},
        authority_version = authority_version + 1
      where id = ${first.identity.identityId}
    `;
    const relinked = await relationships.bindProviderAccountIdentity({
      personId: personIds[0] as string,
      expectedPersonControlEpoch: 1,
      issuer: "google",
      subjectDigest,
      subject,
      verifiedEmail: "new@example.com",
      boundAt: new Date(now.getTime() + 5_000).toISOString(),
    });
    expect(relinked).toMatchObject({
      duplicate: false,
      identity: {
        identityId: first.identity.identityId,
        identityStatus: "verified",
        identityAuthorityVersion: 3,
      },
    });

    const auth = new PostgresWebAuth(database, secretBox, "test-token-key");
    const loginAttempt = await auth.beginGoogleAuthAttempt(
      { mode: "login", returnPath: "/home" },
      new Date(now.getTime() + 6_000),
    );
    authAttemptIds.push(loginAttempt.attemptId);
    const completedLogin = await auth.completeGoogleLogin(
      {
        state: loginAttempt.state,
        browserBinding: loginAttempt.browserBinding,
        externalSubjectDigest: subjectDigest,
      },
      new Date(now.getTime() + 7_000),
    );
    expect(completedLogin).toMatchObject({
      kind: "signed_in",
      identityId: first.identity.identityId,
    });
    if (completedLogin.kind !== "signed_in") throw new Error("Google login did not resolve");
    const application = new FlorenceApplication(database as never, null as never, secretBox);
    const observedLogin = {
      kind: "auth.google_identity.login_observed" as const,
      sessionId: completedLogin.session.sessionId,
      personId: completedLogin.session.personId,
      identityId: completedLogin.identityId,
      externalSubjectDigest: subjectDigest,
      verifiedEmail: "returning@example.com",
    };
    await expect(application.process(observedLogin)).resolves.toMatchObject({
      duplicate: false,
      disposition: "google_identity_label_refreshed",
    });
    await expect(application.process(observedLogin)).resolves.toMatchObject({
      duplicate: true,
      disposition: "google_identity_login_confirmed",
    });

    const stored = await database<
      {
        readonly kind: string;
        readonly issuer: string;
        readonly authority_version: number | string;
        readonly subject_ciphertext: Buffer;
        readonly display_label_ciphertext: Buffer;
      }[]
    >`
      select kind, issuer, authority_version, subject_ciphertext, display_label_ciphertext
      from person_identities where id = ${first.identity.identityId}
    `;
    const identity = stored[0];
    expect(identity).toBeDefined();
    expect(identity?.kind).toBe("provider_account");
    expect(identity?.issuer).toBe("google");
    expect(Number(identity?.authority_version)).toBe(3);
    expect(
      secretBox
        .decrypt(
          JSON.parse(identity?.subject_ciphertext.toString("utf8") ?? "null") as unknown,
          `identity-subject:${first.identity.identityId}`,
        )
        .toString("utf8"),
    ).toBe(subject);
    expect(
      secretBox
        .decrypt(
          JSON.parse(identity?.display_label_ciphertext.toString("utf8") ?? "null") as unknown,
          `identity-display-label:${first.identity.identityId}`,
        )
        .toString("utf8"),
    ).toBe("returning@example.com");
  });

  it("retires consumed login secrets and sweeps settled attempts", async () => {
    const now = new Date("2000-01-01T00:00:00.000Z");
    const auth = new PostgresWebAuth(database, secretBox, "test-token-key");
    const attempt = await auth.beginGoogleAuthAttempt({ mode: "login", returnPath: "/home" }, now);
    authAttemptIds = [attempt.attemptId];

    await expect(
      auth.completeGoogleLogin(
        {
          state: attempt.state,
          browserBinding: attempt.browserBinding,
          externalSubjectDigest: sha256("unknown-google-subject"),
        },
        new Date(now.getTime() + 1_000),
      ),
    ).resolves.toEqual({ kind: "not_linked", returnPath: "/home" });

    const stored = await database<
      { readonly secret_ciphertext: Buffer; readonly consumed_at: Date | null }[]
    >`
      select secret_ciphertext, consumed_at
      from web_auth_attempts where id = ${attempt.attemptId}
    `;
    expect(stored[0]?.consumed_at).not.toBeNull();
    expect(
      JSON.parse(
        secretBox
          .decrypt(
            JSON.parse(stored[0]?.secret_ciphertext.toString("utf8") ?? "null") as unknown,
            googleAuthAttemptSecretPurpose(attempt.attemptId),
          )
          .toString("utf8"),
      ),
    ).toEqual({ consumed: true });

    await auth.sweepGoogleAuthAttempts(new Date(now.getTime() + 2_000), 50);
    const remaining = await database<{ readonly present: boolean }[]>`
      select exists(select 1 from web_auth_attempts where id = ${attempt.attemptId}) as present
    `;
    expect(remaining[0]?.present).toBe(false);
    authAttemptIds = [];
  });

  it("reuses one browser binding across concurrent login attempts and never substitutes a session", async () => {
    const now = new Date();
    const auth = new PostgresWebAuth(database, secretBox, "test-token-key");
    const first = await auth.beginGoogleAuthAttempt({ mode: "login", returnPath: "/home" }, now);
    const concurrent = await auth.beginGoogleAuthAttempt(
      { mode: "login", returnPath: "/home" },
      new Date(now.getTime() + 1_000),
      first.browserBinding,
    );
    authAttemptIds = [first.attemptId, concurrent.attemptId];
    expect(concurrent.browserBinding).toBe(first.browserBinding);

    const fakeLinkSession = {
      sessionId: randomUUID(),
      personId: randomUUID(),
      controlEpoch: 1,
    };
    await expect(
      auth.readGoogleAuthAttempt(
        first.state,
        { browserBinding: null, linkSession: fakeLinkSession },
        new Date(now.getTime() + 2_000),
      ),
    ).rejects.toThrow("browser that started it");
    await expect(
      auth.readGoogleAuthAttempt(
        first.state,
        { browserBinding: randomBytes(32).toString("base64url"), linkSession: fakeLinkSession },
        new Date(now.getTime() + 2_000),
      ),
    ).rejects.toThrow("browser that started it");
    await expect(
      auth.readGoogleAuthAttempt(
        first.state,
        { browserBinding: concurrent.browserBinding, linkSession: null },
        new Date(now.getTime() + 2_000),
      ),
    ).resolves.toMatchObject({
      mode: "login",
      browserBindingDigest: sha256(first.browserBinding),
    });
  });

  it("revalidates first-link onboarding and exact subsequent-link assurance", async () => {
    const now = new Date();
    const personId = randomUUID();
    const privateIdentityId = randomUUID();
    const sessionId = randomUUID();
    personIds = [personId];
    await database`
      insert into people (
        id, status, timezone, consented_at, registered_at, created_at, updated_at
      ) values (
        ${personId}, 'registered', 'America/Los_Angeles', ${now}, ${now}, ${now}, ${now}
      )
    `;
    await database`
      insert into person_identities (
        id, person_id, kind, issuer, subject_digest, status,
        observed_at, verified_at, created_at, updated_at
      ) values (
        ${privateIdentityId}, ${personId}, 'phone', 'linq', ${sha256(randomUUID())}, 'verified',
        ${now}, ${now}, ${now}, ${now}
      )
    `;
    await database`
      insert into person_sessions (
        id, person_id, session_digest, person_control_epoch,
        authentication_identity_id, authentication_identity_authority_version,
        assurance_kind, assurance_context, assurance_expires_at,
        idle_expires_at, absolute_expires_at, last_seen_at, created_at
      ) values (
        ${sessionId}, ${personId}, ${sha256(randomUUID())}, 1,
        ${privateIdentityId}, 1,
        'onboarding', ${database.json({})}, ${new Date(now.getTime() + 10 * 60_000)},
        ${new Date(now.getTime() + 60 * 60_000)}, ${new Date(now.getTime() + 2 * 60 * 60_000)},
        ${now}, ${now}
      )
    `;

    const auth = new PostgresWebAuth(database, secretBox, "test-token-key");
    const application = new FlorenceApplication(database as never, null as never, secretBox);
    const earlyNonce = randomBytes(32).toString("base64url");
    await expect(
      application.process({
        kind: "google.oauth.begin",
        personId,
        initiatingSessionId: sessionId,
        stateDigest: sha256(randomUUID()),
        nonce: earlyNonce,
        nonceDigest: sha256(earlyNonce),
        pkceVerifier: randomBytes(48).toString("base64url"),
        returnPath: "/onboarding",
        requestedCapabilities: ["mail", "calendar"],
        accountKind: "personal_family",
        reconnectTarget: null,
        expectedPersonControlEpoch: 1,
        expiresAt: new Date(now.getTime() + 10 * 60_000).toISOString(),
        createdAt: now.toISOString(),
      }),
    ).rejects.toThrow("Google is not the current family onboarding step");
    const firstAttempt = await auth.beginGoogleAuthAttempt(
      {
        mode: "link",
        personId,
        initiatingSessionId: sessionId,
        personControlEpoch: 1,
        returnPath: "/onboarding",
      },
      now,
    );
    authAttemptIds.push(firstAttempt.attemptId);
    const exactLinkSession = { sessionId, personId, controlEpoch: 1 };
    await expect(
      auth.readGoogleAuthAttempt(
        firstAttempt.state,
        { browserBinding: null, linkSession: exactLinkSession },
        new Date(now.getTime() + 1_000),
      ),
    ).resolves.toMatchObject({
      mode: "link",
      browserBindingDigest: sha256(firstAttempt.browserBinding),
    });
    await expect(
      auth.readGoogleAuthAttempt(
        firstAttempt.state,
        {
          browserBinding: randomBytes(32).toString("base64url"),
          linkSession: exactLinkSession,
        },
        new Date(now.getTime() + 1_000),
      ),
    ).resolves.toMatchObject({ mode: "link" });
    await expect(
      auth.readGoogleAuthAttempt(
        firstAttempt.state,
        {
          browserBinding: null,
          linkSession: { ...exactLinkSession, sessionId: randomUUID() },
        },
        new Date(now.getTime() + 1_000),
      ),
    ).rejects.toThrow("browser that started it");
    await expect(
      auth.readGoogleAuthAttempt(
        firstAttempt.state,
        {
          browserBinding: null,
          linkSession: { ...exactLinkSession, personId: randomUUID() },
        },
        new Date(now.getTime() + 1_000),
      ),
    ).rejects.toThrow("browser that started it");
    await expect(
      auth.readGoogleAuthAttempt(
        firstAttempt.state,
        {
          browserBinding: null,
          linkSession: { ...exactLinkSession, controlEpoch: 2 },
        },
        new Date(now.getTime() + 1_000),
      ),
    ).rejects.toThrow("browser that started it");
    await database`
      update person_sessions
      set assurance_kind = 'base', assurance_context = ${database.json({})}, assurance_expires_at = null
      where id = ${sessionId}
    `;
    await expect(
      auth.beginGoogleAuthAttempt({
        mode: "link",
        personId,
        initiatingSessionId: sessionId,
        personControlEpoch: 1,
        returnPath: "/onboarding",
      }),
    ).rejects.toThrow("fresh onboarding handoff");

    const firstSubject = `google-first-${randomUUID()}`;
    const firstBinding = {
      kind: "auth.google_identity.bind" as const,
      stateDigest: sha256(firstAttempt.state),
      browserBindingDigest: sha256(firstAttempt.browserBinding),
      externalSubjectDigest: sha256(firstSubject),
      externalSubject: firstSubject,
      verifiedEmail: "first@example.test",
      boundAt: now.toISOString(),
    };
    await expect(application.process(firstBinding)).rejects.toThrow(
      "Google identity link assurance is no longer current",
    );

    await database`
      update person_sessions
      set assurance_kind = 'onboarding', assurance_context = ${database.json({})},
        assurance_expires_at = ${new Date(now.getTime() + 10 * 60_000)}
      where id = ${sessionId}
    `;
    const firstBindingReceipt = await application.process(firstBinding);
    expect(firstBindingReceipt).toMatchObject({
      disposition: "google_identity_bound",
    });
    const firstGoogleIdentityId = firstBindingReceipt.ids.identityId;
    if (!firstGoogleIdentityId) throw new Error("Google identity binding did not return its identity");

    const sources = new PostgresSourceIntelligence(database, secretBox, { rawRetentionDays: 30 });
    const sourceConnection = await sources.apply({
      kind: "connect_integration",
      personId,
      provider: "google",
      externalSubjectDigest: firstBinding.externalSubjectDigest,
      accountKind: "personal_family",
      activeCapabilities: ["calendar"],
      credentials: { accessToken: "first-source-token", accountEmail: "owner@example.test" },
      expectedPersonControlEpoch: 1,
      reconnectTarget: null,
      connectedAt: new Date(now.getTime() + 1_000).toISOString(),
    });
    if (sourceConnection.kind !== "integration_connected") {
      throw new Error("Google source did not connect");
    }
    const connectorState = sha256(randomUUID());
    const connectorNonce = randomBytes(32).toString("base64url");
    await sources.apply({
      kind: "begin_oauth_attempt",
      personId,
      provider: "google",
      initiatingSessionId: sessionId,
      stateDigest: connectorState,
      nonce: connectorNonce,
      nonceDigest: sha256(connectorNonce),
      pkceVerifier: randomBytes(48).toString("base64url"),
      returnPath: "/sources",
      requestedCapabilities: ["calendar"],
      accountKind: "personal_family",
      reconnectTarget: {
        integrationId: sourceConnection.integrationId,
        expectedControlEpoch: sourceConnection.controlEpoch,
        externalSubjectDigest: firstBinding.externalSubjectDigest,
      },
      expectedPersonControlEpoch: 1,
      expiresAt: new Date(now.getTime() + 10 * 60_000).toISOString(),
      createdAt: now.toISOString(),
    });
    await expect(
      sources.read({
        kind: "oauth_attempt_access",
        provider: "google",
        stateDigest: connectorState,
        asOf: new Date(now.getTime() + 2_000).toISOString(),
      }),
    ).resolves.toMatchObject({
      reconnectTarget: {
        integrationId: sourceConnection.integrationId,
        expectedControlEpoch: sourceConnection.controlEpoch,
        externalSubjectDigest: firstBinding.externalSubjectDigest,
      },
    });

    const exactContext = { action: "link_google_identity", returnPath: "/link-account" };
    await database`
      update person_sessions
      set assurance_kind = 'account_controls',
        assurance_context = ${database.json({ returnPath: "/safety" })},
        assurance_expires_at = ${new Date(now.getTime() + 10 * 60_000)}
      where id = ${sessionId}
    `;
    await expect(
      auth.beginGoogleAuthAttempt({
        mode: "link",
        personId,
        initiatingSessionId: sessionId,
        personControlEpoch: 1,
        returnPath: "/link-account",
      }),
    ).rejects.toThrow("fresh exact private confirmation");

    await database`
      update person_sessions set assurance_context = ${database.json(exactContext)}
      where id = ${sessionId}
    `;
    const subsequentAttempt = await auth.beginGoogleAuthAttempt({
      mode: "link",
      personId,
      initiatingSessionId: sessionId,
      personControlEpoch: 1,
      returnPath: "/link-account",
    });
    authAttemptIds.push(subsequentAttempt.attemptId);
    const subsequentSubject = `google-subsequent-${randomUUID()}`;
    const subsequentBinding = {
      kind: "auth.google_identity.bind" as const,
      stateDigest: sha256(subsequentAttempt.state),
      browserBindingDigest: sha256(subsequentAttempt.browserBinding),
      externalSubjectDigest: sha256(subsequentSubject),
      externalSubject: subsequentSubject,
      verifiedEmail: "subsequent@example.test",
      boundAt: new Date().toISOString(),
    };
    await database`
      update person_sessions
      set assurance_context = ${database.json({ ...exactContext, unrelated: "value" })}
      where id = ${sessionId}
    `;
    await expect(application.process(subsequentBinding)).rejects.toThrow(
      "Google identity link assurance is no longer current",
    );
    await database`
      update person_sessions set assurance_context = ${database.json(exactContext)}
      where id = ${sessionId}
    `;
    const subsequentReceipt = await application.process(subsequentBinding);
    expect(subsequentReceipt).toMatchObject({
      disposition: "google_identity_bound",
    });
    const subsequentIdentityId = subsequentReceipt.ids.identityId;
    if (!subsequentIdentityId) throw new Error("Google identity binding did not return its identity");
    await expect(
      auth.beginGoogleAuthAttempt({
        mode: "link",
        personId,
        initiatingSessionId: sessionId,
        personControlEpoch: 1,
        returnPath: "/link-account",
      }),
    ).rejects.toThrow("fresh exact private confirmation");
    await database`
      update person_sessions
      set assurance_kind = 'account_controls',
        assurance_context = ${database.json({
          action: "revoke_login_identity",
          identityId: firstGoogleIdentityId,
          returnPath: "/safety",
        })},
        assurance_expires_at = ${new Date(now.getTime() + 10 * 60_000)}
      where id = ${sessionId}
    `;
    await expect(
      application.process({
        kind: "web.command",
        actorPersonId: personId,
        command: {
          kind: "revoke_login_identity",
          identityId: subsequentIdentityId,
          assuranceSessionId: sessionId,
        },
      }),
    ).rejects.toThrow("fresh private Florence confirmation");
    await database`
      update person_sessions
      set assurance_context = ${database.json({
        action: "revoke_login_identity",
        identityId: subsequentIdentityId,
        returnPath: "/safety",
      })}
      where id = ${sessionId}
    `;
    await expect(
      application.process({
        kind: "web.command",
        actorPersonId: personId,
        command: {
          kind: "revoke_login_identity",
          identityId: subsequentIdentityId,
          assuranceSessionId: sessionId,
        },
      }),
    ).resolves.toMatchObject({ disposition: "login_identity_revoked" });
    const revoked = await database<
      {
        readonly status: string;
        readonly subject_ciphertext: Buffer | null;
        readonly display_label_ciphertext: Buffer | null;
      }[]
    >`
      select status, subject_ciphertext, display_label_ciphertext
      from person_identities where id = ${subsequentIdentityId}
    `;
    expect(revoked[0]).toEqual({
      status: "revoked",
      subject_ciphertext: null,
      display_label_ciphertext: null,
    });
    const auditBeforeReplay = await database<{ readonly count: number }[]>`
      select count(*)::int as count from audit_events
      where event_type = 'google_identity_revoked' and target_id = ${subsequentIdentityId}
    `;
    await expect(
      application.process({
        kind: "web.command",
        actorPersonId: personId,
        command: {
          kind: "revoke_login_identity",
          identityId: subsequentIdentityId,
          assuranceSessionId: sessionId,
        },
      }),
    ).resolves.toMatchObject({ duplicate: true, disposition: "login_identity_revoked" });
    const auditAfterReplay = await database<{ readonly count: number }[]>`
      select count(*)::int as count from audit_events
      where event_type = 'google_identity_revoked' and target_id = ${subsequentIdentityId}
    `;
    expect(auditAfterReplay[0]?.count).toBe(auditBeforeReplay[0]?.count);

    await database`
      update person_sessions
      set assurance_context = ${database.json({
        action: "revoke_login_identity",
        identityId: firstGoogleIdentityId,
        returnPath: "/safety",
      })}
      where id = ${sessionId}
    `;
    await expect(
      application.process({
        kind: "web.command",
        actorPersonId: personId,
        command: {
          kind: "revoke_login_identity",
          identityId: firstGoogleIdentityId,
          assuranceSessionId: sessionId,
        },
      }),
    ).rejects.toThrow("Add another Google sign-in before removing your only sign-in");
    const remainingIdentity = await database<{ readonly status: string }[]>`
      select status from person_identities where id = ${firstGoogleIdentityId}
    `;
    expect(remainingIdentity[0]?.status).toBe("verified");
  });
});

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
