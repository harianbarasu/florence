import { createHash, randomBytes, randomUUID } from "node:crypto";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { googleAuthAttemptSecretPurpose } from "../../src/modules/auth/index.js";
import { createCoverageLoop, PostgresCoordination } from "../../src/modules/coordination/index.js";
import { PostgresDataControls, PostgresDataExporter } from "../../src/modules/data-controls/index.js";
import { PostgresIdentityRelationships } from "../../src/modules/identity/index.js";
import { PostgresSourceIntelligence } from "../../src/modules/sources/index.js";
import { SecretBox } from "../../src/shared/crypto.js";

const databaseUrl = process.env.TEST_DATABASE_URL;
const schema = process.env.TEST_POSTGRES_SCHEMA ?? "florence_v4";
const describeDatabase = databaseUrl ? describe : describe.skip;

describeDatabase("person data portability and deletion", () => {
  let database: Sql<Record<string, never>>;
  let personId: string | null = null;
  let additionalPersonIds: string[] = [];
  let householdIds: string[] = [];
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
    for (const householdId of householdIds) {
      await database`delete from households where id = ${householdId}`;
    }
    for (const cleanupPersonId of personId ? [personId, ...additionalPersonIds] : additionalPersonIds) {
      await database`
        delete from deletion_requests
        where requested_by_person_id = ${cleanupPersonId} or target_person_id = ${cleanupPersonId}
      `;
      await database`
        delete from revocation_tombstones
        where target_kind = 'person' and target_id_digest = ${sha256(cleanupPersonId)}
      `;
      await database`delete from integrations where person_id = ${cleanupPersonId}`;
      await database`delete from person_sessions where person_id = ${cleanupPersonId}`;
      await database`delete from memory_records where owner_person_id = ${cleanupPersonId}`;
      await database`delete from person_identities where person_id = ${cleanupPersonId}`;
      await database`delete from people where id = ${cleanupPersonId}`;
    }
    personId = null;
    additionalPersonIds = [];
    householdIds = [];
  });

  afterAll(async () => {
    await database.end();
  });

  it("exports inspectable owner data without credentials, then erases identity and auth secrets durably", async () => {
    const now = new Date("2026-08-10T02:00:00.000Z");
    personId = randomUUID();
    const phoneIdentityId = randomUUID();
    const sessionId = randomUUID();
    const phone = "+14155550123";
    const googleSubject = "google-owner-subject";
    const googleEmail = "parent@example.com";

    await database`
      insert into people (
        id, status, timezone, consented_at, registered_at, created_at, updated_at
      ) values (
        ${personId}, 'registered', 'America/Los_Angeles', ${now}, ${now}, ${now}, ${now}
      )
    `;
    const encryptedPhone = secretBox.encrypt(phone, `identity-subject:${phoneIdentityId}`);
    await database`
      insert into person_identities (
        id, person_id, kind, issuer, subject_digest, subject_ciphertext, subject_key_version,
        status, authority_version, observed_at, verified_at, created_at, updated_at
      ) values (
        ${phoneIdentityId}, ${personId}, 'phone', 'linq', ${sha256(phone)},
        ${Buffer.from(JSON.stringify(encryptedPhone), "utf8")}, ${encryptedPhone.kid},
        'verified', 1, ${now}, ${now}, ${now}, ${now}
      )
    `;
    const googleIdentity = await new PostgresIdentityRelationships(
      database,
      secretBox,
    ).bindProviderAccountIdentity({
      personId,
      expectedPersonControlEpoch: 1,
      issuer: "google",
      subjectDigest: sha256(googleSubject),
      subject: googleSubject,
      verifiedEmail: googleEmail,
      boundAt: now.toISOString(),
    });

    await database`
      insert into person_sessions (
        id, person_id, session_digest, person_control_epoch,
        authentication_identity_id, authentication_identity_authority_version,
        assurance_kind, idle_expires_at, absolute_expires_at, last_seen_at, created_at
      ) values (
        ${sessionId}, ${personId}, ${sha256("session-token")}, 1,
        ${phoneIdentityId}, 1, 'base', ${new Date(now.getTime() + 3_600_000)},
        ${new Date(now.getTime() + 7_200_000)}, ${now}, ${now}
      )
    `;

    const integration = await new PostgresSourceIntelligence(database, secretBox, {
      rawRetentionDays: 30,
    }).apply({
      kind: "connect_integration",
      personId,
      provider: "google",
      externalSubjectDigest: sha256(googleSubject),
      accountKind: "work",
      activeCapabilities: ["mail", "calendar"],
      credentials: {
        accountEmail: ` ${googleEmail.toUpperCase()} `,
        accessToken: "access-secret-must-not-export",
        refreshToken: "refresh-secret-must-not-export",
      },
      expectedPersonControlEpoch: 1,
      reconnectTarget: null,
      connectedAt: now.toISOString(),
    });
    if (integration.kind !== "integration_connected") throw new Error("Integration did not connect");
    const storedIntegration = await database<
      {
        readonly credential_ciphertext: Buffer;
        readonly account_label_ciphertext: Buffer;
        readonly last_authorized_capabilities: readonly string[];
      }[]
    >`
      select credential_ciphertext, account_label_ciphertext, last_authorized_capabilities
      from integrations where id = ${integration.integrationId}
    `;
    const storedCredentials = JSON.parse(
      secretBox
        .decrypt(
          JSON.parse(storedIntegration[0]?.credential_ciphertext.toString("utf8") ?? "{}"),
          `florence:integration:${integration.integrationId}:credentials`,
        )
        .toString("utf8"),
    ) as Record<string, unknown>;
    const storedAccountLabel = JSON.parse(
      secretBox
        .decrypt(
          JSON.parse(storedIntegration[0]?.account_label_ciphertext.toString("utf8") ?? "{}"),
          `florence:integration:${integration.integrationId}:account-label`,
        )
        .toString("utf8"),
    ) as unknown;
    expect(storedCredentials).not.toHaveProperty("accountEmail");
    expect(storedAccountLabel).toBe(googleEmail);
    expect(storedIntegration[0]?.last_authorized_capabilities).toEqual(["mail", "calendar"]);
    const grantId = randomUUID();
    await database`
      insert into integration_grants (
        id, integration_id, grant_kind, scope, status, version, created_at
      ) values (
        ${grantId}, ${integration.integrationId}, 'calendar_privacy',
        ${database.json({ mode: "availability_only" })}, 'active', 1, ${now}
      )
    `;

    const authSecret = secretBox.encrypt(
      JSON.stringify({ pkceVerifier: "v".repeat(48), nonce: "n".repeat(32) }),
      googleAuthAttemptSecretPurpose(sessionId),
    );
    await database`
      insert into web_auth_attempts (
        id, provider, mode, state_digest, browser_binding_digest,
        secret_ciphertext, secret_key_version, person_id, initiating_session_id,
        person_control_epoch, return_path, expires_at, created_at
      ) values (
        ${sessionId}, 'google', 'link', ${sha256("web-state")}, ${sha256("browser-binding")},
        ${Buffer.from(JSON.stringify(authSecret), "utf8")}, ${authSecret.kid},
        ${personId}, ${sessionId}, 1, '/sources', ${new Date(now.getTime() + 600_000)}, ${now}
      )
    `;
    const handoffId = randomUUID();
    await database`
      insert into auth_handoffs (
        id, person_id, private_identity_id, private_conversation_id, token_digest,
        purpose, identity_authority_version, expires_at, created_at
      ) values (
        ${handoffId}, ${personId}, ${phoneIdentityId}, null, ${sha256("handoff-token")},
        'web_sign_in', 1, ${new Date(now.getTime() + 600_000)}, ${now}
      )
    `;

    const exported = (await new PostgresDataExporter(database, secretBox, 30).exportPerson(
      personId,
    )) as PersonExport;
    expect(exported.identities).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: phoneIdentityId, kind: "phone", value: phone }),
        expect.objectContaining({
          id: googleIdentity.identity.identityId,
          kind: "provider_account",
          label: googleEmail,
        }),
      ]),
    );
    expect(exported.integrations).toEqual([
      expect.objectContaining({
        id: integration.integrationId,
        accountKind: "work",
        accountEmail: googleEmail,
        capabilities: expect.arrayContaining([
          expect.objectContaining({ capability: "mail", status: "active" }),
          expect.objectContaining({ capability: "calendar", status: "active" }),
        ]),
        grants: [
          expect.objectContaining({
            id: grantId,
            kind: "calendar_privacy",
            scope: { mode: "availability_only" },
            status: "active",
          }),
        ],
      }),
    ]);
    expect(Object.keys(exported.integrations[0] ?? {})).not.toContain("credentials");
    const serializedExport = JSON.stringify(exported);
    expect(serializedExport).not.toContain("access-secret-must-not-export");
    expect(serializedExport).not.toContain("refresh-secret-must-not-export");
    expect(serializedExport).not.toContain('"credentials":');
    expect(serializedExport).not.toContain('"credentialCiphertext":');
    expect(serializedExport).not.toContain('"accessToken":');
    expect(serializedExport).not.toContain('"refreshToken":');

    const controls = new PostgresDataControls(database, secretBox);
    const deletion = await controls.deletePerson({
      actorPersonId: personId,
      deletedAt: new Date(now.getTime() + 300_000),
    });
    expect(deletion).toMatchObject({ duplicate: false });
    expect(deletion.receiptDigest).toMatch(/^[a-f0-9]{64}$/u);

    const identities = await database<
      {
        readonly status: string;
        readonly subject_ciphertext: Buffer | null;
        readonly subject_key_version: string | null;
        readonly display_label_ciphertext: Buffer | null;
        readonly display_label_key_version: string | null;
      }[]
    >`
      select status, subject_ciphertext, subject_key_version,
        display_label_ciphertext, display_label_key_version
      from person_identities where person_id = ${personId}
    `;
    expect(identities).toHaveLength(2);
    expect(identities).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          status: "revoked",
          subject_ciphertext: null,
          subject_key_version: null,
          display_label_ciphertext: null,
          display_label_key_version: null,
        }),
      ]),
    );
    expect(
      identities.every(
        (identity) =>
          identity.subject_ciphertext === null &&
          identity.subject_key_version === null &&
          identity.display_label_ciphertext === null &&
          identity.display_label_key_version === null,
      ),
    ).toBe(true);

    const authRows = await database<
      {
        readonly web_attempts: number;
        readonly handoffs: number;
        readonly integration_secrets: number;
        readonly integration_labels: number;
      }[]
    >`
      select
        (select count(*)::int from web_auth_attempts where person_id = ${personId}) as web_attempts,
        (select count(*)::int from auth_handoffs where person_id = ${personId}) as handoffs,
        (select count(*)::int from integrations
          where person_id = ${personId} and credential_ciphertext is not null) as integration_secrets,
        (select count(*)::int from integrations
          where person_id = ${personId} and account_label_ciphertext is not null) as integration_labels
    `;
    expect(authRows[0]).toEqual({
      web_attempts: 0,
      handoffs: 0,
      integration_secrets: 0,
      integration_labels: 0,
    });

    const persistedReceipt = await database<
      { readonly status: string; readonly receipt_digest: string | null }[]
    >`
      select status, receipt_digest from deletion_requests where id = ${deletion.deletionRequestId}
    `;
    expect(persistedReceipt[0]).toEqual({
      status: "completed",
      receipt_digest: deletion.receiptDigest,
    });
    await expect(
      controls.deletePerson({ actorPersonId: personId, deletedAt: new Date(now.getTime() + 600_000) }),
    ).resolves.toEqual({ ...deletion, duplicate: true });
  });

  it("keeps own membership history but excludes shared data after leaving or before caregiver review", async () => {
    const now = new Date("2026-08-10T04:00:00.000Z");
    personId = randomUUID();
    const adultPersonId = randomUUID();
    const childPersonId = randomUUID();
    const actorIdentityId = randomUUID();
    additionalPersonIds = [adultPersonId, childPersonId];

    const leftHouseholdId = randomUUID();
    const limitedHouseholdId = randomUUID();
    householdIds = [leftHouseholdId, limitedHouseholdId];
    const leftMembershipId = randomUUID();
    const limitedMembershipId = randomUUID();

    await database`
      insert into people (
        id, status, timezone, consented_at, registered_at, created_at, updated_at
      ) values
        (${personId}, 'registered', 'America/Los_Angeles', ${now}, ${now}, ${now}, ${now}),
        (${adultPersonId}, 'registered', 'America/Los_Angeles', ${now}, ${now}, ${now}, ${now}),
        (${childPersonId}, 'registered', 'America/Los_Angeles', ${now}, ${now}, ${now}, ${now})
    `;
    await database`
      insert into person_identities (
        id, person_id, kind, issuer, subject_digest, status,
        authority_version, observed_at, verified_at, created_at, updated_at
      ) values (
        ${actorIdentityId}, ${personId}, 'phone', 'linq', ${sha256(`actor:${personId}`)},
        'verified', 1, ${now}, ${now}, ${now}, ${now}
      )
    `;
    await database`
      insert into households (
        id, timezone, status, membership_version, control_epoch, created_at, updated_at
      ) values
        (${leftHouseholdId}, 'America/Los_Angeles', 'active', 1, 1, ${now}, ${now}),
        (${limitedHouseholdId}, 'America/Los_Angeles', 'active', 1, 1, ${now}, ${now})
    `;
    await database`
      insert into household_memberships (
        id, household_id, person_id, role, status, consented_at, joined_at, ended_at,
        version, created_at, updated_at
      ) values
        (${leftMembershipId}, ${leftHouseholdId}, ${personId}, 'caregiver', 'left', ${now},
          ${new Date(now.getTime() - 86_400_000)}, ${now}, 1, ${now}, ${now}),
        (${randomUUID()}, ${leftHouseholdId}, ${adultPersonId}, 'steward', 'active', ${now},
          ${now}, null, 1, ${now}, ${now}),
        (${randomUUID()}, ${leftHouseholdId}, ${childPersonId}, 'dependent', 'active', ${now},
          ${now}, null, 1, ${now}, ${now}),
        (${limitedMembershipId}, ${limitedHouseholdId}, ${personId}, 'caregiver', 'active', ${now},
          ${now}, null, 1, ${now}, ${now}),
        (${randomUUID()}, ${limitedHouseholdId}, ${adultPersonId}, 'steward', 'active', ${now},
          ${now}, null, 1, ${now}, ${now}),
        (${randomUUID()}, ${limitedHouseholdId}, ${childPersonId}, 'dependent', 'active', ${now},
          ${now}, null, 1, ${now}, ${now})
    `;
    await database`
      insert into membership_capabilities (membership_id, capability, scope, status, granted_at)
      values
        (${leftMembershipId}, 'household.read', '{}'::jsonb, 'active', ${now}),
        (${limitedMembershipId}, 'household.read', '{}'::jsonb, 'active', ${now})
    `;
    await database`
      insert into membership_onboarding (
        membership_id, completed_by_person_id, completed_membership_version,
        completed_profile_review_version, completed_household_intake_version,
        completed_google_decision, completed_at, version, created_at, updated_at
      ) values (
        ${limitedMembershipId}, ${personId}, 1, 1, 0, 'limited', ${now}, 1, ${now}, ${now}
      )
    `;
    await database`
      insert into household_onboarding_intakes (
        household_id, child_roster_reviewed_by_person_id, child_roster_reviewed_at,
        child_roster_household_membership_version, version, created_at, updated_at
      ) values (
        ${limitedHouseholdId}, ${adultPersonId}, ${now}, 1, 1, ${now}, ${now}
      )
    `;

    const personalMemoryId = await seedMemory(database, secretBox, {
      scopeKind: "person",
      personId,
      householdId: null,
      memoryKey: "preference:private",
      content: { preference: "private owner memory" },
      now,
    });
    const limitedHouseholdMemoryId = await seedMemory(database, secretBox, {
      scopeKind: "household",
      personId,
      householdId: limitedHouseholdId,
      memoryKey: "child:private-history",
      content: { fact: "unreviewed shared child history" },
      now,
    });

    const leftConversation = await seedCoverageConversation(database, secretBox, {
      householdId: leftHouseholdId,
      viewerPersonId: personId,
      viewerIdentityId: actorIdentityId,
      label: "left household private coverage",
      now,
    });
    const limitedConversation = await seedCoverageConversation(database, secretBox, {
      householdId: limitedHouseholdId,
      viewerPersonId: personId,
      viewerIdentityId: actorIdentityId,
      label: "unreviewed child coverage",
      now,
    });

    const personalAuditId = randomUUID();
    const leftAuditId = randomUUID();
    const limitedAuditId = randomUUID();
    await database`
      insert into audit_events (
        id, household_id, person_id, conversation_id, sequence, actor_kind,
        event_type, target_type, reason_codes, evidence_refs, occurred_at
      ) values
        (${personalAuditId}, null, ${personId}, null, 1, 'application',
          'person.private.saved', 'person', '{}', '[]'::jsonb, ${now}),
        (${leftAuditId}, ${leftHouseholdId}, ${personId}, ${leftConversation.conversationId}, 1,
          'application', 'left.household.secret', 'coverage_loop', '{}', '[]'::jsonb, ${now}),
        (${limitedAuditId}, ${limitedHouseholdId}, ${personId},
          ${limitedConversation.conversationId}, 1, 'application',
          'limited.household.secret', 'coverage_loop', '{}', '[]'::jsonb, ${now})
    `;

    const exported = (await new PostgresDataExporter(database, secretBox, 30).exportPerson(
      personId,
    )) as PersonExport;

    expect(exported.memberships).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ household_id: leftHouseholdId, status: "left" }),
        expect.objectContaining({ household_id: limitedHouseholdId, status: "active" }),
      ]),
    );
    expect(
      exported.relationships.filter((relationship) => relationship.householdId === leftHouseholdId),
    ).toEqual([]);
    expect(
      exported.relationships
        .filter((relationship) => relationship.householdId === limitedHouseholdId)
        .map((relationship) => relationship.personId),
    ).toEqual(expect.arrayContaining([personId, adultPersonId]));
    expect(exported.relationships).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ householdId: limitedHouseholdId, personId: childPersonId }),
      ]),
    );
    expect(exported.coverageLoops).toEqual([]);
    expect(exported.memories).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: personalMemoryId,
          scopeKind: "person",
          currentRevision: expect.objectContaining({
            content: { preference: "private owner memory" },
          }),
        }),
      ]),
    );
    expect(exported.memories.map((memory) => memory.id)).not.toContain(limitedHouseholdMemoryId);
    expect(exported.audit.map((event) => event.id)).toContain(personalAuditId);
    expect(exported.audit.map((event) => event.id)).not.toEqual(
      expect.arrayContaining([leftAuditId, limitedAuditId]),
    );
  });
});

interface PersonExport {
  readonly identities: readonly {
    readonly id: string;
    readonly kind: string;
    readonly value: string | null;
    readonly label: string | null;
  }[];
  readonly integrations: readonly Record<string, unknown>[];
  readonly memberships: readonly {
    readonly household_id: string;
    readonly status: string;
  }[];
  readonly relationships: readonly {
    readonly householdId: string;
    readonly personId: string;
  }[];
  readonly coverageLoops: readonly { readonly loopId: string }[];
  readonly memories: readonly {
    readonly id: string;
    readonly scopeKind: string;
    readonly currentRevision: { readonly content: unknown };
  }[];
  readonly audit: readonly { readonly id: string }[];
}

async function seedMemory(
  database: Sql<Record<string, never>>,
  secretBox: SecretBox,
  input: {
    readonly scopeKind: "person" | "household";
    readonly personId: string;
    readonly householdId: string | null;
    readonly memoryKey: string;
    readonly content: unknown;
    readonly now: Date;
  },
): Promise<string> {
  const memoryId = randomUUID();
  const revisionId = randomUUID();
  const plaintext = JSON.stringify(input.content);
  const encrypted = secretBox.encrypt(plaintext, `florence:memory-revision:${revisionId}:content`);
  await database`
    insert into memory_records (
      id, scope_kind, owner_person_id, household_id, conversation_id,
      memory_key, status, current_revision_id, version, created_at, updated_at
    ) values (
      ${memoryId}, ${input.scopeKind},
      ${input.scopeKind === "person" ? input.personId : null}, ${input.householdId}, null,
      ${input.memoryKey}, 'accepted', null, 1, ${input.now}, ${input.now}
    )
  `;
  await database`
    insert into memory_revisions (
      id, memory_record_id, revision, content_digest, content_ciphertext,
      content_key_version, scope_digest, evidence_refs, accepted_by_person_id,
      effective_at, ended_at
    ) values (
      ${revisionId}, ${memoryId}, 1, ${sha256(plaintext)},
      ${Buffer.from(JSON.stringify(encrypted), "utf8")}, ${encrypted.kid},
      ${sha256(`${input.scopeKind}:${input.householdId ?? input.personId}`)},
      '[]'::jsonb, ${input.personId}, ${input.now}, null
    )
  `;
  await database`
    update memory_records set current_revision_id = ${revisionId}
    where id = ${memoryId}
  `;
  return memoryId;
}

async function seedCoverageConversation(
  database: Sql<Record<string, never>>,
  secretBox: SecretBox,
  input: {
    readonly householdId: string;
    readonly viewerPersonId: string;
    readonly viewerIdentityId: string;
    readonly label: string;
    readonly now: Date;
  },
): Promise<{ readonly conversationId: string; readonly loopId: string }> {
  const conversationId = randomUUID();
  const participantEpochId = randomUUID();
  const participantSetDigest = sha256(`participants:${conversationId}`);
  await database`
    insert into conversations (
      id, household_id, kind, purpose, status, created_at, updated_at
    ) values (
      ${conversationId}, ${input.householdId}, 'group', ${input.label}, 'active',
      ${input.now}, ${input.now}
    )
  `;
  await database`
    insert into participant_epochs (
      id, conversation_id, sequence, participant_set_digest, authority_digest,
      change_reason, started_at
    ) values (
      ${participantEpochId}, ${conversationId}, 1, ${participantSetDigest},
      ${sha256(`authority:${conversationId}`)}, 'test fixture', ${input.now}
    )
  `;
  await database`
    insert into epoch_participants (
      participant_epoch_id, person_identity_id, person_id,
      registration_status, consented_at, added_at
    ) values (
      ${participantEpochId}, ${input.viewerIdentityId}, ${input.viewerPersonId},
      'registered', ${input.now}, ${input.now}
    )
  `;
  await database`
    insert into participant_policies (
      id, conversation_id, person_id, version, status,
      allow_content_processing, allow_direct_responses, allow_proactive_writes,
      retention_seconds, changed_by_person_id, effective_at
    ) values (
      ${randomUUID()}, ${conversationId}, ${input.viewerPersonId}, 1, 'active',
      true, true, false, 2592000, ${input.viewerPersonId}, ${input.now}
    )
  `;
  await database`
    update conversations set current_epoch_id = ${participantEpochId}
    where id = ${conversationId}
  `;

  const loopId = randomUUID();
  await new PostgresCoordination(database, secretBox).create(
    createCoverageLoop({
      loopId,
      householdId: input.householdId,
      minimumSharedMeaning: input.label,
      unresolvedFacts: [],
      proposedHolderPersonId: null,
      timing: {
        timeZone: "America/Los_Angeles",
        localDate: input.now.toISOString().slice(0, 10),
        eventAt: new Date(input.now.getTime() + 3_600_000).toISOString(),
        deadlineAt: null,
        preparationMinutes: 0,
        travelMinutes: 0,
        earliestUsefulAt: input.now.toISOString(),
        lastResponsibleAt: new Date(input.now.getTime() + 1_800_000).toISOString(),
        resolutionPolicy: "wall_clock_compatible",
      },
      notificationMode: "exceptions_only",
      destination: {
        conversationId,
        participantEpochId,
        participantSetDigest,
        audience: "group",
      },
      sourceEvidenceRefs: [],
      occurredAt: input.now.toISOString(),
    }),
  );
  return { conversationId, loopId };
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
