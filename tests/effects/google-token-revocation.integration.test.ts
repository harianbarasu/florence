import { createHash, randomUUID } from "node:crypto";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { FlorenceApplication } from "../../src/application/index.js";
import { EffectOutbox } from "../../src/modules/effects/index.js";
import { PostgresSourceIntelligence } from "../../src/modules/sources/index.js";
import { SecretBox } from "../../src/shared/crypto.js";

const databaseUrl = process.env.TEST_DATABASE_URL;
const schema = process.env.TEST_POSTGRES_SCHEMA ?? "florence_v4";
const describeDatabase = databaseUrl ? describe : describe.skip;

describeDatabase("Google token revocation lifecycle", () => {
  let database: Sql<Record<string, never>>;
  const personIds: string[] = [];
  const secretBox = new SecretBox(
    "test-v1",
    JSON.stringify({ "test-v1": Buffer.alloc(32, 29).toString("base64") }),
  );

  beforeAll(async () => {
    database = postgres(databaseUrl as string, {
      max: 3,
      connection: { search_path: schema, TimeZone: "UTC" },
    });
    const constraints = await database<{ readonly definition: string }[]>`
      select pg_get_constraintdef(oid) as definition
      from pg_constraint
      where conrelid = 'integrations'::regclass and conname = 'integrations_status_check'
    `;
    if (!constraints[0]?.definition.includes("revocation_pending")) {
      throw new Error(`Expected Google token revocation migration in ${schema}`);
    }
  });

  afterEach(async () => {
    for (const personId of personIds.splice(0)) {
      const effects = await database<
        {
          readonly id: string;
          readonly authorization_decision_id: string;
          readonly action_intent_id: string | null;
        }[]
      >`
        select id, authorization_decision_id, action_intent_id from outbox
        where person_id = ${personId}
      `;
      const effectIds = effects.map((effect) => effect.id);
      const decisionIds = effects.map((effect) => effect.authorization_decision_id);
      const actionIntentIds = effects.flatMap((effect) =>
        effect.action_intent_id === null ? [] : [effect.action_intent_id],
      );
      if (effectIds.length > 0) {
        await database`delete from effect_receipts where outbox_id = any(${database.array(effectIds)}::uuid[])`;
        await database`delete from outbox where id = any(${database.array(effectIds)}::uuid[])`;
      }
      if (decisionIds.length > 0) {
        await database`
          delete from disclosure_decisions where id = any(${database.array(decisionIds)}::uuid[])
        `;
      }
      if (actionIntentIds.length > 0) {
        await database`
          delete from action_approvals
          where action_intent_id = any(${database.array(actionIntentIds)}::uuid[])
        `;
        await database`
          delete from action_intents where id = any(${database.array(actionIntentIds)}::uuid[])
        `;
      }
      await database`delete from audit_events where person_id = ${personId}`;
      await database`delete from integrations where person_id = ${personId}`;
      await database`delete from people where id = ${personId}`;
    }
  });

  afterAll(async () => {
    await database.end();
  });

  it("fences locally, persists no token in the effect, then retires the secret with its receipt", async () => {
    const personId = randomUUID();
    const integrationId = randomUUID();
    const now = new Date();
    personIds.push(personId);
    await database`
      insert into people (
        id, status, timezone, consented_at, registered_at, authority_version,
        control_epoch, created_at, updated_at
      ) values (
        ${personId}, 'registered', 'America/Los_Angeles', ${now}, ${now}, 1, 1, ${now}, ${now}
      )
    `;
    const storedCredential = secretBox.encrypt(
      JSON.stringify({ refreshToken: "provider-refresh-token" }),
      `florence:integration:${integrationId}:credentials`,
    );
    const storedLabel = secretBox.encrypt(
      JSON.stringify("person@example.test"),
      `florence:integration:${integrationId}:account-label`,
    );
    await database`
      insert into integrations (
        id, person_id, provider, external_subject_digest, account_kind, status,
        credential_ciphertext, credential_key_version,
        account_label_ciphertext, account_label_key_version, last_authorized_capabilities,
        control_epoch, connected_at, updated_at
      ) values (
        ${integrationId}, ${personId}, 'google',
        ${createHash("sha256").update(randomUUID()).digest("hex")}, 'personal_family', 'active',
        ${Buffer.from(JSON.stringify(storedCredential), "utf8")}, ${storedCredential.kid},
        ${Buffer.from(JSON.stringify(storedLabel), "utf8")}, ${storedLabel.kid},
        ${database.array(["mail", "calendar"])}::text[],
        1, ${now}, ${now}
      )
    `;

    const application = new FlorenceApplication(
      database as never,
      { defaults: { rawSourceRetentionDays: 30 } } as never,
      secretBox,
    );
    const disconnected = await application.process({
      kind: "web.command",
      actorPersonId: personId,
      command: { kind: "disconnect_integration", integrationId },
    });
    const pending = await database<
      {
        readonly status: string;
        readonly credential_ciphertext: Buffer | null;
        readonly control_epoch: number | string;
      }[]
    >`
      select status, credential_ciphertext, control_epoch from integrations where id = ${integrationId}
    `;
    expect(pending[0]).toMatchObject({ status: "revocation_pending", control_epoch: "2" });
    expect(pending[0]?.credential_ciphertext).not.toBeNull();

    await database`
      update outbox set status = 'ambiguous', last_error_code = 'legacy_terminal_revocation'
      where id = ${disconnected.ids.outboxId as string}
    `;
    await expect(
      application.process({
        kind: "web.command",
        actorPersonId: personId,
        command: { kind: "disconnect_integration", integrationId },
      }),
    ).resolves.toMatchObject({ accepted: true, duplicate: false, ids: { integrationId } });
    const recovered = await database<{ readonly status: string; readonly expires_at: Date }[]>`
      select effect.status, intent.expires_at
      from outbox effect join action_intents intent on intent.id = effect.action_intent_id
      where effect.id = ${disconnected.ids.outboxId as string}
    `;
    expect(recovered[0]?.status).toBe("retry");
    expect(recovered[0]?.expires_at.getUTCFullYear()).toBe(9999);

    const outbox = new EffectOutbox(database as never, secretBox);
    const [effect] = await outbox.claim("google-revocation-test", 1, new Date(now.getTime() + 1_000));
    expect(effect).toMatchObject({
      outboxId: disconnected.ids.outboxId,
      effectKind: "google.oauth_token.revoke",
      payload: { integrationId, expectedIntegrationControlEpoch: 2 },
    });
    expect(JSON.stringify(effect?.payload)).not.toContain("provider-refresh-token");
    const authorization = await outbox.reauthorizeGoogleTokenRevocation(
      effect as NonNullable<typeof effect>,
      async (credentials) => {
        expect(credentials.refreshToken).toBe("provider-refresh-token");
        return { outcome: "revoked" as const, httpStatus: 200 };
      },
      new Date(now.getTime() + 2_000),
    );
    expect(authorization.authorized).toBe(true);
    if (!authorization.authorized) throw new Error("Expected revocation authorization");
    expect(authorization).toMatchObject({ integrationId, expectedIntegrationControlEpoch: 2 });

    await application.process({
      kind: "google.oauth.revoke_receipt",
      outboxId: (effect as NonNullable<typeof effect>).outboxId,
      leaseToken: (effect as NonNullable<typeof effect>).leaseToken,
      idempotencyKey: (effect as NonNullable<typeof effect>).idempotencyKey,
      integrationId: authorization.integrationId,
      expectedIntegrationControlEpoch: authorization.expectedIntegrationControlEpoch,
      receipt: authorization.result,
      occurredAt: new Date(now.getTime() + 3_000).toISOString(),
    });

    const settled = await database<
      {
        readonly status: string;
        readonly credential_ciphertext: Buffer | null;
        readonly credential_key_version: string | null;
        readonly control_epoch: number | string;
        readonly effect_status: string;
        readonly intent_status: string;
        readonly receipt_status: string;
      }[]
    >`
      select integration.status, integration.credential_ciphertext,
        integration.credential_key_version, integration.control_epoch,
        effect.status as effect_status, intent.status as intent_status,
        receipt.status as receipt_status
      from integrations integration
      join outbox effect on effect.integration_id = integration.id
      join action_intents intent on intent.id = effect.action_intent_id
      join effect_receipts receipt on receipt.outbox_id = effect.id
      where integration.id = ${integrationId}
    `;
    expect(settled[0]).toEqual({
      status: "revoked",
      credential_ciphertext: null,
      credential_key_version: null,
      control_epoch: "3",
      effect_status: "confirmed",
      intent_status: "succeeded",
      receipt_status: "confirmed",
    });
    await expect(
      new PostgresSourceIntelligence(database as never, secretBox, {
        rawRetentionDays: 30,
      }).read({
        kind: "integration_profile",
        integrationId,
        personId,
        expectedControlEpoch: 3,
      }),
    ).resolves.toMatchObject({
      kind: "integration_profile",
      accountEmail: "person@example.test",
      integration: {
        status: "revoked",
        activeCapabilities: [],
        lastAuthorizedCapabilities: ["mail", "calendar"],
      },
    });
  });
});
