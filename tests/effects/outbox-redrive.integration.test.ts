import { createHash, randomUUID } from "node:crypto";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { EffectOutbox } from "../../src/modules/effects/index.js";
import { canonicalJson } from "../../src/shared/canonical-json.js";
import { SecretBox } from "../../src/shared/crypto.js";

const databaseUrl = process.env.TEST_DATABASE_URL;
const schema = process.env.TEST_POSTGRES_SCHEMA ?? "florence_v4";
const describeDatabase = databaseUrl ? describe : describe.skip;

describeDatabase("effect delivery redrive", () => {
  let database: Sql<Record<string, never>>;
  const prefixes: string[] = [];
  const secretBox = new SecretBox(
    "test-v1",
    JSON.stringify({ "test-v1": Buffer.alloc(32, 11).toString("base64") }),
  );

  beforeAll(async () => {
    database = postgres(databaseUrl as string, {
      max: 3,
      connection: { search_path: schema, TimeZone: "UTC" },
    });
    const columns = await database<{ present: string | null }[]>`
      select column_name as present from information_schema.columns
      where table_schema = ${schema} and table_name = 'outbox' and column_name = 'redrive_root_id'
    `;
    if (!columns[0]?.present) throw new Error(`Expected redrive migration in ${schema}`);
  });

  afterEach(async () => {
    for (const prefix of prefixes.splice(0)) {
      const effects = await database<{ id: string; authorization_decision_id: string }[]>`
        select id, authorization_decision_id from outbox
        where idempotency_key like ${`${prefix}%`}
          or redrive_root_id in (select id from outbox where idempotency_key like ${`${prefix}%`})
      `;
      if (effects.length === 0) continue;
      const effectIds = effects.map((effect) => effect.id);
      const decisionIds = effects.map((effect) => effect.authorization_decision_id);
      await database`delete from effect_receipts where outbox_id = any(${database.array(effectIds)}::uuid[])`;
      await database`delete from outbox where redrive_root_id = any(${database.array(effectIds)}::uuid[])`;
      await database`delete from outbox where id = any(${database.array(effectIds)}::uuid[])`;
      await database`
        delete from disclosure_decisions
        where id = any(${database.array(decisionIds)}::uuid[])
      `;
    }
  });

  afterAll(async () => {
    await database.end();
  });

  it("creates one fenced successor while preserving the failed attempt and blocks revoked authority", async () => {
    // The database trigger evaluates expiry against its real clock, so a fixed
    // wall-clock fixture eventually becomes invalid even though the behavior
    // under test is unrelated to expiration.
    const now = new Date();
    const prefix = `redrive-test:${randomUUID()}`;
    prefixes.push(prefix);
    const outbox = new EffectOutbox(database, secretBox);
    const root = await outbox.authorizeAndEnqueue(
      {
        effectKind: "linq.message",
        idempotencyKey: `${prefix}:root`,
        data: { notice: "coverage_closed" },
        policy: { operation: "coverage_closure" },
        target: { providerChatId: randomUUID() },
        payload: {
          providerChatId: randomUUID(),
          expectedProviderParticipantDigest: `linq-v1:${"a".repeat(64)}`,
          text: "Covered.",
        },
        reasonCodes: ["test_authority"],
        authorizationExpiresAt: new Date(now.getTime() + 24 * 60 * 60_000),
      },
      now,
    );
    const failedReceipt = canonicalJson({ outcome: "failed", providerMessageId: randomUUID() });
    await database`
      insert into effect_receipts (
        id, outbox_id, idempotency_key, provider_receipt_id, status,
        receipt_digest, occurred_at, reconciled_at, error_code
      ) values (
        ${randomUUID()}, ${root.outboxId}, ${`${prefix}:root`}, ${randomUUID()}, 'failed',
        ${createHash("sha256").update(failedReceipt).digest("hex")}, ${now}, ${now},
        'linq_delivery_failed'
      )
    `;
    await database`
      update outbox set status = 'dead', last_error_code = 'linq_delivery_failed', updated_at = ${now}
      where id = ${root.outboxId}
    `;

    const redriveAt = new Date(now.getTime() + 61_000);
    const concurrent = await Promise.all([
      outbox.redriveFailed(redriveAt, 20),
      outbox.redriveFailed(redriveAt, 20),
    ]);
    expect(concurrent.reduce((sum, count) => sum + count, 0)).toBe(1);
    expect(await outbox.redriveFailed(redriveAt, 20)).toBe(0);

    const attempts = await database<
      {
        id: string;
        status: string;
        idempotency_key: string;
        redrive_root_id: string | null;
        redrive_sequence: number | null;
        payload_digest: string;
      }[]
    >`
      select id, status, idempotency_key, redrive_root_id, redrive_sequence, payload_digest
      from outbox where id = ${root.outboxId} or redrive_root_id = ${root.outboxId}
      order by coalesce(redrive_sequence, 0)
    `;
    expect(attempts).toHaveLength(2);
    expect(attempts[0]).toMatchObject({ id: root.outboxId, status: "dead", redrive_root_id: null });
    expect(attempts[1]).toMatchObject({
      status: "pending",
      redrive_root_id: root.outboxId,
      redrive_sequence: 1,
    });
    expect(attempts[1]?.idempotency_key).not.toBe(attempts[0]?.idempotency_key);
    expect(attempts[1]?.payload_digest).toBe(attempts[0]?.payload_digest);
    const receipts = await database<{ outbox_id: string; count: number }[]>`
      select outbox_id, count(*)::int as count from effect_receipts
      where outbox_id = any(${database.array(attempts.map((attempt) => attempt.id))}::uuid[])
      group by outbox_id
    `;
    expect(receipts).toEqual([{ outbox_id: root.outboxId, count: 1 }]);

    const blockedPrefix = `${prefix}:revoked`;
    const blocked = await outbox.authorizeAndEnqueue(
      {
        effectKind: "linq.message",
        idempotencyKey: blockedPrefix,
        data: { notice: "group_ready" },
        policy: { operation: "private_group_notice" },
        target: { providerChatId: randomUUID() },
        payload: {
          providerChatId: randomUUID(),
          expectedProviderParticipantDigest: `linq-v1:${"b".repeat(64)}`,
          text: "Your group is ready.",
        },
        reasonCodes: ["test_authority"],
        authorizationExpiresAt: new Date(now.getTime() + 24 * 60 * 60_000),
      },
      now,
    );
    await database`
      update outbox set status = 'dead', last_error_code = 'linq_delivery_failed', updated_at = ${now}
      where id = ${blocked.outboxId}
    `;
    await database`
      update disclosure_decisions set revoked_at = ${new Date(now.getTime() + 1_000)}
      where id = (select authorization_decision_id from outbox where id = ${blocked.outboxId})
    `;
    expect(await outbox.redriveFailed(redriveAt, 20)).toBe(0);
  });
});
