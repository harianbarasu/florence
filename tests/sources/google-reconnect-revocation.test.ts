import { describe, expect, it } from "vitest";
import type { Database } from "../../src/db/client.js";
import { PostgresSourceIntelligence } from "../../src/modules/sources/index.js";
import type { EncryptedValue } from "../../src/shared/crypto.js";
import { SecretBox } from "../../src/shared/crypto.js";

const personId = "550e8400-e29b-41d4-a716-446655440000";
const integrationId = "550e8400-e29b-41d4-a716-446655440001";
const attemptId = "550e8400-e29b-41d4-a716-446655440002";
const externalSubjectDigest = "a".repeat(64);
const now = new Date("2026-08-09T20:00:00Z");
const secretBox = new SecretBox(
  "test-v1",
  JSON.stringify({ "test-v1": Buffer.alloc(32, 31).toString("base64") }),
);

describe("Google reconnect and disconnect fencing", () => {
  it("rejects untargeted reuse of every previously known provider subject", async () => {
    const source = sourceWithDatabase((query) => {
      if (query.includes("select status, control_epoch from people")) {
        return [{ status: "registered", control_epoch: 4 }];
      }
      if (query.includes("select person_id from person_identities")) return [];
      if (query.includes("select person_id from integrations")) return [{ person_id: personId }];
      if (query.includes("select id, person_id, provider, external_subject_digest")) {
        return [integrationRow("revoked", 8, null)];
      }
      return [];
    });

    await expect(
      source.apply({
        kind: "connect_integration",
        personId,
        provider: "google",
        externalSubjectDigest,
        accountKind: "personal_family",
        activeCapabilities: ["mail"],
        credentials: { refreshToken: "new-refresh-token", accountEmail: "Person@Example.Test" },
        expectedPersonControlEpoch: 4,
        reconnectTarget: null,
        connectedAt: now.toISOString(),
      }),
    ).rejects.toThrow(/exact targeted reconnect/i);
  });

  it("preserves exact-epoch targeted reconnect for a settled revoked source", async () => {
    const source = sourceWithDatabase((query) => {
      if (query.includes("select status, control_epoch from people")) {
        return [{ status: "registered", control_epoch: 4 }];
      }
      if (query.includes("select person_id from person_identities")) return [];
      if (query.includes("select person_id from integrations")) return [{ person_id: personId }];
      if (query.includes("select id, person_id, provider, external_subject_digest")) {
        return [integrationRow("revoked", 8, null)];
      }
      if (query.includes("select capability.capability") && query.includes("integration_capabilities")) {
        return [{ capability: null, control_epoch: 8 }];
      }
      if (query.includes("update integrations") && query.includes("returning id")) {
        return [integrationRow("active", 9, Buffer.from("sealed"))];
      }
      return [];
    });

    await expect(
      source.apply({
        kind: "connect_integration",
        personId,
        provider: "google",
        externalSubjectDigest,
        accountKind: "personal_family",
        activeCapabilities: ["mail"],
        credentials: { refreshToken: "new-refresh-token", accountEmail: "Person@Example.Test" },
        expectedPersonControlEpoch: 4,
        reconnectTarget: {
          integrationId,
          expectedControlEpoch: 8,
          externalSubjectDigest,
        },
        connectedAt: now.toISOString(),
      }),
    ).resolves.toMatchObject({
      kind: "integration_connected",
      integrationId,
      status: "active",
      controlEpoch: 9,
    });
  });

  it("retires every outstanding untargeted OAuth secret while fencing source access", async () => {
    let retiredAttemptSecret: Buffer | null = null;
    const credential = encryptedBuffer(
      { refreshToken: "refresh-token-retained-until-receipt" },
      `florence:integration:${integrationId}:credentials`,
    );
    const source = sourceWithDatabase((query, values) => {
      if (query.includes("from integrations") && query.includes("for update")) {
        return [integrationRow("active", 7, credential)];
      }
      if (query.includes("select id from oauth_attempts")) return [{ id: attemptId }];
      if (query.includes("update oauth_attempts") && query.includes("secret_ciphertext")) {
        retiredAttemptSecret = values.find((value): value is Buffer => Buffer.isBuffer(value)) ?? null;
        return [];
      }
      if (query.includes("select revision.id")) return [];
      if (query.includes("update integrations") && query.includes("returning control_epoch")) {
        return [{ control_epoch: 8 }];
      }
      return [];
    });

    await expect(
      source.apply({
        kind: "begin_integration_revocation",
        integrationId,
        personId,
        expectedControlEpoch: 7,
        requestedAt: now.toISOString(),
      }),
    ).resolves.toMatchObject({
      kind: "integration_revocation_started",
      integrationId,
      controlEpoch: 8,
    });
    expect(retiredAttemptSecret).not.toBeNull();
    const retiredSecretBytes = Buffer.from(retiredAttemptSecret ?? Buffer.alloc(0));
    expect(
      JSON.parse(
        secretBox
          .decrypt(
            JSON.parse(retiredSecretBytes.toString("utf8")) as EncryptedValue,
            `florence:oauth:${attemptId}:pkce`,
          )
          .toString("utf8"),
      ),
    ).toEqual({ consumed: true });
  });
});

function sourceWithDatabase(
  respond: (query: string, values: readonly unknown[]) => readonly unknown[],
): PostgresSourceIntelligence {
  const database = (async (strings: TemplateStringsArray, ...values: readonly unknown[]) =>
    respond(strings.join(" ").replace(/\s+/gu, " ").trim(), values)) as {
    (strings: TemplateStringsArray, ...values: readonly unknown[]): Promise<readonly unknown[]>;
    array: (values: readonly unknown[]) => readonly unknown[];
  };
  database.array = (values: readonly unknown[]) => values;
  return new PostgresSourceIntelligence(database as unknown as Database, secretBox, {
    rawRetentionDays: 30,
  });
}

function integrationRow(status: string, controlEpoch: number, credentialCiphertext: Buffer | null) {
  return {
    id: integrationId,
    person_id: personId,
    provider: "google",
    external_subject_digest: externalSubjectDigest,
    account_kind: "personal_family",
    status,
    credential_ciphertext: credentialCiphertext,
    credential_key_version: credentialCiphertext ? "test-v1" : null,
    account_label_ciphertext: encryptedBuffer(
      "person@example.test",
      `florence:integration:${integrationId}:account-label`,
    ),
    account_label_key_version: "test-v1",
    last_authorized_capabilities: ["mail"],
    control_epoch: controlEpoch,
    connected_at: now,
    updated_at: now,
  };
}

function encryptedBuffer(value: unknown, purpose: string): Buffer {
  return Buffer.from(JSON.stringify(secretBox.encrypt(JSON.stringify(value), purpose)), "utf8");
}
