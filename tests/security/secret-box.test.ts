import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";
import { BlindIndex } from "../../src/security/blind-index.js";
import { SecretBox } from "../../src/security/secret-box.js";
import {
  type EncryptionContext,
  EncryptionError,
  TenantJsonCipher,
} from "../../src/security/tenant-json-cipher.js";

function encryptionErrorCode(action: () => unknown): string | undefined {
  try {
    action();
  } catch (error) {
    return error instanceof EncryptionError ? error.code : undefined;
  }
  return undefined;
}

describe("SecretBox", () => {
  it("round trips with associated data and rotates tenant JSON encryption", () => {
    const box = new SecretBox(randomBytes(32).toString("base64url"));
    const sealed = box.seal("private family data", "connection:abc");

    expect(sealed).not.toContain("private family data");
    expect(box.open(sealed, "connection:abc")).toBe("private family data");

    const previousKey = randomBytes(32).toString("base64url");
    const activeKey = randomBytes(32).toString("hex");
    const context = {
      tenant: { kind: "household", id: "household-1" },
      table: "household_projections",
      rowId: "household-1",
      field: "state",
    } satisfies EncryptionContext;
    const previousCipher = new TenantJsonCipher({
      activeKeyId: "previous",
      keys: { previous: previousKey },
    });
    const encrypted = previousCipher.seal({ plan: "private family data", revision: 3 }, context);

    expect(encrypted.keyId).toBe("previous");
    expect(encrypted.ciphertext).not.toContain("private family data");
    expect(previousCipher.open(encrypted, context)).toEqual({
      plan: "private family data",
      revision: 3,
    });

    const rotatedCipher = new TenantJsonCipher({
      activeKeyId: "active",
      keys: { previous: previousKey, active: activeKey },
    });
    const rewrapped = rotatedCipher.rewrap(encrypted, context);
    expect(rotatedCipher.hasKey("previous")).toBe(true);
    expect(rotatedCipher.hasKey("missing")).toBe(false);
    expect(rewrapped.keyId).toBe("active");
    expect(rewrapped.ciphertext).not.toBe(encrypted.ciphertext);
    expect(rotatedCipher.open(rewrapped, context)).toEqual({
      plan: "private family data",
      revision: 3,
    });

    const blindIndex = new BlindIndex(randomBytes(32).toString("base64url"));
    const normalized = blindIndex.digest("provider-message", "Florence");
    expect(blindIndex.digest("provider-message", "Ｆｌｏｒｅｎｃｅ")).not.toBe(normalized);
    expect(blindIndex.digest("provider-message", "Ｆｌｏｒｅｎｃｅ".normalize("NFKC"))).toBe(normalized);
    expect(blindIndex.digest("provider-account", "Florence")).not.toBe(normalized);
    expect(normalized).toMatch(/^[A-Za-z0-9_-]{43}$/u);
  });

  it("fails closed when associated data or a tenant JSON envelope changes", () => {
    const box = new SecretBox(randomBytes(32).toString("base64url"));
    const sealed = box.seal("token", "adult:a");

    expect(() => box.open(sealed, "adult:b")).toThrow();

    const cipher = new TenantJsonCipher({
      activeKeyId: "active",
      keys: {
        active: randomBytes(32).toString("base64url"),
        alternate: randomBytes(32).toString("base64url"),
      },
    });
    const context = {
      tenant: { kind: "provider_ingress", id: "linq" },
      table: "provider_inbox",
      rowId: "inbox-1",
      field: "payload",
    } satisfies EncryptionContext;
    const encrypted = cipher.seal({ token: "private" }, context);
    const changedContexts: EncryptionContext[] = [
      { ...context, tenant: { ...context.tenant, kind: "household" } },
      { ...context, tenant: { ...context.tenant, id: "gmail" } },
      { ...context, table: "provider_inbox_conflicts" },
      { ...context, rowId: "inbox-2" },
      { ...context, field: "authentication" },
    ];
    for (const changedContext of changedContexts) {
      expect(encryptionErrorCode(() => cipher.open(encrypted, changedContext))).toBe("authentication_failed");
    }

    expect(encryptionErrorCode(() => cipher.open({ ...encrypted, keyId: "alternate" }, context))).toBe(
      "authentication_failed",
    );
    expect(encryptionErrorCode(() => cipher.open({ ...encrypted, keyId: "missing" }, context))).toBe(
      "unknown_key",
    );
    expect(
      encryptionErrorCode(() =>
        cipher.open({ ...encrypted, ciphertext: encrypted.ciphertext.replace(/^v1:/u, "v2:") }, context),
      ),
    ).toBe("invalid_envelope");

    const tamperedBytes = Buffer.from(encrypted.ciphertext.slice("v1:".length), "base64url");
    const finalIndex = tamperedBytes.length - 1;
    tamperedBytes[finalIndex] = (tamperedBytes[finalIndex] ?? 0) ^ 1;
    expect(
      encryptionErrorCode(() =>
        cipher.open({ ...encrypted, ciphertext: `v1:${tamperedBytes.toString("base64url")}` }, context),
      ),
    ).toBe("authentication_failed");
  });

  it("rejects invalid legacy, keyring, and blind-index keys", () => {
    expect(() => new SecretBox("too-short")).toThrow(/32 bytes/u);
    expect(
      () =>
        new TenantJsonCipher({
          activeKeyId: "active",
          keys: { active: "too-short" },
        }),
    ).toThrow(/32 bytes/u);
    expect(
      () =>
        new TenantJsonCipher({
          activeKeyId: "missing",
          keys: { present: randomBytes(32).toString("base64url") },
        }),
    ).toThrow(/active key/u);
    expect(() => new BlindIndex("too-short")).toThrow(/32 bytes/u);
  });
});
