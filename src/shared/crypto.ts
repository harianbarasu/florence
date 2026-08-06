import { createCipheriv, createDecipheriv, createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { z } from "zod";

const encryptedValueSchema = z
  .object({
    v: z.literal(1),
    kid: z.string().min(1),
    iv: z.string().min(1),
    tag: z.string().min(1),
    ciphertext: z.string(),
  })
  .strict();

export type EncryptedValue = z.infer<typeof encryptedValueSchema>;

export class SecretBox {
  readonly #activeKeyId: string;
  readonly #keys: ReadonlyMap<string, Buffer>;

  public constructor(activeKeyId: string, rawKeyring: string) {
    const parsed = z.record(z.string(), z.string().min(1)).parse(JSON.parse(rawKeyring));
    const keys = new Map<string, Buffer>();
    for (const [keyId, encoded] of Object.entries(parsed)) {
      const key = Buffer.from(encoded, "base64");
      if (key.length !== 32) throw new Error(`Data key ${keyId} must decode to 32 bytes`);
      keys.set(keyId, key);
    }
    if (!keys.has(activeKeyId)) throw new Error(`Active data key ${activeKeyId} is missing`);
    this.#activeKeyId = activeKeyId;
    this.#keys = keys;
  }

  public encrypt(value: Uint8Array | string, purpose: string): EncryptedValue {
    const key = this.#keys.get(this.#activeKeyId);
    if (!key) throw new Error("Active data key is unavailable");
    const iv = randomBytes(12);
    const cipher = createCipheriv("aes-256-gcm", key, iv);
    cipher.setAAD(Buffer.from(purpose, "utf8"));
    const plaintext = typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value);
    const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
    return {
      v: 1,
      kid: this.#activeKeyId,
      iv: iv.toString("base64"),
      tag: cipher.getAuthTag().toString("base64"),
      ciphertext: ciphertext.toString("base64"),
    };
  }

  public decrypt(value: unknown, purpose: string): Buffer {
    const parsed = encryptedValueSchema.parse(value);
    const key = this.#keys.get(parsed.kid);
    if (!key) throw new Error(`Data key ${parsed.kid} is unavailable`);
    const decipher = createDecipheriv("aes-256-gcm", key, Buffer.from(parsed.iv, "base64"));
    decipher.setAAD(Buffer.from(purpose, "utf8"));
    decipher.setAuthTag(Buffer.from(parsed.tag, "base64"));
    return Buffer.concat([decipher.update(Buffer.from(parsed.ciphertext, "base64")), decipher.final()]);
  }
}

export function randomOpaqueToken(bytes = 32): string {
  return randomBytes(bytes).toString("base64url");
}

export function keyedDigest(secret: string, value: string): string {
  return createHmac("sha256", secret).update(value).digest("base64url");
}

export function secureDigestEquals(left: string, right: string): boolean {
  const leftBytes = Buffer.from(left);
  const rightBytes = Buffer.from(right);
  return leftBytes.length === rightBytes.length && timingSafeEqual(leftBytes, rightBytes);
}
