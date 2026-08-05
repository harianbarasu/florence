import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";

const VERSION = "v1";
const IV_BYTES = 12;
const TAG_BYTES = 16;
const KEY_BYTES = 32;

function decodeKey(encoded: string): Buffer {
  const key = /^[\da-f]{64}$/iu.test(encoded)
    ? Buffer.from(encoded, "hex")
    : Buffer.from(encoded, "base64url");
  if (key.length !== KEY_BYTES) {
    throw new Error("FLORENCE_ENCRYPTION_KEY must encode exactly 32 bytes");
  }
  return key;
}

export class SecretBox {
  readonly #key: Buffer;

  constructor(encodedKey: string) {
    this.#key = decodeKey(encodedKey);
  }

  seal(plaintext: string, associatedData: string): string {
    const iv = randomBytes(IV_BYTES);
    const cipher = createCipheriv("aes-256-gcm", this.#key, iv);
    cipher.setAAD(Buffer.from(associatedData, "utf8"));
    const ciphertext = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
    const tag = cipher.getAuthTag();
    return [VERSION, Buffer.concat([iv, tag, ciphertext]).toString("base64url")].join(":");
  }

  open(sealed: string, associatedData: string): string {
    const [version, payload] = sealed.split(":", 2);
    if (version !== VERSION || !payload) {
      throw new Error("Unsupported encrypted value");
    }
    const bytes = Buffer.from(payload, "base64url");
    if (bytes.length < IV_BYTES + TAG_BYTES + 1) {
      throw new Error("Invalid encrypted value");
    }
    const iv = bytes.subarray(0, IV_BYTES);
    const tag = bytes.subarray(IV_BYTES, IV_BYTES + TAG_BYTES);
    const ciphertext = bytes.subarray(IV_BYTES + TAG_BYTES);
    const decipher = createDecipheriv("aes-256-gcm", this.#key, iv);
    decipher.setAAD(Buffer.from(associatedData, "utf8"));
    decipher.setAuthTag(tag);
    return Buffer.concat([decipher.update(ciphertext), decipher.final()]).toString("utf8");
  }
}
