import { createHmac } from "node:crypto";

const KEY_BYTES = 32;
const FORMAT_VERSION = "florence.blind-index.v1";

/** Produces deterministic, domain-separated lookup values without exposing their input. */
export class BlindIndex {
  readonly #key: Buffer;

  constructor(encodedKey: string) {
    this.#key = decodeKey(encodedKey);
  }

  digest(domain: string, value: string): string {
    if (typeof domain !== "string" || domain.length === 0 || typeof value !== "string") {
      throw new TypeError("The blind-index domain and value must be strings.");
    }
    const message = JSON.stringify([FORMAT_VERSION, domain, value]);
    return createHmac("sha256", this.#key).update(message, "utf8").digest("base64url");
  }
}

function decodeKey(encoded: string): Buffer {
  if (/^[\da-f]{64}$/iu.test(encoded)) {
    return Buffer.from(encoded, "hex");
  }

  if (!/^[A-Za-z0-9_-]{43}=?$/u.test(encoded)) {
    throw new Error("The blind-index key must encode exactly 32 bytes.");
  }
  const unpadded = encoded.endsWith("=") ? encoded.slice(0, -1) : encoded;
  const decoded = Buffer.from(unpadded, "base64url");
  if (decoded.length !== KEY_BYTES || decoded.toString("base64url") !== unpadded) {
    throw new Error("The blind-index key must encode exactly 32 bytes.");
  }
  return decoded;
}
