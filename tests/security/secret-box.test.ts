import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";
import { SecretBox } from "../../src/security/secret-box.js";

describe("SecretBox", () => {
  it("round trips with associated data", () => {
    const box = new SecretBox(randomBytes(32).toString("base64url"));
    const sealed = box.seal("private family data", "connection:abc");

    expect(sealed).not.toContain("private family data");
    expect(box.open(sealed, "connection:abc")).toBe("private family data");
  });

  it("fails closed when associated data changes", () => {
    const box = new SecretBox(randomBytes(32).toString("base64url"));
    const sealed = box.seal("token", "adult:a");

    expect(() => box.open(sealed, "adult:b")).toThrow();
  });

  it("rejects invalid keys", () => {
    expect(() => new SecretBox("too-short")).toThrow(/32 bytes/u);
  });
});
