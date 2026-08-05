import { describe, expect, it } from "vitest";
import { canonicalJson, payloadDigest } from "../../src/security/canonical-json.js";

describe("canonical JSON", () => {
  it("is stable across object key order and ignores undefined", () => {
    expect(canonicalJson({ z: 1, a: { y: 2, x: undefined, b: 3 } })).toBe('{"a":{"b":3,"y":2},"z":1}');
    expect(payloadDigest({ a: 1, b: 2 })).toBe(payloadDigest({ b: 2, a: 1 }));
  });
});
