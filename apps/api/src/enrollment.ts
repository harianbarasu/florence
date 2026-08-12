import { createHmac } from "node:crypto";

export type EnrollmentIssue = {
  code: string;
  challengeDigest: string;
};

export class EnrollmentCodes {
  readonly #secret: string;

  constructor(secret: string) {
    if (Buffer.byteLength(secret, "utf8") < 32) {
      throw new Error("FLORENCE_ENROLLMENT_SECRET must contain at least 32 UTF-8 bytes");
    }
    this.#secret = secret;
  }

  issue(input: { commandId: string; householdId: string; adultId: string }): EnrollmentIssue {
    const material = `${input.commandId}\0${input.householdId}\0${input.adultId}`;
    const token = createHmac("sha256", this.#secret)
      .update(`florence-enrollment-code-v1\0${material}`)
      .digest("base64url");
    const code = `FLORENCE-${token}`;
    return { code, challengeDigest: this.digest(code) };
  }

  digestCandidate(message: string | null): string | null {
    const code = message?.trim();
    if (!code?.startsWith("FLORENCE-") || code.length !== 52) return null;
    for (const character of code.slice("FLORENCE-".length)) {
      const digit = character >= "0" && character <= "9";
      const upper = character >= "A" && character <= "Z";
      const lower = character >= "a" && character <= "z";
      if (!digit && !upper && !lower && character !== "_" && character !== "-") return null;
    }
    return this.digest(code);
  }

  private digest(code: string): string {
    return createHmac("sha256", this.#secret)
      .update(`florence-enrollment-challenge-v1\0${code}`)
      .digest("hex");
  }
}
