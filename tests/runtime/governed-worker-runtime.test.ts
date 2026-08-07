import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { GENERAL_ANSWER_SKILL, PRODUCT_SKILLS } from "../../src/modules/orchestration/skills.js";
import { governedSkillDefinitionDigest } from "../../src/runtime/governed-worker-runtime.js";

describe("governed skill definition identity", () => {
  it("pins the complete declared definition and changes when executable meaning changes", () => {
    const baseline = governedSkillDefinitionDigest(GENERAL_ANSWER_SKILL);

    expect(baseline).toBe("ddb2d444be24d37a239d1c811efce2769100789d9f9edc50b2160dc252570159");
    expect(
      governedSkillDefinitionDigest({
        ...GENERAL_ANSWER_SKILL,
        instructions: `${GENERAL_ANSWER_SKILL.instructions}\nChanged behavior.`,
      }),
    ).not.toBe(baseline);
    expect(
      governedSkillDefinitionDigest({
        ...GENERAL_ANSWER_SKILL,
        requestedCapabilities: ["network.public_read"],
      }),
    ).not.toBe(baseline);
  });

  it("backfills the deployed suite with the exact current canonical digests", async () => {
    const migration = await readFile(
      new URL("../../migrations/012_skill_definition_governance.sql", import.meta.url),
      "utf8",
    );
    const migratedPins = new Map(
      [...migration.matchAll(/\('([^']+)', (\d+), '([a-f0-9]{64})'\)/gu)].map((match) => [
        `${match[1]}@${match[2]}`,
        match[3],
      ]),
    );
    const deployedSkills = [...Object.values(PRODUCT_SKILLS), GENERAL_ANSWER_SKILL];

    expect(migratedPins.size).toBe(deployedSkills.length);
    for (const skill of deployedSkills) {
      expect(migratedPins.get(`${skill.id}@${skill.version}`)).toBe(governedSkillDefinitionDigest(skill));
    }
  });
});
