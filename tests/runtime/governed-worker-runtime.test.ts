import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import type { WorkerJob } from "../../src/modules/orchestration/contracts.js";
import { GENERAL_ANSWER_SKILL, PRODUCT_SKILLS } from "../../src/modules/orchestration/skills.js";
import {
  governedSkillDefinitionDigest,
  governedWorkerInputDigest,
} from "../../src/runtime/governed-worker-runtime.js";

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

  it("binds the canonical input digest to the complete worker authority tuple", () => {
    const base: WorkerJob<typeof GENERAL_ANSWER_SKILL.outputSchema> = {
      attemptId: "10000000-0000-4000-8000-000000000001",
      taskVersionId: "10000000-0000-4000-8000-000000000002",
      authority: {
        person: { id: "10000000-0000-4000-8000-000000000003", controlEpoch: 2 },
        household: { id: "10000000-0000-4000-8000-000000000004", controlEpoch: 3 },
        conversation: { id: "10000000-0000-4000-8000-000000000005", authorityVersion: 4 },
      },
      skill: GENERAL_ANSWER_SKILL,
      authorizedContext: "Exact admitted context",
      goal: "Answer one exact request",
      deadline: new Date("2026-08-07T20:00:00.000Z"),
      budget: { maxModelCalls: 1, maxOutputTokens: 1_000 },
    };
    const digest = governedWorkerInputDigest(base);

    expect(
      governedWorkerInputDigest({
        ...base,
        authority: { ...base.authority, person: { ...base.authority.person, controlEpoch: 5 } },
      }),
    ).not.toBe(digest);
    expect(
      governedWorkerInputDigest({
        ...base,
        authority: {
          ...base.authority,
          household: { id: "10000000-0000-4000-8000-000000000004", controlEpoch: 6 },
        },
      }),
    ).not.toBe(digest);
    expect(
      governedWorkerInputDigest({
        ...base,
        authority: {
          ...base.authority,
          conversation: {
            id: "10000000-0000-4000-8000-000000000005",
            authorityVersion: 7,
          },
        },
      }),
    ).not.toBe(digest);
  });
});
