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

    expect(baseline).toBe("dc077100a8d9988d2260a79b668e6c750b306db9789d656277382c12add57b55");
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

  it("preserves migrated pins and gives the current provider contract an exact identity", async () => {
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
    const migratedSkills = [
      ...Object.values(PRODUCT_SKILLS).filter(
        (skill) =>
          skill.id !== "private_source.reconcile" &&
          skill.id !== "relationship.introduction_classify" &&
          skill.id !== "coverage.need_interpret",
      ),
    ];

    expect(migratedPins.size).toBe(migratedSkills.length + 3);
    for (const skill of migratedSkills) {
      expect(migratedPins.get(`${skill.id}@${skill.version}`)).toBe(governedSkillDefinitionDigest(skill));
    }
    expect(migratedPins.get("private_source.reconcile@1")).toBe(
      "998354d86fdc302e53baf7f9f810b381ff73d21d4c1491b30c2fc286c0122c8e",
    );
    expect(migratedPins.get("coverage.need_interpret@3")).toBe(
      "11672c7a656c3ef49449816c12d54bb78b2c915b86761c71e9a0866fafe7eeb7",
    );
    expect(migratedPins.get("general.answer@1")).toBe(
      "ddb2d444be24d37a239d1c811efce2769100789d9f9edc50b2160dc252570159",
    );
    expect(governedSkillDefinitionDigest(PRODUCT_SKILLS.needInterpret)).toBe(
      "e0dbb77250ac82997175fb60a3234a7c43e18e1e46a6153362e8ef25dbcf4260",
    );
    expect(governedSkillDefinitionDigest(PRODUCT_SKILLS.privateSourceReconcile)).toBe(
      "e8ece1f2d9a7dfa56cfb4608b11623476bea9c40f8c5aa6207548b6019d28cac",
    );
    expect(governedSkillDefinitionDigest(PRODUCT_SKILLS.familyIntroduction)).toBe(
      "c2361a238120c1400226dff3fe8e0767249cc9636d7159bfc959c75cffdbcfc2",
    );
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
