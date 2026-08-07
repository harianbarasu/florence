import { createHash, randomUUID } from "node:crypto";
import type postgres from "postgres";
import { z } from "zod";
import type { Database } from "../db/client.js";
import type {
  PinnedSkill,
  WorkerJob,
  WorkerResult,
  WorkerRuntime,
} from "../modules/orchestration/contracts.js";
import { GENERAL_ANSWER_SKILL, PRODUCT_SKILLS } from "../modules/orchestration/skills.js";
import { canonicalDigest, canonicalJson } from "../shared/canonical-json.js";
import type { SecretBox } from "../shared/crypto.js";
import { StaleAuthorityError } from "../shared/errors.js";

const HARNESS_RELEASE = "florence-bounded-v1";
const PRODUCT_SKILL_OWNER = "florence-product";
const PRODUCTION_CHANNEL = "production";
const WORKER_INPUT_SCHEMA = {
  type: "object",
  required: ["goal", "authorizedContext"],
} as const;
const TOOL_CEILING: readonly string[] = [];

interface GovernedSkillDefinition {
  readonly id: string;
  readonly version: number;
  readonly purpose: string;
  readonly instructions: string;
  readonly inputSchema: typeof WORKER_INPUT_SCHEMA;
  readonly outputSchema: postgres.JSONValue;
  readonly outputSchemaName: string;
  readonly riskClass: "low" | "medium" | "high";
  readonly capabilities: {
    readonly requested: readonly string[];
    readonly toolCeiling: readonly string[];
  };
}

interface BootstrapBaseline {
  readonly definitionDigest: string;
  readonly evaluationRelease: string;
  readonly evaluationSuiteDigest: string;
}

/*
 * This is the one production bootstrap exception. These exact definitions were already deployed
 * before evaluated promotion became mandatory. Changing a definition requires a new version and a
 * passed evaluation run; editing these hashes to make drift boot is not a promotion mechanism.
 */
const DEPLOYED_BOOTSTRAP_BASELINE: Readonly<Record<string, BootstrapBaseline>> = {
  "coverage.need_interpret@3": {
    definitionDigest: "11672c7a656c3ef49449816c12d54bb78b2c915b86761c71e9a0866fafe7eeb7",
    evaluationRelease: "coverage-core-3",
    evaluationSuiteDigest: sha256Hex("coverage-core-3:initial-protected-suite"),
  },
  "coverage.commitment_propose@3": {
    definitionDigest: "35a1dc39d5665b172b2629c32942e64977d52740f97a45ce6dbb74cc26b2660e",
    evaluationRelease: "coverage-core-3",
    evaluationSuiteDigest: sha256Hex("coverage-core-3:initial-protected-suite"),
  },
  "coverage.minimum_disclosure@1": {
    definitionDigest: "a7b61d8d6c05e88be705fcf18d977b9568347d4725cf978f165e604c986bcef9",
    evaluationRelease: "coverage-core-1",
    evaluationSuiteDigest: sha256Hex("coverage-core-1:initial-protected-suite"),
  },
  "coverage.outcome_assess@1": {
    definitionDigest: "335a5867f9305c34a803428bcdfccd4d009bd6c063dec42804a4c627fcae65a4",
    evaluationRelease: "coverage-core-1",
    evaluationSuiteDigest: sha256Hex("coverage-core-1:initial-protected-suite"),
  },
  "coverage.response_interpret@2": {
    definitionDigest: "debf1ba2eff0cecd9c4aa1c997570036e7e27f080fafb9784ba448c19f406347",
    evaluationRelease: "coverage-core-2",
    evaluationSuiteDigest: sha256Hex("coverage-core-2:initial-protected-suite"),
  },
  "private_source.reconcile@1": {
    definitionDigest: "998354d86fdc302e53baf7f9f810b381ff73d21d4c1491b30c2fc286c0122c8e",
    evaluationRelease: "private-source-frontier-1",
    evaluationSuiteDigest: sha256Hex("private-source-frontier-1:initial-protected-suite"),
  },
  "general.answer@1": {
    definitionDigest: "ddb2d444be24d37a239d1c811efce2769100789d9f9edc50b2160dc252570159",
    evaluationRelease: "general-answer-1",
    evaluationSuiteDigest: sha256Hex("general-answer-1:initial-protected-suite"),
  },
};

/** Persists the immutable skill/runtime/evaluation pins around an otherwise ephemeral worker. */
export class GovernedWorkerRuntime implements WorkerRuntime {
  public constructor(
    private readonly database: Database,
    private readonly secretBox: SecretBox,
    private readonly delegate: WorkerRuntime,
  ) {}

  public async run<Schema extends z.ZodType>(
    job: WorkerJob<Schema>,
  ): Promise<WorkerResult<z.output<Schema>>> {
    const definitionDigest = governedSkillDefinitionDigest(job.skill);
    const pin = await loadActivePin(this.database, job.skill, definitionDigest);
    const authority = workerAuthorityTuple(job.authority);
    const inputDigest = governedWorkerInputDigest(job);
    const startedAt = new Date();
    await this.database.begin(async (transaction) => {
      await assertCurrentWorkerAuthority(transaction, job.authority);
      const tasks = await transaction<{ id: string }[]>`
        insert into tasks (
          id, household_id, conversation_id, requested_by_person_id,
          task_kind, task_version, purpose, input_digest, status,
          person_control_epoch, household_control_epoch, conversation_authority_version,
          created_at
        ) values (
          ${job.taskVersionId}, ${authority.householdId}, ${authority.conversationId},
          ${authority.requestedByPersonId}, ${job.skill.id}, 1, ${job.skill.purpose},
          ${inputDigest}, 'running', ${authority.personControlEpoch},
          ${authority.householdControlEpoch}, ${authority.conversationAuthorityVersion},
          ${startedAt}
        ) on conflict (id) do update set status = 'running'
          where tasks.input_digest = excluded.input_digest
            and tasks.requested_by_person_id = excluded.requested_by_person_id
            and tasks.person_control_epoch = excluded.person_control_epoch
            and tasks.household_id is not distinct from excluded.household_id
            and tasks.household_control_epoch is not distinct from excluded.household_control_epoch
            and tasks.conversation_id is not distinct from excluded.conversation_id
            and tasks.conversation_authority_version is not distinct from excluded.conversation_authority_version
        returning id
      `;
      if (!tasks[0]) throw new Error("Governed task input differs from its persisted input digest");
      const attempts = await transaction<{ id: string }[]>`
        insert into worker_attempts (
          id, task_id, attempt_number, skill_version_id, skill_definition_digest,
          harness_release, runtime_route, evaluation_release_id, trace_id, status, budget,
          deadline_at, started_at
        ) values (
          ${job.attemptId}, ${job.taskVersionId}, 1, ${pin.skillVersionId}, ${definitionDigest},
          ${HARNESS_RELEASE}, 'pending', ${pin.evaluationReleaseId}, ${randomUUID()}, 'running',
          ${transaction.json(job.budget)}, ${job.deadline}, ${startedAt}
        ) on conflict (id) do update set status = 'running'
          where worker_attempts.task_id = excluded.task_id
            and worker_attempts.skill_version_id = excluded.skill_version_id
            and worker_attempts.skill_definition_digest = excluded.skill_definition_digest
            and worker_attempts.evaluation_release_id = excluded.evaluation_release_id
        returning id
      `;
      if (!attempts[0]) throw new Error("Governed worker attempt differs from its persisted pins");
    });

    const result = await this.delegate.run(job);
    const serialized = canonicalJson(result.proposal ?? { errorCode: result.errorCode ?? "unknown" });
    const encrypted = this.secretBox.encrypt(serialized, "worker-result");
    const resultDigest = sha256Hex(serialized);
    await this.database.begin(async (transaction) => {
      const traces = await transaction<{ trace_id: string }[]>`
        update worker_attempts set status = ${result.status === "proposed" ? "succeeded" : result.status},
          runtime_route = ${result.runtimeRoute}, completed_at = ${result.completedAt}
        where id = ${job.attemptId} and skill_definition_digest = ${definitionDigest}
        returning trace_id
      `;
      const trace = traces[0]?.trace_id;
      if (!trace) throw new Error("Worker attempt trace disappeared or its skill definition changed");
      await transaction`
        insert into worker_results (
          id, worker_attempt_id, output_contract, output_digest, output_ciphertext,
          output_key_version, diagnostics, submitted_at, reconciliation_status
        ) values (
          ${randomUUID()}, ${job.attemptId}, ${job.skill.outputSchemaName}, ${resultDigest},
          ${Buffer.from(JSON.stringify(encrypted), "utf8")}, ${encrypted.kid},
          ${transaction.json({ status: result.status, errorCode: result.errorCode ?? null })},
          ${result.completedAt}, 'pending'
        ) on conflict (worker_attempt_id) do nothing
      `;
      await transaction`
        insert into trace_manifests (
          id, worker_attempt_id, trace_id, manifest, retention_until, created_at
        ) values (
          ${randomUUID()}, ${job.attemptId}, ${trace}, ${transaction.json({
            inputDigest,
            skillId: job.skill.id,
            skillVersion: job.skill.version,
            skillDefinitionDigest: definitionDigest,
            harnessRelease: HARNESS_RELEASE,
            runtimeRoute: result.runtimeRoute,
            evaluationRelease: job.skill.evaluationRelease,
            status: result.status,
            modelCalls: result.status === "expired" ? 0 : 1,
          })}, ${new Date(Date.now() + 7 * 86_400_000)}, ${result.completedAt}
        ) on conflict (worker_attempt_id) do nothing
      `;
      const completedTasks = await transaction<{ id: string }[]>`
        update tasks set status = ${result.status === "proposed" ? "completed" : "failed"},
          completed_at = ${result.completedAt}
        where id = ${job.taskVersionId}
          and input_digest = ${inputDigest}
          and requested_by_person_id = ${authority.requestedByPersonId}
          and person_control_epoch = ${authority.personControlEpoch}
          and household_id is not distinct from ${authority.householdId}::uuid
          and household_control_epoch is not distinct from ${authority.householdControlEpoch}::bigint
          and conversation_id is not distinct from ${authority.conversationId}::uuid
          and conversation_authority_version is not distinct from ${authority.conversationAuthorityVersion}::bigint
        returning id
      `;
      if (!completedTasks[0]) {
        throw new Error("Governed task disappeared or its persisted authority changed");
      }
    });
    return result;
  }

  public async reconcile(
    attemptId: string,
    status: "accepted" | "partially_accepted" | "rejected" | "stale",
  ): Promise<void> {
    await this.database`
      update worker_results set reconciliation_status = ${status}, reconciled_at = now()
      where worker_attempt_id = ${attemptId} and reconciliation_status = 'pending'
    `;
  }
}

interface WorkerAuthorityTuple {
  readonly requestedByPersonId: string;
  readonly personControlEpoch: number;
  readonly householdId: string | null;
  readonly householdControlEpoch: number | null;
  readonly conversationId: string | null;
  readonly conversationAuthorityVersion: number | null;
}

function workerAuthorityTuple(authority: WorkerJob["authority"]): WorkerAuthorityTuple {
  return {
    requestedByPersonId: authority.person.id,
    personControlEpoch: authority.person.controlEpoch,
    householdId: authority.household?.id ?? null,
    householdControlEpoch: authority.household?.controlEpoch ?? null,
    conversationId: authority.conversation?.id ?? null,
    conversationAuthorityVersion: authority.conversation?.authorityVersion ?? null,
  };
}

/** Canonical identity of the exact authorized model input, including every authority fence. */
export function governedWorkerInputDigest(job: WorkerJob): string {
  return sha256Hex(
    canonicalJson({
      skillId: job.skill.id,
      skillVersion: job.skill.version,
      skillDefinitionDigest: governedSkillDefinitionDigest(job.skill),
      authority: workerAuthorityTuple(job.authority),
      goal: job.goal,
      authorizedContextDigest: sha256Hex(job.authorizedContext),
      authorizedImageDigests:
        job.images?.map((image) => ({
          mimeType: image.mimeType,
          sha256: image.sha256,
        })) ?? [],
    }),
  );
}

async function assertCurrentWorkerAuthority(
  transaction: postgres.TransactionSql<Record<string, never>>,
  authority: WorkerJob["authority"],
): Promise<void> {
  const people = await transaction<{ id: string }[]>`
    select id from people
    where id = ${authority.person.id} and status = 'registered'
      and control_epoch = ${authority.person.controlEpoch}
    for share
  `;
  if (!people[0]) throw new StaleAuthorityError("Governed worker person authority is stale");

  if (authority.household) {
    const households = await transaction<{ id: string }[]>`
      select id from households
      where id = ${authority.household.id} and status in ('onboarding', 'active', 'paused')
        and control_epoch = ${authority.household.controlEpoch}
      for share
    `;
    if (!households[0]) {
      throw new StaleAuthorityError("Governed worker household authority is stale");
    }
  }

  if (authority.conversation) {
    const conversations = await transaction<{ id: string }[]>`
      select id from conversations
      where id = ${authority.conversation.id} and status not in ('deletion_fenced', 'deleted')
        and authority_version = ${authority.conversation.authorityVersion}
      for share
    `;
    if (!conversations[0]) {
      throw new StaleAuthorityError("Governed worker conversation authority is stale");
    }
  }
}

/**
 * Registers declared definitions, then verifies that every declaration used by this worker has an
 * explicit active production pin. Only the frozen initially deployed suite is bootstrapped active.
 */
export async function bootstrapGovernedSkills(database: Database): Promise<void> {
  const skills = [...Object.values(PRODUCT_SKILLS), GENERAL_ANSWER_SKILL];
  await database.begin(async (transaction) => {
    for (const skill of skills) {
      const definition = canonicalSkillDefinition(skill);
      const definitionDigest = canonicalDigest(definition);
      const baseline = DEPLOYED_BOOTSTRAP_BASELINE[skillIdentity(skill)];
      if (baseline && baseline.definitionDigest !== definitionDigest) {
        throw new Error(
          `Governed skill definition drifted without a version change: ${skillIdentity(skill)}`,
        );
      }
      if (baseline && baseline.evaluationRelease !== skill.evaluationRelease) {
        throw new Error(
          `Governed skill evaluation pin drifted without a version change: ${skillIdentity(skill)}`,
        );
      }

      let evaluationReleaseId: string | null = null;
      if (baseline) {
        await transaction`
          insert into evaluation_releases (id, release_key, suite_digest, status)
          values (
            ${randomUUID()}, ${baseline.evaluationRelease}, ${baseline.evaluationSuiteDigest}, 'active'
          ) on conflict (release_key) do nothing
        `;
        const evaluationRows = await transaction<{ id: string; suite_digest: string; status: string }[]>`
          select id, suite_digest, status from evaluation_releases
          where release_key = ${baseline.evaluationRelease}
        `;
        const evaluation = evaluationRows[0];
        if (
          !evaluation ||
          evaluation.suite_digest !== baseline.evaluationSuiteDigest ||
          evaluation.status !== "active"
        ) {
          throw new Error(`Bootstrap evaluation release drifted: ${baseline.evaluationRelease}`);
        }
        evaluationReleaseId = evaluation.id;
      } else {
        const evaluationRows = await transaction<{ id: string }[]>`
          select id from evaluation_releases where release_key = ${skill.evaluationRelease}
        `;
        evaluationReleaseId = evaluationRows[0]?.id ?? null;
      }

      await transaction`
        insert into skills (id, skill_key, owner, purpose, risk_class)
        values (
          ${randomUUID()}, ${skill.id}, ${PRODUCT_SKILL_OWNER}, ${skill.purpose}, ${skill.riskClass}
        ) on conflict (skill_key) do nothing
      `;
      const skillRows = await transaction<{ id: string }[]>`
        select id from skills where skill_key = ${skill.id}
      `;
      const skillId = skillRows[0]?.id;
      if (!skillId) throw new Error("Skill registration failed");

      await transaction`
        insert into skill_versions (
          id, skill_id, version, input_schema, output_schema, requested_capabilities,
          tool_ceiling, evaluation_release_id, status, definition_digest
        ) values (
          ${randomUUID()}, ${skillId}, ${skill.version},
          ${transaction.json(definition.inputSchema)},
          ${transaction.json(definition.outputSchema)},
          ${transaction.array([...definition.capabilities.requested])},
          ${transaction.array([...definition.capabilities.toolCeiling])},
          ${evaluationReleaseId}, ${baseline ? "approved" : "candidate"}, ${definitionDigest}
        ) on conflict (skill_id, version) do nothing
      `;
      const versionRows = await transaction<
        {
          id: string;
          definition_digest: string;
          input_schema: unknown;
          output_schema: unknown;
          requested_capabilities: string[];
          tool_ceiling: string[];
          evaluation_release_id: string | null;
          status: string;
        }[]
      >`
        select id, definition_digest, input_schema, output_schema, requested_capabilities,
          tool_ceiling, evaluation_release_id, status
        from skill_versions where skill_id = ${skillId} and version = ${skill.version}
      `;
      const version = versionRows[0];
      if (!version) throw new Error("Skill version registration failed");
      assertPersistedDefinitionMatches(skill, definition, definitionDigest, version);

      if (baseline) {
        if (version.status !== "approved" || version.evaluation_release_id !== evaluationReleaseId) {
          throw new Error(`Bootstrap skill pin is not approved: ${skillIdentity(skill)}`);
        }
        const releaseRows = await transaction<
          { skill_version_id: string; evaluation_release_id: string; active: boolean }[]
        >`
          select skill_version_id, evaluation_release_id, active from skill_release_events
          where skill_id = ${skillId} and channel = ${PRODUCTION_CHANNEL}
          order by occurred_at desc
          for update
        `;
        const active = releaseRows.find((release) => release.active);
        if (!active) {
          if (releaseRows.length > 0) {
            throw new Error(
              `Bootstrap refused to reactivate a skill with production release history: ${skillIdentity(skill)}`,
            );
          }
          await transaction`
            insert into skill_release_events (
              id, skill_id, skill_version_id, channel, event_kind, active,
              evaluation_release_id, occurred_at
            ) values (
              ${randomUUID()}, ${skillId}, ${version.id}, ${PRODUCTION_CHANNEL}, 'promoted', true,
              ${evaluationReleaseId}, now()
            )
          `;
        } else if (
          active.skill_version_id !== version.id ||
          active.evaluation_release_id !== evaluationReleaseId
        ) {
          throw new Error(`Bootstrap refused to replace an active release: ${skillIdentity(skill)}`);
        }
      }
    }
  });

  // This intentionally runs after registration commits. A new candidate remains available for an
  // operator to evaluate and promote, while this worker process fails closed until that happens.
  for (const skill of skills) {
    await loadActivePin(database, skill, governedSkillDefinitionDigest(skill));
  }
}

export function governedSkillDefinitionDigest(skill: PinnedSkill): string {
  return canonicalDigest(canonicalSkillDefinition(skill));
}

function canonicalSkillDefinition(skill: PinnedSkill): GovernedSkillDefinition {
  return {
    id: skill.id,
    version: skill.version,
    purpose: skill.purpose,
    instructions: skill.instructions,
    inputSchema: WORKER_INPUT_SCHEMA,
    outputSchema: JSON.parse(JSON.stringify(z.toJSONSchema(skill.outputSchema))) as postgres.JSONValue,
    outputSchemaName: skill.outputSchemaName,
    riskClass: skill.riskClass,
    capabilities: {
      requested: [...skill.requestedCapabilities].sort(),
      toolCeiling: [...TOOL_CEILING],
    },
  };
}

function assertPersistedDefinitionMatches(
  skill: PinnedSkill,
  definition: GovernedSkillDefinition,
  definitionDigest: string,
  persisted: {
    readonly definition_digest: string;
    readonly input_schema: unknown;
    readonly output_schema: unknown;
    readonly requested_capabilities: readonly string[];
    readonly tool_ceiling: readonly string[];
  },
): void {
  if (
    persisted.definition_digest !== definitionDigest ||
    canonicalJson(persisted.input_schema) !== canonicalJson(definition.inputSchema) ||
    canonicalJson(persisted.output_schema) !== canonicalJson(definition.outputSchema) ||
    canonicalJson([...persisted.requested_capabilities].sort()) !==
      canonicalJson(definition.capabilities.requested) ||
    canonicalJson([...persisted.tool_ceiling].sort()) !== canonicalJson(definition.capabilities.toolCeiling)
  ) {
    throw new Error(`Governed skill definition does not match its immutable pin: ${skillIdentity(skill)}`);
  }
}

async function loadActivePin(
  database: Database,
  skill: PinnedSkill,
  definitionDigest: string,
): Promise<{ skillVersionId: string; evaluationReleaseId: string }> {
  const rows = await database<
    {
      skill_version_id: string;
      definition_digest: string;
      version_status: string;
      evaluation_release_id: string | null;
      evaluation_release_key: string | null;
      evaluation_status: string | null;
      active_skill_version_id: string | null;
      active_evaluation_release_id: string | null;
      has_passed_evaluation: boolean;
    }[]
  >`
    select version.id as skill_version_id, version.definition_digest, version.status as version_status,
      evaluation.id as evaluation_release_id, evaluation.release_key as evaluation_release_key,
      evaluation.status as evaluation_status, release.skill_version_id as active_skill_version_id,
      release.evaluation_release_id as active_evaluation_release_id,
      exists (
        select 1 from evaluation_runs run
        where run.skill_version_id = version.id
          and run.evaluation_release_id = evaluation.id and run.passed
      ) as has_passed_evaluation
    from skills skill_record
    join skill_versions version
      on version.skill_id = skill_record.id and version.version = ${skill.version}
    left join evaluation_releases evaluation on evaluation.id = version.evaluation_release_id
    left join skill_release_events release
      on release.skill_id = skill_record.id and release.channel = ${PRODUCTION_CHANNEL} and release.active
    where skill_record.skill_key = ${skill.id}
  `;
  const row = rows[0];
  if (!row) throw new Error(`Governed skill is not registered: ${skillIdentity(skill)}`);
  if (row.definition_digest !== definitionDigest) {
    throw new Error(`Governed skill definition drifted: ${skillIdentity(skill)}`);
  }
  const baseline = DEPLOYED_BOOTSTRAP_BASELINE[skillIdentity(skill)];
  const isExactBootstrapBaseline =
    baseline?.definitionDigest === definitionDigest && baseline.evaluationRelease === skill.evaluationRelease;
  if (
    row.version_status !== "approved" ||
    row.evaluation_release_id === null ||
    row.evaluation_release_key !== skill.evaluationRelease ||
    row.evaluation_status !== "active" ||
    row.active_skill_version_id !== row.skill_version_id ||
    row.active_evaluation_release_id !== row.evaluation_release_id ||
    (!isExactBootstrapBaseline && !row.has_passed_evaluation)
  ) {
    throw new Error(`Governed skill lacks an evaluated active production promotion: ${skillIdentity(skill)}`);
  }
  return {
    skillVersionId: row.skill_version_id,
    evaluationReleaseId: row.evaluation_release_id,
  };
}

function skillIdentity(skill: Pick<PinnedSkill, "id" | "version">): string {
  return `${skill.id}@${skill.version}`;
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
