import { createHash, randomUUID } from "node:crypto";
import { z } from "zod";
import type { Database } from "../db/client.js";
import type { WorkerJob, WorkerResult, WorkerRuntime } from "../modules/orchestration/contracts.js";
import { GENERAL_ANSWER_SKILL, PRODUCT_SKILLS } from "../modules/orchestration/skills.js";
import { canonicalJson } from "../shared/canonical-json.js";
import type { SecretBox } from "../shared/crypto.js";

const HARNESS_RELEASE = "florence-bounded-v1";

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
    const inputDigest = sha256Hex(
      canonicalJson({
        skillId: job.skill.id,
        skillVersion: job.skill.version,
        goal: job.goal,
        authorizedContextDigest: sha256Hex(job.authorizedContext),
        authorizedImageDigests:
          job.images?.map((image) => ({
            mimeType: image.mimeType,
            sha256: image.sha256,
          })) ?? [],
      }),
    );
    const pin = await this.loadPin(job.skill.id, job.skill.version, job.skill.evaluationRelease);
    const startedAt = new Date();
    await this.database.begin(async (transaction) => {
      await transaction`
        insert into tasks (
          id, task_kind, task_version, purpose, input_digest, status, created_at
        ) values (
          ${job.taskVersionId}, ${job.skill.id}, 1, ${job.skill.purpose}, ${inputDigest},
          'running', ${startedAt}
        ) on conflict (id) do update set status = 'running'
      `;
      await transaction`
        insert into worker_attempts (
          id, task_id, attempt_number, skill_version_id, harness_release, runtime_route,
          evaluation_release_id, trace_id, status, budget, deadline_at, started_at
        ) values (
          ${job.attemptId}, ${job.taskVersionId}, 1, ${pin.skillVersionId}, ${HARNESS_RELEASE},
          'pending', ${pin.evaluationReleaseId}, ${randomUUID()}, 'running',
          ${transaction.json(job.budget)}, ${job.deadline}, ${startedAt}
        ) on conflict (id) do nothing
      `;
    });

    const result = await this.delegate.run(job);
    const serialized = canonicalJson(result.proposal ?? { errorCode: result.errorCode ?? "unknown" });
    const encrypted = this.secretBox.encrypt(serialized, "worker-result");
    const resultDigest = sha256Hex(serialized);
    const traceId = await this.database.begin(async (transaction) => {
      const traces = await transaction<{ trace_id: string }[]>`
        update worker_attempts set status = ${result.status === "proposed" ? "succeeded" : result.status},
          runtime_route = ${result.runtimeRoute}, completed_at = ${result.completedAt}
        where id = ${job.attemptId}
        returning trace_id
      `;
      const trace = traces[0]?.trace_id;
      if (!trace) throw new Error("Worker attempt trace disappeared");
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
            harnessRelease: HARNESS_RELEASE,
            runtimeRoute: result.runtimeRoute,
            evaluationRelease: job.skill.evaluationRelease,
            status: result.status,
            modelCalls: result.status === "expired" ? 0 : 1,
          })}, ${new Date(Date.now() + 7 * 86_400_000)}, ${result.completedAt}
        ) on conflict (worker_attempt_id) do nothing
      `;
      await transaction`
        update tasks set status = ${result.status === "proposed" ? "completed" : "failed"},
          completed_at = ${result.completedAt} where id = ${job.taskVersionId}
      `;
      return trace;
    });
    void traceId;
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

  private async loadPin(skillKey: string, version: number, releaseKey: string) {
    const rows = await this.database<{ skill_version_id: string; evaluation_release_id: string }[]>`
      select version.id as skill_version_id, evaluation.id as evaluation_release_id
      from skills skill
      join skill_versions version on version.skill_id = skill.id and version.version = ${version}
      join evaluation_releases evaluation on evaluation.id = version.evaluation_release_id
      where skill.skill_key = ${skillKey} and evaluation.release_key = ${releaseKey}
        and version.status = 'approved'
    `;
    const row = rows[0];
    if (!row) throw new Error(`Governed skill pin is not active: ${skillKey}@${version}`);
    return { skillVersionId: row.skill_version_id, evaluationReleaseId: row.evaluation_release_id };
  }
}

export async function bootstrapGovernedSkills(database: Database): Promise<void> {
  const skills = [...Object.values(PRODUCT_SKILLS), GENERAL_ANSWER_SKILL];
  await database.begin(async (transaction) => {
    for (const skill of skills) {
      const evaluationRows = await transaction<{ id: string }[]>`
        insert into evaluation_releases (id, release_key, suite_digest, status)
        values (${randomUUID()}, ${skill.evaluationRelease}, ${sha256Hex(`${skill.evaluationRelease}:initial-protected-suite`)}, 'active')
        on conflict (release_key) do update set release_key = excluded.release_key
        returning id
      `;
      const evaluationReleaseId = evaluationRows[0]?.id;
      if (!evaluationReleaseId) throw new Error("Evaluation release bootstrap failed");
      const skillRows = await transaction<{ id: string }[]>`
        insert into skills (id, skill_key, owner, purpose, risk_class)
        values (${randomUUID()}, ${skill.id}, 'florence-product', ${skill.purpose}, ${skill.riskClass})
        on conflict (skill_key) do update set purpose = excluded.purpose, risk_class = excluded.risk_class
        returning id
      `;
      const skillId = skillRows[0]?.id;
      if (!skillId) throw new Error("Skill bootstrap failed");
      const versionRows = await transaction<{ id: string }[]>`
        insert into skill_versions (
          id, skill_id, version, input_schema, output_schema, requested_capabilities,
          tool_ceiling, evaluation_release_id, status
        ) values (
          ${randomUUID()}, ${skillId}, ${skill.version},
          ${transaction.json({ type: "object", required: ["goal", "authorizedContext"] })},
          ${transaction.json(JSON.parse(JSON.stringify(z.toJSONSchema(skill.outputSchema))))},
          ${transaction.array([...skill.requestedCapabilities])}, ${transaction.array([])},
          ${evaluationReleaseId}, 'approved'
        ) on conflict (skill_id, version) do nothing
        returning id
      `;
      const existingVersion = versionRows[0]
        ? versionRows[0]
        : (
            await transaction<{ id: string }[]>`
            select id from skill_versions where skill_id = ${skillId} and version = ${skill.version}
          `
          )[0];
      const skillVersionId = existingVersion?.id;
      if (!skillVersionId) throw new Error("Skill version bootstrap failed");
      const active = await transaction<
        { id: string; skill_version_id: string; evaluation_release_id: string }[]
      >`
        select id, skill_version_id, evaluation_release_id
        from skill_release_events
        where skill_id = ${skillId} and channel = 'production' and active
        for update
      `;
      const currentRelease = active[0];
      if (
        !currentRelease ||
        currentRelease.skill_version_id !== skillVersionId ||
        currentRelease.evaluation_release_id !== evaluationReleaseId
      ) {
        if (currentRelease) {
          await transaction`
            update skill_release_events set active = false where id = ${currentRelease.id}
          `;
        }
        await transaction`
          insert into skill_release_events (
            id, skill_id, skill_version_id, channel, event_kind, active,
            evaluation_release_id, occurred_at
          ) values (
            ${randomUUID()}, ${skillId}, ${skillVersionId}, 'production', 'promoted', true,
            ${evaluationReleaseId}, now()
          )
        `;
      }
    }
  });
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
