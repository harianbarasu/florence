import { createHash, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { Database } from "../db/client.js";
import {
  type PersonalAttentionRule,
  PersonalAttentionRuleSchema,
  personalAttentionIdentity,
  personalAttentionStatement,
} from "../domain/index.js";
import { canonicalJson } from "../security/canonical-json.js";

const instantSchema = z.iso.datetime({ offset: true });
const digestSchema = z.string().regex(/^sha256:[a-f0-9]{64}$/u);
const stableReferenceSchema = z.string().trim().min(1).max(500);
const triageDecisionSchema = z.enum([
  "ignore",
  "retain_private",
  "private_review",
  "private_interrupt",
  "propose_family_episode",
]);

export type ActivePersonalAttentionRule = {
  readonly revisionId: string;
  readonly controlId: string;
  readonly ruleKey: string;
  readonly revision: number;
  readonly rule: PersonalAttentionRule;
  readonly statement: string;
  readonly occurredAt: string;
  readonly evaluatorReleaseId: string;
};

type RuleRevisionRow = {
  id: string;
  rule_key: string;
  revision: number;
  status: "active" | "revoked";
  rule: unknown;
  statement: string;
  source_content_digest: string;
  evaluator_release_id: string;
  occurred_at: Date;
};

export class PersonalAttentionStoreError extends Error {
  public override readonly name = "PersonalAttentionStoreError";

  public constructor(public readonly code: "not_authorized" | "invalid_state" | "conflict") {
    super(code);
  }
}

/** Append-only personal procedural preferences and their deterministic applications. */
export class PostgresPersonalAttentionStore {
  public constructor(private readonly database: Database) {}

  public async recordRelease(rawInput: {
    releaseId: string;
    promptDigest: string;
    schemaVersion: number;
    corpusDigest: string;
    modelRoute: Record<string, unknown>;
    status: "passed" | "failed";
    caseResults: readonly Record<string, unknown>[];
    evaluatedAt: string;
  }): Promise<void> {
    const input = z
      .strictObject({
        releaseId: digestSchema,
        promptDigest: digestSchema,
        schemaVersion: z.number().int().positive(),
        corpusDigest: digestSchema,
        modelRoute: z.record(z.string(), z.unknown()),
        status: z.enum(["passed", "failed"]),
        caseResults: z.array(z.record(z.string(), z.unknown())).max(100),
        evaluatedAt: instantSchema,
      })
      .parse(rawInput);
    const route = this.database.json(JSON.parse(canonicalJson(input.modelRoute)));
    const cases = this.database.json(JSON.parse(canonicalJson(input.caseResults)));
    await this.database.begin(async (transaction) => {
      const inserted = await transaction<{ id: string }[]>`
        insert into personal_learning_releases (
          id, prompt_digest, schema_version, corpus_digest, model_route,
          status, case_results, evaluated_at
        ) values (
          ${input.releaseId}, ${input.promptDigest}, ${input.schemaVersion},
          ${input.corpusDigest}, ${route}, ${input.status}, ${cases}, ${input.evaluatedAt}
        )
        on conflict do nothing
        returning id
      `;
      if (inserted.length === 1) return;
      const existing = await transaction<
        Array<{
          id: string;
          prompt_digest: string;
          schema_version: number;
          corpus_digest: string;
          model_route: unknown;
          status: "passed" | "failed";
          case_results: unknown;
          evaluated_at: Date;
        }>
      >`
        select id, prompt_digest, schema_version, corpus_digest, model_route,
          status, case_results, evaluated_at
        from personal_learning_releases
        where id = ${input.releaseId}
        for update
      `;
      const row = existing[0];
      if (
        !row ||
        row.prompt_digest !== input.promptDigest ||
        row.schema_version !== input.schemaVersion ||
        row.corpus_digest !== input.corpusDigest ||
        canonicalJson(row.model_route) !== canonicalJson(input.modelRoute) ||
        row.status !== input.status ||
        canonicalJson(row.case_results) !== canonicalJson(input.caseResults)
      ) {
        throw new PersonalAttentionStoreError("conflict");
      }
    });
  }

  public async appendExplicitRule(rawInput: {
    householdId: string;
    adultId: string;
    sourceMessageRef: string;
    sourceEventId: string;
    rawText: string;
    occurredAt: string;
    evaluatorReleaseId: string;
    rule: PersonalAttentionRule;
  }): Promise<ActivePersonalAttentionRule> {
    const input = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        sourceMessageRef: stableReferenceSchema,
        sourceEventId: stableReferenceSchema,
        rawText: z.string().min(1).max(20_000),
        occurredAt: instantSchema,
        evaluatorReleaseId: digestSchema,
        rule: PersonalAttentionRuleSchema,
      })
      .parse(rawInput);
    const ruleKey = digest(personalAttentionIdentity(input.rule));
    const sourceDigest = digest(input.rawText);
    return this.database.begin(async (transaction) => {
      await requireActivePersonalOwner(transaction, input.householdId, input.adultId);
      const release = await transaction<{ status: "passed" | "failed" }[]>`
        select status from personal_learning_releases
        where id = ${input.evaluatorReleaseId}
      `;
      if (release[0]?.status !== "passed") {
        throw new PersonalAttentionStoreError("invalid_state");
      }
      const bySource = await transaction<RuleRevisionRow[]>`
        select id, rule_key, revision, status, rule, statement, source_content_digest,
          evaluator_release_id, occurred_at
        from personal_attention_rule_revisions
        where household_id = ${input.householdId} and adult_id = ${input.adultId}
          and source_event_id = ${input.sourceEventId}
        for update
      `;
      if (bySource[0]) {
        const row = bySource[0];
        if (
          row.rule_key !== ruleKey ||
          row.source_content_digest !== sourceDigest ||
          canonicalJson(row.rule) !== canonicalJson(input.rule)
        ) {
          throw new PersonalAttentionStoreError("conflict");
        }
        return activeRule(row);
      }

      const latestRows = await transaction<RuleRevisionRow[]>`
        select id, rule_key, revision, status, rule, statement, source_content_digest,
          evaluator_release_id, occurred_at
        from personal_attention_rule_revisions
        where household_id = ${input.householdId} and adult_id = ${input.adultId}
          and rule_key = ${ruleKey}
        order by revision desc
        limit 1
        for update
      `;
      const latest = latestRows[0];
      const row: RuleRevisionRow = {
        id: randomUUID(),
        rule_key: ruleKey,
        revision: (latest?.revision ?? 0) + 1,
        status: "active",
        rule: input.rule,
        statement: personalAttentionStatement(input.rule),
        source_content_digest: sourceDigest,
        evaluator_release_id: input.evaluatorReleaseId,
        occurred_at: new Date(input.occurredAt),
      };
      await transaction`
        insert into personal_attention_rule_revisions (
          id, household_id, adult_id, rule_key, revision, supersedes_revision_id,
          status, rule, statement, source_message_ref, source_event_id,
          source_content_digest, evaluator_release_id, occurred_at
        ) values (
          ${row.id}, ${input.householdId}, ${input.adultId}, ${row.rule_key}, ${row.revision},
          ${latest?.id ?? null}, 'active',
          ${this.database.json(JSON.parse(canonicalJson(input.rule)))}, ${row.statement},
          ${input.sourceMessageRef}, ${input.sourceEventId}, ${sourceDigest},
          ${input.evaluatorReleaseId}, ${input.occurredAt}
        )
      `;
      await appendPersonalAudit(transaction, this.database, {
        householdId: input.householdId,
        adultId: input.adultId,
        action: latest ? "personal_attention.revised" : "personal_attention.learned",
        targetId: row.id,
        sourceMessageRef: input.sourceMessageRef,
        details: {
          controlId: controlId(row.rule_key, input.rule),
          revision: row.revision,
          statement: row.statement,
          supersedesRevisionId: latest?.id ?? null,
          evaluatorReleaseId: input.evaluatorReleaseId,
          occurredAt: input.occurredAt,
        },
      });
      return activeRule(row);
    });
  }

  public async listActive(rawInput: {
    householdId: string;
    adultId: string;
    asOf: string;
  }): Promise<readonly ActivePersonalAttentionRule[]> {
    const input = z
      .strictObject({ householdId: z.uuid(), adultId: z.uuid(), asOf: instantSchema })
      .parse(rawInput);
    const rows = await this.database<RuleRevisionRow[]>`
      select id, rule_key, revision, status, rule, statement, source_content_digest,
        evaluator_release_id, occurred_at
      from (
        select distinct on (rule_key)
          id, rule_key, revision, status, rule, statement, source_content_digest,
          evaluator_release_id, occurred_at
        from personal_attention_rule_revisions
        where household_id = ${input.householdId} and adult_id = ${input.adultId}
          and occurred_at <= ${input.asOf}
        order by rule_key, revision desc
      ) latest
      where status = 'active'
      order by rule_key
    `;
    return rows.map(activeRule);
  }

  public async revokeExact(rawInput: {
    householdId: string;
    adultId: string;
    controlId: string;
    sourceMessageRef: string;
    sourceEventId: string;
    rawText: string;
    occurredAt: string;
  }): Promise<{ status: "revoked" | "already_revoked" | "unknown"; rule?: ActivePersonalAttentionRule }> {
    const input = z
      .strictObject({
        householdId: z.uuid(),
        adultId: z.uuid(),
        controlId: z.string().regex(/^(?:PREF|ROUTE)-[A-F0-9]{16}$/u),
        sourceMessageRef: stableReferenceSchema,
        sourceEventId: stableReferenceSchema,
        rawText: z.string().min(1).max(20_000),
        occurredAt: instantSchema,
      })
      .parse(rawInput);
    return this.database.begin(async (transaction) => {
      await requireActivePersonalOwner(transaction, input.householdId, input.adultId);
      const duplicate = await transaction<RuleRevisionRow[]>`
        select id, rule_key, revision, status, rule, statement, source_content_digest,
          evaluator_release_id, occurred_at
        from personal_attention_rule_revisions
        where household_id = ${input.householdId} and adult_id = ${input.adultId}
          and source_event_id = ${input.sourceEventId}
        for update
      `;
      if (duplicate[0]) {
        return {
          status: duplicate[0].status === "revoked" ? "already_revoked" : "unknown",
          rule: activeRule(duplicate[0]),
        };
      }
      const allRows = await transaction<RuleRevisionRow[]>`
        select id, rule_key, revision, status, rule, statement, source_content_digest,
          evaluator_release_id, occurred_at
        from personal_attention_rule_revisions
        where household_id = ${input.householdId} and adult_id = ${input.adultId}
        order by rule_key, revision desc
        for update
      `;
      const latestRows = allRows.filter(
        (row, index) => index === 0 || allRows[index - 1]?.rule_key !== row.rule_key,
      );
      const latest = latestRows.find(
        (row) => controlId(row.rule_key, PersonalAttentionRuleSchema.parse(row.rule)) === input.controlId,
      );
      if (!latest) return { status: "unknown" };
      if (latest.status === "revoked") return { status: "already_revoked" };
      const revisionId = randomUUID();
      const revision = latest.revision + 1;
      const rule = PersonalAttentionRuleSchema.parse(latest.rule);
      await transaction`
        insert into personal_attention_rule_revisions (
          id, household_id, adult_id, rule_key, revision, supersedes_revision_id,
          status, rule, statement, source_message_ref, source_event_id,
          source_content_digest, evaluator_release_id, occurred_at
        ) values (
          ${revisionId}, ${input.householdId}, ${input.adultId}, ${latest.rule_key}, ${revision},
          ${latest.id}, 'revoked', ${this.database.json(JSON.parse(canonicalJson(rule)))},
          ${latest.statement}, ${input.sourceMessageRef}, ${input.sourceEventId},
          ${digest(input.rawText)}, ${latest.evaluator_release_id}, ${input.occurredAt}
        )
      `;
      await appendPersonalAudit(transaction, this.database, {
        householdId: input.householdId,
        adultId: input.adultId,
        action: "personal_attention.revoked",
        targetId: revisionId,
        sourceMessageRef: input.sourceMessageRef,
        details: {
          controlId: input.controlId,
          revision,
          statement: latest.statement,
          supersedesRevisionId: latest.id,
          occurredAt: input.occurredAt,
        },
      });
      return {
        status: "revoked",
        rule: activeRule({
          ...latest,
          id: revisionId,
          revision,
          status: "revoked",
          occurred_at: new Date(input.occurredAt),
        }),
      };
    });
  }

  public async recordApplications(
    rawInputs: readonly {
      householdId: string;
      adultId: string;
      ruleRevisionId: string;
      provider: "gmail" | "calendar";
      sourceRef: string;
      sourceDigest: string;
      baselineDecision: z.infer<typeof triageDecisionSchema>;
      appliedDecision: z.infer<typeof triageDecisionSchema>;
      appliedAt: string;
    }[],
  ): Promise<void> {
    const inputs = z
      .array(
        z.strictObject({
          householdId: z.uuid(),
          adultId: z.uuid(),
          ruleRevisionId: z.uuid(),
          provider: z.enum(["gmail", "calendar"]),
          sourceRef: stableReferenceSchema,
          sourceDigest: digestSchema,
          baselineDecision: triageDecisionSchema,
          appliedDecision: triageDecisionSchema,
          appliedAt: instantSchema,
        }),
      )
      .max(100)
      .parse(rawInputs);
    if (inputs.length === 0) return;
    await this.database.begin(async (transaction) => {
      for (const input of inputs) {
        const authoritative = await transaction<{ id: string }[]>`
          select rule.id
          from personal_attention_rule_revisions rule
          where rule.id = ${input.ruleRevisionId}
            and rule.household_id = ${input.householdId} and rule.adult_id = ${input.adultId}
            and rule.status = 'active'
            and not exists (
              select 1 from personal_attention_rule_revisions newer
              where newer.household_id = rule.household_id and newer.adult_id = rule.adult_id
                and newer.rule_key = rule.rule_key and newer.revision > rule.revision
            )
        `;
        if (!authoritative[0]) throw new PersonalAttentionStoreError("invalid_state");
        await transaction`
          insert into personal_attention_applications (
            id, household_id, adult_id, rule_revision_id, provider, source_ref,
            source_digest, baseline_decision, applied_decision, applied_at
          )
          values (
            ${randomUUID()}, ${input.householdId}, ${input.adultId}, ${input.ruleRevisionId},
            ${input.provider}, ${input.sourceRef}, ${input.sourceDigest}, ${input.baselineDecision},
            ${input.appliedDecision}, ${input.appliedAt}
          )
          on conflict do nothing
        `;
      }
    });
  }

  public async exportForAdult(rawInput: { householdId: string; adultId: string; asOf: string }): Promise<{
    revisions: readonly Record<string, unknown>[];
    applications: readonly Record<string, unknown>[];
  }> {
    const input = z
      .strictObject({ householdId: z.uuid(), adultId: z.uuid(), asOf: instantSchema })
      .parse(rawInput);
    return this.database.begin("isolation level repeatable read read only", async (transaction) => {
      const revisions = await transaction<Record<string, unknown>[]>`
        select id, adult_id, rule_key, revision, supersedes_revision_id, status, rule,
          statement, sensitivity, source_message_ref, source_content_digest,
          evaluator_release_id, occurred_at, created_at
        from personal_attention_rule_revisions
        where household_id = ${input.householdId} and adult_id = ${input.adultId}
          and occurred_at <= ${input.asOf}
        order by rule_key, revision
      `;
      const applications = await transaction<Record<string, unknown>[]>`
        select id, adult_id, rule_revision_id, provider, source_ref, source_digest,
          baseline_decision, applied_decision, applied_at, created_at
        from personal_attention_applications
        where household_id = ${input.householdId} and adult_id = ${input.adultId}
          and applied_at <= ${input.asOf}
        order by applied_at, id
      `;
      return { revisions, applications };
    });
  }
}

async function requireActivePersonalOwner(
  transaction: TransactionSql<Record<string, never>>,
  householdId: string,
  adultId: string,
): Promise<void> {
  const rows = await transaction<{ status: string; membership_status: string }[]>`
    select h.status, hm.status as membership_status
    from households h
    join household_memberships hm on hm.household_id = h.id
    where h.id = ${householdId} and hm.adult_id = ${adultId}
    for update of h
  `;
  if (rows[0]?.membership_status !== "active") {
    throw new PersonalAttentionStoreError("not_authorized");
  }
  if (rows[0].status === "deleting") throw new PersonalAttentionStoreError("invalid_state");
}

async function appendPersonalAudit(
  transaction: TransactionSql<Record<string, never>>,
  database: Database,
  input: {
    householdId: string;
    adultId: string;
    action: string;
    targetId: string;
    sourceMessageRef: string;
    details: Record<string, unknown>;
  },
): Promise<void> {
  const rows = await transaction<{ next_audit_sequence: string }[]>`
    select next_audit_sequence from households where id = ${input.householdId} for update
  `;
  const sequence = Number(rows[0]?.next_audit_sequence);
  if (!Number.isSafeInteger(sequence)) throw new PersonalAttentionStoreError("invalid_state");
  await transaction`
    insert into audit_log (
      id, household_id, sequence, actor_kind, actor_id, action, target_type,
      target_id, visibility, owner_adult_id, source_refs, policy_refs, details
    ) values (
      ${randomUUID()}, ${input.householdId}, ${sequence}, 'adult', ${input.adultId},
      ${input.action}, 'personal_attention_rule', ${input.targetId}, 'personal',
      ${input.adultId},
      ${database.json([{ source: "linq", sourceRef: input.sourceMessageRef }])},
      '[]'::jsonb, ${database.json(JSON.parse(canonicalJson(input.details)))}
    )
  `;
  await transaction`
    update households set next_audit_sequence = ${sequence + 1}, updated_at = now()
    where id = ${input.householdId}
  `;
}

function activeRule(row: RuleRevisionRow): ActivePersonalAttentionRule {
  const rule = PersonalAttentionRuleSchema.parse(row.rule);
  return {
    revisionId: row.id,
    controlId: controlId(row.rule_key, rule),
    ruleKey: row.rule_key,
    revision: row.revision,
    rule,
    statement: row.statement,
    occurredAt: row.occurred_at.toISOString(),
    evaluatorReleaseId: row.evaluator_release_id,
  };
}

function controlId(ruleKey: string, rule: PersonalAttentionRule): string {
  const prefix = rule.kind === "preference" ? "PREF" : "ROUTE";
  return `${prefix}-${ruleKey.slice("sha256:".length, "sha256:".length + 16).toUpperCase()}`;
}

function digest(value: string): string {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}
