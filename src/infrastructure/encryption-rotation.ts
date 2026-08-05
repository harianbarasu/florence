import { randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { Database } from "../db/client.js";
import { calendarBusyWindowEncryptionContext } from "../security/calendar-busy-window-privacy.js";
import {
  adultIdentityDetailsContext,
  googleConnectionDetailsContext,
} from "../security/durable-identity-privacy.js";
import {
  type EncryptionContext,
  EncryptionError,
  type EncryptionTenantKind,
  type TenantJsonCipher,
} from "../security/tenant-json-cipher.js";

const DEFAULT_BATCH_SIZE = 100;
const MAXIMUM_BATCH_SIZE = 1_000;

export type EncryptionRotationErrorCode =
  | "authentication_failed"
  | "invalid_envelope"
  | "missing_encryption_key"
  | "rotation_batch_failed"
  | "rotation_in_progress"
  | "target_key_not_active"
  | "unknown_key";

export class EncryptionRotationError extends Error {
  public constructor(
    public readonly code: EncryptionRotationErrorCode,
    options?: ErrorOptions,
  ) {
    super(rotationErrorMessage(code), options);
    this.name = "EncryptionRotationError";
  }
}

/** Fails startup when any durable ciphertext references a key absent from this process. */
export async function assertEncryptionKeyringReady(
  database: Database,
  cipher: TenantJsonCipher,
): Promise<void> {
  const rows = await database<{ key_id: string }[]>`
    select distinct key_id
    from (
      select body_key_id as key_id from provider_inbox where body_key_id is not null
      union all select body_key_id from provider_inbox_conflicts where body_key_id is not null
      union all select state_key_id from household_projections
      union all select snapshot_key_id from application_snapshots
      union all select body_key_id from application_commits
      union all select body_key_id from household_signals
      union all select payload_key_id from jobs where payload_key_id is not null
      union all select payload_key_id from scheduled_triggers where payload_key_id is not null
      union all select payload_key_id from outbox where payload_key_id is not null
      union all select rule_key_id from personal_attention_rule_revisions
      union all select statement_key_id from personal_attention_rule_revisions
      union all select window_key_id from calendar_busy_windows
      union all select details_key_id from adult_identity_details
      union all select details_key_id from external_connections where details_key_id is not null
    ) referenced_keys
    order by key_id
  `;
  if (rows.some((row) => !cipher.hasKey(row.key_id))) {
    throw new EncryptionRotationError("missing_encryption_key");
  }
}

export interface EncryptionRotationBatchResult {
  readonly runId: string;
  readonly status: "running" | "completed";
  readonly rowsRewrapped: number;
  readonly totalRowsRewrapped: number;
}

type RotationRunRow = {
  id: string;
  target_key_id: string;
  rows_rewrapped: string;
};

type ProviderCiphertextRow = {
  id: string;
  encryption_tenant_kind: EncryptionTenantKind;
  encryption_tenant_id: string;
  body_key_id: string;
  body_ciphertext: string;
};

type HouseholdCiphertextRow = {
  id: string;
  household_id: string;
  key_id: string;
  ciphertext: string;
};

type PersonalAttentionCiphertextRow = {
  id: string;
  household_id: string;
  rule_key_id: string;
  rule_ciphertext: string;
  statement_key_id: string;
  statement_ciphertext: string;
};

type CalendarBusyWindowCiphertextRow = {
  connection_id: string;
  household_id: string;
  calendar_id: string;
  external_event_id: string;
  window_key_id: string;
  window_ciphertext: string;
};

/** Rewraps one bounded, transactional batch and persists enough state to resume after interruption. */
export class PostgresEncryptionRotation {
  public constructor(
    private readonly database: Database,
    private readonly cipher: TenantJsonCipher,
    private readonly targetKeyId: string,
  ) {
    if (!targetKeyId || !cipher.hasKey(targetKeyId)) {
      throw new EncryptionRotationError("unknown_key");
    }
    if (cipher.activeKeyId !== targetKeyId) {
      throw new EncryptionRotationError("target_key_not_active");
    }
  }

  public async resumeBatch(batchSize: number = DEFAULT_BATCH_SIZE): Promise<EncryptionRotationBatchResult> {
    if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > MAXIMUM_BATCH_SIZE) {
      throw new RangeError(`Encryption rotation batch size must be between 1 and ${MAXIMUM_BATCH_SIZE}.`);
    }

    const run = await this.#findOrCreateRun();
    try {
      return await this.database.begin(async (transaction) => {
        const lockedRuns = await transaction<RotationRunRow[]>`
          select id, target_key_id, rows_rewrapped::text
          from encryption_rotation_runs
          where id = ${run.id} and status = 'running'
          for update
        `;
        const lockedRun = lockedRuns[0];
        if (lockedRun === undefined || lockedRun.target_key_id !== this.targetKeyId) {
          throw new EncryptionRotationError("rotation_in_progress");
        }

        const rowsRewrapped = await this.#rewrapNextTable(transaction, batchSize);
        if (rowsRewrapped > 0) {
          const updated = await transaction<{ rows_rewrapped: string }[]>`
            update encryption_rotation_runs
            set rows_rewrapped = rows_rewrapped + ${rowsRewrapped}, updated_at = now()
            where id = ${run.id} and status = 'running'
            returning rows_rewrapped::text
          `;
          return {
            runId: run.id,
            status: "running" as const,
            rowsRewrapped,
            totalRowsRewrapped: parseCount(updated[0]?.rows_rewrapped),
          };
        }

        if (await hasPendingCiphertexts(transaction, this.targetKeyId)) {
          await transaction`
            update encryption_rotation_runs set updated_at = now()
            where id = ${run.id} and status = 'running'
          `;
          return {
            runId: run.id,
            status: "running" as const,
            rowsRewrapped: 0,
            totalRowsRewrapped: parseCount(lockedRun.rows_rewrapped),
          };
        }

        await transaction`
          update encryption_rotation_runs
          set status = 'completed', last_error_code = null,
            updated_at = now(), completed_at = now()
          where id = ${run.id} and status = 'running'
        `;
        return {
          runId: run.id,
          status: "completed" as const,
          rowsRewrapped: 0,
          totalRowsRewrapped: parseCount(lockedRun.rows_rewrapped),
        };
      });
    } catch (error) {
      const code = safeRotationErrorCode(error);
      try {
        await this.database`
          update encryption_rotation_runs
          set status = 'failed', last_error_code = ${code}, updated_at = now()
          where id = ${run.id} and status = 'running'
        `;
      } catch {
        // Preserve the original safe failure when the database is also unavailable.
      }
      if (error instanceof EncryptionRotationError) throw error;
      throw new EncryptionRotationError(code, { cause: error });
    }
  }

  async #findOrCreateRun(): Promise<RotationRunRow> {
    return this.database.begin(async (transaction) => {
      await transaction`
        select pg_advisory_xact_lock(hashtextextended('florence:encryption-rotation', 0))
      `;
      const running = await transaction<RotationRunRow[]>`
        select id, target_key_id, rows_rewrapped::text
        from encryption_rotation_runs where status = 'running'
        for update
      `;
      const existing = running[0];
      if (existing !== undefined) {
        if (existing.target_key_id !== this.targetKeyId) {
          throw new EncryptionRotationError("rotation_in_progress");
        }
        return existing;
      }

      const rows = await transaction<RotationRunRow[]>`
        insert into encryption_rotation_runs (id, target_key_id, status)
        values (${randomUUID()}, ${this.targetKeyId}, 'running')
        returning id, target_key_id, rows_rewrapped::text
      `;
      const created = rows[0];
      if (created === undefined) throw new EncryptionRotationError("rotation_batch_failed");
      return created;
    });
  }

  async #rewrapNextTable(
    transaction: TransactionSql<Record<string, never>>,
    batchSize: number,
  ): Promise<number> {
    const inbox = await transaction<ProviderCiphertextRow[]>`
      select id, encryption_tenant_kind, encryption_tenant_id, body_key_id, body_ciphertext
      from provider_inbox where body_key_id <> ${this.targetKeyId}
      order by id limit ${batchSize} for update skip locked
    `;
    if (inbox.length > 0) {
      for (const row of inbox) {
        const rotated = this.#rewrapProvider(row, "provider_inbox");
        await transaction`
          update provider_inbox set body_key_id = ${rotated.keyId},
            body_ciphertext = ${rotated.ciphertext}
          where id = ${row.id}
        `;
      }
      return inbox.length;
    }

    const conflicts = await transaction<ProviderCiphertextRow[]>`
      select id, encryption_tenant_kind, encryption_tenant_id, body_key_id, body_ciphertext
      from provider_inbox_conflicts where body_key_id <> ${this.targetKeyId}
      order by id limit ${batchSize} for update skip locked
    `;
    if (conflicts.length > 0) {
      for (const row of conflicts) {
        const rotated = this.#rewrapProvider(row, "provider_inbox_conflicts");
        await transaction`
          update provider_inbox_conflicts set body_key_id = ${rotated.keyId},
            body_ciphertext = ${rotated.ciphertext}
          where id = ${row.id}
        `;
      }
      return conflicts.length;
    }

    const projections = await transaction<HouseholdCiphertextRow[]>`
      select household_id as id, household_id, state_key_id as key_id,
        state_ciphertext as ciphertext
      from household_projections where state_key_id <> ${this.targetKeyId}
      order by household_id limit ${batchSize} for update skip locked
    `;
    if (projections.length > 0) {
      for (const row of projections) {
        const rotated = this.#rewrapHousehold(row, "household_projections", "state");
        await transaction`
          update household_projections set state_key_id = ${rotated.keyId},
            state_ciphertext = ${rotated.ciphertext}
          where household_id = ${row.household_id}
        `;
      }
      return projections.length;
    }

    const snapshots = await transaction<HouseholdCiphertextRow[]>`
      select household_id as id, household_id, snapshot_key_id as key_id,
        snapshot_ciphertext as ciphertext
      from application_snapshots where snapshot_key_id <> ${this.targetKeyId}
      order by household_id limit ${batchSize} for update skip locked
    `;
    if (snapshots.length > 0) {
      for (const row of snapshots) {
        const rotated = this.#rewrapHousehold(row, "application_snapshots", "snapshot");
        await transaction`
          update application_snapshots set snapshot_key_id = ${rotated.keyId},
            snapshot_ciphertext = ${rotated.ciphertext}
          where household_id = ${row.household_id}
        `;
      }
      return snapshots.length;
    }

    const commits = await transaction<HouseholdCiphertextRow[]>`
      select id, household_id, body_key_id as key_id, body_ciphertext as ciphertext
      from application_commits where body_key_id <> ${this.targetKeyId}
      order by id limit ${batchSize} for update skip locked
    `;
    if (commits.length > 0) {
      for (const row of commits) {
        const rotated = this.#rewrapHousehold(row, "application_commits", "body");
        await transaction`
          update application_commits set body_key_id = ${rotated.keyId},
            body_ciphertext = ${rotated.ciphertext}
          where id = ${row.id}
        `;
      }
      return commits.length;
    }

    const signals = await transaction<HouseholdCiphertextRow[]>`
      select id, household_id, body_key_id as key_id, body_ciphertext as ciphertext
      from household_signals where body_key_id <> ${this.targetKeyId}
      order by id limit ${batchSize} for update skip locked
    `;
    if (signals.length > 0) {
      for (const row of signals) {
        const rotated = this.#rewrapHousehold(row, "household_signals", "body");
        await transaction`
          update household_signals set body_key_id = ${rotated.keyId},
            body_ciphertext = ${rotated.ciphertext}
          where id = ${row.id}
        `;
      }
      return signals.length;
    }

    const personalAttention = await transaction<PersonalAttentionCiphertextRow[]>`
      select id, household_id, rule_key_id, rule_ciphertext, statement_key_id, statement_ciphertext
      from personal_attention_rule_revisions
      where rule_key_id <> ${this.targetKeyId} or statement_key_id <> ${this.targetKeyId}
      order by id limit ${batchSize} for update skip locked
    `;
    if (personalAttention.length > 0) {
      for (const row of personalAttention) {
        const rule = this.#rewrapHousehold(
          {
            id: row.id,
            household_id: row.household_id,
            key_id: row.rule_key_id,
            ciphertext: row.rule_ciphertext,
          },
          "personal_attention_rule_revisions",
          "rule",
        );
        const statement = this.#rewrapHousehold(
          {
            id: row.id,
            household_id: row.household_id,
            key_id: row.statement_key_id,
            ciphertext: row.statement_ciphertext,
          },
          "personal_attention_rule_revisions",
          "statement",
        );
        await transaction`
          update personal_attention_rule_revisions
          set rule_key_id = ${rule.keyId}, rule_ciphertext = ${rule.ciphertext},
            statement_key_id = ${statement.keyId}, statement_ciphertext = ${statement.ciphertext}
          where id = ${row.id}
        `;
      }
      return personalAttention.length;
    }

    const adultDetails = await transaction<HouseholdCiphertextRow[]>`
      select adult_id as id, household_id, details_key_id as key_id, details_ciphertext as ciphertext
      from adult_identity_details where details_key_id <> ${this.targetKeyId}
      order by household_id, adult_id limit ${batchSize} for update skip locked
    `;
    if (adultDetails.length > 0) {
      for (const row of adultDetails) {
        const rotated = this.#rewrap(
          { keyId: row.key_id, ciphertext: row.ciphertext },
          adultIdentityDetailsContext({ householdId: row.household_id, adultId: row.id }),
        );
        await transaction`
          update adult_identity_details
          set details_key_id = ${rotated.keyId}, details_ciphertext = ${rotated.ciphertext}
          where household_id = ${row.household_id} and adult_id = ${row.id}
        `;
      }
      return adultDetails.length;
    }

    const connectionDetails = await transaction<(HouseholdCiphertextRow & { adult_id: string })[]>`
      select id, household_id, adult_id, details_key_id as key_id, details_ciphertext as ciphertext
      from external_connections where details_key_id is not null and details_key_id <> ${this.targetKeyId}
      order by id limit ${batchSize} for update skip locked
    `;
    if (connectionDetails.length > 0) {
      for (const row of connectionDetails) {
        const rotated = this.#rewrap(
          { keyId: row.key_id, ciphertext: row.ciphertext },
          googleConnectionDetailsContext({
            householdId: row.household_id,
            adultId: row.adult_id,
            connectionId: row.id,
          }),
        );
        await transaction`
          update external_connections
          set details_key_id = ${rotated.keyId}, details_ciphertext = ${rotated.ciphertext}
          where id = ${row.id}
        `;
      }
      return connectionDetails.length;
    }

    const calendarBusyWindows = await transaction<CalendarBusyWindowCiphertextRow[]>`
      select connection_id, household_id, calendar_id, external_event_id, window_key_id, window_ciphertext
      from calendar_busy_windows where window_key_id <> ${this.targetKeyId}
      order by connection_id, calendar_id, external_event_id
      limit ${batchSize} for update skip locked
    `;
    if (calendarBusyWindows.length > 0) {
      for (const row of calendarBusyWindows) {
        const rotated = this.#rewrap(
          { keyId: row.window_key_id, ciphertext: row.window_ciphertext },
          calendarBusyWindowEncryptionContext({
            householdId: row.household_id,
            connectionId: row.connection_id,
            calendarId: row.calendar_id,
            externalEventId: row.external_event_id,
          }),
        );
        await transaction`
          update calendar_busy_windows
          set window_key_id = ${rotated.keyId}, window_ciphertext = ${rotated.ciphertext}
          where connection_id = ${row.connection_id} and calendar_id = ${row.calendar_id}
            and external_event_id = ${row.external_event_id}
        `;
      }
      return calendarBusyWindows.length;
    }

    const jobs = await transaction<HouseholdCiphertextRow[]>`
      select id, household_id, payload_key_id as key_id, payload_ciphertext as ciphertext
      from jobs where payload_key_id is not null and payload_key_id <> ${this.targetKeyId}
      order by id limit ${batchSize} for update skip locked
    `;
    if (jobs.length > 0) {
      for (const row of jobs) {
        const rotated = this.#rewrapHousehold(row, "jobs", "payload");
        await transaction`
          update jobs set payload_key_id = ${rotated.keyId},
            payload_ciphertext = ${rotated.ciphertext}
          where id = ${row.id}
        `;
      }
      return jobs.length;
    }

    const timers = await transaction<HouseholdCiphertextRow[]>`
      select id, household_id, payload_key_id as key_id, payload_ciphertext as ciphertext
      from scheduled_triggers
      where payload_key_id is not null and payload_key_id <> ${this.targetKeyId}
      order by id limit ${batchSize} for update skip locked
    `;
    if (timers.length > 0) {
      for (const row of timers) {
        const rotated = this.#rewrapHousehold(row, "scheduled_triggers", "payload");
        await transaction`
          update scheduled_triggers set payload_key_id = ${rotated.keyId},
            payload_ciphertext = ${rotated.ciphertext}
          where id = ${row.id}
        `;
      }
      return timers.length;
    }

    const outbox = await transaction<HouseholdCiphertextRow[]>`
      select id, household_id, payload_key_id as key_id, payload_ciphertext as ciphertext
      from outbox where payload_key_id is not null and payload_key_id <> ${this.targetKeyId}
      order by id limit ${batchSize} for update skip locked
    `;
    if (outbox.length > 0) {
      for (const row of outbox) {
        const rotated = this.#rewrapHousehold(row, "outbox", "payload");
        await transaction`
          update outbox set payload_key_id = ${rotated.keyId},
            payload_ciphertext = ${rotated.ciphertext}
          where id = ${row.id}
        `;
      }
    }
    return outbox.length;
  }

  #rewrapProvider(row: ProviderCiphertextRow, table: "provider_inbox" | "provider_inbox_conflicts") {
    return this.#rewrap(
      { keyId: row.body_key_id, ciphertext: row.body_ciphertext },
      {
        tenant: { kind: row.encryption_tenant_kind, id: row.encryption_tenant_id },
        table,
        rowId: row.id,
        field: "body",
      },
    );
  }

  #rewrapHousehold(
    row: HouseholdCiphertextRow,
    table:
      | "household_projections"
      | "application_snapshots"
      | "application_commits"
      | "household_signals"
      | "personal_attention_rule_revisions"
      | "jobs"
      | "scheduled_triggers"
      | "outbox",
    field: "state" | "snapshot" | "body" | "payload" | "rule" | "statement",
  ) {
    return this.#rewrap(
      { keyId: row.key_id, ciphertext: row.ciphertext },
      {
        tenant: { kind: "household", id: row.household_id },
        table,
        rowId: row.id,
        field,
      },
    );
  }

  #rewrap(sealed: { keyId: string; ciphertext: string }, context: EncryptionContext) {
    const rotated = this.cipher.rewrap(sealed, context);
    if (rotated.keyId !== this.targetKeyId) {
      throw new EncryptionRotationError("target_key_not_active");
    }
    return rotated;
  }
}

async function hasPendingCiphertexts(
  transaction: TransactionSql<Record<string, never>>,
  targetKeyId: string,
): Promise<boolean> {
  const rows = await transaction<{ pending: boolean }[]>`
    select
      exists(select 1 from provider_inbox where body_key_id <> ${targetKeyId})
      or exists(select 1 from provider_inbox_conflicts where body_key_id <> ${targetKeyId})
      or exists(select 1 from household_projections where state_key_id <> ${targetKeyId})
      or exists(select 1 from application_snapshots where snapshot_key_id <> ${targetKeyId})
      or exists(select 1 from application_commits where body_key_id <> ${targetKeyId})
      or exists(select 1 from household_signals where body_key_id <> ${targetKeyId})
      or exists(
        select 1 from personal_attention_rule_revisions
        where rule_key_id <> ${targetKeyId} or statement_key_id <> ${targetKeyId}
      )
      or exists(select 1 from calendar_busy_windows where window_key_id <> ${targetKeyId})
      or exists(select 1 from adult_identity_details where details_key_id <> ${targetKeyId})
      or exists(
        select 1 from external_connections
        where details_key_id is not null and details_key_id <> ${targetKeyId}
      )
      or exists(select 1 from jobs where payload_key_id is not null and payload_key_id <> ${targetKeyId})
      or exists(
        select 1 from scheduled_triggers
        where payload_key_id is not null and payload_key_id <> ${targetKeyId}
      )
      or exists(select 1 from outbox where payload_key_id is not null and payload_key_id <> ${targetKeyId})
      as pending
  `;
  return rows[0]?.pending === true;
}

function safeRotationErrorCode(error: unknown): EncryptionRotationErrorCode {
  if (error instanceof EncryptionRotationError) return error.code;
  if (error instanceof EncryptionError) return error.code;
  return "rotation_batch_failed";
}

function parseCount(value: string | undefined): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new EncryptionRotationError("rotation_batch_failed");
  }
  return parsed;
}

function rotationErrorMessage(code: EncryptionRotationErrorCode): string {
  switch (code) {
    case "missing_encryption_key":
      return "The encryption keyring cannot open all durable ciphertext.";
    case "rotation_in_progress":
      return "A different encryption rotation is already running.";
    case "target_key_not_active":
      return "The rotation target is not the active encryption key.";
    case "unknown_key":
      return "The encryption rotation references an unavailable key.";
    case "invalid_envelope":
      return "The encryption rotation found an invalid envelope.";
    case "authentication_failed":
      return "The encryption rotation could not authenticate a ciphertext.";
    case "rotation_batch_failed":
      return "The encryption rotation batch failed.";
  }
}
