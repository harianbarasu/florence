import { randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { Database } from "../db/client.js";
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
      select body_key_id as key_id from provider_inbox
      union all select body_key_id from provider_inbox_conflicts
      union all select state_key_id from household_projections
      union all select snapshot_key_id from application_snapshots
      union all select body_key_id from application_commits
      union all select body_key_id from household_signals
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
    }
    return signals.length;
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
    table: "household_projections" | "application_snapshots" | "application_commits" | "household_signals",
    field: "state" | "snapshot" | "body",
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
