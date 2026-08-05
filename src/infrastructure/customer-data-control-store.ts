import { createHash, randomUUID } from "node:crypto";
import type { JSONValue, TransactionSql } from "postgres";
import { z } from "zod";
import { ApplicationOutboxIntentSchema } from "../application/contracts.js";
import type { Database } from "../db/client.js";
import type { BlindIndex } from "../security/blind-index.js";
import { payloadDigest } from "../security/canonical-json.js";
import type { TenantJsonCipher } from "../security/tenant-json-cipher.js";

const instantSchema = z.iso.datetime({ offset: true });
const uuidSchema = z.uuid();
const idempotencyKeySchema = z.string().min(1).max(512);
const digestSchema = z.string().regex(/^[a-f0-9]{64}$/u);

export type CustomerDeletionStatus =
  | "awaiting_confirmations"
  | "fenced"
  | "cleaning"
  | "blocked"
  | "cancelled"
  | "expired";

export interface CustomerExportHandoffRecord {
  readonly handoffId: string;
  readonly householdId: string;
  readonly adultId: string;
  readonly privateChannelBindingId: string;
  readonly issuedAt: string;
  readonly expiresAt: string;
  readonly status: "issued" | "consumed" | "expired" | "cancelled";
  readonly tokenDigest: string;
}

export interface CustomerDeletionRecipient {
  readonly adultId: string;
  readonly privateChannelBindingId: string;
}

export interface CustomerDeletionRequestRecord {
  readonly requestId: string;
  readonly status: CustomerDeletionStatus;
  readonly requestedAt: string;
  readonly expiresAt: string;
  readonly recipients: readonly CustomerDeletionRecipient[];
  readonly confirmedAdultIds: readonly string[];
  readonly controlEpoch: number | null;
}

export type CleanupStepKind =
  | "google.gmail_watch.stop"
  | "google.calendar_watch.stop"
  | "google.oauth.revoke"
  | "local.finalize";

export interface CustomerDeletionCleanupLease {
  readonly rowId: string;
  readonly requestId: string;
  readonly householdId: string;
  readonly controlEpoch: number;
  readonly kind: CleanupStepKind;
  readonly connectionId: string | null;
  readonly calendarChannelId: string | null;
  readonly attempt: number;
  readonly leaseToken: string;
}

export interface CustomerCleanupConnection {
  readonly id: string;
  readonly householdId: string;
  readonly adultId: string;
  readonly externalAccountId: string;
  readonly encryptedCredentials: string;
  readonly grantedScopes: readonly string[];
  readonly cursor: Record<string, unknown>;
  readonly metadata: Record<string, unknown>;
}

export interface CustomerCleanupCalendarChannel {
  readonly channelId: string;
  readonly resourceId: string;
}

export class CustomerDataControlStoreError extends Error {
  public override readonly name = "CustomerDataControlStoreError";

  public constructor(
    public readonly code: "not_authorized" | "not_found" | "invalid_state" | "expired" | "consumed",
  ) {
    super(code);
  }
}

type DeletionRequestRow = {
  id: string;
  status: CustomerDeletionStatus | "completed";
  requested_at: Date;
  expires_at: Date;
  control_epoch: string | null;
};

export class PostgresCustomerDataControlStore {
  readonly #database: Database;
  readonly #blindIndex: BlindIndex;
  readonly #sensitiveJson: TenantJsonCipher;

  public constructor(database: Database, blindIndex: BlindIndex, sensitiveJson: TenantJsonCipher) {
    this.#database = database;
    this.#blindIndex = blindIndex;
    this.#sensitiveJson = sensitiveJson;
  }

  public async issueExportHandoff(input: {
    handoffId: string;
    householdId: string;
    adultId: string;
    channelId: string;
    idempotencyKey: string;
    tokenDigest: string;
    issuedAt: string;
    expiresAt: string;
  }): Promise<CustomerExportHandoffRecord> {
    const parsed = z
      .strictObject({
        handoffId: uuidSchema,
        householdId: uuidSchema,
        adultId: uuidSchema,
        channelId: z.string().min(1).max(500),
        idempotencyKey: idempotencyKeySchema,
        tokenDigest: digestSchema,
        issuedAt: instantSchema,
        expiresAt: instantSchema,
      })
      .refine((value) => Date.parse(value.expiresAt) > Date.parse(value.issuedAt))
      .parse(input);

    return this.#database.begin(async (transaction) => {
      const bindingId = await requireActivePrivateDm(
        transaction,
        parsed.householdId,
        parsed.adultId,
        parsed.channelId,
      );
      const inserted = await transaction<{ id: string }[]>`
        insert into customer_export_handoffs (
          id, household_id, adult_id, private_channel_binding_id, idempotency_key,
          token_digest, status, issued_at, expires_at
        ) values (
          ${parsed.handoffId}, ${parsed.householdId}, ${parsed.adultId}, ${bindingId},
          ${parsed.idempotencyKey}, ${parsed.tokenDigest}, 'issued', ${parsed.issuedAt},
          ${parsed.expiresAt}
        )
        on conflict (household_id, adult_id, idempotency_key) do nothing
        returning id
      `;
      const rows = await transaction<
        {
          id: string;
          household_id: string;
          adult_id: string;
          private_channel_binding_id: string;
          token_digest: string;
          status: CustomerExportHandoffRecord["status"];
          issued_at: Date;
          expires_at: Date;
        }[]
      >`
        select id, household_id, adult_id, private_channel_binding_id, token_digest,
          status, issued_at, expires_at
        from customer_export_handoffs
        where household_id = ${parsed.householdId} and adult_id = ${parsed.adultId}
          and idempotency_key = ${parsed.idempotencyKey}
        for update
      `;
      const row = rows[0];
      if (!row || row.id !== parsed.handoffId || row.private_channel_binding_id !== bindingId) {
        throw new CustomerDataControlStoreError("invalid_state");
      }
      if (inserted[0]) {
        await appendAudit(transaction, this.#database, {
          householdId: parsed.householdId,
          adultId: parsed.adultId,
          action: "customer_export.issued",
          targetId: parsed.handoffId,
          occurredAt: parsed.issuedAt,
        });
      }
      return mapExport(row);
    });
  }

  public async consumeExportHandoff(input: {
    handoffId: string;
    tokenDigest: string;
    consumedAt: string;
  }): Promise<
    | { readonly status: "consumed"; readonly householdId: string; readonly adultId: string }
    | { readonly status: "expired" | "invalid" | "already_consumed" }
  > {
    const parsed = z
      .strictObject({ handoffId: uuidSchema, tokenDigest: digestSchema, consumedAt: instantSchema })
      .parse(input);
    return this.#database.begin(async (transaction) => {
      const rows = await transaction<
        {
          household_id: string;
          adult_id: string;
          private_channel_binding_id: string;
          token_digest: string;
          status: CustomerExportHandoffRecord["status"];
          expires_at: Date;
        }[]
      >`
        select household_id, adult_id, private_channel_binding_id, token_digest, status, expires_at
        from customer_export_handoffs where id = ${parsed.handoffId} for update
      `;
      const row = rows[0];
      if (!row || row.token_digest !== parsed.tokenDigest) return { status: "invalid" as const };
      if (row.status === "consumed") return { status: "already_consumed" as const };
      if (row.status !== "issued") return { status: "invalid" as const };
      if (row.expires_at.getTime() <= Date.parse(parsed.consumedAt)) {
        await transaction`
          update customer_export_handoffs set status = 'expired'
          where id = ${parsed.handoffId} and status = 'issued'
        `;
        return { status: "expired" as const };
      }
      const authorized = await transaction<{ ok: boolean }[]>`
        select true as ok
        from households h
        join household_memberships hm on hm.household_id = h.id
        join channel_bindings cb
          on cb.household_id = hm.household_id and cb.adult_id = hm.adult_id
        where h.id = ${row.household_id} and h.status <> 'deleting'
          and hm.adult_id = ${row.adult_id} and hm.status = 'active'
          and cb.id = ${row.private_channel_binding_id} and cb.channel_type = 'private'
          and cb.status = 'active'
        for update of h, hm, cb
      `;
      if (!authorized[0]) return { status: "invalid" as const };
      const updated = await transaction<{ id: string }[]>`
        update customer_export_handoffs
        set status = 'consumed', consumed_at = ${parsed.consumedAt}
        where id = ${parsed.handoffId} and status = 'issued'
        returning id
      `;
      if (!updated[0]) return { status: "already_consumed" as const };
      await appendAudit(transaction, this.#database, {
        householdId: row.household_id,
        adultId: row.adult_id,
        action: "customer_export.consumed",
        targetId: parsed.handoffId,
        occurredAt: parsed.consumedAt,
      });
      return { status: "consumed" as const, householdId: row.household_id, adultId: row.adult_id };
    });
  }

  public async beginDeletion(input: {
    requestId: string;
    householdId: string;
    adultId: string;
    channelId: string;
    requestCodeDigest: string;
    requestedAt: string;
    expiresAt: string;
    challengeDigest(input: {
      requestId: string;
      adultId: string;
      privateChannelBindingId: string;
      expiresAt: string;
    }): string;
  }): Promise<CustomerDeletionRequestRecord> {
    const parsed = z
      .strictObject({
        requestId: uuidSchema,
        householdId: uuidSchema,
        adultId: uuidSchema,
        channelId: z.string().min(1).max(500),
        requestCodeDigest: digestSchema,
        requestedAt: instantSchema,
        expiresAt: instantSchema,
      })
      .refine((value) => Date.parse(value.expiresAt) > Date.parse(value.requestedAt))
      .parse(input);
    return this.#database.begin(async (transaction) => {
      const initiatingBindingId = await requireActivePrivateDm(
        transaction,
        parsed.householdId,
        parsed.adultId,
        parsed.channelId,
      );
      const existing = await loadOpenDeletion(transaction, parsed.householdId);
      if (existing) return loadDeletionRecord(transaction, existing);

      const recipients = await activeDeletionRecipients(transaction, parsed.householdId);
      if (recipients.length !== 2) throw new CustomerDataControlStoreError("invalid_state");
      const initiator = recipients.find((recipient) => recipient.adultId === parsed.adultId);
      if (!initiator || initiator.privateChannelBindingId !== initiatingBindingId) {
        throw new CustomerDataControlStoreError("not_authorized");
      }
      await transaction`
        insert into customer_deletion_requests (
          id, household_id, requested_by_adult_id, initiating_channel_binding_id,
          request_code_digest, status, requested_at, expires_at
        ) values (
          ${parsed.requestId}, ${parsed.householdId}, ${parsed.adultId}, ${initiatingBindingId},
          ${parsed.requestCodeDigest}, 'awaiting_confirmations', ${parsed.requestedAt},
          ${parsed.expiresAt}
        )
      `;
      for (const recipient of recipients) {
        const challengeDigest = digestSchema.parse(
          input.challengeDigest({
            requestId: parsed.requestId,
            adultId: recipient.adultId,
            privateChannelBindingId: recipient.privateChannelBindingId,
            expiresAt: parsed.expiresAt,
          }),
        );
        await transaction`
          insert into customer_deletion_confirmations (
            request_id, adult_id, private_channel_binding_id, challenge_digest
          ) values (
            ${parsed.requestId}, ${recipient.adultId}, ${recipient.privateChannelBindingId},
            ${challengeDigest}
          )
        `;
      }
      await appendAudit(transaction, this.#database, {
        householdId: parsed.householdId,
        adultId: parsed.adultId,
        action: "customer_deletion.requested",
        targetId: parsed.requestId,
        occurredAt: parsed.requestedAt,
      });
      return {
        requestId: parsed.requestId,
        status: "awaiting_confirmations",
        requestedAt: parsed.requestedAt,
        expiresAt: parsed.expiresAt,
        recipients,
        confirmedAdultIds: [],
        controlEpoch: null,
      };
    });
  }

  public async confirmDeletion(input: {
    householdId: string;
    adultId: string;
    channelId: string;
    commandDigest: string;
    idempotencyKey: string;
    confirmedAt: string;
  }): Promise<
    | { readonly status: "invalid" | "expired" | "already_confirmed" }
    | { readonly status: "waiting"; readonly remaining: number; readonly requestId: string }
    | { readonly status: "fenced"; readonly requestId: string; readonly controlEpoch: number }
  > {
    const parsed = z
      .strictObject({
        householdId: uuidSchema,
        adultId: uuidSchema,
        channelId: z.string().min(1).max(500),
        commandDigest: digestSchema,
        idempotencyKey: idempotencyKeySchema,
        confirmedAt: instantSchema,
      })
      .parse(input);
    return this.#database.begin(async (transaction) => {
      const bindingId = await requireActivePrivateDm(
        transaction,
        parsed.householdId,
        parsed.adultId,
        parsed.channelId,
        true,
      );
      const rows = await transaction<
        (DeletionRequestRow & {
          household_id: string;
          challenge_adult_id: string;
          confirmed_at: Date | null;
        })[]
      >`
        select request.id, request.household_id, request.status, request.requested_at,
          request.expires_at, request.control_epoch, confirmation.adult_id as challenge_adult_id,
          confirmation.confirmed_at
        from customer_deletion_requests request
        join customer_deletion_confirmations confirmation on confirmation.request_id = request.id
        where request.household_id = ${parsed.householdId}
          and confirmation.adult_id = ${parsed.adultId}
          and confirmation.private_channel_binding_id = ${bindingId}
          and confirmation.challenge_digest = ${parsed.commandDigest}
          and request.status in ('awaiting_confirmations', 'fenced', 'cleaning', 'blocked')
        for update of request, confirmation
      `;
      const request = rows[0];
      if (!request) return { status: "invalid" as const };
      if (request.status !== "awaiting_confirmations") {
        return { status: "already_confirmed" as const };
      }
      if (request.expires_at.getTime() <= Date.parse(parsed.confirmedAt)) {
        await transaction`
          update customer_deletion_requests set status = 'expired', updated_at = now()
          where id = ${request.id} and status = 'awaiting_confirmations'
        `;
        return { status: "expired" as const };
      }
      if (request.confirmed_at === null) {
        await transaction`
          update customer_deletion_confirmations set confirmed_at = ${parsed.confirmedAt}
          where request_id = ${request.id} and adult_id = ${parsed.adultId}
            and confirmed_at is null
        `;
        await appendAudit(transaction, this.#database, {
          householdId: parsed.householdId,
          adultId: parsed.adultId,
          action: "customer_deletion.confirmed",
          targetId: request.id,
          occurredAt: parsed.confirmedAt,
        });
      }
      const remainingRows = await transaction<{ count: string }[]>`
        select count(*)::text as count from customer_deletion_confirmations
        where request_id = ${request.id} and confirmed_at is null
      `;
      const remaining = Number(remainingRows[0]?.count ?? "0");
      if (remaining > 0) return { status: "waiting" as const, remaining, requestId: request.id };
      return this.#fenceHousehold(transaction, {
        requestId: request.id,
        householdId: parsed.householdId,
        currentInboxIdempotencyKey: parsed.idempotencyKey,
        fencedAt: parsed.confirmedAt,
      });
    });
  }

  public async cancelDeletion(input: {
    householdId: string;
    adultId: string;
    channelId: string;
    requestCodeDigest: string;
    cancelledAt: string;
  }): Promise<"cancelled" | "expired" | "invalid" | "too_late"> {
    const parsed = z
      .strictObject({
        householdId: uuidSchema,
        adultId: uuidSchema,
        channelId: z.string().min(1).max(500),
        requestCodeDigest: digestSchema,
        cancelledAt: instantSchema,
      })
      .parse(input);
    return this.#database.begin(async (transaction) => {
      const bindingId = await requireActivePrivateDm(
        transaction,
        parsed.householdId,
        parsed.adultId,
        parsed.channelId,
        true,
      );
      const rows = await transaction<{ id: string; status: CustomerDeletionStatus; expires_at: Date }[]>`
        select request.id, request.status, request.expires_at
        from customer_deletion_requests request
        join customer_deletion_confirmations confirmation on confirmation.request_id = request.id
        where request.household_id = ${parsed.householdId}
          and request.request_code_digest = ${parsed.requestCodeDigest}
          and confirmation.adult_id = ${parsed.adultId}
          and confirmation.private_channel_binding_id = ${bindingId}
          and request.status in ('awaiting_confirmations', 'fenced', 'cleaning', 'blocked')
        for update of request
      `;
      const request = rows[0];
      if (!request) return "invalid";
      if (request.status !== "awaiting_confirmations") return "too_late";
      if (request.expires_at.getTime() <= Date.parse(parsed.cancelledAt)) {
        await transaction`
          update customer_deletion_requests set status = 'expired', updated_at = now()
          where id = ${request.id}
        `;
        return "expired";
      }
      await transaction`
        update customer_deletion_requests
        set status = 'cancelled', cancelled_at = ${parsed.cancelledAt}, updated_at = now()
        where id = ${request.id} and status = 'awaiting_confirmations'
      `;
      await appendAudit(transaction, this.#database, {
        householdId: parsed.householdId,
        adultId: parsed.adultId,
        action: "customer_deletion.cancelled",
        targetId: request.id,
        occurredAt: parsed.cancelledAt,
      });
      return "cancelled";
    });
  }

  public async currentDeletion(
    householdId: string,
    adultId: string,
    channelId: string,
  ): Promise<CustomerDeletionRequestRecord | null> {
    const parsed = z
      .strictObject({ householdId: uuidSchema, adultId: uuidSchema, channelId: z.string().min(1).max(500) })
      .parse({ householdId, adultId, channelId });
    return this.#database.begin(async (transaction) => {
      await requireActivePrivateDm(transaction, parsed.householdId, parsed.adultId, parsed.channelId, true);
      const request = await loadOpenDeletion(transaction, parsed.householdId);
      return request ? loadDeletionRecord(transaction, request) : null;
    });
  }

  /** The only post-fence message path: one owner-private status at the current control epoch. */
  public async enqueueDeletionStatus(input: {
    householdId: string;
    adultId: string;
    channelId: string;
    idempotencyKey: string;
    body: string;
  }): Promise<void> {
    const parsed = z
      .strictObject({
        householdId: uuidSchema,
        adultId: uuidSchema,
        channelId: z.string().min(1).max(500),
        idempotencyKey: idempotencyKeySchema,
        body: z.string().trim().min(1).max(4_000),
      })
      .parse(input);
    await this.#database.begin(async (transaction) => {
      const bindingId = await requireActivePrivateDm(
        transaction,
        parsed.householdId,
        parsed.adultId,
        parsed.channelId,
        true,
      );
      const rows = await transaction<{ control_epoch: string; request_id: string }[]>`
        select household.control_epoch::text as control_epoch, request.id as request_id
        from households household
        join customer_deletion_requests request on request.household_id = household.id
        join customer_deletion_confirmations confirmation on confirmation.request_id = request.id
        where household.id = ${parsed.householdId}
          and household.status = 'deleting'
          and request.status in ('fenced', 'cleaning', 'blocked')
          and request.control_epoch = household.control_epoch
          and confirmation.adult_id = ${parsed.adultId}
          and confirmation.private_channel_binding_id = ${bindingId}
        for update of household, request
      `;
      const row = rows[0];
      if (!row) throw new CustomerDataControlStoreError("invalid_state");
      const controlEpoch = z.coerce.number().int().nonnegative().parse(row.control_epoch);
      const intentDigest = createHash("sha256")
        .update(`${row.request_id}\0${parsed.adultId}\0${parsed.idempotencyKey}`)
        .digest("hex");
      await insertControlMessage(transaction, this.#sensitiveJson, {
        householdId: parsed.householdId,
        adultId: parsed.adultId,
        intentId: `customer_control.deletion.fenced.status.${intentDigest}`,
        body: parsed.body,
        controlEpoch,
      });
    });
  }

  public async claimCleanupSteps(input: {
    owner: string;
    limit: number;
    leaseSeconds: number;
  }): Promise<readonly CustomerDeletionCleanupLease[]> {
    const parsed = z
      .strictObject({
        owner: z.string().min(1).max(200),
        limit: z.number().int().positive().max(100),
        leaseSeconds: z.number().int().positive().max(3_600),
      })
      .parse(input);
    const leaseToken = randomUUID();
    const rows = await this.#database<
      {
        id: string;
        request_id: string;
        household_id: string;
        control_epoch: string;
        kind: CleanupStepKind;
        connection_id: string | null;
        calendar_channel_id: string | null;
        attempt: number;
        lease_token: string;
      }[]
    >`
      with candidates as (
        select step.id
        from customer_deletion_cleanup_steps step
        join customer_deletion_requests request on request.id = step.request_id
        join households household on household.id = step.household_id
        where request.status in ('fenced', 'cleaning', 'blocked')
          and household.status = 'deleting'
          and household.control_epoch = request.control_epoch
          and (
            (step.status in ('pending', 'retry') and step.available_at <= now())
            or (step.status = 'leased' and step.lease_expires_at < now())
          )
          and not exists (
            select 1 from customer_deletion_cleanup_steps predecessor
            where predecessor.request_id = step.request_id
              and predecessor.step_order < step.step_order
              and predecessor.status <> 'succeeded'
          )
          and (
            step.kind <> 'local.finalize'
            or not exists (
              select 1 from outbox
              where household_id = step.household_id
                and intent_key like 'customer_control.deletion.fenced.%'
                and status in ('pending', 'retry', 'leased')
            )
          )
        order by step.available_at, step.step_order, step.created_at
        for update of step skip locked
        limit ${parsed.limit}
      )
      update customer_deletion_cleanup_steps step
      set status = 'leased', lease_owner = ${parsed.owner}, lease_token = ${leaseToken},
        lease_expires_at = now() + (${parsed.leaseSeconds} * interval '1 second'),
        attempt = attempt + 1, updated_at = now()
      from candidates, customer_deletion_requests request
      where step.id = candidates.id and request.id = step.request_id
      returning step.id, step.request_id, step.household_id, request.control_epoch,
        step.kind, step.connection_id, step.calendar_channel_id, step.attempt, step.lease_token
    `;
    if (rows.length > 0) {
      const requestIds = [...new Set(rows.map((row) => row.request_id))];
      await this.#database`
        update customer_deletion_requests set status = 'cleaning', safe_error_code = null,
          updated_at = now()
        where id = any(${requestIds}::uuid[]) and status in ('fenced', 'blocked')
      `;
    }
    return rows.map((row) => ({
      rowId: row.id,
      requestId: row.request_id,
      householdId: row.household_id,
      controlEpoch: Number(row.control_epoch),
      kind: row.kind,
      connectionId: row.connection_id,
      calendarChannelId: row.calendar_channel_id,
      attempt: row.attempt,
      leaseToken: row.lease_token,
    }));
  }

  public async renewCleanupStepLease(input: {
    rowId: string;
    leaseToken: string;
    leaseSeconds: number;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: uuidSchema,
        leaseToken: uuidSchema,
        leaseSeconds: z.number().int().positive().max(3_600),
      })
      .parse(input);
    const rows = await this.#database<{ id: string }[]>`
      update customer_deletion_cleanup_steps step
      set lease_expires_at = now() + (${parsed.leaseSeconds} * interval '1 second'),
        updated_at = now()
      from customer_deletion_requests request, households household
      where step.id = ${parsed.rowId} and step.status = 'leased'
        and step.lease_token = ${parsed.leaseToken} and step.lease_expires_at > now()
        and request.id = step.request_id
        and request.status in ('fenced', 'cleaning', 'blocked')
        and household.id = step.household_id and household.status = 'deleting'
        and request.control_epoch = household.control_epoch
      returning step.id
    `;
    return rows.length === 1;
  }

  public async loadCleanupConnection(input: {
    requestId: string;
    householdId: string;
    connectionId: string;
  }): Promise<CustomerCleanupConnection | null> {
    const parsed = z
      .strictObject({ requestId: uuidSchema, householdId: uuidSchema, connectionId: uuidSchema })
      .parse(input);
    const rows = await this.#database<
      {
        id: string;
        household_id: string;
        adult_id: string;
        external_account_id: string;
        encrypted_credentials: string | null;
        granted_scopes: string[];
        cursor: Record<string, unknown>;
        metadata: Record<string, unknown>;
      }[]
    >`
      select connection.id, connection.household_id, connection.adult_id,
        connection.external_account_id, connection.encrypted_credentials,
        connection.granted_scopes, connection.cursor, connection.metadata
      from external_connections connection
      join customer_deletion_requests request on request.household_id = connection.household_id
      where request.id = ${parsed.requestId} and request.status in ('fenced', 'cleaning', 'blocked')
        and connection.household_id = ${parsed.householdId} and connection.id = ${parsed.connectionId}
    `;
    const row = rows[0];
    if (!row || row.encrypted_credentials === null) return null;
    return {
      id: row.id,
      householdId: row.household_id,
      adultId: row.adult_id,
      externalAccountId: row.external_account_id,
      encryptedCredentials: row.encrypted_credentials,
      grantedScopes: row.granted_scopes,
      cursor: row.cursor,
      metadata: row.metadata,
    };
  }

  public async loadCleanupCalendarChannel(input: {
    requestId: string;
    householdId: string;
    connectionId: string;
    channelId: string;
  }): Promise<CustomerCleanupCalendarChannel | null> {
    const parsed = z
      .strictObject({
        requestId: uuidSchema,
        householdId: uuidSchema,
        connectionId: uuidSchema,
        channelId: z.string().min(1).max(500),
      })
      .parse(input);
    const rows = await this.#database<{ channel_id: string; resource_id: string }[]>`
      select channel.channel_id, channel.resource_id
      from google_calendar_channels channel
      join customer_deletion_requests request on request.household_id = channel.household_id
      where request.id = ${parsed.requestId} and request.status in ('fenced', 'cleaning', 'blocked')
        and channel.household_id = ${parsed.householdId}
        and channel.connection_id = ${parsed.connectionId}
        and channel.channel_id = ${parsed.channelId}
    `;
    const row = rows[0];
    return row ? { channelId: row.channel_id, resourceId: row.resource_id } : null;
  }

  public async completeCleanupStep(input: {
    rowId: string;
    leaseToken: string;
    completedAt: string;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({ rowId: uuidSchema, leaseToken: uuidSchema, completedAt: instantSchema })
      .parse(input);
    const rows = await this.#database<{ id: string }[]>`
      update customer_deletion_cleanup_steps step
      set status = 'succeeded', completed_at = ${parsed.completedAt}, safe_error_code = null,
        lease_owner = null, lease_token = null, lease_expires_at = null, updated_at = now()
      from customer_deletion_requests request, households household
      where step.id = ${parsed.rowId} and step.status = 'leased'
        and step.lease_token = ${parsed.leaseToken} and request.id = step.request_id
        and step.lease_expires_at > now()
        and household.id = step.household_id and household.status = 'deleting'
        and request.control_epoch = household.control_epoch
      returning step.id
    `;
    return rows.length === 1;
  }

  public async retryCleanupStep(input: {
    rowId: string;
    leaseToken: string;
    retryAt: string;
    safeErrorCode: string;
  }): Promise<boolean> {
    const parsed = z
      .strictObject({
        rowId: uuidSchema,
        leaseToken: uuidSchema,
        retryAt: instantSchema,
        safeErrorCode: z.string().regex(/^[a-z][a-z0-9_.-]{0,99}$/u),
      })
      .parse(input);
    return this.#database.begin(async (transaction) => {
      const rows = await transaction<{ request_id: string }[]>`
        update customer_deletion_cleanup_steps
        set status = 'retry', available_at = ${parsed.retryAt},
          safe_error_code = ${parsed.safeErrorCode}, lease_owner = null, lease_token = null,
          lease_expires_at = null, updated_at = now()
        where id = ${parsed.rowId} and status = 'leased' and lease_token = ${parsed.leaseToken}
          and lease_expires_at > now()
        returning request_id
      `;
      if (!rows[0]) return false;
      await transaction`
        update customer_deletion_requests set status = 'blocked',
          safe_error_code = ${parsed.safeErrorCode}, updated_at = now()
        where id = ${rows[0].request_id} and status in ('fenced', 'cleaning', 'blocked')
      `;
      return true;
    });
  }

  public async finalizeDeletion(input: {
    rowId: string;
    leaseToken: string;
    completedAt: string;
  }): Promise<{ readonly completed: boolean; readonly requestId?: string }> {
    const parsed = z
      .strictObject({ rowId: uuidSchema, leaseToken: uuidSchema, completedAt: instantSchema })
      .parse(input);
    return this.#database.begin(async (transaction) => {
      const steps = await transaction<
        { request_id: string; household_id: string; control_epoch: string; requested_by_adult_id: string }[]
      >`
        select step.request_id, step.household_id, request.control_epoch,
          request.requested_by_adult_id
        from customer_deletion_cleanup_steps step
        join customer_deletion_requests request on request.id = step.request_id
        join households household on household.id = step.household_id
        where step.id = ${parsed.rowId} and step.kind = 'local.finalize'
          and step.status = 'leased' and step.lease_token = ${parsed.leaseToken}
          and step.lease_expires_at > now()
          and request.status = 'cleaning' and household.status = 'deleting'
          and request.control_epoch = household.control_epoch
        for update of step, request, household
      `;
      const step = steps[0];
      if (!step) return { completed: false };
      const incomplete = await transaction<{ count: string }[]>`
        select count(*)::text as count from customer_deletion_cleanup_steps
        where request_id = ${step.request_id} and id <> ${parsed.rowId} and status <> 'succeeded'
      `;
      if (Number(incomplete[0]?.count ?? "1") !== 0) return { completed: false };

      const bindings = await transaction<
        { id: string; external_chat_id: string; external_handle: string | null }[]
      >`
        select id, external_chat_id, external_handle from channel_bindings
        where household_id = ${step.household_id} order by id
      `;
      const routingDigests = [
        ...new Set(
          bindings.flatMap((binding) => [
            this.#identityDigest("linq-chat", binding.external_chat_id),
            ...(binding.external_handle
              ? [this.#identityDigest("linq-handle", binding.external_handle)]
              : []),
          ]),
        ),
      ].sort();
      if (routingDigests.length === 0) throw new CustomerDataControlStoreError("invalid_state");

      const cleanupCounts = await transaction<{ kind: CleanupStepKind; count: string }[]>`
        select kind, count(*)::text as count from customer_deletion_cleanup_steps
        where request_id = ${step.request_id} group by kind order by kind
      `;
      await transaction`
        update external_connections
        set status = 'revoked', encrypted_credentials = null, granted_scopes = '{}',
          email_digest = null, details_key_id = null, details_ciphertext = null,
          cursor = '{}'::jsonb, metadata = '{"deleted":true}'::jsonb, updated_at = now()
        where household_id = ${step.household_id}
      `;
      await transaction`
        update channel_bindings set status = 'revoked', metadata = '{}'::jsonb, updated_at = now()
        where household_id = ${step.household_id}
      `;
      const inbox = await transaction<{ id: string }[]>`
        delete from provider_inbox where household_id = ${step.household_id} returning id
      `;
      if (bindings.length > 0) {
        const chatIds = bindings.map((binding) => binding.external_chat_id);
        const chatDigests = chatIds.map((chatId) => this.#identityDigest("linq-chat", chatId));
        await transaction`
          delete from provider_inbox
          where status in ('pending', 'leased', 'quarantined', 'dead')
            and routing_digests && ${chatDigests}::text[]
        `;
        await transaction`
          delete from channel_suppressions where external_chat_id = any(${chatIds})
        `;
      }
      const report = {
        schemaVersion: 1,
        outcome: "deleted",
        controlEpoch: Number(step.control_epoch),
        cleanupSteps: Object.fromEntries(cleanupCounts.map((item) => [item.kind, Number(item.count)])),
        localInboxRowsRemoved: inbox.length,
        residualExternalData: ["delivered_imessages", "original_google_data"],
        completedAt: parsed.completedAt,
      };
      await transaction`
        insert into customer_deletion_tombstones (
          request_id, household_digest, routing_digests, completed_at, report
        ) values (
          ${step.request_id}, ${this.#identityDigest("household", step.household_id)},
          ${routingDigests}, ${parsed.completedAt}, ${json(this.#database, report)}
        )
      `;
      const memberRows = await transaction<{ adult_id: string }[]>`
        select adult_id from household_memberships where household_id = ${step.household_id}
      `;
      await transaction`delete from households where id = ${step.household_id}`;
      for (const member of memberRows) {
        await transaction`
          delete from adults where id = ${member.adult_id}
            and not exists (select 1 from household_memberships where adult_id = ${member.adult_id})
        `;
      }
      return { completed: true, requestId: step.request_id };
    });
  }

  public async isDeletedLinqIdentity(input: {
    externalChatId: string;
    externalHandle: string;
  }): Promise<boolean> {
    const chatId = z.string().min(1).max(500).parse(input.externalChatId);
    const handle = z.string().min(1).max(500).parse(input.externalHandle);
    const digests = [this.#identityDigest("linq-chat", chatId), this.#identityDigest("linq-handle", handle)];
    const rows = await this.#database<{ request_id: string }[]>`
      select request_id from customer_deletion_tombstones
      where routing_digests && ${digests}::text[] limit 1
    `;
    return rows.length === 1;
  }

  async #fenceHousehold(
    transaction: TransactionSql<Record<string, never>>,
    input: {
      requestId: string;
      householdId: string;
      currentInboxIdempotencyKey: string;
      fencedAt: string;
    },
  ): Promise<{ status: "fenced"; requestId: string; controlEpoch: number }> {
    const households = await transaction<{ status: string; control_epoch: string }[]>`
      select status, control_epoch from households where id = ${input.householdId} for update
    `;
    const household = households[0];
    if (!household) throw new CustomerDataControlStoreError("not_found");
    if (household.status === "deleting") {
      const requests = await transaction<{ control_epoch: string }[]>`
        select control_epoch from customer_deletion_requests where id = ${input.requestId}
      `;
      const epoch = requests[0]?.control_epoch;
      if (!epoch) throw new CustomerDataControlStoreError("invalid_state");
      return { status: "fenced", requestId: input.requestId, controlEpoch: Number(epoch) };
    }
    const controlEpoch = Number(household.control_epoch) + 1;

    const connections = await transaction<
      { id: string; cursor: Record<string, unknown>; encrypted_credentials: string | null }[]
    >`
      select id, cursor, encrypted_credentials from external_connections
      where household_id = ${input.householdId} order by id for update
    `;
    let order = 1;
    for (const connection of connections) {
      if (hasGmailWatch(connection.cursor)) {
        await insertCleanupStep(transaction, {
          requestId: input.requestId,
          householdId: input.householdId,
          connectionId: connection.id,
          stepKey: `gmail-watch:${connection.id}`,
          stepOrder: order++,
          kind: "google.gmail_watch.stop",
        });
      }
      const channels = await transaction<{ channel_id: string }[]>`
        select channel_id from google_calendar_channels
        where household_id = ${input.householdId} and connection_id = ${connection.id}
          and status in ('active', 'retiring')
        order by channel_id
      `;
      for (const channel of channels) {
        await insertCleanupStep(transaction, {
          requestId: input.requestId,
          householdId: input.householdId,
          connectionId: connection.id,
          calendarChannelId: channel.channel_id,
          stepKey: `calendar-watch:${channel.channel_id}`,
          stepOrder: order++,
          kind: "google.calendar_watch.stop",
        });
      }
      if (connection.encrypted_credentials !== null) {
        await insertCleanupStep(transaction, {
          requestId: input.requestId,
          householdId: input.householdId,
          connectionId: connection.id,
          stepKey: `google-grant:${connection.id}`,
          stepOrder: order++,
          kind: "google.oauth.revoke",
        });
      }
    }
    await insertCleanupStep(transaction, {
      requestId: input.requestId,
      householdId: input.householdId,
      stepKey: "local-finalize",
      stepOrder: order,
      kind: "local.finalize",
    });

    const recipients = await activeDeletionRecipients(transaction, input.householdId);
    for (const recipient of recipients) {
      await insertControlMessage(transaction, this.#sensitiveJson, {
        householdId: input.householdId,
        adultId: recipient.adultId,
        intentId: `customer_control.deletion.fenced.${input.requestId}.${recipient.adultId}`,
        body: "Both adults confirmed. Florence has fenced the household and is now disconnecting providers and deleting its local copy. No normal reminders, sync, or messages can run while cleanup finishes.",
        controlEpoch,
      });
    }
    await transaction`
      update jobs set status = 'cancelled', lease_owner = null, lease_token = null,
        lease_expires_at = null, payload_key_id = null, payload_ciphertext = null,
        updated_at = now()
      where household_id = ${input.householdId}
        and status in ('pending', 'retry', 'leased')
    `;
    await transaction`
      update scheduled_triggers set status = 'cancelled', lease_owner = null, lease_token = null,
        lease_expires_at = null, due_at = null, payload_key_id = null, payload_ciphertext = null,
        updated_at = now()
      where household_id = ${input.householdId} and status in ('scheduled', 'claimed')
    `;
    await transaction`
      update daily_brief_runs set status = 'dead', dead_at = ${input.fencedAt},
        last_error_code = 'customer_deletion_fenced', lease_owner = null,
        lease_token = null, lease_expires_at = null, updated_at = now()
      where household_id = ${input.householdId} and status in ('pending', 'retry', 'leased')
    `;
    await transaction`
      update private_review_items review
      set digest_run_id = null, updated_at = now()
      from daily_brief_runs run
      where review.digest_run_id = run.id and run.household_id = ${input.householdId}
        and run.status = 'dead' and run.kind = 'private_review'
        and review.reviewed_at is null
    `;
    await transaction`
      update outbox set status = 'cancelled', lease_owner = null, lease_token = null,
        lease_expires_at = null, payload_key_id = null, payload_ciphertext = null,
        updated_at = now()
      where household_id = ${input.householdId}
        and status in ('pending', 'retry', 'leased', 'ambiguous')
        and intent_key not like 'customer_control.deletion.fenced.%'
    `;
    await transaction`
      update channel_bindings set status = 'paused', updated_at = now()
      where household_id = ${input.householdId} and channel_type = 'group' and status <> 'revoked'
    `;
    await transaction`
      update external_connections
      set status = 'error', metadata = metadata || ${json(this.#database, { deletionFenced: true })},
        updated_at = now()
      where household_id = ${input.householdId} and status <> 'revoked'
    `;
    const currentInboxDigest = this.#blindIndex.digest(
      "provider-idempotency",
      `linq\0${input.currentInboxIdempotencyKey}`,
    );
    await transaction`
      delete from provider_inbox
      where household_id = ${input.householdId}
        and idempotency_digest <> ${currentInboxDigest}
        and status in ('pending', 'leased', 'quarantined', 'dead')
    `;
    await transaction`
      update households set status = 'deleting', control_epoch = ${controlEpoch}, updated_at = now()
      where id = ${input.householdId}
    `;
    await transaction`
      update customer_deletion_requests
      set status = 'fenced', fenced_at = ${input.fencedAt}, control_epoch = ${controlEpoch},
        safe_error_code = null, updated_at = now()
      where id = ${input.requestId} and status = 'awaiting_confirmations'
    `;
    return { status: "fenced", requestId: input.requestId, controlEpoch };
  }

  #identityDigest(kind: string, value: string): string {
    return this.#blindIndex.digest(kind, value);
  }
}

async function requireActivePrivateDm(
  transaction: TransactionSql<Record<string, never>>,
  householdId: string,
  adultId: string,
  channelId: string,
  allowDeleting = false,
): Promise<string> {
  const rows = await transaction<{ id: string }[]>`
    select cb.id
    from households h
    join household_memberships hm on hm.household_id = h.id
    join channel_bindings cb
      on cb.household_id = hm.household_id and cb.adult_id = hm.adult_id
    where h.id = ${householdId} and (${allowDeleting} or h.status <> 'deleting')
      and hm.adult_id = ${adultId} and hm.status = 'active'
      and cb.provider = 'linq' and cb.channel_type = 'private'
      and cb.external_chat_id = ${channelId} and cb.status = 'active'
    for update of h, hm, cb
  `;
  if (rows.length !== 1) throw new CustomerDataControlStoreError("not_authorized");
  return (rows[0] as { id: string }).id;
}

async function activeDeletionRecipients(
  transaction: TransactionSql<Record<string, never>>,
  householdId: string,
): Promise<CustomerDeletionRecipient[]> {
  const rows = await transaction<{ adult_id: string; binding_id: string; binding_count: string }[]>`
    select hm.adult_id, min(cb.id::text)::uuid as binding_id, count(cb.id)::text as binding_count
    from household_memberships hm
    join channel_bindings cb
      on cb.household_id = hm.household_id and cb.adult_id = hm.adult_id
    where hm.household_id = ${householdId} and hm.status = 'active'
      and cb.provider = 'linq' and cb.channel_type = 'private' and cb.status = 'active'
    group by hm.adult_id order by hm.adult_id
  `;
  if (rows.some((row) => Number(row.binding_count) !== 1)) {
    throw new CustomerDataControlStoreError("invalid_state");
  }
  return rows.map((row) => ({
    adultId: row.adult_id,
    privateChannelBindingId: row.binding_id,
  }));
}

async function loadOpenDeletion(
  transaction: TransactionSql<Record<string, never>>,
  householdId: string,
): Promise<DeletionRequestRow | null> {
  const rows = await transaction<DeletionRequestRow[]>`
    select id, status, requested_at, expires_at, control_epoch
    from customer_deletion_requests
    where household_id = ${householdId}
      and status in ('awaiting_confirmations', 'fenced', 'cleaning', 'blocked')
    order by requested_at desc limit 1 for update
  `;
  return rows[0] ?? null;
}

async function loadDeletionRecord(
  transaction: TransactionSql<Record<string, never>>,
  request: DeletionRequestRow,
): Promise<CustomerDeletionRequestRecord> {
  const confirmations = await transaction<
    { adult_id: string; private_channel_binding_id: string; confirmed_at: Date | null }[]
  >`
    select adult_id, private_channel_binding_id, confirmed_at
    from customer_deletion_confirmations where request_id = ${request.id}
    order by adult_id
  `;
  if (request.status === "completed") throw new CustomerDataControlStoreError("invalid_state");
  return {
    requestId: request.id,
    status: request.status,
    requestedAt: request.requested_at.toISOString(),
    expiresAt: request.expires_at.toISOString(),
    recipients: confirmations.map((row) => ({
      adultId: row.adult_id,
      privateChannelBindingId: row.private_channel_binding_id,
    })),
    confirmedAdultIds: confirmations.filter((row) => row.confirmed_at !== null).map((row) => row.adult_id),
    controlEpoch: request.control_epoch === null ? null : Number(request.control_epoch),
  };
}

async function appendAudit(
  transaction: TransactionSql<Record<string, never>>,
  database: Database,
  input: {
    householdId: string;
    adultId: string;
    action: string;
    targetId: string;
    occurredAt: string;
  },
): Promise<void> {
  const rows = await transaction<{ next_audit_sequence: string }[]>`
    select next_audit_sequence from households where id = ${input.householdId} for update
  `;
  const sequence = Number(rows[0]?.next_audit_sequence);
  if (!Number.isSafeInteger(sequence)) throw new CustomerDataControlStoreError("invalid_state");
  await transaction`
    insert into audit_log (
      id, household_id, sequence, actor_kind, actor_id, action, target_type,
      target_id, visibility, owner_adult_id, source_refs, policy_refs, details
    ) values (
      ${randomUUID()}, ${input.householdId}, ${sequence}, 'adult', ${input.adultId},
      ${input.action}, 'customer_data_control', ${input.targetId}, 'personal',
      ${input.adultId}, '[]'::jsonb, '[]'::jsonb,
      ${json(database, { occurredAt: input.occurredAt })}
    )
  `;
  await transaction`
    update households set next_audit_sequence = ${sequence + 1}, updated_at = now()
    where id = ${input.householdId}
  `;
}

async function insertCleanupStep(
  transaction: TransactionSql<Record<string, never>>,
  input: {
    requestId: string;
    householdId: string;
    connectionId?: string;
    calendarChannelId?: string;
    stepKey: string;
    stepOrder: number;
    kind: CleanupStepKind;
  },
): Promise<void> {
  await transaction`
    insert into customer_deletion_cleanup_steps (
      id, request_id, household_id, connection_id, calendar_channel_id,
      step_key, step_order, kind, status
    ) values (
      ${randomUUID()}, ${input.requestId}, ${input.householdId}, ${input.connectionId ?? null},
      ${input.calendarChannelId ?? null}, ${input.stepKey}, ${input.stepOrder}, ${input.kind},
      'pending'
    )
    on conflict (request_id, step_key) do nothing
  `;
}

async function insertControlMessage(
  transaction: TransactionSql<Record<string, never>>,
  cipher: TenantJsonCipher,
  input: {
    householdId: string;
    adultId: string;
    intentId: string;
    body: string;
    controlEpoch: number;
  },
): Promise<void> {
  const intent = ApplicationOutboxIntentSchema.parse({
    intentId: input.intentId,
    householdId: input.householdId,
    idempotencyKey: `florence:${input.intentId}`,
    kind: "conversation.send",
    targetScope: { kind: "personal", adultId: input.adultId },
    messageClass: "status",
    body: input.body,
  });
  const rowId = randomUUID();
  const payloadHash = payloadDigest({ effectKind: intent.kind, payload: intent });
  const sealed = cipher.seal(intent, {
    tenant: { kind: "household", id: input.householdId },
    table: "outbox",
    rowId,
    field: "payload",
  });
  await transaction`
    insert into outbox (
      id, household_id, effect_kind, idempotency_key, payload_digest, payload_key_id,
      payload_ciphertext, status, intent_key, max_attempts, control_epoch
    ) values (
      ${rowId}, ${input.householdId}, ${intent.kind}, ${intent.idempotencyKey},
      ${payloadHash}, ${sealed.keyId}, ${sealed.ciphertext}, 'pending', ${intent.intentId},
      8, ${input.controlEpoch}
    )
    on conflict (household_id, intent_key) where intent_key is not null do nothing
  `;
}

function hasGmailWatch(cursor: Record<string, unknown>): boolean {
  const gmail = cursor.gmail;
  return (
    typeof gmail === "object" &&
    gmail !== null &&
    "watch" in gmail &&
    (gmail as Record<string, unknown>).watch !== null
  );
}

function mapExport(row: {
  id: string;
  household_id: string;
  adult_id: string;
  private_channel_binding_id: string;
  token_digest: string;
  status: CustomerExportHandoffRecord["status"];
  issued_at: Date;
  expires_at: Date;
}): CustomerExportHandoffRecord {
  return {
    handoffId: row.id,
    householdId: row.household_id,
    adultId: row.adult_id,
    privateChannelBindingId: row.private_channel_binding_id,
    issuedAt: row.issued_at.toISOString(),
    expiresAt: row.expires_at.toISOString(),
    status: row.status,
    tokenDigest: row.token_digest,
  };
}

function json(database: Database, value: unknown) {
  return database.json(JSON.parse(JSON.stringify(value)) as JSONValue);
}
