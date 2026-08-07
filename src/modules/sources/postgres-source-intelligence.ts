import { createHash, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { Database } from "../../db/client.js";
import { canonicalDigest, canonicalJson } from "../../shared/canonical-json.js";
import type { EncryptedValue, SecretBox } from "../../shared/crypto.js";
import { ConflictError, NotFoundError, StaleAuthorityError, UnauthorizedError } from "../../shared/errors.js";
import {
  type CalendarPrivacyMode,
  type ConversationSourceAccessMode,
  type IntegrationAccountKind,
  IntegrationAccountKindSchema,
  type IntegrationCapability,
  IntegrationCapabilitySchema,
  type IntegrationStatus,
  type IntegrationView,
  JsonObjectSchema,
  JsonValueSchema,
  type SourceArtifactKind,
  type SourceCommand,
  SourceCommandSchema,
  type SourceIntelligence,
  type SourceMutationResult,
  type SourceQuery,
  SourceQuerySchema,
  type SourceReadResult,
  type SourceScope,
  type SyncCursorState,
} from "./contracts.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

export interface SourceIntelligenceOptions {
  readonly rawRetentionDays: number;
  readonly maxSourceContentBytes?: number;
  readonly maxBlobBytes?: number;
  readonly maxDerivativeBytes?: number;
  readonly privateCandidateRetentionDays?: number;
}

interface IntegrationRow {
  readonly id: string;
  readonly person_id: string;
  readonly provider: string;
  readonly account_kind: string;
  readonly status: string;
  readonly credential_ciphertext: Buffer | null;
  readonly credential_key_version: string | null;
  readonly control_epoch: number | string;
  readonly connected_at: Date;
  readonly updated_at: Date;
}

interface SourceRevisionRow {
  readonly id: string;
  readonly source_object_id: string;
  readonly revision_number: number | string;
  readonly owner_person_id: string | null;
  readonly participant_epoch_id: string | null;
  readonly scope_digest: string;
  readonly content_digest: string;
  readonly content_ciphertext: Buffer | null;
  readonly content_key_version: string | null;
  readonly occurred_at: Date;
  readonly captured_at: Date;
  readonly retention_until: Date;
  readonly revoked_at: Date | null;
  readonly conversation_access_mode: string | null;
}

interface ScopeResolution {
  readonly ownerPersonId: string | null;
  readonly participantEpochId: string | null;
  readonly scopeDigest: string;
  readonly retentionSeconds: number | null;
  readonly conversationAccessMode: ConversationSourceAccessMode | null;
}

interface EncryptedColumn {
  readonly ciphertext: Buffer;
  readonly keyVersion: string;
}

const DAY_MS = 24 * 60 * 60 * 1_000;

/**
 * PostgreSQL adapter for Florence's source-intelligence module. This is the
 * only place provider-normalized source data becomes durable plaintext-free
 * evidence. All mutations own a transaction and all reads re-check scope.
 */
export class PostgresSourceIntelligence implements SourceIntelligence {
  readonly #rawRetentionMs: number;
  readonly #maxSourceContentBytes: number;
  readonly #maxBlobBytes: number;
  readonly #maxDerivativeBytes: number;
  readonly #privateCandidateRetentionMs: number;

  public constructor(
    private readonly database: Executor,
    private readonly secretBox: SecretBox,
    options: SourceIntelligenceOptions,
  ) {
    this.#rawRetentionMs = requirePositiveInteger(options.rawRetentionDays, "rawRetentionDays") * DAY_MS;
    this.#maxSourceContentBytes = options.maxSourceContentBytes ?? 2 * 1024 * 1024;
    this.#maxBlobBytes = options.maxBlobBytes ?? 15 * 1024 * 1024;
    this.#maxDerivativeBytes = options.maxDerivativeBytes ?? 2 * 1024 * 1024;
    this.#privateCandidateRetentionMs = (options.privateCandidateRetentionDays ?? 7) * DAY_MS;
    requirePositiveInteger(this.#maxSourceContentBytes, "maxSourceContentBytes");
    requirePositiveInteger(this.#maxBlobBytes, "maxBlobBytes");
    requirePositiveInteger(this.#maxDerivativeBytes, "maxDerivativeBytes");
    requirePositiveInteger(this.#privateCandidateRetentionMs, "privateCandidateRetentionMs");
  }

  public async apply(commandCandidate: SourceCommand): Promise<SourceMutationResult> {
    const command = SourceCommandSchema.parse(commandCandidate);
    switch (command.kind) {
      case "connect_integration":
        return this.connectIntegration(command);
      case "set_integration_status":
        return this.setIntegrationStatus(command);
      case "revoke_integration":
        return this.revokeIntegration(command);
      case "begin_oauth_attempt":
        return this.beginOAuthAttempt(command);
      case "consume_oauth_attempt":
        return this.consumeOAuthAttempt(command);
      case "checkpoint_cursor":
        return this.checkpointCursor(command);
      case "configure_calendar_privacy":
        return this.configureCalendarPrivacy(command);
      case "reset_integration_sync":
        return this.resetIntegrationSync(command);
      case "reconcile_calendar_catalog":
        return this.reconcileCalendarCatalog(command);
      case "ingest_source":
        return this.ingestSource(command);
      case "store_blob":
        return this.storeBlob(command);
      case "store_derivative":
        return this.storeDerivative(command);
      case "propose_private_candidate":
        return this.proposePrivateCandidate(command);
      case "review_private_candidate":
        return this.reviewPrivateCandidate(command);
      case "mark_source_deleted":
        return this.markSourceDeleted(command);
      case "invalidate_conversation_epoch":
        return this.invalidateConversationEpoch(command);
      case "grant_conversation_private_views":
        return this.grantConversationPrivateViews(command);
      case "sweep_retention":
        return this.sweepRetention(command);
    }
  }

  public async read(queryCandidate: SourceQuery): Promise<SourceReadResult> {
    const query = SourceQuerySchema.parse(queryCandidate);
    switch (query.kind) {
      case "integration_access":
        return this.readIntegrationAccess(query);
      case "integration_profile":
        return this.readIntegrationProfile(query);
      case "oauth_attempt_access":
        return this.readOAuthAttemptAccess(query);
      case "sync_cursor":
        return this.readCursor(query);
      case "calendar_privacy":
        return this.readCalendarPrivacy(query);
      case "source_revision":
        return this.readSourceRevision(query);
      case "source_blob":
        return this.readSourceBlob(query);
      case "source_derivative":
        return this.readSourceDerivative(query);
      case "pending_private_candidates":
        return this.readPendingPrivateCandidates(query);
    }
  }

  private async connectIntegration(
    command: Extract<SourceCommand, { kind: "connect_integration" }>,
  ): Promise<SourceMutationResult> {
    return inTransaction(this.database, async (transaction) => {
      const person = await requireRegisteredPerson(transaction, command.personId, true);
      if (Number(person.control_epoch) !== command.expectedPersonControlEpoch) {
        throw new StaleAuthorityError("Person authority changed before integration connection");
      }
      const lockKey = `integration:${command.provider}:${command.personId}:${command.externalSubjectDigest}`;
      await transaction`select pg_advisory_xact_lock(hashtextextended(${lockKey}, 0))`;

      const existingRows = await transaction<IntegrationRow[]>`
        select id, person_id, provider, account_kind, status, credential_ciphertext,
          credential_key_version, control_epoch, connected_at, updated_at
        from integrations
        where person_id = ${command.personId}
          and provider = ${command.provider}
          and external_subject_digest = ${command.externalSubjectDigest}
          and status in ('active', 'paused', 'reauth_required', 'error')
        order by connected_at desc
        limit 1
        for update
      `;
      const integrationId = existingRows[0]?.id ?? randomUUID();
      const priorCapabilities = existingRows[0]
        ? await loadActiveIntegrationCapabilities(
            transaction,
            integrationId,
            Number(existingRows[0].control_epoch),
          )
        : [];
      const priorCredentials = existingRows[0]?.credential_ciphertext
        ? JsonObjectSchema.parse(
            openJson(
              this.secretBox,
              existingRows[0].credential_ciphertext,
              integrationPurpose(integrationId),
            ),
          )
        : {};
      const credentials = sealJson(
        this.secretBox,
        JsonObjectSchema.parse({ ...priorCredentials, ...command.credentials }),
        integrationPurpose(integrationId),
        this.#maxSourceContentBytes,
      );
      const connectedAt = new Date(command.connectedAt);

      if (existingRows[0]) {
        const rows = await transaction<IntegrationRow[]>`
          update integrations
          set status = 'active',
              account_kind = ${command.accountKind},
              credential_ciphertext = ${credentials.ciphertext},
              credential_key_version = ${credentials.keyVersion},
              control_epoch = control_epoch + 1,
              connected_at = ${connectedAt},
              revoked_at = null,
              updated_at = ${connectedAt}
          where id = ${integrationId}
          returning id, person_id, provider, account_kind, status, credential_ciphertext,
            credential_key_version, control_epoch, connected_at, updated_at
        `;
        await replaceActiveIntegrationCapabilities(
          transaction,
          integrationId,
          command.activeCapabilities,
          connectedAt,
        );
        const removedCapabilities = priorCapabilities.filter(
          (capability) => !command.activeCapabilities.includes(capability),
        );
        if (removedCapabilities.includes("mail")) {
          await invalidateIntegrationArtifacts(
            transaction,
            integrationId,
            ["mail_message", "attachment_manifest"],
            connectedAt,
          );
          await transaction`
            delete from sync_cursors
            where integration_id = ${integrationId}
              and (resource_kind = 'gmail_history' or resource_kind like 'gmail_backfill:%')
          `;
        }
        if (removedCapabilities.includes("calendar")) {
          await transaction`
            update integration_grants
            set status = 'revoked', revoked_at = ${connectedAt}, version = version + 1
            where integration_id = ${integrationId}
              and grant_kind = 'calendar_privacy' and status = 'active'
          `;
          await invalidateIntegrationArtifacts(transaction, integrationId, ["calendar_event"], connectedAt);
          await transaction`
            delete from sync_cursors
            where integration_id = ${integrationId}
              and (resource_kind = 'calendar_catalog' or resource_kind like 'calendar:%')
          `;
        } else if (command.accountKind === "work" && existingRows[0].account_kind !== "work") {
          await transaction`
            update integration_grants
            set scope = jsonb_set(scope, '{mode}', to_jsonb('availability_only'::text), false),
                version = version + 1
            where integration_id = ${integrationId}
              and grant_kind = 'calendar_privacy' and status = 'active'
              and scope->>'mode' = 'full_private'
          `;
          await invalidateIntegrationArtifacts(transaction, integrationId, ["calendar_event"], connectedAt);
          await transaction`
            delete from sync_cursors
            where integration_id = ${integrationId} and resource_kind like 'calendar:%'
          `;
        }
        // Reconnection creates a new authority epoch. Re-scan every still-active
        // capability so work cancelled under the prior epoch cannot strand a
        // source revision that had not yet reached interpretation.
        if (command.activeCapabilities.includes("mail")) {
          await transaction`
            delete from sync_cursors
            where integration_id = ${integrationId}
              and (resource_kind = 'gmail_history' or resource_kind like 'gmail_backfill:%')
          `;
        }
        if (command.activeCapabilities.includes("calendar")) {
          await transaction`
            delete from sync_cursors
            where integration_id = ${integrationId}
              and (resource_kind = 'calendar_catalog' or resource_kind like 'calendar:%')
          `;
        }
        return {
          kind: "integration_connected",
          ...integrationView(requireRow(rows[0], "Integration update failed"), command.activeCapabilities),
        };
      }

      const rows = await transaction<IntegrationRow[]>`
        insert into integrations (
          id, person_id, provider, external_subject_digest, account_kind, status,
          credential_ciphertext, credential_key_version, control_epoch,
          connected_at, updated_at
        ) values (
          ${integrationId}, ${command.personId}, ${command.provider},
          ${command.externalSubjectDigest}, ${command.accountKind}, 'active',
          ${credentials.ciphertext}, ${credentials.keyVersion}, 1, ${connectedAt}, ${connectedAt}
        )
        returning id, person_id, provider, account_kind, status, credential_ciphertext,
          credential_key_version, control_epoch, connected_at, updated_at
      `;
      await replaceActiveIntegrationCapabilities(
        transaction,
        integrationId,
        command.activeCapabilities,
        connectedAt,
      );
      return {
        kind: "integration_connected",
        ...integrationView(requireRow(rows[0], "Integration insert failed"), command.activeCapabilities),
      };
    });
  }

  private async setIntegrationStatus(
    command: Extract<SourceCommand, { kind: "set_integration_status" }>,
  ): Promise<SourceMutationResult> {
    return inTransaction(this.database, async (transaction) => {
      const integration = await loadIntegrationForUpdate(
        transaction,
        command.integrationId,
        command.personId,
      );
      assertControlEpoch(integration, command.expectedControlEpoch);
      if (integration.status === "revoked")
        throw new UnauthorizedError("A revoked integration cannot change status");
      if (command.status === "active" && integration.credential_ciphertext === null) {
        throw new ConflictError("An integration without credentials cannot become active");
      }
      const rows = await transaction<IntegrationRow[]>`
        update integrations
        set status = ${command.status}, control_epoch = control_epoch + 1,
            updated_at = ${new Date(command.changedAt)}
        where id = ${command.integrationId}
        returning id, person_id, provider, account_kind, status, credential_ciphertext,
          credential_key_version, control_epoch, connected_at, updated_at
      `;
      const updated = requireRow(rows[0], "Integration status update failed");
      const activeCapabilities = await loadActiveIntegrationCapabilities(
        transaction,
        command.integrationId,
        Number(updated.control_epoch),
      );
      return {
        kind: "integration_status_changed",
        ...integrationView(updated, activeCapabilities),
      };
    });
  }

  private async revokeIntegration(
    command: Extract<SourceCommand, { kind: "revoke_integration" }>,
  ): Promise<SourceMutationResult> {
    return inTransaction(this.database, async (transaction) => {
      const integration = await loadIntegrationForUpdate(
        transaction,
        command.integrationId,
        command.personId,
      );
      assertControlEpoch(integration, command.expectedControlEpoch);
      if (integration.status === "revoked") {
        return {
          kind: "integration_revoked",
          integrationId: integration.id,
          controlEpoch: Number(integration.control_epoch),
          invalidatedRevisionCount: 0,
          revokedCandidateCount: 0,
        };
      }
      const revokedAt = new Date(command.revokedAt);
      const revisionRows = await transaction<{ readonly id: string }[]>`
        select revision.id
        from source_revisions revision
        join source_objects object on object.id = revision.source_object_id
        where object.integration_id = ${command.integrationId}
          and revision.revoked_at is null
        for update of revision
      `;
      const invalidation = await invalidateRevisions(
        transaction,
        revisionRows.map((row) => row.id),
        revokedAt,
      );
      await transaction`
        update source_objects set status = 'revoked', updated_at = ${revokedAt}
        where integration_id = ${command.integrationId}
      `;
      await transaction`
        update sync_cursors
        set cursor_ciphertext = null, cursor_key_version = null, state = 'exhausted',
            checkpoint_at = ${revokedAt}, updated_at = ${revokedAt}
        where integration_id = ${command.integrationId}
      `;
      await transaction`
        update integration_grants
        set status = 'revoked', revoked_at = ${revokedAt}
        where integration_id = ${command.integrationId} and status = 'active'
      `;
      await transaction`
        update integration_capabilities
        set status = 'revoked', revoked_at = ${revokedAt}, updated_at = ${revokedAt}
        where integration_id = ${command.integrationId} and status = 'active'
      `;
      const updated = await transaction<{ readonly control_epoch: number | string }[]>`
        update integrations
        set status = 'revoked', credential_ciphertext = null, credential_key_version = null,
            control_epoch = control_epoch + 1, revoked_at = ${revokedAt}, updated_at = ${revokedAt}
        where id = ${command.integrationId}
        returning control_epoch
      `;
      return {
        kind: "integration_revoked",
        integrationId: command.integrationId,
        controlEpoch: Number(requireRow(updated[0], "Integration revocation failed").control_epoch),
        invalidatedRevisionCount: invalidation.invalidatedRevisionCount,
        revokedCandidateCount: invalidation.revokedCandidateCount,
      };
    });
  }

  private async beginOAuthAttempt(
    command: Extract<SourceCommand, { kind: "begin_oauth_attempt" }>,
  ): Promise<SourceMutationResult> {
    const createdAt = new Date(command.createdAt);
    const expiresAt = new Date(command.expiresAt);
    if (expiresAt <= createdAt) throw new ConflictError("OAuth attempt must expire after creation");
    return inTransaction(this.database, async (transaction) => {
      const person = await requireRegisteredPerson(transaction, command.personId, true);
      if (Number(person.control_epoch) !== command.expectedPersonControlEpoch) {
        throw new StaleAuthorityError("Person authority changed before OAuth began");
      }
      const sessions = await transaction<{ readonly id: string }[]>`
        select id from person_sessions
        where id = ${command.initiatingSessionId}
          and person_id = ${command.personId}
          and revoked_at is null
          and idle_expires_at > ${createdAt}
          and absolute_expires_at > ${createdAt}
      `;
      if (!sessions[0]) throw new UnauthorizedError("OAuth requires the initiating active session");
      await transaction`select pg_advisory_xact_lock(hashtextextended(${`oauth:${command.stateDigest}`}, 0))`;
      const existing = await transaction<{ readonly id: string }[]>`
        select id from oauth_attempts where state_digest = ${command.stateDigest}
      `;
      if (existing[0]) throw new ConflictError("OAuth state has already been issued");
      const oauthAttemptId = randomUUID();
      const verifier = sealBytes(
        this.secretBox,
        Buffer.from(command.pkceVerifier, "utf8"),
        oauthPurpose(oauthAttemptId),
      );
      await transaction`
        insert into oauth_attempts (
          id, person_id, provider, state_digest, pkce_verifier_ciphertext,
          key_version, return_path, requested_capabilities, account_kind, initiating_session_id,
          person_control_epoch, expires_at, created_at
        ) values (
          ${oauthAttemptId}, ${command.personId}, ${command.provider}, ${command.stateDigest},
          ${verifier.ciphertext}, ${verifier.keyVersion}, ${command.returnPath},
          ${transaction.array(command.requestedCapabilities)}, ${command.accountKind},
          ${command.initiatingSessionId},
          ${command.expectedPersonControlEpoch}, ${expiresAt}, ${createdAt}
        )
      `;
      return {
        kind: "oauth_attempt_started",
        oauthAttemptId,
        expiresAt: expiresAt.toISOString(),
      };
    });
  }

  private async consumeOAuthAttempt(
    command: Extract<SourceCommand, { kind: "consume_oauth_attempt" }>,
  ): Promise<SourceMutationResult> {
    return inTransaction(this.database, async (transaction) => {
      const rows = await transaction<
        {
          readonly id: string;
          readonly person_id: string;
          readonly provider: string;
          readonly initiating_session_id: string | null;
          readonly pkce_verifier_ciphertext: Buffer;
          readonly return_path: string;
          readonly requested_capabilities: string[];
          readonly account_kind: string;
          readonly person_control_epoch: number | string;
          readonly current_control_epoch: number | string;
          readonly person_status: string;
          readonly expires_at: Date;
          readonly consumed_at: Date | null;
        }[]
      >`
        select attempt.id, attempt.person_id, attempt.provider,
          attempt.initiating_session_id, attempt.pkce_verifier_ciphertext, attempt.return_path,
          attempt.requested_capabilities, attempt.account_kind,
          attempt.person_control_epoch, attempt.expires_at, attempt.consumed_at,
          person.control_epoch as current_control_epoch, person.status as person_status
        from oauth_attempts attempt
        join people person on person.id = attempt.person_id
        where attempt.state_digest = ${command.stateDigest}
        for update of attempt, person
      `;
      const attempt = rows[0];
      if (!attempt || attempt.provider !== command.provider) {
        throw new NotFoundError("OAuth attempt does not exist");
      }
      if (attempt.initiating_session_id === null) {
        throw new UnauthorizedError("OAuth attempt is not bound to a Florence session");
      }
      const consumedAt = new Date(command.consumedAt);
      if (attempt.consumed_at !== null) throw new ConflictError("OAuth attempt was already consumed");
      if (attempt.expires_at <= consumedAt) throw new UnauthorizedError("OAuth attempt has expired");
      if (attempt.person_status !== "registered") throw new UnauthorizedError("OAuth owner is not active");
      if (Number(attempt.person_control_epoch) !== Number(attempt.current_control_epoch)) {
        throw new StaleAuthorityError("Person authority changed during OAuth");
      }
      const verifier = openBytes(
        this.secretBox,
        attempt.pkce_verifier_ciphertext,
        oauthPurpose(attempt.id),
      ).toString("utf8");
      await transaction`
        update oauth_attempts set consumed_at = ${consumedAt} where id = ${attempt.id}
      `;
      return {
        kind: "oauth_attempt_consumed",
        oauthAttemptId: attempt.id,
        personId: attempt.person_id,
        provider: attempt.provider,
        initiatingSessionId: attempt.initiating_session_id,
        pkceVerifier: verifier,
        returnPath: attempt.return_path,
        personControlEpoch: Number(attempt.person_control_epoch),
        requestedCapabilities: integrationCapabilities(attempt.requested_capabilities),
        accountKind: integrationAccountKind(attempt.account_kind),
      };
    });
  }

  private async checkpointCursor(
    command: Extract<SourceCommand, { kind: "checkpoint_cursor" }>,
  ): Promise<SourceMutationResult> {
    if (command.state === "active" && command.cursor === null) {
      throw new ConflictError("An active sync cursor requires cursor state");
    }
    return inTransaction(this.database, async (transaction) => {
      const integration = await loadIntegrationForUpdate(
        transaction,
        command.integrationId,
        command.personId,
      );
      assertControlEpoch(integration, command.expectedIntegrationControlEpoch);
      if (integration.status !== "active")
        throw new UnauthorizedError("Only an active integration may checkpoint");
      const lockKey = `cursor:${command.integrationId}:${command.resourceKind}`;
      await transaction`select pg_advisory_xact_lock(hashtextextended(${lockKey}, 0))`;
      const currentRows = await transaction<{ readonly updated_at: Date }[]>`
        select updated_at from sync_cursors
        where integration_id = ${command.integrationId} and resource_kind = ${command.resourceKind}
        for update
      `;
      const currentUpdatedAt = currentRows[0]?.updated_at.toISOString() ?? null;
      if (currentUpdatedAt !== command.expectedUpdatedAt) {
        throw new StaleAuthorityError("Sync cursor changed before checkpoint");
      }
      const sealed =
        command.cursor === null
          ? null
          : sealJson(
              this.secretBox,
              command.cursor,
              cursorPurpose(command.integrationId, command.resourceKind),
              this.#maxSourceContentBytes,
            );
      const updatedAt = new Date(command.updatedAt);
      await transaction`
        insert into sync_cursors (
          id, integration_id, resource_kind, cursor_ciphertext, cursor_key_version,
          state, checkpoint_at, updated_at
        ) values (
          ${randomUUID()}, ${command.integrationId}, ${command.resourceKind},
          ${sealed?.ciphertext ?? null}, ${sealed?.keyVersion ?? null}, ${command.state},
          ${command.checkpointAt === null ? null : new Date(command.checkpointAt)}, ${updatedAt}
        )
        on conflict (integration_id, resource_kind) do update
        set cursor_ciphertext = excluded.cursor_ciphertext,
            cursor_key_version = excluded.cursor_key_version,
            state = excluded.state,
            checkpoint_at = excluded.checkpoint_at,
            updated_at = excluded.updated_at
      `;
      return {
        kind: "cursor_checkpointed",
        integrationId: command.integrationId,
        resourceKind: command.resourceKind,
        state: command.state,
        checkpointAt: command.checkpointAt,
        updatedAt: command.updatedAt,
      };
    });
  }

  private async configureCalendarPrivacy(
    command: Extract<SourceCommand, { kind: "configure_calendar_privacy" }>,
  ): Promise<SourceMutationResult> {
    return inTransaction(this.database, async (transaction) => {
      const integration = await loadIntegrationForUpdate(
        transaction,
        command.integrationId,
        command.personId,
      );
      assertControlEpoch(integration, command.expectedIntegrationControlEpoch);
      if (integration.status === "revoked") {
        throw new UnauthorizedError("A revoked integration cannot change calendar privacy");
      }
      const lockKey = `calendar-privacy:${command.integrationId}:${command.calendarIdDigest}`;
      await transaction`select pg_advisory_xact_lock(hashtextextended(${lockKey}, 0))`;
      const versions = await transaction<
        { readonly version: number | string; readonly mode: string | null }[]
      >`
        select version, scope->>'mode' as mode
        from integration_grants
        where integration_id = ${command.integrationId}
          and grant_kind = 'calendar_privacy'
          and scope->>'calendarIdDigest' = ${command.calendarIdDigest}
        order by version desc
        limit 1
        for update
      `;
      const changedAt = new Date(command.changedAt);
      await transaction`
        update integration_grants
        set status = 'revoked', revoked_at = ${changedAt}
        where integration_id = ${command.integrationId}
          and grant_kind = 'calendar_privacy'
          and scope->>'calendarIdDigest' = ${command.calendarIdDigest}
          and status = 'active'
      `;
      const grantVersion = Number(versions[0]?.version ?? 0) + 1;
      await transaction`
        insert into integration_grants (
          id, integration_id, grant_kind, scope, status, version, created_at
        ) values (
          ${randomUUID()}, ${command.integrationId}, 'calendar_privacy',
          ${transaction.json({ calendarIdDigest: command.calendarIdDigest, mode: command.mode })},
          'active', ${grantVersion}, ${changedAt}
        )
      `;
      const previousMode = versions[0]?.mode;
      const permissionNarrowed =
        (previousMode === "full_private" && command.mode !== "full_private") ||
        (previousMode === "availability_only" && command.mode === "off");
      if (permissionNarrowed) {
        await invalidateIntegrationArtifacts(
          transaction,
          command.integrationId,
          ["calendar_event"],
          changedAt,
        );
        await transaction`
          update sync_cursors
          set cursor_ciphertext = null, cursor_key_version = null,
              state = 'expired', checkpoint_at = null, updated_at = ${changedAt}
          where integration_id = ${command.integrationId}
            and resource_kind like 'calendar:%'
        `;
      }
      const updated = await transaction<{ readonly control_epoch: number | string }[]>`
        update integrations
        set updated_at = ${changedAt}
        where id = ${command.integrationId}
        returning control_epoch
      `;
      return {
        kind: "calendar_privacy_configured",
        integrationId: command.integrationId,
        calendarIdDigest: command.calendarIdDigest,
        mode: command.mode,
        grantVersion,
        integrationControlEpoch: Number(
          requireRow(updated[0], "Integration calendar policy update failed").control_epoch,
        ),
      };
    });
  }

  private async resetIntegrationSync(
    command: Extract<SourceCommand, { kind: "reset_integration_sync" }>,
  ): Promise<SourceMutationResult> {
    return inTransaction(this.database, async (transaction) => {
      const integration = await loadIntegrationForUpdate(
        transaction,
        command.integrationId,
        command.personId,
      );
      assertControlEpoch(integration, command.expectedIntegrationControlEpoch);
      if (integration.status !== "active") {
        throw new UnauthorizedError("Only an active integration can recover synchronization");
      }
      const capabilities = await loadActiveIntegrationCapabilities(
        transaction,
        command.integrationId,
        command.expectedIntegrationControlEpoch,
      );
      if (!capabilities.includes(command.affectedCapability)) {
        throw new UnauthorizedError("The affected integration capability is not active");
      }
      const resetAt = new Date(command.resetAt);
      await invalidateIntegrationArtifacts(
        transaction,
        command.integrationId,
        command.affectedCapability === "mail" ? ["mail_message", "attachment_manifest"] : ["calendar_event"],
        resetAt,
      );
      await transaction`delete from sync_cursors where integration_id = ${command.integrationId}`;
      const updated = await transaction<{ readonly control_epoch: number | string }[]>`
        update integrations
        set control_epoch = control_epoch + 1, updated_at = ${resetAt}
        where id = ${command.integrationId}
        returning control_epoch
      `;
      return {
        kind: "integration_sync_reset",
        integrationId: command.integrationId,
        integrationControlEpoch: Number(
          requireRow(updated[0], "Integration synchronization reset failed").control_epoch,
        ),
        affectedCapability: command.affectedCapability,
      };
    });
  }

  private async reconcileCalendarCatalog(
    command: Extract<SourceCommand, { kind: "reconcile_calendar_catalog" }>,
  ): Promise<SourceMutationResult> {
    return inTransaction(this.database, async (transaction) => {
      const integration = await loadIntegrationForUpdate(
        transaction,
        command.integrationId,
        command.personId,
      );
      assertControlEpoch(integration, command.expectedIntegrationControlEpoch);
      if (integration.status !== "active") {
        throw new UnauthorizedError("Only an active integration can reconcile calendars");
      }
      const capabilities = await loadActiveIntegrationCapabilities(
        transaction,
        command.integrationId,
        command.expectedIntegrationControlEpoch,
      );
      if (!capabilities.includes("calendar")) {
        throw new UnauthorizedError("Calendar capability is not active");
      }
      const activeDigests = new Set(command.activeCalendarIdDigests);
      const configured = await transaction<
        {
          readonly calendar_id_digest: string;
          readonly mode: string;
        }[]
      >`
        select scope->>'calendarIdDigest' as calendar_id_digest, scope->>'mode' as mode
        from integration_grants
        where integration_id = ${command.integrationId}
          and grant_kind = 'calendar_privacy' and status = 'active'
        order by scope->>'calendarIdDigest'
      `;
      const missingDigests = [
        ...new Set(
          configured
            .filter((grant) => grant.mode !== "off" && !activeDigests.has(grant.calendar_id_digest))
            .map((grant) => grant.calendar_id_digest),
        ),
      ].sort();
      const reconciledAt = new Date(command.reconciledAt);
      let retiredCalendarCount = 0;
      for (const calendarIdDigest of missingDigests) {
        const lockKey = `calendar-privacy:${command.integrationId}:${calendarIdDigest}`;
        await transaction`select pg_advisory_xact_lock(hashtextextended(${lockKey}, 0))`;
        const rows = await transaction<{ readonly version: number | string; readonly mode: string | null }[]>`
          select version, scope->>'mode' as mode
          from integration_grants
          where integration_id = ${command.integrationId}
            and grant_kind = 'calendar_privacy'
            and scope->>'calendarIdDigest' = ${calendarIdDigest}
            and status = 'active'
          order by version desc
          limit 1
          for update
        `;
        const current = rows[0];
        if (!current || current.mode === "off" || activeDigests.has(calendarIdDigest)) continue;
        await transaction`
          update integration_grants
          set status = 'revoked', revoked_at = ${reconciledAt}
          where integration_id = ${command.integrationId}
            and grant_kind = 'calendar_privacy'
            and scope->>'calendarIdDigest' = ${calendarIdDigest}
            and status = 'active'
        `;
        await transaction`
          insert into integration_grants (
            id, integration_id, grant_kind, scope, status, version, created_at
          ) values (
            ${randomUUID()}, ${command.integrationId}, 'calendar_privacy',
            ${transaction.json({ calendarIdDigest, mode: "off" })}, 'active',
            ${Number(current.version) + 1}, ${reconciledAt}
          )
        `;
        retiredCalendarCount += 1;
      }

      if (retiredCalendarCount === 0) {
        return {
          kind: "calendar_catalog_reconciled",
          integrationId: command.integrationId,
          integrationControlEpoch: command.expectedIntegrationControlEpoch,
          retiredCalendarCount: 0,
          resetRequired: false,
        };
      }

      await invalidateIntegrationArtifacts(
        transaction,
        command.integrationId,
        ["calendar_event"],
        reconciledAt,
      );
      await transaction`delete from sync_cursors where integration_id = ${command.integrationId}`;
      const updated = await transaction<{ readonly control_epoch: number | string }[]>`
        update integrations
        set control_epoch = control_epoch + 1, updated_at = ${reconciledAt}
        where id = ${command.integrationId}
        returning control_epoch
      `;
      return {
        kind: "calendar_catalog_reconciled",
        integrationId: command.integrationId,
        integrationControlEpoch: Number(
          requireRow(updated[0], "Calendar catalog reconciliation failed").control_epoch,
        ),
        retiredCalendarCount,
        resetRequired: true,
      };
    });
  }

  private async ingestSource(
    command: Extract<SourceCommand, { kind: "ingest_source" }>,
  ): Promise<SourceMutationResult> {
    return inTransaction(this.database, async (transaction) => {
      const capturedAt = new Date(command.capturedAt);
      const occurredAt = new Date(command.occurredAt);
      const scope = await resolveScopeForIngestion(
        transaction,
        command.scope,
        command.integrationId,
        command.expectedIntegrationControlEpoch,
        command.conversationAccessMode,
        occurredAt,
      );
      if (command.artifactKind === "calendar_event") {
        if (command.integrationId === null || command.resourceDigest === undefined) {
          throw new UnauthorizedError("Calendar source is missing its configured resource identity");
        }
        const mode = await loadCalendarPrivacy(transaction, command.integrationId, command.resourceDigest);
        if (mode === "off") throw new UnauthorizedError("Calendar source processing is disabled");
        if (mode === "availability_only") assertAvailabilityOnlyCalendarContent(command.content);
      }
      const requestedRetentionUntil = new Date(command.requestedRetentionUntil);
      const globalCap = new Date(capturedAt.getTime() + this.#rawRetentionMs);
      const retentionUntil =
        scope.retentionSeconds === null
          ? earliestDate(requestedRetentionUntil, globalCap)
          : earliestDate(
              requestedRetentionUntil,
              globalCap,
              new Date(capturedAt.getTime() + scope.retentionSeconds * 1_000),
            );
      const envelope = {
        artifactKind: command.artifactKind,
        origin: command.origin,
        data: command.content,
      };
      const serialized = canonicalJson(envelope);
      assertByteLimit(serialized, this.#maxSourceContentBytes, "Source content");
      const contentDigest = sha256(serialized);
      const objectIdentity = sourceObjectIdentity(
        command.integrationId,
        command.scope,
        command.artifactKind,
        command.origin,
      );
      const externalObjectId = `${command.artifactKind}:${objectIdentity}`;
      await transaction`select pg_advisory_xact_lock(hashtextextended(${`source:${command.origin.system}:${externalObjectId}`}, 0))`;

      const objects = await transaction<
        { readonly id: string; readonly latest_revision_number: number | string; readonly status: string }[]
      >`
        select id, latest_revision_number, status
        from source_objects
        where provider = ${command.origin.system} and external_object_id = ${externalObjectId}
        for update
      `;
      const sourceObjectId = objects[0]?.id ?? randomUUID();
      const latestRevisionNumber = Number(objects[0]?.latest_revision_number ?? 0);
      if (objects[0]) {
        const latestRows = await transaction<SourceRevisionRow[]>`
          select id, source_object_id, revision_number, owner_person_id, participant_epoch_id,
            scope_digest, content_digest, content_ciphertext, content_key_version,
            occurred_at, captured_at, retention_until, revoked_at, conversation_access_mode
          from source_revisions
          where source_object_id = ${sourceObjectId} and revision_number = ${latestRevisionNumber}
        `;
        const latest = latestRows[0];
        if (latest && latest.conversation_access_mode !== scope.conversationAccessMode) {
          throw new ConflictError("A source object's conversation access mode cannot change");
        }
        if (
          latest &&
          latest.scope_digest === scope.scopeDigest &&
          latest.content_digest === contentDigest &&
          latest.revoked_at === null &&
          ((latest.content_ciphertext !== null && latest.retention_until > capturedAt) ||
            latest.retention_until <= latest.captured_at) &&
          objects[0].status === "active"
        ) {
          const privateViewCount =
            scope.conversationAccessMode === "independent_private_views"
              ? await upsertEligiblePrivateViews(
                  transaction,
                  latest.id,
                  requireRow(scope.participantEpochId ?? undefined, "Private source epoch is missing"),
                )
              : 0;
          return {
            kind: "source_ingested",
            sourceObjectId,
            sourceRevisionId: latest.id,
            revisionNumber: Number(latest.revision_number),
            contentDigest,
            scopeDigest: scope.scopeDigest,
            retentionUntil: latest.retention_until.toISOString(),
            rawContentStored: latest.content_ciphertext !== null,
            privateViewCount,
            duplicate: true,
          };
        }
        if (
          latest &&
          (latest.scope_digest !== scope.scopeDigest || latest.content_digest !== contentDigest)
        ) {
          await invalidateRevisionDependents(transaction, [latest.id]);
        }
      } else {
        await transaction`
          insert into source_objects (
            id, integration_id, provider, external_object_id, object_kind,
            status, latest_revision_number, created_at, updated_at
          ) values (
            ${sourceObjectId}, ${command.integrationId}, ${command.origin.system},
            ${externalObjectId}, ${command.artifactKind}, 'active', 0,
            ${capturedAt}, ${capturedAt}
          )
        `;
      }

      const sourceRevisionId = randomUUID();
      const rawContentStored = retentionUntil > capturedAt;
      const content = rawContentStored
        ? sealBytes(this.secretBox, Buffer.from(serialized, "utf8"), sourceRevisionPurpose(sourceRevisionId))
        : null;
      const revisionNumber = latestRevisionNumber + 1;
      await transaction`
        insert into source_revisions (
          id, source_object_id, revision_number, owner_person_id, participant_epoch_id,
          scope_digest, content_digest, content_ciphertext, content_key_version,
          occurred_at, captured_at, retention_until, conversation_access_mode
        ) values (
          ${sourceRevisionId}, ${sourceObjectId}, ${revisionNumber},
          ${scope.ownerPersonId}, ${scope.participantEpochId}, ${scope.scopeDigest},
          ${contentDigest}, ${content?.ciphertext ?? null}, ${content?.keyVersion ?? null},
          ${occurredAt}, ${capturedAt}, ${retentionUntil}, ${scope.conversationAccessMode}
        )
      `;
      await transaction`
        update source_objects
        set status = 'active', latest_revision_number = ${revisionNumber}, updated_at = ${capturedAt}
        where id = ${sourceObjectId}
      `;
      const privateViewCount =
        scope.conversationAccessMode === "independent_private_views" && rawContentStored
          ? await upsertEligiblePrivateViews(
              transaction,
              sourceRevisionId,
              requireRow(scope.participantEpochId ?? undefined, "Private source epoch is missing"),
            )
          : 0;
      return {
        kind: "source_ingested",
        sourceObjectId,
        sourceRevisionId,
        revisionNumber,
        contentDigest,
        scopeDigest: scope.scopeDigest,
        retentionUntil: retentionUntil.toISOString(),
        rawContentStored,
        privateViewCount,
        duplicate: false,
      };
    });
  }

  private async storeBlob(
    command: Extract<SourceCommand, { kind: "store_blob" }>,
  ): Promise<SourceMutationResult> {
    const bytes = Buffer.from(command.bytes);
    if (bytes.length === 0) throw new ConflictError("A source blob cannot be empty");
    if (bytes.length > this.#maxBlobBytes)
      throw new ConflictError("Source blob exceeds the configured limit");
    return inTransaction(this.database, async (transaction) => {
      const storedAt = new Date(command.storedAt);
      const parent = await loadReadableRevision(
        transaction,
        command.sourceRevisionId,
        command.scope,
        storedAt,
        true,
      );
      await assertSourceObjectIntegrationEpoch(
        transaction,
        parent.source_object_id,
        command.integrationId,
        command.expectedIntegrationControlEpoch,
      );
      const contentDigest = sha256(bytes);
      const lockKey = `blob:${command.sourceRevisionId}:${command.blobKind}:${contentDigest}`;
      await transaction`select pg_advisory_xact_lock(hashtextextended(${lockKey}, 0))`;
      const existing = await transaction<{ readonly id: string; readonly retention_until: Date }[]>`
        select id, retention_until from source_blobs
        where source_revision_id = ${command.sourceRevisionId}
          and blob_kind = ${command.blobKind}
          and content_digest = ${contentDigest}
      `;
      if (existing[0]) {
        return {
          kind: "blob_stored",
          sourceBlobId: existing[0].id,
          contentDigest,
          retentionUntil: existing[0].retention_until.toISOString(),
          duplicate: true,
        };
      }
      const sourceBlobId = randomUUID();
      const sealed = sealBytes(this.secretBox, bytes, sourceBlobPurpose(sourceBlobId));
      await transaction`
        insert into source_blobs (
          id, source_revision_id, blob_kind, content_digest, mime_type,
          byte_length, ciphertext, key_version, retention_until, created_at
        ) values (
          ${sourceBlobId}, ${command.sourceRevisionId}, ${command.blobKind}, ${contentDigest},
          ${command.mimeType}, ${bytes.length}, ${sealed.ciphertext}, ${sealed.keyVersion},
          ${parent.retention_until}, ${storedAt}
        )
      `;
      return {
        kind: "blob_stored",
        sourceBlobId,
        contentDigest,
        retentionUntil: parent.retention_until.toISOString(),
        duplicate: false,
      };
    });
  }

  private async storeDerivative(
    command: Extract<SourceCommand, { kind: "store_derivative" }>,
  ): Promise<SourceMutationResult> {
    const serialized = canonicalJson(command.content);
    assertByteLimit(serialized, this.#maxDerivativeBytes, "Source derivative");
    return inTransaction(this.database, async (transaction) => {
      const createdAt = new Date(command.createdAt);
      const parent = await loadReadableRevision(
        transaction,
        command.sourceRevisionId,
        command.scope,
        createdAt,
        true,
      );
      await assertSourceObjectIntegrationEpoch(
        transaction,
        parent.source_object_id,
        command.integrationId,
        command.expectedIntegrationControlEpoch,
      );
      const retentionUntil = earliestDate(new Date(command.requestedRetentionUntil), parent.retention_until);
      if (retentionUntil <= createdAt) throw new ConflictError("Source derivative would already be expired");
      const contentDigest = sha256(serialized);
      const lockKey = `derivative:${command.sourceRevisionId}:${command.derivativeKind}:${contentDigest}`;
      await transaction`select pg_advisory_xact_lock(hashtextextended(${lockKey}, 0))`;
      const existing = await transaction<{ readonly id: string; readonly retention_until: Date }[]>`
        select id, retention_until from source_derivatives
        where parent_source_revision_id = ${command.sourceRevisionId}
          and kind = ${command.derivativeKind}
          and content_digest = ${contentDigest}
      `;
      if (existing[0]) {
        return {
          kind: "derivative_stored",
          sourceDerivativeId: existing[0].id,
          contentDigest,
          retentionUntil: existing[0].retention_until.toISOString(),
          duplicate: true,
        };
      }
      const sourceDerivativeId = randomUUID();
      const sealed = sealBytes(
        this.secretBox,
        Buffer.from(serialized, "utf8"),
        sourceDerivativePurpose(sourceDerivativeId),
      );
      await transaction`
        insert into source_derivatives (
          id, parent_source_revision_id, owner_person_id, participant_epoch_id,
          kind, scope_digest, content_digest, content_ciphertext, content_key_version,
          retention_until, created_at
        ) values (
          ${sourceDerivativeId}, ${command.sourceRevisionId}, ${parent.owner_person_id},
          ${parent.participant_epoch_id}, ${command.derivativeKind}, ${parent.scope_digest},
          ${contentDigest}, ${sealed.ciphertext}, ${sealed.keyVersion}, ${retentionUntil}, ${createdAt}
        )
      `;
      await transaction`
        insert into provenance_edges (
          id, parent_source_revision_id, child_derivative_id, relation, created_at
        ) values (
          ${randomUUID()}, ${command.sourceRevisionId}, ${sourceDerivativeId},
          'derived_from', ${createdAt}
        )
      `;
      return {
        kind: "derivative_stored",
        sourceDerivativeId,
        contentDigest,
        retentionUntil: retentionUntil.toISOString(),
        duplicate: false,
      };
    });
  }

  private async proposePrivateCandidate(
    command: Extract<SourceCommand, { kind: "propose_private_candidate" }>,
  ): Promise<SourceMutationResult> {
    const proposedAt = new Date(command.proposedAt);
    const requestedExpiresAt = new Date(command.requestedExpiresAt);
    if (requestedExpiresAt <= proposedAt)
      throw new ConflictError("Private candidate must expire after proposal");
    const expiresAt = earliestDate(
      requestedExpiresAt,
      new Date(proposedAt.getTime() + this.#privateCandidateRetentionMs),
    );
    const evidenceIds = [...new Set(command.evidenceSourceRevisionIds)].sort();
    const serialized = canonicalJson(command.content);
    assertByteLimit(serialized, this.#maxDerivativeBytes, "Private candidate");
    const contentDigest = sha256(serialized);
    return inTransaction(this.database, async (transaction) => {
      await requireRegisteredPerson(transaction, command.personId, false);
      if (command.integrationId !== null) {
        const integration = await loadIntegrationForUpdate(
          transaction,
          command.integrationId,
          command.personId,
        );
        if (command.expectedIntegrationControlEpoch === null) {
          throw new UnauthorizedError("Private source proposal is missing its integration authority");
        }
        assertControlEpoch(integration, command.expectedIntegrationControlEpoch);
        if (integration.status !== "active") {
          throw new UnauthorizedError("Private source integration is not active");
        }
      }
      const evidenceRows = await transaction<
        {
          readonly id: string;
          readonly owner_person_id: string | null;
          readonly revoked_at: Date | null;
          readonly integration_id: string | null;
        }[]
      >`
        select revision.id, revision.owner_person_id, revision.revoked_at, object.integration_id
        from source_revisions revision
        join source_objects object on object.id = revision.source_object_id
        where revision.id = any(${transaction.array(evidenceIds)}::uuid[])
        for share of revision, object
      `;
      if (evidenceRows.length !== evidenceIds.length)
        throw new NotFoundError("Candidate evidence is incomplete");
      if (evidenceRows.some((row) => row.owner_person_id !== command.personId || row.revoked_at !== null)) {
        throw new UnauthorizedError("Private candidate evidence is not active and person-owned");
      }
      if (
        evidenceRows.some((row) =>
          command.integrationId === null
            ? row.integration_id !== null
            : row.integration_id !== command.integrationId,
        )
      ) {
        throw new UnauthorizedError("Private candidate evidence is outside its integration authority");
      }
      const lockKey = `candidate:${command.personId}:${command.candidateKind}:${contentDigest}`;
      await transaction`select pg_advisory_xact_lock(hashtextextended(${lockKey}, 0))`;
      const existing = await transaction<{ readonly id: string; readonly expires_at: Date }[]>`
        select id, expires_at
        from knowledge_candidates
        where scope_kind = 'person'
          and owner_person_id = ${command.personId}
          and candidate_kind = ${command.candidateKind}
          and content_digest = ${contentDigest}
          and evidence_refs = ${transaction.json(evidenceIds)}::jsonb
          and status = 'pending'
          and (expires_at is null or expires_at > ${proposedAt})
        order by proposed_at desc
        limit 1
      `;
      if (existing[0]) {
        return {
          kind: "private_candidate_proposed",
          candidateId: existing[0].id,
          contentDigest,
          expiresAt: existing[0].expires_at.toISOString(),
          duplicate: true,
        };
      }
      const candidateId = randomUUID();
      const sealed = sealBytes(
        this.secretBox,
        Buffer.from(serialized, "utf8"),
        privateCandidatePurpose(candidateId),
      );
      await transaction`
        insert into knowledge_candidates (
          id, scope_kind, owner_person_id, candidate_kind, content_digest,
          content_ciphertext, content_key_version, evidence_refs, confidence,
          status, proposed_at, expires_at
        ) values (
          ${candidateId}, 'person', ${command.personId}, ${command.candidateKind}, ${contentDigest},
          ${sealed.ciphertext}, ${sealed.keyVersion}, ${transaction.json(evidenceIds)},
          ${command.confidence}, 'pending', ${proposedAt}, ${expiresAt}
        )
      `;
      return {
        kind: "private_candidate_proposed",
        candidateId,
        contentDigest,
        expiresAt: expiresAt.toISOString(),
        duplicate: false,
      };
    });
  }

  private async reviewPrivateCandidate(
    command: Extract<SourceCommand, { kind: "review_private_candidate" }>,
  ): Promise<SourceMutationResult> {
    return inTransaction(this.database, async (transaction) => {
      await requireRegisteredPerson(transaction, command.personId, false);
      const rows = await transaction<
        {
          readonly status: string;
          readonly candidate_kind: string;
          readonly content_digest: string;
          readonly content_ciphertext: Buffer;
          readonly evidence_refs: unknown;
          readonly reviewed_by_person_id: string | null;
          readonly reviewed_at: Date | null;
          readonly expires_at: Date | null;
        }[]
      >`
        select status, candidate_kind, content_digest, content_ciphertext, evidence_refs,
          reviewed_by_person_id, reviewed_at, expires_at
        from knowledge_candidates
        where id = ${command.candidateId}
          and scope_kind = 'person'
          and owner_person_id = ${command.personId}
        for update
      `;
      const candidate = rows[0];
      if (!candidate) throw new NotFoundError("Private review does not exist");
      const reviewedAt = new Date(command.reviewedAt);
      if (candidate.status === command.decision && candidate.reviewed_by_person_id === command.personId) {
        if (command.decision === "accepted") {
          await promotePrivateCandidateMemory(
            transaction,
            this.secretBox,
            command.candidateId,
            command.personId,
            candidate,
            candidate.reviewed_at ?? reviewedAt,
          );
        }
        return {
          kind: "private_candidate_reviewed",
          candidateId: command.candidateId,
          decision: command.decision,
          reviewedAt: (candidate.reviewed_at ?? reviewedAt).toISOString(),
          duplicate: true,
        };
      }
      if (candidate.status !== "pending") {
        throw new ConflictError("Private review is no longer awaiting a decision");
      }
      if (candidate.expires_at !== null && candidate.expires_at <= reviewedAt) {
        await transaction`
          update knowledge_candidates set status = 'expired'
          where id = ${command.candidateId}
        `;
        throw new ConflictError("Private review has expired");
      }
      await transaction`
        update knowledge_candidates
        set status = ${command.decision}, reviewed_by_person_id = ${command.personId},
            reviewed_at = ${reviewedAt}
        where id = ${command.candidateId}
      `;
      if (command.decision === "accepted") {
        await promotePrivateCandidateMemory(
          transaction,
          this.secretBox,
          command.candidateId,
          command.personId,
          candidate,
          reviewedAt,
        );
      }
      return {
        kind: "private_candidate_reviewed",
        candidateId: command.candidateId,
        decision: command.decision,
        reviewedAt: reviewedAt.toISOString(),
        duplicate: false,
      };
    });
  }

  private async markSourceDeleted(
    command: Extract<SourceCommand, { kind: "mark_source_deleted" }>,
  ): Promise<SourceMutationResult> {
    return inTransaction(this.database, async (transaction) => {
      await resolveScopeForInvalidation(
        transaction,
        command.scope,
        command.integrationId,
        command.expectedIntegrationControlEpoch,
      );
      const objectIdentity = sourceObjectIdentity(
        command.integrationId,
        command.scope,
        command.artifactKind,
        command.origin,
      );
      const externalObjectId = `${command.artifactKind}:${objectIdentity}`;
      await transaction`select pg_advisory_xact_lock(hashtextextended(${`source:${command.origin.system}:${externalObjectId}`}, 0))`;
      const objects = await transaction<{ readonly id: string }[]>`
        select id from source_objects
        where provider = ${command.origin.system} and external_object_id = ${externalObjectId}
        for update
      `;
      const object = objects[0];
      if (!object) {
        return {
          kind: "source_deleted",
          sourceObjectId: null,
          invalidatedRevisionCount: 0,
          revokedCandidateCount: 0,
        };
      }
      const revisions = await transaction<{ readonly id: string }[]>`
        select id from source_revisions
        where source_object_id = ${object.id} and revoked_at is null
        for update
      `;
      const invalidation = await invalidateRevisions(
        transaction,
        revisions.map((revision) => revision.id),
        new Date(command.deletedAt),
      );
      await transaction`
        update source_objects set status = 'deleted', updated_at = ${new Date(command.deletedAt)}
        where id = ${object.id}
      `;
      return {
        kind: "source_deleted",
        sourceObjectId: object.id,
        ...invalidation,
      };
    });
  }

  private async invalidateConversationEpoch(
    command: Extract<SourceCommand, { kind: "invalidate_conversation_epoch" }>,
  ): Promise<SourceMutationResult> {
    return inTransaction(this.database, async (transaction) => {
      const epochs = await transaction<{ readonly id: string }[]>`
        select id from participant_epochs where id = ${command.participantEpochId} for update
      `;
      if (!epochs[0]) throw new NotFoundError("Conversation epoch does not exist");
      const revisions = await transaction<{ readonly id: string }[]>`
        select id from source_revisions
        where participant_epoch_id = ${command.participantEpochId} and revoked_at is null
        for update
      `;
      const revisionIds = revisions.map((revision) => revision.id);
      const invalidation = await invalidateRevisions(
        transaction,
        revisionIds,
        new Date(command.invalidatedAt),
      );
      if (revisionIds.length > 0) {
        await transaction`
          update source_objects object
          set status = 'revoked', updated_at = ${new Date(command.invalidatedAt)}
          where exists (
            select 1 from source_revisions revision
            where revision.source_object_id = object.id
              and revision.id = any(${transaction.array(revisionIds)}::uuid[])
          )
        `;
      }
      return {
        kind: "conversation_epoch_invalidated",
        participantEpochId: command.participantEpochId,
        ...invalidation,
      };
    });
  }

  private async grantConversationPrivateViews(
    command: Extract<SourceCommand, { kind: "grant_conversation_private_views" }>,
  ): Promise<SourceMutationResult> {
    return inTransaction(this.database, async (transaction) => {
      const grantedAt = new Date(command.grantedAt);
      await requireEligiblePrivateViewer(
        transaction,
        command.participantEpochId,
        command.personId,
        grantedAt,
      );
      const privateViewCount = await upsertPrivateViewsForPerson(
        transaction,
        command.participantEpochId,
        command.personId,
        grantedAt,
      );
      return {
        kind: "conversation_private_views_granted",
        participantEpochId: command.participantEpochId,
        personId: command.personId,
        privateViewCount,
      };
    });
  }

  private async sweepRetention(
    command: Extract<SourceCommand, { kind: "sweep_retention" }>,
  ): Promise<SourceMutationResult> {
    return inTransaction(this.database, async (transaction) => {
      const asOf = new Date(command.asOf);
      await transaction`
        delete from source_revision_private_views
        where (source_revision_id, person_id) in (
          select source_revision_id, person_id
          from source_revision_private_views
          where retention_until <= ${asOf}
          order by retention_until
          limit ${command.limit}
          for update skip locked
        )
      `;
      const derivativeRows = await transaction<{ readonly id: string }[]>`
        select id from source_derivatives
        where retention_until <= ${asOf}
        order by retention_until
        limit ${command.limit}
        for update skip locked
      `;
      if (derivativeRows.length > 0) {
        await transaction`
          delete from source_derivatives
          where id = any(${transaction.array(derivativeRows.map((row) => row.id))}::uuid[])
        `;
      }
      const blobRows = await transaction<{ readonly id: string }[]>`
        select id from source_blobs
        where retention_until <= ${asOf}
        order by retention_until
        limit ${command.limit}
        for update skip locked
      `;
      if (blobRows.length > 0) {
        await transaction`
          delete from source_blobs
          where id = any(${transaction.array(blobRows.map((row) => row.id))}::uuid[])
        `;
      }
      const revisionRows = await transaction<{ readonly id: string }[]>`
        select id from source_revisions
        where retention_until <= ${asOf} and content_ciphertext is not null
        order by retention_until
        limit ${command.limit}
        for update skip locked
      `;
      if (revisionRows.length > 0) {
        const revisionIds = revisionRows.map((row) => row.id);
        await transaction`
          delete from source_revision_private_views
          where source_revision_id = any(${transaction.array(revisionIds)}::uuid[])
        `;
        await transaction`
          delete from source_derivatives
          where parent_source_revision_id = any(${transaction.array(revisionIds)}::uuid[])
        `;
        await transaction`
          delete from source_blobs
          where source_revision_id = any(${transaction.array(revisionIds)}::uuid[])
        `;
        await transaction`
          update source_revisions
          set content_ciphertext = null, content_key_version = null
          where id = any(${transaction.array(revisionIds)}::uuid[])
        `;
      }
      const expiredCandidates = await transaction<{ readonly id: string }[]>`
        update knowledge_candidates
        set status = 'expired'
        where id in (
          select id from knowledge_candidates
          where status = 'pending' and expires_at is not null and expires_at <= ${asOf}
          order by expires_at
          limit ${command.limit}
          for update skip locked
        )
        returning id
      `;
      const expiredOAuth = await transaction<{ readonly id: string }[]>`
        delete from oauth_attempts
        where id in (
          select id from oauth_attempts
          where expires_at <= ${asOf} or consumed_at is not null
          order by created_at
          limit ${command.limit}
          for update skip locked
        )
        returning id
      `;
      return {
        kind: "retention_swept",
        expiredRevisionCount: revisionRows.length,
        expiredBlobCount: blobRows.length,
        expiredDerivativeCount: derivativeRows.length,
        expiredCandidateCount: expiredCandidates.length,
        expiredOAuthAttemptCount: expiredOAuth.length,
      };
    });
  }

  private async readIntegrationAccess(
    query: Extract<SourceQuery, { kind: "integration_access" }>,
  ): Promise<SourceReadResult> {
    const rows = await this.database<IntegrationRow[]>`
      select id, person_id, provider, account_kind, status, credential_ciphertext,
        credential_key_version, control_epoch, connected_at, updated_at
      from integrations
      where id = ${query.integrationId} and person_id = ${query.personId}
    `;
    const integration = rows[0];
    if (!integration) throw new NotFoundError("Integration does not exist");
    assertControlEpoch(integration, query.expectedControlEpoch);
    if (integration.status !== "active" || integration.credential_ciphertext === null) {
      throw new UnauthorizedError("Integration credentials are not available");
    }
    const activeCapabilities = await loadActiveIntegrationCapabilities(
      this.database,
      integration.id,
      query.expectedControlEpoch,
    );
    if (!activeCapabilities.includes(query.requiredCapability)) {
      throw new UnauthorizedError(`Integration ${query.requiredCapability} capability is not active`);
    }
    const credentials = JsonObjectSchema.parse(
      openJson(this.secretBox, integration.credential_ciphertext, integrationPurpose(integration.id)),
    );
    return {
      kind: "integration_access",
      integration: integrationView(integration, activeCapabilities),
      credentials,
    };
  }

  private async readIntegrationProfile(
    query: Extract<SourceQuery, { kind: "integration_profile" }>,
  ): Promise<SourceReadResult> {
    const rows = await this.database<IntegrationRow[]>`
      select id, person_id, provider, account_kind, status, credential_ciphertext,
        credential_key_version, control_epoch, connected_at, updated_at
      from integrations
      where id = ${query.integrationId} and person_id = ${query.personId}
    `;
    const integration = rows[0];
    if (!integration) throw new NotFoundError("Integration does not exist");
    assertControlEpoch(integration, query.expectedControlEpoch);
    if (integration.status === "revoked" || integration.credential_ciphertext === null) {
      throw new UnauthorizedError("Integration profile is no longer available");
    }
    const credentials = JsonObjectSchema.parse(
      openJson(this.secretBox, integration.credential_ciphertext, integrationPurpose(integration.id)),
    );
    const accountEmail = credentials.accountEmail;
    if (typeof accountEmail !== "string" || accountEmail.length > 320 || !accountEmail.includes("@")) {
      throw new ConflictError("Connected Google account email is unavailable");
    }
    const activeCapabilities = await loadActiveIntegrationCapabilities(
      this.database,
      integration.id,
      query.expectedControlEpoch,
    );
    return {
      kind: "integration_profile",
      integration: integrationView(integration, activeCapabilities),
      accountEmail,
    };
  }

  private async readOAuthAttemptAccess(
    query: Extract<SourceQuery, { kind: "oauth_attempt_access" }>,
  ): Promise<SourceReadResult> {
    const rows = await this.database<
      {
        readonly id: string;
        readonly person_id: string;
        readonly provider: string;
        readonly initiating_session_id: string | null;
        readonly pkce_verifier_ciphertext: Buffer;
        readonly return_path: string;
        readonly requested_capabilities: string[];
        readonly account_kind: string;
        readonly person_control_epoch: number | string;
        readonly current_control_epoch: number | string;
        readonly person_status: string;
        readonly expires_at: Date;
        readonly consumed_at: Date | null;
      }[]
    >`
      select attempt.id, attempt.person_id, attempt.provider,
        attempt.initiating_session_id, attempt.pkce_verifier_ciphertext, attempt.return_path,
        attempt.requested_capabilities, attempt.account_kind,
        attempt.person_control_epoch, attempt.expires_at, attempt.consumed_at,
        person.control_epoch as current_control_epoch, person.status as person_status
      from oauth_attempts attempt
      join people person on person.id = attempt.person_id
      where attempt.state_digest = ${query.stateDigest}
    `;
    const attempt = rows[0];
    if (!attempt || attempt.provider !== query.provider) {
      throw new NotFoundError("OAuth attempt does not exist");
    }
    if (attempt.initiating_session_id === null) {
      throw new UnauthorizedError("OAuth attempt is not bound to a Florence session");
    }
    const asOf = new Date(query.asOf);
    if (attempt.consumed_at !== null) throw new UnauthorizedError("OAuth attempt was already consumed");
    if (attempt.expires_at <= asOf) throw new UnauthorizedError("OAuth attempt has expired");
    if (attempt.person_status !== "registered") throw new UnauthorizedError("OAuth owner is not active");
    if (Number(attempt.person_control_epoch) !== Number(attempt.current_control_epoch)) {
      throw new StaleAuthorityError("Person authority changed during OAuth");
    }
    return {
      kind: "oauth_attempt_access",
      oauthAttemptId: attempt.id,
      personId: attempt.person_id,
      provider: attempt.provider,
      initiatingSessionId: attempt.initiating_session_id,
      pkceVerifier: openBytes(
        this.secretBox,
        attempt.pkce_verifier_ciphertext,
        oauthPurpose(attempt.id),
      ).toString("utf8"),
      returnPath: attempt.return_path,
      personControlEpoch: Number(attempt.person_control_epoch),
      requestedCapabilities: integrationCapabilities(attempt.requested_capabilities),
      accountKind: integrationAccountKind(attempt.account_kind),
      expiresAt: attempt.expires_at.toISOString(),
    };
  }

  private async readCursor(query: Extract<SourceQuery, { kind: "sync_cursor" }>): Promise<SourceReadResult> {
    const integrationRows = await this.database<IntegrationRow[]>`
      select id, person_id, provider, account_kind, status, credential_ciphertext,
        credential_key_version, control_epoch, connected_at, updated_at
      from integrations
      where id = ${query.integrationId} and person_id = ${query.personId}
    `;
    const integration = integrationRows[0];
    if (!integration) throw new NotFoundError("Integration does not exist");
    assertControlEpoch(integration, query.expectedIntegrationControlEpoch);
    if (integration.status === "revoked") throw new UnauthorizedError("Integration was revoked");
    const rows = await this.database<
      {
        readonly cursor_ciphertext: Buffer | null;
        readonly state: string;
        readonly checkpoint_at: Date | null;
        readonly updated_at: Date;
      }[]
    >`
      select cursor_ciphertext, state, checkpoint_at, updated_at
      from sync_cursors
      where integration_id = ${query.integrationId} and resource_kind = ${query.resourceKind}
    `;
    const cursor = rows[0];
    if (!cursor) throw new NotFoundError("Sync cursor does not exist");
    return {
      kind: "sync_cursor",
      integrationId: query.integrationId,
      resourceKind: query.resourceKind,
      state: cursor.state as SyncCursorState,
      cursor:
        cursor.cursor_ciphertext === null
          ? null
          : JsonValueSchema.parse(
              openJson(
                this.secretBox,
                cursor.cursor_ciphertext,
                cursorPurpose(query.integrationId, query.resourceKind),
              ),
            ),
      checkpointAt: cursor.checkpoint_at?.toISOString() ?? null,
      updatedAt: cursor.updated_at.toISOString(),
    };
  }

  private async readCalendarPrivacy(
    query: Extract<SourceQuery, { kind: "calendar_privacy" }>,
  ): Promise<SourceReadResult> {
    const integrationRows = await this.database<IntegrationRow[]>`
      select id, person_id, provider, account_kind, status, credential_ciphertext,
        credential_key_version, control_epoch, connected_at, updated_at
      from integrations
      where id = ${query.integrationId} and person_id = ${query.personId}
    `;
    const integration = integrationRows[0];
    if (!integration) throw new NotFoundError("Integration does not exist");
    assertControlEpoch(integration, query.expectedIntegrationControlEpoch);
    if (integration.status === "revoked") throw new UnauthorizedError("Integration was revoked");
    const rows = await this.database<{ readonly mode: string; readonly version: number | string }[]>`
      select scope->>'mode' as mode, version
      from integration_grants
      where integration_id = ${query.integrationId}
        and grant_kind = 'calendar_privacy'
        and scope->>'calendarIdDigest' = ${query.calendarIdDigest}
        and status = 'active'
      order by version desc
      limit 1
    `;
    const policy = rows[0];
    if (!policy) throw new NotFoundError("Calendar privacy has not been configured");
    return {
      kind: "calendar_privacy",
      integrationId: query.integrationId,
      calendarIdDigest: query.calendarIdDigest,
      mode: policy.mode as CalendarPrivacyMode,
      grantVersion: Number(policy.version),
    };
  }

  private async readSourceRevision(
    query: Extract<SourceQuery, { kind: "source_revision" }>,
  ): Promise<SourceReadResult> {
    return inTransaction(this.database, async (transaction) => {
      const asOf = new Date(query.asOf);
      const revision = await loadReadableRevision(
        transaction,
        query.sourceRevisionId,
        query.scope,
        asOf,
        false,
      );
      await authorizeRevisionRead(transaction, revision, query.scope, query.privateViewerPersonId, asOf);
      if (query.integrationId !== undefined) {
        await assertSourceObjectIntegrationEpoch(
          transaction,
          revision.source_object_id,
          query.integrationId,
          query.expectedIntegrationControlEpoch ?? null,
        );
      }
      if (revision.content_ciphertext === null)
        throw new NotFoundError("Source content is no longer retained");
      return {
        kind: "source_revision",
        sourceRevisionId: revision.id,
        sourceObjectId: revision.source_object_id,
        revisionNumber: Number(revision.revision_number),
        scopeDigest: revision.scope_digest,
        contentDigest: revision.content_digest,
        content: JsonObjectSchema.parse(
          openJson(this.secretBox, revision.content_ciphertext, sourceRevisionPurpose(revision.id)),
        ),
        occurredAt: revision.occurred_at.toISOString(),
        capturedAt: revision.captured_at.toISOString(),
        retentionUntil: revision.retention_until.toISOString(),
      };
    });
  }

  private async readSourceBlob(
    query: Extract<SourceQuery, { kind: "source_blob" }>,
  ): Promise<SourceReadResult> {
    return inTransaction(this.database, async (transaction) => {
      const asOf = new Date(query.asOf);
      const rows = await transaction<
        {
          readonly id: string;
          readonly source_revision_id: string;
          readonly blob_kind: string;
          readonly mime_type: string;
          readonly content_digest: string;
          readonly ciphertext: Buffer;
          readonly retention_until: Date;
          readonly owner_person_id: string | null;
          readonly participant_epoch_id: string | null;
          readonly revision_revoked_at: Date | null;
          readonly revision_captured_at: Date;
          readonly revision_retention_until: Date;
          readonly conversation_access_mode: string | null;
        }[]
      >`
        select blob.id, blob.source_revision_id, blob.blob_kind, blob.mime_type,
          blob.content_digest, blob.ciphertext, blob.retention_until,
          revision.owner_person_id, revision.participant_epoch_id,
          revision.revoked_at as revision_revoked_at,
          revision.captured_at as revision_captured_at,
          revision.retention_until as revision_retention_until,
          revision.conversation_access_mode
        from source_blobs blob
        join source_revisions revision on revision.id = blob.source_revision_id
        where blob.id = ${query.sourceBlobId}
      `;
      const blob = rows[0];
      if (!blob) throw new NotFoundError("Source blob does not exist");
      assertScopeMatchesRow(query.scope, blob);
      if (
        blob.revision_revoked_at !== null ||
        blob.revision_retention_until <= asOf ||
        blob.retention_until <= asOf
      ) {
        throw new NotFoundError("Source blob is no longer retained");
      }
      await authorizeRevisionRead(
        transaction,
        {
          id: blob.source_revision_id,
          owner_person_id: blob.owner_person_id,
          participant_epoch_id: blob.participant_epoch_id,
          conversation_access_mode: blob.conversation_access_mode,
          captured_at: blob.revision_captured_at,
          retention_until: blob.revision_retention_until,
          revoked_at: blob.revision_revoked_at,
        },
        query.scope,
        query.privateViewerPersonId,
        asOf,
      );
      return {
        kind: "source_blob",
        sourceBlobId: blob.id,
        sourceRevisionId: blob.source_revision_id,
        blobKind: blob.blob_kind,
        mimeType: blob.mime_type,
        contentDigest: blob.content_digest,
        bytes: openBytes(this.secretBox, blob.ciphertext, sourceBlobPurpose(blob.id)),
        retentionUntil: blob.retention_until.toISOString(),
      };
    });
  }

  private async readSourceDerivative(
    query: Extract<SourceQuery, { kind: "source_derivative" }>,
  ): Promise<SourceReadResult> {
    return inTransaction(this.database, async (transaction) => {
      const asOf = new Date(query.asOf);
      const rows = await transaction<
        {
          readonly id: string;
          readonly parent_source_revision_id: string;
          readonly owner_person_id: string | null;
          readonly participant_epoch_id: string | null;
          readonly kind: string;
          readonly scope_digest: string;
          readonly content_digest: string;
          readonly content_ciphertext: Buffer;
          readonly retention_until: Date;
          readonly parent_revoked_at: Date | null;
          readonly parent_captured_at: Date;
          readonly parent_retention_until: Date;
          readonly conversation_access_mode: string | null;
        }[]
      >`
        select derivative.id, derivative.parent_source_revision_id,
          derivative.owner_person_id, derivative.participant_epoch_id,
          derivative.kind, derivative.scope_digest, derivative.content_digest,
          derivative.content_ciphertext, derivative.retention_until,
          parent.revoked_at as parent_revoked_at,
          parent.captured_at as parent_captured_at,
          parent.retention_until as parent_retention_until,
          parent.conversation_access_mode
        from source_derivatives derivative
        join source_revisions parent on parent.id = derivative.parent_source_revision_id
        where derivative.id = ${query.sourceDerivativeId}
      `;
      const derivative = rows[0];
      if (!derivative) throw new NotFoundError("Source derivative does not exist");
      assertScopeMatchesRow(query.scope, derivative);
      if (
        derivative.parent_revoked_at !== null ||
        derivative.parent_retention_until <= asOf ||
        derivative.retention_until <= asOf
      ) {
        throw new NotFoundError("Source derivative is no longer retained");
      }
      await authorizeRevisionRead(
        transaction,
        {
          id: derivative.parent_source_revision_id,
          owner_person_id: derivative.owner_person_id,
          participant_epoch_id: derivative.participant_epoch_id,
          conversation_access_mode: derivative.conversation_access_mode,
          captured_at: derivative.parent_captured_at,
          retention_until: derivative.parent_retention_until,
          revoked_at: derivative.parent_revoked_at,
        },
        query.scope,
        query.privateViewerPersonId,
        asOf,
      );
      return {
        kind: "source_derivative",
        sourceDerivativeId: derivative.id,
        sourceRevisionId: derivative.parent_source_revision_id,
        derivativeKind: derivative.kind,
        scopeDigest: derivative.scope_digest,
        contentDigest: derivative.content_digest,
        content: JsonValueSchema.parse(
          openJson(this.secretBox, derivative.content_ciphertext, sourceDerivativePurpose(derivative.id)),
        ),
        retentionUntil: derivative.retention_until.toISOString(),
      };
    });
  }

  private async readPendingPrivateCandidates(
    query: Extract<SourceQuery, { kind: "pending_private_candidates" }>,
  ): Promise<SourceReadResult> {
    return inTransaction(this.database, async (transaction) => {
      await requireRegisteredPerson(transaction, query.personId, false);
      const rows = await transaction<
        {
          readonly id: string;
          readonly candidate_kind: string;
          readonly content_digest: string;
          readonly content_ciphertext: Buffer;
          readonly evidence_refs: unknown;
          readonly confidence: string | number;
          readonly proposed_at: Date;
          readonly expires_at: Date | null;
        }[]
      >`
        select id, candidate_kind, content_digest, content_ciphertext,
          evidence_refs, confidence, proposed_at, expires_at
        from knowledge_candidates
        where scope_kind = 'person' and owner_person_id = ${query.personId}
          and status = 'pending'
          and (expires_at is null or expires_at > ${new Date(query.asOf)})
        order by proposed_at desc
        limit ${query.limit}
      `;
      return {
        kind: "pending_private_candidates",
        personId: query.personId,
        candidates: rows.map((row) => ({
          candidateId: row.id,
          candidateKind: row.candidate_kind,
          contentDigest: row.content_digest,
          content: JsonObjectSchema.parse(
            openJson(this.secretBox, row.content_ciphertext, privateCandidatePurpose(row.id)),
          ),
          evidenceSourceRevisionIds: stringArray(row.evidence_refs),
          confidence: Number(row.confidence),
          proposedAt: row.proposed_at.toISOString(),
          expiresAt: row.expires_at?.toISOString() ?? null,
        })),
      };
    });
  }
}

function inTransaction<Result>(
  executor: Executor,
  operation: (transaction: Transaction) => Promise<Result>,
): Promise<Result> {
  return "begin" in executor
    ? (executor.begin(operation) as unknown as Promise<Result>)
    : operation(executor);
}

async function requireRegisteredPerson(
  transaction: Transaction,
  personId: string,
  forUpdate: boolean,
): Promise<{ readonly status: string; readonly control_epoch: number | string }> {
  const rows = forUpdate
    ? await transaction<{ readonly status: string; readonly control_epoch: number | string }[]>`
        select status, control_epoch from people where id = ${personId} for update
      `
    : await transaction<{ readonly status: string; readonly control_epoch: number | string }[]>`
        select status, control_epoch from people where id = ${personId}
      `;
  const person = rows[0];
  if (!person) throw new NotFoundError("Person does not exist");
  if (person.status !== "registered") throw new UnauthorizedError("Person is not registered");
  return person;
}

async function loadIntegrationForUpdate(
  transaction: Transaction,
  integrationId: string,
  personId: string,
): Promise<IntegrationRow> {
  const rows = await transaction<IntegrationRow[]>`
    select id, person_id, provider, account_kind, status, credential_ciphertext,
      credential_key_version, control_epoch, connected_at, updated_at
    from integrations
    where id = ${integrationId} and person_id = ${personId}
    for update
  `;
  if (!rows[0]) throw new NotFoundError("Integration does not exist");
  return rows[0];
}

async function replaceActiveIntegrationCapabilities(
  transaction: Transaction,
  integrationId: string,
  activeCapabilities: readonly IntegrationCapability[],
  grantedAt: Date,
): Promise<void> {
  await transaction`
    update integration_capabilities
    set status = 'revoked', revoked_at = ${grantedAt}, updated_at = ${grantedAt}
    where integration_id = ${integrationId}
      and status = 'active'
      and capability <> all(${transaction.array([...activeCapabilities])}::text[])
  `;
  for (const capability of activeCapabilities) {
    await transaction`
      insert into integration_capabilities (
        integration_id, capability, status, granted_at, revoked_at, updated_at
      ) values (
        ${integrationId}, ${capability}, 'active', ${grantedAt}, null, ${grantedAt}
      )
      on conflict (integration_id, capability) do update
      set status = 'active', granted_at = excluded.granted_at,
        revoked_at = null, updated_at = excluded.updated_at
    `;
  }
}

async function loadActiveIntegrationCapabilities(
  executor: Executor,
  integrationId: string,
  expectedControlEpoch: number,
): Promise<IntegrationCapability[]> {
  const rows = await executor<
    { readonly capability: string | null; readonly control_epoch: number | string }[]
  >`
    select capability.capability, integration.control_epoch
    from integrations integration
    left join integration_capabilities capability
      on capability.integration_id = integration.id and capability.status = 'active'
    where integration.id = ${integrationId}
    order by capability.capability
  `;
  const snapshot = rows[0];
  if (!snapshot) throw new NotFoundError("Integration does not exist");
  if (Number(snapshot.control_epoch) !== expectedControlEpoch) {
    throw new StaleAuthorityError("Integration authority changed while reading capabilities");
  }
  return rows.flatMap((row) =>
    row.capability === null ? [] : [IntegrationCapabilitySchema.parse(row.capability)],
  );
}

function assertControlEpoch(integration: IntegrationRow, expectedControlEpoch: number): void {
  if (Number(integration.control_epoch) !== expectedControlEpoch) {
    throw new StaleAuthorityError("Integration authority changed");
  }
}

async function resolveScopeForIngestion(
  transaction: Transaction,
  scope: SourceScope,
  integrationId: string | null,
  expectedIntegrationControlEpoch: number | null,
  conversationAccessMode: ConversationSourceAccessMode | undefined,
  occurredAt: Date,
): Promise<ScopeResolution> {
  if (scope.kind === "person") {
    if (conversationAccessMode !== undefined) {
      throw new UnauthorizedError("Person sources cannot use a conversation access mode");
    }
    if ((integrationId === null) !== (expectedIntegrationControlEpoch === null)) {
      throw new UnauthorizedError("Integration identity and authority fence must be supplied together");
    }
    await requireRegisteredPerson(transaction, scope.personId, false);
    if (integrationId === null) {
      return {
        ownerPersonId: scope.personId,
        participantEpochId: null,
        scopeDigest: canonicalDigest({ kind: "person", personId: scope.personId }),
        retentionSeconds: 30 * 24 * 60 * 60,
        conversationAccessMode: null,
      };
    }
    const rows = await transaction<
      { readonly person_id: string; readonly status: string; readonly control_epoch: number | string }[]
    >`
      select person_id, status, control_epoch from integrations where id = ${integrationId}
    `;
    const integration = rows[0];
    if (!integration || integration.person_id !== scope.personId || integration.status !== "active") {
      throw new UnauthorizedError("Integration is not active for the source owner");
    }
    if (Number(integration.control_epoch) !== expectedIntegrationControlEpoch) {
      throw new StaleAuthorityError("Integration authority changed before source ingestion");
    }
    return {
      ownerPersonId: scope.personId,
      participantEpochId: null,
      scopeDigest: canonicalDigest({
        kind: "person",
        personId: scope.personId,
        integrationId,
        integrationControlEpoch: Number(integration.control_epoch),
      }),
      retentionSeconds: 30 * 24 * 60 * 60,
      conversationAccessMode: null,
    };
  }
  if (integrationId !== null || expectedIntegrationControlEpoch !== null) {
    throw new UnauthorizedError("Conversation evidence cannot use a person integration");
  }
  if (conversationAccessMode === undefined) {
    throw new UnauthorizedError("Conversation sources require an explicit access mode");
  }
  return resolveActiveConversationScope(
    transaction,
    scope.participantEpochId,
    conversationAccessMode,
    occurredAt,
  );
}

async function loadCalendarPrivacy(
  transaction: Transaction,
  integrationId: string,
  calendarIdDigest: string,
): Promise<CalendarPrivacyMode> {
  const rows = await transaction<{ readonly mode: string }[]>`
    select scope->>'mode' as mode
    from integration_grants
    where integration_id = ${integrationId}
      and grant_kind = 'calendar_privacy'
      and scope->>'calendarIdDigest' = ${calendarIdDigest}
      and status = 'active'
    order by version desc
    limit 1
  `;
  const mode = rows[0]?.mode;
  if (mode !== "full_private" && mode !== "availability_only" && mode !== "off") {
    throw new UnauthorizedError("Calendar privacy has not been configured");
  }
  return mode;
}

function assertAvailabilityOnlyCalendarContent(content: Readonly<Record<string, unknown>>): void {
  const allowedKeys = new Set(["identityDigest", "start", "end", "status", "busy"]);
  if (Object.keys(content).some((key) => !allowedKeys.has(key))) {
    throw new UnauthorizedError("Availability-only calendar content contains private event detail");
  }
  if (
    typeof content.identityDigest !== "string" ||
    !/^[a-f0-9]{64}$/u.test(content.identityDigest) ||
    typeof content.start !== "string" ||
    typeof content.end !== "string" ||
    typeof content.status !== "string" ||
    typeof content.busy !== "boolean"
  ) {
    throw new ConflictError("Availability-only calendar content is incomplete");
  }
}

async function resolveScopeForInvalidation(
  transaction: Transaction,
  scope: SourceScope,
  integrationId: string | null,
  expectedIntegrationControlEpoch: number | null,
): Promise<void> {
  if (scope.kind === "person") {
    if ((integrationId === null) !== (expectedIntegrationControlEpoch === null)) {
      throw new UnauthorizedError("Integration identity and authority fence must be supplied together");
    }
    await requireRegisteredPerson(transaction, scope.personId, false);
    if (integrationId === null) {
      return;
    }
    const rows = await transaction<
      {
        readonly person_id: string;
        readonly status: string;
        readonly control_epoch: number | string;
      }[]
    >`
      select person_id, status, control_epoch from integrations where id = ${integrationId}
    `;
    const integration = rows[0];
    if (!integration || integration.person_id !== scope.personId || integration.status !== "active") {
      throw new UnauthorizedError("Integration is not active for the source owner");
    }
    if (Number(integration.control_epoch) !== expectedIntegrationControlEpoch) {
      throw new StaleAuthorityError("Integration authority changed before source invalidation");
    }
    return;
  }
  if (integrationId !== null || expectedIntegrationControlEpoch !== null) {
    throw new UnauthorizedError("Conversation evidence cannot use a person integration");
  }
  const rows = await transaction<{ readonly id: string }[]>`
    select id from participant_epochs where id = ${scope.participantEpochId}
  `;
  if (!rows[0]) throw new NotFoundError("Conversation epoch does not exist");
}

async function assertSourceObjectIntegrationEpoch(
  transaction: Transaction,
  sourceObjectId: string,
  expectedIntegrationId: string | null,
  expectedIntegrationControlEpoch: number | null,
): Promise<void> {
  const rows = await transaction<
    {
      readonly integration_id: string | null;
      readonly status: string | null;
      readonly control_epoch: number | string | null;
    }[]
  >`
    select object.integration_id, integration.status, integration.control_epoch
    from source_objects object
    left join integrations integration on integration.id = object.integration_id
    where object.id = ${sourceObjectId}
  `;
  const source = rows[0];
  if (!source) throw new NotFoundError("Source object does not exist");
  if ((expectedIntegrationId === null) !== (expectedIntegrationControlEpoch === null)) {
    throw new UnauthorizedError("Integration identity and authority fence must be supplied together");
  }
  if (expectedIntegrationId === null) {
    if (source.integration_id !== null) {
      throw new UnauthorizedError("Provider source work requires an integration fence");
    }
    return;
  }
  if (
    source.integration_id !== expectedIntegrationId ||
    source.status !== "active" ||
    Number(source.control_epoch) !== expectedIntegrationControlEpoch
  ) {
    throw new StaleAuthorityError("Integration authority changed before source enrichment");
  }
}

async function invalidateIntegrationArtifacts(
  transaction: Transaction,
  integrationId: string,
  artifactKinds: readonly SourceArtifactKind[],
  invalidatedAt: Date,
): Promise<void> {
  const revisions = await transaction<{ readonly id: string }[]>`
    select revision.id
    from source_objects object
    join source_revisions revision on revision.source_object_id = object.id
    where object.integration_id = ${integrationId}
      and object.object_kind = any(${transaction.array([...artifactKinds])}::text[])
      and revision.revoked_at is null
    for update of object, revision
  `;
  await invalidateRevisions(
    transaction,
    revisions.map((revision) => revision.id),
    invalidatedAt,
  );
  await transaction`
    update source_objects
    set status = 'deleted', updated_at = ${invalidatedAt}
    where integration_id = ${integrationId}
      and object_kind = any(${transaction.array([...artifactKinds])}::text[])
  `;
}

async function upsertEligiblePrivateViews(
  transaction: Transaction,
  sourceRevisionId: string,
  participantEpochId: string,
): Promise<number> {
  const rows = await transaction<{ readonly person_id: string }[]>`
    with eligible as (
      select distinct on (revision.id, person.id)
        revision.id as source_revision_id,
        revision.participant_epoch_id,
        participant.person_identity_id,
        person.id as person_id,
        person.control_epoch,
        least(
          revision.retention_until,
          revision.captured_at + policy.retention_seconds * interval '1 second'
        ) as retention_until,
        greatest(
          revision.captured_at,
          participant.consented_at,
          person.registered_at,
          identity.verified_at,
          policy.effective_at
        ) as granted_at
      from source_revisions revision
      join participant_epochs epoch on epoch.id = revision.participant_epoch_id
      join conversations conversation on conversation.id = epoch.conversation_id
      join epoch_participants participant
        on participant.participant_epoch_id = revision.participant_epoch_id
      join person_identities identity on identity.id = participant.person_identity_id
      join people person on person.id = identity.person_id
      join participant_policies policy
        on policy.conversation_id = conversation.id
        and policy.person_id = person.id
        and policy.status = 'active'
      where revision.id = ${sourceRevisionId}
        and revision.participant_epoch_id = ${participantEpochId}
        and revision.conversation_access_mode = 'independent_private_views'
        and revision.revoked_at is null
        and revision.content_ciphertext is not null
        and participant.registration_status = 'registered'
        and participant.consented_at is not null
        and identity.status = 'verified'
        and person.status = 'registered'
        and person.registered_at is not null
        and policy.allow_content_processing = true
        and policy.retention_seconds > 0
        and conversation.status = 'active'
        and least(
          revision.retention_until,
          revision.captured_at + policy.retention_seconds * interval '1 second'
        ) > greatest(
          revision.captured_at,
          participant.consented_at,
          person.registered_at,
          identity.verified_at,
          policy.effective_at
        )
      order by revision.id, person.id, participant.person_identity_id
    )
    insert into source_revision_private_views as existing_view (
      source_revision_id, participant_epoch_id, person_identity_id, person_id, person_control_epoch,
      status, retention_until, granted_at, revoked_at
    )
    select source_revision_id, participant_epoch_id, person_identity_id, person_id,
      control_epoch, 'active', retention_until, granted_at, null
    from eligible
    on conflict (source_revision_id, person_id) do update
    set person_identity_id = excluded.person_identity_id,
        person_control_epoch = excluded.person_control_epoch,
        status = 'active',
        retention_until = excluded.retention_until,
        granted_at = case
          when existing_view.status = 'active'
            and existing_view.person_control_epoch = excluded.person_control_epoch
            then least(existing_view.granted_at, excluded.granted_at)
          else excluded.granted_at
        end,
        revoked_at = null
    returning person_id
  `;
  return rows.length;
}

async function requireEligiblePrivateViewer(
  transaction: Transaction,
  participantEpochId: string,
  personId: string,
  grantedAt: Date,
): Promise<void> {
  const rows = await transaction<{ readonly eligible: number }[]>`
    select 1 as eligible
    from epoch_participants participant
    join participant_epochs epoch on epoch.id = participant.participant_epoch_id
    join conversations conversation on conversation.id = epoch.conversation_id
    join person_identities identity on identity.id = participant.person_identity_id
    join people person on person.id = identity.person_id
    join participant_policies policy
      on policy.conversation_id = conversation.id
      and policy.person_id = person.id
      and policy.status = 'active'
    where participant.participant_epoch_id = ${participantEpochId}
      and identity.person_id = ${personId}
      and participant.registration_status = 'registered'
      and participant.consented_at is not null
      and participant.consented_at <= ${grantedAt}
      and identity.status = 'verified'
      and identity.verified_at <= ${grantedAt}
      and person.status = 'registered'
      and person.registered_at is not null
      and person.registered_at <= ${grantedAt}
      and policy.allow_content_processing = true
      and policy.retention_seconds > 0
      and policy.effective_at <= ${grantedAt}
      and conversation.status = 'active'
    limit 1
  `;
  if (!rows[0]) {
    throw new UnauthorizedError("Person is not an eligible registered participant in this exact epoch");
  }
}

async function upsertPrivateViewsForPerson(
  transaction: Transaction,
  participantEpochId: string,
  personId: string,
  grantedAt: Date,
): Promise<number> {
  const rows = await transaction<{ readonly source_revision_id: string }[]>`
    with eligible as (
      select distinct on (revision.id, person.id)
        revision.id as source_revision_id,
        revision.participant_epoch_id,
        participant.person_identity_id,
        person.id as person_id,
        person.control_epoch,
        least(
          revision.retention_until,
          revision.captured_at + policy.retention_seconds * interval '1 second'
        ) as retention_until
      from source_revisions revision
      join participant_epochs epoch on epoch.id = revision.participant_epoch_id
      join conversations conversation on conversation.id = epoch.conversation_id
      join epoch_participants participant
        on participant.participant_epoch_id = revision.participant_epoch_id
      join person_identities identity on identity.id = participant.person_identity_id
        and identity.person_id = ${personId}
      join people person on person.id = identity.person_id
      join participant_policies policy
        on policy.conversation_id = conversation.id
        and policy.person_id = person.id
        and policy.status = 'active'
      where revision.participant_epoch_id = ${participantEpochId}
        and revision.conversation_access_mode = 'independent_private_views'
        and revision.revoked_at is null
        and revision.content_ciphertext is not null
        and revision.captured_at <= ${grantedAt}
        and revision.retention_until > ${grantedAt}
        and participant.registration_status = 'registered'
        and participant.consented_at is not null
        and identity.status = 'verified'
        and person.status = 'registered'
        and person.control_epoch > 0
        and policy.allow_content_processing = true
        and policy.retention_seconds > 0
        and conversation.status = 'active'
        and least(
          revision.retention_until,
          revision.captured_at + policy.retention_seconds * interval '1 second'
        ) > ${grantedAt}
      order by revision.id, person.id, participant.person_identity_id
    )
    insert into source_revision_private_views as existing_view (
      source_revision_id, participant_epoch_id, person_identity_id, person_id, person_control_epoch,
      status, retention_until, granted_at, revoked_at
    )
    select source_revision_id, participant_epoch_id, person_identity_id, person_id,
      control_epoch, 'active', retention_until, ${grantedAt}, null
    from eligible
    on conflict (source_revision_id, person_id) do update
    set person_identity_id = excluded.person_identity_id,
        person_control_epoch = excluded.person_control_epoch,
        status = 'active',
        retention_until = excluded.retention_until,
        granted_at = excluded.granted_at,
        revoked_at = null
    returning source_revision_id
  `;
  return rows.length;
}

async function resolveActiveConversationScope(
  transaction: Transaction,
  participantEpochId: string,
  conversationAccessMode: ConversationSourceAccessMode = "unanimously_shared",
  occurredAt?: Date,
): Promise<ScopeResolution> {
  // Serialize exact-epoch validation through revision insertion with membership
  // changes. Conversation authority locks in the same conversation-then-epoch order.
  const locked = await transaction<{ readonly epoch_id: string }[]>`
    select epoch.id as epoch_id
    from participant_epochs epoch
    join conversations conversation on conversation.id = epoch.conversation_id
    where epoch.id = ${participantEpochId}
    for update of conversation, epoch
  `;
  if (!locked[0]) throw new NotFoundError("Conversation epoch does not exist");
  const rows = await transaction<
    {
      readonly authority_digest: string;
      readonly started_at: Date;
      readonly ended_at: Date | null;
      readonly conversation_status: string;
      readonly current_epoch_id: string | null;
      readonly participant_count: number | string;
      readonly registered_count: number | string;
      readonly permitted_count: number | string;
      readonly retention_seconds: number | string | null;
    }[]
  >`
    select epoch.authority_digest, epoch.started_at, epoch.ended_at,
      conversation.status as conversation_status,
      conversation.current_epoch_id,
      (select count(*) from epoch_participants participant
        where participant.participant_epoch_id = epoch.id) as participant_count,
      (select count(*) from epoch_participants participant
        where participant.participant_epoch_id = epoch.id
          and participant.registration_status = 'registered'
          and participant.consented_at is not null) as registered_count,
      (select count(*)
        from epoch_participants participant
        join participant_policies policy
          on policy.conversation_id = conversation.id
          and policy.person_id = participant.person_id
          and policy.status = 'active'
        where participant.participant_epoch_id = epoch.id
          and policy.allow_content_processing = true) as permitted_count,
      (select min(policy.retention_seconds)
        from epoch_participants participant
        join participant_policies policy
          on policy.conversation_id = conversation.id
          and policy.person_id = participant.person_id
          and policy.status = 'active'
        where participant.participant_epoch_id = epoch.id
          and policy.allow_content_processing = true) as retention_seconds
    from participant_epochs epoch
    join conversations conversation on conversation.id = epoch.conversation_id
    where epoch.id = ${participantEpochId}
  `;
  const row = rows[0];
  if (!row) throw new NotFoundError("Conversation epoch does not exist");
  const participantCount = Number(row.participant_count);
  if (row.conversation_status !== "active") {
    throw new UnauthorizedError("Conversation source processing is not active");
  }
  if (
    occurredAt !== undefined &&
    (occurredAt < row.started_at || (row.ended_at !== null && occurredAt >= row.ended_at))
  ) {
    throw new StaleAuthorityError("Source event did not occur within the exact participant epoch");
  }
  if (
    conversationAccessMode === "unanimously_shared" &&
    (row.current_epoch_id !== participantEpochId || row.ended_at !== null)
  ) {
    throw new StaleAuthorityError("Conversation epoch is no longer current");
  }
  if (
    participantCount === 0 ||
    (conversationAccessMode === "unanimously_shared" &&
      (Number(row.registered_count) !== participantCount ||
        Number(row.permitted_count) !== participantCount ||
        row.retention_seconds === null))
  ) {
    throw new UnauthorizedError("Conversation content processing is not unanimously authorized");
  }
  return {
    ownerPersonId: null,
    participantEpochId,
    scopeDigest: canonicalDigest({
      kind: "conversation_epoch",
      participantEpochId,
      authorityDigest: row.authority_digest,
    }),
    retentionSeconds: conversationAccessMode === "unanimously_shared" ? Number(row.retention_seconds) : null,
    conversationAccessMode,
  };
}

interface RevisionAccessRow {
  readonly id: string;
  readonly owner_person_id: string | null;
  readonly participant_epoch_id: string | null;
  readonly conversation_access_mode: string | null;
  readonly captured_at: Date;
  readonly retention_until: Date;
  readonly revoked_at: Date | null;
}

async function authorizeRevisionRead(
  transaction: Transaction,
  revision: RevisionAccessRow,
  scope: SourceScope,
  privateViewerPersonId: string | undefined,
  asOf: Date,
): Promise<void> {
  assertScopeMatchesRow(scope, revision);
  if (scope.kind === "person") {
    if (privateViewerPersonId !== undefined || revision.conversation_access_mode !== null) {
      throw new UnauthorizedError("Person source access cannot use a conversation private view");
    }
    await requireRegisteredPerson(transaction, scope.personId, false);
    return;
  }

  if (privateViewerPersonId === undefined) {
    if (revision.conversation_access_mode !== "unanimously_shared") {
      throw new UnauthorizedError("This conversation source requires an exact private viewer");
    }
    await resolveActiveConversationScope(transaction, scope.participantEpochId);
    return;
  }
  if (revision.conversation_access_mode !== "independent_private_views") {
    throw new UnauthorizedError("A private viewer cannot widen a unanimously shared source");
  }

  const rows = await transaction<{ readonly authorized: number }[]>`
    select 1 as authorized
    from source_revision_private_views private_view
    join source_revisions source_revision
      on source_revision.id = private_view.source_revision_id
      and source_revision.participant_epoch_id = private_view.participant_epoch_id
    join participant_epochs epoch on epoch.id = private_view.participant_epoch_id
    join conversations conversation on conversation.id = epoch.conversation_id
    join epoch_participants participant
      on participant.participant_epoch_id = private_view.participant_epoch_id
      and participant.person_identity_id = private_view.person_identity_id
    join person_identities identity
      on identity.id = participant.person_identity_id
      and identity.person_id = private_view.person_id
    join people person on person.id = private_view.person_id
    join participant_policies policy
      on policy.conversation_id = conversation.id
      and policy.person_id = private_view.person_id
      and policy.status = 'active'
    where private_view.source_revision_id = ${revision.id}
      and private_view.participant_epoch_id = ${scope.participantEpochId}
      and private_view.person_id = ${privateViewerPersonId}
      and private_view.status = 'active'
      and private_view.granted_at <= ${asOf}
      and private_view.retention_until > ${asOf}
      and source_revision.revoked_at is null
      and source_revision.retention_until > ${asOf}
      and source_revision.conversation_access_mode = 'independent_private_views'
      and participant.registration_status = 'registered'
      and participant.consented_at is not null
      and identity.status = 'verified'
      and person.status = 'registered'
      and person.control_epoch = private_view.person_control_epoch
      and conversation.status = 'active'
      and policy.allow_content_processing = true
      and policy.retention_seconds > 0
      and policy.effective_at <= ${asOf}
      and least(
        private_view.retention_until,
        source_revision.captured_at + policy.retention_seconds * interval '1 second'
      ) > ${asOf}
    limit 1
  `;
  if (!rows[0]) {
    throw new UnauthorizedError("No active exact private source view exists for this person");
  }
}

async function loadReadableRevision(
  transaction: Transaction,
  revisionId: string,
  scope: SourceScope,
  asOf: Date,
  lock: boolean,
): Promise<SourceRevisionRow> {
  const rows = lock
    ? await transaction<SourceRevisionRow[]>`
        select id, source_object_id, revision_number, owner_person_id, participant_epoch_id,
          scope_digest, content_digest, content_ciphertext, content_key_version,
          occurred_at, captured_at, retention_until, revoked_at, conversation_access_mode
        from source_revisions where id = ${revisionId} for update
      `
    : await transaction<SourceRevisionRow[]>`
        select id, source_object_id, revision_number, owner_person_id, participant_epoch_id,
          scope_digest, content_digest, content_ciphertext, content_key_version,
          occurred_at, captured_at, retention_until, revoked_at, conversation_access_mode
        from source_revisions where id = ${revisionId}
      `;
  const revision = rows[0];
  if (!revision) throw new NotFoundError("Source revision does not exist");
  assertScopeMatchesRow(scope, revision);
  if (revision.revoked_at !== null || revision.retention_until <= asOf) {
    throw new NotFoundError("Source revision is no longer retained");
  }
  return revision;
}

function assertScopeMatchesRow(
  scope: SourceScope,
  row: { readonly owner_person_id: string | null; readonly participant_epoch_id: string | null },
): void {
  const matches =
    scope.kind === "person"
      ? row.owner_person_id === scope.personId && row.participant_epoch_id === null
      : row.participant_epoch_id === scope.participantEpochId && row.owner_person_id === null;
  if (!matches) throw new UnauthorizedError("Source is outside the requested exact scope");
}

async function invalidateRevisions(
  transaction: Transaction,
  revisionIds: readonly string[],
  invalidatedAt: Date,
): Promise<{ readonly invalidatedRevisionCount: number; readonly revokedCandidateCount: number }> {
  if (revisionIds.length === 0) {
    return { invalidatedRevisionCount: 0, revokedCandidateCount: 0 };
  }
  const mutableRevisionIds = [...revisionIds];
  const revokedCandidateCount = await invalidateRevisionDependents(transaction, mutableRevisionIds);
  await transaction`
    update source_revision_private_views
    set status = 'revoked',
        revoked_at = greatest(${invalidatedAt}, granted_at)
    where source_revision_id = any(${transaction.array(mutableRevisionIds)}::uuid[])
      and status = 'active'
  `;
  await transaction`
    delete from source_blobs
    where source_revision_id = any(${transaction.array(mutableRevisionIds)}::uuid[])
  `;
  const invalidated = await transaction<{ readonly id: string }[]>`
    update source_revisions
    set content_ciphertext = null, content_key_version = null,
        revoked_at = coalesce(revoked_at, ${invalidatedAt})
    where id = any(${transaction.array(mutableRevisionIds)}::uuid[])
    returning id
  `;
  return {
    invalidatedRevisionCount: invalidated.length,
    revokedCandidateCount,
  };
}

async function invalidateRevisionDependents(
  transaction: Transaction,
  revisionIds: readonly string[],
): Promise<number> {
  if (revisionIds.length === 0) return 0;
  const mutableRevisionIds = [...revisionIds];
  await transaction`
    delete from source_derivatives
    where parent_source_revision_id = any(${transaction.array(mutableRevisionIds)}::uuid[])
  `;
  const revokedCandidates = await transaction<{ readonly id: string }[]>`
    update knowledge_candidates candidate
    set status = 'revoked'
    where candidate.status = 'pending'
      and exists (
        select 1
        from jsonb_array_elements_text(candidate.evidence_refs) evidence(reference)
        where evidence.reference = any(${transaction.array(mutableRevisionIds)}::text[])
      )
    returning candidate.id
  `;
  return revokedCandidates.length;
}

async function promotePrivateCandidateMemory(
  transaction: Transaction,
  secretBox: SecretBox,
  candidateId: string,
  personId: string,
  candidate: {
    readonly candidate_kind: string;
    readonly content_digest: string;
    readonly content_ciphertext: Buffer;
    readonly evidence_refs: unknown;
  },
  acceptedAt: Date,
): Promise<void> {
  const memoryKey = `${candidate.candidate_kind}:${candidate.content_digest.slice(0, 16)}`;
  await transaction`
    select pg_advisory_xact_lock(hashtextextended(${`memory:${personId}:${memoryKey}`}, 0))
  `;
  const existing = await transaction<
    {
      readonly id: string;
      readonly status: "accepted" | "forgotten";
      readonly current_revision_id: string | null;
    }[]
  >`
    select id, status, current_revision_id
    from memory_records
    where scope_kind = 'person' and owner_person_id = ${personId}
      and memory_key = ${memoryKey} and status in ('accepted', 'forgotten')
    order by case status when 'forgotten' then 0 else 1 end
    for update
  `;
  if (existing[0]?.status === "forgotten") return;
  if (existing[0]?.current_revision_id) return;

  const memoryRecordId = existing[0]?.id ?? randomUUID();
  if (!existing[0]) {
    await transaction`
      insert into memory_records (
        id, scope_kind, owner_person_id, memory_key, status, current_revision_id,
        version, expires_at, revoked_at, created_at, updated_at
      ) values (
        ${memoryRecordId}, 'person', ${personId}, ${memoryKey}, 'accepted', null,
        1, null, null, ${acceptedAt}, ${acceptedAt}
      )
    `;
  }

  const revisionId = randomUUID();
  const plaintext = openBytes(secretBox, candidate.content_ciphertext, privateCandidatePurpose(candidateId));
  const sealed = sealBytes(secretBox, plaintext, memoryRevisionPurpose(revisionId));
  await transaction`
    insert into memory_revisions (
      id, memory_record_id, revision, content_digest, content_ciphertext,
      content_key_version, scope_digest, evidence_refs, accepted_by_person_id,
      effective_at, ended_at
    ) values (
      ${revisionId}, ${memoryRecordId}, 1, ${candidate.content_digest}, ${sealed.ciphertext},
      ${sealed.keyVersion}, ${canonicalDigest({ scopeKind: "person", ownerPersonId: personId })},
      ${transaction.json(stringArray(candidate.evidence_refs))}, ${personId}, ${acceptedAt}, null
    )
  `;
  await transaction`
    update memory_records
    set current_revision_id = ${revisionId}, updated_at = ${acceptedAt}
    where id = ${memoryRecordId} and current_revision_id is null
  `;
}

function sourceObjectIdentity(
  integrationId: string | null,
  scope: SourceScope,
  artifactKind: string,
  origin: { readonly system: string; readonly remoteObjectId: string },
): string {
  return canonicalDigest({
    integrationId,
    scope,
    artifactKind,
    system: origin.system,
    remoteObjectId: origin.remoteObjectId,
  });
}

function integrationView(
  row: IntegrationRow,
  activeCapabilities: readonly IntegrationCapability[],
): IntegrationView {
  return {
    integrationId: row.id,
    personId: row.person_id,
    provider: row.provider,
    accountKind: integrationAccountKind(row.account_kind),
    activeCapabilities: integrationCapabilities(activeCapabilities),
    status: row.status as IntegrationStatus,
    controlEpoch: Number(row.control_epoch),
    connectedAt: row.connected_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}

function integrationCapabilities(capabilities: readonly string[]): IntegrationCapability[] {
  const order = new Map(IntegrationCapabilitySchema.options.map((capability, index) => [capability, index]));
  return capabilities
    .map((capability) => IntegrationCapabilitySchema.parse(capability))
    .sort((left, right) => (order.get(left) ?? 0) - (order.get(right) ?? 0));
}

function integrationAccountKind(accountKind: string): IntegrationAccountKind {
  return IntegrationAccountKindSchema.parse(accountKind);
}

function sealJson(
  secretBox: SecretBox,
  value: unknown,
  purpose: string,
  maximumBytes: number,
): EncryptedColumn {
  const serialized = canonicalJson(value);
  assertByteLimit(serialized, maximumBytes, "Encrypted JSON");
  return sealBytes(secretBox, Buffer.from(serialized, "utf8"), purpose);
}

function sealBytes(secretBox: SecretBox, value: Uint8Array, purpose: string): EncryptedColumn {
  const encrypted = secretBox.encrypt(value, purpose);
  return {
    ciphertext: Buffer.from(JSON.stringify(encrypted), "utf8"),
    keyVersion: encrypted.kid,
  };
}

function openJson(secretBox: SecretBox, value: Buffer, purpose: string): unknown {
  return JSON.parse(openBytes(secretBox, value, purpose).toString("utf8")) as unknown;
}

function openBytes(secretBox: SecretBox, value: Buffer, purpose: string): Buffer {
  const encrypted = JSON.parse(value.toString("utf8")) as EncryptedValue;
  return secretBox.decrypt(encrypted, purpose);
}

function integrationPurpose(integrationId: string): string {
  return `florence:integration:${integrationId}:credentials`;
}

function oauthPurpose(oauthAttemptId: string): string {
  return `florence:oauth:${oauthAttemptId}:pkce`;
}

function cursorPurpose(integrationId: string, resourceKind: string): string {
  return `florence:integration:${integrationId}:cursor:${resourceKind}`;
}

function sourceRevisionPurpose(sourceRevisionId: string): string {
  return `florence:source-revision:${sourceRevisionId}:content`;
}

function sourceBlobPurpose(sourceBlobId: string): string {
  return `florence:source-blob:${sourceBlobId}:bytes`;
}

function sourceDerivativePurpose(sourceDerivativeId: string): string {
  return `florence:source-derivative:${sourceDerivativeId}:content`;
}

function privateCandidatePurpose(candidateId: string): string {
  return `florence:knowledge-candidate:${candidateId}:content`;
}

function memoryRevisionPurpose(memoryRevisionId: string): string {
  return `florence:memory-revision:${memoryRevisionId}:content`;
}

function earliestDate(...dates: readonly Date[]): Date {
  return new Date(Math.min(...dates.map((date) => date.getTime())));
}

function sha256(value: string | Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

function assertByteLimit(value: string, limit: number, label: string): void {
  if (Buffer.byteLength(value, "utf8") > limit)
    throw new ConflictError(`${label} exceeds the configured limit`);
}

function requirePositiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`);
  return value;
}

function requireRow<T>(value: T | undefined, message: string): T {
  if (value === undefined) throw new Error(message);
  return value;
}

function stringArray(value: unknown): string[] {
  if (!Array.isArray(value) || !value.every((entry) => typeof entry === "string")) {
    throw new Error("Stored evidence references are invalid");
  }
  return value;
}
