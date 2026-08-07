import { createHash, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { Database } from "../../db/client.js";
import { keyedDigest, randomOpaqueToken, type SecretBox, secureDigestEquals } from "../../shared/crypto.js";
import { ConflictError, NotFoundError, UnauthorizedError } from "../../shared/errors.js";
import {
  type AssuranceKind,
  type AuthenticatedSession,
  type CreatedHandoff,
  type CreateHandoffInput,
  CreateHandoffInputSchema,
  type HandoffPreview,
  HandoffPurposeSchema,
  type SessionPrincipal,
} from "./contracts.js";

interface HandoffRow {
  id: string;
  person_id: string;
  purpose: string;
  identity_authority_version: number | string;
  current_identity_authority_version: number | string;
  identity_status: string;
  person_status: string;
  person_control_epoch: number | string;
  expires_at: Date;
  consumed_at: Date | null;
  context_ciphertext: Buffer | null;
  context_key_version: string | null;
}

interface SessionRow {
  id: string;
  person_id: string;
  person_control_epoch: number | string;
  current_control_epoch: number | string;
  person_status: string;
  created_at: Date;
  last_seen_at: Date;
  idle_expires_at: Date;
  absolute_expires_at: Date;
  revoked_at: Date | null;
  assurance_kind: AssuranceKind;
  assurance_context: Record<string, string>;
  assurance_expires_at: Date | null;
}

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

export class PostgresWebAuth {
  public constructor(
    private readonly database: Executor,
    private readonly secretBox: SecretBox,
    private readonly tokenKey: string,
  ) {}

  public async createHandoff(inputCandidate: CreateHandoffInput): Promise<CreatedHandoff> {
    const input = CreateHandoffInputSchema.parse(inputCandidate);
    const now = new Date();
    const expiresAt = new Date(now.getTime() + input.expiresInSeconds * 1000);
    const token = randomOpaqueToken(32);
    const tokenDigest = sha256Hex(token);
    const context = this.secretBox.encrypt(JSON.stringify(input.context), "auth-handoff-context");
    const handoffId = randomUUID();
    await inTransaction(this.database, async (transaction) => {
      const identities = await transaction<
        { authority_version: number | string; person_id: string; status: string; person_status: string }[]
      >`
        select identity.authority_version, identity.person_id, identity.status,
          person.status as person_status
        from person_identities identity
        join people person on person.id = identity.person_id
        where identity.id = ${input.privateIdentityId}
        for update of identity, person
      `;
      const identity = identities[0];
      if (!identity || identity.person_id !== input.personId) {
        throw new UnauthorizedError("The private identity does not belong to this person");
      }
      if (identity.status !== "verified" || identity.person_status !== "registered") {
        throw new UnauthorizedError("Only a registered person may receive a web handoff");
      }
      if (input.privateConversationId) {
        const exactDirect = await transaction<{ ok: boolean }[]>`
          select exists(
            select 1
            from conversations conversation
            join participant_epochs epoch on epoch.id = conversation.current_epoch_id
            join epoch_participants participant on participant.participant_epoch_id = epoch.id
            where conversation.id = ${input.privateConversationId}
              and conversation.kind = 'direct'
              and conversation.status = 'active'
              and epoch.ended_at is null
              and participant.person_id = ${input.personId}
          ) as ok
        `;
        if (!exactDirect[0]?.ok) throw new UnauthorizedError("Handoff requires the exact active private DM");
      }
      await transaction`
        insert into auth_handoffs (
          id, person_id, private_identity_id, private_conversation_id, token_digest,
          purpose, identity_authority_version, context_ciphertext, context_key_version,
          expires_at, created_at
        ) values (
          ${handoffId}, ${input.personId}, ${input.privateIdentityId}, ${input.privateConversationId},
          ${tokenDigest}, ${input.purpose}, ${Number(identity.authority_version)},
          ${Buffer.from(JSON.stringify(context), "utf8")}, ${context.kid}, ${expiresAt}, ${now}
        )
      `;
    });
    return { handoffId, token, expiresAt };
  }

  /** Reads only public handoff metadata. It deliberately does not consume the token. */
  public async previewHandoff(token: string, now = new Date()): Promise<HandoffPreview> {
    const row = await this.loadHandoff(token);
    assertUsableHandoff(row, now);
    const context = row.purpose === "group_coverage" ? decryptContext(row, this.secretBox) : {};
    const groupLabel =
      typeof context.groupLabel === "string" &&
      context.groupLabel.length > 0 &&
      context.groupLabel.length <= 240
        ? context.groupLabel
        : null;
    return {
      handoffId: row.id,
      purpose: HandoffPurposeSchema.parse(row.purpose),
      expiresAt: row.expires_at,
      ...(groupLabel ? { groupLabel } : {}),
    };
  }

  /** Atomically consumes a handoff, revalidates its exact identity, and creates one browser session. */
  public async consumeHandoff(token: string, now = new Date()): Promise<AuthenticatedSession> {
    const tokenDigest = sha256Hex(token);
    return inTransaction(this.database, async (transaction) => {
      const rows = await transaction<HandoffRow[]>`
        select handoff.id, handoff.person_id, handoff.purpose,
          handoff.context_ciphertext, handoff.context_key_version,
          handoff.identity_authority_version,
          identity.authority_version as current_identity_authority_version,
          identity.status as identity_status, person.status as person_status,
          person.control_epoch as person_control_epoch,
          handoff.expires_at, handoff.consumed_at
        from auth_handoffs handoff
        join person_identities identity on identity.id = handoff.private_identity_id
        join people person on person.id = handoff.person_id
        where handoff.token_digest = ${tokenDigest}
        for update of handoff, identity, person
      `;
      const row = rows[0];
      if (!row) throw new NotFoundError("This sign-in link is invalid");
      assertUsableHandoff(row, now);
      if (Number(row.identity_authority_version) !== Number(row.current_identity_authority_version)) {
        throw new UnauthorizedError("Identity authority changed; request a new private link");
      }

      const sessionId = randomUUID();
      const sessionToken = randomOpaqueToken(32);
      const sessionDigest = sha256Hex(sessionToken);
      const idleExpiresAt = new Date(now.getTime() + 30 * 24 * 60 * 60 * 1000);
      const absoluteExpiresAt = new Date(now.getTime() + 90 * 24 * 60 * 60 * 1000);
      const assuranceKind: AssuranceKind =
        row.purpose === "google_connect" ||
        row.purpose === "account_controls" ||
        row.purpose === "household_invitation" ||
        row.purpose === "group_coverage" ||
        row.purpose === "private_bridge_standing"
          ? row.purpose
          : "base";
      const assuranceContext = assuranceKind === "base" ? {} : decryptContext(row, this.secretBox);
      const assuranceExpiresAt = assuranceKind === "base" ? null : new Date(now.getTime() + 15 * 60_000);
      await transaction`
        update auth_handoffs set consumed_at = ${now}
        where id = ${row.id} and consumed_at is null
      `;
      await transaction`
        insert into person_sessions (
          id, person_id, session_digest, person_control_epoch, assurance_kind, assurance_context,
          assurance_expires_at, idle_expires_at, absolute_expires_at, last_seen_at, created_at
        ) values (
          ${sessionId}, ${row.person_id}, ${sessionDigest}, ${Number(row.person_control_epoch)},
          ${assuranceKind}, ${transaction.json(assuranceContext)}, ${assuranceExpiresAt}, ${idleExpiresAt}, ${absoluteExpiresAt}, ${now}, ${now}
        )
      `;
      return {
        sessionId,
        personId: row.person_id,
        sessionToken,
        csrfToken: csrfForSession(this.tokenKey, sessionDigest),
        idleExpiresAt,
        absoluteExpiresAt,
        assuranceKind,
        assuranceContext,
        assuranceExpiresAt,
      };
    });
  }

  public async authenticate(sessionToken: string, now = new Date()): Promise<SessionPrincipal> {
    const sessionDigest = sha256Hex(sessionToken);
    const rows = await this.database<SessionRow[]>`
      select session.id, session.person_id, session.person_control_epoch,
        person.control_epoch as current_control_epoch, person.status as person_status,
        session.created_at, session.last_seen_at, session.idle_expires_at,
        session.absolute_expires_at, session.revoked_at,
        session.assurance_kind, session.assurance_context, session.assurance_expires_at
      from person_sessions session
      join people person on person.id = session.person_id
      where session.session_digest = ${sessionDigest}
    `;
    const row = rows[0];
    if (
      !row ||
      row.revoked_at ||
      row.person_status !== "registered" ||
      Number(row.person_control_epoch) !== Number(row.current_control_epoch) ||
      row.idle_expires_at <= now ||
      row.absolute_expires_at <= now
    ) {
      throw new UnauthorizedError("Session is no longer valid");
    }
    const nextIdle = new Date(
      Math.min(now.getTime() + 30 * 24 * 60 * 60 * 1000, row.absolute_expires_at.getTime()),
    );
    if (now.getTime() - row.last_seen_at.getTime() >= 5 * 60 * 1000) {
      await this.database`
        update person_sessions
        set last_seen_at = ${now}, idle_expires_at = ${nextIdle}
        where id = ${row.id} and revoked_at is null
      `;
      row.last_seen_at = now;
      row.idle_expires_at = nextIdle;
    }
    return {
      sessionId: row.id,
      personId: row.person_id,
      controlEpoch: Number(row.person_control_epoch),
      csrfToken: csrfForSession(this.tokenKey, sessionDigest),
      createdAt: row.created_at,
      lastSeenAt: row.last_seen_at,
      idleExpiresAt: row.idle_expires_at,
      absoluteExpiresAt: row.absolute_expires_at,
      assuranceKind: row.assurance_kind,
      assuranceContext: row.assurance_context,
      assuranceExpiresAt: row.assurance_expires_at,
    };
  }

  public verifyCsrf(principal: SessionPrincipal, supplied: string | undefined): void {
    if (!supplied || !secureDigestEquals(principal.csrfToken, supplied)) {
      throw new UnauthorizedError("The request confirmation token is invalid");
    }
  }

  public async revokeSession(sessionId: string, personId: string, now = new Date()): Promise<void> {
    await this.database`
      update person_sessions set revoked_at = ${now}
      where id = ${sessionId} and person_id = ${personId} and revoked_at is null
    `;
  }

  public async revokeAllSessions(personId: string, now = new Date()): Promise<void> {
    await this.database`
      update person_sessions set revoked_at = ${now}
      where person_id = ${personId} and revoked_at is null
    `;
  }

  async loadHandoff(token: string): Promise<HandoffRow> {
    const rows = await this.database<HandoffRow[]>`
      select handoff.id, handoff.person_id, handoff.purpose,
        handoff.context_ciphertext, handoff.context_key_version,
        handoff.identity_authority_version,
        identity.authority_version as current_identity_authority_version,
        identity.status as identity_status, person.status as person_status,
        person.control_epoch as person_control_epoch,
        handoff.expires_at, handoff.consumed_at
      from auth_handoffs handoff
      join person_identities identity on identity.id = handoff.private_identity_id
      join people person on person.id = handoff.person_id
      where handoff.token_digest = ${sha256Hex(token)}
    `;
    const row = rows[0];
    if (!row) throw new NotFoundError("This sign-in link is invalid");
    return row;
  }
}

function decryptContext(row: HandoffRow, secretBox: SecretBox): Record<string, string> {
  if (!row.context_ciphertext || !row.context_key_version) return {};
  const encrypted: unknown = JSON.parse(row.context_ciphertext.toString("utf8"));
  return JSON.parse(secretBox.decrypt(encrypted, "auth-handoff-context").toString("utf8")) as Record<
    string,
    string
  >;
}

function inTransaction<Result>(
  executor: Executor,
  operation: (transaction: Transaction) => Promise<Result>,
): Promise<Result> {
  return "begin" in executor
    ? (executor.begin(operation) as unknown as Promise<Result>)
    : operation(executor);
}

function assertUsableHandoff(row: HandoffRow, now: Date): void {
  if (row.consumed_at) throw new ConflictError("This private link has already been used");
  if (row.expires_at <= now) throw new UnauthorizedError("This private link has expired");
  if (row.identity_status !== "verified" || row.person_status !== "registered") {
    throw new UnauthorizedError("Identity is no longer eligible for this private link");
  }
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function csrfForSession(secret: string, sessionDigest: string): string {
  return keyedDigest(secret, `csrf:${sessionDigest}`);
}
