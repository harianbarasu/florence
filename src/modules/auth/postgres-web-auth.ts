import { createHash, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import type { Database } from "../../db/client.js";
import { keyedDigest, randomOpaqueToken, type SecretBox, secureDigestEquals } from "../../shared/crypto.js";
import { ConflictError, NotFoundError, UnauthorizedError } from "../../shared/errors.js";
import {
  type AssuranceKind,
  type AuthenticatedSession,
  type BeginGoogleAuthAttemptInput,
  BeginGoogleAuthAttemptInputSchema,
  type CompleteGoogleLoginInput,
  CompleteGoogleLoginInputSchema,
  type CreatedGoogleAuthAttempt,
  type CreatedHandoff,
  type CreateHandoffInput,
  CreateHandoffInputSchema,
  GOOGLE_AUTH_ATTEMPT_TTL_SECONDS,
  type GoogleAuthAttemptAccess,
  type GoogleLoginCompletion,
  type HandoffPreview,
  HandoffPurposeSchema,
  hasFreshGoogleIdentityLinkAssurance,
  type SessionPrincipal,
} from "./contracts.js";

interface HandoffRow {
  id: string;
  person_id: string;
  private_identity_id: string;
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
  authentication_identity_id: string;
  authentication_identity_authority_version: number | string;
  current_identity_authority_version: number | string;
  authentication_identity_person_id: string;
  authentication_identity_status: string;
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

interface GoogleAuthAttemptRow {
  id: string;
  provider: string;
  mode: "login" | "link";
  browser_binding_digest: string;
  secret_ciphertext: Buffer;
  secret_key_version: string;
  person_id: string | null;
  initiating_session_id: string | null;
  person_control_epoch: number | string | null;
  return_path: string;
  expires_at: Date;
  consumed_at: Date | null;
  current_person_control_epoch: number | string | null;
  person_status: string | null;
  session_person_id: string | null;
  session_person_control_epoch: number | string | null;
  session_revoked_at: Date | null;
  session_idle_expires_at: Date | null;
  session_absolute_expires_at: Date | null;
  session_authentication_identity_id: string | null;
  session_authentication_identity_authority_version: number | string | null;
  current_session_identity_authority_version: number | string | null;
  session_identity_person_id: string | null;
  session_identity_status: string | null;
}

interface LoginIdentityRow {
  id: string;
  person_id: string;
  authority_version: number | string;
  status: string;
  person_status: string;
  person_control_epoch: number | string;
}

interface GoogleLinkSessionRow {
  readonly assurance_kind: AssuranceKind;
  readonly assurance_context: unknown;
  readonly assurance_expires_at: Date | null;
}

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

export class PostgresWebAuth {
  public constructor(
    private readonly database: Executor,
    private readonly secretBox: SecretBox,
    private readonly tokenKey: string,
  ) {}

  /**
   * Starts a short, browser-bound Google identity ceremony. Login attempts have
   * no presumed Florence owner. Link attempts are fenced to one current session
   * and may be canonized only through FlorenceApplication.process().
   */
  public async beginGoogleAuthAttempt(
    inputCandidate: BeginGoogleAuthAttemptInput,
    now = new Date(),
  ): Promise<CreatedGoogleAuthAttempt> {
    const input = BeginGoogleAuthAttemptInputSchema.parse(inputCandidate);
    const attemptId = randomUUID();
    const state = randomOpaqueToken(32);
    const browserBinding = randomOpaqueToken(32);
    const pkceVerifier = randomOpaqueToken(48);
    const pkceChallenge = createHash("sha256").update(pkceVerifier).digest("base64url");
    const nonce = randomOpaqueToken(32);
    const expiresAt = new Date(now.getTime() + GOOGLE_AUTH_ATTEMPT_TTL_SECONDS * 1000);
    const secret = this.secretBox.encrypt(
      JSON.stringify({ pkceVerifier, nonce }),
      googleAuthAttemptSecretPurpose(attemptId),
    );

    await inTransaction(this.database, async (transaction) => {
      if (input.mode === "link") {
        const sessions = await transaction<GoogleLinkSessionRow[]>`
          select session.assurance_kind, session.assurance_context, session.assurance_expires_at
          from person_sessions session
          join people person on person.id = session.person_id
          join person_identities identity on identity.id = session.authentication_identity_id
          where session.id = ${input.initiatingSessionId}
            and session.person_id = ${input.personId}
            and session.person_control_epoch = ${input.personControlEpoch}
            and person.status = 'registered'
            and person.control_epoch = ${input.personControlEpoch}
            and session.revoked_at is null
            and session.idle_expires_at > ${now}
            and session.absolute_expires_at > ${now}
            and identity.person_id = session.person_id
            and identity.status = 'verified'
            and identity.authority_version = session.authentication_identity_authority_version
          for update of session, person, identity
        `;
        const session = sessions[0];
        if (!session) {
          throw new UnauthorizedError("Google account linking requires the exact active Florence session");
        }
        const existingGoogleIdentities = await transaction<{ readonly present: boolean }[]>`
          select exists(
            select 1 from person_identities
            where person_id = ${input.personId}
              and issuer = 'google'
              and kind = 'provider_account'
              and status = 'verified'
          ) as present
        `;
        if (
          !hasFreshGoogleIdentityLinkAssurance({
            hasVerifiedGoogleIdentity: existingGoogleIdentities[0]?.present === true,
            assuranceKind: session.assurance_kind,
            assuranceContext: session.assurance_context,
            assuranceExpiresAt: session.assurance_expires_at,
            asOf: now,
          })
        ) {
          throw new UnauthorizedError(
            existingGoogleIdentities[0]?.present
              ? "Linking another Google identity requires a fresh exact private confirmation"
              : "Linking the first Google identity requires a fresh onboarding handoff",
          );
        }
      }

      await transaction`
        insert into web_auth_attempts (
          id, provider, mode, state_digest, browser_binding_digest,
          secret_ciphertext, secret_key_version, person_id, initiating_session_id,
          person_control_epoch, return_path, expires_at, created_at
        ) values (
          ${attemptId}, 'google', ${input.mode}, ${sha256Hex(state)}, ${sha256Hex(browserBinding)},
          ${Buffer.from(JSON.stringify(secret), "utf8")}, ${secret.kid},
          ${input.mode === "link" ? input.personId : null},
          ${input.mode === "link" ? input.initiatingSessionId : null},
          ${input.mode === "link" ? input.personControlEpoch : null},
          ${input.returnPath}, ${expiresAt}, ${now}
        )
      `;
    });

    return { attemptId, mode: input.mode, state, browserBinding, pkceChallenge, nonce, expiresAt };
  }

  /**
   * Reopens a Google callback only for the browser that started it. This is
   * deliberately read-only: link-mode authority is committed later by the app.
   */
  public async readGoogleAuthAttempt(
    state: string,
    browserBinding: string,
    now = new Date(),
  ): Promise<GoogleAuthAttemptAccess> {
    const row = await loadGoogleAuthAttempt(this.database, sha256Hex(state), false);
    assertUsableGoogleAuthAttempt(row, browserBinding, now);
    const secret = decryptGoogleAuthSecret(row, this.secretBox);
    const common = {
      attemptId: row.id,
      provider: "google" as const,
      pkceVerifier: secret.pkceVerifier,
      nonce: secret.nonce,
      returnPath: row.return_path,
      expiresAt: row.expires_at,
    };
    if (row.mode === "login") {
      return {
        ...common,
        mode: "login",
        personId: null,
        initiatingSessionId: null,
        personControlEpoch: null,
      };
    }
    if (!row.person_id || !row.initiating_session_id || row.person_control_epoch === null) {
      throw new UnauthorizedError("Google account link authority is incomplete");
    }
    return {
      ...common,
      mode: "link",
      personId: row.person_id,
      initiatingSessionId: row.initiating_session_id,
      personControlEpoch: Number(row.person_control_epoch),
    };
  }

  /**
   * Settles a returning Google login. Unknown subjects are consumed as a
   * privacy-preserving not-linked result and never create a Florence person.
   */
  public async completeGoogleLogin(
    inputCandidate: CompleteGoogleLoginInput,
    now = new Date(),
  ): Promise<GoogleLoginCompletion> {
    const input = CompleteGoogleLoginInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      const row = await loadGoogleAuthAttempt(transaction, sha256Hex(input.state), true);
      assertUsableGoogleAuthAttempt(row, input.browserBinding, now);
      if (row.mode !== "login") {
        throw new UnauthorizedError("A Google account link must be completed through Florence authority");
      }

      const identities = await transaction<LoginIdentityRow[]>`
        select identity.id, identity.person_id, identity.authority_version, identity.status,
          person.status as person_status, person.control_epoch as person_control_epoch
        from person_identities identity
        join people person on person.id = identity.person_id
        where identity.issuer = 'google'
          and identity.kind = 'provider_account'
          and identity.subject_digest = ${input.externalSubjectDigest}
        for update of identity, person
      `;
      const identity = identities[0];
      const retiredSecret = this.secretBox.encrypt(
        JSON.stringify({ consumed: true }),
        googleAuthAttemptSecretPurpose(row.id),
      );
      const consumed = await transaction<{ readonly id: string }[]>`
        update web_auth_attempts
        set consumed_at = ${now},
          secret_ciphertext = ${Buffer.from(JSON.stringify(retiredSecret), "utf8")},
          secret_key_version = ${retiredSecret.kid}
        where id = ${row.id} and consumed_at is null
        returning id
      `;
      if (!consumed[0]) throw new ConflictError("Google sign-in attempt was already used");

      if (identity?.status !== "verified" || identity.person_status !== "registered") {
        return { kind: "not_linked", returnPath: row.return_path };
      }

      const session = await createBaseSession(
        transaction,
        this.tokenKey,
        {
          personId: identity.person_id,
          personControlEpoch: Number(identity.person_control_epoch),
          identityId: identity.id,
          identityAuthorityVersion: Number(identity.authority_version),
        },
        now,
      );
      return {
        kind: "signed_in",
        identityId: identity.id,
        returnPath: row.return_path,
        session,
      };
    });
  }

  public async sweepGoogleAuthAttempts(asOf = new Date(), limit = 500): Promise<number> {
    if (Number.isNaN(asOf.getTime())) throw new TypeError("Google auth retention time is invalid");
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 10_000) {
      throw new TypeError("Google auth retention limit must be between 1 and 10000");
    }
    return inTransaction(this.database, async (transaction) => {
      const deleted = await transaction<{ readonly id: string }[]>`
        delete from web_auth_attempts
        where id in (
          select id from web_auth_attempts
          where (consumed_at is not null and consumed_at <= ${asOf}) or expires_at <= ${asOf}
          order by created_at
          limit ${limit}
          for update skip locked
        )
        returning id
      `;
      return deleted.length;
    });
  }

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
    return {
      handoffId: row.id,
      purpose: HandoffPurposeSchema.parse(row.purpose),
      expiresAt: row.expires_at,
    };
  }

  /** Atomically consumes a handoff, revalidates its exact identity, and creates one browser session. */
  public async consumeHandoff(token: string, now = new Date()): Promise<AuthenticatedSession> {
    const tokenDigest = sha256Hex(token);
    return inTransaction(this.database, async (transaction) => {
      const rows = await transaction<HandoffRow[]>`
        select handoff.id, handoff.person_id, handoff.private_identity_id, handoff.purpose,
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
        row.purpose === "onboarding" ||
        row.purpose === "account_controls" ||
        row.purpose === "household_invitation" ||
        row.purpose === "private_bridge_standing"
          ? row.purpose
          : "base";
      const assuranceContext =
        assuranceKind === "base"
          ? row.purpose === "web_sign_in"
            ? webSignInAssuranceContext(decryptContext(row, this.secretBox))
            : {}
          : decryptContext(row, this.secretBox);
      const assuranceExpiresAt =
        assuranceKind === "base"
          ? null
          : new Date(now.getTime() + (assuranceKind === "onboarding" ? 60 : 15) * 60_000);
      await transaction`
        update auth_handoffs set consumed_at = ${now}
        where id = ${row.id} and consumed_at is null
      `;
      await transaction`
        insert into person_sessions (
          id, person_id, session_digest, person_control_epoch,
          authentication_identity_id, authentication_identity_authority_version,
          assurance_kind, assurance_context, assurance_expires_at,
          idle_expires_at, absolute_expires_at, last_seen_at, created_at
        ) values (
          ${sessionId}, ${row.person_id}, ${sessionDigest}, ${Number(row.person_control_epoch)},
          ${row.private_identity_id}, ${Number(row.current_identity_authority_version)},
          ${assuranceKind}, ${transaction.json(assuranceContext)}, ${assuranceExpiresAt},
          ${idleExpiresAt}, ${absoluteExpiresAt}, ${now}, ${now}
        )
      `;
      return {
        sessionId,
        personId: row.person_id,
        authenticationIdentityId: row.private_identity_id,
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
      select session.id, session.person_id, session.authentication_identity_id,
        session.authentication_identity_authority_version,
        identity.authority_version as current_identity_authority_version,
        identity.person_id as authentication_identity_person_id,
        identity.status as authentication_identity_status,
        session.person_control_epoch,
        person.control_epoch as current_control_epoch, person.status as person_status,
        session.created_at, session.last_seen_at, session.idle_expires_at,
        session.absolute_expires_at, session.revoked_at,
        session.assurance_kind, session.assurance_context, session.assurance_expires_at
      from person_sessions session
      join people person on person.id = session.person_id
      join person_identities identity on identity.id = session.authentication_identity_id
      where session.session_digest = ${sessionDigest}
    `;
    const row = rows[0];
    if (
      !row ||
      row.revoked_at ||
      row.person_status !== "registered" ||
      row.authentication_identity_person_id !== row.person_id ||
      row.authentication_identity_status !== "verified" ||
      Number(row.authentication_identity_authority_version) !==
        Number(row.current_identity_authority_version) ||
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
      authenticationIdentityId: row.authentication_identity_id,
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
      select handoff.id, handoff.person_id, handoff.private_identity_id, handoff.purpose,
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

async function loadGoogleAuthAttempt(
  executor: Executor,
  stateDigest: string,
  lock: boolean,
): Promise<GoogleAuthAttemptRow> {
  const rows = lock
    ? await executor<GoogleAuthAttemptRow[]>`
        select attempt.id, attempt.provider, attempt.mode, attempt.browser_binding_digest,
          attempt.secret_ciphertext, attempt.secret_key_version, attempt.person_id,
          attempt.initiating_session_id, attempt.person_control_epoch, attempt.return_path,
          attempt.expires_at, attempt.consumed_at,
          person.control_epoch as current_person_control_epoch, person.status as person_status,
          session.person_id as session_person_id,
          session.person_control_epoch as session_person_control_epoch,
          session.revoked_at as session_revoked_at,
          session.idle_expires_at as session_idle_expires_at,
          session.absolute_expires_at as session_absolute_expires_at,
          session.authentication_identity_id as session_authentication_identity_id,
          session.authentication_identity_authority_version,
          identity.authority_version as current_session_identity_authority_version,
          identity.person_id as session_identity_person_id,
          identity.status as session_identity_status
        from web_auth_attempts attempt
        left join people person on person.id = attempt.person_id
        left join person_sessions session on session.id = attempt.initiating_session_id
        left join person_identities identity on identity.id = session.authentication_identity_id
        where attempt.provider = 'google' and attempt.state_digest = ${stateDigest}
        for update of attempt
      `
    : await executor<GoogleAuthAttemptRow[]>`
        select attempt.id, attempt.provider, attempt.mode, attempt.browser_binding_digest,
          attempt.secret_ciphertext, attempt.secret_key_version, attempt.person_id,
          attempt.initiating_session_id, attempt.person_control_epoch, attempt.return_path,
          attempt.expires_at, attempt.consumed_at,
          person.control_epoch as current_person_control_epoch, person.status as person_status,
          session.person_id as session_person_id,
          session.person_control_epoch as session_person_control_epoch,
          session.revoked_at as session_revoked_at,
          session.idle_expires_at as session_idle_expires_at,
          session.absolute_expires_at as session_absolute_expires_at,
          session.authentication_identity_id as session_authentication_identity_id,
          session.authentication_identity_authority_version,
          identity.authority_version as current_session_identity_authority_version,
          identity.person_id as session_identity_person_id,
          identity.status as session_identity_status
        from web_auth_attempts attempt
        left join people person on person.id = attempt.person_id
        left join person_sessions session on session.id = attempt.initiating_session_id
        left join person_identities identity on identity.id = session.authentication_identity_id
        where attempt.provider = 'google' and attempt.state_digest = ${stateDigest}
      `;
  const row = rows[0];
  if (!row) throw new NotFoundError("Google sign-in attempt is invalid");
  return row;
}

function assertUsableGoogleAuthAttempt(row: GoogleAuthAttemptRow, browserBinding: string, now: Date): void {
  if (!secureDigestEquals(row.browser_binding_digest, sha256Hex(browserBinding))) {
    throw new UnauthorizedError("Google sign-in must finish in the browser that started it");
  }
  if (row.consumed_at) throw new ConflictError("Google sign-in attempt was already used");
  if (row.expires_at <= now) throw new UnauthorizedError("Google sign-in attempt has expired");
  if (row.mode === "login") return;

  const expectedControlEpoch = Number(row.person_control_epoch);
  if (
    !row.person_id ||
    !row.initiating_session_id ||
    !Number.isSafeInteger(expectedControlEpoch) ||
    row.person_status !== "registered" ||
    Number(row.current_person_control_epoch) !== expectedControlEpoch ||
    row.session_person_id !== row.person_id ||
    Number(row.session_person_control_epoch) !== expectedControlEpoch ||
    row.session_revoked_at !== null ||
    row.session_idle_expires_at === null ||
    row.session_idle_expires_at <= now ||
    row.session_absolute_expires_at === null ||
    row.session_absolute_expires_at <= now ||
    !row.session_authentication_identity_id ||
    row.session_identity_person_id !== row.person_id ||
    row.session_identity_status !== "verified" ||
    Number(row.session_authentication_identity_authority_version) !==
      Number(row.current_session_identity_authority_version)
  ) {
    throw new UnauthorizedError("Google account link authority is no longer current");
  }
}

function decryptGoogleAuthSecret(
  row: GoogleAuthAttemptRow,
  secretBox: SecretBox,
): { readonly pkceVerifier: string; readonly nonce: string } {
  const encrypted: unknown = JSON.parse(row.secret_ciphertext.toString("utf8"));
  const plaintext: unknown = JSON.parse(
    secretBox.decrypt(encrypted, googleAuthAttemptSecretPurpose(row.id)).toString("utf8"),
  );
  if (!plaintext || typeof plaintext !== "object" || Array.isArray(plaintext)) {
    throw new UnauthorizedError("Google sign-in secrets are invalid");
  }
  const pkceVerifier = Reflect.get(plaintext, "pkceVerifier");
  const nonce = Reflect.get(plaintext, "nonce");
  if (
    typeof pkceVerifier !== "string" ||
    !/^[A-Za-z0-9_-]{43,128}$/u.test(pkceVerifier) ||
    typeof nonce !== "string" ||
    !/^[A-Za-z0-9_-]{32,128}$/u.test(nonce)
  ) {
    throw new UnauthorizedError("Google sign-in secrets are invalid");
  }
  return { pkceVerifier, nonce };
}

async function createBaseSession(
  transaction: Transaction,
  tokenKey: string,
  input: {
    readonly personId: string;
    readonly personControlEpoch: number;
    readonly identityId: string;
    readonly identityAuthorityVersion: number;
  },
  now: Date,
): Promise<AuthenticatedSession> {
  const sessionId = randomUUID();
  const sessionToken = randomOpaqueToken(32);
  const sessionDigest = sha256Hex(sessionToken);
  const idleExpiresAt = new Date(now.getTime() + 30 * 24 * 60 * 60 * 1000);
  const absoluteExpiresAt = new Date(now.getTime() + 90 * 24 * 60 * 60 * 1000);
  await transaction`
    insert into person_sessions (
      id, person_id, session_digest, person_control_epoch,
      authentication_identity_id, authentication_identity_authority_version,
      assurance_kind, assurance_context, assurance_expires_at,
      idle_expires_at, absolute_expires_at, last_seen_at, created_at
    ) values (
      ${sessionId}, ${input.personId}, ${sessionDigest}, ${input.personControlEpoch},
      ${input.identityId}, ${input.identityAuthorityVersion},
      'base', ${transaction.json({})}, null,
      ${idleExpiresAt}, ${absoluteExpiresAt}, ${now}, ${now}
    )
  `;
  return {
    sessionId,
    personId: input.personId,
    authenticationIdentityId: input.identityId,
    sessionToken,
    csrfToken: csrfForSession(tokenKey, sessionDigest),
    idleExpiresAt,
    absoluteExpiresAt,
    assuranceKind: "base",
    assuranceContext: {},
    assuranceExpiresAt: null,
  };
}

export function googleAuthAttemptSecretPurpose(attemptId: string): string {
  return `web-google-auth-attempt:${attemptId}`;
}

function decryptContext(row: HandoffRow, secretBox: SecretBox): Record<string, string> {
  if (!row.context_ciphertext || !row.context_key_version) return {};
  const encrypted: unknown = JSON.parse(row.context_ciphertext.toString("utf8"));
  return JSON.parse(secretBox.decrypt(encrypted, "auth-handoff-context").toString("utf8")) as Record<
    string,
    string
  >;
}

function webSignInAssuranceContext(context: Readonly<Record<string, string>>): Record<string, string> {
  const returnPath = context.returnPath;
  return returnPath === "/onboarding" || returnPath === "/people" || returnPath === "/sources"
    ? { returnPath }
    : {};
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
