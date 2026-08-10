-- Google sign-in is an authentication concern, separate from granting Florence
-- access to Gmail or Calendar. Provider subjects are canonical login identities;
-- mutable email addresses are encrypted display labels only.

ALTER TABLE person_identities
  DROP CONSTRAINT person_identities_kind_check;

ALTER TABLE person_identities
  ADD CONSTRAINT person_identities_kind_check CHECK (
    kind IN ('phone', 'email', 'provider_handle', 'provider_account')
  ),
  ADD COLUMN display_label_ciphertext bytea,
  ADD COLUMN display_label_key_version text,
  ADD CONSTRAINT person_identities_display_label_key_pair CHECK (
    (display_label_ciphertext IS NULL) = (display_label_key_version IS NULL)
  );

-- Browser sessions and OAuth attempts are ephemeral. Retiring them avoids a
-- compatibility path in which a session has no auditable authenticating factor.
DELETE FROM oauth_attempts;
DELETE FROM person_sessions;

ALTER TABLE oauth_attempts
  RENAME COLUMN pkce_verifier_ciphertext TO secret_ciphertext;

ALTER TABLE oauth_attempts
  RENAME COLUMN key_version TO secret_key_version;

ALTER TABLE oauth_attempts
  ADD COLUMN nonce_digest text NOT NULL CHECK (nonce_digest ~ '^[a-f0-9]{64}$'),
  ADD COLUMN target_integration_id uuid REFERENCES integrations(id) ON DELETE CASCADE,
  ADD COLUMN target_integration_control_epoch bigint CHECK (target_integration_control_epoch > 0),
  ADD COLUMN target_external_subject_digest text CHECK (
    target_external_subject_digest IS NULL
    OR target_external_subject_digest ~ '^[a-f0-9]{64}$'
  ),
  ADD CONSTRAINT oauth_attempts_reconnect_target_complete CHECK (
    (target_integration_id IS NULL)::integer
      + (target_integration_control_epoch IS NULL)::integer
      + (target_external_subject_digest IS NULL)::integer
    IN (0, 3)
  );

DROP INDEX integrations_one_active_google_subject_person_idx;

CREATE UNIQUE INDEX integrations_one_live_google_subject_idx
  ON integrations (provider, external_subject_digest)
  WHERE provider = 'google' AND status <> 'revoked';

ALTER TABLE person_sessions
  ADD COLUMN authentication_identity_id uuid NOT NULL REFERENCES person_identities(id),
  ADD COLUMN authentication_identity_authority_version bigint NOT NULL CHECK (
    authentication_identity_authority_version > 0
  );

CREATE INDEX person_sessions_authentication_identity_idx
  ON person_sessions (authentication_identity_id, last_seen_at DESC)
  WHERE revoked_at IS NULL;

-- The raw state and browser-binding values are never stored. PKCE and nonce
-- secrets are encrypted together and are available only for the short callback
-- window. A link attempt is fenced to one exact existing Florence session.
CREATE TABLE web_auth_attempts (
  id uuid PRIMARY KEY,
  provider text NOT NULL CHECK (provider = 'google'),
  mode text NOT NULL CHECK (mode IN ('login', 'link')),
  state_digest text NOT NULL UNIQUE CHECK (state_digest ~ '^[a-f0-9]{64}$'),
  browser_binding_digest text NOT NULL CHECK (browser_binding_digest ~ '^[a-f0-9]{64}$'),
  secret_ciphertext bytea NOT NULL,
  secret_key_version text NOT NULL,
  person_id uuid REFERENCES people(id) ON DELETE CASCADE,
  initiating_session_id uuid REFERENCES person_sessions(id) ON DELETE CASCADE,
  person_control_epoch bigint CHECK (person_control_epoch > 0),
  return_path text NOT NULL CHECK (
    return_path LIKE '/%' AND return_path NOT LIKE '//%'
  ),
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (expires_at > created_at),
  CHECK (consumed_at IS NULL OR consumed_at >= created_at),
  CHECK (
    (mode = 'login' AND person_id IS NULL AND initiating_session_id IS NULL
      AND person_control_epoch IS NULL)
    OR
    (mode = 'link' AND person_id IS NOT NULL AND initiating_session_id IS NOT NULL
      AND person_control_epoch IS NOT NULL)
  )
);

CREATE INDEX web_auth_attempts_expiry_idx
  ON web_auth_attempts (expires_at)
  WHERE consumed_at IS NULL;
