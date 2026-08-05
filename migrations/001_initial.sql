CREATE TABLE IF NOT EXISTS schema_migrations (
  version text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE households (
  id uuid PRIMARY KEY,
  name text NOT NULL,
  timezone text NOT NULL,
  status text NOT NULL CHECK (status IN ('onboarding', 'learning', 'active', 'paused', 'deleting')),
  version bigint NOT NULL DEFAULT 0,
  next_signal_sequence bigint NOT NULL DEFAULT 1,
  next_audit_sequence bigint NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE adults (
  id uuid PRIMARY KEY,
  timezone text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE household_memberships (
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  adult_id uuid NOT NULL REFERENCES adults(id) ON DELETE CASCADE,
  role text NOT NULL CHECK (role IN ('owner', 'adult')),
  status text NOT NULL CHECK (status IN ('invited', 'active', 'revoked')),
  consented_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (household_id, adult_id)
);

CREATE TABLE adult_identity_details (
  household_id uuid NOT NULL,
  adult_id uuid NOT NULL,
  details_key_id text NOT NULL,
  details_ciphertext text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (household_id, adult_id),
  FOREIGN KEY (household_id, adult_id)
    REFERENCES household_memberships (household_id, adult_id) ON DELETE CASCADE
);

CREATE INDEX adult_identity_details_key_idx ON adult_identity_details (details_key_id);

CREATE TABLE invitations (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  invited_by_adult_id uuid NOT NULL REFERENCES adults(id),
  invitee_handle_hash text NOT NULL,
  status text NOT NULL CHECK (status IN ('pending', 'accepted', 'expired', 'revoked')),
  expires_at timestamptz NOT NULL,
  accepted_by_adult_id uuid REFERENCES adults(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE channel_bindings (
  id uuid PRIMARY KEY,
  household_id uuid REFERENCES households(id) ON DELETE CASCADE,
  adult_id uuid REFERENCES adults(id) ON DELETE CASCADE,
  provider text NOT NULL CHECK (provider IN ('linq')),
  channel_type text NOT NULL CHECK (channel_type IN ('private', 'group')),
  external_chat_id text NOT NULL,
  external_handle text,
  status text NOT NULL CHECK (status IN ('pending', 'active', 'paused', 'revoked')),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (provider, external_chat_id, external_handle)
);

CREATE INDEX channel_bindings_household_idx ON channel_bindings (household_id, status);

CREATE TABLE external_connections (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  adult_id uuid NOT NULL REFERENCES adults(id) ON DELETE CASCADE,
  provider text NOT NULL CHECK (provider IN ('google')),
  external_account_id text,
  email_digest text,
  details_key_id text,
  details_ciphertext text,
  encrypted_credentials text NOT NULL,
  granted_scopes text[] NOT NULL DEFAULT '{}',
  status text NOT NULL CHECK (status IN ('active', 'reauth_required', 'revoked', 'error')),
  cursor jsonb NOT NULL DEFAULT '{}'::jsonb,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  last_synced_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (adult_id, provider, external_account_id),
  CONSTRAINT external_connections_details_state_check CHECK (
    (status = 'revoked' AND details_key_id IS NULL AND details_ciphertext IS NULL AND email_digest IS NULL)
    OR
    (status <> 'revoked' AND details_key_id IS NOT NULL AND details_ciphertext IS NOT NULL)
  ),
  CONSTRAINT external_connections_metadata_account_label_check CHECK (NOT (metadata ? 'accountLabel'))
);

CREATE INDEX external_connections_details_key_idx
  ON external_connections (details_key_id) WHERE details_key_id IS NOT NULL;

CREATE TABLE oauth_states (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  adult_id uuid NOT NULL REFERENCES adults(id) ON DELETE CASCADE,
  provider text NOT NULL,
  state_hash text NOT NULL UNIQUE,
  return_conversation_id text NOT NULL,
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE source_items (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  owner_adult_id uuid REFERENCES adults(id) ON DELETE CASCADE,
  visibility text NOT NULL CHECK (visibility IN ('personal', 'household')),
  provider text NOT NULL,
  external_id text NOT NULL,
  kind text NOT NULL,
  occurred_at timestamptz NOT NULL,
  content_hash text NOT NULL,
  encrypted_content text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  retention_until timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (provider, external_id)
);

CREATE INDEX source_items_scope_idx ON source_items (household_id, owner_adult_id, visibility, occurred_at DESC);

CREATE TABLE household_signals (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  sequence bigint NOT NULL,
  idempotency_digest text NOT NULL UNIQUE,
  content_digest text NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  body_key_id text NOT NULL,
  body_ciphertext text NOT NULL,
  processing_status text NOT NULL DEFAULT 'pending' CHECK (processing_status IN ('pending', 'processing', 'processed', 'quarantined')),
  UNIQUE (household_id, sequence)
);

CREATE INDEX household_signals_pending_idx ON household_signals (household_id, processing_status, sequence);
CREATE INDEX household_signals_body_key_idx ON household_signals (body_key_id);

CREATE TABLE jobs (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  signal_id uuid REFERENCES household_signals(id) ON DELETE CASCADE,
  connection_id uuid REFERENCES external_connections(id) ON DELETE CASCADE,
  kind text NOT NULL,
  work_kind text,
  status text NOT NULL CHECK (status IN ('pending', 'leased', 'succeeded', 'retry', 'dead', 'cancelled')),
  payload_digest text NOT NULL CHECK (payload_digest ~ '^[a-f0-9]{64}$'),
  payload_key_id text,
  payload_ciphertext text,
  attempt integer NOT NULL DEFAULT 0,
  max_attempts integer NOT NULL DEFAULT 8,
  available_at timestamptz NOT NULL DEFAULT now(),
  lease_owner text,
  lease_token uuid,
  lease_expires_at timestamptz,
  last_error_code text,
  last_error_detail text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT jobs_payload_state_check CHECK (
    (status IN ('pending', 'leased', 'retry') AND payload_key_id IS NOT NULL AND payload_ciphertext IS NOT NULL)
    OR
    (status IN ('succeeded', 'dead', 'cancelled') AND payload_key_id IS NULL AND payload_ciphertext IS NULL)
  )
);

CREATE INDEX jobs_claim_idx ON jobs (status, available_at, created_at) WHERE status IN ('pending', 'retry', 'leased');
CREATE INDEX jobs_household_idx ON jobs (household_id, status);
CREATE INDEX jobs_connection_idx ON jobs (connection_id, kind, work_kind, status) WHERE connection_id IS NOT NULL;
CREATE INDEX jobs_payload_key_idx ON jobs (payload_key_id) WHERE payload_key_id IS NOT NULL;

CREATE TABLE scheduled_triggers (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  trigger_kind text NOT NULL,
  plan_version bigint NOT NULL,
  due_at timestamptz,
  status text NOT NULL CHECK (status IN ('scheduled', 'claimed', 'fired', 'cancelled', 'superseded', 'dead')),
  payload_digest text NOT NULL CHECK (payload_digest ~ '^[a-f0-9]{64}$'),
  payload_key_id text,
  payload_ciphertext text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT scheduled_triggers_payload_state_check CHECK (
    (status IN ('scheduled', 'claimed') AND payload_key_id IS NOT NULL AND payload_ciphertext IS NOT NULL)
    OR
    (status IN ('fired', 'cancelled', 'superseded', 'dead') AND payload_key_id IS NULL AND payload_ciphertext IS NULL)
  ),
  CONSTRAINT scheduled_triggers_due_state_check CHECK (
    (status IN ('scheduled', 'claimed') AND due_at IS NOT NULL)
    OR
    (status IN ('fired', 'cancelled', 'superseded', 'dead') AND due_at IS NULL)
  )
);

CREATE INDEX scheduled_triggers_due_idx ON scheduled_triggers (status, due_at) WHERE status = 'scheduled';
CREATE INDEX scheduled_triggers_payload_key_idx
  ON scheduled_triggers (payload_key_id) WHERE payload_key_id IS NOT NULL;

CREATE TABLE outbox (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  effect_kind text NOT NULL,
  idempotency_key text NOT NULL UNIQUE,
  payload_digest text NOT NULL CHECK (payload_digest ~ '^[a-f0-9]{64}$'),
  payload_key_id text,
  payload_ciphertext text,
  status text NOT NULL CHECK (status IN ('pending', 'leased', 'sent', 'retry', 'ambiguous', 'dead', 'cancelled')),
  attempt integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  lease_owner text,
  lease_token uuid,
  lease_expires_at timestamptz,
  provider_receipt jsonb,
  last_error_code text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT outbox_payload_state_check CHECK (
    (status IN ('pending', 'leased', 'retry', 'ambiguous') AND payload_key_id IS NOT NULL AND payload_ciphertext IS NOT NULL)
    OR
    (status IN ('sent', 'dead', 'cancelled') AND payload_key_id IS NULL AND payload_ciphertext IS NULL)
  )
);

CREATE INDEX outbox_claim_idx ON outbox (status, available_at, created_at) WHERE status IN ('pending', 'retry', 'leased');
CREATE INDEX outbox_payload_key_idx ON outbox (payload_key_id) WHERE payload_key_id IS NOT NULL;

CREATE TABLE audit_log (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  sequence bigint NOT NULL,
  actor_kind text NOT NULL,
  actor_id text,
  action text NOT NULL,
  target_type text NOT NULL,
  target_id text,
  source_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  policy_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  details jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (household_id, sequence)
);
