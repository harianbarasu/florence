CREATE TABLE provider_inbox (
  id uuid PRIMARY KEY,
  provider text NOT NULL,
  idempotency_digest text NOT NULL,
  content_digest text NOT NULL,
  status text NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'leased', 'resolved', 'quarantined', 'dead')),
  household_id uuid REFERENCES households(id) ON DELETE SET NULL,
  routing_digests text[] NOT NULL CHECK (cardinality(routing_digests) > 0),
  encryption_tenant_kind text NOT NULL
    CHECK (encryption_tenant_kind IN ('household', 'provider_ingress')),
  encryption_tenant_id text NOT NULL,
  body_key_id text,
  body_ciphertext text,
  available_at timestamptz NOT NULL DEFAULT now(),
  attempt integer NOT NULL DEFAULT 0,
  max_attempts integer NOT NULL DEFAULT 8 CHECK (max_attempts > 0),
  lease_owner text,
  lease_token uuid,
  lease_expires_at timestamptz,
  quarantine_reason text,
  last_error_code text,
  last_error_detail text,
  received_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (provider, idempotency_digest),
  CONSTRAINT provider_inbox_body_state_check CHECK (
    (status IN ('pending', 'leased') AND body_key_id IS NOT NULL AND body_ciphertext IS NOT NULL)
    OR
    (status IN ('resolved', 'quarantined', 'dead') AND body_key_id IS NULL AND body_ciphertext IS NULL)
  )
);

CREATE INDEX provider_inbox_claim_idx
  ON provider_inbox (available_at, received_at)
  WHERE status IN ('pending', 'leased');

CREATE INDEX provider_inbox_household_idx
  ON provider_inbox (household_id, received_at DESC)
  WHERE household_id IS NOT NULL;

CREATE INDEX provider_inbox_routing_idx
  ON provider_inbox USING gin (routing_digests);

CREATE INDEX provider_inbox_body_key_idx
  ON provider_inbox (body_key_id) WHERE body_key_id IS NOT NULL;

CREATE TABLE provider_inbox_conflicts (
  id uuid PRIMARY KEY,
  inbox_id uuid NOT NULL REFERENCES provider_inbox(id) ON DELETE CASCADE,
  content_digest text NOT NULL,
  encryption_tenant_kind text NOT NULL
    CHECK (encryption_tenant_kind IN ('household', 'provider_ingress')),
  encryption_tenant_id text NOT NULL,
  body_key_id text,
  body_ciphertext text,
  received_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (inbox_id, content_digest)
);

CREATE INDEX provider_inbox_conflicts_body_key_idx
  ON provider_inbox_conflicts (body_key_id) WHERE body_key_id IS NOT NULL;

CREATE TABLE household_projections (
  household_id uuid PRIMARY KEY REFERENCES households(id) ON DELETE CASCADE,
  schema_version integer NOT NULL CHECK (schema_version > 0),
  version bigint NOT NULL CHECK (version >= 0),
  state_key_id text NOT NULL,
  state_ciphertext text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX household_projections_state_key_idx
  ON household_projections (state_key_id);

CREATE TABLE application_snapshots (
  household_id uuid PRIMARY KEY REFERENCES households(id) ON DELETE CASCADE,
  schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version > 0),
  revision bigint NOT NULL CHECK (revision >= 0),
  application_phase text NOT NULL CHECK (
    application_phase IN (
      'awaiting_initiator_consent', 'awaiting_invitation', 'awaiting_invitee_consent',
      'awaiting_group', 'naming_adults', 'building_profile', 'connecting_sources', 'active'
    )
  ),
  snapshot_key_id text NOT NULL,
  snapshot_ciphertext text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX application_snapshots_active_idx
  ON application_snapshots (household_id)
  WHERE application_phase = 'active';

CREATE INDEX application_snapshots_snapshot_key_idx
  ON application_snapshots (snapshot_key_id);

CREATE TABLE application_commits (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  idempotency_digest text NOT NULL,
  content_digest text NOT NULL,
  base_revision bigint NOT NULL CHECK (base_revision >= 0),
  revision bigint NOT NULL CHECK (revision = base_revision + 1),
  body_key_id text NOT NULL,
  body_ciphertext text NOT NULL,
  committed_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (household_id, idempotency_digest),
  UNIQUE (household_id, revision)
);

CREATE INDEX application_commits_household_idx
  ON application_commits (household_id, committed_at, revision);

CREATE INDEX application_commits_body_key_idx
  ON application_commits (body_key_id);

CREATE TABLE encryption_rotation_runs (
  id uuid PRIMARY KEY,
  target_key_id text NOT NULL,
  status text NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
  rows_rewrapped bigint NOT NULL DEFAULT 0 CHECK (rows_rewrapped >= 0),
  last_error_code text,
  started_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  CHECK ((status = 'completed') = (completed_at IS NOT NULL))
);

CREATE UNIQUE INDEX encryption_rotation_runs_active_idx
  ON encryption_rotation_runs (status)
  WHERE status = 'running';

CREATE UNIQUE INDEX channel_bindings_group_identity_idx
  ON channel_bindings (provider, external_chat_id)
  WHERE channel_type = 'group';

CREATE UNIQUE INDEX channel_bindings_private_identity_idx
  ON channel_bindings (provider, external_chat_id, external_handle)
  WHERE channel_type = 'private';

ALTER TABLE channel_bindings
  ADD CONSTRAINT channel_bindings_scope_shape_check
  CHECK (
    (channel_type = 'private' AND household_id IS NOT NULL AND adult_id IS NOT NULL AND external_handle IS NOT NULL)
    OR
    (channel_type = 'group' AND household_id IS NOT NULL AND adult_id IS NULL AND external_handle IS NULL)
  );

ALTER TABLE external_connections
  ALTER COLUMN encrypted_credentials DROP NOT NULL;

ALTER TABLE oauth_states
  ADD COLUMN encrypted_payload text;

ALTER TABLE external_connections
  DROP CONSTRAINT IF EXISTS external_connections_adult_id_provider_external_account_id_key;

CREATE UNIQUE INDEX external_connections_household_account_idx
  ON external_connections (household_id, adult_id, provider, external_account_id);

ALTER TABLE source_items
  ADD COLUMN connection_id uuid REFERENCES external_connections(id) ON DELETE CASCADE,
  ADD COLUMN revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0);

ALTER TABLE source_items
  DROP CONSTRAINT IF EXISTS source_items_provider_external_id_key;

CREATE UNIQUE INDEX source_items_connected_identity_idx
  ON source_items (household_id, provider, connection_id, external_id)
  WHERE connection_id IS NOT NULL;

CREATE UNIQUE INDEX source_items_unconnected_identity_idx
  ON source_items (household_id, provider, external_id)
  WHERE connection_id IS NULL;

ALTER TABLE source_items
  ADD CONSTRAINT source_items_visibility_owner_check
  CHECK ((visibility = 'personal' AND owner_adult_id IS NOT NULL) OR visibility = 'household');

ALTER TABLE scheduled_triggers
  ADD COLUMN timer_key text,
  ADD COLUMN episode_key text,
  ADD COLUMN available_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN attempt integer NOT NULL DEFAULT 0,
  ADD COLUMN lease_owner text,
  ADD COLUMN lease_token uuid,
  ADD COLUMN lease_expires_at timestamptz,
  ADD COLUMN last_error_code text;

DROP INDEX IF EXISTS scheduled_triggers_due_idx;

CREATE INDEX scheduled_triggers_claim_idx
  ON scheduled_triggers (due_at, available_at, created_at)
  WHERE status IN ('scheduled', 'claimed');

CREATE UNIQUE INDEX scheduled_triggers_timer_key_idx
  ON scheduled_triggers (household_id, timer_key)
  WHERE timer_key IS NOT NULL;

ALTER TABLE outbox
  ADD COLUMN intent_key text,
  ADD COLUMN max_attempts integer NOT NULL DEFAULT 8 CHECK (max_attempts > 0),
  ADD COLUMN last_error_detail text,
  ADD COLUMN sent_at timestamptz,
  ADD COLUMN dead_at timestamptz;

CREATE UNIQUE INDEX outbox_intent_key_idx
  ON outbox (household_id, intent_key)
  WHERE intent_key IS NOT NULL;

ALTER TABLE audit_log
  ADD COLUMN visibility text NOT NULL DEFAULT 'household'
    CHECK (visibility IN ('personal', 'household', 'restricted')),
  ADD COLUMN owner_adult_id uuid REFERENCES adults(id) ON DELETE CASCADE,
  ADD CONSTRAINT audit_log_visibility_owner_check
    CHECK ((visibility = 'personal' AND owner_adult_id IS NOT NULL) OR visibility IN ('household', 'restricted'));

CREATE INDEX audit_log_scope_idx
  ON audit_log (household_id, visibility, owner_adult_id, sequence);
