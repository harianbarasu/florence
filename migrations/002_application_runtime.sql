CREATE TABLE provider_inbox (
  id uuid PRIMARY KEY,
  provider text NOT NULL,
  idempotency_key text NOT NULL,
  payload_hash text NOT NULL,
  authentication jsonb NOT NULL,
  event_kind text NOT NULL,
  occurred_at timestamptz NOT NULL,
  payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'leased', 'resolved', 'quarantined', 'dead')),
  household_id uuid REFERENCES households(id) ON DELETE SET NULL,
  resolution jsonb,
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
  UNIQUE (provider, idempotency_key)
);

CREATE INDEX provider_inbox_claim_idx
  ON provider_inbox (available_at, received_at)
  WHERE status IN ('pending', 'leased');

CREATE INDEX provider_inbox_household_idx
  ON provider_inbox (household_id, received_at DESC)
  WHERE household_id IS NOT NULL;

CREATE TABLE provider_inbox_conflicts (
  id uuid PRIMARY KEY,
  inbox_id uuid NOT NULL REFERENCES provider_inbox(id) ON DELETE CASCADE,
  payload_hash text NOT NULL,
  payload jsonb NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (inbox_id, payload_hash)
);

CREATE TABLE household_projections (
  household_id uuid PRIMARY KEY REFERENCES households(id) ON DELETE CASCADE,
  schema_version integer NOT NULL CHECK (schema_version > 0),
  version bigint NOT NULL CHECK (version >= 0),
  state jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE application_snapshots (
  household_id uuid PRIMARY KEY REFERENCES households(id) ON DELETE CASCADE,
  schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version > 0),
  revision bigint NOT NULL CHECK (revision >= 0),
  aggregate jsonb NOT NULL,
  projection jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE application_commits (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  idempotency_key text NOT NULL,
  commit_hash text NOT NULL,
  base_revision bigint NOT NULL CHECK (base_revision >= 0),
  revision bigint NOT NULL CHECK (revision = base_revision + 1),
  signals jsonb NOT NULL,
  changes jsonb NOT NULL,
  outcome jsonb NOT NULL,
  committed_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (household_id, idempotency_key),
  UNIQUE (household_id, revision)
);

CREATE INDEX application_commits_household_idx
  ON application_commits (household_id, committed_at, revision);

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
  ADD COLUMN payload_hash text,
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

CREATE TABLE deletion_tombstones (
  request_id uuid PRIMARY KEY,
  household_id uuid NOT NULL,
  requested_by_adult_id uuid NOT NULL,
  completed_at timestamptz NOT NULL DEFAULT now(),
  report jsonb NOT NULL
);

CREATE INDEX deletion_tombstones_household_idx
  ON deletion_tombstones (household_id, completed_at DESC);
