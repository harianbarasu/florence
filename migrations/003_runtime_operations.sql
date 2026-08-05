ALTER TABLE provider_inbox
  ADD COLUMN retention_until timestamptz NOT NULL DEFAULT (now() + interval '7 days');

CREATE INDEX provider_inbox_retention_idx
  ON provider_inbox (retention_until)
  WHERE status IN ('resolved', 'quarantined', 'dead');

CREATE UNIQUE INDEX invitations_pending_handle_idx
  ON invitations (invitee_handle_hash)
  WHERE status = 'pending';

ALTER TABLE invitations
  ADD COLUMN invitee_adult_id uuid REFERENCES adults(id) ON DELETE CASCADE;

CREATE TABLE channel_suppressions (
  id uuid PRIMARY KEY,
  provider text NOT NULL CHECK (provider IN ('linq')),
  external_chat_id text NOT NULL,
  external_handle_hash text,
  scope text NOT NULL CHECK (scope IN ('private', 'group')),
  status text NOT NULL CHECK (status IN ('suppressed', 'released')),
  reason text NOT NULL,
  changed_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE NULLS NOT DISTINCT (provider, external_chat_id, external_handle_hash),
  CHECK (
    (scope = 'private' AND external_handle_hash IS NOT NULL)
    OR (scope = 'group' AND external_handle_hash IS NULL)
  )
);

CREATE INDEX external_connections_active_email_idx
  ON external_connections (provider, lower(email))
  WHERE status = 'active' AND email IS NOT NULL;

CREATE UNIQUE INDEX channel_bindings_one_household_group_idx
  ON channel_bindings (household_id)
  WHERE provider = 'linq' AND channel_type = 'group' AND status IN ('pending', 'active', 'paused');

CREATE INDEX jobs_runtime_claim_idx
  ON jobs (kind, status, available_at, created_at)
  WHERE status IN ('pending', 'retry', 'leased');

ALTER TABLE jobs
  ADD COLUMN idempotency_key text;

CREATE UNIQUE INDEX jobs_idempotency_key_idx
  ON jobs (idempotency_key)
  WHERE idempotency_key IS NOT NULL;
