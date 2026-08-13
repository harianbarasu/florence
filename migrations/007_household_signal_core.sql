CREATE TABLE household_streams (
  household_id uuid PRIMARY KEY,
  version integer NOT NULL DEFAULT 0 CHECK (version >= 0),
  lease_owner text,
  lease_until timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE household_signals (
  signal_id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES household_streams(household_id) ON DELETE CASCADE,
  idempotency_key text NOT NULL UNIQUE,
  type text NOT NULL,
  payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'retry', 'processing', 'completed', 'dead')),
  attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  available_at timestamptz NOT NULL,
  lease_owner text,
  lease_until timestamptz,
  accepted_at timestamptz NOT NULL,
  completed_at timestamptz,
  last_error text,
  deliberation_input_digest text
    CHECK (deliberation_input_digest IS NULL OR deliberation_input_digest ~ '^[0-9a-f]{64}$'),
  deliberation_result jsonb,
  CHECK ((deliberation_input_digest IS NULL) = (deliberation_result IS NULL))
);

CREATE INDEX household_signals_claim_idx
  ON household_signals(status, available_at, accepted_at);
CREATE INDEX household_signals_household_order_idx
  ON household_signals(household_id, accepted_at, signal_id);

CREATE TABLE household_events (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES household_streams(household_id) ON DELETE CASCADE,
  signal_id uuid NOT NULL REFERENCES household_signals(signal_id) ON DELETE RESTRICT,
  version integer NOT NULL CHECK (version > 0),
  type text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (household_id, version)
);

CREATE INDEX household_events_signal_idx ON household_events(signal_id);

CREATE TABLE outbox_effects (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES household_streams(household_id) ON DELETE CASCADE,
  signal_id uuid NOT NULL REFERENCES household_signals(signal_id) ON DELETE RESTRICT,
  idempotency_key text NOT NULL UNIQUE,
  kind text NOT NULL CHECK (kind IN ('conversation.message', 'google.calendar.create')),
  conversation_id uuid,
  conversation_authority_version integer,
  participant_set_digest text,
  episode_id uuid,
  payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'committed', 'failed')),
  provider_receipt_id text,
  receipt_detail text,
  occurred_at timestamptz NOT NULL,
  receipt_at timestamptz,
  attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  available_at timestamptz NOT NULL DEFAULT now(),
  lease_owner text,
  lease_until timestamptz,
  last_error text,
  CHECK (
    (
      kind = 'conversation.message'
      AND conversation_id IS NOT NULL
      AND conversation_authority_version > 0
      AND participant_set_digest ~ '^[0-9a-f]{64}$'
    ) OR (
      kind = 'google.calendar.create'
      AND conversation_id IS NULL
      AND conversation_authority_version IS NULL
      AND participant_set_digest IS NULL
      AND episode_id IS NULL
      AND payload ->> 'connectionId' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      AND payload ->> 'ownerAdultId' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      AND payload ->> 'actionId' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      AND payload ->> 'approvalDigest' ~ '^[0-9a-f]{64}$'
      AND payload ->> 'candidateId' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      AND (payload ->> 'candidateVersion')::integer = 1
      AND payload ->> 'candidateDigest' ~ '^[0-9a-f]{64}$'
      AND jsonb_typeof(payload -> 'calendar') = 'object'
    )
  )
);

CREATE INDEX outbox_effects_claim_idx
  ON outbox_effects(status, available_at, occurred_at);
CREATE INDEX outbox_effects_conversation_idx
  ON outbox_effects(household_id, conversation_id, occurred_at);

CREATE TABLE episode_timers (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES household_streams(household_id) ON DELETE CASCADE,
  signal_id uuid NOT NULL REFERENCES household_signals(signal_id) ON DELETE RESTRICT,
  idempotency_key text NOT NULL UNIQUE,
  episode_id uuid NOT NULL,
  episode_version integer NOT NULL CHECK (episode_version > 0),
  scheduled_for timestamptz NOT NULL,
  status text NOT NULL DEFAULT 'scheduled'
    CHECK (status IN ('scheduled', 'fired', 'cancelled')),
  fired_at timestamptz,
  cancelled_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  lease_owner text,
  lease_until timestamptz
);

CREATE INDEX episode_timers_due_idx ON episode_timers(status, scheduled_for);
CREATE INDEX episode_timers_episode_idx
  ON episode_timers(household_id, episode_id, episode_version);

CREATE TABLE google_connections (
  connection_id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES household_streams(household_id) ON DELETE CASCADE,
  owner_adult_id uuid NOT NULL,
  status text NOT NULL CHECK (status IN ('pending', 'active', 'disconnected')),
  state_digest text NOT NULL UNIQUE CHECK (state_digest ~ '^[0-9a-f]{64}$'),
  session_binding_digest text
    CHECK (session_binding_digest IS NULL OR session_binding_digest ~ '^[0-9a-f]{64}$'),
  state_expires_at timestamptz NOT NULL,
  state_consumed_at timestamptz,
  google_subject_digest text
    CHECK (google_subject_digest IS NULL OR google_subject_digest ~ '^[0-9a-f]{64}$'),
  email_label text,
  granted_scopes jsonb NOT NULL DEFAULT '[]'::jsonb,
  refresh_token_envelope text,
  gmail_cursor text,
  sync_available_at timestamptz NOT NULL DEFAULT now(),
  sync_lease_owner text,
  sync_lease_until timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (
      status = 'pending'
      AND session_binding_digest IS NOT NULL
      AND google_subject_digest IS NULL
      AND email_label IS NULL
      AND refresh_token_envelope IS NULL
    ) OR (
      status = 'active'
      AND state_consumed_at IS NOT NULL
      AND session_binding_digest IS NULL
      AND google_subject_digest IS NOT NULL
      AND email_label IS NOT NULL
      AND refresh_token_envelope IS NOT NULL
    ) OR (
      status = 'disconnected'
      AND refresh_token_envelope IS NULL
    )
  )
);

CREATE UNIQUE INDEX google_connections_active_subject_unique
  ON google_connections(google_subject_digest) WHERE status = 'active';
CREATE INDEX google_connections_owner_idx
  ON google_connections(household_id, owner_adult_id, created_at);
CREATE INDEX google_connections_sync_idx
  ON google_connections(status, sync_available_at);

CREATE TABLE image_assets (
  asset_id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES household_streams(household_id) ON DELETE CASCADE,
  signal_id uuid NOT NULL,
  expires_at timestamptz NOT NULL,
  envelope bytea NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (octet_length(envelope) > 0 AND octet_length(envelope) <= 22020096)
);

CREATE INDEX image_assets_expiry_idx ON image_assets(expires_at);
