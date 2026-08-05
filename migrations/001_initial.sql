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
  display_name text NOT NULL,
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
  label text NOT NULL,
  external_account_id text,
  email text,
  encrypted_credentials text NOT NULL,
  granted_scopes text[] NOT NULL DEFAULT '{}',
  status text NOT NULL CHECK (status IN ('active', 'reauth_required', 'revoked', 'error')),
  cursor jsonb NOT NULL DEFAULT '{}'::jsonb,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  last_synced_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (adult_id, provider, external_account_id)
);

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
  subject text,
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
  kind text NOT NULL,
  status text NOT NULL CHECK (status IN ('pending', 'leased', 'succeeded', 'retry', 'dead', 'cancelled')),
  payload jsonb NOT NULL,
  attempt integer NOT NULL DEFAULT 0,
  max_attempts integer NOT NULL DEFAULT 8,
  available_at timestamptz NOT NULL DEFAULT now(),
  lease_owner text,
  lease_token uuid,
  lease_expires_at timestamptz,
  last_error_code text,
  last_error_detail text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX jobs_claim_idx ON jobs (status, available_at, created_at) WHERE status IN ('pending', 'retry', 'leased');
CREATE INDEX jobs_household_idx ON jobs (household_id, status);

CREATE TABLE family_episodes (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  episode_type text NOT NULL CHECK (episode_type IN ('commitment', 'research', 'meal_plan', 'household_project')),
  visibility text NOT NULL CHECK (visibility IN ('personal', 'household')),
  owner_adult_id uuid REFERENCES adults(id),
  status text NOT NULL CHECK (status IN ('proposed', 'awaiting_owner', 'open', 'blocked', 'handled', 'dismissed', 'superseded')),
  title text NOT NULL,
  accepted_meaning jsonb NOT NULL,
  source_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  authority jsonb NOT NULL,
  temporal_plan jsonb,
  outcome jsonb,
  version bigint NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  closed_at timestamptz
);

CREATE INDEX family_episodes_open_idx ON family_episodes (household_id, status, updated_at DESC);

CREATE TABLE episode_events (
  id uuid PRIMARY KEY,
  episode_id uuid NOT NULL REFERENCES family_episodes(id) ON DELETE CASCADE,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  event_type text NOT NULL,
  actor_kind text NOT NULL,
  actor_id text,
  evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE approvals (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  episode_id uuid REFERENCES family_episodes(id) ON DELETE CASCADE,
  requested_from_adult_id uuid NOT NULL REFERENCES adults(id),
  action_kind text NOT NULL,
  action_digest text NOT NULL,
  preview jsonb NOT NULL,
  status text NOT NULL CHECK (status IN ('pending', 'approved', 'rejected', 'expired', 'revoked', 'consumed')),
  expires_at timestamptz NOT NULL,
  decided_at timestamptz,
  consumed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX approvals_pending_idx ON approvals (household_id, requested_from_adult_id, status, expires_at);

CREATE TABLE household_policies (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  owner_adult_id uuid REFERENCES adults(id) ON DELETE CASCADE,
  policy_type text NOT NULL,
  scope text NOT NULL CHECK (scope IN ('personal', 'household')),
  conditions jsonb NOT NULL,
  effect jsonb NOT NULL,
  sensitivity text NOT NULL,
  status text NOT NULL CHECK (status IN ('candidate', 'active', 'revoked', 'expired')),
  approved_by_adult_id uuid REFERENCES adults(id),
  approved_at timestamptz,
  valid_until timestamptz,
  provenance jsonb NOT NULL,
  version integer NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE household_memories (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  owner_adult_id uuid REFERENCES adults(id) ON DELETE CASCADE,
  scope text NOT NULL CHECK (scope IN ('personal', 'household')),
  memory_type text NOT NULL,
  encrypted_value text NOT NULL,
  confidence numeric(4,3) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  sensitivity text NOT NULL,
  status text NOT NULL CHECK (status IN ('candidate', 'confirmed', 'revoked', 'expired')),
  valid_from timestamptz,
  valid_until timestamptz,
  provenance jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE scheduled_triggers (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  episode_id uuid REFERENCES family_episodes(id) ON DELETE CASCADE,
  trigger_kind text NOT NULL,
  plan_version bigint NOT NULL,
  due_at timestamptz NOT NULL,
  status text NOT NULL CHECK (status IN ('scheduled', 'claimed', 'fired', 'cancelled', 'superseded')),
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX scheduled_triggers_due_idx ON scheduled_triggers (status, due_at) WHERE status = 'scheduled';

CREATE TABLE worker_attempts (
  id uuid PRIMARY KEY,
  job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  base_household_version bigint NOT NULL,
  worker_release_id text NOT NULL,
  model_route_id text NOT NULL,
  status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled', 'expired')),
  grant_digest text NOT NULL,
  result jsonb,
  error_code text,
  usage jsonb,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);

CREATE TABLE outbox (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  effect_kind text NOT NULL,
  idempotency_key text NOT NULL UNIQUE,
  payload jsonb NOT NULL,
  status text NOT NULL CHECK (status IN ('pending', 'leased', 'sent', 'retry', 'ambiguous', 'dead', 'cancelled')),
  attempt integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  lease_owner text,
  lease_token uuid,
  lease_expires_at timestamptz,
  provider_receipt jsonb,
  last_error_code text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX outbox_claim_idx ON outbox (status, available_at, created_at) WHERE status IN ('pending', 'retry', 'leased');

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

CREATE TABLE deletion_requests (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  requested_by_adult_id uuid NOT NULL REFERENCES adults(id),
  status text NOT NULL CHECK (status IN ('pending', 'confirmed', 'running', 'completed', 'cancelled')),
  confirmation_digest text NOT NULL,
  requested_at timestamptz NOT NULL DEFAULT now(),
  confirmed_at timestamptz,
  completed_at timestamptz,
  report jsonb
);
