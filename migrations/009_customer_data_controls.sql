ALTER TABLE households
  ADD COLUMN control_epoch bigint NOT NULL DEFAULT 0 CHECK (control_epoch >= 0);

CREATE TABLE customer_export_handoffs (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  adult_id uuid NOT NULL REFERENCES adults(id) ON DELETE CASCADE,
  private_channel_binding_id uuid NOT NULL REFERENCES channel_bindings(id) ON DELETE CASCADE,
  idempotency_key text NOT NULL,
  token_digest text NOT NULL UNIQUE,
  status text NOT NULL CHECK (status IN ('issued', 'consumed', 'expired', 'cancelled')),
  issued_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz,
  CHECK (expires_at > issued_at),
  CHECK ((status = 'consumed') = (consumed_at IS NOT NULL)),
  UNIQUE (household_id, adult_id, idempotency_key)
);

CREATE INDEX customer_export_handoffs_expiry_idx
  ON customer_export_handoffs (expires_at)
  WHERE status = 'issued';

CREATE TABLE customer_deletion_requests (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  requested_by_adult_id uuid NOT NULL REFERENCES adults(id),
  initiating_channel_binding_id uuid NOT NULL REFERENCES channel_bindings(id),
  request_code_digest text NOT NULL,
  status text NOT NULL CHECK (
    status IN ('awaiting_confirmations', 'fenced', 'cleaning', 'blocked', 'completed', 'cancelled', 'expired')
  ),
  requested_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  fenced_at timestamptz,
  control_epoch bigint CHECK (control_epoch IS NULL OR control_epoch > 0),
  cancelled_at timestamptz,
  completed_at timestamptz,
  safe_error_code text,
  report jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (expires_at > requested_at),
  CHECK ((status IN ('fenced', 'cleaning', 'blocked', 'completed')) = (fenced_at IS NOT NULL)),
  CHECK ((status IN ('fenced', 'cleaning', 'blocked', 'completed')) = (control_epoch IS NOT NULL)),
  CHECK ((status = 'cancelled') = (cancelled_at IS NOT NULL)),
  CHECK ((status = 'completed') = (completed_at IS NOT NULL))
);

CREATE UNIQUE INDEX customer_deletion_requests_open_household_idx
  ON customer_deletion_requests (household_id)
  WHERE status IN ('awaiting_confirmations', 'fenced', 'cleaning', 'blocked');

CREATE INDEX customer_deletion_requests_cleanup_idx
  ON customer_deletion_requests (status, updated_at)
  WHERE status IN ('fenced', 'cleaning', 'blocked');

CREATE TABLE customer_deletion_confirmations (
  request_id uuid NOT NULL REFERENCES customer_deletion_requests(id) ON DELETE CASCADE,
  adult_id uuid NOT NULL REFERENCES adults(id),
  private_channel_binding_id uuid NOT NULL REFERENCES channel_bindings(id),
  challenge_digest text NOT NULL UNIQUE,
  confirmed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (request_id, adult_id),
  UNIQUE (request_id, private_channel_binding_id)
);

CREATE TABLE customer_deletion_cleanup_steps (
  id uuid PRIMARY KEY,
  request_id uuid NOT NULL REFERENCES customer_deletion_requests(id) ON DELETE CASCADE,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  connection_id uuid REFERENCES external_connections(id) ON DELETE CASCADE,
  calendar_channel_id text REFERENCES google_calendar_channels(channel_id) ON DELETE CASCADE,
  step_key text NOT NULL,
  step_order integer NOT NULL CHECK (step_order > 0),
  kind text NOT NULL CHECK (
    kind IN ('google.gmail_watch.stop', 'google.calendar_watch.stop', 'google.oauth.revoke', 'local.finalize')
  ),
  status text NOT NULL CHECK (status IN ('pending', 'leased', 'retry', 'succeeded')),
  attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  available_at timestamptz NOT NULL DEFAULT now(),
  lease_owner text,
  lease_token uuid,
  lease_expires_at timestamptz,
  safe_error_code text,
  completed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (request_id, step_key),
  CHECK (
    (status = 'leased' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
    OR
    (status <> 'leased' AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
  ),
  CHECK ((status = 'succeeded') = (completed_at IS NOT NULL)),
  CHECK ((kind = 'google.calendar_watch.stop') = (calendar_channel_id IS NOT NULL)),
  CHECK ((kind LIKE 'google.%') = (connection_id IS NOT NULL))
);

CREATE INDEX customer_deletion_cleanup_steps_claim_idx
  ON customer_deletion_cleanup_steps (available_at, step_order, created_at)
  WHERE status IN ('pending', 'retry', 'leased');

CREATE TABLE customer_deletion_tombstones (
  request_id uuid PRIMARY KEY,
  household_digest text NOT NULL UNIQUE,
  routing_digests text[] NOT NULL,
  completed_at timestamptz NOT NULL,
  report jsonb NOT NULL,
  CHECK (cardinality(routing_digests) > 0)
);

CREATE INDEX customer_deletion_tombstones_routing_idx
  ON customer_deletion_tombstones USING gin (routing_digests);

ALTER TABLE jobs
  ADD COLUMN control_epoch bigint NOT NULL DEFAULT 0 CHECK (control_epoch >= 0);

ALTER TABLE scheduled_triggers
  ADD COLUMN control_epoch bigint NOT NULL DEFAULT 0 CHECK (control_epoch >= 0);

ALTER TABLE outbox
  ADD COLUMN control_epoch bigint NOT NULL DEFAULT 0 CHECK (control_epoch >= 0);
