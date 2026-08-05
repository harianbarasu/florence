CREATE TABLE daily_brief_runs (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  local_date date NOT NULL,
  time_zone text NOT NULL,
  scheduled_for timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  idempotency_key text NOT NULL UNIQUE,
  status text NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'leased', 'retry', 'succeeded', 'dead')),
  attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  max_attempts integer NOT NULL CHECK (max_attempts > 0),
  available_at timestamptz NOT NULL,
  lease_owner text,
  lease_token uuid,
  lease_expires_at timestamptz,
  last_error_code text,
  completed_at timestamptz,
  dead_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (household_id, local_date),
  CHECK (expires_at > scheduled_for),
  CHECK (
    (status = 'leased' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
    OR
    (status <> 'leased' AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
  ),
  CHECK ((status = 'succeeded') = (completed_at IS NOT NULL)),
  CHECK ((status = 'dead') = (dead_at IS NOT NULL))
);

CREATE INDEX daily_brief_runs_claim_idx
  ON daily_brief_runs (available_at, scheduled_for, created_at)
  WHERE status IN ('pending', 'retry', 'leased');

CREATE INDEX daily_brief_runs_household_idx
  ON daily_brief_runs (household_id, local_date DESC);
