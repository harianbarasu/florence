CREATE TABLE linq_reconciliation_state (
  integration_digest text PRIMARY KEY
    CHECK (integration_digest ~ '^sha256:[a-f0-9]{64}$'),
  status text NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'leased')),
  available_at timestamptz NOT NULL DEFAULT now(),
  sweep_started_at timestamptz,
  live_not_before_at timestamptz,
  chat_page_loaded boolean NOT NULL DEFAULT false,
  chat_cursor text,
  next_chat_cursor text,
  remaining_chat_ids text[] NOT NULL DEFAULT '{}'::text[]
    CHECK (cardinality(remaining_chat_ids) <= 100),
  message_cursor text,
  attempt integer NOT NULL DEFAULT 0 CHECK (attempt BETWEEN 0 AND 1000),
  lease_owner text,
  lease_token uuid,
  lease_expires_at timestamptz,
  line_present boolean,
  line_reputation text,
  subscription_status text NOT NULL DEFAULT 'unknown'
    CHECK (subscription_status IN ('unknown', 'active', 'missing', 'inactive', 'misconfigured')),
  last_full_sweep_at timestamptz,
  last_webhook_ingress_at timestamptz,
  last_error_code text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (status = 'leased' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
    OR
    (status = 'pending' AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
  ),
  CHECK (
    (sweep_started_at IS NULL AND live_not_before_at IS NULL)
    OR
    (sweep_started_at IS NOT NULL AND live_not_before_at IS NOT NULL)
  )
);

CREATE INDEX linq_reconciliation_available_idx
  ON linq_reconciliation_state (available_at)
  WHERE status = 'pending';
