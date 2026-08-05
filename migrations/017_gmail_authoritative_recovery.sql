CREATE TABLE gmail_recovery_runs (
  connection_id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  adult_id uuid NOT NULL REFERENCES adults(id) ON DELETE CASCADE,
  generation_id uuid NOT NULL,
  target_history_id text NOT NULL CHECK (target_history_id ~ '^[0-9]+$'),
  started_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (connection_id, generation_id),
  FOREIGN KEY (connection_id, household_id, adult_id)
    REFERENCES external_connections(id, household_id, adult_id) ON DELETE CASCADE
);

CREATE TABLE gmail_recovery_seen_messages (
  connection_id uuid NOT NULL,
  generation_id uuid NOT NULL,
  external_id text NOT NULL,
  disposition text NOT NULL CHECK (disposition IN ('present', 'missing')),
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  processed_at timestamptz,
  PRIMARY KEY (connection_id, generation_id, external_id),
  FOREIGN KEY (connection_id, generation_id)
    REFERENCES gmail_recovery_runs(connection_id, generation_id) ON DELETE CASCADE
);

CREATE INDEX gmail_recovery_seen_messages_pending_idx
  ON gmail_recovery_seen_messages (connection_id, generation_id, disposition, external_id)
  WHERE processed_at IS NULL;
