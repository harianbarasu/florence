ALTER TABLE external_connections
  ADD CONSTRAINT external_connections_scoped_identity_key
  UNIQUE (id, household_id, adult_id);

CREATE TABLE google_calendar_channels (
  channel_id text PRIMARY KEY,
  connection_id uuid NOT NULL,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  adult_id uuid NOT NULL REFERENCES adults(id) ON DELETE CASCADE,
  calendar_id text NOT NULL,
  resource_id text NOT NULL,
  resource_uri text NOT NULL,
  token_digest text NOT NULL,
  expires_at timestamptz NOT NULL,
  status text NOT NULL CHECK (status IN ('active', 'retiring', 'stopped')),
  last_message_number numeric(40, 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (connection_id, household_id, adult_id)
    REFERENCES external_connections (id, household_id, adult_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX google_calendar_channels_active_connection_idx
  ON google_calendar_channels (connection_id, calendar_id)
  WHERE status = 'active';

CREATE INDEX google_calendar_channels_expiry_idx
  ON google_calendar_channels (expires_at)
  WHERE status = 'active';

CREATE TABLE calendar_busy_windows (
  connection_id uuid NOT NULL,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  owner_adult_id uuid NOT NULL REFERENCES adults(id) ON DELETE CASCADE,
  calendar_id text NOT NULL,
  external_event_id text NOT NULL,
  source_item_id uuid NOT NULL REFERENCES source_items(id) ON DELETE CASCADE,
  source_revision bigint NOT NULL CHECK (source_revision > 0),
  window_key_id text NOT NULL,
  window_ciphertext text NOT NULL,
  candidate_buckets text[] NOT NULL CHECK (cardinality(candidate_buckets) > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (connection_id, calendar_id, external_event_id),
  FOREIGN KEY (connection_id, household_id, owner_adult_id)
    REFERENCES external_connections (id, household_id, adult_id) ON DELETE CASCADE
);

CREATE INDEX calendar_busy_windows_candidate_buckets_idx
  ON calendar_busy_windows USING gin (candidate_buckets);

CREATE INDEX calendar_busy_windows_window_key_idx
  ON calendar_busy_windows (window_key_id);
