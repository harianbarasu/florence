CREATE TABLE google_calendar_sync_states (
  connection_id uuid NOT NULL,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  adult_id uuid NOT NULL REFERENCES adults(id) ON DELETE CASCADE,
  calendar_id text NOT NULL,
  provider_calendar_id text NOT NULL,
  display_name_ciphertext text NOT NULL,
  access_role text,
  is_primary boolean NOT NULL,
  selected boolean NOT NULL,
  hidden boolean NOT NULL,
  status text NOT NULL CHECK (status IN ('active', 'excluded', 'deleted')),
  selection_source text NOT NULL CHECK (selection_source IN ('provider', 'adult')),
  adult_enabled boolean,
  availability_only boolean NOT NULL,
  last_seen_scan_id uuid NOT NULL,
  catalog_revision bigint NOT NULL CHECK (catalog_revision >= 0),
  sync_state jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (connection_id, calendar_id),
  UNIQUE (connection_id, provider_calendar_id),
  CHECK (access_role IS NULL OR access_role IN (
    'freeBusyReader', 'reader', 'writerWithoutPrivateAccess', 'writer', 'owner'
  )),
  CHECK (sync_state IS NULL OR jsonb_typeof(sync_state) = 'object'),
  CHECK (selection_source = 'adult' OR adult_enabled IS NULL),
  FOREIGN KEY (connection_id, household_id, adult_id)
    REFERENCES external_connections (id, household_id, adult_id) ON DELETE CASCADE
);

CREATE INDEX google_calendar_sync_states_coverage_idx
  ON google_calendar_sync_states (household_id, adult_id, status, connection_id);

CREATE INDEX google_calendar_sync_states_catalog_scan_idx
  ON google_calendar_sync_states (connection_id, last_seen_scan_id);

-- The former singular primary-calendar cursor cannot be authoritative beside
-- the catalog. Force one clean catalog/event rebuild rather than carrying two
-- cursor formats or retaining busy rows whose coverage set is unknowable.
UPDATE external_connections
SET cursor = cursor - 'calendar', updated_at = now()
WHERE provider = 'google' AND cursor ? 'calendar';

UPDATE google_calendar_channels
SET status = 'stopped', updated_at = now()
WHERE status IN ('active', 'retiring');

DELETE FROM calendar_busy_windows;
