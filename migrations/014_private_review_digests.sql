ALTER TABLE daily_brief_runs
  DROP CONSTRAINT daily_brief_runs_household_id_local_date_key,
  ADD COLUMN kind text NOT NULL DEFAULT 'household'
    CHECK (kind IN ('household', 'private_review')),
  ADD COLUMN adult_id uuid REFERENCES adults(id) ON DELETE CASCADE,
  ADD CONSTRAINT daily_brief_runs_scope_check
    CHECK (
      (kind = 'household' AND adult_id IS NULL)
      OR (kind = 'private_review' AND adult_id IS NOT NULL)
    ),
  ADD CONSTRAINT daily_brief_runs_private_owner_key
    UNIQUE (id, household_id, adult_id);

CREATE UNIQUE INDEX daily_brief_runs_household_date_idx
  ON daily_brief_runs (household_id, local_date)
  WHERE kind = 'household';

CREATE UNIQUE INDEX daily_brief_runs_private_date_idx
  ON daily_brief_runs (household_id, adult_id, local_date)
  WHERE kind = 'private_review';

CREATE TABLE private_review_items (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  adult_id uuid NOT NULL REFERENCES adults(id) ON DELETE CASCADE,
  item_key text NOT NULL,
  source text NOT NULL CHECK (source IN ('gmail', 'calendar')),
  summary_ciphertext text NOT NULL,
  observed_at timestamptz NOT NULL,
  payload_digest text NOT NULL,
  digest_run_id uuid,
  reviewed_at timestamptz,
  retention_until timestamptz NOT NULL DEFAULT (now() + interval '7 days'),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (household_id, item_key),
  FOREIGN KEY (digest_run_id, household_id, adult_id)
    REFERENCES daily_brief_runs (id, household_id, adult_id),
  CHECK (retention_until > created_at),
  CHECK (reviewed_at IS NULL OR digest_run_id IS NOT NULL)
);

CREATE INDEX private_review_items_pending_idx
  ON private_review_items (household_id, adult_id, created_at, id)
  WHERE digest_run_id IS NULL;

CREATE INDEX private_review_items_retention_idx
  ON private_review_items (retention_until);

CREATE INDEX daily_brief_runs_private_owner_idx
  ON daily_brief_runs (household_id, adult_id, local_date DESC)
  WHERE kind = 'private_review';
