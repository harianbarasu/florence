ALTER TABLE scheduled_triggers
  DROP CONSTRAINT IF EXISTS scheduled_triggers_status_check;

ALTER TABLE scheduled_triggers
  ADD COLUMN max_attempts integer NOT NULL DEFAULT 8 CHECK (max_attempts > 0),
  ADD COLUMN dead_at timestamptz,
  ADD CONSTRAINT scheduled_triggers_status_check
    CHECK (status IN ('scheduled', 'claimed', 'fired', 'cancelled', 'superseded', 'dead')),
  ADD CONSTRAINT scheduled_triggers_dead_shape_check
    CHECK ((status = 'dead') = (dead_at IS NOT NULL));

CREATE INDEX scheduled_triggers_dead_idx
  ON scheduled_triggers (dead_at DESC)
  WHERE status = 'dead';
