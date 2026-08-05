ALTER TABLE jobs
  ADD COLUMN recovery_generation integer NOT NULL DEFAULT 0
    CHECK (recovery_generation BETWEEN 0 AND 3),
  ADD COLUMN last_rearmed_at timestamptz;
