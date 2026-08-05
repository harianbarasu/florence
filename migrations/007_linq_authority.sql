ALTER TABLE channel_suppressions
  ADD COLUMN source_event_id text;

UPDATE channel_suppressions
SET source_event_id = 'legacy:' || id::text;

ALTER TABLE channel_suppressions
  ALTER COLUMN source_event_id SET NOT NULL,
  ADD CONSTRAINT channel_suppressions_source_event_id_nonempty
    CHECK (source_event_id <> '');
