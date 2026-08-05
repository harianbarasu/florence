ALTER TABLE scheduled_triggers
  ADD COLUMN definition_digest text NOT NULL DEFAULT repeat('0', 64);

ALTER TABLE scheduled_triggers
  ALTER COLUMN definition_digest DROP DEFAULT;

ALTER TABLE scheduled_triggers
  ADD CONSTRAINT scheduled_triggers_definition_digest_check
  CHECK (definition_digest ~ '^[a-f0-9]{64}$');
