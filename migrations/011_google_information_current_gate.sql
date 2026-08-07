ALTER TABLE integrations
  ADD COLUMN information_current_control_epoch bigint
  CHECK (information_current_control_epoch IS NULL OR information_current_control_epoch > 0);
