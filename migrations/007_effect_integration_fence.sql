-- Effects derived from one connected account must stop when that exact
-- integration authority is revoked or replaced. Existing non-integration
-- effects remain valid and intentionally carry no integration fence.

ALTER TABLE outbox
  ADD COLUMN integration_id uuid REFERENCES integrations(id),
  ADD COLUMN integration_control_epoch bigint CHECK (integration_control_epoch > 0),
  ADD CONSTRAINT outbox_integration_fence_complete CHECK (
    (integration_id IS NULL) = (integration_control_epoch IS NULL)
  );

CREATE INDEX outbox_integration_fence_idx
  ON outbox (integration_id, integration_control_epoch, status, updated_at DESC)
  WHERE integration_id IS NOT NULL;
