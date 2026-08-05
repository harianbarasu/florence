DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM channel_bindings
    WHERE provider = 'linq'
      AND channel_type = 'private'
      AND status IN ('pending', 'active', 'paused')
    GROUP BY external_handle
    HAVING count(*) > 1
  ) THEN
    RAISE EXCEPTION 'Cannot enforce one live Linq household per handle while conflicting bindings exist';
  END IF;
END
$$;

ALTER TABLE channel_bindings
  DROP CONSTRAINT IF EXISTS channel_bindings_provider_external_chat_id_external_handle_key;

DROP INDEX IF EXISTS channel_bindings_private_identity_idx;

CREATE UNIQUE INDEX channel_bindings_live_private_identity_idx
  ON channel_bindings (provider, external_chat_id, external_handle)
  WHERE channel_type = 'private' AND status IN ('pending', 'active', 'paused');

CREATE UNIQUE INDEX channel_bindings_live_private_handle_idx
  ON channel_bindings (provider, external_handle)
  WHERE channel_type = 'private' AND status IN ('pending', 'active', 'paused');
