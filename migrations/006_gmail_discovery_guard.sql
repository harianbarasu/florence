UPDATE external_connections
SET cursor = jsonb_set(
  cursor,
  '{gmail}',
  (cursor->'gmail') || jsonb_build_object(
    'schemaVersion', 2,
    'scanProcessedMessageIds', '[]'::jsonb,
    'discovery', 'null'::jsonb
  ),
  false
)
WHERE provider = 'google'
  AND jsonb_typeof(cursor->'gmail') = 'object'
  AND cursor->'gmail'->>'schemaVersion' = '1';
