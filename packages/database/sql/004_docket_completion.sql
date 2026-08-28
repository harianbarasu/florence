DO $florence_docket_completion$
DECLARE
  finite_monitor_constraint text;
BEGIN
  SELECT constraint_row.conname
  INTO finite_monitor_constraint
  FROM pg_constraint constraint_row
  WHERE constraint_row.conrelid='proactive_work'::regclass
    AND constraint_row.contype='c'
    AND pg_get_constraintdef(constraint_row.oid) LIKE '%finite_monitor%'
    AND pg_get_constraintdef(constraint_row.oid) LIKE '%end_condition%'
    AND pg_get_constraintdef(constraint_row.oid) LIKE '%briefing_candidates%'
  ORDER BY constraint_row.conname
  LIMIT 1;

  IF finite_monitor_constraint IS NULL THEN
    RAISE EXCEPTION 'The finite monitor lifecycle constraint was not found';
  END IF;

  EXECUTE format(
    'ALTER TABLE proactive_work DROP CONSTRAINT %I',
    finite_monitor_constraint
  );
END
$florence_docket_completion$;

ALTER TABLE proactive_work
  ADD CONSTRAINT proactive_work_finite_monitor_lifecycle CHECK (
    kind<>'finite_monitor' OR (
      gmail_cursor IS NULL AND calendar_cursor IS NULL AND objective IS NOT NULL
      AND why IS NOT NULL AND current_conclusion IS NOT NULL
      AND end_condition IS NOT NULL AND cardinality(discovery_terms)=0
      AND jsonb_array_length(briefing_candidates)=0
      AND status IN ('active','paused','completed')
    )
  );

ALTER TABLE calendar_actions
  ADD COLUMN google_action_key text
  CHECK (google_action_key IS NULL OR google_action_key ~ '^[0-9a-f]{64}$');
