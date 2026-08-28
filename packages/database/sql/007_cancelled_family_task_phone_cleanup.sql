DO $florence_cancelled_family_task_phone_cleanup$
DECLARE
  due_state_constraint text;
BEGIN
  SELECT constraint_row.conname
  INTO due_state_constraint
  FROM pg_constraint constraint_row
  WHERE constraint_row.conrelid='proactive_work'::regclass
    AND constraint_row.contype='c'
    AND pg_get_constraintdef(constraint_row.oid) LIKE '%next_check_at IS NOT NULL%'
    AND pg_get_constraintdef(constraint_row.oid) LIKE '%next_check_at IS NULL%'
    AND pg_get_constraintdef(constraint_row.oid) LIKE '%status%'
  ORDER BY constraint_row.conname
  LIMIT 1;

  IF due_state_constraint IS NULL THEN
    RAISE EXCEPTION 'The proactive work due-state constraint was not found';
  END IF;

  EXECUTE format(
    'ALTER TABLE proactive_work DROP CONSTRAINT %I',
    due_state_constraint
  );
END
$florence_cancelled_family_task_phone_cleanup$;

ALTER TABLE proactive_work
  ADD CONSTRAINT proactive_work_due_state_check CHECK (
    (status='active' AND next_check_at IS NOT NULL)
    OR
    (status<>'active' AND next_check_at IS NULL)
    OR
    (
      kind='family_task' AND status='cancelled' AND next_check_at IS NOT NULL
      AND jsonb_typeof(task_state->'activePhoneCall')='object'
    )
  );
