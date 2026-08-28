-- A Google source may support several distinct household actions, so pre-key Calendar review
-- proposals cannot be matched back to one docket item safely. Retire only that private-Google
-- legacy cohort; the next authoritative review recreates anything still relevant with an exact
-- action key. Conversational actions and committed Calendar history remain intact.
WITH retired AS (
  DELETE FROM calendar_actions action
  USING sources basis
  WHERE basis.id=action.basis_source_id
    AND basis.household_id=action.household_id
    AND basis.visibility='private'
    AND basis.kind IN ('gmail','calendar')
    AND action.google_action_key IS NULL
    AND action.status IN ('offered','pending','failed')
    AND action.approval_source_id IS NULL
    AND NOT (
      action.status='pending'
      AND action.last_error='calendar_action_execution_claim_v1'
      AND action.retry_at>now()
    )
  RETURNING action.approval_prompt_source_id
)
UPDATE messages message
SET status='failed',sending_at=NULL,retry_at=NULL,
  last_error='Retired during Google Calendar action identity upgrade'
WHERE message.source_id IN (
  SELECT approval_prompt_source_id
  FROM retired
  WHERE approval_prompt_source_id IS NOT NULL
)
  AND message.direction='outbound'
  AND message.status='pending';
