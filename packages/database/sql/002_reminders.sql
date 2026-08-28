ALTER TABLE proactive_work
  DROP CONSTRAINT proactive_work_kind_check,
  DROP CONSTRAINT proactive_work_status_check;

ALTER TABLE proactive_work
  ADD COLUMN reminder_schedule jsonb,
  ADD COLUMN last_run_at timestamptz,
  ADD CONSTRAINT proactive_work_kind_check CHECK (kind IN (
    'initial_private_review','initial_household_briefing','personal_google_poll',
    'family_calendar_poll','finite_monitor','interest_monitor','reminder'
  )),
  ADD CONSTRAINT proactive_work_status_check CHECK (status IN (
    'active','paused','delivering','completed','cancelled'
  )),
  ADD CONSTRAINT proactive_work_reminder_shape_check CHECK (
    kind<>'reminder' OR (
      objective IS NOT NULL AND reminder_schedule IS NOT NULL
      AND jsonb_typeof(reminder_schedule)='object'
      AND why IS NULL AND current_conclusion IS NULL AND end_condition IS NULL
      AND cardinality(discovery_terms)=0
      AND gmail_cursor IS NULL AND calendar_cursor IS NULL
      AND jsonb_array_length(briefing_candidates)=0
      AND status IN ('active','paused','delivering','completed','cancelled')
    )
  ),
  ADD CONSTRAINT proactive_work_non_reminder_fields_check CHECK (
    kind='reminder' OR (reminder_schedule IS NULL AND last_run_at IS NULL)
  );
