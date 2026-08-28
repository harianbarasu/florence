ALTER TABLE proactive_work
  DROP CONSTRAINT proactive_work_kind_check;

ALTER TABLE proactive_work
  ADD COLUMN task_state jsonb,
  ADD CONSTRAINT proactive_work_kind_check CHECK (kind IN (
    'initial_private_review','initial_household_briefing','personal_google_poll',
    'family_calendar_poll','finite_monitor','interest_monitor','reminder','family_task'
  )),
  ADD CONSTRAINT proactive_work_family_task_shape_check CHECK (
    kind<>'family_task' OR (
      objective IS NOT NULL AND current_conclusion IS NOT NULL
      AND why IS NULL AND end_condition IS NULL AND cardinality(discovery_terms)=0
      AND gmail_cursor IS NULL AND calendar_cursor IS NULL
      AND jsonb_array_length(briefing_candidates)=0
      AND reminder_schedule IS NULL AND last_run_at IS NULL
      AND status IN ('active','paused','delivering','completed','cancelled')
      AND task_state IS NOT NULL AND jsonb_typeof(task_state)='object'
      AND octet_length(task_state::text)<=262144
      AND task_state ?& ARRAY[
        'kind','version','generation','phase','claim','continuationItems','pendingCall','steering',
        'publicMapResearchContext','progressRevision','terminal'
      ]
      AND task_state->>'kind'='family_work_v1'
      AND task_state->>'version'='1'
      AND task_state->>'phase' IN ('ready','tool_pending','waiting','terminal')
      AND (
        (task_state->>'phase'='tool_pending' AND jsonb_typeof(task_state->'pendingCall')='object')
        OR (task_state->>'phase'<>'tool_pending' AND task_state->'pendingCall'='null'::jsonb)
      )
      AND (
        (task_state->>'phase'='terminal' AND jsonb_typeof(task_state->'terminal')='object')
        OR (task_state->>'phase'<>'terminal' AND task_state->'terminal'='null'::jsonb)
      )
      AND (
        task_state->>'phase' IN ('ready','tool_pending')
        OR task_state->'claim'='null'::jsonb
      )
      AND (
        (status='active' AND task_state->>'phase' IN ('ready','tool_pending'))
        OR (status='paused' AND task_state->>'phase'='waiting')
        OR (status IN ('delivering','completed','cancelled')
          AND task_state->>'phase'='terminal')
      )
      AND (
        status<>'cancelled' OR task_state->'terminal'->>'outcome'='cancelled'
      )
    )
  ),
  ADD CONSTRAINT proactive_work_non_family_task_state_check CHECK (
    kind='family_task' OR task_state IS NULL
  );
