-- Assigned scheduled work keeps the recurring-series parent separate from a reassignment of one
-- active occurrence. Extend the existing family-task schedule envelope without adding another
-- workflow row or scheduler.
ALTER TABLE proactive_work
  DROP CONSTRAINT proactive_work_family_task_shape_check;

-- Freeze the pre-deploy series owner before any in-flight occurrence can be privately reassigned.
-- An explicit JSON null is distinct from a legacy missing field at runtime.
UPDATE proactive_work
SET reminder_schedule=jsonb_set(
  reminder_schedule,
  '{responsibleAdultId}',
  COALESCE(task_state->'responsibleAdultId','null'::jsonb),
  true
)
WHERE kind='family_task' AND reminder_schedule IS NOT NULL
  AND NOT reminder_schedule ? 'responsibleAdultId';

ALTER TABLE proactive_work
  ADD CONSTRAINT proactive_work_family_task_shape_check CHECK (
    kind<>'family_task' OR (
      objective IS NOT NULL AND current_conclusion IS NOT NULL
      AND why IS NULL AND end_condition IS NULL AND cardinality(discovery_terms)=0
      AND gmail_cursor IS NULL AND calendar_cursor IS NULL
      AND jsonb_array_length(briefing_candidates)=0
      AND status IN ('active','paused','delivering','completed','cancelled')
      AND task_state IS NOT NULL AND jsonb_typeof(task_state)='object'
      AND octet_length(task_state::text)<=262144
      AND task_state ?& ARRAY[
        'kind','version','generation','phase','claim','continuationItems','pendingCall','steering',
        'progressRevision','terminal'
      ]
      AND task_state->>'kind'='family_work_v1'
      AND task_state->>'version'='1'
      AND task_state->>'phase' IN ('ready','tool_pending','waiting','terminal')
      AND (
        reminder_schedule IS NULL OR (
          jsonb_typeof(reminder_schedule)='object'
          AND reminder_schedule ?& ARRAY[
            'version','schedule','paused','occurrenceActive','previousResult'
          ]
          AND reminder_schedule-ARRAY[
            'version','schedule','paused','occurrenceActive','previousResult','completionCondition',
            'responsibleAdultId'
          ]='{}'::jsonb
          AND reminder_schedule->'version'='1'::jsonb
          AND jsonb_typeof(reminder_schedule->'schedule')='object'
          AND jsonb_typeof(reminder_schedule->'paused')='boolean'
          AND jsonb_typeof(reminder_schedule->'occurrenceActive')='boolean'
          AND (
            reminder_schedule->'previousResult'='null'::jsonb
            OR jsonb_typeof(reminder_schedule->'previousResult')='string'
          )
          AND (
            NOT reminder_schedule ? 'completionCondition'
            OR reminder_schedule->'completionCondition'='null'::jsonb
            OR jsonb_typeof(reminder_schedule->'completionCondition')='string'
          )
          AND (
            NOT reminder_schedule ? 'responsibleAdultId'
            OR reminder_schedule->'responsibleAdultId'='null'::jsonb
            OR jsonb_typeof(reminder_schedule->'responsibleAdultId')='string'
          )
        )
      )
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
        OR (
          status='paused' AND (
            task_state->>'phase'='waiting'
            OR (
              task_state->>'phase'='ready'
              AND reminder_schedule->'paused'='true'::jsonb
            )
          )
        )
        OR (status IN ('delivering','completed','cancelled')
          AND task_state->>'phase'='terminal')
      )
      AND (status<>'cancelled' OR task_state->'terminal'->>'outcome'='cancelled')
    )
  );
