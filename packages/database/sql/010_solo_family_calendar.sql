-- A Florence Calendar starts with its founder and gains a second owner when the household expands.
-- Keep the existing ID-only recovery state, while allowing the completed provider-confirmed state
-- to omit the partner connection until that second adult joins.
ALTER TABLE households
  DROP CONSTRAINT households_family_calendar_lifecycle,
  ADD CONSTRAINT households_family_calendar_lifecycle CHECK (
    (family_calendar_id IS NULL OR (
      family_calendar_create_attempted_at IS NOT NULL
      AND length(trim(family_calendar_id)) > 0
      AND family_calendar_id <> 'primary'
    ))
    AND (
      (family_calendar_id IS NULL
        AND family_calendar_owner_connection_id IS NULL
        AND family_calendar_partner_connection_id IS NULL
        AND family_calendar_label IS NULL
        AND family_calendar_created_at IS NULL)
      OR
      (family_calendar_id IS NOT NULL AND (
        (family_calendar_owner_connection_id IS NULL
          AND family_calendar_partner_connection_id IS NULL
          AND family_calendar_label IS NULL
          AND family_calendar_created_at IS NULL)
        OR
        (family_calendar_owner_connection_id IS NOT NULL
          AND (
            family_calendar_partner_connection_id IS NULL
            OR family_calendar_owner_connection_id <> family_calendar_partner_connection_id
          )
          AND family_calendar_label IS NOT NULL
          AND length(trim(family_calendar_label)) > 0
          AND family_calendar_created_at IS NOT NULL)
      ))
    )
  );

-- A solo parent can keep an interest watch in their private Florence thread. The existing
-- visibility/owner constraint still requires every private watch to name its owning adult.
ALTER TABLE proactive_work
  DROP CONSTRAINT proactive_work_check7,
  ADD CONSTRAINT proactive_work_interest_monitor_shape_check CHECK (
    kind<>'interest_monitor' OR (
      gmail_cursor IS NULL AND calendar_cursor IS NULL
      AND objective IS NOT NULL AND why IS NOT NULL AND current_conclusion IS NOT NULL
      AND end_condition IS NULL AND cardinality(discovery_terms)>0
      AND jsonb_array_length(briefing_candidates)=0
      AND status IN ('active','paused')
    )
  );
