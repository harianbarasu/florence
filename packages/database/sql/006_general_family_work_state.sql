ALTER TABLE proactive_work
  DROP CONSTRAINT proactive_work_family_task_shape_check;

UPDATE proactive_work
SET task_state=task_state-'publicMapResearchContext'
WHERE kind='family_task' AND task_state ? 'publicMapResearchContext';

UPDATE proactive_work
SET briefing_candidates=regexp_replace(
  briefing_candidates::text,
  '"familyRelevance"\s*:\s*"(child_care_school_or_activity|household_logistics|enrolled_adult_coordination)"',
  '"familyRelevance": "household"',
  'g'
)::jsonb
WHERE briefing_candidates::text ~ '"familyRelevance"\s*:\s*"(child_care_school_or_activity|household_logistics|enrolled_adult_coordination)"';

-- Presentation used to be inferred from a fact's kind and slot every time it was read. Materialize
-- that meaning once so all runtime readers can require one current, explicit storage shape. This
-- helper accepts both the old scalar fact value and the old/new object forms, and is dropped below.
CREATE FUNCTION florence_migrate_006_memory_presentation(
  stored jsonb,
  fact_slot text,
  fact_label text,
  fact_kind text,
  fact_statement text
) RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  stored_record jsonb := CASE
    WHEN jsonb_typeof(stored)='object' AND jsonb_typeof(stored->'memory')='object'
      THEN stored->'memory'
    WHEN jsonb_typeof(stored)='object' THEN stored
    ELSE '{}'::jsonb
  END;
  has_explicit boolean;
  memory_kind text;
  artifact_kind text;
  candidate_artifact_kind text;
  title text;
  details text;
  tags jsonb := '[]'::jsonb;
  seen_tags text[] := ARRAY[]::text[];
  tag jsonb;
  normalized_tag text;
BEGIN
  has_explicit := stored_record ?| ARRAY['memoryKind','artifactKind','title','details','tags'];

  candidate_artifact_kind := stored_record->>'artifactKind';
  IF candidate_artifact_kind NOT IN ('recipe','list','plan','note','reference','other') THEN
    candidate_artifact_kind := NULL;
  END IF;
  artifact_kind := candidate_artifact_kind;
  IF artifact_kind IS NULL AND NOT has_explicit THEN
    IF fact_slot LIKE 'recipe:%' THEN
      artifact_kind := 'recipe';
    ELSIF fact_slot LIKE 'artifact:%' AND split_part(fact_slot,':',2)
      IN ('recipe','list','plan','note','reference','other') THEN
      artifact_kind := split_part(fact_slot,':',2);
    END IF;
  END IF;

  memory_kind := stored_record->>'memoryKind';
  IF memory_kind IS NULL OR memory_kind NOT IN ('fact','preference','routine','artifact') THEN
    memory_kind := CASE
      WHEN artifact_kind IS NOT NULL THEN 'artifact'
      WHEN NOT has_explicit AND fact_kind='preference' THEN 'preference'
      WHEN NOT has_explicit AND fact_slot LIKE 'routine:%' THEN 'routine'
      ELSE 'fact'
    END;
  END IF;

  IF jsonb_typeof(stored_record->'title')='string' THEN
    title := NULLIF(left(btrim(stored_record->>'title'),300),'');
  END IF;
  IF jsonb_typeof(stored_record->'details')='string' THEN
    details := NULLIF(left(btrim(stored_record->>'details'),12000),'');
  END IF;
  IF jsonb_typeof(stored_record->'tags')='array' THEN
    FOR tag IN SELECT value FROM jsonb_array_elements(stored_record->'tags') LOOP
      IF jsonb_typeof(tag)='string' THEN
        normalized_tag := NULLIF(left(btrim(tag #>> '{}'),80),'');
        IF normalized_tag IS NOT NULL
          AND NOT lower(normalized_tag)=ANY(seen_tags)
          AND cardinality(seen_tags)<20 THEN
          seen_tags := array_append(seen_tags,lower(normalized_tag));
          tags := tags||jsonb_build_array(normalized_tag);
        END IF;
      END IF;
    END LOOP;
  END IF;

  IF memory_kind='artifact' THEN
    artifact_kind := COALESCE(artifact_kind,'other');
    title := COALESCE(title,NULLIF(left(btrim(fact_label),300),''),'Saved item');
    details := COALESCE(details,NULLIF(left(btrim(fact_statement),12000),''),'Saved item');
  ELSE
    artifact_kind := NULL;
  END IF;

  RETURN jsonb_build_object(
    'memoryKind',memory_kind,
    'artifactKind',artifact_kind,
    'title',title,
    'details',details,
    'tags',tags
  );
END;
$$;

WITH stored_facts AS (
  SELECT id,slot,label,kind,value,
    CASE
      WHEN jsonb_typeof(value)='string' THEN value #>> '{}'
      WHEN jsonb_typeof(value)='object' AND jsonb_typeof(value->'statement')='string'
        THEN value->>'statement'
      ELSE value::text
    END AS statement
  FROM facts
)
UPDATE facts fact
SET value=(
  CASE WHEN jsonb_typeof(stored.value)='object' THEN stored.value-'memory' ELSE '{}'::jsonb END
)||jsonb_build_object('statement',stored.statement)||florence_migrate_006_memory_presentation(
  stored.value,stored.slot,stored.label,stored.kind,stored.statement
)
FROM stored_facts stored
WHERE fact.id=stored.id;

-- A resumable initial review stores retained facts inside its scan envelope. Older checkpoints have
-- no memory member (or a scalar placeholder); newer checkpoints already carry the full object.
WITH transformed AS (
  SELECT work.id,COALESCE(jsonb_agg(
    CASE
      WHEN jsonb_typeof(candidate.value)='object'
        AND candidate.value->>'kind'='initial_private_google_scan_v1'
        AND jsonb_typeof(candidate.value #> '{outcomes,facts}')='array'
      THEN jsonb_set(
        candidate.value,
        '{outcomes,facts}',
        COALESCE((
          SELECT jsonb_agg(
            CASE
              WHEN jsonb_typeof(fact.value)='object' THEN fact.value||jsonb_build_object(
                'memory',florence_migrate_006_memory_presentation(
                  fact.value->'memory',
                  COALESCE(fact.value->>'slot',''),
                  COALESCE(fact.value->>'statement','Saved fact'),
                  NULL,
                  COALESCE(fact.value->>'statement','Saved fact')
                )
              )
              ELSE fact.value
            END
            ORDER BY fact.ordinality
          )
          FROM jsonb_array_elements(candidate.value #> '{outcomes,facts}')
            WITH ORDINALITY AS fact(value,ordinality)
        ),'[]'::jsonb),
        false
      )
      ELSE candidate.value
    END
    ORDER BY candidate.ordinality
  ),'[]'::jsonb) AS candidates
  FROM proactive_work work
  CROSS JOIN LATERAL jsonb_array_elements(work.briefing_candidates)
    WITH ORDINALITY AS candidate(value,ordinality)
  WHERE work.kind='initial_private_review'
    AND EXISTS (
      SELECT 1
      FROM jsonb_array_elements(work.briefing_candidates) stored_candidate(value)
      WHERE jsonb_typeof(stored_candidate.value)='object'
        AND stored_candidate.value->>'kind'='initial_private_google_scan_v1'
        AND jsonb_typeof(stored_candidate.value #> '{outcomes,facts}')='array'
    )
  GROUP BY work.id
)
UPDATE proactive_work work
SET briefing_candidates=transformed.candidates
FROM transformed
WHERE work.id=transformed.id;

DROP FUNCTION florence_migrate_006_memory_presentation(jsonb,text,text,text,text);

ALTER TABLE proactive_work
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
        'progressRevision','terminal'
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
  );
