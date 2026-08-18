ALTER TABLE sources DROP CONSTRAINT sources_kind_check;
ALTER TABLE sources ADD CONSTRAINT sources_kind_check
  CHECK (kind IN ('linq_message','gmail','google_file','document','web','setup','calendar'));

CREATE TABLE proactive_work (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  kind text NOT NULL CHECK (kind IN (
    'initial_private_review','initial_household_briefing','gmail_sync','calendar_sync',
    'finite_monitor','interest_discovery'
  )),
  visibility text NOT NULL CHECK (visibility IN ('private','household')),
  owner_adult_id uuid,
  connection_id uuid REFERENCES google_connections(id) ON DELETE CASCADE,
  calendar_id text,
  dedupe_key text NOT NULL,
  objective text,
  why text,
  current_conclusion text,
  end_condition text,
  discovery_terms text[] NOT NULL DEFAULT '{}',
  coarse_location text,
  provider_cursor text,
  conclusion_digest text CHECK (
    conclusion_digest IS NULL OR conclusion_digest ~ '^[0-9a-f]{64}$'
  ),
  briefing_candidates jsonb NOT NULL DEFAULT '[]'::jsonb
    CHECK (jsonb_typeof(briefing_candidates)='array'),
  coverage jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(coverage)='object'),
  status text NOT NULL CHECK (status IN ('waiting_initial','active','paused','completed')),
  next_check_at timestamptz,
  lease_token uuid,
  lease_until timestamptz,
  last_checked_at timestamptz,
  last_changed_at timestamptz,
  last_notified_at timestamptz,
  ignored_suggestions smallint NOT NULL DEFAULT 0 CHECK (ignored_suggestions >= 0),
  failure_count integer NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (household_id,kind,dedupe_key),
  FOREIGN KEY (household_id,owner_adult_id) REFERENCES people(household_id,id),
  CHECK (
    (visibility='private' AND owner_adult_id IS NOT NULL)
    OR (visibility='household' AND owner_adult_id IS NULL)
  ),
  CHECK (
    (lease_token IS NULL AND lease_until IS NULL)
    OR (lease_token IS NOT NULL AND lease_until IS NOT NULL AND status='active')
  ),
  CHECK (
    (status='active' AND next_check_at IS NOT NULL)
    OR (status<>'active' AND next_check_at IS NULL)
  ),
  CHECK (
    kind<>'initial_private_review' OR (
      visibility='private' AND connection_id IS NOT NULL AND calendar_id IS NULL
      AND objective IS NULL AND why IS NULL AND current_conclusion IS NULL
      AND end_condition IS NULL AND cardinality(discovery_terms)=0 AND coarse_location IS NULL
      AND provider_cursor IS NULL AND status IN ('active','completed')
    )
  ),
  CHECK (
    kind<>'initial_household_briefing' OR (
      visibility='household' AND connection_id IS NULL AND calendar_id IS NULL
      AND objective IS NULL AND why IS NULL AND current_conclusion IS NULL
      AND end_condition IS NULL AND cardinality(discovery_terms)=0 AND coarse_location IS NULL
      AND provider_cursor IS NULL AND jsonb_array_length(briefing_candidates)=0
      AND status IN ('waiting_initial','active','completed')
    )
  ),
  CHECK (
    kind<>'gmail_sync' OR (
      visibility='private' AND connection_id IS NOT NULL AND calendar_id IS NULL
      AND objective IS NULL AND why IS NULL AND end_condition IS NULL
      AND cardinality(discovery_terms)=0 AND coarse_location IS NULL
      AND jsonb_array_length(briefing_candidates)=0
      AND status IN ('waiting_initial','active','paused')
    )
  ),
  CHECK (
    kind<>'calendar_sync' OR (
      connection_id IS NOT NULL AND calendar_id IS NOT NULL
      AND objective IS NULL AND why IS NULL AND end_condition IS NULL
      AND cardinality(discovery_terms)=0 AND coarse_location IS NULL
      AND jsonb_array_length(briefing_candidates)=0
      AND status IN ('waiting_initial','active','paused')
    )
  ),
  CHECK (
    kind<>'finite_monitor' OR (
      connection_id IS NULL AND calendar_id IS NULL AND provider_cursor IS NULL
      AND objective IS NOT NULL AND why IS NOT NULL AND end_condition IS NOT NULL
      AND cardinality(discovery_terms)=0 AND coarse_location IS NULL
      AND jsonb_array_length(briefing_candidates)=0
      AND status IN ('active','paused','completed')
    )
  ),
  CHECK (
    kind<>'interest_discovery' OR (
      visibility='household' AND connection_id IS NULL AND calendar_id IS NULL
      AND provider_cursor IS NULL AND objective IS NOT NULL AND why IS NOT NULL
      AND end_condition IS NULL AND cardinality(discovery_terms)>0 AND coarse_location IS NOT NULL
      AND jsonb_array_length(briefing_candidates)=0
      AND status IN ('active','paused','completed')
    )
  ),
  CHECK (
    kind='initial_private_review' OR jsonb_array_length(briefing_candidates)=0
  )
);

CREATE INDEX proactive_work_due ON proactive_work(next_check_at,id)
  WHERE status='active';
CREATE INDEX proactive_work_lease ON proactive_work(lease_until)
  WHERE lease_until IS NOT NULL;
CREATE UNIQUE INDEX proactive_work_initial_review_adult
  ON proactive_work(household_id,owner_adult_id)
  WHERE kind='initial_private_review';

CREATE TABLE proactive_work_sources (
  work_id uuid NOT NULL REFERENCES proactive_work(id) ON DELETE CASCADE,
  source_id uuid NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  PRIMARY KEY (work_id,source_id)
);
