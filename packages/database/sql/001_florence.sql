CREATE TABLE households (
  id uuid PRIMARY KEY,
  name text NOT NULL CHECK (length(trim(name)) > 0),
  time_zone text NOT NULL CHECK (length(trim(time_zone)) > 0),
  family_calendar_create_attempted_at timestamptz,
  family_calendar_id text,
  family_calendar_owner_connection_id uuid,
  family_calendar_partner_connection_id uuid,
  family_calendar_label text,
  family_calendar_created_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT households_family_calendar_lifecycle CHECK (
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
          AND family_calendar_partner_connection_id IS NOT NULL
          AND family_calendar_owner_connection_id <> family_calendar_partner_connection_id
          AND family_calendar_label IS NOT NULL
          AND length(trim(family_calendar_label)) > 0
          AND family_calendar_created_at IS NOT NULL)
      ))
    )
  )
);
CREATE UNIQUE INDEX households_family_calendar_id_unique
  ON households(family_calendar_id)
  WHERE family_calendar_id IS NOT NULL;

CREATE TABLE people (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  kind text NOT NULL CHECK (kind IN ('adult','child')),
  role text NOT NULL CHECK (role IN ('steward','caregiver','dependent')),
  adult_slot smallint,
  display_name text NOT NULL CHECK (length(trim(display_name)) > 0),
  status text NOT NULL CHECK (status IN ('planned','verified','represented')),
  identity_subject_digest text,
  consent_version text,
  consented_at timestamptz,
  guardian_attested_at timestamptz,
  invitation_digest text,
  invitation_expires_at timestamptz,
  invitation_consumed_at timestamptz,
  messages_address text,
  invitation_conversation_id text,
  invitation_identity_digest text,
  invitation_message_id text,
  invitation_issued_at timestamptz,
  invitation_approval_source_id uuid,
  invitation_approved_at timestamptz,
  invitation_retry_at timestamptz,
  invitation_last_error text,
  profile jsonb NOT NULL DEFAULT '{}'::jsonb,
  preferences jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (household_id,adult_slot),
  UNIQUE (household_id,id),
  UNIQUE (household_id,id,identity_subject_digest),
  CONSTRAINT people_identity_subject_digest CHECK (
    identity_subject_digest IS NULL OR identity_subject_digest ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT people_invitation_digest CHECK (
    invitation_digest IS NULL OR invitation_digest ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT people_messages_address_e164 CHECK (
    messages_address IS NULL OR messages_address ~ '^\+[1-9][0-9]{7,14}$'
  ),
  CONSTRAINT people_invitation_identity_digest CHECK (
    invitation_identity_digest IS NULL OR invitation_identity_digest ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT people_kind_state CHECK (
    (kind='adult' AND adult_slot IN (1,2) AND role IN ('steward','caregiver')
      AND status IN ('planned','verified'))
    OR
    (kind='child' AND adult_slot IS NULL AND role='dependent'
      AND status='represented' AND identity_subject_digest IS NULL)
  ),
  CONSTRAINT people_identity_consent_complete CHECK (
    (identity_subject_digest IS NULL AND consent_version IS NULL AND consented_at IS NULL)
    OR
    (identity_subject_digest IS NOT NULL AND consent_version IS NOT NULL AND consented_at IS NOT NULL)
  ),
  CONSTRAINT people_invitation_delivery_complete CHECK (
    (invitation_conversation_id IS NULL AND invitation_identity_digest IS NULL
      AND invitation_message_id IS NULL AND invitation_issued_at IS NULL)
    OR
    (invitation_conversation_id IS NOT NULL AND invitation_identity_digest IS NOT NULL
      AND invitation_message_id IS NOT NULL AND invitation_issued_at IS NOT NULL)
  ),
  CONSTRAINT people_invitation_approval_complete CHECK (
    (invitation_approval_source_id IS NULL AND invitation_approved_at IS NULL
      AND invitation_retry_at IS NULL AND invitation_last_error IS NULL)
    OR
    (invitation_approval_source_id IS NOT NULL AND invitation_approved_at IS NOT NULL)
  )
);
CREATE UNIQUE INDEX people_identity_unique ON people(identity_subject_digest)
  WHERE identity_subject_digest IS NOT NULL;
CREATE UNIQUE INDEX people_invitation_unique ON people(invitation_digest)
  WHERE invitation_digest IS NOT NULL;
CREATE UNIQUE INDEX people_messages_address_unique ON people(messages_address)
  WHERE messages_address IS NOT NULL;
CREATE UNIQUE INDEX people_invitation_conversation_unique ON people(invitation_conversation_id)
  WHERE invitation_conversation_id IS NOT NULL;

CREATE TABLE linq_channels (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  audience text NOT NULL CHECK (audience IN ('private','group')),
  provider_conversation_id text NOT NULL,
  adult_one_id uuid NOT NULL,
  identity_one_digest text NOT NULL CHECK (identity_one_digest ~ '^[0-9a-f]{64}$'),
  adult_two_id uuid,
  identity_two_digest text,
  authority_digest text NOT NULL CHECK (authority_digest ~ '^[0-9a-f]{64}$'),
  bound_at timestamptz NOT NULL,
  revoked_at timestamptz,
  stopped_at timestamptz,
  FOREIGN KEY (household_id,adult_one_id)
    REFERENCES people(household_id,id),
  FOREIGN KEY (household_id,adult_two_id)
    REFERENCES people(household_id,id),
  CHECK (
    (audience='private' AND adult_two_id IS NULL AND identity_two_digest IS NULL)
    OR
    (audience='group' AND adult_two_id IS NOT NULL AND identity_two_digest IS NOT NULL
      AND adult_one_id < adult_two_id AND identity_one_digest <> identity_two_digest)
  )
);
CREATE UNIQUE INDEX linq_active_provider_unique ON linq_channels(provider_conversation_id)
  WHERE revoked_at IS NULL;
CREATE UNIQUE INDEX linq_one_private_per_adult ON linq_channels(household_id,adult_one_id)
  WHERE audience='private' AND revoked_at IS NULL;
CREATE UNIQUE INDEX linq_one_group ON linq_channels(household_id)
  WHERE audience='group' AND revoked_at IS NULL;

CREATE FUNCTION reject_linq_authority_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(OLD.household_id,OLD.audience,OLD.provider_conversation_id,OLD.adult_one_id,
    OLD.identity_one_digest,OLD.adult_two_id,OLD.identity_two_digest,OLD.authority_digest,OLD.bound_at)
  IS DISTINCT FROM ROW(NEW.household_id,NEW.audience,NEW.provider_conversation_id,NEW.adult_one_id,
    NEW.identity_one_digest,NEW.adult_two_id,NEW.identity_two_digest,NEW.authority_digest,NEW.bound_at)
  THEN RAISE EXCEPTION 'Linq authority is immutable; revoke and bind a new row'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER linq_authority_immutable BEFORE UPDATE ON linq_channels
  FOR EACH ROW EXECUTE FUNCTION reject_linq_authority_change();

CREATE TABLE sources (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  kind text NOT NULL CHECK (
    kind IN ('linq_message','gmail','google_file','document','web','setup','calendar')
  ),
  visibility text NOT NULL CHECK (visibility IN ('private','household')),
  owner_adult_id uuid,
  external_key text NOT NULL,
  parent_source_id uuid REFERENCES sources(id) ON DELETE SET NULL,
  label text NOT NULL,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  occurred_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (household_id,kind,external_key),
  FOREIGN KEY (household_id,owner_adult_id) REFERENCES people(household_id,id),
  CHECK (
    (visibility='household' AND owner_adult_id IS NULL)
    OR
    (visibility='private' AND owner_adult_id IS NOT NULL)
  )
);

ALTER TABLE people
  ADD CONSTRAINT people_invitation_approval_source_id_fkey
  FOREIGN KEY (invitation_approval_source_id) REFERENCES sources(id) ON DELETE SET NULL;

CREATE TABLE messages (
  source_id uuid PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
  channel_id uuid NOT NULL REFERENCES linq_channels(id),
  direction text NOT NULL CHECK (direction IN ('inbound','outbound')),
  sender_adult_id uuid REFERENCES people(id),
  move_kind text NOT NULL CHECK (move_kind IN ('message','reply','reaction')),
  text text,
  reaction text,
  images jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(images)='array'),
  has_attachments boolean NOT NULL DEFAULT false,
  provider_event_id text,
  provider_message_id text,
  reply_to_source_id uuid REFERENCES messages(source_id) ON DELETE SET NULL,
  turn_id uuid NOT NULL,
  turn_part smallint NOT NULL CHECK (turn_part BETWEEN -1 AND 2),
  idempotency_key text,
  not_before timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL CHECK (status IN ('received','handled','pending','sending','sent','failed')),
  handled_at timestamptz,
  sending_at timestamptz,
  sent_at timestamptz,
  receipt_detail jsonb,
  retry_at timestamptz,
  last_error text,
  UNIQUE (turn_id,turn_part),
  CHECK (
    (move_kind='reaction' AND turn_part=-1 AND reaction IS NOT NULL AND text IS NULL)
    OR
    (move_kind IN ('message','reply') AND turn_part>=0 AND reaction IS NULL
      AND (text IS NOT NULL OR has_attachments))
  ),
  CHECK (
    (direction='inbound' AND sender_adult_id IS NOT NULL AND provider_event_id IS NOT NULL
      AND provider_message_id IS NOT NULL AND idempotency_key IS NULL
      AND status IN ('received','handled'))
    OR
    (direction='outbound' AND sender_adult_id IS NULL AND provider_event_id IS NULL
      AND idempotency_key IS NOT NULL AND status IN ('pending','sending','sent','failed'))
  ),
  CHECK (status <> 'sent' OR (provider_message_id IS NOT NULL AND sent_at IS NOT NULL))
);
CREATE UNIQUE INDEX messages_provider_event_unique ON messages(provider_event_id)
  WHERE provider_event_id IS NOT NULL;
CREATE UNIQUE INDEX messages_provider_message_unique ON messages(channel_id,provider_message_id)
  WHERE provider_message_id IS NOT NULL;
CREATE UNIQUE INDEX messages_idempotency_unique ON messages(idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE INDEX messages_inbound_due ON messages(COALESCE(retry_at,not_before),source_id)
  WHERE status='received';
CREATE INDEX messages_outbound_due ON messages(COALESCE(retry_at,not_before),source_id)
  WHERE status='pending';

CREATE TABLE documents (
  source_id uuid PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
  saved_by_adult_id uuid NOT NULL REFERENCES people(id),
  filename text NOT NULL,
  mime_type text NOT NULL,
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  retained boolean NOT NULL DEFAULT false,
  content_envelope bytea,
  discard_after timestamptz,
  CHECK (NOT retained OR (content_envelope IS NOT NULL AND discard_after IS NULL))
);

CREATE TABLE facts (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  subject_person_id uuid REFERENCES people(id) ON DELETE CASCADE,
  kind text NOT NULL CHECK (
    kind IN ('identity','school','caregiver','activity','schedule','address','phone','contact',
      'preference','safety','general')
  ),
  slot text NOT NULL,
  label text NOT NULL,
  value jsonb NOT NULL,
  visibility text NOT NULL CHECK (visibility IN ('private','household')),
  owner_adult_id uuid REFERENCES people(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  corrected_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (visibility='household' AND owner_adult_id IS NULL)
    OR
    (visibility='private' AND owner_adult_id IS NOT NULL)
  )
);
CREATE UNIQUE INDEX facts_current_unique ON facts(
  household_id,slot,visibility,
  COALESCE(owner_adult_id,'00000000-0000-0000-0000-000000000000'::uuid)
);

CREATE TABLE fact_sources (
  fact_id uuid NOT NULL REFERENCES facts(id) ON DELETE CASCADE,
  source_id uuid NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  PRIMARY KEY (fact_id,source_id)
);

CREATE TABLE google_connections (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL,
  owner_adult_id uuid NOT NULL,
  status text NOT NULL CHECK (status IN ('pending','active','disconnected')),
  state_digest text NOT NULL UNIQUE CHECK (state_digest ~ '^[0-9a-f]{64}$'),
  session_binding_digest text CHECK (
    session_binding_digest IS NULL OR session_binding_digest ~ '^[0-9a-f]{64}$'
  ),
  state_expires_at timestamptz NOT NULL,
  state_consumed_at timestamptz,
  google_subject_digest text CHECK (
    google_subject_digest IS NULL OR google_subject_digest ~ '^[0-9a-f]{64}$'
  ),
  email_label text,
  granted_scopes text[] NOT NULL DEFAULT '{}',
  refresh_token_envelope text,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (id,household_id,owner_adult_id),
  FOREIGN KEY (household_id,owner_adult_id) REFERENCES people(household_id,id),
  CHECK (
    (status='pending' AND session_binding_digest IS NOT NULL
      AND google_subject_digest IS NULL AND email_label IS NULL AND refresh_token_envelope IS NULL)
    OR
    (status='active' AND state_consumed_at IS NOT NULL AND session_binding_digest IS NULL
      AND google_subject_digest IS NOT NULL AND email_label IS NOT NULL
      AND refresh_token_envelope IS NOT NULL)
    OR
    (status='disconnected' AND refresh_token_envelope IS NULL)
  )
);
CREATE UNIQUE INDEX google_one_active_per_adult
  ON google_connections(household_id,owner_adult_id)
  WHERE status='active';
CREATE UNIQUE INDEX google_active_subject_unique ON google_connections(google_subject_digest)
  WHERE status='active';

CREATE TABLE calendar_actions (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  basis_source_id uuid REFERENCES sources(id) ON DELETE SET NULL,
  approval_source_id uuid REFERENCES sources(id) ON DELETE SET NULL,
  approval_prompt_source_id uuid REFERENCES sources(id) ON DELETE SET NULL,
  payload jsonb NOT NULL,
  provider_event_id text,
  status text NOT NULL DEFAULT 'offered'
    CHECK (status IN ('offered','pending','committed','failed')),
  retry_at timestamptz NOT NULL DEFAULT now(),
  last_error text,
  provider_etag text,
  committed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT calendar_actions_mutation_shape CHECK (
    jsonb_typeof(payload)='object'
    AND payload->>'operation' IN ('create','update','delete')
    AND (
      (payload->>'operation'='create'
        AND jsonb_typeof(payload->'event')='object'
        AND jsonb_typeof(payload->'target')='null')
      OR
      (payload->>'operation'='update'
        AND jsonb_typeof(payload->'event')='object'
        AND jsonb_typeof(payload->'target')='object')
      OR
      (payload->>'operation'='delete'
        AND jsonb_typeof(payload->'event')='null'
        AND jsonb_typeof(payload->'target')='object')
    )
  ),
  CONSTRAINT calendar_actions_authority_state CHECK (
    basis_source_id IS NOT NULL
    AND (
      (status='offered'
        AND payload->>'operation'='create'
        AND approval_prompt_source_id IS NOT NULL
        AND approval_source_id IS NULL
        AND provider_event_id IS NULL
        AND provider_etag IS NULL
        AND committed_at IS NULL
        AND last_error IS NULL)
      OR
      (status='pending'
        AND provider_event_id IS NULL
        AND provider_etag IS NULL
        AND committed_at IS NULL
        AND (
          (approval_source_id IS NOT NULL AND (
            approval_prompt_source_id IS NOT NULL
            OR approval_source_id=basis_source_id
          ))
          OR
          (approval_source_id IS NULL
            AND approval_prompt_source_id IS NULL
            AND payload->>'operation'='create')
        ))
      OR
      (status='committed'
        AND provider_event_id IS NOT NULL
        AND provider_etag IS NOT NULL
        AND committed_at IS NOT NULL
        AND (
          (approval_source_id IS NOT NULL AND (
            approval_prompt_source_id IS NOT NULL
            OR approval_source_id=basis_source_id
          ))
          OR
          (approval_source_id IS NULL
            AND approval_prompt_source_id IS NULL
            AND payload->>'operation'='create')
        ))
      OR
      (status='failed'
        AND provider_event_id IS NULL
        AND provider_etag IS NULL
        AND last_error IS NOT NULL
        AND committed_at IS NULL
        AND (
          (approval_source_id IS NOT NULL AND (
            approval_prompt_source_id IS NOT NULL
            OR approval_source_id=basis_source_id
          ))
          OR
          (approval_source_id IS NULL
            AND approval_prompt_source_id IS NULL
            AND payload->>'operation'='create')
        ))
    )
  )
);
CREATE INDEX calendar_actions_due ON calendar_actions(retry_at,id)
  WHERE status='pending';
CREATE UNIQUE INDEX calendar_actions_prompt_unique
  ON calendar_actions(approval_prompt_source_id)
  WHERE approval_prompt_source_id IS NOT NULL;

CREATE TABLE proactive_work (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  kind text NOT NULL CHECK (kind IN (
    'initial_private_review','initial_household_briefing','personal_google_poll',
    'family_calendar_poll','finite_monitor','interest_monitor'
  )),
  visibility text NOT NULL CHECK (visibility IN ('private','household')),
  owner_adult_id uuid,
  objective text,
  why text,
  current_conclusion text,
  end_condition text,
  discovery_terms text[] NOT NULL DEFAULT '{}',
  gmail_cursor text,
  calendar_cursor text,
  briefing_candidates jsonb NOT NULL DEFAULT '[]'::jsonb
    CHECK (jsonb_typeof(briefing_candidates)='array'),
  status text NOT NULL CHECK (status IN ('active','paused','completed')),
  next_check_at timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (household_id,owner_adult_id) REFERENCES people(household_id,id),
  CHECK (
    (visibility='private' AND owner_adult_id IS NOT NULL)
    OR
    (visibility='household' AND owner_adult_id IS NULL)
  ),
  CHECK (
    (status='active' AND next_check_at IS NOT NULL)
    OR
    (status<>'active' AND next_check_at IS NULL)
  ),
  CHECK (
    kind<>'initial_private_review' OR (
      visibility='private' AND objective IS NULL AND why IS NULL AND current_conclusion IS NULL
      AND end_condition IS NULL AND cardinality(discovery_terms)=0
      AND gmail_cursor IS NULL AND calendar_cursor IS NULL
      AND status IN ('active','completed')
    )
  ),
  CHECK (
    kind<>'initial_household_briefing' OR (
      visibility='household' AND objective IS NULL AND why IS NULL AND current_conclusion IS NULL
      AND end_condition IS NULL AND cardinality(discovery_terms)=0
      AND gmail_cursor IS NULL AND calendar_cursor IS NULL
      AND jsonb_array_length(briefing_candidates)=0
      AND status IN ('active','completed')
    )
  ),
  CHECK (
    kind<>'personal_google_poll' OR (
      visibility='private' AND objective IS NULL AND why IS NULL AND current_conclusion IS NULL
      AND end_condition IS NULL AND cardinality(discovery_terms)=0
      AND jsonb_array_length(briefing_candidates)=0
      AND status IN ('active','paused') AND gmail_cursor IS NOT NULL AND calendar_cursor IS NOT NULL
    )
  ),
  CHECK (
    kind<>'family_calendar_poll' OR (
      visibility='household' AND objective IS NULL AND why IS NULL AND current_conclusion IS NULL
      AND end_condition IS NULL AND cardinality(discovery_terms)=0
      AND gmail_cursor IS NULL AND jsonb_array_length(briefing_candidates)=0
      AND status IN ('active','paused') AND calendar_cursor IS NOT NULL
    )
  ),
  CHECK (
    kind<>'finite_monitor' OR (
      gmail_cursor IS NULL AND calendar_cursor IS NULL AND objective IS NOT NULL
      AND why IS NOT NULL AND current_conclusion IS NOT NULL
      AND end_condition IS NOT NULL AND cardinality(discovery_terms)=0
      AND jsonb_array_length(briefing_candidates)=0
      AND status IN ('active','paused')
    )
  ),
  CHECK (
    kind<>'interest_monitor' OR (
      visibility='household' AND gmail_cursor IS NULL AND calendar_cursor IS NULL
      AND objective IS NOT NULL AND why IS NOT NULL AND current_conclusion IS NOT NULL
      AND end_condition IS NULL AND cardinality(discovery_terms)>0
      AND jsonb_array_length(briefing_candidates)=0
      AND status IN ('active','paused')
    )
  ),
  CHECK (kind='initial_private_review' OR jsonb_array_length(briefing_candidates)=0)
);
CREATE INDEX proactive_work_due ON proactive_work(next_check_at,id)
  WHERE status='active';
CREATE UNIQUE INDEX proactive_work_initial_review_adult
  ON proactive_work(household_id,owner_adult_id)
  WHERE kind='initial_private_review';
CREATE UNIQUE INDEX proactive_work_personal_poll_adult
  ON proactive_work(household_id,owner_adult_id)
  WHERE kind='personal_google_poll';
CREATE UNIQUE INDEX proactive_work_family_calendar_poll
  ON proactive_work(household_id)
  WHERE kind='family_calendar_poll';

CREATE TABLE proactive_work_sources (
  work_id uuid NOT NULL REFERENCES proactive_work(id) ON DELETE CASCADE,
  source_id uuid NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  PRIMARY KEY (work_id,source_id)
);
