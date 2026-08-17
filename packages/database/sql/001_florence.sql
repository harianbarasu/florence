CREATE TABLE households (
  id uuid PRIMARY KEY, singleton boolean NOT NULL DEFAULT true UNIQUE CHECK (singleton),
  name text NOT NULL CHECK (length(trim(name)) > 0),
  time_zone text NOT NULL CHECK (length(trim(time_zone)) > 0),
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE people (
  id uuid PRIMARY KEY, household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  kind text NOT NULL CHECK (kind IN ('adult','child')),
  role text NOT NULL CHECK (role IN ('steward','caregiver','dependent')), adult_slot smallint,
  display_name text NOT NULL CHECK (length(trim(display_name)) > 0),
  status text NOT NULL CHECK (status IN ('planned','verified','represented')),
  identity_subject_digest text, consent_version text, consented_at timestamptz,
  guardian_attested_at timestamptz, invitation_digest text, invitation_expires_at timestamptz,
  invitation_consumed_at timestamptz, profile jsonb NOT NULL DEFAULT '{}'::jsonb,
  preferences jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (household_id,adult_slot), UNIQUE (household_id,id),
  UNIQUE (household_id,id,identity_subject_digest),
  CHECK (identity_subject_digest IS NULL OR identity_subject_digest ~ '^[0-9a-f]{64}$'),
  CHECK (invitation_digest IS NULL OR invitation_digest ~ '^[0-9a-f]{64}$'),
  CHECK ((kind='adult' AND adult_slot IN (1,2) AND role IN ('steward','caregiver')
      AND status IN ('planned','verified')) OR (kind='child' AND adult_slot IS NULL
      AND role='dependent' AND status='represented' AND identity_subject_digest IS NULL)),
  CHECK ((identity_subject_digest IS NULL AND consent_version IS NULL AND consented_at IS NULL)
    OR (identity_subject_digest IS NOT NULL AND consent_version IS NOT NULL AND consented_at IS NOT NULL))
);
CREATE UNIQUE INDEX people_identity_unique ON people(identity_subject_digest)
  WHERE identity_subject_digest IS NOT NULL;
CREATE UNIQUE INDEX people_invitation_unique ON people(invitation_digest)
  WHERE invitation_digest IS NOT NULL;

CREATE TABLE linq_channels (
  id uuid PRIMARY KEY, household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  audience text NOT NULL CHECK (audience IN ('private','group')), provider_conversation_id text NOT NULL,
  adult_one_id uuid NOT NULL, identity_one_digest text NOT NULL CHECK (identity_one_digest ~ '^[0-9a-f]{64}$'),
  adult_two_id uuid, identity_two_digest text,
  authority_digest text NOT NULL CHECK (authority_digest ~ '^[0-9a-f]{64}$'),
  bound_at timestamptz NOT NULL, revoked_at timestamptz, stopped_at timestamptz,
  FOREIGN KEY (household_id,adult_one_id,identity_one_digest)
    REFERENCES people(household_id,id,identity_subject_digest),
  FOREIGN KEY (household_id,adult_two_id,identity_two_digest)
    REFERENCES people(household_id,id,identity_subject_digest),
  CHECK ((audience='private' AND adult_two_id IS NULL AND identity_two_digest IS NULL)
    OR (audience='group' AND adult_two_id IS NOT NULL AND identity_two_digest IS NOT NULL
      AND adult_one_id < adult_two_id AND identity_one_digest <> identity_two_digest))
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
  id uuid PRIMARY KEY, household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  kind text NOT NULL CHECK (kind IN ('linq_message','gmail','google_file','document','web')),
  visibility text NOT NULL CHECK (visibility IN ('private','household')), owner_adult_id uuid,
  external_key text NOT NULL, parent_source_id uuid REFERENCES sources(id) ON DELETE SET NULL,
  label text NOT NULL, metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  occurred_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (household_id,kind,external_key),
  FOREIGN KEY (household_id,owner_adult_id) REFERENCES people(household_id,id),
  CHECK ((visibility='household' AND owner_adult_id IS NULL)
    OR (visibility='private' AND owner_adult_id IS NOT NULL))
);

CREATE TABLE messages (
  source_id uuid PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
  channel_id uuid NOT NULL REFERENCES linq_channels(id), direction text NOT NULL CHECK (direction IN ('inbound','outbound')),
  sender_adult_id uuid REFERENCES people(id), move_kind text NOT NULL CHECK (move_kind IN ('message','reply','reaction')),
  text text, reaction text, images jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(images)='array'),
  has_attachments boolean NOT NULL DEFAULT false,
  provider_event_id text, provider_message_id text,
  reply_to_source_id uuid REFERENCES messages(source_id) ON DELETE SET NULL,
  turn_id uuid NOT NULL, turn_part smallint NOT NULL CHECK (turn_part BETWEEN -1 AND 2), idempotency_key text,
  not_before timestamptz NOT NULL DEFAULT now(), status text NOT NULL CHECK (status IN ('received','handled','pending','sending','sent','failed')),
  handled_at timestamptz, sending_at timestamptz, sent_at timestamptz, receipt_detail jsonb, retry_at timestamptz, last_error text,
  UNIQUE (turn_id,turn_part),
  CHECK ((move_kind='reaction' AND turn_part=-1 AND reaction IS NOT NULL AND text IS NULL)
    OR (move_kind IN ('message','reply') AND turn_part>=0 AND reaction IS NULL
      AND (text IS NOT NULL OR has_attachments))),
  CHECK ((direction='inbound' AND sender_adult_id IS NOT NULL AND provider_event_id IS NOT NULL
      AND provider_message_id IS NOT NULL AND idempotency_key IS NULL AND status IN ('received','handled'))
    OR (direction='outbound' AND sender_adult_id IS NULL AND provider_event_id IS NULL
      AND idempotency_key IS NOT NULL AND status IN ('pending','sending','sent','failed'))),
  CHECK (status <> 'sent' OR (provider_message_id IS NOT NULL AND sent_at IS NOT NULL))
);
CREATE UNIQUE INDEX messages_provider_event_unique ON messages(provider_event_id) WHERE provider_event_id IS NOT NULL;
CREATE UNIQUE INDEX messages_provider_message_unique ON messages(channel_id,provider_message_id) WHERE provider_message_id IS NOT NULL;
CREATE UNIQUE INDEX messages_idempotency_unique ON messages(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX messages_inbound_due ON messages(COALESCE(retry_at,not_before),source_id) WHERE status='received';
CREATE INDEX messages_outbound_due ON messages(COALESCE(retry_at,not_before),source_id) WHERE status='pending';

CREATE TABLE documents (
  source_id uuid PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
  saved_by_adult_id uuid NOT NULL REFERENCES people(id), filename text NOT NULL, mime_type text NOT NULL,
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'), retained boolean NOT NULL DEFAULT false,
  content_envelope bytea, discard_after timestamptz,
  CHECK (NOT retained OR (content_envelope IS NOT NULL AND discard_after IS NULL))
);

CREATE TABLE facts (
  id uuid PRIMARY KEY, household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  subject_person_id uuid REFERENCES people(id) ON DELETE CASCADE,
  kind text NOT NULL CHECK (kind IN ('identity','school','caregiver','activity','schedule','address','phone','contact','preference','safety','general')),
  slot text NOT NULL, label text NOT NULL, value jsonb NOT NULL,
  visibility text NOT NULL CHECK (visibility IN ('private','household')), owner_adult_id uuid REFERENCES people(id),
  created_at timestamptz NOT NULL DEFAULT now(), corrected_at timestamptz, updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((visibility='household' AND owner_adult_id IS NULL) OR (visibility='private' AND owner_adult_id IS NOT NULL))
);
CREATE UNIQUE INDEX facts_current_unique ON facts(household_id,slot,visibility,
  COALESCE(owner_adult_id,'00000000-0000-0000-0000-000000000000'::uuid));
CREATE TABLE fact_sources (
  fact_id uuid NOT NULL REFERENCES facts(id) ON DELETE CASCADE,
  source_id uuid NOT NULL REFERENCES sources(id) ON DELETE CASCADE, PRIMARY KEY (fact_id,source_id)
);

CREATE TABLE follow_ups (
  id uuid PRIMARY KEY, household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  channel_id uuid NOT NULL REFERENCES linq_channels(id), dedupe_key text NOT NULL UNIQUE,
  text text NOT NULL, due_at timestamptz NOT NULL,
  status text NOT NULL DEFAULT 'scheduled' CHECK (status IN ('scheduled','queued','sent','cancelled')),
  sent_message_source_id uuid UNIQUE REFERENCES messages(source_id) ON DELETE SET NULL,
  cancelled_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((status='scheduled' AND sent_message_source_id IS NULL AND cancelled_at IS NULL)
    OR (status IN ('queued','sent') AND sent_message_source_id IS NOT NULL)
    OR (status='cancelled' AND cancelled_at IS NOT NULL))
);
CREATE INDEX follow_ups_due ON follow_ups(due_at,id) WHERE status='scheduled';
CREATE TABLE follow_up_sources (
  follow_up_id uuid NOT NULL REFERENCES follow_ups(id) ON DELETE CASCADE,
  source_id uuid NOT NULL REFERENCES sources(id) ON DELETE CASCADE, PRIMARY KEY (follow_up_id,source_id)
);

CREATE TABLE google_connections (
  id uuid PRIMARY KEY, household_id uuid NOT NULL, owner_adult_id uuid NOT NULL,
  status text NOT NULL CHECK (status IN ('pending','active','disconnected')),
  state_digest text NOT NULL UNIQUE CHECK (state_digest ~ '^[0-9a-f]{64}$'),
  session_binding_digest text CHECK (session_binding_digest IS NULL OR session_binding_digest ~ '^[0-9a-f]{64}$'),
  state_expires_at timestamptz NOT NULL, state_consumed_at timestamptz,
  google_subject_digest text CHECK (google_subject_digest IS NULL OR google_subject_digest ~ '^[0-9a-f]{64}$'),
  email_label text, granted_scopes text[] NOT NULL DEFAULT '{}', refresh_token_envelope text, last_error text,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (id,household_id,owner_adult_id),
  FOREIGN KEY (household_id,owner_adult_id) REFERENCES people(household_id,id),
  CHECK ((status='pending' AND session_binding_digest IS NOT NULL AND google_subject_digest IS NULL
      AND email_label IS NULL AND refresh_token_envelope IS NULL)
    OR (status='active' AND state_consumed_at IS NOT NULL AND session_binding_digest IS NULL
      AND google_subject_digest IS NOT NULL AND email_label IS NOT NULL AND refresh_token_envelope IS NOT NULL)
    OR (status='disconnected' AND refresh_token_envelope IS NULL))
);
CREATE UNIQUE INDEX google_one_active_per_adult ON google_connections(household_id,owner_adult_id) WHERE status='active';
CREATE UNIQUE INDEX google_active_subject_unique ON google_connections(google_subject_digest) WHERE status='active';

CREATE TABLE calendar_actions (
  id uuid PRIMARY KEY, household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  owner_adult_id uuid NOT NULL, connection_id uuid NOT NULL,
  basis_source_id uuid REFERENCES sources(id) ON DELETE SET NULL,
  approval_source_id uuid REFERENCES sources(id) ON DELETE SET NULL,
  operation text NOT NULL CHECK (operation IN ('create','update')), action_id uuid NOT NULL UNIQUE,
  approval_digest text UNIQUE CHECK (approval_digest IS NULL OR approval_digest ~ '^[0-9a-f]{64}$'),
  payload jsonb NOT NULL, payload_digest text NOT NULL CHECK (payload_digest ~ '^[0-9a-f]{64}$'),
  provider_event_id text, status text NOT NULL DEFAULT 'offered' CHECK (status IN ('offered','pending','committed','failed')),
  retry_at timestamptz NOT NULL DEFAULT now(), last_error text, provider_etag text,
  proof_digest text CHECK (proof_digest IS NULL OR proof_digest ~ '^[0-9a-f]{64}$'),
  proof jsonb, committed_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (connection_id,household_id,owner_adult_id) REFERENCES google_connections(id,household_id,owner_adult_id),
  CHECK ((status='offered' AND basis_source_id IS NOT NULL AND approval_source_id IS NULL
      AND approval_digest IS NULL AND committed_at IS NULL)
    OR (status='pending' AND approval_source_id IS NOT NULL AND approval_digest IS NOT NULL AND committed_at IS NULL)
    OR (status='committed' AND approval_source_id IS NOT NULL AND approval_digest IS NOT NULL
      AND provider_event_id IS NOT NULL AND provider_etag IS NOT NULL AND proof_digest IS NOT NULL
      AND proof IS NOT NULL AND committed_at IS NOT NULL)
    OR (status='failed' AND approval_source_id IS NOT NULL AND approval_digest IS NOT NULL
      AND last_error IS NOT NULL AND committed_at IS NULL))
);
CREATE INDEX calendar_actions_due ON calendar_actions(retry_at,id) WHERE status='pending';
