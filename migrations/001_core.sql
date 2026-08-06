-- Florence's normalized PostgreSQL authority model.
-- IDs and sensitive-field encryption are supplied by the application. Provider and
-- worker-framework types intentionally do not appear in this schema.

-- Identity and relationships -------------------------------------------------

CREATE TABLE people (
  id uuid PRIMARY KEY,
  status text NOT NULL CHECK (
    status IN ('provisional', 'registered', 'stopped', 'deletion_fenced', 'merged', 'deleted')
  ),
  display_name_ciphertext bytea,
  display_name_key_version text,
  timezone text,
  quiet_hours jsonb NOT NULL DEFAULT '{}'::jsonb,
  consented_at timestamptz,
  registered_at timestamptz,
  merged_into_person_id uuid REFERENCES people(id),
  authority_version bigint NOT NULL DEFAULT 1 CHECK (authority_version > 0),
  control_epoch bigint NOT NULL DEFAULT 1 CHECK (control_epoch > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (status = 'merged' AND merged_into_person_id IS NOT NULL)
    OR (status <> 'merged' AND merged_into_person_id IS NULL)
  ),
  CHECK ((display_name_ciphertext IS NULL) = (display_name_key_version IS NULL))
);

CREATE TABLE person_identities (
  id uuid PRIMARY KEY,
  person_id uuid NOT NULL REFERENCES people(id),
  kind text NOT NULL CHECK (kind IN ('phone', 'email', 'provider_handle')),
  issuer text NOT NULL,
  subject_digest text NOT NULL CHECK (subject_digest ~ '^[a-f0-9]{64}$'),
  status text NOT NULL CHECK (status IN ('observed', 'pending_claim', 'verified', 'revoked')),
  authority_version bigint NOT NULL DEFAULT 1 CHECK (authority_version > 0),
  observed_at timestamptz NOT NULL,
  verified_at timestamptz,
  revoked_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (issuer, kind, subject_digest),
  CHECK (
    (status IN ('observed', 'pending_claim') AND verified_at IS NULL AND revoked_at IS NULL)
    OR (status = 'verified' AND verified_at IS NOT NULL AND revoked_at IS NULL)
    OR (status = 'revoked' AND revoked_at IS NOT NULL)
  )
);

CREATE UNIQUE INDEX person_identities_one_active_claimed_owner_idx
  ON person_identities (issuer, kind, subject_digest)
  WHERE status = 'verified';
CREATE INDEX person_identities_person_idx ON person_identities (person_id, status);

CREATE TABLE person_sessions (
  id uuid PRIMARY KEY,
  person_id uuid NOT NULL REFERENCES people(id) ON DELETE CASCADE,
  session_digest text NOT NULL UNIQUE CHECK (session_digest ~ '^[a-f0-9]{64}$'),
  person_control_epoch bigint NOT NULL CHECK (person_control_epoch > 0),
  assurance_kind text NOT NULL DEFAULT 'base' CHECK (
    assurance_kind IN ('base', 'google_connect', 'account_controls')
  ),
  assurance_expires_at timestamptz,
  idle_expires_at timestamptz NOT NULL,
  absolute_expires_at timestamptz NOT NULL,
  last_seen_at timestamptz NOT NULL,
  revoked_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (idle_expires_at <= absolute_expires_at),
  CHECK (
    (assurance_kind = 'base' AND assurance_expires_at IS NULL)
    OR (assurance_kind <> 'base' AND assurance_expires_at IS NOT NULL)
  )
);

CREATE INDEX person_sessions_active_idx
  ON person_sessions (person_id, idle_expires_at)
  WHERE revoked_at IS NULL;

CREATE TABLE auth_handoffs (
  id uuid PRIMARY KEY,
  person_id uuid NOT NULL REFERENCES people(id) ON DELETE CASCADE,
  private_identity_id uuid NOT NULL REFERENCES person_identities(id),
  private_conversation_id uuid,
  token_digest text NOT NULL UNIQUE CHECK (token_digest ~ '^[a-f0-9]{64}$'),
  purpose text NOT NULL,
  identity_authority_version bigint NOT NULL CHECK (identity_authority_version > 0),
  context_ciphertext bytea,
  context_key_version text,
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((context_ciphertext IS NULL) = (context_key_version IS NULL))
);

CREATE TABLE households (
  id uuid PRIMARY KEY,
  name_ciphertext bytea,
  name_key_version text,
  timezone text NOT NULL,
  status text NOT NULL CHECK (status IN ('onboarding', 'active', 'paused', 'deletion_fenced', 'deleted')),
  membership_version bigint NOT NULL DEFAULT 1 CHECK (membership_version > 0),
  control_epoch bigint NOT NULL DEFAULT 1 CHECK (control_epoch > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((name_ciphertext IS NULL) = (name_key_version IS NULL))
);

CREATE TABLE household_memberships (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  person_id uuid NOT NULL REFERENCES people(id),
  role text NOT NULL CHECK (role IN ('steward', 'caregiver', 'participant', 'dependent')),
  status text NOT NULL CHECK (status IN ('invited', 'active', 'suspended', 'left', 'revoked')),
  consented_at timestamptz,
  joined_at timestamptz,
  ended_at timestamptz,
  version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (household_id, person_id),
  CHECK (
    (status = 'active' AND joined_at IS NOT NULL AND ended_at IS NULL)
    OR (status IN ('suspended', 'left', 'revoked') AND ended_at IS NOT NULL)
    OR status = 'invited'
  )
);

CREATE INDEX household_memberships_active_idx
  ON household_memberships (household_id, role)
  WHERE status = 'active';

CREATE TABLE membership_capabilities (
  membership_id uuid NOT NULL REFERENCES household_memberships(id) ON DELETE CASCADE,
  capability text NOT NULL,
  scope jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL CHECK (status IN ('active', 'revoked')),
  granted_by_membership_id uuid REFERENCES household_memberships(id),
  granted_at timestamptz NOT NULL,
  revoked_at timestamptz,
  PRIMARY KEY (membership_id, capability),
  CHECK ((status = 'revoked') = (revoked_at IS NOT NULL))
);

CREATE TABLE invitations (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  invited_by_membership_id uuid NOT NULL REFERENCES household_memberships(id),
  invitee_identity_id uuid REFERENCES person_identities(id),
  invitee_subject_digest text NOT NULL CHECK (invitee_subject_digest ~ '^[a-f0-9]{64}$'),
  token_digest text NOT NULL UNIQUE CHECK (token_digest ~ '^[a-f0-9]{64}$'),
  requested_role text NOT NULL CHECK (
    requested_role IN ('steward', 'caregiver', 'participant', 'dependent')
  ),
  requested_capabilities text[] NOT NULL DEFAULT '{}',
  household_membership_version bigint NOT NULL CHECK (household_membership_version > 0),
  status text NOT NULL CHECK (status IN ('pending', 'accepted', 'declined', 'expired', 'revoked')),
  expires_at timestamptz NOT NULL,
  accepted_by_person_id uuid REFERENCES people(id),
  accepted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (status = 'accepted' AND accepted_by_person_id IS NOT NULL AND accepted_at IS NOT NULL)
    OR (status <> 'accepted' AND accepted_by_person_id IS NULL AND accepted_at IS NULL)
  )
);

CREATE UNIQUE INDEX invitations_one_pending_target_idx
  ON invitations (household_id, invitee_subject_digest)
  WHERE status = 'pending';

CREATE TABLE invitation_approvals (
  invitation_id uuid NOT NULL REFERENCES invitations(id) ON DELETE CASCADE,
  approver_membership_id uuid NOT NULL REFERENCES household_memberships(id),
  approved_at timestamptz,
  PRIMARY KEY (invitation_id, approver_membership_id)
);

-- Conversations --------------------------------------------------------------

CREATE TABLE conversations (
  id uuid PRIMARY KEY,
  household_id uuid REFERENCES households(id) ON DELETE CASCADE,
  kind text NOT NULL CHECK (kind IN ('direct', 'group')),
  purpose text NOT NULL,
  status text NOT NULL CHECK (status IN ('active', 'paused', 'deletion_fenced', 'deleted')),
  current_epoch_id uuid,
  authority_version bigint NOT NULL DEFAULT 1 CHECK (authority_version > 0),
  control_epoch bigint NOT NULL DEFAULT 1 CHECK (control_epoch > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE conversation_channels (
  id uuid PRIMARY KEY,
  conversation_id uuid NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  provider text NOT NULL,
  external_channel_id text NOT NULL,
  status text NOT NULL CHECK (status IN ('active', 'paused', 'revoked')),
  bound_at timestamptz NOT NULL,
  revoked_at timestamptz,
  UNIQUE (provider, external_channel_id),
  CHECK ((status = 'revoked') = (revoked_at IS NOT NULL))
);

CREATE TABLE participant_epochs (
  id uuid PRIMARY KEY,
  conversation_id uuid NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  sequence bigint NOT NULL CHECK (sequence > 0),
  participant_set_digest text NOT NULL CHECK (participant_set_digest ~ '^[a-f0-9]{64}$'),
  authority_digest text NOT NULL UNIQUE CHECK (authority_digest ~ '^[a-f0-9]{64}$'),
  change_reason text NOT NULL,
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  UNIQUE (conversation_id, sequence),
  CHECK (ended_at IS NULL OR ended_at >= started_at)
);

CREATE UNIQUE INDEX participant_epochs_one_current_idx
  ON participant_epochs (conversation_id)
  WHERE ended_at IS NULL;

ALTER TABLE conversations
  ADD CONSTRAINT conversations_current_epoch_fk
  FOREIGN KEY (current_epoch_id) REFERENCES participant_epochs(id);

ALTER TABLE auth_handoffs
  ADD CONSTRAINT auth_handoffs_private_conversation_fk
  FOREIGN KEY (private_conversation_id) REFERENCES conversations(id);

CREATE TABLE epoch_participants (
  participant_epoch_id uuid NOT NULL REFERENCES participant_epochs(id) ON DELETE CASCADE,
  person_identity_id uuid NOT NULL REFERENCES person_identities(id),
  person_id uuid NOT NULL REFERENCES people(id),
  registration_status text NOT NULL CHECK (registration_status IN ('provisional', 'registered')),
  consented_at timestamptz,
  added_at timestamptz NOT NULL,
  PRIMARY KEY (participant_epoch_id, person_identity_id),
  UNIQUE (participant_epoch_id, person_id)
);

CREATE TABLE participant_policies (
  id uuid PRIMARY KEY,
  conversation_id uuid NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  person_id uuid NOT NULL REFERENCES people(id),
  version bigint NOT NULL CHECK (version > 0),
  status text NOT NULL CHECK (status IN ('active', 'superseded', 'revoked')),
  allow_content_processing boolean NOT NULL,
  allow_direct_responses boolean NOT NULL,
  allow_proactive_writes boolean NOT NULL,
  retention_seconds integer NOT NULL CHECK (retention_seconds BETWEEN 0 AND 2592000),
  changed_by_person_id uuid NOT NULL REFERENCES people(id),
  approval_participant_digest text CHECK (
    approval_participant_digest IS NULL OR approval_participant_digest ~ '^[a-f0-9]{64}$'
  ),
  effective_at timestamptz NOT NULL,
  superseded_at timestamptz,
  UNIQUE (conversation_id, person_id, version),
  CHECK ((status = 'active') = (superseded_at IS NULL))
);

CREATE UNIQUE INDEX participant_policies_one_active_idx
  ON participant_policies (conversation_id, person_id)
  WHERE status = 'active';

CREATE TABLE conversation_rules (
  id uuid PRIMARY KEY,
  conversation_id uuid NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  rule_key text NOT NULL,
  version bigint NOT NULL CHECK (version > 0),
  status text NOT NULL CHECK (status IN ('candidate', 'active', 'superseded', 'revoked')),
  purpose text NOT NULL,
  allowed_operations text[] NOT NULL DEFAULT '{}',
  participant_set_digest text NOT NULL CHECK (participant_set_digest ~ '^[a-f0-9]{64}$'),
  approval_participant_digest text NOT NULL CHECK (
    approval_participant_digest ~ '^[a-f0-9]{64}$'
  ),
  created_by_person_id uuid NOT NULL REFERENCES people(id),
  effective_at timestamptz,
  ended_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (conversation_id, rule_key, version),
  CHECK ((status = 'active') = (effective_at IS NOT NULL AND ended_at IS NULL))
);

CREATE UNIQUE INDEX conversation_rules_one_active_idx
  ON conversation_rules (conversation_id, rule_key)
  WHERE status = 'active';

CREATE TABLE conversation_rule_approvals (
  conversation_rule_id uuid NOT NULL REFERENCES conversation_rules(id) ON DELETE CASCADE,
  participant_epoch_id uuid NOT NULL REFERENCES participant_epochs(id),
  person_id uuid NOT NULL REFERENCES people(id),
  participant_set_digest text NOT NULL CHECK (participant_set_digest ~ '^[a-f0-9]{64}$'),
  approved_at timestamptz NOT NULL,
  PRIMARY KEY (conversation_rule_id, person_id)
);

CREATE TABLE channel_suppressions (
  id uuid PRIMARY KEY,
  conversation_id uuid NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  created_by_person_id uuid NOT NULL REFERENCES people(id),
  kind text NOT NULL CHECK (
    kind IN ('stop', 'pause', 'read_only', 'retention_cap', 'deletion_fence', 'safety_hold')
  ),
  retention_seconds integer CHECK (retention_seconds BETWEEN 0 AND 2592000),
  reason text NOT NULL,
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL,
  lifted_at timestamptz,
  CHECK ((kind = 'retention_cap') = (retention_seconds IS NOT NULL)),
  CHECK (active = (lifted_at IS NULL))
);

CREATE INDEX channel_suppressions_active_idx
  ON channel_suppressions (conversation_id, kind)
  WHERE active;

-- Participant membership is append-only within an epoch. Closing an epoch may
-- set ended_at, but its identity, sequence, and participant digest cannot change.
CREATE FUNCTION reject_participant_epoch_identity_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.id <> OLD.id
    OR NEW.conversation_id <> OLD.conversation_id
    OR NEW.sequence <> OLD.sequence
    OR NEW.participant_set_digest <> OLD.participant_set_digest
    OR NEW.authority_digest <> OLD.authority_digest
    OR NEW.started_at <> OLD.started_at
  THEN
    RAISE EXCEPTION 'participant epoch identity is immutable';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER participant_epochs_identity_immutable
BEFORE UPDATE ON participant_epochs
FOR EACH ROW EXECUTE FUNCTION reject_participant_epoch_identity_update();

CREATE FUNCTION reject_epoch_participant_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.participant_epoch_id <> OLD.participant_epoch_id
    OR NEW.person_identity_id <> OLD.person_identity_id
    OR NEW.person_id <> OLD.person_id
    OR NEW.added_at <> OLD.added_at
  THEN
    RAISE EXCEPTION 'participant epoch membership is immutable';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER epoch_participants_immutable
BEFORE UPDATE ON epoch_participants
FOR EACH ROW EXECUTE FUNCTION reject_epoch_participant_update();

-- Integrations and evidence ---------------------------------------------------

CREATE TABLE integrations (
  id uuid PRIMARY KEY,
  person_id uuid NOT NULL REFERENCES people(id),
  provider text NOT NULL,
  external_subject_digest text NOT NULL CHECK (external_subject_digest ~ '^[a-f0-9]{64}$'),
  status text NOT NULL CHECK (status IN ('active', 'paused', 'reauth_required', 'revoked', 'error')),
  credential_ciphertext bytea,
  credential_key_version text,
  control_epoch bigint NOT NULL DEFAULT 1 CHECK (control_epoch > 0),
  connected_at timestamptz NOT NULL,
  revoked_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((credential_ciphertext IS NULL) = (credential_key_version IS NULL)),
  CHECK (
    (status = 'revoked' AND credential_ciphertext IS NULL AND revoked_at IS NOT NULL)
    OR status <> 'revoked'
  )
);

CREATE UNIQUE INDEX integrations_one_active_google_subject_person_idx
  ON integrations (person_id, external_subject_digest)
  WHERE provider = 'google' AND status IN ('active', 'paused', 'reauth_required');

CREATE TABLE integration_grants (
  id uuid PRIMARY KEY,
  integration_id uuid NOT NULL REFERENCES integrations(id) ON DELETE CASCADE,
  grant_kind text NOT NULL,
  scope jsonb NOT NULL,
  destination_household_id uuid REFERENCES households(id) ON DELETE CASCADE,
  destination_conversation_id uuid REFERENCES conversations(id) ON DELETE CASCADE,
  status text NOT NULL CHECK (status IN ('active', 'revoked', 'expired')),
  version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
  expires_at timestamptz,
  revoked_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (NOT (destination_household_id IS NOT NULL AND destination_conversation_id IS NOT NULL))
);

CREATE TABLE oauth_attempts (
  id uuid PRIMARY KEY,
  person_id uuid NOT NULL REFERENCES people(id) ON DELETE CASCADE,
  provider text NOT NULL,
  state_digest text NOT NULL UNIQUE CHECK (state_digest ~ '^[a-f0-9]{64}$'),
  pkce_verifier_ciphertext bytea NOT NULL,
  key_version text NOT NULL,
  return_path text NOT NULL,
  person_control_epoch bigint NOT NULL CHECK (person_control_epoch > 0),
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sync_cursors (
  id uuid PRIMARY KEY,
  integration_id uuid NOT NULL REFERENCES integrations(id) ON DELETE CASCADE,
  resource_kind text NOT NULL,
  cursor_ciphertext bytea,
  cursor_key_version text,
  state text NOT NULL CHECK (state IN ('initial', 'active', 'exhausted', 'expired', 'error')),
  checkpoint_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (integration_id, resource_kind),
  CHECK ((cursor_ciphertext IS NULL) = (cursor_key_version IS NULL))
);

CREATE TABLE provider_events (
  id uuid PRIMARY KEY,
  provider text NOT NULL,
  provider_event_id text NOT NULL,
  event_type text NOT NULL,
  external_channel_id text,
  occurred_at timestamptz NOT NULL,
  received_at timestamptz NOT NULL,
  payload_digest text NOT NULL CHECK (payload_digest ~ '^[a-f0-9]{64}$'),
  envelope_ciphertext bytea NOT NULL,
  envelope_key_version text NOT NULL,
  admission_status text NOT NULL CHECK (
    admission_status IN ('verified', 'rejected', 'quarantined')
  ),
  processing_status text NOT NULL CHECK (
    processing_status IN ('pending', 'processing', 'processed', 'ignored', 'failed')
  ),
  processed_at timestamptz,
  error_code text,
  UNIQUE (provider, provider_event_id)
);

CREATE INDEX provider_events_pending_idx
  ON provider_events (processing_status, received_at)
  WHERE processing_status IN ('pending', 'processing');

CREATE TABLE source_objects (
  id uuid PRIMARY KEY,
  integration_id uuid REFERENCES integrations(id) ON DELETE CASCADE,
  provider text NOT NULL,
  external_object_id text NOT NULL,
  object_kind text NOT NULL,
  status text NOT NULL CHECK (status IN ('active', 'deleted', 'revoked')),
  latest_revision_number bigint NOT NULL DEFAULT 0 CHECK (latest_revision_number >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (provider, external_object_id)
);

CREATE TABLE source_revisions (
  id uuid PRIMARY KEY,
  source_object_id uuid NOT NULL REFERENCES source_objects(id) ON DELETE CASCADE,
  revision_number bigint NOT NULL CHECK (revision_number > 0),
  owner_person_id uuid REFERENCES people(id),
  participant_epoch_id uuid REFERENCES participant_epochs(id),
  scope_digest text NOT NULL CHECK (scope_digest ~ '^[a-f0-9]{64}$'),
  content_digest text NOT NULL CHECK (content_digest ~ '^[a-f0-9]{64}$'),
  content_ciphertext bytea,
  content_key_version text,
  occurred_at timestamptz NOT NULL,
  captured_at timestamptz NOT NULL,
  retention_until timestamptz NOT NULL,
  revoked_at timestamptz,
  UNIQUE (source_object_id, revision_number),
  CHECK ((owner_person_id IS NOT NULL)::integer + (participant_epoch_id IS NOT NULL)::integer = 1),
  CHECK ((content_ciphertext IS NULL) = (content_key_version IS NULL))
);

CREATE INDEX source_revisions_owner_idx ON source_revisions (owner_person_id, occurred_at DESC);
CREATE INDEX source_revisions_epoch_idx ON source_revisions (participant_epoch_id, occurred_at DESC);

CREATE TABLE source_blobs (
  id uuid PRIMARY KEY,
  source_revision_id uuid NOT NULL REFERENCES source_revisions(id) ON DELETE CASCADE,
  blob_kind text NOT NULL,
  content_digest text NOT NULL CHECK (content_digest ~ '^[a-f0-9]{64}$'),
  mime_type text NOT NULL,
  byte_length bigint NOT NULL CHECK (byte_length >= 0),
  ciphertext bytea NOT NULL,
  key_version text NOT NULL,
  retention_until timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_revision_id, blob_kind, content_digest)
);

CREATE TABLE source_derivatives (
  id uuid PRIMARY KEY,
  parent_source_revision_id uuid NOT NULL REFERENCES source_revisions(id) ON DELETE CASCADE,
  owner_person_id uuid REFERENCES people(id),
  participant_epoch_id uuid REFERENCES participant_epochs(id),
  kind text NOT NULL,
  scope_digest text NOT NULL CHECK (scope_digest ~ '^[a-f0-9]{64}$'),
  content_digest text NOT NULL CHECK (content_digest ~ '^[a-f0-9]{64}$'),
  content_ciphertext bytea NOT NULL,
  content_key_version text NOT NULL,
  retention_until timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((owner_person_id IS NOT NULL)::integer + (participant_epoch_id IS NOT NULL)::integer = 1),
  UNIQUE (parent_source_revision_id, kind, content_digest)
);

CREATE TABLE provenance_edges (
  id uuid PRIMARY KEY,
  parent_source_revision_id uuid NOT NULL REFERENCES source_revisions(id) ON DELETE CASCADE,
  child_derivative_id uuid NOT NULL REFERENCES source_derivatives(id) ON DELETE CASCADE,
  relation text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (parent_source_revision_id, child_derivative_id, relation)
);

CREATE FUNCTION enforce_derivative_scope_and_retention() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  parent source_revisions%ROWTYPE;
BEGIN
  SELECT * INTO STRICT parent FROM source_revisions WHERE id = NEW.parent_source_revision_id;
  IF NEW.owner_person_id IS DISTINCT FROM parent.owner_person_id
    OR NEW.participant_epoch_id IS DISTINCT FROM parent.participant_epoch_id
    OR NEW.scope_digest <> parent.scope_digest
    OR NEW.retention_until > parent.retention_until
  THEN
    RAISE EXCEPTION 'source derivative may not widen parent scope or retention';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER source_derivatives_scope_and_retention
BEFORE INSERT OR UPDATE ON source_derivatives
FOR EACH ROW EXECUTE FUNCTION enforce_derivative_scope_and_retention();

-- Knowledge and coordination -------------------------------------------------

CREATE TABLE knowledge_candidates (
  id uuid PRIMARY KEY,
  scope_kind text NOT NULL CHECK (scope_kind IN ('person', 'household', 'conversation')),
  owner_person_id uuid REFERENCES people(id) ON DELETE CASCADE,
  household_id uuid REFERENCES households(id) ON DELETE CASCADE,
  conversation_id uuid REFERENCES conversations(id) ON DELETE CASCADE,
  candidate_kind text NOT NULL,
  content_digest text NOT NULL CHECK (content_digest ~ '^[a-f0-9]{64}$'),
  content_ciphertext bytea NOT NULL,
  content_key_version text NOT NULL,
  evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  confidence numeric(5,4) NOT NULL CHECK (confidence BETWEEN 0 AND 1),
  status text NOT NULL CHECK (status IN ('pending', 'accepted', 'rejected', 'expired', 'revoked')),
  proposed_at timestamptz NOT NULL,
  reviewed_by_person_id uuid REFERENCES people(id),
  reviewed_at timestamptz,
  expires_at timestamptz,
  CHECK (
    (scope_kind = 'person' AND owner_person_id IS NOT NULL AND household_id IS NULL AND conversation_id IS NULL)
    OR (scope_kind = 'household' AND owner_person_id IS NULL AND household_id IS NOT NULL AND conversation_id IS NULL)
    OR (scope_kind = 'conversation' AND owner_person_id IS NULL AND household_id IS NULL AND conversation_id IS NOT NULL)
  )
);

CREATE TABLE memory_records (
  id uuid PRIMARY KEY,
  scope_kind text NOT NULL CHECK (scope_kind IN ('person', 'household', 'conversation')),
  owner_person_id uuid REFERENCES people(id) ON DELETE CASCADE,
  household_id uuid REFERENCES households(id) ON DELETE CASCADE,
  conversation_id uuid REFERENCES conversations(id) ON DELETE CASCADE,
  memory_key text NOT NULL,
  status text NOT NULL CHECK (status IN ('accepted', 'expired', 'revoked', 'forgotten')),
  current_revision_id uuid,
  version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
  expires_at timestamptz,
  revoked_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (scope_kind = 'person' AND owner_person_id IS NOT NULL AND household_id IS NULL AND conversation_id IS NULL)
    OR (scope_kind = 'household' AND owner_person_id IS NULL AND household_id IS NOT NULL AND conversation_id IS NULL)
    OR (scope_kind = 'conversation' AND owner_person_id IS NULL AND household_id IS NULL AND conversation_id IS NOT NULL)
  )
);

CREATE UNIQUE INDEX memory_records_person_key_idx
  ON memory_records (owner_person_id, memory_key)
  WHERE scope_kind = 'person' AND status = 'accepted';
CREATE UNIQUE INDEX memory_records_household_key_idx
  ON memory_records (household_id, memory_key)
  WHERE scope_kind = 'household' AND status = 'accepted';
CREATE UNIQUE INDEX memory_records_conversation_key_idx
  ON memory_records (conversation_id, memory_key)
  WHERE scope_kind = 'conversation' AND status = 'accepted';

CREATE TABLE memory_revisions (
  id uuid PRIMARY KEY,
  memory_record_id uuid NOT NULL REFERENCES memory_records(id) ON DELETE CASCADE,
  revision bigint NOT NULL CHECK (revision > 0),
  content_digest text NOT NULL CHECK (content_digest ~ '^[a-f0-9]{64}$'),
  content_ciphertext bytea NOT NULL,
  content_key_version text NOT NULL,
  scope_digest text NOT NULL CHECK (scope_digest ~ '^[a-f0-9]{64}$'),
  evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  accepted_by_person_id uuid REFERENCES people(id),
  effective_at timestamptz NOT NULL,
  ended_at timestamptz,
  UNIQUE (memory_record_id, revision)
);

ALTER TABLE memory_records
  ADD CONSTRAINT memory_records_current_revision_fk
  FOREIGN KEY (current_revision_id) REFERENCES memory_revisions(id);

CREATE TABLE routines (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  status text NOT NULL CHECK (status IN ('active', 'paused', 'retired')),
  current_revision bigint NOT NULL CHECK (current_revision > 0),
  version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (id, current_revision)
);

CREATE TABLE routine_revisions (
  id uuid NOT NULL UNIQUE,
  routine_id uuid NOT NULL REFERENCES routines(id) ON DELETE CASCADE,
  revision bigint NOT NULL CHECK (revision > 0),
  title_digest text NOT NULL CHECK (title_digest ~ '^[a-f0-9]{64}$'),
  minimum_shared_meaning_digest text NOT NULL CHECK (minimum_shared_meaning_digest ~ '^[a-f0-9]{64}$'),
  content_ciphertext bytea NOT NULL,
  content_key_version text NOT NULL,
  recurrence jsonb NOT NULL,
  semantic_time_plan jsonb NOT NULL,
  notification_mode text NOT NULL CHECK (notification_mode IN ('exceptions_only', 'always', 'silent')),
  destination_conversation_id uuid NOT NULL REFERENCES conversations(id),
  participant_epoch_id uuid NOT NULL REFERENCES participant_epochs(id),
  participant_set_digest text NOT NULL CHECK (participant_set_digest ~ '^[a-f0-9]{64}$'),
  audience text NOT NULL CHECK (audience IN ('private', 'group')),
  proposed_holder_person_id uuid REFERENCES people(id),
  standing_holder_person_id uuid REFERENCES people(id),
  standing_authorized_by_person_id uuid REFERENCES people(id),
  standing_authorization_kind text CHECK (standing_authorization_kind IN ('created', 'approved')),
  standing_authorized_at timestamptz,
  source_revision_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  effective_from date NOT NULL,
  effective_through date,
  created_at timestamptz NOT NULL,
  created_by_person_id uuid NOT NULL REFERENCES people(id),
  PRIMARY KEY (routine_id, revision),
  CHECK (effective_through IS NULL OR effective_through >= effective_from),
  CHECK (
    (standing_holder_person_id IS NULL AND standing_authorized_by_person_id IS NULL
      AND standing_authorization_kind IS NULL AND standing_authorized_at IS NULL)
    OR (standing_holder_person_id = standing_authorized_by_person_id
      AND standing_authorization_kind IS NOT NULL AND standing_authorized_at IS NOT NULL
      AND proposed_holder_person_id = standing_holder_person_id)
  )
);

ALTER TABLE routines
  ADD CONSTRAINT routines_current_revision_fk FOREIGN KEY (id, current_revision)
  REFERENCES routine_revisions(routine_id, revision) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE routine_occurrences (
  id uuid PRIMARY KEY,
  routine_id uuid NOT NULL REFERENCES routines(id) ON DELETE CASCADE,
  materialization_key text NOT NULL UNIQUE,
  routine_revision bigint NOT NULL,
  local_date date NOT NULL,
  version bigint NOT NULL CHECK (version > 0),
  supersedes_version bigint,
  plan_version bigint NOT NULL CHECK (plan_version > 0),
  status text NOT NULL CHECK (status IN ('materialized', 'skipped', 'cancelled')),
  title_digest text NOT NULL CHECK (title_digest ~ '^[a-f0-9]{64}$'),
  minimum_shared_meaning_digest text NOT NULL CHECK (minimum_shared_meaning_digest ~ '^[a-f0-9]{64}$'),
  content_ciphertext bytea NOT NULL,
  content_key_version text NOT NULL,
  time_zone text NOT NULL,
  event_at timestamptz,
  deadline_at timestamptz,
  preparation_minutes integer NOT NULL CHECK (preparation_minutes >= 0),
  travel_minutes integer NOT NULL CHECK (travel_minutes >= 0),
  earliest_useful_at timestamptz NOT NULL,
  last_responsible_at timestamptz,
  notification_mode text NOT NULL CHECK (notification_mode IN ('exceptions_only', 'always', 'silent')),
  destination_conversation_id uuid NOT NULL REFERENCES conversations(id),
  participant_epoch_id uuid NOT NULL REFERENCES participant_epochs(id),
  participant_set_digest text NOT NULL CHECK (participant_set_digest ~ '^[a-f0-9]{64}$'),
  audience text NOT NULL CHECK (audience IN ('private', 'group')),
  proposed_holder_person_id uuid REFERENCES people(id),
  standing_holder_person_id uuid REFERENCES people(id),
  standing_authorized_by_person_id uuid REFERENCES people(id),
  standing_authorization_kind text CHECK (standing_authorization_kind IN ('created', 'approved')),
  standing_authorized_at timestamptz,
  source_revision_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  materialized_at timestamptz NOT NULL,
  FOREIGN KEY (routine_id, routine_revision) REFERENCES routine_revisions(routine_id, revision),
  CHECK (supersedes_version IS NULL OR supersedes_version < version),
  CHECK (last_responsible_at IS NOT NULL AND earliest_useful_at <= last_responsible_at)
);

CREATE TABLE coverage_loops (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
  state text NOT NULL CHECK (
    state IN (
      'provisional', 'open', 'awaiting_response', 'covered', 'at_risk',
      'cancelled', 'superseded', 'dismissed', 'expired_uncovered'
    )
  ),
  minimum_shared_meaning_digest text NOT NULL CHECK (minimum_shared_meaning_digest ~ '^[a-f0-9]{64}$'),
  content_ciphertext bytea NOT NULL,
  content_key_version text NOT NULL,
  unresolved_facts jsonb NOT NULL DEFAULT '[]'::jsonb,
  proposed_holder_person_id uuid REFERENCES people(id),
  acknowledged_by_person_id uuid REFERENCES people(id),
  acknowledged_at timestamptz,
  acknowledgment_kind text CHECK (
    acknowledgment_kind IN ('explicit_self', 'standing_routine_self_authorized')
  ),
  holder_disclosure text CHECK (holder_disclosure IN ('shared', 'minimum_only')),
  time_zone text NOT NULL,
  local_date date NOT NULL,
  event_at timestamptz,
  deadline_at timestamptz,
  preparation_minutes integer NOT NULL CHECK (preparation_minutes >= 0),
  travel_minutes integer NOT NULL CHECK (travel_minutes >= 0),
  earliest_useful_at timestamptz NOT NULL,
  last_responsible_at timestamptz NOT NULL,
  plan_version bigint NOT NULL CHECK (plan_version > 0),
  notification_mode text NOT NULL CHECK (notification_mode IN ('exceptions_only', 'always', 'silent')),
  destination_conversation_id uuid NOT NULL REFERENCES conversations(id),
  participant_epoch_id uuid NOT NULL REFERENCES participant_epochs(id),
  participant_set_digest text NOT NULL CHECK (participant_set_digest ~ '^[a-f0-9]{64}$'),
  audience text NOT NULL CHECK (audience IN ('private', 'group')),
  source_evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  routine_occurrence_id uuid REFERENCES routine_occurrences(id),
  routine_occurrence_version bigint,
  routine_id uuid REFERENCES routines(id),
  routine_revision bigint,
  attention_cycle integer NOT NULL DEFAULT 1 CHECK (attention_cycle > 0),
  notification_history jsonb NOT NULL DEFAULT '[]'::jsonb,
  last_transition_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (earliest_useful_at <= last_responsible_at),
  CHECK (
    (state = 'covered' AND acknowledged_by_person_id IS NOT NULL AND acknowledged_at IS NOT NULL
      AND acknowledgment_kind IS NOT NULL AND holder_disclosure IS NOT NULL)
    OR (state <> 'covered' AND acknowledged_by_person_id IS NULL AND acknowledged_at IS NULL
      AND acknowledgment_kind IS NULL AND holder_disclosure IS NULL)
  )
);

CREATE UNIQUE INDEX coverage_loops_routine_occurrence_version_idx
  ON coverage_loops (routine_occurrence_id, routine_occurrence_version)
  WHERE routine_occurrence_id IS NOT NULL;

CREATE INDEX coverage_loops_active_idx
  ON coverage_loops (household_id, state, updated_at)
  WHERE state NOT IN ('cancelled', 'dismissed', 'expired_uncovered', 'superseded');

CREATE TABLE coverage_transitions (
  id uuid PRIMARY KEY,
  coverage_loop_id uuid NOT NULL REFERENCES coverage_loops(id) ON DELETE CASCADE,
  from_state text NOT NULL,
  to_state text NOT NULL,
  from_version bigint NOT NULL CHECK (from_version > 0),
  to_version bigint NOT NULL CHECK (to_version = from_version + 1),
  transition_kind text NOT NULL CHECK (
    transition_kind IN (
      'facts_resolved', 'coverage_requested', 'coverage_acknowledged',
      'coverage_declined_privately', 'coverage_at_risk', 'coverage_participant_revoked', 'cancelled',
      'superseded', 'dismissed', 'expired_uncovered'
    )
  ),
  actor_person_id uuid REFERENCES people(id),
  evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  occurred_at timestamptz NOT NULL,
  UNIQUE (coverage_loop_id, to_version)
);

CREATE TABLE coverage_participants (
  coverage_loop_id uuid NOT NULL REFERENCES coverage_loops(id) ON DELETE CASCADE,
  person_id uuid NOT NULL REFERENCES people(id),
  participation_kind text NOT NULL CHECK (
    participation_kind IN ('proposed_holder', 'acknowledged_holder')
  ),
  loop_version bigint NOT NULL CHECK (loop_version > 0),
  active boolean NOT NULL,
  recorded_at timestamptz NOT NULL,
  PRIMARY KEY (coverage_loop_id, person_id, participation_kind, loop_version)
);

-- Authority, governed work, and effects --------------------------------------

CREATE TABLE worker_leases (
  lease_name text PRIMARY KEY,
  worker_id text NOT NULL,
  release_id text NOT NULL,
  started_at timestamptz NOT NULL,
  last_seen_at timestamptz NOT NULL,
  stopped_at timestamptz,
  CHECK (last_seen_at >= started_at),
  CHECK (stopped_at IS NULL OR stopped_at >= started_at)
);

CREATE TABLE bridge_rules (
  id uuid PRIMARY KEY,
  owner_person_id uuid NOT NULL REFERENCES people(id) ON DELETE CASCADE,
  destination_household_id uuid REFERENCES households(id) ON DELETE CASCADE,
  destination_conversation_id uuid REFERENCES conversations(id) ON DELETE CASCADE,
  rule_key text NOT NULL,
  status text NOT NULL CHECK (status IN ('candidate', 'active', 'paused', 'revoked')),
  current_revision_id uuid,
  version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (destination_household_id IS NOT NULL)::integer
      + (destination_conversation_id IS NOT NULL)::integer = 1
  )
);

CREATE TABLE bridge_rule_revisions (
  id uuid PRIMARY KEY,
  bridge_rule_id uuid NOT NULL REFERENCES bridge_rules(id) ON DELETE CASCADE,
  revision bigint NOT NULL CHECK (revision > 0),
  source_scope_digest text NOT NULL CHECK (source_scope_digest ~ '^[a-f0-9]{64}$'),
  purpose_digest text NOT NULL CHECK (purpose_digest ~ '^[a-f0-9]{64}$'),
  destination_digest text NOT NULL CHECK (destination_digest ~ '^[a-f0-9]{64}$'),
  minimum_meaning_schema jsonb NOT NULL,
  retention_seconds integer NOT NULL CHECK (retention_seconds BETWEEN 0 AND 2592000),
  approved_by_person_id uuid NOT NULL REFERENCES people(id),
  effective_at timestamptz NOT NULL,
  ended_at timestamptz,
  UNIQUE (bridge_rule_id, revision)
);

ALTER TABLE bridge_rules
  ADD CONSTRAINT bridge_rules_current_revision_fk
  FOREIGN KEY (current_revision_id) REFERENCES bridge_rule_revisions(id);

CREATE TABLE disclosure_decisions (
  id uuid PRIMARY KEY,
  outcome text NOT NULL CHECK (outcome IN ('allow', 'deny')),
  actor_person_id uuid REFERENCES people(id),
  household_id uuid REFERENCES households(id),
  conversation_id uuid REFERENCES conversations(id),
  participant_epoch_id uuid REFERENCES participant_epochs(id),
  action_digest text NOT NULL CHECK (action_digest ~ '^[a-f0-9]{64}$'),
  data_digest text NOT NULL CHECK (data_digest ~ '^[a-f0-9]{64}$'),
  policy_digest text NOT NULL CHECK (policy_digest ~ '^[a-f0-9]{64}$'),
  target_digest text NOT NULL CHECK (target_digest ~ '^[a-f0-9]{64}$'),
  reason_codes text[] NOT NULL,
  decided_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  revoked_at timestamptz,
  CHECK (expires_at > decided_at)
);

CREATE TABLE skills (
  id uuid PRIMARY KEY,
  skill_key text NOT NULL UNIQUE,
  owner text NOT NULL,
  purpose text NOT NULL,
  risk_class text NOT NULL CHECK (risk_class IN ('low', 'medium', 'high')),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE skill_versions (
  id uuid PRIMARY KEY,
  skill_id uuid NOT NULL REFERENCES skills(id) ON DELETE CASCADE,
  version integer NOT NULL CHECK (version > 0),
  input_schema jsonb NOT NULL,
  output_schema jsonb NOT NULL,
  requested_capabilities text[] NOT NULL DEFAULT '{}',
  tool_ceiling text[] NOT NULL DEFAULT '{}',
  examples_ciphertext bytea,
  examples_key_version text,
  evaluation_release_id uuid,
  rollback_version_id uuid REFERENCES skill_versions(id),
  status text NOT NULL CHECK (status IN ('candidate', 'approved', 'retired')),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (skill_id, version),
  CHECK ((examples_ciphertext IS NULL) = (examples_key_version IS NULL))
);

CREATE TABLE evaluation_releases (
  id uuid PRIMARY KEY,
  release_key text NOT NULL UNIQUE,
  suite_digest text NOT NULL CHECK (suite_digest ~ '^[a-f0-9]{64}$'),
  status text NOT NULL CHECK (status IN ('candidate', 'active', 'retired')),
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE skill_versions
  ADD CONSTRAINT skill_versions_evaluation_release_fk
  FOREIGN KEY (evaluation_release_id) REFERENCES evaluation_releases(id);

CREATE TABLE skill_release_events (
  id uuid PRIMARY KEY,
  skill_id uuid NOT NULL REFERENCES skills(id) ON DELETE CASCADE,
  skill_version_id uuid NOT NULL REFERENCES skill_versions(id),
  channel text NOT NULL,
  event_kind text NOT NULL CHECK (event_kind IN ('promoted', 'rolled_back', 'retired')),
  active boolean NOT NULL,
  actor_person_id uuid REFERENCES people(id),
  evaluation_release_id uuid NOT NULL REFERENCES evaluation_releases(id),
  occurred_at timestamptz NOT NULL
);

CREATE UNIQUE INDEX skill_release_events_one_active_idx
  ON skill_release_events (skill_id, channel)
  WHERE active;

CREATE TABLE evaluation_runs (
  id uuid PRIMARY KEY,
  evaluation_release_id uuid NOT NULL REFERENCES evaluation_releases(id),
  skill_version_id uuid REFERENCES skill_versions(id),
  model_route text NOT NULL,
  result_digest text NOT NULL CHECK (result_digest ~ '^[a-f0-9]{64}$'),
  passed boolean NOT NULL,
  metrics jsonb NOT NULL,
  started_at timestamptz NOT NULL,
  completed_at timestamptz NOT NULL,
  CHECK (completed_at >= started_at)
);

CREATE TABLE tasks (
  id uuid PRIMARY KEY,
  household_id uuid REFERENCES households(id) ON DELETE CASCADE,
  conversation_id uuid REFERENCES conversations(id) ON DELETE CASCADE,
  requested_by_person_id uuid REFERENCES people(id),
  task_kind text NOT NULL,
  task_version integer NOT NULL CHECK (task_version > 0),
  purpose text NOT NULL,
  input_digest text NOT NULL CHECK (input_digest ~ '^[a-f0-9]{64}$'),
  status text NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
  person_control_epoch bigint CHECK (person_control_epoch > 0),
  household_control_epoch bigint CHECK (household_control_epoch > 0),
  conversation_authority_version bigint CHECK (conversation_authority_version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  CHECK ((requested_by_person_id IS NULL) = (person_control_epoch IS NULL)),
  CHECK ((household_id IS NULL) = (household_control_epoch IS NULL)),
  CHECK ((conversation_id IS NULL) = (conversation_authority_version IS NULL))
);

CREATE TABLE worker_attempts (
  id uuid PRIMARY KEY,
  task_id uuid NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  attempt_number integer NOT NULL CHECK (attempt_number > 0),
  skill_version_id uuid NOT NULL REFERENCES skill_versions(id),
  harness_release text NOT NULL,
  runtime_route text NOT NULL,
  evaluation_release_id uuid NOT NULL REFERENCES evaluation_releases(id),
  trace_id uuid NOT NULL UNIQUE,
  status text NOT NULL CHECK (
    status IN ('queued', 'leased', 'running', 'succeeded', 'failed', 'cancelled', 'expired')
  ),
  budget jsonb NOT NULL,
  deadline_at timestamptz NOT NULL,
  lease_owner text,
  lease_token uuid,
  lease_expires_at timestamptz,
  started_at timestamptz,
  completed_at timestamptz,
  UNIQUE (task_id, attempt_number)
);

CREATE TABLE worker_results (
  id uuid PRIMARY KEY,
  worker_attempt_id uuid NOT NULL UNIQUE REFERENCES worker_attempts(id) ON DELETE CASCADE,
  output_contract text NOT NULL,
  output_digest text NOT NULL CHECK (output_digest ~ '^[a-f0-9]{64}$'),
  output_ciphertext bytea NOT NULL,
  output_key_version text NOT NULL,
  diagnostics jsonb NOT NULL DEFAULT '{}'::jsonb,
  submitted_at timestamptz NOT NULL,
  reconciliation_status text NOT NULL CHECK (
    reconciliation_status IN ('pending', 'accepted', 'partially_accepted', 'rejected', 'stale')
  ),
  reconciled_at timestamptz
);

CREATE TABLE trace_manifests (
  id uuid PRIMARY KEY,
  worker_attempt_id uuid NOT NULL UNIQUE REFERENCES worker_attempts(id) ON DELETE CASCADE,
  trace_id uuid NOT NULL UNIQUE,
  manifest jsonb NOT NULL,
  retention_until timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE jobs (
  id uuid PRIMARY KEY,
  job_kind text NOT NULL,
  household_id uuid REFERENCES households(id) ON DELETE CASCADE,
  person_id uuid REFERENCES people(id) ON DELETE CASCADE,
  conversation_id uuid REFERENCES conversations(id) ON DELETE CASCADE,
  task_id uuid REFERENCES tasks(id) ON DELETE CASCADE,
  idempotency_key text NOT NULL UNIQUE,
  payload_digest text NOT NULL CHECK (payload_digest ~ '^[a-f0-9]{64}$'),
  payload_ciphertext bytea NOT NULL,
  payload_key_version text NOT NULL,
  status text NOT NULL CHECK (
    status IN ('pending', 'leased', 'retry', 'succeeded', 'dead', 'cancelled')
  ),
  attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  max_attempts integer NOT NULL DEFAULT 8 CHECK (max_attempts > 0),
  available_at timestamptz NOT NULL,
  deadline_at timestamptz,
  lease_owner text,
  lease_token uuid,
  lease_expires_at timestamptz,
  person_control_epoch bigint CHECK (person_control_epoch > 0),
  household_control_epoch bigint CHECK (household_control_epoch > 0),
  conversation_authority_version bigint CHECK (conversation_authority_version > 0),
  last_error_code text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((person_id IS NULL) = (person_control_epoch IS NULL)),
  CHECK ((household_id IS NULL) = (household_control_epoch IS NULL)),
  CHECK ((conversation_id IS NULL) = (conversation_authority_version IS NULL))
);

CREATE INDEX jobs_claim_idx
  ON jobs (available_at, created_at)
  WHERE status IN ('pending', 'retry', 'leased');

CREATE TABLE timers (
  id uuid PRIMARY KEY,
  timer_kind text NOT NULL,
  household_id uuid REFERENCES households(id) ON DELETE CASCADE,
  person_id uuid REFERENCES people(id) ON DELETE CASCADE,
  conversation_id uuid REFERENCES conversations(id) ON DELETE CASCADE,
  coverage_loop_id uuid REFERENCES coverage_loops(id) ON DELETE CASCADE,
  loop_version bigint CHECK (loop_version > 0),
  plan_version bigint CHECK (plan_version > 0),
  attention_cycle integer CHECK (attention_cycle > 0),
  participant_epoch_id uuid REFERENCES participant_epochs(id),
  participant_set_digest text CHECK (
    participant_set_digest IS NULL OR participant_set_digest ~ '^[a-f0-9]{64}$'
  ),
  notification_category text CHECK (
    notification_category IN ('coverage_opening', 'coverage_reminder')
  ),
  definition_digest text NOT NULL CHECK (definition_digest ~ '^[a-f0-9]{64}$'),
  due_at timestamptz,
  status text NOT NULL CHECK (
    status IN ('scheduled', 'claimed', 'fired', 'cancelled', 'superseded', 'dead')
  ),
  person_control_epoch bigint CHECK (person_control_epoch > 0),
  household_control_epoch bigint CHECK (household_control_epoch > 0),
  conversation_authority_version bigint CHECK (conversation_authority_version > 0),
  expected_domain_version bigint NOT NULL CHECK (expected_domain_version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((person_id IS NULL) = (person_control_epoch IS NULL)),
  CHECK ((household_id IS NULL) = (household_control_epoch IS NULL)),
  CHECK ((conversation_id IS NULL) = (conversation_authority_version IS NULL)),
  CHECK (
    (status IN ('scheduled', 'claimed') AND due_at IS NOT NULL)
    OR (status IN ('fired', 'cancelled', 'superseded', 'dead') AND due_at IS NULL)
  )
);

CREATE INDEX timers_due_idx ON timers (due_at) WHERE status = 'scheduled';

CREATE TABLE action_intents (
  id uuid PRIMARY KEY,
  household_id uuid REFERENCES households(id) ON DELETE CASCADE,
  person_id uuid REFERENCES people(id),
  conversation_id uuid REFERENCES conversations(id),
  participant_epoch_id uuid REFERENCES participant_epochs(id),
  action_kind text NOT NULL,
  action_digest text NOT NULL CHECK (action_digest ~ '^[a-f0-9]{64}$'),
  data_digest text NOT NULL CHECK (data_digest ~ '^[a-f0-9]{64}$'),
  policy_digest text NOT NULL CHECK (policy_digest ~ '^[a-f0-9]{64}$'),
  target_digest text NOT NULL CHECK (target_digest ~ '^[a-f0-9]{64}$'),
  payload_ciphertext bytea NOT NULL,
  payload_key_version text NOT NULL,
  status text NOT NULL CHECK (
    status IN ('proposed', 'awaiting_approval', 'approved', 'executing', 'succeeded', 'failed', 'ambiguous', 'cancelled', 'expired')
  ),
  person_control_epoch bigint CHECK (person_control_epoch > 0),
  household_control_epoch bigint CHECK (household_control_epoch > 0),
  conversation_authority_version bigint CHECK (conversation_authority_version > 0),
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((person_id IS NULL) = (person_control_epoch IS NULL)),
  CHECK ((household_id IS NULL) = (household_control_epoch IS NULL)),
  CHECK ((conversation_id IS NULL) = (conversation_authority_version IS NULL))
);

CREATE TABLE action_approvals (
  id uuid PRIMARY KEY,
  action_intent_id uuid NOT NULL REFERENCES action_intents(id) ON DELETE CASCADE,
  approved_by_person_id uuid NOT NULL REFERENCES people(id),
  action_digest text NOT NULL CHECK (action_digest ~ '^[a-f0-9]{64}$'),
  data_digest text NOT NULL CHECK (data_digest ~ '^[a-f0-9]{64}$'),
  policy_digest text NOT NULL CHECK (policy_digest ~ '^[a-f0-9]{64}$'),
  target_digest text NOT NULL CHECK (target_digest ~ '^[a-f0-9]{64}$'),
  approved_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  revoked_at timestamptz,
  UNIQUE (action_intent_id, approved_by_person_id),
  CHECK (expires_at > approved_at)
);

CREATE TABLE outbox (
  id uuid PRIMARY KEY,
  authorization_decision_id uuid NOT NULL REFERENCES disclosure_decisions(id),
  action_intent_id uuid REFERENCES action_intents(id),
  household_id uuid REFERENCES households(id) ON DELETE CASCADE,
  person_id uuid REFERENCES people(id),
  conversation_id uuid REFERENCES conversations(id),
  participant_epoch_id uuid REFERENCES participant_epochs(id),
  expected_participant_digest text CHECK (
    expected_participant_digest IS NULL OR expected_participant_digest ~ '^[a-f0-9]{64}$'
  ),
  coverage_loop_id uuid REFERENCES coverage_loops(id) ON DELETE CASCADE,
  coverage_loop_version bigint CHECK (coverage_loop_version > 0),
  effect_kind text NOT NULL,
  idempotency_key text NOT NULL UNIQUE,
  payload_digest text NOT NULL CHECK (payload_digest ~ '^[a-f0-9]{64}$'),
  payload_ciphertext bytea NOT NULL,
  payload_key_version text NOT NULL,
  status text NOT NULL CHECK (
    status IN ('pending', 'leased', 'retry', 'submitted', 'confirmed', 'ambiguous', 'dead', 'cancelled')
  ),
  attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  reconciliation_attempt_count integer NOT NULL DEFAULT 0 CHECK (reconciliation_attempt_count >= 0),
  available_at timestamptz NOT NULL,
  lease_owner text,
  lease_token uuid,
  lease_expires_at timestamptz,
  person_control_epoch bigint CHECK (person_control_epoch > 0),
  household_control_epoch bigint CHECK (household_control_epoch > 0),
  conversation_authority_version bigint CHECK (conversation_authority_version > 0),
  last_error_code text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((person_id IS NULL) = (person_control_epoch IS NULL)),
  CHECK ((household_id IS NULL) = (household_control_epoch IS NULL)),
  CHECK ((conversation_id IS NULL) = (conversation_authority_version IS NULL)),
  CHECK ((conversation_id IS NULL) = (participant_epoch_id IS NULL)),
  CHECK ((conversation_id IS NULL) = (expected_participant_digest IS NULL)),
  CHECK ((coverage_loop_id IS NULL) = (coverage_loop_version IS NULL))
);

CREATE INDEX outbox_claim_idx
  ON outbox (available_at, created_at)
  WHERE status IN ('pending', 'retry', 'leased');

CREATE FUNCTION enforce_outbox_authorization() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  decision disclosure_decisions%ROWTYPE;
BEGIN
  SELECT * INTO STRICT decision
  FROM disclosure_decisions
  WHERE id = NEW.authorization_decision_id;
  IF decision.outcome <> 'allow'
    OR decision.revoked_at IS NOT NULL
    OR decision.expires_at <= now()
    OR (NEW.action_intent_id IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM action_intents intent
      WHERE intent.id = NEW.action_intent_id
        AND intent.action_digest = decision.action_digest
        AND intent.data_digest = decision.data_digest
        AND intent.policy_digest = decision.policy_digest
        AND intent.target_digest = decision.target_digest
    ))
  THEN
    RAISE EXCEPTION 'outbox row requires a current matching allow decision';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER outbox_requires_authorization
BEFORE INSERT OR UPDATE OF authorization_decision_id, action_intent_id ON outbox
FOR EACH ROW EXECUTE FUNCTION enforce_outbox_authorization();

CREATE TABLE effect_receipts (
  id uuid PRIMARY KEY,
  outbox_id uuid NOT NULL REFERENCES outbox(id),
  idempotency_key text NOT NULL,
  provider_receipt_id text,
  status text NOT NULL CHECK (status IN ('submitted', 'confirmed', 'failed', 'ambiguous')),
  receipt_digest text NOT NULL CHECK (receipt_digest ~ '^[a-f0-9]{64}$'),
  receipt_ciphertext bytea,
  receipt_key_version text,
  occurred_at timestamptz NOT NULL,
  reconciled_at timestamptz,
  error_code text,
  UNIQUE (outbox_id, receipt_digest),
  CHECK ((receipt_ciphertext IS NULL) = (receipt_key_version IS NULL))
);

CREATE INDEX effect_receipts_provider_receipt_idx
  ON effect_receipts (provider_receipt_id)
  WHERE provider_receipt_id IS NOT NULL;

CREATE TABLE audit_events (
  id uuid PRIMARY KEY,
  household_id uuid REFERENCES households(id) ON DELETE CASCADE,
  person_id uuid REFERENCES people(id) ON DELETE CASCADE,
  conversation_id uuid REFERENCES conversations(id) ON DELETE CASCADE,
  sequence bigint NOT NULL CHECK (sequence > 0),
  actor_kind text NOT NULL CHECK (actor_kind IN ('person', 'application', 'worker_proposal', 'operator')),
  actor_id uuid,
  event_type text NOT NULL,
  target_type text NOT NULL,
  target_id uuid,
  reason_codes text[] NOT NULL DEFAULT '{}',
  evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  decision_manifest jsonb NOT NULL DEFAULT '{}'::jsonb,
  occurred_at timestamptz NOT NULL,
  CHECK (household_id IS NOT NULL OR person_id IS NOT NULL OR conversation_id IS NOT NULL)
);

CREATE UNIQUE INDEX audit_events_household_sequence_idx
  ON audit_events (household_id, sequence)
  WHERE household_id IS NOT NULL;
CREATE INDEX audit_events_person_idx ON audit_events (person_id, occurred_at DESC);
CREATE INDEX audit_events_conversation_idx ON audit_events (conversation_id, occurred_at DESC);

CREATE TABLE deletion_requests (
  id uuid PRIMARY KEY,
  requested_by_person_id uuid NOT NULL REFERENCES people(id),
  target_kind text NOT NULL CHECK (target_kind IN ('person', 'household', 'conversation', 'integration')),
  target_person_id uuid REFERENCES people(id),
  target_household_id uuid REFERENCES households(id),
  target_conversation_id uuid REFERENCES conversations(id),
  target_integration_id uuid REFERENCES integrations(id),
  target_control_epoch bigint NOT NULL CHECK (target_control_epoch > 0),
  status text NOT NULL CHECK (status IN ('requested', 'fenced', 'erasing', 'completed', 'failed')),
  requested_at timestamptz NOT NULL,
  completed_at timestamptz,
  receipt_digest text CHECK (receipt_digest IS NULL OR receipt_digest ~ '^[a-f0-9]{64}$'),
  CHECK (
    (target_person_id IS NOT NULL)::integer
      + (target_household_id IS NOT NULL)::integer
      + (target_conversation_id IS NOT NULL)::integer
      + (target_integration_id IS NOT NULL)::integer = 1
  )
);

CREATE TABLE revocation_tombstones (
  id uuid PRIMARY KEY,
  target_kind text NOT NULL,
  target_id_digest text NOT NULL CHECK (target_id_digest ~ '^[a-f0-9]{64}$'),
  control_epoch bigint NOT NULL CHECK (control_epoch > 0),
  reason_code text NOT NULL,
  revoked_at timestamptz NOT NULL,
  expires_at timestamptz,
  UNIQUE (target_kind, target_id_digest, control_epoch)
);
