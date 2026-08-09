-- Family onboarding is a resumable product journey layered over canonical
-- people, households, memberships, dependents, invitations, and integrations.
-- These records capture only explicit review/decision fences; they are not a
-- second family model or a generic workflow engine.

CREATE TABLE person_onboarding (
  person_id uuid PRIMARY KEY REFERENCES people(id) ON DELETE CASCADE,
  profile_reviewed_by_person_id uuid REFERENCES people(id),
  reviewed_person_authority_version bigint CHECK (
    reviewed_person_authority_version > 0
  ),
  profile_review_version bigint NOT NULL DEFAULT 0 CHECK (profile_review_version >= 0),
  profile_reviewed_at timestamptz,
  selected_household_id uuid REFERENCES households(id) ON DELETE SET NULL,
  reminders_sent smallint NOT NULL DEFAULT 0 CHECK (reminders_sent BETWEEN 0 AND 2),
  last_reminded_at timestamptz,
  reminders_suppressed_at timestamptz,
  last_progressed_at timestamptz NOT NULL,
  version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (profile_reviewed_by_person_id IS NULL
      AND reviewed_person_authority_version IS NULL
      AND profile_review_version = 0
      AND profile_reviewed_at IS NULL)
    OR
    (profile_reviewed_by_person_id = person_id
      AND reviewed_person_authority_version IS NOT NULL
      AND profile_review_version > 0
      AND profile_reviewed_at IS NOT NULL)
  ),
  CHECK (
    (reminders_sent = 0 AND last_reminded_at IS NULL)
    OR (reminders_sent > 0 AND last_reminded_at IS NOT NULL)
  )
);

CREATE INDEX person_onboarding_selected_household_idx
  ON person_onboarding (selected_household_id)
  WHERE selected_household_id IS NOT NULL;

CREATE TABLE household_onboarding_intakes (
  household_id uuid PRIMARY KEY REFERENCES households(id) ON DELETE CASCADE,
  coordinator_disposition text NOT NULL DEFAULT 'unanswered' CHECK (
    coordinator_disposition IN ('unanswered', 'just_me', 'invite_later', 'proposed')
  ),
  proposed_coordinator_name_ciphertext bytea,
  proposed_coordinator_name_key_version text,
  coordinator_answered_by_person_id uuid REFERENCES people(id),
  coordinator_answered_at timestamptz,
  coordinator_invite_deferred_by_person_id uuid REFERENCES people(id),
  coordinator_invite_deferred_at timestamptz,
  child_roster_reviewed_by_person_id uuid REFERENCES people(id),
  child_roster_reviewed_at timestamptz,
  child_roster_household_membership_version bigint CHECK (
    child_roster_household_membership_version > 0
  ),
  version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (proposed_coordinator_name_ciphertext IS NULL) =
      (proposed_coordinator_name_key_version IS NULL)
  ),
  CHECK (
    (coordinator_disposition = 'proposed') =
      (proposed_coordinator_name_ciphertext IS NOT NULL)
  ),
  CHECK (
    (coordinator_disposition = 'unanswered'
      AND coordinator_answered_by_person_id IS NULL
      AND coordinator_answered_at IS NULL)
    OR
    (coordinator_disposition <> 'unanswered'
      AND coordinator_answered_by_person_id IS NOT NULL
      AND coordinator_answered_at IS NOT NULL)
  ),
  CHECK (
    (coordinator_invite_deferred_by_person_id IS NULL) =
      (coordinator_invite_deferred_at IS NULL)
  ),
  CHECK (
    coordinator_invite_deferred_at IS NULL OR coordinator_disposition = 'proposed'
  ),
  CHECK (
    (child_roster_reviewed_by_person_id IS NULL
      AND child_roster_reviewed_at IS NULL
      AND child_roster_household_membership_version IS NULL)
    OR
    (child_roster_reviewed_by_person_id IS NOT NULL
      AND child_roster_reviewed_at IS NOT NULL
      AND child_roster_household_membership_version IS NOT NULL)
  )
);

CREATE TABLE membership_onboarding (
  membership_id uuid PRIMARY KEY REFERENCES household_memberships(id) ON DELETE CASCADE,
  shared_context_reviewed_by_person_id uuid REFERENCES people(id),
  shared_context_household_intake_version bigint CHECK (
    shared_context_household_intake_version > 0
  ),
  shared_context_reviewed_at timestamptz,
  completed_by_person_id uuid REFERENCES people(id),
  completed_membership_version bigint CHECK (completed_membership_version > 0),
  completed_profile_review_version bigint CHECK (completed_profile_review_version > 0),
  completed_household_intake_version bigint CHECK (completed_household_intake_version > 0),
  completed_google_decision text CHECK (completed_google_decision IN ('connected', 'limited')),
  version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
  completed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (shared_context_reviewed_by_person_id IS NULL
      AND shared_context_household_intake_version IS NULL
      AND shared_context_reviewed_at IS NULL)
    OR
    (shared_context_reviewed_by_person_id IS NOT NULL
      AND shared_context_household_intake_version IS NOT NULL
      AND shared_context_reviewed_at IS NOT NULL)
  ),
  CHECK (
    (completed_by_person_id IS NULL
      AND completed_membership_version IS NULL
      AND completed_profile_review_version IS NULL
      AND completed_household_intake_version IS NULL
      AND completed_google_decision IS NULL
      AND completed_at IS NULL)
    OR
    (completed_by_person_id IS NOT NULL
      AND completed_membership_version IS NOT NULL
      AND completed_profile_review_version IS NOT NULL
      AND completed_household_intake_version IS NOT NULL
      AND completed_google_decision IS NOT NULL
      AND completed_at IS NOT NULL)
  )
);

ALTER TABLE person_sessions
  DROP CONSTRAINT person_sessions_assurance_kind_check;

ALTER TABLE person_sessions
  ADD CONSTRAINT person_sessions_assurance_kind_check CHECK (
    assurance_kind IN (
      'base', 'google_connect', 'account_controls', 'household_invitation',
      'group_coverage', 'private_bridge_standing', 'onboarding'
    )
  );
