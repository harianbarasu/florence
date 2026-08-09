-- The family intake records a reviewed, informational roster of the adults a
-- parent expects Florence to coordinate with. Typed names never create people,
-- memberships, invitations, or messaging authority. An intent can be bound to
-- one exact observed person only when the canonical invitation is created.

ALTER TABLE household_onboarding_intakes
  DROP COLUMN coordinator_disposition,
  DROP COLUMN proposed_coordinator_name_ciphertext,
  DROP COLUMN proposed_coordinator_name_key_version,
  DROP COLUMN coordinator_answered_by_person_id,
  DROP COLUMN coordinator_answered_at,
  DROP COLUMN coordinator_invite_deferred_by_person_id,
  DROP COLUMN coordinator_invite_deferred_at,
  ADD COLUMN adult_roster_reviewed_by_person_id uuid REFERENCES people(id),
  ADD COLUMN adult_roster_reviewed_at timestamptz,
  ADD CONSTRAINT household_onboarding_adult_roster_review_complete CHECK (
    (adult_roster_reviewed_by_person_id IS NULL AND adult_roster_reviewed_at IS NULL)
    OR
    (adult_roster_reviewed_by_person_id IS NOT NULL AND adult_roster_reviewed_at IS NOT NULL)
  );

CREATE TABLE household_onboarding_adult_intents (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  display_name_ciphertext bytea NOT NULL,
  display_name_key_version text NOT NULL,
  role text NOT NULL CHECK (role IN ('steward', 'caregiver')),
  matched_person_id uuid REFERENCES people(id),
  invitation_id uuid UNIQUE REFERENCES invitations(id),
  recorded_by_person_id uuid NOT NULL REFERENCES people(id),
  version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((matched_person_id IS NULL) = (invitation_id IS NULL))
);

CREATE INDEX household_onboarding_adult_intents_household_idx
  ON household_onboarding_adult_intents (household_id, created_at, id);

CREATE UNIQUE INDEX household_onboarding_adult_intents_matched_person_idx
  ON household_onboarding_adult_intents (household_id, matched_person_id)
  WHERE matched_person_id IS NOT NULL;

-- A supporting adult may finish their private branch before a steward has
-- created the shared intake. Version zero records that exact absence; it does
-- not grant authority over, or imply review of, any shared family context.
ALTER TABLE membership_onboarding
  DROP CONSTRAINT membership_onboarding_completed_household_intake_version_check,
  ADD CONSTRAINT membership_onboarding_completed_household_intake_version_check CHECK (
    completed_household_intake_version >= 0
  );

-- A supporting adult's private completion is current until shared child
-- context exists. Once a steward reviews or changes that context, the adult
-- must explicitly review that exact intake version before broad family data is
-- available to them again.
CREATE FUNCTION family_membership_onboarding_is_current(expected_membership_id uuid)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
  SELECT EXISTS (
    SELECT 1
    FROM household_memberships membership
    JOIN membership_onboarding onboarding ON onboarding.membership_id = membership.id
    LEFT JOIN household_onboarding_intakes intake ON intake.household_id = membership.household_id
    WHERE membership.id = expected_membership_id
      AND membership.status = 'active'
      AND onboarding.completed_at IS NOT NULL
      AND (
        membership.role = 'steward'
        OR intake.child_roster_reviewed_at IS NULL
        OR (
          onboarding.shared_context_household_intake_version = intake.version
          AND onboarding.completed_household_intake_version = intake.version
        )
      )
  )
$$;

-- A queued onboarding reminder remains authorized only for the exact saved
-- onboarding state that produced its copy. Progress, suppression, or
-- relationship-local completion invalidates it before provider submission.
ALTER TABLE outbox
  ADD COLUMN person_onboarding_version bigint CHECK (person_onboarding_version > 0),
  ADD CONSTRAINT outbox_person_onboarding_fence_has_person CHECK (
    person_onboarding_version IS NULL OR person_id IS NOT NULL
  );

CREATE FUNCTION person_onboarding_fence_is_current(
  expected_person_id uuid,
  expected_onboarding_version bigint
)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
  SELECT EXISTS (
    SELECT 1
    FROM person_onboarding onboarding
    WHERE onboarding.person_id = expected_person_id
      AND onboarding.version = expected_onboarding_version
      AND onboarding.reminders_suppressed_at IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM household_memberships membership
        WHERE membership.person_id = expected_person_id
          AND membership.household_id = onboarding.selected_household_id
          AND membership.status = 'active'
          AND family_membership_onboarding_is_current(membership.id)
      )
  )
$$;
