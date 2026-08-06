-- The first Florence family profile is deliberately child-specific. These are
-- the few facts needed to recognize the same child across parent wording,
-- school messages, calendars, and activities without inventing a generic CRM.

CREATE TABLE dependent_profiles (
  person_id uuid PRIMARY KEY REFERENCES people(id) ON DELETE CASCADE,
  aliases_ciphertext bytea,
  aliases_key_version text,
  birth_year smallint CHECK (birth_year BETWEEN 1900 AND 2100),
  school_ciphertext bytea,
  school_key_version text,
  activities_ciphertext bytea,
  activities_key_version text,
  updated_by_person_id uuid NOT NULL REFERENCES people(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((aliases_ciphertext IS NULL) = (aliases_key_version IS NULL)),
  CHECK ((school_ciphertext IS NULL) = (school_key_version IS NULL)),
  CHECK ((activities_ciphertext IS NULL) = (activities_key_version IS NULL))
);

-- Florence normally identifies people by a one-way subject digest. Initiating
-- an explicitly authorized private enrollment conversation also requires the
-- original handle, so retain it encrypted and never expose it as household
-- content.
ALTER TABLE person_identities
  ADD COLUMN subject_ciphertext bytea,
  ADD COLUMN subject_key_version text,
  ADD CONSTRAINT person_identities_subject_ciphertext_key_pair CHECK (
    (subject_ciphertext IS NULL) = (subject_key_version IS NULL)
  );

ALTER TABLE people
  ADD COLUMN onboarding_step text NOT NULL DEFAULT 'consent_pending'
  CHECK (onboarding_step IN ('consent_pending', 'name_pending', 'family_pending', 'complete'));

UPDATE people
SET onboarding_step = CASE
  WHEN status = 'registered' AND display_name_ciphertext IS NULL THEN 'name_pending'
  WHEN status = 'registered' AND EXISTS (
    SELECT 1 FROM household_memberships membership
    WHERE membership.person_id = people.id AND membership.status = 'active'
  ) THEN 'complete'
  WHEN status = 'registered' THEN 'family_pending'
  ELSE 'consent_pending'
END;

-- Household invitations are authorized from one exact current group audience.
-- If that audience changes before the private invitation is submitted, the
-- effect must fail closed and the inviter can review the new group instead.
ALTER TABLE invitations
  ADD COLUMN source_conversation_id uuid REFERENCES conversations(id),
  ADD COLUMN source_participant_epoch_id uuid REFERENCES participant_epochs(id),
  ADD COLUMN source_participant_digest text CHECK (
    source_participant_digest IS NULL OR source_participant_digest ~ '^[a-f0-9]{64}$'
  ),
  ADD CONSTRAINT invitations_source_epoch_complete CHECK (
    (source_conversation_id IS NULL AND source_participant_epoch_id IS NULL AND source_participant_digest IS NULL)
    OR
    (source_conversation_id IS NOT NULL AND source_participant_epoch_id IS NOT NULL AND source_participant_digest IS NOT NULL)
  );

ALTER TABLE outbox
  ADD COLUMN invitation_id uuid REFERENCES invitations(id),
  ADD COLUMN invitee_identity_authority_version bigint CHECK (invitee_identity_authority_version > 0),
  ADD COLUMN redrive_root_id uuid REFERENCES outbox(id),
  ADD COLUMN redrive_sequence integer CHECK (redrive_sequence > 0),
  ADD COLUMN source_conversation_id uuid REFERENCES conversations(id),
  ADD COLUMN source_participant_epoch_id uuid REFERENCES participant_epochs(id),
  ADD COLUMN source_expected_participant_digest text CHECK (
    source_expected_participant_digest IS NULL OR source_expected_participant_digest ~ '^[a-f0-9]{64}$'
  ),
  ADD COLUMN source_conversation_authority_version bigint CHECK (
    source_conversation_authority_version > 0
  ),
  ADD CONSTRAINT outbox_invitation_fence_complete CHECK (
    (invitation_id IS NULL) = (invitee_identity_authority_version IS NULL)
  ),
  ADD CONSTRAINT outbox_redrive_complete CHECK (
    (redrive_root_id IS NULL) = (redrive_sequence IS NULL)
  ),
  ADD CONSTRAINT outbox_source_conversation_fence_complete CHECK (
    (source_conversation_id IS NULL AND source_participant_epoch_id IS NULL
      AND source_expected_participant_digest IS NULL AND source_conversation_authority_version IS NULL)
    OR
    (source_conversation_id IS NOT NULL AND source_participant_epoch_id IS NOT NULL
      AND source_expected_participant_digest IS NOT NULL AND source_conversation_authority_version IS NOT NULL)
  );

CREATE UNIQUE INDEX outbox_redrive_sequence_idx
  ON outbox (redrive_root_id, redrive_sequence)
  WHERE redrive_root_id IS NOT NULL;

-- A current provider audience digest lets an already-consented private DM
-- receive transactional family invitations even when its last inbound event
-- has aged out. The effect worker still rereads Linq and fails closed if the
-- live audience has changed.
ALTER TABLE conversation_channels
  ADD COLUMN latest_participant_digest text
  CHECK (
    latest_participant_digest IS NULL
    OR latest_participant_digest ~ '^linq-v1:[a-f0-9]{64}$'
  );
