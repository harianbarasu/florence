-- A cold-added group participant may be provisional, so Florence cannot use a
-- registered-person fence for the first private enrollment DM. Fence that one
-- outbound recipient by the exact observed identity and subject instead.

ALTER TABLE outbox
  ADD COLUMN recipient_identity_id uuid REFERENCES person_identities(id),
  ADD COLUMN recipient_identity_authority_version bigint CHECK (
    recipient_identity_authority_version > 0
  ),
  ADD COLUMN recipient_identity_subject_digest text CHECK (
    recipient_identity_subject_digest IS NULL
    OR recipient_identity_subject_digest ~ '^[a-f0-9]{64}$'
  ),
  ADD CONSTRAINT outbox_recipient_identity_fence_complete CHECK (
    (
      recipient_identity_id IS NULL
      AND recipient_identity_authority_version IS NULL
      AND recipient_identity_subject_digest IS NULL
    )
    OR
    (
      recipient_identity_id IS NOT NULL
      AND recipient_identity_authority_version IS NOT NULL
      AND recipient_identity_subject_digest IS NOT NULL
    )
  );

CREATE INDEX outbox_recipient_identity_fence_idx
  ON outbox (recipient_identity_id, recipient_identity_authority_version, status, updated_at DESC)
  WHERE recipient_identity_id IS NOT NULL;
