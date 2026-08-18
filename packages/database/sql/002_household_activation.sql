ALTER TABLE people
  ADD COLUMN messages_address text,
  ADD COLUMN invitation_conversation_id text,
  ADD COLUMN invitation_identity_digest text,
  ADD COLUMN invitation_message_id text,
  ADD COLUMN invitation_issued_at timestamptz,
  ADD COLUMN invitation_reminded_at timestamptz,
  ADD COLUMN invitation_approval_source_id uuid REFERENCES sources(id) ON DELETE SET NULL,
  ADD COLUMN invitation_approved_at timestamptz,
  ADD COLUMN invitation_retry_at timestamptz,
  ADD COLUMN invitation_last_error text,
  ADD CONSTRAINT people_messages_address_e164
    CHECK (messages_address IS NULL OR messages_address ~ '^\+[1-9][0-9]{7,14}$'),
  ADD CONSTRAINT people_invitation_identity_digest
    CHECK (invitation_identity_digest IS NULL OR invitation_identity_digest ~ '^[0-9a-f]{64}$'),
  ADD CONSTRAINT people_invitation_delivery_complete CHECK (
    (invitation_conversation_id IS NULL AND invitation_identity_digest IS NULL
      AND invitation_message_id IS NULL AND invitation_issued_at IS NULL)
    OR
    (invitation_conversation_id IS NOT NULL AND invitation_identity_digest IS NOT NULL
      AND invitation_message_id IS NOT NULL AND invitation_issued_at IS NOT NULL)
  ),
  ADD CONSTRAINT people_invitation_approval_complete CHECK (
    (invitation_approval_source_id IS NULL AND invitation_approved_at IS NULL
      AND invitation_retry_at IS NULL AND invitation_last_error IS NULL)
    OR
    (invitation_approval_source_id IS NOT NULL AND invitation_approved_at IS NOT NULL)
  );

CREATE UNIQUE INDEX people_messages_address_unique ON people(messages_address)
  WHERE messages_address IS NOT NULL;
CREATE UNIQUE INDEX people_invitation_conversation_unique ON people(invitation_conversation_id)
  WHERE invitation_conversation_id IS NOT NULL;

ALTER TABLE households
  ADD COLUMN family_calendar_id text,
  ADD COLUMN family_calendar_owner_connection_id uuid,
  ADD COLUMN family_calendar_partner_connection_id uuid,
  ADD COLUMN family_calendar_label text,
  ADD COLUMN family_calendar_created_at timestamptz,
  ADD CONSTRAINT households_family_calendar_complete CHECK (
    (family_calendar_id IS NULL AND family_calendar_owner_connection_id IS NULL
      AND family_calendar_partner_connection_id IS NULL AND family_calendar_label IS NULL
      AND family_calendar_created_at IS NULL)
    OR
    (family_calendar_id IS NOT NULL AND (
      (family_calendar_owner_connection_id IS NULL
        AND family_calendar_partner_connection_id IS NULL AND family_calendar_label IS NULL
        AND family_calendar_created_at IS NULL)
      OR
      (family_calendar_owner_connection_id IS NOT NULL
        AND family_calendar_partner_connection_id IS NOT NULL AND family_calendar_label IS NOT NULL
        AND family_calendar_created_at IS NOT NULL)
    ))
  );

ALTER TABLE calendar_actions
  ADD COLUMN calendar_id text NOT NULL DEFAULT 'primary',
  ADD COLUMN audience text NOT NULL DEFAULT 'private'
    CHECK (audience IN ('private','household'));

ALTER TABLE sources DROP CONSTRAINT sources_kind_check;
ALTER TABLE sources ADD CONSTRAINT sources_kind_check
  CHECK (kind IN ('linq_message','gmail','google_file','document','web','setup'));
