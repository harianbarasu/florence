ALTER TABLE person_sessions DROP CONSTRAINT person_sessions_assurance_kind_check;
ALTER TABLE person_sessions ADD CONSTRAINT person_sessions_assurance_kind_check CHECK (
  assurance_kind IN (
    'base', 'google_connect', 'account_controls', 'household_invitation', 'group_coverage',
    'private_bridge_standing'
  )
);
ALTER TABLE person_sessions
  ADD COLUMN assurance_context jsonb NOT NULL DEFAULT '{}'::jsonb;
