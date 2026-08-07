-- Observe-only group evidence remains exact to the participant epoch in which
-- it occurred. Access is either the existing unanimous shared path or a set of
-- independently retained, person-control-epoch-fenced private views.

ALTER TABLE conversation_channels
  ADD COLUMN latest_participant_checked_at timestamptz;

-- The identity is the immutable member of an exact audience. Its claimed
-- person may legitimately change when a provisional handle is attached to an
-- existing account, and one person may own multiple handles in one group.
ALTER TABLE epoch_participants
  DROP CONSTRAINT epoch_participants_participant_epoch_id_person_id_key;

CREATE OR REPLACE FUNCTION reject_epoch_participant_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.participant_epoch_id <> OLD.participant_epoch_id
    OR NEW.person_identity_id <> OLD.person_identity_id
    OR NEW.added_at <> OLD.added_at
  THEN
    RAISE EXCEPTION 'participant epoch identity membership is immutable';
  END IF;
  IF NEW.person_id <> OLD.person_id AND NOT EXISTS (
    SELECT 1 FROM person_identities identity
    WHERE identity.id = NEW.person_identity_id
      AND identity.person_id = NEW.person_id
      AND identity.status = 'verified'
  ) THEN
    RAISE EXCEPTION 'participant epoch person may follow only its verified identity owner';
  END IF;
  RETURN NEW;
END;
$$;

UPDATE epoch_participants participant
SET person_id = identity.person_id
FROM person_identities identity
WHERE identity.id = participant.person_identity_id
  AND identity.status = 'verified'
  AND participant.person_id <> identity.person_id;

ALTER TABLE source_revisions
  ADD COLUMN conversation_access_mode text,
  ADD CONSTRAINT source_revisions_exact_epoch_key UNIQUE (id, participant_epoch_id);

UPDATE source_revisions
SET conversation_access_mode = 'unanimously_shared'
WHERE participant_epoch_id IS NOT NULL;

ALTER TABLE source_revisions
  ADD CONSTRAINT source_revisions_conversation_access_mode_check CHECK (
    (owner_person_id IS NOT NULL) = (conversation_access_mode IS NULL)
    AND (
      conversation_access_mode IS NULL
      OR conversation_access_mode IN ('unanimously_shared', 'independent_private_views')
    )
  );

CREATE TABLE source_revision_private_views (
  source_revision_id uuid NOT NULL,
  participant_epoch_id uuid NOT NULL,
  person_identity_id uuid NOT NULL,
  person_id uuid NOT NULL,
  person_control_epoch bigint NOT NULL CHECK (person_control_epoch > 0),
  status text NOT NULL CHECK (status IN ('active', 'revoked')),
  retention_until timestamptz NOT NULL,
  granted_at timestamptz NOT NULL,
  revoked_at timestamptz,
  PRIMARY KEY (source_revision_id, person_id),
  FOREIGN KEY (source_revision_id, participant_epoch_id)
    REFERENCES source_revisions(id, participant_epoch_id) ON DELETE CASCADE,
  FOREIGN KEY (participant_epoch_id, person_identity_id)
    REFERENCES epoch_participants(participant_epoch_id, person_identity_id) ON DELETE CASCADE,
  FOREIGN KEY (person_id) REFERENCES people(id) ON DELETE CASCADE,
  CHECK (retention_until > granted_at),
  CHECK (
    (status = 'active' AND revoked_at IS NULL)
    OR (status = 'revoked' AND revoked_at IS NOT NULL AND revoked_at >= granted_at)
  )
);

CREATE INDEX source_revision_private_views_person_active_idx
  ON source_revision_private_views (person_id, retention_until, source_revision_id)
  WHERE status = 'active';

CREATE INDEX source_revision_private_views_epoch_person_idx
  ON source_revision_private_views (participant_epoch_id, person_id, retention_until);

CREATE INDEX source_revision_private_views_expiry_idx
  ON source_revision_private_views (retention_until)
  WHERE status = 'active';

CREATE FUNCTION enforce_source_revision_private_view_scope() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  revision source_revisions%ROWTYPE;
BEGIN
  SELECT * INTO STRICT revision
  FROM source_revisions
  WHERE id = NEW.source_revision_id AND participant_epoch_id = NEW.participant_epoch_id;

  IF revision.conversation_access_mode <> 'independent_private_views'
    OR NEW.retention_until > revision.retention_until
    OR NEW.granted_at < revision.captured_at
    OR NOT EXISTS (
      SELECT 1
      FROM epoch_participants participant
      JOIN person_identities identity ON identity.id = participant.person_identity_id
      WHERE participant.participant_epoch_id = NEW.participant_epoch_id
        AND participant.person_identity_id = NEW.person_identity_id
        AND identity.person_id = NEW.person_id
        AND identity.status = 'verified'
    )
  THEN
    RAISE EXCEPTION 'private source view may not widen revision access, scope, or retention';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER source_revision_private_views_scope
BEFORE INSERT OR UPDATE ON source_revision_private_views
FOR EACH ROW EXECUTE FUNCTION enforce_source_revision_private_view_scope();

-- Pre-migration family-coverage rules were created before coordination reads
-- had their own explicit operation name. Preserve their exact audience while
-- adding only the missing operation.
UPDATE conversation_rules
SET allowed_operations = array_append(allowed_operations, 'coverage_coordination')
WHERE rule_key IN ('family_coverage', 'family_coverage_proposal')
  AND status IN ('active', 'candidate')
  AND NOT ('coverage_coordination' = ANY(allowed_operations));
