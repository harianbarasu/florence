-- Keep every source-backed outbound effect authorized until provider submission.
-- The first evidence fence covered independent private group views only; coverage
-- coordination also derives from direct-person and unanimously shared sources.

CREATE FUNCTION source_evidence_set_is_current(
  evidence_ids uuid[],
  exact_participant_epoch_id uuid,
  exact_viewer_person_id uuid,
  checked_at timestamptz
) RETURNS boolean
LANGUAGE sql
STABLE
SET search_path FROM CURRENT
AS $$
  SELECT cardinality(evidence_ids) = 0
    OR (
      exact_participant_epoch_id IS NOT NULL
      AND exact_viewer_person_id IS NOT NULL
      AND cardinality(evidence_ids) = (
        SELECT count(DISTINCT evidence.source_revision_id)
        FROM unnest(evidence_ids) evidence(source_revision_id)
        JOIN source_revisions revision
          ON revision.id = evidence.source_revision_id
          AND revision.revoked_at IS NULL
          AND revision.content_ciphertext IS NOT NULL
          AND revision.retention_until > checked_at + interval '3 minutes'
        JOIN source_objects object
          ON object.id = revision.source_object_id
          AND object.status = 'active'
          AND object.latest_revision_number = revision.revision_number
        WHERE
          (
            revision.owner_person_id = exact_viewer_person_id
            AND revision.participant_epoch_id IS NULL
            AND revision.conversation_access_mode IS NULL
          )
          OR (
            revision.owner_person_id IS NULL
            AND revision.participant_epoch_id = exact_participant_epoch_id
            AND revision.conversation_access_mode = 'unanimously_shared'
            AND EXISTS (
              SELECT 1
              FROM epoch_participants participant
              JOIN participant_policies policy
                ON policy.conversation_id = (
                  SELECT epoch.conversation_id
                  FROM participant_epochs epoch
                  WHERE epoch.id = exact_participant_epoch_id
                )
                AND policy.person_id = participant.person_id
                AND policy.status = 'active'
                AND policy.allow_content_processing = true
                AND policy.retention_seconds > 0
              WHERE participant.participant_epoch_id = exact_participant_epoch_id
                AND participant.person_id = exact_viewer_person_id
                AND participant.registration_status = 'registered'
                AND participant.consented_at IS NOT NULL
            )
          )
          OR EXISTS (
            SELECT 1
            FROM current_private_source_evidence_access access
            WHERE access.source_revision_id = evidence.source_revision_id
              AND access.participant_epoch_id = exact_participant_epoch_id
              AND access.person_id = exact_viewer_person_id
              AND access.access_starts_at <= checked_at
              AND access.access_expires_at > checked_at + interval '3 minutes'
          )
      )
    )
$$;
