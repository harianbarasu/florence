-- A response derived from private group evidence must remain authorized until
-- the irreversible provider submission, not merely while the model is running.

ALTER TABLE outbox
  ADD COLUMN evidence_source_revision_ids uuid[] NOT NULL DEFAULT '{}',
  ADD CONSTRAINT outbox_evidence_source_revision_count CHECK (
    cardinality(evidence_source_revision_ids) BETWEEN 0 AND 32
  ),
  ADD CONSTRAINT outbox_evidence_source_scope_complete CHECK (
    cardinality(evidence_source_revision_ids) = 0
    OR (
      person_id IS NOT NULL
      AND source_conversation_id IS NOT NULL
      AND source_participant_epoch_id IS NOT NULL
    )
  );

-- The preceding release could queue a private invocation without persisting
-- its evidence set. Fail those not-yet-submitted effects closed on upgrade;
-- new attempts will carry exact evidence IDs through this migration's fence.
UPDATE disclosure_decisions decision
SET revoked_at = now()
FROM outbox effect
WHERE effect.authorization_decision_id = decision.id
  AND effect.idempotency_key LIKE 'private-group-invocation:%'
  AND effect.status IN ('pending', 'retry', 'leased', 'dead')
  AND decision.revoked_at IS NULL;

UPDATE outbox
SET status = 'cancelled',
    lease_owner = NULL,
    lease_token = NULL,
    lease_expires_at = NULL,
    last_error_code = 'missing_source_evidence_fence',
    updated_at = now()
WHERE idempotency_key LIKE 'private-group-invocation:%'
  AND status IN ('pending', 'retry', 'leased', 'dead');

CREATE VIEW current_private_source_evidence_access AS
SELECT
  revision.id AS source_revision_id,
  revision.participant_epoch_id,
  private_view.person_id,
  greatest(
    revision.captured_at,
    private_view.granted_at,
    participant.consented_at,
    identity.verified_at,
    person.registered_at,
    policy.effective_at
  ) AS access_starts_at,
  least(
    revision.retention_until,
    private_view.retention_until,
    revision.captured_at + policy.retention_seconds * interval '1 second'
  ) AS access_expires_at
FROM source_revisions revision
JOIN source_objects object
  ON object.id = revision.source_object_id
  AND object.status = 'active'
  AND object.latest_revision_number = revision.revision_number
JOIN source_revision_private_views private_view
  ON private_view.source_revision_id = revision.id
  AND private_view.participant_epoch_id = revision.participant_epoch_id
JOIN participant_epochs epoch
  ON epoch.id = revision.participant_epoch_id
  AND epoch.ended_at IS NULL
JOIN conversations conversation
  ON conversation.id = epoch.conversation_id
  AND conversation.status = 'active'
  AND conversation.current_epoch_id = epoch.id
JOIN epoch_participants participant
  ON participant.participant_epoch_id = private_view.participant_epoch_id
  AND participant.person_identity_id = private_view.person_identity_id
  AND participant.registration_status = 'registered'
  AND participant.consented_at IS NOT NULL
JOIN person_identities identity
  ON identity.id = participant.person_identity_id
  AND identity.person_id = private_view.person_id
  AND identity.status = 'verified'
  AND identity.verified_at IS NOT NULL
JOIN people person
  ON person.id = private_view.person_id
  AND person.status = 'registered'
  AND person.registered_at IS NOT NULL
  AND person.control_epoch = private_view.person_control_epoch
JOIN participant_policies policy
  ON policy.conversation_id = conversation.id
  AND policy.person_id = private_view.person_id
  AND policy.status = 'active'
  AND policy.allow_content_processing = true
  AND policy.retention_seconds > 0
WHERE revision.revoked_at IS NULL
  AND revision.content_ciphertext IS NOT NULL
  AND revision.conversation_access_mode = 'independent_private_views'
  AND private_view.status = 'active'
  AND private_view.revoked_at IS NULL;

CREATE FUNCTION private_source_evidence_set_is_current(
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
        JOIN current_private_source_evidence_access access
          ON access.source_revision_id = evidence.source_revision_id
          AND access.participant_epoch_id = exact_participant_epoch_id
          AND access.person_id = exact_viewer_person_id
          AND access.access_starts_at <= checked_at
          -- Linq requests may run for up to two minutes. Do not cross a hard
          -- evidence-retention boundary between this check and provider POST.
          AND access.access_expires_at > checked_at + interval '3 minutes'
      )
    )
$$;
