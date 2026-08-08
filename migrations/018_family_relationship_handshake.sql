-- A household member may propose how Florence should address one exact
-- observed participant, but the proposed person does not become canonical
-- until that same private identity confirms the invitation.

ALTER TABLE invitations
  ADD COLUMN proposed_display_name_ciphertext bytea,
  ADD COLUMN proposed_display_name_key_version text,
  ADD COLUMN source_revision_id uuid REFERENCES source_revisions(id),
  ADD CONSTRAINT invitations_proposed_display_name_key_pair CHECK (
    (proposed_display_name_ciphertext IS NULL) = (proposed_display_name_key_version IS NULL)
  );

CREATE INDEX invitations_pending_source_revision_idx
  ON invitations (source_revision_id)
  WHERE status = 'pending' AND source_revision_id IS NOT NULL;

-- Old pending invitations and per-chat approvals expressed a different
-- consent contract. They cannot be grandfathered into standing household
-- authority; users can make one fresh natural introduction instead.
UPDATE invitations
SET status = 'revoked', updated_at = now()
WHERE status = 'pending';

WITH revoked_rules AS (
  UPDATE conversation_rules
  SET status = CASE WHEN status = 'active' THEN 'superseded' ELSE 'revoked' END,
    ended_at = COALESCE(ended_at, now())
  WHERE rule_key IN ('family_coverage', 'family_coverage_proposal')
    AND status IN ('active', 'candidate')
  RETURNING conversation_id
)
UPDATE conversations
SET authority_version = authority_version + 1, updated_at = now()
WHERE id IN (SELECT DISTINCT conversation_id FROM revoked_rules);
