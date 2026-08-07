-- Unresolved family coverage escalates privately to every current parent steward.
-- This table is the durable reliance audience: it records who Florence relied on,
-- the exact source and private-DM authority used, and the outbox row that owns
-- provider delivery state. Missing or ineligible private DMs remain explicit
-- unavailable attention instead of widening into another audience.

ALTER TABLE timers
  DROP CONSTRAINT timers_notification_category_check;

ALTER TABLE timers
  ADD CONSTRAINT timers_notification_category_check CHECK (
    notification_category IN (
      'coverage_opening',
      'coverage_reminder',
      'coverage_steward_escalation'
    )
  );

CREATE TABLE coverage_reliance_audiences (
  id uuid PRIMARY KEY,
  coverage_loop_id uuid NOT NULL REFERENCES coverage_loops(id) ON DELETE CASCADE,
  loop_version bigint NOT NULL CHECK (loop_version > 0),
  attention_cycle integer NOT NULL CHECK (attention_cycle > 0),
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  household_control_epoch bigint NOT NULL CHECK (household_control_epoch > 0),
  membership_id uuid NOT NULL REFERENCES household_memberships(id),
  membership_version bigint NOT NULL CHECK (membership_version > 0),
  person_id uuid NOT NULL REFERENCES people(id),
  person_control_epoch bigint NOT NULL CHECK (person_control_epoch > 0),
  minimum_shared_meaning_digest text NOT NULL CHECK (
    minimum_shared_meaning_digest ~ '^[a-f0-9]{64}$'
  ),
  source_conversation_id uuid NOT NULL REFERENCES conversations(id),
  source_conversation_authority_version bigint NOT NULL CHECK (
    source_conversation_authority_version > 0
  ),
  source_participant_epoch_id uuid NOT NULL REFERENCES participant_epochs(id),
  source_participant_set_digest text NOT NULL CHECK (
    source_participant_set_digest ~ '^[a-f0-9]{64}$'
  ),
  target_conversation_id uuid REFERENCES conversations(id),
  target_conversation_authority_version bigint CHECK (
    target_conversation_authority_version > 0
  ),
  target_participant_epoch_id uuid REFERENCES participant_epochs(id),
  target_participant_set_digest text CHECK (
    target_participant_set_digest IS NULL
    OR target_participant_set_digest ~ '^[a-f0-9]{64}$'
  ),
  target_provider_chat_id text,
  target_provider_participant_digest text CHECK (
    target_provider_participant_digest IS NULL
    OR target_provider_participant_digest ~ '^linq-v1:[a-f0-9]{64}$'
  ),
  outbox_id uuid UNIQUE REFERENCES outbox(id),
  dispatch_state text NOT NULL CHECK (dispatch_state IN ('queued', 'unavailable')),
  unavailable_reason text CHECK (
    unavailable_reason IN (
      'no_exact_private_dm',
      'private_authority_denied',
      'provider_audience_changed'
    )
  ),
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  UNIQUE (coverage_loop_id, loop_version, attention_cycle, person_id),
  CHECK (
    (dispatch_state = 'queued'
      AND unavailable_reason IS NULL
      AND target_conversation_id IS NOT NULL
      AND target_conversation_authority_version IS NOT NULL
      AND target_participant_epoch_id IS NOT NULL
      AND target_participant_set_digest IS NOT NULL
      AND target_provider_chat_id IS NOT NULL
      AND target_provider_participant_digest IS NOT NULL
      AND outbox_id IS NOT NULL)
    OR
    (dispatch_state = 'unavailable'
      AND unavailable_reason IS NOT NULL
      AND target_conversation_id IS NULL
      AND target_conversation_authority_version IS NULL
      AND target_participant_epoch_id IS NULL
      AND target_participant_set_digest IS NULL
      AND target_provider_chat_id IS NULL
      AND target_provider_participant_digest IS NULL
      AND outbox_id IS NULL)
  )
);

CREATE INDEX coverage_reliance_audiences_person_attention_idx
  ON coverage_reliance_audiences (person_id, updated_at DESC)
  WHERE dispatch_state = 'unavailable';

CREATE INDEX coverage_reliance_audiences_loop_idx
  ON coverage_reliance_audiences (
    coverage_loop_id,
    loop_version,
    attention_cycle,
    dispatch_state
  );
