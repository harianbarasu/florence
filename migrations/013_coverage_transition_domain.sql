-- Keep the persisted transition domain aligned with CoverageTransitionSchema.
-- Migration 001 predated changed-evidence revisions and omitted coverage_revised.

ALTER TABLE coverage_transitions
  DROP CONSTRAINT coverage_transitions_transition_kind_check;

ALTER TABLE coverage_transitions
  ADD CONSTRAINT coverage_transitions_transition_kind_check CHECK (
    transition_kind IN (
      'facts_resolved',
      'coverage_revised',
      'coverage_requested',
      'coverage_acknowledged',
      'coverage_declined_privately',
      'coverage_at_risk',
      'coverage_participant_revoked',
      'cancelled',
      'superseded',
      'dismissed',
      'expired_uncovered'
    )
  );
