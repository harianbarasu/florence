-- A conversational routine is only a pending, encrypted proposal until the
-- exact proposed holder confirms a Florence prompt. Bind each prompt to its
-- candidate so a provider reply receipt can select the target without model
-- authority or a free-floating "yes" heuristic.

ALTER TABLE outbox
  ADD COLUMN routine_pattern_candidate_id uuid
    REFERENCES knowledge_candidates(id) ON DELETE RESTRICT;

CREATE INDEX outbox_routine_pattern_candidate_idx
  ON outbox (routine_pattern_candidate_id, created_at DESC)
  WHERE routine_pattern_candidate_id IS NOT NULL;

CREATE FUNCTION enforce_outbox_routine_pattern_binding()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  candidate knowledge_candidates%ROWTYPE;
BEGIN
  IF TG_OP = 'UPDATE'
    AND OLD.routine_pattern_candidate_id IS NOT NULL
    AND NEW.routine_pattern_candidate_id IS DISTINCT FROM OLD.routine_pattern_candidate_id
  THEN
    RAISE EXCEPTION 'A routine-pattern prompt binding cannot be changed or cleared';
  END IF;

  IF NEW.routine_pattern_candidate_id IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT * INTO STRICT candidate
  FROM knowledge_candidates
  WHERE id = NEW.routine_pattern_candidate_id;

  IF candidate.scope_kind <> 'conversation'
    OR candidate.candidate_kind <> 'routine_pattern'
    OR candidate.conversation_id IS DISTINCT FROM NEW.conversation_id
    OR candidate.status <> 'pending'
    OR candidate.expires_at IS NULL
    OR candidate.expires_at <= now()
    OR NEW.effect_kind <> 'linq.message'
  THEN
    RAISE EXCEPTION 'Outbox routine-pattern binding requires one pending exact-conversation candidate';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER outbox_routine_pattern_binding_exact
BEFORE INSERT OR UPDATE OF routine_pattern_candidate_id, conversation_id, effect_kind
ON outbox
FOR EACH ROW
EXECUTE FUNCTION enforce_outbox_routine_pattern_binding();
