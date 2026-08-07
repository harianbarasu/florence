-- A source correlation is a one-way case key. Provider identifiers and raw
-- content remain encrypted in source revisions; only their stable digest is
-- available for assembling a current private reconciliation frontier.

ALTER TABLE source_objects
  ADD COLUMN correlation_digest text
  CHECK (correlation_digest IS NULL OR correlation_digest ~ '^[a-f0-9]{64}$');

CREATE INDEX source_objects_private_correlation_idx
  ON source_objects (integration_id, correlation_digest, updated_at DESC)
  WHERE correlation_digest IS NOT NULL;

CREATE FUNCTION enforce_source_object_correlation_identity() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.correlation_digest IS NOT NULL
     AND NEW.correlation_digest IS DISTINCT FROM OLD.correlation_digest
  THEN
    RAISE EXCEPTION 'source object correlation identity is immutable';
  END IF;
  IF OLD.correlation_digest IS NULL
     AND NEW.correlation_digest IS NOT NULL
     AND OLD.provider NOT IN ('gmail', 'gmail.attachment')
  THEN
    RAISE EXCEPTION 'only an existing Gmail object may adopt a correlation identity';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER source_objects_correlation_identity
BEFORE UPDATE OF correlation_digest ON source_objects
FOR EACH ROW EXECUTE FUNCTION enforce_source_object_correlation_identity();

CREATE TABLE private_source_frontiers (
  id uuid PRIMARY KEY,
  owner_person_id uuid NOT NULL REFERENCES people(id) ON DELETE CASCADE,
  integration_id uuid NOT NULL REFERENCES integrations(id) ON DELETE CASCADE,
  case_kind text NOT NULL CHECK (case_kind = 'gmail_thread'),
  case_key_digest text NOT NULL CHECK (case_key_digest ~ '^[a-f0-9]{64}$'),
  version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
  frontier_digest text NOT NULL CHECK (frontier_digest ~ '^[a-f0-9]{64}$'),
  source_generation bigint NOT NULL DEFAULT 0 CHECK (source_generation >= 0),
  reconciled_generation bigint NOT NULL DEFAULT 0 CHECK (reconciled_generation >= 0),
  evidence_source_revision_ids uuid[] NOT NULL,
  current_candidate_id uuid REFERENCES knowledge_candidates(id) ON DELETE CASCADE,
  disposition text NOT NULL CHECK (disposition IN ('quiet', 'candidate')),
  reconciled_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (owner_person_id, integration_id, case_kind, case_key_digest),
  CHECK (cardinality(evidence_source_revision_ids) BETWEEN 1 AND 150),
  CHECK (reconciled_generation <= source_generation),
  CHECK (
    (disposition = 'candidate' AND current_candidate_id IS NOT NULL)
    OR (disposition = 'quiet' AND current_candidate_id IS NULL)
  )
);

CREATE INDEX private_source_frontiers_evidence_idx
  ON private_source_frontiers USING gin (evidence_source_revision_ids);
